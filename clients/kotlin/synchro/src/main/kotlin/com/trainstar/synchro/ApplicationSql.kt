package com.trainstar.synchro

import java.util.Locale

/**
 * The application SQL boundary accepts one read statement or one ordinary DML
 * statement. It parses SQL tokens before SQLite receives the statement.
 *
 * This is deliberately a positive grammar. It does not try to remove unsafe
 * text from an otherwise unrestricted SQLite program.
 */
internal object ApplicationSql {
    enum class Kind {
        READ,
        WRITE,
    }

    data class Statement(
        val kind: Kind,
        val writeTarget: String? = null,
        val writeOperation: String? = null,
    )

    fun authorizeRead(sql: String): Statement = authorize(sql, Kind.READ)

    fun authorizeWrite(sql: String): Statement = authorize(sql, Kind.WRITE)

    fun requireApplicationObject(name: String) {
        if (isReservedIdentifier(name)) {
            throw IllegalArgumentException("Synchro reserved objects are not available to application SQL")
        }
    }

    private fun authorize(sql: String, expected: Kind): Statement {
        val tokens = SqlLexer(sql).tokens()
        if (tokens.isEmpty()) throw IllegalArgumentException("Application SQL is empty")
        if (tokens.count { it is Token.Symbol && it.value == ";" } > 1 ||
            tokens.lastOrNull().let { it is Token.Symbol && it.value == ";" }.not() &&
            tokens.any { it is Token.Symbol && it.value == ";" }
        ) {
            throw IllegalArgumentException("Application SQL must contain one statement")
        }
        val statementTokens = if (tokens.lastOrNull() == Token.Symbol(";")) tokens.dropLast(1) else tokens
        if (statementTokens.isEmpty()) throw IllegalArgumentException("Application SQL is empty")

        validateIdentifiers(statementTokens)
        val parser = StatementParser(statementTokens)
        return parser.parse(expected)
    }

    private fun validateIdentifiers(tokens: List<Token>) {
        tokens.forEach { token ->
            if (token is Token.Identifier && isReservedIdentifier(token.value)) {
                throw IllegalArgumentException("Synchro reserved objects are not available to application SQL")
            }
        }
    }

    private fun isReservedIdentifier(name: String): Boolean {
        val canonical = name.lowercase(Locale.ROOT)
        return canonical.startsWith("_synchro_") || canonical.startsWith("sqlite_")
    }

    private sealed class Token {
        data class Identifier(val value: String) : Token()
        data class Keyword(val value: String) : Token()
        data class Symbol(val value: String) : Token()
        data object Literal : Token()
        data object Parameter : Token()
    }

    /** A SQLite lexer that keeps identifiers separate from literals and comments. */
    private class SqlLexer(private val source: String) {
        private var index = 0

        fun tokens(): List<Token> {
            val result = mutableListOf<Token>()
            while (true) {
                skipTrivia()
                if (index >= source.length) return result
                result += nextToken()
            }
        }

        private fun skipTrivia() {
            while (index < source.length) {
                when {
                    source[index].isWhitespace() -> index += 1
                    source.startsWith("--", index) -> {
                        index += 2
                        while (index < source.length && source[index] != '\n' && source[index] != '\r') index += 1
                    }
                    source.startsWith("/*", index) -> {
                        val end = source.indexOf("*/", index + 2)
                        if (end < 0) throw IllegalArgumentException("Application SQL has an unterminated comment")
                        index = end + 2
                    }
                    else -> return
                }
            }
        }

        private fun nextToken(): Token {
            return when (val character = source[index]) {
                '\'', '"', '`', '[' -> quoted(character)
                '?', ':', '@', '$' -> parameter()
                in '0'..'9' -> number()
                in 'A'..'Z', in 'a'..'z', '_' -> word()
                else -> {
                    index += 1
                    Token.Symbol(character.toString())
                }
            }
        }

        private fun quoted(opening: Char): Token {
            val closing = if (opening == '[') ']' else opening
            index += 1
            val value = StringBuilder()
            while (index < source.length) {
                val character = source[index++]
                if (character == closing) {
                    if (opening != '[' && index < source.length && source[index] == closing) {
                        value.append(closing)
                        index += 1
                        continue
                    }
                    return if (opening == '\'') Token.Literal else Token.Identifier(value.toString())
                }
                value.append(character)
            }
            throw IllegalArgumentException("Application SQL has an unterminated quoted value")
        }

        private fun parameter(): Token {
            val opening = source[index++]
            if (opening == '?') {
                while (index < source.length && source[index].isDigit()) index += 1
                return Token.Parameter
            }
            if (index >= source.length || !isIdentifierStart(source[index])) {
                throw IllegalArgumentException("Application SQL has an invalid parameter")
            }
            index += 1
            while (index < source.length && isIdentifierPart(source[index])) index += 1
            return Token.Parameter
        }

        private fun number(): Token {
            index += 1
            while (index < source.length && (source[index].isDigit() || source[index] in ".eE+-")) index += 1
            return Token.Literal
        }

        private fun word(): Token {
            val start = index++
            while (index < source.length && isIdentifierPart(source[index])) index += 1
            val value = source.substring(start, index)
            return if (value.uppercase(Locale.ROOT) in KEYWORDS) {
                Token.Keyword(value.uppercase(Locale.ROOT))
            } else {
                Token.Identifier(value)
            }
        }

        private fun isIdentifierStart(character: Char): Boolean =
            character == '_' || character.isLetter()

        private fun isIdentifierPart(character: Char): Boolean =
            character == '_' || character == '$' || character.isLetterOrDigit()
    }

    /**
     * This parser permits only SQLite read and DML statement heads. CTE bodies
     * are parsed as read-only statements. Therefore a CTE cannot hide DDL or a
     * write to Synchro state.
     */
    private class StatementParser(private val tokens: List<Token>) {
        private var index = 0

        fun parse(expected: Kind): Statement {
            parseWithClauseIfPresent()
            val statement = when (keywordAt(index)) {
                "SELECT" -> Statement(Kind.READ)
                "INSERT" -> parseInsert()
                "UPDATE" -> parseUpdate()
                "DELETE" -> parseDelete()
                else -> throw IllegalArgumentException("Application SQL statement is not allowed")
            }
            if (statement.kind != expected) {
                throw IllegalArgumentException("Application SQL operation is not valid for this API")
            }
            rejectForbiddenOperations()
            return statement
        }

        private fun parseWithClauseIfPresent() {
            if (keywordAt(index) != "WITH") return
            index += 1
            if (keywordAt(index) == "RECURSIVE") index += 1
            do {
                consumeIdentifier("CTE name")
                if (symbolAt(index) == "(") skipBalancedParentheses()
                requireKeyword("AS")
                if (keywordAt(index) == "NOT") {
                    index += 1
                    requireKeyword("MATERIALIZED")
                } else if (keywordAt(index) == "MATERIALIZED") {
                    index += 1
                }
                if (symbolAt(index) != "(") throw IllegalArgumentException("Application SQL CTE has no query")
                val body = consumeBalancedParentheses()
                StatementParser(body).parse(Kind.READ)
            } while (consumeSymbol(","))
        }

        private fun parseInsert(): Statement {
            index += 1
            if (keywordAt(index) == "OR") {
                throw IllegalArgumentException("Application SQL conflict clauses are not allowed for synced writes")
            }
            requireKeyword("INTO")
            return Statement(Kind.WRITE, consumeObjectName(), "insert")
        }

        private fun parseUpdate(): Statement {
            index += 1
            if (keywordAt(index) == "OR") {
                throw IllegalArgumentException("Application SQL conflict clauses are not allowed for synced writes")
            }
            return Statement(Kind.WRITE, consumeObjectName(), "update")
        }

        private fun parseDelete(): Statement {
            index += 1
            requireKeyword("FROM")
            return Statement(Kind.WRITE, consumeObjectName(), "delete")
        }

        private fun consumeObjectName(): String {
            var finalName = consumeIdentifier("table name")
            while (consumeSymbol(".")) finalName = consumeIdentifier("qualified table name")
            return finalName
        }

        private fun consumeIdentifier(subject: String): String {
            val token = tokens.getOrNull(index)
            if (token !is Token.Identifier) throw IllegalArgumentException("Application SQL has an invalid $subject")
            index += 1
            return token.value
        }

        private fun requireKeyword(value: String) {
            if (keywordAt(index) != value) throw IllegalArgumentException("Application SQL is missing $value")
            index += 1
        }

        private fun consumeSymbol(value: String): Boolean {
            if (symbolAt(index) != value) return false
            index += 1
            return true
        }

        private fun skipBalancedParentheses() {
            consumeBalancedParentheses()
        }

        private fun consumeBalancedParentheses(): List<Token> {
            if (symbolAt(index) != "(") throw IllegalArgumentException("Application SQL has unbalanced parentheses")
            index += 1
            val start = index
            var depth = 1
            while (index < tokens.size) {
                when (tokens[index]) {
                    Token.Symbol("(") -> depth += 1
                    Token.Symbol(")") -> {
                        depth -= 1
                        if (depth == 0) {
                            val body = tokens.subList(start, index).toList()
                            index += 1
                            return body
                        }
                    }
                    else -> Unit
                }
                index += 1
            }
            throw IllegalArgumentException("Application SQL has unbalanced parentheses")
        }

        private fun rejectForbiddenOperations() {
            tokens.forEach { token ->
                if (token is Token.Keyword && token.value in FORBIDDEN_KEYWORDS) {
                    throw IllegalArgumentException("Application SQL statement is not allowed")
                }
            }
        }

        private fun keywordAt(position: Int): String? = (tokens.getOrNull(position) as? Token.Keyword)?.value

        private fun symbolAt(position: Int): String? = (tokens.getOrNull(position) as? Token.Symbol)?.value
    }

    private val KEYWORDS = setOf(
        "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ATTACH",
        "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
        "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
        "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH",
        "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE",
        "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL",
        "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX",
        "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN",
        "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING",
        "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER",
        "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE",
        "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK",
        "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO",
        "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES",
        "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT",
    )

    private val FORBIDDEN_KEYWORDS = setOf(
        "ALTER", "ANALYZE", "ATTACH", "BEGIN", "COMMIT", "CREATE", "DETACH", "DROP", "EXPLAIN", "PRAGMA",
        "REINDEX", "RELEASE", "ROLLBACK", "SAVEPOINT", "VACUUM",
    )
}
