package com.trainstar.synchro

import kotlinx.serialization.json.*
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.CodingErrorAction
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.security.MessageDigest
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

internal object Integrity {
    const val maxWireJSONBytes = 64 * 1024 * 1024
    private const val maxWireJSONDepth = 128
    private val rowIdentityDomain = "synchro:v3:row-identity:v1\u0000".toByteArray()
    private val rowDigestDomain = "synchro:v3:row-digest:v1\u0000".toByteArray()
    private val scopeDigestDomain = "synchro:v3:scope-digest:v1\u0000".toByteArray()
    private val schemaManifestDomain = "synchro:v3:schema-manifest:v1\u0000".toByteArray()

    data class RowDigest(val identity: ByteArray, val checksum: ChecksumObject)

    fun schemaManifestHash(manifest: SchemaManifest): String {
        val body = buildJsonObject {
            put("schema_version", manifest.schemaVersion)
            put("parent_schema", manifest.parentSchema?.let { parent ->
                buildJsonObject {
                    put("version", parent.version)
                    put("hash", parent.hash)
                }
            } ?: JsonNull)
            put("transition_class", manifest.transitionClass)
            put("compatibility_floor", manifest.compatibilityFloor)
            putJsonArray("tables") {
                manifest.tables.sortedWith { left, right ->
                    compareUnsigned(left.tableID.toByteArray(), right.tableID.toByteArray())
                }.forEach { table ->
                    addJsonObject {
                        put("table_id", table.tableID)
                        put("relation_id", table.relationID)
                        put("name", table.name)
                        put(
                            "composition",
                            when (table.composition) {
                                CompositionClass.SINGLE_SCOPE -> "single_scope"
                                CompositionClass.MULTI_SCOPE -> "multi_scope"
                            }
                        )
                        put("primary_key_field_id", table.primaryKeyFieldID)
                        putJsonObject("lifecycle") {
                            put("created_at_field_id", table.lifecycle.createdAtFieldID?.let(::JsonPrimitive) ?: JsonNull)
                            put("updated_at_field_id", table.lifecycle.updatedAtFieldID?.let(::JsonPrimitive) ?: JsonNull)
                            put("deleted_at_field_id", table.lifecycle.deletedAtFieldID?.let(::JsonPrimitive) ?: JsonNull)
                        }
                        putJsonArray("fields") {
                            table.fields.sortedWith { left, right ->
                                compareUnsigned(left.fieldID.toByteArray(), right.fieldID.toByteArray())
                            }.forEach { field ->
                                addJsonObject {
                                    put("field_id", field.fieldID)
                                    put("name", field.name)
                                    put("type", field.typeName)
                                    put("nullable", field.nullable)
                                    put("writable", field.writable)
                                    field.precision?.let { put("precision", it) }
                                    field.scale?.let { put("scale", it) }
                                }
                            }
                        }
                        putJsonArray("indexes") {
                            table.indexes.sortedWith { left, right ->
                                compareUnsigned(left.indexID.toByteArray(), right.indexID.toByteArray())
                            }.forEach { index ->
                                addJsonObject {
                                    put("index_id", index.indexID)
                                    put("name", index.name)
                                    putJsonArray("field_ids") { index.fieldIDs.forEach(::add) }
                                    put("unique", index.unique)
                                }
                            }
                        }
                    }
                }
            }
        }
        return hex(sha256(schemaManifestDomain + canonicalJSON(body).toByteArray()))
    }

    fun rowIdentity(table: LocalSchemaTable, pk: JsonObject): ByteArray {
        val field = table.columns.singleOrNull { it.fieldID == table.primaryKeyFieldID }
            ?: invalid("primary key field")
        val value = pk.takeIf { it.size == 1 }?.get(table.primaryKeyFieldID)
            ?: invalid("primary key")
        return bytes {
            put(rowIdentityDomain)
            text(table.tableID)
            text(table.primaryKeyFieldID)
            typedValue(value, field, true)
        }
    }

    fun rowDigest(
        schemaHash: String,
        table: LocalSchemaTable,
        pk: JsonObject,
        row: JsonObject,
        serverVersion: String,
    ): RowDigest {
        val (identity, preimage) = rowDigestPreimage(schemaHash, table, pk, row, serverVersion)
        return RowDigest(identity, checksum(sha256(preimage)))
    }

    fun rowDigestPreimage(
        schemaHash: String,
        table: LocalSchemaTable,
        pk: JsonObject,
        row: JsonObject,
        serverVersion: String,
    ): Pair<ByteArray, ByteArray> {
        require(serverVersion.isNotEmpty()) { "server version is empty" }
        val identity = rowIdentity(table, pk)
        val body = rowBody(table, pk, row)
        val preimage = bytes {
            put(rowDigestDomain)
            put(decodeHex(schemaHash))
            blob(identity)
            blob(body)
            text(serverVersion)
        }
        return identity to preimage
    }

    fun scopeDigest(
        schemaHash: String,
        scopeID: String,
        entries: List<Pair<ByteArray, ChecksumObject>>,
    ): ChecksumObject {
        return checksum(sha256(scopeDigestPreimage(schemaHash, scopeID, entries)))
    }

    fun scopeDigestPreimage(
        schemaHash: String,
        scopeID: String,
        entries: List<Pair<ByteArray, ChecksumObject>>,
    ): ByteArray {
        require(scopeID.isNotEmpty()) { "scope id is empty" }
        val sorted = entries.sortedWith { left, right -> compareUnsigned(left.first, right.first) }
        sorted.forEach { (identity, _) -> validateRowIdentity(identity) }
        sorted.zipWithNext().forEach { (left, right) ->
            require(!left.first.contentEquals(right.first)) { "scope contains a duplicate row identity" }
        }
        return bytes {
            put(scopeDigestDomain)
            put(decodeHex(schemaHash))
            text(scopeID)
            u64(sorted.size.toLong())
            sorted.forEach { (identity, digest) ->
                digest.validate()
                blob(identity)
                put(decodeHex(digest.digest))
            }
        }
    }

    fun encodedTypedValue(source: String, field: LocalSchemaColumn): ByteArray {
        val value = Json { isLenient = false }.parseToJsonElement(source)
        if (value !is JsonNull && field.logicalType == "int" && !canonicalInteger(source)) invalid(field.fieldID)
        if (value !is JsonNull && field.logicalType == "float") {
            val primitive = value as? JsonPrimitive
            val number = primitive?.takeIf { !it.isString }?.doubleOrNull ?: invalid(field.fieldID)
            if (canonicalDouble(number) != source) invalid(field.fieldID)
        }
        return bytes { typedValue(value, field, false) }
    }

    /** Validates one authored wire value without changing its representation. */
    fun validateTypedValue(value: JsonElement, field: LocalSchemaColumn, requirePresent: Boolean = false) {
        bytes { typedValue(value, field, requirePresent) }
    }

    /** Protocol mutation timestamps use exactly six UTC fractional digits. */
    fun validateCanonicalClientVersion(value: String) {
        if (!canonicalDateTime(value)) invalid("client version")
    }

    fun isValidText(value: String): Boolean = validText(value)

    fun validateCanonicalWireJSON(source: String) {
        if (source.toByteArray(Charsets.UTF_8).size > maxWireJSONBytes) invalid("JSON response size")
        val cursor = JSONCursor()
        parseWireJSONValue(source, cursor, 0)
        skipJSONWhitespace(source, cursor)
        if (cursor.index != source.length) invalid("trailing JSON data")
    }

    fun decodeCanonicalWireJSON(source: ByteArray): String {
        if (source.size > maxWireJSONBytes) invalid("JSON response size")
        val decoded = try {
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(source))
                .toString()
        } catch (_: Exception) {
            invalid("JSON UTF-8")
        }
        validateCanonicalWireJSON(decoded)
        return decoded
    }

    fun stableUUID(domain: String, values: List<String>): String {
        val digest = sha256(bytes {
            put(domain.toByteArray())
            byte(0)
            values.forEach(::text)
        }).copyOf(16)
        digest[6] = ((digest[6].toInt() and 0x0f) or 0x50).toByte()
        digest[8] = ((digest[8].toInt() and 0x3f) or 0x80).toByte()
        val hex = hex(digest)
        return "${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}"
    }

    private fun rowBody(table: LocalSchemaTable, pk: JsonObject, row: JsonObject): ByteArray {
        val fieldIDs = table.columns.map { it.fieldID }.toSet()
        if (row.keys != fieldIDs || row[table.primaryKeyFieldID] != pk[table.primaryKeyFieldID]) {
            invalid("row field set or primary key")
        }
        val fields = table.columns.sortedWith { left, right ->
            compareUnsigned(left.fieldID.toByteArray(), right.fieldID.toByteArray())
        }
        return bytes {
            u32(fields.size)
            fields.forEach { field ->
                text(field.fieldID)
                typedValue(row.getValue(field.fieldID), field, false)
            }
        }
    }

    private fun Encoder.typedValue(value: JsonElement, field: LocalSchemaColumn, requirePresent: Boolean) {
        val type = field.logicalType
        byte(typeTag(type))
        if (value is JsonNull) {
            if (requirePresent || !field.nullable) invalid(field.fieldID)
            byte(0)
            return
        }
        byte(1)
        val primitive = value as? JsonPrimitive ?: invalid(field.fieldID)
        when (type) {
            "string" -> text(primitive.takeIf { it.isString }?.content?.takeIf(::validText) ?: invalid(field.fieldID))
            "int" -> i32(
                primitive.takeIf { !it.isString && canonicalInteger(it.content) }?.intOrNull
                    ?: invalid(field.fieldID)
            )
            "int64" -> i64(primitive.takeIf { it.isString && canonicalInteger(it.content) }?.content?.toLongOrNull() ?: invalid(field.fieldID))
            "decimal" -> blob(primitive.takeIf {
                it.isString && canonicalDecimal(it.content) && decimalFits(it.content, field.precision, field.scale)
            }?.content?.toByteArray() ?: invalid(field.fieldID))
            "float" -> {
                val number = primitive.takeIf { !it.isString }?.doubleOrNull?.takeIf { it.isFinite() } ?: invalid(field.fieldID)
                u64(java.lang.Double.doubleToRawLongBits(if (number == 0.0) 0.0 else number))
            }
            "boolean" -> byte(
                if (primitive.takeIf { !it.isString }?.booleanOrNull ?: invalid(field.fieldID)) 1 else 0
            )
            "datetime" -> blob(primitive.takeIf { it.isString && canonicalDateTime(it.content) }?.content?.toByteArray() ?: invalid(field.fieldID))
            "date" -> blob(primitive.takeIf { it.isString && canonicalDate(it.content) }?.content?.toByteArray() ?: invalid(field.fieldID))
            "time" -> blob(primitive.takeIf { it.isString && canonicalTime(it.content) }?.content?.toByteArray() ?: invalid(field.fieldID))
            "json" -> {
                val source = primitive.takeIf { it.isString }?.content ?: invalid(field.fieldID)
                validateSafeJSONIntegers(source)
                val parsed = Json { isLenient = false }.parseToJsonElement(source)
                val canonical = canonicalJSON(parsed)
                if (canonical != source) invalid(field.fieldID)
                blob(canonical.toByteArray())
            }
            "bytes" -> {
                val source = primitive.takeIf { it.isString && '=' !in it.content }?.content ?: invalid(field.fieldID)
                val decoded = decodeBase64URL(source) ?: invalid(field.fieldID)
                val canonical = encodeBase64URL(decoded)
                if (canonical != source) invalid(field.fieldID)
                blob(decoded)
            }
            else -> invalid(field.fieldID)
        }
    }

    private fun typeTag(type: String): Int = when (type) {
        "string" -> 0x01
        "int" -> 0x02
        "int64" -> 0x03
        "decimal" -> 0x04
        "float" -> 0x05
        "boolean" -> 0x06
        "datetime" -> 0x07
        "date" -> 0x08
        "time" -> 0x09
        "json" -> 0x0a
        "bytes" -> 0x0b
        else -> invalid("portable type $type")
    }

    private fun canonicalInteger(value: String): Boolean =
        value == "0" || Regex("-?[1-9][0-9]*").matches(value)

    private fun canonicalDecimal(value: String): Boolean =
        value == "0" || Regex("-?(?:[1-9][0-9]*(?:\\.[0-9]*[1-9])?|0\\.[0-9]*[1-9])").matches(value)

    private fun decimalFits(value: String, precision: Int?, scale: Int?): Boolean {
        if (precision == null || scale == null || precision <= 0 || scale < 0 || scale > precision) return false
        val parts = value.removePrefix("-").split('.', limit = 2)
        val integerDigits = parts[0].trimStart('0').length
        val fractionDigits = parts.getOrNull(1)?.length ?: 0
        return integerDigits <= precision - scale &&
            fractionDigits <= scale &&
            integerDigits + fractionDigits <= precision
    }

    private fun canonicalDateTime(value: String): Boolean = try {
        value.length == 27 && value.endsWith('Z') &&
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                .withResolverStyle(ResolverStyle.STRICT)
                .withZone(ZoneOffset.UTC)
                .parse(value) != null
    } catch (_: Exception) { false }

    private fun canonicalDate(value: String): Boolean = try {
        value.length == 10 && LocalDate.parse(
            value,
            DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.STRICT)
        ).toString() == value
    } catch (_: Exception) { false }

    private fun canonicalTime(value: String): Boolean = try {
        value.length == 15 && DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
            .withResolverStyle(ResolverStyle.STRICT)
            .parse(value) != null
    } catch (_: Exception) { false }

    private fun validText(value: String): Boolean {
        var index = 0
        while (index < value.length) {
            val character = value[index]
            if (Character.isHighSurrogate(character)) {
                if (index + 1 >= value.length || !Character.isLowSurrogate(value[index + 1])) return false
                index += 2
            } else {
                if (Character.isLowSurrogate(character)) return false
                index += 1
            }
        }
        return true
    }

    private fun canonicalJSON(value: JsonElement): String = when (value) {
        JsonNull -> "null"
        is JsonPrimitive -> when {
            value.isString -> {
                if (!validText(value.content)) invalid("JSON string")
                Json.encodeToString(JsonPrimitive.serializer(), value)
            }
            value.booleanOrNull != null -> value.boolean.toString()
            else -> canonicalDouble(value.double)
        }
        is JsonArray -> value.joinToString(separator = ",", prefix = "[", postfix = "]") { canonicalJSON(it) }
        is JsonObject -> value.entries.sortedWith { left, right ->
            compareUTF16(left.key, right.key)
        }.joinToString(separator = ",", prefix = "{", postfix = "}") { (key, item) ->
            if (!validText(key)) invalid("JSON object key")
            Json.encodeToString(JsonPrimitive.serializer(), JsonPrimitive(key)) + ":" + canonicalJSON(item)
        }
    }

    private fun canonicalDouble(value: Double): String {
        if (!value.isFinite()) invalid("JSON number")
        if (value == 0.0) return "0"
        val shortest = shortestRoundTripDecimal(value)
        val magnitude = kotlin.math.abs(value)
        if (magnitude >= 1e-6 && magnitude < 1e21) {
            return shortest.toPlainString()
        }
        val digits = shortest.unscaledValue().abs().toString()
        val exponent = digits.length - shortest.scale() - 1
        val sign = if (value < 0) "-" else ""
        val mantissa = if (digits.length == 1) digits else "${digits.first()}.${digits.drop(1)}"
        return sign + mantissa + "e" + if (exponent < 0) exponent.toString() else "+$exponent"
    }

    private fun shortestRoundTripDecimal(value: Double): BigDecimal {
        val source = BigDecimal.valueOf(value)
        for (precision in 1..17) {
            val candidate = source.round(MathContext(precision, RoundingMode.HALF_EVEN)).stripTrailingZeros()
            if (java.lang.Double.doubleToRawLongBits(candidate.toDouble()) == java.lang.Double.doubleToRawLongBits(value)) {
                return candidate
            }
        }
        return source.stripTrailingZeros()
    }

    private fun validateSafeJSONIntegers(source: String) {
        var index = 0
        while (index < source.length) {
            if (source[index] == '"') {
                index += 1
                while (index < source.length) {
                    if (source[index] == '\\') index += 2
                    else if (source[index] == '"') { index += 1; break }
                    else index += 1
                }
                continue
            }
            if (source[index] == '-' || source[index].isDigit()) {
                val start = index++
                while (index < source.length && source[index] !in "\t\n\r ,]}") index += 1
                if (unsafeJSONInteger(source.substring(start, index))) invalid("unsafe JSON integer")
                continue
            }
            index += 1
        }
    }

    private fun unsafeJSONInteger(source: String): Boolean {
        var unsigned = source.removePrefix("-")
        var exponent = 0
        val exponentIndex = unsigned.indexOfAny(charArrayOf('e', 'E'))
        if (exponentIndex >= 0) {
            val text = unsigned.substring(exponentIndex + 1)
            unsigned = unsigned.substring(0, exponentIndex)
            if (text.length > 7) return true
            exponent = text.toIntOrNull() ?: return true
        }
        var fractionDigits = 0
        val point = unsigned.indexOf('.')
        if (point >= 0) {
            fractionDigits = unsigned.length - point - 1
            unsigned = unsigned.removeRange(point, point + 1)
        }
        unsigned = unsigned.trimStart('0')
        if (unsigned.isEmpty()) return false
        val scale = fractionDigits - exponent
        if (scale > 0) {
            if (scale >= unsigned.length) return false
            val split = unsigned.length - scale
            if (unsigned.substring(split).any { it != '0' }) return false
            unsigned = unsigned.substring(0, split)
        } else if (scale < 0) {
            if (unsigned.length - scale > 16) return true
            unsigned += "0".repeat(-scale)
        }
        unsigned = unsigned.trimStart('0')
        return when {
            unsigned.length < 16 -> false
            unsigned.length > 16 -> true
            else -> unsigned > "9007199254740991"
        }
    }

    private fun parseWireJSONValue(source: String, cursor: JSONCursor, depth: Int) {
        if (depth > maxWireJSONDepth) invalid("JSON nesting depth")
        skipJSONWhitespace(source, cursor)
        if (cursor.index >= source.length) invalid("missing JSON value")
        when (source[cursor.index]) {
            '"' -> parseWireJSONString(source, cursor)
            '[' -> {
                cursor.index += 1
                skipJSONWhitespace(source, cursor)
                if (cursor.index < source.length && source[cursor.index] == ']') {
                    cursor.index += 1
                    return
                }
                while (true) {
                    parseWireJSONValue(source, cursor, depth + 1)
                    skipJSONWhitespace(source, cursor)
                    if (cursor.index >= source.length) invalid("unterminated JSON array")
                    if (source[cursor.index] == ']') {
                        cursor.index += 1
                        return
                    }
                    if (source[cursor.index] != ',') invalid("JSON array")
                    cursor.index += 1
                }
            }
            '{' -> {
                cursor.index += 1
                skipJSONWhitespace(source, cursor)
                if (cursor.index < source.length && source[cursor.index] == '}') {
                    cursor.index += 1
                    return
                }
                val keys = mutableSetOf<String>()
                while (true) {
                    skipJSONWhitespace(source, cursor)
                    val key = parseWireJSONString(source, cursor)
                    if (!keys.add(key)) invalid("duplicate JSON member")
                    skipJSONWhitespace(source, cursor)
                    if (cursor.index >= source.length || source[cursor.index] != ':') invalid("JSON object")
                    cursor.index += 1
                    parseWireJSONValue(source, cursor, depth + 1)
                    skipJSONWhitespace(source, cursor)
                    if (cursor.index >= source.length) invalid("unterminated JSON object")
                    if (source[cursor.index] == '}') {
                        cursor.index += 1
                        return
                    }
                    if (source[cursor.index] != ',') invalid("JSON object")
                    cursor.index += 1
                }
            }
            '-', in '0'..'9' -> {
                val start = cursor.index
                while (cursor.index < source.length && source[cursor.index] !in "\t\n\r ,]}") cursor.index += 1
                val token = source.substring(start, cursor.index)
                val number = token.toDoubleOrNull()?.takeIf { it.isFinite() } ?: invalid("JSON number")
                if (canonicalDouble(number) != token) invalid("noncanonical JSON number")
            }
            'f' -> consumeWireLiteral("false", source, cursor)
            'n' -> consumeWireLiteral("null", source, cursor)
            't' -> consumeWireLiteral("true", source, cursor)
            else -> invalid("JSON value")
        }
    }

    private fun parseWireJSONString(source: String, cursor: JSONCursor): String {
        if (cursor.index >= source.length || source[cursor.index] != '"') invalid("JSON string")
        val start = cursor.index++
        while (cursor.index < source.length) {
            if (source[cursor.index] == '\\') cursor.index += 2
            else if (source[cursor.index] == '"') {
                cursor.index += 1
                val value = try {
                    Json.parseToJsonElement(source.substring(start, cursor.index)).jsonPrimitive.content
                } catch (_: Exception) {
                    invalid("JSON string")
                }
                if (!validText(value)) invalid("JSON string")
                return value
            } else cursor.index += 1
        }
        invalid("unterminated JSON string")
    }

    private fun consumeWireLiteral(literal: String, source: String, cursor: JSONCursor) {
        if (!source.startsWith(literal, cursor.index)) invalid("JSON literal")
        cursor.index += literal.length
    }

    private fun skipJSONWhitespace(source: String, cursor: JSONCursor) {
        while (cursor.index < source.length && source[cursor.index] in "\t\n\r ") cursor.index += 1
    }

    private fun compareUTF16(left: String, right: String): Int {
        val count = minOf(left.length, right.length)
        for (index in 0 until count) {
            val comparison = left[index].code.compareTo(right[index].code)
            if (comparison != 0) return comparison
        }
        return left.length.compareTo(right.length)
    }

    private fun decodeBase64URL(source: String): ByteArray? {
        if (source.length % 4 == 1) return null
        val output = java.io.ByteArrayOutputStream(source.length * 3 / 4)
        var bits = 0
        var count = 0
        source.forEach { character ->
            val value = when (character) {
                in 'A'..'Z' -> character.code - 'A'.code
                in 'a'..'z' -> character.code - 'a'.code + 26
                in '0'..'9' -> character.code - '0'.code + 52
                '-' -> 62
                '_' -> 63
                else -> return null
            }
            bits = (bits shl 6) or value
            count += 6
            if (count >= 8) {
                count -= 8
                output.write((bits shr count) and 0xff)
            }
        }
        if (count > 0 && bits and ((1 shl count) - 1) != 0) return null
        return output.toByteArray()
    }

    private fun encodeBase64URL(source: ByteArray): String {
        val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        val output = StringBuilder((source.size * 4 + 2) / 3)
        var index = 0
        while (index < source.size) {
            val first = source[index].toInt() and 0xff
            val second = if (index + 1 < source.size) source[index + 1].toInt() and 0xff else 0
            val third = if (index + 2 < source.size) source[index + 2].toInt() and 0xff else 0
            output.append(alphabet[first shr 2])
            output.append(alphabet[(first and 0x03) shl 4 or (second shr 4)])
            if (index + 1 < source.size) output.append(alphabet[(second and 0x0f) shl 2 or (third shr 6)])
            if (index + 2 < source.size) output.append(alphabet[third and 0x3f])
            index += 3
        }
        return output.toString()
    }

    private fun compareUnsigned(left: ByteArray, right: ByteArray): Int {
        val count = minOf(left.size, right.size)
        for (index in 0 until count) {
            val comparison = (left[index].toInt() and 0xff).compareTo(right[index].toInt() and 0xff)
            if (comparison != 0) return comparison
        }
        return left.size.compareTo(right.size)
    }

    private fun decodeHex(value: String): ByteArray {
        if (value.length != 64 || value.any { it !in '0'..'9' && it !in 'a'..'f' }) invalid("SHA-256 hex")
        return ByteArray(32) { index -> value.substring(index * 2, index * 2 + 2).toInt(16).toByte() }
    }

    private fun validateRowIdentity(identity: ByteArray) {
        var position = 0
        if (!consumeExact(identity, rowIdentityDomain, position)) invalid("row identity domain")
        position += rowIdentityDomain.size
        position = consumeNonemptyText(identity, position)
        position = consumeNonemptyText(identity, position)
        if (position > identity.size - 2) invalid("row identity primary key")
        val tag = identity[position].toInt() and 0xff
        val presence = identity[position + 1].toInt() and 0xff
        position += 2
        if (presence != 1) invalid("row identity primary key presence")
        position = when (tag) {
            0x01 -> {
                val (value, next) = consumeBlob(identity, position)
                decodeUTF8(value)
                next
            }
            0x02 -> consumeFixed(identity, position, 4)
            0x03 -> consumeFixed(identity, position, 8)
            else -> invalid("row identity primary key type tag")
        }
        if (position != identity.size) invalid("row identity trailing bytes")
    }

    private fun consumeExact(input: ByteArray, expected: ByteArray, position: Int): Boolean =
        position <= input.size - expected.size &&
            input.copyOfRange(position, position + expected.size).contentEquals(expected)

    private fun consumeNonemptyText(input: ByteArray, position: Int): Int {
        val (value, next) = consumeBlob(input, position)
        if (value.isEmpty()) invalid("row identity text")
        decodeUTF8(value)
        return next
    }

    private fun consumeBlob(input: ByteArray, position: Int): Pair<ByteArray, Int> {
        if (position > input.size - 8) invalid("row identity length")
        val length = ByteBuffer.wrap(input, position, 8).order(ByteOrder.BIG_ENDIAN).long
        if (length < 0 || length > Int.MAX_VALUE || position + 8 > input.size - length.toInt()) {
            invalid("row identity value")
        }
        val start = position + 8
        val end = start + length.toInt()
        return input.copyOfRange(start, end) to end
    }

    private fun consumeFixed(input: ByteArray, position: Int, count: Int): Int {
        if (position > input.size - count) invalid("row identity primary key")
        return position + count
    }

    private fun decodeUTF8(value: ByteArray) {
        try {
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(value))
        } catch (_: Exception) {
            invalid("row identity UTF-8")
        }
    }

    private fun checksum(digest: ByteArray) = ChecksumObject("sha256", 1, "hex", hex(digest))

    // Provider resolution in MessageDigest.getInstance dominates a per-row
    // digest, and state inspection digests every retained row.
    private val sha256Digest = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }
    private fun sha256(value: ByteArray): ByteArray = sha256Digest.get().digest(value)

    // A Formatter allocation for each byte dominates hex encoding, and state
    // inspection encodes every retained row identity and checksum.
    private val hexDigits = "0123456789abcdef".toCharArray()
    internal fun hex(value: ByteArray): String {
        val output = CharArray(value.size * 2)
        for (index in value.indices) {
            val byte = value[index].toInt() and 0xff
            output[index * 2] = hexDigits[byte ushr 4]
            output[index * 2 + 1] = hexDigits[byte and 0x0f]
        }
        return String(output)
    }
    private fun bytes(block: Encoder.() -> Unit): ByteArray = Encoder().apply(block).value()
    private fun invalid(subject: String): Nothing = throw IllegalArgumentException("invalid $subject")

    private class Encoder {
        private val output = java.io.ByteArrayOutputStream()
        fun value(): ByteArray = output.toByteArray()
        fun put(value: ByteArray) { output.write(value) }
        fun byte(value: Int) { output.write(value) }
        fun u32(value: Int) { put(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array()) }
        fun i32(value: Int) = u32(value)
        fun u64(value: Long) { put(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array()) }
        fun i64(value: Long) = u64(value)
        fun blob(value: ByteArray) { u64(value.size.toLong()); put(value) }
        fun text(value: String) = blob(value.toByteArray())
    }

    private data class JSONCursor(var index: Int = 0)
}
