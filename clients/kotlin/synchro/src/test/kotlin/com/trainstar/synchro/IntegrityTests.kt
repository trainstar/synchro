package com.trainstar.synchro

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.nio.file.Files
import java.nio.file.Paths
import java.security.MessageDigest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class IntegrityTests {
    @Test
    fun schemaManifestHashMatchesAuthoredContract() {
        val expected = "5dc97fc5ea571dd7555d877e08cecc102113c6efd63976d37d498341c8b32d51"
        val manifest = SchemaManifest(
            schemaVersion = 42,
            schemaHash = expected,
            parentSchema = SchemaRef(
                41,
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            ),
            transitionClass = "class_3",
            compatibilityFloor = 42,
            tables = listOf(
                TableSchema(
                    tableID = "tbl_documents",
                    relationID = "rel_documents",
                    name = "documents",
                    primaryKeyFieldID = "fld_documents_id",
                    lifecycle = LifecycleSchema(),
                    composition = CompositionClass.MULTI_SCOPE,
                    fields = listOf(
                        ColumnSchema(
                            fieldID = "fld_documents_id",
                            name = "id",
                            typeName = "string",
                            nullable = false,
                            writable = false,
                        ),
                        ColumnSchema(
                            fieldID = "fld_documents_amount",
                            name = "amount",
                            typeName = "decimal",
                            nullable = false,
                            writable = true,
                            precision = 18,
                            scale = 4,
                        ),
                    ),
                    indexes = listOf(
                        IndexSchema(
                            indexID = "idx_documents_amount",
                            name = "idx_documents_amount",
                            fieldIDs = listOf("fld_documents_amount"),
                            unique = false,
                        )
                    ),
                )
            ),
        )

        assertEquals(expected, Integrity.schemaManifestHash(manifest))
        assertEquals(
            false,
            Integrity.schemaManifestHash(
                manifest.copy(tables = listOf(manifest.tables.single().copy(name = "changed_documents")))
            ) == expected,
        )
    }

    @Test
    fun canonicalWireJsonValidation() {
        Integrity.validateCanonicalWireJSON("{\"b\":1,\"a\":0.000001,\"text\":\"\\ufdd0\"}")
        listOf(
            "{\"value\":-0}",
            "{\"value\":1.0}",
            "{\"value\":1e-6}",
            "{\"value\":1,\"value\":1}",
        ).forEach { source ->
            assertThrows(IllegalArgumentException::class.java) {
                Integrity.validateCanonicalWireJSON(source)
            }
        }
        val deep = "[".repeat(130) + "0" + "]".repeat(130)
        assertThrows(IllegalArgumentException::class.java) {
            Integrity.validateCanonicalWireJSON(deep)
        }
        assertThrows(IllegalArgumentException::class.java) {
            Integrity.decodeCanonicalWireJSON(byteArrayOf(0x22, 0xc3.toByte(), 0x28, 0x22))
        }
        Integrity.validateCanonicalWireJSON("{\"float\":9007199254740992}")
    }

    @Test
    fun decimalCapacityAndUnicodeDomains() {
        val decimal = LocalSchemaColumn(
            fieldID = "field-decimal",
            name = "decimal_value",
            logicalType = "decimal",
            nullable = false,
            writable = true,
            precision = 6,
            scale = 2,
            sqliteDefaultSQL = null,
            isPrimaryKey = false,
        )
        Integrity.encodedTypedValue("\"1234.56\"", decimal)
        assertThrows(IllegalArgumentException::class.java) {
            Integrity.encodedTypedValue("\"123456\"", decimal)
        }

        val pk = buildJsonObject { put("field-id", "row-1") }
        val textRow = buildJsonObject {
            put("field-id", "row-1")
            put("field-json", "{\"n\":0}")
            put("field-text", "\uFDD0")
        }
        assertEquals(64, Integrity.rowDigest("b".repeat(64), canonicalTable, pk, textRow, "version-1").checksum.digest.length)

        val jsonRow = buildJsonObject {
            put("field-id", "row-1")
            put("field-json", "{\"value\":\"\uFDD0\"}")
            put("field-text", "valid")
        }
        assertEquals(64, Integrity.rowDigest("b".repeat(64), canonicalTable, pk, jsonRow, "version-1").checksum.digest.length)
    }

    @Test
    fun rowDigestUsesCanonicalAndSafeJsonNumbers() {
        val pk = buildJsonObject { put("field-id", "row-1") }
        listOf(
            "{\"n\":0.000001}",
            "{\"n\":1e-7}",
        ).forEach { source ->
            val row = buildJsonObject {
                put("field-id", "row-1")
                put("field-json", source)
                put("field-text", "valid")
            }
            assertEquals(
                64,
                Integrity.rowDigest("b".repeat(64), canonicalTable, pk, row, "version-1").checksum.digest.length
            )
        }

        listOf(
            "{\"n\":1e-6}",
            "{\"n\":9007199254740992}",
            "{\"n\":100000000000000000000}",
            "{\"n\":1e+21}",
        ).forEach { source ->
            val row = buildJsonObject {
                put("field-id", "row-1")
                put("field-json", source)
                put("field-text", "valid")
            }
            assertThrows(IllegalArgumentException::class.java) {
                Integrity.rowDigest("b".repeat(64), canonicalTable, pk, row, "version-1")
            }
        }
    }

    @Test
    fun rowDigestRejectsUnpairedSurrogate() {
        val pk = buildJsonObject { put("field-id", "row-1") }
        val row = buildJsonObject {
            put("field-id", "row-1")
            put("field-json", "{\"n\":0}")
            put("field-text", String(charArrayOf('\uD800')))
        }

        assertThrows(IllegalArgumentException::class.java) {
            Integrity.rowDigest("b".repeat(64), canonicalTable, pk, row, "version-1")
        }
    }

    @Test
    fun rowDigestRejectsUnpairedSurrogateInJsonString() {
        val pk = buildJsonObject { put("field-id", "row-1") }
        val row = buildJsonObject {
            put("field-id", "row-1")
            put("field-json", "{\"value\":\"\\ud800\"}")
            put("field-text", "valid")
        }

        assertThrows(IllegalArgumentException::class.java) {
            Integrity.rowDigest("b".repeat(64), canonicalTable, pk, row, "version-1")
        }
    }

    @Test
    fun authoredChecksumVectors() {
        val root = generateSequence(Paths.get("").toAbsolutePath().normalize()) { it.parent }
            .take(8)
            .first { Files.exists(it.resolve("conformance/vectors/canonical-v1.json")) }
        val document = Json.parseToJsonElement(
            String(Files.readAllBytes(root.resolve("conformance/vectors/canonical-v1.json")), Charsets.UTF_8)
        ).jsonObject
        val checksumKinds = setOf("typed_value", "row_identity", "row_digest", "scope_digest")
        var executed = 0

        document.getValue("vectors").jsonArray.forEach { element ->
            val vector = element.jsonObject
            val kind = vector.getValue("kind").jsonPrimitive.content
            if (kind !in checksumKinds) return@forEach
            executed += 1
            val valid = vector.getValue("valid").jsonPrimitive.content.toBooleanStrict()
            val vectorID = vector.getValue("vector_id").jsonPrimitive.content
            val result = runCatching { executeVector(vector, kind) }
            if (valid) {
                if (result.isFailure) {
                    throw AssertionError("valid authored vector failed: $vectorID", result.exceptionOrNull())
                }
                val output = result.getOrThrow()
                val expected = vector.getValue("expected").jsonObject
                assertEquals(vectorID, expected.getValue("canonical_bytes_hex").jsonPrimitive.content, output.preimage.lowerHex())
                assertEquals(
                    vectorID,
                    expected.getValue("expected_bytes_sha256").jsonPrimitive.content,
                    MessageDigest.getInstance("SHA-256").digest(output.preimage).lowerHex(),
                )
                val expectedDigest = expected["expected_sha256"]?.takeUnless { it is kotlinx.serialization.json.JsonNull }?.jsonPrimitive?.content
                assertEquals(vectorID, expectedDigest, output.digest)
            } else if (result.isSuccess) {
                throw AssertionError("invalid authored vector was accepted: $vectorID")
            }
        }

        assertEquals(90, executed)
    }

    @Test
    fun rowDigestRejectsInvalidTemporalValues() {
        val pk = buildJsonObject { put("field-id", "row-1") }
        val valid = buildJsonObject {
            put("field-id", "row-1")
            put("field-datetime", "2024-02-29T12:34:56.123456Z")
            put("field-date", "2024-02-29")
            put("field-time", "23:59:59.999999")
        }

        assertEquals(
            64,
            Integrity.rowDigest("b".repeat(64), temporalTable, pk, valid, "version-1").checksum.digest.length
        )

        val invalidValues = mapOf(
            "field-datetime" to "2023-02-29T12:34:56.123456Z",
            "field-date" to "2023-02-29",
            "field-time" to "24:00:00.000000",
        )
        invalidValues.forEach { (field, value) ->
            val row = JsonObject(valid.toMutableMap().apply { put(field, kotlinx.serialization.json.JsonPrimitive(value)) })
            assertThrows(IllegalArgumentException::class.java) {
                Integrity.rowDigest("b".repeat(64), temporalTable, pk, row, "version-1")
            }
        }
    }

    @Test
    fun scopeDigestRejectsMalformedRowIdentities() {
        val identity = Integrity.rowIdentity(table, buildJsonObject { put("field-id", 7) })
        val digest = ChecksumObject("sha256", 1, "hex", "a".repeat(64))

        assertEquals(
            64,
            Integrity.scopeDigest("b".repeat(64), "scope-1", listOf(identity to digest)).digest.length
        )

        val invalidTag = identity.copyOf().also { it[it.size - 6] = 0xff.toByte() }
        val invalidUTF8 = identity.copyOf().also {
            val tableTextStart = "synchro:v3:row-identity:v1\u0000".toByteArray().size + 8
            it[tableTextStart] = 0xff.toByte()
        }
        val malformed = listOf(
            invalidTag,
            invalidUTF8,
            identity.copyOf(identity.size - 1),
            identity + byteArrayOf(0),
        )
        malformed.forEach { value ->
            assertThrows(IllegalArgumentException::class.java) {
                Integrity.scopeDigest("b".repeat(64), "scope-1", listOf(value to digest))
            }
        }
    }

    private val table = LocalSchemaTable(
        tableID = "table-1",
        relationID = "relation-1",
        tableName = "records",
        primaryKeyFieldID = "field-id",
        createdAtFieldID = null,
        updatedAtFieldID = null,
        deletedAtFieldID = null,
        updatedAtColumn = "",
        deletedAtColumn = "",
        composition = CompositionClass.SINGLE_SCOPE,
        primaryKey = listOf("id"),
        columns = listOf(
            LocalSchemaColumn(
                fieldID = "field-id",
                name = "id",
                logicalType = "int",
                nullable = false,
                writable = false,
                precision = null,
                scale = null,
                sqliteDefaultSQL = null,
                isPrimaryKey = true,
            )
        ),
    )

    private val temporalTable = LocalSchemaTable(
        tableID = "table-temporal",
        relationID = "relation-temporal",
        tableName = "temporal_records",
        primaryKeyFieldID = "field-id",
        createdAtFieldID = null,
        updatedAtFieldID = null,
        deletedAtFieldID = null,
        updatedAtColumn = "",
        deletedAtColumn = "",
        composition = CompositionClass.SINGLE_SCOPE,
        primaryKey = listOf("id"),
        columns = listOf(
            column("field-id", "id", "string", false),
            column("field-datetime", "datetime_value", "datetime", false),
            column("field-date", "date_value", "date", false),
            column("field-time", "time_value", "time", false),
        ),
    )

    private val canonicalTable = LocalSchemaTable(
        tableID = "table-canonical",
        relationID = "relation-canonical",
        tableName = "canonical_records",
        primaryKeyFieldID = "field-id",
        createdAtFieldID = null,
        updatedAtFieldID = null,
        deletedAtFieldID = null,
        updatedAtColumn = "",
        deletedAtColumn = "",
        composition = CompositionClass.SINGLE_SCOPE,
        primaryKey = listOf("id"),
        columns = listOf(
            column("field-id", "id", "string", false),
            column("field-json", "json_value", "json", false),
            column("field-text", "text_value", "string", false),
        ),
    )

    private fun column(fieldID: String, name: String, type: String, nullable: Boolean) =
        LocalSchemaColumn(
            fieldID = fieldID,
            name = name,
            logicalType = type,
            nullable = nullable,
            writable = fieldID != "field-id",
            precision = null,
            scale = null,
            sqliteDefaultSQL = null,
            isPrimaryKey = fieldID == "field-id",
        )

    private fun executeVector(vector: JsonObject, kind: String): VectorExecution {
        val input = vector.getValue("input").jsonObject
        if (kind == "typed_value") {
            val spec = input.getValue("field_spec").jsonObject
            val field = LocalSchemaColumn(
                fieldID = "vector-field",
                name = "vector_value",
                logicalType = spec.getValue("type").jsonPrimitive.content,
                nullable = spec.getValue("nullable").jsonPrimitive.content.toBooleanStrict(),
                writable = true,
                precision = spec["precision"]?.jsonPrimitive?.content?.toInt(),
                scale = spec["scale"]?.jsonPrimitive?.content?.toInt(),
                sqliteDefaultSQL = null,
                isPrimaryKey = false,
            )
            val source = input.getValue("raw_json").jsonPrimitive.content
            return VectorExecution(Integrity.encodedTypedValue(source, field), null)
        }

        if (kind == "row_digest") {
            val manifest = Json.decodeFromString<SchemaManifest>(input.getValue("manifest_json").jsonPrimitive.content)
            val tableID = input.getValue("table_id").jsonPrimitive.content
            val table = manifest.localTables().single { it.tableID == tableID }
            val pkValue = Json.parseToJsonElement(input.getValue("pk_json").jsonPrimitive.content)
            val pk = JsonObject(mapOf(table.primaryKeyFieldID to pkValue))
            val row = parseStrictObject(input.getValue("row_json").jsonPrimitive.content)
            val serverVersion = input.getValue("server_version").jsonPrimitive.content
            val preimage = Integrity.rowDigestPreimage(
                manifest.schemaHash,
                table,
                pk,
                row,
                serverVersion,
            ).second
            val digest = Integrity.rowDigest(manifest.schemaHash, table, pk, row, serverVersion).checksum.digest
            return VectorExecution(preimage, digest)
        }

        if (kind == "row_identity") {
            val manifest = Json.decodeFromString<SchemaManifest>(input.getValue("manifest_json").jsonPrimitive.content)
            val tableID = input.getValue("table_id").jsonPrimitive.content
            val table = manifest.localTables().single { it.tableID == tableID }
            val pkValue = Json.parseToJsonElement(input.getValue("pk_json").jsonPrimitive.content)
            val pk = JsonObject(mapOf(table.primaryKeyFieldID to pkValue))
            return VectorExecution(Integrity.rowIdentity(table, pk), null)
        }

        val entries = input.getValue("entries").jsonArray.map { element ->
            val entry = element.jsonObject
            entry.getValue("row_identity_hex").jsonPrimitive.content.decodeLowerHex() to
                ChecksumObject(
                    "sha256",
                    1,
                    "hex",
                    entry.getValue("row_digest_hex").jsonPrimitive.content,
                )
        }
        val preimage = Integrity.scopeDigestPreimage(
            input.getValue("schema_hash").jsonPrimitive.content,
            input.getValue("scope_id").jsonPrimitive.content,
            entries,
        )
        val digest = Integrity.scopeDigest(
            input.getValue("schema_hash").jsonPrimitive.content,
            input.getValue("scope_id").jsonPrimitive.content,
            entries,
        ).digest
        return VectorExecution(preimage, digest)
    }

    private fun parseStrictObject(source: String): JsonObject {
        rejectDuplicateTopLevelKeys(source)
        return Json.parseToJsonElement(source).jsonObject
    }

    private fun rejectDuplicateTopLevelKeys(source: String) {
        val cursor = Cursor()
        skipWhitespace(source, cursor)
        require(cursor.index < source.length && source[cursor.index] == '{') { "row is not an object" }
        cursor.index += 1
        val keys = mutableSetOf<String>()
        while (true) {
            skipWhitespace(source, cursor)
            if (cursor.index < source.length && source[cursor.index] == '}') return
            val token = consumeString(source, cursor)
            val key = Json.parseToJsonElement(token).jsonPrimitive.content
            require(keys.add(key)) { "duplicate row field" }
            skipWhitespace(source, cursor)
            require(cursor.index < source.length && source[cursor.index] == ':') { "invalid row object" }
            cursor.index += 1
            consumeValue(source, cursor)
            skipWhitespace(source, cursor)
            if (cursor.index < source.length && source[cursor.index] == ',') {
                cursor.index += 1
                continue
            }
            require(cursor.index < source.length && source[cursor.index] == '}') { "invalid row object" }
            return
        }
    }

    private fun consumeString(source: String, cursor: Cursor): String {
        require(cursor.index < source.length && source[cursor.index] == '"') { "invalid JSON string" }
        val start = cursor.index++
        while (cursor.index < source.length) {
            if (source[cursor.index] == '\\') cursor.index += 2
            else if (source[cursor.index] == '"') {
                cursor.index += 1
                return source.substring(start, cursor.index)
            } else cursor.index += 1
        }
        error("unterminated JSON string")
    }

    private fun consumeValue(source: String, cursor: Cursor) {
        skipWhitespace(source, cursor)
        require(cursor.index < source.length) { "missing JSON value" }
        if (source[cursor.index] == '"') {
            consumeString(source, cursor)
            return
        }
        if (source[cursor.index] == '{' || source[cursor.index] == '[') {
            val closers = mutableListOf(if (source[cursor.index] == '{') '}' else ']')
            cursor.index += 1
            while (cursor.index < source.length && closers.isNotEmpty()) {
                when (source[cursor.index]) {
                    '"' -> consumeString(source, cursor)
                    '{' -> { closers.add('}'); cursor.index += 1 }
                    '[' -> { closers.add(']'); cursor.index += 1 }
                    closers.last() -> { closers.removeAt(closers.lastIndex); cursor.index += 1 }
                    else -> cursor.index += 1
                }
            }
            require(closers.isEmpty()) { "unterminated JSON value" }
            return
        }
        while (cursor.index < source.length && source[cursor.index] != ',' && source[cursor.index] != '}') {
            cursor.index += 1
        }
    }

    private fun skipWhitespace(source: String, cursor: Cursor) {
        while (cursor.index < source.length && source[cursor.index].isWhitespace()) cursor.index += 1
    }

    private fun String.decodeLowerHex(): ByteArray {
        require(length % 2 == 0 && all { it in '0'..'9' || it in 'a'..'f' }) { "invalid lowercase hex" }
        return ByteArray(length / 2) { index -> substring(index * 2, index * 2 + 2).toInt(16).toByte() }
    }

    private fun ByteArray.lowerHex(): String = joinToString("") { "%02x".format(it.toInt() and 0xff) }

    private data class Cursor(var index: Int = 0)
    private data class VectorExecution(val preimage: ByteArray, val digest: String?)
}
