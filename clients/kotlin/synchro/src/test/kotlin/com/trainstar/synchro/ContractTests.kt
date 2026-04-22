package com.trainstar.synchro

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class ContractTests {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun testConnectNoneFixtureDecodesAndValidates() {
        val response = decodeFixtureValue<ConnectResponse>(
            "conformance/protocol/connect-none.json",
            listOf("expected", "response")
        )

        assertEquals(SchemaAction.NONE, response.schema.action)
        assertEquals(13L, response.scopeSetVersion)
        assertEquals(null, response.schemaDefinition)
        response.validate()
    }

    @Test
    fun testConnectRebuildLocalFixtureDecodesAndValidates() {
        val response = decodeFixtureValue<ConnectResponse>(
            "conformance/protocol/connect-rebuild-local.json",
            listOf("expected", "response")
        )

        assertEquals(SchemaAction.REBUILD_LOCAL, response.schema.action)
        assertNotNull(response.schemaDefinition)
        assertEquals(2, response.scopes.add.size)
        response.validate()
    }

    @Test
    fun testPullRequiredChecksumsFixtureDecodesAndValidates() {
        val response = decodeFixtureValue<PullResponse>(
            "conformance/protocol/pull-required-checksums.json",
            listOf("expected", "response")
        )

        assertEquals(13L, response.scopeSetVersion)
        assertEquals("v2.workouts_user_u_123_890.sig", response.scopeCursors["workouts_user:u_123"])
        response.validate()
    }

    @Test
    fun testRebuildFixturePagesDecodeAndValidate() {
        val pages = decodeFixtureValue<List<RebuildResponse>>(
            "conformance/scopes/rebuild-single-scope.json",
            listOf("expected", "pages")
        )

        assertEquals(2, pages.size)
        assertTrue(pages[1].isFinalPage())
        pages[0].validate()
        pages[1].validate()
    }

    @Test
    fun testPortableSchemaManifestFixtureDecodesAndValidates() {
        val manifest = decodeFixtureValue<SchemaManifest>(
            "conformance/schema/schema-manifest-portable.json",
            listOf("manifest")
        )

        assertEquals(2, manifest.tables.size)
        assertEquals(CompositionClass.MULTI_SCOPE, manifest.tables[1].composition)
        assertEquals("updated_at", manifest.tables[0].updatedAtColumn)
        assertEquals("deleted_at", manifest.tables[0].deletedAtColumn)
        manifest.validate()
    }

    @Test
    fun testPortableSchemaManifestConvertsToLocalSchemaTables() {
        val manifest = decodeFixtureValue<SchemaManifest>(
            "conformance/schema/schema-manifest-portable.json",
            listOf("manifest")
        )

        val tables = manifest.localTables()

        assertEquals(2, tables.size)
        assertEquals("workouts", tables[0].tableName)
        assertEquals(listOf("id"), tables[0].primaryKey)
        assertEquals("updated_at", tables[0].updatedAtColumn)
        assertEquals("deleted_at", tables[0].deletedAtColumn)
        assertTrue(tables[0].columns.any { it.name == "id" && it.isPrimaryKey })
        assertTrue(tables[1].columns.any { it.name == "user_id" && !it.isPrimaryKey })
    }

    @Test
    fun testPortableSchemaManifestFixtureUsesCanonicalTypeNames() {
        val manifest = decodeFixtureValue<SchemaManifest>(
            "conformance/schema/schema-manifest-portable.json",
            listOf("manifest")
        )

        val allowed = setOf("string", "int", "int64", "float", "boolean", "datetime", "date", "time", "json", "bytes")
        val emittedTypes = manifest.tables.flatMap { it.columns.orEmpty() }.map { it.typeName }.toSet()

        assertTrue(emittedTypes.isNotEmpty())
        assertTrue("fixture emitted non-canonical portable types: ${emittedTypes - allowed}", emittedTypes.subtract(allowed).isEmpty())
    }

    @Test
    fun testSQLiteSchemaNormalizesPatternAliases() {
        assertEquals("float", SQLiteSchema.normalizedLogicalType("numeric(5,1)"))
        assertEquals("string", SQLiteSchema.normalizedLogicalType("varchar(255)"))
        assertEquals("json", SQLiteSchema.normalizedLogicalType("text[]"))
        assertEquals("string", SQLiteSchema.normalizedLogicalType("interval"))
        assertEquals("string", SQLiteSchema.normalizedLogicalType("int4range"))
    }

    @Test
    fun testUpgradeRequiredErrorFixtureDecodes() {
        val response = decodeFixtureValue<ErrorResponse>(
            "conformance/protocol/error-upgrade-required.json",
            listOf("expected", "response")
        )

        assertEquals(ProtocolErrorCode.UPGRADE_REQUIRED, response.error.code)
        assertFalse(response.error.retryable)
    }

    private inline fun <reified T> decodeFixtureValue(path: String, jsonPath: List<String>): T {
        val root = json.parseToJsonElement(String(Files.readAllBytes(findFixture(path))))
        val nested = valueAt(root, jsonPath)
        return json.decodeFromString(nested.toString())
    }

    private fun valueAt(root: JsonElement, jsonPath: List<String>): JsonElement {
        var current = root
        for (key in jsonPath) {
            current = (current as JsonObject)[key]
                ?: error("missing json path component $key")
        }
        return current
    }

    private fun findFixture(relativePath: String): Path {
        var current: Path? = Paths.get("").toAbsolutePath().normalize()
        repeat(8) {
            val candidate = current!!.resolve(relativePath).normalize()
            if (Files.exists(candidate)) {
                return candidate
            }
            current = current!!.parent
        }
        error("fixture not found: $relativePath from ${Paths.get("").toAbsolutePath()}")
    }
}
