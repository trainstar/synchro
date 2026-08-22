package com.trainstar.synchro

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
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
        assertEquals(1, response.scopes.add.size)
        assertTrue(response.scopeCursorUpdates.containsKey("exercises_public"))
        assertEquals(null, response.scopeCursorUpdates["exercises_public"])
        val existingScopes = mapOf("exercises_public" to ScopeCursorRef("historical-cursor"))
        response.validate(existingScopes, 12)
        assertTrue(runCatching {
            response.copy(scopeCursorUpdates = emptyMap()).validate(existingScopes, 12)
        }.isFailure)
    }

    @Test
    fun testConnectRejectsStaleAndRehashedInvalidManifest() {
        val response = decodeFixtureValue<ConnectResponse>(
            "conformance/protocol/connect-rebuild-local.json",
            listOf("expected", "response")
        )
        val manifest = requireNotNull(response.schemaDefinition)
        val stale = manifest.copy(tables = manifest.tables.toMutableList().also { tables ->
            tables[0] = tables[0].copy(name = "changed")
        })
        assertTrue(runCatching { response.copy(schemaDefinition = stale).validate() }.isFailure)

        val invalid = manifest.copy(compatibilityFloor = 1)
        val invalidHash = Integrity.schemaManifestHash(invalid)
        assertTrue(runCatching {
            response.copy(
                schema = response.schema.copy(hash = invalidHash),
                schemaDefinition = invalid.copy(schemaHash = invalidHash),
            ).validate()
        }.isFailure)
    }

    @Test
    fun testConnectUnsupportedFixtureDecodesAndValidates() {
        val response = decodeFixtureValue<ConnectResponse>(
            "conformance/protocol/connect-unsupported.json",
            listOf("expected", "response")
        )

        assertEquals(SchemaAction.UNSUPPORTED, response.schema.action)
        assertEquals(null, response.schemaDefinition)
        assertTrue(response.scopes.add.isEmpty())
        response.validate()
    }

    @Test
    fun testConnectRejectsProtocolMismatch() {
        val response = decodeFixtureValue<ConnectResponse>(
            "conformance/protocol/connect-none.json",
            listOf("expected", "response")
        ).copy(protocolVersion = 2)
        assertTrue(runCatching { response.validate() }.isFailure)
    }

    @Test
    fun testConnectRejectsRegressingOrUnadvancedAssignmentVersion() {
        val response = decodeFixtureValue<ConnectResponse>(
            "conformance/protocol/connect-none.json",
            listOf("expected", "response")
        )
        val existingScopes = mapOf(
            "workouts_user:u_123" to ScopeCursorRef("cursor-a"),
            "exercises_public" to ScopeCursorRef("cursor-b"),
        )
        response.validate(existingScopes, 13)

        assertTrue(runCatching {
            response.copy(scopeSetVersion = 12).validate(existingScopes, 13)
        }.isFailure)
        assertTrue(runCatching {
            response.copy(
                scopes = ScopeAssignmentDelta(
                    add = listOf(ScopeAssignment("new-scope", null)),
                    remove = emptyList(),
                )
            ).validate(existingScopes, 13)
        }.isFailure)
    }

    @Test
    fun testPushRejectsBatchAndOutcomeIdentityMismatch() {
        val schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH)
        val requestMutation = Mutation(
            mutationID = "00000000-0000-5000-8000-000000000001",
            table = "table-orders",
            op = Operation.INSERT,
            pk = JsonObject(mapOf("field-id" to JsonPrimitive("r1"))),
            authoredSchema = schema,
            clientVersion = "2026-01-01T00:00:00.000000Z",
            columns = JsonObject(mapOf("field-title" to JsonPrimitive("Title"))),
        )
        val request = PushRequest(
            clientID = "client-1",
            clientGeneration = 1,
            batchID = "00000000-0000-5000-8000-000000000002",
            schema = schema,
            mutations = listOf(requestMutation),
        )
        val outcome = AcceptedMutation(
            mutationID = requestMutation.mutationID,
            table = requestMutation.table,
            pk = requestMutation.pk,
            outcomeSchema = schema,
            status = MutationStatus.APPLIED,
            serverRow = JsonObject(
                mapOf("field-id" to JsonPrimitive("r1"), "field-title" to JsonPrimitive("Title"))
            ),
            rowChecksum = validChecksum,
            serverVersion = "opaque-version",
        )
        var response = PushResponse(
            batchID = "00000000-0000-5000-8000-000000000003",
            serverTime = "2026-01-01T00:00:00.000000Z",
            accepted = listOf(outcome),
            rejected = emptyList(),
        )
        assertTrue(runCatching { response.validate(request) }.isFailure)

        response = response.copy(batchID = request.batchID, accepted = listOf(outcome.copy(table = "table-other")))
        assertTrue(runCatching { response.validate(request) }.isFailure)
    }

    @Test
    fun testPushOutcomeShapeUsesRequestedOperation() {
        val row = JsonObject(
            mapOf("field-id" to JsonPrimitive("r1"), "field-title" to JsonPrimitive("Title"))
        )
        listOf(Operation.INSERT, Operation.UPDATE).forEach { operation ->
            val request = makePushRequest(operation)
            val valid = AcceptedMutation(
                mutationID = request.mutations[0].mutationID,
                table = request.mutations[0].table,
                pk = request.mutations[0].pk,
                outcomeSchema = request.schema,
                status = MutationStatus.APPLIED,
                serverRow = row,
                rowChecksum = validChecksum,
                serverVersion = "server-version",
            )
            PushResponse(
                request.batchID,
                "2026-01-01T00:00:00.000000Z",
                listOf(valid),
                emptyList(),
            ).validate(request)
            assertTrue(
                runCatching {
                    PushResponse(
                        request.batchID,
                        "2026-01-01T00:00:00.000000Z",
                        listOf(valid.copy(serverRow = null, rowChecksum = null)),
                        emptyList(),
                    ).validate(request)
                }.isFailure
            )
        }

        val deleteRequest = makePushRequest(Operation.DELETE)
        val deleteOutcome = AcceptedMutation(
            mutationID = deleteRequest.mutations[0].mutationID,
            table = deleteRequest.mutations[0].table,
            pk = deleteRequest.mutations[0].pk,
            outcomeSchema = deleteRequest.schema,
            status = MutationStatus.APPLIED,
            serverVersion = "delete-fence",
        )
        PushResponse(
            deleteRequest.batchID,
            "2026-01-01T00:00:00.000000Z",
            listOf(deleteOutcome),
            emptyList(),
        ).validate(deleteRequest)
        assertTrue(
            runCatching {
                PushResponse(
                    deleteRequest.batchID,
                    "2026-01-01T00:00:00.000000Z",
                    listOf(deleteOutcome.copy(serverRow = row)),
                    emptyList(),
                ).validate(deleteRequest)
            }.isFailure
        )
    }

    @Test
    fun testPushRejectedOutcomeShapeAndCodeMatchStatus() {
        val request = makePushRequest(Operation.UPDATE)
        val mutation = request.mutations[0]
        var conflict = RejectedMutation(
            mutationID = mutation.mutationID,
            table = mutation.table,
            pk = mutation.pk,
            outcomeSchema = request.schema,
            status = MutationStatus.CONFLICT,
            code = MutationRejectionCode.VERSION_CONFLICT,
            message = "conflict",
        )
        rejectedResponse(conflict, request).validate(request)
        conflict = conflict.copy(serverVersion = "fence-version")
        rejectedResponse(conflict, request).validate(request)

        val row = JsonObject(
            mapOf("field-id" to JsonPrimitive("r1"), "field-title" to JsonPrimitive("server"))
        )
        assertTrue(
            runCatching { rejectedResponse(conflict.copy(serverRow = row, rowChecksum = null), request).validate(request) }.isFailure
        )
        assertTrue(
            runCatching {
                rejectedResponse(
                    conflict.copy(serverRow = row, rowChecksum = validChecksum, serverVersion = null),
                    request,
                ).validate(request)
            }.isFailure
        )
        assertTrue(
            runCatching {
                rejectedResponse(
                    conflict.copy(code = MutationRejectionCode.POLICY_REJECTED, serverVersion = null),
                    request,
                ).validate(request)
            }.isFailure
        )

        val terminal = conflict.copy(
            status = MutationStatus.REJECTED_TERMINAL,
            code = MutationRejectionCode.POLICY_REJECTED,
            serverVersion = null,
        )
        rejectedResponse(terminal, request).validate(request)
        assertTrue(
            runCatching {
                rejectedResponse(terminal.copy(code = MutationRejectionCode.VERSION_CONFLICT), request).validate(request)
            }.isFailure
        )
        assertTrue(
            runCatching {
                rejectedResponse(terminal.copy(serverVersion = "not-permitted"), request).validate(request)
            }.isFailure
        )
    }

    @Test
    fun testSchemaIncompatibleDeleteAllowsEmptyFieldIDs() {
        val deleteRequest = makePushRequest(Operation.DELETE)
        val deleteMutation = deleteRequest.mutations.single()
        val deleteOutcome = RejectedMutation(
            mutationID = deleteMutation.mutationID,
            table = deleteMutation.table,
            pk = deleteMutation.pk,
            outcomeSchema = deleteRequest.schema,
            status = MutationStatus.REJECTED_TERMINAL,
            code = MutationRejectionCode.SCHEMA_INCOMPATIBLE,
            message = "table removed",
            retryable = false,
            authoredSchema = deleteMutation.authoredSchema,
            currentSchema = deleteRequest.schema,
            incompatibleFieldIDs = emptyList(),
        )
        rejectedResponse(deleteOutcome, deleteRequest).validate(deleteRequest)

        val updateRequest = makePushRequest(Operation.UPDATE)
        val updateMutation = updateRequest.mutations.single()
        val updateOutcome = deleteOutcome.copy(
            mutationID = updateMutation.mutationID,
            table = updateMutation.table,
            pk = updateMutation.pk,
            outcomeSchema = updateRequest.schema,
            authoredSchema = updateMutation.authoredSchema,
            currentSchema = updateRequest.schema,
        )
        assertTrue(runCatching { rejectedResponse(updateOutcome, updateRequest).validate(updateRequest) }.isFailure)
    }

    @Test
    fun testPullRequiredChecksumsFixtureDecodesAndValidates() {
        val response = decodeFixtureValue<PullResponse>(
            "conformance/protocol/pull-required-checksums.json",
            listOf("expected", "response")
        )

        assertEquals(13L, response.scopeSetVersion)
        assertEquals("workouts_user_u_123_890.sig", response.scopeCursors["workouts_user:u_123"])
        response.validate()
        response.validate(setOf("workouts_user:u_123"), 13)
        assertTrue(runCatching {
            response.copy(scopeSetVersion = 12).validate(setOf("workouts_user:u_123"), 13)
        }.isFailure)
    }

    @Test
    fun testPullValidationRejectsInvalidOperationAndScopeBindings() {
        val scopeID = "scope-a"
        val checksum = ChecksumObject("sha256", 1, "hex", "0".repeat(64))
        val change = ChangeRecord(
            scope = scopeID,
            table = "items",
            op = Operation.INSERT,
            pk = buildJsonObject { put("id", "row-a") },
            serverVersion = "server-version",
        )
        val invalidOperation = PullResponse(
            changes = listOf(change),
            scopeSetVersion = 1,
            scopeCursors = mapOf(scopeID to "cursor-a"),
            scopeUpdates = ScopeAssignmentDelta(emptyList(), emptyList()),
            rebuild = emptyList(),
            hasMore = false,
            checksums = mapOf(scopeID to checksum),
        )
        assertTrue(runCatching { invalidOperation.validate(setOf(scopeID)) }.isFailure)

        val invalidScope = invalidOperation.copy(
            changes = listOf(change.copy(scope = "scope-b", op = Operation.DELETE))
        )
        assertTrue(runCatching { invalidScope.validate(setOf(scopeID)) }.isFailure)

        val invalidAssignment = invalidOperation.copy(
            changes = emptyList(),
            scopeUpdates = ScopeAssignmentDelta(
                add = listOf(ScopeAssignment("scope-b", "forged-cursor")),
                remove = emptyList(),
            ),
            checksums = mapOf(scopeID to checksum, "scope-b" to checksum),
        )
        assertTrue(runCatching { invalidAssignment.validate(setOf(scopeID)) }.isFailure)
    }

    @Test
    fun testRebuildFixturePagesDecodeAndValidate() {
        val request = decodeFixtureValue<RebuildRequest>(
            "conformance/scopes/rebuild-single-scope.json",
            listOf("input", "request")
        )
        val pages = decodeFixtureValue<List<RebuildResponse>>(
            "conformance/scopes/rebuild-single-scope.json",
            listOf("expected", "pages")
        )

        assertEquals(2, pages.size)
        assertTrue(pages[1].isFinalPage())
        pages[0].validate(request)
        pages[1].validate(request)
        assertTrue(runCatching { pages[0].copy(scope = "another-scope").validate(request) }.isFailure)
    }

    @Test
    fun testPortableSchemaManifestFixtureDecodesAndValidates() {
        val manifest = decodeFixtureValue<SchemaManifest>(
            "conformance/schema/schema-manifest-portable.json",
            listOf("manifest")
        )

        assertEquals(2, manifest.tables.size)
        assertEquals(CompositionClass.MULTI_SCOPE, manifest.tables[1].composition)
        assertEquals("fld_workouts_updated_at", manifest.tables[0].lifecycle.updatedAtFieldID)
        assertEquals("fld_workouts_deleted_at", manifest.tables[0].lifecycle.deletedAtFieldID)
        manifest.validate()
    }

    @Test
    fun testPortableSchemaManifestRejectsSemanticMutants() {
        val manifest = decodeFixtureValue<SchemaManifest>(
            "conformance/schema/schema-manifest-portable.json",
            listOf("manifest")
        )
        val firstTable = manifest.tables[0]
        fun withFirstTable(table: TableSchema) = manifest.copy(
            tables = manifest.tables.toMutableList().also { it[0] = table }
        )

        val decimal = withFirstTable(firstTable.copy(
            fields = firstTable.fields.toMutableList().also { fields ->
                fields[1] = fields[1].copy(typeName = "decimal")
            }
        ))
        val primaryKey = withFirstTable(firstTable.copy(
            fields = firstTable.fields.toMutableList().also { fields ->
                fields[0] = fields[0].copy(writable = true)
            }
        ))
        val lifecycle = withFirstTable(firstTable.copy(
            fields = firstTable.fields.toMutableList().also { fields ->
                fields[2] = fields[2].copy(typeName = "string")
            }
        ))
        val relationIdentity = manifest.copy(
            tables = manifest.tables.toMutableList().also { tables ->
                tables[1] = tables[1].copy(relationID = tables[0].relationID)
            }
        )
        val index = withFirstTable(firstTable.copy(
            indexes = listOf(firstTable.indexes[0].copy(fieldIDs = emptyList()))
        ))

        listOf(decimal, primaryKey, lifecycle, relationIdentity, index).forEach { mutant ->
            assertTrue(runCatching { mutant.validate() }.isFailure)
        }
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

        val allowed = setOf("string", "int", "int64", "decimal", "float", "boolean", "datetime", "date", "time", "json", "bytes")
        val emittedTypes = manifest.tables.flatMap { it.fields }.map { it.typeName }.toSet()

        assertTrue(emittedTypes.isNotEmpty())
        assertTrue("fixture emitted non-canonical portable types: ${emittedTypes - allowed}", emittedTypes.subtract(allowed).isEmpty())
    }

    @Test
    fun testPortableSchemaManifestRejectsUnknownFieldType() {
        val manifest = decodeFixtureValue<SchemaManifest>(
            "conformance/schema/schema-manifest-portable.json",
            listOf("manifest")
        )
        val table = manifest.tables[0]
        val invalid = manifest.copy(
            tables = manifest.tables.toMutableList().also { tables ->
                tables[0] = table.copy(
                    fields = table.fields.toMutableList().also { fields ->
                        fields[0] = fields[0].copy(typeName = "uuid")
                    }
                )
            }
        )

        assertTrue(runCatching { invalid.localTables() }.isFailure)
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

    private val validChecksum: ChecksumObject
        get() = ChecksumObject("sha256", 1, "hex", "a".repeat(64))

    private fun makePushRequest(operation: Operation): PushRequest {
        val schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH)
        val mutation = Mutation(
            mutationID = "00000000-0000-5000-8000-000000000001",
            table = "table-orders",
            op = operation,
            pk = JsonObject(mapOf("field-id" to JsonPrimitive("r1"))),
            authoredSchema = schema,
            baseVersion = if (operation == Operation.INSERT) null else "base-version",
            clientVersion = "2026-01-01T00:00:00.000000Z",
            columns = if (operation == Operation.DELETE) {
                null
            } else {
                JsonObject(mapOf("field-title" to JsonPrimitive("Title")))
            },
        )
        return PushRequest(
            clientID = "client-1",
            clientGeneration = 1,
            batchID = "00000000-0000-5000-8000-000000000002",
            schema = schema,
            mutations = listOf(mutation),
        )
    }

    private fun rejectedResponse(outcome: RejectedMutation, request: PushRequest): PushResponse =
        PushResponse(
            batchID = request.batchID,
            serverTime = "2026-01-01T00:00:00.000000Z",
            accepted = emptyList(),
            rejected = listOf(outcome),
        )

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
