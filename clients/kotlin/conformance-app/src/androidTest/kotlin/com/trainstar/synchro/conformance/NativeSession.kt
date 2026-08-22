package com.trainstar.synchro.conformance

import android.content.Context
import android.os.Process
import android.util.Base64
import com.trainstar.synchro.AnyCodable
import com.trainstar.synchro.CheckpointInspection
import com.trainstar.synchro.PendingMutationInspection
import com.trainstar.synchro.ProvenanceInspection
import com.trainstar.synchro.RejectedMutationInspection
import com.trainstar.synchro.RebuildAttemptInspection
import com.trainstar.synchro.RebuildPageReceiptInspection
import com.trainstar.synchro.RowMetadataInspection
import com.trainstar.synchro.SchemaRef
import com.trainstar.synchro.ScopeInspection
import com.trainstar.synchro.ScopeRowInspection
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig
import com.trainstar.synchro.SyncEvent
import com.trainstar.synchro.SyncFailure
import com.trainstar.synchro.SyncStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.Closeable
import java.io.File
import java.security.MessageDigest
import java.time.Instant
import java.util.Locale
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

internal class NativeSession(private val context: Context) : Closeable {
    private val json = Json {
        ignoreUnknownKeys = false
        isLenient = false
    }
    private val callScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var client: SynchroClient? = null
    private var eventSubscription: com.trainstar.synchro.Cancellable? = null
    private val events = CopyOnWriteArrayList<JsonObject>()
    private val eventsOverflowed = AtomicBoolean()
    private var proxy: TransportMilestoneProxy? = null
    private var databaseFile: File? = null
    private val calls = ClientCallLifecycle(
        callScope,
        invokeMethod = { _, method -> dispatchMethod(requireClient(), method) },
        readStatus = { requireClient().getSyncStatus() },
    )

    fun execute(source: String): String {
        val response = try {
            require(source.toByteArray(Charsets.UTF_8).size <= MAXIMUM_COMMAND_BYTES) { "command is too large" }
            val command = json.parseToJsonElement(source).requireObject("command")
            val result = executeCommand(command)
            passedResponse(result)
        } catch (_: CaptureQueryException) {
            errorResponse("capture_query_failed")
        } catch (_: CaptureCardinalityException) {
            errorResponse("capture_row_cardinality")
        } catch (_: SerializationException) {
            errorResponse("invalid_command")
        } catch (_: IllegalArgumentException) {
            errorResponse("invalid_command")
        } catch (_: IllegalStateException) {
            errorResponse("invalid_command")
        } catch (_: Throwable) {
            errorResponse("execution_failed")
        }
        return encodeBounded(response)
    }

    override fun close() {
        proxy?.close()
        proxy = null
        callScope.cancel()
        runBlocking { calls.joinAll() }
        eventSubscription?.cancel()
        eventSubscription = null
        client?.close()
        client = null
    }

    private fun executeCommand(command: JsonObject): JsonObject {
        require(command.requiredInt("schema_version") == SCHEMA_VERSION) { "schema version is invalid" }
        return when (command.requiredString("operation")) {
            "open" -> open(command)
            "local-action" -> localAction(command)
            "begin-call" -> beginCall(command)
            "await-call" -> awaitCall(command)
            "lifecycle" -> lifecycle(command)
            "arm-transport-pause" -> armTransportPause(command)
            "await-transport-pause" -> awaitTransportPause(command)
            "resume-transport-pause" -> resumeTransportPause(command)
            "capture" -> capture(command)
            else -> throw IllegalArgumentException("operation is unsupported")
        }.withSessionObservations()
    }

    private fun open(command: JsonObject): JsonObject {
        command.requireAllowed(
            "schema_version", "operation", "database_key", "database_mode", "server_url",
            "auth_token", "client_id", "seed_database_name", "platform", "app_version",
            "pull_page_size", "transport_capacity",
        )
        check(client == null) { "client is already open" }
        val databaseKey = command.requiredString("database_key")
        require(databaseKey.isNotEmpty() && databaseKey.length <= 128 && File(databaseKey).name == databaseKey) {
            "database key is invalid"
        }
        val file = context.getDatabasePath(databaseKey)
        val databaseMode = command.requiredString("database_mode")
        when (databaseMode) {
            "create" -> context.deleteDatabase(databaseKey)
            "reuse" -> require(!file.exists()) { "seed database target already exists" }
            "existing" -> require(file.isFile) { "existing database is unavailable" }
            else -> throw IllegalArgumentException("database mode is invalid")
        }
        val upstream = command.requiredString("server_url")
        require(upstream.length <= 4_096 && (upstream.startsWith("http://") || upstream.startsWith("https://"))) {
            "server URL is invalid"
        }
        val authToken = command.requiredString("auth_token")
        val clientID = command.requiredString("client_id")
        val platform = command.optionalString("platform") ?: "android"
        val appVersion = command.optionalString("app_version") ?: "0.3.0"
        val pullPageSize = command.optionalInt("pull_page_size") ?: 100
        val seedDatabaseName = command.optionalString("seed_database_name")
        require(authToken.isNotEmpty() && authToken.length <= 16_384) { "auth token is invalid" }
        require(clientID.isNotEmpty() && clientID.length <= 128) { "client ID is invalid" }
        require(platform.isNotEmpty() && platform.length <= 128) { "platform is invalid" }
        require(appVersion.isNotEmpty() && appVersion.length <= 128) { "app version is invalid" }
        require(pullPageSize in 1..1_000) { "pull page size is invalid" }
        val seedDatabasePath = seedDatabaseName?.let { name ->
            require(name.length in 1..128 && File(name).name == name) { "seed database name is invalid" }
            File(context.filesDir, name).also { seed ->
                require(seed.isFile && !seed.isDirectory) { "seed database is unavailable" }
            }.absolutePath
        }
        require((databaseMode == "reuse") == (seedDatabasePath != null)) { "seed database mode is invalid" }
        val transportCapacity = command.optionalInt("transport_capacity") ?: DEFAULT_TRANSPORT_CAPACITY
        require(transportCapacity in 1..MAXIMUM_TRANSPORT_CAPACITY) { "transport capacity is invalid" }
        val newProxy = TransportMilestoneProxy(upstream, transportCapacity)
        val newClient = try {
            SynchroClient(
                SynchroConfig(
                    dbPath = file.absolutePath,
                    serverURL = newProxy.localURL,
                    authProvider = { authToken },
                    clientID = clientID,
                    platform = platform,
                    appVersion = appVersion,
                    syncInterval = 3_600.0,
                    pushDebounce = 3_600.0,
                    pullPageSize = pullPageSize,
                    seedDatabasePath = seedDatabasePath,
                ),
                context,
            )
        } catch (error: Throwable) {
            newProxy.close()
            throw error
        }
        databaseFile = file
        proxy = newProxy
        client = newClient
        eventSubscription = newClient.onSyncEvent { event ->
            synchronized(events) {
                if (events.size < MAXIMUM_EVENTS) {
                    events += normalizeEvent(event)
                } else {
                    eventsOverflowed.set(true)
                }
            }
        }
        return stateResult()
    }

    private fun localAction(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "local_action")
        val action = command.requiredObject("local_action")
        action.requireOnly("operation", "table_name", "primary_key_field", "primary_key", "fields")
        val operation = action.requiredString("operation")
        require(operation in setOf("insert", "update", "delete")) { "local operation is invalid" }
        val tableName = action.requiredIdentifier("table_name")
        val primaryKeyField = action.requiredIdentifier("primary_key_field")
        require(!isReservedTable(tableName)) { "reserved table is unavailable" }
        val primaryKey = decodeTypedValue(action.requiredObject("primary_key"))
        require(primaryKey != null) { "primary key is null" }
        val fieldsObject = action.requiredObject("fields")
        require(fieldsObject.size <= MAXIMUM_FIELDS) { "too many local fields" }
        val fields = fieldsObject.mapValues { (_, value) -> decodeTypedValue(value.requireObject("field value")) }.toMutableMap()
        fields.remove(primaryKeyField)?.let { supplied ->
            require(valuesEqual(supplied, primaryKey)) { "primary key values differ" }
        }
        val client = requireClient()
        val rowsAffected = when (operation) {
            "insert" -> {
                val names = fields.keys.sorted()
                val columns = (listOf(primaryKeyField) + names).joinToString(", ") { quoteIdentifier(it) }
                val placeholders = List(names.size + 1) { "?" }.joinToString(", ")
                client.transaction { transaction ->
                    transaction.execute(
                        "INSERT INTO ${quoteIdentifier(tableName)} ($columns) VALUES ($placeholders)",
                        arrayOf(primaryKey, *names.map { fields[it] }.toTypedArray()),
                    )
                }.rowsAffected
            }
            "update" -> {
                require(fields.isNotEmpty()) { "update fields are empty" }
                val names = fields.keys.sorted()
                val assignments = names.joinToString(", ") { "${quoteIdentifier(it)} = ?" }
                client.transaction { transaction ->
                    transaction.execute(
                        "UPDATE ${quoteIdentifier(tableName)} SET $assignments WHERE ${quoteIdentifier(primaryKeyField)} = ?",
                        arrayOf(*names.map { fields[it] }.toTypedArray(), primaryKey),
                    )
                }.rowsAffected
            }
            else -> {
                require(fields.isEmpty()) { "delete fields are not empty" }
                client.transaction { transaction ->
                    transaction.execute(
                        "DELETE FROM ${quoteIdentifier(tableName)} WHERE ${quoteIdentifier(primaryKeyField)} = ?",
                        arrayOf(primaryKey),
                    )
                }.rowsAffected
            }
        }
        check(rowsAffected == 1) { "local action affected an unexpected row count" }
        return stateResult(rowsAffected)
    }

    private fun beginCall(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "call_id", "method")
        val observation = calls.begin("current", command.requiredCallID(), command.requiredString("method"))
        return callResult(observation)
    }

    private fun awaitCall(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "call_id")
        val observation = runBlocking { calls.await("current", command.requiredCallID()) }
        return callResult(observation)
    }

    private fun lifecycle(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "lifecycle_operation")
        val client = requireClient()
        when (command.requiredString("lifecycle_operation")) {
            "stop" -> runBlocking { client.stop() }
            "enter-background" -> client.onApplicationBackground()
            "enter-foreground" -> client.onApplicationForeground()
            else -> throw IllegalArgumentException("lifecycle operation is invalid")
        }
        return stateResult()
    }

    private fun armTransportPause(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "transport_operation")
        requireProxy().arm(command.requiredString("transport_operation"))
        return stateResult()
    }

    private fun awaitTransportPause(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "transport_operation")
        requireProxy().await(command.requiredString("transport_operation"))
        return stateResult()
    }

    private fun resumeTransportPause(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation")
        requireProxy().resume()
        return stateResult()
    }

    private fun capture(command: JsonObject): JsonObject {
        command.requireOnly("schema_version", "operation", "row_selectors")
        val selectors = command.optionalArray("row_selectors") ?: JsonArray(emptyList())
        require(selectors.size <= MAXIMUM_SELECTORS) { "too many row selectors" }
        val rows = buildJsonArray {
            selectors.forEach { value ->
                val selector = value.requireObject("row selector")
                selector.requireOnly("table_name", "primary_key_field", "primary_key")
                val table = selector.requiredIdentifier("table_name")
                val field = selector.requiredIdentifier("primary_key_field")
                require(!isReservedTable(table)) { "reserved table is unavailable" }
                val primaryKey = decodeTypedValue(selector.requiredObject("primary_key"))
                require(primaryKey != null) { "primary key is null" }
                val selected = try {
                    requireClient().query(
                        "SELECT * FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(field)} = ?",
                        arrayOf(primaryKey),
                    )
                } catch (_: Throwable) {
                    throw CaptureQueryException()
                }
                if (selected.size > 1) throw CaptureCardinalityException()
                selected.firstOrNull()?.let { add(normalizeRow(it)) }
            }
        }
        require(rows.size <= MAXIMUM_ROWS) { "too many captured rows" }
        return stateResult(applicationRows = rows)
    }

    private fun stateResult(
        rowsAffected: Int? = null,
        applicationRows: JsonArray? = null,
    ): JsonObject {
        val client = requireClient()
        val pending = client.inspectPendingMutations()
        val rejected = client.inspectRejectedMutations()
        val schema = client.inspectSchema()
        val scopes = client.inspectScopes()
        val scopeRows = client.inspectScopeRows()
        val metadata = client.inspectRowMetadata()
        val checkpoints = client.inspectCheckpoints()
        val provenance = client.inspectProvenance()
        val rebuild = client.inspectRebuildState()
        require(pending.size <= MAXIMUM_RECORDS && rejected.size <= MAXIMUM_RECORDS) { "inspection output is too large" }
        require(scopes.size <= MAXIMUM_RECORDS && scopeRows.size <= MAXIMUM_RECORDS) { "inspection output is too large" }
        require(metadata.size <= MAXIMUM_RECORDS && checkpoints.size <= MAXIMUM_RECORDS) { "inspection output is too large" }
        require(provenance.size <= MAXIMUM_RECORDS && rebuild.attempts.size <= MAXIMUM_RECORDS) { "inspection output is too large" }
        require(rebuild.receipts.size <= MAXIMUM_RECORDS) { "inspection output is too large" }
        require(pending.all { it.authoredFields.size <= MAXIMUM_FIELDS }) { "inspection fields are too large" }
        return buildJsonObject {
            put("status", client.getSyncStatus().state.wireName)
            rowsAffected?.let { put("rows_affected", it) }
            put("pending_change_count", client.pendingChangeCount())
            applicationRows?.let { put("application_rows", it) }
            put("retained_mutations", normalizePending(pending))
            put("rejected_mutations", normalizeRejected(rejected))
            schema.currentSchema?.let { put("schema", normalizeSchema(it)) }
            put("scope_states", normalizeScopes(scopes))
            put("scope_rows", normalizeScopeRows(scopeRows))
            put("row_metadata", normalizeRowMetadata(metadata))
            put("checkpoints", normalizeCheckpoints(checkpoints))
            put("provenance", normalizeProvenance(provenance))
            put("rebuild_attempts", normalizeRebuildAttempts(rebuild.attempts))
            put("rebuild_receipts", normalizeRebuildReceipts(rebuild.receipts))
            put("events", JsonArray(events.toList()))
            put("events_overflowed", eventsOverflowed.get())
            blockingFailure(client.getSyncStatus())?.let { put("failure", normalizeFailure(it)) }
        }
    }

    private fun JsonObject.withSessionObservations(): JsonObject = buildJsonObject {
        this@withSessionObservations.forEach { (key, value) -> put(key, value) }
        put("process_id", Process.myPid().toString())
        databaseFile?.let { put("database_identity_fingerprint", databaseIdentityFingerprint()) }
        proxy?.let { value ->
            put("transport_milestone", value.milestoneObservation())
            put("transport_observations", value.observationSnapshot())
        }
    }

    private suspend fun dispatchMethod(client: SynchroClient, method: String) {
        when (method) {
            "start" -> client.start()
            "sync-now" -> client.syncNow()
            "retry-after-error" -> client.retry()
            "reset-schema-and-start" -> client.resetSchema()
            else -> throw IllegalArgumentException("synchronization method is invalid")
        }
    }

    private fun callResult(value: ClientCallObservation): JsonObject = buildJsonObject {
        put("call_id", value.callID)
        put("state", value.state)
        value.completion?.let { put("completion", it.wireName) }
        value.errorCategory?.let { put("call_error_category", it) }
        client?.let { put("status", it.getSyncStatus().state.wireName) }
    }

    private fun normalizePending(values: List<PendingMutationInspection>): JsonArray = buildJsonArray {
        values.sortedBy { it.localOrder }.forEach { value ->
            add(buildJsonObject {
                put("mutation_id", value.mutationID)
                put("local_order", value.localOrder)
                put("table_id", value.tableID)
                put("table_name", value.tableName)
                put("record_id", value.recordID)
                put("primary_key_field_id", value.primaryKeyFieldID)
                put("primary_key_logical_type", value.primaryKeyLogicalType)
                put("operation", value.operation.name.lowercase(Locale.US))
                put("authored_schema", normalizeSchema(value.authoredSchema))
                put("base_version", value.baseVersion?.let(::JsonPrimitive) ?: JsonNull)
                put("client_version", value.clientVersion)
                put("status", value.status.name.lowercase(Locale.US))
                put("authored_fields", buildJsonArray {
                    value.authoredFields.sortedBy { it.fieldID }.forEach { field ->
                        add(buildJsonObject {
                            put("field_id", field.fieldID)
                            put("logical_type", field.logicalType)
                            put("value", normalizeValue(field.value))
                        })
                    }
                })
            })
        }
    }

    private fun normalizeRejected(values: List<RejectedMutationInspection>): JsonArray = buildJsonArray {
        values.sortedBy { it.mutationID }.forEach { value ->
            add(buildJsonObject {
                put("mutation_id", value.mutationID)
                put("table_name", value.tableName)
                put("record_id", value.recordID)
                put("status", value.status.name.lowercase(Locale.US))
                put("code", value.code.name.lowercase(Locale.US))
                put("message", value.message?.let(::JsonPrimitive) ?: JsonNull)
                put("server_row", value.serverRowJSON?.let(::parseJSON) ?: JsonNull)
                put("server_version", value.serverVersion?.let(::JsonPrimitive) ?: JsonNull)
                put("mutation", parseJSON(value.mutationJSON))
                put("rejection", parseJSON(value.rejectionJSON))
                put("created_at", value.createdAt)
                put("updated_at", value.updatedAt)
            })
        }
    }

    private fun normalizeScopes(values: List<ScopeInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("scope_id", value.scopeID)
                put("cursor", value.cursor?.let(::JsonPrimitive) ?: JsonNull)
                put("checksum", value.checksum?.let(::JsonPrimitive) ?: JsonNull)
                put("generation", value.generation)
                put("local_checksum", value.localChecksum)
            })
        }
    }

    private fun normalizeScopeRows(values: List<ScopeRowInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("scope_id", value.scopeID)
                put("table_name", value.tableName)
                put("record_id", value.recordID)
                put("checksum", value.checksum)
                put("generation", value.generation)
            })
        }
    }

    private fun normalizeRowMetadata(values: List<RowMetadataInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("table_name", value.tableName)
                put("record_id", value.recordID)
                put("server_version", value.serverVersion)
                put("row_checksum", value.rowChecksumJSON?.let(::JsonPrimitive) ?: JsonNull)
            })
        }
    }

    private fun normalizeCheckpoints(values: List<CheckpointInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("scope_id", value.scopeID)
                put("cursor", value.cursor?.let(::JsonPrimitive) ?: JsonNull)
                put("checksum", value.checksum?.let(::JsonPrimitive) ?: JsonNull)
                put("local_checksum", value.localChecksum)
            })
        }
    }

    private fun normalizeProvenance(values: List<ProvenanceInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("table_name", value.tableName)
                put("record_id", value.recordID)
                put("scope_ids", buildJsonArray { value.scopeIDs.forEach { add(JsonPrimitive(it)) } })
                put("server_version", value.serverVersion?.let(::JsonPrimitive) ?: JsonNull)
            })
        }
    }

    private fun normalizeRebuildAttempts(values: List<RebuildAttemptInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("scope_id", value.scopeID)
                put("rebuild_id", value.rebuildID)
                put("client_generation", value.clientGeneration)
                put("schema_version", value.schema.version)
                put("schema_hash", value.schema.hash)
                put("generation", value.generation)
                put("cursor", value.cursor?.let(::JsonPrimitive) ?: JsonNull)
                put("page_limit", value.pageLimit)
            })
        }
    }

    private fun normalizeRebuildReceipts(values: List<RebuildPageReceiptInspection>): JsonArray = buildJsonArray {
        values.forEach { value ->
            add(buildJsonObject {
                put("scope_id", value.scopeID)
                put("rebuild_id", value.rebuildID)
                put("request_cursor", value.requestCursor?.let(::JsonPrimitive) ?: JsonNull)
                put("is_final", value.isFinal)
                put("final_scope_cursor", value.finalScopeCursor?.let(::JsonPrimitive) ?: JsonNull)
                put("final_checksum", value.finalChecksumJSON?.let(::parseJSON) ?: JsonNull)
            })
        }
    }

    private fun normalizeEvent(event: SyncEvent): JsonObject = buildJsonObject {
        when (event) {
            is SyncEvent.StateChanged -> {
                put("type", "state_changed")
                put("status", event.change.to.wireName)
            }
            is SyncEvent.Backoff -> put("type", "backoff")
            is SyncEvent.SchemaApplying -> {
                put("type", "schema_applying")
                put("source_schema", normalizeSchema(event.schema.source))
                put("target_schema", normalizeSchema(event.schema.target))
                put("schema_action", event.schema.action.name.lowercase(Locale.US))
            }
            is SyncEvent.SchemaApplied -> {
                put("type", "schema_applied")
                put("source_schema", normalizeSchema(event.schema.source))
                put("target_schema", normalizeSchema(event.schema.target))
                put("schema_action", event.schema.action.name.lowercase(Locale.US))
            }
            is SyncEvent.MutationAccepted -> {
                put("type", "mutation_accepted")
                put("mutation_id", event.mutation.mutationID)
                put("table_id", event.mutation.tableID)
            }
            is SyncEvent.MutationRejected -> {
                put("type", "mutation_rejected")
                put("mutation_id", event.mutation.mutationID)
                put("table_id", event.mutation.tableID)
                event.mutation.rejectionCode?.let { put("rejection_code", it.name.lowercase(Locale.US)) }
            }
            is SyncEvent.RebuildRequested -> {
                put("type", "rebuild_requested")
                put("scope_id", event.rebuild.scopeID)
                put("rebuild_id", event.rebuild.rebuildID)
            }
            is SyncEvent.RebuildCompleted -> {
                put("type", "rebuild_completed")
                put("scope_id", event.rebuild.scopeID)
                put("rebuild_id", event.rebuild.rebuildID)
            }
            is SyncEvent.Failure -> {
                put("type", "failure")
                put("failure", normalizeFailure(event.failure))
            }
        }
    }

    private fun normalizeFailure(value: SyncFailure): JsonObject = buildJsonObject {
        put("operation", value.operation.wireName)
        put("code", value.code.wireName)
        put("retryable", value.retryable)
        put("message", value.message)
        put("recoveryAction", value.recoveryAction.wireName)
        put("metadata", buildJsonObject {
            value.metadata.toSortedMap().forEach { (key, metadataValue) -> put(key, metadataValue) }
        })
    }

    private fun normalizeSchema(value: SchemaRef): JsonObject = buildJsonObject {
        put("version", value.version)
        put("hash", value.hash)
    }

    private fun normalizeRow(row: Map<String, Any?>): JsonObject {
        require(row.size <= MAXIMUM_FIELDS) { "captured row is too wide" }
        return buildJsonObject {
            row.toSortedMap().forEach { (key, value) -> put(key, normalizeValue(value)) }
        }
    }

    private fun normalizeValue(value: Any?): JsonElement = when (value) {
        null -> JsonNull
        is AnyCodable -> normalizeValue(value.value)
        is Boolean -> JsonPrimitive(value)
        is ByteArray -> {
            require(value.size <= MAXIMUM_VALUE_BYTES) { "captured blob is too large" }
            JsonPrimitive(Base64.encodeToString(value, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING))
        }
        is Byte, is Short, is Int, is Long -> JsonPrimitive((value as Number).toLong())
        is Float, is Double -> {
            val number = (value as Number).toDouble()
            require(number.isFinite()) { "captured number is not finite" }
            JsonPrimitive(number)
        }
        is Number -> JsonPrimitive(value.toString())
        is String -> {
            require(value.toByteArray(Charsets.UTF_8).size <= MAXIMUM_VALUE_BYTES) { "captured text is too large" }
            JsonPrimitive(value)
        }
        is Instant -> JsonPrimitive(value.toString())
        else -> throw IllegalArgumentException("captured value is unsupported")
    }

    private fun decodeTypedValue(value: JsonObject): Any? {
        value.requireOnly("type", "value")
        return when (value.requiredString("type")) {
            "null" -> {
                require(value["value"] is JsonNull) { "typed null is invalid" }
                null
            }
            "string" -> value.requiredPrimitive("value").content
            "boolean" -> value.requiredPrimitive("value").boolean
            "integer" -> value.requiredPrimitive("value").long
            "double" -> value.requiredPrimitive("value").double.also {
                require(it.isFinite()) { "typed double is not finite" }
            }
            "bytes" -> decodeBase64URL(value.requiredPrimitive("value").content)
            else -> throw IllegalArgumentException("typed value is unsupported")
        }
    }

    private fun decodeBase64URL(value: String): ByteArray {
        require(value.length <= MAXIMUM_ENCODED_VALUE_BYTES && value.none { it == '=' }) { "base64url is invalid" }
        require(value.all { it.isLetterOrDigit() || it == '-' || it == '_' }) { "base64url is invalid" }
        val decoded = Base64.decode(value, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
        require(Base64.encodeToString(decoded, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING) == value) {
            "base64url is not canonical"
        }
        return decoded
    }

    private fun databaseIdentityFingerprint(): String {
        val path = databaseFile?.canonicalPath ?: throw IllegalStateException("database is not open")
        val digest = MessageDigest.getInstance("SHA-256")
            .digest("synchro-android-database-v1\u0000$path".toByteArray(Charsets.UTF_8))
        return digest.joinToString("") { "%02x".format(Locale.US, it.toInt() and 0xff) }
    }

    private fun requireClient(): SynchroClient = client ?: throw IllegalStateException("client is not open")
    private fun requireProxy(): TransportMilestoneProxy = proxy ?: throw IllegalStateException("transport is not open")

    private fun passedResponse(result: JsonObject): JsonObject = buildJsonObject {
        put("schema_version", SCHEMA_VERSION)
        put("outcome", "passed")
        put("result", result)
        put("error_code", JsonNull)
    }

    private fun errorResponse(code: String): JsonObject = buildJsonObject {
        put("schema_version", SCHEMA_VERSION)
        put("outcome", "error")
        put("result", JsonNull)
        put("error_code", code)
    }

    private fun encodeBounded(response: JsonObject): String {
        val encoded = json.encodeToString(JsonObject.serializer(), response)
        if (encoded.toByteArray(Charsets.UTF_8).size <= MAXIMUM_RESPONSE_BYTES) return encoded
        return json.encodeToString(JsonObject.serializer(), errorResponse("execution_failed"))
    }

    private fun parseJSON(value: String): JsonElement = try {
        json.parseToJsonElement(value)
    } catch (_: SerializationException) {
        throw IllegalArgumentException("retained JSON is invalid")
    }

    private fun blockingFailure(status: SyncStatus): SyncFailure? = (status as? SyncStatus.Error)?.failure

    private fun quoteIdentifier(value: String): String = "\"$value\""

    private fun isReservedTable(value: String): Boolean {
        val normalized = value.lowercase(Locale.US)
        return normalized.startsWith("_synchro_") || normalized.startsWith("sqlite_")
    }

    private fun valuesEqual(left: Any?, right: Any?): Boolean = when {
        left is ByteArray && right is ByteArray -> left.contentEquals(right)
        else -> left == right
    }

    private class CaptureQueryException : RuntimeException()
    private class CaptureCardinalityException : RuntimeException()

    internal companion object {
        const val SCHEMA_VERSION = 1
        const val MAXIMUM_COMMAND_BYTES = 1 shl 20
        const val MAXIMUM_RESPONSE_BYTES = 1 shl 20
        const val DEFAULT_TRANSPORT_CAPACITY = 512
        const val MAXIMUM_TRANSPORT_CAPACITY = 4_096
        const val MAXIMUM_EVENTS = 512
        const val MAXIMUM_RECORDS = 512
        const val MAXIMUM_FIELDS = 256
        const val MAXIMUM_SELECTORS = 128
        const val MAXIMUM_ROWS = 256
        const val MAXIMUM_VALUE_BYTES = 1 shl 20
        const val MAXIMUM_ENCODED_VALUE_BYTES = 1_398_102
        val CALL_ID = Regex("[a-z][a-z0-9_-]{0,127}")
        val IDENTIFIER = Regex("[A-Za-z_][A-Za-z0-9_]{0,127}")
    }
}

/*
 * The host sends one JSON object per line with schema_version and operation.
 * open requires database_key, database_mode, server_url, auth_token, and client_id.
 * open permits seed_database_name, platform, app_version, pull_page_size, and transport_capacity.
 * local-action requires local_action with operation, table_name, primary_key_field, primary_key, and fields.
 * begin-call requires call_id and method. await-call requires call_id.
 * lifecycle requires lifecycle_operation. Transport arm and await require transport_operation.
 * Transport resume has no operation-specific members.
 * capture requires row_selectors. Typed values contain exactly type and value.
 * Android transforms database_key and database_mode into one application-private database path.
 */

private fun JsonElement.requireObject(name: String): JsonObject =
    this as? JsonObject ?: throw IllegalArgumentException("$name must be an object")

private fun JsonObject.requireOnly(vararg names: String) {
    require(keys == names.toSet()) { "object members are invalid" }
}

private fun JsonObject.requireAllowed(vararg names: String) {
    require(keys.all { it in names }) { "object members are invalid" }
}

private fun JsonObject.requiredObject(name: String): JsonObject =
    this[name]?.requireObject(name) ?: throw IllegalArgumentException("$name is required")

private fun JsonObject.requiredPrimitive(name: String): JsonPrimitive =
    this[name] as? JsonPrimitive ?: throw IllegalArgumentException("$name is required")

private fun JsonObject.requiredString(name: String): String =
    requiredPrimitive(name).takeIf { it.isString }?.content
        ?: throw IllegalArgumentException("$name must be a string")

private fun JsonObject.optionalString(name: String): String? {
    val value = this[name] ?: return null
    if (value is JsonNull) return null
    return (value as? JsonPrimitive)?.takeIf { it.isString }?.content
        ?: throw IllegalArgumentException("$name must be a string")
}

private fun JsonObject.requiredInt(name: String): Int =
    requiredPrimitive(name).takeIf { !it.isString }?.content?.toIntOrNull()
        ?: throw IllegalArgumentException("$name must be an integer")

private fun JsonObject.optionalInt(name: String): Int? {
    val value = this[name] ?: return null
    if (value is JsonNull) return null
    return (value as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toIntOrNull()
        ?: throw IllegalArgumentException("$name must be an integer")
}

private fun JsonObject.optionalArray(name: String): JsonArray? {
    val value = this[name] ?: return null
    if (value is JsonNull) return null
    return value as? JsonArray ?: throw IllegalArgumentException("$name must be an array")
}

private fun JsonObject.requiredIdentifier(name: String): String =
    requiredString(name).also { require(NativeSession.IDENTIFIER.matches(it)) { "$name is invalid" } }

private fun JsonObject.requiredCallID(): String =
    requiredString("call_id").also { require(NativeSession.CALL_ID.matches(it)) { "call ID is invalid" } }
