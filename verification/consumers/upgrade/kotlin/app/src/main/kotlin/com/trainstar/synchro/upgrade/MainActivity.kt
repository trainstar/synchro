package com.trainstar.synchro.upgrade

import android.app.Activity
import android.os.Bundle
import android.util.Base64
import com.trainstar.synchro.ColumnDef
import com.trainstar.synchro.PendingMutationInspection
import com.trainstar.synchro.SyncStatus
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

// Runs the steps that conformance/upgrade publishes for one package phase
// and reports each observation. The application uses only the public SDK.
class MainActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val controlURL = intent.getStringExtra("control_url") ?: return
        Thread {
            runBlocking {
                val config = JSONObject(request("$controlURL/config", null))
                val result = JSONObject()
                    .put("phase", config.getString("phase"))
                    .put("package_version", BuildConfig.SYNCHRO_VERSION)
                    .put("error", "")
                    .put("observations", JSONArray())
                try {
                    result.put("observations", run(config))
                } catch (failure: Throwable) {
                    result.put("error", failure.toString())
                }
                request("$controlURL/result", result.toString())
            }
        }.start()
    }

    private fun request(url: String, body: String?): String {
        val connection = URL(url).openConnection() as HttpURLConnection
        try {
            connection.connectTimeout = 30_000
            connection.readTimeout = 30_000
            if (body != null) {
                connection.requestMethod = "POST"
                connection.doOutput = true
                connection.setRequestProperty("Content-Type", "application/json")
                connection.outputStream.use { it.write(body.toByteArray()) }
            }
            check(connection.responseCode in 200..299) { "control request failed with ${connection.responseCode}" }
            return connection.inputStream.use { it.readBytes().decodeToString() }
        } finally {
            connection.disconnect()
        }
    }

    private suspend fun run(config: JSONObject): JSONArray {
        val snapshots = config.getJSONArray("snapshots")
        val steps = config.getJSONArray("steps")
        val observations = JSONArray()
        var client: SynchroClient? = null
        for (index in 0 until steps.length()) {
            val step = steps.getJSONObject(index)
            val operation = step.getString("op")
            try {
                when (operation) {
                    "open" -> client = SynchroClient(
                        SynchroConfig(
                            dbPath = step.getString("database"),
                            serverURL = config.getString("server_url"),
                            authProvider = { config.getString("token") },
                            clientID = step.getString("client_id"),
                            appVersion = config.getString("app_version"),
                            syncInterval = 3_600.0,
                            pushDebounce = 3_600.0,
                        ),
                        this,
                    )
                    "start" -> checkNotNull(client).start()
                    "sync" -> synchronize(checkNotNull(client))
                    "stop" -> checkNotNull(client).stop()
                    "close" -> {
                        checkNotNull(client).close()
                        client = null
                    }
                    "create_local_table" -> checkNotNull(client).createTable(
                        "local_notes",
                        listOf(
                            ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                            ColumnDef("body", "TEXT", nullable = false),
                        ),
                    )
                    "execute" -> {
                        val params = step.optJSONArray("params") ?: JSONArray()
                        val values = Array<Any?>(params.length()) { params.get(it).takeUnless { value -> value == JSONObject.NULL } }
                        checkNotNull(client).execute(step.getString("sql"), values)
                    }
                    "observe" -> observations.put(observe(checkNotNull(client), step.getString("name"), snapshots))
                    else -> error("unknown step $operation")
                }
            } catch (failure: Throwable) {
                throw IllegalStateException("step $index $operation failed", failure)
            }
        }
        return observations
    }

    private fun observe(client: SynchroClient, name: String, snapshots: JSONArray): JSONObject {
        val tables = JSONObject()
        for (index in 0 until snapshots.length()) {
            val snapshot = snapshots.getJSONObject(index)
            val rows = JSONArray()
            for (row in client.query(snapshot.getString("sql"))) {
                val item = JSONObject()
                for ((column, value) in row) {
                    item.put(column, if (value is ByteArray) Base64.encodeToString(value, Base64.NO_WRAP) else value ?: JSONObject.NULL)
                }
                rows.put(item)
            }
            tables.put(snapshot.getString("name"), rows)
        }
        return JSONObject()
            .put("name", name)
            .put("snapshots", tables)
            .put("pending", JSONArray(client.inspectPendingMutations().map(::inspection)))
            .put("pending_count", client.pendingChangeCount())
            .put("rejected_count", client.inspectRejectedMutations().size)
    }

    private fun inspection(mutation: PendingMutationInspection): JSONObject = JSONObject()
        .put("mutation_id", mutation.mutationID)
        .put("local_order", mutation.localOrder)
        .put("table_id", mutation.tableID)
        .put("table_name", mutation.tableName)
        .put("record_id", mutation.recordID)
        .put("primary_key_field_id", mutation.primaryKeyFieldID)
        .put("primary_key_logical_type", mutation.primaryKeyLogicalType)
        .put("operation", mutation.operation.name.lowercase())
        .put("schema_version", mutation.authoredSchema.version)
        .put("schema_hash", mutation.authoredSchema.hash)
        .put("base_version", mutation.baseVersion ?: JSONObject.NULL)
        .put("client_version", mutation.clientVersion)
        .put("status", mutation.status.name.lowercase())
        .put("source_kind", mutation.sourceKind)
        .put("depends_on_mutation_id", mutation.dependsOnMutationID ?: JSONObject.NULL)
        .put("normalized_mutation_id", mutation.normalizedMutationID ?: JSONObject.NULL)
        .put("sealed_batch_id", mutation.sealedBatchID ?: JSONObject.NULL)
        .put("sealed_ordinal", mutation.sealedOrdinal ?: JSONObject.NULL)
        .put(
            "fields",
            JSONArray(
                mutation.authoredFields.map { field ->
                    JSONObject()
                        .put("field_id", field.fieldID)
                        .put("logical_type", field.logicalType)
                        .put("value", JSONObject.wrap(field.value.value) ?: JSONObject.NULL)
                },
            ),
        )

    // syncNow requires a connected engine. A retryable failure moves the
    // engine to backoff, and the step waits for its scheduled retry.
    private suspend fun synchronize(client: SynchroClient) {
        awaitReady(client)
        try {
            client.syncNow()
        } catch (failure: Exception) {
            if (!awaitReady(client, failOnError = false)) throw failure
        }
    }

    private suspend fun awaitReady(client: SynchroClient, failOnError: Boolean = true): Boolean {
        repeat(600) {
            when (val status = client.getSyncStatus()) {
                is SyncStatus.Ready -> return true
                is SyncStatus.Error -> if (failOnError) error("sync engine entered error: ${status.failure.code}") else return false
                is SyncStatus.Stopped, is SyncStatus.Uninitialized -> if (!failOnError) return false
                else -> Unit
            }
            delay(100)
        }
        error("sync engine did not reach Ready within 60 seconds")
    }
}
