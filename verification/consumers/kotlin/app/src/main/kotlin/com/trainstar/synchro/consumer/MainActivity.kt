package com.trainstar.synchro.consumer

import android.app.Activity
import android.os.Bundle
import com.trainstar.synchro.ColumnDef
import com.trainstar.synchro.RetryableError
import com.trainstar.synchro.SyncStatus
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import java.io.File

class MainActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val smokeConfig = File(filesDir, "packaged-smoke-config.json")
        if (smokeConfig.isFile) {
            Thread {
                runBlocking {
                    runPackagedSmoke(smokeConfig)
                }
            }.start()
            return
        }

        val client = SynchroClient(
            SynchroConfig(
                dbPath = "consumer.db",
                serverURL = "http://127.0.0.1",
                authProvider = { "unused" },
                clientID = "00000000-0000-4000-8000-000000000001",
                appVersion = "consumer"
            ),
            this
        )
        client.createTable(
            "consumer_probe",
            listOf(
                ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                ColumnDef("value", "TEXT", nullable = false),
            ),
        )
        client.close()
    }

    private suspend fun runPackagedSmoke(configFile: File) {
        val config = JSONObject(configFile.readText())
        check(config.getInt("schema_version") == 1)
        val phase = config.getString("phase")
        check(phase == "initial" || phase == "resume")
        val client = SynchroClient(
            SynchroConfig(
                dbPath = "consumer.db",
                serverURL = config.getString("server_url"),
                authProvider = { config.getString("token") },
                clientID = config.getString("client_id"),
                // The application version, not the package version. The test
                // adapter gates clients below MIN_CLIENT_VERSION 1.0.0.
                appVersion = "1.0.0",
                syncInterval = 3_600.0,
                pushDebounce = 3_600.0,
                maxRetryAttempts = 1,
            ),
            this,
        )

        val observeSQL = config.getString("observe_sql")
        if (phase == "initial") {
            runAndWaitForScheduledPullRetry(client) {
                client.start()
            }
            // start() can return before the first cycle applies the server
            // schema, and the dataset inserts require that schema. The
            // public status reaches Ready when the schema is applied.
            awaitReadyStatus(client)
            statements(config, "initial_sql").forEach { client.execute(it) }
            awaitConvergence(client, observeSQL)
            val durableSQL = statements(config, "durable_sql")
            durableSQL.forEach { client.execute(it) }
            val pending = client.pendingChangeCount()
            check(pending == durableSQL.size)
            writePhaseResult(phase, pending, observe(client, observeSQL))
            return
        }

        val pendingBeforeResume = client.pendingChangeCount()
        check(pendingBeforeResume > 0)
        runAndWaitForScheduledPullRetry(client) {
            client.start()
        }
        // syncNow before the engine publishes connectionReady throws, so the
        // resume waits for the public Ready status first.
        awaitReadyStatus(client)
        // The harness authors remote rows while this process is dead. Only
        // ordinary synchronization can deliver them to the local query path.
        awaitConvergence(client, observeSQL)
        val pendingAfterResume = client.pendingChangeCount()
        val observed = observe(client, observeSQL)
        client.stop()
        client.close()
        writePhaseResult(phase, pendingAfterResume, observed)
    }

    private fun statements(config: JSONObject, name: String): List<String> {
        val values = config.getJSONArray(name)
        return (0 until values.length()).map { values.getString(it) }
    }

    // Waits until the queue is empty and the pulled server total equals the
    // sum of the local sets. Only a server rollup and a pull can make them equal.
    private suspend fun awaitConvergence(client: SynchroClient, observeSQL: String) {
        val deadline = System.nanoTime() + 90_000_000_000L
        while (true) {
            runAndWaitForScheduledPullRetry(client) {
                client.syncNow()
            }
            if (client.pendingChangeCount() == 0 && client.queryOne(observeSQL)?.get("converged") == "1") {
                return
            }
            check(System.nanoTime() < deadline) { "client did not converge within 90 seconds" }
            delay(500)
        }
    }

    // Reports each observation column as the text of the value that the
    // public query path returned. An INTEGER value arrives as a Long.
    private fun observe(client: SynchroClient, observeSQL: String): JSONObject {
        val row = checkNotNull(client.queryOne(observeSQL))
        val observed = JSONObject()
        for ((field, value) in row) {
            if (field == "converged") continue
            observed.put(
                field,
                when (value) {
                    is String -> value
                    is Long -> value.toString()
                    else -> error("observation $field has an unexpected value type")
                },
            )
        }
        return observed
    }

    private suspend fun runAndWaitForScheduledPullRetry(
        client: SynchroClient,
        operation: suspend () -> Unit,
    ) {
        try {
            operation()
            return
        } catch (failure: RetryableError) {
            if (failure.interruptedOperation != "pulling") {
                throw failure
            }
            val backoff = client.getSyncStatus() as? SyncStatus.Backoff
            if (backoff?.operation != "pulling") {
                throw failure
            }
            repeat(300) {
                when (client.getSyncStatus()) {
                    is SyncStatus.Ready -> return
                    is SyncStatus.Error,
                    is SyncStatus.Stopped,
                    is SyncStatus.Uninitialized -> throw failure
                    else -> delay(100)
                }
            }
            throw IllegalStateException(
                "scheduled pull retry did not return to Ready within 30 seconds",
                failure,
            )
        }
    }

    private suspend fun awaitReadyStatus(client: SynchroClient) {
        // A bounded wait on the public status. The initial cycle applies the
        // server schema before the engine reports Ready.
        repeat(600) {
            when (val status = client.getSyncStatus()) {
                is SyncStatus.Ready -> return
                is SyncStatus.Error -> error("sync engine entered error: ${status.failure.code}")
                else -> delay(100)
            }
        }
        error("sync engine did not reach Ready within 60 seconds")
    }

    private fun writePhaseResult(phase: String, pendingCount: Int, observed: JSONObject) {
        val result = JSONObject()
            .put("schema_version", 1)
            .put("phase", phase)
            .put("status", "passed")
            .put("pid", android.os.Process.myPid())
            .put("pending_change_count", pendingCount)
            .put("observed", observed)
        val destination = File(filesDir, "$phase-result.json")
        val temporary = File(filesDir, ".$phase-result.json.tmp")
        temporary.writeText(result.toString())
        check(temporary.renameTo(destination))
    }
}
