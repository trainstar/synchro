@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro.consumer

import android.app.Activity
import android.os.Bundle
import com.trainstar.synchro.ColumnDef
import com.trainstar.synchro.SyncStatus
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig
import com.trainstar.synchro.inspection.TransportObservationCollector
import com.trainstar.synchro.inspection.TransportOperationClass
import com.trainstar.synchro.inspection.withTransportObservation
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import java.io.File
import java.time.Instant

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
        val orderID = config.getString("order_id")
        val transportCollector = TransportObservationCollector(capacity = 256)
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
            ).withTransportObservation(transportCollector),
            this,
        )

        if (phase == "initial") {
            client.start()
            // start() can return before the first cycle applies the server
            // schema, and the customers insert requires that schema. The
            // public status reaches Ready when the schema is applied.
            awaitReadyStatus(client)
            val timestamp = Instant.now().toString()
            client.execute(
                "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
                arrayOf(
                    config.getString("customer_id"),
                    config.getString("user_id"),
                    "Packaged Consumer",
                    timestamp,
                    timestamp,
                ),
            )
            client.execute(
                "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
                arrayOf(
                    orderID,
                    config.getString("customer_id"),
                    config.getString("user_id"),
                    """{"street":"Packaged Initial"}""",
                    timestamp,
                    timestamp,
                ),
            )
            client.syncNow()
            check(client.pendingChangeCount() == 0)
            val snapshot = transportCollector.snapshot()
            check(!snapshot.overflowed)
            val observations = snapshot.observations
            check(
                listOf(
                    TransportOperationClass.CONNECT,
                    TransportOperationClass.PUSH,
                    TransportOperationClass.PULL,
                ).all { operation ->
                    observations.any { it.operationClass == operation && it.statusCode in 200..299 }
                },
            )
            client.execute(
                "UPDATE orders SET ship_address = ?, updated_at = ? WHERE id = ?",
                arrayOf("""{"street":"Packaged Durable"}""", Instant.now().toString(), orderID),
            )
            val pending = client.pendingChangeCount()
            check(pending == 1)
            writePhaseResult(phase, pending)
            return
        }

        val durable = client.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            arrayOf(orderID),
        )?.get("ship_address")
        val pendingBeforeResume = client.pendingChangeCount()
        check(durable == """{"street":"Packaged Durable"}""" && pendingBeforeResume > 0)
        client.start()
        // syncNow before the engine publishes connectionReady throws, so the
        // resume waits for the public Ready status first.
        awaitReadyStatus(client)
        client.syncNow()
        val pendingAfterResume = client.pendingChangeCount()
        check(pendingAfterResume == 0)
        writePhaseResult(phase, pendingAfterResume)
        client.stop()
        client.close()
    }

    private fun awaitReadyStatus(client: SynchroClient) {
        // A bounded wait on the public status. The initial cycle applies the
        // server schema before the engine reports Ready.
        repeat(600) {
            when (val status = client.getSyncStatus()) {
                is SyncStatus.Ready -> return
                is SyncStatus.Error -> error("sync engine entered error: ${'$'}{status.failure.code}")
                else -> Thread.sleep(100)
            }
        }
        error("sync engine did not reach Ready within 60 seconds")
    }

    private fun writePhaseResult(phase: String, pendingCount: Int) {
        val result = JSONObject()
            .put("schema_version", 1)
            .put("phase", phase)
            .put("status", "passed")
            .put("pid", android.os.Process.myPid())
            .put("pending_change_count", pendingCount)
        val destination = File(filesDir, "$phase-result.json")
        val temporary = File(filesDir, ".$phase-result.json.tmp")
        temporary.writeText(result.toString())
        check(temporary.renameTo(destination))
    }
}
