package com.trainstar.synchro.consumer

import android.app.Activity
import android.os.Bundle
import com.trainstar.synchro.ColumnDef
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig

class MainActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

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
}
