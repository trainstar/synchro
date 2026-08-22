package com.trainstar.synchro.consumer

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class PackagedArtifactTest {
    @Test
    fun localSqlUsesPackagedArtifact() {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        context.deleteDatabase("consumer-test.db")
        val client = SynchroClient(
            SynchroConfig(
                dbPath = "consumer-test.db",
                serverURL = "http://127.0.0.1",
                authProvider = { "unused" },
                clientID = "00000000-0000-4000-8000-000000000002",
                appVersion = "consumer"
            ),
            context
        )
        try {
            client.execute("CREATE TABLE consumer_probe (id TEXT PRIMARY KEY, value TEXT NOT NULL)")
            client.execute(
                "INSERT INTO consumer_probe (id, value) VALUES (?, ?)",
                arrayOf("probe", "packaged")
            )
            assertNotNull(client.queryOne("SELECT id FROM consumer_probe WHERE value = ?", arrayOf("packaged")))
        } finally {
            client.close()
            context.deleteDatabase("consumer-test.db")
        }
    }
}
