package com.trainstar.synchro.conformance

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class NativeSessionTest {
    @Test
    fun routesCommandsToIndependentLogicalSessions() {
        NativeSession(InstrumentationRegistry.getInstrumentation().targetContext).use { session ->
            val first = session.execute(openCommand("session-one", "one.sqlite", "client-one"))
            val second = session.execute(openCommand("session-two", "two.sqlite", "client-two"))
            val firstCapture = session.execute(captureCommand("session-one"))

            assertTrue(first.contains("\"outcome\":\"passed\""))
            assertTrue(second.contains("\"outcome\":\"passed\""))
            assertTrue(firstCapture.contains("\"outcome\":\"passed\""))
        }
    }

    private fun openCommand(sessionID: String, databaseKey: String, clientID: String): String =
        """{"schema_version":1,"operation":"open","session_id":"$sessionID","database_key":"$databaseKey","database_mode":"create","server_url":"http://127.0.0.1:1","auth_token":"token","client_id":"$clientID","platform":"android","app_version":"0.3.0"}"""

    private fun captureCommand(sessionID: String): String =
        """{"schema_version":1,"operation":"capture","session_id":"$sessionID","row_selectors":[]}"""
}
