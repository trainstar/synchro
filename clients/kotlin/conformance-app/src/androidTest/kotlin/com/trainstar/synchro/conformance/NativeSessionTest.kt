package com.trainstar.synchro.conformance

import android.database.sqlite.SQLiteDatabase
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
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
            val repeatedCapture = session.execute(captureCommand("session-one"))

            assertTrue(first.contains("\"outcome\":\"passed\""))
            assertTrue(second.contains("\"outcome\":\"passed\""))
            assertTrue(firstCapture.contains("\"outcome\":\"passed\""))
            val fingerprint = Regex("\\\"durable_state_fingerprint\\\":\\\"([0-9a-f]{64})\\\"")
            val firstFingerprint = fingerprint.find(firstCapture)?.groupValues?.get(1)
            assertTrue(firstFingerprint != null)
            assertEquals(firstFingerprint, fingerprint.find(repeatedCapture)?.groupValues?.get(1))
        }
    }

    @Test
    fun durableFingerprintIncludesFailureRecoveryStateButExcludesLifecycle() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val databaseKey = "fingerprint.sqlite"
        NativeSession(context).use { session ->
            session.execute(openCommand("session", databaseKey, "client"))
            val baseline = fingerprint(session.execute(captureCommand("session")))

            SQLiteDatabase.openDatabase(
                context.getDatabasePath(databaseKey).absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL("UPDATE _synchro_client_state SET lifecycle_state = 'stopped', updated_at = '2026-01-01T00:00:00.000000Z'")
            }
            assertEquals(baseline, fingerprint(session.execute(captureCommand("session"))))

            SQLiteDatabase.openDatabase(
                context.getDatabasePath(databaseKey).absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL(
                    """
                    UPDATE _synchro_client_state
                    SET lifecycle_state = 'error', error_operation = 'connecting', error_code = 'network_error',
                        error_retryable = 1, error_message = 'network failure', error_recovery_action = 'retry',
                        error_diagnostics = '{}', error_acknowledged = 0, updated_at = '2026-01-01T00:00:01.000000Z'
                    """.trimIndent(),
                )
            }
            assertNotEquals(baseline, fingerprint(session.execute(captureCommand("session"))))
        }
        context.deleteDatabase(databaseKey)
    }

    @Test
    fun durableFingerprintIncludesIndexesAndTriggers() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val databaseKey = "fingerprint-schema.sqlite"
        NativeSession(context).use { session ->
            session.execute(openCommand("session", databaseKey, "client"))
            val baseline = fingerprint(session.execute(captureCommand("session")))

            SQLiteDatabase.openDatabase(
                context.getDatabasePath(databaseKey).absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL("CREATE INDEX fingerprint_error_code ON _synchro_client_state(error_code)")
            }
            val withIndex = fingerprint(session.execute(captureCommand("session")))
            assertNotEquals(baseline, withIndex)

            SQLiteDatabase.openDatabase(
                context.getDatabasePath(databaseKey).absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL(
                    """
                    CREATE TRIGGER fingerprint_client_state_trigger
                    AFTER UPDATE ON _synchro_client_state
                    BEGIN
                        SELECT 1;
                    END
                    """.trimIndent(),
                )
            }
            assertNotEquals(withIndex, fingerprint(session.execute(captureCommand("session"))))
        }
        context.deleteDatabase(databaseKey)
    }

    @Test
    fun durableFingerprintIncludesAutoincrementSequence() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val databaseKey = "fingerprint-sequence.sqlite"
        NativeSession(context).use { session ->
            session.execute(openCommand("session", databaseKey, "client"))
            SQLiteDatabase.openDatabase(
                context.getDatabasePath(databaseKey).absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL("CREATE TABLE fingerprint_sequence_test (id INTEGER PRIMARY KEY AUTOINCREMENT)")
            }
            val baseline = fingerprint(session.execute(captureCommand("session")))

            SQLiteDatabase.openDatabase(
                context.getDatabasePath(databaseKey).absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL("INSERT INTO fingerprint_sequence_test DEFAULT VALUES")
                database.execSQL("DELETE FROM fingerprint_sequence_test")
            }
            assertNotEquals(baseline, fingerprint(session.execute(captureCommand("session"))))
        }
        context.deleteDatabase(databaseKey)
    }

    private fun fingerprint(response: String): String {
        val match = Regex("\\\"durable_state_fingerprint\\\":\\\"([0-9a-f]{64})\\\"").find(response)
        assertTrue(match != null)
        return match!!.groupValues[1]
    }

    private fun openCommand(sessionID: String, databaseKey: String, clientID: String): String =
        """{"schema_version":1,"operation":"open","session_id":"$sessionID","database_key":"$databaseKey","database_mode":"create","server_url":"http://127.0.0.1:1","auth_token":"token","client_id":"$clientID","platform":"android","app_version":"0.3.0"}"""

    private fun captureCommand(sessionID: String): String =
        """{"schema_version":1,"operation":"capture","session_id":"$sessionID","row_selectors":[]}"""
}
