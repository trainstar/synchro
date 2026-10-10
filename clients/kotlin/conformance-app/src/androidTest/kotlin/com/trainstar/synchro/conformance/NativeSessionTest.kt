package com.trainstar.synchro.conformance

import android.database.sqlite.SQLiteDatabase
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class NativeSessionTest {
    @Test
    fun routesCommandsToIndependentLogicalSessions() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val maximumRows = 256
        NativeSession(context).use { session ->
            val first = session.execute(openCommand("session-one", "one.sqlite", "client-one"))
            val second = session.execute(openCommand("session-two", "two.sqlite", "client-two"))
            assertTrue(first.contains("\"outcome\":\"passed\""))
            assertTrue(second.contains("\"outcome\":\"passed\""))
            val selectedCapture = """{"schema_version":1,"operation":"capture","session_id":"session-one","row_selectors":[{"table_name":"local_capture_rows","primary_key_field":"id","primary_key":{"type":"string","value":"first"}},{"table_name":"local_capture_rows","primary_key_field":"id","primary_key":{"type":"string","value":"second"}}]}"""
            SQLiteDatabase.openDatabase(
                context.getDatabasePath("one.sqlite").absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL("CREATE TABLE local_capture_rows (id TEXT PRIMARY KEY, value TEXT NOT NULL)")
                database.execSQL("INSERT INTO local_capture_rows VALUES ('first', 'first-value'), ('second', 'second-value')")
            }
            val firstCapture = session.execute(selectedCapture)
            val repeatedCapture = session.execute(selectedCapture)
            assertTrue(firstCapture.contains("\"outcome\":\"passed\""))
            val firstResult = JSONObject(firstCapture).getJSONObject("result")
            assertEquals(2, firstResult.getInt("application_row_count"))
            val firstRows = firstResult.getJSONArray("application_rows")
            assertEquals(2, firstRows.length())
            assertEquals("first", firstRows.getJSONObject(0).getString("id"))
            assertEquals("first-value", firstRows.getJSONObject(0).getString("value"))
            assertEquals("second", firstRows.getJSONObject(1).getString("id"))
            assertEquals("second-value", firstRows.getJSONObject(1).getString("value"))
            val secondResult = JSONObject(session.execute(captureCommand("session-two"))).getJSONObject("result")
            assertEquals(0, secondResult.getInt("application_row_count"))
            assertEquals(0, secondResult.getJSONArray("application_rows").length())
            val fingerprint = Regex("\\\"durable_state_fingerprint\\\":\\\"([0-9a-f]{64})\\\"")
            val firstFingerprint = fingerprint.find(firstCapture)?.groupValues?.get(1)
            assertTrue(firstFingerprint != null)
            assertEquals(firstFingerprint, fingerprint.find(repeatedCapture)?.groupValues?.get(1))
            SQLiteDatabase.openDatabase(
                context.getDatabasePath("one.sqlite").absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                database.execSQL("INSERT INTO local_capture_rows VALUES ('unselected', 'unselected-value')")
            }
            val incompleteResult = JSONObject(session.execute(selectedCapture)).getJSONObject("result")
            assertEquals(3, incompleteResult.getInt("application_row_count"))
            val incompleteRows = incompleteResult.getJSONArray("application_rows")
            assertEquals(2, incompleteRows.length())
            assertEquals(firstRows.toString(), incompleteRows.toString())
            assertTrue(incompleteResult.getInt("application_row_count") > incompleteRows.length())
            SQLiteDatabase.openDatabase(
                context.getDatabasePath("one.sqlite").absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { database ->
                repeat(maximumRows - 2) { index ->
                    database.execSQL("INSERT INTO local_capture_rows VALUES (?, ?)", arrayOf("bulk-$index", "value-$index"))
                }
            }
            val overLimitResult = JSONObject(session.execute(selectedCapture)).getJSONObject("result")
            assertEquals(maximumRows + 1, overLimitResult.getInt("application_row_count"))
            assertFalse(overLimitResult.has("application_rows"))
            assertFalse(overLimitResult.has("application_row_storage_classes"))
        }
        context.deleteDatabase("one.sqlite")
        context.deleteDatabase("two.sqlite")
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
