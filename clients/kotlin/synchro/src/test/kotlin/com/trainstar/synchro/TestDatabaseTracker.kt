package com.trainstar.synchro

import android.content.Context
import java.util.UUID

internal class TestDatabaseTracker {
    private val databases = mutableListOf<SynchroDatabase>()

    fun create(context: Context, prefix: String = "synchro_test"): SynchroDatabase {
        return open(context, "${prefix}_${UUID.randomUUID()}.sqlite")
    }

    fun open(context: Context, dbPath: String): SynchroDatabase {
        val db = SynchroDatabase(context, dbPath)
        databases += db
        return db
    }

    fun closeAll() {
        databases.forEach { it.close() }
        databases.clear()
    }
}
