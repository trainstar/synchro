package com.trainstar.synchro

import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.os.Build
import android.system.Os
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.UUID

internal object SeedDatabaseInstaller {
    private val sqliteHeader = "SQLite format 3\u0000".toByteArray(Charsets.US_ASCII)
    private val requiredTables = setOf(
        "_synchro_meta",
        "_synchro_pending_changes",
        "_synchro_mutation_values",
        "_synchro_capture_context",
        "_synchro_capture_fields",
        "_synchro_push_batch_members",
        "_synchro_schema_archives",
        "_synchro_scopes",
        "_synchro_scope_rows",
        "_synchro_seed_receipts",
        "_synchro_row_versions",
        "_synchro_rebuild_attempts",
        "_synchro_rebuild_page_receipts",
        "_synchro_push_batches",
        "_synchro_rejected_mutations",
        "_synchro_backoff",
    )
    private val requiredMetadata = setOf(
        MetaKey.CHECKPOINT.key,
        MetaKey.SCHEMA_VERSION.key,
        MetaKey.SCHEMA_HASH.key,
        MetaKey.LOCAL_SCHEMA.key,
        MetaKey.SCHEMA_MANIFEST.key,
        MetaKey.SCOPE_SET_VERSION.key,
        MetaKey.SNAPSHOT_COMPLETE.key,
        MetaKey.SYNC_LOCK.key,
    )
    private val emptyWorkTables = setOf(
        "_synchro_pending_changes",
        "_synchro_mutation_values",
        "_synchro_capture_context",
        "_synchro_capture_fields",
        "_synchro_push_batch_members",
        "_synchro_push_batches",
        "_synchro_rejected_mutations",
        "_synchro_rebuild_attempts",
        "_synchro_rebuild_page_receipts",
        "_synchro_backoff",
    )
    private val sidecarSuffixes = listOf("-journal", "-wal", "-shm")
    private val json = Json { ignoreUnknownKeys = false }

    private data class SeedReceipt(
        val scopeID: String,
        val receipt: String,
        val schemaVersion: Long,
        val schemaHash: String,
        val cardinality: Long,
        val checksum: ChecksumObject,
    )

    private data class SeedScope(
        val scopeID: String,
        val checksum: ChecksumObject,
        val generation: Long,
    )

    private data class SeedRowKey(val tableName: String, val recordID: String)

    private data class SeedRowVersion(
        val serverVersion: String,
        val checksum: ChecksumObject,
    )

    @Synchronized
    fun installIfNeeded(context: Context, seedPath: String, databasePath: String) {
        val destination = context.getDatabasePath(databasePath)
        val seed = File(seedPath)
        if (destination.exists() || !seed.exists()) return

        val parent = destination.parentFile
            ?: throw IOException("Database path has no parent directory")
        if (!parent.exists() && !parent.mkdirs()) {
            throw IOException("Could not create the database directory")
        }

        val temporary = File(parent, ".${destination.name}.seed-${UUID.randomUUID()}")
        try {
            seed.copyTo(temporary)
            validate(context, temporary)
            removeSidecars(temporary)

            if (destination.exists()) {
                removeDatabaseFamily(temporary)
                return
            }
            publish(temporary, destination)
        } catch (failure: Throwable) {
            try {
                removeDatabaseFamily(temporary)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    private fun validate(context: Context, file: File) {
        if (!hasSQLiteHeader(file)) {
            throw InvalidSeedDatabaseException("Seed file is not a SQLite database")
        }
        val database = SynchroDatabase.open(context, file.absolutePath)
        try {
            try {
                database.readTransaction { db -> validateMigratedSeed(database, db) }
            } catch (failure: InvalidSeedDatabaseException) {
                throw failure
            } catch (failure: Throwable) {
                throw InvalidSeedDatabaseException("Seed database semantic verification failed", failure)
            }
        } finally {
            database.close()
        }
    }

    private fun validateMigratedSeed(database: SynchroDatabase, db: SQLiteDatabase) {
        if (!hasValidIntegrity(db)) {
            invalidSeed("Seed database integrity check failed")
        }
        if (db.version != SynchroDatabase.DATABASE_VERSION) {
            invalidSeed("Seed database migration state is invalid")
        }

        val tables = queryStrings(db, "SELECT name FROM sqlite_master WHERE type = 'table'")
        if (!tables.containsAll(requiredTables)) {
            invalidSeed("Seed database is missing required Synchro tables")
        }

        val metadata = loadMetadata(db)
        if (!metadata.keys.containsAll(requiredMetadata)) {
            invalidSeed("Seed database is missing required Synchro metadata")
        }
        validateMetadata(metadata)
        emptyWorkTables.forEach { table ->
            if (rowCount(db, table) != 0L) {
                invalidSeed("Seed database contains client work")
            }
        }

        val schemaVersion = parsePositiveSafeInteger(metadata.getValue(MetaKey.SCHEMA_VERSION.key))
            ?: invalidSeed("Seed database schema version is invalid")
        val schemaHash = metadata.getValue(MetaKey.SCHEMA_HASH.key)
        val manifest = decode<SchemaManifest>(metadata.getValue(MetaKey.SCHEMA_MANIFEST.key), "schema manifest")
        try {
            manifest.validate()
        } catch (_: IllegalArgumentException) {
            invalidSeed("Seed database schema manifest is invalid")
        }
        if (manifest.schemaVersion != schemaVersion || manifest.schemaHash != schemaHash ||
            Integrity.schemaManifestHash(manifest) != schemaHash
        ) {
            invalidSeed("Seed database schema metadata is inconsistent")
        }

        val expectedLocalSchema = try {
            manifest.localTables()
        } catch (_: IllegalArgumentException) {
            invalidSeed("Seed database local schema is invalid")
        }
        val storedLocalSchema = decode<List<LocalSchemaTable>>(
            metadata.getValue(MetaKey.LOCAL_SCHEMA.key),
            "local schema",
        )
        if (storedLocalSchema != expectedLocalSchema) {
            invalidSeed("Seed database local schema does not match its manifest")
        }
        validatePhysicalLocalSchema(db, expectedLocalSchema)
        validateCurrentSchemaArchive(db, schemaVersion, schemaHash, expectedLocalSchema)

        val scopes = loadScopes(db)
        val receipts = loadReceipts(db, schemaVersion, schemaHash)
        if (scopes.keys != receipts.keys) {
            invalidSeed("Seed database scope receipts are incomplete")
        }
        for ((scopeID, scope) in scopes) {
            val receipt = receipts.getValue(scopeID)
            if (scope.checksum != receipt.checksum) {
                invalidSeed("Seed database receipt checksum does not match its scope")
            }
        }

        val tablesByName = expectedLocalSchema.associateBy { it.tableName }
        val versions = loadRowVersions(db, tablesByName)
        val materializedRows = loadMaterializedRowKeys(db, expectedLocalSchema)
        if (versions.keys != materializedRows) {
            invalidSeed("Seed database row-version provenance is incomplete")
        }
        val scopeCardinalities = validateScopeRows(db, scopes, versions, materializedRows)
        if (scopeCardinalities.keys != scopes.keys) {
            invalidSeed("Seed database scope provenance is incomplete")
        }

        val pullProcessor = PullProcessor(database)
        for ((scopeID, receipt) in receipts) {
            if (scopeCardinalities.getValue(scopeID) != receipt.cardinality) {
                invalidSeed("Seed database receipt cardinality does not match its scope")
            }
            val computed = try {
                pullProcessor.computeScopeChecksum(db, scopeID, schemaHash, tablesByName)
            } catch (_: Exception) {
                invalidSeed("Seed database row or scope digest verification failed")
            }
            if (computed != receipt.checksum) {
                invalidSeed("Seed database scope digest does not match its receipt")
            }
        }
    }

    private fun loadMetadata(db: SQLiteDatabase): Map<String, String> {
        val metadata = linkedMapOf<String, String>()
        db.rawQuery("SELECT key, value FROM _synchro_meta", null).use { cursor ->
            while (cursor.moveToNext()) {
                val key = cursor.getString(0)
                if (metadata.put(key, cursor.getString(1)) != null) {
                    invalidSeed("Seed database metadata contains a duplicate key")
                }
            }
        }
        return metadata
    }

    private fun validateMetadata(metadata: Map<String, String>) {
        if (metadata[MetaKey.SNAPSHOT_COMPLETE.key] != "1") {
            invalidSeed("Seed database snapshot is incomplete")
        }
        if (metadata[MetaKey.SYNC_LOCK.key] != "0" || metadata[MetaKey.CHECKPOINT.key] != "0") {
            invalidSeed("Seed database runtime metadata is not clean")
        }
        if (parseNonnegativeSafeInteger(metadata.getValue(MetaKey.SCOPE_SET_VERSION.key)) == null) {
            invalidSeed("Seed database scope-set version is invalid")
        }
        val generation = metadata[MetaKey.CLIENT_GENERATION.key]
        if (generation != null && generation != "0") {
            invalidSeed("Seed database has a client generation")
        }
        if (metadata.containsKey(MetaKey.CLIENT_ID.key) || metadata.containsKey(MetaKey.CLIENT_SERVER_ID.key)) {
            invalidSeed("Seed database has a client binding")
        }
    }

    private fun validatePhysicalLocalSchema(db: SQLiteDatabase, tables: List<LocalSchemaTable>) {
        for (table in tables) {
            val columns = mutableListOf<PhysicalColumn>()
            db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(table.tableName)})", null).use { cursor ->
                while (cursor.moveToNext()) {
                    columns += PhysicalColumn(
                        name = cursor.getString(cursor.getColumnIndexOrThrow("name")),
                        type = cursor.getString(cursor.getColumnIndexOrThrow("type")).uppercase(),
                        notNull = cursor.getInt(cursor.getColumnIndexOrThrow("notnull")) != 0,
                        defaultValue = cursor.getColumnIndexOrThrow("dflt_value").let { index ->
                            if (cursor.isNull(index)) null else cursor.getString(index)
                        },
                        primaryKey = cursor.getInt(cursor.getColumnIndexOrThrow("pk")),
                    )
                }
            }
            val expected = table.columns.map { column ->
                PhysicalColumn(
                    name = column.name,
                    type = SQLiteSchema.sqliteType(column.logicalType),
                    notNull = !column.nullable && !column.isPrimaryKey,
                    defaultValue = column.sqliteDefaultSQL,
                    primaryKey = if (column.isPrimaryKey) 1 else 0,
                )
            }
            if (columns != expected) {
                invalidSeed("Seed database local table does not match its manifest")
            }
        }
    }

    private data class PhysicalColumn(
        val name: String,
        val type: String,
        val notNull: Boolean,
        val defaultValue: String?,
        val primaryKey: Int,
    )

    private fun validateCurrentSchemaArchive(
        db: SQLiteDatabase,
        schemaVersion: Long,
        schemaHash: String,
        localSchema: List<LocalSchemaTable>,
    ) {
        db.rawQuery(
            "SELECT manifest_json FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
            arrayOf(schemaVersion.toString(), schemaHash),
        ).use { cursor ->
            if (!cursor.moveToFirst()) {
                invalidSeed("Seed database is missing its current schema archive")
            }
            val archived = decode<List<LocalSchemaTable>>(cursor.getString(0), "schema archive")
            if (archived != localSchema || cursor.moveToNext()) {
                invalidSeed("Seed database current schema archive is inconsistent")
            }
        }
    }

    private fun loadScopes(db: SQLiteDatabase): Map<String, SeedScope> {
        val scopes = linkedMapOf<String, SeedScope>()
        db.rawQuery(
            "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                val scopeID = cursor.getString(0)
                if (scopeID.isEmpty() || !cursor.isNull(1) || cursor.isNull(2) ||
                    cursor.getType(3) != Cursor.FIELD_TYPE_INTEGER || cursor.getLong(3) != 0L || cursor.isNull(4)
                ) {
                    invalidSeed("Seed database scope state is invalid")
                }
                val checksum = decodeChecksum(cursor.getString(2), "scope checksum")
                val localChecksum = decodeChecksum(cursor.getString(4), "local scope checksum")
                if (checksum != localChecksum || scopes.put(scopeID, SeedScope(scopeID, checksum, 0L)) != null) {
                    invalidSeed("Seed database scope state is inconsistent")
                }
            }
        }
        return scopes
    }

    private fun loadReceipts(
        db: SQLiteDatabase,
        schemaVersion: Long,
        schemaHash: String,
    ): Map<String, SeedReceipt> {
        val receipts = linkedMapOf<String, SeedReceipt>()
        db.rawQuery(
            "SELECT scope_id, receipt, schema_version, schema_hash, cardinality, checksum FROM _synchro_seed_receipts",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                if (cursor.getType(2) != Cursor.FIELD_TYPE_INTEGER ||
                    cursor.getType(4) != Cursor.FIELD_TYPE_INTEGER
                ) {
                    invalidSeed("Seed database receipt counters are invalid")
                }
                val scopeID = cursor.getString(0)
                val receipt = SeedReceipt(
                    scopeID = scopeID,
                    receipt = cursor.getString(1),
                    schemaVersion = cursor.getLong(2),
                    schemaHash = cursor.getString(3),
                    cardinality = cursor.getLong(4),
                    checksum = decodeChecksum(cursor.getString(5), "receipt checksum"),
                )
                if (scopeID.isEmpty() || receipt.receipt.isEmpty() || receipt.schemaVersion != schemaVersion ||
                    receipt.schemaHash != schemaHash || receipt.cardinality < 0L ||
                    receipts.put(scopeID, receipt) != null
                ) {
                    invalidSeed("Seed database receipt binding is invalid")
                }
            }
        }
        return receipts
    }

    private fun loadRowVersions(
        db: SQLiteDatabase,
        tablesByName: Map<String, LocalSchemaTable>,
    ): Map<SeedRowKey, SeedRowVersion> {
        val versions = linkedMapOf<SeedRowKey, SeedRowVersion>()
        db.rawQuery(
            "SELECT table_name, record_id, server_version, row_checksum FROM _synchro_row_versions",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                val tableName = cursor.getString(0)
                val recordID = cursor.getString(1)
                val serverVersion = cursor.getString(2)
                if (tableName !in tablesByName || serverVersion.isEmpty() || cursor.isNull(3)) {
                    invalidSeed("Seed database row version is invalid")
                }
                val key = SeedRowKey(tableName, recordID)
                val version = SeedRowVersion(serverVersion, decodeChecksum(cursor.getString(3), "row checksum"))
                if (versions.put(key, version) != null) {
                    invalidSeed("Seed database row version is duplicated")
                }
            }
        }
        return versions
    }

    private fun loadMaterializedRowKeys(
        db: SQLiteDatabase,
        tables: List<LocalSchemaTable>,
    ): Set<SeedRowKey> {
        val rows = linkedSetOf<SeedRowKey>()
        for (table in tables) {
            val primaryKey = table.columns.singleOrNull { it.fieldID == table.primaryKeyFieldID }
                ?: invalidSeed("Seed database local schema has no primary key")
            val relation = SQLiteHelpers.quoteIdentifier(table.tableName)
            val column = SQLiteHelpers.quoteIdentifier(primaryKey.name)
            db.rawQuery("SELECT $column FROM $relation", null).use { cursor ->
                while (cursor.moveToNext()) {
                    val recordID = seedRecordID(cursor, 0, primaryKey.logicalType)
                    if (!rows.add(SeedRowKey(table.tableName, recordID))) {
                        invalidSeed("Seed database contains a duplicate materialized row")
                    }
                }
            }
        }
        return rows
    }

    private fun seedRecordID(cursor: Cursor, index: Int, logicalType: String): String {
        return when (logicalType) {
            "string" -> cursor.getString(index).takeIf { cursor.getType(index) == Cursor.FIELD_TYPE_STRING }
            "int" -> cursor.getLong(index).takeIf {
                cursor.getType(index) == Cursor.FIELD_TYPE_INTEGER && it in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong()
            }?.toString()
            "int64" -> cursor.getLong(index).takeIf { cursor.getType(index) == Cursor.FIELD_TYPE_INTEGER }?.toString()
            else -> null
        } ?: invalidSeed("Seed database materialized row has an invalid primary key")
    }

    private fun validateScopeRows(
        db: SQLiteDatabase,
        scopes: Map<String, SeedScope>,
        versions: Map<SeedRowKey, SeedRowVersion>,
        materializedRows: Set<SeedRowKey>,
    ): Map<String, Long> {
        val cardinalities = scopes.keys.associateWithTo(linkedMapOf()) { 0L }
        val provenRows = linkedSetOf<SeedRowKey>()
        val seen = mutableSetOf<Pair<String, SeedRowKey>>()
        db.rawQuery(
            "SELECT scope_id, table_name, record_id, checksum, generation FROM _synchro_scope_rows",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                val scopeID = cursor.getString(0)
                val key = SeedRowKey(cursor.getString(1), cursor.getString(2))
                val version = versions[key]
                if (scopeID !in scopes || key !in materializedRows || version == null ||
                    cursor.getString(3) != version.checksum.digest ||
                    cursor.getType(4) != Cursor.FIELD_TYPE_INTEGER || cursor.getLong(4) != scopes.getValue(scopeID).generation ||
                    !seen.add(scopeID to key)
                ) {
                    invalidSeed("Seed database scope provenance is invalid")
                }
                provenRows += key
                cardinalities[scopeID] = cardinalities.getValue(scopeID) + 1L
            }
        }
        if (provenRows != materializedRows) {
            invalidSeed("Seed database has a row without complete scope provenance")
        }
        return cardinalities
    }

    private fun decodeChecksum(source: String, subject: String): ChecksumObject {
        val checksum = decode<ChecksumObject>(source, subject)
        try {
            checksum.validate()
        } catch (_: IllegalArgumentException) {
            invalidSeed("Seed database $subject is invalid")
        }
        return checksum
    }

    private inline fun <reified T> decode(source: String, subject: String): T = try {
        json.decodeFromString(source)
    } catch (_: Exception) {
        invalidSeed("Seed database $subject is invalid")
    }

    private fun rowCount(db: SQLiteDatabase, table: String): Long {
        db.rawQuery("SELECT COUNT(*) FROM ${SQLiteHelpers.quoteIdentifier(table)}", null).use { cursor ->
            if (!cursor.moveToFirst() || cursor.getType(0) != Cursor.FIELD_TYPE_INTEGER) {
                invalidSeed("Seed database work state is invalid")
            }
            return cursor.getLong(0)
        }
    }

    private fun parsePositiveSafeInteger(value: String): Long? =
        parseNonnegativeSafeInteger(value)?.takeIf { it > 0L }

    private fun parseNonnegativeSafeInteger(value: String): Long? {
        if (value != "0" && !value.matches(Regex("[1-9][0-9]*"))) return null
        return value.toLongOrNull()?.takeIf { it <= 9_007_199_254_740_991L }
    }

    private fun hasSQLiteHeader(file: File): Boolean {
        val actual = ByteArray(sqliteHeader.size)
        val bytesRead = file.inputStream().use { input -> input.read(actual) }
        return bytesRead == sqliteHeader.size && actual.contentEquals(sqliteHeader)
    }

    private fun hasValidIntegrity(db: SQLiteDatabase): Boolean {
        db.rawQuery("PRAGMA integrity_check", null).use { cursor ->
            return cursor.moveToFirst() &&
                cursor.getString(0) == "ok" &&
                !cursor.moveToNext()
        }
    }

    private fun queryStrings(db: SQLiteDatabase, sql: String): Set<String> {
        val values = mutableSetOf<String>()
        db.rawQuery(sql, null).use { cursor ->
            while (cursor.moveToNext()) values += cursor.getString(0)
        }
        return values
    }

    private fun invalidSeed(message: String): Nothing = throw InvalidSeedDatabaseException(message)

    private fun publish(temporary: File, destination: File) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Files.move(temporary.toPath(), destination.toPath(), StandardCopyOption.ATOMIC_MOVE)
        } else {
            Os.rename(temporary.absolutePath, destination.absolutePath)
        }
    }

    private fun removeDatabaseFamily(file: File) {
        removeFile(file)
        removeSidecars(file)
    }

    private fun removeSidecars(file: File) {
        sidecarSuffixes.forEach { suffix -> removeFile(File(file.path + suffix)) }
    }

    private fun removeFile(file: File) {
        if (file.exists() && !file.delete()) {
            throw IOException("Could not remove temporary seed database file")
        }
    }
}

private class InvalidSeedDatabaseException(message: String, cause: Throwable? = null) : IOException(message, cause)
