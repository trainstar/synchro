import Foundation
@preconcurrency import GRDB

private struct RebuildReceiptGroupKey: Hashable {
    let scopeID: String
    let rebuildID: String
}

public final class SynchroClient: @unchecked Sendable {
    private let config: SynchroConfig
    let database: SynchroDatabase
    private let httpClient: HttpClient
    private let schemaManager: SchemaManager
    private let changeTracker: ChangeTracker
    private let pullProcessor: PullProcessor
    private let pushProcessor: PushProcessor
    private let syncEngine: SyncEngine
    private let closeLock = NSLock()
    private var closeTask: Task<Void, Error>?

    public init(config: SynchroConfig) throws {
        try config.validate()
        self.config = config
        if let seedPath = config.seedDatabasePath {
            try SeedDatabaseInstaller.installIfNeeded(seedPath: seedPath, databasePath: config.dbPath)
        }
        self.database = try SynchroDatabase(path: config.dbPath)
        self.httpClient = HttpClient(config: config)
        self.schemaManager = SchemaManager(database: database)
        self.changeTracker = ChangeTracker(database: database)
        self.pullProcessor = PullProcessor(database: database)
        self.pushProcessor = PushProcessor(database: database, changeTracker: changeTracker)
        self.syncEngine = SyncEngine(
            config: config,
            database: database,
            httpClient: httpClient,
            schemaManager: schemaManager,
            changeTracker: changeTracker,
            pullProcessor: pullProcessor,
            pushProcessor: pushProcessor
        )
    }

    // MARK: - Core SQL

    public func query(_ sql: String, params: [(any DatabaseValueConvertible)?]? = nil) throws -> [Row] {
        try database.query(sql, params: params)
    }

    public func queryOne(_ sql: String, params: [(any DatabaseValueConvertible)?]? = nil) throws -> Row? {
        try database.queryOne(sql, params: params)
    }

    public func execute(_ sql: String, params: [(any DatabaseValueConvertible)?]? = nil) throws -> ExecResult {
        try database.execute(sql, params: params)
    }

    // MARK: - Transactions

    public func readTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try database.readTransaction(block)
    }

    public func writeTransaction<T>(_ block: (ApplicationTransaction) throws -> T) throws -> T {
        try database.applicationWriteTransaction(block)
    }

    /// Runs one write transaction whose synced mutations the server applies all together or not at all.
    ///
    /// The transaction opens through the same path as `writeTransaction`, so nesting
    /// behavior is identical. Before commit, the client validates the group. An
    /// invalid group throws `SynchroError.atomicGroupInvalid` and rolls back.
    ///
    /// A failed group uses the ordinary outcome paths and has no revert path. After a
    /// failed group with a conflict, local state holds the server row for the
    /// conflicting member and local values for every other member.
    public func atomicWriteTransaction<T>(_ block: (ApplicationTransaction) throws -> T) throws -> T {
        try database.applicationAtomicWriteTransaction(
            validate: { db, groupID in
                try pushProcessor.validateAtomicGroup(db, groupID: groupID, clientID: config.clientID)
            },
            block
        )
    }

    /// Runs one write transaction whose capture retains only the named
    /// authored columns of the named table. A runtime support column the
    /// statement also writes stays out of the captured payload.
    public func authoredWriteTransaction<T>(
        tableName: String,
        operation: String,
        columnNames: [String],
        _ block: (ApplicationTransaction) throws -> T
    ) throws -> T {
        try database.applicationAuthoredWriteTransaction(
            tableName: tableName,
            operation: operation,
            columnNames: columnNames,
            block
        )
    }

    public func authoredWriteTransaction<T>(
        tableName: String,
        operation: Operation,
        columnNames: [String],
        _ block: (ApplicationTransaction) throws -> T
    ) throws -> T {
        try authoredWriteTransaction(
            tableName: tableName,
            operation: operation.rawValue,
            columnNames: columnNames,
            block
        )
    }

    // MARK: - Prepared Statements

    public func withPreparedStatement<T>(_ sql: String, _ block: (Statement) throws -> T) throws -> T {
        try database.dbPool.read { db in
            let statement = try db.makeStatement(sql: sql)
            return try block(statement)
        }
    }

    // MARK: - Batch

    public func executeBatch(_ statements: [SQLStatement]) throws -> Int {
        try database.executeBatch(statements)
    }

    // MARK: - Schema (local-only tables)

    public func createTable(_ name: String, columns: [ColumnDef], options: TableOptions? = nil) throws {
        try database.createTable(name, columns: columns, options: options)
    }

    public func alterTable(_ name: String, addColumns: [ColumnDef]) throws {
        try database.alterTable(name, addColumns: addColumns)
    }

    public func createIndex(_ table: String, columns: [String], unique: Bool = false) throws {
        try database.createIndex(table, columns: columns, unique: unique)
    }

    // MARK: - Observation

    public func onChange(tables: [String], callback: @escaping () -> Void) -> any Cancellable {
        DatabaseCancellableWrapper(database.onChange(tables: tables, callback: callback))
    }

    /// Delivers the initial snapshot and changed query results on the main queue.
    public func watch(_ sql: String, params: [(any DatabaseValueConvertible)?]? = nil, tables: [String], callback: @escaping ([Row]) -> Void) -> any Cancellable {
        DatabaseCancellableWrapper(database.watch(sql, params: params, tables: tables, callback: callback))
    }

    // MARK: - Lifecycle

    public func close() async throws {
        let task: Task<Void, Error> = {
            closeLock.lock()
            defer { closeLock.unlock() }
            if let closeTask {
                return closeTask
            }
            let closeTask = Task { [weak self] in
                guard let self else { return }
                await self.syncEngine.shutdown()
                try self.database.close()
            }
            self.closeTask = closeTask
            return closeTask
        }()
        try await task.value
    }

    public var path: String {
        database.path
    }

    // MARK: - Sync Status

    /// Returns the number of local changes waiting to be pushed to the server.
    public func pendingChangeCount() throws -> Int {
        try changeTracker.pendingChangeCount()
    }

    public func getSyncStatus() -> SyncStatus {
        syncEngine.getSyncStatus()
    }

    /// Returns the unresolved queue records.
    ///
    /// Deprecated: use `inspectRetainedMutationRecords()`. This method keeps
    /// its published behavior. It throws `SynchroError.invalidResponse` when the
    /// queue holds a legacy import, because a legacy record has no current binding.
    public func inspectPendingMutations() throws -> [PendingMutationInspection] {
        try changeTracker.inspectPendingMutations()
    }

    /// Returns every retained queue record, including local terminal states.
    ///
    /// Deprecated: use `inspectRetainedMutationRecords()`. This method keeps
    /// its published behavior. It throws `SynchroError.invalidResponse` when the
    /// queue holds a legacy import, because a legacy record has no current binding.
    public func inspectRetainedMutations() throws -> [PendingMutationInspection] {
        try changeTracker.inspectRetainedMutations()
    }

    /// Returns every retained queue record in its stored representation.
    /// A legacy import is a `.legacy` record with only its stored fields.
    public func inspectRetainedMutationRecords() throws -> [RetainedMutationInspection] {
        try changeTracker.inspectRetainedMutationRecords()
    }

    /// Returns the retained terminal outcomes.
    ///
    /// Deprecated: use `inspectRejectedMutationRecords()`. This method keeps
    /// its published behavior. It throws `SynchroError.invalidResponse` when a
    /// legacy rejection is present, because a legacy rejection has no exact JSON.
    public func inspectRejectedMutations() throws -> [RejectedMutationInspection] {
        try inspectRejectedMutationRecords().map { record in
            guard let rejection = record.current else {
                throw SynchroError.invalidResponse(message: "retained rejection has no complete durable mutation")
            }
            return rejection
        }
    }

    /// Returns the retained terminal outcomes in their stored representation.
    /// A legacy rejection is a `.legacy` record with only its stored fields.
    public func inspectRejectedMutationRecords() throws -> [RetainedRejectionInspection] {
        try database.readTransaction(Self.inspectRejectedMutations)
    }

    private static func inspectRejectedMutations(_ db: GRDB.Database) throws -> [RetainedRejectionInspection] {
        try SynchroMeta.listRejectedMutations(db).map { rejected in
            guard let status = MutationStatus(rawValue: rejected.status),
                  status == .conflict || status == .rejectedTerminal,
                  let code = MutationRejectionCode(rawValue: rejected.code) else {
                throw SynchroError.invalidResponse(message: "retained rejection is invalid")
            }
            // A rejection stored before the mutation ledger has no exact JSON and no ledger row.
            if rejected.mutationJSON == nil, rejected.rejectedJSON == nil, rejected.localOrder == nil {
                return .legacy(LegacyRejectionInspection(
                    mutationID: rejected.mutationID,
                    tableName: rejected.tableName,
                    recordID: rejected.recordID,
                    status: status,
                    code: code,
                    message: rejected.message,
                    serverRowJSON: rejected.serverRowJSON,
                    serverVersion: rejected.serverVersion,
                    createdAt: rejected.createdAt,
                    updatedAt: rejected.updatedAt
                ))
            }
            guard let localOrder = rejected.localOrder,
                  let mutationJSON = rejected.mutationJSON,
                  let rejectionJSON = rejected.rejectedJSON,
                  let mutationData = mutationJSON.data(using: .utf8),
                  let rejectionData = rejectionJSON.data(using: .utf8) else {
                throw SynchroError.invalidResponse(message: "retained rejection has no complete durable mutation")
            }
            let decoder = JSONDecoder.synchroDecoder()
            let mutation: Mutation
            let rejection: RejectedMutation
            do {
                mutation = try decoder.decode(Mutation.self, from: mutationData)
                rejection = try decoder.decode(RejectedMutation.self, from: rejectionData)
            } catch {
                throw SynchroError.invalidResponse(message: "retained rejection payload is invalid")
            }
            guard mutation.mutationID == rejected.mutationID,
                  rejection.mutationID == rejected.mutationID,
                  mutation.table == rejection.table,
                  mutation.pk == rejection.pk,
                  rejection.status == status,
                  rejection.code == code else {
                throw SynchroError.invalidResponse(message: "retained rejection identity is inconsistent")
            }
            return .current(RejectedMutationInspection(
                mutationID: rejected.mutationID,
                localOrder: localOrder,
                tableName: rejected.tableName,
                recordID: rejected.recordID,
                status: status,
                code: code,
                message: rejected.message,
                serverRowJSON: rejected.serverRowJSON,
                serverVersion: rejected.serverVersion,
                mutationJSON: mutationJSON,
                rejectionJSON: rejectionJSON,
                mutation: mutation,
                rejection: rejection,
                createdAt: rejected.createdAt,
                updatedAt: rejected.updatedAt
            ))
        }
    }

    func inspectCurrentSchema() throws -> SchemaRef? {
        try database.readTransaction(Self.inspectSchema)
    }

    func inspectScopeStates() throws -> [ScopeStateInspection] {
        try database.readTransaction(Self.inspectScopeStates)
    }

    func inspectScopeRows() throws -> [ScopeRowInspection] {
        try database.readTransaction(Self.inspectScopeRows)
    }

    func inspectClientStateCapture(maximumRecords: Int) throws -> ClientStateCaptureInspection {
        guard maximumRecords >= 0, maximumRecords < Int.max else {
            throw SynchroError.invalidResponse(message: "inspection record limit is invalid")
        }
        return try database.stateInspectionTransaction { db, provenanceMaintenanceWorkCursor in
            try Self.inspectClientStateCapture(
                db,
                maximumRecords: maximumRecords,
                provenanceMaintenanceWorkCursor: provenanceMaintenanceWorkCursor
            )
        }
    }

    /// Reads counts, details, and application rows from one read-only snapshot.
    /// `readApplicationRows` runs inside that snapshot and must not escape its transaction.
    func inspectClientStateSnapshot(
        maximumRecords: Int,
        readApplicationRows: (ClientStateCaptureInspection, ApplicationTransaction) throws -> Void
    ) throws -> ClientStateSnapshotInspection {
        guard maximumRecords >= 0, maximumRecords < Int.max else {
            throw SynchroError.invalidResponse(message: "inspection record limit is invalid")
        }
        return try database.stateInspectionTransaction { db, provenanceMaintenanceWorkCursor in
            try db.readOnly {
                let capture = try Self.inspectClientStateCapture(
                    db,
                    maximumRecords: maximumRecords,
                    provenanceMaintenanceWorkCursor: provenanceMaintenanceWorkCursor
                )
                let snapshot = ClientStateSnapshotInspection(
                    capture: capture,
                    pendingChangeCount: try changeTracker.countPendingChanges(db),
                    retainedMutations: capture.mutationLedgerCount <= maximumRecords
                        ? try changeTracker.inspectMutations(db, includeTerminal: true)
                        : nil,
                    rejectedMutations: capture.rejectedMutationCount <= maximumRecords
                        ? try Self.inspectRejectedMutations(db)
                        : nil,
                    blockingFailure: try SynchroMeta.getBlockingFailure(db)
                )
                try readApplicationRows(capture, ApplicationTransaction(readOnlyDatabase: db))
                return snapshot
            }
        }
    }

    private static func inspectClientStateCapture(
        _ db: GRDB.Database,
        maximumRecords: Int,
        provenanceMaintenanceWorkCursor: Int64
    ) throws -> ClientStateCaptureInspection {
        let provenanceCount = try Self.inspectCount(
            db,
            sql: "SELECT COUNT(*) FROM (SELECT table_name, record_id FROM _synchro_scope_rows GROUP BY table_name, record_id)"
        )
        let scopeStateCount = try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_scopes")
        let scopeRowCount = try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_scope_rows")
        let rebuildAttemptCount = try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_rebuild_attempts")
        let rebuildReceiptCount = try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_rebuild_page_receipts")
        let scopeStates = try Self.inspectScopeStates(db)
        let scopeRows = try Self.inspectScopeRows(db)
        let rebuildAttempts = try Self.inspectRebuildAttempts(db)
        let rebuildReceipts = try Self.inspectRebuildReceipts(db)
        let rowMetadataCount = try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_row_versions")
        let rowMetadata = try SynchroMeta.listRowMetadata(db, limit: maximumRecords).map { metadata in
            RowMetadataInspection(
                tableName: metadata.tableName,
                recordID: metadata.recordID,
                serverVersion: metadata.serverVersion,
                rowChecksum: metadata.rowChecksum
            )
        }
        let scopeStatesTruncated = scopeStates.count > maximumRecords
        let scopeRowsTruncated = scopeRows.count > maximumRecords
        let rebuildAttemptsTruncated = rebuildAttempts.count > maximumRecords
        let rebuildReceiptsTruncated = rebuildReceipts.count > maximumRecords
        let rowMetadataTruncated = rowMetadataCount > maximumRecords
        let migration = try Self.inspectMigrationJournal(db, maximumRecords: maximumRecords)
        let physicalSchema = try Self.inspectPhysicalSchema(db, maximumRecords: maximumRecords, migration: migration)
        return ClientStateCaptureInspection(
            schema: try Self.inspectSchema(db),
            scopeStates: Array(scopeStates.prefix(maximumRecords)),
            scopeStatesTruncated: scopeStatesTruncated,
            scopeRows: Array(scopeRows.prefix(maximumRecords)),
            scopeRowsTruncated: scopeRowsTruncated,
            rebuildAttempts: Array(rebuildAttempts.prefix(maximumRecords)),
            rebuildAttemptsTruncated: rebuildAttemptsTruncated,
            rebuildReceipts: Array(rebuildReceipts.prefix(maximumRecords)),
            rebuildReceiptsTruncated: rebuildReceiptsTruncated,
            rowMetadata: rowMetadata,
            rowMetadataTruncated: rowMetadataTruncated,
            overflowed: scopeStatesTruncated
                || scopeRowsTruncated
                || rebuildAttemptsTruncated
                || rebuildReceiptsTruncated
                || rowMetadataTruncated
                || migration.truncated
                || physicalSchema.truncated,
            applicationRowCount: try Self.inspectApplicationRowCount(db),
            mutationLedgerCount: try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_pending_changes"),
            mutationOutcomeCount: try Self.inspectCount(
                db,
                sql: "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state IN ('accepted', 'rejected')"
            ),
            sealedBatchCount: try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_push_batches"),
            rejectedMutationCount: try Self.inspectCount(db, sql: "SELECT COUNT(*) FROM _synchro_rejected_mutations"),
            scopeStateCount: scopeStateCount,
            scopeRowCount: scopeRowCount,
            provenanceCount: provenanceCount,
            rowMetadataCount: rowMetadataCount,
            rebuildAttemptCount: rebuildAttemptCount,
            rebuildReceiptCount: rebuildReceiptCount,
            provenanceMaintenanceWorkCursor: provenanceMaintenanceWorkCursor,
            migrationJournal: migration.journal,
            migrationJournalTruncated: migration.truncated,
            physicalSchema: physicalSchema.columns,
            physicalSchemaTruncated: physicalSchema.truncated
        )
    }

    private static func inspectMigrationJournal(
        _ db: GRDB.Database,
        maximumRecords: Int
    ) throws -> (journal: MigrationJournalInspection?, truncated: Bool) {
        guard let preflight = try Row.fetchOne(db, sql: """
            SELECT typeof(source_schema_version) = 'integer' AND typeof(target_schema_version) = 'integer' AND
                typeof(journal_version) = 'integer' AND typeof(migration_plan_version) = 'integer' AND
                typeof(is_schema_reset) = 'integer' AND typeof(source_schema_hash) = 'text' AND
                typeof(target_schema_hash) = 'text' AND typeof(action) = 'text' AND typeof(phase) = 'text' AND
                typeof(target_manifest_json) = 'text' AND typeof(affected_scopes_json) = 'text' AND
                typeof(scope_cursor_updates_json) = 'text' AND typeof(migration_plan_json) = 'text' AND
                typeof(migration_plan_hash) = 'text' AS storage_valid,
                CASE WHEN ? = 0 THEN 0 ELSE
                coalesce(length(CAST(source_schema_version AS BLOB)), 0) + coalesce(length(CAST(target_schema_version AS BLOB)), 0) +
                coalesce(length(CAST(journal_version AS BLOB)), 0) + coalesce(length(CAST(target_manifest_json AS BLOB)), 0) +
                coalesce(length(CAST(affected_scopes_json AS BLOB)), 0) + coalesce(length(CAST(scope_cursor_updates_json AS BLOB)), 0) +
                coalesce(length(CAST(migration_plan_version AS BLOB)), 0) + coalesce(length(CAST(migration_plan_json AS BLOB)), 0) +
                coalesce(length(CAST(migration_plan_hash AS BLOB)), 0) + coalesce(length(CAST(is_schema_reset AS BLOB)), 0) +
                coalesce(length(CAST(action AS BLOB)), 0) + coalesce(length(CAST(phase AS BLOB)), 0) +
                coalesce(length(CAST(source_schema_hash AS BLOB)), 0) + coalesce(length(CAST(target_schema_hash AS BLOB)), 0)
                END AS byte_count FROM _synchro_schema_migration WHERE singleton = 1
            """, arguments: [maximumRecords]) else {
            return (nil, false)
        }
        let byteCount: Int64 = try preflight.decode(Int64.self, forColumn: "byte_count")
        guard maximumRecords > 0, byteCount <= 65_536 else { return (nil, true) }
        let storageValid: Bool = try preflight.decode(Bool.self, forColumn: "storage_valid")
        guard storageValid, byteCount >= 0 else {
            throw SynchroError.invalidResponse(message: "migration inspection journal storage is invalid")
        }
        guard let row = try Row.fetchOne(db, sql: """
            SELECT source_schema_version, source_schema_hash, target_schema_version, target_schema_hash,
                   action, phase, journal_version, target_manifest_json, affected_scopes_json,
                   scope_cursor_updates_json, migration_plan_version, migration_plan_json,
                   migration_plan_hash, is_schema_reset
            FROM _synchro_schema_migration WHERE singleton = 1
            """) else {
            throw SynchroError.invalidResponse(message: "migration inspection journal is invalid")
        }
        do {
            let keys = ["target_manifest_json", "affected_scopes_json", "scope_cursor_updates_json",
                        "migration_plan_json", "migration_plan_hash"]
            var stored: [String: String] = [:]
            for key in keys {
                let value: String = try row.decode(String.self, forColumn: key)
                stored[key] = value
            }
            for key in ["journal_version", "migration_plan_version", "is_schema_reset"] {
                let value: Int64 = try row.decode(Int64.self, forColumn: key)
                stored[key] = String(value)
            }
            let action: String = try row.decode(String.self, forColumn: "action")
            let phase: String = try row.decode(String.self, forColumn: "phase")
            let sourceHash: String = try row.decode(String.self, forColumn: "source_schema_hash")
            let targetHash: String = try row.decode(String.self, forColumn: "target_schema_hash")
            let sourceVersion: Int64 = try row.decode(Int64.self, forColumn: "source_schema_version")
            let targetVersion: Int64 = try row.decode(Int64.self, forColumn: "target_schema_version")
            return (MigrationJournalInspection(
                source: SchemaRef(version: sourceVersion, hash: sourceHash),
                target: SchemaRef(version: targetVersion, hash: targetHash),
                action: action,
                phase: phase,
                stored: stored
            ), false)
        } catch {
            throw SynchroError.invalidResponse(message: "migration inspection journal values are invalid")
        }
    }

    private static func inspectPhysicalSchema(
        _ db: GRDB.Database,
        maximumRecords: Int,
        migration: (journal: MigrationJournalInspection?, truncated: Bool)
    ) throws -> (columns: [PhysicalSchemaColumnInspection], truncated: Bool) {
        if migration.truncated { return ([], true) }
        let source: (tables: [LocalSchemaTable]?, truncated: Bool)
        var targetNames: [String] = []
        if let journal = migration.journal {
            do {
                if journal.source != SchemaRef(version: 0, hash: "") { try journal.source.validate() }
                try journal.target.validate()
                guard let encoded = journal.stored["target_manifest_json"] else {
                    throw SynchroError.invalidResponse(message: "migration inspection target is missing")
                }
                let target = try JSONDecoder().decode(SchemaManifest.self, from: Data(encoded.utf8))
                try target.validate()
                guard SchemaRef(version: target.schemaVersion, hash: target.schemaHash) == journal.target,
                      try Integrity.schemaManifestHash(target) == journal.target.hash else {
                    throw SynchroError.invalidResponse(message: "migration inspection target binding is invalid")
                }
                targetNames = target.tables.map(\.name)
                if journal.source.version == 0 {
                    source = ([], false)
                } else {
                    source = try inspectSchemaProjection(db, maximumRecords: maximumRecords,
                        sql: "SELECT schema_json AS metadata FROM _synchro_schema_archive WHERE schema_version = ? AND schema_hash = ?",
                        arguments: [journal.source.version, journal.source.hash])
                    if source.tables?.isEmpty != false && !source.truncated {
                        throw SynchroError.invalidResponse(message: "migration inspection source archive is missing")
                    }
                }
            } catch {
                throw SynchroError.invalidResponse(message: "migration inspection identities are invalid")
            }
        } else {
            source = try inspectSchemaProjection(db, maximumRecords: maximumRecords,
                sql: "SELECT value AS metadata FROM _synchro_meta WHERE key = 'local_schema'", arguments: [])
        }
        if source.truncated { return ([], true) }
        let tables = source.tables ?? []
        guard Set(tables.map(\.tableID)).count == tables.count,
              Set(tables.map(\.tableName)).count == tables.count,
              tables.allSatisfy({ !$0.tableID.isEmpty && !$0.relationID.isEmpty }) else {
            throw SynchroError.invalidResponse(message: "inspection source table identities are invalid")
        }
        let names = Set(tables.map(\.tableName) + targetNames).sorted()
        guard names.allSatisfy({ !$0.isEmpty && !$0.contains("\u{0}") &&
            !$0.lowercased().hasPrefix("_synchro_") && !$0.lowercased().hasPrefix("sqlite_") }) else {
            throw SynchroError.invalidResponse(message: "inspection table names are invalid")
        }
        if names.count > maximumRecords { return ([], true) }
        var columns: [PhysicalSchemaColumnInspection] = []
        var byteCount = 0
        for tableName in names {
            let rows = try Row.fetchAll(db, sql: "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?) ORDER BY cid LIMIT ?",
                                        arguments: [tableName, maximumRecords - columns.count + 1])
            for row in rows {
                if columns.count == maximumRecords { return (columns, true) }
                let name: String = row["name"]
                let type: String = row["type"]
                byteCount += tableName.utf8.count + name.utf8.count + type.utf8.count
                if byteCount > 65_536 { return (columns, true) }
                columns.append(PhysicalSchemaColumnInspection(
                    tableName: tableName, name: name, type: type,
                    notNull: (row["notnull"] as Int) != 0, primaryKeyPosition: row["pk"]
                ))
            }
        }
        return (columns, false)
    }

    private static func inspectSchemaProjection(
        _ db: GRDB.Database,
        maximumRecords: Int,
        sql: String,
        arguments: StatementArguments
    ) throws -> (tables: [LocalSchemaTable]?, truncated: Bool) {
        guard let byteCount = try Int64.fetchOne(db,
            sql: "SELECT length(CAST(metadata AS BLOB)) FROM (\(sql))",
            arguments: arguments) else { return (nil, false) }
        if maximumRecords == 0 || byteCount > 65_536 { return (nil, true) }
        guard byteCount >= 0, let encoded = try String.fetchOne(db, sql: sql, arguments: arguments) else {
            throw SynchroError.invalidResponse(message: "inspection schema projection is missing")
        }
        do {
            let tables = try JSONDecoder().decode([LocalSchemaTable].self, from: Data(encoded.utf8))
            if tables.count > maximumRecords { return (nil, true) }
            return (tables, false)
        } catch {
            throw SynchroError.invalidResponse(message: "inspection schema projection is invalid")
        }
    }

    func inspectRowMetadata(tableName: String, recordID: String) throws -> RowMetadataInspection? {
        try database.readTransaction { db in
            guard let metadata = try SynchroMeta.getRowMetadata(db, tableName: tableName, recordID: recordID) else {
                return nil
            }
            return RowMetadataInspection(
                tableName: metadata.tableName,
                recordID: metadata.recordID,
                serverVersion: metadata.serverVersion,
                rowChecksum: metadata.rowChecksum
            )
        }
    }

    func inspectRebuildAttempts() throws -> [RebuildAttemptInspection] {
        try database.readTransaction(Self.inspectRebuildAttempts)
    }

    private static func inspectSchema(_ db: GRDB.Database) throws -> SchemaRef? {
        let version = try SynchroMeta.getInt64(db, key: .schemaVersion)
        let hash = try SynchroMeta.get(db, key: .schemaHash) ?? ""
        if version == 0 && hash.isEmpty {
            return nil
        }
        let schema = SchemaRef(version: version, hash: hash)
        try schema.validate()
        return schema
    }

    private static func inspectCount(_ db: GRDB.Database, sql: String) throws -> Int {
        guard let count = try Int64.fetchOne(db, sql: sql), count >= 0, count <= Int64(Int.max) else {
            throw SynchroError.invalidResponse(message: "durable state count is invalid")
        }
        return Int(count)
    }

    private static func inspectApplicationRowCount(_ db: GRDB.Database) throws -> Int {
        guard let encoded = try SynchroMeta.get(db, key: .localSchema),
              let data = encoded.data(using: .utf8) else {
            return 0
        }
        let tables: [LocalSchemaTable]
        do {
            tables = try JSONDecoder().decode([LocalSchemaTable].self, from: data)
        } catch {
            throw SynchroError.invalidResponse(message: "stored local schema is invalid")
        }
        var total = 0
        for table in tables {
            let count = try inspectCount(
                db,
                sql: "SELECT COUNT(*) FROM \(SQLiteHelpers.quoteIdentifier(table.tableName))"
            )
            let (next, overflow) = total.addingReportingOverflow(count)
            guard !overflow else {
                throw SynchroError.invalidResponse(message: "application row count is invalid")
            }
            total = next
        }
        return total
    }

    private static func inspectScopeStates(_ db: GRDB.Database) throws -> [ScopeStateInspection] {
        try SynchroMeta.getAllScopes(db).map { scope in
            ScopeStateInspection(
                scopeID: scope.scopeID,
                cursor: scope.cursor,
                checksum: scope.checksum,
                localChecksum: scope.localChecksum,
                generation: scope.generation
            )
        }
    }

    private static func inspectScopeRows(_ db: GRDB.Database) throws -> [ScopeRowInspection] {
        var result: [ScopeRowInspection] = []
        for scope in try SynchroMeta.getAllScopes(db) {
            result.append(contentsOf: try SynchroMeta.getScopeRowChecksums(db, scopeID: scope.scopeID).map { row in
                ScopeRowInspection(
                    scopeID: scope.scopeID,
                    tableName: row.tableName,
                    recordID: row.recordID,
                    checksum: row.checksum,
                    generation: row.generation
                )
            })
        }
        return result.sorted { left, right in
            let leftKey = [left.scopeID, left.tableName, left.recordID]
            let rightKey = [right.scopeID, right.tableName, right.recordID]
            return leftKey.lexicographicallyPrecedes(rightKey)
        }
    }

    private static func inspectRebuildAttempts(_ db: GRDB.Database) throws -> [RebuildAttemptInspection] {
        try SynchroMeta.getAllScopes(db).compactMap { scope in
            guard let attempt = try SynchroMeta.getRebuildAttempt(db, scopeID: scope.scopeID) else {
                return nil
            }
            return RebuildAttemptInspection(
                scopeID: attempt.scopeID,
                rebuildID: attempt.rebuildID,
                clientGeneration: attempt.clientGeneration,
                schemaVersion: attempt.schemaVersion,
                schemaHash: attempt.schemaHash,
                generation: attempt.generation,
                cursor: attempt.cursor,
                pageLimit: attempt.pageLimit
            )
        }
    }

    func inspectRebuildReceipts() throws -> [RebuildReceiptInspection] {
        try database.readTransaction(Self.inspectRebuildReceipts)
    }

    private static func inspectRebuildReceipts(_ db: GRDB.Database) throws -> [RebuildReceiptInspection] {
        let receipts = try SynchroMeta.listRebuildPageReceipts(db)
        let grouped = Dictionary(grouping: receipts) { receipt in
            RebuildReceiptGroupKey(scopeID: receipt.scopeID, rebuildID: receipt.rebuildID)
        }
        return try grouped.keys.sorted { left, right in
            if left.scopeID == right.scopeID {
                return Self.utf8Less(left.rebuildID, right.rebuildID)
            }
            return Self.utf8Less(left.scopeID, right.scopeID)
        }.map { key in
            try Self.inspectRebuildReceipts(
                db: db,
                receipts: grouped[key] ?? []
            )
        }
    }

    public func clearRejectedMutations() throws {
        try database.writeTransaction { db in
            try SynchroMeta.clearRejectedMutations(db)
        }
    }

    // MARK: - Sync Control

    public func start(options: SyncOptions? = nil) async throws {
        try await syncEngine.start(options: options)
    }

    public func stop() async {
        await syncEngine.stop()
    }

    public func syncNow() async throws {
        try await syncEngine.syncNow()
    }

    public func enterBackground() async {
        await syncEngine.enterBackground()
    }

    public func enterForeground() async throws {
        try await syncEngine.enterForeground()
    }

    public func retryAfterError() async throws {
        try await syncEngine.retryAfterError()
    }

    public func resetSchemaAndStart() async throws {
        try await syncEngine.resetSchemaAndStart()
    }

    // MARK: - Status

    public func onStatusChange(_ callback: @escaping (SyncStatus) -> Void) -> any Cancellable {
        syncEngine.onStatusChange(callback)
    }

    public func onSyncEvent(_ callback: @escaping (SyncEvent) -> Void) -> any Cancellable {
        syncEngine.onEvent(callback)
    }

    public func getBlockingFailure() throws -> SyncFailure? {
        try syncEngine.getBlockingFailure()
    }

    public func onConflict(_ callback: @escaping (ConflictEvent) -> Void) -> any Cancellable {
        syncEngine.onConflict(callback)
    }

    private static func inspectRebuildReceipts(
        db: GRDB.Database,
        receipts: [LocalRebuildPageReceipt]
    ) throws -> RebuildReceiptInspection {
        guard let first = receipts.first else {
            return RebuildReceiptInspection(
                rebuildIDFingerprint: "",
                pageCount: 0,
                returnedRecordCount: 0,
                requestChainExpected: [],
                requestChainObserved: [],
                recordIdentitiesHex: [],
                receivedRowChecksums: [],
                computedRowChecksums: [],
                computedScopeChecksum: nil,
                finalScopeChecksum: nil,
                storedScopeChecksum: nil,
                localScopeChecksum: nil
            )
        }

        let decoder = JSONDecoder.synchroDecoder()
        var decoded: [(receipt: LocalRebuildPageReceipt, request: RebuildRequest, response: RebuildResponse, finalChecksum: ChecksumObject?)] = []
        decoded.reserveCapacity(receipts.count)
        for receipt in receipts {
            let request = try decodeExactReceiptJSON(receipt.requestJSON, as: RebuildRequest.self, decoder: decoder)
            let response = try decodeExactReceiptJSON(receipt.responseJSON, as: RebuildResponse.self, decoder: decoder)
            let finalChecksum: ChecksumObject?
            if let finalChecksumJSON = receipt.finalChecksumJSON {
                finalChecksum = try decodeExactReceiptJSON(finalChecksumJSON, as: ChecksumObject.self, decoder: decoder)
            } else {
                finalChecksum = nil
            }
            decoded.append((receipt, request, response, finalChecksum))
        }

        var requestChainExpected: [String] = []
        var requestChainObserved: [String] = []
        func appendChain(_ expected: String?, _ observed: String?) {
            requestChainExpected.append(expected ?? "null")
            requestChainObserved.append(observed ?? "null")
        }
        var requestCursorIndexes: [String: [Int]] = [:]
        for (index, item) in decoded.enumerated() {
            let requestKey = cursorKey(item.receipt.requestCursor)
            requestCursorIndexes[requestKey, default: []].append(index)
            appendChain(item.receipt.scopeID, item.request.scope)
            appendChain(cursorFingerprint(item.receipt.rebuildID), cursorFingerprint(item.request.rebuildID))
            appendChain(item.receipt.requestCursor.map(cursorFingerprint), item.request.cursor.map(cursorFingerprint))
            appendChain(item.receipt.scopeID, item.response.scope)
            appendChain(String(item.receipt.isFinal), String(!item.response.hasMore))
            appendChain(
                (item.response.hasMore ? nil : item.response.finalScopeCursor).map(cursorFingerprint),
                item.receipt.finalScopeCursor.map(cursorFingerprint)
            )
            appendChain(item.response.checksum.map(checksumKey), item.finalChecksum.map(checksumKey))
            appendChain(item.response.hasMore ? "cursor" : "final", item.response.cursor == nil ? "final" : "cursor")
            appendChain(item.response.hasMore ? "no-final-cursor" : "final-cursor", item.response.finalScopeCursor == nil ? "no-final-cursor" : "final-cursor")
            appendChain(item.response.hasMore ? "no-checksum" : "checksum", item.response.checksum == nil ? "no-checksum" : "checksum")
        }

        var orderedIndexes: [Int] = []
        var consumed = Set<Int>()
        var expectedCursor: String? = nil
        var finalPageCount = 0
        while let indexes = requestCursorIndexes[cursorKey(expectedCursor)], indexes.count == 1,
              let index = indexes.first, consumed.insert(index).inserted {
            let item = decoded[index]
            orderedIndexes.append(index)
            if item.response.hasMore {
                guard let nextCursor = item.response.cursor else {
                    break
                }
                expectedCursor = nextCursor
            } else {
                finalPageCount += 1
                expectedCursor = nil
                break
            }
        }
        appendChain(String(decoded.count), String(consumed.count))
        appendChain("1", String(finalPageCount))
        appendChain("final", orderedIndexes.last.map { decoded[$0].response.hasMore ? "partial" : "final" })

        let traversalIndexes: [Int]
        if consumed.count == decoded.count {
            traversalIndexes = orderedIndexes
        } else {
            traversalIndexes = decoded.indices.sorted { left, right in
                let leftKey = cursorKey(decoded[left].receipt.requestCursor)
                let rightKey = cursorKey(decoded[right].receipt.requestCursor)
                return leftKey.utf8.lexicographicallyPrecedes(rightKey.utf8)
            }
        }

        var returnedRecordCount = 0
        var recordIdentitiesHex: [String] = []
        var receivedRowChecksums: [String] = []
        var computedRowChecksums: [String] = []
        var entries: [(identity: Data, digest: ChecksumObject)] = []
        var schemaCache: [String: [String: LocalSchemaTable]] = [:]
        for index in traversalIndexes {
            let item = decoded[index]
            returnedRecordCount += item.response.records.count
            let schemaKey = "\(item.request.schema.version):\(item.request.schema.hash)"
            let tables: [String: LocalSchemaTable]
            if let cached = schemaCache[schemaKey] {
                tables = cached
            } else {
                guard let archived = try SynchroMeta.getArchivedSchemaTables(
                    db,
                    version: item.request.schema.version,
                    hash: item.request.schema.hash
                ) else {
                    throw SynchroError.invalidResponse(message: "rebuild receipt schema archive is missing")
                }
                var tableMap: [String: LocalSchemaTable] = [:]
                for table in archived {
                    guard tableMap.updateValue(table, forKey: table.tableID) == nil else {
                        throw SynchroError.invalidResponse(message: "rebuild receipt schema archive is invalid")
                    }
                }
                tables = tableMap
                schemaCache[schemaKey] = tableMap
            }
            for record in item.response.records {
                guard let table = tables[record.table] else {
                    throw SynchroError.invalidResponse(message: "rebuild receipt table metadata is missing")
                }
                let digest: (identity: Data, checksum: ChecksumObject)
                do {
                    digest = try Integrity.rowDigest(
                        schemaHash: item.request.schema.hash,
                        table: table,
                        pk: record.pk,
                        row: record.row,
                        serverVersion: record.serverVersion
                    )
                } catch {
                    throw SynchroError.invalidResponse(message: "rebuild receipt record metadata is invalid")
                }
                recordIdentitiesHex.append(hexString(digest.identity))
                receivedRowChecksums.append(checksumKey(record.rowChecksum))
                computedRowChecksums.append(checksumKey(digest.checksum))
                entries.append((identity: digest.identity, digest: digest.checksum))
            }
        }

        let finalIndexes = decoded.indices.filter { !decoded[$0].response.hasMore }
        let finalChecksum: ChecksumObject? = finalIndexes.count == 1 ? decoded[finalIndexes[0]].response.checksum : nil
        let schemaHashes = Set(decoded.map { $0.request.schema.hash })
        let computedScopeChecksum = schemaHashes.count == 1 ? checksumKey(try Integrity.scopeDigest(
            schemaHash: schemaHashes.first!,
            scopeID: first.scopeID,
            entries: entries.sorted { $0.identity.lexicographicallyPrecedes($1.identity) }
        )) : nil
        let scope = try SynchroMeta.getScope(db, scopeID: first.scopeID)
        let storedScopeChecksum = try scope?.checksum.map {
            checksumKey(try decodeExactReceiptJSON($0, as: ChecksumObject.self, decoder: decoder))
        }
        // A scope holds no local checksum until it computes one, and an absent
        // checksum is recorded as an empty value rather than a JSON object.
        let localScopeChecksum = try scope.flatMap { record -> String? in
            guard !record.localChecksum.isEmpty else { return nil }
            return checksumKey(try decodeExactReceiptJSON(record.localChecksum, as: ChecksumObject.self, decoder: decoder))
        }

        return RebuildReceiptInspection(
            rebuildIDFingerprint: TransportObservationCollector.cursorFingerprint(first.rebuildID),
            pageCount: receipts.count,
            returnedRecordCount: returnedRecordCount,
            requestChainExpected: requestChainExpected,
            requestChainObserved: requestChainObserved,
            recordIdentitiesHex: recordIdentitiesHex,
            receivedRowChecksums: receivedRowChecksums,
            computedRowChecksums: computedRowChecksums,
            computedScopeChecksum: computedScopeChecksum,
            finalScopeChecksum: finalChecksum.map(checksumKey),
            storedScopeChecksum: storedScopeChecksum,
            localScopeChecksum: localScopeChecksum
        )
    }

    private static func cursorFingerprint(_ value: String) -> String {
        TransportObservationCollector.cursorFingerprint(value)
    }

    private static func hexString(_ value: Data) -> String {
        Integrity.hexString(value)
    }

    private static func checksumKey(_ value: ChecksumObject) -> String {
        "\(value.algorithm):\(value.version):\(value.encoding):\(value.digest)"
    }

    private static func decodeExactReceiptJSON<T: Codable & Equatable>(
        _ source: String,
        as type: T.Type,
        decoder: JSONDecoder
    ) throws -> T {
        guard let data = source.data(using: .utf8) else {
            throw SynchroError.invalidResponse(message: "rebuild receipt JSON is invalid")
        }
        do {
            try Integrity.validateCanonicalWireJSON(data)
            try validateReceiptJSONShape(data, as: type)
            let value = try decoder.decode(type, from: data)
            return value
        } catch let error as SynchroError {
            // The specific rejection names which receipt member failed, and a
            // generic message cannot be acted on.
            throw error
        } catch {
            throw SynchroError.invalidResponse(message: "rebuild receipt JSON is invalid: \(error)")
        }
    }

    private static func validateReceiptJSONShape<T>(_ data: Data, as type: T.Type) throws {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw SynchroError.invalidResponse(message: "rebuild receipt JSON shape is invalid")
        }
        if type == RebuildRequest.self {
            try requireReceiptKeys(
                object,
                required: ["client_id", "client_generation", "schema", "scope", "rebuild_id", "limit"],
                optional: ["cursor"]
            )
            guard let schema = object["schema"] as? [String: Any] else {
                throw SynchroError.invalidResponse(message: "rebuild receipt request schema is invalid")
            }
            try requireReceiptKeys(schema, required: ["version", "hash"])
            return
        }
        if type == RebuildResponse.self {
            try requireReceiptKeys(
                object,
                required: ["scope", "records", "has_more"],
                optional: ["cursor", "final_scope_cursor", "checksum"]
            )
            guard let records = object["records"] as? [[String: Any]] else {
                throw SynchroError.invalidResponse(message: "rebuild receipt records are invalid")
            }
            for record in records {
                try requireReceiptKeys(
                    record,
                    required: ["table", "pk", "row", "row_checksum", "server_version"]
                )
                guard record["pk"] is [String: Any], record["row"] is [String: Any],
                      let checksum = record["row_checksum"] as? [String: Any] else {
                    throw SynchroError.invalidResponse(message: "rebuild receipt record shape is invalid")
                }
                try requireReceiptKeys(checksum, required: ["algorithm", "version", "encoding", "digest"])
            }
            if let checksum = object["checksum"], !(checksum is NSNull) {
                guard let checksum = checksum as? [String: Any] else {
                    throw SynchroError.invalidResponse(message: "rebuild receipt checksum shape is invalid")
                }
                try requireReceiptKeys(checksum, required: ["algorithm", "version", "encoding", "digest"])
            }
            return
        }
        if type == ChecksumObject.self {
            try requireReceiptKeys(object, required: ["algorithm", "version", "encoding", "digest"])
            return
        }
        throw SynchroError.invalidResponse(message: "rebuild receipt JSON type is invalid")
    }

    private static func requireReceiptKeys(
        _ object: [String: Any],
        required: Set<String>,
        optional: Set<String> = []
    ) throws {
        let keys = Set(object.keys)
        guard required.isSubset(of: keys), keys.isSubset(of: required.union(optional)) else {
            // The missing and unexpected members name the mismatch, because the
            // member set alone cannot be compared by a reader.
            let missing = required.subtracting(keys).sorted()
            let unexpected = keys.subtracting(required.union(optional)).sorted()
            throw SynchroError.invalidResponse(
                message: "rebuild receipt JSON members are invalid: missing \(missing), unexpected \(unexpected)"
            )
        }
    }

    private static func cursorKey(_ cursor: String?) -> String {
        cursor.map { "value:\($0)" } ?? "null"
    }

    private static func utf8Less(_ lhs: String, _ rhs: String) -> Bool {
        lhs.utf8.lexicographicallyPrecedes(rhs.utf8)
    }

}
