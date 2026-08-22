import Foundation
@preconcurrency import GRDB

struct StoredFieldValue: Codable, Sendable, Equatable {
    let fieldID: String
    let logicalType: String
    let kind: String
    let integerValue: Int64?
    let realValue: Double?
    let textValue: String?
    let blobValue: Data?

    var wireValue: AnyCodable {
        switch kind {
        case "null":
            return AnyCodable(NSNull())
        case "boolean":
            return AnyCodable(integerValue != 0)
        case "integer":
            if logicalType == "int64" {
                return AnyCodable(String(integerValue ?? 0))
            }
            return AnyCodable(integerValue ?? 0)
        case "real":
            return AnyCodable(realValue ?? 0)
        case "blob":
            let encoded = (blobValue ?? Data()).base64EncodedString()
                .replacingOccurrences(of: "+", with: "-")
                .replacingOccurrences(of: "/", with: "_")
                .replacingOccurrences(of: "=", with: "")
            return AnyCodable(encoded)
        default:
            return AnyCodable(textValue ?? "")
        }
    }

    func immutableValue() throws -> AnyCodable {
        switch kind {
        case "null":
            return AnyCodable(NSNull())
        case "boolean":
            guard let integerValue, integerValue == 0 || integerValue == 1 else {
                throw SynchroError.invalidResponse(message: "stored mutation boolean value is invalid")
            }
            return AnyCodable(integerValue == 1)
        case "integer":
            guard let integerValue else {
                throw SynchroError.invalidResponse(message: "stored mutation integer value is invalid")
            }
            return logicalType == "int64" ? AnyCodable(String(integerValue)) : AnyCodable(integerValue)
        case "real":
            guard let realValue, realValue.isFinite else {
                throw SynchroError.invalidResponse(message: "stored mutation real value is invalid")
            }
            return AnyCodable(realValue)
        case "text":
            guard let textValue else {
                throw SynchroError.invalidResponse(message: "stored mutation text value is invalid")
            }
            return AnyCodable(textValue)
        case "blob":
            guard let blobValue else {
                throw SynchroError.invalidResponse(message: "stored mutation blob value is invalid")
            }
            let encoded = blobValue.base64EncodedString()
                .replacingOccurrences(of: "+", with: "-")
                .replacingOccurrences(of: "/", with: "_")
                .replacingOccurrences(of: "=", with: "")
            return AnyCodable(encoded)
        default:
            throw SynchroError.invalidResponse(message: "stored mutation value has an invalid kind")
        }
    }
}

struct PendingChange: Codable, Sendable, Equatable {
    let mutationID: String
    let localOrder: Int64
    let tableID: String?
    let recordID: String
    let tableName: String
    let pkFieldID: String?
    let pkLogicalType: String?
    let operation: String
    var baseUpdatedAt: String?
    let clientUpdatedAt: String
    let authoredSchemaVersion: Int64?
    let authoredSchemaHash: String?
    var lifecycleState: String
    let sourceKind: String
    var dependencyMutationID: String?
    var normalizedMutationID: String?
    let sealedBatchID: String?
    let sealedOrdinal: Int64?
    var fieldValuesByID: [String: StoredFieldValue] = [:]

    // Kept for source compatibility with pre-ledger inspection code.
    var localRevision: Int64 { localOrder }

    init(
        mutationID: String,
        localOrder: Int64,
        tableID: String?,
        recordID: String,
        tableName: String,
        pkFieldID: String?,
        pkLogicalType: String?,
        operation: String,
        baseUpdatedAt: String?,
        clientUpdatedAt: String,
        authoredSchemaVersion: Int64?,
        authoredSchemaHash: String?,
        lifecycleState: String,
        sourceKind: String,
        dependencyMutationID: String?,
        normalizedMutationID: String?,
        sealedBatchID: String?,
        sealedOrdinal: Int64?,
        fieldValuesByID: [String: StoredFieldValue] = [:]
    ) {
        self.mutationID = mutationID
        self.localOrder = localOrder
        self.tableID = tableID
        self.recordID = recordID
        self.tableName = tableName
        self.pkFieldID = pkFieldID
        self.pkLogicalType = pkLogicalType
        self.operation = operation == "create" ? "insert" : operation
        self.baseUpdatedAt = baseUpdatedAt
        self.clientUpdatedAt = clientUpdatedAt
        self.authoredSchemaVersion = authoredSchemaVersion
        self.authoredSchemaHash = authoredSchemaHash
        self.lifecycleState = lifecycleState
        self.sourceKind = sourceKind
        self.dependencyMutationID = dependencyMutationID
        self.normalizedMutationID = normalizedMutationID
        self.sealedBatchID = sealedBatchID
        self.sealedOrdinal = sealedOrdinal
        self.fieldValuesByID = fieldValuesByID
    }

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case localOrder = "local_order"
        case localRevision = "local_revision"
        case tableID = "table_id"
        case recordID = "record_id"
        case tableName = "table_name"
        case pkFieldID = "pk_field_id"
        case pkLogicalType = "pk_logical_type"
        case operation
        case baseUpdatedAt = "base_updated_at"
        case clientUpdatedAt = "client_updated_at"
        case authoredSchemaVersion = "authored_schema_version"
        case authoredSchemaHash = "authored_schema_hash"
        case lifecycleState = "lifecycle_state"
        case sourceKind = "source_kind"
        case dependencyMutationID = "dependency_mutation_id"
        case normalizedMutationID = "normalized_mutation_id"
        case sealedBatchID = "sealed_batch_id"
        case sealedOrdinal = "sealed_ordinal"
        case fieldValuesByID = "field_values"
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let tableName = try c.decode(String.self, forKey: .tableName)
        let recordID = try c.decode(String.self, forKey: .recordID)
        let operationText = try c.decode(String.self, forKey: .operation)
        let clientVersion = try c.decode(String.self, forKey: .clientUpdatedAt)
        let oldRevision = try c.decodeIfPresent(Int64.self, forKey: .localRevision) ?? 0
        let mutationID = try c.decodeIfPresent(String.self, forKey: .mutationID)
            ?? Integrity.stableUUID(
                domain: "synchro:v3:local-mutation-id:v1",
                values: [tableName, recordID, operationText, clientVersion, String(oldRevision)]
            )
        self.init(
            mutationID: mutationID,
            localOrder: try c.decodeIfPresent(Int64.self, forKey: .localOrder) ?? oldRevision,
            tableID: try c.decodeIfPresent(String.self, forKey: .tableID),
            recordID: recordID,
            tableName: tableName,
            pkFieldID: try c.decodeIfPresent(String.self, forKey: .pkFieldID),
            pkLogicalType: try c.decodeIfPresent(String.self, forKey: .pkLogicalType),
            operation: operationText,
            baseUpdatedAt: try c.decodeIfPresent(String.self, forKey: .baseUpdatedAt),
            clientUpdatedAt: clientVersion,
            authoredSchemaVersion: try c.decodeIfPresent(Int64.self, forKey: .authoredSchemaVersion),
            authoredSchemaHash: try c.decodeIfPresent(String.self, forKey: .authoredSchemaHash),
            lifecycleState: try c.decodeIfPresent(String.self, forKey: .lifecycleState) ?? "sealed",
            sourceKind: try c.decodeIfPresent(String.self, forKey: .sourceKind) ?? "legacy_import",
            dependencyMutationID: try c.decodeIfPresent(String.self, forKey: .dependencyMutationID),
            normalizedMutationID: try c.decodeIfPresent(String.self, forKey: .normalizedMutationID),
            sealedBatchID: try c.decodeIfPresent(String.self, forKey: .sealedBatchID),
            sealedOrdinal: try c.decodeIfPresent(Int64.self, forKey: .sealedOrdinal),
            fieldValuesByID: try c.decodeIfPresent([String: StoredFieldValue].self, forKey: .fieldValuesByID) ?? [:]
        )
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(mutationID, forKey: .mutationID)
        try c.encode(localOrder, forKey: .localOrder)
        try c.encodeIfPresent(tableID, forKey: .tableID)
        try c.encode(recordID, forKey: .recordID)
        try c.encode(tableName, forKey: .tableName)
        try c.encodeIfPresent(pkFieldID, forKey: .pkFieldID)
        try c.encodeIfPresent(pkLogicalType, forKey: .pkLogicalType)
        try c.encode(operation, forKey: .operation)
        try c.encodeIfPresent(baseUpdatedAt, forKey: .baseUpdatedAt)
        try c.encode(clientUpdatedAt, forKey: .clientUpdatedAt)
        try c.encodeIfPresent(authoredSchemaVersion, forKey: .authoredSchemaVersion)
        try c.encodeIfPresent(authoredSchemaHash, forKey: .authoredSchemaHash)
        try c.encode(lifecycleState, forKey: .lifecycleState)
        try c.encode(sourceKind, forKey: .sourceKind)
        try c.encodeIfPresent(dependencyMutationID, forKey: .dependencyMutationID)
        try c.encodeIfPresent(normalizedMutationID, forKey: .normalizedMutationID)
        try c.encodeIfPresent(sealedBatchID, forKey: .sealedBatchID)
        try c.encodeIfPresent(sealedOrdinal, forKey: .sealedOrdinal)
        try c.encode(fieldValuesByID, forKey: .fieldValuesByID)
    }

    init(row: Row, fieldValuesByID: [String: StoredFieldValue] = [:]) {
        self.init(
            mutationID: row["mutation_id"],
            localOrder: row["local_order"],
            tableID: row["table_id"],
            recordID: row["record_id"],
            tableName: row["table_name"],
            pkFieldID: row["pk_field_id"],
            pkLogicalType: row["pk_logical_type"],
            operation: row["operation"],
            baseUpdatedAt: row["base_version"],
            clientUpdatedAt: row["client_version"],
            authoredSchemaVersion: row["authored_schema_version"],
            authoredSchemaHash: row["authored_schema_hash"],
            lifecycleState: row["lifecycle_state"],
            sourceKind: row["source_kind"],
            dependencyMutationID: row["dependency_mutation_id"],
            normalizedMutationID: row["normalized_mutation_id"],
            sealedBatchID: row["sealed_batch_id"],
            sealedOrdinal: row["sealed_ordinal"],
            fieldValuesByID: fieldValuesByID
        )
    }
}

final class ChangeTracker: @unchecked Sendable {
    private let database: SynchroDatabase

    private struct LogicalRowIdentity: Hashable {
        let tableID: String
        let pkFieldID: String
        let pkLogicalType: String
        let recordID: String
    }

    init(database: SynchroDatabase) {
        self.database = database
    }

    func inspectPendingMutations() throws -> [PendingMutationInspection] {
        try inspectMutations(includeRejected: false)
    }

    func inspectRetainedMutations() throws -> [PendingMutationInspection] {
        try inspectMutations(includeRejected: true)
    }

    private func inspectMutations(includeRejected: Bool) throws -> [PendingMutationInspection] {
        try database.readTransaction { db in
            let rejectedState = includeRejected ? ", 'rejected'" : ""
            let rows = try Row.fetchAll(
                db,
                sql: """
                    SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                           pk_logical_type, operation, base_version, client_version,
                           authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                           dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                    FROM _synchro_pending_changes
                    WHERE lifecycle_state IN (
                        'unsealed', 'sealed', 'legacy_blocked', 'blocked_by_predecessor',
                        'superseded_before_send', 'cancelled_before_send'\(rejectedState)
                    )
                    ORDER BY local_order
                    """
            )
            return try rows.map { row in
                let change = PendingChange(row: row)
                guard let tableID = change.tableID,
                      let primaryKeyFieldID = change.pkFieldID,
                      let primaryKeyLogicalType = change.pkLogicalType,
                      let schemaVersion = change.authoredSchemaVersion,
                      let schemaHash = change.authoredSchemaHash,
                      let operation = inspectableOperation(for: change.operation),
                      let status = localMutationStatus(for: change.lifecycleState) else {
                    throw SynchroError.invalidResponse(message: "stored mutation cannot be inspected")
                }
                let authoredFields = try loadFieldValues(db, mutationID: change.mutationID)
                    .values
                    .sorted { $0.fieldID < $1.fieldID }
                    .map { value in
                        AuthoredMutationField(
                            fieldID: value.fieldID,
                            logicalType: value.logicalType,
                            value: try value.immutableValue()
                        )
                    }
                return PendingMutationInspection(
                    mutationID: change.mutationID,
                    localOrder: change.localOrder,
                    tableID: tableID,
                    tableName: change.tableName,
                    recordID: change.recordID,
                    primaryKeyFieldID: primaryKeyFieldID,
                    primaryKeyLogicalType: primaryKeyLogicalType,
                    operation: operation,
                    authoredSchema: SchemaRef(version: schemaVersion, hash: schemaHash),
                    baseVersion: change.baseUpdatedAt,
                    clientVersion: change.clientUpdatedAt,
                    status: status,
                    sourceKind: change.sourceKind,
                    dependsOnMutationID: change.dependencyMutationID,
                    normalizedMutationID: change.normalizedMutationID,
                    sealedBatchID: change.sealedBatchID,
                    sealedOrdinal: change.sealedOrdinal.flatMap(Int.init),
                    authoredFields: authoredFields
                )
            }
        }
    }

    func pendingChanges(limit: Int = 100) throws -> [PendingChange] {
        try database.writeTransaction { db in
            try normalizeUnsealedChains(db)
            return try pendingChanges(db, limit: limit)
        }
    }

    func pendingChanges(_ db: GRDB.Database, limit: Int) throws -> [PendingChange] {
        try normalizeUnsealedChains(db)
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                       pk_logical_type, operation, base_version, client_version,
                       authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                       dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                FROM _synchro_pending_changes
                WHERE lifecycle_state = 'unsealed'
                  AND dependency_mutation_id IS NULL
                  AND (operation = 'insert' OR base_version IS NOT NULL)
                ORDER BY local_order ASC
                LIMIT ?
                """,
            arguments: [limit]
        )
        return try rows.map { row in
            try PendingChange(row: row, fieldValuesByID: loadFieldValues(db, mutationID: row["mutation_id"]))
        }
    }

    func hydratePendingForPush(pending: [PendingChange], syncedTables: [LocalSchemaTable]) throws -> [PushRecord] {
        try database.readTransaction { db in
            try hydratePendingForPush(db, pending: pending, syncedTables: syncedTables)
        }
    }

    func hydratePendingForPush(
        _ db: GRDB.Database,
        pending: [PendingChange],
        syncedTables: [LocalSchemaTable]
    ) throws -> [PushRecord] {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableID, $0) })
        return try pending.map { change in
            guard let tableID = change.tableID,
                  let schema = tableMap[tableID] else {
                throw SynchroError.invalidResponse(message: "cannot hydrate pending change for unknown table \(change.tableName)")
            }
            guard change.operation == "insert" || change.operation == "update" || change.operation == "delete" else {
                throw SynchroError.invalidResponse(message: "cannot hydrate unknown local operation \(change.operation)")
            }
            if change.operation != "insert" && (change.baseUpdatedAt ?? "").isEmpty {
                throw SynchroError.invalidResponse(message: "cannot hydrate mutation without a server version for \(change.tableName)/\(change.recordID)")
            }
            let values = change.fieldValuesByID
            if change.operation != "delete" && values.isEmpty {
                throw SynchroError.invalidResponse(message: "cannot hydrate mutation without immutable authored fields for \(change.tableName)/\(change.recordID)")
            }
            let data = Dictionary(uniqueKeysWithValues: schema.columns.compactMap { column in
                values[column.fieldID].map { (column.name, $0.wireValue) }
            })
            return PushRecord(
                id: change.recordID,
                tableName: change.tableName,
                operation: change.operation,
                data: change.operation == "delete" ? nil : data,
                clientUpdatedAt: change.clientUpdatedAt,
                baseUpdatedAt: change.operation == "insert" ? nil : change.baseUpdatedAt,
                localRevision: change.localOrder,
                fieldValuesByID: values
            )
        }
    }

    func fieldValues(_ db: GRDB.Database, mutationID: String) throws -> [String: StoredFieldValue] {
        try loadFieldValues(db, mutationID: mutationID)
    }

    func markPendingAsSealed(_ db: GRDB.Database, batchID: String, pending: [PendingChange]) throws {
        for (ordinal, change) in pending.enumerated() {
            try db.execute(
                sql: "INSERT INTO _synchro_push_batch_members (batch_id, mutation_id, ordinal) VALUES (?, ?, ?)",
                arguments: [batchID, change.mutationID, ordinal]
            )
            try db.execute(
                sql: """
                    UPDATE _synchro_pending_changes
                    SET lifecycle_state = 'sealed', sealed_batch_id = ?, sealed_ordinal = ?, updated_at = ?
                    WHERE mutation_id = ? AND lifecycle_state = 'unsealed'
                    """,
                arguments: [batchID, ordinal, timestampNow(), change.mutationID]
            )
            guard db.changesCount == 1 else {
                throw SynchroError.invalidResponse(message: "mutation was not sealed exactly once")
            }
        }
    }

    func entriesForBatch(_ db: GRDB.Database, batchID: String) throws -> [PendingChange] {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT l.mutation_id, l.local_order, l.table_id, l.record_id, l.table_name, l.pk_field_id,
                       l.pk_logical_type, l.operation, l.base_version, l.client_version,
                       l.authored_schema_version, l.authored_schema_hash, l.lifecycle_state, l.source_kind,
                       l.dependency_mutation_id, l.normalized_mutation_id, l.sealed_batch_id, l.sealed_ordinal
                FROM _synchro_push_batch_members m
                JOIN _synchro_pending_changes l ON l.mutation_id = m.mutation_id
                WHERE m.batch_id = ?
                ORDER BY m.ordinal
                """,
            arguments: [batchID]
        )
        return try rows.map { row in
            try PendingChange(row: row, fieldValuesByID: loadFieldValues(db, mutationID: row["mutation_id"]))
        }
    }

    func markAccepted(
        _ db: GRDB.Database,
        mutationID: String,
        acceptedJSON: String
    ) throws {
        try db.execute(
            sql: """
                UPDATE _synchro_pending_changes
                SET lifecycle_state = 'accepted', accepted_json = ?, updated_at = ?
                WHERE mutation_id = ? AND lifecycle_state IN ('sealed', 'unsealed', 'legacy_blocked')
                """,
            arguments: [acceptedJSON, timestampNow(), mutationID]
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "accepted mutation identity is not mutable")
        }
    }

    func markRejected(
        _ db: GRDB.Database,
        mutationID: String,
        rejectedJSON: String
    ) throws {
        try db.execute(
            sql: """
                UPDATE _synchro_pending_changes
                SET lifecycle_state = 'rejected', rejected_json = ?, updated_at = ?
                WHERE mutation_id = ? AND lifecycle_state IN ('sealed', 'unsealed', 'legacy_blocked')
                """,
            arguments: [rejectedJSON, timestampNow(), mutationID]
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "rejected mutation identity is not mutable")
        }
    }

    func successors(_ db: GRDB.Database, predecessorID: String) throws -> [PendingChange] {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                       pk_logical_type, operation, base_version, client_version,
                       authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                       dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                FROM _synchro_pending_changes
                WHERE dependency_mutation_id = ? AND lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')
                ORDER BY local_order
                """,
            arguments: [predecessorID]
        )
        return try rows.map { row in
            try PendingChange(row: row, fieldValuesByID: loadFieldValues(db, mutationID: row["mutation_id"]))
        }
    }

    func refreshUnsealedSuccessor(
        _ db: GRDB.Database,
        mutationID: String,
        serverVersion: String
    ) throws {
        try db.execute(
            sql: """
                UPDATE _synchro_pending_changes
                SET base_version = ?, dependency_mutation_id = NULL, updated_at = ?
                WHERE mutation_id = ? AND lifecycle_state = 'unsealed'
                  AND operation IN ('update', 'delete')
                """,
            arguments: [serverVersion, timestampNow(), mutationID]
        )
    }

    func blockDependents(_ db: GRDB.Database, predecessorID: String) throws {
        try db.execute(
            sql: """
                WITH RECURSIVE descendants(mutation_id) AS (
                    SELECT mutation_id
                    FROM _synchro_pending_changes
                    WHERE dependency_mutation_id = ?
                      AND lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')
                    UNION ALL
                    SELECT child.mutation_id
                    FROM _synchro_pending_changes child
                    JOIN descendants parent ON child.dependency_mutation_id = parent.mutation_id
                    WHERE child.lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')
                )
                UPDATE _synchro_pending_changes
                SET lifecycle_state = 'blocked_by_predecessor', updated_at = ?
                WHERE mutation_id IN (SELECT mutation_id FROM descendants)
                  AND lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')
                """,
            arguments: [predecessorID, timestampNow()]
        )
    }

    func laterUnresolved(
        _ db: GRDB.Database,
        after predecessor: PendingChange
    ) throws -> [PendingChange] {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                       pk_logical_type, operation, base_version, client_version,
                       authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                       dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                FROM _synchro_pending_changes
                WHERE table_id = ? AND pk_field_id = ? AND pk_logical_type = ? AND record_id = ?
                  AND local_order > ?
                  AND lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')
                ORDER BY local_order
                """,
            arguments: [
                predecessor.tableID,
                predecessor.pkFieldID,
                predecessor.pkLogicalType,
                predecessor.recordID,
                predecessor.localOrder,
            ]
        )
        return try rows.map { row in
            try PendingChange(row: row, fieldValuesByID: loadFieldValues(db, mutationID: row["mutation_id"]))
        }
    }

    func removePending(entries: [PendingChange]) throws {
        guard !entries.isEmpty else { return }
        try database.writeTransaction { db in
            for entry in entries {
                try db.execute(
                    sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'cancelled_before_send', updated_at = ? WHERE mutation_id = ? AND lifecycle_state = 'unsealed'",
                    arguments: [timestampNow(), entry.mutationID]
                )
            }
        }
    }

    func removePendingByIDs(entries: [(tableName: String, recordID: String)]) throws {
        guard !entries.isEmpty else { return }
        try database.writeTransaction { db in
            for entry in entries {
                try db.execute(
                    sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'cancelled_before_send', updated_at = ? WHERE table_name = ? AND record_id = ? AND lifecycle_state = 'unsealed'",
                    arguments: [timestampNow(), entry.tableName, entry.recordID]
                )
            }
        }
    }

    func clearTable(table: String) throws {
        try database.writeTransaction { db in
            try db.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'cancelled_before_send', updated_at = ? WHERE table_name = ? AND lifecycle_state = 'unsealed'",
                arguments: [timestampNow(), table]
            )
        }
    }

    func clearAll() throws {
        try database.writeTransaction { db in
            try db.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'cancelled_before_send', updated_at = ? WHERE lifecycle_state = 'unsealed'",
                arguments: [timestampNow()]
            )
        }
    }

    func hasPendingChanges() throws -> Bool {
        try pendingChangeCount() > 0
    }

    func hasUnsealedChanges() throws -> Bool {
        try database.readTransaction { db in
            try Bool.fetchOne(
                db,
                sql: "SELECT EXISTS(SELECT 1 FROM _synchro_pending_changes WHERE lifecycle_state = 'unsealed')"
            ) ?? false
        }
    }

    func pendingChangeCount() throws -> Int {
        try database.writeTransaction { db in
            try normalizeUnsealedChains(db)
            return try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')"
            ) ?? 0
        }
    }

    // MARK: - Immutable capture normalization

    private func normalizeUnsealedChains(_ db: GRDB.Database) throws {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                       pk_logical_type, operation, base_version, client_version,
                       authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                       dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                FROM _synchro_pending_changes
                WHERE lifecycle_state = 'unsealed' AND sealed_batch_id IS NULL
                ORDER BY local_order
                """
        )
        let entries = try rows.map { row in
            try PendingChange(row: row, fieldValuesByID: loadFieldValues(db, mutationID: row["mutation_id"]))
        }
        let grouped = Dictionary(grouping: entries) { entry -> LogicalRowIdentity? in
            guard let tableID = entry.tableID,
                  let pkFieldID = entry.pkFieldID,
                  let pkLogicalType = entry.pkLogicalType else { return nil }
            return LogicalRowIdentity(
                tableID: tableID,
                pkFieldID: pkFieldID,
                pkLogicalType: pkLogicalType,
                recordID: entry.recordID
            )
        }

        for (identity, groupedEntries) in grouped {
            guard identity != nil else { continue }
            let chain = groupedEntries.sorted { $0.localOrder < $1.localOrder }
            guard chain.count > 1,
                  chain.first?.dependencyMutationID == nil,
                  chain.dropFirst().enumerated().allSatisfy({ index, entry in
                      entry.dependencyMutationID == chain[index].mutationID
                  }) else {
                continue
            }

            // A normalized payload must never combine two schema bindings. Keep
            // the predecessor sendable and retain every successor dependency.
            let schemaRefs = Set(chain.map { "\($0.authoredSchemaVersion ?? 0):\($0.authoredSchemaHash ?? "")" })
            if schemaRefs.count != 1 {
                if let deleteIndex = chain.firstIndex(where: { $0.operation == "delete" }), deleteIndex < chain.count - 1 {
                    for successor in chain[(deleteIndex + 1)...] {
                        try db.execute(
                            sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'blocked_by_predecessor', updated_at = ? WHERE mutation_id = ? AND lifecycle_state = 'unsealed'",
                            arguments: [timestampNow(), successor.mutationID]
                        )
                    }
                }
                continue
            }

            try normalize(chain, db: db)
        }

        // A delete has no resurrection operation. Keep the delete sendable and
        // block every later action using the typed logical row identity.
        let current = try loadUnsealedEntries(db)
        let currentGrouped = Dictionary(grouping: current) { entry -> LogicalRowIdentity? in
            guard let tableID = entry.tableID,
                  let pkFieldID = entry.pkFieldID,
                  let pkLogicalType = entry.pkLogicalType else { return nil }
            return LogicalRowIdentity(tableID: tableID, pkFieldID: pkFieldID, pkLogicalType: pkLogicalType, recordID: entry.recordID)
        }
        for (identity, entriesForIdentity) in currentGrouped where identity != nil {
            let chain = entriesForIdentity.sorted { $0.localOrder < $1.localOrder }
            guard let deleteIndex = chain.firstIndex(where: { $0.operation == "delete" }),
                  deleteIndex < chain.count - 1 else { continue }
            for successor in chain[(deleteIndex + 1)...] where successor.lifecycleState == "unsealed" {
                try db.execute(
                    sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'blocked_by_predecessor', updated_at = ? WHERE mutation_id = ? AND lifecycle_state = 'unsealed'",
                    arguments: [timestampNow(), successor.mutationID]
                )
            }
        }
    }

    private func loadUnsealedEntries(_ db: GRDB.Database) throws -> [PendingChange] {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                       pk_logical_type, operation, base_version, client_version,
                       authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                       dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                FROM _synchro_pending_changes WHERE lifecycle_state = 'unsealed' ORDER BY local_order
                """
        )
        return try rows.map { row in
            try PendingChange(row: row, fieldValuesByID: loadFieldValues(db, mutationID: row["mutation_id"]))
        }
    }

    private func normalize(_ chain: [PendingChange], db: GRDB.Database) throws {
        guard let first = chain.first, let last = chain.last else { return }
        let deleteIndex = chain.firstIndex(where: { $0.operation == "delete" })
        if first.operation == "insert", let deleteIndex, deleteIndex == chain.count - 1 {
            let cancellationID = UUID().uuidString.lowercased()
            for entry in chain {
                try markSource(db, entry: entry, state: "cancelled_before_send", normalizedID: cancellationID)
            }
            return
        }
        if let deleteIndex, deleteIndex != chain.count - 1 {
            // The later blocker pass handles the suffix.  The delete itself must
            // retain its original base and identity.
            return
        }

        var finalValues: [String: StoredFieldValue] = [:]
        for entry in chain {
            for (fieldID, value) in entry.fieldValuesByID {
                finalValues[fieldID] = value
            }
        }
        let operation: String
        switch first.operation {
        case "insert": operation = "insert"
        case "update": operation = deleteIndex == nil ? "update" : "delete"
        default: return
        }
        let normalizedID = UUID().uuidString.lowercased()
        let now = timestampNow()
        try db.execute(
            sql: """
                    INSERT INTO _synchro_pending_changes
                    (mutation_id, capture_uuid, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                     operation, authored_schema_version, authored_schema_hash, base_version, client_version,
                     lifecycle_state, source_kind, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unsealed', 'normalized', ?, ?)
                """,
            arguments: [
                normalizedID, normalizedID, first.tableID, first.tableName, first.recordID,
                first.pkFieldID, first.pkLogicalType, operation, first.authoredSchemaVersion,
                first.authoredSchemaHash, operation == "insert" ? nil : first.baseUpdatedAt,
                last.clientUpdatedAt, now, now,
            ]
        )
        for value in finalValues.values where operation != "delete" {
            try insertValue(db, mutationID: normalizedID, value: value)
        }
        for entry in chain {
            try markSource(db, entry: entry, state: "superseded_before_send", normalizedID: normalizedID)
        }
    }

    private func markSource(_ db: GRDB.Database, entry: PendingChange, state: String, normalizedID: String?) throws {
        try db.execute(
            sql: "UPDATE _synchro_pending_changes SET lifecycle_state = ?, normalized_mutation_id = ?, updated_at = ? WHERE mutation_id = ? AND lifecycle_state = 'unsealed'",
            arguments: [state, normalizedID, timestampNow(), entry.mutationID]
        )
    }

    private func insertValue(_ db: GRDB.Database, mutationID: String, value: StoredFieldValue) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_mutation_values
                    (mutation_id, field_id, logical_type, value_kind, value_integer, value_real, value_text, value_blob)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
            arguments: [mutationID, value.fieldID, value.logicalType, value.kind, value.integerValue, value.realValue, value.textValue, value.blobValue]
        )
    }

    private func loadFieldValues(_ db: GRDB.Database, mutationID: String) throws -> [String: StoredFieldValue] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT field_id, logical_type, value_kind, value_integer, value_real, value_text, value_blob FROM _synchro_mutation_values WHERE mutation_id = ? ORDER BY field_id",
            arguments: [mutationID]
        )
        return Dictionary(uniqueKeysWithValues: rows.map { row in
            let value = StoredFieldValue(
                fieldID: row["field_id"],
                logicalType: row["logical_type"],
                kind: row["value_kind"],
                integerValue: row["value_integer"],
                realValue: row["value_real"],
                textValue: row["value_text"],
                blobValue: row["value_blob"]
            )
            return (value.fieldID, value)
        })
    }

    private func localMutationStatus(for lifecycleState: String) -> LocalMutationStatus? {
        switch lifecycleState {
        case "unsealed": return .pending
        case "sealed": return .sealed
        case "rejected": return .serverRejected
        case "superseded_before_send": return .supersededBeforeSend
        case "cancelled_before_send": return .cancelledBeforeSend
        case "legacy_blocked", "blocked_by_predecessor": return .blockedByPredecessor
        default: return nil
        }
    }

    private func inspectableOperation(for operation: String) -> Operation? {
        switch operation {
        case "insert": return .insert
        case "update": return .update
        case "delete": return .delete
        default: return nil
        }
    }

    private func timestampNow() -> String {
        ISO8601DateFormatter().string(from: Date())
    }
}
