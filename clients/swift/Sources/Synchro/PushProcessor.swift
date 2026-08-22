import Foundation
@preconcurrency import GRDB

final class PushProcessor: @unchecked Sendable {
    private struct SealedPushBatch {
        let request: PushRequest
        let requestJSON: String
        let pending: [PendingChange]
        let syncedTables: [LocalSchemaTable]
    }

    private let database: SynchroDatabase
    private let changeTracker: ChangeTracker
    private let encoder = JSONEncoder.synchroEncoder()
    private let decoder = JSONDecoder.synchroDecoder()

    init(database: SynchroDatabase, changeTracker: ChangeTracker) {
        self.database = database
        self.changeTracker = changeTracker
    }

    struct PushOutcome: Sendable {
        let response: PushResponse
        let conflicts: [ConflictEvent]
    }

    func processPush(
        httpClient: HttpClient,
        clientID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        syncedTables: [LocalSchemaTable],
        batchSize: Int = 100,
        expectedBatchID: String? = nil
    ) async throws -> PushOutcome? {
        guard let batch = try loadOrSealBatch(
            clientID: clientID,
            clientGeneration: clientGeneration,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            syncedTables: syncedTables,
            batchSize: batchSize,
            expectedBatchID: expectedBatchID
        ) else { return nil }

        let network: (response: PushResponse, body: Data)
        do {
            network = try await httpClient.pushWithBody(request: batch.request, bodyJSON: batch.requestJSON)
        } catch let error as BindingRenewalError {
            // The request binding failed before a mutation outcome existed. Keep
            // this exact batch immutable until SyncEngine installs the new binding.
            try database.writeTransaction { db in
                try markBatchRenewalRequired(db, batchID: batch.request.batchID)
            }
            throw error
        }
        let response = network.response
        try response.validate(for: batch.request)
        let sentPending = Dictionary(uniqueKeysWithValues: zip(batch.request.mutations, batch.pending).map {
            ($0.mutationID, $1)
        })

        let reconciliation = try database.writeSyncLockedTransaction { db in
            let conflicts = try applyAcceptedInTransaction(
                db,
                accepted: response.accepted,
                syncedTables: batch.syncedTables,
                currentTables: syncedTables,
                sentPending: sentPending,
                outcomeJSONByID: try exactObjectJSONMap(
                    network.body,
                    key: "accepted",
                    type: AcceptedMutation.self,
                    id: \.mutationID
                ),
                historicalSchema: batch.request.schema,
                historicalTables: batch.syncedTables
            )
            let rejectedOutcome = try applyRejectedOutcomeInTransaction(
                db,
                rejected: response.rejected,
                syncedTables: batch.syncedTables,
                currentTables: syncedTables,
                sentPending: sentPending,
                outcomeJSONByID: try exactObjectJSONMap(
                    network.body,
                    key: "rejected",
                    type: RejectedMutation.self,
                    id: \.mutationID
                ),
                originalMutationJSONByID: try exactObjectJSONMap(
                    Data(batch.requestJSON.utf8),
                    key: "mutations",
                    type: Mutation.self,
                    id: \.mutationID
                ),
                historicalSchema: batch.request.schema,
                historicalTables: batch.syncedTables
            )
            try completeBatch(db, batchID: batch.request.batchID)
            try SynchroMeta.clearMatchingBackoffRecord(
                db,
                resumeState: .pushing,
                workIdentity: batch.request.batchID
            )
            return (conflicts, rejectedOutcome)
        }

        return PushOutcome(response: response, conflicts: reconciliation.0 + reconciliation.1.conflicts)
    }

    private func buildMutations(
        _ db: GRDB.Database,
        from pending: [PendingChange],
        schemaVersion: Int64,
        schemaHash: String,
        syncedTables: [LocalSchemaTable]
    ) throws -> [Mutation] {
        let requestSchema = SchemaRef(version: schemaVersion, hash: schemaHash)
        return try pending.map { change in
            guard let tableID = change.tableID,
                  let pkFieldID = change.pkFieldID,
                  let pkLogicalType = change.pkLogicalType else {
                throw SynchroError.invalidResponse(message: "mutation ledger lacks immutable schema identity")
            }
            let authoredVersion = change.authoredSchemaVersion ?? schemaVersion
            let authoredHash = change.authoredSchemaHash ?? schemaHash
            guard authoredVersion > 0, !authoredHash.isEmpty else {
                throw SynchroError.invalidResponse(message: "mutation ledger lacks authored schema identity")
            }
            let authoredSchema = SchemaRef(version: authoredVersion, hash: authoredHash)
            let schema = try historicalTable(
                db,
                tableID: tableID,
                schema: authoredSchema,
                sealedSchema: authoredSchema == requestSchema ? requestSchema : nil,
                sealedTables: authoredSchema == requestSchema ? syncedTables : nil
            )
            let columns: [String: AnyCodable]? = change.operation == "delete"
                ? nil
                : Dictionary(uniqueKeysWithValues: change.fieldValuesByID.values.map { ($0.fieldID, $0.wireValue) })
            try validateCapturedMutation(change, schema: schema)
            return Mutation(
                mutationID: change.mutationID,
                table: tableID,
                op: try mutationOperation(for: change.operation),
                pk: [pkFieldID: AnyCodable(try primaryKeyValue(change.recordID, logicalType: pkLogicalType, tableName: schema.tableName))],
                authoredSchema: authoredSchema,
                baseVersion: change.operation == "insert" ? nil : change.baseUpdatedAt,
                clientVersion: change.clientUpdatedAt,
                columns: columns
            )
        }
    }

    private func mutationOperation(for operation: String) throws -> Operation {
        switch operation {
        case "insert": return .insert
        case "update": return .update
        case "delete": return .delete
        default:
            throw SynchroError.invalidResponse(message: "unknown local operation \(operation)")
        }
    }

    private func primaryKeyValue(_ recordID: String, logicalType: String, tableName: String) throws -> Any {
        switch logicalType {
        case "string": return recordID
        case "int":
            guard let value = Int64(recordID), value >= Int64(Int32.min), value <= Int64(Int32.max) else {
                throw SynchroError.invalidResponse(message: "invalid integer primary key for \(tableName)")
            }
            return value
        case "int64":
            guard recordID == "0" || (recordID.first != "-" ? recordID.first != "0" : recordID.dropFirst().first != "0"),
                  !recordID.isEmpty,
                  Int64(recordID) != nil,
                  recordID.filter({ $0 == "-" }).count <= 1,
                  recordID.dropFirst(recordID.first == "-" ? 1 : 0).allSatisfy(\.isNumber) else {
                throw SynchroError.invalidResponse(message: "invalid int64 primary key for \(tableName)")
            }
            return recordID
        default:
            throw SynchroError.invalidResponse(message: "unsupported primary key type for \(tableName)")
        }
    }

    private func validateCapturedMutation(_ change: PendingChange, schema: LocalSchemaTable) throws {
        guard let pkFieldID = change.pkFieldID,
              let pkType = change.pkLogicalType,
              pkFieldID == schema.primaryKeyFieldID,
              let pkField = schema.columns.first(where: { $0.fieldID == pkFieldID }),
              pkField.logicalType == pkType else {
            throw SynchroError.invalidResponse(message: "mutation ledger primary-key identity is invalid")
        }
        do {
            try Integrity.validateTypedValue(
                AnyCodable(try primaryKeyValue(change.recordID, logicalType: pkType, tableName: schema.tableName)),
                field: pkField,
                requirePresent: true
            )
            try Integrity.validateCanonicalClientVersion(change.clientUpdatedAt)
        } catch {
            throw SynchroError.invalidResponse(message: "mutation contains a noncanonical primary key or client version")
        }

        let values = change.fieldValuesByID
        if change.operation != "delete" && values.isEmpty {
            throw SynchroError.invalidResponse(message: "mutation has no immutable authored values")
        }
        for value in values.values {
            guard let field = schema.columns.first(where: { $0.fieldID == value.fieldID }),
                  field.logicalType == value.logicalType,
                  change.operation == "delete" || field.writable else {
                throw SynchroError.invalidResponse(message: "mutation field type changed before sealing")
            }
            guard storedValueShapeIsValid(value) else {
                throw SynchroError.invalidResponse(message: "mutation contains an invalid stored portable value")
            }
            do {
                try Integrity.validateTypedValue(value.wireValue, field: field)
            } catch {
                throw SynchroError.invalidResponse(message: "mutation contains an invalid portable value")
            }
        }
    }

    private func storedValueShapeIsValid(_ value: StoredFieldValue) -> Bool {
        switch value.kind {
        case "null":
            return value.integerValue == nil
                && value.realValue == nil
                && value.textValue == nil
                && value.blobValue == nil
        case "boolean":
            return value.logicalType == "boolean"
                && (value.integerValue == 0 || value.integerValue == 1)
                && value.realValue == nil
                && value.textValue == nil
                && value.blobValue == nil
        case "integer":
            return (value.logicalType == "int" || value.logicalType == "int64")
                && value.integerValue != nil
                && value.realValue == nil
                && value.textValue == nil
                && value.blobValue == nil
        case "real":
            return value.logicalType == "float"
                && value.realValue?.isFinite == true
                && value.integerValue == nil
                && value.textValue == nil
                && value.blobValue == nil
        case "text":
            return ["string", "decimal", "datetime", "date", "time", "json"].contains(value.logicalType)
                && value.textValue != nil
                && value.integerValue == nil
                && value.realValue == nil
                && value.blobValue == nil
        case "blob":
            return value.logicalType == "bytes"
                && value.blobValue != nil
                && value.integerValue == nil
                && value.realValue == nil
                && value.textValue == nil
        default:
            return false
        }
    }

    private func loadOrSealBatch(
        clientID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        syncedTables: [LocalSchemaTable],
        batchSize: Int,
        expectedBatchID: String?
    ) throws -> SealedPushBatch? {
        try database.writeTransaction { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                    SELECT batch_id, request_json, pending_json, schema_json
                    FROM _synchro_push_batches
                    WHERE state = 'pending'
                    ORDER BY created_at, batch_id
                    LIMIT 2
                    """
            )
            guard rows.count <= 1 else {
                throw SynchroError.invalidResponse(message: "multiple active sealed push batches")
            }
            if let row = rows.first {
                if let expectedBatchID {
                    let batchID: String = row["batch_id"]
                    guard batchID == expectedBatchID else {
                        throw SynchroError.invalidResponse(message: "durable push retry identity does not match the sealed batch")
                    }
                }
                let batch = try decodeBatch(db, row: row)
                try validateSealedBatch(db, batch: batch, clientID: clientID)
                return batch
            }

            guard expectedBatchID == nil else {
                throw SynchroError.invalidResponse(message: "durable push retry batch is unavailable")
            }

            let pending = try changeTracker.pendingChanges(db, limit: batchSize)
            guard !pending.isEmpty else { return nil }
            let mutations = try buildMutations(
                db,
                from: pending,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                syncedTables: syncedTables
            )
            guard !mutations.isEmpty else { return nil }
            let request = PushRequest(
                clientID: clientID,
                clientGeneration: clientGeneration,
                batchID: UUID().uuidString.lowercased(),
                schema: SchemaRef(version: schemaVersion, hash: schemaHash),
                mutations: mutations
            )
            try request.validate(syncedTables: syncedTables)
            let requestJSON = try encodeString(request)
            let batch = SealedPushBatch(request: request, requestJSON: requestJSON, pending: pending, syncedTables: syncedTables)
            try validateSealedBatch(db, batch: batch, clientID: clientID)
            try db.execute(
                sql: """
                    INSERT INTO _synchro_push_batches
                        (batch_id, request_json, pending_json, schema_json, state, created_at)
                    VALUES (?, ?, ?, ?, 'pending', substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
                    """,
                arguments: [request.batchID, requestJSON, try encodeString(pending), try encodeString(syncedTables)]
            )
            try changeTracker.markPendingAsSealed(db, batchID: request.batchID, pending: pending)
            return batch
        }
    }

    private func decodeBatch(_ db: GRDB.Database, row: Row) throws -> SealedPushBatch {
        let requestJSON: String = row["request_json"]
        let pendingJSON: String = row["pending_json"]
        let schemaJSON: String = row["schema_json"]
        do {
            let request = try decoder.decode(PushRequest.self, from: Data(requestJSON.utf8))
            let storedMembers = try changeTracker.entriesForBatch(db, batchID: row["batch_id"])
            let pending: [PendingChange]
            if storedMembers.count == request.mutations.count {
                pending = storedMembers
            } else {
                // Batches sealed before v9 have no immutable membership rows.
                // Their request JSON and pending JSON remain the compatibility source.
                pending = try decoder.decode([PendingChange].self, from: Data(pendingJSON.utf8))
            }
            return SealedPushBatch(
                request: request,
                requestJSON: requestJSON,
                pending: pending,
                syncedTables: try decoder.decode([LocalSchemaTable].self, from: Data(schemaJSON.utf8))
            )
        } catch {
            throw SynchroError.invalidResponse(message: "stored sealed push batch is invalid")
        }
    }

    private func validateSealedBatch(
        _ db: GRDB.Database,
        batch: SealedPushBatch,
        clientID: String
    ) throws {
        guard batch.request.clientID == clientID,
              batch.request.mutations.count == batch.pending.count else {
            throw SynchroError.invalidResponse(message: "stored sealed push batch identity is invalid")
        }
        try batch.request.validate(syncedTables: batch.syncedTables)
        for (mutation, pending) in zip(batch.request.mutations, batch.pending) {
            guard mutation.mutationID == pending.mutationID else {
                throw SynchroError.invalidResponse(message: "stored push mutation identity is invalid")
            }
            let authoredTable = try historicalTable(
                db,
                tableID: mutation.table,
                schema: mutation.authoredSchema,
                sealedSchema: batch.request.schema,
                sealedTables: batch.syncedTables
            )
            try validateCapturedMutation(pending, schema: authoredTable)
            guard try self.mutation(from: pending, schema: authoredTable) == mutation else {
                throw SynchroError.invalidResponse(message: "stored push mutation payload is invalid")
            }
        }
    }

    private func validateRenewedRequest(
        _ db: GRDB.Database,
        request: PushRequest,
        syncedTables: [LocalSchemaTable]
    ) throws {
        try request.validate(syncedTables: syncedTables)
        for mutation in request.mutations {
            guard let source = try ledgerEntry(db, mutationID: mutation.mutationID),
                  source.tableID == mutation.table,
                  source.authoredSchemaVersion == mutation.authoredSchema.version,
                  source.authoredSchemaHash == mutation.authoredSchema.hash else {
                throw SynchroError.invalidResponse(message: "renewed mutation does not match its durable ledger identity")
            }
            let authoredTable = try historicalTable(
                db,
                tableID: mutation.table,
                schema: mutation.authoredSchema,
                sealedSchema: request.schema == mutation.authoredSchema ? request.schema : nil,
                sealedTables: request.schema == mutation.authoredSchema ? syncedTables : nil
            )
            try validateCapturedMutation(source, schema: authoredTable)
            let expected = try self.mutation(from: source, schema: authoredTable)
            guard expected == mutation else {
                throw SynchroError.invalidResponse(message: "renewed mutation changed its authored payload")
            }
        }
    }

    private func mutation(from pending: PendingChange, schema: LocalSchemaTable) throws -> Mutation {
        let tableID = pending.tableID ?? schema.tableID
        let pkFieldID = pending.pkFieldID ?? schema.primaryKeyFieldID
        let pkType = pending.pkLogicalType
            ?? schema.columns.first(where: { $0.fieldID == pkFieldID })?.logicalType
            ?? "string"
        return Mutation(
            mutationID: pending.mutationID,
            table: tableID,
            op: try mutationOperation(for: pending.operation),
            pk: [
                pkFieldID: AnyCodable(
                    try primaryKeyValue(
                        pending.recordID,
                        logicalType: pkType,
                        tableName: schema.tableName
                    )
                ),
            ],
            authoredSchema: SchemaRef(
                version: pending.authoredSchemaVersion ?? 1,
                hash: pending.authoredSchemaHash ?? ""
            ),
            baseVersion: pending.operation == "insert" ? nil : pending.baseUpdatedAt,
            clientVersion: pending.clientUpdatedAt,
            columns: pending.operation == "delete"
                ? nil
                : Dictionary(
                    uniqueKeysWithValues: pending.fieldValuesByID.values.map {
                        ($0.fieldID, $0.wireValue)
                    }
                )
        )
    }

    private func markBatchRenewalRequired(_ db: GRDB.Database, batchID: String) throws {
        try db.execute(
            sql: "UPDATE _synchro_push_batches SET state = 'renewal_required' WHERE batch_id = ? AND state = 'pending'",
            arguments: [batchID]
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "sealed push batch renewal state was not durable")
        }
    }

    private func encodeString<T: Encodable>(_ value: T) throws -> String {
        let data = try encoder.encode(value)
        guard let encoded = String(data: data, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "sealed push JSON is not UTF-8")
        }
        return encoded
    }

    private func completeBatch(_ db: GRDB.Database, batchID: String) throws {
        try db.execute(
            sql: """
                UPDATE _synchro_push_batches
                SET state = 'completed', completed_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
                WHERE batch_id = ? AND state = 'pending'
                """,
            arguments: [batchID]
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "sealed push batch completion was not durable")
        }
    }

    // MARK: - Internal (visible for testing)

    @discardableResult
    func renewSealedBatchesAfterBindingChange(
        clientID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        syncedTables: [LocalSchemaTable]
    ) throws -> Bool {
        try database.writeTransaction { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                    SELECT batch_id, request_json
                    FROM _synchro_push_batches
                    WHERE state = 'renewal_required'
                    ORDER BY created_at, batch_id
                    LIMIT 2
                    """
            )
            guard rows.count <= 1 else {
                throw SynchroError.invalidResponse(message: "multiple sealed push batches require renewal")
            }
            guard let row = rows.first else { return false }

            let oldBatchID: String = row["batch_id"]
            let oldRequest: PushRequest
            do {
                oldRequest = try decoder.decode(PushRequest.self, from: Data((row["request_json"] as String).utf8))
            } catch {
                throw SynchroError.invalidResponse(message: "sealed push renewal request is invalid")
            }
            let installedSchema = SchemaRef(version: schemaVersion, hash: schemaHash)
            try installedSchema.validate()
            guard oldRequest.clientID == clientID,
                  oldRequest.batchID == oldBatchID,
                  oldRequest.clientGeneration != clientGeneration || oldRequest.schema != installedSchema else {
                throw SynchroError.invalidResponse(message: "reconnect did not change the sealed push binding")
            }

            let members = try changeTracker.entriesForBatch(db, batchID: oldBatchID)
            guard members.count == oldRequest.mutations.count,
                  zip(members, oldRequest.mutations).allSatisfy({ member, mutation in
                      member.mutationID == mutation.mutationID
                          && member.lifecycleState == "sealed"
                          && member.sealedBatchID == oldBatchID
                  }) else {
                throw SynchroError.invalidResponse(message: "sealed push renewal membership is invalid")
            }

            let successor = PushRequest(
                clientID: clientID,
                clientGeneration: clientGeneration,
                batchID: UUID().uuidString.lowercased(),
                schema: installedSchema,
                mutations: oldRequest.mutations
            )
            try validateRenewedRequest(db, request: successor, syncedTables: syncedTables)
            let successorJSON = try encodeString(successor)
            let renewedMembers = members.enumerated().map { ordinal, member in
                PendingChange(
                    mutationID: member.mutationID,
                    localOrder: member.localOrder,
                    tableID: member.tableID,
                    recordID: member.recordID,
                    tableName: member.tableName,
                    pkFieldID: member.pkFieldID,
                    pkLogicalType: member.pkLogicalType,
                    operation: member.operation,
                    baseUpdatedAt: member.baseUpdatedAt,
                    clientUpdatedAt: member.clientUpdatedAt,
                    authoredSchemaVersion: member.authoredSchemaVersion,
                    authoredSchemaHash: member.authoredSchemaHash,
                    lifecycleState: "sealed",
                    sourceKind: member.sourceKind,
                    dependencyMutationID: member.dependencyMutationID,
                    normalizedMutationID: member.normalizedMutationID,
                    sealedBatchID: successor.batchID,
                    sealedOrdinal: Int64(ordinal),
                    fieldValuesByID: member.fieldValuesByID
                )
            }
            try db.execute(
                sql: """
                    INSERT INTO _synchro_push_batches
                        (batch_id, request_json, pending_json, schema_json, state, created_at)
                    VALUES (?, ?, ?, ?, 'pending', substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
                    """,
                arguments: [
                    successor.batchID,
                    successorJSON,
                    try encodeString(renewedMembers),
                    try encodeString(syncedTables),
                ]
            )
            try db.execute(
                sql: "UPDATE _synchro_push_batches SET state = 'superseded' WHERE batch_id = ? AND state = 'renewal_required'",
                arguments: [oldBatchID]
            )
            guard db.changesCount == 1 else {
                throw SynchroError.invalidResponse(message: "sealed push batch supersession was not durable")
            }

            for member in members {
                try db.execute(
                    sql: """
                        UPDATE _synchro_pending_changes
                        SET lifecycle_state = 'unsealed', sealed_batch_id = NULL, sealed_ordinal = NULL,
                            updated_at = ?
                        WHERE mutation_id = ? AND lifecycle_state = 'sealed' AND sealed_batch_id = ?
                        """,
                    arguments: [timestampNow(), member.mutationID, oldBatchID]
                )
                guard db.changesCount == 1 else {
                    throw SynchroError.invalidResponse(message: "sealed mutation could not leave its retired batch")
                }
            }
            try changeTracker.markPendingAsSealed(db, batchID: successor.batchID, pending: members)
            return true
        }
    }

    func hasRenewalRequiredBatches() throws -> Bool {
        try database.readTransaction { db in
            try Bool.fetchOne(
                db,
                sql: "SELECT EXISTS(SELECT 1 FROM _synchro_push_batches WHERE state = 'renewal_required')"
            ) ?? false
        }
    }

    func applyAccepted(
        accepted: [AcceptedMutation],
        syncedTables: [LocalSchemaTable],
        sentPending: [String: PendingChange] = [:]
    ) throws -> [ConflictEvent] {
        try database.writeSyncLockedTransaction { db in
            try applyAcceptedInTransaction(
                db,
                accepted: accepted,
                syncedTables: syncedTables,
                currentTables: syncedTables,
                sentPending: sentPending
            )
        }
    }

    private func applyAcceptedInTransaction(
        _ db: GRDB.Database,
        accepted: [AcceptedMutation],
        syncedTables: [LocalSchemaTable],
        currentTables: [LocalSchemaTable],
        sentPending: [String: PendingChange],
        outcomeJSONByID: [String: String] = [:],
        historicalSchema: SchemaRef? = nil,
        historicalTables: [LocalSchemaTable]? = nil
    ) throws -> [ConflictEvent] {
        let outcomeTableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableID, $0) })
        let currentTableMap = Dictionary(uniqueKeysWithValues: currentTables.map { ($0.tableID, $0) })
        for outcome in accepted {
            let source = try exactLedgerSource(
                db,
                mutationID: outcome.mutationID,
                sentPending: sentPending
            )
            guard let outcomeTable = outcomeTableMap[outcome.table] else {
                throw SynchroError.invalidResponse(message: "unknown logical table \(outcome.table)")
            }
            let recordID = try validateOutcomeIdentity(
                source: source,
                tableID: outcome.table,
                pk: outcome.pk,
                fallbackSchema: outcomeTable
            )
            let currentTable = source.flatMap {
                self.currentTable(for: $0, tablesByID: currentTableMap)
            } ?? currentTableMap[outcome.table]
            let later = try source.map { try changeTracker.laterUnresolved(db, after: $0) } ?? []
            let successors = try source.map {
                try changeTracker.successors(db, predecessorID: $0.mutationID)
            } ?? []

            let projection: AuthoritativeProjection?
            if let row = outcome.serverRow {
                let historical = try historicalTable(
                    db,
                    tableID: outcome.table,
                    schema: outcome.outcomeSchema,
                    sealedSchema: historicalSchema,
                    sealedTables: historicalTables
                )
                try verifyAuthoritativeRow(
                    row,
                    pk: outcome.pk,
                    rowChecksum: outcome.rowChecksum,
                    serverVersion: outcome.serverVersion,
                    outcomeSchema: outcome.outcomeSchema,
                    historicalTable: historical
                )
                projection = projectAuthoritativeRow(
                    historical: historical,
                    current: currentTable,
                    source: source,
                    pk: outcome.pk,
                    row: row
                )
            } else {
                projection = nil
            }

            let patches = try currentTable.flatMap { table in
                try prepareLocalPatches(
                    db,
                    current: table,
                    source: source,
                    later: later
                )
            }
            let hasAuthoritativeAbsence = outcome.serverRow == nil && source?.operation == "delete"
            let canApply = currentTable != nil
                && (outcome.serverRow == nil || projection != nil)
                && patches != nil
                && !(hasAuthoritativeAbsence && !later.isEmpty)

            if let source {
                try changeTracker.markAccepted(
                    db,
                    mutationID: source.mutationID,
                    acceptedJSON: outcomeJSONByID[outcome.mutationID] ?? (try encodeString(outcome))
                )
            } else {
                try markCompatibilityPendingAccepted(db, tableName: outcomeTable.tableName, recordID: recordID)
            }

            if canApply, let currentTable, let patches {
                if let projection {
                    try applyAuthoritativeRow(db, projection: projection)
                } else if hasAuthoritativeAbsence {
                    try applyAuthoritativeAbsence(
                        db,
                        schema: currentTable,
                        recordID: recordID,
                        pkLogicalType: source?.pkLogicalType
                    )
                }
                try reapplyLocalPatches(
                    db,
                    patches: patches,
                    schema: currentTable,
                    recordID: recordID,
                    pkLogicalType: source?.pkLogicalType
                )
                try SynchroMeta.upsertRowVersion(
                    db,
                    tableName: currentTable.tableName,
                    recordID: recordID,
                    serverVersion: outcome.serverVersion,
                    rowChecksum: outcome.rowChecksum
                )
            } else if outcome.serverRow != nil || hasAuthoritativeAbsence {
                if hasAuthoritativeAbsence, let currentTable {
                    try SynchroMeta.upsertRowVersion(
                        db,
                        tableName: currentTable.tableName,
                        recordID: recordID,
                        serverVersion: outcome.serverVersion,
                        rowChecksum: nil
                    )
                }
                try SynchroMeta.invalidateAllScopes(db)
            }

            for successor in successors where successor.lifecycleState == "unsealed" {
                if successor.operation == "update" || successor.operation == "delete" {
                    try changeTracker.refreshUnsealedSuccessor(
                        db,
                        mutationID: successor.mutationID,
                        serverVersion: outcome.serverVersion
                    )
                }
            }
        }
        return []
    }

    private struct RejectedOutcome {
        var conflicts: [ConflictEvent]
    }

    func applyRejected(
        rejected: [RejectedMutation],
        syncedTables: [LocalSchemaTable],
        sentPending: [String: PendingChange] = [:]
    ) throws -> [ConflictEvent] {
        try applyRejectedOutcome(rejected: rejected, syncedTables: syncedTables, sentPending: sentPending).conflicts
    }

    private func applyRejectedOutcome(
        rejected: [RejectedMutation],
        syncedTables: [LocalSchemaTable],
        sentPending: [String: PendingChange]
    ) throws -> RejectedOutcome {
        try database.writeSyncLockedTransaction { db in
            try applyRejectedOutcomeInTransaction(
                db,
                rejected: rejected,
                syncedTables: syncedTables,
                currentTables: syncedTables,
                sentPending: sentPending
            )
        }
    }

    private func applyRejectedOutcomeInTransaction(
        _ db: GRDB.Database,
        rejected: [RejectedMutation],
        syncedTables: [LocalSchemaTable],
        currentTables: [LocalSchemaTable],
        sentPending: [String: PendingChange],
        outcomeJSONByID: [String: String] = [:],
        originalMutationJSONByID: [String: String] = [:],
        historicalSchema: SchemaRef? = nil,
        historicalTables: [LocalSchemaTable]? = nil
    ) throws -> RejectedOutcome {
        let outcomeTableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableID, $0) })
        let currentTableMap = Dictionary(uniqueKeysWithValues: currentTables.map { ($0.tableID, $0) })
        var conflicts: [ConflictEvent] = []
        for outcome in rejected {
            let source = try exactLedgerSource(
                db,
                mutationID: outcome.mutationID,
                sentPending: sentPending
            )
            guard let outcomeTable = outcomeTableMap[outcome.table] else {
                throw SynchroError.invalidResponse(message: "unknown logical table \(outcome.table)")
            }
            let recordID = try validateOutcomeIdentity(
                source: source,
                tableID: outcome.table,
                pk: outcome.pk,
                fallbackSchema: outcomeTable
            )
            let currentTable = source.flatMap {
                self.currentTable(for: $0, tablesByID: currentTableMap)
            } ?? currentTableMap[outcome.table]
            let later = try source.map { try changeTracker.laterUnresolved(db, after: $0) } ?? []
            let rejectedJSON: String
            if let exactJSON = outcomeJSONByID[outcome.mutationID] {
                rejectedJSON = exactJSON
            } else {
                rejectedJSON = try encodeString(outcome)
            }
            let originalJSON: String?
            if let exactJSON = originalMutationJSONByID[outcome.mutationID] {
                originalJSON = exactJSON
            } else if let source {
                let authoredTable = try historicalTable(
                    db,
                    tableID: source.tableID ?? outcome.table,
                    schema: SchemaRef(
                        version: source.authoredSchemaVersion ?? outcome.outcomeSchema.version,
                        hash: source.authoredSchemaHash ?? outcome.outcomeSchema.hash
                    ),
                    sealedSchema: historicalSchema,
                    sealedTables: historicalTables
                )
                originalJSON = try encodeOriginalMutation(source, schema: authoredTable)
            } else {
                originalJSON = nil
            }

            let projection: AuthoritativeProjection?
            if let row = outcome.serverRow {
                guard let serverVersion = outcome.serverVersion else {
                    throw SynchroError.invalidResponse(message: "rejected row lacks checksum or server version")
                }
                let historical = try historicalTable(
                    db,
                    tableID: outcome.table,
                    schema: outcome.outcomeSchema,
                    sealedSchema: historicalSchema,
                    sealedTables: historicalTables
                )
                try verifyAuthoritativeRow(
                    row,
                    pk: outcome.pk,
                    rowChecksum: outcome.rowChecksum,
                    serverVersion: serverVersion,
                    outcomeSchema: outcome.outcomeSchema,
                    historicalTable: historical
                )
                projection = projectAuthoritativeRow(
                    historical: historical,
                    current: currentTable,
                    source: source,
                    pk: outcome.pk,
                    row: row
                )
            } else {
                projection = nil
            }

            let patches = try currentTable.flatMap { table in
                try prepareLocalPatches(
                    db,
                    current: table,
                    source: source,
                    later: later
                )
            }
            let hasAuthoritativeAbsence = outcome.serverRow == nil
                && outcome.status == .conflict
                && (outcome.code == .rowDeleted || outcome.code == .rowNotFound)
            let canApply = currentTable != nil
                && (outcome.serverRow == nil || projection != nil)
                && patches != nil
                && !(hasAuthoritativeAbsence && !later.isEmpty)

            try SynchroMeta.upsertRejectedMutation(
                db,
                mutationID: outcome.mutationID,
                tableName: currentTable?.tableName ?? outcomeTable.tableName,
                recordID: recordID,
                status: outcome.status.rawValue,
                code: outcome.code.rawValue,
                message: outcome.message,
                serverRow: outcome.serverRow,
                serverVersion: outcome.serverVersion,
                mutationJSON: originalJSON,
                rejectedJSON: rejectedJSON
            )
            if let source {
                try changeTracker.markRejected(db, mutationID: source.mutationID, rejectedJSON: rejectedJSON)
                try changeTracker.blockDependents(db, predecessorID: source.mutationID)
            } else {
                try markCompatibilityPendingRejected(db, tableName: outcomeTable.tableName, recordID: recordID)
            }

            if canApply, let currentTable, let patches {
                if let projection {
                    try applyAuthoritativeRow(db, projection: projection)
                } else if hasAuthoritativeAbsence {
                    try applyAuthoritativeAbsence(
                        db,
                        schema: currentTable,
                        recordID: recordID,
                        pkLogicalType: source?.pkLogicalType
                    )
                }
                try reapplyLocalPatches(
                    db,
                    patches: patches,
                    schema: currentTable,
                    recordID: recordID,
                    pkLogicalType: source?.pkLogicalType
                )
                if let serverVersion = outcome.serverVersion {
                    try SynchroMeta.upsertRowVersion(
                        db,
                        tableName: currentTable.tableName,
                        recordID: recordID,
                        serverVersion: serverVersion,
                        rowChecksum: outcome.rowChecksum
                    )
                }
            } else if outcome.serverRow != nil || hasAuthoritativeAbsence {
                if hasAuthoritativeAbsence, let currentTable, let serverVersion = outcome.serverVersion {
                    try SynchroMeta.upsertRowVersion(
                        db,
                        tableName: currentTable.tableName,
                        recordID: recordID,
                        serverVersion: serverVersion,
                        rowChecksum: nil
                    )
                }
                try SynchroMeta.invalidateAllScopes(db)
            }

            if outcome.status == .conflict {
                let serverData = outcome.serverRow.map { row in
                    Dictionary(uniqueKeysWithValues: (currentTable ?? outcomeTable).columns.compactMap { column in
                        row[column.fieldID].map { (column.name, $0) }
                    })
                }
                conflicts.append(ConflictEvent(table: currentTable?.tableName ?? outcomeTable.tableName, recordID: recordID, clientData: nil, serverData: serverData))
            }
        }
        return RejectedOutcome(conflicts: conflicts)
    }

    private func encodeOriginalMutation(_ pending: PendingChange, schema: LocalSchemaTable) throws -> String {
        try encodeString(mutation(from: pending, schema: schema))
    }

    private struct AuthoritativeProjection {
        let currentTable: LocalSchemaTable
        let values: [(column: LocalSchemaColumn, value: AnyCodable)]
    }

    private struct LocalPatch {
        let operation: String
        let values: [(column: LocalSchemaColumn, value: AnyCodable)]
        let deletedAtValue: DatabaseValue?
    }

    private func historicalTable(
        _ db: GRDB.Database,
        tableID: String,
        schema: SchemaRef,
        sealedSchema: SchemaRef? = nil,
        sealedTables: [LocalSchemaTable]? = nil
    ) throws -> LocalSchemaTable {
        var candidates: [[LocalSchemaTable]] = []
        if sealedSchema == schema, let sealedTables {
            candidates.append(sealedTables)
        }

        let sealedRows = try Row.fetchAll(
            db,
            sql: "SELECT request_json, schema_json FROM _synchro_push_batches ORDER BY created_at, batch_id"
        )
        for row in sealedRows {
            let requestJSON: String = row["request_json"]
            guard let request = try? decoder.decode(PushRequest.self, from: Data(requestJSON.utf8)),
                  request.schema == schema else {
                continue
            }
            let schemaJSON: String = row["schema_json"]
            do {
                candidates.append(
                    try decoder.decode([LocalSchemaTable].self, from: Data(schemaJSON.utf8))
                )
            } catch {
                throw SynchroError.invalidResponse(message: "sealed historical schema is invalid")
            }
        }

        if let archiveJSON = try String.fetchOne(
            db,
            sql: "SELECT schema_json FROM _synchro_schema_archive WHERE schema_version = ? AND schema_hash = ?",
            arguments: [schema.version, schema.hash]
        ) {
            do {
                candidates.append(
                    try decoder.decode([LocalSchemaTable].self, from: Data(archiveJSON.utf8))
                )
            } catch {
                throw SynchroError.invalidResponse(message: "archived historical schema is invalid")
            }
        }

        guard let tables = candidates.first else {
            throw SynchroError.invalidResponse(message: "outcome schema is not retained locally")
        }
        guard candidates.dropFirst().allSatisfy({ $0 == tables }) else {
            throw SynchroError.invalidResponse(message: "historical schema binding is inconsistent")
        }
        guard let table = tables.first(where: { $0.tableID == tableID }) else {
            throw SynchroError.invalidResponse(message: "outcome schema lacks its logical table")
        }
        return table
    }

    private func exactLedgerSource(
        _ db: GRDB.Database,
        mutationID: String,
        sentPending: [String: PendingChange]
    ) throws -> PendingChange? {
        let expected = sentPending[mutationID]
        if !sentPending.isEmpty && expected == nil {
            throw SynchroError.invalidResponse(message: "push outcome is not a sealed batch member")
        }
        let lookupID = expected?.mutationID ?? mutationID
        let stored = try ledgerEntry(db, mutationID: lookupID)
        if !sentPending.isEmpty && stored == nil {
            throw SynchroError.invalidResponse(message: "push outcome targets an unknown mutation")
        }
        if let expected, let stored {
            guard expected.localOrder == stored.localOrder,
                  expected.tableID == stored.tableID,
                  expected.pkFieldID == stored.pkFieldID,
                  expected.pkLogicalType == stored.pkLogicalType,
                  expected.recordID == stored.recordID else {
                throw SynchroError.invalidResponse(message: "push outcome does not match sealed membership")
            }
        }
        return stored
    }

    private func validateOutcomeIdentity(
        source: PendingChange?,
        tableID: String,
        pk: [String: AnyCodable],
        fallbackSchema: LocalSchemaTable
    ) throws -> String {
        guard let source else {
            return try recordID(from: pk, schema: fallbackSchema)
        }
        guard source.tableID == tableID,
              let pkFieldID = source.pkFieldID,
              let pkLogicalType = source.pkLogicalType,
              pk.count == 1,
              pk.keys.first == pkFieldID,
              let value = pk[pkFieldID] else {
            throw SynchroError.invalidResponse(message: "push outcome does not match the local mutation identity")
        }
        let result = try recordID(from: value, logicalType: pkLogicalType, tableName: source.tableName)
        guard result == source.recordID else {
            throw SynchroError.invalidResponse(message: "push outcome has a different typed primary key")
        }
        return result
    }

    private func currentTable(
        for source: PendingChange,
        tablesByID: [String: LocalSchemaTable]
    ) -> LocalSchemaTable? {
        guard let tableID = source.tableID,
              let pkFieldID = source.pkFieldID,
              let pkLogicalType = source.pkLogicalType,
              let table = tablesByID[tableID],
              table.primaryKeyFieldID == pkFieldID,
              table.columns.first(where: { $0.fieldID == pkFieldID })?.logicalType == pkLogicalType else {
            return nil
        }
        return table
    }

    private func verifyAuthoritativeRow(
        _ row: [String: AnyCodable],
        pk: [String: AnyCodable],
        rowChecksum: ChecksumObject?,
        serverVersion: String,
        outcomeSchema: SchemaRef,
        historicalTable: LocalSchemaTable
    ) throws {
        guard let rowChecksum else {
            throw SynchroError.invalidResponse(message: "authoritative row is missing its checksum")
        }
        let computed: ChecksumObject
        do {
            computed = try Integrity.rowDigest(
                schemaHash: outcomeSchema.hash,
                table: historicalTable,
                pk: pk,
                row: row,
                serverVersion: serverVersion
            ).checksum
        } catch {
            throw SynchroError.invalidResponse(message: "authoritative row is invalid for its outcome schema")
        }
        guard computed == rowChecksum else {
            throw SynchroError.invalidResponse(message: "authoritative row checksum mismatch")
        }
    }

    private func projectAuthoritativeRow(
        historical: LocalSchemaTable,
        current: LocalSchemaTable?,
        source: PendingChange?,
        pk: [String: AnyCodable],
        row: [String: AnyCodable]
    ) -> AuthoritativeProjection? {
        guard let current,
              historical.tableID == current.tableID,
              historical.primaryKeyFieldID == current.primaryKeyFieldID,
              pk.keys.count == 1,
              pk.keys.first == historical.primaryKeyFieldID,
              current.primaryKey.count == 1 else {
            return nil
        }
        if let source {
            guard source.tableID == historical.tableID,
                  source.pkFieldID == historical.primaryKeyFieldID else {
                return nil
            }
        }
        guard let historicalPK = historical.columns.first(where: { $0.fieldID == historical.primaryKeyFieldID }),
              let currentPK = current.columns.first(where: { $0.fieldID == current.primaryKeyFieldID }),
              historicalPK.logicalType == currentPK.logicalType,
              source?.pkLogicalType == nil || source?.pkLogicalType == currentPK.logicalType else {
            return nil
        }

        let historicalByID = Dictionary(uniqueKeysWithValues: historical.columns.map { ($0.fieldID, $0) })
        var values: [(column: LocalSchemaColumn, value: AnyCodable)] = []
        for column in current.columns {
            guard let historicalColumn = historicalByID[column.fieldID] else { continue }
            guard historicalColumn.logicalType == column.logicalType,
                  let value = row[column.fieldID] else {
                return nil
            }
            values.append((column, value))
        }
        guard values.contains(where: { $0.column.fieldID == current.primaryKeyFieldID }) else {
            return nil
        }
        return AuthoritativeProjection(currentTable: current, values: values)
    }

    private func prepareLocalPatches(
        _ db: GRDB.Database,
        current: LocalSchemaTable,
        source: PendingChange?,
        later: [PendingChange]
    ) throws -> [LocalPatch]? {
        guard let source else { return later.isEmpty ? [] : nil }
        let columnsByID = Dictionary(uniqueKeysWithValues: current.columns.map { ($0.fieldID, $0) })
        var patches: [LocalPatch] = []
        for intent in later {
            guard intent.tableID == source.tableID,
                  intent.pkFieldID == source.pkFieldID,
                  intent.pkLogicalType == source.pkLogicalType,
                  intent.recordID == source.recordID else {
                return nil
            }
            switch intent.operation {
            case "insert", "update":
                guard !intent.fieldValuesByID.isEmpty else { return nil }
                var values: [(column: LocalSchemaColumn, value: AnyCodable)] = []
                for stored in intent.fieldValuesByID.values.sorted(by: { $0.fieldID < $1.fieldID }) {
                    guard let column = columnsByID[stored.fieldID],
                          column.logicalType == stored.logicalType else {
                        return nil
                    }
                    do {
                        try Integrity.validateTypedValue(stored.wireValue, field: column)
                    } catch {
                        throw SynchroError.invalidResponse(message: "later local patch has an invalid portable value")
                    }
                    values.append((column, stored.wireValue))
                }
                patches.append(LocalPatch(operation: intent.operation, values: values, deletedAtValue: nil))
            case "delete":
                guard let deletedAtFieldID = current.deletedAtFieldID,
                      let deletedAtColumn = columnsByID[deletedAtFieldID] else {
                    return nil
                }
                let deletedAtValue: DatabaseValue
                if let stored = intent.fieldValuesByID[deletedAtFieldID],
                   stored.logicalType == deletedAtColumn.logicalType {
                    do {
                        try Integrity.validateTypedValue(stored.wireValue, field: deletedAtColumn)
                        deletedAtValue = try databaseValue(from: stored.wireValue, column: deletedAtColumn)
                    } catch {
                        throw SynchroError.invalidResponse(message: "local delete marker is invalid")
                    }
                } else if let existing = try readColumnValue(
                    db,
                    schema: current,
                    recordID: source.recordID,
                    pkLogicalType: source.pkLogicalType,
                    columnName: current.deletedAtColumn
                ), case .null = existing.storage {
                    return nil
                } else if let existing = try readColumnValue(
                    db,
                    schema: current,
                    recordID: source.recordID,
                    pkLogicalType: source.pkLogicalType,
                    columnName: current.deletedAtColumn
                ) {
                    deletedAtValue = existing
                } else {
                    return nil
                }
                patches.append(LocalPatch(operation: intent.operation, values: [], deletedAtValue: deletedAtValue))
            default:
                return nil
            }
        }
        return patches
    }

    private func applyAuthoritativeRow(
        _ db: GRDB.Database,
        projection: AuthoritativeProjection
    ) throws {
        let schema = projection.currentTable
        guard let pkColumn = schema.primaryKey.first else {
            throw SynchroError.invalidResponse(message: "current local table has no primary key")
        }
        let columnNames = projection.values.map(\.column.name)
        let quotedColumns = columnNames.map(SQLiteHelpers.quoteIdentifier).joined(separator: ", ")
        let updates: String = columnNames.filter { $0 != pkColumn }.map {
            let quoted = SQLiteHelpers.quoteIdentifier($0)
            return "\(quoted) = excluded.\(quoted)"
        }.joined(separator: ", ")
        let action: String = updates.isEmpty ? "DO NOTHING" : "DO UPDATE SET \(updates)"
        let values = try projection.values.map {
            try databaseValue(from: $0.value, column: $0.column)
        }
        try db.execute(
            sql: """
                INSERT INTO \(SQLiteHelpers.quoteIdentifier(schema.tableName)) (\(quotedColumns))
                VALUES (\(SQLiteHelpers.placeholders(count: values.count)))
                ON CONFLICT (\(SQLiteHelpers.quoteIdentifier(pkColumn))) \(action)
                """,
            arguments: StatementArguments(values)
        )
    }

    private func applyAuthoritativeAbsence(
        _ db: GRDB.Database,
        schema: LocalSchemaTable,
        recordID: String,
        pkLogicalType: String?
    ) throws {
        guard let pkColumn = schema.primaryKey.first,
              let schemaPK = schema.columns.first(where: { $0.fieldID == schema.primaryKeyFieldID }) else {
            throw SynchroError.invalidResponse(message: "current local table has no primary key")
        }
        let type = pkLogicalType ?? schemaPK.logicalType
        try db.execute(
            sql: "DELETE FROM \(SQLiteHelpers.quoteIdentifier(schema.tableName)) WHERE \(SQLiteHelpers.quoteIdentifier(pkColumn)) = ?",
            arguments: [try primaryKeyDatabaseValue(recordID, logicalType: type, tableName: schema.tableName)]
        )
    }

    private func reapplyLocalPatches(
        _ db: GRDB.Database,
        patches: [LocalPatch],
        schema: LocalSchemaTable,
        recordID: String,
        pkLogicalType: String?
    ) throws {
        guard let pkColumn = schema.primaryKey.first,
              let schemaPK = schema.columns.first(where: { $0.fieldID == schema.primaryKeyFieldID }) else {
            throw SynchroError.invalidResponse(message: "current local table has no primary key")
        }
        let primaryKey = try primaryKeyDatabaseValue(
            recordID,
            logicalType: pkLogicalType ?? schemaPK.logicalType,
            tableName: schema.tableName
        )
        for patch in patches {
            switch patch.operation {
            case "insert", "update":
                guard !patch.values.isEmpty else { continue }
                let assignments = patch.values.map {
                    "\(SQLiteHelpers.quoteIdentifier($0.column.name)) = ?"
                }.joined(separator: ", ")
                let values = try patch.values.map {
                    try databaseValue(from: $0.value, column: $0.column)
                } + [primaryKey]
                try db.execute(
                    sql: "UPDATE \(SQLiteHelpers.quoteIdentifier(schema.tableName)) SET \(assignments) WHERE \(SQLiteHelpers.quoteIdentifier(pkColumn)) = ?",
                    arguments: StatementArguments(values)
                )
            case "delete":
                guard !schema.deletedAtColumn.isEmpty,
                      let deletedAtValue = patch.deletedAtValue else {
                    throw SynchroError.invalidResponse(message: "local delete patch has no soft-delete marker")
                }
                try db.execute(
                    sql: "UPDATE \(SQLiteHelpers.quoteIdentifier(schema.tableName)) SET \(SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)) = ? WHERE \(SQLiteHelpers.quoteIdentifier(pkColumn)) = ?",
                    arguments: StatementArguments([deletedAtValue, primaryKey])
                )
            default:
                throw SynchroError.invalidResponse(message: "later local patch has an invalid operation")
            }
        }
    }

    private func readColumnValue(
        _ db: GRDB.Database,
        schema: LocalSchemaTable,
        recordID: String,
        pkLogicalType: String?,
        columnName: String
    ) throws -> DatabaseValue? {
        guard let pkColumn = schema.primaryKey.first,
              let schemaPK = schema.columns.first(where: { $0.fieldID == schema.primaryKeyFieldID }),
              let row = try Row.fetchOne(
                db,
                sql: "SELECT \(SQLiteHelpers.quoteIdentifier(columnName)) FROM \(SQLiteHelpers.quoteIdentifier(schema.tableName)) WHERE \(SQLiteHelpers.quoteIdentifier(pkColumn)) = ?",
                arguments: [
                    try primaryKeyDatabaseValue(
                        recordID,
                        logicalType: pkLogicalType ?? schemaPK.logicalType,
                        tableName: schema.tableName
                    ),
                ]
              ) else {
            return nil
        }
        return row[columnName]
    }

    private func databaseValue(
        from value: AnyCodable,
        column: LocalSchemaColumn
    ) throws -> DatabaseValue {
        if value.value is NSNull { return .null }
        switch column.logicalType {
        case "string", "decimal", "datetime", "date", "time", "json":
            guard let text = value.value as? String else {
                throw SynchroError.invalidResponse(message: "invalid value for \(column.fieldID)")
            }
            return text.databaseValue
        case "int":
            let integer = try exactInt64(value.value, fieldID: column.fieldID)
            guard integer >= Int64(Int32.min), integer <= Int64(Int32.max) else {
                throw SynchroError.invalidResponse(message: "invalid value for \(column.fieldID)")
            }
            return integer.databaseValue
        case "int64":
            guard let text = value.value as? String,
                  canonicalInteger(text),
                  let integer = Int64(text) else {
                throw SynchroError.invalidResponse(message: "invalid value for \(column.fieldID)")
            }
            return integer.databaseValue
        case "float":
            return try exactDouble(value.value, fieldID: column.fieldID).databaseValue
        case "boolean":
            guard let boolean = value.value as? Bool else {
                throw SynchroError.invalidResponse(message: "invalid value for \(column.fieldID)")
            }
            return (boolean ? 1 : 0).databaseValue
        case "bytes":
            guard let text = value.value as? String,
                  let data = decodeBase64URL(text) else {
                throw SynchroError.invalidResponse(message: "invalid value for \(column.fieldID)")
            }
            return data.databaseValue
        default:
            throw SynchroError.invalidResponse(message: "unsupported portable type \(column.logicalType)")
        }
    }

    private func exactInt64(_ value: Any, fieldID: String) throws -> Int64 {
        if let value = value as? Int64 { return value }
        if let value = value as? Int { return Int64(value) }
        if let value = value as? NSNumber,
           CFGetTypeID(value) != CFBooleanGetTypeID(),
           value.doubleValue == Double(value.int64Value) {
            return value.int64Value
        }
        throw SynchroError.invalidResponse(message: "invalid value for \(fieldID)")
    }

    private func exactDouble(_ value: Any, fieldID: String) throws -> Double {
        let number: Double
        if let value = value as? Double { number = value }
        else if let value = value as? Float { number = Double(value) }
        else if let value = value as? Int64 { number = Double(value) }
        else if let value = value as? Int { number = Double(value) }
        else if let value = value as? NSNumber, CFGetTypeID(value) != CFBooleanGetTypeID() {
            number = value.doubleValue
        } else {
            throw SynchroError.invalidResponse(message: "invalid value for \(fieldID)")
        }
        guard number.isFinite else {
            throw SynchroError.invalidResponse(message: "invalid value for \(fieldID)")
        }
        return number
    }

    private func canonicalInteger(_ value: String) -> Bool {
        if value == "0" { return true }
        let bytes = Array(value.utf8)
        let start = bytes.first == 45 ? 1 : 0
        guard start < bytes.count, bytes[start] != 48 else { return false }
        return bytes[start...].allSatisfy { $0 >= 48 && $0 <= 57 }
    }

    private func decodeBase64URL(_ value: String) -> Data? {
        guard !value.contains("=") else { return nil }
        var standard = value.replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        standard += String(repeating: "=", count: (4 - standard.count % 4) % 4)
        guard let decoded = Data(base64Encoded: standard) else { return nil }
        let canonical = decoded.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        return canonical == value ? decoded : nil
    }

    private func primaryKeyDatabaseValue(
        _ recordID: String,
        logicalType: String,
        tableName: String
    ) throws -> DatabaseValue {
        switch logicalType {
        case "string":
            return recordID.databaseValue
        case "int", "int64":
            guard canonicalInteger(recordID), let integer = Int64(recordID) else {
                throw SynchroError.invalidResponse(message: "invalid integer primary key for \(tableName)")
            }
            if logicalType == "int",
               !(Int64(Int32.min)...Int64(Int32.max)).contains(integer) {
                throw SynchroError.invalidResponse(message: "invalid integer primary key for \(tableName)")
            }
            return integer.databaseValue
        default:
            throw SynchroError.invalidResponse(message: "unsupported primary key type for \(tableName)")
        }
    }

    private func recordID(from pk: [String: AnyCodable], schema: LocalSchemaTable) throws -> String {
        guard pk.count == 1,
              let field = schema.columns.first(where: { $0.fieldID == schema.primaryKeyFieldID }),
              let value = pk[schema.primaryKeyFieldID] else {
            throw SynchroError.invalidResponse(message: "invalid primary key for table \(schema.tableName)")
        }
        return try recordID(from: value, logicalType: field.logicalType, tableName: schema.tableName)
    }

    private func recordID(
        from value: AnyCodable,
        logicalType: String,
        tableName: String
    ) throws -> String {
        switch logicalType {
        case "string":
            guard let string = value.value as? String else {
                throw SynchroError.invalidResponse(message: "invalid string primary key for \(tableName)")
            }
            return string
        case "int":
            let integer = try exactInt64(value.value, fieldID: tableName)
            guard (Int64(Int32.min)...Int64(Int32.max)).contains(integer) else {
                throw SynchroError.invalidResponse(message: "invalid integer primary key for \(tableName)")
            }
            return String(integer)
        case "int64":
            guard let string = value.value as? String,
                  canonicalInteger(string),
                  Int64(string) != nil else {
                throw SynchroError.invalidResponse(message: "invalid int64 primary key for \(tableName)")
            }
            return string
        default:
            throw SynchroError.invalidResponse(message: "unsupported primary key type for \(tableName)")
        }
    }

    private func ledgerEntry(_ db: GRDB.Database, mutationID: String) throws -> PendingChange? {
        guard let row = try Row.fetchOne(
            db,
            sql: """
                SELECT mutation_id, local_order, table_id, record_id, table_name, pk_field_id,
                       pk_logical_type, operation, base_version, client_version,
                       authored_schema_version, authored_schema_hash, lifecycle_state, source_kind,
                       dependency_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal
                FROM _synchro_pending_changes WHERE mutation_id = ?
                """,
            arguments: [mutationID]
        ) else { return nil }
        return try PendingChange(row: row, fieldValuesByID: changeTracker.fieldValues(db, mutationID: mutationID))
    }

    private func markCompatibilityPendingAccepted(_ db: GRDB.Database, sent: PendingChange) throws {
        try markCompatibilityPendingAccepted(db, tableName: sent.tableName, recordID: sent.recordID, mutationID: sent.mutationID)
    }

    private func markCompatibilityPendingAccepted(_ db: GRDB.Database, tableName: String, recordID: String, mutationID: String? = nil) throws {
        if let mutationID,
           try Row.fetchOne(db, sql: "SELECT 1 FROM _synchro_pending_changes WHERE mutation_id = ?", arguments: [mutationID]) != nil {
            return
        }
        try db.execute(
            sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted', updated_at = ? WHERE table_name = ? AND record_id = ? AND lifecycle_state = 'unsealed'",
            arguments: [timestampNow(), tableName, recordID]
        )
    }

    private func markCompatibilityPendingRejected(_ db: GRDB.Database, tableName: String, recordID: String) throws {
        try db.execute(
            sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'rejected', updated_at = ? WHERE table_name = ? AND record_id = ? AND lifecycle_state = 'unsealed'",
            arguments: [timestampNow(), tableName, recordID]
        )
    }

    private func timestampNow() -> String {
        ISO8601DateFormatter().string(from: Date())
    }

    private func exactObjectJSONMap<T: Decodable>(
        _ data: Data,
        key: String,
        type: T.Type,
        id: (T) -> String
    ) throws -> [String: String] {
        let bytes = Array(data)
        var index = 0
        skipJSONWhitespace(bytes, index: &index)
        guard index < bytes.count, bytes[index] == 0x7b else {
            throw SynchroError.invalidResponse(message: "push response is not a JSON object")
        }
        index += 1
        var result: [String: String] = [:]
        var found = false
        while true {
            skipJSONWhitespace(bytes, index: &index)
            if index < bytes.count, bytes[index] == 0x7d {
                index += 1
                break
            }
            let keyStart = index
            try skipJSONString(bytes, index: &index)
            guard let memberName = String(data: Data(bytes[keyStart..<index]), encoding: .utf8),
                  let decodedName = try? JSONDecoder().decode(String.self, from: Data(memberName.utf8)) else {
                throw SynchroError.invalidResponse(message: "push response has an invalid member")
            }
            skipJSONWhitespace(bytes, index: &index)
            guard index < bytes.count, bytes[index] == 0x3a else {
                throw SynchroError.invalidResponse(message: "push response member lacks a colon")
            }
            index += 1
            skipJSONWhitespace(bytes, index: &index)
            if decodedName == key {
                found = true
                guard index < bytes.count, bytes[index] == 0x5b else {
                    throw SynchroError.invalidResponse(message: "push response member is not an array")
                }
                index += 1
                while true {
                    skipJSONWhitespace(bytes, index: &index)
                    if index < bytes.count, bytes[index] == 0x5d {
                        index += 1
                        break
                    }
                    let valueStart = index
                    try skipJSONValue(bytes, index: &index)
                    let raw = Data(bytes[valueStart..<index])
                    let value = try decoder.decode(T.self, from: raw)
                    result[id(value)] = String(data: raw, encoding: .utf8)
                    skipJSONWhitespace(bytes, index: &index)
                    if index < bytes.count, bytes[index] == 0x2c {
                        index += 1
                    } else if index < bytes.count, bytes[index] == 0x5d {
                        index += 1
                        break
                    } else {
                        throw SynchroError.invalidResponse(message: "push response array is malformed")
                    }
                }
            } else {
                try skipJSONValue(bytes, index: &index)
            }
            skipJSONWhitespace(bytes, index: &index)
            if index < bytes.count, bytes[index] == 0x2c {
                index += 1
            } else if index < bytes.count, bytes[index] == 0x7d {
                index += 1
                break
            } else {
                throw SynchroError.invalidResponse(message: "push response object is malformed")
            }
        }
        guard found else {
            throw SynchroError.invalidResponse(message: "push response lacks \(key)")
        }
        return result
    }

    private func skipJSONWhitespace(_ bytes: [UInt8], index: inout Int) {
        while index < bytes.count && (bytes[index] == 0x20 || bytes[index] == 0x09 || bytes[index] == 0x0a || bytes[index] == 0x0d) {
            index += 1
        }
    }

    private func skipJSONString(_ bytes: [UInt8], index: inout Int) throws {
        guard index < bytes.count, bytes[index] == 0x22 else {
            throw SynchroError.invalidResponse(message: "push response string is malformed")
        }
        index += 1
        while index < bytes.count {
            switch bytes[index] {
            case 0x22:
                index += 1
                return
            case 0x5c:
                index += 2
            default:
                index += 1
            }
        }
        throw SynchroError.invalidResponse(message: "push response string is unterminated")
    }

    private func skipJSONValue(_ bytes: [UInt8], index: inout Int) throws {
        skipJSONWhitespace(bytes, index: &index)
        guard index < bytes.count else {
            throw SynchroError.invalidResponse(message: "push response value is missing")
        }
        switch bytes[index] {
        case 0x22:
            try skipJSONString(bytes, index: &index)
        case 0x7b:
            index += 1
            while true {
                skipJSONWhitespace(bytes, index: &index)
                if index < bytes.count, bytes[index] == 0x7d {
                    index += 1
                    return
                }
                try skipJSONString(bytes, index: &index)
                skipJSONWhitespace(bytes, index: &index)
                guard index < bytes.count, bytes[index] == 0x3a else {
                    throw SynchroError.invalidResponse(message: "push response object is malformed")
                }
                index += 1
                try skipJSONValue(bytes, index: &index)
                skipJSONWhitespace(bytes, index: &index)
                if index < bytes.count, bytes[index] == 0x2c {
                    index += 1
                } else if index < bytes.count, bytes[index] == 0x7d {
                    index += 1
                    return
                } else {
                    throw SynchroError.invalidResponse(message: "push response object is malformed")
                }
            }
        case 0x5b:
            index += 1
            while true {
                skipJSONWhitespace(bytes, index: &index)
                if index < bytes.count, bytes[index] == 0x5d {
                    index += 1
                    return
                }
                try skipJSONValue(bytes, index: &index)
                skipJSONWhitespace(bytes, index: &index)
                if index < bytes.count, bytes[index] == 0x2c {
                    index += 1
                } else if index < bytes.count, bytes[index] == 0x5d {
                    index += 1
                    return
                } else {
                    throw SynchroError.invalidResponse(message: "push response array is malformed")
                }
            }
        default:
            while index < bytes.count && ![0x20, 0x09, 0x0a, 0x0d, 0x2c, 0x5d, 0x7d].contains(bytes[index]) {
                index += 1
            }
        }
    }
}
