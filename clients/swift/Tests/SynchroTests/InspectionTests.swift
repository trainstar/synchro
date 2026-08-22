import XCTest
@testable import Synchro

final class InspectionTests: XCTestCase {
    func testPendingInspectionSurvivesRestartAndUsesAuthoredValues() async throws {
        let config = try prepareClientConfig()
        let firstClient = try SynchroClient(config: config)

        XCTAssertEqual(firstClient.getSyncStatus(), .localReady)
        _ = try firstClient.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            params: ["o1", "first authored", "2026-01-01T00:00:00.000000Z"]
        )
        _ = try firstClient.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            params: ["o2", "second authored", "2026-01-01T00:00:01.000000Z"]
        )
        let internalDatabase = try SynchroDatabase(path: config.dbPath)
        try internalDatabase.writeSyncLockedTransaction { db in
            try db.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'superseded_before_send' WHERE record_id = ?",
                arguments: ["o1"]
            )
            try db.execute(
                sql: "UPDATE orders SET title = ?",
                arguments: ["current row value"]
            )
        }
        try internalDatabase.close()
        try await firstClient.close()

        let restartedClient = try SynchroClient(config: config)
        let inspections = try restartedClient.inspectPendingMutations()

        XCTAssertEqual(inspections.map(\.recordID), ["o1", "o2"])
        XCTAssertEqual(inspections.map(\.localOrder), inspections.map(\.localOrder).sorted())
        XCTAssertEqual(inspections[0].status, .supersededBeforeSend)
        XCTAssertEqual(inspections[1].status, .pending)
        XCTAssertEqual(inspections[0].operation, .insert)
        XCTAssertEqual(inspections[0].authoredSchema, SchemaRef(version: 1, hash: protocolTestSchemaHash))
        XCTAssertEqual(
            inspections[0].authoredFields.first(where: { $0.fieldID == "title" })?.value,
            AnyCodable("first authored")
        )
        XCTAssertEqual(
            inspections[1].authoredFields.first(where: { $0.fieldID == "title" })?.value,
            AnyCodable("second authored")
        )
        XCTAssertFalse(inspections.contains { inspection in
            inspection.authoredFields.contains { $0.value == AnyCodable("current row value") }
        })

        try await restartedClient.close()
        removeDatabase(at: config.dbPath)
    }

    func testRejectedInspectionSurvivesRestartAndClearRetainsQueueIntent() async throws {
        let config = try prepareClientConfig()
        let firstClient = try SynchroClient(config: config)

        _ = try firstClient.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            params: ["o1", "authored", "2026-01-01T00:00:00.000000Z"]
        )
        let pending = try XCTUnwrap(firstClient.inspectPendingMutations().first)
        let mutationID = pending.mutationID
        let mutation = Mutation(
            mutationID: mutationID,
            table: pending.tableID,
            op: pending.operation,
            pk: [pending.primaryKeyFieldID: AnyCodable(pending.recordID)],
            authoredSchema: pending.authoredSchema,
            baseVersion: pending.baseVersion,
            clientVersion: pending.clientVersion,
            columns: Dictionary(uniqueKeysWithValues: pending.authoredFields.map { ($0.fieldID, $0.value) })
        )
        let rejection = RejectedMutation(
            mutationID: mutationID,
            table: pending.tableID,
            pk: mutation.pk,
            outcomeSchema: pending.authoredSchema,
            status: .rejectedTerminal,
            code: .policyRejected,
            message: "not allowed",
            retryable: false,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: nil,
            authoredSchema: nil,
            currentSchema: nil,
            incompatibleFieldIDs: nil
        )
        let mutationJSON = try XCTUnwrap(String(data: JSONEncoder.synchroEncoder().encode(mutation), encoding: .utf8))
        let rejectionJSON = try XCTUnwrap(String(data: JSONEncoder.synchroEncoder().encode(rejection), encoding: .utf8))
        let internalDatabase = try SynchroDatabase(path: config.dbPath)
        try internalDatabase.writeTransaction { db in
            try db.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'rejected' WHERE mutation_id = ?",
                arguments: [mutationID]
            )
            try SynchroMeta.upsertRejectedMutation(
                db,
                mutationID: mutationID,
                tableName: "orders",
                recordID: "o1",
                status: "rejected_terminal",
                code: "policy_rejected",
                message: "not allowed",
                serverRow: nil,
                serverVersion: nil,
                mutationJSON: mutationJSON,
                rejectedJSON: rejectionJSON
            )
        }
        try internalDatabase.close()
        try await firstClient.close()

        let restartedClient = try SynchroClient(config: config)
        let rejected = try XCTUnwrap(restartedClient.inspectRejectedMutations().first)
        XCTAssertEqual(rejected.status, .rejectedTerminal)
        XCTAssertEqual(rejected.code, .policyRejected)
        XCTAssertEqual(rejected.localOrder, pending.localOrder)
        XCTAssertEqual(rejected.mutation, mutation)
        XCTAssertEqual(rejected.rejection, rejection)
        XCTAssertEqual(rejected.message, "not allowed")
        XCTAssertEqual(rejected.mutationJSON, mutationJSON)
        XCTAssertEqual(rejected.rejectionJSON, rejectionJSON)

        let retainedBeforeClear = try XCTUnwrap(restartedClient.inspectRetainedMutations().first)
        XCTAssertEqual(retainedBeforeClear.status, .serverRejected)

        try restartedClient.clearRejectedMutations()

        XCTAssertTrue(try restartedClient.inspectRejectedMutations().isEmpty)
        XCTAssertEqual(try restartedClient.inspectRetainedMutations(), [retainedBeforeClear])
        try await restartedClient.close()

        let afterClearRestart = try SynchroClient(config: config)
        XCTAssertTrue(try afterClearRestart.inspectRejectedMutations().isEmpty)
        XCTAssertEqual(try afterClearRestart.inspectRetainedMutations(), [retainedBeforeClear])
        XCTAssertEqual(try afterClearRestart.inspectCurrentSchema(), pending.authoredSchema)
        try await afterClearRestart.close()
        removeDatabase(at: config.dbPath)
    }

    func testScopeInspectionReturnsOrderedDurableState() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let internalDatabase = try SynchroDatabase(path: config.dbPath)
        try internalDatabase.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: "scope-b",
                cursor: "cursor-b",
                checksum: "checksum-b",
                generation: 2,
                localChecksum: "checksum-b"
            )
            try SynchroMeta.upsertScope(
                db,
                scopeID: "scope-a",
                cursor: nil,
                checksum: nil,
                generation: 1,
                localChecksum: "local-a"
            )
            try SynchroMeta.upsertScopeRow(
                db,
                scopeID: "scope-b",
                tableName: "orders",
                recordID: "o2",
                checksum: "row-b",
                generation: 2
            )
            try SynchroMeta.upsertScopeRow(
                db,
                scopeID: "scope-a",
                tableName: "orders",
                recordID: "o1",
                checksum: "row-a",
                generation: 1
            )
            try SynchroMeta.upsertRowVersion(
                db,
                tableName: "orders",
                recordID: "o1",
                serverVersion: "version-a",
                rowChecksum: ChecksumObject(
                    algorithm: "sha256",
                    version: 1,
                    encoding: "hex",
                    digest: String(repeating: "a", count: 64)
                )
            )
            try SynchroMeta.upsertRebuildAttempt(
                db,
                attempt: LocalRebuildAttempt(
                    scopeID: "scope-a",
                    rebuildID: "rebuild-a",
                    clientGeneration: 3,
                    schemaVersion: 1,
                    schemaHash: protocolTestSchemaHash,
                    generation: 1,
                    cursor: nil,
                    pageLimit: 100
                )
            )
        }
        try internalDatabase.close()

        XCTAssertEqual(try client.inspectScopeStates().map(\.scopeID), ["scope-a", "scope-b"])
        XCTAssertEqual(try client.inspectScopeRows().map(\.recordID), ["o1", "o2"])
        let metadata = try XCTUnwrap(client.inspectRowMetadata(tableName: "orders", recordID: "o1"))
        XCTAssertEqual(metadata.serverVersion, "version-a")
        XCTAssertNotNil(metadata.rowChecksum)
        let attempt = try XCTUnwrap(client.inspectRebuildAttempts().first)
        XCTAssertEqual(attempt.scopeID, "scope-a")
        XCTAssertEqual(attempt.rebuildID, "rebuild-a")
        XCTAssertEqual(attempt.schemaHash, protocolTestSchemaHash)
        XCTAssertEqual(attempt.pageLimit, 100)

        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    func testRebuildReceiptProofAcceptsValidTwoPageReceipts() async throws {
        let fixture = try makeRebuildReceiptFixture()
        let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)

        XCTAssertEqual(proof.rebuildIDFingerprint, TransportObservationCollector.cursorFingerprint("proof-rebuild"))
        XCTAssertEqual(proof.pageCount, 2)
        XCTAssertEqual(proof.returnedRecordCount, 3)
        XCTAssertTrue(proof.requestChainValid)
        XCTAssertTrue(proof.recordsInCanonicalOrder)
        XCTAssertTrue(proof.rowChecksumsValid)
        XCTAssertTrue(proof.scopeChecksumValid)
        XCTAssertTrue(proof.finalChecksumMatchesLocal)
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptProofAcceptsExplicitNullWireMembers() async throws {
        let fixture = try makeRebuildReceiptFixture()
        try updateReceiptJSON(fixture.database, column: "request_json", requestCursor: nil) {
            $0["cursor"] = NSNull()
        }
        try updateReceiptJSON(fixture.database, column: "response_json", requestCursor: nil) {
            $0["final_scope_cursor"] = NSNull()
            $0["checksum"] = NSNull()
        }
        try updateReceiptJSON(fixture.database, column: "response_json", requestCursor: "page-2") {
            $0["cursor"] = NSNull()
        }

        let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)
        XCTAssertTrue(proof.requestChainValid)
        XCTAssertTrue(proof.rowChecksumsValid)
        XCTAssertTrue(proof.scopeChecksumValid)
        XCTAssertTrue(proof.finalChecksumMatchesLocal)
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptProofRejectsUnknownWireMember() async throws {
        let fixture = try makeRebuildReceiptFixture()
        try updateReceiptJSON(fixture.database, column: "response_json", requestCursor: nil) {
            $0["unexpected"] = true
        }
        XCTAssertThrowsError(try fixture.client.inspectRebuildReceiptProofs())
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptProofControlsForgedChecksums() async throws {
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: nil) { response in
                response.records[0].rowChecksum.digest = String(repeating: "f", count: 64)
            }
            let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)
            XCTAssertTrue(proof.requestChainValid)
            XCTAssertTrue(proof.recordsInCanonicalOrder)
            XCTAssertFalse(proof.rowChecksumsValid)
            XCTAssertTrue(proof.scopeChecksumValid)
            try await closeRebuildReceiptFixture(fixture)
        }
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: "page-2") { response in
                response.checksum?.digest = String(repeating: "e", count: 64)
            }
            try fixture.database.writeTransaction { db in
                try db.execute(
                    sql: "UPDATE _synchro_rebuild_page_receipts SET final_checksum = ? WHERE scope_id = ? AND rebuild_id = ? AND request_cursor = ?",
                    arguments: [try json(ChecksumObject(
                        algorithm: "sha256",
                        version: 1,
                        encoding: "hex",
                        digest: String(repeating: "e", count: 64)
                    )), "proof-scope", "proof-rebuild", "page-2"]
                )
            }
            let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)
            XCTAssertTrue(proof.requestChainValid)
            XCTAssertTrue(proof.rowChecksumsValid)
            XCTAssertFalse(proof.scopeChecksumValid)
            XCTAssertFalse(proof.finalChecksumMatchesLocal)
            try await closeRebuildReceiptFixture(fixture)
        }
    }

    func testRebuildReceiptProofControlsOrderAndCursorChain() async throws {
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: nil) { response in
                response.records.reverse()
            }
            let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)
            XCTAssertTrue(proof.requestChainValid)
            XCTAssertFalse(proof.recordsInCanonicalOrder)
            XCTAssertTrue(proof.rowChecksumsValid)
            try await closeRebuildReceiptFixture(fixture)
        }
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: nil) { response in
                response.cursor = "unconsumed"
            }
            let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)
            XCTAssertFalse(proof.requestChainValid)
            XCTAssertTrue(proof.rowChecksumsValid)
            try await closeRebuildReceiptFixture(fixture)
        }
    }

    func testRebuildReceiptProofControlsExtraUnconsumedReceipt() async throws {
        let fixture = try makeRebuildReceiptFixture()
        let request = RebuildRequest(
            clientID: "inspection-device",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            scope: "proof-scope",
            rebuildID: "proof-rebuild",
            cursor: "orphan",
            limit: 2
        )
        let response = RebuildResponse(
            scope: "proof-scope",
            records: [],
            cursor: "orphan-next",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        try fixture.database.writeTransaction { db in
            try SynchroMeta.insertRebuildPageReceipt(
                db,
                scopeID: "proof-scope",
                rebuildID: "proof-rebuild",
                requestCursor: request.cursor,
                requestJSON: try json(request),
                responseJSON: try json(response),
                finalScopeCursor: nil,
                finalChecksumJSON: nil
            )
        }
        let proof = try XCTUnwrap(fixture.client.inspectRebuildReceiptProofs().first)
        XCTAssertEqual(proof.pageCount, 3)
        XCTAssertFalse(proof.requestChainValid)
        XCTAssertEqual(proof.returnedRecordCount, 3)
        try await closeRebuildReceiptFixture(fixture)
    }

    private func prepareClientConfig() throws -> SynchroConfig {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_inspection_\(UUID().uuidString).sqlite")
        let config = SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://localhost:8080")!,
            authProvider: { "test-token" },
            clientID: "inspection-device",
            appVersion: "1.0.0"
        )
        let database = try SynchroDatabase(path: path)
        let table = LocalSchemaTable(
            tableName: "orders",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [
                SchemaColumn(name: "id", logicalType: "string", nullable: false, isPrimaryKey: true),
                SchemaColumn(name: "title", logicalType: "string"),
                SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false),
                SchemaColumn(name: "deleted_at", logicalType: "datetime"),
            ]
        )
        try SchemaManager(database: database).createSyncedTables(
            schema: SchemaResponse(
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                serverTime: Date(),
                tables: [table]
            )
        )
        try database.close()
        return config
    }

    private typealias RebuildReceiptFixture = (
        client: SynchroClient,
        database: SynchroDatabase,
        config: SynchroConfig
    )

    private func makeRebuildReceiptFixture() throws -> RebuildReceiptFixture {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let database = try SynchroDatabase(path: config.dbPath)
        let tables = try XCTUnwrap(try database.readTransaction {
            try SynchroMeta.getArchivedSchemaTables($0, version: 1, hash: protocolTestSchemaHash)
        })
        let table = try XCTUnwrap(tables.first)
        let records = try ["o1", "o2", "o3"].map { id in
            let pk = [table.primaryKeyFieldID: AnyCodable(id)]
            let row: [String: AnyCodable] = [
                "id": AnyCodable(id),
                "title": AnyCodable("title-\(id)"),
                "updated_at": AnyCodable("2026-01-01T00:00:0\(id.dropFirst().first!).000000Z"),
                "deleted_at": AnyCodable(NSNull()),
            ]
            let digest = try Integrity.rowDigest(
                schemaHash: protocolTestSchemaHash,
                table: table,
                pk: pk,
                row: row,
                serverVersion: "2026-01-01T00:00:0\(id.dropFirst().first!).000000Z"
            )
            return RebuildRecord(
                table: table.tableID,
                pk: pk,
                row: row,
                rowChecksum: digest.checksum,
                serverVersion: "2026-01-01T00:00:0\(id.dropFirst().first!).000000Z"
            )
        }
        let entries = try records.map { record in
            try Integrity.rowDigest(
                schemaHash: protocolTestSchemaHash,
                table: table,
                pk: record.pk,
                row: record.row,
                serverVersion: record.serverVersion
            )
        }
        let finalChecksum = try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: "proof-scope",
            entries: entries.map { (identity: $0.identity, digest: $0.checksum) }
        )
        let firstRequest = RebuildRequest(
            clientID: "inspection-device",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            scope: "proof-scope",
            rebuildID: "proof-rebuild",
            cursor: nil,
            limit: 2
        )
        let secondRequest = RebuildRequest(
            clientID: "inspection-device",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            scope: "proof-scope",
            rebuildID: "proof-rebuild",
            cursor: "page-2",
            limit: 2
        )
        let firstResponse = RebuildResponse(
            scope: "proof-scope",
            records: Array(records.prefix(2)),
            cursor: "page-2",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        let secondResponse = RebuildResponse(
            scope: "proof-scope",
            records: [records[2]],
            cursor: nil,
            hasMore: false,
            finalScopeCursor: "scope-final",
            checksum: finalChecksum
        )
        try database.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: "proof-scope",
                cursor: "scope-final",
                checksum: try json(finalChecksum),
                generation: 1,
                localChecksum: try json(finalChecksum)
            )
            try SynchroMeta.insertRebuildPageReceipt(
                db,
                scopeID: "proof-scope",
                rebuildID: "proof-rebuild",
                requestCursor: nil,
                requestJSON: try json(firstRequest),
                responseJSON: try json(firstResponse),
                finalScopeCursor: nil,
                finalChecksumJSON: nil
            )
            try SynchroMeta.insertRebuildPageReceipt(
                db,
                scopeID: "proof-scope",
                rebuildID: "proof-rebuild",
                requestCursor: "page-2",
                requestJSON: try json(secondRequest),
                responseJSON: try json(secondResponse),
                finalScopeCursor: "scope-final",
                finalChecksumJSON: try json(finalChecksum)
            )
        }
        return (client, database, config)
    }

    private func updateReceipt(
        _ database: SynchroDatabase,
        requestCursor: String?,
        update: (inout RebuildResponse) throws -> Void
    ) throws {
        let row = try XCTUnwrap(try database.queryOne(
            "SELECT response_json FROM _synchro_rebuild_page_receipts WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?",
            params: ["proof-scope", "proof-rebuild", requestCursor == nil ? 1 : 0, requestCursor ?? ""]
        ))
        let source: String = try XCTUnwrap(row["response_json"])
        var response = try JSONDecoder.synchroDecoder().decode(RebuildResponse.self, from: Data(source.utf8))
        try update(&response)
        try database.writeTransaction { db in
            try db.execute(
                sql: "UPDATE _synchro_rebuild_page_receipts SET response_json = ? WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?",
                arguments: [try json(response), "proof-scope", "proof-rebuild", requestCursor == nil ? 1 : 0, requestCursor ?? ""]
            )
        }
    }

    private func updateReceiptJSON(
        _ database: SynchroDatabase,
        column: String,
        requestCursor: String?,
        update: (inout [String: Any]) throws -> Void
    ) throws {
        guard column == "request_json" || column == "response_json" else {
            XCTFail("unsupported rebuild receipt JSON column")
            return
        }
        let row = try XCTUnwrap(try database.queryOne(
            "SELECT \(column) FROM _synchro_rebuild_page_receipts WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?",
            params: ["proof-scope", "proof-rebuild", requestCursor == nil ? 1 : 0, requestCursor ?? ""]
        ))
        let source: String = try XCTUnwrap(row[column])
        var object = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(source.utf8)) as? [String: Any])
        try update(&object)
        let encoded = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
        let replacement = try XCTUnwrap(String(data: encoded, encoding: .utf8))
        try database.writeTransaction { db in
            try db.execute(
                sql: "UPDATE _synchro_rebuild_page_receipts SET \(column) = ? WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?",
                arguments: [replacement, "proof-scope", "proof-rebuild", requestCursor == nil ? 1 : 0, requestCursor ?? ""]
            )
        }
    }

    private func json<T: Encodable>(_ value: T) throws -> String {
        try XCTUnwrap(String(data: JSONEncoder.synchroEncoder().encode(value), encoding: .utf8))
    }

    private func closeRebuildReceiptFixture(_ fixture: RebuildReceiptFixture) async throws {
        try await fixture.client.close()
        try fixture.database.close()
        removeDatabase(at: fixture.config.dbPath)
    }

    private func removeDatabase(at path: String) {
        let fileManager = FileManager.default
        for suffix in ["", "-journal", "-wal", "-shm"] {
            try? fileManager.removeItem(atPath: path + suffix)
        }
    }
}
