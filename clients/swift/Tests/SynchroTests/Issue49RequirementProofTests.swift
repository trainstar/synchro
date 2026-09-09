import Foundation
import XCTest
@preconcurrency import GRDB
@testable @_spi(Inspection) import Synchro

final class Issue49RequirementProofTests: XCTestCase {
    private struct TableFixture {
        let name: String
        let seededID: String
        let insertedID: String
        let updateColumn: String
        let cloneSQL: String
    }

    private struct QueueObservation: Equatable {
        let mutationID: String
        let localOrder: Int64
        let tableName: String
        let recordID: String
        let operation: Synchro.Operation
        let baseVersion: String?
        let authoredFields: [AuthoredMutationField]
    }

    private let fixtures = [
        TableFixture(
            name: "regions",
            seededID: "10000000-0000-0000-0000-000000000001",
            insertedID: "20000000-0000-0000-0000-000000000001",
            updateColumn: "name",
            cloneSQL: """
                INSERT INTO regions
                SELECT created_at, updated_at, ?, description, deleted_at, name
                FROM regions WHERE id = ?
                """
        ),
        TableFixture(
            name: "nations",
            seededID: "10000000-0000-0000-0000-000000000002",
            insertedID: "20000000-0000-0000-0000-000000000002",
            updateColumn: "name",
            cloneSQL: """
                INSERT INTO nations
                SELECT iso_code, metadata, deleted_at, updated_at, name, created_at, region_id, ?
                FROM nations WHERE id = ?
                """
        ),
        TableFixture(
            name: "suppliers",
            seededID: "10000000-0000-0000-0000-000000000003",
            insertedID: "20000000-0000-0000-0000-000000000003",
            updateColumn: "name",
            cloneSQL: """
                INSERT INTO suppliers
                SELECT address, created_at, updated_at, phone, name, website, deleted_at,
                       rating, tags, ?, nation_id, is_active
                FROM suppliers WHERE id = ?
                """
        ),
        TableFixture(
            name: "parts",
            seededID: "10000000-0000-0000-0000-000000000004",
            insertedID: "20000000-0000-0000-0000-000000000004",
            updateColumn: "name",
            cloneSQL: """
                INSERT INTO parts
                SELECT ?, name, description, brand, tags, created_at, specifications, updated_at,
                       weight_kg, retail_price, size_cm, deleted_at, part_type, manufacturer
                FROM parts WHERE id = ?
                """
        ),
        TableFixture(
            name: "part_suppliers",
            seededID: "10000000-0000-0000-0000-000000000005",
            insertedID: "20000000-0000-0000-0000-000000000005",
            updateColumn: "notes",
            cloneSQL: """
                INSERT INTO part_suppliers
                SELECT part_id, notes, deleted_at, supply_cost, updated_at, created_at,
                       lead_time_days, supplier_id, available_quantity, ?
                FROM part_suppliers WHERE id = ?
                """
        ),
        TableFixture(
            name: "categories",
            seededID: "10000000-0000-0000-0000-000000000006",
            insertedID: "20000000-0000-0000-0000-000000000006",
            updateColumn: "name",
            cloneSQL: """
                INSERT INTO categories
                SELECT ?, updated_at, parent_id, deleted_at, created_at, metadata, sort_order, name
                FROM categories WHERE id = ?
                """
        ),
    ]

    func testRegisteredLocalSQLCrudProducesCanonicalMutationsAndRejectsIncompleteObservations() async throws {
        for operation in [Synchro.Operation.insert, .update, .delete] {
            let databasePath = temporaryDatabasePath("crud-\(operation.rawValue)")
            let client = try SynchroClient(config: config(databasePath: databasePath, seedPath: canonicalSeedPath()))
            defer { removeDatabaseFamily(databasePath) }

            for fixture in fixtures {
                switch operation {
                case .insert:
                    XCTAssertEqual(
                        try client.execute(fixture.cloneSQL, params: [fixture.insertedID, fixture.seededID]).rowsAffected,
                        1
                    )
                case .update:
                    XCTAssertEqual(
                        try client.execute(
                            "UPDATE \(fixture.name) SET \(fixture.updateColumn) = ? WHERE id = ?",
                            params: ["Issue 49 update", fixture.seededID]
                        ).rowsAffected,
                        1
                    )
                case .delete:
                    _ = try client.execute("DELETE FROM \(fixture.name) WHERE id = ?", params: [fixture.seededID])
                    XCTAssertNotNil(
                        try client.queryOne(
                            "SELECT deleted_at FROM \(fixture.name) WHERE id = ? AND deleted_at IS NOT NULL",
                            params: [fixture.seededID]
                        )
                    )
                case .upsert:
                    XCTFail("The proof does not request upsert")
                }
            }

            try client.createTable("local_notes", columns: [
                ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
                ColumnDef(name: "body", type: "TEXT"),
            ])
            XCTAssertEqual(
                try client.execute("INSERT INTO local_notes (id, body) VALUES (?, ?)", params: ["note-1", "local"]).rowsAffected,
                1
            )

            let observed = try client.inspectPendingMutations()
            XCTAssertTrue(
                queueMatches(observed, operation: operation),
                "\(operation.rawValue) observed \(observed.map { "\($0.tableName):\($0.operation.rawValue):\($0.status.rawValue)" })"
            )
            XCTAssertEqual(
                try client.pendingChangeCount(),
                fixtures.count,
                "\(operation.rawValue) pending count"
            )

            let missingTableMutant = Array(observed.dropLast())
            XCTAssertFalse(queueMatches(missingTableMutant, operation: operation))

            let wrongOperation = operation == .insert ? Synchro.Operation.update : Synchro.Operation.insert
            XCTAssertFalse(queueMatches(observed, operation: wrongOperation))

            try await client.close()
        }
    }

    func testOpaqueBaseVersionAndAuthoredQueueSurviveRestartAndRejectRewriting() async throws {
        let databasePath = temporaryDatabasePath("durable-queue")
        defer { removeDatabaseFamily(databasePath) }
        let config = config(databasePath: databasePath, seedPath: canonicalSeedPath())
        let category = fixtures.last!
        let first = try SynchroClient(config: config)
        let inspection = SynchroInspection(client: first)
        let authoritativeVersion = try XCTUnwrap(
            inspection.rowMetadata(tableName: category.name, recordID: category.seededID)?.serverVersion
        )
        XCTAssertFalse(authoritativeVersion.isEmpty)

        XCTAssertEqual(
            try first.execute(
                "UPDATE categories SET name = ?, updated_at = ? WHERE id = ?",
                params: ["Offline category", "2026-02-02T00:00:00.000000Z", category.seededID]
            ).rowsAffected,
            1
        )
        let beforeRestart = try XCTUnwrap(first.inspectPendingMutations().only)
        XCTAssertEqual(beforeRestart.baseVersion, authoritativeVersion)
        XCTAssertEqual(beforeRestart.operation, .update)
        XCTAssertTrue(beforeRestart.authoredFields.contains { field in
            field.value.value as? String == "Offline category"
        })
        try await first.close()

        let reopened = try SynchroClient(config: config)
        let afterRestart = try XCTUnwrap(reopened.inspectPendingMutations().only)
        XCTAssertEqual(observation(afterRestart), observation(beforeRestart))
        XCTAssertEqual(
            try reopened.queryOne("SELECT name FROM categories WHERE id = ?", params: [category.seededID])?["name"] as? String,
            "Offline category"
        )

        let rewrittenVersionMutant = QueueObservation(
            mutationID: afterRestart.mutationID,
            localOrder: afterRestart.localOrder,
            tableName: afterRestart.tableName,
            recordID: afterRestart.recordID,
            operation: afterRestart.operation,
            baseVersion: authoritativeVersion + "-rewritten",
            authoredFields: afterRestart.authoredFields
        )
        XCTAssertNotEqual(rewrittenVersionMutant, observation(beforeRestart))

        let regeneratedIdentityMutant = QueueObservation(
            mutationID: UUID().uuidString.lowercased(),
            localOrder: afterRestart.localOrder,
            tableName: afterRestart.tableName,
            recordID: afterRestart.recordID,
            operation: afterRestart.operation,
            baseVersion: afterRestart.baseVersion,
            authoredFields: afterRestart.authoredFields
        )
        XCTAssertNotEqual(regeneratedIdentityMutant, observation(beforeRestart))

        XCTAssertThrowsError(try reopened.execute("DELETE FROM _synchro_pending_changes"))
        XCTAssertEqual(try reopened.inspectPendingMutations().map(observation), [observation(beforeRestart)])
        try await reopened.close()
    }

    func testPortableSeedRejectsQueuedIntentWithoutPublishingMutant() async throws {
        let mutableSeedPath = temporaryDatabasePath("mutated-seed")
        let rejectedDestinationPath = temporaryDatabasePath("rejected-seed")
        defer {
            removeDatabaseFamily(mutableSeedPath)
            removeDatabaseFamily(rejectedDestinationPath)
        }

        let baseline = try SynchroClient(
            config: config(databasePath: mutableSeedPath, seedPath: canonicalSeedPath())
        )
        XCTAssertEqual(
            try baseline.queryOne(
                "SELECT name FROM categories WHERE id = ?",
                params: [fixtures.last!.seededID]
            )?["name"] as? String,
            "Seed Category"
        )
        XCTAssertTrue(try baseline.inspectPendingMutations().isEmpty)

        XCTAssertEqual(
            try baseline.execute(
                "UPDATE categories SET name = ? WHERE id = ?",
                params: ["Unauthorized queued seed intent", fixtures.last!.seededID]
            ).rowsAffected,
            1
        )
        XCTAssertEqual(try baseline.inspectPendingMutations().count, 1)
        try await baseline.close()

        XCTAssertThrowsError(
            try SynchroClient(
                config: config(databasePath: rejectedDestinationPath, seedPath: mutableSeedPath)
            )
        )
        assertNoDatabaseFamily(rejectedDestinationPath)
    }

    func testPullApplyCommitsOpaqueCursorAndChecksumsAtomicallyWithoutEcho() throws {
        let scopeID = "orders:atomic"
        let cursor = "c1.A_-~%2F.雪"
        let (database, processor, table) = try makePullEnvironment("atomic-success")
        let path = database.path
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "opaque-old-cursor",
                checksum: nil,
                generation: 7
            )
        }
        let row = proofRow(recordID: "row-1", value: "server", versionTime: "2026-03-01T00:00:00.000000Z")
        let change = try makeChangeRecord(
            scope: scopeID,
            schema: table,
            op: .upsert,
            pk: ["id": AnyCodable("row-1")],
            row: row,
            serverVersion: "opaque-server-v1"
        )
        let rowDigest = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: table,
            pk: ["id": AnyCodable("row-1")],
            row: row,
            serverVersion: "opaque-server-v1"
        )
        let scopeDigest = try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: scopeID,
            entries: [(identity: rowDigest.identity, digest: rowDigest.checksum)]
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [table],
            scopeCursors: [scopeID: cursor],
            checksums: [scopeID: scopeDigest],
            schemaHash: protocolTestSchemaHash
        )

        XCTAssertEqual(
            try database.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["row-1"])?["ship_address"] as? String,
            "server"
        )
        XCTAssertEqual(try ChangeTracker(database: database).pendingChangeCount(), 0)
        try database.readTransaction { connection in
            XCTAssertEqual(try SynchroMeta.getScope(connection, scopeID: scopeID)?.cursor, cursor)
            XCTAssertEqual(
                try SynchroMeta.getRowVersion(connection, tableName: table.tableName, recordID: "row-1"),
                "opaque-server-v1"
            )
        }
        try database.close()

        let reopened = try SynchroDatabase(path: path)
        try reopened.readTransaction { connection in
            XCTAssertEqual(try SynchroMeta.getScope(connection, scopeID: scopeID)?.cursor, cursor)
            XCTAssertEqual(
                try SynchroMeta.getRowVersion(connection, tableName: table.tableName, recordID: "row-1"),
                "opaque-server-v1"
            )
        }
        XCTAssertEqual(try ChangeTracker(database: reopened).pendingChangeCount(), 0)
        try reopened.close()
        removeDatabaseFamily(path)

        let (mutantDatabase, mutantProcessor, mutantTable) = try makePullEnvironment("atomic-mutant")
        let mutantPath = mutantDatabase.path
        try mutantDatabase.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "opaque-old-cursor",
                checksum: nil,
                generation: 7
            )
        }
        let corrupted = ChangeRecord(
            scope: change.scope,
            table: change.table,
            op: change.op,
            pk: change.pk,
            row: change.row,
            rowChecksum: ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: String(repeating: "f", count: 64)
            ),
            serverVersion: change.serverVersion
        )
        XCTAssertThrowsError(try mutantProcessor.applyScopeChanges(
            changes: [corrupted],
            syncedTables: [mutantTable],
            scopeCursors: [scopeID: cursor],
            checksums: [scopeID: scopeDigest],
            schemaHash: protocolTestSchemaHash
        ))
        XCTAssertNil(try mutantDatabase.queryOne("SELECT id FROM orders WHERE id = ?", params: ["row-1"]))
        XCTAssertEqual(try ChangeTracker(database: mutantDatabase).pendingChangeCount(), 0)
        XCTAssertEqual(
            try mutantDatabase.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor },
            "opaque-old-cursor"
        )
        try mutantDatabase.close()
        let reopenedMutant = try SynchroDatabase(path: mutantPath)
        XCTAssertNil(try reopenedMutant.queryOne("SELECT id FROM orders WHERE id = ?", params: ["row-1"]))
        XCTAssertEqual(
            try reopenedMutant.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor },
            "opaque-old-cursor"
        )
        try reopenedMutant.close()
        removeDatabaseFamily(mutantPath)

        let (healthDatabase, healthProcessor, healthTable) = try makePullEnvironment("cursor-health-mutant")
        let healthPath = healthDatabase.path
        try healthDatabase.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "healthy-old-cursor",
                checksum: nil,
                generation: 7
            )
        }
        let healthChange = try makeChangeRecord(
            scope: scopeID,
            schema: healthTable,
            op: .upsert,
            pk: ["id": AnyCodable("row-1")],
            row: row,
            serverVersion: "opaque-server-v1"
        )
        try healthProcessor.applyScopeChanges(
            changes: [healthChange],
            syncedTables: [healthTable],
            scopeCursors: [scopeID: cursor],
            checksums: [scopeID: protocolEmptyScopeChecksum(scopeID: scopeID)],
            schemaHash: protocolTestSchemaHash
        )
        XCTAssertEqual(
            try healthDatabase.queryOne(
                "SELECT ship_address FROM orders WHERE id = ?",
                params: ["row-1"]
            )?["ship_address"] as? String,
            "server"
        )
        XCTAssertNil(try healthDatabase.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor })
        XCTAssertNil(try healthDatabase.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.checksum })
        XCTAssertEqual(try ChangeTracker(database: healthDatabase).pendingChangeCount(), 0)
        try healthDatabase.close()
        let reopenedHealth = try SynchroDatabase(path: healthPath)
        XCTAssertNil(try reopenedHealth.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor })
        XCTAssertEqual(
            try reopenedHealth.readTransaction {
                try SynchroMeta.getRowVersion($0, tableName: healthTable.tableName, recordID: "row-1")
            },
            "opaque-server-v1"
        )
        try reopenedHealth.close()
        removeDatabaseFamily(healthPath)
    }

    func testRebuildRestartReplayAndFinalizationPreserveUnrelatedAndProtectedRows() throws {
        let targetScope = "orders:target"
        let otherScope = "orders:other"
        let (database, processor, table) = try makePullEnvironment("rebuild")
        let path = database.path
        for recordID in ["orphan", "shared", "protected", "local-only"] {
            try insertProofRow(database, recordID: recordID, value: "local-\(recordID)")
        }
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: targetScope, cursor: "target-old", checksum: nil, generation: 3)
            try SynchroMeta.upsertScope(connection, scopeID: otherScope, cursor: "other-stable", checksum: nil, generation: 8)
            for recordID in ["orphan", "shared", "protected"] {
                try SynchroMeta.upsertScopeRow(
                    connection,
                    scopeID: targetScope,
                    tableName: table.tableName,
                    recordID: recordID,
                    checksum: String(repeating: "0", count: 64),
                    generation: 3
                )
            }
            try SynchroMeta.upsertScopeRow(
                connection,
                scopeID: otherScope,
                tableName: table.tableName,
                recordID: "shared",
                checksum: String(repeating: "1", count: 64),
                generation: 8
            )
        }
        _ = try database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["protected-local-intent", "protected"])
        let protectedMutation = try XCTUnwrap(try ChangeTracker(database: database).inspectPendingMutations().only)

        let attempt = try processor.beginScopeRebuild(
            scopeID: targetScope,
            clientGeneration: 4,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [table]
        )
        XCTAssertNil(try database.queryOne("SELECT id FROM orders WHERE id = 'orphan'", params: nil))
        for retained in ["shared", "protected", "local-only"] {
            XCTAssertNotNil(try database.queryOne("SELECT id FROM orders WHERE id = ?", params: [retained]))
        }
        XCTAssertEqual(try ChangeTracker(database: database).inspectPendingMutations().map(\.mutationID), [protectedMutation.mutationID])
        XCTAssertEqual(
            try database.readTransaction { try SynchroMeta.getScope($0, scopeID: otherScope)?.cursor },
            "other-stable"
        )
        try database.close()

        let reopened = try SynchroDatabase(path: path)
        let restarted = PullProcessor(database: reopened)
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: targetScope) },
            attempt
        )
        let serverRow = proofRow(
            recordID: "protected",
            value: "server-protected",
            versionTime: "2026-03-02T00:00:00.000000Z"
        )
        let rowDigest = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: table,
            pk: ["id": AnyCodable("protected")],
            row: serverRow,
            serverVersion: "opaque-rebuild-v1"
        )
        let record = RebuildRecord(
            table: table.tableID,
            pk: ["id": AnyCodable("protected")],
            row: serverRow,
            rowChecksum: rowDigest.checksum,
            serverVersion: "opaque-rebuild-v1"
        )
        let firstRequest = rebuildRequest(attempt: attempt)
        let firstResponse = RebuildResponse(
            scope: targetScope,
            records: [record],
            cursor: "page-two",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        let continued = try restarted.applyScopeRebuildPage(
            attempt: attempt,
            request: firstRequest,
            requestBody: try wireData(firstRequest),
            response: firstResponse,
            responseBody: try wireData(firstResponse),
            syncedTables: [table]
        )
        let replayed = try restarted.applyScopeRebuildPage(
            attempt: attempt,
            request: firstRequest,
            requestBody: try wireData(firstRequest),
            response: firstResponse,
            responseBody: try wireData(firstResponse),
            syncedTables: [table]
        )
        XCTAssertEqual(replayed, continued)
        XCTAssertEqual(
            try reopened.query("SELECT * FROM _synchro_rebuild_page_receipts", params: nil).count,
            1
        )

        let finalRequest = rebuildRequest(attempt: continued)
        let wrongFinal = RebuildResponse(
            scope: targetScope,
            records: [],
            cursor: nil,
            hasMore: false,
            finalScopeCursor: "target-final",
            checksum: protocolEmptyScopeChecksum(scopeID: targetScope)
        )
        XCTAssertThrowsError(try restarted.applyScopeRebuildPage(
            attempt: continued,
            request: finalRequest,
            requestBody: try wireData(finalRequest),
            response: wrongFinal,
            responseBody: try wireData(wrongFinal),
            syncedTables: [table]
        ))
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: targetScope) },
            continued
        )
        XCTAssertNil(try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.cursor })

        let finalChecksum = try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: targetScope,
            entries: [(identity: rowDigest.identity, digest: rowDigest.checksum)]
        )
        let finalResponse = RebuildResponse(
            scope: targetScope,
            records: [],
            cursor: nil,
            hasMore: false,
            finalScopeCursor: "target-final",
            checksum: finalChecksum
        )
        _ = try restarted.applyScopeRebuildPage(
            attempt: continued,
            request: finalRequest,
            requestBody: try wireData(finalRequest),
            response: finalResponse,
            responseBody: try wireData(finalResponse),
            syncedTables: [table]
        )
        XCTAssertEqual(
            try reopened.queryOne("SELECT ship_address FROM orders WHERE id = 'protected'", params: nil)?["ship_address"] as? String,
            "protected-local-intent"
        )
        XCTAssertNotNil(try reopened.queryOne("SELECT id FROM orders WHERE id = 'shared'", params: nil))
        XCTAssertNotNil(try reopened.queryOne("SELECT id FROM orders WHERE id = 'local-only'", params: nil))
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.cursor },
            "target-final"
        )
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: otherScope)?.cursor },
            "other-stable"
        )
        XCTAssertEqual(try ChangeTracker(database: reopened).inspectPendingMutations().map(\.mutationID), [protectedMutation.mutationID])
        try reopened.close()

        let finalReopen = try SynchroDatabase(path: path)
        XCTAssertNil(try finalReopen.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: targetScope) })
        XCTAssertEqual(
            try finalReopen.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.cursor },
            "target-final"
        )
        try finalReopen.close()
        removeDatabaseFamily(path)
    }

    func testQueueNormalizationAndSealedRetriesKeepExactBytesAcrossResponseFaultsAndRestart() async throws {
        let (database, table) = try makePushEnvironment("sealed-retry")
        let path = database.path
        let tracker = ChangeTracker(database: database)
        var processor = PushProcessor(database: database, changeTracker: tracker)
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["queue-row", "first", "proof-user", "2026-03-03T00:00:00.000000Z"]
        )
        _ = try database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["normalized", "queue-row"])

        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfiguration)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        let http = HttpClient(
            config: SynchroConfig(
                dbPath: path,
                serverURL: URL(string: "http://issue49.invalid")!,
                authProvider: { "token" },
                clientID: "issue-49-client",
                appVersion: "1.0.0"
            ),
            session: session
        )
        let decoder = JSONDecoder.synchroDecoder()
        let encoder = JSONEncoder.synchroEncoder()
        var bodies: [Data] = []
        var responseIndex = 0
        MockURLProtocol.requestHandler = { request in
            let body = try XCTUnwrap(request.bodyData())
            bodies.append(body)
            defer { responseIndex += 1 }
            if responseIndex == 0 || responseIndex == 1 {
                let status = responseIndex == 0 ? 429 : 503
                let code = status == 429 ? "retry_later" : "temporary_unavailable"
                let payload = try JSONSerialization.data(withJSONObject: [
                    "error": ["code": code, "message": "retry", "retryable": true],
                ])
                return (
                    HTTPURLResponse(
                        url: request.url!,
                        statusCode: status,
                        httpVersion: nil,
                        headerFields: ["Retry-After": "1"]
                    )!,
                    payload
                )
            }
            if responseIndex == 2 {
                throw URLError(.networkConnectionLost)
            }
            let push = try decoder.decode(PushRequest.self, from: body)
            let row = self.proofRow(
                recordID: "queue-row",
                value: "normalized",
                versionTime: "2026-03-03T01:00:00.000000Z"
            )
            let accepted = try makeAcceptedMutation(
                mutationID: try XCTUnwrap(push.mutations.first?.mutationID),
                schema: table,
                pk: ["id": AnyCodable("queue-row")],
                status: .applied,
                serverRow: row,
                serverVersion: "opaque-accepted-v2"
            )
            return (
                HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: nil,
                    headerFields: ["Content-Type": "application/json"]
                )!,
                try encoder.encode(PushResponse(
                    batchID: push.batchID,
                    serverTime: "2026-03-03T01:00:00.000000Z",
                    accepted: [accepted],
                    rejected: []
                ))
            )
        }

        for _ in 0..<2 {
            do {
                _ = try await processor.processPush(
                    httpClient: http,
                    clientID: "issue-49-client",
                    clientGeneration: 1,
                    schemaVersion: 1,
                    schemaHash: protocolTestSchemaHash,
                    syncedTables: [table]
                )
                XCTFail("Retryable response was accepted")
            } catch is RetryableError {
            }
        }
        let sealedRequest = try decoder.decode(PushRequest.self, from: bodies[0])
        XCTAssertEqual(sealedRequest.mutations.count, 1)
        XCTAssertEqual(sealedRequest.mutations[0].op, .insert)
        XCTAssertEqual(sealedRequest.mutations[0].columns?["ship_address"], AnyCodable("normalized"))
        XCTAssertEqual(bodies[0], bodies[1])
        let normalizedLedger = try database.query(
            "SELECT lifecycle_state, normalized_mutation_id, mutation_id FROM _synchro_pending_changes ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(normalizedLedger.map { $0["lifecycle_state"] as String? }, ["superseded_before_send", "superseded_before_send", "sealed"])

        _ = try database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["successor", "queue-row"])
        do {
            _ = try await processor.processPush(
                httpClient: http,
                clientID: "issue-49-client",
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [table]
            )
            XCTFail("Response loss was accepted")
        } catch is RetryableError {
        }
        XCTAssertEqual(bodies[0], bodies[2])
        try database.close()

        let reopened = try SynchroDatabase(path: path)
        let reopenedTracker = ChangeTracker(database: reopened)
        processor = PushProcessor(database: reopened, changeTracker: reopenedTracker)
        _ = try await processor.processPush(
            httpClient: http,
            clientID: "issue-49-client",
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            syncedTables: [table]
        )
        XCTAssertEqual(bodies[0], bodies[3])
        let successor = try XCTUnwrap(try reopenedTracker.pendingChanges().only)
        XCTAssertNotEqual(successor.mutationID, sealedRequest.mutations[0].mutationID)
        XCTAssertNil(successor.dependencyMutationID)
        XCTAssertEqual(successor.baseUpdatedAt, "opaque-accepted-v2")
        let acceptedLedger = try XCTUnwrap(try reopened.queryOne(
            "SELECT lifecycle_state, accepted_json FROM _synchro_pending_changes WHERE mutation_id = ?",
            params: [sealedRequest.mutations[0].mutationID]
        ))
        XCTAssertEqual(acceptedLedger["lifecycle_state"] as String?, "accepted")
        XCTAssertNotNil(acceptedLedger["accepted_json"] as String?)
        XCTAssertEqual(
            try reopened.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["queue-row"])?["ship_address"] as? String,
            "successor"
        )
        try reopened.close()
        removeDatabaseFamily(path)

        let (cancelDatabase, cancelTable) = try makePushEnvironment("cancel-chain")
        let cancelPath = cancelDatabase.path
        _ = try cancelDatabase.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["temporary", "temporary", "proof-user", "2026-03-03T00:00:00.000000Z"]
        )
        _ = try cancelDatabase.execute("DELETE FROM orders WHERE id = ?", params: ["temporary"])
        let cancelTracker = ChangeTracker(database: cancelDatabase)
        let cancelProcessor = PushProcessor(database: cancelDatabase, changeTracker: cancelTracker)
        let cancelledPush = try await cancelProcessor.processPush(
            httpClient: http,
            clientID: "issue-49-client",
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            syncedTables: [cancelTable]
        )
        XCTAssertNil(cancelledPush)
        XCTAssertFalse(try cancelTracker.hasPendingChanges())
        XCTAssertEqual(
            try cancelDatabase.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                params: nil
            ).map { $0["lifecycle_state"] as String? },
            ["cancelled_before_send", "cancelled_before_send"]
        )
        try cancelDatabase.close()
        removeDatabaseFamily(cancelPath)
    }

    func testStateTransitionGraphAndBlockingFailureRejectUnlistedOrMalformedState() throws {
        let allowed: [SyncStatus: Set<SyncStatus>] = [
            .uninitialized: [.localReady, .error, .stopped],
            .localReady: [.connecting, .error, .stopped],
            .connecting: [.schemaApplying, .ready, .backoff, .error, .stopped],
            .schemaApplying: [.ready, .rebuilding, .error, .stopped],
            .ready: [.connecting, .pushing, .pulling, .rebuilding, .error, .stopped],
            .pushing: [.pushing, .ready, .pulling, .connecting, .backoff, .error, .stopped],
            .pulling: [.pulling, .ready, .rebuilding, .connecting, .backoff, .error, .stopped],
            .rebuilding: [.rebuilding, .ready, .connecting, .backoff, .error, .stopped],
            .backoff: [.connecting, .pushing, .pulling, .rebuilding, .error, .stopped],
            .error: [.localReady, .stopped],
            .stopped: [.localReady],
        ]
        for current in SyncStatus.allCases {
            for next in SyncStatus.allCases {
                XCTAssertEqual(
                    current.permitsTransition(to: next),
                    allowed[current, default: []].contains(next),
                    "\(current.rawValue) -> \(next.rawValue)"
                )
            }
        }
        var unlistedEdgeMutant = allowed
        unlistedEdgeMutant[.stopped, default: []].insert(.pulling)
        XCTAssertNotEqual(
            SyncStatus.stopped.permitsTransition(to: .pulling),
            unlistedEdgeMutant[.stopped]!.contains(.pulling)
        )

        let path = temporaryDatabasePath("blocking-failure")
        let database = try SynchroDatabase(path: path)
        let failure = SyncFailure(
            operation: .pulling,
            code: .syncIntegrityFailure,
            retryable: false,
            message: "invalid terminal checksum",
            recoveryAction: .schemaReset,
            metadata: ["scope": "fingerprint"]
        )
        try database.writeTransaction { try SynchroMeta.setBlockingFailure($0, failure: failure) }
        try database.close()
        let reopened = try SynchroDatabase(path: path)
        XCTAssertEqual(try reopened.readTransaction { try SynchroMeta.getBlockingFailure($0) }, failure)
        try reopened.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_blocking_error SET operation = 'unknown' WHERE singleton = 1"
            )
        }
        XCTAssertThrowsError(try reopened.readTransaction { try SynchroMeta.getBlockingFailure($0) })
        try reopened.close()
        removeDatabaseFamily(path)
    }

    func testRejectedOutcomeAndBlockedSuccessorRemainLinkedAndInspectableAcrossRestart() throws {
        let (database, table) = try makePushEnvironment("rejected-successor")
        let path = database.path
        let tracker = ChangeTracker(database: database)
        let processor = PushProcessor(database: database, changeTracker: tracker)
        try database.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: ["rejected-row", "server-base", "proof-user", "2026-03-04T00:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: table.tableName,
                recordID: "rejected-row",
                serverVersion: "opaque-base-v1",
                rowChecksum: nil
            )
        }
        _ = try database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["first-local", "rejected-row"]
        )
        let predecessor = try XCTUnwrap(try tracker.pendingChanges().only)
        try database.writeTransaction { connection in
            try tracker.markPendingAsSealed(
                connection,
                batchID: UUID().uuidString.lowercased(),
                pending: [predecessor]
            )
        }
        _ = try database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["successor-local", "rejected-row"]
        )
        let authoritativeRow = proofRow(
            recordID: "rejected-row",
            value: "server-conflict",
            versionTime: "2026-03-04T01:00:00.000000Z"
        )
        let rejection = try makeRejectedMutation(
            mutationID: predecessor.mutationID,
            schema: table,
            pk: ["id": AnyCodable("rejected-row")],
            status: .conflict,
            code: .versionConflict,
            message: "conflict",
            serverRow: authoritativeRow,
            serverVersion: "opaque-conflict-v2"
        )
        let wrongIdentityMutant = try makeRejectedMutation(
            mutationID: UUID().uuidString.lowercased(),
            schema: table,
            pk: ["id": AnyCodable("rejected-row")],
            status: .conflict,
            code: .versionConflict,
            message: "conflict",
            serverRow: authoritativeRow,
            serverVersion: "opaque-conflict-v2"
        )
        XCTAssertThrowsError(try processor.applyRejected(
            rejected: [wrongIdentityMutant],
            syncedTables: [table],
            sentPending: [predecessor.mutationID: predecessor]
        ))
        XCTAssertEqual(
            try database.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                params: nil
            ).map { $0["lifecycle_state"] as String? },
            ["sealed", "unsealed"]
        )
        XCTAssertTrue(try database.readTransaction { try SynchroMeta.listRejectedMutations($0).isEmpty })

        _ = try processor.applyRejected(
            rejected: [rejection],
            syncedTables: [table],
            sentPending: [predecessor.mutationID: predecessor]
        )
        let successor = try XCTUnwrap(try database.queryOne(
            "SELECT mutation_id, lifecycle_state, dependency_mutation_id, base_version FROM _synchro_pending_changes WHERE mutation_id <> ?",
            params: [predecessor.mutationID]
        ))
        XCTAssertEqual(successor["lifecycle_state"] as String?, "blocked_by_predecessor")
        XCTAssertEqual(successor["dependency_mutation_id"] as String?, predecessor.mutationID)
        XCTAssertNil(successor["base_version"] as String?)
        XCTAssertEqual(
            try database.queryOne(
                "SELECT ship_address FROM orders WHERE id = ?",
                params: ["rejected-row"]
            )?["ship_address"] as? String,
            "successor-local"
        )
        try database.close()

        let reopened = try SynchroDatabase(path: path)
        let rejections = try reopened.readTransaction { try SynchroMeta.listRejectedMutations($0) }
        XCTAssertEqual(rejections.count, 1)
        XCTAssertEqual(rejections[0].mutationID, predecessor.mutationID)
        XCTAssertEqual(rejections[0].status, MutationStatus.conflict.rawValue)
        XCTAssertEqual(rejections[0].code, MutationRejectionCode.versionConflict.rawValue)
        XCTAssertEqual(rejections[0].serverVersion, "opaque-conflict-v2")
        let reopenedSuccessor = try XCTUnwrap(try reopened.queryOne(
            "SELECT mutation_id, lifecycle_state, dependency_mutation_id, base_version FROM _synchro_pending_changes WHERE mutation_id <> ?",
            params: [predecessor.mutationID]
        ))
        let rejectedLedger = try XCTUnwrap(try reopened.queryOne(
            "SELECT lifecycle_state, rejected_json FROM _synchro_pending_changes WHERE mutation_id = ?",
            params: [predecessor.mutationID]
        ))
        XCTAssertEqual(rejectedLedger["lifecycle_state"] as String?, "rejected")
        XCTAssertNotNil(rejectedLedger["rejected_json"] as String?)
        XCTAssertNotEqual(reopenedSuccessor["mutation_id"] as String?, predecessor.mutationID)
        XCTAssertEqual(reopenedSuccessor["lifecycle_state"] as String?, "blocked_by_predecessor")
        XCTAssertEqual(reopenedSuccessor["dependency_mutation_id"] as String?, predecessor.mutationID)
        XCTAssertNil(reopenedSuccessor["base_version"] as String?)
        try reopened.close()
        removeDatabaseFamily(path)
    }

    private var proofTable: LocalSchemaTable {
        SchemaTable(
            tableName: "orders",
            pushPolicy: "owner_only",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [
                SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                SchemaColumn(name: "ship_address", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
                SchemaColumn(name: "user_id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
            ]
        )
    }

    private func makePullEnvironment(
        _ label: String
    ) throws -> (SynchroDatabase, PullProcessor, LocalSchemaTable) {
        let database = try SynchroDatabase(path: temporaryDatabasePath(label))
        let table = proofTable
        try SchemaManager(database: database).createSyncedTables(
            schema: SchemaResponse(
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                serverTime: Date(),
                tables: [table]
            )
        )
        return (database, PullProcessor(database: database), table)
    }

    private func makePushEnvironment(_ label: String) throws -> (SynchroDatabase, LocalSchemaTable) {
        let (database, _, table) = try makePullEnvironment(label)
        return (database, table)
    }

    private func proofRow(
        recordID: String,
        value: String,
        versionTime: String
    ) -> [String: AnyCodable] {
        [
            "id": AnyCodable(recordID),
            "ship_address": AnyCodable(value),
            "user_id": AnyCodable("proof-user"),
            "updated_at": AnyCodable(versionTime),
            "deleted_at": AnyCodable(NSNull()),
        ]
    }

    private func insertProofRow(
        _ database: SynchroDatabase,
        recordID: String,
        value: String
    ) throws {
        try database.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: [recordID, value, "proof-user", "2026-03-01T00:00:00.000000Z"]
            )
        }
    }

    private func rebuildRequest(attempt: LocalRebuildAttempt) -> RebuildRequest {
        RebuildRequest(
            clientID: "issue-49-client",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: attempt.scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
    }

    private func wireData<T: Encodable>(_ value: T) throws -> Data {
        try JSONEncoder.synchroEncoder().encode(value)
    }

    private func queueMatches(_ observed: [PendingMutationInspection], operation: Synchro.Operation) -> Bool {
        let expectedRecords = Set(fixtures.map { fixture in
            "\(fixture.name):\(operation == .insert ? fixture.insertedID : fixture.seededID)"
        })
        let actualRecords = Set(observed.map { "\($0.tableName):\($0.recordID)" })
        return observed.count == fixtures.count &&
            actualRecords == expectedRecords &&
            observed.allSatisfy { $0.operation == operation && $0.status == .pending }
    }

    private func observation(_ mutation: PendingMutationInspection) -> QueueObservation {
        QueueObservation(
            mutationID: mutation.mutationID,
            localOrder: mutation.localOrder,
            tableName: mutation.tableName,
            recordID: mutation.recordID,
            operation: mutation.operation,
            baseVersion: mutation.baseVersion,
            authoredFields: mutation.authoredFields
        )
    }

    private func config(databasePath: String, seedPath: String) -> SynchroConfig {
        SynchroConfig(
            dbPath: databasePath,
            serverURL: URL(string: "http://127.0.0.1:1")!,
            authProvider: { "unused" },
            clientID: "issue-49-native-proof",
            appVersion: "1.0.0",
            seedDatabasePath: seedPath
        )
    }

    private func canonicalSeedPath() -> String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .appendingPathComponent("../../../react-native/example/seed.db")
            .standardizedFileURL.path
    }

    private func temporaryDatabasePath(_ label: String) -> String {
        URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("synchro-issue-49-\(label)-\(UUID().uuidString).sqlite")
            .path
    }

    private func removeDatabaseFamily(_ path: String) {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            try? FileManager.default.removeItem(atPath: path + suffix)
        }
    }

    private func assertNoDatabaseFamily(
        _ path: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: path + suffix), file: file, line: line)
        }
    }
}

private extension Array {
    var only: Element? {
        count == 1 ? self[0] : nil
    }
}
