import Foundation
import XCTest
import os
@preconcurrency import GRDB
@testable @_spi(Inspection) import Synchro

final class Issue49CompleteRequirementProofTests: XCTestCase {
    private struct Environment {
        let path: String
        let database: SynchroDatabase
        let table: LocalSchemaTable
    }

    private struct PortableSeed {
        let path: String
        let scopeID: String
        let table: LocalSchemaTable
        let rowDigest: (identity: Data, checksum: ChecksumObject)
        let receipt: String
    }

    private var temporaryPaths: [String] = []
    private var sessions: [URLSession] = []

    override func tearDown() {
        MockURLProtocol.requestHandler = nil
        sessions.forEach { $0.invalidateAndCancel() }
        sessions.removeAll()
        temporaryPaths.forEach(removeDatabaseFamily)
        temporaryPaths.removeAll()
        super.tearDown()
    }

    func testAcceptedApplyAndPullEchoRemainSingleAndRejectChangedReplayAtomically() throws {
        let scopeID = "orders:echo"
        let environment = try makeEnvironment("apply-echo")
        let database = environment.database
        let tracker = ChangeTracker(database: database)
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "cursor-before-write",
                checksum: nil
            )
        }
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["echo-row", "local", "u1", "2026-04-01T00:00:00.000000Z"]
        )
        let pending = try XCTUnwrap(try tracker.pendingChanges().only)
        let serverRow = orderRow(
            recordID: "echo-row",
            address: "server-canonical",
            updatedAt: "2026-04-01T01:00:00.000000Z"
        )
        let accepted = try makeAcceptedMutation(
            mutationID: pending.mutationID,
            schema: environment.table,
            pk: orderPK("echo-row"),
            serverRow: serverRow,
            serverVersion: "opaque-server-version-1"
        )
        _ = try PushProcessor(database: database, changeTracker: tracker).applyAccepted(
            accepted: [accepted],
            syncedTables: [environment.table],
            sentPending: [pending.mutationID: pending]
        )

        let echo = ChangeRecord(
            scope: scopeID,
            table: environment.table.tableID,
            op: .upsert,
            pk: orderPK("echo-row"),
            row: serverRow,
            rowChecksum: accepted.rowChecksum,
            serverVersion: accepted.serverVersion
        )
        let checksum = try scopeChecksum(scopeID: scopeID, changes: [echo], table: environment.table)
        let pull = PullProcessor(database: database)
        try pull.applyScopeChanges(
            changes: [echo],
            syncedTables: [environment.table],
            scopeCursors: [scopeID: "cursor-after-echo"],
            checksums: [scopeID: checksum],
            schemaHash: protocolTestSchemaHash
        )

        XCTAssertEqual(try tracker.pendingChangeCount(), 0)
        XCTAssertEqual(
            try database.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                params: nil
            ).map { $0["lifecycle_state"] as String? },
            ["accepted"]
        )
        XCTAssertEqual(
            try database.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["echo-row"])?["ship_address"] as String?,
            "server-canonical"
        )

        let replayBaseline = try durableSnapshot(database)
        try pull.applyScopeChanges(
            changes: [echo],
            syncedTables: [environment.table],
            scopeCursors: [scopeID: "cursor-after-echo"],
            checksums: [scopeID: checksum],
            schemaHash: protocolTestSchemaHash
        )
        try assertNoDurableProgress(replayBaseline, database)

        var changedEcho = echo
        changedEcho.row = orderRow(
            recordID: "echo-row",
            address: "tampered-echo",
            updatedAt: "2026-04-01T01:00:00.000000Z"
        )
        let beforeMutant = try durableSnapshot(database)
        XCTAssertThrowsError(try pull.applyScopeChanges(
            changes: [changedEcho],
            syncedTables: [environment.table],
            scopeCursors: [scopeID: "cursor-mutant"],
            checksums: [scopeID: checksum],
            schemaHash: protocolTestSchemaHash
        ))
        try assertNoDurableProgress(beforeMutant, database)
        try database.close()
    }

    func testRebuildAndAssignmentCleanupRemainScopeLocalAndPreservePendingIntent() throws {
        let targetScope = "orders:target"
        let otherScope = "orders:other"
        let removedScope = "orders:removed"
        let environment = try makeEnvironment("rebuild-isolation")
        let database = environment.database
        for recordID in ["orphan", "shared", "protected", "local-only"] {
            try insertOrderWithoutCapture(database, recordID: recordID, address: "local-\(recordID)")
        }
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: targetScope, cursor: "target-old", checksum: nil, generation: 3)
            try SynchroMeta.upsertScope(connection, scopeID: otherScope, cursor: "other-stable", checksum: nil, generation: 8)
            for recordID in ["orphan", "shared", "protected"] {
                try SynchroMeta.upsertScopeRow(
                    connection,
                    scopeID: targetScope,
                    tableName: "orders",
                    recordID: recordID,
                    checksum: String(repeating: "0", count: 64),
                    generation: 3
                )
            }
            try SynchroMeta.upsertScopeRow(
                connection,
                scopeID: otherScope,
                tableName: "orders",
                recordID: "shared",
                checksum: String(repeating: "1", count: 64),
                generation: 8
            )
        }
        _ = try database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["protected-local-intent", "protected"]
        )
        let pendingID = try XCTUnwrap(
            try ChangeTracker(database: database).inspectPendingMutations().only
        ).mutationID
        let pull = PullProcessor(database: database)
        let attempt = try pull.beginScopeRebuild(
            scopeID: targetScope,
            clientGeneration: 4,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [environment.table]
        )

        XCTAssertNil(try database.queryOne("SELECT id FROM orders WHERE id = 'orphan'", params: nil))
        for recordID in ["shared", "protected", "local-only"] {
            XCTAssertNotNil(try database.queryOne("SELECT id FROM orders WHERE id = ?", params: [recordID]))
        }
        try database.close()

        let reopened = try SynchroDatabase(path: environment.path)
        let restarted = PullProcessor(database: reopened)
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: targetScope) },
            attempt
        )
        let request = rebuildRequest(attempt)
        let wrongFinal = RebuildResponse(
            scope: targetScope,
            records: [],
            cursor: nil,
            hasMore: false,
            finalScopeCursor: "target-final",
            checksum: ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: String(repeating: "f", count: 64)
            )
        )
        let beforeWrongFinal = try durableSnapshot(reopened)
        XCTAssertThrowsError(try restarted.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try wireData(request),
            response: wrongFinal,
            responseBody: try wireData(wrongFinal),
            syncedTables: [environment.table]
        ))
        try assertNoDurableProgress(beforeWrongFinal, reopened)

        var finalResponse = wrongFinal
        finalResponse.checksum = protocolEmptyScopeChecksum(scopeID: targetScope)
        _ = try restarted.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try wireData(request),
            response: finalResponse,
            responseBody: try wireData(finalResponse),
            syncedTables: [environment.table]
        )
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.cursor },
            "target-final"
        )
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: otherScope)?.cursor },
            "other-stable"
        )
        XCTAssertNotNil(try reopened.queryOne("SELECT id FROM orders WHERE id = 'shared'", params: nil))
        XCTAssertNotNil(try reopened.queryOne("SELECT id FROM orders WHERE id = 'local-only'", params: nil))
        XCTAssertEqual(
            try reopened.queryOne("SELECT ship_address FROM orders WHERE id = 'protected'", params: nil)?["ship_address"] as String?,
            "protected-local-intent"
        )

        try reopened.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: removedScope, cursor: "remove-old", checksum: nil)
            try SynchroMeta.upsertScopeRow(
                connection,
                scopeID: removedScope,
                tableName: "orders",
                recordID: "protected",
                checksum: String(repeating: "2", count: 64),
                generation: 0
            )
        }
        let removalMutant = ChangeRecord(
            scope: removedScope,
            table: environment.table.tableID,
            op: .upsert,
            pk: orderPK("protected"),
            row: orderRow(recordID: "protected", address: "mutant", updatedAt: "2026-04-02T00:00:00.000000Z"),
            rowChecksum: ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: String(repeating: "f", count: 64)
            ),
            serverVersion: "mutant-version"
        )
        let beforeRemovalMutant = try durableSnapshot(reopened)
        XCTAssertThrowsError(try restarted.applyScopeChanges(
            changes: [removalMutant],
            syncedTables: [environment.table],
            scopeCursors: [:],
            checksums: nil,
            schemaHash: protocolTestSchemaHash,
            scopeUpdates: ScopeAssignmentDelta(add: [], remove: [removedScope]),
            scopeSetVersion: 9
        ))
        try assertNoDurableProgress(beforeRemovalMutant, reopened)

        try restarted.applyScopeChanges(
            changes: [],
            syncedTables: [environment.table],
            scopeCursors: [:],
            checksums: nil,
            schemaHash: protocolTestSchemaHash,
            scopeUpdates: ScopeAssignmentDelta(add: [], remove: [removedScope]),
            scopeSetVersion: 9
        )
        XCTAssertNil(try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: removedScope) })
        XCTAssertNotNil(try reopened.queryOne("SELECT id FROM orders WHERE id = 'protected'", params: nil))
        XCTAssertEqual(
            try ChangeTracker(database: reopened).inspectPendingMutations().map(\.mutationID),
            [pendingID]
        )
        try reopened.close()
    }

    func testSQLiteApplyFailureCannotAdvanceCursorOrLeavePartialRows() throws {
        let scopeID = "orders:atomic-failure"
        let environment = try makeEnvironment("cursor-atomicity")
        let database = environment.database
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: scopeID, cursor: "cursor-before", checksum: nil)
            try connection.execute(sql: """
                CREATE TRIGGER reject_issue_49_apply
                BEFORE INSERT ON orders
                WHEN NEW.id = 'fault-row'
                BEGIN
                    SELECT RAISE(ABORT, 'injected apply failure');
                END
                """)
        }
        let change = try canonicalChange(
            scopeID: scopeID,
            recordID: "fault-row",
            address: "server",
            serverVersion: "atomic-version",
            table: environment.table
        )
        let checksum = try scopeChecksum(scopeID: scopeID, changes: [change], table: environment.table)
        let beforeFault = try durableSnapshot(database)
        XCTAssertThrowsError(try PullProcessor(database: database).applyScopeChanges(
            changes: [change],
            syncedTables: [environment.table],
            scopeCursors: [scopeID: "cursor-after"],
            checksums: [scopeID: checksum],
            schemaHash: protocolTestSchemaHash
        ))
        try assertNoDurableProgress(beforeFault, database)
        XCTAssertNil(try database.queryOne("SELECT id FROM orders WHERE id = 'fault-row'", params: nil))
        XCTAssertEqual(
            try database.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor },
            "cursor-before"
        )

        try database.writeTransaction { try $0.execute(sql: "DROP TRIGGER reject_issue_49_apply") }
        try PullProcessor(database: database).applyScopeChanges(
            changes: [change],
            syncedTables: [environment.table],
            scopeCursors: [scopeID: "cursor-after"],
            checksums: [scopeID: checksum],
            schemaHash: protocolTestSchemaHash
        )
        try database.close()

        let reopened = try SynchroDatabase(path: environment.path)
        XCTAssertNotNil(try reopened.queryOne("SELECT id FROM orders WHERE id = 'fault-row'", params: nil))
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor },
            "cursor-after"
        )
        XCTAssertEqual(
            try reopened.readTransaction {
                try SynchroMeta.getRowVersion($0, tableName: "orders", recordID: "fault-row")
            },
            "atomic-version"
        )
        try reopened.close()
    }

    func testTypedRowsChecksumsVersionsAndTerminalMapsRejectEveryMutatedEncoding() async throws {
        let scopeID = "typed:integrity"
        let table = typedTable()
        let environment = try makeEnvironment("typed-integrity", table: table)
        let database = environment.database
        try database.writeTransaction {
            try SynchroMeta.upsertScope($0, scopeID: scopeID, cursor: "typed-old", checksum: nil)
        }
        let pk = ["field-id": AnyCodable("typed-row")]
        let validRow = typedRow(recordID: "typed-row")
        let checksum = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: table,
            pk: pk,
            row: validRow,
            serverVersion: "typed-version"
        ).checksum
        let validChange = ChangeRecord(
            scope: scopeID,
            table: table.tableID,
            op: .upsert,
            pk: pk,
            row: validRow,
            rowChecksum: checksum,
            serverVersion: "typed-version"
        )
        var rowMutants: [(String, [String: AnyCodable])] = []
        rowMutants.append(("unknown field", replacing(validRow, key: "field-unknown", value: AnyCodable("unknown"))))
        rowMutants.append(("omitted field", removing(validRow, key: "field-title")))
        rowMutants.append(("physical alias", replacing(removing(validRow, key: "field-title"), key: "title", value: AnyCodable("alias"))))
        rowMutants.append(("alternate case", replacing(removing(validRow, key: "field-title"), key: "FIELD-TITLE", value: AnyCodable("case"))))
        rowMutants.append(("string as number", replacing(validRow, key: "field-title", value: AnyCodable(Int64(7)))))
        rowMutants.append(("int as string", replacing(validRow, key: "field-count", value: AnyCodable("7"))))
        rowMutants.append(("int64 as number", replacing(validRow, key: "field-large-count", value: AnyCodable(Int64(8)))))
        rowMutants.append(("boolean as SQLite integer", replacing(validRow, key: "field-enabled", value: AnyCodable(Int64(1)))))
        rowMutants.append(("decimal as JSON number", replacing(validRow, key: "field-amount", value: AnyCodable(12.3))))
        rowMutants.append(("JSON as object", replacing(validRow, key: "field-document", value: AnyCodable(["a": 1]))))
        rowMutants.append(("bytes as padded base64", replacing(validRow, key: "field-payload", value: AnyCodable("AAEC="))))
        rowMutants.append(("datetime as number", replacing(validRow, key: "field-updated-at", value: AnyCodable(Int64(1_775_001_600)))))

        for (name, row) in rowMutants {
            var mutant = validChange
            mutant.row = row
            let before = try durableSnapshot(database)
            XCTAssertThrowsError(try PullProcessor(database: database).applyScopeChanges(
                changes: [mutant],
                syncedTables: [table],
                scopeCursors: [scopeID: "cursor-\(name)"],
                checksums: nil,
                schemaHash: protocolTestSchemaHash
            ), "accepted typed-row mutant: \(name)")
            try assertNoDurableProgress(before, database)
        }

        var wrongPK = validChange
        wrongPK.pk = ["field-id": AnyCodable("different-row")]
        var wrongVersion = validChange
        wrongVersion.serverVersion = "different-version"
        var wrongDigest = validChange
        wrongDigest.rowChecksum = ChecksumObject(
            algorithm: "sha256",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "f", count: 64)
        )
        for mutant in [wrongPK, wrongVersion, wrongDigest] {
            let before = try durableSnapshot(database)
            XCTAssertThrowsError(try PullProcessor(database: database).applyScopeChanges(
                changes: [mutant],
                syncedTables: [table],
                scopeCursors: [scopeID: "identity-mutant"],
                checksums: nil,
                schemaHash: protocolTestSchemaHash
            ))
            try assertNoDurableProgress(before, database)
        }

        let beforeDuplicate = try durableSnapshot(database)
        XCTAssertThrowsError(try Integrity.validateCanonicalWireJSON(
            Data(#"{"field-id":"typed-row","field-id":"duplicate"}"#.utf8)
        ))
        try assertNoDurableProgress(beforeDuplicate, database)

        let incompleteMaps = [
            PullResponse(
                changes: [],
                scopeSetVersion: 1,
                scopeCursors: [:],
                scopeUpdates: ScopeAssignmentDelta(add: [], remove: []),
                rebuild: [],
                hasMore: false,
                checksums: [:]
            ),
            PullResponse(
                changes: [],
                scopeSetVersion: 1,
                scopeCursors: [:],
                scopeUpdates: ScopeAssignmentDelta(add: [], remove: []),
                rebuild: [],
                hasMore: false,
                checksums: [
                    scopeID: protocolEmptyScopeChecksum(scopeID: scopeID),
                    "typed:extra": protocolEmptyScopeChecksum(scopeID: "typed:extra"),
                ]
            ),
        ]
        for response in incompleteMaps {
            let before = try durableSnapshot(database)
            XCTAssertThrowsError(try response.validate(activeScopes: [scopeID], requestScopeSetVersion: 1))
            try assertNoDurableProgress(before, database)
        }

        let validIdentity = try Integrity.rowIdentity(table: table, pk: validChange.pk)
        XCTAssertThrowsError(try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: scopeID,
            entries: [
                (identity: validIdentity, digest: checksum),
                (identity: validIdentity, digest: checksum),
            ]
        ))

        try PullProcessor(database: database).applyScopeChanges(
            changes: [validChange],
            syncedTables: [table],
            scopeCursors: [scopeID: "must-not-install"],
            checksums: [scopeID: protocolEmptyScopeChecksum(scopeID: scopeID)],
            schemaHash: protocolTestSchemaHash
        )
        let invalidated = try XCTUnwrap(database.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID) })
        XCTAssertNil(invalidated.cursor)
        XCTAssertNil(invalidated.checksum)
        XCTAssertNotEqual(invalidated.cursor, "must-not-install")
        try database.close()

        try assertRowDigestFailureOnRebuild()
        try assertRowDigestFailureOnPush()
        try await assertDuplicateWireMemberRejectedBeforeApply()
    }

    func testQueueTerminalStatesAndRetryFaultsPreserveSealedRequestAndLedger() async throws {
        let environment = try makeEnvironment("queue-terminal")
        var database = environment.database
        var tracker = ChangeTracker(database: database)
        var processor = PushProcessor(database: database, changeTracker: tracker)
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["queue-row", "first", "u1", "2026-04-04T00:00:00.000000Z"]
        )
        _ = try database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["normalized", "queue-row"]
        )

        let requestBodies = OSAllocatedUnfairLock(initialState: [Data]())
        let responseIndex = OSAllocatedUnfairLock(initialState: 0)
        let acceptedOutcome = OSAllocatedUnfairLock(initialState: Optional<AcceptedMutation>.none)
        let http = makeHTTPClient(path: environment.path)
        MockURLProtocol.requestHandler = { request in
            let body = try XCTUnwrap(request.bodyData())
            requestBodies.withLock { $0.append(body) }
            let index = responseIndex.withLock { value in
                defer { value += 1 }
                return value
            }
            if index == 0 {
                return try self.retryResponse(request: request, status: 429, code: "retry_later")
            }
            if index == 1 {
                return try self.retryResponse(request: request, status: 503, code: "temporary_unavailable")
            }
            if index == 2 {
                throw URLError(.networkConnectionLost)
            }
            let push = try JSONDecoder.synchroDecoder().decode(PushRequest.self, from: body)
            let row = self.orderRow(
                recordID: "queue-row",
                address: "normalized",
                updatedAt: "2026-04-04T01:00:00.000000Z"
            )
            let accepted = try self.makeAcceptedMutation(
                mutationID: try XCTUnwrap(push.mutations.only?.mutationID),
                schema: environment.table,
                pk: self.orderPK("queue-row"),
                serverRow: row,
                serverVersion: "queue-server-version"
            )
            acceptedOutcome.withLock { $0 = accepted }
            let response = PushResponse(
                batchID: push.batchID,
                serverTime: "2026-04-04T01:00:00.000000Z",
                accepted: [accepted],
                rejected: []
            )
            return try self.successResponse(request: request, value: response)
        }

        let firstFailure = await pushFailure(processor, http: http, table: environment.table)
        XCTAssertTrue(firstFailure is RetryableError)
        let sealedBaseline = try durableSnapshot(database)
        XCTAssertEqual(
            try database.queryOne("SELECT state FROM _synchro_push_batches", params: nil)?["state"] as String?,
            "pending"
        )
        let secondFailure = await pushFailure(processor, http: http, table: environment.table)
        XCTAssertTrue(secondFailure is RetryableError)
        try assertNoDurableProgress(sealedBaseline, database)
        let responseLoss = await pushFailure(processor, http: http, table: environment.table)
        XCTAssertTrue(responseLoss is RetryableError)
        try assertNoDurableProgress(sealedBaseline, database)
        let failedBodies = requestBodies.withLock { $0 }
        XCTAssertEqual(failedBodies.count, 3)
        XCTAssertTrue(failedBodies.dropFirst().allSatisfy { $0 == failedBodies[0] })

        try database.close()
        database = try SynchroDatabase(path: environment.path)
        tracker = ChangeTracker(database: database)
        processor = PushProcessor(database: database, changeTracker: tracker)
        try assertNoDurableProgress(sealedBaseline, database)

        _ = try database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["successor", "queue-row"]
        )
        _ = try await processor.processPush(
            httpClient: http,
            clientID: "issue-49-client",
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            syncedTables: [environment.table]
        )
        let allBodies = requestBodies.withLock { $0 }
        XCTAssertEqual(allBodies.first, allBodies.last)
        XCTAssertEqual(
            try database.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                params: nil
            ).map { $0["lifecycle_state"] as String? },
            ["superseded_before_send", "superseded_before_send", "accepted", "unsealed"]
        )
        XCTAssertEqual(
            try database.queryOne("SELECT state FROM _synchro_push_batches", params: nil)?["state"] as String?,
            "completed"
        )
        XCTAssertEqual(
            try database.queryOne("SELECT ship_address FROM orders WHERE id = 'queue-row'", params: nil)?["ship_address"] as String?,
            "successor"
        )

        let accepted = try XCTUnwrap(acceptedOutcome.withLock { $0 })
        var changedOutcome = accepted
        changedOutcome.serverRow = orderRow(
            recordID: "queue-row",
            address: "different-terminal",
            updatedAt: "2026-04-04T02:00:00.000000Z"
        )
        changedOutcome.rowChecksum = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: environment.table,
            pk: orderPK("queue-row"),
            row: try XCTUnwrap(changedOutcome.serverRow),
            serverVersion: accepted.serverVersion
        ).checksum
        let beforeChangedTerminal = try durableSnapshot(database)
        XCTAssertThrowsError(try processor.applyAccepted(
            accepted: [changedOutcome],
            syncedTables: [environment.table]
        ))
        try assertNoDurableProgress(beforeChangedTerminal, database)
        try database.close()

        try await assertCancelledBeforeSend(http: http)
        try assertConflictAndBlockedSuccessor()
        try assertTerminalRejection()
        try await assertTamperedSealedBatchDoesNotTransmit()
    }

    func testInterruptedMigrationRejectsEveryInconsistentJournalBeforeProgress() throws {
        let successful = try makePreparedMigration("migration-success")
        let successfulPending = try ChangeTracker(database: successful.database).inspectPendingMutations()
        try successful.database.close()
        let recovered = try SynchroDatabase(path: successful.path)
        let recoveredJournal = try XCTUnwrap(SchemaManager(database: recovered).recoverMigrationIfNeeded())
        XCTAssertEqual(recoveredJournal.phase, .applied)
        XCTAssertEqual(
            try recovered.readTransaction { try SynchroMeta.getInt64($0, key: .schemaVersion) },
            2
        )
        XCTAssertTrue(try hasColumn(recovered, table: "orders", column: "notes"))
        XCTAssertEqual(
            try recovered.queryOne("SELECT value FROM local_settings WHERE key = 'theme'", params: nil)?["value"] as String?,
            "dark"
        )
        XCTAssertEqual(try ChangeTracker(database: recovered).inspectPendingMutations(), successfulPending)
        XCTAssertNil(try SchemaManager(database: recovered).activeMigration())
        try recovered.close()

        let mutations: [(String, (GRDB.Database) throws -> Void)] = [
            ("physical", { connection in
                try connection.execute(sql: "ALTER TABLE orders RENAME COLUMN ship_address TO corrupted_address")
            }),
            ("reference", { connection in
                try connection.execute(
                    sql: "UPDATE _synchro_schema_migration SET target_schema_hash = ? WHERE singleton = 1",
                    arguments: [String(repeating: "f", count: 64)]
                )
            }),
            ("body", { connection in
                try connection.execute(
                    sql: "UPDATE _synchro_schema_migration SET target_manifest_json = '{}' WHERE singleton = 1"
                )
            }),
            ("plan", { connection in
                try connection.execute(
                    sql: "UPDATE _synchro_schema_migration SET migration_plan_hash = ? WHERE singleton = 1",
                    arguments: [String(repeating: "f", count: 64)]
                )
            }),
            ("phase", { connection in
                try connection.execute(
                    sql: "UPDATE _synchro_schema_migration SET phase = 'applied' WHERE singleton = 1"
                )
            }),
        ]

        for (name, mutate) in mutations {
            let prepared = try makePreparedMigration("migration-\(name)")
            let pendingBefore = try ChangeTracker(database: prepared.database).inspectPendingMutations()
            try prepared.database.writeTransaction(mutate)
            let beforeRecovery = try durableSnapshot(prepared.database)
            try prepared.database.close()

            let reopened = try SynchroDatabase(path: prepared.path)
            XCTAssertThrowsError(try SchemaManager(database: reopened).recoverMigrationIfNeeded(), name)
            try assertNoDurableProgress(beforeRecovery, reopened)
            XCTAssertEqual(
                try reopened.readTransaction { try SynchroMeta.getInt64($0, key: .schemaVersion) },
                1
            )
            XCTAssertEqual(
                try reopened.queryOne("SELECT value FROM local_settings WHERE key = 'theme'", params: nil)?["value"] as String?,
                "dark"
            )
            XCTAssertEqual(try ChangeTracker(database: reopened).inspectPendingMutations(), pendingBefore)
            XCTAssertNotNil(try reopened.queryOne("SELECT * FROM _synchro_schema_migration", params: nil))
            XCTAssertFalse(try hasColumn(reopened, table: "orders", column: "notes"))
            try reopened.close()
        }
    }

    func testPortableSeedContinuesNormallyAndTamperingNeverPublishesDatabase() async throws {
        try assertSeedRejection("seed-missing-continuation") { connection in
            try connection.execute(sql: "DELETE FROM _synchro_seed_receipts")
        }
        try assertSeedRejection("seed-cross-snapshot") { connection in
            try connection.execute(
                sql: "UPDATE _synchro_scope_rows SET checksum = ?",
                arguments: [String(repeating: "f", count: 64)]
            )
        }
        try assertSeedRejection("seed-misbound-receipt") { connection in
            try connection.execute(sql: "UPDATE _synchro_seed_receipts SET receipt = ''")
        }
        try assertSeedRejection("seed-skipped-verification") { connection in
            try SynchroMeta.set(connection, key: .snapshotComplete, value: "0")
        }
        try assertSeedWithQueuedIntentIsRejected()

        let seed = try createPortableSeed("seed-valid")
        let destination = temporaryDatabasePath("seed-continuation")
        try SeedDatabaseInstaller.installIfNeeded(seedPath: seed.path, databasePath: destination)
        let database = try SynchroDatabase(path: destination)
        let continued = try canonicalChange(
            scopeID: seed.scopeID,
            recordID: "continued-row",
            address: "continued",
            serverVersion: "continued-version",
            table: seed.table
        )
        let continuedDigest = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: seed.table,
            pk: continued.pk,
            row: try XCTUnwrap(continued.row),
            serverVersion: continued.serverVersion
        )
        let finalChecksum = try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: seed.scopeID,
            entries: [
                (identity: seed.rowDigest.identity, digest: seed.rowDigest.checksum),
                (identity: continuedDigest.identity, digest: continuedDigest.checksum),
            ]
        )
        let requests = OSAllocatedUnfairLock(initialState: [(path: String, body: Data)]())
        MockURLProtocol.requestHandler = { request in
            let body = try XCTUnwrap(request.bodyData())
            requests.withLock { $0.append((request.url!.path, body)) }
            if request.url!.path.hasSuffix("/sync/connect") {
                return try self.successResponse(
                    request: request,
                    value: ConnectResponse(
                        serverTime: "2026-04-06T00:00:00.000000Z",
                        protocolVersion: 3,
                        clientGeneration: 1,
                        scopeSetVersion: 0,
                        schema: SchemaDescriptor(version: 1, hash: protocolTestSchemaHash, action: .none, reason: nil),
                        scopes: ScopeAssignmentDelta(add: [], remove: []),
                        scopeCursorUpdates: [seed.scopeID: "seed-base-cursor"],
                        schemaDefinition: nil,
                        affectedScopes: nil
                    )
                )
            }
            if request.url!.path.hasSuffix("/sync/pull") {
                return try self.successResponse(
                    request: request,
                    value: PullResponse(
                        changes: [continued],
                        scopeSetVersion: 0,
                        scopeCursors: [seed.scopeID: "seed-next-cursor"],
                        scopeUpdates: ScopeAssignmentDelta(add: [], remove: []),
                        rebuild: [],
                        hasMore: false,
                        checksums: [seed.scopeID: finalChecksum]
                    )
                )
            }
            return try self.errorResponse(request: request, status: 500)
        }
        let engine = makeEngine(database: database, path: destination)
        try await engine.start()

        let observed = requests.withLock { $0 }
        let connectBody = try XCTUnwrap(observed.first(where: { $0.path.hasSuffix("/sync/connect") })?.body)
        let connect = try JSONDecoder.synchroDecoder().decode(ConnectRequest.self, from: connectBody)
        XCTAssertEqual(connect.seedReceipts, [seed.scopeID: seed.receipt])
        XCTAssertNil(connect.knownScopes[seed.scopeID]?.cursor)
        let pullBody = try XCTUnwrap(observed.first(where: { $0.path.hasSuffix("/sync/pull") })?.body)
        let pull = try JSONDecoder.synchroDecoder().decode(PullRequest.self, from: pullBody)
        XCTAssertEqual(pull.scopes[seed.scopeID]?.cursor, "seed-base-cursor")
        XCTAssertEqual(
            try database.queryOne("SELECT ship_address FROM orders WHERE id = 'seed-row'", params: nil)?["ship_address"] as String?,
            "Seeded Address"
        )
        XCTAssertEqual(
            try database.queryOne("SELECT ship_address FROM orders WHERE id = 'continued-row'", params: nil)?["ship_address"] as String?,
            "continued"
        )
        XCTAssertTrue(try database.query("SELECT * FROM _synchro_seed_receipts", params: nil).isEmpty)
        XCTAssertEqual(
            try database.readTransaction { try SynchroMeta.getScope($0, scopeID: seed.scopeID)?.cursor },
            "seed-next-cursor"
        )
        await engine.shutdown()
        try database.close()
    }

    func testMembershipInvalidationAutomaticallyRebuildsOnlyAffectedScope() async throws {
        let targetScope = "orders:auto-rebuild"
        let otherScope = "orders:stable"
        let failing = try makeEnvironment("automatic-rebuild-failure")
        try failing.database.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: targetScope, cursor: "stale-target", checksum: nil, generation: 4)
            try SynchroMeta.upsertScope(connection, scopeID: otherScope, cursor: "stable-cursor", checksum: nil, generation: 9)
        }
        MockURLProtocol.requestHandler = { request in
            if request.url!.path.hasSuffix("/sync/connect") {
                return try self.successResponse(
                    request: request,
                    value: self.connectNone(
                        scopeSetVersion: 0,
                        cursorUpdates: Dictionary(uniqueKeysWithValues: [(targetScope, Optional<String>.none)])
                    )
                )
            }
            if request.url!.path.hasSuffix("/sync/rebuild") {
                return try self.successResponse(
                    request: request,
                    value: RebuildResponse(
                        scope: "orders:wrong-scope",
                        records: [],
                        cursor: nil,
                        hasMore: false,
                        finalScopeCursor: "must-not-install",
                        checksum: protocolEmptyScopeChecksum(scopeID: "orders:wrong-scope")
                    )
                )
            }
            return try self.errorResponse(request: request, status: 500)
        }
        let failingEngine = makeEngine(database: failing.database, path: failing.path)
        do {
            try await failingEngine.start()
            XCTFail("Invalid rebuild finality was accepted")
        } catch {
        }
        XCTAssertNil(try failing.database.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.cursor })
        XCTAssertNil(try failing.database.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.checksum })
        XCTAssertEqual(
            try failing.database.readTransaction { try SynchroMeta.getScope($0, scopeID: otherScope)?.cursor },
            "stable-cursor"
        )
        XCTAssertTrue(try failing.database.query("SELECT * FROM _synchro_rebuild_page_receipts", params: nil).isEmpty)
        XCTAssertTrue(try failing.database.query("SELECT * FROM orders", params: nil).isEmpty)
        await failingEngine.shutdown()
        try failing.database.close()

        let success = try makeEnvironment("automatic-rebuild-success")
        try success.database.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: targetScope, cursor: "stale-target", checksum: nil, generation: 4)
            try SynchroMeta.upsertScope(connection, scopeID: otherScope, cursor: "stable-cursor", checksum: nil, generation: 9)
        }
        let paths = OSAllocatedUnfairLock(initialState: [String]())
        MockURLProtocol.requestHandler = { request in
            paths.withLock { $0.append(request.url!.path) }
            if request.url!.path.hasSuffix("/sync/connect") {
                return try self.successResponse(
                    request: request,
                    value: self.connectNone(
                        scopeSetVersion: 0,
                        cursorUpdates: Dictionary(uniqueKeysWithValues: [(targetScope, Optional<String>.none)])
                    )
                )
            }
            if request.url!.path.hasSuffix("/sync/rebuild") {
                let rebuild = try JSONDecoder.synchroDecoder().decode(
                    RebuildRequest.self,
                    from: try XCTUnwrap(request.bodyData())
                )
                XCTAssertEqual(rebuild.scope, targetScope)
                return try self.successResponse(
                    request: request,
                    value: RebuildResponse(
                        scope: targetScope,
                        records: [],
                        cursor: nil,
                        hasMore: false,
                        finalScopeCursor: "rebuilt-cursor",
                        checksum: protocolEmptyScopeChecksum(scopeID: targetScope)
                    )
                )
            }
            if request.url!.path.hasSuffix("/sync/pull") {
                return try self.successResponse(
                    request: request,
                    value: PullResponse(
                        changes: [],
                        scopeSetVersion: 0,
                        scopeCursors: [targetScope: "post-rebuild-cursor"],
                        scopeUpdates: ScopeAssignmentDelta(add: [], remove: []),
                        rebuild: [],
                        hasMore: false,
                        checksums: [
                            targetScope: protocolEmptyScopeChecksum(scopeID: targetScope),
                            otherScope: protocolEmptyScopeChecksum(scopeID: otherScope),
                        ]
                    )
                )
            }
            return try self.errorResponse(request: request, status: 500)
        }
        let successEngine = makeEngine(database: success.database, path: success.path)
        try await successEngine.start()
        let observedPaths = paths.withLock { $0 }
        let rebuildIndex = try XCTUnwrap(observedPaths.firstIndex(where: { $0.hasSuffix("/sync/rebuild") }))
        let pullIndex = try XCTUnwrap(observedPaths.firstIndex(where: { $0.hasSuffix("/sync/pull") }))
        XCTAssertLessThan(rebuildIndex, pullIndex)
        XCTAssertEqual(
            try success.database.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.cursor },
            "post-rebuild-cursor"
        )
        XCTAssertEqual(
            try success.database.readTransaction { try SynchroMeta.getScope($0, scopeID: targetScope)?.generation },
            6
        )
        XCTAssertEqual(
            try success.database.readTransaction { try SynchroMeta.getScope($0, scopeID: otherScope)?.cursor },
            "stable-cursor"
        )
        XCTAssertEqual(
            try success.database.readTransaction { try SynchroMeta.getScope($0, scopeID: otherScope)?.generation },
            9
        )
        XCTAssertNil(try success.database.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: targetScope) })
        await successEngine.shutdown()
        try success.database.close()
    }

    func testHistoricalOutcomeReplayUsesSealedSchemaAndRejectsChangedReplay() async throws {
        let oldTable = projectionTable(titleName: "legacy_title", enabledName: "legacy_enabled")
        let currentTable = projectionTable(titleName: "title", enabledName: "enabled")
        let oldHash = protocolTestSchemaHash
        let currentHash = String(repeating: "1", count: 64)
        let environment = try makeEnvironment("historical-outcome", table: oldTable, schemaHash: oldHash)
        var database = environment.database
        _ = try database.execute(
            "INSERT INTO projection_orders (id, legacy_title, legacy_enabled, updated_at) VALUES (?, ?, ?, ?)",
            params: ["history-row", "authored", 0, "2026-04-08T00:00:00.000000Z"]
        )
        var processor = PushProcessor(database: database, changeTracker: ChangeTracker(database: database))
        let responseIndex = OSAllocatedUnfairLock(initialState: 0)
        let validOutcome = OSAllocatedUnfairLock(initialState: Optional<AcceptedMutation>.none)
        let http = makeHTTPClient(path: environment.path)
        MockURLProtocol.requestHandler = { request in
            let body = try XCTUnwrap(request.bodyData())
            let push = try JSONDecoder.synchroDecoder().decode(PushRequest.self, from: body)
            let index = responseIndex.withLock { value in
                defer { value += 1 }
                return value
            }
            if index == 0 {
                return try self.retryResponse(request: request, status: 503, code: "temporary_unavailable")
            }
            let row = self.projectionRow(table: oldTable, recordID: "history-row", title: "server", enabled: true)
            let accepted = try self.makeAcceptedMutation(
                mutationID: try XCTUnwrap(push.mutations.only?.mutationID),
                schema: oldTable,
                schemaRef: SchemaRef(version: 1, hash: oldHash),
                pk: ["field-id": AnyCodable("history-row")],
                serverRow: row,
                serverVersion: "historical-version"
            )
            validOutcome.withLock { $0 = accepted }
            var outcome = accepted
            if index == 1 {
                outcome.rowChecksum = ChecksumObject(
                    algorithm: "sha256",
                    version: 1,
                    encoding: "hex",
                    digest: String(repeating: "f", count: 64)
                )
            }
            return try self.successResponse(
                request: request,
                value: PushResponse(
                    batchID: push.batchID,
                    serverTime: "2026-04-08T01:00:00.000000Z",
                    accepted: [outcome],
                    rejected: []
                )
            )
        }

        let initialFailure = await pushFailure(processor, http: http, table: oldTable, schemaHash: oldHash)
        XCTAssertTrue(initialFailure is RetryableError)
        try SchemaManager(database: database).reconcileLocalSchema(
            schemaVersion: 2,
            schemaHash: currentHash,
            tables: [currentTable]
        )
        _ = try database.execute(
            "UPDATE projection_orders SET title = ? WHERE id = ?",
            params: ["later-local", "history-row"]
        )

        let beforeWrongHistorical = try durableSnapshot(database)
        let historicalFailure = await pushFailure(processor, http: http, table: currentTable, schemaHash: currentHash)
        XCTAssertNotNil(historicalFailure)
        try assertNoDurableProgress(beforeWrongHistorical, database)

        _ = try await processor.processPush(
            httpClient: http,
            clientID: "issue-49-client",
            clientGeneration: 1,
            schemaVersion: 2,
            schemaHash: currentHash,
            syncedTables: [currentTable]
        )
        XCTAssertEqual(
            try database.queryOne("SELECT title FROM projection_orders WHERE id = 'history-row'", params: nil)?["title"] as String?,
            "later-local"
        )
        XCTAssertEqual(
            try database.queryOne("SELECT enabled FROM projection_orders WHERE id = 'history-row'", params: nil)?["enabled"] as Int64?,
            1
        )
        XCTAssertEqual(
            try database.queryOne("SELECT state FROM _synchro_push_batches", params: nil)?["state"] as String?,
            "completed"
        )
        try database.close()

        database = try SynchroDatabase(path: environment.path)
        processor = PushProcessor(database: database, changeTracker: ChangeTracker(database: database))
        let acceptedJSON = try XCTUnwrap(
            try database.queryOne(
                "SELECT accepted_json FROM _synchro_pending_changes WHERE lifecycle_state = 'accepted'",
                params: nil
            )?["accepted_json"] as String?
        )
        let persisted = try JSONDecoder.synchroDecoder().decode(AcceptedMutation.self, from: Data(acceptedJSON.utf8))
        XCTAssertEqual(persisted.outcomeSchema, SchemaRef(version: 1, hash: oldHash))
        XCTAssertEqual(persisted, try XCTUnwrap(validOutcome.withLock { $0 }))

        var changedReplay = persisted
        let changedRow = projectionRow(table: oldTable, recordID: "history-row", title: "changed-replay", enabled: true)
        changedReplay.serverRow = changedRow
        changedReplay.rowChecksum = try Integrity.rowDigest(
            schemaHash: oldHash,
            table: oldTable,
            pk: ["field-id": AnyCodable("history-row")],
            row: changedRow,
            serverVersion: persisted.serverVersion
        ).checksum
        let beforeChangedReplay = try durableSnapshot(database)
        XCTAssertThrowsError(try processor.applyAccepted(accepted: [changedReplay], syncedTables: [currentTable]))
        try assertNoDurableProgress(beforeChangedReplay, database)
        try database.close()
    }

    private var ordersTable: LocalSchemaTable {
        get throws {
            try XCTUnwrap(try protocolOrdersSchemaManifest().localTables().only)
        }
    }

    private func makeEnvironment(
        _ label: String,
        table: LocalSchemaTable? = nil,
        schemaHash: String = protocolTestSchemaHash
    ) throws -> Environment {
        let path = temporaryDatabasePath(label)
        let database = try SynchroDatabase(path: path)
        let installedTable = try table ?? ordersTable
        try SchemaManager(database: database).reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: schemaHash,
            tables: [installedTable]
        )
        return Environment(path: path, database: database, table: installedTable)
    }

    private func orderPK(_ recordID: String) -> [String: AnyCodable] {
        ["field-id": AnyCodable(recordID)]
    }

    private func orderRow(
        recordID: String,
        address: String,
        updatedAt: String
    ) -> [String: AnyCodable] {
        [
            "field-id": AnyCodable(recordID),
            "field-ship-address": AnyCodable(address),
            "field-user-id": AnyCodable("u1"),
            "field-updated-at": AnyCodable(updatedAt),
            "field-deleted-at": AnyCodable(NSNull()),
        ]
    }

    private func canonicalChange(
        scopeID: String,
        recordID: String,
        address: String,
        serverVersion: String,
        table: LocalSchemaTable
    ) throws -> ChangeRecord {
        let pk = orderPK(recordID)
        let row = orderRow(
            recordID: recordID,
            address: address,
            updatedAt: "2026-04-01T00:00:00.000000Z"
        )
        return ChangeRecord(
            scope: scopeID,
            table: table.tableID,
            op: .upsert,
            pk: pk,
            row: row,
            rowChecksum: try Integrity.rowDigest(
                schemaHash: protocolTestSchemaHash,
                table: table,
                pk: pk,
                row: row,
                serverVersion: serverVersion
            ).checksum,
            serverVersion: serverVersion
        )
    }

    private func scopeChecksum(
        scopeID: String,
        changes: [ChangeRecord],
        table: LocalSchemaTable
    ) throws -> ChecksumObject {
        try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: scopeID,
            entries: try changes.map { change in
                let digest = try Integrity.rowDigest(
                    schemaHash: protocolTestSchemaHash,
                    table: table,
                    pk: change.pk,
                    row: try XCTUnwrap(change.row),
                    serverVersion: change.serverVersion
                )
                return (identity: digest.identity, digest: digest.checksum)
            }
        )
    }

    private func makeAcceptedMutation(
        mutationID: String,
        schema: LocalSchemaTable,
        schemaRef: SchemaRef = SchemaRef(version: 1, hash: protocolTestSchemaHash),
        pk: [String: AnyCodable],
        serverRow: [String: AnyCodable],
        serverVersion: String
    ) throws -> AcceptedMutation {
        AcceptedMutation(
            mutationID: mutationID,
            table: schema.tableID,
            pk: pk,
            outcomeSchema: schemaRef,
            status: .applied,
            serverRow: serverRow,
            rowChecksum: try Integrity.rowDigest(
                schemaHash: schemaRef.hash,
                table: schema,
                pk: pk,
                row: serverRow,
                serverVersion: serverVersion
            ).checksum,
            serverVersion: serverVersion
        )
    }

    private func makeRejectedMutation(
        mutationID: String,
        schema: LocalSchemaTable,
        pk: [String: AnyCodable],
        status: MutationStatus,
        code: MutationRejectionCode,
        message: String,
        serverRow: [String: AnyCodable]? = nil,
        serverVersion: String? = nil
    ) throws -> RejectedMutation {
        let checksum: ChecksumObject?
        if let serverRow, let serverVersion {
            checksum = try Integrity.rowDigest(
                schemaHash: protocolTestSchemaHash,
                table: schema,
                pk: pk,
                row: serverRow,
                serverVersion: serverVersion
            ).checksum
        } else {
            checksum = nil
        }
        return RejectedMutation(
            mutationID: mutationID,
            table: schema.tableID,
            pk: pk,
            outcomeSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            status: status,
            code: code,
            message: message,
            retryable: false,
            serverRow: serverRow,
            rowChecksum: checksum,
            serverVersion: serverVersion,
            authoredSchema: nil,
            currentSchema: nil,
            incompatibleFieldIDs: nil
        )
    }

    private func insertOrderWithoutCapture(
        _ database: SynchroDatabase,
        recordID: String,
        address: String
    ) throws {
        try database.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: [recordID, address, "u1", "2026-04-01T00:00:00.000000Z"]
            )
        }
    }

    private func rebuildRequest(_ attempt: LocalRebuildAttempt) -> RebuildRequest {
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

    private func wireData<Value: Encodable>(_ value: Value) throws -> Data {
        try JSONEncoder.synchroEncoder().encode(value)
    }

    private func typedTable() -> LocalSchemaTable {
        LocalSchemaTable(
            tableID: "table-typed-orders",
            relationID: "relation-typed-orders",
            tableName: "typed_orders",
            primaryKeyFieldID: "field-id",
            createdAtFieldID: nil,
            updatedAtFieldID: "field-updated-at",
            deletedAtFieldID: "field-deleted-at",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: [
                localColumn("field-id", "id", "string", nullable: false, writable: false, primaryKey: true),
                localColumn("field-title", "title", "string", nullable: false, writable: true),
                localColumn("field-count", "count", "int", nullable: true, writable: true),
                localColumn("field-large-count", "large_count", "int64", nullable: true, writable: true),
                localColumn("field-enabled", "enabled", "boolean", nullable: true, writable: true),
                localColumn(
                    "field-amount",
                    "amount",
                    "decimal",
                    nullable: true,
                    writable: true,
                    precision: 5,
                    scale: 2
                ),
                localColumn("field-document", "document", "json", nullable: true, writable: true),
                localColumn("field-score", "score", "float", nullable: true, writable: true),
                localColumn("field-payload", "payload", "bytes", nullable: true, writable: true),
                localColumn("field-updated-at", "updated_at", "datetime", nullable: false, writable: false),
                localColumn("field-deleted-at", "deleted_at", "datetime", nullable: true, writable: false),
            ]
        )
    }

    private func localColumn(
        _ fieldID: String,
        _ name: String,
        _ logicalType: String,
        nullable: Bool,
        writable: Bool,
        precision: Int? = nil,
        scale: Int? = nil,
        primaryKey: Bool = false
    ) -> LocalSchemaColumn {
        LocalSchemaColumn(
            fieldID: fieldID,
            name: name,
            logicalType: logicalType,
            nullable: nullable,
            writable: writable,
            precision: precision,
            scale: scale,
            sqliteDefaultSQL: nil,
            isPrimaryKey: primaryKey
        )
    }

    private func typedRow(recordID: String) -> [String: AnyCodable] {
        [
            "field-id": AnyCodable(recordID),
            "field-title": AnyCodable("canonical"),
            "field-count": AnyCodable(Int64(7)),
            "field-large-count": AnyCodable("8"),
            "field-enabled": AnyCodable(true),
            "field-amount": AnyCodable("12.3"),
            "field-document": AnyCodable(#"{"a":1}"#),
            "field-score": AnyCodable(1.25),
            "field-payload": AnyCodable("AAEC"),
            "field-updated-at": AnyCodable("2026-04-03T00:00:00.000000Z"),
            "field-deleted-at": AnyCodable(NSNull()),
        ]
    }

    private func replacing(
        _ row: [String: AnyCodable],
        key: String,
        value: AnyCodable
    ) -> [String: AnyCodable] {
        var replaced = row
        replaced[key] = value
        return replaced
    }

    private func removing(_ row: [String: AnyCodable], key: String) -> [String: AnyCodable] {
        var changed = row
        changed.removeValue(forKey: key)
        return changed
    }

    private func assertRowDigestFailureOnRebuild() throws {
        let environment = try makeEnvironment("rebuild-row-digest")
        let scopeID = "orders:rebuild-row-digest"
        try environment.database.writeTransaction {
            try SynchroMeta.upsertScope($0, scopeID: scopeID, cursor: "before-rebuild", checksum: nil)
        }
        let processor = PullProcessor(database: environment.database)
        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [environment.table]
        )
        let request = rebuildRequest(attempt)
        let row = orderRow(recordID: "rebuild-row", address: "server", updatedAt: "2026-04-03T00:00:00.000000Z")
        let record = RebuildRecord(
            table: environment.table.tableID,
            pk: orderPK("rebuild-row"),
            row: row,
            rowChecksum: ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: String(repeating: "f", count: 64)
            ),
            serverVersion: "rebuild-version"
        )
        let response = RebuildResponse(
            scope: scopeID,
            records: [record],
            cursor: "next-page",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        let before = try durableSnapshot(environment.database)
        XCTAssertThrowsError(try processor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try wireData(request),
            response: response,
            responseBody: try wireData(response),
            syncedTables: [environment.table]
        ))
        try assertNoDurableProgress(before, environment.database)
        try environment.database.close()
    }

    private func assertRowDigestFailureOnPush() throws {
        let environment = try makeEnvironment("push-row-digest")
        let tracker = ChangeTracker(database: environment.database)
        _ = try environment.database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["push-row", "local", "u1", "2026-04-03T00:00:00.000000Z"]
        )
        let pending = try XCTUnwrap(try tracker.pendingChanges().only)
        var accepted = try makeAcceptedMutation(
            mutationID: pending.mutationID,
            schema: environment.table,
            pk: orderPK("push-row"),
            serverRow: orderRow(recordID: "push-row", address: "server", updatedAt: "2026-04-03T01:00:00.000000Z"),
            serverVersion: "push-version"
        )
        accepted.rowChecksum = ChecksumObject(
            algorithm: "sha256",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "f", count: 64)
        )
        let before = try durableSnapshot(environment.database)
        XCTAssertThrowsError(try PushProcessor(database: environment.database, changeTracker: tracker).applyAccepted(
            accepted: [accepted],
            syncedTables: [environment.table],
            sentPending: [pending.mutationID: pending]
        ))
        try assertNoDurableProgress(before, environment.database)
        try environment.database.close()
    }

    private func assertDuplicateWireMemberRejectedBeforeApply() async throws {
        let environment = try makeEnvironment("duplicate-wire-member")
        let baseline = try durableSnapshot(environment.database)
        let http = makeHTTPClient(path: environment.path)
        MockURLProtocol.requestHandler = { request in
            let body = Data("""
                {"changes":[],"scope_set_version":0,"scope_cursors":{},"scope_updates":{"add":[],"remove":[]},"rebuild":[],"has_more":false,"checksums":{},"checksums":{}}
                """.utf8)
            return (
                HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: nil,
                    headerFields: ["Content-Type": "application/json"]
                )!,
                body
            )
        }
        do {
            _ = try await http.pull(request: PullRequest(
                clientID: "issue-49-client",
                clientGeneration: 1,
                schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
                scopeSetVersion: 0,
                scopes: [:],
                limit: 100
            ))
            XCTFail("Duplicate wire member was accepted")
        } catch {
        }
        try assertNoDurableProgress(baseline, environment.database)
        try environment.database.close()
    }

    private func pushFailure(
        _ processor: PushProcessor,
        http: HttpClient,
        table: LocalSchemaTable,
        schemaHash: String = protocolTestSchemaHash
    ) async -> Error? {
        do {
            _ = try await processor.processPush(
                httpClient: http,
                clientID: "issue-49-client",
                clientGeneration: 1,
                schemaVersion: schemaHash == protocolTestSchemaHash ? 1 : 2,
                schemaHash: schemaHash,
                syncedTables: [table]
            )
            return nil
        } catch {
            return error
        }
    }

    private func assertCancelledBeforeSend(http: HttpClient) async throws {
        let environment = try makeEnvironment("queue-cancelled")
        _ = try environment.database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["cancelled", "temporary", "u1", "2026-04-04T00:00:00.000000Z"]
        )
        _ = try environment.database.execute("DELETE FROM orders WHERE id = 'cancelled'", params: nil)
        let tracker = ChangeTracker(database: environment.database)
        let processor = PushProcessor(database: environment.database, changeTracker: tracker)
        let outcome = try await processor.processPush(
            httpClient: http,
            clientID: "issue-49-client",
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            syncedTables: [environment.table]
        )
        XCTAssertNil(outcome)
        XCTAssertEqual(
            try environment.database.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                params: nil
            ).map { $0["lifecycle_state"] as String? },
            ["cancelled_before_send", "cancelled_before_send"]
        )
        let snapshot = try durableSnapshot(environment.database)
        try environment.database.close()
        let reopened = try SynchroDatabase(path: environment.path)
        try assertNoDurableProgress(snapshot, reopened)
        XCTAssertEqual(
            try ChangeTracker(database: reopened).inspectRetainedMutations().map(\.status),
            [.cancelledBeforeSend, .cancelledBeforeSend]
        )
        try reopened.close()
    }

    private func assertConflictAndBlockedSuccessor() throws {
        let environment = try makeEnvironment("queue-conflict")
        let database = environment.database
        try insertOrderWithoutCapture(database, recordID: "conflict-row", address: "server-base")
        try database.writeTransaction {
            try SynchroMeta.upsertRowVersion(
                $0,
                tableName: "orders",
                recordID: "conflict-row",
                serverVersion: "conflict-base",
                rowChecksum: nil
            )
        }
        _ = try database.execute(
            "UPDATE orders SET ship_address = 'first-local' WHERE id = 'conflict-row'",
            params: nil
        )
        let tracker = ChangeTracker(database: database)
        let predecessor = try XCTUnwrap(try tracker.pendingChanges().only)
        let batchID = UUID().uuidString.lowercased()
        try database.writeTransaction {
            try tracker.markPendingAsSealed($0, batchID: batchID, pending: [predecessor])
        }
        _ = try database.execute(
            "UPDATE orders SET ship_address = 'later-local' WHERE id = 'conflict-row'",
            params: nil
        )
        let serverRow = orderRow(
            recordID: "conflict-row",
            address: "server-conflict",
            updatedAt: "2026-04-05T00:00:00.000000Z"
        )
        let rejection = try makeRejectedMutation(
            mutationID: predecessor.mutationID,
            schema: environment.table,
            pk: orderPK("conflict-row"),
            status: .conflict,
            code: .versionConflict,
            message: "conflict",
            serverRow: serverRow,
            serverVersion: "conflict-server-version"
        )
        let processor = PushProcessor(database: database, changeTracker: tracker)
        var wrongIdentity = rejection
        wrongIdentity.mutationID = UUID().uuidString.lowercased()
        let beforeWrongID = try durableSnapshot(database)
        XCTAssertThrowsError(try processor.applyRejected(
            rejected: [wrongIdentity],
            syncedTables: [environment.table],
            sentPending: [predecessor.mutationID: predecessor]
        ))
        try assertNoDurableProgress(beforeWrongID, database)

        _ = try processor.applyRejected(
            rejected: [rejection],
            syncedTables: [environment.table],
            sentPending: [predecessor.mutationID: predecessor]
        )
        XCTAssertEqual(
            try database.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                params: nil
            ).map { $0["lifecycle_state"] as String? },
            ["rejected", "blocked_by_predecessor"]
        )
        let snapshot = try durableSnapshot(database)
        try database.close()

        let reopened = try SynchroDatabase(path: environment.path)
        try assertNoDurableProgress(snapshot, reopened)
        let retained = try ChangeTracker(database: reopened).inspectRetainedMutations()
        XCTAssertEqual(retained.map(\.status), [.serverRejected, .blockedByPredecessor])
        XCTAssertEqual(retained[1].dependsOnMutationID, predecessor.mutationID)
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.listRejectedMutations($0).only?.mutationID },
            predecessor.mutationID
        )
        try reopened.close()
    }

    private func assertTerminalRejection() throws {
        let environment = try makeEnvironment("queue-terminal-rejected")
        let database = environment.database
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["rejected-row", "local", "u1", "2026-04-05T00:00:00.000000Z"]
        )
        let tracker = ChangeTracker(database: database)
        let source = try XCTUnwrap(try tracker.pendingChanges().only)
        let rejection = try makeRejectedMutation(
            mutationID: source.mutationID,
            schema: environment.table,
            pk: orderPK("rejected-row"),
            status: .rejectedTerminal,
            code: .policyRejected,
            message: "policy"
        )
        let processor = PushProcessor(database: database, changeTracker: tracker)
        var wrongTable = rejection
        wrongTable.table = "wrong-table"
        let beforeWrongTable = try durableSnapshot(database)
        XCTAssertThrowsError(try processor.applyRejected(
            rejected: [wrongTable],
            syncedTables: [environment.table]
        ))
        try assertNoDurableProgress(beforeWrongTable, database)

        _ = try processor.applyRejected(rejected: [rejection], syncedTables: [environment.table])
        XCTAssertEqual(
            try database.queryOne(
                "SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?",
                params: [source.mutationID]
            )?["lifecycle_state"] as String?,
            "rejected"
        )
        let snapshot = try durableSnapshot(database)
        try database.close()
        let reopened = try SynchroDatabase(path: environment.path)
        try assertNoDurableProgress(snapshot, reopened)
        XCTAssertEqual(
            try ChangeTracker(database: reopened).inspectRetainedMutations().only?.status,
            .serverRejected
        )
        XCTAssertEqual(
            try reopened.readTransaction { try SynchroMeta.listRejectedMutations($0).only?.mutationID },
            source.mutationID
        )
        try reopened.close()
    }

    private func assertTamperedSealedBatchDoesNotTransmit() async throws {
        let environment = try makeEnvironment("tampered-sealed-batch")
        let tracker = ChangeTracker(database: environment.database)
        let processor = PushProcessor(database: environment.database, changeTracker: tracker)
        _ = try environment.database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["tampered-batch-row", "original", "u1", "2026-04-05T00:00:00.000000Z"]
        )
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        let http = makeHTTPClient(path: environment.path)
        MockURLProtocol.requestHandler = { request in
            requestCount.withLock { $0 += 1 }
            return try self.retryResponse(request: request, status: 503, code: "temporary_unavailable")
        }
        let sealFailure = await pushFailure(processor, http: http, table: environment.table)
        XCTAssertTrue(sealFailure is RetryableError)
        try environment.database.writeTransaction { connection in
            try connection.execute(
                sql: """
                    UPDATE _synchro_mutation_values
                    SET value_text = 'tampered'
                    WHERE field_id = 'field-ship-address'
                    """
            )
        }
        let before = try durableSnapshot(environment.database)
        let tamperFailure = await pushFailure(processor, http: http, table: environment.table)
        XCTAssertNotNil(tamperFailure)
        XCTAssertEqual(requestCount.withLock { $0 }, 1)
        try assertNoDurableProgress(before, environment.database)
        try environment.database.close()
    }

    private func makePreparedMigration(_ label: String) throws -> Environment {
        let path = temporaryDatabasePath(label)
        let database = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: database)
        var sourceManifest = protocolOrdersSchemaManifest(includeNotes: false)
        sourceManifest.schemaHash = try Integrity.schemaManifestHash(sourceManifest)
        let sourceTable = try XCTUnwrap(try sourceManifest.localTables().only)
        try manager.createSyncedTables(schema: SchemaResponse(
            schemaVersion: sourceManifest.schemaVersion,
            schemaHash: sourceManifest.schemaHash,
            serverTime: Date(),
            manifest: sourceManifest
        ))
        _ = try database.execute(
            "CREATE TABLE local_settings (key TEXT PRIMARY KEY, value TEXT)",
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO local_settings (key, value) VALUES ('theme', 'dark')",
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["migration-row", "offline", "u1", "2026-04-05T00:00:00.000000Z"]
        )
        let target = try targetManifest(sourceHash: sourceManifest.schemaHash)
        _ = try manager.prepareMigration(
            targetManifest: target,
            action: .replace,
            affectedScopes: [],
            scopeCursorUpdates: [:],
            schemaReset: false
        )
        return Environment(path: path, database: database, table: sourceTable)
    }

    private func targetManifest(sourceHash: String) throws -> SchemaManifest {
        var manifest = protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: sourceHash),
            transitionClass: "class_2",
            compatibilityFloor: 1
        )
        manifest.schemaHash = try Integrity.schemaManifestHash(manifest)
        return manifest
    }

    private func hasColumn(
        _ database: SynchroDatabase,
        table: String,
        column: String
    ) throws -> Bool {
        try database.query("PRAGMA table_info(\(SQLiteHelpers.quoteIdentifier(table)))", params: nil)
            .contains { ($0["name"] as String?) == column }
    }

    private func createPortableSeed(_ label: String) throws -> PortableSeed {
        let path = temporaryDatabasePath(label)
        let database = try SynchroDatabase(path: path)
        var manifest = protocolOrdersSchemaManifest()
        manifest.schemaHash = try Integrity.schemaManifestHash(manifest)
        let table = try XCTUnwrap(try manifest.localTables().only)
        let scopeID = "orders:portable"
        let row = orderRow(
            recordID: "seed-row",
            address: "Seeded Address",
            updatedAt: "2026-04-06T00:00:00.000000Z"
        )
        let pk = orderPK("seed-row")
        let digest = try Integrity.rowDigest(
            schemaHash: manifest.schemaHash,
            table: table,
            pk: pk,
            row: row,
            serverVersion: "seed-server-version"
        )
        let scopeChecksum = try Integrity.scopeDigest(
            schemaHash: manifest.schemaHash,
            scopeID: scopeID,
            entries: [(identity: digest.identity, digest: digest.checksum)]
        )
        let checksumJSON = try jsonString(scopeChecksum)
        let receipt = "sc1.issue-49-portable-receipt.signature"
        let manifestJSON = try jsonString(manifest)
        try SchemaManager(database: database).createSyncedTables(schema: SchemaResponse(
            schemaVersion: manifest.schemaVersion,
            schemaHash: manifest.schemaHash,
            serverTime: Date(),
            manifest: manifest
        ))
        try database.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: """
                    INSERT INTO orders (id, ship_address, user_id, updated_at, deleted_at)
                    VALUES (?, ?, ?, ?, NULL)
                    """,
                arguments: ["seed-row", "Seeded Address", "u1", "2026-04-06T00:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: table.tableName,
                recordID: "seed-row",
                serverVersion: "seed-server-version",
                rowChecksum: digest.checksum
            )
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: nil,
                checksum: checksumJSON,
                generation: 0,
                localChecksum: checksumJSON
            )
            try SynchroMeta.upsertScopeRow(
                connection,
                scopeID: scopeID,
                tableName: table.tableName,
                recordID: "seed-row",
                checksum: digest.checksum.digest,
                generation: 0
            )
            try connection.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, 1, ?)
                    """,
                arguments: [scopeID, receipt, manifest.schemaVersion, manifest.schemaHash, checksumJSON]
            )
            try SynchroMeta.set(connection, key: .schemaManifest, value: manifestJSON)
            try SynchroMeta.setInt64(connection, key: .scopeSetVersion, value: 0)
            try SynchroMeta.set(connection, key: .snapshotComplete, value: "1")
        }
        try database.close()
        removeSQLiteSidecars(path)
        return PortableSeed(
            path: path,
            scopeID: scopeID,
            table: table,
            rowDigest: digest,
            receipt: receipt
        )
    }

    private func assertSeedRejection(
        _ label: String,
        mutate: (GRDB.Database) throws -> Void
    ) throws {
        let seed = try createPortableSeed(label)
        let destination = temporaryDatabasePath("\(label)-destination")
        let raw = try DatabaseQueue(path: seed.path)
        try raw.write(mutate)
        try raw.close()
        removeSQLiteSidecars(seed.path)
        let sourceBefore = try Data(contentsOf: URL(fileURLWithPath: seed.path))

        XCTAssertThrowsError(try SeedDatabaseInstaller.installIfNeeded(
            seedPath: seed.path,
            databasePath: destination
        ))
        XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: seed.path)), sourceBefore)
        assertNoDatabaseFamily(destination)
    }

    private func assertSeedWithQueuedIntentIsRejected() throws {
        let seed = try createPortableSeed("seed-queued-intent")
        let destination = temporaryDatabasePath("seed-queued-intent-destination")
        let database = try SynchroDatabase(path: seed.path)
        _ = try database.execute(
            "UPDATE orders SET ship_address = 'queued-intent' WHERE id = 'seed-row'",
            params: nil
        )
        XCTAssertEqual(try ChangeTracker(database: database).pendingChangeCount(), 1)
        try database.close()
        removeSQLiteSidecars(seed.path)

        XCTAssertThrowsError(try SeedDatabaseInstaller.installIfNeeded(
            seedPath: seed.path,
            databasePath: destination
        ))
        assertNoDatabaseFamily(destination)
    }

    private func connectNone(
        scopeSetVersion: Int64,
        cursorUpdates: [String: String?] = [:]
    ) -> ConnectResponse {
        ConnectResponse(
            serverTime: "2026-04-07T00:00:00.000000Z",
            protocolVersion: 3,
            clientGeneration: 1,
            scopeSetVersion: scopeSetVersion,
            schema: SchemaDescriptor(
                version: 1,
                hash: protocolTestSchemaHash,
                action: .none,
                reason: nil
            ),
            scopes: ScopeAssignmentDelta(add: [], remove: []),
            scopeCursorUpdates: cursorUpdates,
            schemaDefinition: nil,
            affectedScopes: nil
        )
    }

    private func makeEngine(database: SynchroDatabase, path: String) -> SyncEngine {
        let config = SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://issue49.invalid")!,
            authProvider: { "token" },
            clientID: "issue-49-client",
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 0
        )
        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfiguration)
        sessions.append(session)
        let tracker = ChangeTracker(database: database)
        return SyncEngine(
            config: config,
            database: database,
            httpClient: HttpClient(config: config, session: session),
            schemaManager: SchemaManager(database: database),
            changeTracker: tracker,
            pullProcessor: PullProcessor(database: database),
            pushProcessor: PushProcessor(database: database, changeTracker: tracker)
        )
    }

    private func projectionTable(titleName: String, enabledName: String) -> LocalSchemaTable {
        LocalSchemaTable(
            tableID: "table-projection-orders",
            relationID: "relation-projection-orders",
            tableName: "projection_orders",
            primaryKeyFieldID: "field-id",
            createdAtFieldID: nil,
            updatedAtFieldID: "field-updated-at",
            deletedAtFieldID: "field-deleted-at",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: [
                localColumn("field-id", "id", "string", nullable: false, writable: false, primaryKey: true),
                localColumn("field-title", titleName, "string", nullable: true, writable: true),
                localColumn("field-enabled", enabledName, "boolean", nullable: true, writable: true),
                localColumn("field-updated-at", "updated_at", "datetime", nullable: false, writable: false),
                localColumn("field-deleted-at", "deleted_at", "datetime", nullable: true, writable: false),
            ]
        )
    }

    private func projectionRow(
        table: LocalSchemaTable,
        recordID: String,
        title: String,
        enabled: Bool
    ) -> [String: AnyCodable] {
        Dictionary(uniqueKeysWithValues: table.columns.map { column in
            let value: Any
            switch column.fieldID {
            case "field-id": value = recordID
            case "field-title": value = title
            case "field-enabled": value = enabled
            case "field-updated-at": value = "2026-04-08T01:00:00.000000Z"
            case "field-deleted-at": value = NSNull()
            default: fatalError("unexpected projection field")
            }
            return (column.fieldID, AnyCodable(value))
        })
    }

    private func makeHTTPClient(path: String) -> HttpClient {
        let config = SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://issue49.invalid")!,
            authProvider: { "token" },
            clientID: "issue-49-client",
            appVersion: "1.0.0"
        )
        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfiguration)
        sessions.append(session)
        return HttpClient(config: config, session: session)
    }

    private func successResponse<Value: Encodable>(
        request: URLRequest,
        value: Value
    ) throws -> (HTTPURLResponse, Data) {
        (
            HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!,
            try JSONEncoder.synchroEncoder().encode(value)
        )
    }

    private func retryResponse(
        request: URLRequest,
        status: Int,
        code: String
    ) throws -> (HTTPURLResponse, Data) {
        (
            HTTPURLResponse(
                url: request.url!,
                statusCode: status,
                httpVersion: nil,
                headerFields: [
                    "Content-Type": "application/json",
                    "Retry-After": "1",
                ]
            )!,
            try JSONSerialization.data(withJSONObject: [
                "error": [
                    "code": code,
                    "message": "retry",
                    "retryable": true,
                ],
            ])
        )
    }

    private func errorResponse(
        request: URLRequest,
        status: Int
    ) throws -> (HTTPURLResponse, Data) {
        (
            HTTPURLResponse(
                url: request.url!,
                statusCode: status,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!,
            try JSONSerialization.data(withJSONObject: [
                "error": [
                    "code": "internal_error",
                    "message": "unexpected request",
                    "retryable": false,
                ],
            ])
        )
    }

    private func jsonString<Value: Encodable>(_ value: Value) throws -> String {
        try XCTUnwrap(String(data: try JSONEncoder.synchroEncoder().encode(value), encoding: .utf8))
    }

    private func durableSnapshot(_ database: SynchroDatabase) throws -> [String: [String]] {
        try database.readTransaction { connection in
            let tables = try String.fetchAll(
                connection,
                sql: """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                    ORDER BY name
                    """
            )
            var snapshot: [String: [String]] = [
                "__schema": try String.fetchAll(
                    connection,
                    sql: """
                        SELECT type || '|' || name || '|' || tbl_name || '|' || quote(sql)
                        FROM sqlite_master
                        WHERE name NOT LIKE 'sqlite_%'
                        ORDER BY type, name
                        """
                ),
            ]
            for table in tables {
                let columns = try connection.columns(in: table).map(\.name)
                guard !columns.isEmpty else {
                    snapshot[table] = []
                    continue
                }
                let quotedColumns = columns.map(SQLiteHelpers.quoteIdentifier)
                let valueExpression = quotedColumns
                    .map { "quote(\($0))" }
                    .joined(separator: " || char(31) || ")
                snapshot[table] = try String.fetchAll(
                    connection,
                    sql: """
                        SELECT \(valueExpression)
                        FROM \(SQLiteHelpers.quoteIdentifier(table))
                        ORDER BY \(quotedColumns.joined(separator: ", "))
                        """
                )
            }
            return snapshot
        }
    }

    private func assertNoDurableProgress(
        _ expected: [String: [String]],
        _ database: SynchroDatabase,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        XCTAssertEqual(expected, try durableSnapshot(database), file: file, line: line)
    }

    private func temporaryDatabasePath(_ label: String) -> String {
        let path = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("synchro-issue-49-complete-\(label)-\(UUID().uuidString).sqlite")
            .path
        temporaryPaths.append(path)
        return path
    }

    private func removeSQLiteSidecars(_ path: String) {
        for suffix in ["-journal", "-wal", "-shm"] {
            try? FileManager.default.removeItem(atPath: path + suffix)
        }
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
        let url = URL(fileURLWithPath: path)
        let prefix = ".\(url.lastPathComponent).seed-"
        let siblings = (try? FileManager.default.contentsOfDirectory(atPath: url.deletingLastPathComponent().path)) ?? []
        XCTAssertFalse(siblings.contains(where: { $0.hasPrefix(prefix) }), file: file, line: line)
    }
}

private extension Array {
    var only: Element? {
        count == 1 ? self[0] : nil
    }
}
