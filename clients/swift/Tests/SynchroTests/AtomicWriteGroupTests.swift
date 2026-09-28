import XCTest
import GRDB
@testable import Synchro

final class AtomicWriteGroupTests: XCTestCase {
    private let ordersTable = SchemaTable(
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
    private let clientID = "test-device"
    private let encoder = JSONEncoder.synchroEncoder()
    private var db: SynchroDatabase!
    private var tracker: ChangeTracker!
    private var processor: PushProcessor!

    override func setUpWithError() throws {
        try openDatabase(tables: [ordersTable])
    }

    private func openDatabase(tables: [SchemaTable]) throws {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_atomic_\(UUID().uuidString).sqlite")
        db = try SynchroDatabase(path: path)
        try SchemaManager(database: db).createSyncedTables(
            schema: SchemaResponse(schemaVersion: 1, schemaHash: protocolTestSchemaHash, serverTime: Date(), tables: tables)
        )
        tracker = ChangeTracker(database: db)
        processor = PushProcessor(database: db, changeTracker: tracker)
    }

    override func tearDownWithError() throws {
        try db.close()
    }

    // MARK: - Capture

    func testGroupCapturesOneGroupIDAndRemovesTheMetaKey() throws {
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try insertOrder(transaction, id: "g2", address: "b")
        }
        try insertOrder(id: "u1", address: "c")

        let groups = try db.query(
            "SELECT record_id, atomic_group_id FROM _synchro_pending_changes ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(groups.map { $0["record_id"] as String }, ["g1", "g2", "u1"])
        let groupID = try XCTUnwrap(groups[0]["atomic_group_id"] as String?)
        XCTAssertEqual(groupID, groupID.lowercased())
        XCTAssertNotNil(UUID(uuidString: groupID))
        XCTAssertEqual(groups[1]["atomic_group_id"] as String?, groupID)
        XCTAssertNil(groups[2]["atomic_group_id"] as String?)
        XCTAssertNil(try atomicGroupMeta())
    }

    func testEachGroupHasANewGroupID() throws {
        try atomicWrite { try insertOrder($0, id: "g1", address: "a") }
        try atomicWrite { try insertOrder($0, id: "g2", address: "b") }

        let groupIDs = try db.query(
            "SELECT atomic_group_id FROM _synchro_pending_changes ORDER BY local_order",
            params: nil
        ).map { $0["atomic_group_id"] as String? }
        XCTAssertEqual(groupIDs.count, 2)
        XCTAssertNotNil(groupIDs[0])
        XCTAssertNotNil(groupIDs[1])
        XCTAssertNotEqual(groupIDs[0], groupIDs[1])
    }

    func testEmptyGroupCommitsAsAnOrdinaryWrite() throws {
        let result = try atomicWrite { transaction -> Int in
            try transaction.execute("CREATE TABLE IF NOT EXISTS app_notes (id TEXT PRIMARY KEY)")
            try transaction.execute("INSERT INTO app_notes (id) VALUES ('n1')")
            return 7
        }

        XCTAssertEqual(result, 7)
        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM app_notes"), 1)
        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes"), 0)
        XCTAssertNil(try atomicGroupMeta())
    }

    func testFailingBodyRollsBackRowsCaptureAndGroupID() throws {
        struct BodyFailure: Error {}

        XCTAssertThrowsError(try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            throw BodyFailure()
        }) { XCTAssertTrue($0 is BodyFailure) }

        try assertRolledBack()
    }

    // MARK: - Rule 1: delete followed by a write

    func testDeleteFollowedByWriteInGroupRollsBack() throws {
        XCTAssertThrowsError(try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute(
                "UPDATE orders SET deleted_at = ? WHERE id = ?",
                params: ["2026-01-01T10:30:00.000Z", "g1"]
            )
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["after delete", "g1"])
        }) { self.assertReason($0, .deleteFollowedByWrite) }

        try assertRolledBack()
    }

    func testDeleteAsLastGroupEntryCommits() throws {
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "g1"])
            try transaction.execute(
                "UPDATE orders SET deleted_at = ? WHERE id = ?",
                params: ["2026-01-01T10:30:00.000Z", "g1"]
            )
        }

        XCTAssertEqual(try tracker.pendingChanges().count, 0)
        XCTAssertEqual(
            try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE lifecycle_state = 'cancelled_before_send'"),
            3
        )
    }

    func testDeleteBeforeTheGroupDoesNotFailTheGroup() throws {
        try markSynced(recordID: "s1")
        _ = try db.execute(
            "UPDATE orders SET deleted_at = ? WHERE id = ?",
            params: ["2026-01-01T10:30:00.000Z", "s1"]
        )

        try atomicWrite { transaction in
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["later", "s1"])
        }

        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL"), 1)
    }

    // MARK: - Rule 2: normalization of group rows only

    func testGroupChainNormalizesToOneGroupedEntry() throws {
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "g1"])
        }

        let rows = try db.query(
            "SELECT lifecycle_state, source_kind, operation, atomic_group_id, dependency_mutation_id FROM _synchro_pending_changes ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(rows.map { $0["lifecycle_state"] as String }, ["superseded_before_send", "superseded_before_send", "unsealed"])
        XCTAssertEqual(rows[2]["source_kind"] as String?, "normalized")
        XCTAssertEqual(rows[2]["operation"] as String?, "insert")
        XCTAssertNil(rows[2]["dependency_mutation_id"] as String?)
        XCTAssertNotNil(rows[2]["atomic_group_id"] as String?)
        XCTAssertEqual(rows[2]["atomic_group_id"] as String?, rows[0]["atomic_group_id"] as String?)
    }

    func testChainMergesOnlyEntriesWithAnEqualGroup() throws {
        try insertOrder(id: "r1", address: "a")
        try atomicWrite { transaction in
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "r1"])
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["c", "r1"])
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["d", "r1"])
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["e", "r1"])

        _ = try tracker.pendingChanges()

        let live = try db.query(
            """
            SELECT mutation_id, operation, atomic_group_id, dependency_mutation_id
            FROM _synchro_pending_changes WHERE lifecycle_state = 'unsealed' ORDER BY local_order
            """,
            params: nil
        )
        XCTAssertEqual(live.map { $0["operation"] as String }, ["insert", "update", "update"])
        XCTAssertNil(live[0]["atomic_group_id"] as String?)
        XCTAssertNotNil(live[1]["atomic_group_id"] as String?)
        XCTAssertNil(live[2]["atomic_group_id"] as String?)
        XCTAssertNil(live[0]["dependency_mutation_id"] as String?)
        XCTAssertEqual(live[1]["dependency_mutation_id"] as String?, live[0]["mutation_id"] as String?)
        XCTAssertEqual(live[2]["dependency_mutation_id"] as String?, live[1]["mutation_id"] as String?)
        let groupValues = try tracker.inspectPendingMutations().first { $0.mutationID == live[1]["mutation_id"] as String }
        XCTAssertEqual(groupValues?.authoredFields.first { $0.fieldID == "ship_address" }?.value, AnyCodable("c"))
    }

    // MARK: - Rule 3: too many mutations

    func testGroupAtMutationCountLimitCommits() throws {
        try atomicWrite { transaction in
            for index in 0..<1000 {
                try insertOrder(transaction, id: String(format: "g%04d", index), address: "a")
            }
        }

        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL"), 1000)
    }

    func testGroupAboveMutationCountLimitRollsBack() throws {
        XCTAssertThrowsError(try atomicWrite { transaction in
            for index in 0..<1001 {
                try insertOrder(transaction, id: String(format: "g%04d", index), address: "a")
            }
        }) { self.assertReason($0, .tooManyMutations) }

        try assertRolledBack()
    }

    func testMutationCountUsesTheNormalizedGroup() throws {
        try atomicWrite { transaction in
            for index in 0..<1000 {
                try insertOrder(transaction, id: String(format: "g%04d", index), address: "a")
            }
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "g0000"])
        }

        XCTAssertEqual(
            try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL AND lifecycle_state = 'unsealed'"),
            1000
        )
    }

    // MARK: - Rule 4: mutation too large

    func testInsertAtNormalizedMutationLimitCommits() throws {
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: insertAddress(recordID: "g1", normalizedOctets: 65_536))
        }

        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL"), 1)
    }

    func testInsertAboveNormalizedMutationLimitRollsBack() throws {
        XCTAssertThrowsError(try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "small")
            try insertOrder(transaction, id: "g2", address: insertAddress(recordID: "g2", normalizedOctets: 65_537))
        }) { self.assertReason($0, .mutationTooLarge) }

        try assertRolledBack()
    }

    func testUpdateWithUnknownBaseIsMeasuredWithTheBasePlaceholder() throws {
        let placeholder = String(repeating: "v", count: 64)
        let emptyOctets = try updateOctetsWithEmptyAddress(baseVersion: placeholder)
        try insertOrder(id: "u1", address: "a")
        try insertOrder(id: "u2", address: "a")

        try atomicWrite { transaction in
            try transaction.execute(
                "UPDATE orders SET ship_address = ? WHERE id = ?",
                params: [String(repeating: "a", count: 65_536 - emptyOctets), "u1"]
            )
        }
        XCTAssertThrowsError(try atomicWrite { transaction in
            try transaction.execute(
                "UPDATE orders SET ship_address = ? WHERE id = ?",
                params: [String(repeating: "a", count: 65_537 - emptyOctets), "u2"]
            )
        }) { self.assertReason($0, .mutationTooLarge) }

        let updates = try db.query(
            "SELECT record_id, base_version, dependency_mutation_id FROM _synchro_pending_changes WHERE operation = 'update' AND lifecycle_state = 'unsealed'",
            params: nil
        )
        XCTAssertEqual(updates.map { $0["record_id"] as String }, ["u1"])
        XCTAssertNil(updates[0]["base_version"] as String?)
        XCTAssertNotNil(updates[0]["dependency_mutation_id"] as String?)
    }

    func testMutationAboveAuthoredColumnLimitRollsBack() throws {
        let authoredColumns = (0...PushLimits.maxAuthoredColumns).map { SchemaColumn(name: String(format: "c%03d", $0)) }
        let wideTable = SchemaTable(
            tableName: "wide_items",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [SchemaColumn(name: "id", nullable: false, isPrimaryKey: true)]
                + authoredColumns
                + [
                    SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false),
                    SchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true),
                ]
        )
        try db.close()
        try openDatabase(tables: [wideTable])
        func insert(_ transaction: ApplicationTransaction, id: String, columns: ArraySlice<SchemaColumn>) throws {
            let names = ["id"] + columns.map(\.name) + ["updated_at"]
            let values = [id] + columns.map { _ in "v" } + ["2026-01-01T10:00:00.000Z"]
            try transaction.execute(
                "INSERT INTO wide_items (\(names.joined(separator: ", "))) VALUES (\(names.map { _ in "?" }.joined(separator: ", ")))",
                params: values
            )
        }

        try atomicWrite { try insert($0, id: "w256", columns: authoredColumns.dropLast()) }
        XCTAssertThrowsError(try atomicWrite { try insert($0, id: "w257", columns: authoredColumns[...]) }) {
            self.assertReason($0, .mutationTooLarge)
        }

        XCTAssertEqual(
            try db.query("SELECT record_id FROM _synchro_pending_changes", params: nil).map { $0["record_id"] as String },
            ["w256"]
        )
    }

    // MARK: - Rule 5: request too large

    func testWorstCaseRequestAtRequestLimitCommits() throws {
        let addresses = try requestLimitAddresses(extraOctets: 0)

        try atomicWrite { transaction in
            for (id, address) in addresses {
                try insertOrder(transaction, id: id, address: address)
            }
        }

        XCTAssertEqual(
            try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL"),
            addresses.count
        )
    }

    func testWorstCaseRequestAboveRequestLimitRollsBack() throws {
        let addresses = try requestLimitAddresses(extraOctets: 1)

        XCTAssertThrowsError(try atomicWrite { transaction in
            for (id, address) in addresses {
                try insertOrder(transaction, id: id, address: address)
            }
        }) { self.assertReason($0, .requestTooLarge) }

        try assertRolledBack()
    }

    // MARK: - Batch selection

    func testUngroupedRunStopsAtTheFirstGroupedEntry() throws {
        try insertOrder(id: "u1", address: "a")
        try insertOrder(id: "u2", address: "a")
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try insertOrder(transaction, id: "g2", address: "a")
        }
        try insertOrder(id: "u3", address: "a")

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).map(\.recordID), ["u1", "u2"])
        XCTAssertEqual(try tracker.pendingChanges(limit: 1).map(\.recordID), ["u1"])
    }

    func testUngroupedNormalizedMutationKeepsItsFirstSourceOrder() throws {
        try insertOrder(id: "parent", address: "a")
        try insertOrder(id: "child", address: "a")
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "parent"])

        let batch = try tracker.pendingChanges(limit: 100)

        XCTAssertEqual(batch.map(\.recordID), ["parent", "child"])
        XCTAssertEqual(batch.first?.sourceKind, "normalized")
        XCTAssertEqual(batch.first?.fieldValuesByID["ship_address"]?.textValue, "b")
        XCTAssertEqual(try tracker.pendingChanges(limit: 1).map(\.recordID), ["parent"])
    }

    func testGroupFirstSelectsTheCompleteGroupAndIgnoresTheBatchSize() throws {
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try insertOrder(transaction, id: "g2", address: "a")
            try insertOrder(transaction, id: "g3", address: "a")
        }
        try insertOrder(id: "u1", address: "a")

        let batch = try tracker.pendingChanges(limit: 1)

        XCTAssertEqual(batch.map(\.recordID), ["g1", "g2", "g3"])
        XCTAssertEqual(Set(batch.map(\.atomicGroupID)).count, 1)
    }

    func testGroupWaitsForAnUnresolvedMemberDependency() throws {
        try insertOrder(id: "d1", address: "a")
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "d1"])
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["c", "d1"])
        }
        let predecessorID = try mutationID(recordID: "d1", operation: "insert")
        let update = try XCTUnwrap(db.queryOne(
            """
            SELECT mutation_id, dependency_mutation_id FROM _synchro_pending_changes
            WHERE record_id = 'd1' AND operation = 'update' AND lifecycle_state = 'unsealed'
            """,
            params: nil
        ))
        XCTAssertEqual(update["dependency_mutation_id"] as String?, predecessorID)
        try db.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'sealed' WHERE mutation_id = ?",
                arguments: [predecessorID]
            )
        }

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).count, 0)

        try db.writeTransaction { connection in
            try tracker.markAccepted(connection, mutationID: predecessorID, acceptedJSON: "{}")
            try tracker.refreshUnsealedSuccessor(connection, mutationID: update["mutation_id"], serverVersion: "server-version-1")
        }

        let group = try tracker.pendingChanges(limit: 100)
        XCTAssertEqual(group.map(\.recordID), ["g1", "d1"])
        XCTAssertEqual(group.last?.fieldValuesByID["ship_address"]?.textValue, "c")
    }

    func testUngroupedChainBeforeAGroupIsSentBeforeTheGroup() throws {
        try insertOrder(id: "r1", address: "a")
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "r1"])
        try atomicWrite { transaction in
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["c", "r1"])
            try insertOrder(transaction, id: "g1", address: "a")
        }

        let insert = try tracker.pendingChanges(limit: 100)
        XCTAssertEqual(insert.map(\.operation), ["insert"])
        XCTAssertNil(insert.first?.atomicGroupID)
        try acceptAndRefreshSuccessors(insert)

        let update = try tracker.pendingChanges(limit: 100)
        XCTAssertEqual(update.map(\.operation), ["update"])
        XCTAssertNil(update.first?.atomicGroupID)
        XCTAssertEqual(update.first?.fieldValuesByID["ship_address"]?.textValue, "b")
        try acceptAndRefreshSuccessors(update)

        let group = try tracker.pendingChanges(limit: 100)
        XCTAssertEqual(group.map(\.recordID), ["r1", "g1"])
        XCTAssertEqual(group.first?.fieldValuesByID["ship_address"]?.textValue, "c")
        XCTAssertNotNil(group.first?.atomicGroupID)
        XCTAssertEqual(Set(group.map(\.atomicGroupID)).count, 1)
    }

    func testGroupWaitsForAMemberUpdateWithoutABaseVersion() throws {
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES ('n1', 'a', 'u1', '2026-01-01T10:00:00.000Z')"
            )
        }
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "n1"])
        }
        let update = try XCTUnwrap(db.queryOne(
            "SELECT base_version, dependency_mutation_id FROM _synchro_pending_changes WHERE record_id = 'n1'",
            params: nil
        ))
        XCTAssertNil(update["base_version"] as String?)
        XCTAssertNil(update["dependency_mutation_id"] as String?)

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).count, 0)
    }

    func testGroupWaitsWhileAMemberIsNotUnsealed() throws {
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try insertOrder(transaction, id: "g2", address: "a")
        }
        let memberID = try mutationID(recordID: "g2", operation: "insert")
        try db.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'legacy_blocked' WHERE mutation_id = ?",
                arguments: [memberID]
            )
        }

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).count, 0)
    }

    // MARK: - Group blocking

    func testDependentUpdatesAfterBlockedPredecessorBecomeBlockedBeforeSelection() throws {
        try insertOrder(id: "d1", address: "a")
        let predecessorID = try mutationID(recordID: "d1", operation: "insert")
        try db.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'blocked_by_predecessor' WHERE mutation_id = ?",
                arguments: [predecessorID]
            )
        }
        for address in ["b", "c", "d"] {
            _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: [address, "d1"])
        }

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).count, 0)

        let dependents = try db.query(
            "SELECT mutation_id, lifecycle_state, dependency_mutation_id FROM _synchro_pending_changes WHERE operation = 'update' ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(dependents.count, 3)
        XCTAssertTrue(dependents.allSatisfy { ($0["lifecycle_state"] as String?) == "blocked_by_predecessor" })
        XCTAssertEqual(
            dependents.map { $0["dependency_mutation_id"] as String? },
            [predecessorID] + dependents.dropLast().map { $0["mutation_id"] as String? }
        )
    }

    func testGroupMemberAfterLegacyBlockedPredecessorBlocksTheWholeGroup() throws {
        try insertOrder(id: "d1", address: "a")
        let predecessorID = try mutationID(recordID: "d1", operation: "insert")
        try db.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'legacy_blocked' WHERE mutation_id = ?",
                arguments: [predecessorID]
            )
        }
        try atomicWrite { transaction in
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "d1"])
            try insertOrder(transaction, id: "g1", address: "a")
        }

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).count, 0)

        let states = try db.query(
            "SELECT record_id, lifecycle_state, dependency_mutation_id FROM _synchro_pending_changes WHERE lifecycle_state <> 'legacy_blocked' ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(states.map { $0["record_id"] as String }, ["d1", "g1"])
        XCTAssertTrue(states.allSatisfy { ($0["lifecycle_state"] as String?) == "blocked_by_predecessor" })
        XCTAssertEqual(states[0]["dependency_mutation_id"] as String?, predecessorID)
        XCTAssertNil(states[1]["dependency_mutation_id"] as String?)
    }

    func testBlockingOneUnsentMemberBlocksTheWholeGroup() throws {
        try insertOrder(id: "d1", address: "a")
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "d1"])
            try insertOrder(transaction, id: "g2", address: "a")
        }
        try insertOrder(id: "u1", address: "a")
        let predecessorID = try mutationID(recordID: "d1", operation: "insert")

        try db.writeTransaction { connection in
            try tracker.markRejected(connection, mutationID: predecessorID, rejectedJSON: "{}")
            try tracker.blockDependents(connection, predecessorID: predecessorID)
        }

        let states = try db.query(
            "SELECT record_id, lifecycle_state FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(states.map { $0["record_id"] as String }, ["g1", "d1", "g2"])
        XCTAssertTrue(states.allSatisfy { ($0["lifecycle_state"] as String?) == "blocked_by_predecessor" })
        XCTAssertEqual(try tracker.pendingChanges(limit: 100).map(\.recordID), ["u1"])
    }

    func testMemberAfterABlockedEntryBlocksTheWholeGroup() throws {
        try markSynced(recordID: "s1")
        _ = try db.execute(
            "UPDATE orders SET deleted_at = ? WHERE id = ?",
            params: ["2026-01-01T10:30:00.000Z", "s1"]
        )
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["after delete", "s1"])
        XCTAssertEqual(try tracker.pendingChanges(limit: 100).map(\.operation), ["delete"])
        try atomicWrite { transaction in
            try insertOrder(transaction, id: "g1", address: "a")
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["later", "s1"])
        }

        _ = try tracker.pendingChanges(limit: 100)

        let states = try db.query(
            "SELECT lifecycle_state FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL",
            params: nil
        ).map { $0["lifecycle_state"] as String }
        XCTAssertEqual(states, ["blocked_by_predecessor", "blocked_by_predecessor"])
    }

    func testMemberThatDependsOnABlockedEntryBlocksTheGroupAndItsDependents() throws {
        try insertOrder(id: "r1", address: "a")
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["b", "r1"])
        let rejectedID = try mutationID(recordID: "r1", operation: "insert")
        try db.writeTransaction { connection in
            try tracker.markRejected(connection, mutationID: rejectedID, rejectedJSON: "{}")
            try tracker.blockDependents(connection, predecessorID: rejectedID)
        }
        try atomicWrite { transaction in
            try transaction.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["c", "r1"])
            try insertOrder(transaction, id: "g1", address: "a")
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["later", "g1"])

        XCTAssertEqual(try tracker.pendingChanges(limit: 100).count, 0)

        let states = try db.query(
            "SELECT record_id, atomic_group_id, lifecycle_state FROM _synchro_pending_changes WHERE lifecycle_state <> 'rejected' ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(states.map { $0["record_id"] as String }, ["r1", "r1", "g1", "g1"])
        XCTAssertEqual(states.map { $0["atomic_group_id"] as String? != nil }, [false, true, true, false])
        XCTAssertTrue(states.allSatisfy { ($0["lifecycle_state"] as String?) == "blocked_by_predecessor" })
    }

    // MARK: - Helpers

    /// Marks each entry accepted and gives each unsealed successor a server base version.
    private func acceptAndRefreshSuccessors(_ entries: [PendingChange]) throws {
        try db.writeTransaction { connection in
            for entry in entries {
                try tracker.markAccepted(connection, mutationID: entry.mutationID, acceptedJSON: "{}")
                for successor in try tracker.successors(connection, predecessorID: entry.mutationID) {
                    try tracker.refreshUnsealedSuccessor(
                        connection,
                        mutationID: successor.mutationID,
                        serverVersion: "server-version-\(entry.mutationID)"
                    )
                }
            }
        }
    }

    private func atomicWrite<T>(_ body: (ApplicationTransaction) throws -> T) throws -> T {
        try db.applicationAtomicWriteTransaction(
            validate: { connection, groupID in
                try processor.validateAtomicGroup(connection, groupID: groupID, clientID: clientID)
            },
            body
        )
    }

    private func insertOrder(_ transaction: ApplicationTransaction, id: String, address: String) throws {
        try transaction.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: [id, address, "u1", "2026-01-01T10:00:00.000Z"]
        )
    }

    private func insertOrder(id: String, address: String) throws {
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: [id, address, "u1", "2026-01-01T10:00:00.000Z"]
        )
    }

    /// Inserts a row and records it as synced with no pending change.
    private func markSynced(recordID: String) throws {
        try insertOrder(id: recordID, address: "a")
        try db.writeTransaction { connection in
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: recordID,
                serverVersion: "server-version-1",
                rowChecksum: nil
            )
        }
        try tracker.clearAll()
    }

    private func ordersMutation(recordID: String, op: Synchro.Operation, baseVersion: String?, address: String) -> Mutation {
        Mutation(
            mutationID: UUID().uuidString.lowercased(),
            table: ordersTable.tableID,
            op: op,
            pk: ["id": AnyCodable(recordID)],
            authoredSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            baseVersion: baseVersion,
            clientVersion: "2026-01-01T10:00:00.000000Z",
            columns: op == .insert
                ? ["ship_address": AnyCodable(address), "user_id": AnyCodable("u1")]
                : ["ship_address": AnyCodable(address)]
        )
    }

    /// Gives an address that makes a captured `orders` insert have exactly the given normalized octets.
    private func insertAddress(recordID: String, normalizedOctets: Int) throws -> String {
        let empty = try PushLimits.measure(
            ordersMutation(recordID: recordID, op: .insert, baseVersion: nil, address: ""),
            encoder: encoder
        )
        return String(repeating: "a", count: normalizedOctets - empty.normalizedJSON.count)
    }

    /// Captures an update with an empty address and gives its normalized octets with the given base.
    ///
    /// The capture confirms that an update authors only the changed address.
    private func updateOctetsWithEmptyAddress(baseVersion: String) throws -> Int {
        try markSynced(recordID: "u0")
        _ = try db.execute("UPDATE orders SET ship_address = '' WHERE id = 'u0'", params: nil)
        let captured = try XCTUnwrap(tracker.pendingChanges().first)
        XCTAssertEqual(captured.operation, "update")
        XCTAssertEqual(Set(captured.fieldValuesByID.values.map(\.fieldID)), ["ship_address"])
        try tracker.clearAll()
        return try PushLimits.measure(
            ordersMutation(recordID: "u0", op: .update, baseVersion: baseVersion, address: ""),
            encoder: encoder
        ).normalizedJSON.count
    }

    /// Gives 17 inserts whose worst-case atomic request is the request limit plus `extraOctets`.
    ///
    /// Each insert stays within the mutation limits, so only the request rule can fail.
    private func requestLimitAddresses(extraOctets: Int) throws -> [(String, String)] {
        let ids = (0..<17).map { String(format: "g%02d", $0) }
        let reserve = try PushLimits.envelopeReserve(
            clientID: clientID,
            batchID: UUID().uuidString.lowercased(),
            schemaHash: String(repeating: "f", count: 64),
            atomic: true,
            encoder: encoder
        )
        let empty = try PushLimits.measure(
            ordersMutation(recordID: ids[0], op: .insert, baseVersion: nil, address: ""),
            encoder: encoder
        )
        XCTAssertEqual(reserve.body, reserve.canonical)
        XCTAssertEqual(empty.element.body, empty.element.canonical)
        let maxText = PushLimits.maxNormalizedMutationOctets - empty.normalizedJSON.count
        let totalText = PushLimits.maxRequestOctets + extraOctets
            - reserve.body - ids.count * empty.element.body - (ids.count - 1)
        XCTAssertLessThanOrEqual(totalText, ids.count * maxText)
        return ids.enumerated().map { index, id in
            let length = totalText / ids.count + (index < totalText % ids.count ? 1 : 0)
            return (id, String(repeating: "a", count: length))
        }
    }

    private func mutationID(recordID: String, operation: String) throws -> String {
        try XCTUnwrap(
            db.queryOne(
                "SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = ? AND operation = ?",
                params: [recordID, operation]
            )?["mutation_id"] as String?
        )
    }

    private func count(_ sql: String) throws -> Int? {
        try db.queryOne(sql, params: nil)?["count"] as Int?
    }

    private func atomicGroupMeta() throws -> String? {
        try db.queryOne("SELECT value FROM _synchro_meta WHERE key = 'atomic_group_id'", params: nil)?["value"] as String?
    }

    private func assertReason(
        _ error: Error,
        _ expected: AtomicGroupInvalidReason,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case let SynchroError.atomicGroupInvalid(reason) = error else {
            return XCTFail("unexpected error \(error)", file: file, line: line)
        }
        XCTAssertEqual(reason, expected, file: file, line: line)
    }

    private func assertRolledBack(file: StaticString = #filePath, line: UInt = #line) throws {
        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM orders WHERE id LIKE 'g%'"), 0, file: file, line: line)
        XCTAssertEqual(
            try count("SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE atomic_group_id IS NOT NULL"),
            0,
            file: file,
            line: line
        )
        XCTAssertEqual(try count("SELECT COUNT(*) AS count FROM _synchro_mutation_values"), 0, file: file, line: line)
        XCTAssertNil(try atomicGroupMeta(), file: file, line: line)
    }
}
