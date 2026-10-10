import XCTest
import GRDB
@testable @_spi(Inspection) import Synchro

final class InspectionTests: XCTestCase {
    private struct RollbackError: Error {}

    func testAcceptedOutcomeCapturePreservesStoredIdentityRawJSONAndBounds() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        for id in ["o1", "o2"] {
            _ = try client.execute("INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                params: [id, "authored", "2026-01-01T00:00:00.000000Z"])
        }
        let ids = try client.inspectRetainedMutations().map(\.mutationID)
        XCTAssertEqual(ids.count, 2)
        let first = " { \"mutation_id\": \"\(ids[0])\", \"marker\": \"é\" }\n"
        let second = "{\"marker\":\"second\", \"mutation_id\":\"\(ids[1])\"}"
        try client.database.writeTransaction { db in
            for (id, outcome) in zip(ids, [first, second]) {
                try db.execute(sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted', accepted_json = ? WHERE mutation_id = ?",
                    arguments: [outcome, id])
            }
        }
        let before = try client.database.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order", params: nil)
        var rows: [String] = []
        let inspection = SynchroInspection(client: client)
        let snapshot = try inspection.captureSnapshot(maximumRecords: 8) { _, transaction in
            rows = try transaction.query("SELECT id FROM orders ORDER BY id").map { $0["id"] }
            XCTAssertThrowsError(try transaction.execute("UPDATE orders SET title = 'not read only'"))
        }
        XCTAssertEqual(rows, ["o1", "o2"])
        XCTAssertEqual(snapshot.capture.acceptedMutationOutcomes, [ids[0]: first, ids[1]: second])
        XCTAssertFalse(snapshot.capture.acceptedMutationOutcomesTruncated)
        XCTAssertEqual(snapshot.capture.mutationLedgerCount, 2)
        XCTAssertEqual(snapshot.pendingChangeCount, 0)
        XCTAssertEqual(snapshot.retainedMutations, [])
        XCTAssertEqual(try client.inspectRetainedMutations(), [])
        XCTAssertEqual(try client.database.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order", params: nil), before)
        let countBound = try inspection.captureState(maximumRecords: 1)
        XCTAssertTrue(countBound.acceptedMutationOutcomesTruncated)
        XCTAssertEqual(countBound.acceptedMutationOutcomes, [:])
        let padding = 65_536 - ids.reduce(0) { $0 + $1.utf8.count } - first.utf8.count - second.utf8.count
        let exact = first + String(repeating: " ", count: padding)
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_pending_changes SET accepted_json = ? WHERE mutation_id = ?", arguments: [exact, ids[0]]) }
        XCTAssertFalse(try inspection.captureState(maximumRecords: 8).acceptedMutationOutcomesTruncated)
        XCTAssertEqual(try inspection.captureState(maximumRecords: 8).acceptedMutationOutcomes[ids[0]], exact)
        for oversized in [exact + " ", String(repeating: "x", count: 4_194_304)] {
            try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_pending_changes SET accepted_json = ? WHERE mutation_id = ?", arguments: [oversized, ids[0]]) }
            let capture = try inspection.captureState(maximumRecords: 8)
            XCTAssertTrue(capture.acceptedMutationOutcomesTruncated)
            XCTAssertTrue(capture.overflowed)
            XCTAssertEqual(capture.acceptedMutationOutcomes, [:])
        }
        for invalid in [DatabaseValue.null, Data("{}".utf8).databaseValue] {
            try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_pending_changes SET accepted_json = ? WHERE mutation_id = ?", arguments: [invalid, ids[0]]) }
            XCTAssertThrowsError(try inspection.captureState(maximumRecords: 8)) { error in
                guard case SynchroError.invalidResponse = error else { return XCTFail("Expected invalid response") }
            }
        }
        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    func testRecoveredStartupPauseStopAndCloseDrainWithoutTransport() async throws {
        for closing in [false, true] {
            let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("startup_pause_\(UUID().uuidString).sqlite")
            let collector = TransportObservationCollector()
            let client = try SynchroClient(config: SynchroConfig(dbPath: path, serverURL: URL(string: "http://test.local")!,
                authProvider: { "token" }, clientID: "inspection", appVersion: "1.0.0", transportObservationCollector: collector))
            var manifest = protocolOrdersSchemaManifest()
            manifest.schemaHash = try Integrity.schemaManifestHash(manifest)
            _ = try SchemaManager(database: client.database).prepareMigration(targetManifest: manifest, action: .replace,
                affectedScopes: [], scopeCursorUpdates: [:], schemaReset: false)
            try collector.armPause(for: MigrationCheckpoint.committed)
            let startFinished = expectation(description: "startup caller drains")
            let startup = Task { () -> Error? in
                defer { startFinished.fulfill() }
                do { try await client.start(); return nil } catch { return error }
            }
            try await collector.awaitPause(for: MigrationCheckpoint.committed, timeout: 2)
            XCTAssertEqual(try SynchroInspection(client: client).captureState(maximumRecords: 32).schema,
                SchemaRef(version: manifest.schemaVersion, hash: manifest.schemaHash))
            let stopFinished = expectation(description: "lifecycle shutdown drains")
            let shutdown = Task {
                if closing { try await client.close() } else { await client.stop() }
                stopFinished.fulfill()
            }
            await fulfillment(of: [stopFinished, startFinished], timeout: 2)
            startup.cancel()
            let error = await startup.value
            try await shutdown.value
            XCTAssertNotNil(error)
            XCTAssertFalse(collector.isMigrationCheckpointPaused)
            XCTAssertEqual(collector.snapshot().sequenceCheckpoint, 0)
            if !closing { try await client.close() }
            removeDatabase(at: path)
        }
    }

    func testMigrationCaptureReadsRawBindingsAndPhysicalColumnsWithoutNormalizingIntent() async throws {
        let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("migration_capture_\(UUID().uuidString).sqlite")
        let collector = TransportObservationCollector()
        let config = SynchroConfig(dbPath: path, serverURL: URL(string: "http://test.local")!,
                                  authProvider: { "token" }, clientID: "inspection", appVersion: "1.0.0",
                                  transportObservationCollector: collector)
        let client = try SynchroClient(config: config)
        let manager = SchemaManager(database: client.database)
        var source = protocolOrdersSchemaManifest()
        source.schemaHash = try Integrity.schemaManifestHash(source)
        _ = try manager.prepareMigration(targetManifest: source, action: .replace, affectedScopes: [],
                                         scopeCursorUpdates: [:], schemaReset: false)
        let fresh = try SynchroInspection(client: client).captureState(maximumRecords: 32)
        XCTAssertEqual(fresh.migrationJournal?.source, SchemaRef(version: 0, hash: ""))
        XCTAssertEqual(fresh.physicalSchema, [])
        XCTAssertFalse(fresh.physicalSchemaTruncated)
        let initialJournal = try XCTUnwrap(fresh.migrationJournal)
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET migration_plan_version = 'invalid'") }
        XCTAssertThrowsError(try SynchroInspection(client: client).captureState(maximumRecords: 32)) { error in
            guard case SynchroError.invalidResponse = error else { return XCTFail("Expected invalid response") }
        }
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET migration_plan_version = ?", arguments: [initialJournal.stored["migration_plan_version"]!]) }
        for (column, version) in [("source_schema_version", initialJournal.source.version), ("target_schema_version", initialJournal.target.version)] {
            try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET \(column) = ?", arguments: [String(repeating: "x", count: 4_194_304)]) }
            let omitted = try SynchroInspection(client: client).captureState(maximumRecords: 32)
            XCTAssertNil(omitted.migrationJournal)
            XCTAssertTrue(omitted.migrationJournalTruncated)
            try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET \(column) = ?", arguments: [version]) }
        }
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET target_manifest_json = ?", arguments: [Data([0xff])]) }
        XCTAssertThrowsError(try SynchroInspection(client: client).captureState(maximumRecords: 32)) { error in
            guard case SynchroError.invalidResponse = error else { return XCTFail("Expected invalid response") }
        }
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET target_manifest_json = ?", arguments: [initialJournal.stored["target_manifest_json"]!]) }
        try client.database.writeSchemaMigrationTransaction { _ = try manager.applyPreparedMigrationInTransaction($0) }
        try manager.finishAppliedMigrationIfPossible()
        _ = try client.database.execute("CREATE TABLE local_settings (value TEXT)", params: nil)
        _ = try client.database.execute("INSERT INTO local_settings VALUES ('sentinel')", params: nil)
        _ = try client.database.execute("INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES ('o1', 'first', 'u1', '2026-01-01T00:00:00.000000Z')", params: nil)
        _ = try client.database.execute("UPDATE orders SET ship_address = 'later' WHERE id = 'o1'", params: nil)
        var target = protocolOrdersSchemaManifest(includeNotes: true, schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: source.schemaHash), transitionClass: "class_2")
        target.schemaHash = try Integrity.schemaManifestHash(target)
        let tracker = ChangeTracker(database: client.database)
        let engine = SyncEngine(config: config, database: client.database, httpClient: HttpClient(config: config),
            schemaManager: manager, changeTracker: tracker, pullProcessor: PullProcessor(database: client.database),
            pushProcessor: PushProcessor(database: client.database, changeTracker: tracker))
        let response = ConnectResponse(serverTime: "2026-01-01T00:00:00.000000Z", protocolVersion: 3,
            clientGeneration: 1, scopeSetVersion: 0,
            schema: SchemaDescriptor(version: 2, hash: target.schemaHash, action: .replace, reason: nil),
            scopes: ScopeAssignmentDelta(add: [], remove: []), scopeCursorUpdates: [:], schemaDefinition: target)
        try collector.armPause(for: MigrationCheckpoint.prepared)
        let installation = Task { try await engine.installConnectedState(response) }
        defer { collector.cancelPauseBarrier(); installation.cancel() }
        try await collector.awaitPause(for: MigrationCheckpoint.prepared, timeout: 2)
        XCTAssertTrue(collector.isMigrationCheckpointPaused)
        let before = try client.database.query("SELECT * FROM _synchro_schema_migration", params: nil)
        let inspection = SynchroInspection(client: client)
        let prepared = try inspection.captureSnapshot(maximumRecords: 32) { _, transaction in
            XCTAssertEqual(try transaction.queryOne("SELECT value FROM local_settings")?["value"] as String?, "sentinel")
            XCTAssertThrowsError(try transaction.execute("UPDATE local_settings SET value = 'changed'"))
        }
        let journal = try XCTUnwrap(prepared.capture.migrationJournal)
        XCTAssertEqual(journal.source, SchemaRef(version: 1, hash: source.schemaHash))
        XCTAssertEqual(journal.target, SchemaRef(version: 2, hash: target.schemaHash))
        XCTAssertEqual(journal.phase, "prepared")
        XCTAssertEqual(journal.stored["migration_plan_json"], before.first?["migration_plan_json"] as String?)
        XCTAssertEqual(journal.stored["target_manifest_json"], before.first?["target_manifest_json"] as String?)
        XCTAssertEqual(journal.stored["journal_version"], "1")
        XCTAssertFalse(prepared.capture.physicalSchema.contains { $0.name == "notes" })
        XCTAssertFalse(prepared.capture.physicalSchema.contains { $0.tableName == "local_settings" })
        XCTAssertEqual(prepared.capture.mutationLedgerCount, 2)
        XCTAssertEqual(try XCTUnwrap(prepared.retainedMutations).currentRecords().map(\.status), [.pending, .pending])
        XCTAssertEqual(try inspection.captureSnapshot(maximumRecords: 32) { _, _ in }, prepared)
        XCTAssertEqual(try client.database.query("SELECT * FROM _synchro_schema_migration", params: nil), before)
        try collector.armPause(for: MigrationCheckpoint.committed)
        try collector.resumePause()
        try await collector.awaitPause(for: MigrationCheckpoint.committed, timeout: 2)
        XCTAssertTrue(collector.isMigrationCheckpointPaused)
        let committed = try inspection.captureSnapshot(maximumRecords: 32) { _, _ in }
        XCTAssertEqual(committed.capture.migrationJournal?.phase, "applied")
        XCTAssertEqual(committed.capture.schema, journal.target)
        XCTAssertTrue(committed.capture.physicalSchema.contains { $0.name == "notes" && !$0.notNull })
        XCTAssertEqual(committed.retainedMutations, prepared.retainedMutations)
        let bounded = try inspection.captureState(maximumRecords: 0)
        XCTAssertTrue(bounded.migrationJournalTruncated)
        XCTAssertTrue(bounded.physicalSchemaTruncated)
        XCTAssertTrue(bounded.overflowed)
        XCTAssertNil(bounded.migrationJournal)
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET migration_plan_json = ?", arguments: [String(repeating: "x", count: 4_194_304)]) }
        let oversized = try inspection.captureState(maximumRecords: 32)
        XCTAssertTrue(oversized.migrationJournalTruncated)
        XCTAssertNil(oversized.migrationJournal)
        XCTAssertTrue(oversized.physicalSchemaTruncated)
        XCTAssertEqual(oversized.physicalSchema, [])
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_schema_migration SET migration_plan_json = ?", arguments: [journal.stored["migration_plan_json"]!]) }
        try collector.resumePause()
        try await installation.value
        XCTAssertFalse(collector.isMigrationCheckpointPaused)
        try await client.close()
        removeDatabase(at: path)
    }

    func testCaptureRejectsMaximumIntegerRecordLimitWithoutCallingRowReader() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let inspection = SynchroInspection(client: client)
        XCTAssertThrowsError(try inspection.captureState(maximumRecords: Int.max)) { error in
            guard case SynchroError.invalidResponse = error else { return XCTFail("Expected invalid response") }
        }
        XCTAssertThrowsError(try inspection.captureSnapshot(maximumRecords: Int.max) { _, _ in
            XCTFail("Invalid limit must not call the row reader")
        }) { error in
            guard case SynchroError.invalidResponse = error else { return XCTFail("Expected invalid response") }
        }
        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    func testMigrationPhysicalCaptureDetectsPrematureTargetAndLeftoverSourceTables() async throws {
        let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("migration_union_\(UUID().uuidString).sqlite")
        let client = try SynchroClient(config: SynchroConfig(dbPath: path, serverURL: URL(string: "http://test.local")!,
            authProvider: { "token" }, clientID: "inspection", appVersion: "1.0.0"))
        let manager = SchemaManager(database: client.database)
        var source = protocolOrdersSchemaManifest()
        var retired = source.tables[0]
        retired.tableID = "table-retired"
        retired.relationID = "relation-retired"
        retired.name = "retired"
        source.tables.append(retired)
        source.schemaHash = try Integrity.schemaManifestHash(source)
        _ = try manager.prepareMigration(targetManifest: source, action: .replace, affectedScopes: [],
            scopeCursorUpdates: [:], schemaReset: false)
        try client.database.writeSchemaMigrationTransaction { _ = try manager.applyPreparedMigrationInTransaction($0) }
        try manager.finishAppliedMigrationIfPossible()
        _ = try client.database.execute("CREATE TABLE unrelated_local (id TEXT)", params: nil)
        var target = protocolOrdersSchemaManifest(schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: source.schemaHash), transitionClass: "class_4", compatibilityFloor: 2)
        var added = target.tables[0]
        added.tableID = "table-added"
        added.relationID = "relation-added"
        added.name = "added"
        target.tables.append(added)
        target.schemaHash = try Integrity.schemaManifestHash(target)
        _ = try manager.prepareMigration(targetManifest: target, action: .replace, affectedScopes: [],
            scopeCursorUpdates: [:], schemaReset: true)
        let inspection = SynchroInspection(client: client)
        let prepared = try inspection.captureState(maximumRecords: 32)
        XCTAssertFalse(prepared.physicalSchemaTruncated)
        XCTAssertTrue(prepared.physicalSchema.contains { $0.tableName == "retired" })
        XCTAssertFalse(prepared.physicalSchema.contains { $0.tableName == "added" || $0.tableName == "unrelated_local" })
        try client.database.writeTransaction { try $0.execute(sql: "CREATE TABLE added (id TEXT)") }
        let premature = try inspection.captureState(maximumRecords: 32)
        XCTAssertTrue(premature.physicalSchema.contains { $0.tableName == "added" })
        XCTAssertFalse(premature.physicalSchema.contains { $0.tableName == "unrelated_local" })
        try client.database.writeTransaction { try $0.execute(sql: "DROP TABLE added") }
        let archived = try XCTUnwrap(client.database.queryOne("SELECT schema_json FROM _synchro_schema_archive WHERE schema_version = 1", params: nil)?["schema_json"] as String?)
        try client.database.writeTransaction { db in
            try db.execute(sql: "UPDATE _synchro_schema_archive SET schema_hash = ? WHERE schema_version = 1", arguments: [String(repeating: "c", count: 64)])
        }
        XCTAssertThrowsError(try inspection.captureState(maximumRecords: 32))
        try client.database.writeTransaction { db in
            try db.execute(sql: "UPDATE _synchro_schema_archive SET schema_hash = ? WHERE schema_version = 1", arguments: [source.schemaHash])
            try db.execute(sql: "UPDATE _synchro_schema_migration SET target_schema_hash = ?", arguments: [String(repeating: "d", count: 64)])
        }
        XCTAssertThrowsError(try inspection.captureState(maximumRecords: 32))
        try client.database.writeTransaction { db in
            try db.execute(sql: "UPDATE _synchro_schema_migration SET target_schema_hash = ?", arguments: [target.schemaHash])
        }
        try client.database.writeTransaction { db in
            try db.execute(sql: "UPDATE _synchro_schema_archive SET schema_json = '{}' WHERE schema_version = 1")
        }
        XCTAssertThrowsError(try inspection.captureState(maximumRecords: 32))
        try client.database.writeTransaction { db in
            try db.execute(sql: "UPDATE _synchro_schema_archive SET schema_json = ? WHERE schema_version = 1", arguments: [archived + String(repeating: " ", count: 4_194_304)])
        }
        let oversized = try inspection.captureState(maximumRecords: 32)
        XCTAssertNotNil(oversized.migrationJournal)
        XCTAssertTrue(oversized.physicalSchemaTruncated)
        XCTAssertEqual(oversized.physicalSchema, [])
        try client.database.writeTransaction { db in
            try db.execute(sql: "UPDATE _synchro_schema_archive SET schema_json = ? WHERE schema_version = 1", arguments: [archived])
        }
        try client.database.writeSchemaMigrationTransaction { _ = try manager.applyPreparedMigrationInTransaction($0) }
        let committed = try inspection.captureState(maximumRecords: 32)
        XCTAssertFalse(committed.physicalSchemaTruncated)
        XCTAssertTrue(committed.physicalSchema.contains { $0.tableName == "added" })
        XCTAssertFalse(committed.physicalSchema.contains { $0.tableName == "retired" || $0.tableName == "unrelated_local" })
        try client.database.writeTransaction { try $0.execute(sql: "CREATE TABLE retired (id TEXT)") }
        let leftover = try inspection.captureState(maximumRecords: 32)
        XCTAssertTrue(leftover.physicalSchema.contains { $0.tableName == "retired" })
        XCTAssertFalse(leftover.physicalSchema.contains { $0.tableName == "unrelated_local" })
        try client.database.writeTransaction { try $0.execute(sql: "DROP TABLE retired") }
        try await client.close()
        removeDatabase(at: path)
    }

    func testProvenanceMaintenanceWorkCountsCommittedRowsAndKeepsRowCountSeparate() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let database = try SynchroDatabase(path: config.dbPath)

        XCTAssertEqual(try database.stateInspectionTransaction { _, cursor in cursor }, 0)
        try database.writeTransaction { db in
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
                scopeID: "scope-a",
                tableName: "orders",
                recordID: "order-a",
                checksum: "checksum-a",
                generation: 1
            )
        }
        XCTAssertEqual(try database.stateInspectionTransaction { _, cursor in cursor }, 1)
        XCTAssertEqual(try client.inspectScopeRows().count, 1)

        try database.writeTransaction { db in
            try SynchroMeta.upsertScopeRow(
                db,
                scopeID: "scope-a",
                tableName: "orders",
                recordID: "order-a",
                checksum: "checksum-b",
                generation: 1
            )
        }
        XCTAssertEqual(try database.stateInspectionTransaction { _, cursor in cursor }, 2)
        XCTAssertEqual(try client.inspectScopeRows().count, 1)

        try database.writeTransaction { db in
            try SynchroMeta.deleteScopeRow(
                db,
                scopeID: "scope-a",
                tableName: "orders",
                recordID: "order-a"
            )
        }
        XCTAssertEqual(try database.stateInspectionTransaction { _, cursor in cursor }, 3)
        XCTAssertEqual(try client.inspectScopeRows().count, 0)

        try database.close()
        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    func testProvenanceMaintenanceWorkDoesNotAdvanceOnRollback() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let database = try SynchroDatabase(path: config.dbPath)

        XCTAssertThrowsError(try database.writeTransaction { db in
            try SynchroMeta.upsertScopeRow(
                db,
                scopeID: "scope-a",
                tableName: "orders",
                recordID: "order-a",
                checksum: "checksum-a",
                generation: 1
            )
            throw RollbackError()
        })
        XCTAssertEqual(try database.stateInspectionTransaction { _, cursor in cursor }, 0)
        XCTAssertEqual(try client.inspectScopeRows().count, 0)

        try database.close()
        try await client.close()
        removeDatabase(at: config.dbPath)
    }

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

    func testAtomicStateCaptureReturnsOrderedBoundedDurableState() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let internalDatabase = try SynchroDatabase(path: config.dbPath)
        try internalDatabase.writeTransaction { db in
            try SynchroMeta.setInt64(db, key: .schemaVersion, value: 1)
            try SynchroMeta.set(db, key: .schemaHash, value: protocolTestSchemaHash)
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

        let inspection = SynchroInspection(client: client)
        let capture = try inspection.captureState(maximumRecords: 1)
        XCTAssertEqual(capture.schema, SchemaRef(version: 1, hash: protocolTestSchemaHash))
        XCTAssertEqual(capture.scopeStates.map(\.scopeID), ["scope-a"])
        XCTAssertEqual(capture.scopeRows.map(\.recordID), ["o1"])
        XCTAssertEqual(capture.rebuildAttempts.map(\.rebuildID), ["rebuild-a"])
        XCTAssertTrue(capture.scopeStatesTruncated)
        XCTAssertTrue(capture.scopeRowsTruncated)
        XCTAssertFalse(capture.rebuildAttemptsTruncated)
        XCTAssertFalse(capture.rebuildReceiptsTruncated)
        XCTAssertFalse(capture.rowMetadataTruncated)
        XCTAssertTrue(capture.overflowed)
        XCTAssertEqual(capture.provenanceMaintenanceWorkCursor, 0)

        XCTAssertEqual(capture.applicationRowCount, 0)
        XCTAssertEqual(capture.mutationLedgerCount, 0)
        XCTAssertEqual(capture.mutationOutcomeCount, 0)
        XCTAssertEqual(capture.sealedBatchCount, 0)
        XCTAssertEqual(capture.rejectedMutationCount, 0)
        XCTAssertEqual(capture.scopeStateCount, 2)
        XCTAssertEqual(capture.scopeRowCount, 2)
        XCTAssertEqual(capture.provenanceCount, 2)
        XCTAssertEqual(capture.rowMetadataCount, 1)
        XCTAssertEqual(capture.rowMetadata.map(\.recordID), ["o1"])
        XCTAssertEqual(capture.rebuildAttemptCount, 1)
        XCTAssertEqual(capture.rebuildReceiptCount, 0)

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

    func testAtomicStateCaptureIncludesUnscopedRowMetadataAndReportsOverflow() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let database = try SynchroDatabase(path: config.dbPath)
        try database.writeTransaction { db in
            try SynchroMeta.upsertRowVersion(
                db,
                tableName: "orders",
                recordID: "unscoped-a",
                serverVersion: "version-a",
                rowChecksum: nil
            )
            try SynchroMeta.upsertRowVersion(
                db,
                tableName: "orders",
                recordID: "unscoped-b",
                serverVersion: "version-b",
                rowChecksum: nil
            )
        }
        try database.close()

        let inspection = SynchroInspection(client: client)
        let bounded = try inspection.captureState(maximumRecords: 1)
        XCTAssertEqual(bounded.scopeRows, [])
        XCTAssertEqual(bounded.rowMetadataCount, 2)
        XCTAssertEqual(bounded.rowMetadata.map(\.recordID), ["unscoped-a"])
        XCTAssertTrue(bounded.rowMetadataTruncated)
        XCTAssertTrue(bounded.overflowed)

        let complete = try inspection.captureState(maximumRecords: 4)
        XCTAssertEqual(complete.scopeRows, [])
        XCTAssertEqual(complete.rowMetadata.map(\.recordID), ["unscoped-a", "unscoped-b"])
        XCTAssertFalse(complete.rowMetadataTruncated)
        XCTAssertFalse(complete.overflowed)

        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    func testAtomicStateCaptureRejectsMixedCommittedGenerations() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        _ = try client.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            params: ["o1", "authored", "2026-01-01T00:00:00.000000Z"]
        )
        let rejection = try rejectionFixture(client)
        let database = try SynchroDatabase(path: config.dbPath)
        let inspection = SynchroInspection(client: client)
        let writerStarted = expectation(description: "writer started")
        let writer = Task.detached { [database] in
            writerStarted.fulfill()
            for generation in 1...200 {
                try database.writeSyncLockedTransaction { db in
                    try db.execute(sql: "DELETE FROM _synchro_rebuild_attempts")
                    try SynchroMeta.clearAllScopeRows(db)
                    try SynchroMeta.clearAllScopes(db)
                    try SynchroMeta.clearRejectedMutations(db)
                    guard generation.isMultiple(of: 2) else {
                        try db.execute(
                            sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'unsealed' WHERE mutation_id = ?",
                            arguments: [rejection.mutationID]
                        )
                        try db.execute(sql: "UPDATE orders SET title = 'odd' WHERE id = 'o1'")
                        return
                    }
                    let value = Int64(generation)
                    try db.execute(
                        sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'rejected' WHERE mutation_id = ?",
                        arguments: [rejection.mutationID]
                    )
                    try SynchroMeta.upsertRejectedMutation(
                        db,
                        mutationID: rejection.mutationID,
                        tableName: "orders",
                        recordID: "o1",
                        status: "rejected_terminal",
                        code: "policy_rejected",
                        message: "gen-\(generation)",
                        serverRow: nil,
                        serverVersion: nil,
                        mutationJSON: rejection.mutationJSON,
                        rejectedJSON: rejection.rejectionJSON
                    )
                    try db.execute(sql: "UPDATE orders SET title = ? WHERE id = 'o1'", arguments: ["gen-\(generation)"])
                    try SynchroMeta.upsertScope(
                        db,
                        scopeID: "scope",
                        cursor: "cursor-\(generation)",
                        checksum: "checksum-\(generation)",
                        generation: value,
                        localChecksum: "local-\(generation)"
                    )
                    try SynchroMeta.upsertScopeRow(
                        db,
                        scopeID: "scope",
                        tableName: "orders",
                        recordID: "order-\(generation)",
                        checksum: "row-\(generation)",
                        generation: value
                    )
                    try SynchroMeta.upsertRebuildAttempt(
                        db,
                        attempt: LocalRebuildAttempt(
                            scopeID: "scope",
                            rebuildID: "rebuild-\(generation)",
                            clientGeneration: value,
                            schemaVersion: 1,
                            schemaHash: protocolTestSchemaHash,
                            generation: value,
                            cursor: "cursor-\(generation)",
                            pageLimit: 100
                        )
                    )
                }
                await Task.yield()
            }
        }
        await fulfillment(of: [writerStarted], timeout: 1)

        for _ in 1...200 {
            let capture = try inspection.captureState(maximumRecords: 4)
            try assertOneGeneration(capture)

            var title: String?
            let snapshot = try inspection.captureSnapshot(maximumRecords: 4) { _, transaction in
                title = try transaction.queryOne("SELECT title FROM orders WHERE id = 'o1'")?["title"]
            }
            try assertOneGeneration(snapshot.capture)
            let rejected = try XCTUnwrap(snapshot.rejectedMutations).currentRecords()
            let retained = try XCTUnwrap(snapshot.retainedMutations)
            XCTAssertEqual(rejected.count, snapshot.capture.rejectedMutationCount)
            XCTAssertEqual(retained.map(\.mutationID), [rejection.mutationID])
            if let scope = snapshot.capture.scopeStates.first {
                let generation = "gen-\(scope.generation)"
                XCTAssertEqual(rejected.map(\.message), [generation])
                XCTAssertEqual(retained.map(\.status), [.serverRejected])
                XCTAssertEqual(snapshot.pendingChangeCount, 0)
                XCTAssertEqual(title, generation)
            } else {
                XCTAssertEqual(rejected, [])
                XCTAssertEqual(retained.map(\.status), [.pending])
                XCTAssertEqual(snapshot.pendingChangeCount, 1)
                XCTAssertTrue(title == "odd" || title == "authored")
            }
            await Task.yield()
        }
        try await writer.value

        try database.close()
        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    /// Commits a transition directly after the first transaction of the snapshot ends.
    /// The client writer runs queued work in order. The commit of that transaction queues the
    /// transition, so a snapshot split into a second transaction reads the transition.
    func testSnapshotKeepsOneStateWhenATransitionFollowsItsFirstTransaction() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        _ = try client.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            params: ["o1", "before", "2026-01-01T00:00:00.000000Z"]
        )
        let acceptedID = try XCTUnwrap(client.inspectRetainedMutations().first?.mutationID)
        _ = try client.execute("UPDATE orders SET title = 'updated-before' WHERE id = 'o1'")
        let beforeOutcome = " { \"mutation_id\": \"\(acceptedID)\", \"marker\": \"before\" }\n"
        let afterOutcome = beforeOutcome + " \n"
        try client.database.writeTransaction { try $0.execute(sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted', accepted_json = ? WHERE mutation_id = ?", arguments: [beforeOutcome, acceptedID]) }
        let inspection = SynchroInspection(client: client)
        let transition = expectation(description: "transition committed")
        let trigger = TransitionAfterCommit(pool: client.database.dbPool, committed: transition, acceptedOutcomeAfter: afterOutcome)
        client.database.dbPool.add(transactionObserver: trigger, extent: .nextTransaction)

        var rows: [String] = []
        let snapshot = try inspection.captureSnapshot(maximumRecords: 8) { _, transaction in
            rows = try transaction.query("SELECT id FROM orders ORDER BY id").map { $0["id"] }
        }
        await fulfillment(of: [transition], timeout: 30)

        XCTAssertEqual(rows, ["o1"])
        XCTAssertEqual(snapshot.capture.applicationRowCount, 1)
        XCTAssertEqual(snapshot.capture.scopeStateCount, 0)
        XCTAssertEqual(snapshot.capture.scopeStates, [])
        XCTAssertEqual(snapshot.capture.acceptedMutationOutcomes, [acceptedID: beforeOutcome])
        XCTAssertEqual(try XCTUnwrap(snapshot.retainedMutations).map(\.recordID), ["o1"])

        var laterRows: [String] = []
        let later = try inspection.captureSnapshot(maximumRecords: 8) { _, transaction in
            laterRows = try transaction.query("SELECT id FROM orders ORDER BY id").map { $0["id"] }
        }
        XCTAssertEqual(laterRows, ["o1", "o2"])
        XCTAssertEqual(later.capture.applicationRowCount, 2)
        XCTAssertEqual(later.capture.scopeStates.map(\.scopeID), ["probe-scope"])
        XCTAssertEqual(later.capture.acceptedMutationOutcomes, [acceptedID: afterOutcome])

        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    private func assertOneGeneration(_ capture: ClientStateCaptureInspection) throws {
        XCTAssertFalse(capture.overflowed)
        XCTAssertEqual(capture.scopeStateCount, capture.scopeStates.count)
        XCTAssertEqual(capture.scopeRowCount, capture.scopeRows.count)
        XCTAssertEqual(capture.rebuildAttemptCount, capture.rebuildAttempts.count)
        if let scope = capture.scopeStates.first {
            let row = try XCTUnwrap(capture.scopeRows.first)
            let attempt = try XCTUnwrap(capture.rebuildAttempts.first)
            XCTAssertEqual(scope.generation, row.generation)
            XCTAssertEqual(scope.generation, attempt.generation)
            XCTAssertEqual(scope.cursor, row.recordID.replacingOccurrences(of: "order-", with: "cursor-"))
            XCTAssertEqual(scope.cursor, attempt.cursor)
        }
    }

    private func rejectionFixture(
        _ client: SynchroClient
    ) throws -> (mutationID: String, mutationJSON: String, rejectionJSON: String) {
        let pending = try XCTUnwrap(client.inspectPendingMutations().first)
        let mutation = Mutation(
            mutationID: pending.mutationID,
            table: pending.tableID,
            op: pending.operation,
            pk: [pending.primaryKeyFieldID: AnyCodable(pending.recordID)],
            authoredSchema: pending.authoredSchema,
            baseVersion: pending.baseVersion,
            clientVersion: pending.clientVersion,
            columns: Dictionary(uniqueKeysWithValues: pending.authoredFields.map { ($0.fieldID, $0.value) })
        )
        let rejection = RejectedMutation(
            mutationID: pending.mutationID,
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
        return (
            pending.mutationID,
            try XCTUnwrap(String(data: JSONEncoder.synchroEncoder().encode(mutation), encoding: .utf8)),
            try XCTUnwrap(String(data: JSONEncoder.synchroEncoder().encode(rejection), encoding: .utf8))
        )
    }

    func testSnapshotReportsNormalizedLedgerAfterPendingCount() async throws {
        let config = try prepareClientConfig()
        let client = try SynchroClient(config: config)
        let inspection = SynchroInspection(client: client)
        _ = try client.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            params: ["o1", "inserted", "2026-01-01T00:00:00.000000Z"]
        )
        _ = try client.execute("UPDATE orders SET title = ? WHERE id = ?", params: ["updated", "o1"])

        XCTAssertEqual(try client.inspectRetainedMutations().map(\.status), [.pending, .pending])
        XCTAssertEqual(try inspection.captureState(maximumRecords: 8).mutationLedgerCount, 2)

        XCTAssertEqual(try client.pendingChangeCount(), 1)
        var title: String?
        let snapshot = try inspection.captureSnapshot(maximumRecords: 8) { _, transaction in
            title = try transaction.queryOne("SELECT title FROM orders WHERE id = 'o1'")?["title"]
            XCTAssertThrowsError(try transaction.execute("UPDATE orders SET title = 'snapshot write' WHERE id = 'o1'"))
        }

        XCTAssertEqual(title, "updated")
        XCTAssertEqual(snapshot.capture.mutationLedgerCount, 3)
        let retained = try XCTUnwrap(snapshot.retainedMutations).currentRecords()
        XCTAssertEqual(retained.map(\.status), [.supersededBeforeSend, .supersededBeforeSend, .pending])
        XCTAssertEqual(retained.map(\.sourceKind).last, "normalized")
        XCTAssertEqual(retained.map(\.operation).last, .insert)
        XCTAssertEqual(
            retained.last?.authoredFields.first { $0.fieldID == "title" }?.value,
            AnyCodable("updated")
        )
        XCTAssertEqual(retained.dropLast().map(\.normalizedMutationID), [retained[2].mutationID, retained[2].mutationID])
        XCTAssertEqual(snapshot.pendingChangeCount, 1)
        XCTAssertEqual(snapshot.rejectedMutations, [])
        XCTAssertNil(snapshot.blockingFailure)

        let repeated = try inspection.captureSnapshot(maximumRecords: 8) { _, _ in }
        XCTAssertEqual(repeated, snapshot)
        XCTAssertEqual(try client.queryOne("SELECT title FROM orders WHERE id = 'o1'")?["title"], "updated")

        let bounded = try inspection.captureSnapshot(maximumRecords: 2) { _, _ in }
        XCTAssertNil(bounded.retainedMutations)
        XCTAssertEqual(bounded.rejectedMutations, [])

        try await client.close()
        removeDatabase(at: config.dbPath)
    }

    func testCaptureBoundsRebuildReceiptGroupsNotPages() async throws {
        let fixture = try makeRebuildReceiptFixture()
        let capture = try SynchroInspection(client: fixture.client).captureState(maximumRecords: 1)

        XCTAssertEqual(capture.rebuildReceiptCount, 2)
        XCTAssertEqual(capture.rebuildReceipts.map(\.pageCount), [2])
        XCTAssertFalse(capture.rebuildReceiptsTruncated)
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptStructuralFactsForValidTwoPageReceipts() async throws {
        let fixture = try makeRebuildReceiptFixture()
        let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)

        XCTAssertEqual(receipt.rebuildIDFingerprint, TransportObservationCollector.cursorFingerprint("proof-rebuild"))
        XCTAssertEqual(receipt.pageCount, 2)
        XCTAssertEqual(receipt.returnedRecordCount, 3)
        XCTAssertEqual(receipt.requestChainExpected, receipt.requestChainObserved)
        XCTAssertEqual(receipt.recordIdentitiesHex, receipt.recordIdentitiesHex.sorted())
        XCTAssertEqual(receipt.recordIdentitiesHex.count, Set(receipt.recordIdentitiesHex).count)
        XCTAssertEqual(receipt.receivedRowChecksums, receipt.computedRowChecksums)
        XCTAssertNotNil(receipt.computedScopeChecksum)
        XCTAssertEqual(receipt.computedScopeChecksum, receipt.finalScopeChecksum)
        XCTAssertEqual(receipt.finalScopeChecksum, receipt.storedScopeChecksum)
        XCTAssertEqual(receipt.finalScopeChecksum, receipt.localScopeChecksum)
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptStructuralFactsAcceptExplicitNullWireMembers() async throws {
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

        let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)
        XCTAssertEqual(receipt.requestChainExpected, receipt.requestChainObserved)
        XCTAssertEqual(receipt.receivedRowChecksums, receipt.computedRowChecksums)
        XCTAssertNotNil(receipt.computedScopeChecksum)
        XCTAssertEqual(receipt.computedScopeChecksum, receipt.finalScopeChecksum)
        XCTAssertEqual(receipt.finalScopeChecksum, receipt.storedScopeChecksum)
        XCTAssertEqual(receipt.finalScopeChecksum, receipt.localScopeChecksum)
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptStructuralFactsRejectUnknownWireMember() async throws {
        let fixture = try makeRebuildReceiptFixture()
        try updateReceiptJSON(fixture.database, column: "response_json", requestCursor: nil) {
            $0["unexpected"] = true
        }
        XCTAssertThrowsError(try fixture.client.inspectRebuildReceipts())
        try await closeRebuildReceiptFixture(fixture)
    }

    func testRebuildReceiptStructuralFactsControlForgedChecksums() async throws {
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: nil) { response in
                response.records[0].rowChecksum.digest = String(repeating: "f", count: 64)
            }
            let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)
            XCTAssertEqual(receipt.requestChainExpected, receipt.requestChainObserved)
            XCTAssertEqual(receipt.recordIdentitiesHex, receipt.recordIdentitiesHex.sorted())
            XCTAssertNotEqual(receipt.receivedRowChecksums, receipt.computedRowChecksums)
            XCTAssertEqual(receipt.computedScopeChecksum, receipt.finalScopeChecksum)
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
            let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)
            XCTAssertEqual(receipt.requestChainExpected, receipt.requestChainObserved)
            XCTAssertEqual(receipt.receivedRowChecksums, receipt.computedRowChecksums)
            XCTAssertNotEqual(receipt.computedScopeChecksum, receipt.finalScopeChecksum)
            XCTAssertNotEqual(receipt.finalScopeChecksum, receipt.storedScopeChecksum)
            try await closeRebuildReceiptFixture(fixture)
        }
    }

    func testRebuildReceiptStructuralFactsControlOrderAndCursorChain() async throws {
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: nil) { response in
                response.records.reverse()
            }
            let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)
            XCTAssertEqual(receipt.requestChainExpected, receipt.requestChainObserved)
            XCTAssertNotEqual(receipt.recordIdentitiesHex, receipt.recordIdentitiesHex.sorted())
            XCTAssertEqual(receipt.receivedRowChecksums, receipt.computedRowChecksums)
            try await closeRebuildReceiptFixture(fixture)
        }
        do {
            let fixture = try makeRebuildReceiptFixture()
            try updateReceipt(fixture.database, requestCursor: nil) { response in
                response.cursor = "unconsumed"
            }
            let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)
            XCTAssertNotEqual(receipt.requestChainExpected, receipt.requestChainObserved)
            XCTAssertEqual(receipt.receivedRowChecksums, receipt.computedRowChecksums)
            try await closeRebuildReceiptFixture(fixture)
        }
    }

    func testRebuildReceiptStructuralFactsControlExtraUnconsumedReceipt() async throws {
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
        let receipt = try XCTUnwrap(fixture.client.inspectRebuildReceipts().first)
        XCTAssertEqual(receipt.pageCount, 3)
        XCTAssertNotEqual(receipt.requestChainExpected, receipt.requestChainObserved)
        XCTAssertEqual(receipt.returnedRecordCount, 3)
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

/// Queues one sync-locked transition on the writer when the observed transaction commits.
private final class TransitionAfterCommit: TransactionObserver, @unchecked Sendable {
    private let pool: DatabasePool
    private let committed: XCTestExpectation
    private let acceptedOutcomeAfter: String

    init(pool: DatabasePool, committed: XCTestExpectation, acceptedOutcomeAfter: String) {
        self.pool = pool
        self.committed = committed
        self.acceptedOutcomeAfter = acceptedOutcomeAfter
    }

    func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool { false }
    func databaseDidChange(with event: DatabaseEvent) {}
    func databaseDidRollback(_ db: Database) {}

    func databaseDidCommit(_ db: Database) {
        pool.asyncWrite({ db in
            try SynchroMeta.setSyncLock(db, locked: true)
            try db.execute(sql: "UPDATE _synchro_pending_changes SET accepted_json = ? WHERE lifecycle_state = 'accepted'", arguments: [self.acceptedOutcomeAfter])
            try db.execute(sql: "INSERT INTO orders (id, title, updated_at) VALUES ('o2', 'after', '2026-01-02T00:00:00.000000Z')")
            try SynchroMeta.upsertScope(db, scopeID: "probe-scope", cursor: nil, checksum: nil, generation: 1, localChecksum: "")
            try SynchroMeta.setSyncLock(db, locked: false)
        }, completion: { [committed] _, result in
            if case .failure(let error) = result { XCTFail("transition failed: \(error)") }
            committed.fulfill()
        })
    }
}
