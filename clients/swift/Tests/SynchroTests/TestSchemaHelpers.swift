import Foundation
@testable import Synchro

let protocolTestSchemaHash = try! Integrity.schemaManifestHash(protocolOrdersSchemaManifest())

func protocolOrdersSchemaManifest(
    includeNotes: Bool = false,
    schemaVersion: Int64 = 1,
    parentSchema: SchemaRef? = nil,
    transitionClass: String = "initial",
    compatibilityFloor: Int64 = 1
) -> SchemaManifest {
    var fields = [
        ColumnSchema(fieldID: "field-id", name: "id", type: "string", nullable: false, writable: false, precision: nil, scale: nil),
        ColumnSchema(fieldID: "field-ship-address", name: "ship_address", type: "string", nullable: true, writable: true, precision: nil, scale: nil),
        ColumnSchema(fieldID: "field-user-id", name: "user_id", type: "string", nullable: false, writable: true, precision: nil, scale: nil),
        ColumnSchema(fieldID: "field-updated-at", name: "updated_at", type: "datetime", nullable: false, writable: false, precision: nil, scale: nil),
        ColumnSchema(fieldID: "field-deleted-at", name: "deleted_at", type: "datetime", nullable: true, writable: false, precision: nil, scale: nil),
    ]
    if includeNotes {
        fields.append(ColumnSchema(
            fieldID: "field-notes",
            name: "notes",
            type: "string",
            nullable: true,
            writable: true,
            precision: nil,
            scale: nil
        ))
    }
    return SchemaManifest(
        schemaVersion: schemaVersion,
        schemaHash: String(repeating: "0", count: 64),
        parentSchema: parentSchema,
        transitionClass: transitionClass,
        compatibilityFloor: compatibilityFloor,
        tables: [
            TableSchema(
                tableID: "table-orders",
                relationID: "relation-orders",
                name: "orders",
                primaryKeyFieldID: "field-id",
                lifecycle: LifecycleSchema(
                    createdAtFieldID: nil,
                    updatedAtFieldID: "field-updated-at",
                    deletedAtFieldID: "field-deleted-at"
                ),
                composition: .singleScope,
                fields: fields,
                indexes: []
            )
        ]
    )
}

typealias SchemaColumn = LocalSchemaColumn
typealias SchemaTable = LocalSchemaTable

extension ColumnSchema {
    init(name: String, type: String, nullable: Bool) {
        self.init(
            fieldID: name,
            name: name,
            type: type,
            nullable: nullable,
            writable: name != "id" && name != "created_at" && name != "updated_at" && name != "deleted_at",
            precision: nil,
            scale: nil
        )
    }
}

extension TableSchema {
    init(
        name: String,
        primaryKey: [String],
        updatedAtColumn: String,
        deletedAtColumn: String,
        composition: CompositionClass,
        columns: [ColumnSchema],
        indexes: [IndexSchema]?
    ) {
        let fieldsByName = Dictionary(uniqueKeysWithValues: columns.map { ($0.name, $0.fieldID) })
        self.init(
            tableID: name,
            relationID: "relation-\(name)",
            name: name,
            primaryKeyFieldID: fieldsByName[primaryKey[0]]!,
            lifecycle: LifecycleSchema(
                createdAtFieldID: fieldsByName["created_at"],
                updatedAtFieldID: fieldsByName[updatedAtColumn],
                deletedAtFieldID: fieldsByName[deletedAtColumn]
            ),
            composition: composition,
            fields: columns,
            indexes: indexes ?? []
        )
    }
}

extension SchemaManifest {
    init(tables: [TableSchema]) {
        self.init(
            schemaVersion: 1,
            schemaHash: String(repeating: "0", count: 64),
            parentSchema: nil,
            transitionClass: "initial",
            compatibilityFloor: 1,
            tables: tables
        )
    }
}

extension LocalSchemaColumn {
    init(
        name: String,
        dbType: String = "text",
        logicalType: String = "string",
        nullable: Bool = true,
        precision: Int? = nil,
        scale: Int? = nil,
        defaultSQL: String? = nil,
        defaultKind: String = "none",
        sqliteDefaultSQL: String? = nil,
        isPrimaryKey: Bool = false
    ) {
        let _ = dbType
        let _ = defaultKind
        self.init(
            fieldID: name,
            name: name,
            logicalType: logicalType,
            nullable: nullable,
            writable: !isPrimaryKey && name != "created_at" && name != "updated_at" && name != "deleted_at",
            precision: precision,
            scale: scale,
            sqliteDefaultSQL: sqliteDefaultSQL ?? defaultSQL,
            isPrimaryKey: isPrimaryKey
        )
    }
}

extension LocalSchemaTable {
    init(
        tableName: String,
        pushPolicy: String = "owner_only",
        parentTable: String? = nil,
        parentFKCol: String? = nil,
        dependencies: [String]? = nil,
        updatedAtColumn: String,
        deletedAtColumn: String,
        composition: CompositionClass? = .singleScope,
        primaryKey: [String],
        columns: [LocalSchemaColumn]
    ) {
        let _ = pushPolicy
        let _ = parentTable
        let _ = parentFKCol
        let _ = dependencies
        self.init(
            tableID: tableName,
            relationID: "relation-\(tableName)",
            tableName: tableName,
            primaryKeyFieldID: primaryKey[0],
            createdAtFieldID: columns.first(where: { $0.name == "created_at" })?.fieldID,
            updatedAtFieldID: columns.first(where: { $0.name == updatedAtColumn })?.fieldID,
            deletedAtFieldID: columns.first(where: { $0.name == deletedAtColumn })?.fieldID,
            updatedAtColumn: updatedAtColumn,
            deletedAtColumn: deletedAtColumn,
            composition: composition,
            primaryKey: primaryKey,
            columns: columns
        )
    }

    fileprivate var testManifestTable: TableSchema {
        let manifestColumns = columns.map { column in
            ColumnSchema(
                fieldID: column.fieldID,
                name: column.name,
                type: column.logicalType,
                nullable: column.nullable,
                writable: column.writable
                    && column.fieldID != primaryKeyFieldID
                    && column.fieldID != createdAtFieldID
                    && column.fieldID != updatedAtFieldID
                    && column.fieldID != deletedAtFieldID,
                precision: column.precision,
                scale: column.scale
            )
        }
        return TableSchema(
            name: tableName,
            primaryKey: primaryKey,
            updatedAtColumn: updatedAtColumn,
            deletedAtColumn: deletedAtColumn,
            composition: composition ?? .singleScope,
            columns: manifestColumns,
            indexes: nil
        )
    }

    var localSchema: LocalSchemaTable {
        self
    }
}

extension SchemaResponse {
    init(schemaVersion: Int64, schemaHash: String, serverTime: Date, tables: [LocalSchemaTable]) {
        self.init(
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            serverTime: serverTime,
            manifest: SchemaManifest(tables: tables.map(\.testManifestTable))
        )
    }

    var tables: [SchemaTable] {
        (try? localTables()) ?? []
    }
}

func makeAcceptedMutation(
    mutationID: String,
    schema: LocalSchemaTable,
    pk: [String: AnyCodable],
    status: MutationStatus,
    serverRow: [String: AnyCodable]?,
    serverVersion: String
) throws -> AcceptedMutation {
    let checksum = try serverRow.map {
        try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: schema,
            pk: pk,
            row: $0,
            serverVersion: serverVersion
        ).checksum
    }
    return AcceptedMutation(
        mutationID: mutationID,
        table: schema.tableID,
        pk: pk,
        outcomeSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
        status: status,
        serverRow: serverRow,
        rowChecksum: checksum,
        serverVersion: serverVersion
    )
}

func makeRejectedMutation(
    mutationID: String,
    schema: LocalSchemaTable,
    pk: [String: AnyCodable],
    status: MutationStatus,
    code: MutationRejectionCode,
    message: String,
    serverRow: [String: AnyCodable]? = nil,
    serverVersion: String? = nil,
    authoredSchema: SchemaRef? = nil,
    currentSchema: SchemaRef? = nil,
    incompatibleFieldIDs: [String]? = nil
) throws -> RejectedMutation {
    let checksum = try serverRow.flatMap { row in
        try serverVersion.map {
            try Integrity.rowDigest(
                schemaHash: protocolTestSchemaHash,
                table: schema,
                pk: pk,
                row: row,
                serverVersion: $0
            ).checksum
        }
    }
    return RejectedMutation(
        mutationID: mutationID,
        table: schema.tableID,
        pk: pk,
        outcomeSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
        status: status,
        code: code,
        message: message,
        retryable: nil,
        serverRow: serverRow,
        rowChecksum: checksum,
        serverVersion: serverVersion,
        authoredSchema: authoredSchema,
        currentSchema: currentSchema,
        incompatibleFieldIDs: incompatibleFieldIDs
    )
}

func makeChangeRecord(
    scope: String,
    schema: LocalSchemaTable,
    op: Synchro.Operation,
    pk: [String: AnyCodable],
    row: [String: AnyCodable]?,
    serverVersion: String
) throws -> ChangeRecord {
    let checksum = try row.map {
        try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: schema,
            pk: pk,
            row: $0,
            serverVersion: serverVersion
        ).checksum
    }
    return ChangeRecord(
        scope: scope,
        table: schema.tableID,
        op: op,
        pk: pk,
        row: row,
        rowChecksum: checksum,
        serverVersion: serverVersion
    )
}

func protocolEmptyScopeChecksum(scopeID: String) -> ChecksumObject {
    try! Integrity.scopeDigest(
        schemaHash: protocolTestSchemaHash,
        scopeID: scopeID,
        entries: []
    )
}
