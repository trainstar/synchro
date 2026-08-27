@_spi(Inspection)
public struct SynchroInspection: Sendable {
    private let client: SynchroClient

    public init(client: SynchroClient) {
        self.client = client
    }

    public func currentSchema() throws -> SchemaRef? {
        try client.inspectCurrentSchema()
    }

    public func scopeStates() throws -> [ScopeStateInspection] {
        try client.inspectScopeStates()
    }

    public func scopeRows() throws -> [ScopeRowInspection] {
        try client.inspectScopeRows()
    }

    public func captureState(maximumRecords: Int) throws -> ClientStateCaptureInspection {
        try client.inspectClientStateCapture(maximumRecords: maximumRecords)
    }

    public func rowMetadata(tableName: String, recordID: String) throws -> RowMetadataInspection? {
        try client.inspectRowMetadata(tableName: tableName, recordID: recordID)
    }

    public func rebuildAttempts() throws -> [RebuildAttemptInspection] {
        try client.inspectRebuildAttempts()
    }

    public func rebuildReceipts() throws -> [RebuildReceiptInspection] {
        try client.inspectRebuildReceipts()
    }
}
