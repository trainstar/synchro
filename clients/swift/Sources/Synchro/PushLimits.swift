import Foundation

/// Measures push requests with the limits that the server applies.
///
/// The server measures the request body, the RFC 8785 form of the request, and
/// the RFC 8785 form of each normalized mutation.
enum PushLimits {
    static let maxRequestOctets = 1_048_576
    static let maxNormalizedMutationOctets = 65_536
    static let maxAuthoredColumns = 256
    /// The largest client generation and schema version that the server accepts.
    static let maxProtocolInteger: Int64 = 9_007_199_254_740_991

    /// Gives the octet counts of one JSON text in the two request measures.
    struct Octets: Equatable {
        var body: Int
        var canonical: Int

        /// Gives the request octets after the request gets one more mutation element.
        ///
        /// One comma comes before each element after the first element.
        func appending(_ element: Octets, afterElement: Bool) -> Octets {
            let separator = afterElement ? 1 : 0
            return Octets(
                body: body + separator + element.body,
                canonical: canonical + separator + element.canonical
            )
        }

        var fitsRequestLimit: Bool {
            body <= PushLimits.maxRequestOctets && canonical <= PushLimits.maxRequestOctets
        }
    }

    struct MutationMeasure {
        /// The octets of the mutation as one element of the request `mutations` array.
        let element: Octets
        /// The RFC 8785 text of the normalized mutation that the server measures.
        let normalizedJSON: Data
        let authoredColumns: Int

        var exceedsMutationLimits: Bool {
            authoredColumns > PushLimits.maxAuthoredColumns
                || normalizedJSON.count > PushLimits.maxNormalizedMutationOctets
        }
    }

    static func requestOctets(_ request: PushRequest, encoder: JSONEncoder) throws -> Octets {
        let body = try encoder.encode(request)
        return Octets(body: body.count, canonical: try canonicalJSON(body).count)
    }

    /// Measures the request envelope with the largest generation and schema version.
    ///
    /// A successor batch after a binding renewal has the same members or fewer
    /// and can have larger values. This reserve makes sure that the successor also fits.
    static func envelopeReserve(
        clientID: String,
        batchID: String,
        schemaHash: String,
        encoder: JSONEncoder
    ) throws -> Octets {
        try requestOctets(
            PushRequest(
                clientID: clientID,
                clientGeneration: maxProtocolInteger,
                batchID: batchID,
                schema: SchemaRef(version: maxProtocolInteger, hash: schemaHash),
                mutations: []
            ),
            encoder: encoder
        )
    }

    static func measure(_ mutation: Mutation, encoder: JSONEncoder) throws -> MutationMeasure {
        let body = try encoder.encode(mutation)
        let parsed = try parse(body)
        guard let object = parsed as? [String: Any] else {
            throw invalidMutation
        }
        let columns: [String: Any]?
        if let value = object["columns"] {
            guard let value = value as? [String: Any] else { throw invalidMutation }
            columns = value
        } else {
            columns = nil
        }
        return MutationMeasure(
            element: Octets(body: body.count, canonical: try Integrity.canonicalJSONValue(parsed).count),
            normalizedJSON: try Integrity.canonicalJSONValue(normalizedForm(object, columns: columns)),
            authoredColumns: columns?.count ?? 0
        )
    }

    /// Writes the RFC 8785 form of a JSON text.
    ///
    /// The server changes each JSON integer to a double and does not apply the
    /// I-JSON safe integer check. This function does the same.
    static func canonicalJSON(_ json: Data) throws -> Data {
        try Integrity.canonicalJSONValue(parse(json))
    }

    private static var invalidMutation: SynchroError {
        .invalidResponse(message: "push mutation cannot be measured")
    }

    private static func parse(_ json: Data) throws -> Any {
        try JSONDecoder().decode(AnyCodable.self, from: json).value
    }

    private static func normalizedForm(_ mutation: [String: Any], columns: [String: Any]?) throws -> [Any] {
        guard let mutationID = mutation["mutation_id"] as? String,
              let table = mutation["table"] as? String,
              let pk = mutation["pk"] as? [String: Any],
              pk.count == 1,
              let primaryKey = pk.first,
              let schema = mutation["authored_schema"] as? [String: Any],
              let version = schema["version"] as? Int64,
              let hash = schema["hash"] as? String,
              let op = mutation["op"] as? String,
              let clientVersion = mutation["client_version"] as? String else {
            throw invalidMutation
        }
        let base: [Any] = mutation["base_version"].map { [1, $0] } ?? [0]
        let authoredColumns: [Any] = columns.map { columns in
            let pairs: [Any] = columns
                .sorted { $0.key.utf8.lexicographicallyPrecedes($1.key.utf8) }
                .map { [$0.key, $0.value] as [Any] }
            return [1, pairs]
        } ?? [0]
        return [
            "mutation-v1",
            mutationID,
            table,
            [primaryKey.key, primaryKey.value] as [Any],
            [String(version), hash] as [Any],
            op,
            base,
            clientVersion,
            authoredColumns,
        ]
    }
}
