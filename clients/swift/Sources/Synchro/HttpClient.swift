import Foundation

final class HttpClient: @unchecked Sendable {
    private let config: SynchroConfig
    private let session: URLSession
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    init(config: SynchroConfig, session: URLSession = .shared) {
        self.config = config
        self.session = session
        self.encoder = JSONEncoder.synchroEncoder()
        self.decoder = JSONDecoder.synchroDecoder()
    }

    // MARK: - Endpoints

    func register(request: RegisterRequest) async throws -> RegisterResponse {
        try await post("/sync/register", body: request)
    }

    func pull(request: PullRequest) async throws -> PullResponse {
        try await post("/sync/pull", body: request)
    }

    func push(request: PushRequest) async throws -> PushResponse {
        try await post("/sync/push", body: request)
    }

    func rebuild(request: RebuildRequest) async throws -> RebuildResponse {
        try await post("/sync/rebuild", body: request)
    }

    func fetchSchema() async throws -> SchemaResponse {
        try await get("/sync/schema")
    }

    func fetchTables() async throws -> TableMetaResponse {
        try await get("/sync/tables")
    }

    // MARK: - HTTP

    private func post<Req: Encodable, Resp: Decodable>(_ path: String, body: Req) async throws -> Resp {
        let url = config.serverURL.appendingPathComponent(path)
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try encoder.encode(body)
        return try await perform(request)
    }

    private func get<Resp: Decodable>(_ path: String) async throws -> Resp {
        let url = config.serverURL.appendingPathComponent(path)
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        return try await perform(request)
    }

    private func perform<Resp: Decodable>(_ request: URLRequest) async throws -> Resp {
        var req = request

        let token = try await config.authProvider()
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue(config.appVersion, forHTTPHeaderField: "X-App-Version")

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await session.data(for: req)
        } catch {
            throw RetryableError(
                underlying: .networkError(underlying: error),
                retryAfter: nil
            )
        }

        guard let httpResponse = response as? HTTPURLResponse else {
            throw SynchroError.invalidResponse(message: "not an HTTP response")
        }

        switch httpResponse.statusCode {
        case 200:
            do {
                return try decoder.decode(Resp.self, from: data)
            } catch {
                throw SynchroError.invalidResponse(message: "decode failed: \(error.localizedDescription)")
            }

        case 409:
            let body = try? decoder.decode(SchemaMismatchBody.self, from: data)
            throw SynchroError.schemaMismatch(
                serverVersion: body?.serverSchemaVersion ?? 0,
                serverHash: body?.serverSchemaHash ?? ""
            )

        case 426:
            let minimumVersion = errorMessage(from: data) ?? "unknown"
            throw SynchroError.upgradeRequired(
                currentVersion: config.appVersion,
                minimumVersion: minimumVersion
            )

        case 429, 503:
            let retryAfter = parseRetryAfter(httpResponse)
            throw RetryableError(
                underlying: SynchroError.serverError(
                    status: httpResponse.statusCode,
                    message: errorMessage(from: data) ?? "service unavailable"
                ),
                retryAfter: retryAfter
            )

        default:
            let msg = errorMessage(from: data) ?? "HTTP \(httpResponse.statusCode)"
            throw SynchroError.serverError(status: httpResponse.statusCode, message: msg)
        }
    }

    private func parseRetryAfter(_ response: HTTPURLResponse) -> TimeInterval? {
        guard let value = response.value(forHTTPHeaderField: "Retry-After"),
              let seconds = Double(value) else {
            return nil
        }
        return seconds
    }

    private func errorMessage(from data: Data) -> String? {
        if let body = try? JSONDecoder().decode([String: String].self, from: data) {
            return body["error"]
        }
        return nil
    }
}

struct RetryableError: Error {
    let underlying: SynchroError
    let retryAfter: TimeInterval?
}

private struct SchemaMismatchBody: Decodable {
    let code: String?
    let message: String?
    let serverSchemaVersion: Int64?
    let serverSchemaHash: String?

    enum CodingKeys: String, CodingKey {
        case code
        case message
        case serverSchemaVersion = "server_schema_version"
        case serverSchemaHash = "server_schema_hash"
    }
}
