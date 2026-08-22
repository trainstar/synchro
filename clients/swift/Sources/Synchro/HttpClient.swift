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

    func connect(
        request: ConnectRequest,
        requestBody: Data? = nil
    ) async throws -> ConnectResponse {
        let body = try requestBody ?? connectRequestBody(request)
        return try await postData(
            "/sync/connect",
            data: body,
            retryContext: try retryContext(resumeState: .connecting, workIdentity: body)
        )
    }

    func connectRequestBody(_ request: ConnectRequest) throws -> Data {
        try encoder.encode(request)
    }

    func pull(
        request: PullRequest,
        requestBody: Data? = nil
    ) async throws -> PullResponse {
        let body = try requestBody ?? pullRequestBody(request)
        let cursorObservation = pullCursorFingerprints(from: body)
        return try await postData(
            "/sync/pull",
            data: body,
            retryContext: try retryContext(resumeState: .pulling, workIdentity: body),
            cursorFingerprints: cursorObservation?.fingerprints,
            cursorFingerprintsComplete: cursorObservation?.complete
        )
    }

    func pullRequestBody(_ request: PullRequest) throws -> Data {
        try encoder.encode(request)
    }

    func push(request: PushRequest, bodyJSON: String? = nil) async throws -> PushResponse {
        let body: Data
        if let bodyJSON {
            body = Data(bodyJSON.utf8)
        } else {
            body = try encoder.encode(request)
        }
        return try await postData(
            "/sync/push",
            data: body,
            retryContext: RetryContext(resumeState: .pushing, workIdentity: request.batchID)
        )
    }

    func pushWithBody(request: PushRequest, bodyJSON: String) async throws -> (response: PushResponse, body: Data) {
        try await postDataWithBody(
            "/sync/push",
            data: Data(bodyJSON.utf8),
            retryContext: RetryContext(resumeState: .pushing, workIdentity: request.batchID)
        )
    }

    func rebuild(request: RebuildRequest) async throws -> RebuildResponse {
        try await rebuildWithBody(request: request).response
    }

    func rebuildRequestBody(_ request: RebuildRequest) throws -> Data {
        try encoder.encode(request)
    }

    func rebuildWithBody(
        request: RebuildRequest,
        requestBody: Data? = nil
    ) async throws -> (response: RebuildResponse, requestBody: Data, responseBody: Data) {
        let encodedRequest = try requestBody ?? rebuildRequestBody(request)
        let result: (response: RebuildResponse, body: Data) = try await postDataWithBody(
            "/sync/rebuild",
            data: encodedRequest,
            retryContext: try retryContext(resumeState: .rebuilding, workIdentity: encodedRequest)
        )
        return (response: result.response, requestBody: encodedRequest, responseBody: result.body)
    }

    func fetchSchema() async throws -> SchemaResponse {
        let url = config.serverURL.appendingPathComponent("/sync/schema")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        return try await performWithBody(request, retryContext: nil).response
    }

    // MARK: - HTTP

    private func postData<Resp: Decodable>(
        _ path: String,
        data: Data,
        retryContext: RetryContext,
        cursorFingerprints: [String]? = nil,
        cursorFingerprintsComplete: Bool? = nil
    ) async throws -> Resp {
        let url = config.serverURL.appendingPathComponent(path)
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = data
        return try await performWithBody(
            request,
            retryContext: retryContext,
            cursorFingerprints: cursorFingerprints,
            cursorFingerprintsComplete: cursorFingerprintsComplete
        ).response
    }

    private func postDataWithBody<Resp: Decodable>(
        _ path: String,
        data: Data,
        retryContext: RetryContext
    ) async throws -> (response: Resp, body: Data) {
        let url = config.serverURL.appendingPathComponent(path)
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = data
        return try await performWithBody(request, retryContext: retryContext)
    }

    private func performWithBody<Resp: Decodable>(
        _ request: URLRequest,
        retryContext: RetryContext?,
        cursorFingerprints: [String]? = nil,
        cursorFingerprintsComplete: Bool? = nil
    ) async throws -> (response: Resp, body: Data) {
        var req = request

        let token = try await config.authProvider()
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue(config.appVersion, forHTTPHeaderField: "X-App-Version")

        let (data, response) = try await performTransportAttempt(
            req,
            retryContext: retryContext,
            cursorFingerprints: cursorFingerprints,
            cursorFingerprintsComplete: cursorFingerprintsComplete
        )

        guard let httpResponse = response as? HTTPURLResponse else {
            throw SynchroError.invalidResponse(message: "not an HTTP response")
        }

        switch httpResponse.statusCode {
        case 200:
            do {
                try Integrity.validateCanonicalWireJSON(data)
                return (try decoder.decode(Resp.self, from: data), data)
            } catch {
                debugLogHTTPFailure(request: req, statusCode: httpResponse.statusCode)
                throw SynchroError.invalidResponse(message: "decode failed: \(error.localizedDescription)")
            }

        case 400:
            let error = try requireProtocolError(
                from: data,
                statusCode: httpResponse.statusCode,
                codes: [.invalidRequest, .invalidSchemaReference]
            )
            throw SynchroError.protocolError(
                status: httpResponse.statusCode,
                code: error.code,
                message: error.message
            )

        case 401:
            let error = try requireProtocolError(
                from: data,
                statusCode: httpResponse.statusCode,
                codes: [.authRequired]
            )
            throw SynchroError.protocolError(
                status: httpResponse.statusCode,
                code: error.code,
                message: error.message
            )

        case 409:
            if let body = decodeProtocolError(from: data), body.code == .clientGenerationExpired {
                guard body.retryable == false,
                      let currentGeneration = body.currentClientGeneration,
                      currentGeneration > 0 else {
                    throw SynchroError.invalidResponse(message: "client generation expiry response is invalid")
                }
                throw BindingRenewalError.clientGenerationExpired(
                    currentClientGeneration: currentGeneration
                )
            }
            if protocolErrorCode(from: data) == .rebuildRestartRequired {
                guard let body = decodeProtocolError(from: data),
                      body.retryable == false,
                      let scopeID = body.scopeID,
                      !scopeID.isEmpty else {
                    throw SynchroError.invalidResponse(message: "rebuild restart response is invalid")
                }
                throw RebuildRestartRequiredError(scopeID: scopeID)
            }
            let error = try requireProtocolError(
                from: data,
                statusCode: httpResponse.statusCode,
                codes: [.idempotencyConflict, .clientRetired]
            )
            throw SynchroError.protocolError(
                status: httpResponse.statusCode,
                code: error.code,
                message: error.message
            )

        case 422:
            if let body = decodeProtocolError(from: data), body.code == .schemaMismatch {
                guard body.retryable == false,
                      let currentSchema = body.currentSchema,
                      let receivedSchema = body.receivedSchema else {
                    throw SynchroError.invalidResponse(message: "schema mismatch response is invalid")
                }
                do {
                    try currentSchema.validate()
                    try receivedSchema.validate()
                } catch {
                    throw SynchroError.invalidResponse(message: "schema mismatch response has an invalid schema reference")
                }
                throw BindingRenewalError.schemaMismatch(
                    currentSchema: currentSchema,
                    receivedSchema: receivedSchema
                )
            }
            let msg = errorMessage(from: data) ?? "schema or contract violation"
            throw SynchroError.serverError(status: httpResponse.statusCode, message: msg)

        case 426:
            let minimumVersion = errorMessage(from: data) ?? "unknown"
            throw SynchroError.upgradeRequired(
                currentVersion: config.appVersion,
                minimumVersion: minimumVersion
            )

        case 429, 503:
            guard let error = decodeProtocolError(from: data),
                  isRetryableServiceEnvelope(error, statusCode: httpResponse.statusCode),
                  let retryAfter = parseRetryAfter(httpResponse) else {
                throw SynchroError.invalidResponse(message: "retryable service response is invalid")
            }
            let underlying = SynchroError.serverError(
                status: httpResponse.statusCode,
                message: error.message
            )
            guard let retryContext else { throw underlying }
            throw RetryableError(
                underlying: underlying,
                retryAfter: retryAfter,
                resumeState: retryContext.resumeState,
                workIdentity: retryContext.workIdentity,
                classification: httpResponse.statusCode == 429 ? .http429 : .http503
            )

        case 500:
            let error = try requireProtocolError(
                from: data,
                statusCode: httpResponse.statusCode,
                codes: [.syncIntegrityFailure]
            )
            throw SynchroError.protocolError(
                status: httpResponse.statusCode,
                code: error.code,
                message: error.message
            )

        default:
            debugLogHTTPFailure(request: req, statusCode: httpResponse.statusCode)
            let msg = errorMessage(from: data) ?? "HTTP \(httpResponse.statusCode)"
            throw SynchroError.serverError(status: httpResponse.statusCode, message: msg)
        }
    }

    private func performTransportAttempt(
        _ request: URLRequest,
        retryContext: RetryContext?,
        cursorFingerprints: [String]?,
        cursorFingerprintsComplete: Bool?
    ) async throws -> (data: Data, response: URLResponse) {
        let operationClass = TransportOperationClass.classify(path: request.url?.path ?? "")
        let attemptStarted = DispatchTime.now().uptimeNanoseconds
        var observedStatusCode = 0
        var observationRecorded = false
        let requestFacts = transportRequestFacts(operationClass: operationClass, body: request.httpBody)
        defer {
            if !observationRecorded {
                recordTransportObservation(
                    operationClass: operationClass,
                    statusCode: observedStatusCode,
                    attemptStarted: attemptStarted,
                    cursorFingerprints: cursorFingerprints,
                    cursorFingerprintsComplete: cursorFingerprintsComplete,
                    requestFacts: requestFacts,
                    responseBody: nil,
                    requestBody: request.httpBody
                )
            }
        }

        let (bytes, response): (URLSession.AsyncBytes, URLResponse)
        do {
            (bytes, response) = try await session.bytes(for: request)
        } catch {
            let underlying = SynchroError.networkError(underlying: error)
            guard let retryContext else { throw underlying }
            throw RetryableError(
                underlying: underlying,
                retryAfter: nil,
                resumeState: retryContext.resumeState,
                workIdentity: retryContext.workIdentity,
                classification: .network
            )
        }
        observedStatusCode = (response as? HTTPURLResponse)?.statusCode ?? 0

        if response.expectedContentLength > Int64(Integrity.maxWireJSONBytes) {
            throw SynchroError.invalidResponse(message: "response is too large")
        }
        var data = Data()
        do {
            for try await byte in bytes {
                guard data.count < Integrity.maxWireJSONBytes else {
                    throw SynchroError.invalidResponse(message: "response is too large")
                }
                data.append(byte)
            }
        } catch let error as SynchroError {
            throw error
        } catch {
            let underlying = SynchroError.networkError(underlying: error)
            guard let retryContext else { throw underlying }
            throw RetryableError(
                underlying: underlying,
                retryAfter: nil,
                resumeState: retryContext.resumeState,
                workIdentity: retryContext.workIdentity,
                classification: .network
            )
        }

        recordTransportObservation(
            operationClass: operationClass,
            statusCode: observedStatusCode,
            attemptStarted: attemptStarted,
            cursorFingerprints: cursorFingerprints,
            cursorFingerprintsComplete: cursorFingerprintsComplete,
            requestFacts: requestFacts,
            responseBody: observedStatusCode == 200 ? data : nil,
            requestBody: request.httpBody
        )
        observationRecorded = true
        try await config.transportObservationCollector?.pauseIfArmed(for: operationClass)
        return (data, response)
    }

    private func recordTransportObservation(
        operationClass: TransportOperationClass,
        statusCode: Int,
        attemptStarted: UInt64,
        cursorFingerprints: [String]?,
        cursorFingerprintsComplete: Bool?,
        requestFacts: TransportRequestFacts? = nil,
        responseBody: Data? = nil,
        requestBody: Data? = nil
    ) {
        let attemptEnded = DispatchTime.now().uptimeNanoseconds
        config.transportObservationCollector?.record(
            operationClass: operationClass,
            statusCode: statusCode,
            durationNanoseconds: attemptEnded >= attemptStarted ? attemptEnded - attemptStarted : 0,
            cursorFingerprints: operationClass == .pull ? cursorFingerprints ?? [] : nil,
            cursorFingerprintsComplete: operationClass == .pull ? cursorFingerprintsComplete ?? false : nil,
            requestFacts: requestFacts,
            rebuildResponseFacts: operationClass == .rebuild ? rebuildResponseFacts(from: responseBody, requestBody: requestBody) : nil,
            pullResponseFacts: operationClass == .pull ? pullResponseFacts(from: responseBody) : nil
        )
    }

    private func transportRequestFacts(
        operationClass: TransportOperationClass,
        body: Data?
    ) -> TransportRequestFacts? {
        guard let body else { return nil }
        switch operationClass {
        case .connect:
            guard let request = try? decoder.decode(ConnectRequest.self, from: body) else { return nil }
            return TransportRequestFacts(
                clientGeneration: request.clientGeneration,
                schemaVersion: request.schema.version,
                schemaHash: request.schema.hash,
                protocolVersion: request.protocolVersion,
                scopeSetVersion: request.scopeSetVersion,
                scopeCount: request.knownScopes.count,
                limit: nil,
                rebuildIDFingerprint: nil,
                cursorFingerprint: nil,
                cursorPresent: nil
            )
        case .pull:
            guard let request = try? decoder.decode(PullRequest.self, from: body) else { return nil }
            return TransportRequestFacts(
                clientGeneration: request.clientGeneration,
                schemaVersion: request.schema.version,
                schemaHash: request.schema.hash,
                protocolVersion: nil,
                scopeSetVersion: request.scopeSetVersion,
                scopeCount: request.scopes.count,
                limit: request.limit,
                rebuildIDFingerprint: nil,
                cursorFingerprint: nil,
                cursorPresent: nil
            )
        case .rebuild:
            guard let request = try? decoder.decode(RebuildRequest.self, from: body) else { return nil }
            return TransportRequestFacts(
                clientGeneration: request.clientGeneration,
                schemaVersion: request.schema.version,
                schemaHash: request.schema.hash,
                protocolVersion: nil,
                scopeSetVersion: nil,
                scopeCount: nil,
                limit: request.limit,
                rebuildIDFingerprint: TransportObservationCollector.cursorFingerprint(request.rebuildID),
                cursorFingerprint: request.cursor.map(TransportObservationCollector.cursorFingerprint),
                cursorPresent: request.cursor != nil
            )
        default:
            return nil
        }
    }

    private func rebuildResponseFacts(
        from data: Data?,
        requestBody: Data?
    ) -> TransportRebuildResponseFacts? {
        guard let data,
              (try? Integrity.validateCanonicalWireJSON(data)) != nil,
              let response = try? decoder.decode(RebuildResponse.self, from: data),
              let requestBody,
              let request = try? decoder.decode(RebuildRequest.self, from: requestBody) else { return nil }
        return TransportRebuildResponseFacts(
            recordCount: response.records.count,
            hasMore: response.hasMore,
            hasCursor: response.cursor != nil,
            hasFinalScopeCursor: response.finalScopeCursor != nil,
            hasChecksum: response.checksum != nil,
            scopeMatchesRequest: response.scope == request.scope
        )
    }

    private func pullResponseFacts(from data: Data?) -> TransportPullResponseFacts? {
        guard let data, let response = try? decoder.decode(PullResponse.self, from: data) else { return nil }
        return TransportPullResponseFacts(
            changeCount: response.changes.count,
            hasMore: response.hasMore,
            rebuildScopeCount: response.rebuild.count,
            checksumCount: response.checksums?.count ?? 0
        )
    }

    private func debugLogHTTPFailure(request: URLRequest, statusCode: Int) {
#if DEBUG
        let path = request.url?.path ?? "<unknown>"
        NSLog("Synchro HTTP failure path=%@ status=%d", path, statusCode)
#endif
    }

    private func isRetryableServiceEnvelope(_ error: ErrorBody, statusCode: Int) -> Bool {
        guard error.retryable else { return false }
        switch statusCode {
        case 429:
            return error.code == .retryLater
        case 503:
            return error.code == .capturePending || error.code == .temporaryUnavailable
        default:
            return false
        }
    }

    private func parseRetryAfter(_ response: HTTPURLResponse) -> TimeInterval? {
        guard let rawValue = response.value(forHTTPHeaderField: "Retry-After") else {
            return nil
        }
        let value = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty else { return nil }
        if let seconds = Double(value), seconds.isFinite, seconds >= 0 {
            return seconds
        }

        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE',' dd MMM yyyy HH':'mm':'ss zzz"
        guard let date = formatter.date(from: value) else {
            return nil
        }
        return max(0, date.timeIntervalSinceNow)
    }

    private func requireProtocolError(
        from data: Data,
        statusCode: Int,
        codes: Set<ProtocolErrorCode>
    ) throws -> ErrorBody {
        guard let error = decodeProtocolError(from: data),
              codes.contains(error.code),
              error.retryable == false,
              !error.message.isEmpty else {
            throw SynchroError.invalidResponse(
                message: "HTTP \(statusCode) protocol error response is invalid"
            )
        }
        return error
    }

    private func retryContext(
        resumeState: RetryResumeState,
        workIdentity: Data
    ) throws -> RetryContext {
        guard let identity = String(data: workIdentity, encoding: .utf8), !identity.isEmpty else {
            throw SynchroError.invalidResponse(message: "retry request identity is not UTF-8 JSON")
        }
        return RetryContext(resumeState: resumeState, workIdentity: identity)
    }

    private func pullCursorFingerprints(from data: Data) -> (fingerprints: [String], complete: Bool)? {
        guard config.transportObservationCollector != nil else { return nil }
        struct PullCursorEnvelope: Decodable {
            let scopes: [String: ScopeCursorRef]
        }
        guard let envelope = try? decoder.decode(PullCursorEnvelope.self, from: data) else {
            return ([], false)
        }
        let fingerprints = envelope.scopes.values.compactMap(\.cursor)
            .map(TransportObservationCollector.cursorFingerprint)
            .sorted()
        let maximum = TransportObservationCollector.maximumCursorFingerprints
        return (Array(fingerprints.prefix(maximum)), fingerprints.count <= maximum)
    }

    private func errorMessage(from data: Data) -> String? {
        if let body = try? decoder.decode(ErrorResponse.self, from: data) {
            return body.error.message
        }
        if let body = try? JSONDecoder().decode([String: String].self, from: data) {
            return body["error"]
        }
        return nil
    }

    private func decodeProtocolError(from data: Data) -> ErrorBody? {
        try? decoder.decode(ErrorResponse.self, from: data).error
    }

    private func protocolErrorCode(from data: Data) -> ProtocolErrorCode? {
        struct CodeOnlyErrorResponse: Decodable {
            struct Body: Decodable {
                let code: ProtocolErrorCode
            }

            let error: Body
        }
        return try? decoder.decode(CodeOnlyErrorResponse.self, from: data).error.code
    }
}

private struct RetryContext {
    let resumeState: RetryResumeState
    let workIdentity: String
}

enum BindingRenewalError: Error, Sendable, Equatable {
    case clientGenerationExpired(currentClientGeneration: Int64)
    case schemaMismatch(currentSchema: SchemaRef, receivedSchema: SchemaRef)
}

struct RebuildRestartRequiredError: Error, Sendable, Equatable {
    let scopeID: String
}

struct RetryableError: Error {
    let underlying: SynchroError
    let retryAfter: TimeInterval?
    let resumeState: RetryResumeState
    let workIdentity: String
    let classification: RetryClassification
}
