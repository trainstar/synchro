@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import com.trainstar.synchro.inspection.TransportObservationCollector
import com.trainstar.synchro.inspection.TransportOperationClass
import com.trainstar.synchro.inspection.TransportPullResponseFacts
import com.trainstar.synchro.inspection.TransportRebuildResponseFacts
import com.trainstar.synchro.inspection.TransportRequestFacts
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/** A first-execution generation rejection requires reconnect, not retry. */
internal class ClientGenerationExpiredException(
    val currentGeneration: Long,
) : Exception("client generation expired")

/** Keeps the exact validated response JSON for immutable outcome persistence. */
internal data class RawPushResponse(
    val response: PushResponse,
    val bodyJSON: String,
)

internal data class RawRebuildResponse(
    val response: RebuildResponse,
    val requestJSON: String,
    val responseJSON: String,
)

class HttpClient(
    private val config: SynchroConfig,
    private val client: OkHttpClient = OkHttpClient()
) {
    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
        explicitNulls = false
    }
    private val strictJSON = Json { ignoreUnknownKeys = false }

    // MARK: - Endpoints

    suspend fun connect(request: ConnectRequest): ConnectResponse =
        connectExact(request, connectRequestJSON(request))

    internal fun connectRequestJSON(request: ConnectRequest): String {
        val encoded = json.encodeToJsonElement(ConnectRequest.serializer(), request) as JsonObject
        val knownScopes = JsonObject(request.knownScopes.mapValues { (_, scope) ->
            JsonObject(mapOf("cursor" to (scope.cursor?.let(::JsonPrimitive) ?: JsonNull)))
        })
        return json.encodeToString(
            JsonObject.serializer(),
            JsonObject(encoded.toMutableMap().apply { put("known_scopes", knownScopes) }),
        ).also(Integrity::validateCanonicalWireJSON)
    }

    internal suspend fun connectExact(request: ConnectRequest, requestJSON: String): ConnectResponse {
        val validatedRequestJSON = validateExactRequestJSON(request, requestJSON)
        return post(
            "/sync/connect",
            validatedRequestJSON,
            RetryContext(RetryOperation.CONNECTING, validatedRequestJSON),
            requestFacts = transportRequestFacts(request),
        )
    }

    suspend fun pull(request: PullRequest): PullResponse =
        pullExact(request, pullRequestJSON(request))

    internal fun pullRequestJSON(request: PullRequest): String =
        json.encodeToString(request).also(Integrity::validateCanonicalWireJSON)

    internal suspend fun pullExact(request: PullRequest, requestJSON: String): PullResponse {
        val validatedRequestJSON = validateExactRequestJSON(request, requestJSON)
        val cursorObservation = pullCursorObservation(request)
        return post(
            "/sync/pull",
            validatedRequestJSON,
            RetryContext(RetryOperation.PULLING, validatedRequestJSON),
            cursorFingerprints = cursorObservation?.fingerprints,
            cursorFingerprintsComplete = cursorObservation?.complete,
            requestFacts = transportRequestFacts(request),
        )
    }

    suspend fun push(request: PushRequest): PushResponse = post(
        "/sync/push",
        json.encodeToString(request),
        RetryContext(RetryOperation.PUSHING, request.batchID),
        requestFacts = transportRequestFacts(request),
    )

    /** Sends the exact JSON persisted when a batch was sealed. */
    internal suspend fun pushSealed(requestJSON: String, batchID: String): PushResponse =
        post(
            "/sync/push",
            requestJSON,
            RetryContext(RetryOperation.PUSHING, batchID),
            requestFacts = sealedPushRequestFacts(requestJSON),
        )

    /** Sends a sealed request and retains the exact response JSON. */
    internal suspend fun pushSealedWithBody(requestJSON: String, batchID: String): RawPushResponse {
        val result: HTTPResult<PushResponse> = postWithBody(
            "/sync/push",
            requestJSON,
            RetryContext(RetryOperation.PUSHING, batchID),
            requestFacts = sealedPushRequestFacts(requestJSON),
        )
        return RawPushResponse(result.value, result.bodyJSON)
    }

    suspend fun rebuild(request: RebuildRequest): RebuildResponse = rebuildWithBody(request).response

    internal fun rebuildRequestJSON(request: RebuildRequest): String =
        json.encodeToString(request).also(Integrity::validateCanonicalWireJSON)

    internal suspend fun rebuildWithBody(
        request: RebuildRequest,
        requestJSON: String = rebuildRequestJSON(request),
    ): RawRebuildResponse {
        val result: HTTPResult<RebuildResponse> = postWithBody(
            "/sync/rebuild",
            requestJSON,
            RetryContext(RetryOperation.REBUILDING, requestJSON),
            requestFacts = transportRequestFacts(request),
        )
        return RawRebuildResponse(result.value, requestJSON, result.bodyJSON)
    }

    suspend fun fetchSchema(): SchemaResponse =
        get("/sync/schema", retryContext = null)

    // MARK: - HTTP

    private suspend inline fun <reified Resp> post(
        path: String,
        body: String,
        retryContext: RetryContext,
        cursorFingerprints: List<String>? = null,
        cursorFingerprintsComplete: Boolean? = null,
        requestFacts: TransportRequestFacts? = null,
    ): Resp = postWithBody<Resp>(
        path,
        body,
        retryContext,
        cursorFingerprints,
        cursorFingerprintsComplete,
        requestFacts,
    ).value

    private suspend inline fun <reified Resp> postWithBody(
        path: String,
        body: String,
        retryContext: RetryContext,
        cursorFingerprints: List<String>? = null,
        cursorFingerprintsComplete: Boolean? = null,
        requestFacts: TransportRequestFacts? = null,
    ): HTTPResult<Resp> {
        val url = config.serverURL.trimEnd('/') + path
        val request = Request.Builder()
            .url(url)
            .post(body.toRequestBody("application/json".toMediaType()))
            .header("Content-Type", "application/json")
            .build()
        return performWithBody(
            request,
            retryContext,
            cursorFingerprints,
            cursorFingerprintsComplete,
            requestFacts,
        )
    }

    private suspend inline fun <reified Resp> get(path: String, retryContext: RetryContext?): Resp {
        val url = config.serverURL.trimEnd('/') + path
        val request = Request.Builder()
            .url(url)
            .get()
            .header("Accept", "application/json")
            .build()
        return perform(request, retryContext)
    }

    private suspend inline fun <reified Resp> perform(request: Request, retryContext: RetryContext?): Resp =
        performWithBody<Resp>(request, retryContext).value

    private suspend inline fun <reified Resp> performWithBody(
        request: Request,
        retryContext: RetryContext?,
        cursorFingerprints: List<String>? = null,
        cursorFingerprintsComplete: Boolean? = null,
        requestFacts: TransportRequestFacts? = null,
    ): HTTPResult<Resp> {
        val token = config.authProvider()
        val authedRequest = request.newBuilder()
            .header("Authorization", "Bearer $token")
            .header("X-App-Version", config.appVersion)
            .build()

        val operationClass = TransportOperationClass.classify(authedRequest.url.encodedPath)
        val attemptStarted = System.nanoTime()
        var observedStatusCode = 0
        var observationRecorded = false

        try {
            val response: Response
            try {
                response = client.suspendEnqueue(authedRequest)
            } catch (e: IOException) {
                val underlying = SynchroError.NetworkError(e)
                if (retryContext == null) throw underlying
                throw RetryableError(
                    underlying = underlying,
                    retryAfter = null,
                    interruptedOperation = retryContext.interruptedOperation,
                    workIdentity = retryContext.workIdentity,
                    retryClassification = RetryClassification.NETWORK,
                )
            }
            observedStatusCode = response.code

            val responseContent = response.body
            val declaredCharset = responseContent?.contentType()?.charset(Charsets.UTF_8)
            if (declaredCharset != null && declaredCharset != Charsets.UTF_8) {
                responseContent.close()
                throw SynchroError.InvalidResponse("response is not UTF-8 JSON")
            }
            val bodyBytes = try {
                readBoundedBody(responseContent)
            } catch (e: IOException) {
                val underlying = SynchroError.NetworkError(e)
                if (retryContext == null) throw underlying
                throw RetryableError(
                    underlying = underlying,
                    retryAfter = null,
                    interruptedOperation = retryContext.interruptedOperation,
                    workIdentity = retryContext.workIdentity,
                    retryClassification = RetryClassification.NETWORK,
                )
            }
            val responseBody = try {
                Integrity.decodeCanonicalWireJSON(bodyBytes)
            } catch (e: Exception) {
                throw SynchroError.InvalidResponse("decode failed: ${e.message}")
            }

            recordTransportObservation(
                operationClass = operationClass,
                statusCode = observedStatusCode,
                attemptStarted = attemptStarted,
                cursorFingerprints = cursorFingerprints,
                cursorFingerprintsComplete = cursorFingerprintsComplete,
                requestFacts = requestFacts,
                responseBody = if (observedStatusCode == 200) responseBody else null,
                errorCode = observedErrorCode(observedStatusCode, responseBody),
            )
            observationRecorded = true
            val rebuildCursorOverride = config.transportObservationCollector?.pauseIfArmed(operationClass)
            val effectiveResponseBody = applyRebuildCursorOverride(
                observedStatusCode,
                responseBody,
                rebuildCursorOverride,
            )

            when (response.code) {
                200 -> {
                    try {
                        return HTTPResult(json.decodeFromString<Resp>(effectiveResponseBody), effectiveResponseBody)
                    } catch (e: Exception) {
                        throw SynchroError.InvalidResponse("decode failed: ${e.message}")
                    }
                }

                409 -> {
                    val error = decodeProtocolError(effectiveResponseBody)
                    if (error?.code == ProtocolErrorCode.CLIENT_GENERATION_EXPIRED) {
                        val currentGeneration = error.currentClientGeneration
                        if (error.retryable || currentGeneration == null || currentGeneration <= 0) {
                            throw SynchroError.InvalidResponse("client generation expiry response is invalid")
                        }
                        throw ClientGenerationExpiredException(currentGeneration)
                    }
                    if (error?.code == ProtocolErrorCode.REBUILD_RESTART_REQUIRED) {
                        val scopeID = error.scopeID
                        if (error.retryable || scopeID.isNullOrEmpty()) {
                            throw SynchroError.InvalidResponse("rebuild restart response is invalid")
                        }
                        throw RebuildRestartRequiredException(scopeID)
                    }
                    val msg = errorMessage(effectiveResponseBody) ?: "semantic conflict"
                    throw SynchroError.ServerError(status = response.code, serverMessage = msg)
                }

                422 -> {
                    val error = decodeProtocolError(effectiveResponseBody)
                        ?: throw SynchroError.InvalidResponse("schema mismatch response is invalid")
                    if (error.code == ProtocolErrorCode.SCHEMA_MISMATCH) {
                        val currentSchema = error.currentSchema
                        val receivedSchema = error.receivedSchema
                        if (error.retryable || currentSchema == null || receivedSchema == null) {
                            throw SynchroError.InvalidResponse("schema mismatch response is invalid")
                        }
                        try {
                            currentSchema.validate()
                            receivedSchema.validate()
                        } catch (_: ContractException) {
                            throw SynchroError.InvalidResponse("schema mismatch response has an invalid schema reference")
                        }
                        throw SynchroError.SchemaMismatch(
                            serverVersion = currentSchema.version,
                            serverHash = currentSchema.hash,
                        )
                    }
                    val msg = errorMessage(effectiveResponseBody) ?: "schema or contract violation"
                    throw SynchroError.ServerError(status = response.code, serverMessage = msg)
                }

                426 -> {
                    val msg = errorMessage(effectiveResponseBody) ?: "client upgrade required"
                    throw SynchroError.UpgradeRequired(
                        currentVersion = config.appVersion,
                        minimumVersion = msg
                    )
                }

                429, 503 -> {
                    val error = decodeRetryableProtocolError(effectiveResponseBody)
                        ?: throw SynchroError.InvalidResponse("retryable error response is invalid")
                    val validEnvelope = when (response.code) {
                        429 -> error.code == ProtocolErrorCode.RETRY_LATER && error.retryable
                        else -> error.code in setOf(
                            ProtocolErrorCode.CAPTURE_PENDING,
                            ProtocolErrorCode.TEMPORARY_UNAVAILABLE,
                        ) && error.retryable
                    }
                    if (!validEnvelope) {
                        throw SynchroError.InvalidResponse("retryable error response is invalid")
                    }
                    val retryAfter = parseRetryAfter(response.header("Retry-After"))
                        ?: throw SynchroError.InvalidResponse("retryable error response has an invalid Retry-After")
                    val underlying = SynchroError.ServerError(
                        status = response.code,
                        serverMessage = error.message,
                    )
                    if (retryContext == null) throw underlying
                    throw RetryableError(
                        underlying = underlying,
                        retryAfter = retryAfter.delaySeconds,
                        interruptedOperation = retryContext.interruptedOperation,
                        workIdentity = retryContext.workIdentity,
                        retryClassification = if (response.code == 429) {
                            RetryClassification.HTTP_429
                        } else {
                            RetryClassification.HTTP_503
                        },
                        retryAfterDeadlineMs = retryAfter.deadlineMs,
                    )
                }

                else -> {
                    val msg = errorMessage(effectiveResponseBody) ?: "HTTP ${response.code}"
                    throw SynchroError.ServerError(status = response.code, serverMessage = msg)
                }
            }
        } finally {
            if (!observationRecorded) {
                recordTransportObservation(
                    operationClass = operationClass,
                    statusCode = observedStatusCode,
                    attemptStarted = attemptStarted,
                    cursorFingerprints = cursorFingerprints,
                    cursorFingerprintsComplete = cursorFingerprintsComplete,
                    requestFacts = requestFacts,
                )
            }
        }
    }

    private fun recordTransportObservation(
        operationClass: TransportOperationClass,
        statusCode: Int,
        attemptStarted: Long,
        cursorFingerprints: List<String>?,
        cursorFingerprintsComplete: Boolean?,
        requestFacts: TransportRequestFacts?,
        responseBody: String? = null,
        errorCode: String? = null,
    ) {
        val duration = (System.nanoTime() - attemptStarted).coerceAtLeast(1)
        config.transportObservationCollector?.record(
            operationClass = operationClass,
            statusCode = statusCode,
            errorCode = errorCode,
            durationNanoseconds = duration,
            cursorFingerprints = if (operationClass == TransportOperationClass.PULL) {
                cursorFingerprints ?: emptyList()
            } else {
                null
            },
            cursorFingerprintsComplete = if (operationClass == TransportOperationClass.PULL) {
                cursorFingerprintsComplete ?: false
            } else {
                null
            },
            requestFacts = requestFacts,
            rebuildResponseFacts = if (operationClass == TransportOperationClass.REBUILD) {
                rebuildResponseFacts(responseBody)
            } else {
                null
            },
            pullResponseFacts = if (operationClass == TransportOperationClass.PULL) {
                pullResponseFacts(responseBody)
            } else {
                null
            },
        )
    }

    private fun applyRebuildCursorOverride(
        statusCode: Int,
        responseBody: String,
        override: String?,
    ): String {
        if (override == null) return responseBody
        if (statusCode != 200) {
            throw SynchroError.InvalidResponse("rebuild cursor fault requires a successful response")
        }
        // SCN-REBUILD-FORGED-CURSOR-001 requires one server-impossible continuation fault.
        val response = try {
            strictJSON.parseToJsonElement(responseBody) as JsonObject
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("rebuild cursor fault target is invalid")
        }
        val cursor = response["cursor"] as? JsonPrimitive
        if (cursor?.isString != true) {
            throw SynchroError.InvalidResponse("rebuild cursor fault target is invalid")
        }
        return json.encodeToString(
            JsonObject.serializer(),
            JsonObject(response.toMutableMap().also { it["cursor"] = JsonPrimitive(override) }),
        ).also(Integrity::validateCanonicalWireJSON)
    }

    private fun transportRequestFacts(request: ConnectRequest): TransportRequestFacts = TransportRequestFacts(
        clientGeneration = request.clientGeneration,
        schemaVersion = request.schema.version,
        schemaHash = request.schema.hash,
        protocolVersion = request.protocolVersion,
        scopeSetVersion = request.scopeSetVersion,
        scopeCount = request.knownScopes.size,
    )

    private fun transportRequestFacts(request: PullRequest): TransportRequestFacts = TransportRequestFacts(
        clientGeneration = request.clientGeneration,
        schemaVersion = request.schema.version,
        schemaHash = request.schema.hash,
        scopeSetVersion = request.scopeSetVersion,
        scopeCount = request.scopes.size,
        limit = request.limit,
    )

    private fun transportRequestFacts(request: RebuildRequest): TransportRequestFacts = TransportRequestFacts(
        clientGeneration = request.clientGeneration,
        schemaVersion = request.schema.version,
        schemaHash = request.schema.hash,
        limit = request.limit,
        scopeFingerprint = TransportObservationCollector.cursorFingerprint(request.scope),
        rebuildIDFingerprint = TransportObservationCollector.cursorFingerprint(request.rebuildID),
        cursorFingerprint = request.cursor?.let(TransportObservationCollector::cursorFingerprint),
        cursorPresent = request.cursor != null,
    )

    private fun transportRequestFacts(request: PushRequest): TransportRequestFacts = TransportRequestFacts(
        clientGeneration = request.clientGeneration,
        schemaVersion = request.schema.version,
        schemaHash = request.schema.hash,
        mutationCount = request.mutations.size,
    )

    private fun sealedPushRequestFacts(requestJSON: String): TransportRequestFacts? =
        config.transportObservationCollector?.let {
            runCatching { transportRequestFacts(json.decodeFromString<PushRequest>(requestJSON)) }.getOrNull()
        }

    private fun pullCursorObservation(request: PullRequest): PullCursorObservation? {
        if (config.transportObservationCollector == null) return null
        val fingerprints = request.scopes.values.mapNotNull { it.cursor }
            .map(TransportObservationCollector::cursorFingerprint)
            .distinct()
            .sorted()
        return PullCursorObservation(
            fingerprints = fingerprints.take(TransportObservationCollector.maximumCursorFingerprints),
            complete = fingerprints.size <= TransportObservationCollector.maximumCursorFingerprints,
        )
    }

    private fun rebuildResponseFacts(responseBody: String?): TransportRebuildResponseFacts? = responseBody?.let { body ->
        runCatching { json.decodeFromString<RebuildResponse>(body) }.getOrNull()?.let { response ->
            TransportRebuildResponseFacts(
                recordCount = response.records.size,
                hasMore = response.hasMore,
                hasCursor = response.cursor != null,
                hasFinalScopeCursor = response.finalScopeCursor != null,
                hasChecksum = response.checksum != null,
                scopeFingerprint = TransportObservationCollector.cursorFingerprint(response.scope),
                finalScopeCursorFingerprint = response.finalScopeCursor?.let(
                    TransportObservationCollector::cursorFingerprint,
                ),
            )
        }
    }

    private fun pullResponseFacts(responseBody: String?): TransportPullResponseFacts? = responseBody?.let { body ->
        runCatching { json.decodeFromString<PullResponse>(body) }.getOrNull()?.let { response ->
            val fingerprints = response.scopeCursors.values
                .map(TransportObservationCollector::cursorFingerprint)
                .distinct()
                .sorted()
            TransportPullResponseFacts(
                changeCount = response.changes.size,
                hasMore = response.hasMore,
                rebuildScopeCount = response.rebuild.size,
                checksumCount = response.checksums?.size ?: 0,
                scopeCursorFingerprints = fingerprints.take(TransportObservationCollector.maximumCursorFingerprints),
                scopeCursorFingerprintsComplete = fingerprints.size <= TransportObservationCollector.maximumCursorFingerprints,
            )
        }
    }

    private fun errorMessage(body: String): String? {
        decodeProtocolError(body)?.let { return it.message }
        return try {
            val map = json.decodeFromString<Map<String, String>>(body)
            map["error"]
        } catch (_: Exception) {
            null
        }
    }

    private fun observedErrorCode(statusCode: Int, responseBody: String?): String? {
        if (statusCode in 200..299 || responseBody == null) return null
        return decodeProtocolError(responseBody)?.code?.let { json.encodeToString(it).removeSurrounding("\"") }
    }

    private fun decodeProtocolError(body: String): ErrorBody? = try {
        json.decodeFromString<ErrorResponse>(body).error
    } catch (_: Exception) {
        null
    }

    private fun decodeRetryableProtocolError(body: String): ErrorBody? = try {
        strictJSON.decodeFromString<ErrorResponse>(body).error
    } catch (_: Exception) {
        null
    }

    private fun readBoundedBody(body: ResponseBody?): ByteArray {
        if (body == null) return byteArrayOf()
        return body.use { responseBody ->
            val expectedLength = responseBody.contentLength()
            if (expectedLength > Integrity.maxWireJSONBytes) {
                throw SynchroError.InvalidResponse("response is too large")
            }
            val output = java.io.ByteArrayOutputStream(
                if (expectedLength in 0..8192) expectedLength.toInt() else 8192
            )
            val buffer = ByteArray(8192)
            val input = responseBody.byteStream()
            var total = 0
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                total += count
                if (total > Integrity.maxWireJSONBytes) {
                    throw SynchroError.InvalidResponse("response is too large")
                }
                output.write(buffer, 0, count)
            }
            output.toByteArray()
        }
    }

    private inline fun <reified RequestType> validateExactRequestJSON(
        request: RequestType,
        requestJSON: String,
    ): String {
        return try {
            Integrity.validateCanonicalWireJSON(requestJSON)
            if (json.decodeFromString<RequestType>(requestJSON) != request) {
                throw SynchroError.InvalidResponse("retry request identity differs from its decoded value")
            }
            requestJSON
        } catch (error: SynchroError.InvalidResponse) {
            throw error
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("retry request identity is invalid")
        }
    }

    private data class HTTPResult<T>(val value: T, val bodyJSON: String)
    private data class PullCursorObservation(val fingerprints: List<String>, val complete: Boolean)
    private data class RetryContext(val interruptedOperation: String, val workIdentity: String)
}

internal data class RetryAfter(val delaySeconds: Double?, val deadlineMs: Long?)

internal fun parseRetryAfter(value: String?): RetryAfter? {
    val encoded = value?.trim()?.takeIf { it.isNotEmpty() } ?: return null
    if (encoded.matches(RETRY_AFTER_DELAY_PATTERN)) {
        encoded.toDoubleOrNull()?.takeIf { it.isFinite() }?.let {
            return RetryAfter(delaySeconds = it, deadlineMs = null)
        }
    }
    return try {
        RetryAfter(
            delaySeconds = null,
            deadlineMs = ZonedDateTime.parse(encoded, DateTimeFormatter.RFC_1123_DATE_TIME)
                .toInstant()
                .toEpochMilli(),
        )
    } catch (_: Exception) {
        null
    }
}

private val RETRY_AFTER_DELAY_PATTERN = Regex("(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?")

private suspend fun OkHttpClient.suspendEnqueue(request: Request): Response {
    return suspendCancellableCoroutine { continuation ->
        val call = newCall(request)
        continuation.invokeOnCancellation { call.cancel() }
        call.enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                if (continuation.isActive) {
                    continuation.resumeWithException(e)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                if (continuation.isActive) {
                    continuation.resume(response)
                } else {
                    response.close()
                }
            }
        })
    }
}
