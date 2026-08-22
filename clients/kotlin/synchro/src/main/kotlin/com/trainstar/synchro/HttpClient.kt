package com.trainstar.synchro

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
        )
    }

    suspend fun pull(request: PullRequest): PullResponse =
        pullExact(request, pullRequestJSON(request))

    internal fun pullRequestJSON(request: PullRequest): String =
        json.encodeToString(request).also(Integrity::validateCanonicalWireJSON)

    internal suspend fun pullExact(request: PullRequest, requestJSON: String): PullResponse {
        val validatedRequestJSON = validateExactRequestJSON(request, requestJSON)
        return post(
            "/sync/pull",
            validatedRequestJSON,
            RetryContext(RetryOperation.PULLING, validatedRequestJSON),
        )
    }

    suspend fun push(request: PushRequest): PushResponse = post(
        "/sync/push",
        json.encodeToString(request),
        RetryContext(RetryOperation.PUSHING, request.batchID),
    )

    /** Sends the exact JSON persisted when a batch was sealed. */
    internal suspend fun pushSealed(requestJSON: String, batchID: String): PushResponse =
        post("/sync/push", requestJSON, RetryContext(RetryOperation.PUSHING, batchID))

    /** Sends a sealed request and retains the exact response JSON. */
    internal suspend fun pushSealedWithBody(requestJSON: String, batchID: String): RawPushResponse {
        val result: HTTPResult<PushResponse> = postWithBody(
            "/sync/push",
            requestJSON,
            RetryContext(RetryOperation.PUSHING, batchID),
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
    ): Resp = postWithBody<Resp>(path, body, retryContext).value

    private suspend inline fun <reified Resp> postWithBody(
        path: String,
        body: String,
        retryContext: RetryContext,
    ): HTTPResult<Resp> {
        val url = config.serverURL.trimEnd('/') + path
        val request = Request.Builder()
            .url(url)
            .post(body.toRequestBody("application/json".toMediaType()))
            .header("Content-Type", "application/json")
            .build()
        return performWithBody(request, retryContext)
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
    ): HTTPResult<Resp> {
        val token = config.authProvider()
        val authedRequest = request.newBuilder()
            .header("Authorization", "Bearer $token")
            .header("X-App-Version", config.appVersion)
            .build()

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

        when (response.code) {
            200 -> {
                try {
                    return HTTPResult(json.decodeFromString<Resp>(responseBody), responseBody)
                } catch (e: Exception) {
                    throw SynchroError.InvalidResponse("decode failed: ${e.message}")
                }
            }

            409 -> {
                val error = decodeProtocolError(responseBody)
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
                val msg = errorMessage(responseBody) ?: "semantic conflict"
                throw SynchroError.ServerError(status = response.code, serverMessage = msg)
            }

            422 -> {
                val error = decodeProtocolError(responseBody)
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
                val msg = errorMessage(responseBody) ?: "schema or contract violation"
                throw SynchroError.ServerError(status = response.code, serverMessage = msg)
            }

            426 -> {
                val msg = errorMessage(responseBody) ?: "client upgrade required"
                throw SynchroError.UpgradeRequired(
                    currentVersion = config.appVersion,
                    minimumVersion = msg
                )
            }

            429, 503 -> {
                val error = decodeRetryableProtocolError(responseBody)
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
                val msg = errorMessage(responseBody) ?: "HTTP ${response.code}"
                throw SynchroError.ServerError(status = response.code, serverMessage = msg)
            }
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

    private fun parseRetryAfter(value: String?): RetryAfter? {
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
    private data class RetryAfter(val delaySeconds: Double?, val deadlineMs: Long?)
    private data class RetryContext(val interruptedOperation: String, val workIdentity: String)
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
