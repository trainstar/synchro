package com.trainstar.synchro

import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class HttpClient(
    private val config: SynchroConfig,
    private val client: OkHttpClient = OkHttpClient()
) {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    // MARK: - Endpoints

    suspend fun register(request: RegisterRequest): RegisterResponse =
        post("/sync/register", json.encodeToString(request))

    suspend fun connect(request: VNextConnectRequest): VNextConnectResponse =
        post("/sync/connect", json.encodeToString(request))

    suspend fun pull(request: PullRequest): PullResponse =
        post("/sync/pull", json.encodeToString(request))

    suspend fun pull(request: VNextPullRequest): VNextPullResponse =
        post("/sync/pull", json.encodeToString(request))

    suspend fun push(request: PushRequest): PushResponse =
        post("/sync/push", json.encodeToString(request))

    suspend fun push(request: VNextPushRequest): VNextPushResponse =
        post("/sync/push", json.encodeToString(request))

    suspend fun rebuild(request: RebuildRequest): RebuildResponse =
        post("/sync/rebuild", json.encodeToString(request))

    suspend fun rebuild(request: VNextRebuildRequest): VNextRebuildResponse =
        post("/sync/rebuild", json.encodeToString(request))

    suspend fun fetchSchema(): SchemaResponse =
        get("/sync/schema")

    suspend fun fetchTables(): TableMetaResponse =
        get("/sync/tables")

    // MARK: - HTTP

    private suspend inline fun <reified Resp> post(path: String, body: String): Resp {
        val url = config.serverURL.trimEnd('/') + path
        val request = Request.Builder()
            .url(url)
            .post(body.toRequestBody("application/json".toMediaType()))
            .header("Content-Type", "application/json")
            .build()
        return perform(request)
    }

    private suspend inline fun <reified Resp> get(path: String): Resp {
        val url = config.serverURL.trimEnd('/') + path
        val request = Request.Builder()
            .url(url)
            .get()
            .header("Accept", "application/json")
            .build()
        return perform(request)
    }

    private suspend inline fun <reified Resp> perform(request: Request): Resp {
        val token = config.authProvider()
        val authedRequest = request.newBuilder()
            .header("Authorization", "Bearer $token")
            .header("X-App-Version", config.appVersion)
            .build()

        val response: Response
        try {
            response = client.suspendEnqueue(authedRequest)
        } catch (e: IOException) {
            throw RetryableError(
                underlying = SynchroError.NetworkError(e),
                retryAfter = null
            )
        }

        val responseBody = response.body?.string() ?: ""

        when (response.code) {
            200 -> {
                try {
                    return json.decodeFromString<Resp>(responseBody)
                } catch (e: Exception) {
                    throw SynchroError.InvalidResponse("decode failed: ${e.message}")
                }
            }

            409 -> {
                val msg = errorMessage(responseBody) ?: "semantic conflict"
                throw SynchroError.ServerError(status = response.code, serverMessage = msg)
            }

            422 -> {
                val body = decodeSchemaMismatch(responseBody)
                if (body != null) {
                    throw SynchroError.SchemaMismatch(
                        serverVersion = body.serverSchemaVersion ?: 0,
                        serverHash = body.serverSchemaHash ?: ""
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
                val retryAfter = response.header("Retry-After")?.toDoubleOrNull()
                throw RetryableError(
                    underlying = SynchroError.ServerError(
                        status = response.code,
                        serverMessage = errorMessage(responseBody) ?: "service unavailable"
                    ),
                    retryAfter = retryAfter
                )
            }

            else -> {
                val msg = errorMessage(responseBody) ?: "HTTP ${response.code}"
                throw SynchroError.ServerError(status = response.code, serverMessage = msg)
            }
        }
    }

    private fun errorMessage(body: String): String? {
        try {
            return json.decodeFromString<VNextErrorResponse>(body).error.message
        } catch (_: Exception) {
        }
        return try {
            val map = json.decodeFromString<Map<String, String>>(body)
            map["error"]
        } catch (_: Exception) {
            null
        }
    }

    private fun decodeSchemaMismatch(body: String): SchemaMismatchBody? {
        try {
            return json.decodeFromString<SchemaMismatchBody>(body)
        } catch (_: Exception) {
        }

        return try {
            val vnext = json.decodeFromString<VNextErrorResponse>(body)
            if (vnext.error.code == VNextProtocolErrorCode.SCHEMA_MISMATCH) {
                SchemaMismatchBody(
                    code = vnext.error.code.name.lowercase(),
                    message = vnext.error.message,
                    serverSchemaVersion = null,
                    serverSchemaHash = null
                )
            } else {
                null
            }
        } catch (_: Exception) {
            null
        }
    }
}

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
                }
            }
        })
    }
}
