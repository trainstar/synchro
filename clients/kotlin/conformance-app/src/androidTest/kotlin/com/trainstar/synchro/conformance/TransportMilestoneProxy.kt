package com.trainstar.synchro.conformance

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.security.MessageDigest
import java.util.Locale
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

internal class TransportMilestoneProxy(
    private val upstreamBaseURL: String,
    private val capacity: Int,
) : Closeable {
    private val server = ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
    private val accepting = AtomicBoolean(true)
    private val sequence = AtomicLong()
    private val json = Json { ignoreUnknownKeys = false }
    private val observations = CopyOnWriteArrayList<TransportObservation>()
    private val activeSockets = ConcurrentHashMap.newKeySet<Socket>()
    private val workers = Executors.newFixedThreadPool(MAXIMUM_ACTIVE_REQUESTS)
    private val barrier = Object()
    private var armedOperation: String? = null
    private var nextArmedOperation: String? = null
    private var reached = false
    private var resumed = false
    private var rebuildCursorOverride: String? = null
    private var overflowed = false
    private val acceptThread = Thread(::acceptLoop, "synchro-conformance-proxy").apply {
        isDaemon = true
        start()
    }

    val localURL: String get() = "http://127.0.0.1:${server.localPort}"

    fun arm(operation: String) {
        requireOperation(operation)
        synchronized(barrier) {
            if (armedOperation == null) {
                armedOperation = operation
                reached = false
                resumed = false
            } else {
                check(reached && !resumed && nextArmedOperation == null) { "a transport pause is already armed" }
                nextArmedOperation = operation
            }
        }
    }

    fun await(operation: String, timeoutMillis: Long = PAUSE_TIMEOUT_MILLIS) {
        requireOperation(operation)
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        synchronized(barrier) {
            check(armedOperation == operation) { "the requested transport pause is not armed" }
            while (!reached) {
                val remaining = deadline - System.nanoTime()
                check(remaining > 0) { "the transport pause was not reached" }
                TimeUnit.NANOSECONDS.timedWait(barrier, remaining)
            }
        }
    }

    fun resume() {
        synchronized(barrier) {
            check(armedOperation != null && reached && !resumed) { "no reached transport pause is available" }
            resumed = true
            barrier.notifyAll()
        }
    }

    fun overridePausedRebuildCursor(cursor: String) {
        require(cursor.isNotEmpty() && cursor.toByteArray(Charsets.UTF_8).size <= 4_096) {
            "rebuild cursor override is invalid"
        }
        synchronized(barrier) {
            check(armedOperation == "rebuild" && reached && !resumed && rebuildCursorOverride == null) {
                "no paused rebuild response is available"
            }
            rebuildCursorOverride = cursor
        }
    }

    fun milestoneObservation(): JsonObject = synchronized(barrier) {
        buildJsonObject {
            armedOperation?.let { put("operation_class", it) }
            put("state", when {
                armedOperation == null -> "idle"
                resumed -> "resumed"
                reached -> "reached"
                else -> "armed"
            })
        }
    }

    fun observationSnapshot(): JsonObject {
        val values: List<TransportObservation>
        val didOverflow: Boolean
        val checkpoint: Long
        synchronized(observations) {
            values = observations.toList()
            didOverflow = overflowed
            checkpoint = sequence.get()
        }
        return buildJsonObject {
            put("observations", buildJsonArray {
                values.forEach { value ->
                    add(buildJsonObject {
                        put("sequence", value.sequence)
                        put("operation_class", value.operation)
                        put("status_code", value.statusCode)
                        put("error_code", value.errorCode?.let(::JsonPrimitive) ?: JsonNull)
                        put("retryable", value.retryable)
                        put("duration_nanoseconds", value.durationNanoseconds)
                        value.cursorFingerprints?.let { fingerprints ->
                            put("cursor_fingerprints", buildJsonArray {
                                fingerprints.forEach { add(JsonPrimitive(it)) }
                            })
                            put("cursor_fingerprints_complete", value.cursorFingerprintsComplete)
                        }
                        value.requestFacts?.let { put("request_facts", it) }
                        value.rebuildResponseFacts?.let { put("rebuild_response_facts", it) }
                        value.pullResponseFacts?.let { put("pull_response_facts", it) }
                    })
                }
            })
            put("overflowed", didOverflow)
            put("sequence_checkpoint", checkpoint)
        }
    }

    override fun close() {
        if (!accepting.compareAndSet(true, false)) return
        synchronized(barrier) {
            resumed = true
            rebuildCursorOverride = null
            barrier.notifyAll()
        }
        server.close()
        activeSockets.forEach { runCatching { it.close() } }
        acceptThread.join(1_000)
        check(!acceptThread.isAlive) { "transport accept thread did not terminate" }
        workers.shutdownNow()
        check(workers.awaitTermination(1, TimeUnit.SECONDS)) { "transport workers did not terminate" }
    }

    private fun acceptLoop() {
        while (accepting.get()) {
            val socket = try {
                server.accept()
            } catch (_: Exception) {
                return
            }
            activeSockets += socket
            try {
                workers.execute { handle(socket) }
            } catch (error: RuntimeException) {
                activeSockets -= socket
                runCatching { socket.close() }
                if (accepting.get()) throw error
            }
        }
    }

    private fun handle(socket: Socket) {
        val started = System.nanoTime()
        var operation = "other"
        var statusCode = 0
        var errorCode: String? = null
        var retryable = true
        var requestEvidence: RequestEvidence? = null
        var responseEvidence: ResponseEvidence? = null
        var observationRecorded = false
        try {
            socket.use { clientSocket ->
                clientSocket.soTimeout = SOCKET_TIMEOUT_MILLIS
                val input = BufferedInputStream(clientSocket.getInputStream())
                val output = BufferedOutputStream(clientSocket.getOutputStream())
                val requestLine = readLine(input, MAXIMUM_REQUEST_LINE_BYTES)
                    ?: throw IllegalArgumentException("request line is missing")
                val requestParts = requestLine.split(' ')
                require(requestParts.size == 3 && requestParts[2] == "HTTP/1.1") { "request line is invalid" }
                val method = requestParts[0]
                val target = requestParts[1]
                require(method == "GET" || method == "POST") { "request method is unsupported" }
                require(target.startsWith('/') && !target.startsWith("//")) { "request target is invalid" }
                val headers = readHeaders(input)
                val contentLength = headers["content-length"]?.singleOrNull()?.toIntOrNull() ?: 0
                require(contentLength in 0..MAXIMUM_BODY_BYTES) { "request body is too large" }
                require(headers["transfer-encoding"] == null) { "chunked requests are unsupported" }
                val body = ByteArray(contentLength)
                var offset = 0
                while (offset < body.size) {
                    val count = input.read(body, offset, body.size - offset)
                    if (count < 0) throw IllegalArgumentException("request body is incomplete")
                    offset += count
                }
                operation = operationFor(target)
                requestEvidence = requestEvidence(operation, body)
                val response = forward(method, target, headers, body)
                statusCode = response.statusCode
                val outcome = wireOutcome(response)
                errorCode = outcome.errorCode
                retryable = outcome.retryable
                responseEvidence = responseEvidence(operation, response)
                val duration = (System.nanoTime() - started).coerceAtLeast(1)
                appendObservation(operation, statusCode, errorCode, retryable, duration, requestEvidence, responseEvidence)
                observationRecorded = true
                pauseIfArmed(operation)
                writeResponse(output, overrideRebuildCursorIfArmed(operation, response))
            }
        } catch (_: Exception) {
            runCatching { socket.close() }
        } finally {
            activeSockets -= socket
            if (!observationRecorded) {
                val duration = (System.nanoTime() - started).coerceAtLeast(1)
                appendObservation(operation, statusCode, errorCode, retryable, duration, requestEvidence, responseEvidence)
            }
        }
    }

    private fun pauseIfArmed(operation: String) {
        synchronized(barrier) {
            if (armedOperation != operation || reached) return
            reached = true
            barrier.notifyAll()
            while (!resumed && accepting.get()) barrier.wait()
            armedOperation = nextArmedOperation
            nextArmedOperation = null
            reached = false
            resumed = false
        }
    }

    private fun overrideRebuildCursorIfArmed(operation: String, response: ProxyResponse): ProxyResponse {
        val override = synchronized(barrier) {
            if (operation != "rebuild") return@synchronized null
            rebuildCursorOverride.also { rebuildCursorOverride = null }
        } ?: return response
        check(response.statusCode == 200) { "rebuild cursor override requires a successful response" }
        val body = json.parseToJsonElement(response.body.toString(Charsets.UTF_8)).jsonObject
        check(body["cursor"]?.let { it is JsonPrimitive && it.isString } == true) {
            "rebuild cursor override target is invalid"
        }
        val rewritten = JsonObject(body.toMutableMap().also { it["cursor"] = JsonPrimitive(override) })
        return response.copy(body = rewritten.toString().toByteArray(Charsets.UTF_8))
    }

    private fun forward(
        method: String,
        target: String,
        headers: Map<String, List<String>>,
        body: ByteArray,
    ): ProxyResponse {
        val connection = URL(upstreamBaseURL.trimEnd('/') + target).openConnection() as HttpURLConnection
        try {
            connection.instanceFollowRedirects = false
            connection.connectTimeout = 30_000
            connection.readTimeout = 60_000
            connection.requestMethod = method
            connection.setRequestProperty("Accept-Encoding", "identity")
            headers.forEach { (name, values) ->
                if (name !in HOP_BY_HOP_REQUEST_HEADERS) {
                    values.forEach { value -> connection.addRequestProperty(name, value) }
                }
            }
            if (method == "POST") {
                connection.doOutput = true
                connection.setFixedLengthStreamingMode(body.size)
                connection.outputStream.use { it.write(body) }
            }
            val status = connection.responseCode
            val declaredLength = connection.contentLengthLong
            require(declaredLength <= MAXIMUM_RESPONSE_BYTES) { "response body is too large" }
            val responseBody = (if (status >= 400) connection.errorStream else connection.inputStream)
                ?.use { stream -> readBounded(stream, declaredLength, MAXIMUM_RESPONSE_BYTES) }
                ?: ByteArray(0)
            val reason = connection.responseMessage ?: ""
            val responseHeaders = connection.headerFields
                .filterKeys { it != null && it.lowercase(Locale.US) !in HOP_BY_HOP_RESPONSE_HEADERS }
                .mapKeys { it.key!! }
                .mapValues { it.value.orEmpty().filterNotNull() }
            return ProxyResponse(status, reason, responseHeaders, responseBody)
        } finally {
            connection.disconnect()
        }
    }

    private fun writeResponse(output: BufferedOutputStream, response: ProxyResponse) {
        val reason = response.reason.replace("\r", "").replace("\n", "")
        output.write("HTTP/1.1 ${response.statusCode} $reason\r\n".toByteArray(Charsets.US_ASCII))
        response.headers.forEach { (name, values) ->
            values.forEach { value ->
                val bounded = value.replace("\r", "").replace("\n", "")
                output.write("$name: $bounded\r\n".toByteArray(Charsets.ISO_8859_1))
            }
        }
        output.write("Content-Length: ${response.body.size}\r\nConnection: close\r\n\r\n".toByteArray(Charsets.US_ASCII))
        output.write(response.body)
        output.flush()
    }

    private fun readBounded(input: InputStream, declaredLength: Long, maximum: Int): ByteArray {
        require(declaredLength <= maximum) { "response body is too large" }
        val initialCapacity = when {
            declaredLength in 0..maximum.toLong() -> declaredLength.toInt()
            else -> RESPONSE_BUFFER_BYTES
        }
        val output = ByteArrayOutputStream(initialCapacity)
        val buffer = ByteArray(RESPONSE_BUFFER_BYTES)
        var total = 0
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            require(total <= maximum - count) { "response body is too large" }
            output.write(buffer, 0, count)
            total += count
        }
        return output.toByteArray()
    }

    private fun readHeaders(input: BufferedInputStream): Map<String, List<String>> {
        val result = linkedMapOf<String, MutableList<String>>()
        var bytes = 0
        repeat(MAXIMUM_HEADERS) {
            val line = readLine(input, MAXIMUM_HEADER_LINE_BYTES)
                ?: throw IllegalArgumentException("headers are incomplete")
            bytes += line.length
            require(bytes <= MAXIMUM_HEADER_BYTES) { "headers are too large" }
            if (line.isEmpty()) return result
            val separator = line.indexOf(':')
            require(separator > 0) { "header is invalid" }
            val name = line.substring(0, separator).trim().lowercase(Locale.US)
            val value = line.substring(separator + 1).trim()
            require(name.matches(HEADER_NAME)) { "header name is invalid" }
            result.getOrPut(name) { mutableListOf() }.add(value)
        }
        throw IllegalArgumentException("too many headers")
    }

    private fun readLine(input: BufferedInputStream, maximum: Int): String? {
        val bytes = ArrayList<Byte>()
        while (bytes.size <= maximum) {
            val value = input.read()
            if (value < 0) return if (bytes.isEmpty()) null else throw IllegalArgumentException("line is incomplete")
            if (value == '\n'.code) {
                require(bytes.isNotEmpty() && bytes.removeAt(bytes.lastIndex) == '\r'.code.toByte()) {
                    "line ending is invalid"
                }
                return bytes.toByteArray().toString(Charsets.ISO_8859_1)
            }
            bytes.add(value.toByte())
        }
        throw IllegalArgumentException("line is too large")
    }

    private fun appendObservation(
        operation: String,
        statusCode: Int,
        errorCode: String?,
        retryable: Boolean,
        duration: Long,
        request: RequestEvidence?,
        response: ResponseEvidence?,
    ) {
        synchronized(observations) {
            val next = sequence.incrementAndGet()
            if (observations.size >= capacity) {
                overflowed = true
                return
            }
            observations += TransportObservation(
                sequence = next,
                operation = operation,
                statusCode = statusCode,
                errorCode = errorCode,
                retryable = retryable,
                durationNanoseconds = duration,
                cursorFingerprints = request?.cursorFingerprints,
                cursorFingerprintsComplete = request?.cursorFingerprintsComplete,
                requestFacts = request?.facts,
                rebuildResponseFacts = response?.rebuild,
                pullResponseFacts = response?.pull,
            )
        }
    }

    private fun wireOutcome(response: ProxyResponse): WireOutcome {
        if (response.statusCode in 200..299) return WireOutcome(null, false)
        val error = runCatching {
            json.parseToJsonElement(response.body.toString(Charsets.UTF_8))
                .jsonObject.getValue("error").jsonObject
        }.getOrNull() ?: return WireOutcome(null, false)
        return WireOutcome(
            errorCode = runCatching { error.requiredString("code") }.getOrNull(),
            retryable = runCatching { error.requiredBoolean("retryable") }.getOrDefault(false),
        )
    }

    private fun requestEvidence(operation: String, body: ByteArray): RequestEvidence? {
        if (operation !in setOf("connect", "pull", "push", "rebuild")) return null
        val value = json.parseToJsonElement(body.toString(Charsets.UTF_8)).jsonObject
        val schema = value.getValue("schema").jsonObject
        val common = linkedMapOf<String, kotlinx.serialization.json.JsonElement>(
            "schema_version" to JsonPrimitive(schema.requiredLong("version")),
            "schema_hash" to JsonPrimitive(schema.requiredString("hash")),
        )
        value.optionalLong("client_generation")?.let { common["client_generation"] = JsonPrimitive(it) }
        var cursorFingerprints: List<String>? = null
        var cursorFingerprintsComplete: Boolean? = null
        when (operation) {
            "connect" -> {
                common["protocol_version"] = JsonPrimitive(value.requiredInt("protocol_version"))
                common["scope_set_version"] = JsonPrimitive(value.requiredLong("scope_set_version"))
                common["scope_count"] = JsonPrimitive(value.getValue("known_scopes").jsonObject.size)
            }
            "pull" -> {
                common["scope_set_version"] = JsonPrimitive(value.requiredLong("scope_set_version"))
                val scopes = value.getValue("scopes").jsonObject
                common["scope_count"] = JsonPrimitive(scopes.size)
                common["limit"] = JsonPrimitive(value.requiredInt("limit"))
                val all = scopes.values.mapNotNull { scopeValue ->
                    val cursor = scopeValue.jsonObject["cursor"]
                    if (cursor == null || cursor is JsonNull) {
                        null
                    } else {
                        require(cursor.jsonPrimitive.isString) { "pull cursor is invalid" }
                        fingerprint(cursor.jsonPrimitive.content)
                    }
                }.distinct().sorted()
                cursorFingerprintsComplete = all.size <= MAXIMUM_CURSOR_FINGERPRINTS
                cursorFingerprints = all.take(MAXIMUM_CURSOR_FINGERPRINTS)
            }
            "push" -> {
                common["mutation_count"] = JsonPrimitive(value.getValue("mutations").jsonArray.size)
            }
            "rebuild" -> {
                common["scope_fingerprint"] = JsonPrimitive(fingerprint(value.requiredString("scope")))
                common["limit"] = JsonPrimitive(value.requiredInt("limit"))
                common["rebuild_id_fingerprint"] = JsonPrimitive(fingerprint(value.requiredString("rebuild_id")))
                val cursor = value["cursor"]
                val present = cursor != null && cursor !is JsonNull
                common["cursor_present"] = JsonPrimitive(present)
                if (present) {
                    require(cursor!!.jsonPrimitive.isString) { "rebuild cursor is invalid" }
                    common["cursor_fingerprint"] = JsonPrimitive(fingerprint(cursor.jsonPrimitive.content))
                }
            }
        }
        return RequestEvidence(JsonObject(common), cursorFingerprints, cursorFingerprintsComplete)
    }

    private fun responseEvidence(
        operation: String,
        response: ProxyResponse,
    ): ResponseEvidence? {
        if (response.statusCode != 200 || operation !in setOf("pull", "rebuild")) return null
        val value = json.parseToJsonElement(response.body.toString(Charsets.UTF_8)).jsonObject
        return when (operation) {
            "pull" -> ResponseEvidence(
                pull = buildJsonObject {
                    val all = value.getValue("scope_cursors").jsonObject.values.map { cursor ->
                        require(cursor.jsonPrimitive.isString) { "pull response cursor is invalid" }
                        fingerprint(cursor.jsonPrimitive.content)
                    }.distinct().sorted()
                    put("change_count", value.getValue("changes").jsonArray.size)
                    put("has_more", value.requiredBoolean("has_more"))
                    put("rebuild_scope_count", value.getValue("rebuild").jsonArray.size)
                    val checksums = value["checksums"]
                    put("checksum_count", if (checksums == null || checksums is JsonNull) 0 else checksums.jsonObject.size)
                    put("scope_cursor_fingerprints", buildJsonArray {
                        all.take(MAXIMUM_CURSOR_FINGERPRINTS).forEach { add(JsonPrimitive(it)) }
                    })
                    put("scope_cursor_fingerprints_complete", all.size <= MAXIMUM_CURSOR_FINGERPRINTS)
                },
            )
            else -> ResponseEvidence(
                rebuild = buildJsonObject {
                    val finalScopeCursor = value["final_scope_cursor"]
                    put("record_count", value.getValue("records").jsonArray.size)
                    put("has_more", value.requiredBoolean("has_more"))
                    put("has_cursor", value["cursor"] != null && value["cursor"] !is JsonNull)
                    put("has_final_scope_cursor", finalScopeCursor != null && finalScopeCursor !is JsonNull)
                    put("has_checksum", value["checksum"] != null && value["checksum"] !is JsonNull)
                    put("scope_fingerprint", fingerprint(value.requiredString("scope")))
                    if (finalScopeCursor != null && finalScopeCursor !is JsonNull) {
                        require(finalScopeCursor.jsonPrimitive.isString) { "final scope cursor is invalid" }
                        put("final_scope_cursor_fingerprint", fingerprint(finalScopeCursor.jsonPrimitive.content))
                    }
                },
            )
        }
    }

    private fun fingerprint(value: String): String = MessageDigest.getInstance("SHA-256")
        .digest(value.toByteArray(Charsets.UTF_8))
        .joinToString("") { "%02x".format(Locale.US, it.toInt() and 0xff) }

    private fun operationFor(target: String): String {
        val path = target.substringBefore('?')
        return when {
            path.endsWith("/sync/connect") -> "connect"
            path.endsWith("/sync/pull") -> "pull"
            path.endsWith("/sync/push") -> "push"
            path.endsWith("/sync/rebuild") -> "rebuild"
            path.endsWith("/sync/checkpoint") -> "checkpoint"
            path.endsWith("/sync/schema") -> "schemas"
            else -> "other"
        }
    }

    private fun requireOperation(operation: String) {
        require(operation in OPERATION_CLASSES) { "transport operation is invalid" }
    }

    private data class ProxyResponse(
        val statusCode: Int,
        val reason: String,
        val headers: Map<String, List<String>>,
        val body: ByteArray,
    )

    private data class TransportObservation(
        val sequence: Long,
        val operation: String,
        val statusCode: Int,
        val errorCode: String?,
        val retryable: Boolean,
        val durationNanoseconds: Long,
        val cursorFingerprints: List<String>?,
        val cursorFingerprintsComplete: Boolean?,
        val requestFacts: JsonObject?,
        val rebuildResponseFacts: JsonObject?,
        val pullResponseFacts: JsonObject?,
    )

    private data class RequestEvidence(
        val facts: JsonObject,
        val cursorFingerprints: List<String>?,
        val cursorFingerprintsComplete: Boolean?,
    )

    private data class ResponseEvidence(
        val rebuild: JsonObject? = null,
        val pull: JsonObject? = null,
    )

    private data class WireOutcome(
        val errorCode: String?,
        val retryable: Boolean,
    )

    private companion object {
        const val PAUSE_TIMEOUT_MILLIS = 10_000L
        const val SOCKET_TIMEOUT_MILLIS = 70_000
        const val MAXIMUM_ACTIVE_REQUESTS = 16
        const val MAXIMUM_REQUEST_LINE_BYTES = 8_192
        const val MAXIMUM_HEADER_LINE_BYTES = 8_192
        const val MAXIMUM_HEADER_BYTES = 65_536
        const val MAXIMUM_HEADERS = 64
        const val MAXIMUM_BODY_BYTES = 1 shl 20
        const val MAXIMUM_RESPONSE_BYTES = 8 shl 20
        const val RESPONSE_BUFFER_BYTES = 8_192
        const val MAXIMUM_CURSOR_FINGERPRINTS = 16
        val HEADER_NAME = Regex("[!#$%&'*+.^_`|~0-9A-Za-z-]+")
        val OPERATION_CLASSES = setOf("connect", "pull", "push", "checkpoint", "schemas", "rebuild", "other")
        val HOP_BY_HOP_REQUEST_HEADERS = setOf("host", "connection", "content-length", "transfer-encoding", "accept-encoding")
        val HOP_BY_HOP_RESPONSE_HEADERS = setOf("connection", "content-length", "transfer-encoding")
    }
}

private fun JsonObject.requiredString(name: String): String =
    (getValue(name) as? JsonPrimitive)?.takeIf { it.isString }?.content
        ?: throw IllegalArgumentException("transport string is invalid")

private fun JsonObject.requiredLong(name: String): Long =
    (getValue(name) as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toLongOrNull()
        ?: throw IllegalArgumentException("transport integer is invalid")

private fun JsonObject.optionalLong(name: String): Long? {
    val value = this[name] ?: return null
    if (value is JsonNull) return null
    return (value as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toLongOrNull()
        ?: throw IllegalArgumentException("transport integer is invalid")
}

private fun JsonObject.requiredInt(name: String): Int =
    requiredLong(name).takeIf { it in Int.MIN_VALUE..Int.MAX_VALUE }?.toInt()
        ?: throw IllegalArgumentException("transport integer is out of bounds")

private fun JsonObject.requiredBoolean(name: String): Boolean =
    (getValue(name) as? JsonPrimitive)?.takeIf { !it.isString }?.content?.let {
        when (it) {
            "true" -> true
            "false" -> false
            else -> null
        }
    } ?: throw IllegalArgumentException("transport boolean is invalid")
