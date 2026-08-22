package com.trainstar.synchro.rn

import org.json.JSONArray
import org.json.JSONObject
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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** A bounded loopback forwarder used for real transport observation and pauses. */
internal class TransportObservationProxy(
    private val upstreamBaseURL: String,
    private val capacity: Int,
) : Closeable {
    private val server = ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
    private val accepting = AtomicBoolean(true)
    private val workers = Executors.newFixedThreadPool(16)
    private val observations = mutableListOf<JSONObject>()
    private val observationLock = Any()
    private val barrier = Object()
    private var sequence = 0L
    private var overflowed = false
    private var armedOperation: String? = null
    private var nextOperation: String? = null
    private var reached = false
    private var resumed = false
    private val acceptThread = Thread(::acceptLoop, "synchro-rn-transport-forwarder").apply {
        isDaemon = true
        start()
    }

    val localURL: String get() = "http://127.0.0.1:${server.localPort}"

    fun arm(operation: String) {
        requireOperation(operation)
        synchronized(barrier) {
            when {
                armedOperation == null -> armedOperation = operation
                reached && nextOperation == null -> nextOperation = operation
                else -> error("a transport pause is already armed")
            }
        }
    }

    fun await(operation: String, timeoutMillis: Long) {
        requireOperation(operation)
        require(timeoutMillis in 1..60_000) { "transport pause timeout is invalid" }
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        synchronized(barrier) {
            check(armedOperation == operation) { "transport pause operation does not match" }
            while (!reached) {
                val remaining = deadline - System.nanoTime()
                check(remaining > 0) { "transport pause timed out" }
                TimeUnit.NANOSECONDS.timedWait(barrier, remaining)
            }
        }
    }

    fun resume() {
        synchronized(barrier) {
            check(armedOperation != null && reached && !resumed) { "transport is not paused" }
            resumed = true
            barrier.notifyAll()
        }
    }

    fun snapshot(): String = synchronized(observationLock) {
        JSONObject().apply {
            put("observations", JSONArray(observations.map { JSONObject(it.toString()) }))
            put("overflowed", overflowed)
            put("sequence_checkpoint", sequence)
        }.toString()
    }

    override fun close() {
        if (!accepting.compareAndSet(true, false)) return
        synchronized(barrier) {
            resumed = true
            barrier.notifyAll()
        }
        runCatching { server.close() }
        acceptThread.join(1_000)
        workers.shutdownNow()
        workers.awaitTermination(1, TimeUnit.SECONDS)
    }

    private fun acceptLoop() {
        while (accepting.get()) {
            val socket = try {
                server.accept()
            } catch (_: Exception) {
                return
            }
            workers.execute { handle(socket) }
        }
    }

    private fun handle(socket: Socket) {
        val started = System.nanoTime()
        var operation = "other"
        var status = 0
        try {
            socket.use { downstream ->
                downstream.soTimeout = 70_000
                val input = BufferedInputStream(downstream.getInputStream())
                val output = BufferedOutputStream(downstream.getOutputStream())
                val requestLine = readLine(input, 8_192) ?: error("request line is absent")
                val parts = requestLine.split(' ')
                require(parts.size == 3 && parts[2] == "HTTP/1.1") { "request line is invalid" }
                require(parts[0] == "GET" || parts[0] == "POST") { "request method is invalid" }
                val target = parts[1]
                require(target.startsWith('/') && !target.startsWith("//")) { "request target is invalid" }
                val headers = readHeaders(input)
                val length = headers["content-length"]?.singleOrNull()?.toIntOrNull() ?: 0
                require(length in 0..MAXIMUM_REQUEST_BYTES && headers["transfer-encoding"] == null) {
                    "request body is invalid"
                }
                val body = ByteArray(length)
                var offset = 0
                while (offset < body.size) {
                    val count = input.read(body, offset, body.size - offset)
                    check(count >= 0) { "request body is incomplete" }
                    offset += count
                }
                operation = operationFor(target)
                val requestFacts = requestFacts(operation, body)
                val response = forward(parts[0], target, headers, body)
                status = response.status
                val responseFacts = if (status == 200) {
                    responseFacts(operation, response.body, requestFacts?.scope)
                } else {
                    null
                }
                appendObservation(
                    operation,
                    status,
                    (System.nanoTime() - started).coerceAtLeast(1),
                    requestFacts,
                    responseFacts,
                )
                pauseIfArmed(operation)
                writeResponse(output, response)
            }
        } catch (_: Exception) {
            runCatching { socket.close() }
        }
    }

    private fun appendObservation(
        operation: String,
        status: Int,
        duration: Long,
        request: RequestFacts?,
        response: ResponseFacts?,
    ) {
        synchronized(observationLock) {
            sequence += 1
            if (observations.size >= capacity) {
                overflowed = true
                return
            }
            observations += JSONObject().apply {
                put("sequence", sequence)
                put("operation_class", operation)
                put("status_code", status)
                put("duration_nanoseconds", duration)
                request?.cursorFingerprints?.let {
                    put("cursor_fingerprints", JSONArray(it))
                    put("cursor_fingerprints_complete", request.cursorFingerprintsComplete)
                }
                request?.value?.let { put("request_facts", it) }
                response?.rebuild?.let { put("rebuild_response_facts", it) }
                response?.pull?.let { put("pull_response_facts", it) }
            }
        }
    }

    private fun pauseIfArmed(operation: String) {
        synchronized(barrier) {
            if (armedOperation != operation || reached) return
            reached = true
            barrier.notifyAll()
            while (!resumed && accepting.get()) barrier.wait()
            armedOperation = nextOperation
            nextOperation = null
            reached = false
            resumed = false
        }
    }

    private fun forward(
        method: String,
        target: String,
        headers: Map<String, List<String>>,
        body: ByteArray,
    ): Response {
        val connection = URL(upstreamBaseURL.trimEnd('/') + target).openConnection() as HttpURLConnection
        try {
            connection.instanceFollowRedirects = false
            connection.connectTimeout = 30_000
            connection.readTimeout = 60_000
            connection.requestMethod = method
            connection.setRequestProperty("Accept-Encoding", "identity")
            headers.forEach { (name, values) ->
                if (name !in HOP_REQUEST_HEADERS) values.forEach { connection.addRequestProperty(name, it) }
            }
            if (method == "POST") {
                connection.doOutput = true
                connection.setFixedLengthStreamingMode(body.size)
                connection.outputStream.use { it.write(body) }
            }
            val status = connection.responseCode
            val responseBody = (if (status >= 400) connection.errorStream else connection.inputStream)
                ?.use { readBounded(it, MAXIMUM_RESPONSE_BYTES) } ?: ByteArray(0)
            val responseHeaders = connection.headerFields
                .filterKeys { it != null && it.lowercase(Locale.US) !in HOP_RESPONSE_HEADERS }
                .mapKeys { it.key!! }
                .mapValues { it.value.orEmpty().filterNotNull() }
            return Response(status, connection.responseMessage ?: "", responseHeaders, responseBody)
        } finally {
            connection.disconnect()
        }
    }

    private fun writeResponse(output: BufferedOutputStream, response: Response) {
        val reason = response.reason.replace("\r", "").replace("\n", "")
        output.write("HTTP/1.1 ${response.status} $reason\r\n".toByteArray(Charsets.US_ASCII))
        response.headers.forEach { (name, values) ->
            values.forEach { value ->
                output.write("$name: ${value.replace("\r", "").replace("\n", "")}\r\n".toByteArray(Charsets.ISO_8859_1))
            }
        }
        output.write("Content-Length: ${response.body.size}\r\nConnection: close\r\n\r\n".toByteArray(Charsets.US_ASCII))
        output.write(response.body)
        output.flush()
    }

    private fun requestFacts(operation: String, body: ByteArray): RequestFacts? {
        if (operation !in setOf("connect", "pull", "rebuild")) return null
        val request = JSONObject(body.toString(Charsets.UTF_8))
        val schema = request.getJSONObject("schema")
        val facts = JSONObject().apply {
            put("schema_version", schema.getLong("version"))
            put("schema_hash", schema.getString("hash"))
            if (request.has("client_generation") && !request.isNull("client_generation")) {
                put("client_generation", request.getLong("client_generation"))
            }
        }
        var scope: String? = null
        var cursors: List<String>? = null
        var complete: Boolean? = null
        when (operation) {
            "connect" -> {
                facts.put("protocol_version", request.getInt("protocol_version"))
                facts.put("scope_set_version", request.getLong("scope_set_version"))
                facts.put("scope_count", request.getJSONObject("known_scopes").length())
            }
            "pull" -> {
                facts.put("scope_set_version", request.getLong("scope_set_version"))
                val scopes = request.getJSONObject("scopes")
                facts.put("scope_count", scopes.length())
                facts.put("limit", request.getInt("limit"))
                cursors = scopes.keys().asSequence().mapNotNull { key ->
                    scopes.getJSONObject(key).optString("cursor").takeIf { it.isNotEmpty() }?.let(::fingerprint)
                }.distinct().sorted().toList()
                complete = cursors.size <= 16
                cursors = cursors.take(16)
            }
            "rebuild" -> {
                scope = request.getString("scope")
                facts.put("limit", request.getInt("limit"))
                facts.put("rebuild_id_fingerprint", fingerprint(request.getString("rebuild_id")))
                val present = request.has("cursor") && !request.isNull("cursor")
                facts.put("cursor_present", present)
                if (present) facts.put("cursor_fingerprint", fingerprint(request.getString("cursor")))
            }
        }
        return RequestFacts(facts, scope, cursors, complete)
    }

    private fun responseFacts(operation: String, body: ByteArray, requestedScope: String?): ResponseFacts? {
        if (operation !in setOf("pull", "rebuild")) return null
        val response = JSONObject(body.toString(Charsets.UTF_8))
        return if (operation == "pull") {
            ResponseFacts(pull = JSONObject().apply {
                put("change_count", response.getJSONArray("changes").length())
                put("has_more", response.getBoolean("has_more"))
                put("rebuild_scope_count", response.getJSONArray("rebuild").length())
                put("checksum_count", response.optJSONObject("checksums")?.length() ?: 0)
            })
        } else {
            ResponseFacts(rebuild = JSONObject().apply {
                put("record_count", response.getJSONArray("records").length())
                put("has_more", response.getBoolean("has_more"))
                put("has_cursor", response.has("cursor") && !response.isNull("cursor"))
                put("has_final_scope_cursor", response.has("final_scope_cursor") && !response.isNull("final_scope_cursor"))
                put("has_checksum", response.has("checksum") && !response.isNull("checksum"))
                put("scope_matches_request", response.getString("scope") == requestedScope)
            })
        }
    }

    private fun readHeaders(input: BufferedInputStream): Map<String, List<String>> {
        val result = linkedMapOf<String, MutableList<String>>()
        var total = 0
        repeat(64) {
            val line = readLine(input, 8_192) ?: error("headers are incomplete")
            total += line.length
            require(total <= 65_536) { "headers are too large" }
            if (line.isEmpty()) return result
            val separator = line.indexOf(':')
            require(separator > 0) { "header is invalid" }
            val name = line.substring(0, separator).trim().lowercase(Locale.US)
            val value = line.substring(separator + 1).trim()
            result.getOrPut(name) { mutableListOf() }.add(value)
        }
        error("too many headers")
    }

    private fun readLine(input: BufferedInputStream, maximum: Int): String? {
        val output = ByteArrayOutputStream()
        while (output.size() <= maximum) {
            val value = input.read()
            if (value < 0) return if (output.size() == 0) null else error("line is incomplete")
            if (value == '\n'.code) {
                val data = output.toByteArray()
                require(data.isNotEmpty() && data.last() == '\r'.code.toByte()) { "line ending is invalid" }
                return data.copyOf(data.size - 1).toString(Charsets.ISO_8859_1)
            }
            output.write(value)
        }
        error("line is too large")
    }

    private fun readBounded(input: InputStream, maximum: Int): ByteArray {
        val output = ByteArrayOutputStream()
        val buffer = ByteArray(8_192)
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            require(output.size() <= maximum - count) { "response is too large" }
            output.write(buffer, 0, count)
        }
        return output.toByteArray()
    }

    private fun operationFor(target: String): String = when {
        target.substringBefore('?').endsWith("/sync/connect") -> "connect"
        target.substringBefore('?').endsWith("/sync/pull") -> "pull"
        target.substringBefore('?').endsWith("/sync/push") -> "push"
        target.substringBefore('?').endsWith("/sync/rebuild") -> "rebuild"
        target.substringBefore('?').endsWith("/sync/checkpoint") -> "checkpoint"
        target.substringBefore('?').endsWith("/sync/schema") -> "schemas"
        else -> "other"
    }

    private fun requireOperation(value: String) {
        require(value in OPERATIONS) { "transport operation is invalid" }
    }

    private fun fingerprint(value: String): String = MessageDigest.getInstance("SHA-256")
        .digest(value.toByteArray(Charsets.UTF_8))
        .joinToString("") { "%02x".format(Locale.US, it.toInt() and 0xff) }

    private data class Response(val status: Int, val reason: String, val headers: Map<String, List<String>>, val body: ByteArray)
    private data class RequestFacts(
        val value: JSONObject,
        val scope: String?,
        val cursorFingerprints: List<String>?,
        val cursorFingerprintsComplete: Boolean?,
    )
    private data class ResponseFacts(val rebuild: JSONObject? = null, val pull: JSONObject? = null)

    private companion object {
        const val MAXIMUM_REQUEST_BYTES = 1 shl 20
        const val MAXIMUM_RESPONSE_BYTES = 8 shl 20
        val OPERATIONS = setOf("connect", "pull", "push", "checkpoint", "schemas", "rebuild", "other")
        val HOP_REQUEST_HEADERS = setOf("host", "connection", "content-length", "transfer-encoding", "accept-encoding")
        val HOP_RESPONSE_HEADERS = setOf("connection", "content-length", "transfer-encoding")
    }
}
