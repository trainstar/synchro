package com.trainstar.synchro.conformance

import android.net.LocalServerSocket
import android.os.Bundle
import android.os.Process
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Test
import org.junit.runner.RunWith
import java.io.BufferedInputStream
import java.io.BufferedOutputStream

@RunWith(AndroidJUnit4::class)
class NativeSessionInstrumentationTest {
    @Test
    fun serveHostControlledSession() {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val socketName = InstrumentationRegistry.getArguments()
            .requiredArgument("synchro.native.socket")
        require(SOCKET_NAME.matches(socketName)) { "synchro.native.socket is invalid" }

        val server = LocalServerSocket(socketName)
        instrumentation.sendStatus(
            0,
            Bundle().apply {
                putString("synchro.native.ready", "true")
                putString("synchro.native.process-id", Process.myPid().toString())
            },
        )
        NativeSession(instrumentation.targetContext).use { session ->
            // LocalServerSocket declares no Closeable interface on every
            // supported Android version, so the server closes explicitly rather
            // than through a Closeable scope.
            try {
                val connection = server.accept()
                connection.use { socket ->
                    val input = BufferedInputStream(socket.inputStream)
                    val output = BufferedOutputStream(socket.outputStream)
                    while (true) {
                        val command = readBoundedLine(input) ?: break
                        val response = session.execute(command)
                        val bytes = response.toByteArray(Charsets.UTF_8)
                        check(bytes.size <= NativeSession.MAXIMUM_RESPONSE_BYTES) { "response is too large" }
                        output.write(bytes)
                        output.write('\n'.code)
                        output.flush()
                    }
                }
            } finally {
                server.close()
            }
        }
    }

    private fun readBoundedLine(input: BufferedInputStream): String? {
        val bytes = ArrayList<Byte>()
        while (bytes.size <= MAXIMUM_LINE_BYTES) {
            val value = input.read()
            if (value < 0) return if (bytes.isEmpty()) null else throw IllegalArgumentException("command line is incomplete")
            if (value == '\n'.code) {
                if (bytes.lastOrNull() == '\r'.code.toByte()) bytes.removeAt(bytes.lastIndex)
                return bytes.toByteArray().toString(Charsets.UTF_8)
            }
            bytes.add(value.toByte())
        }
        throw IllegalArgumentException("command line is too large")
    }

    private companion object {
        const val MAXIMUM_LINE_BYTES = 1 shl 20
        val SOCKET_NAME = Regex("[A-Za-z0-9._-]{1,96}")
    }
}

private fun Bundle.requiredArgument(name: String): String =
    getString(name)?.takeIf(String::isNotBlank)
        ?: throw IllegalArgumentException("$name is required")
