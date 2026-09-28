package com.trainstar.synchro

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonArray
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Measures push requests against the server push limits.
 *
 * The body form is the exact JSON text that the client stores and sends.
 * The canonical form is the RFC 8785 text of the parsed body.
 */
internal object PushLimits {
    const val MAX_REQUEST_OCTETS = 1_048_576
    const val MAX_NORMALIZED_MUTATION_OCTETS = 65_536
    const val MAX_AUTHORED_COLUMNS = 256
    const val MAX_PROTOCOL_INTEGER = 9_007_199_254_740_991L

    /** The octets of a request in the body form and in the canonical form. */
    data class RequestSize(val body: Long, val canonical: Long, val mutationCount: Int) {
        val withinLimit: Boolean
            get() = body <= MAX_REQUEST_OCTETS && canonical <= MAX_REQUEST_OCTETS

        /** Adds one mutation element and its separator to the request. */
        fun adding(mutation: MutationSize): RequestSize {
            val separator = if (mutationCount == 0) 0 else 1
            return RequestSize(
                body = body + mutation.body + separator,
                canonical = canonical + mutation.canonical + separator,
                mutationCount = mutationCount + 1,
            )
        }
    }

    /** The measures of one mutation element. */
    data class MutationSize(
        val body: Int,
        val canonical: Int,
        val normalized: Int,
        val authoredColumns: Int,
    ) {
        val withinLimits: Boolean
            get() = authoredColumns <= MAX_AUTHORED_COLUMNS && normalized <= MAX_NORMALIZED_MUTATION_OCTETS
    }

    /** Measures the request encoding with an empty mutations array. */
    fun envelope(json: Json, request: PushRequest): RequestSize {
        val body = json.encodeToString(request.copy(mutations = emptyList()))
        val canonical = Integrity.canonicalJSON(Json.parseToJsonElement(body))
        return RequestSize(octets(body).toLong(), octets(canonical).toLong(), mutationCount = 0)
    }

    /**
     * Measures the envelope of a new batch. The client generation and the
     * schema version are the protocol maximum. This reserve keeps a renewed
     * successor in the request limit.
     */
    fun reservedEnvelope(json: Json, clientID: String, batchID: String, schemaHash: String, atomic: Boolean): RequestSize =
        envelope(
            json,
            PushRequest(
                clientID = clientID,
                clientGeneration = MAX_PROTOCOL_INTEGER,
                batchID = batchID,
                schema = SchemaRef(MAX_PROTOCOL_INTEGER, schemaHash),
                mutations = emptyList(),
                atomic = atomic.takeIf { it },
            ),
        )

    fun mutation(json: Json, mutation: Mutation): MutationSize {
        val body = json.encodeToString(mutation)
        val parsed = Json.parseToJsonElement(body).jsonObject
        return MutationSize(
            body = octets(body),
            canonical = octets(Integrity.canonicalJSON(parsed)),
            normalized = octets(Integrity.canonicalJSON(normalizedMutation(parsed))),
            authoredColumns = parsed["columns"]?.jsonObject?.size ?: 0,
        )
    }

    /** Builds the server normalized mutation array from one parsed mutation element. */
    fun normalizedMutation(mutation: JsonObject): JsonArray {
        val (pkFieldID, pkValue) = mutation.getValue("pk").jsonObject.entries.single()
        val authoredSchema = mutation.getValue("authored_schema").jsonObject
        val baseVersion = mutation["base_version"]
        val columns = mutation["columns"]?.jsonObject
        return buildJsonArray {
            add("mutation-v1")
            add(mutation.getValue("mutation_id"))
            add(mutation.getValue("table"))
            addJsonArray {
                add(pkFieldID)
                add(pkValue)
            }
            addJsonArray {
                add(authoredSchema.getValue("version").jsonPrimitive.content)
                add(authoredSchema.getValue("hash"))
            }
            add(mutation.getValue("op"))
            addJsonArray {
                if (baseVersion == null) {
                    add(0)
                } else {
                    add(1)
                    add(baseVersion)
                }
            }
            add(mutation.getValue("client_version"))
            addJsonArray {
                if (columns == null) {
                    add(0)
                } else {
                    add(1)
                    addJsonArray {
                        columns.entries
                            .sortedWith { left, right ->
                                Integrity.compareUnsigned(left.key.toByteArray(), right.key.toByteArray())
                            }
                            .forEach { (fieldID, value) ->
                                addJsonArray {
                                    add(fieldID)
                                    add(value)
                                }
                            }
                    }
                }
            }
        }
    }

    private fun octets(text: String): Int = text.toByteArray(Charsets.UTF_8).size
}
