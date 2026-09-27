package com.trainstar.synchro

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Test

class PushLimitsTests {
    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        ignoreUnknownKeys = false
        encodeDefaults = true
        explicitNulls = false
    }

    private val update = Mutation(
        mutationID = "0f8fad5b-d9cb-469f-a165-70867728950e",
        table = "t_notes",
        op = Operation.UPDATE,
        pk = JsonObject(mapOf("f_id" to JsonPrimitive("7c9e6679-7425-40de-944b-e07fc1f90ae7"))),
        authoredSchema = SchemaRef(3, "a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31"),
        baseVersion = "v:42",
        clientVersion = "2026-09-27T10:00:00.000000Z",
        columns = Json.parseToJsonElement(
            """{"f_score":1.5,"f_none":null,"f_flag":true,"f_count":7,"f_body":"a/b"}""",
        ).jsonObject,
    )

    private fun normalized(mutation: Mutation): String = Integrity.canonicalJSON(
        PushLimits.normalizedMutation(Json.parseToJsonElement(json.encodeToString(mutation)).jsonObject),
    )

    @Test
    fun normalizedMutationMatchesTheServerForm() {
        val expected = "[\"mutation-v1\",\"0f8fad5b-d9cb-469f-a165-70867728950e\",\"t_notes\"," +
            "[\"f_id\",\"7c9e6679-7425-40de-944b-e07fc1f90ae7\"]," +
            "[\"3\",\"a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31\"]," +
            "\"update\",[1,\"v:42\"],\"2026-09-27T10:00:00.000000Z\"," +
            "[1,[[\"f_body\",\"a/b\"],[\"f_count\",7],[\"f_flag\",true],[\"f_none\",null],[\"f_score\",1.5]]]]"

        assertEquals(expected, normalized(update))
        val size = PushLimits.mutation(json, update)
        assertEquals(320, size.normalized)
        assertEquals(5, size.authoredColumns)
    }

    @Test
    fun normalizedMutationMarksAnAbsentBaseAndAbsentColumns() {
        val prefix = "[\"mutation-v1\",\"0f8fad5b-d9cb-469f-a165-70867728950e\",\"t_notes\"," +
            "[\"f_id\",\"7c9e6679-7425-40de-944b-e07fc1f90ae7\"]," +
            "[\"3\",\"a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31\"],"
        val insert = update.copy(op = Operation.INSERT, baseVersion = null)
        val delete = update.copy(op = Operation.DELETE, columns = null)

        assertEquals(
            prefix + "\"insert\",[0],\"2026-09-27T10:00:00.000000Z\"," +
                "[1,[[\"f_body\",\"a/b\"],[\"f_count\",7],[\"f_flag\",true],[\"f_none\",null],[\"f_score\",1.5]]]]",
            normalized(insert),
        )
        assertEquals(prefix + "\"delete\",[1,\"v:42\"],\"2026-09-27T10:00:00.000000Z\",[0]]", normalized(delete))
        assertEquals(0, PushLimits.mutation(json, delete).authoredColumns)
    }
}
