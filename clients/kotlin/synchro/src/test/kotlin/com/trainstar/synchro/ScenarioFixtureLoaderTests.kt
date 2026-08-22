package com.trainstar.synchro

import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class ScenarioFixtureLoaderTests {
    @Test
    fun loadsAuthoredScenarioByCatalogID() {
        val scenario = ScenarioFixtureLoader.load("SCN-SCHEMA-QUEUED-MUTATION-001")

        assertEquals("SCN-SCHEMA-QUEUED-MUTATION-001", scenario.getValue("id").jsonPrimitive.content)
        assertEquals(2, scenario.getValue("schema_version").jsonPrimitive.content.toInt())
        assertEquals(
            listOf("server-black-box", "native-e2e", "fault-injection", "negative-control"),
            scenario.getValue("proof_types").jsonArray.map { it.jsonPrimitive.content }
        )
    }

    @Test
    fun rejectsUnknownScenarioID() {
        assertThrows(IllegalStateException::class.java) {
            ScenarioFixtureLoader.load("SCN-NOT-AUTHORED-001")
        }
    }
}
