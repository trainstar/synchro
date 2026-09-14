package com.trainstar.synchro

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest

internal object ScenarioFixtureLoader {
    @Serializable
    private data class Catalog(
        @kotlinx.serialization.SerialName("schema_version") val schemaVersion: Int,
        val scenarios: List<Entry>,
    )

    @Serializable
    private data class Entry(
        @kotlinx.serialization.SerialName("scenario_id") val scenarioID: String,
        val path: String,
        val sha256: String,
    )

    private val json = Json { ignoreUnknownKeys = false }

    fun load(id: String): JsonObject {
        val root = repositoryRoot()
        val catalog = json.decodeFromString<Catalog>(
            String(Files.readAllBytes(root.resolve("conformance/catalog.json")), Charsets.UTF_8)
        )
        check(catalog.schemaVersion == 1) { "unsupported scenario catalog schema version" }
        val entry = catalog.scenarios.singleOrNull { it.scenarioID == id }
            ?: error("scenario is not present in the catalog: $id")
        val resolvedRoot = root.toRealPath()
        val scenarioPath = root.resolve(entry.path).normalize().toRealPath()
        check(scenarioPath.startsWith(resolvedRoot)) { "scenario path escapes the repository: $id" }
        val data = Files.readAllBytes(scenarioPath)
        val digest = MessageDigest.getInstance("SHA-256").digest(data)
            .joinToString("") { "%02x".format(it.toInt() and 0xff) }
        check(digest == entry.sha256) { "scenario digest does not match the catalog: $id" }
        val scenario = json.parseToJsonElement(data.decodeToString()).jsonObject
        check(scenario["id"]?.jsonPrimitive?.content == id) {
            "scenario identity does not match the catalog: $id"
        }
        return scenario
    }

    private fun repositoryRoot(): Path {
        var current: Path? = Paths.get("").toAbsolutePath().normalize()
        repeat(8) {
            if (Files.exists(current!!.resolve("conformance/catalog.json"))) return current!!
            current = current!!.parent
        }
        error("repository root was not found")
    }
}
