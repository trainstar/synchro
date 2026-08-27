@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import com.trainstar.synchro.inspection.SynchroInspection
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.KType
import kotlin.reflect.KVisibility
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.primaryConstructor
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Test

class InspectionFacadeContractTests {
    @Serializable
    private data class FacadeContract(
        @SerialName("schema_version") val schemaVersion: Int,
        val facade: String,
        val operations: List<Operation>,
        val models: List<Model>,
    )

    @Serializable
    private data class Operation(
        val name: String,
        val parameters: List<Member>,
        val result: TypeShape,
    )

    @Serializable
    private data class Model(
        val name: String,
        val fields: List<Member>,
    )

    @Serializable
    private data class Member(
        val name: String,
        val type: TypeShape,
    )

    @Serializable
    private data class TypeShape(
        val name: String,
        val nullable: Boolean,
        val element: TypeShape? = null,
    )

    @Test
    fun kotlinInspectionFacadeMatchesSharedContract() {
        val contract = Json { ignoreUnknownKeys = false }.decodeFromString<FacadeContract>(
            String(Files.readAllBytes(repositoryRoot().resolve("conformance/protocol/inspection-facade-v1.json"))),
        )
        assertEquals(1, contract.schemaVersion)
        assertEquals(SynchroInspection::class.simpleName, contract.facade)

        val functions = SynchroInspection::class.declaredMemberFunctions
            .filter { it.visibility == KVisibility.PUBLIC }
        val actualOperations = functions.map { function ->
            Operation(
                name = function.name,
                parameters = function.parameters
                    .filter { it.kind == KParameter.Kind.VALUE }
                    .map { Member(requireNotNull(it.name), it.type.toShape()) },
                result = function.returnType.toShape(),
            )
        }.sortedBy(Operation::name)
        assertEquals(contract.operations.sortedBy(Operation::name), actualOperations)

        val reachableModels = linkedMapOf<String, KClass<*>>()
        functions.forEach { function ->
            function.parameters.filter { it.kind == KParameter.Kind.VALUE }
                .forEach { collectModels(it.type, reachableModels) }
            collectModels(function.returnType, reachableModels)
        }
        val actualModels = reachableModels.values.map { model ->
            Model(
                name = requireNotNull(model.simpleName),
                fields = requireNotNull(model.primaryConstructor).parameters.map {
                    Member(requireNotNull(it.name), it.type.toShape())
                },
            )
        }.sortedBy(Model::name)
        assertEquals(contract.models.sortedBy(Model::name), actualModels)
    }

    private fun collectModels(type: KType, result: MutableMap<String, KClass<*>>) {
        val classifier = type.classifier as? KClass<*> ?: error("facade type classifier is unavailable")
        if (classifier == List::class) {
            collectModels(requireNotNull(type.arguments.single().type), result)
            return
        }
        if (classifier in setOf(String::class, Boolean::class, Int::class, Long::class)) return
        val name = requireNotNull(classifier.simpleName)
        if (result.putIfAbsent(name, classifier) != null) return
        requireNotNull(classifier.primaryConstructor).parameters.forEach { collectModels(it.type, result) }
    }

    private fun KType.toShape(): TypeShape {
        val classifier = classifier as? KClass<*> ?: error("facade type classifier is unavailable")
        return when (classifier) {
            List::class -> TypeShape(
                name = "array",
                nullable = isMarkedNullable,
                element = requireNotNull(arguments.single().type).toShape(),
            )
            String::class -> TypeShape("string", isMarkedNullable)
            Boolean::class -> TypeShape("bool", isMarkedNullable)
            Int::class -> TypeShape("int", isMarkedNullable)
            Long::class -> TypeShape("int64", isMarkedNullable)
            else -> TypeShape(requireNotNull(classifier.simpleName), isMarkedNullable)
        }
    }

    private fun repositoryRoot(): Path {
        var current: Path? = Paths.get("").toAbsolutePath().normalize()
        repeat(8) {
            if (Files.exists(current!!.resolve("conformance/protocol/inspection-facade-v1.json"))) return current!!
            current = current!!.parent
        }
        error("repository root was not found")
    }
}
