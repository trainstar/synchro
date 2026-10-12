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
        val references: List<String>,
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
        val parameters: List<TypeShape>? = null,
        val result: TypeShape? = null,
    )

    @Test
    fun kotlinInspectionFacadeMatchesSharedContract() {
        val contract = Json { ignoreUnknownKeys = false }.decodeFromString<FacadeContract>(
            String(Files.readAllBytes(repositoryRoot().resolve("conformance/protocol/inspection-facade-v2.json"))),
        )
        assertEquals(2, contract.schemaVersion)
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
        val reachedReferences = sortedSetOf<String>()
        functions.forEach { function ->
            function.parameters.filter { it.kind == KParameter.Kind.VALUE }
                .forEach { collectModels(it.type, contract.references, reachableModels, reachedReferences) }
            collectModels(function.returnType, contract.references, reachableModels, reachedReferences)
        }
        assertEquals(contract.references.sorted(), reachedReferences.toList())
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

    /** Walks facade-owned models. The public client API owns each named reference. */
    private fun collectModels(
        type: KType,
        references: List<String>,
        result: MutableMap<String, KClass<*>>,
        reachedReferences: MutableSet<String>,
    ) {
        val classifier = type.classifier as? KClass<*> ?: error("facade type classifier is unavailable")
        if (classifier == List::class || classifier == Map::class || classifier.isFunction()) {
            type.arguments.forEach { collectModels(requireNotNull(it.type), references, result, reachedReferences) }
            return
        }
        if (classifier in setOf(String::class, Boolean::class, Int::class, Long::class, Unit::class)) return
        val name = requireNotNull(classifier.simpleName)
        if (name in references) {
            reachedReferences += name
            return
        }
        if (result.putIfAbsent(name, classifier) != null) return
        requireNotNull(classifier.primaryConstructor).parameters.forEach {
            collectModels(it.type, references, result, reachedReferences)
        }
    }

    private fun KClass<*>.isFunction(): Boolean = qualifiedName?.matches(Regex("kotlin\\.Function\\d+")) == true

    private fun KType.toShape(): TypeShape {
        val classifier = classifier as? KClass<*> ?: error("facade type classifier is unavailable")
        return when (classifier) {
            Map::class -> {
                require(arguments.first().type?.classifier == String::class)
                TypeShape("string-map", isMarkedNullable, element = requireNotNull(arguments.last().type).toShape())
            }
            List::class -> TypeShape(
                name = "array",
                nullable = isMarkedNullable,
                element = requireNotNull(arguments.single().type).toShape(),
            )
            String::class -> TypeShape("string", isMarkedNullable)
            Boolean::class -> TypeShape("bool", isMarkedNullable)
            Int::class -> TypeShape("int", isMarkedNullable)
            Long::class -> TypeShape("int64", isMarkedNullable)
            Unit::class -> TypeShape("void", isMarkedNullable)
            else -> if (classifier.isFunction()) {
                val shapes = arguments.map { requireNotNull(it.type).toShape() }
                TypeShape("function", isMarkedNullable, parameters = shapes.dropLast(1), result = shapes.last())
            } else {
                TypeShape(requireNotNull(classifier.simpleName), isMarkedNullable)
            }
        }
    }

    private fun repositoryRoot(): Path {
        var current: Path? = Paths.get("").toAbsolutePath().normalize()
        repeat(8) {
            if (Files.exists(current!!.resolve("conformance/protocol/inspection-facade-v2.json"))) return current!!
            current = current!!.parent
        }
        error("repository root was not found")
    }
}
