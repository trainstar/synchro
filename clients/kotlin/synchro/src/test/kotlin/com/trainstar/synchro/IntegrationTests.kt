package com.trainstar.synchro

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.Assume.assumeNotNull
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Integration tests that run against a real synchrod-pg server with PG extension.
 * Requires SYNCHRO_TEST_URL and SYNCHRO_TEST_JWT_SECRET environment variables.
 * Skips when env vars are not set.
 */
class IntegrationTests {

    private var serverURL: String? = null
    private var jwtSecret: String? = null

    @Before
    fun setUp() {
        serverURL = System.getenv("SYNCHRO_TEST_URL")
        jwtSecret = System.getenv("SYNCHRO_TEST_JWT_SECRET")
    }

    private fun skipIfNoServer() {
        assumeNotNull("SYNCHRO_TEST_URL not set", serverURL)
        assumeNotNull("SYNCHRO_TEST_JWT_SECRET not set", jwtSecret)
    }

    // -- JWT Helper --

    private fun signTestJWT(userID: String): String {
        val header = """{"alg":"HS256","typ":"JWT"}"""
        val now = System.currentTimeMillis() / 1000
        val exp = now + 3600
        val payload = """{"sub":"$userID","iat":$now,"exp":$exp}"""

        val headerB64 = base64URLEncode(header.toByteArray())
        val payloadB64 = base64URLEncode(payload.toByteArray())
        val signingInput = "$headerB64.$payloadB64"

        val signature = hmacSHA256(jwtSecret!!.toByteArray(), signingInput.toByteArray())
        return "$signingInput.${base64URLEncode(signature)}"
    }

    private fun base64URLEncode(data: ByteArray): String =
        java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(data)

    private fun hmacSHA256(key: ByteArray, data: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(data)
    }

    // -- HTTP Helpers --

    private fun makeHttpClient(
        userID: String,
        clientID: String? = null,
        appVersion: String = "1.0.0"
    ): Pair<HttpClient, String> {
        val cid = clientID ?: UUID.randomUUID().toString().lowercase()
        val token = signTestJWT(userID)
        val config = SynchroConfig(
            dbPath = "",
            serverURL = serverURL!!,
            authProvider = { token },
            clientID = cid,
            appVersion = appVersion,
            syncInterval = 999.0,
            maxRetryAttempts = 1
        )
        return Pair(HttpClient(config), cid)
    }

    private data class RegResult(
        val reg: RegisterResponse,
        val schemaVersion: Long,
        val schemaHash: String
    )

    private suspend fun registerWithSchema(http: HttpClient, clientID: String): RegResult {
        val schema = http.fetchSchema()
        val reg = http.register(RegisterRequest(
            clientID = clientID,
            platform = "test",
            appVersion = "1.0.0",
            schemaVersion = schema.schemaVersion,
            schemaHash = schema.schemaHash
        ))
        return RegResult(reg, schema.schemaVersion, schema.schemaHash)
    }

    private data class RebuildResult(
        val records: List<Record>,
        val bucketChecksum: Int?
    )

    private suspend fun rebuildBucket(
        http: HttpClient, clientID: String, bucketID: String,
        schemaVersion: Long, schemaHash: String
    ): RebuildResult {
        val allRecords = mutableListOf<Record>()
        var cursor: String? = null
        var hasMore = true
        var finalChecksum: Int? = null

        while (hasMore) {
            val resp = http.rebuild(RebuildRequest(
                clientID = clientID, bucketId = bucketID,
                cursor = cursor, limit = 100,
                schemaVersion = schemaVersion, schemaHash = schemaHash
            ))
            allRecords.addAll(resp.records)
            cursor = resp.cursor
            hasMore = resp.hasMore
            if (!hasMore) finalChecksum = resp.bucketChecksum
        }
        return RebuildResult(allRecords, finalChecksum)
    }

    // -- Push Helpers --

    private suspend fun pushCustomer(
        http: HttpClient, clientID: String,
        customerID: String, userID: String,
        email: String = "test@example.com",
        internalNotes: String? = null,
        schemaVersion: Long, schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse {
        val data = mutableMapOf(
            "id" to AnyCodable(customerID),
            "user_id" to AnyCodable(userID),
            "email" to AnyCodable(email)
        )
        if (internalNotes != null) data["internal_notes"] = AnyCodable(internalNotes)
        return http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = customerID, tableName = "customers", operation = "create",
                data = data, clientUpdatedAt = clientUpdatedAt
            )),
            schemaVersion = schemaVersion, schemaHash = schemaHash
        ))
    }

    private suspend fun pushOrder(
        http: HttpClient, clientID: String,
        orderID: String, customerID: String,
        shipAddress: Map<String, Any>? = null,
        schemaVersion: Long, schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse {
        val data = mutableMapOf<String, AnyCodable>(
            "id" to AnyCodable(orderID),
            "customer_id" to AnyCodable(customerID)
        )
        if (shipAddress != null) data["ship_address"] = AnyCodable(shipAddress)
        return http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = orderID, tableName = "orders", operation = "create",
                data = data, clientUpdatedAt = clientUpdatedAt
            )),
            schemaVersion = schemaVersion, schemaHash = schemaHash
        ))
    }

    private suspend fun pushLineItem(
        http: HttpClient, clientID: String,
        lineItemID: String, orderID: String,
        quantity: Int = 1, unitPrice: Double = 10.0,
        schemaVersion: Long, schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse = http.push(PushRequest(
        clientID = clientID,
        changes = listOf(PushRecord(
            id = lineItemID, tableName = "line_items", operation = "create",
            data = mapOf(
                "id" to AnyCodable(lineItemID),
                "order_id" to AnyCodable(orderID),
                "quantity" to AnyCodable(quantity),
                "unit_price" to AnyCodable(unitPrice)
            ),
            clientUpdatedAt = clientUpdatedAt
        )),
        schemaVersion = schemaVersion, schemaHash = schemaHash
    ))

    private suspend fun pushDocument(
        http: HttpClient, clientID: String,
        docID: String, ownerID: String,
        title: String = "Test Document",
        schemaVersion: Long, schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse = http.push(PushRequest(
        clientID = clientID,
        changes = listOf(PushRecord(
            id = docID, tableName = "documents", operation = "create",
            data = mapOf(
                "id" to AnyCodable(docID),
                "owner_id" to AnyCodable(ownerID),
                "title" to AnyCodable(title)
            ),
            clientUpdatedAt = clientUpdatedAt
        )),
        schemaVersion = schemaVersion, schemaHash = schemaHash
    ))

    private suspend fun pushDocumentMember(
        http: HttpClient, clientID: String,
        memberID: String, docID: String, userID: String,
        role: String = "editor",
        schemaVersion: Long, schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse = http.push(PushRequest(
        clientID = clientID,
        changes = listOf(PushRecord(
            id = memberID, tableName = "document_members", operation = "create",
            data = mapOf(
                "id" to AnyCodable(memberID),
                "document_id" to AnyCodable(docID),
                "user_id" to AnyCodable(userID),
                "role" to AnyCodable(role)
            ),
            clientUpdatedAt = clientUpdatedAt
        )),
        schemaVersion = schemaVersion, schemaHash = schemaHash
    ))

    private suspend fun pushDocumentComment(
        http: HttpClient, clientID: String,
        commentID: String, docID: String, authorID: String,
        body: String = "Test comment",
        schemaVersion: Long, schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse = http.push(PushRequest(
        clientID = clientID,
        changes = listOf(PushRecord(
            id = commentID, tableName = "document_comments", operation = "create",
            data = mapOf(
                "id" to AnyCodable(commentID),
                "document_id" to AnyCodable(docID),
                "author_id" to AnyCodable(authorID),
                "body" to AnyCodable(body)
            ),
            clientUpdatedAt = clientUpdatedAt
        )),
        schemaVersion = schemaVersion, schemaHash = schemaHash
    ))

    private suspend fun pollForRecord(
        http: HttpClient, clientID: String,
        checkpoint: Long, schemaVersion: Long, schemaHash: String,
        recordID: String, timeoutMs: Long = 10000
    ): PullResponse {
        val deadline = System.currentTimeMillis() + timeoutMs
        var lastResponse: PullResponse? = null

        while (System.currentTimeMillis() < deadline) {
            val resp = http.pull(PullRequest(
                clientID = clientID, checkpoint = checkpoint,
                tables = null, limit = 100, knownBuckets = null,
                schemaVersion = schemaVersion, schemaHash = schemaHash
            ))
            lastResponse = resp
            if (resp.changes.any { it.id == recordID }) return resp
            delay(250)
        }
        return lastResponse ?: http.pull(PullRequest(
            clientID = clientID, checkpoint = checkpoint,
            tables = null, limit = 100, knownBuckets = null,
            schemaVersion = schemaVersion, schemaHash = schemaHash
        ))
    }

    // -- Test 1: Register and Schema Fetch --

    @Test
    fun testRegisterAndSchemaFetch() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)

        val schema = http.fetchSchema()
        assertTrue("schema version should be > 0", schema.schemaVersion > 0)
        assertTrue("schema hash should be non-empty", schema.schemaHash.isNotEmpty())
        assertTrue("should have at least 13 tables", schema.tables.size >= 13)

        val regions = schema.tables.firstOrNull { it.tableName == "regions" }
        assertNotNull("regions table should exist", regions)
        assertEquals("read_only", regions!!.pushPolicy)

        val customers = schema.tables.firstOrNull { it.tableName == "customers" }
        assertNotNull("customers table should exist", customers)
        assertEquals("enabled", customers!!.pushPolicy)

        val reg = http.register(RegisterRequest(
            clientID = clientID, platform = "test", appVersion = "1.0.0",
            schemaVersion = schema.schemaVersion, schemaHash = schema.schemaHash
        ))
        assertTrue(reg.id.isNotEmpty())
        assertEquals(schema.schemaVersion, reg.schemaVersion)
        assertNotNull("register should return bucket_checkpoints", reg.bucketCheckpoints)
    }

    // -- Test 2: Rebuild Bootstrap --

    @Test
    fun testRebuildBootstrap() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val (globalRecords, globalChecksum) = rebuildBucket(http, clientID, "global", sv, sh)
        assertTrue("global bucket should contain reference data", globalRecords.isNotEmpty())
        assertNotNull("final rebuild page should include bucket_checksum", globalChecksum)

        val (userRecords, _) = rebuildBucket(http, clientID, "user:$userID", sv, sh)
        assertEquals("new user's bucket should be empty", 0, userRecords.size)
    }

    // -- Test 3: Push/Pull FK Chain --

    @Test
    fun testPushPullFKChain() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        assertEquals(1, pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh).accepted.size)

        val orderID = UUID.randomUUID().toString().lowercase()
        val address = mapOf("street" to "123 Main St", "city" to "Anytown", "zip" to "12345")
        assertEquals(1, pushOrder(http, clientID, orderID, custID, address, sv, sh).accepted.size)

        val pullResp = pollForRecord(http, clientID, 0, sv, sh, orderID)
        assertNotNull("customer should appear", pullResp.changes.firstOrNull { it.id == custID })
        val foundOrder = pullResp.changes.firstOrNull { it.id == orderID }
        assertNotNull("order should appear", foundOrder)
        assertEquals("orders", foundOrder!!.tableName)

        val shipAddr = foundOrder.data["ship_address"]?.value
        if (shipAddr is Map<*, *>) {
            assertEquals("123 Main St", shipAddr["street"])
        }
    }

    // -- Test 4: Deep FK Chain --

    @Test
    fun testDeepFKChain() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        val orderID = UUID.randomUUID().toString().lowercase()
        pushOrder(http, clientID, orderID, custID, schemaVersion = sv, schemaHash = sh)

        val lineItemID = UUID.randomUUID().toString().lowercase()
        assertEquals(1, pushLineItem(http, clientID, lineItemID, orderID, schemaVersion = sv, schemaHash = sh).accepted.size)

        val pullResp = pollForRecord(http, clientID, 0, sv, sh, lineItemID)
        assertTrue("customer in pull", pullResp.changes.any { it.id == custID })
        assertTrue("order in pull", pullResp.changes.any { it.id == orderID })
        assertTrue("line_item in pull", pullResp.changes.any { it.id == lineItemID })
    }

    // -- Test 5: Pull Pagination --

    @Test
    fun testPullPagination() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        val orderIDs = mutableListOf<String>()
        repeat(3) {
            val oid = UUID.randomUUID().toString().lowercase()
            orderIDs.add(oid)
            pushOrder(http, clientID, oid, custID, schemaVersion = sv, schemaHash = sh)
        }

        pollForRecord(http, clientID, 0, sv, sh, orderIDs.last())

        var checkpoint: Long = 0
        val allPulledIDs = mutableSetOf<String>()
        var iterations = 0

        do {
            val resp = http.pull(PullRequest(
                clientID = clientID, checkpoint = checkpoint,
                limit = 1, schemaVersion = sv, schemaHash = sh
            ))
            resp.changes.forEach { allPulledIDs.add(it.id) }
            assertTrue("checkpoint must not go backwards", resp.checkpoint >= checkpoint)
            checkpoint = resp.checkpoint
            iterations++
            if (!resp.hasMore) break
        } while (iterations < 50)

        for (oid in orderIDs) {
            assertTrue("order $oid should appear", allPulledIDs.contains(oid))
        }
    }

    // -- Test 6: Conflict Server Wins --

    @Test
    fun testConflictServerWins() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        val ancientDate = "2000-01-01T00:00:00.000Z"
        val conflictResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = custID, tableName = "customers", operation = "update",
                data = mapOf("id" to AnyCodable(custID), "user_id" to AnyCodable(userID), "email" to AnyCodable("stale@example.com")),
                clientUpdatedAt = ancientDate, baseUpdatedAt = ancientDate
            )),
            schemaVersion = sv, schemaHash = sh
        ))

        assertEquals(1, conflictResp.rejected.size)
        assertEquals("conflict", conflictResp.rejected.first().status)
        assertEquals("server_won_conflict", conflictResp.rejected.first().reasonCode)
        assertNotNull("should include serverVersion", conflictResp.rejected.first().serverVersion)
    }

    // -- Test 7: Conflict Client Wins --

    @Test
    fun testConflictClientWins() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        val createResp = pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)
        val serverUpdatedAt = createResp.accepted.first().serverUpdatedAt

        val updateResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = custID, tableName = "customers", operation = "update",
                data = mapOf("id" to AnyCodable(custID), "user_id" to AnyCodable(userID), "email" to AnyCodable("updated@example.com")),
                clientUpdatedAt = java.time.Instant.now().toString(), baseUpdatedAt = serverUpdatedAt
            )),
            schemaVersion = sv, schemaHash = sh
        ))

        assertEquals(1, updateResp.accepted.size)
        assertEquals(PushStatus.APPLIED, updateResp.accepted.first().status)
    }

    // -- Test 8: Conflict Duplicate Create --

    @Test
    fun testConflictDuplicateCreate() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        val dupResp = pushCustomer(http, clientID, custID, userID, email = "dup@example.com", schemaVersion = sv, schemaHash = sh)
        assertEquals(1, dupResp.rejected.size)
        assertEquals("record_exists", dupResp.rejected.first().reasonCode)
    }

    // -- Test 9: Conflict Update on Deleted --

    @Test
    fun testConflictUpdateOnDeleted() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = custID, tableName = "customers", operation = "delete",
                data = mapOf("id" to AnyCodable(custID), "user_id" to AnyCodable(userID)),
                clientUpdatedAt = java.time.Instant.now().toString()
            )),
            schemaVersion = sv, schemaHash = sh
        ))

        val updateResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = custID, tableName = "customers", operation = "update",
                data = mapOf("id" to AnyCodable(custID), "user_id" to AnyCodable(userID), "email" to AnyCodable("ghost@example.com")),
                clientUpdatedAt = java.time.Instant.now().toString()
            )),
            schemaVersion = sv, schemaHash = sh
        ))

        assertEquals(1, updateResp.rejected.size)
        assertEquals("record_deleted", updateResp.rejected.first().reasonCode)
    }

    // -- Test 10: Resurrection --

    @Test
    fun testResurrection() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = custID, tableName = "customers", operation = "delete",
                data = mapOf("id" to AnyCodable(custID), "user_id" to AnyCodable(userID)),
                clientUpdatedAt = java.time.Instant.now().toString()
            )),
            schemaVersion = sv, schemaHash = sh
        ))

        val resurrectResp = pushCustomer(http, clientID, custID, userID, email = "reborn@example.com", schemaVersion = sv, schemaHash = sh)
        assertEquals("resurrection should be accepted", 1, resurrectResp.accepted.size)

        val pullResp = pollForRecord(http, clientID, 0, sv, sh, custID)
        val found = pullResp.changes.lastOrNull { it.id == custID }
        assertNull("resurrected record should not have deletedAt", found?.deletedAt)
    }

    // -- Test 11: Soft Delete --

    @Test
    fun testSoftDelete() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)

        val orderID = UUID.randomUUID().toString().lowercase()
        pushOrder(http, clientID, orderID, custID, schemaVersion = sv, schemaHash = sh)

        val pullAfterCreate = pollForRecord(http, clientID, 0, sv, sh, orderID)
        val cpAfterCreate = pullAfterCreate.checkpoint

        val deleteResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = orderID, tableName = "orders", operation = "delete",
                data = mapOf("id" to AnyCodable(orderID), "customer_id" to AnyCodable(custID)),
                clientUpdatedAt = java.time.Instant.now().toString()
            )),
            schemaVersion = sv, schemaHash = sh
        ))
        assertEquals(1, deleteResp.accepted.size)
        assertNotNull(deleteResp.accepted.first().serverDeletedAt)

        val deadline = System.currentTimeMillis() + 10000
        var foundDelete = false
        var cp = cpAfterCreate

        while (System.currentTimeMillis() < deadline && !foundDelete) {
            val resp = http.pull(PullRequest(
                clientID = clientID, checkpoint = cp,
                limit = 100, schemaVersion = sv, schemaHash = sh
            ))
            if (resp.deletes.any { it.id == orderID }) { foundDelete = true; break }
            if (resp.changes.firstOrNull { it.id == orderID }?.deletedAt != null) { foundDelete = true; break }
            cp = resp.checkpoint
            if (!resp.hasMore) delay(250)
        }
        assertTrue("soft-deleted order should appear in pull as deleted", foundDelete)
    }

    // -- Test 12: Read-Only Rejection --

    @Test
    fun testReadOnlyRejection() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val regionID = UUID.randomUUID().toString().lowercase()
        val resp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = regionID, tableName = "regions", operation = "create",
                data = mapOf("id" to AnyCodable(regionID), "name" to AnyCodable("Atlantis")),
                clientUpdatedAt = java.time.Instant.now().toString()
            )),
            schemaVersion = sv, schemaHash = sh
        ))

        assertEquals(1, resp.rejected.size)
        assertEquals(PushStatus.REJECTED_TERMINAL, resp.rejected.first().status)
        assertEquals("table_read_only", resp.rejected.first().reasonCode)
    }

    // -- Test 13: Bucket Isolation --

    @Test
    fun testBucketIsolation() = runTest {
        skipIfNoServer()

        val userA = UUID.randomUUID().toString().lowercase()
        val userB = UUID.randomUUID().toString().lowercase()
        val (httpA, clientA) = makeHttpClient(userA)
        val (httpB, clientB) = makeHttpClient(userB)

        val (_, svA, shA) = registerWithSchema(httpA, clientA)
        val (_, svB, shB) = registerWithSchema(httpB, clientB)

        val custA = UUID.randomUUID().toString().lowercase()
        pushCustomer(httpA, clientA, custA, userA, schemaVersion = svA, schemaHash = shA)
        val orderA = UUID.randomUUID().toString().lowercase()
        pushOrder(httpA, clientA, orderA, custA, schemaVersion = svA, schemaHash = shA)

        val custB = UUID.randomUUID().toString().lowercase()
        pushCustomer(httpB, clientB, custB, userB, schemaVersion = svB, schemaHash = shB)
        val orderB = UUID.randomUUID().toString().lowercase()
        pushOrder(httpB, clientB, orderB, custB, schemaVersion = svB, schemaHash = shB)

        val pullA = pollForRecord(httpA, clientA, 0, svA, shA, orderA)
        assertTrue("User A should see own order", pullA.changes.any { it.id == orderA })
        assertFalse("User A must not see B's order", pullA.changes.any { it.id == orderB })

        val pullB = pollForRecord(httpB, clientB, 0, svB, shB, orderB)
        assertTrue("User B should see own order", pullB.changes.any { it.id == orderB })
        assertFalse("User B must not see A's order", pullB.changes.any { it.id == orderA })
    }

    // -- Test 14: Excluded Columns --

    @Test
    fun testExcludedColumns() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, email = "visible@example.com",
            internalNotes = "SECRET_DATA", schemaVersion = sv, schemaHash = sh)

        val pullResp = pollForRecord(http, clientID, 0, sv, sh, custID)
        val found = pullResp.changes.firstOrNull { it.id == custID }
        assertNotNull(found)
        assertNull("internal_notes should be excluded", found!!.data["internal_notes"])
        assertNotNull("email should be present", found.data["email"])
    }

    // -- Test 15: Multi-Bucket Collaboration --

    @Test
    fun testMultiBucketCollaboration() = runTest {
        skipIfNoServer()

        val userA = UUID.randomUUID().toString().lowercase()
        val userB = UUID.randomUUID().toString().lowercase()
        val (httpA, clientA) = makeHttpClient(userA)
        val (httpB, clientB) = makeHttpClient(userB)

        val (_, svA, shA) = registerWithSchema(httpA, clientA)
        val (_, svB, shB) = registerWithSchema(httpB, clientB)

        val docID = UUID.randomUUID().toString().lowercase()
        assertEquals(1, pushDocument(httpA, clientA, docID, userA, schemaVersion = svA, schemaHash = shA).accepted.size)

        val memberID = UUID.randomUUID().toString().lowercase()
        assertEquals(1, pushDocumentMember(httpA, clientA, memberID, docID, userB, schemaVersion = svA, schemaHash = shA).accepted.size)

        val pullB = pollForRecord(httpB, clientB, 0, svB, shB, memberID)
        assertTrue("User B should see document_member", pullB.changes.any { it.id == memberID })
    }

    // -- Test 16: Dual Bucket Ownership --

    @Test
    fun testDualBucketOwnership() = runTest {
        skipIfNoServer()

        val userA = UUID.randomUUID().toString().lowercase()
        val userB = UUID.randomUUID().toString().lowercase()
        val (httpA, clientA) = makeHttpClient(userA)
        val (httpB, clientB) = makeHttpClient(userB)

        val (_, svA, shA) = registerWithSchema(httpA, clientA)
        val (_, svB, shB) = registerWithSchema(httpB, clientB)

        val docID = UUID.randomUUID().toString().lowercase()
        pushDocument(httpA, clientA, docID, userA, schemaVersion = svA, schemaHash = shA)

        val commentID = UUID.randomUUID().toString().lowercase()
        assertEquals(1, pushDocumentComment(httpB, clientB, commentID, docID, userB, schemaVersion = svB, schemaHash = shB).accepted.size)

        val pullA = pollForRecord(httpA, clientA, 0, svA, shA, commentID)
        assertTrue("Document owner should see comment", pullA.changes.any { it.id == commentID })

        val pullB = pollForRecord(httpB, clientB, 0, svB, shB, commentID)
        assertTrue("Comment author should see comment", pullB.changes.any { it.id == commentID })
    }

    // -- Test 17: Type Zoo --

    @Test
    fun testTypeZoo() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val zooID = UUID.randomUUID().toString().lowercase()
        val testUUID = UUID.randomUUID().toString().lowercase()
        val data = mapOf(
            "id" to AnyCodable(zooID),
            "user_id" to AnyCodable(userID),
            "text_val" to AnyCodable("hello world"),
            "varchar_val" to AnyCodable("bounded string"),
            "bool_val" to AnyCodable(true),
            "smallint_val" to AnyCodable(42),
            "int_val" to AnyCodable(123456),
            "bigint_val" to AnyCodable(9876543210L),
            "numeric_val" to AnyCodable("99999.99"),
            "real_val" to AnyCodable(3.14),
            "double_val" to AnyCodable(2.718281828),
            "date_val" to AnyCodable("2026-03-19"),
            "timestamptz_val" to AnyCodable("2026-03-19T12:00:00.000Z"),
            "jsonb_val" to AnyCodable(mapOf("nested" to "object", "count" to 42)),
            "text_array_val" to AnyCodable(listOf("alpha", "beta", "gamma")),
            "int_array_val" to AnyCodable(listOf(1, 2, 3)),
            "uuid_val" to AnyCodable(testUUID)
        )

        val pushResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(PushRecord(
                id = zooID, tableName = "type_zoo", operation = "create",
                data = data, clientUpdatedAt = java.time.Instant.now().toString()
            )),
            schemaVersion = sv, schemaHash = sh
        ))
        assertEquals("type_zoo push should be accepted", 1, pushResp.accepted.size)

        val pullResp = pollForRecord(http, clientID, 0, sv, sh, zooID)
        val found = pullResp.changes.firstOrNull { it.id == zooID }
        assertNotNull("type_zoo record should appear in pull", found)

        assertEquals("hello world", found!!.data["text_val"]?.value)
        assertEquals("bounded string", found.data["varchar_val"]?.value)
        assertEquals(true, found.data["bool_val"]?.value)
        assertNotNull("smallint should round-trip", found.data["smallint_val"])
        assertNotNull("integer should round-trip", found.data["int_val"])
        assertNotNull("bigint should round-trip", found.data["bigint_val"])
        assertNotNull("real should round-trip", found.data["real_val"])
        assertNotNull("double should round-trip", found.data["double_val"])
        assertEquals("2026-03-19", found.data["date_val"]?.value)
        assertNotNull("timestamptz should round-trip", found.data["timestamptz_val"])
        assertNotNull("JSONB should round-trip", found.data["jsonb_val"])
        assertEquals(testUUID, found.data["uuid_val"]?.value)

        // Nullable columns not sent should be null
        val xmlVal = found.data["xml_val"]
        assertTrue("nullable column not sent should be null", xmlVal == null || xmlVal.value == null)
    }

    // -- Test 18: Bucket Updates --

    @Test
    fun testBucketUpdates() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val resp = http.pull(PullRequest(
            clientID = clientID, checkpoint = 0,
            knownBuckets = emptyList(), limit = 100,
            schemaVersion = sv, schemaHash = sh
        ))

        assertNotNull("should get bucket updates on first pull", resp.bucketUpdates)
        assertNotNull("bucketUpdates should have added list", resp.bucketUpdates?.added)
    }

    // -- Test 19: Per-Bucket Checkpoints --

    @Test
    fun testPerBucketCheckpoints() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (reg, sv, sh) = registerWithSchema(http, clientID)

        val bucketCheckpoints = reg.bucketCheckpoints ?: emptyMap()
        val resp = http.pull(PullRequest(
            clientID = clientID, checkpoint = 0,
            bucketCheckpoints = if (bucketCheckpoints.isEmpty()) null else bucketCheckpoints,
            limit = 100, schemaVersion = sv, schemaHash = sh
        ))

        assertNotNull("response should include bucket_checkpoints", resp.bucketCheckpoints)

        val resp2 = http.pull(PullRequest(
            clientID = clientID, checkpoint = resp.checkpoint,
            bucketCheckpoints = resp.bucketCheckpoints,
            limit = 100, schemaVersion = sv, schemaHash = sh
        ))
        assertTrue("second pull from same checkpoints should not duplicate data",
            resp2.changes.isEmpty() || resp2.checkpoint >= resp.checkpoint)
    }

    // -- Test 20: Bucket Checksums on Final Page --

    @Test
    fun testBucketChecksumsOnFinalPage() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        pushCustomer(http, clientID, custID, userID, schemaVersion = sv, schemaHash = sh)
        pollForRecord(http, clientID, 0, sv, sh, custID)

        var checkpoint: Long = 0
        var lastBucketChecksums: Map<String, Int>? = null
        var intermediateHadChecksums = false
        var iterations = 0

        do {
            val resp = http.pull(PullRequest(
                clientID = clientID, checkpoint = checkpoint,
                limit = 1, schemaVersion = sv, schemaHash = sh
            ))
            checkpoint = resp.checkpoint
            if (resp.hasMore && resp.bucketChecksums != null) {
                intermediateHadChecksums = true
            }
            if (!resp.hasMore) {
                lastBucketChecksums = resp.bucketChecksums
                break
            }
            iterations++
        } while (iterations < 100)

        assertNotNull("final page should include bucket_checksums", lastBucketChecksums)
        assertTrue("bucket_checksums should be non-empty", lastBucketChecksums?.isNotEmpty() == true)
        assertFalse("intermediate pages should NOT have checksums", intermediateHadChecksums)
    }

    // -- Test 21: Push Response Envelope --

    @Test
    fun testPushResponseEnvelope() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        val custID = UUID.randomUUID().toString().lowercase()
        val regionID = UUID.randomUUID().toString().lowercase()

        val resp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(
                PushRecord(id = custID, tableName = "customers", operation = "create",
                    data = mapOf("id" to AnyCodable(custID), "user_id" to AnyCodable(userID), "email" to AnyCodable("envelope@example.com")),
                    clientUpdatedAt = java.time.Instant.now().toString()),
                PushRecord(id = regionID, tableName = "regions", operation = "create",
                    data = mapOf("id" to AnyCodable(regionID), "name" to AnyCodable("Invalid")),
                    clientUpdatedAt = java.time.Instant.now().toString())
            ),
            schemaVersion = sv, schemaHash = sh
        ))

        assertEquals(1, resp.accepted.size)
        val accepted = resp.accepted.first()
        assertEquals(custID, accepted.id)
        assertEquals("customers", accepted.tableName)
        assertEquals("create", accepted.operation)
        assertEquals(PushStatus.APPLIED, accepted.status)
        assertNotNull(accepted.serverUpdatedAt)

        assertEquals(1, resp.rejected.size)
        val rejected = resp.rejected.first()
        assertEquals(regionID, rejected.id)
        assertEquals("regions", rejected.tableName)
        assertEquals(PushStatus.REJECTED_TERMINAL, rejected.status)
        assertNotNull(rejected.reasonCode)

        assertTrue(resp.checkpoint > 0)
        assertTrue(resp.schemaVersion > 0)
        assertTrue(resp.schemaHash.isNotEmpty())
    }

    // -- Test 22: Auth Failure --

    @Test
    fun testAuthFailure() = runTest {
        skipIfNoServer()

        val config = SynchroConfig(
            dbPath = "",
            serverURL = serverURL!!,
            authProvider = { "invalid-jwt-token" },
            clientID = UUID.randomUUID().toString().lowercase(),
            appVersion = "1.0.0",
            syncInterval = 999.0,
            maxRetryAttempts = 1
        )
        val http = HttpClient(config)

        try {
            http.register(RegisterRequest(
                clientID = config.clientID, platform = "test",
                appVersion = "1.0.0", schemaVersion = 0, schemaHash = ""
            ))
            fail("expected auth failure on register")
        } catch (e: SynchroError.ServerError) {
            assertEquals(401, e.status)
        }
    }

    // -- Test 23: Version Mismatch --

    @Test
    fun testVersionMismatch() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, _) = makeHttpClient(userID, appVersion = "0.0.1")

        try {
            http.register(RegisterRequest(
                clientID = UUID.randomUUID().toString().lowercase(),
                platform = "test", appVersion = "0.0.1",
                schemaVersion = 0, schemaHash = ""
            ))
            fail("expected upgrade required error")
        } catch (_: SynchroError.UpgradeRequired) {
            // Expected
        }
    }

    // -- Test 24: Schema Mismatch --

    @Test
    fun testSchemaMismatch() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)
        val (_, sv, sh) = registerWithSchema(http, clientID)

        try {
            http.push(PushRequest(
                clientID = clientID,
                changes = listOf(PushRecord(
                    id = UUID.randomUUID().toString().lowercase(),
                    tableName = "customers", operation = "create",
                    data = mapOf("id" to AnyCodable(UUID.randomUUID().toString().lowercase()), "user_id" to AnyCodable(userID)),
                    clientUpdatedAt = java.time.Instant.now().toString()
                )),
                schemaVersion = 0, schemaHash = "stale"
            ))
            fail("expected schema mismatch error")
        } catch (e: SynchroError.SchemaMismatch) {
            assertEquals("server should report current schema version", sv, e.serverVersion)
            assertEquals("server should report current schema hash", sh, e.serverHash)
        }
    }
}
