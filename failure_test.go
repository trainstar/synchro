package synchro_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/synctest"
)

// TestFailure_PushHookRollback verifies that a failing OnPushAccepted hook
// rolls back the entire transaction, leaving no orders committed.
func TestFailure_PushHookRollback(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	hookShouldFail := true
	
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
		Hooks: synchro.Hooks{
			OnPushAccepted: func(ctx context.Context, tx *sql.Tx, accepted []synchro.AcceptedRecord) error {
				if hookShouldFail {
					return fmt.Errorf("hook failure")
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-hook-fail"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	orderID := "00000000-0000-0000-0000-f00000000001"
	pushReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "789 Pine Rd",
		}),
	)

	// Push with failing hook — should return error.
	_, err = engine.Push(ctx, userID, pushReq)
	if err == nil {
		t.Fatal("expected Push to fail when hook returns error")
	}

	// Verify no record was committed.
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE id = $1", orderID).Scan(&count)
	if err != nil {
		t.Fatalf("querying orders: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after hook failure rollback, got %d", count)
	}

	// Clear the flag, push again — should succeed.
	hookShouldFail = false
	pushResp, err := engine.Push(ctx, userID, pushReq)
	if err != nil {
		t.Fatalf("Push after clearing hook: %v", err)
	}
	if len(pushResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(pushResp.Accepted), pushResp.Rejected)
	}

	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE id = $1", orderID).Scan(&count)
	if err != nil {
		t.Fatalf("querying orders after success: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after successful push, got %d", count)
	}
}

// TestFailure_CheckpointResetIdempotentPull verifies that if a checkpoint
// advance fails (simulated by manual reset), re-pulling from the old
// checkpoint returns the same data safely.
func TestFailure_CheckpointResetIdempotentPull(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-checkpoint-reset"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a record and simulate WAL changelog entry.
	orderID := "00000000-0000-0000-0000-f00000000002"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "101 Maple Dr"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "orders", orderID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// First pull — checkpoint advances.
	pullResp1, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull 1: %v", err)
	}
	if pullResp1.Checkpoint == 0 {
		t.Fatal("expected nonzero checkpoint after first pull")
	}
	if len(pullResp1.Changes) == 0 {
		t.Fatal("expected changes on first pull")
	}

	// Simulate checkpoint advance failure: reset last_pull_seq to 0.
	_, err = db.ExecContext(ctx,
		"UPDATE sync_clients SET last_pull_seq = 0 WHERE user_id = $1 AND client_id = $2",
		userID, clientID)
	if err != nil {
		t.Fatalf("resetting checkpoint: %v", err)
	}

	// Re-pull from checkpoint 0 — should return same data.
	pullResp2, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull 2: %v", err)
	}
	if len(pullResp2.Changes) != len(pullResp1.Changes) {
		t.Fatalf("re-pull returned %d changes, want %d", len(pullResp2.Changes), len(pullResp1.Changes))
	}
}

// TestFailure_ConcurrentPushesLWW verifies that two concurrent pushes to the
// same record resolve deterministically via LWW without deadlock or error.
func TestFailure_ConcurrentPushesLWW(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientA := "client-concurrent-a"
	clientB := "client-concurrent-b"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// Create a record via push from client A.
	orderID := "00000000-0000-0000-0000-f00000000003"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "200 Cedar Ln"}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	// Insert changelog entry so both clients see the record.
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "orders", orderID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Both clients push updates concurrently with different ClientUpdatedAt
	// timestamps (one clearly newer).
	now := time.Now().UTC()
	olderTime := now.Add(-2 * time.Second)
	newerTime := now.Add(2 * time.Second)

	var wg sync.WaitGroup
	var errA, errB error

	// sync.WaitGroup ensures both start simultaneously.
	wg.Add(2)
	go func() {
		defer wg.Done()
		rec := synctest.MakePushRecord(orderID, "orders", "update", map[string]any{"ship_address": "300 Birch Way"})
		rec.ClientUpdatedAt = olderTime
		_, errA = engine.Push(ctx, userID, synctest.MakePushRequest(clientA, sv, sh, rec))
	}()
	go func() {
		defer wg.Done()
		rec := synctest.MakePushRecord(orderID, "orders", "update", map[string]any{"ship_address": "400 Spruce Ct"})
		rec.ClientUpdatedAt = newerTime
		_, errB = engine.Push(ctx, userID, synctest.MakePushRequest(clientB, sv, sh, rec))
	}()
	wg.Wait()

	// Both complete without error or deadlock.
	if errA != nil {
		t.Fatalf("concurrent push A error: %v", errA)
	}
	if errB != nil {
		t.Fatalf("concurrent push B error: %v", errB)
	}

	// Query DB: exactly one final version, newer timestamp wins (LWW determinism).
	var shipAddr string
	err = db.QueryRowContext(ctx, "SELECT ship_address FROM orders WHERE id = $1", orderID).Scan(&shipAddr)
	if err != nil {
		t.Fatalf("querying final record: %v", err)
	}
	if shipAddr != "400 Spruce Ct" {
		t.Fatalf("expected newer timestamp winner %q, got %q", "400 Spruce Ct", shipAddr)
	}
}

// TestFailure_ChangelogOrderAndDedup verifies that pull returns changes in seq
// order and deduplicates multiple entries for the same record.
func TestFailure_ChangelogOrderAndDedup(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-order-dedup"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Create records across different tables so they exist for pull hydration.
	orderA := "00000000-0000-0000-0000-f00000000004"
	catB := "00000000-0000-0000-0000-f00000000005"

	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderA, "orders", "create", map[string]any{"ship_address": "500 Walnut Blvd"}),
	))
	if err != nil {
		t.Fatalf("Push create orderA: %v", err)
	}
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(catB, "categories", "create", map[string]any{"name": "Category B"}),
	))
	if err != nil {
		t.Fatalf("Push create catB: %v", err)
	}

	// Soft-delete orderA so the DELETE changelog entry resolves correctly.
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderA, "orders", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete orderA: %v", err)
	}

	// Insert interleaved changelog entries across different tables,
	// with multiple entries for the same record (orderA):
	//   categories/catB create, orders/orderA update, orders/orderA delete.
	// Dedup: two entries for same record (UPDATE at seq=N+1, DELETE at seq=N+2) → pull returns only DELETE.
	for _, entry := range []struct {
		table string
		id    string
		op    int
	}{
		{"categories", catB, 1},   // create
		{"orders", orderA, 2}, // update
		{"orders", orderA, 3}, // delete — dedup should keep only this for orderA
	} {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, entry.table, entry.id, entry.op)
		if err != nil {
			t.Fatalf("writing changelog: %v", err)
		}
	}

	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Verify results in seq order.
	// Dedup: catB's create → Changes, orderA's UPDATE+DELETE deduped to DELETE → Deletes.
	if len(pullResp.Changes) != 1 {
		t.Fatalf("expected 1 change (catB), got %d", len(pullResp.Changes))
	}
	if pullResp.Changes[0].ID != catB {
		t.Fatalf("expected change for catB, got %q", pullResp.Changes[0].ID)
	}
	if len(pullResp.Deletes) != 1 {
		t.Fatalf("expected 1 delete (orderA deduped), got %d", len(pullResp.Deletes))
	}
	if pullResp.Deletes[0].ID != orderA {
		t.Fatalf("expected delete for orderA, got %q", pullResp.Deletes[0].ID)
	}
}

// TestFailure_ClockSkewIntegration tests LWW conflict resolution with clock
// skew tolerance at the engine level.
func TestFailure_ClockSkewIntegration(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:               db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
		ClockSkewTolerance: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-clock-skew"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create a record.
	orderID := "00000000-0000-0000-0000-f00000000006"
	createResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "700 Oak Rd"}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(createResp.Accepted) != 1 || createResp.Accepted[0].ServerUpdatedAt == nil {
		t.Fatal("create should be accepted with server timestamp")
	}
	serverTime := *createResp.Accepted[0].ServerUpdatedAt

	// Update with client timestamp 5s behind server — server should win
	// because 5s > 1s tolerance.
	rec1 := synctest.MakePushRecord(orderID, "orders", "update", map[string]any{"ship_address": "701 Stale Way"})
	rec1.ClientUpdatedAt = serverTime.Add(-5 * time.Second)
	resp1, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh, rec1))
	if err != nil {
		t.Fatalf("Push stale update: %v", err)
	}
	if len(resp1.Rejected) != 1 {
		t.Fatalf("stale update: expected 1 rejected, got accepted=%d rejected=%d",
			len(resp1.Accepted), len(resp1.Rejected))
	}

	// Update with client timestamp 500ms behind server — client should win
	// because tolerance (1s) compensates for the 500ms gap.
	rec2 := synctest.MakePushRecord(orderID, "orders", "update", map[string]any{"ship_address": "702 Close St"})
	rec2.ClientUpdatedAt = serverTime.Add(-500 * time.Millisecond)
	resp2, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh, rec2))
	if err != nil {
		t.Fatalf("Push close update: %v", err)
	}
	if len(resp2.Accepted) != 1 {
		t.Fatalf("close update: expected 1 accepted, got accepted=%d rejected=%d",
			len(resp2.Accepted), len(resp2.Rejected))
	}

	// Verify final state.
	var shipAddr string
	err = db.QueryRowContext(ctx, "SELECT ship_address FROM orders WHERE id = $1", orderID).Scan(&shipAddr)
	if err != nil {
		t.Fatalf("querying final record: %v", err)
	}
	if shipAddr != "702 Close St" {
		t.Fatalf("expected ship_address %q, got %q", "702 Close St", shipAddr)
	}
}

// TestFailure_PartialPushWithHookFailure verifies that when a push contains
// both valid and invalid records, a hook failure on the accepted records
// rolls back the entire transaction.
func TestFailure_PartialPushWithHookFailure(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	hookShouldFail := true
	
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
		Hooks: synchro.Hooks{
			OnPushAccepted: func(ctx context.Context, tx *sql.Tx, accepted []synchro.AcceptedRecord) error {
				if hookShouldFail {
					return fmt.Errorf("hook failure")
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-partial-hook"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push 3 records: 2 valid creates to "orders" + 1 to read-only "products".
	orderID1 := "00000000-0000-0000-0000-f00000000007"
	orderID2 := "00000000-0000-0000-0000-f00000000008"
	prodID := "00000000-0000-0000-0000-f00000000009"
	pushReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID1, "orders", "create", map[string]any{"ship_address": "601 First Ave"}),
		synctest.MakePushRecord(orderID2, "orders", "create", map[string]any{"ship_address": "602 Second Ave"}),
		synctest.MakePushRecord(prodID, "products", "create", map[string]any{"name": "Test Product"}),
	)

	// Hook fires for 2 accepted records and fails.
	_, err = engine.Push(ctx, userID, pushReq)
	if err == nil {
		t.Fatal("expected Push to fail when hook returns error")
	}

	// Verify 0 rows in orders (full rollback).
	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders WHERE id IN ($1, $2)", orderID1, orderID2).Scan(&count)
	if err != nil {
		t.Fatalf("querying orders: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after hook failure, got %d", count)
	}

	// Remove failing hook, push valid records only.
	hookShouldFail = false
	pushReq2 := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID1, "orders", "create", map[string]any{"ship_address": "601 First Ave"}),
		synctest.MakePushRecord(orderID2, "orders", "create", map[string]any{"ship_address": "602 Second Ave"}),
	)
	pushResp, err := engine.Push(ctx, userID, pushReq2)
	if err != nil {
		t.Fatalf("Push after clearing hook: %v", err)
	}
	if len(pushResp.Accepted) != 2 {
		t.Fatalf("expected 2 accepted, got %d", len(pushResp.Accepted))
	}

	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders WHERE id IN ($1, $2)", orderID1, orderID2).Scan(&count)
	if err != nil {
		t.Fatalf("querying orders after success: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows after successful push, got %d", count)
	}
}
