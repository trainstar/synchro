package synchro_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/synctest"
)

// schemaFields fetches the current schema version and hash from the engine.
func schemaFields(t *testing.T, ctx context.Context, engine *synchro.Engine) (int64, string) {
	t.Helper()
	v, h, err := engine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest: %v", err)
	}
	return v, h
}

func TestIntegration_PushPullRoundTrip(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-client-1"

	// Register client
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push a create
	itemID := "00000000-0000-0000-0000-000000000010"
	pushReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{
			"name":        "Test Item",
			"description": "A test item",
		}),
	)

	pushResp, err := engine.Push(ctx, userID, pushReq)
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(pushResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(pushResp.Accepted), pushResp.Rejected)
	}

	// RYOW: ServerUpdatedAt should be set on the accepted result
	if pushResp.Accepted[0].ServerUpdatedAt == nil {
		t.Error("expected ServerUpdatedAt to be set on create")
	}

	// Simulate WAL: write a changelog entry for the created record
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull should return the created record
	pullReq := synctest.MakePullRequest(clientID, 0, sv, sh)
	pullResp, err := engine.Pull(ctx, userID, pullReq)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pullResp.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(pullResp.Changes))
	}
	if pullResp.Changes[0].ID != itemID {
		t.Errorf("pulled record ID = %q, want %q", pullResp.Changes[0].ID, itemID)
	}

	// Push an update
	updateReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "update", map[string]any{
			"name": "Updated Item",
		}),
	)
	updateResp, err := engine.Push(ctx, userID, updateReq)
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}
	if len(updateResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted update, got %d", len(updateResp.Accepted))
	}

	// RYOW: ServerUpdatedAt should be set on update
	if updateResp.Accepted[0].ServerUpdatedAt == nil {
		t.Error("expected ServerUpdatedAt to be set on update")
	}

	// Simulate WAL for the update
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 2)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull with previous checkpoint
	pullReq2 := synctest.MakePullRequest(clientID, pullResp.Checkpoint, sv, sh)
	pullResp2, err := engine.Pull(ctx, userID, pullReq2)
	if err != nil {
		t.Fatalf("Pull after update: %v", err)
	}
	if len(pullResp2.Changes) != 1 {
		t.Fatalf("expected 1 change after update, got %d", len(pullResp2.Changes))
	}

	// Push a delete
	deleteReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "delete", nil),
	)
	deleteResp, err := engine.Push(ctx, userID, deleteReq)
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}
	if len(deleteResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted delete, got %d", len(deleteResp.Accepted))
	}

	// RYOW: ServerDeletedAt should be set on delete
	if deleteResp.Accepted[0].ServerDeletedAt == nil {
		t.Error("expected ServerDeletedAt to be set on delete")
	}

	// Simulate WAL for the delete
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 3)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull should return the delete
	pullReq3 := synctest.MakePullRequest(clientID, pullResp2.Checkpoint, sv, sh)
	pullResp3, err := engine.Pull(ctx, userID, pullReq3)
	if err != nil {
		t.Fatalf("Pull after delete: %v", err)
	}
	if len(pullResp3.Deletes) != 1 {
		t.Fatalf("expected 1 delete entry, got %d", len(pullResp3.Deletes))
	}
	if pullResp3.Deletes[0].ID != itemID {
		t.Errorf("deleted record ID = %q, want %q", pullResp3.Deletes[0].ID, itemID)
	}
}

func TestIntegration_RLSEnforcement(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	// Apply RLS policies
	policies := synchro.GenerateRLSPolicies(reg)
	for _, stmt := range policies {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userA := "00000000-0000-0000-0000-00000000000a"
	userB := "00000000-0000-0000-0000-00000000000b"
	clientA := "client-a"
	clientB := "client-b"

	// Register both clients
	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// User A creates a record
	itemID := "00000000-0000-0000-0000-000000000020"
	pushReq := synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{
			"name": "User A Item",
		}),
	)
	resp, err := engine.Push(ctx, userA, pushReq)
	if err != nil {
		t.Fatalf("Push by user A: %v", err)
	}
	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(resp.Accepted), resp.Rejected)
	}

	// User B tries to update user A's record — RLS should block it
	updateReq := synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(itemID, "items", "update", map[string]any{
			"name": "Hijacked",
		}),
	)
	updateResp, err := engine.Push(ctx, userB, updateReq)
	if err != nil {
		t.Fatalf("Push by user B: %v", err)
	}

	if len(updateResp.Accepted) != 0 {
		t.Errorf("expected 0 accepted for user B's update, got %d", len(updateResp.Accepted))
	}
	if len(updateResp.Rejected) != 1 {
		t.Fatalf("expected 1 rejected for user B's update, got %d", len(updateResp.Rejected))
	}

	// User A can still update their own record
	updateReqA := synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemID, "items", "update", map[string]any{
			"name": "Updated by A",
		}),
	)
	updateRespA, err := engine.Push(ctx, userA, updateReqA)
	if err != nil {
		t.Fatalf("Push update by user A: %v", err)
	}
	if len(updateRespA.Accepted) != 1 {
		t.Errorf("expected 1 accepted for user A's update, got %d (rejected: %v)", len(updateRespA.Accepted), updateRespA.Rejected)
	}
}

func TestIntegration_RYOW_PushReturnsServerTimestamps(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-ryow"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create
	itemID := "00000000-0000-0000-0000-000000000030"
	createResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{
			"name": "RYOW test",
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(createResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(createResp.Accepted))
	}

	cr := createResp.Accepted[0]
	if cr.ServerUpdatedAt == nil {
		t.Fatal("ServerUpdatedAt should be set on create")
	}
	if cr.ServerUpdatedAt.IsZero() {
		t.Error("ServerUpdatedAt should not be zero")
	}
	if cr.ServerDeletedAt != nil {
		t.Error("ServerDeletedAt should be nil on create")
	}
	createTS := *cr.ServerUpdatedAt

	// Checkpoint should be 0 (no stale MAX(seq))
	if createResp.Checkpoint != 0 {
		t.Errorf("Checkpoint = %d, want 0", createResp.Checkpoint)
	}

	// Update
	updateResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "update", map[string]any{
			"name": "RYOW updated",
		}),
	))
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}
	if len(updateResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted update, got %d", len(updateResp.Accepted))
	}

	ur := updateResp.Accepted[0]
	if ur.ServerUpdatedAt == nil {
		t.Fatal("ServerUpdatedAt should be set on update")
	}
	if !ur.ServerUpdatedAt.After(createTS) && !ur.ServerUpdatedAt.Equal(createTS) {
		t.Errorf("update ServerUpdatedAt (%v) should be >= create ServerUpdatedAt (%v)",
			ur.ServerUpdatedAt, createTS)
	}

	// Delete
	delResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}
	if len(delResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted delete, got %d", len(delResp.Accepted))
	}
	dr := delResp.Accepted[0]
	if dr.ServerDeletedAt == nil {
		t.Fatal("ServerDeletedAt should be set on delete")
	}
	if dr.ServerDeletedAt.IsZero() {
		t.Error("ServerDeletedAt should not be zero")
	}
}

func TestIntegration_Compaction(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
		Compactor: &synchro.CompactorConfig{
			StaleThreshold: 24 * time.Hour, // won't deactivate during this test
			BatchSize:      100,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientA := "client-compact-a"
	clientB := "client-compact-b"

	// Register two clients
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// Push a record
	itemID := "00000000-0000-0000-0000-000000000040"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "compact test"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Simulate WAL: write 20 changelog entries
	for i := 0; i < 20; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", itemID, 2)
		if err != nil {
			t.Fatalf("writing changelog entry %d: %v", i, err)
		}
	}

	// Client A pulls all (advances to seq 20)
	pullRespA, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientA, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull A: %v", err)
	}

	// Client B pulls only first 10 (advances to seq 10)
	pullReqB := synctest.MakePullRequest(clientB, 0, sv, sh)
	pullReqB.Limit = 10
	pullRespB, err := engine.Pull(ctx, userID, pullReqB)
	if err != nil {
		t.Fatalf("Pull B: %v", err)
	}

	if pullRespA.Checkpoint <= pullRespB.Checkpoint {
		t.Fatalf("A checkpoint (%d) should be > B checkpoint (%d)", pullRespA.Checkpoint, pullRespB.Checkpoint)
	}

	// Run compaction — safe seq = MIN(A, B) = B's checkpoint
	result, err := engine.RunCompaction(ctx)
	if err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	if result.SafeSeq != pullRespB.Checkpoint {
		t.Errorf("SafeSeq = %d, want %d (client B's checkpoint)", result.SafeSeq, pullRespB.Checkpoint)
	}
	if result.DeletedEntries == 0 {
		t.Error("expected some entries to be deleted")
	}

	// Both clients can still pull from their checkpoints
	_, err = engine.Pull(ctx, userID, synctest.MakePullRequest(clientA, pullRespA.Checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull A after compaction: %v", err)
	}
	_, err = engine.Pull(ctx, userID, synctest.MakePullRequest(clientB, pullRespB.Checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull B after compaction: %v", err)
	}
}

func TestIntegration_StaleClientDeactivation(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
		Compactor: &synchro.CompactorConfig{
			StaleThreshold: 1 * time.Millisecond,
			BatchSize:      100,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-stale"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push a record and create changelog entries
	itemID := "00000000-0000-0000-0000-000000000041"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "stale test"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull to advance checkpoint
	_, err = engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Wait so stale threshold kicks in
	time.Sleep(5 * time.Millisecond)

	// Run compaction — should deactivate the client
	result, err := engine.RunCompaction(ctx)
	if err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	if result.DeactivatedClients != 1 {
		t.Errorf("DeactivatedClients = %d, want 1", result.DeactivatedClients)
	}

	// Verify client is deactivated
	var isActive bool
	err = db.QueryRowContext(ctx,
		"SELECT is_active FROM sync_clients WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&isActive)
	if err != nil {
		t.Fatalf("querying client: %v", err)
	}
	if isActive {
		t.Error("expected client to be deactivated")
	}
}

func TestIntegration_ResyncRequired(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-resync-required"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create changelog entries
	for i := 0; i < 5; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", fmt.Sprintf("id-%d", i), 1)
		if err != nil {
			t.Fatalf("writing changelog: %v", err)
		}
	}

	// Pull to get a checkpoint
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if pullResp.Checkpoint == 0 {
		t.Fatal("expected nonzero checkpoint")
	}

	// Simulate compaction: delete entries below the pulled checkpoint
	_, err = db.ExecContext(ctx,
		"DELETE FROM sync_changelog WHERE seq <= $1", pullResp.Checkpoint-1)
	if err != nil {
		t.Fatalf("simulating compaction: %v", err)
	}

	// Add new entries so min_seq > old checkpoint
	for i := 0; i < 3; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", fmt.Sprintf("new-id-%d", i), 1)
		if err != nil {
			t.Fatalf("writing new changelog: %v", err)
		}
	}

	// Delete the remaining old entry so minSeq jumps forward
	_, err = db.ExecContext(ctx,
		"DELETE FROM sync_changelog WHERE seq = $1", pullResp.Checkpoint)
	if err != nil {
		t.Fatalf("deleting old entry: %v", err)
	}

	// Now pull with old checkpoint — should get resync_required
	pullResp2, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, pullResp.Checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull with stale checkpoint: %v", err)
	}
	if !pullResp2.ResyncRequired {
		t.Fatal("expected ResyncRequired = true")
	}
	if pullResp2.Checkpoint != pullResp.Checkpoint {
		t.Errorf("checkpoint should be preserved: got %d, want %d", pullResp2.Checkpoint, pullResp.Checkpoint)
	}
}

func TestIntegration_ResyncRoundTrip(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-resync-roundtrip"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create several records across tables
	itemIDs := []string{
		"00000000-0000-0000-0000-000000000050",
		"00000000-0000-0000-0000-000000000051",
		"00000000-0000-0000-0000-000000000052",
	}
	for _, id := range itemIDs {
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(id, "items", "create", map[string]any{"name": "resync " + id}),
		))
		if err != nil {
			t.Fatalf("Push %s: %v", id, err)
		}
	}

	// Write changelog entries (simulate WAL)
	for _, id := range itemIDs {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", id, 1)
		if err != nil {
			t.Fatalf("writing changelog: %v", err)
		}
	}

	// Resync — page through all records with small limit
	var allRecords []synchro.Record
	var cursor *synchro.ResyncCursor
	pages := 0

	for {
		resyncResp, err := engine.Resync(ctx, userID, &synchro.ResyncRequest{
			ClientID:      clientID,
			Cursor:        cursor,
			Limit:         2, // Small limit to force pagination
			SchemaVersion: sv,
			SchemaHash:    sh,
		})
		if err != nil {
			t.Fatalf("Resync page %d: %v", pages, err)
		}
		pages++

		allRecords = append(allRecords, resyncResp.Records...)

		if !resyncResp.HasMore {
			if resyncResp.Checkpoint == 0 {
				t.Error("expected nonzero checkpoint on final page")
			}
			break
		}

		cursor = resyncResp.Cursor
		if cursor == nil {
			t.Fatal("expected cursor on intermediate page")
		}

		if pages > 50 {
			t.Fatal("too many pages — possible infinite loop")
		}
	}

	if len(allRecords) < len(itemIDs) {
		t.Errorf("resync returned %d records, expected at least %d", len(allRecords), len(itemIDs))
	}
	t.Logf("resync completed in %d pages with %d records", pages, len(allRecords))

	// Normal pull should work after resync
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull after resync: %v", err)
	}
	// Pull from checkpoint 0 should still find the changelog entries
	if len(pullResp.Changes) == 0 && len(pullResp.Deletes) == 0 {
		t.Log("no changes in pull after resync (changelog may have been consumed)")
	}
}

func TestIntegration_ResyncReactivatesClient(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
		Compactor: &synchro.CompactorConfig{
			StaleThreshold: 1 * time.Millisecond,
			BatchSize:      100,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-resync-reactivate"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create a record
	itemID := "00000000-0000-0000-0000-000000000060"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "reactivate test"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Write changelog entry
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull to advance checkpoint
	_, err = engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Deactivate client via compaction
	time.Sleep(5 * time.Millisecond)
	_, err = engine.RunCompaction(ctx)
	if err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	// Verify client is deactivated
	var isActive bool
	err = db.QueryRowContext(ctx,
		"SELECT is_active FROM sync_clients WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&isActive)
	if err != nil {
		t.Fatalf("querying client: %v", err)
	}
	if isActive {
		t.Fatal("expected client to be deactivated before resync")
	}

	// Resync — page through all records (single page should suffice)
	resyncResp, err := engine.Resync(ctx, userID, &synchro.ResyncRequest{
		ClientID:      clientID,
		SchemaVersion: sv,
		SchemaHash:    sh,
	})
	if err != nil {
		t.Fatalf("Resync: %v", err)
	}
	if resyncResp.HasMore {
		// Drain remaining pages
		for resyncResp.HasMore {
			resyncResp, err = engine.Resync(ctx, userID, &synchro.ResyncRequest{
				ClientID:      clientID,
				Cursor:        resyncResp.Cursor,
				SchemaVersion: sv,
				SchemaHash:    sh,
			})
			if err != nil {
				t.Fatalf("Resync continuation: %v", err)
			}
		}
	}

	// Verify client is reactivated
	err = db.QueryRowContext(ctx,
		"SELECT is_active FROM sync_clients WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&isActive)
	if err != nil {
		t.Fatalf("querying client after resync: %v", err)
	}
	if !isActive {
		t.Error("expected client to be reactivated after resync")
	}
}

func TestIntegration_RunCompaction_NotConfigured(t *testing.T) {
	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: reg,
		// No Compactor configured
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	_, err = engine.RunCompaction(ctx)
	if err == nil {
		t.Fatal("expected error when compaction not configured")
	}
}
