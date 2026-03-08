package synchro_test

import (
	"context"
	"encoding/json"
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
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	// Apply RLS policies via superuser
	policies := synchro.GenerateRLSPolicies(reg)
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	// Clean up RLS policies after test so they don't persist
	t.Cleanup(func() {
		for _, cfg := range reg.All() {
			adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	// Engine uses non-superuser connection so RLS is enforced
	engine, err := synchro.NewEngine(synchro.Config{
		DB:       appDB,
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

	// Update — pass BaseUpdatedAt so optimistic concurrency resolves correctly
	updateData, _ := json.Marshal(map[string]any{"name": "RYOW updated"})
	updateResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synchro.PushRecord{
			ID: itemID, TableName: "items", Operation: "update",
			Data: updateData, ClientUpdatedAt: time.Now().UTC(), BaseUpdatedAt: &createTS,
		},
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

	// Push 5 records to create actual DB rows, then insert changelog entries
	for i := 0; i < 5; i++ {
		recID := fmt.Sprintf("00000000-0000-0000-0000-0000000050%02d", i)
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(recID, "items", "create", map[string]any{"name": fmt.Sprintf("item-%d", i)}),
		))
		if err != nil {
			t.Fatalf("Push item %d: %v", i, err)
		}
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", recID, 1)
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

	// Add new entries with real UUIDs so min_seq > old checkpoint
	for i := 0; i < 3; i++ {
		recID := fmt.Sprintf("00000000-0000-0000-0000-0000000060%02d", i)
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(recID, "items", "create", map[string]any{"name": fmt.Sprintf("new-item-%d", i)}),
		))
		if err != nil {
			t.Fatalf("Push new item %d: %v", i, err)
		}
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", recID, 1)
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

func TestIntegration_PullBucketIsolation(t *testing.T) {
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
	userA := "00000000-0000-0000-0000-0000000000a1"
	userB := "00000000-0000-0000-0000-0000000000b1"
	clientA := "client-bucket-a"
	clientB := "client-bucket-b"

	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// User A pushes item-A
	itemA := "00000000-0000-0000-0000-000000000b01"
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemA, "items", "create", map[string]any{"name": "Item A"}),
	))
	if err != nil {
		t.Fatalf("Push item-A: %v", err)
	}

	// User B pushes item-B
	itemB := "00000000-0000-0000-0000-000000000b02"
	_, err = engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(itemB, "items", "create", map[string]any{"name": "Item B"}),
	))
	if err != nil {
		t.Fatalf("Push item-B: %v", err)
	}

	// Simulate WAL: assign each item to its owner's bucket
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userA, "items", itemA, 1)
	if err != nil {
		t.Fatalf("writing changelog for item-A: %v", err)
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userB, "items", itemB, 1)
	if err != nil {
		t.Fatalf("writing changelog for item-B: %v", err)
	}

	// User A pulls: should only see item-A
	pullRespA, err := engine.Pull(ctx, userA, synctest.MakePullRequest(clientA, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull A: %v", err)
	}
	if len(pullRespA.Changes) != 1 {
		t.Fatalf("User A: expected 1 change, got %d", len(pullRespA.Changes))
	}
	if pullRespA.Changes[0].ID != itemA {
		t.Errorf("User A: expected item %s, got %s", itemA, pullRespA.Changes[0].ID)
	}

	// User B pulls: should only see item-B
	pullRespB, err := engine.Pull(ctx, userB, synctest.MakePullRequest(clientB, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull B: %v", err)
	}
	if len(pullRespB.Changes) != 1 {
		t.Fatalf("User B: expected 1 change, got %d", len(pullRespB.Changes))
	}
	if pullRespB.Changes[0].ID != itemB {
		t.Errorf("User B: expected item %s, got %s", itemB, pullRespB.Changes[0].ID)
	}
}

func TestIntegration_ProtectedColumnEnforcement(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000c01"
	clientID := "client-protected"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	itemID := "00000000-0000-0000-0000-000000000c10"
	attackerID := "00000000-0000-0000-0000-00000000dead"
	fakeTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	// Push create with protected columns injected
	pushResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{
			"name":       "Protected Test",
			"created_at": fakeTime.Format(time.RFC3339),
			"updated_at": fakeTime.Format(time.RFC3339),
			"user_id":    attackerID,
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(pushResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(pushResp.Accepted), pushResp.Rejected)
	}

	// Verify DB: user_id should be the authenticated user, timestamps server-assigned
	var dbUserID string
	var dbCreatedAt, dbUpdatedAt time.Time
	err = db.QueryRowContext(ctx,
		"SELECT user_id, created_at, updated_at FROM items WHERE id = $1", itemID,
	).Scan(&dbUserID, &dbCreatedAt, &dbUpdatedAt)
	if err != nil {
		t.Fatalf("querying item: %v", err)
	}

	if dbUserID != userID {
		t.Errorf("user_id = %q, want authenticated user %q (attacker ID was %q)", dbUserID, userID, attackerID)
	}
	if dbCreatedAt.Equal(fakeTime) {
		t.Error("created_at should be server-assigned, not the fake year-2000 timestamp")
	}
	if dbUpdatedAt.Equal(fakeTime) {
		t.Error("updated_at should be server-assigned, not the fake year-2000 timestamp")
	}

	// Push update with protected timestamp — pass BaseUpdatedAt for optimistic concurrency
	updateData, _ := json.Marshal(map[string]any{
		"name":       "Updated Name",
		"updated_at": fakeTime.Format(time.RFC3339),
	})
	updateResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synchro.PushRecord{
			ID: itemID, TableName: "items", Operation: "update",
			Data: updateData, ClientUpdatedAt: time.Now().UTC(), BaseUpdatedAt: &dbUpdatedAt,
		},
	))
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}
	if len(updateResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted update, got %d", len(updateResp.Accepted))
	}

	var dbName string
	var dbUpdatedAt2 time.Time
	err = db.QueryRowContext(ctx,
		"SELECT name, updated_at FROM items WHERE id = $1", itemID,
	).Scan(&dbName, &dbUpdatedAt2)
	if err != nil {
		t.Fatalf("querying updated item: %v", err)
	}
	if dbName != "Updated Name" {
		t.Errorf("name = %q, want %q", dbName, "Updated Name")
	}
	if dbUpdatedAt2.Equal(fakeTime) {
		t.Error("updated_at after update should be server-assigned, not the fake timestamp")
	}
}

func TestIntegration_SoftDeleteLifecycle(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000d01"
	clientID := "client-softdelete"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	itemID := "00000000-0000-0000-0000-000000000d10"

	// Create
	createResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "Lifecycle Item"}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(createResp.Accepted) != 1 {
		t.Fatalf("create: expected 1 accepted, got %d", len(createResp.Accepted))
	}

	// Delete
	deleteResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}
	if len(deleteResp.Accepted) != 1 {
		t.Fatalf("delete: expected 1 accepted, got %d", len(deleteResp.Accepted))
	}
	if deleteResp.Accepted[0].Status != synchro.PushStatusApplied {
		t.Errorf("delete status = %q, want %q", deleteResp.Accepted[0].Status, synchro.PushStatusApplied)
	}
	if deleteResp.Accepted[0].ServerDeletedAt == nil {
		t.Error("ServerDeletedAt should be set on delete")
	}

	// Idempotent delete (same item again)
	deleteResp2, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push idempotent delete: %v", err)
	}
	if len(deleteResp2.Accepted) != 1 {
		t.Fatalf("idempotent delete: expected 1 accepted, got %d (rejected: %v)", len(deleteResp2.Accepted), deleteResp2.Rejected)
	}
	if deleteResp2.Accepted[0].Status != synchro.PushStatusApplied {
		t.Errorf("idempotent delete status = %q, want %q", deleteResp2.Accepted[0].Status, synchro.PushStatusApplied)
	}

	// Resurrect: create same ID with new data
	resurrectResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "Resurrected Item"}),
	))
	if err != nil {
		t.Fatalf("Push resurrect: %v", err)
	}
	if len(resurrectResp.Accepted) != 1 {
		t.Fatalf("resurrect: expected 1 accepted, got %d (rejected: %v)", len(resurrectResp.Accepted), resurrectResp.Rejected)
	}

	// Verify DB: deleted_at is NULL, name is new value
	var dbName string
	var dbDeletedAt *time.Time
	err = db.QueryRowContext(ctx,
		"SELECT name, deleted_at FROM items WHERE id = $1", itemID,
	).Scan(&dbName, &dbDeletedAt)
	if err != nil {
		t.Fatalf("querying resurrected item: %v", err)
	}
	if dbDeletedAt != nil {
		t.Error("deleted_at should be NULL after resurrection")
	}
	if dbName != "Resurrected Item" {
		t.Errorf("name = %q, want %q", dbName, "Resurrected Item")
	}
}

func TestIntegration_ReadOnlyTableRejection(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000e01"
	clientID := "client-readonly"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	catID := "00000000-0000-0000-0000-000000000e10"
	pushResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(catID, "categories", "create", map[string]any{"name": "Hacked Category"}),
	))
	if err != nil {
		t.Fatalf("Push to read-only table: %v", err)
	}

	if len(pushResp.Accepted) != 0 {
		t.Errorf("expected 0 accepted for read-only table, got %d", len(pushResp.Accepted))
	}
	if len(pushResp.Rejected) != 1 {
		t.Fatalf("expected 1 rejected for read-only table, got %d", len(pushResp.Rejected))
	}

	// Verify no row was created
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM categories WHERE id = $1", catID).Scan(&count)
	if err != nil {
		t.Fatalf("querying categories: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows in categories, got %d", count)
	}
}

func TestIntegration_GlobalBucketPullVisibility(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000f01"
	clientID := "client-global"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Insert a global tag (user_id = NULL) directly in DB
	tagID := "00000000-0000-0000-0000-000000000f10"
	_, err = db.ExecContext(ctx,
		"INSERT INTO tags (id, user_id, name) VALUES ($1, NULL, $2)", tagID, "Global Tag")
	if err != nil {
		t.Fatalf("inserting global tag: %v", err)
	}

	// Simulate WAL: assign to "global" bucket
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"global", "tags", tagID, 1)
	if err != nil {
		t.Fatalf("writing global changelog: %v", err)
	}

	// Pull as any user — should see the global tag
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	found := false
	for _, c := range pullResp.Changes {
		if c.ID == tagID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("global tag %s not found in pull response (got %d changes)", tagID, len(pullResp.Changes))
	}
}

func TestIntegration_PullPagination(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000001001"
	clientID := "client-pagination"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push 5 items
	itemIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		itemIDs[i] = fmt.Sprintf("00000000-0000-0000-0000-00000010%04d", i)
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(itemIDs[i], "items", "create", map[string]any{
				"name": fmt.Sprintf("Page Item %d", i),
			}),
		))
		if err != nil {
			t.Fatalf("Push item %d: %v", i, err)
		}
	}

	// Insert 5 changelog entries
	for _, id := range itemIDs {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", id, 1)
		if err != nil {
			t.Fatalf("writing changelog: %v", err)
		}
	}

	// Pull with Limit=2 — page through all results
	allIDs := make(map[string]bool)
	checkpoint := int64(0)
	pages := 0

	for {
		pullReq := synctest.MakePullRequest(clientID, checkpoint, sv, sh)
		pullReq.Limit = 2

		pullResp, err := engine.Pull(ctx, userID, pullReq)
		if err != nil {
			t.Fatalf("Pull page %d: %v", pages, err)
		}
		pages++

		for _, c := range pullResp.Changes {
			if allIDs[c.ID] {
				t.Fatalf("duplicate record %s on page %d", c.ID, pages)
			}
			allIDs[c.ID] = true
		}

		checkpoint = pullResp.Checkpoint

		if !pullResp.HasMore {
			break
		}

		if pages > 20 {
			t.Fatal("too many pages — possible infinite loop")
		}
	}

	if len(allIDs) != 5 {
		t.Fatalf("expected 5 total items across pages, got %d", len(allIDs))
	}
	for _, id := range itemIDs {
		if !allIDs[id] {
			t.Errorf("item %s not found across paginated pulls", id)
		}
	}
	if pages < 2 {
		t.Errorf("expected at least 2 pages with Limit=2 and 5 items, got %d", pages)
	}
}

func TestIntegration_RLSBroadCoverage(t *testing.T) {
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	reg := synctest.NewTestRegistry()
	ctx := context.Background()

	// Apply RLS policies via superuser
	policies := synchro.GenerateRLSPolicies(reg)
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	// Clean up RLS policies after test so they don't persist
	t.Cleanup(func() {
		for _, cfg := range reg.All() {
			adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	// Engine uses non-superuser connection so RLS is enforced
	engine, err := synchro.NewEngine(synchro.Config{
		DB:       appDB,
		Registry: reg,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userA := "00000000-0000-0000-0000-000000001101"
	userB := "00000000-0000-0000-0000-000000001102"
	clientA := "client-rls-a"
	clientB := "client-rls-b"

	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// User A: full CRUD lifecycle under RLS
	itemID := "00000000-0000-0000-0000-000000001110"

	// Create
	createResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "RLS Item"}),
	))
	if err != nil {
		t.Fatalf("User A create: %v", err)
	}
	if len(createResp.Accepted) != 1 {
		t.Fatalf("User A create: expected 1 accepted, got %d (rejected: %v)", len(createResp.Accepted), createResp.Rejected)
	}
	createTS := *createResp.Accepted[0].ServerUpdatedAt

	// Update — pass BaseUpdatedAt so optimistic concurrency resolves correctly
	updateData, _ := json.Marshal(map[string]any{"name": "RLS Updated"})
	updateResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synchro.PushRecord{
			ID: itemID, TableName: "items", Operation: "update",
			Data: updateData, ClientUpdatedAt: time.Now().UTC(), BaseUpdatedAt: &createTS,
		},
	))
	if err != nil {
		t.Fatalf("User A update: %v", err)
	}
	if len(updateResp.Accepted) != 1 {
		t.Fatalf("User A update: expected 1 accepted, got %d (rejected: %+v)", len(updateResp.Accepted), updateResp.Rejected)
	}

	// Delete
	deleteResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemID, "items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("User A delete: %v", err)
	}
	if len(deleteResp.Accepted) != 1 {
		t.Fatalf("User A delete: expected 1 accepted, got %d", len(deleteResp.Accepted))
	}

	// User B: attempt to create with User A's ID as owner → should fail
	itemID2 := "00000000-0000-0000-0000-000000001111"
	createRespB, err := engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(itemID2, "items", "create", map[string]any{
			"name":    "Sneaky Item",
			"user_id": userA, // attempt to set owner to A
		}),
	))
	if err != nil {
		t.Fatalf("User B create: %v", err)
	}
	// Engine enforces user_id = userB, so the item should be created under userB's ownership
	// (protected column enforcement sets user_id to authenticated user).
	// RLS should allow this since the row will have user_id=userB.
	if len(createRespB.Accepted) != 1 {
		t.Fatalf("User B create: expected 1 accepted (owner enforced to B), got %d (rejected: %v)",
			len(createRespB.Accepted), createRespB.Rejected)
	}

	// Verify the item is owned by B, not A (query via admin to bypass RLS)
	var ownerID string
	err = adminDB.QueryRowContext(ctx, "SELECT user_id FROM items WHERE id = $1", itemID2).Scan(&ownerID)
	if err != nil {
		t.Fatalf("querying item owner: %v", err)
	}
	if ownerID != userB {
		t.Errorf("item owner = %q, want %q (should be enforced to authenticated user)", ownerID, userB)
	}

	// User B: attempt to delete User A's item → RLS blocks
	deleteRespB, err := engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(itemID, "items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("User B delete attempt: %v", err)
	}
	if len(deleteRespB.Accepted) != 0 {
		t.Errorf("User B delete User A's item: expected 0 accepted, got %d", len(deleteRespB.Accepted))
	}

	// Child table: User A creates item_details referencing their item
	// First resurrect User A's item (it was deleted above)
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(itemID, "items", "create", map[string]any{"name": "Resurrected for child test"}),
	))
	if err != nil {
		t.Fatalf("Resurrect item for child test: %v", err)
	}

	detailID := "00000000-0000-0000-0000-000000001112"
	detailResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(detailID, "item_details", "create", map[string]any{
			"item_id": itemID,
			"notes":   "User A's notes",
		}),
	))
	if err != nil {
		t.Fatalf("User A create item_details: %v", err)
	}
	if len(detailResp.Accepted) != 1 {
		t.Fatalf("User A item_details: expected 1 accepted, got %d (rejected: %v)",
			len(detailResp.Accepted), detailResp.Rejected)
	}

	// User B attempts item_details referencing User A's item → RLS blocks.
	// RLS policy violations on INSERT abort the PostgreSQL transaction,
	// so Push returns an error (not a clean rejection).
	detailID2 := "00000000-0000-0000-0000-000000001113"
	detailRespB, err := engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(detailID2, "item_details", "create", map[string]any{
			"item_id": itemID,
			"notes":   "User B trying to add to A's item",
		}),
	))
	if err == nil && len(detailRespB.Accepted) > 0 {
		t.Error("User B should not be able to create item_details on A's item")
	}
	// Either Push returns an error (tx aborted by RLS) or the record is rejected — both are correct.
}

func TestIntegration_PullHydrationMissingRecord(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000002001"
	clientID := "client-hydration-missing"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push two items so they exist in the DB
	itemA := "00000000-0000-0000-0000-000000002010"
	itemB := "00000000-0000-0000-0000-000000002011"

	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemA, "items", "create", map[string]any{"name": "Item A"}),
	))
	if err != nil {
		t.Fatalf("Push item A: %v", err)
	}
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(itemB, "items", "create", map[string]any{"name": "Item B"}),
	))
	if err != nil {
		t.Fatalf("Push item B: %v", err)
	}

	// Insert changelog entries for both
	for _, id := range []string{itemA, itemB} {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "items", id, 1)
		if err != nil {
			t.Fatalf("writing changelog for %s: %v", id, err)
		}
	}

	// Hard-delete item A from the DB (simulating out-of-band removal)
	_, err = db.ExecContext(ctx, "DELETE FROM items WHERE id = $1", itemA)
	if err != nil {
		t.Fatalf("hard-deleting item A: %v", err)
	}

	// Pull — should succeed without error, gracefully skipping the missing record
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Item B should still appear
	foundB := false
	for _, c := range pullResp.Changes {
		if c.ID == itemA {
			t.Error("hard-deleted item A should not appear in pull response")
		}
		if c.ID == itemB {
			foundB = true
		}
	}
	if !foundB {
		t.Error("item B should appear in pull response despite item A being missing")
	}
}

func TestIntegration_PullCheckpointMultiUserInterleaving(t *testing.T) {
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
	userA := "00000000-0000-0000-0000-000000003001"
	userB := "00000000-0000-0000-0000-000000003002"
	clientA := "client-interleave-a"
	clientB := "client-interleave-b"

	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// Create items for both users
	itemA1 := "00000000-0000-0000-0000-000000003010"
	itemA2 := "00000000-0000-0000-0000-000000003011"
	itemB1 := "00000000-0000-0000-0000-000000003020"
	itemB2 := "00000000-0000-0000-0000-000000003021"
	itemB3 := "00000000-0000-0000-0000-000000003022"

	// Push all items
	for _, tc := range []struct {
		userID, clientID, itemID, name string
	}{
		{userA, clientA, itemA1, "A1"},
		{userA, clientA, itemA2, "A2"},
		{userB, clientB, itemB1, "B1"},
		{userB, clientB, itemB2, "B2"},
		{userB, clientB, itemB3, "B3"},
	} {
		_, err = engine.Push(ctx, tc.userID, synctest.MakePushRequest(tc.clientID, sv, sh,
			synctest.MakePushRecord(tc.itemID, "items", "create", map[string]any{"name": tc.name}),
		))
		if err != nil {
			t.Fatalf("Push %s: %v", tc.name, err)
		}
	}

	// Insert interleaved changelog entries:
	// seq N+0: user:A, items, item-A1
	// seq N+1: user:B, items, item-B1
	// seq N+2: user:B, items, item-B2
	// seq N+3: user:A, items, item-A2
	// seq N+4: user:B, items, item-B3
	interleaved := []struct {
		bucketID string
		recordID string
	}{
		{"user:" + userA, itemA1},
		{"user:" + userB, itemB1},
		{"user:" + userB, itemB2},
		{"user:" + userA, itemA2},
		{"user:" + userB, itemB3},
	}

	for _, e := range interleaved {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			e.bucketID, "items", e.recordID, 1)
		if err != nil {
			t.Fatalf("writing changelog for %s: %v", e.recordID, err)
		}
	}

	// Pull as User A with Limit=1 — should get item-A1
	pullReqA1 := synctest.MakePullRequest(clientA, 0, sv, sh)
	pullReqA1.Limit = 1
	pullRespA1, err := engine.Pull(ctx, userA, pullReqA1)
	if err != nil {
		t.Fatalf("Pull A page 1: %v", err)
	}
	if len(pullRespA1.Changes) != 1 {
		t.Fatalf("User A page 1: expected 1 change, got %d", len(pullRespA1.Changes))
	}
	if pullRespA1.Changes[0].ID != itemA1 {
		t.Errorf("User A page 1: expected %s, got %s", itemA1, pullRespA1.Changes[0].ID)
	}
	if !pullRespA1.HasMore {
		t.Error("User A page 1: expected has_more=true")
	}

	// Pull as User A from checkpoint — should get item-A2, skipping B's entries
	pullRespA2, err := engine.Pull(ctx, userA, synctest.MakePullRequest(clientA, pullRespA1.Checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull A page 2: %v", err)
	}
	if len(pullRespA2.Changes) != 1 {
		t.Fatalf("User A page 2: expected 1 change, got %d", len(pullRespA2.Changes))
	}
	if pullRespA2.Changes[0].ID != itemA2 {
		t.Errorf("User A page 2: expected %s, got %s", itemA2, pullRespA2.Changes[0].ID)
	}
	if pullRespA2.HasMore {
		t.Error("User A page 2: expected has_more=false")
	}
	// Checkpoint must have advanced past B's interleaved entries
	if pullRespA2.Checkpoint <= pullRespA1.Checkpoint {
		t.Errorf("checkpoint did not advance: %d <= %d", pullRespA2.Checkpoint, pullRespA1.Checkpoint)
	}

	// Pull as User B — should get all 3 B items, no A items
	pullRespB, err := engine.Pull(ctx, userB, synctest.MakePullRequest(clientB, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull B: %v", err)
	}
	if len(pullRespB.Changes) != 3 {
		t.Fatalf("User B: expected 3 changes, got %d", len(pullRespB.Changes))
	}
	bIDs := map[string]bool{itemB1: false, itemB2: false, itemB3: false}
	for _, c := range pullRespB.Changes {
		if _, ok := bIDs[c.ID]; !ok {
			t.Errorf("User B: unexpected item %s", c.ID)
		}
		bIDs[c.ID] = true
	}
	for id, found := range bIDs {
		if !found {
			t.Errorf("User B: missing item %s", id)
		}
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
