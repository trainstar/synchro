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

func snapshotClientReady(t *testing.T, ctx context.Context, engine *synchro.Engine, userID, clientID string, schemaVersion int64, schemaHash string) int64 {
	t.Helper()

	var (
		cursor     *synchro.SnapshotCursor
		checkpoint int64
	)
	for {
		resp, err := engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
			ClientID:      clientID,
			Cursor:        cursor,
			Limit:         100,
			SchemaVersion: schemaVersion,
			SchemaHash:    schemaHash,
		})
		if err != nil {
			t.Fatalf("Snapshot bootstrap for %s: %v", clientID, err)
		}
		if !resp.HasMore {
			pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, resp.Checkpoint, schemaVersion, schemaHash))
			if err != nil {
				t.Fatalf("Post-snapshot pull readiness check for %s: %v", clientID, err)
			}
			if pullResp.SnapshotRequired {
				t.Fatalf("client %s still requires snapshot after completed snapshot", clientID)
			}
			return resp.Checkpoint
		}
		cursor = resp.Cursor
		checkpoint = resp.Checkpoint
		if checkpoint < 0 {
			t.Fatalf("invalid snapshot checkpoint for %s: %d", clientID, checkpoint)
		}
	}
}

func TestIntegration_PushPullRoundTrip(t *testing.T) {
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
	clientID := "test-client-1"

	// Register client
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a create
	orderID := "00000000-0000-0000-0000-000000000010"
	pushReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "123 Main St",
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
		"user:"+userID, "orders", orderID, 1)
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
	if pullResp.Changes[0].ID != orderID {
		t.Errorf("pulled record ID = %q, want %q", pullResp.Changes[0].ID, orderID)
	}

	// Push an update
	updateReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "update", map[string]any{
			"ship_address": "456 Oak Ave",
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
		"user:"+userID, "orders", orderID, 2)
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
		synctest.MakePushRecord(orderID, "orders", "delete", nil),
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
		"user:"+userID, "orders", orderID, 3)
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
	if pullResp3.Deletes[0].ID != orderID {
		t.Errorf("deleted record ID = %q, want %q", pullResp3.Deletes[0].ID, orderID)
	}
}

func TestIntegration_RLSEnforcement(t *testing.T) {
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	
	ctx := context.Background()

	// Create a temporary engine with adminDB for RLS policy generation
	rlsEngine, rlsErr := synchro.NewEngine(ctx, &synchro.Config{
		DB:     adminDB,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if rlsErr != nil {
		t.Fatalf("NewEngine for RLS: %v", rlsErr)
	}

	// Apply RLS policies via superuser
	policies := synchro.GenerateRLSPolicies(rlsEngine.Registry(), "user_id")
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	// Clean up RLS policies after test so they don't persist
	t.Cleanup(func() {
		for _, cfg := range rlsEngine.Registry().All() {
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	// Engine uses non-superuser connection so RLS is enforced
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       appDB,
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
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// User A creates a record
	orderID := "00000000-0000-0000-0000-000000000020"
	pushReq := synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "100 Elm St",
		}),
	)
	resp, err := engine.Push(ctx, userA, pushReq)
	if err != nil {
		t.Fatalf("Push by user A: %v", err)
	}
	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(resp.Accepted), resp.Rejected)
	}

	// User B tries to update user A's record. RLS should block it
	updateReq := synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "update", map[string]any{
			"ship_address": "Hijacked",
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
		synctest.MakePushRecord(orderID, "orders", "update", map[string]any{
			"ship_address": "Updated by A",
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
	clientID := "test-ryow"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Create
	orderID := "00000000-0000-0000-0000-000000000030"
	createResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "RYOW test",
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

	// Update. pass BaseUpdatedAt so optimistic concurrency resolves correctly
	updateData, _ := json.Marshal(map[string]any{"ship_address": "RYOW updated"})
	updateResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synchro.PushRecord{
			ID: orderID, TableName: "orders", Operation: "update",
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
		synctest.MakePushRecord(orderID, "orders", "delete", nil),
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
	snapshotClientReady(t, ctx, engine, userID, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientB, sv, sh)

	// Push a record
	orderID := "00000000-0000-0000-0000-000000000040"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "800 Compact Ln"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Simulate WAL: write 20 changelog entries
	for i := 0; i < 20; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "orders", orderID, 2)
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

	// Run compaction. safe seq = MIN(A, B) = B's checkpoint
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
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a record and create changelog entries
	orderID := "00000000-0000-0000-0000-000000000041"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "801 Stale Dr"}),
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

	// Pull to advance checkpoint
	_, err = engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Wait so stale threshold kicks in
	time.Sleep(5 * time.Millisecond)

	// Run compaction. should deactivate the client
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
	clientID := "test-resync-required"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push 5 records to create actual DB rows, then insert changelog entries
	for i := 0; i < 5; i++ {
		recID := fmt.Sprintf("00000000-0000-0000-0000-0000000050%02d", i)
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(recID, "orders", "create", map[string]any{"ship_address": fmt.Sprintf("order-%d", i)}),
		))
		if err != nil {
			t.Fatalf("Push order %d: %v", i, err)
		}
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "orders", recID, 1)
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
			synctest.MakePushRecord(recID, "orders", "create", map[string]any{"ship_address": fmt.Sprintf("new-order-%d", i)}),
		))
		if err != nil {
			t.Fatalf("Push new order %d: %v", i, err)
		}
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "orders", recID, 1)
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

	// Now pull with old checkpoint. should get snapshot_required
	pullResp2, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, pullResp.Checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull with stale checkpoint: %v", err)
	}
	if !pullResp2.SnapshotRequired {
		t.Fatal("expected SnapshotRequired = true")
	}
	if pullResp2.SnapshotReason != synchro.SnapshotReasonCheckpointBeforeLimit {
		t.Fatalf("snapshot reason = %q, want %q", pullResp2.SnapshotReason, synchro.SnapshotReasonCheckpointBeforeLimit)
	}
	if pullResp2.Checkpoint != pullResp.Checkpoint {
		t.Errorf("checkpoint should be preserved: got %d, want %d", pullResp2.Checkpoint, pullResp.Checkpoint)
	}
}

func TestIntegration_SnapshotRoundTrip(t *testing.T) {
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
	clientID := "test-resync-roundtrip"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create several records across tables
	orderIDs := []string{
		"00000000-0000-0000-0000-000000000050",
		"00000000-0000-0000-0000-000000000051",
		"00000000-0000-0000-0000-000000000052",
	}
	for _, id := range orderIDs {
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(id, "orders", "create", map[string]any{"ship_address": "resync " + id}),
		))
		if err != nil {
			t.Fatalf("Push %s: %v", id, err)
		}
	}

	// Write changelog entries (simulate WAL)
	for _, id := range orderIDs {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "orders", id, 1)
		if err != nil {
			t.Fatalf("writing changelog: %v", err)
		}
	}

	// Resync. page through all records with small limit
	var allRecords []synchro.Record
	var cursor *synchro.SnapshotCursor
	var finalCheckpoint int64
	pages := 0

	for {
		snapshotResp, err := engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
			ClientID:      clientID,
			Cursor:        cursor,
			Limit:         2, // Small limit to force pagination
			SchemaVersion: sv,
			SchemaHash:    sh,
		})
		if err != nil {
			t.Fatalf("Snapshot page %d: %v", pages, err)
		}
		pages++

		allRecords = append(allRecords, snapshotResp.Records...)

		if !snapshotResp.HasMore {
			if snapshotResp.Checkpoint == 0 {
				t.Error("expected nonzero checkpoint on final page")
			}
			finalCheckpoint = snapshotResp.Checkpoint
			break
		}

		cursor = snapshotResp.Cursor
		if cursor == nil {
			t.Fatal("expected cursor on intermediate page")
		}

		if pages > 50 {
			t.Fatal("too many pages. possible infinite loop")
		}
	}

	if len(allRecords) < len(orderIDs) {
		t.Errorf("snapshot returned %d records, expected at least %d", len(allRecords), len(orderIDs))
	}
	t.Logf("snapshot completed in %d pages with %d records", pages, len(allRecords))

	// Normal pull should work after snapshot when resuming from the snapshot checkpoint.
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, finalCheckpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull after snapshot: %v", err)
	}
	if len(pullResp.Changes) == 0 && len(pullResp.Deletes) == 0 {
		t.Log("no changes in pull after snapshot (changelog may have been consumed)")
	}
}

func TestIntegration_SnapshotReactivatesClient(t *testing.T) {
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
	orderID := "00000000-0000-0000-0000-000000000060"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "900 Activate Ave"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Write changelog entry
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "orders", orderID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	initialSnapshot, err := engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
		ClientID:      clientID,
		SchemaVersion: sv,
		SchemaHash:    sh,
	})
	if err != nil {
		t.Fatalf("Initial Snapshot: %v", err)
	}
	for initialSnapshot.HasMore {
		initialSnapshot, err = engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
			ClientID:      clientID,
			Cursor:        initialSnapshot.Cursor,
			SchemaVersion: sv,
			SchemaHash:    sh,
		})
		if err != nil {
			t.Fatalf("Initial Snapshot continuation: %v", err)
		}
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
		t.Fatal("expected client to be deactivated before snapshot")
	}

	snapshotResp, err := engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
		ClientID:      clientID,
		SchemaVersion: sv,
		SchemaHash:    sh,
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshotResp.HasMore {
		for snapshotResp.HasMore {
			snapshotResp, err = engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
				ClientID:      clientID,
				Cursor:        snapshotResp.Cursor,
				SchemaVersion: sv,
				SchemaHash:    sh,
			})
			if err != nil {
				t.Fatalf("Snapshot continuation: %v", err)
			}
		}
	}

	// Verify client is reactivated
	err = db.QueryRowContext(ctx,
		"SELECT is_active FROM sync_clients WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&isActive)
	if err != nil {
		t.Fatalf("querying client after snapshot: %v", err)
	}
	if !isActive {
		t.Error("expected client to be reactivated after snapshot")
	}
}

func TestIntegration_PullBucketIsolation(t *testing.T) {
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
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// User A pushes order-A
	orderA := "00000000-0000-0000-0000-000000000b01"
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderA, "orders", "create", map[string]any{"ship_address": "100 Alpha St"}),
	))
	if err != nil {
		t.Fatalf("Push order-A: %v", err)
	}

	// User B pushes order-B
	orderB := "00000000-0000-0000-0000-000000000b02"
	_, err = engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(orderB, "orders", "create", map[string]any{"ship_address": "200 Beta St"}),
	))
	if err != nil {
		t.Fatalf("Push order-B: %v", err)
	}

	// Simulate WAL: assign each order to its owner's bucket
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userA, "orders", orderA, 1)
	if err != nil {
		t.Fatalf("writing changelog for order-A: %v", err)
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userB, "orders", orderB, 1)
	if err != nil {
		t.Fatalf("writing changelog for order-B: %v", err)
	}

	// User A pulls: should only see order-A
	pullRespA, err := engine.Pull(ctx, userA, synctest.MakePullRequest(clientA, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull A: %v", err)
	}
	if len(pullRespA.Changes) != 1 {
		t.Fatalf("User A: expected 1 change, got %d", len(pullRespA.Changes))
	}
	if pullRespA.Changes[0].ID != orderA {
		t.Errorf("User A: expected order %s, got %s", orderA, pullRespA.Changes[0].ID)
	}

	// User B pulls: should only see order-B
	pullRespB, err := engine.Pull(ctx, userB, synctest.MakePullRequest(clientB, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull B: %v", err)
	}
	if len(pullRespB.Changes) != 1 {
		t.Fatalf("User B: expected 1 change, got %d", len(pullRespB.Changes))
	}
	if pullRespB.Changes[0].ID != orderB {
		t.Errorf("User B: expected order %s, got %s", orderB, pullRespB.Changes[0].ID)
	}
}

func TestIntegration_ProtectedColumnEnforcement(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000c01"
	clientID := "client-protected"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	orderID := "00000000-0000-0000-0000-000000000c10"
	attackerID := "00000000-0000-0000-0000-00000000dead"
	fakeTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	// Push create with protected columns injected
	pushResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "300 Protected Ave",
			"created_at":   fakeTime.Format(time.RFC3339),
			"updated_at":   fakeTime.Format(time.RFC3339),
			"user_id":      attackerID,
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
		"SELECT user_id, created_at, updated_at FROM orders WHERE id = $1", orderID,
	).Scan(&dbUserID, &dbCreatedAt, &dbUpdatedAt)
	if err != nil {
		t.Fatalf("querying order: %v", err)
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

	// Push update with protected timestamp. pass BaseUpdatedAt for optimistic concurrency
	updateData, _ := json.Marshal(map[string]any{
		"ship_address": "400 Updated Blvd",
		"updated_at":   fakeTime.Format(time.RFC3339),
	})
	updateResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synchro.PushRecord{
			ID: orderID, TableName: "orders", Operation: "update",
			Data: updateData, ClientUpdatedAt: time.Now().UTC(), BaseUpdatedAt: &dbUpdatedAt,
		},
	))
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}
	if len(updateResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted update, got %d", len(updateResp.Accepted))
	}

	var dbShipAddr string
	var dbUpdatedAt2 time.Time
	err = db.QueryRowContext(ctx,
		"SELECT ship_address, updated_at FROM orders WHERE id = $1", orderID,
	).Scan(&dbShipAddr, &dbUpdatedAt2)
	if err != nil {
		t.Fatalf("querying updated order: %v", err)
	}
	if dbShipAddr != "400 Updated Blvd" {
		t.Errorf("ship_address = %q, want %q", dbShipAddr, "400 Updated Blvd")
	}
	if dbUpdatedAt2.Equal(fakeTime) {
		t.Error("updated_at after update should be server-assigned, not the fake timestamp")
	}
}

func TestIntegration_SoftDeleteLifecycle(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000d01"
	clientID := "client-softdelete"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	orderID := "00000000-0000-0000-0000-000000000d10"

	// Create
	createResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "500 Lifecycle Ln"}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(createResp.Accepted) != 1 {
		t.Fatalf("create: expected 1 accepted, got %d", len(createResp.Accepted))
	}

	// Delete
	deleteResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "delete", nil),
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

	// Idempotent delete (same order again)
	deleteResp2, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "delete", nil),
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
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "600 Resurrection Dr"}),
	))
	if err != nil {
		t.Fatalf("Push resurrect: %v", err)
	}
	if len(resurrectResp.Accepted) != 1 {
		t.Fatalf("resurrect: expected 1 accepted, got %d (rejected: %v)", len(resurrectResp.Accepted), resurrectResp.Rejected)
	}

	// Verify DB: deleted_at is NULL, ship_address is new value
	var dbShipAddr string
	var dbDeletedAt *time.Time
	err = db.QueryRowContext(ctx,
		"SELECT ship_address, deleted_at FROM orders WHERE id = $1", orderID,
	).Scan(&dbShipAddr, &dbDeletedAt)
	if err != nil {
		t.Fatalf("querying resurrected order: %v", err)
	}
	if dbDeletedAt != nil {
		t.Error("deleted_at should be NULL after resurrection")
	}
	if dbShipAddr != "600 Resurrection Dr" {
		t.Errorf("ship_address = %q, want %q", dbShipAddr, "600 Resurrection Dr")
	}
}

func TestIntegration_ReadOnlyTableRejection(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000e01"
	clientID := "client-readonly"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	prodID := "00000000-0000-0000-0000-000000000e10"
	pushResp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(prodID, "products", "create", map[string]any{"name": "Hacked Product"}),
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
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM products WHERE id = $1", prodID).Scan(&count)
	if err != nil {
		t.Fatalf("querying products: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows in products, got %d", count)
	}
}

func TestIntegration_GlobalBucketPullVisibility(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000000f01"
	clientID := "client-global"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Insert a global category (user_id = NULL) directly in DB
	catID := "00000000-0000-0000-0000-000000000f10"
	_, err = db.ExecContext(ctx,
		"INSERT INTO categories (id, user_id, name) VALUES ($1, NULL, $2)", catID, "Global Category")
	if err != nil {
		t.Fatalf("inserting global category: %v", err)
	}

	// Simulate WAL: assign to "global" bucket
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"global", "categories", catID, 1)
	if err != nil {
		t.Fatalf("writing global changelog: %v", err)
	}

	// Pull as any user. should see the global category
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	found := false
	for _, c := range pullResp.Changes {
		if c.ID == catID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("global category %s not found in pull response (got %d changes)", catID, len(pullResp.Changes))
	}
}

func TestIntegration_PullPagination(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000001001"
	clientID := "client-pagination"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push 5 orders
	orderIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		orderIDs[i] = fmt.Sprintf("00000000-0000-0000-0000-00000010%04d", i)
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(orderIDs[i], "orders", "create", map[string]any{
				"ship_address": fmt.Sprintf("Order %d", i),
			}),
		))
		if err != nil {
			t.Fatalf("Push order %d: %v", i, err)
		}
	}

	// Insert 5 changelog entries
	for _, id := range orderIDs {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "orders", id, 1)
		if err != nil {
			t.Fatalf("writing changelog: %v", err)
		}
	}

	// Pull with Limit=2. page through all results
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
			t.Fatal("too many pages. possible infinite loop")
		}
	}

	if len(allIDs) != 5 {
		t.Fatalf("expected 5 total orders across pages, got %d", len(allIDs))
	}
	for _, id := range orderIDs {
		if !allIDs[id] {
			t.Errorf("order %s not found across paginated pulls", id)
		}
	}
	if pages < 2 {
		t.Errorf("expected at least 2 pages with Limit=2 and 5 orders, got %d", pages)
	}
}

func TestIntegration_RLSBroadCoverage(t *testing.T) {
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	
	ctx := context.Background()

	// Create a temporary engine with adminDB for RLS policy generation
	rlsEngine, rlsErr := synchro.NewEngine(ctx, &synchro.Config{
		DB:     adminDB,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if rlsErr != nil {
		t.Fatalf("NewEngine for RLS: %v", rlsErr)
	}

	// Apply RLS policies via superuser
	policies := synchro.GenerateRLSPolicies(rlsEngine.Registry(), "user_id")
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	// Clean up RLS policies after test so they don't persist
	t.Cleanup(func() {
		for _, cfg := range rlsEngine.Registry().All() {
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	// Engine uses non-superuser connection so RLS is enforced
	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       appDB,
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
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// User A: full CRUD lifecycle under RLS
	orderID := "00000000-0000-0000-0000-000000001110"

	// Create
	createResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "100 RLS Ave"}),
	))
	if err != nil {
		t.Fatalf("User A create: %v", err)
	}
	if len(createResp.Accepted) != 1 {
		t.Fatalf("User A create: expected 1 accepted, got %d (rejected: %v)", len(createResp.Accepted), createResp.Rejected)
	}
	createTS := *createResp.Accepted[0].ServerUpdatedAt

	// Update. pass BaseUpdatedAt so optimistic concurrency resolves correctly
	updateData, _ := json.Marshal(map[string]any{"ship_address": "200 RLS Blvd"})
	updateResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synchro.PushRecord{
			ID: orderID, TableName: "orders", Operation: "update",
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
		synctest.MakePushRecord(orderID, "orders", "delete", nil),
	))
	if err != nil {
		t.Fatalf("User A delete: %v", err)
	}
	if len(deleteResp.Accepted) != 1 {
		t.Fatalf("User A delete: expected 1 accepted, got %d", len(deleteResp.Accepted))
	}

	// User B: attempt to create with User A's ID as owner → should fail
	orderID2 := "00000000-0000-0000-0000-000000001111"
	createRespB, err := engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(orderID2, "orders", "create", map[string]any{
			"ship_address": "300 Sneaky St",
			"user_id":      userA, // attempt to set owner to A
		}),
	))
	if err != nil {
		t.Fatalf("User B create: %v", err)
	}
	// Engine enforces user_id = userB, so the order should be created under userB's ownership
	// (protected column enforcement sets user_id to authenticated user).
	// RLS should allow this since the row will have user_id=userB.
	if len(createRespB.Accepted) != 1 {
		t.Fatalf("User B create: expected 1 accepted (owner enforced to B), got %d (rejected: %v)",
			len(createRespB.Accepted), createRespB.Rejected)
	}

	// Verify the order is owned by B, not A (query via admin to bypass RLS)
	var ownerID string
	err = adminDB.QueryRowContext(ctx, "SELECT user_id FROM orders WHERE id = $1", orderID2).Scan(&ownerID)
	if err != nil {
		t.Fatalf("querying order owner: %v", err)
	}
	if ownerID != userB {
		t.Errorf("order owner = %q, want %q (should be enforced to authenticated user)", ownerID, userB)
	}

	// User B: attempt to delete User A's order → RLS blocks
	deleteRespB, err := engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "delete", nil),
	))
	if err != nil {
		t.Fatalf("User B delete attempt: %v", err)
	}
	if len(deleteRespB.Accepted) != 0 {
		t.Errorf("User B delete User A's order: expected 0 accepted, got %d", len(deleteRespB.Accepted))
	}

	// Child table: User A creates order_details referencing their order
	// First resurrect User A's order (it was deleted above)
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "400 Child Test Ave"}),
	))
	if err != nil {
		t.Fatalf("Resurrect order for child test: %v", err)
	}

	detailID := "00000000-0000-0000-0000-000000001112"
	detailResp, err := engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(detailID, "order_details", "create", map[string]any{
			"order_id":     orderID,
			"product_name": "Widget A",
		}),
	))
	if err != nil {
		t.Fatalf("User A create order_details: %v", err)
	}
	if len(detailResp.Accepted) != 1 {
		t.Fatalf("User A order_details: expected 1 accepted, got %d (rejected: %v)",
			len(detailResp.Accepted), detailResp.Rejected)
	}

	// User B attempts order_details referencing User A's order → RLS blocks.
	// RLS policy violations on INSERT abort the PostgreSQL transaction,
	// so Push returns an error (not a clean rejection).
	detailID2 := "00000000-0000-0000-0000-000000001113"
	detailRespB, err := engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(detailID2, "order_details", "create", map[string]any{
			"order_id":     orderID,
			"product_name": "Widget B",
		}),
	))
	if err == nil && len(detailRespB.Accepted) > 0 {
		t.Error("User B should not be able to create order_details on A's order")
	}
	// Either Push returns an error (tx aborted by RLS) or the record is rejected. both are correct.
}

func TestIntegration_PullHydrationMissingRecord(t *testing.T) {
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
	userID := "00000000-0000-0000-0000-000000002001"
	clientID := "client-hydration-missing"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push two orders so they exist in the DB
	orderA := "00000000-0000-0000-0000-000000002010"
	orderB := "00000000-0000-0000-0000-000000002011"

	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderA, "orders", "create", map[string]any{"ship_address": "100 Alpha St"}),
	))
	if err != nil {
		t.Fatalf("Push order A: %v", err)
	}
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderB, "orders", "create", map[string]any{"ship_address": "200 Beta St"}),
	))
	if err != nil {
		t.Fatalf("Push order B: %v", err)
	}

	// Insert changelog entries for both
	for _, id := range []string{orderA, orderB} {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userID, "orders", id, 1)
		if err != nil {
			t.Fatalf("writing changelog for %s: %v", id, err)
		}
	}

	// Hard-delete order A from the DB (simulating out-of-band removal)
	_, err = db.ExecContext(ctx, "DELETE FROM orders WHERE id = $1", orderA)
	if err != nil {
		t.Fatalf("hard-deleting order A: %v", err)
	}

	// Pull. should succeed without error, gracefully skipping the missing record
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Order B should still appear
	foundB := false
	for _, c := range pullResp.Changes {
		if c.ID == orderA {
			t.Error("hard-deleted order A should not appear in pull response")
		}
		if c.ID == orderB {
			foundB = true
		}
	}
	if !foundB {
		t.Error("order B should appear in pull response despite order A being missing")
	}
}

func TestIntegration_PullCheckpointMultiUserInterleaving(t *testing.T) {
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
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// Create orders for both users
	orderA1 := "00000000-0000-0000-0000-000000003010"
	orderA2 := "00000000-0000-0000-0000-000000003011"
	orderB1 := "00000000-0000-0000-0000-000000003020"
	orderB2 := "00000000-0000-0000-0000-000000003021"
	orderB3 := "00000000-0000-0000-0000-000000003022"

	// Push all orders
	for _, tc := range []struct {
		userID, clientID, orderID, name string
	}{
		{userA, clientA, orderA1, "A1"},
		{userA, clientA, orderA2, "A2"},
		{userB, clientB, orderB1, "B1"},
		{userB, clientB, orderB2, "B2"},
		{userB, clientB, orderB3, "B3"},
	} {
		_, err = engine.Push(ctx, tc.userID, synctest.MakePushRequest(tc.clientID, sv, sh,
			synctest.MakePushRecord(tc.orderID, "orders", "create", map[string]any{"ship_address": tc.name}),
		))
		if err != nil {
			t.Fatalf("Push %s: %v", tc.name, err)
		}
	}

	// Insert interleaved changelog entries:
	// seq N+0: user:A, orders, order-A1
	// seq N+1: user:B, orders, order-B1
	// seq N+2: user:B, orders, order-B2
	// seq N+3: user:A, orders, order-A2
	// seq N+4: user:B, orders, order-B3
	interleaved := []struct {
		bucketID string
		recordID string
	}{
		{"user:" + userA, orderA1},
		{"user:" + userB, orderB1},
		{"user:" + userB, orderB2},
		{"user:" + userA, orderA2},
		{"user:" + userB, orderB3},
	}

	for _, e := range interleaved {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			e.bucketID, "orders", e.recordID, 1)
		if err != nil {
			t.Fatalf("writing changelog for %s: %v", e.recordID, err)
		}
	}

	// Pull as User A with Limit=1. should get order-A1
	pullReqA1 := synctest.MakePullRequest(clientA, 0, sv, sh)
	pullReqA1.Limit = 1
	pullRespA1, err := engine.Pull(ctx, userA, pullReqA1)
	if err != nil {
		t.Fatalf("Pull A page 1: %v", err)
	}
	if len(pullRespA1.Changes) != 1 {
		t.Fatalf("User A page 1: expected 1 change, got %d", len(pullRespA1.Changes))
	}
	if pullRespA1.Changes[0].ID != orderA1 {
		t.Errorf("User A page 1: expected %s, got %s", orderA1, pullRespA1.Changes[0].ID)
	}
	if !pullRespA1.HasMore {
		t.Error("User A page 1: expected has_more=true")
	}

	// Pull as User A from checkpoint. should get order-A2, skipping B's entries
	pullRespA2, err := engine.Pull(ctx, userA, synctest.MakePullRequest(clientA, pullRespA1.Checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull A page 2: %v", err)
	}
	if len(pullRespA2.Changes) != 1 {
		t.Fatalf("User A page 2: expected 1 change, got %d", len(pullRespA2.Changes))
	}
	if pullRespA2.Changes[0].ID != orderA2 {
		t.Errorf("User A page 2: expected %s, got %s", orderA2, pullRespA2.Changes[0].ID)
	}
	if pullRespA2.HasMore {
		t.Error("User A page 2: expected has_more=false")
	}
	// Checkpoint must have advanced past B's interleaved entries
	if pullRespA2.Checkpoint <= pullRespA1.Checkpoint {
		t.Errorf("checkpoint did not advance: %d <= %d", pullRespA2.Checkpoint, pullRespA1.Checkpoint)
	}

	// Pull as User B. should get all 3 B orders, no A orders
	pullRespB, err := engine.Pull(ctx, userB, synctest.MakePullRequest(clientB, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull B: %v", err)
	}
	if len(pullRespB.Changes) != 3 {
		t.Fatalf("User B: expected 3 changes, got %d", len(pullRespB.Changes))
	}
	bIDs := map[string]bool{orderB1: false, orderB2: false, orderB3: false}
	for _, c := range pullRespB.Changes {
		if _, ok := bIDs[c.ID]; !ok {
			t.Errorf("User B: unexpected order %s", c.ID)
		}
		bIDs[c.ID] = true
	}
	for id, found := range bIDs {
		if !found {
			t.Errorf("User B: missing order %s", id)
		}
	}
}

func TestIntegration_RunCompaction_NotConfigured(t *testing.T) {
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

// collectSnapshotRecords pages through a full snapshot and returns all records.
func collectSnapshotRecords(t *testing.T, ctx context.Context, engine *synchro.Engine, userID, clientID string, sv int64, sh string) []synchro.Record {
	t.Helper()
	var all []synchro.Record
	var cursor *synchro.SnapshotCursor
	for i := 0; i < 100; i++ {
		resp, err := engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
			ClientID:      clientID,
			Cursor:        cursor,
			Limit:         100,
			SchemaVersion: sv,
			SchemaHash:    sh,
		})
		if err != nil {
			t.Fatalf("Snapshot page %d for user %s: %v", i, userID, err)
		}
		all = append(all, resp.Records...)
		if !resp.HasMore {
			break
		}
		cursor = resp.Cursor
	}
	return all
}

func TestIntegration_SnapshotUserIsolation(t *testing.T) {
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	
	ctx := context.Background()

	// Apply RLS policies via superuser
	rlsEngine, rlsErr := synchro.NewEngine(ctx, &synchro.Config{
		DB:     adminDB,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if rlsErr != nil {
		t.Fatalf("NewEngine for RLS: %v", rlsErr)
	}

	policies := synchro.GenerateRLSPolicies(rlsEngine.Registry(), "user_id")
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	t.Cleanup(func() {
		for _, cfg := range rlsEngine.Registry().All() {
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       appDB,
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
	userA := "00000000-0000-0000-0000-a00000000001"
	userB := "00000000-0000-0000-0000-b00000000002"
	clientA := "snapshot-iso-A"
	clientB := "snapshot-iso-B"

	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	// Bootstrap both clients
	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// User A pushes 3 orders
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("00000000-0000-0000-0000-aaa0000000%02d", i)
		_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
			synctest.MakePushRecord(id, "orders", "create", map[string]any{"ship_address": "A-addr-" + id}),
		))
		if err != nil {
			t.Fatalf("Push A order %d: %v", i, err)
		}
		// Write changelog so snapshot can find a checkpoint
		_, err = adminDB.ExecContext(ctx, "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userA, "orders", id, 1)
		if err != nil {
			t.Fatalf("changelog A: %v", err)
		}
	}

	// User B pushes 2 orders
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("00000000-0000-0000-0000-bbb0000000%02d", i)
		_, err = engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
			synctest.MakePushRecord(id, "orders", "create", map[string]any{"ship_address": "B-addr-" + id}),
		))
		if err != nil {
			t.Fatalf("Push B order %d: %v", i, err)
		}
		_, err = adminDB.ExecContext(ctx, "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			"user:"+userB, "orders", id, 1)
		if err != nil {
			t.Fatalf("changelog B: %v", err)
		}
	}

	// Snapshot as User B. should only see User B's orders
	recordsB := collectSnapshotRecords(t, ctx, engine, userB, clientB, sv, sh)
	orderCountB := 0
	for _, r := range recordsB {
		if r.TableName == "orders" {
			orderCountB++
			// Verify it's User B's record by checking data
			var data map[string]any
			if err := json.Unmarshal(r.Data, &data); err != nil {
				t.Fatalf("unmarshal order data: %v", err)
			}
			addr, _ := data["ship_address"].(string)
			if len(addr) > 0 && addr[0] != 'B' {
				t.Errorf("User B snapshot contains User A's order: id=%s addr=%s", r.ID, addr)
			}
		}
	}
	if orderCountB != 2 {
		t.Errorf("User B snapshot: got %d orders, want 2", orderCountB)
	}

	// Snapshot as User A. should only see User A's orders
	recordsA := collectSnapshotRecords(t, ctx, engine, userA, clientA, sv, sh)
	orderCountA := 0
	for _, r := range recordsA {
		if r.TableName == "orders" {
			orderCountA++
			var data map[string]any
			if err := json.Unmarshal(r.Data, &data); err != nil {
				t.Fatalf("unmarshal order data: %v", err)
			}
			addr, _ := data["ship_address"].(string)
			if len(addr) > 0 && addr[0] != 'A' {
				t.Errorf("User A snapshot contains User B's order: id=%s addr=%s", r.ID, addr)
			}
		}
	}
	if orderCountA != 3 {
		t.Errorf("User A snapshot: got %d orders, want 3", orderCountA)
	}
}

func TestIntegration_SnapshotGlobalRecords(t *testing.T) {
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	
	ctx := context.Background()

	rlsEngine, rlsErr := synchro.NewEngine(ctx, &synchro.Config{
		DB:     adminDB,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if rlsErr != nil {
		t.Fatalf("NewEngine for RLS: %v", rlsErr)
	}

	policies := synchro.GenerateRLSPolicies(rlsEngine.Registry(), "user_id")
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	t.Cleanup(func() {
		for _, cfg := range rlsEngine.Registry().All() {
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       appDB,
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
	userA := "00000000-0000-0000-0000-a00000000010"
	userB := "00000000-0000-0000-0000-b00000000020"
	clientA := "snapshot-global-A"
	clientB := "snapshot-global-B"

	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// Insert a global category (user_id IS NULL) via superuser. visible to all
	globalCatID := "00000000-0000-0000-0000-e10000000001"
	_, err = adminDB.ExecContext(ctx,
		"INSERT INTO categories (id, user_id, name) VALUES ($1, NULL, $2)", globalCatID, "Global Category")
	if err != nil {
		t.Fatalf("insert global category: %v", err)
	}

	// User A creates a user-owned category
	userACatID := "00000000-0000-0000-0000-a10000000001"
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(userACatID, "categories", "create", map[string]any{"name": "User A Category"}),
	))
	if err != nil {
		t.Fatalf("Push A category: %v", err)
	}

	// Write changelog for both
	for _, entry := range []struct{ bucket, id string }{
		{"global", globalCatID},
		{"user:" + userA, userACatID},
	} {
		_, err = adminDB.ExecContext(ctx, "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			entry.bucket, "categories", entry.id, 1)
		if err != nil {
			t.Fatalf("changelog: %v", err)
		}
	}

	// User B snapshots. should see global category but NOT User A's category
	recordsB := collectSnapshotRecords(t, ctx, engine, userB, clientB, sv, sh)
	var globalSeen, userASeen bool
	for _, r := range recordsB {
		if r.TableName == "categories" {
			if r.ID == globalCatID {
				globalSeen = true
			}
			if r.ID == userACatID {
				userASeen = true
			}
		}
	}
	if !globalSeen {
		t.Error("User B snapshot missing global category")
	}
	if userASeen {
		t.Error("User B snapshot contains User A's category. ownership leak")
	}

	// User A snapshots. should see both global and own category
	recordsA := collectSnapshotRecords(t, ctx, engine, userA, clientA, sv, sh)
	globalSeen = false
	var ownSeen bool
	for _, r := range recordsA {
		if r.TableName == "categories" {
			if r.ID == globalCatID {
				globalSeen = true
			}
			if r.ID == userACatID {
				ownSeen = true
			}
		}
	}
	if !globalSeen {
		t.Error("User A snapshot missing global category")
	}
	if !ownSeen {
		t.Error("User A snapshot missing own category")
	}
}

func TestIntegration_SnapshotChildTableIsolation(t *testing.T) {
	adminDB, appDB := synctest.TestDBWithAppRole(t)
	
	ctx := context.Background()

	rlsEngine, rlsErr := synchro.NewEngine(ctx, &synchro.Config{
		DB:     adminDB,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if rlsErr != nil {
		t.Fatalf("NewEngine for RLS: %v", rlsErr)
	}

	policies := synchro.GenerateRLSPolicies(rlsEngine.Registry(), "user_id")
	for _, stmt := range policies {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			t.Logf("applying RLS policy (may already exist): %v", err)
		}
	}

	t.Cleanup(func() {
		for _, cfg := range rlsEngine.Registry().All() {
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DISABLE ROW LEVEL SECURITY", cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_read_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_ins_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_upd_"+cfg.TableName, cfg.TableName))
			_, _ = adminDB.ExecContext(ctx, fmt.Sprintf("DROP POLICY IF EXISTS %q ON %q", "sync_write_del_"+cfg.TableName, cfg.TableName))
		}
	})

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:       appDB,
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
	userA := "00000000-0000-0000-0000-a00000000030"
	userB := "00000000-0000-0000-0000-b00000000040"
	clientA := "snapshot-child-A"
	clientB := "snapshot-child-B"

	_, err = engine.RegisterClient(ctx, userA, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userB, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}

	snapshotClientReady(t, ctx, engine, userA, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userB, clientB, sv, sh)

	// User A creates an order and a child order_detail
	orderA := "00000000-0000-0000-0000-aaa0000010a0"
	detailA := "00000000-0000-0000-0000-aaa0000010a1"
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderA, "orders", "create", map[string]any{"ship_address": "A-parent"}),
	))
	if err != nil {
		t.Fatalf("Push A order: %v", err)
	}
	_, err = engine.Push(ctx, userA, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(detailA, "order_details", "create", map[string]any{
			"order_id":     orderA,
			"product_name": "Widget-A",
			"quantity":     3,
		}),
	))
	if err != nil {
		t.Fatalf("Push A detail: %v", err)
	}

	// User B creates an order and a child order_detail
	orderB := "00000000-0000-0000-0000-bbb0000010b0"
	detailB := "00000000-0000-0000-0000-bbb0000010b1"
	_, err = engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(orderB, "orders", "create", map[string]any{"ship_address": "B-parent"}),
	))
	if err != nil {
		t.Fatalf("Push B order: %v", err)
	}
	_, err = engine.Push(ctx, userB, synctest.MakePushRequest(clientB, sv, sh,
		synctest.MakePushRecord(detailB, "order_details", "create", map[string]any{
			"order_id":     orderB,
			"product_name": "Widget-B",
			"quantity":     5,
		}),
	))
	if err != nil {
		t.Fatalf("Push B detail: %v", err)
	}

	// Write changelog entries
	for _, e := range []struct{ bucket, table, id string }{
		{"user:" + userA, "orders", orderA},
		{"user:" + userA, "order_details", detailA},
		{"user:" + userB, "orders", orderB},
		{"user:" + userB, "order_details", detailB},
	} {
		_, err = adminDB.ExecContext(ctx, "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			e.bucket, e.table, e.id, 1)
		if err != nil {
			t.Fatalf("changelog: %v", err)
		}
	}

	// User B snapshots. should see only B's order and B's detail
	recordsB := collectSnapshotRecords(t, ctx, engine, userB, clientB, sv, sh)
	for _, r := range recordsB {
		if r.TableName == "orders" && r.ID == orderA {
			t.Error("User B snapshot contains User A's order")
		}
		if r.TableName == "order_details" && r.ID == detailA {
			t.Error("User B snapshot contains User A's order_detail. child table leak")
		}
	}

	// Verify User B does see their own child record
	var detailBSeen bool
	for _, r := range recordsB {
		if r.TableName == "order_details" && r.ID == detailB {
			detailBSeen = true
		}
	}
	if !detailBSeen {
		t.Error("User B snapshot missing own order_detail")
	}
}

// ---------------------------------------------------------------------------
// Per-Bucket Checkpoint Tests
// ---------------------------------------------------------------------------

func TestIntegration_PerBucketCheckpointInitOnRegister(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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

	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-per-bucket-init"

	resp, err := engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Registration should return per-bucket checkpoints for all subscribed buckets.
	if resp.BucketCheckpoints == nil {
		t.Fatal("BucketCheckpoints is nil after registration")
	}

	userBucket := fmt.Sprintf("user:%s", userID)
	if _, ok := resp.BucketCheckpoints[userBucket]; !ok {
		t.Errorf("missing bucket checkpoint for %q", userBucket)
	}
	if _, ok := resp.BucketCheckpoints["global"]; !ok {
		t.Error("missing bucket checkpoint for 'global'")
	}

	// Verify rows exist in sync_client_checkpoints
	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&count)
	if err != nil {
		t.Fatalf("querying sync_client_checkpoints: %v", err)
	}
	if count < 2 {
		t.Errorf("expected at least 2 bucket checkpoint rows, got %d", count)
	}
}

func TestIntegration_PerBucketPullAdvancesCheckpoints(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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
	clientID := "client-per-bucket-pull"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a record to create changelog entries.
	orderID := "00000000-0000-0000-0000-ab0000000001"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "100 Bucket St"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Simulate WAL: write changelog entries for the push.
	userBucket := fmt.Sprintf("user:%s", userID)
	for i := 0; i < 5; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			userBucket, "orders", orderID, 2)
		if err != nil {
			t.Fatalf("writing changelog entry %d: %v", i, err)
		}
	}

	// Read current per-bucket checkpoints from DB (after snapshot advanced them).
	currentCheckpoints := make(map[string]int64)
	rows, err := db.QueryContext(ctx,
		"SELECT bucket_id, checkpoint FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2",
		userID, clientID)
	if err != nil {
		t.Fatalf("reading current checkpoints: %v", err)
	}
	for rows.Next() {
		var bid string
		var cp int64
		if err := rows.Scan(&bid, &cp); err != nil {
			t.Fatalf("scanning checkpoint: %v", err)
		}
		currentCheckpoints[bid] = cp
	}
	_ = rows.Close()

	preCP := currentCheckpoints[userBucket]

	// Pull using per-bucket checkpoints from current state.
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePerBucketPullRequest(
		clientID, currentCheckpoints, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	if pullResp.BucketCheckpoints == nil {
		t.Fatal("PullResponse.BucketCheckpoints is nil")
	}

	// The user bucket checkpoint should have advanced past the pre-pull value.
	if pullResp.BucketCheckpoints[userBucket] <= preCP {
		t.Errorf("user bucket checkpoint did not advance: pre=%d, post=%d", preCP, pullResp.BucketCheckpoints[userBucket])
	}

	// Verify the per-bucket checkpoints were persisted to the database.
	var persistedCP int64
	err = db.QueryRowContext(ctx,
		"SELECT checkpoint FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2 AND bucket_id = $3",
		userID, clientID, userBucket).Scan(&persistedCP)
	if err != nil {
		t.Fatalf("querying persisted checkpoint: %v", err)
	}
	if persistedCP != pullResp.BucketCheckpoints[userBucket] {
		t.Errorf("persisted checkpoint %d != response checkpoint %d",
			persistedCP, pullResp.BucketCheckpoints[userBucket])
	}
}

func TestIntegration_StaleBucketTriggersRebuild(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
		Compactor:  &synchro.CompactorConfig{StaleThreshold: 24 * time.Hour, BatchSize: 100},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-stale-rebuild"
	userBucket := fmt.Sprintf("user:%s", userID)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push some records and write changelog entries.
	orderID := "00000000-0000-0000-0000-5b0000000001"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "200 Stale Ln"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Write 20 changelog entries to simulate activity.
	for i := 0; i < 20; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			userBucket, "orders", orderID, 2)
		if err != nil {
			t.Fatalf("writing changelog entry %d: %v", i, err)
		}
	}

	// Set the client's per-bucket checkpoint to seq 5 (behind compaction).
	_, err = db.ExecContext(ctx,
		`UPDATE sync_client_checkpoints SET checkpoint = 5
		WHERE user_id = $1 AND client_id = $2 AND bucket_id = $3`,
		userID, clientID, userBucket)
	if err != nil {
		t.Fatalf("setting bucket checkpoint: %v", err)
	}

	// Delete changelog entries with seq <= 10 (simulate compaction).
	_, err = db.ExecContext(ctx,
		"DELETE FROM sync_changelog WHERE seq <= 10")
	if err != nil {
		t.Fatalf("simulating compaction: %v", err)
	}

	// Pull with per-bucket checkpoints (checkpoint 5 is behind min_seq after compaction).
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePerBucketPullRequest(
		clientID,
		map[string]int64{userBucket: 5, "global": 5},
		sv, sh,
	))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// The response should indicate rebuild is needed for the stale bucket.
	if len(pullResp.RebuildBuckets) == 0 {
		t.Fatal("expected RebuildBuckets to be non-empty when checkpoint is behind compaction boundary")
	}

	found := false
	for _, bid := range pullResp.RebuildBuckets {
		if bid == userBucket {
			found = true
		}
	}
	if !found {
		t.Errorf("RebuildBuckets = %v, expected to contain %q", pullResp.RebuildBuckets, userBucket)
	}
}

func TestIntegration_RebuildFlowViaEdges(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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
	clientID := "client-rebuild-flow"
	userBucket := fmt.Sprintf("user:%s", userID)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push 3 records.
	orderIDs := []string{
		"00000000-0000-0000-0000-eb0000000001",
		"00000000-0000-0000-0000-eb0000000002",
		"00000000-0000-0000-0000-eb0000000003",
	}
	for _, oid := range orderIDs {
		_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(oid, "orders", "create", map[string]any{"ship_address": "Rebuild Ave"}),
		))
		if err != nil {
			t.Fatalf("Push %s: %v", oid, err)
		}
	}

	// Insert sync_bucket_edges and changelog entries (simulates WAL consumer behavior).
	for _, oid := range orderIDs {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_bucket_edges (table_name, record_id, bucket_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
			"orders", oid, userBucket)
		if err != nil {
			t.Fatalf("inserting bucket edge for %s: %v", oid, err)
		}
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			userBucket, "orders", oid, 1)
		if err != nil {
			t.Fatalf("inserting changelog for %s: %v", oid, err)
		}
	}

	// Rebuild the user bucket.
	rebuildResp, err := engine.Rebuild(ctx, userID, synctest.MakeRebuildRequest(clientID, userBucket, sv, sh))
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// Should get all 3 records back.
	if len(rebuildResp.Records) < 3 {
		t.Errorf("expected at least 3 rebuild records, got %d", len(rebuildResp.Records))
	}

	// Checkpoint should be set to MAX(seq) from changelog.
	if rebuildResp.Checkpoint == 0 {
		t.Error("rebuild checkpoint should be non-zero")
	}

	// HasMore should be false for small result sets.
	if rebuildResp.HasMore {
		t.Error("expected HasMore=false for small rebuild")
	}

	// Verify the bucket checkpoint was persisted.
	var persistedCP int64
	err = db.QueryRowContext(ctx,
		"SELECT checkpoint FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2 AND bucket_id = $3",
		userID, clientID, userBucket).Scan(&persistedCP)
	if err != nil {
		t.Fatalf("querying persisted checkpoint after rebuild: %v", err)
	}
	if persistedCP != rebuildResp.Checkpoint {
		t.Errorf("persisted checkpoint %d != rebuild checkpoint %d", persistedCP, rebuildResp.Checkpoint)
	}
}

func TestIntegration_LegacyClientFallback(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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
	clientID := "client-legacy-fallback"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a record.
	orderID := "00000000-0000-0000-0000-af0000000001"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "Legacy Ln"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Write changelog entries.
	userBucket := fmt.Sprintf("user:%s", userID)
	for i := 0; i < 5; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			userBucket, "orders", orderID, 2)
		if err != nil {
			t.Fatalf("writing changelog entry %d: %v", i, err)
		}
	}

	// Pull using legacy single checkpoint (no BucketCheckpoints field).
	// This mimics an old client that hasn't been updated.
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Legacy Pull: %v", err)
	}

	// Legacy path: should require snapshot for first pull (checkpoint=0, never pulled).
	// After snapshot, pull should work normally.
	if !pullResp.SnapshotRequired {
		// If snapshot not required, legacy pull returned data normally.
		if pullResp.Checkpoint == 0 {
			t.Error("legacy pull returned checkpoint 0 without requiring snapshot")
		}
	}

	// Either way, verify per-bucket checkpoints were kept in sync.
	var legacySeq int64
	err = db.QueryRowContext(ctx,
		"SELECT COALESCE(last_pull_seq, 0) FROM sync_clients WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&legacySeq)
	if err != nil {
		t.Fatalf("querying legacy checkpoint: %v", err)
	}

	var bucketCP int64
	err = db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(checkpoint), 0) FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&bucketCP)
	if err != nil {
		t.Fatalf("querying bucket checkpoint: %v", err)
	}

	// Legacy and per-bucket checkpoints should stay in sync.
	if legacySeq != bucketCP {
		t.Errorf("legacy checkpoint %d != max bucket checkpoint %d (should stay in sync)", legacySeq, bucketCP)
	}
}

func TestIntegration_CompactionSafeSeqReadsPerBucketCheckpoints(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc: synchro.UserBucket("user_id"),
		Compactor:  &synchro.CompactorConfig{StaleThreshold: 24 * time.Hour, BatchSize: 100},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	sv, sh := schemaFields(t, ctx, engine)
	userID := "00000000-0000-0000-0000-000000000001"
	clientA := "client-compact-bucket-a"
	clientB := "client-compact-bucket-b"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientA, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient A: %v", err)
	}
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientB, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient B: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientA, sv, sh)
	snapshotClientReady(t, ctx, engine, userID, clientB, sv, sh)

	// Push a record and write 20 changelog entries.
	orderID := "00000000-0000-0000-0000-c50000000001"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientA, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{"ship_address": "Compact St"}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	userBucket := fmt.Sprintf("user:%s", userID)
	for i := 0; i < 20; i++ {
		_, err = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			userBucket, "orders", orderID, 2)
		if err != nil {
			t.Fatalf("writing changelog entry %d: %v", i, err)
		}
	}

	// Read current checkpoints for both clients from DB.
	readCheckpoints := func(cid string) map[string]int64 {
		cps := make(map[string]int64)
		cpRows, err := db.QueryContext(ctx,
			"SELECT bucket_id, checkpoint FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2",
			userID, cid)
		if err != nil {
			t.Fatalf("reading checkpoints for %s: %v", cid, err)
		}
		for cpRows.Next() {
			var bid string
			var cp int64
			if err := cpRows.Scan(&bid, &cp); err != nil {
				t.Fatalf("scanning checkpoint: %v", err)
			}
			cps[bid] = cp
		}
		_ = cpRows.Close()
		return cps
	}

	// Client A pulls all (using per-bucket mode).
	pullRespA, err := engine.Pull(ctx, userID, synctest.MakePerBucketPullRequest(
		clientA, readCheckpoints(clientA), sv, sh))
	if err != nil {
		t.Fatalf("Pull A: %v", err)
	}

	// Client B pulls only first 10 (using per-bucket mode).
	pullReqB := synctest.MakePerBucketPullRequest(clientB, readCheckpoints(clientB), sv, sh)
	pullReqB.Limit = 10
	pullRespB, err := engine.Pull(ctx, userID, pullReqB)
	if err != nil {
		t.Fatalf("Pull B: %v", err)
	}

	if pullRespA.Checkpoint <= pullRespB.Checkpoint {
		t.Fatalf("A checkpoint (%d) should be > B checkpoint (%d)", pullRespA.Checkpoint, pullRespB.Checkpoint)
	}

	// Run compaction.
	result, err := engine.RunCompaction(ctx)
	if err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}

	// SafeSeq should equal the minimum per-bucket checkpoint (client B's).
	if result.SafeSeq != pullRespB.Checkpoint {
		t.Errorf("SafeSeq = %d, want %d (should be min of per-bucket checkpoints)", result.SafeSeq, pullRespB.Checkpoint)
	}
	if result.DeletedEntries == 0 {
		t.Error("expected some entries to be deleted by compaction")
	}

	// Both clients can still pull from their checkpoints.
	_, err = engine.Pull(ctx, userID, synctest.MakePerBucketPullRequest(
		clientA, pullRespA.BucketCheckpoints, sv, sh))
	if err != nil {
		t.Fatalf("Pull A after compaction: %v", err)
	}
	_, err = engine.Pull(ctx, userID, synctest.MakePerBucketPullRequest(
		clientB, pullRespB.BucketCheckpoints, sv, sh))
	if err != nil {
		t.Fatalf("Pull B after compaction: %v", err)
	}
}

func TestIntegration_ChecksumPresentOnPull(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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
	clientID := "checksum-test-client"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a record
	orderID := "00000000-0000-0000-0000-0000000000c1"
	pushReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "Checksum Lane",
		}),
	)
	_, err = engine.Push(ctx, userID, pushReq)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Simulate WAL
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "orders", orderID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull and verify checksum is present on each record
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, 0, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pullResp.Changes) == 0 {
		t.Fatal("expected at least 1 change")
	}
	for _, record := range pullResp.Changes {
		if record.Checksum == nil {
			t.Errorf("record %s/%s has nil checksum", record.TableName, record.ID)
		}
	}
}

func TestIntegration_BucketChecksumMatchesXOR(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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
	clientID := "bucket-cs-test"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push 3 records
	bucketID := "user:" + userID
	ids := make([]string, 3)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("00000000-0000-0000-0000-00000000b%03d", i)
		ids[i-1] = id
		pushReq := synctest.MakePushRequest(clientID, sv, sh,
			synctest.MakePushRecord(id, "orders", "create", map[string]any{
				"ship_address": fmt.Sprintf("Address %d", i),
			}),
		)
		if _, err := engine.Push(ctx, userID, pushReq); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
		// Simulate WAL changelog entry
		_, _ = db.ExecContext(ctx,
			"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
			bucketID, "orders", id, 1)
	}

	// Compute checksums from the actual stored rows (row_to_json), matching
	// what hydrateRecords does. This is the same canonical JSON the server uses.
	var expectedXOR int32
	for _, id := range ids {
		var rowJSON string
		if err := db.QueryRowContext(ctx, "SELECT row_to_json(t)::text FROM orders t WHERE id = $1", id).Scan(&rowJSON); err != nil {
			t.Fatalf("row_to_json for %s: %v", id, err)
		}
		cs := int32(synchro.ComputeRecordChecksum(rowJSON))
		expectedXOR ^= cs

		// Insert bucket edge with the same checksum (simulating WAL consumer
		// that computes from the full row, not a partial map).
		_, _ = db.ExecContext(ctx,
			"INSERT INTO sync_bucket_edges (table_name, record_id, bucket_id, checksum) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
			"orders", id, bucketID, cs)
	}

	// Per-bucket pull to get final page with bucket checksums
	bucketCheckpoints := map[string]int64{bucketID: 0, "global": 0}
	pullReq := synctest.MakePerBucketPullRequest(clientID, bucketCheckpoints, sv, sh)
	pullReq.Limit = synchro.MaxPullLimit
	pullResp, err := engine.Pull(ctx, userID, pullReq)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if pullResp.HasMore {
		t.Fatal("expected final page (has_more = false)")
	}
	if pullResp.BucketChecksums == nil {
		t.Fatal("expected BucketChecksums on final page")
	}

	serverBucketCS, ok := pullResp.BucketChecksums[bucketID]
	if !ok {
		t.Fatalf("BucketChecksums missing bucket %q", bucketID)
	}

	// XOR of per-record checksums from the pull response must match the
	// bucket-level checksum (both computed from the same row_to_json source).
	var pullXOR int32
	for _, record := range pullResp.Changes {
		if record.BucketID == bucketID && record.Checksum != nil {
			pullXOR ^= *record.Checksum
		}
	}

	if pullXOR != expectedXOR {
		t.Fatalf("pull XOR (%d) does not match expected XOR from row_to_json (%d)", pullXOR, expectedXOR)
	}
	if serverBucketCS != expectedXOR {
		t.Fatalf("BucketChecksums[%q] = %d, want %d (XOR of per-record checksums)", bucketID, serverBucketCS, expectedXOR)
	}
}

func TestIntegration_DebugEndpointReturnsClientState(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
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
	clientID := "debug-integ-client"

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "android", "3.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Push a record to have some data
	orderID := "00000000-0000-0000-0000-0000000000d1"
	pushReq := synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(orderID, "orders", "create", map[string]any{
			"ship_address": "Debug Street",
		}),
	)
	if _, err := engine.Push(ctx, userID, pushReq); err != nil {
		t.Fatalf("Push: %v", err)
	}
	// Simulate WAL
	_, _ = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "orders", orderID, 1)

	// Call debug endpoint
	resp, err := engine.ClientDebugInfo(ctx, userID, clientID)
	if err != nil {
		t.Fatalf("ClientDebugInfo: %v", err)
	}

	// Verify client info
	if resp.Client.ClientID != clientID {
		t.Fatalf("client_id = %q, want %q", resp.Client.ClientID, clientID)
	}
	if resp.Client.Platform != "android" {
		t.Fatalf("platform = %q, want 'android'", resp.Client.Platform)
	}
	if !resp.Client.IsActive {
		t.Fatal("client should be active")
	}

	// Verify buckets present
	if len(resp.Buckets) == 0 {
		t.Fatal("expected at least one bucket in debug response")
	}

	// Verify changelog stats
	if resp.ChangelogStats.TotalEntries == 0 {
		t.Fatal("expected non-zero changelog entries")
	}
	if resp.ChangelogStats.MaxSeq == 0 {
		t.Fatal("expected non-zero max seq")
	}

	// Verify server time is recent
	if resp.ServerTime.IsZero() {
		t.Fatal("server_time should not be zero")
	}
}
