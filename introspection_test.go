package synchro_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/synctest"
)

func TestIntrospect_FullTable(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	cfg := engine.Registry().Get("orders")
	if !cfg.HasUpdatedAt() {
		t.Error("orders: expected HasUpdatedAt=true")
	}
	if !cfg.HasDeletedAt() {
		t.Error("orders: expected HasDeletedAt=true")
	}
	if !cfg.HasCreatedAt() {
		t.Error("orders: expected HasCreatedAt=true")
	}
	if cfg.UpdatedAtCol() != "updated_at" {
		t.Errorf("orders: UpdatedAtCol() = %q, want %q", cfg.UpdatedAtCol(), "updated_at")
	}
	if cfg.DeletedAtCol() != "deleted_at" {
		t.Errorf("orders: DeletedAtCol() = %q, want %q", cfg.DeletedAtCol(), "deleted_at")
	}
}

func TestIntrospect_BareTable(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	cfg := engine.Registry().Get("bare_items")
	if cfg.HasUpdatedAt() {
		t.Error("bare_items: expected HasUpdatedAt=false")
	}
	if cfg.HasDeletedAt() {
		t.Error("bare_items: expected HasDeletedAt=false")
	}
	if cfg.HasCreatedAt() {
		t.Error("bare_items: expected HasCreatedAt=false")
	}
	if cfg.UpdatedAtCol() != "" {
		t.Errorf("bare_items: UpdatedAtCol() = %q, want empty", cfg.UpdatedAtCol())
	}
	if cfg.DeletedAtCol() != "" {
		t.Errorf("bare_items: DeletedAtCol() = %q, want empty", cfg.DeletedAtCol())
	}
}

func TestIntrospect_PartialTable(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	cfg := engine.Registry().Get("partial_items")
	if !cfg.HasUpdatedAt() {
		t.Error("partial_items: expected HasUpdatedAt=true")
	}
	if cfg.HasDeletedAt() {
		t.Error("partial_items: expected HasDeletedAt=false")
	}
	if cfg.HasCreatedAt() {
		t.Error("partial_items: expected HasCreatedAt=false")
	}
}

func TestIntrospect_MixedRegistry(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	// Full table
	orders := engine.Registry().Get("orders")
	if !orders.HasUpdatedAt() || !orders.HasDeletedAt() {
		t.Error("orders should have both timestamp columns")
	}
	// Bare table
	bare := engine.Registry().Get("bare_items")
	if bare.HasUpdatedAt() || bare.HasDeletedAt() {
		t.Error("bare_items should have no timestamp columns")
	}
	// Partial table
	partial := engine.Registry().Get("partial_items")
	if !partial.HasUpdatedAt() || partial.HasDeletedAt() {
		t.Error("partial_items should have only updated_at")
	}
}

func TestIntrospect_CustomColumnName(t *testing.T) {
	db := synctest.TestDB(t)

	// Create a table with custom column name
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS custom_ts (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_id UUID NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		modified_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	if err != nil {
		t.Fatalf("creating custom_ts table: %v", err)
	}

	registry := synchro.NewRegistry()
	registry.Register(&synchro.TableConfig{
		TableName:   "custom_ts",
		PushPolicy:  synchro.PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})

	engine, err := synchro.NewEngine(synchro.Config{
		DB:              db,
		Registry:        registry,
		UpdatedAtColumn: "modified_at",
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	cfg := engine.Registry().Get("custom_ts")
	if !cfg.HasUpdatedAt() {
		t.Error("expected HasUpdatedAt=true with custom column name")
	}
	if cfg.UpdatedAtCol() != "modified_at" {
		t.Errorf("UpdatedAtCol() = %q, want %q", cfg.UpdatedAtCol(), "modified_at")
	}
}

func TestProtectedColumns_BareTable(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	cfg := engine.Registry().Get("bare_items")
	// Timestamp columns should NOT be in protected set for bare table
	if cfg.IsProtected("updated_at") {
		t.Error("bare_items: updated_at should not be protected")
	}
	if cfg.IsProtected("deleted_at") {
		t.Error("bare_items: deleted_at should not be protected")
	}
	if cfg.IsProtected("created_at") {
		t.Error("bare_items: created_at should not be protected")
	}
	// id and user_id should still be protected
	if !cfg.IsProtected("id") {
		t.Error("bare_items: id should be protected")
	}
	if !cfg.IsProtected("user_id") {
		t.Error("bare_items: user_id should be protected")
	}
}

func TestProtectedColumns_FullTable(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	cfg := engine.Registry().Get("orders")
	// Timestamp columns should be in protected set for full table
	if !cfg.IsProtected("updated_at") {
		t.Error("orders: updated_at should be protected")
	}
	if !cfg.IsProtected("deleted_at") {
		t.Error("orders: deleted_at should be protected")
	}
	if !cfg.IsProtected("created_at") {
		t.Error("orders: created_at should be protected")
	}
}

func TestPush_BareTable_Create(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-bare-1"
	sv, sh := schemaFields(t, ctx, engine)

	// Register client
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push create to bare table
	recordID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	resp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "create", map[string]any{
			"name": "bare item 1",
		}),
	))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(resp.Accepted))
	}
	if resp.Accepted[0].ServerUpdatedAt != nil {
		t.Error("ServerUpdatedAt should be nil for bare table")
	}
}

func TestPush_BareTable_Update(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-bare-2"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create a record first
	recordID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "create", map[string]any{
			"name": "original",
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	// Update without conflict resolution (last push wins)
	resp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "update", map[string]any{
			"name": "updated",
		}),
	))
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}

	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(resp.Accepted))
	}
	if resp.Accepted[0].Status != "applied" {
		t.Errorf("status = %q, want applied", resp.Accepted[0].Status)
	}

	// Verify the data was actually updated
	var name string
	err = db.QueryRowContext(ctx, "SELECT name FROM bare_items WHERE id = $1", recordID).Scan(&name)
	if err != nil {
		t.Fatalf("querying updated record: %v", err)
	}
	if name != "updated" {
		t.Errorf("name = %q, want %q", name, "updated")
	}
}

func TestPush_BareTable_HardDelete(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-bare-3"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create a record
	recordID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "create", map[string]any{
			"name": "to delete",
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	// Hard delete
	resp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}

	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(resp.Accepted))
	}
	if resp.Accepted[0].ServerDeletedAt != nil {
		t.Error("ServerDeletedAt should be nil for hard delete")
	}

	// Verify row is actually gone
	var count int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM bare_items WHERE id = $1", recordID).Scan(&count)
	if err != nil {
		t.Fatalf("counting deleted record: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows, got %d (row should be hard deleted)", count)
	}
}

func TestPush_BareTable_HardDelete_Idempotent(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-bare-4"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Create and delete
	recordID := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "create", map[string]any{"name": "temp"}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}

	// Second delete should be idempotent
	resp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push second delete: %v", err)
	}

	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(resp.Accepted))
	}
	if resp.Accepted[0].ReasonCode != "already_deleted" {
		t.Errorf("reason_code = %q, want already_deleted", resp.Accepted[0].ReasonCode)
	}
}

func TestPush_FullTable_SoftDelete_Preserved(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-full-1"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	recordID := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "orders", "create", map[string]any{
			"ship_address": "123 Main St",
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	// Delete should be soft delete for full table
	resp, err := engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "orders", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}

	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(resp.Accepted))
	}
	if resp.Accepted[0].ServerDeletedAt == nil {
		t.Error("ServerDeletedAt should be set for soft delete")
	}

	// Verify row still exists with deleted_at set
	var deletedAt *time.Time
	err = db.QueryRowContext(ctx, "SELECT deleted_at FROM orders WHERE id = $1", recordID).Scan(&deletedAt)
	if err != nil {
		t.Fatalf("querying soft-deleted record: %v", err)
	}
	if deletedAt == nil {
		t.Error("deleted_at should be non-nil for soft delete")
	}
}

func TestPush_FullTable_LWW_Preserved(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-lww-1"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	recordID := "11111111-1111-1111-1111-111111111111"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "orders", "create", map[string]any{
			"ship_address": "123 Main St",
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	// Push update with a very old client timestamp — should lose to server
	oldData, _ := json.Marshal(map[string]any{"ship_address": "456 Other St"})
	pushResp, err := engine.Push(ctx, userID, &synchro.PushRequest{
		ClientID:      clientID,
		SchemaVersion: sv,
		SchemaHash:    sh,
		Changes: []synchro.PushRecord{{
			ID:              recordID,
			TableName:       "orders",
			Operation:       "update",
			Data:            oldData,
			ClientUpdatedAt: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		}},
	})
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}

	if len(pushResp.Rejected) != 1 {
		t.Fatalf("expected 1 rejected (server wins), got accepted=%d rejected=%d",
			len(pushResp.Accepted), len(pushResp.Rejected))
	}
	if pushResp.Rejected[0].ReasonCode != "server_won_conflict" {
		t.Errorf("reason_code = %q, want server_won_conflict", pushResp.Rejected[0].ReasonCode)
	}
}

func TestPull_BareTable_AfterHardDelete(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-pull-hard-1"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Bootstrap client via snapshot so it can pull
	checkpoint := snapshotClientReady(t, ctx, engine, userID, clientID, sv, sh)

	// Create a record
	recordID := "44444444-4444-4444-4444-444444444444"
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "create", map[string]any{
			"name": "will be hard deleted",
		}),
	))
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}

	// Hard delete the record
	_, err = engine.Push(ctx, userID, synctest.MakePushRequest(clientID, sv, sh,
		synctest.MakePushRecord(recordID, "bare_items", "delete", nil),
	))
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}

	// Insert a delete changelog entry for the bare table record to simulate
	// what the WAL consumer would do for a hard DELETE.
	bucketID := "user:" + userID
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		bucketID, "bare_items", recordID, int(synchro.OpDelete))
	if err != nil {
		t.Fatalf("inserting changelog delete entry: %v", err)
	}

	// Pull — should see the delete in Deletes list
	pullResp, err := engine.Pull(ctx, userID, synctest.MakePullRequest(clientID, checkpoint, sv, sh))
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	foundDelete := false
	for _, d := range pullResp.Deletes {
		if d.ID == recordID && d.TableName == "bare_items" {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Errorf("expected bare_items delete entry in Deletes list, got deletes=%v changes=%d",
			pullResp.Deletes, len(pullResp.Changes))
	}
}

func TestSchemaHash_DiffersBetweenFullAndBare(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	// Engine with full-column tables only
	fullRegistry := synctest.NewTestRegistry()
	fullEngine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: fullRegistry,
	})
	if err != nil {
		t.Fatalf("NewEngine (full): %v", err)
	}
	_, fullHash, err := fullEngine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest (full): %v", err)
	}

	// Engine with mixed tables
	mixedRegistry := synctest.NewMixedTestRegistry()
	mixedEngine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: mixedRegistry,
	})
	if err != nil {
		t.Fatalf("NewEngine (mixed): %v", err)
	}
	_, mixedHash, err := mixedEngine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest (mixed): %v", err)
	}

	if fullHash == mixedHash {
		t.Error("schema hash should differ between full-column and mixed registries")
	}
}

func TestSnapshot_BareTable(t *testing.T) {
	db := synctest.TestDB(t)
	registry := synctest.NewMixedTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:       db,
		Registry: registry,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	ctx := context.Background()
	userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	clientID := "client-snap-1"
	sv, sh := schemaFields(t, ctx, engine)

	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Insert records into bare table directly
	_, err = db.ExecContext(ctx, "INSERT INTO bare_items (id, user_id, name) VALUES ($1, $2, $3)",
		"22222222-2222-2222-2222-222222222222", userID, "snap item 1")
	if err != nil {
		t.Fatalf("inserting bare_items: %v", err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO bare_items (id, user_id, name) VALUES ($1, $2, $3)",
		"33333333-3333-3333-3333-333333333333", userID, "snap item 2")
	if err != nil {
		t.Fatalf("inserting bare_items: %v", err)
	}

	// Snapshot — bare table should return all rows (no deleted_at filter)
	resp, err := engine.Snapshot(ctx, userID, &synchro.SnapshotRequest{
		ClientID:      clientID,
		Limit:         100,
		SchemaVersion: sv,
		SchemaHash:    sh,
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Count bare_items records in snapshot
	bareCount := 0
	for _, r := range resp.Records {
		if r.TableName == "bare_items" {
			bareCount++
			if r.UpdatedAt != nil {
				t.Error("bare_items record should have nil UpdatedAt")
			}
		}
	}
	if bareCount < 2 {
		t.Errorf("expected at least 2 bare_items in snapshot, got %d", bareCount)
	}
}
