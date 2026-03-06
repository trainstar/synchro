package synchro_test

import (
	"context"
	"testing"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/synctest"
)

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

	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "test-client-1"

	// Register client
	_, err = engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0"))
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	// Push a create
	itemID := "00000000-0000-0000-0000-000000000010"
	pushReq := &synchro.PushRequest{
		ClientID: clientID,
		Changes: []synchro.PushRecord{
			synctest.MakePushRecord(itemID, "items", "create", map[string]any{
				"name":        "Test Item",
				"description": "A test item",
			}),
		},
	}

	pushResp, err := engine.Push(ctx, userID, pushReq)
	if err != nil {
		t.Fatalf("Push create: %v", err)
	}
	if len(pushResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(pushResp.Accepted), pushResp.Rejected)
	}

	// Simulate WAL: write a changelog entry for the created record
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 1)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull should return the created record
	pullResp, err := engine.Pull(ctx, userID, &synchro.PullRequest{
		ClientID:   clientID,
		Checkpoint: 0,
	})
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
	updateReq := &synchro.PushRequest{
		ClientID: clientID,
		Changes: []synchro.PushRecord{
			synctest.MakePushRecord(itemID, "items", "update", map[string]any{
				"name": "Updated Item",
			}),
		},
	}
	updateResp, err := engine.Push(ctx, userID, updateReq)
	if err != nil {
		t.Fatalf("Push update: %v", err)
	}
	if len(updateResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted update, got %d", len(updateResp.Accepted))
	}

	// Simulate WAL for the update
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 2)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull with previous checkpoint
	pullResp2, err := engine.Pull(ctx, userID, &synchro.PullRequest{
		ClientID:   clientID,
		Checkpoint: pullResp.Checkpoint,
	})
	if err != nil {
		t.Fatalf("Pull after update: %v", err)
	}
	if len(pullResp2.Changes) != 1 {
		t.Fatalf("expected 1 change after update, got %d", len(pullResp2.Changes))
	}

	// Push a delete
	deleteReq := &synchro.PushRequest{
		ClientID: clientID,
		Changes: []synchro.PushRecord{
			synctest.MakePushRecord(itemID, "items", "delete", nil),
		},
	}
	deleteResp, err := engine.Push(ctx, userID, deleteReq)
	if err != nil {
		t.Fatalf("Push delete: %v", err)
	}
	if len(deleteResp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted delete, got %d", len(deleteResp.Accepted))
	}

	// Simulate WAL for the delete
	_, err = db.ExecContext(ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ($1, $2, $3, $4)",
		"user:"+userID, "items", itemID, 3)
	if err != nil {
		t.Fatalf("writing changelog: %v", err)
	}

	// Pull should return the delete
	pullResp3, err := engine.Pull(ctx, userID, &synchro.PullRequest{
		ClientID:   clientID,
		Checkpoint: pullResp2.Checkpoint,
	})
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
			// Policies may already exist from a previous test run; try DROP + CREATE
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
	pushReq := &synchro.PushRequest{
		ClientID: clientA,
		Changes: []synchro.PushRecord{
			synctest.MakePushRecord(itemID, "items", "create", map[string]any{
				"name": "User A Item",
			}),
		},
	}
	resp, err := engine.Push(ctx, userA, pushReq)
	if err != nil {
		t.Fatalf("Push by user A: %v", err)
	}
	if len(resp.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d (rejected: %v)", len(resp.Accepted), resp.Rejected)
	}

	// User B tries to update user A's record — RLS should block it
	updateReq := &synchro.PushRequest{
		ClientID: clientB,
		Changes: []synchro.PushRecord{
			synctest.MakePushRecord(itemID, "items", "update", map[string]any{
				"name": "Hijacked",
			}),
		},
	}
	updateResp, err := engine.Push(ctx, userB, updateReq)
	if err != nil {
		t.Fatalf("Push by user B: %v", err)
	}

	// The update should be rejected (record not found via RLS)
	if len(updateResp.Accepted) != 0 {
		t.Errorf("expected 0 accepted for user B's update, got %d", len(updateResp.Accepted))
	}
	if len(updateResp.Rejected) != 1 {
		t.Fatalf("expected 1 rejected for user B's update, got %d", len(updateResp.Rejected))
	}

	// User A can still update their own record
	updateReqA := &synchro.PushRequest{
		ClientID: clientA,
		Changes: []synchro.PushRecord{
			synctest.MakePushRecord(itemID, "items", "update", map[string]any{
				"name": "Updated by A",
			}),
		},
	}
	updateRespA, err := engine.Push(ctx, userA, updateReqA)
	if err != nil {
		t.Fatalf("Push update by user A: %v", err)
	}
	if len(updateRespA.Accepted) != 1 {
		t.Errorf("expected 1 accepted for user A's update, got %d (rejected: %v)", len(updateRespA.Accepted), updateRespA.Rejected)
	}
}
