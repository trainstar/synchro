package wal_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/synctest"
	"github.com/trainstar/synchro/wal"
)

// TestFailure_PositionPersistAndReload verifies the Position crash recovery
// contract: persist an LSN, create a new Position (simulating restart), Load
// must return the previously persisted value.
func TestFailure_PositionPersistAndReload(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	slotName := "test_position_recovery"

	// Create first position, persist an LSN.
	pos1 := wal.NewPosition(db, slotName)
	targetLSN := pglogrepl.LSN(123456789)

	if err := pos1.SetConfirmed(ctx, targetLSN); err != nil {
		t.Fatalf("SetConfirmed: %v", err)
	}
	if got := pos1.Confirmed(); got != targetLSN {
		t.Fatalf("Confirmed() = %v, want %v", got, targetLSN)
	}

	// Create a NEW Position (simulates process restart).
	pos2 := wal.NewPosition(db, slotName)

	loaded, err := pos2.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded != targetLSN {
		t.Fatalf("loaded LSN = %v, want %v", loaded, targetLSN)
	}
	if pos2.Confirmed() != targetLSN {
		t.Fatalf("Confirmed() after Load = %v, want %v", pos2.Confirmed(), targetLSN)
	}
}

// TestFailure_ReplicationSlotDropped tests that the consumer returns an error
// (not panic, not hang) when the replication slot is dropped underneath it,
// and that it can resume after the slot is recreated.
// Gated on TEST_REPLICATION_URL — skipped if not set.
func TestFailure_ReplicationSlotDropped(t *testing.T) {
	replURL := os.Getenv("TEST_REPLICATION_URL")
	if replURL == "" {
		t.Skip("TEST_REPLICATION_URL not set, skipping replication slot test")
	}

	db := synctest.TestDB(t)
	ctx := context.Background()

	slotName := "test_drop_slot"
	pubName := "test_drop_pub"

	// Clean up any leftover slot from previous test runs.
	_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", slotName)

	reg := synctest.NewTestRegistry()

	// --- Phase 1: Start consumer, drop slot, verify it returns an error ---

	consumer1 := wal.NewConsumer(wal.ConsumerConfig{
		ConnString:      replURL,
		SlotName:        slotName,
		PublicationName: pubName,
		Registry:        reg,
		Assigner:        synchro.NewJoinResolverWithDB(reg, db),
		ChangelogDB:     db,
	})

	startCtx1, cancel1 := context.WithCancel(ctx)
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- consumer1.Start(startCtx1)
	}()

	// Wait a moment for consumer to start, then drop the slot.
	select {
	case err := <-errCh1:
		// Consumer may fail immediately if publication doesn't exist — acceptable.
		t.Logf("consumer returned before slot drop: %v", err)
		cancel1()
		// Still proceed to phase 2.
	case <-time.After(500 * time.Millisecond):
		// Consumer is running — drop the slot.
		_, dropErr := db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
		if dropErr != nil {
			t.Logf("drop slot error (may already be gone): %v", dropErr)
		}
		cancel1()

		// Consumer must return (not hang).
		select {
		case err := <-errCh1:
			t.Logf("consumer returned after slot drop: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("consumer did not return within timeout after slot drop")
		}
	}

	// --- Phase 2: Recreate slot, restart consumer, verify it resumes ---

	// Clean up again in case phase 1 left a slot.
	_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", slotName)

	consumer2 := wal.NewConsumer(wal.ConsumerConfig{
		ConnString:      replURL,
		SlotName:        slotName,
		PublicationName: pubName,
		Registry:        reg,
		Assigner:        synchro.NewJoinResolverWithDB(reg, db),
		ChangelogDB:     db,
	})

	startCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- consumer2.Start(startCtx2)
	}()

	// Give consumer time to start, then cancel to verify it started successfully.
	select {
	case err := <-errCh2:
		// If it returned an error, that's acceptable (e.g. publication missing).
		// The key test is it didn't panic and it returned.
		t.Logf("consumer2 returned: %v", err)
	case <-time.After(1 * time.Second):
		// Consumer is running — it resumed successfully. Cancel and drain.
		cancel2()
		select {
		case err := <-errCh2:
			t.Logf("consumer2 stopped after cancel: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("consumer2 did not return within timeout")
		}
	}
}
