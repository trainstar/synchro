package wal_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/migrate"
	"github.com/trainstar/synchro/synctest"
	"github.com/trainstar/synchro/wal"
)

// walTestEnv holds the infrastructure for a WAL integration test.
type walTestEnv struct {
	DB             *sql.DB
	ReplicationURL string
	PubName        string
	SlotName       string
	Consumer       *wal.Consumer
	Registry       *synchro.Registry
	Cancel         context.CancelFunc
	ErrCh          chan error
}

// setupWALTest creates a temp DB, runs migrations, creates app tables,
// sets REPLICA IDENTITY FULL, creates publication + slot, starts consumer.
// tables specifies which tables to include in the publication.
func setupWALTest(t *testing.T, tables []string) (*walTestEnv, context.Context) {
	t.Helper()

	replicationURL := os.Getenv("TEST_REPLICATION_URL")
	if replicationURL == "" {
		t.Skip("TEST_REPLICATION_URL not set, skipping WAL integration test")
	}

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping WAL integration test")
	}

	dbName, db := synctest.CreateTempDB(t, databaseURL)
	t.Cleanup(func() {
		_ = db.Close()
		synctest.DropTempDB(databaseURL, dbName)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(func() { cancel() })

	// Run infrastructure migrations
	for _, stmt := range migrate.Migrations() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("running migration: %v", err)
		}
	}

	// Create custom types
	appTypesDDL := []string{
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status') THEN
				CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');
			END IF;
		END $$`,
	}
	for _, stmt := range appTypesDDL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("creating app type: %v", err)
		}
	}

	// Create all app tables
	appDDL := []string{
		`CREATE TABLE IF NOT EXISTS orders (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id UUID NOT NULL,
			status order_status NOT NULL DEFAULT 'pending',
			order_date TIMESTAMPTZ NOT NULL DEFAULT now(),
			required_date DATE,
			preferred_ship_time TIME WITHOUT TIME ZONE,
			ship_address TEXT NOT NULL DEFAULT '',
			metadata JSONB NOT NULL DEFAULT '{}',
			total_cents BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)`,
		`CREATE TABLE IF NOT EXISTS order_details (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			order_id UUID NOT NULL REFERENCES orders(id),
			product_name TEXT NOT NULL DEFAULT '',
			quantity INTEGER NOT NULL DEFAULT 1,
			unit_price NUMERIC(10,2) NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)`,
		`CREATE TABLE IF NOT EXISTS categories (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id UUID,
			name TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			search_tags TEXT[] NOT NULL DEFAULT '{}',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)`,
	}
	for _, stmt := range appDDL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("creating app table: %v", err)
		}
	}

	// Set REPLICA IDENTITY FULL on specified tables
	for _, tbl := range tables {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl)); err != nil {
			t.Fatalf("setting replica identity on %s: %v", tbl, err)
		}
	}

	// Create publication and slot with unique names
	pubName := fmt.Sprintf("synchro_wal_test_%d", time.Now().UnixNano())
	slotName := fmt.Sprintf("synchro_slot_test_%d", time.Now().UnixNano())

	tableList := ""
	for i, tbl := range tables {
		if i > 0 {
			tableList += ", "
		}
		tableList += tbl
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, tableList))
	if err != nil {
		t.Fatalf("creating publication: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		_, _ = db.ExecContext(context.Background(),
			fmt.Sprintf("SELECT pg_drop_replication_slot('%s') FROM pg_replication_slots WHERE slot_name = '%s'", slotName, slotName))
	})

	tempReplicationURL := synctest.ReplaceDSNDatabase(replicationURL, dbName)

	reg := synctest.NewTestRegistry()

	// Run introspection so the registry knows which tables have deleted_at
	// columns. Without this, soft delete detection in the WAL decoder will
	// not fire because HasDeletedAt() returns false.
	if err := reg.Introspect(ctx, db, "", ""); err != nil {
		t.Fatalf("introspecting registry: %v", err)
	}

	assigner := synchro.NewJoinResolverWithDB(reg, db)

	consumer := wal.NewConsumer(&wal.ConsumerConfig{
		ConnString:      tempReplicationURL,
		SlotName:        slotName,
		PublicationName: pubName,
		Registry:        reg,
		Assigner:        assigner,
		ChangelogDB:     db,
		StandbyTimeout:  2 * time.Second,
	})

	consumerCtx, consumerCancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Give the consumer time to connect and start replication
	time.Sleep(2 * time.Second)

	env := &walTestEnv{
		DB:             db,
		ReplicationURL: tempReplicationURL,
		PubName:        pubName,
		SlotName:       slotName,
		Consumer:       consumer,
		Registry:       reg,
		Cancel:         consumerCancel,
		ErrCh:          errCh,
	}

	t.Cleanup(func() {
		consumerCancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("consumer error on shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("consumer did not shut down within 5s")
		}
	})

	return env, ctx
}

func TestWAL_ConsumerEndToEnd(t *testing.T) {
	env, ctx := setupWALTest(t, []string{"orders"})
	db := env.DB

	// --- INSERT ---
	userID := "00000000-0000-0000-0000-000000000001"
	orderID := "00000000-0000-0000-0000-aa0000000001"
	_, err := db.ExecContext(ctx,
		"INSERT INTO orders (id, user_id, ship_address) VALUES ($1, $2, $3)",
		orderID, userID, "123 WAL St")
	if err != nil {
		t.Fatalf("INSERT order: %v", err)
	}

	insertEntry := pollChangelog(t, ctx, db, "orders", orderID, 10*time.Second)
	if insertEntry.BucketID != "user:"+userID {
		t.Errorf("INSERT bucket_id = %q, want %q", insertEntry.BucketID, "user:"+userID)
	}
	if insertEntry.Operation != int(synchro.OpInsert) {
		t.Errorf("INSERT operation = %d, want %d (OpInsert)", insertEntry.Operation, synchro.OpInsert)
	}

	// --- UPDATE ---
	_, err = db.ExecContext(ctx, "UPDATE orders SET ship_address = '456 WAL Ave' WHERE id = $1", orderID)
	if err != nil {
		t.Fatalf("UPDATE order: %v", err)
	}

	updateEntry := pollChangelogAfter(t, ctx, db, "orders", orderID, insertEntry.Seq, 10*time.Second)
	if updateEntry.BucketID != "user:"+userID {
		t.Errorf("UPDATE bucket_id = %q, want %q", updateEntry.BucketID, "user:"+userID)
	}
	if updateEntry.Operation != int(synchro.OpUpdate) {
		t.Errorf("UPDATE operation = %d, want %d (OpUpdate)", updateEntry.Operation, synchro.OpUpdate)
	}

	// --- SOFT DELETE ---
	_, err = db.ExecContext(ctx, "UPDATE orders SET deleted_at = now() WHERE id = $1", orderID)
	if err != nil {
		t.Fatalf("SOFT DELETE order: %v", err)
	}

	deleteEntry := pollChangelogAfter(t, ctx, db, "orders", orderID, updateEntry.Seq, 10*time.Second)
	if deleteEntry.BucketID != "user:"+userID {
		t.Errorf("SOFT DELETE bucket_id = %q, want %q", deleteEntry.BucketID, "user:"+userID)
	}
	if deleteEntry.Operation != int(synchro.OpDelete) {
		t.Errorf("SOFT DELETE operation = %d, want %d (OpDelete)", deleteEntry.Operation, synchro.OpDelete)
	}
}

func TestWAL_ChildTableBucketAssignment(t *testing.T) {
	env, ctx := setupWALTest(t, []string{"orders", "order_details"})
	db := env.DB

	userID := "00000000-0000-0000-0000-000000000001"
	orderID := "00000000-0000-0000-0000-bb0000000001"

	// Insert parent order with user_id
	_, err := db.ExecContext(ctx,
		"INSERT INTO orders (id, user_id, ship_address) VALUES ($1, $2, $3)",
		orderID, userID, "100 Parent St")
	if err != nil {
		t.Fatalf("INSERT parent order: %v", err)
	}

	// Wait for parent changelog entry
	parentEntry := pollChangelog(t, ctx, db, "orders", orderID, 10*time.Second)
	if parentEntry.BucketID != "user:"+userID {
		t.Errorf("parent bucket_id = %q, want %q", parentEntry.BucketID, "user:"+userID)
	}

	// Insert child detail — order_details has NO user_id column.
	// Bucket must be resolved via parent chain: order_details.order_id -> orders.user_id
	detailID := "00000000-0000-0000-0000-bb0000000002"
	_, err = db.ExecContext(ctx,
		"INSERT INTO order_details (id, order_id, product_name) VALUES ($1, $2, $3)",
		detailID, orderID, "Widget A")
	if err != nil {
		t.Fatalf("INSERT order_details: %v", err)
	}

	childEntry := pollChangelog(t, ctx, db, "order_details", detailID, 10*time.Second)
	if childEntry.BucketID != "user:"+userID {
		t.Errorf("child bucket_id = %q, want %q (resolved via parent chain)", childEntry.BucketID, "user:"+userID)
	}
	if childEntry.Operation != int(synchro.OpInsert) {
		t.Errorf("child operation = %d, want %d (OpInsert)", childEntry.Operation, synchro.OpInsert)
	}
}

func TestWAL_GlobalTableBucketAssignment(t *testing.T) {
	env, ctx := setupWALTest(t, []string{"categories"})
	db := env.DB

	// Insert category with user_id=NULL -> should get bucket "global"
	globalCategoryID := "00000000-0000-0000-0000-cc0000000001"
	_, err := db.ExecContext(ctx,
		"INSERT INTO categories (id, user_id, name) VALUES ($1, NULL, $2)",
		globalCategoryID, "Global Category")
	if err != nil {
		t.Fatalf("INSERT global category: %v", err)
	}

	globalEntry := pollChangelog(t, ctx, db, "categories", globalCategoryID, 10*time.Second)
	if globalEntry.BucketID != "global" {
		t.Errorf("global category bucket_id = %q, want %q", globalEntry.BucketID, "global")
	}
	if globalEntry.Operation != int(synchro.OpInsert) {
		t.Errorf("global category operation = %d, want %d (OpInsert)", globalEntry.Operation, synchro.OpInsert)
	}

	// Insert category with user_id=X -> should get bucket "user:X"
	userID := "00000000-0000-0000-0000-000000000099"
	userCategoryID := "00000000-0000-0000-0000-cc0000000002"
	_, err = db.ExecContext(ctx,
		"INSERT INTO categories (id, user_id, name) VALUES ($1, $2, $3)",
		userCategoryID, userID, "User Category")
	if err != nil {
		t.Fatalf("INSERT user category: %v", err)
	}

	userEntry := pollChangelog(t, ctx, db, "categories", userCategoryID, 10*time.Second)
	if userEntry.BucketID != "user:"+userID {
		t.Errorf("user category bucket_id = %q, want %q", userEntry.BucketID, "user:"+userID)
	}
	if userEntry.Operation != int(synchro.OpInsert) {
		t.Errorf("user category operation = %d, want %d (OpInsert)", userEntry.Operation, synchro.OpInsert)
	}
}

func TestWAL_OwnershipChange(t *testing.T) {
	env, ctx := setupWALTest(t, []string{"orders"})
	db := env.DB

	userA := "00000000-0000-0000-0000-00000000000a"
	userB := "00000000-0000-0000-0000-00000000000b"
	orderID := "00000000-0000-0000-0000-dd0000000001"

	// Insert order owned by user A
	_, err := db.ExecContext(ctx,
		"INSERT INTO orders (id, user_id, ship_address) VALUES ($1, $2, $3)",
		orderID, userA, "100 Ownership St")
	if err != nil {
		t.Fatalf("INSERT order: %v", err)
	}

	insertEntry := pollChangelog(t, ctx, db, "orders", orderID, 10*time.Second)
	if insertEntry.BucketID != "user:"+userA {
		t.Fatalf("initial bucket_id = %q, want %q", insertEntry.BucketID, "user:"+userA)
	}

	// Change ownership from A to B
	_, err = db.ExecContext(ctx,
		"UPDATE orders SET user_id = $1 WHERE id = $2", userB, orderID)
	if err != nil {
		t.Fatalf("UPDATE ownership: %v", err)
	}

	// Poll for changelog entries after the insert.
	// The consumer should produce:
	// - DELETE in old bucket "user:A" (removed from A's view)
	// - INSERT in new bucket "user:B" (added to B's view)
	deadline := time.Now().Add(10 * time.Second)
	var foundDeleteA, foundInsertB bool
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(ctx,
			`SELECT bucket_id, operation FROM sync_changelog
			 WHERE table_name = 'orders' AND record_id = $1 AND seq > $2
			 ORDER BY seq`, orderID, insertEntry.Seq)
		if err != nil {
			t.Fatalf("querying changelog: %v", err)
		}

		for rows.Next() {
			var bucketID string
			var op int
			if err := rows.Scan(&bucketID, &op); err != nil {
				_ = rows.Close()
				t.Fatalf("scanning: %v", err)
			}
			if bucketID == "user:"+userA && op == int(synchro.OpDelete) {
				foundDeleteA = true
			}
			if bucketID == "user:"+userB && op == int(synchro.OpInsert) {
				foundInsertB = true
			}
		}
		_ = rows.Close()

		if foundDeleteA && foundInsertB {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if !foundDeleteA {
		t.Error("expected DELETE entry in old bucket user:A")
	}
	if !foundInsertB {
		t.Error("expected INSERT entry in new bucket user:B")
	}
}

type changelogRow struct {
	Seq       int64
	BucketID  string
	TableName string
	RecordID  string
	Operation int
}

func pollChangelog(t *testing.T, ctx context.Context, db *sql.DB, tableName, recordID string, timeout time.Duration) changelogRow {
	t.Helper()
	return pollChangelogAfter(t, ctx, db, tableName, recordID, 0, timeout)
}

func pollChangelogAfter(t *testing.T, ctx context.Context, db *sql.DB, tableName, recordID string, afterSeq int64, timeout time.Duration) changelogRow {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var row changelogRow
		err := db.QueryRowContext(ctx,
			`SELECT seq, bucket_id, table_name, record_id, operation
			 FROM sync_changelog
			 WHERE table_name = $1 AND record_id = $2 AND seq > $3
			 ORDER BY seq DESC LIMIT 1`,
			tableName, recordID, afterSeq,
		).Scan(&row.Seq, &row.BucketID, &row.TableName, &row.RecordID, &row.Operation)
		if err == nil {
			return row
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for changelog entry: table=%s record=%s afterSeq=%d", tableName, recordID, afterSeq)
	return changelogRow{}
}
