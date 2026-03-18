package synchro_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/synctest"
)

func resetSchemaManifest(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), "TRUNCATE sync_schema_manifest"); err != nil {
		t.Fatalf("truncate sync_schema_manifest: %v", err)
	}
}

func TestIntegration_PullPushRejectBootstrapSchema(t *testing.T) {
	db := synctest.TestDB(t)
	resetSchemaManifest(t, db)
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

	userID := "00000000-0000-0000-0000-000000000101"
	clientID := "strict-schema-client"

	if _, err := engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "ios", "1.0.0")); err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}

	_, err = engine.Pull(ctx, userID, &synchro.PullRequest{
		ClientID:      clientID,
		Checkpoint:    0,
		SchemaVersion: 0,
		SchemaHash:    "",
	})
	if !errors.Is(err, synchro.ErrSchemaMismatch) {
		t.Fatalf("Pull error = %v, want %v", err, synchro.ErrSchemaMismatch)
	}

	_, err = engine.Push(ctx, userID, &synchro.PushRequest{
		ClientID:      clientID,
		Changes:       nil,
		SchemaVersion: 0,
		SchemaHash:    "",
	})
	if !errors.Is(err, synchro.ErrSchemaMismatch) {
		t.Fatalf("Push error = %v, want %v", err, synchro.ErrSchemaMismatch)
	}
}

func TestIntegration_SchemaManifest_RaceSafe(t *testing.T) {
	db := synctest.TestDB(t)
	resetSchemaManifest(t, db)
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

	const workers = 24

	type result struct {
		version int64
		hash    string
		err     error
	}

	results := make(chan result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, h, err := engine.CurrentSchemaManifest(ctx)
			results <- result{version: v, hash: h, err: err}
		}()
	}
	wg.Wait()
	close(results)

	var expectedVersion int64
	var expectedHash string
	first := true
	for r := range results {
		if r.err != nil {
			t.Fatalf("CurrentSchemaManifest: %v", r.err)
		}
		if first {
			first = false
			expectedVersion = r.version
			expectedHash = r.hash
			continue
		}
		if r.version != expectedVersion || r.hash != expectedHash {
			t.Fatalf("non-deterministic manifest under race: got (%d,%s), want (%d,%s)", r.version, r.hash, expectedVersion, expectedHash)
		}
	}

	var total int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sync_schema_manifest WHERE schema_hash = $1", expectedHash,
	).Scan(&total); err != nil {
		t.Fatalf("count manifest rows: %v", err)
	}
	if total != 1 {
		t.Fatalf("manifest rows for hash = %d, want 1", total)
	}

	history, err := engine.SchemaManifestHistory(ctx, 10)
	if err != nil {
		t.Fatalf("SchemaManifestHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("history length = %d, want 1", len(history))
	}
	if history[0].SchemaVersion != expectedVersion || history[0].SchemaHash != expectedHash {
		t.Fatalf("history[0] = (%d,%s), want (%d,%s)", history[0].SchemaVersion, history[0].SchemaHash, expectedVersion, expectedHash)
	}
}

func TestIntegration_SchemaManifest_ChangesOnDrift(t *testing.T) {
	db := synctest.TestDB(t)
	resetSchemaManifest(t, db)
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

	v1, h1, err := engine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest before drift: %v", err)
	}

	col := fmt.Sprintf("schema_drift_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE orders ADD COLUMN %s TEXT NOT NULL DEFAULT ''", col),
	); err != nil {
		t.Fatalf("alter table add column: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE orders DROP COLUMN IF EXISTS %s", col))
	})

	v2, h2, err := engine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest after drift: %v", err)
	}
	if h1 == h2 {
		t.Fatalf("schema hash did not change after drift: %s", h1)
	}
	if v2 <= v1 {
		t.Fatalf("schema version did not advance: before=%d after=%d", v1, v2)
	}
}

func TestIntegration_EnumTypeResolution(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	// The test schema already creates the order_status enum and the orders
	// table with a status column of type order_status. Verify that schema
	// discovery resolves the enum to "string".
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

	schema, err := engine.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}

	var found bool
	for _, table := range schema.Tables {
		if table.TableName != "orders" {
			continue
		}
		for _, col := range table.Columns {
			if col.Name == "status" {
				found = true
				if col.LogicalType != "string" {
					t.Errorf("enum column 'status' logical type = %q, want %q", col.LogicalType, "string")
				}
				if col.DBType != "order_status" {
					t.Errorf("enum column 'status' db_type = %q, want %q", col.DBType, "order_status")
				}
			}
		}
	}
	if !found {
		t.Fatal("column 'status' not found in orders schema")
	}
}

func TestIntegration_DomainTypeResolution(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	// Create a domain type and a table using it.
	_, err := db.ExecContext(ctx, `
		DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'positive_int') THEN
				CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
			END IF;
		END $$
	`)
	if err != nil {
		t.Fatalf("creating domain type: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS domain_test (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			quantity positive_int NOT NULL DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)
	`)
	if err != nil {
		t.Fatalf("creating domain_test table: %v", err)
	}

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:     db,
		Tables: []synchro.Table{{Name: "domain_test"}},
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

	schema, err := engine.Schema(ctx)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}

	var found bool
	for _, table := range schema.Tables {
		if table.TableName != "domain_test" {
			continue
		}
		for _, col := range table.Columns {
			if col.Name == "quantity" {
				found = true
				if col.LogicalType != "int" {
					t.Errorf("domain column 'quantity' logical type = %q, want %q", col.LogicalType, "int")
				}
			}
		}
	}
	if !found {
		t.Fatal("column 'quantity' not found in domain_test schema")
	}
}

func TestIntegration_SchemaManifest_DeterministicAcrossOrderVariations(t *testing.T) {
	db := synctest.TestDB(t)
	resetSchemaManifest(t, db)
	ctx := context.Background()

	suffix := time.Now().UnixNano()
	t1 := fmt.Sprintf("ord_a_%d", suffix)
	t2 := fmt.Sprintf("ord_b_%d", suffix)
	t3 := fmt.Sprintf("ord_c_%d", suffix)

	create := func(name string) {
		stmt := fmt.Sprintf(`CREATE TABLE %s (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)`, name)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	create(t1)
	create(t2)
	create(t3)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t3))
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t2))
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", t1))
	})

	tablesA := []synchro.Table{{Name: t1}, {Name: t2}, {Name: t3}}
	tablesB := []synchro.Table{{Name: t2}, {Name: t3}, {Name: t1}}

	engineA, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:         db,
		Tables:     tablesA,
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if err != nil {
		t.Fatalf("NewEngine A: %v", err)
	}
	engineB, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:         db,
		Tables:     tablesB,
		BucketFunc: synchro.UserBucket("user_id"),
	})
	if err != nil {
		t.Fatalf("NewEngine B: %v", err)
	}

	vA, hA, err := engineA.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest A: %v", err)
	}
	vB, hB, err := engineB.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest B: %v", err)
	}

	if hA != hB || vA != vB {
		t.Fatalf("manifest mismatch across equivalent registries: A=(%d,%s) B=(%d,%s)", vA, hA, vB, hB)
	}
}
