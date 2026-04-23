package seeddb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func testPostgres(t *testing.T) *sql.DB {
	t.Helper()

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening postgres database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("pinging postgres database: %v", err)
	}

	return db
}

func registerSeedTestTable(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	ctx := context.Background()
	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			title TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)
	`, quotePGIdent(tableName))
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("creating test table: %v", err)
	}

	registerSQL := fmt.Sprintf(
		"SELECT synchro_register_table('%s', $$SELECT ARRAY['global'] FROM %s WHERE id = $1::text$$, 'id', 'updated_at', 'deleted_at', 'read_only')",
		tableName,
		tableName,
	)
	if _, err := db.ExecContext(ctx, registerSQL); err != nil {
		t.Fatalf("registering synced table: %v", err)
	}

	t.Cleanup(func() {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SELECT synchro_unregister_table('%s')", tableName)); err != nil {
			t.Errorf("unregistering synced table %s: %v", tableName, err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotePGIdent(tableName))); err != nil {
			t.Errorf("dropping test table %s: %v", tableName, err)
		}
	})
}

func registerSharedScope(t *testing.T, db *sql.DB, scopeID string, portable bool) {
	t.Helper()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "SELECT synchro_register_shared_scope($1, $2)", scopeID, portable); err != nil {
		t.Fatalf("registering shared scope %s: %v", scopeID, err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(ctx, "SELECT synchro_unregister_shared_scope($1)", scopeID); err != nil {
			t.Errorf("unregistering shared scope %s: %v", scopeID, err)
		}
	})
}

func TestGenerateCreatesClientCompatibleSeedDatabase(t *testing.T) {
	db := testPostgres(t)
	tableName := "test_seed_orders"
	registerSeedTestTable(t, db, tableName)

	outputPath := filepath.Join(t.TempDir(), "seed.db")
	if err := Generate(context.Background(), db, GenerateOptions{
		OutputPath: outputPath,
		Overwrite:  false,
	}); err != nil {
		t.Fatalf("generate seed database: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(outputPath + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected generated seed to avoid runtime sidecar %s", suffix)
		}
	}

	sqliteDB, err := sql.Open("sqlite", outputPath)
	if err != nil {
		t.Fatalf("opening generated seed database: %v", err)
	}
	defer sqliteDB.Close()

	var schemaVersion string
	if err := sqliteDB.QueryRow("SELECT value FROM _synchro_meta WHERE key = 'schema_version'").Scan(&schemaVersion); err != nil {
		t.Fatalf("reading schema_version: %v", err)
	}
	if schemaVersion == "" || schemaVersion == "0" {
		t.Fatalf("expected non-zero schema_version, got %q", schemaVersion)
	}

	var scopeSetVersion string
	if err := sqliteDB.QueryRow("SELECT value FROM _synchro_meta WHERE key = 'scope_set_version'").Scan(&scopeSetVersion); err != nil {
		t.Fatalf("reading scope_set_version: %v", err)
	}
	if scopeSetVersion != "0" {
		t.Fatalf("expected scope_set_version=0, got %q", scopeSetVersion)
	}

	var localSchemaRaw string
	if err := sqliteDB.QueryRow("SELECT value FROM _synchro_meta WHERE key = 'local_schema'").Scan(&localSchemaRaw); err != nil {
		t.Fatalf("reading local_schema: %v", err)
	}
	var localTables []localSchemaTable
	if err := json.Unmarshal([]byte(localSchemaRaw), &localTables); err != nil {
		t.Fatalf("decoding local_schema: %v", err)
	}
	if len(localTables) == 0 {
		t.Fatal("expected at least one local schema table")
	}
	foundTable := false
	for _, table := range localTables {
		if table.TableName == tableName {
			foundTable = true
			break
		}
	}
	if !foundTable {
		t.Fatalf("local schema missing %s", tableName)
	}

	triggerNames := map[string]bool{}
	rows, err := sqliteDB.Query("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", tableName)
	if err != nil {
		t.Fatalf("querying triggers: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scanning trigger name: %v", err)
		}
		triggerNames[name] = true
	}
	for _, name := range []string{
		"_synchro_cdc_insert_" + tableName,
		"_synchro_cdc_update_" + tableName,
		"_synchro_cdc_delete_" + tableName,
	} {
		if !triggerNames[name] {
			t.Fatalf("missing trigger %s", name)
		}
	}

	var pendingCount int
	if err := sqliteDB.QueryRow("SELECT COUNT(*) FROM _synchro_pending_changes").Scan(&pendingCount); err != nil {
		t.Fatalf("reading pending queue size: %v", err)
	}
	if pendingCount != 0 {
		t.Fatalf("expected generated seed to start with an empty pending queue, got %d rows", pendingCount)
	}

	_, err = sqliteDB.Exec(
		fmt.Sprintf("INSERT INTO %s (id, user_id, title, updated_at, deleted_at) VALUES (?, ?, ?, ?, NULL)", quoteIdentifier(tableName)),
		"00000000-0000-0000-0000-000000000001",
		"user-1",
		"seed row",
		"2026-03-23T00:00:00.000Z",
	)
	if err != nil {
		t.Fatalf("inserting into generated table: %v", err)
	}

	var operation string
	if err := sqliteDB.QueryRow(
		"SELECT operation FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
		tableName,
		"00000000-0000-0000-0000-000000000001",
	).Scan(&operation); err != nil {
		t.Fatalf("reading pending change: %v", err)
	}
	if operation != "create" {
		t.Fatalf("expected pending operation=create, got %q", operation)
	}
}

func TestManifestValidateAllowsTablesWithoutSyncTimestamps(t *testing.T) {
	env := manifestEnvelope{
		SchemaVersion: 1,
		SchemaHash:    "hash-1",
		Manifest: schemaManifest{
			Tables: []tableSchema{
				{
					Name:       "test_items",
					PrimaryKey: []string{"id"},
					Columns: []columnSchema{
						{Name: "id", Type: "text", Nullable: false},
						{Name: "title", Type: "text", Nullable: false},
					},
				},
			},
		},
	}

	if err := env.validate(); err != nil {
		t.Fatalf("manifest with optional timestamps should validate: %v", err)
	}
}

func TestCDCTriggerSQLSupportsTablesWithoutDeletedAt(t *testing.T) {
	table := localSchemaTable{
		TableName:       "test_items",
		UpdatedAtColumn: "updated_at",
		DeletedAtColumn: "",
		PrimaryKey:      []string{"id"},
		Columns: []localSchemaColumn{
			{Name: "id", LogicalType: "string", IsPrimaryKey: true},
			{Name: "title", LogicalType: "string"},
			{Name: "updated_at", LogicalType: "datetime"},
		},
	}

	statements := cdcTriggerSQL(table)
	if len(statements) != 6 {
		t.Fatalf("expected 6 trigger statements, got %d", len(statements))
	}
	if !strings.Contains(statements[5], "AFTER DELETE ON") {
		t.Fatalf("expected hard delete trigger for tables without deleted_at, got %q", statements[5])
	}
	if strings.Contains(statements[5], `SET "" =`) {
		t.Fatalf("delete trigger should not reference an empty deleted_at column: %q", statements[5])
	}
}

func TestGenerateRejectsExistingOutputWithoutOverwrite(t *testing.T) {
	db := testPostgres(t)
	tableName := "test_seed_overwrite"
	registerSeedTestTable(t, db, tableName)

	outputPath := filepath.Join(t.TempDir(), "seed.db")
	if err := Generate(context.Background(), db, GenerateOptions{
		OutputPath: outputPath,
		Overwrite:  false,
	}); err != nil {
		t.Fatalf("initial generate seed database: %v", err)
	}

	err := Generate(context.Background(), db, GenerateOptions{
		OutputPath: outputPath,
		Overwrite:  false,
	})
	if !errors.Is(err, ErrOutputExists) {
		t.Fatalf("expected ErrOutputExists, got %v", err)
	}
}

func TestGenerateHydratesPortableRowsAndScopeState(t *testing.T) {
	db := testPostgres(t)
	tableName := "test_seed_portable"
	registerSeedTestTable(t, db, tableName)
	registerSharedScope(t, db, "global", true)

	ctx := context.Background()
	recordID := "00000000-0000-0000-0000-000000000099"
	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (id, user_id, title, updated_at, deleted_at) VALUES ($1, $2, $3, '2026-03-23T00:00:00Z'::timestamptz, NULL)",
			quotePGIdent(tableName),
		),
		recordID,
		"user-1",
		"portable row",
	); err != nil {
		t.Fatalf("inserting portable row: %v", err)
	}
	if _, err := db.ExecContext(
		ctx,
		"INSERT INTO sync_bucket_edges (table_name, record_id, bucket_id, checksum) VALUES ($1, $2, 'global', 12345) ON CONFLICT (table_name, record_id, bucket_id) DO UPDATE SET checksum = excluded.checksum",
		tableName,
		recordID,
	); err != nil {
		t.Fatalf("inserting portable edge: %v", err)
	}
	if _, err := db.ExecContext(
		ctx,
		"INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) VALUES ('global', $1, $2, 1)",
		tableName,
		recordID,
	); err != nil {
		t.Fatalf("inserting portable changelog: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "portable-seed.db")
	if err := Generate(context.Background(), db, GenerateOptions{
		OutputPath: outputPath,
		Overwrite:  false,
	}); err != nil {
		t.Fatalf("generate portable seed database: %v", err)
	}

	sqliteDB, err := sql.Open("sqlite", outputPath)
	if err != nil {
		t.Fatalf("opening generated portable seed database: %v", err)
	}
	defer sqliteDB.Close()

	var title string
	if err := sqliteDB.QueryRow(
		fmt.Sprintf("SELECT title FROM %s WHERE id = ?", quoteIdentifier(tableName)),
		recordID,
	).Scan(&title); err != nil {
		t.Fatalf("reading portable row: %v", err)
	}
	if title != "portable row" {
		t.Fatalf("expected portable row title, got %q", title)
	}

	var scopeCursor string
	var scopeChecksum string
	var localChecksum int64
	if err := sqliteDB.QueryRow(
		"SELECT cursor, checksum, local_checksum FROM _synchro_scopes WHERE scope_id = 'global'",
	).Scan(&scopeCursor, &scopeChecksum, &localChecksum); err != nil {
		t.Fatalf("reading portable scope state: %v", err)
	}
	if scopeCursor == "" {
		t.Fatal("expected non-empty portable scope cursor")
	}
	if scopeChecksum == "" {
		t.Fatal("expected non-empty portable scope checksum")
	}
	if localChecksum != 12345 {
		t.Fatalf("expected local scope checksum 12345, got %d", localChecksum)
	}

	var checkpointRows int64
	if err := sqliteDB.QueryRow(
		"SELECT COUNT(*) FROM _synchro_bucket_checkpoints WHERE bucket_id = 'global'",
	).Scan(&checkpointRows); err != nil {
		t.Fatalf("reading portable bucket checkpoint rows: %v", err)
	}
	if checkpointRows != 0 {
		t.Fatalf("expected no portable bucket checkpoint rows, got %d", checkpointRows)
	}

	var memberRows int64
	if err := sqliteDB.QueryRow(
		"SELECT COUNT(*) FROM _synchro_bucket_members WHERE bucket_id = 'global' AND table_name = ? AND record_id = ?",
		tableName,
		recordID,
	).Scan(&memberRows); err != nil {
		t.Fatalf("reading portable bucket member rows: %v", err)
	}
	if memberRows != 0 {
		t.Fatalf("expected no portable bucket member rows, got %d", memberRows)
	}

	var scopeRowCount int64
	var scopeRowChecksum int64
	if err := sqliteDB.QueryRow(
		"SELECT count(*), COALESCE(MAX(checksum), 0) FROM _synchro_scope_rows WHERE scope_id = 'global' AND table_name = ? AND record_id = ?",
		tableName,
		recordID,
	).Scan(&scopeRowCount, &scopeRowChecksum); err != nil {
		t.Fatalf("reading portable scope row: %v", err)
	}
	if scopeRowCount != 1 {
		t.Fatalf("expected one portable scope row, got %d", scopeRowCount)
	}
	if scopeRowChecksum != 12345 {
		t.Fatalf("expected portable scope row checksum 12345, got %d", scopeRowChecksum)
	}

	var snapshotComplete string
	if err := sqliteDB.QueryRow("SELECT value FROM _synchro_meta WHERE key = 'snapshot_complete'").Scan(&snapshotComplete); err != nil {
		t.Fatalf("reading snapshot_complete: %v", err)
	}
	if snapshotComplete != "1" {
		t.Fatalf("expected snapshot_complete=1, got %q", snapshotComplete)
	}

	var knownBucketsRaw string
	if err := sqliteDB.QueryRow("SELECT value FROM _synchro_meta WHERE key = 'known_buckets'").Scan(&knownBucketsRaw); err != nil {
		t.Fatalf("reading known_buckets: %v", err)
	}
	var knownBuckets []string
	if err := json.Unmarshal([]byte(knownBucketsRaw), &knownBuckets); err != nil {
		t.Fatalf("decoding known_buckets: %v", err)
	}
	foundGlobal := false
	for _, bucketID := range knownBuckets {
		if bucketID == "global" {
			foundGlobal = true
			break
		}
	}
	if !foundGlobal {
		t.Fatalf("expected known_buckets to contain global, got %v", knownBuckets)
	}

	var pendingCount int
	if err := sqliteDB.QueryRow("SELECT COUNT(*) FROM _synchro_pending_changes").Scan(&pendingCount); err != nil {
		t.Fatalf("reading portable seed pending queue size: %v", err)
	}
	if pendingCount != 0 {
		t.Fatalf("expected portable seed to start with an empty pending queue, got %d rows", pendingCount)
	}

	rows, err := sqliteDB.Query("SELECT identifier FROM grdb_migrations ORDER BY identifier")
	if err != nil {
		t.Fatalf("reading grdb migrations: %v", err)
	}
	defer rows.Close()

	var identifiers []string
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			t.Fatalf("scanning grdb migration identifier: %v", err)
		}
		identifiers = append(identifiers, identifier)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating grdb migration identifiers: %v", err)
	}
	if diff := cmp.Diff(clientCompatibleMigrationIdentifiers, identifiers); diff != "" {
		t.Fatalf("unexpected grdb migration identifiers (-want +got):\n%s", diff)
	}
}

func quotePGIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
