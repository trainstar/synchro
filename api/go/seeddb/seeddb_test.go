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
		_, _ = db.ExecContext(ctx, fmt.Sprintf("SELECT synchro_unregister_table('%s')", tableName))
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotePGIdent(tableName)))
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

func quotePGIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
