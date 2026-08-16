package seeddb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	pgxstdlib "github.com/jackc/pgx/v5/stdlib"
)

type manifestMutationConnector struct {
	connector driver.Connector
	mutate    func([]byte) ([]byte, error)
}

func (c *manifestMutationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &manifestMutationConn{Conn: conn, mutate: c.mutate}, nil
}

func (c *manifestMutationConnector) Driver() driver.Driver {
	return c.connector.Driver()
}

type manifestMutationConn struct {
	driver.Conn
	mutate func([]byte) ([]byte, error)
}

func (c *manifestMutationConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err != nil || !strings.Contains(query, "synchro_portable_seed_manifest") {
		return rows, err
	}
	return &manifestMutationRows{Rows: rows, mutate: c.mutate}, nil
}

func (c *manifestMutationConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (c *manifestMutationConn) Ping(ctx context.Context) error {
	pinger, ok := c.Conn.(driver.Pinger)
	if !ok {
		return nil
	}
	return pinger.Ping(ctx)
}

type manifestMutationRows struct {
	driver.Rows
	mutate func([]byte) ([]byte, error)
	done   bool
}

type transactionRecordingConnector struct {
	connector driver.Connector
	events    *[]string
}

func (c *transactionRecordingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &transactionRecordingConn{Conn: conn, events: c.events}, nil
}

func (c *transactionRecordingConnector) Driver() driver.Driver {
	return c.connector.Driver()
}

type transactionRecordingConn struct {
	driver.Conn
	events *[]string
}

func (c *transactionRecordingConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	*c.events = append(*c.events, query)
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (r *manifestMutationRows) Next(values []driver.Value) error {
	if err := r.Rows.Next(values); err != nil || r.done {
		return err
	}
	if len(values) != 1 {
		return errors.New("portable seed manifest query returned an invalid row")
	}
	var raw []byte
	switch value := values[0].(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		return fmt.Errorf("portable seed manifest has unsupported type %T", values[0])
	}
	mutated, err := r.mutate(raw)
	if err != nil {
		return err
	}
	values[0] = mutated
	r.done = true
	return nil
}

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

func manifestMutatingPostgres(
	t *testing.T,
	mutate func([]byte) ([]byte, error),
) *sql.DB {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	config, err := pgx.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("parsing postgres database URL: %v", err)
	}
	db := sql.OpenDB(&manifestMutationConnector{
		connector: pgxstdlib.GetConnector(*config),
		mutate:    mutate,
	})
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("pinging manifest-mutating postgres database: %v", err)
	}
	return db
}

func corruptSeedTokenMAC(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", errors.New("seed token envelope is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || base64.RawURLEncoding.EncodeToString(payload) != parts[1] {
		return "", errors.New("seed token payload encoding is invalid")
	}
	mac, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(mac) != 32 || base64.RawURLEncoding.EncodeToString(mac) != parts[2] {
		return "", errors.New("seed token MAC encoding is invalid")
	}
	mac[0] ^= 1
	parts[2] = base64.RawURLEncoding.EncodeToString(mac)
	return strings.Join(parts, "."), nil
}

func registerSeedTestTable(t *testing.T, db *sql.DB, tableName string) {
	registerSeedTestTableForScope(t, db, tableName, "global")
}

func registerSeedTestTableForScope(t *testing.T, db *sql.DB, tableName, scopeID string) {
	t.Helper()

	ctx := context.Background()
	functionName := tableName + "_membership"
	functionIdentity := "public." + quotePGIdent(functionName)
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

	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT %s::text WHERE p_id IS NOT NULL;
		END;
		REVOKE ALL ON FUNCTION %s(text) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION %s(text) TO synchro_owner, synchro_worker;
		GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
		GRANT SELECT ON TABLE %s TO synchro_owner, synchro_worker;
		ALTER TABLE %s ENABLE ROW LEVEL SECURITY;
		CREATE POLICY synchro_owner_all ON %s
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true)
		;
		CREATE POLICY synchro_worker_select ON %s
			AS PERMISSIVE FOR SELECT TO synchro_worker USING (true)
	`,
		functionIdentity,
		quotePGLiteral(scopeID),
		functionIdentity,
		functionIdentity,
		quotePGIdent(tableName),
		quotePGIdent(tableName),
		quotePGIdent(tableName),
		quotePGIdent(tableName),
	)
	if _, err := db.ExecContext(ctx, functionSQL); err != nil {
		t.Fatalf("creating membership function: %v", err)
	}

	if _, err := db.ExecContext(
		ctx,
		"SELECT synchro.synchro_register_table($1, $2, 'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only')",
		"public."+tableName,
		functionIdentity,
	); err != nil {
		t.Fatalf("registering synced table: %v", err)
	}
	if !waitForSeedTableState(ctx, db, tableName, true) {
		t.Fatalf("registered table %q did not activate", tableName)
	}

	t.Cleanup(func() {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SELECT synchro.synchro_unregister_table('%s')", tableName)); err != nil {
			t.Errorf("unregistering synced table %s: %v", tableName, err)
			return
		}
		if !waitForSeedTableState(ctx, db, tableName, false) {
			t.Errorf("registered table %q did not deactivate", tableName)
			return
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s(text)", functionIdentity)); err != nil {
			t.Errorf("dropping membership function %s: %v", functionName, err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotePGIdent(tableName))); err != nil {
			t.Errorf("dropping test table %s: %v", tableName, err)
		}
	})
}

func waitForSeedTableState(ctx context.Context, db *sql.DB, tableName string, expected bool) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var active bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_registry r
				JOIN synchro.sync_registry_generations g ON g.generation = r.registry_generation
				WHERE g.state = 'active' AND r.table_name = $1
			)
		`, tableName).Scan(&active)
		if err == nil && active == expected {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func waitForPortableEdge(ctx context.Context, db *sql.DB, tableName, recordID string) bool {
	return waitForPortableEdgeInScope(ctx, db, tableName, recordID, "global")
}

func waitForPortableEdgeInScope(ctx context.Context, db *sql.DB, tableName, recordID, scopeID string) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var exists bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_bucket_edges
				WHERE table_name = $1 AND record_id = $2 AND bucket_id = $3
			)
		`, tableName, recordID, scopeID).Scan(&exists)
		if err == nil && exists {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func registerSharedScope(t *testing.T, db *sql.DB, scopeID string, portable bool) {
	t.Helper()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "SELECT synchro.synchro_register_shared_scope($1, $2)", scopeID, portable); err != nil {
		t.Fatalf("registering shared scope %s: %v", scopeID, err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(ctx, "SELECT synchro.synchro_unregister_shared_scope($1)", scopeID); err != nil {
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
		Manifest: schemaManifest{
			SchemaVersion:      1,
			TransitionClass:    "initial",
			CompatibilityFloor: 1,
			Tables: []tableSchema{
				{
					ID:                "table-items",
					RelationID:        "relation-items",
					Name:              "test_items",
					PrimaryKeyFieldID: "field-id",
					Composition:       "single_scope",
					Fields: []fieldSchema{
						{ID: "field-id", Name: "id", Type: "string", Nullable: false},
						{ID: "field-title", Name: "title", Type: "string", Nullable: false},
					},
				},
			},
		},
	}
	schemaHash, err := schemaManifestDigest(env.Manifest)
	if err != nil {
		t.Fatalf("hash schema manifest: %v", err)
	}
	env.SchemaHash = schemaHash
	env.Manifest.SchemaHash = schemaHash

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

func TestTemporaryOutputPreservesDestinationUntilPublication(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "seed.db")
	if err := os.WriteFile(outputPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("write existing output: %v", err)
	}

	temporaryPath, err := prepareTemporaryOutput(outputPath, true)
	if err != nil {
		t.Fatalf("prepare temporary output: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(temporaryPath) })
	if err := os.WriteFile(temporaryPath, []byte("new"), 0o600); err != nil {
		t.Fatalf("write temporary output: %v", err)
	}
	before, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read destination before publication: %v", err)
	}
	if string(before) != "old" {
		t.Fatalf("destination changed before publication: %q", before)
	}

	if err := publishOutput(temporaryPath, outputPath, true); err != nil {
		t.Fatalf("publish output: %v", err)
	}
	after, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read published destination: %v", err)
	}
	if string(after) != "new" {
		t.Fatalf("published destination = %q, want new", after)
	}
}

func TestDestinationSidecarsArePreservedWhenGenerationIsRejected(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "seed.db")
	family := map[string][]byte{
		outputPath:          []byte("old destination"),
		outputPath + "-wal": []byte("old wal"),
		outputPath + "-shm": []byte("old shm"),
	}
	for path, contents := range family {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("write destination family member %s: %v", path, err)
		}
	}

	if _, err := prepareTemporaryOutput(outputPath, true); err == nil {
		t.Fatal("accepted a destination with stale sqlite sidecars")
	}
	for path, want := range family {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read preserved destination family member %s: %v", path, err)
		}
		if string(got) != string(want) {
			t.Fatalf("destination family member %s changed from %q to %q", path, want, got)
		}
	}
}

func TestPublishRechecksDestinationSidecars(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "seed.db")
	temporaryPath := filepath.Join(directory, "temporary.db")
	if err := os.WriteFile(outputPath, []byte("old destination"), 0o600); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	if err := os.WriteFile(outputPath+"-wal", []byte("old wal"), 0o600); err != nil {
		t.Fatalf("write destination wal: %v", err)
	}
	if err := os.WriteFile(temporaryPath, []byte("new destination"), 0o600); err != nil {
		t.Fatalf("write temporary output: %v", err)
	}

	if err := publishOutput(temporaryPath, outputPath, true); err == nil {
		t.Fatal("published over a destination with a stale sqlite sidecar")
	}
	for path, want := range map[string]string{
		outputPath:          "old destination",
		outputPath + "-wal": "old wal",
	} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read preserved destination family member %s: %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("destination family member %s = %q, want %q", path, got, want)
		}
	}
	if got, err := os.ReadFile(temporaryPath); err != nil {
		t.Fatalf("read unpublished temporary output: %v", err)
	} else if string(got) != "new destination" {
		t.Fatalf("temporary output changed to %q", got)
	}
}

func TestVerificationFailureRollsBackExportTransaction(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	config, err := pgx.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("parsing postgres database URL: %v", err)
	}
	events := []string{}
	db := sql.OpenDB(&transactionRecordingConnector{
		connector: pgxstdlib.GetConnector(*config),
		events:    &events,
	})
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	pgTx, err := beginPGReadTransaction(ctx, db)
	if err != nil {
		t.Fatalf("begin export transaction: %v", err)
	}
	badArtifact := filepath.Join(t.TempDir(), "invalid.db")
	if err := os.WriteFile(badArtifact, []byte("not sqlite"), 0o600); err != nil {
		t.Fatalf("write invalid sqlite artifact: %v", err)
	}
	if err := verifySQLiteOutput(ctx, badArtifact, manifestEnvelope{}, nil, portableSeedManifest{}, seedSnapshotComplete); err == nil {
		t.Fatal("accepted an invalid finalized sqlite artifact")
	}
	pgTx.Close(ctx)

	if len(events) != 2 || events[0] != "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE" || events[1] != "ROLLBACK" {
		t.Fatalf("export transaction events = %v, want BEGIN followed by ROLLBACK", events)
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
	if !waitForPortableEdge(ctx, db, tableName, recordID) {
		t.Fatal("portable row did not become WAL-materialized")
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

	var scopeCursor sql.NullString
	var scopeChecksum string
	var localChecksum string
	if err := sqliteDB.QueryRow(
		"SELECT cursor, checksum, local_checksum FROM _synchro_scopes WHERE scope_id = 'global'",
	).Scan(&scopeCursor, &scopeChecksum, &localChecksum); err != nil {
		t.Fatalf("reading portable scope state: %v", err)
	}
	if scopeCursor.Valid {
		t.Fatal("portable receipt must not be installed as a runtime cursor")
	}
	var parsedScopeChecksum checksumObject
	if err := json.Unmarshal([]byte(scopeChecksum), &parsedScopeChecksum); err != nil {
		t.Fatalf("decoding portable scope checksum: %v", err)
	}
	if err := parsedScopeChecksum.validate(); err != nil {
		t.Fatalf("validating portable scope checksum: %v", err)
	}
	if localChecksum != scopeChecksum {
		t.Fatal("local scope checksum does not match the verified authoritative checksum")
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
	var scopeRowChecksum string
	if err := sqliteDB.QueryRow(
		"SELECT count(*), COALESCE(MAX(checksum), '') FROM _synchro_scope_rows WHERE scope_id = 'global' AND table_name = ? AND record_id = ?",
		tableName,
		recordID,
	).Scan(&scopeRowCount, &scopeRowChecksum); err != nil {
		t.Fatalf("reading portable scope row: %v", err)
	}
	if scopeRowCount != 1 {
		t.Fatalf("expected one portable scope row, got %d", scopeRowCount)
	}
	if len(scopeRowChecksum) != 64 {
		t.Fatalf("portable row digest length = %d, want 64", len(scopeRowChecksum))
	}

	var receipt string
	if err := sqliteDB.QueryRow(
		"SELECT receipt FROM _synchro_seed_receipts WHERE scope_id = 'global'",
	).Scan(&receipt); err != nil {
		t.Fatalf("reading portable scope receipt: %v", err)
	}
	if receipt == "" {
		t.Fatal("portable scope receipt is empty")
	}

	var serverVersion string
	var rowChecksum string
	if err := sqliteDB.QueryRow(
		"SELECT server_version, row_checksum FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
		tableName,
		recordID,
	).Scan(&serverVersion, &rowChecksum); err != nil {
		t.Fatalf("reading portable row version: %v", err)
	}
	if serverVersion == "" || rowChecksum == "" {
		t.Fatal("portable row version metadata is incomplete")
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

func TestSQLiteSnapshotCompletionVerifiesStagedAndFinalArtifacts(t *testing.T) {
	db := testPostgres(t)
	tableName := "test_seed_snapshot_completion"
	registerSeedTestTable(t, db, tableName)

	directory := t.TempDir()
	finalPath := filepath.Join(directory, "final.db")
	if err := Generate(context.Background(), db, GenerateOptions{OutputPath: finalPath}); err != nil {
		t.Fatalf("generate final seed: %v", err)
	}
	env, tables, portable := reconstructSeedVerificationInputs(t, db, finalPath)

	stagedPath := filepath.Join(directory, "staged.db")
	copySeedArtifact(t, finalPath, stagedPath)
	mutateSeedSQLite(t, stagedPath, "UPDATE _synchro_meta SET value = ? WHERE key = 'snapshot_complete'", seedSnapshotIncomplete)

	if err := verifySQLiteOutput(context.Background(), stagedPath, env, tables, portable, seedSnapshotIncomplete); err != nil {
		t.Fatalf("verify staged seed: %v", err)
	}
	if err := verifySQLiteOutput(context.Background(), stagedPath, env, tables, portable, seedSnapshotComplete); err == nil {
		t.Fatal("staged seed passed final verification")
	}
	if err := finalizeSQLiteOutput(context.Background(), stagedPath); err != nil {
		t.Fatalf("finalize staged seed: %v", err)
	}
	if err := verifySQLiteOutput(context.Background(), stagedPath, env, tables, portable, seedSnapshotComplete); err != nil {
		t.Fatalf("verify finalized seed: %v", err)
	}
	if err := verifySQLiteOutput(context.Background(), stagedPath, env, tables, portable, seedSnapshotIncomplete); err == nil {
		t.Fatal("finalized seed passed staged verification")
	}
}

func TestGenerateRejectsMACOnlyPortableSeedTokenCorruption(t *testing.T) {
	db := testPostgres(t)
	tableName := "test_seed_mac_verification"
	scopeID := "seed-mac-verification"
	registerSeedTestTableForScope(t, db, tableName, scopeID)
	registerSharedScope(t, db, scopeID, true)

	tests := []struct {
		name  string
		token func(*portableSeedScope) *string
	}{
		{
			name: "page token",
			token: func(scope *portableSeedScope) *string {
				return &scope.PageToken
			},
		},
		{
			name: "continuation receipt",
			token: func(scope *portableSeedScope) *string {
				return &scope.Continuation
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corruptingDB := manifestMutatingPostgres(t, func(raw []byte) ([]byte, error) {
				var manifest portableSeedManifest
				if err := decodeJSON(raw, &manifest); err != nil {
					return nil, err
				}
				if len(manifest.PortableScopes) != 1 {
					return nil, fmt.Errorf("portable seed scope count = %d, want 1", len(manifest.PortableScopes))
				}
				token := test.token(&manifest.PortableScopes[0])
				original := *token
				corrupted, err := corruptSeedTokenMAC(original)
				if err != nil {
					return nil, err
				}
				*token = corrupted
				originalParts := strings.Split(original, ".")
				corruptedParts := strings.Split(corrupted, ".")
				if len(originalParts) != 3 || len(corruptedParts) != 3 ||
					originalParts[0] != corruptedParts[0] ||
					originalParts[1] != corruptedParts[1] ||
					originalParts[2] == corruptedParts[2] {
					return nil, errors.New("seed token corruption changed data outside the MAC")
				}
				return json.Marshal(manifest)
			})

			directory := t.TempDir()
			outputPath := filepath.Join(directory, "seed.db")
			originalDestination := []byte("existing destination")
			if err := os.WriteFile(outputPath, originalDestination, 0o600); err != nil {
				t.Fatalf("writing existing destination: %v", err)
			}

			err := Generate(context.Background(), corruptingDB, GenerateOptions{
				OutputPath: outputPath,
				Overwrite:  true,
			})
			if err == nil {
				t.Fatal("generated a seed from a token with a corrupt MAC")
			}
			actualDestination, readErr := os.ReadFile(outputPath)
			if readErr != nil {
				t.Fatalf("reading preserved destination: %v", readErr)
			}
			if string(actualDestination) != string(originalDestination) {
				t.Fatal("token verification failure changed the existing destination")
			}
			for _, suffix := range []string{"-wal", "-shm"} {
				if _, statErr := os.Stat(outputPath + suffix); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("token verification failure retained sidecar %s", suffix)
				}
			}
			entries, readDirErr := os.ReadDir(directory)
			if readDirErr != nil {
				t.Fatalf("reading output directory: %v", readDirErr)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".synchro-seed-") {
					t.Fatalf("token verification failure retained temporary output %s", entry.Name())
				}
			}
		})
	}
}

func TestPublishVerifiedSQLiteOutputRejectsArtifactCorruption(t *testing.T) {
	db := testPostgres(t)
	tableName := "test_seed_verified_publication"
	scopeID := "seed-verification"
	registerSeedTestTableForScope(t, db, tableName, scopeID)
	registerSharedScope(t, db, scopeID, true)

	ctx := context.Background()
	recordID := "00000000-0000-0000-0000-000000000100"
	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf("INSERT INTO %s (id, user_id, title, updated_at, deleted_at) VALUES ($1, $2, $3, now(), NULL)", quotePGIdent(tableName)),
		recordID,
		"user-1",
		"verification row",
	); err != nil {
		t.Fatalf("inserting verification row: %v", err)
	}
	if !waitForPortableEdgeInScope(ctx, db, tableName, recordID, scopeID) {
		t.Fatal("verification row did not become WAL-materialized")
	}

	directory := t.TempDir()
	originalPath := filepath.Join(directory, "verified.db")
	if err := Generate(ctx, db, GenerateOptions{OutputPath: originalPath}); err != nil {
		t.Fatalf("generate verification seed: %v", err)
	}
	env, tables, portable := reconstructSeedVerificationInputs(t, db, originalPath)

	zeroChecksum := checksumObject{
		Algorithm: "sha256",
		Version:   1,
		Encoding:  "hex",
		Digest:    strings.Repeat("0", 64),
	}
	zeroChecksumJSON, err := json.Marshal(zeroChecksum)
	if err != nil {
		t.Fatalf("encode corrupt checksum: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(t *testing.T, path string)
	}{
		{
			name: "metadata",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "UPDATE _synchro_meta SET value = 'corrupt' WHERE key = 'schema_hash'")
			},
		},
		{
			name: "trigger",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "DROP TRIGGER "+quoteIdentifier("_synchro_cdc_insert_"+tableName))
			},
		},
		{
			name: "schema",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "ALTER TABLE "+quoteIdentifier(tableName)+" ADD COLUMN corruption TEXT")
			},
		},
		{
			name: "row digest",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "UPDATE _synchro_row_versions SET row_checksum = ?", string(zeroChecksumJSON))
			},
		},
		{
			name: "scope receipt",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "UPDATE _synchro_seed_receipts SET receipt = receipt || 'corrupt'")
			},
		},
		{
			name: "scope digest",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "UPDATE _synchro_scopes SET checksum = ?, local_checksum = ?", string(zeroChecksumJSON), string(zeroChecksumJSON))
			},
		},
		{
			name: "scope row generation",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, "UPDATE _synchro_scope_rows SET generation = 1")
			},
		},
		{
			name: "queue",
			mutate: func(t *testing.T, path string) {
				mutateSeedSQLite(t, path, `INSERT INTO _synchro_pending_changes
					(record_id, table_name, operation, client_updated_at)
					VALUES ('corrupt', 'corrupt', 'create', '2026-01-01T00:00:00.000000Z')`)
			},
		},
		{
			name: "sidecar",
			mutate: func(t *testing.T, path string) {
				if err := os.WriteFile(path+"-wal", []byte("corrupt"), 0o600); err != nil {
					t.Fatalf("write corrupt sidecar: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			temporaryPath := filepath.Join(directory, strings.ReplaceAll(test.name, " ", "-")+".db")
			copySeedArtifact(t, originalPath, temporaryPath)
			test.mutate(t, temporaryPath)

			destinationPath := filepath.Join(directory, strings.ReplaceAll(test.name, " ", "-")+"-published.db")
			if err := publishVerifiedSQLiteOutput(ctx, temporaryPath, destinationPath, false, env, tables, portable); err == nil {
				t.Fatal("published a corrupt seed artifact")
			}
			if _, err := os.Stat(destinationPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corrupt artifact changed destination: %v", err)
			}
		})
	}
}

func reconstructSeedVerificationInputs(t *testing.T, pg *sql.DB, path string) (manifestEnvelope, []localSchemaTable, portableSeedManifest) {
	t.Helper()
	ctx := context.Background()
	env, err := loadManifest(ctx, pg)
	if err != nil {
		t.Fatalf("load schema manifest: %v", err)
	}
	if err := env.validate(); err != nil {
		t.Fatalf("validate schema manifest: %v", err)
	}
	tables, err := env.localTables()
	if err != nil {
		t.Fatalf("build local schema tables: %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatalf("open generated seed: %v", err)
	}
	defer db.Close()
	metadata := make(map[string]string)
	metadataRows, err := db.QueryContext(ctx, "SELECT key, value FROM _synchro_meta")
	if err != nil {
		t.Fatalf("read seed metadata: %v", err)
	}
	for metadataRows.Next() {
		var key, value string
		if err := metadataRows.Scan(&key, &value); err != nil {
			_ = metadataRows.Close()
			t.Fatalf("scan seed metadata: %v", err)
		}
		metadata[key] = value
	}
	if err := metadataRows.Err(); err != nil {
		_ = metadataRows.Close()
		t.Fatalf("iterate seed metadata: %v", err)
	}
	if err := metadataRows.Close(); err != nil {
		t.Fatalf("close seed metadata: %v", err)
	}
	pageLimit, err := strconv.ParseInt(metadata["seed_page_limit"], 10, 64)
	if err != nil {
		t.Fatalf("parse seed page limit: %v", err)
	}
	var boundary seedSnapshotBoundary
	if err := decodeJSON([]byte(metadata["seed_snapshot_boundary"]), &boundary); err != nil {
		t.Fatalf("decode seed boundary: %v", err)
	}
	portable := portableSeedManifest{
		ExportID:           metadata["seed_export_id"],
		ExportManifestHash: metadata["seed_export_manifest_hash"],
		SchemaVersion:      env.SchemaVersion,
		SchemaHash:         env.SchemaHash,
		StreamGeneration:   metadata["seed_stream_generation"],
		SnapshotBoundary:   boundary,
		PageLimit:          pageLimit,
	}
	receipts, err := db.QueryContext(ctx, `SELECT scope_id, receipt, cardinality, checksum
		FROM _synchro_seed_receipts ORDER BY scope_id`)
	if err != nil {
		t.Fatalf("read seed receipts: %v", err)
	}
	for receipts.Next() {
		var scopeID, receipt, rawChecksum string
		var cardinality int64
		if err := receipts.Scan(&scopeID, &receipt, &cardinality, &rawChecksum); err != nil {
			_ = receipts.Close()
			t.Fatalf("scan seed receipt: %v", err)
		}
		var checksum checksumObject
		if err := decodeJSON([]byte(rawChecksum), &checksum); err != nil {
			_ = receipts.Close()
			t.Fatalf("decode seed receipt checksum: %v", err)
		}
		portable.PortableScopes = append(portable.PortableScopes, portableSeedScope{
			ID:           scopeID,
			Cardinality:  cardinality,
			Checksum:     checksum,
			Continuation: receipt,
		})
	}
	if err := receipts.Err(); err != nil {
		_ = receipts.Close()
		t.Fatalf("iterate seed receipts: %v", err)
	}
	if err := receipts.Close(); err != nil {
		t.Fatalf("close seed receipts: %v", err)
	}
	return env, tables, portable
}

func copySeedArtifact(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read seed artifact: %v", err)
	}
	if err := os.WriteFile(destination, contents, 0o600); err != nil {
		t.Fatalf("copy seed artifact: %v", err)
	}
}

func mutateSeedSQLite(t *testing.T, path, statement string, arguments ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed for corruption: %v", err)
	}
	if _, err := db.Exec(statement, arguments...); err != nil {
		_ = db.Close()
		t.Fatalf("corrupt seed artifact: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close corrupt seed artifact: %v", err)
	}
	if err := removeSQLiteSidecars(path); err != nil {
		t.Fatalf("remove corruption sidecars: %v", err)
	}
}

func quotePGIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quotePGLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
