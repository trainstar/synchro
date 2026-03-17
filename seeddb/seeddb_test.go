package seeddb

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

var testSchema = schemaResponse{
	SchemaVersion: 1,
	SchemaHash:    "test-hash-123",
	Tables: []schemaTable{
		{
			TableName:       "users",
			PushPolicy:      "owner_only",
			UpdatedAtColumn: "updated_at",
			DeletedAtColumn: "deleted_at",
			PrimaryKey:      []string{"id"},
			Columns: []schemaColumn{
				{Name: "id", DBType: "uuid", LogicalType: "string", Nullable: false, IsPrimaryKey: true},
				{Name: "name", DBType: "text", LogicalType: "string", Nullable: false},
				{Name: "email", DBType: "text", LogicalType: "string", Nullable: true},
				{Name: "age", DBType: "integer", LogicalType: "int", Nullable: true},
				{Name: "score", DBType: "double precision", LogicalType: "float", Nullable: true},
				{Name: "active", DBType: "boolean", LogicalType: "boolean", Nullable: false, SQLiteDefaultSQL: "1"},
				{Name: "metadata", DBType: "jsonb", LogicalType: "json", Nullable: true},
				{Name: "avatar", DBType: "bytea", LogicalType: "bytes", Nullable: true},
				{Name: "updated_at", DBType: "timestamp with time zone", LogicalType: "datetime", Nullable: false},
				{Name: "deleted_at", DBType: "timestamp with time zone", LogicalType: "datetime", Nullable: true},
			},
		},
		{
			TableName:       "tasks",
			PushPolicy:      "owner_only",
			UpdatedAtColumn: "updated_at",
			DeletedAtColumn: "deleted_at",
			PrimaryKey:      []string{"id"},
			Columns: []schemaColumn{
				{Name: "id", DBType: "uuid", LogicalType: "string", Nullable: false, IsPrimaryKey: true},
				{Name: "title", DBType: "text", LogicalType: "string", Nullable: false},
				{Name: "user_id", DBType: "uuid", LogicalType: "string", Nullable: false},
				{Name: "due_date", DBType: "date", LogicalType: "date", Nullable: true},
				{Name: "updated_at", DBType: "timestamp with time zone", LogicalType: "datetime", Nullable: false},
				{Name: "deleted_at", DBType: "timestamp with time zone", LogicalType: "datetime", Nullable: true},
			},
		},
	},
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync/schema" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(testSchema)
	}))
}

func generateTestDB(t *testing.T) (string, *sql.DB) {
	t.Helper()

	srv := newTestServer(t)
	t.Cleanup(srv.Close)

	dbPath := filepath.Join(t.TempDir(), "test-seed.db")

	cfg := Config{
		ServerURL:  srv.URL,
		AuthToken:  "test-token",
		OutputPath: dbPath,
	}

	if err := Generate(cfg); err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("opening generated database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return dbPath, db
}

func TestGenerateHasInfrastructureTables(t *testing.T) {
	_, db := generateTestDB(t)

	// Verify _synchro_pending_changes table exists.
	var pcCount int
	err := db.QueryRowContext(context.Background(),"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_synchro_pending_changes'").Scan(&pcCount)
	if err != nil {
		t.Fatalf("querying sqlite_master: %v", err)
	}
	if pcCount != 1 {
		t.Fatalf("expected _synchro_pending_changes table to exist, got count=%d", pcCount)
	}

	// Verify _synchro_meta table exists with correct values.
	expectedMeta := map[string]string{
		"sync_lock":         "0",
		"checkpoint":        "0",
		"schema_version":    "1",
		"schema_hash":       "test-hash-123",
		"snapshot_complete":  "0",
	}

	rows, err := db.QueryContext(context.Background(),"SELECT key, value FROM _synchro_meta ORDER BY key")
	if err != nil {
		t.Fatalf("querying _synchro_meta: %v", err)
	}
	defer func() { _ = rows.Close() }()

	gotMeta := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatalf("scanning meta row: %v", err)
		}
		gotMeta[key] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating meta rows: %v", err)
	}

	for key, want := range expectedMeta {
		got, ok := gotMeta[key]
		if !ok {
			t.Errorf("missing meta key %q", key)
			continue
		}
		if got != want {
			t.Errorf("meta key %q: got %q, want %q", key, got, want)
		}
	}

	// Verify grdb_migrations table has synchro_v1.
	var migrationID string
	err = db.QueryRowContext(context.Background(),"SELECT identifier FROM grdb_migrations WHERE identifier = 'synchro_v1'").Scan(&migrationID)
	if err != nil {
		t.Fatalf("querying grdb_migrations: %v", err)
	}
	if migrationID != "synchro_v1" {
		t.Errorf("expected migration 'synchro_v1', got %q", migrationID)
	}
}

func TestGenerateHasCorrectSyncedTables(t *testing.T) {
	_, db := generateTestDB(t)

	// Verify both tables exist.
	for _, tableName := range []string{"users", "tasks"} {
		var count int
		err := db.QueryRowContext(context.Background(),"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(&count)
		if err != nil {
			t.Fatalf("querying for table %s: %v", tableName, err)
		}
		if count != 1 {
			t.Fatalf("expected table %s to exist", tableName)
		}
	}

	// Verify users table columns via PRAGMA.
	rows, err := db.QueryContext(context.Background(),"PRAGMA table_info(users)")
	if err != nil {
		t.Fatalf("querying pragma for users: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type colInfo struct {
		name    string
		colType string
		notNull bool
		pk      bool
	}

	expectedCols := []colInfo{
		{name: "id", colType: "TEXT", notNull: false, pk: true},
		{name: "name", colType: "TEXT", notNull: true, pk: false},
		{name: "email", colType: "TEXT", notNull: false, pk: false},
		{name: "age", colType: "INTEGER", notNull: false, pk: false},
		{name: "score", colType: "REAL", notNull: false, pk: false},
		{name: "active", colType: "INTEGER", notNull: true, pk: false},
		{name: "metadata", colType: "TEXT", notNull: false, pk: false},
		{name: "avatar", colType: "BLOB", notNull: false, pk: false},
		{name: "updated_at", colType: "TEXT", notNull: true, pk: false},
		{name: "deleted_at", colType: "TEXT", notNull: false, pk: false},
	}

	var gotCols []colInfo
	for rows.Next() {
		var cid int
		var name, typeName string
		var notNull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("scanning column info: %v", err)
		}
		gotCols = append(gotCols, colInfo{
			name:    name,
			colType: typeName,
			notNull: notNull == 1,
			pk:      pk == 1,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating column rows: %v", err)
	}

	if len(gotCols) != len(expectedCols) {
		t.Fatalf("users table: expected %d columns, got %d", len(expectedCols), len(gotCols))
	}

	for i, want := range expectedCols {
		got := gotCols[i]
		if got.name != want.name {
			t.Errorf("column %d: expected name %q, got %q", i, want.name, got.name)
		}
		if got.colType != want.colType {
			t.Errorf("column %q: expected type %q, got %q", want.name, want.colType, got.colType)
		}
		if got.notNull != want.notNull {
			t.Errorf("column %q: expected notNull=%v, got %v", want.name, want.notNull, got.notNull)
		}
		if got.pk != want.pk {
			t.Errorf("column %q: expected pk=%v, got %v", want.name, want.pk, got.pk)
		}
	}
}

func TestGenerateHasCorrectCDCTriggers(t *testing.T) {
	_, db := generateTestDB(t)

	// Each table should have 3 triggers: insert, update, delete.
	for _, tableName := range []string{"users", "tasks"} {
		expectedTriggers := []string{
			"_synchro_cdc_insert_" + tableName,
			"_synchro_cdc_update_" + tableName,
			"_synchro_cdc_delete_" + tableName,
		}

		for _, triggerName := range expectedTriggers {
			var count int
			err := db.QueryRowContext(context.Background(),
				"SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?",
				triggerName,
			).Scan(&count)
			if err != nil {
				t.Fatalf("querying for trigger %s: %v", triggerName, err)
			}
			if count != 1 {
				t.Errorf("expected trigger %s to exist for table %s", triggerName, tableName)
			}
		}
	}

	// Verify total trigger count is 6 (3 per table * 2 tables).
	var totalTriggers int
	err := db.QueryRowContext(context.Background(),"SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'").Scan(&totalTriggers)
	if err != nil {
		t.Fatalf("counting triggers: %v", err)
	}
	if totalTriggers != 6 {
		t.Errorf("expected 6 total triggers, got %d", totalTriggers)
	}
}

func TestGenerateCanInsertAndQuery(t *testing.T) {
	_, db := generateTestDB(t)

	// Disable sync_lock so triggers fire (it's already '0' by default).
	// Insert a user row.
	_, err := db.ExecContext(context.Background(),`
		INSERT INTO "users" (id, name, email, age, score, active, updated_at)
		VALUES ('user-1', 'Alice', 'alice@example.com', 30, 95.5, 1, '2024-01-01T00:00:00.000Z')
	`)
	if err != nil {
		t.Fatalf("inserting user: %v", err)
	}

	// Query it back.
	var id, name string
	var age int
	var score float64
	err = db.QueryRowContext(context.Background(),"SELECT id, name, age, score FROM users WHERE id = 'user-1'").Scan(&id, &name, &age, &score)
	if err != nil {
		t.Fatalf("querying user: %v", err)
	}
	if id != "user-1" || name != "Alice" || age != 30 || score != 95.5 {
		t.Errorf("unexpected user data: id=%q name=%q age=%d score=%f", id, name, age, score)
	}

	// Verify the CDC trigger fired and created a pending change.
	var recordID, operation string
	err = db.QueryRowContext(context.Background(),"SELECT record_id, operation FROM _synchro_pending_changes WHERE table_name = 'users'").Scan(&recordID, &operation)
	if err != nil {
		t.Fatalf("querying pending changes: %v", err)
	}
	if recordID != "user-1" {
		t.Errorf("expected pending change record_id='user-1', got %q", recordID)
	}
	if operation != "create" {
		t.Errorf("expected pending change operation='create', got %q", operation)
	}

	// Insert a task and verify cross-table isolation.
	_, err = db.ExecContext(context.Background(),`
		INSERT INTO "tasks" (id, title, user_id, updated_at)
		VALUES ('task-1', 'Do stuff', 'user-1', '2024-01-01T00:00:00.000Z')
	`)
	if err != nil {
		t.Fatalf("inserting task: %v", err)
	}

	var taskCount int
	err = db.QueryRowContext(context.Background(),"SELECT COUNT(*) FROM _synchro_pending_changes").Scan(&taskCount)
	if err != nil {
		t.Fatalf("counting pending changes: %v", err)
	}
	if taskCount != 2 {
		t.Errorf("expected 2 pending changes, got %d", taskCount)
	}
}

func TestSQLiteTypeMapping(t *testing.T) {
	tests := []struct {
		logicalType string
		want        string
	}{
		{"string", "TEXT"},
		{"int", "INTEGER"},
		{"int64", "INTEGER"},
		{"float", "REAL"},
		{"boolean", "INTEGER"},
		{"datetime", "TEXT"},
		{"date", "TEXT"},
		{"time", "TEXT"},
		{"json", "TEXT"},
		{"bytes", "BLOB"},
		{"unknown_type", "TEXT"},
		{"", "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.logicalType, func(t *testing.T) {
			got := SQLiteType(tt.logicalType)
			if got != tt.want {
				t.Errorf("SQLiteType(%q) = %q, want %q", tt.logicalType, got, tt.want)
			}
		})
	}
}

func TestGenerateCreateTableSQL(t *testing.T) {
	table := schemaTable{
		TableName:       "items",
		UpdatedAtColumn: "updated_at",
		DeletedAtColumn: "deleted_at",
		PrimaryKey:      []string{"id"},
		Columns: []schemaColumn{
			{Name: "id", LogicalType: "string", Nullable: false, IsPrimaryKey: true},
			{Name: "name", LogicalType: "string", Nullable: false},
			{Name: "count", LogicalType: "int", Nullable: true},
			{Name: "weight", LogicalType: "float", Nullable: true},
			{Name: "enabled", LogicalType: "boolean", Nullable: false, SQLiteDefaultSQL: "1"},
			{Name: "data", LogicalType: "json", Nullable: true},
			{Name: "blob_data", LogicalType: "bytes", Nullable: true},
			{Name: "updated_at", LogicalType: "datetime", Nullable: false},
			{Name: "deleted_at", LogicalType: "datetime", Nullable: true},
		},
	}

	sql := GenerateCreateTableSQL(&table)

	// Verify table name is quoted.
	if !strings.Contains(sql, `"items"`) {
		t.Error("expected quoted table name")
	}

	// Verify column definitions.
	expectedFragments := []string{
		`"id" TEXT PRIMARY KEY`,
		`"name" TEXT NOT NULL`,
		`"count" INTEGER`,
		`"weight" REAL`,
		`"enabled" INTEGER NOT NULL DEFAULT 1`,
		`"data" TEXT`,
		`"blob_data" BLOB`,
		`"updated_at" TEXT NOT NULL`,
		`"deleted_at" TEXT`,
	}

	for _, frag := range expectedFragments {
		if !strings.Contains(sql, frag) {
			t.Errorf("expected SQL to contain %q, got:\n%s", frag, sql)
		}
	}

	// Verify it starts with CREATE TABLE IF NOT EXISTS.
	if !strings.HasPrefix(sql, "CREATE TABLE IF NOT EXISTS") {
		t.Errorf("expected SQL to start with CREATE TABLE IF NOT EXISTS, got:\n%s", sql)
	}
}

func TestGenerateCDCTriggers(t *testing.T) {
	table := schemaTable{
		TableName:       "items",
		UpdatedAtColumn: "updated_at",
		DeletedAtColumn: "deleted_at",
		PrimaryKey:      []string{"id"},
		Columns: []schemaColumn{
			{Name: "id", LogicalType: "string", Nullable: false, IsPrimaryKey: true},
			{Name: "name", LogicalType: "string", Nullable: false},
			{Name: "updated_at", LogicalType: "datetime", Nullable: false},
			{Name: "deleted_at", LogicalType: "datetime", Nullable: true},
		},
	}

	triggers := GenerateCDCTriggers(&table)

	if len(triggers) != 3 {
		t.Fatalf("expected 3 triggers, got %d", len(triggers))
	}

	// Insert trigger.
	insertTrigger := triggers[0]
	if !strings.Contains(insertTrigger, `"_synchro_cdc_insert_items"`) {
		t.Error("insert trigger: missing correct name")
	}
	if !strings.Contains(insertTrigger, `AFTER INSERT ON "items"`) {
		t.Error("insert trigger: missing AFTER INSERT clause")
	}
	if !strings.Contains(insertTrigger, "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'") {
		t.Error("insert trigger: missing WHEN clause")
	}
	if !strings.Contains(insertTrigger, `NEW."id"`) {
		t.Error("insert trigger: missing NEW.id reference")
	}
	if !strings.Contains(insertTrigger, "'create'") {
		t.Error("insert trigger: missing 'create' operation")
	}

	// Update trigger.
	updateTrigger := triggers[1]
	if !strings.Contains(updateTrigger, `"_synchro_cdc_update_items"`) {
		t.Error("update trigger: missing correct name")
	}
	if !strings.Contains(updateTrigger, `AFTER UPDATE ON "items"`) {
		t.Error("update trigger: missing AFTER UPDATE clause")
	}
	if !strings.Contains(updateTrigger, `OLD."updated_at"`) {
		t.Error("update trigger: missing OLD.updated_at reference")
	}
	if !strings.Contains(updateTrigger, `NEW."deleted_at" IS NOT NULL AND OLD."deleted_at" IS NULL`) {
		t.Error("update trigger: missing soft-delete detection logic")
	}
	if !strings.Contains(updateTrigger, "DELETE FROM _synchro_pending_changes") {
		t.Error("update trigger: missing orphan cleanup")
	}

	// Delete trigger.
	deleteTrigger := triggers[2]
	if !strings.Contains(deleteTrigger, `"_synchro_cdc_delete_items"`) {
		t.Error("delete trigger: missing correct name")
	}
	if !strings.Contains(deleteTrigger, `BEFORE DELETE ON "items"`) {
		t.Error("delete trigger: missing BEFORE DELETE clause")
	}
	if !strings.Contains(deleteTrigger, `UPDATE "items" SET "deleted_at"`) {
		t.Error("delete trigger: missing soft-delete UPDATE")
	}
	if !strings.Contains(deleteTrigger, "RAISE(IGNORE)") {
		t.Error("delete trigger: missing RAISE(IGNORE)")
	}

	// Verify the file output was cleaned up (for TestGenerate tests).
	_ = os.Remove(filepath.Join(os.TempDir(), "trigger-test.db"))
}
