package seeddb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	_ "modernc.org/sqlite"
)

// Config holds the configuration for seed database generation.
type Config struct {
	ServerURL  string
	AuthToken  string
	OutputPath string
	WithData   bool // When true, registers a temporary client and fetches snapshot data.
}

type schemaResponse struct {
	SchemaVersion int64         `json:"schema_version"`
	SchemaHash    string        `json:"schema_hash"`
	Tables        []schemaTable `json:"tables"`
}

type schemaTable struct {
	TableName       string         `json:"table_name"`
	PushPolicy      string         `json:"push_policy"`
	UpdatedAtColumn string         `json:"updated_at_column"`
	DeletedAtColumn string         `json:"deleted_at_column"`
	PrimaryKey      []string       `json:"primary_key"`
	Columns         []schemaColumn `json:"columns"`
}

type schemaColumn struct {
	Name             string `json:"name"`
	DBType           string `json:"db_type"`
	LogicalType      string `json:"logical_type"`
	Nullable         bool   `json:"nullable"`
	SQLiteDefaultSQL string `json:"sqlite_default_sql,omitempty"`
	IsPrimaryKey     bool   `json:"is_primary_key"`
}

// Protocol types for register + snapshot.

type registerRequest struct {
	ClientID      string `json:"client_id"`
	Platform      string `json:"platform"`
	AppVersion    string `json:"app_version"`
	SchemaVersion int64  `json:"schema_version"`
	SchemaHash    string `json:"schema_hash"`
}

type registerResponse struct {
	ID            string `json:"id"`
	SchemaVersion int64  `json:"schema_version"`
	SchemaHash    string `json:"schema_hash"`
}

type snapshotRequest struct {
	ClientID      string          `json:"client_id"`
	Cursor        *snapshotCursor `json:"cursor,omitempty"`
	Limit         int             `json:"limit,omitempty"`
	SchemaVersion int64           `json:"schema_version"`
	SchemaHash    string          `json:"schema_hash"`
}

type snapshotCursor struct {
	Checkpoint int64  `json:"checkpoint"`
	TableIndex int    `json:"table_idx"`
	AfterID    string `json:"after_id"`
}

type snapshotResponse struct {
	Records []snapshotRecord `json:"records"`
	Cursor  *snapshotCursor  `json:"cursor,omitempty"`
	HasMore bool             `json:"has_more"`
}

type snapshotRecord struct {
	ID        string          `json:"id"`
	TableName string          `json:"table_name"`
	Data      json.RawMessage `json:"data"`
	UpdatedAt string          `json:"updated_at"`
	DeletedAt *string         `json:"deleted_at,omitempty"`
}

// FetchSchema fetches the schema from a running synchrod server.
func FetchSchema(serverURL, authToken string) (*schemaResponse, error) {
	url := strings.TrimRight(serverURL, "/") + "/sync/schema"

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching schema: server returned %d", resp.StatusCode)
	}

	var schema schemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("decoding schema: %w", err)
	}

	return &schema, nil
}

// Generate creates a seed SQLite database from a synchrod server's schema.
// When cfg.WithData is true, it also registers a temporary client and fetches
// snapshot data to pre-populate tables.
func Generate(cfg Config) error {
	schema, err := FetchSchema(cfg.ServerURL, cfg.AuthToken)
	if err != nil {
		return fmt.Errorf("fetching schema: %w", err)
	}

	db, err := sql.Open("sqlite", cfg.OutputPath)
	if err != nil {
		return fmt.Errorf("opening sqlite database: %w", err)
	}
	defer db.Close()

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := createInfrastructureTables(tx, schema); err != nil {
		return fmt.Errorf("creating infrastructure tables: %w", err)
	}

	// Build column map for data insertion.
	tableColumns := make(map[string][]schemaColumn, len(schema.Tables))
	for i := range schema.Tables {
		table := &schema.Tables[i]
		ddl := GenerateCreateTableSQL(table)
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("creating table %s: %w", table.TableName, err)
		}

		triggers := GenerateCDCTriggers(table)
		for _, trigger := range triggers {
			if _, err := tx.ExecContext(ctx, trigger); err != nil {
				return fmt.Errorf("creating trigger for table %s: %w", table.TableName, err)
			}
		}

		tableColumns[table.TableName] = table.Columns
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	// Fetch and insert snapshot data if requested.
	if cfg.WithData {
		if err := fetchAndInsertData(db, cfg, schema, tableColumns); err != nil {
			return fmt.Errorf("inserting snapshot data: %w", err)
		}
	}

	// Set WAL journal mode outside the transaction.
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("setting WAL journal mode: %w", err)
	}

	return nil
}

// fetchAndInsertData registers a temporary client, pages through the snapshot
// endpoint, and inserts records with sync_lock=1 so CDC triggers don't fire.
func fetchAndInsertData(db *sql.DB, cfg Config, schema *schemaResponse, tableColumns map[string][]schemaColumn) error {
	baseURL := strings.TrimRight(cfg.ServerURL, "/")

	// Register a temporary client.
	regReq := registerRequest{
		ClientID:      "synchroseed-" + fmt.Sprintf("%d", hashString(cfg.OutputPath)),
		Platform:      "seed",
		AppVersion:    "1.0.0",
		SchemaVersion: schema.SchemaVersion,
		SchemaHash:    schema.SchemaHash,
	}
	regResp, err := postJSON[registerResponse](baseURL+"/sync/register", cfg.AuthToken, regReq)
	if err != nil {
		return fmt.Errorf("registering seed client: %w", err)
	}

	// Set sync_lock=1 to prevent CDC triggers from firing during inserts.
	if _, err := db.ExecContext(context.Background(), "UPDATE _synchro_meta SET value = '1' WHERE key = 'sync_lock'"); err != nil {
		return fmt.Errorf("setting sync lock: %w", err)
	}
	defer func() { _, _ = db.ExecContext(context.Background(), "UPDATE _synchro_meta SET value = '0' WHERE key = 'sync_lock'") }()

	// Page through snapshot.
	var cursor *snapshotCursor
	var totalRecords int
	for {
		snapReq := snapshotRequest{
			ClientID:      regReq.ClientID,
			Cursor:        cursor,
			Limit:         500,
			SchemaVersion: regResp.SchemaVersion,
			SchemaHash:    regResp.SchemaHash,
		}
		snapResp, err := postJSON[snapshotResponse](baseURL+"/sync/snapshot", cfg.AuthToken, snapReq)
		if err != nil {
			return fmt.Errorf("fetching snapshot page: %w", err)
		}

		if len(snapResp.Records) > 0 {
			if err := insertRecords(db, snapResp.Records, tableColumns); err != nil {
				return fmt.Errorf("inserting records: %w", err)
			}
			totalRecords += len(snapResp.Records)
		}

		if !snapResp.HasMore {
			break
		}
		cursor = snapResp.Cursor
	}

	fmt.Printf("  inserted %d records from snapshot\n", totalRecords)
	return nil
}

// insertRecords inserts snapshot records into the seed database.
func insertRecords(db *sql.DB, records []snapshotRecord, tableColumns map[string][]schemaColumn) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, rec := range records {
		cols, ok := tableColumns[rec.TableName]
		if !ok {
			continue // table not in schema, skip
		}

		// Decode the JSON data map.
		var data map[string]json.RawMessage
		if err := json.Unmarshal(rec.Data, &data); err != nil {
			return fmt.Errorf("decoding record %s/%s: %w", rec.TableName, rec.ID, err)
		}

		// Build INSERT with all columns present in the data.
		var colNames []string
		var placeholders []string
		var values []any

		for _, col := range cols {
			rawVal, exists := data[col.Name]
			if !exists {
				continue
			}

			colNames = append(colNames, quoteIdentifier(col.Name))
			placeholders = append(placeholders, "?")

			// Decode JSON value to a Go type SQLite can handle.
			var val any
			if err := json.Unmarshal(rawVal, &val); err != nil {
				values = append(values, string(rawVal))
			} else {
				values = append(values, val)
			}
		}

		if len(colNames) == 0 {
			continue
		}

		insertSQL := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
			quoteIdentifier(rec.TableName),
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
		)

		if _, err := tx.ExecContext(ctx, insertSQL, values...); err != nil {
			return fmt.Errorf("inserting record %s/%s: %w", rec.TableName, rec.ID, err)
		}
	}

	return tx.Commit()
}

// postJSON sends a POST request with JSON body and decodes the response.
func postJSON[T any](url, authToken string, body any) (*T, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned %d", resp.StatusCode)
	}

	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &result, nil
}

func hashString(s string) uint32 {
	var h uint32
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}

func createInfrastructureTables(tx *sql.Tx, schema *schemaResponse) error {
	ctx := context.Background()
	infraSQL := `
CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
    record_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    base_updated_at TEXT,
    client_updated_at TEXT NOT NULL,
    PRIMARY KEY (table_name, record_id)
);

CREATE TABLE IF NOT EXISTS _synchro_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS grdb_migrations (
    identifier TEXT NOT NULL PRIMARY KEY
);
`
	if _, err := tx.ExecContext(ctx, infraSQL); err != nil {
		return fmt.Errorf("creating infrastructure tables: %w", err)
	}

	metaValues := []struct{ key, value string }{
		{"sync_lock", "0"},
		{"checkpoint", "0"},
		{"schema_version", fmt.Sprintf("%d", schema.SchemaVersion)},
		{"schema_hash", schema.SchemaHash},
		{"snapshot_complete", "0"},
	}
	for _, m := range metaValues {
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES (?, ?)", m.key, m.value); err != nil {
			return fmt.Errorf("inserting meta key %s: %w", m.key, err)
		}
	}

	if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO grdb_migrations (identifier) VALUES ('synchro_v1')"); err != nil {
		return fmt.Errorf("inserting grdb migration: %w", err)
	}

	return nil
}

// SQLiteType maps a logical type to a SQLite storage type.
func SQLiteType(logicalType string) string {
	switch logicalType {
	case "string":
		return "TEXT"
	case "int", "int64":
		return "INTEGER"
	case "float":
		return "REAL"
	case "boolean":
		return "INTEGER"
	case "datetime", "date", "time":
		return "TEXT"
	case "json":
		return "TEXT"
	case "bytes":
		return "BLOB"
	default:
		return "TEXT"
	}
}

// GenerateCreateTableSQL generates a CREATE TABLE IF NOT EXISTS statement for a schema table.
func GenerateCreateTableSQL(table *schemaTable) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", quoteIdentifier(table.TableName)))

	for i, col := range table.Columns {
		b.WriteString(fmt.Sprintf("    %s %s", quoteIdentifier(col.Name), SQLiteType(col.LogicalType)))

		if col.IsPrimaryKey {
			b.WriteString(" PRIMARY KEY")
		}
		if !col.Nullable && !col.IsPrimaryKey {
			b.WriteString(" NOT NULL")
		}
		if col.SQLiteDefaultSQL != "" {
			b.WriteString(fmt.Sprintf(" DEFAULT %s", col.SQLiteDefaultSQL))
		}

		if i < len(table.Columns)-1 {
			b.WriteString(",")
		}

		b.WriteString("\n")
	}

	b.WriteString(")")

	return b.String()
}

// GenerateCDCTriggers generates the 3 CDC triggers (insert, update, delete) for a schema table.
func GenerateCDCTriggers(table *schemaTable) []string {
	tableName := table.TableName
	escapedName := escapeSQLString(tableName)

	pkCol := "id"
	if len(table.PrimaryKey) > 0 {
		pkCol = table.PrimaryKey[0]
	}

	updatedAtCol := table.UpdatedAtColumn
	deletedAtCol := table.DeletedAtColumn

	quotedTable := quoteIdentifier(tableName)
	quotedPK := quoteIdentifier(pkCol)
	quotedUpdatedAt := quoteIdentifier(updatedAtCol)
	quotedDeletedAt := quoteIdentifier(deletedAtCol)

	lockCheck := "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'"
	tsNow := "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

	insertTrigger := fmt.Sprintf(`CREATE TRIGGER "_synchro_cdc_insert_%s"
AFTER INSERT ON %s
WHEN %s
BEGIN
    INSERT INTO _synchro_pending_changes (record_id, table_name, operation, client_updated_at)
    VALUES (NEW.%s, '%s', 'create', %s)
    ON CONFLICT (table_name, record_id) DO UPDATE SET
        operation = CASE
            WHEN _synchro_pending_changes.operation = 'delete' THEN 'update'
            ELSE _synchro_pending_changes.operation
        END,
        client_updated_at = excluded.client_updated_at;
END`, tableName, quotedTable, lockCheck, quotedPK, escapedName, tsNow)

	updateTrigger := fmt.Sprintf(`CREATE TRIGGER "_synchro_cdc_update_%s"
AFTER UPDATE ON %s
WHEN %s
BEGIN
    INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
    VALUES (
        NEW.%s, '%s',
        CASE WHEN NEW.%s IS NOT NULL AND OLD.%s IS NULL THEN 'delete' ELSE 'update' END,
        OLD.%s,
        %s
    )
    ON CONFLICT (table_name, record_id) DO UPDATE SET
        operation = CASE
            WHEN _synchro_pending_changes.operation = 'create' AND excluded.operation = 'update' THEN 'create'
            WHEN _synchro_pending_changes.operation = 'create' AND excluded.operation = 'delete' THEN 'delete'
            ELSE excluded.operation
        END,
        base_updated_at = CASE
            WHEN _synchro_pending_changes.operation = 'create' AND excluded.operation = 'delete' THEN NULL
            ELSE COALESCE(_synchro_pending_changes.base_updated_at, excluded.base_updated_at)
        END,
        client_updated_at = excluded.client_updated_at;
    DELETE FROM _synchro_pending_changes
    WHERE table_name = '%s' AND record_id = NEW.%s
      AND operation = 'delete'
      AND base_updated_at IS NULL;
END`, tableName, quotedTable, lockCheck,
		quotedPK, escapedName,
		quotedDeletedAt, quotedDeletedAt,
		quotedUpdatedAt,
		tsNow,
		escapedName, quotedPK)

	deleteTrigger := fmt.Sprintf(`CREATE TRIGGER "_synchro_cdc_delete_%s"
BEFORE DELETE ON %s
WHEN %s
BEGIN
    UPDATE %s SET %s = %s WHERE %s = OLD.%s;
    SELECT RAISE(IGNORE);
END`, tableName, quotedTable, lockCheck,
		quotedTable, quotedDeletedAt, tsNow, quotedPK, quotedPK)

	return []string{insertTrigger, updateTrigger, deleteTrigger}
}

func quoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
