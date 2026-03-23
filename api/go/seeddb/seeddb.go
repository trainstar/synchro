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

	_ "modernc.org/sqlite"
)

var ErrOutputExists = errors.New("seed output already exists")

type GenerateOptions struct {
	OutputPath string
	Overwrite  bool
}

type manifestEnvelope struct {
	SchemaVersion int64          `json:"schema_version"`
	SchemaHash    string         `json:"schema_hash"`
	Manifest      schemaManifest `json:"manifest"`
}

type schemaManifest struct {
	Tables []tableSchema `json:"tables"`
}

type tableSchema struct {
	Name            string         `json:"name"`
	PrimaryKey      []string       `json:"primary_key"`
	UpdatedAtColumn string         `json:"updated_at_column"`
	DeletedAtColumn string         `json:"deleted_at_column"`
	Composition     string         `json:"composition,omitempty"`
	Columns         []columnSchema `json:"columns"`
}

type columnSchema struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type localSchemaTable struct {
	TableName       string              `json:"tableName"`
	UpdatedAtColumn string              `json:"updatedAtColumn"`
	DeletedAtColumn string              `json:"deletedAtColumn"`
	Composition     string              `json:"composition,omitempty"`
	PrimaryKey      []string            `json:"primaryKey"`
	Columns         []localSchemaColumn `json:"columns"`
}

type localSchemaColumn struct {
	Name             string  `json:"name"`
	LogicalType      string  `json:"logicalType"`
	Nullable         bool    `json:"nullable"`
	SQLiteDefaultSQL *string `json:"sqliteDefaultSQL"`
	IsPrimaryKey     bool    `json:"isPrimaryKey"`
}

func Generate(ctx context.Context, pg *sql.DB, opts GenerateOptions) error {
	if pg == nil {
		return errors.New("postgres database is required")
	}
	if strings.TrimSpace(opts.OutputPath) == "" {
		return errors.New("output path is required")
	}

	env, err := loadManifest(ctx, pg)
	if err != nil {
		return err
	}
	if err := env.validate(); err != nil {
		return err
	}

	localTables, err := env.localTables()
	if err != nil {
		return err
	}

	if err := prepareOutput(opts.OutputPath, opts.Overwrite); err != nil {
		return err
	}

	sqliteDB, err := sql.Open("sqlite", opts.OutputPath)
	if err != nil {
		return fmt.Errorf("opening sqlite output: %w", err)
	}
	defer sqliteDB.Close()

	if _, err := sqliteDB.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		return fmt.Errorf("enabling wal mode: %w", err)
	}

	tx, err := sqliteDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting sqlite transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if err := createInternalTables(ctx, tx); err != nil {
		return err
	}
	if err := createSyncedTables(ctx, tx, localTables); err != nil {
		return err
	}
	if err := writeMetadata(ctx, tx, env, localTables); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing sqlite seed: %w", err)
	}
	rollback = false

	if _, err := sqliteDB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("checkpointing sqlite seed: %w", err)
	}

	return nil
}

func loadManifest(ctx context.Context, pg *sql.DB) (manifestEnvelope, error) {
	var raw []byte
	if err := pg.QueryRowContext(ctx, "SELECT synchro_schema_manifest()").Scan(&raw); err != nil {
		return manifestEnvelope{}, fmt.Errorf("loading schema manifest: %w", err)
	}

	var env manifestEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return manifestEnvelope{}, fmt.Errorf("decoding schema manifest: %w", err)
	}
	return env, nil
}

func (m manifestEnvelope) validate() error {
	if m.SchemaVersion <= 0 {
		return fmt.Errorf("schema_version must be positive, got %d", m.SchemaVersion)
	}
	if m.SchemaHash == "" {
		return errors.New("schema_hash must not be empty")
	}
	if len(m.Manifest.Tables) == 0 {
		return errors.New("schema manifest contains no synced tables")
	}

	tableNames := make(map[string]struct{}, len(m.Manifest.Tables))
	for _, table := range m.Manifest.Tables {
		if table.Name == "" {
			return errors.New("schema manifest contains an empty table name")
		}
		if _, exists := tableNames[table.Name]; exists {
			return fmt.Errorf("schema manifest contains duplicate table %s", table.Name)
		}
		tableNames[table.Name] = struct{}{}

		if len(table.PrimaryKey) != 1 {
			return fmt.Errorf("table %s must use exactly one primary key column for local seed generation", table.Name)
		}
		if table.UpdatedAtColumn == "" {
			return fmt.Errorf("table %s is missing updated_at_column", table.Name)
		}
		if table.DeletedAtColumn == "" {
			return fmt.Errorf("table %s is missing deleted_at_column", table.Name)
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("table %s has no columns", table.Name)
		}

		columnNames := make(map[string]struct{}, len(table.Columns))
		for _, column := range table.Columns {
			if column.Name == "" {
				return fmt.Errorf("table %s contains an empty column name", table.Name)
			}
			if _, exists := columnNames[column.Name]; exists {
				return fmt.Errorf("table %s contains duplicate column %s", table.Name, column.Name)
			}
			columnNames[column.Name] = struct{}{}
		}

		if _, ok := columnNames[table.PrimaryKey[0]]; !ok {
			return fmt.Errorf("table %s primary key column %s is missing from columns", table.Name, table.PrimaryKey[0])
		}
		if _, ok := columnNames[table.UpdatedAtColumn]; !ok {
			return fmt.Errorf("table %s updated_at column %s is missing from columns", table.Name, table.UpdatedAtColumn)
		}
		if _, ok := columnNames[table.DeletedAtColumn]; !ok {
			return fmt.Errorf("table %s deleted_at column %s is missing from columns", table.Name, table.DeletedAtColumn)
		}
	}

	return nil
}

func (m manifestEnvelope) localTables() ([]localSchemaTable, error) {
	tables := make([]localSchemaTable, 0, len(m.Manifest.Tables))
	for _, table := range m.Manifest.Tables {
		pk := table.PrimaryKey[0]
		localColumns := make([]localSchemaColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			isPK := column.Name == pk
			localColumns = append(localColumns, localSchemaColumn{
				Name:             column.Name,
				LogicalType:      column.Type,
				Nullable:         column.Nullable,
				SQLiteDefaultSQL: nil,
				IsPrimaryKey:     isPK,
			})
		}
		tables = append(tables, localSchemaTable{
			TableName:       table.Name,
			UpdatedAtColumn: table.UpdatedAtColumn,
			DeletedAtColumn: table.DeletedAtColumn,
			Composition:     table.Composition,
			PrimaryKey:      append([]string(nil), table.PrimaryKey...),
			Columns:         localColumns,
		})
	}
	return tables, nil
}

func prepareOutput(path string, overwrite bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	if _, err := os.Stat(path); err == nil {
		if !overwrite {
			return ErrOutputExists
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("removing existing output: %w", err)
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			_ = os.Remove(path + suffix)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking output path: %w", err)
	}
	return nil
}

func createInternalTables(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
			record_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			operation TEXT NOT NULL,
			base_updated_at TEXT,
			client_updated_at TEXT NOT NULL,
			PRIMARY KEY (table_name, record_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_bucket_members (
			bucket_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			checksum INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (bucket_id, table_name, record_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_bucket_checkpoints (
			bucket_id TEXT PRIMARY KEY,
			checkpoint INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_scopes (
			scope_id TEXT PRIMARY KEY,
			cursor TEXT,
			checksum TEXT,
			generation INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_scope_rows (
			scope_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			generation INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (scope_id, table_name, record_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_synchro_scope_rows_record
			ON _synchro_scope_rows (table_name, record_id)`,
		`INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('sync_lock', '0')`,
		`INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('checkpoint', '0')`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("creating sqlite internals: %w", err)
		}
	}

	return nil
}

func createSyncedTables(ctx context.Context, tx *sql.Tx, tables []localSchemaTable) error {
	for _, table := range tables {
		if _, err := tx.ExecContext(ctx, createTableSQL(table)); err != nil {
			return fmt.Errorf("creating synced table %s: %w", table.TableName, err)
		}
		for _, stmt := range cdcTriggerSQL(table) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("creating cdc triggers for %s: %w", table.TableName, err)
			}
		}
	}
	return nil
}

func writeMetadata(ctx context.Context, tx *sql.Tx, env manifestEnvelope, tables []localSchemaTable) error {
	localSchemaJSON, err := json.Marshal(tables)
	if err != nil {
		return fmt.Errorf("encoding local schema: %w", err)
	}

	meta := map[string]string{
		"schema_version":    fmt.Sprintf("%d", env.SchemaVersion),
		"schema_hash":       env.SchemaHash,
		"local_schema":      string(localSchemaJSON),
		"scope_set_version": "0",
		"known_buckets":     "[]",
		"snapshot_complete": "0",
	}

	for key, value := range meta {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _synchro_meta (key, value) VALUES (?, ?)
			 ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
			key,
			value,
		); err != nil {
			return fmt.Errorf("writing seed metadata %s: %w", key, err)
		}
	}

	return nil
}

func createTableSQL(table localSchemaTable) string {
	columnDefs := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		def := fmt.Sprintf("%s %s", quoteIdentifier(column.Name), sqliteType(column.LogicalType))
		if column.IsPrimaryKey {
			def += " PRIMARY KEY"
		}
		if !column.Nullable && !column.IsPrimaryKey {
			def += " NOT NULL"
		}
		if column.SQLiteDefaultSQL != nil && *column.SQLiteDefaultSQL != "" {
			def += " DEFAULT " + *column.SQLiteDefaultSQL
		}
		columnDefs = append(columnDefs, def)
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdentifier(table.TableName),
		strings.Join(columnDefs, ", "),
	)
}

func cdcTriggerSQL(table localSchemaTable) []string {
	name := table.TableName
	pkColumn := table.PrimaryKey[0]
	lockCheck := "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'"
	nowExpr := "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
	escapedName := escapeSQLString(name)
	insertTrigger := quoteIdentifier("_synchro_cdc_insert_" + name)
	updateTrigger := quoteIdentifier("_synchro_cdc_update_" + name)
	deleteTrigger := quoteIdentifier("_synchro_cdc_delete_" + name)
	quotedTable := quoteIdentifier(name)
	quotedPK := quoteIdentifier(pkColumn)
	quotedUpdatedAt := quoteIdentifier(table.UpdatedAtColumn)
	quotedDeletedAt := quoteIdentifier(table.DeletedAtColumn)

	return []string{
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s", insertTrigger),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s", updateTrigger),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s", deleteTrigger),
		fmt.Sprintf(`CREATE TRIGGER %s
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
END`, insertTrigger, quotedTable, lockCheck, quotedPK, escapedName, nowExpr),
		fmt.Sprintf(`CREATE TRIGGER %s
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
END`, updateTrigger, quotedTable, lockCheck, quotedPK, escapedName, quotedDeletedAt, quotedDeletedAt, quotedUpdatedAt, nowExpr, escapedName, quotedPK),
		fmt.Sprintf(`CREATE TRIGGER %s
BEFORE DELETE ON %s
WHEN %s
BEGIN
	UPDATE %s SET %s = %s WHERE %s = OLD.%s;
	SELECT RAISE(IGNORE);
END`, deleteTrigger, quotedTable, lockCheck, quotedTable, quotedDeletedAt, nowExpr, quotedPK, quotedPK),
	}
}

func sqliteType(logicalType string) string {
	switch logicalType {
	case "string":
		return "TEXT"
	case "int", "int64":
		return "INTEGER"
	case "float":
		return "REAL"
	case "boolean":
		return "INTEGER"
	case "datetime", "date", "time", "json":
		return "TEXT"
	case "bytes":
		return "BLOB"
	default:
		return "TEXT"
	}
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}
