package seeddb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

var ErrOutputExists = errors.New("seed output already exists")

type GenerateOptions struct {
	OutputPath string
	Overwrite  bool
}

type pgQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

type portableSeedManifest struct {
	SchemaVersion  int64               `json:"schema_version"`
	SchemaHash     string              `json:"schema_hash"`
	PortableScopes []portableSeedScope `json:"portable_scopes"`
}

type portableSeedScope struct {
	ID       string `json:"id"`
	Cursor   string `json:"cursor"`
	Checksum string `json:"checksum"`
}

type portableSeedPage struct {
	Scope   string               `json:"scope"`
	Records []portableSeedRecord `json:"records"`
	Cursor  *string              `json:"cursor"`
	HasMore bool                 `json:"has_more"`
}

type portableSeedRecord struct {
	Table    string         `json:"table"`
	RecordID string         `json:"record_id"`
	Checksum *int64         `json:"checksum"`
	Row      map[string]any `json:"row"`
}

func Generate(ctx context.Context, pg *sql.DB, opts GenerateOptions) error {
	if pg == nil {
		return errors.New("postgres database is required")
	}
	if strings.TrimSpace(opts.OutputPath) == "" {
		return errors.New("output path is required")
	}

	pgTx, err := pg.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("starting postgres snapshot transaction: %w", err)
	}
	defer pgTx.Rollback()

	env, err := loadManifest(ctx, pgTx)
	if err != nil {
		return err
	}
	if err := env.validate(); err != nil {
		return err
	}
	portable, err := loadPortableSeedManifest(ctx, pgTx)
	if err != nil {
		return err
	}
	if portable.SchemaVersion != env.SchemaVersion {
		return fmt.Errorf(
			"portable seed manifest schema_version %d does not match schema manifest %d",
			portable.SchemaVersion,
			env.SchemaVersion,
		)
	}
	if portable.SchemaHash != env.SchemaHash {
		return fmt.Errorf(
			"portable seed manifest schema_hash %q does not match schema manifest %q",
			portable.SchemaHash,
			env.SchemaHash,
		)
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
	if err := writePortableSeedData(ctx, pgTx, tx, localTables, portable); err != nil {
		return err
	}
	if err := createSyncedTableTriggers(ctx, tx, localTables); err != nil {
		return err
	}
	if err := writeMetadata(ctx, tx, env, localTables, portable); err != nil {
		return err
	}

	if err := pgTx.Commit(); err != nil {
		return fmt.Errorf("committing postgres snapshot transaction: %w", err)
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

func loadManifest(ctx context.Context, pg pgQueryer) (manifestEnvelope, error) {
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

func loadPortableSeedManifest(ctx context.Context, pg pgQueryer) (portableSeedManifest, error) {
	var raw []byte
	if err := pg.QueryRowContext(ctx, "SELECT synchro_portable_seed_manifest()").Scan(&raw); err != nil {
		return portableSeedManifest{}, fmt.Errorf("loading portable seed manifest: %w", err)
	}

	var manifest portableSeedManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return portableSeedManifest{}, fmt.Errorf("decoding portable seed manifest: %w", err)
	}
	return manifest, nil
}

func loadPortableSeedPage(
	ctx context.Context,
	pg pgQueryer,
	scopeID string,
	cursor string,
	limit int,
) (portableSeedPage, error) {
	var raw []byte
	if err := pg.QueryRowContext(
		ctx,
		"SELECT synchro_portable_seed_scope($1, $2, $3)",
		scopeID,
		cursor,
		limit,
	).Scan(&raw); err != nil {
		return portableSeedPage{}, fmt.Errorf("loading portable seed page for %s: %w", scopeID, err)
	}

	var page portableSeedPage
	if err := json.Unmarshal(raw, &page); err != nil {
		return portableSeedPage{}, fmt.Errorf("decoding portable seed page for %s: %w", scopeID, err)
	}
	return page, nil
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
		if table.UpdatedAtColumn != "" {
			if _, ok := columnNames[table.UpdatedAtColumn]; !ok {
				return fmt.Errorf("table %s updated_at column %s is missing from columns", table.Name, table.UpdatedAtColumn)
			}
		}
		if table.DeletedAtColumn != "" {
			if _, ok := columnNames[table.DeletedAtColumn]; !ok {
				return fmt.Errorf("table %s deleted_at column %s is missing from columns", table.Name, table.DeletedAtColumn)
			}
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
	}
	return nil
}

func createSyncedTableTriggers(ctx context.Context, tx *sql.Tx, tables []localSchemaTable) error {
	for _, table := range tables {
		for _, stmt := range cdcTriggerSQL(table) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("creating cdc triggers for %s: %w", table.TableName, err)
			}
		}
	}
	return nil
}

func writePortableSeedData(
	ctx context.Context,
	pg pgQueryer,
	tx *sql.Tx,
	tables []localSchemaTable,
	manifest portableSeedManifest,
) error {
	tableIndex := make(map[string]localSchemaTable, len(tables))
	for _, table := range tables {
		tableIndex[table.TableName] = table
	}

	for _, scope := range manifest.PortableScopes {
		if strings.TrimSpace(scope.ID) == "" {
			return errors.New("portable seed manifest contains an empty scope id")
		}
		generation := int64(0)
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation) VALUES (?, ?, ?, ?)
			 ON CONFLICT (scope_id) DO UPDATE SET
			    cursor = excluded.cursor,
			    checksum = excluded.checksum,
			    generation = excluded.generation`,
			scope.ID,
			scope.Cursor,
			scope.Checksum,
			generation,
		); err != nil {
			return fmt.Errorf("writing portable scope %s: %w", scope.ID, err)
		}

		checkpoint, err := parseScopeCheckpoint(scope.ID, scope.Cursor)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _synchro_bucket_checkpoints (bucket_id, checkpoint) VALUES (?, ?)
			 ON CONFLICT (bucket_id) DO UPDATE SET checkpoint = excluded.checkpoint`,
			scope.ID,
			checkpoint,
		); err != nil {
			return fmt.Errorf("writing portable checkpoint for %s: %w", scope.ID, err)
		}

		cursor := ""
		for {
			page, err := loadPortableSeedPage(ctx, pg, scope.ID, cursor, 1000)
			if err != nil {
				return err
			}
			for _, record := range page.Records {
				table, ok := tableIndex[record.Table]
				if !ok {
					return fmt.Errorf("portable seed record references unknown table %s", record.Table)
				}
				if err := upsertPortableRecord(ctx, tx, table, record); err != nil {
					return err
				}
				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, generation)
					 VALUES (?, ?, ?, ?)
					 ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET generation = excluded.generation`,
					scope.ID,
					record.Table,
					record.RecordID,
					generation,
				); err != nil {
					return fmt.Errorf("writing portable scope row %s/%s/%s: %w", scope.ID, record.Table, record.RecordID, err)
				}
				if err := upsertPortableBucketMember(ctx, tx, scope.ID, record); err != nil {
					return err
				}
			}

			if !page.HasMore {
				break
			}
			if page.Cursor == nil || *page.Cursor == "" {
				return fmt.Errorf("portable seed scope %s returned has_more without a continuation cursor", scope.ID)
			}
			cursor = *page.Cursor
		}
	}

	return nil
}

func upsertPortableRecord(ctx context.Context, tx *sql.Tx, table localSchemaTable, record portableSeedRecord) error {
	values := make([]any, 0, len(table.Columns))
	columns := make([]string, 0, len(table.Columns))
	assignments := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columns = append(columns, quoteIdentifier(column.Name))
		value, err := sqliteValue(column, record.Row[column.Name])
		if err != nil {
			return fmt.Errorf(
				"converting portable value for %s.%s record %s: %w",
				table.TableName,
				column.Name,
				record.RecordID,
				err,
			)
		}
		values = append(values, value)
		if !column.IsPrimaryKey {
			assignments = append(assignments, fmt.Sprintf("%s = excluded.%s", quoteIdentifier(column.Name), quoteIdentifier(column.Name)))
		}
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	sqlText := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(table.TableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if len(assignments) > 0 {
		sqlText += fmt.Sprintf(
			" ON CONFLICT (%s) DO UPDATE SET %s",
			quoteIdentifier(primaryKeyColumn(table)),
			strings.Join(assignments, ", "),
		)
	}

	if _, err := tx.ExecContext(ctx, sqlText, values...); err != nil {
		return fmt.Errorf("upserting portable row into %s: %w", table.TableName, err)
	}
	return nil
}

func upsertPortableBucketMember(ctx context.Context, tx *sql.Tx, scopeID string, record portableSeedRecord) error {
	if record.Checksum != nil {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _synchro_bucket_members (bucket_id, table_name, record_id, checksum) VALUES (?, ?, ?, ?)
			 ON CONFLICT (bucket_id, table_name, record_id) DO UPDATE SET checksum = excluded.checksum`,
			scopeID,
			record.Table,
			record.RecordID,
			*record.Checksum,
		); err != nil {
			return fmt.Errorf("writing portable bucket member %s/%s/%s: %w", scopeID, record.Table, record.RecordID, err)
		}
		return nil
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO _synchro_bucket_members (bucket_id, table_name, record_id, checksum) VALUES (?, ?, ?, NULL)
		 ON CONFLICT (bucket_id, table_name, record_id) DO UPDATE SET checksum = excluded.checksum`,
		scopeID,
		record.Table,
		record.RecordID,
	); err != nil {
		return fmt.Errorf("writing portable bucket member %s/%s/%s: %w", scopeID, record.Table, record.RecordID, err)
	}
	return nil
}

func primaryKeyColumn(table localSchemaTable) string {
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			return column.Name
		}
	}
	if len(table.PrimaryKey) > 0 {
		return table.PrimaryKey[0]
	}
	return "id"
}

func parseScopeCheckpoint(scopeID string, cursor string) (int64, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	checkpoint, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("portable scope %s cursor %q is not a decimal sequence value: %w", scopeID, cursor, err)
	}
	return checkpoint, nil
}

func sqliteValue(column localSchemaColumn, raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}

	switch column.LogicalType {
	case "string", "datetime", "date", "time":
		return fmt.Sprint(raw), nil
	case "json":
		if value, ok := raw.(string); ok {
			return value, nil
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("encoding json: %w", err)
		}
		return string(encoded), nil
	case "boolean":
		switch value := raw.(type) {
		case bool:
			if value {
				return int64(1), nil
			}
			return int64(0), nil
		case float64:
			if value == 0 {
				return int64(0), nil
			}
			if value == 1 {
				return int64(1), nil
			}
		case string:
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "true", "1":
				return int64(1), nil
			case "false", "0":
				return int64(0), nil
			}
		}
		return nil, fmt.Errorf("unsupported boolean value %T", raw)
	case "int", "int64":
		switch value := raw.(type) {
		case float64:
			if math.Trunc(value) != value {
				return nil, fmt.Errorf("non-integer numeric value %v", value)
			}
			return int64(value), nil
		case int64:
			return value, nil
		case int:
			return int64(value), nil
		case json.Number:
			return value.Int64()
		case string:
			return strconv.ParseInt(value, 10, 64)
		}
		return nil, fmt.Errorf("unsupported integer value %T", raw)
	case "float":
		switch value := raw.(type) {
		case float64:
			return value, nil
		case float32:
			return float64(value), nil
		case int64:
			return float64(value), nil
		case int:
			return float64(value), nil
		case json.Number:
			return value.Float64()
		case string:
			return strconv.ParseFloat(value, 64)
		}
		return nil, fmt.Errorf("unsupported float value %T", raw)
	case "bytes":
		switch value := raw.(type) {
		case []byte:
			return value, nil
		case string:
			return []byte(value), nil
		}
		return nil, fmt.Errorf("unsupported bytes value %T", raw)
	default:
		return raw, nil
	}
}

func writeMetadata(
	ctx context.Context,
	tx *sql.Tx,
	env manifestEnvelope,
	tables []localSchemaTable,
	portable portableSeedManifest,
) error {
	localSchemaJSON, err := json.Marshal(tables)
	if err != nil {
		return fmt.Errorf("encoding local schema: %w", err)
	}
	knownBucketsJSON, err := json.Marshal(portableScopeIDs(portable.PortableScopes))
	if err != nil {
		return fmt.Errorf("encoding portable scope ids: %w", err)
	}
	snapshotComplete := "0"
	if len(portable.PortableScopes) > 0 {
		snapshotComplete = "1"
	}

	meta := map[string]string{
		"schema_version":    fmt.Sprintf("%d", env.SchemaVersion),
		"schema_hash":       env.SchemaHash,
		"local_schema":      string(localSchemaJSON),
		"scope_set_version": "0",
		"known_buckets":     string(knownBucketsJSON),
		"snapshot_complete": snapshotComplete,
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

func portableScopeIDs(scopes []portableSeedScope) []string {
	ids := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		ids = append(ids, scope.ID)
	}
	return ids
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
	hasUpdatedAt := strings.TrimSpace(table.UpdatedAtColumn) != ""
	hasDeletedAt := strings.TrimSpace(table.DeletedAtColumn) != ""
	escapedName := escapeSQLString(name)
	insertTrigger := quoteIdentifier("_synchro_cdc_insert_" + name)
	updateTrigger := quoteIdentifier("_synchro_cdc_update_" + name)
	deleteTrigger := quoteIdentifier("_synchro_cdc_delete_" + name)
	quotedTable := quoteIdentifier(name)
	quotedPK := quoteIdentifier(pkColumn)
	quotedUpdatedAt := quoteIdentifier(table.UpdatedAtColumn)
	quotedDeletedAt := quoteIdentifier(table.DeletedAtColumn)

	baseUpdatedAtExpr := "NULL"
	if hasUpdatedAt {
		baseUpdatedAtExpr = "OLD." + quotedUpdatedAt
	}

	updateOperationExpr := "'update'"
	if hasDeletedAt {
		updateOperationExpr = fmt.Sprintf(
			"CASE WHEN NEW.%s IS NOT NULL AND OLD.%s IS NULL THEN 'delete' ELSE 'update' END",
			quotedDeletedAt,
			quotedDeletedAt,
		)
	}

	updateCleanupSQL := ""
	if hasDeletedAt {
		updateCleanupSQL = fmt.Sprintf(`
	DELETE FROM _synchro_pending_changes
	WHERE table_name = '%s' AND record_id = NEW.%s
	  AND operation = 'delete'
	  AND base_updated_at IS NULL;`, escapedName, quotedPK)
	}

	deleteTriggerSQL := fmt.Sprintf(`CREATE TRIGGER %s
BEFORE DELETE ON %s
WHEN %s
BEGIN
	UPDATE %s SET %s = %s WHERE %s = OLD.%s;
	SELECT RAISE(IGNORE);
END`, deleteTrigger, quotedTable, lockCheck, quotedTable, quotedDeletedAt, nowExpr, quotedPK, quotedPK)
	if !hasDeletedAt {
		deleteTriggerSQL = fmt.Sprintf(`CREATE TRIGGER %s
AFTER DELETE ON %s
WHEN %s
BEGIN
	INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
	VALUES (OLD.%s, '%s', 'delete', %s, %s)
	ON CONFLICT (table_name, record_id) DO UPDATE SET
		operation = 'delete',
		base_updated_at = COALESCE(_synchro_pending_changes.base_updated_at, excluded.base_updated_at),
		client_updated_at = excluded.client_updated_at;
END`, deleteTrigger, quotedTable, lockCheck, quotedPK, escapedName, baseUpdatedAtExpr, nowExpr)
	}

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
		%s,
		%s,
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
%s
END`, updateTrigger, quotedTable, lockCheck, quotedPK, escapedName, updateOperationExpr, baseUpdatedAtExpr, nowExpr, updateCleanupSQL),
		deleteTriggerSQL,
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
