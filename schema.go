package synchro

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

type schemaManifest struct {
	Version   int64
	Hash      string
	CreatedAt time.Time
}

// SchemaManifestEntry represents one persisted schema manifest version.
type SchemaManifestEntry struct {
	SchemaVersion int64     `json:"schema_version"`
	SchemaHash    string    `json:"schema_hash"`
	CreatedAt     time.Time `json:"created_at"`
}

type schemaStore struct{}

func (s *schemaStore) GetManifest(ctx context.Context, db DB, registry *Registry) (schemaManifest, error) {
	tables, err := s.buildCanonicalSchemaTables(ctx, db, registry)
	if err != nil {
		return schemaManifest{}, err
	}
	hash, err := computeSchemaHash(tables)
	if err != nil {
		return schemaManifest{}, err
	}
	return s.getOrCreateManifest(ctx, db, hash)
}

func (s *schemaStore) GetSchema(ctx context.Context, db DB, registry *Registry) (*SchemaResponse, error) {
	tables, err := s.buildCanonicalSchemaTables(ctx, db, registry)
	if err != nil {
		return nil, err
	}
	hash, err := computeSchemaHash(tables)
	if err != nil {
		return nil, err
	}
	manifest, err := s.getOrCreateManifest(ctx, db, hash)
	if err != nil {
		return nil, err
	}

	return &SchemaResponse{
		SchemaVersion: manifest.Version,
		SchemaHash:    manifest.Hash,
		ServerTime:    time.Now().UTC(),
		Tables:        tables,
	}, nil
}

func (s *schemaStore) ListManifests(ctx context.Context, db DB, limit int) ([]SchemaManifestEntry, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := db.QueryContext(ctx, `
		SELECT schema_version, schema_hash, created_at
		FROM sync_schema_manifest
		ORDER BY schema_version DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("listing schema manifests: %w", err)
	}
	defer rows.Close()

	result := make([]SchemaManifestEntry, 0, limit)
	for rows.Next() {
		var row SchemaManifestEntry
		if err := rows.Scan(&row.SchemaVersion, &row.SchemaHash, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning schema manifest row: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading schema manifest rows: %w", err)
	}
	return result, nil
}

func (s *schemaStore) buildCanonicalSchemaTables(ctx context.Context, db DB, registry *Registry) ([]SchemaTable, error) {
	configs := registry.All()
	slices.SortFunc(configs, func(a, b *TableConfig) int {
		return strings.Compare(a.TableName, b.TableName)
	})

	tables := make([]SchemaTable, 0, len(configs))
	for _, cfg := range configs {
		cols, pk, err := loadTableColumnsAndPK(ctx, db, cfg.TableName)
		if err != nil {
			return nil, err
		}
		if len(pk) == 0 {
			return nil, fmt.Errorf("%w: table %q has no primary key", ErrUnsupportedSchemaFeature, cfg.TableName)
		}
		if len(pk) > 1 {
			return nil, fmt.Errorf("%w: table %q has composite primary key", ErrUnsupportedSchemaFeature, cfg.TableName)
		}

		deps := append([]string{}, cfg.Dependencies...)
		slices.Sort(deps)

		table := SchemaTable{
			TableName:            cfg.TableName,
			PushPolicy:           string(cfg.PushPolicy),
			ParentTable:          cfg.ParentTable,
			ParentFKCol:          cfg.ParentFKCol,
			Dependencies:         deps,
			UpdatedAtColumn:      cfg.UpdatedAtColumn,
			DeletedAtColumn:      cfg.DeletedAtColumn,
			PrimaryKey:           pk,
			BucketByColumn:       cfg.BucketByColumn,
			BucketPrefix:         cfg.BucketPrefix,
			GlobalWhenBucketNull: cfg.GlobalWhenBucketNull,
			AllowGlobalRead:      cfg.AllowGlobalRead,
			BucketFunction:       cfg.BucketFunction,
			Columns:              cols,
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (s *schemaStore) getOrCreateManifest(ctx context.Context, db DB, hash string) (schemaManifest, error) {
	if txBeginner, ok := db.(TxBeginner); ok {
		tx, err := txBeginner.BeginTx(ctx, nil)
		if err != nil {
			return schemaManifest{}, fmt.Errorf("starting manifest tx: %w", err)
		}
		defer tx.Rollback()
		manifest, err := getOrCreateManifestTx(ctx, tx, hash)
		if err != nil {
			return schemaManifest{}, err
		}
		if err := tx.Commit(); err != nil {
			return schemaManifest{}, fmt.Errorf("committing manifest tx: %w", err)
		}
		return manifest, nil
	}
	return getOrCreateManifestTx(ctx, db, hash)
}

func getOrCreateManifestTx(ctx context.Context, db DB, hash string) (schemaManifest, error) {
	if _, err := db.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", int64(91110042)); err != nil {
		return schemaManifest{}, fmt.Errorf("locking schema manifest: %w", err)
	}

	var manifest schemaManifest
	err := db.QueryRowContext(ctx, `
		SELECT schema_version, schema_hash, created_at
		FROM sync_schema_manifest
		WHERE schema_hash = $1
	`, hash).Scan(&manifest.Version, &manifest.Hash, &manifest.CreatedAt)
	if err == nil {
		return manifest, nil
	}
	if err != sql.ErrNoRows {
		return schemaManifest{}, fmt.Errorf("querying schema manifest by hash: %w", err)
	}

	var nextVersion int64
	if err := db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(schema_version), 0) + 1 FROM sync_schema_manifest").Scan(&nextVersion); err != nil {
		return schemaManifest{}, fmt.Errorf("computing next schema version: %w", err)
	}

	if err := db.QueryRowContext(ctx, `
		INSERT INTO sync_schema_manifest (schema_version, schema_hash)
		VALUES ($1, $2)
		RETURNING schema_version, schema_hash, created_at
	`, nextVersion, hash).Scan(&manifest.Version, &manifest.Hash, &manifest.CreatedAt); err != nil {
		return schemaManifest{}, fmt.Errorf("inserting schema manifest row: %w", err)
	}
	return manifest, nil
}

type columnMeta struct {
	Name       string
	DBType     string
	Nullable   bool
	DefaultSQL string
	IsPK       bool
}

func loadTableColumnsAndPK(ctx context.Context, db DB, table string) ([]SchemaColumn, []string, error) {
	schemaName, tableName := splitSchemaTable(table)

	pkCols, err := loadPrimaryKeyColumns(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, nil, err
	}
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, c := range pkCols {
		pkSet[c] = struct{}{}
	}

	colRows, err := queryColumns(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer colRows.Close()

	var cols []SchemaColumn
	for colRows.Next() {
		var c columnMeta
		if err := colRows.Scan(&c.Name, &c.DBType, &c.Nullable, &c.DefaultSQL); err != nil {
			return nil, nil, fmt.Errorf("scanning schema column for %q: %w", table, err)
		}
		_, c.IsPK = pkSet[c.Name]
		logicalType, err := mapLogicalType(c.DBType)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: table %q column %q type %q", ErrUnsupportedSchemaFeature, table, c.Name, c.DBType)
		}
		cols = append(cols, SchemaColumn{
			Name:         c.Name,
			DBType:       c.DBType,
			LogicalType:  logicalType,
			Nullable:     c.Nullable,
			DefaultSQL:   c.DefaultSQL,
			IsPrimaryKey: c.IsPK,
		})
	}
	if err := colRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("reading schema columns for %q: %w", table, err)
	}
	return cols, pkCols, nil
}

func queryColumns(ctx context.Context, db DB, schemaName, tableName string) (*sql.Rows, error) {
	if schemaName != "" {
		return db.QueryContext(ctx, `
			SELECT
				a.attname AS column_name,
				pg_catalog.format_type(a.atttypid, a.atttypmod) AS db_type,
				NOT a.attnotnull AS nullable,
				COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS default_sql
			FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
			WHERE c.relname = $1
			  AND n.nspname = $2
			  AND a.attnum > 0
			  AND NOT a.attisdropped
			ORDER BY a.attnum
		`, tableName, schemaName)
	}
	return db.QueryContext(ctx, `
		SELECT
			a.attname AS column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS db_type,
			NOT a.attnotnull AS nullable,
			COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS default_sql
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE c.relname = $1
		  AND n.nspname = ANY (current_schemas(false))
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum
	`, tableName)
}

func loadPrimaryKeyColumns(ctx context.Context, db DB, schemaName, tableName string) ([]string, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if schemaName != "" {
		rows, err = db.QueryContext(ctx, `
			SELECT a.attname
			FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			JOIN unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ord) ON true
			JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ord.attnum
			WHERE i.indisprimary
			  AND c.relname = $1
			  AND n.nspname = $2
			ORDER BY ord.ord
		`, tableName, schemaName)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT a.attname
			FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			JOIN unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ord) ON true
			JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ord.attnum
			WHERE i.indisprimary
			  AND c.relname = $1
			  AND n.nspname = ANY (current_schemas(false))
			ORDER BY ord.ord
		`, tableName)
	}
	if err != nil {
		return nil, fmt.Errorf("querying primary key columns for %q: %w", tableName, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("scanning primary key column for %q: %w", tableName, err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading primary key columns for %q: %w", tableName, err)
	}
	return cols, nil
}

func splitSchemaTable(name string) (schema string, table string) {
	if idx := strings.IndexByte(name, '.'); idx > 0 && idx < len(name)-1 {
		return name[:idx], name[idx+1:]
	}
	return "", name
}

var rePrefix = regexp.MustCompile(`^([a-z ]+)(\(.+\))?$`)

func mapLogicalType(dbType string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(dbType))
	if strings.HasSuffix(t, "[]") {
		return "", ErrUnsupportedSchemaFeature
	}

	switch t {
	case "bool", "boolean":
		return "boolean", nil
	case "smallint", "integer", "int", "serial", "smallserial":
		return "int", nil
	case "bigint", "bigserial":
		return "int64", nil
	case "real", "double precision", "numeric", "decimal":
		return "float", nil
	case "text", "character varying", "varchar", "character", "char", "uuid":
		return "string", nil
	case "json", "jsonb":
		return "json", nil
	case "bytea":
		return "bytes", nil
	case "date":
		return "date", nil
	case "time without time zone", "time with time zone":
		return "time", nil
	case "timestamp without time zone", "timestamp with time zone":
		return "datetime", nil
	}

	if strings.HasPrefix(t, "character varying(") || strings.HasPrefix(t, "character(") {
		return "string", nil
	}
	if strings.HasPrefix(t, "numeric(") || strings.HasPrefix(t, "decimal(") {
		return "float", nil
	}

	if m := rePrefix.FindStringSubmatch(t); len(m) > 1 {
		switch m[1] {
		case "timestamp without time zone", "timestamp with time zone":
			return "datetime", nil
		}
	}
	return "", ErrUnsupportedSchemaFeature
}

func computeSchemaHash(tables []SchemaTable) (string, error) {
	payload := struct {
		Tables []SchemaTable `json:"tables"`
	}{
		Tables: tables,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshalling schema for hash: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validateClientSchema(manifest schemaManifest, reqVersion int64, reqHash string, allowBootstrap bool) error {
	if allowBootstrap && reqVersion == 0 && reqHash == "" {
		return nil
	}
	if reqVersion <= 0 || reqHash == "" {
		return ErrSchemaMismatch
	}
	if reqVersion != manifest.Version || reqHash != manifest.Hash {
		return ErrSchemaMismatch
	}
	return nil
}
