package seeddb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

var (
	rowIdentityDomain    = []byte("synchro:v3:row-identity:v1\x00")
	rowDigestDomain      = []byte("synchro:v3:row-digest:v1\x00")
	scopeDigestDomain    = []byte("synchro:v3:scope-digest:v1\x00")
	schemaManifestDomain = []byte("synchro:v3:schema-manifest:v1\x00")
	exportManifestDomain = []byte("synchro:v3:seed-export-manifest:v1\x00")
)

const maxSafeJSONInteger = 9007199254740991

type sqliteInternalColumnSpec struct {
	name       string
	typeName   string
	notNull    bool
	defaultSQL *string
	pkOrdinal  int
}

type sqliteInternalIndexSpec struct {
	name       string
	sql        string
	columns    []string
	collations []string
	descending []bool
	unique     bool
}

type sqliteInternalTableSpec struct {
	name    string
	columns []sqliteInternalColumnSpec
	indexes []sqliteInternalIndexSpec
}

func sqliteDefault(value string) *string {
	return &value
}

// These specifications mirror the generator's canonical internal DDL in seeddb.go.
func portableSeedInternalTableSpecs() []sqliteInternalTableSpec {
	return []sqliteInternalTableSpec{
		{
			name: "_synchro_pending_changes",
			columns: []sqliteInternalColumnSpec{
				{name: "record_id", typeName: "TEXT", notNull: true, pkOrdinal: 2},
				{name: "table_name", typeName: "TEXT", notNull: true, pkOrdinal: 1},
				{name: "operation", typeName: "TEXT", notNull: true},
				{name: "base_updated_at", typeName: "TEXT"},
				{name: "client_updated_at", typeName: "TEXT", notNull: true},
				{name: "local_revision", typeName: "INTEGER", notNull: true, defaultSQL: sqliteDefault("0")},
			},
		},
		{
			name: "_synchro_meta",
			columns: []sqliteInternalColumnSpec{
				{name: "key", typeName: "TEXT", pkOrdinal: 1},
				{name: "value", typeName: "TEXT", notNull: true},
			},
		},
		{
			name: "grdb_migrations",
			columns: []sqliteInternalColumnSpec{
				{name: "identifier", typeName: "TEXT", notNull: true, pkOrdinal: 1},
			},
		},
		{
			name: "_synchro_scopes",
			columns: []sqliteInternalColumnSpec{
				{name: "scope_id", typeName: "TEXT", pkOrdinal: 1},
				{name: "cursor", typeName: "TEXT"},
				{name: "checksum", typeName: "TEXT"},
				{name: "generation", typeName: "INTEGER", notNull: true, defaultSQL: sqliteDefault("0")},
				{name: "local_checksum", typeName: "TEXT", notNull: true, defaultSQL: sqliteDefault("''")},
			},
		},
		{
			name: "_synchro_scope_rows",
			columns: []sqliteInternalColumnSpec{
				{name: "scope_id", typeName: "TEXT", notNull: true, pkOrdinal: 1},
				{name: "table_name", typeName: "TEXT", notNull: true, pkOrdinal: 2},
				{name: "record_id", typeName: "TEXT", notNull: true, pkOrdinal: 3},
				{name: "checksum", typeName: "TEXT", notNull: true},
				{name: "generation", typeName: "INTEGER", notNull: true, defaultSQL: sqliteDefault("0")},
			},
			indexes: []sqliteInternalIndexSpec{
				{
					name:       "idx_synchro_scope_rows_record",
					sql:        "CREATE INDEX idx_synchro_scope_rows_record ON _synchro_scope_rows (table_name, record_id)",
					columns:    []string{"table_name", "record_id"},
					collations: []string{"BINARY", "BINARY"},
					descending: []bool{false, false},
				},
			},
		},
		{
			name: "_synchro_seed_receipts",
			columns: []sqliteInternalColumnSpec{
				{name: "scope_id", typeName: "TEXT", pkOrdinal: 1},
				{name: "receipt", typeName: "TEXT", notNull: true},
				{name: "schema_version", typeName: "INTEGER", notNull: true},
				{name: "schema_hash", typeName: "TEXT", notNull: true},
				{name: "cardinality", typeName: "INTEGER", notNull: true},
				{name: "checksum", typeName: "TEXT", notNull: true},
			},
		},
		{
			name: "_synchro_row_versions",
			columns: []sqliteInternalColumnSpec{
				{name: "table_name", typeName: "TEXT", notNull: true, pkOrdinal: 1},
				{name: "record_id", typeName: "TEXT", notNull: true, pkOrdinal: 2},
				{name: "server_version", typeName: "TEXT", notNull: true},
				{name: "row_checksum", typeName: "TEXT"},
			},
		},
		{
			name: "_synchro_rebuild_attempts",
			columns: []sqliteInternalColumnSpec{
				{name: "scope_id", typeName: "TEXT", pkOrdinal: 1},
				{name: "rebuild_id", typeName: "TEXT", notNull: true},
				{name: "client_generation", typeName: "INTEGER", notNull: true},
				{name: "schema_version", typeName: "INTEGER", notNull: true},
				{name: "schema_hash", typeName: "TEXT", notNull: true},
				{name: "generation", typeName: "INTEGER", notNull: true},
				{name: "cursor", typeName: "TEXT"},
				{name: "page_limit", typeName: "INTEGER", notNull: true},
			},
		},
		{
			name: "_synchro_push_batches",
			columns: []sqliteInternalColumnSpec{
				{name: "batch_id", typeName: "TEXT", pkOrdinal: 1},
				{name: "request_json", typeName: "TEXT", notNull: true},
				{name: "pending_json", typeName: "TEXT", notNull: true},
				{name: "schema_json", typeName: "TEXT", notNull: true},
				{name: "state", typeName: "TEXT", notNull: true},
				{name: "created_at", typeName: "TEXT", notNull: true},
				{name: "completed_at", typeName: "TEXT"},
			},
		},
		{
			name: "_synchro_rejected_mutations",
			columns: []sqliteInternalColumnSpec{
				{name: "mutation_id", typeName: "TEXT", pkOrdinal: 1},
				{name: "table_name", typeName: "TEXT", notNull: true},
				{name: "record_id", typeName: "TEXT", notNull: true},
				{name: "status", typeName: "TEXT", notNull: true},
				{name: "code", typeName: "TEXT", notNull: true},
				{name: "message", typeName: "TEXT"},
				{name: "server_row_json", typeName: "TEXT"},
				{name: "server_version", typeName: "TEXT"},
				{name: "created_at", typeName: "TEXT", notNull: true},
				{name: "updated_at", typeName: "TEXT", notNull: true},
			},
			indexes: []sqliteInternalIndexSpec{
				{
					name:       "idx_synchro_rejected_mutations_record",
					sql:        "CREATE INDEX idx_synchro_rejected_mutations_record ON _synchro_rejected_mutations (table_name, record_id)",
					columns:    []string{"table_name", "record_id"},
					collations: []string{"BINARY", "BINARY"},
					descending: []bool{false, false},
				},
			},
		},
	}
}

func verifySQLiteInternalTable(ctx context.Context, db *sql.DB, spec sqliteInternalTableSpec) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quoteIdentifier(spec.name)+")")
	if err != nil {
		return fmt.Errorf("reading sqlite seed table %s: %w", spec.name, err)
	}
	defer rows.Close()
	for index, expected := range spec.columns {
		if !rows.Next() {
			return fmt.Errorf("sqlite seed table %s is missing column %s", spec.name, expected.name)
		}
		var cid, notNull, primaryKey int
		var name, typeName string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("reading sqlite seed table %s column: %w", spec.name, err)
		}
		if cid != index || name != expected.name || typeName != expected.typeName ||
			(notNull != 0) != expected.notNull || primaryKey != expected.pkOrdinal ||
			!sameSQLiteDefault(defaultValue, expected.defaultSQL) {
			return fmt.Errorf("sqlite seed table %s column %s does not match the canonical schema", spec.name, expected.name)
		}
	}
	if rows.Next() {
		return fmt.Errorf("sqlite seed table %s has an unexpected column", spec.name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed table %s: %w", spec.name, err)
	}

	if err := verifySQLiteInternalTableSQL(ctx, db, spec.name); err != nil {
		return err
	}
	return verifySQLiteInternalIndexes(ctx, db, spec)
}

func sameSQLiteDefault(actual any, expected *string) bool {
	if expected == nil {
		return actual == nil
	}
	var value string
	switch actual := actual.(type) {
	case string:
		value = actual
	case []byte:
		value = string(actual)
	default:
		return false
	}
	return value == *expected
}

func verifySQLiteInternalTableSQL(ctx context.Context, db *sql.DB, tableName string) error {
	var statement string
	if err := db.QueryRowContext(
		ctx,
		"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
		tableName,
	).Scan(&statement); err != nil {
		return fmt.Errorf("reading sqlite seed table %s definition: %w", tableName, err)
	}
	words := strings.FieldsFunc(strings.ToUpper(statement), func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	forbidden := map[string]struct{}{
		"AS": {}, "ASC": {}, "AUTOINCREMENT": {}, "CHECK": {}, "COLLATE": {}, "CONFLICT": {},
		"CONSTRAINT": {}, "DEFERRABLE": {}, "DESC": {}, "GENERATED": {}, "ON": {}, "REFERENCES": {},
		"STRICT": {}, "STORED": {}, "UNIQUE": {}, "VIRTUAL": {},
	}
	for _, word := range words {
		if _, exists := forbidden[word]; exists {
			return fmt.Errorf("sqlite seed table %s has an unsupported constraint", tableName)
		}
	}
	for index := 0; index+1 < len(words); index++ {
		if words[index] == "WITHOUT" && words[index+1] == "ROWID" {
			return fmt.Errorf("sqlite seed table %s has an unsupported constraint", tableName)
		}
	}
	return nil
}

func verifySQLiteInternalIndexes(ctx context.Context, db *sql.DB, spec sqliteInternalTableSpec) error {
	type actualIndex struct {
		name    string
		unique  bool
		origin  string
		partial bool
	}
	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+quoteIdentifier(spec.name)+")")
	if err != nil {
		return fmt.Errorf("reading sqlite seed indexes for %s: %w", spec.name, err)
	}
	defer rows.Close()
	actual := make([]actualIndex, 0, len(spec.indexes)+1)
	for rows.Next() {
		if len(actual) > len(spec.indexes)+1 {
			return fmt.Errorf("sqlite seed table %s has too many indexes", spec.name)
		}
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			return fmt.Errorf("reading sqlite seed index for %s: %w", spec.name, err)
		}
		actual = append(actual, actualIndex{name: name, unique: unique != 0, origin: origin, partial: partial != 0})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed indexes for %s: %w", spec.name, err)
	}
	if len(actual) != len(spec.indexes)+1 {
		return fmt.Errorf("sqlite seed table %s index set does not match the canonical schema", spec.name)
	}

	pkColumns := make([]string, 0)
	for _, column := range spec.columns {
		if column.pkOrdinal > 0 {
			for len(pkColumns) < column.pkOrdinal {
				pkColumns = append(pkColumns, "")
			}
			pkColumns[column.pkOrdinal-1] = column.name
		}
	}
	seenPK := false
	seenRequired := make(map[string]struct{}, len(spec.indexes))
	primaryKeySpec := sqliteInternalIndexSpec{
		columns:    pkColumns,
		collations: make([]string, len(pkColumns)),
		descending: make([]bool, len(pkColumns)),
	}
	for index := range primaryKeySpec.collations {
		primaryKeySpec.collations[index] = "BINARY"
	}
	for _, index := range actual {
		switch index.origin {
		case "pk":
			if seenPK || !index.unique || index.partial || !sameSQLiteIndexColumns(ctx, db, index.name, primaryKeySpec) {
				return fmt.Errorf("sqlite seed table %s primary key index does not match the canonical schema", spec.name)
			}
			seenPK = true
		case "c":
			expected, ok := findSQLiteIndexSpec(spec.indexes, index.name)
			if !ok || index.partial || index.unique != expected.unique ||
				!sameSQLiteIndexSQL(ctx, db, index.name, expected.sql) ||
				!sameSQLiteIndexColumns(ctx, db, index.name, expected) {
				return fmt.Errorf("sqlite seed table %s index %s does not match the canonical schema", spec.name, index.name)
			}
			seenRequired[index.name] = struct{}{}
		default:
			return fmt.Errorf("sqlite seed table %s has an unsupported index %s", spec.name, index.name)
		}
	}
	if !seenPK || len(seenRequired) != len(spec.indexes) {
		return fmt.Errorf("sqlite seed table %s required indexes are incomplete", spec.name)
	}
	return nil
}

func findSQLiteIndexSpec(specs []sqliteInternalIndexSpec, name string) (sqliteInternalIndexSpec, bool) {
	for _, spec := range specs {
		if spec.name == name {
			return spec, true
		}
	}
	return sqliteInternalIndexSpec{}, false
}

func sameSQLiteIndexSQL(ctx context.Context, db *sql.DB, indexName, expected string) bool {
	var actual string
	if err := db.QueryRowContext(
		ctx,
		"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
		indexName,
	).Scan(&actual); err != nil {
		return false
	}
	return canonicalSQLiteSQL(actual) == canonicalSQLiteSQL(expected)
}

func canonicalSQLiteSQL(statement string) string {
	return strings.ToLower(strings.Join(strings.Fields(statement), " "))
}

func sameSQLiteIndexColumns(ctx context.Context, db *sql.DB, indexName string, expected sqliteInternalIndexSpec) bool {
	rows, err := db.QueryContext(ctx, "PRAGMA index_xinfo("+quoteIdentifier(indexName)+")")
	if err != nil {
		return false
	}
	defer rows.Close()
	keyIndex := 0
	auxiliaryRows := 0
	for rows.Next() {
		var sequence, columnID, descending, key int
		var name, collation sql.NullString
		if err := rows.Scan(&sequence, &columnID, &name, &descending, &collation, &key); err != nil {
			return false
		}
		if key == 0 {
			if columnID != -1 || name.Valid || descending != 0 || !collation.Valid || collation.String != "BINARY" {
				return false
			}
			auxiliaryRows++
			continue
		}
		if keyIndex >= len(expected.columns) || sequence != keyIndex || !name.Valid ||
			name.String != expected.columns[keyIndex] || !collation.Valid ||
			collation.String != expected.collations[keyIndex] ||
			(descending != 0) != expected.descending[keyIndex] {
			return false
		}
		if columnID < 0 {
			return false
		}
		keyIndex++
	}
	return rows.Err() == nil && keyIndex == len(expected.columns) && auxiliaryRows <= 1
}

type seedDigestRow struct {
	identity []byte
	digest   [sha256.Size]byte
}

func (checksum checksumObject) validate() error {
	if checksum.Algorithm != "sha256" || checksum.Version != 1 || checksum.Encoding != "hex" {
		return errors.New("unsupported checksum format")
	}
	decoded, err := hex.DecodeString(checksum.Digest)
	if err != nil || len(decoded) != sha256.Size || checksum.Digest != strings.ToLower(checksum.Digest) {
		return errors.New("invalid checksum digest")
	}
	return nil
}

func schemaManifestDigest(manifest schemaManifest) (string, error) {
	canonical, err := canonicalJSON(schemaManifestHashBody(manifest))
	if err != nil {
		return "", fmt.Errorf("canonicalizing schema manifest: %w", err)
	}
	return digestCanonical(schemaManifestDomain, canonical), nil
}

func exportManifestDigest(manifest portableSeedManifest) (string, error) {
	canonical, err := canonicalJSON(exportManifestHashBody(manifest))
	if err != nil {
		return "", fmt.Errorf("canonicalizing portable seed manifest: %w", err)
	}
	return digestCanonical(exportManifestDomain, canonical), nil
}

func digestCanonical(domain, canonical []byte) string {
	hasher := sha256.New()
	_, _ = hasher.Write(domain)
	_, _ = hasher.Write(canonical)
	return hex.EncodeToString(hasher.Sum(nil))
}

func schemaManifestHashBody(manifest schemaManifest) map[string]any {
	tables := append([]tableSchema(nil), manifest.Tables...)
	sort.Slice(tables, func(i, j int) bool { return bytes.Compare([]byte(tables[i].ID), []byte(tables[j].ID)) < 0 })
	canonicalTables := make([]any, 0, len(tables))
	for _, table := range tables {
		fields := append([]fieldSchema(nil), table.Fields...)
		sort.Slice(fields, func(i, j int) bool { return bytes.Compare([]byte(fields[i].ID), []byte(fields[j].ID)) < 0 })
		canonicalFields := make([]any, 0, len(table.Fields))
		for _, field := range fields {
			value := map[string]any{
				"field_id": field.ID,
				"name":     field.Name,
				"type":     field.Type,
				"nullable": field.Nullable,
				"writable": field.Writable,
			}
			if field.Precision != nil {
				value["precision"] = *field.Precision
			}
			if field.Scale != nil {
				value["scale"] = *field.Scale
			}
			canonicalFields = append(canonicalFields, value)
		}

		indexes := append([]indexSchema(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool { return bytes.Compare([]byte(indexes[i].ID), []byte(indexes[j].ID)) < 0 })
		canonicalIndexes := make([]any, 0, len(indexes))
		for _, index := range indexes {
			fieldIDs := make([]any, len(index.FieldIDs))
			for i, fieldID := range index.FieldIDs {
				fieldIDs[i] = fieldID
			}
			canonicalIndexes = append(canonicalIndexes, map[string]any{
				"index_id":  index.ID,
				"name":      index.Name,
				"field_ids": fieldIDs,
				"unique":    index.Unique,
			})
		}

		canonicalTables = append(canonicalTables, map[string]any{
			"table_id":             table.ID,
			"relation_id":          table.RelationID,
			"name":                 table.Name,
			"primary_key_field_id": table.PrimaryKeyFieldID,
			"lifecycle": map[string]any{
				"created_at_field_id": nullableString(table.Lifecycle.CreatedAtFieldID),
				"updated_at_field_id": nullableString(table.Lifecycle.UpdatedAtFieldID),
				"deleted_at_field_id": nullableString(table.Lifecycle.DeletedAtFieldID),
			},
			"composition": table.Composition,
			"fields":      canonicalFields,
			"indexes":     canonicalIndexes,
		})
	}

	var parent any
	if manifest.ParentSchema != nil {
		parent = map[string]any{
			"version": manifest.ParentSchema.Version,
			"hash":    manifest.ParentSchema.Hash,
		}
	}
	return map[string]any{
		"schema_version":      manifest.SchemaVersion,
		"parent_schema":       parent,
		"transition_class":    manifest.TransitionClass,
		"compatibility_floor": manifest.CompatibilityFloor,
		"tables":              canonicalTables,
	}
}

func exportManifestHashBody(manifest portableSeedManifest) map[string]any {
	canonicalScopes := make([]any, 0, len(manifest.PortableScopes))
	for _, scope := range manifest.PortableScopes {
		canonicalScopes = append(canonicalScopes, map[string]any{
			"id":                    scope.ID,
			"registry_generation":   scope.RegistryGeneration,
			"membership_generation": scope.MembershipGeneration,
			"retention_generation":  scope.RetentionGeneration,
			"cardinality":           scope.Cardinality,
			"checksum":              checksumJSONValue(scope.Checksum),
		})
	}
	return map[string]any{
		"export_id":         manifest.ExportID,
		"schema_version":    manifest.SchemaVersion,
		"schema_hash":       manifest.SchemaHash,
		"stream_generation": manifest.StreamGeneration,
		"snapshot_boundary": snapshotBoundaryJSONValue(manifest.SnapshotBoundary),
		"page_limit":        manifest.PageLimit,
		"portable_scopes":   canonicalScopes,
	}
}

func checksumJSONValue(checksum checksumObject) map[string]any {
	return map[string]any{
		"algorithm": checksum.Algorithm,
		"version":   checksum.Version,
		"encoding":  checksum.Encoding,
		"digest":    checksum.Digest,
	}
}

func snapshotBoundaryJSONValue(boundary seedSnapshotBoundary) map[string]any {
	value := map[string]any{"position_kind": boundary.PositionKind}
	if boundary.CommitLSN != nil {
		value["commit_lsn"] = *boundary.CommitLSN
	}
	return value
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func verifyPortableSeedRecord(
	env manifestEnvelope,
	table localSchemaTable,
	record portableSeedRecord,
) (string, seedDigestRow, error) {
	if err := record.RowChecksum.validate(); err != nil {
		return "", seedDigestRow{}, fmt.Errorf("portable seed row checksum: %w", err)
	}
	if record.ServerVersion == "" {
		return "", seedDigestRow{}, errors.New("portable seed row has an empty server version")
	}
	if len(record.PK) != 1 || len(record.Row) != len(table.Columns) {
		return "", seedDigestRow{}, errors.New("portable seed row has an invalid field set")
	}

	columns := append([]localSchemaColumn(nil), table.Columns...)
	sortColumnsByFieldID(columns)
	var primary localSchemaColumn
	var primaryValue any
	rowBody := appendU32(nil, uint32(len(columns)))
	for _, column := range columns {
		value, exists := record.Row[column.FieldID]
		if !exists {
			return "", seedDigestRow{}, errors.New("portable seed row omits a manifest field")
		}
		encoded, err := encodeTypedValue(column.LogicalType, value, column.Nullable)
		if err != nil {
			return "", seedDigestRow{}, fmt.Errorf("portable seed row field %s: %w", column.FieldID, err)
		}
		rowBody = appendText(rowBody, column.FieldID)
		rowBody = append(rowBody, encoded...)
		if column.IsPrimaryKey {
			primary = column
			primaryValue = value
		}
	}
	if primary.FieldID == "" {
		return "", seedDigestRow{}, errors.New("portable seed table has no primary key field")
	}
	pkValue, exists := record.PK[primary.FieldID]
	if !exists {
		return "", seedDigestRow{}, errors.New("portable seed row primary key does not match the manifest")
	}
	encodedPK, err := encodeTypedValue(primary.LogicalType, pkValue, false)
	if err != nil {
		return "", seedDigestRow{}, fmt.Errorf("portable seed primary key: %w", err)
	}
	encodedRowPK, err := encodeTypedValue(primary.LogicalType, primaryValue, false)
	if err != nil || !bytes.Equal(encodedPK, encodedRowPK) {
		return "", seedDigestRow{}, errors.New("portable seed row primary key does not match its row")
	}

	identity := append([]byte(nil), rowIdentityDomain...)
	identity = appendText(identity, table.TableID)
	identity = appendText(identity, primary.FieldID)
	identity = append(identity, encodedPK...)
	schemaHash, err := decodeDigest(env.SchemaHash)
	if err != nil {
		return "", seedDigestRow{}, fmt.Errorf("schema hash: %w", err)
	}
	hasher := sha256.New()
	_, _ = hasher.Write(rowDigestDomain)
	_, _ = hasher.Write(schemaHash)
	writeBlob(hasher, identity)
	writeBlob(hasher, rowBody)
	writeText(hasher, record.ServerVersion)
	actual := hasher.Sum(nil)
	if hex.EncodeToString(actual) != record.RowChecksum.Digest {
		return "", seedDigestRow{}, errors.New("portable seed row digest does not match its row")
	}

	var digest [sha256.Size]byte
	copy(digest[:], actual)
	recordID, err := sqliteRecordID(primary.LogicalType, pkValue)
	if err != nil {
		return "", seedDigestRow{}, err
	}
	return recordID, seedDigestRow{identity: identity, digest: digest}, nil
}

func verifyScopeDigest(schemaHash, scopeID string, rows []seedDigestRow, expected checksumObject) error {
	schemaDigest, err := decodeDigest(schemaHash)
	if err != nil {
		return fmt.Errorf("schema hash: %w", err)
	}
	sortDigestRows(rows)
	for i := 1; i < len(rows); i++ {
		if bytes.Equal(rows[i-1].identity, rows[i].identity) {
			return errors.New("portable scope contains a duplicate row identity")
		}
	}
	hasher := sha256.New()
	_, _ = hasher.Write(scopeDigestDomain)
	_, _ = hasher.Write(schemaDigest)
	writeText(hasher, scopeID)
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(rows)))
	_, _ = hasher.Write(count[:])
	for _, row := range rows {
		writeBlob(hasher, row.identity)
		_, _ = hasher.Write(row.digest[:])
	}
	if hex.EncodeToString(hasher.Sum(nil)) != expected.Digest {
		return errors.New("portable scope digest does not match its rows")
	}
	return nil
}

func encodeTypedValue(logicalType string, value any, nullable bool) ([]byte, error) {
	tag, err := portableTypeTag(logicalType)
	if err != nil {
		return nil, err
	}
	encoded := []byte{tag}
	if value == nil {
		if !nullable {
			return nil, errors.New("non-null field is null")
		}
		return append(encoded, 0), nil
	}
	encoded = append(encoded, 1)

	switch logicalType {
	case "string":
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) {
			return nil, errors.New("invalid string")
		}
		return appendText(encoded, text), nil
	case "int":
		number, ok := value.(json.Number)
		if !ok || !canonicalInteger(string(number)) {
			return nil, errors.New("invalid int")
		}
		parsed, err := strconv.ParseInt(string(number), 10, 32)
		if err != nil {
			return nil, errors.New("int is out of range")
		}
		var payload [4]byte
		binary.BigEndian.PutUint32(payload[:], uint32(int32(parsed)))
		return append(encoded, payload[:]...), nil
	case "int64":
		text, ok := value.(string)
		if !ok || !canonicalInteger(text) {
			return nil, errors.New("invalid int64")
		}
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, errors.New("int64 is out of range")
		}
		var payload [8]byte
		binary.BigEndian.PutUint64(payload[:], uint64(parsed))
		return append(encoded, payload[:]...), nil
	case "decimal":
		text, ok := value.(string)
		if !ok || !canonicalDecimal(text) {
			return nil, errors.New("invalid decimal")
		}
		return appendBlob(encoded, []byte(text)), nil
	case "float":
		number, ok := value.(json.Number)
		if !ok {
			return nil, errors.New("invalid float")
		}
		canonical, err := canonicalFloatNumber(string(number))
		if err != nil || canonical != string(number) {
			return nil, errors.New("invalid float")
		}
		parsed, err := strconv.ParseFloat(canonical, 64)
		if err != nil {
			return nil, errors.New("invalid float")
		}
		if parsed == 0 {
			parsed = 0
		}
		var payload [8]byte
		binary.BigEndian.PutUint64(payload[:], math.Float64bits(parsed))
		return append(encoded, payload[:]...), nil
	case "boolean":
		boolean, ok := value.(bool)
		if !ok {
			return nil, errors.New("invalid boolean")
		}
		if boolean {
			return append(encoded, 1), nil
		}
		return append(encoded, 0), nil
	case "datetime":
		text, ok := value.(string)
		if !ok || !validDateTime(text) {
			return nil, errors.New("invalid datetime")
		}
		return appendBlob(encoded, []byte(text)), nil
	case "date":
		text, ok := value.(string)
		if !ok || !validDate(text) {
			return nil, errors.New("invalid date")
		}
		return appendBlob(encoded, []byte(text)), nil
	case "time":
		text, ok := value.(string)
		if !ok || !validTime(text) {
			return nil, errors.New("invalid time")
		}
		return appendBlob(encoded, []byte(text)), nil
	case "json":
		text, ok := value.(string)
		canonical, err := canonicalizeRFC8785JSON([]byte(text))
		if !ok || err != nil || string(canonical) != text {
			return nil, errors.New("invalid canonical JSON")
		}
		return appendBlob(encoded, []byte(text)), nil
	case "bytes":
		text, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid bytes")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(text)
		if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != text {
			return nil, errors.New("invalid base64url bytes")
		}
		return appendBlob(encoded, decoded), nil
	default:
		return nil, errors.New("unsupported portable type")
	}
}

func portableTypeTag(logicalType string) (byte, error) {
	tags := map[string]byte{
		"string": 0x01, "int": 0x02, "int64": 0x03, "decimal": 0x04,
		"float": 0x05, "boolean": 0x06, "datetime": 0x07, "date": 0x08,
		"time": 0x09, "json": 0x0a, "bytes": 0x0b,
	}
	tag, ok := tags[logicalType]
	if !ok {
		return 0, fmt.Errorf("unsupported portable type %q", logicalType)
	}
	return tag, nil
}

func sqliteRecordID(logicalType string, value any) (string, error) {
	switch logicalType {
	case "string", "int64":
		text, ok := value.(string)
		if !ok {
			return "", errors.New("invalid portable seed primary key")
		}
		return text, nil
	case "int":
		number, ok := value.(json.Number)
		if !ok {
			return "", errors.New("invalid portable seed primary key")
		}
		return string(number), nil
	default:
		return "", errors.New("unsupported portable seed primary key type")
	}
}

func decodeDigest(value string) ([]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
		return nil, errors.New("invalid SHA-256 digest")
	}
	return decoded, nil
}

func canonicalJSON(raw any) ([]byte, error) {
	var output []byte
	if err := appendCanonicalJSON(&output, raw); err != nil {
		return nil, err
	}
	return output, nil
}

func canonicalizeRFC8785JSON(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("JSON contains invalid UTF-8")
	}
	validator := json.NewDecoder(bytes.NewReader(raw))
	validator.UseNumber()
	if err := validateJSONValue(validator); err != nil {
		return nil, err
	}
	if _, err := validator.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("JSON contains more than one value")
		}
		return nil, fmt.Errorf("decoding trailing JSON: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return canonicalJSON(value)
}

func validateJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decoding JSON value: %w", err)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("decoding JSON member name: %w", err)
				}
				key, ok := keyToken.(string)
				if !ok || !utf8.ValidString(key) {
					return errors.New("JSON object has an invalid member name")
				}
				if _, exists := seen[key]; exists {
					return errors.New("JSON object has a duplicate member")
				}
				seen[key] = struct{}{}
				if err := validateJSONValue(decoder); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return errors.New("JSON object has an invalid closing delimiter")
			}
			return nil
		case '[':
			for decoder.More() {
				if err := validateJSONValue(decoder); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return errors.New("JSON array has an invalid closing delimiter")
			}
			return nil
		default:
			return errors.New("JSON contains an unexpected delimiter")
		}
	case json.Number:
		_, err := canonicalJSONNumber(value.String())
		return err
	case string:
		if !utf8.ValidString(value) {
			return errors.New("JSON string contains invalid UTF-8")
		}
		return nil
	case bool, nil:
		return nil
	default:
		return fmt.Errorf("JSON contains unsupported value %T", token)
	}
}

func appendCanonicalJSON(output *[]byte, value any) error {
	switch value := value.(type) {
	case nil:
		*output = append(*output, "null"...)
	case bool:
		if value {
			*output = append(*output, "true"...)
		} else {
			*output = append(*output, "false"...)
		}
	case string:
		if !utf8.ValidString(value) {
			return errors.New("JSON string contains invalid UTF-8")
		}
		appendCanonicalJSONString(output, value)
	case json.Number:
		encoded, err := canonicalJSONNumber(value.String())
		if err != nil {
			return err
		}
		*output = append(*output, encoded...)
	case int:
		*output = strconv.AppendInt(*output, int64(value), 10)
	case int32:
		*output = strconv.AppendInt(*output, int64(value), 10)
	case int64:
		*output = strconv.AppendInt(*output, value, 10)
	case uint:
		*output = strconv.AppendUint(*output, uint64(value), 10)
	case uint32:
		*output = strconv.AppendUint(*output, uint64(value), 10)
	case uint64:
		*output = strconv.AppendUint(*output, value, 10)
	case float64:
		encoded, err := canonicalJSONNumber(strconv.FormatFloat(value, 'g', -1, 64))
		if err != nil {
			return err
		}
		*output = append(*output, encoded...)
	case []any:
		*output = append(*output, '[')
		for index, element := range value {
			if index > 0 {
				*output = append(*output, ',')
			}
			if err := appendCanonicalJSON(output, element); err != nil {
				return err
			}
		}
		*output = append(*output, ']')
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			if !utf8.ValidString(key) {
				return errors.New("JSON object member name contains invalid UTF-8")
			}
			keys = append(keys, key)
		}
		sortJSONKeys(keys)
		*output = append(*output, '{')
		for index, key := range keys {
			if index > 0 {
				*output = append(*output, ',')
			}
			appendCanonicalJSONString(output, key)
			*output = append(*output, ':')
			if err := appendCanonicalJSON(output, value[key]); err != nil {
				return err
			}
		}
		*output = append(*output, '}')
	default:
		return fmt.Errorf("JSON value has unsupported type %T", value)
	}
	return nil
}

func canonicalJSONNumber(value string) (string, error) {
	return canonicalJSONNumberWithSafeInteger(value, true)
}

func canonicalFloatNumber(value string) (string, error) {
	return canonicalJSONNumberWithSafeInteger(value, false)
}

func canonicalJSONNumberWithSafeInteger(value string, safeInteger bool) (string, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return "", errors.New("JSON number is outside finite binary64")
	}
	if safeInteger && math.Trunc(parsed) == parsed && math.Abs(parsed) > maxSafeJSONInteger {
		return "", errors.New("JSON integer is outside the safe range")
	}
	if parsed == 0 {
		return "0", nil
	}
	encoded, err := json.Marshal(parsed)
	if err != nil {
		return "", fmt.Errorf("canonicalizing JSON number: %w", err)
	}
	return string(encoded), nil
}

func appendCanonicalJSONString(output *[]byte, value string) {
	*output = append(*output, '"')
	for _, character := range value {
		switch character {
		case '"':
			*output = append(*output, '\\', '"')
		case '\\':
			*output = append(*output, '\\', '\\')
		case '\b':
			*output = append(*output, '\\', 'b')
		case '\f':
			*output = append(*output, '\\', 'f')
		case '\n':
			*output = append(*output, '\\', 'n')
		case '\r':
			*output = append(*output, '\\', 'r')
		case '\t':
			*output = append(*output, '\\', 't')
		default:
			if character < 0x20 {
				*output = append(*output, '\\', 'u', '0', '0', "0123456789abcdef"[character>>4], "0123456789abcdef"[character&0x0f])
				continue
			}
			*output = append(*output, string(character)...)
		}
	}
	*output = append(*output, '"')
}

func sortJSONKeys(keys []string) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && compareUTF16(keys[j], keys[j-1]) < 0; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

func compareUTF16(left, right string) int {
	leftUnits := utf16.Encode([]rune(left))
	rightUnits := utf16.Encode([]rune(right))
	for index := 0; index < len(leftUnits) && index < len(rightUnits); index++ {
		if leftUnits[index] < rightUnits[index] {
			return -1
		}
		if leftUnits[index] > rightUnits[index] {
			return 1
		}
	}
	if len(leftUnits) < len(rightUnits) {
		return -1
	}
	if len(leftUnits) > len(rightUnits) {
		return 1
	}
	return 0
}

func isLowerUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range []byte(value) {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func canonicalInteger(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value == "-0" || value[0] == '+' {
		return false
	}
	digits := value
	if value[0] == '-' {
		digits = value[1:]
	}
	if digits == "" || digits[0] == '0' {
		return false
	}
	for _, char := range digits {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func canonicalDecimal(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value == "-0" || value[0] == '+' || strings.ContainsAny(value, "eE") {
		return false
	}
	unsigned := strings.TrimPrefix(value, "-")
	parts := strings.Split(unsigned, ".")
	if len(parts) > 2 || parts[0] == "" || (len(parts[0]) > 1 && parts[0][0] == '0') {
		return false
	}
	for _, char := range strings.Join(parts, "") {
		if char < '0' || char > '9' {
			return false
		}
	}
	if len(parts) == 2 && (parts[1] == "" || parts[1][len(parts[1])-1] == '0') {
		return false
	}
	return parts[0] != "0" || len(parts) == 2
}

func validDateTime(value string) bool {
	if len(value) != len("2006-01-02T15:04:05.000000Z") {
		return false
	}
	_, err := time.Parse("2006-01-02T15:04:05.000000Z", value)
	return err == nil
}

func validDate(value string) bool {
	if len(value) != len("2006-01-02") {
		return false
	}
	_, err := time.Parse("2006-01-02", value)
	return err == nil
}

func validTime(value string) bool {
	if len(value) != len("15:04:05.000000") {
		return false
	}
	_, err := time.Parse("15:04:05.000000", value)
	return err == nil
}

func appendBlob(destination, value []byte) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	destination = append(destination, length[:]...)
	return append(destination, value...)
}

func appendText(destination []byte, value string) []byte {
	return appendBlob(destination, []byte(value))
}

func appendU32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeBlob(writer byteWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func writeText(writer byteWriter, value string) {
	writeBlob(writer, []byte(value))
}

func sortColumnsByFieldID(columns []localSchemaColumn) {
	for i := 1; i < len(columns); i++ {
		for j := i; j > 0 && bytes.Compare([]byte(columns[j].FieldID), []byte(columns[j-1].FieldID)) < 0; j-- {
			columns[j], columns[j-1] = columns[j-1], columns[j]
		}
	}
}

func sortDigestRows(rows []seedDigestRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && bytes.Compare(rows[j].identity, rows[j-1].identity) < 0; j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}
