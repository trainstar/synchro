package seeddb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

var ErrOutputExists = errors.New("seed output already exists")

const (
	seedSnapshotIncomplete = "0"
	seedSnapshotComplete   = "1"
)

var clientCompatibleMigrationIdentifiers = []string{
	"synchro_v1",
	"synchro_v2_buckets",
	"synchro_v3_scopes",
	"synchro_v4_scope_integrity",
	"synchro_v5_rejected_mutations",
	"synchro_v6_protocol_3",
	"synchro_v7_pending_local_revision",
	"synchro_v8_sealed_push_batches",
}

const sqliteUserVersion = 6

type GenerateOptions struct {
	OutputPath string
	Overwrite  bool
}

type pgQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type pgReadTransaction struct {
	conn *sql.Conn
	done bool
}

func beginPGReadTransaction(ctx context.Context, db *sql.DB) (*pgReadTransaction, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring postgres export connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("starting postgres export transaction: %w", err)
	}
	return &pgReadTransaction{conn: conn}, nil
}

func (tx *pgReadTransaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *pgReadTransaction) Commit(ctx context.Context) error {
	if tx.done {
		return nil
	}
	if _, err := tx.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	tx.done = true
	return nil
}

func (tx *pgReadTransaction) Close(ctx context.Context) {
	if !tx.done {
		_, _ = tx.conn.ExecContext(ctx, "ROLLBACK")
	}
	_ = tx.conn.Close()
}

type manifestEnvelope struct {
	SchemaVersion int64          `json:"schema_version"`
	SchemaHash    string         `json:"schema_hash"`
	ServerTime    string         `json:"server_time"`
	Manifest      schemaManifest `json:"manifest"`
}

type schemaManifest struct {
	SchemaVersion      int64         `json:"schema_version"`
	SchemaHash         string        `json:"schema_hash"`
	ParentSchema       *schemaRef    `json:"parent_schema"`
	TransitionClass    string        `json:"transition_class"`
	CompatibilityFloor int64         `json:"compatibility_floor"`
	Tables             []tableSchema `json:"tables"`
}

type schemaRef struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
}

type tableSchema struct {
	Name              string          `json:"name"`
	ID                string          `json:"table_id"`
	RelationID        string          `json:"relation_id"`
	PrimaryKeyFieldID string          `json:"primary_key_field_id"`
	Lifecycle         lifecycleSchema `json:"lifecycle"`
	Composition       string          `json:"composition"`
	Fields            []fieldSchema   `json:"fields"`
	Indexes           []indexSchema   `json:"indexes"`
}

type lifecycleSchema struct {
	CreatedAtFieldID *string `json:"created_at_field_id"`
	UpdatedAtFieldID *string `json:"updated_at_field_id"`
	DeletedAtFieldID *string `json:"deleted_at_field_id"`
}

type fieldSchema struct {
	ID        string `json:"field_id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Writable  bool   `json:"writable"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

type indexSchema struct {
	ID       string   `json:"index_id"`
	Name     string   `json:"name"`
	FieldIDs []string `json:"field_ids"`
	Unique   bool     `json:"unique"`
}

type localSchemaTable struct {
	TableID           string              `json:"tableID"`
	RelationID        string              `json:"relationID"`
	TableName         string              `json:"tableName"`
	PrimaryKeyFieldID string              `json:"primaryKeyFieldID"`
	CreatedAtFieldID  *string             `json:"createdAtFieldID"`
	UpdatedAtFieldID  *string             `json:"updatedAtFieldID"`
	DeletedAtFieldID  *string             `json:"deletedAtFieldID"`
	UpdatedAtColumn   string              `json:"updatedAtColumn"`
	DeletedAtColumn   string              `json:"deletedAtColumn"`
	Composition       string              `json:"composition,omitempty"`
	PrimaryKey        []string            `json:"primaryKey"`
	Columns           []localSchemaColumn `json:"columns"`
}

type localSchemaColumn struct {
	FieldID          string  `json:"fieldID"`
	Name             string  `json:"name"`
	LogicalType      string  `json:"logicalType"`
	Nullable         bool    `json:"nullable"`
	Writable         bool    `json:"writable"`
	Precision        *int    `json:"precision"`
	Scale            *int    `json:"scale"`
	SQLiteDefaultSQL *string `json:"sqliteDefaultSQL"`
	IsPrimaryKey     bool    `json:"isPrimaryKey"`
}

type portableSeedManifest struct {
	ExportID           string               `json:"export_id"`
	ExportManifestHash string               `json:"export_manifest_hash"`
	SchemaVersion      int64                `json:"schema_version"`
	SchemaHash         string               `json:"schema_hash"`
	StreamGeneration   string               `json:"stream_generation"`
	SnapshotBoundary   seedSnapshotBoundary `json:"snapshot_boundary"`
	PageLimit          int64                `json:"page_limit"`
	PortableScopes     []portableSeedScope  `json:"portable_scopes"`
}

type seedSnapshotBoundary struct {
	PositionKind string  `json:"position_kind"`
	CommitLSN    *string `json:"commit_lsn,omitempty"`
}

type portableSeedScope struct {
	ID                   string         `json:"id"`
	RegistryGeneration   int64          `json:"registry_generation"`
	MembershipGeneration int64          `json:"membership_generation"`
	RetentionGeneration  int64          `json:"retention_generation"`
	Cardinality          int64          `json:"cardinality"`
	Checksum             checksumObject `json:"checksum"`
	Continuation         string         `json:"continuation"`
	PageToken            string         `json:"page_token"`
}

type portableSeedPage struct {
	Scope     string               `json:"scope"`
	Records   []portableSeedRecord `json:"records"`
	PageToken *string              `json:"page_token"`
	HasMore   bool                 `json:"has_more"`
}

type portableSeedRecord struct {
	Table         string         `json:"table"`
	PK            map[string]any `json:"pk"`
	RowChecksum   checksumObject `json:"row_checksum"`
	ServerVersion string         `json:"server_version"`
	Row           map[string]any `json:"row"`
}

type checksumObject struct {
	Algorithm string `json:"algorithm"`
	Version   int    `json:"version"`
	Encoding  string `json:"encoding"`
	Digest    string `json:"digest"`
}

func Generate(ctx context.Context, pg *sql.DB, opts GenerateOptions) error {
	if pg == nil {
		return errors.New("postgres database is required")
	}
	if strings.TrimSpace(opts.OutputPath) == "" {
		return errors.New("output path is required")
	}

	outputPath, err := prepareTemporaryOutput(opts.OutputPath, opts.Overwrite)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = os.Remove(outputPath)
			_ = removeSQLiteSidecars(outputPath)
		}
	}()

	pgTx, err := beginPGReadTransaction(ctx, pg)
	if err != nil {
		return err
	}
	defer pgTx.Close(ctx)

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
	if err := portable.validate(); err != nil {
		return err
	}

	localTables, err := env.localTables()
	if err != nil {
		return err
	}

	sqliteDB, err := sql.Open("sqlite", outputPath)
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
	if err := writePortableSeedData(ctx, pgTx, tx, env, localTables, portable); err != nil {
		return err
	}
	if err := createSyncedTableTriggers(ctx, tx, localTables); err != nil {
		return err
	}
	if err := writeMetadata(ctx, tx, env, localTables, portable, seedSnapshotIncomplete); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing sqlite seed: %w", err)
	}
	rollback = false

	if _, err := sqliteDB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("checkpointing sqlite seed: %w", err)
	}
	if err := sqliteDB.Close(); err != nil {
		return fmt.Errorf("closing sqlite seed output: %w", err)
	}
	sqliteDB = nil
	if err := removeSQLiteSidecars(outputPath); err != nil {
		return err
	}
	if err := verifySQLiteOutput(ctx, outputPath, env, localTables, portable, seedSnapshotIncomplete); err != nil {
		return err
	}
	if err := finalizeSQLiteOutput(ctx, outputPath); err != nil {
		return err
	}
	if err := verifySQLiteOutput(ctx, outputPath, env, localTables, portable, seedSnapshotComplete); err != nil {
		return err
	}
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("committing postgres export transaction: %w", err)
	}
	if err := publishOutput(outputPath, opts.OutputPath, opts.Overwrite); err != nil {
		return err
	}
	published = true

	return nil
}

func removeSQLiteSidecars(path string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		sidecar := path + suffix
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing sqlite sidecar %s: %w", sidecar, err)
		}
	}
	return nil
}

func loadManifest(ctx context.Context, pg pgQueryer) (manifestEnvelope, error) {
	var raw []byte
	if err := pg.QueryRowContext(ctx, "SELECT synchro.synchro_schema_manifest()").Scan(&raw); err != nil {
		return manifestEnvelope{}, fmt.Errorf("loading schema manifest: %w", err)
	}

	var env manifestEnvelope
	if err := decodeJSON(raw, &env); err != nil {
		return manifestEnvelope{}, fmt.Errorf("decoding schema manifest: %w", err)
	}
	return env, nil
}

func loadPortableSeedManifest(ctx context.Context, pg pgQueryer) (portableSeedManifest, error) {
	var raw []byte
	if err := pg.QueryRowContext(ctx, "SELECT synchro.synchro_portable_seed_manifest()").Scan(&raw); err != nil {
		return portableSeedManifest{}, fmt.Errorf("loading portable seed manifest: %w", err)
	}

	var manifest portableSeedManifest
	if err := decodeJSON(raw, &manifest); err != nil {
		return portableSeedManifest{}, fmt.Errorf("decoding portable seed manifest: %w", err)
	}
	return manifest, nil
}

func loadPortableSeedPage(
	ctx context.Context,
	pg pgQueryer,
	scopeID string,
	pageToken string,
	continuationReceipt string,
	expectedRowOrdinal int64,
	limit int,
) (portableSeedPage, error) {
	var raw []byte
	if err := pg.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)",
		scopeID,
		pageToken,
		continuationReceipt,
		expectedRowOrdinal,
		limit,
	).Scan(&raw); err != nil {
		return portableSeedPage{}, fmt.Errorf("loading portable seed page: %w", err)
	}

	var page portableSeedPage
	if err := decodeJSON(raw, &page); err != nil {
		return portableSeedPage{}, fmt.Errorf("decoding portable seed page: %w", err)
	}
	return page, nil
}

func decodeJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func positiveSafeInteger(value int64) bool {
	return value > 0 && value <= maxSafeJSONInteger
}

func decodeDigestError(value string) bool {
	_, err := decodeDigest(value)
	return err != nil
}

func (m manifestEnvelope) validate() error {
	if !positiveSafeInteger(m.SchemaVersion) {
		return fmt.Errorf("schema_version must be positive, got %d", m.SchemaVersion)
	}
	if _, err := decodeDigest(m.SchemaHash); err != nil {
		return fmt.Errorf("schema_hash: %w", err)
	}
	if m.Manifest.SchemaVersion != m.SchemaVersion || m.Manifest.SchemaHash != m.SchemaHash {
		return errors.New("schema manifest reference does not match its envelope")
	}
	if !positiveSafeInteger(m.Manifest.CompatibilityFloor) ||
		m.Manifest.CompatibilityFloor > m.SchemaVersion ||
		(m.Manifest.TransitionClass != "class_2" && m.Manifest.CompatibilityFloor != m.SchemaVersion) {
		return errors.New("schema manifest has an invalid compatibility_floor")
	}
	if m.Manifest.ParentSchema != nil &&
		(!positiveSafeInteger(m.Manifest.ParentSchema.Version) ||
			m.Manifest.ParentSchema.Version >= m.SchemaVersion ||
			decodeDigestError(m.Manifest.ParentSchema.Hash)) {
		return errors.New("schema manifest has an invalid parent_schema")
	}
	switch m.Manifest.TransitionClass {
	case "initial", "class_2", "class_3", "class_4":
	default:
		return errors.New("schema manifest has an invalid transition_class")
	}
	if (m.Manifest.TransitionClass == "initial") != (m.Manifest.ParentSchema == nil) {
		return errors.New("schema manifest parent_schema does not match its transition_class")
	}
	if m.Manifest.TransitionClass == "class_2" && m.Manifest.CompatibilityFloor > m.Manifest.ParentSchema.Version {
		return errors.New("Class 2 schema manifest has an invalid compatibility_floor")
	}
	if len(m.Manifest.Tables) == 0 {
		return errors.New("schema manifest contains no synced tables")
	}

	tableNames := make(map[string]struct{}, len(m.Manifest.Tables))
	tableIDs := make(map[string]struct{}, len(m.Manifest.Tables))
	relationIDs := make(map[string]struct{}, len(m.Manifest.Tables))
	for _, table := range m.Manifest.Tables {
		if table.Name == "" {
			return errors.New("schema manifest contains an empty table name")
		}
		if _, exists := tableNames[table.Name]; exists {
			return fmt.Errorf("schema manifest contains duplicate table %s", table.Name)
		}
		tableNames[table.Name] = struct{}{}
		if table.ID == "" {
			return fmt.Errorf("table %s has an empty table_id", table.Name)
		}
		if table.RelationID == "" {
			return fmt.Errorf("table %s has an empty relation_id", table.Name)
		}
		if _, exists := relationIDs[table.RelationID]; exists {
			return fmt.Errorf("schema manifest contains duplicate relation_id %s", table.RelationID)
		}
		if _, exists := tableIDs[table.ID]; exists {
			return fmt.Errorf("schema manifest contains duplicate table_id %s", table.ID)
		}
		tableIDs[table.ID] = struct{}{}
		relationIDs[table.RelationID] = struct{}{}

		if table.PrimaryKeyFieldID == "" {
			return fmt.Errorf("table %s has an empty primary_key_field_id", table.Name)
		}
		if len(table.Fields) == 0 {
			return fmt.Errorf("table %s has no fields", table.Name)
		}
		if table.Composition != "single_scope" && table.Composition != "multi_scope" {
			return fmt.Errorf("table %s has an invalid composition", table.Name)
		}

		fieldIDs := make(map[string]struct{}, len(table.Fields))
		fieldsByID := make(map[string]fieldSchema, len(table.Fields))
		fieldNames := make(map[string]struct{}, len(table.Fields))
		for _, field := range table.Fields {
			if field.ID == "" {
				return fmt.Errorf("table %s contains an empty field_id", table.Name)
			}
			if _, exists := fieldIDs[field.ID]; exists {
				return fmt.Errorf("table %s contains duplicate field_id %s", table.Name, field.ID)
			}
			fieldIDs[field.ID] = struct{}{}
			fieldsByID[field.ID] = field
			if field.Name == "" {
				return fmt.Errorf("table %s contains an empty field name", table.Name)
			}
			if _, exists := fieldNames[field.Name]; exists {
				return fmt.Errorf("table %s contains duplicate field %s", table.Name, field.Name)
			}
			fieldNames[field.Name] = struct{}{}
			if _, err := portableTypeTag(field.Type); err != nil {
				return fmt.Errorf("table %s field %s: %w", table.Name, field.ID, err)
			}
			if field.Type == "decimal" {
				if field.Precision == nil || field.Scale == nil {
					return fmt.Errorf("table %s field %s decimal metadata is incomplete", table.Name, field.ID)
				}
				if *field.Precision <= 0 || *field.Precision > maxSafeJSONInteger ||
					*field.Scale < 0 || *field.Scale > maxSafeJSONInteger || *field.Scale > *field.Precision {
					return fmt.Errorf("table %s field %s decimal metadata is invalid", table.Name, field.ID)
				}
			} else if field.Precision != nil || field.Scale != nil {
				return fmt.Errorf("table %s field %s has decimal metadata on a non-decimal type", table.Name, field.ID)
			}
		}

		if _, ok := fieldIDs[table.PrimaryKeyFieldID]; !ok {
			return fmt.Errorf("table %s primary key field %s is missing from fields", table.Name, table.PrimaryKeyFieldID)
		}
		for _, field := range table.Fields {
			if field.ID == table.PrimaryKeyFieldID {
				if field.Nullable || field.Writable {
					return fmt.Errorf("table %s primary key field is nullable or writable", table.Name)
				}
				if field.Type != "string" && field.Type != "int" && field.Type != "int64" {
					return fmt.Errorf("table %s primary key field has an unsupported type", table.Name)
				}
			}
		}
		for name, fieldID := range map[string]*string{
			"created_at": table.Lifecycle.CreatedAtFieldID,
			"updated_at": table.Lifecycle.UpdatedAtFieldID,
			"deleted_at": table.Lifecycle.DeletedAtFieldID,
		} {
			if fieldID != nil {
				if _, ok := fieldIDs[*fieldID]; !ok {
					return fmt.Errorf("table %s %s lifecycle field %s is missing from fields", table.Name, name, *fieldID)
				}
				field := fieldsByID[*fieldID]
				if field.Type != "datetime" || field.Writable {
					return fmt.Errorf("table %s %s lifecycle field %s is not a nonwritable datetime", table.Name, name, *fieldID)
				}
			}
		}
		indexIDs := make(map[string]struct{}, len(table.Indexes))
		indexNames := make(map[string]struct{}, len(table.Indexes))
		for _, index := range table.Indexes {
			if index.ID == "" || index.Name == "" || len(index.FieldIDs) == 0 {
				return fmt.Errorf("table %s contains an invalid index", table.Name)
			}
			if _, exists := indexIDs[index.ID]; exists {
				return fmt.Errorf("table %s contains duplicate index_id %s", table.Name, index.ID)
			}
			if _, exists := indexNames[index.Name]; exists {
				return fmt.Errorf("table %s contains duplicate index name %s", table.Name, index.Name)
			}
			indexIDs[index.ID] = struct{}{}
			indexNames[index.Name] = struct{}{}
			indexedFields := make(map[string]struct{}, len(index.FieldIDs))
			for _, fieldID := range index.FieldIDs {
				if _, exists := fieldIDs[fieldID]; !exists {
					return fmt.Errorf("table %s index %s references an unknown field", table.Name, index.ID)
				}
				if _, exists := indexedFields[fieldID]; exists {
					return fmt.Errorf("table %s index %s contains duplicate fields", table.Name, index.ID)
				}
				indexedFields[fieldID] = struct{}{}
			}
		}
	}
	actualHash, err := schemaManifestDigest(m.Manifest)
	if err != nil {
		return err
	}
	if actualHash != m.SchemaHash {
		return errors.New("schema manifest hash does not match its canonical body")
	}

	return nil
}

func (m portableSeedManifest) validate() error {
	if !isLowerUUID(m.ExportID) || strings.TrimSpace(m.StreamGeneration) == "" {
		return errors.New("portable seed manifest has an invalid export binding")
	}
	if _, err := decodeDigest(m.ExportManifestHash); err != nil {
		return fmt.Errorf("portable seed manifest export_manifest_hash: %w", err)
	}
	if !positiveSafeInteger(m.SchemaVersion) {
		return errors.New("portable seed manifest has an invalid schema_version")
	}
	if _, err := decodeDigest(m.SchemaHash); err != nil {
		return fmt.Errorf("portable seed manifest schema_hash: %w", err)
	}
	if !positiveSafeInteger(m.PageLimit) {
		return errors.New("portable seed manifest has an invalid page_limit")
	}
	if m.SnapshotBoundary.PositionKind == "generation_start" {
		if m.SnapshotBoundary.CommitLSN != nil {
			return errors.New("portable seed manifest has an invalid generation boundary")
		}
	} else if m.SnapshotBoundary.PositionKind != "transaction_end" || m.SnapshotBoundary.CommitLSN == nil || *m.SnapshotBoundary.CommitLSN == "" {
		return errors.New("portable seed manifest has an invalid transaction boundary")
	}
	seen := make(map[string]struct{}, len(m.PortableScopes))
	previousScopeID := ""
	for scopeIndex, scope := range m.PortableScopes {
		if strings.TrimSpace(scope.ID) == "" {
			return errors.New("portable seed manifest contains an empty scope id")
		}
		if _, exists := seen[scope.ID]; exists {
			return errors.New("portable seed manifest contains a duplicate scope id")
		}
		if scopeIndex > 0 && !strictlyOrderedID(previousScopeID, scope.ID) {
			return errors.New("portable seed manifest scopes are not in canonical order")
		}
		seen[scope.ID] = struct{}{}
		previousScopeID = scope.ID
		if !positiveSafeInteger(scope.RegistryGeneration) || !positiveSafeInteger(scope.MembershipGeneration) ||
			!positiveSafeInteger(scope.RetentionGeneration) || scope.Cardinality < 0 || scope.Cardinality > maxSafeJSONInteger {
			return errors.New("portable seed manifest contains invalid scope generations")
		}
		if scope.Continuation == "" {
			return errors.New("portable seed manifest contains an empty continuation receipt")
		}
		if scope.PageToken == "" {
			return errors.New("portable seed manifest contains an empty page token")
		}
		if err := scope.Checksum.validate(); err != nil {
			return fmt.Errorf("portable seed manifest checksum: %w", err)
		}
	}
	actualHash, err := exportManifestDigest(m)
	if err != nil {
		return err
	}
	if actualHash != m.ExportManifestHash {
		return errors.New("portable seed export manifest hash does not match its canonical body")
	}
	return nil
}

func strictlyOrderedID(previous, next string) bool {
	return bytes.Compare([]byte(previous), []byte(next)) < 0
}

func (m manifestEnvelope) localTables() ([]localSchemaTable, error) {
	tables := make([]localSchemaTable, 0, len(m.Manifest.Tables))
	for _, table := range m.Manifest.Tables {
		fieldsByID := make(map[string]fieldSchema, len(table.Fields))
		for _, field := range table.Fields {
			fieldsByID[field.ID] = field
		}
		pk := fieldsByID[table.PrimaryKeyFieldID].Name
		localColumns := make([]localSchemaColumn, 0, len(table.Fields))
		for _, field := range table.Fields {
			isPK := field.ID == table.PrimaryKeyFieldID
			localColumns = append(localColumns, localSchemaColumn{
				FieldID:          field.ID,
				Name:             field.Name,
				LogicalType:      field.Type,
				Nullable:         field.Nullable,
				Writable:         field.Writable,
				Precision:        field.Precision,
				Scale:            field.Scale,
				SQLiteDefaultSQL: nil,
				IsPrimaryKey:     isPK,
			})
		}
		tables = append(tables, localSchemaTable{
			TableID:           table.ID,
			RelationID:        table.RelationID,
			TableName:         table.Name,
			PrimaryKeyFieldID: table.PrimaryKeyFieldID,
			CreatedAtFieldID:  table.Lifecycle.CreatedAtFieldID,
			UpdatedAtFieldID:  table.Lifecycle.UpdatedAtFieldID,
			DeletedAtFieldID:  table.Lifecycle.DeletedAtFieldID,
			UpdatedAtColumn:   lifecycleFieldName(fieldsByID, table.Lifecycle.UpdatedAtFieldID),
			DeletedAtColumn:   lifecycleFieldName(fieldsByID, table.Lifecycle.DeletedAtFieldID),
			Composition:       table.Composition,
			PrimaryKey:        []string{pk},
			Columns:           localColumns,
		})
	}
	return tables, nil
}

func lifecycleFieldName(fields map[string]fieldSchema, fieldID *string) string {
	if fieldID == nil {
		return ""
	}
	return fields[*fieldID].Name
}

func prepareTemporaryOutput(path string, overwrite bool) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("creating output directory: %w", err)
	}
	if err := checkDestinationFamily(path, overwrite); err != nil {
		return "", err
	}

	temporary, err := os.CreateTemp(filepath.Dir(path), ".synchro-seed-*.db")
	if err != nil {
		return "", fmt.Errorf("creating temporary seed output: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return "", fmt.Errorf("closing temporary seed output: %w", err)
	}
	return temporaryPath, nil
}

func publishOutput(temporaryPath, outputPath string, overwrite bool) error {
	if err := checkDestinationFamily(outputPath, overwrite); err != nil {
		return err
	}
	if overwrite {
		if err := os.Rename(temporaryPath, outputPath); err != nil {
			return fmt.Errorf("publishing seed output: %w", err)
		}
		return nil
	}
	if err := os.Link(temporaryPath, outputPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrOutputExists
		}
		return fmt.Errorf("publishing seed output: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		_ = os.Remove(outputPath)
		return fmt.Errorf("removing temporary seed output: %w", err)
	}
	return nil
}

func checkDestinationFamily(path string, overwrite bool) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Lstat(path + suffix); err == nil {
			return fmt.Errorf("seed destination sidecar %s is present", suffix)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("checking seed destination sidecar %s: %w", suffix, err)
		}
	}
	if _, err := os.Stat(path); err == nil {
		if !overwrite {
			return ErrOutputExists
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking output path: %w", err)
	}
	return nil
}

func verifySQLiteOutput(
	ctx context.Context,
	path string,
	env manifestEnvelope,
	tables []localSchemaTable,
	portable portableSeedManifest,
	snapshotComplete string,
) error {
	if err := ensureSQLiteSidecarsAbsent(path); err != nil {
		return err
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&immutable=1"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("opening finalized sqlite seed: %w", err)
	}

	if err := verifySQLiteIntegrity(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := verifySQLiteSchema(ctx, db, tables); err != nil {
		_ = db.Close()
		return err
	}
	if err := verifySQLiteMetadata(ctx, db, env, tables, portable, snapshotComplete); err != nil {
		_ = db.Close()
		return err
	}
	if err := verifySQLiteQueues(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := verifySQLiteSeedState(ctx, db, env, tables, portable); err != nil {
		_ = db.Close()
		return err
	}
	if err := verifySQLiteMigrations(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("closing verified sqlite seed: %w", err)
	}
	if err := ensureSQLiteSidecarsAbsent(path); err != nil {
		return err
	}
	return nil
}

func finalizeSQLiteOutput(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("opening staged sqlite seed: %w", err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting sqlite finalization transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	result, err := tx.ExecContext(
		ctx,
		"UPDATE _synchro_meta SET value = ? WHERE key = 'snapshot_complete'",
		seedSnapshotComplete,
	)
	if err != nil {
		return fmt.Errorf("marking sqlite seed complete: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking sqlite seed completion update: %w", err)
	}
	if updated != 1 {
		return errors.New("sqlite seed completion metadata is missing or duplicated")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing sqlite seed finalization: %w", err)
	}
	rollback = false

	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("checkpointing finalized sqlite seed: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("closing finalized sqlite seed: %w", err)
	}
	if err := removeSQLiteSidecars(path); err != nil {
		return err
	}
	return nil
}

func ensureSQLiteSidecarsAbsent(path string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Lstat(path + suffix); err == nil {
			return fmt.Errorf("sqlite seed sidecar %s is present", suffix)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("checking sqlite seed sidecar %s: %w", suffix, err)
		}
	}
	return nil
}

func verifySQLiteIntegrity(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("checking sqlite seed integrity: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("reading sqlite seed integrity result: %w", err)
		}
		if result != "ok" {
			return errors.New("sqlite seed integrity check failed")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed integrity result: %w", err)
	}
	if count != 1 {
		return errors.New("sqlite seed integrity check failed")
	}
	return nil
}

func verifySQLiteQueues(ctx context.Context, db *sql.DB) error {
	for _, table := range []string{
		"_synchro_pending_changes",
		"_synchro_push_batches",
		"_synchro_rejected_mutations",
	} {
		var count int64
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdentifier(table)).Scan(&count); err != nil {
			return fmt.Errorf("checking sqlite seed %s: %w", table, err)
		}
		if count != 0 {
			return fmt.Errorf("sqlite seed %s is not empty", table)
		}
	}
	return nil
}

func verifySQLiteMigrations(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT identifier FROM grdb_migrations")
	if err != nil {
		return fmt.Errorf("reading sqlite seed migrations: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]struct{}, len(clientCompatibleMigrationIdentifiers))
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			return fmt.Errorf("reading sqlite seed migration: %w", err)
		}
		if _, exists := seen[identifier]; exists {
			return errors.New("sqlite seed has duplicate migration metadata")
		}
		seen[identifier] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed migrations: %w", err)
	}
	if len(seen) != len(clientCompatibleMigrationIdentifiers) {
		return errors.New("sqlite seed migration metadata is incomplete")
	}
	for _, identifier := range clientCompatibleMigrationIdentifiers {
		if _, exists := seen[identifier]; !exists {
			return errors.New("sqlite seed migration metadata is incomplete")
		}
	}
	return nil
}

func verifySQLiteSchema(ctx context.Context, db *sql.DB, tables []localSchemaTable) error {
	expectedTables := make(map[string]struct{})
	for _, spec := range portableSeedInternalTableSpecs() {
		expectedTables[spec.name] = struct{}{}
		if err := verifySQLiteInternalTable(ctx, db, spec); err != nil {
			return err
		}
	}
	for _, table := range tables {
		expectedTables[table.TableName] = struct{}{}
		if err := verifySQLiteTable(ctx, db, table); err != nil {
			return err
		}
	}
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return fmt.Errorf("reading sqlite seed schema: %w", err)
	}
	defer rows.Close()
	actualTables := make(map[string]struct{}, len(expectedTables))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("reading sqlite seed table name: %w", err)
		}
		actualTables[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed schema: %w", err)
	}
	if !sameStringSet(actualTables, expectedTables) {
		return errors.New("sqlite seed table schema does not match the manifest")
	}

	expectedTriggers := make(map[string]string, len(tables)*3)
	for _, table := range tables {
		statements := cdcTriggerSQL(table)
		for _, statement := range statements[3:] {
			name := triggerName(statement)
			expectedTriggers[name] = strings.TrimSpace(statement)
		}
	}
	triggerRows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")
	if err != nil {
		return fmt.Errorf("reading sqlite seed triggers: %w", err)
	}
	defer triggerRows.Close()
	actualTriggers := make(map[string]string, len(expectedTriggers))
	for triggerRows.Next() {
		var name, statement string
		if err := triggerRows.Scan(&name, &statement); err != nil {
			return fmt.Errorf("reading sqlite seed trigger: %w", err)
		}
		actualTriggers[name] = strings.TrimSpace(statement)
	}
	if err := triggerRows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed triggers: %w", err)
	}
	if len(actualTriggers) != len(expectedTriggers) {
		return errors.New("sqlite seed trigger set does not match the manifest")
	}
	for name, expected := range expectedTriggers {
		if actualTriggers[name] != expected {
			return errors.New("sqlite seed trigger definition does not match the manifest")
		}
	}
	return nil
}

func verifySQLiteTable(ctx context.Context, db *sql.DB, table localSchemaTable) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quoteIdentifier(table.TableName)+")")
	if err != nil {
		return fmt.Errorf("reading sqlite seed table %s: %w", table.TableName, err)
	}
	defer rows.Close()
	for index, expected := range table.Columns {
		if !rows.Next() {
			return fmt.Errorf("sqlite seed table %s is missing column %s", table.TableName, expected.Name)
		}
		var cid, notNull, primaryKey int
		var name, typeName string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("reading sqlite seed table %s column: %w", table.TableName, err)
		}
		if cid != index || name != expected.Name || typeName != sqliteType(expected.LogicalType) ||
			(notNull != 0) != (!expected.Nullable && !expected.IsPrimaryKey) ||
			(primaryKey != 0) != expected.IsPrimaryKey {
			return fmt.Errorf("sqlite seed table %s column %s does not match the manifest", table.TableName, expected.Name)
		}
	}
	if rows.Next() {
		return fmt.Errorf("sqlite seed table %s has an unexpected column", table.TableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed table %s: %w", table.TableName, err)
	}
	return nil
}

func triggerName(statement string) string {
	parts := strings.Fields(statement)
	if len(parts) < 3 {
		return ""
	}
	return strings.Trim(parts[2], `"`)
}

func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, exists := right[value]; !exists {
			return false
		}
	}
	return true
}

func verifySQLiteMetadata(
	ctx context.Context,
	db *sql.DB,
	env manifestEnvelope,
	tables []localSchemaTable,
	portable portableSeedManifest,
	snapshotComplete string,
) error {
	expected, err := seedMetadata(env, tables, portable, snapshotComplete)
	if err != nil {
		return err
	}
	expected["sync_lock"] = "0"
	expected["checkpoint"] = "0"
	rows, err := db.QueryContext(ctx, "SELECT key, value FROM _synchro_meta")
	if err != nil {
		return fmt.Errorf("reading sqlite seed metadata: %w", err)
	}
	defer rows.Close()
	actual := make(map[string]string, len(expected))
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("reading sqlite seed metadata row: %w", err)
		}
		actual[key] = value
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed metadata: %w", err)
	}
	if len(actual) != len(expected) {
		return errors.New("sqlite seed metadata is incomplete")
	}
	for key, value := range expected {
		if actual[key] != value {
			return fmt.Errorf("sqlite seed metadata %s does not match the export", key)
		}
	}
	return nil
}

type localSeedRow struct {
	tableName string
	recordID  string
	digest    seedDigestRow
	checksum  checksumObject
}

func verifySQLiteSeedState(
	ctx context.Context,
	db *sql.DB,
	env manifestEnvelope,
	tables []localSchemaTable,
	portable portableSeedManifest,
) error {
	if err := verifySQLiteScopes(ctx, db, env, portable); err != nil {
		return err
	}
	versions, err := loadSQLiteRowVersions(ctx, db, tables)
	if err != nil {
		return err
	}
	localRows, err := loadSQLiteSeedRows(ctx, db, env, tables, versions)
	if err != nil {
		return err
	}
	if len(localRows) != len(versions) {
		return errors.New("sqlite seed row-version provenance is incomplete")
	}
	for key := range versions {
		if _, exists := localRows[key]; !exists {
			return errors.New("sqlite seed row-version provenance references an absent row")
		}
	}
	return verifySQLiteScopeRows(ctx, db, env.SchemaHash, portable, localRows)
}

func verifySQLiteScopes(ctx context.Context, db *sql.DB, env manifestEnvelope, portable portableSeedManifest) error {
	expected := make(map[string]portableSeedScope, len(portable.PortableScopes))
	for _, scope := range portable.PortableScopes {
		expected[scope.ID] = scope
	}
	rows, err := db.QueryContext(ctx, "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes")
	if err != nil {
		return fmt.Errorf("reading sqlite seed scopes: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]struct{}, len(expected))
	for rows.Next() {
		var scopeID, checksum, localChecksum string
		var cursor sql.NullString
		var generation int64
		if err := rows.Scan(&scopeID, &cursor, &checksum, &generation, &localChecksum); err != nil {
			return fmt.Errorf("reading sqlite seed scope: %w", err)
		}
		scope, exists := expected[scopeID]
		if !exists || cursor.Valid || generation != 0 || checksum != localChecksum {
			return errors.New("sqlite seed scope state does not match the export")
		}
		var stored checksumObject
		if err := decodeJSON([]byte(checksum), &stored); err != nil || stored != scope.Checksum {
			return errors.New("sqlite seed scope checksum does not match the export")
		}
		seen[scopeID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed scopes: %w", err)
	}
	if len(seen) != len(expected) {
		return errors.New("sqlite seed scope set is incomplete")
	}

	receipts, err := db.QueryContext(ctx, `SELECT scope_id, receipt, schema_version, schema_hash, cardinality, checksum
		FROM _synchro_seed_receipts`)
	if err != nil {
		return fmt.Errorf("reading sqlite seed receipts: %w", err)
	}
	defer receipts.Close()
	receiptCount := 0
	for receipts.Next() {
		var scopeID, receipt, schemaHash, checksum string
		var schemaVersion, cardinality int64
		if err := receipts.Scan(&scopeID, &receipt, &schemaVersion, &schemaHash, &cardinality, &checksum); err != nil {
			return fmt.Errorf("reading sqlite seed receipt: %w", err)
		}
		scope, exists := expected[scopeID]
		if !exists || receipt != scope.Continuation || schemaVersion != env.SchemaVersion || schemaHash != env.SchemaHash || cardinality != scope.Cardinality {
			return errors.New("sqlite seed receipt does not match the export")
		}
		var stored checksumObject
		if err := decodeJSON([]byte(checksum), &stored); err != nil || stored != scope.Checksum {
			return errors.New("sqlite seed receipt checksum does not match the export")
		}
		receiptCount++
	}
	if err := receipts.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed receipts: %w", err)
	}
	if receiptCount != len(expected) {
		return errors.New("sqlite seed receipts are incomplete")
	}
	return nil
}

func loadSQLiteRowVersions(ctx context.Context, db *sql.DB, tables []localSchemaTable) (map[string]seedRowVersion, error) {
	tableNames := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		tableNames[table.TableName] = struct{}{}
	}
	rows, err := db.QueryContext(ctx, "SELECT table_name, record_id, server_version, row_checksum FROM _synchro_row_versions")
	if err != nil {
		return nil, fmt.Errorf("reading sqlite seed row versions: %w", err)
	}
	defer rows.Close()
	versions := make(map[string]seedRowVersion)
	for rows.Next() {
		var tableName, recordID, serverVersion, rawChecksum string
		if err := rows.Scan(&tableName, &recordID, &serverVersion, &rawChecksum); err != nil {
			return nil, fmt.Errorf("reading sqlite seed row version: %w", err)
		}
		if _, exists := tableNames[tableName]; !exists || serverVersion == "" {
			return nil, errors.New("sqlite seed row-version provenance is invalid")
		}
		var checksum checksumObject
		if err := decodeJSON([]byte(rawChecksum), &checksum); err != nil || checksum.validate() != nil {
			return nil, errors.New("sqlite seed row-version checksum is invalid")
		}
		key := sqliteSeedRowKey(tableName, recordID)
		if _, exists := versions[key]; exists {
			return nil, errors.New("sqlite seed row-version provenance has a duplicate")
		}
		versions[key] = seedRowVersion{serverVersion: serverVersion, checksum: checksum}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating sqlite seed row versions: %w", err)
	}
	return versions, nil
}

type seedRowVersion struct {
	serverVersion string
	checksum      checksumObject
}

func loadSQLiteSeedRows(
	ctx context.Context,
	db *sql.DB,
	env manifestEnvelope,
	tables []localSchemaTable,
	versions map[string]seedRowVersion,
) (map[string]localSeedRow, error) {
	localRows := make(map[string]localSeedRow, len(versions))
	for _, table := range tables {
		columns := make([]string, len(table.Columns))
		for index, column := range table.Columns {
			columns[index] = quoteIdentifier(column.Name)
		}
		rows, err := db.QueryContext(ctx, "SELECT "+strings.Join(columns, ", ")+" FROM "+quoteIdentifier(table.TableName))
		if err != nil {
			return nil, fmt.Errorf("reading sqlite seed rows for %s: %w", table.TableName, err)
		}
		for rows.Next() {
			values := make([]any, len(table.Columns))
			destinations := make([]any, len(values))
			for index := range values {
				destinations[index] = &values[index]
			}
			if err := rows.Scan(destinations...); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("reading sqlite seed row for %s: %w", table.TableName, err)
			}
			row, primaryValue, err := sqliteRowWireValues(table, values)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			recordID, err := sqliteRecordID(primaryKeyLogicalType(table), primaryValue)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			key := sqliteSeedRowKey(table.TableName, recordID)
			version, exists := versions[key]
			if !exists {
				_ = rows.Close()
				return nil, errors.New("sqlite seed row has no verified server version")
			}
			record := portableSeedRecord{
				Table:         table.TableID,
				PK:            map[string]any{primaryKeyFieldID(table): primaryValue},
				RowChecksum:   version.checksum,
				ServerVersion: version.serverVersion,
				Row:           row,
			}
			verifiedRecordID, digest, err := verifyPortableSeedRecord(env, table, record)
			if err != nil || verifiedRecordID != recordID {
				_ = rows.Close()
				return nil, errors.New("sqlite seed row digest does not match its provenance")
			}
			if _, exists := localRows[key]; exists {
				_ = rows.Close()
				return nil, errors.New("sqlite seed has duplicate materialized rows")
			}
			localRows[key] = localSeedRow{tableName: table.TableName, recordID: recordID, digest: digest, checksum: version.checksum}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterating sqlite seed rows for %s: %w", table.TableName, err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("closing sqlite seed rows for %s: %w", table.TableName, err)
		}
	}
	return localRows, nil
}

func sqliteRowWireValues(table localSchemaTable, values []any) (map[string]any, any, error) {
	if len(values) != len(table.Columns) {
		return nil, nil, errors.New("sqlite seed row has an invalid column count")
	}
	row := make(map[string]any, len(table.Columns))
	var primaryValue any
	for index, column := range table.Columns {
		value, err := sqliteWireValue(column, values[index])
		if err != nil {
			return nil, nil, fmt.Errorf("reading sqlite seed field %s: %w", column.FieldID, err)
		}
		row[column.FieldID] = value
		if column.IsPrimaryKey {
			primaryValue = value
		}
	}
	return row, primaryValue, nil
}

func sqliteWireValue(column localSchemaColumn, value any) (any, error) {
	if value == nil {
		if !column.Nullable {
			return nil, errors.New("non-null field is null")
		}
		return nil, nil
	}
	switch column.LogicalType {
	case "string", "decimal", "datetime", "date", "time", "json":
		text, ok := sqliteText(value)
		if !ok {
			return nil, errors.New("field is not text")
		}
		return text, nil
	case "int":
		integer, ok := sqliteInteger(value)
		if !ok || integer < math.MinInt32 || integer > math.MaxInt32 {
			return nil, errors.New("field is not an int32")
		}
		return json.Number(strconv.FormatInt(integer, 10)), nil
	case "int64":
		integer, ok := sqliteInteger(value)
		if !ok {
			return nil, errors.New("field is not an int64")
		}
		return strconv.FormatInt(integer, 10), nil
	case "float":
		floatValue, ok := sqliteFloat(value)
		if !ok || math.IsInf(floatValue, 0) || math.IsNaN(floatValue) {
			return nil, errors.New("field is not a finite float")
		}
		canonical, err := canonicalFloatNumber(strconv.FormatFloat(floatValue, 'g', -1, 64))
		if err != nil {
			return nil, err
		}
		return json.Number(canonical), nil
	case "boolean":
		integer, ok := sqliteInteger(value)
		if !ok || (integer != 0 && integer != 1) {
			return nil, errors.New("field is not a boolean")
		}
		return integer == 1, nil
	case "bytes":
		bytes, ok := value.([]byte)
		if !ok {
			return nil, errors.New("field is not bytes")
		}
		return base64.RawURLEncoding.EncodeToString(bytes), nil
	default:
		return nil, errors.New("field has an unsupported portable type")
	}
}

func sqliteText(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

func sqliteInteger(value any) (int64, bool) {
	switch value := value.(type) {
	case int64:
		return value, true
	case int:
		return int64(value), true
	default:
		return 0, false
	}
}

func sqliteFloat(value any) (float64, bool) {
	switch value := value.(type) {
	case float64:
		return value, true
	case int64:
		return float64(value), true
	default:
		return 0, false
	}
}

func primaryKeyLogicalType(table localSchemaTable) string {
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			return column.LogicalType
		}
	}
	return ""
}

func primaryKeyFieldID(table localSchemaTable) string {
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			return column.FieldID
		}
	}
	return ""
}

func sqliteSeedRowKey(tableName, recordID string) string {
	return tableName + "\x00" + recordID
}

func verifySQLiteScopeRows(
	ctx context.Context,
	db *sql.DB,
	schemaHash string,
	portable portableSeedManifest,
	localRows map[string]localSeedRow,
) error {
	expectedScopes := make(map[string]portableSeedScope, len(portable.PortableScopes))
	for _, scope := range portable.PortableScopes {
		expectedScopes[scope.ID] = scope
	}
	rows, err := db.QueryContext(ctx, "SELECT scope_id, table_name, record_id, checksum, generation FROM _synchro_scope_rows")
	if err != nil {
		return fmt.Errorf("reading sqlite seed scope rows: %w", err)
	}
	defer rows.Close()
	byScope := make(map[string][]seedDigestRow, len(expectedScopes))
	seenRows := make(map[string]struct{}, len(localRows))
	seenScopeRows := make(map[string]struct{})
	for rows.Next() {
		var scopeID, tableName, recordID, checksum string
		var generation int64
		if err := rows.Scan(&scopeID, &tableName, &recordID, &checksum, &generation); err != nil {
			return fmt.Errorf("reading sqlite seed scope row: %w", err)
		}
		if _, exists := expectedScopes[scopeID]; !exists || generation != 0 {
			return errors.New("sqlite seed scope row has an unknown scope")
		}
		key := sqliteSeedRowKey(tableName, recordID)
		local, exists := localRows[key]
		if !exists || local.checksum.Digest != checksum {
			return errors.New("sqlite seed scope row does not match a verified row")
		}
		scopeKey := scopeID + "\x00" + key
		if _, exists := seenScopeRows[scopeKey]; exists {
			return errors.New("sqlite seed has duplicate scope rows")
		}
		seenScopeRows[scopeKey] = struct{}{}
		seenRows[key] = struct{}{}
		byScope[scopeID] = append(byScope[scopeID], local.digest)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating sqlite seed scope rows: %w", err)
	}
	if len(seenRows) != len(localRows) {
		return errors.New("sqlite seed has a row without scope provenance")
	}
	for scopeID, scope := range expectedScopes {
		digestRows := byScope[scopeID]
		if int64(len(digestRows)) != scope.Cardinality {
			return errors.New("sqlite seed scope cardinality does not match the export")
		}
		if err := verifyScopeDigest(schemaHash, scopeID, digestRows, scope.Checksum); err != nil {
			return err
		}
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
			local_revision INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (table_name, record_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS grdb_migrations (
			identifier TEXT NOT NULL PRIMARY KEY
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_scopes (
			scope_id TEXT PRIMARY KEY,
			cursor TEXT,
			checksum TEXT,
			generation INTEGER NOT NULL DEFAULT 0,
			local_checksum TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_scope_rows (
			scope_id TEXT NOT NULL,
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			checksum TEXT NOT NULL,
			generation INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (scope_id, table_name, record_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_synchro_scope_rows_record
			ON _synchro_scope_rows (table_name, record_id)`,
		`CREATE TABLE IF NOT EXISTS _synchro_seed_receipts (
			scope_id TEXT PRIMARY KEY,
			receipt TEXT NOT NULL,
			schema_version INTEGER NOT NULL,
			schema_hash TEXT NOT NULL,
			cardinality INTEGER NOT NULL,
			checksum TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_row_versions (
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			server_version TEXT NOT NULL,
			row_checksum TEXT,
			PRIMARY KEY (table_name, record_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_rebuild_attempts (
			scope_id TEXT PRIMARY KEY,
			rebuild_id TEXT NOT NULL,
			client_generation INTEGER NOT NULL,
			schema_version INTEGER NOT NULL,
			schema_hash TEXT NOT NULL,
			generation INTEGER NOT NULL,
			cursor TEXT,
			page_limit INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_push_batches (
			batch_id TEXT PRIMARY KEY,
			request_json TEXT NOT NULL,
			pending_json TEXT NOT NULL,
			schema_json TEXT NOT NULL,
			state TEXT NOT NULL,
			created_at TEXT NOT NULL,
			completed_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS _synchro_rejected_mutations (
			mutation_id TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			record_id TEXT NOT NULL,
			status TEXT NOT NULL,
			code TEXT NOT NULL,
			message TEXT,
			server_row_json TEXT,
			server_version TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_synchro_rejected_mutations_record
			ON _synchro_rejected_mutations (table_name, record_id)`,
		`INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('sync_lock', '0')`,
		`INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('checkpoint', '0')`,
		fmt.Sprintf("PRAGMA user_version = %d", sqliteUserVersion),
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("creating sqlite internals: %w", err)
		}
	}

	for _, identifier := range clientCompatibleMigrationIdentifiers {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO grdb_migrations (identifier) VALUES (?)`,
			identifier,
		); err != nil {
			return fmt.Errorf("recording client migration %s: %w", identifier, err)
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
	env manifestEnvelope,
	tables []localSchemaTable,
	manifest portableSeedManifest,
) error {
	tableIndex := make(map[string]localSchemaTable, len(tables))
	for _, table := range tables {
		tableIndex[table.TableID] = table
	}

	for _, scope := range manifest.PortableScopes {
		if strings.TrimSpace(scope.ID) == "" {
			return errors.New("portable seed manifest contains an empty scope id")
		}
		if err := scope.Checksum.validate(); err != nil {
			return fmt.Errorf("portable scope checksum: %w", err)
		}
		if scope.Continuation == "" {
			return errors.New("portable scope has an empty continuation receipt")
		}
		checksumJSON, err := json.Marshal(scope.Checksum)
		if err != nil {
			return fmt.Errorf("encoding portable scope checksum: %w", err)
		}
		generation := int64(0)
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation, local_checksum) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT (scope_id) DO UPDATE SET
			    cursor = excluded.cursor,
			    checksum = excluded.checksum,
			    generation = excluded.generation,
			    local_checksum = excluded.local_checksum`,
			scope.ID,
			nil,
			string(checksumJSON),
			generation,
			string(checksumJSON),
		); err != nil {
			return fmt.Errorf("writing portable scope: %w", err)
		}

		pageToken := scope.PageToken
		seenPageTokens := map[string]struct{}{pageToken: {}}
		observed := int64(0)
		expectedOrdinal := int64(0)
		digestRows := make([]seedDigestRow, 0)
		var previousIdentity []byte
		for {
			page, err := loadPortableSeedPage(
				ctx,
				pg,
				scope.ID,
				pageToken,
				scope.Continuation,
				expectedOrdinal,
				int(manifest.PageLimit),
			)
			if err != nil {
				return err
			}
			if page.Scope != scope.ID {
				return errors.New("portable seed page scope does not match its manifest entry")
			}
			for _, record := range page.Records {
				table, ok := tableIndex[record.Table]
				if !ok {
					return fmt.Errorf("portable seed record references unknown table %s", record.Table)
				}
				recordID, digestRow, err := verifyPortableSeedRecord(env, table, record)
				if err != nil {
					return err
				}
				if previousIdentity != nil && bytes.Compare(previousIdentity, digestRow.identity) >= 0 {
					return errors.New("portable seed records are not in strict row identity order")
				}
				previousIdentity = append(previousIdentity[:0], digestRow.identity...)
				if err := upsertPortableRecord(ctx, tx, table, record); err != nil {
					return err
				}
				rowChecksumJSON, err := json.Marshal(record.RowChecksum)
				if err != nil {
					return fmt.Errorf("encoding portable row checksum: %w", err)
				}
				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO _synchro_row_versions (table_name, record_id, server_version, row_checksum)
					 VALUES (?, ?, ?, ?)
					 ON CONFLICT (table_name, record_id) DO UPDATE SET
					    server_version = excluded.server_version,
					    row_checksum = excluded.row_checksum`,
					table.TableName,
					recordID,
					record.ServerVersion,
					string(rowChecksumJSON),
				); err != nil {
					return fmt.Errorf("writing portable row version: %w", err)
				}
				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, checksum, generation)
					 VALUES (?, ?, ?, ?, ?)
					 ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET
					    checksum = excluded.checksum,
					    generation = excluded.generation`,
					scope.ID,
					table.TableName,
					recordID,
					record.RowChecksum.Digest,
					generation,
				); err != nil {
					return fmt.Errorf("writing portable scope row %s/%s/%s: %w", scope.ID, record.Table, recordID, err)
				}
				digestRows = append(digestRows, digestRow)
				observed++
				expectedOrdinal++
			}

			if !page.HasMore {
				if page.PageToken != nil {
					return errors.New("portable seed terminal page has a page token")
				}
				break
			}
			if page.PageToken == nil || *page.PageToken == "" {
				return errors.New("portable seed page returned has_more without a page token")
			}
			pageToken = *page.PageToken
			if _, exists := seenPageTokens[pageToken]; exists {
				return errors.New("portable seed page repeated a page token")
			}
			seenPageTokens[pageToken] = struct{}{}
		}
		if observed != scope.Cardinality {
			return errors.New("portable seed scope cardinality does not match its manifest entry")
		}
		if err := verifyScopeDigest(env.SchemaHash, scope.ID, digestRows, scope.Checksum); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _synchro_seed_receipts
			 (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			scope.ID,
			scope.Continuation,
			env.SchemaVersion,
			env.SchemaHash,
			observed,
			string(checksumJSON),
		); err != nil {
			return fmt.Errorf("writing portable scope receipt: %w", err)
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
		value, err := sqliteValue(column, record.Row[column.FieldID])
		if err != nil {
			return fmt.Errorf("converting portable value for field %s: %w", column.FieldID, err)
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

func sqliteValue(column localSchemaColumn, raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}

	switch column.LogicalType {
	case "string", "decimal", "datetime", "date", "time":
		value, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("unsupported text value %T", raw)
		}
		return value, nil
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
		value, ok := raw.(bool)
		if ok {
			if value {
				return int64(1), nil
			}
			return int64(0), nil
		}
		return nil, fmt.Errorf("unsupported boolean value %T", raw)
	case "int", "int64":
		value, ok := raw.(json.Number)
		if ok {
			return value.Int64()
		}
		return nil, fmt.Errorf("unsupported integer value %T", raw)
	case "float":
		value, ok := raw.(json.Number)
		if ok {
			return value.Float64()
		}
		return nil, fmt.Errorf("unsupported float value %T", raw)
	case "bytes":
		value, ok := raw.(string)
		if ok {
			decoded, err := base64.RawURLEncoding.DecodeString(value)
			if err != nil {
				return nil, fmt.Errorf("decoding bytes: %w", err)
			}
			return decoded, nil
		}
		return nil, fmt.Errorf("unsupported bytes value %T", raw)
	default:
		return nil, fmt.Errorf("unsupported logical type %q", column.LogicalType)
	}
}

func writeMetadata(
	ctx context.Context,
	tx *sql.Tx,
	env manifestEnvelope,
	tables []localSchemaTable,
	portable portableSeedManifest,
	snapshotComplete string,
) error {
	meta, err := seedMetadata(env, tables, portable, snapshotComplete)
	if err != nil {
		return err
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

func seedMetadata(
	env manifestEnvelope,
	tables []localSchemaTable,
	portable portableSeedManifest,
	snapshotComplete string,
) (map[string]string, error) {
	localSchemaJSON, err := json.Marshal(tables)
	if err != nil {
		return nil, fmt.Errorf("encoding local schema: %w", err)
	}
	schemaManifestJSON, err := json.Marshal(env.Manifest)
	if err != nil {
		return nil, fmt.Errorf("encoding schema manifest: %w", err)
	}
	boundaryJSON, err := canonicalJSON(snapshotBoundaryJSONValue(portable.SnapshotBoundary))
	if err != nil {
		return nil, fmt.Errorf("encoding portable seed boundary: %w", err)
	}
	return map[string]string{
		"schema_version":            fmt.Sprintf("%d", env.SchemaVersion),
		"schema_hash":               env.SchemaHash,
		"local_schema":              string(localSchemaJSON),
		"schema_manifest":           string(schemaManifestJSON),
		"scope_set_version":         "0",
		"snapshot_complete":         snapshotComplete,
		"seed_export_id":            portable.ExportID,
		"seed_export_manifest_hash": portable.ExportManifestHash,
		"seed_stream_generation":    portable.StreamGeneration,
		"seed_snapshot_boundary":    string(boundaryJSON),
		"seed_page_limit":           strconv.FormatInt(portable.PageLimit, 10),
	}, nil
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

// cdcTriggerSQL builds the seed capture triggers.
//
// A trigger body is stored in the database schema and is parsed again by every
// client that opens the seed. SQLite adds UPSERT in 3.24 and Android API 24
// ships SQLite 3.9, so an "ON CONFLICT ... DO UPDATE" body makes the whole seed
// schema unreadable on the minimum supported cell. Each body therefore updates
// the conflicting row first and inserts only when no row exists.
func cdcTriggerSQL(table localSchemaTable) []string {
	name := table.TableName
	pkColumn := table.PrimaryKey[0]
	lockCheck := "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'"
	nowExpr := "substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'"
	hasDeletedAt := strings.TrimSpace(table.DeletedAtColumn) != ""
	escapedName := escapeSQLString(name)
	insertTrigger := quoteIdentifier("_synchro_cdc_insert_" + name)
	updateTrigger := quoteIdentifier("_synchro_cdc_update_" + name)
	deleteTrigger := quoteIdentifier("_synchro_cdc_delete_" + name)
	quotedTable := quoteIdentifier(name)
	quotedPK := quoteIdentifier(pkColumn)
	quotedDeletedAt := quoteIdentifier(table.DeletedAtColumn)

	baseUpdatedAtExpr := fmt.Sprintf(
		"(SELECT server_version FROM _synchro_row_versions WHERE table_name = '%s' AND record_id = OLD.%s)",
		escapedName,
		quotedPK,
	)

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
	UPDATE _synchro_pending_changes SET
		operation = 'delete',
		base_updated_at = COALESCE(base_updated_at, %s),
		local_revision = local_revision + 1,
		client_updated_at = %s
	WHERE table_name = '%s' AND record_id = OLD.%s;
	INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
	SELECT OLD.%s, '%s', 'delete', %s, %s
	WHERE NOT EXISTS (
		SELECT 1 FROM _synchro_pending_changes
		WHERE table_name = '%s' AND record_id = OLD.%s
	);
END`, deleteTrigger, quotedTable, lockCheck,
			baseUpdatedAtExpr, nowExpr, escapedName, quotedPK,
			quotedPK, escapedName, baseUpdatedAtExpr, nowExpr,
			escapedName, quotedPK)
	}

	return []string{
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s", insertTrigger),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s", updateTrigger),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s", deleteTrigger),
		fmt.Sprintf(`CREATE TRIGGER %s
AFTER INSERT ON %s
WHEN %s
BEGIN
	UPDATE _synchro_pending_changes SET
		operation = CASE
			WHEN operation = 'delete' THEN 'update'
			ELSE operation
		END,
		local_revision = local_revision + 1,
		client_updated_at = %s
	WHERE table_name = '%s' AND record_id = NEW.%s;
	INSERT INTO _synchro_pending_changes (record_id, table_name, operation, client_updated_at)
	SELECT NEW.%s, '%s', 'create', %s
	WHERE NOT EXISTS (
		SELECT 1 FROM _synchro_pending_changes
		WHERE table_name = '%s' AND record_id = NEW.%s
	);
END`, insertTrigger, quotedTable, lockCheck,
			nowExpr, escapedName, quotedPK,
			quotedPK, escapedName, nowExpr,
			escapedName, quotedPK),
		fmt.Sprintf(`CREATE TRIGGER %s
AFTER UPDATE ON %s
WHEN %s
BEGIN
	UPDATE _synchro_pending_changes SET
		operation = CASE
			WHEN operation = 'create' AND (%s) = 'update' THEN 'create'
			WHEN operation = 'create' AND (%s) = 'delete' THEN 'delete'
			ELSE (%s)
		END,
		base_updated_at = CASE
			WHEN operation = 'create' AND (%s) = 'delete' THEN NULL
			ELSE COALESCE(base_updated_at, %s)
		END,
		local_revision = local_revision + 1,
		client_updated_at = %s
	WHERE table_name = '%s' AND record_id = NEW.%s;
	INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
	SELECT NEW.%s, '%s', %s, %s, %s
	WHERE NOT EXISTS (
		SELECT 1 FROM _synchro_pending_changes
		WHERE table_name = '%s' AND record_id = NEW.%s
	);
%s
END`, updateTrigger, quotedTable, lockCheck,
			updateOperationExpr, updateOperationExpr, updateOperationExpr,
			updateOperationExpr, baseUpdatedAtExpr, nowExpr, escapedName, quotedPK,
			quotedPK, escapedName, updateOperationExpr, baseUpdatedAtExpr, nowExpr,
			escapedName, quotedPK, updateCleanupSQL),
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
	case "decimal", "datetime", "date", "time", "json":
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
