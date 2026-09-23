package observer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// PostgresConfig configures one observer connection and its closed allowlists.
type PostgresConfig struct {
	DB                 *sql.DB
	SourceTables       []SourceTable
	SanitizedFunctions []SanitizedFunction
	MaximumRows        int
}

// Postgres observes PostgreSQL through one supplied restricted role connection.
type Postgres struct {
	database    *sql.DB
	sources     map[string]relationSpec
	functions   map[string]SanitizedFunction
	maximumRows int
}

type relationSpec struct {
	name        string
	relation    string
	columns     []string
	orderBy     []string
	maximumRows int
}

var operationalCatalogs = map[string]relationSpec{
	"pg_catalog.pg_replication_slots": {
		name:     "pg_catalog.pg_replication_slots",
		relation: "pg_catalog.pg_replication_slots",
		columns:  []string{"slot_name", "slot_type", "plugin", "active", "confirmed_flush_lsn"},
		orderBy:  []string{"slot_name"},
	},
	"pg_catalog.pg_publication": {
		name:     "pg_catalog.pg_publication",
		relation: "pg_catalog.pg_publication",
		columns:  []string{"pubname", "puballtables", "pubinsert", "pubupdate", "pubdelete"},
		orderBy:  []string{"pubname"},
	},
	"pg_catalog.pg_stat_activity": {
		name:     "pg_catalog.pg_stat_activity",
		relation: "pg_catalog.pg_stat_activity",
		columns:  []string{"datname", "backend_type", "state", "wait_event_type"},
		orderBy:  []string{"datname", "backend_type", "state"},
	},
	"pg_catalog.pg_stat_database": {
		name:     "pg_catalog.pg_stat_database",
		relation: "pg_catalog.pg_stat_database",
		columns:  []string{"datname", "numbackends", "xact_commit", "xact_rollback", "deadlocks"},
		orderBy:  []string{"datname"},
	},
}

// NewPostgres creates a read-only observer with explicit source and function allowlists.
func NewPostgres(config PostgresConfig) (*Postgres, error) {
	if config.DB == nil {
		return nil, errors.New("observer database is required")
	}
	if config.MaximumRows == 0 {
		config.MaximumRows = defaultMaximumRows
	}
	if config.MaximumRows < 1 || config.MaximumRows > maximumRows {
		return nil, errors.New("observer maximum row count is invalid")
	}
	observer := &Postgres{
		database:    config.DB,
		sources:     make(map[string]relationSpec, len(config.SourceTables)),
		functions:   make(map[string]SanitizedFunction, len(config.SanitizedFunctions)),
		maximumRows: config.MaximumRows,
	}
	for _, source := range config.SourceTables {
		spec, err := normalizeSourceTable(source, config.MaximumRows)
		if err != nil {
			return nil, err
		}
		if _, exists := observer.sources[spec.name]; exists {
			return nil, errors.New("observer source table is duplicated")
		}
		observer.sources[spec.name] = spec
	}
	for _, function := range config.SanitizedFunctions {
		if function.Name == "" || !function.ReadOnly || !validQualifiedIdentifier(function.Function) {
			return nil, errors.New("observer sanitized function is invalid")
		}
		if _, exists := observer.functions[function.Name]; exists {
			return nil, errors.New("observer sanitized function is duplicated")
		}
		query := "SELECT to_jsonb(" + quoteQualifiedIdentifier(function.Function) + "())"
		if err := ValidateReadOnlySQL(query); err != nil {
			return nil, errors.New("observer sanitized function is unsafe")
		}
		observer.functions[function.Name] = function
	}
	return observer, nil
}

func normalizeSourceTable(source SourceTable, defaultLimit int) (relationSpec, error) {
	if source.Name == "" || !validQualifiedIdentifier(source.Relation) || len(source.Columns) == 0 || len(source.OrderBy) == 0 {
		return relationSpec{}, errors.New("observer source table is invalid")
	}
	limit := source.MaximumRows
	if limit == 0 {
		limit = defaultLimit
	}
	if limit < 1 || limit > maximumRows {
		return relationSpec{}, errors.New("observer source table row limit is invalid")
	}
	columns := append([]string(nil), source.Columns...)
	orderBy := append([]string(nil), source.OrderBy...)
	seenColumns := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if !validQualifiedIdentifier(column) || strings.Contains(column, ".") {
			return relationSpec{}, errors.New("observer source table column is invalid")
		}
		if _, exists := seenColumns[column]; exists {
			return relationSpec{}, errors.New("observer source table column is duplicated")
		}
		seenColumns[column] = struct{}{}
	}
	for _, column := range orderBy {
		if _, exists := seenColumns[column]; !exists {
			return relationSpec{}, errors.New("observer source table order column is invalid")
		}
	}
	spec := relationSpec{name: source.Name, relation: source.Relation, columns: columns, orderBy: orderBy, maximumRows: limit}
	if err := ValidateReadOnlySQL(relationQuery(spec)); err != nil {
		return relationSpec{}, errors.New("observer source table query is unsafe")
	}
	return spec, nil
}

// Snapshot captures all requested values in one read-only repeatable-read transaction.
func (observer *Postgres) Snapshot(ctx context.Context, request SnapshotRequest) (Snapshot, error) {
	if observer == nil || observer.database == nil {
		return Snapshot{}, errors.New("observer is unavailable")
	}
	if ctx == nil {
		return Snapshot{}, errors.New("observer context is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := observer.validateRequest(request); err != nil {
		return Snapshot{}, err
	}
	tx, err := observer.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return Snapshot{}, errors.New("begin observer transaction failed")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		return Snapshot{}, errors.New("set observer transaction read-only failed")
	}
	result := Snapshot{
		SourceTables:        make([]RelationSnapshot, 0, len(request.SourceTables)),
		OperationalCatalogs: make([]RelationSnapshot, 0, len(request.OperationalCatalogs)),
		Functions:           make([]FunctionSnapshot, 0, len(request.SanitizedFunctions)),
	}
	for _, name := range request.SourceTables {
		snapshot, err := observer.snapshotRelation(ctx, tx, observer.sources[name])
		if err != nil {
			return Snapshot{}, err
		}
		result.SourceTables = append(result.SourceTables, snapshot)
	}
	for _, name := range request.OperationalCatalogs {
		spec := operationalCatalogs[name]
		spec.maximumRows = observer.maximumRows
		snapshot, err := observer.snapshotRelation(ctx, tx, spec)
		if err != nil {
			return Snapshot{}, err
		}
		result.OperationalCatalogs = append(result.OperationalCatalogs, snapshot)
	}
	for _, name := range request.SanitizedFunctions {
		snapshot, err := observer.snapshotFunction(ctx, tx, observer.functions[name])
		if err != nil {
			return Snapshot{}, err
		}
		result.Functions = append(result.Functions, snapshot)
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, errors.New("commit observer transaction failed")
	}
	return result, nil
}

func (observer *Postgres) validateRequest(request SnapshotRequest) error {
	if !uniqueAllowed(request.SourceTables, observer.sources) {
		return errors.New("observer source table request is invalid")
	}
	if !uniqueAllowed(request.OperationalCatalogs, operationalCatalogs) {
		return errors.New("observer operational catalog request is invalid")
	}
	if !uniqueAllowed(request.SanitizedFunctions, observer.functions) {
		return errors.New("observer sanitized function request is invalid")
	}
	return nil
}

func uniqueAllowed[T any](values []string, allowed map[string]T) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return false
		}
		if _, exists := allowed[value]; !exists {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func (observer *Postgres) snapshotRelation(ctx context.Context, tx *sql.Tx, spec relationSpec) (RelationSnapshot, error) {
	query := relationQuery(spec)
	if err := ValidateReadOnlySQL(query); err != nil {
		return RelationSnapshot{}, errors.New("observer relation query is unsafe")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return RelationSnapshot{}, errors.New("read observer relation failed")
	}
	rows, err := decodeRows(raw)
	if err != nil {
		return RelationSnapshot{}, errors.New("decode observer relation failed")
	}
	return RelationSnapshot{Name: spec.name, Rows: rows}, nil
}

func (observer *Postgres) snapshotFunction(ctx context.Context, tx *sql.Tx, function SanitizedFunction) (FunctionSnapshot, error) {
	query := "SELECT to_jsonb(" + quoteQualifiedIdentifier(function.Function) + "())"
	if err := ValidateReadOnlySQL(query); err != nil {
		return FunctionSnapshot{}, errors.New("observer function query is unsafe")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return FunctionSnapshot{}, errors.New("read observer function failed")
	}
	if !json.Valid(raw) {
		return FunctionSnapshot{}, errors.New("decode observer function failed")
	}
	return FunctionSnapshot{Name: function.Name, Value: append(json.RawMessage(nil), raw...)}, nil
}

func relationQuery(spec relationSpec) string {
	columns := make([]string, len(spec.columns))
	for index, column := range spec.columns {
		columns[index] = quoteIdentifier(column)
	}
	orderBy := make([]string, len(spec.orderBy))
	for index, column := range spec.orderBy {
		orderBy[index] = quoteIdentifier(column)
	}
	return "SELECT COALESCE(json_agg(row_to_json(observer_row)), '[]'::json) " +
		"FROM (SELECT " + strings.Join(columns, ", ") + " FROM " + quoteQualifiedIdentifier(spec.relation) +
		" ORDER BY " + strings.Join(orderBy, ", ") + " LIMIT " + fmt.Sprintf("%d", spec.maximumRows) + ") AS observer_row"
}

func decodeRows(raw []byte) ([]map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, errors.New("missing JSON rows")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var rows []map[string]json.RawMessage
	if err := decoder.Decode(&rows); err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []map[string]json.RawMessage{}
	}
	for _, row := range rows {
		if row == nil {
			return nil, errors.New("observer row is not an object")
		}
	}
	return rows, nil
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteQualifiedIdentifier(value string) string {
	parts := strings.Split(value, ".")
	for index, part := range parts {
		parts[index] = quoteIdentifier(part)
	}
	return strings.Join(parts, ".")
}
