// Package observer provides bounded read-only PostgreSQL observation.
package observer

import (
	"context"
	"encoding/json"
)

const (
	defaultMaximumRows = 1000
	maximumRows        = 10000
)

// Observer captures one consistent, read-only authoritative snapshot.
type Observer interface {
	Snapshot(ctx context.Context, req SnapshotRequest) (Snapshot, error)
}

// SnapshotRequest selects only preconfigured source tables, catalogs, and functions.
// It does not accept caller-supplied SQL.
type SnapshotRequest struct {
	SourceTables        []string `json:"source_tables,omitempty"`
	OperationalCatalogs []string `json:"operational_catalogs,omitempty"`
	SanitizedFunctions  []string `json:"sanitized_functions,omitempty"`
}

// Snapshot contains data captured in one repeatable-read transaction.
type Snapshot struct {
	SourceTables        []RelationSnapshot `json:"source_tables"`
	OperationalCatalogs []RelationSnapshot `json:"operational_catalogs"`
	Functions           []FunctionSnapshot `json:"functions"`
}

// RelationSnapshot contains deterministic JSON rows from one allowed relation.
type RelationSnapshot struct {
	Name string                       `json:"name"`
	Rows []map[string]json.RawMessage `json:"rows"`
}

// FunctionSnapshot contains one JSON value from a preapproved zero-argument function.
type FunctionSnapshot struct {
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"`
}

// SourceTable defines one explicitly named source relation and its safe projection.
type SourceTable struct {
	Name        string
	Relation    string
	Columns     []string
	OrderBy     []string
	MaximumRows int
}

// SanitizedFunction defines one explicitly approved zero-argument extension function.
// ReadOnly must be true because the observer never invokes mutable functions.
type SanitizedFunction struct {
	Name     string
	Function string
	ReadOnly bool
}
