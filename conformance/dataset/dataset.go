// Package dataset defines one synthetic training-application dataset for
// realistic sync proof and characterization. It owns the source schema, its
// registration, an authored small flow, and a deterministic seeded generator.
//
// Expected results come from source rows and the authored business rule in
// ScopeRowsSQL. They never come from the tested encoder or a reference engine.
package dataset

import (
	"bytes"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

// SchemaSQL creates the source tables, indexes, and application triggers.
//
//go:embed schema.sql
var SchemaSQL string

// RegistrationSQL registers the source tables, scopes, and dependencies.
// Run it after SchemaSQL and CREATE EXTENSION synchro_pg.
//
//go:embed register.sql
var RegistrationSQL string

// CatalogScope is the shared portable scope for global catalog rows.
const CatalogScope = "catalog"

// Column is one synced source column and its authored portable type.
type Column struct {
	Name string
	Type string
}

// Table is one registered source table and its synced columns in schema order.
type Table struct {
	Name     string
	Pushable bool
	Columns  []Column
}

func timestamps(columns ...Column) []Column {
	return append(columns,
		Column{"created_at", "datetime"},
		Column{"updated_at", "datetime"},
		Column{"deleted_at", "datetime"},
	)
}

// Tables lists every registered table. Generated columns are not synced.
var Tables = []Table{
	{Name: "organizations", Columns: timestamps(
		Column{"id", "string"}, Column{"name", "string"}, Column{"slug", "string"}, Column{"settings", "json"})},
	{Name: "organization_members", Columns: timestamps(
		Column{"id", "string"}, Column{"organization_id", "string"}, Column{"user_id", "string"},
		Column{"role", "string"}, Column{"joined_at", "datetime"})},
	{Name: "equipment", Columns: timestamps(
		Column{"id", "string"}, Column{"name", "string"}, Column{"category", "string"})},
	{Name: "exercises", Columns: timestamps(
		Column{"id", "string"}, Column{"organization_id", "string"}, Column{"name", "string"},
		Column{"instructions", "string"}, Column{"muscle_groups", "json"}, Column{"difficulty", "int"},
		Column{"metrics", "json"}, Column{"search_text", "string"})},
	{Name: "exercise_equipment", Columns: timestamps(
		Column{"id", "string"}, Column{"exercise_id", "string"}, Column{"equipment_id", "string"})},
	{Name: "programs", Pushable: true, Columns: timestamps(
		Column{"id", "string"}, Column{"organization_id", "string"}, Column{"owner_id", "string"},
		Column{"title", "string"}, Column{"description", "string"}, Column{"visibility", "string"},
		Column{"week_count", "int"}, Column{"settings", "json"})},
	{Name: "workouts", Pushable: true, Columns: timestamps(
		Column{"id", "string"}, Column{"program_id", "string"}, Column{"owner_id", "string"},
		Column{"name", "string"}, Column{"notes", "string"}, Column{"scheduled_on", "date"},
		Column{"started_at", "datetime"}, Column{"duration_seconds", "int"},
		Column{"total_volume_kg", "decimal"}, Column{"external_ref", "int64"})},
	{Name: "workout_exercises", Pushable: true, Columns: timestamps(
		Column{"id", "string"}, Column{"workout_id", "string"}, Column{"program_id", "string"},
		Column{"exercise_id", "string"}, Column{"owner_id", "string"}, Column{"position", "int"},
		Column{"target", "json"})},
	{Name: "exercise_sets", Pushable: true, Columns: timestamps(
		Column{"id", "string"}, Column{"workout_exercise_id", "string"}, Column{"workout_id", "string"},
		Column{"program_id", "string"}, Column{"owner_id", "string"}, Column{"set_index", "int"},
		Column{"reps", "int"}, Column{"weight_kg", "decimal"}, Column{"rpe", "float"},
		Column{"duration_ms", "int64"}, Column{"completed_at", "datetime"}, Column{"note", "string"})},
	{Name: "workout_media", Pushable: true, Columns: timestamps(
		Column{"id", "string"}, Column{"workout_id", "string"}, Column{"owner_id", "string"},
		Column{"content_type", "string"}, Column{"body", "bytes"}, Column{"byte_size", "int"})},
}

// TableNames returns the registered table names in registration order.
func TableNames() []string {
	names := make([]string, 0, len(Tables))
	for _, table := range Tables {
		names = append(names, table.Name)
	}
	return names
}

// LookupTable returns the catalog entry for one table name.
func LookupTable(name string) (Table, bool) {
	for _, table := range Tables {
		if table.Name == name {
			return table, true
		}
	}
	return Table{}, false
}

// SourceRowsSQL selects every source row of one table. Each column is SQL
// text in the canonical form that CompareWire accepts for its type.
func SourceRowsSQL(table Table) string {
	expressions := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		expressions = append(expressions, canonicalExpression(column))
	}
	return "SELECT " + strings.Join(expressions, ", ") + " FROM public." + table.Name + " ORDER BY id"
}

func canonicalExpression(column Column) string {
	name := column.Name
	switch column.Type {
	case "decimal":
		return "trim_scale(" + name + ")::text"
	case "datetime":
		return "to_char(" + name + " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
	case "date":
		return "to_char(" + name + ", 'YYYY-MM-DD')"
	case "json":
		return "to_jsonb(" + name + ")::text"
	case "bytes":
		return "encode(" + name + ", 'hex')"
	default:
		return name + "::text"
	}
}

// ScopeRowsSQL is the authored business rule. It returns every expected
// (scope_id, table_name, record_id) pair from live source rows:
//   - organizations and their member rows belong to org:{organization_id}
//   - a member row also belongs to user:{user_id}
//   - equipment and global exercises belong to the catalog scope
//   - an organization exercise and its equipment links belong to its org scope
//   - a live program belongs to user:{owner_id}, and to org:{organization_id}
//     when its visibility is organization
//   - a live workout, workout exercise, or set follows its live program
//   - media belongs to user:{owner_id}
const ScopeRowsSQL = `
WITH live_programs AS (
    SELECT id, organization_id, owner_id, visibility FROM public.programs WHERE deleted_at IS NULL
), program_scopes AS (
    SELECT id AS program_id, 'user:' || owner_id AS scope_id FROM live_programs
    UNION ALL
    SELECT id, 'org:' || organization_id::text FROM live_programs WHERE visibility = 'organization'
)
SELECT 'org:' || id::text, 'organizations', id::text FROM public.organizations WHERE deleted_at IS NULL
UNION ALL
SELECT 'org:' || organization_id::text, 'organization_members', id::text FROM public.organization_members WHERE deleted_at IS NULL
UNION ALL
SELECT 'user:' || user_id, 'organization_members', id::text FROM public.organization_members WHERE deleted_at IS NULL
UNION ALL
SELECT 'catalog', 'equipment', id::text FROM public.equipment WHERE deleted_at IS NULL
UNION ALL
SELECT COALESCE('org:' || organization_id::text, 'catalog'), 'exercises', id::text FROM public.exercises WHERE deleted_at IS NULL
UNION ALL
SELECT COALESCE('org:' || exercise.organization_id::text, 'catalog'), 'exercise_equipment', link.id::text
FROM public.exercise_equipment AS link
JOIN public.exercises AS exercise ON exercise.id = link.exercise_id AND exercise.deleted_at IS NULL
WHERE link.deleted_at IS NULL
UNION ALL
SELECT scope.scope_id, 'programs', scope.program_id::text FROM program_scopes AS scope
UNION ALL
SELECT scope.scope_id, 'workouts', child.id::text
FROM public.workouts AS child JOIN program_scopes AS scope ON scope.program_id = child.program_id
WHERE child.deleted_at IS NULL
UNION ALL
SELECT scope.scope_id, 'workout_exercises', child.id::text
FROM public.workout_exercises AS child JOIN program_scopes AS scope ON scope.program_id = child.program_id
WHERE child.deleted_at IS NULL
UNION ALL
SELECT scope.scope_id, 'exercise_sets', child.id::text
FROM public.exercise_sets AS child JOIN program_scopes AS scope ON scope.program_id = child.program_id
WHERE child.deleted_at IS NULL
UNION ALL
SELECT 'user:' || owner_id, 'workout_media', id::text FROM public.workout_media WHERE deleted_at IS NULL`

// AssignedScopesSQL is the authored assignment rule for one user ($1). A user
// receives the catalog, the private user scope, and one org scope for each
// live membership. The dataset operator grants exactly those org scopes.
const AssignedScopesSQL = `
SELECT 'catalog'
UNION SELECT 'user:' || $1::text
UNION SELECT 'org:' || organization_id::text FROM public.organization_members
WHERE user_id = $1::text AND deleted_at IS NULL
ORDER BY 1`

// ErrValueMismatch reports a wire value that differs from its source value.
var ErrValueMismatch = errors.New("dataset wire value differs from the source value")

// CompareWire compares one protocol wire value with its source value from
// SourceRowsSQL. source is nil for SQL NULL.
func CompareWire(portableType string, wire json.RawMessage, source *string) error {
	wire = bytes.TrimSpace(wire)
	if source == nil {
		if string(wire) == "null" {
			return nil
		}
		return fmt.Errorf("%w: want null", ErrValueMismatch)
	}
	if string(wire) == "null" {
		return fmt.Errorf("%w: got null", ErrValueMismatch)
	}
	switch portableType {
	case "int", "float":
		var number json.Number
		if len(wire) == 0 || wire[0] == '"' || json.Unmarshal(wire, &number) != nil {
			return fmt.Errorf("%w: %s is not a JSON number", ErrValueMismatch, portableType)
		}
		if portableType == "int" {
			if number.String() != *source {
				return fmt.Errorf("%w: int %s, want %s", ErrValueMismatch, number, *source)
			}
			return nil
		}
		got, gotErr := strconv.ParseFloat(number.String(), 64)
		want, wantErr := strconv.ParseFloat(*source, 64)
		if gotErr != nil || wantErr != nil || got != want {
			return fmt.Errorf("%w: float %s, want %s", ErrValueMismatch, number, *source)
		}
		return nil
	}
	var text string
	if err := json.Unmarshal(wire, &text); err != nil {
		return fmt.Errorf("%w: %s is not a JSON string", ErrValueMismatch, portableType)
	}
	switch portableType {
	case "string", "int64", "decimal", "datetime", "date":
		if text != *source {
			return fmt.Errorf("%w: %s %q, want %q", ErrValueMismatch, portableType, text, *source)
		}
	case "json":
		got, gotErr := decodeJSON(text)
		want, wantErr := decodeJSON(*source)
		if gotErr != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
			return fmt.Errorf("%w: json %q, want %q", ErrValueMismatch, text, *source)
		}
	case "bytes":
		got, err := base64.RawURLEncoding.DecodeString(text)
		if err != nil || fmt.Sprintf("%x", got) != *source {
			return fmt.Errorf("%w: bytes differ", ErrValueMismatch)
		}
	default:
		return fmt.Errorf("%w: unsupported type %s", ErrValueMismatch, portableType)
	}
	return nil
}

func decodeJSON(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("trailing JSON value")
	}
	return normalizeNumbers(value), nil
}

// normalizeNumbers compares JSON numbers by exact value, not by spelling.
func normalizeNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		if rational, ok := new(big.Rat).SetString(typed.String()); ok {
			return rational.RatString()
		}
		return typed.String()
	case []any:
		for index := range typed {
			typed[index] = normalizeNumbers(typed[index])
		}
	case map[string]any:
		for key := range typed {
			typed[key] = normalizeNumbers(typed[key])
		}
	}
	return value
}
