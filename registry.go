package synchro

import (
	"context"
	"fmt"
)

// Table is the minimal user-facing type for registering a sync table.
// Behavioral control (authorization, bucketing) is configured at the engine
// level via AuthorizeWriteFunc and BucketFunc.
type Table struct {
	Name    string   // database table name
	Exclude []string // per-table column exclusions
}

// FKRelation describes a foreign key relationship between two tables.
type FKRelation struct {
	Column    string // local FK column
	RefTable  string // referenced table
	RefColumn string // referenced column (usually PK)
}

// TableConfig holds sync metadata for a table. Built by introspection from
// a minimal Table registration. External consumers (WAL decoder, seeddb)
// access IDColumn, HasDeletedAt(), DeletedAtCol(), etc.
type TableConfig struct {
	// TableName is the database table name.
	TableName string

	// ExcludeColumns are columns omitted from introspection and client schema.
	ExcludeColumns []string

	// IDColumn is the primary key column name (introspected, defaults to "id").
	IDColumn string

	// Introspected FK relationships.
	parentTable string       // auto-detected single parent (exactly one FK to another registered table)
	parentFKCol string       // the FK column pointing to parent
	foreignKeys []FKRelation // all FK relationships to registered tables

	// Column metadata.
	columnNullable map[string]bool // introspected from pg_attribute.attnotnull

	// Timestamp columns (existing).
	hasUpdatedAt    bool
	hasDeletedAt    bool
	hasCreatedAt    bool
	updatedAtColumn string // resolved name or "" if absent
	deletedAtColumn string // resolved name or "" if absent

	// Protected columns (computed: PK + timestamps + FK columns).
	protectedSet map[string]bool
}

// HasUpdatedAt returns true if the table has an updated_at column.
func (c *TableConfig) HasUpdatedAt() bool { return c.hasUpdatedAt }

// HasDeletedAt returns true if the table has a deleted_at column.
func (c *TableConfig) HasDeletedAt() bool { return c.hasDeletedAt }

// HasCreatedAt returns true if the table has a created_at column.
func (c *TableConfig) HasCreatedAt() bool { return c.hasCreatedAt }

// UpdatedAtCol returns the resolved updated_at column name, or "" if absent.
func (c *TableConfig) UpdatedAtCol() string { return c.updatedAtColumn }

// DeletedAtCol returns the resolved deleted_at column name, or "" if absent.
func (c *TableConfig) DeletedAtCol() string { return c.deletedAtColumn }

// ParentTable returns the auto-detected parent table name, or "" if none.
func (c *TableConfig) ParentTable() string { return c.parentTable }

// ParentFKCol returns the FK column pointing to the parent table, or "" if none.
func (c *TableConfig) ParentFKCol() string { return c.parentFKCol }

// ForeignKeys returns all introspected FK relationships to registered tables.
func (c *TableConfig) ForeignKeys() []FKRelation {
	return append([]FKRelation{}, c.foreignKeys...)
}

// ColumnNullable returns whether a column is nullable. Returns false if column
// is not known (not introspected).
func (c *TableConfig) ColumnNullable(col string) bool {
	if c.columnNullable == nil {
		return false
	}
	return c.columnNullable[col]
}

// buildProtectedSet computes a preliminary set of protected columns.
// Called during registerTable before introspection runs.
func (c *TableConfig) buildProtectedSet() map[string]bool {
	s := make(map[string]bool, 6)
	s[c.IDColumn] = true
	return s
}

// finalizeProtectedSet rebuilds the protected set after introspection,
// adding timestamp columns and FK columns.
func (c *TableConfig) finalizeProtectedSet() {
	s := make(map[string]bool, 6+len(c.foreignKeys))
	s[c.IDColumn] = true
	if c.hasCreatedAt {
		s["created_at"] = true
	}
	if c.hasUpdatedAt {
		s[c.updatedAtColumn] = true
	}
	if c.hasDeletedAt {
		s[c.deletedAtColumn] = true
	}
	// All FK columns are protected.
	for _, fk := range c.foreignKeys {
		s[fk.Column] = true
	}
	c.protectedSet = s
}

// IsProtected returns true if the column is protected from client writes.
func (c *TableConfig) IsProtected(col string) bool {
	return c.protectedSet[col]
}

// AllowedInsertColumns returns the set of columns allowed in an INSERT.
// This includes all non-protected columns plus id and FK columns
// (which are set once on creation). When column metadata has been
// introspected, columns that don't exist in the table are silently
// dropped — this prevents middleware like StampColumn from injecting
// columns into child tables that lack them (e.g., user_id on
// order_details when only the parent orders table has user_id).
func (c *TableConfig) AllowedInsertColumns(dataCols []string) []string {
	allowed := make([]string, 0, len(dataCols))
	for _, col := range dataCols {
		if c.columnNullable != nil {
			if _, exists := c.columnNullable[col]; !exists {
				continue // column doesn't exist in the table schema
			}
		}
		if !c.protectedSet[col] || col == c.IDColumn || c.isFKColumn(col) {
			allowed = append(allowed, col)
		}
	}
	return allowed
}

// AllowedUpdateColumns returns columns from dataCols that are allowed in an UPDATE.
// Only non-protected columns — no PKs, FK columns, or timestamps.
// When column metadata has been introspected, columns that don't exist
// in the table are silently dropped.
func (c *TableConfig) AllowedUpdateColumns(dataCols []string) []string {
	allowed := make([]string, 0, len(dataCols))
	for _, col := range dataCols {
		if c.columnNullable != nil {
			if _, exists := c.columnNullable[col]; !exists {
				continue // column doesn't exist in the table schema
			}
		}
		if !c.protectedSet[col] {
			allowed = append(allowed, col)
		}
	}
	return allowed
}

// isFKColumn returns true if col is one of the introspected FK columns.
func (c *TableConfig) isFKColumn(col string) bool {
	for _, fk := range c.foreignKeys {
		if fk.Column == col {
			return true
		}
	}
	return false
}

// Registry holds all syncable table configurations.
type Registry struct {
	tables     map[string]*TableConfig
	tableOrder []string
	pushOrder  []string // topologically sorted (parents before children)
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		tables:     make(map[string]*TableConfig),
		tableOrder: make([]string, 0),
	}
}

// registerTable creates a TableConfig from a Table with merged exclude columns.
// This is called internally by NewEngine.
func (r *Registry) registerTable(t Table, globalExclude []string) {
	merged := make([]string, 0, len(globalExclude)+len(t.Exclude))
	seen := make(map[string]bool, len(globalExclude)+len(t.Exclude))
	for _, col := range globalExclude {
		if !seen[col] {
			merged = append(merged, col)
			seen[col] = true
		}
	}
	for _, col := range t.Exclude {
		if !seen[col] {
			merged = append(merged, col)
			seen[col] = true
		}
	}

	cfg := &TableConfig{
		TableName:      t.Name,
		ExcludeColumns: merged,
		IDColumn:       "id", // default, overridden by introspection
	}
	cfg.protectedSet = cfg.buildProtectedSet()

	r.tables[cfg.TableName] = cfg
	r.tableOrder = append(r.tableOrder, cfg.TableName)
}

// RegisterForTest adds a fully-specified TableConfig to the registry.
// This is ONLY for unit tests that need a registry without DB introspection.
func (r *Registry) RegisterForTest(cfg *TableConfig) {
	if cfg.IDColumn == "" {
		cfg.IDColumn = "id"
	}
	if cfg.protectedSet == nil {
		cfg.protectedSet = cfg.buildProtectedSet()
	}
	r.tables[cfg.TableName] = cfg
	r.tableOrder = append(r.tableOrder, cfg.TableName)
}

// Get returns the configuration for a table, or nil if not registered.
func (r *Registry) Get(name string) *TableConfig {
	return r.tables[name]
}

// All returns all registered table configurations in registration order.
func (r *Registry) All() []*TableConfig {
	result := make([]*TableConfig, 0, len(r.tableOrder))
	for _, name := range r.tableOrder {
		result = append(result, r.tables[name])
	}
	return result
}

// TableNames returns the names of all registered tables.
func (r *Registry) TableNames() []string {
	return append([]string{}, r.tableOrder...)
}

// IsRegistered returns true if the table is registered.
func (r *Registry) IsRegistered(tableName string) bool {
	_, ok := r.tables[tableName]
	return ok
}

// PushOrder returns table names in topological order (parents before children).
// Populated after Introspect runs FK detection.
func (r *Registry) PushOrder() []string {
	return append([]string{}, r.pushOrder...)
}

// Validate checks all registered tables for configuration errors.
// Only checks that table names are non-empty and unique.
func (r *Registry) Validate() error {
	seen := make(map[string]bool, len(r.tableOrder))
	for _, name := range r.tableOrder {
		if name == "" {
			return fmt.Errorf("synchro: table name must not be empty")
		}
		if seen[name] {
			return fmt.Errorf("synchro: duplicate table name %q", name)
		}
		seen[name] = true
	}
	return nil
}

// Introspect queries the database for each registered table to determine
// which timestamp columns exist, detect PKs, FKs, and column nullability.
func (r *Registry) Introspect(ctx context.Context, db DB, updatedAtColumn, deletedAtColumn string) error {
	if updatedAtColumn == "" {
		updatedAtColumn = "updated_at"
	}
	if deletedAtColumn == "" {
		deletedAtColumn = "deleted_at"
	}

	for _, tcfg := range r.All() {
		// Timestamp introspection (existing).
		hasUA, hasDA, hasCA, err := introspectTimestampColumns(ctx, db, tcfg.TableName, updatedAtColumn, deletedAtColumn)
		if err != nil {
			return fmt.Errorf("introspecting %q: %w", tcfg.TableName, err)
		}
		tcfg.hasUpdatedAt = hasUA
		tcfg.hasDeletedAt = hasDA
		tcfg.hasCreatedAt = hasCA
		if hasUA {
			tcfg.updatedAtColumn = updatedAtColumn
		}
		if hasDA {
			tcfg.deletedAtColumn = deletedAtColumn
		}

		// PK introspection.
		schemaName, tableName := splitSchemaTable(tcfg.TableName)
		pkCols, err := loadPrimaryKeyColumns(ctx, db, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("introspecting PK for %q: %w", tcfg.TableName, err)
		}
		if len(pkCols) == 1 {
			tcfg.IDColumn = pkCols[0]
		}

		// Column nullability introspection.
		nullable, err := loadColumnNullability(ctx, db, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("introspecting column nullability for %q: %w", tcfg.TableName, err)
		}
		tcfg.columnNullable = nullable
	}

	// FK introspection: detect foreign keys between registered tables.
	if err := r.introspectForeignKeys(ctx, db); err != nil {
		return err
	}

	// Finalize protected sets after FK detection.
	for _, tcfg := range r.All() {
		tcfg.finalizeProtectedSet()
	}

	// Build push order via topological sort.
	r.pushOrder = r.topologicalSort()

	return nil
}

// introspectForeignKeys queries pg_constraint for FK relationships between registered tables.
func (r *Registry) introspectForeignKeys(ctx context.Context, db DB) error {
	for _, tcfg := range r.All() {
		schemaName, tableName := splitSchemaTable(tcfg.TableName)
		fks, err := loadForeignKeys(ctx, db, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("introspecting FKs for %q: %w", tcfg.TableName, err)
		}

		// Filter to only FKs that reference other registered tables.
		var registered []FKRelation
		for _, fk := range fks {
			if r.IsRegistered(fk.RefTable) {
				registered = append(registered, fk)
			}
		}
		tcfg.foreignKeys = registered

		// Auto-detect parent: if exactly one FK to another registered table, set it.
		if len(registered) == 1 {
			tcfg.parentTable = registered[0].RefTable
			tcfg.parentFKCol = registered[0].Column
		}
	}
	return nil
}

// topologicalSort returns table names ordered so that parents come before children.
// Uses Kahn's algorithm.
func (r *Registry) topologicalSort() []string {
	// Build adjacency: parent -> []children, and in-degree count.
	inDegree := make(map[string]int, len(r.tableOrder))
	children := make(map[string][]string, len(r.tableOrder))
	for _, name := range r.tableOrder {
		inDegree[name] = 0
	}
	for _, tcfg := range r.All() {
		if tcfg.parentTable != "" && r.IsRegistered(tcfg.parentTable) {
			inDegree[tcfg.TableName]++
			children[tcfg.parentTable] = append(children[tcfg.parentTable], tcfg.TableName)
		}
	}

	// Seed queue with zero-degree nodes.
	queue := make([]string, 0, len(r.tableOrder))
	for _, name := range r.tableOrder {
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		sorted = append(sorted, node)
		for _, child := range children[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	// If some tables were not sorted (cycle), append them at the end.
	if len(sorted) < len(r.tableOrder) {
		sortedSet := make(map[string]bool, len(sorted))
		for _, s := range sorted {
			sortedSet[s] = true
		}
		for _, name := range r.tableOrder {
			if !sortedSet[name] {
				sorted = append(sorted, name)
			}
		}
	}

	return sorted
}

// loadColumnNullability queries pg_attribute for column nullability.
func loadColumnNullability(ctx context.Context, db DB, schemaName, tableName string) (map[string]bool, error) {
	var rows interface {
		Next() bool
		Scan(dest ...any) error
		Close() error
		Err() error
	}
	var err error

	if schemaName != "" {
		rows, err = db.QueryContext(ctx, `
			SELECT a.attname, NOT a.attnotnull AS nullable
			FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = $1
			  AND n.nspname = $2
			  AND a.attnum > 0
			  AND NOT a.attisdropped
		`, tableName, schemaName)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT a.attname, NOT a.attnotnull AS nullable
			FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = $1
			  AND n.nspname = ANY (current_schemas(false))
			  AND a.attnum > 0
			  AND NOT a.attisdropped
		`, tableName)
	}
	if err != nil {
		return nil, fmt.Errorf("querying column nullability for %q: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]bool)
	for rows.Next() {
		var name string
		var nullable bool
		if err := rows.Scan(&name, &nullable); err != nil {
			return nil, fmt.Errorf("scanning nullability for %q: %w", tableName, err)
		}
		result[name] = nullable
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading nullability for %q: %w", tableName, err)
	}
	return result, nil
}

// loadForeignKeys queries pg_constraint for FK relationships on a table.
func loadForeignKeys(ctx context.Context, db DB, schemaName, tableName string) ([]FKRelation, error) {
	query := `
		SELECT
			a.attname AS fk_column,
			ref_class.relname AS ref_table,
			ref_attr.attname AS ref_column
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_class ref_class ON ref_class.oid = con.confrelid
		JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
		JOIN pg_catalog.pg_attribute ref_attr ON ref_attr.attrelid = con.confrelid AND ref_attr.attnum = ANY(con.confkey)
		WHERE con.contype = 'f'
		  AND c.relname = $1
	`
	var args []any
	if schemaName != "" {
		query += " AND n.nspname = $2"
		args = []any{tableName, schemaName}
	} else {
		query += " AND n.nspname = ANY (current_schemas(false))"
		args = []any{tableName}
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys for %q: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()

	var fks []FKRelation
	for rows.Next() {
		var fk FKRelation
		if err := rows.Scan(&fk.Column, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, fmt.Errorf("scanning FK for %q: %w", tableName, err)
		}
		fks = append(fks, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading FKs for %q: %w", tableName, err)
	}
	return fks, nil
}
