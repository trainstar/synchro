package synchro

import "fmt"

// SyncDirection defines how data flows for a table.
type SyncDirection string

const (
	// Bidirectional means the table supports both push and pull (user-owned data).
	Bidirectional SyncDirection = "bidirectional"
	// ServerOnly means the table only supports pull (reference data).
	ServerOnly SyncDirection = "server_only"
	// SystemAndUser means the table pulls both system (NULL owner) and user records,
	// but only accepts pushes for user-owned records.
	SystemAndUser SyncDirection = "system_and_user"
)

// defaultProtectedColumns are columns that clients may never write directly.
var defaultProtectedColumns = map[string]bool{
	"created_at": true,
	"updated_at": true,
	"deleted_at": true,
}

// TableConfig defines sync behavior for a table.
type TableConfig struct {
	// TableName is the database table name.
	TableName string

	// Direction specifies if the table is bidirectional or server-only.
	Direction SyncDirection

	// OwnerColumn is the column name for user ownership (empty for server-only).
	OwnerColumn string

	// ParentTable is the parent table name for child tables.
	ParentTable string

	// ParentFKCol is the foreign key column pointing to the parent table.
	ParentFKCol string

	// SyncColumns specifies which columns to include in pull responses (nil = all columns).
	SyncColumns []string

	// Dependencies lists tables that must sync first (for push ordering).
	Dependencies []string

	// IDColumn is the primary key column name (defaults to "id").
	IDColumn string

	// UpdatedAtColumn is the timestamp column for change tracking (defaults to "updated_at").
	UpdatedAtColumn string

	// DeletedAtColumn is the soft delete column (defaults to "deleted_at").
	DeletedAtColumn string

	// ProtectedColumns are additional columns (beyond defaults) that clients may not write.
	// Default protected: id, created_at, updated_at, deleted_at, OwnerColumn.
	ProtectedColumns []string

	// protectedSet is a pre-computed lookup set, populated by Register().
	protectedSet map[string]bool
}

// buildProtectedSet computes the full set of protected columns.
func (c *TableConfig) buildProtectedSet() map[string]bool {
	s := make(map[string]bool, len(defaultProtectedColumns)+len(c.ProtectedColumns)+3)
	for col := range defaultProtectedColumns {
		s[col] = true
	}
	s[c.IDColumn] = true
	if c.OwnerColumn != "" {
		s[c.OwnerColumn] = true
	}
	if c.ParentFKCol != "" {
		s[c.ParentFKCol] = true
	}
	for _, col := range c.ProtectedColumns {
		s[col] = true
	}
	return s
}

// IsProtected returns true if the column is protected from client writes.
func (c *TableConfig) IsProtected(col string) bool {
	return c.protectedSet[col]
}

// AllowedInsertColumns returns the set of columns allowed in an INSERT.
// This includes all non-protected columns plus id, owner col, and parent FK col
// (which are set once on creation).
func (c *TableConfig) AllowedInsertColumns(dataCols []string) []string {
	allowed := make([]string, 0, len(dataCols))
	for _, col := range dataCols {
		// Allow: non-protected columns + id + owner + parent FK (set once at insert)
		if !c.protectedSet[col] || col == c.IDColumn || col == c.OwnerColumn || col == c.ParentFKCol {
			allowed = append(allowed, col)
		}
	}
	return allowed
}

// AllowedUpdateColumns returns columns from dataCols that are allowed in an UPDATE.
// Only non-protected columns — no PKs, ownership FKs, or timestamps.
func (c *TableConfig) AllowedUpdateColumns(dataCols []string) []string {
	allowed := make([]string, 0, len(dataCols))
	for _, col := range dataCols {
		if !c.protectedSet[col] {
			allowed = append(allowed, col)
		}
	}
	return allowed
}

// Registry holds all syncable table configurations.
type Registry struct {
	tables     map[string]*TableConfig
	tableOrder []string
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		tables:     make(map[string]*TableConfig),
		tableOrder: make([]string, 0),
	}
}

// Register adds a table configuration to the registry.
func (r *Registry) Register(cfg *TableConfig) {
	if cfg.IDColumn == "" {
		cfg.IDColumn = "id"
	}
	if cfg.UpdatedAtColumn == "" {
		cfg.UpdatedAtColumn = "updated_at"
	}
	if cfg.DeletedAtColumn == "" {
		cfg.DeletedAtColumn = "deleted_at"
	}

	cfg.protectedSet = cfg.buildProtectedSet()

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

// BidirectionalTables returns all tables configured for bidirectional sync.
func (r *Registry) BidirectionalTables() []*TableConfig {
	var result []*TableConfig
	for _, name := range r.tableOrder {
		cfg := r.tables[name]
		if cfg.Direction == Bidirectional {
			result = append(result, cfg)
		}
	}
	return result
}

// ServerOnlyTables returns all tables configured for server-only sync.
func (r *Registry) ServerOnlyTables() []*TableConfig {
	var result []*TableConfig
	for _, name := range r.tableOrder {
		cfg := r.tables[name]
		if cfg.Direction == ServerOnly {
			result = append(result, cfg)
		}
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

// IsPushable returns true if the table accepts push operations.
func (r *Registry) IsPushable(tableName string) bool {
	cfg := r.tables[tableName]
	return cfg != nil && (cfg.Direction == Bidirectional || cfg.Direction == SystemAndUser)
}

// Validate checks all registered tables for configuration errors.
func (r *Registry) Validate() error {
	for _, name := range r.tableOrder {
		cfg := r.tables[name]

		// ProtectedColumns must not contain default protected columns (redundant)
		for _, col := range cfg.ProtectedColumns {
			if defaultProtectedColumns[col] {
				return fmt.Errorf("table %q: ProtectedColumns contains default protected column %q (redundant)", name, col)
			}
			if col == cfg.IDColumn {
				return fmt.Errorf("table %q: ProtectedColumns contains PK column %q (redundant)", name, col)
			}
			if col == cfg.OwnerColumn {
				return fmt.Errorf("table %q: ProtectedColumns contains ownership column %q (redundant)", name, col)
			}
		}

		// Child tables with ParentTable must reference a registered table
		if cfg.ParentTable != "" {
			parentCfg := r.tables[cfg.ParentTable]
			if parentCfg == nil {
				return fmt.Errorf("table %q references unregistered parent %q", name, cfg.ParentTable)
			}
			if cfg.ParentFKCol == "" {
				return fmt.Errorf("table %q has ParentTable %q but no ParentFKCol", name, cfg.ParentTable)
			}
		}

		// Parent chain must terminate at a table with OwnerColumn (no orphaned chains)
		if cfg.ParentTable != "" {
			visited := map[string]bool{name: true}
			current := cfg
			for current.ParentTable != "" {
				if visited[current.ParentTable] {
					return fmt.Errorf("table %q: cycle detected in parent chain at %q", name, current.ParentTable)
				}
				visited[current.ParentTable] = true
				parent := r.tables[current.ParentTable]
				if parent == nil {
					return fmt.Errorf("table %q: broken parent chain, %q is not registered", name, current.ParentTable)
				}
				current = parent
			}
			if current.OwnerColumn == "" {
				return fmt.Errorf("table %q: parent chain root %q has no OwnerColumn (orphaned chain)", name, current.TableName)
			}
		}

		// Pushable tables without OwnerColumn and without ParentTable must be ServerOnly
		if r.IsPushable(name) && cfg.OwnerColumn == "" && cfg.ParentTable == "" {
			return fmt.Errorf("table %q is pushable but has no OwnerColumn or ParentTable", name)
		}
	}
	return nil
}
