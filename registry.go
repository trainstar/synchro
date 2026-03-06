package synchro

import "fmt"

// PushPolicy defines whether a table accepts client writes.
type PushPolicy string

const (
	// PushPolicyDisabled means the table is read-only from clients.
	PushPolicyDisabled PushPolicy = "disabled"
	// PushPolicyOwnerOnly means pushes are allowed when ownership can be resolved.
	PushPolicyOwnerOnly PushPolicy = "owner_only"
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

	// PushPolicy controls whether pushes are accepted for this table.
	// Default: owner_only when OwnerColumn/ParentTable is set, otherwise disabled.
	PushPolicy PushPolicy

	// OwnerColumn is the column name for user ownership.
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

	// BucketByColumn enables fast-path bucketing without SQL function execution.
	// Bucket ID is formed as BucketPrefix + value(column).
	BucketByColumn string

	// BucketPrefix prefixes fast-path bucket values. Default "user:".
	BucketPrefix string

	// GlobalWhenBucketNull emits "global" when BucketByColumn value is NULL/empty.
	GlobalWhenBucketNull bool

	// AllowGlobalRead enables NULL-owner rows to be readable by all users via RLS.
	// Default false.
	AllowGlobalRead bool

	// BucketFunction optionally overrides the global SQL bucket resolver function
	// for this table. Signature:
	//   (table_name text, operation text, new_row jsonb, old_row jsonb) -> setof text.
	BucketFunction string

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
	if cfg.PushPolicy == "" {
		if cfg.OwnerColumn != "" || cfg.ParentTable != "" {
			cfg.PushPolicy = PushPolicyOwnerOnly
		} else {
			cfg.PushPolicy = PushPolicyDisabled
		}
	}
	if cfg.BucketByColumn == "" && cfg.BucketFunction == "" && cfg.OwnerColumn != "" {
		cfg.BucketByColumn = cfg.OwnerColumn
	}
	if cfg.BucketByColumn != "" && cfg.BucketPrefix == "" {
		cfg.BucketPrefix = "user:"
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
	return cfg != nil && cfg.PushPolicy != PushPolicyDisabled
}

// Validate checks all registered tables for configuration errors.
func (r *Registry) Validate() error {
	for _, name := range r.tableOrder {
		cfg := r.tables[name]

		if cfg.PushPolicy != PushPolicyDisabled && cfg.PushPolicy != PushPolicyOwnerOnly {
			return fmt.Errorf("%w: table %q has %q", ErrInvalidPushPolicy, name, cfg.PushPolicy)
		}

		if cfg.GlobalWhenBucketNull && !cfg.AllowGlobalRead {
			return fmt.Errorf("%w: table %q has GlobalWhenBucketNull without AllowGlobalRead", ErrInvalidBucketConfig, name)
		}

		for _, col := range cfg.ProtectedColumns {
			if defaultProtectedColumns[col] {
				return fmt.Errorf("%w: table %q contains default protected column %q", ErrRedundantProtected, name, col)
			}
			if col == cfg.IDColumn {
				return fmt.Errorf("%w: table %q contains PK column %q", ErrRedundantProtected, name, col)
			}
			if col == cfg.OwnerColumn {
				return fmt.Errorf("%w: table %q contains ownership column %q", ErrRedundantProtected, name, col)
			}
		}

		if cfg.ParentTable != "" {
			parentCfg := r.tables[cfg.ParentTable]
			if parentCfg == nil {
				return fmt.Errorf("%w: table %q references %q", ErrUnregisteredParent, name, cfg.ParentTable)
			}
			if cfg.ParentFKCol == "" {
				return fmt.Errorf("%w: table %q has ParentTable %q", ErrMissingParentFKCol, name, cfg.ParentTable)
			}
		}

		if cfg.ParentTable != "" {
			visited := map[string]bool{name: true}
			current := cfg
			for current.ParentTable != "" {
				if visited[current.ParentTable] {
					return fmt.Errorf("%w: table %q at %q", ErrCycleDetected, name, current.ParentTable)
				}
				visited[current.ParentTable] = true
				parent := r.tables[current.ParentTable]
				if parent == nil {
					return fmt.Errorf("%w: table %q, %q is not registered", ErrUnregisteredParent, name, current.ParentTable)
				}
				current = parent
			}
			if current.OwnerColumn == "" {
				return fmt.Errorf("%w: table %q, root %q has no OwnerColumn", ErrOrphanedChain, name, current.TableName)
			}
		}

		if r.IsPushable(name) && cfg.OwnerColumn == "" && cfg.ParentTable == "" {
			return fmt.Errorf("%w: table %q has no OwnerColumn or ParentTable", ErrMissingOwnership, name)
		}
	}
	return nil
}
