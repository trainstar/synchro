package synchro

import (
	"context"
	"fmt"
)

// OwnershipResolver determines which buckets a record belongs to.
type OwnershipResolver interface {
	ResolveOwner(ctx context.Context, db DB, table string, recordID string, data map[string]any) ([]string, error)
}

// JoinResolver is the default OwnershipResolver that uses registry metadata.
// It also implements wal.BucketAssigner when constructed with NewJoinResolverWithDB.
type JoinResolver struct {
	registry *Registry
	db       DB
}

// NewJoinResolver creates a JoinResolver from a registry.
// The returned resolver can be used as an OwnershipResolver but not as a
// wal.BucketAssigner (no DB handle). Use NewJoinResolverWithDB for WAL consumer usage.
func NewJoinResolver(registry *Registry) *JoinResolver {
	return &JoinResolver{registry: registry}
}

// NewJoinResolverWithDB creates a JoinResolver that also satisfies wal.BucketAssigner.
// The db handle is used by AssignBuckets to resolve parent-chain ownership.
func NewJoinResolverWithDB(registry *Registry, db DB) *JoinResolver {
	return &JoinResolver{registry: registry, db: db}
}

// AssignBuckets implements wal.BucketAssigner by delegating to ResolveOwner.
func (r *JoinResolver) AssignBuckets(ctx context.Context, table string, recordID string, _ Operation, data map[string]any) ([]string, error) {
	if r.db == nil {
		return nil, fmt.Errorf("JoinResolver: AssignBuckets requires a DB handle; use NewJoinResolverWithDB")
	}
	return r.ResolveOwner(ctx, r.db, table, recordID, data)
}

// ResolveOwner determines bucket IDs for a record.
func (r *JoinResolver) ResolveOwner(ctx context.Context, db DB, table string, recordID string, data map[string]any) ([]string, error) {
	cfg := r.registry.Get(table)
	if cfg == nil {
		return nil, fmt.Errorf("table %q not registered", table)
	}

	if cfg.BucketByColumn != "" {
		if raw, ok := data[cfg.BucketByColumn]; ok && raw != nil {
			v := fmt.Sprintf("%v", raw)
			if v != "" {
				return []string{cfg.BucketPrefix + v}, nil
			}
		}
		if cfg.GlobalWhenBucketNull {
			return []string{"global"}, nil
		}
		return []string{}, nil
	}

	if cfg.OwnerColumn != "" {
		if raw, ok := data[cfg.OwnerColumn]; ok && raw != nil {
			v := fmt.Sprintf("%v", raw)
			if v != "" {
				prefix := cfg.BucketPrefix
				if prefix == "" {
					prefix = "user:"
				}
				return []string{prefix + v}, nil
			}
		}
		if cfg.AllowGlobalRead {
			return []string{"global"}, nil
		}
		return []string{}, nil
	}

	if cfg.ParentTable != "" {
		return r.resolveViaParentChain(ctx, db, cfg, data)
	}

	return []string{"global"}, nil
}

// resolveViaParentChain builds a JOIN query through the parent chain to find the owner.
func (r *JoinResolver) resolveViaParentChain(ctx context.Context, db DB, cfg *TableConfig, data map[string]any) ([]string, error) {
	parentIDVal, ok := data[cfg.ParentFKCol]
	if !ok || parentIDVal == nil {
		return nil, fmt.Errorf("table %q: missing parent FK %q", cfg.TableName, cfg.ParentFKCol)
	}
	parentIDStr := fmt.Sprintf("%v", parentIDVal)

	chain := []*TableConfig{cfg}
	current := cfg
	for current.ParentTable != "" {
		parent := r.registry.Get(current.ParentTable)
		if parent == nil {
			return nil, fmt.Errorf("table %q: parent %q not registered", current.TableName, current.ParentTable)
		}
		chain = append(chain, parent)
		if parent.OwnerColumn != "" || parent.BucketByColumn != "" {
			break
		}
		current = parent
	}

	root := chain[len(chain)-1]
	if root.OwnerColumn == "" && root.BucketByColumn == "" {
		return nil, fmt.Errorf("table %q: parent chain root %q has no ownership bucket column", cfg.TableName, root.TableName)
	}

	ownerCol := root.BucketByColumn
	if ownerCol == "" {
		ownerCol = root.OwnerColumn
	}
	prefix := root.BucketPrefix
	if prefix == "" {
		prefix = "user:"
	}

	if len(chain) == 2 {
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
			quoteIdentifier(ownerCol),
			quoteIdentifier(root.TableName),
			quoteIdentifier(root.IDColumn))

		var ownerID *string
		if err := db.QueryRowContext(ctx, query, parentIDStr).Scan(&ownerID); err != nil {
			return nil, fmt.Errorf("resolving ownership: %w", err)
		}
		if ownerID == nil || *ownerID == "" {
			if root.AllowGlobalRead {
				return []string{"global"}, nil
			}
			return []string{}, nil
		}
		return []string{prefix + *ownerID}, nil
	}

	query := fmt.Sprintf("SELECT %s.%s", fmt.Sprintf("p%d", len(chain)-2), quoteIdentifier(ownerCol))
	query += fmt.Sprintf(" FROM %s p0", quoteIdentifier(chain[1].TableName))

	for i := 2; i < len(chain); i++ {
		prev := chain[i-1]
		curr := chain[i]
		alias := fmt.Sprintf("p%d", i-1)
		prevAlias := fmt.Sprintf("p%d", i-2)
		query += fmt.Sprintf(" JOIN %s %s ON %s.%s = %s.%s",
			quoteIdentifier(curr.TableName), alias,
			prevAlias, quoteIdentifier(prev.ParentFKCol),
			alias, quoteIdentifier(curr.IDColumn))
	}

	query += fmt.Sprintf(" WHERE p0.%s = $1", quoteIdentifier(chain[1].IDColumn))

	var ownerID *string
	if err := db.QueryRowContext(ctx, query, parentIDStr).Scan(&ownerID); err != nil {
		return nil, fmt.Errorf("resolving ownership via chain: %w", err)
	}
	if ownerID == nil || *ownerID == "" {
		if root.AllowGlobalRead {
			return []string{"global"}, nil
		}
		return []string{}, nil
	}
	return []string{prefix + *ownerID}, nil
}
