package synchro

import (
	"context"
	"fmt"
)

// OwnershipResolver determines which buckets a record belongs to.
// Returns a list of bucket IDs (e.g., ["user:abc-123", "global"]).
type OwnershipResolver interface {
	ResolveOwner(ctx context.Context, db DB, table string, recordID string, data map[string]any) ([]string, error)
}

// JoinResolver is the default OwnershipResolver that uses registry metadata.
type JoinResolver struct {
	registry *Registry
}

// NewJoinResolver creates a JoinResolver from a registry.
func NewJoinResolver(registry *Registry) *JoinResolver {
	return &JoinResolver{registry: registry}
}

// ResolveOwner determines bucket IDs for a record.
func (r *JoinResolver) ResolveOwner(ctx context.Context, db DB, table string, recordID string, data map[string]any) ([]string, error) {
	cfg := r.registry.Get(table)
	if cfg == nil {
		return nil, fmt.Errorf("table %q not registered", table)
	}

	// ServerOnly tables go to the global bucket
	if cfg.Direction == ServerOnly {
		return []string{"global"}, nil
	}

	// Tables with OwnerColumn — read from data (zero queries)
	if cfg.OwnerColumn != "" {
		ownerID, ok := data[cfg.OwnerColumn]
		if !ok || ownerID == nil {
			// SystemAndUser tables with NULL owner → global
			if cfg.Direction == SystemAndUser {
				return []string{"global"}, nil
			}
			return nil, fmt.Errorf("table %q record %q: missing owner column %q", table, recordID, cfg.OwnerColumn)
		}
		ownerStr := fmt.Sprintf("%v", ownerID)
		buckets := []string{fmt.Sprintf("user:%s", ownerStr)}
		if cfg.Direction == SystemAndUser {
			// SystemAndUser records are in both user and global buckets
			buckets = append(buckets, "global")
		}
		return buckets, nil
	}

	// Child table — resolve via parent chain with a single JOIN query
	if cfg.ParentTable != "" {
		return r.resolveViaParentChain(ctx, db, cfg, data)
	}

	return nil, fmt.Errorf("table %q record %q: cannot determine ownership (no OwnerColumn or ParentTable)", table, recordID)
}

// resolveViaParentChain builds a JOIN query through the parent chain to find the owner.
func (r *JoinResolver) resolveViaParentChain(ctx context.Context, db DB, cfg *TableConfig, data map[string]any) ([]string, error) {
	// Get the parent FK value from data
	parentIDVal, ok := data[cfg.ParentFKCol]
	if !ok || parentIDVal == nil {
		return nil, fmt.Errorf("table %q: missing parent FK %q", cfg.TableName, cfg.ParentFKCol)
	}
	parentIDStr := fmt.Sprintf("%v", parentIDVal)

	// Walk up the parent chain to find the root with OwnerColumn
	chain := []*TableConfig{cfg}
	current := cfg
	for current.ParentTable != "" {
		parent := r.registry.Get(current.ParentTable)
		if parent == nil {
			return nil, fmt.Errorf("table %q: parent %q not registered", current.TableName, current.ParentTable)
		}
		chain = append(chain, parent)
		if parent.OwnerColumn != "" {
			break
		}
		current = parent
	}

	root := chain[len(chain)-1]
	if root.OwnerColumn == "" {
		return nil, fmt.Errorf("table %q: parent chain root %q has no OwnerColumn", cfg.TableName, root.TableName)
	}

	// Build a single query with JOINs through the chain
	// For a 2-level chain (child → parent), this is:
	//   SELECT parent.owner_col FROM parent WHERE parent.id = $1
	// For a 3-level chain (grandchild → child → parent):
	//   SELECT p2.owner_col FROM child p1 JOIN parent p2 ON p1.parent_fk = p2.id WHERE p1.id = $1
	if len(chain) == 2 {
		// Direct parent has OwnerColumn
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1",
			quoteIdentifier(root.OwnerColumn),
			quoteIdentifier(root.TableName),
			quoteIdentifier(root.IDColumn))

		var ownerID *string
		err := db.QueryRowContext(ctx, query, parentIDStr).Scan(&ownerID)
		if err != nil {
			return nil, fmt.Errorf("resolving ownership: %w", err)
		}
		if ownerID == nil {
			if root.Direction == SystemAndUser {
				return []string{"global"}, nil
			}
			return nil, fmt.Errorf("table %q: owner is nil", root.TableName)
		}
		return []string{fmt.Sprintf("user:%s", *ownerID)}, nil
	}

	// Multi-level: build JOIN query
	// chain is [child, ..., root]. We query from chain[1] (first parent) up.
	query := fmt.Sprintf("SELECT %s.%s", fmt.Sprintf("p%d", len(chain)-2), quoteIdentifier(root.OwnerColumn))
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
	err := db.QueryRowContext(ctx, query, parentIDStr).Scan(&ownerID)
	if err != nil {
		return nil, fmt.Errorf("resolving ownership via chain: %w", err)
	}
	if ownerID == nil {
		if root.Direction == SystemAndUser {
			return []string{"global"}, nil
		}
		return nil, fmt.Errorf("table %q: owner is nil at root %q", cfg.TableName, root.TableName)
	}
	return []string{fmt.Sprintf("user:%s", *ownerID)}, nil
}
