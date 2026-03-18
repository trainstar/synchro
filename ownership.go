package synchro

import (
	"context"
	"encoding/json"
	"fmt"
)

// JoinResolver resolves record ownership by walking the introspected FK chain.
// It satisfies the wal.BucketAssigner interface when constructed with
// NewJoinResolverWithDB.
type JoinResolver struct {
	registry   *Registry
	db         DB
	bucketFunc BucketFunc
}

// NewJoinResolver creates a JoinResolver from a registry.
// The returned resolver can be used as a BucketAssigner but not for
// FK chain resolution (no DB handle). Use NewJoinResolverWithDB for
// WAL consumer usage.
func NewJoinResolver(registry *Registry) *JoinResolver {
	return &JoinResolver{registry: registry}
}

// NewJoinResolverWithDB creates a JoinResolver that also satisfies wal.BucketAssigner.
// The db handle is used by AssignBuckets to resolve parent-chain ownership.
// An optional BucketFunc can be provided to determine ownership from record data
// (e.g. UserBucket("user_id")). Without it, root tables always resolve to "global".
func NewJoinResolverWithDB(registry *Registry, db DB, opts ...BucketFunc) *JoinResolver {
	var fn BucketFunc
	if len(opts) > 0 {
		fn = opts[0]
	}
	return &JoinResolver{registry: registry, db: db, bucketFunc: fn}
}

// AssignBuckets implements wal.BucketAssigner by delegating to BucketFunc
// (if set) and falling back to FK chain resolution via ResolveOwner.
func (r *JoinResolver) AssignBuckets(ctx context.Context, table string, recordID string, op Operation, data map[string]any) ([]string, error) {
	if r.db == nil {
		return nil, fmt.Errorf("JoinResolver: AssignBuckets requires a DB handle; use NewJoinResolverWithDB")
	}
	// Try BucketFunc first. it handles direct ownership columns (e.g. user_id).
	if r.bucketFunc != nil {
		buckets := r.bucketFunc(table, op.String(), data)
		if len(buckets) > 0 {
			return buckets, nil
		}
	}
	return r.ResolveOwner(ctx, r.db, table, recordID, data)
}

// ResolveOwner determines bucket IDs for a record by walking the introspected
// FK parent chain. Returns ["global"] for tables with no parent chain.
func (r *JoinResolver) ResolveOwner(ctx context.Context, db DB, table string, recordID string, data map[string]any) ([]string, error) {
	cfg := r.registry.Get(table)
	if cfg == nil {
		return nil, fmt.Errorf("table %q not registered", table)
	}

	if cfg.parentTable == "" {
		return []string{"global"}, nil
	}

	return r.resolveViaParentChain(ctx, db, cfg, data)
}

// resolveViaParentChain walks the FK parent chain, loading each parent record
// and testing BucketFunc until ownership is resolved or the root is reached.
func (r *JoinResolver) resolveViaParentChain(ctx context.Context, db DB, cfg *TableConfig, data map[string]any) ([]string, error) {
	parentIDVal, ok := data[cfg.parentFKCol]
	if !ok || parentIDVal == nil {
		return nil, fmt.Errorf("table %q: missing parent FK %q", cfg.TableName, cfg.parentFKCol)
	}

	current := cfg
	currentID := fmt.Sprintf("%v", parentIDVal)

	for current.parentTable != "" {
		parent := r.registry.Get(current.parentTable)
		if parent == nil {
			return nil, fmt.Errorf("table %q: parent %q not registered", current.TableName, current.parentTable)
		}

		// Load parent record data.
		query := fmt.Sprintf("SELECT row_to_json(t)::text FROM %s t WHERE %s = $1",
			quoteIdentifier(parent.TableName),
			quoteIdentifier(parent.IDColumn))

		var dataStr string
		if err := db.QueryRowContext(ctx, query, currentID).Scan(&dataStr); err != nil {
			return nil, fmt.Errorf("resolving parent chain: %w", err)
		}

		var parentData map[string]any
		if err := json.Unmarshal([]byte(dataStr), &parentData); err != nil {
			return nil, fmt.Errorf("unmarshalling parent data: %w", err)
		}

		// Try BucketFunc on the parent record.
		if r.bucketFunc != nil {
			buckets := r.bucketFunc(parent.TableName, "update", parentData)
			if len(buckets) > 0 {
				return buckets, nil
			}
		}

		// Continue up the chain.
		if parent.parentTable == "" {
			break
		}
		fkVal, ok := parentData[parent.parentFKCol]
		if !ok || fkVal == nil {
			break
		}
		currentID = fmt.Sprintf("%v", fkVal)
		current = parent
	}

	return []string{"global"}, nil
}
