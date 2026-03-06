package synchro

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
)

var sqlFunctionNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_\.]*$`)

// SQLBucketResolverConfig configures SQL-first bucket resolution.
type SQLBucketResolverConfig struct {
	// DB is used for SQL rule execution and optional fallback lookups.
	DB DB

	// Registry provides table config and default ownership metadata.
	Registry *Registry

	// GlobalFunction is the SQL resolver function used when a table does not
	// define TableConfig.BucketFunction.
	// Signature: (table_name text, operation text, new_row jsonb, old_row jsonb) -> setof text.
	GlobalFunction string

	// EnableGoFallback allows fallback to OwnershipResolver when SQL rule execution fails.
	// Default false for strict SQL-only operation.
	EnableGoFallback bool

	// GoFallback is used only when EnableGoFallback is true.
	// Defaults to JoinResolver.
	GoFallback OwnershipResolver

	// MaxBucketsPerEvent bounds fanout for a single row change. 0 means unlimited.
	MaxBucketsPerEvent int

	// Logger receives fallback warnings.
	Logger *slog.Logger
}

// SQLBucketResolver resolves row buckets via SQL rules first, then optional Go fallback.
type SQLBucketResolver struct {
	db               DB
	registry         *Registry
	globalFunction   string
	enableGoFallback bool
	goFallback       OwnershipResolver
	maxBuckets       int
	logger           *slog.Logger
}

// NewSQLBucketResolver creates a SQL-first resolver.
func NewSQLBucketResolver(cfg SQLBucketResolverConfig) (*SQLBucketResolver, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("synchro: SQLBucketResolver DB is required")
	}
	if cfg.Registry == nil {
		return nil, fmt.Errorf("synchro: SQLBucketResolver Registry is required")
	}
	if cfg.GlobalFunction != "" && !sqlFunctionNamePattern.MatchString(cfg.GlobalFunction) {
		return nil, fmt.Errorf("synchro: invalid SQL function name %q", cfg.GlobalFunction)
	}
	for _, t := range cfg.Registry.All() {
		if t.BucketFunction != "" && !sqlFunctionNamePattern.MatchString(t.BucketFunction) {
			return nil, fmt.Errorf("synchro: invalid table bucket function %q for table %q", t.BucketFunction, t.TableName)
		}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	fallback := cfg.GoFallback
	if fallback == nil {
		fallback = NewJoinResolver(cfg.Registry)
	}

	return &SQLBucketResolver{
		db:               cfg.DB,
		registry:         cfg.Registry,
		globalFunction:   cfg.GlobalFunction,
		enableGoFallback: cfg.EnableGoFallback,
		goFallback:       fallback,
		maxBuckets:       cfg.MaxBucketsPerEvent,
		logger:           logger,
	}, nil
}

// AssignBuckets resolves buckets for a row change.
// This method matches wal.BucketAssigner.
func (r *SQLBucketResolver) AssignBuckets(
	ctx context.Context,
	table string,
	recordID string,
	operation Operation,
	data map[string]any,
) ([]string, error) {
	cfg := r.registry.Get(table)
	if cfg == nil {
		return nil, ErrTableNotRegistered
	}

	resolverFn := cfg.BucketFunction
	if resolverFn == "" {
		resolverFn = r.globalFunction
	}

	var (
		buckets []string
		err     error
	)

	if resolverFn != "" {
		buckets, err = r.callSQLRule(ctx, resolverFn, table, operation, data)
		if err != nil && r.enableGoFallback {
			r.logger.WarnContext(ctx, "bucket SQL rule failed, falling back to Go resolver",
				"table", table, "record_id", recordID, "err", err)
			buckets, err = r.goFallback.ResolveOwner(ctx, r.db, table, recordID, data)
		}
	} else {
		buckets, err = r.defaultBuckets(ctx, cfg, table, recordID, data)
	}
	if err != nil {
		return nil, err
	}

	buckets = dedupeBuckets(buckets)
	if r.maxBuckets > 0 && len(buckets) > r.maxBuckets {
		return nil, fmt.Errorf("synchro: bucket fanout %d exceeds limit %d for %s/%s", len(buckets), r.maxBuckets, table, recordID)
	}
	return buckets, nil
}

func (r *SQLBucketResolver) defaultBuckets(
	ctx context.Context,
	cfg *TableConfig,
	table string,
	recordID string,
	data map[string]any,
) ([]string, error) {
	if cfg.BucketByColumn != "" {
		if owner, ok := data[cfg.BucketByColumn]; ok && owner != nil && fmt.Sprintf("%v", owner) != "" {
			return []string{cfg.BucketPrefix + fmt.Sprintf("%v", owner)}, nil
		}
		if cfg.GlobalWhenBucketNull {
			return []string{"global"}, nil
		}
		return []string{}, nil
	}
	if cfg.OwnerColumn != "" {
		if owner, ok := data[cfg.OwnerColumn]; ok && owner != nil && fmt.Sprintf("%v", owner) != "" {
			return []string{cfg.BucketPrefix + fmt.Sprintf("%v", owner)}, nil
		}
		if cfg.AllowGlobalRead {
			return []string{"global"}, nil
		}
		return []string{}, nil
	}
	if cfg.ParentTable != "" {
		return r.goFallback.ResolveOwner(ctx, r.db, table, recordID, data)
	}
	return []string{"global"}, nil
}

func (r *SQLBucketResolver) callSQLRule(
	ctx context.Context,
	functionName string,
	table string,
	operation Operation,
	data map[string]any,
) ([]string, error) {
	newRow, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshalling row payload: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT bucket_id FROM %s($1, $2, $3::jsonb, NULL::jsonb) AS bucket_id",
		functionName,
	)
	rows, err := r.db.QueryContext(ctx, query, table, operation.String(), string(newRow))
	if err != nil {
		return nil, fmt.Errorf("executing SQL bucket rule %q: %w", functionName, err)
	}
	defer rows.Close()

	buckets := make([]string, 0, 4)
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("scanning SQL bucket rule row: %w", err)
		}
		if bucket != "" {
			buckets = append(buckets, bucket)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading SQL bucket rule rows: %w", err)
	}
	return buckets, nil
}

func dedupeBuckets(in []string) []string {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, b := range in {
		if b == "" {
			continue
		}
		if _, ok := seen[b]; ok {
			continue
		}
		seen[b] = struct{}{}
		out = append(out, b)
	}
	return out
}
