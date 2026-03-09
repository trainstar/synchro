package synchro

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

// Config configures the sync engine.
type Config struct {
	// DB is the database connection pool.
	DB *sql.DB

	// Registry holds table configurations.
	Registry *Registry

	// Hooks for lifecycle callbacks.
	Hooks Hooks

	// ConflictResolver resolves push conflicts. Defaults to LWW.
	ConflictResolver ConflictResolver

	// Ownership resolves record ownership for bucketing. Defaults to JoinResolver.
	Ownership OwnershipResolver

	// MinClientVersion is the minimum supported client version (semver).
	MinClientVersion string

	// ClockSkewTolerance is added to client timestamps during LWW comparison.
	ClockSkewTolerance time.Duration

	// Compactor configures changelog compaction. Optional.
	Compactor *CompactorConfig

	// Logger for sync operations. Defaults to slog.Default().
	Logger *slog.Logger
}

// Engine is the top-level sync orchestrator.
type Engine struct {
	db         *sql.DB
	registry   *Registry
	resolver   ConflictResolver
	ownership  OwnershipResolver
	hooks      Hooks
	push       *pushProcessor
	pull       *pullProcessor
	clients    *clientStore
	changelog  *changelogStore
	checkpoint *checkpointStore
	schema     *schemaStore
	compactor  *Compactor
	config     Config
	logger     *slog.Logger
}

// NewEngine creates a new sync engine from the given configuration.
func NewEngine(cfg Config) (*Engine, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("synchro: DB is required")
	}
	if cfg.Registry == nil {
		return nil, fmt.Errorf("synchro: Registry is required")
	}

	if err := cfg.Registry.Validate(); err != nil {
		return nil, fmt.Errorf("synchro: registry validation failed: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	resolver := cfg.ConflictResolver
	if resolver == nil {
		resolver = &LWWResolver{ClockSkewTolerance: cfg.ClockSkewTolerance}
	}

	ownership := cfg.Ownership
	if ownership == nil {
		ownership = NewJoinResolver(cfg.Registry)
	}

	cl := &changelogStore{}
	cp := &checkpointStore{}
	schema := &schemaStore{}

	var compactor *Compactor
	if cfg.Compactor != nil {
		compactor = NewCompactor(cfg.Compactor)
	}

	e := &Engine{
		db:         cfg.DB,
		registry:   cfg.Registry,
		resolver:   resolver,
		ownership:  ownership,
		hooks:      cfg.Hooks,
		clients:    &clientStore{},
		changelog:  cl,
		checkpoint: cp,
		schema:     schema,
		compactor:  compactor,
		config:     cfg,
		logger:     logger,
		push: &pushProcessor{
			registry:  cfg.Registry,
			resolver:  resolver,
			changelog: cl,
			hooks:     cfg.Hooks,
			logger:    logger,
		},
		pull: &pullProcessor{
			registry:   cfg.Registry,
			changelog:  cl,
			checkpoint: cp,
			logger:     logger,
		},
	}

	return e, nil
}

// Registry returns the engine's table registry.
func (e *Engine) Registry() *Registry {
	return e.registry
}

// RegisterClient registers or updates a sync client.
func (e *Engine) RegisterClient(ctx context.Context, userID string, req *RegisterRequest) (*RegisterResponse, error) {
	manifest, err := e.schema.GetManifest(ctx, e.db, e.registry)
	if err != nil {
		return nil, err
	}
	if err := validateClientSchema(manifest, req.SchemaVersion, req.SchemaHash, true); err != nil {
		return nil, err
	}

	// Version check
	if e.config.MinClientVersion != "" && req.AppVersion != "" {
		if err := CheckVersion(req.AppVersion, e.config.MinClientVersion); err != nil {
			if e.hooks.OnSchemaIncompatible != nil {
				e.hooks.OnSchemaIncompatible(ctx, req.ClientID, req.AppVersion, e.config.MinClientVersion)
			}
			return nil, err
		}
	}

	client, err := e.clients.RegisterClient(ctx, e.db, userID, req)
	if err != nil {
		return nil, err
	}

	var lastPullSeq int64
	if client.LastPullSeq != nil {
		lastPullSeq = *client.LastPullSeq
	}

	return &RegisterResponse{
		ID:            client.ID,
		ServerTime:    time.Now().UTC(),
		LastSyncAt:    client.LastSyncAt,
		Checkpoint:    lastPullSeq,
		SchemaVersion: manifest.Version,
		SchemaHash:    manifest.Hash,
	}, nil
}

// Pull retrieves changes for a client since their checkpoint.
func (e *Engine) Pull(ctx context.Context, userID string, req *PullRequest) (*PullResponse, error) {
	manifest, err := e.schema.GetManifest(ctx, e.db, e.registry)
	if err != nil {
		return nil, err
	}
	if err := validateClientSchema(manifest, req.SchemaVersion, req.SchemaHash, false); err != nil {
		return nil, err
	}

	// Verify client is registered and check staleness
	client, err := e.clients.GetClient(ctx, e.db, userID, req.ClientID)
	if err != nil {
		return nil, ErrClientNotRegistered
	}

	if e.hooks.OnStaleClient != nil && client.LastSyncAt != nil {
		if !e.hooks.OnStaleClient(ctx, req.ClientID, *client.LastSyncAt) {
			return nil, ErrStaleClient
		}
	}

	if req.Checkpoint == 0 && client.LastPullAt == nil {
		return &PullResponse{
			SnapshotRequired: true,
			SnapshotReason:   SnapshotReasonInitialSyncRequired,
			Changes:          []Record{},
			Deletes:          []DeleteEntry{},
			Checkpoint:       req.Checkpoint,
			SchemaVersion:    manifest.Version,
			SchemaHash:       manifest.Hash,
		}, nil
	}

	minSeq, err := e.changelog.MinSeq(ctx, e.db)
	if err != nil {
		return nil, fmt.Errorf("checking min seq: %w", err)
	}
	if req.Checkpoint > 0 && minSeq > 0 && req.Checkpoint < minSeq {
		if e.hooks.OnSnapshotRequired != nil {
			e.hooks.OnSnapshotRequired(ctx, req.ClientID, req.Checkpoint, minSeq, SnapshotReasonCheckpointBeforeLimit)
		}
		return &PullResponse{
			SnapshotRequired: true,
			SnapshotReason:   SnapshotReasonCheckpointBeforeLimit,
			Changes:          []Record{},
			Deletes:          []DeleteEntry{},
			Checkpoint:       req.Checkpoint,
			SchemaVersion:    manifest.Version,
			SchemaHash:       manifest.Hash,
		}, nil
	}

	// Get client's bucket subscriptions
	buckets := client.BucketSubs
	if len(buckets) == 0 {
		buckets = []string{fmt.Sprintf("user:%s", userID), "global"}
	}

	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("beginning pull transaction: %w", err)
	}
	defer tx.Rollback()

	if err := SetAuthContext(ctx, tx, userID); err != nil {
		return nil, fmt.Errorf("setting pull auth context: %w", err)
	}

	resp, err := e.pull.processPull(ctx, tx, req, buckets)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing pull transaction: %w", err)
	}
	resp.SchemaVersion = manifest.Version
	resp.SchemaHash = manifest.Hash

	// Compute bucket updates if client sent known buckets
	if len(req.KnownBuckets) > 0 {
		resp.BucketUpdates = diffBuckets(req.KnownBuckets, buckets)
	}

	// Advance checkpoint
	if resp.Checkpoint > req.Checkpoint {
		if err := e.checkpoint.AdvanceCheckpoint(ctx, e.db, userID, req.ClientID, resp.Checkpoint); err != nil {
			e.logger.WarnContext(ctx, "failed to advance checkpoint",
				"err", err, "client_id", req.ClientID)
		}
	}

	// Update last pull timestamp
	if err := e.clients.UpdateLastSync(ctx, e.db, userID, req.ClientID, "pull"); err != nil {
		e.logger.WarnContext(ctx, "failed to update client last pull",
			"err", err, "client_id", req.ClientID)
	}

	if e.hooks.OnPullComplete != nil {
		e.hooks.OnPullComplete(ctx, req.ClientID, resp.Checkpoint, len(resp.Changes))
	}

	return resp, nil
}

// Push processes client changes within a transaction.
func (e *Engine) Push(ctx context.Context, userID string, req *PushRequest) (*PushResponse, error) {
	manifest, err := e.schema.GetManifest(ctx, e.db, e.registry)
	if err != nil {
		return nil, err
	}
	if err := validateClientSchema(manifest, req.SchemaVersion, req.SchemaHash, false); err != nil {
		return nil, err
	}

	// Verify client is registered and check staleness
	client, err := e.clients.GetClient(ctx, e.db, userID, req.ClientID)
	if err != nil {
		return nil, ErrClientNotRegistered
	}

	if e.hooks.OnStaleClient != nil && client.LastSyncAt != nil {
		if !e.hooks.OnStaleClient(ctx, req.ClientID, *client.LastSyncAt) {
			return nil, ErrStaleClient
		}
	}

	accepted := make([]PushResult, 0)
	rejected := make([]PushResult, 0)

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Set RLS context
	if err := SetAuthContext(ctx, tx, userID); err != nil {
		return nil, fmt.Errorf("setting auth context: %w", err)
	}

	for _, record := range req.Changes {
		result, err := e.push.processPush(ctx, tx, userID, &record)
		if err != nil {
			e.logger.ErrorContext(ctx, "failed to process push record",
				"err", err, "table", record.TableName, "id", record.ID)
			rejected = append(rejected, PushResult{
				ID: record.ID, TableName: record.TableName, Operation: record.Operation,
				Status:     PushStatusRejectedRetryable,
				ReasonCode: "internal_error",
				Message:    "internal error",
			})
			continue
		}

		if result.Status == PushStatusApplied {
			accepted = append(accepted, *result)
		} else {
			rejected = append(rejected, *result)
		}
	}

	// Fire OnPushAccepted hook within the transaction
	if e.hooks.OnPushAccepted != nil && len(accepted) > 0 {
		records := make([]AcceptedRecord, len(accepted))
		for i, a := range accepted {
			op, _ := ParseOperation(a.Operation)
			records[i] = AcceptedRecord{
				ID: a.ID, TableName: a.TableName, Operation: op,
			}
		}
		if err := e.hooks.OnPushAccepted(ctx, tx, records); err != nil {
			return nil, fmt.Errorf("OnPushAccepted hook: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing push transaction: %w", err)
	}

	// Update last push timestamp
	if err := e.clients.UpdateLastSync(ctx, e.db, userID, req.ClientID, "push"); err != nil {
		e.logger.WarnContext(ctx, "failed to update client last push",
			"err", err, "client_id", req.ClientID)
	}

	return &PushResponse{
		Accepted:      accepted,
		Rejected:      rejected,
		Checkpoint:    0,
		ServerTime:    time.Now().UTC(),
		SchemaVersion: manifest.Version,
		SchemaHash:    manifest.Hash,
	}, nil
}

// TableMetadata returns sync metadata for all registered tables.
func (e *Engine) TableMetadata(ctx context.Context) (*TableMetaResponse, error) {
	manifest, err := e.schema.GetManifest(ctx, e.db, e.registry)
	if err != nil {
		return nil, err
	}

	configs := e.registry.All()
	slices.SortFunc(configs, func(a, b *TableConfig) int {
		return strings.Compare(a.TableName, b.TableName)
	})
	tables := make([]TableMeta, 0, len(configs))

	for _, cfg := range configs {
		deps := append([]string{}, cfg.Dependencies...)
		slices.Sort(deps)

		meta := TableMeta{
			TableName:            cfg.TableName,
			PushPolicy:           string(cfg.PushPolicy),
			Dependencies:         deps,
			ParentTable:          cfg.ParentTable,
			ParentFKCol:          cfg.ParentFKCol,
			UpdatedAtColumn:      cfg.UpdatedAtColumn,
			DeletedAtColumn:      cfg.DeletedAtColumn,
			BucketByColumn:       cfg.BucketByColumn,
			BucketPrefix:         cfg.BucketPrefix,
			GlobalWhenBucketNull: cfg.GlobalWhenBucketNull,
			AllowGlobalRead:      cfg.AllowGlobalRead,
			BucketFunction:       cfg.BucketFunction,
		}
		if meta.Dependencies == nil {
			meta.Dependencies = []string{}
		}
		tables = append(tables, meta)
	}

	return &TableMetaResponse{
		Tables:        tables,
		ServerTime:    time.Now().UTC(),
		SchemaVersion: manifest.Version,
		SchemaHash:    manifest.Hash,
	}, nil
}

// Schema returns the full server schema contract.
func (e *Engine) Schema(ctx context.Context) (*SchemaResponse, error) {
	return e.schema.GetSchema(ctx, e.db, e.registry)
}

// CurrentSchemaManifest returns current server schema version/hash.
func (e *Engine) CurrentSchemaManifest(ctx context.Context) (int64, string, error) {
	manifest, err := e.schema.GetManifest(ctx, e.db, e.registry)
	if err != nil {
		return 0, "", err
	}
	return manifest.Version, manifest.Hash, nil
}

// Snapshot performs a full snapshot for a client. Returns paginated results using a stateless cursor.
func (e *Engine) Snapshot(ctx context.Context, userID string, req *SnapshotRequest) (*SnapshotResponse, error) {
	manifest, err := e.schema.GetManifest(ctx, e.db, e.registry)
	if err != nil {
		return nil, err
	}
	if err := validateClientSchema(manifest, req.SchemaVersion, req.SchemaHash, false); err != nil {
		return nil, err
	}

	// Verify client is registered
	_, err = e.clients.GetClient(ctx, e.db, userID, req.ClientID)
	if err != nil {
		return nil, ErrClientNotRegistered
	}

	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("beginning snapshot transaction: %w", err)
	}
	defer tx.Rollback()

	if err := SetAuthContext(ctx, tx, userID); err != nil {
		return nil, fmt.Errorf("setting snapshot auth context: %w", err)
	}

	resp, err := e.pull.processSnapshot(ctx, tx, req.Cursor, req.Limit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing snapshot transaction: %w", err)
	}
	resp.SchemaVersion = manifest.Version
	resp.SchemaHash = manifest.Hash

	// When snapshot completes, advance client checkpoint and reactivate.
	if !resp.HasMore {
		if err := e.checkpoint.AdvanceCheckpoint(ctx, e.db, userID, req.ClientID, resp.Checkpoint); err != nil {
			e.logger.WarnContext(ctx, "failed to advance snapshot checkpoint",
				"err", err, "client_id", req.ClientID)
		}
		// Reactivate client (may have been deactivated by compactor).
		if _, err := e.db.ExecContext(ctx,
			"UPDATE sync_clients SET is_active = true, updated_at = now() WHERE user_id = $1 AND client_id = $2",
			userID, req.ClientID); err != nil {
			e.logger.WarnContext(ctx, "failed to reactivate client after snapshot",
				"err", err, "client_id", req.ClientID)
		}
	}

	return resp, nil
}

// RunCompaction runs a single compaction cycle. Returns an error if no
// Compactor was configured.
func (e *Engine) RunCompaction(ctx context.Context) (CompactResult, error) {
	if e.compactor == nil {
		return CompactResult{}, fmt.Errorf("synchro: compaction not configured")
	}

	result, err := e.compactor.RunCompaction(ctx, e.db)
	if err != nil {
		return result, err
	}

	if e.hooks.OnCompaction != nil {
		e.hooks.OnCompaction(ctx, result)
	}

	return result, nil
}

// StartCompaction runs compaction on a recurring interval in a background goroutine.
// The goroutine stops when ctx is cancelled. No-op if no Compactor was configured.
func (e *Engine) StartCompaction(ctx context.Context, interval time.Duration) {
	if e.compactor == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := e.compactor.RunCompaction(ctx, e.db)
				if err != nil {
					e.logger.ErrorContext(ctx, "compaction failed", "err", err)
					continue
				}
				if e.hooks.OnCompaction != nil {
					e.hooks.OnCompaction(ctx, result)
				}
			}
		}
	}()
}

// SchemaManifestHistory returns persisted manifest rows ordered by newest version first.
func (e *Engine) SchemaManifestHistory(ctx context.Context, limit int) ([]SchemaManifestEntry, error) {
	return e.schema.ListManifests(ctx, e.db, limit)
}

// diffBuckets compares known (client) buckets with current (server) buckets
// and returns a BucketUpdate describing additions and removals. Returns nil
// if there are no changes.
func diffBuckets(known, current []string) *BucketUpdate {
	knownSet := make(map[string]struct{}, len(known))
	for _, b := range known {
		knownSet[b] = struct{}{}
	}
	currentSet := make(map[string]struct{}, len(current))
	for _, b := range current {
		currentSet[b] = struct{}{}
	}

	var added, removed []string
	for _, b := range current {
		if _, ok := knownSet[b]; !ok {
			added = append(added, b)
		}
	}
	for _, b := range known {
		if _, ok := currentSet[b]; !ok {
			removed = append(removed, b)
		}
	}

	if len(added) == 0 && len(removed) == 0 {
		return nil
	}
	return &BucketUpdate{Added: added, Removed: removed}
}
