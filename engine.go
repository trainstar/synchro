package synchro

import (
	"context"
	"database/sql"
	"errors"
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

	// Tables defines the tables to sync. Replaces Registry for new users.
	Tables []Table

	// Exclude lists global column exclusions applied to all tables.
	Exclude []string

	// AuthorizeWrite authorizes and optionally transforms push records.
	AuthorizeWrite AuthorizeWriteFunc

	// BucketFunc determines which buckets a record belongs to.
	BucketFunc BucketFunc

	// Hooks for lifecycle callbacks.
	Hooks Hooks

	// ConflictResolver resolves push conflicts. Defaults to LWW.
	ConflictResolver ConflictResolver

	// MinClientVersion is the minimum supported client version (semver).
	MinClientVersion string

	// ClockSkewTolerance is added to client timestamps during LWW comparison.
	ClockSkewTolerance time.Duration

	// Compactor configures changelog compaction. Optional.
	Compactor *CompactorConfig

	// UpdatedAtColumn is the column name convention for change tracking.
	// Introspected per table. Default: "updated_at".
	UpdatedAtColumn string

	// DeletedAtColumn is the column name convention for soft deletes.
	// Introspected per table. Default: "deleted_at".
	DeletedAtColumn string

	// Logger for sync operations. Defaults to slog.Default().
	Logger *slog.Logger
}

// Engine is the top-level sync orchestrator.
type Engine struct {
	db             *sql.DB
	registry       *Registry
	resolver       ConflictResolver
	authorizeWrite AuthorizeWriteFunc
	bucketFunc     BucketFunc
	hooks          Hooks
	push           *pushProcessor
	pull           *pullProcessor
	clients        *clientStore
	changelog      *changelogStore
	checkpoint     *checkpointStore
	schema         *schemaStore
	compactor      *Compactor
	config         Config
	logger         *slog.Logger
}

// NewEngine creates a new sync engine from the given configuration.
func NewEngine(ctx context.Context, cfg *Config) (*Engine, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("synchro: DB is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, fmt.Errorf("synchro: Tables is required")
	}

	if cfg.UpdatedAtColumn == "" {
		cfg.UpdatedAtColumn = "updated_at"
	}
	if cfg.DeletedAtColumn == "" {
		cfg.DeletedAtColumn = "deleted_at"
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Ensure infrastructure tables have all required columns.
	// This auto-adds columns introduced in newer library versions so the
	// consuming application never needs to re-run Migrations().
	if err := ensureInfraSchema(ctx, cfg.DB, logger); err != nil {
		return nil, fmt.Errorf("synchro: %w", err)
	}

	// Build registry from Tables.
	registry := NewRegistry()
	for _, t := range cfg.Tables {
		registry.registerTable(t, cfg.Exclude)
	}

	if err := registry.Validate(); err != nil {
		return nil, fmt.Errorf("synchro: registry validation failed: %w", err)
	}

	// Introspect: timestamps + PKs + FKs + nullability + dependency order.
	if err := registry.Introspect(ctx, cfg.DB, cfg.UpdatedAtColumn, cfg.DeletedAtColumn); err != nil {
		return nil, fmt.Errorf("synchro: %w", err)
	}
	for _, tcfg := range registry.All() {
		logger.Info("table introspected", "table", tcfg.TableName,
			"updated_at", tcfg.HasUpdatedAt(), "deleted_at", tcfg.HasDeletedAt(), "created_at", tcfg.HasCreatedAt(),
			"parent", tcfg.parentTable, "parent_fk", tcfg.parentFKCol)
	}

	resolver := cfg.ConflictResolver
	if resolver == nil {
		resolver = &LWWResolver{ClockSkewTolerance: cfg.ClockSkewTolerance}
	}

	cl := &changelogStore{}
	cp := &checkpointStore{}
	schema := &schemaStore{}

	var compactor *Compactor
	if cfg.Compactor != nil {
		compactor = NewCompactor(cfg.Compactor)
	}

	e := &Engine{
		db:             cfg.DB,
		registry:       registry,
		resolver:       resolver,
		authorizeWrite: cfg.AuthorizeWrite,
		bucketFunc:     cfg.BucketFunc,
		hooks:          cfg.Hooks,
		clients:        &clientStore{},
		changelog:      cl,
		checkpoint:     cp,
		schema:         schema,
		compactor:      compactor,
		config:         *cfg,
		logger:         logger,
		push: &pushProcessor{
			registry:       registry,
			resolver:       resolver,
			authorizeWrite: cfg.AuthorizeWrite,
			changelog:      cl,
			hooks:          cfg.Hooks,
			logger:         logger,
		},
		pull: &pullProcessor{
			registry:   registry,
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

	// Initialize per-bucket checkpoints for subscribed buckets
	buckets := client.BucketSubs
	if len(buckets) == 0 {
		buckets = []string{fmt.Sprintf("user:%s", userID), "global"}
	}
	if err := e.checkpoint.InitBucketCheckpoints(ctx, e.db, userID, req.ClientID, buckets); err != nil {
		e.logger.WarnContext(ctx, "failed to init bucket checkpoints",
			"err", err, "client_id", req.ClientID)
	}

	// Fetch current bucket checkpoints
	bucketCheckpoints, err := e.checkpoint.GetBucketCheckpoints(ctx, e.db, userID, req.ClientID)
	if err != nil {
		e.logger.WarnContext(ctx, "failed to get bucket checkpoints for registration response",
			"err", err, "client_id", req.ClientID)
	}

	return &RegisterResponse{
		ID:                client.ID,
		ServerTime:        time.Now().UTC(),
		LastSyncAt:        client.LastSyncAt,
		Checkpoint:        lastPullSeq,
		BucketCheckpoints: bucketCheckpoints,
		SchemaVersion:     manifest.Version,
		SchemaHash:        manifest.Hash,
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

	// Get client's bucket subscriptions
	buckets := client.BucketSubs
	if len(buckets) == 0 {
		buckets = []string{fmt.Sprintf("user:%s", userID), "global"}
	}

	// Detect per-bucket vs legacy mode
	isPerBucket := req.BucketCheckpoints != nil && len(req.BucketCheckpoints) > 0

	if isPerBucket {
		return e.pullPerBucket(ctx, userID, req, client, buckets, manifest)
	}
	return e.pullLegacy(ctx, userID, req, client, buckets, manifest)
}

// pullPerBucket handles pull for clients using per-bucket checkpoints.
func (e *Engine) pullPerBucket(ctx context.Context, userID string, req *PullRequest, client *Client, buckets []string, manifest schemaManifest) (*PullResponse, error) {
	// Detect stale buckets that need rebuild
	staleBuckets, err := e.pull.detectStaleBuckets(ctx, e.db, req.BucketCheckpoints, buckets)
	if err != nil {
		return nil, fmt.Errorf("detecting stale buckets: %w", err)
	}

	if len(staleBuckets) > 0 {
		// Fire hooks
		for _, bid := range staleBuckets {
			if e.hooks.OnBucketRebuild != nil {
				e.hooks.OnBucketRebuild(ctx, req.ClientID, bid)
			}
		}

		return &PullResponse{
			Changes:           []Record{},
			Deletes:           []DeleteEntry{},
			Checkpoint:        req.Checkpoint,
			BucketCheckpoints: req.BucketCheckpoints,
			RebuildBuckets:    staleBuckets,
			HasMore:           false,
			SchemaVersion:     manifest.Version,
			SchemaHash:        manifest.Hash,
		}, nil
	}

	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("beginning pull transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := SetAuthContext(ctx, tx, userID); err != nil {
		return nil, fmt.Errorf("setting pull auth context: %w", err)
	}

	resp, err := e.pull.processPerBucketPull(ctx, tx, req, buckets)
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

	// Advance per-bucket checkpoints
	if resp.BucketCheckpoints != nil {
		if err := e.checkpoint.AdvanceBucketCheckpoints(ctx, e.db, userID, req.ClientID, resp.BucketCheckpoints); err != nil {
			e.logger.WarnContext(ctx, "failed to advance bucket checkpoints",
				"err", err, "client_id", req.ClientID)
		}
	}

	// Also advance legacy checkpoint for backwards compat
	if resp.Checkpoint > req.Checkpoint {
		if err := e.checkpoint.AdvanceCheckpoint(ctx, e.db, userID, req.ClientID, resp.Checkpoint); err != nil {
			e.logger.WarnContext(ctx, "failed to advance legacy checkpoint",
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

// pullLegacy handles pull for clients using the legacy single checkpoint.
func (e *Engine) pullLegacy(ctx context.Context, userID string, req *PullRequest, client *Client, buckets []string, manifest schemaManifest) (*PullResponse, error) {
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

	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("beginning pull transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

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

// Rebuild performs a per-bucket rebuild for a client. Returns paginated results
// from sync_bucket_edges + app table hydration.
func (e *Engine) Rebuild(ctx context.Context, userID string, req *RebuildRequest) (*RebuildResponse, error) {
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
		return nil, fmt.Errorf("beginning rebuild transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := SetAuthContext(ctx, tx, userID); err != nil {
		return nil, fmt.Errorf("setting rebuild auth context: %w", err)
	}

	resp, err := e.pull.processRebuild(ctx, tx, req.BucketID, req.Cursor, req.Limit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing rebuild transaction: %w", err)
	}
	resp.SchemaVersion = manifest.Version
	resp.SchemaHash = manifest.Hash

	// When rebuild completes, set this bucket's checkpoint
	if !resp.HasMore {
		if err := e.checkpoint.SetBucketCheckpoint(ctx, e.db, userID, req.ClientID, req.BucketID, resp.Checkpoint); err != nil {
			e.logger.WarnContext(ctx, "failed to set bucket checkpoint after rebuild",
				"err", err, "client_id", req.ClientID, "bucket_id", req.BucketID)
		}
		// Also update legacy checkpoint for backwards compat
		if err := e.checkpoint.AdvanceCheckpoint(ctx, e.db, userID, req.ClientID, resp.Checkpoint); err != nil {
			e.logger.WarnContext(ctx, "failed to advance legacy checkpoint after rebuild",
				"err", err, "client_id", req.ClientID)
		}
		// Reactivate client (may have been deactivated by compactor).
		if _, err := e.db.ExecContext(ctx,
			"UPDATE sync_clients SET is_active = true, updated_at = now() WHERE user_id = $1 AND client_id = $2",
			userID, req.ClientID); err != nil {
			e.logger.WarnContext(ctx, "failed to reactivate client after rebuild",
				"err", err, "client_id", req.ClientID)
		}
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
	defer func() { _ = tx.Rollback() }()

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
		// Compute push_policy by probing AuthorizeWrite.
		pushPolicy := "enabled"
		if e.authorizeWrite != nil {
			testRecord := &PushRecord{
				TableName: cfg.TableName,
				ID:        "__probe__",
				Operation: "create",
			}
			_, err := e.authorizeWrite(ctx, "__probe__", testRecord)
			if err != nil && errors.Is(err, ErrTableReadOnly) {
				pushPolicy = "disabled"
			}
		}

		// Compute dependencies from FK graph.
		var deps []string
		for _, fk := range cfg.foreignKeys {
			deps = append(deps, fk.RefTable)
		}
		slices.Sort(deps)

		meta := TableMeta{
			TableName:       cfg.TableName,
			PushPolicy:      pushPolicy,
			Dependencies:    deps,
			ParentTable:     cfg.parentTable,
			ParentFKCol:     cfg.parentFKCol,
			UpdatedAtColumn: cfg.UpdatedAtCol(),
			DeletedAtColumn: cfg.DeletedAtCol(),
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
// Deprecated: Use Rebuild for per-bucket reconstruction. Kept for legacy client compatibility.
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
	defer func() { _ = tx.Rollback() }()

	if err := SetAuthContext(ctx, tx, userID); err != nil {
		return nil, fmt.Errorf("setting snapshot auth context: %w", err)
	}

	resp, err := e.pull.processSnapshot(ctx, tx, userID, req.Cursor, req.Limit)
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

// ClientDebugInfo returns diagnostic information about a sync client for
// support and debugging. Includes client registration state, per-bucket
// checkpoints, membership counts, checksums, and changelog statistics.
func (e *Engine) ClientDebugInfo(ctx context.Context, userID, clientID string) (*ClientDebugResponse, error) {
	client, err := e.clients.GetClient(ctx, e.db, userID, clientID)
	if err != nil {
		return nil, ErrClientNotRegistered
	}

	// Build client info
	info := ClientDebugInfo{
		ID:         client.ID,
		ClientID:   client.ClientID,
		Platform:   client.Platform,
		AppVersion: client.AppVersion,
		IsActive:   client.IsActive,
		LastSyncAt: client.LastSyncAt,
		LastPullAt: client.LastPullAt,
		LastPushAt: client.LastPushAt,
		BucketSubs: client.BucketSubs,
	}
	if client.LastPullSeq != nil {
		info.LegacyCheckpoint = client.LastPullSeq
	}
	if info.BucketSubs == nil {
		info.BucketSubs = []string{}
	}

	// Per-bucket checkpoints
	bucketCheckpoints, err := e.checkpoint.GetBucketCheckpoints(ctx, e.db, userID, clientID)
	if err != nil {
		return nil, fmt.Errorf("getting bucket checkpoints for debug: %w", err)
	}

	// Per-bucket membership counts and checksums
	var buckets []ServerBucketDebug
	for bucketID, cp := range bucketCheckpoints {
		bd := ServerBucketDebug{
			BucketID:   bucketID,
			Checkpoint: cp,
		}

		// Member count
		var count int
		err := e.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sync_bucket_edges WHERE bucket_id = $1",
			bucketID).Scan(&count)
		if err == nil {
			bd.MemberCount = count
		}

		// Bucket checksum via BIT_XOR (column is INTEGER, native int4).
		var xorVal sql.NullInt32
		err = e.db.QueryRowContext(ctx,
			"SELECT BIT_XOR(checksum) FROM sync_bucket_edges WHERE bucket_id = $1 AND checksum IS NOT NULL",
			bucketID).Scan(&xorVal)
		if err == nil && xorVal.Valid {
			cs := xorVal.Int32
			bd.Checksum = &cs
		}

		buckets = append(buckets, bd)
	}
	if buckets == nil {
		buckets = []ServerBucketDebug{}
	}

	// Changelog statistics
	var stats ChangelogDebugStats
	_ = e.db.QueryRowContext(ctx,
		"SELECT COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0), COUNT(*) FROM sync_changelog",
	).Scan(&stats.MinSeq, &stats.MaxSeq, &stats.TotalEntries)

	return &ClientDebugResponse{
		Client:         info,
		Buckets:        buckets,
		ChangelogStats: stats,
		ServerTime:     time.Now().UTC(),
	}, nil
}

// NewBucketAssigner returns an adapter wrapping BucketFunc + Registry for WAL consumer use.
func (e *Engine) NewBucketAssigner() *BucketAssignerAdapter {
	return &BucketAssignerAdapter{
		resolver: NewJoinResolverWithDB(e.registry, e.db, e.bucketFunc),
	}
}

// BucketAssignerAdapter wraps BucketFunc + JoinResolver for WAL consumer bucket assignment.
// Delegates FK chain resolution to JoinResolver to avoid duplicating that logic.
type BucketAssignerAdapter struct {
	resolver *JoinResolver
}

// AssignBuckets implements wal.BucketAssigner by delegating to JoinResolver.
func (a *BucketAssignerAdapter) AssignBuckets(ctx context.Context, table string, recordID string, operation Operation, data map[string]any) ([]string, error) {
	return a.resolver.AssignBuckets(ctx, table, recordID, operation, data)
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
