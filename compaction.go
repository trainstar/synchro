package synchro

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// CompactorConfig configures changelog compaction.
type CompactorConfig struct {
	// StaleThreshold is how long since last sync before a client is deactivated.
	// Default: 7 days.
	StaleThreshold time.Duration

	// BatchSize is the maximum rows deleted per batch. Default: 10000.
	BatchSize int

	// Logger for compaction operations. Falls back to slog.Default().
	Logger *slog.Logger
}

// CompactResult reports what a compaction run accomplished.
type CompactResult struct {
	DeactivatedClients int64
	SafeSeq            int64
	DeletedEntries     int64
}

// Compactor runs changelog compaction.
type Compactor struct {
	staleThreshold time.Duration
	batchSize      int
	logger         *slog.Logger
}

// NewCompactor creates a Compactor from the given config.
func NewCompactor(cfg *CompactorConfig) *Compactor {
	threshold := 7 * 24 * time.Hour
	batchSize := 10000
	logger := slog.Default()

	if cfg != nil {
		if cfg.StaleThreshold > 0 {
			threshold = cfg.StaleThreshold
		}
		if cfg.BatchSize > 0 {
			batchSize = cfg.BatchSize
		}
		if cfg.Logger != nil {
			logger = cfg.Logger
		}
	}

	return &Compactor{
		staleThreshold: threshold,
		batchSize:      batchSize,
		logger:         logger,
	}
}

// DeactivateStaleClients marks clients as inactive if they haven't synced within the threshold.
func (c *Compactor) DeactivateStaleClients(ctx context.Context, db DB) (int64, error) {
	result, err := db.ExecContext(ctx,
		`UPDATE sync_clients SET is_active = false, updated_at = now()
		 WHERE is_active = true
		   AND last_sync_at IS NOT NULL
		   AND last_sync_at < now() - $1::interval`,
		c.staleThreshold.String())
	if err != nil {
		return 0, fmt.Errorf("deactivating stale clients: %w", err)
	}
	return result.RowsAffected()
}

// SafeSeq returns the minimum checkpoint across all buckets for all active clients.
// Uses sync_client_checkpoints as the primary source (per-bucket cursors).
// Falls back to sync_clients.last_pull_seq for clients that haven't been
// upgraded to per-bucket checkpoints yet.
// Returns 0 if there are no active clients (nothing safe to compact).
func (c *Compactor) SafeSeq(ctx context.Context, db DB) (int64, error) {
	// Get minimum from per-bucket checkpoints
	var bucketMin int64
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(MIN(cp.checkpoint), 0)
		FROM sync_client_checkpoints cp
		JOIN sync_clients c ON c.user_id = cp.user_id AND c.client_id = cp.client_id
		WHERE c.is_active = true`).Scan(&bucketMin)
	if err != nil {
		return 0, fmt.Errorf("computing safe seq from bucket checkpoints: %w", err)
	}

	// Also check legacy last_pull_seq for clients without per-bucket checkpoints
	var legacyMin int64
	err = db.QueryRowContext(ctx,
		`SELECT COALESCE(MIN(c.last_pull_seq), 0)
		FROM sync_clients c
		WHERE c.is_active = true
		  AND c.last_pull_seq IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1 FROM sync_client_checkpoints cp
			WHERE cp.user_id = c.user_id AND cp.client_id = c.client_id
		  )`).Scan(&legacyMin)
	if err != nil {
		return 0, fmt.Errorf("computing safe seq from legacy checkpoints: %w", err)
	}

	// Return the minimum of both sources
	switch {
	case bucketMin == 0 && legacyMin == 0:
		return 0, nil
	case bucketMin == 0:
		return legacyMin, nil
	case legacyMin == 0:
		return bucketMin, nil
	default:
		if bucketMin < legacyMin {
			return bucketMin, nil
		}
		return legacyMin, nil
	}
}

// Compact deletes changelog entries with seq <= safeSeq in batches.
func (c *Compactor) Compact(ctx context.Context, db DB, safeSeq int64) (int64, error) {
	if safeSeq <= 0 {
		return 0, nil
	}

	var total int64
	for {
		result, err := db.ExecContext(ctx,
			`DELETE FROM sync_changelog WHERE seq IN (
				SELECT seq FROM sync_changelog WHERE seq <= $1 ORDER BY seq LIMIT $2
			)`, safeSeq, c.batchSize)
		if err != nil {
			return total, fmt.Errorf("compacting changelog: %w", err)
		}
		n, _ := result.RowsAffected()
		total += n
		if int(n) < c.batchSize {
			break
		}
	}
	return total, nil
}

// RunCompaction orchestrates a full compaction: deactivate stale clients,
// compute the safe boundary, then delete old changelog entries.
func (c *Compactor) RunCompaction(ctx context.Context, db DB) (CompactResult, error) {
	var result CompactResult

	deactivated, err := c.DeactivateStaleClients(ctx, db)
	if err != nil {
		return result, err
	}
	result.DeactivatedClients = deactivated

	safeSeq, err := c.SafeSeq(ctx, db)
	if err != nil {
		return result, err
	}
	result.SafeSeq = safeSeq

	deleted, err := c.Compact(ctx, db, safeSeq)
	if err != nil {
		return result, err
	}
	result.DeletedEntries = deleted

	c.logger.InfoContext(ctx, "compaction complete",
		"deactivated_clients", deactivated,
		"safe_seq", safeSeq,
		"deleted_entries", deleted)

	return result, nil
}
