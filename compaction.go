package synchro

import "context"

// Compactor defines the interface for changelog compaction (Phase 2).
// Compaction removes changelog entries that all active clients have already consumed.
type Compactor interface {
	// Compact removes changelog entries with seq <= safeSeq.
	// safeSeq is the minimum last_pull_seq across all active clients.
	Compact(ctx context.Context, db DB, safeSeq int64) (removed int64, err error)

	// SafeSeq calculates the safe compaction boundary.
	SafeSeq(ctx context.Context, db DB) (int64, error)
}
