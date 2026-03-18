package synchro

import (
	"context"
	"database/sql"
	"fmt"
)

// checkpointStore tracks per-client pull progress.
type checkpointStore struct{}

// GetCheckpoint returns the last pulled sequence for a client (legacy single-checkpoint).
func (s *checkpointStore) GetCheckpoint(ctx context.Context, db DB, userID, clientID string) (int64, error) {
	var seq int64
	err := db.QueryRowContext(ctx,
		"SELECT last_pull_seq FROM sync_clients WHERE user_id = $1 AND client_id = $2",
		userID, clientID).Scan(&seq)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("getting checkpoint: %w", err)
	}
	return seq, nil
}

// AdvanceCheckpoint updates the client's last pull sequence (legacy single-checkpoint).
// Also advances all per-bucket checkpoints to keep the two systems in sync.
func (s *checkpointStore) AdvanceCheckpoint(ctx context.Context, db DB, userID, clientID string, seq int64) error {
	_, err := db.ExecContext(ctx,
		"UPDATE sync_clients SET last_pull_seq = $3, last_pull_at = now(), last_sync_at = now() WHERE user_id = $1 AND client_id = $2 AND (last_pull_seq IS NULL OR last_pull_seq < $3)",
		userID, clientID, seq)
	if err != nil {
		return fmt.Errorf("advancing checkpoint: %w", err)
	}
	// Keep per-bucket checkpoints in sync with the legacy checkpoint.
	_, err = db.ExecContext(ctx,
		`UPDATE sync_client_checkpoints
		SET checkpoint = $3, updated_at = now()
		WHERE user_id = $1 AND client_id = $2 AND checkpoint < $3`,
		userID, clientID, seq)
	if err != nil {
		return fmt.Errorf("advancing bucket checkpoints from legacy: %w", err)
	}
	return nil
}

// GetBucketCheckpoints returns per-bucket checkpoints for a client.
func (s *checkpointStore) GetBucketCheckpoints(ctx context.Context, db DB, userID, clientID string) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT bucket_id, checkpoint FROM sync_client_checkpoints WHERE user_id = $1 AND client_id = $2",
		userID, clientID)
	if err != nil {
		return nil, fmt.Errorf("getting bucket checkpoints: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]int64)
	for rows.Next() {
		var bucketID string
		var cp int64
		if err := rows.Scan(&bucketID, &cp); err != nil {
			return nil, fmt.Errorf("scanning bucket checkpoint: %w", err)
		}
		result[bucketID] = cp
	}
	return result, rows.Err()
}

// AdvanceBucketCheckpoint updates a single bucket's checkpoint for a client.
// Only advances if the new seq is greater than the current value.
func (s *checkpointStore) AdvanceBucketCheckpoint(ctx context.Context, db DB, userID, clientID, bucketID string, seq int64) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO sync_client_checkpoints (user_id, client_id, bucket_id, checkpoint)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, client_id, bucket_id) DO UPDATE
		SET checkpoint = EXCLUDED.checkpoint, updated_at = now()
		WHERE sync_client_checkpoints.checkpoint < EXCLUDED.checkpoint`,
		userID, clientID, bucketID, seq)
	if err != nil {
		return fmt.Errorf("advancing bucket checkpoint: %w", err)
	}
	return nil
}

// AdvanceBucketCheckpoints updates multiple bucket checkpoints atomically.
func (s *checkpointStore) AdvanceBucketCheckpoints(ctx context.Context, db DB, userID, clientID string, checkpoints map[string]int64) error {
	for bucketID, seq := range checkpoints {
		if err := s.AdvanceBucketCheckpoint(ctx, db, userID, clientID, bucketID, seq); err != nil {
			return err
		}
	}
	return nil
}

// SetBucketCheckpoint sets a bucket's checkpoint to an exact value (used after rebuild).
func (s *checkpointStore) SetBucketCheckpoint(ctx context.Context, db DB, userID, clientID, bucketID string, seq int64) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO sync_client_checkpoints (user_id, client_id, bucket_id, checkpoint)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, client_id, bucket_id) DO UPDATE
		SET checkpoint = EXCLUDED.checkpoint, updated_at = now()`,
		userID, clientID, bucketID, seq)
	if err != nil {
		return fmt.Errorf("setting bucket checkpoint: %w", err)
	}
	return nil
}

// InitBucketCheckpoints creates initial checkpoint rows (checkpoint=0) for each
// subscribed bucket. Used during client registration. Existing rows are not overwritten.
func (s *checkpointStore) InitBucketCheckpoints(ctx context.Context, db DB, userID, clientID string, bucketIDs []string) error {
	for _, bucketID := range bucketIDs {
		_, err := db.ExecContext(ctx,
			`INSERT INTO sync_client_checkpoints (user_id, client_id, bucket_id, checkpoint)
			VALUES ($1, $2, $3, 0)
			ON CONFLICT (user_id, client_id, bucket_id) DO NOTHING`,
			userID, clientID, bucketID)
		if err != nil {
			return fmt.Errorf("initializing bucket checkpoint for %q: %w", bucketID, err)
		}
	}
	return nil
}

// MinBucketCheckpoint returns the minimum checkpoint across all buckets
// for all active clients. Used by compaction to determine the safe-seq.
func (s *checkpointStore) MinBucketCheckpoint(ctx context.Context, db DB) (int64, error) {
	var seq int64
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(MIN(cp.checkpoint), 0)
		FROM sync_client_checkpoints cp
		JOIN sync_clients c ON c.user_id = cp.user_id AND c.client_id = cp.client_id
		WHERE c.is_active = true`).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("computing min bucket checkpoint: %w", err)
	}
	return seq, nil
}
