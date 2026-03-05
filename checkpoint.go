package synchro

import (
	"context"
	"database/sql"
	"fmt"
)

// checkpointStore tracks per-client pull progress.
type checkpointStore struct{}

// GetCheckpoint returns the last pulled sequence for a client.
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

// AdvanceCheckpoint updates the client's last pull sequence.
func (s *checkpointStore) AdvanceCheckpoint(ctx context.Context, db DB, userID, clientID string, seq int64) error {
	_, err := db.ExecContext(ctx,
		"UPDATE sync_clients SET last_pull_seq = $3, last_pull_at = now(), last_sync_at = now() WHERE user_id = $1 AND client_id = $2 AND (last_pull_seq IS NULL OR last_pull_seq < $3)",
		userID, clientID, seq)
	if err != nil {
		return fmt.Errorf("advancing checkpoint: %w", err)
	}
	return nil
}
