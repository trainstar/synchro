package synchro

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Client represents a registered sync client device.
type Client struct {
	ID          string     `json:"id"`
	UserID      string     `json:"user_id"`
	ClientID    string     `json:"client_id"`
	ClientName  *string    `json:"client_name,omitempty"`
	Platform    string     `json:"platform"`
	AppVersion  string     `json:"app_version"`
	BucketSubs  []string   `json:"bucket_subs,omitempty"`
	LastSyncAt  *time.Time `json:"last_sync_at,omitempty"`
	LastPullAt  *time.Time `json:"last_pull_at,omitempty"`
	LastPushAt  *time.Time `json:"last_push_at,omitempty"`
	LastPullSeq *int64     `json:"last_pull_seq,omitempty"`
	IsActive    bool       `json:"is_active"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// clientStore handles client registration and management.
type clientStore struct{}

// RegisterClient registers or updates a sync client.
func (s *clientStore) RegisterClient(ctx context.Context, db DB, userID string, req *RegisterRequest) (*Client, error) {
	userBucket := fmt.Sprintf("user:%s", userID)

	query := `
		INSERT INTO sync_clients (user_id, client_id, client_name, platform, app_version, bucket_subs, is_active)
		VALUES ($1, $2, $3, $4, $5, ARRAY[$6::text, 'global'], true)
		ON CONFLICT (user_id, client_id) DO UPDATE SET
			client_name = EXCLUDED.client_name,
			platform = EXCLUDED.platform,
			app_version = EXCLUDED.app_version,
			is_active = true,
			updated_at = now()
		RETURNING id, user_id, client_id, client_name, platform, app_version,
			array_to_string(bucket_subs, ',') as bucket_subs_csv,
			last_sync_at, last_pull_at, last_push_at, last_pull_seq,
			is_active, created_at, updated_at
	`

	var c Client
	var clientName sql.NullString
	var lastSyncAt, lastPullAt, lastPushAt sql.NullTime
	var lastPullSeq sql.NullInt64
	var bucketSubsCSV sql.NullString

	err := db.QueryRowContext(ctx, query,
		userID, req.ClientID, req.ClientName, req.Platform, req.AppVersion, userBucket,
	).Scan(
		&c.ID, &c.UserID, &c.ClientID, &clientName, &c.Platform, &c.AppVersion,
		&bucketSubsCSV,
		&lastSyncAt, &lastPullAt, &lastPushAt, &lastPullSeq,
		&c.IsActive, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("registering client: %w", err)
	}

	if clientName.Valid {
		c.ClientName = &clientName.String
	}
	if lastSyncAt.Valid {
		c.LastSyncAt = &lastSyncAt.Time
	}
	if lastPullAt.Valid {
		c.LastPullAt = &lastPullAt.Time
	}
	if lastPushAt.Valid {
		c.LastPushAt = &lastPushAt.Time
	}
	if lastPullSeq.Valid {
		c.LastPullSeq = &lastPullSeq.Int64
	}
	if bucketSubsCSV.Valid && bucketSubsCSV.String != "" {
		c.BucketSubs = strings.Split(bucketSubsCSV.String, ",")
	}

	return &c, nil
}

// GetClient retrieves a registered client.
func (s *clientStore) GetClient(ctx context.Context, db DB, userID, clientID string) (*Client, error) {
	query := `
		SELECT id, user_id, client_id, client_name, platform, app_version,
			array_to_string(bucket_subs, ',') as bucket_subs_csv,
			last_sync_at, last_pull_at, last_push_at, last_pull_seq,
			is_active, created_at, updated_at
		FROM sync_clients
		WHERE user_id = $1 AND client_id = $2
	`

	var c Client
	var clientName sql.NullString
	var lastSyncAt, lastPullAt, lastPushAt sql.NullTime
	var lastPullSeq sql.NullInt64
	var bucketSubsCSV sql.NullString

	err := db.QueryRowContext(ctx, query, userID, clientID).Scan(
		&c.ID, &c.UserID, &c.ClientID, &clientName, &c.Platform, &c.AppVersion,
		&bucketSubsCSV,
		&lastSyncAt, &lastPullAt, &lastPushAt, &lastPullSeq,
		&c.IsActive, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrRecordNotFound
		}
		return nil, fmt.Errorf("getting client: %w", err)
	}

	if clientName.Valid {
		c.ClientName = &clientName.String
	}
	if lastSyncAt.Valid {
		c.LastSyncAt = &lastSyncAt.Time
	}
	if lastPullAt.Valid {
		c.LastPullAt = &lastPullAt.Time
	}
	if lastPushAt.Valid {
		c.LastPushAt = &lastPushAt.Time
	}
	if lastPullSeq.Valid {
		c.LastPullSeq = &lastPullSeq.Int64
	}
	if bucketSubsCSV.Valid && bucketSubsCSV.String != "" {
		c.BucketSubs = strings.Split(bucketSubsCSV.String, ",")
	}

	return &c, nil
}

// GetBucketSubs returns the bucket subscriptions for a client.
func (s *clientStore) GetBucketSubs(ctx context.Context, db DB, userID, clientID string) ([]string, error) {
	query := `SELECT array_to_string(bucket_subs, ',') FROM sync_clients WHERE user_id = $1 AND client_id = $2`

	var csv sql.NullString
	err := db.QueryRowContext(ctx, query, userID, clientID).Scan(&csv)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrClientNotRegistered
		}
		return nil, fmt.Errorf("getting bucket subs: %w", err)
	}

	if !csv.Valid || csv.String == "" {
		return []string{fmt.Sprintf("user:%s", userID), "global"}, nil
	}

	return strings.Split(csv.String, ","), nil
}

// UpdateLastSync updates the last sync timestamp for a client.
func (s *clientStore) UpdateLastSync(ctx context.Context, db DB, userID, clientID, syncType string) error {
	var column string
	switch syncType {
	case "pull":
		column = "last_pull_at"
	case "push":
		column = "last_push_at"
	default:
		column = "last_sync_at"
	}

	query := fmt.Sprintf(`
		UPDATE sync_clients
		SET %s = now(), last_sync_at = now()
		WHERE user_id = $1 AND client_id = $2
	`, quoteIdentifier(column))

	_, err := db.ExecContext(ctx, query, userID, clientID)
	if err != nil {
		return fmt.Errorf("updating client last sync: %w", err)
	}
	return nil
}
