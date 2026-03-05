package synchro

import (
	"context"
	"database/sql"
	"time"
)

// AcceptedRecord contains information about a successfully pushed record.
type AcceptedRecord struct {
	ID        string
	TableName string
	Operation Operation
}

// Hooks defines lifecycle callbacks for sync operations.
// All hooks are optional — nil hooks are skipped.
type Hooks struct {
	// OnPushAccepted is called within the push transaction after records are applied.
	// Use this for side effects like rebuilding search indexes.
	OnPushAccepted func(ctx context.Context, tx *sql.Tx, accepted []AcceptedRecord) error

	// OnConflict is called when a conflict is detected and resolved.
	// This is informational — it cannot change the resolution.
	OnConflict func(ctx context.Context, conflict Conflict, resolution Resolution)

	// OnPullComplete is called after a successful pull operation.
	OnPullComplete func(ctx context.Context, clientID string, checkpoint int64, count int)

	// OnSchemaIncompatible is called when a client's version is below minimum.
	OnSchemaIncompatible func(ctx context.Context, clientID string, clientVer string, minVer string)

	// OnStaleClient is called when a client hasn't synced recently.
	// Return true to allow the sync, false to reject with ErrStaleClient.
	OnStaleClient func(ctx context.Context, clientID string, lastSync time.Time) bool
}
