package synchro

import (
	"context"
	"encoding/json"
	"time"
)

// Conflict provides full context for conflict resolution.
type Conflict struct {
	Table       string
	RecordID    string
	ClientID    string
	UserID      string
	ClientData  json.RawMessage
	ServerData  json.RawMessage
	ClientTime  time.Time
	ServerTime  time.Time
	BaseVersion *time.Time
}

// Resolution describes how a conflict was resolved.
type Resolution struct {
	// Winner is "client" or "server".
	Winner string
	// Reason explains the resolution.
	Reason string
}

// ConflictResolver resolves conflicts between client and server data.
type ConflictResolver interface {
	Resolve(ctx context.Context, conflict Conflict) (Resolution, error)
}

// LWWResolver implements Last-Write-Wins conflict resolution.
// The client timestamp is adjusted by ClockSkewTolerance before comparison.
type LWWResolver struct {
	ClockSkewTolerance time.Duration
}

// Resolve implements ConflictResolver using LWW semantics.
func (r *LWWResolver) Resolve(_ context.Context, c Conflict) (Resolution, error) {
	clientTime := c.ClientTime.Add(r.ClockSkewTolerance)

	if c.BaseVersion != nil {
		// Optimistic: check if server changed since client's base
		if !c.BaseVersion.Equal(c.ServerTime) && !c.BaseVersion.After(c.ServerTime) {
			// Server modified since base — use LWW
			if clientTime.After(c.ServerTime) {
				return Resolution{Winner: "client", Reason: "client timestamp newer (LWW with base)"}, nil
			}
			return Resolution{Winner: "server", Reason: "server version is newer"}, nil
		}
		// Server unchanged since base — client wins
		return Resolution{Winner: "client", Reason: "server unchanged since base version"}, nil
	}

	// No base version — pure LWW
	if clientTime.After(c.ServerTime) {
		return Resolution{Winner: "client", Reason: "client timestamp newer (LWW)"}, nil
	}
	return Resolution{Winner: "server", Reason: "server version is newer"}, nil
}

// ServerWinsResolver always resolves in favor of the server.
type ServerWinsResolver struct{}

// Resolve implements ConflictResolver by always choosing the server.
func (r *ServerWinsResolver) Resolve(_ context.Context, _ Conflict) (Resolution, error) {
	return Resolution{Winner: "server", Reason: "server always wins"}, nil
}
