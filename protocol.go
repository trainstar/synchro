package synchro

import (
	"encoding/json"
	"time"
)

// Operation represents the type of change in a sync operation.
type Operation int

const (
	OpInsert Operation = 1
	OpUpdate Operation = 2
	OpDelete Operation = 3
)

// String returns the string representation of an Operation.
func (o Operation) String() string {
	switch o {
	case OpInsert:
		return "create"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// ParseOperation converts a string to an Operation.
func ParseOperation(s string) (Operation, bool) {
	switch s {
	case "create":
		return OpInsert, true
	case "update":
		return OpUpdate, true
	case "delete":
		return OpDelete, true
	default:
		return 0, false
	}
}

// Record represents a single synced record.
type Record struct {
	ID        string          `json:"id"`
	TableName string          `json:"table_name"`
	Data      json.RawMessage `json:"data"`
	UpdatedAt time.Time       `json:"updated_at"`
	DeletedAt *time.Time      `json:"deleted_at,omitempty"`
}

// RegisterRequest is the request body for client registration.
type RegisterRequest struct {
	ClientID   string  `json:"client_id"`
	ClientName *string `json:"client_name,omitempty"`
	Platform   string  `json:"platform"`
	AppVersion string  `json:"app_version"`
}

// RegisterResponse is the response for client registration.
type RegisterResponse struct {
	ID         string     `json:"id"`
	ServerTime time.Time  `json:"server_time"`
	LastSyncAt *time.Time `json:"last_sync_at,omitempty"`
	Checkpoint int64      `json:"checkpoint"`
}

// PullRequest is the request for pulling changes.
type PullRequest struct {
	ClientID     string   `json:"client_id"`
	Checkpoint   int64    `json:"checkpoint"`
	Tables       []string `json:"tables,omitempty"`
	Limit        int      `json:"limit,omitempty"`
	KnownBuckets []string `json:"known_buckets,omitempty"`
}

// DefaultPullLimit is the default number of records per pull.
const DefaultPullLimit = 100

// MaxPullLimit is the maximum records per pull.
const MaxPullLimit = 1000

// PullResponse is the response for pulling changes.
type PullResponse struct {
	Changes       []Record      `json:"changes"`
	Deletes       []DeleteEntry `json:"deletes"`
	Checkpoint    int64         `json:"checkpoint"`
	HasMore       bool          `json:"has_more"`
	BucketUpdates *BucketUpdate `json:"bucket_updates,omitempty"`
}

// DeleteEntry represents a deleted record in a pull response.
type DeleteEntry struct {
	ID        string `json:"id"`
	TableName string `json:"table_name"`
}

// BucketUpdate describes changes to a client's bucket subscriptions.
type BucketUpdate struct {
	Added   []string `json:"added,omitempty"`
	Removed []string `json:"removed,omitempty"`
}

// PushRequest is the request body for pushing changes.
type PushRequest struct {
	ClientID string       `json:"client_id"`
	Changes  []PushRecord `json:"changes"`
}

// PushRecord represents a single record being pushed from the client.
type PushRecord struct {
	ID              string          `json:"id"`
	TableName       string          `json:"table_name"`
	Operation       string          `json:"operation"`
	Data            json.RawMessage `json:"data,omitempty"`
	ClientUpdatedAt time.Time       `json:"client_updated_at"`
	BaseUpdatedAt   *time.Time      `json:"base_updated_at,omitempty"`
}

// PushResponse is the response for pushing changes.
type PushResponse struct {
	Accepted   []PushResult `json:"accepted"`
	Rejected   []PushResult `json:"rejected"`
	Checkpoint int64        `json:"checkpoint"`
	ServerTime time.Time    `json:"server_time"`
}

// PushResult represents the result of processing a single push record.
type PushResult struct {
	ID            string  `json:"id"`
	TableName     string  `json:"table_name"`
	Operation     string  `json:"operation"`
	Status        string  `json:"status"`
	Reason        string  `json:"reason,omitempty"`
	ServerVersion *Record `json:"server_version,omitempty"`
}

// Push status constants.
const (
	PushStatusApplied  = "applied"
	PushStatusConflict = "conflict"
	PushStatusError    = "error"
)

// TableMeta describes a single table's sync configuration.
type TableMeta struct {
	TableName    string   `json:"table_name"`
	Direction    string   `json:"direction"`
	Dependencies []string `json:"dependencies"`
	ParentTable  string   `json:"parent_table,omitempty"`
}

// TableMetaResponse is the response for the table metadata endpoint.
type TableMetaResponse struct {
	Tables     []TableMeta `json:"tables"`
	ServerTime time.Time   `json:"server_time"`
}
