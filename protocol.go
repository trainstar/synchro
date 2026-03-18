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
	BucketID  string          `json:"bucket_id,omitempty"`
	Data      json.RawMessage `json:"data"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
	DeletedAt *time.Time      `json:"deleted_at,omitempty"`
	Checksum  *int32          `json:"checksum,omitempty"`
}

// RegisterRequest is the request body for client registration.
type RegisterRequest struct {
	ClientID      string  `json:"client_id"`
	ClientName    *string `json:"client_name,omitempty"`
	Platform      string  `json:"platform"`
	AppVersion    string  `json:"app_version"`
	SchemaVersion int64   `json:"schema_version,omitempty"`
	SchemaHash    string  `json:"schema_hash,omitempty"`
}

// RegisterResponse is the response for client registration.
type RegisterResponse struct {
	ID                string           `json:"id"`
	ServerTime        time.Time        `json:"server_time"`
	LastSyncAt        *time.Time       `json:"last_sync_at,omitempty"`
	Checkpoint        int64            `json:"checkpoint"`
	BucketCheckpoints map[string]int64 `json:"bucket_checkpoints,omitempty"`
	SchemaVersion     int64            `json:"schema_version"`
	SchemaHash        string           `json:"schema_hash"`
}

// PullRequest is the request for pulling changes.
type PullRequest struct {
	ClientID          string           `json:"client_id"`
	Checkpoint        int64            `json:"checkpoint,omitempty"`
	BucketCheckpoints map[string]int64 `json:"bucket_checkpoints,omitempty"`
	Tables            []string         `json:"tables,omitempty"`
	Limit             int              `json:"limit,omitempty"`
	KnownBuckets      []string         `json:"known_buckets,omitempty"`
	SchemaVersion     int64            `json:"schema_version,omitempty"`
	SchemaHash        string           `json:"schema_hash,omitempty"`
}

// DefaultPullLimit is the default number of records per pull.
const DefaultPullLimit = 100

// MaxPullLimit is the maximum records per pull.
const MaxPullLimit = 1000

// PullResponse is the response for pulling changes.
type PullResponse struct {
	Changes           []Record            `json:"changes"`
	Deletes           []DeleteEntry       `json:"deletes"`
	Checkpoint        int64               `json:"checkpoint"`
	BucketCheckpoints map[string]int64    `json:"bucket_checkpoints,omitempty"`
	HasMore           bool                `json:"has_more"`
	RebuildBuckets    []string            `json:"rebuild_buckets,omitempty"`
	BucketChecksums   map[string]int32    `json:"bucket_checksums,omitempty"`
	SnapshotRequired  bool                `json:"snapshot_required,omitempty"`
	SnapshotReason    string              `json:"snapshot_reason,omitempty"`
	BucketUpdates     *BucketUpdate       `json:"bucket_updates,omitempty"`
	SchemaVersion     int64               `json:"schema_version"`
	SchemaHash        string              `json:"schema_hash"`
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
	ClientID      string       `json:"client_id"`
	Changes       []PushRecord `json:"changes"`
	SchemaVersion int64        `json:"schema_version,omitempty"`
	SchemaHash    string       `json:"schema_hash,omitempty"`
}

// PushRecord represents a single record being pushed from the client.
type PushRecord struct {
	ID              string          `json:"id"`
	TableName       string          `json:"table_name"`
	Operation       string          `json:"operation"`
	Data            json.RawMessage `json:"data,omitempty"`
	ClientUpdatedAt time.Time       `json:"client_updated_at"`
	BaseUpdatedAt   *time.Time      `json:"base_updated_at,omitempty"`

	// Existing holds the current server-side row data for UPDATE/DELETE
	// operations. Populated by the engine before calling AuthorizeWriteFunc.
	// Not serialized over the wire.
	Existing map[string]any `json:"-"`
}

// PushResponse is the response for pushing changes.
type PushResponse struct {
	Accepted      []PushResult `json:"accepted"`
	Rejected      []PushResult `json:"rejected"`
	Checkpoint    int64        `json:"checkpoint"`
	ServerTime    time.Time    `json:"server_time"`
	SchemaVersion int64        `json:"schema_version"`
	SchemaHash    string       `json:"schema_hash"`
}

// PushResult represents the result of processing a single push record.
type PushResult struct {
	ID              string     `json:"id"`
	TableName       string     `json:"table_name"`
	Operation       string     `json:"operation"`
	Status          string     `json:"status"`
	ReasonCode      string     `json:"reason_code,omitempty"`
	Message         string     `json:"message,omitempty"`
	ServerVersion   *Record    `json:"server_version,omitempty"`
	ServerUpdatedAt *time.Time `json:"server_updated_at,omitempty"`
	ServerDeletedAt *time.Time `json:"server_deleted_at,omitempty"`
}

// Push status constants.
const (
	PushStatusApplied           = "applied"
	PushStatusConflict          = "conflict"
	PushStatusRejectedTerminal  = "rejected_terminal"
	PushStatusRejectedRetryable = "rejected_retryable"
)

// Pull snapshot reason constants (legacy, kept for backwards compatibility).
const (
	SnapshotReasonInitialSyncRequired = "initial_sync_required"
	SnapshotReasonCheckpointBeforeLimit = "checkpoint_before_retention"
	SnapshotReasonHistoryUnavailable    = "history_unavailable"
)

// TableMeta describes a single table's sync configuration.
type TableMeta struct {
	TableName       string   `json:"table_name"`
	PushPolicy      string   `json:"push_policy"`
	Dependencies    []string `json:"dependencies"`
	ParentTable     string   `json:"parent_table,omitempty"`
	ParentFKCol     string   `json:"parent_fk_col,omitempty"`
	UpdatedAtColumn string   `json:"updated_at_column,omitempty"`
	DeletedAtColumn string   `json:"deleted_at_column,omitempty"`
}

// TableMetaResponse is the response for the table metadata endpoint.
type TableMetaResponse struct {
	Tables        []TableMeta `json:"tables"`
	ServerTime    time.Time   `json:"server_time"`
	SchemaVersion int64       `json:"schema_version"`
	SchemaHash    string      `json:"schema_hash"`
}

// RebuildRequest is the request for rebuilding a single bucket.
type RebuildRequest struct {
	ClientID      string `json:"client_id"`
	BucketID      string `json:"bucket_id"`
	Cursor        string `json:"cursor,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	SchemaVersion int64  `json:"schema_version,omitempty"`
	SchemaHash    string `json:"schema_hash,omitempty"`
}

// RebuildResponse is the response for a bucket rebuild page.
type RebuildResponse struct {
	Records        []Record `json:"records"`
	Cursor         string   `json:"cursor,omitempty"`
	Checkpoint     int64    `json:"checkpoint"`
	HasMore        bool     `json:"has_more"`
	BucketChecksum *int32   `json:"bucket_checksum,omitempty"`
	SchemaVersion  int64    `json:"schema_version"`
	SchemaHash     string   `json:"schema_hash"`
}

// DefaultRebuildLimit is the default number of records per rebuild page.
const DefaultRebuildLimit = 100

// MaxRebuildLimit is the maximum records per rebuild page.
const MaxRebuildLimit = 1000

// SnapshotRequest is the request for a full snapshot (legacy, deprecated).
type SnapshotRequest struct {
	ClientID      string          `json:"client_id"`
	Cursor        *SnapshotCursor `json:"cursor,omitempty"`
	Limit         int             `json:"limit,omitempty"`
	SchemaVersion int64           `json:"schema_version,omitempty"`
	SchemaHash    string          `json:"schema_hash,omitempty"`
}

// SnapshotCursor tracks pagination state across snapshot pages (legacy, deprecated).
type SnapshotCursor struct {
	Checkpoint int64  `json:"checkpoint"`
	TableIndex int    `json:"table_idx"`
	AfterID    string `json:"after_id"`
}

// SnapshotResponse is the response for a full snapshot page (legacy, deprecated).
type SnapshotResponse struct {
	Records       []Record        `json:"records"`
	Cursor        *SnapshotCursor `json:"cursor,omitempty"`
	Checkpoint    int64           `json:"checkpoint"`
	HasMore       bool            `json:"has_more"`
	SchemaVersion int64           `json:"schema_version"`
	SchemaHash    string          `json:"schema_hash"`
}

// Schema default kind constants.
const (
	DefaultKindNone       = "none"
	DefaultKindPortable   = "portable"
	DefaultKindServerOnly = "server_only"
)

// SchemaColumn describes a table column for client-side table creation.
type SchemaColumn struct {
	Name             string `json:"name"`
	DBType           string `json:"db_type"`
	LogicalType      string `json:"logical_type"`
	Nullable         bool   `json:"nullable"`
	DefaultSQL       string `json:"default_sql,omitempty"`
	DefaultKind      string `json:"default_kind"`
	SQLiteDefaultSQL string `json:"sqlite_default_sql,omitempty"`
	IsPrimaryKey     bool   `json:"is_primary_key"`
}

// SchemaTable describes table metadata and column definitions.
type SchemaTable struct {
	TableName       string         `json:"table_name"`
	PushPolicy      string         `json:"push_policy"`
	ParentTable     string         `json:"parent_table,omitempty"`
	ParentFKCol     string         `json:"parent_fk_col,omitempty"`
	Dependencies    []string       `json:"dependencies,omitempty"`
	UpdatedAtColumn string         `json:"updated_at_column,omitempty"`
	DeletedAtColumn string         `json:"deleted_at_column,omitempty"`
	PrimaryKey      []string       `json:"primary_key"`
	Columns         []SchemaColumn `json:"columns"`
}

// SchemaResponse returns the full sync schema contract.
type SchemaResponse struct {
	SchemaVersion int64         `json:"schema_version"`
	SchemaHash    string        `json:"schema_hash"`
	ServerTime    time.Time     `json:"server_time"`
	Tables        []SchemaTable `json:"tables"`
}

// ClientDebugResponse is the response for the debug endpoint.
type ClientDebugResponse struct {
	Client         ClientDebugInfo     `json:"client"`
	Buckets        []ServerBucketDebug `json:"buckets"`
	ChangelogStats ChangelogDebugStats `json:"changelog_stats"`
	ServerTime     time.Time           `json:"server_time"`
}

// ClientDebugInfo contains client registration state.
type ClientDebugInfo struct {
	ID              string     `json:"id"`
	ClientID        string     `json:"client_id"`
	Platform        string     `json:"platform"`
	AppVersion      string     `json:"app_version"`
	IsActive        bool       `json:"is_active"`
	LastSyncAt      *time.Time `json:"last_sync_at,omitempty"`
	LastPullAt      *time.Time `json:"last_pull_at,omitempty"`
	LastPushAt      *time.Time `json:"last_push_at,omitempty"`
	LegacyCheckpoint *int64   `json:"legacy_checkpoint,omitempty"`
	BucketSubs      []string   `json:"bucket_subs"`
}

// ServerBucketDebug contains per-bucket debug state.
type ServerBucketDebug struct {
	BucketID    string  `json:"bucket_id"`
	Checkpoint  int64   `json:"checkpoint"`
	MemberCount int     `json:"member_count"`
	Checksum    *int32  `json:"checksum,omitempty"`
}

// ChangelogDebugStats contains changelog statistics.
type ChangelogDebugStats struct {
	MinSeq       int64 `json:"min_seq"`
	MaxSeq       int64 `json:"max_seq"`
	TotalEntries int64 `json:"total_entries"`
}
