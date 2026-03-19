package synchroapi

import "encoding/json"

// RegisterRequest is the request body for POST /sync/register.
type RegisterRequest struct {
	ClientID      string `json:"client_id"`
	Platform      string `json:"platform"`
	AppVersion    string `json:"app_version"`
	SchemaVersion int64  `json:"schema_version"`
	SchemaHash    string `json:"schema_hash"`
}

// PullRequest is the request body for POST /sync/pull.
type PullRequest struct {
	ClientID          string           `json:"client_id"`
	Checkpoint        int64            `json:"checkpoint,omitempty"`
	BucketCheckpoints map[string]int64 `json:"bucket_checkpoints,omitempty"`
	Tables            []string         `json:"tables,omitempty"`
	Limit             int              `json:"limit,omitempty"`
	KnownBuckets      []string         `json:"known_buckets,omitempty"`
	SchemaVersion     int64            `json:"schema_version"`
	SchemaHash        string           `json:"schema_hash"`
}

// PushRequest is the request body for POST /sync/push.
type PushRequest struct {
	ClientID      string            `json:"client_id"`
	Changes       []json.RawMessage `json:"changes"`
	SchemaVersion int64             `json:"schema_version"`
	SchemaHash    string            `json:"schema_hash"`
}

// RebuildRequest is the request body for POST /sync/rebuild.
type RebuildRequest struct {
	ClientID      string `json:"client_id"`
	BucketID      string `json:"bucket_id"`
	Cursor        string `json:"cursor,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	SchemaVersion int64  `json:"schema_version,omitempty"`
	SchemaHash    string `json:"schema_hash,omitempty"`
}

// ErrorResponse is a structured error response from the extension.
type ErrorResponse struct {
	Error string `json:"error"`
}

// SchemaMismatchResponse is returned when client/server schemas diverge.
type SchemaMismatchResponse struct {
	Error               string `json:"error"`
	ServerSchemaVersion int64  `json:"server_schema_version"`
	ServerSchemaHash    string `json:"server_schema_hash"`
}
