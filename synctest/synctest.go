package synctest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/trainstar/synchro"
)

// NewTestTables returns the minimal Northwind-based table list for testing.
func NewTestTables() []synchro.Table {
	return []synchro.Table{
		{Name: "orders"},
		{Name: "order_details"},
		{Name: "products"},
		{Name: "categories"},
	}
}

// NewMixedTestTables returns tables with both full-column and bare tables
// for introspection testing.
func NewMixedTestTables() []synchro.Table {
	return []synchro.Table{
		{Name: "orders"},
		{Name: "bare_items"},
		{Name: "partial_items"},
	}
}

// NewTestRegistry creates a minimal Northwind-based registry for testing.
// Uses RegisterForTest so no DB connection is needed.
func NewTestRegistry() *synchro.Registry {
	r := synchro.NewRegistry()

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "orders",
	})

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "order_details",
	})

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "products",
	})

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "categories",
	})

	return r
}

// NewMixedTestRegistry creates a registry with both full-column and bare tables
// for introspection testing.
func NewMixedTestRegistry() *synchro.Registry {
	r := synchro.NewRegistry()

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "orders",
	})

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "bare_items",
	})

	r.RegisterForTest(&synchro.TableConfig{
		TableName: "partial_items",
	})

	return r
}

// MakePushRecord creates a PushRecord for testing.
func MakePushRecord(id, table, operation string, data map[string]any) synchro.PushRecord {
	jsonData, _ := json.Marshal(data)
	return synchro.PushRecord{
		ID:              id,
		TableName:       table,
		Operation:       operation,
		Data:            jsonData,
		ClientUpdatedAt: time.Now().UTC(),
	}
}

// MakeRegisterRequest creates a RegisterRequest for testing.
func MakeRegisterRequest(clientID, platform, version string) *synchro.RegisterRequest {
	return &synchro.RegisterRequest{
		ClientID:      clientID,
		Platform:      platform,
		AppVersion:    version,
		SchemaVersion: 0,
		SchemaHash:    "",
	}
}

// MakePushRequest creates a PushRequest with schema fields for testing.
func MakePushRequest(clientID string, schemaVersion int64, schemaHash string, changes ...synchro.PushRecord) *synchro.PushRequest {
	return &synchro.PushRequest{
		ClientID:      clientID,
		SchemaVersion: schemaVersion,
		SchemaHash:    schemaHash,
		Changes:       changes,
	}
}

// MakePullRequest creates a PullRequest with schema fields for testing.
func MakePullRequest(clientID string, checkpoint int64, schemaVersion int64, schemaHash string) *synchro.PullRequest {
	return &synchro.PullRequest{
		ClientID:      clientID,
		Checkpoint:    checkpoint,
		SchemaVersion: schemaVersion,
		SchemaHash:    schemaHash,
	}
}

// MakePerBucketPullRequest creates a PullRequest using per-bucket checkpoints.
func MakePerBucketPullRequest(clientID string, bucketCheckpoints map[string]int64, schemaVersion int64, schemaHash string) *synchro.PullRequest {
	return &synchro.PullRequest{
		ClientID:          clientID,
		BucketCheckpoints: bucketCheckpoints,
		SchemaVersion:     schemaVersion,
		SchemaHash:        schemaHash,
	}
}

// MakeRebuildRequest creates a RebuildRequest for testing.
func MakeRebuildRequest(clientID, bucketID string, schemaVersion int64, schemaHash string) *synchro.RebuildRequest {
	return &synchro.RebuildRequest{
		ClientID:      clientID,
		BucketID:      bucketID,
		SchemaVersion: schemaVersion,
		SchemaHash:    schemaHash,
	}
}

// MustMarshal marshals v to JSON or panics.
func MustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("synctest.MustMarshal: %v", err))
	}
	return data
}
