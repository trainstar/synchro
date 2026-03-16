package synctest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/trainstar/synchro"
)

// NewTestRegistry creates a minimal Northwind-based registry for testing.
func NewTestRegistry() *synchro.Registry {
	r := synchro.NewRegistry()

	r.Register(&synchro.TableConfig{
		TableName:   "orders",
		PushPolicy:  synchro.PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})

	r.Register(&synchro.TableConfig{
		TableName:    "order_details",
		PushPolicy:   synchro.PushPolicyOwnerOnly,
		ParentTable:  "orders",
		ParentFKCol:  "order_id",
		Dependencies: []string{"orders"},
	})

	r.Register(&synchro.TableConfig{
		TableName:  "products",
		PushPolicy: synchro.PushPolicyDisabled,
	})

	r.Register(&synchro.TableConfig{
		TableName:       "categories",
		PushPolicy:      synchro.PushPolicyOwnerOnly,
		OwnerColumn:     "user_id",
		AllowGlobalRead: true,
	})

	return r
}

// NewMixedTestRegistry creates a registry with both full-column and bare tables
// for introspection testing.
func NewMixedTestRegistry() *synchro.Registry {
	r := synchro.NewRegistry()

	r.Register(&synchro.TableConfig{
		TableName:   "orders",
		PushPolicy:  synchro.PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})

	r.Register(&synchro.TableConfig{
		TableName:   "bare_items",
		PushPolicy:  synchro.PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})

	r.Register(&synchro.TableConfig{
		TableName:   "partial_items",
		PushPolicy:  synchro.PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
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

// MustMarshal marshals v to JSON or panics.
func MustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("synctest.MustMarshal: %v", err))
	}
	return data
}
