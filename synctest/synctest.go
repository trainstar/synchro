package synctest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/trainstar/synchro"
)

// NewTestRegistry creates a minimal registry for testing with a few tables.
func NewTestRegistry() *synchro.Registry {
	r := synchro.NewRegistry()

	r.Register(&synchro.TableConfig{
		TableName:   "items",
		PushPolicy:  synchro.PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})

	r.Register(&synchro.TableConfig{
		TableName:    "item_details",
		PushPolicy:   synchro.PushPolicyOwnerOnly,
		ParentTable:  "items",
		ParentFKCol:  "item_id",
		Dependencies: []string{"items"},
	})

	r.Register(&synchro.TableConfig{
		TableName:  "categories",
		PushPolicy: synchro.PushPolicyDisabled,
	})

	r.Register(&synchro.TableConfig{
		TableName:       "tags",
		PushPolicy:      synchro.PushPolicyOwnerOnly,
		OwnerColumn:     "user_id",
		AllowGlobalRead: true,
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

// MustMarshal marshals v to JSON or panics.
func MustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("synctest.MustMarshal: %v", err))
	}
	return data
}
