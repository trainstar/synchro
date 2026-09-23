package scenarios

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestQueueReplayInputsFollowAuthoredHistory(t *testing.T) {
	scenario, current := queueReplayInputFixture(t)
	for index, step := range scenario.Steps {
		for _, kind := range step.NativeBinding.Workload.MutationKinds {
			for left, right := 0, len(kind.FieldIDs)-1; left < right; left, right = left+1, right-1 {
				kind.FieldIDs[left], kind.FieldIDs[right] = kind.FieldIDs[right], kind.FieldIDs[left]
			}
		}
		before, err := json.Marshal([]any{step, current})
		if err != nil {
			t.Fatal(err)
		}
		inputs, err := BuildQueueReplayInputs(step, current, uint64(index*2+1))
		if err != nil {
			t.Fatalf("build %s: %v", step.ID, err)
		}
		after, err := json.Marshal([]any{step, current})
		if err != nil || string(before) != string(after) {
			t.Fatalf("builder changed its inputs: %v", err)
		}
		if uint64(len(inputs.Local)) != step.NativeBinding.Workload.RecordCount {
			t.Fatalf("step %s local count = %d", step.ID, len(inputs.Local))
		}
		encoded, err := json.Marshal(inputs.Local)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(encoded)
		if got := hex.EncodeToString(digest[:]); got != step.NativeBinding.Workload.Expectation.OperationDigest {
			t.Fatalf("step %s operation digest = %s", step.ID, got)
		}
		if index+1 < len(scenario.Steps) && inputs.NextSchema.SchemaFact != scenario.Steps[index+1].NativeBinding.Workload.AuthoredSchema {
			t.Fatalf("step %s next schema does not match authored history", step.ID)
		}
		var push struct {
			Delivery  string `json:"delivery"`
			CommitLSN string `json:"commit_lsn"`
			EndLSN    string `json:"end_lsn"`
			Request   struct {
				BatchID   string     `json:"batch_id"`
				Schema    SchemaFact `json:"schema"`
				Mutations []struct {
					MutationID     string            `json:"mutation_id"`
					AuthoredSchema SchemaFact        `json:"authored_schema"`
					Columns        map[string]string `json:"columns"`
				} `json:"mutations"`
			} `json:"request"`
		}
		if err := json.Unmarshal(inputs.DropPush.Payload, &push); err != nil {
			t.Fatal(err)
		}
		if push.Delivery != "drop_after_server" || push.Request.BatchID != inputs.BatchID || inputs.BatchID == "" ||
			push.Request.Schema != inputs.NextSchema.SchemaFact || len(push.Request.Mutations) != len(inputs.Local) {
			t.Fatalf("step %s changed the push envelope", step.ID)
		}
		for ordinal, operation := range inputs.Local {
			var local struct {
				MutationID string `json:"mutation_id"`
				Columns    []struct {
					FieldID string `json:"field_id"`
					Value   string `json:"value"`
				} `json:"columns"`
			}
			if err := json.Unmarshal(operation.Payload, &local); err != nil {
				t.Fatal(err)
			}
			mutation := push.Request.Mutations[ordinal]
			if mutation.MutationID != local.MutationID || mutation.AuthoredSchema != current.SchemaFact || len(mutation.Columns) != len(local.Columns) {
				t.Fatalf("step %s mutation %d lost authored identity", step.ID, ordinal)
			}
			for _, column := range local.Columns {
				if mutation.Columns[column.FieldID] != column.Value {
					t.Fatalf("step %s mutation %d changed field %s", step.ID, ordinal, column.FieldID)
				}
			}
		}
		if index == 0 && (push.CommitLSN != "1" || push.EndLSN != "2") {
			t.Fatalf("initial commit boundary = %s/%s", push.CommitLSN, push.EndLSN)
		}
		if index == 0 && inputs.BatchID != "a2c6f93c-a22d-43f7-90fa-970a8560d485" {
			t.Fatalf("initial batch UUID = %s", inputs.BatchID)
		}
		for _, field := range inputs.NextSchema.Tables[0].Fields {
			if field.FieldID == "id" {
				continue
			}
			if field.DefaultWireJSON == nil || *field.DefaultWireJSON != `""` {
				t.Fatalf("field %s lost its encoded empty-string default", field.FieldID)
			}
		}
		current = inputs.NextSchema
	}
	crud := current.CRUDSchema()
	if crud.Version != current.Version || crud.Hash != current.Hash || len(crud.Tables) != 1 || len(crud.Tables[0].Fields) != 3 {
		t.Fatal("shared schema lost the current CRUD inputs")
	}
}

func TestQueueReplayInputsRejectChangedOrIncompleteWorkloads(t *testing.T) {
	scenario, current := queueReplayInputFixture(t)
	tests := []struct {
		name   string
		mutate func(*Step)
		digest bool
	}{
		{"missing binding", func(step *Step) { step.NativeBinding = nil }, false},
		{"zero batch", func(step *Step) { step.NativeBinding.Workload.BatchSize = 0 }, false},
		{"empty targets", func(step *Step) { step.NativeBinding.Workload.Targets = nil }, false},
		{"oversized record count", func(step *Step) { step.NativeBinding.Workload.RecordCount = 1001 }, false},
		{"wrong current schema", func(step *Step) { step.NativeBinding.Workload.AuthoredSchema.Version++ }, false},
		{"missing terminal field", func(step *Step) {
			step.NativeBinding.Workload.MutationKinds[1].FieldIDs = []string{"value"}
		}, false},
		{"duplicate field", func(step *Step) {
			step.NativeBinding.Workload.MutationKinds[1].FieldIDs = []string{"value", "value"}
		}, false},
		{"unknown field", func(step *Step) {
			step.NativeBinding.Workload.MutationKinds[1].FieldIDs = []string{"absent", "value"}
		}, false},
		{"changed seed", func(step *Step) { step.NativeBinding.Workload.Seed++ }, true},
		{"changed digest", func(step *Step) {
			step.NativeBinding.Workload.Expectation.OperationDigest = strings.Repeat("0", 64)
		}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			step := cloneScenario(scenario).Steps[0]
			test.mutate(&step)
			inputs, err := BuildQueueReplayInputs(step, current, 1)
			if err == nil || len(inputs.Local) != 0 {
				t.Fatal("invalid workload returned complete or partial inputs")
			}
			if strings.Contains(err.Error(), "operation digest") != test.digest {
				t.Fatalf("workload failed at the wrong boundary: %v", err)
			}
		})
	}
}

func TestQueueReplayNextSchemaPreservesOrderingAndDefaults(t *testing.T) {
	_, current := queueReplayInputFixture(t)
	defaultValue := `{"large":"9007199254740993"}`
	for index := range current.Tables[0].Fields {
		if current.Tables[0].Fields[index].FieldID == "value" {
			current.Tables[0].Fields[index].DefaultWireJSON = &defaultValue
		}
	}
	current.Tables[0].Fields[0], current.Tables[0].Fields[2] = current.Tables[0].Fields[2], current.Tables[0].Fields[0]
	current.Tables[0].Indexes = append(
		[]QueueReplaySchemaIndex{{IndexID: "zulu", Name: "zulu", FieldIDs: []string{"value"}}},
		append(current.Tables[0].Indexes, QueueReplaySchemaIndex{IndexID: "alpha", Name: "alpha", FieldIDs: []string{"id"}})...,
	)
	next, publish, err := queueReplayNextSchema(current, "obsolete_value", 2)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Body   string                   `json:"body"`
		Tables []QueueReplaySchemaTable `json:"tables"`
	}
	if err := json.Unmarshal(publish.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Tables []struct {
			Fields []struct {
				FieldID string `json:"field_id"`
			} `json:"fields"`
			Indexes []QueueReplaySchemaIndex `json:"indexes"`
		} `json:"tables"`
	}
	if err := json.Unmarshal([]byte(payload.Body), &manifest); err != nil {
		t.Fatal(err)
	}
	var fieldIDs, indexIDs []string
	for _, field := range manifest.Tables[0].Fields {
		fieldIDs = append(fieldIDs, field.FieldID)
	}
	for _, index := range manifest.Tables[0].Indexes {
		indexIDs = append(indexIDs, index.IndexID)
	}
	if !reflect.DeepEqual(fieldIDs, []string{"id", "queue_value_2", "value"}) ||
		!reflect.DeepEqual(indexIDs, []string{"alpha", "items-pk", "zulu"}) {
		t.Fatalf("manifest order fields=%v indexes=%v", fieldIDs, indexIDs)
	}
	for _, table := range []QueueReplaySchemaTable{next.Tables[0], payload.Tables[0]} {
		for _, field := range table.Fields {
			switch field.FieldID {
			case "value":
				if field.DefaultWireJSON == nil || *field.DefaultWireJSON != defaultValue {
					t.Fatal("retained default changed")
				}
			case "queue_value_2":
				if field.DefaultWireJSON == nil || *field.DefaultWireJSON != `""` {
					t.Fatal("new default is not valid encoded string JSON")
				}
			}
		}
	}
}

func TestQueueReplayNextSchemaRejectsMissingDuplicateAndInvalidFields(t *testing.T) {
	tests := []struct {
		name    string
		removed string
		mutate  func(*QueueReplaySchema)
	}{
		{"missing removed field", "absent", func(*QueueReplaySchema) {}},
		{"primary key removal", "id", func(*QueueReplaySchema) {}},
		{"duplicate field", "obsolete_value", func(schema *QueueReplaySchema) {
			schema.Tables[0].Fields = append(schema.Tables[0].Fields, schema.Tables[0].Fields[1])
		}},
		{"new field collision", "obsolete_value", func(schema *QueueReplaySchema) {
			schema.Tables[0].Fields = append(schema.Tables[0].Fields, QueueReplaySchemaField{FieldID: "queue_value_2", Name: "queue_value_2", Type: "string", Writable: true})
		}},
		{"invalid default", "obsolete_value", func(schema *QueueReplaySchema) {
			value := ""
			schema.Tables[0].Fields[2].DefaultWireJSON = &value
		}},
		{"index references removed field", "obsolete_value", func(schema *QueueReplaySchema) {
			schema.Tables[0].Indexes[0].FieldIDs = []string{"obsolete_value"}
		}},
	}
	scenario, _ := queueReplayInputFixture(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current, err := InitialQueueReplaySchema(scenario.Model.Setup[0])
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&current)
			if _, _, err := queueReplayNextSchema(current, test.removed, 2); err == nil {
				t.Fatal("invalid schema transition was accepted")
			}
		})
	}
}

func queueReplayInputFixture(t *testing.T) (Scenario, QueueReplaySchema) {
	t.Helper()
	scenario, err := LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/queue-replay-001.json")
	if err != nil {
		t.Fatal(err)
	}
	current, err := InitialQueueReplaySchema(scenario.Model.Setup[0])
	if err != nil {
		t.Fatal(err)
	}
	return scenario, current
}
