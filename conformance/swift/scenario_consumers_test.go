package swift

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestQueueReplayDirectOperationsFollowAuthoredSchemaHistory(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/queue-replay-001.json")
	if err != nil {
		t.Fatalf("load queue-replay scenario: %v", err)
	}
	steps, err := swiftScenarioStepMap(scenario, queueReplayScenarioID, 9)
	if err != nil {
		t.Fatalf("validate queue-replay scenario: %v", err)
	}
	current, err := queueInitialSchema(scenario.Model.Setup[0])
	if err != nil {
		t.Fatalf("read queue-replay initial schema: %v", err)
	}
	for index := 1; index <= 9; index++ {
		step := steps[scenarios.StepID(fmt.Sprintf("STEP-PERF-QUEUE-REPLAY-%03d", index))]
		_, _, push, batchID, next, err := queueWorkloadOperations(step, current, uint64(index*2-1))
		if err != nil {
			t.Fatalf("derive queue-replay step %d: %v", index, err)
		}
		if batchID == "" || scenarios.OperationKey(push) != "push/submit" {
			t.Fatalf("queue-replay step %d did not derive a push batch", index)
		}
		if index < 9 {
			aliasNames := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}
			var wantHash string
			for _, alias := range scenario.NativeIdentityAliases {
				if alias.Alias == "queue-schema-"+aliasNames[index] {
					var value struct {
						Hash string `json:"hash"`
					}
					if err := json.Unmarshal(alias.Value, &value); err != nil {
						t.Fatalf("decode queue-replay schema alias %q: %v", alias.Alias, err)
					}
					wantHash = value.Hash
				}
			}
			if next.Hash != wantHash {
				t.Fatalf("queue-replay schema %d hash = %q, want %q", index+1, next.Hash, wantHash)
			}
		}
		current = next
	}
}

func TestSeededEmptyStartupDirectBindingGroupsRemainClosed(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/seeded-empty-startup-001.json")
	if err != nil {
		t.Fatalf("load seeded-startup scenario: %v", err)
	}
	steps, err := swiftScenarioStepMap(scenario, seededEmptyStartupScenarioID, 15)
	if err != nil {
		t.Fatalf("validate seeded-startup scenario: %v", err)
	}
	for _, number := range []int{3, 6, 9, 11, 13, 15} {
		id := scenarios.StepID(fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", number))
		step := steps[id]
		if step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" || step.NativeBinding.Method != "start" || step.NativeBinding.Completion != "idle" {
			t.Fatalf("seeded-startup step %s is not a synchronous start binding", id)
		}
		if scenarios.OperationKey(step.Operation) != "connect/send" {
			t.Fatalf("seeded-startup step %s operation = %s", id, scenarios.OperationKey(step.Operation))
		}
	}
}
