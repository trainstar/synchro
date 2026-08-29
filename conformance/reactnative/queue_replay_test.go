package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateQueueReplayScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateQueueReplayScenario(loadQueueReplayAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored queue-replay scenario: %v", err)
	}
}

func TestValidateQueueReplayScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"workload count", func(scenario *scenarios.Scenario) {
			scenario.Steps[0].NativeBinding.Workload.RecordCount++
		}},
		{"iOS proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-QUEUE-REPLAY-RN-IOS-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-queue-replay-ios"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneQueueReplayScenario(loadQueueReplayAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateQueueReplayScenario(scenario); err == nil {
				t.Fatal("changed queue-replay contract was accepted")
			}
		})
	}
}

func TestQueueReplayWorkloadsFollowAuthoredCountsAndDigests(t *testing.T) {
	scenario := loadQueueReplayAuthoredScenario(t)
	workloads, err := queueReplayWorkloads(scenario)
	if err != nil {
		t.Fatalf("derive queue-replay workloads: %v", err)
	}
	if len(workloads) != len(scenario.Steps) {
		t.Fatalf("derived queue-replay workloads = %d, want %d", len(workloads), len(scenario.Steps))
	}
	for index, workload := range workloads {
		want := scenario.Steps[index].NativeBinding.Workload.RecordCount
		if uint64(len(workload.local)) != want {
			t.Fatalf("queue-replay workload %d local writes = %d, want %d", index+1, len(workload.local), want)
		}
		if scenarios.OperationKey(workload.publish) != "model/publish-schema" || scenarios.OperationKey(workload.dropPush) != "push/submit" {
			t.Fatalf("queue-replay workload %d did not derive schema and push operations", index+1)
		}
	}
}

func TestQueueReplayStageCountMatchesDerivedCoordinatorStages(t *testing.T) {
	scenario := loadQueueReplayAuthoredScenario(t)
	workloads, err := queueReplayWorkloads(scenario)
	if err != nil {
		t.Fatalf("derive queue-replay workloads: %v", err)
	}
	coordinator := &QueueReplayCoordinator{steps: workloads}
	want := 4
	for _, workload := range workloads {
		want += len(workload.local) + 5
	}
	if actual := coordinator.StageCount(); actual != want {
		t.Fatalf("queue-replay coordinator stage count = %d, want %d", actual, want)
	}
}

func TestNewQueueReplayCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewQueueReplayCoordinator(QueueReplayCoordinatorConfig{
		Scenario: loadQueueReplayAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android queue-replay coordinator was rejected: %v", err)
	}
	defer func() {
		if err := coordinator.Close(context.Background()); err != nil {
			t.Errorf("close Android queue-replay coordinator: %v", err)
		}
	}()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android queue-replay coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android queue-replay adapter URL = %q", coordinator.adapter)
	}
}

func loadQueueReplayAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadQueueReplayScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored queue-replay scenario: %v", err)
	}
	return scenario
}

func cloneQueueReplayScenario(scenario scenarios.Scenario) scenarios.Scenario {
	data, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}
