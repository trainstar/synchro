package kotlin

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestQueueReplayWorkloadOutcomeCountsMatchAuthoredPayload(t *testing.T) {
	step := queueReplayStep(t, "STEP-PERF-QUEUE-REPLAY-007")
	total, rejected := 1000, 1
	captured, err := queueCapturedOutcomeCountsFromResult(Result{MutationOutcomeCount: &total, RejectedMutationCount: &rejected})
	if err != nil {
		t.Fatalf("read captured queue-replay outcomes: %v", err)
	}
	if err := validateQueueWorkloadOutcomeCounts(step, queueOutcomeCounts{}, captured); err != nil {
		t.Fatalf("validate captured queue-replay outcomes: %v", err)
	}
}

func TestQueueReplayWorkloadOutcomeCountsRejectMassRejection(t *testing.T) {
	step := queueReplayStep(t, "STEP-PERF-QUEUE-REPLAY-007")
	total, rejected := 1000, 1000
	captured, err := queueCapturedOutcomeCountsFromResult(Result{MutationOutcomeCount: &total, RejectedMutationCount: &rejected})
	if err != nil {
		t.Fatalf("read captured queue-replay outcomes: %v", err)
	}
	err = validateQueueWorkloadOutcomeCounts(step, queueOutcomeCounts{}, captured)
	if err == nil {
		t.Fatal("queue-replay accepted mass rejection")
	}
	for _, value := range []string{
		"STEP-PERF-QUEUE-REPLAY-007",
		"observed accepted=0 rejected=1000",
		"expected accepted=999 rejected=1",
	} {
		if !strings.Contains(err.Error(), value) {
			t.Fatalf("queue-replay mass-rejection error = %q, missing %q", err, value)
		}
	}
}

func queueReplayStep(t *testing.T, id scenarios.StepID) scenarios.Step {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/queue-replay-001.json")
	if err != nil {
		t.Fatalf("load queue-replay scenario: %v", err)
	}
	for _, step := range scenario.Steps {
		if step.ID == id {
			return step
		}
	}
	t.Fatalf("queue-replay step %s is absent", id)
	return scenarios.Step{}
}
