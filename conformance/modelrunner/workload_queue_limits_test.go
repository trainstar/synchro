package modelrunner

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestExpandPendingMutationsWorkloadCreatesExactDurablePartitions(t *testing.T) {
	model := installedWorkloadModel(t, "conformance/scenarios/performance/queue-replay-001.json")
	samples := []struct {
		accepted uint64
		rejected uint64
	}{
		{accepted: 1, rejected: 1},
		{accepted: 99, rejected: 1},
		{accepted: 999, rejected: 1},
	}

	for _, sample := range samples {
		t.Run(strconvSampleName(sample.accepted+sample.rejected), func(t *testing.T) {
			before := model.Snapshot()
			payload := rawWorkloadPayload(t, map[string]any{
				"profile":        "pending_mutations",
				"user_id":        "user-a",
				"client_id":      "client-a",
				"table_id":       "items",
				"accepted_count": sample.accepted,
				"rejected_count": sample.rejected,
			})
			operations, err := expandPendingMutationsWorkload(before, payload)
			if err != nil {
				t.Fatalf("expand pending mutations: %v", err)
			}
			if !reflect.DeepEqual(before, model.Snapshot()) {
				t.Fatal("pending-mutation expansion changed reference state directly")
			}

			wantTotal := int(sample.accepted + sample.rejected)
			counts := make(map[string]int)
			mutationIDs := make(map[reference.MutationID]struct{}, wantTotal)
			for _, operation := range operations {
				key := scenarios.OperationKey(operation)
				counts[key]++
				if key == "workload/prepare" || key == "model/install-current-contract" {
					t.Fatalf("expansion returned prohibited operation %q", key)
				}
				class, known := scenarios.LookupOperationClass(key)
				if !known || class != scenarios.OperationClassReference {
					t.Fatalf("expanded operation %q is not a closed reference operation", key)
				}
				if err := scenarios.ValidateOperation(operation); err != nil {
					t.Fatalf("validate expanded operation %q: %v", key, err)
				}
				if key == "local/write" {
					var write struct {
						MutationID string `json:"mutation_id"`
					}
					if err := json.Unmarshal(operation.Payload, &write); err != nil || write.MutationID == "" {
						t.Fatalf("decode local mutation identity: %v", err)
					}
					mutationIDs[reference.MutationID(write.MutationID)] = struct{}{}
				}
			}
			if counts["local/write"] != wantTotal || len(mutationIDs) != wantTotal {
				t.Fatalf("durable local writes = %d/%d, want %d unique writes", counts["local/write"], len(mutationIDs), wantTotal)
			}
			if counts["process/restart-client"] != 1 || counts["model/publish-schema"] != 1 || counts["push/submit"] != 2 {
				t.Fatalf("substantive expansion counts = %#v", counts)
			}

			pushResults := make([]reference.StepResult, 0, 2)
			for _, operation := range operations {
				result, err := model.ApplyResolved(context.Background(), operation, reference.ResolvedOperationInput{})
				if err != nil {
					t.Fatalf("apply expanded operation %s: %v", scenarios.OperationKey(operation), err)
				}
				if scenarios.OperationKey(operation) == "push/submit" {
					pushResults = append(pushResults, result)
				}
			}
			after := model.Snapshot()
			if len(pushResults) != 2 || pushResults[0].HTTP == nil || pushResults[0].HTTP.Status != 503 || pushResults[0].Push == nil || pushResults[0].Push.Replay != reference.ReplayDispositionExecuted {
				t.Fatalf("first response-loss push = %#v", pushResults)
			}
			if pushResults[1].HTTP == nil || pushResults[1].HTTP.Status != 200 || pushResults[1].Push == nil || pushResults[1].Push.Replay != reference.ReplayDispositionReplayed {
				t.Fatalf("replayed push = %#v", pushResults[1])
			}
			if after.CurrentSchema.Version != before.CurrentSchema.Version+1 {
				t.Fatalf("current schema version = %d, want %d", after.CurrentSchema.Version, before.CurrentSchema.Version+1)
			}
			if len(after.Batches)-len(before.Batches) != 1 || len(after.Mutations)-len(before.Mutations) != wantTotal {
				t.Fatalf("durable server deltas = batches %d mutations %d", len(after.Batches)-len(before.Batches), len(after.Mutations)-len(before.Mutations))
			}

			applied, terminal := 0, 0
			for _, entry := range after.Mutations {
				if _, created := mutationIDs[entry.Key.Mutation]; !created {
					continue
				}
				switch entry.Value.Outcome.State {
				case reference.MutationOutcomeApplied:
					applied++
				case reference.MutationOutcomeRejectedTerminal:
					if entry.Value.Outcome.Reason != "schema_incompatible" {
						t.Fatalf("terminal rejection reason = %q", entry.Value.Outcome.Reason)
					}
					terminal++
				default:
					t.Fatalf("mutation %q has outcome %q", entry.Key.Mutation, entry.Value.Outcome.State)
				}
			}
			if applied != int(sample.accepted) || terminal != int(sample.rejected) {
				t.Fatalf("durable outcome partition = %d applied and %d terminal, want %d and %d", applied, terminal, sample.accepted, sample.rejected)
			}

			local := workloadLocalSnapshot(t, after, "user-a", "client-a")
			acceptedLocal, rejectedLocal := 0, 0
			for _, queued := range local.DurableQueue {
				if _, created := mutationIDs[queued.Mutation]; !created {
					continue
				}
				switch queued.Status {
				case reference.LocalMutationStatusAccepted:
					acceptedLocal++
				case reference.LocalMutationStatusServerRejected:
					rejectedLocal++
				default:
					t.Fatalf("durable local mutation %q has status %q", queued.Mutation, queued.Status)
				}
			}
			if acceptedLocal != int(sample.accepted) || rejectedLocal != int(sample.rejected) {
				t.Fatalf("local durable partition = %d accepted and %d rejected", acceptedLocal, rejectedLocal)
			}
		})
	}
}

func TestExpandPendingMutationsWorkloadRejectsUnauthoredCounts(t *testing.T) {
	model := installedWorkloadModel(t, "conformance/scenarios/performance/queue-replay-001.json")
	for _, counts := range [][2]uint64{{0, 1}, {1, 0}, {2, 1}, {999, 2}} {
		payload := rawWorkloadPayload(t, map[string]any{
			"profile": "pending_mutations", "user_id": "user-a", "client_id": "client-a", "table_id": "items",
			"accepted_count": counts[0], "rejected_count": counts[1],
		})
		if operations, err := expandPendingMutationsWorkload(model.Snapshot(), payload); err == nil || operations != nil {
			t.Fatalf("counts %d/%d produced operations %#v and error %v", counts[0], counts[1], operations, err)
		}
	}
}

func TestExpandConfiguredLimitsWorkloadProducesExactClosedSamplePlan(t *testing.T) {
	model := installedWorkloadModel(t, "conformance/scenarios/performance/configured-bounds-001.json")
	snapshot := model.Snapshot()
	payload := rawWorkloadPayload(t, map[string]any{
		"profile":                  "configured_limits",
		"max_scope_fanout":         8,
		"max_impact_rows":          1000,
		"pull_maximum":             1000,
		"rebuild_maximum":          1000,
		"compaction_batch_maximum": 10000,
		"backfill_batch_maximum":   1000,
	})
	plan, err := expandConfiguredLimitsWorkload(snapshot, payload)
	if err != nil {
		t.Fatalf("expand configured limits: %v", err)
	}
	if !reflect.DeepEqual(snapshot, model.Snapshot()) {
		t.Fatal("configured-limit expansion changed reference state directly")
	}
	if len(plan.Samples) != 63 {
		t.Fatalf("configured sample count = %d, want 63", len(plan.Samples))
	}
	type stratum struct {
		family   WorkloadSampleFamily
		boundary WorkloadSampleBoundary
	}
	counts := make(map[stratum]int)
	targets := make(map[int]struct{}, len(plan.Samples))
	for _, sample := range plan.Samples {
		if sample.ExpandedOperationIndex < 0 || sample.ExpandedOperationIndex >= len(plan.Operations) {
			t.Fatalf("sample target %d is outside %d operations", sample.ExpandedOperationIndex, len(plan.Operations))
		}
		if _, duplicate := targets[sample.ExpandedOperationIndex]; duplicate {
			t.Fatalf("expanded operation %d is sampled more than once", sample.ExpandedOperationIndex)
		}
		targets[sample.ExpandedOperationIndex] = struct{}{}
		operation := plan.Operations[sample.ExpandedOperationIndex]
		if value := configuredTargetValue(t, sample.Family, operation); value != sample.Value {
			t.Fatalf("%s/%s target value = %d, want %d", sample.Family, sample.Boundary, value, sample.Value)
		}
		counts[stratum{family: sample.Family, boundary: sample.Boundary}]++
	}
	for _, family := range []WorkloadSampleFamily{
		WorkloadSampleFanout,
		WorkloadSampleImpact,
		WorkloadSamplePull,
		WorkloadSampleRebuild,
		WorkloadSampleCompaction,
		WorkloadSampleBackfill,
		WorkloadSamplePush,
	} {
		for _, boundary := range []WorkloadSampleBoundary{WorkloadBoundaryLower, WorkloadBoundaryUpper, WorkloadBoundaryInvalid} {
			if counts[stratum{family: family, boundary: boundary}] != 3 {
				t.Fatalf("%s/%s count = %d, want 3", family, boundary, counts[stratum{family: family, boundary: boundary}])
			}
		}
	}
	if len(counts) != 21 {
		t.Fatalf("configured strata count = %d, want 21", len(counts))
	}
	for index, operation := range plan.Operations {
		key := scenarios.OperationKey(operation)
		if key == "workload/prepare" || key == "model/install-current-contract" {
			t.Fatalf("expanded operation %d is prohibited: %s", index, key)
		}
		class, found := scenarios.LookupOperationClass(key)
		if !found || class != scenarios.OperationClassReference {
			t.Fatalf("expanded operation %d is not closed: %s", index, key)
		}
		if err := scenarios.ValidateOperation(operation); err != nil {
			t.Fatalf("validate expanded operation %d %s: %v", index, key, err)
		}
	}
}

func TestConfiguredLimitsWorkloadRecordsEveryOutcomeAndReplay(t *testing.T) {
	scenario := loadWorkloadScenario(t, "conformance/scenarios/performance/configured-bounds-001.json")
	result, err := RunScenario(context.Background(), scenario)
	if err != nil {
		t.Fatalf("run configured-limit scenario: %v", err)
	}
	if !result.Passed || !result.Replay.StateMatch || len(result.Steps) != 1 {
		t.Fatalf("configured-limit run = passed %t replay %t steps %d", result.Passed, result.Replay.StateMatch, len(result.Steps))
	}
	execution := result.Steps[0]
	if !configuredSampleRecordsSatisfied(execution) {
		t.Fatal("configured-limit sample records do not satisfy the closed outcome contract")
	}
	if execution.Err != nil || execution.Result.Kind != reference.StepResultKindLifecycle || len(execution.Expanded) == 0 || scenarios.OperationKey(execution.Expanded[len(execution.Expanded)-1]) != "process/restart-client" || !transitionSemanticallyValid(result) {
		t.Fatal("configured-limit outer macro result or transition is invalid")
	}
	for _, sample := range execution.Samples {
		if sample.Boundary == WorkloadBoundaryInvalid {
			if !reflect.DeepEqual(sample.Before, sample.After) {
				t.Fatalf("invalid %s sample changed state", sample.Family)
			}
			switch sample.Family {
			case WorkloadSampleFanout, WorkloadSampleImpact, WorkloadSampleBackfill, WorkloadSampleCompaction:
				if sample.Result != nil || sample.ErrorCode != "invalid_limit" {
					t.Fatalf("invalid %s administrative outcome = result %#v code %q", sample.Family, sample.Result, sample.ErrorCode)
				}
			case WorkloadSamplePull, WorkloadSampleRebuild, WorkloadSamplePush:
				if sample.ErrorCode != "" || sample.Result == nil || sample.Result.HTTP == nil || sample.Result.HTTP.Status != 400 || !sample.Result.HTTP.HasCode || sample.Result.HTTP.Code != "invalid_request" {
					t.Fatalf("invalid %s endpoint outcome is not HTTP 400 invalid_request", sample.Family)
				}
			}
			continue
		}
		if sample.Result == nil || sample.ErrorCode != "" {
			t.Fatalf("valid %s/%s sample was not accepted", sample.Family, sample.Boundary)
		}
		switch sample.Family {
		case WorkloadSampleBackfill:
			wantBatches := (uint64(len(sample.Before.Rows)) + sample.Value - 1) / sample.Value
			if sample.Result.Schema == nil || sample.Result.Schema.BatchSize != sample.Value || sample.Result.Schema.BatchCount != wantBatches {
				t.Fatalf("backfill observation = %#v, want size %d and count %d", sample.Result.Schema, sample.Value, wantBatches)
			}
		case WorkloadSampleCompaction:
			if sample.Result.Retention == nil || sample.Result.Retention.BatchSize != sample.Value || sample.Result.Retention.DeletedCount > sample.Value {
				t.Fatalf("compaction observation = %#v, want batch size %d", sample.Result.Retention, sample.Value)
			}
		case WorkloadSamplePush:
			if sample.Result.Push == nil || uint64(len(sample.Result.Push.Mutations)) != sample.Value {
				t.Fatalf("push mutation observations = %d, want %d", len(sample.Result.Push.Mutations), sample.Value)
			}
		}
	}
	replayMutant := execution
	replayMutant.Samples = append([]WorkloadSampleExecution(nil), execution.Samples...)
	replayMutant.Samples[0].Value++
	if sameExecution(execution, replayMutant) {
		t.Fatal("replay equality ignored a typed sample record")
	}
}

func TestOwnedQueueAndLimitScenariosKeepExactAuthoredStrata(t *testing.T) {
	queue := loadWorkloadScenario(t, "conformance/scenarios/performance/queue-replay-001.json")
	strata := make(map[uint64]int)
	for _, step := range queue.Steps {
		profile, accepted, rejected, ok := pendingWorkloadNumbers(step.Operation.Payload)
		if !ok || profile != "pending_mutations" || rejected != 1 {
			t.Fatalf("queue step %q has invalid workload payload", step.ID)
		}
		strata[accepted+rejected]++
		if step.ExpectedOutcome.Disposition != "success" {
			t.Fatalf("queue step %q weakened its expected outcome", step.ID)
		}
	}
	if !reflect.DeepEqual(strata, map[uint64]int{2: 3, 100: 3, 1000: 3}) {
		t.Fatalf("queue strata = %#v", strata)
	}

	limits := loadWorkloadScenario(t, "conformance/scenarios/performance/configured-bounds-001.json")
	if len(limits.Steps) != 1 || limits.Steps[0].ExpectedOutcome.Disposition != "success" {
		t.Fatal("configured-limit scenario weakened its single successful expected outcome")
	}
	var configured struct {
		Profile                string `json:"profile"`
		MaxScopeFanout         uint64 `json:"max_scope_fanout"`
		MaxImpactRows          uint64 `json:"max_impact_rows"`
		PullMaximum            uint64 `json:"pull_maximum"`
		RebuildMaximum         uint64 `json:"rebuild_maximum"`
		CompactionBatchMaximum uint64 `json:"compaction_batch_maximum"`
		BackfillBatchMaximum   uint64 `json:"backfill_batch_maximum"`
	}
	if err := json.Unmarshal(limits.Steps[0].Operation.Payload, &configured); err != nil {
		t.Fatalf("decode configured-limit scenario payload: %v", err)
	}
	if configured.Profile != "configured_limits" || configured.MaxScopeFanout != 8 || configured.MaxImpactRows != 1000 || configured.PullMaximum != 1000 || configured.RebuildMaximum != 1000 || configured.CompactionBatchMaximum != 10000 || configured.BackfillBatchMaximum != 1000 {
		t.Fatalf("configured-limit maxima = %#v", configured)
	}
}

func installedWorkloadModel(t *testing.T, scenarioPath string) *reference.Model {
	t.Helper()
	authored := loadWorkloadScenario(t, scenarioPath)
	model, err := NewModel(20260811)
	if err != nil {
		t.Fatalf("create workload model: %v", err)
	}
	if _, err := model.ApplyResolved(context.Background(), authored.Model.Setup[0], reference.ResolvedOperationInput{}); err != nil {
		t.Fatalf("install workload contract: %v", err)
	}
	return model
}

func loadWorkloadScenario(t *testing.T, scenarioPath string) scenarios.Scenario {
	t.Helper()
	authored, err := scenarios.LoadFile(context.Background(), "../..", scenarioPath)
	if err != nil {
		t.Fatalf("load workload scenario: %v", err)
	}
	return authored
}

func rawWorkloadPayload(t *testing.T, value map[string]any) map[string]json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode workload payload: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatalf("decode workload payload: %v", err)
	}
	return payload
}

func workloadLocalSnapshot(t *testing.T, snapshot reference.StateSnapshot, userID, clientID string) reference.ClientLocalState {
	t.Helper()
	wanted := reference.ClientKey{UserID: reference.UserID(userID), ClientID: reference.ClientID(clientID)}
	for _, entry := range snapshot.ClientLocal {
		if entry.Key == wanted {
			return entry.Value
		}
	}
	t.Fatalf("client local state %q/%q is absent", userID, clientID)
	return reference.ClientLocalState{}
}

func configuredTargetValue(t *testing.T, family WorkloadSampleFamily, operation scenarios.Operation) uint64 {
	t.Helper()
	switch family {
	case WorkloadSampleFanout, WorkloadSampleImpact, WorkloadSampleBackfill:
		if scenarios.OperationKey(operation) != "model/stage-registry-membership-generation" {
			t.Fatalf("%s sample targets %s", family, scenarios.OperationKey(operation))
		}
		var stage struct {
			BatchSize  uint64 `json:"batch_size"`
			ScopeRules []struct {
				PositiveFanoutBound uint64 `json:"positive_fanout_bound"`
			} `json:"scope_rules"`
			DependencyImpacts []struct {
				PositiveRowBound uint64 `json:"positive_row_bound"`
			} `json:"dependency_impacts"`
		}
		if err := json.Unmarshal(operation.Payload, &stage); err != nil || len(stage.ScopeRules) != 1 || len(stage.DependencyImpacts) != 1 {
			t.Fatalf("decode %s stage target: %v", family, err)
		}
		switch family {
		case WorkloadSampleFanout:
			return stage.ScopeRules[0].PositiveFanoutBound
		case WorkloadSampleImpact:
			return stage.DependencyImpacts[0].PositiveRowBound
		default:
			return stage.BatchSize
		}
	case WorkloadSamplePull, WorkloadSampleRebuild:
		var request struct {
			Limit uint64 `json:"limit"`
		}
		if err := json.Unmarshal(operation.Payload, &request); err != nil {
			t.Fatalf("decode %s target: %v", family, err)
		}
		return request.Limit
	case WorkloadSampleCompaction:
		var request struct {
			BatchSize uint64 `json:"batch_size"`
		}
		if err := json.Unmarshal(operation.Payload, &request); err != nil {
			t.Fatalf("decode compaction target: %v", err)
		}
		return request.BatchSize
	case WorkloadSamplePush:
		var envelope struct {
			Request struct {
				Mutations []json.RawMessage `json:"mutations"`
			} `json:"request"`
		}
		if err := json.Unmarshal(operation.Payload, &envelope); err != nil {
			t.Fatalf("decode push target: %v", err)
		}
		return uint64(len(envelope.Request.Mutations))
	default:
		t.Fatalf("unknown configured sample family %q", family)
		return 0
	}
}

func strconvSampleName(total uint64) string {
	return "total-" + strconv.FormatUint(total, 10)
}
