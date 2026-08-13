package modelrunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func replayMatches(ctx context.Context, scenario scenarios.Scenario, result Result) (bool, error) {
	model, err := NewModel(result.Replay.Seed)
	if err != nil {
		return false, err
	}
	replayed, err := run(ctx, model, scenario, false)
	if err != nil {
		return false, fmt.Errorf("replay model run: %w", err)
	}
	if len(result.Setup) != len(replayed.Setup) || len(result.Steps) != len(replayed.Steps) {
		return false, errors.New("replay operation count changed")
	}
	for index := range result.Setup {
		if !sameExecution(result.Setup[index], replayed.Setup[index]) {
			return false, fmt.Errorf("replay setup snapshot changed at index %d", index)
		}
	}
	for index := range result.Steps {
		if !sameExecution(result.Steps[index], replayed.Steps[index]) {
			return false, fmt.Errorf("replay step snapshot changed at index %d", index)
		}
	}
	if !reflect.DeepEqual(result.FinalSnapshot, replayed.FinalSnapshot) {
		return false, errors.New("replay final model snapshot changed")
	}
	return true, nil
}

func sameExecution(left, right OperationExecution) bool {
	if left.StepID != right.StepID || left.OperationKey != right.OperationKey || !reflect.DeepEqual(left.Before, right.Before) || !reflect.DeepEqual(left.After, right.After) || !reflect.DeepEqual(left.Result, right.Result) || !reflect.DeepEqual(left.Expanded, right.Expanded) || !reflect.DeepEqual(left.Samples, right.Samples) {
		return false
	}
	leftCode, leftOK := ErrorCodeOK(left.Err)
	rightCode, rightOK := ErrorCodeOK(right.Err)
	return leftOK == rightOK && leftCode == rightCode
}

func transitionSemanticallyValid(result Result) bool {
	return transitionSemanticFailure(result) == ""
}

func transitionSemanticFailure(result Result) string {
	if len(result.Setup) != 1 || result.Setup[0].Err != nil || result.Setup[0].Result.Kind != reference.StepResultKindContractInstalled {
		return "the setup operation did not install the contract"
	}
	if result.Setup[0].After.ProtocolVersion != 3 || result.Setup[0].After.CurrentSchema == (reference.SchemaRef{}) || !result.Setup[0].After.Installation.Installed || result.Setup[0].After.Installation.ProtocolVersion != 3 {
		return "the setup snapshot does not contain the installed protocol 3 contract"
	}
	for _, execution := range result.Steps {
		if execution.Err != nil {
			if !reflect.DeepEqual(execution.Before, execution.After) {
				return fmt.Sprintf("step %s changed state after its expected error", execution.StepID)
			}
			continue
		}
		if execution.After.ProtocolVersion != 3 {
			return fmt.Sprintf("step %s changed the protocol version", execution.StepID)
		}
		if !resultKindMatches(execution) {
			return fmt.Sprintf("step %s returned the wrong result kind for %s", execution.StepID, execution.OperationKey)
		}
		if !operationTransitionChanged(execution) {
			return fmt.Sprintf("step %s did not produce the required transition for %s", execution.StepID, execution.OperationKey)
		}
	}
	return ""
}

func resultKindMatches(execution OperationExecution) bool {
	key := execution.OperationKey
	if len(execution.Expanded) > 0 {
		key = scenarios.OperationKey(execution.Expanded[len(execution.Expanded)-1])
	}
	want := map[string]reference.StepResultKind{
		"artifact/install-portable-seed":                reference.StepResultKindLocal,
		"connect/send":                                  reference.StepResultKindConnect,
		"local/apply-pull-page":                         reference.StepResultKindLocal,
		"local/apply-rebuild-page":                      reference.StepResultKindLocal,
		"local/begin-rebuild":                           reference.StepResultKindLocal,
		"local/finalize-rebuild":                        reference.StepResultKindLocal,
		"local/write":                                   reference.StepResultKindLocal,
		"model/activate-registry-membership-generation": reference.StepResultKindSchema,
		"model/commit-source-transaction":               reference.StepResultKindWAL,
		"model/compact-scope":                           reference.StepResultKindRetention,
		"model/expire-client-generation":                reference.StepResultKindClient,
		"model/install-current-contract":                reference.StepResultKindContractInstalled,
		"model/publish-schema":                          reference.StepResultKindSchema,
		"model/set-client-assignments":                  reference.StepResultKindClient,
		"model/stage-registry-membership-generation":    reference.StepResultKindSchema,
		"process/acknowledge-contiguous-prefix":         reference.StepResultKindWAL,
		"process/materialize-source-transaction":        reference.StepResultKindWAL,
		"process/repair-and-retry-source-transaction":   reference.StepResultKindWAL,
		"process/response-loss":                         reference.StepResultKindPush,
		"process/restart-client":                        reference.StepResultKindLifecycle,
		"process/restart-wal-worker":                    reference.StepResultKindWAL,
		"pull/request-page":                             reference.StepResultKindPull,
		"push/submit":                                   reference.StepResultKindPush,
		"rebuild/request-page":                          reference.StepResultKindRebuild,
	}
	wanted, ok := want[key]
	return ok && execution.Result.Kind == wanted
}

func operationTransitionChanged(execution OperationExecution) bool {
	key := execution.OperationKey
	if len(execution.Expanded) > 0 {
		key = scenarios.OperationKey(execution.Expanded[len(execution.Expanded)-1])
	}
	before, after := execution.Before, execution.After
	switch key {
	case "model/install-current-contract":
		return after.CurrentSchema != (reference.SchemaRef{}) && after.Installation.ProtocolVersion == 3
	case "model/commit-source-transaction":
		return len(after.Stream.Transactions) > len(before.Stream.Transactions)
	case "process/materialize-source-transaction", "process/repair-and-retry-source-transaction":
		if len(after.Stream.Materializations) > len(before.Stream.Materializations) || after.Stream.Authority.GlobalMaterializationBoundary != before.Stream.Authority.GlobalMaterializationBoundary || !reflect.DeepEqual(before.Stream.Poison, after.Stream.Poison) {
			return true
		}
		return execution.Result.WAL != nil && execution.Result.WAL.PriorMaterialization == execution.Result.WAL.NewMaterialization
	case "process/acknowledge-contiguous-prefix":
		return after.Stream.Authority.AcknowledgedEndLSN >= before.Stream.Authority.AcknowledgedEndLSN
	case "process/restart-wal-worker", "process/restart-client", "process/response-loss":
		return len(after.Events) > len(before.Events)
	case "model/stage-registry-membership-generation":
		return len(after.Registry.Generations) > len(before.Registry.Generations)
	case "model/activate-registry-membership-generation":
		return after.Registry.CurrentGeneration >= before.Registry.CurrentGeneration
	case "model/publish-schema":
		return len(after.Schemas) > len(before.Schemas) && after.CurrentSchema != before.CurrentSchema
	case "model/set-client-assignments":
		return !reflect.DeepEqual(before.Clients, after.Clients)
	case "model/expire-client-generation":
		return !reflect.DeepEqual(before.Clients, after.Clients)
	case "model/compact-scope":
		return !reflect.DeepEqual(before.Scopes, after.Scopes) || !reflect.DeepEqual(before.RetentionFloors, after.RetentionFloors)
	case "local/write":
		return localQueueOrRowsChanged(before, after)
	case "local/apply-pull-page", "artifact/install-portable-seed":
		return !reflect.DeepEqual(before.ClientLocal, after.ClientLocal)
	case "local/begin-rebuild", "local/apply-rebuild-page", "local/finalize-rebuild":
		return !reflect.DeepEqual(before.Rebuilds, after.Rebuilds) || !reflect.DeepEqual(before.ClientLocal, after.ClientLocal)
	case "push/submit":
		return !reflect.DeepEqual(before.Batches, after.Batches) || !reflect.DeepEqual(before.Mutations, after.Mutations) || !reflect.DeepEqual(before.ClientLocal, after.ClientLocal) || execution.Result.Push != nil && execution.Result.Push.Replay == reference.ReplayDispositionReplayed
	case "pull/request-page", "rebuild/request-page", "connect/send":
		return execution.Result.HTTP != nil && execution.Result.HTTP.Status >= 200 && execution.Result.HTTP.Status < 600
	default:
		return false
	}
}

func localQueueOrRowsChanged(before, after reference.StateSnapshot) bool {
	if len(before.ClientLocal) != len(after.ClientLocal) {
		return true
	}
	for index := range after.ClientLocal {
		if !reflect.DeepEqual(before.ClientLocal[index].Value.DurableQueue, after.ClientLocal[index].Value.DurableQueue) || !reflect.DeepEqual(before.ClientLocal[index].Value.Rows, after.ClientLocal[index].Value.Rows) {
			return true
		}
	}
	return false
}

func performanceContractSatisfied(scenarioID string, result Result) bool {
	if !transitionSemanticallyValid(result) {
		return false
	}
	switch scenarioID {
	case "SCN-PERF-WARM-CONNECT-001":
		return exactOperationCount(result, "connect/send") == 1 && exactHTTPCount(result) == 1
	case "SCN-PERF-STEADY-PULL-001":
		return steadyPullSatisfied(result)
	case "SCN-PERF-PENDING-CYCLE-001":
		return pendingCycleSatisfied(result)
	case "SCN-PERF-REBUILD-REQUESTS-001":
		return exactOperationCount(result, "connect/send") == 1 && exactOperationCount(result, "pull/request-page") == 1 && exactOperationCount(result, "rebuild/request-page") == 1 && rebuildPageIsBounded(result)
	case "SCN-PERF-CORE-SYNC-PATH-001":
		return exactOperationCount(result, "connect/send") == 1 && exactOperationCount(result, "push/submit") == 1 && exactOperationCount(result, "pull/request-page") == 1 && exactOperationCount(result, "rebuild/request-page") == 1 && exactHTTPCount(result) == 4
	case "SCN-PERF-FANOUT-001":
		return profileStrataSatisfied(result, "scope_topology", map[uint64]int{1: 3, 2: 3, 8: 3}, true)
	case "SCN-PERF-SHARED-PRIVATE-SCOPES-001":
		return topologyPairStrataSatisfied(result, map[string]int{"1/1000": 3, "8/1000": 3})
	case "SCN-PERF-REBUILD-CARDINALITY-001", "SCN-PERF-REBUILD-APPLY-001":
		return profileStrataSatisfied(result, "scope_cardinality", map[uint64]int{1: 3, 101: 3, 1000: 3}, false)
	case "SCN-PERF-SCHEMA-CHECK-001":
		return exactOperationCount(result, "connect/send") == 18 && exactHTTPCount(result) == 18
	case "SCN-PERF-SEEDED-EMPTY-STARTUP-001":
		return seededEmptyStartupSatisfied(result)
	case "SCN-PERF-QUEUE-REPLAY-001":
		return queueReplayStrataSatisfied(result)
	case "SCN-PERF-MULTI-SCOPE-PROVENANCE-001":
		return profileStrataSatisfied(result, "scope_topology", map[uint64]int{1: 3, 2: 3}, false)
	case "SCN-PERF-CONFIGURED-BOUNDS-001":
		return configuredLimitsSatisfied(result)
	default:
		return false
	}
}

func exactOperationCount(result Result, key string) int {
	count := 0
	for _, execution := range result.Steps {
		if execution.OperationKey == key {
			count++
		}
	}
	return count
}

func exactHTTPCount(result Result) int {
	count := 0
	for _, execution := range result.Steps {
		if execution.Operation.ContractOperation == "connect" || execution.Operation.ContractOperation == "push" || execution.Operation.ContractOperation == "pull" || execution.Operation.ContractOperation == "rebuild" {
			count++
		}
	}
	return count
}

func steadyPullSatisfied(result Result) bool {
	if exactOperationCount(result, "pull/request-page") != 1 || exactOperationCount(result, "local/apply-pull-page") != 1 {
		return false
	}
	for _, execution := range result.Steps {
		if execution.OperationKey == "pull/request-page" {
			return execution.Result.Pull != nil && !execution.Result.Pull.HasMore && len(execution.Result.Pull.ScopeChecksums) > 0
		}
	}
	return false
}

func pendingCycleSatisfied(result Result) bool {
	if exactOperationCount(result, "local/write") != 1 || exactOperationCount(result, "push/submit") != 1 || exactOperationCount(result, "pull/request-page") != 1 {
		return false
	}
	for _, execution := range result.Steps {
		if execution.OperationKey == "push/submit" {
			if execution.Result.Push == nil || len(execution.Result.Push.Mutations) == 0 {
				return false
			}
			for _, mutation := range execution.Result.Push.Mutations {
				if mutation.State != reference.MutationOutcomeApplied && mutation.State != reference.MutationOutcomeConflict && mutation.State != reference.MutationOutcomeRejectedTerminal {
					return false
				}
			}
		}
	}
	return true
}

func rebuildPageIsBounded(result Result) bool {
	for _, execution := range result.Steps {
		if execution.OperationKey == "rebuild/request-page" {
			return execution.Result.Rebuild != nil && execution.Result.Rebuild.PageOrdinal >= 1
		}
	}
	return false
}

func profileStrataSatisfied(result Result, profile string, wanted map[uint64]int, impactMatches bool) bool {
	counts := make(map[uint64]int)
	for _, execution := range result.Steps {
		if execution.OperationKey != "workload/prepare" {
			continue
		}
		value, impact, gotProfile, ok := workloadNumbers(execution.Operation.Payload)
		if !ok || gotProfile != profile {
			return false
		}
		counts[value]++
		if impactMatches && value != impact {
			return false
		}
		if len(execution.Expanded) == 0 {
			return false
		}
	}
	return reflect.DeepEqual(counts, wanted)
}

func topologyPairStrataSatisfied(result Result, wanted map[string]int) bool {
	counts := make(map[string]int)
	for _, execution := range result.Steps {
		if execution.OperationKey != "workload/prepare" {
			continue
		}
		fanout, impact, profile, ok := workloadNumbers(execution.Operation.Payload)
		if !ok || profile != "scope_topology" || len(execution.Expanded) == 0 {
			return false
		}
		counts[fmt.Sprintf("%d/%d", fanout, impact)]++
	}
	return reflect.DeepEqual(counts, wanted)
}

func queueReplayStrataSatisfied(result Result) bool {
	wanted := map[uint64]int{2: 3, 100: 3, 1000: 3}
	counts := make(map[uint64]int)
	for _, execution := range result.Steps {
		if execution.OperationKey != "workload/prepare" {
			continue
		}
		profile, accepted, rejected, ok := pendingWorkloadNumbers(execution.Operation.Payload)
		if !ok || profile != "pending_mutations" || rejected != 1 {
			return false
		}
		counts[accepted+rejected]++
	}
	return reflect.DeepEqual(counts, wanted)
}

func configuredLimitsSatisfied(result Result) bool {
	if exactOperationCount(result, "workload/prepare") != 1 {
		return false
	}
	var authored map[string]json.RawMessage
	if err := json.Unmarshal(result.Steps[0].Operation.Payload, &authored); err != nil || len(authored) != 7 {
		return false
	}
	for _, name := range []string{"profile", "max_scope_fanout", "max_impact_rows", "pull_maximum", "rebuild_maximum", "compaction_batch_maximum", "backfill_batch_maximum"} {
		if _, found := authored[name]; !found {
			return false
		}
	}
	var payload struct {
		Profile                string `json:"profile"`
		MaxScopeFanout         uint64 `json:"max_scope_fanout"`
		MaxImpactRows          uint64 `json:"max_impact_rows"`
		PullMaximum            uint64 `json:"pull_maximum"`
		RebuildMaximum         uint64 `json:"rebuild_maximum"`
		CompactionBatchMaximum uint64 `json:"compaction_batch_maximum"`
		BackfillBatchMaximum   uint64 `json:"backfill_batch_maximum"`
	}
	if err := json.Unmarshal(result.Steps[0].Operation.Payload, &payload); err != nil || payload.Profile != "configured_limits" {
		return false
	}
	if payload.MaxScopeFanout != 8 || payload.MaxImpactRows != 1000 || payload.PullMaximum != 1000 || payload.RebuildMaximum != 1000 || payload.CompactionBatchMaximum != 10000 || payload.BackfillBatchMaximum != 1000 {
		return false
	}
	return configuredSampleRecordsSatisfied(result.Steps[0])
}

func configuredSampleRecordsSatisfied(execution OperationExecution) bool {
	maximums := map[WorkloadSampleFamily]uint64{
		WorkloadSampleFanout:     8,
		WorkloadSampleImpact:     1000,
		WorkloadSamplePull:       1000,
		WorkloadSampleRebuild:    1000,
		WorkloadSampleCompaction: 10000,
		WorkloadSampleBackfill:   1000,
		WorkloadSamplePush:       configuredPushMutationMaximum,
	}
	targets := map[WorkloadSampleFamily]string{
		WorkloadSampleFanout:     "model/stage-registry-membership-generation",
		WorkloadSampleImpact:     "model/stage-registry-membership-generation",
		WorkloadSamplePull:       "pull/request-page",
		WorkloadSampleRebuild:    "rebuild/request-page",
		WorkloadSampleCompaction: "model/compact-scope",
		WorkloadSampleBackfill:   "model/stage-registry-membership-generation",
		WorkloadSamplePush:       "push/submit",
	}
	type stratum struct {
		family   WorkloadSampleFamily
		boundary WorkloadSampleBoundary
	}
	counts := make(map[stratum]int)
	for _, sample := range execution.Samples {
		maximum, known := maximums[sample.Family]
		if !known || sample.ExpandedOperationIndex < 0 || sample.ExpandedOperationIndex >= len(execution.Expanded) {
			return false
		}
		if scenarios.OperationKey(execution.Expanded[sample.ExpandedOperationIndex]) != targets[sample.Family] {
			return false
		}
		wantValue := uint64(0)
		switch sample.Boundary {
		case WorkloadBoundaryLower:
			wantValue = 1
		case WorkloadBoundaryUpper:
			wantValue = maximum
		case WorkloadBoundaryInvalid:
			wantValue = maximum + 1
		default:
			return false
		}
		if sample.Value != wantValue || !configuredSampleOutcomeSatisfied(sample) {
			return false
		}
		counts[stratum{family: sample.Family, boundary: sample.Boundary}]++
	}
	if len(execution.Samples) != len(maximums)*3*3 {
		return false
	}
	for family := range maximums {
		for _, boundary := range []WorkloadSampleBoundary{WorkloadBoundaryLower, WorkloadBoundaryUpper, WorkloadBoundaryInvalid} {
			if counts[stratum{family: family, boundary: boundary}] != 3 {
				return false
			}
		}
	}
	return true
}

func configuredSampleOutcomeSatisfied(sample WorkloadSampleExecution) bool {
	if sample.Boundary == WorkloadBoundaryInvalid {
		if !reflect.DeepEqual(sample.Before, sample.After) {
			return false
		}
		switch sample.Family {
		case WorkloadSampleFanout, WorkloadSampleImpact, WorkloadSampleBackfill, WorkloadSampleCompaction:
			return sample.Result == nil && sample.ErrorCode == "invalid_limit"
		case WorkloadSamplePull, WorkloadSampleRebuild, WorkloadSamplePush:
			return sample.ErrorCode == "" && sample.Result != nil && sample.Result.HTTP != nil && sample.Result.HTTP.Status == 400 && sample.Result.HTTP.HasCode && sample.Result.HTTP.Code == "invalid_request"
		default:
			return false
		}
	}
	if sample.ErrorCode != "" || sample.Result == nil {
		return false
	}
	result := sample.Result
	switch sample.Family {
	case WorkloadSampleFanout, WorkloadSampleImpact:
		return result.Kind == reference.StepResultKindSchema && result.Schema != nil && result.Schema.Reason == "membership_generation_staged"
	case WorkloadSampleBackfill:
		if result.Kind != reference.StepResultKindSchema || result.Schema == nil || result.Schema.BatchSize != sample.Value {
			return false
		}
		rowCount := uint64(len(sample.Before.Rows))
		return result.Schema.BatchCount == (rowCount+sample.Value-1)/sample.Value
	case WorkloadSamplePull:
		return successfulSampleHTTP(result, reference.StepResultKindPull) && result.Pull != nil
	case WorkloadSampleRebuild:
		return successfulSampleHTTP(result, reference.StepResultKindRebuild) && result.Rebuild != nil && uint64(len(result.Rebuild.Records)) <= sample.Value
	case WorkloadSampleCompaction:
		return result.Kind == reference.StepResultKindRetention && result.Retention != nil && result.Retention.BatchSize == sample.Value && result.Retention.DeletedCount <= sample.Value
	case WorkloadSamplePush:
		return successfulSampleHTTP(result, reference.StepResultKindPush) && result.Push != nil && uint64(len(result.Push.Mutations)) == sample.Value
	default:
		return false
	}
}

func successfulSampleHTTP(result *reference.StepResult, kind reference.StepResultKind) bool {
	return result.Kind == kind && result.HTTP != nil && result.HTTP.Status == 200 && !result.HTTP.HasCode
}

func seededEmptyStartupSatisfied(result Result) bool {
	if len(result.Setup) != 1 || len(result.Setup[0].After.Clients) != 6 || exactOperationCount(result, "artifact/install-portable-seed") != 3 || exactOperationCount(result, "model/set-client-assignments") != 6 || exactOperationCount(result, "connect/send") != 6 || exactHTTPCount(result) != 6 || len(result.Steps) != 15 {
		return false
	}
	for _, entry := range result.Setup[0].After.Clients {
		if len(entry.Value.ScopeAssignments) != 0 {
			return false
		}
	}
	seeded := make(map[reference.ClientKey]struct{}, 3)
	empty := make(map[reference.ClientKey]struct{}, 3)
	for index := 0; index < 9; index += 3 {
		install, assignment, connect := result.Steps[index], result.Steps[index+1], result.Steps[index+2]
		client, ok := operationClient(install.Operation.Payload)
		if !ok || install.OperationKey != "artifact/install-portable-seed" || assignment.OperationKey != "model/set-client-assignments" || connect.OperationKey != "connect/send" || !sameOperationClient(assignment.Operation.Payload, client) || !sameOperationClient(connect.Operation.Payload, client) || !seedInstallationSatisfied(install, client) || !assignmentSatisfied(assignment, client) || !seededConnectSatisfied(connect, client) {
			return false
		}
		if _, duplicate := seeded[client]; duplicate {
			return false
		}
		seeded[client] = struct{}{}
	}
	for index := 9; index < 15; index += 2 {
		assignment, connect := result.Steps[index], result.Steps[index+1]
		client, ok := operationClient(assignment.Operation.Payload)
		if !ok || assignment.OperationKey != "model/set-client-assignments" || connect.OperationKey != "connect/send" || !sameOperationClient(connect.Operation.Payload, client) || !assignmentSatisfied(assignment, client) || !emptyConnectSatisfied(connect, client) {
			return false
		}
		if _, duplicate := empty[client]; duplicate {
			return false
		}
		empty[client] = struct{}{}
	}
	return len(seeded) == 3 && len(empty) == 3
}

func operationClient(payload json.RawMessage) (reference.ClientKey, bool) {
	var value struct {
		UserID   string `json:"user_id"`
		ClientID string `json:"client_id"`
	}
	if json.Unmarshal(payload, &value) != nil || value.UserID == "" || value.ClientID == "" {
		return reference.ClientKey{}, false
	}
	return reference.ClientKey{UserID: reference.UserID(value.UserID), ClientID: reference.ClientID(value.ClientID)}, true
}

func sameOperationClient(payload json.RawMessage, wanted reference.ClientKey) bool {
	got, ok := operationClient(payload)
	return ok && got == wanted
}

func seedInstallationSatisfied(execution OperationExecution, client reference.ClientKey) bool {
	beforeClient, beforeClientFound := snapshotClient(execution.Before.Clients, client)
	afterClient, afterClientFound := snapshotClient(execution.After.Clients, client)
	if execution.Result.Local == nil || !reflect.DeepEqual(execution.Before.Authorization, execution.After.Authorization) || !beforeClientFound || !afterClientFound || !reflect.DeepEqual(beforeClient, afterClient) {
		return false
	}
	before, beforeFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	after, afterFound := snapshotLocalClient(execution.After.ClientLocal, client)
	if !beforeFound || !afterFound || len(before.Rows) != 0 || len(before.Provenance) != 0 || len(before.SeedReceipts) != 0 || len(before.ScopeCheckpoints) != 0 {
		return false
	}
	return len(after.Rows) == 1000 && len(after.Provenance) == 1000 && len(after.SeedReceipts) == 1 && after.SeedReceipts[0].Scope == "scope-a" && after.SeedReceipts[0].Cardinality == 1000 && len(after.ScopeCheckpoints) == 0
}

func assignmentSatisfied(execution OperationExecution, client reference.ClientKey) bool {
	if execution.Result.Client == nil || execution.Result.Client.Client != client || !reflect.DeepEqual(execution.Before.Authorization, execution.After.Authorization) {
		return false
	}
	before, beforeFound := snapshotClient(execution.Before.Clients, client)
	after, afterFound := snapshotClient(execution.After.Clients, client)
	if !beforeFound || !afterFound || len(before.ScopeAssignments) != 0 || len(after.ScopeAssignments) != 1 || after.ScopeAssignments[0].Scope != "scope-a" || !after.ScopeAssignments[0].Assigned || !after.ScopeAssignments[0].RebuildRequired || after.ScopeSetVersion != before.ScopeSetVersion+1 {
		return false
	}
	return true
}

func seededConnectSatisfied(execution OperationExecution, client reference.ClientKey) bool {
	if execution.Result.Connect == nil || execution.Result.HTTP == nil || execution.Result.HTTP.Status != 200 || execution.Result.Connect.Client != client || execution.Result.Connect.Schema.Action != reference.SchemaActionNone || !reflect.DeepEqual(execution.Before.Authorization, execution.After.Authorization) || !connectHasSeedReceipt(execution.Operation.Payload) || !connectHasNoKnownScopes(execution.Operation.Payload) {
		return false
	}
	if !reflect.DeepEqual(execution.Result.Connect.AddedScopes, []reference.ScopeID{"scope-a"}) || !connectCursorDisposition(execution.Result.Connect, "scope-a", reference.CursorDispositionIssued) {
		return false
	}
	before, beforeFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	after, afterFound := snapshotLocalClient(execution.After.ClientLocal, client)
	if !beforeFound || !afterFound || len(before.SeedReceipts) != 1 || len(after.Rows) != 1000 || len(after.Provenance) != 1000 || len(after.SeedReceipts) != 0 {
		return false
	}
	assignment, assignmentFound := localAssignment(after, "scope-a")
	checkpoint, checkpointFound := localCheckpoint(after, "scope-a")
	return assignmentFound && assignment.Assigned && !assignment.RebuildRequired && checkpointFound && checkpoint.HasCursor && checkpoint.Position == before.SeedReceipts[0].SnapshotBoundary
}

func emptyConnectSatisfied(execution OperationExecution, client reference.ClientKey) bool {
	if execution.Result.Connect == nil || execution.Result.HTTP == nil || execution.Result.HTTP.Status != 200 || execution.Result.Connect.Client != client || execution.Result.Connect.Schema.Action != reference.SchemaActionNone || !reflect.DeepEqual(execution.Before.Authorization, execution.After.Authorization) || connectHasSeedReceipt(execution.Operation.Payload) || !connectHasNoKnownScopes(execution.Operation.Payload) {
		return false
	}
	if !reflect.DeepEqual(execution.Result.Connect.AddedScopes, []reference.ScopeID{"scope-a"}) || !connectCursorDisposition(execution.Result.Connect, "scope-a", reference.CursorDispositionRebuildRequired) {
		return false
	}
	before, beforeFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	after, afterFound := snapshotLocalClient(execution.After.ClientLocal, client)
	if !beforeFound || !afterFound || len(before.Rows) != 0 || len(before.Provenance) != 0 || len(before.SeedReceipts) != 0 || len(after.Rows) != 0 || len(after.Provenance) != 0 || len(after.SeedReceipts) != 0 {
		return false
	}
	assignment, assignmentFound := localAssignment(after, "scope-a")
	checkpoint, checkpointFound := localCheckpoint(after, "scope-a")
	return assignmentFound && assignment.Assigned && assignment.RebuildRequired && checkpointFound && !checkpoint.HasCursor
}

func snapshotClient(entries []reference.SnapshotEntry[reference.ClientKey, reference.ClientState], client reference.ClientKey) (reference.ClientState, bool) {
	for _, entry := range entries {
		if entry.Key == client {
			return entry.Value, true
		}
	}
	return reference.ClientState{}, false
}

func snapshotLocalClient(entries []reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState], client reference.ClientKey) (reference.ClientLocalState, bool) {
	for _, entry := range entries {
		if entry.Key == client {
			return entry.Value, true
		}
	}
	return reference.ClientLocalState{}, false
}

func localAssignment(local reference.ClientLocalState, scope reference.ScopeID) (reference.LocalScopeAssignment, bool) {
	for _, assignment := range local.ScopeAssignments {
		if assignment.Scope == scope {
			return assignment, true
		}
	}
	return reference.LocalScopeAssignment{}, false
}

func localCheckpoint(local reference.ClientLocalState, scope reference.ScopeID) (reference.LocalScopeCheckpoint, bool) {
	for _, checkpoint := range local.ScopeCheckpoints {
		if checkpoint.Scope == scope {
			return checkpoint, true
		}
	}
	return reference.LocalScopeCheckpoint{}, false
}

func connectHasSeedReceipt(payload json.RawMessage) bool {
	var value struct {
		SeedReceipts map[string]string `json:"seed_receipts"`
	}
	return json.Unmarshal(payload, &value) == nil && len(value.SeedReceipts) == 1 && value.SeedReceipts["scope-a"] == "local_seed_receipt"
}

func connectHasNoKnownScopes(payload json.RawMessage) bool {
	var value struct {
		KnownScopes []json.RawMessage `json:"known_scopes"`
	}
	return json.Unmarshal(payload, &value) == nil && value.KnownScopes != nil && len(value.KnownScopes) == 0
}

func connectCursorDisposition(connect *reference.ConnectObservation, scope reference.ScopeID, wanted reference.CursorDisposition) bool {
	if connect == nil || len(connect.ScopeCursors) != 1 {
		return false
	}
	return connect.ScopeCursors[0].Scope == scope && connect.ScopeCursors[0].Disposition == wanted
}

func workloadNumbers(raw json.RawMessage) (value, impact uint64, profile string, ok bool) {
	var payload struct {
		Profile     string `json:"profile"`
		ScopeFanout uint64 `json:"scope_fanout"`
		ImpactRows  uint64 `json:"impact_rows"`
		RecordCount uint64 `json:"record_count"`
		PageSize    uint64 `json:"page_size"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return 0, 0, "", false
	}
	switch payload.Profile {
	case "scope_topology":
		return payload.ScopeFanout, payload.ImpactRows, payload.Profile, true
	case "scope_cardinality":
		return payload.RecordCount, payload.PageSize, payload.Profile, true
	default:
		return 0, 0, payload.Profile, false
	}
}

func pendingWorkloadNumbers(raw json.RawMessage) (profile string, accepted, rejected uint64, ok bool) {
	var payload struct {
		Profile  string `json:"profile"`
		Accepted uint64 `json:"accepted_count"`
		Rejected uint64 `json:"rejected_count"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return "", 0, 0, false
	}
	return payload.Profile, payload.Accepted, payload.Rejected, payload.Profile == "pending_mutations"
}

// Keep the closed evaluator's operation order deterministic for diagnostics.
func sortedOperationKeys(result Result) []string {
	keys := make([]string, 0, len(result.Steps))
	for _, execution := range result.Steps {
		keys = append(keys, execution.OperationKey)
	}
	sort.Strings(keys)
	return keys
}
