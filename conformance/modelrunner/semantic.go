package modelrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
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
	switch scenarioID {
	case "SCN-PERF-WARM-CONNECT-001":
		return warmConnectSatisfied(result)
	case "SCN-PERF-STEADY-PULL-001":
		return steadyPullSatisfied(result)
	case "SCN-PERF-PENDING-CYCLE-001":
		return pendingCycleSatisfied(result)
	case "SCN-PERF-REBUILD-REQUESTS-001":
		return rebuildRequestsSatisfied(result)
	case "SCN-PERF-CORE-SYNC-PATH-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return exactOperationCount(result, "connect/send") == 1 && exactOperationCount(result, "push/submit") == 1 && exactOperationCount(result, "pull/request-page") == 1 && exactOperationCount(result, "rebuild/request-page") == 1 && exactHTTPCount(result) == 4
	case "SCN-PERF-FANOUT-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return profileStrataSatisfied(result, "scope_topology", map[uint64]int{1: 3, 2: 3, 8: 3}, true)
	case "SCN-PERF-SHARED-PRIVATE-SCOPES-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return topologyPairStrataSatisfied(result, map[string]int{"1/1000": 3, "8/1000": 3})
	case "SCN-PERF-REBUILD-CARDINALITY-001", "SCN-PERF-REBUILD-APPLY-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return profileStrataSatisfied(result, "scope_cardinality", map[uint64]int{1: 3, 101: 3, 1000: 3}, false)
	case "SCN-PERF-SEEDED-EMPTY-STARTUP-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return seededEmptyStartupSatisfied(result)
	case "SCN-PERF-QUEUE-REPLAY-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return queueReplayStrataSatisfied(result)
	case "SCN-PERF-MULTI-SCOPE-PROVENANCE-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
		return profileStrataSatisfied(result, "scope_topology", map[uint64]int{1: 3, 2: 3}, false)
	case "SCN-PERF-CONFIGURED-BOUNDS-001":
		if !transitionSemanticallyValid(result) {
			return false
		}
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

type semanticConnectScope struct {
	ScopeID string `json:"scope_id"`
}

type semanticSchemaReference struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type semanticConnectRequest struct {
	UserID           string                  `json:"user_id"`
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	Schema           semanticSchemaReference `json:"schema"`
	ScopeSetVersion  uint64                  `json:"scope_set_version"`
	KnownScopes      []semanticConnectScope  `json:"known_scopes"`
}

type semanticPullScope struct {
	ScopeID      string `json:"scope_id"`
	CursorSource string `json:"cursor_source"`
}

type semanticPullRequest struct {
	UserID           string                  `json:"user_id"`
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	Schema           semanticSchemaReference `json:"schema"`
	ScopeSetVersion  uint64                  `json:"scope_set_version"`
	Scopes           []semanticPullScope     `json:"scopes"`
	Limit            uint64                  `json:"limit"`
}

type semanticLocalWriteRequest struct {
	AuthenticatedUserID string `json:"authenticated_user_id"`
	ClientID            string `json:"client_id"`
	MutationID          string `json:"mutation_id"`
	TableID             string `json:"table_id"`
	Operation           string `json:"operation"`
}

type semanticPushMutation struct {
	MutationID string `json:"mutation_id"`
	Table      string `json:"table"`
	Operation  string `json:"op"`
}

type semanticPushRequest struct {
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	BatchID          string                  `json:"batch_id"`
	Schema           semanticSchemaReference `json:"schema"`
	Mutations        []semanticPushMutation  `json:"mutations"`
}

type semanticPushEnvelope struct {
	AuthenticatedUserID string              `json:"authenticated_user_id"`
	Request             semanticPushRequest `json:"request"`
	CommitLSN           string              `json:"commit_lsn"`
	EndLSN              string              `json:"end_lsn"`
}

type semanticMaterializeRequest struct {
	StreamGeneration string `json:"stream_generation"`
	CommitLSN        string `json:"commit_lsn"`
}

type semanticRebuildRequest struct {
	UserID           string                  `json:"user_id"`
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	Schema           semanticSchemaReference `json:"schema"`
	ScopeID          string                  `json:"scope_id"`
	RebuildID        string                  `json:"rebuild_id"`
	CursorSource     string                  `json:"cursor_source"`
	Limit            uint64                  `json:"limit"`
}

type semanticPushChecksum struct {
	Algorithm string `json:"algorithm"`
	Version   uint64 `json:"version"`
	Encoding  string `json:"encoding"`
	Digest    string `json:"digest"`
}

type semanticPushOutcome struct {
	MutationID    string                     `json:"mutation_id"`
	Table         string                     `json:"table"`
	Status        string                     `json:"status"`
	ServerRow     map[string]json.RawMessage `json:"server_row"`
	ServerVersion *string                    `json:"server_version"`
	RowChecksum   *semanticPushChecksum      `json:"row_checksum"`
}

type semanticPushResponse struct {
	BatchID  string            `json:"batch_id"`
	Accepted []json.RawMessage `json:"accepted"`
	Rejected []json.RawMessage `json:"rejected"`
}

func warmConnectSatisfied(result Result) bool {
	steps, ok := exactSemanticSteps(
		result,
		"model/set-client-assignments",
		"connect/send",
		"rebuild/request-page",
		"local/begin-rebuild",
		"local/apply-rebuild-page",
		"local/finalize-rebuild",
		"pull/request-page",
		"local/apply-pull-page",
		"model/commit-source-transaction",
		"process/materialize-source-transaction",
		"connect/send",
		"pull/request-page",
		"local/apply-pull-page",
	)
	if !ok || !freshRebuildAssignmentSatisfied(steps[0]) || !freshRebuildConnectSatisfied(steps[1]) {
		return false
	}
	baseline, ok := warmPullBaselineSatisfied(steps[2], steps[3], steps[4], steps[5])
	if !ok || !warmBaselineAcknowledgementSatisfied(steps[6], steps[7], baseline) {
		return false
	}
	if !steadyPullMaterializationSatisfied(steps[8], steps[9], baseline.Scope) || !warmConnectExecutionSatisfied(steps[10]) {
		return false
	}
	return steadyTerminalPullSatisfied(steps[11], baseline) && steadyTerminalApplySatisfied(steps[12], steps[11], baseline)
}

func warmBaselineAcknowledgementSatisfied(pull, apply OperationExecution, baseline steadyPullBaseline) bool {
	if !successfulEndpointResult(pull.Result, reference.StepResultKindPull, false) || pull.Result.Pull == nil || !reflect.DeepEqual(pull.Before.ClientLocal, pull.After.ClientLocal) {
		return false
	}
	var request semanticPullRequest
	if json.Unmarshal(pull.Operation.Payload, &request) != nil || request.UserID != string(baseline.Client.UserID) || request.ClientID != string(baseline.Client.ClientID) || len(request.Scopes) != 1 || request.Scopes[0].ScopeID != string(baseline.Scope) || request.Scopes[0].CursorSource != "local_checkpoint" || !semanticSchemaMatches(request.Schema, pull.Before.CurrentSchema) {
		return false
	}
	beforeServer, beforeFound := snapshotClient(pull.Before.Clients, baseline.Client)
	afterServer, afterFound := snapshotClient(pull.After.Clients, baseline.Client)
	local, localFound := snapshotLocalClient(pull.Before.ClientLocal, baseline.Client)
	checkpoint, checkpointFound := localCheckpoint(local, baseline.Scope)
	beforeAssignment, beforeAssignmentFound := serverAssignment(beforeServer, baseline.Scope)
	afterAssignment, afterAssignmentFound := serverAssignment(afterServer, baseline.Scope)
	if !beforeFound || !afterFound || !localFound || !checkpointFound || checkpoint != baseline.Checkpoint || !beforeAssignmentFound || !afterAssignmentFound || !beforeAssignment.RebuildRequired || afterAssignment.RebuildRequired {
		return false
	}
	observation := pull.Result.Pull
	if len(observation.Changes) != 0 || len(observation.AddedScopes) != 0 || len(observation.RemovedScopes) != 0 || len(observation.RebuildScopes) != 0 || observation.HasMore || len(observation.ScopeCursors) != 1 || observation.ScopeCursors[0].Scope != baseline.Scope || observation.ScopeCursors[0].Disposition != reference.CursorDispositionAcknowledged {
		return false
	}
	if !scopeChecksumsMatch(observation.ScopeChecksums, pull.Before, beforeServer) || !sameScopeAssignmentLineage(beforeServer.ScopeAssignments, afterServer.ScopeAssignments) || !sameStateExceptServerClient(pull.Before, pull.After, baseline.Client) {
		return false
	}
	if apply.Err != nil || apply.Result.Kind != reference.StepResultKindLocal || apply.Result.Local == nil || apply.Result.Local.Client != baseline.Client || apply.Result.Local.Status != reference.LocalMutationStatusAccepted || !reflect.DeepEqual(apply.Before, pull.After) || !reflect.DeepEqual(apply.Before, apply.After) {
		return false
	}
	var applyRequest struct {
		UserID       string `json:"user_id"`
		ClientID     string `json:"client_id"`
		SourceStepID string `json:"source_step_id"`
	}
	return json.Unmarshal(apply.Operation.Payload, &applyRequest) == nil && applyRequest.UserID == string(baseline.Client.UserID) && applyRequest.ClientID == string(baseline.Client.ClientID) && applyRequest.SourceStepID == string(pull.StepID)
}

func warmConnectExecutionSatisfied(execution OperationExecution) bool {
	if !connectExecutionSatisfied(execution) {
		return false
	}
	var request semanticConnectRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil {
		return false
	}
	connect := execution.Result.Connect
	client := connect.Client
	if request.UserID != string(client.UserID) || request.ClientID != string(client.ClientID) {
		return false
	}
	beforeClient, beforeFound := snapshotClient(execution.Before.Clients, client)
	afterClient, afterFound := snapshotClient(execution.After.Clients, client)
	beforeLocal, beforeLocalFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	afterLocal, afterLocalFound := snapshotLocalClient(execution.After.ClientLocal, client)
	if !beforeFound || !afterFound || !beforeLocalFound || !afterLocalFound {
		return false
	}
	if request.ClientGeneration != uint64(beforeClient.CurrentGeneration) || request.ScopeSetVersion != uint64(beforeClient.ScopeSetVersion) || !semanticSchemaMatches(request.Schema, execution.Before.CurrentSchema) {
		return false
	}
	if !assignmentLineageMatchesScopeState(execution.Before, beforeClient) || !assignmentLineageMatchesScopeState(execution.After, afterClient) {
		return false
	}
	if !sameScopeAssignmentLineage(beforeClient.ScopeAssignments, afterClient.ScopeAssignments) {
		return false
	}
	if !sameLocalToServer(beforeLocal.ScopeAssignments, beforeClient.ScopeAssignments) || !sameLocalToServer(afterLocal.ScopeAssignments, afterClient.ScopeAssignments) {
		return false
	}
	if !reflect.DeepEqual(scopeIDsFromConnectRequest(request), activeScopeIDs(beforeClient)) || len(connect.AddedScopes) != 0 || len(connect.RemovedScopes) != 0 {
		return false
	}
	if connect.Generation != afterClient.CurrentGeneration || connect.ScopeSetVersion != afterClient.ScopeSetVersion || afterLocal.AuthoritativeScopeSetVersion != afterClient.ScopeSetVersion {
		return false
	}
	if connect.Schema.Action != reference.SchemaActionNone || connect.Schema.Source != execution.After.CurrentSchema || connect.Schema.Target != execution.After.CurrentSchema {
		return false
	}
	if !connectCursorObservationsMatch(connect.ScopeCursors, afterClient.ScopeAssignments, reference.CursorDispositionUnchanged) {
		return false
	}
	if len(afterLocal.ScopeAssignments) != len(afterClient.ScopeAssignments) || afterLocal.Lifecycle.State != reference.ClientLifecycleReady || !sameStateExceptClientTransition(execution.Before, execution.After, client) || !singleConnectedEventAppended(execution.Before, execution.After, client) {
		return false
	}
	return true
}

func steadyPullSatisfied(result Result) bool {
	steps, ok := exactSemanticSteps(
		result,
		"rebuild/request-page",
		"local/begin-rebuild",
		"local/apply-rebuild-page",
		"local/finalize-rebuild",
		"model/commit-source-transaction",
		"process/materialize-source-transaction",
		"pull/request-page",
		"local/apply-pull-page",
	)
	if !ok {
		return false
	}
	baseline, ok := steadyPullBaselineSatisfied(steps[0], steps[1], steps[2], steps[3])
	if !ok {
		return false
	}
	if !steadyPullMaterializationSatisfied(steps[4], steps[5], baseline.Scope) {
		return false
	}
	pull := steps[6]
	apply := steps[7]
	if !steadyTerminalPullSatisfied(pull, baseline) {
		return false
	}
	if !steadyTerminalApplySatisfied(apply, pull, baseline) {
		return false
	}
	return true
}

type steadyPullBaseline struct {
	Client     reference.ClientKey
	Scope      reference.ScopeID
	Checkpoint reference.LocalScopeCheckpoint
}

func steadyPullBaselineSatisfied(request, begin, apply, finalize OperationExecution) (steadyPullBaseline, bool) {
	return pullBaselineSatisfied(request, begin, apply, finalize, true)
}

func warmPullBaselineSatisfied(request, begin, apply, finalize OperationExecution) (steadyPullBaseline, bool) {
	return pullBaselineSatisfied(request, begin, apply, finalize, false)
}

func pullBaselineSatisfied(request, begin, apply, finalize OperationExecution, serverAcknowledged bool) (steadyPullBaseline, bool) {
	var rebuild semanticRebuildRequest
	if json.Unmarshal(request.Operation.Payload, &rebuild) != nil || !singlePageRebuildExecutionSatisfied(request, rebuild) || len(request.Result.Rebuild.Records) != 0 {
		return steadyPullBaseline{}, false
	}
	client := reference.ClientKey{UserID: reference.UserID(rebuild.UserID), ClientID: reference.ClientID(rebuild.ClientID)}
	scope := reference.ScopeID(rebuild.ScopeID)
	if !steadyLocalRebuildStepSatisfied(begin, client, reference.LocalMutationStatusPending) || !steadyLocalRebuildStepSatisfied(apply, client, reference.LocalMutationStatusPending) || !steadyLocalRebuildStepSatisfied(finalize, client, reference.LocalMutationStatusAccepted) {
		return steadyPullBaseline{}, false
	}
	for _, execution := range []OperationExecution{begin, apply, finalize} {
		operationClient, operationScope, operationRebuild, ok := localRebuildIdentity(execution.Operation.Payload)
		if !ok || operationClient != client || operationScope != scope || operationRebuild != reference.RebuildID(rebuild.RebuildID) {
			return steadyPullBaseline{}, false
		}
	}
	before, beforeFound := snapshotLocalClient(begin.Before.ClientLocal, client)
	after, afterFound := snapshotLocalClient(finalize.After.ClientLocal, client)
	checkpoint, checkpointFound := localCheckpoint(after, scope)
	assignment, assignmentFound := localAssignment(after, scope)
	beforeCheckpointsValid := len(before.ScopeCheckpoints) == 0
	if !serverAcknowledged && len(before.ScopeCheckpoints) == 1 {
		placeholder := before.ScopeCheckpoints[0]
		beforeCheckpointsValid = placeholder.Scope == scope && !placeholder.HasCursor && placeholder.Cursor == (reference.OpaqueToken{}) && !placeholder.HasChecksum && placeholder.Checksum == (reference.Checksum{}) && !placeholder.Verified
	}
	if !beforeFound || !afterFound || !checkpointFound || !assignmentFound || len(before.Rows) != 0 || len(before.Provenance) != 0 || !beforeCheckpointsValid || len(after.Rows) != 0 || len(after.Provenance) != 0 || len(after.ScopeCheckpoints) != 1 || len(after.RebuildAttempts) != 1 {
		return steadyPullBaseline{}, false
	}
	attempt := after.RebuildAttempts[0]
	if attempt.Rebuild != reference.RebuildID(rebuild.RebuildID) || attempt.Scope != scope || attempt.Phase != reference.LocalRebuildAttemptPhaseCompleted || !assignment.Assigned || assignment.RebuildRequired {
		return steadyPullBaseline{}, false
	}
	observation := request.Result.Rebuild
	if !checkpoint.HasCursor || checkpoint.Cursor == (reference.OpaqueToken{}) || checkpoint.Cursor != observation.FinalCursor || !checkpoint.HasChecksum || !checkpoint.Verified || checkpoint.Checksum != observation.Checksum || checkpoint.Position != expectedRebuildBoundary(request.Before) || checkpoint.Position.Kind != reference.PositionKindGenerationStart {
		return steadyPullBaseline{}, false
	}
	server, serverFound := snapshotClient(finalize.After.Clients, client)
	serverScope, serverScopeFound := serverAssignment(server, scope)
	if !serverFound || !serverScopeFound || !assignmentLineageMatchesScopeState(finalize.After, server) {
		return steadyPullBaseline{}, false
	}
	if serverAcknowledged && !sameLocalToServer(after.ScopeAssignments, server.ScopeAssignments) {
		return steadyPullBaseline{}, false
	}
	if !serverAcknowledged && (!serverScope.RebuildRequired || assignment.RebuildRequired || !sameLocalAssignmentLineage(after.ScopeAssignments, server.ScopeAssignments)) {
		return steadyPullBaseline{}, false
	}
	return steadyPullBaseline{Client: client, Scope: scope, Checkpoint: checkpoint}, true
}

func steadyLocalRebuildStepSatisfied(execution OperationExecution, client reference.ClientKey, status reference.LocalMutationStatus) bool {
	return execution.Err == nil && execution.Result.Kind == reference.StepResultKindLocal && execution.Result.Local != nil && execution.Result.Local.Client == client && execution.Result.Local.Status == status
}

func localRebuildIdentity(payload json.RawMessage) (reference.ClientKey, reference.ScopeID, reference.RebuildID, bool) {
	var request struct {
		UserID    string `json:"user_id"`
		ClientID  string `json:"client_id"`
		ScopeID   string `json:"scope_id"`
		RebuildID string `json:"rebuild_id"`
	}
	if json.Unmarshal(payload, &request) != nil || request.UserID == "" || request.ClientID == "" || request.ScopeID == "" || request.RebuildID == "" {
		return reference.ClientKey{}, "", "", false
	}
	return reference.ClientKey{UserID: reference.UserID(request.UserID), ClientID: reference.ClientID(request.ClientID)}, reference.ScopeID(request.ScopeID), reference.RebuildID(request.RebuildID), true
}

func steadyPullMaterializationSatisfied(commit, materialize OperationExecution, scope reference.ScopeID) bool {
	if commit.Err != nil || commit.Result.Kind != reference.StepResultKindWAL || commit.Result.WAL == nil || materialize.Err != nil || materialize.Result.Kind != reference.StepResultKindWAL || materialize.Result.WAL == nil {
		return false
	}
	var committed semanticMaterializeRequest
	var requested semanticMaterializeRequest
	if json.Unmarshal(commit.Operation.Payload, &committed) != nil || json.Unmarshal(materialize.Operation.Payload, &requested) != nil || committed.StreamGeneration == "" || committed.CommitLSN == "" || committed != requested {
		return false
	}
	lsn, err := strconv.ParseUint(requested.CommitLSN, 10, 64)
	transaction := reference.TransactionReplayKey{StreamGeneration: reference.StreamGeneration(requested.StreamGeneration), CommitLSN: reference.CommitLSN(lsn)}
	if err != nil || commit.Result.WAL.Transaction != transaction || materialize.Result.WAL.Transaction != transaction {
		return false
	}
	if len(commit.After.Stream.Transactions) != len(commit.Before.Stream.Transactions)+1 || len(materialize.After.Rows) != len(materialize.Before.Rows)+1 || len(materialize.After.Stream.Materializations) != len(materialize.Before.Stream.Materializations)+1 || !reflect.DeepEqual(materialize.Before.ClientLocal, materialize.After.ClientLocal) {
		return false
	}
	if len(materialize.After.Rows) != 1 || !independentRowChecksumMatches(materialize.After, materialize.After.Rows[0].Value) {
		return false
	}
	beforeScope, beforeFound := snapshotScope(materialize.Before.Scopes, scope)
	afterScope, afterFound := snapshotScope(materialize.After.Scopes, scope)
	computed, checksumOK := independentScopeChecksum(materialize.After, scope)
	return beforeFound && afterFound && checksumOK && computed == afterScope.Checksum && len(afterScope.Effects) == len(beforeScope.Effects)+1 && len(afterScope.Membership) == len(beforeScope.Membership)+1 && afterScope.Cardinality == beforeScope.Cardinality+1 && afterScope.Checksum != beforeScope.Checksum
}

func steadyTerminalPullSatisfied(execution OperationExecution, baseline steadyPullBaseline) bool {
	if !successfulEndpointResult(execution.Result, reference.StepResultKindPull, false) || execution.Result.Pull == nil || !reflect.DeepEqual(execution.Before.ClientLocal, execution.After.ClientLocal) {
		return false
	}
	var request semanticPullRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil || request.UserID != string(baseline.Client.UserID) || request.ClientID != string(baseline.Client.ClientID) || len(request.Scopes) != 1 || request.Scopes[0].ScopeID != string(baseline.Scope) || request.Scopes[0].CursorSource != "local_checkpoint" || !semanticSchemaMatches(request.Schema, execution.Before.CurrentSchema) {
		return false
	}
	server, serverFound := snapshotClient(execution.Before.Clients, baseline.Client)
	afterServer, afterServerFound := snapshotClient(execution.After.Clients, baseline.Client)
	local, localFound := snapshotLocalClient(execution.Before.ClientLocal, baseline.Client)
	checkpoint, checkpointFound := localCheckpoint(local, baseline.Scope)
	if !serverFound || !afterServerFound || !localFound || !checkpointFound || checkpoint != baseline.Checkpoint || !checkpoint.HasCursor || checkpoint.Cursor == (reference.OpaqueToken{}) || !checkpoint.HasChecksum || !checkpoint.Verified || request.ClientGeneration != uint64(server.CurrentGeneration) || request.ScopeSetVersion != uint64(server.ScopeSetVersion) {
		return false
	}
	if !sameScopeAssignmentLineage(server.ScopeAssignments, afterServer.ScopeAssignments) || !assignmentLineageMatchesScopeState(execution.Before, server) || !assignmentLineageMatchesScopeState(execution.After, afterServer) || !sameLocalToServer(local.ScopeAssignments, server.ScopeAssignments) || !sameStateExceptServerClient(execution.Before, execution.After, baseline.Client) {
		return false
	}
	pull := execution.Result.Pull
	if pull.HasMore || len(pull.Changes) != 1 || len(pull.ScopeCursors) != 1 || len(pull.ScopeChecksums) != 1 || len(pull.AddedScopes) != 0 || len(pull.RemovedScopes) != 0 || len(pull.RebuildScopes) != 0 {
		return false
	}
	change := pull.Changes[0]
	cursor := pull.ScopeCursors[0]
	checksum := pull.ScopeChecksums[0]
	if change.Scope != baseline.Scope || change.Operation != reference.EffectOperationUpsert || !change.HasChecksum || cursor.Scope != baseline.Scope || cursor.Disposition != reference.CursorDispositionIssued || checksum.Scope != baseline.Scope || !checksum.HasChecksum || checksum.Checksum == baseline.Checkpoint.Checksum {
		return false
	}
	scope, found := snapshotScope(execution.Before.Scopes, baseline.Scope)
	authoritative, rowFound := snapshotRowByIdentity(execution.Before.Rows, change.Row)
	serverCheckpoint, serverCheckpointFound := clientCheckpoint(afterServer, baseline.Scope)
	computedScope, scopeChecksumOK := independentScopeChecksum(execution.Before, baseline.Scope)
	return found && rowFound && serverCheckpointFound && serverCheckpoint.HasCursor && serverCheckpoint.Cursor != (reference.OpaqueToken{}) && serverCheckpoint.Cursor != baseline.Checkpoint.Cursor && serverCheckpoint.Position == baseline.Checkpoint.Position && checksum.Checksum == scope.Checksum && scopeChecksumOK && computedScope == scope.Checksum && authoritative.Version == change.Version && authoritative.Checksum == change.Checksum && independentRowChecksumMatches(execution.Before, authoritative)
}

func steadyTerminalApplySatisfied(apply, pull OperationExecution, baseline steadyPullBaseline) bool {
	if apply.Err != nil || apply.Result.Kind != reference.StepResultKindLocal || apply.Result.Local == nil || apply.Result.Local.Client != baseline.Client || apply.Result.Local.Status != reference.LocalMutationStatusAccepted || !reflect.DeepEqual(apply.Before, pull.After) || !sameStateExceptLocalTarget(apply.Before, apply.After, baseline.Client) {
		return false
	}
	var request struct {
		UserID       string `json:"user_id"`
		ClientID     string `json:"client_id"`
		SourceStepID string `json:"source_step_id"`
	}
	if json.Unmarshal(apply.Operation.Payload, &request) != nil || request.UserID != string(baseline.Client.UserID) || request.ClientID != string(baseline.Client.ClientID) || request.SourceStepID != string(pull.StepID) {
		return false
	}
	before, beforeFound := snapshotLocalClient(apply.Before.ClientLocal, baseline.Client)
	after, afterFound := snapshotLocalClient(apply.After.ClientLocal, baseline.Client)
	checkpoint, checkpointFound := localCheckpoint(after, baseline.Scope)
	server, serverFound := snapshotClient(pull.After.Clients, baseline.Client)
	serverCheckpoint, serverCheckpointFound := clientCheckpoint(server, baseline.Scope)
	if !beforeFound || !afterFound || !checkpointFound || !serverFound || !serverCheckpointFound || len(before.Rows) != 0 || len(before.Provenance) != 0 || len(after.Rows) != 1 || len(after.Provenance) != 1 || len(after.ScopeCheckpoints) != 1 || !reflect.DeepEqual(before.ScopeAssignments, after.ScopeAssignments) || !sameLocalToServer(after.ScopeAssignments, server.ScopeAssignments) || !assignmentLineageMatchesScopeState(apply.After, server) {
		return false
	}
	terminalChecksum := checksumForScope(pull.Result.Pull.ScopeChecksums, baseline.Scope)
	expectedPosition := pull.Before.Stream.Authority.GlobalMaterializationBoundary
	computedScope, scopeChecksumOK := independentScopeChecksum(apply.After, baseline.Scope)
	if !checkpoint.HasCursor || checkpoint.Cursor == (reference.OpaqueToken{}) || !serverCheckpoint.HasCursor || serverCheckpoint.Cursor == (reference.OpaqueToken{}) || !checkpoint.HasChecksum || !checkpoint.Verified || checkpoint.Checksum != terminalChecksum || checkpoint.Checksum == baseline.Checkpoint.Checksum || !scopeChecksumOK || checkpoint.Checksum != computedScope || checkpoint.Cursor != serverCheckpoint.Cursor || checkpoint.Cursor == baseline.Checkpoint.Cursor || checkpoint.Position != expectedPosition || serverCheckpoint.Position != baseline.Checkpoint.Position {
		return false
	}
	change := pull.Result.Pull.Changes[0]
	row := after.Rows[0]
	provenance := after.Provenance[0]
	authoritative, authoritativeFound := snapshotRowByIdentity(apply.After.Rows, row.Identity)
	return authoritativeFound && independentRowChecksumMatches(apply.After, authoritative) && row.Identity == change.Row && row.HasServerVersion && row.ServerVersion == change.Version && row.HasChecksum && row.Checksum != (reference.Checksum{}) && row.Checksum == change.Checksum && row.Checksum == authoritative.Checksum && !row.Deleted && reflect.DeepEqual(row.Fields, authoritative.FieldValues) && provenance.Row == change.Row && reflect.DeepEqual(provenance.Scopes, []reference.ScopeID{baseline.Scope}) && provenance.Version == change.Version
}

func continuousSemanticSteps(steps []OperationExecution) bool {
	for index := 1; index < len(steps); index++ {
		if !reflect.DeepEqual(steps[index-1].After, steps[index].Before) {
			return false
		}
	}
	return true
}

func clientCheckpoint(client reference.ClientState, scope reference.ScopeID) (reference.ClientCheckpoint, bool) {
	for _, checkpoint := range client.Checkpoints {
		if checkpoint.Scope == scope {
			return checkpoint, true
		}
	}
	return reference.ClientCheckpoint{}, false
}

func pendingCycleSatisfied(result Result) bool {
	steps, ok := exactSemanticSteps(result, "local/write", "push/submit", "process/materialize-source-transaction", "pull/request-page")
	if !ok {
		return false
	}
	localWrite, push, materialize, pull := steps[0], steps[1], steps[2], steps[3]
	client, mutation, ok := pendingTraceIdentity(localWrite, push)
	if !ok || !pendingLocalWriteSatisfied(localWrite, client, mutation) || !pendingPushSatisfied(push, client, mutation) || !pendingMaterializationSatisfied(materialize, push, client, mutation) {
		return false
	}
	return terminalPullExecutionSatisfied(pull) && reflect.DeepEqual(materialize.After, pull.Before) && reflect.DeepEqual(pull.After, result.FinalSnapshot)
}

func rebuildRequestsSatisfied(result Result) bool {
	steps, ok := exactSemanticSteps(
		result,
		"model/commit-source-transaction",
		"process/materialize-source-transaction",
		"model/set-client-assignments",
		"connect/send",
		"local/begin-rebuild",
		"rebuild/request-page",
		"model/commit-source-transaction",
		"process/materialize-source-transaction",
		"local/apply-rebuild-page",
		"rebuild/request-page",
		"local/apply-rebuild-page",
		"local/finalize-rebuild",
		"pull/request-page",
	)
	if !ok || exactOperationCount(result, "connect/send") != 1 || exactOperationCount(result, "pull/request-page") != 1 || exactOperationCount(result, "rebuild/request-page") != 2 || exactHTTPCount(result) != 4 {
		return false
	}
	if !sourceMaterializationSatisfied(steps[0], steps[1], "scope-a", 2) || !freshRebuildAssignmentSatisfied(steps[2]) || !freshRebuildConnectSatisfied(steps[3]) {
		return false
	}
	trace, ok := firstRebuildPageSatisfied(steps[5])
	if !ok || !concurrentRebuildChangeSatisfied(steps[6], steps[7], trace) || !finalRebuildPageSatisfied(steps[9], trace) || !rebuildRequestsLocalFlowSatisfied(steps[4], steps[8], steps[10], steps[11], trace) || !postRebuildPullSatisfied(steps[12], trace) {
		return false
	}
	for _, step := range result.Steps {
		if step.Result.Schema != nil || step.OperationKey == "schema/fetch" {
			return false
		}
	}
	return true
}

func freshRebuildAssignmentSatisfied(execution OperationExecution) bool {
	if execution.Err != nil || execution.Result.Kind != reference.StepResultKindClient || execution.Result.Client == nil || !reflect.DeepEqual(execution.Before.Rows, execution.After.Rows) || !reflect.DeepEqual(execution.Before.Scopes, execution.After.Scopes) {
		return false
	}
	client := reference.ClientKey{UserID: "user-a", ClientID: "client-a"}
	_, beforeFound := snapshotClient(execution.Before.Clients, client)
	after, afterFound := snapshotClient(execution.After.Clients, client)
	assignment, assignmentFound := serverAssignment(after, "scope-a")
	_, localFound := snapshotLocalClient(execution.After.ClientLocal, client)
	return !beforeFound && afterFound && !localFound && after.CurrentGeneration == 0 && after.ScopeSetVersion == 1 && len(after.ScopeAssignments) == 1 && assignmentFound && assignment.Assigned && assignment.RebuildRequired
}

func freshRebuildConnectSatisfied(execution OperationExecution) bool {
	if !connectExecutionSatisfied(execution) {
		return false
	}
	var authored struct {
		UserID           string                   `json:"user_id"`
		ClientID         string                   `json:"client_id"`
		ClientGeneration json.RawMessage          `json:"client_generation"`
		Schema           *semanticSchemaReference `json:"schema"`
		ScopeSetVersion  *uint64                  `json:"scope_set_version"`
		KnownScopes      *[]semanticConnectScope  `json:"known_scopes"`
	}
	if json.Unmarshal(execution.Operation.Payload, &authored) != nil || string(authored.ClientGeneration) != "null" || authored.Schema == nil || *authored.Schema != (semanticSchemaReference{}) || authored.ScopeSetVersion == nil || *authored.ScopeSetVersion != 0 || authored.KnownScopes == nil || len(*authored.KnownScopes) != 0 {
		return false
	}
	client := reference.ClientKey{UserID: reference.UserID(authored.UserID), ClientID: reference.ClientID(authored.ClientID)}
	before, beforeFound := snapshotClient(execution.Before.Clients, client)
	after, afterFound := snapshotClient(execution.After.Clients, client)
	_, beforeLocalFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	afterLocal, afterLocalFound := snapshotLocalClient(execution.After.ClientLocal, client)
	if !beforeFound || !afterFound || beforeLocalFound || !afterLocalFound || before.CurrentGeneration != 0 || after.CurrentGeneration != 1 || before.ScopeSetVersion != 1 || after.ScopeSetVersion != 1 || !sameScopeAssignmentLineage(before.ScopeAssignments, after.ScopeAssignments) || !sameLocalToServer(afterLocal.ScopeAssignments, after.ScopeAssignments) || !assignmentLineageMatchesScopeState(execution.After, after) {
		return false
	}
	connect := execution.Result.Connect
	return connect.Client == client && connect.Generation == 1 && connect.ScopeSetVersion == 1 && reflect.DeepEqual(connect.AddedScopes, []reference.ScopeID{"scope-a"}) && len(connect.RemovedScopes) == 0 && connect.Schema.Source == (reference.SchemaRef{}) && connect.Schema.Target == execution.After.CurrentSchema && connect.Schema.Action == reference.SchemaActionReplace && connectCursorObservationsMatch(connect.ScopeCursors, after.ScopeAssignments, reference.CursorDispositionRebuildRequired) && afterLocal.CurrentSchema == execution.After.CurrentSchema && afterLocal.AuthoritativeScopeSetVersion == after.ScopeSetVersion && afterLocal.Lifecycle.State == reference.ClientLifecycleReady && singleConnectedEventAppended(execution.Before, execution.After, client)
}

func rebuildRequestsLocalFlowSatisfied(begin, firstApply, finalApply, finalize OperationExecution, trace rebuildPageTrace) bool {
	for _, execution := range []OperationExecution{begin, firstApply, finalApply, finalize} {
		client, scope, rebuild, ok := localRebuildIdentity(execution.Operation.Payload)
		if !ok || client != trace.Key.Client || scope != trace.Key.Scope || rebuild != trace.Key.Rebuild {
			return false
		}
	}
	if !steadyLocalRebuildStepSatisfied(begin, trace.Key.Client, reference.LocalMutationStatusPending) || !steadyLocalRebuildStepSatisfied(firstApply, trace.Key.Client, reference.LocalMutationStatusPending) || !steadyLocalRebuildStepSatisfied(finalApply, trace.Key.Client, reference.LocalMutationStatusPending) || !steadyLocalRebuildStepSatisfied(finalize, trace.Key.Client, reference.LocalMutationStatusAccepted) {
		return false
	}
	local, found := snapshotLocalClient(finalize.After.ClientLocal, trace.Key.Client)
	checkpoint, checkpointFound := localCheckpoint(local, trace.Key.Scope)
	assignment, assignmentFound := localAssignment(local, trace.Key.Scope)
	if !found || !checkpointFound || !assignmentFound || len(local.Rows) != 2 || len(local.Provenance) != 2 || len(local.ScopeCheckpoints) != 1 || len(local.RebuildAttempts) != 1 || assignment.RebuildRequired {
		return false
	}
	attempt := local.RebuildAttempts[0]
	return attempt.Rebuild == trace.Key.Rebuild && attempt.Phase == reference.LocalRebuildAttemptPhaseCompleted && checkpoint.HasCursor && checkpoint.HasChecksum && checkpoint.Verified && checkpoint.Checksum == trace.Session.Checksum
}

func postRebuildPullSatisfied(execution OperationExecution, trace rebuildPageTrace) bool {
	if execution.Err != nil || !successfulEndpointResult(execution.Result, reference.StepResultKindPull, false) || execution.Result.Pull == nil {
		return false
	}
	var request semanticPullRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil || request.UserID != string(trace.Key.Client.UserID) || request.ClientID != string(trace.Key.Client.ClientID) || request.Limit != 1 || len(request.Scopes) != 1 || request.Scopes[0].ScopeID != string(trace.Key.Scope) || request.Scopes[0].CursorSource != "local_checkpoint" {
		return false
	}
	pull := execution.Result.Pull
	if pull.HasMore || len(pull.Changes) != 1 || len(pull.AddedScopes) != 0 || len(pull.RemovedScopes) != 0 || len(pull.RebuildScopes) != 0 {
		return false
	}
	added := rowsAddedBetween(trace.Snapshot.Rows, execution.Before.Rows)
	if len(added) != 1 || rebuildSessionContainsRow(trace.Session, added[0].Identity) {
		return false
	}
	change := pull.Changes[0]
	if change.Scope != trace.Key.Scope || change.Row != added[0].Identity || change.Version != added[0].Version || !change.HasChecksum || change.Checksum != added[0].Checksum {
		return false
	}
	server, found := snapshotClient(execution.Before.Clients, trace.Key.Client)
	afterServer, afterFound := snapshotClient(execution.After.Clients, trace.Key.Client)
	beforeAssignment, beforeAssignmentFound := serverAssignment(server, trace.Key.Scope)
	afterAssignment, afterAssignmentFound := serverAssignment(afterServer, trace.Key.Scope)
	afterCheckpoint, checkpointFound := clientCheckpoint(afterServer, trace.Key.Scope)
	if !found || !afterFound || !beforeAssignmentFound || !afterAssignmentFound || !checkpointFound || afterCheckpoint.Position != trace.Session.SnapshotBoundary || afterAssignment.RebuildRequired || beforeAssignment.Scope != afterAssignment.Scope || beforeAssignment.MembershipGeneration != afterAssignment.MembershipGeneration || beforeAssignment.RetentionGeneration != afterAssignment.RetentionGeneration || beforeAssignment.Assigned != afterAssignment.Assigned {
		return false
	}
	return scopeChecksumsMatch(pull.ScopeChecksums, execution.Before, server)
}

func exactSemanticSteps(result Result, keys ...string) ([]OperationExecution, bool) {
	if !semanticSetupSatisfied(result) || len(result.Steps) != len(keys) {
		return nil, false
	}
	for index, key := range keys {
		if result.Steps[index].OperationKey != key {
			return nil, false
		}
	}
	if !continuousSemanticSteps(result.Steps) || len(result.Steps) > 0 && !reflect.DeepEqual(result.Steps[len(result.Steps)-1].After, result.FinalSnapshot) {
		return nil, false
	}
	return result.Steps, true
}

func semanticSetupSatisfied(result Result) bool {
	if len(result.Setup) != 1 || result.Setup[0].Err != nil || result.Setup[0].Result.Kind != reference.StepResultKindContractInstalled {
		return false
	}
	after := result.Setup[0].After
	return after.ProtocolVersion == 3 && after.CurrentSchema != (reference.SchemaRef{}) && after.Installation.Installed && after.Installation.ProtocolVersion == 3
}

func connectExecutionSatisfied(execution OperationExecution) bool {
	return execution.Err == nil && successfulEndpointResult(execution.Result, reference.StepResultKindConnect, false) && execution.Result.Connect != nil
}

func terminalPullExecutionSatisfied(execution OperationExecution) bool {
	if execution.Err != nil || !successfulEndpointResult(execution.Result, reference.StepResultKindPull, false) || execution.Result.Pull == nil {
		return false
	}
	var request semanticPullRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil || request.UserID == "" || request.ClientID == "" || len(request.Scopes) == 0 {
		return false
	}
	client := reference.ClientKey{UserID: reference.UserID(request.UserID), ClientID: reference.ClientID(request.ClientID)}
	server, found := snapshotClient(execution.Before.Clients, client)
	local, localFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	if !found || !localFound || request.ClientGeneration != uint64(server.CurrentGeneration) || !semanticSchemaMatches(request.Schema, execution.Before.CurrentSchema) || request.ScopeSetVersion != uint64(server.ScopeSetVersion) || !reflect.DeepEqual(scopeIDsFromPullRequest(request), activeScopeIDs(server)) || !sameLocalToServer(local.ScopeAssignments, server.ScopeAssignments) || !assignmentLineageMatchesScopeState(execution.Before, server) || !activeScopeChecksumsAreIndependent(execution.Before, server) {
		return false
	}
	pull := execution.Result.Pull
	if pull.HasMore || len(pull.Changes) != 0 || len(pull.AddedScopes) != 0 || len(pull.RemovedScopes) != 0 || !reflect.DeepEqual(pull.RebuildScopes, activeScopeIDs(server)) {
		return false
	}
	if !connectCursorObservationsMatch(pull.ScopeCursors, server.ScopeAssignments, reference.CursorDispositionRebuildRequired) || !scopeChecksumsMatch(pull.ScopeChecksums, execution.Before, server) {
		return false
	}
	return reflect.DeepEqual(execution.Before, execution.After)
}

func successfulEndpointResult(result reference.StepResult, kind reference.StepResultKind, expectsBody bool) bool {
	if result.Kind != kind || result.HTTP == nil {
		return false
	}
	http := result.HTTP
	if http.Status != 200 || http.HasCode || http.Code != "" || http.Retryable || http.HasRetryAfterMilliseconds || http.RetryAfterMilliseconds != 0 {
		return false
	}
	if expectsBody {
		return len(http.Body) != 0
	}
	return len(http.Body) == 0
}

func localApplyExecutionSatisfied(apply, pull OperationExecution) bool {
	if apply.Err != nil || apply.Result.Kind != reference.StepResultKindLocal || apply.Result.Local == nil || apply.Result.Local.Status != reference.LocalMutationStatusAccepted {
		return false
	}
	var request struct {
		UserID       string `json:"user_id"`
		ClientID     string `json:"client_id"`
		SourceStepID string `json:"source_step_id"`
	}
	if json.Unmarshal(apply.Operation.Payload, &request) != nil || request.SourceStepID != string(pull.StepID) || request.UserID == "" || request.ClientID == "" {
		return false
	}
	client := reference.ClientKey{UserID: reference.UserID(request.UserID), ClientID: reference.ClientID(request.ClientID)}
	if apply.Result.Local.Client != client || !reflect.DeepEqual(apply.Before, pull.After) || !sameStateExceptLocalTarget(apply.Before, apply.After, client) {
		return false
	}
	before, beforeFound := snapshotLocalClient(apply.Before.ClientLocal, client)
	after, afterFound := snapshotLocalClient(apply.After.ClientLocal, client)
	if !beforeFound || !afterFound || len(before.ScopeCheckpoints) != 0 || len(after.ScopeCheckpoints) != 1 || !reflect.DeepEqual(before.Rows, after.Rows) || !reflect.DeepEqual(before.Provenance, after.Provenance) || !reflect.DeepEqual(before.DurableQueue, after.DurableQueue) || !reflect.DeepEqual(before.Outcomes, after.Outcomes) {
		return false
	}
	checkpoint := after.ScopeCheckpoints[0]
	if checkpoint.Scope != requestScopeID(pull.Operation.Payload) || checkpoint.HasCursor || checkpoint.Cursor != (reference.OpaqueToken{}) || !checkpoint.HasChecksum || !checkpoint.Verified {
		return false
	}
	return checksumForScope(pull.Result.Pull.ScopeChecksums, checkpoint.Scope) == checkpoint.Checksum
}

func pendingTraceIdentity(localWrite, push OperationExecution) (reference.ClientKey, reference.MutationID, bool) {
	var local semanticLocalWriteRequest
	var envelope semanticPushEnvelope
	if json.Unmarshal(localWrite.Operation.Payload, &local) != nil || json.Unmarshal(push.Operation.Payload, &envelope) != nil || local.AuthenticatedUserID == "" || local.ClientID == "" || local.MutationID == "" || envelope.AuthenticatedUserID != local.AuthenticatedUserID || envelope.Request.ClientID != local.ClientID || len(envelope.Request.Mutations) != 1 || envelope.Request.Mutations[0].MutationID != local.MutationID || envelope.Request.Mutations[0].Table != local.TableID || envelope.Request.Mutations[0].Operation != local.Operation {
		return reference.ClientKey{}, "", false
	}
	return reference.ClientKey{UserID: reference.UserID(local.AuthenticatedUserID), ClientID: reference.ClientID(local.ClientID)}, reference.MutationID(local.MutationID), true
}

func pendingLocalWriteSatisfied(execution OperationExecution, client reference.ClientKey, mutation reference.MutationID) bool {
	if execution.Err != nil || execution.Result.Kind != reference.StepResultKindLocal || execution.Result.Local == nil || execution.Result.Local.Client != client || execution.Result.Local.Mutation != mutation || execution.Result.Local.Status != reference.LocalMutationStatusPending {
		return false
	}
	before, beforeFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	after, afterFound := snapshotLocalClient(execution.After.ClientLocal, client)
	if !beforeFound || !afterFound || !sameStateExceptLocalTarget(execution.Before, execution.After, client) || len(before.Rows) != 0 || len(before.DurableQueue) != 0 || len(before.Outcomes) != 0 || len(after.Rows) != 1 || len(after.DurableQueue) != 1 || len(after.Outcomes) != 0 {
		return false
	}
	queued := after.DurableQueue[0]
	row := after.Rows[0]
	return queued.Mutation == mutation && queued.Status == reference.LocalMutationStatusPending && !queued.HasBaseVersion && queued.Row == row.Identity && !row.HasServerVersion && !row.HasChecksum && !row.Deleted
}

func pendingPushSatisfied(execution OperationExecution, client reference.ClientKey, mutation reference.MutationID) bool {
	if execution.Err != nil || !successfulEndpointResult(execution.Result, reference.StepResultKindPush, true) || execution.Result.Push == nil || execution.Result.Push.Batch.Client != client || execution.Result.Push.Replay != reference.ReplayDispositionExecuted || len(execution.Result.Push.Mutations) != 1 || execution.Result.Push.Mutations[0].Mutation != mutation {
		return false
	}
	observation := execution.Result.Push.Mutations[0]
	if observation.State != reference.MutationOutcomeApplied {
		return false
	}
	var envelope semanticPushEnvelope
	if json.Unmarshal(execution.Operation.Payload, &envelope) != nil || envelope.Request.ClientGeneration == 0 || envelope.Request.BatchID == "" || envelope.CommitLSN == "" || envelope.EndLSN == "" || !semanticSchemaMatches(envelope.Request.Schema, execution.Before.CurrentSchema) {
		return false
	}
	beforeServer, beforeServerFound := snapshotClient(execution.Before.Clients, client)
	if !beforeServerFound || envelope.Request.ClientGeneration != uint64(beforeServer.CurrentGeneration) {
		return false
	}
	key := reference.MutationKey{Client: client, Mutation: mutation}
	ledger, found := snapshotMutation(execution.After.Mutations, key)
	if !found || ledger.Outcome.Mutation != mutation || ledger.Outcome.State != observation.State || ledger.Outcome.Reason != observation.Reason {
		return false
	}
	local, localFound := snapshotLocalClient(execution.After.ClientLocal, client)
	beforeLocal, beforeFound := snapshotLocalClient(execution.Before.ClientLocal, client)
	if !localFound || !beforeFound || len(local.DurableQueue) != 1 || len(local.Outcomes) != 1 || len(beforeLocal.DurableQueue) != 1 || len(beforeLocal.Outcomes) != 0 || !reflect.DeepEqual(local.Outcomes[0], ledger.Outcome) {
		return false
	}
	queued := local.DurableQueue[0]
	row, rowFound := localRowForIdentity(local.Rows, queued.Row)
	if !rowFound || queued.Mutation != mutation || queued.Status != reference.LocalMutationStatusAccepted {
		return false
	}
	if !row.HasServerVersion || row.ServerVersion == "" || !row.HasChecksum || row.Checksum == (reference.Checksum{}) || row.Deleted {
		return false
	}
	if !pushOutcomeBaseMatches(ledger.Outcome.Response, row) {
		return false
	}
	batch, batchFound := snapshotBatch(execution.After.Batches, execution.Result.Push.Batch)
	if !batchFound || batch.HTTPStatus != 200 || batch.Execution != reference.BatchExecutionCompleted || len(batch.Outcomes) != 1 || !bytes.Equal(batch.SealedCanonicalResponse, execution.Result.HTTP.Body) || !bytes.Equal(batch.Outcomes[0].Response, ledger.Outcome.Response) {
		return false
	}
	var response semanticPushResponse
	if json.Unmarshal(execution.Result.HTTP.Body, &response) != nil || response.BatchID != envelope.Request.BatchID || len(response.Accepted) != 1 || len(response.Rejected) != 0 || !bytes.Equal(response.Accepted[0], ledger.Outcome.Response) {
		return false
	}
	source, sourceFound := sourceRowByIdentity(execution.After.Stream.SourceRows, row.Identity)
	return sourceFound && authoritativeMatchesLocal(source.Row, row) && independentRowChecksumMatches(execution.After, source.Row)
}

func pendingMaterializationSatisfied(execution, push OperationExecution, client reference.ClientKey, mutation reference.MutationID) bool {
	if execution.Err != nil || execution.Result.Kind != reference.StepResultKindWAL || execution.Result.WAL == nil || !reflect.DeepEqual(execution.Before.ClientLocal, execution.After.ClientLocal) || !reflect.DeepEqual(execution.Before.Clients, execution.After.Clients) || !reflect.DeepEqual(execution.Before.Batches, execution.After.Batches) || !reflect.DeepEqual(execution.Before.Mutations, execution.After.Mutations) {
		return false
	}
	var request semanticMaterializeRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil {
		return false
	}
	var envelope semanticPushEnvelope
	if json.Unmarshal(push.Operation.Payload, &envelope) != nil {
		return false
	}
	commit, err := strconv.ParseUint(request.CommitLSN, 10, 64)
	pushCommit, pushCommitErr := strconv.ParseUint(envelope.CommitLSN, 10, 64)
	pushEnd, pushEndErr := strconv.ParseUint(envelope.EndLSN, 10, 64)
	transactionKey := reference.TransactionReplayKey{StreamGeneration: reference.StreamGeneration(request.StreamGeneration), CommitLSN: reference.CommitLSN(commit)}
	if err != nil || pushCommitErr != nil || pushEndErr != nil || commit == 0 || commit != pushCommit || request.StreamGeneration != string(push.Before.Stream.Authority.ActiveGeneration) || execution.Result.WAL.Transaction != transactionKey {
		return false
	}
	if len(execution.After.Rows) != len(execution.Before.Rows)+1 || len(execution.After.Stream.Materializations) != len(execution.Before.Stream.Materializations)+1 {
		return false
	}
	key := reference.MutationKey{Client: client, Mutation: mutation}
	ledger, found := snapshotMutation(execution.After.Mutations, key)
	if !found || ledger.Outcome.State != reference.MutationOutcomeApplied {
		return false
	}
	local, found := snapshotLocalClient(execution.After.ClientLocal, client)
	if !found {
		return false
	}
	localRow, found := localRowForMutation(local, mutation)
	if !found {
		return false
	}
	authoritative, found := snapshotRowByIdentity(execution.After.Rows, localRow.Identity)
	source, sourceFound := sourceRowByIdentity(execution.After.Stream.SourceRows, localRow.Identity)
	transaction, transactionFound := snapshotTransaction(execution.After.Stream.Transactions, transactionKey)
	if !found || !sourceFound || !transactionFound || transaction.EndLSN != reference.EndLSN(pushEnd) || transaction.Lifecycle != reference.TransactionLifecycleMaterialized || len(transaction.Events) != 1 || transaction.Events[0].ReplayKey.Transaction != transactionKey || !transaction.Events[0].HasAfter || !sourceImageMatchesAuthoritative(transaction.Events[0].After, authoritative) || !authoritativeRowsEquivalent(source.Row, authoritative) || !authoritativeMatchesLocal(authoritative, localRow) || !independentRowChecksumMatches(execution.After, authoritative) {
		return false
	}
	return len(envelope.Request.Mutations) == 1 && envelope.Request.Mutations[0].MutationID == string(mutation)
}

func singlePageRebuildExecutionSatisfied(execution OperationExecution, request semanticRebuildRequest) bool {
	if execution.Err != nil || !successfulEndpointResult(execution.Result, reference.StepResultKindRebuild, false) || execution.Result.Rebuild == nil || request.CursorSource != "none" {
		return false
	}
	client, _, assignment, scope, ok := rebuildRequestLineage(execution.Before, request)
	if !ok {
		return false
	}
	key := reference.RebuildKey{Client: client, Scope: assignment.Scope, Rebuild: reference.RebuildID(request.RebuildID)}
	if _, found := snapshotRebuild(execution.Before.Rebuilds, key); found || !sameStateExceptRebuilds(execution.Before, execution.After) {
		return false
	}
	observation := execution.Result.Rebuild
	if observation.Attempt != key || observation.PageOrdinal != 1 || observation.Replayed || observation.Restarted || observation.HasContinuation || !observation.HasFinalCursor || observation.FinalCursor == (reference.OpaqueToken{}) || !observation.HasChecksum {
		return false
	}
	session, found := snapshotRebuild(execution.After.Rebuilds, key)
	if !found || !rebuildSessionLineageMatches(session, request, assignment, scope, execution.Before) || session.Status != reference.RebuildStatusComplete || session.SessionID == "" || len(session.Pages) != 1 || len(session.StagedRows) != len(observation.Records) || session.NextRowOrdinal != uint64(len(session.StagedRows)+1) || session.HasContinuation || session.Continuation != (reference.OpaqueToken{}) || !session.HasFinalCursor || session.FinalCursor != observation.FinalCursor || session.Checksum != observation.Checksum {
		return false
	}
	page := session.Pages[0]
	if !rebuildObservationMatchesPage(*observation, page) || page.HasToken || page.Token != (reference.OpaqueToken{}) || page.FinalCursor != session.FinalCursor || len(page.Rows) != len(session.StagedRows) || len(page.CanonicalResponse) == 0 {
		return false
	}
	computed, checksumOK := independentScopeChecksum(execution.Before, key.Scope)
	return checksumOK && computed == session.Checksum && session.Checksum == scope.Checksum && rebuildSessionRowsMatchSnapshot(execution.Before, session) && rebuildPagesCoverStaged(session)
}

type rebuildPageTrace struct {
	Key         reference.RebuildKey
	Request     semanticRebuildRequest
	Snapshot    reference.StateSnapshot
	Session     reference.RebuildSession
	Observation reference.RebuildObservation
}

func firstRebuildPageSatisfied(execution OperationExecution) (rebuildPageTrace, bool) {
	var request semanticRebuildRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil || execution.Err != nil || !successfulEndpointResult(execution.Result, reference.StepResultKindRebuild, false) || execution.Result.Rebuild == nil || request.CursorSource != "none" || request.Limit != 1 {
		return rebuildPageTrace{}, false
	}
	client, _, assignment, scope, ok := rebuildRequestLineage(execution.Before, request)
	if !ok {
		return rebuildPageTrace{}, false
	}
	key := reference.RebuildKey{Client: client, Scope: assignment.Scope, Rebuild: reference.RebuildID(request.RebuildID)}
	if _, found := snapshotRebuild(execution.Before.Rebuilds, key); found || !sameStateExceptRebuilds(execution.Before, execution.After) {
		return rebuildPageTrace{}, false
	}
	observation := execution.Result.Rebuild
	if observation.Attempt != key || observation.PageOrdinal != 1 || observation.Replayed || observation.Restarted || len(observation.Records) != 1 || !observation.HasContinuation || observation.Continuation == (reference.OpaqueToken{}) || observation.HasFinalCursor || observation.FinalCursor != (reference.OpaqueToken{}) || observation.HasChecksum || observation.Checksum != (reference.Checksum{}) {
		return rebuildPageTrace{}, false
	}
	session, found := snapshotRebuild(execution.After.Rebuilds, key)
	if !found || !rebuildSessionLineageMatches(session, request, assignment, scope, execution.Before) || session.Status != reference.RebuildStatusStaged || session.SessionID == "" || len(session.StagedRows) < 2 || len(session.Pages) != 1 || !session.HasContinuation || session.Continuation != observation.Continuation || session.NextRowOrdinal != 2 || session.HasFinalCursor || session.FinalCursor != (reference.OpaqueToken{}) {
		return rebuildPageTrace{}, false
	}
	page := session.Pages[0]
	computed, checksumOK := independentScopeChecksum(execution.Before, key.Scope)
	if !checksumOK || computed != session.Checksum || session.Checksum != scope.Checksum || !rebuildObservationMatchesPage(*observation, page) || page.HasToken || page.Token != (reference.OpaqueToken{}) || page.Continuation != session.Continuation || len(page.CanonicalResponse) == 0 || !rebuildSessionRowsMatchSnapshot(execution.Before, session) {
		return rebuildPageTrace{}, false
	}
	return rebuildPageTrace{Key: key, Request: request, Snapshot: execution.Before, Session: session, Observation: *observation}, true
}

func sourceMaterializationSatisfied(commit, materialize OperationExecution, scopeID reference.ScopeID, newRows int) bool {
	if newRows <= 0 || commit.Err != nil || commit.Result.Kind != reference.StepResultKindWAL || commit.Result.WAL == nil || commit.Result.HTTP != nil || materialize.Err != nil || materialize.Result.Kind != reference.StepResultKindWAL || materialize.Result.WAL == nil || materialize.Result.HTTP != nil {
		return false
	}
	var authored struct {
		StreamGeneration string            `json:"stream_generation"`
		CommitLSN        string            `json:"commit_lsn"`
		EndLSN           string            `json:"end_lsn"`
		Events           []json.RawMessage `json:"events"`
	}
	var request semanticMaterializeRequest
	if json.Unmarshal(commit.Operation.Payload, &authored) != nil || json.Unmarshal(materialize.Operation.Payload, &request) != nil || authored.StreamGeneration == "" || authored.CommitLSN == "" || authored.StreamGeneration != request.StreamGeneration || authored.CommitLSN != request.CommitLSN || len(authored.Events) != newRows {
		return false
	}
	commitLSN, commitErr := strconv.ParseUint(authored.CommitLSN, 10, 64)
	endLSN, endErr := strconv.ParseUint(authored.EndLSN, 10, 64)
	key := reference.TransactionReplayKey{StreamGeneration: reference.StreamGeneration(authored.StreamGeneration), CommitLSN: reference.CommitLSN(commitLSN)}
	if commitErr != nil || endErr != nil || commitLSN == 0 || endLSN < commitLSN || commit.Result.WAL.Transaction != key || materialize.Result.WAL.Transaction != key || len(commit.After.Stream.Transactions) != len(commit.Before.Stream.Transactions)+1 || !reflect.DeepEqual(commit.Before.Rows, commit.After.Rows) || len(materialize.After.Rows) != len(materialize.Before.Rows)+newRows || len(materialize.After.Stream.Materializations) != len(materialize.Before.Stream.Materializations)+newRows || !reflect.DeepEqual(materialize.Before.ClientLocal, materialize.After.ClientLocal) || !reflect.DeepEqual(materialize.Before.Rebuilds, materialize.After.Rebuilds) {
		return false
	}
	transaction, found := snapshotTransaction(materialize.After.Stream.Transactions, key)
	if !found || transaction.EndLSN != reference.EndLSN(endLSN) || transaction.Lifecycle != reference.TransactionLifecycleMaterialized || len(transaction.Events) != newRows || materialize.After.Stream.Authority.GlobalMaterializationBoundary != (reference.StreamPosition{StreamGeneration: key.StreamGeneration, Kind: reference.PositionKindTransactionEnd, CommitLSN: key.CommitLSN}) {
		return false
	}
	beforeScope, beforeFound := snapshotScope(materialize.Before.Scopes, scopeID)
	afterScope, afterFound := snapshotScope(materialize.After.Scopes, scopeID)
	computed, checksumOK := independentScopeChecksum(materialize.After, scopeID)
	if !beforeFound || !afterFound || afterScope.Cardinality < beforeScope.Cardinality || !checksumOK || computed != afterScope.Checksum || int(afterScope.Cardinality-beforeScope.Cardinality) != newRows {
		return false
	}
	for _, row := range materialize.After.Rows {
		if !independentRowChecksumMatches(materialize.After, row.Value) || !canonicalRowIdentityMatches(materialize.After, row.Value) {
			return false
		}
	}
	return true
}

func concurrentRebuildChangeSatisfied(commit, materialize OperationExecution, trace rebuildPageTrace) bool {
	beforeSession, beforeFound := snapshotRebuild(commit.Before.Rebuilds, trace.Key)
	afterCommitSession, afterCommitFound := snapshotRebuild(commit.After.Rebuilds, trace.Key)
	afterSession, afterFound := snapshotRebuild(materialize.After.Rebuilds, trace.Key)
	if !beforeFound || !afterCommitFound || !afterFound || !reflect.DeepEqual(beforeSession, trace.Session) || !reflect.DeepEqual(afterCommitSession, trace.Session) || !reflect.DeepEqual(afterSession, trace.Session) || !sourceMaterializationSatisfied(commit, materialize, trace.Key.Scope, 1) {
		return false
	}
	if materialize.After.Stream.Authority.GlobalMaterializationBoundary.StreamGeneration != trace.Session.SnapshotBoundary.StreamGeneration || materialize.After.Stream.Authority.GlobalMaterializationBoundary.CommitLSN <= trace.Session.SnapshotBoundary.CommitLSN {
		return false
	}
	added := rowsAddedBetween(trace.Snapshot.Rows, materialize.After.Rows)
	if len(added) != 1 || rebuildSessionContainsRow(trace.Session, added[0].Identity) {
		return false
	}
	scope, found := snapshotScope(materialize.After.Scopes, trace.Key.Scope)
	return found && scope.Checksum != trace.Session.Checksum && scope.Cardinality == reference.Cardinality(len(trace.Session.StagedRows)+1)
}

func finalRebuildPageSatisfied(execution OperationExecution, trace rebuildPageTrace) bool {
	var request semanticRebuildRequest
	if json.Unmarshal(execution.Operation.Payload, &request) != nil || execution.Err != nil || !successfulEndpointResult(execution.Result, reference.StepResultKindRebuild, false) || execution.Result.Rebuild == nil || request.CursorSource != "local_rebuild_continuation" || !sameRebuildRequestIdentity(request, trace.Request) {
		return false
	}
	_, _, assignment, scope, ok := rebuildRequestLineage(execution.Before, request)
	if !ok {
		return false
	}
	before, beforeFound := snapshotRebuild(execution.Before.Rebuilds, trace.Key)
	after, afterFound := snapshotRebuild(execution.After.Rebuilds, trace.Key)
	if !beforeFound || !afterFound || !reflect.DeepEqual(before, trace.Session) || !sameStateExceptRebuilds(execution.Before, execution.After) || !rebuildSessionLineageMatches(after, request, assignment, scope, trace.Snapshot) || after.Status != reference.RebuildStatusComplete || len(after.Pages) != 2 || !reflect.DeepEqual(after.Pages[0], trace.Session.Pages[0]) || !reflect.DeepEqual(after.StagedRows, trace.Session.StagedRows) {
		return false
	}
	observation := execution.Result.Rebuild
	page := after.Pages[1]
	if observation.Attempt != trace.Key || observation.PageOrdinal != trace.Session.NextRowOrdinal || observation.Replayed || observation.Restarted || observation.HasContinuation || observation.Continuation != (reference.OpaqueToken{}) || !observation.HasFinalCursor || observation.FinalCursor == (reference.OpaqueToken{}) || !observation.HasChecksum || observation.Checksum != trace.Session.Checksum || !rebuildObservationMatchesPage(*observation, page) {
		return false
	}
	if !page.HasToken || page.Token != trace.Session.Continuation || page.HasContinuation || page.Continuation != (reference.OpaqueToken{}) || !page.HasFinalCursor || page.FinalCursor != observation.FinalCursor || !page.HasChecksum || page.Checksum != observation.Checksum || len(page.CanonicalResponse) == 0 || after.HasContinuation || after.Continuation != (reference.OpaqueToken{}) || !after.HasFinalCursor || after.FinalCursor != page.FinalCursor || after.Checksum != page.Checksum || after.NextRowOrdinal != uint64(len(after.StagedRows)+1) {
		return false
	}
	if !rebuildObservationMatchesPage(trace.Observation, after.Pages[0]) || !rebuildPagesCoverStaged(after) {
		return false
	}
	added := rowsAddedBetween(trace.Snapshot.Rows, execution.After.Rows)
	if len(added) != 1 || rebuildSessionContainsRow(after, added[0].Identity) {
		return false
	}
	computed, checksumOK := independentScopeChecksum(trace.Snapshot, trace.Key.Scope)
	currentScope, currentFound := snapshotScope(execution.After.Scopes, trace.Key.Scope)
	return checksumOK && computed == after.Checksum && currentFound && currentScope.Checksum != after.Checksum
}

func rebuildRequestLineage(snapshot reference.StateSnapshot, request semanticRebuildRequest) (reference.ClientKey, reference.ClientState, reference.ScopeAssignment, reference.ScopeState, bool) {
	if request.UserID == "" || request.ClientID == "" || request.ClientGeneration == 0 || request.ScopeID == "" || request.RebuildID == "" || request.Limit == 0 || !semanticSchemaMatches(request.Schema, snapshot.CurrentSchema) {
		return reference.ClientKey{}, reference.ClientState{}, reference.ScopeAssignment{}, reference.ScopeState{}, false
	}
	client := reference.ClientKey{UserID: reference.UserID(request.UserID), ClientID: reference.ClientID(request.ClientID)}
	server, serverFound := snapshotClient(snapshot.Clients, client)
	local, localFound := snapshotLocalClient(snapshot.ClientLocal, client)
	assignment, assignmentFound := serverAssignment(server, reference.ScopeID(request.ScopeID))
	scope, scopeFound := snapshotScope(snapshot.Scopes, reference.ScopeID(request.ScopeID))
	if !serverFound || !localFound || !assignmentFound || !assignment.Assigned || !scopeFound || request.ClientGeneration != uint64(server.CurrentGeneration) || !assignmentLineageMatchesScopeState(snapshot, server) || !sameLocalToServer(local.ScopeAssignments, server.ScopeAssignments) {
		return reference.ClientKey{}, reference.ClientState{}, reference.ScopeAssignment{}, reference.ScopeState{}, false
	}
	return client, server, assignment, scope, true
}

func rebuildSessionLineageMatches(session reference.RebuildSession, request semanticRebuildRequest, assignment reference.ScopeAssignment, scope reference.ScopeState, boundarySnapshot reference.StateSnapshot) bool {
	return session.ClientGeneration == reference.Generation(request.ClientGeneration) && uint64(session.PageLimit) == request.Limit && session.Scope == assignment.Scope && session.Schema == boundarySnapshot.CurrentSchema && session.MembershipGeneration == assignment.MembershipGeneration && session.RetentionGeneration == assignment.RetentionGeneration && session.StreamGeneration == scope.StreamGeneration && session.SnapshotBoundary == expectedRebuildBoundary(boundarySnapshot) && session.AcceptedWriteEpoch == clientAcceptedWriteEpoch(boundarySnapshot, reference.ClientKey{UserID: reference.UserID(request.UserID), ClientID: reference.ClientID(request.ClientID)})
}

func rebuildObservationMatchesPage(observation reference.RebuildObservation, page reference.RebuildPage) bool {
	if observation.PageOrdinal != page.Ordinal || observation.HasContinuation != page.HasContinuation || observation.Continuation != page.Continuation || observation.HasFinalCursor != page.HasFinalCursor || observation.FinalCursor != page.FinalCursor || observation.HasChecksum != page.HasChecksum || observation.Checksum != page.Checksum || len(observation.Records) != len(page.Rows) {
		return false
	}
	for index := range page.Rows {
		if !sameRebuildRecord(observation.Records[index], page.Rows[index]) {
			return false
		}
	}
	return true
}

func rebuildSessionRowsMatchSnapshot(snapshot reference.StateSnapshot, session reference.RebuildSession) bool {
	scope, found := snapshotScope(snapshot.Scopes, session.Scope)
	if !found || int(scope.Cardinality) != len(session.StagedRows) {
		return false
	}
	rows := make([]reference.AuthoritativeRow, 0, scope.Cardinality)
	seen := make(map[reference.RowIdentity]struct{}, scope.Cardinality)
	for _, membership := range scope.Membership {
		if !membership.Included {
			continue
		}
		row, rowFound := snapshotRowByIdentity(snapshot.Rows, membership.Row)
		if !rowFound {
			return false
		}
		if _, duplicate := seen[row.Identity]; duplicate {
			return false
		}
		seen[row.Identity] = struct{}{}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(left, right int) bool { return lessSemanticRowIdentity(rows[left].Identity, rows[right].Identity) })
	if len(rows) != len(session.StagedRows) {
		return false
	}
	for index := range rows {
		staged := session.StagedRows[index]
		if staged.Ordinal != uint64(index+1) || !reflect.DeepEqual(staged.Row, rows[index]) || !independentRowChecksumMatches(snapshot, staged.Row) || !canonicalRowIdentityMatches(snapshot, staged.Row) {
			return false
		}
	}
	return true
}

func rebuildPagesCoverStaged(session reference.RebuildSession) bool {
	if len(session.StagedRows) == 0 {
		return len(session.Pages) == 1 && session.Pages[0].Ordinal == 1 && len(session.Pages[0].Rows) == 0 && len(session.Pages[0].CanonicalResponse) != 0
	}
	offset := 0
	for _, page := range session.Pages {
		if len(page.CanonicalResponse) == 0 || len(page.Rows) == 0 || offset >= len(session.StagedRows) || page.Ordinal != session.StagedRows[offset].Ordinal || len(page.Rows) > int(session.PageLimit) {
			return false
		}
		for _, row := range page.Rows {
			if offset >= len(session.StagedRows) || !reflect.DeepEqual(row, session.StagedRows[offset].Row) {
				return false
			}
			offset++
		}
	}
	return offset == len(session.StagedRows)
}

func sameRebuildRequestIdentity(left, right semanticRebuildRequest) bool {
	return left.UserID == right.UserID && left.ClientID == right.ClientID && left.ClientGeneration == right.ClientGeneration && left.Schema == right.Schema && left.ScopeID == right.ScopeID && left.RebuildID == right.RebuildID && left.Limit == right.Limit
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
	return successfulEndpointResult(*result, kind, kind == reference.StepResultKindPush)
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
	if !connectExecutionSatisfied(execution) || execution.Result.Connect.Client != client || execution.Result.Connect.Schema.Action != reference.SchemaActionNone || !reflect.DeepEqual(execution.Before.Authorization, execution.After.Authorization) || !connectHasSeedReceipt(execution.Operation.Payload) || !connectHasNoKnownScopes(execution.Operation.Payload) {
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
	if !connectExecutionSatisfied(execution) || execution.Result.Connect.Client != client || execution.Result.Connect.Schema.Action != reference.SchemaActionNone || !reflect.DeepEqual(execution.Before.Authorization, execution.After.Authorization) || connectHasSeedReceipt(execution.Operation.Payload) || !connectHasNoKnownScopes(execution.Operation.Payload) {
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

func semanticSchemaMatches(schema semanticSchemaReference, wanted reference.SchemaRef) bool {
	if schema.Version != wanted.Version || len(schema.Hash) != sha256.Size*2 || schema.Hash != string(bytes.ToLower([]byte(schema.Hash))) {
		return false
	}
	decoded, err := hex.DecodeString(schema.Hash)
	return err == nil && len(decoded) == len(wanted.Hash) && bytes.Equal(decoded, wanted.Hash[:])
}

func serverAssignment(client reference.ClientState, scope reference.ScopeID) (reference.ScopeAssignment, bool) {
	for _, assignment := range client.ScopeAssignments {
		if assignment.Scope == scope {
			return assignment, true
		}
	}
	return reference.ScopeAssignment{}, false
}

func assignmentLineageMatchesScopeState(snapshot reference.StateSnapshot, client reference.ClientState) bool {
	seen := make(map[reference.ScopeID]struct{}, len(client.ScopeAssignments))
	for _, assignment := range client.ScopeAssignments {
		if assignment.Scope == "" || assignment.MembershipGeneration == 0 || assignment.RetentionGeneration == 0 {
			return false
		}
		if _, duplicate := seen[assignment.Scope]; duplicate {
			return false
		}
		seen[assignment.Scope] = struct{}{}
		scope, found := snapshotScope(snapshot.Scopes, assignment.Scope)
		if !found || scope.Schema != snapshot.CurrentSchema || scope.MembershipGeneration != assignment.MembershipGeneration || scope.RetentionGeneration != assignment.RetentionGeneration || scope.StreamGeneration != snapshot.Stream.Authority.ActiveGeneration {
			return false
		}
	}
	return true
}

func sameStateExceptServerClient(before, after reference.StateSnapshot, client reference.ClientKey) bool {
	beforeCopy := before
	afterCopy := after
	beforeCopy.Clients = withoutSnapshotEntry(beforeCopy.Clients, client)
	afterCopy.Clients = withoutSnapshotEntry(afterCopy.Clients, client)
	return reflect.DeepEqual(beforeCopy, afterCopy)
}

func activeScopeIDs(client reference.ClientState) []reference.ScopeID {
	result := make([]reference.ScopeID, 0, len(client.ScopeAssignments))
	for _, assignment := range client.ScopeAssignments {
		if assignment.Assigned {
			result = append(result, assignment.Scope)
		}
	}
	sortScopeIDsForSemantic(result)
	return result
}

func scopeIDsFromConnectRequest(request semanticConnectRequest) []reference.ScopeID {
	result := make([]reference.ScopeID, 0, len(request.KnownScopes))
	for _, scope := range request.KnownScopes {
		result = append(result, reference.ScopeID(scope.ScopeID))
	}
	sortScopeIDsForSemantic(result)
	return result
}

func scopeIDsFromPullRequest(request semanticPullRequest) []reference.ScopeID {
	result := make([]reference.ScopeID, 0, len(request.Scopes))
	for _, scope := range request.Scopes {
		if scope.CursorSource != "none" {
			return nil
		}
		result = append(result, reference.ScopeID(scope.ScopeID))
	}
	sortScopeIDsForSemantic(result)
	return result
}

func requestScopeID(payload json.RawMessage) reference.ScopeID {
	var request semanticPullRequest
	if json.Unmarshal(payload, &request) != nil || len(request.Scopes) != 1 {
		return ""
	}
	return reference.ScopeID(request.Scopes[0].ScopeID)
}

func sortScopeIDsForSemantic(scopes []reference.ScopeID) {
	sort.Slice(scopes, func(left, right int) bool {
		return scopes[left] < scopes[right]
	})
}

func sameScopeAssignmentLineage(left, right []reference.ScopeAssignment) bool {
	leftCopy := append([]reference.ScopeAssignment(nil), left...)
	rightCopy := append([]reference.ScopeAssignment(nil), right...)
	sort.Slice(leftCopy, func(left, right int) bool { return leftCopy[left].Scope < leftCopy[right].Scope })
	sort.Slice(rightCopy, func(left, right int) bool { return rightCopy[left].Scope < rightCopy[right].Scope })
	if len(leftCopy) != len(rightCopy) {
		return false
	}
	for index := range leftCopy {
		if index > 0 && leftCopy[index-1].Scope == leftCopy[index].Scope || index > 0 && rightCopy[index-1].Scope == rightCopy[index].Scope {
			return false
		}
		if leftCopy[index].Scope != rightCopy[index].Scope || leftCopy[index].MembershipGeneration != rightCopy[index].MembershipGeneration || leftCopy[index].RetentionGeneration != rightCopy[index].RetentionGeneration || leftCopy[index].Assigned != rightCopy[index].Assigned {
			return false
		}
	}
	return true
}

func sameLocalToServer(local []reference.LocalScopeAssignment, server []reference.ScopeAssignment) bool {
	if !sameLocalAssignmentLineage(local, server) {
		return false
	}
	localCopy := append([]reference.LocalScopeAssignment(nil), local...)
	serverCopy := append([]reference.ScopeAssignment(nil), server...)
	sort.Slice(localCopy, func(left, right int) bool { return localCopy[left].Scope < localCopy[right].Scope })
	sort.Slice(serverCopy, func(left, right int) bool { return serverCopy[left].Scope < serverCopy[right].Scope })
	for index := range localCopy {
		if localCopy[index].RebuildRequired != serverCopy[index].RebuildRequired {
			return false
		}
	}
	return true
}

func sameLocalAssignmentLineage(local []reference.LocalScopeAssignment, server []reference.ScopeAssignment) bool {
	localCopy := append([]reference.LocalScopeAssignment(nil), local...)
	serverCopy := append([]reference.ScopeAssignment(nil), server...)
	sort.Slice(localCopy, func(left, right int) bool { return localCopy[left].Scope < localCopy[right].Scope })
	sort.Slice(serverCopy, func(left, right int) bool { return serverCopy[left].Scope < serverCopy[right].Scope })
	if len(localCopy) != len(serverCopy) {
		return false
	}
	for index := range localCopy {
		if index > 0 && localCopy[index-1].Scope == localCopy[index].Scope || index > 0 && serverCopy[index-1].Scope == serverCopy[index].Scope {
			return false
		}
		if localCopy[index].Scope != serverCopy[index].Scope || localCopy[index].MembershipGeneration != serverCopy[index].MembershipGeneration || localCopy[index].RetentionGeneration != serverCopy[index].RetentionGeneration || localCopy[index].Assigned != serverCopy[index].Assigned {
			return false
		}
	}
	return true
}

func connectCursorObservationsMatch(observations []reference.ScopeCursorObservation, assignments []reference.ScopeAssignment, wanted reference.CursorDisposition) bool {
	active := make(map[reference.ScopeID]struct{})
	for _, assignment := range assignments {
		if assignment.Assigned {
			if _, duplicate := active[assignment.Scope]; duplicate {
				return false
			}
			active[assignment.Scope] = struct{}{}
		}
	}
	if len(observations) != len(active) {
		return false
	}
	seen := make(map[reference.ScopeID]struct{}, len(observations))
	for _, observation := range observations {
		if observation.Disposition != wanted {
			return false
		}
		if _, known := active[observation.Scope]; !known {
			return false
		}
		if _, duplicate := seen[observation.Scope]; duplicate {
			return false
		}
		seen[observation.Scope] = struct{}{}
	}
	return len(seen) == len(active)
}

func scopeChecksumsMatch(checksums []reference.ScopeChecksumObservation, snapshot reference.StateSnapshot, client reference.ClientState) bool {
	active := activeScopeIDs(client)
	if len(checksums) != len(active) {
		return false
	}
	seen := make(map[reference.ScopeID]struct{}, len(checksums))
	for _, checksum := range checksums {
		if !checksum.HasChecksum {
			return false
		}
		if _, duplicate := seen[checksum.Scope]; duplicate {
			return false
		}
		seen[checksum.Scope] = struct{}{}
		state, found := snapshotScope(snapshot.Scopes, checksum.Scope)
		if !found || state.Checksum != checksum.Checksum {
			return false
		}
	}
	for _, scope := range active {
		if _, found := seen[scope]; !found {
			return false
		}
	}
	return true
}

func checksumForScope(checksums []reference.ScopeChecksumObservation, scope reference.ScopeID) reference.Checksum {
	for _, checksum := range checksums {
		if checksum.Scope == scope && checksum.HasChecksum {
			return checksum.Checksum
		}
	}
	return reference.Checksum{}
}

func snapshotScope(entries []reference.SnapshotEntry[reference.ScopeID, reference.ScopeState], scope reference.ScopeID) (reference.ScopeState, bool) {
	for _, entry := range entries {
		if entry.Key == scope {
			return entry.Value, true
		}
	}
	return reference.ScopeState{}, false
}

func snapshotMutation(entries []reference.SnapshotEntry[reference.MutationKey, reference.MutationLedger], key reference.MutationKey) (reference.MutationLedger, bool) {
	for _, entry := range entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return reference.MutationLedger{}, false
}

func snapshotBatch(entries []reference.SnapshotEntry[reference.BatchKey, reference.BatchLedger], key reference.BatchKey) (reference.BatchLedger, bool) {
	for _, entry := range entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return reference.BatchLedger{}, false
}

func snapshotRebuild(entries []reference.SnapshotEntry[reference.RebuildKey, reference.RebuildSession], key reference.RebuildKey) (reference.RebuildSession, bool) {
	for _, entry := range entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return reference.RebuildSession{}, false
}

func snapshotRowByIdentity(entries []reference.SnapshotEntry[reference.RowIdentity, reference.AuthoritativeRow], identity reference.RowIdentity) (reference.AuthoritativeRow, bool) {
	for _, entry := range entries {
		if entry.Key == identity {
			return entry.Value, true
		}
	}
	return reference.AuthoritativeRow{}, false
}

func sourceRowByIdentity(entries []reference.SourceRowEntry, identity reference.RowIdentity) (reference.SourceRowEntry, bool) {
	for _, entry := range entries {
		if entry.Identity == identity {
			return entry, true
		}
	}
	return reference.SourceRowEntry{}, false
}

func snapshotTransaction(entries []reference.StreamTransaction, key reference.TransactionReplayKey) (reference.StreamTransaction, bool) {
	for _, entry := range entries {
		if entry.ReplayKey == key {
			return entry, true
		}
	}
	return reference.StreamTransaction{}, false
}

func localRowForIdentity(rows []reference.LocalRow, identity reference.RowIdentity) (reference.LocalRow, bool) {
	for _, row := range rows {
		if row.Identity == identity {
			return row, true
		}
	}
	return reference.LocalRow{}, false
}

func localRowForMutation(local reference.ClientLocalState, mutation reference.MutationID) (reference.LocalRow, bool) {
	for _, queued := range local.DurableQueue {
		if queued.Mutation == mutation {
			return localRowForIdentity(local.Rows, queued.Row)
		}
	}
	return reference.LocalRow{}, false
}

func authoritativeMatchesLocal(authoritative reference.AuthoritativeRow, local reference.LocalRow) bool {
	return authoritative.Identity == local.Identity && authoritative.Version == local.ServerVersion && local.HasServerVersion && authoritative.Checksum == local.Checksum && local.HasChecksum && authoritative.Deleted == local.Deleted && reflect.DeepEqual(authoritative.FieldValues, local.Fields)
}

func authoritativeRowsEquivalent(left, right reference.AuthoritativeRow) bool {
	return left.Identity == right.Identity && left.Version == right.Version && left.Checksum == right.Checksum && left.Deleted == right.Deleted && reflect.DeepEqual(left.FieldValues, right.FieldValues)
}

func sourceImageMatchesAuthoritative(image reference.SourceImage, row reference.AuthoritativeRow) bool {
	return image.Identity.Kind == reference.RegistrationKindSynced && image.Identity.SyncedRow == row.Identity && image.Version == row.Version && image.HasChecksum && image.Checksum == row.Checksum && image.Deleted == row.Deleted && reflect.DeepEqual(image.Fields, row.FieldValues)
}

func independentRowChecksumMatches(snapshot reference.StateSnapshot, row reference.AuthoritativeRow) bool {
	if row.Identity.TableID == "" || row.Identity.PrimaryKeyFieldID == "" || row.Identity.CanonicalWireJSON == "" || row.Version == "" || row.Checksum == (reference.Checksum{}) || !canonicalRowIdentityMatches(snapshot, row) {
		return false
	}
	record, found := snapshotSchema(snapshot.Schemas, snapshot.CurrentSchema)
	if !found || len(record.Body) == 0 {
		return false
	}
	manifest, err := vectors.ParseManifest(record.Body)
	if err != nil || manifest.Hash() != snapshot.CurrentSchema.Hash {
		return false
	}
	table, tableFound := schemaTable(record, row.Identity.TableID)
	if !tableFound || table.PrimaryKeyFieldID != row.Identity.PrimaryKeyFieldID || len(table.Fields) != len(row.FieldValues) {
		return false
	}
	fieldTypes := make(map[reference.FieldID]reference.PortableType, len(table.Fields))
	for _, field := range table.Fields {
		if _, duplicate := fieldTypes[field.ID]; duplicate {
			return false
		}
		fieldTypes[field.ID] = field.PortableType
	}
	fields := make([]vectors.RowField, 0, len(row.FieldValues))
	seen := make(map[reference.FieldID]struct{}, len(row.FieldValues))
	primaryFound := false
	for _, field := range row.FieldValues {
		portable, known := fieldTypes[field.Field]
		if !known || portable != field.Type || field.WireJSON == "" {
			return false
		}
		if _, duplicate := seen[field.Field]; duplicate {
			return false
		}
		seen[field.Field] = struct{}{}
		if field.Field == row.Identity.PrimaryKeyFieldID {
			primaryFound = field.WireJSON == row.Identity.CanonicalWireJSON
		}
		fields = append(fields, vectors.RowField{FieldID: string(field.Field), Value: json.RawMessage(field.WireJSON)})
	}
	if !primaryFound {
		return false
	}
	digest, err := vectors.RowDigest(manifest, string(row.Identity.TableID), vectors.Row{PK: json.RawMessage(row.Identity.CanonicalWireJSON), Fields: fields}, string(row.Version))
	return err == nil && reference.Checksum(digest) == row.Checksum
}

func canonicalRowIdentityMatches(snapshot reference.StateSnapshot, row reference.AuthoritativeRow) bool {
	record, found := snapshotSchema(snapshot.Schemas, snapshot.CurrentSchema)
	if !found {
		return false
	}
	manifest, err := vectors.ParseManifest(record.Body)
	if err != nil || manifest.Hash() != snapshot.CurrentSchema.Hash {
		return false
	}
	identity, err := vectors.RowIdentity(manifest, string(row.Identity.TableID), json.RawMessage(row.Identity.CanonicalWireJSON))
	return err == nil && string(identity) == row.Identity.CanonicalIdentityBytes
}

func independentScopeChecksum(snapshot reference.StateSnapshot, scopeID reference.ScopeID) (reference.Checksum, bool) {
	scope, found := snapshotScope(snapshot.Scopes, scopeID)
	if !found || scopeID == "" || scope.Schema != snapshot.CurrentSchema || scope.Cardinality != reference.Cardinality(lenIncludedMembership(scope.Membership)) {
		return reference.Checksum{}, false
	}
	type digestRow struct {
		identity string
		checksum reference.Checksum
	}
	rows := make([]digestRow, 0, scope.Cardinality)
	seen := make(map[reference.RowIdentity]struct{}, scope.Cardinality)
	for _, membership := range scope.Membership {
		if !membership.Included {
			continue
		}
		if _, duplicate := seen[membership.Row]; duplicate {
			return reference.Checksum{}, false
		}
		seen[membership.Row] = struct{}{}
		row, rowFound := snapshotRowByIdentity(snapshot.Rows, membership.Row)
		if !rowFound || row.Deleted || !canonicalRowIdentityMatches(snapshot, row) || !independentRowChecksumMatches(snapshot, row) {
			return reference.Checksum{}, false
		}
		rows = append(rows, digestRow{identity: row.Identity.CanonicalIdentityBytes, checksum: row.Checksum})
	}
	sort.Slice(rows, func(left, right int) bool { return rows[left].identity < rows[right].identity })
	for index := 1; index < len(rows); index++ {
		if rows[index-1].identity == rows[index].identity {
			return reference.Checksum{}, false
		}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("synchro:v3:scope-digest:v1\x00"))
	_, _ = hash.Write(snapshot.CurrentSchema.Hash[:])
	writeSemanticBlob(hash, []byte(scopeID))
	writeSemanticUint64(hash, uint64(len(rows)))
	for _, row := range rows {
		writeSemanticBlob(hash, []byte(row.identity))
		_, _ = hash.Write(row.checksum[:])
	}
	var checksum reference.Checksum
	copy(checksum[:], hash.Sum(nil))
	return checksum, true
}

func activeScopeChecksumsAreIndependent(snapshot reference.StateSnapshot, client reference.ClientState) bool {
	for _, scopeID := range activeScopeIDs(client) {
		computed, valid := independentScopeChecksum(snapshot, scopeID)
		stored, found := snapshotScope(snapshot.Scopes, scopeID)
		if !valid || !found || computed != stored.Checksum {
			return false
		}
	}
	return true
}

func snapshotSchema(entries []reference.SnapshotEntry[reference.SchemaRef, reference.SchemaManifest], schema reference.SchemaRef) (reference.SchemaManifest, bool) {
	for _, entry := range entries {
		if entry.Key == schema {
			return entry.Value, true
		}
	}
	return reference.SchemaManifest{}, false
}

func schemaTable(manifest reference.SchemaManifest, tableID reference.TableID) (reference.TableManifest, bool) {
	for _, table := range manifest.Tables {
		if table.ID == tableID {
			return table, true
		}
	}
	return reference.TableManifest{}, false
}

func lenIncludedMembership(memberships []reference.ScopeMembership) int {
	count := 0
	for _, membership := range memberships {
		if membership.Included {
			count++
		}
	}
	return count
}

func writeSemanticBlob(destination interface{ Write([]byte) (int, error) }, value []byte) {
	writeSemanticUint64(destination, uint64(len(value)))
	_, _ = destination.Write(value)
}

func writeSemanticUint64(destination interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = destination.Write(encoded[:])
}

func rowsAddedBetween(before, after []reference.SnapshotEntry[reference.RowIdentity, reference.AuthoritativeRow]) []reference.AuthoritativeRow {
	known := make(map[reference.RowIdentity]struct{}, len(before))
	for _, row := range before {
		known[row.Key] = struct{}{}
	}
	added := make([]reference.AuthoritativeRow, 0)
	for _, row := range after {
		if _, found := known[row.Key]; !found {
			added = append(added, row.Value)
		}
	}
	return added
}

func rebuildSessionContainsRow(session reference.RebuildSession, identity reference.RowIdentity) bool {
	for _, row := range session.StagedRows {
		if row.Row.Identity == identity {
			return true
		}
	}
	return false
}

func lessSemanticRowIdentity(left, right reference.RowIdentity) bool {
	if left.CanonicalIdentityBytes != right.CanonicalIdentityBytes {
		return left.CanonicalIdentityBytes < right.CanonicalIdentityBytes
	}
	if left.TableID != right.TableID {
		return left.TableID < right.TableID
	}
	if left.PrimaryKeyFieldID != right.PrimaryKeyFieldID {
		return left.PrimaryKeyFieldID < right.PrimaryKeyFieldID
	}
	if left.PortableType != right.PortableType {
		return left.PortableType < right.PortableType
	}
	return left.CanonicalWireJSON < right.CanonicalWireJSON
}

func pushOutcomeBaseMatches(response []byte, row reference.LocalRow) bool {
	var outcome semanticPushOutcome
	if json.Unmarshal(response, &outcome) != nil || outcome.MutationID == "" || outcome.Table != string(row.Identity.TableID) || outcome.Status != string(reference.MutationOutcomeApplied) || outcome.ServerVersion == nil || outcome.RowChecksum == nil || outcome.RowChecksum.Algorithm != "sha256" || outcome.RowChecksum.Version != 1 || outcome.RowChecksum.Encoding != "hex" || *outcome.ServerVersion != string(row.ServerVersion) || len(outcome.ServerRow) != len(row.Fields) {
		return false
	}
	for _, field := range row.Fields {
		value, found := outcome.ServerRow[string(field.Field)]
		if !found || !bytes.Equal(bytes.TrimSpace(value), bytes.TrimSpace([]byte(field.WireJSON))) {
			return false
		}
	}
	decoded, err := hex.DecodeString(outcome.RowChecksum.Digest)
	if err != nil || len(decoded) != len(row.Checksum) {
		return false
	}
	var checksum reference.Checksum
	copy(checksum[:], decoded)
	return row.HasChecksum && checksum == row.Checksum
}

func expectedRebuildBoundary(snapshot reference.StateSnapshot) reference.StreamPosition {
	boundary := snapshot.Stream.Authority.GlobalMaterializationBoundary
	if boundary.StreamGeneration == "" {
		return reference.StreamPosition{StreamGeneration: snapshot.Stream.Authority.ActiveGeneration, Kind: reference.PositionKindGenerationStart}
	}
	if boundary.Kind == reference.PositionKindEffect {
		return reference.StreamPosition{StreamGeneration: boundary.StreamGeneration, Kind: reference.PositionKindTransactionEnd, CommitLSN: boundary.CommitLSN}
	}
	return boundary
}

func scopeChecksum(snapshot reference.StateSnapshot, scope reference.ScopeID) reference.Checksum {
	state, found := snapshotScope(snapshot.Scopes, scope)
	if !found {
		return reference.Checksum{}
	}
	return state.Checksum
}

func clientAcceptedWriteEpoch(snapshot reference.StateSnapshot, client reference.ClientKey) reference.AcceptedWriteEpoch {
	state, found := snapshotClient(snapshot.Clients, client)
	if !found {
		return 0
	}
	return state.AcceptedWriteEpoch
}

func sameRebuildRecord(observation reference.RebuildRecordObservation, row reference.AuthoritativeRow) bool {
	return observation.Row == row.Identity && observation.Version == row.Version && observation.Deleted == row.Deleted && observation.HasChecksum && observation.Checksum == row.Checksum
}

func singleConnectedEventAppended(before, after reference.StateSnapshot, client reference.ClientKey) bool {
	if len(after.Events) != len(before.Events)+1 {
		return false
	}
	for index := range before.Events {
		if !reflect.DeepEqual(before.Events[index], after.Events[index]) {
			return false
		}
	}
	if len(after.Events) == 0 {
		return false
	}
	event := after.Events[len(after.Events)-1]
	return event.Kind == reference.ModelEventConnected && event.HasClient && event.Client == client
}

func sameStateExceptClientTransition(before, after reference.StateSnapshot, client reference.ClientKey) bool {
	beforeCopy := before
	afterCopy := after
	beforeCopy.Clients = withoutSnapshotEntry(beforeCopy.Clients, client)
	afterCopy.Clients = withoutSnapshotEntry(afterCopy.Clients, client)
	beforeCopy.ClientLocal = withoutSnapshotEntry(beforeCopy.ClientLocal, client)
	afterCopy.ClientLocal = withoutSnapshotEntry(afterCopy.ClientLocal, client)
	beforeCopy.Events = nil
	afterCopy.Events = nil
	return reflect.DeepEqual(beforeCopy, afterCopy)
}

func sameStateExceptLocalTarget(before, after reference.StateSnapshot, client reference.ClientKey) bool {
	beforeCopy := before
	afterCopy := after
	beforeCopy.ClientLocal = withoutSnapshotEntry(beforeCopy.ClientLocal, client)
	afterCopy.ClientLocal = withoutSnapshotEntry(afterCopy.ClientLocal, client)
	return reflect.DeepEqual(beforeCopy, afterCopy)
}

func sameStateExceptRebuilds(before, after reference.StateSnapshot) bool {
	beforeCopy := before
	afterCopy := after
	beforeCopy.Rebuilds = nil
	afterCopy.Rebuilds = nil
	return reflect.DeepEqual(beforeCopy, afterCopy)
}

func withoutSnapshotEntry[K comparable, V any](entries []reference.SnapshotEntry[K, V], key K) []reference.SnapshotEntry[K, V] {
	result := make([]reference.SnapshotEntry[K, V], 0, len(entries))
	for _, entry := range entries {
		if entry.Key != key {
			result = append(result, entry)
		}
	}
	return result
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
