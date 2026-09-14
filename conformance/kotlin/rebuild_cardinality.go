package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const rebuildCardinalityScenarioID = "SCN-PERF-REBUILD-CARDINALITY-001"

var rebuildCardinalityAliasNames = []string{
	"client-generation-one",
	"current-schema",
	"scope-a",
	"items-table",
}

// RebuildCardinalityResult records direct Kotlin Android evidence for all authored cardinality samples.
type RebuildCardinalityResult struct {
	Calls              []ClientCallResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type rebuildCardinalityWorkload struct {
	Profile     string `json:"profile"`
	ScopeID     string `json:"scope_id"`
	RecordCount uint64 `json:"record_count"`
	PageSize    uint64 `json:"page_size"`
}

type rebuildCardinalityExpansion struct {
	PageCount int
}

type rebuildCardinalityControlPayload struct {
	UserID             string    `json:"user_id"`
	ClientID           string    `json:"client_id"`
	ClientGeneration   int64     `json:"client_generation"`
	Schema             schemaRef `json:"schema"`
	ScopeID            string    `json:"scope_id"`
	RebuildID          string    `json:"rebuild_id"`
	Limit              uint64    `json:"limit"`
	PageOrdinal        uint64    `json:"page_ordinal"`
	RequestTokenSource string    `json:"request_token_source"`
	CursorSource       string    `json:"cursor_source"`
}

type rebuildCardinalityCommitPayload struct {
	Events []struct {
		Operation string `json:"operation"`
	} `json:"events"`
}

type rebuildCardinalityIdentityEvidence struct {
	Resolutions      []blackbox.NativeIdentityResolution
	RuntimeSchema    schemaRef
	RuntimeScope     string
	ApplicationTable string
	ClientGeneration int64
}

// RunRebuildCardinalityScenario executes every authored cardinality sample through Kotlin Android.
func RunRebuildCardinalityScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform) (RebuildCardinalityResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, rebuildCardinalityScenarioID, 9)
	if err != nil {
		return RebuildCardinalityResult{}, err
	}
	if controller == nil || platform == nil {
		return RebuildCardinalityResult{}, errors.New("Kotlin Android rebuild-cardinality dependencies are unavailable")
	}
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-PERF-REBUILD-CARDINALITY-SEMANTIC-001")
	if err != nil {
		return RebuildCardinalityResult{}, err
	}
	clients, workloads, _, err := rebuildCardinalityBindings(scenario, steps)
	if err != nil {
		return RebuildCardinalityResult{}, err
	}
	captureSources := []string{
		"application-rows",
		"pending-mutations",
		"rejected-mutations",
		"checkpoints",
		"provenance",
		"rebuild-state",
	}

	modelResult, err := modelrunner.RunScenario(ctx, scenario)
	if err != nil {
		return RebuildCardinalityResult{}, fmt.Errorf("derive Kotlin Android rebuild-cardinality source operations from the authored model: %w", err)
	}
	if !modelResult.Passed || len(modelResult.Steps) != len(scenario.Steps) {
		return RebuildCardinalityResult{}, errors.New("authored rebuild-cardinality model did not close all workload steps")
	}

	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return RebuildCardinalityResult{}, fmt.Errorf("install Kotlin Android rebuild-cardinality contract: %w", err)
	}

	calls := make([]ClientCallResult, 0, len(scenario.Steps))
	callEvidence := make([][]TransportObservation, 0, len(scenario.Steps))
	clientFacts := make([]CaptureFacts, 0, len(scenario.Steps)*len(captureSources))
	currentRecordCount := uint64(0)
	for index, authoredStep := range scenario.Steps {
		step := steps[authoredStep.ID]
		modelStep := modelResult.Steps[index]
		if modelStep.StepID != authoredStep.ID {
			return RebuildCardinalityResult{}, fmt.Errorf("authored rebuild-cardinality model step %s is bound to %s", authoredStep.ID, modelStep.StepID)
		}
		workload := workloads[authoredStep.ID]
		expansion, err := executeKotlinRebuildCardinalityExpansion(ctx, controller, modelStep.Expanded, step, workload, currentRecordCount)
		if err != nil {
			return RebuildCardinalityResult{}, fmt.Errorf("execute Kotlin Android rebuild-cardinality source for step %s: %w", authoredStep.ID, err)
		}
		wantPages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
		if expansion.PageCount != wantPages {
			return RebuildCardinalityResult{}, fmt.Errorf("Kotlin Android rebuild-cardinality step %s has %d rebuild pages, want %d", authoredStep.ID, expansion.PageCount, wantPages)
		}

		client := clients[step.NativeBinding.ClientID]
		if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "empty"}); err != nil {
			return RebuildCardinalityResult{}, fmt.Errorf("install fresh Kotlin Android rebuild-cardinality client %s: %w", client.ClientID, err)
		}
		scopeSetVersion, err := rebuildCardinalityScopeSetVersion(scenario, client, workload.ScopeID)
		if err != nil {
			return RebuildCardinalityResult{}, err
		}
		call, transport, err := runKotlinRebuildCardinalityCall(ctx, platform, client, modelStep.Expanded, workload, scopeSetVersion)
		if err != nil {
			return RebuildCardinalityResult{}, fmt.Errorf("run Kotlin Android rebuild-cardinality client %s: %w", client.ClientID, err)
		}
		if err := validateKotlinRebuildCardinalityCall(call, transport, workload); err != nil {
			return RebuildCardinalityResult{}, fmt.Errorf("validate Kotlin Android rebuild-cardinality client %s: %w", client.ClientID, err)
		}
		capturedFacts, err := platform.Capture(ctx, []Client{client}, captureSources)
		if err != nil {
			return RebuildCardinalityResult{}, fmt.Errorf("capture Kotlin Android rebuild-cardinality client %s state: %w", client.ClientID, err)
		}
		if err := closeKotlinRebuildCardinalityClient(ctx, platform, client); err != nil {
			return RebuildCardinalityResult{}, fmt.Errorf("release Kotlin Android rebuild-cardinality client %s: %w", client.ClientID, err)
		}
		calls = append(calls, call)
		callEvidence = append(callEvidence, transport)
		clientFacts = append(clientFacts, capturedFacts...)
		currentRecordCount = workload.RecordCount
	}

	actualClient, err := mergeKotlinCaptureFacts(clientFacts)
	if err != nil {
		return RebuildCardinalityResult{}, err
	}

	clientKeys := make([]string, 0, len(clients))
	for _, client := range rebuildCardinalityClientsInOrder(clients) {
		clientKeys = append(clientKeys, client.Key)
	}
	serverCaptures, err := controller.Capture(ctx, clientKeys, []string{"server-state"})
	if err != nil {
		return RebuildCardinalityResult{}, fmt.Errorf("capture Kotlin Android rebuild-cardinality server state: %w", err)
	}
	if len(serverCaptures) != 1 {
		return RebuildCardinalityResult{}, fmt.Errorf("capture Kotlin Android rebuild-cardinality server state returned %d captures, want 1", len(serverCaptures))
	}
	serverFacts := serverCaptures[0].StateFacts

	evidence, err := resolveKotlinRebuildCardinalityIdentities(controller, scenario.NativeIdentityAliases, callEvidence, actualClient)
	if err != nil {
		return RebuildCardinalityResult{}, err
	}
	if err := validateKotlinRebuildCardinalityState(expected, serverFacts, actualClient, evidence); err != nil {
		return RebuildCardinalityResult{}, err
	}

	return RebuildCardinalityResult{
		Calls:              calls,
		ClientFacts:        clientFacts,
		ServerFacts:        serverFacts,
		IdentityResolution: evidence.Resolutions,
	}, nil
}

func rebuildCardinalityBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step) (map[string]Client, map[scenarios.StepID]rebuildCardinalityWorkload, uint64, error) {
	clients := make(map[string]Client, len(steps))
	workloads := make(map[scenarios.StepID]rebuildCardinalityWorkload, len(steps))
	pageSize := uint64(0)
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "workload" || binding.Workload == nil || binding.UserID == "" || binding.ClientID == "" {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality step %s workload binding is invalid", step.ID)
		}
		if steps[step.ID].ID != step.ID {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality step %s is not bound in the step map", step.ID)
		}
		var workload rebuildCardinalityWorkload
		if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil || workload.Profile != "scope_cardinality" || workload.ScopeID != "scope-a" || workload.RecordCount == 0 || workload.PageSize == 0 {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality step %s workload payload is invalid", step.ID)
		}
		if workload.RecordCount != binding.Workload.RecordCount || binding.Workload.BatchSize != binding.Workload.RecordCount || len(binding.Workload.Targets) != 1 || binding.Workload.Targets[0].ScopeID != workload.ScopeID || binding.Workload.Targets[0].TableID != "items" || binding.Workload.Targets[0].PrimaryKeyFieldID != "id" {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality step %s workload target is invalid", step.ID)
		}
		if len(binding.Workload.MutationKinds) != 1 || binding.Workload.MutationKinds[0].Operation != "insert" || binding.Workload.MutationKinds[0].Count != workload.RecordCount || !reflect.DeepEqual(binding.Workload.MutationKinds[0].FieldIDs, []string{"value"}) {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality step %s workload mutation binding is invalid", step.ID)
		}
		if step.ExpectedOutcome.Disposition != "success" {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality step %s does not expect success", step.ID)
		}
		if pageSize == 0 {
			pageSize = workload.PageSize
		} else if pageSize != workload.PageSize {
			return nil, nil, 0, errors.New("Kotlin Android rebuild-cardinality workload page sizes differ")
		}
		if _, duplicate := clients[binding.ClientID]; duplicate {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-cardinality client %s is not fresh", binding.ClientID)
		}
		clients[binding.ClientID] = Client{
			Key:         "rebuild-cardinality-" + binding.ClientID,
			UserID:      binding.UserID,
			ClientID:    binding.ClientID,
			DatabaseKey: "rebuild-cardinality-" + binding.ClientID,
		}
		workloads[step.ID] = workload
	}
	if len(clients) != len(scenario.Steps) || len(workloads) != len(scenario.Steps) || pageSize == 0 {
		return nil, nil, 0, errors.New("Kotlin Android rebuild-cardinality workload bindings are incomplete")
	}
	return clients, workloads, pageSize, nil
}

func rebuildCardinalityClientsInOrder(clients map[string]Client) []Client {
	keys := make([]string, 0, len(clients))
	for key := range clients {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Client, 0, len(keys))
	for _, key := range keys {
		result = append(result, clients[key])
	}
	return result
}

func closeKotlinRebuildCardinalityClient(ctx context.Context, platform *Platform, client Client) error {
	state, err := platform.clientFor(client)
	if err != nil {
		return err
	}
	state.mu.Lock()
	if state.terminated || state.session == nil || state.activeCall != nil {
		state.mu.Unlock()
		return errors.New("Kotlin Android rebuild-cardinality client is unavailable for release")
	}
	session := state.session
	state.session = nil
	state.terminated = true
	state.started = false
	state.mu.Unlock()
	return session.Close(ctx)
}

func rebuildCardinalityScopeSetVersion(scenario scenarios.Scenario, client Client, scopeID string) (int64, error) {
	var payload struct {
		Clients []struct {
			UserID           string   `json:"user_id"`
			ClientID         string   `json:"client_id"`
			ScopeSetVersion  int64    `json:"scope_set_version"`
			AssignedScopeIDs []string `json:"assigned_scope_ids"`
		} `json:"clients"`
	}
	if len(scenario.Model.Setup) != 1 || json.Unmarshal(scenario.Model.Setup[0].Payload, &payload) != nil {
		return 0, errors.New("Kotlin Android rebuild-cardinality authored client setup is invalid")
	}
	for _, candidate := range payload.Clients {
		if candidate.UserID != client.UserID || candidate.ClientID != client.ClientID {
			continue
		}
		if candidate.ScopeSetVersion <= 0 || len(candidate.AssignedScopeIDs) != 1 || candidate.AssignedScopeIDs[0] != scopeID {
			return 0, errors.New("Kotlin Android rebuild-cardinality authored client scope setup is invalid")
		}
		return candidate.ScopeSetVersion, nil
	}
	return 0, fmt.Errorf("Kotlin Android rebuild-cardinality client %s is absent from the authored setup", client.ClientID)
}

func executeKotlinRebuildCardinalityExpansion(ctx context.Context, controller *blackbox.NativeController, operations []scenarios.Operation, step scenarios.Step, workload rebuildCardinalityWorkload, priorRecordCount uint64) (rebuildCardinalityExpansion, error) {
	if len(operations) == 0 {
		return rebuildCardinalityExpansion{}, errors.New("Kotlin Android rebuild-cardinality workload expansion is empty")
	}
	if step.NativeBinding == nil {
		return rebuildCardinalityExpansion{}, errors.New("Kotlin Android rebuild-cardinality workload binding is absent")
	}
	pageCount := 0
	commitSeen := false
	materializeSeen := false
	stageSeen := false
	activateSeen := false
	beginSeen := false
	requestSeen := false
	applySeen := false
	finalizeSeen := false
	currentRebuildID := ""
	for _, operation := range operations {
		key := scenarios.OperationKey(operation)
		switch key {
		case "model/stage-registry-membership-generation":
			if stageSeen || commitSeen || materializeSeen || beginSeen {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion has an out-of-order membership stage")
			}
			stageSeen = true
		case "model/activate-registry-membership-generation":
			if !stageSeen || activateSeen || commitSeen || materializeSeen || beginSeen {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion has an out-of-order membership activation")
			}
			activateSeen = true
		case "model/commit-source-transaction":
			if commitSeen || materializeSeen || beginSeen || (stageSeen && !activateSeen) {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion has an out-of-order source commit")
			}
			if err := validateKotlinRebuildCardinalityCommit(operation, priorRecordCount, workload.RecordCount); err != nil {
				return rebuildCardinalityExpansion{}, err
			}
			observation, err := controller.ApplyStep(ctx, operation)
			if err != nil || observation.Disposition != "success" {
				return rebuildCardinalityExpansion{}, fmt.Errorf("apply source commit: %w", kotlinResultError(err, observation.Disposition))
			}
			commitSeen = true
		case "process/materialize-source-transaction":
			if !commitSeen || materializeSeen || beginSeen {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion materialization is out of order")
			}
			observation, err := controller.ProcessStep(ctx, nil, operation)
			if err != nil || observation.Disposition != "success" {
				return rebuildCardinalityExpansion{}, fmt.Errorf("materialize source transaction: %w", kotlinResultError(err, observation.Disposition))
			}
			materializeSeen = true
		case "local/begin-rebuild":
			if !materializeSeen || beginSeen || pageCount != 0 {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild begin is out of order")
			}
			payload, err := decodeKotlinRebuildCardinalityControl(operation)
			if err != nil {
				return rebuildCardinalityExpansion{}, err
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.Limit != workload.PageSize || payload.RebuildID == "" || payload.Schema.Version != int64(step.NativeBinding.Workload.AuthoredSchema.Version) || payload.Schema.Hash != step.NativeBinding.Workload.AuthoredSchema.Hash {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild begin binding is invalid")
			}
			currentRebuildID = payload.RebuildID
			beginSeen = true
		case "rebuild/request-page":
			if !beginSeen || finalizeSeen {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild request is out of order")
			}
			payload, err := decodeKotlinRebuildCardinalityControl(operation)
			if err != nil {
				return rebuildCardinalityExpansion{}, err
			}
			wantCursorSource := "none"
			if pageCount > 0 {
				wantCursorSource = "local_rebuild_continuation"
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.Limit != workload.PageSize || payload.RebuildID != currentRebuildID || payload.CursorSource != wantCursorSource {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild request binding is invalid")
			}
			requestSeen = true
		case "local/apply-rebuild-page":
			if !beginSeen || !requestSeen || finalizeSeen {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild apply is out of order")
			}
			payload, err := decodeKotlinRebuildCardinalityControl(operation)
			if err != nil {
				return rebuildCardinalityExpansion{}, err
			}
			wantOrdinal := uint64(pageCount)*workload.PageSize + 1
			wantTokenSource := "none"
			if pageCount > 0 {
				wantTokenSource = "local_rebuild_continuation"
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.RebuildID != currentRebuildID || payload.PageOrdinal != wantOrdinal || payload.RequestTokenSource != wantTokenSource {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild apply binding is invalid")
			}
			pageCount++
			requestSeen = false
			applySeen = true
		case "local/finalize-rebuild":
			if !beginSeen || !applySeen || requestSeen || finalizeSeen {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild finalize is out of order")
			}
			payload, err := decodeKotlinRebuildCardinalityControl(operation)
			if err != nil {
				return rebuildCardinalityExpansion{}, err
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.RebuildID != currentRebuildID {
				return rebuildCardinalityExpansion{}, errors.New("workload expansion rebuild finalize binding is invalid")
			}
			finalizeSeen = true
		default:
			return rebuildCardinalityExpansion{}, fmt.Errorf("workload expansion operation %q is unsupported", key)
		}
	}
	if stageSeen != activateSeen || !commitSeen || !materializeSeen || !beginSeen || !applySeen || !finalizeSeen || requestSeen || pageCount == 0 {
		return rebuildCardinalityExpansion{}, errors.New("workload expansion did not close source and rebuild phases")
	}
	return rebuildCardinalityExpansion{PageCount: pageCount}, nil
}

func runKotlinRebuildCardinalityCall(ctx context.Context, platform *Platform, client Client, operations []scenarios.Operation, workload rebuildCardinalityWorkload, scopeSetVersion int64) (ClientCallResult, []TransportObservation, error) {
	rebuilds := make([]scenarios.Operation, 0)
	for _, operation := range operations {
		if scenarios.OperationKey(operation) == "rebuild/request-page" {
			rebuilds = append(rebuilds, operation)
		}
	}
	wantPages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	if len(rebuilds) != wantPages || scopeSetVersion <= 0 {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-cardinality staged workload is invalid")
	}
	state, err := platform.clientFor(client)
	if err != nil {
		return ClientCallResult{}, nil, err
	}
	state.mu.Lock()
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()

	callID := "rebuild_cardinality_" + client.ClientID
	begin, err := platform.BeginCall(ctx, CallRequest{Client: client, CallID: callID, Method: "start", Operations: []scenarios.Operation{rebuilds[0]}})
	if err != nil {
		return ClientCallResult{}, nil, err
	}
	if begin.CallID != callID || begin.State != "in_flight" || begin.Completion != "" || len(begin.Steps) != 1 || begin.Steps[0].Disposition != "success" || begin.Steps[0].Wire == nil || begin.Steps[0].Wire.HTTPStatus != 200 || begin.Steps[0].Wire.ErrorCode != nil || begin.Steps[0].Wire.Retryable {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-cardinality first page did not enter the staged call")
	}
	for index := 1; index < len(rebuilds); index++ {
		step, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: rebuilds[index]})
		if err != nil {
			return ClientCallResult{}, nil, fmt.Errorf("await Kotlin Android rebuild-cardinality page %d: %w", index+1, err)
		}
		if step.Disposition != "success" || step.Wire == nil || step.Wire.HTTPStatus != 200 || step.Wire.ErrorCode != nil || step.Wire.Retryable {
			return ClientCallResult{}, nil, fmt.Errorf("Kotlin Android rebuild-cardinality page %d did not succeed", index+1)
		}
	}
	pull, err := rebuildCardinalityPullOperation(rebuilds[0], scopeSetVersion, workload.PageSize)
	if err != nil {
		return ClientCallResult{}, nil, err
	}
	pullStep, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: pull})
	if err != nil {
		return ClientCallResult{}, nil, fmt.Errorf("await Kotlin Android rebuild-cardinality final pull: %w", err)
	}
	if pullStep.Disposition != "success" || pullStep.Wire == nil || pullStep.Wire.HTTPStatus != 200 || pullStep.Wire.ErrorCode != nil || pullStep.Wire.Retryable {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-cardinality final pull did not succeed")
	}
	completed, err := platform.AwaitCall(ctx, CallRequest{Client: client, CallID: callID})
	if err != nil {
		return ClientCallResult{}, nil, fmt.Errorf("complete Kotlin Android rebuild-cardinality call: %w", err)
	}
	if completed.CallID != callID || completed.State != "completed" || completed.Completion != "idle" {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-cardinality call did not complete idle")
	}
	state.mu.Lock()
	transport, transportErr := state.session.ObservationsAfter(transportCheckpoint)
	state.mu.Unlock()
	if transportErr != nil {
		return ClientCallResult{}, nil, fmt.Errorf("capture Kotlin Android rebuild-cardinality transport: %w", transportErr)
	}
	return completed, transport, nil
}

func rebuildCardinalityPullOperation(firstRebuild scenarios.Operation, scopeSetVersion int64, pageSize uint64) (scenarios.Operation, error) {
	control, err := decodeKotlinRebuildCardinalityControl(firstRebuild)
	if err != nil || control.UserID == "" || control.ClientID == "" || control.ClientGeneration <= 0 || control.Schema.Version <= 0 || control.Schema.Hash == "" || control.ScopeID == "" || scopeSetVersion <= 0 || pageSize == 0 {
		return scenarios.Operation{}, errors.New("Kotlin Android rebuild-cardinality final pull binding is invalid")
	}
	payload, err := json.Marshal(map[string]any{
		"user_id":           control.UserID,
		"client_id":         control.ClientID,
		"client_generation": control.ClientGeneration,
		"schema":            control.Schema,
		"scope_set_version": scopeSetVersion,
		"scopes": []map[string]string{{
			"scope_id":      control.ScopeID,
			"cursor_source": "local_checkpoint",
		}},
		"limit": pageSize,
	})
	if err != nil {
		return scenarios.Operation{}, fmt.Errorf("encode Kotlin Android rebuild-cardinality final pull: %w", err)
	}
	operation := scenarios.Operation{ContractOperation: "pull", Name: "request-page", Payload: payload}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, fmt.Errorf("validate Kotlin Android rebuild-cardinality final pull: %w", err)
	}
	return operation, nil
}

func validateKotlinRebuildCardinalityCommit(operation scenarios.Operation, priorRecordCount, recordCount uint64) error {
	var payload rebuildCardinalityCommitPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Events) == 0 {
		return errors.New("Kotlin Android rebuild-cardinality source commit payload is invalid")
	}
	wantEvents := uint64(1)
	wantOperation := "update"
	if recordCount > priorRecordCount {
		wantEvents = recordCount - priorRecordCount
		wantOperation = "insert"
	}
	if uint64(len(payload.Events)) != wantEvents {
		return fmt.Errorf("Kotlin Android rebuild-cardinality source event count = %d, want %d", len(payload.Events), wantEvents)
	}
	for _, event := range payload.Events {
		if event.Operation != wantOperation {
			return fmt.Errorf("Kotlin Android rebuild-cardinality source event operation = %q, want %q", event.Operation, wantOperation)
		}
	}
	return nil
}

func decodeKotlinRebuildCardinalityControl(operation scenarios.Operation) (rebuildCardinalityControlPayload, error) {
	var payload rebuildCardinalityControlPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return rebuildCardinalityControlPayload{}, fmt.Errorf("decode Kotlin Android rebuild-cardinality %s payload: %w", scenarios.OperationKey(operation), err)
	}
	return payload, nil
}

func validateKotlinRebuildCardinalityCall(call ClientCallResult, observations []TransportObservation, workload rebuildCardinalityWorkload) error {
	if call.State != "completed" || call.Completion != "idle" {
		return fmt.Errorf("completion = %q, want idle", call.Completion)
	}
	pageCount := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	if len(observations) != pageCount+2 {
		return fmt.Errorf("transport observation count = %d, want %d", len(observations), pageCount+2)
	}
	if observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return errors.New("Kotlin Android rebuild-cardinality call does not start with connect and end with pull")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable == nil || *observation.Retryable || observation.ErrorCode != nil {
			return errors.New("Kotlin Android rebuild-cardinality call contains an unsuccessful transport response")
		}
	}
	for index := 0; index < pageCount; index++ {
		observation := observations[index+1]
		if observation.OperationClass != "rebuild" || observation.RequestFacts == nil || observation.RebuildResponseFacts == nil {
			return fmt.Errorf("rebuild page %d transport facts are incomplete", index+1)
		}
		request := observation.RequestFacts
		if request.ClientGeneration == nil || *request.ClientGeneration <= 0 || request.Limit == nil || uint64(*request.Limit) != workload.PageSize || request.ScopeFingerprint == nil || request.RebuildIDFingerprint == nil || request.CursorPresent == nil {
			return fmt.Errorf("rebuild page %d request identity is incomplete", index+1)
		}
		if index == 0 {
			if *request.CursorPresent || request.CursorFingerprint != nil {
				return errors.New("first rebuild page used a continuation cursor")
			}
		} else if !*request.CursorPresent || request.CursorFingerprint == nil {
			return fmt.Errorf("rebuild page %d has no continuation cursor", index+1)
		}
		response := observation.RebuildResponseFacts
		remaining := workload.RecordCount - uint64(index)*workload.PageSize
		wantRecords := workload.PageSize
		if remaining < wantRecords {
			wantRecords = remaining
		}
		if response.RecordCount != int(wantRecords) || response.ScopeFingerprint != *request.ScopeFingerprint {
			return fmt.Errorf("rebuild page %d record or scope count is invalid", index+1)
		}
		terminal := index == pageCount-1
		if terminal {
			if response.HasMore || response.HasCursor || !response.HasFinalScopeCursor || !response.HasChecksum || response.FinalScopeCursorFingerprint == nil {
				return errors.New("terminal rebuild page did not prove finality")
			}
		} else if !response.HasMore || !response.HasCursor || response.HasFinalScopeCursor || response.HasChecksum || response.FinalScopeCursorFingerprint != nil {
			return fmt.Errorf("intermediate rebuild page %d has invalid finality", index+1)
		}
	}
	pull := observations[len(observations)-1]
	if pull.RequestFacts == nil || pull.PullResponseFacts == nil || pull.RequestFacts.ClientGeneration == nil || pull.RequestFacts.ScopeCount == nil || *pull.RequestFacts.ScopeCount != 1 || pull.RequestFacts.ScopeSetVersion == nil || pull.RequestFacts.Limit == nil || uint64(*pull.RequestFacts.Limit) != workload.PageSize || !pull.PullResponseFacts.ScopeCursorFingerprintsComplete || pull.PullResponseFacts.HasMore {
		return errors.New("Kotlin Android rebuild-cardinality final pull facts are incomplete")
	}
	return nil
}

func resolveKotlinRebuildCardinalityIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, calls [][]TransportObservation, actualClient scenarios.StateFacts) (rebuildCardinalityIdentityEvidence, error) {
	if len(aliases) != len(rebuildCardinalityAliasNames) || len(calls) == 0 || len(actualClient.Clients) == 0 {
		return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(rebuildCardinalityAliasNames))
	for _, name := range rebuildCardinalityAliasNames {
		wanted[name] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return rebuildCardinalityIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-cardinality identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality identity aliases are incomplete")
	}

	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return rebuildCardinalityIdentityEvidence{}, err
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	applicationTable := ""
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
		if value.Alias == "items-table" {
			applicationTable = value.ApplicationIdentifier
		}
	}
	if applicationTable == "" {
		return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality table identity is incomplete")
	}

	var generation int64
	for _, call := range calls {
		for _, observation := range call {
			if observation.RequestFacts == nil || observation.RequestFacts.ClientGeneration == nil {
				continue
			}
			if generation == 0 {
				generation = *observation.RequestFacts.ClientGeneration
			} else if generation != *observation.RequestFacts.ClientGeneration {
				return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality client generation changed between fresh clients")
			}
		}
	}
	if generation <= 0 {
		return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality client generation is absent")
	}
	encodedGeneration, err := json.Marshal(generation)
	if err != nil {
		return rebuildCardinalityIdentityEvidence{}, fmt.Errorf("encode Kotlin Android rebuild-cardinality client generation: %w", err)
	}
	runtime["client-generation-one"] = encodedGeneration

	var runtimeSchema schemaRef
	if json.Unmarshal(runtime["current-schema"], &runtimeSchema) != nil || runtimeSchema.Version <= 0 || runtimeSchema.Hash == "" {
		return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality schema identity is invalid")
	}
	var runtimeScope string
	if json.Unmarshal(runtime["scope-a"], &runtimeScope) != nil || runtimeScope == "" {
		return rebuildCardinalityIdentityEvidence{}, errors.New("Kotlin Android rebuild-cardinality scope identity is invalid")
	}
	for _, client := range actualClient.Clients {
		if client.CurrentSchema == nil || int64(client.CurrentSchema.Version) != runtimeSchema.Version || client.CurrentSchema.Hash != runtimeSchema.Hash {
			return rebuildCardinalityIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-cardinality client %s schema identity differs from the server binding", client.ClientID)
		}
		for _, provenance := range client.Provenance {
			if provenance.TableID != applicationTable || len(provenance.Scopes) != 1 || provenance.Scopes[0] != runtimeScope {
				return rebuildCardinalityIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-cardinality client %s provenance identity is invalid", client.ClientID)
			}
		}
		for _, checkpoint := range client.Checkpoints {
			if checkpoint.ScopeID != runtimeScope {
				return rebuildCardinalityIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-cardinality client %s checkpoint identity is invalid", client.ClientID)
			}
		}
	}
	resolutions, err := resolveKotlinNativeIdentities(aliases, runtime)
	if err != nil {
		return rebuildCardinalityIdentityEvidence{}, err
	}
	return rebuildCardinalityIdentityEvidence{
		Resolutions:      resolutions,
		RuntimeSchema:    runtimeSchema,
		RuntimeScope:     runtimeScope,
		ApplicationTable: applicationTable,
		ClientGeneration: generation,
	}, nil
}

func validateKotlinRebuildCardinalityState(expected, server, actualClient scenarios.StateFacts, evidence rebuildCardinalityIdentityEvidence) error {
	serverExpected := scenarios.CloneStateFacts(expected)
	serverExpected.Clients = nil
	if err := validateKotlinStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Kotlin Android rebuild-cardinality server state differs from the authored model: %w", err)
	}
	actual, err := mergeKotlinStateFacts(server, actualClient)
	if err != nil {
		return err
	}
	if err := validateKotlinStateProjection(expected, actual); err != nil {
		return fmt.Errorf("Kotlin Android rebuild-cardinality state differs from the authored model: %w", err)
	}
	if len(evidence.Resolutions) != len(rebuildCardinalityAliasNames) || evidence.ClientGeneration <= 0 || evidence.RuntimeScope == "" || evidence.ApplicationTable == "" {
		return errors.New("Kotlin Android rebuild-cardinality identity evidence is incomplete")
	}
	return nil
}
