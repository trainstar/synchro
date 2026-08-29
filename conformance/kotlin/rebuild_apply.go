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

const rebuildApplyScenarioID = "SCN-PERF-REBUILD-APPLY-001"

var rebuildApplyAliasNames = []string{
	"client-generation-one",
	"current-schema",
	"scope-a",
	"items-table",
}

// RebuildApplyResult records direct Kotlin Android evidence for all authored rebuild samples.
type RebuildApplyResult struct {
	Calls              []ClientCallResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type rebuildApplyWorkload struct {
	Profile     string `json:"profile"`
	ScopeID     string `json:"scope_id"`
	RecordCount uint64 `json:"record_count"`
	PageSize    uint64 `json:"page_size"`
}

type rebuildApplyExpansion struct {
	PageCount int
}

type rebuildApplyControlPayload struct {
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

type rebuildApplyCommitPayload struct {
	Events []struct {
		Operation string `json:"operation"`
	} `json:"events"`
}

type rebuildApplyIdentityEvidence struct {
	Resolutions      []blackbox.NativeIdentityResolution
	RuntimeSchema    schemaRef
	RuntimeScope     string
	ApplicationTable string
	ClientGeneration int64
}

// RunRebuildApplyScenario executes every authored rebuild workload through Kotlin Android.
func RunRebuildApplyScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform) (RebuildApplyResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, rebuildApplyScenarioID, 9)
	if err != nil {
		return RebuildApplyResult{}, err
	}
	if controller == nil || platform == nil {
		return RebuildApplyResult{}, errors.New("Kotlin Android rebuild-apply dependencies are unavailable")
	}
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-PERF-REBUILD-APPLY-SEMANTIC-001")
	if err != nil {
		return RebuildApplyResult{}, err
	}
	clients, workloads, _, err := rebuildApplyBindings(scenario, steps)
	if err != nil {
		return RebuildApplyResult{}, err
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
		return RebuildApplyResult{}, fmt.Errorf("derive Kotlin Android rebuild-apply source operations from the authored model: %w", err)
	}
	if !modelResult.Passed || len(modelResult.Steps) != len(scenario.Steps) {
		return RebuildApplyResult{}, errors.New("authored rebuild-apply model did not close all workload steps")
	}

	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return RebuildApplyResult{}, fmt.Errorf("install Kotlin Android rebuild-apply contract: %w", err)
	}

	calls := make([]ClientCallResult, 0, len(scenario.Steps))
	callEvidence := make([][]TransportObservation, 0, len(scenario.Steps))
	clientFacts := make([]CaptureFacts, 0, len(scenario.Steps)*len(captureSources))
	currentRecordCount := uint64(0)
	for index, authoredStep := range scenario.Steps {
		step := steps[authoredStep.ID]
		modelStep := modelResult.Steps[index]
		if modelStep.StepID != authoredStep.ID {
			return RebuildApplyResult{}, fmt.Errorf("authored rebuild-apply model step %s is bound to %s", authoredStep.ID, modelStep.StepID)
		}
		workload := workloads[authoredStep.ID]
		expansion, err := executeKotlinRebuildApplyExpansion(ctx, controller, modelStep.Expanded, step, workload, currentRecordCount)
		if err != nil {
			return RebuildApplyResult{}, fmt.Errorf("execute Kotlin Android rebuild-apply source for step %s: %w", authoredStep.ID, err)
		}
		wantPages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
		if expansion.PageCount != wantPages {
			return RebuildApplyResult{}, fmt.Errorf("Kotlin Android rebuild-apply step %s has %d rebuild pages, want %d", authoredStep.ID, expansion.PageCount, wantPages)
		}

		client := clients[step.NativeBinding.ClientID]
		if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "empty"}); err != nil {
			return RebuildApplyResult{}, fmt.Errorf("install fresh Kotlin Android rebuild-apply client %s: %w", client.ClientID, err)
		}
		scopeSetVersion, err := rebuildApplyScopeSetVersion(scenario, client, workload.ScopeID)
		if err != nil {
			return RebuildApplyResult{}, err
		}
		call, transport, err := runKotlinRebuildApplyCall(ctx, platform, client, modelStep.Expanded, workload, scopeSetVersion)
		if err != nil {
			return RebuildApplyResult{}, fmt.Errorf("run Kotlin Android rebuild-apply client %s: %w", client.ClientID, err)
		}
		if err := validateKotlinRebuildApplyCall(call, transport, workload); err != nil {
			return RebuildApplyResult{}, fmt.Errorf("validate Kotlin Android rebuild-apply client %s: %w", client.ClientID, err)
		}
		capturedFacts, err := platform.Capture(ctx, []Client{client}, captureSources)
		if err != nil {
			return RebuildApplyResult{}, fmt.Errorf("capture Kotlin Android rebuild-apply client %s state: %w", client.ClientID, err)
		}
		if err := closeKotlinRebuildApplyClient(ctx, platform, client); err != nil {
			return RebuildApplyResult{}, fmt.Errorf("release Kotlin Android rebuild-apply client %s: %w", client.ClientID, err)
		}
		calls = append(calls, call)
		callEvidence = append(callEvidence, transport)
		clientFacts = append(clientFacts, capturedFacts...)
		currentRecordCount = workload.RecordCount
	}

	actualClient, err := mergeKotlinCaptureFacts(clientFacts)
	if err != nil {
		return RebuildApplyResult{}, err
	}

	clientKeys := make([]string, 0, len(clients))
	for _, client := range rebuildApplyClientsInOrder(clients) {
		clientKeys = append(clientKeys, client.Key)
	}
	serverCaptures, err := controller.Capture(ctx, clientKeys, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return RebuildApplyResult{}, fmt.Errorf("capture Kotlin Android rebuild-apply server state: %w", kotlinResultError(err, ""))
	}
	serverFacts := serverCaptures[0].StateFacts

	evidence, err := resolveKotlinRebuildApplyIdentities(controller, scenario.NativeIdentityAliases, callEvidence, actualClient)
	if err != nil {
		return RebuildApplyResult{}, err
	}
	if err := validateKotlinRebuildApplyState(expected, serverFacts, actualClient, evidence); err != nil {
		return RebuildApplyResult{}, err
	}

	return RebuildApplyResult{
		Calls:              calls,
		ClientFacts:        clientFacts,
		ServerFacts:        serverFacts,
		IdentityResolution: evidence.Resolutions,
	}, nil
}

func rebuildApplyBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step) (map[string]Client, map[scenarios.StepID]rebuildApplyWorkload, uint64, error) {
	clients := make(map[string]Client, len(steps))
	workloads := make(map[scenarios.StepID]rebuildApplyWorkload, len(steps))
	pageSize := uint64(0)
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "workload" || binding.Workload == nil || binding.UserID == "" || binding.ClientID == "" {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply step %s workload binding is invalid", step.ID)
		}
		if steps[step.ID].ID != step.ID {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply step %s is not bound in the step map", step.ID)
		}
		var workload rebuildApplyWorkload
		if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil || workload.Profile != "scope_cardinality" || workload.ScopeID != "scope-a" || workload.RecordCount == 0 || workload.PageSize == 0 {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply step %s workload payload is invalid", step.ID)
		}
		if workload.RecordCount != binding.Workload.RecordCount || len(binding.Workload.Targets) != 1 || binding.Workload.Targets[0].ScopeID != workload.ScopeID || binding.Workload.Targets[0].TableID != "items" || binding.Workload.Targets[0].PrimaryKeyFieldID != "id" {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply step %s workload target is invalid", step.ID)
		}
		if len(binding.Workload.MutationKinds) != 1 || binding.Workload.MutationKinds[0].Operation != "insert" || binding.Workload.MutationKinds[0].Count != workload.RecordCount || !reflect.DeepEqual(binding.Workload.MutationKinds[0].FieldIDs, []string{"value"}) {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply step %s workload mutation binding is invalid", step.ID)
		}
		if step.ExpectedOutcome.Disposition != "success" {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply step %s does not expect success", step.ID)
		}
		if pageSize == 0 {
			pageSize = workload.PageSize
		} else if pageSize != workload.PageSize {
			return nil, nil, 0, errors.New("Kotlin Android rebuild-apply workload page sizes differ")
		}
		if _, duplicate := clients[binding.ClientID]; duplicate {
			return nil, nil, 0, fmt.Errorf("Kotlin Android rebuild-apply client %s is not fresh", binding.ClientID)
		}
		clients[binding.ClientID] = Client{
			Key:         "rebuild-apply-" + binding.ClientID,
			UserID:      binding.UserID,
			ClientID:    binding.ClientID,
			DatabaseKey: "rebuild-apply-" + binding.ClientID,
		}
		workloads[step.ID] = workload
	}
	if len(clients) != len(scenario.Steps) || len(workloads) != len(scenario.Steps) || pageSize == 0 {
		return nil, nil, 0, errors.New("Kotlin Android rebuild-apply workload bindings are incomplete")
	}
	return clients, workloads, pageSize, nil
}

func rebuildApplyClientsInOrder(clients map[string]Client) []Client {
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

func closeKotlinRebuildApplyClient(ctx context.Context, platform *Platform, client Client) error {
	state, err := platform.clientFor(client)
	if err != nil {
		return err
	}
	state.mu.Lock()
	if state.terminated || state.session == nil || state.activeCall != nil {
		state.mu.Unlock()
		return errors.New("Kotlin Android rebuild-apply client is unavailable for release")
	}
	session := state.session
	state.session = nil
	state.terminated = true
	state.started = false
	state.mu.Unlock()
	return session.Close(ctx)
}

func rebuildApplyScopeSetVersion(scenario scenarios.Scenario, client Client, scopeID string) (int64, error) {
	var payload struct {
		Clients []struct {
			UserID           string   `json:"user_id"`
			ClientID         string   `json:"client_id"`
			ScopeSetVersion  int64    `json:"scope_set_version"`
			AssignedScopeIDs []string `json:"assigned_scope_ids"`
		} `json:"clients"`
	}
	if len(scenario.Model.Setup) != 1 || json.Unmarshal(scenario.Model.Setup[0].Payload, &payload) != nil {
		return 0, errors.New("Kotlin Android rebuild-apply authored client setup is invalid")
	}
	for _, candidate := range payload.Clients {
		if candidate.UserID != client.UserID || candidate.ClientID != client.ClientID {
			continue
		}
		if candidate.ScopeSetVersion <= 0 || len(candidate.AssignedScopeIDs) != 1 || candidate.AssignedScopeIDs[0] != scopeID {
			return 0, errors.New("Kotlin Android rebuild-apply authored client scope setup is invalid")
		}
		return candidate.ScopeSetVersion, nil
	}
	return 0, fmt.Errorf("Kotlin Android rebuild-apply client %s is absent from the authored setup", client.ClientID)
}

func executeKotlinRebuildApplyExpansion(ctx context.Context, controller *blackbox.NativeController, operations []scenarios.Operation, step scenarios.Step, workload rebuildApplyWorkload, priorRecordCount uint64) (rebuildApplyExpansion, error) {
	if len(operations) == 0 {
		return rebuildApplyExpansion{}, errors.New("Kotlin Android rebuild-apply workload expansion is empty")
	}
	if step.NativeBinding == nil {
		return rebuildApplyExpansion{}, errors.New("Kotlin Android rebuild-apply workload binding is absent")
	}
	pageCount := 0
	commitSeen := false
	materializeSeen := false
	beginSeen := false
	requestSeen := false
	applySeen := false
	finalizeSeen := false
	currentRebuildID := ""
	for _, operation := range operations {
		key := scenarios.OperationKey(operation)
		switch key {
		case "model/stage-registry-membership-generation", "model/activate-registry-membership-generation":
			if commitSeen || materializeSeen || beginSeen {
				return rebuildApplyExpansion{}, fmt.Errorf("workload expansion places %s after source execution", key)
			}
			// The Kotlin fixture already uses the authored single-scope source registration.
			// The model-only membership staging must not mutate it.
		case "model/commit-source-transaction":
			if commitSeen || materializeSeen || beginSeen || finalizeSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion has more than one source commit")
			}
			if err := validateKotlinRebuildApplyCommit(operation, priorRecordCount, workload.RecordCount); err != nil {
				return rebuildApplyExpansion{}, err
			}
			observation, err := controller.ApplyStep(ctx, operation)
			if err != nil || observation.Disposition != "success" {
				return rebuildApplyExpansion{}, fmt.Errorf("apply source commit: %w", kotlinResultError(err, observation.Disposition))
			}
			commitSeen = true
		case "process/materialize-source-transaction":
			if !commitSeen || materializeSeen || beginSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion materialization is out of order")
			}
			observation, err := controller.ProcessStep(ctx, nil, operation)
			if err != nil || observation.Disposition != "success" {
				return rebuildApplyExpansion{}, fmt.Errorf("materialize source transaction: %w", kotlinResultError(err, observation.Disposition))
			}
			materializeSeen = true
		case "local/begin-rebuild":
			if !materializeSeen || beginSeen || pageCount != 0 {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild begin is out of order")
			}
			payload, err := decodeKotlinRebuildApplyControl(operation)
			if err != nil {
				return rebuildApplyExpansion{}, err
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.Limit != workload.PageSize || payload.RebuildID == "" || payload.Schema.Version != int64(step.NativeBinding.Workload.AuthoredSchema.Version) || payload.Schema.Hash != step.NativeBinding.Workload.AuthoredSchema.Hash {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild begin binding is invalid")
			}
			currentRebuildID = payload.RebuildID
			beginSeen = true
		case "rebuild/request-page":
			if !beginSeen || finalizeSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild request is out of order")
			}
			payload, err := decodeKotlinRebuildApplyControl(operation)
			if err != nil {
				return rebuildApplyExpansion{}, err
			}
			wantCursorSource := "none"
			if pageCount > 0 {
				wantCursorSource = "local_rebuild_continuation"
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.Limit != workload.PageSize || payload.RebuildID != currentRebuildID || payload.CursorSource != wantCursorSource {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild request binding is invalid")
			}
			if applySeen {
				applySeen = false
			}
			requestSeen = true
		case "local/apply-rebuild-page":
			if !beginSeen || !requestSeen || finalizeSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild apply is out of order")
			}
			payload, err := decodeKotlinRebuildApplyControl(operation)
			if err != nil {
				return rebuildApplyExpansion{}, err
			}
			wantOrdinal := uint64(pageCount)*workload.PageSize + 1
			wantTokenSource := "none"
			if pageCount > 0 {
				wantTokenSource = "local_rebuild_continuation"
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.RebuildID != currentRebuildID || payload.PageOrdinal != wantOrdinal || payload.RequestTokenSource != wantTokenSource {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild apply binding is invalid")
			}
			pageCount++
			requestSeen = false
			applySeen = true
		case "local/finalize-rebuild":
			if !beginSeen || !applySeen || requestSeen || finalizeSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild finalize is out of order")
			}
			payload, err := decodeKotlinRebuildApplyControl(operation)
			if err != nil {
				return rebuildApplyExpansion{}, err
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID || payload.ScopeID != workload.ScopeID || payload.RebuildID != currentRebuildID {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild finalize binding is invalid")
			}
			finalizeSeen = true
		default:
			return rebuildApplyExpansion{}, fmt.Errorf("workload expansion operation %q is unsupported", key)
		}
	}
	if !commitSeen || !materializeSeen || !beginSeen || !applySeen || !finalizeSeen || requestSeen || pageCount == 0 {
		return rebuildApplyExpansion{}, errors.New("workload expansion did not close source and rebuild phases")
	}
	return rebuildApplyExpansion{PageCount: pageCount}, nil
}

func runKotlinRebuildApplyCall(ctx context.Context, platform *Platform, client Client, operations []scenarios.Operation, workload rebuildApplyWorkload, scopeSetVersion int64) (ClientCallResult, []TransportObservation, error) {
	rebuilds := make([]scenarios.Operation, 0)
	for _, operation := range operations {
		if scenarios.OperationKey(operation) == "rebuild/request-page" {
			rebuilds = append(rebuilds, operation)
		}
	}
	wantPages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	if len(rebuilds) != wantPages || scopeSetVersion <= 0 {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-apply staged workload is invalid")
	}
	state, err := platform.clientFor(client)
	if err != nil {
		return ClientCallResult{}, nil, err
	}
	state.mu.Lock()
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()

	callID := "rebuild_apply_" + client.ClientID
	begin, err := platform.BeginCall(ctx, CallRequest{Client: client, CallID: callID, Method: "start", Operations: []scenarios.Operation{rebuilds[0]}})
	if err != nil {
		return ClientCallResult{}, nil, err
	}
	if begin.CallID != callID || begin.State != "in_flight" || begin.Completion != "" || len(begin.Steps) != 1 || begin.Steps[0].Disposition != "success" || begin.Steps[0].Wire == nil || begin.Steps[0].Wire.HTTPStatus != 200 || begin.Steps[0].Wire.ErrorCode != nil || begin.Steps[0].Wire.Retryable {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-apply first page did not enter the staged call")
	}
	for index := 1; index < len(rebuilds); index++ {
		step, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: rebuilds[index]})
		if err != nil {
			return ClientCallResult{}, nil, fmt.Errorf("await Kotlin Android rebuild-apply page %d: %w", index+1, err)
		}
		if step.Disposition != "success" || step.Wire == nil || step.Wire.HTTPStatus != 200 || step.Wire.ErrorCode != nil || step.Wire.Retryable {
			return ClientCallResult{}, nil, fmt.Errorf("Kotlin Android rebuild-apply page %d did not succeed", index+1)
		}
	}
	pull, err := rebuildApplyPullOperation(rebuilds[0], scopeSetVersion, workload.PageSize)
	if err != nil {
		return ClientCallResult{}, nil, err
	}
	pullStep, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: pull})
	if err != nil {
		return ClientCallResult{}, nil, fmt.Errorf("await Kotlin Android rebuild-apply final pull: %w", err)
	}
	if pullStep.Disposition != "success" || pullStep.Wire == nil || pullStep.Wire.HTTPStatus != 200 || pullStep.Wire.ErrorCode != nil || pullStep.Wire.Retryable {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-apply final pull did not succeed")
	}
	completed, err := platform.AwaitCall(ctx, CallRequest{Client: client, CallID: callID})
	if err != nil {
		return ClientCallResult{}, nil, fmt.Errorf("complete Kotlin Android rebuild-apply call: %w", err)
	}
	if completed.CallID != callID || completed.State != "completed" || completed.Completion != "idle" {
		return ClientCallResult{}, nil, errors.New("Kotlin Android rebuild-apply call did not complete idle")
	}
	state.mu.Lock()
	transport, transportErr := state.session.ObservationsAfter(transportCheckpoint)
	state.mu.Unlock()
	if transportErr != nil {
		return ClientCallResult{}, nil, fmt.Errorf("capture Kotlin Android rebuild-apply transport: %w", transportErr)
	}
	return completed, transport, nil
}

func rebuildApplyPullOperation(firstRebuild scenarios.Operation, scopeSetVersion int64, pageSize uint64) (scenarios.Operation, error) {
	control, err := decodeKotlinRebuildApplyControl(firstRebuild)
	if err != nil || control.UserID == "" || control.ClientID == "" || control.ClientGeneration <= 0 || control.Schema.Version <= 0 || control.Schema.Hash == "" || control.ScopeID == "" || scopeSetVersion <= 0 || pageSize == 0 {
		return scenarios.Operation{}, errors.New("Kotlin Android rebuild-apply final pull binding is invalid")
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
		return scenarios.Operation{}, fmt.Errorf("encode Kotlin Android rebuild-apply final pull: %w", err)
	}
	operation := scenarios.Operation{ContractOperation: "pull", Name: "request-page", Payload: payload}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, fmt.Errorf("validate Kotlin Android rebuild-apply final pull: %w", err)
	}
	return operation, nil
}

func validateKotlinRebuildApplyCommit(operation scenarios.Operation, priorRecordCount, recordCount uint64) error {
	var payload rebuildApplyCommitPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Events) == 0 {
		return errors.New("Kotlin Android rebuild-apply source commit payload is invalid")
	}
	wantEvents := uint64(1)
	wantOperation := "update"
	if recordCount > priorRecordCount {
		wantEvents = recordCount - priorRecordCount
		wantOperation = "insert"
	}
	if uint64(len(payload.Events)) != wantEvents {
		return fmt.Errorf("Kotlin Android rebuild-apply source event count = %d, want %d", len(payload.Events), wantEvents)
	}
	for _, event := range payload.Events {
		if event.Operation != wantOperation {
			return fmt.Errorf("Kotlin Android rebuild-apply source event operation = %q, want %q", event.Operation, wantOperation)
		}
	}
	return nil
}

func decodeKotlinRebuildApplyControl(operation scenarios.Operation) (rebuildApplyControlPayload, error) {
	var payload rebuildApplyControlPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return rebuildApplyControlPayload{}, fmt.Errorf("decode Kotlin Android rebuild-apply %s payload: %w", scenarios.OperationKey(operation), err)
	}
	return payload, nil
}

func validateKotlinRebuildApplyCall(call ClientCallResult, observations []TransportObservation, workload rebuildApplyWorkload) error {
	if call.State != "completed" || call.Completion != "idle" {
		return fmt.Errorf("completion = %q, want idle", call.Completion)
	}
	pageCount := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	if len(observations) != pageCount+2 {
		return fmt.Errorf("transport observation count = %d, want %d", len(observations), pageCount+2)
	}
	if observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return errors.New("Kotlin Android rebuild-apply call does not start with connect and end with pull")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable == nil || *observation.Retryable || observation.ErrorCode != nil {
			return errors.New("Kotlin Android rebuild-apply call contains an unsuccessful transport response")
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
		return errors.New("Kotlin Android rebuild-apply final pull facts are incomplete")
	}
	return nil
}

func resolveKotlinRebuildApplyIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, calls [][]TransportObservation, actualClient scenarios.StateFacts) (rebuildApplyIdentityEvidence, error) {
	if len(aliases) != len(rebuildApplyAliasNames) || len(calls) == 0 || len(actualClient.Clients) == 0 {
		return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(rebuildApplyAliasNames))
	for _, name := range rebuildApplyAliasNames {
		wanted[name] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return rebuildApplyIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-apply identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply identity aliases are incomplete")
	}

	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return rebuildApplyIdentityEvidence{}, err
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
		return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply table identity is incomplete")
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
				return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply client generation changed between fresh clients")
			}
		}
	}
	if generation <= 0 {
		return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply client generation is absent")
	}
	encodedGeneration, err := json.Marshal(generation)
	if err != nil {
		return rebuildApplyIdentityEvidence{}, fmt.Errorf("encode Kotlin Android rebuild-apply client generation: %w", err)
	}
	runtime["client-generation-one"] = encodedGeneration

	var runtimeSchema schemaRef
	if json.Unmarshal(runtime["current-schema"], &runtimeSchema) != nil || runtimeSchema.Version <= 0 || runtimeSchema.Hash == "" {
		return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply schema identity is invalid")
	}
	var runtimeScope string
	if json.Unmarshal(runtime["scope-a"], &runtimeScope) != nil || runtimeScope == "" {
		return rebuildApplyIdentityEvidence{}, errors.New("Kotlin Android rebuild-apply scope identity is invalid")
	}
	for _, client := range actualClient.Clients {
		if client.CurrentSchema == nil || int64(client.CurrentSchema.Version) != runtimeSchema.Version || client.CurrentSchema.Hash != runtimeSchema.Hash {
			return rebuildApplyIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-apply client %s schema identity differs from the server binding", client.ClientID)
		}
		for _, provenance := range client.Provenance {
			if provenance.TableID != applicationTable || len(provenance.Scopes) != 1 || provenance.Scopes[0] != runtimeScope {
				return rebuildApplyIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-apply client %s provenance identity is invalid", client.ClientID)
			}
		}
		for _, checkpoint := range client.Checkpoints {
			if checkpoint.ScopeID != runtimeScope {
				return rebuildApplyIdentityEvidence{}, fmt.Errorf("Kotlin Android rebuild-apply client %s checkpoint identity is invalid", client.ClientID)
			}
		}
	}
	resolutions, err := resolveKotlinNativeIdentities(aliases, runtime)
	if err != nil {
		return rebuildApplyIdentityEvidence{}, err
	}
	return rebuildApplyIdentityEvidence{
		Resolutions:      resolutions,
		RuntimeSchema:    runtimeSchema,
		RuntimeScope:     runtimeScope,
		ApplicationTable: applicationTable,
		ClientGeneration: generation,
	}, nil
}

func validateKotlinRebuildApplyState(expected, server, actualClient scenarios.StateFacts, evidence rebuildApplyIdentityEvidence) error {
	serverExpected := scenarios.CloneStateFacts(expected)
	serverExpected.Clients = nil
	if err := validateKotlinStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Kotlin Android rebuild-apply server state differs from the authored model: %w", err)
	}
	actual, err := mergeKotlinStateFacts(server, actualClient)
	if err != nil {
		return err
	}
	if err := validateKotlinStateProjection(expected, actual); err != nil {
		return fmt.Errorf("Kotlin Android rebuild-apply state differs from the authored model: %w", err)
	}
	if len(evidence.Resolutions) != len(rebuildApplyAliasNames) || evidence.ClientGeneration <= 0 || evidence.RuntimeScope == "" || evidence.ApplicationTable == "" {
		return errors.New("Kotlin Android rebuild-apply identity evidence is incomplete")
	}
	return nil
}
