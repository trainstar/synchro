package swift

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

// RebuildApplyResult records direct Swift evidence for all authored rebuild samples.
type RebuildApplyResult struct {
	Calls              []SynchronizationResult
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

// RunRebuildApplyScenario executes every authored rebuild workload through Swift.
func RunRebuildApplyScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform) (RebuildApplyResult, error) {
	steps, err := swiftScenarioStepMap(scenario, rebuildApplyScenarioID, 9)
	if err != nil {
		return RebuildApplyResult{}, err
	}
	if controller == nil || platform == nil {
		return RebuildApplyResult{}, errors.New("Swift rebuild-apply dependencies are unavailable")
	}
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-PERF-REBUILD-APPLY-SEMANTIC-001")
	if err != nil {
		return RebuildApplyResult{}, err
	}
	clients, workloads, err := rebuildApplyBindings(scenario, steps)
	if err != nil {
		return RebuildApplyResult{}, err
	}

	modelResult, err := modelrunner.RunScenario(ctx, scenario)
	if err != nil {
		return RebuildApplyResult{}, fmt.Errorf("derive Swift rebuild-apply source operations from the authored model: %w", err)
	}
	if !modelResult.Passed || len(modelResult.Steps) != len(scenario.Steps) {
		return RebuildApplyResult{}, errors.New("authored rebuild-apply model did not close all workload steps")
	}

	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return RebuildApplyResult{}, fmt.Errorf("install Swift rebuild-apply contract: %w", err)
	}

	calls := make([]SynchronizationResult, 0, len(scenario.Steps))
	currentRecordCount := uint64(0)
	for index, authoredStep := range scenario.Steps {
		step := steps[authoredStep.ID]
		modelStep := modelResult.Steps[index]
		if modelStep.StepID != authoredStep.ID {
			return RebuildApplyResult{}, fmt.Errorf("authored rebuild-apply model step %s is bound to %s", authoredStep.ID, modelStep.StepID)
		}
		workload := workloads[authoredStep.ID]
		expansion, err := executeRebuildApplyExpansion(ctx, controller, modelStep.Expanded, step, workload, currentRecordCount)
		if err != nil {
			return RebuildApplyResult{}, fmt.Errorf("execute Swift rebuild-apply source for step %s: %w", authoredStep.ID, err)
		}
		wantPages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
		if expansion.PageCount != wantPages {
			return RebuildApplyResult{}, fmt.Errorf("Swift rebuild-apply step %s has %d rebuild pages, want %d", authoredStep.ID, expansion.PageCount, wantPages)
		}

		client := clients[step.NativeBinding.ClientID]
		if err := platform.Install(ctx, client, "empty", ""); err != nil {
			return RebuildApplyResult{}, fmt.Errorf("install fresh Swift rebuild-apply client %s: %w", client.ClientID, err)
		}
		call, err := swiftScenarioCall(ctx, platform, client, "start")
		if err != nil {
			return RebuildApplyResult{}, fmt.Errorf("run Swift rebuild-apply client %s: %w", client.ClientID, err)
		}
		if err := validateRebuildApplyCall(call, workload); err != nil {
			return RebuildApplyResult{}, fmt.Errorf("validate Swift rebuild-apply client %s: %w", client.ClientID, err)
		}
		calls = append(calls, call)
		currentRecordCount = workload.RecordCount
	}

	clientFacts, err := platform.Capture(ctx, clientsInOrder(clients), []string{
		"application-rows",
		"pending-mutations",
		"rejected-mutations",
		"checkpoints",
		"provenance",
		"rebuild-state",
	})
	if err != nil {
		return RebuildApplyResult{}, fmt.Errorf("capture Swift rebuild-apply client state: %w", err)
	}
	actualClient, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return RebuildApplyResult{}, err
	}

	clientKeys := make([]string, 0, len(clients))
	for _, client := range clientsInOrder(clients) {
		clientKeys = append(clientKeys, client.Key)
	}
	serverCaptures, err := controller.Capture(ctx, clientKeys, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return RebuildApplyResult{}, fmt.Errorf("capture Swift rebuild-apply server state: %w", err)
	}
	serverFacts := serverCaptures[0].StateFacts

	evidence, err := resolveRebuildApplyIdentities(controller, scenario.NativeIdentityAliases, calls, actualClient)
	if err != nil {
		return RebuildApplyResult{}, err
	}
	if err := validateRebuildApplyState(expected, serverFacts, actualClient, evidence); err != nil {
		return RebuildApplyResult{}, err
	}

	return RebuildApplyResult{
		Calls:              calls,
		ClientFacts:        clientFacts,
		ServerFacts:        serverFacts,
		IdentityResolution: evidence.Resolutions,
	}, nil
}

func rebuildApplyBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step) (map[string]Client, map[scenarios.StepID]rebuildApplyWorkload, error) {
	clients := make(map[string]Client, len(steps))
	workloads := make(map[scenarios.StepID]rebuildApplyWorkload, len(steps))
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "workload" || binding.Workload == nil || binding.UserID == "" || binding.ClientID == "" {
			return nil, nil, fmt.Errorf("Swift rebuild-apply step %s workload binding is invalid", step.ID)
		}
		if steps[step.ID].ID != step.ID {
			return nil, nil, fmt.Errorf("Swift rebuild-apply step %s is not bound in the step map", step.ID)
		}
		var workload rebuildApplyWorkload
		if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil || workload.Profile != "scope_cardinality" || workload.ScopeID != "scope-a" || workload.RecordCount == 0 || workload.PageSize == 0 {
			return nil, nil, fmt.Errorf("Swift rebuild-apply step %s workload payload is invalid", step.ID)
		}
		if workload.RecordCount != binding.Workload.RecordCount || workload.PageSize != 100 || len(binding.Workload.Targets) != 1 || binding.Workload.Targets[0].ScopeID != workload.ScopeID || binding.Workload.Targets[0].TableID != "items" || binding.Workload.Targets[0].PrimaryKeyFieldID != "id" {
			return nil, nil, fmt.Errorf("Swift rebuild-apply step %s workload target is invalid", step.ID)
		}
		if len(binding.Workload.MutationKinds) != 1 || binding.Workload.MutationKinds[0].Operation != "insert" || binding.Workload.MutationKinds[0].Count != workload.RecordCount || !reflect.DeepEqual(binding.Workload.MutationKinds[0].FieldIDs, []string{"value"}) {
			return nil, nil, fmt.Errorf("Swift rebuild-apply step %s workload mutation binding is invalid", step.ID)
		}
		if step.ExpectedOutcome.Disposition != "success" {
			return nil, nil, fmt.Errorf("Swift rebuild-apply step %s does not expect success", step.ID)
		}
		if _, duplicate := clients[binding.ClientID]; duplicate {
			return nil, nil, fmt.Errorf("Swift rebuild-apply client %s is not fresh", binding.ClientID)
		}
		clients[binding.ClientID] = Client{
			Key:         "rebuild-apply-" + binding.ClientID,
			UserID:      binding.UserID,
			ClientID:    binding.ClientID,
			DatabaseKey: "rebuild-apply-" + binding.ClientID,
		}
		workloads[step.ID] = workload
	}
	if len(clients) != len(scenario.Steps) || len(workloads) != len(scenario.Steps) {
		return nil, nil, errors.New("Swift rebuild-apply workload bindings are incomplete")
	}
	return clients, workloads, nil
}

func clientsInOrder(clients map[string]Client) []Client {
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

func executeRebuildApplyExpansion(ctx context.Context, controller *blackbox.NativeController, operations []scenarios.Operation, step scenarios.Step, workload rebuildApplyWorkload, priorRecordCount uint64) (rebuildApplyExpansion, error) {
	if len(operations) == 0 {
		return rebuildApplyExpansion{}, errors.New("Swift rebuild-apply workload expansion is empty")
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
			// The fixed Swift fixture already uses the authored single-scope source
			// registration. The model-only membership staging must not mutate it.
		case "model/commit-source-transaction":
			if commitSeen || materializeSeen || beginSeen || finalizeSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion has more than one source commit")
			}
			if err := validateRebuildApplyCommit(operation, priorRecordCount, workload.RecordCount); err != nil {
				return rebuildApplyExpansion{}, err
			}
			observation, err := controller.ApplyStep(ctx, operation)
			if err != nil || observation.Disposition != "success" {
				return rebuildApplyExpansion{}, fmt.Errorf("apply source commit: %w", resultError(err, observation.Disposition))
			}
			commitSeen = true
		case "process/materialize-source-transaction":
			if !commitSeen || materializeSeen || beginSeen {
				return rebuildApplyExpansion{}, errors.New("workload expansion materialization is out of order")
			}
			observation, err := controller.ProcessStep(ctx, nil, operation)
			if err != nil || observation.Disposition != "success" {
				return rebuildApplyExpansion{}, fmt.Errorf("materialize source transaction: %w", resultError(err, observation.Disposition))
			}
			materializeSeen = true
		case "local/begin-rebuild":
			if !materializeSeen || beginSeen || pageCount != 0 {
				return rebuildApplyExpansion{}, errors.New("workload expansion rebuild begin is out of order")
			}
			payload, err := decodeRebuildApplyControl(operation)
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
			payload, err := decodeRebuildApplyControl(operation)
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
			payload, err := decodeRebuildApplyControl(operation)
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
			payload, err := decodeRebuildApplyControl(operation)
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

func validateRebuildApplyCommit(operation scenarios.Operation, priorRecordCount, recordCount uint64) error {
	var payload rebuildApplyCommitPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Events) == 0 {
		return errors.New("rebuild-apply source commit payload is invalid")
	}
	wantEvents := uint64(1)
	wantOperation := "update"
	if recordCount > priorRecordCount {
		wantEvents = recordCount - priorRecordCount
		wantOperation = "insert"
	}
	if uint64(len(payload.Events)) != wantEvents {
		return fmt.Errorf("rebuild-apply source event count = %d, want %d", len(payload.Events), wantEvents)
	}
	for _, event := range payload.Events {
		if event.Operation != wantOperation {
			return fmt.Errorf("rebuild-apply source event operation = %q, want %q", event.Operation, wantOperation)
		}
	}
	return nil
}

func decodeRebuildApplyControl(operation scenarios.Operation) (rebuildApplyControlPayload, error) {
	var payload rebuildApplyControlPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return rebuildApplyControlPayload{}, fmt.Errorf("decode rebuild-apply %s payload: %w", scenarios.OperationKey(operation), err)
	}
	return payload, nil
}

func validateRebuildApplyCall(call SynchronizationResult, workload rebuildApplyWorkload) error {
	if call.Completion != "idle" {
		return fmt.Errorf("completion = %q, want idle", call.Completion)
	}
	pageCount := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	observations := call.transportObservations
	if len(observations) != pageCount+2 {
		return fmt.Errorf("transport observation count = %d, want %d", len(observations), pageCount+2)
	}
	if observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return errors.New("rebuild-apply call does not start with connect and end with pull")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable || observation.ErrorCode != nil {
			return errors.New("rebuild-apply call contains an unsuccessful transport response")
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
	if pull.RequestFacts == nil || pull.PullResponseFacts == nil || pull.RequestFacts.ClientGeneration == nil || pull.RequestFacts.ScopeCount == nil || *pull.RequestFacts.ScopeCount != 1 || pull.RequestFacts.ScopeSetVersion == nil || !pull.PullResponseFacts.ScopeCursorFingerprintsComplete || pull.PullResponseFacts.HasMore {
		return errors.New("rebuild-apply final pull facts are incomplete")
	}
	return nil
}

func resolveRebuildApplyIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, calls []SynchronizationResult, actualClient scenarios.StateFacts) (rebuildApplyIdentityEvidence, error) {
	if len(aliases) != len(rebuildApplyAliasNames) || len(calls) == 0 || len(actualClient.Clients) == 0 {
		return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(rebuildApplyAliasNames))
	for _, name := range rebuildApplyAliasNames {
		wanted[name] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return rebuildApplyIdentityEvidence{}, fmt.Errorf("Swift rebuild-apply identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply identity aliases are incomplete")
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
		return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply table identity is incomplete")
	}

	var generation int64
	for _, call := range calls {
		for _, observation := range call.transportObservations {
			if observation.RequestFacts == nil || observation.RequestFacts.ClientGeneration == nil {
				continue
			}
			if generation == 0 {
				generation = *observation.RequestFacts.ClientGeneration
			} else if generation != *observation.RequestFacts.ClientGeneration {
				return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply client generation changed between fresh clients")
			}
		}
	}
	if generation <= 0 {
		return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply client generation is absent")
	}
	encodedGeneration, err := json.Marshal(generation)
	if err != nil {
		return rebuildApplyIdentityEvidence{}, fmt.Errorf("encode Swift rebuild-apply client generation: %w", err)
	}
	runtime["client-generation-one"] = encodedGeneration

	var runtimeSchema schemaRef
	if json.Unmarshal(runtime["current-schema"], &runtimeSchema) != nil || runtimeSchema.Version <= 0 || runtimeSchema.Hash == "" {
		return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply schema identity is invalid")
	}
	var runtimeScope string
	if json.Unmarshal(runtime["scope-a"], &runtimeScope) != nil || runtimeScope == "" {
		return rebuildApplyIdentityEvidence{}, errors.New("Swift rebuild-apply scope identity is invalid")
	}
	for _, client := range actualClient.Clients {
		if client.CurrentSchema == nil || int64(client.CurrentSchema.Version) != runtimeSchema.Version || client.CurrentSchema.Hash != runtimeSchema.Hash {
			return rebuildApplyIdentityEvidence{}, fmt.Errorf("Swift rebuild-apply client %s schema identity differs from the server binding", client.ClientID)
		}
		for _, provenance := range client.Provenance {
			if provenance.TableID != applicationTable || len(provenance.Scopes) != 1 || provenance.Scopes[0] != runtimeScope {
				return rebuildApplyIdentityEvidence{}, fmt.Errorf("Swift rebuild-apply client %s provenance identity is invalid", client.ClientID)
			}
		}
		for _, checkpoint := range client.Checkpoints {
			if checkpoint.ScopeID != runtimeScope {
				return rebuildApplyIdentityEvidence{}, fmt.Errorf("Swift rebuild-apply client %s checkpoint identity is invalid", client.ClientID)
			}
		}
	}
	resolutions, err := resolveSwiftNativeIdentities(aliases, runtime)
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

func validateRebuildApplyState(expected, server, actualClient scenarios.StateFacts, evidence rebuildApplyIdentityEvidence) error {
	serverExpected := scenarios.CloneStateFacts(expected)
	serverExpected.Clients = nil
	if err := validateSwiftStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Swift rebuild-apply server state differs from the authored model: %w", err)
	}
	actual, err := mergeSwiftStateFacts(server, actualClient)
	if err != nil {
		return err
	}
	if err := validateSwiftStateProjection(expected, actual); err != nil {
		return fmt.Errorf("Swift rebuild-apply state differs from the authored model: %w", err)
	}
	if len(evidence.Resolutions) != len(rebuildApplyAliasNames) || evidence.ClientGeneration <= 0 || evidence.RuntimeScope == "" || evidence.ApplicationTable == "" {
		return errors.New("Swift rebuild-apply identity evidence is incomplete")
	}
	return nil
}
