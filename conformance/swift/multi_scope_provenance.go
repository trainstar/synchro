package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const multiScopeProvenanceScenarioID = "SCN-PERF-MULTI-SCOPE-PROVENANCE-001"

// MultiScopeProvenanceResult records direct Swift evidence for the authored
// multi-scope provenance scenario.
type MultiScopeProvenanceResult struct {
	Calls              []SynchronizationResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type multiScopeProvenancePlan struct {
	Expected          scenarios.StateFacts
	Steps             []scenarios.Step
	Calls             map[scenarios.StepID]*multiScopeProvenanceCall
	CallOrder         []scenarios.StepID
	Clients           map[string]Client
	TransactionCount  uint64
	SyncedEventCount  uint64
	CaptureEventCount uint64
}

type multiScopeProvenanceCall struct {
	Step               scenarios.Step
	Client             Client
	KnownScopeCount    uint64
	MeasuredScopeCount uint64
	Rebuilds           []*multiScopeProvenanceRebuild
}

type multiScopeProvenanceRebuild struct {
	ScopeID   string
	RebuildID string
	Limit     uint64
	Begin     scenarios.Step
	Request   scenarios.Step
	Apply     scenarios.Step
	Finalize  scenarios.Step
}

type multiScopeProvenanceCommitPayload struct {
	StreamGeneration string `json:"stream_generation"`
	CommitLSN        string `json:"commit_lsn"`
	EndLSN           string `json:"end_lsn"`
	Events           []struct {
		Relation  string `json:"relation"`
		Operation string `json:"operation"`
		After     *struct {
			Identity struct {
				Kind      string `json:"kind"`
				SyncedRow *struct {
					CanonicalWireJSON string `json:"canonical_wire_json"`
				} `json:"synced_row"`
			} `json:"identity"`
		} `json:"after"`
	} `json:"events"`
}

type multiScopeProvenanceMaterializePayload struct {
	StreamGeneration string `json:"stream_generation"`
	CommitLSN        string `json:"commit_lsn"`
}

type multiScopeProvenanceStagePayload struct {
	RegistryGeneration uint64   `json:"registry_generation"`
	BatchSize          uint64   `json:"batch_size"`
	AffectedScopes     []string `json:"affected_scopes"`
	ScopeRules         []struct {
		Relation    string `json:"relation"`
		Evaluations []struct {
			Scopes []string `json:"scopes"`
		} `json:"evaluations"`
	} `json:"scope_rules"`
}

type multiScopeProvenanceActivatePayload struct {
	RegistryGeneration uint64 `json:"registry_generation"`
}

type multiScopeProvenanceAssignmentPayload struct {
	UserID      string `json:"user_id"`
	ClientID    string `json:"client_id"`
	Assignments []struct {
		ScopeID string `json:"scope_id"`
	} `json:"assignments"`
}

type multiScopeProvenanceConnectPayload struct {
	UserID      string `json:"user_id"`
	ClientID    string `json:"client_id"`
	KnownScopes []struct {
		ScopeID string `json:"scope_id"`
	} `json:"known_scopes"`
}

type multiScopeProvenanceRebuildPayload struct {
	UserID             string `json:"user_id"`
	ClientID           string `json:"client_id"`
	ScopeID            string `json:"scope_id"`
	RebuildID          string `json:"rebuild_id"`
	Limit              uint64 `json:"limit"`
	CursorSource       string `json:"cursor_source"`
	PageOrdinal        uint64 `json:"page_ordinal"`
	RequestTokenSource string `json:"request_token_source"`
}

type multiScopeProvenanceMeasurementPayload struct {
	Parameters map[string]uint64 `json:"parameters"`
}

type multiScopeProvenanceIdentityEvidence struct {
	Resolutions []blackbox.NativeIdentityResolution
}

// RunMultiScopeProvenanceScenario executes the authored multi-scope provenance flow through Swift.
func RunMultiScopeProvenanceScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform) (MultiScopeProvenanceResult, error) {
	if controller == nil || platform == nil {
		return MultiScopeProvenanceResult{}, errors.New("Swift multi-scope provenance dependencies are unavailable")
	}
	plan, err := multiScopeProvenancePlanForScenario(scenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("install Swift multi-scope provenance contract: %w", err)
	}

	installed := make(map[string]bool, len(plan.Clients))
	calls := make([]SynchronizationResult, 0, len(plan.CallOrder))
	for _, step := range plan.Steps {
		switch scenarios.OperationKey(step.Operation) {
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/set-client-assignments":
			observation, applyErr := controller.ApplyStep(ctx, step.Operation)
			if applyErr != nil || observation.Disposition != "success" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("apply Swift multi-scope provenance step %s: %w", step.ID, resultError(applyErr, observation.Disposition))
			}
		case "process/materialize-source-transaction":
			observation, processErr := controller.ProcessStep(ctx, nil, step.Operation)
			if processErr != nil || observation.Disposition != "success" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("materialize Swift multi-scope provenance step %s: %w", step.ID, resultError(processErr, observation.Disposition))
			}
		case "connect/send":
			call := plan.Calls[step.ID]
			if call == nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance call %s is absent", step.ID)
			}
			method := "sync-now"
			if !installed[call.Client.Key] {
				if installErr := platform.Install(ctx, call.Client, "empty", ""); installErr != nil {
					return MultiScopeProvenanceResult{}, fmt.Errorf("install Swift multi-scope provenance client %s: %w", call.Client.ClientID, installErr)
				}
				installed[call.Client.Key] = true
				method = "start"
			}
			result, callErr := swiftScenarioCall(ctx, platform, call.Client, method)
			if callErr != nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("run Swift multi-scope provenance client %s: %w", call.Client.ClientID, callErr)
			}
			if err := validateMultiScopeProvenanceCall(scenario, call, result); err != nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("validate Swift multi-scope provenance client %s: %w", call.Client.ClientID, err)
			}
			calls = append(calls, result)
		case "local/begin-rebuild", "rebuild/request-page", "local/apply-rebuild-page", "local/finalize-rebuild":
		default:
			return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s has unsupported operation %s", step.ID, scenarios.OperationKey(step.Operation))
		}
	}
	if len(calls) != len(plan.CallOrder) || len(installed) != len(plan.Clients) {
		return MultiScopeProvenanceResult{}, errors.New("Swift multi-scope provenance calls did not cover every authored client")
	}

	clients := multiScopeProvenanceClientsInOrder(plan.Clients)
	clientFacts, err := platform.Capture(ctx, clients, []string{
		"application-rows",
		"pending-mutations",
		"rejected-mutations",
		"checkpoints",
		"provenance",
		"rebuild-state",
	})
	if err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("capture Swift multi-scope provenance client state: %w", err)
	}
	actualClient, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	clientKeys := make([]string, 0, len(clients))
	for _, client := range clients {
		clientKeys = append(clientKeys, client.Key)
	}
	serverCaptures, err := controller.Capture(ctx, clientKeys, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return MultiScopeProvenanceResult{}, fmt.Errorf("capture Swift multi-scope provenance server state: %w", err)
	}
	evidence, err := resolveMultiScopeProvenanceIdentities(controller, plan, scenario.NativeIdentityAliases, calls, actualClient, serverCaptures[0].StateFacts)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	if err := validateMultiScopeProvenanceState(plan, serverCaptures[0].StateFacts, actualClient, evidence); err != nil {
		return MultiScopeProvenanceResult{}, err
	}

	return MultiScopeProvenanceResult{
		Calls:              calls,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: evidence.Resolutions,
	}, nil
}

func multiScopeProvenancePlanForScenario(scenario scenarios.Scenario) (multiScopeProvenancePlan, error) {
	steps, err := swiftScenarioStepMap(scenario, multiScopeProvenanceScenarioID, 83)
	if err != nil {
		return multiScopeProvenancePlan{}, err
	}
	if len(steps) == 0 {
		return multiScopeProvenancePlan{}, errors.New("Swift multi-scope provenance scenario has no steps")
	}
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-PERF-MULTI-SCOPE-PROVENANCE-SEMANTIC-001")
	if err != nil {
		return multiScopeProvenancePlan{}, err
	}
	plan := multiScopeProvenancePlan{
		Expected: expected,
		Steps:    append([]scenarios.Step(nil), scenario.Steps...),
		Calls:    make(map[scenarios.StepID]*multiScopeProvenanceCall),
		Clients:  make(map[string]Client),
	}
	commits := make(map[string]bool)
	stages := make(map[uint64]struct{})
	var currentCall *multiScopeProvenanceCall
	var currentRebuild *multiScopeProvenanceRebuild
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" {
			return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s does not expect success", step.ID)
		}
		key := scenarios.OperationKey(step.Operation)
		switch key {
		case "model/commit-source-transaction":
			currentCall = nil
			currentRebuild = nil
			payload, decodeErr := decodeMultiScopeProvenanceCommit(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, decodeErr)
			}
			commitKey := payload.StreamGeneration + "\x00" + payload.CommitLSN
			if commits[commitKey] {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance commit %s is duplicated", payload.CommitLSN)
			}
			commits[commitKey] = false
			plan.TransactionCount++
			for _, event := range payload.Events {
				if event.Relation == "" || event.Operation == "" || event.After == nil || event.After.Identity.Kind == "" {
					return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance commit %s event is invalid", payload.CommitLSN)
				}
				if event.After.Identity.Kind == "synced" {
					plan.SyncedEventCount++
				} else if event.After.Identity.Kind == "capture_dependency" {
					plan.CaptureEventCount++
				} else {
					return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance commit %s event identity is unsupported", payload.CommitLSN)
				}
			}
		case "process/materialize-source-transaction":
			currentCall = nil
			currentRebuild = nil
			payload, decodeErr := decodeMultiScopeProvenanceMaterialize(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, decodeErr)
			}
			commitKey := payload.StreamGeneration + "\x00" + payload.CommitLSN
			materialized, found := commits[commitKey]
			if !found || materialized {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance materialization %s is invalid", payload.CommitLSN)
			}
			commits[commitKey] = true
		case "model/stage-registry-membership-generation":
			currentCall = nil
			currentRebuild = nil
			payload, decodeErr := decodeMultiScopeProvenanceStage(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, decodeErr)
			}
			if _, duplicate := stages[payload.RegistryGeneration]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance registry generation %d is duplicated", payload.RegistryGeneration)
			}
			stages[payload.RegistryGeneration] = struct{}{}
		case "model/activate-registry-membership-generation":
			currentCall = nil
			currentRebuild = nil
			var payload multiScopeProvenanceActivatePayload
			if decodeErr := json.Unmarshal(step.Operation.Payload, &payload); decodeErr != nil || payload.RegistryGeneration == 0 {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance activation %s is invalid", step.ID)
			}
			if _, staged := stages[payload.RegistryGeneration]; !staged {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance activation %d has no staged generation", payload.RegistryGeneration)
			}
		case "model/set-client-assignments":
			currentCall = nil
			currentRebuild = nil
			if err := validateMultiScopeProvenanceAssignment(step.Operation); err != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, err)
			}
		case "connect/send":
			if currentRebuild != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance call %s begins before a rebuild closes", step.ID)
			}
			call, decodeErr := newMultiScopeProvenanceCall(step)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, decodeErr
			}
			if _, duplicate := plan.Calls[step.ID]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance call %s is duplicated", step.ID)
			}
			plan.Calls[step.ID] = call
			plan.CallOrder = append(plan.CallOrder, step.ID)
			if existing, found := plan.Clients[call.Client.Key]; found && existing != call.Client {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance client %s is inconsistent", call.Client.ClientID)
			}
			plan.Clients[call.Client.Key] = call.Client
			currentCall = call
		case "local/begin-rebuild":
			if currentCall == nil || currentRebuild != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance rebuild begin %s is out of order", step.ID)
			}
			rebuild, decodeErr := newMultiScopeProvenanceRebuild(step, currentCall)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, decodeErr
			}
			currentCall.Rebuilds = append(currentCall.Rebuilds, rebuild)
			currentRebuild = rebuild
		case "rebuild/request-page":
			if err := bindMultiScopeProvenanceRebuildRequest(step, currentCall, currentRebuild); err != nil {
				return multiScopeProvenancePlan{}, err
			}
		case "local/apply-rebuild-page":
			if err := bindMultiScopeProvenanceRebuildApply(step, currentCall, currentRebuild); err != nil {
				return multiScopeProvenancePlan{}, err
			}
		case "local/finalize-rebuild":
			if err := bindMultiScopeProvenanceRebuildFinalize(step, currentCall, currentRebuild); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentRebuild = nil
		default:
			return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance operation %q is unsupported", key)
		}
	}
	if currentRebuild != nil {
		return multiScopeProvenancePlan{}, errors.New("Swift multi-scope provenance scenario ends with a rebuild in progress")
	}
	for _, materialized := range commits {
		if !materialized {
			return multiScopeProvenancePlan{}, errors.New("Swift multi-scope provenance source transaction was not materialized")
		}
	}
	if plan.TransactionCount == 0 || plan.SyncedEventCount == 0 || plan.CaptureEventCount == 0 || len(plan.CallOrder) == 0 || len(plan.Clients) == 0 {
		return multiScopeProvenancePlan{}, errors.New("Swift multi-scope provenance scenario coverage is incomplete")
	}
	if expected.TransactionCount == nil || *expected.TransactionCount != plan.TransactionCount {
		return multiScopeProvenancePlan{}, errors.New("Swift multi-scope provenance transaction count differs from the authored state")
	}
	return plan, nil
}

func decodeMultiScopeProvenanceCommit(operation scenarios.Operation) (multiScopeProvenanceCommitPayload, error) {
	var payload multiScopeProvenanceCommitPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.StreamGeneration == "" || payload.CommitLSN == "" || payload.EndLSN == "" {
		return multiScopeProvenanceCommitPayload{}, errors.New("source commit payload is invalid")
	}
	return payload, nil
}

func decodeMultiScopeProvenanceMaterialize(operation scenarios.Operation) (multiScopeProvenanceMaterializePayload, error) {
	var payload multiScopeProvenanceMaterializePayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.StreamGeneration == "" || payload.CommitLSN == "" {
		return multiScopeProvenanceMaterializePayload{}, errors.New("source materialization payload is invalid")
	}
	return payload, nil
}

func decodeMultiScopeProvenanceStage(operation scenarios.Operation) (multiScopeProvenanceStagePayload, error) {
	var payload multiScopeProvenanceStagePayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.RegistryGeneration == 0 || payload.BatchSize == 0 || len(payload.AffectedScopes) == 0 || len(payload.ScopeRules) == 0 {
		return multiScopeProvenanceStagePayload{}, errors.New("membership stage payload is invalid")
	}
	for _, scope := range payload.AffectedScopes {
		if scope == "" {
			return multiScopeProvenanceStagePayload{}, errors.New("membership stage affected scope is invalid")
		}
	}
	for _, rule := range payload.ScopeRules {
		if rule.Relation == "" || len(rule.Evaluations) == 0 {
			return multiScopeProvenanceStagePayload{}, errors.New("membership stage rule is invalid")
		}
		for _, evaluation := range rule.Evaluations {
			if len(evaluation.Scopes) == 0 {
				return multiScopeProvenanceStagePayload{}, errors.New("membership stage evaluation is invalid")
			}
		}
	}
	return payload, nil
}

func validateMultiScopeProvenanceAssignment(operation scenarios.Operation) error {
	var payload multiScopeProvenanceAssignmentPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.UserID == "" || payload.ClientID == "" || len(payload.Assignments) == 0 {
		return errors.New("scope assignment payload is invalid")
	}
	for _, assignment := range payload.Assignments {
		if assignment.ScopeID == "" {
			return errors.New("scope assignment is invalid")
		}
	}
	return nil
}

func newMultiScopeProvenanceCall(step scenarios.Step) (*multiScopeProvenanceCall, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Kind != "public-call" || binding.UserID == "" || binding.ClientID == "" || binding.Stage != "synchronous" || binding.Completion != "idle" {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s binding is invalid", step.ID)
	}
	var payload multiScopeProvenanceConnectPayload
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID || len(payload.KnownScopes) == 0 {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s payload is invalid", step.ID)
	}
	for _, scope := range payload.KnownScopes {
		if scope.ScopeID == "" {
			return nil, fmt.Errorf("Swift multi-scope provenance call %s scope is invalid", step.ID)
		}
	}
	measuredScopeCount, err := multiScopeProvenanceMeasuredScopeCount(step)
	if err != nil {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s: %w", step.ID, err)
	}
	return &multiScopeProvenanceCall{
		Step:               step,
		Client:             Client{Key: "multi-scope-provenance-" + binding.UserID + "-" + binding.ClientID, UserID: binding.UserID, ClientID: binding.ClientID, DatabaseKey: "multi-scope-provenance-" + binding.UserID + "-" + binding.ClientID},
		KnownScopeCount:    uint64(len(payload.KnownScopes)),
		MeasuredScopeCount: measuredScopeCount,
	}, nil
}

func multiScopeProvenanceMeasuredScopeCount(step scenarios.Step) (uint64, error) {
	if step.MeasurementSample == nil {
		return 0, nil
	}
	raw, err := json.Marshal(step.MeasurementSample)
	if err != nil {
		return 0, fmt.Errorf("encode measurement sample: %w", err)
	}
	var payload multiScopeProvenanceMeasurementPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, errors.New("measurement sample is invalid")
	}
	count, found := payload.Parameters["provenance_scope_count"]
	if !found || count == 0 {
		return 0, errors.New("measurement scope count is invalid")
	}
	return count, nil
}

func newMultiScopeProvenanceRebuild(step scenarios.Step, call *multiScopeProvenanceCall) (*multiScopeProvenanceRebuild, error) {
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil {
		return nil, fmt.Errorf("Swift multi-scope provenance rebuild %s: %w", step.ID, err)
	}
	if !multiScopeProvenanceRebuildClientMatches(payload, call.Client) || payload.Limit == 0 {
		return nil, fmt.Errorf("Swift multi-scope provenance rebuild %s binding is invalid", step.ID)
	}
	return &multiScopeProvenanceRebuild{ScopeID: payload.ScopeID, RebuildID: payload.RebuildID, Limit: payload.Limit, Begin: step}, nil
}

func bindMultiScopeProvenanceRebuildRequest(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if rebuild == nil || rebuild.Request.ID != "" {
		return fmt.Errorf("Swift multi-scope provenance rebuild request %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) || payload.CursorSource == "" {
		return fmt.Errorf("Swift multi-scope provenance rebuild request %s is invalid", step.ID)
	}
	rebuild.Request = step
	return nil
}

func bindMultiScopeProvenanceRebuildApply(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if rebuild == nil || rebuild.Request.ID == "" || rebuild.Apply.ID != "" {
		return fmt.Errorf("Swift multi-scope provenance rebuild apply %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) || payload.PageOrdinal == 0 || payload.RequestTokenSource == "" {
		return fmt.Errorf("Swift multi-scope provenance rebuild apply %s is invalid", step.ID)
	}
	rebuild.Apply = step
	return nil
}

func bindMultiScopeProvenanceRebuildFinalize(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if rebuild == nil || rebuild.Apply.ID == "" || rebuild.Finalize.ID != "" {
		return fmt.Errorf("Swift multi-scope provenance rebuild finalize %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) {
		return fmt.Errorf("Swift multi-scope provenance rebuild finalize %s is invalid", step.ID)
	}
	rebuild.Finalize = step
	return nil
}

func decodeMultiScopeProvenanceRebuild(operation scenarios.Operation) (multiScopeProvenanceRebuildPayload, error) {
	var payload multiScopeProvenanceRebuildPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.UserID == "" || payload.ClientID == "" || payload.ScopeID == "" || payload.RebuildID == "" {
		return multiScopeProvenanceRebuildPayload{}, errors.New("rebuild payload is invalid")
	}
	return payload, nil
}

func multiScopeProvenanceRebuildClientMatches(payload multiScopeProvenanceRebuildPayload, client Client) bool {
	return payload.UserID == client.UserID && payload.ClientID == client.ClientID
}

func multiScopeProvenanceRebuildMatches(payload multiScopeProvenanceRebuildPayload, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) bool {
	return call != nil && rebuild != nil && multiScopeProvenanceRebuildClientMatches(payload, call.Client) && payload.ScopeID == rebuild.ScopeID && payload.RebuildID == rebuild.RebuildID && (payload.Limit == 0 || payload.Limit == rebuild.Limit)
}

func multiScopeProvenancePullPageSize(scenario scenarios.Scenario) (int, error) {
	plan, err := multiScopeProvenancePlanForScenario(scenario)
	if err != nil {
		return 0, err
	}
	var pageSize uint64
	for _, callID := range plan.CallOrder {
		for _, rebuild := range plan.Calls[callID].Rebuilds {
			if pageSize == 0 {
				pageSize = rebuild.Limit
			} else if pageSize != rebuild.Limit {
				return 0, errors.New("Swift multi-scope provenance rebuild page sizes differ")
			}
		}
	}
	if pageSize == 0 || pageSize > 1000 {
		return 0, errors.New("Swift multi-scope provenance rebuild page size is invalid")
	}
	return int(pageSize), nil
}

func validateMultiScopeProvenanceCall(scenario scenarios.Scenario, call *multiScopeProvenanceCall, result SynchronizationResult) error {
	if call == nil || result.Completion != "idle" {
		completion := "no call"
		if call != nil {
			completion = result.Completion
			if completion == "" {
				completion = "none"
			}
		}
		outcomes := make([]string, 0, len(result.transportObservations))
		for _, observation := range result.transportObservations {
			entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
			if observation.ErrorCode != nil {
				entry += ":" + *observation.ErrorCode
			}
			outcomes = append(outcomes, entry)
		}
		return fmt.Errorf("Swift multi-scope provenance call did not complete: completion %s observations %v", completion, outcomes)
	}
	observations := result.transportObservations
	if len(observations) < 2 || observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return errors.New("Swift multi-scope provenance call shape is invalid")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable || observation.ErrorCode != nil {
			return errors.New("Swift multi-scope provenance call contains an unsuccessful transport response")
		}
	}
	if err := validateSwiftWireObservation(scenario, string(call.Step.ID), observations[0]); err != nil {
		return err
	}
	rebuilds := make([]transportObservation, 0, len(call.Rebuilds))
	for _, observation := range observations[1 : len(observations)-1] {
		if observation.OperationClass != "rebuild" {
			return errors.New("Swift multi-scope provenance call has an unexpected transport operation")
		}
		rebuilds = append(rebuilds, observation)
	}
	if len(rebuilds) != len(call.Rebuilds) {
		return fmt.Errorf("Swift multi-scope provenance rebuild count = %d, want %d", len(rebuilds), len(call.Rebuilds))
	}
	for index, rebuild := range call.Rebuilds {
		if err := validateMultiScopeProvenanceRebuildCall(scenario, rebuild, rebuilds[index]); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiScopeProvenanceRebuildCall(scenario scenarios.Scenario, rebuild *multiScopeProvenanceRebuild, observation transportObservation) error {
	if rebuild == nil || rebuild.Request.ID == "" || rebuild.Apply.ID == "" || rebuild.Finalize.ID == "" || observation.RequestFacts == nil || observation.RebuildResponseFacts == nil {
		return errors.New("Swift multi-scope provenance rebuild evidence is incomplete")
	}
	payload, err := decodeMultiScopeProvenanceRebuild(rebuild.Request.Operation)
	if err != nil {
		return err
	}
	request := observation.RequestFacts
	// The client mints its own rebuild identifier. The authored value is a
	// symbolic alias that identity resolution binds to the runtime rebuild, so
	// only its presence and the authored page bound are asserted here.
	if observation.OperationClass != "rebuild" || request.Limit == nil || uint64(*request.Limit) != payload.Limit || request.RebuildIDFingerprint == nil || *request.RebuildIDFingerprint == "" || request.CursorPresent == nil || *request.CursorPresent {
		limit := "none"
		if request.Limit != nil {
			limit = strconv.FormatInt(int64(*request.Limit), 10)
		}
		fingerprint := "none"
		if request.RebuildIDFingerprint != nil {
			fingerprint = *request.RebuildIDFingerprint
		}
		cursor := "none"
		if request.CursorPresent != nil {
			cursor = strconv.FormatBool(*request.CursorPresent)
		}
		return fmt.Errorf("Swift multi-scope provenance rebuild request differs from the authored operation: class %s limit %s want %d fingerprint %s cursor %s", observation.OperationClass, limit, payload.Limit, fingerprint, cursor)
	}
	response := observation.RebuildResponseFacts
	if response.HasMore || response.HasCursor || !response.HasFinalScopeCursor || !response.HasChecksum || response.FinalScopeCursorFingerprint == nil || response.ScopeFingerprint == "" || request.ScopeFingerprint == nil || response.ScopeFingerprint != *request.ScopeFingerprint {
		return errors.New("Swift multi-scope provenance rebuild response does not prove finality")
	}
	return validateSwiftWireObservation(scenario, string(rebuild.Request.ID), observation)
}

func multiScopeProvenanceClientsInOrder(clients map[string]Client) []Client {
	keys := make([]string, 0, len(clients))
	for key := range clients {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]Client, 0, len(keys))
	for _, key := range keys {
		values = append(values, clients[key])
	}
	return values
}

func resolveMultiScopeProvenanceIdentities(controller *blackbox.NativeController, plan multiScopeProvenancePlan, aliases []scenarios.NativeIdentityAlias, calls []SynchronizationResult, actual, server scenarios.StateFacts) (multiScopeProvenanceIdentityEvidence, error) {
	if len(aliases) == 0 || len(calls) == 0 || len(actual.Clients) == 0 {
		return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance identity evidence is incomplete")
	}
	// IdentityValues binds only server-owned kinds and skips the rest, so the
	// completeness check below is what proves every alias resolved.
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("read Swift multi-scope provenance controller identities: %w", err)
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	for _, value := range values {
		if value.Alias == "" || len(value.RuntimeValue) == 0 {
			return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance controller identity is invalid")
		}
		if _, duplicate := runtime[value.Alias]; duplicate {
			return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance controller identity is duplicated")
		}
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
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
				return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance generation changed between calls")
			}
		}
	}
	if generation <= 0 {
		return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance generation is absent")
	}
	var runtimeSchema schemaRef
	for _, client := range actual.Clients {
		if client.CurrentSchema == nil {
			return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance client schema is absent")
		}
		schema := schemaRef{Version: int64(client.CurrentSchema.Version), Hash: client.CurrentSchema.Hash}
		if runtimeSchema.Version == 0 {
			runtimeSchema = schema
		} else if runtimeSchema != schema {
			return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance schemas differ")
		}
	}
	for _, alias := range aliases {
		var value any
		switch alias.Kind {
		case "client-generation":
			value = generation
		case "schema":
			value = runtimeSchema
		case "rebuild-id":
			rebuildID, rebuildErr := multiScopeProvenanceRuntimeRebuildID(plan, server, alias)
			if rebuildErr != nil {
				return multiScopeProvenanceIdentityEvidence{}, rebuildErr
			}
			value = rebuildID
		case "row-version", "checksum":
			row, rowErr := multiScopeProvenanceRuntimeRow(plan, server, alias)
			if rowErr != nil {
				return multiScopeProvenanceIdentityEvidence{}, rowErr
			}
			if alias.Kind == "row-version" {
				value = row.Version
			} else {
				value = row.Checksum
			}
		default:
			continue
		}
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("encode Swift multi-scope provenance identity %s: %w", alias.Alias, marshalErr)
		}
		runtime[alias.Alias] = encoded
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("Swift multi-scope provenance alias %s has no runtime value", alias.Alias)
		}
	}
	resolutions, err := resolveSwiftNativeIdentities(aliases, runtime)
	if err != nil {
		return multiScopeProvenanceIdentityEvidence{}, err
	}
	return multiScopeProvenanceIdentityEvidence{Resolutions: resolutions}, nil
}

func validateMultiScopeProvenanceState(plan multiScopeProvenancePlan, server, actual scenarios.StateFacts, evidence multiScopeProvenanceIdentityEvidence) error {
	serverExpected := scenarios.CloneStateFacts(plan.Expected)
	serverExpected.Clients = nil
	if err := validateSwiftStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Swift multi-scope provenance server state differs from the authored model: %w", err)
	}
	if len(evidence.Resolutions) == 0 {
		return errors.New("Swift multi-scope provenance identity resolutions are absent")
	}
	expectedBase := scenarios.CloneStateFacts(plan.Expected)
	actualBase := scenarios.CloneStateFacts(actual)
	for index := range expectedBase.Clients {
		expectedBase.Clients[index].Provenance = nil
		expectedBase.Clients[index].Checkpoints = nil
		expectedBase.Clients[index].Queue = nil
		expectedBase.Clients[index].Outcomes = nil
	}
	for index := range actualBase.Clients {
		actualBase.Clients[index].Provenance = nil
		actualBase.Clients[index].Checkpoints = nil
		actualBase.Clients[index].Queue = nil
		actualBase.Clients[index].Outcomes = nil
	}
	if err := validateSwiftStateProjection(expectedBase, actualBase); err != nil {
		return fmt.Errorf("Swift multi-scope provenance client counts differ from the authored model: %w", err)
	}
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(evidence.Resolutions))
	for _, resolution := range evidence.Resolutions {
		if _, duplicate := resolved[resolution.Alias]; duplicate {
			return errors.New("Swift multi-scope provenance identity resolution is duplicated")
		}
		resolved[resolution.Alias] = resolution
	}
	actualClients := make(map[string]scenarios.ClientDurabilityFact, len(actual.Clients))
	for _, client := range actual.Clients {
		actualClients[client.UserID+"\x00"+client.ClientID] = client
	}
	for _, expectedClient := range plan.Expected.Clients {
		actualClient, found := actualClients[expectedClient.UserID+"\x00"+expectedClient.ClientID]
		if !found {
			return fmt.Errorf("Swift multi-scope provenance client %s is absent", expectedClient.ClientID)
		}
		if err := validateMultiScopeProvenanceProvenance(expectedClient.Provenance, actualClient.Provenance, resolved); err != nil {
			return fmt.Errorf("Swift multi-scope provenance client %s: %w", expectedClient.ClientID, err)
		}
	}
	return nil
}

func validateMultiScopeProvenanceProvenance(expected, actual []scenarios.ProvenanceFact, resolutions map[string]blackbox.NativeIdentityResolution) error {
	if len(expected) != len(actual) {
		return errors.New("provenance count differs")
	}
	expected = append([]scenarios.ProvenanceFact(nil), expected...)
	actual = append([]scenarios.ProvenanceFact(nil), actual...)
	sort.Slice(expected, func(left, right int) bool {
		return expected[left].CanonicalWireJSON < expected[right].CanonicalWireJSON
	})
	sort.Slice(actual, func(left, right int) bool {
		return actual[left].CanonicalWireJSON < actual[right].CanonicalWireJSON
	})
	for index := range expected {
		want := expected[index]
		got := actual[index]
		if want.CanonicalWireJSON != got.CanonicalWireJSON || !reflect.DeepEqual(want.Scopes, got.Scopes) || want.Version != got.Version || !multiScopeProvenanceTableIdentityMatches(resolutions, want.TableID, got.TableID) {
			return errors.New("provenance differs from the authored identity")
		}
	}
	return nil
}

func multiScopeProvenanceTableIdentityMatches(resolutions map[string]blackbox.NativeIdentityResolution, authored, runtime string) bool {
	for _, resolution := range resolutions {
		if resolutionMatchesString(resolution, authored, runtime) {
			return true
		}
	}
	return false
}

// multiScopeProvenanceRuntimeRebuildID binds a rebuild alias to the rebuild the
// server recorded for the client and scope named by the alias anchor step.
func multiScopeProvenanceRuntimeRebuildID(plan multiScopeProvenancePlan, server scenarios.StateFacts, alias scenarios.NativeIdentityAlias) (string, error) {
	if len(alias.StepIDs) != 1 {
		return "", fmt.Errorf("Swift multi-scope provenance rebuild alias %s has no single anchor step", alias.Alias)
	}
	for _, callID := range plan.CallOrder {
		call := plan.Calls[callID]
		for _, rebuild := range call.Rebuilds {
			if rebuild.Begin.ID != alias.StepIDs[0] {
				continue
			}
			for _, recorded := range server.Rebuilds {
				if recorded.UserID == call.Client.UserID && recorded.ClientID == call.Client.ClientID && recorded.ScopeID == rebuild.ScopeID {
					return recorded.RebuildID, nil
				}
			}
			return "", fmt.Errorf("Swift multi-scope provenance rebuild alias %s has no server rebuild", alias.Alias)
		}
	}
	return "", fmt.Errorf("Swift multi-scope provenance rebuild alias %s names no authored rebuild", alias.Alias)
}

// multiScopeProvenanceRuntimeRow binds a row alias to the server row created by
// the commit step the alias anchors to.
func multiScopeProvenanceRuntimeRow(plan multiScopeProvenancePlan, server scenarios.StateFacts, alias scenarios.NativeIdentityAlias) (scenarios.RowFact, error) {
	if len(alias.StepIDs) != 1 {
		return scenarios.RowFact{}, fmt.Errorf("Swift multi-scope provenance row alias %s has no single anchor step", alias.Alias)
	}
	for _, step := range plan.Steps {
		if step.ID != alias.StepIDs[0] {
			continue
		}
		payload, err := decodeMultiScopeProvenanceCommit(step.Operation)
		if err != nil {
			return scenarios.RowFact{}, err
		}
		for _, event := range payload.Events {
			if event.After == nil || event.After.Identity.SyncedRow == nil {
				continue
			}
			canonical := event.After.Identity.SyncedRow.CanonicalWireJSON
			for _, row := range server.Rows {
				if row.CanonicalWireJSON == canonical {
					return row, nil
				}
			}
		}
		return scenarios.RowFact{}, fmt.Errorf("Swift multi-scope provenance row alias %s has no server row", alias.Alias)
	}
	return scenarios.RowFact{}, fmt.Errorf("Swift multi-scope provenance row alias %s names no authored commit", alias.Alias)
}
