package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const multiScopeProvenanceScenarioID = "SCN-PERF-MULTI-SCOPE-PROVENANCE-001"

// MultiScopeProvenanceResult records direct Kotlin Android evidence for the authored scenario.
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
	CallID             string
	Connect            multiScopeProvenanceConnectPayload
	KnownScopeCount    uint64
	AssignedScopeCount uint64
	MeasuredScopeCount uint64
	Rebuilds           []*multiScopeProvenanceRebuild
}

type multiScopeProvenanceRebuild struct {
	ClientGeneration int64
	Schema           schemaRef
	ScopeID          string
	RebuildID        string
	Limit            uint64
	Begin            scenarios.Step
	Request          scenarios.Step
	Apply            scenarios.Step
	Finalize         scenarios.Step
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
	UserID           string    `json:"user_id"`
	ClientID         string    `json:"client_id"`
	RuntimeVersion   int64     `json:"runtime_version"`
	ProtocolVersion  int64     `json:"protocol_version"`
	ClientGeneration *int64    `json:"client_generation"`
	Schema           schemaRef `json:"schema"`
	SchemaReset      bool      `json:"schema_reset"`
	ScopeSetVersion  uint64    `json:"scope_set_version"`
	KnownScopes      []struct {
		ScopeID string `json:"scope_id"`
	} `json:"known_scopes"`
}

type multiScopeProvenanceRebuildPayload struct {
	UserID             string    `json:"user_id"`
	ClientID           string    `json:"client_id"`
	ClientGeneration   int64     `json:"client_generation"`
	Schema             schemaRef `json:"schema"`
	ScopeID            string    `json:"scope_id"`
	RebuildID          string    `json:"rebuild_id"`
	Limit              uint64    `json:"limit"`
	CursorSource       string    `json:"cursor_source"`
	PageOrdinal        uint64    `json:"page_ordinal"`
	RequestTokenSource string    `json:"request_token_source"`
}

type multiScopeProvenanceMeasurementPayload struct {
	Parameters map[string]uint64 `json:"parameters"`
}

type multiScopeProvenanceIdentityEvidence struct {
	Resolutions    []blackbox.NativeIdentityResolution
	Revocations    []string
	CallOperations []string
	TableNames     map[string]string
}

// RunMultiScopeProvenanceScenario executes the authored flow through Kotlin Android.
func RunMultiScopeProvenanceScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, artifact *blackbox.NativeArtifact, platform *Platform) (MultiScopeProvenanceResult, error) {
	if controller == nil || artifact == nil || platform == nil {
		return MultiScopeProvenanceResult{}, errors.New("Kotlin Android multi-scope provenance dependencies are unavailable")
	}
	modelScenario, err := multiScopeProvenanceModelScenario(scenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	modelResult, err := modelrunner.RunScenario(ctx, modelScenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("derive Kotlin Android multi-scope provenance operations: %w", err)
	}
	if err := validateMultiScopeProvenanceModelResult(scenario, modelResult); err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	plan, err := multiScopeProvenancePlanForScenario(scenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("install Kotlin Android multi-scope provenance contract: %w", err)
	}

	started := make(map[string]bool, len(plan.Clients))
	appliedScopeSetVersions := make(map[string]int64, len(plan.Clients))
	appliedScopeCounts := make(map[string]int, len(plan.Clients))
	calls := make([]SynchronizationResult, 0, len(plan.CallOrder))
	for index, step := range plan.Steps {
		modelOperation := modelResult.Steps[index].Operation
		switch scenarios.OperationKey(modelOperation) {
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/set-client-assignments":
			observation, applyErr := controller.ApplyStep(ctx, modelOperation)
			if applyErr != nil || observation.Disposition != "success" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("apply Kotlin Android multi-scope provenance step %s: %w", step.ID, kotlinResultError(applyErr, observation.Disposition))
			}
		case "process/materialize-source-transaction":
			observation, processErr := controller.ProcessStep(ctx, nil, modelOperation)
			if processErr != nil || observation.Disposition != "success" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("materialize Kotlin Android multi-scope provenance step %s: %w", step.ID, kotlinResultError(processErr, observation.Disposition))
			}
		case "connect/send":
			call := plan.Calls[step.ID]
			if call == nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Kotlin Android multi-scope provenance call %s is absent", step.ID)
			}
			method := call.Step.NativeBinding.Method
			if started[call.Client.Key] {
				if _, stopErr := platform.Lifecycle(ctx, LifecycleRequest{Client: call.Client, Operation: "stop"}); stopErr != nil {
					return MultiScopeProvenanceResult{}, fmt.Errorf("stop Kotlin Android client %s before connect: %w", call.Client.ClientID, stopErr)
				}
				method = "start"
			}
			if !started[call.Client.Key] {
				started[call.Client.Key] = true
				if installErr := platform.Install(ctx, InstallRequest{Client: call.Client, Initialization: "empty"}); installErr != nil {
					return MultiScopeProvenanceResult{}, fmt.Errorf("install Kotlin Android multi-scope provenance client %s: %w", call.Client.ClientID, installErr)
				}
				method = "start"
			}
			result, callErr := kotlinScenarioCall(ctx, platform, call.Client, method)
			if callErr != nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("run Kotlin Android multi-scope provenance client %s: %w", call.Client.ClientID, callErr)
			}
			runtimeSchema, schemaErr := multiScopeProvenanceRuntimeSchema(controller, scenario.NativeIdentityAliases)
			if schemaErr != nil {
				return MultiScopeProvenanceResult{}, schemaErr
			}
			if err := validateMultiScopeProvenanceCall(scenario, call, result, runtimeSchema, appliedScopeSetVersions[call.Client.ClientID], appliedScopeCounts[call.Client.ClientID]); err != nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("validate Kotlin Android multi-scope provenance client %s: %w", call.Client.ClientID, err)
			}
			appliedScopeSetVersions[call.Client.ClientID] = int64(call.Connect.ScopeSetVersion)
			appliedScopeCounts[call.Client.ClientID] = int(call.AssignedScopeCount)
			calls = append(calls, result)
		case "local/begin-rebuild", "rebuild/request-page", "local/apply-rebuild-page", "local/finalize-rebuild":
			// The public synchronization call executes these authored protocol operations.
		default:
			return MultiScopeProvenanceResult{}, fmt.Errorf("Kotlin Android multi-scope provenance step %s has unsupported operation %s", step.ID, scenarios.OperationKey(step.Operation))
		}
	}
	if len(calls) != len(plan.CallOrder) || len(started) != len(plan.Clients) {
		return MultiScopeProvenanceResult{}, errors.New("Kotlin Android multi-scope provenance calls did not cover every client")
	}

	clients := multiScopeProvenanceClientsInOrder(plan.Clients)
	clientFacts, err := platform.Capture(ctx, clients, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance", "rebuild-state"})
	if err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("capture Kotlin Android multi-scope provenance client state: %w", err)
	}
	actualClient, err := mergeKotlinCaptureFacts(clientFacts)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	clientKeys := make([]string, 0, len(clients))
	for _, client := range clients {
		clientKeys = append(clientKeys, client.Key)
	}
	serverCaptures, err := controller.Capture(ctx, clientKeys, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return MultiScopeProvenanceResult{}, fmt.Errorf("capture Kotlin Android multi-scope provenance server state: %w", kotlinResultError(err, ""))
	}
	serverState := serverCaptures[0].StateFacts
	evidence, err := resolveMultiScopeProvenanceIdentities(controller, plan, scenario.NativeIdentityAliases, calls, actualClient, serverState)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	if err := validateMultiScopeProvenanceState(plan, serverState, actualClient, evidence); err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	return MultiScopeProvenanceResult{Calls: calls, ClientFacts: clientFacts, ServerFacts: serverState, IdentityResolution: evidence.Resolutions}, nil
}

func validateMultiScopeProvenanceModelResult(scenario scenarios.Scenario, result modelrunner.Result) error {
	if !result.Passed || len(result.Setup) != 1 || len(result.Steps) != len(scenario.Steps) {
		return errors.New("authored multi-scope provenance model did not close all steps")
	}
	if !reflect.DeepEqual(result.Setup[0].Operation, scenario.Model.Setup[0]) {
		return errors.New("authored multi-scope provenance model setup differs")
	}
	for index, authoredStep := range scenario.Steps {
		modelStep := result.Steps[index]
		if modelStep.StepID != authoredStep.ID || !reflect.DeepEqual(modelStep.Operation, authoredStep.Operation) || modelStep.Err != nil {
			return fmt.Errorf("authored multi-scope provenance model step %s differs", authoredStep.ID)
		}
	}
	return nil
}

func multiScopeProvenanceModelScenario(scenario scenarios.Scenario) (scenarios.Scenario, error) {
	modelScenario := scenario
	modelScenario.Model.ExpectedState = append([]scenarios.ModelExpectation(nil), scenario.Model.ExpectedState...)
	expectations := make([]scenarios.ModelExpectation, 0, len(modelScenario.Model.ExpectedState))
	for _, expectation := range modelScenario.Model.ExpectedState {
		if expectation.Predicate.Name != "performance-contract-satisfied" {
			expectations = append(expectations, expectation)
		}
	}
	modelScenario.Model.ExpectedState = expectations
	for index := range modelScenario.Model.ExpectedState {
		facts := modelScenario.Model.ExpectedState[index].StateFacts
		if facts == nil {
			continue
		}
		normalized, err := scenarios.NormalizeStateFacts(*facts)
		if err != nil {
			return scenarios.Scenario{}, fmt.Errorf("normalize authored multi-scope provenance expectation %s: %w", modelScenario.Model.ExpectedState[index].ID, err)
		}
		modelScenario.Model.ExpectedState[index].StateFacts = &normalized
	}
	return modelScenario, nil
}

func multiScopeProvenancePlanForScenario(scenario scenarios.Scenario) (multiScopeProvenancePlan, error) {
	if _, err := kotlinScenarioStepMap(scenario, multiScopeProvenanceScenarioID, len(scenario.Steps)); err != nil {
		return multiScopeProvenancePlan{}, err
	}
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-PERF-MULTI-SCOPE-PROVENANCE-SEMANTIC-001")
	if err != nil {
		return multiScopeProvenancePlan{}, err
	}
	plan := multiScopeProvenancePlan{Expected: expected, Steps: append([]scenarios.Step(nil), scenario.Steps...), Calls: make(map[scenarios.StepID]*multiScopeProvenanceCall), Clients: make(map[string]Client)}
	commits := make(map[string]bool)
	stages := make(map[uint64]bool)
	assignedScopes := make(map[string]uint64)
	callIDs := make(map[string]scenarios.StepID)
	var currentCall *multiScopeProvenanceCall
	var currentRebuild *multiScopeProvenanceRebuild
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" {
			return multiScopeProvenancePlan{}, fmt.Errorf("Kotlin Android multi-scope provenance step %s does not expect success", step.ID)
		}
		if err := scenarios.ValidateOperation(step.Operation); err != nil {
			return multiScopeProvenancePlan{}, fmt.Errorf("Kotlin Android multi-scope provenance step %s is invalid: %w", step.ID, err)
		}
		switch key := scenarios.OperationKey(step.Operation); key {
		case "model/commit-source-transaction":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall, currentRebuild = nil, nil
			payload, decodeErr := decodeMultiScopeProvenanceCommit(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("step %s: %w", step.ID, decodeErr)
			}
			commitKey := payload.StreamGeneration + "\x00" + payload.CommitLSN
			if _, duplicate := commits[commitKey]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("commit %s is duplicated", payload.CommitLSN)
			}
			commits[commitKey] = false
			plan.TransactionCount++
			for _, event := range payload.Events {
				if event.Relation == "" || event.Operation == "" || event.After == nil || event.After.Identity.Kind == "" {
					return multiScopeProvenancePlan{}, fmt.Errorf("commit %s event is invalid", payload.CommitLSN)
				}
				switch event.After.Identity.Kind {
				case "synced":
					plan.SyncedEventCount++
				case "capture_dependency":
					plan.CaptureEventCount++
				default:
					return multiScopeProvenancePlan{}, fmt.Errorf("commit %s event identity is unsupported", payload.CommitLSN)
				}
			}
		case "process/materialize-source-transaction":
			if err := validateMultiScopeProvenanceControllerBinding(step, "process"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall, currentRebuild = nil, nil
			payload, decodeErr := decodeMultiScopeProvenanceMaterialize(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, decodeErr
			}
			commitKey := payload.StreamGeneration + "\x00" + payload.CommitLSN
			materialized, found := commits[commitKey]
			if !found || materialized {
				return multiScopeProvenancePlan{}, fmt.Errorf("materialization %s is invalid", payload.CommitLSN)
			}
			commits[commitKey] = true
		case "model/stage-registry-membership-generation":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall, currentRebuild = nil, nil
			payload, decodeErr := decodeMultiScopeProvenanceStage(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, decodeErr
			}
			if _, duplicate := stages[payload.RegistryGeneration]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("registry generation %d is duplicated", payload.RegistryGeneration)
			}
			stages[payload.RegistryGeneration] = false
		case "model/activate-registry-membership-generation":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall, currentRebuild = nil, nil
			var payload multiScopeProvenanceActivatePayload
			if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.RegistryGeneration == 0 {
				return multiScopeProvenancePlan{}, fmt.Errorf("activation %s is invalid", step.ID)
			}
			activated, staged := stages[payload.RegistryGeneration]
			if !staged || activated {
				return multiScopeProvenancePlan{}, fmt.Errorf("activation %d has no staged generation", payload.RegistryGeneration)
			}
			stages[payload.RegistryGeneration] = true
		case "model/set-client-assignments":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall, currentRebuild = nil, nil
			var assignment multiScopeProvenanceAssignmentPayload
			if err := validateMultiScopeProvenanceAssignment(step.Operation); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			_ = json.Unmarshal(step.Operation.Payload, &assignment)
			assignedScopes[assignment.ClientID] = uint64(len(assignment.Assignments))
		case "connect/send":
			if currentRebuild != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("call %s begins before a rebuild closes", step.ID)
			}
			call, decodeErr := newMultiScopeProvenanceCall(step, assignedScopes)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, decodeErr
			}
			if _, duplicate := plan.Calls[step.ID]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("call %s is duplicated", step.ID)
			}
			if previous, duplicate := callIDs[call.CallID]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("call ID %s is used by %s and %s", call.CallID, previous, step.ID)
			}
			callIDs[call.CallID] = step.ID
			plan.Calls[step.ID] = call
			plan.CallOrder = append(plan.CallOrder, step.ID)
			plan.Clients[call.Client.Key] = call.Client
			currentCall = call
		case "local/begin-rebuild":
			if currentCall == nil || currentRebuild != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("rebuild begin %s is out of order", step.ID)
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
			return multiScopeProvenancePlan{}, fmt.Errorf("Kotlin Android multi-scope provenance operation %q is unsupported", key)
		}
	}
	if currentRebuild != nil {
		return multiScopeProvenancePlan{}, errors.New("multi-scope provenance scenario ends with a rebuild in progress")
	}
	for _, materialized := range commits {
		if !materialized {
			return multiScopeProvenancePlan{}, errors.New("source transaction was not materialized")
		}
	}
	for generation, activated := range stages {
		if !activated {
			return multiScopeProvenancePlan{}, fmt.Errorf("registry generation %d was not activated", generation)
		}
	}
	if plan.TransactionCount == 0 || plan.SyncedEventCount == 0 || plan.CaptureEventCount == 0 || len(plan.CallOrder) == 0 || len(plan.Clients) == 0 {
		return multiScopeProvenancePlan{}, errors.New("multi-scope provenance coverage is incomplete")
	}
	if expected.TransactionCount == nil || *expected.TransactionCount != plan.TransactionCount {
		return multiScopeProvenancePlan{}, errors.New("transaction count differs from authored state")
	}
	return plan, nil
}

func validateMultiScopeProvenanceControllerBinding(step scenarios.Step, transport string) error {
	if step.Transport != transport || step.NativeBinding == nil || step.NativeBinding.Kind != "controller" {
		return fmt.Errorf("Kotlin Android multi-scope provenance step %s controller binding is invalid", step.ID)
	}
	return nil
}

func validateMultiScopeProvenancePublicBinding(step scenarios.Step, call *multiScopeProvenanceCall) error {
	binding := step.NativeBinding
	if (step.Transport != "http" && step.Transport != "local") || binding == nil || binding.Kind != "public-call" || binding.UserID == "" || binding.ClientID == "" || binding.CallID == nil || *binding.CallID == "" || binding.Stage != "synchronous" || !multiScopeProvenanceCallMethod(binding.Method) || binding.Completion != "idle" {
		return fmt.Errorf("Kotlin Android multi-scope provenance step %s public binding is invalid", step.ID)
	}
	if call != nil && (binding.UserID != call.Client.UserID || binding.ClientID != call.Client.ClientID || string(*binding.CallID) != call.CallID) {
		return fmt.Errorf("Kotlin Android multi-scope provenance step %s does not belong to call %s", step.ID, call.CallID)
	}
	return nil
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

func newMultiScopeProvenanceCall(step scenarios.Step, assignedScopes map[string]uint64) (*multiScopeProvenanceCall, error) {
	if err := validateMultiScopeProvenancePublicBinding(step, nil); err != nil {
		return nil, err
	}
	binding := step.NativeBinding
	var payload multiScopeProvenanceConnectPayload
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID || payload.RuntimeVersion != 3 || payload.ProtocolVersion != 3 || payload.Schema.Version <= 0 || payload.Schema.Hash == "" || payload.ScopeSetVersion == 0 || len(payload.KnownScopes) == 0 {
		return nil, fmt.Errorf("Kotlin Android multi-scope provenance call %s payload is invalid", step.ID)
	}
	if payload.ClientGeneration != nil && *payload.ClientGeneration <= 0 {
		return nil, fmt.Errorf("call %s generation is invalid", step.ID)
	}
	seenScopes := make(map[string]struct{}, len(payload.KnownScopes))
	for _, scope := range payload.KnownScopes {
		if scope.ScopeID == "" {
			return nil, fmt.Errorf("call %s scope is invalid", step.ID)
		}
		if _, duplicate := seenScopes[scope.ScopeID]; duplicate {
			return nil, fmt.Errorf("call %s scope is duplicated", step.ID)
		}
		seenScopes[scope.ScopeID] = struct{}{}
	}
	measuredScopeCount, err := multiScopeProvenanceMeasuredScopeCount(step)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", step.ID, err)
	}
	client := Client{Key: "multi-scope-provenance-" + binding.UserID + "-" + binding.ClientID, UserID: binding.UserID, ClientID: binding.ClientID, DatabaseKey: "multi-scope-provenance-" + binding.UserID + "-" + binding.ClientID}
	return &multiScopeProvenanceCall{Step: step, Client: client, CallID: string(*binding.CallID), Connect: payload, KnownScopeCount: uint64(len(payload.KnownScopes)), AssignedScopeCount: multiScopeProvenanceAssignedScopeCount(assignedScopes, binding.ClientID, payload), MeasuredScopeCount: measuredScopeCount}, nil
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
	if err := validateMultiScopeProvenancePublicBinding(step, call); err != nil {
		return nil, err
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil {
		return nil, err
	}
	if call == nil || !multiScopeProvenanceRebuildClientMatches(payload, call.Client) || payload.ClientGeneration <= 0 || payload.Schema.Version <= 0 || payload.Schema.Hash == "" || payload.Limit == 0 {
		return nil, fmt.Errorf("Kotlin Android multi-scope provenance rebuild %s binding is invalid", step.ID)
	}
	return &multiScopeProvenanceRebuild{ClientGeneration: payload.ClientGeneration, Schema: payload.Schema, ScopeID: payload.ScopeID, RebuildID: payload.RebuildID, Limit: payload.Limit, Begin: step}, nil
}

func bindMultiScopeProvenanceRebuildRequest(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if err := validateMultiScopeProvenancePublicBinding(step, call); err != nil {
		return err
	}
	if rebuild == nil || rebuild.Request.ID != "" {
		return fmt.Errorf("rebuild request %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) || payload.ClientGeneration <= 0 || payload.Schema.Version <= 0 || payload.Schema.Hash == "" || payload.CursorSource == "" || payload.ClientGeneration != rebuild.ClientGeneration || payload.Schema != rebuild.Schema {
		return fmt.Errorf("rebuild request %s is invalid", step.ID)
	}
	rebuild.Request = step
	return nil
}

func bindMultiScopeProvenanceRebuildApply(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if err := validateMultiScopeProvenancePublicBinding(step, call); err != nil {
		return err
	}
	if rebuild == nil || rebuild.Request.ID == "" || rebuild.Apply.ID != "" {
		return fmt.Errorf("rebuild apply %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) || payload.PageOrdinal == 0 || payload.RequestTokenSource == "" {
		return fmt.Errorf("rebuild apply %s is invalid", step.ID)
	}
	rebuild.Apply = step
	return nil
}

func bindMultiScopeProvenanceRebuildFinalize(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if err := validateMultiScopeProvenancePublicBinding(step, call); err != nil {
		return err
	}
	if rebuild == nil || rebuild.Apply.ID == "" || rebuild.Finalize.ID != "" {
		return fmt.Errorf("rebuild finalize %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) {
		return fmt.Errorf("rebuild finalize %s is invalid", step.ID)
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
				return 0, errors.New("Kotlin Android multi-scope provenance rebuild page sizes differ")
			}
		}
	}
	if pageSize == 0 || pageSize > 1000 {
		return 0, errors.New("Kotlin Android multi-scope provenance rebuild page size is invalid")
	}
	return int(pageSize), nil
}

func validateMultiScopeProvenanceCall(scenario scenarios.Scenario, call *multiScopeProvenanceCall, result SynchronizationResult, runtimeSchema schemaRef, appliedScopeSetVersion int64, appliedScopeCount int) error {
	if call == nil || result.Completion != "idle" {
		return errors.New("Kotlin Android multi-scope provenance call did not complete idle")
	}
	observations := result.transportObservations
	if len(observations) == 0 || observations[len(observations)-1].OperationClass != "pull" {
		return fmt.Errorf("call shape is invalid: %s", multiScopeProvenanceObservationSummary(observations))
	}
	cold := observations[0].OperationClass == "connect"
	var request *TransportRequestFacts
	if cold {
		request = observations[0].RequestFacts
		if request == nil || request.ProtocolVersion == nil || int64(*request.ProtocolVersion) != call.Connect.ProtocolVersion {
			return errors.New("connect request facts are invalid")
		}
	} else if len(observations) != 1 {
		return fmt.Errorf("warm call must carry one pull: %s", multiScopeProvenanceObservationSummary(observations))
	}
	pullRequest := observations[len(observations)-1].RequestFacts
	if pullRequest == nil {
		return errors.New("pull carries no request facts")
	}
	if pullRequest.SchemaVersion != runtimeSchema.Version || pullRequest.SchemaHash != runtimeSchema.Hash {
		return fmt.Errorf("pull schema differs from runtime schema: %s", multiScopeProvenanceObservationSummary(observations))
	}
	expectedScopeSetVersion := int64(call.Connect.ScopeSetVersion)
	expectedScopeCount := int(call.AssignedScopeCount)
	if !cold {
		expectedScopeSetVersion, expectedScopeCount = appliedScopeSetVersion, appliedScopeCount
	}
	if pullRequest.ScopeSetVersion == nil || *pullRequest.ScopeSetVersion != expectedScopeSetVersion {
		return fmt.Errorf("pull scope set version = %v, want %d", derefMultiScopeInt64(pullRequest.ScopeSetVersion), expectedScopeSetVersion)
	}
	if pullRequest.ScopeCount == nil || *pullRequest.ScopeCount != expectedScopeCount {
		return fmt.Errorf("pull scope count = %v, want %d", derefMultiScopeInt(pullRequest.ScopeCount), expectedScopeCount)
	}
	if cold && request.ClientGeneration != nil && call.Connect.ClientGeneration != nil && *request.ClientGeneration != *call.Connect.ClientGeneration {
		return errors.New("connect generation differs from authored operation")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable == nil || *observation.Retryable || observation.ErrorCode != nil {
			return errors.New("call contains an unsuccessful transport response")
		}
	}
	if err := validateKotlinWireObservation(scenario, string(call.Step.ID), observations[0]); err != nil {
		return err
	}
	rebuilds := make([]TransportObservation, 0, len(call.Rebuilds))
	if len(observations) >= 2 {
		for _, observation := range observations[1 : len(observations)-1] {
			if observation.OperationClass != "rebuild" {
				return errors.New("call has an unexpected transport operation")
			}
			rebuilds = append(rebuilds, observation)
		}
	}
	expectedRebuilds := len(call.Rebuilds)
	if cold && request.SchemaVersion == 0 {
		expectedRebuilds = int(call.AssignedScopeCount)
	}
	if expectedRebuilds < len(call.Rebuilds) {
		return errors.New("authored rebuild count exceeds assigned scope count")
	}
	if len(rebuilds) != expectedRebuilds {
		return fmt.Errorf("rebuild count = %d, want %d", len(rebuilds), expectedRebuilds)
	}
	bootstrapRebuilds := expectedRebuilds - len(call.Rebuilds)
	for index, rebuild := range call.Rebuilds {
		if err := validateMultiScopeProvenanceRebuildCall(scenario, rebuild, rebuilds[index+bootstrapRebuilds], runtimeSchema); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiScopeProvenanceRebuildCall(scenario scenarios.Scenario, rebuild *multiScopeProvenanceRebuild, observation TransportObservation, runtimeSchema schemaRef) error {
	if rebuild == nil || rebuild.Request.ID == "" || rebuild.Apply.ID == "" || rebuild.Finalize.ID == "" || observation.RequestFacts == nil || observation.RebuildResponseFacts == nil {
		return errors.New("rebuild evidence is incomplete")
	}
	payload, err := decodeMultiScopeProvenanceRebuild(rebuild.Request.Operation)
	if err != nil {
		return err
	}
	request := observation.RequestFacts
	if observation.OperationClass != "rebuild" || request.ClientGeneration == nil || *request.ClientGeneration != payload.ClientGeneration {
		return errors.New("rebuild class or client generation is invalid")
	}
	if request.SchemaVersion != runtimeSchema.Version || request.SchemaHash != runtimeSchema.Hash {
		return errors.New("rebuild schema differs from runtime schema")
	}
	if request.Limit == nil || uint64(*request.Limit) != payload.Limit {
		return errors.New("rebuild limit differs from authored limit")
	}
	if request.ScopeFingerprint == nil || *request.ScopeFingerprint == "" || request.RebuildIDFingerprint == nil || *request.RebuildIDFingerprint == "" {
		return errors.New("rebuild carries no scope or rebuild fingerprint")
	}
	if request.CursorPresent == nil || *request.CursorPresent {
		return errors.New("single-page rebuild must not carry a cursor")
	}
	response := observation.RebuildResponseFacts
	if response.HasMore || response.HasCursor || !response.HasFinalScopeCursor || !response.HasChecksum || response.FinalScopeCursorFingerprint == nil || response.ScopeFingerprint != *request.ScopeFingerprint {
		return errors.New("rebuild response does not prove finality")
	}
	return validateKotlinWireObservation(scenario, string(rebuild.Request.ID), observation)
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

func multiScopeProvenanceCallResult(plan multiScopeProvenancePlan, calls []SynchronizationResult, stepID scenarios.StepID) (SynchronizationResult, error) {
	for index, callID := range plan.CallOrder {
		if callID != stepID {
			continue
		}
		if index >= len(calls) {
			return SynchronizationResult{}, fmt.Errorf("call %s has no runtime result", stepID)
		}
		return calls[index], nil
	}
	return SynchronizationResult{}, fmt.Errorf("call %s is absent from the plan", stepID)
}

func resolveMultiScopeProvenanceIdentities(controller *blackbox.NativeController, plan multiScopeProvenancePlan, aliases []scenarios.NativeIdentityAlias, calls []SynchronizationResult, actual, server scenarios.StateFacts) (multiScopeProvenanceIdentityEvidence, error) {
	if len(aliases) == 0 || len(calls) == 0 || len(actual.Clients) == 0 {
		return multiScopeProvenanceIdentityEvidence{}, errors.New("Kotlin Android multi-scope provenance identity evidence is incomplete")
	}
	serverAliases := make([]scenarios.NativeIdentityAlias, 0, len(aliases))
	for _, alias := range aliases {
		switch alias.Kind {
		case "schema", "scope", "table", "primary-key", "batch-id", "mutation-id":
			serverAliases = append(serverAliases, alias)
		}
	}
	values, err := controller.IdentityValues(serverAliases)
	if err != nil {
		return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("read Kotlin Android multi-scope provenance controller identities: %w", err)
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	tableNames := make(map[string]string)
	authoredTables := make(map[string]string)
	for _, alias := range aliases {
		if alias.Kind != "table" {
			continue
		}
		var authored string
		if json.Unmarshal(alias.Value, &authored) == nil {
			authoredTables[alias.Alias] = authored
		}
	}
	for _, value := range values {
		if value.Alias == "" || len(value.RuntimeValue) == 0 {
			return multiScopeProvenanceIdentityEvidence{}, errors.New("controller identity is invalid")
		}
		if _, duplicate := runtime[value.Alias]; duplicate {
			return multiScopeProvenanceIdentityEvidence{}, errors.New("controller identity is duplicated")
		}
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
		if authored := authoredTables[value.Alias]; authored != "" && value.ApplicationIdentifier != "" {
			tableNames[authored] = value.ApplicationIdentifier
		}
	}
	if err := validateMultiScopeProvenanceRuntimeRebuilds(plan, calls, server); err != nil {
		return multiScopeProvenanceIdentityEvidence{}, err
	}
	generation, err := multiScopeProvenanceRuntimeClientGeneration(plan, calls, aliases)
	if err != nil {
		return multiScopeProvenanceIdentityEvidence{}, err
	}
	for _, alias := range aliases {
		var value any
		var found bool
		switch alias.Kind {
		case "client-generation":
			value, found = generation, true
		case "scope-set-version":
			value, err = multiScopeProvenanceRuntimeScopeSetVersion(plan, calls, alias)
			found = err == nil
		case "rebuild-id":
			value, err = multiScopeProvenanceRuntimeRebuildID(plan, server, alias)
			found = err == nil
		case "row-version", "checksum":
			var row scenarios.RowFact
			row, err = multiScopeProvenanceRuntimeRow(plan, server, alias)
			if err == nil {
				if alias.Kind == "row-version" {
					// A row fact reports the authored version, so the runtime
					// version comes from the versions the capture observed.
					runtimeVersion, bound := controller.RuntimeRowVersions()[row.CanonicalWireJSON]
					if !bound || runtimeVersion == "" {
						return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("Kotlin multi-scope provenance row alias %s has no runtime version", alias.Alias)
					}
					value = runtimeVersion
				} else {
					value = row.Checksum
				}
				found = true
			}
		}
		if !found {
			if err != nil {
				return multiScopeProvenanceIdentityEvidence{}, err
			}
			continue
		}
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("encode identity %s: %w", alias.Alias, marshalErr)
		}
		if existing, duplicate := runtime[alias.Alias]; duplicate && !reflect.DeepEqual(existing, encoded) {
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("identity %s has conflicting runtime values", alias.Alias)
		}
		runtime[alias.Alias] = encoded
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("alias %s has no runtime value", alias.Alias)
		}
	}
	resolutions, err := resolveKotlinNativeIdentities(aliases, runtime)
	if err != nil {
		return multiScopeProvenanceIdentityEvidence{}, err
	}
	operations := make([]string, 0, len(calls))
	for index, call := range calls {
		classes := make([]string, 0, len(call.transportObservations))
		for _, observation := range call.transportObservations {
			classes = append(classes, observation.OperationClass)
		}
		operations = append(operations, fmt.Sprintf("%d:%s", index, strings.Join(classes, "+")))
	}
	return multiScopeProvenanceIdentityEvidence{Resolutions: resolutions, Revocations: controller.AppliedScopeRevocations(), CallOperations: operations, TableNames: tableNames}, nil
}

func multiScopeProvenanceRuntimeClientGeneration(plan multiScopeProvenancePlan, calls []SynchronizationResult, aliases []scenarios.NativeIdentityAlias) (int64, error) {
	var anchor *scenarios.NativeIdentityAlias
	for index := range aliases {
		if aliases[index].Kind != "client-generation" {
			continue
		}
		if anchor != nil {
			return 0, errors.New("client-generation aliases are duplicated")
		}
		anchor = &aliases[index]
	}
	if anchor == nil || len(anchor.StepIDs) != 1 {
		return 0, errors.New("client-generation alias has no single anchor")
	}
	result, err := multiScopeProvenanceCallResult(plan, calls, anchor.StepIDs[0])
	if err != nil {
		return 0, err
	}
	var generation int64
	for _, observation := range result.transportObservations {
		if observation.RequestFacts != nil && observation.RequestFacts.ClientGeneration != nil && *observation.RequestFacts.ClientGeneration > 0 {
			generation = *observation.RequestFacts.ClientGeneration
			break
		}
	}
	if generation == 0 {
		return 0, fmt.Errorf("client-generation alias %s has no runtime value", anchor.Alias)
	}
	for _, call := range calls {
		for _, observation := range call.transportObservations {
			if observation.RequestFacts != nil && observation.RequestFacts.ClientGeneration != nil && *observation.RequestFacts.ClientGeneration != generation {
				return 0, errors.New("generation changed between calls")
			}
		}
	}
	return generation, nil
}

func multiScopeProvenanceRuntimeScopeSetVersion(plan multiScopeProvenancePlan, calls []SynchronizationResult, alias scenarios.NativeIdentityAlias) (uint64, error) {
	if len(alias.StepIDs) != 1 {
		return 0, fmt.Errorf("scope-set-version alias %s has no single anchor", alias.Alias)
	}
	result, err := multiScopeProvenanceCallResult(plan, calls, alias.StepIDs[0])
	if err != nil {
		return 0, err
	}
	if len(result.transportObservations) == 0 || result.transportObservations[0].RequestFacts == nil || result.transportObservations[0].RequestFacts.ScopeSetVersion == nil || *result.transportObservations[0].RequestFacts.ScopeSetVersion <= 0 {
		return 0, fmt.Errorf("scope-set-version alias %s has no runtime value", alias.Alias)
	}
	return uint64(*result.transportObservations[0].RequestFacts.ScopeSetVersion), nil
}

func validateMultiScopeProvenanceState(plan multiScopeProvenancePlan, server, actual scenarios.StateFacts, evidence multiScopeProvenanceIdentityEvidence) error {
	serverExpected := scenarios.CloneStateFacts(plan.Expected)
	serverExpected.Clients = nil
	if err := validateKotlinStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Kotlin Android multi-scope provenance server state differs: %w; rebuilds %s", err, multiScopeProvenanceRebuildSummary(server.Rebuilds))
	}
	if len(evidence.Resolutions) == 0 {
		return errors.New("Kotlin Android multi-scope provenance identity resolutions are absent")
	}
	expectedBase := multiScopeProvenanceClientProjection(plan.Expected)
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
	if err := validateKotlinStateProjection(expectedBase, actualBase); err != nil {
		observed := make([]string, 0)
		for _, client := range actual.Clients {
			for _, checkpoint := range client.Checkpoints {
				observed = append(observed, client.ClientID+":"+checkpoint.ScopeID)
			}
		}
		sort.Strings(observed)
		return fmt.Errorf("Kotlin Android multi-scope provenance client counts differ: %w; differences %v; revocations %v; checkpoints %v; calls %v",
			err, kotlinClientCountDifferences(expectedBase.Clients, actualBase.Clients), evidence.Revocations, observed, evidence.CallOperations)
	}
	resolved, err := kotlinResolutionMap(evidence.Resolutions)
	if err != nil {
		return err
	}
	actualClients := make(map[string]scenarios.ClientDurabilityFact, len(actual.Clients))
	for _, client := range actual.Clients {
		actualClients[client.UserID+"\x00"+client.ClientID] = client
	}
	for _, expectedClient := range plan.Expected.Clients {
		actualClient, found := actualClients[expectedClient.UserID+"\x00"+expectedClient.ClientID]
		if !found {
			return fmt.Errorf("Kotlin Android multi-scope provenance client %s is absent", expectedClient.ClientID)
		}
		if err := validateMultiScopeProvenanceProvenance(expectedClient.Provenance, actualClient.Provenance, resolved, evidence.TableNames); err != nil {
			return fmt.Errorf("Kotlin Android multi-scope provenance client %s: %w", expectedClient.ClientID, err)
		}
	}
	return nil
}

func validateMultiScopeProvenanceProvenance(expected, actual []scenarios.ProvenanceFact, resolutions map[string]blackbox.NativeIdentityResolution, tableNames map[string]string) error {
	if len(expected) != len(actual) {
		return errors.New("provenance count differs")
	}
	expected = append([]scenarios.ProvenanceFact(nil), expected...)
	actual = append([]scenarios.ProvenanceFact(nil), actual...)
	sort.Slice(expected, func(left, right int) bool {
		return expected[left].CanonicalWireJSON < expected[right].CanonicalWireJSON
	})
	sort.Slice(actual, func(left, right int) bool { return actual[left].CanonicalWireJSON < actual[right].CanonicalWireJSON })
	for index := range expected {
		want, got := expected[index], actual[index]
		unresolved := make([]string, 0, 4)
		if tableNames[want.TableID] != got.TableID {
			unresolved = append(unresolved, "table")
		}
		if !multiScopeProvenanceCanonicalIdentityMatches(resolutions, want.CanonicalWireJSON, got.CanonicalWireJSON) {
			unresolved = append(unresolved, "record")
		}
		if !multiScopeProvenanceScopesMatch(resolutions, want.Scopes, got.Scopes) {
			unresolved = append(unresolved, "scopes")
		}
		if !multiScopeProvenanceStringIdentityMatches(resolutions, want.Version, got.Version) {
			unresolved = append(unresolved, "version")
		}
		if len(unresolved) != 0 {
			return fmt.Errorf("provenance differs in %v: authored %s/%s/%v/%s observed %s/%s/%v/%s", unresolved, want.TableID, want.CanonicalWireJSON, want.Scopes, want.Version, got.TableID, got.CanonicalWireJSON, got.Scopes, got.Version)
		}
	}
	return nil
}

// multiScopeProvenanceCallMethod reports whether an authored call method drives
// a public call. A connect on a client that already runs declares start,
// because sync-now pulls without connecting.
func multiScopeProvenanceCallMethod(method string) bool {
	return method == "sync-now" || method == "start"
}

func multiScopeProvenanceCanonicalIdentityMatches(resolutions map[string]blackbox.NativeIdentityResolution, authored, runtime string) bool {
	for _, resolution := range resolutions {
		if kotlinResolutionMatchesCanonicalString(resolution, authored, runtime) {
			return true
		}
	}
	return false
}

func multiScopeProvenanceScopesMatch(resolutions map[string]blackbox.NativeIdentityResolution, authored, runtime []string) bool {
	if len(authored) != len(runtime) {
		return false
	}
	claimed := make([]bool, len(runtime))
	for _, authoredScope := range authored {
		paired := false
		for index, runtimeScope := range runtime {
			if claimed[index] || !multiScopeProvenanceStringIdentityMatches(resolutions, authoredScope, runtimeScope) {
				continue
			}
			claimed[index], paired = true, true
			break
		}
		if !paired {
			return false
		}
	}
	return true
}

func multiScopeProvenanceStringIdentityMatches(resolutions map[string]blackbox.NativeIdentityResolution, authored, runtime string) bool {
	for _, resolution := range resolutions {
		if kotlinResolutionMatchesString(resolution, authored, runtime) {
			return true
		}
	}
	return false
}

func validateMultiScopeProvenanceRuntimeRebuilds(plan multiScopeProvenancePlan, calls []SynchronizationResult, server scenarios.StateFacts) error {
	if len(calls) != len(plan.CallOrder) {
		return fmt.Errorf("runtime call count = %d, want %d", len(calls), len(plan.CallOrder))
	}
	for callIndex, callID := range plan.CallOrder {
		call := plan.Calls[callID]
		observations := calls[callIndex].transportObservations
		for _, rebuild := range call.Rebuilds {
			runtimeID, err := multiScopeProvenanceRuntimeRebuildIDForClientScope(server, call.Client, rebuild.ScopeID)
			if err != nil {
				return err
			}
			matches := 0
			for _, observation := range observations {
				if observation.OperationClass == "rebuild" && observation.RequestFacts != nil && observation.RequestFacts.RebuildIDFingerprint != nil && *observation.RequestFacts.RebuildIDFingerprint == cursorFingerprint(runtimeID) {
					matches++
				}
			}
			if matches != 1 {
				return fmt.Errorf("rebuild %s matched %d runtime rebuilds, want 1", rebuild.Begin.ID, matches)
			}
		}
	}
	return nil
}

func multiScopeProvenanceRuntimeRebuildID(plan multiScopeProvenancePlan, server scenarios.StateFacts, alias scenarios.NativeIdentityAlias) (string, error) {
	if len(alias.StepIDs) != 1 {
		return "", fmt.Errorf("rebuild alias %s has no single anchor", alias.Alias)
	}
	for _, callID := range plan.CallOrder {
		call := plan.Calls[callID]
		for _, rebuild := range call.Rebuilds {
			if rebuild.Begin.ID == alias.StepIDs[0] {
				return multiScopeProvenanceRuntimeRebuildIDForClientScope(server, call.Client, rebuild.ScopeID)
			}
		}
	}
	return "", fmt.Errorf("rebuild alias %s names no authored rebuild", alias.Alias)
}

func multiScopeProvenanceRuntimeRebuildIDForClientScope(server scenarios.StateFacts, client Client, scopeID string) (string, error) {
	var rebuildID string
	matches := 0
	for _, recorded := range server.Rebuilds {
		if recorded.UserID != client.UserID || recorded.ClientID != client.ClientID || recorded.ScopeID != scopeID {
			continue
		}
		if recorded.RebuildID == "" {
			return "", fmt.Errorf("server rebuild for client %s and scope %s has no identity", client.ClientID, scopeID)
		}
		matches++
		rebuildID = recorded.RebuildID
	}
	if matches != 1 {
		return "", fmt.Errorf("server rebuild for client %s and scope %s is ambiguous", client.ClientID, scopeID)
	}
	return rebuildID, nil
}

func multiScopeProvenanceRuntimeRow(plan multiScopeProvenancePlan, server scenarios.StateFacts, alias scenarios.NativeIdentityAlias) (scenarios.RowFact, error) {
	if len(alias.StepIDs) != 1 {
		return scenarios.RowFact{}, fmt.Errorf("row alias %s has no single anchor", alias.Alias)
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
		return scenarios.RowFact{}, fmt.Errorf("row alias %s has no server row", alias.Alias)
	}
	return scenarios.RowFact{}, fmt.Errorf("row alias %s names no authored commit", alias.Alias)
}

func multiScopeProvenanceRuntimeSchema(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias) (schemaRef, error) {
	schemaAliases := make([]scenarios.NativeIdentityAlias, 0, 1)
	for _, alias := range aliases {
		if alias.Kind == "schema" {
			schemaAliases = append(schemaAliases, alias)
		}
	}
	values, err := controller.IdentityValues(schemaAliases)
	if err != nil {
		return schemaRef{}, fmt.Errorf("resolve Kotlin Android multi-scope provenance schema: %w", err)
	}
	for _, value := range values {
		if value.Alias != "current-schema" {
			continue
		}
		var resolved schemaRef
		if json.Unmarshal(value.RuntimeValue, &resolved) != nil || resolved.Version <= 0 || resolved.Hash == "" {
			return schemaRef{}, errors.New("runtime schema is invalid")
		}
		return resolved, nil
	}
	return schemaRef{}, errors.New("scenario declares no current-schema alias")
}

func multiScopeProvenanceRebuildSummary(rebuilds []scenarios.RebuildFact) string {
	if len(rebuilds) == 0 {
		return "[]"
	}
	entries := make([]string, 0, len(rebuilds))
	for _, rebuild := range rebuilds {
		entries = append(entries, fmt.Sprintf("%s/%s/%s:%s", rebuild.UserID, rebuild.ClientID, rebuild.ScopeID, rebuild.Status))
	}
	sort.Strings(entries)
	return "[" + strings.Join(entries, " ") + "]"
}

func derefMultiScopeInt64(value *int64) any {
	if value == nil {
		return "none"
	}
	return *value
}

func derefMultiScopeInt(value *int) any {
	if value == nil {
		return "none"
	}
	return *value
}

func multiScopeProvenanceObservationSummary(observations []TransportObservation) string {
	entries := make([]string, 0, len(observations))
	for _, observation := range observations {
		entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
		if facts := observation.RequestFacts; facts != nil {
			entry += fmt.Sprintf(":schema=%v/%v:scopeSet=%v:scopes=%v", facts.SchemaVersion, facts.SchemaHash, derefMultiScopeInt64(facts.ScopeSetVersion), derefMultiScopeInt(facts.ScopeCount))
		}
		entries = append(entries, entry)
	}
	return strings.Join(entries, " ")
}

func multiScopeProvenanceAssignedScopeCount(assignedScopes map[string]uint64, clientID string, payload multiScopeProvenanceConnectPayload) uint64 {
	if assigned, found := assignedScopes[clientID]; found {
		return assigned
	}
	return uint64(len(payload.KnownScopes))
}

func multiScopeProvenanceClientProjection(facts scenarios.StateFacts) scenarios.StateFacts {
	projection := scenarios.CloneStateFacts(facts)
	projection.TransactionCount = nil
	projection.RowCount = nil
	projection.ScopeCount = nil
	projection.RebuildCount = nil
	projection.BatchCount = nil
	projection.MutationCount = nil
	projection.ConfiguredLimits = nil
	projection.Registry = nil
	projection.Stream = nil
	projection.Transactions = nil
	projection.Rows = nil
	projection.Scopes = nil
	projection.Poison = nil
	projection.Rebuilds = nil
	return projection
}

// kotlinClientCountDifferences names every client count that differs from the
// authored model. The projection comparison reports only that the state
// differs, and a reader cannot act on that alone.
func kotlinClientCountDifferences(expected, actual []scenarios.ClientDurabilityFact) []string {
	byClient := make(map[string]scenarios.ClientDurabilityFact, len(actual))
	for _, client := range actual {
		byClient[client.UserID+"/"+client.ClientID] = client
	}
	differences := make([]string, 0)
	for _, want := range expected {
		key := want.UserID + "/" + want.ClientID
		got, found := byClient[key]
		if !found {
			differences = append(differences, key+":absent")
			continue
		}
		delete(byClient, key)
		counts := []struct {
			name string
			want *uint64
			got  *uint64
		}{
			{"row", want.RowCount, got.RowCount},
			{"provenance", want.ProvenanceCount, got.ProvenanceCount},
			{"checkpoint", want.CheckpointCount, got.CheckpointCount},
			{"queue", want.QueueCount, got.QueueCount},
			{"outcome", want.OutcomeCount, got.OutcomeCount},
			{"sealed-batch", want.SealedBatchCount, got.SealedBatchCount},
			{"rebuild-attempt", want.RebuildAttemptCount, got.RebuildAttemptCount},
		}
		for _, count := range counts {
			if count.want == nil {
				continue
			}
			if count.got == nil {
				differences = append(differences, fmt.Sprintf("%s:%s want %d got none", key, count.name, *count.want))
				continue
			}
			if *count.want != *count.got {
				differences = append(differences, fmt.Sprintf("%s:%s want %d got %d", key, count.name, *count.want, *count.got))
			}
		}
	}
	for key := range byClient {
		differences = append(differences, key+":unexpected")
	}
	sort.Strings(differences)
	return differences
}
