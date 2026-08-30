package swift

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
	Step            scenarios.Step
	Client          Client
	CallID          string
	Connect         multiScopeProvenanceConnectPayload
	KnownScopeCount uint64
	// AssignedScopeCount is the scope count the authored assignments grant this
	// client. A pull carries the assigned set, not the connect known set.
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
	Resolutions []blackbox.NativeIdentityResolution
}

// RunMultiScopeProvenanceScenario executes the authored multi-scope provenance flow through Swift.
func RunMultiScopeProvenanceScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, artifact *blackbox.NativeArtifact, platform *Platform) (MultiScopeProvenanceResult, error) {
	if controller == nil || artifact == nil || platform == nil {
		return MultiScopeProvenanceResult{}, errors.New("Swift multi-scope provenance dependencies are unavailable")
	}
	modelScenario, err := multiScopeProvenanceModelScenario(scenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	modelResult, err := modelrunner.RunScenario(ctx, modelScenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("derive Swift multi-scope provenance source operations from the authored model: %w", err)
	}
	if err := validateMultiScopeProvenanceModelResult(scenario, modelResult); err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	plan, err := multiScopeProvenancePlanForScenario(scenario)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	if len(scenario.Model.Setup) == 0 {
		return MultiScopeProvenanceResult{}, errors.New("Swift multi-scope provenance setup is absent")
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("install Swift multi-scope provenance contract: %w", err)
	}

	// The setup declares every client local_ready with its assigned scope, and
	// the reference model asserts each connect adds no scope and leaves cursors
	// unchanged. A client the scenario authors no rebuild for must therefore
	// reach that state before the exercise begins. A client the scenario does
	// author rebuilds for starts empty and rebuilds during the exercise exactly
	// as those steps declare.
	authoredRebuilds := multiScopeProvenanceAuthoredRebuildCounts(plan)
	setupRebuilds := make(map[string]bool)
	started := make(map[string]bool, len(plan.Clients))
	// A request reports the assignment set the client has already applied.
	appliedScopeSetVersions := make(map[string]int64, len(plan.Clients))
	appliedScopeCounts := make(map[string]int, len(plan.Clients))
	calls := make([]SynchronizationResult, 0, len(plan.CallOrder))
	for index, step := range plan.Steps {
		modelOperation := modelResult.Steps[index].Operation
		switch scenarios.OperationKey(modelOperation) {
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/set-client-assignments":
			if step.Transport != "model" || step.NativeBinding == nil || step.NativeBinding.Kind != "controller" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s controller binding is invalid", step.ID)
			}
			observation, applyErr := controller.ApplyStep(ctx, modelOperation)
			if applyErr != nil || observation.Disposition != "success" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("apply Swift multi-scope provenance step %s: %w", step.ID, resultError(applyErr, observation.Disposition))
			}
		case "process/materialize-source-transaction":
			if step.Transport != "process" || step.NativeBinding == nil || step.NativeBinding.Kind != "controller" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s process binding is invalid", step.ID)
			}
			observation, processErr := controller.ProcessStep(ctx, nil, modelOperation)
			if processErr != nil || observation.Disposition != "success" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("materialize Swift multi-scope provenance step %s: %w", step.ID, resultError(processErr, observation.Disposition))
			}
		case "connect/send":
			if step.Transport != "http" || step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s connect binding is invalid", step.ID)
			}
			call := plan.Calls[step.ID]
			if call == nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance call %s is absent", step.ID)
			}
			method := call.Step.NativeBinding.Method
			if !started[call.Client.Key] {
				started[call.Client.Key] = true
				// A client the scenario authors no rebuild for must reach the
				// declared local_ready state before its authored call. That
				// establishment creates its own rebuild session, which belongs to
				// the setup and not to the authored steps.
				if installErr := platform.Install(ctx, call.Client, "empty", ""); installErr != nil {
					return MultiScopeProvenanceResult{}, fmt.Errorf("install Swift multi-scope provenance client %s: %w", call.Client.ClientID, installErr)
				}
				if authoredRebuilds[call.Client.Key] == 0 {
					before, captureErr := multiScopeProvenanceRebuildIdentities(ctx, controller, plan)
					if captureErr != nil {
						return MultiScopeProvenanceResult{}, captureErr
					}
					establishment, establishErr := swiftScenarioCall(ctx, platform, call.Client, "start")
					if establishErr != nil {
						return MultiScopeProvenanceResult{}, fmt.Errorf("establish Swift multi-scope provenance client %s: %w", call.Client.ClientID, establishErr)
					}
					if establishment.Completion != "idle" {
						return MultiScopeProvenanceResult{}, fmt.Errorf("establish Swift multi-scope provenance client %s reached %s", call.Client.ClientID, establishment.Completion)
					}
					after, captureErr := multiScopeProvenanceRebuildIdentities(ctx, controller, plan)
					if captureErr != nil {
						return MultiScopeProvenanceResult{}, captureErr
					}
					for identity := range after {
						if !before[identity] {
							setupRebuilds[identity] = true
						}
					}
					// Establishment brings the client to the declared initial
					// state, so its authored call reports the authored assignment
					// set rather than an empty one.
					appliedScopeSetVersions[call.Client.ClientID] = int64(call.Connect.ScopeSetVersion)
					appliedScopeCounts[call.Client.ClientID] = int(call.AssignedScopeCount)
				} else {
					method = "start"
				}
			}
			result, callErr := swiftScenarioCall(ctx, platform, call.Client, method)
			if callErr != nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("run Swift multi-scope provenance client %s: %w", call.Client.ClientID, callErr)
			}
			// Each membership activation publishes a new manifest, so the server
			// schema advances during the scenario. Resolve it per call instead of
			// once, or a later call is compared against a stale schema.
			runtimeSchema, schemaErr := multiScopeProvenanceRuntimeSchema(controller, scenario.NativeIdentityAliases)
			if schemaErr != nil {
				return MultiScopeProvenanceResult{}, schemaErr
			}
			if err := validateMultiScopeProvenanceCall(scenario, call, result, runtimeSchema, appliedScopeSetVersions[call.Client.ClientID], appliedScopeCounts[call.Client.ClientID]); err != nil {
				return MultiScopeProvenanceResult{}, fmt.Errorf("validate Swift multi-scope provenance client %s: %w", call.Client.ClientID, err)
			}
			appliedScopeSetVersions[call.Client.ClientID] = int64(call.Connect.ScopeSetVersion)
			appliedScopeCounts[call.Client.ClientID] = int(call.AssignedScopeCount)
			calls = append(calls, result)
		case "local/begin-rebuild", "local/apply-rebuild-page", "local/finalize-rebuild":
			if step.Transport != "local" || step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s local binding is invalid", step.ID)
			}
		case "rebuild/request-page":
			// A rebuild page request is an HTTP operation. The surrounding begin,
			// apply, and finalize steps are local client operations.
			if step.Transport != "http" || step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" {
				return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s rebuild request binding is invalid", step.ID)
			}
		default:
			return MultiScopeProvenanceResult{}, fmt.Errorf("Swift multi-scope provenance step %s has unsupported operation %s", step.ID, scenarios.OperationKey(step.Operation))
		}
	}
	if len(calls) != len(plan.CallOrder) || len(started) != len(plan.Clients) {
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
	if err != nil {
		return MultiScopeProvenanceResult{}, fmt.Errorf("capture Swift multi-scope provenance server state: %w", err)
	}
	if len(serverCaptures) != 1 {
		return MultiScopeProvenanceResult{}, fmt.Errorf("capture Swift multi-scope provenance server state returned %d captures, want 1", len(serverCaptures))
	}
	serverState := multiScopeProvenanceExerciseState(serverCaptures[0].StateFacts, setupRebuilds)
	evidence, err := resolveMultiScopeProvenanceIdentities(controller, plan, scenario.NativeIdentityAliases, calls, actualClient, serverState)
	if err != nil {
		return MultiScopeProvenanceResult{}, err
	}
	if err := validateMultiScopeProvenanceState(plan, serverState, actualClient, evidence); err != nil {
		return MultiScopeProvenanceResult{}, err
	}

	return MultiScopeProvenanceResult{
		Calls:              calls,
		ClientFacts:        clientFacts,
		ServerFacts:        serverState,
		IdentityResolution: evidence.Resolutions,
	}, nil
}

func validateMultiScopeProvenanceModelResult(scenario scenarios.Scenario, result modelrunner.Result) error {
	if !result.Passed || len(result.Setup) != 1 || len(result.Steps) != len(scenario.Steps) {
		return errors.New("authored multi-scope provenance model did not close all workload steps")
	}
	if !reflect.DeepEqual(result.Setup[0].Operation, scenario.Model.Setup[0]) {
		return errors.New("authored multi-scope provenance model setup differs from the authored setup")
	}
	for index, authoredStep := range scenario.Steps {
		modelStep := result.Steps[index]
		if modelStep.StepID != authoredStep.ID {
			return fmt.Errorf("authored multi-scope provenance model step %s is bound to %s", authoredStep.ID, modelStep.StepID)
		}
		if !reflect.DeepEqual(modelStep.Operation, authoredStep.Operation) {
			return fmt.Errorf("authored multi-scope provenance model operation for step %s differs from the authored operation", authoredStep.ID)
		}
		if modelStep.Err != nil {
			return fmt.Errorf("authored multi-scope provenance model step %s returned an error: %w", authoredStep.ID, modelStep.Err)
		}
	}
	return nil
}

func multiScopeProvenanceModelScenario(scenario scenarios.Scenario) (scenarios.Scenario, error) {
	modelScenario := scenario
	modelScenario.Model.ExpectedState = append([]scenarios.ModelExpectation(nil), scenario.Model.ExpectedState...)
	// This scenario binds performance samples to native connect calls, not to
	// model workload/prepare operations. The native consumer validates those
	// samples, so the reference model evaluates only its semantic expectations.
	expectations := make([]scenarios.ModelExpectation, 0, len(modelScenario.Model.ExpectedState))
	for _, expectation := range modelScenario.Model.ExpectedState {
		if expectation.Predicate.Name == "performance-contract-satisfied" {
			continue
		}
		expectations = append(expectations, expectation)
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
	steps, err := swiftScenarioStepMap(scenario, multiScopeProvenanceScenarioID, len(scenario.Steps))
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
	stages := make(map[uint64]bool)
	assignedScopes := make(map[string]uint64)
	callIDs := make(map[string]scenarios.StepID)
	var currentCall *multiScopeProvenanceCall
	var currentRebuild *multiScopeProvenanceRebuild
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" {
			return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s does not expect success", step.ID)
		}
		if err := scenarios.ValidateOperation(step.Operation); err != nil {
			return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s operation is invalid: %w", step.ID, err)
		}
		key := scenarios.OperationKey(step.Operation)
		switch key {
		case "model/commit-source-transaction":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall = nil
			currentRebuild = nil
			payload, decodeErr := decodeMultiScopeProvenanceCommit(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, decodeErr)
			}
			commitKey := payload.StreamGeneration + "\x00" + payload.CommitLSN
			if _, duplicate := commits[commitKey]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance commit %s is duplicated", payload.CommitLSN)
			}
			commits[commitKey] = false
			plan.TransactionCount++
			for _, event := range payload.Events {
				if event.Relation == "" || event.Operation == "" || event.After == nil || event.After.Identity.Kind == "" {
					return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance commit %s event is invalid", payload.CommitLSN)
				}
				switch event.After.Identity.Kind {
				case "synced":
					plan.SyncedEventCount++
				case "capture_dependency":
					plan.CaptureEventCount++
				default:
					return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance commit %s event identity is unsupported", payload.CommitLSN)
				}
			}
		case "process/materialize-source-transaction":
			if err := validateMultiScopeProvenanceControllerBinding(step, "process"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
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
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall = nil
			currentRebuild = nil
			payload, decodeErr := decodeMultiScopeProvenanceStage(step.Operation)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, decodeErr)
			}
			if _, duplicate := stages[payload.RegistryGeneration]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance registry generation %d is duplicated", payload.RegistryGeneration)
			}
			stages[payload.RegistryGeneration] = false
		case "model/activate-registry-membership-generation":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall = nil
			currentRebuild = nil
			var payload multiScopeProvenanceActivatePayload
			if decodeErr := json.Unmarshal(step.Operation.Payload, &payload); decodeErr != nil || payload.RegistryGeneration == 0 {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance activation %s is invalid", step.ID)
			}
			activated, staged := stages[payload.RegistryGeneration]
			if !staged || activated {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance activation %d has no staged generation", payload.RegistryGeneration)
			}
			stages[payload.RegistryGeneration] = true
		case "model/set-client-assignments":
			if err := validateMultiScopeProvenanceControllerBinding(step, "model"); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			currentCall = nil
			currentRebuild = nil
			if err := validateMultiScopeProvenanceAssignment(step.Operation); err != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s: %w", step.ID, err)
			}
			var assignment multiScopeProvenanceAssignmentPayload
			if err := json.Unmarshal(step.Operation.Payload, &assignment); err != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance step %s assignment is invalid", step.ID)
			}
			assignedScopes[assignment.ClientID] = uint64(len(assignment.Assignments))
		case "connect/send":
			if err := validateMultiScopeProvenancePublicBinding(step, nil); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			if currentRebuild != nil {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance call %s begins before a rebuild closes", step.ID)
			}
			call, decodeErr := newMultiScopeProvenanceCall(step, assignedScopes)
			if decodeErr != nil {
				return multiScopeProvenancePlan{}, decodeErr
			}
			if _, duplicate := plan.Calls[step.ID]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance call %s is duplicated", step.ID)
			}
			if previous, duplicate := callIDs[call.CallID]; duplicate {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance call ID %s is used by steps %s and %s", call.CallID, previous, step.ID)
			}
			callIDs[call.CallID] = step.ID
			plan.Calls[step.ID] = call
			plan.CallOrder = append(plan.CallOrder, step.ID)
			if existing, found := plan.Clients[call.Client.Key]; found && existing != call.Client {
				return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance client %s is inconsistent", call.Client.ClientID)
			}
			plan.Clients[call.Client.Key] = call.Client
			currentCall = call
		case "local/begin-rebuild":
			if err := validateMultiScopeProvenancePublicBinding(step, currentCall); err != nil {
				return multiScopeProvenancePlan{}, err
			}
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
			if err := validateMultiScopeProvenancePublicBinding(step, currentCall); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			if err := bindMultiScopeProvenanceRebuildRequest(step, currentCall, currentRebuild); err != nil {
				return multiScopeProvenancePlan{}, err
			}
		case "local/apply-rebuild-page":
			if err := validateMultiScopeProvenancePublicBinding(step, currentCall); err != nil {
				return multiScopeProvenancePlan{}, err
			}
			if err := bindMultiScopeProvenanceRebuildApply(step, currentCall, currentRebuild); err != nil {
				return multiScopeProvenancePlan{}, err
			}
		case "local/finalize-rebuild":
			if err := validateMultiScopeProvenancePublicBinding(step, currentCall); err != nil {
				return multiScopeProvenancePlan{}, err
			}
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
	for generation, activated := range stages {
		if !activated {
			return multiScopeProvenancePlan{}, fmt.Errorf("Swift multi-scope provenance registry generation %d was not activated", generation)
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

func validateMultiScopeProvenanceControllerBinding(step scenarios.Step, transport string) error {
	if step.Transport != transport || step.NativeBinding == nil || step.NativeBinding.Kind != "controller" {
		return fmt.Errorf("Swift multi-scope provenance step %s controller binding is invalid", step.ID)
	}
	return nil
}

func validateMultiScopeProvenancePublicBinding(step scenarios.Step, call *multiScopeProvenanceCall) error {
	binding := step.NativeBinding
	if (step.Transport != "http" && step.Transport != "local") || binding == nil || binding.Kind != "public-call" || binding.UserID == "" || binding.ClientID == "" || binding.CallID == nil || *binding.CallID == "" || binding.Stage != "synchronous" || binding.Method != "sync-now" || binding.Completion != "idle" {
		return fmt.Errorf("Swift multi-scope provenance step %s public binding is invalid", step.ID)
	}
	if call != nil && (binding.UserID != call.Client.UserID || binding.ClientID != call.Client.ClientID || string(*binding.CallID) != call.CallID) {
		return fmt.Errorf("Swift multi-scope provenance step %s does not belong to public call %s", step.ID, call.CallID)
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
	binding := step.NativeBinding
	if err := validateMultiScopeProvenancePublicBinding(step, nil); err != nil {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s binding is invalid", step.ID)
	}
	var payload multiScopeProvenanceConnectPayload
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID || payload.RuntimeVersion != 3 || payload.ProtocolVersion != 3 || payload.Schema.Version <= 0 || payload.Schema.Hash == "" || payload.ScopeSetVersion == 0 || len(payload.KnownScopes) == 0 {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s payload is invalid", step.ID)
	}
	if payload.ClientGeneration != nil && *payload.ClientGeneration <= 0 {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s client generation is invalid", step.ID)
	}
	seenScopes := make(map[string]struct{}, len(payload.KnownScopes))
	for _, scope := range payload.KnownScopes {
		if scope.ScopeID == "" {
			return nil, fmt.Errorf("Swift multi-scope provenance call %s scope is invalid", step.ID)
		}
		if _, duplicate := seenScopes[scope.ScopeID]; duplicate {
			return nil, fmt.Errorf("Swift multi-scope provenance call %s scope is duplicated", step.ID)
		}
		seenScopes[scope.ScopeID] = struct{}{}
	}
	measuredScopeCount, err := multiScopeProvenanceMeasuredScopeCount(step)
	if err != nil {
		return nil, fmt.Errorf("Swift multi-scope provenance call %s: %w", step.ID, err)
	}
	return &multiScopeProvenanceCall{
		Step:               step,
		Client:             Client{Key: "multi-scope-provenance-" + binding.UserID + "-" + binding.ClientID, UserID: binding.UserID, ClientID: binding.ClientID, DatabaseKey: "multi-scope-provenance-" + binding.UserID + "-" + binding.ClientID},
		CallID:             string(*binding.CallID),
		Connect:            payload,
		KnownScopeCount:    uint64(len(payload.KnownScopes)),
		AssignedScopeCount: multiScopeProvenanceAssignedScopeCount(assignedScopes, binding.ClientID, payload),
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
	if err := validateMultiScopeProvenancePublicBinding(step, call); err != nil {
		return nil, err
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil {
		return nil, fmt.Errorf("Swift multi-scope provenance rebuild %s: %w", step.ID, err)
	}
	if call == nil || !multiScopeProvenanceRebuildClientMatches(payload, call.Client) || payload.ClientGeneration <= 0 || payload.Schema.Version <= 0 || payload.Schema.Hash == "" || payload.Limit == 0 {
		return nil, fmt.Errorf("Swift multi-scope provenance rebuild %s binding is invalid", step.ID)
	}
	return &multiScopeProvenanceRebuild{ClientGeneration: payload.ClientGeneration, Schema: payload.Schema, ScopeID: payload.ScopeID, RebuildID: payload.RebuildID, Limit: payload.Limit, Begin: step}, nil
}

func bindMultiScopeProvenanceRebuildRequest(step scenarios.Step, call *multiScopeProvenanceCall, rebuild *multiScopeProvenanceRebuild) error {
	if rebuild == nil || rebuild.Request.ID != "" {
		return fmt.Errorf("Swift multi-scope provenance rebuild request %s is out of order", step.ID)
	}
	payload, err := decodeMultiScopeProvenanceRebuild(step.Operation)
	if err != nil || !multiScopeProvenanceRebuildMatches(payload, call, rebuild) || payload.ClientGeneration <= 0 || payload.Schema.Version <= 0 || payload.Schema.Hash == "" || payload.CursorSource == "" || payload.ClientGeneration != rebuild.ClientGeneration || payload.Schema != rebuild.Schema {
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

func validateMultiScopeProvenanceCall(scenario scenarios.Scenario, call *multiScopeProvenanceCall, result SynchronizationResult, runtimeSchema schemaRef, appliedScopeSetVersion int64, appliedScopeCount int) error {
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
		steps := make([]string, 0, len(result.Steps))
		for _, step := range result.Steps {
			entry := step.Disposition
			if step.ErrorCode != nil {
				entry += ":" + *step.ErrorCode
			}
			if step.Completion != "" {
				entry += ":" + step.Completion
			}
			steps = append(steps, entry)
		}
		return fmt.Errorf("Swift multi-scope provenance call did not complete: completion %s observations %v steps %v", completion, outcomes, steps)
	}
	observations := result.transportObservations
	if len(observations) == 0 || observations[len(observations)-1].OperationClass != "pull" {
		return fmt.Errorf("Swift multi-scope provenance call shape is invalid: observed %s", multiScopeProvenanceObservationSummary(observations))
	}
	// A client with no usable cursor bootstraps with connect, one rebuild for
	// each assigned scope, then pull. A client that already holds a usable
	// cursor pulls directly. The scenario connects one client twice.
	cold := observations[0].OperationClass == "connect"
	var request *transportRequestFacts
	if cold {
		request = observations[0].RequestFacts
		if request == nil {
			return errors.New("Swift multi-scope provenance connect carries no request facts")
		}
		if request.ProtocolVersion == nil || int64(*request.ProtocolVersion) != call.Connect.ProtocolVersion {
			return fmt.Errorf("Swift multi-scope provenance connect protocol version = %v, want %d", derefInt(request.ProtocolVersion), call.Connect.ProtocolVersion)
		}
	} else if len(observations) != 1 {
		return fmt.Errorf("Swift multi-scope provenance warm call must carry one pull: observed %s", multiScopeProvenanceObservationSummary(observations))
	}
	// An empty client bootstraps with connect, rebuild, then pull. The bootstrap
	// connect carries no local schema, so the authored schema, scope set version,
	// and scope count appear on the pull that closes the call.
	pull := observations[len(observations)-1]
	pullRequest := pull.RequestFacts
	if pullRequest == nil {
		return errors.New("Swift multi-scope provenance pull carries no request facts")
	}
	if pullRequest.SchemaVersion != runtimeSchema.Version || pullRequest.SchemaHash != runtimeSchema.Hash {
		return fmt.Errorf("Swift multi-scope provenance pull schema = %v/%v, want %v/%v; observed %s", pullRequest.SchemaVersion, pullRequest.SchemaHash, runtimeSchema.Version, runtimeSchema.Hash, multiScopeProvenanceObservationSummary(observations))
	}
	// A request carries the last locally applied assignment set. A cold call
	// connects first, so its pull already carries the authored version. A warm
	// call learns the transition from the pull response itself, so its request
	// still carries the version the client applied before this call.
	expectedScopeSetVersion := int64(call.Connect.ScopeSetVersion)
	if !cold {
		expectedScopeSetVersion = appliedScopeSetVersion
	}
	if pullRequest.ScopeSetVersion == nil || *pullRequest.ScopeSetVersion != expectedScopeSetVersion {
		return fmt.Errorf("Swift multi-scope provenance pull scope set version = %v, want %d; observed %s", derefInt64(pullRequest.ScopeSetVersion), expectedScopeSetVersion, multiScopeProvenanceObservationSummary(observations))
	}
	// The scope count follows the same rule as the assignment version. A warm
	// call reports the set the client applied before the pull that delivers the
	// transition.
	expectedScopeCount := int(call.AssignedScopeCount)
	if !cold {
		expectedScopeCount = appliedScopeCount
	}
	if pullRequest.ScopeCount == nil || *pullRequest.ScopeCount != expectedScopeCount {
		return fmt.Errorf("Swift multi-scope provenance pull scope count = %v, want %d; observed %s", derefInt(pullRequest.ScopeCount), expectedScopeCount, multiScopeProvenanceObservationSummary(observations))
	}
	if cold && request.ClientGeneration != nil && call.Connect.ClientGeneration != nil && *request.ClientGeneration != *call.Connect.ClientGeneration {
		return errors.New("Swift multi-scope provenance connect generation differs from the authored operation")
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
	// A warm call carries only the pull, so it has no intermediate observations.
	middle := []transportObservation(nil)
	if len(observations) >= 2 {
		middle = observations[1 : len(observations)-1]
	}
	for _, observation := range middle {
		if observation.OperationClass != "rebuild" {
			return errors.New("Swift multi-scope provenance call has an unexpected transport operation")
		}
		rebuilds = append(rebuilds, observation)
	}
	// An empty client rebuilds once for each scope it must materialize before its
	// first pull. The authored rebuild steps describe those rebuilds. A scope the
	// scenario does not author is protocol machinery, so it is excluded from the
	// authored comparison.
	expectedRebuilds := len(call.Rebuilds)
	if cold && request.SchemaVersion == 0 {
		expectedRebuilds = int(call.AssignedScopeCount)
	}
	if expectedRebuilds < len(call.Rebuilds) {
		return fmt.Errorf("Swift multi-scope provenance authors %d rebuilds for %d assigned scopes", len(call.Rebuilds), call.AssignedScopeCount)
	}
	if len(rebuilds) != expectedRebuilds {
		return fmt.Errorf("Swift multi-scope provenance rebuild count = %d, want %d; observed %s", len(rebuilds), expectedRebuilds, multiScopeProvenanceObservationSummary(observations))
	}
	bootstrapRebuilds := expectedRebuilds - len(call.Rebuilds)
	for index, rebuild := range call.Rebuilds {
		if err := validateMultiScopeProvenanceRebuildCall(scenario, rebuild, rebuilds[index+bootstrapRebuilds], runtimeSchema); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiScopeProvenanceRebuildCall(scenario scenarios.Scenario, rebuild *multiScopeProvenanceRebuild, observation transportObservation, runtimeSchema schemaRef) error {
	if rebuild == nil || rebuild.Request.ID == "" || rebuild.Apply.ID == "" || rebuild.Finalize.ID == "" || observation.RequestFacts == nil || observation.RebuildResponseFacts == nil {
		return errors.New("Swift multi-scope provenance rebuild evidence is incomplete")
	}
	payload, err := decodeMultiScopeProvenanceRebuild(rebuild.Request.Operation)
	if err != nil {
		return err
	}
	request := observation.RequestFacts
	if observation.OperationClass != "rebuild" {
		return fmt.Errorf("Swift multi-scope provenance rebuild class = %s, want rebuild", observation.OperationClass)
	}
	if request.ClientGeneration == nil || *request.ClientGeneration != payload.ClientGeneration {
		return fmt.Errorf("Swift multi-scope provenance rebuild client generation = %v, want %d", derefInt64(request.ClientGeneration), payload.ClientGeneration)
	}
	// The authored schema is an alias value. A Class 3 transition advances the
	// server schema during this scenario, so compare against the resolved schema.
	if request.SchemaVersion != runtimeSchema.Version || request.SchemaHash != runtimeSchema.Hash {
		return fmt.Errorf("Swift multi-scope provenance rebuild schema = %v/%v, want %v/%v", request.SchemaVersion, request.SchemaHash, runtimeSchema.Version, runtimeSchema.Hash)
	}
	if request.Limit == nil || uint64(*request.Limit) != payload.Limit {
		return fmt.Errorf("Swift multi-scope provenance rebuild limit = %v, want %d", derefInt(request.Limit), payload.Limit)
	}
	// The client mints its own rebuild identifier. The authored value is a
	// symbolic alias that identity resolution binds to the runtime rebuild.
	if request.ScopeFingerprint == nil || *request.ScopeFingerprint == "" || request.RebuildIDFingerprint == nil || *request.RebuildIDFingerprint == "" {
		return errors.New("Swift multi-scope provenance rebuild carries no scope or rebuild fingerprint")
	}
	if request.CursorPresent == nil || *request.CursorPresent {
		return errors.New("Swift multi-scope provenance rebuild must not carry a cursor")
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

func multiScopeProvenanceCallResult(plan multiScopeProvenancePlan, calls []SynchronizationResult, stepID scenarios.StepID) (SynchronizationResult, error) {
	for index, callID := range plan.CallOrder {
		if callID != stepID {
			continue
		}
		if index >= len(calls) {
			return SynchronizationResult{}, fmt.Errorf("Swift multi-scope provenance call %s has no runtime result", stepID)
		}
		return calls[index], nil
	}
	return SynchronizationResult{}, fmt.Errorf("Swift multi-scope provenance call %s is absent from the plan", stepID)
}

func resolveMultiScopeProvenanceIdentities(controller *blackbox.NativeController, plan multiScopeProvenancePlan, aliases []scenarios.NativeIdentityAlias, calls []SynchronizationResult, actual, server scenarios.StateFacts) (multiScopeProvenanceIdentityEvidence, error) {
	if len(aliases) == 0 || len(calls) == 0 || len(actual.Clients) == 0 {
		return multiScopeProvenanceIdentityEvidence{}, errors.New("Swift multi-scope provenance identity evidence is incomplete")
	}
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
			row, rowErr := multiScopeProvenanceRuntimeRow(plan, server, alias)
			if rowErr == nil {
				if alias.Kind == "row-version" {
					value = row.Version
				} else {
					value = row.Checksum
				}
				found = true
			} else {
				err = rowErr
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
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("encode Swift multi-scope provenance identity %s: %w", alias.Alias, marshalErr)
		}
		if existing, duplicate := runtime[alias.Alias]; duplicate && !reflect.DeepEqual(existing, encoded) {
			return multiScopeProvenanceIdentityEvidence{}, fmt.Errorf("Swift multi-scope provenance identity %s has conflicting runtime values", alias.Alias)
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

func multiScopeProvenanceRuntimeClientGeneration(plan multiScopeProvenancePlan, calls []SynchronizationResult, aliases []scenarios.NativeIdentityAlias) (int64, error) {
	var anchor *scenarios.NativeIdentityAlias
	for index := range aliases {
		if aliases[index].Kind != "client-generation" {
			continue
		}
		if anchor != nil {
			return 0, errors.New("Swift multi-scope provenance client-generation aliases are duplicated")
		}
		anchor = &aliases[index]
	}
	if anchor == nil {
		return 0, errors.New("Swift multi-scope provenance client-generation alias is absent")
	}
	if len(anchor.StepIDs) != 1 {
		return 0, fmt.Errorf("Swift multi-scope provenance client-generation alias %s has no single anchor step", anchor.Alias)
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
		return 0, fmt.Errorf("Swift multi-scope provenance client-generation alias %s has no runtime value", anchor.Alias)
	}
	for _, call := range calls {
		for _, observation := range call.transportObservations {
			if observation.RequestFacts != nil && observation.RequestFacts.ClientGeneration != nil && *observation.RequestFacts.ClientGeneration != generation {
				return 0, errors.New("Swift multi-scope provenance generation changed between calls")
			}
		}
	}
	return generation, nil
}

func multiScopeProvenanceRuntimeScopeSetVersion(plan multiScopeProvenancePlan, calls []SynchronizationResult, alias scenarios.NativeIdentityAlias) (uint64, error) {
	if len(alias.StepIDs) != 1 {
		return 0, fmt.Errorf("Swift multi-scope provenance scope-set-version alias %s has no single anchor step", alias.Alias)
	}
	if _, found := plan.Calls[alias.StepIDs[0]]; !found {
		return 0, fmt.Errorf("Swift multi-scope provenance scope-set-version alias %s names no connect step", alias.Alias)
	}
	result, err := multiScopeProvenanceCallResult(plan, calls, alias.StepIDs[0])
	if err != nil {
		return 0, err
	}
	if len(result.transportObservations) == 0 || result.transportObservations[0].RequestFacts == nil || result.transportObservations[0].RequestFacts.ScopeSetVersion == nil || *result.transportObservations[0].RequestFacts.ScopeSetVersion <= 0 {
		return 0, fmt.Errorf("Swift multi-scope provenance scope-set-version alias %s has no runtime value", alias.Alias)
	}
	return uint64(*result.transportObservations[0].RequestFacts.ScopeSetVersion), nil
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
	// The client capture carries client families only. Compare it against the
	// authored client families, exactly as the server comparison drops them.
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

func validateMultiScopeProvenanceRuntimeRebuilds(plan multiScopeProvenancePlan, calls []SynchronizationResult, server scenarios.StateFacts) error {
	if len(calls) != len(plan.CallOrder) {
		return fmt.Errorf("Swift multi-scope provenance runtime call count = %d, want %d", len(calls), len(plan.CallOrder))
	}
	for callIndex, callID := range plan.CallOrder {
		call := plan.Calls[callID]
		observations := calls[callIndex].transportObservations
		for _, rebuild := range call.Rebuilds {
			runtimeID, err := multiScopeProvenanceRuntimeRebuildIDForClientScope(server, call.Client, rebuild.ScopeID)
			if err != nil {
				return err
			}
			// The client chooses the order in which it rebuilds its scopes, so
			// match each authored rebuild to the observation carrying that scope's
			// server rebuild identity instead of assuming the authored order.
			matches := 0
			for _, observation := range observations {
				if observation.OperationClass != "rebuild" {
					continue
				}
				request := observation.RequestFacts
				if request != nil && request.RebuildIDFingerprint != nil && *request.RebuildIDFingerprint == cursorFingerprint(runtimeID) {
					matches++
				}
			}
			if matches != 1 {
				return fmt.Errorf("Swift multi-scope provenance rebuild %s matched %d runtime rebuilds for scope %s, want 1", rebuild.Begin.ID, matches, rebuild.ScopeID)
			}
		}
	}
	return nil
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
			return multiScopeProvenanceRuntimeRebuildIDForClientScope(server, call.Client, rebuild.ScopeID)
		}
	}
	return "", fmt.Errorf("Swift multi-scope provenance rebuild alias %s names no authored rebuild", alias.Alias)
}

func multiScopeProvenanceRuntimeRebuildIDForClientScope(server scenarios.StateFacts, client Client, scopeID string) (string, error) {
	var rebuildID string
	matches := 0
	for _, recorded := range server.Rebuilds {
		if recorded.UserID != client.UserID || recorded.ClientID != client.ClientID || recorded.ScopeID != scopeID {
			continue
		}
		if recorded.RebuildID == "" {
			return "", fmt.Errorf("Swift multi-scope provenance server rebuild for client %s and scope %s has no identity", client.ClientID, scopeID)
		}
		matches++
		rebuildID = recorded.RebuildID
	}
	if matches != 1 {
		return "", fmt.Errorf("Swift multi-scope provenance server rebuild for client %s and scope %s is ambiguous", client.ClientID, scopeID)
	}
	return rebuildID, nil
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

// Report an absent optional request fact as "none" so a divergence names the
// observed value instead of a pointer.
func derefInt64(value *int64) any {
	if value == nil {
		return "none"
	}
	return *value
}

func derefInt(value *int) any {
	if value == nil {
		return "none"
	}
	return *value
}

// Summarize an observed call so a divergence names the whole exchange. An empty
// client bootstraps with connect, rebuild, then pull, so the authored schema
// appears only after the bootstrap connect.
func multiScopeProvenanceObservationSummary(observations []transportObservation) string {
	entries := make([]string, 0, len(observations))
	for _, observation := range observations {
		entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
		if facts := observation.RequestFacts; facts != nil {
			entry += fmt.Sprintf(":schema=%v/%v:scopeSet=%v:scopes=%v",
				facts.SchemaVersion, facts.SchemaHash,
				derefInt64(facts.ScopeSetVersion), derefInt(facts.ScopeCount))
		}
		entries = append(entries, entry)
	}
	return strings.Join(entries, " ")
}

// The authored schema hash is an alias value, not a literal to assert. The
// server owns the runtime schema, so resolve the alias before comparing any
// observation against it.
func multiScopeProvenanceRuntimeSchema(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias) (schemaRef, error) {
	// Only the schema alias binds before the scenario runs. Row and batch aliases
	// bind to values the exercise creates, so resolving them here would fail.
	schemaAliases := make([]scenarios.NativeIdentityAlias, 0, 1)
	for _, alias := range aliases {
		if alias.Kind == "schema" {
			schemaAliases = append(schemaAliases, alias)
		}
	}
	values, err := controller.IdentityValues(schemaAliases)
	if err != nil {
		return schemaRef{}, fmt.Errorf("resolve Swift multi-scope provenance schema identity: %w", err)
	}
	for _, value := range values {
		if value.Alias != "current-schema" {
			continue
		}
		var resolved schemaRef
		if json.Unmarshal(value.RuntimeValue, &resolved) != nil || resolved.Version <= 0 || resolved.Hash == "" {
			return schemaRef{}, errors.New("Swift multi-scope provenance runtime schema is invalid")
		}
		return resolved, nil
	}
	return schemaRef{}, errors.New("Swift multi-scope provenance scenario declares no current-schema alias")
}

// multiScopeProvenanceAssignedScopeCount reports the scope count the authored
// assignments grant a client. A client with no authored assignment keeps the
// scope set its connect declares.
func multiScopeProvenanceAssignedScopeCount(assignedScopes map[string]uint64, clientID string, payload multiScopeProvenanceConnectPayload) uint64 {
	if assigned, found := assignedScopes[clientID]; found {
		return assigned
	}
	return uint64(len(payload.KnownScopes))
}

// multiScopeProvenanceAuthoredRebuildCounts reports how many rebuilds the
// scenario authors for each client.
func multiScopeProvenanceAuthoredRebuildCounts(plan multiScopeProvenancePlan) map[string]int {
	counts := make(map[string]int, len(plan.Clients))
	for _, callID := range plan.CallOrder {
		if call := plan.Calls[callID]; call != nil {
			counts[call.Client.Key] += len(call.Rebuilds)
		}
	}
	return counts
}

// multiScopeProvenanceRebuildIdentities reports the rebuild identities the
// server holds now.
func multiScopeProvenanceRebuildIdentities(ctx context.Context, controller *blackbox.NativeController, plan multiScopeProvenancePlan) (map[string]bool, error) {
	keys := make([]string, 0, len(plan.Clients))
	for _, callID := range plan.CallOrder {
		if call := plan.Calls[callID]; call != nil {
			keys = append(keys, call.Client.Key)
		}
	}
	captures, err := controller.Capture(ctx, keys, []string{"server-state"})
	if err != nil {
		return nil, fmt.Errorf("capture Swift multi-scope provenance rebuild identities: %w", err)
	}
	if len(captures) != 1 {
		return nil, fmt.Errorf("capture Swift multi-scope provenance rebuild identities returned %d captures, want 1", len(captures))
	}
	identities := make(map[string]bool, len(captures[0].StateFacts.Rebuilds))
	for _, rebuild := range captures[0].StateFacts.Rebuilds {
		identities[rebuild.RebuildID] = true
	}
	return identities, nil
}

// multiScopeProvenanceExerciseState removes the setup rebuilds so the observed
// facts describe the authored steps.
func multiScopeProvenanceExerciseState(facts scenarios.StateFacts, setupRebuilds map[string]bool) scenarios.StateFacts {
	if len(setupRebuilds) == 0 {
		return facts
	}
	retained := make([]scenarios.RebuildFact, 0, len(facts.Rebuilds))
	for _, rebuild := range facts.Rebuilds {
		if setupRebuilds[rebuild.RebuildID] {
			continue
		}
		retained = append(retained, rebuild)
	}
	facts.Rebuilds = retained
	count := uint64(len(retained))
	facts.RebuildCount = &count
	return facts
}

// multiScopeProvenanceClientProjection keeps the authored client families and
// drops the server families a client capture never reports.
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
