package nativeexecution

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

type nativePlan struct {
	clients []scenarios.NativeClient
	actions []Action
}

type nativeClientIdentity struct {
	userID   string
	clientID string
}

// deriveNativePlan translates the surviving step bindings into the closed
// driver actions. It does not run the reference model or modelrunner.
func deriveNativePlan(scenario scenarios.Scenario, obligation scenarios.ProofObligation) (nativePlan, error) {
	if obligation.ProofType != "native-e2e" {
		return nativePlan{}, fmt.Errorf("obligation %s is not native-e2e", obligation.ObligationID)
	}
	if len(scenario.Model.Setup) != 1 {
		return nativePlan{}, fmt.Errorf("scenario %s has %d model setup operations, want one", scenario.ID, len(scenario.Model.Setup))
	}

	identities := make(map[nativeClientIdentity]string)
	clients := make([]scenarios.NativeClient, 0)
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding == nil {
			return nativePlan{}, fmt.Errorf("scenario step %s has no native binding", step.ID)
		}
		if binding.Kind == "controller" {
			continue
		}
		if binding.UserID == "" || binding.ClientID == "" {
			return nativePlan{}, fmt.Errorf("scenario step %s native binding has no client identity", step.ID)
		}
		identity := nativeClientIdentity{userID: binding.UserID, clientID: binding.ClientID}
		if _, found := identities[identity]; found {
			continue
		}
		key := fmt.Sprintf("client-%03d", len(clients)+1)
		identities[identity] = key
		clients = append(clients, scenarios.NativeClient{
			Key:         key,
			UserID:      identity.userID,
			ClientID:    identity.clientID,
			DatabaseKey: fmt.Sprintf("database-%03d", len(clients)+1),
		})
	}
	if len(clients) == 0 {
		return nativePlan{}, fmt.Errorf("scenario %s has no native client binding", scenario.ID)
	}

	plan := nativePlan{clients: clients}
	add := func(phase, actor, command string, stepIDs []scenarios.StepID, parameters any) error {
		encoded, err := json.Marshal(parameters)
		if err != nil {
			return fmt.Errorf("encode derived %s/%s parameters: %w", actor, command, err)
		}
		plan.actions = append(plan.actions, Action{
			ID:            scenarios.NativeActionID(fmt.Sprintf("NDRV-%03d", len(plan.actions)+1)),
			Phase:         phase,
			Actor:         actor,
			Command:       command,
			CoversStepIDs: append([]scenarios.StepID(nil), stepIDs...),
			Parameters:    encoded,
		})
		return nil
	}
	if err := add("setup", "controller", "install-model", nil, struct{}{}); err != nil {
		return nativePlan{}, err
	}

	opened := make(map[string]bool, len(clients))
	seedSteps := make(map[string]scenarios.StepID, len(clients))
	ensureOpen := func(phase, key string) error {
		if opened[key] {
			return nil
		}
		parameters := scenarios.NativeClientOpenParameters{
			ClientKey:      key,
			DatabaseMode:   "create",
			Initialization: "empty",
		}
		if seedStepID, found := seedSteps[key]; found {
			parameters.DatabaseMode = "reuse"
			parameters.Initialization = "seed"
			parameters.SeedStepID = &seedStepID
		}
		if err := add(phase, "client", "open", nil, parameters); err != nil {
			return err
		}
		opened[key] = true
		return nil
	}
	clientKey := func(binding *scenarios.NativeStepBinding) (string, error) {
		key, found := identities[nativeClientIdentity{userID: binding.UserID, clientID: binding.ClientID}]
		if !found {
			return "", fmt.Errorf("native binding client %q/%q is not derived", binding.UserID, binding.ClientID)
		}
		return key, nil
	}

	for index := 0; index < len(scenario.Steps); index++ {
		step := scenario.Steps[index]
		binding := step.NativeBinding
		if binding == nil {
			return nativePlan{}, fmt.Errorf("scenario step %s has no native binding", step.ID)
		}
		if binding.Kind == "public-call" && binding.Stage == "synchronous" {
			key, err := clientKey(binding)
			if err != nil {
				return nativePlan{}, err
			}
			if err := ensureOpen(step.Phase, key); err != nil {
				return nativePlan{}, err
			}
			group := []scenarios.StepID{step.ID}
			for index+1 < len(scenario.Steps) {
				next := scenario.Steps[index+1]
				nextBinding := next.NativeBinding
				if nextBinding == nil || nextBinding.Kind != "public-call" || nextBinding.Stage != "synchronous" || nextBinding.CallID == nil || binding.CallID == nil || *nextBinding.CallID != *binding.CallID {
					break
				}
				group = append(group, next.ID)
				index++
			}
			if err := add(step.Phase, "client", "synchronize-step", group, scenarios.NativeSynchronizeParameters{ClientKey: key, Method: binding.Method, Completion: binding.Completion}); err != nil {
				return nativePlan{}, err
			}
			if err := addLifecycleActions(scenario.NativeLifecycleBoundaries, group[len(group)-1], key, add); err != nil {
				return nativePlan{}, err
			}
			continue
		}

		switch binding.Kind {
		case "controller":
			command := "apply-step"
			if step.Transport == "http" {
				command = "request-step"
			}
			if err := add(step.Phase, "controller", command, []scenarios.StepID{step.ID}, struct{}{}); err != nil {
				return nativePlan{}, err
			}
		case "artifact":
			key, err := clientKey(binding)
			if err != nil {
				return nativePlan{}, err
			}
			if err := add(step.Phase, "artifact", "stage-step", []scenarios.StepID{step.ID}, scenarios.NativeClientParameters{ClientKey: key}); err != nil {
				return nativePlan{}, err
			}
			seedSteps[key] = step.ID
		case "local-write":
			key, err := clientKey(binding)
			if err != nil {
				return nativePlan{}, err
			}
			if err := ensureOpen(step.Phase, key); err != nil {
				return nativePlan{}, err
			}
			if err := add(step.Phase, "client", "execute-step", []scenarios.StepID{step.ID}, scenarios.NativeClientParameters{ClientKey: key}); err != nil {
				return nativePlan{}, err
			}
		case "process":
			key, err := clientKey(binding)
			if err != nil {
				return nativePlan{}, err
			}
			if err := ensureOpen(step.Phase, key); err != nil {
				return nativePlan{}, err
			}
			if err := add(step.Phase, "process", "execute-step", []scenarios.StepID{step.ID}, scenarios.NativeProcessStepParameters{ClientKey: &key}); err != nil {
				return nativePlan{}, err
			}
		case "public-call":
			key, err := clientKey(binding)
			if err != nil {
				return nativePlan{}, err
			}
			if err := ensureOpen(step.Phase, key); err != nil {
				return nativePlan{}, err
			}
			if binding.CallID == nil {
				return nativePlan{}, fmt.Errorf("scenario step %s public call has no call ID", step.ID)
			}
			switch binding.Stage {
			case "begin":
				err = add(step.Phase, "client", "begin-call", []scenarios.StepID{step.ID}, scenarios.NativeBeginCallParameters{ClientKey: key, CallID: *binding.CallID, Method: binding.Method})
			case "await-step":
				err = add(step.Phase, "observer", "await-step", []scenarios.StepID{step.ID}, scenarios.NativeAwaitStepParameters{ClientKey: key, CallID: binding.CallID})
			case "await-call":
				err = add(step.Phase, "observer", "await-step", []scenarios.StepID{step.ID}, scenarios.NativeAwaitStepParameters{ClientKey: key, CallID: binding.CallID})
				if err == nil {
					err = add(step.Phase, "client", "await-call", nil, scenarios.NativeAwaitCallParameters{ClientKey: key, CallID: *binding.CallID, Completion: binding.Completion})
				}
			default:
				err = fmt.Errorf("scenario step %s has unsupported public call stage %q", step.ID, binding.Stage)
			}
			if err != nil {
				return nativePlan{}, err
			}
		default:
			return nativePlan{}, fmt.Errorf("scenario step %s has unsupported native binding kind %q", step.ID, binding.Kind)
		}
		key := ""
		if binding.Kind != "controller" {
			key, _ = clientKey(binding)
		}
		if err := addLifecycleActions(scenario.NativeLifecycleBoundaries, step.ID, key, add); err != nil {
			return nativePlan{}, err
		}
	}

	expectationIDs, err := obligationExpectationIDs(scenario, obligation)
	if err != nil {
		return nativePlan{}, err
	}
	sources := captureSources(scenario, expectationIDs)
	if len(sources) != 0 {
		keys := make([]string, len(clients))
		for index, client := range clients {
			keys[index] = client.Key
		}
		if err := add("verify", "observer", "capture", nil, scenarios.NativeCaptureParameters{ClientKeys: keys, Sources: sources, ExpectationIDs: expectationIDs}); err != nil {
			return nativePlan{}, err
		}
	}
	if len(obligation.PerformanceBudgetIDs) != 0 || len(obligation.RequiredMeasurementIDs) != 0 {
		if err := add("verify", "observer", "measure", nil, scenarios.NativeMeasureParameters{PerformanceBudgetIDs: append([]contract.BudgetID(nil), obligation.PerformanceBudgetIDs...), MeasurementIDs: append([]contract.MeasurementID(nil), obligation.RequiredMeasurementIDs...)}); err != nil {
			return nativePlan{}, err
		}
	}
	return plan, nil
}

func addLifecycleActions(boundaries []scenarios.NativeLifecycleBoundary, stepID scenarios.StepID, clientKey string, add func(string, string, string, []scenarios.StepID, any) error) error {
	for _, boundary := range boundaries {
		if boundary.AfterStepID != stepID {
			continue
		}
		if clientKey == "" {
			return fmt.Errorf("native lifecycle boundary %q follows a client-free step", boundary.ID)
		}
		if err := add(boundary.Phase, "client", "lifecycle", nil, scenarios.NativeLifecycleParameters{ClientKey: clientKey, Operation: boundary.Method}); err != nil {
			return err
		}
	}
	return nil
}

func obligationExpectationIDs(scenario scenarios.Scenario, obligation scenarios.ProofObligation) ([]scenarios.ExpectationID, error) {
	assertions := make(map[scenarios.ExpectationID]struct{})
	byID := make(map[string]scenarios.Assertion, len(scenario.Assertions))
	for _, assertion := range scenario.Assertions {
		byID[string(assertion.ID)] = assertion
	}
	for _, assertionID := range obligation.AssertionIDs {
		assertion, found := byID[string(assertionID)]
		if !found {
			return nil, fmt.Errorf("native obligation %s has unknown assertion %s", obligation.ObligationID, assertionID)
		}
		for _, expectationID := range assertion.ExpectationIDs {
			assertions[expectationID] = struct{}{}
		}
	}
	if len(assertions) == 0 {
		return nil, fmt.Errorf("native obligation %s has no expectation", obligation.ObligationID)
	}
	ids := make([]scenarios.ExpectationID, 0, len(assertions))
	for id := range assertions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	return ids, nil
}

func captureSources(scenario scenarios.Scenario, expectationIDs []scenarios.ExpectationID) []string {
	wanted := make(map[scenarios.ExpectationID]struct{}, len(expectationIDs))
	for _, expectationID := range expectationIDs {
		wanted[expectationID] = struct{}{}
	}
	byID := make(map[scenarios.ExpectationID]scenarios.ModelExpectation, len(scenario.Model.ExpectedState))
	for _, expectation := range scenario.Model.ExpectedState {
		byID[expectation.ID] = expectation
	}
	sources := make(map[string]struct{})
	for expectationID := range wanted {
		expectation := byID[expectationID]
		switch expectation.Predicate.ContractPredicate {
		case "wire-outcome":
			sources["request-trace"] = struct{}{}
		case "state-transition":
			sources["sync-events"] = struct{}{}
		case "performance-measurement":
			sources["sync-status"] = struct{}{}
		case "state-equality":
			if expectation.StateFacts == nil {
				sources["sync-status"] = struct{}{}
				continue
			}
			facts := expectation.StateFacts
			if facts.TransactionCount != nil || facts.RowCount != nil || facts.ScopeCount != nil || facts.RebuildCount != nil || facts.BatchCount != nil || facts.MutationCount != nil || facts.ConfiguredLimits != nil || facts.Transactions != nil || facts.Registry != nil || facts.Stream != nil || facts.Rows != nil || facts.Scopes != nil || facts.Poison != nil || facts.Rebuilds != nil {
				sources["server-state"] = struct{}{}
			}
			for _, client := range facts.Clients {
				if client.CurrentSchema != nil || client.SealedBatchCount != nil {
					sources["sync-status"] = struct{}{}
				}
				if client.RowCount != nil {
					sources["application-rows"] = struct{}{}
				}
				if client.ProvenanceCount != nil || client.Provenance != nil {
					sources["provenance"] = struct{}{}
				}
				if client.CheckpointCount != nil || client.Checkpoints != nil {
					sources["checkpoints"] = struct{}{}
				}
				if client.QueueCount != nil || client.Queue != nil {
					sources["pending-mutations"] = struct{}{}
				}
				if client.OutcomeCount != nil || client.Outcomes != nil {
					sources["rejected-mutations"] = struct{}{}
				}
				if client.RebuildAttemptCount != nil {
					sources["rebuild-state"] = struct{}{}
				}
			}
		}
	}
	result := make([]string, 0, len(sources))
	for source := range sources {
		result = append(result, source)
	}
	sort.Strings(result)
	return result
}

func measurementSamplesByStep(scenario scenarios.Scenario, required []contract.MeasurementID) map[scenarios.StepID]scenarios.MeasurementSample {
	samples := make(map[scenarios.StepID]scenarios.MeasurementSample)
	requiredIDs := make(map[contract.MeasurementID]struct{}, len(required))
	for _, measurementID := range required {
		requiredIDs[measurementID] = struct{}{}
	}
	for _, step := range scenario.Steps {
		if step.MeasurementSample != nil {
			if _, wanted := requiredIDs[step.MeasurementSample.MeasurementID]; !wanted {
				continue
			}
			samples[step.ID] = *step.MeasurementSample
		}
	}
	for _, binding := range scenario.MeasurementBindings {
		if _, wanted := requiredIDs[binding.MeasurementSample.MeasurementID]; !wanted {
			continue
		}
		if _, found := samples[binding.StepID]; !found {
			samples[binding.StepID] = binding.MeasurementSample
		}
	}
	return samples
}
