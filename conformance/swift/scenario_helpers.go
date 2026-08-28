package swift

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func swiftScenarioStepMap(scenario scenarios.Scenario, id string, count int) (map[scenarios.StepID]scenarios.Step, error) {
	if string(scenario.ID) != id {
		return nil, fmt.Errorf("Swift scenario %s is not %s", scenario.ID, id)
	}
	if len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return nil, errors.New("Swift scenario model setup is invalid")
	}
	if len(scenario.Steps) != count {
		return nil, fmt.Errorf("Swift scenario %s has %d steps, want %d", scenario.ID, len(scenario.Steps), count)
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil {
			return nil, fmt.Errorf("Swift scenario step %s has no native binding", step.ID)
		}
		if _, duplicate := steps[step.ID]; duplicate {
			return nil, fmt.Errorf("Swift scenario step %s is duplicated", step.ID)
		}
		steps[step.ID] = step
	}
	return steps, nil
}

func swiftScenarioExpectedState(scenario scenarios.Scenario, id string) (scenarios.StateFacts, error) {
	for _, expectation := range scenario.Model.ExpectedState {
		if string(expectation.ID) == id && expectation.StateFacts != nil {
			return *expectation.StateFacts, nil
		}
	}
	return scenarios.StateFacts{}, fmt.Errorf("Swift expected state %s is absent", id)
}

func swiftScenarioOperation(steps map[scenarios.StepID]scenarios.Step, id, key string) (scenarios.Operation, error) {
	step, found := steps[scenarios.StepID(id)]
	if !found {
		return scenarios.Operation{}, fmt.Errorf("Swift scenario step %s is absent", id)
	}
	if scenarios.OperationKey(step.Operation) != key {
		return scenarios.Operation{}, fmt.Errorf("Swift scenario step %s operation is %s, want %s", id, scenarios.OperationKey(step.Operation), key)
	}
	return step.Operation, nil
}

func swiftScenarioClient(step scenarios.Step, client Client) error {
	binding := step.NativeBinding
	if binding == nil || binding.UserID != client.UserID || binding.ClientID != client.ClientID {
		return fmt.Errorf("Swift scenario step %s client identity does not match", step.ID)
	}
	return nil
}

func swiftScenarioCall(ctx context.Context, platform *Platform, client Client, method string) (SynchronizationResult, error) {
	if ctx == nil {
		return SynchronizationResult{}, errors.New("Swift scenario call context is required")
	}
	state, err := platform.client(client)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.terminated || state.session == nil || state.pendingLoss != nil || state.activeCall != nil {
		return SynchronizationResult{}, errors.New("Swift scenario client is unavailable for public call")
	}
	before, err := captureRunner(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	started := time.Now()
	completed, observations, err := runCallToCompletion(ctx, state, platform.nextCallID(state), method)
	if err != nil {
		return SynchronizationResult{}, err
	}
	after, err := captureRunner(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	window, err := windowFromResults(started, before, after, observations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state.started = true
	return synchronizationResult(completed.Completion, nil, window), nil
}

func swiftScenarioWire(result SynchronizationResult, operationClass string) (transportObservation, error) {
	for _, observation := range result.transportObservations {
		if observation.OperationClass == operationClass {
			return observation, nil
		}
	}
	return transportObservation{}, fmt.Errorf("Swift scenario call has no %s transport observation", operationClass)
}

func validateSwiftWireExpectation(scenario scenarios.Scenario, stepID, operationClass string, result SynchronizationResult) error {
	var expected *scenarios.WireExpectation
	for index := range scenario.WireExpectations {
		if scenario.WireExpectations[index].StepID == scenarios.StepID(stepID) {
			expected = &scenario.WireExpectations[index]
			break
		}
	}
	if expected == nil {
		return fmt.Errorf("Swift wire expectation %s is absent", stepID)
	}
	observed, err := swiftScenarioWire(result, operationClass)
	if err != nil {
		return err
	}
	if observed.StatusCode != expected.HTTPStatus || observed.Retryable != expected.Retryable || !equalOptionalStrings(observed.ErrorCode, expected.ErrorCode) {
		return fmt.Errorf("Swift wire result %s differs from its authored expectation", stepID)
	}
	return nil
}

func equalOptionalStrings(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func mergeSwiftStateFacts(values ...scenarios.StateFacts) (scenarios.StateFacts, error) {
	var merged scenarios.StateFacts
	clients := make(map[string]int)
	for _, value := range values {
		if value.TransactionCount != nil {
			merged.TransactionCount = value.TransactionCount
		}
		if value.RowCount != nil {
			merged.RowCount = value.RowCount
		}
		if value.ScopeCount != nil {
			merged.ScopeCount = value.ScopeCount
		}
		if value.RebuildCount != nil {
			merged.RebuildCount = value.RebuildCount
		}
		if value.BatchCount != nil {
			merged.BatchCount = value.BatchCount
		}
		if value.MutationCount != nil {
			merged.MutationCount = value.MutationCount
		}
		if value.ConfiguredLimits != nil {
			merged.ConfiguredLimits = value.ConfiguredLimits
		}
		if value.Registry != nil {
			merged.Registry = value.Registry
		}
		if value.Stream != nil {
			merged.Stream = value.Stream
		}
		merged.Transactions = append(merged.Transactions, value.Transactions...)
		merged.Rows = append(merged.Rows, value.Rows...)
		merged.Scopes = append(merged.Scopes, value.Scopes...)
		merged.Poison = append(merged.Poison, value.Poison...)
		merged.Rebuilds = append(merged.Rebuilds, value.Rebuilds...)
		for _, client := range value.Clients {
			key := client.UserID + "\x00" + client.ClientID
			if index, found := clients[key]; found {
				merged.Clients[index] = mergeSwiftClientFacts(merged.Clients[index], client)
			} else {
				clients[key] = len(merged.Clients)
				merged.Clients = append(merged.Clients, client)
			}
		}
	}
	normalized, err := scenarios.NormalizeStateFacts(merged)
	if err != nil {
		return scenarios.StateFacts{}, fmt.Errorf("normalize merged Swift state facts: %w", err)
	}
	return normalized, nil
}

func mergeSwiftClientFacts(left, right scenarios.ClientDurabilityFact) scenarios.ClientDurabilityFact {
	if right.CurrentSchema != nil {
		left.CurrentSchema = right.CurrentSchema
	}
	if right.RowCount != nil {
		left.RowCount = right.RowCount
	}
	if right.ProvenanceCount != nil {
		left.ProvenanceCount = right.ProvenanceCount
	}
	if right.CheckpointCount != nil {
		left.CheckpointCount = right.CheckpointCount
	}
	if right.QueueCount != nil {
		left.QueueCount = right.QueueCount
	}
	if right.OutcomeCount != nil {
		left.OutcomeCount = right.OutcomeCount
	}
	if right.SealedBatchCount != nil {
		left.SealedBatchCount = right.SealedBatchCount
	}
	if right.RebuildAttemptCount != nil {
		left.RebuildAttemptCount = right.RebuildAttemptCount
	}
	if right.Provenance != nil {
		left.Provenance = append(left.Provenance, right.Provenance...)
	}
	if right.Checkpoints != nil {
		left.Checkpoints = append(left.Checkpoints, right.Checkpoints...)
	}
	if right.Queue != nil {
		left.Queue = append(left.Queue, right.Queue...)
	}
	if right.Outcomes != nil {
		left.Outcomes = append(left.Outcomes, right.Outcomes...)
	}
	return left
}

func validateSwiftStateProjection(expected, actual scenarios.StateFacts) error {
	normalizedExpected, err := scenarios.NormalizeStateFacts(expected)
	if err != nil {
		return fmt.Errorf("normalize expected Swift state facts: %w", err)
	}
	normalizedActual, err := scenarios.NormalizeStateFacts(actual)
	if err != nil {
		return fmt.Errorf("normalize actual Swift state facts: %w", err)
	}
	if !scenarios.StateFactsProjectionEqual(normalizedExpected, normalizedActual) {
		return errors.New("Swift state differs from the authored model")
	}
	return nil
}
