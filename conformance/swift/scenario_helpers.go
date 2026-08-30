package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
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
	state.started = completed.Completion == "idle"
	return synchronizationResult(completed.Completion, nil, window), nil
}

func (p *Platform) captureSnapshot(ctx context.Context, client Client) (runnerResult, error) {
	state, err := p.client(client)
	if err != nil {
		return runnerResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.terminated || state.session == nil {
		return runnerResult{}, errors.New("Swift client is unavailable for inspection")
	}
	return captureRunner(ctx, state)
}

func resolveSwiftNativeIdentities(aliases []scenarios.NativeIdentityAlias, runtime map[string]json.RawMessage) ([]blackbox.NativeIdentityResolution, error) {
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range aliases {
		value := runtime[alias.Alias]
		for _, id := range alias.StepIDs {
			stepID := id
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &stepID, RuntimeValue: append(json.RawMessage(nil), value...)})
		}
		for _, id := range alias.ExpectationIDs {
			expectationID := id
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &expectationID, RuntimeValue: append(json.RawMessage(nil), value...)})
		}
	}
	return blackbox.ResolveNativeIdentityAliases(aliases, observations)
}

func completedSwiftRebuildID(events []eventRecord, scopeID string) (string, error) {
	var rebuildID string
	matches := 0
	for _, event := range events {
		if event.Type != "rebuild_completed" {
			continue
		}
		if event.ScopeID == nil || event.RebuildID == nil || *event.ScopeID == "" || *event.RebuildID == "" {
			return "", errors.New("Swift completed rebuild event is invalid")
		}
		if *event.ScopeID == scopeID {
			matches++
			rebuildID = *event.RebuildID
		}
	}
	if matches != 1 {
		return "", errors.New("Swift completed rebuild identity is ambiguous")
	}
	return rebuildID, nil
}

func validateCompletedEmptyRebuildReceipt(receipt rebuildReceiptRecord, rebuildID string, pageCount int) bool {
	return receipt.RebuildIDFingerprint == cursorFingerprint(rebuildID) &&
		receipt.PageCount == pageCount &&
		receipt.ReturnedRecordCount == 0 &&
		reflect.DeepEqual(receipt.RequestChainExpected, receipt.RequestChainObserved) &&
		sortedUniqueStrings(receipt.RecordIdentitiesHex) &&
		reflect.DeepEqual(receipt.ReceivedRowChecksums, receipt.ComputedRowChecksums) &&
		receipt.ComputedScopeChecksum != nil &&
		receipt.FinalScopeChecksum != nil &&
		*receipt.ComputedScopeChecksum == *receipt.FinalScopeChecksum
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
	observed, err := swiftScenarioWire(result, operationClass)
	if err != nil {
		return err
	}
	return validateSwiftWireObservation(scenario, stepID, observed)
}

func validateSwiftWireObservation(scenario scenarios.Scenario, stepID string, observed transportObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.StatusCode != expected.HTTPStatus || observed.Retryable != expected.Retryable || !equalOptionalStrings(observed.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Swift wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Swift wire expectation %s is absent", stepID)
}

// validateSwiftBaselineCallShape reports whether a call performed the cold
// bootstrap. A client with no usable cursor connects, rebuilds each scope, and
// pulls, so its first call carries more than one request.
func validateSwiftBaselineCallShape(result SynchronizationResult) bool {
	observations := result.transportObservations
	if result.Completion != "idle" || len(observations) < 3 {
		return false
	}
	if observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return false
	}
	for _, observation := range observations[1 : len(observations)-1] {
		if observation.OperationClass != "rebuild" {
			return false
		}
	}
	return true
}

func validateSwiftSteadyPullBaselineWires(scenario scenarios.Scenario, result SynchronizationResult) error {
	if !validateSwiftBaselineCallShape(result) {
		return errors.New("Swift steady-pull baseline call shape is invalid")
	}
	connect := result.transportObservations[0]
	if connect.StatusCode != 200 || connect.Retryable || connect.ErrorCode != nil {
		return errors.New("Swift steady-pull baseline connect did not succeed")
	}
	for _, observation := range result.transportObservations[1 : len(result.transportObservations)-1] {
		if err := validateSwiftWireObservation(scenario, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", observation); err != nil {
			return err
		}
	}
	return validateSwiftWireObservation(scenario, "STEP-PERF-STEADY-PULL-001", result.transportObservations[len(result.transportObservations)-1])
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
		return fmt.Errorf("Swift state differs from the authored model: %s", swiftStateProjectionDifference(normalizedExpected, normalizedActual))
	}
	return nil
}

// swiftStateProjectionDifference reports a bounded window around the first
// difference. The comparison alone cannot name the diverging fact.
func swiftStateProjectionDifference(expected, actual scenarios.StateFacts) string {
	expectedJSON, expectedErr := json.Marshal(expected)
	actualJSON, actualErr := json.Marshal(actual)
	if expectedErr != nil || actualErr != nil {
		return "state facts are not encodable"
	}
	index := 0
	for index < len(expectedJSON) && index < len(actualJSON) && expectedJSON[index] == actualJSON[index] {
		index++
	}
	start := index - 120
	if start < 0 {
		start = 0
	}
	return fmt.Sprintf("first difference at byte %d\n  authored: %s\n  observed: %s",
		index,
		boundedSwiftStateWindow(expectedJSON, start),
		boundedSwiftStateWindow(actualJSON, start))
}

func boundedSwiftStateWindow(encoded []byte, start int) string {
	if start >= len(encoded) {
		return "(end of state)"
	}
	end := start + 320
	if end > len(encoded) {
		end = len(encoded)
	}
	return string(encoded[start:end])
}
