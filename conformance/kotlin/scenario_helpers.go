package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func kotlinScenarioStepMap(scenario scenarios.Scenario, id string, count int) (map[scenarios.StepID]scenarios.Step, error) {
	if string(scenario.ID) != id {
		return nil, fmt.Errorf("Kotlin Android scenario %s is not %s", scenario.ID, id)
	}
	if len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return nil, errors.New("Kotlin Android scenario model setup is invalid")
	}
	if len(scenario.Steps) != count {
		return nil, fmt.Errorf("Kotlin Android scenario %s has %d steps, want %d", scenario.ID, len(scenario.Steps), count)
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil {
			return nil, fmt.Errorf("Kotlin Android scenario step %s has no native binding", step.ID)
		}
		if _, duplicate := steps[step.ID]; duplicate {
			return nil, fmt.Errorf("Kotlin Android scenario step %s is duplicated", step.ID)
		}
		steps[step.ID] = step
	}
	return steps, nil
}

func kotlinScenarioExpectedState(scenario scenarios.Scenario, id string) (scenarios.StateFacts, error) {
	for _, expectation := range scenario.Model.ExpectedState {
		if string(expectation.ID) == id && expectation.StateFacts != nil {
			return *expectation.StateFacts, nil
		}
	}
	return scenarios.StateFacts{}, fmt.Errorf("Kotlin Android expected state %s is absent", id)
}

func kotlinScenarioOperation(steps map[scenarios.StepID]scenarios.Step, id, key string) (scenarios.Operation, error) {
	step, found := steps[scenarios.StepID(id)]
	if !found {
		return scenarios.Operation{}, fmt.Errorf("Kotlin Android scenario step %s is absent", id)
	}
	if scenarios.OperationKey(step.Operation) != key {
		return scenarios.Operation{}, fmt.Errorf("Kotlin Android scenario step %s operation is %s, want %s", id, scenarios.OperationKey(step.Operation), key)
	}
	return step.Operation, nil
}

func kotlinScenarioClient(step scenarios.Step, client Client) error {
	binding := step.NativeBinding
	if binding == nil || binding.UserID != client.UserID || binding.ClientID != client.ClientID {
		return fmt.Errorf("Kotlin Android scenario step %s client identity does not match", step.ID)
	}
	return nil
}

func kotlinScenarioCall(ctx context.Context, platform *Platform, client Client, method string) (SynchronizationResult, error) {
	if ctx == nil {
		return SynchronizationResult{}, errors.New("Kotlin Android scenario call context is required")
	}
	state, err := platform.clientFor(client)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("scenario public call"); err != nil {
		return SynchronizationResult{}, err
	}
	before, err := captureClientState(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	completed, observations, err := platform.runPublicCall(ctx, state, method)
	if err != nil {
		return SynchronizationResult{}, err
	}
	window, err := platform.completeWindow(ctx, state, checkpoint, started, before)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if state.restarted {
		window.replayedMutations = replayedMutationCount(observations)
		state.restarted = false
	}
	state.started = completed.Completion == "idle"
	return synchronizationResult(completed.Completion, nil, window), nil
}

func (p *Platform) scenarioSnapshot(ctx context.Context, client Client) (Result, error) {
	state, err := p.clientFor(client)
	if err != nil {
		return Result{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	// A scenario inspection is read-only and is taken at a staged call
	// barrier, so an active call must not disqualify it. The broader
	// availability check guards operations that start or mutate work.
	if state.terminated || state.session == nil {
		return Result{}, errors.New("Kotlin Android client is unavailable for scenario inspection")
	}
	return captureClientState(ctx, state)
}

func resolveKotlinNativeIdentities(aliases []scenarios.NativeIdentityAlias, runtime map[string]json.RawMessage) ([]blackbox.NativeIdentityResolution, error) {
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range aliases {
		value := runtime[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("Kotlin Android alias %q has no runtime evidence", alias.Alias)
		}
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

func kotlinScenarioWire(result SynchronizationResult, operationClass string) (TransportObservation, error) {
	for _, observation := range result.transportObservations {
		if observation.OperationClass == operationClass {
			return observation, nil
		}
	}
	return TransportObservation{}, fmt.Errorf("Kotlin Android scenario call has no %s transport observation", operationClass)
}

func validateKotlinWireExpectation(scenario scenarios.Scenario, stepID, operationClass string, result SynchronizationResult) error {
	observed, err := kotlinScenarioWire(result, operationClass)
	if err != nil {
		return err
	}
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.StatusCode != expected.HTTPStatus || observed.Retryable == nil || *observed.Retryable != expected.Retryable || !equalKotlinOptionalStrings(observed.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Kotlin Android wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Kotlin Android wire expectation %s is absent", stepID)
}

func validateKotlinSteadyPullBaselineShape(result SynchronizationResult) bool {
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

func validateKotlinSteadyPullBaselineWires(scenario scenarios.Scenario, result SynchronizationResult) error {
	if !validateKotlinSteadyPullBaselineShape(result) {
		return errors.New("Kotlin Android steady-pull baseline call shape is invalid")
	}
	connect := result.transportObservations[0]
	if connect.StatusCode != 200 || connect.Retryable == nil || *connect.Retryable || connect.ErrorCode != nil {
		return errors.New("Kotlin Android steady-pull baseline connect did not succeed")
	}
	for _, observation := range result.transportObservations[1 : len(result.transportObservations)-1] {
		if err := validateKotlinWireObservation(scenario, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", observation); err != nil {
			return err
		}
	}
	return validateKotlinWireObservation(scenario, "STEP-PERF-STEADY-PULL-001", result.transportObservations[len(result.transportObservations)-1])
}

func validateKotlinWireObservation(scenario scenarios.Scenario, stepID string, observed TransportObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.StatusCode != expected.HTTPStatus || observed.Retryable == nil || *observed.Retryable != expected.Retryable || !equalKotlinOptionalStrings(observed.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Kotlin Android wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Kotlin Android wire expectation %s is absent", stepID)
}

func equalKotlinOptionalStrings(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func mergeKotlinCaptureFacts(values []CaptureFacts) (scenarios.StateFacts, error) {
	parts := make([]scenarios.StateFacts, 0, len(values))
	for _, value := range values {
		parts = append(parts, value.StateFacts)
	}
	return mergeKotlinStateFacts(parts...)
}

func mergeKotlinStateFacts(values ...scenarios.StateFacts) (scenarios.StateFacts, error) {
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
				merged.Clients[index] = mergeKotlinClientFacts(merged.Clients[index], client)
			} else {
				clients[key] = len(merged.Clients)
				merged.Clients = append(merged.Clients, client)
			}
		}
	}
	normalized, err := scenarios.NormalizeStateFacts(merged)
	if err != nil {
		return scenarios.StateFacts{}, fmt.Errorf("normalize merged Kotlin Android state facts: %w", err)
	}
	return normalized, nil
}

func mergeKotlinClientFacts(left, right scenarios.ClientDurabilityFact) scenarios.ClientDurabilityFact {
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

func validateKotlinStateProjection(expected, actual scenarios.StateFacts) error {
	normalizedExpected, err := scenarios.NormalizeStateFacts(expected)
	if err != nil {
		return fmt.Errorf("normalize expected Kotlin Android state facts: %w", err)
	}
	normalizedActual, err := scenarios.NormalizeStateFacts(actual)
	if err != nil {
		return fmt.Errorf("normalize actual Kotlin Android state facts: %w", err)
	}
	if !scenarios.StateFactsProjectionEqual(normalizedExpected, normalizedActual) {
		return errors.New("Kotlin Android state differs from the authored model")
	}
	return nil
}

func kotlinResultError(err error, disposition string) error {
	if err != nil {
		return err
	}
	if disposition == "" {
		return errors.New("result is absent")
	}
	return fmt.Errorf("terminal disposition is %q", disposition)
}

func kotlinResolutionMap(resolutions []blackbox.NativeIdentityResolution) (map[string]blackbox.NativeIdentityResolution, error) {
	result := make(map[string]blackbox.NativeIdentityResolution, len(resolutions))
	for _, resolution := range resolutions {
		if _, duplicate := result[resolution.Alias]; duplicate {
			return nil, errors.New("Kotlin Android native identity resolution is duplicated")
		}
		result[resolution.Alias] = resolution
	}
	return result, nil
}

func kotlinResolutionMatchesString(resolution blackbox.NativeIdentityResolution, authored, runtime string) bool {
	var resolvedAuthored, resolvedRuntime string
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored == authored && resolvedRuntime == runtime
}

func kotlinResolutionAuthoredMatchesString(resolution blackbox.NativeIdentityResolution, authored string) bool {
	var resolved string
	return json.Unmarshal(resolution.AuthoredValue, &resolved) == nil && resolved == authored
}

func kotlinResolutionMatchesSchema(resolution blackbox.NativeIdentityResolution, authored scenarios.SchemaFact, runtime schemaRef) bool {
	var resolvedAuthored, resolvedRuntime schemaRef
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored.Version == int64(authored.Version) && resolvedAuthored.Hash == authored.Hash &&
		resolvedRuntime == runtime
}

func kotlinResolutionMatchesCanonicalString(resolution blackbox.NativeIdentityResolution, authoredCanonical, runtimeCanonical string) bool {
	var resolvedAuthored, resolvedRuntime, authored, runtime string
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		json.Unmarshal([]byte(authoredCanonical), &authored) == nil &&
		json.Unmarshal([]byte(runtimeCanonical), &runtime) == nil &&
		resolvedAuthored == authored && resolvedRuntime == runtime
}

func kotlinResolutionMatchesSchemaRuntime(resolution blackbox.NativeIdentityResolution, runtime schemaRef) bool {
	var resolved schemaRef
	return json.Unmarshal(resolution.RuntimeValue, &resolved) == nil && resolved == runtime
}
