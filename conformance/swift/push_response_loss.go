package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const pushResponseLossScenarioID = "SCN-PUSH-RESPONSE-LOSS-001"

// PushResponseLossResult records direct Swift evidence for the push response-loss scenario.
type PushResponseLossResult struct {
	InitialCall        SynchronizationResult
	ReplayCall         SynchronizationResult
	EqualReplay        blackbox.NativeStepObservation
	ChangedReplay      blackbox.NativeStepObservation
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type pushResponseLossPayload struct {
	AuthenticatedUserID string `json:"authenticated_user_id"`
	Request             struct {
		ClientID  string            `json:"client_id"`
		BatchID   string            `json:"batch_id"`
		Mutations []json.RawMessage `json:"mutations"`
	} `json:"request"`
	Delivery string `json:"delivery"`
}

// RunPushResponseLossScenario executes the authored response-loss and replay flow through Swift.
func RunPushResponseLossScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (PushResponseLossResult, error) {
	steps, err := swiftScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		return PushResponseLossResult{}, err
	}
	if controller == nil || platform == nil {
		return PushResponseLossResult{}, errors.New("Swift push-response-loss dependencies are unavailable")
	}
	if err := validatePushResponseLossBindings(scenario, steps, client); err != nil {
		return PushResponseLossResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PushResponseLossResult{}, fmt.Errorf("install Swift push-response-loss contract: %w", err)
	}
	if err := platform.Install(ctx, client, "current", ""); err != nil {
		return PushResponseLossResult{}, fmt.Errorf("install Swift push-response-loss client: %w", err)
	}

	localWrite, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-001", "local/write")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	localWrite, err = controller.ApplicationWrite(localWrite)
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("bind Swift response-loss local write: %w", err)
	}
	local, err := platform.ApplyStep(ctx, client, localWrite)
	if err != nil || local.Disposition != "success" {
		return PushResponseLossResult{}, fmt.Errorf("apply Swift response-loss local write: %w", resultError(err, local.Disposition))
	}

	initialPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-002", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	initial, err := platform.Synchronize(ctx, client, "start", RequestOperations{initialPush})
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("run Swift response-loss push: %w", err)
	}
	if err := validatePushResponseLossInitialCall(scenario, "STEP-PUSH-RESPONSE-LOSS-002", initial); err != nil {
		return PushResponseLossResult{}, err
	}

	boundInitial, err := pushResponseLossAppliedPush(initialPush)
	if err != nil {
		return PushResponseLossResult{}, err
	}
	if err := controller.BindApplicationPush(boundInitial); err != nil {
		return PushResponseLossResult{}, fmt.Errorf("bind Swift response-loss committed push: %w", err)
	}

	recordedLoss, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-003", "process/response-loss")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	loss, err := platform.ProcessStep(ctx, client, recordedLoss)
	if err != nil || loss.Disposition != "success" {
		return PushResponseLossResult{}, fmt.Errorf("record Swift response loss: %w", resultError(err, loss.Disposition))
	}

	replayPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-004", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	replay, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("replay Swift response-loss push: %w", err)
	}
	if err := validatePushResponseLossReplayCall(scenario, "STEP-PUSH-RESPONSE-LOSS-004", replay); err != nil {
		return PushResponseLossResult{}, err
	}

	equalPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-005", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	equalReplay, err := controller.RequestStep(ctx, equalPush)
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("replay equal Swift response-loss batch: %w", err)
	}
	if err := validatePushResponseLossNativeWire(scenario, "STEP-PUSH-RESPONSE-LOSS-005", equalReplay); err != nil {
		return PushResponseLossResult{}, err
	}

	changedPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-006", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	changedReplay, err := controller.RequestStep(ctx, changedPush)
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("submit changed Swift response-loss batch: %w", err)
	}
	if err := validatePushResponseLossNativeWire(scenario, "STEP-PUSH-RESPONSE-LOSS-006", changedReplay); err != nil {
		return PushResponseLossResult{}, err
	}

	initialPayload, err := decodePushResponseLossPayload(initialPush)
	if err != nil {
		return PushResponseLossResult{}, err
	}
	batchCount, err := pushResponseLossBatchCount(initialPush, replayPush, equalPush, changedPush)
	if err != nil {
		return PushResponseLossResult{}, err
	}
	clients := []Client{client}
	clientFacts, err := platform.Capture(ctx, clients, []string{"pending-mutations", "rejected-mutations"})
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("capture Swift response-loss client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return PushResponseLossResult{}, fmt.Errorf("capture Swift response-loss server state: %w", err)
	}
	if err := validatePushResponseLossState(clientFacts, serverCaptures[0].StateFacts, len(clients), batchCount, len(initialPayload.Request.Mutations)); err != nil {
		return PushResponseLossResult{}, err
	}
	identities, err := resolvePushResponseLossIdentities(controller, scenario.NativeIdentityAliases, initial, replay)
	if err != nil {
		return PushResponseLossResult{}, err
	}

	return PushResponseLossResult{
		InitialCall:        initial,
		ReplayCall:         replay,
		EqualReplay:        equalReplay,
		ChangedReplay:      changedReplay,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: identities,
	}, nil
}

func validatePushResponseLossBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, client Client) error {
	wires := make(map[scenarios.StepID]scenarios.WireExpectation, len(scenario.WireExpectations))
	for _, wire := range scenario.WireExpectations {
		if _, duplicate := wires[wire.StepID]; duplicate {
			return fmt.Errorf("Swift push-response-loss wire expectation %s is duplicated", wire.StepID)
		}
		wires[wire.StepID] = wire
	}
	expected := []struct {
		id, key, kind, stage, method string
	}{
		{"STEP-PUSH-RESPONSE-LOSS-001", "local/write", "local-write", "", ""},
		{"STEP-PUSH-RESPONSE-LOSS-002", "push/submit", "public-call", "begin", "start"},
		{"STEP-PUSH-RESPONSE-LOSS-003", "process/response-loss", "public-call", "await-step", ""},
		{"STEP-PUSH-RESPONSE-LOSS-004", "push/submit", "public-call", "await-call", ""},
		{"STEP-PUSH-RESPONSE-LOSS-005", "push/submit", "controller", "", ""},
		{"STEP-PUSH-RESPONSE-LOSS-006", "push/submit", "controller", "", ""},
	}
	var callID scenarios.NativeCallID
	for _, wanted := range expected {
		step := steps[scenarios.StepID(wanted.id)]
		if _, err := swiftScenarioOperation(steps, wanted.id, wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Swift push-response-loss binding %s is invalid", wanted.id)
		}
		if wanted.kind == "local-write" || wanted.kind == "public-call" {
			if err := swiftScenarioClient(step, client); err != nil {
				return err
			}
		}
		if wanted.kind != "public-call" {
			continue
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Swift push-response-loss binding %s has no call identity", wanted.id)
		}
		if callID == "" {
			callID = *binding.CallID
		} else if callID != *binding.CallID {
			return errors.New("Swift push-response-loss bindings do not share one public call")
		}
	}
	wireSteps := []scenarios.StepID{
		"STEP-PUSH-RESPONSE-LOSS-002",
		"STEP-PUSH-RESPONSE-LOSS-004",
		"STEP-PUSH-RESPONSE-LOSS-005",
		"STEP-PUSH-RESPONSE-LOSS-006",
	}
	if len(wires) != len(wireSteps) {
		return fmt.Errorf("Swift push-response-loss has %d wire expectations, want %d", len(wires), len(wireSteps))
	}
	for _, id := range wireSteps {
		if _, found := wires[id]; !found {
			return fmt.Errorf("Swift push-response-loss wire expectation %s is absent", id)
		}
	}
	finalStep := steps[scenarios.StepID("STEP-PUSH-RESPONSE-LOSS-004")]
	finalWire := wires[finalStep.ID]
	if finalStep.NativeBinding.Completion != pushResponseLossNativeCompletion(finalWire) {
		return errors.New("Swift push-response-loss final completion does not match its authored wire expectation")
	}
	initialPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-002", "push/submit")
	if err != nil {
		return err
	}
	replayPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-004", "push/submit")
	if err != nil {
		return err
	}
	equalPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-005", "push/submit")
	if err != nil {
		return err
	}
	changedPush, err := swiftScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-006", "push/submit")
	if err != nil {
		return err
	}
	return validatePushResponseLossRequests(initialPush, replayPush, equalPush, changedPush)
}

func validatePushResponseLossRequests(initial, replay, equal, changed scenarios.Operation) error {
	initialPayload, err := decodePushResponseLossPayload(initial)
	if err != nil {
		return err
	}
	replayPayload, err := decodePushResponseLossPayload(replay)
	if err != nil {
		return err
	}
	equalPayload, err := decodePushResponseLossPayload(equal)
	if err != nil {
		return err
	}
	changedPayload, err := decodePushResponseLossPayload(changed)
	if err != nil {
		return err
	}
	if initialPayload.Delivery != "drop_after_server" || replayPayload.Delivery != "apply" || equalPayload.Delivery != "apply" || changedPayload.Delivery != "apply" {
		return errors.New("Swift push-response-loss request deliveries are invalid")
	}
	if initialPayload.AuthenticatedUserID == "" || initialPayload.Request.ClientID == "" || initialPayload.Request.BatchID == "" || len(initialPayload.Request.Mutations) == 0 {
		return errors.New("Swift push-response-loss initial request is incomplete")
	}
	if replayPayload.AuthenticatedUserID != initialPayload.AuthenticatedUserID || equalPayload.AuthenticatedUserID != initialPayload.AuthenticatedUserID || changedPayload.AuthenticatedUserID != initialPayload.AuthenticatedUserID || replayPayload.Request.ClientID != initialPayload.Request.ClientID || equalPayload.Request.ClientID != initialPayload.Request.ClientID || changedPayload.Request.ClientID != initialPayload.Request.ClientID || replayPayload.Request.BatchID != initialPayload.Request.BatchID || equalPayload.Request.BatchID != initialPayload.Request.BatchID || changedPayload.Request.BatchID != initialPayload.Request.BatchID {
		return errors.New("Swift push-response-loss replay identity differs from the initial request")
	}
	if !equalPushResponseLossRequest(initial, replay) || !equalPushResponseLossRequest(initial, equal) || !equalPushResponseLossMutations(initialPayload.Request.Mutations, replayPayload.Request.Mutations) || !equalPushResponseLossMutations(initialPayload.Request.Mutations, equalPayload.Request.Mutations) {
		return errors.New("Swift push-response-loss equal replay changed the sealed request")
	}
	if equalPushResponseLossRequest(initial, changed) || equalPushResponseLossMutations(initialPayload.Request.Mutations, changedPayload.Request.Mutations) {
		return errors.New("Swift push-response-loss changed replay did not change the sealed request")
	}
	return nil
}

func equalPushResponseLossRequest(left, right scenarios.Operation) bool {
	canonicalRequest := func(operation scenarios.Operation) ([]byte, error) {
		var payload struct {
			Request json.RawMessage `json:"request"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Request) == 0 {
			return nil, errors.New("decode Swift push-response-loss sealed request failed")
		}
		var request any
		if err := json.Unmarshal(payload.Request, &request); err != nil {
			return nil, errors.New("decode Swift push-response-loss sealed request failed")
		}
		return json.Marshal(request)
	}
	leftRequest, leftErr := canonicalRequest(left)
	rightRequest, rightErr := canonicalRequest(right)
	return leftErr == nil && rightErr == nil && string(leftRequest) == string(rightRequest)
}

func decodePushResponseLossPayload(operation scenarios.Operation) (pushResponseLossPayload, error) {
	var payload pushResponseLossPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return pushResponseLossPayload{}, fmt.Errorf("decode Swift push-response-loss request: %w", err)
	}
	return payload, nil
}

func equalPushResponseLossMutations(left, right []json.RawMessage) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		var leftValue, rightValue any
		if json.Unmarshal(left[index], &leftValue) != nil || json.Unmarshal(right[index], &rightValue) != nil {
			return false
		}
		leftJSON, leftErr := json.Marshal(leftValue)
		rightJSON, rightErr := json.Marshal(rightValue)
		if leftErr != nil || rightErr != nil || string(leftJSON) != string(rightJSON) {
			return false
		}
	}
	return true
}

func pushResponseLossAppliedPush(operation scenarios.Operation) (scenarios.Operation, error) {
	var payload map[string]any
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return scenarios.Operation{}, errors.New("decode Swift response-loss committed push failed")
	}
	if payload["delivery"] != "drop_after_server" {
		return scenarios.Operation{}, errors.New("Swift response-loss committed push does not drop its response")
	}
	payload["delivery"] = "apply"
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, errors.New("encode Swift response-loss committed push failed")
	}
	bound := operation
	bound.Payload = encoded
	if err := scenarios.ValidateOperation(bound); err != nil {
		return scenarios.Operation{}, fmt.Errorf("validate Swift response-loss committed push: %w", err)
	}
	return bound, nil
}

func pushResponseLossBatchCount(operations ...scenarios.Operation) (int, error) {
	batches := make(map[string]struct{}, len(operations))
	for _, operation := range operations {
		payload, err := decodePushResponseLossPayload(operation)
		if err != nil || payload.Request.BatchID == "" {
			return 0, errors.New("decode Swift push-response-loss batch identity failed")
		}
		batches[payload.Request.BatchID] = struct{}{}
	}
	if len(batches) == 0 {
		return 0, errors.New("Swift push-response-loss has no batch identity")
	}
	return len(batches), nil
}

func validatePushResponseLossInitialCall(scenario scenarios.Scenario, stepID string, call SynchronizationResult) error {
	wire, err := pushResponseLossWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if call.Completion != pushResponseLossNativeCompletion(wire) || len(call.Steps) != 1 || len(call.transportObservations) == 0 {
		return errors.New("Swift push-response-loss initial call did not block after one request")
	}
	last := call.transportObservations[len(call.transportObservations)-1]
	if last.OperationClass != "push" || last.StatusCode < 200 || last.StatusCode >= 300 || last.ErrorCode != nil || last.Retryable || call.Steps[0].Wire == nil || call.Steps[0].Wire.HTTPStatus != last.StatusCode {
		return errors.New("Swift push-response-loss initial call did not preserve a committed response")
	}
	return nil
}

func validatePushResponseLossReplayCall(scenario scenarios.Scenario, stepID string, call SynchronizationResult) error {
	wire, err := pushResponseLossWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if call.Completion != pushResponseLossNativeCompletion(wire) {
		return errors.New("Swift push-response-loss replay completion differs from its authored wire expectation")
	}
	pushes := 0
	for _, observation := range call.transportObservations {
		if observation.OperationClass == "push" {
			pushes++
		}
	}
	if pushes != 1 {
		return fmt.Errorf("Swift push-response-loss replay sent %d push requests, want 1", pushes)
	}
	return validateSwiftWireExpectation(scenario, stepID, "push", call)
}

func validatePushResponseLossNativeWire(scenario scenarios.Scenario, stepID string, observed blackbox.NativeStepObservation) error {
	expected, err := pushResponseLossWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if observed.Wire == nil || observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, expected.ErrorCode) {
		return fmt.Errorf("Swift push-response-loss wire result %s differs from its authored expectation", stepID)
	}
	wantDisposition := "error"
	if pushResponseLossNativeCompletion(expected) == "idle" {
		wantDisposition = "success"
	}
	if observed.Disposition != wantDisposition || !equalOptionalStrings(observed.ErrorCode, expected.ErrorCode) {
		return fmt.Errorf("Swift push-response-loss result %s has the wrong terminal disposition", stepID)
	}
	return nil
}

func pushResponseLossWireExpectation(scenario scenarios.Scenario, stepID scenarios.StepID) (scenarios.WireExpectation, error) {
	var found scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == stepID {
			found = wire
			count++
		}
	}
	if count != 1 {
		return scenarios.WireExpectation{}, fmt.Errorf("Swift push-response-loss wire expectation %s count = %d, want 1", stepID, count)
	}
	return found, nil
}

func pushResponseLossNativeCompletion(wire scenarios.WireExpectation) string {
	if wire.Action == "unsupported" {
		return "error"
	}
	if wire.HTTPStatus >= 200 && wire.HTTPStatus < 300 {
		return "idle"
	}
	if wire.Retryable || wire.HTTPStatus == 0 {
		return "blocked"
	}
	return "error"
}

func validatePushResponseLossState(captures []CaptureFacts, server scenarios.StateFacts, clientCount, batchCount, mutationCount int) error {
	client, err := mergeSwiftCaptureFacts(captures)
	if err != nil {
		return err
	}
	if len(client.Clients) != clientCount || client.Clients[0].QueueCount == nil || *client.Clients[0].QueueCount != 0 || client.Clients[0].SealedBatchCount == nil || *client.Clients[0].SealedBatchCount != 0 {
		return errors.New("Swift push-response-loss client queue did not reconcile exactly once")
	}
	if server.BatchCount == nil || *server.BatchCount != uint64(batchCount) || server.MutationCount == nil || *server.MutationCount != uint64(mutationCount) {
		return errors.New("Swift push-response-loss replay changed committed batch state")
	}
	return nil
}

func resolvePushResponseLossIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, initial, replay SynchronizationResult) ([]blackbox.NativeIdentityResolution, error) {
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, err
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}

	var generation int64
	generationObserved := false
	for callIndex, call := range []SynchronizationResult{initial, replay} {
		pushes := 0
		for _, observation := range call.transportObservations {
			if observation.OperationClass != "push" {
				continue
			}
			pushes++
			if observation.RequestFacts == nil || observation.RequestFacts.ClientGeneration == nil {
				return nil, fmt.Errorf("Swift push-response-loss call %d has no client generation evidence", callIndex+1)
			}
			observed := *observation.RequestFacts.ClientGeneration
			if generationObserved && generation != observed {
				return nil, errors.New("Swift push-response-loss client generation changed between calls")
			}
			generation = observed
			generationObserved = true
		}
		if pushes != 1 {
			return nil, fmt.Errorf("Swift push-response-loss call %d observed %d push requests, want 1", callIndex+1, pushes)
		}
	}
	if !generationObserved || generation <= 0 {
		return nil, errors.New("Swift push-response-loss client generation is absent")
	}
	encodedGeneration, err := json.Marshal(generation)
	if err != nil {
		return nil, fmt.Errorf("encode Swift push-response-loss client generation: %w", err)
	}
	for _, alias := range aliases {
		if alias.Kind == "client-generation" {
			runtime[alias.Alias] = encodedGeneration
		}
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("Swift push-response-loss alias %q has no runtime evidence", alias.Alias)
		}
	}
	return resolveSwiftNativeIdentities(aliases, runtime)
}
