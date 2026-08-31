package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const pushResponseLossScenarioID = "SCN-PUSH-RESPONSE-LOSS-001"

// PushResponseLossResult records direct Kotlin Android evidence for the push response-loss scenario.
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

// RunPushResponseLossScenario executes the authored response-loss and replay flow through Kotlin Android.
func RunPushResponseLossScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (PushResponseLossResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		return PushResponseLossResult{}, err
	}
	if controller == nil || platform == nil {
		return PushResponseLossResult{}, errors.New("Kotlin Android push-response-loss dependencies are unavailable")
	}
	if err := validatePushResponseLossBindings(scenario, steps, client); err != nil {
		return PushResponseLossResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PushResponseLossResult{}, fmt.Errorf("install Kotlin Android push-response-loss contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "current"}); err != nil {
		return PushResponseLossResult{}, fmt.Errorf("install Kotlin Android push-response-loss client: %w", err)
	}

	localWrite, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-001", "local/write")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	localWrite, err = controller.ApplicationWrite(localWrite)
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("bind Kotlin Android response-loss local write: %w", err)
	}
	local, err := platform.ApplyStep(ctx, client, localWrite)
	if err != nil || local.Disposition != "success" {
		return PushResponseLossResult{}, fmt.Errorf("apply Kotlin Android response-loss local write: %w", kotlinResultError(err, local.Disposition))
	}

	initialPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-002", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	initial, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "start", Operations: []scenarios.Operation{initialPush}})
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("run Kotlin Android response-loss push: %w", err)
	}
	if err := validatePushResponseLossInitialCall(scenario, "STEP-PUSH-RESPONSE-LOSS-002", initial); err != nil {
		return PushResponseLossResult{}, err
	}

	boundInitial, err := pushResponseLossAppliedPush(initialPush)
	if err != nil {
		return PushResponseLossResult{}, err
	}
	if err := controller.BindApplicationPush(boundInitial); err != nil {
		return PushResponseLossResult{}, fmt.Errorf("bind Kotlin Android response-loss committed push: %w", err)
	}

	recordedLoss, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-003", "process/response-loss")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	loss, err := platform.ProcessStep(ctx, client, recordedLoss)
	if err != nil || loss.Disposition != "success" {
		return PushResponseLossResult{}, fmt.Errorf("record Kotlin Android response loss: %w", kotlinResultError(err, loss.Disposition))
	}

	replayPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-004", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	replay, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("replay Kotlin Android response-loss push: %w", err)
	}
	if err := validatePushResponseLossReplayCall(scenario, "STEP-PUSH-RESPONSE-LOSS-004", replay); err != nil {
		return PushResponseLossResult{}, err
	}

	equalPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-005", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	equalReplay, err := controller.RequestStep(ctx, equalPush)
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("replay equal Kotlin Android response-loss batch: %w", err)
	}
	if err := validatePushResponseLossNativeWire(scenario, "STEP-PUSH-RESPONSE-LOSS-005", equalReplay); err != nil {
		return PushResponseLossResult{}, err
	}

	changedPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-006", "push/submit")
	if err != nil {
		return PushResponseLossResult{}, err
	}
	changedReplay, err := controller.RequestStep(ctx, changedPush)
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("submit changed Kotlin Android response-loss batch: %w", err)
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
		return PushResponseLossResult{}, fmt.Errorf("capture Kotlin Android response-loss client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil {
		return PushResponseLossResult{}, fmt.Errorf("capture Kotlin Android response-loss server state: %w", err)
	}
	if len(serverCaptures) != 1 {
		return PushResponseLossResult{}, fmt.Errorf("Kotlin Android response-loss server capture count = %d, want 1", len(serverCaptures))
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
			return fmt.Errorf("Kotlin Android push-response-loss wire expectation %s count is greater than 1, want 1", wire.StepID)
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
		if _, err := kotlinScenarioOperation(steps, wanted.id, wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil {
			return fmt.Errorf("Kotlin Android push-response-loss binding %s is absent, want %s/%s/%s", wanted.id, wanted.kind, wanted.stage, wanted.method)
		}
		if binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Kotlin Android push-response-loss binding %s = %s/%s/%s/%s, want %s/%s/%s/success", wanted.id, binding.Kind, binding.Stage, binding.Method, step.ExpectedOutcome.Disposition, wanted.kind, wanted.stage, wanted.method)
		}
		if wanted.kind == "local-write" || wanted.kind == "public-call" {
			if err := kotlinScenarioClient(step, client); err != nil {
				return err
			}
		}
		if wanted.kind != "public-call" {
			continue
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Kotlin Android push-response-loss binding %s call identity is absent, want a nonempty identity", wanted.id)
		}
		if callID == "" {
			callID = *binding.CallID
		} else if callID != *binding.CallID {
			return fmt.Errorf("Kotlin Android push-response-loss binding %s call identity = %q, want %q", wanted.id, *binding.CallID, callID)
		}
	}
	wireSteps := []scenarios.StepID{
		"STEP-PUSH-RESPONSE-LOSS-002",
		"STEP-PUSH-RESPONSE-LOSS-004",
		"STEP-PUSH-RESPONSE-LOSS-005",
		"STEP-PUSH-RESPONSE-LOSS-006",
	}
	if len(wires) != len(wireSteps) {
		return fmt.Errorf("Kotlin Android push-response-loss wire expectation count = %d, want %d", len(wires), len(wireSteps))
	}
	for _, id := range wireSteps {
		if _, found := wires[id]; !found {
			return fmt.Errorf("Kotlin Android push-response-loss wire expectation %s count = 0, want 1", id)
		}
	}
	finalStep := steps[scenarios.StepID("STEP-PUSH-RESPONSE-LOSS-004")]
	finalWire := wires[finalStep.ID]
	wantCompletion := pushResponseLossNativeCompletion(finalWire)
	if finalStep.NativeBinding.Completion != wantCompletion {
		return fmt.Errorf("Kotlin Android push-response-loss final completion = %q, want %q", finalStep.NativeBinding.Completion, wantCompletion)
	}
	initialPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-002", "push/submit")
	if err != nil {
		return err
	}
	replayPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-004", "push/submit")
	if err != nil {
		return err
	}
	equalPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-005", "push/submit")
	if err != nil {
		return err
	}
	changedPush, err := kotlinScenarioOperation(steps, "STEP-PUSH-RESPONSE-LOSS-006", "push/submit")
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
	wantDeliveries := []string{"drop_after_server", "apply", "apply", "apply"}
	gotDeliveries := []string{initialPayload.Delivery, replayPayload.Delivery, equalPayload.Delivery, changedPayload.Delivery}
	for index := range gotDeliveries {
		if gotDeliveries[index] != wantDeliveries[index] {
			return fmt.Errorf("Kotlin Android push-response-loss request %d delivery = %q, want %q", index+1, gotDeliveries[index], wantDeliveries[index])
		}
	}
	if initialPayload.AuthenticatedUserID == "" || initialPayload.Request.ClientID == "" || initialPayload.Request.BatchID == "" || len(initialPayload.Request.Mutations) == 0 {
		return fmt.Errorf("Kotlin Android push-response-loss initial identity and mutation count = %q/%q/%q/%d, want nonempty values and at least 1 mutation", initialPayload.AuthenticatedUserID, initialPayload.Request.ClientID, initialPayload.Request.BatchID, len(initialPayload.Request.Mutations))
	}
	for index, payload := range []pushResponseLossPayload{replayPayload, equalPayload, changedPayload} {
		if payload.AuthenticatedUserID != initialPayload.AuthenticatedUserID || payload.Request.ClientID != initialPayload.Request.ClientID || payload.Request.BatchID != initialPayload.Request.BatchID {
			return fmt.Errorf("Kotlin Android push-response-loss replay %d identity = %q/%q/%q, want %q/%q/%q", index+1, payload.AuthenticatedUserID, payload.Request.ClientID, payload.Request.BatchID, initialPayload.AuthenticatedUserID, initialPayload.Request.ClientID, initialPayload.Request.BatchID)
		}
	}
	if !equalPushResponseLossRequest(initial, replay) || !equalPushResponseLossRequest(initial, equal) || !equalPushResponseLossMutations(initialPayload.Request.Mutations, replayPayload.Request.Mutations) || !equalPushResponseLossMutations(initialPayload.Request.Mutations, equalPayload.Request.Mutations) {
		return errors.New("Kotlin Android push-response-loss equal replay equality = false, want true for both sealed requests and mutation lists")
	}
	if equalPushResponseLossRequest(initial, changed) || equalPushResponseLossMutations(initialPayload.Request.Mutations, changedPayload.Request.Mutations) {
		return errors.New("Kotlin Android push-response-loss changed replay equality = true, want false for both the sealed request and mutation list")
	}
	return nil
}

func equalPushResponseLossRequest(left, right scenarios.Operation) bool {
	canonicalRequest := func(operation scenarios.Operation) ([]byte, error) {
		var payload struct {
			Request json.RawMessage `json:"request"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Request) == 0 {
			return nil, errors.New("decode Kotlin Android push-response-loss sealed request failed")
		}
		var request any
		if err := json.Unmarshal(payload.Request, &request); err != nil {
			return nil, errors.New("decode Kotlin Android push-response-loss sealed request failed")
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
		return pushResponseLossPayload{}, fmt.Errorf("decode Kotlin Android push-response-loss request: %w", err)
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
		return scenarios.Operation{}, errors.New("decode Kotlin Android response-loss committed push failed")
	}
	if payload["delivery"] != "drop_after_server" {
		return scenarios.Operation{}, fmt.Errorf("Kotlin Android response-loss committed push delivery = %q, want %q", payload["delivery"], "drop_after_server")
	}
	payload["delivery"] = "apply"
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, errors.New("encode Kotlin Android response-loss committed push failed")
	}
	bound := operation
	bound.Payload = encoded
	if err := scenarios.ValidateOperation(bound); err != nil {
		return scenarios.Operation{}, fmt.Errorf("validate Kotlin Android response-loss committed push: %w", err)
	}
	return bound, nil
}

func pushResponseLossBatchCount(operations ...scenarios.Operation) (int, error) {
	batches := make(map[string]struct{}, len(operations))
	for _, operation := range operations {
		payload, err := decodePushResponseLossPayload(operation)
		if err != nil || payload.Request.BatchID == "" {
			return 0, errors.New("Kotlin Android push-response-loss batch identity is absent, want a nonempty identity")
		}
		batches[payload.Request.BatchID] = struct{}{}
	}
	if len(batches) == 0 {
		return 0, errors.New("Kotlin Android push-response-loss unique batch count = 0, want at least 1")
	}
	return len(batches), nil
}

func validatePushResponseLossInitialCall(scenario scenarios.Scenario, stepID string, call SynchronizationResult) error {
	wire, err := pushResponseLossWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	wantCompletion := pushResponseLossNativeCompletion(wire)
	if call.Completion != wantCompletion || len(call.Steps) != 1 || len(call.transportObservations) == 0 {
		return fmt.Errorf("Kotlin Android push-response-loss initial call shape = %q/%d/%d, want %q/1/at least 1", call.Completion, len(call.Steps), len(call.transportObservations), wantCompletion)
	}
	last := call.transportObservations[len(call.transportObservations)-1]
	if last.OperationClass != "push" || last.StatusCode < 200 || last.StatusCode >= 300 || last.ErrorCode != nil || last.Retryable == nil || *last.Retryable {
		return fmt.Errorf("Kotlin Android push-response-loss committed response = %s/%d/%s/%s, want push/2xx/false/none", last.OperationClass, last.StatusCode, pushResponseLossOptionalBool(last.Retryable), pushResponseLossOptionalString(last.ErrorCode))
	}
	stepWire := call.Steps[0].Wire
	if stepWire == nil {
		return fmt.Errorf("Kotlin Android push-response-loss lost response wire = absent, want %d/%t/%s", wire.HTTPStatus, wire.Retryable, pushResponseLossOptionalString(wire.ErrorCode))
	}
	if stepWire.HTTPStatus != wire.HTTPStatus || stepWire.Retryable != wire.Retryable || !equalKotlinOptionalStrings(stepWire.ErrorCode, wire.ErrorCode) {
		return fmt.Errorf("Kotlin Android push-response-loss lost response wire = %d/%t/%s, want %d/%t/%s", stepWire.HTTPStatus, stepWire.Retryable, pushResponseLossOptionalString(stepWire.ErrorCode), wire.HTTPStatus, wire.Retryable, pushResponseLossOptionalString(wire.ErrorCode))
	}
	return nil
}

func validatePushResponseLossReplayCall(scenario scenarios.Scenario, stepID string, call SynchronizationResult) error {
	wire, err := pushResponseLossWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	wantCompletion := pushResponseLossNativeCompletion(wire)
	if call.Completion != wantCompletion {
		return fmt.Errorf("Kotlin Android push-response-loss replay completion = %q, want %q", call.Completion, wantCompletion)
	}
	pushes := 0
	for _, observation := range call.transportObservations {
		if observation.OperationClass == "push" {
			pushes++
		}
	}
	if pushes != 1 {
		return fmt.Errorf("Kotlin Android push-response-loss replay push request count = %d, want 1", pushes)
	}
	observed, err := kotlinScenarioWire(call, "push")
	if err != nil {
		return err
	}
	if observed.StatusCode != wire.HTTPStatus || observed.Retryable == nil || *observed.Retryable != wire.Retryable || !equalKotlinOptionalStrings(observed.ErrorCode, wire.ErrorCode) {
		return fmt.Errorf("Kotlin Android push-response-loss wire result %s = %d/%s/%s, want %d/%t/%s", stepID, observed.StatusCode, pushResponseLossOptionalBool(observed.Retryable), pushResponseLossOptionalString(observed.ErrorCode), wire.HTTPStatus, wire.Retryable, pushResponseLossOptionalString(wire.ErrorCode))
	}
	return nil
}

func validatePushResponseLossNativeWire(scenario scenarios.Scenario, stepID string, observed blackbox.NativeStepObservation) error {
	expected, err := pushResponseLossWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if observed.Wire == nil {
		return fmt.Errorf("Kotlin Android push-response-loss wire result %s = absent, want %d/%t/%s", stepID, expected.HTTPStatus, expected.Retryable, pushResponseLossOptionalString(expected.ErrorCode))
	}
	if observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !equalKotlinOptionalStrings(observed.Wire.ErrorCode, expected.ErrorCode) {
		return fmt.Errorf("Kotlin Android push-response-loss wire result %s = %d/%t/%s, want %d/%t/%s", stepID,
			observed.Wire.HTTPStatus, observed.Wire.Retryable, pushResponseLossOptionalString(observed.Wire.ErrorCode),
			expected.HTTPStatus, expected.Retryable, pushResponseLossOptionalString(expected.ErrorCode))
	}
	wantDisposition := "error"
	if pushResponseLossNativeCompletion(expected) == "idle" {
		wantDisposition = "success"
	}
	if observed.Disposition != wantDisposition || !equalKotlinOptionalStrings(observed.ErrorCode, expected.ErrorCode) {
		return fmt.Errorf("Kotlin Android push-response-loss result %s = %s/%s, want %s/%s", stepID, observed.Disposition, pushResponseLossOptionalString(observed.ErrorCode), wantDisposition, pushResponseLossOptionalString(expected.ErrorCode))
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
		return scenarios.WireExpectation{}, fmt.Errorf("Kotlin Android push-response-loss wire expectation %s count = %d, want 1", stepID, count)
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
	client, err := mergeKotlinCaptureFacts(captures)
	if err != nil {
		return err
	}
	if len(client.Clients) != clientCount {
		return fmt.Errorf("Kotlin Android push-response-loss captured client count = %d, want %d", len(client.Clients), clientCount)
	}
	// The non-retryable identity conflict must not remove the sealed request.
	if client.Clients[0].SealedBatchCount == nil {
		return errors.New("Kotlin Android push-response-loss client sealed batch count = absent, want 1 preserved sealed request")
	}
	if *client.Clients[0].SealedBatchCount != 1 {
		return fmt.Errorf("Kotlin Android push-response-loss client sealed batch count = %d, want 1 preserved sealed request", *client.Clients[0].SealedBatchCount)
	}
	if client.Clients[0].QueueCount == nil {
		return fmt.Errorf("Kotlin Android push-response-loss client queue count = absent, want %d preserved mutations", mutationCount)
	}
	if *client.Clients[0].QueueCount != uint64(mutationCount) {
		return fmt.Errorf("Kotlin Android push-response-loss client queue count = %d, want %d preserved mutations", *client.Clients[0].QueueCount, mutationCount)
	}
	if server.BatchCount == nil {
		return fmt.Errorf("Kotlin Android push-response-loss server batch count = absent, want %d", batchCount)
	}
	if server.MutationCount == nil {
		return fmt.Errorf("Kotlin Android push-response-loss server mutation count = absent, want %d", mutationCount)
	}
	if *server.BatchCount != uint64(batchCount) {
		return fmt.Errorf("Kotlin Android push-response-loss server batch count = %d, want %d", *server.BatchCount, batchCount)
	}
	if *server.MutationCount != uint64(mutationCount) {
		return fmt.Errorf("Kotlin Android push-response-loss server mutation count = %d, want %d", *server.MutationCount, mutationCount)
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
				return nil, fmt.Errorf("Kotlin Android push-response-loss call %d client generation evidence = absent, want one positive generation", callIndex+1)
			}
			observed := *observation.RequestFacts.ClientGeneration
			if generationObserved && generation != observed {
				return nil, fmt.Errorf("Kotlin Android push-response-loss call %d client generation = %d, want %d", callIndex+1, observed, generation)
			}
			generation = observed
			generationObserved = true
		}
		if pushes != 1 {
			return nil, fmt.Errorf("Kotlin Android push-response-loss call %d push request count = %d, want 1", callIndex+1, pushes)
		}
	}
	if !generationObserved || generation <= 0 {
		return nil, fmt.Errorf("Kotlin Android push-response-loss client generation = %d with observed = %t, want a positive observed generation", generation, generationObserved)
	}
	encodedGeneration, err := json.Marshal(generation)
	if err != nil {
		return nil, fmt.Errorf("encode Kotlin Android push-response-loss client generation: %w", err)
	}
	for _, alias := range aliases {
		if alias.Kind == "client-generation" {
			runtime[alias.Alias] = encodedGeneration
		}
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("Kotlin Android push-response-loss alias %q runtime evidence = absent, want a nonempty value", alias.Alias)
		}
	}
	return resolveKotlinNativeIdentities(aliases, runtime)
}

func pushResponseLossOptionalString(value *string) string {
	if value == nil {
		return "none"
	}
	return *value
}

func pushResponseLossOptionalBool(value *bool) string {
	if value == nil {
		return "none"
	}
	return fmt.Sprintf("%t", *value)
}
