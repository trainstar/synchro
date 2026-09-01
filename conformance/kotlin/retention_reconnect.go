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

const retentionReconnectScenarioID = "SCN-RETENTION-RECONNECT-001"

// RetentionReconnectResult records direct Kotlin Android evidence for expired-generation recovery.
type RetentionReconnectResult struct {
	InitialCall        RetentionReconnectCall
	RenewalCall        RetentionReconnectCall
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// RetentionReconnectCall records one public call and its terminal result.
type RetentionReconnectCall struct {
	Completion string
	Call       *ClientCallResult
	Transport  []TransportObservation
}

type retentionReconnectBinding struct {
	id       scenarios.StepID
	key      string
	kind     string
	stage    string
	method   string
	terminal bool
	call     string
}

// RunRetentionReconnectScenario executes the authored expired-generation reconnect flow through Kotlin Android.
func RunRetentionReconnectScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (RetentionReconnectResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, retentionReconnectScenarioID, 9)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if controller == nil || platform == nil {
		return RetentionReconnectResult{}, errors.New("Kotlin Android retention-reconnect dependencies are unavailable")
	}
	if err := validateRetentionReconnectBindings(scenario, steps, client); err != nil {
		return RetentionReconnectResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("install Kotlin Android retention-reconnect contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "current"}); err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("install Kotlin Android retention-reconnect client: %w", err)
	}

	localWrite, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-LOCAL-WRITE-001", "local/write")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	authoredWrite := localWrite
	localWrite, err = controller.ApplicationWrite(localWrite)
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("bind Kotlin Android retention-reconnect local write: %w", err)
	}
	intentPrimary, err := retentionReconnectWrittenPrimaryKey(authoredWrite, localWrite)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	local, err := platform.ApplyStep(ctx, client, localWrite)
	if err != nil || local.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("apply Kotlin Android retention-reconnect local write: %w", kotlinResultError(err, local.Disposition))
	}

	sealedPush, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001", "push/submit")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	releaseFault, armed, err := platform.armTemporaryUnavailablePush([]scenarios.Operation{sealedPush})
	if err != nil || !armed {
		return RetentionReconnectResult{}, fmt.Errorf("arm Kotlin Android retention-reconnect temporary-unavailable push: %w", err)
	}
	defer releaseFault()
	initialCall, err := runRetentionReconnectInitialCall(ctx, scenario, platform, client)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	authoredMutations, err := retentionReconnectAuthoredMutationCount(sealedPush)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	initialSnapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("capture Kotlin Android retention-reconnect sealed queue: %w", err)
	}
	sealedBatchID, sealedMutationIDs, err := retentionReconnectQueueIdentityEvidence(initialSnapshot, authoredMutations)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	mutationIDs, err := retentionReconnectSealedIdentities(sealedMutationIDs, authoredMutations)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if err := validateRetentionReconnectQueue(initialSnapshot, mutationIDs); err != nil {
		return RetentionReconnectResult{}, err
	}

	commit, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, applyErr := controller.ApplyStep(ctx, commit); applyErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("commit Kotlin Android retention-reconnect history: %w", kotlinResultError(applyErr, observed.Disposition))
	}
	materialize, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, processErr := controller.ProcessStep(ctx, nil, materialize); processErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("materialize Kotlin Android retention-reconnect history: %w", kotlinResultError(processErr, observed.Disposition))
	}

	rebuildPin, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-REBUILD-PIN-001", "rebuild/request-page")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if err := registerRetentionReconnectPinClient(ctx, controller, rebuildPin); err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, requestErr := controller.RequestStep(ctx, rebuildPin); requestErr != nil || observed.Disposition != "success" {
		status := "none"
		message := "none"
		if observed.Wire != nil {
			status = fmt.Sprintf("%d", observed.Wire.HTTPStatus)
			message = observed.Wire.Message
		}
		return RetentionReconnectResult{}, fmt.Errorf("create Kotlin Android retention-reconnect rebuild pin: %w (error code %s, http status %s, message %q)",
			kotlinResultError(requestErr, observed.Disposition), pushResponseLossOptionalString(observed.ErrorCode), status, message)
	} else if err := validateRetentionReconnectNativeWire(scenario, "STEP-RETENTION-RECONNECT-REBUILD-PIN-001", observed); err != nil {
		return RetentionReconnectResult{}, err
	}
	pinnedRebuildIdentity, err := retentionReconnectPinnedRebuildID(rebuildPin)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	pinnedRebuilds := "unavailable"
	if pinned, pinErr := controller.Capture(ctx, []string{client.Key}, []string{"server-state"}); pinErr == nil && len(pinned) == 1 {
		entries := make([]string, 0, len(pinned[0].StateFacts.Rebuilds))
		for _, value := range pinned[0].StateFacts.Rebuilds {
			entries = append(entries, fmt.Sprintf("%s:limit=%d:continuation=%t", value.ScopeID, value.PageLimit, value.HasContinuation))
		}
		pinnedRebuilds = fmt.Sprintf("%v", entries)
	}

	expire, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-EXPIRE-001", "model/expire-client-generation")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, applyErr := controller.ApplyStep(ctx, expire); applyErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("expire Kotlin Android retention-reconnect generation: %w", kotlinResultError(applyErr, observed.Disposition))
	}

	rejectedPush, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-REJECT-OLD-001", "push/submit")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	renew, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-RENEW-001", "connect/send")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	releaseFault()
	renewalCall, err := runRetentionReconnectRenewal(ctx, scenario, platform, client, steps, rejectedPush, renew)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	finalSnapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("capture Kotlin Android retention-reconnect renewed queue: %w", err)
	}
	if err := validateRetentionReconnectQueue(finalSnapshot, mutationIDs); err != nil {
		return RetentionReconnectResult{}, err
	}

	compact, err := kotlinScenarioOperation(steps, "STEP-RETENTION-RECONNECT-COMPACT-001", "model/compact-scope")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, applyErr := controller.ApplyStep(ctx, compact); applyErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("compact Kotlin Android retention-reconnect scope: %w", kotlinResultError(applyErr, observed.Disposition))
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"pending-mutations", "rejected-mutations", "rebuild-state"})
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("capture Kotlin Android retention-reconnect client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return RetentionReconnectResult{}, fmt.Errorf("capture Kotlin Android retention-reconnect server state: %w", kotlinResultError(err, ""))
	}
	if err := validateRetentionReconnectCompaction(serverCaptures[0].StateFacts, rebuildPin); err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("%w (rebuilds when pinned: %s)", err, pinnedRebuilds)
	}
	identities, err := resolveRetentionReconnectIdentities(controller, scenario.NativeIdentityAliases, initialCall, renewalCall, serverCaptures[0].StateFacts, sealedBatchID, sealedMutationIDs, intentPrimary, pinnedRebuildIdentity)
	if err != nil {
		return RetentionReconnectResult{}, err
	}

	return RetentionReconnectResult{
		InitialCall:        initialCall,
		RenewalCall:        renewalCall,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: identities,
	}, nil
}

func runRetentionReconnectInitialCall(ctx context.Context, scenario scenarios.Scenario, platform *Platform, client Client) (RetentionReconnectCall, error) {
	call, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return RetentionReconnectCall{}, fmt.Errorf("run Kotlin Android retention-reconnect sealed push: %w", err)
	}
	wire, err := retentionReconnectWireExpectation(scenario, "STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001")
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	outcomes := make([]string, 0, len(call.transportObservations))
	for _, observation := range call.transportObservations {
		entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
		if observation.ErrorCode != nil {
			entry += ":" + *observation.ErrorCode
		}
		outcomes = append(outcomes, entry)
	}
	if call.Completion != retentionReconnectNativeCompletion(wire) {
		return RetentionReconnectCall{}, fmt.Errorf("Kotlin Android retention-reconnect sealed push completion = %q, want %q; observed %v",
			call.Completion, retentionReconnectNativeCompletion(wire), outcomes)
	}
	pushes := make([]TransportObservation, 0, 1)
	for _, observed := range call.transportObservations {
		if observed.OperationClass == "push" {
			pushes = append(pushes, observed)
		}
	}
	// The contract bounds the retry delay, the durable retry metadata, and the
	// request identity. It does not bound the attempt count inside one public
	// call. The Kotlin engine retries the sealed push inside the call through
	// native backoff, so the call records one push observation per attempt.
	// Every attempt must observe the authored temporary-unavailable wire result.
	if len(pushes) == 0 {
		return RetentionReconnectCall{}, fmt.Errorf("Kotlin Android retention-reconnect sealed push transport count = 0, want at least 1; observed %v", outcomes)
	}
	for _, push := range pushes {
		if err := validateKotlinWireObservation(scenario, "STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001", push); err != nil {
			return RetentionReconnectCall{}, err
		}
	}
	return RetentionReconnectCall{Completion: call.Completion, Call: nil, Transport: call.transportObservations}, nil
}

func runRetentionReconnectRenewal(ctx context.Context, scenario scenarios.Scenario, platform *Platform, client Client, steps map[scenarios.StepID]scenarios.Step, rejectedPush, renew scenarios.Operation) (RetentionReconnectCall, error) {
	step := steps["STEP-RETENTION-RECONNECT-REJECT-OLD-001"]
	if step.NativeBinding == nil || step.NativeBinding.CallID == nil {
		return RetentionReconnectCall{}, errors.New("Kotlin Android retention-reconnect renewal call identity is absent")
	}
	state, err := platform.clientFor(client)
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	state.mu.Lock()
	checkpoint := state.session.Checkpoint()
	state.mu.Unlock()
	wire, err := retentionReconnectWireExpectation(scenario, "STEP-RETENTION-RECONNECT-RENEW-001")
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	completion, transport, err := awaitRetentionReconnectRecovery(ctx, platform, client, state, checkpoint, retentionReconnectNativeCompletion(wire))
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	if completion != retentionReconnectNativeCompletion(wire) {
		return RetentionReconnectCall{}, fmt.Errorf("Kotlin Android retention-reconnect renewal settled at %q, want %q", completion, retentionReconnectNativeCompletion(wire))
	}
	transportSteps := []scenarios.StepID{"STEP-RETENTION-RECONNECT-REJECT-OLD-001", "STEP-RETENTION-RECONNECT-RENEW-001"}
	if len(transport) != len(transportSteps) {
		return RetentionReconnectCall{}, errors.New("Kotlin Android retention-reconnect renewal transport count differs from its authored call")
	}
	for index, stepID := range transportSteps {
		if err := validateKotlinWireObservation(scenario, string(stepID), transport[index]); err != nil {
			return RetentionReconnectCall{}, err
		}
	}
	return RetentionReconnectCall{Completion: completion, Call: nil, Transport: transport}, nil
}

// retentionReconnectRenewalPair selects the rejected push and its following generation-renewing connect.
func retentionReconnectRenewalPair(transport []TransportObservation) (TransportObservation, TransportObservation, bool) {
	for index, observation := range transport {
		if observation.OperationClass != "push" || observation.ErrorCode == nil || *observation.ErrorCode != "client_generation_expired" {
			continue
		}
		for _, candidate := range transport[index+1:] {
			if candidate.OperationClass == "connect" {
				return observation, candidate, true
			}
		}
		return TransportObservation{}, TransportObservation{}, false
	}
	return TransportObservation{}, TransportObservation{}, false
}

// awaitRetentionReconnectRecovery waits for the client's automatic expired-generation recovery.
func awaitRetentionReconnectRecovery(ctx context.Context, platform *Platform, client Client, state *platformClient, checkpoint uint64, want string) (string, []TransportObservation, error) {
	deadline, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ctx = deadline
	for {
		snapshot, err := platform.scenarioSnapshot(ctx, client)
		if err != nil {
			return "", nil, fmt.Errorf("poll Kotlin Android retention-reconnect recovery: %w", err)
		}
		state.mu.Lock()
		transport, observationErr := state.session.ObservationsAfter(checkpoint)
		state.mu.Unlock()
		if observationErr != nil {
			return "", nil, fmt.Errorf("capture Kotlin Android retention-reconnect renewal transport: %w", observationErr)
		}
		status := pushResponseLossOptionalString(snapshot.Status)
		settled := snapshot.Failure == nil && status != "backoff" && status != "error"
		rejected, renewed, paired := retentionReconnectRenewalPair(transport)
		if settled && paired {
			return want, []TransportObservation{rejected, renewed}, nil
		}
		select {
		case <-ctx.Done():
			outcomes := make([]string, 0, len(transport))
			for _, observation := range transport {
				entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
				if observation.ErrorCode != nil {
					entry += ":" + *observation.ErrorCode
				}
				outcomes = append(outcomes, entry)
			}
			failure := "none"
			if snapshot.Failure != nil {
				failure = fmt.Sprintf("%s/%s/%s", snapshot.Failure.Operation, snapshot.Failure.Code, snapshot.Failure.RecoveryAction)
			}
			return "", nil, fmt.Errorf("wait for Kotlin Android retention-reconnect recovery: %w (status %s, want %s, failure %s, transport %v)", ctx.Err(), status, want, failure, outcomes)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func validateRetentionReconnectBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, client Client) error {
	allowedWireSteps := map[scenarios.StepID]struct{}{
		"STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001": {},
		"STEP-RETENTION-RECONNECT-REBUILD-PIN-001":    {},
		"STEP-RETENTION-RECONNECT-REJECT-OLD-001":     {},
		"STEP-RETENTION-RECONNECT-RENEW-001":          {},
	}
	expected := []retentionReconnectBinding{
		{"STEP-RETENTION-RECONNECT-LOCAL-WRITE-001", "local/write", "local-write", "", "", false, ""},
		{"STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001", "push/submit", "public-call", "synchronous", "start", true, "initial"},
		{"STEP-RETENTION-RECONNECT-COMMIT-001", "model/commit-source-transaction", "controller", "", "", false, ""},
		{"STEP-RETENTION-RECONNECT-MATERIALIZE-001", "process/materialize-source-transaction", "controller", "", "", false, ""},
		{"STEP-RETENTION-RECONNECT-REBUILD-PIN-001", "rebuild/request-page", "controller", "", "", false, ""},
		{"STEP-RETENTION-RECONNECT-EXPIRE-001", "model/expire-client-generation", "controller", "", "", false, ""},
		{"STEP-RETENTION-RECONNECT-REJECT-OLD-001", "push/submit", "public-call", "observe", "", false, "renewal"},
		{"STEP-RETENTION-RECONNECT-RENEW-001", "connect/send", "public-call", "await-call", "", true, "renewal"},
		{"STEP-RETENTION-RECONNECT-COMPACT-001", "model/compact-scope", "controller", "", "", false, ""},
	}
	callIDs := make(map[string]scenarios.NativeCallID)
	wired := make(map[scenarios.StepID]struct{}, len(scenario.WireExpectations))
	for _, wire := range scenario.WireExpectations {
		if _, allowed := allowedWireSteps[wire.StepID]; !allowed {
			return fmt.Errorf("Kotlin Android retention-reconnect wire expectation %s is unexpected", wire.StepID)
		}
		if _, duplicate := wired[wire.StepID]; duplicate {
			return fmt.Errorf("Kotlin Android retention-reconnect wire expectation %s is duplicated", wire.StepID)
		}
		wired[wire.StepID] = struct{}{}
	}
	if len(wired) != len(allowedWireSteps) {
		return errors.New("Kotlin Android retention-reconnect wire expectations are incomplete")
	}
	for _, wanted := range expected {
		step, found := steps[wanted.id]
		if !found {
			return fmt.Errorf("Kotlin Android retention-reconnect binding %s is absent", wanted.id)
		}
		if _, err := kotlinScenarioOperation(steps, string(wanted.id), wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Kotlin Android retention-reconnect binding %s is invalid", wanted.id)
		}
		if wanted.kind == "local-write" || wanted.kind == "public-call" {
			if err := kotlinScenarioClient(step, client); err != nil {
				return err
			}
		}
		if wanted.kind != "public-call" {
			continue
		}
		if _, err := retentionReconnectWireExpectation(scenario, wanted.id); err != nil {
			return err
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Kotlin Android retention-reconnect binding %s has no call identity", wanted.id)
		}
		if prior, found := callIDs[wanted.call]; found && prior != *binding.CallID {
			return fmt.Errorf("Kotlin Android retention-reconnect call %q has inconsistent identities", wanted.call)
		}
		callIDs[wanted.call] = *binding.CallID
		wire, err := retentionReconnectWireExpectation(scenario, wanted.id)
		if err != nil {
			return err
		}
		if wanted.terminal && binding.Completion != retentionReconnectNativeCompletion(wire) {
			return fmt.Errorf("Kotlin Android retention-reconnect step %s completion does not match its authored wire expectation", wanted.id)
		}
		if !wanted.terminal && binding.Completion != "" {
			return fmt.Errorf("Kotlin Android retention-reconnect step %s declares a nonterminal completion", wanted.id)
		}
	}
	if len(callIDs) != 2 {
		return errors.New("Kotlin Android retention-reconnect public call bindings are incomplete")
	}
	for stepID := range allowedWireSteps {
		if _, found := steps[stepID]; !found {
			return fmt.Errorf("Kotlin Android retention-reconnect wire expectation %s references an absent step", stepID)
		}
	}
	return nil
}

func retentionReconnectWireExpectation(scenario scenarios.Scenario, stepID scenarios.StepID) (scenarios.WireExpectation, error) {
	var found scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == stepID {
			found = wire
			count++
		}
	}
	if count != 1 {
		return scenarios.WireExpectation{}, fmt.Errorf("Kotlin Android retention-reconnect wire expectation %s count = %d, want 1", stepID, count)
	}
	return found, nil
}

func retentionReconnectNativeCompletion(wire scenarios.WireExpectation) string {
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

func validateRetentionReconnectNativeWire(scenario scenarios.Scenario, stepID string, observed blackbox.NativeStepObservation) error {
	wire, err := retentionReconnectWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != wire.HTTPStatus || observed.Wire.Retryable != wire.Retryable || !equalKotlinOptionalStrings(observed.Wire.ErrorCode, wire.ErrorCode) {
		return fmt.Errorf("Kotlin Android retention-reconnect controller wire %s differs from its authored expectation", stepID)
	}
	return nil
}

func registerRetentionReconnectPinClient(ctx context.Context, controller *blackbox.NativeController, rebuildPin scenarios.Operation) error {
	var pin struct {
		UserID   string          `json:"user_id"`
		ClientID string          `json:"client_id"`
		Schema   json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(rebuildPin.Payload, &pin); err != nil || pin.UserID == "" || pin.ClientID == "" || len(pin.Schema) == 0 {
		return errors.New("Kotlin Android retention-reconnect rebuild pin identity is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"user_id": pin.UserID, "client_id": pin.ClientID, "runtime_version": 3, "protocol_version": 3,
		"schema_reset": false, "schema": pin.Schema, "scope_set_version": 0, "known_scopes": []any{},
	})
	if err != nil {
		return errors.New("encode Kotlin Android retention-reconnect pin client connect failed")
	}
	connect := scenarios.Operation{ContractOperation: "connect", Name: "send", Payload: payload}
	observed, requestErr := controller.RequestStep(ctx, connect)
	if requestErr != nil || observed.Disposition != "success" {
		message := "none"
		if observed.Wire != nil {
			message = observed.Wire.Message
		}
		return fmt.Errorf("register Kotlin Android retention-reconnect pin client: %w (message %q)", kotlinResultError(requestErr, observed.Disposition), message)
	}
	return nil
}

type retentionReconnectPrimaryKey struct {
	Authored string
	Runtime  string
}

func retentionReconnectWrittenPrimaryKey(authored, bound scenarios.Operation) (retentionReconnectPrimaryKey, error) {
	authoredValue, err := retentionReconnectPrimaryKeyValue(authored)
	if err != nil {
		return retentionReconnectPrimaryKey{}, fmt.Errorf("Kotlin Android retention-reconnect authored write primary key is invalid: %w", err)
	}
	runtimeValue, err := retentionReconnectPrimaryKeyValue(bound)
	if err != nil {
		return retentionReconnectPrimaryKey{}, fmt.Errorf("Kotlin Android retention-reconnect bound write primary key is invalid: %w", err)
	}
	return retentionReconnectPrimaryKey{Authored: authoredValue, Runtime: runtimeValue}, nil
}

func retentionReconnectPrimaryKeyValue(operation scenarios.Operation) (string, error) {
	var payload struct {
		PK map[string]json.RawMessage `json:"pk"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.PK) == 0 {
		return "", errors.New("primary key is absent")
	}
	raw, found := payload.PK["value"]
	if !found {
		if len(payload.PK) != 1 {
			return "", errors.New("primary key is ambiguous")
		}
		for _, only := range payload.PK {
			raw = only
		}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", errors.New("primary key value is invalid")
	}
	return value, nil
}

func retentionReconnectPinnedRebuildID(operation scenarios.Operation) (string, error) {
	var payload struct {
		RebuildID string `json:"rebuild_id"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.RebuildID == "" {
		return "", errors.New("Kotlin Android retention-reconnect rebuild pin identity is absent")
	}
	return payload.RebuildID, nil
}

func retentionReconnectAuthoredMutationCount(operation scenarios.Operation) (int, error) {
	var payload struct {
		Request struct {
			Mutations []struct {
				MutationID string `json:"mutation_id"`
			} `json:"mutations"`
		} `json:"request"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Request.Mutations) == 0 {
		return 0, errors.New("Kotlin Android retention-reconnect sealed push mutations are invalid")
	}
	ids := make(map[string]struct{}, len(payload.Request.Mutations))
	for _, mutation := range payload.Request.Mutations {
		if mutation.MutationID == "" {
			return 0, errors.New("Kotlin Android retention-reconnect sealed push mutation identity is absent")
		}
		if _, duplicate := ids[mutation.MutationID]; duplicate {
			return 0, errors.New("Kotlin Android retention-reconnect sealed push mutation identity is duplicated")
		}
		ids[mutation.MutationID] = struct{}{}
	}
	return len(ids), nil
}

func retentionReconnectQueueIdentityEvidence(snapshot Result, expectedCount int) (string, []string, error) {
	if snapshot.MutationLedgerCount == nil || *snapshot.MutationLedgerCount != expectedCount || expectedCount <= 0 || !presentJSON(snapshot.RetainedMutations) {
		return "", nil, errors.New("Kotlin Android retention-reconnect sealed queue detail is incomplete")
	}
	var values []retainedMutation
	if err := decodeFactArray(snapshot.RetainedMutations, &values, maximumRecords); err != nil || len(values) != expectedCount {
		return "", nil, errors.New("Kotlin Android retention-reconnect sealed queue detail is invalid")
	}
	if _, err := androidQueuedMutationFacts(snapshot.RetainedMutations); err != nil {
		return "", nil, fmt.Errorf("Kotlin Android retention-reconnect sealed queue detail is invalid: %w", err)
	}
	batchID := ""
	mutationIDs := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.MutationID == "" || value.Status == "" || value.SealedBatchID == nil || *value.SealedBatchID == "" {
			return "", nil, errors.New("Kotlin Android retention-reconnect sealed queue identity is incomplete")
		}
		if _, duplicate := seen[value.MutationID]; duplicate {
			return "", nil, errors.New("Kotlin Android retention-reconnect sealed queue mutation identity is duplicated")
		}
		seen[value.MutationID] = struct{}{}
		if batchID == "" {
			batchID = *value.SealedBatchID
		} else if batchID != *value.SealedBatchID {
			return "", nil, errors.New("Kotlin Android retention-reconnect sealed queue batch identity is inconsistent")
		}
		mutationIDs = append(mutationIDs, value.MutationID)
	}
	return batchID, mutationIDs, nil
}

// retentionReconnectSealedIdentities records the runtime mutation identities in the sealed queue.
func retentionReconnectSealedIdentities(sealed []string, authoredMutations int) (map[string]struct{}, error) {
	if len(sealed) != authoredMutations {
		return nil, fmt.Errorf("Kotlin Android retention-reconnect sealed batch carried %d mutations, want %d", len(sealed), authoredMutations)
	}
	ids := make(map[string]struct{}, len(sealed))
	for _, mutation := range sealed {
		if mutation == "" {
			return nil, errors.New("Kotlin Android retention-reconnect sealed batch mutation identity is absent")
		}
		if _, duplicate := ids[mutation]; duplicate {
			return nil, errors.New("Kotlin Android retention-reconnect sealed batch mutation identity is duplicated")
		}
		ids[mutation] = struct{}{}
	}
	return ids, nil
}

func validateRetentionReconnectQueue(snapshot Result, expected map[string]struct{}) error {
	if snapshot.MutationLedgerCount == nil || *snapshot.MutationLedgerCount != len(expected) || !presentJSON(snapshot.RetainedMutations) {
		return errors.New("Kotlin Android retention-reconnect durable queue count differs from the authored sealed intent")
	}
	var values []retainedMutation
	if err := decodeFactArray(snapshot.RetainedMutations, &values, maximumRecords); err != nil || len(values) != len(expected) {
		return errors.New("Kotlin Android retention-reconnect durable queue detail differs from the authored sealed intent")
	}
	if _, err := androidQueuedMutationFacts(snapshot.RetainedMutations); err != nil {
		return fmt.Errorf("Kotlin Android retention-reconnect durable queue detail is invalid: %w", err)
	}
	seen := make(map[string]struct{}, len(values))
	for _, mutation := range values {
		if mutation.MutationID == "" || mutation.Status == "" {
			return errors.New("Kotlin Android retention-reconnect durable queue record is incomplete")
		}
		if _, wanted := expected[mutation.MutationID]; !wanted {
			return errors.New("Kotlin Android retention-reconnect durable queue changed the authored mutation identity")
		}
		if _, duplicate := seen[mutation.MutationID]; duplicate {
			return errors.New("Kotlin Android retention-reconnect durable queue repeats an authored mutation")
		}
		seen[mutation.MutationID] = struct{}{}
	}
	return nil
}

func validateRetentionReconnectCompaction(server scenarios.StateFacts, rebuild scenarios.Operation) error {
	var payload struct {
		ScopeID   string `json:"scope_id"`
		RebuildID string `json:"rebuild_id"`
		Limit     uint64 `json:"limit"`
	}
	if err := json.Unmarshal(rebuild.Payload, &payload); err != nil || payload.ScopeID == "" || payload.RebuildID == "" || payload.Limit == 0 {
		return errors.New("Kotlin Android retention-reconnect rebuild pin payload is invalid")
	}
	matches := 0
	for _, value := range server.Rebuilds {
		if value.ScopeID != payload.ScopeID || value.RebuildID != payload.RebuildID {
			continue
		}
		if value.PageLimit != payload.Limit || !value.HasContinuation {
			return fmt.Errorf("Kotlin Android retention-reconnect rebuild pin differs from its authored request: scope %q rebuild %q page limit %d continuation %t; authored limit %d", value.ScopeID, value.RebuildID, value.PageLimit, value.HasContinuation, payload.Limit)
		}
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("Kotlin Android retention-reconnect active rebuild pin is absent: %d rebuilds match pin %q for scope %q", matches, payload.RebuildID, payload.ScopeID)
	}
	scopeMatches := 0
	for _, scope := range server.Scopes {
		if scope.ScopeID == payload.ScopeID {
			scopeMatches++
		}
	}
	if scopeMatches != 1 {
		return errors.New("Kotlin Android retention-reconnect compacted scope is absent")
	}
	return nil
}

func resolveRetentionReconnectIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, initial, renewal RetentionReconnectCall, server scenarios.StateFacts, sealedBatchID string, sealedMutations []string, written retentionReconnectPrimaryKey, pinnedRebuildID string) ([]blackbox.NativeIdentityResolution, error) {
	serverAliases := make([]scenarios.NativeIdentityAlias, 0, len(aliases))
	for _, alias := range aliases {
		if alias.Kind == "mutation-id" || alias.Kind == "batch-id" {
			continue
		}
		if alias.Kind == "primary-key" && retentionReconnectAliasNames(alias, written.Authored) {
			continue
		}
		serverAliases = append(serverAliases, alias)
	}
	values, err := controller.IdentityValues(serverAliases)
	if err != nil {
		return nil, err
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}
	observed, err := retentionReconnectObservedIdentityValues(aliases, initial, renewal, server, sealedBatchID, sealedMutations, written, pinnedRebuildID)
	if err != nil {
		return nil, err
	}
	for alias, value := range observed {
		runtime[alias] = value
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("Kotlin Android retention-reconnect alias %q has no runtime evidence", alias.Alias)
		}
	}
	return resolveKotlinNativeIdentities(aliases, runtime)
}

func retentionReconnectObservedIdentityValues(aliases []scenarios.NativeIdentityAlias, initial, renewal RetentionReconnectCall, server scenarios.StateFacts, sealedBatchID string, sealedMutations []string, written retentionReconnectPrimaryKey, pinnedRebuildID string) (map[string]json.RawMessage, error) {
	initialPushes := 0
	for _, observation := range initial.Transport {
		if observation.OperationClass == "push" {
			initialPushes++
		}
	}
	// The initial call records one push per in-call backoff attempt, and the
	// contract does not bound that count. The renewal pair shape is authored.
	if initialPushes == 0 || len(renewal.Transport) != 2 || renewal.Transport[0].OperationClass != "push" || renewal.Transport[1].OperationClass != "connect" {
		return nil, fmt.Errorf("Kotlin Android retention-reconnect transport identity evidence is incomplete: initial %v, renewal %v", retentionReconnectTransportDescription(initial.Transport), retentionReconnectTransportDescription(renewal.Transport))
	}
	var generation int64
	var scopeSetVersion int64
	generationObserved := false
	for _, observation := range append(append([]TransportObservation{}, initial.Transport...), renewal.Transport...) {
		if observation.RequestFacts == nil || observation.RequestFacts.ClientGeneration == nil {
			return nil, errors.New("Kotlin Android retention-reconnect client generation evidence is absent")
		}
		observedGeneration := *observation.RequestFacts.ClientGeneration
		if !generationObserved {
			generation = observedGeneration
			generationObserved = true
		} else if generation != observedGeneration {
			return nil, errors.New("Kotlin Android retention-reconnect client generation evidence is inconsistent")
		}
	}
	renewingConnect := renewal.Transport[1]
	if renewingConnect.RequestFacts == nil || renewingConnect.RequestFacts.ScopeSetVersion == nil {
		return nil, errors.New("Kotlin Android retention-reconnect scope-set version evidence is invalid")
	}
	scopeSetVersion = *renewingConnect.RequestFacts.ScopeSetVersion
	if !generationObserved {
		return nil, errors.New("Kotlin Android retention-reconnect transport identities are incomplete")
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	var err error
	for _, alias := range aliases {
		switch alias.Kind {
		case "client-generation":
			runtime[alias.Alias], err = json.Marshal(generation)
		case "scope-set-version":
			runtime[alias.Alias], err = json.Marshal(scopeSetVersion)
		case "row-version", "checksum":
			rowValue, rowErr := retentionReconnectObservedRowValue(alias, aliases, server.Rows)
			if rowErr != nil {
				return nil, rowErr
			}
			runtime[alias.Alias], err = json.Marshal(rowValue)
		case "rebuild-id":
			rebuildID, rebuildErr := retentionReconnectObservedRebuildID(alias, aliases, server.Rebuilds, pinnedRebuildID)
			if rebuildErr != nil {
				return nil, rebuildErr
			}
			runtime[alias.Alias], err = json.Marshal(rebuildID)
		case "mutation-id":
			if len(sealedMutations) != 1 {
				return nil, fmt.Errorf("Kotlin Android retention-reconnect sealed batch carried %d mutation identities, want 1", len(sealedMutations))
			}
			runtime[alias.Alias], err = json.Marshal(sealedMutations[0])
		case "batch-id":
			if sealedBatchID == "" {
				return nil, errors.New("Kotlin Android retention-reconnect sealed batch identity is absent")
			}
			runtime[alias.Alias], err = json.Marshal(sealedBatchID)
		case "primary-key":
			if !retentionReconnectAliasNames(alias, written.Authored) {
				continue
			}
			runtime[alias.Alias], err = json.Marshal(written.Runtime)
		}
		if err != nil {
			return nil, fmt.Errorf("encode Kotlin Android retention-reconnect alias %q: %w", alias.Alias, err)
		}
	}
	return runtime, nil
}

func retentionReconnectTransportDescription(observations []TransportObservation) []string {
	result := make([]string, 0, len(observations))
	for _, observation := range observations {
		result = append(result, fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode))
	}
	return result
}

func retentionReconnectObservedRowValue(alias scenarios.NativeIdentityAlias, aliases []scenarios.NativeIdentityAlias, rows []scenarios.RowFact) (string, error) {
	if len(rows) == 0 {
		return "", fmt.Errorf("Kotlin Android retention-reconnect %s evidence is absent", alias.Kind)
	}
	values := make(map[string]struct{})
	for _, primary := range aliases {
		if primary.Kind != "primary-key" || !retentionReconnectAliasesShareOwner(alias, primary) {
			continue
		}
		var authored string
		if err := json.Unmarshal(primary.Value, &authored); err != nil || authored == "" {
			return "", fmt.Errorf("Kotlin Android retention-reconnect primary-key evidence for %q is invalid", alias.Alias)
		}
		encoded, err := json.Marshal(authored)
		if err != nil {
			return "", fmt.Errorf("encode Kotlin Android retention-reconnect primary-key evidence for %q: %w", alias.Alias, err)
		}
		for _, row := range rows {
			if row.CanonicalWireJSON != string(encoded) {
				continue
			}
			value := row.Version
			if alias.Kind == "checksum" {
				value = row.Checksum
			}
			if value == "" {
				return "", fmt.Errorf("Kotlin Android retention-reconnect %s evidence for %q is empty", alias.Kind, alias.Alias)
			}
			values[value] = struct{}{}
		}
	}
	if len(values) != 1 {
		return "", fmt.Errorf("Kotlin Android retention-reconnect %s evidence for %q is ambiguous", alias.Kind, alias.Alias)
	}
	for value := range values {
		return value, nil
	}
	return "", fmt.Errorf("Kotlin Android retention-reconnect %s evidence for %q is absent", alias.Kind, alias.Alias)
}

func retentionReconnectObservedRebuildID(alias scenarios.NativeIdentityAlias, aliases []scenarios.NativeIdentityAlias, rebuilds []scenarios.RebuildFact, pinnedRebuildID string) (string, error) {
	var scopeID string
	for _, scope := range aliases {
		if scope.Kind != "scope" || !retentionReconnectAliasesShareOwner(alias, scope) {
			continue
		}
		if err := json.Unmarshal(scope.Value, &scopeID); err != nil || scopeID == "" {
			return "", fmt.Errorf("Kotlin Android retention-reconnect scope evidence for %q is invalid", alias.Alias)
		}
		break
	}
	if scopeID == "" {
		return "", fmt.Errorf("Kotlin Android retention-reconnect rebuild scope evidence for %q is absent", alias.Alias)
	}
	var rebuildID string
	matches := 0
	for _, rebuild := range rebuilds {
		if rebuild.ScopeID != scopeID || (pinnedRebuildID != "" && rebuild.RebuildID != pinnedRebuildID) {
			continue
		}
		if rebuild.RebuildID == "" {
			return "", fmt.Errorf("Kotlin Android retention-reconnect rebuild evidence for %q is invalid", alias.Alias)
		}
		matches++
		rebuildID = rebuild.RebuildID
	}
	if matches != 1 {
		return "", fmt.Errorf("Kotlin Android retention-reconnect rebuild evidence for %q is ambiguous", alias.Alias)
	}
	return rebuildID, nil
}

func retentionReconnectAliasesShareOwner(left, right scenarios.NativeIdentityAlias) bool {
	for _, leftOwner := range left.StepIDs {
		for _, rightOwner := range right.StepIDs {
			if leftOwner == rightOwner {
				return true
			}
		}
	}
	return false
}

func retentionReconnectAliasNames(alias scenarios.NativeIdentityAlias, authored string) bool {
	var value string
	if err := json.Unmarshal(alias.Value, &value); err != nil {
		return false
	}
	return value != "" && value == authored
}
