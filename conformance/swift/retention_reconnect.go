package swift

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

// RetentionReconnectResult records direct Swift evidence for expired-generation recovery.
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
	Call       *CallResult
	Transport  []transportObservation
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

// RunRetentionReconnectScenario executes the authored expired-generation reconnect flow through Swift.
func RunRetentionReconnectScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (RetentionReconnectResult, error) {
	steps, err := swiftScenarioStepMap(scenario, retentionReconnectScenarioID, 9)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if controller == nil || platform == nil {
		return RetentionReconnectResult{}, errors.New("Swift retention-reconnect dependencies are unavailable")
	}
	if err := validateRetentionReconnectBindings(scenario, steps, client); err != nil {
		return RetentionReconnectResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("install Swift retention-reconnect contract: %w", err)
	}
	if err := platform.Install(ctx, client, "current", ""); err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("install Swift retention-reconnect client: %w", err)
	}

	localWrite, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-LOCAL-WRITE-001", "local/write")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	authoredWrite := localWrite
	localWrite, err = controller.ApplicationWrite(localWrite)
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("bind Swift retention-reconnect local write: %w", err)
	}
	// The locally written row is never delivered as an applied transaction, so
	// no server record binds its identity. The bound write carries the runtime
	// identity the controller assigned it.
	intentPrimary, err := retentionReconnectWrittenPrimaryKey(authoredWrite, localWrite)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	local, err := platform.ApplyStep(ctx, client, localWrite)
	if err != nil || local.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("apply Swift retention-reconnect local write: %w", resultError(err, local.Disposition))
	}

	sealedPush, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001", "push/submit")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	// The sealed batch stays undelivered until the authored generation expiry.
	// The client retries that batch through durable backoff, so the fault
	// outlives the call that observes it and is released only when the scenario
	// expects the batch to reach the server.
	releaseFault, armed, err := platform.armTemporaryUnavailablePush(RequestOperations{sealedPush})
	if err != nil || !armed {
		return RetentionReconnectResult{}, fmt.Errorf("arm Swift retention-reconnect temporary-unavailable push: %w", err)
	}
	defer releaseFault()
	initialCall, err := runRetentionReconnectInitialCall(ctx, scenario, platform, client, sealedPush)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	authoredMutations, err := retentionReconnectAuthoredMutationCount(sealedPush)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	initialSnapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("capture Swift retention-reconnect sealed queue: %w", err)
	}
	// The authored mutation identity is an alias. The sealed batch the proxy
	// intercepted carries the identity the client minted, so the durable queue
	// is compared against the batch that actually left the client.
	mutationIDs, err := retentionReconnectSealedIdentities(platform.SealedPushMutationIDs(), authoredMutations)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if err := validateRetentionReconnectQueue(initialSnapshot, mutationIDs); err != nil {
		return RetentionReconnectResult{}, err
	}

	commit, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, applyErr := controller.ApplyStep(ctx, commit); applyErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("commit Swift retention-reconnect history: %w", resultError(applyErr, observed.Disposition))
	}
	materialize, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, processErr := controller.ProcessStep(ctx, nil, materialize); processErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("materialize Swift retention-reconnect history: %w", resultError(processErr, observed.Disposition))
	}

	rebuildPin, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-REBUILD-PIN-001", "rebuild/request-page")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if err := registerRetentionReconnectPinClient(ctx, controller, rebuildPin); err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, requestErr := controller.RequestStep(ctx, rebuildPin); requestErr != nil || observed.Disposition != "success" {
		// The disposition alone cannot name the rejection, so the reported code
		// and wire status accompany it.
		status := "none"
		message := "none"
		if observed.Wire != nil {
			status = fmt.Sprintf("%d", observed.Wire.HTTPStatus)
			message = observed.Wire.Message
		}
		return RetentionReconnectResult{}, fmt.Errorf("create Swift retention-reconnect rebuild pin: %w (error code %s, http status %s, message %q)",
			resultError(requestErr, observed.Disposition), optionalStringOrNone(observed.ErrorCode), status, message)
	} else if err := validateRetentionReconnectNativeWire(scenario, "STEP-RETENTION-RECONNECT-REBUILD-PIN-001", observed); err != nil {
		return RetentionReconnectResult{}, err
	}
	// The scenario names the rebuild it pins, and that identity distinguishes
	// the pin from the rebuilds the client performs for its own reasons.
	pinnedRebuildIdentity, err := retentionReconnectPinnedRebuildID(rebuildPin)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	// The pin must hold an incomplete rebuild, because a completed rebuild
	// pins no retention. Recording its state here separates a pin that never
	// held a continuation from one a later step released.
	pinnedRebuilds := "unavailable"
	if pinned, pinErr := controller.Capture(ctx, []string{client.Key}, []string{"server-state"}); pinErr == nil && len(pinned) == 1 {
		entries := make([]string, 0, len(pinned[0].StateFacts.Rebuilds))
		for _, value := range pinned[0].StateFacts.Rebuilds {
			entries = append(entries, fmt.Sprintf("%s:limit=%d:continuation=%t", value.ScopeID, value.PageLimit, value.HasContinuation))
		}
		pinnedRebuilds = fmt.Sprintf("%v", entries)
	}

	expire, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-EXPIRE-001", "model/expire-client-generation")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, applyErr := controller.ApplyStep(ctx, expire); applyErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("expire Swift retention-reconnect generation: %w", resultError(applyErr, observed.Disposition))
	}

	rejectedPush, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-REJECT-OLD-001", "push/submit")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	renew, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-RENEW-001", "connect/send")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	// The generation has expired, so the batch may now reach the server, where
	// it is rejected as an expired generation.
	releaseFault()
	renewalCall, err := runRetentionReconnectRenewal(ctx, scenario, platform, client, steps, rejectedPush, renew)
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	finalSnapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("capture Swift retention-reconnect renewed queue: %w", err)
	}
	if err := validateRetentionReconnectQueue(finalSnapshot, mutationIDs); err != nil {
		return RetentionReconnectResult{}, err
	}

	compact, err := swiftScenarioOperation(steps, "STEP-RETENTION-RECONNECT-COMPACT-001", "model/compact-scope")
	if err != nil {
		return RetentionReconnectResult{}, err
	}
	if observed, applyErr := controller.ApplyStep(ctx, compact); applyErr != nil || observed.Disposition != "success" {
		return RetentionReconnectResult{}, fmt.Errorf("compact Swift retention-reconnect scope: %w", resultError(applyErr, observed.Disposition))
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"pending-mutations", "rejected-mutations", "rebuild-state"})
	if err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("capture Swift retention-reconnect client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return RetentionReconnectResult{}, fmt.Errorf("capture Swift retention-reconnect server state: %w", err)
	}
	if err := validateRetentionReconnectCompaction(serverCaptures[0].StateFacts, rebuildPin); err != nil {
		return RetentionReconnectResult{}, fmt.Errorf("%w (rebuilds when pinned: %s)", err, pinnedRebuilds)
	}
	identities, err := resolveRetentionReconnectIdentities(controller, scenario.NativeIdentityAliases, initialCall, renewalCall, serverCaptures[0].StateFacts, platform.SealedPushBatchID(), platform.SealedPushMutationIDs(), intentPrimary, pinnedRebuildIdentity)
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

func runRetentionReconnectInitialCall(ctx context.Context, scenario scenarios.Scenario, platform *Platform, client Client, sealedPush scenarios.Operation) (RetentionReconnectCall, error) {
	call, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return RetentionReconnectCall{}, fmt.Errorf("run Swift retention-reconnect sealed push: %w", err)
	}
	wire, err := retentionReconnectWireExpectation(scenario, "STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001")
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	if call.Completion != retentionReconnectNativeCompletion(wire) {
		outcomes := make([]string, 0, len(call.transportObservations))
		for _, observation := range call.transportObservations {
			entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
			if observation.ErrorCode != nil {
				entry += ":" + *observation.ErrorCode
			}
			outcomes = append(outcomes, entry)
		}
		// The proxy push count separates a client that never observed the
		// injected response from a push that never reached the client.
		return RetentionReconnectCall{}, fmt.Errorf("Swift retention-reconnect sealed push completion = %q, want %q; observed %v; proxy saw %d pushes %v; fault misses %v",
			call.Completion, retentionReconnectNativeCompletion(wire), outcomes, platform.ProxiedPushCount(), platform.ProxiedPushOutcomes(), platform.TemporaryUnavailablePushMisses())
	}
	pushes := make([]transportObservation, 0, 1)
	for _, observed := range call.transportObservations {
		if observed.OperationClass == "push" {
			pushes = append(pushes, observed)
		}
	}
	if len(pushes) != 1 {
		return RetentionReconnectCall{}, errors.New("Swift retention-reconnect sealed push has an unexpected transport count")
	}
	if err := validateSwiftWireObservation(scenario, "STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001", pushes[0]); err != nil {
		// The proxy record separates an injected response the client observed
		// from a push the proxy forwarded upstream unchanged. The full
		// observation list shows which operation the client recorded.
		all := make([]string, 0, len(call.transportObservations))
		for _, observation := range call.transportObservations {
			entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
			if observation.ErrorCode != nil {
				entry += ":" + *observation.ErrorCode
			}
			all = append(all, entry)
		}
		return RetentionReconnectCall{}, fmt.Errorf("%w; observed %v; proxy saw %d pushes %v; fault misses %v",
			err, all, platform.ProxiedPushCount(), platform.ProxiedPushOutcomes(), platform.TemporaryUnavailablePushMisses())
	}
	return RetentionReconnectCall{Completion: call.Completion, Transport: call.transportObservations}, nil
}

func runRetentionReconnectRenewal(ctx context.Context, scenario scenarios.Scenario, platform *Platform, client Client, steps map[scenarios.StepID]scenarios.Step, rejectedPush, renew scenarios.Operation) (RetentionReconnectCall, error) {
	step := steps["STEP-RETENTION-RECONNECT-REJECT-OLD-001"]
	if step.NativeBinding == nil || step.NativeBinding.CallID == nil {
		return RetentionReconnectCall{}, errors.New("Swift retention-reconnect renewal call identity is absent")
	}
	state, err := platform.client(client)
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	state.mu.Lock()
	checkpoint := state.session.Checkpoint()
	state.mu.Unlock()

	// An expired client generation is an automatic recovery dispatch, not an
	// error state, so no public method drives this step. The client resumes its
	// interrupted push on its durable deadline, receives the expired-generation
	// rejection, reconnects, and settles. The harness observes that recovery.
	wire, err := retentionReconnectWireExpectation(scenario, "STEP-RETENTION-RECONNECT-RENEW-001")
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	completion, transport, err := awaitRetentionReconnectRecovery(ctx, scenario, state, checkpoint, retentionReconnectNativeCompletion(wire))
	if err != nil {
		return RetentionReconnectCall{}, err
	}
	if completion != retentionReconnectNativeCompletion(wire) {
		return RetentionReconnectCall{}, fmt.Errorf("Swift retention-reconnect renewal settled at %q, want %q",
			completion, retentionReconnectNativeCompletion(wire))
	}
	transportSteps := []scenarios.StepID{"STEP-RETENTION-RECONNECT-REJECT-OLD-001", "STEP-RETENTION-RECONNECT-RENEW-001"}
	if len(transport) != len(transportSteps) {
		return RetentionReconnectCall{}, errors.New("Swift retention-reconnect renewal transport count differs from its authored call")
	}
	for index, stepID := range transportSteps {
		if err := validateSwiftWireObservation(scenario, string(stepID), transport[index]); err != nil {
			return RetentionReconnectCall{}, err
		}
	}
	return RetentionReconnectCall{Completion: completion, Transport: transport}, nil
}

// retentionReconnectRenewalPair selects the rejected push and the connect that
// renews the generation. The client resumes its normal loop after the renewal,
// so the authored pair is the rejection and the first connect that follows it.
func retentionReconnectRenewalPair(transport []transportObservation) (transportObservation, transportObservation, bool) {
	for index, observation := range transport {
		if observation.OperationClass != "push" || observation.ErrorCode == nil ||
			*observation.ErrorCode != "client_generation_expired" {
			continue
		}
		for _, candidate := range transport[index+1:] {
			if candidate.OperationClass == "connect" {
				return observation, candidate, true
			}
		}
		return transportObservation{}, transportObservation{}, false
	}
	return transportObservation{}, transportObservation{}, false
}

// awaitRetentionReconnectRecovery waits for the client to complete its own
// expired-generation recovery. The client resumes the interrupted push on its
// durable deadline, the server rejects the expired generation, the client
// reconnects, and it settles. It reports the settled status and the transport
// the recovery produced.
func awaitRetentionReconnectRecovery(ctx context.Context, scenario scenarios.Scenario, state *platformClient, checkpoint uint64, want string) (string, []transportObservation, error) {
	_ = scenario
	// The client resumes on its durable retry deadline, so the recovery
	// completes in seconds. A bounded wait reports what the client held instead
	// of running to the scenario deadline.
	deadline, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ctx = deadline
	for {
		snapshot, err := captureRunnerBatch(ctx, state, nil)
		if err != nil {
			return "", nil, fmt.Errorf("poll Swift retention-reconnect recovery: %w (runner reported: %s)", err, state.session.stderrReport())
		}
		state.mu.Lock()
		transport, observationErr := state.session.ObservationsAfter(checkpoint)
		state.mu.Unlock()
		if observationErr != nil {
			return "", nil, fmt.Errorf("capture Swift retention-reconnect renewal transport: %w", observationErr)
		}
		status := optionalStringOrNone(snapshot.Status)
		// The client settles at an idle completion when it holds no blocking
		// failure and rests in neither backoff nor error. That is the same
		// mapping the runner uses to report a call completion.
		settled := snapshot.Failure == nil && status != "backoff" && status != "error"
		// The authored call covers the rejected push and the connect that
		// renews the generation. The client continues its normal loop after
		// that, so the authored pair is selected rather than the whole window.
		rejected, renewed, paired := retentionReconnectRenewalPair(transport)
		if settled && paired {
			return want, []transportObservation{rejected, renewed}, nil
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
			return "", nil, fmt.Errorf("wait for Swift retention-reconnect recovery: %w (status %s, want %s, failure %s, transport %v)",
				ctx.Err(), status, want, failure, outcomes)
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
			return fmt.Errorf("Swift retention-reconnect wire expectation %s is unexpected", wire.StepID)
		}
		if _, duplicate := wired[wire.StepID]; duplicate {
			return fmt.Errorf("Swift retention-reconnect wire expectation %s is duplicated", wire.StepID)
		}
		wired[wire.StepID] = struct{}{}
	}
	if len(wired) != len(allowedWireSteps) {
		return errors.New("Swift retention-reconnect wire expectations are incomplete")
	}
	for _, wanted := range expected {
		step, found := steps[wanted.id]
		if !found {
			return fmt.Errorf("Swift retention-reconnect binding %s is absent", wanted.id)
		}
		if _, err := swiftScenarioOperation(steps, string(wanted.id), wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Swift retention-reconnect binding %s is invalid", wanted.id)
		}
		if wanted.kind == "local-write" || wanted.kind == "public-call" {
			if err := swiftScenarioClient(step, client); err != nil {
				return err
			}
		}
		if wanted.kind != "public-call" {
			continue
		}
		wire, err := retentionReconnectWireExpectation(scenario, wanted.id)
		if err != nil {
			return err
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Swift retention-reconnect binding %s has no call identity", wanted.id)
		}
		if prior, found := callIDs[wanted.call]; found && prior != *binding.CallID {
			return fmt.Errorf("Swift retention-reconnect call %q has inconsistent identities", wanted.call)
		}
		callIDs[wanted.call] = *binding.CallID
		if wanted.terminal && binding.Completion != retentionReconnectNativeCompletion(wire) {
			return fmt.Errorf("Swift retention-reconnect step %s completion does not match its authored wire expectation", wanted.id)
		}
		if !wanted.terminal && binding.Completion != "" {
			return fmt.Errorf("Swift retention-reconnect step %s declares a nonterminal completion", wanted.id)
		}
	}
	if len(callIDs) != 2 {
		return errors.New("Swift retention-reconnect public call bindings are incomplete")
	}
	for stepID := range allowedWireSteps {
		if _, found := steps[stepID]; !found {
			return fmt.Errorf("Swift retention-reconnect wire expectation %s references an absent step", stepID)
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
		return scenarios.WireExpectation{}, fmt.Errorf("Swift retention-reconnect wire expectation %s count = %d, want 1", stepID, count)
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

func validateRetentionReconnectStepWire(scenario scenarios.Scenario, stepID string, observed StepObservation) error {
	wire, err := retentionReconnectWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != wire.HTTPStatus || observed.Wire.Retryable != wire.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, wire.ErrorCode) {
		return fmt.Errorf("Swift retention-reconnect wire result %s differs from its authored expectation", stepID)
	}
	return nil
}

func validateRetentionReconnectNativeWire(scenario scenarios.Scenario, stepID string, observed blackbox.NativeStepObservation) error {
	wire, err := retentionReconnectWireExpectation(scenario, scenarios.StepID(stepID))
	if err != nil {
		return err
	}
	if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != wire.HTTPStatus || observed.Wire.Retryable != wire.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, wire.ErrorCode) {
		return fmt.Errorf("Swift retention-reconnect controller wire %s differs from its authored expectation", stepID)
	}
	return nil
}

// registerRetentionReconnectPinClient connects the client that pins the
// rebuild. The authored model declares that client, and server client state
// exists only after a connect, so the harness establishes it through the
// protocol instead of writing server state directly. A first connect presents
// no client generation and no known scope, which is what creates generation one.
func registerRetentionReconnectPinClient(ctx context.Context, controller *blackbox.NativeController, rebuildPin scenarios.Operation) error {
	var pin struct {
		UserID   string          `json:"user_id"`
		ClientID string          `json:"client_id"`
		Schema   json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(rebuildPin.Payload, &pin); err != nil || pin.UserID == "" || pin.ClientID == "" || len(pin.Schema) == 0 {
		return errors.New("Swift retention-reconnect rebuild pin identity is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"user_id":          pin.UserID,
		"client_id":        pin.ClientID,
		"runtime_version":  3,
		"protocol_version": 3,
		"schema_reset":     false,
		"schema":           pin.Schema,
		// A client that has reconciled no scope set presents version zero and
		// no known scope. The server answers with the scopes it assigns.
		"scope_set_version": 0,
		"known_scopes":      []any{},
	})
	if err != nil {
		return errors.New("encode Swift retention-reconnect pin client connect failed")
	}
	connect := scenarios.Operation{ContractOperation: "connect", Name: "send", Payload: payload}
	observed, requestErr := controller.RequestStep(ctx, connect)
	if requestErr != nil || observed.Disposition != "success" {
		message := "none"
		if observed.Wire != nil {
			message = observed.Wire.Message
		}
		return fmt.Errorf("register Swift retention-reconnect pin client: %w (message %q)",
			resultError(requestErr, observed.Disposition), message)
	}
	return nil
}

type retentionReconnectPrimaryKey struct {
	Authored string
	Runtime  string
}

// retentionReconnectWrittenPrimaryKey reports the authored and runtime primary
// key of the locally written row. The controller assigns the runtime identity
// when it binds the write, and no server record holds it, because this scenario
// never delivers that row as an applied transaction.
func retentionReconnectWrittenPrimaryKey(authored, bound scenarios.Operation) (retentionReconnectPrimaryKey, error) {
	authoredValue, err := retentionReconnectPrimaryKeyValue(authored)
	if err != nil {
		return retentionReconnectPrimaryKey{}, fmt.Errorf("Swift retention-reconnect authored write primary key is invalid: %w", err)
	}
	runtimeValue, err := retentionReconnectPrimaryKeyValue(bound)
	if err != nil {
		return retentionReconnectPrimaryKey{}, fmt.Errorf("Swift retention-reconnect bound write primary key is invalid: %w", err)
	}
	return retentionReconnectPrimaryKey{Authored: authoredValue, Runtime: runtimeValue}, nil
}

// retentionReconnectPrimaryKeyValue reads one primary-key value. An authored
// write names its key by field and value, and a bound write names it by its
// runtime column.
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

// retentionReconnectPinnedRebuildID reports the rebuild identity the scenario
// pinned. The request carries it, so the harness knows which record is the pin.
func retentionReconnectPinnedRebuildID(operation scenarios.Operation) (string, error) {
	var payload struct {
		RebuildID string `json:"rebuild_id"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.RebuildID == "" {
		return "", errors.New("Swift retention-reconnect rebuild pin identity is absent")
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
		return 0, errors.New("Swift retention-reconnect sealed push mutations are invalid")
	}
	ids := make(map[string]struct{}, len(payload.Request.Mutations))
	for _, mutation := range payload.Request.Mutations {
		if mutation.MutationID == "" {
			return 0, errors.New("Swift retention-reconnect sealed push mutation identity is absent")
		}
		if _, duplicate := ids[mutation.MutationID]; duplicate {
			return 0, errors.New("Swift retention-reconnect sealed push mutation identity is duplicated")
		}
		ids[mutation.MutationID] = struct{}{}
	}
	return len(ids), nil
}

// retentionReconnectSealedIdentities records the mutation identities the sealed
// batch carried on the wire. The authored identity is an alias, so the sealed
// batch supplies the runtime identity. A transport failure must not create a new
// mutation identity, so the durable queue must hold exactly these identities
// through the failed push, the generation expiry, and the renewal.
func retentionReconnectSealedIdentities(sealed []string, authoredMutations int) (map[string]struct{}, error) {
	if len(sealed) != authoredMutations {
		return nil, fmt.Errorf("Swift retention-reconnect sealed batch carried %d mutations, want %d", len(sealed), authoredMutations)
	}
	ids := make(map[string]struct{}, len(sealed))
	for _, mutation := range sealed {
		if mutation == "" {
			return nil, errors.New("Swift retention-reconnect sealed batch mutation identity is absent")
		}
		if _, duplicate := ids[mutation]; duplicate {
			return nil, errors.New("Swift retention-reconnect sealed batch mutation identity is duplicated")
		}
		ids[mutation] = struct{}{}
	}
	return ids, nil
}

func validateRetentionReconnectQueue(snapshot runnerResult, expected map[string]struct{}) error {
	if snapshot.MutationLedgerCount == nil || *snapshot.MutationLedgerCount != len(expected) || len(snapshot.RetainedMutations) != len(expected) {
		return errors.New("Swift retention-reconnect durable queue count differs from the authored sealed intent")
	}
	seen := make(map[string]struct{}, len(snapshot.RetainedMutations))
	for _, mutation := range snapshot.RetainedMutations {
		if mutation.MutationID == "" || mutation.Status == "" {
			return errors.New("Swift retention-reconnect durable queue record is incomplete")
		}
		if _, wanted := expected[mutation.MutationID]; !wanted {
			return errors.New("Swift retention-reconnect durable queue changed the authored mutation identity")
		}
		if _, duplicate := seen[mutation.MutationID]; duplicate {
			return errors.New("Swift retention-reconnect durable queue repeats an authored mutation")
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
		return errors.New("Swift retention-reconnect rebuild pin payload is invalid")
	}
	// The pin is the rebuild this scenario created, named by its own rebuild
	// identity. The client rebuilds the same scope for its own reasons, so a
	// scope holds other complete and incomplete rebuilds that are not the pin.
	matchingRebuilds := 0
	for _, value := range server.Rebuilds {
		if value.ScopeID != payload.ScopeID || value.RebuildID != payload.RebuildID {
			continue
		}
		// A released pin holds no continuation, and a pin that pins nothing
		// fails the compaction proof.
		if value.PageLimit != payload.Limit || !value.HasContinuation {
			return fmt.Errorf("Swift retention-reconnect rebuild pin differs from its authored request: scope %q rebuild %q page limit %d continuation %t; authored limit %d",
				value.ScopeID, value.RebuildID, value.PageLimit, value.HasContinuation, payload.Limit)
		}
		matchingRebuilds++
	}
	if matchingRebuilds != 1 {
		return fmt.Errorf("Swift retention-reconnect active rebuild pin is absent: %d rebuilds match pin %q for scope %q", matchingRebuilds, payload.RebuildID, payload.ScopeID)
	}
	matchingScopes := 0
	for _, scope := range server.Scopes {
		if scope.ScopeID == payload.ScopeID {
			matchingScopes++
		}
	}
	if matchingScopes != 1 {
		return errors.New("Swift retention-reconnect compacted scope is absent")
	}
	return nil
}

func resolveRetentionReconnectIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, initial, renewal RetentionReconnectCall, server scenarios.StateFacts, sealedBatchID string, sealedMutations []string, written retentionReconnectPrimaryKey, pinnedRebuildID string) ([]blackbox.NativeIdentityResolution, error) {
	// The controller resolves identities the server records. The client mints
	// its batch and mutation identities, and this scenario never delivers that
	// batch as an applied transaction, so no server record holds them. Those
	// aliases resolve from the sealed batch the proxy intercepted.
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
			return nil, fmt.Errorf("Swift retention-reconnect alias %q has no runtime evidence", alias.Alias)
		}
	}
	return resolveSwiftNativeIdentities(aliases, runtime)
}

func retentionReconnectObservedIdentityValues(aliases []scenarios.NativeIdentityAlias, initial, renewal RetentionReconnectCall, server scenarios.StateFacts, sealedBatchID string, sealedMutations []string, written retentionReconnectPrimaryKey, pinnedRebuildID string) (map[string]json.RawMessage, error) {
	// The sealed batch is pushed by a lifecycle method, so the client connects
	// before it pushes. The authored renewal covers the rejected push and the
	// connect that renews the generation.
	describe := func(observations []transportObservation) []string {
		entries := make([]string, 0, len(observations))
		for _, observation := range observations {
			entries = append(entries, fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode))
		}
		return entries
	}
	initialPushes := 0
	for _, observation := range initial.Transport {
		if observation.OperationClass == "push" {
			initialPushes++
		}
	}
	if initialPushes != 1 || len(renewal.Transport) != 2 || renewal.Transport[0].OperationClass != "push" || renewal.Transport[1].OperationClass != "connect" {
		return nil, fmt.Errorf("Swift retention-reconnect transport identity evidence is incomplete: initial %v, renewal %v",
			describe(initial.Transport), describe(renewal.Transport))
	}
	var generation int64
	var scopeSetVersion int64
	generationObserved := false
	scopeSetVersionObserved := false
	for _, observation := range append(append([]transportObservation{}, initial.Transport...), renewal.Transport...) {
		if observation.RequestFacts == nil || observation.RequestFacts.ClientGeneration == nil {
			return nil, errors.New("Swift retention-reconnect client generation evidence is absent")
		}
		observedGeneration := *observation.RequestFacts.ClientGeneration
		if !generationObserved {
			generation = observedGeneration
			generationObserved = true
		} else if generation != observedGeneration {
			return nil, errors.New("Swift retention-reconnect client generation evidence is inconsistent")
		}
	}
	// The renewing connect carries the scope set the authored renewal reports.
	// The client also connects before its sealed push, and that earlier connect
	// reports the scope set the client already held.
	renewingConnect := renewal.Transport[1]
	if renewingConnect.RequestFacts == nil || renewingConnect.RequestFacts.ScopeSetVersion == nil {
		return nil, errors.New("Swift retention-reconnect scope-set version evidence is invalid")
	}
	scopeSetVersion = *renewingConnect.RequestFacts.ScopeSetVersion
	scopeSetVersionObserved = true
	if !generationObserved || !scopeSetVersionObserved {
		return nil, errors.New("Swift retention-reconnect transport identities are incomplete")
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
			// The client mints the mutation identity, and the sealed batch the
			// proxy intercepted carries it. The authored push is never delivered
			// as an applied transaction, so no server transaction records it.
			if len(sealedMutations) != 1 {
				return nil, fmt.Errorf("Swift retention-reconnect sealed batch carried %d mutation identities, want 1", len(sealedMutations))
			}
			runtime[alias.Alias], err = json.Marshal(sealedMutations[0])
		case "batch-id":
			if sealedBatchID == "" {
				return nil, errors.New("Swift retention-reconnect sealed batch identity is absent")
			}
			runtime[alias.Alias], err = json.Marshal(sealedBatchID)
		case "primary-key":
			// Only the locally written row lacks a server record. Every other
			// primary key binds through the controller.
			if !retentionReconnectAliasNames(alias, written.Authored) {
				continue
			}
			runtime[alias.Alias], err = json.Marshal(written.Runtime)
		}
		if err != nil {
			return nil, fmt.Errorf("encode Swift retention-reconnect alias %q: %w", alias.Alias, err)
		}
	}
	return runtime, nil
}

func retentionReconnectObservedRowValue(alias scenarios.NativeIdentityAlias, aliases []scenarios.NativeIdentityAlias, rows []scenarios.RowFact) (string, error) {
	if len(rows) == 0 {
		return "", fmt.Errorf("Swift retention-reconnect %s evidence is absent", alias.Kind)
	}
	values := make(map[string]struct{})
	for _, primary := range aliases {
		if primary.Kind != "primary-key" || !retentionReconnectAliasesShareOwner(alias, primary) {
			continue
		}
		var authored string
		if err := json.Unmarshal(primary.Value, &authored); err != nil || authored == "" {
			return "", fmt.Errorf("Swift retention-reconnect primary-key evidence for %q is invalid", alias.Alias)
		}
		encoded, err := json.Marshal(authored)
		if err != nil {
			return "", fmt.Errorf("encode Swift retention-reconnect primary-key evidence for %q: %w", alias.Alias, err)
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
				return "", fmt.Errorf("Swift retention-reconnect %s evidence for %q is empty", alias.Kind, alias.Alias)
			}
			values[value] = struct{}{}
		}
	}
	if len(values) != 1 {
		return "", fmt.Errorf("Swift retention-reconnect %s evidence for %q is ambiguous", alias.Kind, alias.Alias)
	}
	for value := range values {
		return value, nil
	}
	return "", fmt.Errorf("Swift retention-reconnect %s evidence for %q is absent", alias.Kind, alias.Alias)
}

func retentionReconnectObservedRebuildID(alias scenarios.NativeIdentityAlias, aliases []scenarios.NativeIdentityAlias, rebuilds []scenarios.RebuildFact, pinnedRebuildID string) (string, error) {
	var scopeID string
	for _, scope := range aliases {
		if scope.Kind != "scope" || !retentionReconnectAliasesShareOwner(alias, scope) {
			continue
		}
		if err := json.Unmarshal(scope.Value, &scopeID); err != nil || scopeID == "" {
			return "", fmt.Errorf("Swift retention-reconnect scope evidence for %q is invalid", alias.Alias)
		}
		break
	}
	if scopeID == "" {
		return "", fmt.Errorf("Swift retention-reconnect rebuild scope evidence for %q is absent", alias.Alias)
	}
	// The client rebuilds the same scope for its own reasons, so a scope holds
	// more than one rebuild. The pin the scenario created selects the record,
	// and its runtime identity still comes from the captured state.
	var rebuildID string
	matches := 0
	for _, rebuild := range rebuilds {
		if rebuild.ScopeID != scopeID || (pinnedRebuildID != "" && rebuild.RebuildID != pinnedRebuildID) {
			continue
		}
		if rebuild.RebuildID == "" {
			return "", fmt.Errorf("Swift retention-reconnect rebuild evidence for %q is invalid", alias.Alias)
		}
		matches++
		rebuildID = rebuild.RebuildID
	}
	if matches != 1 {
		return "", fmt.Errorf("Swift retention-reconnect rebuild evidence for %q is ambiguous", alias.Alias)
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

// retentionReconnectAliasNames reports whether one alias carries the authored
// value it is compared against.
func retentionReconnectAliasNames(alias scenarios.NativeIdentityAlias, authored string) bool {
	var value string
	if err := json.Unmarshal(alias.Value, &value); err != nil {
		return false
	}
	return value != "" && value == authored
}
