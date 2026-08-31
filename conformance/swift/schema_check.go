package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const schemaCheckScenarioID = "SCN-PERF-SCHEMA-CHECK-001"

// SchemaCheckResult records each authored schema-dispatch call executed through Swift.
type SchemaCheckResult struct {
	Calls []SynchronizationResult
}

// RunSchemaCheckScenario executes the authored schema transition classes through Swift.
func RunSchemaCheckScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform) (SchemaCheckResult, error) {
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 43)
	if err != nil {
		return SchemaCheckResult{}, err
	}
	if controller == nil || platform == nil {
		return SchemaCheckResult{}, errors.New("Swift schema-check dependencies are unavailable")
	}
	publicCount, err := validateSchemaCheckBindings(scenario, steps)
	if err != nil {
		return SchemaCheckResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SchemaCheckResult{}, fmt.Errorf("install Swift schema-check contract: %w", err)
	}

	clients := make(map[string]Client)
	installed := make(map[string]bool)
	completedBoundaries := make(map[string]bool)
	result := SchemaCheckResult{Calls: make([]SynchronizationResult, 0, publicCount)}

	runPublic := func(stepID string) error {
		call, runErr := runSchemaCheckPublicStep(ctx, scenario, steps, platform, clients, installed, completedBoundaries, stepID)
		if runErr != nil {
			return runErr
		}
		result.Calls = append(result.Calls, call)
		return nil
	}
	runApply := func(stepID, operationKey string) error {
		if applyErr := applySchemaCheckControllerStep(ctx, controller, steps, stepID, operationKey); applyErr != nil {
			return applyErr
		}
		return nil
	}
	runProcess := func(stepID, operationKey string) error {
		if processErr := processSchemaCheckControllerStep(ctx, controller, steps, stepID, operationKey); processErr != nil {
			return processErr
		}
		return nil
	}

	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-001",
		"STEP-PERF-SCHEMA-CHECK-002",
		"STEP-PERF-SCHEMA-CHECK-003",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}
	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-001",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-002",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-003",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS1-COMMIT-001", "model/commit-source-transaction"); err != nil {
		return SchemaCheckResult{}, err
	}
	if err := runProcess("STEP-PERF-SCHEMA-CHECK-CLASS1-MATERIALIZE-001", "process/materialize-source-transaction"); err != nil {
		return SchemaCheckResult{}, err
	}
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS1-STAGE-001", "model/stage-registry-membership-generation"); err != nil {
		return SchemaCheckResult{}, err
	}
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS1-ACTIVATE-001", "model/activate-registry-membership-generation"); err != nil {
		return SchemaCheckResult{}, err
	}
	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-004",
		"STEP-PERF-SCHEMA-CHECK-005",
		"STEP-PERF-SCHEMA-CHECK-006",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}

	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-001",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-002",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-003",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001", "model/publish-schema"); err != nil {
		return SchemaCheckResult{}, err
	}
	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-007",
		"STEP-PERF-SCHEMA-CHECK-008",
		"STEP-PERF-SCHEMA-CHECK-009",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}

	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-001",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-002",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-003",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-001",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-002",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-003",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS3-PUBLISH-001", "model/publish-schema"); err != nil {
		return SchemaCheckResult{}, err
	}
	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-010",
		"STEP-PERF-SCHEMA-CHECK-011",
		"STEP-PERF-SCHEMA-CHECK-012",
		"STEP-PERF-SCHEMA-CHECK-013",
		"STEP-PERF-SCHEMA-CHECK-014",
		"STEP-PERF-SCHEMA-CHECK-015",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}

	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-001",
		"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-002",
		"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-003",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-001",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-002",
		"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-003",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS4-PUBLISH-001", "model/publish-schema"); err != nil {
		return SchemaCheckResult{}, err
	}
	for _, stepID := range []string{
		"STEP-PERF-SCHEMA-CHECK-016",
		"STEP-PERF-SCHEMA-CHECK-017",
		"STEP-PERF-SCHEMA-CHECK-018",
	} {
		if err := runPublic(stepID); err != nil {
			return SchemaCheckResult{}, err
		}
	}

	if len(result.Calls) != publicCount {
		return SchemaCheckResult{}, fmt.Errorf("Swift schema-check calls = %d, want %d", len(result.Calls), publicCount)
	}
	if len(completedBoundaries) != len(scenario.NativeLifecycleBoundaries) {
		return SchemaCheckResult{}, fmt.Errorf("Swift schema-check lifecycle boundaries = %d, want %d", len(completedBoundaries), len(scenario.NativeLifecycleBoundaries))
	}
	return result, nil
}

func validateSchemaCheckBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step) (int, error) {
	wireCounts := make(map[scenarios.StepID]int, len(scenario.WireExpectations))
	for _, expected := range scenario.WireExpectations {
		if _, found := steps[expected.StepID]; !found {
			return 0, fmt.Errorf("Swift schema-check wire expectation %s references an absent step", expected.StepID)
		}
		wireCounts[expected.StepID]++
	}

	publicCount := 0
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding == nil || step.ExpectedOutcome.Disposition != "success" {
			return 0, fmt.Errorf("Swift schema-check binding %s is invalid", step.ID)
		}
		switch binding.Kind {
		case "public-call":
			if scenarios.OperationKey(step.Operation) != "connect/send" || binding.Stage != "synchronous" || binding.Method != "start" || binding.CallID == nil || *binding.CallID == "" {
				return 0, fmt.Errorf("Swift schema-check public binding %s is invalid", step.ID)
			}
			if _, err := schemaCheckClientForStep(step); err != nil {
				return 0, err
			}
			if wireCounts[step.ID] != 1 {
				return 0, fmt.Errorf("Swift schema-check step %s has %d wire expectations, want 1", step.ID, wireCounts[step.ID])
			}
			wire, err := schemaCheckWireExpectation(scenario, step.ID)
			if err != nil {
				return 0, err
			}
			if binding.Completion != schemaCheckNativeCompletion(wire) {
				return 0, fmt.Errorf("Swift schema-check step %s completion %q does not match its authored wire expectation", step.ID, binding.Completion)
			}
			publicCount++
		case "controller":
			if !schemaCheckControllerOperation(scenarios.OperationKey(step.Operation)) {
				return 0, fmt.Errorf("Swift schema-check controller step %s operation %q is unsupported", step.ID, scenarios.OperationKey(step.Operation))
			}
			if wireCounts[step.ID] != 0 {
				return 0, fmt.Errorf("Swift schema-check controller step %s has wire expectations", step.ID)
			}
		default:
			return 0, fmt.Errorf("Swift schema-check step %s binding kind %q is unsupported", step.ID, binding.Kind)
		}
	}

	for _, expected := range scenario.WireExpectations {
		step := steps[expected.StepID]
		if step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" {
			return 0, fmt.Errorf("Swift schema-check wire expectation %s does not cover a public call", expected.StepID)
		}
	}
	return publicCount, nil
}

func schemaCheckControllerOperation(key string) bool {
	switch key {
	case "model/commit-source-transaction", "process/materialize-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/publish-schema":
		return true
	default:
		return false
	}
}

func schemaCheckClientForStep(step scenarios.Step) (Client, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Kind != "public-call" {
		return Client{}, fmt.Errorf("Swift schema-check step %s is not a public call", step.ID)
	}
	var payload struct {
		UserID   string `json:"user_id"`
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID {
		return Client{}, fmt.Errorf("Swift schema-check step %s client identity does not match its authored operation", step.ID)
	}
	key := "schema-check-" + binding.UserID + "-" + binding.ClientID
	return Client{Key: key, UserID: binding.UserID, ClientID: binding.ClientID, DatabaseKey: key}, nil
}

func runSchemaCheckPublicStep(ctx context.Context, scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, platform *Platform, clients map[string]Client, installed, completedBoundaries map[string]bool, stepID string) (SynchronizationResult, error) {
	step, found := steps[scenarios.StepID(stepID)]
	if !found {
		return SynchronizationResult{}, fmt.Errorf("Swift schema-check step %s is absent", stepID)
	}
	client, err := schemaCheckClientForStep(step)
	if err != nil {
		return SynchronizationResult{}, err
	}
	clients[client.Key] = client
	cold := !installed[client.Key]
	if cold {
		if err := platform.Install(ctx, client, "empty", ""); err != nil {
			return SynchronizationResult{}, fmt.Errorf("install Swift schema-check client %s: %w", client.ClientID, err)
		}
		installed[client.Key] = true
	}
	call, err := swiftScenarioCall(ctx, platform, client, step.NativeBinding.Method)
	if err != nil {
		return SynchronizationResult{}, fmt.Errorf("run Swift schema-check step %s: %w", stepID, err)
	}
	if err := validateSchemaCheckPublicCall(ctx, platform, client, scenario, step, call, cold); err != nil {
		return SynchronizationResult{}, err
	}
	if err := runSchemaCheckLifecycleBoundaries(ctx, scenario, step, client, platform, completedBoundaries); err != nil {
		return SynchronizationResult{}, err
	}
	return call, nil
}

func applySchemaCheckControllerStep(ctx context.Context, controller *blackbox.NativeController, steps map[scenarios.StepID]scenarios.Step, stepID, operationKey string) error {
	operation, err := swiftScenarioOperation(steps, stepID, operationKey)
	if err != nil {
		return err
	}
	step := steps[scenarios.StepID(stepID)]
	if step.NativeBinding == nil || step.NativeBinding.Kind != "controller" {
		return fmt.Errorf("Swift schema-check controller binding %s is invalid", stepID)
	}
	observation, err := controller.ApplyStep(ctx, operation)
	if err != nil || observation.Disposition != "success" {
		return fmt.Errorf("apply Swift schema-check controller step %s: %w", stepID, resultError(err, observation.Disposition))
	}
	return nil
}

func processSchemaCheckControllerStep(ctx context.Context, controller *blackbox.NativeController, steps map[scenarios.StepID]scenarios.Step, stepID, operationKey string) error {
	operation, err := swiftScenarioOperation(steps, stepID, operationKey)
	if err != nil {
		return err
	}
	step := steps[scenarios.StepID(stepID)]
	if step.NativeBinding == nil || step.NativeBinding.Kind != "controller" {
		return fmt.Errorf("Swift schema-check controller binding %s is invalid", stepID)
	}
	observation, err := controller.ProcessStep(ctx, nil, operation)
	if err != nil || observation.Disposition != "success" {
		return fmt.Errorf("process Swift schema-check controller step %s: %w", stepID, resultError(err, observation.Disposition))
	}
	return nil
}

func runSchemaCheckLifecycleBoundaries(ctx context.Context, scenario scenarios.Scenario, step scenarios.Step, client Client, platform *Platform, completed map[string]bool) error {
	for _, boundary := range scenario.NativeLifecycleBoundaries {
		if boundary.AfterStepID != step.ID {
			continue
		}
		if completed[boundary.ID] {
			return fmt.Errorf("Swift schema-check lifecycle boundary %s ran more than once", boundary.ID)
		}
		if boundary.Method != "stop" || boundary.UserID != client.UserID || boundary.ClientID != client.ClientID {
			return fmt.Errorf("Swift schema-check lifecycle boundary %s is not bound to step %s", boundary.ID, step.ID)
		}
		observation, err := platform.Lifecycle(ctx, client, boundary.Method)
		if err != nil || observation.Disposition != "success" {
			return fmt.Errorf("run Swift schema-check lifecycle boundary %s: %w", boundary.ID, resultError(err, observation.Disposition))
		}
		completed[boundary.ID] = true
	}
	return nil
}

func validateSchemaCheckPublicCall(ctx context.Context, platform *Platform, client Client, scenario scenarios.Scenario, step scenarios.Step, call SynchronizationResult, cold bool) error {
	wire, err := schemaCheckWireExpectation(scenario, step.ID)
	if err != nil {
		return err
	}
	wantCompletion := schemaCheckNativeCompletion(wire)
	if call.Completion != wantCompletion {
		outcomes := make([]string, 0, len(call.transportObservations))
		for _, observation := range call.transportObservations {
			entry := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
			if observation.ErrorCode != nil {
				entry += ":" + *observation.ErrorCode
			}
			outcomes = append(outcomes, entry)
		}
		// A completion alone cannot name the failure. Report the disposition and
		// error code the client recorded for each step it ran.
		dispositions := make([]string, 0, len(call.Steps))
		for _, observed := range call.Steps {
			entry := observed.Disposition
			if observed.ErrorCode != nil {
				entry += ":" + *observed.ErrorCode
			}
			dispositions = append(dispositions, entry)
		}
		snapshot, captureErr := platform.captureSnapshot(ctx, client)
		if captureErr != nil {
			return fmt.Errorf(
				"Swift schema-check step %s completed %q, want %q, observations %v, dispositions %v; capture failure: %v",
				step.ID, call.Completion, wantCompletion, outcomes, dispositions, captureErr,
			)
		}
		if snapshot.Failure != nil {
			return fmt.Errorf(
				"Swift schema-check step %s completed %q, want %q, observations %v, dispositions %v; failure operation %q, code %q, retryable %t, recovery action %q",
				step.ID, call.Completion, wantCompletion, outcomes, dispositions,
				snapshot.Failure.Operation, snapshot.Failure.Code, snapshot.Failure.Retryable, snapshot.Failure.RecoveryAction,
			)
		}
		return fmt.Errorf(
			"Swift schema-check step %s completed %q, want %q, observations %v, dispositions %v; runner reported no failure",
			step.ID, call.Completion, wantCompletion, outcomes, dispositions,
		)
	}
	// An authored step names a protocol operation, not one request. A client
	// with no usable cursor bootstraps by connecting, rebuilding, and pulling,
	// and a client that observes a schema or membership transition re-syncs
	// before it settles. The authored connect and its wire outcome are the
	// evidence for the step, so the request count is not asserted.
	if cold && !validateSwiftBaselineCallShape(call) {
		return fmt.Errorf("Swift schema-check step %s did not bootstrap its client", step.ID)
	}
	transport, err := swiftScenarioWire(call, "connect")
	if err != nil {
		return fmt.Errorf("Swift schema-check step %s: %w", step.ID, err)
	}
	if err := validateSwiftWireObservation(scenario, string(step.ID), transport); err != nil {
		return err
	}
	if len(call.Steps) == 0 {
		// A call that re-syncs reports no authored step observation, so the
		// transport wire result above is the evidence for this step.
		return nil
	}
	observed := call.Steps[0]
	if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != wire.HTTPStatus || observed.Wire.Retryable != wire.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, wire.ErrorCode) {
		return fmt.Errorf("Swift schema-check step %s wire result differs from its authored expectation", step.ID)
	}
	return nil
}

func schemaCheckWireExpectation(scenario scenarios.Scenario, stepID scenarios.StepID) (scenarios.WireExpectation, error) {
	var found scenarios.WireExpectation
	count := 0
	for _, expected := range scenario.WireExpectations {
		if expected.StepID == stepID {
			found = expected
			count++
		}
	}
	if count != 1 {
		return scenarios.WireExpectation{}, fmt.Errorf("Swift schema-check wire expectation %s count = %d, want 1", stepID, count)
	}
	return found, nil
}

func schemaCheckNativeCompletion(wire scenarios.WireExpectation) string {
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
