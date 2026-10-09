package swift

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const schemaCheckScenarioID = "SCN-PERF-SCHEMA-CHECK-001"

// SchemaCheckResult records each authored schema-dispatch call executed through Swift.
type SchemaCheckResult struct {
	Calls               []SynchronizationResult
	ProofCalls          []SynchronizationResult
	InterruptedCuts     []SchemaProofInterruptedCut
	ProofCaptures       map[scenarios.ExpectationID]runnerResult
	ProofPushes         []schemaProofPush
	ProofServerCaptures map[scenarios.ExpectationID]blackbox.NativeCaptureFacts
}

type SchemaProofInterruptedCut struct {
	CallID        scenarios.NativeCallID
	RestartStepID scenarios.StepID
	Checkpoint    string
	Capture       runnerResult
	Transport     []transportObservation
}

// RunSchemaCheckScenario executes the authored schema transition classes through Swift.
func RunSchemaCheckScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform) (SchemaCheckResult, error) {
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 64)
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

	installed := make(map[string]bool)
	completedBoundaries := make(map[string]bool)
	result := SchemaCheckResult{Calls: make([]SynchronizationResult, 0, publicCount), ProofCaptures: make(map[scenarios.ExpectationID]runnerResult), ProofServerCaptures: make(map[scenarios.ExpectationID]blackbox.NativeCaptureFacts)}

	runPublic := func(stepID string) error {
		call, runErr := runSchemaCheckPublicStep(ctx, scenario, steps, controller, platform, installed, completedBoundaries, stepID)
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
	proof, err := prepareSchemaProof(ctx, scenario, steps, controller, platform, &result)
	if err != nil {
		return SchemaCheckResult{}, err
	}
	completedBoundaries["schema_proof_prepared_stop"] = true
	completedBoundaries["schema_proof_committed_stop"] = true
	if err := runApply("STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001", "model/publish-schema"); err != nil {
		return SchemaCheckResult{}, err
	}
	if err := recoverSchemaProof(ctx, scenario, steps, controller, platform, proof, &result); err != nil {
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
		if strings.HasPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-") {
			if step.Transport == "http" && wireCounts[step.ID] != 1 || step.Transport != "http" && wireCounts[step.ID] != 0 {
				return 0, errors.New("schema proof wire closure is invalid")
			}
			continue
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

func runSchemaCheckPublicStep(ctx context.Context, scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, controller *blackbox.NativeController, platform *Platform, installed, completedBoundaries map[string]bool, stepID string) (SynchronizationResult, error) {
	step, found := steps[scenarios.StepID(stepID)]
	if !found {
		return SynchronizationResult{}, fmt.Errorf("Swift schema-check step %s is absent", stepID)
	}
	client, err := schemaCheckClientForStep(step)
	if err != nil {
		return SynchronizationResult{}, err
	}
	cold := !installed[client.Key]
	if !cold && step.NativeBinding.Initialization != "" {
		return SynchronizationResult{}, fmt.Errorf("Swift schema-check step %s repeats client initialization", stepID)
	}
	if cold {
		initialization := "empty"
		if step.NativeBinding.Initialization != "" {
			initialization = step.NativeBinding.Initialization
		}
		if err := platform.Install(ctx, client, initialization, ""); err != nil {
			return SynchronizationResult{}, fmt.Errorf("install Swift schema-check client %s: %w", client.ClientID, err)
		}
		installed[client.Key] = true
		cold = initialization == "empty"
	}
	call, err := swiftScenarioCall(ctx, platform, client, step.NativeBinding.Method)
	if err != nil {
		return SynchronizationResult{}, fmt.Errorf("run Swift schema-check step %s: %w", stepID, err)
	}
	target, scope, affected, err := schemaCheckRuntimeReferences(controller, scenario, step)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if err := validateSchemaCheckPublicCall(scenario, step, call, cold, target, scope, affected); err != nil {
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

func validateSchemaCheckPublicCall(scenario scenarios.Scenario, step scenarios.Step, call SynchronizationResult, cold bool, target schemaRef, scope string, affected bool) error {
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
		if call.after != nil && call.after.Failure != nil {
			failure := call.after.Failure
			return fmt.Errorf(
				"Swift schema-check step %s completed %q, want %q, observations %v, dispositions %v; failure operation %q, code %q, retryable %t, recovery action %q",
				step.ID, call.Completion, wantCompletion, outcomes, dispositions,
				failure.Operation, failure.Code, failure.Retryable, failure.RecoveryAction,
			)
		}
		return fmt.Errorf(
			"Swift schema-check step %s completed %q, want %q, observations %v, dispositions %v; runner reported no failure",
			step.ID, call.Completion, wantCompletion, outcomes, dispositions,
		)
	}
	if wire.Action == "unsupported" {
		if call.after == nil {
			return fmt.Errorf("Swift schema-check step %s final capture is absent", step.ID)
		}
		snapshot := call.after
		if snapshot.Failure == nil || snapshot.Failure.Operation != "schema" || snapshot.Failure.Code != "unsupported_schema" || snapshot.Failure.Retryable || snapshot.Failure.RecoveryAction != "schema_reset" {
			return fmt.Errorf("Swift schema-check step %s did not persist the unsupported_schema recovery state", step.ID)
		}
	}
	// An authored step names a protocol operation, not one request. A client
	// with no usable cursor bootstraps by connecting, rebuilding, and pulling,
	// and a client that observes a schema or membership transition re-syncs
	// before it settles. The authored connect and its wire outcome are the
	// evidence for the step, so the request count is not asserted.
	if cold && !validateSwiftBaselineCallShape(call) {
		return fmt.Errorf("Swift schema-check step %s did not bootstrap its client", step.ID)
	}
	if err := validateSchemaCheckDispatch(step, wire.Action, call, target, scope, affected); err != nil {
		return fmt.Errorf("Swift schema-check step %s: %w", step.ID, err)
	}
	transport := call.transportObservations[0]
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

func schemaCheckRuntimeReferences(controller *blackbox.NativeController, scenario scenarios.Scenario, step scenarios.Step) (schemaRef, string, bool, error) {
	var setup struct {
		InitialSchema struct {
			Schema schemaRef `json:"schema"`
		} `json:"initial_schema"`
	}
	if len(scenario.Model.Setup) != 1 || json.Unmarshal(scenario.Model.Setup[0].Payload, &setup) != nil {
		return schemaRef{}, "", false, errors.New("Swift schema-check initial schema is invalid")
	}
	authoredTarget := setup.InitialSchema.Schema
	var affectedScopes []string
	foundStep := false
	for _, candidate := range scenario.Steps {
		if candidate.ID == step.ID {
			foundStep = true
			break
		}
		if scenarios.OperationKey(candidate.Operation) == "model/publish-schema" {
			var published struct {
				Schema         schemaRef `json:"schema"`
				AffectedScopes []string  `json:"affected_scopes"`
			}
			if json.Unmarshal(candidate.Operation.Payload, &published) != nil {
				return schemaRef{}, "", false, errors.New("Swift schema-check published schema is invalid")
			}
			authoredTarget, affectedScopes = published.Schema, published.AffectedScopes
		}
	}
	if !foundStep || step.NativeBinding == nil {
		return schemaRef{}, "", false, errors.New("Swift schema-check step is unbound")
	}
	scopes := make(map[string]bool)
	for _, candidate := range scenario.Steps {
		if candidate.NativeBinding == nil || candidate.NativeBinding.Kind != "public-call" || candidate.NativeBinding.UserID != step.NativeBinding.UserID {
			continue
		}
		var payload struct {
			KnownScopes []struct {
				ScopeID string `json:"scope_id"`
			} `json:"known_scopes"`
		}
		if json.Unmarshal(candidate.Operation.Payload, &payload) != nil {
			return schemaRef{}, "", false, errors.New("Swift schema-check authored scope is invalid")
		}
		for _, scope := range payload.KnownScopes {
			scopes[scope.ScopeID] = true
		}
	}
	if len(scopes) != 1 {
		return schemaRef{}, "", false, errors.New("Swift schema-check authored client scope is ambiguous")
	}
	var authoredScope string
	for scope := range scopes {
		authoredScope = scope
	}
	var aliases []scenarios.NativeIdentityAlias
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Kind == "schema" {
			var value schemaRef
			if json.Unmarshal(alias.Value, &value) == nil && value == authoredTarget {
				aliases = append(aliases, alias)
			}
		}
		if alias.Kind == "scope" {
			var value string
			if json.Unmarshal(alias.Value, &value) == nil && value == authoredScope {
				aliases = append(aliases, alias)
			}
		}
	}
	if len(aliases) != 2 {
		return schemaRef{}, "", false, errors.New("Swift schema-check runtime aliases are ambiguous")
	}
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return schemaRef{}, "", false, err
	}
	var target schemaRef
	var scope string
	for _, value := range values {
		if value.Kind == "schema" {
			err = json.Unmarshal(value.RuntimeValue, &target)
		}
		if value.Kind == "scope" {
			err = json.Unmarshal(value.RuntimeValue, &scope)
		}
		if err != nil {
			return schemaRef{}, "", false, err
		}
	}
	wire, err := schemaCheckWireExpectation(scenario, step.ID)
	if err != nil || target.Version <= 0 || !validLowerHexDigest(target.Hash) || scope == "" {
		return schemaRef{}, "", false, errors.New("Swift schema-check runtime references are invalid")
	}
	return target, scope, wire.Action == "rebuild_local" && slices.Contains(affectedScopes, authoredScope), nil
}

func validateSchemaCheckDispatch(step scenarios.Step, action string, call SynchronizationResult, target schemaRef, scope string, affected bool) error {
	before, after := call.before, call.after
	if before == nil || after == nil || before.TransportObservations == nil || after.TransportObservations == nil || before.TransportObservations.Overflowed || after.TransportObservations.Overflowed || len(call.transportObservations) == 0 {
		return errors.New("schema dispatch captures or transport window are incomplete")
	}
	for _, capture := range []*runnerResult{before, after} {
		if err := capture.requireCompleteMigrationCapture(); err != nil {
			return err
		}
		for _, truncated := range []*bool{capture.CaptureOverflowed, capture.ScopeStatesTruncated, capture.ScopeRowsTruncated, capture.RebuildAttemptsTruncated, capture.RebuildReceiptsTruncated, capture.RowMetadataTruncated} {
			if truncated != nil && *truncated {
				return errors.New("schema dispatch captured state is truncated")
			}
		}
		if capture.ApplicationRowCount != nil && *capture.ApplicationRowCount > maximumRunnerRows || len(capture.Events) >= maximumRunnerRecords {
			return errors.New("schema dispatch captured state or event ring is full")
		}
		for _, count := range []*int{capture.MutationLedgerCount, capture.MutationOutcomeCount, capture.SealedBatchCount, capture.RejectedMutationCount, capture.ScopeStateCount, capture.ScopeRowCount, capture.ProvenanceCount, capture.RowMetadataCount, capture.RebuildAttemptCount, capture.RebuildReceiptCount} {
			if count != nil && *count > maximumRunnerRecords {
				return errors.New("schema dispatch captured state is out of bounds")
			}
		}
	}
	checkpoint := before.TransportObservations.SequenceCheckpoint
	if after.TransportObservations.SequenceCheckpoint != checkpoint+uint64(len(call.transportObservations)) {
		return errors.New("schema dispatch transport window changed")
	}
	for index, observation := range call.transportObservations {
		if observation.Sequence != checkpoint+uint64(index+1) {
			return errors.New("schema dispatch transport window has a gap")
		}
	}
	connect := call.transportObservations[0]
	facts := connect.ConnectResponseFacts
	if connect.OperationClass != "connect" || connect.StatusCode != 200 || connect.RequestFacts == nil || facts == nil || facts.validate() != nil || !facts.AffectedScopesComplete || !facts.ScopeCursorUpdatesComplete || facts.Action != action || facts.SchemaVersion != target.Version || facts.SchemaHash != target.Hash {
		return errors.New("initial connect action or target schema differs from authored dispatch")
	}
	source := schemaRef{}
	if before.Schema != nil {
		source = *before.Schema
	}
	if connect.RequestFacts.SchemaVersion != source.Version || connect.RequestFacts.SchemaHash != source.Hash {
		return errors.New("connect did not present the captured source schema")
	}
	if len(before.ScopeStates) > 1 || len(after.ScopeStates) != 1 || after.ScopeStates[0].ScopeID != scope || before.ScopeStatesTruncated != nil && *before.ScopeStatesTruncated || after.ScopeStatesTruncated != nil && *after.ScopeStatesTruncated {
		return errors.New("schema dispatch scope captures are incomplete")
	}
	var oldCursor *string
	if len(before.ScopeStates) == 1 {
		if before.ScopeStates[0].ScopeID != scope {
			return errors.New("schema dispatch source scope differs from authored scope")
		}
		oldCursor = before.ScopeStates[0].Cursor
	}
	presented := []string{}
	if oldCursor != nil {
		presented = append(presented, cursorFingerprint(*oldCursor))
	}
	if connect.CursorFingerprintsComplete == nil || !*connect.CursorFingerprintsComplete || !slices.Equal(connect.CursorFingerprints, presented) || connect.CursorFingerprints == nil {
		return errors.New("connect did not present the captured old cursor")
	}
	wantSchema := target
	// A final capture does not identify the schema activation cut.
	if action == "unsupported" {
		wantSchema = source
	}
	if after.Schema == nil || *after.Schema != wantSchema {
		return errors.New("schema dispatch final schema differs from its accepted target")
	}
	if len(after.Events) < len(before.Events) || len(before.Events) > 0 && !reflect.DeepEqual(after.Events[:len(before.Events)], before.Events) {
		return errors.New("schema dispatch event window is incomplete")
	}
	schemaEvents := []string{}
	for _, event := range after.Events[len(before.Events):] {
		if event.Type != "schema_applying" && event.Type != "schema_applied" {
			continue
		}
		if event.SourceSchema == nil || *event.SourceSchema != source || event.TargetSchema == nil || *event.TargetSchema != target || event.SchemaAction == nil || *event.SchemaAction != action {
			return errors.New("schema event source, target, or action differs from authored dispatch")
		}
		schemaEvents = append(schemaEvents, event.Type)
	}
	wantEvents := []string{}
	if action == "replace" || action == "rebuild_local" {
		wantEvents = []string{"schema_applying", "schema_applied"}
	}
	if !slices.Equal(schemaEvents, wantEvents) {
		return errors.New("schema dispatch event sequence differs from authored action")
	}
	scopeFingerprint := cursorFingerprint(scope)
	wantAffected := []string{}
	if affected {
		wantAffected = append(wantAffected, scopeFingerprint)
	}
	if (action == "rebuild_local") != affected || !slices.Equal(facts.AffectedScopeFingerprints, wantAffected) {
		return errors.New("schema dispatch affected scopes differ from authored assignment")
	}
	for fingerprint := range facts.ScopeCursorUpdates {
		if fingerprint != scopeFingerprint {
			return errors.New("connect issued a cursor update for an unexpected scope")
		}
	}
	var parameters struct {
		SchemaCase string `json:"schema_case"`
	}
	if step.MeasurementSample != nil && json.Unmarshal(step.MeasurementSample.Parameters, &parameters) != nil {
		return errors.New("schema dispatch measurement case is invalid")
	}
	membershipRecovery := parameters.SchemaCase == "class_1"
	issued, hasUpdate := facts.ScopeCursorUpdates[scopeFingerprint]
	if affected && (!hasUpdate || issued != nil) || hasUpdate && issued == nil && !affected && !membershipRecovery {
		return errors.New("connect cursor reset differs from required affected rebuild")
	}
	if oldCursor != nil && source != target && !affected && action != "unsupported" && (!hasUpdate || issued == nil || *issued == presented[0]) {
		return errors.New("connect did not replace the historical unaffected cursor")
	}
	if action == "unsupported" {
		if len(call.transportObservations) != 1 || len(facts.ScopeCursorUpdates) != 0 {
			return errors.New("unsupported schema dispatch continued synchronization")
		}
		return nil
	}
	firstPull := -1
	var terminalBeforePull *string
	rebuilt := false
	for index, observation := range call.transportObservations[1:] {
		if observation.OperationClass == "pull" {
			if observation.RequestFacts == nil || observation.RequestFacts.SchemaVersion != target.Version || observation.RequestFacts.SchemaHash != target.Hash {
				return errors.New("pull did not use the activated target schema")
			}
			if firstPull == -1 {
				firstPull = index + 1
			}
		}
		if observation.OperationClass == "rebuild" {
			if oldCursor != nil && !affected && !membershipRecovery {
				return errors.New("unaffected schema dispatch caused an unnecessary rebuild")
			}
			if observation.RequestFacts == nil || observation.RequestFacts.SchemaVersion != target.Version || observation.RequestFacts.SchemaHash != target.Hash || observation.RequestFacts.ScopeFingerprint == nil || *observation.RequestFacts.ScopeFingerprint != scopeFingerprint {
				return errors.New("schema rebuild target or scope differs from authored dispatch")
			}
			response := observation.RebuildResponseFacts
			if observation.StatusCode == 200 && response != nil && response.HasFinalScopeCursor && response.FinalScopeCursorFingerprint != nil && validLowerHexDigest(*response.FinalScopeCursorFingerprint) && response.ScopeFingerprint == scopeFingerprint {
				rebuilt = true
				if firstPull == -1 {
					terminalBeforePull = response.FinalScopeCursorFingerprint
				}
			}
		}
	}
	if firstPull == -1 || affected && !rebuilt {
		return errors.New("schema dispatch omitted its target pull or required affected rebuild")
	}
	pull := call.transportObservations[firstPull]
	if pull.StatusCode != 200 || pull.CursorFingerprints == nil || pull.CursorFingerprintsComplete == nil || !*pull.CursorFingerprintsComplete || !validCursorFingerprintSet(pull.CursorFingerprints) {
		return errors.New("first target-schema pull cursor facts are incomplete")
	}
	wantCursors := presented
	if hasUpdate && issued != nil {
		wantCursors = []string{*issued}
	} else if affected || oldCursor == nil || hasUpdate && issued == nil {
		wantCursors = []string{}
		if terminalBeforePull != nil {
			wantCursors = []string{*terminalBeforePull}
		}
	} else if membershipRecovery && rebuilt {
		if terminalBeforePull != nil {
			wantCursors = []string{*terminalBeforePull}
		} else if len(pull.CursorFingerprints) == 0 {
			wantCursors = []string{}
		}
	}
	if !slices.Equal(pull.CursorFingerprints, wantCursors) {
		return errors.New("first target-schema pull did not use the response-issued replacement or rebuilt cursor")
	}
	return nil
}

type schemaProofLane struct {
	client           Client
	write            scenarios.Operation
	baseline, intent runnerResult
	original         schemaProofPush
	localOriginal    retainedMutation
}

func schemaProofStep(steps map[scenarios.StepID]scenarios.Step, suffix string) scenarios.Step {
	return steps[scenarios.StepID("STEP-PERF-SCHEMA-CHECK-PROOF-"+suffix+"-001")]
}

func captureSchemaProof(result *SchemaCheckResult, suffix string, capture runnerResult) error {
	if err := capture.requireCompleteMigrationCapture(); err != nil {
		return err
	}
	if err := capture.requireCompleteAcceptedMutationOutcomes(); err != nil {
		return err
	}
	id := scenarios.ExpectationID("EXPECT-PERF-SCHEMA-CHECK-PROOF-" + suffix + "-001")
	if _, found := result.ProofCaptures[id]; found {
		return errors.New("schema proof capture identity repeated")
	}
	result.ProofCaptures[id] = capture
	return nil
}

func prepareSchemaProof(ctx context.Context, scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, controller *blackbox.NativeController, platform *Platform, result *SchemaCheckResult) ([]schemaProofLane, error) {
	if scenario.NativeLocalFixture == nil {
		return nil, errors.New("schema proof local fixture is absent")
	}
	source, err := schemaProofSchema(controller, scenario, "schema-v1")
	if err != nil {
		return nil, err
	}
	commit := schemaProofStep(steps, "BASELINE-COMMIT")
	if observed, err := controller.ApplyStep(ctx, commit.Operation); err != nil || observed.Disposition != "success" {
		return nil, fmt.Errorf("commit proof baseline: %v", err)
	}
	materialize := schemaProofStep(steps, "BASELINE-MATERIALIZE")
	if observed, err := controller.ProcessStep(ctx, nil, materialize.Operation); err != nil || observed.Disposition != "success" {
		return nil, fmt.Errorf("materialize proof baseline: %v", err)
	}
	lanes := make([]schemaProofLane, 0, 2)
	for _, name := range []string{"PREPARED", "COMMITTED"} {
		bootstrap := schemaProofStep(steps, name+"-BOOTSTRAP")
		client, err := schemaCheckClientForStep(bootstrap)
		if err != nil {
			return nil, err
		}
		if err := platform.Install(ctx, client, "empty", "", scenario.NativeLocalFixture); err != nil {
			return nil, err
		}
		state, _ := platform.client(client)
		key, _ := json.Marshal(scenario.NativeLocalFixture.ID)
		state.selectors["schema-proof-sentinel"] = runnerRowSelector{TableName: scenario.NativeLocalFixture.TableName, PrimaryKeyField: "id", PrimaryKey: key}
		suffix := name + "-WRITE"
		if name == "COMMITTED" {
			suffix = "COMMITTED-M1-WRITE"
		}
		write, err := controller.ApplicationWrite(schemaProofStep(steps, suffix).Operation)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Table string                     `json:"table_id"`
			PK    map[string]json.RawMessage `json:"pk"`
		}
		if json.Unmarshal(write.Payload, &payload) != nil || len(payload.PK) != 1 {
			return nil, errors.New("proof row identity is invalid")
		}
		call, err := swiftScenarioCall(ctx, platform, client, "start")
		if err != nil || call.Completion != "idle" {
			return nil, fmt.Errorf("bootstrap proof client: %v", err)
		}
		result.ProofCalls = append(result.ProofCalls, call)
		for field, value := range payload.PK {
			state.selectors["schema-proof-row"] = runnerRowSelector{TableName: payload.Table, PrimaryKeyField: field, PrimaryKey: value}
		}
		if observed, err := platform.Lifecycle(ctx, client, "stop"); err != nil || observed.Disposition != "success" {
			return nil, fmt.Errorf("stop proof client: %v", err)
		}
		baseline, err := platform.captureSnapshot(ctx, client)
		if err != nil {
			return nil, err
		}
		if err := requireSchemaProofSentinel(scenario.NativeLocalFixture, baseline); err != nil {
			return nil, err
		}
		if baseline.Schema == nil || *baseline.Schema != source || baseline.MutationLedgerCount == nil || *baseline.MutationLedgerCount != 0 {
			return nil, errors.New("proof bootstrap did not establish clean S1")
		}
		if err := requireSchemaProofPhysical(controller, scenario, baseline); err != nil {
			return nil, err
		}
		baselineWrite, err := scenarios.SchemaProofBaselineWrite(scenario, schemaProofStep(steps, suffix).Operation)
		if err != nil {
			return nil, err
		}
		baselineWrite, err = controller.ApplicationWrite(baselineWrite)
		if err != nil {
			return nil, err
		}
		if err := scenarios.RequireLocalWriteRow(baselineWrite, baseline.ApplicationRows); err != nil {
			return nil, err
		}
		if len(call.transportObservations) == 0 {
			return nil, errors.New("proof bootstrap connect is absent")
		}
		if err := validateSwiftWireObservation(scenario, string(bootstrap.ID), call.transportObservations[0]); err != nil {
			return nil, err
		}
		if err := captureSchemaProof(result, name+"-S1", baseline); err != nil {
			return nil, err
		}
		lanes = append(lanes, schemaProofLane{client: client, write: write, baseline: baseline})
	}
	for index := range lanes {
		lane := &lanes[index]
		if observed, err := platform.ApplyStep(ctx, lane.client, lane.write); err != nil || observed.Disposition != "success" {
			return nil, fmt.Errorf("apply proof intent: %v", err)
		}
		intent, err := platform.captureSnapshot(ctx, lane.client)
		if err != nil {
			return nil, err
		}
		if len(intent.RetainedMutations) != 1 || intent.RetainedMutations[0].AuthoredSchema != *lane.baseline.Schema || intent.RetainedMutations[0].Status != "pending" || intent.RetainedMutations[0].SourceKind == "normalized" || intent.RetainedMutations[0].NormalizedMutationID != nil || intent.RetainedMutations[0].DependsOnMutationID != nil {
			return nil, errors.New("proof intent did not retain its S1 binding")
		}
		if err := scenarios.RequireLocalWriteRow(lane.write, intent.ApplicationRows); err != nil {
			return nil, err
		}
		lane.intent = intent
		lane.localOriginal = intent.RetainedMutations[0]
		if index == 0 {
			if err := captureSchemaProof(result, "PREPARED-INTENT", intent); err != nil {
				return nil, err
			}
		}
	}
	committed := &lanes[1]
	state, _ := platform.client(committed.client)
	push := schemaProofStep(steps, "COMMITTED-M1-SEND").Operation
	if err := bindSchemaProofPush(controller, push); err != nil {
		return nil, err
	}
	connect := schemaProofStep(steps, "COMMITTED-M1-CONNECT")
	if _, err := platform.synchronizeWithResponseLoss(ctx, state, "start", RequestOperations{connect.Operation, push}, "00000000-0000-4000-8000-000000033111"); err != nil {
		return nil, err
	}
	if state.pendingLoss == nil {
		return nil, errors.New("M1 response cut is absent")
	}
	committed.intent = state.pendingLoss.restartCapture
	platform.mu.Lock()
	for _, observed := range platform.schemaProofPushes {
		if observed.ClientID == committed.client.ClientID {
			committed.original = observed
			break
		}
	}
	platform.mu.Unlock()
	request, err := decodeSchemaProofPush(committed.original)
	if err != nil {
		return nil, errors.New("initial M1 request is incomplete")
	}
	if err := requireSchemaProofApplied(committed.original, request.Mutations[0].MutationID, *committed.baseline.Schema); err != nil {
		return nil, err
	}
	if len(committed.intent.RetainedMutations) != 1 {
		return nil, errors.New("initial M1 retained ledger is not a singleton")
	}
	sealed := committed.intent.RetainedMutations[0]
	if sealed.MutationID != committed.localOriginal.MutationID || sealed.Status != "sealed" || sealed.DependsOnMutationID != nil || sealed.SealedBatchID == nil || *sealed.SealedBatchID != request.BatchID || sealed.SealedOrdinal == nil || *sealed.SealedOrdinal != 0 {
		return nil, errors.New("initial M1 bytes are not bound to its sealed original record")
	}
	if err := requireSchemaProofMutation(sealed, request.Mutations[0]); err != nil {
		return nil, err
	}
	if len(committed.intent.AcceptedMutationOutcomes) != 0 {
		return nil, errors.New("initial M1 response was reconciled before the process cut")
	}
	if err := requireSchemaProofOriginal(committed.localOriginal, committed.intent.RetainedMutations); err != nil {
		return nil, err
	}
	if err := captureSchemaProof(result, "COMMITTED-M1-SEALED", committed.intent); err != nil {
		return nil, err
	}
	if err := captureSchemaProof(result, "COMMITTED-M1-ACCEPTED", committed.intent); err != nil {
		return nil, err
	}
	if err := validateSwiftWireObservation(scenario, string(connect.ID), state.pendingLoss.observations[0]); err != nil {
		return nil, err
	}
	result.InterruptedCuts = append(result.InterruptedCuts, SchemaProofInterruptedCut{CallID: *connect.NativeBinding.CallID, RestartStepID: schemaProofStep(steps, "COMMITTED-M1-RESTART").ID, Checkpoint: "push", Capture: committed.intent, Transport: state.pendingLoss.observations})
	if observed, err := platform.ProcessStep(ctx, committed.client, schemaProofStep(steps, "COMMITTED-M1-RESTART").Operation); err != nil || observed.Disposition != "success" {
		return nil, fmt.Errorf("restart M1 response cut: %v", err)
	}
	return lanes, nil
}

func bindSchemaProofPush(controller *blackbox.NativeController, operation scenarios.Operation) error {
	var payload map[string]json.RawMessage
	if json.Unmarshal(operation.Payload, &payload) != nil {
		return errors.New("proof push binding payload is invalid")
	}
	payload["delivery"] = json.RawMessage(`"apply"`)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	operation.Payload = encoded
	return controller.BindApplicationPush(operation)
}

type schemaProofPushRequest struct {
	BatchID   string         `json:"batch_id"`
	Schema    schemaRef      `json:"schema"`
	Mutations []wireMutation `json:"mutations"`
}

func decodeSchemaProofPush(push schemaProofPush) (schemaProofPushRequest, error) {
	var request schemaProofPushRequest
	if json.Unmarshal(push.Request, &request) != nil || request.BatchID == "" || len(request.Mutations) != 1 {
		return request, errors.New("proof push does not contain one named mutation")
	}
	return request, nil
}

func beginSchemaProofCheckpoint(ctx context.Context, platform *Platform, lane schemaProofLane, step scenarios.Step) (runnerResult, error) {
	state, err := platform.client(lane.client)
	if err != nil {
		return runnerResult{}, err
	}
	before, err := captureRunner(ctx, state)
	if err != nil {
		return runnerResult{}, err
	}
	checkpoint := state.session.Checkpoint()
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: step.NativeBinding.Checkpoint}); err != nil {
		return runnerResult{}, err
	}
	begin, err := state.session.Execute(ctx, Request{Operation: "begin-call", CallID: string(*step.NativeBinding.CallID), Method: "start"})
	if err != nil {
		return runnerResult{}, err
	}
	call, err := runnerClientCallResult(begin)
	if err != nil || call.State != "in_flight" {
		return runnerResult{}, errors.New("checkpoint call did not enter flight")
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: step.NativeBinding.Checkpoint}); err != nil {
		return runnerResult{}, err
	}
	state.activeCall = &platformCall{id: string(*step.NativeBinding.CallID), checkpoint: checkpoint, started: time.Now(), before: before, paused: true}
	return captureRunner(ctx, state)
}

func finishSchemaProofCall(ctx context.Context, platform *Platform, client Client) (SynchronizationResult, error) {
	state, _ := platform.client(client)
	active := state.activeCall
	if active == nil {
		return SynchronizationResult{}, errors.New("proof call is absent")
	}
	completed, err := state.session.Execute(ctx, Request{Operation: "await-call", CallID: active.id})
	if err != nil {
		return SynchronizationResult{}, err
	}
	call, err := runnerClientCallResult(completed)
	if err != nil || call.State != "completed" || call.Completion != "idle" {
		return SynchronizationResult{}, errors.New("proof call did not complete idle")
	}
	after, err := captureRunner(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	observations, err := state.session.ObservationsAfter(active.checkpoint)
	if err != nil {
		return SynchronizationResult{}, err
	}
	window, err := windowFromResults(active.started, active.before, after, observations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state.activeCall = nil
	state.started = true
	return synchronizationResult(call.Completion, call.CallErrorCategory, nil, window), nil
}

func recoverSchemaProof(ctx context.Context, scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, controller *blackbox.NativeController, platform *Platform, lanes []schemaProofLane, result *SchemaCheckResult) error {
	target, err := schemaProofSchema(controller, scenario, "schema-v2")
	if err != nil {
		return err
	}
	for index, name := range []string{"PREPARED", "COMMITTED"} {
		lane := lanes[index]
		cut, err := beginSchemaProofCheckpoint(ctx, platform, lane, schemaProofStep(steps, name+"-MIGRATE"))
		if err != nil {
			return err
		}
		journal, err := decodeMigrationJournal(cut.MigrationJournal)
		if err != nil || journal == nil || journal.Source != *lane.baseline.Schema || journal.Target != target || journal.Action != "replace" {
			return errors.New("migration cut has the wrong source or target")
		}
		if err := requireSchemaProofSentinel(scenario.NativeLocalFixture, cut); err != nil {
			return err
		}
		var updates map[string]*string
		if json.Unmarshal([]byte(journal.Stored["scope_cursor_updates_json"]), &updates) != nil {
			return errors.New("migration cursor journal is invalid")
		}
		if len(cut.ScopeStates) != 1 {
			return errors.New("migration scope capture is incomplete")
		}
		issued, found := updates[cut.ScopeStates[0].ScopeID]
		if !found || issued == nil || *issued == "" || len(lane.intent.ScopeStates) != 1 || lane.intent.ScopeStates[0].Cursor == nil || *issued == *lane.intent.ScopeStates[0].Cursor {
			return errors.New("migration did not issue a new unaffected cursor")
		}
		state, _ := platform.client(lane.client)
		observations, err := state.session.ObservationsAfter(state.activeCall.checkpoint)
		if err != nil || len(observations) == 0 || observations[0].ConnectResponseFacts == nil {
			return errors.New("migration cut is not bound to its actual connect")
		}
		for _, observation := range observations[1:] {
			if observation.OperationClass != "schemas" || observation.StatusCode != 200 {
				return errors.New("migration checkpoint continued beyond its schema input")
			}
		}
		facts := observations[0].ConnectResponseFacts
		fingerprint := cursorFingerprint(cut.ScopeStates[0].ScopeID)
		replacement := facts.ScopeCursorUpdates[fingerprint]
		if !facts.ScopeCursorUpdatesComplete || replacement == nil || *replacement != cursorFingerprint(*issued) {
			return errors.New("journal cursor differs from actual response-issued replacement")
		}
		if err := validateSchemaProofActivation(lane.intent, cut, *journal, *issued, name == "PREPARED"); err != nil {
			return err
		}
		if err := validateSwiftWireObservation(scenario, string(schemaProofStep(steps, name+"-MIGRATE").ID), observations[0]); err != nil {
			return err
		}
		if observations[0].OperationClass != "connect" || observations[0].StatusCode != 200 || facts.Action != "replace" || facts.SchemaVersion != journal.Target.Version || facts.SchemaHash != journal.Target.Hash || !facts.AffectedScopesComplete || len(facts.AffectedScopeFingerprints) != 0 {
			return errors.New("migration checkpoint has no complete real replace response")
		}
		if err := requireSchemaProofPhysical(controller, scenario, cut); err != nil {
			return err
		}
		if err := captureSchemaProof(result, name+"-JOURNAL", cut); err != nil {
			return err
		}
		if observed, err := platform.ProcessStep(ctx, lane.client, schemaProofStep(steps, name+"-CUT").Operation); err != nil || observed.Disposition != "success" {
			return fmt.Errorf("restart checkpoint cut: %v", err)
		}
		result.InterruptedCuts = append(result.InterruptedCuts, SchemaProofInterruptedCut{CallID: *schemaProofStep(steps, name+"-MIGRATE").NativeBinding.CallID, RestartStepID: schemaProofStep(steps, name+"-CUT").ID, Checkpoint: schemaProofStep(steps, name+"-MIGRATE").NativeBinding.Checkpoint, Capture: cut, Transport: observations})
		platform.mu.Lock()
		traffic := platform.schemaProofRequests
		platform.mu.Unlock()
		recovered, err := beginSchemaProofCheckpoint(ctx, platform, lane, schemaProofStep(steps, name+"-RECOVER"))
		if err != nil {
			return err
		}
		platform.mu.Lock()
		newTraffic := platform.schemaProofRequests
		platform.mu.Unlock()
		if traffic != newTraffic {
			return errors.New("startup recovery sent HTTP traffic before its committed checkpoint")
		}
		if recovered.Schema == nil || *recovered.Schema != journal.Target || len(recovered.ScopeStates) != 1 || recovered.ScopeStates[0].Cursor == nil || *recovered.ScopeStates[0].Cursor != *issued || !reflect.DeepEqual(cut.RetainedMutations, recovered.RetainedMutations) {
			return errors.New("offline recovery did not preserve target schema, cursor, or intent")
		}
		if recovered.ProcessID == cut.ProcessID || recovered.DatabaseIdentityFingerprint != cut.DatabaseIdentityFingerprint {
			return errors.New("checkpoint cut did not replace the process against the same database")
		}
		if err := requireSchemaProofSentinel(scenario.NativeLocalFixture, recovered); err != nil {
			return err
		}
		recoveryJournal, err := decodeMigrationJournal(recovered.MigrationJournal)
		if err != nil || recoveryJournal != nil {
			return errors.New("compatible recovery did not clear its completed migration journal")
		}
		if err := requireSchemaProofPhysical(controller, scenario, recovered); err != nil {
			return err
		}
		if err := captureSchemaProof(result, name+"-RECOVERED", recovered); err != nil {
			return err
		}
		if err := scenarios.RequireLocalWriteRow(lane.write, recovered.ApplicationRows); err != nil {
			return err
		}
		if name == "COMMITTED" {
			m2, err := controller.ApplicationWrite(schemaProofStep(steps, "COMMITTED-M2-WRITE").Operation)
			if err != nil {
				return err
			}
			if observed, err := platform.ApplyStep(ctx, lane.client, m2); err != nil || observed.Disposition != "success" {
				return fmt.Errorf("write later intent during recovery pause: %v", err)
			}
			if err := runSchemaProofReplay(ctx, scenario.NativeLocalFixture, steps, controller, platform, lane, m2, result); err != nil {
				return err
			}
		} else {
			if err := bindSchemaProofPush(controller, schemaProofStep(steps, "PREPARED-PUSH").Operation); err != nil {
				return err
			}
			if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
				return err
			}
		}
		call, err := finishSchemaProofCall(ctx, platform, lane.client)
		if err != nil {
			return err
		}
		result.ProofCalls = append(result.ProofCalls, call)
		if err := requireSchemaProofSentinel(scenario.NativeLocalFixture, *call.after); err != nil {
			return err
		}
		finalWrite := lane.write
		if name == "COMMITTED" {
			finalWrite, err = controller.ApplicationWrite(schemaProofStep(steps, "COMMITTED-M2-WRITE").Operation)
			if err != nil {
				return err
			}
		}
		if err := scenarios.RequireLocalWriteRow(finalWrite, call.after.ApplicationRows); err != nil {
			return err
		}
		if call.after.PendingChangeCount == nil || *call.after.PendingChangeCount != 0 {
			return errors.New("proof call left pending mutations")
		}
		if err := requireSchemaProofPhysical(controller, scenario, *call.after); err != nil {
			return err
		}
		if err := validateSchemaProofRecoveryWire(scenario, steps, name, call, journal.Target, recovered); err != nil {
			return err
		}
		if err := captureSchemaProof(result, name+"-FINAL", *call.after); err != nil {
			return err
		}
		if name == "PREPARED" {
			if len(call.after.AcceptedMutationOutcomes) != 1 || len(call.after.RetainedMutations) != 0 || call.after.MutationLedgerCount == nil || *call.after.MutationLedgerCount != 1 {
				return errors.New("prepared final ledger does not contain exactly one accepted original")
			}
			pushes := schemaProofClientPushes(platform, lane.client.ClientID)
			if len(pushes) != 1 {
				return errors.New("prepared lane did not send exactly one push")
			}
			request, err := decodeSchemaProofPush(pushes[0])
			if err != nil || request.Schema != journal.Target {
				return errors.New("prepared push did not use S2")
			}
			if err := requireSchemaProofMutation(lane.localOriginal, request.Mutations[0]); err != nil {
				return err
			}
			if err := requireSchemaProofApplied(pushes[0], request.Mutations[0].MutationID, journal.Target); err != nil {
				return err
			}
			if err := requireSchemaProofStoredOutcome(*call.after, pushes[0]); err != nil {
				return err
			}
			if err := requireSchemaProofRowMetadata(*call.after, lane.localOriginal, pushes[0]); err != nil {
				return err
			}
		} else {
			if len(call.after.AcceptedMutationOutcomes) != 2 || len(call.after.RetainedMutations) != 0 || call.after.MutationLedgerCount == nil || *call.after.MutationLedgerCount != 2 {
				return errors.New("committed final ledger does not contain exactly two accepted originals")
			}
			pushes := schemaProofClientPushes(platform, lane.client.ClientID)
			if len(pushes) != 3 {
				return errors.New("committed lane did not send exactly three pushes")
			}
			if err := requireSchemaProofStoredOutcome(*call.after, pushes[1]); err != nil {
				return err
			}
			if err := requireSchemaProofStoredOutcome(*call.after, pushes[2]); err != nil {
				return err
			}
			if err := requireSchemaProofRowMetadata(*call.after, lane.localOriginal, pushes[2]); err != nil {
				return err
			}
		}
	}
	platform.mu.Lock()
	result.ProofPushes = append([]schemaProofPush(nil), platform.schemaProofPushes...)
	platform.mu.Unlock()
	return nil
}

func requireSchemaProofSentinel(fixture *scenarios.NativeLocalFixture, capture runnerResult) error {
	if err := capture.requireCompleteMigrationCapture(); err != nil {
		return err
	}
	for _, row := range capture.ApplicationRows {
		var id, value string
		if json.Unmarshal(row["id"], &id) == nil && id == fixture.ID {
			if json.Unmarshal(row["value"], &value) != nil || value != fixture.Value {
				return errors.New("local sentinel changed")
			}
			return nil
		}
	}
	return errors.New("actual local sentinel row is absent")
}

func sameSchemaProofIntent(before, after []retainedMutation) bool {
	if len(before) != len(after) {
		return false
	}
	for index, want := range before {
		got := after[index]
		if want.MutationID != got.MutationID || want.LocalOrder != got.LocalOrder || want.TableID != got.TableID || want.TableName != got.TableName || want.PrimaryKeyFieldID != got.PrimaryKeyFieldID || want.PrimaryKeyLogicalType != got.PrimaryKeyLogicalType || want.SourceKind != got.SourceKind || want.RecordID != got.RecordID || want.Operation != got.Operation || want.AuthoredSchema != got.AuthoredSchema || !equalOptionalStrings(want.BaseVersion, got.BaseVersion) || want.ClientVersion != got.ClientVersion || !reflect.DeepEqual(want.AuthoredFields, got.AuthoredFields) {
			return false
		}
	}
	return true
}

func validateSchemaProofActivation(before, cut runnerResult, journal migrationJournalCapture, issued string, prepared bool) error {
	if before.Schema == nil || cut.Schema == nil || journal.Source != *before.Schema || len(before.ScopeStates) != 1 || len(cut.ScopeStates) != 1 || before.ScopeStates[0].Cursor == nil || cut.ScopeStates[0].Cursor == nil || before.ScopeStates[0].ScopeID != cut.ScopeStates[0].ScopeID || issued == "" || issued == *before.ScopeStates[0].Cursor {
		return errors.New("schema activation captures are incomplete")
	}
	if !reflect.DeepEqual(before.RetainedMutations, cut.RetainedMutations) {
		return errors.New("schema activation changed queued intent")
	}
	if prepared {
		if journal.Phase != "prepared" || *cut.Schema != journal.Source || !bytes.Equal(before.PhysicalSchema, cut.PhysicalSchema) || *cut.ScopeStates[0].Cursor != *before.ScopeStates[0].Cursor {
			return errors.New("prepared cut changed active schema, physical schema, or cursor")
		}
		return nil
	}
	if journal.Phase != "applied" || *cut.Schema != journal.Target || *cut.ScopeStates[0].Cursor != issued || bytes.Equal(before.PhysicalSchema, cut.PhysicalSchema) {
		return errors.New("committed cut did not activate schema, DDL, and cursor together")
	}
	return nil
}

func compareSchemaProofServer(before, after blackbox.NativeCaptureFacts) error {
	if len(before.StateFacts.Rows) == 0 || len(before.StateFacts.MutationOutcomes) == 0 || !reflect.DeepEqual(before.StateFacts.Rows, after.StateFacts.Rows) || !reflect.DeepEqual(before.StateFacts.MutationOutcomes, after.StateFacts.MutationOutcomes) {
		return errors.New("M1 replay changed authoritative row/version/checksum or outcomes")
	}
	return nil
}

func schemaProofSchema(controller *blackbox.NativeController, scenario scenarios.Scenario, aliasName string) (schemaRef, error) {
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Kind == "schema" && alias.Alias == aliasName {
			values, err := controller.IdentityValues([]scenarios.NativeIdentityAlias{alias})
			if err != nil || len(values) != 1 {
				return schemaRef{}, fmt.Errorf("resolve proof schema: %v", err)
			}
			var value schemaRef
			if json.Unmarshal(values[0].RuntimeValue, &value) != nil || value.Version <= 0 || !validLowerHexDigest(value.Hash) {
				return schemaRef{}, errors.New("proof runtime schema is invalid")
			}
			return value, nil
		}
	}
	return schemaRef{}, errors.New("proof schema alias is absent")
}

func schemaProofClientPushes(platform *Platform, clientID string) []schemaProofPush {
	platform.mu.Lock()
	defer platform.mu.Unlock()
	var values []schemaProofPush
	for _, push := range platform.schemaProofPushes {
		if push.ClientID == clientID {
			values = append(values, push)
		}
	}
	return values
}

func requireSchemaProofOriginal(original retainedMutation, values []retainedMutation) error {
	matched := false
	for _, value := range values {
		if value.MutationID == original.MutationID {
			if matched {
				return errors.New("original local record is ambiguous")
			}
			matched = true
			if !sameSchemaProofIntent([]retainedMutation{original}, []retainedMutation{value}) || !equalOptionalStrings(original.DependsOnMutationID, value.DependsOnMutationID) {
				return errors.New("original local record changed immutable intent")
			}
			if original.NormalizedMutationID != nil || value.NormalizedMutationID != nil || value.SourceKind == "normalized" {
				return errors.New("single-write original acquired normalized lineage")
			}
			if value.Status == "pending" {
				if original.Status != "pending" || value.SealedBatchID != nil || value.SealedOrdinal != nil {
					return errors.New("pending original has invalid sealing state")
				}
			} else if value.Status == "sealed" {
				if value.SealedBatchID == nil || *value.SealedBatchID == "" || value.SealedOrdinal == nil || *value.SealedOrdinal != 0 {
					return errors.New("sealed original has no exact singleton membership")
				}
			} else {
				return errors.New("original local record has an unexpected state")
			}
			if original.SealedBatchID != nil && (!equalOptionalStrings(original.SealedBatchID, value.SealedBatchID) || !reflect.DeepEqual(original.SealedOrdinal, value.SealedOrdinal)) {
				return errors.New("sealed original changed its established membership")
			}
		}
	}
	if !matched {
		return errors.New("original local record is no longer inspectable")
	}
	return nil
}

func requireSchemaProofSuccessorTransition(before, after retainedMutation, base string) error {
	if before.Status != "pending" || before.SealedBatchID != nil || before.SealedOrdinal != nil || before.DependsOnMutationID == nil || base == "" || after.Status != "sealed" || after.BaseVersion == nil || *after.BaseVersion != base || after.DependsOnMutationID != nil || after.SealedBatchID == nil || *after.SealedBatchID == "" || after.SealedOrdinal == nil || *after.SealedOrdinal != 0 {
		return errors.New("M2 did not seal against its validated accepted predecessor base")
	}
	compare := after
	compare.BaseVersion = before.BaseVersion
	compare.DependsOnMutationID = before.DependsOnMutationID
	if !sameSchemaProofIntent([]retainedMutation{before}, []retainedMutation{compare}) || before.NormalizedMutationID != nil || after.NormalizedMutationID != nil || after.SourceKind == "normalized" {
		return errors.New("original M2 changed more than base and dependency")
	}
	return nil
}

func requireSchemaProofMutation(intent retainedMutation, mutation wireMutation) error {
	if mutation.MutationID == "" || mutation.MutationID != intent.MutationID || mutation.AuthoredSchema != intent.AuthoredSchema || mutation.Table != intent.TableID || mutation.Operation != intent.Operation || mutation.ClientVersion != intent.ClientVersion || !equalOptionalStrings(mutation.BaseVersion, intent.BaseVersion) || len(mutation.PrimaryKey) != 1 || len(mutation.Columns) != len(intent.AuthoredFields) {
		return errors.New("sealed proof mutation changed authored intent")
	}
	var recordID string
	if json.Unmarshal(mutation.PrimaryKey[intent.PrimaryKeyFieldID], &recordID) != nil || recordID != intent.RecordID {
		return errors.New("sealed proof mutation changed its row identity")
	}
	for _, field := range intent.AuthoredFields {
		value, found := mutation.Columns[field.FieldID]
		var authoredValue, wireValue *string
		if !found || json.Unmarshal(field.Value, &authoredValue) != nil || authoredValue == nil || json.Unmarshal(value, &wireValue) != nil || wireValue == nil || *wireValue != *authoredValue {
			return errors.New("sealed proof mutation changed authored fields")
		}
	}
	return nil
}

func requireSchemaProofStoredOutcome(capture runnerResult, push schemaProofPush) error {
	if err := capture.requireCompleteAcceptedMutationOutcomes(); err != nil {
		return err
	}
	request, err := decodeSchemaProofPush(push)
	if err != nil {
		return err
	}
	stored, found := capture.AcceptedMutationOutcomes[request.Mutations[0].MutationID]
	if !found {
		return errors.New("actual accepted mutation identity is absent from durable storage")
	}
	var response struct {
		Accepted []json.RawMessage `json:"accepted"`
	}
	if json.Unmarshal(push.Response, &response) != nil || len(response.Accepted) != 1 || blackbox.CompareSemanticJSON([]byte(stored), response.Accepted[0], blackbox.NormalizationSpec{}) != nil {
		return errors.New("durable accepted outcome differs from its actual server response")
	}
	return nil
}

func requireSchemaProofRowMetadata(capture runnerResult, original retainedMutation, push schemaProofPush) error {
	var response struct {
		Accepted []struct {
			ServerVersion string `json:"server_version"`
		} `json:"accepted"`
	}
	if json.Unmarshal(push.Response, &response) != nil || len(response.Accepted) != 1 || response.Accepted[0].ServerVersion == "" {
		return errors.New("validated proof accepted version is absent")
	}
	if original.TableName == "" || original.RecordID == "" {
		return errors.New("proof row metadata identity is absent")
	}
	matches := 0
	for _, record := range capture.RowMetadataRecords {
		if record.TableName == original.TableName && record.RecordID == original.RecordID {
			matches++
			if record.ServerVersion != response.Accepted[0].ServerVersion {
				return errors.New("proof row metadata version differs from its latest accepted outcome")
			}
		}
	}
	if matches != 1 {
		return errors.New("proof row metadata is absent or ambiguous")
	}
	return nil
}

func validateSchemaProofRecoveryWire(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, name string, call SynchronizationResult, target schemaRef, recovered runnerResult) error {
	if len(call.transportObservations) < 3 {
		return errors.New("proof recovery omitted real connect, push, or pull traffic")
	}
	if recovered.Schema == nil || *recovered.Schema != target || len(recovered.ScopeStates) != 1 || recovered.ScopeStates[0].Cursor == nil || *recovered.ScopeStates[0].Cursor == "" {
		return errors.New("proof recovered schema or installed cursor is absent")
	}
	installed := []string{cursorFingerprint(*recovered.ScopeStates[0].Cursor)}
	if err := validateSwiftWireObservation(scenario, string(schemaProofStep(steps, name+"-RECOVER").ID), call.transportObservations[0]); err != nil {
		return err
	}
	connect := call.transportObservations[0]
	facts := connect.ConnectResponseFacts
	if connect.OperationClass != "connect" || connect.RequestFacts == nil || connect.RequestFacts.SchemaVersion != target.Version || connect.RequestFacts.SchemaHash != target.Hash || connect.CursorFingerprintsComplete == nil || !*connect.CursorFingerprintsComplete || !slices.Equal(connect.CursorFingerprints, installed) || facts == nil || facts.Action != "none" || facts.SchemaVersion != target.Version || facts.SchemaHash != target.Hash {
		return errors.New("recovery connect did not use recovered S2")
	}
	foundPull := false
	for _, observation := range call.transportObservations[1:] {
		if observation.OperationClass != "pull" {
			continue
		}
		if observation.StatusCode != 200 || observation.RequestFacts == nil || observation.RequestFacts.SchemaVersion != target.Version || observation.RequestFacts.SchemaHash != target.Hash || observation.CursorFingerprintsComplete == nil || !*observation.CursorFingerprintsComplete || !slices.Equal(observation.CursorFingerprints, installed) {
			return errors.New("first proof pull did not use the recovered S2 cursor successfully")
		}
		foundPull = true
		break
	}
	if !foundPull {
		return errors.New("proof recovery omitted its first target pull")
	}
	pull := call.transportObservations[len(call.transportObservations)-1]
	if pull.OperationClass != "pull" || pull.RequestFacts == nil || pull.RequestFacts.SchemaVersion != target.Version || pull.RequestFacts.SchemaHash != target.Hash {
		return errors.New("proof terminal pull did not use S2")
	}
	return validateSwiftWireObservation(scenario, string(schemaProofStep(steps, name+"-COMPLETE").ID), pull)
}

func requireSchemaProofPhysical(controller *blackbox.NativeController, scenario scenarios.Scenario, capture runnerResult) error {
	if capture.Schema == nil {
		return errors.New("physical schema has no active binding")
	}
	var manifest json.RawMessage
	alias, writeSuffix := "schema-v1", "PREPARED-WRITE"
	switch capture.Schema.Version {
	case 1:
		var setup struct {
			InitialSchema json.RawMessage `json:"initial_schema"`
		}
		if len(scenario.Model.Setup) == 0 || json.Unmarshal(scenario.Model.Setup[0].Payload, &setup) != nil {
			return errors.New("authored S1 physical manifest is invalid")
		}
		manifest = setup.InitialSchema
	case 2:
		alias, writeSuffix = "schema-v2", "COMMITTED-M2-WRITE"
		for _, step := range scenario.Steps {
			if step.ID == "STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001" {
				manifest = step.Operation.Payload
			}
		}
	default:
		return errors.New("schema proof physical manifest is not S1 or S2")
	}
	schema, err := schemaProofSchema(controller, scenario, alias)
	if err != nil {
		return err
	}
	if schema != *capture.Schema {
		return errors.New("physical schema active identity differs from its authored binding")
	}
	for _, step := range scenario.Steps {
		if step.ID == scenarios.StepID("STEP-PERF-SCHEMA-CHECK-PROOF-"+writeSuffix+"-001") {
			write, err := controller.ApplicationWrite(step.Operation)
			if err != nil {
				return err
			}
			return compareSchemaProofPhysical(manifest, step.Operation.Payload, write.Payload, capture.PhysicalSchema)
		}
	}
	return errors.New("schema proof authored physical field binding is absent")
}

func compareSchemaProofPhysical(manifest, authoredWrite, runtimeWrite, physical json.RawMessage) error {
	var authored struct {
		Tables []struct {
			TableID string `json:"table_id"`
			Fields  []struct {
				ID         string `json:"field_id"`
				Type       string `json:"type"`
				Nullable   *bool  `json:"nullable"`
				PrimaryKey *bool  `json:"primary_key"`
			} `json:"fields"`
		} `json:"tables"`
	}
	var logical, runtime struct {
		Table   string                     `json:"table_id"`
		PK      map[string]json.RawMessage `json:"pk"`
		Columns map[string]json.RawMessage `json:"columns"`
	}
	if json.Unmarshal(manifest, &authored) != nil || len(authored.Tables) != 1 || authored.Tables[0].TableID != "items" || len(authored.Tables[0].Fields) < 2 || len(authored.Tables[0].Fields) > 3 {
		return errors.New("schema proof authored table manifest is incomplete")
	}
	if json.Unmarshal(authoredWrite, &logical) != nil || json.Unmarshal(runtimeWrite, &runtime) != nil || logical.Table != authored.Tables[0].TableID || runtime.Table != "cf_items" || len(logical.PK) != 1 || len(runtime.PK) != 1 || logical.PK["id"] == nil || runtime.PK["id"] == nil || len(logical.Columns) != len(runtime.Columns) || len(logical.Columns)+1 != len(authored.Tables[0].Fields) {
		return errors.New("schema proof physical field binding is incomplete")
	}
	expected := make(map[string]physicalSchemaColumn)
	for _, field := range authored.Tables[0].Fields {
		if (field.ID != "id" && field.ID != "value" && field.ID != "note") || field.Nullable == nil || field.PrimaryKey == nil || *field.PrimaryKey != (field.ID == "id") || field.Type != "string" {
			return errors.New("schema proof authored field storage is incomplete or unsupported")
		}
		pk := 0
		if *field.PrimaryKey {
			pk = 1
		} else {
			value, found := logical.Columns[field.ID]
			runtimeValue, bound := runtime.Columns[field.ID]
			var authoredValue, boundValue *string
			if !found || !bound || json.Unmarshal(value, &authoredValue) != nil || authoredValue == nil || json.Unmarshal(runtimeValue, &boundValue) != nil || boundValue == nil || *boundValue != *authoredValue {
				return errors.New("authored physical field has an invalid fixture binding")
			}
		}
		if _, found := expected[field.ID]; found {
			return errors.New("authored physical field binding is duplicated")
		}
		expected[field.ID] = physicalSchemaColumn{TableName: "cf_items", Name: field.ID, Type: "TEXT", NotNull: !*field.Nullable && !*field.PrimaryKey, PrimaryKeyPosition: pk}
	}
	if _, found := expected["id"]; !found {
		return errors.New("authored fixture primary key is absent")
	}
	if _, found := expected["value"]; !found {
		return errors.New("authored fixture value field is absent")
	}
	// The independent cf_items fixture adds ownership and lifecycle columns beyond authored proof fields.
	for _, column := range []physicalSchemaColumn{
		{TableName: "cf_items", Name: "owner_id", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "updated_at", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "deleted_at", Type: "TEXT"},
	} {
		expected[column.Name] = column
	}
	columns, err := decodePhysicalSchema(physical)
	if err != nil {
		return err
	}
	subjectCount := 0
	for _, column := range columns {
		if column.TableName != "cf_items" {
			continue
		}
		subjectCount++
		want, found := expected[column.Name]
		if !found || column != want {
			return errors.New("physical schema column type, nullability, or primary key differs from the complete manifest")
		}
	}
	if subjectCount != len(expected) {
		return errors.New("physical fixture column set differs from the complete independent expectation")
	}
	return nil
}

func requireSchemaProofApplied(push schemaProofPush, id string, schema schemaRef) error {
	var response struct {
		Accepted []struct {
			MutationID string    `json:"mutation_id"`
			Status     string    `json:"status"`
			Schema     schemaRef `json:"outcome_schema"`
		} `json:"accepted"`
		Rejected []json.RawMessage `json:"rejected"`
	}
	if push.Status != 200 || json.Unmarshal(push.Response, &response) != nil || len(response.Accepted) != 1 || len(response.Rejected) != 0 || response.Accepted[0].MutationID != id || response.Accepted[0].Status != "applied" || response.Accepted[0].Schema != schema {
		return errors.New("real historical push did not return one applied S1 outcome")
	}
	return nil
}

func runSchemaProofReplay(ctx context.Context, fixture *scenarios.NativeLocalFixture, steps map[scenarios.StepID]scenarios.Step, controller *blackbox.NativeController, platform *Platform, lane schemaProofLane, m2 scenarios.Operation, result *SchemaCheckResult) error {
	state, _ := platform.client(lane.client)
	beforeM2, err := captureRunner(ctx, state)
	if err != nil {
		return err
	}
	if len(beforeM2.RetainedMutations) != 2 || len(beforeM2.AcceptedMutationOutcomes) != 0 {
		return errors.New("unreconciled M1 and M2 ledger is not exact")
	}
	if err := requireSchemaProofOriginal(lane.localOriginal, beforeM2.RetainedMutations); err != nil {
		return err
	}
	var m2Original retainedMutation
	for _, value := range beforeM2.RetainedMutations {
		if value.SourceKind == lane.localOriginal.SourceKind && value.MutationID != lane.localOriginal.MutationID && value.LocalOrder > lane.localOriginal.LocalOrder {
			if m2Original.MutationID != "" {
				return errors.New("M2 original intent is ambiguous")
			}
			m2Original = value
		}
	}
	if m2Original.MutationID == "" || m2Original.AuthoredSchema != *beforeM2.Schema || !equalOptionalStrings(m2Original.BaseVersion, lane.localOriginal.BaseVersion) || m2Original.Status != "pending" || m2Original.NormalizedMutationID != nil || m2Original.SealedBatchID != nil || m2Original.SealedOrdinal != nil || m2Original.DependsOnMutationID == nil || *m2Original.DependsOnMutationID != lane.localOriginal.MutationID {
		return errors.New("M2 original did not retain its actual local base and S2 binding")
	}
	if err := scenarios.RequireLocalWriteRow(m2, beforeM2.ApplicationRows); err != nil {
		return err
	}
	if err := captureSchemaProof(result, "COMMITTED-M2-INTENT", beforeM2); err != nil {
		return err
	}
	serverBefore, err := controller.Capture(ctx, []string{lane.client.Key}, []string{"server-state"})
	if err != nil || len(serverBefore) != 1 {
		return fmt.Errorf("capture before replay: %v", err)
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: "push"}); err != nil {
		return err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
		return err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: "push"}); err != nil {
		return err
	}
	platform.mu.Lock()
	var replay schemaProofPush
	for _, push := range platform.schemaProofPushes {
		if push.ClientID == lane.client.ClientID {
			replay = push
		}
	}
	platform.mu.Unlock()
	originalRequest, err := decodeSchemaProofPush(lane.original)
	if err != nil {
		return err
	}
	replayRequest, err := decodeSchemaProofPush(replay)
	if err != nil {
		return err
	}
	if originalRequest.Mutations[0].MutationID != lane.localOriginal.MutationID || replayRequest.Schema != *beforeM2.Schema || !reflect.DeepEqual(originalRequest.Mutations, replayRequest.Mutations) || originalRequest.BatchID == replayRequest.BatchID && !bytes.Equal(lane.original.Request, replay.Request) {
		return errors.New("replay changed immutable M1 or reused batch bytes")
	}
	m1ID := originalRequest.Mutations[0].MutationID
	if err := requireSchemaProofApplied(replay, m1ID, originalRequest.Mutations[0].AuthoredSchema); err != nil {
		return err
	}
	var first, second struct {
		Accepted []json.RawMessage `json:"accepted"`
	}
	if json.Unmarshal(lane.original.Response, &first) != nil || json.Unmarshal(replay.Response, &second) != nil || len(first.Accepted) != 1 || len(second.Accepted) != 1 {
		return errors.New("historical outcome capture is incomplete")
	}
	if err := blackbox.CompareSemanticJSON(first.Accepted[0], second.Accepted[0], blackbox.NormalizationSpec{}); err != nil {
		return errors.New("M1 replay did not return the original historical outcome")
	}
	serverAfter, err := controller.Capture(ctx, []string{lane.client.Key}, []string{"server-state"})
	if err != nil || len(serverAfter) != 1 {
		return fmt.Errorf("capture after replay: %v", err)
	}
	if err := compareSchemaProofServer(serverBefore[0], serverAfter[0]); err != nil {
		return err
	}
	pausedM1, err := captureRunner(ctx, state)
	if err != nil {
		return err
	}
	result.ProofServerCaptures["EXPECT-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-SERVER-BEFORE-001"] = serverBefore[0]
	result.ProofServerCaptures["EXPECT-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-SERVER-AFTER-001"] = serverAfter[0]
	if len(pausedM1.RetainedMutations) != 2 || len(pausedM1.AcceptedMutationOutcomes) != 0 {
		return errors.New("M1 replay reconciled before its response pause")
	}
	if err := requireSchemaProofOriginal(lane.localOriginal, pausedM1.RetainedMutations); err != nil {
		return err
	}
	if err := requireSchemaProofOriginal(m2Original, pausedM1.RetainedMutations); err != nil {
		return err
	}
	for _, value := range pausedM1.RetainedMutations {
		if value.MutationID == m2Original.MutationID && !reflect.DeepEqual(value, m2Original) {
			return errors.New("unreconciled original M2 changed before M1 acceptance")
		}
	}
	if err := bindSchemaProofPush(controller, schemaProofStep(steps, "COMMITTED-M2-REPLY").Operation); err != nil {
		return err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: "push"}); err != nil {
		return err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
		return err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: "push"}); err != nil {
		return err
	}
	reconciled, err := captureRunner(ctx, state)
	if err != nil {
		return err
	}
	if err := requireSchemaProofStoredOutcome(reconciled, replay); err != nil {
		return err
	}
	if len(reconciled.RetainedMutations) != 1 || len(reconciled.AcceptedMutationOutcomes) != 1 {
		return errors.New("M1 reconciliation did not leave exactly one sealed M2")
	}
	sealedM2 := reconciled.RetainedMutations[0]
	var accepted struct {
		Accepted []struct {
			ServerVersion string `json:"server_version"`
		} `json:"accepted"`
	}
	if json.Unmarshal(replay.Response, &accepted) != nil || len(accepted.Accepted) != 1 || accepted.Accepted[0].ServerVersion == "" {
		return errors.New("M1 accepted predecessor base is absent")
	}
	if err := requireSchemaProofSuccessorTransition(m2Original, sealedM2, accepted.Accepted[0].ServerVersion); err != nil {
		return err
	}
	pushes := schemaProofClientPushes(platform, lane.client.ClientID)
	if len(pushes) != 3 {
		return errors.New("M2 response pause is not the third actual push")
	}
	m2Request, err := decodeSchemaProofPush(pushes[2])
	if err != nil || m2Request.Schema != *beforeM2.Schema || m2Request.Mutations[0].MutationID != m2Original.MutationID || sealedM2.SealedBatchID == nil || *sealedM2.SealedBatchID != m2Request.BatchID {
		return errors.New("M2 push is not bound to its sealed original and S2")
	}
	if err := requireSchemaProofMutation(sealedM2, m2Request.Mutations[0]); err != nil {
		return err
	}
	if err := requireSchemaProofApplied(pushes[2], m2Original.MutationID, *beforeM2.Schema); err != nil {
		return err
	}
	if _, found := reconciled.AcceptedMutationOutcomes[m2Original.MutationID]; found {
		return errors.New("M2 reconciled before its response pause")
	}
	if err := captureSchemaProof(result, "COMMITTED-M2-PAUSED", reconciled); err != nil {
		return err
	}
	if err := scenarios.RequireLocalWriteRow(m2, reconciled.ApplicationRows); err != nil {
		return err
	}
	if err := requireSchemaProofSentinel(fixture, reconciled); err != nil {
		return err
	}
	if _, err = state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
		return err
	}
	return nil
}
