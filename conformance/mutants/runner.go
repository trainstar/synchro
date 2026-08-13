package mutants

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const setupStepID scenarios.StepID = "__model_setup__"

// Run executes one scenario through closed operations and checks one
// requirement-owned semantic assertion against the unmodified subject result.
func Run(ctx context.Context, scenario scenarios.Scenario, mutant Mutant) (Result, error) {
	result := Result{ScenarioID: string(scenario.ID)}
	if ctx == nil {
		return result, errors.New("mutant run context is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	descriptor, isBase := baseOrMutantDescriptor(mutant)
	if descriptor.Name == "" {
		return result, invalidDescriptor(descriptor)
	}
	result.Mutant = descriptor.Name
	result.Descriptor = descriptor
	result.RequirementID = descriptor.RequirementID
	if !isBase {
		if err := validateApprovedDescriptor(descriptor); err != nil {
			return result, err
		}
	}

	subject, err := requireSubject(mutant)
	if err != nil {
		return result, err
	}
	rawSubject, ok := subject.(rawResultSubject)
	if !ok {
		return result, errors.New("mutant subject does not expose deterministic raw results")
	}
	stepSubject, _ := subject.(stepAwareSubject)

	var binding expectationBinding
	if !isBase {
		binding, err = ownedAssertion(scenario, descriptor)
		if err != nil {
			return result, err
		}
		result.AssertionID = string(binding.Assertion.ID)
	}
	if err := validateScenarioOperations(scenario); err != nil {
		return result, err
	}

	execute := func(stepID scenarios.StepID, operation scenarios.Operation, expected scenarios.ExpectedOutcome) bool {
		if err := ctx.Err(); err != nil {
			result.Failure = Failure{Kind: FailureExecution, Reason: "scenario context ended before operation execution"}
			return false
		}
		if stepSubject != nil {
			stepSubject.SetStepID(string(stepID))
		}
		observed, operationErr, panicked := invoke(mutant, ctx, operation)
		unmodified, found := rawSubject.RawResult(string(stepID))
		execution := Execution{
			StepID:       stepID,
			Operation:    cloneOperation(operation),
			OperationKey: operationKey(operation),
			Observed:     cloneStepResult(observed),
			Err:          operationErr,
		}
		if found {
			execution.Expected = cloneStepResult(unmodified)
		}
		result.Executions = append(result.Executions, execution)

		if panicked {
			result.Failure = Failure{Kind: FailureCrash, Reason: "mutant operation panicked"}
			return false
		}
		if operationErr != nil {
			result.Failure = Failure{Kind: FailureExecution, Reason: "mutant operation returned an error"}
			return false
		}
		if !found {
			result.Failure = Failure{Kind: FailureExecution, Reason: "subject did not record the unmodified operation result"}
			return false
		}
		if err := matchExpectedOutcome(expected, nil); err != nil {
			result.Failure = Failure{Kind: FailureUnrelated, Reason: "operation outcome did not match the authored outcome"}
			return false
		}
		if err := validateObservedResult(operation, observed); err != nil {
			result.Failure = Failure{Kind: FailureMalformed, Reason: err.Error()}
			return false
		}
		return true
	}

	if len(scenario.Model.Setup) != 1 || !execute(setupStepID, scenario.Model.Setup[0], scenarios.ExpectedOutcome{Disposition: "success"}) {
		if result.Failure.Kind == FailureNone {
			result.Failure = Failure{Kind: FailureContract, Reason: "model setup did not complete"}
		}
		return result, nil
	}
	for _, step := range scenario.Steps {
		if !execute(step.ID, step.Operation, step.ExpectedOutcome) {
			return result, nil
		}
	}

	if isBase {
		result.Passed = true
		return result, nil
	}

	state, hasState := mutant.(mutationState)
	if !hasState || !state.MutationApplied() {
		result.Failure = Failure{
			Kind:          FailureSurvived,
			RequirementID: descriptor.RequirementID,
			AssertionID:   string(binding.Assertion.ID),
			Reason:        "mutant did not change one eligible semantic outcome",
		}
		return result, nil
	}

	execution, found := targetExecution(result.Executions, descriptor)
	if !found {
		result.Failure = Failure{
			Kind:          FailureSurvived,
			RequirementID: descriptor.RequirementID,
			AssertionID:   string(binding.Assertion.ID),
			Reason:        "mutant target operation was not executed",
		}
		return result, nil
	}
	detected, kind, reason := detectSemanticChange(descriptor, binding, execution)
	result.Failure = Failure{
		Kind:          kind,
		RequirementID: descriptor.RequirementID,
		AssertionID:   string(binding.Assertion.ID),
		Reason:        reason,
	}
	if detected {
		result.Detected = true
		result.Passed = false
		return result, nil
	}
	return result, nil
}

func baseOrMutantDescriptor(mutant Mutant) (Descriptor, bool) {
	if isNilInterface(mutant) {
		return Descriptor{}, false
	}
	provider, ok := mutant.(descriptorProvider)
	if !ok {
		return Descriptor{}, false
	}
	descriptor := provider.Descriptor()
	return descriptor, descriptor.Kind == MutationKindBase
}

func validateScenarioOperations(scenario scenarios.Scenario) error {
	if len(scenario.Model.Setup) != 1 {
		return errors.New("scenario model setup must contain exactly one operation")
	}
	if operationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("scenario model setup must install the current contract")
	}
	if err := scenarios.ValidateOperation(scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("validate model setup: %w", err)
	}
	for _, step := range scenario.Steps {
		if step.ID == "" {
			return errors.New("scenario step ID is required")
		}
		if err := scenarios.ValidateOperation(step.Operation); err != nil {
			return fmt.Errorf("validate step %s: %w", step.ID, err)
		}
	}
	return nil
}

func invoke(mutant Mutant, ctx context.Context, operation scenarios.Operation) (result reference.StepResult, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			result = reference.StepResult{}
			err = errors.New("mutant operation panicked")
			panicked = true
		}
	}()
	result, err = mutant.Execute(ctx, cloneOperation(operation))
	return result, err, false
}

func matchExpectedOutcome(expected scenarios.ExpectedOutcome, operationErr error) error {
	switch expected.Disposition {
	case "success":
		if operationErr != nil {
			return operationErr
		}
		return nil
	case "error":
		if operationErr == nil || expected.ErrorCode == nil || canonicalErrorCode(operationErr) != *expected.ErrorCode {
			return errors.New("authored error outcome did not match the canonical error code")
		}
		return nil
	default:
		return errors.New("authored outcome disposition is not closed")
	}
}

func canonicalErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var provider interface{ ErrorCode() string }
	if errors.As(err, &provider) {
		return provider.ErrorCode()
	}
	var codeProvider interface{ Code() string }
	if errors.As(err, &codeProvider) {
		return codeProvider.Code()
	}
	return ""
}

func validateObservedResult(operation scenarios.Operation, result reference.StepResult) error {
	wanted, ok := expectedResultKind(operationKey(operation))
	if !ok {
		return errors.New("operation is not in the closed result-kind registry")
	}
	if result.Kind != wanted {
		return errors.New("operation returned an unexpected typed result kind")
	}
	if result.HTTP != nil {
		if result.HTTP.Status < 100 || result.HTTP.Status > 599 {
			return errors.New("operation returned an invalid HTTP status")
		}
		if result.HTTP.HasCode != (result.HTTP.Code != "") {
			return errors.New("operation returned inconsistent HTTP code presence")
		}
		if !result.HTTP.HasRetryAfterMilliseconds && result.HTTP.RetryAfterMilliseconds != 0 {
			return errors.New("operation returned a hidden retry-after value")
		}
	}
	if requiresHTTP(operationKey(operation)) && result.HTTP == nil {
		return errors.New("endpoint operation omitted its HTTP observation")
	}
	switch wanted {
	case reference.StepResultKindConnect:
		if result.Connect == nil {
			return errors.New("connect operation omitted its typed observation")
		}
	case reference.StepResultKindLocal:
		if result.Local == nil {
			return errors.New("local operation omitted its typed observation")
		}
	case reference.StepResultKindLifecycle:
		if result.Lifecycle == nil {
			return errors.New("lifecycle operation omitted its typed observation")
		}
	case reference.StepResultKindPush:
		if result.Push == nil {
			return errors.New("push operation omitted its typed observation")
		}
	case reference.StepResultKindPull:
		if result.Pull == nil {
			return errors.New("pull operation omitted its typed observation")
		}
	case reference.StepResultKindRebuild:
		if result.Rebuild == nil {
			return errors.New("rebuild operation omitted its typed observation")
		}
	case reference.StepResultKindWAL:
		if result.WAL == nil {
			return errors.New("WAL operation omitted its typed observation")
		}
	case reference.StepResultKindSchema:
		if result.Schema == nil {
			return errors.New("schema operation omitted its typed observation")
		}
	case reference.StepResultKindRetention:
		if result.Retention == nil {
			return errors.New("retention operation omitted its typed observation")
		}
	case reference.StepResultKindClient:
		if result.Client == nil {
			return errors.New("client operation omitted its typed observation")
		}
	}
	return nil
}

func expectedResultKind(key string) (reference.StepResultKind, bool) {
	wanted, ok := map[string]reference.StepResultKind{
		"artifact/install-portable-seed":                reference.StepResultKindLocal,
		"connect/send":                                  reference.StepResultKindConnect,
		"local/apply-pull-page":                         reference.StepResultKindLocal,
		"local/apply-rebuild-page":                      reference.StepResultKindLocal,
		"local/begin-rebuild":                           reference.StepResultKindLocal,
		"local/finalize-rebuild":                        reference.StepResultKindLocal,
		"local/write":                                   reference.StepResultKindLocal,
		"model/activate-registry-membership-generation": reference.StepResultKindSchema,
		"model/commit-source-transaction":               reference.StepResultKindWAL,
		"model/compact-scope":                           reference.StepResultKindRetention,
		"model/expire-client-generation":                reference.StepResultKindClient,
		"model/install-current-contract":                reference.StepResultKindContractInstalled,
		"model/publish-schema":                          reference.StepResultKindSchema,
		"model/set-client-assignments":                  reference.StepResultKindClient,
		"model/stage-registry-membership-generation":    reference.StepResultKindSchema,
		"process/acknowledge-contiguous-prefix":         reference.StepResultKindWAL,
		"process/materialize-source-transaction":        reference.StepResultKindWAL,
		"process/repair-and-retry-source-transaction":   reference.StepResultKindWAL,
		"process/response-loss":                         reference.StepResultKindPush,
		"process/restart-client":                        reference.StepResultKindLifecycle,
		"process/restart-wal-worker":                    reference.StepResultKindWAL,
		"pull/request-page":                             reference.StepResultKindPull,
		"push/submit":                                   reference.StepResultKindPush,
		"rebuild/request-page":                          reference.StepResultKindRebuild,
	}[key]
	return wanted, ok
}

func requiresHTTP(key string) bool {
	switch key {
	case "connect/send", "push/submit", "pull/request-page", "rebuild/request-page":
		return true
	default:
		return false
	}
}

func successfulHTTP(result reference.StepResult) bool {
	return result.HTTP != nil && result.HTTP.Status == 200 && !result.HTTP.HasCode && !result.HTTP.Retryable
}

func targetExecution(executions []Execution, descriptor Descriptor) (Execution, bool) {
	for _, execution := range executions {
		if execution.OperationKey != descriptor.OperationKey {
			continue
		}
		if descriptor.Kind == MutationKindConstantChecksum {
			if execution.Expected.Pull == nil || execution.Expected.Pull.HasMore || len(execution.Expected.Pull.ScopeChecksums) == 0 {
				continue
			}
		}
		return execution, true
	}
	return Execution{}, false
}

func detectSemanticChange(descriptor Descriptor, binding expectationBinding, execution Execution) (bool, FailureKind, string) {
	if execution.Err != nil {
		return false, FailureExecution, "mutant operation failed before assertion evaluation"
	}
	if !successfulHTTP(execution.Expected) || !successfulHTTP(execution.Observed) {
		return false, FailureUnrelated, "target operation was not a successful canonical delivery"
	}
	if binding.StateExpectation != nil && !stateExpectationCoversPull(*binding.StateExpectation, execution.Expected) {
		return false, FailureUnrelated, "authored pull state expectation does not cover the expected delivery"
	}
	switch descriptor.Kind {
	case MutationKindOmitMutation:
		return detectOmittedMutation(execution)
	case MutationKindConstantChecksum:
		return detectConstantChecksum(execution)
	case MutationKindDuplicateDelivery:
		return detectDuplicateDelivery(execution)
	case MutationKindWrongScope:
		return detectWrongScope(execution)
	default:
		return false, FailureUnrelated, "mutant kind is not closed"
	}
}

type pushMutationEnvelope struct {
	Request struct {
		Mutations []struct {
			MutationID string `json:"mutation_id"`
		} `json:"mutations"`
	} `json:"request"`
}

func detectOmittedMutation(execution Execution) (bool, FailureKind, string) {
	var envelope pushMutationEnvelope
	if err := json.Unmarshal(execution.Operation.Payload, &envelope); err != nil {
		return false, FailureMalformed, "push request could not be decoded for semantic comparison"
	}
	if len(envelope.Request.Mutations) == 0 || execution.Expected.Push == nil || execution.Observed.Push == nil {
		return false, FailureFieldPresence, "push mutation outcomes are absent"
	}
	ids := make([]reference.MutationID, len(envelope.Request.Mutations))
	seenIDs := make(map[reference.MutationID]struct{}, len(ids))
	for index, mutation := range envelope.Request.Mutations {
		if mutation.MutationID == "" {
			return false, FailureMalformed, "push request contains an empty mutation identifier"
		}
		ids[index] = reference.MutationID(mutation.MutationID)
		if _, duplicate := seenIDs[ids[index]]; duplicate {
			return false, FailureMalformed, "push request contains duplicate mutation identifiers"
		}
		seenIDs[ids[index]] = struct{}{}
	}
	if !validMutationOutcomes(execution.Expected.Push.Mutations, ids) {
		return false, FailureUnrelated, "unmodified push result does not conserve authored mutation outcomes"
	}
	observed := execution.Observed.Push.Mutations
	if len(observed) != len(execution.Expected.Push.Mutations)-1 || !validObservedMutationOutcomes(observed, ids) {
		return false, FailureUnrelated, "observed push result is not an exact one-outcome omission"
	}
	omitted := -1
	observedIndex := 0
	for expectedIndex := range execution.Expected.Push.Mutations {
		if observedIndex < len(observed) && observed[observedIndex] == execution.Expected.Push.Mutations[expectedIndex] {
			observedIndex++
			continue
		}
		if omitted != -1 {
			return false, FailureUnrelated, "observed push result changes more than one mutation outcome"
		}
		omitted = expectedIndex
	}
	if omitted == -1 || observedIndex != len(observed) {
		return false, FailureUnrelated, "observed push result did not omit exactly one authored outcome"
	}
	return true, FailureSemantic, "owned mutation-outcome assertion detected one missing durable outcome"
}

func validMutationOutcomes(outcomes []reference.MutationObservation, ids []reference.MutationID) bool {
	if len(outcomes) != len(ids) {
		return false
	}
	for index, outcome := range outcomes {
		if outcome.Mutation != ids[index] || !validMutationObservation(outcome) {
			return false
		}
	}
	return true
}

func validObservedMutationOutcomes(outcomes []reference.MutationObservation, ids []reference.MutationID) bool {
	seen := make(map[reference.MutationID]struct{}, len(outcomes))
	known := make(map[reference.MutationID]struct{}, len(ids))
	for _, id := range ids {
		known[id] = struct{}{}
	}
	for _, outcome := range outcomes {
		if outcome.Mutation == "" || !validMutationObservation(outcome) {
			return false
		}
		if _, found := known[outcome.Mutation]; !found {
			return false
		}
		if _, duplicate := seen[outcome.Mutation]; duplicate {
			return false
		}
		seen[outcome.Mutation] = struct{}{}
	}
	return true
}

func validMutationState(state reference.MutationOutcomeState) bool {
	switch state {
	case reference.MutationOutcomeApplied, reference.MutationOutcomeConflict, reference.MutationOutcomeRejectedTerminal:
		return true
	default:
		return false
	}
}

func validMutationObservation(outcome reference.MutationObservation) bool {
	if !validMutationState(outcome.State) {
		return false
	}
	if outcome.State == reference.MutationOutcomeApplied {
		return outcome.Reason == ""
	}
	return outcome.Reason != ""
}

func detectConstantChecksum(execution Execution) (bool, FailureKind, string) {
	if execution.Expected.Pull == nil || execution.Observed.Pull == nil || execution.Expected.Pull.HasMore || execution.Observed.Pull.HasMore {
		return false, FailureFieldPresence, "terminal pull checksum observation is absent"
	}
	expected := execution.Expected.Pull.ScopeChecksums
	observed := execution.Observed.Pull.ScopeChecksums
	if len(expected) == 0 || len(observed) != len(expected) {
		return false, FailureFieldPresence, "terminal pull checksum set is incomplete"
	}
	differences := 0
	for index := range expected {
		if !expected[index].HasChecksum || !observed[index].HasChecksum || expected[index].Scope != observed[index].Scope {
			return false, FailureFieldPresence, "terminal pull checksum field is incomplete"
		}
		if expected[index].Checksum != observed[index].Checksum {
			differences++
		}
	}
	if differences != 1 {
		return false, FailureUnrelated, "observed terminal pull changed zero or more than one checksum"
	}
	withObservedChecksums := cloneStepResult(execution.Expected)
	withObservedChecksums.Pull.ScopeChecksums = append([]reference.ScopeChecksumObservation(nil), observed...)
	for index := range withObservedChecksums.Pull.ScopeChecksums {
		withObservedChecksums.Pull.ScopeChecksums[index].Checksum = observed[index].Checksum
	}
	if !reflect.DeepEqual(withObservedChecksums, execution.Observed) {
		return false, FailureUnrelated, "observed terminal pull changed data outside one checksum"
	}
	return true, FailureSemantic, "owned integrity assertion detected a non-authoritative constant digest"
}

func detectDuplicateDelivery(execution Execution) (bool, FailureKind, string) {
	if execution.Expected.Pull == nil || execution.Observed.Pull == nil || len(execution.Expected.Pull.Changes) == 0 {
		return false, FailureFieldPresence, "pull delivery effects are absent"
	}
	if !validPullChanges(execution.Expected.Pull.Changes) || !validPullChanges(execution.Observed.Pull.Changes) {
		return false, FailureFieldPresence, "pull delivery effect fields are incomplete"
	}
	if len(execution.Observed.Pull.Changes) != len(execution.Expected.Pull.Changes)+1 {
		return false, FailureUnrelated, "observed pull changed effect cardinality by more than one"
	}
	for index := range execution.Expected.Pull.Changes {
		withRemoval := append([]reference.PullChangeObservation(nil), execution.Observed.Pull.Changes[:index]...)
		withRemoval = append(withRemoval, execution.Observed.Pull.Changes[index+1:]...)
		if reflect.DeepEqual(withRemoval, execution.Expected.Pull.Changes) && equalPullEnvelopeExceptChanges(execution.Expected.Pull, execution.Observed.Pull) {
			return true, FailureSemantic, "owned delivery assertion detected one duplicated effect"
		}
	}
	return false, FailureUnrelated, "observed pull effects are not one exact duplicate"
}

func detectWrongScope(execution Execution) (bool, FailureKind, string) {
	if execution.Expected.Pull == nil || execution.Observed.Pull == nil || len(execution.Expected.Pull.Changes) == 0 {
		return false, FailureFieldPresence, "pull delivery effects are absent"
	}
	if !validPullChanges(execution.Expected.Pull.Changes) || !validPullChanges(execution.Observed.Pull.Changes) {
		return false, FailureFieldPresence, "pull delivery effect fields are incomplete"
	}
	if len(execution.Observed.Pull.Changes) != len(execution.Expected.Pull.Changes) || !equalPullEnvelopeExceptChanges(execution.Expected.Pull, execution.Observed.Pull) {
		return false, FailureUnrelated, "observed pull changed fields outside one scope binding"
	}
	differences := 0
	for index := range execution.Expected.Pull.Changes {
		expected := execution.Expected.Pull.Changes[index]
		observed := execution.Observed.Pull.Changes[index]
		observedWithoutScope := observed
		observedWithoutScope.Scope = expected.Scope
		if reflect.DeepEqual(expected, observedWithoutScope) && expected.Scope != observed.Scope {
			differences++
		}
	}
	if differences != 1 {
		return false, FailureUnrelated, "observed pull changed zero or more than one scope binding"
	}
	return true, FailureSemantic, "owned scope assertion detected one row delivered under the wrong scope"
}

func validPullChanges(changes []reference.PullChangeObservation) bool {
	for _, change := range changes {
		if change.Scope == "" || change.Row.CanonicalIdentityBytes == "" || change.Version == "" || !change.HasChecksum {
			return false
		}
		switch change.Operation {
		case reference.EffectOperationDelete, reference.EffectOperationUpsert:
		default:
			return false
		}
	}
	return true
}

func equalPullEnvelopeExceptChanges(expected, observed *reference.PullObservation) bool {
	if expected == nil || observed == nil {
		return expected == observed
	}
	return reflect.DeepEqual(expected.ScopeCursors, observed.ScopeCursors) &&
		reflect.DeepEqual(expected.AddedScopes, observed.AddedScopes) &&
		reflect.DeepEqual(expected.RemovedScopes, observed.RemovedScopes) &&
		reflect.DeepEqual(expected.RebuildScopes, observed.RebuildScopes) &&
		expected.HasMore == observed.HasMore &&
		reflect.DeepEqual(expected.ScopeChecksums, observed.ScopeChecksums)
}

// RequireAllDetected rejects every survivor and every non-semantic failure.
func RequireAllDetected(results []Result) error {
	wanted := approvedDescriptors
	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.Descriptor.Kind == MutationKindBase || result.Mutant == "base" {
			continue
		}
		expected, found := wanted[result.Mutant]
		if !found {
			return fmt.Errorf("unexpected mutant result %q", result.Mutant)
		}
		if result.Descriptor != expected || result.ScenarioID != expected.ScenarioID || result.RequirementID != expected.RequirementID || result.AssertionID != expected.AssertionID {
			return fmt.Errorf("mutant %q did not use its exact authored binding", result.Mutant)
		}
		if _, duplicate := seen[result.Mutant]; duplicate {
			return fmt.Errorf("mutant %q was executed more than once", result.Mutant)
		}
		seen[result.Mutant] = struct{}{}
		if !result.Detected || result.Passed || result.Failure.Kind != FailureSemantic {
			return fmt.Errorf("mutant %q was not detected by its semantic assertion", result.Mutant)
		}
		if result.Failure.RequirementID != expected.RequirementID || result.Failure.AssertionID != expected.AssertionID {
			return fmt.Errorf("mutant %q has no exact requirement-owned assertion detection", result.Mutant)
		}
	}
	if len(seen) != len(wanted) {
		return fmt.Errorf("detected %d mutants, want %d", len(seen), len(wanted))
	}
	return nil
}

type expectationBinding struct {
	Assertion        scenarios.Assertion
	Expectations     []scenarios.ModelExpectation
	StateExpectation *scenarios.ModelExpectation
	WireExpectations []scenarios.WireExpectation
}

type approvedWireExpectation struct {
	StepID       string
	ContractCase string
	HTTPStatus   int
	Retryable    bool
}

var approvedWireExpectations = map[string][]approvedWireExpectation{
	OmitMutationName: {
		{StepID: "STEP-PERF-PENDING-CYCLE-002", ContractCase: "push_success", HTTPStatus: 200},
		{StepID: "STEP-PERF-PENDING-CYCLE-003", ContractCase: "pull_success", HTTPStatus: 200},
	},
	ConstantChecksumName: {
		{StepID: "STEP-PERF-STEADY-PULL-001", ContractCase: "pull_success", HTTPStatus: 200},
	},
	DuplicateDeliveryName: {
		{StepID: "STEP-PULL-DIVERGENT-REBUILD-A-REQUEST-001", ContractCase: "rebuild_success", HTTPStatus: 200},
		{StepID: "STEP-PULL-DIVERGENT-REBUILD-B-REQUEST-001", ContractCase: "rebuild_success", HTTPStatus: 200},
		{StepID: "STEP-PULL-DIVERGENT-PAGE-001", ContractCase: "pull_success", HTTPStatus: 200},
		{StepID: "STEP-PULL-DIVERGENT-PAGE-002", ContractCase: "pull_success", HTTPStatus: 200},
	},
	WrongScopeName: {
		{StepID: "STEP-PULL-DIVERGENT-REBUILD-A-REQUEST-001", ContractCase: "rebuild_success", HTTPStatus: 200},
		{StepID: "STEP-PULL-DIVERGENT-REBUILD-B-REQUEST-001", ContractCase: "rebuild_success", HTTPStatus: 200},
		{StepID: "STEP-PULL-DIVERGENT-PAGE-001", ContractCase: "pull_success", HTTPStatus: 200},
		{StepID: "STEP-PULL-DIVERGENT-PAGE-002", ContractCase: "pull_success", HTTPStatus: 200},
	},
}

var approvedExpectationIDs = map[string][]string{
	OmitMutationName:      {"EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001"},
	ConstantChecksumName:  {"EXPECT-PERF-STEADY-PULL-SEMANTIC-001"},
	DuplicateDeliveryName: {"EXPECT-PULL-DIVERGENT-SEMANTIC-001", "EXPECT-PULL-DIVERGENT-WIRE-001"},
	WrongScopeName:        {"EXPECT-PULL-DIVERGENT-SEMANTIC-001", "EXPECT-PULL-DIVERGENT-WIRE-001"},
}

var approvedAssertionRequirements = map[string][]string{
	OmitMutationName:      {RequirementMutationOutcome},
	ConstantChecksumName:  {RequirementChecksum},
	DuplicateDeliveryName: {RequirementDuplicate, "SYNC-PULL-002"},
	WrongScopeName:        {RequirementWrongScope, "SYNC-PULL-002"},
}

func ownedAssertion(scenario scenarios.Scenario, descriptor Descriptor) (expectationBinding, error) {
	if string(scenario.ID) != descriptor.ScenarioID {
		return expectationBinding{}, fmt.Errorf("mutant %q requires scenario %q", descriptor.Name, descriptor.ScenarioID)
	}
	if !containsRequirement(scenario.RequirementIDs, descriptor.RequirementID) {
		return expectationBinding{}, fmt.Errorf("scenario does not declare mutant requirement %q", descriptor.RequirementID)
	}
	assertion, found := assertionByID(scenario.Assertions, descriptor.AssertionID)
	if !found {
		return expectationBinding{}, fmt.Errorf("scenario does not declare mutant assertion %q", descriptor.AssertionID)
	}
	if !containsRequirement(assertion.RequirementIDs, descriptor.RequirementID) || !hasOwnership(scenario, descriptor.RequirementID, descriptor.AssertionID) {
		return expectationBinding{}, fmt.Errorf("assertion %q is not owned by requirement %q", descriptor.AssertionID, descriptor.RequirementID)
	}
	if len(assertion.DetectsControlIDs) != 0 || assertion.Predicate.Name == "negative-control-detected" {
		return expectationBinding{}, fmt.Errorf("assertion %q is not a non-control semantic assertion", descriptor.AssertionID)
	}
	approvedRequirements, hasApprovedRequirements := approvedAssertionRequirements[descriptor.Name]
	if (hasApprovedRequirements && !reflect.DeepEqual(assertion.RequirementIDs, contractRequirementIDs(approvedRequirements))) ||
		assertion.Predicate.ContractPredicate != "wire-outcome" || assertion.Predicate.Name != preferredPredicate(descriptor.Kind) ||
		!emptyObject(assertion.Predicate.Payload) || assertion.Oracle.Kind != "wire-contract" ||
		assertion.Oracle.ExpectedSource != "authored-model" || assertion.Oracle.ObservedSource != "system-under-test" {
		return expectationBinding{}, fmt.Errorf("assertion %q is not the required exact semantic oracle", descriptor.AssertionID)
	}
	wantedIDs, found := approvedExpectationIDs[descriptor.Name]
	if !found {
		wantedIDs = expectationIDStrings(assertion.ExpectationIDs)
	}
	if !reflect.DeepEqual(expectationIDStrings(assertion.ExpectationIDs), wantedIDs) {
		return expectationBinding{}, fmt.Errorf("assertion %q has an unexpected expectation binding", descriptor.AssertionID)
	}
	expectations, err := resolveExpectations(scenario, assertion.ExpectationIDs)
	if err != nil {
		return expectationBinding{}, fmt.Errorf("assertion %q: %w", descriptor.AssertionID, err)
	}
	if err := validateApprovedExpectations(descriptor, expectations); err != nil {
		return expectationBinding{}, fmt.Errorf("assertion %q: %w", descriptor.AssertionID, err)
	}
	wireExpectations, err := resolveWireExpectations(scenario, descriptor.AssertionID)
	if err != nil {
		return expectationBinding{}, fmt.Errorf("assertion %q: %w", descriptor.AssertionID, err)
	}
	if err := validateApprovedWireExpectations(descriptor, wireExpectations); err != nil {
		return expectationBinding{}, fmt.Errorf("assertion %q: %w", descriptor.AssertionID, err)
	}
	binding := expectationBinding{Assertion: assertion, Expectations: expectations, WireExpectations: wireExpectations}
	for index := range expectations {
		if expectations[index].Predicate.Name == "state-equals-authored-model" {
			state := expectations[index]
			binding.StateExpectation = &state
		}
	}
	return binding, nil
}

func contractRequirementIDs(values []string) []contract.RequirementID {
	result := make([]contract.RequirementID, len(values))
	for index, value := range values {
		result[index] = contract.RequirementID(value)
	}
	return result
}

func expectationIDStrings(values []scenarios.ExpectationID) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value)
	}
	return result
}

func emptyObject(payload json.RawMessage) bool {
	return string(bytes.TrimSpace(payload)) == "{}"
}

func resolveExpectations(scenario scenarios.Scenario, ids []scenarios.ExpectationID) ([]scenarios.ModelExpectation, error) {
	byID := make(map[scenarios.ExpectationID]scenarios.ModelExpectation, len(scenario.Model.ExpectedState))
	for _, expectation := range scenario.Model.ExpectedState {
		if _, duplicate := byID[expectation.ID]; duplicate {
			return nil, fmt.Errorf("expectation %q is duplicated", expectation.ID)
		}
		byID[expectation.ID] = expectation
	}
	result := make([]scenarios.ModelExpectation, len(ids))
	seen := make(map[scenarios.ExpectationID]struct{}, len(ids))
	for index, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("expectation %q is bound more than once", id)
		}
		expectation, found := byID[id]
		if !found {
			return nil, fmt.Errorf("expectation %q is absent", id)
		}
		seen[id] = struct{}{}
		result[index] = expectation
	}
	return result, nil
}

func validateApprovedExpectations(descriptor Descriptor, expectations []scenarios.ModelExpectation) error {
	for _, expectation := range expectations {
		if !emptyObject(expectation.Predicate.Payload) {
			return fmt.Errorf("expectation %q has a nonempty predicate payload", expectation.ID)
		}
		switch expectation.ID {
		case "EXPECT-PULL-DIVERGENT-SEMANTIC-001":
			if expectation.Predicate.ContractPredicate != "state-equality" || expectation.Predicate.Name != "state-equals-authored-model" || !exactDivergentStateFacts(expectation.StateFacts) {
				return fmt.Errorf("expectation %q is not the approved pull state expectation", expectation.ID)
			}
		case "EXPECT-PULL-DIVERGENT-WIRE-001", "EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001", "EXPECT-PERF-STEADY-PULL-SEMANTIC-001":
			if expectation.Predicate.ContractPredicate != "wire-outcome" || expectation.Predicate.Name != "canonical-wire-outcome" || expectation.StateFacts != nil {
				return fmt.Errorf("expectation %q is not the approved wire expectation", expectation.ID)
			}
		default:
			return fmt.Errorf("expectation %q is not approved for mutant %q", expectation.ID, descriptor.Name)
		}
	}
	return nil
}

func resolveWireExpectations(scenario scenarios.Scenario, assertionID string) ([]scenarios.WireExpectation, error) {
	result := make([]scenarios.WireExpectation, 0)
	for _, expectation := range scenario.WireExpectations {
		if string(expectation.AssertionID) == assertionID {
			result = append(result, expectation)
		}
	}
	if len(result) == 0 {
		return nil, errors.New("wire expectation closure is empty")
	}
	return result, nil
}

func validateApprovedWireExpectations(descriptor Descriptor, actual []scenarios.WireExpectation) error {
	wanted := approvedWireExpectations[descriptor.Name]
	if len(wanted) == 0 {
		for _, expectation := range actual {
			if expectation.HTTPStatus != 200 || expectation.Retryable || expectation.ErrorCode != nil {
				return errors.New("wire expectation closure is not a successful exact HTTP closure")
			}
		}
		return nil
	}
	if len(actual) != len(wanted) {
		return fmt.Errorf("wire expectation closure has %d entries, want %d", len(actual), len(wanted))
	}
	for index, expectation := range actual {
		approved := wanted[index]
		if string(expectation.StepID) != approved.StepID || expectation.ContractCase != approved.ContractCase || expectation.HTTPStatus != approved.HTTPStatus || expectation.Retryable != approved.Retryable || expectation.ErrorCode != nil {
			return fmt.Errorf("wire expectation closure entry %d is not approved", index)
		}
	}
	return nil
}

func exactDivergentStateFacts(actual *scenarios.StateFacts) bool {
	if actual == nil || len(actual.Clients) != 1 {
		return false
	}
	client := actual.Clients[0]
	return client.UserID == "user-a" && client.ClientID == "client-a" &&
		uint64Value(client.RowCount) == 3 && uint64Value(client.ProvenanceCount) == 3 && uint64Value(client.CheckpointCount) == 2 &&
		reflect.DeepEqual(client.Provenance, []scenarios.ProvenanceFact{
			{TableID: "items-a", CanonicalWireJSON: `"row-a"`, Scopes: []string{"scope-a"}, Version: "a-v2"},
			{TableID: "items-b", CanonicalWireJSON: `"row-a"`, Scopes: []string{"scope-a"}, Version: "collision-v1"},
			{TableID: "items-a", CanonicalWireJSON: `"row-b"`, Scopes: []string{"scope-b"}, Version: "b-v1"},
		}) && reflect.DeepEqual(client.Checkpoints, []scenarios.CheckpointFact{
		{ScopeID: "scope-a", HasCursor: true, HasChecksum: true, Verified: true},
		{ScopeID: "scope-b", HasCursor: true, HasChecksum: true, Verified: true},
	})
}

func uint64Value(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func stateExpectationCoversPull(expectation scenarios.ModelExpectation, result reference.StepResult) bool {
	if result.Pull == nil || expectation.StateFacts == nil || len(expectation.StateFacts.Clients) != 1 {
		return false
	}
	provenance := expectation.StateFacts.Clients[0].Provenance
	for _, change := range result.Pull.Changes {
		covered := false
		for _, fact := range provenance {
			if fact.TableID == string(change.Row.TableID) && fact.CanonicalWireJSON == change.Row.CanonicalWireJSON && fact.Version == string(change.Version) && containsString(fact.Scopes, string(change.Scope)) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func assertionByID(assertions []scenarios.Assertion, assertionID string) (scenarios.Assertion, bool) {
	for _, assertion := range assertions {
		if string(assertion.ID) == assertionID {
			return assertion, true
		}
	}
	return scenarios.Assertion{}, false
}

func preferredPredicate(kind MutationKind) string {
	return "canonical-wire-outcome"
}

func hasOwnership(scenario scenarios.Scenario, requirementID, assertionID string) bool {
	for _, ownership := range scenario.Ownership {
		if ownership.ScenarioID == scenario.ID && string(ownership.RequirementID) == requirementID && string(ownership.AssertionID) == assertionID {
			return true
		}
	}
	return false
}

func containsRequirement(values []contract.RequirementID, wanted string) bool {
	for _, value := range values {
		if string(value) == wanted {
			return true
		}
	}
	return false
}
