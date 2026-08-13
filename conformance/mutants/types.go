// Package mutants verifies that the conformance harness detects semantic
// defects in typed reference-model observations.
package mutants

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// Subject is the typed operation surface used by a mutant.
type Subject interface {
	Execute(context.Context, scenarios.Operation) (reference.StepResult, error)
}

// Mutant is one deterministic decorator over a typed subject.
type Mutant interface {
	Subject() Subject
	Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error)
}

// MutationKind identifies the one semantic change made by a mutant.
type MutationKind string

const (
	MutationKindBase              MutationKind = "base"
	MutationKindOmitMutation      MutationKind = "omit_mutation_outcome"
	MutationKindConstantChecksum  MutationKind = "constant_digest"
	MutationKindDuplicateDelivery MutationKind = "duplicate_delivered_effect"
	MutationKindWrongScope        MutationKind = "wrong_scope_row"
)

// Stable requirement IDs used by the four self-mutants.
const (
	RequirementMutationOutcome = "SYNC-MUTATION-002"
	RequirementChecksum        = "SYNC-INTEGRITY-002"
	RequirementDuplicate       = "SYNC-PULL-001"
	RequirementWrongScope      = "SYNC-PULL-001"
)

// Stable scenario and assertion IDs bind each self-mutant to one authored proof.
const (
	ScenarioMutationOutcome = "SCN-PERF-PENDING-CYCLE-001"
	ScenarioChecksum        = "SCN-PERF-STEADY-PULL-001"
	ScenarioDuplicate       = "SCN-PULL-DIVERGENT-CHECKPOINTS-001"
	ScenarioWrongScope      = "SCN-PULL-DIVERGENT-CHECKPOINTS-001"

	AssertionMutationOutcome = "ASSERT-PERF-PENDING-CYCLE-SEMANTIC-001"
	AssertionChecksum        = "ASSERT-PERF-STEADY-PULL-SEMANTIC-001"
	AssertionDuplicate       = "ASSERT-PULL-DIVERGENT-SEMANTIC-001"
	AssertionWrongScope      = "ASSERT-PULL-DIVERGENT-SEMANTIC-001"
)

// Descriptor identifies the requirement-owned assertion for one mutant.
type Descriptor struct {
	Name          string
	Kind          MutationKind
	ScenarioID    string
	RequirementID string
	AssertionID   string
	OperationKey  string
}

var approvedDescriptors = map[string]Descriptor{
	OmitMutationName: {
		Name: OmitMutationName, Kind: MutationKindOmitMutation, ScenarioID: ScenarioMutationOutcome,
		RequirementID: RequirementMutationOutcome, AssertionID: AssertionMutationOutcome, OperationKey: "push/submit",
	},
	ConstantChecksumName: {
		Name: ConstantChecksumName, Kind: MutationKindConstantChecksum, ScenarioID: ScenarioChecksum,
		RequirementID: RequirementChecksum, AssertionID: AssertionChecksum, OperationKey: "pull/request-page",
	},
	DuplicateDeliveryName: {
		Name: DuplicateDeliveryName, Kind: MutationKindDuplicateDelivery, ScenarioID: ScenarioDuplicate,
		RequirementID: RequirementDuplicate, AssertionID: AssertionDuplicate, OperationKey: "pull/request-page",
	},
	WrongScopeName: {
		Name: WrongScopeName, Kind: MutationKindWrongScope, ScenarioID: ScenarioWrongScope,
		RequirementID: RequirementWrongScope, AssertionID: AssertionWrongScope, OperationKey: "pull/request-page",
	},
}

// FailureKind identifies why a run did not produce a semantic detection.
type FailureKind string

const (
	FailureNone          FailureKind = ""
	FailureContract      FailureKind = "contract"
	FailureExecution     FailureKind = "execution"
	FailureCrash         FailureKind = "crash"
	FailureMalformed     FailureKind = "malformed"
	FailureFieldPresence FailureKind = "field_presence"
	FailureUnrelated     FailureKind = "unrelated"
	FailureSurvived      FailureKind = "survived"
	FailureSemantic      FailureKind = "semantic"
)

// Failure records a fail-closed run disposition.
type Failure struct {
	Kind          FailureKind
	RequirementID string
	AssertionID   string
	Reason        string
}

// Execution records one closed operation and its expected and observed result.
type Execution struct {
	StepID       scenarios.StepID
	Operation    scenarios.Operation
	OperationKey string
	Expected     reference.StepResult
	Observed     reference.StepResult
	Err          error
}

// Result records one complete base or mutant execution.
type Result struct {
	ScenarioID    string
	Mutant        string
	Descriptor    Descriptor
	Executions    []Execution
	Passed        bool
	Detected      bool
	RequirementID string
	AssertionID   string
	Failure       Failure
}

type descriptorProvider interface {
	Descriptor() Descriptor
}

type mutationState interface {
	MutationApplied() bool
}

type stepAwareSubject interface {
	SetStepID(string)
}

type rawResultSubject interface {
	RawResult(string) (reference.StepResult, bool)
}

func descriptorOf(mutant Mutant) (Descriptor, bool) {
	provider, ok := mutant.(descriptorProvider)
	if !ok {
		return Descriptor{}, false
	}
	descriptor := provider.Descriptor()
	if descriptor.Name == "" || descriptor.Kind == MutationKindBase || descriptor.ScenarioID == "" || descriptor.RequirementID == "" || descriptor.AssertionID == "" || descriptor.OperationKey == "" {
		return Descriptor{}, false
	}
	return descriptor, true
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func cloneOperation(operation scenarios.Operation) scenarios.Operation {
	operation.Payload = append(json.RawMessage(nil), operation.Payload...)
	return operation
}

func cloneStepResult(source reference.StepResult) reference.StepResult {
	result := source
	if source.HTTP != nil {
		value := *source.HTTP
		value.Body = append([]byte(nil), source.HTTP.Body...)
		result.HTTP = &value
	}
	if source.Connect != nil {
		value := *source.Connect
		value.AddedScopes = append([]reference.ScopeID(nil), source.Connect.AddedScopes...)
		value.RemovedScopes = append([]reference.ScopeID(nil), source.Connect.RemovedScopes...)
		value.ScopeCursors = append([]reference.ScopeCursorObservation(nil), source.Connect.ScopeCursors...)
		result.Connect = &value
	}
	if source.Local != nil {
		value := *source.Local
		result.Local = &value
	}
	if source.Lifecycle != nil {
		value := *source.Lifecycle
		result.Lifecycle = &value
	}
	if source.Push != nil {
		value := *source.Push
		value.Mutations = append([]reference.MutationObservation(nil), source.Push.Mutations...)
		result.Push = &value
	}
	if source.Pull != nil {
		value := *source.Pull
		value.Changes = append([]reference.PullChangeObservation(nil), source.Pull.Changes...)
		value.ScopeCursors = append([]reference.ScopeCursorObservation(nil), source.Pull.ScopeCursors...)
		value.AddedScopes = append([]reference.ScopeID(nil), source.Pull.AddedScopes...)
		value.RemovedScopes = append([]reference.ScopeID(nil), source.Pull.RemovedScopes...)
		value.RebuildScopes = append([]reference.ScopeID(nil), source.Pull.RebuildScopes...)
		value.ScopeChecksums = append([]reference.ScopeChecksumObservation(nil), source.Pull.ScopeChecksums...)
		result.Pull = &value
	}
	if source.Rebuild != nil {
		value := *source.Rebuild
		value.Records = append([]reference.RebuildRecordObservation(nil), source.Rebuild.Records...)
		result.Rebuild = &value
	}
	if source.WAL != nil {
		value := *source.WAL
		value.AffectedScopes = append([]reference.ScopeID(nil), source.WAL.AffectedScopes...)
		result.WAL = &value
	}
	if source.Schema != nil {
		value := *source.Schema
		value.AffectedScopes = append([]reference.ScopeID(nil), source.Schema.AffectedScopes...)
		result.Schema = &value
	}
	if source.Retention != nil {
		value := *source.Retention
		result.Retention = &value
	}
	if source.Client != nil {
		value := *source.Client
		result.Client = &value
	}
	return result
}

func operationKey(operation scenarios.Operation) string {
	return scenarios.OperationKey(operation)
}

func requireSubject(mutant Mutant) (Subject, error) {
	if isNilInterface(mutant) {
		return nil, errors.New("mutant is required")
	}
	subject := mutant.Subject()
	if isNilInterface(subject) {
		return nil, errors.New("mutant subject is required")
	}
	return subject, nil
}

func invalidDescriptor(descriptor Descriptor) error {
	return fmt.Errorf("mutant descriptor %q is incomplete", descriptor.Name)
}

func validateApprovedDescriptor(descriptor Descriptor) error {
	approved, found := approvedDescriptors[descriptor.Name]
	if found && descriptor != approved {
		return fmt.Errorf("mutant descriptor %q is not an approved exact binding", descriptor.Name)
	}
	return nil
}
