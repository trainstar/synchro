package mutants

import (
	"context"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const DuplicateDeliveryName = "duplicate-delivery"

// DuplicateDelivery delivers one pull effect twice.
type DuplicateDelivery struct {
	subject Subject
	applied bool
}

// NewDuplicateDelivery decorates subject with the duplicate-effect defect.
func NewDuplicateDelivery(subject Subject) *DuplicateDelivery {
	return &DuplicateDelivery{subject: subject}
}

func (m *DuplicateDelivery) Subject() Subject {
	if m == nil {
		return nil
	}
	return m.subject
}

func (m *DuplicateDelivery) Descriptor() Descriptor {
	return Descriptor{
		Name:          DuplicateDeliveryName,
		Kind:          MutationKindDuplicateDelivery,
		ScenarioID:    ScenarioDuplicate,
		RequirementID: RequirementDuplicate,
		AssertionID:   AssertionDuplicate,
		OperationKey:  "pull/request-page",
	}
}

func (m *DuplicateDelivery) MutationApplied() bool {
	return m != nil && m.applied
}

func (m *DuplicateDelivery) Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error) {
	result, err := m.subject.Execute(ctx, op)
	if err != nil || m.applied || operationKey(op) != "pull/request-page" || !successfulHTTP(result) || result.Pull == nil || len(result.Pull.Changes) == 0 {
		return result, err
	}
	mutated := cloneStepResult(result)
	change := mutated.Pull.Changes[0]
	mutated.Pull.Changes = append(mutated.Pull.Changes, reference.PullChangeObservation{})
	copy(mutated.Pull.Changes[1:], mutated.Pull.Changes[:len(mutated.Pull.Changes)-1])
	mutated.Pull.Changes[0] = change
	m.applied = true
	return mutated, nil
}
