package mutants

import (
	"context"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const OmitMutationName = "omit-mutation-outcome"

// OmitMutation removes exactly one otherwise valid push mutation outcome.
type OmitMutation struct {
	subject Subject
	applied bool
}

// NewOmitMutation decorates subject with the omitted-outcome defect.
func NewOmitMutation(subject Subject) *OmitMutation {
	return &OmitMutation{subject: subject}
}

func (m *OmitMutation) Subject() Subject {
	if m == nil {
		return nil
	}
	return m.subject
}

func (m *OmitMutation) Descriptor() Descriptor {
	return Descriptor{
		Name:          OmitMutationName,
		Kind:          MutationKindOmitMutation,
		ScenarioID:    ScenarioMutationOutcome,
		RequirementID: RequirementMutationOutcome,
		AssertionID:   AssertionMutationOutcome,
		OperationKey:  "push/submit",
	}
}

func (m *OmitMutation) MutationApplied() bool {
	return m != nil && m.applied
}

func (m *OmitMutation) Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error) {
	result, err := m.subject.Execute(ctx, op)
	if err != nil || m.applied || operationKey(op) != "push/submit" || !successfulHTTP(result) || result.Push == nil || len(result.Push.Mutations) == 0 {
		return result, err
	}
	mutated := cloneStepResult(result)
	mutated.Push.Mutations = append([]reference.MutationObservation(nil), mutated.Push.Mutations[:len(mutated.Push.Mutations)-1]...)
	m.applied = true
	return mutated, nil
}
