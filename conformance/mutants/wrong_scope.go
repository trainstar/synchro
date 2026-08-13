package mutants

import (
	"context"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const WrongScopeName = "wrong-scope-row"

// WrongScope exposes one otherwise valid pull row under a different scope.
type WrongScope struct {
	subject Subject
	applied bool
}

// NewWrongScope decorates subject with the wrong-scope defect.
func NewWrongScope(subject Subject) *WrongScope {
	return &WrongScope{subject: subject}
}

func (m *WrongScope) Subject() Subject {
	if m == nil {
		return nil
	}
	return m.subject
}

func (m *WrongScope) Descriptor() Descriptor {
	return Descriptor{
		Name:          WrongScopeName,
		Kind:          MutationKindWrongScope,
		ScenarioID:    ScenarioWrongScope,
		RequirementID: RequirementWrongScope,
		AssertionID:   AssertionWrongScope,
		OperationKey:  "pull/request-page",
	}
}

func (m *WrongScope) MutationApplied() bool {
	return m != nil && m.applied
}

func (m *WrongScope) Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error) {
	result, err := m.subject.Execute(ctx, op)
	if err != nil || m.applied || operationKey(op) != "pull/request-page" || !successfulHTTP(result) || result.Pull == nil || len(result.Pull.Changes) == 0 {
		return result, err
	}
	mutated := cloneStepResult(result)
	wrong := reference.ScopeID("wrong-scope")
	if mutated.Pull.Changes[0].Scope == wrong {
		wrong = "wrong-scope-alt"
	}
	mutated.Pull.Changes[0].Scope = wrong
	m.applied = true
	return mutated, nil
}
