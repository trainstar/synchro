package mutants

import (
	"context"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const ConstantChecksumName = "constant-digest"

// ConstantChecksum replaces one terminal authoritative checksum with a fixed value.
type ConstantChecksum struct {
	subject Subject
	applied bool
}

// NewConstantChecksum decorates subject with the constant-digest defect.
func NewConstantChecksum(subject Subject) *ConstantChecksum {
	return &ConstantChecksum{subject: subject}
}

func (m *ConstantChecksum) Subject() Subject {
	if m == nil {
		return nil
	}
	return m.subject
}

func (m *ConstantChecksum) Descriptor() Descriptor {
	return Descriptor{
		Name:          ConstantChecksumName,
		Kind:          MutationKindConstantChecksum,
		ScenarioID:    ScenarioChecksum,
		RequirementID: RequirementChecksum,
		AssertionID:   AssertionChecksum,
		OperationKey:  "pull/request-page",
	}
}

func (m *ConstantChecksum) MutationApplied() bool {
	return m != nil && m.applied
}

func (m *ConstantChecksum) Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error) {
	result, err := m.subject.Execute(ctx, op)
	if err != nil || m.applied || operationKey(op) != "pull/request-page" || !successfulHTTP(result) || result.Pull == nil || result.Pull.HasMore || len(result.Pull.ScopeChecksums) == 0 || !result.Pull.ScopeChecksums[0].HasChecksum {
		return result, err
	}
	mutated := cloneStepResult(result)
	mutated.Pull.ScopeChecksums[0].Checksum = constantDigest
	m.applied = true
	return mutated, nil
}

var constantDigest reference.Checksum = func() reference.Checksum {
	var digest reference.Checksum
	for index := range digest {
		digest[index] = 0xa5
	}
	return digest
}()
