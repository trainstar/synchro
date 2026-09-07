package invariants

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func assertNoViolations(t *testing.T, violations []Violation, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("checker error = %v", err)
	}
	assertViolationBounds(t, violations)
	if len(violations) != 0 {
		t.Fatalf("violations = %+v, want none", violations)
	}
}

func assertCaughtRule(t *testing.T, violations []Violation, err error, ruleID RuleID) {
	t.Helper()
	if err != nil {
		t.Fatalf("checker error = %v", err)
	}
	assertViolationBounds(t, violations)
	if len(violations) != 1 || violations[0].RuleID != ruleID {
		t.Fatalf("violations = %+v, want only rule %q", violations, ruleID)
	}
}

func assertViolationBounds(t *testing.T, violations []Violation) {
	t.Helper()
	for _, violation := range violations {
		if violation.Family == "" || violation.RuleID == "" {
			t.Fatalf("violation has no family or rule: %+v", violation)
		}
		if len(violation.Evidence) > MaximumViolationEvidenceFields {
			t.Fatalf("evidence field count = %d", len(violation.Evidence))
		}
		for _, field := range violation.Evidence {
			if len(field.Name) > MaximumViolationEvidenceNameBytes {
				t.Fatalf("evidence name length = %d", len(field.Name))
			}
			if len(field.Value) > MaximumViolationEvidenceValueBytes {
				t.Fatalf("evidence value length = %d", len(field.Value))
			}
			if !utf8.ValidString(field.Name) || !utf8.ValidString(field.Value) {
				t.Fatalf("evidence is not valid UTF-8: %+v", field)
			}
		}
	}
}

func TestBoundedViolationEnforcesExportedEvidenceLimits(t *testing.T) {
	evidence := make([]EvidenceField, MaximumViolationEvidenceFields+2)
	for index := range evidence {
		evidence[index] = EvidenceField{
			Name:  strings.Repeat("n", MaximumViolationEvidenceNameBytes+1),
			Value: strings.Repeat("界", MaximumViolationEvidenceValueBytes),
		}
	}
	violation := boundedViolation(InvariantMutationConservation, RuleMutationWireShapeInvalid, 1, evidence...)
	assertViolationBounds(t, []Violation{violation})
	if len(violation.Evidence) != MaximumViolationEvidenceFields {
		t.Fatalf("evidence field count = %d, want %d", len(violation.Evidence), MaximumViolationEvidenceFields)
	}
}

func TestOrderedViolationsUsesSequenceFamilyRuleAndEvidence(t *testing.T) {
	a := boundedViolation(
		InvariantMutationConservation, RuleMutationOutcomeOrder, 1,
		EvidenceField{Name: "scope_id", Value: "scope-a"},
	)
	b := boundedViolation(
		InvariantMutationConservation, RuleMutationOutcomeOrder, 1,
		EvidenceField{Name: "scope_id", Value: "scope-b"},
	)
	scope := boundedViolation(InvariantScopeIsolation, RuleScopeUnexpectedChange, 1)
	later := boundedViolation(InvariantMutationConservation, RuleMutationWireShapeInvalid, 2)
	want := []Violation{a, b, scope, later}
	got := orderedViolations([]Violation{later, scope, b, a})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered violations = %+v, want %+v", got, want)
	}
}
