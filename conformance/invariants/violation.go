package invariants

import "unicode/utf8"

const (
	// MaximumViolationEvidenceFields limits evidence cardinality for one violation.
	MaximumViolationEvidenceFields = 8
	// MaximumViolationEvidenceNameBytes limits one evidence field name.
	MaximumViolationEvidenceNameBytes = 64
	// MaximumViolationEvidenceValueBytes limits one evidence field value.
	MaximumViolationEvidenceValueBytes = 256
)

// InvariantFamily identifies one invariant checker family.
type InvariantFamily string

const (
	InvariantMutationConservation InvariantFamily = "mutation-conservation"
	InvariantCursorMonotonicity   InvariantFamily = "cursor-monotonicity"
	InvariantChecksumConvergence  InvariantFamily = "checksum-convergence"
	InvariantScopeIsolation       InvariantFamily = "scope-isolation"
	InvariantNoStateForks         InvariantFamily = "no-state-forks"
)

// RuleID is a stable machine-readable invariant rule identifier.
type RuleID string

// EvidenceField is one bounded diagnostic value.
// Checkers limit names, values, and field counts with the exported maximums.
type EvidenceField struct {
	Name  string
	Value string
}

// Violation identifies one failed invariant rule with bounded evidence.
type Violation struct {
	Family              InvariantFamily
	RuleID              RuleID
	ObservationSequence uint64
	Evidence            []EvidenceField
}

func boundedViolation(family InvariantFamily, ruleID RuleID, sequence uint64, evidence ...EvidenceField) Violation {
	if len(evidence) > MaximumViolationEvidenceFields {
		evidence = evidence[:MaximumViolationEvidenceFields]
	}
	bounded := make([]EvidenceField, len(evidence))
	for index, field := range evidence {
		bounded[index] = EvidenceField{
			Name:  boundedUTF8(field.Name, MaximumViolationEvidenceNameBytes),
			Value: boundedUTF8(field.Value, MaximumViolationEvidenceValueBytes),
		}
	}
	return Violation{
		Family:              family,
		RuleID:              ruleID,
		ObservationSequence: sequence,
		Evidence:            bounded,
	}
}

func boundedUTF8(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
