package invariants

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
