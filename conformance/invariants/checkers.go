package invariants

// NotImplementedError reports that an invariant checker has no implementation.
type NotImplementedError struct{}

// Error returns the stable checker implementation error.
func (NotImplementedError) Error() string {
	return "invariant checker is not implemented"
}

// ErrNotImplemented prevents an unimplemented checker from reporting no violations.
var ErrNotImplemented = NotImplementedError{}

// CheckMutationConservation checks the accepted and rejected mutation partition.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:171-258.
func CheckMutationConservation([]Observation) ([]Violation, error) {
	return nil, ErrNotImplemented
}

// CheckCursorMonotonicity checks raw client cursors against ordered server positions.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:18-74.
func CheckCursorMonotonicity([]Observation) ([]Violation, error) {
	return nil, ErrNotImplemented
}

// CheckChecksumConvergence recomputes row and scope digests with vectors.RowDigest and vectors.ScopeDigest.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:261-313.
func CheckChecksumConvergence([]Observation) ([]Violation, error) {
	return nil, ErrNotImplemented
}

// CheckScopeIsolation checks selected scopes, membership edges, and cardinalities.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:316-347.
func CheckScopeIsolation([]Observation) ([]Violation, error) {
	return nil, ErrNotImplemented
}

// CheckNoStateForks checks process replacement, database identity, and durable state equality.
// It generalizes conformance/kotlin/platform.go:1412-1438 and conformance/kotlin/platform.go:1524-1531.
// It also generalizes conformance/reactnative/queue_replay.go:1002-1016.
func CheckNoStateForks([]Observation) ([]Violation, error) {
	return nil, ErrNotImplemented
}
