package invariants

import (
	"sort"
	"strings"
)

func orderedObservations(observations []Observation) []Observation {
	ordered := append([]Observation(nil), observations...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].Sequence < ordered[right].Sequence
	})
	return ordered
}

func orderedWireExchanges(exchanges []WireExchangeObservation) []WireExchangeObservation {
	ordered := append([]WireExchangeObservation(nil), exchanges...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].Sequence < ordered[right].Sequence
	})
	return ordered
}

func orderedViolations(violations []Violation) []Violation {
	ordered := append([]Violation(nil), violations...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return compareViolations(ordered[left], ordered[right]) < 0
	})
	return ordered
}

func compareViolations(left, right Violation) int {
	if left.ObservationSequence != right.ObservationSequence {
		if left.ObservationSequence < right.ObservationSequence {
			return -1
		}
		return 1
	}
	if left.Family != right.Family {
		return strings.Compare(string(left.Family), string(right.Family))
	}
	if left.RuleID != right.RuleID {
		return strings.Compare(string(left.RuleID), string(right.RuleID))
	}
	for index := 0; index < len(left.Evidence) && index < len(right.Evidence); index++ {
		if left.Evidence[index].Name != right.Evidence[index].Name {
			return strings.Compare(left.Evidence[index].Name, right.Evidence[index].Name)
		}
		if left.Evidence[index].Value != right.Evidence[index].Value {
			return strings.Compare(left.Evidence[index].Value, right.Evidence[index].Value)
		}
	}
	if len(left.Evidence) < len(right.Evidence) {
		return -1
	}
	if len(left.Evidence) > len(right.Evidence) {
		return 1
	}
	return 0
}

func sortedStringKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
