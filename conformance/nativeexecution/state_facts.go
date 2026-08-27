package nativeexecution

import "github.com/trainstar/synchro/conformance/scenarios"

func preserveStateFactProjections(source, target []scenarios.ModelExpectation) {
	byID := make(map[scenarios.ExpectationID]*scenarios.StateFacts, len(source))
	for _, expectation := range source {
		if expectation.StateFacts == nil {
			continue
		}
		facts := scenarios.CloneStateFacts(*expectation.StateFacts)
		byID[expectation.ID] = &facts
	}
	for index := range target {
		if facts, found := byID[target[index].ID]; found {
			copy := scenarios.CloneStateFacts(*facts)
			target[index].StateFacts = &copy
		}
	}
}
