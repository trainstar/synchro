package scenarios

// IsModelCorpusScenario selects scenarios whose native proof uses the legacy
// form-A plan or no native plan. Consumer: scenarios/server/scenarios_test.go.
func IsModelCorpusScenario(scenario Scenario) bool {
	return len(scenario.NativeIdentityAliases) == 0
}

// IsNativeDerivationScenario selects form-B scenarios with bound native steps
// and at least one selectable native-e2e support-cell obligation. Consumer:
// nativeexecution/derive_test.go.
func IsNativeDerivationScenario(scenario Scenario) bool {
	if len(scenario.NativeIdentityAliases) == 0 {
		return false
	}
	hasBoundStep := false
	for _, step := range scenario.Steps {
		if step.NativeBinding != nil {
			hasBoundStep = true
			break
		}
	}
	if !hasBoundStep {
		return false
	}
	for _, obligation := range scenario.ProofObligations {
		if obligation.ProofType == "native-e2e" && obligation.SupportCellID != nil {
			return true
		}
	}
	return false
}
