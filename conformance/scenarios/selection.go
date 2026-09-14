package scenarios

// IsModelCorpusScenario selects scenarios without direct client proof bindings.
// Consumer: scenarios/server/scenarios_test.go.
func IsModelCorpusScenario(scenario Scenario) bool {
	return len(scenario.NativeIdentityAliases) == 0
}
