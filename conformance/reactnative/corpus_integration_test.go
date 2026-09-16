//go:build reactnativeintegration

package reactnative

import (
	"context"
	"os"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRealReactNativeCorpusIOS(t *testing.T) {
	runRealReactNativeCorpus(t, "ios", "SUP-RN-IOS-CURRENT-001")
}

func TestRealReactNativeCorpusAndroid(t *testing.T) {
	runRealReactNativeCorpus(t, "android", "SUP-RN-ANDROID-CURRENT-001")
}

func runRealReactNativeCorpus(t *testing.T, platform, cell string) {
	t.Helper()
	// Each existing runner owns a fresh cluster. Sharing an attached database
	// would retain schema changes from an earlier scenario.
	if os.Getenv("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL") != "" {
		t.Fatal("React Native corpus requires isolated local PostgreSQL instances, not an attached database")
	}
	runners := map[string]func(*testing.T, string){
		warmConnectScenarioID:          runRealReactNativeWarmConnect,
		steadyPullScenarioID:           runRealReactNativeSteadyPull,
		pendingCycleScenarioID:         runRealReactNativePendingCycle,
		queueReplayScenarioID:          runRealReactNativeQueueReplay,
		rebuildApplyScenarioID:         runRealReactNativeRebuildApply,
		rebuildCardinalityScenarioID:   runRealReactNativeRebuildCardinality,
		rebuildRequestsScenarioID:      runRealReactNativeRebuildRequests,
		seededEmptyStartupScenarioID:   runRealReactNativeSeededEmptyStartup,
		multiScopeProvenanceScenarioID: runRealReactNativeMultiScopeProvenance,
		schemaCheckScenarioID:          runRealReactNativeSchemaCheck,
		pushResponseLossScenarioID:     runRealReactNativePushResponseLoss,
		forgedCursorScenarioID:         runRealReactNativeForgedCursor,
		retentionReconnectScenarioID:   runRealReactNativeRetentionReconnect,
		schemaQueuedMutationScenarioID: runRealReactNativeSchemaQueuedMutation,
	}
	authored, err := scenarios.LoadAll(context.Background(), "../..")
	if err != nil {
		t.Fatalf("load React Native corpus: %v", err)
	}
	selected, err := corpusScenarioIDs(authored, cell, runners)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range selected {
		t.Run(id, func(t *testing.T) {
			runners[id](t, platform)
		})
	}
}
