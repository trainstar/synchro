//go:build reactnativeintegration

package reactnative

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
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
	// Each runner needs the initial state of a fresh cluster. An attached
	// database is reset before each runner, so the cluster must outlive each one.
	attached := os.Getenv("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL") != ""
	var environment blackbox.EnvironmentConfig
	if attached {
		var err error
		environment, err = blackbox.LoadLocalEnvironment()
		if err != nil {
			t.Fatalf("load React Native attached environment: %v", err)
		}
		if environment.AttachDestroyOnClose {
			t.Fatal("React Native corpus requires SYNCHRO_CONFORMANCE_ATTACH_DESTROY_ON_CLOSE=false")
		}
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
		scopeEmptyPullScenarioID:       runRealReactNativeScopeEmptyPull,
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
			if attached {
				resetContext, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				err := blackbox.ResetAttachedDatabase(resetContext, environment)
				cancel()
				if err != nil {
					t.Fatalf("reset attached database: %v", err)
				}
			}
			runners[id](t, platform)
		})
	}
}
