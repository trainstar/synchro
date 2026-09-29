//go:build reactnativeintegration

package reactnative

import (
	"context"
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
	runners := map[string]func(*testing.T, string){
		warmConnectScenarioID:          runRealReactNativeWarmConnect,
		steadyPullScenarioID:           runRealReactNativeSteadyPull,
		pendingCycleScenarioID:         runRealReactNativePendingCycle,
		queueReplayScenarioID:          runRealReactNativeQueueReplay,
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
	// The authored dataset flow is not a scenario document. Its expectations
	// are the hand-written dataset checkpoints.
	t.Run("dataset", func(t *testing.T) { runRealReactNativeDataset(t, platform) })
}

// newReactNativeScenarioHarness provisions or attaches the configured server
// and resets it to the authored fixture state, the same as the Swift and
// Kotlin scenario fixtures.
func newReactNativeScenarioHarness(t *testing.T, ctx context.Context) (*blackbox.Harness, *blackbox.NativeController) {
	t.Helper()
	environment, err := blackbox.LoadLocalEnvironment()
	if err != nil {
		t.Fatalf("load React Native conformance environment: %v", err)
	}
	provisionContext, cancelProvision := context.WithTimeout(ctx, 2*time.Minute)
	harness, err := blackbox.Provision(provisionContext, blackbox.HarnessConfig{Environment: environment})
	cancelProvision()
	if err != nil {
		t.Fatalf("provision React Native conformance harness: %v", err)
	}
	if deadline, ok := t.Deadline(); ok {
		disarm := harness.CloseBeforeDeadline(deadline)
		t.Cleanup(func() { disarm() })
	}
	// Under a desktop load average of 40 to 60, the WAL worker materialized a
	// pushed transaction later than the 30 s default wait.
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness, WaitTimeout: 90 * time.Second})
	if err != nil {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = harness.Close(closeContext)
		t.Fatalf("create React Native native controller: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close React Native native controller: %v", err)
		}
	})
	resetContext, cancelReset := context.WithTimeout(ctx, 5*time.Minute)
	err = harness.ResetScenarioServer(resetContext)
	cancelReset()
	if err != nil {
		t.Fatalf("reset React Native scenario server: %v", err)
	}
	return harness, controller
}
