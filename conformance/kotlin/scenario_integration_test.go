//go:build kotlinintegration

package kotlin

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRealKotlinPerformance(t *testing.T) {
	// Each scenario runs in its own subtest, so its fixture cleanup closes
	// the harness and releases the shared installation lock before the next
	// scenario provisions. One shared subtest deadlocks on that lock.
	t.Run("assertion", func(t *testing.T) {
		t.Run("steady-pull", runKotlinSteadyPull)
		t.Run("pending-cycle", runKotlinPendingCycle)
		t.Run("queue-replay", runKotlinQueueReplay)
		t.Run("rebuild-requests", runKotlinRebuildRequests)
		t.Run("rebuild-apply", runKotlinRebuildApply)
		t.Run("rebuild-cardinality", runKotlinRebuildCardinality)
		t.Run("forged-cursor", runKotlinForgedCursor)
		t.Run("push-response-loss", runKotlinPushResponseLoss)
		t.Run("retention-reconnect", runKotlinRetentionReconnect)
		t.Run("schema-queued-mutation", runKotlinSchemaQueuedMutation)
		t.Run("schema-check", runKotlinSchemaCheck)
		// A scenario that holds a seed artifact runs last. Its artifact closes
		// after the subtest returns, so it cannot reset the server for a
		// successor.
		t.Run("seeded-empty-startup", runKotlinSeededEmptyStartup)
		t.Run("multi-scope-provenance", runKotlinMultiScopeProvenance)
	})
}

func runKotlinSteadyPull(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/performance/steady-pull-001.json", 0)
	result, err := RunSteadyPullScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "steady-pull-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android steady-pull scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android steady-pull identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinPendingCycle(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/performance/pending-cycle-001.json", 0)
	if _, err := RunPendingCycleScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "pending-cycle-client-a"}); err != nil {
		t.Fatalf("run direct Kotlin Android pending-cycle scenario: %v", err)
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinQueueReplay(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/performance/queue-replay-001.json", 0)
	result, err := RunQueueReplayScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "queue-replay-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android queue-replay scenario: %v", err)
	}
	if len(result.ReplayCalls) != 9 {
		t.Fatalf("Kotlin Android queue-replay replay calls = %d, want 9", len(result.ReplayCalls))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinSeededEmptyStartup(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/performance/seeded-empty-startup-001.json", 0)
	root := filepath.Join("..", "..")
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedToolPath == "" {
		seedToolPath = filepath.Join(root, "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make Kotlin Android seed staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{Harness: harness, SeedToolPath: seedToolPath, StagingDirectory: stagingDirectory})
	if err != nil {
		t.Fatalf("create Kotlin Android seed artifact: %v", err)
	}
	t.Cleanup(func() {
		if err := artifact.Close(context.Background()); err != nil {
			t.Errorf("close Kotlin Android seed artifact: %v", err)
		}
	})
	result, err := RunSeededEmptyStartupScenario(ctx, scenario, controller, artifact, platform)
	if err != nil {
		t.Fatalf("run direct Kotlin Android seeded-empty-startup scenario: %v", err)
	}
	if len(result.Clients) != 6 {
		t.Fatalf("Kotlin Android seeded-empty-startup clients = %d, want 6", len(result.Clients))
	}
}

func runKotlinMultiScopeProvenance(t *testing.T) {
	t.Helper()
	const scenarioPath = "conformance/scenarios/performance/multi-scope-provenance-001.json"
	pullPageSize := kotlinMultiScopeProvenancePageSize(t, scenarioPath)
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, scenarioPath, pullPageSize)
	root := filepath.Join("..", "..")
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedToolPath == "" {
		seedToolPath = filepath.Join(root, "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make Kotlin Android multi-scope provenance staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{Harness: harness, SeedToolPath: seedToolPath, StagingDirectory: stagingDirectory})
	if err != nil {
		t.Fatalf("create Kotlin Android multi-scope provenance artifact: %v", err)
	}
	t.Cleanup(func() {
		if err := artifact.Close(context.Background()); err != nil {
			t.Errorf("close Kotlin Android multi-scope provenance artifact: %v", err)
		}
	})
	result, err := RunMultiScopeProvenanceScenario(ctx, scenario, controller, artifact, platform)
	if err != nil {
		t.Fatalf("run direct Kotlin Android multi-scope-provenance scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android multi-scope-provenance identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func kotlinMultiScopeProvenancePageSize(t *testing.T, scenarioPath string) int {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve Kotlin Android multi-scope provenance repository root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, scenarioPath)
	if err != nil {
		t.Fatalf("load Kotlin Android multi-scope provenance authored scenario: %v", err)
	}
	pageSize, err := multiScopeProvenancePullPageSize(scenario)
	if err != nil {
		t.Fatalf("read Kotlin Android multi-scope provenance authored pull page size: %v", err)
	}
	return pageSize
}

func runKotlinPushResponseLoss(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/server/push-response-loss-001.json", 0)
	result, err := RunPushResponseLossScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android push-response-loss scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android push-response-loss identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinRetentionReconnect(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/server/retention-reconnect-001.json", 1)
	result, err := RunRetentionReconnectScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "retention-reconnect-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android retention-reconnect scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android retention-reconnect identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinSchemaQueuedMutation(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/server/schema-queued-mutation-001.json", 100)
	result, err := RunSchemaQueuedMutationScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "schema-queued-mutation-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android schema-queued-mutation scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android schema-queued-mutation identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinSchemaCheck(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/performance/schema-check-001.json", 0)
	result, err := RunSchemaCheckScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Kotlin Android schema-check scenario: %v", err)
	}
	// The consumer binds one public call to each authored wire expectation, so a
	// short call list means the run skipped an authored schema transition.
	if len(result.Calls) != len(scenario.WireExpectations) {
		t.Fatalf("Kotlin Android schema-check calls = %d, want %d", len(result.Calls), len(scenario.WireExpectations))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinRebuildRequests(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/performance/rebuild-requests-001.json", 1)
	result, err := RunRebuildRequestsScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "rebuild-requests-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android rebuild-requests scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android rebuild-requests identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinRebuildApply(t *testing.T) {
	t.Helper()
	const scenarioPath = "conformance/scenarios/performance/rebuild-apply-001.json"
	pullPageSize := kotlinRebuildApplyPageSize(t, scenarioPath)
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, scenarioPath, pullPageSize)
	result, err := RunRebuildApplyScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Kotlin Android rebuild-apply scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android rebuild-apply identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func kotlinRebuildApplyPageSize(t *testing.T, scenarioPath string) int {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve Kotlin Android rebuild-apply repository root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, scenarioPath)
	if err != nil {
		t.Fatalf("load Kotlin Android rebuild-apply authored scenario: %v", err)
	}
	steps, err := kotlinScenarioStepMap(scenario, rebuildApplyScenarioID, 9)
	if err != nil {
		t.Fatalf("validate Kotlin Android rebuild-apply authored scenario: %v", err)
	}
	_, _, pageSize, err := rebuildApplyBindings(scenario, steps)
	if err != nil || pageSize > 1000 {
		t.Fatalf("read Kotlin Android rebuild-apply authored pull page size: %v", err)
	}
	return int(pageSize)
}

func runKotlinRebuildCardinality(t *testing.T) {
	t.Helper()
	const scenarioPath = "conformance/scenarios/performance/rebuild-cardinality-001.json"
	pullPageSize := kotlinRebuildCardinalityPageSize(t, scenarioPath)
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, scenarioPath, pullPageSize)
	result, err := RunRebuildCardinalityScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Kotlin Android rebuild-cardinality scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android rebuild-cardinality identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func kotlinRebuildCardinalityPageSize(t *testing.T, scenarioPath string) int {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve Kotlin Android rebuild-cardinality repository root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, scenarioPath)
	if err != nil {
		t.Fatalf("load Kotlin Android rebuild-cardinality authored scenario: %v", err)
	}
	steps, err := kotlinScenarioStepMap(scenario, rebuildCardinalityScenarioID, 9)
	if err != nil {
		t.Fatalf("validate Kotlin Android rebuild-cardinality authored scenario: %v", err)
	}
	_, _, pageSize, err := rebuildCardinalityBindings(scenario, steps)
	if err != nil {
		t.Fatalf("read Kotlin Android rebuild-cardinality authored pull page size: %v", err)
	}
	if pageSize > uint64(^uint(0)>>1) {
		t.Fatal("Kotlin Android rebuild-cardinality authored pull page size exceeds int")
	}
	return int(pageSize)
}

func runKotlinForgedCursor(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "conformance/scenarios/server/rebuild-forged-cursor-001.json", 1)
	root := filepath.Join("..", "..")
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedToolPath == "" {
		seedToolPath = filepath.Join(root, "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make Kotlin Android forged-cursor seed staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{Harness: harness, SeedToolPath: seedToolPath, StagingDirectory: stagingDirectory})
	if err != nil {
		t.Fatalf("create Kotlin Android forged-cursor seed artifact: %v", err)
	}
	t.Cleanup(func() {
		if err := artifact.Close(context.Background()); err != nil {
			t.Errorf("close Kotlin Android forged-cursor seed artifact: %v", err)
		}
	})
	result, err := RunForgedCursorScenario(ctx, scenario, controller, artifact, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "forged-cursor-client-a"})
	if err != nil {
		t.Fatalf("run direct Kotlin Android forged-cursor scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Kotlin Android forged-cursor identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func resetKotlinPerformanceServer(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	// A capture dependency registration stays pending while its source table
	// holds rows, so the replayed registrations never activate unless every
	// diagnostic source a scenario writes is empty first.
	for _, table := range blackbox.DiagnosticSourceTables() {
		if err := harness.Source().ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clear Kotlin Android performance source table %s: %v", table, err)
		}
	}
	reinstall, err := harness.ReinstallExtension(ctx)
	if err != nil {
		t.Fatalf("reset Kotlin Android performance extension state: %v", err)
	}
	minimumGeneration := int64(0)
	for phase := 0; phase < 2; phase++ {
		deadline := time.Now().Add(90 * time.Second)
		var ready blackbox.ExtensionReinstallObservation
		readyObserved := false
		for time.Now().Before(deadline) {
			ready, err = harness.Operator().ObserveExtensionReinstall(ctx, reinstall.ReinstallLSN)
			if err == nil && ready.WorkerPID > 0 && ready.WorkerPID != reinstall.PriorWorkerPID && ready.ActiveSlotName == harness.Names().ReplicationSlot && ready.RestartLSN != "" && ready.SlotActive && ready.RestartLSNAtOrAfterReinstall && ready.ActiveRegistryGeneration > minimumGeneration && ready.PendingRegistryGenerationCount == 0 && ready.NoValidationFailurePoison {
				readyObserved = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !readyObserved {
			// The loop exits on an unmet condition, not only on an error, so name
			// every condition. Reporting err alone prints a nil error.
			t.Fatalf("wait for Kotlin Android performance extension reset phase %d: err %v worker %d prior %d slot %q want %q restartLSN %q slotActive %v restartAtOrAfter %v activeGeneration %d minimum %d pendingGenerations %d noPoison %v",
				phase, err, ready.WorkerPID, reinstall.PriorWorkerPID,
				ready.ActiveSlotName, harness.Names().ReplicationSlot,
				ready.RestartLSN, ready.SlotActive, ready.RestartLSNAtOrAfterReinstall,
				ready.ActiveRegistryGeneration, minimumGeneration,
				ready.PendingRegistryGenerationCount, ready.NoValidationFailurePoison)
		}
		if phase == 0 {
			minimumGeneration = ready.ActiveRegistryGeneration
			// A scenario can transition any diagnostic source-table column. The
			// reinstall has cleared every registry generation, so this is the only
			// point where restoring authored column shapes invalidates no registration.
			if err := harness.Operator().RestoreDiagnosticSourceTableShapes(ctx); err != nil {
				t.Fatalf("restore Kotlin Android performance source table shapes: %v", err)
			}
			if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
				t.Fatalf("restore Kotlin Android performance source registrations: %v", err)
			}
		}
	}
}

func newKotlinPerformanceFixture(t *testing.T, scenarioPath string, pullPageSize int) (context.Context, scenarios.Scenario, *blackbox.Harness, *blackbox.NativeController, *Platform) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatal("TestRealKotlinPerformance requires --provision --install")
	}
	adbPath := os.Getenv("SYNCHRO_KOTLIN_ADB")
	deviceSerial := os.Getenv("SYNCHRO_KOTLIN_DEVICE_SERIAL")
	applicationAPK := os.Getenv("SYNCHRO_KOTLIN_APPLICATION_APK")
	instrumentationAPK := os.Getenv("SYNCHRO_KOTLIN_INSTRUMENTATION_APK")
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if adbPath == "" || deviceSerial == "" || applicationAPK == "" || instrumentationAPK == "" || seedToolPath == "" {
		t.Fatal("Kotlin Android performance environment is incomplete")
	}
	environment, err := blackbox.LoadLocalEnvironment()
	if err != nil {
		t.Fatalf("load Kotlin Android conformance environment: %v", err)
	}
	provisionContext, cancelProvision := context.WithTimeout(context.Background(), 2*time.Minute)
	harness, err := blackbox.Provision(provisionContext, blackbox.HarnessConfig{Environment: environment})
	cancelProvision()
	if err != nil {
		t.Fatalf("provision Kotlin Android conformance harness: %v", err)
	}
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = harness.Close(closeContext)
		t.Fatalf("create Kotlin Android native controller: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close Kotlin Android native controller: %v", err)
		}
	})
	platform, err := NewPlatform(Config{
		ADBPath: adbPath, DeviceSerial: deviceSerial, ApplicationAPKPath: applicationAPK, InstrumentationAPKPath: instrumentationAPK,
		ApplicationID: "com.trainstar.synchro.conformance", InstrumentationComponent: "com.trainstar.synchro.conformance.test/androidx.test.runner.AndroidJUnitRunner",
		ServerURL: harness.AdapterURL(), AuthToken: func(tokenContext context.Context, client Client) (string, error) {
			return harness.NativeBearerToken(tokenContext, client.UserID, time.Now())
		},
		Platform: "android", AppVersion: "0.3.0", PullPageSize: pullPageSize, PushBatchSize: 1000,
	})
	if err != nil {
		t.Fatalf("create Kotlin Android direct platform: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := platform.Close(closeContext); err != nil {
			t.Errorf("close Kotlin Android direct platform: %v", err)
		}
	})
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	t.Cleanup(cancel)
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, scenarioPath)
	if err != nil {
		t.Fatalf("load Kotlin Android performance scenario %s: %v", scenarioPath, err)
	}
	return ctx, scenario, harness, controller, platform
}
