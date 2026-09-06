//go:build swiftintegration

package swift

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRealSwiftPerformance(t *testing.T) {
	// Each scenario runs as its own subtest. A shared subtest stops at the first
	// failure, which hides every scenario after it and makes the run order a
	// hidden dependency. A scenario that fails before its reset leaves server
	// state behind, so confirm a later failure with a targeted rerun.
	for _, scenario := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"steady-pull", runSwiftSteadyPull},
		{"rebuild-apply", runSwiftRebuildApply},
		{"rebuild-cardinality", runSwiftRebuildCardinality},
		{"push-response-loss", runSwiftPushResponseLoss},
		{"retention-reconnect", runSwiftRetentionReconnect},
		{"rebuild-requests", runSwiftRebuildRequests},
		{"forged-cursor", runSwiftForgedCursor},
		{"pending-cycle", runSwiftPendingCycle},
		{"queue-replay", runSwiftQueueReplay},
		{"seeded-empty-startup", runSwiftSeededEmptyStartup},
		{"multi-scope-provenance", runSwiftMultiScopeProvenance},
		{"schema-queued-mutation", runSwiftSchemaQueuedMutation},
		{"schema-check", runSwiftSchemaCheck},
	} {
		t.Run(scenario.name, func(t *testing.T) { scenario.run(t) })
	}
}

func runSwiftRetentionReconnect(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "retention-reconnect-001.json"), 1)
	result, err := RunRetentionReconnectScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "retention-reconnect-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift retention-reconnect scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift retention-reconnect identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftPushResponseLoss(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "push-response-loss-001.json"), 0)
	result, err := RunPushResponseLossScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift push-response-loss scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift push-response-loss identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftSchemaQueuedMutation(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "schema-queued-mutation-001.json"), 100)
	result, err := RunSchemaQueuedMutationScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "schema-queued-mutation-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift schema-queued-mutation scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift schema-queued-mutation identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftMultiScopeProvenance(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "multi-scope-provenance-001.json"), 100)
	artifact := newSwiftPerformanceArtifact(t, harness)
	result, err := RunMultiScopeProvenanceScenario(ctx, scenario, controller, artifact, platform)
	if err != nil {
		t.Fatalf("run direct Swift multi-scope-provenance scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift multi-scope-provenance identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftSchemaCheck(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "schema-check-001.json"), 0)
	result, err := RunSchemaCheckScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Swift schema-check scenario: %v", err)
	}
	// The consumer binds one public call to each authored wire expectation, so a
	// short call list means the run skipped an authored schema transition.
	if len(result.Calls) != len(scenario.WireExpectations) {
		t.Fatalf("Swift schema-check calls = %d, want %d", len(result.Calls), len(scenario.WireExpectations))
	}
}

func runSwiftSteadyPull(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "steady-pull-001.json"), 0)
	result, err := RunSteadyPullScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "steady-pull-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift steady-pull scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift steady-pull identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftRebuildApply(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "rebuild-apply-001.json"), 100)
	result, err := RunRebuildApplyScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Swift rebuild-apply scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift rebuild-apply identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftRebuildCardinality(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "rebuild-cardinality-001.json"), 100)
	result, err := RunRebuildCardinalityScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Swift rebuild-cardinality scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift rebuild-cardinality identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftRebuildRequests(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "rebuild-requests-001.json"), 1)
	result, err := RunRebuildRequestsScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "rebuild-requests-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift rebuild-requests scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift rebuild-requests identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftForgedCursor(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "rebuild-forged-cursor-001.json"), 1)
	root := filepath.Join("..", "..")
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedToolPath == "" {
		seedToolPath = filepath.Join(root, "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make Swift forged-cursor seed staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{Harness: harness, SeedToolPath: seedToolPath, StagingDirectory: stagingDirectory})
	if err != nil {
		t.Fatalf("create Swift forged-cursor seed artifact: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := artifact.Close(closeContext); err != nil {
			t.Errorf("close Swift forged-cursor seed artifact: %v", err)
		}
	})
	result, err := RunForgedCursorScenario(ctx, scenario, controller, artifact, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "forged-cursor-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift forged-cursor scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift forged-cursor identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
}

func runSwiftPendingCycle(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "pending-cycle-001.json"), 0)
	_, err := RunPendingCycleScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "pending-cycle-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift pending-cycle scenario: %v", err)
	}
}

func runSwiftQueueReplay(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "queue-replay-001.json"), 0)
	result, err := RunQueueReplayScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "queue-replay-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift queue-replay scenario: %v", err)
	}
	if len(result.ReplayCalls) != 9 {
		t.Fatalf("Swift queue-replay replay calls = %d, want 9", len(result.ReplayCalls))
	}
}

func resetSwiftPerformanceServer(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	// A capture dependency registration stays pending while its source table
	// holds rows, so the replayed registrations never activate unless every
	// diagnostic source a scenario writes is empty first.
	for _, table := range blackbox.DiagnosticSourceTables() {
		if err := harness.Source().ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clear Swift performance source table %s: %v", table, err)
		}
	}
	reinstall, err := harness.ReinstallExtension(ctx)
	if err != nil {
		t.Fatalf("reset Swift performance extension state: %v", err)
	}
	minimumGeneration := int64(0)
	for phase := 0; phase < 2; phase++ {
		deadline := time.Now().Add(90 * time.Second)
		var ready blackbox.ExtensionReinstallObservation
		readyObserved := false
		for time.Now().Before(deadline) {
			ready, err = harness.Operator().ObserveExtensionReinstall(ctx, reinstall.ReinstallLSN)
			if err == nil && ready.WorkerPID > 0 && ready.WorkerPID != reinstall.PriorWorkerPID &&
				ready.ActiveSlotName == harness.Names().ReplicationSlot && ready.RestartLSN != "" && ready.SlotActive &&
				ready.RestartLSNAtOrAfterReinstall && ready.ActiveRegistryGeneration > minimumGeneration &&
				ready.PendingRegistryGenerationCount == 0 && ready.NoValidationFailurePoison {
				readyObserved = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !readyObserved {
			// The loop exits on an unmet condition, not only on an error, so name
			// every condition. Reporting err alone prints a nil error.
			t.Fatalf("wait for Swift performance extension reset phase %d: err %v worker %d prior %d slot %q want %q restartLSN %q slotActive %v restartAtOrAfter %v activeGeneration %d minimum %d pendingGenerations %d noPoison %v",
				phase, err, ready.WorkerPID, reinstall.PriorWorkerPID,
				ready.ActiveSlotName, harness.Names().ReplicationSlot,
				ready.RestartLSN, ready.SlotActive, ready.RestartLSNAtOrAfterReinstall,
				ready.ActiveRegistryGeneration, minimumGeneration,
				ready.PendingRegistryGenerationCount, ready.NoValidationFailurePoison)
		}
		if phase == 0 {
			minimumGeneration = ready.ActiveRegistryGeneration
			// A scenario transitions the shared schema-queue field with a data
			// definition change. The reinstall has cleared every registry
			// generation, so this is the only point where restoring the
			// authored column shape invalidates no registration.
			if err := harness.Operator().RestoreSchemaQueueFixture(ctx); err != nil {
				t.Fatalf("restore Swift performance schema-queue fixture: %v", err)
			}
			if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
				t.Fatalf("restore Swift performance source registrations: %v", err)
			}
		}
	}
}

func runSwiftSeededEmptyStartup(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "seeded-empty-startup-001.json"), 0)
	root := filepath.Join("..", "..")
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedToolPath == "" {
		seedToolPath = filepath.Join(root, "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make Swift seed staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{
		Harness:          harness,
		SeedToolPath:     seedToolPath,
		StagingDirectory: stagingDirectory,
	})
	if err != nil {
		t.Fatalf("create Swift seed artifact: %v", err)
	}
	t.Cleanup(func() {
		if err := artifact.Close(context.Background()); err != nil {
			t.Errorf("close Swift seed artifact: %v", err)
		}
	})
	result, err := RunSeededEmptyStartupScenario(ctx, scenario, controller, artifact, platform)
	if err != nil {
		t.Fatalf("run direct Swift seeded-empty-startup scenario: %v", err)
	}
	if len(result.Clients) != 6 {
		t.Fatalf("Swift seeded-empty-startup clients = %d, want 6", len(result.Clients))
	}
}

// swiftPerformanceSuiteReset resets the server once, before the first
// scenario of this suite runs.
var swiftPerformanceSuiteReset sync.Once

func newSwiftPerformanceFixture(t *testing.T, scenarioPath string, pullPageSize int) (context.Context, scenarios.Scenario, *blackbox.Harness, *blackbox.NativeController, *Platform) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatal("TestRealSwiftPerformance requires --provision --install")
	}
	runnerPath := os.Getenv("SYNCHRO_SWIFT_NATIVE_RUNNER")
	if runnerPath == "" {
		t.Fatal("SYNCHRO_SWIFT_NATIVE_RUNNER is required")
	}
	environment, err := blackbox.LoadLocalEnvironment()
	if err != nil {
		t.Fatalf("load Swift conformance environment: %v", err)
	}
	provisionContext, cancelProvision := context.WithTimeout(context.Background(), 2*time.Minute)
	harness, err := blackbox.Provision(provisionContext, blackbox.HarnessConfig{Environment: environment})
	cancelProvision()
	if err != nil {
		t.Fatalf("provision Swift conformance harness: %v", err)
	}
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = harness.Close(closeContext)
		t.Fatalf("create Swift native controller: %v", err)
	}
	// Each scenario resets the server when it finishes, so only the first
	// scenario can inherit state. A target that runs before this suite, such as
	// the warm-connect target, leaves its own state on the same instance, so the
	// suite starts from a known state rather than that inherited one.
	swiftPerformanceSuiteReset.Do(func() {
		resetContext, cancelReset := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancelReset()
		resetSwiftPerformanceServer(t, resetContext, harness)
	})
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close Swift native controller: %v", err)
		}
	})

	databaseDirectory := t.TempDir()
	if err := os.Chmod(databaseDirectory, 0o700); err != nil {
		t.Fatalf("make Swift database directory private: %v", err)
	}
	platform, err := NewPlatform(Config{
		RunnerPath:                   runnerPath,
		ApplicationDatabaseDirectory: databaseDirectory,
		ServerURL:                    harness.AdapterURL(),
		AuthToken: func(tokenContext context.Context, client Client) (string, error) {
			return harness.NativeBearerToken(tokenContext, client.UserID, time.Now())
		},
		Platform:      "macos",
		AppVersion:    "0.3.0",
		PullPageSize:  pullPageSize,
		PushBatchSize: 1000,
	})
	if err != nil {
		t.Fatalf("create Swift direct platform: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := platform.Close(closeContext); err != nil {
			t.Errorf("close Swift direct platform: %v", err)
		}
	})

	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, filepath.Join("conformance", "scenarios", scenarioPath))
	if err != nil {
		t.Fatalf("load Swift performance scenario %s: %v", scenarioPath, err)
	}
	// The reset runs as a cleanup so a scenario that fails still restores server
	// state. A trailing call never runs after t.Fatalf, which leaves every later
	// scenario running against the failed scenario's state. Cleanups run last
	// registered first, so this reset precedes the platform and controller close
	// and the context cancel.
	t.Cleanup(func() {
		// SYNCHRO_KEEP_SERVER_STATE retains the failing server state for post
		// mortem inspection. The reset destroys the evidence a failure leaves.
		if os.Getenv("SYNCHRO_KEEP_SERVER_STATE") != "" && t.Failed() {
			return
		}
		resetSwiftPerformanceServer(t, ctx, harness)
	})
	return ctx, scenario, harness, controller, platform
}

// newSwiftPerformanceArtifact builds a portable seed artifact for a scenario
// whose setup declares an established client.
func newSwiftPerformanceArtifact(t *testing.T, harness *blackbox.Harness) *blackbox.NativeArtifact {
	t.Helper()
	seedToolPath := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedToolPath == "" {
		seedToolPath = filepath.Join("..", "..", "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make Swift performance seed staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{Harness: harness, SeedToolPath: seedToolPath, StagingDirectory: stagingDirectory})
	if err != nil {
		t.Fatalf("create Swift performance seed artifact: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := artifact.Close(closeContext); err != nil {
			t.Errorf("close Swift performance seed artifact: %v", err)
		}
	})
	return artifact
}
