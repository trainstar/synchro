//go:build swiftintegration

package swift

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRealSwiftPerformance(t *testing.T) {
	t.Run("assertion", func(t *testing.T) {
		runSwiftSteadyPull(t)
		runSwiftRebuildApply(t)
		runSwiftRebuildCardinality(t)
		runSwiftPushResponseLoss(t)
		runSwiftRetentionReconnect(t)
		runSwiftRebuildRequests(t)
		runSwiftForgedCursor(t)
		runSwiftPendingCycle(t)
		runSwiftQueueReplay(t)
		runSwiftSeededEmptyStartup(t)
		// Ordered last so an unrelated failure cannot stop the scenarios above it.
		runSwiftSchemaQueuedMutation(t)
	})
}

func runSwiftRetentionReconnect(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "retention-reconnect-001.json"), 1)
	result, err := RunRetentionReconnectScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "retention-reconnect-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift retention-reconnect scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift retention-reconnect identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftPushResponseLoss(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "push-response-loss-001.json"), 0)
	result, err := RunPushResponseLossScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift push-response-loss scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift push-response-loss identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftSchemaQueuedMutation(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("server", "schema-queued-mutation-001.json"), 100)
	result, err := RunSchemaQueuedMutationScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "schema-queued-mutation-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift schema-queued-mutation scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift schema-queued-mutation identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftSteadyPull(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "steady-pull-001.json"), 0)
	result, err := RunSteadyPullScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "steady-pull-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift steady-pull scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift steady-pull identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftRebuildApply(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "rebuild-apply-001.json"), 100)
	result, err := RunRebuildApplyScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Swift rebuild-apply scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift rebuild-apply identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftRebuildCardinality(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "rebuild-cardinality-001.json"), 100)
	result, err := RunRebuildCardinalityScenario(ctx, scenario, controller, platform)
	if err != nil {
		t.Fatalf("run direct Swift rebuild-cardinality scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift rebuild-cardinality identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftRebuildRequests(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "rebuild-requests-001.json"), 1)
	result, err := RunRebuildRequestsScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "rebuild-requests-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift rebuild-requests scenario: %v", err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("Swift rebuild-requests identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
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
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftPendingCycle(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "pending-cycle-001.json"), 0)
	_, err := RunPendingCycleScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "pending-cycle-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift pending-cycle scenario: %v", err)
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func runSwiftQueueReplay(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, filepath.Join("performance", "queue-replay-001.json"), 0)
	result, err := RunQueueReplayScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "queue-replay-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift queue-replay scenario: %v", err)
	}
	if len(result.ReplayCalls) != 9 {
		t.Fatalf("Swift queue-replay replay calls = %d, want 9", len(result.ReplayCalls))
	}
	resetSwiftPerformanceServer(t, ctx, harness)
}

func resetSwiftPerformanceServer(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_items"); err != nil {
		t.Fatalf("clear Swift performance source state: %v", err)
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
			t.Fatalf("wait for Swift performance extension reset: %v", err)
		}
		if phase == 0 {
			minimumGeneration = ready.ActiveRegistryGeneration
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

func newSwiftPerformanceFixture(t *testing.T, scenarioPath string, pullPageSize int) (context.Context, scenarios.Scenario, *blackbox.Harness, *blackbox.NativeController, *Platform) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatal("TestRealSwiftPerformance requires --provision --install")
	}
	runnerPath := os.Getenv("SYNCHRO_SWIFT_NATIVE_RUNNER")
	if runnerPath == "" {
		t.Fatal("SYNCHRO_SWIFT_NATIVE_RUNNER is required")
	}
	environment, err := blackbox.LoadEnvironment()
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
	return ctx, scenario, harness, controller, platform
}
