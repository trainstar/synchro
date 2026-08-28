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
		runSwiftPendingCycle(t)
		runSwiftQueueReplay(t)
		runSwiftSeededEmptyStartup(t)
	})
}

func runSwiftSteadyPull(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, "steady-pull-001.json")
	result, err := RunSteadyPullScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "steady-pull-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift steady-pull scenario: %v", err)
	}
	if len(result.BaselineCall.transportObservations) != 3 || len(result.MeasuredCall.transportObservations) != 1 {
		t.Fatalf("Swift steady-pull transport calls = %d and %d, want 3 and 1", len(result.BaselineCall.transportObservations), len(result.MeasuredCall.transportObservations))
	}
}

func runSwiftPendingCycle(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, "pending-cycle-001.json")
	result, err := RunPendingCycleScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "pending-cycle-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift pending-cycle scenario: %v", err)
	}
	if len(result.PushCall.transportObservations) != 2 || len(result.PullCall.transportObservations) != 1 {
		t.Fatalf("Swift pending-cycle transport calls = %d and %d, want 2 and 1", len(result.PushCall.transportObservations), len(result.PullCall.transportObservations))
	}
}

func runSwiftQueueReplay(t *testing.T) {
	t.Helper()
	ctx, scenario, _, controller, platform := newSwiftPerformanceFixture(t, "queue-replay-001.json")
	result, err := RunQueueReplayScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "queue-replay-client-a"})
	if err != nil {
		t.Fatalf("run direct Swift queue-replay scenario: %v", err)
	}
	if len(result.ReplayCalls) != 9 {
		t.Fatalf("Swift queue-replay replay calls = %d, want 9", len(result.ReplayCalls))
	}
}

func runSwiftSeededEmptyStartup(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newSwiftPerformanceFixture(t, "seeded-empty-startup-001.json")
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

func newSwiftPerformanceFixture(t *testing.T, filename string) (context.Context, scenarios.Scenario, *blackbox.Harness, *blackbox.NativeController, *Platform) {
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
		Platform:   "macos",
		AppVersion: "0.3.0",
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
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, "conformance/scenarios/performance/"+filename)
	if err != nil {
		t.Fatalf("load Swift performance scenario %s: %v", filename, err)
	}
	return ctx, scenario, harness, controller, platform
}
