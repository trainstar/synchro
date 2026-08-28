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
	t.Run("assertion", func(t *testing.T) {
		runKotlinSteadyPull(t)
		runKotlinPendingCycle(t)
		runKotlinQueueReplay(t)
		runKotlinSeededEmptyStartup(t)
	})
}

func runKotlinSteadyPull(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "steady-pull-001.json")
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
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "pending-cycle-001.json")
	if _, err := RunPendingCycleScenario(ctx, scenario, controller, platform, Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "pending-cycle-client-a"}); err != nil {
		t.Fatalf("run direct Kotlin Android pending-cycle scenario: %v", err)
	}
	resetKotlinPerformanceServer(t, ctx, harness)
}

func runKotlinQueueReplay(t *testing.T) {
	t.Helper()
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "queue-replay-001.json")
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
	ctx, scenario, harness, controller, platform := newKotlinPerformanceFixture(t, "seeded-empty-startup-001.json")
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

func resetKotlinPerformanceServer(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_items"); err != nil {
		t.Fatalf("clear Kotlin Android performance source state: %v", err)
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
			t.Fatalf("wait for Kotlin Android performance extension reset: %v", err)
		}
		if phase == 0 {
			minimumGeneration = ready.ActiveRegistryGeneration
			if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
				t.Fatalf("restore Kotlin Android performance source registrations: %v", err)
			}
		}
	}
}

func newKotlinPerformanceFixture(t *testing.T, filename string) (context.Context, scenarios.Scenario, *blackbox.Harness, *blackbox.NativeController, *Platform) {
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
	environment, err := blackbox.LoadEnvironment()
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
		Platform: "android", AppVersion: "0.3.0", PushBatchSize: 1000,
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
	scenario, err := scenarios.LoadFile(ctx, repositoryRoot, "conformance/scenarios/performance/"+filename)
	if err != nil {
		t.Fatalf("load Kotlin Android performance scenario %s: %v", filename, err)
	}
	return ctx, scenario, harness, controller, platform
}
