//go:build reactnativeintegration

package reactnative

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealReactNativeSeededEmptyStartupIOS(t *testing.T) {
	runRealReactNativeSeededEmptyStartup(t, "ios")
}

func TestRealReactNativeSeededEmptyStartupAndroid(t *testing.T) {
	runRealReactNativeSeededEmptyStartup(t, "android")
}

func runRealReactNativeSeededEmptyStartup(t *testing.T, platform string) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatalf("React Native %s seeded-empty-startup requires --provision --install", platform)
	}
	detoxConfiguration := os.Getenv("SYNCHRO_RN_DETOX_CONFIGURATION")
	if detoxConfiguration == "" {
		t.Fatal("SYNCHRO_RN_DETOX_CONFIGURATION is required")
	}
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	runContext, cancelRun := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancelRun()
	scenario, err := LoadSeededEmptyStartupScenario(runContext, repositoryRoot)
	if err != nil {
		t.Fatalf("load React Native seeded-empty-startup scenario: %v", err)
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load React Native conformance environment: %v", err)
	}
	provisionContext, cancelProvision := context.WithTimeout(runContext, 2*time.Minute)
	harness, err := blackbox.Provision(provisionContext, blackbox.HarnessConfig{Environment: environment})
	cancelProvision()
	if err != nil {
		t.Fatalf("provision React Native conformance harness: %v", err)
	}
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = harness.Close(closeContext)
		t.Fatalf("create React Native native controller: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close React Native native controller: %v", err)
		}
	})
	seedTool := os.Getenv("SYNCHRO_SEED_TOOL")
	if seedTool == "" {
		seedTool = filepath.Join(repositoryRoot, "bin", "synchro-seed")
	}
	stagingDirectory := t.TempDir()
	if err := os.Chmod(stagingDirectory, 0o700); err != nil {
		t.Fatalf("make React Native seed staging directory private: %v", err)
	}
	artifact, err := blackbox.NewNativeArtifact(blackbox.NativeArtifactConfig{Harness: harness, SeedToolPath: seedTool, StagingDirectory: stagingDirectory})
	if err != nil {
		t.Fatalf("create React Native seed artifact: %v", err)
	}
	t.Cleanup(func() {
		if err := artifact.Close(context.Background()); err != nil {
			t.Errorf("close React Native seed artifact: %v", err)
		}
	})
	coordinator, err := NewSeededEmptyStartupCoordinator(SeededEmptyStartupCoordinatorConfig{
		Scenario: scenario, Harness: harness, Controller: controller, Artifact: artifact, Platform: platform,
	})
	if err != nil {
		t.Fatalf("create React Native %s seeded-empty-startup coordinator: %v", platform, err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Close(closeContext); err != nil {
			t.Errorf("close React Native %s seeded-empty-startup coordinator: %v", platform, err)
		}
	})
	if err := coordinator.Prepare(runContext); err != nil {
		t.Fatalf("prepare React Native %s seeded-empty-startup coordinator: %v", platform, err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- coordinator.Serve(runContext) }()

	t.Run("assertion", func(t *testing.T) {
		resultPath := filepath.Join(t.TempDir(), fmt.Sprintf("react-native-%s-seeded-empty-startup.json", platform))
		command := exec.CommandContext(runContext, "npx", "detox", "test", "e2e/seeded-empty-startup.test.ts", "--config-path", "./.detoxrc.steady-pull.js", "--configuration", detoxConfiguration, "--json", "--outputFile", resultPath)
		command.Dir = filepath.Join(repositoryRoot, "clients", "react-native", "example")
		for _, assignment := range os.Environ() {
			if strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_URL=") || strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_TOKEN=") || strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_STAGE_COUNT=") {
				continue
			}
			command.Env = append(command.Env, assignment)
		}
		command.Env = append(command.Env,
			"SYNCHRO_RN_COORDINATOR_URL="+coordinator.URL(),
			"SYNCHRO_RN_COORDINATOR_TOKEN="+coordinator.Token(),
			"SYNCHRO_RN_COORDINATOR_STAGE_COUNT="+strconv.Itoa(coordinator.StageCount()),
		)
		output, err := command.CombinedOutput()
		if err != nil {
			_, coordinatorErr := coordinator.Result()
			t.Fatalf("run React Native %s seeded-empty-startup Detox test: %v; coordinator: %v\n%s", platform, err, coordinatorErr, output)
		}
		expectedTestPath := filepath.Join(repositoryRoot, "clients", "react-native", "example", "e2e", "seeded-empty-startup.test.ts")
		if err := validateDetoxSingleTestResult(resultPath, expectedTestPath, "executes the seeded-empty-startup coordinator sequence", "seeded-empty-startup"); err != nil {
			t.Fatalf("validate React Native %s seeded-empty-startup Detox result: %v\n%s", platform, err, output)
		}
		if !coordinator.Completed() {
			t.Fatalf("React Native %s seeded-empty-startup coordinator did not complete", platform)
		}
		result, err := coordinator.Result()
		if err != nil {
			t.Fatalf("read React Native %s seeded-empty-startup result: %v", platform, err)
		}
		if result.StartupCount == 0 || len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
			t.Fatalf("React Native %s seeded-empty-startup result is incomplete", platform)
		}
	})

	closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = coordinator.Close(closeContext)
	cancel()
	if err != nil {
		t.Fatalf("stop React Native %s seeded-empty-startup coordinator: %v", platform, err)
	}
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("serve React Native %s seeded-empty-startup coordinator: %v", platform, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("React Native %s seeded-empty-startup coordinator did not stop", platform)
	}
}
