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

func TestRealReactNativeForgedCursorIOS(t *testing.T) {
	t.Run("assertion", func(t *testing.T) {
		runRealReactNativeForgedCursor(t, "ios")
	})
}

func TestRealReactNativeForgedCursorAndroid(t *testing.T) {
	t.Run("assertion", func(t *testing.T) {
		runRealReactNativeForgedCursor(t, "android")
	})
}

func runRealReactNativeForgedCursor(t *testing.T, platform string) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatalf("React Native %s forged-cursor requires --provision --install", platform)
	}
	detoxConfiguration := os.Getenv("SYNCHRO_RN_DETOX_CONFIGURATION")
	if detoxConfiguration == "" {
		t.Fatal("SYNCHRO_RN_DETOX_CONFIGURATION is required")
	}
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	runContext, cancelRun := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancelRun()
	scenario, err := LoadForgedCursorScenario(runContext, repositoryRoot)
	if err != nil {
		t.Fatalf("load React Native forged-cursor scenario: %v", err)
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
		_ = harness.Close(context.Background())
		t.Fatalf("create React Native native controller: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close React Native native controller: %v", err)
		}
	})
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: scenario, Harness: harness, Controller: controller, Platform: platform,
	})
	if err != nil {
		t.Fatalf("create React Native %s forged-cursor coordinator: %v", platform, err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Close(closeContext); err != nil {
			t.Errorf("close React Native %s forged-cursor coordinator: %v", platform, err)
		}
	})
	if err := coordinator.Prepare(runContext); err != nil {
		t.Fatalf("prepare React Native %s forged-cursor coordinator: %v", platform, err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- coordinator.Serve(runContext) }()

	resultPath := filepath.Join(t.TempDir(), fmt.Sprintf("react-native-%s-forged-cursor.json", platform))
	command := exec.CommandContext(runContext, "npx", "detox", "test", "e2e/forged-cursor.test.ts", "--config-path", "./.detoxrc.steady-pull.js", "--configuration", detoxConfiguration, "--json", "--outputFile", resultPath)
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
		"SYNCHRO_RN_COORDINATOR_STAGE_COUNT="+strconv.Itoa(coordinator.ExchangeCount()),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		_, coordinatorErr := coordinator.Result()
		t.Fatalf("run React Native %s forged-cursor Detox test: %v; coordinator: %v\n%s", platform, err, coordinatorErr, output)
	}
	expectedTestPath := filepath.Join(repositoryRoot, "clients", "react-native", "example", "e2e", "forged-cursor.test.ts")
	if err := validateDetoxSingleTestResult(resultPath, expectedTestPath, "executes the forged-cursor coordinator sequence", "forged-cursor"); err != nil {
		t.Fatalf("validate React Native %s forged-cursor Detox result: %v\n%s", platform, err, output)
	}
	if !coordinator.Completed() {
		t.Fatalf("React Native %s forged-cursor coordinator did not complete", platform)
	}
	result, err := coordinator.Result()
	if err != nil {
		t.Fatalf("read React Native %s forged-cursor result: %v", platform, err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("React Native %s forged-cursor identity resolutions = %d, want %d", platform, len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	if result.ServerFacts.RebuildCount == nil || *result.ServerFacts.RebuildCount != 1 || len(result.ServerFacts.Rebuilds) != 1 {
		t.Fatalf("React Native %s forged-cursor final rebuild facts are incomplete", platform)
	}

	closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = coordinator.Close(closeContext)
	closeCancel()
	if err != nil {
		t.Fatalf("stop React Native %s forged-cursor coordinator: %v", platform, err)
	}
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("serve React Native %s forged-cursor coordinator: %v", platform, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("React Native %s forged-cursor coordinator did not stop", platform)
	}
}
