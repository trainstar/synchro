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

func TestRealReactNativeMultiScopeProvenanceIOS(t *testing.T) {
	runRealReactNativeMultiScopeProvenance(t, "ios")
}
func TestRealReactNativeMultiScopeProvenanceAndroid(t *testing.T) {
	runRealReactNativeMultiScopeProvenance(t, "android")
}

func runRealReactNativeMultiScopeProvenance(t *testing.T, platform string) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatalf("React Native %s multi-scope provenance requires --provision --install", platform)
	}
	configuration := os.Getenv("SYNCHRO_RN_DETOX_CONFIGURATION")
	if configuration == "" {
		t.Fatal("SYNCHRO_RN_DETOX_CONFIGURATION is required")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	scenario, err := LoadMultiScopeProvenanceScenario(ctx, root)
	if err != nil {
		t.Fatalf("load React Native multi-scope provenance scenario: %v", err)
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load React Native conformance environment: %v", err)
	}
	provisionCtx, stopProvision := context.WithTimeout(ctx, 2*time.Minute)
	harness, err := blackbox.Provision(provisionCtx, blackbox.HarnessConfig{Environment: environment})
	stopProvision()
	if err != nil {
		t.Fatalf("provision React Native conformance harness: %v", err)
	}
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		_ = harness.Close(context.Background())
		t.Fatalf("create React Native native controller: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeCtx); err != nil {
			t.Errorf("close React Native native controller: %v", err)
		}
	})
	coordinator, err := NewMultiScopeProvenanceCoordinator(MultiScopeProvenanceCoordinatorConfig{Scenario: scenario, Harness: harness, Controller: controller, Platform: platform})
	if err != nil {
		t.Fatalf("create React Native %s multi-scope provenance coordinator: %v", platform, err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = coordinator.Close(closeCtx)
	})
	if err := coordinator.Prepare(ctx); err != nil {
		t.Fatalf("prepare React Native %s multi-scope provenance coordinator: %v", platform, err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- coordinator.Serve(ctx) }()
	// The gate reads one assertion subtest for this target, as every other
	// React Native integration test provides.
	t.Run("assertion", func(t *testing.T) {
		resultPath := filepath.Join(t.TempDir(), fmt.Sprintf("react-native-%s-multi-scope-provenance.json", platform))
		command := exec.CommandContext(ctx, "npx", "detox", "test", "e2e/multi-scope-provenance.test.ts", "--config-path", "./.detoxrc.steady-pull.js", "--configuration", configuration, "--json", "--outputFile", resultPath)
		command.Dir = filepath.Join(root, "clients", "react-native", "example")
		for _, assignment := range os.Environ() {
			if !strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_URL=") && !strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_TOKEN=") && !strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_STAGE_COUNT=") {
				command.Env = append(command.Env, assignment)
			}
		}
		command.Env = append(command.Env, "SYNCHRO_RN_COORDINATOR_URL="+coordinator.URL(), "SYNCHRO_RN_COORDINATOR_TOKEN="+coordinator.Token(), "SYNCHRO_RN_COORDINATOR_STAGE_COUNT="+strconv.Itoa(coordinator.ExchangeCount()))
		output, err := command.CombinedOutput()
		if err != nil {
			_, coordinatorErr := coordinator.Result()
			t.Fatalf("run React Native %s multi-scope provenance Detox test: %v; coordinator: %v\n%s", platform, err, coordinatorErr, output)
		}
		expectedTestPath := filepath.Join(root, "clients", "react-native", "example", "e2e", "multi-scope-provenance.test.ts")
		if err := validateDetoxSingleTestResult(resultPath, expectedTestPath, "executes the multi-scope-provenance coordinator sequence", "multi-scope-provenance"); err != nil {
			t.Fatalf("validate React Native %s multi-scope provenance Detox result: %v\n%s", platform, err, output)
		}
		if !coordinator.Completed() {
			t.Fatalf("React Native %s multi-scope provenance coordinator did not complete", platform)
		}
		result, err := coordinator.Result()
		if err != nil {
			t.Fatalf("read React Native %s multi-scope provenance result: %v", platform, err)
		}
		if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
			t.Fatalf("React Native %s multi-scope provenance identity resolutions = %d, want %d", platform, len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
		}
	})
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = coordinator.Close(closeCtx)
	closeCancel()
	if err != nil {
		t.Fatalf("stop React Native %s multi-scope provenance coordinator: %v", platform, err)
	}
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("serve React Native %s multi-scope provenance coordinator: %v", platform, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("React Native %s multi-scope provenance coordinator did not stop", platform)
	}
}
