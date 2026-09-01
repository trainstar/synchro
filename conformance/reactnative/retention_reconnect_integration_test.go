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

func TestRealReactNativeRetentionReconnectIOS(t *testing.T) {
	t.Run("assertion", func(t *testing.T) { runRealReactNativeRetentionReconnect(t, "ios") })
}

func TestRealReactNativeRetentionReconnectAndroid(t *testing.T) {
	t.Run("assertion", func(t *testing.T) { runRealReactNativeRetentionReconnect(t, "android") })
}

func runRealReactNativeRetentionReconnect(t *testing.T, platform string) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatalf("React Native %s retention-reconnect requires --provision --install", platform)
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
	scenario, err := LoadRetentionReconnectScenario(runContext, repositoryRoot)
	if err != nil {
		t.Fatalf("load React Native retention-reconnect scenario: %v", err)
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
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
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
	coordinator, err := NewRetentionReconnectCoordinator(RetentionReconnectCoordinatorConfig{
		Scenario: scenario, Harness: harness, Controller: controller, Platform: platform,
	})
	if err != nil {
		t.Fatalf("create React Native %s retention-reconnect coordinator: %v", platform, err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := coordinator.Close(closeContext); err != nil {
			t.Errorf("close React Native %s retention-reconnect coordinator: %v", platform, err)
		}
	})
	if err := coordinator.Prepare(runContext); err != nil {
		t.Fatalf("prepare React Native %s retention-reconnect coordinator: %v", platform, err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- coordinator.Serve(runContext) }()

	resultPath := filepath.Join(t.TempDir(), fmt.Sprintf("react-native-%s-retention-reconnect.json", platform))
	command := exec.CommandContext(runContext, "npx", "detox", "test", "e2e/retention-reconnect.test.ts", "--config-path", "./.detoxrc.steady-pull.js", "--configuration", detoxConfiguration, "--json", "--outputFile", resultPath)
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
		t.Fatalf("run React Native %s retention-reconnect Detox test: %v; coordinator: %v\n%s", platform, err, coordinatorErr, output)
	}
	expectedTestPath := filepath.Join(repositoryRoot, "clients", "react-native", "example", "e2e", "retention-reconnect.test.ts")
	if err := validateDetoxSingleTestResult(resultPath, expectedTestPath, "executes the retention-reconnect coordinator sequence", "retention-reconnect"); err != nil {
		t.Fatalf("validate React Native %s retention-reconnect Detox result: %v\n%s", platform, err, output)
	}
	if !coordinator.Completed() {
		t.Fatalf("React Native %s retention-reconnect coordinator did not complete", platform)
	}
	result, err := coordinator.Result()
	if err != nil {
		t.Fatalf("read React Native %s retention-reconnect result: %v", platform, err)
	}
	if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
		t.Fatalf("React Native %s retention-reconnect identity resolutions = %d, want %d", platform, len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
	}
	if len(result.ServerFacts.Scopes) == 0 || len(result.ServerFacts.Rebuilds) == 0 {
		t.Fatalf("React Native %s retention-reconnect server state lacks scope or rebuild evidence", platform)
	}

	closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = coordinator.Close(closeContext)
	closeCancel()
	if err != nil {
		t.Fatalf("stop React Native %s retention-reconnect coordinator: %v", platform, err)
	}
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("serve React Native %s retention-reconnect coordinator: %v", platform, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("React Native %s retention-reconnect coordinator did not stop", platform)
	}
}
