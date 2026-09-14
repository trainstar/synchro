//go:build reactnativeintegration

package reactnative

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

var (
	warmConnectProvision = flag.Bool("provision", false, "provision the isolated PostgreSQL harness")
	warmConnectInstall   = flag.Bool("install", false, "install the candidate extension")
)

func TestRealReactNativeWarmConnectIOS(t *testing.T) {
	runRealReactNativeWarmConnect(t, "ios")
}

func TestRealReactNativeWarmConnectAndroid(t *testing.T) {
	runRealReactNativeWarmConnect(t, "android")
}

func runRealReactNativeWarmConnect(t *testing.T, platform string) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatalf("React Native %s warm-connect requires --provision --install", platform)
	}
	detoxConfiguration := os.Getenv("SYNCHRO_RN_DETOX_CONFIGURATION")
	if detoxConfiguration == "" {
		t.Fatal("SYNCHRO_RN_DETOX_CONFIGURATION is required")
	}

	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	runContext, cancelRun := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancelRun()
	scenario, err := LoadWarmConnectScenario(runContext, repositoryRoot)
	if err != nil {
		t.Fatalf("load React Native warm-connect scenario: %v", err)
	}
	environment, err := blackbox.LoadLocalEnvironment()
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
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close React Native native controller: %v", err)
		}
	})

	coordinator, err := NewCoordinator(CoordinatorConfig{
		Scenario:   scenario,
		Harness:    harness,
		Controller: controller,
		Platform:   platform,
	})
	if err != nil {
		t.Fatalf("create React Native %s coordinator: %v", platform, err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := coordinator.Close(closeContext); err != nil {
			t.Errorf("close React Native %s coordinator: %v", platform, err)
		}
	})
	if err := coordinator.Prepare(runContext); err != nil {
		t.Fatalf("prepare React Native %s coordinator: %v", platform, err)
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- coordinator.Serve(runContext)
	}()

	t.Run("assertion", func(t *testing.T) {
		resultPath := filepath.Join(t.TempDir(), fmt.Sprintf("react-native-%s-warm-connect.json", platform))
		command := exec.CommandContext(
			runContext,
			"npx", "detox", "test", "e2e/warm-connect.test.ts",
			"--config-path", "./.detoxrc.warm-connect.js",
			"--configuration", detoxConfiguration,
			"--json", "--outputFile", resultPath,
		)
		command.Dir = filepath.Join(repositoryRoot, "clients", "react-native", "example")
		for _, assignment := range os.Environ() {
			if strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_URL=") || strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_TOKEN=") {
				continue
			}
			command.Env = append(command.Env, assignment)
		}
		command.Env = append(command.Env,
			"SYNCHRO_RN_COORDINATOR_URL="+coordinator.URL(),
			"SYNCHRO_RN_COORDINATOR_TOKEN="+coordinator.Token(),
		)
		output, err := command.CombinedOutput()
		if err != nil {
			_, coordinatorErr := coordinator.Result()
			t.Fatalf("run React Native %s warm-connect Detox test: %v; coordinator: %v\n%s", platform, err, coordinatorErr, output)
		}
		expectedTestPath := filepath.Join(repositoryRoot, "clients", "react-native", "example", "e2e", "warm-connect.test.ts")
		if err := validateDetoxWarmConnectResult(resultPath, expectedTestPath, "executes the warm-connect coordinator sequence"); err != nil {
			t.Fatalf("validate React Native %s warm-connect Detox result: %v\n%s", platform, err, output)
		}
		if !coordinator.Completed() {
			t.Fatalf("React Native %s warm-connect coordinator did not complete", platform)
		}
		result, err := coordinator.Result()
		if err != nil {
			t.Fatalf("read React Native %s warm-connect result: %v", platform, err)
		}
		if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
			t.Fatalf("React Native %s identity resolutions = %d, want %d", platform, len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
		}
	})

	closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = coordinator.Close(closeContext)
	closeCancel()
	if err != nil {
		t.Fatalf("stop React Native %s coordinator: %v", platform, err)
	}
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("serve React Native %s coordinator: %v", platform, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("React Native %s coordinator did not stop", platform)
	}
}
