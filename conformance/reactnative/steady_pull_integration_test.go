//go:build reactnativeintegration

package reactnative

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealReactNativeSteadyPullIOS(t *testing.T) {
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatal("React Native iOS steady-pull requires --provision --install")
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
	scenario, err := LoadSteadyPullScenario(runContext, repositoryRoot)
	if err != nil {
		t.Fatalf("load React Native steady-pull scenario: %v", err)
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
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close React Native native controller: %v", err)
		}
	})

	coordinator, err := NewSteadyPullCoordinator(SteadyPullCoordinatorConfig{
		Scenario:   scenario,
		Harness:    harness,
		Controller: controller,
		Platform:   "ios",
	})
	if err != nil {
		t.Fatalf("create React Native iOS steady-pull coordinator: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := coordinator.Close(closeContext); err != nil {
			t.Errorf("close React Native iOS steady-pull coordinator: %v", err)
		}
	})
	if err := coordinator.Prepare(runContext); err != nil {
		t.Fatalf("prepare React Native iOS steady-pull coordinator: %v", err)
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- coordinator.Serve(runContext)
	}()

	t.Run("assertion", func(t *testing.T) {
		resultPath := filepath.Join(t.TempDir(), "react-native-ios-steady-pull.json")
		command := exec.CommandContext(
			runContext,
			"npx", "detox", "test", "e2e/steady-pull.test.ts",
			"--config-path", "./.detoxrc.steady-pull.js",
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
			t.Fatalf("run React Native iOS steady-pull Detox test: %v; coordinator: %v\n%s", err, coordinatorErr, output)
		}
		expectedTestPath := filepath.Join(repositoryRoot, "clients", "react-native", "example", "e2e", "steady-pull.test.ts")
		if err := validateDetoxSteadyPullResult(resultPath, expectedTestPath, "executes the steady-pull coordinator sequence"); err != nil {
			t.Fatalf("validate React Native iOS steady-pull Detox result: %v\n%s", err, output)
		}
		if !coordinator.Completed() {
			t.Fatalf("React Native iOS steady-pull coordinator did not complete")
		}
		result, err := coordinator.Result()
		if err != nil {
			t.Fatalf("read React Native iOS steady-pull result: %v", err)
		}
		if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
			t.Fatalf("React Native iOS steady-pull identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
		}
	})

	closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = coordinator.Close(closeContext)
	closeCancel()
	if err != nil {
		t.Fatalf("stop React Native iOS steady-pull coordinator: %v", err)
	}
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("serve React Native iOS steady-pull coordinator: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("React Native iOS steady-pull coordinator did not stop")
	}
}
