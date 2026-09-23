//go:build swiftintegration

package swift

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

var (
	warmConnectProvision = flag.Bool("provision", false, "provision the isolated PostgreSQL harness")
	warmConnectInstall   = flag.Bool("install", false, "install the candidate extension")
)

func TestRealSwiftWarmConnect(t *testing.T) {
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatal("TestRealSwiftWarmConnect requires --provision --install")
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
	scenarioContext, cancelScenario := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelScenario()
	scenario, err := scenarios.LoadFile(scenarioContext, repositoryRoot, "conformance/scenarios/performance/warm-connect-001.json")
	if err != nil {
		t.Fatalf("load Swift warm-connect scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "warm-connect-client-a"}
	t.Run("assertion", func(t *testing.T) {
		result, err := RunWarmConnectScenario(scenarioContext, scenario, controller, platform, client)
		if err != nil {
			t.Fatalf("run direct Swift warm-connect scenario: %v", err)
		}
		if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
			t.Fatalf("Swift identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
		}
		if len(result.WarmCall.Steps) != 2 || len(result.WarmCall.transportObservations) != 2 {
			t.Fatalf("Swift warm request count = %d, want 2", len(result.WarmCall.transportObservations))
		}
	})
}
