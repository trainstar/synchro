//go:build kotlinintegration

package kotlin

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

func TestRealKotlinWarmConnect(t *testing.T) {
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatal("TestRealKotlinWarmConnect requires --provision --install")
	}
	adbPath := os.Getenv("SYNCHRO_KOTLIN_ADB")
	deviceSerial := os.Getenv("SYNCHRO_KOTLIN_DEVICE_SERIAL")
	applicationAPK := os.Getenv("SYNCHRO_KOTLIN_APPLICATION_APK")
	instrumentationAPK := os.Getenv("SYNCHRO_KOTLIN_INSTRUMENTATION_APK")
	if adbPath == "" || deviceSerial == "" || applicationAPK == "" || instrumentationAPK == "" {
		t.Fatal("Kotlin warm-connect Android environment is incomplete")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load Kotlin conformance environment: %v", err)
	}
	provisionContext, cancelProvision := context.WithTimeout(context.Background(), 2*time.Minute)
	harness, err := blackbox.Provision(provisionContext, blackbox.HarnessConfig{Environment: environment})
	cancelProvision()
	if err != nil {
		t.Fatalf("provision Kotlin conformance harness: %v", err)
	}
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = harness.Close(closeContext)
		t.Fatalf("create Kotlin native controller: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close Kotlin native controller: %v", err)
		}
	})

	platform, err := NewPlatform(Config{
		ADBPath:                  adbPath,
		DeviceSerial:             deviceSerial,
		ApplicationAPKPath:       applicationAPK,
		InstrumentationAPKPath:   instrumentationAPK,
		ApplicationID:            "com.trainstar.synchro.conformance",
		InstrumentationComponent: "com.trainstar.synchro.conformance.test/androidx.test.runner.AndroidJUnitRunner",
		ServerURL:                harness.AdapterURL(),
		AuthToken: func(tokenContext context.Context, client Client) (string, error) {
			return harness.NativeBearerToken(tokenContext, client.UserID, time.Now())
		},
		Platform:   "android",
		AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create Kotlin direct platform: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := platform.Close(closeContext); err != nil {
			t.Errorf("close Kotlin direct platform: %v", err)
		}
	})

	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenarioContext, cancelScenario := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancelScenario()
	scenario, err := scenarios.LoadFile(scenarioContext, repositoryRoot, "conformance/scenarios/performance/warm-connect-001.json")
	if err != nil {
		t.Fatalf("load Kotlin warm-connect scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "warm-connect-client-a"}
	t.Run("assertion", func(t *testing.T) {
		result, err := RunWarmConnectScenario(scenarioContext, scenario, controller, platform, client)
		if err != nil {
			t.Fatalf("run direct Kotlin warm-connect scenario: %v", err)
		}
		if len(result.IdentityResolution) != len(scenario.NativeIdentityAliases) {
			t.Fatalf("Kotlin identity resolutions = %d, want %d", len(result.IdentityResolution), len(scenario.NativeIdentityAliases))
		}
		if len(result.WarmCall.Steps) != 2 || len(result.WarmCall.transportObservations) != 2 {
			t.Fatalf("Kotlin warm request count = %d, want 2", len(result.WarmCall.transportObservations))
		}
	})
}
