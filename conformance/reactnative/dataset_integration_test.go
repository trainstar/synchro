//go:build reactnativeintegration

package reactnative

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/dataset"
)

// datasetExchangeBound bounds the device command loop. The authored flow
// issues fewer commands, and the device stops at the complete response.
const datasetExchangeBound = 64

// runRealReactNativeDataset runs the authored training dataset flow through
// the React Native bridge on one platform.
func runRealReactNativeDataset(t *testing.T, platform string) {
	t.Helper()
	if !*warmConnectProvision || !*warmConnectInstall {
		t.Fatalf("React Native %s dataset requires --provision --install", platform)
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
	environment, err := blackbox.LoadLocalEnvironment()
	if err != nil {
		t.Fatalf("load React Native dataset environment: %v", err)
	}
	provisionContext, cancelProvision := context.WithTimeout(runContext, 2*time.Minute)
	harness, err := blackbox.Provision(provisionContext, blackbox.HarnessConfig{Environment: environment})
	cancelProvision()
	if err != nil {
		t.Fatalf("provision React Native dataset harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close React Native dataset harness: %v", err)
		}
	})
	if err := harness.ApplySourceSetup(runContext, blackbox.SourceSetup{Name: "dataset", SchemaSQL: dataset.SchemaSQL, RegistrationSQL: dataset.RegistrationSQL, Tables: dataset.TableNames()}); err != nil {
		t.Fatalf("register the React Native dataset: %v", err)
	}
	source, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open the React Native dataset source: %v", err)
	}
	defer source.Close()
	coordinator, err := NewDatasetCoordinator(harness, platform)
	if err != nil {
		t.Fatalf("create React Native %s dataset coordinator: %v", platform, err)
	}
	defer coordinator.Close()
	coordinator.Start(runContext, source, t.Logf)

	resultPath := filepath.Join(t.TempDir(), "react-native-"+platform+"-dataset.json")
	command, err := newCorpusDetoxCommand(runContext, "test", "e2e/dataset.test.ts", "--config-path", "./.detoxrc.steady-pull.js", "--configuration", detoxConfiguration, "--json", "--outputFile", resultPath)
	if err != nil {
		t.Fatalf("create React Native %s dataset Detox command: %v", platform, err)
	}
	command.Dir = filepath.Join(repositoryRoot, "clients", "react-native", "example")
	for _, assignment := range os.Environ() {
		if !strings.HasPrefix(assignment, "SYNCHRO_RN_COORDINATOR_") {
			command.Env = append(command.Env, assignment)
		}
	}
	command.Env = append(command.Env, "SYNCHRO_RN_COORDINATOR_URL="+coordinator.URL(), "SYNCHRO_RN_COORDINATOR_TOKEN="+coordinator.Token(), "SYNCHRO_RN_COORDINATOR_STAGE_COUNT="+strconv.Itoa(datasetExchangeBound))
	output, err := command.CombinedOutput()
	if flowErr := coordinator.Result(); flowErr != nil {
		t.Fatalf("React Native %s dataset flow: %v\n%s", platform, flowErr, output)
	}
	if err != nil {
		t.Fatalf("run React Native %s dataset Detox test: %v\n%s", platform, err, output)
	}
	expectedTestPath := filepath.Join(repositoryRoot, "clients", "react-native", "example", "e2e", "dataset.test.ts")
	if err := validateDetoxSingleTestResult(resultPath, expectedTestPath, "executes the dataset coordinator sequence", "dataset"); err != nil {
		t.Fatalf("validate React Native %s dataset Detox result: %v\n%s", platform, err, output)
	}
}
