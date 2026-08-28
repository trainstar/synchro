package swift

import (
	"context"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const seededEmptyStartupScenarioID = "SCN-PERF-SEEDED-EMPTY-STARTUP-001"

// SeededStartupClientResult records one direct seeded or empty startup.
type SeededStartupClientResult struct {
	Client       Client
	Seeded       bool
	ArtifactStep *blackbox.NativeStepObservation
	StartupCall  SynchronizationResult
}

// SeededEmptyStartupResult records direct Swift evidence for all six clients.
type SeededEmptyStartupResult struct {
	Clients []SeededStartupClientResult
}

// RunSeededEmptyStartupScenario executes the authored seeded and empty startup flows through Swift.
func RunSeededEmptyStartupScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, artifact *blackbox.NativeArtifact, platform *Platform) (SeededEmptyStartupResult, error) {
	steps, err := swiftScenarioStepMap(scenario, seededEmptyStartupScenarioID, 15)
	if err != nil {
		return SeededEmptyStartupResult{}, err
	}
	if controller == nil || artifact == nil || platform == nil {
		return SeededEmptyStartupResult{}, errors.New("Swift seeded-startup dependencies are unavailable")
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SeededEmptyStartupResult{}, fmt.Errorf("install Swift seeded-startup contract: %w", err)
	}
	clients := make([]SeededStartupClientResult, 0, 6)
	for _, prefix := range []string{"seeded", "empty"} {
		for ordinal := 1; ordinal <= 3; ordinal++ {
			clientID := fmt.Sprintf("client-%s-%d", prefix, ordinal)
			userID := fmt.Sprintf("user-%s-%d", prefix, ordinal)
			client := Client{Key: clientID, UserID: userID, ClientID: clientID, DatabaseKey: "seeded-empty-startup-" + clientID}
			artifactID := ""
			assignmentNumber := 0
			if prefix == "seeded" {
				artifactID = fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", (ordinal-1)*3+1)
				assignmentNumber = parseStepNumber(artifactID) + 1
			} else {
				assignmentNumber = 10 + (ordinal-1)*2
			}
			assignmentID := fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", assignmentNumber)
			startupID := fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", parseStepNumber(assignmentID)+1)
			artifactStep := scenarios.Operation{}
			var err error
			var artifactObservation *blackbox.NativeStepObservation
			if prefix == "seeded" {
				artifactStep, err = swiftScenarioOperation(steps, artifactID, "artifact/install-portable-seed")
				if err != nil {
					return SeededEmptyStartupResult{}, err
				}
				observation, err := artifact.StageStep(ctx, artifactStep)
				if err != nil {
					return SeededEmptyStartupResult{}, fmt.Errorf("stage Swift seeded startup artifact %s: %w", artifactID, err)
				}
				artifactObservation = &observation
			}
			assignment, err := swiftScenarioOperation(steps, assignmentID, "model/set-client-assignments")
			if err != nil {
				return SeededEmptyStartupResult{}, err
			}
			if _, err := controller.ApplyStep(ctx, assignment); err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("assign Swift startup client %s: %w", clientID, err)
			}
			seeded := prefix == "seeded"
			seedPath := ""
			if seeded {
				seedPath, err = artifact.SeedDatabasePath(ctx, userID, clientID, scenarios.StepID(artifactID))
				if err != nil {
					return SeededEmptyStartupResult{}, fmt.Errorf("resolve Swift seeded startup artifact %s: %w", clientID, err)
				}
			}
			initialization := "empty"
			if seeded {
				initialization = "seed"
			}
			if err := platform.Install(ctx, client, initialization, seedPath); err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("install Swift %s startup client %s: %w", initialization, clientID, err)
			}
			_, err = swiftScenarioOperation(steps, startupID, "connect/send")
			if err != nil {
				return SeededEmptyStartupResult{}, err
			}
			binding := steps[scenarios.StepID(startupID)].NativeBinding
			if binding == nil || binding.Method != "start" || binding.Completion != "idle" || binding.UserID != userID || binding.ClientID != clientID {
				return SeededEmptyStartupResult{}, fmt.Errorf("Swift startup binding %s is invalid", startupID)
			}
			call, err := swiftScenarioCall(ctx, platform, client, "start")
			if err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("run Swift startup client %s: %w", clientID, err)
			}
			connect, err := swiftScenarioWire(call, "connect")
			if err != nil {
				return SeededEmptyStartupResult{}, err
			}
			if call.Completion != "idle" || connect.StatusCode != 200 || connect.Retryable {
				snapshot, captureErr := platform.captureSnapshot(ctx, client)
				if captureErr == nil && snapshot.Failure != nil {
					return SeededEmptyStartupResult{}, fmt.Errorf("Swift startup client %s completed %q with connect status %d; operation = %s, code = %s, recovery = %s", clientID, call.Completion, connect.StatusCode, snapshot.Failure.Operation, snapshot.Failure.Code, snapshot.Failure.RecoveryAction)
				}
				return SeededEmptyStartupResult{}, fmt.Errorf("Swift startup client %s completed %q with connect status %d; capture = %v", clientID, call.Completion, connect.StatusCode, captureErr)
			}
			result := SeededStartupClientResult{Client: client, Seeded: seeded, StartupCall: call}
			result.ArtifactStep = artifactObservation
			clients = append(clients, result)
		}
	}
	return SeededEmptyStartupResult{Clients: clients}, nil
}

func parseStepNumber(id string) int {
	if len(id) < 3 {
		return 0
	}
	var value int
	_, _ = fmt.Sscanf(id[len(id)-3:], "%03d", &value)
	return value
}
