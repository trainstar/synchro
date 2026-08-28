package kotlin

import (
	"context"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const seededEmptyStartupScenarioID = "SCN-PERF-SEEDED-EMPTY-STARTUP-001"

// SeededStartupClientResult records one direct seeded or empty Kotlin Android startup.
type SeededStartupClientResult struct {
	Client       Client
	Seeded       bool
	ArtifactStep *blackbox.NativeStepObservation
	StartupCall  SynchronizationResult
}

// SeededEmptyStartupResult records direct Kotlin Android evidence for all startup clients.
type SeededEmptyStartupResult struct {
	Clients []SeededStartupClientResult
}

// RunSeededEmptyStartupScenario executes the authored seeded and empty startup flows through Kotlin Android.
func RunSeededEmptyStartupScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, artifact *blackbox.NativeArtifact, platform *Platform) (SeededEmptyStartupResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, seededEmptyStartupScenarioID, 15)
	if err != nil {
		return SeededEmptyStartupResult{}, err
	}
	if controller == nil || artifact == nil || platform == nil {
		return SeededEmptyStartupResult{}, errors.New("Kotlin Android seeded-startup dependencies are unavailable")
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SeededEmptyStartupResult{}, fmt.Errorf("install Kotlin Android seeded-startup contract: %w", err)
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
				assignmentNumber = parseStartupStepNumber(artifactID) + 1
			} else {
				assignmentNumber = 10 + (ordinal-1)*2
			}
			assignmentID := fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", assignmentNumber)
			startupID := fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", parseStartupStepNumber(assignmentID)+1)
			var artifactObservation *blackbox.NativeStepObservation
			if prefix == "seeded" {
				artifactStep, err := kotlinScenarioOperation(steps, artifactID, "artifact/install-portable-seed")
				if err != nil {
					return SeededEmptyStartupResult{}, err
				}
				observation, err := artifact.StageStep(ctx, artifactStep)
				if err != nil {
					return SeededEmptyStartupResult{}, fmt.Errorf("stage Kotlin Android seeded startup artifact %s: %w", artifactID, err)
				}
				artifactObservation = &observation
			}
			assignment, err := kotlinScenarioOperation(steps, assignmentID, "model/set-client-assignments")
			if err != nil {
				return SeededEmptyStartupResult{}, err
			}
			if observation, err := controller.ApplyStep(ctx, assignment); err != nil || observation.Disposition != "success" {
				return SeededEmptyStartupResult{}, fmt.Errorf("assign Kotlin Android startup client %s: %w", clientID, kotlinResultError(err, observation.Disposition))
			}
			seeded := prefix == "seeded"
			seedPath := ""
			if seeded {
				seedPath, err = artifact.SeedDatabasePath(ctx, userID, clientID, scenarios.StepID(artifactID))
				if err != nil {
					return SeededEmptyStartupResult{}, fmt.Errorf("resolve Kotlin Android seeded startup artifact %s: %w", clientID, err)
				}
			}
			initialization := "empty"
			if seeded {
				initialization = "seed"
			}
			if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: initialization, SeedPath: seedPath}); err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("install Kotlin Android %s startup client %s: %w", initialization, clientID, err)
			}
			if _, err := kotlinScenarioOperation(steps, startupID, "connect/send"); err != nil {
				return SeededEmptyStartupResult{}, err
			}
			binding := steps[scenarios.StepID(startupID)].NativeBinding
			if binding == nil || binding.Method != "start" || binding.Completion != "idle" || binding.UserID != userID || binding.ClientID != clientID {
				return SeededEmptyStartupResult{}, fmt.Errorf("Kotlin Android startup binding %s is invalid", startupID)
			}
			call, err := kotlinScenarioCall(ctx, platform, client, "start")
			if err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("run Kotlin Android startup client %s: %w", clientID, err)
			}
			connect, err := kotlinScenarioWire(call, "connect")
			if err != nil {
				return SeededEmptyStartupResult{}, err
			}
			if call.Completion != "idle" || connect.StatusCode != 200 || connect.Retryable == nil || *connect.Retryable {
				return SeededEmptyStartupResult{}, fmt.Errorf("Kotlin Android startup client %s completed %q with connect status %d", clientID, call.Completion, connect.StatusCode)
			}
			if err := validateKotlinWireExpectation(scenario, startupID, "connect", call); err != nil {
				return SeededEmptyStartupResult{}, err
			}
			clients = append(clients, SeededStartupClientResult{Client: client, Seeded: seeded, ArtifactStep: artifactObservation, StartupCall: call})
		}
	}
	return SeededEmptyStartupResult{Clients: clients}, nil
}

func parseStartupStepNumber(id string) int {
	if len(id) < 3 {
		return 0
	}
	var value int
	_, _ = fmt.Sscanf(id[len(id)-3:], "%03d", &value)
	return value
}
