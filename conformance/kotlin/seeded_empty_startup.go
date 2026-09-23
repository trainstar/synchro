package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	Clients              []SeededStartupClientResult
	RejectedSeedControls []string
	RestartStep          StepObservation
	ResumeCall           SynchronizationResult
}

type kotlinSeedStartupControl struct {
	name     string
	sql      string
	truncate bool
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
	seedPaths := make([]string, 0, 3)
	rejectedControls := make([]string, 0, 4)
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
				seedPaths = append(seedPaths, seedPath)
				if ordinal == 3 {
					seedPath, err = makeKotlinSeedStartupMutant(ctx, seedPath, kotlinTokenBindingControl())
					if err != nil {
						return SeededEmptyStartupResult{}, err
					}
					defer removeKotlinSeedStartupMutant(seedPath)
				}
			} else {
				if len(seedPaths) != 3 {
					return SeededEmptyStartupResult{}, errors.New("Kotlin Android seeded-startup control source is unavailable")
				}
				controls := kotlinRejectedSeedControls(ordinal)
				observed, err := rejectKotlinSeedStartupMutants(ctx, platform, client, seedPaths[0], controls)
				if err != nil {
					return SeededEmptyStartupResult{}, err
				}
				rejectedControls = append(rejectedControls, observed...)
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
			expectedRebuildScopes := 2
			if seeded && ordinal != 3 {
				expectedRebuildScopes = 1
			}
			if err := validateKotlinSeededStartupTrace(call, kotlinBoolCount(seeded), 2, expectedRebuildScopes); err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("validate Kotlin Android startup client %s: %w", clientID, err)
			}
			clients = append(clients, SeededStartupClientResult{Client: client, Seeded: seeded, ArtifactStep: artifactObservation, StartupCall: call})
		}
	}
	if len(rejectedControls) != 4 {
		return SeededEmptyStartupResult{}, fmt.Errorf("Kotlin Android rejected seed controls = %d, want 4", len(rejectedControls))
	}
	restartClient := clients[0].Client
	restartPayload, err := json.Marshal(map[string]string{"user_id": restartClient.UserID, "client_id": restartClient.ClientID})
	if err != nil {
		return SeededEmptyStartupResult{}, errors.New("encode Kotlin Android seeded-startup restart")
	}
	restart, err := platform.ProcessStep(ctx, restartClient, scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: restartPayload})
	if err != nil || restart.Disposition != "success" {
		return SeededEmptyStartupResult{}, fmt.Errorf("restart Kotlin Android seeded-startup client: %w", kotlinResultError(err, restart.Disposition))
	}
	resumed, err := kotlinScenarioCall(ctx, platform, restartClient, "start")
	if err != nil {
		return SeededEmptyStartupResult{}, fmt.Errorf("resume Kotlin Android seeded-startup client: %w", err)
	}
	if err := validateKotlinSeededStartupResume(clients[0].StartupCall, resumed, 2); err != nil {
		return SeededEmptyStartupResult{}, err
	}
	return SeededEmptyStartupResult{Clients: clients, RejectedSeedControls: rejectedControls, RestartStep: restart, ResumeCall: resumed}, nil
}

func kotlinBoolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func kotlinTokenBindingControl() kotlinSeedStartupControl {
	return kotlinSeedStartupControl{
		name: "token-binding",
		sql:  "UPDATE _synchro_seed_receipts SET receipt = substr(receipt, 1, length(receipt) - 43) || CASE substr(receipt, length(receipt) - 42, 1) WHEN 'A' THEN 'B' ELSE 'A' END || substr(receipt, length(receipt) - 41)",
	}
}

func kotlinRejectedSeedControls(ordinal int) []kotlinSeedStartupControl {
	switch ordinal {
	case 1:
		return []kotlinSeedStartupControl{{name: "metadata-free", sql: "DELETE FROM _synchro_seed_receipts"}}
	case 2:
		return []kotlinSeedStartupControl{{name: "receipt-binding", sql: `UPDATE _synchro_seed_receipts SET checksum = '{"algorithm":"sha256","version":1,"encoding":"hex","digest":"0000000000000000000000000000000000000000000000000000000000000000"}'`}}
	case 3:
		return []kotlinSeedStartupControl{
			{name: "verification", sql: "UPDATE _synchro_meta SET value = '0' WHERE key = 'snapshot_complete'"},
			{name: "tamper", truncate: true},
		}
	default:
		return nil
	}
}

func rejectKotlinSeedStartupMutants(ctx context.Context, platform *Platform, client Client, source string, controls []kotlinSeedStartupControl) ([]string, error) {
	observed := make([]string, 0, len(controls))
	for _, control := range controls {
		path, err := makeKotlinSeedStartupMutant(ctx, source, control)
		if err != nil {
			return nil, err
		}
		installErr := platform.Install(ctx, InstallRequest{Client: client, Initialization: "seed", SeedPath: path})
		removeErr := removeKotlinSeedStartupMutant(path)
		if installErr == nil {
			return nil, fmt.Errorf("Kotlin Android seeded-startup %s control published a database", control.name)
		}
		var commandErr *CommandError
		if !errors.As(installErr, &commandErr) || commandErr.Code != "execution_failed" || !strings.Contains(commandErr.Detail, "Seed database") {
			return nil, fmt.Errorf("Kotlin Android seeded-startup %s control returned an unrelated failure: %w", control.name, installErr)
		}
		if removeErr != nil {
			return nil, removeErr
		}
		observed = append(observed, control.name)
	}
	return observed, nil
}

func makeKotlinSeedStartupMutant(ctx context.Context, source string, control kotlinSeedStartupControl) (string, error) {
	input, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("open Kotlin Android seeded-startup %s source: %w", control.name, err)
	}
	defer input.Close()
	output, err := os.CreateTemp(filepath.Dir(source), ".kotlin-seed-control-*.sqlite")
	if err != nil {
		return "", fmt.Errorf("create Kotlin Android seeded-startup %s artifact: %w", control.name, err)
	}
	path := output.Name()
	keep := false
	defer func() {
		_ = output.Close()
		if !keep {
			_ = removeKotlinSeedStartupMutant(path)
		}
	}()
	if _, err := io.Copy(output, input); err != nil || output.Sync() != nil || output.Close() != nil {
		return "", fmt.Errorf("copy Kotlin Android seeded-startup %s artifact", control.name)
	}
	if control.truncate {
		if err := os.Truncate(path, 64); err != nil {
			return "", fmt.Errorf("tamper Kotlin Android seeded-startup artifact: %w", err)
		}
	} else {
		tool, err := exec.LookPath("sqlite3")
		if err != nil {
			return "", errors.New("Kotlin Android seeded-startup sqlite3 fault injector is unavailable")
		}
		command := exec.CommandContext(ctx, tool, path, control.sql+"; SELECT changes();")
		command.Stderr = io.Discard
		output, err := command.Output()
		if err != nil {
			return "", fmt.Errorf("apply Kotlin Android seeded-startup %s control: %w", control.name, err)
		}
		if strings.TrimSpace(string(output)) != "1" {
			return "", fmt.Errorf("Kotlin Android seeded-startup %s control did not mutate one row", control.name)
		}
	}
	keep = true
	return path, nil
}

func removeKotlinSeedStartupMutant(path string) error {
	var failures []error
	for _, candidate := range []string{path, path + "-journal", path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return fmt.Errorf("remove Kotlin Android seeded-startup control artifact: %w", err)
	}
	return nil
}

func validateKotlinSeededStartupTrace(call SynchronizationResult, expectedConnectScopes, expectedPullScopes, expectedRebuildScopes int) error {
	observations := call.transportObservations
	if call.Completion != "idle" || len(observations) < expectedRebuildScopes+2 || observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return errors.New("startup request sequence is invalid")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable == nil || *observation.Retryable || observation.ErrorCode != nil {
			return errors.New("startup request did not succeed")
		}
	}
	connect := observations[0]
	pull := observations[len(observations)-1]
	if connect.RequestFacts == nil || connect.RequestFacts.ScopeCount == nil || *connect.RequestFacts.ScopeCount != expectedConnectScopes ||
		pull.RequestFacts == nil || pull.RequestFacts.ScopeCount == nil || *pull.RequestFacts.ScopeCount != expectedPullScopes ||
		pull.CursorFingerprintsComplete == nil || !*pull.CursorFingerprintsComplete || len(pull.CursorFingerprints) != expectedPullScopes ||
		pull.PullResponseFacts == nil || pull.PullResponseFacts.HasMore || pull.PullResponseFacts.RebuildScopeCount != 0 ||
		pull.PullResponseFacts.ChecksumCount != expectedPullScopes || !pull.PullResponseFacts.ScopeCursorFingerprintsComplete ||
		len(pull.PullResponseFacts.ScopeCursorFingerprints) != expectedPullScopes {
		return errors.New("startup scope continuation facts are invalid")
	}
	type rebuildState struct {
		id       string
		complete bool
	}
	rebuilds := make(map[string]rebuildState, expectedRebuildScopes)
	finalCursors := make(map[string]struct{}, expectedRebuildScopes)
	activeScope := ""
	for _, observation := range observations[1 : len(observations)-1] {
		if observation.OperationClass != "rebuild" || observation.RequestFacts == nil || observation.RequestFacts.ScopeFingerprint == nil ||
			observation.RequestFacts.RebuildIDFingerprint == nil || observation.RebuildResponseFacts == nil ||
			observation.RebuildResponseFacts.ScopeFingerprint != *observation.RequestFacts.ScopeFingerprint {
			return errors.New("startup rebuild facts are invalid")
		}
		scope := *observation.RequestFacts.ScopeFingerprint
		state, found := rebuilds[scope]
		if !found {
			if activeScope != "" {
				return errors.New("startup rebuild scopes overlap")
			}
			state.id = *observation.RequestFacts.RebuildIDFingerprint
			activeScope = scope
		} else if state.complete || activeScope != scope || state.id != *observation.RequestFacts.RebuildIDFingerprint {
			return errors.New("startup rebuild continuation is invalid")
		}
		response := observation.RebuildResponseFacts
		if response.HasMore {
			if !response.HasCursor || response.HasFinalScopeCursor || response.HasChecksum || response.FinalScopeCursorFingerprint != nil {
				return errors.New("startup intermediate rebuild finality is invalid")
			}
		} else {
			if response.HasCursor || !response.HasFinalScopeCursor || !response.HasChecksum || response.FinalScopeCursorFingerprint == nil {
				return errors.New("startup terminal rebuild finality is invalid")
			}
			state.complete = true
			activeScope = ""
			finalCursors[*response.FinalScopeCursorFingerprint] = struct{}{}
		}
		rebuilds[scope] = state
	}
	if activeScope != "" || len(rebuilds) != expectedRebuildScopes || len(finalCursors) != expectedRebuildScopes {
		return errors.New("startup rebuilt scope count is invalid")
	}
	for cursor := range finalCursors {
		if !containsKotlinSeedFingerprint(pull.CursorFingerprints, cursor) {
			return errors.New("startup pull omitted a rebuilt cursor")
		}
	}
	return nil
}

func validateKotlinSeededStartupResume(initial, resumed SynchronizationResult, expectedScopes int) error {
	if len(resumed.transportObservations) != 2 {
		return fmt.Errorf("Kotlin Android seeded-startup resume request count = %d, want 2", len(resumed.transportObservations))
	}
	if err := validateKotlinSeededStartupTrace(resumed, expectedScopes, expectedScopes, 0); err != nil {
		return fmt.Errorf("validate Kotlin Android seeded-startup resume: %w", err)
	}
	initialPull := initial.transportObservations[len(initial.transportObservations)-1]
	resumedPull := resumed.transportObservations[1]
	if initialPull.PullResponseFacts == nil || !equalKotlinSeedFingerprints(resumedPull.CursorFingerprints, initialPull.PullResponseFacts.ScopeCursorFingerprints) {
		return errors.New("Kotlin Android seeded-startup restart did not resume from durable cursors")
	}
	return nil
}

func containsKotlinSeedFingerprint(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func equalKotlinSeedFingerprints(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func parseStartupStepNumber(id string) int {
	if len(id) < 3 {
		return 0
	}
	var value int
	_, _ = fmt.Sscanf(id[len(id)-3:], "%03d", &value)
	return value
}
