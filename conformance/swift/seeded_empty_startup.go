package swift

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

// SeededStartupClientResult records one direct seeded or empty startup.
type SeededStartupClientResult struct {
	Client       Client
	Seeded       bool
	ArtifactStep *blackbox.NativeStepObservation
	StartupCall  SynchronizationResult
}

// SeededEmptyStartupResult records direct Swift evidence for all six clients.
type SeededEmptyStartupResult struct {
	Clients              []SeededStartupClientResult
	RejectedSeedControls []string
	RestartStep          StepObservation
	ResumeCall           SynchronizationResult
}

type swiftSeedStartupControl struct {
	name     string
	sql      string
	truncate bool
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
				seedPaths = append(seedPaths, seedPath)
				if ordinal == 3 {
					seedPath, err = makeSwiftSeedStartupMutant(ctx, seedPath, swiftTokenBindingControl())
					if err != nil {
						return SeededEmptyStartupResult{}, err
					}
					defer removeSwiftSeedStartupMutant(seedPath)
				}
			} else {
				if len(seedPaths) != 3 {
					return SeededEmptyStartupResult{}, errors.New("Swift seeded-startup control source is unavailable")
				}
				controls := swiftRejectedSeedControls(ordinal)
				observed, err := rejectSwiftSeedStartupMutants(ctx, platform, client, seedPaths[0], controls)
				if err != nil {
					return SeededEmptyStartupResult{}, err
				}
				rejectedControls = append(rejectedControls, observed...)
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
			expectedRebuildScopes := 2
			if seeded && ordinal != 3 {
				expectedRebuildScopes = 1
			}
			if err := validateSwiftSeededStartupTrace(call, boolCount(seeded), 2, expectedRebuildScopes); err != nil {
				return SeededEmptyStartupResult{}, fmt.Errorf("validate Swift startup client %s: %w", clientID, err)
			}
			result := SeededStartupClientResult{Client: client, Seeded: seeded, StartupCall: call}
			result.ArtifactStep = artifactObservation
			clients = append(clients, result)
		}
	}
	if len(rejectedControls) != 4 {
		return SeededEmptyStartupResult{}, fmt.Errorf("Swift rejected seed controls = %d, want 4", len(rejectedControls))
	}
	restartClient := clients[0].Client
	restartPayload, err := json.Marshal(map[string]string{"user_id": restartClient.UserID, "client_id": restartClient.ClientID})
	if err != nil {
		return SeededEmptyStartupResult{}, errors.New("encode Swift seeded-startup restart")
	}
	restart, err := platform.ProcessStep(ctx, restartClient, scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: restartPayload})
	if err != nil || restart.Disposition != "success" {
		return SeededEmptyStartupResult{}, fmt.Errorf("restart Swift seeded-startup client: %w", errors.Join(err, dispositionError(restart.Disposition)))
	}
	resumed, err := swiftScenarioCall(ctx, platform, restartClient, "start")
	if err != nil {
		return SeededEmptyStartupResult{}, fmt.Errorf("resume Swift seeded-startup client: %w", err)
	}
	if err := validateSwiftSeededStartupResume(clients[0].StartupCall, resumed, 2); err != nil {
		return SeededEmptyStartupResult{}, err
	}
	return SeededEmptyStartupResult{Clients: clients, RejectedSeedControls: rejectedControls, RestartStep: restart, ResumeCall: resumed}, nil
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func dispositionError(disposition string) error {
	if disposition == "success" {
		return nil
	}
	return fmt.Errorf("disposition is %q", disposition)
}

func swiftTokenBindingControl() swiftSeedStartupControl {
	return swiftSeedStartupControl{
		name: "token-binding",
		sql:  "UPDATE _synchro_seed_receipts SET receipt = substr(receipt, 1, length(receipt) - 43) || CASE substr(receipt, length(receipt) - 42, 1) WHEN 'A' THEN 'B' ELSE 'A' END || substr(receipt, length(receipt) - 41)",
	}
}

func swiftRejectedSeedControls(ordinal int) []swiftSeedStartupControl {
	switch ordinal {
	case 1:
		return []swiftSeedStartupControl{{name: "metadata-free", sql: "DELETE FROM _synchro_seed_receipts"}}
	case 2:
		return []swiftSeedStartupControl{{name: "receipt-binding", sql: `UPDATE _synchro_seed_receipts SET checksum = '{"algorithm":"sha256","version":1,"encoding":"hex","digest":"0000000000000000000000000000000000000000000000000000000000000000"}'`}}
	case 3:
		return []swiftSeedStartupControl{
			{name: "verification", sql: "UPDATE _synchro_meta SET value = '0' WHERE key = 'snapshot_complete'"},
			{name: "tamper", truncate: true},
		}
	default:
		return nil
	}
}

func rejectSwiftSeedStartupMutants(ctx context.Context, platform *Platform, client Client, source string, controls []swiftSeedStartupControl) ([]string, error) {
	observed := make([]string, 0, len(controls))
	for _, control := range controls {
		path, err := makeSwiftSeedStartupMutant(ctx, source, control)
		if err != nil {
			return nil, err
		}
		installErr := platform.Install(ctx, client, "seed", path)
		removeErr := removeSwiftSeedStartupMutant(path)
		if installErr == nil {
			return nil, fmt.Errorf("Swift seeded-startup %s control published a database", control.name)
		}
		var commandErr *CommandError
		if !errors.As(installErr, &commandErr) || commandErr.Code != "execution_failed" {
			return nil, fmt.Errorf("Swift seeded-startup %s control returned an unrelated failure: %w", control.name, installErr)
		}
		if removeErr != nil {
			return nil, removeErr
		}
		observed = append(observed, control.name)
	}
	return observed, nil
}

func makeSwiftSeedStartupMutant(ctx context.Context, source string, control swiftSeedStartupControl) (string, error) {
	input, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("open Swift seeded-startup %s source: %w", control.name, err)
	}
	defer input.Close()
	output, err := os.CreateTemp(filepath.Dir(source), ".swift-seed-control-*.sqlite")
	if err != nil {
		return "", fmt.Errorf("create Swift seeded-startup %s artifact: %w", control.name, err)
	}
	path := output.Name()
	keep := false
	defer func() {
		_ = output.Close()
		if !keep {
			_ = removeSwiftSeedStartupMutant(path)
		}
	}()
	if _, err := io.Copy(output, input); err != nil || output.Sync() != nil || output.Close() != nil {
		return "", fmt.Errorf("copy Swift seeded-startup %s artifact", control.name)
	}
	if control.truncate {
		if err := os.Truncate(path, 64); err != nil {
			return "", fmt.Errorf("tamper Swift seeded-startup artifact: %w", err)
		}
	} else {
		tool, err := exec.LookPath("sqlite3")
		if err != nil {
			return "", errors.New("Swift seeded-startup sqlite3 fault injector is unavailable")
		}
		command := exec.CommandContext(ctx, tool, path, control.sql+"; SELECT changes();")
		command.Stderr = io.Discard
		output, err := command.Output()
		if err != nil {
			return "", fmt.Errorf("apply Swift seeded-startup %s control: %w", control.name, err)
		}
		if strings.TrimSpace(string(output)) != "1" {
			return "", fmt.Errorf("Swift seeded-startup %s control did not mutate one row", control.name)
		}
	}
	keep = true
	return path, nil
}

func removeSwiftSeedStartupMutant(path string) error {
	var failures []error
	for _, candidate := range []string{path, path + "-journal", path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return fmt.Errorf("remove Swift seeded-startup control artifact: %w", err)
	}
	return nil
}

func validateSwiftSeededStartupTrace(call SynchronizationResult, expectedConnectScopes, expectedPullScopes, expectedRebuildScopes int) error {
	observations := call.transportObservations
	if call.Completion != "idle" || len(observations) < expectedRebuildScopes+2 || observations[0].OperationClass != "connect" || observations[len(observations)-1].OperationClass != "pull" {
		return errors.New("startup request sequence is invalid")
	}
	for _, observation := range observations {
		if observation.StatusCode != 200 || observation.Retryable || observation.ErrorCode != nil {
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
		if !containsSwiftSeedFingerprint(pull.CursorFingerprints, cursor) {
			return errors.New("startup pull omitted a rebuilt cursor")
		}
	}
	return nil
}

func validateSwiftSeededStartupResume(initial, resumed SynchronizationResult, expectedScopes int) error {
	if len(resumed.transportObservations) != 2 {
		return fmt.Errorf("Swift seeded-startup resume request count = %d, want 2", len(resumed.transportObservations))
	}
	if err := validateSwiftSeededStartupTrace(resumed, expectedScopes, expectedScopes, 0); err != nil {
		return fmt.Errorf("validate Swift seeded-startup resume: %w", err)
	}
	initialPull := initial.transportObservations[len(initial.transportObservations)-1]
	resumedPull := resumed.transportObservations[1]
	if initialPull.PullResponseFacts == nil || !equalSwiftSeedFingerprints(resumedPull.CursorFingerprints, initialPull.PullResponseFacts.ScopeCursorFingerprints) {
		return errors.New("Swift seeded-startup restart did not resume from durable cursors")
	}
	return nil
}

func containsSwiftSeedFingerprint(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func equalSwiftSeedFingerprints(left, right []string) bool {
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

func parseStepNumber(id string) int {
	if len(id) < 3 {
		return 0
	}
	var value int
	_, _ = fmt.Sscanf(id[len(id)-3:], "%03d", &value)
	return value
}
