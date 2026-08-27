package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const configuredBoundsScenarioPath = "conformance/scenarios/performance/configured-bounds-001.json"

type configuredBoundsRunner struct {
	t           *testing.T
	ctx         context.Context
	harness     *blackbox.Harness
	token       string
	ownerField  string
	repetitions map[string]int
}

type configuredBoundValue struct {
	BoundFamily string `json:"bound_family"`
	Boundary    string `json:"boundary"`
	Value       int    `json:"value"`
}

func TestRealConfiguredBoundsMeasurement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)

	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := scenarios.LoadFile(ctx, repoRoot, configuredBoundsScenarioPath)
	if err != nil {
		t.Fatalf("load configured-bounds scenario: %v", err)
	}
	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		t.Fatalf("load conformance contract: %v", err)
	}
	catalog, err := bundle.PerformanceCatalogBinding()
	if err != nil {
		t.Fatalf("bind performance catalog: %v", err)
	}
	definition, found := catalog.RequiredMeasurement("MEAS-CONFIGURED-BOUNDS-001")
	if !found {
		t.Fatal("configured-bounds measurement definition is missing")
	}
	obligationID, supportCellID, err := configuredBoundsProofIdentity()
	if err != nil {
		t.Fatal(err)
	}

	runner := configuredBoundsRunner{
		t:           t,
		ctx:         ctx,
		harness:     harness,
		token:       token,
		ownerField:  loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id"),
		repetitions: make(map[string]int),
	}
	observations := make([]scenarios.MeasurementObservation, 0, len(scenario.MeasurementBindings))
	for index, binding := range scenario.MeasurementBindings {
		observation, err := runner.observe(binding.MeasurementSample.Operation, definition, index)
		if err != nil {
			t.Fatalf("execute configured-bounds sample %d: %v", index+1, err)
		}
		observations = append(observations, observation)
	}

	definitions := []contract.RequiredMeasurement{definition}
	if err := scenarios.ValidateMeasurementObservationClosure(
		scenario,
		obligationID,
		supportCellID,
		definitions,
		observations,
	); err != nil {
		t.Fatalf("validate configured-bounds observation closure: %v", err)
	}

	controls := []struct {
		name     string
		mutate   func(*scenarios.MeasurementObservation)
		category scenarios.MeasurementClosureFailureCategory
	}{
		{"operation ID", func(observation *scenarios.MeasurementObservation) {
			observation.Operation.ID = "MOP-CONFIGURED-BOUNDS-WRONG-001"
		}, scenarios.MeasurementClosureOperationIDMismatch},
		{"operation family", func(observation *scenarios.MeasurementObservation) {
			observation.Operation.Family = "impact"
		}, scenarios.MeasurementClosureOperationFamilyMismatch},
		{"operation boundary", func(observation *scenarios.MeasurementObservation) {
			observation.Operation.Boundary = "invalid"
		}, scenarios.MeasurementClosureOperationBoundaryMismatch},
	}
	for _, control := range controls {
		t.Run("rejects wrong "+control.name, func(t *testing.T) {
			mutated := append([]scenarios.MeasurementObservation(nil), observations...)
			control.mutate(&mutated[0])
			err := scenarios.ValidateMeasurementObservationClosure(
				scenario,
				obligationID,
				supportCellID,
				definitions,
				mutated,
			)
			var closureFailure scenarios.MeasurementClosureFailure
			if !errors.As(err, &closureFailure) || closureFailure.Category != control.category {
				t.Fatalf("wrong-%s mutant was not rejected by %s: %v", control.name, control.category, err)
			}
		})
	}
}

func (runner *configuredBoundsRunner) observe(
	operation scenarios.MeasurementOperationTarget,
	definition contract.RequiredMeasurement,
	index int,
) (scenarios.MeasurementObservation, error) {
	var authored configuredBoundValue
	if err := json.Unmarshal(operation.Value, &authored); err != nil {
		return scenarios.MeasurementObservation{}, errors.New("decode configured bound value failed")
	}
	maximum, err := configuredBoundMaximum(operation.Family)
	if err != nil {
		return scenarios.MeasurementObservation{}, err
	}
	boundary, err := classifyConfiguredBoundary(authored.Value, maximum)
	if err != nil {
		return scenarios.MeasurementObservation{}, err
	}
	accepted, err := runner.exercise(operation.Family, authored.Value, index)
	if err != nil {
		return scenarios.MeasurementObservation{}, err
	}
	wantAccepted := boundary != "invalid"
	if accepted != wantAccepted {
		return scenarios.MeasurementObservation{}, fmt.Errorf(
			"%s %s boundary acceptance = %t, want %t",
			operation.Family,
			boundary,
			accepted,
			wantAccepted,
		)
	}

	familyID := configuredBoundFamilyID(operation.Family)
	repetitionKey := operation.Family + "|" + boundary
	runner.repetitions[repetitionKey]++
	repetition := runner.repetitions[repetitionKey]
	observedValue, err := json.Marshal(configuredBoundValue{
		BoundFamily: operation.Family,
		Boundary:    boundary,
		Value:       authored.Value,
	})
	if err != nil {
		return scenarios.MeasurementObservation{}, errors.New("encode configured bound value failed")
	}
	metrics, err := configuredBoundMetrics(definition, boundary)
	if err != nil {
		return scenarios.MeasurementObservation{}, err
	}
	return scenarios.MeasurementObservation{
		StepID: "STEP-PERF-CONFIGURED-BOUNDS-001",
		Operation: scenarios.MeasurementOperationTarget{
			ID:       scenarios.MeasurementOperationID(fmt.Sprintf("MOP-CONFIGURED-BOUNDS-%s-%s-%03d", familyID, strings.ToUpper(boundary), repetition)),
			Family:   operation.Family,
			Boundary: boundary,
			Value:    observedValue,
		},
		MeasurementID: "MEAS-CONFIGURED-BOUNDS-001",
		StratumID:     contract.StratumID(fmt.Sprintf("STR-%s-%s-001", familyID, strings.ToUpper(boundary))),
		SampleID:      fmt.Sprintf("SAMPLE-CONFIGURED-BOUNDS-%s-%s-%03d", familyID, strings.ToUpper(boundary), repetition),
		Metrics:       metrics,
	}, nil
}

func (runner *configuredBoundsRunner) exercise(family string, value, index int) (bool, error) {
	switch family {
	case "fanout":
		return runner.harness.Operator().ExerciseConfiguredFanoutLimit(runner.ctx, value)
	case "impact":
		return runner.harness.Operator().ExerciseConfiguredImpactLimit(runner.ctx, value)
	case "pull":
		return runner.exercisePull(value, index)
	case "rebuild":
		return runner.exerciseRebuild(value, index)
	case "compaction":
		return runner.harness.Operator().ExerciseConfiguredCompactionLimit(runner.ctx, value)
	case "backfill":
		return runner.harness.Operator().ExerciseConfiguredBackfillLimit(runner.ctx, value)
	case "push_mutations":
		return runner.exercisePush(value, index)
	default:
		return false, fmt.Errorf("configured bound family %q is unsupported", family)
	}
}

func (runner *configuredBoundsRunner) exercisePull(value, index int) (bool, error) {
	client := connectRealProtocolClient(runner.t, runner.ctx, runner.harness, runner.token, fmt.Sprintf("configured-pull-%03d", index))
	status, response := postSync(
		runner.t,
		runner.ctx,
		runner.harness.AdapterURL(),
		runner.token,
		"/sync/pull",
		realPullPayload(client, client.Scopes, value),
	)
	switch status {
	case http.StatusOK:
		if _, ok := response["changes"].([]any); !ok {
			return false, errors.New("configured pull response changes are invalid")
		}
		return true, nil
	case http.StatusBadRequest:
		assertPhase4ProtocolError(runner.t, status, response, http.StatusBadRequest, "invalid_request")
		return false, nil
	default:
		return false, fmt.Errorf("configured pull status = %d", status)
	}
}

func (runner *configuredBoundsRunner) exerciseRebuild(value, index int) (bool, error) {
	client := connectRealProtocolClient(runner.t, runner.ctx, runner.harness, runner.token, fmt.Sprintf("configured-rebuild-%03d", index))
	status, response := requestRealRebuildPage(
		runner.t,
		runner.ctx,
		runner.harness,
		runner.token,
		client,
		"cf:global",
		fmt.Sprintf("00000000-0000-4000-f%03x-000000000001", index),
		nil,
		value,
	)
	switch status {
	case http.StatusOK:
		if response["scope"] != "cf:global" {
			return false, errors.New("configured rebuild returned the wrong scope")
		}
		if _, ok := response["records"].([]any); !ok {
			return false, errors.New("configured rebuild records are invalid")
		}
		return true, nil
	case http.StatusBadRequest:
		assertPhase4ProtocolError(runner.t, status, response, http.StatusBadRequest, "invalid_request")
		return false, nil
	default:
		return false, fmt.Errorf("configured rebuild status = %d", status)
	}
}

func (runner *configuredBoundsRunner) exercisePush(value, index int) (bool, error) {
	client := connectRealProtocolClient(runner.t, runner.ctx, runner.harness, runner.token, fmt.Sprintf("configured-push-%03d", index))
	table := requireRealTable(runner.t, client, "cf_items")
	recordGroup := fmt.Sprintf("c%03x", index)
	mutationGroup := fmt.Sprintf("d%03x", index)
	mutations, recordIDs := phase4BoundedInsertMutations(
		client,
		table,
		runner.ownerField,
		value,
		recordGroup,
		mutationGroup,
		"configured-bound",
	)
	status, response := postSync(
		runner.t,
		runner.ctx,
		runner.harness.AdapterURL(),
		runner.token,
		"/sync/push",
		phase4PushPayload(
			client,
			fmt.Sprintf("00000000-0000-4000-e%03x-000000000001", index),
			mutations,
		),
	)
	accepted := status == http.StatusOK
	if accepted {
		acceptedOutcomes := requireOutcomeList(runner.t, response, "accepted")
		rejectedOutcomes := requireOutcomeList(runner.t, response, "rejected")
		if len(acceptedOutcomes) != value || len(rejectedOutcomes) != 0 {
			return false, errors.New("configured push outcomes are invalid")
		}
	} else if status == http.StatusBadRequest {
		assertPhase4ProtocolError(runner.t, status, response, http.StatusBadRequest, "invalid_request")
	} else {
		return false, fmt.Errorf("configured push status = %d", status)
	}

	observation, err := runner.harness.Operator().ObserveDiagnosticPush(runner.ctx, client.ID, recordIDs)
	if err != nil {
		return false, fmt.Errorf("observe configured push state: %w", err)
	}
	wantBatches, wantMutations, wantRows, wantEpoch := int64(0), int64(0), int64(0), int64(1)
	if accepted {
		wantBatches = 1
		wantMutations = int64(value)
		wantRows = int64(value)
		wantEpoch = 2
	}
	if observation.BatchCount != wantBatches || observation.MutationCount != wantMutations ||
		observation.SourceRowCount != wantRows || observation.AcceptedWriteEpoch != wantEpoch {
		return false, errors.New("configured push durable state is invalid")
	}
	return accepted, nil
}

func configuredBoundsProofIdentity() (contract.ObligationID, contract.SupportCellID, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "linux/amd64":
		return "OBL-PERF-CONFIGURED-BOUNDS-PG-LINUX-X64-001", "SUP-PG-LINUX-X64-001", nil
	case "darwin/arm64":
		return "OBL-PERF-CONFIGURED-BOUNDS-PG-MACOS-ARM64-001", "SUP-PG-MACOS-ARM64-001", nil
	default:
		return "", "", fmt.Errorf("configured-bounds proof has no support cell for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
}

func configuredBoundMaximum(family string) (int, error) {
	switch family {
	case "fanout":
		return 8, nil
	case "impact", "pull", "rebuild", "backfill", "push_mutations":
		return 1000, nil
	case "compaction":
		return 10000, nil
	default:
		return 0, fmt.Errorf("configured bound family %q is unsupported", family)
	}
}

func classifyConfiguredBoundary(value, maximum int) (string, error) {
	switch value {
	case 1:
		return "lower", nil
	case maximum:
		return "upper", nil
	case maximum + 1:
		return "invalid", nil
	default:
		return "", fmt.Errorf("configured bound value %d does not select a declared boundary", value)
	}
}

func configuredBoundFamilyID(family string) string {
	if family == "push_mutations" {
		return "PUSH"
	}
	return strings.ToUpper(family)
}

func configuredBoundMetrics(definition contract.RequiredMeasurement, boundary string) ([]scenarios.MeasurementMetricValue, error) {
	values := make([]scenarios.MeasurementMetricValue, 0, len(definition.Metrics))
	for _, metric := range definition.Metrics {
		value := float64(0)
		switch metric.Name {
		case "lower_bound_acceptance":
			if boundary == "lower" {
				value = 1
			}
		case "upper_bound_acceptance":
			if boundary == "upper" {
				value = 1
			}
		case "invalid_bound_rejection":
			if boundary == "invalid" {
				value = 1
			}
		default:
			return nil, fmt.Errorf("configured-bounds metric %q is unsupported", metric.Name)
		}
		values = append(values, scenarios.MeasurementMetricValue{MetricID: metric.ID, Value: value})
	}
	return values, nil
}
