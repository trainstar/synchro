package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const configuredBoundsScenarioPath = "conformance/scenarios/performance/configured-bounds-001.json"

type configuredBoundsRunner struct {
	t          *testing.T
	ctx        context.Context
	harness    *blackbox.Harness
	token      string
	ownerField string
}

type configuredBoundValue struct {
	BoundFamily string `json:"bound_family"`
	Boundary    string `json:"boundary"`
	Value       int    `json:"value"`
}

// configuredBoundServerFact contains only terminal facts observed from the
// extension SQL boundary or the extension-backed protocol boundary.
type configuredBoundServerFact struct {
	Accepted   bool
	HTTPStatus int
	SQLState   string
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

	declarations := configuredBoundDeclarations(t, scenario.MeasurementBindings)
	ledger, err := blackbox.NewMeasurementBindingLedger(declarations)
	if err != nil {
		t.Fatalf("load configured-bounds bindings: %v", err)
	}
	runner := configuredBoundsRunner{
		t:          t,
		ctx:        ctx,
		harness:    harness,
		token:      token,
		ownerField: loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id"),
	}
	observations := make([]scenarios.MeasurementObservation, 0, len(scenario.MeasurementBindings))
	executions := make([]blackbox.MeasurementBindingIdentity, 0, len(scenario.MeasurementBindings))
	for index, binding := range scenario.MeasurementBindings {
		sample := binding.MeasurementSample
		observation, identity, err := runner.observe(sample, definition, index)
		if err != nil {
			t.Fatalf("execute configured-bounds sample %s: %v", sample.SampleID, err)
		}
		if err := ledger.Bind(identity); err != nil {
			t.Fatalf("bind configured-bounds operation %s: %v", sample.Operation.ID, err)
		}
		observations = append(observations, observation)
		executions = append(executions, identity)
	}
	if len(executions) != 63 {
		t.Fatalf("configured-bounds server operations = %d, want 63", len(executions))
	}
	if err := ledger.Validate(); err != nil {
		t.Fatalf("close configured-bounds server bindings: %v", err)
	}

	if err := scenarios.ValidateMeasurementObservationClosure(
		scenario,
		obligationID,
		supportCellID,
		[]contract.RequiredMeasurement{definition},
		observations,
	); err != nil {
		t.Fatalf("validate configured-bounds observation closure: %v", err)
	}

	t.Run("rejects dropped server binding", func(t *testing.T) {
		controlLedger, err := blackbox.NewMeasurementBindingLedger(declarations)
		if err != nil {
			t.Fatalf("load control bindings: %v", err)
		}
		for _, execution := range executions[1:] {
			if err := controlLedger.Bind(execution); err != nil {
				t.Fatalf("bind control operation %s: %v", execution.OperationID, err)
			}
		}
		err = controlLedger.Validate()
		var failure blackbox.MeasurementBindingFailure
		if !errors.As(err, &failure) || failure.Category != blackbox.MeasurementBindingMissingExecution {
			t.Fatalf("dropped binding was not rejected: %v", err)
		}
	})
}

func configuredBoundDeclarations(t *testing.T, bindings []scenarios.MeasurementBinding) []blackbox.MeasurementBindingIdentity {
	t.Helper()
	declarations := make([]blackbox.MeasurementBindingIdentity, 0, len(bindings))
	for _, binding := range bindings {
		declarations = append(declarations, configuredBoundIdentity(binding.MeasurementSample))
	}
	if len(declarations) != 63 {
		t.Fatalf("configured-bounds authored declarations = %d, want 63", len(declarations))
	}
	return declarations
}

func configuredBoundIdentity(sample scenarios.MeasurementSample) blackbox.MeasurementBindingIdentity {
	return blackbox.MeasurementBindingIdentity{
		MeasurementID:  string(sample.MeasurementID),
		StratumID:      string(sample.StratumID),
		SampleID:       sample.SampleID,
		OperationID:    string(sample.Operation.ID),
		Family:         sample.Operation.Family,
		Boundary:       sample.Operation.Boundary,
		OperationValue: append(json.RawMessage(nil), sample.Operation.Value...),
	}
}

func (runner *configuredBoundsRunner) observe(
	sample scenarios.MeasurementSample,
	definition contract.RequiredMeasurement,
	index int,
) (scenarios.MeasurementObservation, blackbox.MeasurementBindingIdentity, error) {
	var authored configuredBoundValue
	if err := json.Unmarshal(sample.Operation.Value, &authored); err != nil {
		return scenarios.MeasurementObservation{}, blackbox.MeasurementBindingIdentity{}, errors.New("decode configured bound value failed")
	}
	if authored.BoundFamily != sample.Operation.Family || authored.Boundary != sample.Operation.Boundary {
		return scenarios.MeasurementObservation{}, blackbox.MeasurementBindingIdentity{}, errors.New("configured-bound operation identity does not match its value")
	}
	fact, err := runner.exercise(sample.Operation.Family, authored.Value, index)
	if err != nil {
		return scenarios.MeasurementObservation{}, blackbox.MeasurementBindingIdentity{}, err
	}
	if err := assertConfiguredBoundServerFact(sample.Operation.Boundary, fact); err != nil {
		return scenarios.MeasurementObservation{}, blackbox.MeasurementBindingIdentity{}, err
	}
	metrics, err := configuredBoundMetrics(definition, sample.Operation.Boundary, fact)
	if err != nil {
		return scenarios.MeasurementObservation{}, blackbox.MeasurementBindingIdentity{}, err
	}
	return scenarios.MeasurementObservation{
		StepID:        "STEP-PERF-CONFIGURED-BOUNDS-001",
		Operation:     sample.Operation,
		MeasurementID: sample.MeasurementID,
		StratumID:     sample.StratumID,
		SampleID:      sample.SampleID,
		Metrics:       metrics,
	}, configuredBoundIdentity(sample), nil
}

func assertConfiguredBoundServerFact(boundary string, fact configuredBoundServerFact) error {
	switch boundary {
	case "lower", "upper":
		if fact.Accepted {
			return nil
		}
	case "invalid":
		if !fact.Accepted && (fact.HTTPStatus == http.StatusBadRequest || fact.SQLState == "XX000") {
			return nil
		}
	default:
		return fmt.Errorf("configured bound boundary %q is unsupported", boundary)
	}
	return fmt.Errorf("server result does not satisfy %s configured-bound boundary", boundary)
}

func (runner *configuredBoundsRunner) exercise(family string, value, index int) (configuredBoundServerFact, error) {
	switch family {
	case "fanout":
		observation, err := runner.harness.Operator().ExerciseConfiguredFanoutLimit(runner.ctx, value)
		return configuredBoundSQLFact(observation), err
	case "impact":
		observation, err := runner.harness.Operator().ExerciseConfiguredImpactLimit(runner.ctx, value)
		return configuredBoundSQLFact(observation), err
	case "pull":
		return runner.exercisePull(value, index)
	case "rebuild":
		return runner.exerciseRebuild(value, index)
	case "compaction":
		observation, err := runner.harness.Operator().ExerciseConfiguredCompactionLimit(runner.ctx, value)
		return configuredBoundSQLFact(observation), err
	case "backfill":
		observation, err := runner.harness.Operator().ExerciseConfiguredBackfillLimit(runner.ctx, value)
		return configuredBoundSQLFact(observation), err
	case "push_mutations":
		return runner.exercisePush(value, index)
	default:
		return configuredBoundServerFact{}, fmt.Errorf("configured bound family %q is unsupported", family)
	}
}

func configuredBoundSQLFact(observation blackbox.ConfiguredBoundServerObservation) configuredBoundServerFact {
	return configuredBoundServerFact{Accepted: observation.Accepted, SQLState: observation.SQLState}
}

func (runner *configuredBoundsRunner) exercisePull(value, index int) (configuredBoundServerFact, error) {
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
			return configuredBoundServerFact{}, errors.New("configured pull response changes are invalid")
		}
		return configuredBoundServerFact{Accepted: true, HTTPStatus: status}, nil
	case http.StatusBadRequest:
		assertPhase4ProtocolError(runner.t, status, response, http.StatusBadRequest, "invalid_request")
		return configuredBoundServerFact{HTTPStatus: status}, nil
	default:
		return configuredBoundServerFact{}, fmt.Errorf("configured pull status = %d", status)
	}
}

func (runner *configuredBoundsRunner) exerciseRebuild(value, index int) (configuredBoundServerFact, error) {
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
			return configuredBoundServerFact{}, errors.New("configured rebuild returned the wrong scope")
		}
		if _, ok := response["records"].([]any); !ok {
			return configuredBoundServerFact{}, errors.New("configured rebuild records are invalid")
		}
		return configuredBoundServerFact{Accepted: true, HTTPStatus: status}, nil
	case http.StatusBadRequest:
		assertPhase4ProtocolError(runner.t, status, response, http.StatusBadRequest, "invalid_request")
		return configuredBoundServerFact{HTTPStatus: status}, nil
	default:
		return configuredBoundServerFact{}, fmt.Errorf("configured rebuild status = %d", status)
	}
}

func (runner *configuredBoundsRunner) exercisePush(value, index int) (configuredBoundServerFact, error) {
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
			return configuredBoundServerFact{}, errors.New("configured push outcomes are invalid")
		}
	} else if status == http.StatusBadRequest {
		assertPhase4ProtocolError(runner.t, status, response, http.StatusBadRequest, "invalid_request")
	} else {
		return configuredBoundServerFact{}, fmt.Errorf("configured push status = %d", status)
	}

	observation, err := runner.harness.Operator().ObserveDiagnosticPush(runner.ctx, client.ID, recordIDs)
	if err != nil {
		return configuredBoundServerFact{}, fmt.Errorf("observe configured push state: %w", err)
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
		return configuredBoundServerFact{}, errors.New("configured push durable state is invalid")
	}
	return configuredBoundServerFact{Accepted: accepted, HTTPStatus: status}, nil
}

func configuredBoundsProofIdentity() (contract.ObligationID, contract.SupportCellID, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "linux/amd64":
		return "OBL-PERF-CONFIGURED-BOUNDS-PG-LINUX-X64-001", "SUP-PG-LINUX-X64-001", nil
	default:
		return "", "", fmt.Errorf("configured-bounds proof has no support cell for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
}

func configuredBoundMetrics(definition contract.RequiredMeasurement, boundary string, fact configuredBoundServerFact) ([]scenarios.MeasurementMetricValue, error) {
	values := make([]scenarios.MeasurementMetricValue, 0, len(definition.Metrics))
	for _, metric := range definition.Metrics {
		value := 0.0
		switch metric.Name {
		case "lower_bound_acceptance":
			if boundary == "lower" && fact.Accepted {
				value = 1
			}
		case "upper_bound_acceptance":
			if boundary == "upper" && fact.Accepted {
				value = 1
			}
		case "invalid_bound_rejection":
			if boundary == "invalid" && !fact.Accepted {
				value = 1
			}
		default:
			return nil, fmt.Errorf("configured-bounds metric %q is unsupported", metric.Name)
		}
		values = append(values, scenarios.MeasurementMetricValue{MetricID: metric.ID, Value: value})
	}
	return values, nil
}
