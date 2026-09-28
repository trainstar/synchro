package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/soak"
)

type soakTestTransport func(*http.Request) (*http.Response, error)

func (f soakTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestSoakWirePreservesOriginalRecorderPairs(t *testing.T) {
	recorder, err := blackbox.NewRecorder(blackbox.RecorderConfig{AttachmentRoot: t.TempDir() + "/attachments"})
	if err != nil {
		t.Fatal(err)
	}
	requests := [][]byte{[]byte(`{ "client_id":"soak-client-a","scopes":{"scope-a":{"cursor":"old"}} }`), []byte(`{"client_id":"soak-client-a","scopes":{"scope-a":{"cursor":"new"}}}`)}
	responses := []string{`{"scope_cursors":{"scope-a":"new"},"changes":[{"value":9223372036854775807}]}`, `{"scope_cursors":{"scope-a":"next"},"changes":[]}`}
	operation := soak.Operation{Kind: soak.OperationPull, UserID: soakUserID, ClientID: soakClientID, ScopeID: "scope-a"}
	index := 0
	client := blackbox.Client{BaseURL: "http://localhost", HTTP: &http.Client{Transport: soakTestTransport(func(*http.Request) (*http.Response, error) {
		body := responses[index]
		index++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	client = client.WithRecorder(recorder, soakBodyLimit)
	harness := &liveSoakHarness{recorder: recorder}
	calls := make([]soakRecordedCall, 0, 2)
	for i, raw := range requests {
		request := blackbox.Request{Method: "POST", Path: "/sync/pull", Class: "soak-pull", Body: raw}
		if _, err := client.Do(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		call, err := harness.recordedCall(i, request)
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
		wire, err := harness.wireFromCall(operation, call, false, false, false)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire.RequestBody, raw) || string(wire.ResponseBody) != responses[i] || wire.Sequence != uint64(i+1) {
			t.Fatal("recorder bytes or identity changed")
		}
	}
	crossPaired := calls[1]
	crossPaired.metadata.ResponseAttachmentID = calls[0].metadata.ResponseAttachmentID
	crossPaired.metadata.ResponseBodySHA256 = calls[0].metadata.ResponseBodySHA256
	if _, err := harness.wireFromCall(operation, crossPaired, false, false, false); err == nil {
		t.Fatal("cross-paired recorder metadata was accepted")
	}
	wrongEndpoint := calls[0]
	wrongEndpoint.path = "/sync/schema"
	if _, err := harness.wireFromCall(operation, wrongEndpoint, false, false, false); err == nil {
		t.Fatal("schema read was presented as pull wire")
	}
}

func TestSoakFaultRejectsWrongOperationAndUnsupportedRecipes(t *testing.T) {
	catalog, err := faults.LoadCatalog(context.Background(), soakRepositoryRoot)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := soak.Generate(1, soak.Config{OperationCount: 7, FaultRate: 100}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	operation := plan.Operations[1]
	for _, path := range []string{"/sync/connect", "/sync/pull", "/sync/push?other=1"} {
		if err := validateSoakFaultRequest(operation, blackbox.Request{Method: "POST", Path: path}); err == nil {
			t.Fatalf("push fault accepted endpoint %s", path)
		}
	}
	request := blackbox.Request{Method: "POST", Path: "/sync/push"}
	if err := validateSoakFaultRequest(operation, request); err != nil {
		t.Fatal(err)
	}
	operation.FaultPlan.Injection.Operator = "delay"
	if err := validateSoakFaultRequest(operation, request); err == nil {
		t.Fatal("unsupported recipe became another fault")
	}
}

func TestSoakFaultResponseLossRetriesExactRequestAndChecksSideEffects(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "once-only", true: "duplicate-write"}[duplicate], func(t *testing.T) {
			calls, durableWrites := 0, 0
			raw := []byte(`{ "batch_id":"same-batch", "mutations":[{"value":9223372036854775807}] }`)
			client := blackbox.Client{BaseURL: "http://localhost", HTTP: &http.Client{Transport: soakTestTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				if request.URL.Path != "/sync/push" || !bytes.Equal(body, raw) {
					t.Fatal("retry changed endpoint or request bytes")
				}
				if duplicate || calls == 1 {
					durableWrites++
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"accepted":[]}`))}, nil
			})}}
			observations := 0
			_, err := responseLossRequest(context.Background(), client, blackbox.Request{Method: "POST", Path: "/sync/push", Class: "soak-push", Body: raw}, func(context.Context) ([]byte, error) {
				observations++
				if calls != observations {
					t.Fatal("activation was observed before the executed transport boundary")
				}
				return json.Marshal(durableWrites)
			})
			if (err != nil) != duplicate || calls != 2 || observations != 2 {
				t.Fatalf("calls=%d observations=%d error=%v", calls, observations, err)
			}
		})
	}
}

func TestSoakFaultRequiresActualResponseLoss(t *testing.T) {
	client := blackbox.Client{BaseURL: "http://localhost", HTTP: &http.Client{Transport: soakTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transmission failed before response")
	})}}
	observed := false
	_, err := responseLossRequest(context.Background(), client, blackbox.Request{Method: "POST", Path: "/sync/pull", Class: "soak-pull"}, func(context.Context) ([]byte, error) {
		observed = true
		return nil, nil
	})
	if err == nil || observed {
		t.Fatal("pre-response failure was reported as an activated response-loss control")
	}
}

func TestSoakWALRestartRequiresOnceOnlyMaterialization(t *testing.T) {
	before := blackbox.WALRecordObservation{RecordID: "record", CommitLSN: "0/1", EndLSN: "0/2", RowVersion: "version", FenceCoverage: "materialized"}
	after := before
	after.ReplayCount = 1
	stages := blackbox.WALRecordStageObservation{FenceCount: 1, EventCount: 1, ProjectionCount: 1, CapturedCount: 1, EdgeCount: 1, ChangeCount: 1}
	valid := blackbox.WALReplayRestartObservation{
		PriorProgress:                     blackbox.WALProgressObservation{SlotMatchesProgress: true},
		WorkerExitedBeforeAcknowledgement: true, WorkerRestarted: true,
		BeforeRestart: blackbox.WALPipelineObservation{Records: []blackbox.WALRecordObservation{before}},
		AfterRestart:  blackbox.WALPipelineObservation{Records: []blackbox.WALRecordObservation{after}, WorkerRunning: true, ContiguousAcknowledged: true, AcknowledgementMatchesObservedEnd: true, SlotMatchesObservedEnd: true, AcknowledgedEndLSN: "0/2"},
		BeforeStages:  stages, AfterStages: stages,
	}
	if err := validateSoakWALRestart(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*blackbox.WALReplayRestartObservation){
		func(v *blackbox.WALReplayRestartObservation) { v.WorkerExitedBeforeAcknowledgement = false },
		func(v *blackbox.WALReplayRestartObservation) { v.WorkerRestarted = false },
		func(v *blackbox.WALReplayRestartObservation) { v.AfterStages.EdgeCount++ },
		func(v *blackbox.WALReplayRestartObservation) {
			v.AfterRestart.Records = append(v.AfterRestart.Records, v.AfterRestart.Records[0])
		},
	} {
		candidate := valid
		mutate(&candidate)
		if err := validateSoakWALRestart(candidate); err == nil {
			t.Fatal("false WAL recovery was accepted")
		}
	}
}

// TestSoak runs bounded seeded stress with an explicit operation budget. It
// retains each journal and each failed run's wire bodies in a new directory
// under SOAK_ARTIFACT_DIR. With SOAK_REPLAY_JOURNAL set, it only replays that
// retained journal in a new cluster.
func TestSoak(t *testing.T) {
	if strings.TrimSpace(os.Getenv("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT")) == "" {
		t.Skip("the black-box environment is not configured")
	}
	if !*provision || !*install {
		t.Fatal("TestSoak requires --provision --install")
	}
	if os.Getenv("SOAK_DURATION") != "" {
		t.Fatal("SOAK_DURATION is retired because it only estimated an operation count; set SOAK_OPERATIONS")
	}
	ctx := context.Background()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Minute))
		defer cancel()
	}
	catalog, err := faults.LoadCatalog(ctx, soakRepositoryRoot)
	if err != nil {
		t.Fatalf("load soak fault catalog: %v", err)
	}
	if journal := os.Getenv("SOAK_REPLAY_JOURNAL"); journal != "" {
		t.Run("replay", func(t *testing.T) { replaySoakJournal(t, ctx, journal, catalog) })
		return
	}
	seed, err := parseSoakSeed(os.Getenv("SOAK_SEED"))
	if err != nil {
		t.Fatal(err)
	}
	operations, err := parseSoakOperations(os.Getenv("SOAK_OPERATIONS"))
	if err != nil {
		t.Fatal(err)
	}
	artifactRoot := os.Getenv("SOAK_ARTIFACT_DIR")
	if artifactRoot == "" {
		t.Fatal("SOAK_ARTIFACT_DIR is required so failure evidence survives test cleanup")
	}
	if err := os.MkdirAll(artifactRoot, 0o700); err != nil {
		t.Fatalf("create soak artifact root: %v", err)
	}
	evidence, err := os.MkdirTemp(artifactRoot, fmt.Sprintf("seed-%d-", seed))
	if err != nil {
		t.Fatalf("create soak evidence directory: %v", err)
	}
	t.Logf("soak evidence directory: %s", evidence)
	coverage := func(control string) soak.Config {
		return soak.Config{
			OperationCount: soak.MinimumCoverageOperations,
			Users:          []string{soakUserID},
			Clients:        []string{soakClientID},
			Scopes:         []string{soakPrivateScope, soakSharedScope},
			Control:        control,
		}
	}

	t.Run("live", func(t *testing.T) {
		config := coverage("")
		config.OperationCount = operations
		config.FaultRate = soak.DefaultFaultRate
		plan, err := soak.Generate(seed, config, catalog)
		if err != nil {
			t.Fatalf("generate live soak plan: %v", err)
		}
		result, journal, err := runRetainedSoak(ctx, plan, evidence, "live")
		t.Logf("live soak seed=%d operations=%d/%d elapsed=%s journal=%s", seed, result.OperationsExecuted, len(plan.Operations), result.Elapsed, journal)
		if err != nil {
			t.Fatalf("run live soak: %v; replay with make soak-replay SOAK_REPLAY_JOURNAL=%s", err, journal)
		}
		if result.OperationsExecuted != len(plan.Operations) || len(result.Violations) != 0 {
			t.Fatalf("live soak result = operations %d/%d, violations %d", result.OperationsExecuted, len(plan.Operations), len(result.Violations))
		}
	})

	t.Run("checksum-corruption-fails", func(t *testing.T) {
		plan, err := soak.Generate(seed, coverage(soakControlChecksum), catalog)
		if err != nil {
			t.Fatalf("generate checksum control plan: %v", err)
		}
		result, _, runErr := runRetainedSoak(ctx, plan, evidence, "checksum-corruption")
		if !errors.Is(runErr, soak.ErrInvariantViolation) || !hasChecksumRowDigestViolation(result.Violations) {
			t.Fatalf("checksum control error = %v, violations = %#v", runErr, result.Violations)
		}
	})

	// Each delivered-row control changes the delivered WAL-restart row while the
	// client's row digest and scope metadata are forged to match. Only the
	// independent source comparison can detect it.
	runDeliveredRowControl := func(t *testing.T, control, name string, rule invariants.RuleID) (soak.RunResult, string) {
		t.Helper()
		plan, err := soak.Generate(seed, coverage(control), catalog)
		if err != nil {
			t.Fatalf("generate %s control plan: %v", name, err)
		}
		result, journal, runErr := runRetainedSoak(ctx, plan, evidence, name)
		if !errors.Is(runErr, soak.ErrInvariantViolation) || len(result.Violations) == 0 {
			t.Fatalf("%s control error = %v, violations = %#v", name, runErr, result.Violations)
		}
		for _, violation := range result.Violations {
			if violation.Family != soak.InvariantSourceState || violation.RuleID != rule {
				t.Fatalf("%s was judged by %s/%s, want only %s", name, violation.Family, violation.RuleID, rule)
			}
		}
		return result, journal
	}

	t.Run("omitted-delivery-fails", func(t *testing.T) {
		runDeliveredRowControl(t, soakControlOmitDelivery, "omitted-delivery", soak.RuleSourceStateMembership)
	})

	// A wrong held value replays as the same logical failure in a new cluster,
	// although that cluster allocates different runtime table and field IDs.
	// The same defect on another field has a different identity.
	t.Run("wrong-value-replays-in-a-new-cluster", func(t *testing.T) {
		wrongValue, journal := runDeliveredRowControl(t, soakControlWrongValue, "wrong-value", soak.RuleSourceStateValueMismatch)
		if field := violationEvidence(wrongValue.Violations[0], "field"); field != "value" {
			t.Fatalf("wrong-value control named field %q, want value", field)
		}
		replaySoakJournal(t, ctx, journal, catalog)
		wrongOwner, _ := runDeliveredRowControl(t, soakControlWrongOwner, "wrong-owner", soak.RuleSourceStateValueMismatch)
		if field := violationEvidence(wrongOwner.Violations[0], "field"); field != "owner_id" {
			t.Fatalf("wrong-owner control named field %q, want owner_id", field)
		}
		if outcome := soak.CompareReplay(wrongValue.Failure, wrongOwner.Failure); outcome != soak.ReplayDiverged {
			t.Fatalf("a wrong value on another field compared as %s, want diverged", outcome)
		}
	})
}

func violationEvidence(violation invariants.Violation, name string) string {
	for _, field := range violation.Evidence {
		if field.Name == name {
			return field.Value
		}
	}
	return ""
}

// runRetainedSoak runs one plan in its own cluster. The journal is written to
// the evidence directory, and a failed run keeps its wire bodies there before
// the cluster and attachment root are removed.
func runRetainedSoak(ctx context.Context, plan soak.Plan, evidence, name string) (soak.RunResult, string, error) {
	journal := filepath.Join(evidence, name+".jsonl")
	harness, err := newLiveSoakHarness(ctx, plan.Seed, plan.Config.Control)
	if err != nil {
		return soak.RunResult{}, journal, fmt.Errorf("create %s soak harness: %w", name, err)
	}
	result, runErr := soak.Run(ctx, plan, harness, journal)
	if runErr != nil {
		runErr = errors.Join(runErr, harness.retainWire(filepath.Join(evidence, name+"-wire")))
	}
	closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return result, journal, errors.Join(runErr, harness.Close(closeContext))
}

// replaySoakJournal rebuilds the retained plan in a new cluster and requires
// the same outcome: success for a sealed journal, or the same failure identity
// for a failed journal. An unidentified failure is inconclusive and fails.
func replaySoakJournal(t *testing.T, ctx context.Context, path string, catalog *faults.Catalog) {
	t.Helper()
	journal, readErr := soak.ReadJournal(path)
	if readErr != nil && !errors.Is(readErr, soak.ErrJournalUnsealed) {
		t.Fatalf("read retained soak journal: %v", readErr)
	}
	plan, err := soak.ReplayPlan(journal, catalog)
	if err != nil {
		t.Fatalf("rebuild retained soak plan: %v", err)
	}
	harness, err := newLiveSoakHarness(ctx, plan.Seed, plan.Config.Control)
	if err != nil {
		t.Fatalf("create soak replay harness: %v", err)
	}
	// The harness holds the shared installation lock, so it closes before any
	// later cluster in the same test provisions.
	replayed, replayErr := soak.ReplayRun(ctx, path, catalog, harness)
	closeLiveSoakHarness(t, harness)
	if readErr == nil {
		if replayErr != nil {
			t.Fatalf("sealed soak journal replay failed: %v", replayErr)
		}
		t.Logf("replayed sealed soak journal seed=%d operations=%d elapsed=%s", plan.Seed, replayed.OperationsExecuted, replayed.Elapsed)
		return
	}
	facts := journal.OperationFacts
	if len(facts) == 0 || facts[len(facts)-1].Status != "failed" {
		t.Fatalf("unsealed soak journal has no terminal failure fact")
	}
	original := facts[len(facts)-1]
	switch outcome := soak.CompareReplay(&original, replayed.Failure); outcome {
	case soak.ReplayReproduced:
		t.Logf("replay reproduced retained failure at operation %d with code %s, stage %q, and %d violations",
			original.Sequence, original.FailureCode, original.FailureStage, original.ViolationCount)
	case soak.ReplayInconclusive:
		t.Fatalf("soak replay is inconclusive: the retained %s failure at operation %d has no precise identity; replay error: %v",
			original.FailureCode, original.Sequence, replayErr)
	default:
		t.Fatalf("soak replay did not reproduce the retained failure: retained=%s replayed=%s error=%v",
			mustMarshalJSON(original), mustMarshalJSON(replayed.Failure), replayErr)
	}
}

func hasChecksumRowDigestViolation(violations []invariants.Violation) bool {
	for _, violation := range violations {
		if violation.Family == invariants.InvariantChecksumConvergence && violation.RuleID == invariants.RuleChecksumRowDigestMismatch {
			return true
		}
	}
	return false
}

func closeLiveSoakHarness(t *testing.T, harness *liveSoakHarness) {
	t.Helper()
	closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := harness.Close(closeContext); err != nil {
		t.Errorf("close live soak harness: %v", err)
	}
}
