package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
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

func TestSoak(t *testing.T) {
	if strings.TrimSpace(os.Getenv("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT")) == "" {
		t.Skip("the black-box environment is not configured")
	}
	if !*provision || !*install {
		t.Fatal("TestSoak requires --provision --install")
	}
	seed, err := parseSoakSeed(os.Getenv("SOAK_SEED"))
	if err != nil {
		t.Fatal(err)
	}
	duration, err := parseSoakDuration(os.Getenv("SOAK_DURATION"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration*6+15*time.Minute)
	defer cancel()
	catalog, err := faults.LoadCatalog(ctx, soakRepositoryRoot)
	if err != nil {
		t.Fatalf("load soak fault catalog: %v", err)
	}

	t.Run("live", func(t *testing.T) {
		config, err := soak.ConfigForDuration(duration)
		if err != nil {
			t.Fatalf("configure live soak: %v", err)
		}
		config.Users = []string{soakUserID}
		config.Clients = []string{soakClientID}
		config.Scopes = []string{soakPrivateScope, soakSharedScope}
		plan, err := soak.Generate(seed, config, catalog)
		if err != nil {
			t.Fatalf("generate live soak plan: %v", err)
		}
		harness, err := newLiveSoakHarness(ctx, seed, false)
		if err != nil {
			t.Fatalf("create live soak harness: %v", err)
		}
		t.Cleanup(func() { closeLiveSoakHarness(t, harness) })
		result, err := soak.Run(ctx, plan, harness, t.TempDir()+"/live-soak.jsonl")
		if err != nil {
			t.Fatalf("run live soak: %v; operations %d/%d; violations: %#v", err, result.OperationsExecuted, len(plan.Operations), result.Violations)
		}
		if result.OperationsExecuted != len(plan.Operations) || len(result.Violations) != 0 {
			t.Fatalf("live soak result = operations %d/%d, violations %d", result.OperationsExecuted, len(plan.Operations), len(result.Violations))
		}
	})

	t.Run("checksum-corruption-replays-from-seed", func(t *testing.T) {
		config := soak.Config{
			OperationCount: soak.MinimumCoverageOperations,
			Users:          []string{soakUserID},
			Clients:        []string{soakClientID},
			Scopes:         []string{soakPrivateScope, soakSharedScope},
			FaultRate:      0,
		}
		plan, err := soak.Generate(seed, config, catalog)
		if err != nil {
			t.Fatalf("generate checksum replay plan: %v", err)
		}
		journalPath := t.TempDir() + "/checksum-corruption.jsonl"
		firstHarness, err := newLiveSoakHarness(ctx, seed, true)
		if err != nil {
			t.Fatalf("create checksum fault harness: %v", err)
		}
		first, firstErr := soak.Run(ctx, plan, firstHarness, journalPath)
		closeLiveSoakHarness(t, firstHarness)
		if !errors.Is(firstErr, soak.ErrInvariantViolation) {
			t.Fatalf("checksum fault error = %v, want ErrInvariantViolation", firstErr)
		}
		if !hasChecksumRowDigestViolation(first.Violations) {
			t.Fatalf("checksum fault violations = %#v", first.Violations)
		}

		replayHarness, err := newLiveSoakHarness(ctx, seed, true)
		if err != nil {
			t.Fatalf("create checksum replay harness: %v", err)
		}
		t.Cleanup(func() { closeLiveSoakHarness(t, replayHarness) })
		replayed, replayErr := soak.ReplayRun(ctx, journalPath, catalog, replayHarness)
		if !errors.Is(replayErr, soak.ErrInvariantViolation) {
			t.Fatalf("checksum replay error = %v, want ErrInvariantViolation", replayErr)
		}
		if !reflect.DeepEqual(first.Violations, replayed.Violations) {
			t.Fatalf("checksum replay violations differ: first=%#v replay=%#v", first.Violations, replayed.Violations)
		}
	})
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
