package faults

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/trainstar/synchro/conformance/barriers"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestLoadCatalogAndValidatePlanExactly(t *testing.T) {
	ctx := context.Background()
	root := repositoryRoot(t)
	catalog, err := LoadCatalog(ctx, root)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	scenario, err := scenarios.LoadFile(ctx, root, "conformance/scenarios/server/push-response-loss-001.json")
	if err != nil {
		t.Fatalf("load scenario: %v", err)
	}
	if len(scenario.FaultPlans) != 1 {
		t.Fatalf("fault plan count = %d, want 1", len(scenario.FaultPlans))
	}
	plan := scenario.FaultPlans[0]
	if err := ValidatePlan(plan, catalog); err != nil {
		t.Fatalf("validate exact plan: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*scenarios.FaultPlan)
	}{
		{"requirement", func(plan *scenarios.FaultPlan) { plan.RequirementID = "SYNC-TIME-001" }},
		{"control", func(plan *scenarios.FaultPlan) { plan.ControlID = "CTRL-TIMESTAMP-001" }},
		{"fault", func(plan *scenarios.FaultPlan) { plan.FaultID = "FAULT-TIME-001" }},
		{"malformed barrier", func(plan *scenarios.FaultPlan) { plan.BarrierID = "barrier" }},
		{"mechanism", func(plan *scenarios.FaultPlan) { plan.Injection.Mechanism = "state-fault" }},
		{"target", func(plan *scenarios.FaultPlan) { plan.Injection.Target = "other target" }},
		{"operator", func(plan *scenarios.FaultPlan) { plan.Injection.Operator = "replace" }},
		{"scenario parameter", func(plan *scenarios.FaultPlan) { plan.Injection.Parameters.Scenario = "other scenario" }},
		{"defect parameter", func(plan *scenarios.FaultPlan) { plan.Injection.Parameters.Defect = "other defect" }},
		{"precondition parameter", func(plan *scenarios.FaultPlan) { plan.Injection.Parameters.Precondition = "other precondition" }},
		{"assertions", func(plan *scenarios.FaultPlan) { plan.ExpectedAssertionIDs = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutant := plan
			mutant.ExpectedAssertionIDs = append(mutant.ExpectedAssertionIDs[:0:0], plan.ExpectedAssertionIDs...)
			test.mutate(&mutant)
			if err := ValidatePlan(mutant, catalog); err == nil {
				t.Fatal("mutated recipe was accepted")
			}
		})
	}

	mutatedCatalog := cloneCatalog(catalog)
	for index := range mutatedCatalog.Controls {
		if mutatedCatalog.Controls[index].ID == string(plan.ControlID) {
			mutatedCatalog.Controls[index].SubjectType = "invalid-subject-type"
			break
		}
	}
	if err := ValidatePlan(plan, mutatedCatalog); err == nil {
		t.Fatal("catalog subject-type mutation was accepted")
	}
}

func TestLoadCatalogRejectsDuplicateMembersAndCancellation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "conformance", "faults")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	duplicate := `{"$schema":"` + CatalogSchemaURI + `","$schema":"` + CatalogSchemaURI + `"}`
	if err := os.WriteFile(filepath.Join(path, "catalog.json"), []byte(duplicate), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCatalog(context.Background(), root); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("duplicate catalog error = %v, want invalid catalog", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := LoadCatalog(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled catalog error = %v, want context cancellation", err)
	}
}

func TestResponseLossAndTimeoutFollowUpstreamCompletion(t *testing.T) {
	t.Run("response loss", func(t *testing.T) {
		completed := make(chan struct{}, 1)
		body := newTrackedBody("private-response-canary")
		fault := newWireFault(t, WireOptions{Mode: WireResponseLoss}, roundTripFunc(func(*http.Request) (*http.Response, error) {
			completed <- struct{}{}
			return responseWithBody(body), nil
		}))
		response, err := fault.RoundTrip(testRequest(t, "private-request-canary"))
		if response != nil {
			t.Fatal("response loss returned a response")
		}
		if !errors.Is(err, ErrResponseLost) {
			t.Fatalf("response loss error = %v, want %v", err, ErrResponseLost)
		}
		if strings.Contains(err.Error(), "private-response-canary") || strings.Contains(err.Error(), "private-request-canary") {
			t.Fatalf("response loss error exposes payload: %v", err)
		}
		<-completed
		<-body.done
	})

	t.Run("timeout", func(t *testing.T) {
		completed := make(chan struct{}, 1)
		body := newTrackedBody("timeout-body")
		fault := newWireFault(t, WireOptions{Mode: WireTimeout}, roundTripFunc(func(*http.Request) (*http.Response, error) {
			completed <- struct{}{}
			return responseWithBody(body), nil
		}))
		response, err := fault.RoundTrip(testRequest(t, "timeout-request"))
		if response != nil {
			t.Fatal("timeout returned a response")
		}
		if !errors.Is(err, ErrInjectedTimeout) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout error = %v, want injected deadline timeout", err)
		}
		var timeout interface{ Timeout() bool }
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("timeout error does not implement net timeout behavior: %v", err)
		}
		<-completed
		<-body.done
	})
}

func TestTruncationAndWireCancellationCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	owner, err := NewController(ctx)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	body := newTrackedBody("abcdef")
	fault, err := NewWireFault(ctx, owner, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWithBody(body), nil
	}), WireOptions{Mode: WireTruncate, TruncateAfter: 3})
	if err != nil {
		t.Fatalf("new truncate fault: %v", err)
	}
	response, err := fault.RoundTrip(testRequest(t, "truncate-request"))
	if err != nil {
		t.Fatalf("truncate round trip: %v", err)
	}
	data, readErr := io.ReadAll(response.Body)
	if string(data) != "abc" {
		t.Fatalf("truncated data = %q, want %q", data, "abc")
	}
	if !errors.Is(readErr, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated read error = %v, want unexpected EOF", readErr)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close truncated response: %v", err)
	}
	<-body.done

	cleanupBody := newTrackedBody("cleanup-body")
	cleanupFault, err := NewWireFault(ctx, owner, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWithBody(cleanupBody), nil
	}), WireOptions{Mode: WireTruncate, TruncateAfter: 1})
	if err != nil {
		t.Fatalf("new cleanup fault: %v", err)
	}
	cleanupResponse, err := cleanupFault.RoundTrip(testRequest(t, "cleanup-request"))
	if err != nil {
		t.Fatalf("cleanup round trip: %v", err)
	}
	cancel()
	<-cleanupFault.Done()
	<-owner.Done()
	<-cleanupBody.done
	if _, err := cleanupResponse.Body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed response read error = %v", err)
	}
}

func TestDuplicateAndReplayRequireReplayableRequests(t *testing.T) {
	t.Run("duplicate", func(t *testing.T) {
		calls := 0
		var received []string
		fault := newWireFault(t, WireOptions{Mode: WireDuplicate}, roundTripFunc(func(request *http.Request) (*http.Response, error) {
			data, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			calls++
			received = append(received, string(data))
			return responseWithBody(io.NopCloser(strings.NewReader("response"))), nil
		}))
		response, err := fault.RoundTrip(testRequest(t, "sealed-request"))
		if err != nil {
			t.Fatalf("duplicate round trip: %v", err)
		}
		defer response.Body.Close()
		if calls != 2 || len(received) != 2 || received[0] != "sealed-request" || received[1] != "sealed-request" {
			t.Fatalf("duplicate calls = %d, received = %#v", calls, received)
		}
	})

	t.Run("replay", func(t *testing.T) {
		calls := 0
		fault := newWireFault(t, WireOptions{Mode: WireReplay, ReplayCount: 3}, roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return responseWithBody(io.NopCloser(strings.NewReader("response"))), nil
		}))
		response, err := fault.RoundTrip(testRequest(t, "replay-request"))
		if err != nil {
			t.Fatalf("replay round trip: %v", err)
		}
		defer response.Body.Close()
		if calls != 3 {
			t.Fatalf("replay calls = %d, want 3", calls)
		}
	})

	t.Run("nonreplayable request", func(t *testing.T) {
		calls := 0
		fault := newWireFault(t, WireOptions{Mode: WireDuplicate}, roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return responseWithBody(http.NoBody), nil
		}))
		request := testRequest(t, "private-unreplayable-canary")
		request.GetBody = nil
		response, err := fault.RoundTrip(request)
		if response != nil {
			t.Fatal("nonreplayable request returned a response")
		}
		if !errors.Is(err, ErrRequestNotReplayable) {
			t.Fatalf("nonreplayable request error = %v, want %v", err, ErrRequestNotReplayable)
		}
		if calls != 0 {
			t.Fatalf("nonreplayable request reached upstream %d times", calls)
		}
		if strings.Contains(err.Error(), "private-unreplayable-canary") {
			t.Fatalf("nonreplayable error exposes request data: %v", err)
		}
	})
}

func TestNamedBarrierTerminatesProcess(t *testing.T) {
	ctx := context.Background()
	owner, err := NewController(ctx)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	defer owner.Close()
	barrierController, err := barriers.NewController([]barriers.Definition{{
		ID:           "BAR-PROCESS-TEST-001",
		Participants: []string{"terminator"},
		ReleaseOrder: 1,
	}})
	if err != nil {
		t.Fatalf("new barrier controller: %v", err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestFaultProcessHelper$")
	command.Env = append(os.Environ(), "GO_WANT_FAULT_PROCESS=1")
	process, err := StartProcess(ctx, owner, command)
	if err != nil {
		t.Fatalf("start process: %v", err)
	}
	defer process.Close()
	termination, err := process.TerminateAt(ctx, barrierController, "BAR-PROCESS-TEST-001", "terminator")
	if err != nil {
		t.Fatalf("arm termination: %v", err)
	}
	if err := barrierController.WaitForArrivals(ctx, "BAR-PROCESS-TEST-001", 1); err != nil {
		t.Fatalf("wait for terminator arrival: %v", err)
	}
	if err := barrierController.Release(ctx, "BAR-PROCESS-TEST-001"); err != nil {
		t.Fatalf("release termination barrier: %v", err)
	}
	if err := termination.Wait(ctx); err != nil {
		t.Fatalf("wait for termination: %v", err)
	}
	if err := process.Wait(ctx); err == nil {
		t.Fatal("terminated process exited successfully")
	}
}

func TestCancellationRestoresArtifactsAndTerminatesProcesses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	owner, err := NewController(ctx)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	path := filepath.Join(t.TempDir(), "artifact.txt")
	if err := os.WriteFile(path, []byte("original"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	artifact, err := TamperArtifact(ctx, owner, path, []byte("tampered"))
	if err != nil {
		t.Fatalf("tamper artifact: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "tampered" {
		t.Fatalf("tampered contents = %q", data)
	}
	if err := artifact.Restore(); err != nil {
		t.Fatalf("restore artifact: %v", err)
	}
	<-artifact.Done()
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "original" {
		t.Fatalf("explicitly restored contents = %q, want original", data)
	}
	artifact, err = TamperArtifact(ctx, owner, path, []byte("tampered"))
	if err != nil {
		t.Fatalf("retamper artifact: %v", err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestFaultProcessHelper$")
	command.Env = append(os.Environ(), "GO_WANT_FAULT_PROCESS=1")
	process, err := StartProcess(ctx, owner, command)
	if err != nil {
		t.Fatalf("start process: %v", err)
	}
	barrierController, err := barriers.NewController([]barriers.Definition{{
		ID:           "BAR-CANCEL-PROCESS-001",
		Participants: []string{"terminator"},
		ReleaseOrder: 1,
	}})
	if err != nil {
		t.Fatalf("new cancellation barrier controller: %v", err)
	}
	termination, err := process.TerminateAt(ctx, barrierController, "BAR-CANCEL-PROCESS-001", "terminator")
	if err != nil {
		t.Fatalf("arm cancellation termination: %v", err)
	}
	if err := barrierController.WaitForArrivals(ctx, "BAR-CANCEL-PROCESS-001", 1); err != nil {
		t.Fatalf("wait for cancellation terminator arrival: %v", err)
	}
	cancel()
	<-artifact.Done()
	<-termination.Done()
	<-process.Done()
	<-owner.Done()
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "original" {
		t.Fatalf("restored contents = %q, want original", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("restored mode = %o, want 640", info.Mode().Perm())
	}
	if err := process.Wait(context.Background()); err == nil {
		t.Fatal("canceled process exited successfully")
	}
	if err := termination.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled termination error = %v, want context cancellation", err)
	}
}

func TestWireResponseLossIsDeterministicAcrossRuns(t *testing.T) {
	for run := 0; run < 32; run++ {
		calls := 0
		fault := newWireFault(t, WireOptions{Mode: WireResponseLoss}, roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return responseWithBody(http.NoBody), nil
		}))
		response, err := fault.RoundTrip(testRequest(t, "deterministic"))
		if response != nil || !errors.Is(err, ErrResponseLost) || calls != 1 {
			t.Fatalf("run %d result = response %v, error %v, calls %d", run, response, err, calls)
		}
		if err := fault.Close(); err != nil {
			t.Fatalf("run %d close fault: %v", run, err)
		}
	}
}

func TestFaultProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_FAULT_PROCESS") != "1" {
		return
	}
	select {}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type trackedBody struct {
	mu     sync.Mutex
	reader *strings.Reader
	done   chan struct{}
	once   sync.Once
	closed bool
}

func newTrackedBody(value string) *trackedBody {
	return &trackedBody{reader: strings.NewReader(value), done: make(chan struct{})}
}

func (b *trackedBody) Read(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	return b.reader.Read(data)
}

func (b *trackedBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		close(b.done)
	})
	return nil
}

func responseWithBody(body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       body,
	}
}

func newWireFault(t *testing.T, options WireOptions, upstream http.RoundTripper) *WireFault {
	t.Helper()
	fault, err := NewWireFault(context.Background(), nil, upstream, options)
	if err != nil {
		t.Fatalf("new wire fault: %v", err)
	}
	t.Cleanup(func() {
		if err := fault.Close(); err != nil {
			t.Errorf("close wire fault: %v", err)
		}
	})
	return fault
}

func testRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, "https://example.invalid/sync", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func cloneCatalog(catalog *Catalog) *Catalog {
	clone := *catalog
	clone.Faults = append([]Fault(nil), catalog.Faults...)
	clone.Controls = append([]Control(nil), catalog.Controls...)
	for index := range clone.Controls {
		clone.Controls[index].RequirementIDs = append([]string(nil), catalog.Controls[index].RequirementIDs...)
		clone.Controls[index].NormativeReferences = append([]string(nil), catalog.Controls[index].NormativeReferences...)
	}
	return &clone
}
