package reactnative

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateRetentionReconnectScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateRetentionReconnectScenario(loadRetentionReconnectAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored retention-reconnect scenario: %v", err)
	}
}

func TestValidateRetentionReconnectScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{
			name: "terminal completion",
			mutate: func(scenario *scenarios.Scenario) {
				step := retentionReconnectScenarioStep(t, scenario, retentionReconnectStepOrder[7])
				binding := *step.NativeBinding
				binding.Completion = "error"
				step.NativeBinding = &binding
			},
		},
		{
			name: "temporary unavailable wire",
			mutate: func(scenario *scenarios.Scenario) {
				retentionReconnectScenarioWire(t, scenario, retentionReconnectStepOrder[1]).HTTPStatus = http.StatusOK
			},
		},
		{
			name: "identity owner",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.NativeIdentityAliases[0].StepIDs[0] = "STEP-RETENTION-RECONNECT-ABSENT-001"
			},
		},
		{
			name: "Android proof target",
			mutate: func(scenario *scenarios.Scenario) {
				for index := range scenario.ProofObligations {
					if scenario.ProofObligations[index].ObligationID == "OBL-RETENTION-RECONNECT-RN-ANDROID-CURRENT-001" {
						scenario.ProofObligations[index].MakeTarget = "test-rn-retention-android"
					}
				}
			},
		},
		{
			name: "assertion oracle",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.Assertions[0].Oracle.ExpectedSource = "system-under-test"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneRetentionReconnectScenario(t, loadRetentionReconnectAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateRetentionReconnectScenario(scenario); err == nil {
				t.Fatal("changed retention-reconnect contract was accepted")
			}
		})
	}
}

func TestNewRetentionReconnectCoordinatorUsesHostLoopbackProxy(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()

	coordinator, err := NewRetentionReconnectCoordinator(RetentionReconnectCoordinatorConfig{
		Scenario: loadRetentionReconnectAuthoredScenario(t), Platform: "android", ServerURL: upstream.URL, AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create retention-reconnect coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android adapter URL = %q", coordinator.adapter)
	}
	if coordinator.upstream != upstream.URL {
		t.Fatalf("upstream URL = %q, want %q", coordinator.upstream, upstream.URL)
	}
	if coordinator.ExchangeCount() != 10 {
		t.Fatalf("exchange count = %d, want 10", coordinator.ExchangeCount())
	}
	command := coordinator.command("client", "open", map[string]any{"client_key": coordinator.main.clientID}, nil)
	if command.Action.Steps == nil || len(command.Action.Steps) != 0 {
		t.Fatalf("empty command steps = %#v", command.Action.Steps)
	}
}

func TestRetentionReconnectCaptureUsesDistinctRunnerInputAndResultKeys(t *testing.T) {
	if got, want := strings.Join(retentionReconnectCaptureSources(), ","), "scope-state,pending-mutations,rejected-mutations,sync-status,request-trace"; got != want {
		t.Fatalf("capture sources = %q, want %q", got, want)
	}
	if got, want := strings.Join(retentionReconnectCaptureResultKeys(), ","), "client_state,pending_mutations,rejected_mutations,sync_status,request_trace"; got != want {
		t.Fatalf("capture result keys = %q, want %q", got, want)
	}
}

func TestRetentionReconnectQueueAllowsInspectableRejection(t *testing.T) {
	coordinator := &RetentionReconnectCoordinator{
		sealedGeneration:  1,
		sealedBatchID:     "batch-runtime",
		sealedMutationIDs: []string{"mutation-runtime"},
	}
	capture := finalCapture{
		ClientState: json.RawMessage(`{"schema":{"version":1,"hash":"` + strings.Repeat("a", 64) + `"},"provenanceMaintenanceWorkCursor":"cursor","mutationLedgerCount":1}`),
		Pending:     json.RawMessage(`[{"mutationID":"mutation-runtime","status":"sealed"}]`),
		Rejected:    json.RawMessage(`[{"mutationID":"mutation-runtime","status":"rejected_terminal","code":"policy_rejected"}]`),
	}
	if err := coordinator.validateQueue(capture); err != nil {
		t.Fatalf("validate retention-reconnect queue with an inspectable rejection: %v", err)
	}
}

func TestRetentionReconnectQueueCountFailureNamesObservedValues(t *testing.T) {
	coordinator := &RetentionReconnectCoordinator{
		sealedGeneration:  1,
		sealedBatchID:     "batch-runtime",
		sealedMutationIDs: []string{"mutation-runtime"},
	}
	capture := finalCapture{
		ClientState: json.RawMessage(`{"schema":{"version":1,"hash":"` + strings.Repeat("a", 64) + `"},"provenanceMaintenanceWorkCursor":"cursor","mutationLedgerCount":1}`),
		Pending:     json.RawMessage(`[]`),
		Rejected:    json.RawMessage(`[{"mutationID":"mutation-runtime","status":"rejected_terminal","code":"policy_rejected"}]`),
	}
	err := coordinator.validateQueue(capture)
	if err == nil {
		t.Fatal("retention-reconnect queue count mismatch was accepted")
	}
	for _, wanted := range []string{"ledger count = 1", "pending count = 0", "sealed intent count = 1"} {
		if !strings.Contains(err.Error(), wanted) {
			t.Fatalf("retention-reconnect queue count error = %q, want component %q", err, wanted)
		}
	}
}

func TestRetentionReconnectProxyKeepsFaultUntilRelease(t *testing.T) {
	var forwarded atomic.Uint64
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		forwarded.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/sync/push":
			writer.WriteHeader(http.StatusConflict)
			_, _ = writer.Write([]byte(`{"error":{"code":"client_generation_expired","retryable":false}}`))
		case "/sync/connect":
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"client_generation":2}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	coordinator, err := NewRetentionReconnectCoordinator(RetentionReconnectCoordinatorConfig{
		Scenario: loadRetentionReconnectAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create retention-reconnect coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	sealed := `{"client_id":"client-a","client_generation":1,"batch_id":"runtime-batch","mutations":[{"mutation_id":"runtime-mutation"}]}`
	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		coordinator.proxyAdapter(response, httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(sealed)))
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("faulted push %d status = %d, want %d", attempt+1, response.Code, http.StatusServiceUnavailable)
		}
		if retryAfter := response.Header().Get("Retry-After"); retryAfter != "5" {
			t.Fatalf("faulted push %d Retry-After = %q, want 5", attempt+1, retryAfter)
		}
	}
	if forwarded.Load() != 0 {
		t.Fatalf("faulted pushes reached upstream %d times", forwarded.Load())
	}

	coordinator.releaseFault()
	rejectedResponse := httptest.NewRecorder()
	coordinator.proxyAdapter(rejectedResponse, httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(sealed)))
	if rejectedResponse.Code != http.StatusConflict {
		t.Fatalf("released sealed push status = %d, want %d", rejectedResponse.Code, http.StatusConflict)
	}
	select {
	case <-coordinator.rejectedPush:
	case <-time.After(time.Second):
		t.Fatal("released sealed push did not signal its rejection")
	}

	renewalResponse := httptest.NewRecorder()
	coordinator.proxyAdapter(renewalResponse, httptest.NewRequest(http.MethodPost, "/sync/connect", strings.NewReader(`{"client_id":"client-a","client_generation":1}`)))
	if renewalResponse.Code != http.StatusOK {
		t.Fatalf("renewal connect status = %d, want %d", renewalResponse.Code, http.StatusOK)
	}
	contextWithTimeout, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.waitForRenewedConnect(contextWithTimeout); err != nil {
		t.Fatalf("wait for renewed connect: %v", err)
	}
	if forwarded.Load() != 2 {
		t.Fatalf("released push and renewal forwarded %d times, want 2", forwarded.Load())
	}
}

func TestRetentionReconnectProxyRejectsChangedSealedBatch(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	coordinator, err := NewRetentionReconnectCoordinator(RetentionReconnectCoordinatorConfig{
		Scenario: loadRetentionReconnectAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create retention-reconnect coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	sealed := `{"client_id":"client-a","client_generation":1,"batch_id":"runtime-batch","mutations":[{"mutation_id":"runtime-mutation"}]}`
	initial := httptest.NewRecorder()
	coordinator.proxyAdapter(initial, httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(sealed)))
	if initial.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial sealed push status = %d, want %d", initial.Code, http.StatusServiceUnavailable)
	}
	changed := strings.Replace(sealed, "runtime-batch", "changed-batch", 1)
	response := httptest.NewRecorder()
	coordinator.proxyAdapter(response, httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(changed)))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("changed sealed push status = %d, want %d", response.Code, http.StatusBadGateway)
	}
}

func TestRetentionReconnectObservedIdentitiesUseTraceAndCapturedState(t *testing.T) {
	scenario := loadRetentionReconnectAuthoredScenario(t)
	localAlias := retentionReconnectPrimaryAliasForStep(t, scenario, retentionReconnectStepOrder[0])
	var authoredLocal string
	if err := json.Unmarshal(localAlias.Value, &authoredLocal); err != nil {
		t.Fatalf("decode local primary-key alias: %v", err)
	}

	commitAliases := retentionReconnectPrimaryAliasesForStep(t, scenario, retentionReconnectStepOrder[2])
	rows := make([]scenarios.RowFact, 0, len(commitAliases))
	for _, alias := range commitAliases {
		var primary string
		if err := json.Unmarshal(alias.Value, &primary); err != nil {
			t.Fatalf("decode committed primary-key alias %q: %v", alias.Alias, err)
		}
		encoded, err := json.Marshal(primary)
		if err != nil {
			t.Fatalf("encode committed primary-key alias %q: %v", alias.Alias, err)
		}
		rows = append(rows, scenarios.RowFact{CanonicalWireJSON: string(encoded), Version: "runtime-version", Checksum: strings.Repeat("a", 64)})
	}
	scopeAlias := retentionReconnectAliasForKind(t, scenario, "scope")
	var scopeID string
	if err := json.Unmarshal(scopeAlias.Value, &scopeID); err != nil {
		t.Fatalf("decode scope alias: %v", err)
	}

	coordinator := &RetentionReconnectCoordinator{
		identities:        scenario.NativeIdentityAliases,
		localIntent:       retentionReconnectPrimaryKey{authored: authoredLocal, runtime: "runtime-local"},
		pinnedRebuildID:   "runtime-rebuild",
		sealedBatchID:     "runtime-batch",
		sealedMutationIDs: []string{"runtime-mutation"},
		sealedGeneration:  17,
		traceEvidence:     &retentionReconnectTraceEvidence{generation: 17, scopeSet: 23},
	}
	runtime, err := coordinator.observedIdentityValues(scenarios.StateFacts{
		Rows: rows,
		Rebuilds: []scenarios.RebuildFact{{
			ScopeID: scopeID, RebuildID: "runtime-rebuild",
		}},
	})
	if err != nil {
		t.Fatalf("resolve observed retention-reconnect identities: %v", err)
	}

	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "client-generation").Alias], "17")
	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "scope-set-version").Alias], "23")
	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "mutation-id").Alias], `"runtime-mutation"`)
	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "batch-id").Alias], `"runtime-batch"`)
	assertRetentionReconnectRawJSON(t, runtime[localAlias.Alias], `"runtime-local"`)
	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "row-version").Alias], `"runtime-version"`)
	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "checksum").Alias], `"`+strings.Repeat("a", 64)+`"`)
	assertRetentionReconnectRawJSON(t, runtime[retentionReconnectAliasForKind(t, scenario, "rebuild-id").Alias], `"runtime-rebuild"`)
}

func TestValidateRetentionReconnectCompactionRequiresActivePin(t *testing.T) {
	scenario := loadRetentionReconnectAuthoredScenario(t)
	pin := retentionReconnectScenarioStep(t, &scenario, retentionReconnectStepOrder[4]).Operation
	var payload struct {
		ScopeID   string `json:"scope_id"`
		RebuildID string `json:"rebuild_id"`
		Limit     uint64 `json:"limit"`
	}
	if err := json.Unmarshal(pin.Payload, &payload); err != nil {
		t.Fatalf("decode rebuild pin: %v", err)
	}
	server := scenarios.StateFacts{
		Scopes:   []scenarios.ScopeFact{{ScopeID: payload.ScopeID}},
		Rebuilds: []scenarios.RebuildFact{{ScopeID: payload.ScopeID, RebuildID: payload.RebuildID, PageLimit: payload.Limit, HasContinuation: true}},
	}
	if err := validateRetentionReconnectCompaction(server, pin); err != nil {
		t.Fatalf("validate active rebuild pin: %v", err)
	}
	server.Rebuilds[0].HasContinuation = false
	if err := validateRetentionReconnectCompaction(server, pin); err == nil {
		t.Fatal("compaction accepted a released rebuild pin")
	}
}

func loadRetentionReconnectAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadRetentionReconnectScenario(context.Background(), repositoryRoot)
	if err != nil {
		t.Fatalf("load authored retention-reconnect scenario: %v", err)
	}
	return scenario
}

func cloneRetentionReconnectScenario(t *testing.T, scenario scenarios.Scenario) scenarios.Scenario {
	t.Helper()
	encoded, err := json.Marshal(scenario)
	if err != nil {
		t.Fatalf("encode retention-reconnect scenario: %v", err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(encoded, &clone); err != nil {
		t.Fatalf("decode retention-reconnect scenario: %v", err)
	}
	return clone
}

func retentionReconnectScenarioStep(t *testing.T, scenario *scenarios.Scenario, id scenarios.StepID) *scenarios.Step {
	t.Helper()
	for index := range scenario.Steps {
		if scenario.Steps[index].ID == id {
			return &scenario.Steps[index]
		}
	}
	t.Fatalf("retention-reconnect step %s is absent", id)
	return nil
}

func retentionReconnectScenarioWire(t *testing.T, scenario *scenarios.Scenario, id scenarios.StepID) *scenarios.WireExpectation {
	t.Helper()
	for index := range scenario.WireExpectations {
		if scenario.WireExpectations[index].StepID == id {
			return &scenario.WireExpectations[index]
		}
	}
	t.Fatalf("retention-reconnect wire %s is absent", id)
	return nil
}

func retentionReconnectAliasForKind(t *testing.T, scenario scenarios.Scenario, kind string) scenarios.NativeIdentityAlias {
	t.Helper()
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Kind == kind {
			return alias
		}
	}
	t.Fatalf("retention-reconnect alias kind %q is absent", kind)
	return scenarios.NativeIdentityAlias{}
}

func retentionReconnectPrimaryAliasForStep(t *testing.T, scenario scenarios.Scenario, id scenarios.StepID) scenarios.NativeIdentityAlias {
	t.Helper()
	aliases := retentionReconnectPrimaryAliasesForStep(t, scenario, id)
	if len(aliases) != 1 {
		t.Fatalf("retention-reconnect primary aliases for %s = %d, want 1", id, len(aliases))
	}
	return aliases[0]
}

func retentionReconnectPrimaryAliasesForStep(t *testing.T, scenario scenarios.Scenario, id scenarios.StepID) []scenarios.NativeIdentityAlias {
	t.Helper()
	aliases := make([]scenarios.NativeIdentityAlias, 0)
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Kind != "primary-key" {
			continue
		}
		for _, owner := range alias.StepIDs {
			if owner == id {
				aliases = append(aliases, alias)
				break
			}
		}
	}
	if len(aliases) == 0 {
		t.Fatalf("retention-reconnect primary aliases for %s are absent", id)
	}
	return aliases
}

func assertRetentionReconnectRawJSON(t *testing.T, actual json.RawMessage, wanted string) {
	t.Helper()
	if string(actual) != wanted {
		t.Fatalf("runtime identity = %s, want %s", actual, wanted)
	}
}
