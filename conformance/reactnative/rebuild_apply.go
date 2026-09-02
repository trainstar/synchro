package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	rebuildApplyScenarioPath = "conformance/scenarios/performance/rebuild-apply-001.json"
	rebuildApplyScenarioID   = "SCN-PERF-REBUILD-APPLY-001"
)

var rebuildApplyAliasNames = []string{"client-generation-one", "current-schema", "scope-a", "items-table"}

type rebuildApplyWorkload struct {
	Profile     string `json:"profile"`
	ScopeID     string `json:"scope_id"`
	RecordCount uint64 `json:"record_count"`
	PageSize    uint64 `json:"page_size"`
}

// RebuildApplyCoordinatorConfig configures one rebuild-apply sidecar.
type RebuildApplyCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
}

// RebuildApplyCoordinatorResult contains final server and identity evidence.
type RebuildApplyCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// RebuildApplyCoordinator drives each authored workload through one native bridge.
type RebuildApplyCoordinator struct {
	config RebuildApplyCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string

	steps      []scenarios.Step
	workloads  []rebuildApplyWorkload
	expanded   [][]scenarios.Operation
	expected   *scenarios.StateFacts
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	tableName  string

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	stage     rebuildApplyStage
	nextSeq   uint64
	current   int
	process   *actionProcessIdentity
	traces    []traceSnapshot
	result    RebuildApplyCoordinatorResult
}

type rebuildApplyStage uint8

const (
	rebuildApplyStageOpen rebuildApplyStage = iota
	rebuildApplyStageSynchronize
	rebuildApplyStageCapture
	rebuildApplyStageComplete
)

// LoadRebuildApplyScenario loads the authored rebuild-apply contract.
func LoadRebuildApplyScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, rebuildApplyScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native rebuild-apply scenario: %w", err)
	}
	if err := ValidateRebuildApplyScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateRebuildApplyScenario rejects changes to the closed RN contract.
func ValidateRebuildApplyScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != rebuildApplyScenarioID || len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native rebuild-apply scenario contract is invalid")
	}
	if len(scenario.Steps) == 0 || len(scenario.NativeLifecycleBoundaries) != 0 || len(scenario.NativeIdentityAliases) != len(rebuildApplyAliasNames) {
		return errors.New("React Native rebuild-apply scenario structure is invalid")
	}
	seenClients := make(map[string]struct{}, len(scenario.Steps))
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "workload" || binding.Workload == nil || binding.UserID == "" || binding.ClientID == "" || step.ExpectedOutcome.Disposition != "success" {
			return errors.New("React Native rebuild-apply workload binding is invalid")
		}
		var workload rebuildApplyWorkload
		if json.Unmarshal(step.Operation.Payload, &workload) != nil || workload.Profile != "scope_cardinality" || workload.ScopeID == "" || workload.RecordCount == 0 || workload.PageSize == 0 || workload.RecordCount != binding.Workload.RecordCount || len(binding.Workload.Targets) != 1 || binding.Workload.Targets[0].ScopeID != workload.ScopeID || binding.Workload.Targets[0].TableID != "items" || binding.Workload.Targets[0].PrimaryKeyFieldID != "id" {
			return errors.New("React Native rebuild-apply workload payload is invalid")
		}
		if _, duplicate := seenClients[binding.ClientID]; duplicate {
			return errors.New("React Native rebuild-apply workload client is not fresh")
		}
		seenClients[binding.ClientID] = struct{}{}
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native rebuild-apply identity alias is invalid")
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range rebuildApplyAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native rebuild-apply identity alias %q is absent", name)
		}
	}
	semantic, performance := false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-REBUILD-APPLY-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-REBUILD-APPLY-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !performance || rebuildApplyExpectedState(scenario) == nil {
		return errors.New("React Native rebuild-apply assertions are invalid")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-REBUILD-APPLY-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-REBUILD-APPLY-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-REBUILD-APPLY-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-REBUILD-APPLY-001", "CTRL-REBUILD-002") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-PERF-REBUILD-APPLY-RN-IOS-CURRENT-001"] != 1 || obligations["OBL-PERF-REBUILD-APPLY-RN-ANDROID-CURRENT-001"] != 1 || obligations["OBL-PERF-REBUILD-APPLY-CONTROL-001"] != 1 {
		return errors.New("React Native rebuild-apply proof obligations are invalid")
	}
	return nil
}

// NewRebuildApplyCoordinator creates an authenticated host-loopback sidecar.
func NewRebuildApplyCoordinator(config RebuildApplyCoordinatorConfig) (*RebuildApplyCoordinator, error) {
	if err := ValidateRebuildApplyScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native rebuild-apply coordinator platform must be ios or android")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native rebuild-apply coordinator auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	adapter, err := nativeAdapterURL(serverURL, config.Platform)
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native rebuild-apply coordinator capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native rebuild-apply coordinator")
	}
	coordinator := &RebuildApplyCoordinator{config: config, listener: listener, token: token, adapter: adapter, expected: rebuildApplyExpectedState(config.Scenario), identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...), runtimeIDs: make(map[string]json.RawMessage), nextSeq: 1}
	coordinator.server = &http.Server{Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return coordinator, nil
}

// Prepare installs the contract and derives every source workload operation.
func (c *RebuildApplyCoordinator) Prepare(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCoordinatorUnavailable
	}
	if c.prepared {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native rebuild-apply coordinator dependencies are unavailable")
	}
	if c.config.AuthToken == "" {
		token, err := c.config.Harness.NativeBearerToken(ctx, "user-a", time.Now())
		if err != nil {
			return errors.New("mint React Native rebuild-apply adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native rebuild-apply contract: %w", err)
	}
	model, err := modelrunner.RunScenario(ctx, c.config.Scenario)
	if err != nil || !model.Passed || len(model.Steps) != len(c.config.Scenario.Steps) {
		return errors.New("derive React Native rebuild-apply source operations")
	}
	for index, step := range c.config.Scenario.Steps {
		var workload rebuildApplyWorkload
		if json.Unmarshal(step.Operation.Payload, &workload) != nil {
			return errors.New("decode React Native rebuild-apply workload")
		}
		if model.Steps[index].StepID != step.ID {
			return errors.New("React Native rebuild-apply model step order changed")
		}
		c.steps, c.workloads, c.expanded = append(c.steps, step), append(c.workloads, workload), append(c.expanded, model.Steps[index].Expanded)
	}
	if err := c.bindServerIdentities(); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve runs the sidecar until it closes.
func (c *RebuildApplyCoordinator) Serve(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	if err := c.Prepare(ctx); err != nil {
		return err
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(closeCtx)
			cancel()
		case <-stop:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (c *RebuildApplyCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}
func (c *RebuildApplyCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}
func (c *RebuildApplyCoordinator) Completed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c != nil && c.completed && c.failed == nil
}
func (c *RebuildApplyCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	return len(c.config.Scenario.Steps)*int(rebuildApplyStageComplete) + 1
}
func (c *RebuildApplyCoordinator) Result() (RebuildApplyCoordinatorResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	served := uint64(0)
	if c.nextSeq > 0 {
		served = c.nextSeq - 1
	}
	if c.failed != nil {
		return RebuildApplyCoordinatorResult{}, fmt.Errorf("%w (exchanges served=%d versus ExchangeCount=%d)", c.failed, served, c.ExchangeCount())
	}
	if !c.completed {
		return RebuildApplyCoordinatorResult{}, fmt.Errorf("React Native rebuild-apply coordinator has not completed (exchanges served=%d versus ExchangeCount=%d)", served, c.ExchangeCount())
	}
	return c.result, nil
}

func (c *RebuildApplyCoordinator) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	shutdownErr, listenErr := c.server.Shutdown(ctx), c.listener.Close()
	if shutdownErr != nil {
		return shutdownErr
	}
	if listenErr != nil && !errors.Is(listenErr, net.ErrClosed) {
		return listenErr
	}
	return nil
}

func (c *RebuildApplyCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/exchange" {
		writeExchangeError(writer, http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writeExchangeError(writer, http.StatusMethodNotAllowed)
		return
	}
	if !validBearer(request.Header.Get("Authorization"), c.token) {
		writeExchangeError(writer, http.StatusUnauthorized)
		return
	}
	if request.Header.Get("Content-Type") != "application/json" || request.ContentLength > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusUnsupportedMediaType)
		return
	}
	body, err := ioReadAll(request)
	if err != nil || len(body) > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusRequestEntityTooLarge)
		return
	}
	exchange, err := decodeExchangeRequest(body)
	if err != nil {
		writeExchangeError(writer, http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.prepared || c.failed != nil || c.completed || exchange.Sequence != c.nextSeq {
		c.failed = errors.New("React Native rebuild-apply exchange is unavailable or non-monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native rebuild-apply exchange %d failed: %w", exchange.Sequence, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(request.Context(), exchange.Sequence)
	if err != nil {
		c.failed = err
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = errors.New("React Native rebuild-apply exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *RebuildApplyCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == rebuildApplyStageOpen {
		if !isJSONNull(raw) {
			return errInvalidExchange
		}
		return nil
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return errInvalidExchange
	}
	switch c.stage {
	case rebuildApplyStageSynchronize:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
	case rebuildApplyStageCapture:
		if err := c.validateSynchronized(envelope.Result); err != nil {
			return err
		}
	case rebuildApplyStageComplete:
		capture, err := decodeCapture(envelope.Result, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof"})
		if err != nil {
			return err
		}
		if err := c.validateCapture(capture); err != nil {
			return err
		}
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *RebuildApplyCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	if c.stage == rebuildApplyStageComplete && c.current == len(c.steps) {
		if err := c.finish(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State, response.Command, c.completed = "complete", nil, true
		return response, nil
	}
	if c.current >= len(c.steps) {
		return exchangeResponse{}, errors.New("React Native rebuild-apply workload index is invalid")
	}
	step, workload := c.steps[c.current], c.workloads[c.current]
	clientKey := "rebuild-apply-" + step.NativeBinding.ClientID
	switch c.stage {
	case rebuildApplyStageOpen:
		if err := c.executeSource(ctx, c.expanded[c.current], workload, c.priorRecordCount()); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command(clientKey, step.NativeBinding.ClientID, "client", "open", map[string]any{"client_key": clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil}, nil)
		c.stage = rebuildApplyStageSynchronize
	case rebuildApplyStageSynchronize:
		response.Command = c.command(clientKey, step.NativeBinding.ClientID, "client", "synchronize-step", map[string]any{"client_key": clientKey, "method": "start", "completion": "idle"}, nil)
		c.stage = rebuildApplyStageCapture
	case rebuildApplyStageCapture:
		response.Command = c.command(clientKey, step.NativeBinding.ClientID, "observer", "capture", map[string]any{
			"client_keys":   []string{clientKey},
			"sources":       []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"},
			"detail_policy": "complete-or-omit",
			"durable_proof_identity": map[string]any{
				"table_name": c.tableName, "record_id": "rebuild-apply-absent-row",
			},
		}, nil)
		c.stage = rebuildApplyStageComplete
	case rebuildApplyStageComplete:
		c.current++
		if c.current == len(c.steps) {
			return c.advanceLocked(ctx, sequence)
		}
		c.stage = rebuildApplyStageOpen
		return c.advanceLocked(ctx, sequence)
	}
	return response, nil
}

func (c *RebuildApplyCoordinator) command(key, id, actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, stepID := range stepIDs {
		step := c.stepsByID()[stepID]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: step.Operation.ContractOperation, Name: step.Operation.Name, Payload: copyRaw(step.Operation.Payload)}})
	}
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: conformanceRuntime{ClientKey: key, Database: "rn-rebuild-apply-" + id + ".db", ClientID: id, ServerURL: c.adapter, AuthToken: c.config.AuthToken}}
}

func (c *RebuildApplyCoordinator) stepsByID() map[scenarios.StepID]scenarios.Step {
	values := make(map[scenarios.StepID]scenarios.Step, len(c.steps))
	for _, step := range c.steps {
		values[step.ID] = step
	}
	return values
}
func (c *RebuildApplyCoordinator) priorRecordCount() uint64 {
	if c.current == 0 {
		return 0
	}
	return c.workloads[c.current-1].RecordCount
}

func (c *RebuildApplyCoordinator) validateSynchronized(raw json.RawMessage) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if decodeStrictMembers(raw, &members, 4, "rebuild-apply synchronized result") != nil {
		return errInvalidExchange
	}
	var completion string
	if json.Unmarshal(members["completion"], &completion) != nil || completion != "idle" || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native rebuild-apply synchronization is invalid")
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return errors.New("React Native rebuild-apply process identity changed")
	}
	return nil
}

func (c *RebuildApplyCoordinator) validateCapture(capture finalCapture) error {
	if c.current >= len(c.steps) {
		return errors.New("React Native rebuild-apply capture has no workload")
	}
	if validateEmptyArray(capture.Pending) != nil || validateEmptyArray(capture.Rejected) != nil || validateReadyStatus(capture.Status) != nil {
		return errors.New("React Native rebuild-apply queues or status are invalid")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	workload := c.workloads[c.current]
	clientID := c.steps[c.current].NativeBinding.ClientID
	expected := c.expectedClient(clientID)
	if expected == nil || expected.RowCount == nil || expected.ProvenanceCount == nil || expected.CheckpointCount == nil || expected.RebuildAttemptCount == nil {
		return errors.New("React Native rebuild-apply authored client state is unavailable")
	}
	var provenance []clientScopeRow
	if err := decodeStrictValue(capture.Provenance, &provenance); err != nil {
		return fmt.Errorf("React Native rebuild-apply client %s provenance detail is invalid: %w", clientID, err)
	}
	detailCount := workload.RecordCount
	if detailCount > 512 {
		detailCount = 0
	}
	counts := []struct {
		name     string
		observed uint64
		expected uint64
	}{
		{"application rows", state.ApplicationRowCount, *expected.RowCount},
		{"provenance", state.ProvenanceCount, *expected.ProvenanceCount},
		{"scope states", state.ScopeStateCount, *expected.CheckpointCount},
		{"scope rows", state.ScopeRowCount, workload.RecordCount},
		{"scope state details", uint64(len(state.ScopeStates)), *expected.CheckpointCount},
		{"provenance details", uint64(len(provenance)), detailCount},
		{"active rebuild attempts", state.RebuildAttemptCount, 0},
		{"active rebuild attempt details", uint64(len(state.RebuildAttempts)), state.RebuildAttemptCount},
	}
	counts = append(counts, struct {
		name     string
		observed uint64
		expected uint64
	}{"scope row details", uint64(len(state.ScopeRows)), detailCount})
	for _, count := range counts {
		if count.observed != count.expected {
			return rebuildApplyCountError(clientID, count.name, count.observed, count.expected)
		}
	}
	for index := range provenance {
		if provenance[index] != state.ScopeRows[index] {
			return fmt.Errorf("React Native rebuild-apply client %s provenance detail %d differs from scope state", clientID, index+1)
		}
	}
	proof, err := decodeDurableProof(capture.DurableProof)
	if err != nil {
		return fmt.Errorf("React Native rebuild-apply client %s terminal rebuild proof is invalid: %w", clientID, err)
	}
	wantPages := (workload.RecordCount + workload.PageSize - 1) / workload.PageSize
	if state.RebuildReceiptCount != wantPages {
		return rebuildApplyCountError(clientID, "receipt pages", state.RebuildReceiptCount, wantPages)
	}
	var receiptPages uint64
	for _, receipt := range proof.RebuildReceiptProofs {
		if receipt.PageCount == 0 {
			return rebuildApplyCountError(clientID, "terminal receipt page group", receipt.PageCount, 1)
		}
		if receiptPages > wantPages || receipt.PageCount > wantPages-receiptPages {
			return rebuildApplyCountError(clientID, "terminal receipt pages", receiptPages+receipt.PageCount, wantPages)
		}
		receiptPages += receipt.PageCount
	}
	for _, count := range []struct {
		name     string
		observed uint64
		expected uint64
	}{
		{"terminal receipt pages", receiptPages, wantPages},
		{"terminal receipt attempts", uint64(len(proof.RebuildReceiptProofs)), *expected.RebuildAttemptCount},
	} {
		if count.observed != count.expected {
			return rebuildApplyCountError(clientID, count.name, count.observed, count.expected)
		}
	}
	rebuildAttempts, err := rebuildAttemptFactCount(state.RebuildAttempts, proof.RebuildReceiptProofs)
	if err != nil {
		return fmt.Errorf("React Native rebuild-apply client %s rebuild attempt facts are invalid: %w", clientID, err)
	}
	if rebuildAttempts != *expected.RebuildAttemptCount {
		return rebuildApplyCountError(clientID, "rebuild attempt facts", rebuildAttempts, *expected.RebuildAttemptCount)
	}
	if len(capture.Events) == 0 {
		return errors.New("React Native rebuild-apply durable evidence is incomplete")
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	if err := validateRebuildApplyTrace(trace, workload, len(c.steps[c.current].NativeBinding.Workload.Targets)); err != nil {
		return err
	}
	c.traces = append(c.traces, trace)
	return nil
}

func rebuildApplyCountError(clientID, name string, observed, expected uint64) error {
	return fmt.Errorf("React Native rebuild-apply client %s %s count: observed=%d expected=%d", clientID, name, observed, expected)
}

func validateRebuildApplyTrace(trace traceSnapshot, workload rebuildApplyWorkload, scopeCount int) error {
	pages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	if trace.Overflowed || len(trace.Observations) != pages+2 || trace.SequenceCheckpoint != uint64(len(trace.Observations)) || validateTraceSequence(trace.Observations) != nil || validateTraceOperation(trace.Observations[0], "connect") != nil || validateTraceOperation(trace.Observations[len(trace.Observations)-1], "pull") != nil {
		return errors.New("React Native rebuild-apply trace is invalid")
	}
	for index := 0; index < pages; index++ {
		observation := trace.Observations[index+1]
		if validateTraceOperation(observation, "rebuild") != nil {
			return fmt.Errorf("React Native rebuild-apply page %d is invalid", index+1)
		}
		limit, limitErr := requestInteger(observation, "limit")
		cursor, cursorErr := requestStringOptional(observation, "cursor_fingerprint")
		facts, factsErr := decodeRebuildResponseFacts(observation.RebuildResponseFacts)
		remaining := workload.RecordCount - uint64(index)*workload.PageSize
		want := workload.PageSize
		if remaining < want {
			want = remaining
		}
		if limitErr != nil || limit != workload.PageSize || factsErr != nil || facts.RecordCount == nil || *facts.RecordCount != want || cursorErr != nil || index == 0 && cursor != "" || index > 0 && cursor == "" {
			return fmt.Errorf("React Native rebuild-apply page %d binding is invalid", index+1)
		}
		terminal := index == pages-1
		if terminal && (*facts.HasMore || *facts.HasCursor || !*facts.HasFinalScopeCursor || !*facts.HasChecksum) {
			return errors.New("React Native rebuild-apply finality is invalid")
		}
		if !terminal && (!*facts.HasMore || !*facts.HasCursor || *facts.HasFinalScopeCursor || *facts.HasChecksum) {
			return errors.New("React Native rebuild-apply continuation is invalid")
		}
	}
	pull := trace.Observations[len(trace.Observations)-1]
	if count, err := requestInteger(pull, "scope_count"); err != nil || count != uint64(scopeCount) {
		return errors.New("React Native rebuild-apply final pull scope is invalid")
	}
	return nil
}

func requestStringOptional(observation transportObservation, name string) (string, error) {
	var facts map[string]json.RawMessage
	if json.Unmarshal(observation.RequestFacts, &facts) != nil {
		return "", errors.New("React Native request facts are invalid")
	}
	raw, found := facts[name]
	if !found || isJSONNull(raw) {
		return "", nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || value == "" {
		return "", errors.New("React Native request fact is invalid")
	}
	return value, nil
}

func (c *RebuildApplyCoordinator) executeSource(ctx context.Context, operations []scenarios.Operation, workload rebuildApplyWorkload, prior uint64) error {
	committed, materialized, pages := false, false, 0
	for _, operation := range operations {
		switch scenarios.OperationKey(operation) {
		case "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "local/begin-rebuild", "rebuild/request-page", "local/apply-rebuild-page", "local/finalize-rebuild":
			if scenarios.OperationKey(operation) == "rebuild/request-page" {
				pages++
			}
		case "model/commit-source-transaction":
			if committed || validateRebuildApplyCommit(operation, prior, workload.RecordCount) != nil {
				return errors.New("React Native rebuild-apply source commit is invalid")
			}
			result, err := c.config.Controller.ApplyStep(ctx, operation)
			if err != nil || result.Disposition != "success" {
				return fmt.Errorf("apply React Native rebuild-apply source: %w", nativeResultError(err, result.Disposition))
			}
			committed = true
		case "process/materialize-source-transaction":
			if !committed || materialized {
				return errors.New("React Native rebuild-apply materialization is out of order")
			}
			result, err := c.config.Controller.ProcessStep(ctx, nil, operation)
			if err != nil || result.Disposition != "success" {
				return fmt.Errorf("materialize React Native rebuild-apply source: %w", nativeResultError(err, result.Disposition))
			}
			materialized = true
		default:
			return fmt.Errorf("React Native rebuild-apply operation %q is unsupported", scenarios.OperationKey(operation))
		}
	}
	if !committed || !materialized || pages != int((workload.RecordCount+workload.PageSize-1)/workload.PageSize) {
		return errors.New("React Native rebuild-apply source expansion is incomplete")
	}
	return nil
}

func validateRebuildApplyCommit(operation scenarios.Operation, prior, current uint64) error {
	var payload struct {
		Events []struct {
			Operation string `json:"operation"`
		} `json:"events"`
	}
	if json.Unmarshal(operation.Payload, &payload) != nil || len(payload.Events) == 0 {
		return errors.New("rebuild-apply source commit payload is invalid")
	}
	wantCount, wantOperation := uint64(1), "update"
	if current > prior {
		wantCount, wantOperation = current-prior, "insert"
	}
	if uint64(len(payload.Events)) != wantCount {
		return errors.New("rebuild-apply source event count is invalid")
	}
	for _, event := range payload.Events {
		if event.Operation != wantOperation {
			return errors.New("rebuild-apply source event operation is invalid")
		}
	}
	return nil
}

func (c *RebuildApplyCoordinator) bindServerIdentities() error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native rebuild-apply server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		if value.Alias == "items-table" {
			c.tableName = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" {
		return errors.New("React Native rebuild-apply table identity is unavailable")
	}
	return nil
}

func (c *RebuildApplyCoordinator) finish(ctx context.Context) error {
	if len(c.traces) != len(c.steps) {
		return errors.New("React Native rebuild-apply trace evidence is incomplete")
	}
	var generation uint64
	for _, trace := range c.traces {
		for _, observation := range trace.Observations {
			value, err := requestInteger(observation, "client_generation")
			if err != nil || value == 0 {
				return errors.New("React Native rebuild-apply client generation is absent")
			}
			if generation == 0 {
				generation = value
			} else if generation != value {
				return errors.New("React Native rebuild-apply client generation changed")
			}
		}
	}
	encoded, err := json.Marshal(generation)
	if err != nil {
		return err
	}
	c.runtimeIDs["client-generation-one"] = encoded
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return fmt.Errorf("React Native rebuild-apply alias %q has no runtime evidence", alias.Alias)
		}
		for _, stepID := range alias.StepIDs {
			owner := stepID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &owner, RuntimeValue: value})
		}
		for _, expectationID := range alias.ExpectationIDs {
			owner := expectationID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &owner, RuntimeValue: value})
		}
	}
	resolutions, err := blackbox.ResolveNativeIdentityAliases(c.identities, observations)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(c.steps))
	for _, step := range c.steps {
		keys = append(keys, "rebuild-apply-"+step.NativeBinding.ClientID)
	}
	sort.Strings(keys)
	captures, err := c.config.Controller.Capture(ctx, keys, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return fmt.Errorf("capture React Native rebuild-apply server state: %w", nativeResultError(err, ""))
	}
	if err := validateServerState(*c.expected, captures[0].StateFacts); err != nil {
		return err
	}
	c.result = RebuildApplyCoordinatorResult{ServerFacts: captures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *RebuildApplyCoordinator) expectedClient(id string) *scenarios.ClientDurabilityFact {
	for index := range c.expected.Clients {
		if c.expected.Clients[index].ClientID == id {
			return &c.expected.Clients[index]
		}
	}
	return nil
}
func rebuildApplyExpectedState(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		value := scenario.Model.ExpectedState[index]
		if value.ID == "EXPECT-PERF-REBUILD-APPLY-SEMANTIC-001" && value.StateFacts != nil {
			return value.StateFacts
		}
	}
	return nil
}
