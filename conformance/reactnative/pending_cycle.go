package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	pendingCycleScenarioPath = "conformance/scenarios/performance/pending-cycle-001.json"
	pendingCycleScenarioID   = "SCN-PERF-PENDING-CYCLE-001"

	// Installing the current schema performs connect, rebuild, and pull.
	pendingCycleBootstrapRequests = 3

	// A pull before its push materializes returns capture_pending.
	pendingCycleCapturePendingStatus = 503
)

var pendingCycleStepOrder = []scenarios.StepID{
	"STEP-PERF-PENDING-CYCLE-001",
	"STEP-PERF-PENDING-CYCLE-002",
	"STEP-PERF-PENDING-CYCLE-MATERIALIZE-001",
	"STEP-PERF-PENDING-CYCLE-003",
}

var pendingCycleAliasNames = []string{
	"pending-mutation",
	"pending-batch",
	"client-generation-one",
	"current-schema",
	"items-table",
	"pending-row-primary-key",
	"scope-a",
	"scope-set-version-one",
}

// LoadPendingCycleScenario loads only the authored pending-cycle scenario.
func LoadPendingCycleScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, pendingCycleScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native pending-cycle scenario: %w", err)
	}
	if err := ValidatePendingCycleScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidatePendingCycleScenario rejects changes to the closed RN pending-cycle contract.
func ValidatePendingCycleScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != pendingCycleScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native pending-cycle scenario contract is invalid")
	}
	if len(scenario.Steps) != len(pendingCycleStepOrder) {
		return errors.New("React Native pending-cycle step set changed")
	}
	for index, step := range scenario.Steps {
		if step.ID != pendingCycleStepOrder[index] || step.NativeBinding == nil {
			return errors.New("React Native pending-cycle step order or binding changed")
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 0 || len(scenario.NativeIdentityAliases) != len(pendingCycleAliasNames) {
		return errors.New("React Native pending-cycle lifecycle or identity contract changed")
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native pending-cycle identity alias is invalid")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return errors.New("React Native pending-cycle identity alias is duplicated")
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range pendingCycleAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native pending-cycle identity alias %q is absent", name)
		}
	}
	if scenario.Steps[0].NativeBinding.Kind != "local-write" || scenario.Steps[1].NativeBinding.Kind != "public-call" ||
		scenario.Steps[2].NativeBinding.Kind != "controller" || scenario.Steps[3].NativeBinding.Kind != "public-call" {
		return errors.New("React Native pending-cycle native binding kinds changed")
	}
	if scenarios.OperationKey(scenario.Steps[0].Operation) != "local/write" ||
		scenarios.OperationKey(scenario.Steps[1].Operation) != "push/submit" ||
		scenarios.OperationKey(scenario.Steps[2].Operation) != "process/materialize-source-transaction" ||
		scenarios.OperationKey(scenario.Steps[3].Operation) != "pull/request-page" {
		return errors.New("React Native pending-cycle operation set changed")
	}
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" {
			return errors.New("React Native pending-cycle expected outcome is absent")
		}
	}
	semantic, performance := false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-PENDING-CYCLE-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-PENDING-CYCLE-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !performance {
		return errors.New("React Native pending-cycle assertion contract changed")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-PENDING-CYCLE-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-PENDING-CYCLE-001", "CTRL-MUTATION-002") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-PERF-PENDING-CYCLE-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-CONTROL-001"] != 1 {
		return errors.New("React Native pending-cycle proof obligations are invalid")
	}
	return nil
}

// PendingCycleCoordinatorConfig configures one authenticated RN pending-cycle sidecar.
type PendingCycleCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// PendingCycleCoordinator is the command sidecar for one RN pending-cycle run.
type PendingCycleCoordinator struct {
	config   PendingCycleCoordinatorConfig
	listener net.Listener
	server   *http.Server
	token    string
	adapter  string
	database string

	steps      map[scenarios.StepID]scenarios.Step
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	userID     string
	clientID   string
	clientKey  string
	tableName  string
	primaryKey string

	mu          sync.Mutex
	prepared    bool
	closed      bool
	completed   bool
	failed      error
	stage       pendingCycleStage
	nextSeq     uint64
	process     *actionProcessIdentity
	finalResult *finalCapture
	result      PendingCycleCoordinatorResult
}

type pendingCycleStage uint8

const (
	pendingCycleStageOpen pendingCycleStage = iota
	// The authored write targets the current application schema, so the client
	// installs that schema before it writes. The Swift consumer performs the
	// same start call through its current initialization path.
	pendingCycleStageInstallCurrent
	// The engine rejects a second start, so the install call stops the client
	// before the authored push starts it again. initializeCurrent in
	// conformance/swift/platform.go performs the same stop.
	pendingCycleStageInstallStop
	pendingCycleStageLocalWrite
	pendingCycleStagePush
	pendingCycleStagePull
	pendingCycleStageFinalCapture
	pendingCycleStageApplicationRows
	pendingCycleStageComplete
)

// PendingCycleCoordinatorResult contains validated server and native identity evidence.
type PendingCycleCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// NewPendingCycleCoordinator creates an authenticated host-loopback listener.
func NewPendingCycleCoordinator(config PendingCycleCoordinatorConfig) (*PendingCycleCoordinator, error) {
	if err := ValidatePendingCycleScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native pending-cycle coordinator platform must be ios or android")
	}
	identity, err := extractPendingCycleClientIdentity(config.Scenario)
	if err != nil {
		return nil, err
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native pending-cycle coordinator auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	adapterURL, err := nativeAdapterURL(serverURL, config.Platform)
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native pending-cycle coordinator capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-pending-cycle-")
		if err != nil {
			return nil, errors.New("create React Native pending-cycle private database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native pending-cycle database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native pending-cycle coordinator")
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	coordinator := &PendingCycleCoordinator{
		config: config, listener: listener, token: token, adapter: adapterURL, database: database,
		steps: steps, identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), userID: identity.userID, clientID: identity.clientID, clientKey: identity.clientID,
		nextSeq: 1,
		server:  &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second},
	}
	coordinator.server.Handler = coordinator
	return coordinator, nil
}

// Prepare installs the authored model and binds initial runtime identities.
func (c *PendingCycleCoordinator) Prepare(ctx context.Context) error {
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.config.AuthToken == "" && c.config.Harness != nil {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.userID, time.Now())
		if err != nil {
			return errors.New("mint React Native pending-cycle adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native pending-cycle coordinator dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native pending-cycle contract: %w", err)
	}
	localWrite, err := c.config.Controller.ApplicationWrite(c.steps[pendingCycleStepOrder[0]].Operation)
	if err != nil {
		return fmt.Errorf("bind React Native pending mutation to the application schema: %w", err)
	}
	localStep := c.steps[pendingCycleStepOrder[0]]
	localStep.Operation = localWrite
	c.steps[pendingCycleStepOrder[0]] = localStep
	if err := c.bindRuntimeIdentities(false); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *PendingCycleCoordinator) Serve(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	if err := c.Prepare(ctx); err != nil {
		return err
	}
	shutdown := make(chan struct{})
	defer close(shutdown)
	go func() {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(shutdownContext)
			cancel()
		case <-shutdown:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (c *PendingCycleCoordinator) Handler() http.Handler { return c }

// URL returns the host-loopback sidecar URL for every platform.
func (c *PendingCycleCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *PendingCycleCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

func (c *PendingCycleCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *PendingCycleCoordinator) Result() (PendingCycleCoordinatorResult, error) {
	if c == nil {
		return PendingCycleCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return PendingCycleCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return PendingCycleCoordinatorResult{}, errors.New("React Native pending-cycle coordinator has not completed")
	}
	return c.result, nil
}

func (c *PendingCycleCoordinator) Close(ctx context.Context) error {
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
	shutdownErr := c.server.Shutdown(ctx)
	listenErr := c.listener.Close()
	if shutdownErr != nil {
		return shutdownErr
	}
	if listenErr != nil && !errors.Is(listenErr, net.ErrClosed) {
		return listenErr
	}
	return nil
}

func (c *PendingCycleCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if request.Header.Get("Content-Type") != "application/json" {
		writeExchangeError(writer, http.StatusUnsupportedMediaType)
		return
	}
	if request.ContentLength > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusRequestEntityTooLarge)
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
	if c.closed || !c.prepared || c.failed != nil || c.completed {
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if exchange.Sequence != c.nextSeq {
		c.failed = errors.New("React Native pending-cycle exchange sequence is not monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native pending-cycle exchange sequence %d failed: %w", exchange.Sequence, err)
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
		c.failed = errors.New("React Native pending-cycle exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *PendingCycleCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == pendingCycleStageOpen {
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
	case pendingCycleStageInstallCurrent:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
		return nil
	case pendingCycleStageInstallStop:
		return c.validateSynchronizedResult(envelope.Result, "idle")
	case pendingCycleStageLocalWrite:
		if c.process == nil {
			return errors.New("React Native pending-cycle process identity is unavailable")
		}
		return validateStoppedLifecycleResult(envelope.Result, *c.process)
	case pendingCycleStagePush:
		return c.validateLocalResult(envelope.Result)
	case pendingCycleStagePull:
		return c.validateSynchronizedResult(envelope.Result, c.steps[pendingCycleStepOrder[1]].NativeBinding.Completion)
	case pendingCycleStageFinalCapture:
		return c.validateSynchronizedResult(envelope.Result, c.steps[pendingCycleStepOrder[3]].NativeBinding.Completion)
	case pendingCycleStageApplicationRows:
		capture, err := decodeCapture(envelope.Result, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace"})
		if err != nil {
			return err
		}
		if err := validatePendingCycleCapture(c.config.Scenario, capture); err != nil {
			return err
		}
		c.finalResult = &capture
		return nil
	case pendingCycleStageComplete:
		if c.finalResult == nil {
			return errors.New("React Native pending-cycle final capture is unavailable")
		}
		rows, err := captureRows(envelope.Result)
		if err != nil {
			return err
		}
		c.finalResult.Rows = rows
		return nil
	default:
		return errInvalidExchange
	}
}

func (c *PendingCycleCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case pendingCycleStageOpen:
		response.Command = c.command("client", "open", map[string]any{"client_key": c.clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil}, nil)
	case pendingCycleStageInstallCurrent:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
	case pendingCycleStageInstallStop:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
	case pendingCycleStageLocalWrite:
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{pendingCycleStepOrder[0]})
	case pendingCycleStagePush:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": c.steps[pendingCycleStepOrder[1]].NativeBinding.Method, "completion": c.steps[pendingCycleStepOrder[1]].NativeBinding.Completion}, []scenarios.StepID{pendingCycleStepOrder[1]})
	case pendingCycleStagePull:
		if err := c.preparePull(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": c.steps[pendingCycleStepOrder[3]].NativeBinding.Method, "completion": c.steps[pendingCycleStepOrder[3]].NativeBinding.Completion}, []scenarios.StepID{pendingCycleStepOrder[3]})
	case pendingCycleStageFinalCapture:
		response.Command = c.command("observer", "capture", map[string]any{"client_keys": []string{c.clientKey}, "sources": []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace"}}, nil)
	case pendingCycleStageApplicationRows:
		response.Command = c.command("observer", "capture", map[string]any{"client_keys": []string{c.clientKey}, "sources": []string{"application-rows"}, "row_selectors": []map[string]any{{"table_name": c.tableName, "primary_key_field": c.primaryKey, "primary_key": c.runtimeRecordID()}}}, nil)
	case pendingCycleStageComplete:
		if err := c.validateCompletionLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	}
	if c.stage != pendingCycleStageComplete {
		c.stage++
	}
	return response, nil
}

func (c *PendingCycleCoordinator) preparePull(ctx context.Context) error {
	if c.config.Controller == nil {
		return errors.New("React Native pending-cycle coordinator controller is unavailable")
	}
	push := c.steps[pendingCycleStepOrder[1]].Operation
	if err := c.config.Controller.BindApplicationPush(push); err != nil {
		return fmt.Errorf("bind React Native pending-cycle push transaction: %w", err)
	}
	materialize := c.steps[pendingCycleStepOrder[2]].Operation
	result, err := c.config.Controller.ProcessStep(ctx, nil, materialize)
	if err != nil || result.Disposition != c.steps[pendingCycleStepOrder[2]].ExpectedOutcome.Disposition {
		return fmt.Errorf("materialize React Native pending mutation: %w", nativeResultError(err, result.Disposition))
	}
	return c.bindRuntimeIdentities(true)
}

func (c *PendingCycleCoordinator) validateLocalResult(raw json.RawMessage) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "pending-cycle local result"); err != nil {
		return err
	}
	var rows uint64
	if json.Unmarshal(members["rows_affected"], &rows) != nil || rows == 0 {
		return errors.New("React Native pending-cycle local write affected no rows")
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateSynchronizedResult(raw json.RawMessage, completion string) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "pending-cycle synchronized result"); err != nil {
		return err
	}
	var actualCompletion string
	if json.Unmarshal(members["completion"], &actualCompletion) != nil || actualCompletion != completion || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native pending-cycle synchronized result is invalid")
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateProcess(raw json.RawMessage) error {
	process, err := decodeActionProcessIdentity(raw)
	if err != nil || c.process == nil {
		return errors.New("React Native pending-cycle process identity is invalid")
	}
	if process.ProcessID != c.process.ProcessID || process.DatabaseIdentityFingerprint != c.process.DatabaseIdentityFingerprint {
		return errors.New("React Native pending-cycle process identity changed")
	}
	return nil
}

func (c *PendingCycleCoordinator) validateCompletionLocked(ctx context.Context) error {
	if c.config.Controller == nil || c.finalResult == nil {
		return errors.New("React Native pending-cycle final evidence is unavailable")
	}
	rows, err := decodeRows(c.finalResult.Rows)
	if err != nil {
		return err
	}
	localWrites := 0
	for _, step := range c.config.Scenario.Steps {
		if scenarios.OperationKey(step.Operation) == "local/write" {
			localWrites++
		}
	}
	pullScopes, err := pendingCyclePullScopeCount(c.steps[pendingCycleStepOrder[3]].Operation)
	if err != nil || len(rows) != localWrites {
		return errors.New("React Native pending-cycle application row cardinality is invalid")
	}
	state, err := decodeClientState(c.finalResult.ClientState)
	if err != nil || len(state.ScopeStates) != pullScopes || state.ApplicationRowCount != uint64(len(rows)) || state.ScopeRowCount != uint64(len(state.ScopeRows)) || state.ScopeStateCount != uint64(len(state.ScopeStates)) {
		return errors.New("React Native pending-cycle client state is inconsistent")
	}
	if len(c.finalResult.Pending) == 0 || validateEmptyArray(c.finalResult.Pending) != nil || len(c.finalResult.Rejected) == 0 || validateEmptyArray(c.finalResult.Rejected) != nil {
		return errors.New("React Native pending-cycle mutation queues are not empty")
	}
	if err := validateReadyStatus(c.finalResult.Status); err != nil {
		return err
	}
	if len(rows) == 0 || len(state.ScopeRows) != len(rows) || state.ScopeRows[0].RecordID != c.runtimeRecordID() || !rowUsesRuntimePrimary(rows[0], c.primaryKey, c.runtimeRecordID()) {
		return errors.New("React Native pending-cycle row identity is invalid")
	}
	if err := validateProvenance(c.finalResult.Provenance, state.ScopeStates[0], state.ScopeRows[0]); err != nil {
		return err
	}
	if err := validatePendingCycleTrace(c.config.Scenario, c.finalResult.Trace); err != nil {
		return err
	}
	serverCaptures, err := c.config.Controller.Capture(ctx, []string{c.clientKey}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return fmt.Errorf("capture React Native pending-cycle server state: %w", nativeResultError(err, ""))
	}
	resolutions, err := c.resolveIdentities()
	if err != nil {
		return err
	}
	c.result = PendingCycleCoordinatorResult{ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *PendingCycleCoordinator) bindRuntimeIdentities(includePrimary bool) error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" || includePrimary {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native pending-cycle runtime identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		switch value.Alias {
		case "items-table":
			c.tableName = value.ApplicationIdentifier
		case "pending-row-primary-key":
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" || includePrimary && c.primaryKey == "" {
		return errors.New("React Native pending-cycle runtime application identities are unavailable")
	}
	return nil
}

func (c *PendingCycleCoordinator) runtimeRecordID() string {
	var value string
	_ = json.Unmarshal(c.runtimeIDs["pending-row-primary-key"], &value)
	return value
}

func (c *PendingCycleCoordinator) resolveIdentities() ([]blackbox.NativeIdentityResolution, error) {
	// The controller binds only server-owned identities. The client generation
	// and scope set version are observed on the wire, exactly as the steady-pull
	// consumer resolves them.
	trace, err := captureTraceFromRaw(c.finalResult.Trace)
	if err != nil {
		return nil, err
	}
	push, pull, err := pendingCycleAuthoredObservations(trace)
	if err != nil {
		return nil, err
	}
	generation, err := requestInteger(push, "client_generation")
	if err != nil {
		return nil, err
	}
	scopeSetVersion, err := requestInteger(pull, "scope_set_version")
	if err != nil {
		return nil, err
	}
	for alias, value := range map[string]any{
		"client-generation-one": generation,
		"scope-set-version-one": scopeSetVersion,
	} {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return nil, fmt.Errorf("encode React Native pending-cycle alias %q: %w", alias, marshalErr)
		}
		c.runtimeIDs[alias] = encoded
	}
	if len(c.runtimeIDs) != len(pendingCycleAliasNames) {
		missing := make([]string, 0, len(pendingCycleAliasNames))
		for _, name := range pendingCycleAliasNames {
			if _, found := c.runtimeIDs[name]; !found {
				missing = append(missing, name)
			}
		}
		return nil, fmt.Errorf("React Native pending-cycle identity evidence is incomplete: missing %v", missing)
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		for _, stepID := range alias.StepIDs {
			owner := stepID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &owner, RuntimeValue: value})
		}
		for _, expectationID := range alias.ExpectationIDs {
			owner := expectationID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &owner, RuntimeValue: value})
		}
	}
	return blackbox.ResolveNativeIdentityAliases(c.identities, observations)
}

func (c *PendingCycleCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		step := c.steps[id]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: step.Operation.ContractOperation, Name: step.Operation.Name, Payload: copyRaw(step.Operation.Payload)}})
	}
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: conformanceRuntime{ClientKey: c.clientKey, Database: c.database, ClientID: c.clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken}}
}

type pendingCycleClientIdentity struct{ userID, clientID string }

func extractPendingCycleClientIdentity(scenario scenarios.Scenario) (pendingCycleClientIdentity, error) {
	var payload struct {
		AuthenticatedUserID string `json:"authenticated_user_id"`
		ClientID            string `json:"client_id"`
	}
	if err := json.Unmarshal(scenario.Steps[0].Operation.Payload, &payload); err != nil || payload.AuthenticatedUserID == "" || payload.ClientID == "" {
		return pendingCycleClientIdentity{}, errors.New("React Native pending-cycle client identity is invalid")
	}
	for _, step := range scenario.Steps {
		if step.NativeBinding.UserID != "" && step.NativeBinding.UserID != payload.AuthenticatedUserID || step.NativeBinding.ClientID != "" && step.NativeBinding.ClientID != payload.ClientID {
			return pendingCycleClientIdentity{}, errors.New("React Native pending-cycle native identity differs across steps")
		}
	}
	return pendingCycleClientIdentity{userID: payload.AuthenticatedUserID, clientID: payload.ClientID}, nil
}

func pendingCyclePullScopeCount(operation scenarios.Operation) (int, error) {
	var payload struct {
		Scopes []json.RawMessage `json:"scopes"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Scopes) == 0 {
		return 0, errors.New("React Native pending-cycle pull scopes are invalid")
	}
	return len(payload.Scopes), nil
}

func validatePendingCycleCapture(scenario scenarios.Scenario, capture finalCapture) error {
	if len(capture.ClientState) == 0 || len(capture.Pending) == 0 || len(capture.Rejected) == 0 || len(capture.Status) == 0 || len(capture.Provenance) == 0 || len(capture.Trace) == 0 {
		return errors.New("React Native pending-cycle capture is incomplete")
	}
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" {
			return errors.New("React Native pending-cycle authored outcome is not successful")
		}
	}
	return nil
}

// pendingCycleAuthoredObservations returns the authored push and the pull that
// completes after it. The authored push uses the start method, and starting the
// client performs a whole connect, push, and pull cycle, so the authored steps
// name protocol operations rather than individual requests. A pull between an
// accepted push and its materialization returns 503 capture_pending and the
// client retries it. See docs/src/content/docs/spec/02-client-contract.mdx
// lines 536 to 539.
func pendingCycleAuthoredObservations(trace traceSnapshot) (transportObservation, transportObservation, error) {
	var push, pull transportObservation
	if trace.Overflowed || len(trace.Observations) <= pendingCycleBootstrapRequests ||
		trace.SequenceCheckpoint != uint64(len(trace.Observations)) ||
		validateTraceSequence(trace.Observations) != nil {
		classes := make([]string, 0, len(trace.Observations))
		for _, observation := range trace.Observations {
			classes = append(classes, observation.OperationClass)
		}
		return push, pull, fmt.Errorf("React Native pending-cycle request trace is incomplete: observations %v checkpoint %d overflowed %t", classes, trace.SequenceCheckpoint, trace.Overflowed)
	}
	// The client installs the current schema before the authored steps run, so
	// the trace opens with that bootstrap. validateBootstrapTrace documents the
	// same connect, rebuild, and pull shape.
	for index, operation := range []string{"connect", "rebuild", "pull"} {
		if err := validateTraceOperation(trace.Observations[index], operation); err != nil {
			return push, pull, fmt.Errorf("React Native pending-cycle bootstrap %s trace is invalid: %w", operation, err)
		}
	}
	authored := trace.Observations[pendingCycleBootstrapRequests:]
	index := -1
	for position, observation := range authored {
		if observation.DurationNanoseconds == 0 || !hasJSONValue(observation.RequestFacts) {
			return push, pull, fmt.Errorf("React Native pending-cycle %s request facts are absent", observation.OperationClass)
		}
		if observation.OperationClass != "push" {
			continue
		}
		if index >= 0 {
			return push, pull, errors.New("React Native pending-cycle sent more than one push")
		}
		index = position
	}
	if index < 0 {
		return push, pull, errors.New("React Native pending-cycle push is absent from the request trace")
	}
	// Nothing may fail before the push is accepted.
	for _, observation := range authored[:index] {
		if observation.StatusCode != 200 {
			return push, pull, fmt.Errorf("React Native pending-cycle %s request failed before its push", observation.OperationClass)
		}
	}
	for _, observation := range authored[index+1:] {
		if observation.StatusCode == pendingCycleCapturePendingStatus || observation.OperationClass != "pull" {
			continue
		}
		return authored[index], observation, nil
	}
	return push, pull, errors.New("React Native pending-cycle pull is absent after its push")
}

func validatePendingCycleTrace(scenario scenarios.Scenario, raw json.RawMessage) error {
	trace, err := captureTraceFromRaw(raw)
	if err != nil {
		return err
	}
	push, pull, err := pendingCycleAuthoredObservations(trace)
	if err != nil {
		return err
	}
	if push.StatusCode != pendingCycleWireStatus(scenario, pendingCycleStepOrder[1]) ||
		push.CursorFingerprints != nil || push.CursorFingerprintsComplete != nil ||
		hasJSONValue(push.RebuildResponseFacts) || hasJSONValue(push.PullResponseFacts) {
		return errors.New("React Native pending-cycle push trace is invalid")
	}
	if validateTraceOperation(pull, "pull") != nil ||
		pull.StatusCode != pendingCycleWireStatus(scenario, pendingCycleStepOrder[3]) {
		return fmt.Errorf("React Native pending-cycle pull trace is invalid: status %d", pull.StatusCode)
	}
	return nil
}

func pendingCycleWireStatus(scenario scenarios.Scenario, stepID scenarios.StepID) int {
	for _, expectation := range scenario.WireExpectations {
		if expectation.StepID == stepID {
			return expectation.HTTPStatus
		}
	}
	return 0
}
