package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	rebuildRequestsScenarioPath = "conformance/scenarios/performance/rebuild-requests-001.json"
	rebuildRequestsScenarioID   = "SCN-PERF-REBUILD-REQUESTS-001"
)

var rebuildRequestsStepOrder = []scenarios.StepID{
	"STEP-PERF-REBUILD-REQUESTS-INITIAL-COMMIT-001",
	"STEP-PERF-REBUILD-REQUESTS-INITIAL-MATERIALIZE-001",
	"STEP-PERF-REBUILD-REQUESTS-ASSIGN-001",
	"STEP-PERF-REBUILD-REQUESTS-001",
	"STEP-PERF-REBUILD-REQUESTS-BEGIN-001",
	"STEP-PERF-REBUILD-REQUESTS-003",
	"STEP-PERF-REBUILD-REQUESTS-CONCURRENT-COMMIT-001",
	"STEP-PERF-REBUILD-REQUESTS-CONCURRENT-MATERIALIZE-001",
	"STEP-PERF-REBUILD-REQUESTS-APPLY-001",
	"STEP-PERF-REBUILD-REQUESTS-004",
	"STEP-PERF-REBUILD-REQUESTS-APPLY-002",
	"STEP-PERF-REBUILD-REQUESTS-FINALIZE-001",
	"STEP-PERF-REBUILD-REQUESTS-002",
}

var rebuildRequestsAliasNames = []string{
	"current-schema",
	"client-generation-one",
	"scope-a",
	"rebuild-cycle",
	"scope-set-version-one",
	"items-table",
	"row-a-primary-key",
	"row-b-primary-key",
	"row-c-primary-key",
}

// RebuildRequestsCoordinatorConfig configures one authenticated React Native sidecar.
type RebuildRequestsCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
}

// RebuildRequestsCoordinatorResult contains validated server and native identity evidence.
type RebuildRequestsCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type rebuildRequestsIdentityEvidence struct {
	runtime      map[string]json.RawMessage
	resolutions  []blackbox.NativeIdentityResolution
	tableName    string
	primaryField string
}

type rebuildRequestsStage uint8

const (
	rebuildRequestsStageOpen rebuildRequestsStage = iota
	rebuildRequestsStageBegin
	rebuildRequestsStageFirstPage
	rebuildRequestsStageFinalPage
	rebuildRequestsStagePull
	rebuildRequestsStageAwaitCall
	rebuildRequestsStageFinalCapture
	rebuildRequestsStageApplicationRows
	rebuildRequestsStageComplete
)

// RebuildRequestsCoordinator drives the authored first-sync rebuild flow through React Native.
type RebuildRequestsCoordinator struct {
	config RebuildRequestsCoordinatorConfig

	listener  net.Listener
	server    *http.Server
	token     string
	adapter   string
	upstream  string
	database  string
	transport *http.Client

	steps      map[scenarios.StepID]scenarios.Step
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	tableName  string
	primaryKey string
	callID     string

	mu            sync.Mutex
	prepared      bool
	closed        bool
	completed     bool
	failed        error
	stage         rebuildRequestsStage
	nextSeq       uint64
	process       *actionProcessIdentity
	sourceApplied bool
	finalResult   *finalCapture
	result        RebuildRequestsCoordinatorResult
}

// LoadRebuildRequestsScenario loads the authored rebuild-requests contract.
func LoadRebuildRequestsScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, rebuildRequestsScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native rebuild-requests scenario: %w", err)
	}
	if err := ValidateRebuildRequestsScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateRebuildRequestsScenario rejects changes to the closed RN rebuild-requests contract.
func ValidateRebuildRequestsScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != rebuildRequestsScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native rebuild-requests scenario contract is invalid")
	}
	if len(scenario.Steps) != len(rebuildRequestsStepOrder) || len(scenario.NativeLifecycleBoundaries) != 0 ||
		len(scenario.NativeIdentityAliases) != len(rebuildRequestsAliasNames) {
		return errors.New("React Native rebuild-requests scenario structure is invalid")
	}
	for index, step := range scenario.Steps {
		if step.ID != rebuildRequestsStepOrder[index] || step.NativeBinding == nil || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native rebuild-requests step %s binding is invalid", step.ID)
		}
	}

	expected := []struct {
		id         scenarios.StepID
		operation  string
		kind       string
		stage      string
		method     string
		completion string
	}{
		{rebuildRequestsStepOrder[0], "model/commit-source-transaction", "controller", "", "", ""},
		{rebuildRequestsStepOrder[1], "process/materialize-source-transaction", "controller", "", "", ""},
		{rebuildRequestsStepOrder[2], "model/set-client-assignments", "controller", "", "", ""},
		{rebuildRequestsStepOrder[3], "connect/send", "public-call", "begin", "start", ""},
		{rebuildRequestsStepOrder[4], "local/begin-rebuild", "public-call", "await-step", "", ""},
		{rebuildRequestsStepOrder[5], "rebuild/request-page", "public-call", "await-step", "", ""},
		{rebuildRequestsStepOrder[6], "model/commit-source-transaction", "controller", "", "", ""},
		{rebuildRequestsStepOrder[7], "process/materialize-source-transaction", "controller", "", "", ""},
		{rebuildRequestsStepOrder[8], "local/apply-rebuild-page", "public-call", "await-step", "", ""},
		{rebuildRequestsStepOrder[9], "rebuild/request-page", "public-call", "await-step", "", ""},
		{rebuildRequestsStepOrder[10], "local/apply-rebuild-page", "public-call", "await-step", "", ""},
		{rebuildRequestsStepOrder[11], "local/finalize-rebuild", "public-call", "await-step", "", ""},
		{rebuildRequestsStepOrder[12], "pull/request-page", "public-call", "await-call", "", "idle"},
	}
	var callID string
	for index, wanted := range expected {
		step := scenario.Steps[index]
		if scenarios.OperationKey(step.Operation) != wanted.operation {
			return fmt.Errorf("React Native rebuild-requests step %s operation is %s, want %s", step.ID, scenarios.OperationKey(step.Operation), wanted.operation)
		}
		binding := step.NativeBinding
		if binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || binding.Completion != wanted.completion {
			return fmt.Errorf("React Native rebuild-requests step %s native binding is invalid", step.ID)
		}
		if wanted.kind != "public-call" {
			continue
		}
		if binding.UserID != userID || binding.ClientID != clientID || binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("React Native rebuild-requests step %s client binding is invalid", step.ID)
		}
		if callID == "" {
			callID = string(*binding.CallID)
		} else if callID != string(*binding.CallID) {
			return errors.New("React Native rebuild-requests steps do not share one call identity")
		}
	}

	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native rebuild-requests identity alias is invalid")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return errors.New("React Native rebuild-requests identity alias is duplicated")
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range rebuildRequestsAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native rebuild-requests identity alias %q is absent", name)
		}
	}

	semantic, performance := false, false
	for _, assertion := range scenario.Assertions {
		switch string(assertion.ID) {
		case "ASSERT-PERF-REBUILD-REQUESTS-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-REBUILD-REQUESTS-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !performance {
		return errors.New("React Native rebuild-requests assertions are invalid")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		switch string(obligation.ObligationID) {
		case "OBL-PERF-REBUILD-REQUESTS-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[string(obligation.ObligationID)]++
			}
		case "OBL-PERF-REBUILD-REQUESTS-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[string(obligation.ObligationID)]++
			}
		case "OBL-PERF-REBUILD-REQUESTS-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-REBUILD-REQUESTS-001", "CTRL-REBUILD-003") {
				obligations[string(obligation.ObligationID)]++
			}
		}
	}
	if obligations["OBL-PERF-REBUILD-REQUESTS-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-REBUILD-REQUESTS-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-REBUILD-REQUESTS-CONTROL-001"] != 1 {
		return errors.New("React Native rebuild-requests proof obligations are invalid")
	}
	return nil
}

// NewRebuildRequestsCoordinator creates an authenticated loopback sidecar and adapter proxy.
func NewRebuildRequestsCoordinator(config RebuildRequestsCoordinatorConfig) (*RebuildRequestsCoordinator, error) {
	if err := ValidateRebuildRequestsScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native rebuild-requests coordinator platform must be ios or android")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native rebuild-requests coordinator auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	upstream, err := nativeAdapterURL(serverURL, "ios")
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native rebuild-requests coordinator capability")
	}
	database, err := randomDatabaseNameWithPrefix("rn-rebuild-requests-")
	if err != nil {
		return nil, errors.New("create React Native rebuild-requests private database name")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native rebuild-requests coordinator")
	}
	adapter, err := nativeAdapterURL("http://"+listener.Addr().String(), config.Platform)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	coordinator := &RebuildRequestsCoordinator{
		config: config, listener: listener, token: token, adapter: adapter, upstream: upstream,
		database: database, transport: &http.Client{}, steps: steps,
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), nextSeq: 1,
		callID: string(*scenarioCallID(config.Scenario)),
	}
	coordinator.server = &http.Server{
		Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second,
	}
	return coordinator, nil
}

func scenarioCallID(scenario scenarios.Scenario) *scenarios.NativeCallID {
	for _, step := range scenario.Steps {
		if step.NativeBinding != nil && step.NativeBinding.CallID != nil {
			value := *step.NativeBinding.CallID
			return &value
		}
	}
	return nil
}

// Prepare installs the authored model, applies setup operations, and binds server identities.
func (c *RebuildRequestsCoordinator) Prepare(ctx context.Context) error {
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
		return errors.New("React Native rebuild-requests coordinator dependencies are unavailable")
	}
	if c.config.AuthToken == "" {
		token, err := c.config.Harness.NativeBearerToken(ctx, userID, time.Now())
		if err != nil {
			return errors.New("mint React Native rebuild-requests adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native rebuild-requests contract: %w", err)
	}
	model, err := modelrunner.RunScenario(ctx, c.config.Scenario)
	if err != nil || !model.Passed || len(model.Steps) != len(c.steps) {
		return fmt.Errorf("derive React Native rebuild-requests source operations: %w", nativeResultError(err, "model did not pass"))
	}
	if err := c.applyControllerStep(ctx, rebuildRequestsStepOrder[0], "model/commit-source-transaction"); err != nil {
		return fmt.Errorf("commit React Native rebuild-requests snapshot: %w", err)
	}
	if err := c.processControllerStep(ctx, rebuildRequestsStepOrder[1], "process/materialize-source-transaction"); err != nil {
		return fmt.Errorf("materialize React Native rebuild-requests snapshot: %w", err)
	}
	if err := c.applyControllerStep(ctx, rebuildRequestsStepOrder[2], "model/set-client-assignments"); err != nil {
		return fmt.Errorf("assign React Native rebuild-requests scope: %w", err)
	}
	if err := c.bindServerIdentities(false); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

func (c *RebuildRequestsCoordinator) applyControllerStep(ctx context.Context, id scenarios.StepID, wanted string) error {
	step, found := c.steps[id]
	if !found || scenarios.OperationKey(step.Operation) != wanted {
		return fmt.Errorf("React Native rebuild-requests step %s operation is invalid", id)
	}
	result, err := c.config.Controller.ApplyStep(ctx, step.Operation)
	if err != nil || result.Disposition != step.ExpectedOutcome.Disposition {
		return nativeResultError(err, result.Disposition)
	}
	return nil
}

func (c *RebuildRequestsCoordinator) processControllerStep(ctx context.Context, id scenarios.StepID, wanted string) error {
	step, found := c.steps[id]
	if !found || scenarios.OperationKey(step.Operation) != wanted {
		return fmt.Errorf("React Native rebuild-requests step %s operation is invalid", id)
	}
	result, err := c.config.Controller.ProcessStep(ctx, nil, step.Operation)
	if err != nil || result.Disposition != step.ExpectedOutcome.Disposition {
		return nativeResultError(err, result.Disposition)
	}
	return nil
}

// Serve runs the sidecar until it closes.
func (c *RebuildRequestsCoordinator) Serve(ctx context.Context) error {
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

func (c *RebuildRequestsCoordinator) Handler() http.Handler { return c }

func (c *RebuildRequestsCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *RebuildRequestsCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

func (c *RebuildRequestsCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *RebuildRequestsCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	return int(rebuildRequestsStageComplete) + 1
}

func (c *RebuildRequestsCoordinator) Result() (RebuildRequestsCoordinatorResult, error) {
	if c == nil {
		return RebuildRequestsCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return RebuildRequestsCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return RebuildRequestsCoordinatorResult{}, errors.New("React Native rebuild-requests coordinator has not completed")
	}
	return c.result, nil
}

func (c *RebuildRequestsCoordinator) Close(ctx context.Context) error {
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

func (c *RebuildRequestsCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/exchange" {
		c.proxyAdapter(writer, request)
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
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
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
	closed, prepared, failed, completed := c.closed, c.prepared, c.failed != nil, c.completed
	if closed || !prepared || failed || completed || exchange.Sequence != c.nextSeq {
		// A latched failure is the root cause. Overwriting it with this
		// rejection hides the defect the device run must name.
		if !failed {
			c.failed = fmt.Errorf("React Native rebuild-requests exchange is unavailable or non-monotonic: closed=%t prepared=%t completed=%t got sequence=%d want sequence=%d", closed, prepared, completed, exchange.Sequence, c.nextSeq)
		}
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native rebuild-requests exchange %d failed: %w", exchange.Sequence, err)
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
		c.failed = errors.New("React Native rebuild-requests exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *RebuildRequestsCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == rebuildRequestsStageOpen {
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
	case rebuildRequestsStageBegin:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
	case rebuildRequestsStageFirstPage:
		return c.validateCallBegun(envelope.Result)
	case rebuildRequestsStageFinalPage, rebuildRequestsStagePull, rebuildRequestsStageAwaitCall:
		return c.validateAwaited(envelope.Result)
	case rebuildRequestsStageFinalCapture:
		return c.validateCallCompleted(envelope.Result)
	case rebuildRequestsStageApplicationRows:
		capture, err := c.decodeCaptureResult(envelope.Result, []string{
			"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof",
		})
		if err != nil {
			return err
		}
		c.finalResult = &capture
	case rebuildRequestsStageComplete:
		capture, err := c.decodeCaptureResult(envelope.Result, []string{"application_rows"})
		if err != nil {
			return err
		}
		if c.finalResult == nil {
			return errors.New("React Native rebuild-requests final capture is unavailable")
		}
		c.finalResult.Rows = capture.Rows
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *RebuildRequestsCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case rebuildRequestsStageOpen:
		response.Command = c.command("client", "open", map[string]any{
			"client_key": clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil,
		}, nil)
		c.stage = rebuildRequestsStageBegin
	case rebuildRequestsStageBegin:
		response.Command = c.command("client", "begin-call", map[string]any{
			"client_key": clientKey, "call_id": c.callID, "method": "start",
		}, []scenarios.StepID{rebuildRequestsStepOrder[3]})
		c.stage = rebuildRequestsStageFirstPage
	case rebuildRequestsStageFirstPage:
		response.Command = c.command("observer", "await-step", map[string]any{
			"client_key": clientKey, "call_id": c.callID,
		}, []scenarios.StepID{rebuildRequestsStepOrder[5]})
		c.stage = rebuildRequestsStageFinalPage
	case rebuildRequestsStageFinalPage:
		response.Command = c.command("observer", "await-step", map[string]any{
			"client_key": clientKey, "call_id": c.callID,
		}, []scenarios.StepID{rebuildRequestsStepOrder[9]})
		c.stage = rebuildRequestsStagePull
	case rebuildRequestsStagePull:
		response.Command = c.command("observer", "await-step", map[string]any{
			"client_key": clientKey, "call_id": c.callID,
		}, []scenarios.StepID{rebuildRequestsStepOrder[12]})
		c.stage = rebuildRequestsStageAwaitCall
	case rebuildRequestsStageAwaitCall:
		response.Command = c.command("client", "await-call", map[string]any{
			"client_key": clientKey, "call_id": c.callID,
		}, nil)
		c.stage = rebuildRequestsStageFinalCapture
	case rebuildRequestsStageFinalCapture:
		recordID, err := c.runtimeRecordID("row-c-primary-key")
		if err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys":            []string{clientKey},
			"sources":                []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"},
			"durable_proof_identity": map[string]any{"table_name": c.tableName, "record_id": recordID},
		}, nil)
		c.stage = rebuildRequestsStageApplicationRows
	case rebuildRequestsStageApplicationRows:
		if c.finalResult == nil {
			return exchangeResponse{}, errors.New("React Native rebuild-requests final capture is unavailable")
		}
		selectors, err := c.applicationSelectors()
		if err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys":   []string{clientKey},
			"sources":       []string{"application-rows"},
			"row_selectors": selectors,
		}, nil)
		c.stage = rebuildRequestsStageComplete
	case rebuildRequestsStageComplete:
		if err := c.validateCompletionLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	}
	return response, nil
}

func (c *RebuildRequestsCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		step, found := c.steps[id]
		if !found {
			continue
		}
		steps = append(steps, conformanceStep{Operation: conformanceOperation{
			ContractOperation: step.Operation.ContractOperation,
			Name:              step.Operation.Name,
			Payload:           copyRaw(step.Operation.Payload),
		}})
	}
	return &conformanceCommand{
		SchemaVersion: 1,
		Action:        conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps},
		Runtime:       conformanceRuntime{ClientKey: clientKey, Database: c.database, ClientID: clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken},
	}
}

func (c *RebuildRequestsCoordinator) decodeCaptureResult(raw json.RawMessage, keys []string) (finalCapture, error) {
	capture, err := decodeCapture(raw, keys)
	if err != nil {
		return finalCapture{}, err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "React Native rebuild-requests capture result"); err != nil {
		return finalCapture{}, err
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return finalCapture{}, errors.New("React Native rebuild-requests capture process identity changed")
	}
	return capture, nil
}

func (c *RebuildRequestsCoordinator) validateCallBegun(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-begun"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "React Native rebuild-requests begun call result"); err != nil {
		return fmt.Errorf("React Native rebuild-requests begun call result members=%d want=4: %w", len(members), err)
	}
	var actualID, state string
	if json.Unmarshal(members["call_id"], &actualID) != nil || actualID != c.callID ||
		json.Unmarshal(members["state"], &state) != nil || state != "in_flight" {
		return fmt.Errorf("React Native rebuild-requests begun call is invalid: call_id=%q state=%q, want %q in_flight", actualID, state, c.callID)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return fmt.Errorf("React Native rebuild-requests begun call process identity changed: got=%#v want=%#v decode_error=%v", process, c.process, err)
	}
	return nil
}

func (c *RebuildRequestsCoordinator) validateAwaited(raw json.RawMessage) error {
	if err := validateActionResult(raw, "awaited"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "React Native rebuild-requests awaited result"); err != nil {
		return err
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return err
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return errors.New("React Native rebuild-requests awaited process identity changed")
	}
	return nil
}

func (c *RebuildRequestsCoordinator) validateCallCompleted(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-completed"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 6, "React Native rebuild-requests completed call result"); err != nil {
		return err
	}
	var actualID, state, completion string
	if json.Unmarshal(members["call_id"], &actualID) != nil || actualID != c.callID ||
		json.Unmarshal(members["state"], &state) != nil || state != "completed" ||
		json.Unmarshal(members["completion"], &completion) != nil || completion != "idle" {
		return errors.New("React Native rebuild-requests call completion is invalid")
	}
	if err := validateReadyStatus(members["status"]); err != nil {
		return err
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return errors.New("React Native rebuild-requests call process identity changed")
	}
	return nil
}

func (c *RebuildRequestsCoordinator) bindServerIdentities(includePrimary bool) error {
	if c.config.Controller == nil {
		return errors.New("React Native rebuild-requests controller is unavailable")
	}
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" || includePrimary && alias.Kind == "primary-key" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native rebuild-requests server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		switch value.Alias {
		case "items-table":
			c.tableName = value.ApplicationIdentifier
		case "row-a-primary-key":
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" {
		return errors.New("React Native rebuild-requests table identity is unavailable")
	}
	if includePrimary && c.primaryKey == "" {
		return errors.New("React Native rebuild-requests primary-key identity is unavailable")
	}
	for _, alias := range aliases {
		if len(c.runtimeIDs[alias.Alias]) == 0 {
			return fmt.Errorf("React Native rebuild-requests server identity %q is unavailable", alias.Alias)
		}
	}
	return nil
}

func (c *RebuildRequestsCoordinator) runtimeRecordID(alias string) (string, error) {
	var value string
	if json.Unmarshal(c.runtimeIDs[alias], &value) != nil || value == "" {
		return "", fmt.Errorf("React Native rebuild-requests runtime record identity %q is invalid", alias)
	}
	return value, nil
}

func (c *RebuildRequestsCoordinator) applicationSelectors() ([]map[string]any, error) {
	selectors := make([]map[string]any, 0, 3)
	for _, alias := range []string{"row-a-primary-key", "row-b-primary-key", "row-c-primary-key"} {
		recordID, err := c.runtimeRecordID(alias)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, map[string]any{
			"table_name": c.tableName, "primary_key_field": c.primaryKey, "primary_key": recordID,
		})
	}
	return selectors, nil
}

func (c *RebuildRequestsCoordinator) proxyAdapter(writer http.ResponseWriter, request *http.Request) {
	if c == nil || c.transport == nil || c.upstream == "" {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	target := strings.TrimRight(c.upstream, "/") + request.URL.RequestURI()
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target, request.Body)
	if err != nil {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	for name, values := range request.Header {
		if strings.EqualFold(name, "Host") {
			continue
		}
		for _, value := range values {
			upstreamRequest.Header.Add(name, value)
		}
	}
	response, err := c.transport.Do(upstreamRequest)
	if err != nil {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	if request.Method == http.MethodPost && request.URL.Path == "/sync/rebuild" && response.StatusCode == http.StatusOK {
		if err := c.observeFirstRebuildResponse(request.Context(), body); err != nil {
			c.mu.Lock()
			if c.failed == nil {
				c.failed = err
			}
			c.mu.Unlock()
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
	}
	for name, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(body)
}

func (c *RebuildRequestsCoordinator) observeFirstRebuildResponse(ctx context.Context, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sourceApplied {
		return nil
	}
	if c.config.Controller == nil || !c.prepared || c.closed {
		return errors.New("React Native rebuild-requests source barrier is unavailable")
	}
	if err := validateFirstRebuildResponse(body); err != nil {
		return err
	}
	commit := c.steps[rebuildRequestsStepOrder[6]]
	if scenarios.OperationKey(commit.Operation) != "model/commit-source-transaction" {
		return errors.New("React Native rebuild-requests concurrent commit operation is invalid")
	}
	result, err := c.config.Controller.ApplyStep(ctx, commit.Operation)
	if err != nil || result.Disposition != commit.ExpectedOutcome.Disposition {
		return fmt.Errorf("apply React Native rebuild-requests concurrent source row: %w", nativeResultError(err, result.Disposition))
	}
	materialize := c.steps[rebuildRequestsStepOrder[7]]
	if scenarios.OperationKey(materialize.Operation) != "process/materialize-source-transaction" {
		return errors.New("React Native rebuild-requests concurrent materialization operation is invalid")
	}
	result, err = c.config.Controller.ProcessStep(ctx, nil, materialize.Operation)
	if err != nil || result.Disposition != materialize.ExpectedOutcome.Disposition {
		return fmt.Errorf("materialize React Native rebuild-requests concurrent source row: %w", nativeResultError(err, result.Disposition))
	}
	if err := c.bindServerIdentities(true); err != nil {
		return err
	}
	c.sourceApplied = true
	return nil
}

func validateFirstRebuildResponse(raw []byte) error {
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) < 3 || len(members) > 6 {
		return errors.New("React Native rebuild-requests first rebuild response is invalid")
	}
	var records []json.RawMessage
	var hasMore bool
	if json.Unmarshal(members["records"], &records) != nil || len(records) != 1 ||
		json.Unmarshal(members["has_more"], &hasMore) != nil || !hasMore {
		return errors.New("React Native rebuild-requests first rebuild response is not an intermediate page")
	}
	var cursor string
	if rawCursor, found := members["cursor"]; !found || json.Unmarshal(rawCursor, &cursor) != nil || cursor == "" {
		return errors.New("React Native rebuild-requests first rebuild cursor is absent")
	}
	for _, key := range []string{"final_scope_cursor", "checksum"} {
		if rawValue, found := members[key]; found && !isJSONNull(rawValue) {
			return fmt.Errorf("React Native rebuild-requests first rebuild %s is non-null", key)
		}
	}
	return nil
}

func (c *RebuildRequestsCoordinator) validateCompletionLocked(ctx context.Context) error {
	if c.config.Controller == nil || c.finalResult == nil || !c.sourceApplied {
		return errors.New("React Native rebuild-requests final evidence is unavailable")
	}
	serverCaptures, err := c.config.Controller.Capture(ctx, []string{clientKey}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return fmt.Errorf("capture React Native rebuild-requests server state: %w", nativeResultError(err, ""))
	}
	server := serverCaptures[0].StateFacts
	trace, err := captureTraceFromRaw(c.finalResult.Trace)
	if err != nil {
		return err
	}
	if err := validateRebuildRequestsTransport(c.config.Scenario, trace); err != nil {
		return err
	}
	if len(trace.Observations) != 4 {
		return errors.New("React Native rebuild-requests transport evidence is incomplete")
	}
	if err := c.bindServerIdentities(true); err != nil {
		return err
	}
	evidence, err := c.resolveIdentities(server, trace)
	if err != nil {
		return err
	}
	if err := c.validateState(server, trace, evidence); err != nil {
		return err
	}
	c.result = RebuildRequestsCoordinatorResult{ServerFacts: server, IdentityResolution: evidence.resolutions}
	return nil
}

func (c *RebuildRequestsCoordinator) resolveIdentities(server scenarios.StateFacts, trace traceSnapshot) (rebuildRequestsIdentityEvidence, error) {
	if len(c.identities) != len(rebuildRequestsAliasNames) || len(server.Rebuilds) != 1 || len(trace.Observations) != 4 {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(rebuildRequestsAliasNames))
	for _, name := range rebuildRequestsAliasNames {
		wanted[name] = struct{}{}
	}
	for _, alias := range c.identities {
		if _, found := wanted[alias.Alias]; !found {
			return rebuildRequestsIdentityEvidence{}, fmt.Errorf("React Native rebuild-requests identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests identity alias set is incomplete")
	}
	runtime := make(map[string]json.RawMessage, len(c.runtimeIDs))
	for alias, value := range c.runtimeIDs {
		runtime[alias] = copyRaw(value)
	}
	firstRequest := trace.Observations[1]
	pullRequest := trace.Observations[3]
	generation, err := requestInteger(firstRequest, "client_generation")
	if err != nil || generation == 0 {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests client generation evidence is invalid")
	}
	scopeSetVersion, err := requestInteger(pullRequest, "scope_set_version")
	if err != nil || scopeSetVersion == 0 {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests scope-set version evidence is invalid")
	}
	rebuildID := server.Rebuilds[0].RebuildID
	if rebuildID == "" {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests server rebuild identity is invalid")
	}
	rebuildFingerprint, err := requestString(firstRequest, "rebuild_id_fingerprint")
	if err != nil || rebuildFingerprint != hashFingerprint(rebuildID) {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests server rebuild identity differs from the request")
	}
	for alias, value := range map[string]any{
		"client-generation-one": generation,
		"rebuild-cycle":         rebuildID,
		"scope-set-version-one": scopeSetVersion,
	} {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return rebuildRequestsIdentityEvidence{}, fmt.Errorf("encode React Native rebuild-requests alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	for _, alias := range rebuildRequestsAliasNames {
		if len(runtime[alias]) == 0 {
			return rebuildRequestsIdentityEvidence{}, fmt.Errorf("React Native rebuild-requests alias %q has no runtime evidence", alias)
		}
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		for _, stepID := range alias.StepIDs {
			owner := stepID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &owner, RuntimeValue: runtime[alias.Alias]})
		}
		for _, expectationID := range alias.ExpectationIDs {
			owner := expectationID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &owner, RuntimeValue: runtime[alias.Alias]})
		}
	}
	resolutions, err := blackbox.ResolveNativeIdentityAliases(c.identities, observations)
	if err != nil {
		return rebuildRequestsIdentityEvidence{}, err
	}
	if c.tableName == "" || c.primaryKey == "" {
		return rebuildRequestsIdentityEvidence{}, errors.New("React Native rebuild-requests application identity is incomplete")
	}
	return rebuildRequestsIdentityEvidence{runtime: runtime, resolutions: resolutions, tableName: c.tableName, primaryField: c.primaryKey}, nil
}

func validateRebuildRequestsTransport(scenario scenarios.Scenario, trace traceSnapshot) error {
	ids := []string{
		"STEP-PERF-REBUILD-REQUESTS-001",
		"STEP-PERF-REBUILD-REQUESTS-003",
		"STEP-PERF-REBUILD-REQUESTS-004",
		"STEP-PERF-REBUILD-REQUESTS-002",
	}
	if trace.Overflowed || len(trace.Observations) != len(ids) || trace.SequenceCheckpoint != uint64(len(ids)) || validateTraceSequence(trace.Observations) != nil {
		return errors.New("React Native rebuild-requests transport trace is incomplete")
	}
	for index, operation := range []string{"connect", "rebuild", "rebuild", "pull"} {
		if err := validateTraceOperation(trace.Observations[index], operation); err != nil {
			return fmt.Errorf("React Native rebuild-requests %s trace is invalid: %w", operation, err)
		}
		if err := validateRebuildRequestsWireObservation(scenario, ids[index], trace.Observations[index]); err != nil {
			return err
		}
	}
	first := trace.Observations[1]
	final := trace.Observations[2]
	pull := trace.Observations[3]
	firstGeneration, generationErr := requestInteger(first, "client_generation")
	finalGeneration, finalGenerationErr := requestInteger(final, "client_generation")
	if generationErr != nil || finalGenerationErr != nil || firstGeneration == 0 || firstGeneration != finalGeneration {
		return errors.New("React Native rebuild-requests page generations are inconsistent")
	}
	firstSchema, firstSchemaErr := requestInteger(first, "schema_version")
	finalSchema, finalSchemaErr := requestInteger(final, "schema_version")
	firstHash, firstHashErr := requestString(first, "schema_hash")
	finalHash, finalHashErr := requestString(final, "schema_hash")
	firstScope, firstScopeErr := requestString(first, "scope_fingerprint")
	finalScope, finalScopeErr := requestString(final, "scope_fingerprint")
	firstRebuild, firstRebuildErr := requestString(first, "rebuild_id_fingerprint")
	finalRebuild, finalRebuildErr := requestString(final, "rebuild_id_fingerprint")
	firstLimit, firstLimitErr := requestInteger(first, "limit")
	finalLimit, finalLimitErr := requestInteger(final, "limit")
	if firstSchemaErr != nil || finalSchemaErr != nil || firstHashErr != nil || finalHashErr != nil || firstScopeErr != nil || finalScopeErr != nil || firstRebuildErr != nil || finalRebuildErr != nil || firstLimitErr != nil || finalLimitErr != nil || firstSchema != finalSchema || firstHash != finalHash || firstScope != finalScope || firstRebuild != finalRebuild || firstLimit != 1 || finalLimit != 1 {
		return errors.New("React Native rebuild-requests page identities are inconsistent")
	}
	firstFacts, err := decodeRebuildResponseFacts(first.RebuildResponseFacts)
	if err != nil || firstFacts.RecordCount == nil || *firstFacts.RecordCount != 1 || firstFacts.HasMore == nil || !*firstFacts.HasMore || firstFacts.HasCursor == nil || !*firstFacts.HasCursor || firstFacts.HasFinalScopeCursor == nil || *firstFacts.HasFinalScopeCursor || firstFacts.HasChecksum == nil || *firstFacts.HasChecksum || firstFacts.FinalScopeCursorFingerprint != nil {
		return errors.New("React Native rebuild-requests first page is not an intermediate page")
	}
	finalFacts, err := decodeRebuildResponseFacts(final.RebuildResponseFacts)
	if err != nil || finalFacts.RecordCount == nil || *finalFacts.RecordCount != 1 || finalFacts.HasMore == nil || *finalFacts.HasMore || finalFacts.HasCursor == nil || *finalFacts.HasCursor || finalFacts.HasFinalScopeCursor == nil || !*finalFacts.HasFinalScopeCursor || finalFacts.HasChecksum == nil || !*finalFacts.HasChecksum || finalFacts.FinalScopeCursorFingerprint == nil {
		return errors.New("React Native rebuild-requests final page is not terminal")
	}
	pullGeneration, pullGenerationErr := requestInteger(pull, "client_generation")
	pullSchema, pullSchemaErr := requestInteger(pull, "schema_version")
	pullHash, pullHashErr := requestString(pull, "schema_hash")
	scopeSet, scopeSetErr := requestInteger(pull, "scope_set_version")
	scopeCount, scopeCountErr := requestInteger(pull, "scope_count")
	limit, limitErr := requestInteger(pull, "limit")
	pullFacts, pullErr := decodePullResponseFacts(pull.PullResponseFacts)
	if pullGenerationErr != nil || pullSchemaErr != nil || pullHashErr != nil || scopeSetErr != nil || scopeCountErr != nil || limitErr != nil || pullErr != nil || pullGeneration != firstGeneration || pullSchema != firstSchema || pullHash != firstHash || scopeSet == 0 || scopeCount != 1 || limit != 1 || pullFacts.ChangeCount == nil || *pullFacts.ChangeCount != 1 || pullFacts.HasMore == nil || *pullFacts.HasMore || pullFacts.RebuildScopeCount == nil || *pullFacts.RebuildScopeCount != 0 || pullFacts.ChecksumCount == nil || *pullFacts.ChecksumCount != 1 || pullFacts.ScopeCursorFingerprintsComplete == nil || !*pullFacts.ScopeCursorFingerprintsComplete || len(pullFacts.ScopeCursorFingerprints) != 1 || pull.CursorFingerprintsComplete == nil || !*pull.CursorFingerprintsComplete || len(pull.CursorFingerprints) != 1 || pull.CursorFingerprints[0] != *finalFacts.FinalScopeCursorFingerprint || pullFacts.ScopeCursorFingerprints[0] != pull.CursorFingerprints[0] {
		return errors.New("React Native rebuild-requests incremental pull is invalid")
	}
	return nil
}

func validateRebuildRequestsWireObservation(scenario scenarios.Scenario, stepID string, observed transportObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.StatusCode != expected.HTTPStatus || expected.HTTPStatus != http.StatusOK || observed.StatusCode != http.StatusOK || expected.Retryable || expected.ErrorCode != nil {
			return fmt.Errorf("React Native rebuild-requests wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("React Native rebuild-requests wire expectation %s is absent", stepID)
}

func (c *RebuildRequestsCoordinator) validateState(server scenarios.StateFacts, trace traceSnapshot, evidence rebuildRequestsIdentityEvidence) error {
	if server.TransactionCount == nil || *server.TransactionCount != 2 || server.RowCount == nil || *server.RowCount != 3 || server.ScopeCount == nil || *server.ScopeCount != 2 || server.RebuildCount == nil || *server.RebuildCount != 1 || len(server.Transactions) != 2 || len(server.Rows) != 3 || len(server.Scopes) != 2 || len(server.Rebuilds) != 1 {
		return errors.New("React Native rebuild-requests server state is incomplete")
	}
	cardinalities := make([]uint64, 0, len(server.Scopes))
	for _, scope := range server.Scopes {
		cardinalities = append(cardinalities, scope.Cardinality)
	}
	sort.Slice(cardinalities, func(left, right int) bool { return cardinalities[left] < cardinalities[right] })
	rebuild := server.Rebuilds[0]
	if !reflectDeepEqualUint64(cardinalities, []uint64{0, 3}) || rebuild.UserID != userID || rebuild.ClientID != clientID || rebuild.ScopeID != "scope-a" || rebuild.PageLimit != 1 || rebuild.StagedRowCount != 2 || rebuild.PageCount != 2 || !rebuild.HasContinuation || !rebuild.HasFinalCursor {
		return errors.New("React Native rebuild-requests server state differs from the authored flow")
	}
	state, err := decodeClientState(c.finalResult.ClientState)
	if err != nil {
		return err
	}
	if state.ApplicationRowCount != 3 || state.MutationLedgerCount != 0 || state.MutationOutcomeCount != 0 || state.SealedBatchCount != 0 || state.RejectedMutationCount != 0 || state.ScopeStateCount != 1 || state.ScopeRowCount != 3 || state.ProvenanceCount != 3 || state.RowMetadataCount != 3 || state.RebuildAttemptCount != 0 || state.RebuildReceiptCount != 1 || len(state.ScopeStates) != 1 || len(state.ScopeRows) != 3 {
		return errors.New("React Native rebuild-requests client durable counts are incomplete")
	}
	if err := validateEmptyArray(c.finalResult.Pending); err != nil || validateEmptyArray(c.finalResult.Rejected) != nil {
		return errors.New("React Native rebuild-requests mutation queues are not empty")
	}
	if err := validateReadyStatus(c.finalResult.Status); err != nil {
		return err
	}
	proof, err := decodeDurableProof(c.finalResult.DurableProof)
	if err != nil || proof.RowMetadata == nil || len(proof.RebuildReceiptProofs) != 1 {
		return errors.New("React Native rebuild-requests durable proof is incomplete")
	}
	receipt := proof.RebuildReceiptProofs[0]
	wantFinalChecksum := c.config.Platform == "ios"
	if receipt.PageCount != 2 || receipt.ReturnedRecordCount != 2 || !receipt.RequestChainValid || !receipt.RecordsInCanonicalOrder || !receipt.RowChecksumsValid || !receipt.ScopeChecksumValid || receipt.FinalChecksumMatches != wantFinalChecksum {
		return errors.New("React Native rebuild-requests receipt proof is invalid")
	}
	var runtimeSchema clientSchema
	if json.Unmarshal(evidence.runtime["current-schema"], &runtimeSchema) != nil || state.Schema == nil || *state.Schema != runtimeSchema {
		return errors.New("React Native rebuild-requests schema identity is inconsistent")
	}
	var runtimeScope string
	if json.Unmarshal(evidence.runtime["scope-a"], &runtimeScope) != nil || runtimeScope == "" || state.ScopeStates[0].ScopeID != runtimeScope {
		return errors.New("React Native rebuild-requests scope identity is inconsistent")
	}
	if state.ScopeStates[0].Cursor == nil || state.ScopeStates[0].Checksum == nil {
		return errors.New("React Native rebuild-requests checkpoint identity is incomplete")
	}
	storedChecksum, storedErr := checksumDigest(state.ScopeStates[0].Checksum)
	localChecksum, localErr := checksumDigest(&state.ScopeStates[0].LocalChecksum)
	if storedErr != nil || localErr != nil || storedChecksum == nil || localChecksum == nil || *storedChecksum != *localChecksum || trace.Observations[3].CursorFingerprints[0] != hashFingerprint(*state.ScopeStates[0].Cursor) {
		return errors.New("React Native rebuild-requests checkpoint is not verified")
	}
	if err := c.validateRows(state, proof, evidence); err != nil {
		return err
	}
	if err := c.validateApplicationRows(evidence); err != nil {
		return err
	}
	return nil
}

func (c *RebuildRequestsCoordinator) validateRows(state inspectedClientState, proof durableProof, evidence rebuildRequestsIdentityEvidence) error {
	runtimeRecords := make(map[string]struct{}, 3)
	for _, alias := range []string{"row-a-primary-key", "row-b-primary-key", "row-c-primary-key"} {
		value, err := c.runtimeRecordID(alias)
		if err != nil {
			return err
		}
		runtimeRecords[value] = struct{}{}
	}
	if len(runtimeRecords) != 3 {
		return errors.New("React Native rebuild-requests row identities are not distinct")
	}
	rowByID := make(map[string]clientScopeRow, len(state.ScopeRows))
	for _, row := range state.ScopeRows {
		if row.ScopeID == "" || row.TableName != evidence.tableName || row.RecordID == "" || row.Checksum == "" {
			return errors.New("React Native rebuild-requests scope row identity is invalid")
		}
		if _, duplicate := rowByID[row.RecordID]; duplicate {
			return errors.New("React Native rebuild-requests scope row identity is duplicated")
		}
		if _, expected := runtimeRecords[row.RecordID]; !expected {
			return errors.New("React Native rebuild-requests scope row identity is not authored")
		}
		rowByID[row.RecordID] = row
	}
	var metadata durableMetadata
	if proof.RowMetadata != nil {
		metadata = *proof.RowMetadata
	}
	rowC, err := c.runtimeRecordID("row-c-primary-key")
	if err != nil || metadata.TableName != evidence.tableName || metadata.RecordID != rowC || metadata.ServerVersion == "" || metadata.RowChecksum == nil || *metadata.RowChecksum != rowByID[rowC].Checksum {
		return errors.New("React Native rebuild-requests selected row metadata is inconsistent")
	}
	var provenance []clientScopeRow
	if err := decodeStrictValue(c.finalResult.Provenance, &provenance); err != nil || len(provenance) != 3 {
		return errors.New("React Native rebuild-requests provenance is incomplete")
	}
	seen := make(map[string]struct{}, len(provenance))
	for _, row := range provenance {
		expected, found := rowByID[row.RecordID]
		if !found || row.ScopeID != expected.ScopeID || row.TableName != expected.TableName || row.Checksum != expected.Checksum || row.Generation != expected.Generation {
			return errors.New("React Native rebuild-requests provenance differs from durable state")
		}
		seen[row.RecordID] = struct{}{}
	}
	if len(seen) != len(runtimeRecords) {
		return errors.New("React Native rebuild-requests provenance identities are incomplete")
	}
	return nil
}

func (c *RebuildRequestsCoordinator) validateApplicationRows(evidence rebuildRequestsIdentityEvidence) error {
	rows, err := decodeRows(c.finalResult.Rows)
	if err != nil || len(rows) != 3 {
		return errors.New("React Native rebuild-requests application rows are incomplete")
	}
	identities := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		value, found := row[evidence.primaryField]
		var recordID string
		if !found || json.Unmarshal(value, &recordID) != nil || recordID == "" {
			return errors.New("React Native rebuild-requests application row identity is invalid")
		}
		identities[recordID] = struct{}{}
	}
	if len(identities) != 3 {
		return errors.New("React Native rebuild-requests application row identities are duplicated")
	}
	for _, alias := range []string{"row-a-primary-key", "row-b-primary-key", "row-c-primary-key"} {
		value, err := c.runtimeRecordID(alias)
		if err != nil {
			return err
		}
		if _, found := identities[value]; !found {
			return errors.New("React Native rebuild-requests application rows do not match durable identities")
		}
	}
	return nil
}

func reflectDeepEqualUint64(left, right []uint64) bool {
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
