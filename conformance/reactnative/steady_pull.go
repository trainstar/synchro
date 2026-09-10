package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	steadyPullScenarioPath = "conformance/scenarios/performance/steady-pull-001.json"
	steadyPullScenarioID   = "SCN-PERF-STEADY-PULL-001"
)

var steadyPullStepOrder = []scenarios.StepID{
	"STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001",
	"STEP-PERF-STEADY-PULL-BASELINE-BEGIN-001",
	"STEP-PERF-STEADY-PULL-BASELINE-APPLY-001",
	"STEP-PERF-STEADY-PULL-BASELINE-FINALIZE-001",
	"STEP-PERF-STEADY-PULL-COMMIT-001",
	"STEP-PERF-STEADY-PULL-MATERIALIZE-001",
	"STEP-PERF-STEADY-PULL-001",
	"STEP-PERF-STEADY-PULL-002",
}

var steadyPullAliasNames = []string{
	"client-generation-one",
	"current-schema",
	"scope-a",
	"scope-b",
	"baseline-rebuild",
	"scope-set-version-one",
	"items-table",
	"row-a-primary-key",
	"row-version-one",
	"row-a-checksum",
	"scope-a-checksum",
}

// LoadSteadyPullScenario loads only the authored steady-pull scenario.
func LoadSteadyPullScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, steadyPullScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native steady-pull scenario: %w", err)
	}
	if err := ValidateSteadyPullScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateSteadyPullScenario rejects changes to the closed RN steady-pull contract.
func ValidateSteadyPullScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != steadyPullScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native steady-pull scenario contract is invalid")
	}
	if !orderedIdentifiersEqual(scenario.RequirementIDs, []string{
		"SYNC-CURSOR-002",
		"SYNC-INTEGRITY-001",
		"SYNC-INTEGRITY-003",
		"SYNC-INTEGRITY-004",
		"SYNC-INTEGRITY-005",
		"SYNC-INTEGRITY-006",
		"SYNC-PULL-005",
		"SYNC-INTEGRITY-002",
	}) {
		return errors.New("React Native steady-pull requirement set changed")
	}
	if len(scenario.Steps) != len(steadyPullStepOrder) {
		return errors.New("React Native steady-pull step set changed")
	}
	for index, step := range scenario.Steps {
		if step.ID != steadyPullStepOrder[index] || step.NativeBinding == nil {
			return errors.New("React Native steady-pull step order or binding changed")
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 0 {
		return errors.New("React Native steady-pull lifecycle boundaries changed")
	}
	if len(scenario.NativeIdentityAliases) != len(steadyPullAliasNames) {
		return errors.New("React Native steady-pull identity alias set changed")
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native steady-pull identity alias is invalid")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return errors.New("React Native steady-pull identity alias is duplicated")
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range steadyPullAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native steady-pull identity alias %q is absent", name)
		}
	}
	semantic, wire, performance, pullProgress := false, false, false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-STEADY-PULL-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-STEADY-PULL-WIRE-001":
			wire = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-STEADY-PULL-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-STEADY-PULL-PULL-005":
			pullProgress = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !wire || !performance || !pullProgress || steadyPullExpectedState(scenario) == nil {
		return errors.New("React Native steady-pull assertion or expected state changed")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-STEADY-PULL-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-STEADY-PULL-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-STEADY-PULL-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-STEADY-PULL-001", "CTRL-INTEGRITY-002") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-PERF-STEADY-PULL-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-STEADY-PULL-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-STEADY-PULL-CONTROL-001"] != 1 {
		return errors.New("React Native steady-pull proof obligations are invalid")
	}
	return nil
}

// SteadyPullCoordinatorConfig configures one authenticated React Native steady-pull sidecar.
type SteadyPullCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// SteadyPullCoordinator is the command sidecar for one React Native steady-pull run.
type SteadyPullCoordinator struct {
	config SteadyPullCoordinatorConfig

	listener  net.Listener
	server    *http.Server
	token     string
	adapter   string
	database  string
	proxy     *httputil.ReverseProxy
	transport *http.Transport

	steps      map[scenarios.StepID]scenarios.Step
	expected   *scenarios.StateFacts
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	tableName  string
	primaryKey string

	mu            sync.Mutex
	prepared      bool
	closed        bool
	completed     bool
	failed        error
	stage         steadyPullExchangeStage
	nextSeq       uint64
	bootstrap     *traceSnapshot
	pristine      *finalCapture
	finalResult   *finalCapture
	restartResult *finalCapture
	process       *actionProcessIdentity
	faultIndex    int
	beforeClose   *finalCapture
	faultMu       sync.Mutex
	armedFault    steadyPullFault
	appliedFault  steadyPullFault
	faultFailure  error
	result        SteadyPullCoordinatorResult
}

// SteadyPullCoordinatorResult contains validated server and native identity evidence.
type SteadyPullCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type steadyPullExchangeStage uint8

const (
	steadyPullStageOpen steadyPullExchangeStage = iota
	steadyPullStageBaselineSynchronize
	steadyPullStageBaselineCapture
	steadyPullStageMeasuredSynchronize
	steadyPullStageFaultSynchronize
	steadyPullStageFaultCapture
	steadyPullStageFinalCapture
	steadyPullStageApplicationRows
	steadyPullStageRestart
	steadyPullStageRestartCapture
	steadyPullStageRestartRows
	steadyPullStageComplete
)

type steadyPullFault string

const (
	steadyPullMalformedTypedRow steadyPullFault = "malformed-typed-row"
	steadyPullRowDigest         steadyPullFault = "row-digest"
	steadyPullScopeDigest       steadyPullFault = "scope-digest"
	steadyPullTerminalMap       steadyPullFault = "terminal-map"
	steadyPullDuplicateEffect   steadyPullFault = "duplicate-effect"
	steadyPullCursorBeforeApply steadyPullFault = "cursor-before-apply"
	steadyPullResponseLimit                     = 16 << 20
)

var steadyPullFaults = []steadyPullFault{
	steadyPullMalformedTypedRow,
	steadyPullRowDigest,
	steadyPullScopeDigest,
	steadyPullTerminalMap,
	steadyPullDuplicateEffect,
	steadyPullCursorBeforeApply,
}

// NewSteadyPullCoordinator creates an authenticated loopback listener for one supported platform.
func NewSteadyPullCoordinator(config SteadyPullCoordinatorConfig) (*SteadyPullCoordinator, error) {
	if err := ValidateSteadyPullScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native steady-pull coordinator platform must be ios or android")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native steady-pull coordinator auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	upstream, err := url.Parse(serverURL)
	if err != nil || upstream.Scheme == "" || upstream.Host == "" {
		return nil, errors.New("React Native steady-pull adapter URL is invalid")
	}
	if _, err := nativeAdapterURL(serverURL, config.Platform); err != nil {
		return nil, err
	}
	if err := validateReactNativeSteadyPullFaultPlans(config.Scenario); err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native steady-pull coordinator capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-steady-pull-")
		if err != nil {
			return nil, errors.New("create React Native steady-pull private database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native steady-pull database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native steady-pull coordinator")
	}
	adapterURL, err := nativeAdapterURL("http://"+listener.Addr().String(), config.Platform)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = transport
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	coordinator := &SteadyPullCoordinator{
		config:     config,
		listener:   listener,
		token:      token,
		adapter:    adapterURL,
		database:   database,
		proxy:      proxy,
		transport:  transport,
		steps:      steps,
		expected:   steadyPullExpectedState(config.Scenario),
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage),
		nextSeq:    1,
	}
	proxy.ModifyResponse = coordinator.modifyAdapterResponse
	coordinator.server = &http.Server{
		Handler:           coordinator,
		MaxHeaderBytes:    16 * 1024,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       30 * time.Second,
	}
	return coordinator, nil
}

// Prepare installs the authored model and binds pre-commit runtime identities.
func (c *SteadyPullCoordinator) Prepare(ctx context.Context) error {
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
		token, err := c.config.Harness.NativeBearerToken(ctx, userID, time.Now())
		if err != nil {
			return errors.New("mint React Native steady-pull adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if c.config.Controller != nil {
		if c.config.Harness == nil {
			return errors.New("React Native steady-pull coordinator harness is unavailable")
		}
		if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
			return fmt.Errorf("install React Native steady-pull contract: %w", err)
		}
		if err := c.bindRuntimeIdentities(ctx, false); err != nil {
			return err
		}
	}
	if c.config.Controller != nil && c.tableName == "" {
		return errors.New("React Native steady-pull runtime table identity is unavailable")
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *SteadyPullCoordinator) Serve(ctx context.Context) error {
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

// Handler exposes the authenticated exchange handler for focused unit tests.
func (c *SteadyPullCoordinator) Handler() http.Handler { return c }

// URL returns the loopback sidecar URL.
func (c *SteadyPullCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

// Token returns the capability required by the exchange endpoint.
func (c *SteadyPullCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// Completed reports whether the coordinator validated the final application row capture.
func (c *SteadyPullCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

// Result returns the validated final server and identity evidence.
func (c *SteadyPullCoordinator) Result() (SteadyPullCoordinatorResult, error) {
	if c == nil {
		return SteadyPullCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return SteadyPullCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return SteadyPullCoordinatorResult{}, errors.New("React Native steady-pull coordinator has not completed")
	}
	return c.result, nil
}

// Close stops the sidecar without closing the externally owned controller.
func (c *SteadyPullCoordinator) Close(ctx context.Context) error {
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
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	if listenErr != nil && !errors.Is(listenErr, net.ErrClosed) {
		return listenErr
	}
	return nil
}

func (c *SteadyPullCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/exchange" {
		c.proxy.ServeHTTP(writer, request)
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
		c.failed = errors.New("React Native steady-pull exchange sequence is not monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native steady-pull exchange sequence %d failed at stage %d: %w", exchange.Sequence, c.stage, err)
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
		c.failed = errors.New("React Native steady-pull exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *SteadyPullCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == steadyPullStageOpen {
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
	case steadyPullStageBaselineSynchronize:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
		return nil
	case steadyPullStageBaselineCapture:
		if err := c.validateSynchronizedResult(envelope.Result); err != nil {
			return err
		}
		return nil
	case steadyPullStageMeasuredSynchronize:
		capture, err := c.validateCaptureResult(envelope.Result, steadyPullDurableCaptureKeys(true))
		if err != nil {
			return err
		}
		trace, err := captureTraceFromRaw(capture.Trace)
		if err != nil {
			return err
		}
		if err := validateSteadyPullBaselineTrace(trace); err != nil {
			return err
		}
		if err := validateBootstrapRebuildEvidence(capture.DurableProof, trace); err != nil {
			return err
		}
		if err := validateSteadyPullBaselineWires(c.config.Scenario, trace); err != nil {
			return err
		}
		c.bootstrap = &trace
		c.pristine = &capture
		return nil
	case steadyPullStageFaultSynchronize:
		if c.faultIndex >= len(steadyPullFaults) {
			return errors.New("React Native steady-pull fault index is invalid")
		}
		if err := c.validateFailedSynchronizedResult(envelope.Result, steadyPullFaults[c.faultIndex]); err != nil {
			return err
		}
		return c.verifyFault(steadyPullFaults[c.faultIndex])
	case steadyPullStageFaultCapture:
		capture, err := c.validateCaptureResult(envelope.Result, append(steadyPullDurableCaptureKeys(false), "request_trace"))
		if err != nil {
			return err
		}
		if err := validateSteadyPullFailureCapture(capture); err != nil {
			return err
		}
		trace, err := captureTraceFromRaw(capture.Trace)
		if err != nil {
			return err
		}
		if err := validateSteadyPullFaultTrace(trace, c.bootstrap, c.faultIndex); err != nil {
			return err
		}
		if c.pristine == nil || !equalReactNativeSteadyPullDurableState(*c.pristine, capture) {
			return fmt.Errorf("React Native steady-pull %s fault changed durable rows or cursor state", steadyPullFaults[c.faultIndex])
		}
		c.faultIndex++
		return nil
	case steadyPullStageFinalCapture:
		if err := c.validateSynchronizedResult(envelope.Result); err != nil {
			return err
		}
		return nil
	case steadyPullStageApplicationRows:
		capture, err := c.validateCaptureResult(envelope.Result, []string{
			"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof",
		})
		if err != nil {
			return err
		}
		if err := validateSteadyPullFinalCapture(c.config.Scenario, capture, c.bootstrap); err != nil {
			return err
		}
		c.finalResult = &capture
		return nil
	case steadyPullStageRestart:
		if c.finalResult == nil {
			return errors.New("React Native steady-pull final capture is unavailable")
		}
		rows, err := captureRows(envelope.Result)
		if err != nil {
			return err
		}
		c.finalResult.Rows = rows
		beforeClose := *c.finalResult
		c.beforeClose = &beforeClose
		return nil
	case steadyPullStageRestartCapture:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		if c.process == nil || process.DatabaseIdentityFingerprint != c.process.DatabaseIdentityFingerprint {
			return errors.New("React Native steady-pull database identity changed after restart")
		}
		c.process = &process
		return nil
	case steadyPullStageRestartRows:
		capture, err := c.validateCaptureResult(envelope.Result, steadyPullDurableCaptureKeys(true))
		if err != nil {
			return err
		}
		if c.beforeClose == nil || !equalReactNativeSteadyPullDurableState(*c.beforeClose, capture) {
			return errors.New("React Native steady-pull restart changed durable rows or cursor state")
		}
		c.restartResult = &capture
		return nil
	case steadyPullStageComplete:
		if c.finalResult == nil || c.restartResult == nil || c.beforeClose == nil {
			return errors.New("React Native steady-pull restart evidence is unavailable")
		}
		rows, err := captureRows(envelope.Result)
		if err != nil {
			return err
		}
		c.restartResult.Rows = rows
		if !jsonValuesEqual(c.beforeClose.Rows, c.restartResult.Rows) {
			return errors.New("React Native steady-pull application rows changed after restart")
		}
		return nil
	default:
		return errInvalidExchange
	}
}

func (c *SteadyPullCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case steadyPullStageOpen:
		response.Command = c.command("client", "open", map[string]any{
			"client_key": clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil,
		}, nil)
		c.stage = steadyPullStageBaselineSynchronize
	case steadyPullStageBaselineSynchronize:
		response.Command = c.command("client", "synchronize-step", map[string]any{
			"client_key": clientKey, "method": "start", "completion": "idle",
		}, []scenarios.StepID{
			"STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001",
			"STEP-PERF-STEADY-PULL-BASELINE-BEGIN-001",
			"STEP-PERF-STEADY-PULL-BASELINE-APPLY-001",
			"STEP-PERF-STEADY-PULL-BASELINE-FINALIZE-001",
		})
		c.stage = steadyPullStageBaselineCapture
	case steadyPullStageBaselineCapture:
		response.Command = c.steadyPullCaptureCommand(true, true)
		c.stage = steadyPullStageMeasuredSynchronize
	case steadyPullStageMeasuredSynchronize:
		if c.config.Controller == nil {
			return exchangeResponse{}, errors.New("React Native steady-pull coordinator controller is unavailable")
		}
		committed, err := c.config.Controller.ApplyStep(ctx, c.steps["STEP-PERF-STEADY-PULL-COMMIT-001"].Operation)
		if err != nil || committed.Disposition != "success" {
			return exchangeResponse{}, fmt.Errorf("commit React Native steady-pull source row: %w", nativeResultError(err, committed.Disposition))
		}
		materialized, err := c.config.Controller.ProcessStep(ctx, nil, c.steps["STEP-PERF-STEADY-PULL-MATERIALIZE-001"].Operation)
		if err != nil || materialized.Disposition != "success" {
			return exchangeResponse{}, fmt.Errorf("materialize React Native steady-pull source row: %w", nativeResultError(err, materialized.Disposition))
		}
		if err := c.bindRuntimeIdentities(ctx, true); err != nil {
			return exchangeResponse{}, err
		}
		if err := c.armFault(steadyPullFaults[0]); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("client", "synchronize-step", map[string]any{
			"client_key": clientKey, "method": "sync-now", "completion": "idle",
		}, []scenarios.StepID{"STEP-PERF-STEADY-PULL-001", "STEP-PERF-STEADY-PULL-002"})
		c.stage = steadyPullStageFaultSynchronize
	case steadyPullStageFaultSynchronize:
		response.Command = c.steadyPullFaultCaptureCommand()
		c.stage = steadyPullStageFaultCapture
	case steadyPullStageFaultCapture:
		if c.faultIndex < len(steadyPullFaults) {
			if err := c.armFault(steadyPullFaults[c.faultIndex]); err != nil {
				return exchangeResponse{}, err
			}
			response.Command = c.command("client", "synchronize-step", map[string]any{
				"client_key": clientKey, "method": "retry-after-error", "completion": "idle",
			}, []scenarios.StepID{"STEP-PERF-STEADY-PULL-001", "STEP-PERF-STEADY-PULL-002"})
			c.stage = steadyPullStageFaultSynchronize
		} else {
			response.Command = c.command("client", "synchronize-step", map[string]any{
				"client_key": clientKey, "method": "retry-after-error", "completion": "idle",
			}, []scenarios.StepID{"STEP-PERF-STEADY-PULL-001", "STEP-PERF-STEADY-PULL-002"})
			c.stage = steadyPullStageFinalCapture
		}
	case steadyPullStageFinalCapture:
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{clientKey},
			"sources":     []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"},
		}, nil)
		c.stage = steadyPullStageApplicationRows
	case steadyPullStageApplicationRows:
		if c.finalResult == nil {
			return exchangeResponse{}, errors.New("React Native steady-pull final capture is unavailable")
		}
		metadata, err := durableRowMetadata(c.finalResult.DurableProof)
		if err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{clientKey},
			"sources":     []string{"application-rows"},
			"row_selectors": []map[string]any{{
				"table_name": c.tableName, "primary_key_field": c.primaryKey, "primary_key": metadata.RecordID,
			}},
		}, nil)
		c.stage = steadyPullStageRestart
	case steadyPullStageRestart:
		response.Command = c.command("client", "open", map[string]any{
			"client_key": clientKey, "database_mode": "reuse", "initialization": "empty", "seed_step_id": nil,
		}, nil)
		c.stage = steadyPullStageRestartCapture
	case steadyPullStageRestartCapture:
		response.Command = c.steadyPullCaptureCommand(true, false)
		c.stage = steadyPullStageRestartRows
	case steadyPullStageRestartRows:
		if c.restartResult == nil {
			return exchangeResponse{}, errors.New("React Native steady-pull restart capture is unavailable")
		}
		metadata, err := durableRowMetadata(c.restartResult.DurableProof)
		if err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{clientKey},
			"sources":     []string{"application-rows"},
			"row_selectors": []map[string]any{{
				"table_name": c.tableName, "primary_key_field": c.primaryKey, "primary_key": metadata.RecordID,
			}},
		}, nil)
		c.stage = steadyPullStageComplete
	case steadyPullStageComplete:
		if err := c.validateCompletionLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	}
	return response, nil
}

func validateReactNativeSteadyPullFaultPlans(scenario scenarios.Scenario) error {
	required := map[string]bool{
		"FPL-PERF-STEADY-PULL-CURSOR-002":    false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-003": false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-004": false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-005": false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-006": false,
		"FPL-PERF-STEADY-PULL-PULL-005":      false,
	}
	for _, plan := range scenario.FaultPlans {
		if _, found := required[string(plan.ID)]; found {
			required[string(plan.ID)] = true
		}
	}
	for id, found := range required {
		if !found {
			return fmt.Errorf("React Native steady-pull fault plan %s is absent", id)
		}
	}
	return nil
}

func steadyPullDurableCaptureKeys(includeTrace bool) []string {
	keys := []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "provenance", "durable_proof"}
	if includeTrace {
		keys = append(keys, "sync_events", "request_trace")
	}
	return keys
}

func (c *SteadyPullCoordinator) steadyPullCaptureCommand(includeTrace, absentRow bool) *conformanceCommand {
	sources := []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "provenance", "durable-proof"}
	if includeTrace {
		sources = append(sources, "sync-events", "request-trace")
	}
	parameters := map[string]any{"client_keys": []string{clientKey}, "sources": sources}
	if absentRow {
		parameters["durable_proof_identity"] = map[string]any{"table_name": c.tableName, "record_id": "bootstrap-absent-row"}
	}
	return c.command("observer", "capture", parameters, nil)
}

func (c *SteadyPullCoordinator) steadyPullFaultCaptureCommand() *conformanceCommand {
	parameters := map[string]any{
		"client_keys": []string{clientKey},
		"sources":     []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "provenance", "durable-proof", "request-trace"},
		"durable_proof_identity": map[string]any{
			"table_name": c.tableName, "record_id": "bootstrap-absent-row",
		},
	}
	return c.command("observer", "capture", parameters, nil)
}

func validateSteadyPullFaultTrace(trace traceSnapshot, bootstrap *traceSnapshot, faultIndex int) error {
	if bootstrap == nil || faultIndex < 0 || faultIndex >= len(steadyPullFaults) {
		return errors.New("React Native steady-pull fault trace context is invalid")
	}
	expectedCount := len(bootstrap.Observations) + faultIndex + 1
	if trace.Overflowed || len(trace.Observations) != expectedCount || trace.SequenceCheckpoint != uint64(expectedCount) {
		return fmt.Errorf("React Native steady-pull %s fault trace is incomplete", steadyPullFaults[faultIndex])
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return err
	}
	for index, observation := range bootstrap.Observations {
		if !transportObservationsEqual(trace.Observations[index], observation) {
			return errors.New("React Native steady-pull bootstrap trace changed during a fault")
		}
	}
	for _, observation := range trace.Observations[len(bootstrap.Observations):] {
		if err := validateTraceOperation(observation, "pull"); err != nil || observation.StatusCode != http.StatusOK {
			return fmt.Errorf("React Native steady-pull %s fault trace is invalid", steadyPullFaults[faultIndex])
		}
	}
	return nil
}

func (c *SteadyPullCoordinator) validateFailedSynchronizedResult(raw json.RawMessage, fault steadyPullFault) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "failed synchronized result"); err != nil {
		return err
	}
	var completion string
	if json.Unmarshal(members["completion"], &completion) != nil || completion != "error" {
		return fmt.Errorf("React Native steady-pull %s fault did not fail synchronization", fault)
	}
	if err := validateSteadyPullFailureStatus(members["status"]); err != nil {
		return fmt.Errorf("React Native steady-pull %s fault status is invalid: %w", fault, err)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return errors.New("React Native steady-pull process identity changed during a fault")
	}
	return nil
}

func validateSteadyPullFailureCapture(capture finalCapture) error {
	return validateSteadyPullFailureStatus(capture.Status)
}

func validateSteadyPullFailureStatus(raw json.RawMessage) error {
	var status syncStatus
	if err := jsonstrict.Decode(raw, &status); err != nil {
		return errors.New("failure status is invalid")
	}
	if status.State != "error" || !isJSONNull(status.RetryAt) || !isJSONNull(status.Operation) {
		return errors.New("failure status state is invalid")
	}
	var failure map[string]json.RawMessage
	if err := decodeStrictMembers(status.Failure, &failure, 4, "failure evidence"); err != nil {
		return err
	}
	var operation, code, recovery string
	var retryable bool
	if json.Unmarshal(failure["operation"], &operation) != nil || operation != "pulling" ||
		json.Unmarshal(failure["code"], &code) != nil || code != "invalid_response" ||
		json.Unmarshal(failure["retryable"], &retryable) != nil || retryable ||
		json.Unmarshal(failure["recovery_action"], &recovery) != nil || recovery != "retry" {
		return errors.New("failure evidence is invalid")
	}
	return nil
}

func equalReactNativeSteadyPullDurableState(left, right finalCapture) bool {
	return jsonValuesEqual(left.ClientState, right.ClientState) &&
		jsonValuesEqual(left.Pending, right.Pending) &&
		jsonValuesEqual(left.Rejected, right.Rejected) &&
		jsonValuesEqual(left.Provenance, right.Provenance) &&
		jsonValuesEqual(left.DurableProof, right.DurableProof)
}

func jsonValuesEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil && reflect.DeepEqual(leftValue, rightValue)
}

func (c *SteadyPullCoordinator) armFault(fault steadyPullFault) error {
	c.faultMu.Lock()
	defer c.faultMu.Unlock()
	if c.armedFault != "" || c.appliedFault != "" || c.faultFailure != nil {
		return errors.New("React Native steady-pull response fault is already active")
	}
	c.armedFault = fault
	return nil
}

func (c *SteadyPullCoordinator) verifyFault(fault steadyPullFault) error {
	c.faultMu.Lock()
	defer c.faultMu.Unlock()
	defer func() {
		c.appliedFault = ""
		c.faultFailure = nil
	}()
	if c.faultFailure != nil {
		return c.faultFailure
	}
	if c.armedFault != "" || c.appliedFault != fault {
		return fmt.Errorf("React Native steady-pull %s fault did not mutate one pull response", fault)
	}
	return nil
}

func (c *SteadyPullCoordinator) modifyAdapterResponse(response *http.Response) error {
	if response.StatusCode != http.StatusOK || response.Request == nil || !strings.HasSuffix(response.Request.URL.Path, "/sync/pull") {
		return nil
	}
	c.faultMu.Lock()
	fault := c.armedFault
	if fault == "" {
		c.faultMu.Unlock()
		return nil
	}
	c.armedFault = ""
	c.faultMu.Unlock()
	body, err := io.ReadAll(io.LimitReader(response.Body, steadyPullResponseLimit+1))
	_ = response.Body.Close()
	if err != nil || len(body) > steadyPullResponseLimit {
		err = errors.New("React Native steady-pull pull response exceeds the fault bound")
	} else {
		body, err = mutateReactNativeSteadyPullResponse(body, fault)
	}
	if err != nil {
		c.faultMu.Lock()
		c.faultFailure = err
		c.faultMu.Unlock()
		return err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
	c.faultMu.Lock()
	c.appliedFault = fault
	c.faultMu.Unlock()
	return nil
}

func mutateReactNativeSteadyPullResponse(body []byte, fault steadyPullFault) ([]byte, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, errors.New("decode React Native steady-pull pull response failed")
	}
	var changes []map[string]json.RawMessage
	if err := json.Unmarshal(response["changes"], &changes); err != nil || len(changes) != 1 {
		return nil, errors.New("React Native steady-pull fault requires one canonical change")
	}
	switch fault {
	case steadyPullMalformedTypedRow:
		if err := mutateReactNativeSteadyPullTypedRow(changes[0]); err != nil {
			return nil, err
		}
	case steadyPullRowDigest:
		if err := mutateReactNativeSteadyPullChecksum(changes[0]); err != nil {
			return nil, err
		}
	case steadyPullScopeDigest:
		if err := mutateReactNativeSteadyPullScopeDigest(response); err != nil {
			return nil, err
		}
	case steadyPullTerminalMap:
		if err := mutateReactNativeSteadyPullTerminalMap(response); err != nil {
			return nil, err
		}
	case steadyPullDuplicateEffect:
		duplicate, err := cloneReactNativeSteadyPullChange(changes[0])
		if err != nil {
			return nil, err
		}
		changes = append(changes, duplicate)
	case steadyPullCursorBeforeApply:
		later, err := cloneReactNativeSteadyPullChange(changes[0])
		if err != nil {
			return nil, err
		}
		if err := mutateReactNativeSteadyPullPrimaryKey(later); err != nil {
			return nil, err
		}
		changes = append(changes, later)
	default:
		return nil, errors.New("React Native steady-pull fault is unsupported")
	}
	if fault != steadyPullScopeDigest && fault != steadyPullTerminalMap {
		encoded, err := json.Marshal(changes)
		if err != nil {
			return nil, errors.New("encode React Native steady-pull changes failed")
		}
		response["changes"] = encoded
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, errors.New("encode React Native steady-pull pull response failed")
	}
	return encoded, nil
}

func mutateReactNativeSteadyPullTypedRow(change map[string]json.RawMessage) error {
	var row map[string]json.RawMessage
	var primary map[string]json.RawMessage
	if json.Unmarshal(change["row"], &row) != nil || len(row) == 0 || json.Unmarshal(change["pk"], &primary) != nil {
		return errors.New("React Native steady-pull typed-row fault target is invalid")
	}
	fields := make([]string, 0, len(row))
	for field := range row {
		if _, isPrimary := primary[field]; !isPrimary {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return errors.New("React Native steady-pull typed-row fault has no writable field")
	}
	sort.Strings(fields)
	row[fields[0]] = json.RawMessage("1")
	encoded, err := json.Marshal(row)
	if err != nil {
		return errors.New("encode React Native steady-pull typed-row fault failed")
	}
	change["row"] = encoded
	return nil
}

func mutateReactNativeSteadyPullChecksum(change map[string]json.RawMessage) error {
	var checksum map[string]json.RawMessage
	if json.Unmarshal(change["row_checksum"], &checksum) != nil || len(checksum) == 0 {
		return errors.New("React Native steady-pull row-digest fault target is invalid")
	}
	checksum["digest"] = json.RawMessage(`"0000000000000000000000000000000000000000000000000000000000000000"`)
	encoded, err := json.Marshal(checksum)
	if err != nil {
		return errors.New("encode React Native steady-pull row-digest fault failed")
	}
	change["row_checksum"] = encoded
	return nil
}

func mutateReactNativeSteadyPullScopeDigest(response map[string]json.RawMessage) error {
	var checksums map[string]json.RawMessage
	if json.Unmarshal(response["checksums"], &checksums) != nil || len(checksums) != 1 {
		return errors.New("React Native steady-pull scope-digest fault target is invalid")
	}
	for scope, raw := range checksums {
		var checksum map[string]json.RawMessage
		if json.Unmarshal(raw, &checksum) != nil {
			return errors.New("React Native steady-pull scope digest is invalid")
		}
		checksum["algorithm"] = json.RawMessage(`"sha1"`)
		encoded, err := json.Marshal(checksum)
		if err != nil {
			return errors.New("encode React Native steady-pull scope-digest fault failed")
		}
		checksums[scope] = encoded
	}
	encoded, err := json.Marshal(checksums)
	if err != nil {
		return errors.New("encode React Native steady-pull scope checksum map failed")
	}
	response["checksums"] = encoded
	return nil
}

func mutateReactNativeSteadyPullTerminalMap(response map[string]json.RawMessage) error {
	var checksums map[string]json.RawMessage
	if json.Unmarshal(response["checksums"], &checksums) != nil || len(checksums) != 1 {
		return errors.New("React Native steady-pull terminal-map fault target is invalid")
	}
	for _, checksum := range checksums {
		checksums["native-extra-scope"] = append(json.RawMessage(nil), checksum...)
		break
	}
	encoded, err := json.Marshal(checksums)
	if err != nil {
		return errors.New("encode React Native steady-pull terminal-map fault failed")
	}
	response["checksums"] = encoded
	return nil
}

func cloneReactNativeSteadyPullChange(change map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	encoded, err := json.Marshal(change)
	if err != nil {
		return nil, errors.New("encode React Native steady-pull effect failed")
	}
	var clone map[string]json.RawMessage
	if json.Unmarshal(encoded, &clone) != nil {
		return nil, errors.New("clone React Native steady-pull effect failed")
	}
	return clone, nil
}

func mutateReactNativeSteadyPullPrimaryKey(change map[string]json.RawMessage) error {
	var primary map[string]json.RawMessage
	var row map[string]json.RawMessage
	if json.Unmarshal(change["pk"], &primary) != nil || len(primary) != 1 || json.Unmarshal(change["row"], &row) != nil {
		return errors.New("React Native steady-pull cursor-before-apply fault target is invalid")
	}
	value, _ := json.Marshal("native-cursor-before-apply")
	for field := range primary {
		primary[field] = value
		if _, found := row[field]; found {
			row[field] = value
		}
	}
	change["pk"], _ = json.Marshal(primary)
	change["row"], _ = json.Marshal(row)
	return nil
}

func (c *SteadyPullCoordinator) validateSynchronizedResult(raw json.RawMessage) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "synchronized result"); err != nil {
		return err
	}
	var completion string
	if json.Unmarshal(members["completion"], &completion) != nil || completion != "idle" || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native steady-pull synchronized result is invalid")
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return errors.New("React Native steady-pull process identity changed")
	}
	return nil
}

func (c *SteadyPullCoordinator) validateCaptureResult(raw json.RawMessage, keys []string) (finalCapture, error) {
	capture, err := decodeCapture(raw, keys)
	if err != nil {
		return finalCapture{}, err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "capture result"); err != nil {
		return finalCapture{}, err
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || c.process == nil || process != *c.process {
		return finalCapture{}, errors.New("React Native steady-pull capture process identity changed")
	}
	return capture, nil
}

func (c *SteadyPullCoordinator) validateCompletionLocked(ctx context.Context) error {
	if c.config.Controller == nil {
		return errors.New("React Native steady-pull coordinator controller is unavailable")
	}
	if c.finalResult == nil {
		return errors.New("React Native steady-pull final capture is unavailable")
	}
	rows, err := decodeRows(c.finalResult.Rows)
	if err != nil {
		return err
	}
	metadata, err := durableRowMetadata(c.finalResult.DurableProof)
	if err != nil {
		return err
	}
	if len(rows) != 1 || !rowUsesRuntimePrimary(rows[0], c.primaryKey, metadata.RecordID) {
		return errors.New("React Native steady-pull application row identity is invalid")
	}
	serverCaptures, err := c.config.Controller.Capture(ctx, []string{clientKey}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return fmt.Errorf("capture React Native steady-pull server state: %w", nativeResultError(err, ""))
	}
	resolutions, err := c.resolveIdentities(metadata)
	if err != nil {
		return err
	}
	if err := validateSteadyPullState(c.config.Scenario, serverCaptures[0].StateFacts, *c.finalResult, resolutions, c.tableName, c.primaryKey); err != nil {
		return err
	}
	c.result = SteadyPullCoordinatorResult{ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *SteadyPullCoordinator) bindRuntimeIdentities(ctx context.Context, includePrimary bool) error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" || includePrimary && alias.Kind == "primary-key" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native steady-pull runtime identities: %w", err)
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
	if c.tableName == "" || includePrimary && c.primaryKey == "" {
		return errors.New("React Native steady-pull runtime application identities are unavailable")
	}
	_ = ctx
	return nil
}

func (c *SteadyPullCoordinator) resolveIdentities(metadata durableMetadata) ([]blackbox.NativeIdentityResolution, error) {
	if c.finalResult == nil || c.bootstrap == nil {
		return nil, errors.New("React Native steady-pull identity evidence is incomplete")
	}
	state, err := decodeClientState(c.finalResult.ClientState)
	if err != nil {
		return nil, err
	}
	if len(state.ScopeStates) != 1 || len(state.ScopeRows) != 1 || state.Schema == nil {
		return nil, errors.New("React Native steady-pull client identity state is incomplete")
	}
	trace, err := captureTraceFromRaw(c.finalResult.Trace)
	if err != nil {
		return nil, err
	}
	measured, err := steadyPullTrace(trace, c.bootstrap)
	if err != nil {
		return nil, err
	}
	generation, err := requestInteger(c.bootstrap.Observations[1], "client_generation")
	if err != nil {
		return nil, err
	}
	scopeSetVersion, err := requestInteger(measured[0], "scope_set_version")
	if err != nil {
		return nil, err
	}
	rebuildID, err := completedRebuildID(c.finalResult.Events, state.ScopeStates[0].ScopeID)
	if err != nil {
		return nil, err
	}
	scopeChecksum, err := checksumDigest(state.ScopeStates[0].Checksum)
	if err != nil || scopeChecksum == nil {
		return nil, errors.New("React Native steady-pull scope checksum identity is invalid")
	}
	runtime := make(map[string]json.RawMessage, len(c.identities))
	for alias, value := range map[string]any{
		"client-generation-one": generation,
		"baseline-rebuild":      rebuildID,
		"scope-set-version-one": scopeSetVersion,
		"row-version-one":       metadata.ServerVersion,
		"row-a-checksum":        state.ScopeRows[0].Checksum,
		"scope-a-checksum":      *scopeChecksum,
		"current-schema":        state.Schema,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode React Native steady-pull alias %q: %w", alias, err)
		}
		runtime[alias] = encoded
	}
	for alias, value := range c.runtimeIDs {
		runtime[alias] = copyRaw(value)
	}
	for _, alias := range steadyPullAliasNames {
		if len(runtime[alias]) == 0 {
			return nil, fmt.Errorf("React Native steady-pull alias %q has no runtime evidence", alias)
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
	return blackbox.ResolveNativeIdentityAliases(c.identities, observations)
}

func (c *SteadyPullCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		step := c.steps[id]
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

func randomDatabaseNameWithPrefix(prefix string) (string, error) {
	token, err := randomToken(12)
	if err != nil {
		return "", err
	}
	return prefix + token + ".db", nil
}

func ioReadAll(request *http.Request) ([]byte, error) {
	return io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
}

func decodeStrictMembers(raw json.RawMessage, destination *map[string]json.RawMessage, expected int, name string) error {
	if err := jsonstrict.Decode(raw, destination); err != nil || len(*destination) != expected {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}

func validateSteadyPullBaselineTrace(trace traceSnapshot) error {
	if err := validateBootstrapTrace(trace); err != nil {
		return err
	}
	return nil
}

func validateSteadyPullBaselineWires(scenario scenarios.Scenario, trace traceSnapshot) error {
	if len(trace.Observations) != 3 {
		return errors.New("React Native steady-pull baseline trace is incomplete")
	}
	connect := trace.Observations[0]
	if connect.StatusCode != 200 {
		return errors.New("React Native steady-pull baseline connect did not succeed")
	}
	for _, observation := range trace.Observations[1:2] {
		if err := validateSteadyPullWireObservation(scenario, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", observation); err != nil {
			return err
		}
	}
	return validateSteadyPullWireObservation(scenario, "STEP-PERF-STEADY-PULL-001", trace.Observations[2])
}

func validateSteadyPullWireObservation(scenario scenarios.Scenario, stepID string, observed transportObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if expected.HTTPStatus != observed.StatusCode || expected.HTTPStatus != 200 || expected.Retryable || expected.ErrorCode != nil {
			return fmt.Errorf("React Native steady-pull wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("React Native steady-pull wire expectation %s is absent", stepID)
}

func steadyPullTrace(final traceSnapshot, bootstrap *traceSnapshot) ([]transportObservation, error) {
	if bootstrap == nil {
		return nil, errors.New("React Native steady-pull bootstrap trace is unavailable")
	}
	expectedCount := len(bootstrap.Observations) + len(steadyPullFaults) + 1
	legacyCount := len(bootstrap.Observations) + 1
	if bootstrap.Overflowed || len(bootstrap.Observations) != 3 || final.Overflowed ||
		len(final.Observations) != expectedCount && len(final.Observations) != legacyCount ||
		final.SequenceCheckpoint != uint64(len(final.Observations)) {
		return nil, errors.New("React Native steady-pull trace is incomplete")
	}
	if err := validateTraceSequence(final.Observations); err != nil {
		return nil, err
	}
	for index, observation := range bootstrap.Observations {
		if !transportObservationsEqual(final.Observations[index], observation) {
			return nil, errors.New("React Native steady-pull bootstrap trace changed after its checkpoint")
		}
	}
	for _, observation := range final.Observations[len(bootstrap.Observations):] {
		if err := validateTraceOperation(observation, "pull"); err != nil {
			return nil, fmt.Errorf("React Native steady-pull measured trace is invalid: %w", err)
		}
	}
	return final.Observations[len(final.Observations)-1:], nil
}

func validateSteadyPullFinalCapture(scenario scenarios.Scenario, capture finalCapture, bootstrap *traceSnapshot) error {
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if err := validateFinalClientEvidenceForExpected(steadyPullExpectedState(scenario), state, capture); err != nil {
		return err
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	measured, err := steadyPullTrace(trace, bootstrap)
	if err != nil {
		return err
	}
	if err := validateSteadyPullWireObservation(scenario, "STEP-PERF-STEADY-PULL-001", measured[0]); err != nil {
		return err
	}
	return validateSteadyPullTransportIdentities(state, capture, *bootstrap, measured)
}

func validateSteadyPullTransportIdentities(state inspectedClientState, capture finalCapture, bootstrap traceSnapshot, measured []transportObservation) error {
	if len(bootstrap.Observations) != 3 || len(measured) != 1 || len(state.ScopeStates) != 1 || len(state.ScopeRows) != 1 {
		return errors.New("React Native steady-pull transport identity evidence is incomplete")
	}
	generation, err := requestInteger(bootstrap.Observations[1], "client_generation")
	if err != nil || generation == 0 {
		return errors.New("React Native steady-pull client generation is invalid")
	}
	scopeSetVersion, err := requestInteger(bootstrap.Observations[2], "scope_set_version")
	if err != nil {
		return errors.New("React Native steady-pull baseline scope-set version is invalid")
	}
	requests := []transportObservation{bootstrap.Observations[1], bootstrap.Observations[2], measured[0]}
	for _, request := range requests {
		actualGeneration, generationErr := requestInteger(request, "client_generation")
		version, versionErr := requestInteger(request, "schema_version")
		hash, hashErr := requestString(request, "schema_hash")
		if generationErr != nil || actualGeneration != generation || versionErr != nil || state.Schema == nil || version != state.Schema.Version || hashErr != nil || hash != state.Schema.Hash {
			return errors.New("React Native steady-pull request schema or generation drifted")
		}
	}
	for _, request := range []transportObservation{bootstrap.Observations[2], measured[0]} {
		actualVersion, versionErr := requestInteger(request, "scope_set_version")
		count, countErr := requestInteger(request, "scope_count")
		if versionErr != nil || actualVersion != scopeSetVersion || countErr != nil || count != uint64(len(state.ScopeStates)) {
			return errors.New("React Native steady-pull request scope projection drifted")
		}
	}
	rebuildID, err := completedRebuildID(capture.Events, state.ScopeStates[0].ScopeID)
	if err != nil {
		return err
	}
	fingerprint, err := requestString(bootstrap.Observations[1], "rebuild_id_fingerprint")
	if err != nil || fingerprint != hashFingerprint(rebuildID) {
		return errors.New("React Native steady-pull rebuild identity drifted")
	}
	rebuild, err := decodeRebuildResponseFacts(bootstrap.Observations[1].RebuildResponseFacts)
	if err != nil || rebuild.FinalScopeCursorFingerprint == nil {
		return errors.New("React Native steady-pull rebuild cursor identity is absent")
	}
	baselinePull, err := decodePullResponseFacts(bootstrap.Observations[2].PullResponseFacts)
	if err != nil || *baselinePull.HasMore || *baselinePull.ChangeCount != 0 || *baselinePull.RebuildScopeCount != 0 || len(baselinePull.ScopeCursorFingerprints) != 1 {
		return errors.New("React Native steady-pull baseline pull facts are invalid")
	}
	measuredPull, err := decodePullResponseFacts(measured[0].PullResponseFacts)
	if err != nil || *measuredPull.HasMore || *measuredPull.ChangeCount != 1 || *measuredPull.RebuildScopeCount != 0 || *measuredPull.ChecksumCount != 1 || len(measuredPull.ScopeCursorFingerprints) != 1 {
		return errors.New("React Native steady-pull measured pull facts are invalid")
	}
	if measured[0].CursorFingerprintsComplete == nil || !*measured[0].CursorFingerprintsComplete || len(measured[0].CursorFingerprints) != 1 || state.ScopeStates[0].Cursor == nil {
		return errors.New("React Native steady-pull cursor identity evidence is incomplete")
	}
	if !reflect.DeepEqual(bootstrap.Observations[2].CursorFingerprints, []string{*rebuild.FinalScopeCursorFingerprint}) ||
		!reflect.DeepEqual(measured[0].CursorFingerprints, baselinePull.ScopeCursorFingerprints) ||
		!reflect.DeepEqual(measuredPull.ScopeCursorFingerprints, []string{hashFingerprint(*state.ScopeStates[0].Cursor)}) {
		return errors.New("React Native steady-pull cursor identity is inconsistent")
	}
	return nil
}

func validateSteadyPullState(scenario scenarios.Scenario, server scenarios.StateFacts, capture finalCapture, resolutions []blackbox.NativeIdentityResolution, tableName, primaryKey string) error {
	expected := steadyPullExpectedState(scenario)
	if expected == nil {
		return errors.New("React Native steady-pull authored state is unavailable")
	}
	serverExpected := scenarios.CloneStateFacts(*expected)
	serverExpected.Clients = nil
	if err := validateServerState(serverExpected, server); err != nil {
		return err
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if err := validateFinalClientEvidenceForExpected(expected, state, capture); err != nil {
		return err
	}
	if len(expected.Clients) != 1 || len(expected.Rows) != 1 || len(expected.Scopes) != 2 || len(resolutions) != len(steadyPullAliasNames) {
		return errors.New("React Native steady-pull semantic state is incomplete")
	}
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(resolutions))
	for _, resolution := range resolutions {
		if _, duplicate := resolved[resolution.Alias]; duplicate {
			return errors.New("React Native steady-pull identity resolution is duplicated")
		}
		resolved[resolution.Alias] = resolution
	}
	if len(resolved) != len(steadyPullAliasNames) {
		return errors.New("React Native steady-pull identity resolution is incomplete")
	}
	client := expected.Clients[0]
	if client.RowCount == nil || client.ProvenanceCount == nil || client.CheckpointCount == nil || state.ApplicationRowCount != *client.RowCount || state.ProvenanceCount != *client.ProvenanceCount || state.ScopeStateCount != *client.CheckpointCount || len(client.Provenance) != 1 || len(client.Provenance[0].Scopes) != 1 || len(client.Checkpoints) != 1 || client.Checkpoints[0].Checksum == nil {
		return errors.New("React Native steady-pull client state differs from the authored model")
	}
	metadata, err := durableRowMetadata(capture.DurableProof)
	if err != nil {
		return err
	}
	scopeChecksum, err := checksumDigest(state.ScopeStates[0].Checksum)
	if err != nil || scopeChecksum == nil {
		return errors.New("React Native steady-pull scope checksum identity is invalid")
	}
	provenance := client.Provenance[0]
	checkpoint := client.Checkpoints[0]
	runtimeSchema := *state.Schema
	if !resolutionAuthoredStringMatches(resolved["items-table"], provenance.TableID) || tableName != state.ScopeRows[0].TableName || metadata.TableName != tableName ||
		!resolutionCanonicalStringMatches(resolved["row-a-primary-key"], provenance.CanonicalWireJSON, state.ScopeRows[0].RecordID) ||
		!resolutionStringMatches(resolved["scope-a"], provenance.Scopes[0], state.ScopeStates[0].ScopeID) ||
		!resolutionStringMatches(resolved["row-version-one"], provenance.Version, metadata.ServerVersion) ||
		!resolutionStringMatches(resolved["scope-a"], checkpoint.ScopeID, state.ScopeStates[0].ScopeID) ||
		!resolutionStringMatches(resolved["scope-a-checksum"], *checkpoint.Checksum, *scopeChecksum) ||
		!resolutionSchemaRuntimeMatches(resolved["current-schema"], runtimeSchema) {
		return errors.New("React Native steady-pull client identities differ from the authored model")
	}
	if !checkpoint.HasCursor || !checkpoint.HasChecksum || !checkpoint.Verified {
		return errors.New("React Native steady-pull checkpoint state differs from the authored model")
	}
	row := expected.Rows[0]
	if !resolutionAuthoredStringMatches(resolved["items-table"], row.TableID) ||
		!resolutionCanonicalStringMatches(resolved["row-a-primary-key"], row.CanonicalWireJSON, state.ScopeRows[0].RecordID) ||
		!resolutionStringMatches(resolved["row-version-one"], row.Version, metadata.ServerVersion) ||
		!resolutionStringMatches(resolved["row-a-checksum"], row.Checksum, state.ScopeRows[0].Checksum) {
		return errors.New("React Native steady-pull row identities differ from the authored model")
	}
	for _, scope := range expected.Scopes {
		resolution, found := resolved[scope.ScopeID]
		if !found || !resolutionAuthoredStringMatches(resolution, scope.ScopeID) {
			return errors.New("React Native steady-pull scope identities differ from the authored model")
		}
	}
	rows, err := decodeRows(capture.Rows)
	if err != nil || len(rows) != 1 || !rowUsesRuntimePrimary(rows[0], primaryKey, metadata.RecordID) {
		return errors.New("React Native steady-pull application row does not use its runtime primary key")
	}
	return nil
}

func resolutionCanonicalStringMatches(resolution blackbox.NativeIdentityResolution, authoredCanonical, runtime string) bool {
	var authored, resolvedAuthored, resolvedRuntime string
	return json.Unmarshal([]byte(authoredCanonical), &authored) == nil &&
		json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil && resolvedAuthored == authored &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil && resolvedRuntime == runtime
}

func resolutionSchemaRuntimeMatches(resolution blackbox.NativeIdentityResolution, runtime clientSchema) bool {
	var resolved clientSchema
	return json.Unmarshal(resolution.RuntimeValue, &resolved) == nil && resolved == runtime
}

func steadyPullExpectedState(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		value := scenario.Model.ExpectedState[index]
		if value.ID == "EXPECT-PERF-STEADY-PULL-SEMANTIC-001" && value.StateFacts != nil {
			return value.StateFacts
		}
	}
	return nil
}
