package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	scopeEmptyPullScenarioPath = "conformance/scenarios/server/scope-empty-pull-001.json"
	scopeEmptyPullScenarioID   = "SCN-SCOPE-EMPTY-PULL-001"
	scopeEmptyPullClientKey    = "scope-empty-pull-client-a"
)

var scopeEmptyPullAliasNames = []string{"current-schema", "rows-table", "identity-scope", "granted-scope", "shared-row-primary-key", "granted-rebuild"}

var scopeEmptyPullStartCaptureSources = []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "request-trace"}
var scopeEmptyPullStartCaptureKeys = []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "request_trace"}
var scopeEmptyPullFinalCaptureSources = []string{"application-rows", "scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace"}
var scopeEmptyPullFinalCaptureKeys = []string{"application_rows", "client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace"}

// ScopeEmptyPullCoordinatorConfig configures one empty scope set pull sidecar.
type ScopeEmptyPullCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
}

// ScopeEmptyPullCoordinatorResult contains the resolved identity evidence.
type ScopeEmptyPullCoordinatorResult struct {
	IdentityResolution []blackbox.NativeIdentityResolution
}

type scopeEmptyPullSteps struct {
	revoke, connect, firstPull, grant, commit, materialize, syncPull, rebuild scenarios.Step
}

// scopeEmptyPullEvidence holds the runtime identities that the final capture must match.
type scopeEmptyPullEvidence struct {
	grantedScope string
	tableName    string
	primaryField string
	recordID     string
}

// ScopeEmptyPullCoordinator drives the authored empty scope set pull through one native bridge.
type ScopeEmptyPullCoordinator struct {
	config ScopeEmptyPullCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string

	steps     scopeEmptyPullSteps
	pageLimit uint64
	evidence  scopeEmptyPullEvidence
	runtime   map[string]json.RawMessage

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	nextSeq   uint64
	waiting   string
	process   actionProcessIdentity
	start     traceSnapshot
	result    ScopeEmptyPullCoordinatorResult
}

// LoadScopeEmptyPullScenario loads the authored empty scope set pull contract.
func LoadScopeEmptyPullScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, scopeEmptyPullScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native scope-empty-pull scenario: %w", err)
	}
	if err := ValidateScopeEmptyPullScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateScopeEmptyPullScenario rejects changes to the closed React Native contract.
func ValidateScopeEmptyPullScenario(scenario scenarios.Scenario) error {
	if _, _, err := scopeEmptyPullScenarioSteps(scenario); err != nil {
		return err
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range scopeEmptyPullAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native scope-empty-pull identity alias %q is absent", name)
		}
	}
	if len(aliases) != len(scopeEmptyPullAliasNames) || len(scenario.NativeIdentityAliases) != len(scopeEmptyPullAliasNames) {
		return errors.New("React Native scope-empty-pull identity aliases are invalid")
	}
	semantic := false
	for _, assertion := range scenario.Assertions {
		if assertion.ID == "ASSERT-SCOPE-EMPTY-PULL-SEMANTIC-001" {
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic {
		return errors.New("React Native scope-empty-pull assertions are invalid")
	}
	counts := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		switch string(obligation.ObligationID) {
		case "OBL-SCOPE-EMPTY-PULL-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				counts["ios"]++
			}
		case "OBL-SCOPE-EMPTY-PULL-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				counts["android"]++
			}
		case "OBL-SCOPE-EMPTY-PULL-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-SCOPE-EMPTY-PULL-001", "CTRL-SCOPE-009") {
				counts["control"]++
			}
		}
	}
	if counts["ios"] != 1 || counts["android"] != 1 || counts["control"] != 1 {
		return errors.New("React Native scope-empty-pull proof obligations are invalid")
	}
	return nil
}

func scopeEmptyPullScenarioSteps(scenario scenarios.Scenario) (scopeEmptyPullSteps, uint64, error) {
	if string(scenario.ID) != scopeEmptyPullScenarioID || len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return scopeEmptyPullSteps{}, 0, errors.New("React Native scope-empty-pull scenario contract is invalid")
	}
	if len(scenario.Steps) != 8 || len(scenario.NativeLifecycleBoundaries) != 0 {
		return scopeEmptyPullSteps{}, 0, errors.New("React Native scope-empty-pull scenario structure is invalid")
	}
	byID := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for _, step := range scenario.Steps {
		byID[step.ID] = step
	}
	var steps scopeEmptyPullSteps
	for _, wanted := range []struct {
		id, key, callID, method string
		target                  *scenarios.Step
	}{
		{"STEP-SCOPE-EMPTY-PULL-REVOKE-001", "model/set-client-assignments", "", "", &steps.revoke},
		{"STEP-SCOPE-EMPTY-PULL-CONNECT-001", "connect/send", "empty_scope_start", "start", &steps.connect},
		{"STEP-SCOPE-EMPTY-PULL-FIRST-PULL-001", "pull/request-page", "empty_scope_start", "start", &steps.firstPull},
		{"STEP-SCOPE-EMPTY-PULL-GRANT-001", "model/set-client-assignments", "", "", &steps.grant},
		{"STEP-SCOPE-EMPTY-PULL-COMMIT-001", "model/commit-source-transaction", "", "", &steps.commit},
		{"STEP-SCOPE-EMPTY-PULL-MATERIALIZE-001", "process/materialize-source-transaction", "", "", &steps.materialize},
		{"STEP-SCOPE-EMPTY-PULL-SYNC-PULL-001", "pull/request-page", "empty_scope_sync", "sync-now", &steps.syncPull},
		{"STEP-SCOPE-EMPTY-PULL-REBUILD-001", "rebuild/request-page", "empty_scope_sync", "sync-now", &steps.rebuild},
	} {
		step, found := byID[scenarios.StepID(wanted.id)]
		if !found || scenarios.OperationKey(step.Operation) != wanted.key || step.NativeBinding == nil || step.ExpectedOutcome.Disposition != "success" {
			return scopeEmptyPullSteps{}, 0, fmt.Errorf("React Native scope-empty-pull step %s is invalid", wanted.id)
		}
		binding := step.NativeBinding
		if wanted.callID == "" {
			if binding.Kind != "controller" {
				return scopeEmptyPullSteps{}, 0, fmt.Errorf("React Native scope-empty-pull step %s is not a controller step", wanted.id)
			}
		} else if binding.Kind != "public-call" || binding.UserID != "user-a" || binding.ClientID != "client-a" || binding.CallID == nil || string(*binding.CallID) != wanted.callID || binding.Stage != "synchronous" || binding.Method != wanted.method || binding.Completion != "idle" {
			return scopeEmptyPullSteps{}, 0, fmt.Errorf("React Native scope-empty-pull public call %s is invalid", wanted.id)
		}
		*wanted.target = step
	}
	var limit uint64
	for _, step := range []scenarios.Step{steps.firstPull, steps.syncPull, steps.rebuild} {
		var payload struct {
			Limit uint64 `json:"limit"`
		}
		if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.Limit == 0 || limit != 0 && payload.Limit != limit {
			return scopeEmptyPullSteps{}, 0, errors.New("React Native scope-empty-pull authored page limit is invalid")
		}
		limit = payload.Limit
	}
	return steps, limit, nil
}

// NewScopeEmptyPullCoordinator creates an authenticated host-loopback sidecar.
func NewScopeEmptyPullCoordinator(config ScopeEmptyPullCoordinatorConfig) (*ScopeEmptyPullCoordinator, error) {
	if err := ValidateScopeEmptyPullScenario(config.Scenario); err != nil {
		return nil, err
	}
	steps, limit, err := scopeEmptyPullScenarioSteps(config.Scenario)
	if err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native scope-empty-pull coordinator platform must be ios or android")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native scope-empty-pull coordinator auth token is required")
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
		return nil, errors.New("create React Native scope-empty-pull coordinator capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native scope-empty-pull coordinator")
	}
	coordinator := &ScopeEmptyPullCoordinator{config: config, listener: listener, token: token, adapter: adapter, steps: steps, pageLimit: limit, nextSeq: 1}
	coordinator.server = &http.Server{Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return coordinator, nil
}

// Prepare installs the contract and revokes every scope of the test user.
func (c *ScopeEmptyPullCoordinator) Prepare(ctx context.Context) error {
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
		return errors.New("React Native scope-empty-pull coordinator dependencies are unavailable")
	}
	if c.config.AuthToken == "" {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.steps.connect.NativeBinding.UserID, time.Now())
		if err != nil {
			return errors.New("mint React Native scope-empty-pull adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native scope-empty-pull contract: %w", err)
	}
	if result, err := c.config.Controller.ApplyStep(ctx, c.steps.revoke.Operation); err != nil || result.Disposition != "success" {
		return fmt.Errorf("revoke React Native scope-empty-pull identity scope: %w", nativeResultError(err, result.Disposition))
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve runs the sidecar until it closes.
func (c *ScopeEmptyPullCoordinator) Serve(ctx context.Context) error {
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

func (c *ScopeEmptyPullCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *ScopeEmptyPullCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

func (c *ScopeEmptyPullCoordinator) Completed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

// ExchangeCount is five commands and one completion.
func (c *ScopeEmptyPullCoordinator) ExchangeCount() int {
	return 6
}

func (c *ScopeEmptyPullCoordinator) Result() (ScopeEmptyPullCoordinatorResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return ScopeEmptyPullCoordinatorResult{}, fmt.Errorf("%w (waiting=%q, exchanges served=%d)", c.failed, c.waiting, c.nextSeq-1)
	}
	if !c.completed {
		return ScopeEmptyPullCoordinatorResult{}, fmt.Errorf("React Native scope-empty-pull coordinator has not completed (waiting=%q, exchanges served=%d)", c.waiting, c.nextSeq-1)
	}
	return c.result, nil
}

func (c *ScopeEmptyPullCoordinator) Close(ctx context.Context) error {
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

func (c *ScopeEmptyPullCoordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/exchange" {
		writeExchangeError(w, http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		writeExchangeError(w, http.StatusMethodNotAllowed)
		return
	}
	if !validBearer(r.Header.Get("Authorization"), c.token) {
		writeExchangeError(w, http.StatusUnauthorized)
		return
	}
	if r.Header.Get("Content-Type") != "application/json" || r.ContentLength > maximumExchangeBytes {
		writeExchangeError(w, http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		writeExchangeError(w, http.StatusRequestEntityTooLarge)
		return
	}
	exchange, err := decodeExchangeRequest(body)
	if err != nil {
		writeExchangeError(w, http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.prepared || c.failed != nil || c.completed || exchange.Sequence != c.nextSeq {
		c.failed = errors.New("React Native scope-empty-pull exchange is unavailable or non-monotonic")
		writeExchangeError(w, http.StatusConflict)
		return
	}
	response, err := c.exchangeLocked(r.Context(), exchange)
	if err != nil {
		c.failed = fmt.Errorf("React Native scope-empty-pull exchange %d failed: %w", exchange.Sequence, err)
		writeExchangeError(w, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = errors.New("React Native scope-empty-pull exchange response is invalid")
		writeExchangeError(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

// exchangeLocked validates the result of the previous command and returns the next command.
func (c *ScopeEmptyPullCoordinator) exchangeLocked(ctx context.Context, exchange exchangeRequest) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: exchange.Sequence, State: "command"}
	if c.waiting == "" {
		if !isJSONNull(exchange.Result) {
			return exchangeResponse{}, errInvalidExchange
		}
		c.waiting = "open"
		response.Command = c.command("client", "open", map[string]any{"client_key": scopeEmptyPullClientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil})
		return response, nil
	}
	envelope, err := decodeResultEnvelope(exchange.Result)
	if err != nil || envelope.Outcome != "passed" {
		return exchangeResponse{}, errInvalidExchange
	}
	switch c.waiting {
	case "open":
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return exchangeResponse{}, err
		}
		c.process = process
		c.waiting = "start"
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": scopeEmptyPullClientKey, "method": c.steps.connect.NativeBinding.Method, "completion": "idle"})
	case "start":
		if err := validateScopeEmptyPullSynchronized(envelope.Result, c.process); err != nil {
			return exchangeResponse{}, err
		}
		c.waiting = "start-capture"
		response.Command = c.command("observer", "capture", map[string]any{"client_keys": []string{scopeEmptyPullClientKey}, "sources": scopeEmptyPullStartCaptureSources})
	case "start-capture":
		capture, err := decodeCapture(envelope.Result, scopeEmptyPullStartCaptureKeys)
		if err != nil {
			return exchangeResponse{}, err
		}
		if c.start, err = validateScopeEmptyPullStartCapture(capture, c.pageLimit); err != nil {
			return exchangeResponse{}, err
		}
		if err := c.grantScope(ctx); err != nil {
			return exchangeResponse{}, err
		}
		c.waiting = "sync"
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": scopeEmptyPullClientKey, "method": c.steps.syncPull.NativeBinding.Method, "completion": "idle"})
	case "sync":
		if err := validateScopeEmptyPullSynchronized(envelope.Result, c.process); err != nil {
			return exchangeResponse{}, err
		}
		c.waiting = "final-capture"
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{scopeEmptyPullClientKey},
			"sources":     scopeEmptyPullFinalCaptureSources,
			"row_selectors": []map[string]any{{
				"table_name": c.evidence.tableName, "primary_key_field": c.evidence.primaryField, "primary_key": c.evidence.recordID,
			}},
		})
	case "final-capture":
		capture, err := decodeCapture(envelope.Result, scopeEmptyPullFinalCaptureKeys)
		if err != nil {
			return exchangeResponse{}, err
		}
		rebuildID, err := completedRebuildID(capture.Events, c.evidence.grantedScope)
		if err != nil {
			return exchangeResponse{}, err
		}
		if err := validateScopeEmptyPullFinalCapture(capture, c.start, c.pageLimit, c.evidence, rebuildID); err != nil {
			return exchangeResponse{}, err
		}
		if err := c.finish(rebuildID); err != nil {
			return exchangeResponse{}, err
		}
		c.waiting, c.completed = "complete", true
		response.State = "complete"
	default:
		return exchangeResponse{}, errInvalidExchange
	}
	return response, nil
}

// grantScope applies the server grant and row after the start capture proves that the client has no scope.
func (c *ScopeEmptyPullCoordinator) grantScope(ctx context.Context) error {
	for _, step := range []scenarios.Step{c.steps.grant, c.steps.commit} {
		if result, err := c.config.Controller.ApplyStep(ctx, step.Operation); err != nil || result.Disposition != "success" {
			return fmt.Errorf("apply React Native scope-empty-pull step %s: %w", step.ID, nativeResultError(err, result.Disposition))
		}
	}
	if result, err := c.config.Controller.ProcessStep(ctx, nil, c.steps.materialize.Operation); err != nil || result.Disposition != "success" {
		return fmt.Errorf("materialize React Native scope-empty-pull row: %w", nativeResultError(err, result.Disposition))
	}
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.config.Scenario.NativeIdentityAliases))
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if alias.Kind != "rebuild-id" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native scope-empty-pull server identities: %w", err)
	}
	c.runtime = make(map[string]json.RawMessage, len(c.config.Scenario.NativeIdentityAliases))
	identifiers := make(map[string]string, len(values))
	for _, value := range values {
		c.runtime[value.Alias] = copyRaw(value.RuntimeValue)
		identifiers[value.Alias] = value.ApplicationIdentifier
	}
	if json.Unmarshal(c.runtime["granted-scope"], &c.evidence.grantedScope) != nil || c.evidence.grantedScope == "" || json.Unmarshal(c.runtime["shared-row-primary-key"], &c.evidence.recordID) != nil || c.evidence.recordID == "" {
		return errors.New("React Native scope-empty-pull runtime scope or row identity is invalid")
	}
	c.evidence.tableName, c.evidence.primaryField = identifiers["rows-table"], identifiers["shared-row-primary-key"]
	if c.evidence.tableName == "" || c.evidence.primaryField == "" {
		return errors.New("React Native scope-empty-pull application identity evidence is incomplete")
	}
	return nil
}

func (c *ScopeEmptyPullCoordinator) finish(rebuildID string) error {
	encoded, err := json.Marshal(rebuildID)
	if err != nil {
		return err
	}
	c.runtime["granted-rebuild"] = encoded
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		value := c.runtime[alias.Alias]
		if len(value) == 0 {
			return fmt.Errorf("React Native scope-empty-pull alias %q has no runtime evidence", alias.Alias)
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
	resolutions, err := blackbox.ResolveNativeIdentityAliases(c.config.Scenario.NativeIdentityAliases, observations)
	if err != nil {
		return err
	}
	c.result = ScopeEmptyPullCoordinatorResult{IdentityResolution: resolutions}
	return nil
}

func (c *ScopeEmptyPullCoordinator) command(actor, name string, parameters map[string]any) *conformanceCommand {
	clientID := c.steps.connect.NativeBinding.ClientID
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}}, Runtime: conformanceRuntime{ClientKey: scopeEmptyPullClientKey, Database: "rn-scope-empty-pull-" + clientID + ".db", ClientID: clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken, PullPageSize: c.pageLimit}}
}

func validateScopeEmptyPullSynchronized(raw json.RawMessage, process actionProcessIdentity) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if decodeStrictMembers(raw, &members, 4, "scope-empty-pull synchronized result") != nil {
		return errInvalidExchange
	}
	var completion string
	if json.Unmarshal(members["completion"], &completion) != nil || completion != "idle" || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native scope-empty-pull synchronization did not complete idle")
	}
	observed, err := decodeActionProcessIdentity(members["process"])
	if err != nil || observed != process {
		return errors.New("React Native scope-empty-pull process identity changed")
	}
	return nil
}

// validateScopeEmptyPullStartCapture proves that the start call of a client
// with no scope connects and then pulls with an empty scopes map.
func validateScopeEmptyPullStartCapture(capture finalCapture, limit uint64) (traceSnapshot, error) {
	if validateEmptyArray(capture.Pending) != nil || validateEmptyArray(capture.Rejected) != nil || validateReadyStatus(capture.Status) != nil {
		return traceSnapshot{}, errors.New("React Native scope-empty-pull start queues or status are invalid")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return traceSnapshot{}, err
	}
	if state.ScopeStateCount != 0 || len(state.ScopeStates) != 0 || state.ScopeRowCount != 0 || state.ApplicationRowCount != 0 {
		return traceSnapshot{}, errors.New("React Native scope-empty-pull client knows a scope or a row after start")
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return traceSnapshot{}, err
	}
	if trace.Overflowed || len(trace.Observations) != 2 || trace.SequenceCheckpoint != 2 || validateTraceSequence(trace.Observations) != nil {
		return traceSnapshot{}, fmt.Errorf("React Native scope-empty-pull start produced %v, want connect then pull", scopeEmptyPullClassNames(trace.Observations))
	}
	if err := validateTraceOperation(trace.Observations[0], "connect"); err != nil {
		return traceSnapshot{}, fmt.Errorf("React Native scope-empty-pull connect is invalid: %w", err)
	}
	if err := validateWarmConnectConnectRequest(trace.Observations[0], true); err != nil {
		return traceSnapshot{}, fmt.Errorf("React Native scope-empty-pull connect is not a fresh connect with no scope: %w", err)
	}
	if _, err := validateScopeEmptyPullTracePull(trace.Observations[1], limit, 0); err != nil {
		return traceSnapshot{}, err
	}
	return trace, nil
}

// validateScopeEmptyPullFinalCapture proves that one normal cycle with no
// reconnect pulls with an empty scopes map, rebuilds the added scope, and
// materializes its row.
func validateScopeEmptyPullFinalCapture(capture finalCapture, start traceSnapshot, limit uint64, evidence scopeEmptyPullEvidence, rebuildID string) error {
	if validateEmptyArray(capture.Pending) != nil || validateEmptyArray(capture.Rejected) != nil || validateReadyStatus(capture.Status) != nil {
		return errors.New("React Native scope-empty-pull final queues or status are invalid")
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	if trace.Overflowed || len(trace.Observations) != 4 || trace.SequenceCheckpoint != 4 || validateTraceSequence(trace.Observations) != nil || len(start.Observations) != 2 {
		return fmt.Errorf("React Native scope-empty-pull session produced %v, want connect, pull, pull, rebuild", scopeEmptyPullClassNames(trace.Observations))
	}
	for index := range start.Observations {
		if !transportObservationsEqual(trace.Observations[index], start.Observations[index]) {
			return errors.New("React Native scope-empty-pull start trace changed")
		}
	}
	startVersion, err := requestInteger(start.Observations[1], "scope_set_version")
	if err != nil {
		return err
	}
	syncVersion, err := validateScopeEmptyPullTracePull(trace.Observations[2], limit, 1)
	if err != nil {
		return err
	}
	if syncVersion != startVersion {
		return fmt.Errorf("React Native scope-empty-pull sync pull scope set version = %d, want %d", syncVersion, startVersion)
	}
	rebuild := trace.Observations[3]
	if err := validateTraceOperation(rebuild, "rebuild"); err != nil {
		return fmt.Errorf("React Native scope-empty-pull rebuild is invalid: %w", err)
	}
	scope, scopeErr := requestString(rebuild, "scope_fingerprint")
	rebuildFingerprint, rebuildErr := requestString(rebuild, "rebuild_id_fingerprint")
	cursor, cursorErr := requestStringOptional(rebuild, "cursor_fingerprint")
	rebuildLimit, limitErr := requestInteger(rebuild, "limit")
	if scopeErr != nil || scope != hashFingerprint(evidence.grantedScope) || rebuildErr != nil || rebuildFingerprint != hashFingerprint(rebuildID) || cursorErr != nil || cursor != "" || limitErr != nil || rebuildLimit != limit {
		return errors.New("React Native scope-empty-pull rebuild request does not target the added scope")
	}
	facts, err := decodeRebuildResponseFacts(rebuild.RebuildResponseFacts)
	if err != nil {
		return err
	}
	if *facts.RecordCount != 1 || *facts.HasMore || !*facts.HasFinalScopeCursor || !*facts.HasChecksum || *facts.ScopeFingerprint != hashFingerprint(evidence.grantedScope) {
		return errors.New("React Native scope-empty-pull rebuild response is not one terminal page with the granted row")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if state.ScopeStateCount != 1 || len(state.ScopeStates) != 1 || state.ScopeStates[0].ScopeID != evidence.grantedScope || state.ScopeStates[0].Cursor == nil || state.ScopeStates[0].Checksum == nil {
		return errors.New("React Native scope-empty-pull client did not persist the granted scope checkpoint")
	}
	if hashFingerprint(*state.ScopeStates[0].Cursor) != *facts.FinalScopeCursorFingerprint {
		return errors.New("React Native scope-empty-pull scope checkpoint is not the rebuild terminal cursor")
	}
	if state.ScopeRowCount != 1 || len(state.ScopeRows) != 1 || state.ScopeRows[0].ScopeID != evidence.grantedScope || state.ScopeRows[0].TableName != evidence.tableName || state.ScopeRows[0].RecordID != evidence.recordID {
		return errors.New("React Native scope-empty-pull row provenance does not bind the granted scope")
	}
	var provenance []clientScopeRow
	if decodeStrictValue(capture.Provenance, &provenance) != nil || len(provenance) != 1 || provenance[0] != state.ScopeRows[0] {
		return errors.New("React Native scope-empty-pull provenance differs from scope state")
	}
	rows, err := decodeRows(capture.Rows)
	if err != nil {
		return err
	}
	if state.ApplicationRowCount != 1 || len(rows) != 1 || !rowUsesRuntimePrimary(rows[0], evidence.primaryField, evidence.recordID) {
		return errors.New("React Native scope-empty-pull client did not materialize the granted row")
	}
	if state.RebuildAttemptCount != 0 || len(state.RebuildAttempts) != 0 {
		return errors.New("React Native scope-empty-pull rebuild did not complete")
	}
	return nil
}

// validateScopeEmptyPullTracePull returns the scope set version of one pull
// that carries an empty scopes map.
func validateScopeEmptyPullTracePull(observation transportObservation, limit uint64, rebuildScopes uint64) (uint64, error) {
	if err := validateTraceOperation(observation, "pull"); err != nil {
		return 0, fmt.Errorf("React Native scope-empty-pull pull is invalid: %w", err)
	}
	scopes, scopesErr := requestInteger(observation, "scope_count")
	pullLimit, limitErr := requestInteger(observation, "limit")
	version, versionErr := requestInteger(observation, "scope_set_version")
	if scopesErr != nil || scopes != 0 || limitErr != nil || pullLimit != limit || versionErr != nil || len(observation.CursorFingerprints) != 0 {
		return 0, errors.New("React Native scope-empty-pull pull does not carry an empty scopes map")
	}
	facts, err := decodePullResponseFacts(observation.PullResponseFacts)
	if err != nil {
		return 0, err
	}
	if *facts.ChangeCount != 0 || *facts.HasMore || *facts.RebuildScopeCount != rebuildScopes || len(facts.ScopeCursorFingerprints) != 0 {
		return 0, fmt.Errorf("React Native scope-empty-pull pull response differs from an assignment reconciliation with %d added scopes", rebuildScopes)
	}
	return version, nil
}

func scopeEmptyPullClassNames(observations []transportObservation) []string {
	names := make([]string, len(observations))
	for index, observation := range observations {
		names[index] = observation.OperationClass
	}
	return names
}
