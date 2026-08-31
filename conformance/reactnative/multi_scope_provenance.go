package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	multiScopeProvenanceScenarioPath = "conformance/scenarios/performance/multi-scope-provenance-001.json"
	multiScopeProvenanceScenarioID   = "SCN-PERF-MULTI-SCOPE-PROVENANCE-001"
)

// MultiScopeProvenanceCoordinatorConfig configures one authenticated native sidecar.
type MultiScopeProvenanceCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
}

// MultiScopeProvenanceCoordinatorResult contains the checked server result.
type MultiScopeProvenanceCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type multiScopeProvenanceCall struct {
	step       scenarios.Step
	operations []scenarios.Step
	stepIDs    []scenarios.StepID
	key        string
}

// MultiScopeProvenanceCoordinator drives the complete authored workload through one native bridge.
type MultiScopeProvenanceCoordinator struct {
	config MultiScopeProvenanceCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string

	calls        []multiScopeProvenanceCall
	expected     scenarios.StateFacts
	started      map[string]bool
	captures     map[string]finalCapture
	callCaptures map[scenarios.StepID]finalCapture
	// The adapter treats the bearer claim as the authoritative user, and this
	// scenario drives one client per user. One shared token would make every
	// client act as the first user.
	authTokens map[string]string

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	nextSeq   uint64
	current   int
	waiting   string
	result    MultiScopeProvenanceCoordinatorResult
}

// LoadMultiScopeProvenanceScenario loads the authored multi-scope scenario.
func LoadMultiScopeProvenanceScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, filepath.Clean(multiScopeProvenanceScenarioPath))
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native multi-scope provenance scenario: %w", err)
	}
	if err := ValidateMultiScopeProvenanceScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateMultiScopeProvenanceScenario rejects a changed native contract.
func ValidateMultiScopeProvenanceScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != multiScopeProvenanceScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native multi-scope provenance scenario contract is invalid")
	}
	if _, err := multiScopeProvenanceCalls(scenario); err != nil {
		return err
	}
	semantic, wire, performance := false, false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-MULTI-SCOPE-PROVENANCE-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-MULTI-SCOPE-PROVENANCE-WIRE-001":
			wire = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-MULTI-SCOPE-PROVENANCE-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !wire || !performance || multiScopeProvenanceExpected(scenario) == nil {
		return errors.New("React Native multi-scope provenance assertions are invalid")
	}
	counts := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		switch string(obligation.ObligationID) {
		case "OBL-PERF-MULTI-SCOPE-PROVENANCE-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				counts["ios"]++
			}
		case "OBL-PERF-MULTI-SCOPE-PROVENANCE-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				counts["android"]++
			}
		case "OBL-PERF-MULTI-SCOPE-PROVENANCE-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-MULTI-SCOPE-PROVENANCE-001", "CTRL-PROVENANCE-002") {
				counts["control"]++
			}
		}
	}
	if counts["ios"] != 1 || counts["android"] != 1 || counts["control"] != 1 {
		return errors.New("React Native multi-scope provenance proof obligations are invalid")
	}
	return nil
}

// NewMultiScopeProvenanceCoordinator creates a host-loopback coordinator for either supported platform.
func NewMultiScopeProvenanceCoordinator(config MultiScopeProvenanceCoordinatorConfig) (*MultiScopeProvenanceCoordinator, error) {
	if err := ValidateMultiScopeProvenanceScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native multi-scope provenance platform must be ios or android")
	}
	if (config.Controller == nil || config.Harness == nil) && (config.ServerURL == "" || config.AuthToken == "") {
		return nil, errors.New("React Native multi-scope provenance dependencies are unavailable")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	serverURL := config.ServerURL
	if serverURL == "" {
		serverURL = config.Harness.AdapterURL()
	}
	adapter, err := nativeAdapterURL(serverURL, config.Platform)
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native multi-scope provenance capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native multi-scope provenance coordinator")
	}
	calls, err := multiScopeProvenanceCalls(config.Scenario)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	coordinator := &MultiScopeProvenanceCoordinator{config: config, listener: listener, token: token, adapter: adapter, calls: calls, expected: *multiScopeProvenanceExpected(config.Scenario), started: make(map[string]bool), captures: make(map[string]finalCapture), callCaptures: make(map[scenarios.StepID]finalCapture), authTokens: make(map[string]string), nextSeq: 1}
	coordinator.server = &http.Server{Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return coordinator, nil
}

// Prepare installs the authored contract and verifies the reference model.
func (c *MultiScopeProvenanceCoordinator) Prepare(ctx context.Context) error {
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
		return errors.New("React Native multi-scope provenance dependencies are unavailable")
	}
	// Each authored client belongs to its own user, so each client carries its
	// own bearer token. A configured token overrides minting for every client.
	for _, call := range c.calls {
		if _, found := c.authTokens[call.key]; found {
			continue
		}
		if c.config.AuthToken != "" {
			c.authTokens[call.key] = c.config.AuthToken
			continue
		}
		token, err := c.config.Harness.NativeBearerToken(ctx, call.step.NativeBinding.UserID, time.Now())
		if err != nil {
			return errors.New("mint React Native multi-scope provenance adapter bearer token")
		}
		c.authTokens[call.key] = token
	}
	modelScenario := c.config.Scenario
	modelScenario.Model.ExpectedState = multiScopeProvenanceSemanticExpectations(modelScenario.Model.ExpectedState)
	model, err := modelrunner.RunScenario(ctx, modelScenario)
	if err != nil || !model.Passed || len(model.Steps) != len(c.config.Scenario.Steps) {
		return errors.New("validate React Native multi-scope provenance authored model")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native multi-scope provenance contract: %w", err)
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve runs until Close or context cancellation.
func (c *MultiScopeProvenanceCoordinator) Serve(ctx context.Context) error {
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

func (c *MultiScopeProvenanceCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}
func (c *MultiScopeProvenanceCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}
func (c *MultiScopeProvenanceCoordinator) Completed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c != nil && c.completed && c.failed == nil
}
func (c *MultiScopeProvenanceCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	return len(c.calls)*3 + 1
}
func (c *MultiScopeProvenanceCoordinator) Result() (MultiScopeProvenanceCoordinatorResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return MultiScopeProvenanceCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return MultiScopeProvenanceCoordinatorResult{}, errors.New("React Native multi-scope provenance coordinator has not completed")
	}
	return c.result, nil
}

func (c *MultiScopeProvenanceCoordinator) Close(ctx context.Context) error {
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

func (c *MultiScopeProvenanceCoordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		writeExchangeError(w, http.StatusConflict)
		return
	}
	if err := c.acceptLocked(exchange.Result); err != nil {
		c.failed = err
		writeExchangeError(w, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(r.Context(), exchange.Sequence)
	if err != nil {
		c.failed = err
		writeExchangeError(w, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil {
		c.failed = err
		writeExchangeError(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

func (c *MultiScopeProvenanceCoordinator) acceptLocked(raw json.RawMessage) error {
	if c.waiting == "" {
		if !isJSONNull(raw) {
			return errInvalidExchange
		}
		return nil
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return errInvalidExchange
	}
	switch c.waiting {
	case "open":
		_, err = validateOpenedResult(envelope.Result)
	case "stop":
		err = validateActionResult(envelope.Result, "lifecycle")
	case "sync":
		err = validateActionResult(envelope.Result, "synchronized")
	case "capture":
		capture, captureErr := decodeCapture(envelope.Result, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace"})
		if captureErr != nil {
			return captureErr
		}
		if err = validateMultiScopeProvenanceCapture(capture); err == nil {
			c.captures[c.calls[c.current].key] = capture
			c.callCaptures[c.calls[c.current].step.ID] = capture
		}
	default:
		return errInvalidExchange
	}
	return err
}

func (c *MultiScopeProvenanceCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	if c.waiting == "capture" {
		c.current++
		c.waiting = ""
	}
	if c.current == len(c.calls) {
		if err := c.finishLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		c.completed = true
		response.State = "complete"
		return response, nil
	}
	call := c.calls[c.current]
	if c.waiting == "" {
		if err := c.executeOperations(ctx, call.operations); err != nil {
			return exchangeResponse{}, err
		}
		if c.started[call.key] {
			c.waiting = "stop"
			response.Command = c.command(call, "client", "lifecycle", map[string]any{"client_key": call.key, "operation": "stop"})
		} else {
			c.started[call.key] = true
			c.waiting = "open"
			response.Command = c.command(call, "client", "open", map[string]any{"client_key": call.key, "database_mode": "create", "initialization": "empty", "seed_step_id": nil})
		}
		return response, nil
	}
	if c.waiting == "open" || c.waiting == "stop" {
		c.waiting = "sync"
		response.Command = c.command(call, "client", "synchronize-step", map[string]any{"client_key": call.key, "method": "start", "completion": "idle"})
		return response, nil
	}
	if c.waiting == "sync" {
		c.waiting = "capture"
		response.Command = c.command(call, "observer", "capture", map[string]any{"client_keys": []string{call.key}, "sources": []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace"}})
		return response, nil
	}
	return exchangeResponse{}, errInvalidExchange
}

func (c *MultiScopeProvenanceCoordinator) command(call multiScopeProvenanceCall, actor, name string, parameters map[string]any) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(call.stepIDs))
	for _, id := range call.stepIDs {
		for _, step := range c.config.Scenario.Steps {
			if step.ID == id {
				steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: step.Operation.ContractOperation, Name: step.Operation.Name, Payload: copyRaw(step.Operation.Payload)}})
				break
			}
		}
	}
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: conformanceRuntime{ClientKey: call.key, Database: "rn-multi-scope-provenance-" + call.step.NativeBinding.ClientID + ".db", ClientID: call.step.NativeBinding.ClientID, ServerURL: c.adapter, AuthToken: c.authTokens[call.key]}}
}

func (c *MultiScopeProvenanceCoordinator) executeOperations(ctx context.Context, steps []scenarios.Step) error {
	for _, step := range steps {
		var result blackbox.NativeStepObservation
		var err error
		switch scenarios.OperationKey(step.Operation) {
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/set-client-assignments":
			result, err = c.config.Controller.ApplyStep(ctx, step.Operation)
		case "process/materialize-source-transaction":
			result, err = c.config.Controller.ProcessStep(ctx, nil, step.Operation)
		default:
			return fmt.Errorf("React Native multi-scope provenance operation %q is unsupported", scenarios.OperationKey(step.Operation))
		}
		if err != nil || result.Disposition != "success" {
			return fmt.Errorf("execute React Native multi-scope provenance step %s: %w", step.ID, nativeResultError(err, result.Disposition))
		}
	}
	return nil
}

func (c *MultiScopeProvenanceCoordinator) finishLocked(ctx context.Context) error {
	if len(c.captures) != len(c.expected.Clients) {
		return errors.New("React Native multi-scope provenance client captures are incomplete")
	}
	for _, expected := range c.expected.Clients {
		capture, found := c.captures[expected.UserID+"\x00"+expected.ClientID]
		if !found {
			return fmt.Errorf("React Native multi-scope provenance client %s is absent", expected.ClientID)
		}
		if err := validateMultiScopeProvenanceClient(expected, capture); err != nil {
			return fmt.Errorf("React Native multi-scope provenance client %s: %w", expected.ClientID, err)
		}
	}
	keys := make([]string, 0, len(c.calls))
	for _, call := range c.calls {
		keys = append(keys, call.key)
	}
	sort.Strings(keys)
	captures, err := c.config.Controller.Capture(ctx, keys, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return fmt.Errorf("capture React Native multi-scope provenance server state: %w", nativeResultError(err, ""))
	}
	if err := validateServerState(c.expected, captures[0].StateFacts); err != nil {
		return err
	}
	resolutions, err := c.resolveIdentities(captures[0].StateFacts)
	if err != nil {
		return err
	}
	c.result = MultiScopeProvenanceCoordinatorResult{ServerFacts: captures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *MultiScopeProvenanceCoordinator) resolveIdentities(server scenarios.StateFacts) ([]blackbox.NativeIdentityResolution, error) {
	runtime := make(map[string]json.RawMessage, len(c.config.Scenario.NativeIdentityAliases))
	serverAliases := make([]scenarios.NativeIdentityAlias, 0)
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		switch alias.Kind {
		case "schema", "scope", "table", "primary-key":
			serverAliases = append(serverAliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(serverAliases)
	if err != nil {
		return nil, fmt.Errorf("resolve React Native multi-scope provenance server identities: %w", err)
	}
	for _, value := range values {
		runtime[value.Alias] = copyRaw(value.RuntimeValue)
	}
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if len(runtime[alias.Alias]) != 0 {
			continue
		}
		value, err := c.runtimeIdentity(alias, runtime, server)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode React Native multi-scope provenance identity %s: %w", alias.Alias, err)
		}
		runtime[alias.Alias] = encoded
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("React Native multi-scope provenance alias %s has no runtime value", alias.Alias)
		}
		for _, stepID := range alias.StepIDs {
			owner := stepID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &owner, RuntimeValue: runtime[alias.Alias]})
		}
		for _, expectationID := range alias.ExpectationIDs {
			owner := expectationID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &owner, RuntimeValue: runtime[alias.Alias]})
		}
	}
	return blackbox.ResolveNativeIdentityAliases(c.config.Scenario.NativeIdentityAliases, observations)
}

func (c *MultiScopeProvenanceCoordinator) runtimeIdentity(alias scenarios.NativeIdentityAlias, runtime map[string]json.RawMessage, server scenarios.StateFacts) (any, error) {
	if len(alias.StepIDs) != 1 {
		return nil, fmt.Errorf("React Native multi-scope provenance alias %s has no single anchor", alias.Alias)
	}
	anchor := alias.StepIDs[0]
	switch alias.Kind {
	case "client-generation":
		capture, found := c.callCaptures[anchor]
		if !found {
			return nil, fmt.Errorf("React Native multi-scope provenance generation anchor %s is absent", anchor)
		}
		trace, err := captureTraceFromRaw(capture.Trace)
		if err != nil {
			return nil, err
		}
		for _, observation := range trace.Observations {
			if generation, err := requestInteger(observation, "client_generation"); err == nil && generation > 0 {
				return generation, nil
			}
		}
	case "rebuild-id":
		call, found := c.callForStep(anchor)
		if !found {
			return nil, fmt.Errorf("React Native multi-scope provenance rebuild anchor %s is absent", anchor)
		}
		step, found := c.stepByID(anchor)
		if !found {
			return nil, fmt.Errorf("React Native multi-scope provenance rebuild step %s is absent", anchor)
		}
		var payload struct {
			ScopeID string `json:"scope_id"`
		}
		if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.ScopeID == "" {
			return nil, fmt.Errorf("React Native multi-scope provenance rebuild step %s is invalid", anchor)
		}
		// The authored step names a scope placeholder, and the client only ever
		// sees the scope identifier the server assigns. Server evidence keeps
		// the authored name, so the rebuild identity resolves there. The Kotlin
		// consumer resolves the same identity the same way.
		return multiScopeProvenanceServerRebuildID(server, call, payload.ScopeID)
	case "row-version", "checksum":
		primary, err := multiScopeProvenanceRuntimePrimary(anchor, c.config.Scenario.NativeIdentityAliases, runtime)
		if err != nil {
			return nil, err
		}
		canonical, err := json.Marshal(primary)
		if err != nil {
			return nil, err
		}
		for _, row := range server.Rows {
			if row.CanonicalWireJSON != string(canonical) {
				continue
			}
			if alias.Kind == "row-version" {
				// A row fact reports the authored version, so the runtime
				// version comes from the versions the capture observed.
				runtimeVersion, bound := c.config.Controller.RuntimeRowVersions()[row.CanonicalWireJSON]
				if !bound || runtimeVersion == "" {
					return nil, fmt.Errorf("React Native multi-scope provenance row alias %s has no runtime version", alias.Alias)
				}
				return runtimeVersion, nil
			}
			return row.Checksum, nil
		}
	}
	return nil, fmt.Errorf("React Native multi-scope provenance alias %s has no runtime evidence", alias.Alias)
}

func (c *MultiScopeProvenanceCoordinator) callForStep(id scenarios.StepID) (multiScopeProvenanceCall, bool) {
	for _, call := range c.calls {
		for _, stepID := range call.stepIDs {
			if stepID == id {
				return call, true
			}
		}
	}
	return multiScopeProvenanceCall{}, false
}
func (c *MultiScopeProvenanceCoordinator) stepByID(id scenarios.StepID) (scenarios.Step, bool) {
	for _, step := range c.config.Scenario.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return scenarios.Step{}, false
}

// multiScopeProvenanceServerRebuildID resolves the rebuild identity the server
// recorded for one client and one authored scope. The client cannot supply it,
// because the client observes the assigned scope identifier rather than the
// authored placeholder the step names.
func multiScopeProvenanceServerRebuildID(server scenarios.StateFacts, call multiScopeProvenanceCall, scopeID string) (string, error) {
	userID := call.step.NativeBinding.UserID
	clientID := call.step.NativeBinding.ClientID
	var rebuildID string
	matches := 0
	for _, recorded := range server.Rebuilds {
		if recorded.UserID != userID || recorded.ClientID != clientID || recorded.ScopeID != scopeID {
			continue
		}
		if recorded.RebuildID == "" {
			return "", fmt.Errorf("server rebuild for client %s and scope %s has no identity", clientID, scopeID)
		}
		matches++
		rebuildID = recorded.RebuildID
	}
	if matches != 1 {
		return "", fmt.Errorf("server rebuild for client %s and scope %s matched %d records, want 1", clientID, scopeID, matches)
	}
	return rebuildID, nil
}

func multiScopeProvenanceRuntimePrimary(anchor scenarios.StepID, aliases []scenarios.NativeIdentityAlias, runtime map[string]json.RawMessage) (string, error) {
	for _, alias := range aliases {
		if alias.Kind == "primary-key" {
			for _, stepID := range alias.StepIDs {
				if stepID == anchor {
					var primary string
					if json.Unmarshal(runtime[alias.Alias], &primary) == nil && primary != "" {
						return primary, nil
					}
				}
			}
		}
	}
	return "", fmt.Errorf("React Native multi-scope provenance primary key for %s is unavailable", anchor)
}

func multiScopeProvenanceCalls(scenario scenarios.Scenario) ([]multiScopeProvenanceCall, error) {
	var calls []multiScopeProvenanceCall
	pending := make([]scenarios.Step, 0)
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" || scenarios.ValidateOperation(step.Operation) != nil {
			return nil, fmt.Errorf("React Native multi-scope provenance step %s is invalid", step.ID)
		}
		key := scenarios.OperationKey(step.Operation)
		if key == "connect/send" {
			binding := step.NativeBinding
			if step.Transport != "http" || binding == nil || binding.Kind != "public-call" || binding.UserID == "" || binding.ClientID == "" || !multiScopeProvenanceCallMethod(binding.Method) || binding.Completion != "idle" {
				return nil, fmt.Errorf("React Native multi-scope provenance connect %s is invalid", step.ID)
			}
			calls = append(calls, multiScopeProvenanceCall{step: step, operations: append([]scenarios.Step(nil), pending...), stepIDs: []scenarios.StepID{step.ID}, key: binding.UserID + "\x00" + binding.ClientID})
			pending = nil
			continue
		}
		if step.NativeBinding == nil {
			return nil, fmt.Errorf("React Native multi-scope provenance binding %s is absent", step.ID)
		}
		if step.NativeBinding.Kind == "controller" {
			pending = append(pending, step)
			continue
		}
		if len(calls) == 0 || step.NativeBinding.Kind != "public-call" || step.NativeBinding.UserID != calls[len(calls)-1].step.NativeBinding.UserID || step.NativeBinding.ClientID != calls[len(calls)-1].step.NativeBinding.ClientID {
			return nil, fmt.Errorf("React Native multi-scope provenance public step %s is invalid", step.ID)
		}
		calls[len(calls)-1].stepIDs = append(calls[len(calls)-1].stepIDs, step.ID)
	}
	if len(calls) == 0 || len(pending) != 0 {
		return nil, errors.New("React Native multi-scope provenance calls are incomplete")
	}
	return calls, nil
}

func multiScopeProvenanceExpected(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		value := &scenario.Model.ExpectedState[index]
		if value.ID == "EXPECT-PERF-MULTI-SCOPE-PROVENANCE-SEMANTIC-001" && value.StateFacts != nil {
			return value.StateFacts
		}
	}
	return nil
}
func multiScopeProvenanceSemanticExpectations(values []scenarios.ModelExpectation) []scenarios.ModelExpectation {
	result := make([]scenarios.ModelExpectation, 0, len(values))
	for _, value := range values {
		if value.Predicate.Name != "performance-contract-satisfied" {
			result = append(result, value)
		}
	}
	return result
}

func validateMultiScopeProvenanceCapture(capture finalCapture) error {
	if err := validateEmptyArray(capture.Pending); err != nil {
		return err
	}
	if err := validateEmptyArray(capture.Rejected); err != nil {
		return err
	}
	if err := validateReadyStatus(capture.Status); err != nil {
		return err
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil || trace.Overflowed || len(trace.Observations) == 0 || validateTraceSequence(trace.Observations) != nil || validateTraceOperation(trace.Observations[len(trace.Observations)-1], "pull") != nil {
		return errors.New("React Native multi-scope provenance trace is invalid")
	}
	// Each reported count must agree with its own detail list. Provenance count
	// is a different quantity than the scope-row count, because one row that
	// belongs to two scopes produces two scope rows and one provenance record.
	// The authored model owns the provenance value.
	if state.ScopeStateCount != uint64(len(state.ScopeStates)) || state.ScopeRowCount != uint64(len(state.ScopeRows)) {
		// A count alone cannot explain the mismatch. Name each reported count
		// and each detail length so one run shows which pair disagrees.
		return fmt.Errorf(
			"React Native multi-scope provenance durable counts are inconsistent: "+
				"scope state count %d with %d details, scope row count %d with %d details",
			state.ScopeStateCount, len(state.ScopeStates),
			state.ScopeRowCount, len(state.ScopeRows),
		)
	}
	return nil
}

func validateMultiScopeProvenanceClient(expected scenarios.ClientDurabilityFact, capture finalCapture) error {
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if expected.RowCount == nil || expected.ProvenanceCount == nil || expected.CheckpointCount == nil || expected.RebuildAttemptCount == nil {
		return errors.New("authored client facts are incomplete")
	}
	// Name each count that differs. A bare message hides which of the three
	// disagrees and costs a second run to learn it.
	differences := make([]string, 0, 3)
	for _, count := range []struct {
		name string
		want uint64
		got  uint64
	}{
		{"row", *expected.RowCount, state.ApplicationRowCount},
		{"provenance", *expected.ProvenanceCount, state.ProvenanceCount},
		{"checkpoint", *expected.CheckpointCount, state.ScopeStateCount},
	} {
		if count.want != count.got {
			differences = append(differences, fmt.Sprintf("%s want %d got %d", count.name, count.want, count.got))
		}
	}
	if len(differences) > 0 {
		return fmt.Errorf("durable client counts differ from the authored model: %s", strings.Join(differences, ", "))
	}
	if uint64(len(state.ScopeStates)) != *expected.CheckpointCount {
		return errors.New("durable client details differ from the authored model")
	}
	if state.RebuildAttemptCount != 0 || state.RebuildReceiptCount != *expected.RebuildAttemptCount {
		return errors.New("durable rebuild evidence differs from the authored model")
	}
	return nil
}

// multiScopeProvenanceCallMethod reports whether an authored call method drives
// a public call. A connect on a client that already runs declares start,
// because sync-now pulls without connecting.
func multiScopeProvenanceCallMethod(method string) bool {
	return method == "sync-now" || method == "start"
}
