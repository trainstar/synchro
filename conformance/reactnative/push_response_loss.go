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
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	pushResponseLossScenarioPath = "conformance/scenarios/server/push-response-loss-001.json"
	pushResponseLossScenarioID   = "SCN-PUSH-RESPONSE-LOSS-001"
)

var pushResponseLossStepOrder = []scenarios.StepID{
	"STEP-PUSH-RESPONSE-LOSS-001", "STEP-PUSH-RESPONSE-LOSS-002", "STEP-PUSH-RESPONSE-LOSS-003",
	"STEP-PUSH-RESPONSE-LOSS-004", "STEP-PUSH-RESPONSE-LOSS-005", "STEP-PUSH-RESPONSE-LOSS-006",
}

var pushResponseLossAliasNames = []string{
	"response-loss-mutation", "response-loss-batch", "client-generation-one", "current-schema", "items-table", "response-loss-primary-key",
}

// LoadPushResponseLossScenario loads only the authored response-loss scenario.
func LoadPushResponseLossScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, pushResponseLossScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native push-response-loss scenario: %w", err)
	}
	if err := ValidatePushResponseLossScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidatePushResponseLossScenario rejects changes to the closed RN response-loss contract.
func ValidatePushResponseLossScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != pushResponseLossScenarioID || len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native push-response-loss scenario contract is invalid")
	}
	if len(scenario.Steps) != len(pushResponseLossStepOrder) || len(scenario.NativeLifecycleBoundaries) != 0 {
		return errors.New("React Native push-response-loss step or lifecycle contract changed")
	}
	want := []struct{ operation, kind, stage, method, completion string }{
		{"local/write", "local-write", "", "", ""},
		{"push/submit", "public-call", "begin", "start", ""},
		{"process/response-loss", "public-call", "await-step", "", ""},
		{"push/submit", "public-call", "await-call", "", "idle"},
		{"push/submit", "controller", "", "", ""},
		{"push/submit", "controller", "", "", ""},
	}
	var callID string
	for index, step := range scenario.Steps {
		binding := step.NativeBinding
		if step.ID != pushResponseLossStepOrder[index] || binding == nil || scenarios.OperationKey(step.Operation) != want[index].operation || binding.Kind != want[index].kind || binding.Stage != want[index].stage || binding.Method != want[index].method || binding.Completion != want[index].completion || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native push-response-loss step %d contract is invalid", index+1)
		}
		if binding.Kind != "controller" && (binding.UserID == "" || binding.ClientID == "") {
			return fmt.Errorf("React Native push-response-loss step %s native identity is incomplete", step.ID)
		}
		if binding.Kind == "public-call" {
			if binding.CallID == nil || *binding.CallID == "" {
				return fmt.Errorf("React Native push-response-loss step %s call identity is absent", step.ID)
			}
			if callID == "" {
				callID = string(*binding.CallID)
			} else if callID != string(*binding.CallID) {
				return errors.New("React Native push-response-loss public calls do not share one call identity")
			}
		}
	}
	if err := validatePushResponseLossAliases(scenario.NativeIdentityAliases); err != nil {
		return err
	}
	if err := validatePushResponseLossOperations(scenario); err != nil {
		return err
	}
	if err := validatePushResponseLossAssertion(scenario); err != nil {
		return err
	}
	return validatePushResponseLossProofs(scenario)
}

func validatePushResponseLossAliases(aliases []scenarios.NativeIdentityAlias) error {
	if len(aliases) != len(pushResponseLossAliasNames) {
		return fmt.Errorf("React Native push-response-loss identity alias count = %d, want %d", len(aliases), len(pushResponseLossAliasNames))
	}
	seen := make(map[string]bool, len(aliases))
	for _, alias := range aliases {
		if seen[alias.Alias] {
			return fmt.Errorf("React Native push-response-loss identity alias %q is duplicated", alias.Alias)
		}
		seen[alias.Alias] = true
		kind, steps, expectations := pushResponseLossAliasContract(alias.Alias)
		if kind == "" || alias.Kind != kind || !pushResponseLossStepIDsEqual(alias.StepIDs, steps) || !pushResponseLossExpectationIDsEqual(alias.ExpectationIDs, expectations) {
			return fmt.Errorf("React Native push-response-loss identity alias %q is invalid", alias.Alias)
		}
	}
	for _, name := range pushResponseLossAliasNames {
		if !seen[name] {
			return fmt.Errorf("React Native push-response-loss identity alias %q is absent", name)
		}
	}
	return nil
}

func pushResponseLossAliasContract(alias string) (string, []scenarios.StepID, []scenarios.ExpectationID) {
	all := []scenarios.StepID{pushResponseLossStepOrder[0], pushResponseLossStepOrder[1], pushResponseLossStepOrder[3], pushResponseLossStepOrder[4], pushResponseLossStepOrder[5]}
	batchSteps := []scenarios.StepID{pushResponseLossStepOrder[1], pushResponseLossStepOrder[2], pushResponseLossStepOrder[3], pushResponseLossStepOrder[4], pushResponseLossStepOrder[5]}
	pushes := []scenarios.StepID{pushResponseLossStepOrder[1], pushResponseLossStepOrder[3], pushResponseLossStepOrder[4], pushResponseLossStepOrder[5]}
	switch alias {
	case "response-loss-mutation":
		return "mutation-id", all, []scenarios.ExpectationID{"EXPECT-PUSH-RESPONSE-LOSS-SEMANTIC-001"}
	case "response-loss-batch":
		return "batch-id", batchSteps, []scenarios.ExpectationID{"EXPECT-PUSH-RESPONSE-LOSS-SEMANTIC-001"}
	case "client-generation-one":
		return "client-generation", pushes, nil
	case "current-schema":
		return "schema", all, nil
	case "items-table":
		return "table", all, nil
	case "response-loss-primary-key":
		return "primary-key", all, nil
	default:
		return "", nil, nil
	}
}

func pushResponseLossStepIDsEqual(left, right []scenarios.StepID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func pushResponseLossExpectationIDsEqual(left, right []scenarios.ExpectationID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func validatePushResponseLossOperations(scenario scenarios.Scenario) error {
	operations := make(map[scenarios.StepID]scenarios.Operation, len(scenario.Steps))
	for _, step := range scenario.Steps {
		operations[step.ID] = step.Operation
	}
	if scenarios.OperationKey(operations[pushResponseLossStepOrder[2]]) != "process/response-loss" {
		return errors.New("React Native push-response-loss response-loss operation is invalid")
	}
	ids := []scenarios.StepID{pushResponseLossStepOrder[1], pushResponseLossStepOrder[3], pushResponseLossStepOrder[4], pushResponseLossStepOrder[5]}
	decoded := make([]pushResponseLossPayload, len(ids))
	for i, id := range ids {
		op, err := pushResponseLossOperation(operations, id, "push/submit")
		if err != nil {
			return err
		}
		decoded[i], err = decodePushResponseLossPayload(op)
		if err != nil {
			return err
		}
	}
	if decoded[0].Delivery != "drop_after_server" || decoded[1].Delivery != "apply" || decoded[2].Delivery != "apply" || decoded[3].Delivery != "apply" || decoded[0].AuthenticatedUserID == "" || decoded[0].Request.ClientID == "" || decoded[0].Request.BatchID == "" || len(decoded[0].Request.Mutations) == 0 {
		return errors.New("React Native push-response-loss request bindings are invalid")
	}
	for _, payload := range decoded[1:] {
		if payload.AuthenticatedUserID != decoded[0].AuthenticatedUserID || payload.Request.ClientID != decoded[0].Request.ClientID || payload.Request.BatchID != decoded[0].Request.BatchID {
			return errors.New("React Native push-response-loss replay identity differs from the initial request")
		}
	}
	if !equalPushResponseLossRequest(operations[ids[0]], operations[ids[1]]) || !equalPushResponseLossRequest(operations[ids[0]], operations[ids[2]]) || !equalPushResponseLossMutations(decoded[0].Request.Mutations, decoded[1].Request.Mutations) || !equalPushResponseLossMutations(decoded[0].Request.Mutations, decoded[2].Request.Mutations) || equalPushResponseLossRequest(operations[ids[0]], operations[ids[3]]) || equalPushResponseLossMutations(decoded[0].Request.Mutations, decoded[3].Request.Mutations) {
		return errors.New("React Native push-response-loss replay content bindings are invalid")
	}
	want := map[scenarios.StepID]struct {
		caseName  string
		status    int
		retryable bool
		code      string
	}{
		pushResponseLossStepOrder[1]: {"transport_failure", 0, true, ""}, pushResponseLossStepOrder[3]: {"push_success", 200, false, ""},
		pushResponseLossStepOrder[4]: {"push_success", 200, false, ""}, pushResponseLossStepOrder[5]: {"idempotency_conflict", 409, false, "idempotency_conflict"},
	}
	if len(scenario.WireExpectations) != len(want) {
		return errors.New("React Native push-response-loss wire expectation count is invalid")
	}
	for id, expected := range want {
		wire, found := pushResponseLossWireExpectation(scenario, id)
		code := ""
		if found && wire.ErrorCode != nil {
			code = *wire.ErrorCode
		}
		if !found || wire.ContractCase != expected.caseName || wire.HTTPStatus != expected.status || wire.Retryable != expected.retryable || code != expected.code {
			return fmt.Errorf("React Native push-response-loss wire expectation %s is invalid", id)
		}
	}
	return nil
}

func pushResponseLossOperation(operations map[scenarios.StepID]scenarios.Operation, id scenarios.StepID, key string) (scenarios.Operation, error) {
	op, found := operations[id]
	if !found || scenarios.OperationKey(op) != key {
		return scenarios.Operation{}, fmt.Errorf("React Native push-response-loss operation %s is invalid", id)
	}
	return op, nil
}

func validatePushResponseLossAssertion(scenario scenarios.Scenario) error {
	for _, assertion := range scenario.Assertions {
		if assertion.ID == "ASSERT-PUSH-RESPONSE-LOSS-SEMANTIC-001" && assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.Kind == "wire-contract" && assertion.Oracle.ExpectedSource == "authored-model" && assertion.Oracle.ObservedSource == "system-under-test" && len(assertion.ExpectationIDs) == 1 && assertion.ExpectationIDs[0] == "EXPECT-PUSH-RESPONSE-LOSS-SEMANTIC-001" && len(assertion.DetectsControlIDs) == 1 && assertion.DetectsControlIDs[0] == "CTRL-IDEMPOTENCY-001" {
			return nil
		}
	}
	return errors.New("React Native push-response-loss assertion contract is invalid")
}

func validatePushResponseLossProofs(scenario scenarios.Scenario) error {
	want := map[string]struct{ proof, cell, target string }{
		"OBL-PUSH-RESPONSE-LOSS-RN-IOS-CURRENT-001":     {"native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios"},
		"OBL-PUSH-RESPONSE-LOSS-RN-ANDROID-CURRENT-001": {"native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android"},
		"OBL-PUSH-RESPONSE-LOSS-CONTROL-001":            {"negative-control", "", "test-conformance"},
	}
	counts := make(map[string]int)
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		expected, found := want[id]
		if !found {
			continue
		}
		fault, control := "", ""
		if expected.proof == "negative-control" {
			fault, control = "FPL-PUSH-RESPONSE-LOSS-001", "CTRL-IDEMPOTENCY-001"
		}
		if proofTargetMatches(obligation, expected.proof, expected.cell, expected.target, fault, control) {
			counts[id]++
		}
	}
	for id := range want {
		if counts[id] != 1 {
			return fmt.Errorf("React Native push-response-loss proof obligation %s count = %d, want 1", id, counts[id])
		}
	}
	return nil
}

type pushResponseLossPayload struct {
	AuthenticatedUserID string `json:"authenticated_user_id"`
	Request             struct {
		ClientID  string            `json:"client_id"`
		BatchID   string            `json:"batch_id"`
		Mutations []json.RawMessage `json:"mutations"`
	} `json:"request"`
	Delivery  string `json:"delivery"`
	CommitLSN string `json:"commit_lsn"`
	EndLSN    string `json:"end_lsn"`
}

func decodePushResponseLossPayload(operation scenarios.Operation) (pushResponseLossPayload, error) {
	var payload pushResponseLossPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return payload, fmt.Errorf("decode React Native push-response-loss request: %w", err)
	}
	return payload, nil
}

func equalPushResponseLossRequest(left, right scenarios.Operation) bool {
	canonical := func(operation scenarios.Operation) ([]byte, error) {
		var payload struct {
			Request json.RawMessage `json:"request"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Request) == 0 {
			return nil, errors.New("request absent")
		}
		var value any
		if err := json.Unmarshal(payload.Request, &value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	l, le := canonical(left)
	r, re := canonical(right)
	return le == nil && re == nil && bytes.Equal(l, r)
}

func equalPushResponseLossMutations(left, right []json.RawMessage) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		var l, r any
		if json.Unmarshal(left[i], &l) != nil || json.Unmarshal(right[i], &r) != nil {
			return false
		}
		lj, le := json.Marshal(l)
		rj, re := json.Marshal(r)
		if le != nil || re != nil || !bytes.Equal(lj, rj) {
			return false
		}
	}
	return true
}

func pushResponseLossAppliedOperation(operation scenarios.Operation) (scenarios.Operation, error) {
	var payload map[string]any
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload["delivery"] != "drop_after_server" {
		return scenarios.Operation{}, errors.New("React Native push-response-loss committed push is invalid")
	}
	payload["delivery"] = "apply"
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, err
	}
	operation.Payload = encoded
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, err
	}
	return operation, nil
}

func pushResponseLossWireExpectation(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.WireExpectation, bool) {
	var found scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == id {
			found = wire
			count++
		}
	}
	return found, count == 1
}

// PushResponseLossCoordinatorConfig configures one authenticated RN response-loss sidecar.
type PushResponseLossCoordinatorConfig struct {
	Scenario                                             scenarios.Scenario
	Harness                                              *blackbox.Harness
	Controller                                           *blackbox.NativeController
	Platform, ServerURL, AuthToken, AppVersion, Database string
}

// PushResponseLossCoordinator is the command sidecar for one RN response-loss run.
type PushResponseLossCoordinator struct {
	config                                             PushResponseLossCoordinatorConfig
	listener                                           net.Listener
	server                                             *http.Server
	transport                                          *http.Client
	token, adapter, upstream, database                 string
	steps                                              map[scenarios.StepID]scenarios.Step
	identities                                         []scenarios.NativeIdentityAlias
	runtimeIDs                                         map[string]json.RawMessage
	authTokens                                         map[string]string
	userID, clientID, clientKey, tableName, primaryKey string

	proxyMu                  sync.Mutex
	pushRequests             uint64
	proxyErr                 error
	pushCommitted            chan struct{}
	pushCommittedOnce        sync.Once
	allowInitialResponse     chan struct{}
	allowInitialResponseOnce sync.Once
	replayCompleted          chan struct{}
	replayCompletedOnce      sync.Once

	mu                          sync.Mutex
	prepared, closed, completed bool
	failed                      error
	stage                       pushResponseLossStage
	nextSeq                     uint64
	process                     *actionProcessIdentity
	finalResult                 *finalCapture
	serverFacts                 *scenarios.StateFacts
	equalReplay, changedReplay  blackbox.NativeStepObservation
	result                      PushResponseLossCoordinatorResult
}

type pushResponseLossStage uint8

const (
	pushResponseLossStageOpen pushResponseLossStage = iota
	pushResponseLossStageBootstrap
	pushResponseLossStageStop
	pushResponseLossStageLocalWrite
	pushResponseLossStageBeginCall
	pushResponseLossStageAwaitStep
	pushResponseLossStageAwaitCall
	pushResponseLossStageFinalCapture
	pushResponseLossStageApplicationRows
	pushResponseLossStageComplete
)

// PushResponseLossCoordinatorResult contains validated server and native identity evidence.
type PushResponseLossCoordinatorResult struct {
	EqualReplay        blackbox.NativeStepObservation
	ChangedReplay      blackbox.NativeStepObservation
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// NewPushResponseLossCoordinator creates an authenticated host-loopback proxy.
func NewPushResponseLossCoordinator(config PushResponseLossCoordinatorConfig) (*PushResponseLossCoordinator, error) {
	if err := ValidatePushResponseLossScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native push-response-loss coordinator platform must be ios or android")
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native push-response-loss coordinator auth token is required")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	upstream, err := nativeAdapterURL(serverURL, "ios")
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	adapter, err := nativeAdapterURL("http://"+listener.Addr().String(), config.Platform)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-push-response-loss-")
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	if !validDatabaseName(database) {
		_ = listener.Close()
		return nil, errors.New("React Native push-response-loss database name is invalid")
	}
	identity, err := pushResponseLossClientIdentity(config.Scenario)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	c := &PushResponseLossCoordinator{config: config, listener: listener, token: token, adapter: adapter, upstream: upstream, database: database, transport: &http.Client{Timeout: 2 * time.Minute}, steps: steps, identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...), runtimeIDs: make(map[string]json.RawMessage), authTokens: make(map[string]string), userID: identity.userID, clientID: identity.clientID, clientKey: identity.clientID, nextSeq: 1, pushCommitted: make(chan struct{}), allowInitialResponse: make(chan struct{}), replayCompleted: make(chan struct{})}
	c.server = &http.Server{Handler: c, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return c, nil
}

type pushResponseLossIdentity struct{ userID, clientID string }

func pushResponseLossClientIdentity(scenario scenarios.Scenario) (pushResponseLossIdentity, error) {
	var payload struct {
		AuthenticatedUserID string `json:"authenticated_user_id"`
		ClientID            string `json:"client_id"`
	}
	if err := json.Unmarshal(scenario.Steps[0].Operation.Payload, &payload); err != nil || payload.AuthenticatedUserID == "" || payload.ClientID == "" {
		return pushResponseLossIdentity{}, errors.New("React Native push-response-loss client identity is invalid")
	}
	for _, step := range scenario.Steps {
		if step.NativeBinding.UserID != "" && step.NativeBinding.UserID != payload.AuthenticatedUserID || step.NativeBinding.ClientID != "" && step.NativeBinding.ClientID != payload.ClientID {
			return pushResponseLossIdentity{}, errors.New("React Native push-response-loss client identity differs across steps")
		}
	}
	return pushResponseLossIdentity{payload.AuthenticatedUserID, payload.ClientID}, nil
}

// Prepare installs the authored model and maps the local write to runtime schema identifiers.
func (c *PushResponseLossCoordinator) Prepare(ctx context.Context) error {
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
		return errors.New("React Native push-response-loss coordinator dependencies are unavailable")
	}
	if c.config.AuthToken == "" {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.userID, time.Now())
		if err != nil {
			return err
		}
		c.config.AuthToken = token
	}
	c.authTokens[c.clientKey] = c.config.AuthToken
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native push-response-loss contract: %w", err)
	}
	local, err := c.config.Controller.ApplicationWrite(c.steps[pushResponseLossStepOrder[0]].Operation)
	if err != nil {
		return err
	}
	step := c.steps[pushResponseLossStepOrder[0]]
	step.Operation = local
	c.steps[pushResponseLossStepOrder[0]] = step
	if err := c.bindServerIdentities(false); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *PushResponseLossCoordinator) Serve(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	if err := c.Prepare(ctx); err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(closeCtx)
			cancel()
		case <-done:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (c *PushResponseLossCoordinator) Handler() http.Handler { return c }
func (c *PushResponseLossCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}
func (c *PushResponseLossCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// ExchangeCount returns the exact number of exchanges required by this coordinator.
func (c *PushResponseLossCoordinator) ExchangeCount() int {
	return int(pushResponseLossStageComplete) + 1
}
func (c *PushResponseLossCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}
func (c *PushResponseLossCoordinator) Result() (PushResponseLossCoordinatorResult, error) {
	if c == nil {
		return PushResponseLossCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return PushResponseLossCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return PushResponseLossCoordinatorResult{}, errors.New("React Native push-response-loss coordinator has not completed")
	}
	return c.result, nil
}

func (c *PushResponseLossCoordinator) Close(ctx context.Context) error {
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
	c.recordProxyFailure(errors.New("React Native push-response-loss coordinator closed"))
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

func (c *PushResponseLossCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
		c.failed = errors.New("React Native push-response-loss exchange is unavailable or non-monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = err
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
		c.failed = errors.New("React Native push-response-loss exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *PushResponseLossCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == pushResponseLossStageOpen {
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
	case pushResponseLossStageBootstrap:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
	case pushResponseLossStageStop:
		return validatePushResponseLossSynchronized(envelope.Result, "idle", c.process)
	case pushResponseLossStageLocalWrite:
		if c.process == nil {
			return errors.New("React Native push-response-loss process identity is unavailable")
		}
		return validateStoppedLifecycleResult(envelope.Result, *c.process)
	case pushResponseLossStageBeginCall:
		return validatePushResponseLossLocal(envelope.Result, c.process)
	case pushResponseLossStageAwaitStep:
		return c.validateCallBegun(envelope.Result)
	case pushResponseLossStageAwaitCall:
		return c.validateAwaited(envelope.Result)
	case pushResponseLossStageFinalCapture:
		return c.validateCallCompleted(envelope.Result)
	case pushResponseLossStageApplicationRows:
		capture, err := decodeCapture(envelope.Result, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace"})
		if err != nil {
			return err
		}
		if err := c.validateFinalCapture(capture); err != nil {
			return err
		}
		c.finalResult = &capture
	case pushResponseLossStageComplete:
		rows, err := captureRows(envelope.Result)
		if err != nil {
			return err
		}
		if c.finalResult == nil {
			return errors.New("React Native push-response-loss final capture is unavailable")
		}
		c.finalResult.Rows = rows
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *PushResponseLossCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case pushResponseLossStageOpen:
		response.Command = c.command("client", "open", map[string]any{"client_key": c.clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil}, nil)
		c.stage = pushResponseLossStageBootstrap
	case pushResponseLossStageBootstrap:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
		c.stage = pushResponseLossStageStop
	case pushResponseLossStageStop:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
		c.stage = pushResponseLossStageLocalWrite
	case pushResponseLossStageLocalWrite:
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{pushResponseLossStepOrder[0]})
		c.stage = pushResponseLossStageBeginCall
	case pushResponseLossStageBeginCall:
		response.Command = c.command("client", "begin-call", map[string]any{"client_key": c.clientKey, "call_id": c.callID(), "method": "start"}, []scenarios.StepID{pushResponseLossStepOrder[1]})
		c.stage = pushResponseLossStageAwaitStep
	case pushResponseLossStageAwaitStep:
		response.Command = c.command("observer", "await-step", map[string]any{"client_key": c.clientKey, "call_id": c.callID()}, []scenarios.StepID{pushResponseLossStepOrder[2]})
		c.stage = pushResponseLossStageAwaitCall
	case pushResponseLossStageAwaitCall:
		if err := c.waitForPushCommit(ctx); err != nil {
			return exchangeResponse{}, err
		}
		if err := c.bindCommittedPush(); err != nil {
			return exchangeResponse{}, err
		}
		c.releaseInitialResponse()
		response.Command = c.command("client", "await-call", map[string]any{"client_key": c.clientKey, "call_id": c.callID()}, []scenarios.StepID{pushResponseLossStepOrder[3]})
		c.stage = pushResponseLossStageFinalCapture
	case pushResponseLossStageFinalCapture:
		if err := c.waitForReplay(ctx); err != nil {
			return exchangeResponse{}, err
		}
		if err := c.runControllerReplays(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "capture", map[string]any{"client_keys": []string{c.clientKey}, "sources": []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace"}}, nil)
		c.stage = pushResponseLossStageApplicationRows
	case pushResponseLossStageApplicationRows:
		if c.finalResult == nil {
			return exchangeResponse{}, errors.New("React Native push-response-loss final capture is unavailable")
		}
		captures, err := c.config.Controller.Capture(ctx, []string{c.clientKey}, []string{"server-state"})
		if err != nil {
			return exchangeResponse{}, err
		}
		if len(captures) != 1 {
			return exchangeResponse{}, errors.New("React Native push-response-loss server state capture is invalid")
		}
		server := captures[0].StateFacts
		if server.BatchCount == nil || *server.BatchCount != 1 || server.MutationCount == nil || *server.MutationCount != 1 {
			return exchangeResponse{}, errors.New("React Native push-response-loss server state is invalid")
		}
		c.serverFacts = &server
		if err := c.bindServerIdentities(true); err != nil {
			return exchangeResponse{}, err
		}
		recordID, err := c.runtimeRecordID()
		if err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "capture", map[string]any{"client_keys": []string{c.clientKey}, "sources": []string{"application-rows"}, "row_selectors": []map[string]any{{"table_name": c.tableName, "primary_key_field": c.primaryKey, "primary_key": recordID}}}, nil)
		c.stage = pushResponseLossStageComplete
	case pushResponseLossStageComplete:
		if err := c.validateCompletionLocked(); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	default:
		return exchangeResponse{}, errInvalidExchange
	}
	return response, nil
}

func (c *PushResponseLossCoordinator) command(actor, name string, parameters map[string]any, ids []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(ids))
	for _, id := range ids {
		step := c.steps[id]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: step.Operation.ContractOperation, Name: step.Operation.Name, Payload: copyRaw(step.Operation.Payload)}})
	}
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: conformanceRuntime{ClientKey: c.clientKey, Database: c.database, ClientID: c.clientID, ServerURL: c.adapter, AuthToken: c.authTokens[c.clientKey]}}
}
func (c *PushResponseLossCoordinator) callID() string {
	for _, step := range c.steps {
		if step.NativeBinding != nil && step.NativeBinding.CallID != nil {
			return string(*step.NativeBinding.CallID)
		}
	}
	return "response_loss_initial"
}

func validatePushResponseLossLocal(raw json.RawMessage, process *actionProcessIdentity) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "React Native push-response-loss local result"); err != nil {
		return err
	}
	var rows uint64
	if json.Unmarshal(members["rows_affected"], &rows) != nil || rows == 0 {
		return errors.New("React Native push-response-loss local write affected no rows")
	}
	return validatePushResponseLossProcess(members["process"], process)
}

func validatePushResponseLossSynchronized(raw json.RawMessage, completion string, process *actionProcessIdentity) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "React Native push-response-loss synchronized result"); err != nil {
		return err
	}
	var actual string
	if json.Unmarshal(members["completion"], &actual) != nil || actual != completion || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native push-response-loss synchronized result is invalid")
	}
	return validatePushResponseLossProcess(members["process"], process)
}
func validatePushResponseLossProcess(raw json.RawMessage, expected *actionProcessIdentity) error {
	actual, err := decodeActionProcessIdentity(raw)
	if err != nil || expected == nil || actual != *expected {
		return errors.New("React Native push-response-loss process identity changed")
	}
	return nil
}
func (c *PushResponseLossCoordinator) validateCallBegun(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-begun"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "React Native push-response-loss call-begun result"); err != nil {
		return err
	}
	var id, state string
	if json.Unmarshal(members["call_id"], &id) != nil || id != c.callID() || json.Unmarshal(members["state"], &state) != nil || state != "in_flight" {
		return errors.New("React Native push-response-loss call did not enter flight")
	}
	return validatePushResponseLossProcess(members["process"], c.process)
}
func (c *PushResponseLossCoordinator) validateAwaited(raw json.RawMessage) error {
	if err := validateActionResult(raw, "awaited"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "React Native push-response-loss awaited result"); err != nil {
		return err
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return err
	}
	return validatePushResponseLossProcess(members["process"], c.process)
}
func (c *PushResponseLossCoordinator) validateCallCompleted(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-completed"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 6, "React Native push-response-loss call-completed result"); err != nil {
		return err
	}
	var id, state, completion string
	if json.Unmarshal(members["call_id"], &id) != nil || id != c.callID() || json.Unmarshal(members["state"], &state) != nil || state != "completed" || json.Unmarshal(members["completion"], &completion) != nil || completion != "idle" {
		return errors.New("React Native push-response-loss call completion is invalid")
	}
	if err := validateReadyStatus(members["status"]); err != nil {
		return err
	}
	return validatePushResponseLossProcess(members["process"], c.process)
}

func (c *PushResponseLossCoordinator) bindServerIdentities(primary bool) error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" || primary && (alias.Kind == "primary-key" || alias.Kind == "batch-id" || alias.Kind == "mutation-id") {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native push-response-loss identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		if value.Alias == "items-table" {
			c.tableName = value.ApplicationIdentifier
		}
		if value.Alias == "response-loss-primary-key" {
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" || primary && c.primaryKey == "" {
		return errors.New("React Native push-response-loss runtime application identities are unavailable")
	}
	return nil
}
func (c *PushResponseLossCoordinator) bindCommittedPush() error {
	operation, err := pushResponseLossAppliedOperation(c.steps[pushResponseLossStepOrder[1]].Operation)
	if err != nil {
		return err
	}
	if err := c.config.Controller.BindApplicationPush(operation); err != nil {
		return err
	}
	return nil
}
func (c *PushResponseLossCoordinator) runControllerReplays(ctx context.Context) error {
	var err error
	c.equalReplay, err = c.config.Controller.RequestStep(ctx, c.steps[pushResponseLossStepOrder[4]].Operation)
	if err != nil {
		return err
	}
	if err := validatePushResponseLossNativeWire(c.config.Scenario, pushResponseLossStepOrder[4], c.equalReplay); err != nil {
		return err
	}
	c.changedReplay, err = c.config.Controller.RequestStep(ctx, c.steps[pushResponseLossStepOrder[5]].Operation)
	if err != nil {
		return err
	}
	return validatePushResponseLossNativeWire(c.config.Scenario, pushResponseLossStepOrder[5], c.changedReplay)
}
func validatePushResponseLossNativeWire(scenario scenarios.Scenario, id scenarios.StepID, observed blackbox.NativeStepObservation) error {
	expected, found := pushResponseLossWireExpectation(scenario, id)
	if !found || observed.Wire == nil {
		return errors.New("React Native push-response-loss wire result is absent")
	}
	sameCode := expected.ErrorCode == nil && observed.Wire.ErrorCode == nil || expected.ErrorCode != nil && observed.Wire.ErrorCode != nil && *expected.ErrorCode == *observed.Wire.ErrorCode
	wantDisposition := "error"
	if expected.HTTPStatus >= 200 && expected.HTTPStatus < 300 {
		wantDisposition = "success"
	}
	if observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !sameCode || observed.Disposition != wantDisposition {
		return fmt.Errorf("React Native push-response-loss wire result %s is invalid", id)
	}
	return nil
}

func (c *PushResponseLossCoordinator) validateFinalCapture(capture finalCapture) error {
	if len(capture.ClientState) == 0 || len(capture.Pending) == 0 || len(capture.Rejected) == 0 || len(capture.Status) == 0 || len(capture.Provenance) == 0 || len(capture.Trace) == 0 {
		return errors.New("React Native push-response-loss final capture is incomplete")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if state.Schema == nil || len(state.ScopeStates) != 1 || len(state.ScopeRows) != 1 || state.ApplicationRowCount != 1 || state.MutationLedgerCount != 1 || state.SealedBatchCount != 1 || state.RejectedMutationCount != 0 {
		return errors.New("React Native push-response-loss durable client state is invalid")
	}
	if validateEmptyArray(capture.Pending) != nil || validateEmptyArray(capture.Rejected) != nil {
		return errors.New("React Native push-response-loss mutation queues are not empty")
	}
	if err := validateReadyStatus(capture.Status); err != nil {
		return err
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	if err := validatePushResponseLossTrace(c.config.Scenario, trace); err != nil {
		return err
	}
	return validateProvenance(capture.Provenance, state.ScopeStates[0], state.ScopeRows[0])
}
func validatePushResponseLossTrace(scenario scenarios.Scenario, trace traceSnapshot) error {
	initialWire, initialFound := pushResponseLossWireExpectation(scenario, pushResponseLossStepOrder[1])
	replayWire, replayFound := pushResponseLossWireExpectation(scenario, pushResponseLossStepOrder[3])
	if !initialFound || !replayFound {
		return fmt.Errorf("React Native push-response-loss replay wire expectations found = %t/%t, want true/true", initialFound, replayFound)
	}
	if trace.Overflowed || trace.SequenceCheckpoint != uint64(len(trace.Observations)) {
		return fmt.Errorf("React Native push-response-loss request trace shape = overflowed:%t observations:%d checkpoint:%d, want overflowed:false and checkpoint:%d", trace.Overflowed, len(trace.Observations), trace.SequenceCheckpoint, len(trace.Observations))
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return fmt.Errorf("React Native push-response-loss request trace sequence = %s, want contiguous sequence: %w", pushResponseLossTraceSummary(trace.Observations), err)
	}
	pushes := make([]transportObservation, 0, 2)
	for _, observation := range trace.Observations {
		if observation.OperationClass == "push" {
			pushes = append(pushes, observation)
		}
	}
	if len(pushes) != 2 {
		return fmt.Errorf("React Native push-response-loss push replay trace = %s (count %d), want two pushes with statuses [%d %d]", pushResponseLossTraceSummary(pushes), len(pushes), initialWire.HTTPStatus, replayWire.HTTPStatus)
	}
	if err := validatePushResponseLossTracePush("initial response-loss", pushes[0], initialWire); err != nil {
		return err
	}
	if err := validatePushResponseLossTracePush("unchanged replay", pushes[1], replayWire); err != nil {
		return err
	}
	var generation uint64
	for index, push := range pushes {
		actual, err := requestInteger(push, "client_generation")
		if err != nil {
			return fmt.Errorf("React Native push-response-loss push %d client generation = invalid (%v), want a positive integer shared by both pushes", index+1, err)
		}
		if actual == 0 {
			return fmt.Errorf("React Native push-response-loss push %d client generation = %d, want a positive integer", index+1, actual)
		}
		if generation != 0 && actual != generation {
			return fmt.Errorf("React Native push-response-loss push %d client generation = %d, want %d from the initial push", index+1, actual, generation)
		}
		generation = actual
	}
	return nil
}

func validatePushResponseLossTracePush(label string, observed transportObservation, expected scenarios.WireExpectation) error {
	if observed.OperationClass != "push" || observed.StatusCode != expected.HTTPStatus || observed.DurationNanoseconds == 0 || !hasJSONValue(observed.RequestFacts) {
		return fmt.Errorf("React Native push-response-loss %s trace = operation:%q status:%d duration:%d request_facts:%t, want operation:%q status:%d nonzero-duration:true request_facts:true for wire retryable:%t error_code:%s", label, observed.OperationClass, observed.StatusCode, observed.DurationNanoseconds, hasJSONValue(observed.RequestFacts), "push", expected.HTTPStatus, expected.Retryable, pushResponseLossTraceErrorCode(expected.ErrorCode))
	}
	return nil
}

func pushResponseLossTraceSummary(observations []transportObservation) string {
	values := make([]string, 0, len(observations))
	for _, observation := range observations {
		values = append(values, fmt.Sprintf("%d:%s/%d", observation.Sequence, observation.OperationClass, observation.StatusCode))
	}
	return "[" + strings.Join(values, ",") + "]"
}

func pushResponseLossTraceErrorCode(value *string) string {
	if value == nil {
		return "none"
	}
	return *value
}
func (c *PushResponseLossCoordinator) runtimeRecordID() (string, error) {
	var value string
	if json.Unmarshal(c.runtimeIDs["response-loss-primary-key"], &value) != nil || value == "" {
		return "", errors.New("React Native push-response-loss runtime primary key is unavailable")
	}
	return value, nil
}
func (c *PushResponseLossCoordinator) validateCompletionLocked() error {
	if c.finalResult == nil {
		return errors.New("React Native push-response-loss final evidence is unavailable")
	}
	rows, err := decodeRows(c.finalResult.Rows)
	if err != nil {
		return err
	}
	recordID, idErr := c.runtimeRecordID()
	if idErr != nil {
		return idErr
	}
	if len(rows) != 1 || !rowUsesRuntimePrimary(rows[0], c.primaryKey, recordID) {
		return errors.New("React Native push-response-loss application row identity is invalid")
	}
	if c.serverFacts == nil {
		return errors.New("React Native push-response-loss server state is unavailable")
	}
	trace, err := captureTraceFromRaw(c.finalResult.Trace)
	if err != nil {
		return err
	}
	identities, err := c.resolveIdentities(trace)
	if err != nil {
		return err
	}
	c.result = PushResponseLossCoordinatorResult{c.equalReplay, c.changedReplay, *c.serverFacts, identities}
	return nil
}
func (c *PushResponseLossCoordinator) resolveIdentities(trace traceSnapshot) ([]blackbox.NativeIdentityResolution, error) {
	var generation uint64
	for _, observation := range trace.Observations {
		if observation.OperationClass != "push" {
			continue
		}
		value, err := requestInteger(observation, "client_generation")
		if err != nil {
			return nil, errors.New("React Native push-response-loss client generation evidence is invalid")
		}
		if value == 0 {
			return nil, errors.New("React Native push-response-loss client generation evidence is invalid")
		}
		if generation != 0 && generation != value {
			return nil, errors.New("React Native push-response-loss client generation changed")
		}
		generation = value
	}
	if generation == 0 {
		return nil, errors.New("React Native push-response-loss client generation is absent")
	}
	encoded, _ := json.Marshal(generation)
	c.runtimeIDs["client-generation-one"] = encoded
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("React Native push-response-loss alias %q has no runtime evidence", alias.Alias)
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
	return blackbox.ResolveNativeIdentityAliases(c.identities, observations)
}

func (c *PushResponseLossCoordinator) proxyAdapter(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		c.recordProxyFailure(errors.New("React Native push-response-loss request body is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	isPush := request.Method == http.MethodPost && request.URL.Path == "/sync/push"
	pushNumber := uint64(0)
	if isPush {
		pushNumber = c.beginPushRequest()
		if pushNumber > 2 {
			c.recordProxyFailure(errors.New("React Native push-response-loss sent more than two push requests"))
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
	}
	target := strings.TrimRight(c.upstream, "/") + request.URL.RequestURI()
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target, bytes.NewReader(body))
	if err != nil {
		c.recordProxyFailure(err)
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
		c.recordProxyFailure(err)
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maximumExchangeBytes+1))
	if err != nil || len(responseBody) > maximumExchangeBytes {
		c.recordProxyFailure(errors.New("React Native push-response-loss upstream response is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	if isPush {
		if response.StatusCode != http.StatusOK {
			c.recordProxyFailure(fmt.Errorf("React Native push-response-loss push status = %d", response.StatusCode))
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
		if pushNumber == 1 {
			c.signalPushCommitted()
			// The iOS HTTP client half-closes after it sends the request
			// body, and the server reports that as request-context
			// cancellation while the client still reads. The hold waits for
			// the coordinated release only, and the loss below reaches a
			// connected client or fails silently on a gone one.
			<-c.allowInitialResponse
			c.loseInitialResponse(writer)
			return
		}
		c.signalReplayCompleted()
	}
	for name, values := range response.Header {
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(responseBody)))
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(responseBody)
}
func (c *PushResponseLossCoordinator) beginPushRequest() uint64 {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	c.pushRequests++
	return c.pushRequests
}
func (c *PushResponseLossCoordinator) loseInitialResponse(writer http.ResponseWriter) {
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		c.recordProxyFailure(errors.New("React Native push-response-loss response writer cannot drop a response"))
		return
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		c.recordProxyFailure(err)
		return
	}
	// A bare close lets the Android HTTP client repeat the request before it records the failure.
	if _, err := connection.Write([]byte("SYNCHRO RESPONSE LOSS\r\n\r\n")); err != nil {
		c.recordProxyFailure(err)
	}
	_ = connection.Close()
}
func (c *PushResponseLossCoordinator) recordProxyFailure(err error) {
	c.proxyMu.Lock()
	if c.proxyErr == nil {
		c.proxyErr = err
	}
	c.proxyMu.Unlock()
	c.signalPushCommitted()
	c.signalReplayCompleted()
	c.releaseInitialResponse()
}
func (c *PushResponseLossCoordinator) proxyFailure(stage string) error {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.proxyErr != nil {
		return fmt.Errorf("React Native push-response-loss %s proxy failed: %w", stage, c.proxyErr)
	}
	return nil
}
func (c *PushResponseLossCoordinator) signalPushCommitted() {
	c.pushCommittedOnce.Do(func() { close(c.pushCommitted) })
}
func (c *PushResponseLossCoordinator) releaseInitialResponse() {
	c.allowInitialResponseOnce.Do(func() { close(c.allowInitialResponse) })
}
func (c *PushResponseLossCoordinator) signalReplayCompleted() {
	c.replayCompletedOnce.Do(func() { close(c.replayCompleted) })
}
func (c *PushResponseLossCoordinator) waitForPushCommit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.pushCommitted:
	}
	return c.proxyFailure("push commit")
}
func (c *PushResponseLossCoordinator) waitForReplay(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.replayCompleted:
	}
	return c.proxyFailure("replay")
}
