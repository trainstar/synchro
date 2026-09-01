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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	retentionReconnectScenarioPath = "conformance/scenarios/server/retention-reconnect-001.json"
	retentionReconnectScenarioID   = "SCN-RETENTION-RECONNECT-001"
)

var retentionReconnectStepOrder = []scenarios.StepID{
	"STEP-RETENTION-RECONNECT-LOCAL-WRITE-001",
	"STEP-RETENTION-RECONNECT-SEAL-OLD-BATCH-001",
	"STEP-RETENTION-RECONNECT-COMMIT-001",
	"STEP-RETENTION-RECONNECT-MATERIALIZE-001",
	"STEP-RETENTION-RECONNECT-REBUILD-PIN-001",
	"STEP-RETENTION-RECONNECT-EXPIRE-001",
	"STEP-RETENTION-RECONNECT-REJECT-OLD-001",
	"STEP-RETENTION-RECONNECT-RENEW-001",
	"STEP-RETENTION-RECONNECT-COMPACT-001",
}

// RetentionReconnectCoordinatorConfig configures one expired-generation React Native sidecar.
type RetentionReconnectCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// RetentionReconnectCoordinatorResult contains final server and identity evidence.
type RetentionReconnectCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type retentionReconnectClient struct {
	userID   string
	clientID string
}

type retentionReconnectPrimaryKey struct {
	authored string
	runtime  string
}

type retentionReconnectTraceEvidence struct {
	initialPush  transportObservation
	rejectedPush transportObservation
	renewed      transportObservation
	generation   uint64
	scopeSet     uint64
}

// RetentionReconnectCoordinator drives the authored retention-reconnect scenario through React Native.
type RetentionReconnectCoordinator struct {
	config RetentionReconnectCoordinatorConfig

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
	authTokens map[string]string
	main       retentionReconnectClient

	initialCallID string
	initialWire   scenarios.WireExpectation
	pinWire       scenarios.WireExpectation
	rejectionWire scenarios.WireExpectation
	renewalWire   scenarios.WireExpectation

	localIntent         retentionReconnectPrimaryKey
	sealedMutationCount int
	pinnedRebuildID     string

	proxyMu            sync.Mutex
	faultArmed         bool
	sealedRequest      json.RawMessage
	sealedBatchID      string
	sealedMutationIDs  []string
	sealedGeneration   uint64
	rejectedGeneration uint64
	renewedGeneration  uint64
	proxyErr           error
	sealedPush         chan struct{}
	sealedPushOnce     sync.Once
	rejectedPush       chan struct{}
	rejectedPushOnce   sync.Once
	renewedConnect     chan struct{}
	renewedConnectOnce sync.Once

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	stage     retentionReconnectStage
	nextSeq   uint64
	process   *actionProcessIdentity

	finalCapture  *finalCapture
	initialTrace  *traceSnapshot
	traceEvidence *retentionReconnectTraceEvidence
	result        RetentionReconnectCoordinatorResult
}

type retentionReconnectStage uint8

const (
	retentionReconnectStageOpen retentionReconnectStage = iota
	retentionReconnectStageBootstrapOpened
	retentionReconnectStageBootstrapSynchronized
	retentionReconnectStageStopped
	retentionReconnectStageLocalWritten
	retentionReconnectStageInitialBegun
	retentionReconnectStageInitialBackoff
	retentionReconnectStageInitialCaptured
	retentionReconnectStageRenewed
	retentionReconnectStageFinalCaptured
	retentionReconnectStageComplete
)

// LoadRetentionReconnectScenario loads the authored retention-reconnect contract.
func LoadRetentionReconnectScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, retentionReconnectScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native retention-reconnect scenario: %w", err)
	}
	if err := ValidateRetentionReconnectScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateRetentionReconnectScenario rejects changes to the closed RN contract.
func ValidateRetentionReconnectScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != retentionReconnectScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native retention-reconnect scenario contract is invalid")
	}
	if len(scenario.Steps) != len(retentionReconnectStepOrder) || len(scenario.NativeLifecycleBoundaries) != 0 {
		return errors.New("React Native retention-reconnect scenario structure is invalid")
	}

	type expectedBinding struct {
		operation string
		kind      string
		stage     string
		method    string
		terminal  bool
		call      string
	}
	expected := []expectedBinding{
		{"local/write", "local-write", "", "", false, ""},
		{"push/submit", "public-call", "synchronous", "start", true, "initial"},
		{"model/commit-source-transaction", "controller", "", "", false, ""},
		{"process/materialize-source-transaction", "controller", "", "", false, ""},
		{"rebuild/request-page", "controller", "", "", false, ""},
		{"model/expire-client-generation", "controller", "", "", false, ""},
		{"push/submit", "public-call", "observe", "", false, "renewal"},
		{"connect/send", "public-call", "await-call", "", true, "renewal"},
		{"model/compact-scope", "controller", "", "", false, ""},
	}
	allowedWires := map[scenarios.StepID]struct{}{
		retentionReconnectStepOrder[1]: {},
		retentionReconnectStepOrder[4]: {},
		retentionReconnectStepOrder[6]: {},
		retentionReconnectStepOrder[7]: {},
	}
	wired := make(map[scenarios.StepID]struct{}, len(scenario.WireExpectations))
	for _, wire := range scenario.WireExpectations {
		if _, allowed := allowedWires[wire.StepID]; !allowed {
			return fmt.Errorf("React Native retention-reconnect wire expectation %s is unexpected", wire.StepID)
		}
		if _, duplicate := wired[wire.StepID]; duplicate {
			return fmt.Errorf("React Native retention-reconnect wire expectation %s is duplicated", wire.StepID)
		}
		wired[wire.StepID] = struct{}{}
	}
	if len(wired) != len(allowedWires) {
		return errors.New("React Native retention-reconnect wire expectations are incomplete")
	}

	callIDs := make(map[string]scenarios.NativeCallID)
	var main retentionReconnectClient
	for index, step := range scenario.Steps {
		binding := step.NativeBinding
		wanted := expected[index]
		if step.ID != retentionReconnectStepOrder[index] || binding == nil ||
			scenarios.OperationKey(step.Operation) != wanted.operation || binding.Kind != wanted.kind ||
			binding.Stage != wanted.stage || binding.Method != wanted.method ||
			step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native retention-reconnect step %d binding is invalid", index+1)
		}
		if binding.Kind == "local-write" || binding.Kind == "public-call" {
			if binding.UserID == "" || binding.ClientID == "" {
				return fmt.Errorf("React Native retention-reconnect step %s native identity is incomplete", step.ID)
			}
			if main.clientID == "" {
				main = retentionReconnectClient{userID: binding.UserID, clientID: binding.ClientID}
			} else if main.userID != binding.UserID || main.clientID != binding.ClientID {
				return errors.New("React Native retention-reconnect native client identity changed")
			}
		}
		if binding.Kind != "public-call" {
			continue
		}
		wire, err := retentionReconnectWireExpectation(scenario, step.ID)
		if err != nil {
			return err
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("React Native retention-reconnect step %s call identity is absent", step.ID)
		}
		if prior, found := callIDs[wanted.call]; found && prior != *binding.CallID {
			return fmt.Errorf("React Native retention-reconnect call %q has inconsistent identities", wanted.call)
		}
		callIDs[wanted.call] = *binding.CallID
		if wanted.terminal && binding.Completion != retentionReconnectCompletion(wire) {
			return fmt.Errorf("React Native retention-reconnect step %s completion does not match its authored wire", step.ID)
		}
		if !wanted.terminal && binding.Completion != "" {
			return fmt.Errorf("React Native retention-reconnect step %s has a nonterminal completion", step.ID)
		}
	}
	if main.clientID == "" || len(callIDs) != 2 {
		return errors.New("React Native retention-reconnect public calls are incomplete")
	}
	if _, err := retentionReconnectPinClient(scenario); err != nil {
		return err
	}
	if err := validateRetentionReconnectAliases(scenario.NativeIdentityAliases, scenario.Steps); err != nil {
		return err
	}
	if err := validateRetentionReconnectAssertions(scenario); err != nil {
		return err
	}
	return validateRetentionReconnectProofs(scenario)
}

func validateRetentionReconnectAliases(aliases []scenarios.NativeIdentityAlias, steps []scenarios.Step) error {
	if len(aliases) == 0 {
		return errors.New("React Native retention-reconnect aliases are absent")
	}
	knownSteps := make(map[scenarios.StepID]struct{}, len(steps))
	for _, step := range steps {
		knownSteps[step.ID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if alias.Alias == "" || alias.Kind == "" || len(alias.StepIDs) == 0 {
			return errors.New("React Native retention-reconnect alias is incomplete")
		}
		if _, duplicate := seen[alias.Alias]; duplicate {
			return fmt.Errorf("React Native retention-reconnect alias %q is duplicated", alias.Alias)
		}
		seen[alias.Alias] = struct{}{}
		for _, stepID := range alias.StepIDs {
			if _, found := knownSteps[stepID]; !found {
				return fmt.Errorf("React Native retention-reconnect alias %q owns absent step %s", alias.Alias, stepID)
			}
		}
	}
	return nil
}

func validateRetentionReconnectAssertions(scenario scenarios.Scenario) error {
	wanted := map[string]bool{
		"ASSERT-RETENTION-RECONNECT-EXPIRY-SEMANTIC-001":     false,
		"ASSERT-RETENTION-RECONNECT-COMPACTION-SEMANTIC-001": false,
	}
	for _, assertion := range scenario.Assertions {
		id := string(assertion.ID)
		if _, found := wanted[id]; !found {
			continue
		}
		if assertion.Predicate.ContractPredicate != "wire-outcome" || assertion.Oracle.Kind != "wire-contract" ||
			assertion.Oracle.ExpectedSource != "authored-model" || assertion.Oracle.ObservedSource != "system-under-test" ||
			len(assertion.DetectsControlIDs) != 1 {
			return fmt.Errorf("React Native retention-reconnect assertion %s is invalid", assertion.ID)
		}
		wanted[id] = true
	}
	for id, found := range wanted {
		if !found {
			return fmt.Errorf("React Native retention-reconnect assertion %s is absent", id)
		}
	}
	return nil
}

func validateRetentionReconnectProofs(scenario scenarios.Scenario) error {
	wanted := map[string]struct {
		cell string
	}{
		"OBL-RETENTION-RECONNECT-RN-IOS-CURRENT-001":     {cell: "SUP-RN-IOS-CURRENT-001"},
		"OBL-RETENTION-RECONNECT-RN-ANDROID-CURRENT-001": {cell: "SUP-RN-ANDROID-CURRENT-001"},
	}
	counts := make(map[string]int, len(wanted))
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		expected, found := wanted[id]
		if !found {
			continue
		}
		target := "test-rn-e2e-ios"
		if strings.Contains(id, "ANDROID") {
			target = "test-rn-e2e-android"
		}
		if proofTargetMatches(obligation, "native-e2e", expected.cell, target, "", "") {
			counts[id]++
		}
	}
	for id := range wanted {
		if counts[id] != 1 {
			return fmt.Errorf("React Native retention-reconnect proof obligation %s count = %d, want 1", id, counts[id])
		}
	}
	return nil
}

func retentionReconnectWireExpectation(scenario scenarios.Scenario, stepID scenarios.StepID) (scenarios.WireExpectation, error) {
	var value scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == stepID {
			value = wire
			count++
		}
	}
	if count != 1 {
		return scenarios.WireExpectation{}, fmt.Errorf("React Native retention-reconnect wire expectation %s count = %d, want 1", stepID, count)
	}
	return value, nil
}

func retentionReconnectCompletion(wire scenarios.WireExpectation) string {
	if wire.Action == "unsupported" {
		return "error"
	}
	if wire.HTTPStatus >= http.StatusOK && wire.HTTPStatus < http.StatusMultipleChoices {
		return "idle"
	}
	if wire.Retryable || wire.HTTPStatus == 0 {
		return "blocked"
	}
	return "error"
}

func retentionReconnectMainClient(scenario scenarios.Scenario) (retentionReconnectClient, error) {
	for _, step := range scenario.Steps {
		if step.ID != retentionReconnectStepOrder[0] {
			continue
		}
		if step.NativeBinding == nil || step.NativeBinding.UserID == "" || step.NativeBinding.ClientID == "" {
			return retentionReconnectClient{}, errors.New("React Native retention-reconnect main client identity is absent")
		}
		return retentionReconnectClient{userID: step.NativeBinding.UserID, clientID: step.NativeBinding.ClientID}, nil
	}
	return retentionReconnectClient{}, errors.New("React Native retention-reconnect local write is absent")
}

func retentionReconnectPinClient(scenario scenarios.Scenario) (retentionReconnectClient, error) {
	for _, step := range scenario.Steps {
		if step.ID != retentionReconnectStepOrder[4] {
			continue
		}
		var payload struct {
			UserID   string `json:"user_id"`
			ClientID string `json:"client_id"`
		}
		if err := jsonstrict.Decode(step.Operation.Payload, &payload); err != nil || payload.UserID == "" || payload.ClientID == "" {
			return retentionReconnectClient{}, errors.New("React Native retention-reconnect pin client identity is invalid")
		}
		return retentionReconnectClient{userID: payload.UserID, clientID: payload.ClientID}, nil
	}
	return retentionReconnectClient{}, errors.New("React Native retention-reconnect rebuild pin is absent")
}

func retentionReconnectAuthoredMutationCount(operation scenarios.Operation) (int, error) {
	var payload struct {
		Request struct {
			Mutations []struct {
				MutationID string `json:"mutation_id"`
			} `json:"mutations"`
		} `json:"request"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || len(payload.Request.Mutations) == 0 {
		return 0, errors.New("React Native retention-reconnect sealed push mutations are invalid")
	}
	ids := make(map[string]struct{}, len(payload.Request.Mutations))
	for _, mutation := range payload.Request.Mutations {
		if mutation.MutationID == "" {
			return 0, errors.New("React Native retention-reconnect sealed push mutation identity is absent")
		}
		if _, duplicate := ids[mutation.MutationID]; duplicate {
			return 0, errors.New("React Native retention-reconnect sealed push mutation identity is duplicated")
		}
		ids[mutation.MutationID] = struct{}{}
	}
	return len(ids), nil
}

func retentionReconnectPinnedRebuildID(operation scenarios.Operation) (string, error) {
	var payload struct {
		RebuildID string `json:"rebuild_id"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.RebuildID == "" {
		return "", errors.New("React Native retention-reconnect rebuild identity is absent")
	}
	return payload.RebuildID, nil
}

func retentionReconnectStepOperation(steps map[scenarios.StepID]scenarios.Step, stepID scenarios.StepID, key string) (scenarios.Operation, error) {
	step, found := steps[stepID]
	if !found || scenarios.OperationKey(step.Operation) != key {
		return scenarios.Operation{}, fmt.Errorf("React Native retention-reconnect step %s operation is invalid", stepID)
	}
	return step.Operation, nil
}

// NewRetentionReconnectCoordinator creates an authenticated loopback proxy and command sidecar.
func NewRetentionReconnectCoordinator(config RetentionReconnectCoordinatorConfig) (*RetentionReconnectCoordinator, error) {
	if err := ValidateRetentionReconnectScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native retention-reconnect coordinator platform must be ios or android")
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native retention-reconnect coordinator auth token is required")
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
		return nil, fmt.Errorf("resolve React Native retention-reconnect upstream URL: %w", err)
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, fmt.Errorf("create React Native retention-reconnect coordinator capability: %w", err)
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-retention-reconnect-")
		if err != nil {
			return nil, fmt.Errorf("create React Native retention-reconnect database name: %w", err)
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native retention-reconnect database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for React Native retention-reconnect coordinator: %w", err)
	}
	closeListener := func(errorValue error) (*RetentionReconnectCoordinator, error) {
		_ = listener.Close()
		return nil, errorValue
	}
	adapter, err := nativeAdapterURL("http://"+listener.Addr().String(), config.Platform)
	if err != nil {
		return closeListener(fmt.Errorf("resolve React Native retention-reconnect proxy URL: %w", err))
	}
	main, err := retentionReconnectMainClient(config.Scenario)
	if err != nil {
		return closeListener(err)
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	sealedPush, err := retentionReconnectStepOperation(steps, retentionReconnectStepOrder[1], "push/submit")
	if err != nil {
		return closeListener(err)
	}
	sealedMutationCount, err := retentionReconnectAuthoredMutationCount(sealedPush)
	if err != nil {
		return closeListener(err)
	}
	pinnedRebuildID, err := retentionReconnectPinnedRebuildID(steps[retentionReconnectStepOrder[4]].Operation)
	if err != nil {
		return closeListener(err)
	}
	initialWire, err := retentionReconnectWireExpectation(config.Scenario, retentionReconnectStepOrder[1])
	if err != nil {
		return closeListener(err)
	}
	pinWire, err := retentionReconnectWireExpectation(config.Scenario, retentionReconnectStepOrder[4])
	if err != nil {
		return closeListener(err)
	}
	rejectionWire, err := retentionReconnectWireExpectation(config.Scenario, retentionReconnectStepOrder[6])
	if err != nil {
		return closeListener(err)
	}
	renewalWire, err := retentionReconnectWireExpectation(config.Scenario, retentionReconnectStepOrder[7])
	if err != nil {
		return closeListener(err)
	}
	initialBinding := steps[retentionReconnectStepOrder[1]].NativeBinding
	renewalBinding := steps[retentionReconnectStepOrder[7]].NativeBinding
	if initialBinding == nil || initialBinding.CallID == nil || renewalBinding == nil || renewalBinding.CallID == nil {
		return closeListener(errors.New("React Native retention-reconnect call bindings are absent"))
	}
	coordinator := &RetentionReconnectCoordinator{
		config: config, listener: listener, token: token, adapter: adapter, upstream: upstream, database: database,
		transport: &http.Client{Timeout: 2 * time.Minute}, steps: steps,
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), authTokens: make(map[string]string), main: main,
		initialCallID: string(*initialBinding.CallID),
		initialWire:   initialWire, pinWire: pinWire, rejectionWire: rejectionWire, renewalWire: renewalWire,
		sealedMutationCount: sealedMutationCount, pinnedRebuildID: pinnedRebuildID,
		faultArmed: true, sealedPush: make(chan struct{}), rejectedPush: make(chan struct{}), renewedConnect: make(chan struct{}),
		nextSeq: 1,
	}
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

// Prepare installs the authored server state and maps the local application write.
func (c *RetentionReconnectCoordinator) Prepare(ctx context.Context) error {
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
		return fmt.Errorf("prepare React Native retention-reconnect coordinator: %w", err)
	}
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native retention-reconnect coordinator dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native retention-reconnect contract: %w", err)
	}
	local := c.steps[retentionReconnectStepOrder[0]].Operation
	bound, err := c.config.Controller.ApplicationWrite(local)
	if err != nil {
		return fmt.Errorf("bind React Native retention-reconnect local write: %w", err)
	}
	intent, err := retentionReconnectWrittenPrimaryKey(local, bound)
	if err != nil {
		return err
	}
	step := c.steps[retentionReconnectStepOrder[0]]
	step.Operation = bound
	c.steps[retentionReconnectStepOrder[0]] = step
	if c.config.AuthToken != "" {
		c.authTokens[c.main.clientID] = c.config.AuthToken
	} else {
		token, tokenErr := c.config.Harness.NativeBearerToken(ctx, c.main.userID, time.Now())
		if tokenErr != nil {
			return fmt.Errorf("mint React Native retention-reconnect bearer token: %w", tokenErr)
		}
		c.authTokens[c.main.clientID] = token
	}
	if err := c.bindInitialServerIdentities(); err != nil {
		return err
	}
	c.mu.Lock()
	c.localIntent = intent
	c.prepared = true
	c.mu.Unlock()
	return nil
}

func (c *RetentionReconnectCoordinator) bindInitialServerIdentities() error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native retention-reconnect initial server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
	}
	for _, alias := range aliases {
		if len(c.runtimeIDs[alias.Alias]) == 0 {
			return fmt.Errorf("React Native retention-reconnect initial alias %q is unavailable", alias.Alias)
		}
	}
	return nil
}

// Serve runs the sidecar until it closes.
func (c *RetentionReconnectCoordinator) Serve(ctx context.Context) error {
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
			closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(closeContext)
			cancel()
		case <-done:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve React Native retention-reconnect coordinator: %w", err)
}

func (c *RetentionReconnectCoordinator) Handler() http.Handler { return c }

func (c *RetentionReconnectCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *RetentionReconnectCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// ExchangeCount returns all coordinator commands and the terminal exchange.
func (c *RetentionReconnectCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	return int(retentionReconnectStageComplete)
}

func (c *RetentionReconnectCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *RetentionReconnectCoordinator) Result() (RetentionReconnectCoordinatorResult, error) {
	if c == nil {
		return RetentionReconnectCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return RetentionReconnectCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return RetentionReconnectCoordinatorResult{}, errors.New("React Native retention-reconnect coordinator has not completed")
	}
	return c.result, nil
}

func (c *RetentionReconnectCoordinator) Close(ctx context.Context) error {
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
	c.releaseFault()
	shutdownErr := c.server.Shutdown(ctx)
	listenerErr := c.listener.Close()
	if shutdownErr != nil {
		return fmt.Errorf("shut down React Native retention-reconnect coordinator: %w", shutdownErr)
	}
	if listenerErr != nil && !errors.Is(listenerErr, net.ErrClosed) {
		return fmt.Errorf("close React Native retention-reconnect listener: %w", listenerErr)
	}
	return nil
}

func (c *RetentionReconnectCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
		c.failed = fmt.Errorf("React Native retention-reconnect exchange unavailable: closed=%t prepared=%t failed=%v completed=%t sequence=%d want=%d", c.closed, c.prepared, c.failed, c.completed, exchange.Sequence, c.nextSeq)
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native retention-reconnect exchange %d stage %d result failed: %w", exchange.Sequence, c.stage, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(request.Context(), exchange.Sequence)
	if err != nil {
		c.failed = fmt.Errorf("React Native retention-reconnect exchange %d stage %d advance failed: %w", exchange.Sequence, c.stage, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = fmt.Errorf("React Native retention-reconnect response encoding: bytes=%d maximum=%d error=%v", len(encoded), maximumExchangeBytes, err)
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *RetentionReconnectCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == retentionReconnectStageOpen {
		if !isJSONNull(raw) {
			return errors.New("React Native retention-reconnect opening result is not null")
		}
		return nil
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return errInvalidExchange
	}
	switch c.stage {
	case retentionReconnectStageBootstrapOpened:
		process, processErr := validateOpenedResult(envelope.Result)
		if processErr != nil {
			return processErr
		}
		c.process = &process
	case retentionReconnectStageBootstrapSynchronized:
		return c.validateSynchronized(envelope.Result, "idle")
	case retentionReconnectStageStopped:
		if c.process == nil {
			return errors.New("React Native retention-reconnect process identity is unavailable")
		}
		return validateStoppedLifecycleResult(envelope.Result, *c.process)
	case retentionReconnectStageLocalWritten:
		return c.validateLocal(envelope.Result)
	case retentionReconnectStageInitialBegun:
		return c.validateCallBegun(envelope.Result)
	case retentionReconnectStageInitialBackoff:
		return c.validateInitialBackoff(envelope.Result)
	case retentionReconnectStageInitialCaptured:
		capture, captureErr := decodeCapture(envelope.Result, retentionReconnectCaptureResultKeys())
		if captureErr != nil {
			return captureErr
		}
		if captureErr = c.validateInitialCapture(capture); captureErr != nil {
			return captureErr
		}
	case retentionReconnectStageRenewed:
		return c.validateRenewedCall(envelope.Result)
	case retentionReconnectStageFinalCaptured:
		capture, captureErr := decodeCapture(envelope.Result, retentionReconnectCaptureResultKeys())
		if captureErr != nil {
			return captureErr
		}
		if captureErr = c.validateFinalCapture(capture); captureErr != nil {
			return captureErr
		}
		c.finalCapture = &capture
	default:
		return fmt.Errorf("React Native retention-reconnect accepted result at stage %d", c.stage)
	}
	return nil
}

func retentionReconnectCaptureSources() []string {
	return []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "request-trace"}
}

// retentionReconnectCaptureResultKeys names the response members produced for
// retentionReconnectCaptureSources. The runner input and response schemas use
// different names, so one list cannot serve both boundaries.
func retentionReconnectCaptureResultKeys() []string {
	return []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "request_trace"}
}

func (c *RetentionReconnectCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case retentionReconnectStageOpen:
		response.Command = c.command("client", "open", map[string]any{
			"client_key": c.main.clientID, "database_mode": "create", "initialization": "empty", "seed_step_id": nil,
		}, nil)
		c.stage = retentionReconnectStageBootstrapOpened
	case retentionReconnectStageBootstrapOpened:
		response.Command = c.command("client", "synchronize-step", map[string]any{
			"client_key": c.main.clientID, "method": "start", "completion": "idle",
		}, nil)
		c.stage = retentionReconnectStageBootstrapSynchronized
	case retentionReconnectStageBootstrapSynchronized:
		response.Command = c.command("client", "lifecycle", map[string]any{
			"client_key": c.main.clientID, "operation": "stop",
		}, nil)
		c.stage = retentionReconnectStageStopped
	case retentionReconnectStageStopped:
		response.Command = c.command("client", "execute-step", map[string]any{
			"client_key": c.main.clientID,
		}, []scenarios.StepID{retentionReconnectStepOrder[0]})
		c.stage = retentionReconnectStageLocalWritten
	case retentionReconnectStageLocalWritten:
		binding := c.steps[retentionReconnectStepOrder[1]].NativeBinding
		response.Command = c.command("client", "begin-call", map[string]any{
			"client_key": c.main.clientID, "call_id": c.initialCallID, "method": binding.Method,
		}, []scenarios.StepID{retentionReconnectStepOrder[1]})
		c.stage = retentionReconnectStageInitialBegun
	case retentionReconnectStageInitialBegun:
		if err := c.waitForSealedPush(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "await-step", map[string]any{
			"client_key": c.main.clientID, "call_id": c.initialCallID,
		}, []scenarios.StepID{retentionReconnectStepOrder[1]})
		c.stage = retentionReconnectStageInitialBackoff
	case retentionReconnectStageInitialBackoff:
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{c.main.clientID}, "sources": retentionReconnectCaptureSources(),
		}, nil)
		c.stage = retentionReconnectStageInitialCaptured
	case retentionReconnectStageInitialCaptured:
		if err := c.prepareRenewal(ctx); err != nil {
			return exchangeResponse{}, err
		}
		// The managed start retries its sealed durable batch without another
		// public call. Wait for that recovery before returning await-call. The
		// runner reports backoff as terminal, so returning earlier would make
		// the automatic renewal appear as another blocked completion.
		if err := c.waitForRenewedConnect(ctx); err != nil {
			return exchangeResponse{}, err
		}
		// The managed start remains registered after await-step observes the
		// retryable fault. Awaiting that same task lets the client perform its
		// automatic expired-generation recovery without creating a second path.
		response.Command = c.command("client", "await-call", map[string]any{
			"client_key": c.main.clientID, "call_id": c.initialCallID,
		}, []scenarios.StepID{retentionReconnectStepOrder[6], retentionReconnectStepOrder[7]})
		c.stage = retentionReconnectStageRenewed
	case retentionReconnectStageRenewed:
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{c.main.clientID}, "sources": retentionReconnectCaptureSources(),
		}, nil)
		c.stage = retentionReconnectStageFinalCaptured
	case retentionReconnectStageFinalCaptured:
		if err := c.finishLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.stage = retentionReconnectStageComplete
		c.completed = true
	default:
		return exchangeResponse{}, fmt.Errorf("React Native retention-reconnect advance stage %d", c.stage)
	}
	return response, nil
}

func (c *RetentionReconnectCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, stepID := range stepIDs {
		step := c.steps[stepID]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{
			ContractOperation: step.Operation.ContractOperation,
			Name:              step.Operation.Name,
			Payload:           copyRaw(step.Operation.Payload),
		}})
	}
	return &conformanceCommand{
		SchemaVersion: 1,
		Action:        conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps},
		Runtime: conformanceRuntime{
			ClientKey: c.main.clientID, Database: c.database, ClientID: c.main.clientID,
			ServerURL: c.adapter, AuthToken: c.authTokens[c.main.clientID],
		},
	}
}

func (c *RetentionReconnectCoordinator) validateSynchronized(raw json.RawMessage, completion string) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "retention-reconnect synchronized result"); err != nil {
		return err
	}
	var actual string
	if err := json.Unmarshal(members["completion"], &actual); err != nil || actual != completion {
		return errors.New("React Native retention-reconnect synchronized completion is invalid")
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return err
	}
	return c.validateProcess(members["process"], "synchronized")
}

func (c *RetentionReconnectCoordinator) validateLocal(raw json.RawMessage) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "retention-reconnect local result"); err != nil {
		return err
	}
	var rows uint64
	if err := json.Unmarshal(members["rows_affected"], &rows); err != nil || rows != 1 {
		return errors.New("React Native retention-reconnect local write did not affect one row")
	}
	return c.validateProcess(members["process"], "local write")
}

func (c *RetentionReconnectCoordinator) validateCallBegun(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-begun"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "retention-reconnect call-begun result"); err != nil {
		return err
	}
	var callID, state string
	if err := json.Unmarshal(members["call_id"], &callID); err != nil || callID != c.initialCallID {
		return errors.New("React Native retention-reconnect initial call identity is invalid")
	}
	if err := json.Unmarshal(members["state"], &state); err != nil || state != "in_flight" {
		return errors.New("React Native retention-reconnect initial call did not enter flight")
	}
	return c.validateProcess(members["process"], "initial call")
}

func (c *RetentionReconnectCoordinator) validateInitialBackoff(raw json.RawMessage) error {
	if retentionReconnectCompletion(c.initialWire) != "blocked" {
		return errors.New("React Native retention-reconnect initial wire does not derive a blocked completion")
	}
	if err := validateActionResult(raw, "awaited"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "retention-reconnect initial backoff result"); err != nil {
		return err
	}
	var status syncStatus
	var statusMembers map[string]json.RawMessage
	if err := jsonstrict.Decode(members["status"], &statusMembers); err != nil || len(statusMembers) != 4 ||
		jsonstrict.Decode(members["status"], &status) != nil || status.State != "backoff" || isJSONNull(status.RetryAt) {
		return errors.New("React Native retention-reconnect initial call did not retain retryable backoff")
	}
	return c.validateProcess(members["process"], "initial backoff")
}

func (c *RetentionReconnectCoordinator) validateRenewedCall(raw json.RawMessage) error {
	wantCompletion := retentionReconnectCompletion(c.renewalWire)
	if err := validateActionResult(raw, "call-completed"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 6, "retention-reconnect renewed call result"); err != nil {
		return err
	}
	var callID, state, completion string
	if err := json.Unmarshal(members["call_id"], &callID); err != nil || callID != c.initialCallID {
		return errors.New("React Native retention-reconnect recovery call identity is invalid")
	}
	if err := json.Unmarshal(members["state"], &state); err != nil || state != "completed" {
		return errors.New("React Native retention-reconnect recovery call state is invalid")
	}
	if err := json.Unmarshal(members["completion"], &completion); err != nil || completion != wantCompletion {
		return fmt.Errorf("React Native retention-reconnect recovery completion = %q, want %q", completion, wantCompletion)
	}
	if wantCompletion != "idle" || validateReadyStatus(members["status"]) != nil {
		return errors.New("React Native retention-reconnect recovery did not settle ready")
	}
	return c.validateProcess(members["process"], "renewed call")
}

func (c *RetentionReconnectCoordinator) validateProcess(raw json.RawMessage, stage string) error {
	actual, err := decodeActionProcessIdentity(raw)
	if err != nil || c.process == nil || actual != *c.process {
		return fmt.Errorf("React Native retention-reconnect %s process identity changed", stage)
	}
	return nil
}

func (c *RetentionReconnectCoordinator) validateInitialCapture(capture finalCapture) error {
	if err := c.validateQueue(capture); err != nil {
		return fmt.Errorf("validate React Native retention-reconnect sealed queue: %w", err)
	}
	var status syncStatus
	if err := jsonstrict.Decode(capture.Status, &status); err != nil || status.State != "backoff" {
		return errors.New("React Native retention-reconnect sealed queue did not remain in backoff")
	}
	trace, err := retentionReconnectTrace(capture.Trace)
	if err != nil {
		return err
	}
	initial, err := c.validateInitialTrace(trace)
	if err != nil {
		return err
	}
	c.initialTrace = &trace
	if initial.Sequence == 0 {
		return errors.New("React Native retention-reconnect initial push trace is absent")
	}
	return nil
}

func (c *RetentionReconnectCoordinator) validateFinalCapture(capture finalCapture) error {
	if c.initialTrace == nil {
		return errors.New("React Native retention-reconnect initial trace is unavailable")
	}
	if err := c.validateQueue(capture); err != nil {
		return fmt.Errorf("validate React Native retention-reconnect renewed queue: %w", err)
	}
	if err := validateReadyStatus(capture.Status); err != nil {
		return fmt.Errorf("React Native retention-reconnect final status is invalid: %w", err)
	}
	trace, err := retentionReconnectTrace(capture.Trace)
	if err != nil {
		return err
	}
	evidence, err := c.validateRecoveryTrace(*c.initialTrace, trace)
	if err != nil {
		return err
	}
	c.traceEvidence = &evidence
	return nil
}

func retentionReconnectTrace(raw json.RawMessage) (traceSnapshot, error) {
	trace, err := captureTraceFromRaw(raw)
	if err != nil {
		return traceSnapshot{}, err
	}
	if trace.Overflowed || trace.SequenceCheckpoint != uint64(len(trace.Observations)) || validateTraceSequence(trace.Observations) != nil {
		return traceSnapshot{}, errors.New("React Native retention-reconnect request trace is invalid")
	}
	return trace, nil
}

func (c *RetentionReconnectCoordinator) validateInitialTrace(trace traceSnapshot) (transportObservation, error) {
	pushes := make([]transportObservation, 0, 1)
	for _, observed := range trace.Observations {
		if observed.OperationClass == "push" && observed.StatusCode == c.initialWire.HTTPStatus {
			pushes = append(pushes, observed)
		}
	}
	if len(pushes) != 1 {
		return transportObservation{}, fmt.Errorf("React Native retention-reconnect initial push count = %d, want 1", len(pushes))
	}
	if err := c.validatePushTrace(pushes[0], c.initialWire); err != nil {
		return transportObservation{}, err
	}
	generation, err := requestInteger(pushes[0], "client_generation")
	if err != nil || generation == 0 {
		return transportObservation{}, errors.New("React Native retention-reconnect initial generation evidence is invalid")
	}
	sealedGeneration, _, _, err := c.sealedIdentity()
	if err != nil || generation != sealedGeneration {
		return transportObservation{}, errors.New("React Native retention-reconnect initial generation differs from sealed request")
	}
	return pushes[0], nil
}

func (c *RetentionReconnectCoordinator) validateRecoveryTrace(initial, final traceSnapshot) (retentionReconnectTraceEvidence, error) {
	if len(final.Observations) < len(initial.Observations) {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect final trace lost initial observations")
	}
	for index, observed := range initial.Observations {
		if !transportObservationsEqual(observed, final.Observations[index]) {
			return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect initial trace changed")
		}
	}
	initialPush, err := c.validateInitialTrace(initial)
	if err != nil {
		return retentionReconnectTraceEvidence{}, err
	}
	var rejected *transportObservation
	var renewed *transportObservation
	for index := len(initial.Observations); index < len(final.Observations); index++ {
		observed := final.Observations[index]
		if rejected == nil && observed.OperationClass == "push" && observed.StatusCode == c.rejectionWire.HTTPStatus {
			if err := c.validatePushTrace(observed, c.rejectionWire); err != nil {
				return retentionReconnectTraceEvidence{}, err
			}
			copy := observed
			rejected = &copy
			continue
		}
		if rejected != nil && observed.OperationClass == "connect" && observed.StatusCode == c.renewalWire.HTTPStatus {
			if err := c.validateConnectTrace(observed, c.renewalWire); err != nil {
				return retentionReconnectTraceEvidence{}, err
			}
			copy := observed
			renewed = &copy
			break
		}
	}
	if rejected == nil || renewed == nil {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect recovery trace is incomplete")
	}
	generation, err := requestInteger(initialPush, "client_generation")
	if err != nil || generation == 0 {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect initial generation is invalid")
	}
	rejectedGeneration, err := requestInteger(*rejected, "client_generation")
	if err != nil || rejectedGeneration != generation {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect rejected push generation changed")
	}
	renewalGeneration, err := requestInteger(*renewed, "client_generation")
	if err != nil || renewalGeneration != generation {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect renewal generation changed")
	}
	scopeSet, err := requestInteger(*renewed, "scope_set_version")
	if err != nil {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect scope-set evidence is absent")
	}
	sealedGeneration, rejectedProxyGeneration, renewedProxyGeneration, proxyErr := c.sealedIdentity()
	if proxyErr != nil || sealedGeneration != generation || rejectedProxyGeneration != generation || renewedProxyGeneration <= generation {
		return retentionReconnectTraceEvidence{}, errors.New("React Native retention-reconnect proxy generation evidence is invalid")
	}
	return retentionReconnectTraceEvidence{
		initialPush: initialPush, rejectedPush: *rejected, renewed: *renewed, generation: generation, scopeSet: scopeSet,
	}, nil
}

func (c *RetentionReconnectCoordinator) validatePushTrace(observed transportObservation, wire scenarios.WireExpectation) error {
	if observed.OperationClass != "push" || observed.StatusCode != wire.HTTPStatus || observed.DurationNanoseconds == 0 || !hasJSONValue(observed.RequestFacts) {
		return errors.New("React Native retention-reconnect push trace differs from its authored wire")
	}
	count, err := requestInteger(observed, "mutation_count")
	if err != nil || count != uint64(c.sealedMutationCount) {
		return errors.New("React Native retention-reconnect push trace mutation count changed")
	}
	return nil
}

func (c *RetentionReconnectCoordinator) validateConnectTrace(observed transportObservation, wire scenarios.WireExpectation) error {
	if observed.OperationClass != "connect" || observed.StatusCode != wire.HTTPStatus || observed.DurationNanoseconds == 0 || !hasJSONValue(observed.RequestFacts) {
		return errors.New("React Native retention-reconnect connect trace differs from its authored wire")
	}
	if _, err := requestInteger(observed, "scope_set_version"); err != nil {
		return errors.New("React Native retention-reconnect renewal scope-set trace is invalid")
	}
	return nil
}

func (c *RetentionReconnectCoordinator) validateQueue(capture finalCapture) error {
	sealedGeneration, _, _, err := c.sealedIdentity()
	if err != nil || sealedGeneration == 0 {
		return errors.New("React Native retention-reconnect sealed identity is unavailable")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	var pending []struct {
		MutationID string `json:"mutationID"`
		Status     string `json:"status"`
	}
	if err := decodeStrictValue(capture.Pending, &pending); err != nil || pending == nil {
		return errors.New("React Native retention-reconnect pending queue is invalid")
	}
	if err := validateEmptyArray(capture.Rejected); err != nil {
		return errors.New("React Native retention-reconnect rejected queue is not empty")
	}
	_, mutationIDs, _, _, _, identityErr := c.proxyIdentity()
	if identityErr != nil || state.MutationLedgerCount != uint64(len(mutationIDs)) || len(pending) != len(mutationIDs) {
		return errors.New("React Native retention-reconnect durable queue count differs from sealed intent")
	}
	wanted := make(map[string]struct{}, len(mutationIDs))
	for _, mutationID := range mutationIDs {
		if mutationID == "" {
			return errors.New("React Native retention-reconnect sealed mutation identity is absent")
		}
		if _, duplicate := wanted[mutationID]; duplicate {
			return errors.New("React Native retention-reconnect sealed mutation identity is duplicated")
		}
		wanted[mutationID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(pending))
	for _, mutation := range pending {
		if mutation.MutationID == "" || mutation.Status == "" {
			return errors.New("React Native retention-reconnect durable queue record is incomplete")
		}
		if _, found := wanted[mutation.MutationID]; !found {
			return errors.New("React Native retention-reconnect durable queue changed the sealed mutation identity")
		}
		if _, duplicate := seen[mutation.MutationID]; duplicate {
			return errors.New("React Native retention-reconnect durable queue repeats a sealed mutation")
		}
		seen[mutation.MutationID] = struct{}{}
	}
	return nil
}

func (c *RetentionReconnectCoordinator) prepareRenewal(ctx context.Context) error {
	if c.config.Controller == nil {
		return errors.New("React Native retention-reconnect controller is unavailable")
	}
	commit, err := retentionReconnectStepOperation(c.steps, retentionReconnectStepOrder[2], "model/commit-source-transaction")
	if err != nil {
		return err
	}
	if observed, applyErr := c.config.Controller.ApplyStep(ctx, commit); applyErr != nil || observed.Disposition != "success" {
		return fmt.Errorf("commit React Native retention-reconnect history: %w", nativeResultError(applyErr, observed.Disposition))
	}
	materialize, err := retentionReconnectStepOperation(c.steps, retentionReconnectStepOrder[3], "process/materialize-source-transaction")
	if err != nil {
		return err
	}
	if observed, processErr := c.config.Controller.ProcessStep(ctx, nil, materialize); processErr != nil || observed.Disposition != "success" {
		return fmt.Errorf("materialize React Native retention-reconnect history: %w", nativeResultError(processErr, observed.Disposition))
	}
	pin, err := retentionReconnectStepOperation(c.steps, retentionReconnectStepOrder[4], "rebuild/request-page")
	if err != nil {
		return err
	}
	if err := registerRetentionReconnectPinClient(ctx, c.config.Controller, pin); err != nil {
		return err
	}
	observed, requestErr := c.config.Controller.RequestStep(ctx, pin)
	if requestErr != nil || observed.Disposition != "success" {
		return fmt.Errorf("create React Native retention-reconnect rebuild pin: %w", nativeResultError(requestErr, observed.Disposition))
	}
	if err := validateRetentionReconnectNativeWire(c.pinWire, observed); err != nil {
		return err
	}
	server, err := c.captureServer(ctx)
	if err != nil {
		return err
	}
	if err := validateRetentionReconnectCompaction(server, pin); err != nil {
		return fmt.Errorf("validate React Native retention-reconnect active rebuild pin: %w", err)
	}
	expire, err := retentionReconnectStepOperation(c.steps, retentionReconnectStepOrder[5], "model/expire-client-generation")
	if err != nil {
		return err
	}
	if observed, applyErr := c.config.Controller.ApplyStep(ctx, expire); applyErr != nil || observed.Disposition != "success" {
		return fmt.Errorf("expire React Native retention-reconnect generation: %w", nativeResultError(applyErr, observed.Disposition))
	}
	c.releaseFault()
	return nil
}

func registerRetentionReconnectPinClient(ctx context.Context, controller *blackbox.NativeController, rebuild scenarios.Operation) error {
	var pin struct {
		UserID   string          `json:"user_id"`
		ClientID string          `json:"client_id"`
		Schema   json.RawMessage `json:"schema"`
	}
	if err := jsonstrict.Decode(rebuild.Payload, &pin); err != nil || pin.UserID == "" || pin.ClientID == "" || len(pin.Schema) == 0 {
		return errors.New("React Native retention-reconnect pin client identity is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"user_id":           pin.UserID,
		"client_id":         pin.ClientID,
		"runtime_version":   3,
		"protocol_version":  3,
		"schema_reset":      false,
		"schema":            pin.Schema,
		"scope_set_version": 0,
		"known_scopes":      []any{},
	})
	if err != nil {
		return errors.New("encode React Native retention-reconnect pin client connect failed")
	}
	connect := scenarios.Operation{ContractOperation: "connect", Name: "send", Payload: payload}
	observed, requestErr := controller.RequestStep(ctx, connect)
	if requestErr != nil || observed.Disposition != "success" {
		return fmt.Errorf("register React Native retention-reconnect pin client: %w", nativeResultError(requestErr, observed.Disposition))
	}
	return nil
}

func validateRetentionReconnectNativeWire(wire scenarios.WireExpectation, observed blackbox.NativeStepObservation) error {
	if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != wire.HTTPStatus ||
		observed.Wire.Retryable != wire.Retryable || !retentionReconnectOptionalStringEqual(observed.Wire.ErrorCode, wire.ErrorCode) {
		return errors.New("React Native retention-reconnect controller wire differs from its authored expectation")
	}
	return nil
}

func retentionReconnectOptionalStringEqual(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func (c *RetentionReconnectCoordinator) captureServer(ctx context.Context) (scenarios.StateFacts, error) {
	captures, err := c.config.Controller.Capture(ctx, []string{c.main.clientID}, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("capture React Native retention-reconnect server state: captures=%d error=%v", len(captures), err)
	}
	return captures[0].StateFacts, nil
}

func (c *RetentionReconnectCoordinator) finishLocked(ctx context.Context) error {
	if c.finalCapture == nil || c.traceEvidence == nil {
		return errors.New("React Native retention-reconnect final evidence is unavailable")
	}
	compact, err := retentionReconnectStepOperation(c.steps, retentionReconnectStepOrder[8], "model/compact-scope")
	if err != nil {
		return err
	}
	if observed, applyErr := c.config.Controller.ApplyStep(ctx, compact); applyErr != nil || observed.Disposition != "success" {
		return fmt.Errorf("compact React Native retention-reconnect scope: %w", nativeResultError(applyErr, observed.Disposition))
	}
	server, err := c.captureServer(ctx)
	if err != nil {
		return err
	}
	pin, err := retentionReconnectStepOperation(c.steps, retentionReconnectStepOrder[4], "rebuild/request-page")
	if err != nil {
		return err
	}
	if err := validateRetentionReconnectCompaction(server, pin); err != nil {
		return fmt.Errorf("validate React Native retention-reconnect compaction: %w", err)
	}
	identities, err := c.resolveIdentities(server)
	if err != nil {
		return err
	}
	c.result = RetentionReconnectCoordinatorResult{ServerFacts: server, IdentityResolution: identities}
	return nil
}

func validateRetentionReconnectCompaction(server scenarios.StateFacts, rebuild scenarios.Operation) error {
	var payload struct {
		ScopeID   string `json:"scope_id"`
		RebuildID string `json:"rebuild_id"`
		Limit     uint64 `json:"limit"`
	}
	if err := jsonstrict.Decode(rebuild.Payload, &payload); err != nil || payload.ScopeID == "" || payload.RebuildID == "" || payload.Limit == 0 {
		return errors.New("React Native retention-reconnect rebuild pin payload is invalid")
	}
	matches := 0
	for _, value := range server.Rebuilds {
		if value.ScopeID != payload.ScopeID || value.RebuildID != payload.RebuildID {
			continue
		}
		if value.PageLimit != payload.Limit || !value.HasContinuation {
			return errors.New("React Native retention-reconnect rebuild pin does not retain its continuation")
		}
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("React Native retention-reconnect active rebuild pin count = %d, want 1", matches)
	}
	scopes := 0
	for _, scope := range server.Scopes {
		if scope.ScopeID == payload.ScopeID {
			scopes++
		}
	}
	if scopes != 1 {
		return errors.New("React Native retention-reconnect compacted scope is absent")
	}
	return nil
}

func retentionReconnectWrittenPrimaryKey(authored, bound scenarios.Operation) (retentionReconnectPrimaryKey, error) {
	authoredValue, err := retentionReconnectPrimaryKeyValue(authored)
	if err != nil {
		return retentionReconnectPrimaryKey{}, fmt.Errorf("React Native retention-reconnect authored primary key is invalid: %w", err)
	}
	runtimeValue, err := retentionReconnectPrimaryKeyValue(bound)
	if err != nil {
		return retentionReconnectPrimaryKey{}, fmt.Errorf("React Native retention-reconnect runtime primary key is invalid: %w", err)
	}
	return retentionReconnectPrimaryKey{authored: authoredValue, runtime: runtimeValue}, nil
}

func retentionReconnectPrimaryKeyValue(operation scenarios.Operation) (string, error) {
	var payload struct {
		PK map[string]json.RawMessage `json:"pk"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || len(payload.PK) == 0 {
		return "", errors.New("primary key is absent")
	}
	raw, found := payload.PK["value"]
	if !found {
		if len(payload.PK) != 1 {
			return "", errors.New("primary key is ambiguous")
		}
		for _, value := range payload.PK {
			raw = value
		}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", errors.New("primary key value is invalid")
	}
	return value, nil
}

func (c *RetentionReconnectCoordinator) resolveIdentities(server scenarios.StateFacts) ([]blackbox.NativeIdentityResolution, error) {
	if c.traceEvidence == nil {
		return nil, errors.New("React Native retention-reconnect trace identity evidence is unavailable")
	}
	serverAliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "mutation-id" || alias.Kind == "batch-id" {
			continue
		}
		if alias.Kind == "primary-key" && retentionReconnectAliasNames(alias, c.localIntent.authored) {
			continue
		}
		serverAliases = append(serverAliases, alias)
	}
	values, err := c.config.Controller.IdentityValues(serverAliases)
	if err != nil {
		return nil, fmt.Errorf("resolve React Native retention-reconnect server identities: %w", err)
	}
	runtime := make(map[string]json.RawMessage, len(c.identities))
	for alias, value := range c.runtimeIDs {
		runtime[alias] = copyRaw(value)
	}
	for _, value := range values {
		runtime[value.Alias] = copyRaw(value.RuntimeValue)
	}
	observed, err := c.observedIdentityValues(server)
	if err != nil {
		return nil, err
	}
	for alias, value := range observed {
		runtime[alias] = value
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := runtime[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("React Native retention-reconnect alias %q has no runtime evidence", alias.Alias)
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

func (c *RetentionReconnectCoordinator) observedIdentityValues(server scenarios.StateFacts) (map[string]json.RawMessage, error) {
	batchID, mutationIDs, _, _, _, err := c.proxyIdentity()
	if err != nil {
		return nil, err
	}
	runtime := make(map[string]json.RawMessage, len(c.identities))
	for _, alias := range c.identities {
		var value any
		switch alias.Kind {
		case "client-generation":
			value = c.traceEvidence.generation
		case "scope-set-version":
			value = c.traceEvidence.scopeSet
		case "row-version", "checksum":
			observed, observedErr := retentionReconnectObservedRowValue(alias, c.identities, server.Rows)
			if observedErr != nil {
				return nil, observedErr
			}
			value = observed
		case "rebuild-id":
			observed, observedErr := retentionReconnectObservedRebuildID(alias, c.identities, server.Rebuilds, c.pinnedRebuildID)
			if observedErr != nil {
				return nil, observedErr
			}
			value = observed
		case "mutation-id":
			if len(mutationIDs) != 1 {
				return nil, fmt.Errorf("React Native retention-reconnect sealed mutation count = %d, want 1", len(mutationIDs))
			}
			value = mutationIDs[0]
		case "batch-id":
			value = batchID
		case "primary-key":
			if !retentionReconnectAliasNames(alias, c.localIntent.authored) {
				continue
			}
			value = c.localIntent.runtime
		default:
			continue
		}
		encoded, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			return nil, fmt.Errorf("encode React Native retention-reconnect alias %q: %w", alias.Alias, encodeErr)
		}
		runtime[alias.Alias] = encoded
	}
	return runtime, nil
}

func retentionReconnectObservedRowValue(alias scenarios.NativeIdentityAlias, aliases []scenarios.NativeIdentityAlias, rows []scenarios.RowFact) (string, error) {
	values := make(map[string]struct{})
	for _, primary := range aliases {
		if primary.Kind != "primary-key" || !retentionReconnectAliasesShareOwner(alias, primary) {
			continue
		}
		var authored string
		if err := json.Unmarshal(primary.Value, &authored); err != nil || authored == "" {
			return "", fmt.Errorf("React Native retention-reconnect primary-key evidence for %q is invalid", alias.Alias)
		}
		encoded, err := json.Marshal(authored)
		if err != nil {
			return "", err
		}
		for _, row := range rows {
			if row.CanonicalWireJSON != string(encoded) {
				continue
			}
			value := row.Version
			if alias.Kind == "checksum" {
				value = row.Checksum
			}
			if value == "" {
				return "", fmt.Errorf("React Native retention-reconnect %s evidence for %q is empty", alias.Kind, alias.Alias)
			}
			values[value] = struct{}{}
		}
	}
	if len(values) != 1 {
		return "", fmt.Errorf("React Native retention-reconnect %s evidence for %q is ambiguous", alias.Kind, alias.Alias)
	}
	for value := range values {
		return value, nil
	}
	return "", fmt.Errorf("React Native retention-reconnect %s evidence for %q is absent", alias.Kind, alias.Alias)
}

func retentionReconnectObservedRebuildID(alias scenarios.NativeIdentityAlias, aliases []scenarios.NativeIdentityAlias, rebuilds []scenarios.RebuildFact, pinnedRebuildID string) (string, error) {
	var scopeID string
	for _, scope := range aliases {
		if scope.Kind != "scope" || !retentionReconnectAliasesShareOwner(alias, scope) {
			continue
		}
		if err := json.Unmarshal(scope.Value, &scopeID); err != nil || scopeID == "" {
			return "", fmt.Errorf("React Native retention-reconnect rebuild scope evidence for %q is invalid", alias.Alias)
		}
		break
	}
	if scopeID == "" {
		return "", fmt.Errorf("React Native retention-reconnect rebuild scope evidence for %q is absent", alias.Alias)
	}
	matches := 0
	var result string
	for _, rebuild := range rebuilds {
		if rebuild.ScopeID != scopeID || rebuild.RebuildID != pinnedRebuildID {
			continue
		}
		if rebuild.RebuildID == "" {
			return "", fmt.Errorf("React Native retention-reconnect rebuild evidence for %q is invalid", alias.Alias)
		}
		matches++
		result = rebuild.RebuildID
	}
	if matches != 1 {
		return "", fmt.Errorf("React Native retention-reconnect rebuild evidence for %q is ambiguous", alias.Alias)
	}
	return result, nil
}

func retentionReconnectAliasesShareOwner(left, right scenarios.NativeIdentityAlias) bool {
	for _, leftOwner := range left.StepIDs {
		for _, rightOwner := range right.StepIDs {
			if leftOwner == rightOwner {
				return true
			}
		}
	}
	return false
}

func retentionReconnectAliasNames(alias scenarios.NativeIdentityAlias, authored string) bool {
	var value string
	return json.Unmarshal(alias.Value, &value) == nil && value != "" && value == authored
}

func (c *RetentionReconnectCoordinator) proxyAdapter(writer http.ResponseWriter, request *http.Request) {
	if c == nil || c.transport == nil || c.upstream == "" {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		c.recordProxyFailure(errors.New("React Native retention-reconnect proxy request is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	isPush := request.Method == http.MethodPost && request.URL.Path == "/sync/push"
	if isPush && c.temporaryFaultActive() {
		if err := c.observeSealedPush(body); err != nil {
			c.recordProxyFailure(err)
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
		response := faults.NewTemporaryUnavailableResponse(request)
		responseBody, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil || retentionReconnectValidateHTTPWire(response.StatusCode, responseBody, c.initialWire) != nil ||
			response.Header.Get("Retry-After") != faults.TemporaryUnavailableRetryAfter {
			c.recordProxyFailure(errors.New("React Native retention-reconnect temporary-unavailable response is invalid"))
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
		c.signalSealedPush()
		writeRetentionReconnectProxyResponse(writer, response.StatusCode, response.Header, responseBody)
		return
	}
	target := strings.TrimRight(c.upstream, "/") + request.URL.RequestURI()
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target, bytes.NewReader(body))
	if err != nil {
		c.recordProxyFailure(fmt.Errorf("create React Native retention-reconnect upstream request: %w", err))
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
		c.recordProxyFailure(fmt.Errorf("execute React Native retention-reconnect upstream request: %w", err))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maximumExchangeBytes+1))
	if err != nil || len(responseBody) > maximumExchangeBytes {
		c.recordProxyFailure(errors.New("React Native retention-reconnect upstream response is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	if isPush {
		if err := c.observeReleasedPush(body, response.StatusCode, responseBody); err != nil {
			c.recordProxyFailure(err)
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
	}
	if request.Method == http.MethodPost && request.URL.Path == "/sync/connect" {
		if err := c.observeRenewedConnect(body, response.StatusCode, responseBody); err != nil {
			c.recordProxyFailure(err)
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
	}
	writeRetentionReconnectProxyResponse(writer, response.StatusCode, response.Header, responseBody)
}

func writeRetentionReconnectProxyResponse(writer http.ResponseWriter, status int, header http.Header, body []byte) {
	for name, values := range header {
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}

func (c *RetentionReconnectCoordinator) temporaryFaultActive() bool {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	return c.faultArmed
}

func (c *RetentionReconnectCoordinator) observeSealedPush(raw []byte) error {
	batchID, mutationIDs, generation, err := c.decodeSealedPush(raw)
	if err != nil {
		return err
	}
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if len(c.sealedRequest) != 0 {
		if !semanticRawJSONEqual(c.sealedRequest, raw) {
			return errors.New("React Native retention-reconnect temporary fault received a changed sealed batch")
		}
		return nil
	}
	c.sealedRequest = copyRaw(raw)
	c.sealedBatchID = batchID
	c.sealedMutationIDs = append([]string(nil), mutationIDs...)
	c.sealedGeneration = generation
	return nil
}

func (c *RetentionReconnectCoordinator) decodeSealedPush(raw []byte) (string, []string, uint64, error) {
	var request struct {
		ClientID         string `json:"client_id"`
		ClientGeneration uint64 `json:"client_generation"`
		BatchID          string `json:"batch_id"`
		Mutations        []struct {
			MutationID string `json:"mutation_id"`
		} `json:"mutations"`
	}
	if err := jsonstrict.Decode(raw, &request); err != nil || request.ClientID != c.main.clientID || request.ClientGeneration == 0 ||
		request.BatchID == "" || len(request.Mutations) != c.sealedMutationCount {
		return "", nil, 0, errors.New("React Native retention-reconnect sealed push is invalid")
	}
	seen := make(map[string]struct{}, len(request.Mutations))
	ids := make([]string, 0, len(request.Mutations))
	for _, mutation := range request.Mutations {
		if mutation.MutationID == "" {
			return "", nil, 0, errors.New("React Native retention-reconnect sealed mutation identity is absent")
		}
		if _, duplicate := seen[mutation.MutationID]; duplicate {
			return "", nil, 0, errors.New("React Native retention-reconnect sealed mutation identity is duplicated")
		}
		seen[mutation.MutationID] = struct{}{}
		ids = append(ids, mutation.MutationID)
	}
	return request.BatchID, ids, request.ClientGeneration, nil
}

func (c *RetentionReconnectCoordinator) observeReleasedPush(raw []byte, status int, response []byte) error {
	c.proxyMu.Lock()
	sealed := copyRaw(c.sealedRequest)
	alreadyRejected := c.rejectedGeneration != 0
	c.proxyMu.Unlock()
	if len(sealed) == 0 {
		return errors.New("React Native retention-reconnect released push arrived before the sealed batch")
	}
	if !semanticRawJSONEqual(sealed, raw) {
		if alreadyRejected {
			return nil
		}
		return errors.New("React Native retention-reconnect released push changed sealed intent")
	}
	if alreadyRejected {
		return errors.New("React Native retention-reconnect repeated the expired sealed batch")
	}
	if err := retentionReconnectValidateHTTPWire(status, response, c.rejectionWire); err != nil {
		return fmt.Errorf("React Native retention-reconnect expired push wire: %w", err)
	}
	_, _, generation, err := c.decodeSealedPush(raw)
	if err != nil {
		return err
	}
	c.proxyMu.Lock()
	c.rejectedGeneration = generation
	c.proxyMu.Unlock()
	c.signalRejectedPush()
	return nil
}

func (c *RetentionReconnectCoordinator) observeRenewedConnect(raw []byte, status int, response []byte) error {
	c.proxyMu.Lock()
	rejectedGeneration := c.rejectedGeneration
	renewedGeneration := c.renewedGeneration
	c.proxyMu.Unlock()
	if rejectedGeneration == 0 || renewedGeneration != 0 {
		return nil
	}
	if err := retentionReconnectValidateHTTPWire(status, response, c.renewalWire); err != nil {
		return fmt.Errorf("React Native retention-reconnect renewal wire: %w", err)
	}
	var request struct {
		ClientID         string `json:"client_id"`
		ClientGeneration uint64 `json:"client_generation"`
	}
	if err := jsonstrict.Decode(raw, &request); err != nil || request.ClientID != c.main.clientID || request.ClientGeneration != rejectedGeneration {
		return errors.New("React Native retention-reconnect renewal request generation is invalid")
	}
	var result struct {
		ClientGeneration uint64 `json:"client_generation"`
	}
	if err := jsonstrict.Decode(response, &result); err != nil || result.ClientGeneration <= rejectedGeneration {
		return errors.New("React Native retention-reconnect renewal response did not advance generation")
	}
	c.proxyMu.Lock()
	c.renewedGeneration = result.ClientGeneration
	c.proxyMu.Unlock()
	c.signalRenewedConnect()
	return nil
}

func retentionReconnectValidateHTTPWire(status int, body []byte, wire scenarios.WireExpectation) error {
	if status != wire.HTTPStatus {
		return fmt.Errorf("status = %d, want %d", status, wire.HTTPStatus)
	}
	if wire.ErrorCode == nil {
		return jsonstrict.ValidateValue(body)
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := jsonstrict.Decode(body, &envelope); err != nil || envelope.Error.Code != *wire.ErrorCode || envelope.Error.Retryable != wire.Retryable {
		return errors.New("error envelope differs from authored wire")
	}
	return nil
}

func (c *RetentionReconnectCoordinator) releaseFault() {
	if c == nil {
		return
	}
	c.proxyMu.Lock()
	c.faultArmed = false
	c.proxyMu.Unlock()
}

func (c *RetentionReconnectCoordinator) recordProxyFailure(err error) {
	if c == nil || err == nil {
		return
	}
	c.proxyMu.Lock()
	if c.proxyErr == nil {
		c.proxyErr = err
	}
	c.proxyMu.Unlock()
	c.signalSealedPush()
	c.signalRejectedPush()
	c.signalRenewedConnect()
}

func (c *RetentionReconnectCoordinator) signalSealedPush() {
	c.sealedPushOnce.Do(func() { close(c.sealedPush) })
}

func (c *RetentionReconnectCoordinator) signalRejectedPush() {
	c.rejectedPushOnce.Do(func() { close(c.rejectedPush) })
}

func (c *RetentionReconnectCoordinator) signalRenewedConnect() {
	c.renewedConnectOnce.Do(func() { close(c.renewedConnect) })
}

func (c *RetentionReconnectCoordinator) waitForSealedPush(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native retention-reconnect sealed push: %w", ctx.Err())
	case <-c.sealedPush:
	}
	return c.proxyFailure("sealed push")
}

func (c *RetentionReconnectCoordinator) waitForRenewedConnect(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native retention-reconnect renewal: %w", ctx.Err())
	case <-c.renewedConnect:
	}
	return c.proxyFailure("renewal")
}

func (c *RetentionReconnectCoordinator) proxyFailure(stage string) error {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.proxyErr != nil {
		return fmt.Errorf("React Native retention-reconnect %s proxy failed: %w", stage, c.proxyErr)
	}
	return nil
}

func (c *RetentionReconnectCoordinator) sealedIdentity() (uint64, uint64, uint64, error) {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.sealedGeneration == 0 {
		return 0, 0, 0, errors.New("React Native retention-reconnect sealed generation is unavailable")
	}
	return c.sealedGeneration, c.rejectedGeneration, c.renewedGeneration, nil
}

func (c *RetentionReconnectCoordinator) proxyIdentity() (string, []string, uint64, uint64, uint64, error) {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.sealedBatchID == "" || len(c.sealedMutationIDs) == 0 || c.sealedGeneration == 0 {
		return "", nil, 0, 0, 0, errors.New("React Native retention-reconnect sealed identity is unavailable")
	}
	return c.sealedBatchID, append([]string(nil), c.sealedMutationIDs...), c.sealedGeneration, c.rejectedGeneration, c.renewedGeneration, nil
}
