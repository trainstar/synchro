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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	forgedCursorScenarioPath = "conformance/scenarios/server/rebuild-forged-cursor-001.json"
	forgedCursorScenarioID   = "SCN-REBUILD-FORGED-CURSOR-001"
	forgedCursorSeedAsset    = "seed.db"
	forgedCursorOverride     = "native-forged-rebuild-cursor"
	// Keep a response window before Detox cancels the exchange.
	forgedCursorPushWait = 20 * time.Second
)

var forgedCursorStepOrder = []scenarios.StepID{
	"STEP-REBUILD-FORGED-CURSOR-001",
	"STEP-REBUILD-FORGED-CURSOR-002",
	"STEP-REBUILD-FORGED-CURSOR-003",
	"STEP-REBUILD-FORGED-CURSOR-004",
	"STEP-REBUILD-FORGED-CURSOR-005",
	"STEP-REBUILD-FORGED-CURSOR-006",
}

var forgedCursorAliasNames = []string{
	"row-one-mutation",
	"row-two-mutation",
	"source-batch",
	"client-generation-one",
	"current-schema",
	"items-table",
	"row-one-primary-key",
	"row-two-primary-key",
	"scope-a",
	"forged-rebuild",
}

// ForgedCursorCoordinatorConfig configures one forged-cursor sidecar.
type ForgedCursorCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// ForgedCursorCoordinatorResult contains final server and identity evidence.
type ForgedCursorCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// ForgedCursorCoordinator drives the authored staged call through React Native.
type ForgedCursorCoordinator struct {
	config ForgedCursorCoordinatorConfig

	listener    net.Listener
	server      *http.Server
	token       string
	adapter     string
	upstream    string
	upstreamURL *url.URL
	database    string
	transport   *http.Client

	proxyMu             sync.Mutex
	rebuildRequests     uint64
	rebuildResponses    [2]forgedCursorRebuildResponse
	proxyErr            error
	pushCommitted       chan struct{}
	pushCommittedOnce   sync.Once
	firstPageReady      chan struct{}
	firstPageReadyOnce  sync.Once
	forgedPageReady     chan struct{}
	forgedPageReadyOnce sync.Once
	allowPushResponse   chan struct{}
	allowPushOnce       sync.Once
	allowForgedPage     chan struct{}
	allowForgedPageOnce sync.Once

	steps        map[scenarios.StepID]scenarios.Step
	identities   []scenarios.NativeIdentityAlias
	runtimeIDs   map[string]json.RawMessage
	authTokens   map[string]string
	serverClient forgedCursorServerClient
	clientKey    string
	callID       string
	tableName    string
	expected     *scenarios.StateFacts

	mu           sync.Mutex
	prepared     bool
	closed       bool
	completed    bool
	failed       error
	stage        forgedCursorStage
	nextSeq      uint64
	process      *actionProcessIdentity
	serverBefore *scenarios.StateFacts
	serverAfter  *scenarios.StateFacts
	finalCapture *finalCapture
	result       ForgedCursorCoordinatorResult
}

type forgedCursorRebuildResponse struct {
	observed       bool
	upstreamStatus int
	proxiedStatus  int
	upstreamBody   string
	proxiedBody    string
}

type forgedCursorStage uint8

const (
	forgedCursorStageOpen forgedCursorStage = iota
	forgedCursorStageOpened
	forgedCursorStageFirstWrite
	forgedCursorStageSecondWrite
	forgedCursorStageCallBegun
	forgedCursorStageFirstPage
	forgedCursorStageForgedPage
	forgedCursorStageCallComplete
	forgedCursorStageCapture
	forgedCursorStageComplete
	forgedCursorStagePushTimeoutDiagnostic
)

var errForgedCursorPushWaitTimeout = errors.New("React Native forged-cursor push commit timed out")

type forgedCursorServerClient struct {
	UserID           string
	ClientID         string
	ClientGeneration uint64
}

type forgedCursorRuntime struct {
	ClientKey        string `json:"client_key"`
	Database         string `json:"database_path"`
	ClientID         string `json:"client_id"`
	SeedDatabasePath string `json:"seed_database_path"`
	ServerURL        string `json:"server_url"`
	AuthToken        string `json:"auth_token"`
}

type forgedCursorCommand struct {
	SchemaVersion int                 `json:"schema_version"`
	Action        conformanceManifest `json:"action"`
	Runtime       forgedCursorRuntime `json:"runtime"`
}

type forgedCursorExchangeResponse struct {
	SchemaVersion int                  `json:"schema_version"`
	Sequence      uint64               `json:"sequence"`
	State         string               `json:"state"`
	Command       *forgedCursorCommand `json:"command"`
}

// LoadForgedCursorScenario loads the authored forged-cursor contract.
func LoadForgedCursorScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, forgedCursorScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native forged-cursor scenario from %q: %w", forgedCursorScenarioPath, err)
	}
	if err := ValidateForgedCursorScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateForgedCursorScenario rejects changes to the closed RN contract.
func ValidateForgedCursorScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != forgedCursorScenarioID {
		return fmt.Errorf("React Native forged-cursor scenario ID = %q, want %q", scenario.ID, forgedCursorScenarioID)
	}
	if len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return fmt.Errorf("React Native forged-cursor setup count = %d and first operation = %q, want 1 and model/install-current-contract", len(scenario.Model.Setup), firstOperationKey(scenario.Model.Setup))
	}
	if len(scenario.Steps) != len(forgedCursorStepOrder) {
		return fmt.Errorf("React Native forged-cursor step count = %d, want %d", len(scenario.Steps), len(forgedCursorStepOrder))
	}
	expected := []struct {
		key, kind, stage, method, completion string
	}{
		{"local/write", "local-write", "", "", ""},
		{"local/write", "local-write", "", "", ""},
		{"push/submit", "public-call", "begin", "start", ""},
		{"process/materialize-source-transaction", "controller", "", "", ""},
		{"rebuild/request-page", "public-call", "await-step", "", ""},
		{"rebuild/request-page", "public-call", "await-call", "", "error"},
	}
	var callID string
	for index, step := range scenario.Steps {
		binding := step.NativeBinding
		actualCallID := ""
		if binding != nil && binding.CallID != nil {
			actualCallID = string(*binding.CallID)
		}
		if step.ID != forgedCursorStepOrder[index] || binding == nil || scenarios.OperationKey(step.Operation) != expected[index].key ||
			binding.Kind != expected[index].kind || binding.Stage != expected[index].stage || binding.Method != expected[index].method ||
			binding.Completion != expected[index].completion || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native forged-cursor step %d has ID=%q operation=%q kind=%q stage=%q method=%q completion=%q disposition=%q, want ID=%q operation=%q kind=%q stage=%q method=%q completion=%q disposition=success", index+1, step.ID, scenarios.OperationKey(step.Operation), bindingValue(binding, "kind"), bindingValue(binding, "stage"), bindingValue(binding, "method"), bindingValue(binding, "completion"), step.ExpectedOutcome.Disposition, forgedCursorStepOrder[index], expected[index].key, expected[index].kind, expected[index].stage, expected[index].method, expected[index].completion)
		}
		if binding.Kind != "controller" && (binding.UserID == "" || binding.ClientID == "") {
			return fmt.Errorf("React Native forged-cursor step %s has user=%q client=%q", step.ID, binding.UserID, binding.ClientID)
		}
		if binding.Kind == "public-call" {
			if actualCallID == "" {
				return fmt.Errorf("React Native forged-cursor step %s call ID = %q", step.ID, actualCallID)
			}
			if callID == "" {
				callID = actualCallID
			} else if actualCallID != callID {
				return fmt.Errorf("React Native forged-cursor step %s call ID = %q, want %q", step.ID, actualCallID, callID)
			}
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 0 {
		return fmt.Errorf("React Native forged-cursor lifecycle boundary count = %d, want 0", len(scenario.NativeLifecycleBoundaries))
	}
	if err := validateForgedCursorAliases(scenario.NativeIdentityAliases); err != nil {
		return err
	}
	var firstPage, forgedPage struct {
		UserID           string       `json:"user_id"`
		ClientID         string       `json:"client_id"`
		ClientGeneration uint64       `json:"client_generation"`
		Schema           clientSchema `json:"schema"`
		ScopeID          string       `json:"scope_id"`
		RebuildID        string       `json:"rebuild_id"`
		CursorSource     string       `json:"cursor_source"`
		Limit            uint64       `json:"limit"`
	}
	if err := json.Unmarshal(scenario.Steps[4].Operation.Payload, &firstPage); err != nil {
		return fmt.Errorf("decode React Native forged-cursor first-page operation: %w", err)
	}
	if err := json.Unmarshal(scenario.Steps[5].Operation.Payload, &forgedPage); err != nil {
		return fmt.Errorf("decode React Native forged-cursor forged-page operation: %w", err)
	}
	firstPage.CursorSource = ""
	forgedSource := forgedPage.CursorSource
	forgedPage.CursorSource = ""
	if !reflect.DeepEqual(firstPage, forgedPage) || scenarioCursorSource(scenario.Steps[4]) != "none" || forgedSource != "forged" || firstPage.Limit != 1 {
		return fmt.Errorf("React Native forged-cursor page bindings differ or cursor sources are invalid: first=%+v forged=%+v first_source=%q forged_source=%q", firstPage, forgedPage, scenarioCursorSource(scenario.Steps[4]), forgedSource)
	}
	serverClient, err := forgedCursorServerClientFromScenario(scenario)
	if err != nil {
		return err
	}
	for _, step := range scenario.Steps {
		binding := step.NativeBinding
		if binding.Kind != "controller" && (binding.UserID != serverClient.UserID || binding.ClientID != serverClient.ClientID) {
			return fmt.Errorf("React Native forged-cursor step %s native user/client = %q/%q, server user/client = %q/%q", step.ID, binding.UserID, binding.ClientID, serverClient.UserID, serverClient.ClientID)
		}
	}
	if !forgedCursorAssertionIsAuthored(scenario) || forgedCursorExpectedState(scenario) == nil {
		return fmt.Errorf("React Native forged-cursor authored assertion valid=%t expected state present=%t", forgedCursorAssertionIsAuthored(scenario), forgedCursorExpectedState(scenario) != nil)
	}
	expectedWires := map[scenarios.StepID]struct {
		contractCase string
		status       int
		code         string
	}{
		forgedCursorStepOrder[2]: {contractCase: "push_success", status: http.StatusOK},
		forgedCursorStepOrder[4]: {contractCase: "rebuild_success", status: http.StatusOK},
		forgedCursorStepOrder[5]: {contractCase: "invalid_request", status: http.StatusBadRequest, code: "invalid_request"},
	}
	for stepID, expectedWire := range expectedWires {
		wire, err := forgedCursorWireExpectation(scenario, stepID)
		if err != nil {
			return err
		}
		code := ""
		if wire.ErrorCode != nil {
			code = *wire.ErrorCode
		}
		if wire.ContractCase != expectedWire.contractCase || wire.HTTPStatus != expectedWire.status || code != expectedWire.code || wire.Retryable {
			return fmt.Errorf("React Native forged-cursor wire %s={case:%q status:%d code:%q retryable:%t}, want={case:%q status:%d code:%q retryable:false}", stepID, wire.ContractCase, wire.HTTPStatus, code, wire.Retryable, expectedWire.contractCase, expectedWire.status, expectedWire.code)
		}
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-REBUILD-FORGED-CURSOR-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[id]++
			}
		case "OBL-REBUILD-FORGED-CURSOR-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[id]++
			}
		case "OBL-REBUILD-FORGED-CURSOR-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-REBUILD-FORGED-CURSOR-001", "CTRL-REBUILD-007") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-REBUILD-FORGED-CURSOR-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-REBUILD-FORGED-CURSOR-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-REBUILD-FORGED-CURSOR-CONTROL-001"] != 1 {
		return fmt.Errorf("React Native forged-cursor proof obligation matches = iOS:%d Android:%d control:%d, want 1/1/1", obligations["OBL-REBUILD-FORGED-CURSOR-RN-IOS-CURRENT-001"], obligations["OBL-REBUILD-FORGED-CURSOR-RN-ANDROID-CURRENT-001"], obligations["OBL-REBUILD-FORGED-CURSOR-CONTROL-001"])
	}
	return nil
}

func firstOperationKey(operations []scenarios.Operation) string {
	if len(operations) == 0 {
		return ""
	}
	return scenarios.OperationKey(operations[0])
}

func bindingValue(binding *scenarios.NativeStepBinding, field string) string {
	if binding == nil {
		return "<nil>"
	}
	switch field {
	case "kind":
		return binding.Kind
	case "stage":
		return binding.Stage
	case "method":
		return binding.Method
	case "completion":
		return binding.Completion
	default:
		return "<unknown>"
	}
}

func validateForgedCursorAliases(aliases []scenarios.NativeIdentityAlias) error {
	if len(aliases) != len(forgedCursorAliasNames) {
		return fmt.Errorf("React Native forged-cursor identity alias count = %d, want %d", len(aliases), len(forgedCursorAliasNames))
	}
	wanted := make(map[string]bool, len(forgedCursorAliasNames))
	for _, name := range forgedCursorAliasNames {
		wanted[name] = false
	}
	for _, alias := range aliases {
		seen, found := wanted[alias.Alias]
		if !found || seen {
			return fmt.Errorf("React Native forged-cursor identity alias %q found=%t duplicate=%t", alias.Alias, found, seen)
		}
		expectedKind, expectedSteps, expectedExpectations := forgedCursorAliasContract(alias.Alias)
		if alias.Kind != expectedKind || !slices.Equal(alias.StepIDs, expectedSteps) || !slices.Equal(alias.ExpectationIDs, expectedExpectations) {
			return fmt.Errorf("React Native forged-cursor identity alias %q kind=%q steps=%v expectations=%v, want kind=%q steps=%v expectations=%v", alias.Alias, alias.Kind, alias.StepIDs, alias.ExpectationIDs, expectedKind, expectedSteps, expectedExpectations)
		}
		wanted[alias.Alias] = true
	}
	for _, name := range forgedCursorAliasNames {
		if !wanted[name] {
			return fmt.Errorf("React Native forged-cursor identity alias %q present=%t, want true", name, wanted[name])
		}
	}
	return nil
}

func forgedCursorAliasContract(alias string) (string, []scenarios.StepID, []scenarios.ExpectationID) {
	switch alias {
	case "row-one-mutation":
		return "mutation-id", []scenarios.StepID{forgedCursorStepOrder[0], forgedCursorStepOrder[2]}, nil
	case "row-two-mutation":
		return "mutation-id", []scenarios.StepID{forgedCursorStepOrder[1], forgedCursorStepOrder[2]}, nil
	case "source-batch":
		return "batch-id", []scenarios.StepID{forgedCursorStepOrder[2]}, nil
	case "client-generation-one":
		return "client-generation", []scenarios.StepID{forgedCursorStepOrder[2], forgedCursorStepOrder[4], forgedCursorStepOrder[5]}, nil
	case "current-schema":
		return "schema", []scenarios.StepID{forgedCursorStepOrder[0], forgedCursorStepOrder[1], forgedCursorStepOrder[2], forgedCursorStepOrder[4], forgedCursorStepOrder[5]}, nil
	case "items-table":
		return "table", []scenarios.StepID{forgedCursorStepOrder[0], forgedCursorStepOrder[1], forgedCursorStepOrder[2]}, nil
	case "row-one-primary-key":
		return "primary-key", []scenarios.StepID{forgedCursorStepOrder[0], forgedCursorStepOrder[2]}, nil
	case "row-two-primary-key":
		return "primary-key", []scenarios.StepID{forgedCursorStepOrder[1], forgedCursorStepOrder[2]}, nil
	case "scope-a":
		return "scope", []scenarios.StepID{forgedCursorStepOrder[4], forgedCursorStepOrder[5]}, []scenarios.ExpectationID{"EXPECT-REBUILD-FORGED-CURSOR-STATE-001"}
	case "forged-rebuild":
		return "rebuild-id", []scenarios.StepID{forgedCursorStepOrder[4], forgedCursorStepOrder[5]}, []scenarios.ExpectationID{"EXPECT-REBUILD-FORGED-CURSOR-STATE-001"}
	default:
		return "", nil, nil
	}
}

func scenarioCursorSource(step scenarios.Step) string {
	var payload struct {
		CursorSource string `json:"cursor_source"`
	}
	if json.Unmarshal(step.Operation.Payload, &payload) != nil {
		return ""
	}
	return payload.CursorSource
}

func forgedCursorAssertionIsAuthored(scenario scenarios.Scenario) bool {
	for _, assertion := range scenario.Assertions {
		if assertion.ID == "ASSERT-REBUILD-FORGED-CURSOR-SEMANTIC-001" {
			return assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.Kind == "wire-contract" &&
				assertion.Oracle.ExpectedSource == "authored-model" && assertion.Oracle.ObservedSource == "system-under-test" &&
				reflect.DeepEqual(assertion.ExpectationIDs, []scenarios.ExpectationID{"EXPECT-REBUILD-FORGED-CURSOR-WIRE-001", "EXPECT-REBUILD-FORGED-CURSOR-STATE-001"}) &&
				len(assertion.DetectsControlIDs) == 1 && assertion.DetectsControlIDs[0] == "CTRL-REBUILD-007"
		}
	}
	return false
}

// NewForgedCursorCoordinator creates an authenticated host-loopback sidecar.
func NewForgedCursorCoordinator(config ForgedCursorCoordinatorConfig) (*ForgedCursorCoordinator, error) {
	if err := ValidateForgedCursorScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, fmt.Errorf("React Native forged-cursor platform = %q, want ios or android", config.Platform)
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native forged-cursor auth token is empty and harness is unavailable")
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
		return nil, fmt.Errorf("resolve React Native forged-cursor upstream URL %q: %w", serverURL, err)
	}
	upstreamURL, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse React Native forged-cursor upstream URL %q: %w", upstream, err)
	}
	capability, err := randomToken(32)
	if err != nil {
		return nil, fmt.Errorf("create React Native forged-cursor coordinator capability: %w", err)
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-forged-cursor-")
		if err != nil {
			return nil, fmt.Errorf("create React Native forged-cursor database name: %w", err)
		}
	}
	if !validDatabaseName(database) {
		return nil, fmt.Errorf("React Native forged-cursor database name = %q", database)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for React Native forged-cursor coordinator on host loopback: %w", err)
	}
	adapter, err := nativeAdapterURL("http://"+listener.Addr().String(), config.Platform)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("resolve React Native forged-cursor proxy URL for %q: %w", config.Platform, err)
	}
	serverClient, err := forgedCursorServerClientFromScenario(config.Scenario)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	callID := string(*steps[forgedCursorStepOrder[2]].NativeBinding.CallID)
	coordinator := &ForgedCursorCoordinator{
		config: config, listener: listener, token: capability, adapter: adapter, upstream: upstream, upstreamURL: upstreamURL, database: database,
		transport: &http.Client{}, pushCommitted: make(chan struct{}), firstPageReady: make(chan struct{}), forgedPageReady: make(chan struct{}), allowPushResponse: make(chan struct{}), allowForgedPage: make(chan struct{}),
		steps: steps, identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), authTokens: make(map[string]string), serverClient: serverClient,
		clientKey: serverClient.ClientID, callID: callID, expected: forgedCursorExpectedState(config.Scenario), nextSeq: 1,
	}
	coordinator.server = &http.Server{Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return coordinator, nil
}

// Prepare installs the contract and maps local writes before the app opens.
func (c *ForgedCursorCoordinator) Prepare(ctx context.Context) error {
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
		return fmt.Errorf("prepare React Native forged-cursor coordinator with context error: %w", err)
	}
	if c.config.Controller == nil || c.config.Harness == nil {
		return fmt.Errorf("React Native forged-cursor dependencies: controller nil=%t harness nil=%t", c.config.Controller == nil, c.config.Harness == nil)
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native forged-cursor contract: %w", err)
	}
	for _, stepID := range forgedCursorStepOrder[:2] {
		operation, err := c.config.Controller.ApplicationWrite(c.steps[stepID].Operation)
		if err != nil {
			return fmt.Errorf("map React Native forged-cursor local write %s: %w", stepID, err)
		}
		step := c.steps[stepID]
		step.Operation = operation
		c.steps[stepID] = step
	}
	if c.config.AuthToken != "" {
		c.authTokens[c.clientKey] = c.config.AuthToken
	} else {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.serverClient.UserID, time.Now())
		if err != nil {
			return fmt.Errorf("mint React Native forged-cursor bearer token for client key %q and user %q: %w", c.clientKey, c.serverClient.UserID, err)
		}
		c.authTokens[c.clientKey] = token
	}
	if err := c.bindInitialServerIdentities(); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

func (c *ForgedCursorCoordinator) bindInitialServerIdentities() error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native forged-cursor initial server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		if value.Alias == "items-table" {
			c.tableName = value.ApplicationIdentifier
		}
	}
	missing := make([]string, 0)
	for _, alias := range aliases {
		if len(c.runtimeIDs[alias.Alias]) == 0 {
			missing = append(missing, alias.Alias)
		}
	}
	if c.tableName == "" || len(missing) != 0 {
		return fmt.Errorf("React Native forged-cursor initial server identities: table=%q missing=%v", c.tableName, missing)
	}
	return nil
}

// Serve runs the sidecar until it closes.
func (c *ForgedCursorCoordinator) Serve(ctx context.Context) error {
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
			closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(closeContext)
			cancel()
		case <-stop:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve React Native forged-cursor coordinator: %w", err)
}

func (c *ForgedCursorCoordinator) Handler() http.Handler { return c }

func (c *ForgedCursorCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *ForgedCursorCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// ExchangeCount returns the exact Detox exchange count.
func (c *ForgedCursorCoordinator) ExchangeCount() int { return int(forgedCursorStageComplete) }

func (c *ForgedCursorCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *ForgedCursorCoordinator) Result() (ForgedCursorCoordinatorResult, error) {
	if c == nil {
		return ForgedCursorCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return ForgedCursorCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return ForgedCursorCoordinatorResult{}, fmt.Errorf("React Native forged-cursor coordinator completed=%t stage=%d", c.completed, c.stage)
	}
	return c.result, nil
}

func (c *ForgedCursorCoordinator) Close(ctx context.Context) error {
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
	c.releasePushResponse()
	c.releaseForgedPage()
	shutdownErr, listenerErr := c.server.Shutdown(ctx), c.listener.Close()
	if shutdownErr != nil {
		return fmt.Errorf("shut down React Native forged-cursor server: %w", shutdownErr)
	}
	if listenerErr != nil && !errors.Is(listenerErr, net.ErrClosed) {
		return fmt.Errorf("close React Native forged-cursor listener: %w", listenerErr)
	}
	return nil
}

func (c *ForgedCursorCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
		c.failed = fmt.Errorf("React Native forged-cursor exchange unavailable: closed=%t prepared=%t failed=%v completed=%t sequence=%d want=%d", c.closed, c.prepared, c.failed, c.completed, exchange.Sequence, c.nextSeq)
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native forged-cursor exchange %d stage %d result failed: %w", exchange.Sequence, c.stage, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(request.Context(), exchange.Sequence)
	if err != nil {
		c.failed = fmt.Errorf("React Native forged-cursor exchange %d stage %d advance failed: %w", exchange.Sequence, c.stage, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = fmt.Errorf("React Native forged-cursor response encoding: bytes=%d maximum=%d error=%v", len(encoded), maximumExchangeBytes, err)
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *ForgedCursorCoordinator) proxyAdapter(writer http.ResponseWriter, request *http.Request) {
	if c == nil || c.transport == nil || c.upstreamURL == nil {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	requestBody, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
	_ = request.Body.Close()
	if err != nil || len(requestBody) > maximumExchangeBytes {
		c.recordProxyFailure(fmt.Errorf("read React Native forged-cursor proxy request: bytes=%d maximum=%d error=%v", len(requestBody), maximumExchangeBytes, err))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	rebuildRequest := uint64(0)
	if request.Method == http.MethodPost && request.URL.Path == "/sync/rebuild" {
		rebuildRequest = c.beginRebuildRequest()
		if rebuildRequest > 2 {
			err := fmt.Errorf("React Native forged-cursor rebuild request count=%d, want at most 2", rebuildRequest)
			c.recordProxyFailure(err)
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
		if rebuildRequest == 2 {
			select {
			case <-request.Context().Done():
				c.recordProxyFailure(fmt.Errorf("wait to release React Native forged-cursor retry: %w", request.Context().Err()))
				writeExchangeError(writer, http.StatusBadGateway)
				return
			case <-c.allowForgedPage:
			}
		}
		requestBody, err = c.bindRebuildRequestLimit(requestBody, rebuildRequest)
		if err != nil {
			c.recordProxyFailure(err)
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
	}
	request.Body = io.NopCloser(bytes.NewReader(requestBody))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(requestBody)), nil
	}
	if rebuildRequest != 0 && request.ContentLength >= 0 {
		request.ContentLength = int64(len(requestBody))
	}
	isConnect := request.Method == http.MethodPost && request.URL.Path == "/sync/connect"
	isPush := request.Method == http.MethodPost && request.URL.Path == "/sync/push"
	var upstreamStatus int
	var upstreamBody []byte
	proxy := &httputil.ReverseProxy{}
	proxy.Rewrite = func(proxyRequest *httputil.ProxyRequest) {
		proxyRequest.SetURL(c.upstreamURL)
		proxyRequest.Out.Host = c.upstreamURL.Host
		// Preserve the device request because the proxy changes only authored rebuild facts.
		proxyRequest.Out.Header = proxyRequest.In.Header.Clone()
		proxyRequest.Out.ContentLength = proxyRequest.In.ContentLength
		proxyRequest.Out.TransferEncoding = slices.Clone(proxyRequest.In.TransferEncoding)
		proxyRequest.Out.Trailer = proxyRequest.In.Trailer.Clone()
		proxyRequest.Out.Close = proxyRequest.In.Close
		proxyRequest.Out.GetBody = proxyRequest.In.GetBody
	}
	proxy.Transport = c.transport.Transport
	proxy.ModifyResponse = func(response *http.Response) error {
		upstreamStatus = response.StatusCode
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maximumExchangeBytes+1))
		_ = response.Body.Close()
		upstreamBody = append(upstreamBody[:0], body...)
		if readErr != nil || len(body) > maximumExchangeBytes {
			return fmt.Errorf("read React Native forged-cursor upstream response: bytes=%d maximum=%d error=%v", len(body), maximumExchangeBytes, readErr)
		}
		if isConnect && response.StatusCode != http.StatusOK {
			c.recordProxyFailure(fmt.Errorf("React Native forged-cursor connect upstream status=%d want=%d response_bytes=%d response_body=%q", response.StatusCode, http.StatusOK, len(body), boundedRaw(body)))
		}
		proxiedBody := body
		if rebuildRequest == 1 {
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("React Native forged-cursor first page status=%d want=%d upstream_body=%q", response.StatusCode, http.StatusOK, boundedRaw(body))
			}
			proxiedBody, err = mutateForgedCursorFirstResponse(body)
			if err != nil {
				return err
			}
		}
		if rebuildRequest != 0 {
			c.recordRebuildResponse(rebuildRequest, response.StatusCode, response.StatusCode, body, proxiedBody)
		}
		response.Body = io.NopCloser(bytes.NewReader(proxiedBody))
		response.ContentLength = int64(len(proxiedBody))
		response.Header.Set("Content-Length", strconv.Itoa(len(proxiedBody)))
		if rebuildRequest == 1 {
			c.signalFirstPageReady()
		}
		if rebuildRequest == 2 {
			c.signalForgedPageReady()
		}
		if isPush {
			if response.StatusCode != http.StatusOK {
				c.recordProxyFailure(fmt.Errorf("React Native forged-cursor push status=%d want=%d", response.StatusCode, http.StatusOK))
			}
			c.signalPushCommitted()
			select {
			case <-request.Context().Done():
				return fmt.Errorf("wait to release React Native forged-cursor push response: %w", request.Context().Err())
			case <-c.allowPushResponse:
			}
		}
		return nil
	}
	proxy.ErrorHandler = func(responseWriter http.ResponseWriter, _ *http.Request, proxyErr error) {
		proxiedBody := []byte(`{"error":"invalid_request"}`)
		if rebuildRequest != 0 {
			c.recordRebuildResponse(rebuildRequest, upstreamStatus, http.StatusBadGateway, upstreamBody, proxiedBody)
		}
		c.recordProxyFailure(fmt.Errorf("proxy React Native forged-cursor request: %w", proxyErr))
		writeExchangeError(responseWriter, http.StatusBadGateway)
	}
	proxy.ServeHTTP(writer, request)
}

func (c *ForgedCursorCoordinator) beginRebuildRequest() uint64 {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	c.rebuildRequests++
	return c.rebuildRequests
}

func (c *ForgedCursorCoordinator) recordRebuildResponse(requestNumber uint64, upstreamStatus, proxiedStatus int, upstreamBody, proxiedBody []byte) {
	if requestNumber == 0 || requestNumber > uint64(len(c.rebuildResponses)) {
		return
	}
	c.proxyMu.Lock()
	c.rebuildResponses[requestNumber-1] = forgedCursorRebuildResponse{
		observed:       true,
		upstreamStatus: upstreamStatus,
		proxiedStatus:  proxiedStatus,
		upstreamBody:   boundedRaw(upstreamBody),
		proxiedBody:    boundedRaw(proxiedBody),
	}
	c.proxyMu.Unlock()
}

func (c *ForgedCursorCoordinator) rebuildResponseDiagnostic() string {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	return c.rebuildResponseDiagnosticLocked()
}

func (c *ForgedCursorCoordinator) recordProxyFailure(err error) {
	c.proxyMu.Lock()
	if c.proxyErr == nil {
		c.proxyErr = err
	}
	c.proxyMu.Unlock()
	c.signalPushCommitted()
	c.signalFirstPageReady()
	c.signalForgedPageReady()
}

func (c *ForgedCursorCoordinator) signalFirstPageReady() {
	c.firstPageReadyOnce.Do(func() { close(c.firstPageReady) })
}

func (c *ForgedCursorCoordinator) signalPushCommitted() {
	c.pushCommittedOnce.Do(func() { close(c.pushCommitted) })
}

func (c *ForgedCursorCoordinator) signalForgedPageReady() {
	c.forgedPageReadyOnce.Do(func() { close(c.forgedPageReady) })
}

func (c *ForgedCursorCoordinator) releaseForgedPage() {
	c.allowForgedPageOnce.Do(func() { close(c.allowForgedPage) })
}

func (c *ForgedCursorCoordinator) releasePushResponse() {
	c.allowPushOnce.Do(func() { close(c.allowPushResponse) })
}

func (c *ForgedCursorCoordinator) waitForFirstPage(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native forged-cursor first page: %w", ctx.Err())
	case <-c.firstPageReady:
	}
	return c.proxyFailure("first page")
}

func (c *ForgedCursorCoordinator) waitForPushCommit(ctx context.Context) error {
	waitContext, cancel := context.WithTimeout(ctx, forgedCursorPushWait)
	defer cancel()
	select {
	case <-waitContext.Done():
		if errors.Is(waitContext.Err(), context.DeadlineExceeded) {
			select {
			case <-c.pushCommitted:
				return c.proxyFailure("push")
			default:
			}
			return errForgedCursorPushWaitTimeout
		}
		return fmt.Errorf("wait for React Native forged-cursor push commit: %w", waitContext.Err())
	case <-c.pushCommitted:
	}
	return c.proxyFailure("push")
}

func (c *ForgedCursorCoordinator) waitForForgedPage(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native forged-cursor rejected page: %w", ctx.Err())
	case <-c.forgedPageReady:
	}
	return c.proxyFailure("forged page")
}

func (c *ForgedCursorCoordinator) proxyFailure(stage string) error {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.proxyErr != nil {
		return fmt.Errorf("React Native forged-cursor %s proxy failed: %w rebuild_responses=%s", stage, c.proxyErr, c.rebuildResponseDiagnosticLocked())
	}
	return nil
}

func (c *ForgedCursorCoordinator) rebuildResponseDiagnosticLocked() string {
	pages := make([]string, 0, len(c.rebuildResponses))
	for index, response := range c.rebuildResponses {
		if !response.observed {
			continue
		}
		pages = append(pages, fmt.Sprintf("{request:%d upstream_status:%d proxied_status:%d upstream_body:%q proxied_body:%q}", index+1, response.upstreamStatus, response.proxiedStatus, response.upstreamBody, response.proxiedBody))
	}
	return "[" + strings.Join(pages, " ") + "]"
}

func (c *ForgedCursorCoordinator) bindRebuildRequestLimit(raw []byte, requestNumber uint64) ([]byte, error) {
	stepID := forgedCursorStepOrder[4]
	if requestNumber == 2 {
		stepID = forgedCursorStepOrder[5]
	}
	var authored struct {
		Limit uint64 `json:"limit"`
	}
	if err := json.Unmarshal(c.steps[stepID].Operation.Payload, &authored); err != nil {
		return nil, fmt.Errorf("decode React Native forged-cursor authored rebuild limit for %s: %w", stepID, err)
	}
	if authored.Limit == 0 {
		return nil, fmt.Errorf("React Native forged-cursor authored rebuild limit for %s is zero", stepID)
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil {
		return nil, fmt.Errorf("decode React Native forged-cursor rebuild request: %w", err)
	}
	var observed uint64
	if err := json.Unmarshal(members["limit"], &observed); err != nil {
		return nil, fmt.Errorf("decode React Native forged-cursor rebuild request limit: %w", err)
	}
	if observed == 0 {
		return nil, errors.New("React Native forged-cursor rebuild request limit is zero")
	}
	encoded, err := json.Marshal(authored.Limit)
	if err != nil {
		return nil, fmt.Errorf("encode React Native forged-cursor authored rebuild limit: %w", err)
	}
	members["limit"] = encoded
	bound, err := json.Marshal(members)
	if err != nil {
		return nil, fmt.Errorf("encode React Native forged-cursor bound rebuild request: %w", err)
	}
	return bound, nil
}

func mutateForgedCursorFirstResponse(raw []byte) ([]byte, error) {
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) < 3 || len(members) > 6 {
		return nil, fmt.Errorf("React Native forged-cursor first response members=%d: %w", len(members), err)
	}
	var records []json.RawMessage
	var hasMore bool
	if err := json.Unmarshal(members["records"], &records); err != nil {
		return nil, fmt.Errorf("decode React Native forged-cursor first response records: %w", err)
	}
	if len(records) != 1 {
		return nil, fmt.Errorf("React Native forged-cursor first response record count=%d, want 1", len(records))
	}
	if err := json.Unmarshal(members["has_more"], &hasMore); err != nil || !hasMore {
		return nil, fmt.Errorf("React Native forged-cursor first response has_more=%t, want true: %w", hasMore, err)
	}
	var cursor string
	if err := json.Unmarshal(members["cursor"], &cursor); err != nil || cursor == "" {
		return nil, fmt.Errorf("React Native forged-cursor first response cursor present=%t nonempty=%t: %w", members["cursor"] != nil, cursor != "", err)
	}
	for _, name := range []string{"final_scope_cursor", "checksum"} {
		if value, found := members[name]; found && !isJSONNull(value) {
			return nil, fmt.Errorf("React Native forged-cursor first response %s=%s, want absent or null", name, boundedRaw(value))
		}
	}
	override, err := json.Marshal(forgedCursorOverride)
	if err != nil {
		return nil, fmt.Errorf("encode React Native forged-cursor deterministic override: %w", err)
	}
	members["cursor"] = override
	mutated, err := json.Marshal(members)
	if err != nil {
		return nil, fmt.Errorf("encode React Native forged-cursor first response: %w", err)
	}
	return mutated, nil
}

func (c *ForgedCursorCoordinator) acceptLocked(raw json.RawMessage) error {
	if c.stage == forgedCursorStageOpen {
		if !isJSONNull(raw) {
			return fmt.Errorf("React Native forged-cursor opening result = %s, want null", boundedRaw(raw))
		}
		return nil
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil {
		return fmt.Errorf("decode React Native forged-cursor result envelope %s: %w", boundedRaw(raw), err)
	}
	if c.stage == forgedCursorStagePushTimeoutDiagnostic {
		return c.pushTimeoutDiagnostic(envelope)
	}
	if envelope.Outcome != "passed" {
		return fmt.Errorf("React Native forged-cursor command outcome=%q error_code=%v error_detail=%v, want passed", envelope.Outcome, envelope.ErrorCode, envelope.ErrorDetail)
	}
	switch c.stage {
	case forgedCursorStageOpened:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return fmt.Errorf("validate React Native forged-cursor opened result %s: %w", boundedRaw(envelope.Result), err)
		}
		c.process = &process
	case forgedCursorStageFirstWrite, forgedCursorStageSecondWrite:
		if err := c.validateLocalResult(envelope.Result); err != nil {
			return err
		}
	case forgedCursorStageCallBegun:
		if err := c.validateCallBegun(envelope.Result); err != nil {
			return err
		}
	case forgedCursorStageFirstPage, forgedCursorStageForgedPage:
		if err := c.validateAwaited(envelope.Result); err != nil {
			return err
		}
	case forgedCursorStageCallComplete:
		if err := c.validateCallComplete(envelope.Result); err != nil {
			return err
		}
	case forgedCursorStageCapture:
		capture, err := decodeCapture(envelope.Result, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof"})
		if err != nil {
			return fmt.Errorf("decode React Native forged-cursor capture %s: %w", boundedRaw(envelope.Result), err)
		}
		c.finalCapture = &capture
	default:
		return fmt.Errorf("React Native forged-cursor accepted result at stage=%d", c.stage)
	}
	return nil
}

func (c *ForgedCursorCoordinator) pushTimeoutDiagnostic(envelope resultEnvelope) error {
	if envelope.Outcome == "error" {
		detail := "<none>"
		if envelope.ErrorDetail != nil {
			detail = boundedRaw(json.RawMessage(strconv.Quote(*envelope.ErrorDetail)))
		}
		code := "<none>"
		if envelope.ErrorCode != nil {
			code = *envelope.ErrorCode
		}
		return fmt.Errorf("React Native forged-cursor push did not reach the proxy within %s: in-flight call ID=%q completion=unavailable status=unavailable error_code=%q error_detail=%s", forgedCursorPushWait, c.callID, code, detail)
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(envelope.Result, &members); err != nil {
		return fmt.Errorf("React Native forged-cursor push did not reach the proxy within %s: in-flight call ID=%q completion=unavailable status=unavailable error_code=<none> error_detail=<none> result_error=%v", forgedCursorPushWait, c.callID, err)
	}
	if resultErr := validateActionResult(envelope.Result, "call-completed"); len(members) != 6 || resultErr != nil {
		return fmt.Errorf("React Native forged-cursor push did not reach the proxy within %s: in-flight call ID=%q completion=unavailable status=unavailable error_code=<none> error_detail=<none> result_members=%d want=6 result_error=%v", forgedCursorPushWait, c.callID, len(members), resultErr)
	}
	var callID, state, completion string
	callErr := json.Unmarshal(members["call_id"], &callID)
	stateErr := json.Unmarshal(members["state"], &state)
	completionErr := json.Unmarshal(members["completion"], &completion)
	status := boundedRaw(members["status"])
	if callErr != nil || stateErr != nil || completionErr != nil || callID != c.callID || state != "completed" || validateSyncStatusShape(members["status"]) != nil {
		return fmt.Errorf("React Native forged-cursor push did not reach the proxy within %s: in-flight call ID=%q completion=%q status=%s error_code=<none> error_detail=<none> result_errors=%v/%v/%v", forgedCursorPushWait, c.callID, completion, status, callErr, stateErr, completionErr)
	}
	return fmt.Errorf("React Native forged-cursor push did not reach the proxy within %s: in-flight call ID=%q completion=%q status=%s error_code=<none> error_detail=<none>", forgedCursorPushWait, callID, completion, status)
}

func boundedRaw(raw json.RawMessage) string {
	const maximum = 512
	if len(raw) <= maximum {
		return string(raw)
	}
	return string(raw[:maximum]) + fmt.Sprintf("...(%d bytes)", len(raw))
}

func (c *ForgedCursorCoordinator) advanceLocked(ctx context.Context, sequence uint64) (forgedCursorExchangeResponse, error) {
	response := forgedCursorExchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case forgedCursorStageOpen:
		response.Command = c.command("client", "open", map[string]any{"client_key": c.clientKey, "database_mode": "reuse", "initialization": "seed", "seed_step_id": string(forgedCursorStepOrder[0])}, nil)
		c.stage = forgedCursorStageOpened
	case forgedCursorStageOpened:
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{forgedCursorStepOrder[0]})
		c.stage = forgedCursorStageFirstWrite
	case forgedCursorStageFirstWrite:
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{forgedCursorStepOrder[1]})
		c.stage = forgedCursorStageSecondWrite
	case forgedCursorStageSecondWrite:
		binding := c.steps[forgedCursorStepOrder[2]].NativeBinding
		response.Command = c.command("client", "begin-call", map[string]any{"client_key": c.clientKey, "call_id": c.callID, "method": binding.Method}, []scenarios.StepID{forgedCursorStepOrder[2]})
		c.stage = forgedCursorStageCallBegun
	case forgedCursorStageCallBegun:
		if err := c.waitForPushCommit(ctx); err != nil {
			if errors.Is(err, errForgedCursorPushWaitTimeout) {
				// Release a late push. Otherwise the diagnostic would describe the
				// coordinator barrier instead of the client call.
				c.releasePushResponse()
				response.Command = c.command("client", "await-call", map[string]any{
					"client_key": c.clientKey, "call_id": c.callID,
				}, nil)
				c.stage = forgedCursorStagePushTimeoutDiagnostic
				return response, nil
			}
			return forgedCursorExchangeResponse{}, err
		}
		if err := c.bindAndMaterializePush(ctx); err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		c.releasePushResponse()
		response.Command = c.command("observer", "await-step", map[string]any{"client_key": c.clientKey, "call_id": c.callID}, []scenarios.StepID{forgedCursorStepOrder[4]})
		c.stage = forgedCursorStageFirstPage
	case forgedCursorStageFirstPage:
		if err := c.waitForFirstPage(ctx); err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		before, err := c.captureServer(ctx)
		if err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		if err := validateForgedCursorServerPage(c.expected, before); err != nil {
			return forgedCursorExchangeResponse{}, fmt.Errorf("validate React Native forged-cursor pre-rejection server state: %w", err)
		}
		c.serverBefore = &before
		c.releaseForgedPage()
		response.Command = c.command("observer", "await-step", map[string]any{"client_key": c.clientKey, "call_id": c.callID}, []scenarios.StepID{forgedCursorStepOrder[5]})
		c.stage = forgedCursorStageForgedPage
	case forgedCursorStageForgedPage:
		if err := c.waitForForgedPage(ctx); err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		after, err := c.captureServer(ctx)
		if err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		if c.serverBefore == nil {
			return forgedCursorExchangeResponse{}, errors.New("React Native forged-cursor pre-rejection server state is nil")
		}
		if err := validateForgedCursorServerFreeze(*c.serverBefore, after); err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		c.serverAfter = &after
		response.Command = c.command("client", "await-call", map[string]any{"client_key": c.clientKey, "call_id": c.callID, "completion": "error"}, nil)
		c.stage = forgedCursorStageCallComplete
	case forgedCursorStageCallComplete:
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys":            []string{c.clientKey},
			"sources":                []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"},
			"durable_proof_identity": map[string]any{"table_name": c.tableName, "record_id": "forged-cursor-absent-row"},
		}, nil)
		c.stage = forgedCursorStageCapture
	case forgedCursorStageCapture:
		if err := c.finishLocked(); err != nil {
			return forgedCursorExchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.stage = forgedCursorStageComplete
		c.completed = true
	default:
		return forgedCursorExchangeResponse{}, fmt.Errorf("React Native forged-cursor advance stage=%d", c.stage)
	}
	return response, nil
}

func (c *ForgedCursorCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *forgedCursorCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, stepID := range stepIDs {
		step := c.steps[stepID]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: step.Operation.ContractOperation, Name: step.Operation.Name, Payload: copyRaw(step.Operation.Payload)}})
	}
	return &forgedCursorCommand{
		SchemaVersion: 1,
		Action:        conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps},
		Runtime: forgedCursorRuntime{
			ClientKey: c.clientKey, Database: c.database, ClientID: c.serverClient.ClientID, SeedDatabasePath: forgedCursorSeedAsset,
			ServerURL: c.adapter, AuthToken: c.authTokens[c.clientKey],
		},
	}
}

func (c *ForgedCursorCoordinator) bindAndMaterializePush(ctx context.Context) error {
	push := c.steps[forgedCursorStepOrder[2]].Operation
	if err := c.config.Controller.BindApplicationPush(push); err != nil {
		return fmt.Errorf("bind React Native forged-cursor push %s: %w", forgedCursorStepOrder[2], err)
	}
	materialize := c.steps[forgedCursorStepOrder[3]]
	result, err := c.config.Controller.ProcessStep(ctx, nil, materialize.Operation)
	if err != nil || result.Disposition != materialize.ExpectedOutcome.Disposition {
		return fmt.Errorf("materialize React Native forged-cursor step %s: disposition=%q want=%q error=%v", materialize.ID, result.Disposition, materialize.ExpectedOutcome.Disposition, err)
	}
	return nil
}

func (c *ForgedCursorCoordinator) captureServer(ctx context.Context) (scenarios.StateFacts, error) {
	captures, err := c.config.Controller.Capture(ctx, []string{c.clientKey}, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("capture React Native forged-cursor server state: captures=%d want=1 error=%v", len(captures), err)
	}
	return captures[0].StateFacts, nil
}

func (c *ForgedCursorCoordinator) validateLocalResult(raw json.RawMessage) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return fmt.Errorf("validate React Native forged-cursor local result %s: %w", boundedRaw(raw), err)
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "forged-cursor local result"); err != nil {
		return fmt.Errorf("decode React Native forged-cursor local result %s: %w", boundedRaw(raw), err)
	}
	var rows uint64
	if err := json.Unmarshal(members["rows_affected"], &rows); err != nil || rows != 1 {
		return fmt.Errorf("React Native forged-cursor local rows_affected=%d want=1 error=%v", rows, err)
	}
	return c.validateProcess(members["process"], "local")
}

func (c *ForgedCursorCoordinator) validateCallBegun(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-begun"); err != nil {
		return fmt.Errorf("validate React Native forged-cursor call-begun result %s: %w", boundedRaw(raw), err)
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "forged-cursor call-begun result"); err != nil {
		return fmt.Errorf("decode React Native forged-cursor call-begun result %s: %w", boundedRaw(raw), err)
	}
	var callID, state string
	callErr := json.Unmarshal(members["call_id"], &callID)
	stateErr := json.Unmarshal(members["state"], &state)
	if callErr != nil || stateErr != nil || callID != c.callID || state != "in_flight" {
		return fmt.Errorf("React Native forged-cursor begun call_id=%q want=%q state=%q want=in_flight errors=%v/%v", callID, c.callID, state, callErr, stateErr)
	}
	return c.validateProcess(members["process"], "call-begun")
}

func (c *ForgedCursorCoordinator) validateAwaited(raw json.RawMessage) error {
	if err := validateActionResult(raw, "awaited"); err != nil {
		return fmt.Errorf("validate React Native forged-cursor awaited result %s: %w", boundedRaw(raw), err)
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "forged-cursor awaited result"); err != nil {
		return fmt.Errorf("decode React Native forged-cursor awaited result %s: %w", boundedRaw(raw), err)
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return fmt.Errorf("React Native forged-cursor awaited status=%s: %w", boundedRaw(members["status"]), err)
	}
	return c.validateProcess(members["process"], "awaited")
}

func (c *ForgedCursorCoordinator) validateCallComplete(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-completed"); err != nil {
		return fmt.Errorf("validate React Native forged-cursor call-completed result %s: %w", boundedRaw(raw), err)
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 6, "forged-cursor call-completed result"); err != nil {
		return fmt.Errorf("decode React Native forged-cursor call-completed result %s: %w", boundedRaw(raw), err)
	}
	var callID, state, completion string
	callErr := json.Unmarshal(members["call_id"], &callID)
	stateErr := json.Unmarshal(members["state"], &state)
	completionErr := json.Unmarshal(members["completion"], &completion)
	if callErr != nil || stateErr != nil || completionErr != nil || callID != c.callID || state != "completed" || completion != "error" {
		return fmt.Errorf("React Native forged-cursor completed call_id=%q want=%q state=%q want=completed completion=%q want=error errors=%v/%v/%v", callID, c.callID, state, completion, callErr, stateErr, completionErr)
	}
	if err := validateForgedCursorErrorStatus(c.config.Scenario, members["status"]); err != nil {
		return fmt.Errorf("%w rebuild_responses=%s", err, c.rebuildResponseDiagnostic())
	}
	return c.validateProcess(members["process"], "call-completed")
}

func (c *ForgedCursorCoordinator) validateProcess(raw json.RawMessage, stage string) error {
	process, err := decodeActionProcessIdentity(raw)
	if err != nil || c.process == nil || process != *c.process {
		return fmt.Errorf("React Native forged-cursor %s process=%+v initial=%+v error=%v", stage, process, c.process, err)
	}
	return nil
}

func validateForgedCursorErrorStatus(scenario scenarios.Scenario, raw json.RawMessage) error {
	wire, err := forgedCursorWireExpectation(scenario, forgedCursorStepOrder[5])
	if err != nil {
		return err
	}
	var status syncStatus
	var statusMembers map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &statusMembers, 4, "forged-cursor error status"); err != nil || json.Unmarshal(raw, &status) != nil {
		return fmt.Errorf("React Native forged-cursor error status=%s members=%d decode_error=%v", boundedRaw(raw), len(statusMembers), err)
	}
	var failure struct {
		Operation      string `json:"operation"`
		Code           string `json:"code"`
		Retryable      bool   `json:"retryable"`
		RecoveryAction string `json:"recovery_action"`
	}
	var failureMembers map[string]json.RawMessage
	failureErr := decodeStrictMembers(status.Failure, &failureMembers, 4, "forged-cursor failure")
	decodeErr := json.Unmarshal(status.Failure, &failure)
	wantCode := ""
	if wire.ErrorCode != nil {
		wantCode = *wire.ErrorCode
	}
	if status.State != "error" || !isJSONNull(status.RetryAt) || failureErr != nil || decodeErr != nil || failure.Operation != "rebuild" || failure.Code != wantCode || failure.Retryable != wire.Retryable || failure.RecoveryAction == "" {
		return fmt.Errorf("React Native forged-cursor status state=%q retry_at=%s failure={operation:%q code:%q retryable:%t recovery_action:%q} want={state:error retry_at:null operation:rebuild code:%q retryable:%t nonempty_recovery:true} errors=%v/%v", status.State, boundedRaw(status.RetryAt), failure.Operation, failure.Code, failure.Retryable, failure.RecoveryAction, wantCode, wire.Retryable, failureErr, decodeErr)
	}
	return nil
}

func validateForgedCursorServerPage(expected *scenarios.StateFacts, actual scenarios.StateFacts) error {
	if expected == nil || len(expected.Rebuilds) != 1 {
		return fmt.Errorf("React Native forged-cursor authored rebuild details=%d, want 1", rebuildDetailCount(expected))
	}
	if actual.RebuildCount == nil || *actual.RebuildCount != 1 || len(actual.Rebuilds) != 1 {
		return fmt.Errorf("React Native forged-cursor server rebuild count pointer=%v details=%d, want 1/1", actual.RebuildCount, len(actual.Rebuilds))
	}
	want, got := expected.Rebuilds[0], actual.Rebuilds[0]
	if got.RebuildID == "" || got.UserID != want.UserID || got.ClientID != want.ClientID || got.ScopeID != want.ScopeID || got.PageLimit != want.PageLimit || got.StagedRowCount != want.StagedRowCount || got.PageCount != want.PageCount || got.NextRowOrdinal != want.NextRowOrdinal || got.HasContinuation != want.HasContinuation || got.HasFinalCursor != want.HasFinalCursor || got.Status != want.Status {
		return fmt.Errorf("React Native forged-cursor server rebuild={user:%q client:%q scope:%q id:%q limit:%d rows:%d pages:%d next:%d continuation:%t final:%t status:%q}, want={user:%q client:%q scope:%q nonempty_id:true limit:%d rows:%d pages:%d next:%d continuation:%t final:%t status:%q}", got.UserID, got.ClientID, got.ScopeID, got.RebuildID, got.PageLimit, got.StagedRowCount, got.PageCount, got.NextRowOrdinal, got.HasContinuation, got.HasFinalCursor, got.Status, want.UserID, want.ClientID, want.ScopeID, want.PageLimit, want.StagedRowCount, want.PageCount, want.NextRowOrdinal, want.HasContinuation, want.HasFinalCursor, want.Status)
	}
	return nil
}

func rebuildDetailCount(facts *scenarios.StateFacts) int {
	if facts == nil {
		return 0
	}
	return len(facts.Rebuilds)
}

func validateForgedCursorServerFreeze(before, after scenarios.StateFacts) error {
	normalizedBefore, beforeErr := scenarios.NormalizeStateFacts(before)
	normalizedAfter, afterErr := scenarios.NormalizeStateFacts(after)
	if beforeErr != nil || afterErr != nil {
		return fmt.Errorf("normalize React Native forged-cursor freeze: before_error=%v after_error=%v", beforeErr, afterErr)
	}
	if !reflect.DeepEqual(normalizedBefore, normalizedAfter) {
		beforeJSON, _ := json.Marshal(normalizedBefore)
		afterJSON, _ := json.Marshal(normalizedAfter)
		return fmt.Errorf("React Native forged-cursor server state changed: before=%s after=%s", boundedRaw(beforeJSON), boundedRaw(afterJSON))
	}
	return nil
}

func (c *ForgedCursorCoordinator) finishLocked() error {
	if c.finalCapture == nil || c.serverAfter == nil {
		return fmt.Errorf("React Native forged-cursor final evidence: capture nil=%t server nil=%t", c.finalCapture == nil, c.serverAfter == nil)
	}
	if err := c.bindFinalServerIdentities(); err != nil {
		return err
	}
	if err := validateForgedCursorFinalCapture(c.config.Scenario, *c.finalCapture, *c.serverAfter, c.runtimeIDs, c.serverClient.ClientGeneration); err != nil {
		return err
	}
	expected := scenarios.CloneStateFacts(*c.expected)
	expected.Rebuilds[0].RebuildID = c.serverAfter.Rebuilds[0].RebuildID
	if err := validateServerState(expected, *c.serverAfter); err != nil {
		return fmt.Errorf("React Native forged-cursor server projection differs: rebuilds=%d expected=%d error=%w", len(c.serverAfter.Rebuilds), len(expected.Rebuilds), err)
	}
	resolutions, err := resolveForgedCursorServerIdentities(c.identities, c.runtimeIDs)
	if err != nil {
		return err
	}
	c.result = ForgedCursorCoordinatorResult{ServerFacts: *c.serverAfter, IdentityResolution: resolutions}
	return nil
}

func (c *ForgedCursorCoordinator) bindFinalServerIdentities() error {
	values, err := c.config.Controller.IdentityValues(c.identities)
	if err != nil {
		return fmt.Errorf("resolve React Native forged-cursor final server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
	}
	if len(c.serverAfter.Rebuilds) != 1 {
		return fmt.Errorf("React Native forged-cursor final server rebuild details=%d, want 1", len(c.serverAfter.Rebuilds))
	}
	serverValues := map[string]any{
		"client-generation-one": c.serverClient.ClientGeneration,
		"forged-rebuild":        c.serverAfter.Rebuilds[0].RebuildID,
	}
	for alias, value := range serverValues {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode React Native forged-cursor server alias %q value=%v: %w", alias, value, err)
		}
		c.runtimeIDs[alias] = encoded
	}
	missing := make([]string, 0)
	for _, alias := range forgedCursorAliasNames {
		if len(c.runtimeIDs[alias]) == 0 {
			missing = append(missing, alias)
		}
	}
	if len(missing) != 0 {
		return fmt.Errorf("React Native forged-cursor server identity aliases missing=%v, want none", missing)
	}
	return nil
}

func resolveForgedCursorServerIdentities(aliases []scenarios.NativeIdentityAlias, runtime map[string]json.RawMessage) ([]blackbox.NativeIdentityResolution, error) {
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range aliases {
		value := runtime[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("React Native forged-cursor server alias %q runtime bytes=%d, want nonzero", alias.Alias, len(value))
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
	resolutions, err := blackbox.ResolveNativeIdentityAliases(aliases, observations)
	if err != nil {
		return nil, fmt.Errorf("resolve React Native forged-cursor server aliases: observations=%d aliases=%d error=%w", len(observations), len(aliases), err)
	}
	return resolutions, nil
}

func validateForgedCursorFinalCapture(scenario scenarios.Scenario, capture finalCapture, server scenarios.StateFacts, runtime map[string]json.RawMessage, serverGeneration uint64) error {
	if err := validateEmptyArray(capture.Pending); err != nil {
		return fmt.Errorf("React Native forged-cursor pending mutations=%s, want []: %w", boundedRaw(capture.Pending), err)
	}
	if err := validateEmptyArray(capture.Rejected); err != nil {
		return fmt.Errorf("React Native forged-cursor rejected mutations=%s, want []: %w", boundedRaw(capture.Rejected), err)
	}
	if err := validateForgedCursorErrorStatus(scenario, capture.Status); err != nil {
		return err
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return fmt.Errorf("decode React Native forged-cursor client state %s: %w", boundedRaw(capture.ClientState), err)
	}
	if len(server.Rebuilds) != 1 {
		return fmt.Errorf("React Native forged-cursor server rebuild details=%d, want 1", len(server.Rebuilds))
	}
	var schema clientSchema
	var scope string
	schemaErr := json.Unmarshal(runtime["current-schema"], &schema)
	scopeErr := json.Unmarshal(runtime["scope-a"], &scope)
	if schemaErr != nil || scopeErr != nil || schema.Version == 0 || schema.Hash == "" || scope == "" {
		return fmt.Errorf("React Native forged-cursor server schema=%+v scope=%q errors=%v/%v", schema, scope, schemaErr, scopeErr)
	}
	if state.Schema == nil || *state.Schema != schema || state.RebuildAttemptCount != 1 || len(state.RebuildAttempts) != 1 || state.RebuildReceiptCount != 1 {
		return fmt.Errorf("React Native forged-cursor client schema=%+v want=%+v active_rebuild_count=%d details=%d receipts=%d, want schema match and 1/1/1", state.Schema, schema, state.RebuildAttemptCount, len(state.RebuildAttempts), state.RebuildReceiptCount)
	}
	attempt := state.RebuildAttempts[0]
	wantRebuild := server.Rebuilds[0].RebuildID
	// A rebuild attempt may hold no cursor, so name that case rather than
	// dereferencing it for the failure message.
	observedCursor := "none"
	if attempt.Cursor != nil {
		observedCursor = hashFingerprint(*attempt.Cursor)
	}
	if attempt.ScopeID != scope || attempt.RebuildID != wantRebuild || attempt.ClientGeneration != serverGeneration || attempt.SchemaVersion != schema.Version || attempt.SchemaHash != schema.Hash || attempt.Cursor == nil || hashFingerprint(*attempt.Cursor) != hashFingerprint(forgedCursorOverride) || attempt.PageLimit != server.Rebuilds[0].PageLimit || attempt.Generation == 0 {
		return fmt.Errorf("React Native forged-cursor active rebuild={scope:%q id:%q generation:%d schema:%d/%q cursor_fingerprint:%q limit:%d scope_generation:%d}, want={scope:%q id:%q generation:%d schema:%d/%q cursor_fingerprint:%q limit:%d nonzero_scope_generation:true}", attempt.ScopeID, attempt.RebuildID, attempt.ClientGeneration, attempt.SchemaVersion, attempt.SchemaHash, observedCursor, attempt.PageLimit, attempt.Generation, scope, wantRebuild, serverGeneration, schema.Version, schema.Hash, hashFingerprint(forgedCursorOverride), server.Rebuilds[0].PageLimit)
	}
	proof, err := decodeDurableProof(capture.DurableProof)
	if err != nil || proof.RowMetadata != nil || len(proof.RebuildReceiptProofs) != 1 {
		return fmt.Errorf("React Native forged-cursor durable proof row_metadata=%+v receipts=%d want nil/1 error=%v", proof.RowMetadata, len(proof.RebuildReceiptProofs), err)
	}
	receipt := proof.RebuildReceiptProofs[0]
	if receipt.RebuildIDFingerprint != hashFingerprint(wantRebuild) || receipt.PageCount != server.Rebuilds[0].PageCount || receipt.ReturnedRecordCount != server.Rebuilds[0].PageCount || !receipt.RequestChainValid || !receipt.RecordsInCanonicalOrder || !receipt.RowChecksumsValid || receipt.ScopeChecksumValid || receipt.FinalChecksumMatches {
		return fmt.Errorf("React Native forged-cursor receipt={id:%q pages:%d records:%d chain:%t order:%t rows:%t scope:%t final:%t}, want={id:%q pages:%d records:%d chain:true order:true rows:true scope:false final:false}", receipt.RebuildIDFingerprint, receipt.PageCount, receipt.ReturnedRecordCount, receipt.RequestChainValid, receipt.RecordsInCanonicalOrder, receipt.RowChecksumsValid, receipt.ScopeChecksumValid, receipt.FinalChecksumMatches, hashFingerprint(wantRebuild), server.Rebuilds[0].PageCount, server.Rebuilds[0].PageCount)
	}
	distinct, err := rebuildAttemptFactCount(state.RebuildAttempts, proof.RebuildReceiptProofs)
	if err != nil || distinct != 1 {
		return fmt.Errorf("React Native forged-cursor distinct rebuild identities=%d want=1 active=%d receipts=%d error=%v", distinct, len(state.RebuildAttempts), len(proof.RebuildReceiptProofs), err)
	}
	if err := validateForgedCursorEvents(capture.Events, scope, wantRebuild); err != nil {
		return err
	}
	return validateForgedCursorTrace(scenario, capture.Trace, server, runtime, serverGeneration)
}

func validateForgedCursorEvents(raw json.RawMessage, scope, rebuildID string) error {
	var events []map[string]json.RawMessage
	if err := decodeStrictValue(raw, &events); err != nil {
		return fmt.Errorf("decode React Native forged-cursor events %s: %w", boundedRaw(raw), err)
	}
	requested, completed := 0, 0
	observed := make([]string, 0)
	for _, event := range events {
		var kind string
		_ = json.Unmarshal(event["type"], &kind)
		if kind != "rebuild_requested" && kind != "rebuild_completed" {
			continue
		}
		var eventScope, eventRebuild string
		_ = json.Unmarshal(event["scope_id"], &eventScope)
		_ = json.Unmarshal(event["rebuild_id"], &eventRebuild)
		observed = append(observed, kind+":"+eventScope+":"+eventRebuild)
		if eventScope == scope && eventRebuild == rebuildID {
			if kind == "rebuild_requested" {
				requested++
			} else {
				completed++
			}
		}
	}
	if requested != 1 || completed != 0 {
		return fmt.Errorf("React Native forged-cursor events requested=%d completed=%d want=1/0 observed=%v", requested, completed, observed)
	}
	return nil
}

func validateForgedCursorTrace(scenario scenarios.Scenario, raw json.RawMessage, server scenarios.StateFacts, runtime map[string]json.RawMessage, serverGeneration uint64) error {
	trace, err := captureTraceFromRaw(raw)
	if err != nil {
		return err
	}
	wantClasses := []string{"connect", "push", "rebuild", "rebuild"}
	if trace.Overflowed || len(trace.Observations) != len(wantClasses) || trace.SequenceCheckpoint != uint64(len(trace.Observations)) {
		classes := make([]string, 0, len(trace.Observations))
		for _, observation := range trace.Observations {
			classes = append(classes, fmt.Sprintf("%s/%d", observation.OperationClass, observation.StatusCode))
		}
		return fmt.Errorf("React Native forged-cursor trace observations=%v count=%d want=%d checkpoint=%d want=%d overflowed=%t", classes, len(trace.Observations), len(wantClasses), trace.SequenceCheckpoint, len(trace.Observations), trace.Overflowed)
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return fmt.Errorf("React Native forged-cursor trace sequences=%v: %w", forgedCursorTraceSequences(trace), err)
	}
	wires := make(map[scenarios.StepID]scenarios.WireExpectation)
	for _, stepID := range []scenarios.StepID{forgedCursorStepOrder[2], forgedCursorStepOrder[4], forgedCursorStepOrder[5]} {
		wire, wireErr := forgedCursorWireExpectation(scenario, stepID)
		if wireErr != nil {
			return wireErr
		}
		wires[stepID] = wire
	}
	wantStatuses := []int{http.StatusOK, wires[forgedCursorStepOrder[2]].HTTPStatus, wires[forgedCursorStepOrder[4]].HTTPStatus, wires[forgedCursorStepOrder[5]].HTTPStatus}
	for index, observation := range trace.Observations {
		if observation.OperationClass != wantClasses[index] || observation.StatusCode != wantStatuses[index] || observation.DurationNanoseconds == 0 || !hasJSONValue(observation.RequestFacts) {
			return fmt.Errorf("React Native forged-cursor trace %d={class:%q status:%d duration:%d request:%s}, want={class:%q status:%d positive_duration:true request_present:true}", index+1, observation.OperationClass, observation.StatusCode, observation.DurationNanoseconds, boundedRaw(observation.RequestFacts), wantClasses[index], wantStatuses[index])
		}
		if observation.CursorFingerprints != nil || observation.CursorFingerprintsComplete != nil {
			return fmt.Errorf("React Native forged-cursor trace %d top-level cursor fingerprints=%v complete=%v, want nil/nil", index+1, observation.CursorFingerprints, observation.CursorFingerprintsComplete)
		}
	}
	push, first, forged := trace.Observations[1], trace.Observations[2], trace.Observations[3]
	mutationCount, mutationErr := requestInteger(push, "mutation_count")
	pushGeneration, generationErr := requestInteger(push, "client_generation")
	if mutationErr != nil || generationErr != nil || mutationCount != 2 || pushGeneration != serverGeneration || hasJSONValue(push.RebuildResponseFacts) || hasJSONValue(push.PullResponseFacts) {
		return fmt.Errorf("React Native forged-cursor push mutation_count=%d want=2 generation=%d want=%d rebuild_facts=%s pull_facts=%s errors=%v/%v", mutationCount, pushGeneration, serverGeneration, boundedRaw(push.RebuildResponseFacts), boundedRaw(push.PullResponseFacts), mutationErr, generationErr)
	}
	if len(server.Rebuilds) != 1 {
		return fmt.Errorf("React Native forged-cursor trace server rebuild details=%d, want 1", len(server.Rebuilds))
	}
	var schema clientSchema
	var scope string
	schemaErr := json.Unmarshal(runtime["current-schema"], &schema)
	scopeErr := json.Unmarshal(runtime["scope-a"], &scope)
	if schemaErr != nil || scopeErr != nil {
		return fmt.Errorf("React Native forged-cursor trace server schema=%+v scope=%q errors=%v/%v", schema, scope, schemaErr, scopeErr)
	}
	requests := []transportObservation{first, forged}
	for index, observation := range requests {
		generation, generationErr := requestInteger(observation, "client_generation")
		version, versionErr := requestInteger(observation, "schema_version")
		hash, hashErr := requestString(observation, "schema_hash")
		scopeFingerprint, scopeFingerprintErr := requestString(observation, "scope_fingerprint")
		rebuildFingerprint, rebuildFingerprintErr := requestString(observation, "rebuild_id_fingerprint")
		limit, limitErr := requestInteger(observation, "limit")
		if generationErr != nil || versionErr != nil || hashErr != nil || scopeFingerprintErr != nil || rebuildFingerprintErr != nil || limitErr != nil || generation != serverGeneration || version != schema.Version || hash != schema.Hash || scopeFingerprint != hashFingerprint(scope) || rebuildFingerprint != hashFingerprint(server.Rebuilds[0].RebuildID) || limit != server.Rebuilds[0].PageLimit {
			return fmt.Errorf("React Native forged-cursor rebuild request %d={generation:%d schema:%d/%q scope:%q rebuild:%q limit:%d}, want={generation:%d schema:%d/%q scope:%q rebuild:%q limit:%d}, errors=%v/%v/%v/%v/%v/%v", index+1, generation, version, hash, scopeFingerprint, rebuildFingerprint, limit, serverGeneration, schema.Version, schema.Hash, hashFingerprint(scope), hashFingerprint(server.Rebuilds[0].RebuildID), server.Rebuilds[0].PageLimit, generationErr, versionErr, hashErr, scopeFingerprintErr, rebuildFingerprintErr, limitErr)
		}
	}
	firstCursorPresent, firstPresentErr := forgedCursorRequestBool(first, "cursor_present")
	firstCursor, firstCursorErr := requestStringOptional(first, "cursor_fingerprint")
	forgedCursorPresent, forgedPresentErr := forgedCursorRequestBool(forged, "cursor_present")
	forgedFingerprint, forgedFingerprintErr := requestString(forged, "cursor_fingerprint")
	if firstPresentErr != nil || firstCursorErr != nil || forgedPresentErr != nil || forgedFingerprintErr != nil || firstCursorPresent || firstCursor != "" || !forgedCursorPresent || forgedFingerprint != hashFingerprint(forgedCursorOverride) {
		return fmt.Errorf("React Native forged-cursor chain first={present:%t fingerprint:%q} forged={present:%t fingerprint:%q} want first=false/empty forged=true/%q errors=%v/%v/%v/%v", firstCursorPresent, firstCursor, forgedCursorPresent, forgedFingerprint, hashFingerprint(forgedCursorOverride), firstPresentErr, firstCursorErr, forgedPresentErr, forgedFingerprintErr)
	}
	facts, factsErr := decodeRebuildResponseFacts(first.RebuildResponseFacts)
	if factsErr != nil || facts.RecordCount == nil || facts.HasMore == nil || facts.HasCursor == nil || facts.HasFinalScopeCursor == nil || facts.HasChecksum == nil || facts.ScopeFingerprint == nil || *facts.RecordCount != 1 || !*facts.HasMore || !*facts.HasCursor || *facts.HasFinalScopeCursor || *facts.HasChecksum || *facts.ScopeFingerprint != hashFingerprint(scope) {
		return fmt.Errorf("React Native forged-cursor first response={records:%d more:%v cursor:%v final:%v checksum:%v scope:%v} want={records:1 more:true cursor:true final:false checksum:false scope:%q} error=%v", valueOrZero(facts.RecordCount), facts.HasMore, facts.HasCursor, facts.HasFinalScopeCursor, facts.HasChecksum, facts.ScopeFingerprint, hashFingerprint(scope), factsErr)
	}
	if hasJSONValue(forged.RebuildResponseFacts) || hasJSONValue(forged.PullResponseFacts) {
		return fmt.Errorf("React Native forged-cursor rejected response rebuild_facts=%s pull_facts=%s, want absent/absent", boundedRaw(forged.RebuildResponseFacts), boundedRaw(forged.PullResponseFacts))
	}
	return nil
}

func forgedCursorTraceSequences(trace traceSnapshot) []uint64 {
	values := make([]uint64, 0, len(trace.Observations))
	for _, observation := range trace.Observations {
		values = append(values, observation.Sequence)
	}
	return values
}

func forgedCursorRequestBool(observation transportObservation, name string) (bool, error) {
	var facts map[string]json.RawMessage
	if err := json.Unmarshal(observation.RequestFacts, &facts); err != nil {
		return false, fmt.Errorf("React Native forged-cursor request facts=%s: %w", boundedRaw(observation.RequestFacts), err)
	}
	raw, found := facts[name]
	var value bool
	if !found || json.Unmarshal(raw, &value) != nil {
		return false, fmt.Errorf("React Native forged-cursor request fact %q present=%t value=%s", name, found, boundedRaw(raw))
	}
	return value, nil
}

func forgedCursorServerClientFromScenario(scenario scenarios.Scenario) (forgedCursorServerClient, error) {
	if len(scenario.Model.Setup) != 1 {
		return forgedCursorServerClient{}, fmt.Errorf("React Native forged-cursor server setup count=%d, want 1", len(scenario.Model.Setup))
	}
	var setup struct {
		Clients []struct {
			UserID           string `json:"user_id"`
			ClientID         string `json:"client_id"`
			ClientGeneration uint64 `json:"client_generation"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(scenario.Model.Setup[0].Payload, &setup); err != nil {
		return forgedCursorServerClient{}, fmt.Errorf("decode React Native forged-cursor server clients from setup: %w", err)
	}
	if len(setup.Clients) != 1 || setup.Clients[0].UserID == "" || setup.Clients[0].ClientID == "" || setup.Clients[0].ClientGeneration == 0 {
		return forgedCursorServerClient{}, fmt.Errorf("React Native forged-cursor server clients=%+v, want one client with nonempty user/client and positive generation", setup.Clients)
	}
	return forgedCursorServerClient(setup.Clients[0]), nil
}

func forgedCursorWireExpectation(scenario scenarios.Scenario, stepID scenarios.StepID) (scenarios.WireExpectation, error) {
	matches := make([]scenarios.WireExpectation, 0, 1)
	for _, expectation := range scenario.WireExpectations {
		if expectation.StepID == stepID {
			matches = append(matches, expectation)
		}
	}
	if len(matches) != 1 {
		return scenarios.WireExpectation{}, fmt.Errorf("React Native forged-cursor wire expectations for %s=%d, want 1", stepID, len(matches))
	}
	return matches[0], nil
}

func forgedCursorExpectedState(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		expected := scenario.Model.ExpectedState[index]
		if expected.ID == "EXPECT-REBUILD-FORGED-CURSOR-STATE-001" && expected.StateFacts != nil {
			return expected.StateFacts
		}
	}
	return nil
}
