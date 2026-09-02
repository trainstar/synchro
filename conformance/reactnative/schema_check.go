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
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	schemaCheckScenarioPath = "conformance/scenarios/performance/schema-check-001.json"
	schemaCheckScenarioID   = "SCN-PERF-SCHEMA-CHECK-001"
)

var schemaCheckAliasNames = []string{
	"schema-v1",
	"schema-v2",
	"schema-v3",
	"schema-v4",
	"scope-user-a",
	"scope-user-b",
	"client-generation-one",
	"scope-set-version-one",
	"items-table",
	"items-primary-key",
}

var schemaCheckStepOrder = []scenarios.StepID{
	"STEP-PERF-SCHEMA-CHECK-001",
	"STEP-PERF-SCHEMA-CHECK-002",
	"STEP-PERF-SCHEMA-CHECK-003",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-COMMIT-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-MATERIALIZE-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-STAGE-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-ACTIVATE-001",
	"STEP-PERF-SCHEMA-CHECK-004",
	"STEP-PERF-SCHEMA-CHECK-005",
	"STEP-PERF-SCHEMA-CHECK-006",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001",
	"STEP-PERF-SCHEMA-CHECK-007",
	"STEP-PERF-SCHEMA-CHECK-008",
	"STEP-PERF-SCHEMA-CHECK-009",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-003",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-003",
	"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-001",
	"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-002",
	"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS3-PUBLISH-001",
	"STEP-PERF-SCHEMA-CHECK-010",
	"STEP-PERF-SCHEMA-CHECK-011",
	"STEP-PERF-SCHEMA-CHECK-012",
	"STEP-PERF-SCHEMA-CHECK-013",
	"STEP-PERF-SCHEMA-CHECK-014",
	"STEP-PERF-SCHEMA-CHECK-015",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS4-PUBLISH-001",
	"STEP-PERF-SCHEMA-CHECK-016",
	"STEP-PERF-SCHEMA-CHECK-017",
	"STEP-PERF-SCHEMA-CHECK-018",
}

// SchemaCheckCoordinatorConfig configures one authenticated schema-check sidecar.
type SchemaCheckCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
}

// SchemaCheckCoordinatorResult contains server evidence and resolved identities.
type SchemaCheckCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type schemaCheckCall struct {
	step              scenarios.Step
	controllerSteps   []scenarios.Step
	clientKey         string
	sessionKey        string
	serverSchemaAlias string
}

type schemaCheckWaiting uint8

const (
	schemaCheckWaitingNone schemaCheckWaiting = iota
	schemaCheckWaitingOpen
	schemaCheckWaitingSync
	schemaCheckWaitingCapture
	schemaCheckWaitingLifecycle
)

// SchemaCheckCoordinator executes every authored schema dispatch call through React Native.
type SchemaCheckCoordinator struct {
	config SchemaCheckCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string

	calls      []schemaCheckCall
	boundaries map[scenarios.StepID]scenarios.NativeLifecycleBoundary
	databases  map[string]bool
	captures   map[scenarios.StepID]finalCapture
	authTokens map[string]string
	processes  map[string]actionProcessIdentity
	runtimeIDs map[string]json.RawMessage
	tableName  string
	primaryKey string

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	nextSeq   uint64
	current   int
	waiting   schemaCheckWaiting
	result    SchemaCheckCoordinatorResult
}

// LoadSchemaCheckScenario loads only the authored schema-check scenario.
func LoadSchemaCheckScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, schemaCheckScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native schema-check scenario: %w", err)
	}
	if err := ValidateSchemaCheckScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateSchemaCheckScenario rejects changes to the closed RN schema-check contract.
func ValidateSchemaCheckScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != schemaCheckScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native schema-check scenario contract is invalid")
	}
	if len(scenario.Steps) != len(schemaCheckStepOrder) {
		return fmt.Errorf("React Native schema-check steps=%d want=%d", len(scenario.Steps), len(schemaCheckStepOrder))
	}
	for index, step := range scenario.Steps {
		if step.ID != schemaCheckStepOrder[index] {
			return fmt.Errorf("React Native schema-check step index=%d id=%s want=%s", index, step.ID, schemaCheckStepOrder[index])
		}
	}
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		return err
	}
	if len(scenario.WireExpectations) != len(calls) {
		return fmt.Errorf("React Native schema-check wire expectations=%d public calls=%d", len(scenario.WireExpectations), len(calls))
	}
	// The authored scenario owns the call count. Derive it from the public
	// bindings rather than restating a number that can drift from the contract.
	publicSteps := 0
	for _, step := range scenario.Steps {
		if step.NativeBinding != nil && step.NativeBinding.Kind == "public-call" {
			publicSteps++
		}
	}
	if len(scenario.Steps) != len(schemaCheckStepOrder) || len(calls) != publicSteps {
		return fmt.Errorf("React Native schema-check steps=%d calls=%d, want %d steps and %d calls",
			len(scenario.Steps), len(calls), len(schemaCheckStepOrder), publicSteps)
	}
	if err := schemaCheckLifecycleBoundaries(scenario, calls); err != nil {
		return err
	}
	if err := schemaCheckAliases(scenario.NativeIdentityAliases); err != nil {
		return err
	}
	if err := schemaCheckAssertions(scenario); err != nil {
		return err
	}
	if err := schemaCheckProofObligations(scenario); err != nil {
		return err
	}
	plan, err := schemaCheckDispatchPlan(scenario)
	if err != nil {
		return err
	}
	strata, err := schemaCheckStrata(plan)
	if err != nil {
		return err
	}
	counts := make(map[string]uint64, len(plan.Strata))
	seenSamples := make(map[string]struct{})
	for _, call := range calls {
		step := call.step
		wire, wireErr := schemaCheckWireExpectation(scenario, step.ID)
		if wireErr != nil {
			return wireErr
		}
		if step.NativeBinding.Completion != schemaCheckCompletion(wire) {
			return fmt.Errorf("React Native schema-check step %s completion=%q wire_action=%q status=%d", step.ID, step.NativeBinding.Completion, wire.Action, wire.HTTPStatus)
		}
		if step.MeasurementSample == nil {
			continue
		}
		sample := step.MeasurementSample
		if sample.MeasurementID != plan.MeasurementID || sample.SampleID == "" || sample.Operation.Family != "schema-check" {
			return fmt.Errorf("React Native schema-check measurement step %s is invalid", step.ID)
		}
		if _, duplicate := seenSamples[sample.SampleID]; duplicate {
			return fmt.Errorf("React Native schema-check sample %q is duplicated", sample.SampleID)
		}
		caseName, caseErr := schemaCheckCase(step)
		operationCase, operationCaseErr := schemaCheckMeasurementOperationCase(*sample)
		wantCase, found := strata[string(sample.StratumID)]
		if caseErr != nil || operationCaseErr != nil || !found || caseName != wantCase || operationCase != wantCase {
			return fmt.Errorf("React Native schema-check sample step=%s stratum=%q parameter_case=%q operation_case=%q want_case=%q parameter_error=%v operation_error=%v", step.ID, sample.StratumID, caseName, operationCase, wantCase, caseErr, operationCaseErr)
		}
		seenSamples[sample.SampleID] = struct{}{}
		counts[string(sample.StratumID)]++
	}
	for _, stratum := range plan.Strata {
		if counts[string(stratum.StratumID)] != plan.MinimumSampleCountPerStratum {
			return fmt.Errorf("React Native schema-check stratum %s samples=%d want=%d", stratum.StratumID, counts[string(stratum.StratumID)], plan.MinimumSampleCountPerStratum)
		}
	}
	return nil
}

func schemaCheckCalls(scenario scenarios.Scenario) ([]schemaCheckCall, error) {
	if len(scenario.Steps) == 0 {
		return nil, errors.New("React Native schema-check steps are absent")
	}
	calls := make([]schemaCheckCall, 0, len(scenario.WireExpectations))
	pending := make([]scenarios.Step, 0, 5)
	serverSchema := "schema-v1"
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil || step.ExpectedOutcome.Disposition != "success" || scenarios.ValidateOperation(step.Operation) != nil {
			return nil, fmt.Errorf("React Native schema-check step %s is invalid", step.ID)
		}
		key := scenarios.OperationKey(step.Operation)
		switch key {
		case "connect/send":
			binding := step.NativeBinding
			if step.Transport != "http" || binding.Kind != "public-call" || binding.UserID == "" || binding.ClientID == "" ||
				binding.Stage != "synchronous" || binding.Method != "start" || binding.CallID == nil || *binding.CallID == "" {
				return nil, fmt.Errorf("React Native schema-check public step %s binding is invalid", step.ID)
			}
			var payload struct {
				UserID   string `json:"user_id"`
				ClientID string `json:"client_id"`
			}
			if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID {
				return nil, fmt.Errorf("React Native schema-check public step %s identity is invalid", step.ID)
			}
			calls = append(calls, schemaCheckCall{
				step:              step,
				controllerSteps:   append([]scenarios.Step(nil), pending...),
				clientKey:         schemaCheckClientKey(binding.UserID, binding.ClientID),
				sessionKey:        schemaCheckSessionKey(step.ID),
				serverSchemaAlias: serverSchema,
			})
			pending = pending[:0]
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/publish-schema":
			if step.NativeBinding.Kind != "controller" {
				return nil, fmt.Errorf("React Native schema-check controller step %s binding is invalid", step.ID)
			}
			pending = append(pending, step)
			if key == "model/publish-schema" {
				alias, err := schemaCheckPublishedSchemaAlias(step)
				if err != nil {
					return nil, err
				}
				serverSchema = alias
			}
		case "process/materialize-source-transaction":
			if step.NativeBinding.Kind != "controller" {
				return nil, fmt.Errorf("React Native schema-check process step %s binding is invalid", step.ID)
			}
			pending = append(pending, step)
		default:
			return nil, fmt.Errorf("React Native schema-check step %s operation %q is unsupported", step.ID, key)
		}
	}
	if len(pending) != 0 {
		return nil, errors.New("React Native schema-check ends with unapplied controller steps")
	}
	return calls, nil
}

func schemaCheckPublishedSchemaAlias(step scenarios.Step) (string, error) {
	var payload struct {
		Schema struct {
			Version uint64 `json:"version"`
			Hash    string `json:"hash"`
		} `json:"schema"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil {
		return "", fmt.Errorf("decode React Native schema-check published schema %s: %w", step.ID, err)
	}
	for index, hash := range []string{
		"2222222222222222222222222222222222222222222222222222222222222222",
		"3333333333333333333333333333333333333333333333333333333333333333",
		"4444444444444444444444444444444444444444444444444444444444444444",
	} {
		if payload.Schema.Version == uint64(index+2) && payload.Schema.Hash == hash {
			return fmt.Sprintf("schema-v%d", index+2), nil
		}
	}
	return "", fmt.Errorf("React Native schema-check published schema step %s has version=%d hash=%q", step.ID, payload.Schema.Version, payload.Schema.Hash)
}

func schemaCheckLifecycleBoundaries(scenario scenarios.Scenario, calls []schemaCheckCall) error {
	if len(scenario.NativeLifecycleBoundaries) != 18 {
		return fmt.Errorf("React Native schema-check lifecycle boundaries=%d want=18", len(scenario.NativeLifecycleBoundaries))
	}
	callByStep := make(map[scenarios.StepID]schemaCheckCall, len(calls))
	for _, call := range calls {
		callByStep[call.step.ID] = call
	}
	seen := make(map[scenarios.StepID]struct{}, len(scenario.NativeLifecycleBoundaries))
	for _, boundary := range scenario.NativeLifecycleBoundaries {
		call, found := callByStep[boundary.AfterStepID]
		if !found || boundary.ID == "" || boundary.Phase != "setup" || boundary.Method != "stop" ||
			boundary.UserID != call.step.NativeBinding.UserID || boundary.ClientID != call.step.NativeBinding.ClientID {
			return fmt.Errorf("React Native schema-check lifecycle boundary %q is invalid", boundary.ID)
		}
		if _, duplicate := seen[boundary.AfterStepID]; duplicate {
			return fmt.Errorf("React Native schema-check lifecycle step %s is duplicated", boundary.AfterStepID)
		}
		seen[boundary.AfterStepID] = struct{}{}
	}
	return nil
}

func schemaCheckAliases(aliases []scenarios.NativeIdentityAlias) error {
	if len(aliases) != len(schemaCheckAliasNames) {
		return fmt.Errorf("React Native schema-check aliases=%d want=%d", len(aliases), len(schemaCheckAliasNames))
	}
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if alias.Alias == "" {
			return errors.New("React Native schema-check alias is empty")
		}
		if _, duplicate := seen[alias.Alias]; duplicate {
			return fmt.Errorf("React Native schema-check alias %q is duplicated", alias.Alias)
		}
		seen[alias.Alias] = struct{}{}
	}
	for _, name := range schemaCheckAliasNames {
		if _, found := seen[name]; !found {
			return fmt.Errorf("React Native schema-check alias %q is absent", name)
		}
	}
	return nil
}

func schemaCheckAssertions(scenario scenarios.Scenario) error {
	semantic, dispatch, performance := false, false, false
	for _, assertion := range scenario.Assertions {
		switch string(assertion.ID) {
		case "ASSERT-PERF-SCHEMA-CHECK-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-SCHEMA-CHECK-DISPATCH-001":
			dispatch = assertion.Predicate.ContractPredicate == "state-transition" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-SCHEMA-CHECK-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !dispatch || !performance {
		return errors.New("React Native schema-check assertions are invalid")
	}
	return nil
}

func schemaCheckProofObligations(scenario scenarios.Scenario) error {
	matches := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-SCHEMA-CHECK-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				matches[id]++
			}
		case "OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				matches[id]++
			}
		case "OBL-PERF-SCHEMA-CHECK-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-SCHEMA-CHECK-001", "CTRL-SCHEMA-004") {
				matches[id]++
			}
		}
	}
	if matches["OBL-PERF-SCHEMA-CHECK-RN-IOS-CURRENT-001"] != 1 ||
		matches["OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001"] != 1 ||
		matches["OBL-PERF-SCHEMA-CHECK-CONTROL-001"] != 1 {
		return fmt.Errorf("React Native schema-check proof obligations=%v", matches)
	}
	return nil
}

func schemaCheckDispatchPlan(scenario scenarios.Scenario) (scenarios.SchemaDispatchMeasurementPlan, error) {
	for _, expected := range scenario.Model.ExpectedState {
		if expected.ID != "EXPECT-PERF-SCHEMA-CHECK-DISPATCH-001" {
			continue
		}
		var plan scenarios.SchemaDispatchMeasurementPlan
		if expected.Predicate.ContractPredicate != "state-transition" || expected.Predicate.Name != "schema-dispatch-observations-satisfied" ||
			json.Unmarshal(expected.Predicate.Payload, &plan) != nil || plan.MeasurementID != "MEAS-SCHEMA-CHECK-001" ||
			plan.MinimumSampleCountPerStratum == 0 || len(plan.Strata) != 6 {
			return scenarios.SchemaDispatchMeasurementPlan{}, errors.New("React Native schema-check dispatch plan is invalid")
		}
		return plan, nil
	}
	return scenarios.SchemaDispatchMeasurementPlan{}, errors.New("React Native schema-check dispatch plan is absent")
}

func schemaCheckStrata(plan scenarios.SchemaDispatchMeasurementPlan) (map[string]string, error) {
	strata := make(map[string]string, len(plan.Strata))
	for _, stratum := range plan.Strata {
		id := string(stratum.StratumID)
		if id == "" || stratum.SchemaCase == "" {
			return nil, fmt.Errorf("React Native schema-check stratum id=%q case=%q", id, stratum.SchemaCase)
		}
		if _, duplicate := strata[id]; duplicate {
			return nil, fmt.Errorf("React Native schema-check stratum %q is duplicated", id)
		}
		strata[id] = stratum.SchemaCase
	}
	return strata, nil
}

func schemaCheckWireExpectation(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.WireExpectation, error) {
	var result scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == id {
			result = wire
			count++
		}
	}
	if count != 1 || result.ContractCase != "connect_success" || result.HTTPStatus != http.StatusOK || result.Retryable || result.ErrorCode != nil {
		return scenarios.WireExpectation{}, fmt.Errorf("React Native schema-check wire expectation %s count=%d", id, count)
	}
	return result, nil
}

func schemaCheckCompletion(wire scenarios.WireExpectation) string {
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

// NewSchemaCheckCoordinator creates an authenticated loopback coordinator.
func NewSchemaCheckCoordinator(config SchemaCheckCoordinatorConfig) (*SchemaCheckCoordinator, error) {
	if err := ValidateSchemaCheckScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, fmt.Errorf("React Native schema-check platform=%q is invalid", config.Platform)
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native schema-check auth token is required")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
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
		return nil, errors.New("create React Native schema-check capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native schema-check coordinator")
	}
	calls, err := schemaCheckCalls(config.Scenario)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	boundaries := make(map[scenarios.StepID]scenarios.NativeLifecycleBoundary, len(config.Scenario.NativeLifecycleBoundaries))
	for _, boundary := range config.Scenario.NativeLifecycleBoundaries {
		boundaries[boundary.AfterStepID] = boundary
	}
	coordinator := &SchemaCheckCoordinator{
		config: config, listener: listener, token: token, adapter: adapter, calls: calls, boundaries: boundaries,
		databases: make(map[string]bool), captures: make(map[scenarios.StepID]finalCapture), authTokens: make(map[string]string),
		processes: make(map[string]actionProcessIdentity), runtimeIDs: make(map[string]json.RawMessage), nextSeq: 1,
	}
	coordinator.server = &http.Server{
		Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second,
	}
	return coordinator, nil
}

// Prepare installs the authored contract and mints one token for each client key.
func (c *SchemaCheckCoordinator) Prepare(ctx context.Context) error {
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
		return errors.New("React Native schema-check dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native schema-check contract: %w", err)
	}
	for _, call := range c.calls {
		if _, found := c.authTokens[call.clientKey]; found {
			continue
		}
		if c.config.AuthToken != "" {
			c.authTokens[call.clientKey] = c.config.AuthToken
			continue
		}
		token, err := c.config.Harness.NativeBearerToken(ctx, call.step.NativeBinding.UserID, time.Now())
		if err != nil {
			return fmt.Errorf("mint React Native schema-check bearer token for %q: %w", call.clientKey, err)
		}
		c.authTokens[call.clientKey] = token
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve runs the sidecar until it closes.
func (c *SchemaCheckCoordinator) Serve(ctx context.Context) error {
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
	return err
}

// Handler returns the authenticated exchange handler.
func (c *SchemaCheckCoordinator) Handler() http.Handler { return c }

// URL returns the host-loopback sidecar URL.
func (c *SchemaCheckCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

// Token returns the exchange capability.
func (c *SchemaCheckCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// ExchangeCount returns all commands plus the terminal exchange.
func (c *SchemaCheckCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	return len(c.calls)*3 + len(c.boundaries) + 1
}

// Completed reports whether every authored call passed final validation.
func (c *SchemaCheckCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

// Result returns the verified server projection and native identity resolution.
func (c *SchemaCheckCoordinator) Result() (SchemaCheckCoordinatorResult, error) {
	if c == nil {
		return SchemaCheckCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return SchemaCheckCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return SchemaCheckCoordinatorResult{}, errors.New("React Native schema-check coordinator has not completed")
	}
	return c.result, nil
}

// Close stops the sidecar without closing the controller.
func (c *SchemaCheckCoordinator) Close(ctx context.Context) error {
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
	shutdownErr, listenerErr := c.server.Shutdown(ctx), c.listener.Close()
	if shutdownErr != nil {
		return shutdownErr
	}
	if listenerErr != nil && !errors.Is(listenerErr, net.ErrClosed) {
		return listenerErr
	}
	return nil
}

func (c *SchemaCheckCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if c.closed || !c.prepared || c.failed != nil || c.completed || exchange.Sequence != c.nextSeq {
		c.failed = fmt.Errorf("React Native schema-check exchange closed=%t prepared=%t completed=%t sequence=%d want=%d", c.closed, c.prepared, c.completed, exchange.Sequence, c.nextSeq)
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native schema-check exchange=%d call=%d waiting=%d: %w", exchange.Sequence, c.current, c.waiting, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(request.Context(), exchange.Sequence)
	if err != nil {
		c.failed = fmt.Errorf("React Native schema-check exchange=%d call=%d waiting=%d: %w", exchange.Sequence, c.current, c.waiting, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = fmt.Errorf("React Native schema-check response bytes=%d error=%v", len(encoded), err)
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *SchemaCheckCoordinator) acceptLocked(raw json.RawMessage) error {
	if c.waiting == schemaCheckWaitingNone {
		if !isJSONNull(raw) {
			return fmt.Errorf("initial result=%s want=null", boundedRaw(raw))
		}
		return nil
	}
	if c.current >= len(c.calls) {
		return errors.New("React Native schema-check received a result after all calls")
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return fmt.Errorf("command outcome=%q error_code=%v error=%v", envelope.Outcome, envelope.ErrorCode, err)
	}
	call := c.calls[c.current]
	switch c.waiting {
	case schemaCheckWaitingOpen:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return fmt.Errorf("open step %s: %w", call.step.ID, err)
		}
		c.processes[call.sessionKey] = process
	case schemaCheckWaitingSync:
		if err := c.validateSynchronized(call, envelope.Result); err != nil {
			return err
		}
	case schemaCheckWaitingCapture:
		capture, err := c.validateCapture(call, envelope.Result)
		if err != nil {
			return err
		}
		c.captures[call.step.ID] = capture
	case schemaCheckWaitingLifecycle:
		process, found := c.processes[call.sessionKey]
		if !found {
			return fmt.Errorf("React Native schema-check lifecycle process for %s is absent", call.step.ID)
		}
		if err := validateStoppedLifecycleResult(envelope.Result, process); err != nil {
			return fmt.Errorf("lifecycle step %s: %w", call.step.ID, err)
		}
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *SchemaCheckCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	if c.waiting == schemaCheckWaitingCapture {
		if _, stop := c.boundaries[c.calls[c.current].step.ID]; stop {
			c.waiting = schemaCheckWaitingLifecycle
			response.Command = c.command(c.calls[c.current], "client", "lifecycle", map[string]any{
				"client_key": c.calls[c.current].sessionKey, "operation": "stop",
			}, nil)
			return response, nil
		}
		c.current++
		c.waiting = schemaCheckWaitingNone
	}
	if c.waiting == schemaCheckWaitingLifecycle {
		c.current++
		c.waiting = schemaCheckWaitingNone
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
	switch c.waiting {
	case schemaCheckWaitingNone:
		if err := c.applyControllerSteps(ctx, call.controllerSteps); err != nil {
			return exchangeResponse{}, err
		}
		mode := "create"
		if c.databases[call.clientKey] {
			mode = "reuse"
		}
		c.databases[call.clientKey] = true
		c.waiting = schemaCheckWaitingOpen
		response.Command = c.command(call, "client", "open", map[string]any{
			"client_key": call.sessionKey, "database_mode": mode, "initialization": "empty", "seed_step_id": nil,
		}, nil)
	case schemaCheckWaitingOpen:
		c.waiting = schemaCheckWaitingSync
		response.Command = c.command(call, "client", "synchronize-step", map[string]any{
			"client_key": call.sessionKey, "method": call.step.NativeBinding.Method, "completion": call.step.NativeBinding.Completion,
		}, []scenarios.StepID{call.step.ID})
	case schemaCheckWaitingSync:
		c.waiting = schemaCheckWaitingCapture
		// The scenario declares no application record, so it cannot supply the durable-proof identity required by the runner.
		response.Command = c.command(call, "observer", "capture", map[string]any{
			"client_keys": []string{call.sessionKey},
			"sources":     []string{"scope-state", "sync-status", "sync-events", "request-trace"},
		}, nil)
	default:
		return exchangeResponse{}, errInvalidExchange
	}
	return response, nil
}

func (c *SchemaCheckCoordinator) applyControllerSteps(ctx context.Context, steps []scenarios.Step) error {
	for _, step := range steps {
		var (
			result blackbox.NativeStepObservation
			err    error
		)
		switch scenarios.OperationKey(step.Operation) {
		case "process/materialize-source-transaction":
			result, err = c.config.Controller.ProcessStep(ctx, nil, step.Operation)
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/publish-schema":
			result, err = c.config.Controller.ApplyStep(ctx, step.Operation)
		default:
			return fmt.Errorf("React Native schema-check controller step %s operation=%q", step.ID, scenarios.OperationKey(step.Operation))
		}
		if err != nil || result.Disposition != step.ExpectedOutcome.Disposition {
			return fmt.Errorf("React Native schema-check controller step %s disposition=%q want=%q error=%v", step.ID, result.Disposition, step.ExpectedOutcome.Disposition, err)
		}
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateSynchronized(call schemaCheckCall, raw json.RawMessage) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return fmt.Errorf("React Native schema-check synchronized step %s: %w", call.step.ID, err)
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "schema-check synchronized result"); err != nil {
		return err
	}
	var completion string
	if err := json.Unmarshal(members["completion"], &completion); err != nil || completion != call.step.NativeBinding.Completion {
		return fmt.Errorf("React Native schema-check step %s completion=%q want=%q decode_error=%v", call.step.ID, completion, call.step.NativeBinding.Completion, err)
	}
	wantProcess, found := c.processes[call.sessionKey]
	if !found {
		return fmt.Errorf("React Native schema-check synchronization process for %s is absent", call.step.ID)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != wantProcess {
		return fmt.Errorf("React Native schema-check step %s process=%+v want=%+v error=%v", call.step.ID, process, wantProcess, err)
	}
	if call.step.NativeBinding.Completion == "idle" {
		if err := validateReadyStatus(members["status"]); err != nil {
			return fmt.Errorf("React Native schema-check step %s idle status: %w", call.step.ID, err)
		}
		return nil
	}
	var status syncStatus
	if err := json.Unmarshal(members["status"], &status); err != nil || status.State != "error" || !isJSONNull(status.RetryAt) || !hasJSONValue(status.Failure) {
		return fmt.Errorf("React Native schema-check step %s error status=%s decode_error=%v", call.step.ID, boundedRaw(members["status"]), err)
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateCapture(call schemaCheckCall, raw json.RawMessage) (finalCapture, error) {
	capture, err := decodeCapture(raw, []string{"client_state", "sync_status", "sync_events", "request_trace"})
	if err != nil {
		return finalCapture{}, err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "schema-check capture result"); err != nil {
		return finalCapture{}, err
	}
	wantProcess, found := c.processes[call.sessionKey]
	if !found {
		return finalCapture{}, fmt.Errorf("React Native schema-check capture process for %s is absent", call.step.ID)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != wantProcess {
		return finalCapture{}, fmt.Errorf("React Native schema-check step %s capture process=%+v want=%+v error=%v", call.step.ID, process, wantProcess, err)
	}
	if _, err := decodeClientState(capture.ClientState); err != nil {
		return finalCapture{}, fmt.Errorf("React Native schema-check step %s client state: %w", call.step.ID, err)
	}
	if _, err := captureTraceFromRaw(capture.Trace); err != nil {
		return finalCapture{}, fmt.Errorf("React Native schema-check step %s trace: %w", call.step.ID, err)
	}
	return capture, nil
}

func (c *SchemaCheckCoordinator) command(call schemaCheckCall, actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		if id != call.step.ID {
			continue
		}
		steps = append(steps, conformanceStep{Operation: conformanceOperation{
			ContractOperation: call.step.Operation.ContractOperation,
			Name:              call.step.Operation.Name,
			Payload:           copyRaw(call.step.Operation.Payload),
		}})
	}
	return &conformanceCommand{
		SchemaVersion: 1,
		Action: conformanceManifest{
			Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters},
			Steps:  steps,
		},
		Runtime: conformanceRuntime{
			ClientKey: call.sessionKey, Database: schemaCheckDatabase(call.step.NativeBinding.ClientID), ClientID: call.step.NativeBinding.ClientID,
			ServerURL: c.adapter, AuthToken: c.authTokens[call.clientKey],
		},
	}
}

func (c *SchemaCheckCoordinator) finishLocked(ctx context.Context) error {
	if len(c.captures) != len(c.calls) {
		return fmt.Errorf("React Native schema-check captures=%d want=%d", len(c.captures), len(c.calls))
	}
	server, err := c.captureServer(ctx)
	if err != nil {
		return err
	}
	if err := c.bindServerIdentities(); err != nil {
		return err
	}
	plan, err := schemaCheckDispatchPlan(c.config.Scenario)
	if err != nil {
		return err
	}
	counts := make(map[string]uint64, len(plan.Strata))
	for _, call := range c.calls {
		capture, found := c.captures[call.step.ID]
		if !found {
			return fmt.Errorf("React Native schema-check capture for %s is absent", call.step.ID)
		}
		if err := c.validateCallEvidence(call, capture); err != nil {
			return err
		}
		if call.step.MeasurementSample != nil {
			counts[string(call.step.MeasurementSample.StratumID)]++
		}
	}
	for _, stratum := range plan.Strata {
		if counts[string(stratum.StratumID)] != plan.MinimumSampleCountPerStratum {
			return fmt.Errorf("React Native schema-check executed stratum %s samples=%d want=%d", stratum.StratumID, counts[string(stratum.StratumID)], plan.MinimumSampleCountPerStratum)
		}
	}
	resolutions, err := c.resolveIdentities()
	if err != nil {
		return err
	}
	c.result = SchemaCheckCoordinatorResult{ServerFacts: server, IdentityResolution: resolutions}
	return nil
}

func (c *SchemaCheckCoordinator) captureServer(ctx context.Context) (scenarios.StateFacts, error) {
	clients := c.uniqueClients()
	keys := make([]string, 0, len(clients))
	for _, call := range clients {
		key := call.clientKey
		keys = append(keys, key)
	}
	sort.Strings(keys)
	captures, err := c.config.Controller.Capture(ctx, keys, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("capture React Native schema-check server state captures=%d error=%v", len(captures), err)
	}
	return captures[0].StateFacts, nil
}

func (c *SchemaCheckCoordinator) bindServerIdentities() error {
	serverAliases := make([]scenarios.NativeIdentityAlias, 0, len(c.config.Scenario.NativeIdentityAliases))
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		switch alias.Kind {
		case "schema", "scope", "table", "primary-key":
			serverAliases = append(serverAliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(serverAliases)
	if err != nil {
		return fmt.Errorf("resolve React Native schema-check server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		switch value.Alias {
		case "items-table":
			c.tableName = value.ApplicationIdentifier
		case "items-primary-key":
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" || c.primaryKey == "" {
		return fmt.Errorf("React Native schema-check server application identities observed table=%q primary_key=%q want=nonempty table and primary key", c.tableName, c.primaryKey)
	}
	generation, scopeSetVersion, err := c.observedClientIdentities()
	if err != nil {
		return err
	}
	for alias, value := range map[string]uint64{
		"client-generation-one": generation,
		"scope-set-version-one": scopeSetVersion,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode React Native schema-check server identity %q: %w", alias, err)
		}
		c.runtimeIDs[alias] = encoded
	}
	return nil
}

type schemaCheckClientIdentity struct {
	generation      uint64
	scopeSetVersion uint64
}

func (c *SchemaCheckCoordinator) observedClientIdentities() (uint64, uint64, error) {
	clients := c.uniqueClients()
	if len(clients) == 0 {
		return 0, 0, errors.New("React Native schema-check identity clients=0 want=positive")
	}
	observed := make(map[string]schemaCheckClientIdentity, len(clients))
	for _, call := range c.calls {
		capture, found := c.captures[call.step.ID]
		if !found {
			return 0, 0, fmt.Errorf("React Native schema-check identity capture step=%s observed=absent want=present", call.step.ID)
		}
		trace, err := captureTraceFromRaw(capture.Trace)
		if err != nil {
			return 0, 0, fmt.Errorf("React Native schema-check identity trace step=%s observed=invalid want=valid error=%v", call.step.ID, err)
		}
		for _, observation := range trace.Observations {
			if observation.OperationClass != "pull" {
				continue
			}
			generation, generationErr := requestInteger(observation, "client_generation")
			scopeSetVersion, scopeSetErr := requestInteger(observation, "scope_set_version")
			if generationErr != nil || scopeSetErr != nil {
				return 0, 0, fmt.Errorf("React Native schema-check client=%q identity request=%s observed generation_error=%v scope_set_error=%v want=valid client_generation and scope_set_version", call.clientKey, boundedRaw(observation.RequestFacts), generationErr, scopeSetErr)
			}
			observed[call.clientKey] = schemaCheckClientIdentity{generation: generation, scopeSetVersion: scopeSetVersion}
		}
	}
	if len(observed) != len(clients) {
		return 0, 0, fmt.Errorf("React Native schema-check observed identity clients=%d want=%d", len(observed), len(clients))
	}
	var expected schemaCheckClientIdentity
	for _, call := range clients {
		identity := observed[call.clientKey]
		if expected.generation == 0 && expected.scopeSetVersion == 0 {
			expected = identity
		}
		if identity.generation == 0 || identity.scopeSetVersion == 0 || identity != expected {
			return 0, 0, fmt.Errorf("React Native schema-check client=%q observed client_generation=%d scope_set_version=%d want positive shared client_generation=%d scope_set_version=%d", call.clientKey, identity.generation, identity.scopeSetVersion, expected.generation, expected.scopeSetVersion)
		}
	}
	return expected.generation, expected.scopeSetVersion, nil
}

func (c *SchemaCheckCoordinator) validateCallEvidence(call schemaCheckCall, capture finalCapture) error {
	wire, err := schemaCheckWireExpectation(c.config.Scenario, call.step.ID)
	if err != nil {
		return err
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return fmt.Errorf("React Native schema-check step %s client state: %w", call.step.ID, err)
	}
	inputAlias, err := c.stepSchemaAlias(call.step)
	if err != nil {
		return err
	}
	wantSchema := call.serverSchemaAlias
	if wire.Action == "unsupported" {
		wantSchema = inputAlias
	}
	runtimeSchema, err := c.runtimeSchema(wantSchema)
	if err != nil || state.Schema == nil || *state.Schema != runtimeSchema {
		return fmt.Errorf("React Native schema-check step %s client schema=%+v want_alias=%q want=%+v error=%v", call.step.ID, state.Schema, wantSchema, runtimeSchema, err)
	}
	scope, err := c.stepRuntimeScope(call.step)
	if err != nil || len(state.ScopeStates) != 1 || state.ScopeStates[0].ScopeID != scope {
		return fmt.Errorf("React Native schema-check step %s scopes=%+v want=%q error=%v", call.step.ID, state.ScopeStates, scope, err)
	}
	if call.step.NativeBinding.Completion == "idle" {
		if err := validateReadyStatus(capture.Status); err != nil {
			return fmt.Errorf("React Native schema-check step %s capture status: %w", call.step.ID, err)
		}
	} else {
		var status syncStatus
		if err := json.Unmarshal(capture.Status, &status); err != nil || status.State != "error" || !isJSONNull(status.RetryAt) || !hasJSONValue(status.Failure) {
			return fmt.Errorf("React Native schema-check step %s capture error status=%s decode_error=%v", call.step.ID, boundedRaw(capture.Status), err)
		}
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil || trace.Overflowed || len(trace.Observations) == 0 || validateTraceSequence(trace.Observations) != nil {
		return fmt.Errorf("React Native schema-check step %s trace observations=%d overflowed=%t error=%v", call.step.ID, len(trace.Observations), trace.Overflowed, err)
	}
	if !schemaCheckTraceContainsConnect(trace) {
		return fmt.Errorf("React Native schema-check step %s trace did not contain a successful connect", call.step.ID)
	}
	if wire.HTTPStatus != http.StatusOK || wire.ErrorCode != nil || wire.Retryable {
		return fmt.Errorf("React Native schema-check step %s wire status=%d code=%v retryable=%t", call.step.ID, wire.HTTPStatus, wire.ErrorCode, wire.Retryable)
	}
	return nil
}

func schemaCheckTraceContainsConnect(trace traceSnapshot) bool {
	for _, observed := range trace.Observations {
		if observed.OperationClass != "connect" || validateTraceOperation(observed, "connect") != nil {
			continue
		}
		return true
	}
	return false
}

func (c *SchemaCheckCoordinator) resolveIdentities() ([]blackbox.NativeIdentityResolution, error) {
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("React Native schema-check server alias %q is absent", alias.Alias)
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
	return blackbox.ResolveNativeIdentityAliases(c.config.Scenario.NativeIdentityAliases, observations)
}

func (c *SchemaCheckCoordinator) stepSchemaAlias(step scenarios.Step) (string, error) {
	var payload struct {
		Schema clientSchema `json:"schema"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.Schema.Version == 0 || payload.Schema.Hash == "" {
		return "", fmt.Errorf("React Native schema-check step %s input schema is invalid", step.ID)
	}
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if alias.Kind != "schema" {
			continue
		}
		var authored clientSchema
		if json.Unmarshal(alias.Value, &authored) == nil && authored == payload.Schema {
			return alias.Alias, nil
		}
	}
	return "", fmt.Errorf("React Native schema-check step %s schema=%+v has no declared alias", step.ID, payload.Schema)
}

func (c *SchemaCheckCoordinator) stepRuntimeScope(step scenarios.Step) (string, error) {
	var payload struct {
		KnownScopes []struct {
			ScopeID string `json:"scope_id"`
		} `json:"known_scopes"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || len(payload.KnownScopes) != 1 || payload.KnownScopes[0].ScopeID == "" {
		return "", fmt.Errorf("React Native schema-check step %s known scopes are invalid", step.ID)
	}
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if alias.Kind != "scope" {
			continue
		}
		var authored string
		if json.Unmarshal(alias.Value, &authored) == nil && authored == payload.KnownScopes[0].ScopeID {
			var runtime string
			if err := json.Unmarshal(c.runtimeIDs[alias.Alias], &runtime); err != nil || runtime == "" {
				return "", fmt.Errorf("React Native schema-check scope alias %q value=%s error=%v", alias.Alias, boundedRaw(c.runtimeIDs[alias.Alias]), err)
			}
			return runtime, nil
		}
	}
	return "", fmt.Errorf("React Native schema-check step %s scope=%q has no declared alias", step.ID, payload.KnownScopes[0].ScopeID)
}

func (c *SchemaCheckCoordinator) runtimeSchema(alias string) (clientSchema, error) {
	var schema clientSchema
	if err := json.Unmarshal(c.runtimeIDs[alias], &schema); err != nil || schema.Version == 0 || schema.Hash == "" {
		return clientSchema{}, fmt.Errorf("React Native schema-check schema alias %q value=%s error=%v", alias, boundedRaw(c.runtimeIDs[alias]), err)
	}
	return schema, nil
}

func (c *SchemaCheckCoordinator) uniqueClients() []schemaCheckCall {
	clients := make(map[string]schemaCheckCall, len(c.calls))
	for _, call := range c.calls {
		clients[call.clientKey] = call
	}
	keys := make([]string, 0, len(clients))
	for key := range clients {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]schemaCheckCall, 0, len(keys))
	for _, key := range keys {
		result = append(result, clients[key])
	}
	return result
}

func schemaCheckCase(step scenarios.Step) (string, error) {
	if step.MeasurementSample == nil {
		return "", fmt.Errorf("React Native schema-check step %s has no measurement", step.ID)
	}
	var parameters struct {
		SchemaCase string `json:"schema_case"`
	}
	if err := json.Unmarshal(step.MeasurementSample.Parameters, &parameters); err != nil || parameters.SchemaCase == "" {
		return "", fmt.Errorf("React Native schema-check step %s measurement parameters are invalid", step.ID)
	}
	return parameters.SchemaCase, nil
}

func schemaCheckMeasurementOperationCase(sample scenarios.MeasurementSample) (string, error) {
	var value struct {
		SchemaCase string `json:"schema_case"`
	}
	if err := json.Unmarshal(sample.Operation.Value, &value); err != nil || value.SchemaCase == "" {
		return "", fmt.Errorf("React Native schema-check measurement operation %s has invalid schema case", sample.Operation.ID)
	}
	return value.SchemaCase, nil
}

func schemaCheckClientKey(userID, clientID string) string {
	return "schema-check-" + userID + "-" + clientID
}

func schemaCheckSessionKey(stepID scenarios.StepID) string {
	return "schema-check-" + strings.ToLower(strings.TrimPrefix(string(stepID), "STEP-PERF-SCHEMA-CHECK-"))
}

func schemaCheckDatabase(clientID string) string {
	return "rn-schema-check-" + clientID + ".db"
}
