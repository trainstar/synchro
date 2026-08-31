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
	schemaQueuedMutationScenarioPath = "conformance/scenarios/server/schema-queued-mutation-001.json"
	schemaQueuedMutationScenarioID   = "SCN-SCHEMA-QUEUED-MUTATION-001"
)

var schemaQueuedMutationStepOrder = []scenarios.StepID{
	"STEP-SCHEMA-QUEUED-MUTATION-001",
	"STEP-SCHEMA-QUEUED-MUTATION-002",
	"STEP-SCHEMA-QUEUED-MUTATION-003",
	"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-BEGIN-001",
	"STEP-SCHEMA-QUEUED-MUTATION-004",
	"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-FINALIZE-001",
	"STEP-SCHEMA-QUEUED-MUTATION-005",
	"STEP-SCHEMA-QUEUED-MUTATION-006",
	"STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001",
	"STEP-SCHEMA-QUEUED-MUTATION-007",
	"STEP-SCHEMA-QUEUED-MUTATION-008",
	"STEP-SCHEMA-QUEUED-MUTATION-009",
}

var schemaQueuedMutationAliasNames = []string{
	"client-generation-one",
	"schema-one",
	"schema-two",
	"scope-a",
	"schema-baseline-rebuild",
	"scope-set-version-one",
	"queued-mutation",
	"schema-reset-batch",
	"items-table",
	"queued-row-primary-key",
	"schema-one-base-version",
	"schema-one-base-checksum",
}

// SchemaQueuedMutationCoordinatorConfig configures one authenticated RN sidecar.
type SchemaQueuedMutationCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// SchemaQueuedMutationCoordinatorResult contains server facts and resolved identities.
type SchemaQueuedMutationCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// SchemaQueuedMutationCoordinator runs one durable schema-incompatible mutation.
type SchemaQueuedMutationCoordinator struct {
	config   SchemaQueuedMutationCoordinatorConfig
	listener net.Listener
	server   *http.Server
	token    string
	adapter  string
	database string

	steps      map[scenarios.StepID]scenarios.Step
	expected   *scenarios.StateFacts
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	authTokens map[string]string
	userID     string
	clientID   string
	tableName  string
	primaryKey string

	mu          sync.Mutex
	prepared    bool
	closed      bool
	completed   bool
	failed      error
	stage       schemaQueuedMutationStage
	nextSeq     uint64
	process     *actionProcessIdentity
	preRestart  *traceSnapshot
	finalResult *finalCapture
	result      SchemaQueuedMutationCoordinatorResult
}

type schemaQueuedMutationStage uint8

const (
	schemaQueuedMutationStageOpen schemaQueuedMutationStage = iota
	schemaQueuedMutationStageOpened
	schemaQueuedMutationStageBaseline
	schemaQueuedMutationStageLocalWrite
	schemaQueuedMutationStageStopped
	schemaQueuedMutationStageUnsupported
	schemaQueuedMutationStageReset
	schemaQueuedMutationStagePreRestartCapture
	schemaQueuedMutationStageRestarted
	schemaQueuedMutationStageFinalCapture
	schemaQueuedMutationStageComplete
)

// LoadSchemaQueuedMutationScenario loads only the authored queued-mutation scenario.
func LoadSchemaQueuedMutationScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, schemaQueuedMutationScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native schema-queued-mutation scenario: %w", err)
	}
	if err := ValidateSchemaQueuedMutationScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateSchemaQueuedMutationScenario rejects changes to the closed RN contract.
func ValidateSchemaQueuedMutationScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != schemaQueuedMutationScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native schema-queued-mutation scenario contract is invalid")
	}
	if len(scenario.Steps) != len(schemaQueuedMutationStepOrder) {
		return fmt.Errorf("React Native schema-queued-mutation step count=%d want=%d", len(scenario.Steps), len(schemaQueuedMutationStepOrder))
	}
	bindings := []struct {
		id, operation, kind, method, completion string
	}{
		{"STEP-SCHEMA-QUEUED-MUTATION-001", "model/commit-source-transaction", "controller", "", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-002", "process/materialize-source-transaction", "controller", "", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-003", "rebuild/request-page", "public-call", "start", "idle"},
		{"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-BEGIN-001", "local/begin-rebuild", "public-call", "start", "idle"},
		{"STEP-SCHEMA-QUEUED-MUTATION-004", "local/apply-rebuild-page", "public-call", "start", "idle"},
		{"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-FINALIZE-001", "local/finalize-rebuild", "public-call", "start", "idle"},
		{"STEP-SCHEMA-QUEUED-MUTATION-005", "local/write", "local-write", "", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-006", "model/publish-schema", "controller", "", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001", "connect/send", "public-call", "start", "error"},
		{"STEP-SCHEMA-QUEUED-MUTATION-007", "connect/send", "public-call", "reset-schema-and-start", "idle"},
		{"STEP-SCHEMA-QUEUED-MUTATION-008", "push/submit", "public-call", "reset-schema-and-start", "idle"},
		{"STEP-SCHEMA-QUEUED-MUTATION-009", "process/restart-client", "process", "", ""},
	}
	for index, expected := range bindings {
		step := scenario.Steps[index]
		if step.ID != schemaQueuedMutationStepOrder[index] || step.ID != scenarios.StepID(expected.id) ||
			step.NativeBinding == nil || scenarios.OperationKey(step.Operation) != expected.operation ||
			step.NativeBinding.Kind != expected.kind || step.NativeBinding.Method != expected.method ||
			step.NativeBinding.Completion != expected.completion || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native schema-queued-mutation binding %s is invalid", expected.id)
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 0 || len(scenario.NativeIdentityAliases) != len(schemaQueuedMutationAliasNames) {
		return fmt.Errorf("React Native schema-queued-mutation lifecycle=%d aliases=%d", len(scenario.NativeLifecycleBoundaries), len(scenario.NativeIdentityAliases))
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native schema-queued-mutation identity alias is empty")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return fmt.Errorf("React Native schema-queued-mutation identity alias %q is duplicated", alias.Alias)
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range schemaQueuedMutationAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native schema-queued-mutation identity alias %q is absent", name)
		}
	}
	if _, err := schemaQueuedMutationClientIdentity(scenario); err != nil {
		return err
	}
	semantic := false
	for _, assertion := range scenario.Assertions {
		if assertion.ID == "ASSERT-SCHEMA-QUEUED-MUTATION-SEMANTIC-001" {
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || schemaQueuedMutationExpectedState(scenario) == nil {
		return errors.New("React Native schema-queued-mutation assertion or expected state is invalid")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		switch string(obligation.ObligationID) {
		case "OBL-SCHEMA-QUEUED-MUTATION-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[string(obligation.ObligationID)]++
			}
		case "OBL-SCHEMA-QUEUED-MUTATION-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[string(obligation.ObligationID)]++
			}
		case "OBL-SCHEMA-QUEUED-MUTATION-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-SCHEMA-QUEUED-MUTATION-001", "CTRL-SCHEMA-002") {
				obligations[string(obligation.ObligationID)]++
			}
		}
	}
	if obligations["OBL-SCHEMA-QUEUED-MUTATION-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-SCHEMA-QUEUED-MUTATION-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-SCHEMA-QUEUED-MUTATION-CONTROL-001"] != 1 {
		return fmt.Errorf("React Native schema-queued-mutation proof obligations=%v", obligations)
	}
	return nil
}

// NewSchemaQueuedMutationCoordinator creates an authenticated host-loopback sidecar.
func NewSchemaQueuedMutationCoordinator(config SchemaQueuedMutationCoordinatorConfig) (*SchemaQueuedMutationCoordinator, error) {
	if err := ValidateSchemaQueuedMutationScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, fmt.Errorf("React Native schema-queued-mutation platform=%q is invalid", config.Platform)
	}
	identity, err := schemaQueuedMutationClientIdentity(config.Scenario)
	if err != nil {
		return nil, err
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native schema-queued-mutation auth token is required")
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
		return nil, errors.New("create React Native schema-queued-mutation capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-schema-queued-mutation-")
		if err != nil {
			return nil, errors.New("create React Native schema-queued-mutation database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, fmt.Errorf("React Native schema-queued-mutation database=%q is invalid", database)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native schema-queued-mutation coordinator")
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	authTokens := make(map[string]string, 1)
	if config.AuthToken != "" {
		authTokens[identity.clientID] = config.AuthToken
	}
	coordinator := &SchemaQueuedMutationCoordinator{
		config: config, listener: listener, token: token, adapter: adapter, database: database,
		steps: steps, expected: schemaQueuedMutationExpectedState(config.Scenario),
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), authTokens: authTokens,
		userID: identity.userID, clientID: identity.clientID, nextSeq: 1,
		server: &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second},
	}
	coordinator.server.Handler = coordinator
	return coordinator, nil
}

// Prepare installs the authored model and binds the server source operations.
func (c *SchemaQueuedMutationCoordinator) Prepare(ctx context.Context) error {
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
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native schema-queued-mutation dependencies are unavailable")
	}
	if c.authTokens[c.clientID] == "" {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.userID, time.Now())
		if err != nil {
			return fmt.Errorf("mint React Native schema-queued-mutation bearer token for %q: %w", c.clientID, err)
		}
		c.authTokens[c.clientID] = token
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native schema-queued-mutation contract: %w", err)
	}
	commit := c.steps["STEP-SCHEMA-QUEUED-MUTATION-001"].Operation
	if result, err := c.config.Controller.ApplyStep(ctx, commit); err != nil || result.Disposition != "success" {
		return fmt.Errorf("commit React Native schema-queued-mutation baseline disposition=%q error=%w", result.Disposition, nativeResultError(err, result.Disposition))
	}
	materialize := c.steps["STEP-SCHEMA-QUEUED-MUTATION-002"].Operation
	if result, err := c.config.Controller.ProcessStep(ctx, nil, materialize); err != nil || result.Disposition != "success" {
		return fmt.Errorf("materialize React Native schema-queued-mutation baseline disposition=%q error=%w", result.Disposition, nativeResultError(err, result.Disposition))
	}
	write, err := c.config.Controller.ApplicationWrite(c.steps["STEP-SCHEMA-QUEUED-MUTATION-005"].Operation)
	if err != nil {
		return fmt.Errorf("bind React Native schema-queued-mutation local write: %w", err)
	}
	step := c.steps["STEP-SCHEMA-QUEUED-MUTATION-005"]
	step.Operation = write
	c.steps[step.ID] = step
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *SchemaQueuedMutationCoordinator) Serve(ctx context.Context) error {
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
			closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(closeContext)
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

func (c *SchemaQueuedMutationCoordinator) Handler() http.Handler { return c }

func (c *SchemaQueuedMutationCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *SchemaQueuedMutationCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// ExchangeCount returns the fixed count, including the complete exchange.
func (c *SchemaQueuedMutationCoordinator) ExchangeCount() int { return 10 }

func (c *SchemaQueuedMutationCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *SchemaQueuedMutationCoordinator) Result() (SchemaQueuedMutationCoordinatorResult, error) {
	if c == nil {
		return SchemaQueuedMutationCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return SchemaQueuedMutationCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return SchemaQueuedMutationCoordinatorResult{}, errors.New("React Native schema-queued-mutation coordinator has not completed")
	}
	return c.result, nil
}

func (c *SchemaQueuedMutationCoordinator) Close(ctx context.Context) error {
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

func (c *SchemaQueuedMutationCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if c.closed || !c.prepared || c.failed != nil || c.completed {
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if exchange.Sequence != c.nextSeq {
		c.failed = fmt.Errorf("React Native schema-queued-mutation sequence=%d want=%d", exchange.Sequence, c.nextSeq)
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native schema-queued-mutation sequence=%d stage=%d: %w", exchange.Sequence, c.stage, err)
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
		c.failed = fmt.Errorf("React Native schema-queued-mutation response bytes=%d marshal_error=%v", len(encoded), err)
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *SchemaQueuedMutationCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == schemaQueuedMutationStageOpen {
		if !isJSONNull(raw) {
			return fmt.Errorf("React Native schema-queued-mutation initial result=%s want=null", raw)
		}
		return nil
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return fmt.Errorf("React Native schema-queued-mutation envelope outcome=%q decode_error=%v", envelope.Outcome, err)
	}
	switch c.stage {
	case schemaQueuedMutationStageOpened:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
	case schemaQueuedMutationStageBaseline:
		return c.validateSynchronized(envelope.Result, "idle", true)
	case schemaQueuedMutationStageLocalWrite:
		return c.validateLocal(envelope.Result)
	case schemaQueuedMutationStageStopped:
		if c.process == nil {
			return errors.New("React Native schema-queued-mutation process is unavailable")
		}
		return validateStoppedLifecycleResult(envelope.Result, *c.process)
	case schemaQueuedMutationStageUnsupported:
		return c.validateSynchronized(envelope.Result, "error", true)
	case schemaQueuedMutationStageReset:
		return c.validateSynchronized(envelope.Result, "idle", true)
	case schemaQueuedMutationStagePreRestartCapture:
		trace, err := c.validateTraceCapture(envelope.Result, true)
		if err != nil {
			return err
		}
		if err := validateSchemaQueuedMutationTrace(c.config.Scenario, trace); err != nil {
			return err
		}
		c.preRestart = &trace
	case schemaQueuedMutationStageRestarted:
		if _, err := validateOpenedResult(envelope.Result); err != nil {
			return err
		}
	case schemaQueuedMutationStageFinalCapture:
		capture, err := c.validateFinalCapture(envelope.Result)
		if err != nil {
			return err
		}
		c.finalResult = &capture
	default:
		return fmt.Errorf("React Native schema-queued-mutation result stage=%d is invalid", c.stage)
	}
	return nil
}

func (c *SchemaQueuedMutationCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case schemaQueuedMutationStageOpen:
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "client", "open", map[string]any{
			"client_key": schemaQueuedMutationInitialClientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil,
		}, nil)
	case schemaQueuedMutationStageOpened:
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "client", "synchronize-step", map[string]any{
			"client_key": schemaQueuedMutationInitialClientKey, "method": "start", "completion": "idle",
		}, []scenarios.StepID{
			"STEP-SCHEMA-QUEUED-MUTATION-003",
			"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-BEGIN-001",
			"STEP-SCHEMA-QUEUED-MUTATION-004",
			"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-FINALIZE-001",
		})
	case schemaQueuedMutationStageBaseline:
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "client", "execute-step", map[string]any{
			"client_key": schemaQueuedMutationInitialClientKey,
		}, []scenarios.StepID{"STEP-SCHEMA-QUEUED-MUTATION-005"})
	case schemaQueuedMutationStageLocalWrite:
		publish := c.steps["STEP-SCHEMA-QUEUED-MUTATION-006"].Operation
		if result, err := c.config.Controller.ApplyStep(ctx, publish); err != nil || result.Disposition != "success" {
			return exchangeResponse{}, fmt.Errorf("publish React Native schema-queued-mutation disposition=%q error=%w", result.Disposition, nativeResultError(err, result.Disposition))
		}
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "client", "lifecycle", map[string]any{
			"client_key": schemaQueuedMutationInitialClientKey, "operation": "stop",
		}, nil)
	case schemaQueuedMutationStageStopped:
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "client", "synchronize-step", map[string]any{
			"client_key": schemaQueuedMutationInitialClientKey, "method": "start", "completion": "error",
		}, []scenarios.StepID{"STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001"})
	case schemaQueuedMutationStageUnsupported:
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "client", "synchronize-step", map[string]any{
			"client_key": schemaQueuedMutationInitialClientKey, "method": "reset-schema-and-start", "completion": "idle",
		}, []scenarios.StepID{"STEP-SCHEMA-QUEUED-MUTATION-007", "STEP-SCHEMA-QUEUED-MUTATION-008"})
	case schemaQueuedMutationStageReset:
		push := c.steps["STEP-SCHEMA-QUEUED-MUTATION-008"].Operation
		if err := c.config.Controller.BindApplicationPush(push); err != nil {
			return exchangeResponse{}, fmt.Errorf("bind React Native schema-queued-mutation push: %w", err)
		}
		response.Command = c.command(schemaQueuedMutationInitialClientKey, "observer", "capture", map[string]any{
			"client_keys": []string{schemaQueuedMutationInitialClientKey}, "sources": []string{"request-trace"},
		}, nil)
	case schemaQueuedMutationStagePreRestartCapture:
		response.Command = c.command(schemaQueuedMutationRestartClientKey, "client", "open", map[string]any{
			"client_key": schemaQueuedMutationRestartClientKey, "database_mode": "reuse", "initialization": "empty", "seed_step_id": nil,
		}, nil)
	case schemaQueuedMutationStageRestarted:
		response.Command = c.command(schemaQueuedMutationRestartClientKey, "observer", "capture", map[string]any{
			"client_keys": []string{schemaQueuedMutationRestartClientKey},
			"sources":     []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "request-trace"},
		}, nil)
	case schemaQueuedMutationStageFinalCapture:
		if err := c.completeLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	default:
		return exchangeResponse{}, fmt.Errorf("React Native schema-queued-mutation advance stage=%d is invalid", c.stage)
	}
	if c.stage != schemaQueuedMutationStageFinalCapture {
		c.stage++
	}
	return response, nil
}

const (
	schemaQueuedMutationInitialClientKey = "schema-queued-mutation-initial"
	schemaQueuedMutationRestartClientKey = "schema-queued-mutation-restarted"
)

func (c *SchemaQueuedMutationCoordinator) validateSynchronized(raw json.RawMessage, completion string, requireInitialProcess bool) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "schema-queued-mutation synchronized result"); err != nil {
		return err
	}
	var observed string
	if err := json.Unmarshal(members["completion"], &observed); err != nil || observed != completion {
		return fmt.Errorf("React Native schema-queued-mutation completion=%q want=%q decode_error=%v", observed, completion, err)
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return fmt.Errorf("React Native schema-queued-mutation status is invalid: %w", err)
	}
	if !requireInitialProcess {
		return nil
	}
	if c.process == nil {
		return errors.New("React Native schema-queued-mutation initial process is unavailable")
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != *c.process {
		return fmt.Errorf("React Native schema-queued-mutation process=%+v want=%+v decode_error=%v", process, *c.process, err)
	}
	return nil
}

func (c *SchemaQueuedMutationCoordinator) validateLocal(raw json.RawMessage) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "schema-queued-mutation local result"); err != nil {
		return err
	}
	var rows uint64
	if err := json.Unmarshal(members["rows_affected"], &rows); err != nil || rows != 1 {
		return fmt.Errorf("React Native schema-queued-mutation local rows_affected=%d want=1 decode_error=%v", rows, err)
	}
	if c.process == nil {
		return errors.New("React Native schema-queued-mutation local process is unavailable")
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != *c.process {
		return fmt.Errorf("React Native schema-queued-mutation local process=%+v want=%+v decode_error=%v", process, *c.process, err)
	}
	return nil
}

func (c *SchemaQueuedMutationCoordinator) validateTraceCapture(raw json.RawMessage, requireInitialProcess bool) (traceSnapshot, error) {
	capture, err := decodeCapture(raw, []string{"request_trace"})
	if err != nil {
		return traceSnapshot{}, err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "schema-queued-mutation trace capture"); err != nil {
		return traceSnapshot{}, err
	}
	if requireInitialProcess {
		if c.process == nil {
			return traceSnapshot{}, errors.New("React Native schema-queued-mutation trace process is unavailable")
		}
		process, err := decodeActionProcessIdentity(members["process"])
		if err != nil || process != *c.process {
			return traceSnapshot{}, fmt.Errorf("React Native schema-queued-mutation trace process=%+v want=%+v decode_error=%v", process, *c.process, err)
		}
	}
	return captureTraceFromRaw(capture.Trace)
}

func (c *SchemaQueuedMutationCoordinator) validateFinalCapture(raw json.RawMessage) (finalCapture, error) {
	capture, err := decodeCapture(raw, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "request_trace"})
	if err != nil {
		return finalCapture{}, err
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return finalCapture{}, err
	}
	if trace.Overflowed || len(trace.Observations) != 0 || trace.SequenceCheckpoint != 0 {
		return finalCapture{}, fmt.Errorf("React Native schema-queued-mutation restart trace observations=%d checkpoint=%d overflowed=%t", len(trace.Observations), trace.SequenceCheckpoint, trace.Overflowed)
	}
	if err := validateReadyStatus(capture.Status); err != nil {
		return finalCapture{}, fmt.Errorf("React Native schema-queued-mutation restart status: %w", err)
	}
	return capture, nil
}

func validateSchemaQueuedMutationTrace(scenario scenarios.Scenario, trace traceSnapshot) error {
	if trace.Overflowed || trace.SequenceCheckpoint != uint64(len(trace.Observations)) || len(trace.Observations) < 6 {
		return fmt.Errorf("React Native schema-queued-mutation trace observations=%d checkpoint=%d overflowed=%t", len(trace.Observations), trace.SequenceCheckpoint, trace.Overflowed)
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return err
	}
	for index, operation := range []string{"connect", "rebuild", "pull", "connect", "connect"} {
		if err := validateTraceOperation(trace.Observations[index], operation); err != nil {
			return fmt.Errorf("React Native schema-queued-mutation trace operation index=%d operation=%q: %w", index+1, operation, err)
		}
	}
	limit, err := requestInteger(trace.Observations[1], "limit")
	if err != nil || limit != schemaQueuedMutationRebuildLimit(scenario) {
		return fmt.Errorf("React Native schema-queued-mutation rebuild limit=%d want=%d error=%v", limit, schemaQueuedMutationRebuildLimit(scenario), err)
	}
	pushes := make([]transportObservation, 0, 1)
	for _, observation := range trace.Observations[5:] {
		if observation.OperationClass == "push" {
			pushes = append(pushes, observation)
		}
	}
	if len(pushes) != 1 {
		return fmt.Errorf("React Native schema-queued-mutation push observations=%d want=1", len(pushes))
	}
	if pushes[0].StatusCode != schemaQueuedMutationWireStatus(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008") ||
		pushes[0].CursorFingerprints != nil || pushes[0].CursorFingerprintsComplete != nil ||
		hasJSONValue(pushes[0].RebuildResponseFacts) || hasJSONValue(pushes[0].PullResponseFacts) {
		return fmt.Errorf("React Native schema-queued-mutation push status=%d want=%d", pushes[0].StatusCode, schemaQueuedMutationWireStatus(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008"))
	}
	mutations, err := requestInteger(pushes[0], "mutation_count")
	if err != nil || mutations != 1 {
		return fmt.Errorf("React Native schema-queued-mutation push mutation_count=%d want=1 error=%v", mutations, err)
	}
	return nil
}

func schemaQueuedMutationRebuildLimit(scenario scenarios.Scenario) uint64 {
	step := scenario.Steps[2]
	var payload struct {
		Limit uint64 `json:"limit"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil {
		return 0
	}
	return payload.Limit
}

func schemaQueuedMutationWireStatus(scenario scenarios.Scenario, stepID scenarios.StepID) int {
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == stepID {
			return wire.HTTPStatus
		}
	}
	return 0
}

func (c *SchemaQueuedMutationCoordinator) completeLocked(ctx context.Context) error {
	if c.finalResult == nil || c.preRestart == nil || c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native schema-queued-mutation final evidence is unavailable")
	}
	server, identities, err := c.resolveServerIdentities(ctx)
	if err != nil {
		return err
	}
	if err := c.validateDurableResult(); err != nil {
		return err
	}
	c.result = SchemaQueuedMutationCoordinatorResult{ServerFacts: server, IdentityResolution: identities}
	return nil
}

func (c *SchemaQueuedMutationCoordinator) resolveServerIdentities(ctx context.Context) (scenarios.StateFacts, []blackbox.NativeIdentityResolution, error) {
	captures, err := c.config.Controller.Capture(ctx, []string{schemaQueuedMutationRestartClientKey}, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, nil, fmt.Errorf("capture React Native schema-queued-mutation server state captures=%d error=%w", len(captures), nativeResultError(err, ""))
	}
	values, err := c.config.Controller.IdentityValues(c.identities)
	if err != nil {
		return scenarios.StateFacts{}, nil, fmt.Errorf("resolve React Native schema-queued-mutation server aliases: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		switch value.Alias {
		case "items-table":
			c.tableName = value.ApplicationIdentifier
		case "queued-row-primary-key":
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" || c.primaryKey == "" {
		return scenarios.StateFacts{}, nil, fmt.Errorf("React Native schema-queued-mutation application table=%q primary_key=%q", c.tableName, c.primaryKey)
	}
	evidence, err := c.serverEvidence(ctx)
	if err != nil {
		return scenarios.StateFacts{}, nil, err
	}
	for alias, value := range map[string]any{
		"client-generation-one":    evidence.clientGeneration,
		"scope-set-version-one":    evidence.scopeSetVersion,
		"schema-baseline-rebuild":  evidence.rebuildID,
		"schema-one-base-version":  evidence.rowVersion,
		"schema-one-base-checksum": evidence.rowChecksum,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			return scenarios.StateFacts{}, nil, fmt.Errorf("encode React Native schema-queued-mutation server alias %q: %w", alias, err)
		}
		c.runtimeIDs[alias] = encoded
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return scenarios.StateFacts{}, nil, fmt.Errorf("React Native schema-queued-mutation server alias %q is absent", alias.Alias)
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
	resolved, err := blackbox.ResolveNativeIdentityAliases(c.identities, observations)
	if err != nil {
		return scenarios.StateFacts{}, nil, fmt.Errorf("resolve React Native schema-queued-mutation server identity evidence: %w", err)
	}
	return captures[0].StateFacts, resolved, nil
}

type schemaQueuedMutationServerEvidence struct {
	clientGeneration uint64
	scopeSetVersion  uint64
	rebuildID        string
	rowVersion       string
	rowChecksum      string
}

func (c *SchemaQueuedMutationCoordinator) serverEvidence(ctx context.Context) (schemaQueuedMutationServerEvidence, error) {
	observer, err := c.config.Harness.OpenObserver(ctx)
	if err != nil {
		return schemaQueuedMutationServerEvidence{}, fmt.Errorf("open React Native schema-queued-mutation server observer: %w", err)
	}
	defer observer.Close()
	var generation, scopeSet int64
	err = observer.QueryRowContext(ctx, `
		SELECT client_generation, scope_set_version
		FROM synchro.sync_clients
		WHERE user_id = $1 AND client_id = $2`, c.userID, c.clientID).Scan(&generation, &scopeSet)
	if err != nil || generation <= 0 || scopeSet <= 0 {
		return schemaQueuedMutationServerEvidence{}, fmt.Errorf("read React Native schema-queued-mutation server client generation=%d scope_set_version=%d error=%v", generation, scopeSet, err)
	}
	schemaOne, err := c.runtimeSchema("schema-one")
	if err != nil {
		return schemaQueuedMutationServerEvidence{}, err
	}
	var rebuildCount int64
	var rebuildID string
	err = observer.QueryRowContext(ctx, `
		SELECT count(*), COALESCE(min(rebuild_id::text), '')
		FROM synchro.sync_rebuild_sessions
		WHERE user_id = $1 AND client_id = $2 AND schema_version = $3 AND schema_hash = $4`,
		c.userID, c.clientID, schemaOne.Version, schemaOne.Hash).Scan(&rebuildCount, &rebuildID)
	if err != nil || rebuildCount != 1 || rebuildID == "" {
		return schemaQueuedMutationServerEvidence{}, fmt.Errorf("read React Native schema-queued-mutation baseline rebuild identities=%d id=%q error=%v", rebuildCount, rebuildID, err)
	}
	recordID, err := c.runtimeString("queued-row-primary-key")
	if err != nil {
		return schemaQueuedMutationServerEvidence{}, err
	}
	var rowCount int64
	var rowVersion, rowChecksum string
	err = observer.QueryRowContext(ctx, `
		SELECT count(*), COALESCE(min(captured.row_version::text), ''), COALESCE(min(encode(captured.checksum, 'hex')), '')
		FROM synchro.sync_captured_rows captured
		JOIN synchro.sync_registry registry
		  ON registry.registry_generation = captured.registry_generation
		 AND registry.relation_id = captured.relation_id
		WHERE registry.table_name = $1 AND captured.record_id = $2 AND NOT captured.deleted`,
		c.tableName, recordID).Scan(&rowCount, &rowVersion, &rowChecksum)
	if err != nil || rowCount != 1 || rowVersion == "" || len(rowChecksum) != 64 {
		return schemaQueuedMutationServerEvidence{}, fmt.Errorf("read React Native schema-queued-mutation server rows=%d version=%q checksum=%q error=%v", rowCount, rowVersion, rowChecksum, err)
	}
	return schemaQueuedMutationServerEvidence{
		clientGeneration: uint64(generation), scopeSetVersion: uint64(scopeSet), rebuildID: rebuildID,
		rowVersion: rowVersion, rowChecksum: rowChecksum,
	}, nil
}

func (c *SchemaQueuedMutationCoordinator) validateDurableResult() error {
	if c.expected == nil || len(c.expected.Clients) != 1 || len(c.expected.Clients[0].Queue) != 1 || len(c.expected.Clients[0].Outcomes) != 1 {
		return errors.New("React Native schema-queued-mutation authored durable state is unavailable")
	}
	state, err := decodeClientState(c.finalResult.ClientState)
	if err != nil {
		return err
	}
	schemaTwo, err := c.runtimeSchema("schema-two")
	if err != nil {
		return err
	}
	scope, err := c.runtimeString("scope-a")
	if err != nil {
		return err
	}
	expected := c.expected.Clients[0]
	if state.Schema == nil || *state.Schema != schemaTwo || len(state.ScopeStates) != 1 || state.ScopeStates[0].ScopeID != scope ||
		expected.QueueCount == nil || expected.OutcomeCount == nil || state.MutationLedgerCount != *expected.QueueCount || state.MutationOutcomeCount != *expected.OutcomeCount {
		return fmt.Errorf("React Native schema-queued-mutation client schema=%+v want=%+v scopes=%+v want_scope=%q ledger=%d want=%v outcomes=%d want=%v", state.Schema, schemaTwo, state.ScopeStates, scope, state.MutationLedgerCount, expected.QueueCount, state.MutationOutcomeCount, expected.OutcomeCount)
	}
	if err := c.validatePendingMutation(expected.Queue[0]); err != nil {
		return err
	}
	return c.validateRejectedMutation(expected.Outcomes[0])
}

type schemaQueuedMutationPending struct {
	MutationID        string       `json:"mutationID"`
	LocalOrder        uint64       `json:"localOrder"`
	TableID           string       `json:"tableID"`
	TableName         string       `json:"tableName"`
	RecordID          string       `json:"recordID"`
	PrimaryKeyFieldID string       `json:"primaryKeyFieldID"`
	Operation         string       `json:"operation"`
	AuthoredSchema    clientSchema `json:"authoredSchema"`
	BaseVersion       *string      `json:"baseVersion"`
	ClientVersion     string       `json:"clientVersion"`
	Status            string       `json:"status"`
	SealedBatchID     *string      `json:"sealedBatchID"`
	SealedOrdinal     *uint64      `json:"sealedOrdinal"`
	AuthoredFields    []struct {
		FieldID     string          `json:"fieldID"`
		LogicalType string          `json:"logicalType"`
		Value       json.RawMessage `json:"value"`
	} `json:"authoredFields"`
}

func (c *SchemaQueuedMutationCoordinator) validatePendingMutation(expected scenarios.QueuedMutationFact) error {
	var pending []schemaQueuedMutationPending
	if err := decodeStrictValue(c.finalResult.Pending, &pending); err != nil || len(pending) != 1 {
		return fmt.Errorf("React Native schema-queued-mutation pending entries=%d want=1 decode_error=%v", len(pending), err)
	}
	mutationID, err := c.runtimeString("queued-mutation")
	if err != nil {
		return err
	}
	batchID, err := c.runtimeString("schema-reset-batch")
	if err != nil {
		return err
	}
	tableID, err := c.runtimeString("items-table")
	if err != nil {
		return err
	}
	recordID, err := c.runtimeString("queued-row-primary-key")
	if err != nil {
		return err
	}
	baseVersion, err := c.runtimeString("schema-one-base-version")
	if err != nil {
		return err
	}
	schemaOne, err := c.runtimeSchema("schema-one")
	if err != nil {
		return err
	}
	primaryField, err := c.config.Controller.RuntimeFieldID(expected.TableID, "id")
	if err != nil {
		return fmt.Errorf("resolve React Native schema-queued-mutation primary field: %w", err)
	}
	fieldID, err := c.config.Controller.RuntimeFieldID(expected.TableID, expected.AuthoredColumns[0].FieldID)
	if err != nil {
		return fmt.Errorf("resolve React Native schema-queued-mutation retained field: %w", err)
	}
	observed := pending[0]
	if observed.MutationID != mutationID || observed.TableID != tableID || observed.TableName != c.tableName || observed.RecordID != recordID ||
		observed.PrimaryKeyFieldID != primaryField || observed.Operation != expected.Operation || observed.AuthoredSchema != schemaOne ||
		observed.BaseVersion == nil || *observed.BaseVersion != baseVersion || observed.ClientVersion != expected.ClientVersion ||
		observed.Status != expected.Status || observed.SealedBatchID == nil || *observed.SealedBatchID != batchID || observed.SealedOrdinal == nil || *observed.SealedOrdinal != 1 ||
		observed.LocalOrder != expected.LocalOrder || len(observed.AuthoredFields) != len(expected.AuthoredColumns) {
		return fmt.Errorf("React Native schema-queued-mutation pending observed=%+v expected mutation=%q table=%q table_name=%q record=%q primary=%q schema=%+v base=%q batch=%q order=%d", observed, mutationID, tableID, c.tableName, recordID, primaryField, schemaOne, baseVersion, batchID, expected.LocalOrder)
	}
	field := observed.AuthoredFields[0]
	if field.FieldID != fieldID || field.LogicalType != expected.AuthoredColumns[0].Type || !semanticRawJSONEqual(field.Value, json.RawMessage(expected.AuthoredColumns[0].WireJSON)) {
		return fmt.Errorf("React Native schema-queued-mutation retained field id=%q want=%q type=%q want=%q value=%s want=%s", field.FieldID, fieldID, field.LogicalType, expected.AuthoredColumns[0].Type, field.Value, expected.AuthoredColumns[0].WireJSON)
	}
	return nil
}

type schemaQueuedMutationRejected struct {
	MutationID   string `json:"mutationID"`
	TableName    string `json:"tableName"`
	RecordID     string `json:"recordID"`
	Status       string `json:"status"`
	Code         string `json:"code"`
	MutationJSON string `json:"mutationJSON"`
}

func (c *SchemaQueuedMutationCoordinator) validateRejectedMutation(expected scenarios.MutationOutcomeFact) error {
	var rejected []schemaQueuedMutationRejected
	if err := decodeStrictValue(c.finalResult.Rejected, &rejected); err != nil || len(rejected) != 1 {
		return fmt.Errorf("React Native schema-queued-mutation rejected entries=%d want=1 decode_error=%v", len(rejected), err)
	}
	mutationID, err := c.runtimeString("queued-mutation")
	if err != nil {
		return err
	}
	recordID, err := c.runtimeString("queued-row-primary-key")
	if err != nil {
		return err
	}
	observed := rejected[0]
	if observed.MutationID != mutationID || observed.TableName != c.tableName || observed.RecordID != recordID ||
		observed.Status != expected.State || observed.Code != expected.Reason || observed.MutationJSON == "" {
		return fmt.Errorf("React Native schema-queued-mutation rejected observed=%+v expected mutation=%q table=%q record=%q status=%q code=%q", observed, mutationID, c.tableName, recordID, expected.State, expected.Reason)
	}
	return c.validateStoredMutation(observed.MutationJSON)
}

func (c *SchemaQueuedMutationCoordinator) validateStoredMutation(raw string) error {
	var mutation struct {
		MutationID     string                     `json:"mutation_id"`
		Table          string                     `json:"table"`
		PK             map[string]json.RawMessage `json:"pk"`
		AuthoredSchema clientSchema               `json:"authored_schema"`
		Operation      string                     `json:"op"`
		BaseVersion    *string                    `json:"base_version"`
		ClientVersion  string                     `json:"client_version"`
		Columns        map[string]json.RawMessage `json:"columns"`
	}
	if err := json.Unmarshal([]byte(raw), &mutation); err != nil {
		return fmt.Errorf("decode React Native schema-queued-mutation stored mutation=%q error=%w", raw, err)
	}
	expected := c.expected.Clients[0].Queue[0]
	mutationID, err := c.runtimeString("queued-mutation")
	if err != nil {
		return err
	}
	tableID, err := c.runtimeString("items-table")
	if err != nil {
		return err
	}
	recordID, err := c.runtimeString("queued-row-primary-key")
	if err != nil {
		return err
	}
	baseVersion, err := c.runtimeString("schema-one-base-version")
	if err != nil {
		return err
	}
	schemaOne, err := c.runtimeSchema("schema-one")
	if err != nil {
		return err
	}
	primaryField, err := c.config.Controller.RuntimeFieldID(expected.TableID, "id")
	if err != nil {
		return err
	}
	fieldID, err := c.config.Controller.RuntimeFieldID(expected.TableID, expected.AuthoredColumns[0].FieldID)
	if err != nil {
		return err
	}
	primary := mutation.PK[primaryField]
	column := mutation.Columns[fieldID]
	if mutation.MutationID != mutationID || mutation.Table != tableID || !semanticRawJSONEqual(primary, c.runtimeIDs["queued-row-primary-key"]) ||
		mutation.AuthoredSchema != schemaOne || mutation.Operation != expected.Operation || mutation.BaseVersion == nil || *mutation.BaseVersion != baseVersion ||
		mutation.ClientVersion != expected.ClientVersion || len(mutation.Columns) != 1 || !semanticRawJSONEqual(column, json.RawMessage(expected.AuthoredColumns[0].WireJSON)) {
		return fmt.Errorf("React Native schema-queued-mutation stored mutation=%s expected mutation=%q table=%q primary_field=%q record=%q schema=%+v base=%q field=%q", raw, mutationID, tableID, primaryField, recordID, schemaOne, baseVersion, fieldID)
	}
	return nil
}

func (c *SchemaQueuedMutationCoordinator) command(clientKey, actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
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
			ClientKey: clientKey, Database: c.database, ClientID: c.clientID, ServerURL: c.adapter, AuthToken: c.authTokens[c.clientID],
		},
	}
}

func (c *SchemaQueuedMutationCoordinator) runtimeString(alias string) (string, error) {
	var value string
	if err := json.Unmarshal(c.runtimeIDs[alias], &value); err != nil || value == "" {
		return "", fmt.Errorf("React Native schema-queued-mutation server alias %q value=%s decode_error=%v", alias, c.runtimeIDs[alias], err)
	}
	return value, nil
}

func (c *SchemaQueuedMutationCoordinator) runtimeSchema(alias string) (clientSchema, error) {
	var value clientSchema
	if err := json.Unmarshal(c.runtimeIDs[alias], &value); err != nil || value.Version == 0 || value.Hash == "" {
		return clientSchema{}, fmt.Errorf("React Native schema-queued-mutation server schema alias %q value=%s decode_error=%v", alias, c.runtimeIDs[alias], err)
	}
	return value, nil
}

type schemaQueuedMutationClient struct{ userID, clientID string }

func schemaQueuedMutationClientIdentity(scenario scenarios.Scenario) (schemaQueuedMutationClient, error) {
	var identity schemaQueuedMutationClient
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil || step.NativeBinding.Kind == "controller" {
			continue
		}
		if step.NativeBinding.UserID == "" || step.NativeBinding.ClientID == "" {
			return schemaQueuedMutationClient{}, fmt.Errorf("React Native schema-queued-mutation step %s identity is empty", step.ID)
		}
		if identity.userID == "" {
			identity = schemaQueuedMutationClient{userID: step.NativeBinding.UserID, clientID: step.NativeBinding.ClientID}
			continue
		}
		if identity.userID != step.NativeBinding.UserID || identity.clientID != step.NativeBinding.ClientID {
			return schemaQueuedMutationClient{}, fmt.Errorf("React Native schema-queued-mutation step %s identity user=%q client=%q differs from user=%q client=%q", step.ID, step.NativeBinding.UserID, step.NativeBinding.ClientID, identity.userID, identity.clientID)
		}
	}
	if identity.userID == "" || identity.clientID == "" {
		return schemaQueuedMutationClient{}, errors.New("React Native schema-queued-mutation client identity is absent")
	}
	return identity, nil
}

func schemaQueuedMutationExpectedState(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		expected := scenario.Model.ExpectedState[index]
		if expected.ID == "EXPECT-SCHEMA-QUEUED-MUTATION-STATE-001" && expected.StateFacts != nil {
			return expected.StateFacts
		}
	}
	return nil
}
