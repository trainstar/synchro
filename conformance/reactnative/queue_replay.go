package reactnative

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	queueReplayScenarioPath = "conformance/scenarios/performance/queue-replay-001.json"
	queueReplayScenarioID   = "SCN-PERF-QUEUE-REPLAY-001"
)

// LoadQueueReplayScenario loads the authored queue-replay scenario.
func LoadQueueReplayScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, queueReplayScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native queue-replay scenario: %w", err)
	}
	if err := ValidateQueueReplayScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateQueueReplayScenario rejects changes to the closed RN queue-replay contract.
func ValidateQueueReplayScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != queueReplayScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" || len(scenario.Steps) == 0 {
		return errors.New("React Native queue-replay scenario contract is invalid")
	}
	for index, step := range scenario.Steps {
		if step.ID != scenarios.StepID(fmt.Sprintf("STEP-PERF-QUEUE-REPLAY-%03d", index+1)) || step.NativeBinding == nil ||
			step.NativeBinding.Kind != "workload" || step.NativeBinding.Workload == nil ||
			scenarios.OperationKey(step.Operation) != "workload/prepare" || step.ExpectedOutcome.Disposition != "success" {
			return errors.New("React Native queue-replay workload contract changed")
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 0 {
		return errors.New("React Native queue-replay lifecycle contract changed")
	}
	semantic, performance := false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-QUEUE-REPLAY-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-QUEUE-REPLAY-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !performance {
		return errors.New("React Native queue-replay assertion contract changed")
	}
	ios, android := 0, 0
	for _, obligation := range scenario.ProofObligations {
		switch string(obligation.ObligationID) {
		case "OBL-PERF-QUEUE-REPLAY-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				ios++
			}
		case "OBL-PERF-QUEUE-REPLAY-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				android++
			}
		}
	}
	if ios != 1 || android != 1 {
		return errors.New("React Native queue-replay proof obligations are invalid")
	}
	if _, err := queueReplayWorkloads(scenario); err != nil {
		return fmt.Errorf("React Native queue-replay workload contract is invalid: %w", err)
	}
	return nil
}

// QueueReplayCoordinatorConfig configures one authenticated RN queue-replay sidecar.
type QueueReplayCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// QueueReplayCoordinator is the command sidecar for one RN queue-replay run.
type QueueReplayCoordinator struct {
	config   QueueReplayCoordinatorConfig
	listener net.Listener
	server   *http.Server
	token    string
	adapter  string
	database string

	steps      []queueReplayWorkload
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	userID     string
	clientID   string
	clientKey  string

	mu          sync.Mutex
	prepared    bool
	closed      bool
	completed   bool
	failed      error
	stage       queueReplayStage
	nextSeq     uint64
	process     *actionProcessIdentity
	stepIndex   int
	localIndex  int
	finalResult *finalCapture
	result      QueueReplayCoordinatorResult
}

// QueueReplayCoordinatorResult contains validated server and native identity evidence.
type QueueReplayCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type queueReplayStage uint8

const (
	queueReplayStageOpen queueReplayStage = iota
	queueReplayStageOpened
	queueReplayStageBootstrapped
	queueReplayStageLocalWrite
	queueReplayStageStoppedBeforeSchema
	queueReplayStageSchemaBoundary
	queueReplayStageResponseLoss
	queueReplayStageStoppedAfterLoss
	queueReplayStageReplay
	queueReplayStageCapture
	queueReplayStageComplete
)

type queueReplayWorkload struct {
	step     scenarios.Step
	local    []scenarios.Operation
	publish  scenarios.Operation
	dropPush scenarios.Operation
}

// NewQueueReplayCoordinator creates an authenticated host-loopback listener.
func NewQueueReplayCoordinator(config QueueReplayCoordinatorConfig) (*QueueReplayCoordinator, error) {
	if err := ValidateQueueReplayScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native queue-replay coordinator platform must be ios or android")
	}
	identity, err := queueReplayClientIdentity(config.Scenario)
	if err != nil {
		return nil, err
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native queue-replay coordinator auth token is required")
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
		return nil, errors.New("create React Native queue-replay coordinator capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-queue-replay-")
		if err != nil {
			return nil, errors.New("create React Native queue-replay private database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native queue-replay database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native queue-replay coordinator")
	}
	coordinator := &QueueReplayCoordinator{
		config: config, listener: listener, token: token, adapter: adapterURL, database: database,
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), userID: identity.userID, clientID: identity.clientID, clientKey: identity.clientID,
		nextSeq: 1,
		server:  &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second},
	}
	coordinator.server.Handler = coordinator
	return coordinator, nil
}

// Prepare installs the model and derives every authored workload.
func (c *QueueReplayCoordinator) Prepare(ctx context.Context) error {
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
	if c.config.AuthToken == "" && c.config.Harness != nil {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.userID, time.Now())
		if err != nil {
			return errors.New("mint React Native queue-replay adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native queue-replay coordinator dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native queue-replay contract: %w", err)
	}
	workloads, err := queueReplayWorkloads(c.config.Scenario)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.steps = workloads
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *QueueReplayCoordinator) Serve(ctx context.Context) error {
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

func (c *QueueReplayCoordinator) Handler() http.Handler { return c }

// URL returns the host-loopback sidecar URL for every platform.
func (c *QueueReplayCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *QueueReplayCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// StageCount returns the exact number of exchanges required by this coordinator.
func (c *QueueReplayCoordinator) StageCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 4 // open, bootstrap, final capture, complete response
	for _, workload := range c.steps {
		count += len(workload.local) + 5 // writes, stop, schema check, loss, replay stop, replay
	}
	return count
}

func (c *QueueReplayCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *QueueReplayCoordinator) Result() (QueueReplayCoordinatorResult, error) {
	if c == nil {
		return QueueReplayCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return QueueReplayCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return QueueReplayCoordinatorResult{}, errors.New("React Native queue-replay coordinator has not completed")
	}
	return c.result, nil
}

func (c *QueueReplayCoordinator) Close(ctx context.Context) error {
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

func (c *QueueReplayCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
		c.failed = errors.New("React Native queue-replay exchange sequence is not monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native queue-replay exchange sequence %d failed: %w", exchange.Sequence, err)
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
		c.failed = errors.New("React Native queue-replay exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *QueueReplayCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == queueReplayStageOpen {
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
	case queueReplayStageOpened:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
	case queueReplayStageBootstrapped:
		if err := c.validateSynchronized(envelope.Result, "idle"); err != nil {
			return err
		}
	case queueReplayStageReplay:
		if err := c.validateSynchronized(envelope.Result, "idle"); err != nil {
			return err
		}
		c.stepIndex++
		c.localIndex = 0
	case queueReplayStageLocalWrite:
		if err := c.validateLocal(envelope.Result); err != nil {
			return err
		}
		c.localIndex++
	case queueReplayStageStoppedBeforeSchema, queueReplayStageStoppedAfterLoss:
		if err := c.validateStopped(envelope.Result); err != nil {
			return err
		}
	case queueReplayStageSchemaBoundary:
		if err := c.validateSynchronized(envelope.Result, "error"); err != nil {
			return err
		}
	case queueReplayStageResponseLoss:
		if err := c.validateSynchronized(envelope.Result, "blocked"); err != nil {
			return err
		}
	case queueReplayStageCapture:
		capture, err := decodeCapture(envelope.Result, []string{"pending_mutations", "rejected_mutations", "sync_status", "request_trace"})
		if err != nil {
			return err
		}
		if err := c.validateCapture(capture); err != nil {
			return err
		}
		c.finalResult = &capture
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *QueueReplayCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case queueReplayStageOpen:
		response.Command = c.command("client", "open", map[string]any{"client_key": c.clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil}, nil)
		c.stage = queueReplayStageOpened
	case queueReplayStageOpened:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
		c.stage = queueReplayStageBootstrapped
	case queueReplayStageBootstrapped, queueReplayStageReplay:
		if c.stepIndex == len(c.steps) {
			response.Command = c.command("observer", "capture", map[string]any{"client_keys": []string{c.clientKey}, "sources": []string{"pending-mutations", "rejected-mutations", "sync-status", "request-trace"}}, nil)
			c.stage = queueReplayStageCapture
			break
		}
		operation, err := c.localOperation()
		if err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.commandOperation("client", "execute-step", map[string]any{"client_key": c.clientKey}, operation)
		c.stage = queueReplayStageLocalWrite
	case queueReplayStageLocalWrite:
		if c.stepIndex >= len(c.steps) {
			return exchangeResponse{}, errors.New("React Native queue-replay workload is unavailable")
		}
		if c.localIndex < len(c.steps[c.stepIndex].local) {
			operation, err := c.localOperation()
			if err != nil {
				return exchangeResponse{}, err
			}
			response.Command = c.commandOperation("client", "execute-step", map[string]any{"client_key": c.clientKey}, operation)
			break
		}
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
		c.stage = queueReplayStageStoppedBeforeSchema
	case queueReplayStageStoppedBeforeSchema:
		workload := c.steps[c.stepIndex]
		if observation, err := c.config.Controller.ApplyStep(ctx, workload.publish); err != nil || observation.Disposition != "success" {
			return exchangeResponse{}, fmt.Errorf("publish React Native queue-replay schema for step %s: %w", workload.step.ID, nativeResultError(err, observation.Disposition))
		}
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "error"}, nil)
		c.stage = queueReplayStageSchemaBoundary
	case queueReplayStageSchemaBoundary:
		committedPush, err := pushResponseLossAppliedOperation(c.steps[c.stepIndex].dropPush)
		if err != nil {
			return exchangeResponse{}, fmt.Errorf("prepare React Native queue-replay committed response-loss push: %w", err)
		}
		if err := c.config.Controller.BindApplicationPush(committedPush); err != nil {
			return exchangeResponse{}, fmt.Errorf("bind React Native queue-replay response-loss push: %w", err)
		}
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "reset-schema-and-start", "completion": "blocked"}, nil)
		c.stage = queueReplayStageResponseLoss
	case queueReplayStageResponseLoss:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
		c.stage = queueReplayStageStoppedAfterLoss
	case queueReplayStageStoppedAfterLoss:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
		c.stage = queueReplayStageReplay
	case queueReplayStageCapture:
		if err := c.completeLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.stage = queueReplayStageComplete
		c.completed = true
	default:
		return exchangeResponse{}, errInvalidExchange
	}
	return response, nil
}

func (c *QueueReplayCoordinator) localOperation() (scenarios.Operation, error) {
	if c.stepIndex >= len(c.steps) || c.localIndex >= len(c.steps[c.stepIndex].local) {
		return scenarios.Operation{}, errors.New("React Native queue-replay local operation is unavailable")
	}
	operation, err := c.config.Controller.ApplicationWrite(c.steps[c.stepIndex].local[c.localIndex])
	if err != nil {
		return scenarios.Operation{}, fmt.Errorf("bind React Native queue-replay local write %d for step %s: %w", c.localIndex+1, c.steps[c.stepIndex].step.ID, err)
	}
	return operation, nil
}

func (c *QueueReplayCoordinator) validateLocal(raw json.RawMessage) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "queue-replay local result"); err != nil {
		return err
	}
	var rows uint64
	if json.Unmarshal(members["rows_affected"], &rows) != nil || rows == 0 {
		return errors.New("React Native queue-replay local write affected no rows")
	}
	return c.validateProcess(members["process"])
}

func (c *QueueReplayCoordinator) validateSynchronized(raw json.RawMessage, completion string) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "queue-replay synchronized result"); err != nil {
		return err
	}
	var actual string
	if json.Unmarshal(members["completion"], &actual) != nil || actual != completion || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native queue-replay synchronized result is invalid")
	}
	return c.validateProcess(members["process"])
}

func (c *QueueReplayCoordinator) validateStopped(raw json.RawMessage) error {
	if c.process == nil {
		return errors.New("React Native queue-replay process identity is unavailable")
	}
	return validateStoppedLifecycleResult(raw, *c.process)
}

func (c *QueueReplayCoordinator) validateProcess(raw json.RawMessage) error {
	process, err := decodeActionProcessIdentity(raw)
	if err != nil || c.process == nil {
		return errors.New("React Native queue-replay process identity is invalid")
	}
	if process.ProcessID != c.process.ProcessID || process.DatabaseIdentityFingerprint != c.process.DatabaseIdentityFingerprint {
		return errors.New("React Native queue-replay process identity changed")
	}
	return nil
}

func (c *QueueReplayCoordinator) validateCapture(capture finalCapture) error {
	if validateEmptyArray(capture.Pending) != nil || validateReadyStatus(capture.Status) != nil {
		return errors.New("React Native queue-replay final queue status is invalid")
	}
	var rejected []json.RawMessage
	if json.Unmarshal(capture.Rejected, &rejected) != nil || len(rejected) != c.rejectedCount() {
		return errors.New("React Native queue-replay rejected mutation count is invalid")
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil || trace.Overflowed || trace.SequenceCheckpoint != uint64(len(trace.Observations)) || validateTraceSequence(trace.Observations) != nil {
		return errors.New("React Native queue-replay request trace is invalid")
	}
	pushes := 0
	for _, observation := range trace.Observations {
		if observation.OperationClass == "push" && observation.StatusCode == http.StatusOK {
			pushes++
		}
	}
	if pushes != len(c.steps)*2 {
		return errors.New("React Native queue-replay response-loss and replay push trace is invalid")
	}
	return nil
}

func (c *QueueReplayCoordinator) rejectedCount() int {
	count := 0
	for _, workload := range c.steps {
		var payload queueReplayWorkloadPayload
		if json.Unmarshal(workload.step.Operation.Payload, &payload) == nil {
			count += int(payload.RejectedCount)
		}
	}
	return count
}

func (c *QueueReplayCoordinator) completeLocked(ctx context.Context) error {
	if c.finalResult == nil || c.config.Controller == nil {
		return errors.New("React Native queue-replay final evidence is unavailable")
	}
	captures, err := c.config.Controller.Capture(ctx, []string{c.clientKey}, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return fmt.Errorf("capture React Native queue-replay server state: %w", nativeResultError(err, ""))
	}
	expected, err := queueReplayExpectedState(c.config.Scenario)
	if err != nil {
		return err
	}
	if err := validateServerState(expected, captures[0].StateFacts); err != nil {
		return err
	}
	resolutions, err := c.resolveIdentities()
	if err != nil {
		return err
	}
	c.result = QueueReplayCoordinatorResult{ServerFacts: captures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *QueueReplayCoordinator) resolveIdentities() ([]blackbox.NativeIdentityResolution, error) {
	serverAliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" {
			serverAliases = append(serverAliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(serverAliases)
	if err != nil {
		return nil, fmt.Errorf("resolve React Native queue-replay runtime identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
	}
	trace, err := captureTraceFromRaw(c.finalResult.Trace)
	if err != nil {
		return nil, err
	}
	for _, observation := range trace.Observations {
		if observation.OperationClass != "push" {
			continue
		}
		generation, err := requestInteger(observation, "client_generation")
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(generation)
		if err != nil {
			return nil, err
		}
		c.runtimeIDs["client-generation-one"] = encoded
		break
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value, found := c.runtimeIDs[alias.Alias]
		if !found {
			return nil, fmt.Errorf("React Native queue-replay identity evidence is incomplete: %s", alias.Alias)
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

func (c *QueueReplayCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		for _, workload := range c.steps {
			if workload.step.ID == id {
				steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: workload.step.Operation.ContractOperation, Name: workload.step.Operation.Name, Payload: copyRaw(workload.step.Operation.Payload)}})
			}
		}
	}
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: conformanceRuntime{ClientKey: c.clientKey, Database: c.database, ClientID: c.clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken}}
}

func (c *QueueReplayCoordinator) commandOperation(actor, name string, parameters map[string]any, operation scenarios.Operation) *conformanceCommand {
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: []conformanceStep{{Operation: conformanceOperation{ContractOperation: operation.ContractOperation, Name: operation.Name, Payload: copyRaw(operation.Payload)}}}}, Runtime: conformanceRuntime{ClientKey: c.clientKey, Database: c.database, ClientID: c.clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken}}
}

type queueReplayClient struct{ userID, clientID string }

func queueReplayClientIdentity(scenario scenarios.Scenario) (queueReplayClient, error) {
	if len(scenario.Steps) == 0 || scenario.Steps[0].NativeBinding == nil {
		return queueReplayClient{}, errors.New("React Native queue-replay client identity is invalid")
	}
	binding := scenario.Steps[0].NativeBinding
	if binding.UserID == "" || binding.ClientID == "" {
		return queueReplayClient{}, errors.New("React Native queue-replay client identity is invalid")
	}
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil || step.NativeBinding.UserID != binding.UserID || step.NativeBinding.ClientID != binding.ClientID {
			return queueReplayClient{}, errors.New("React Native queue-replay native identity differs across steps")
		}
	}
	return queueReplayClient{userID: binding.UserID, clientID: binding.ClientID}, nil
}

func queueReplayExpectedState(scenario scenarios.Scenario) (scenarios.StateFacts, error) {
	for _, expected := range scenario.Model.ExpectedState {
		if expected.ID == "EXPECT-PERF-QUEUE-REPLAY-SEMANTIC-001" && expected.StateFacts != nil {
			return *expected.StateFacts, nil
		}
	}
	return scenarios.StateFacts{}, errors.New("React Native queue-replay expected state is absent")
}

type queueReplaySchema struct {
	Version uint64
	Hash    string
	Tables  []queueReplaySchemaTable
}

type queueReplaySchemaTable struct {
	TableID           string                   `json:"table_id"`
	RelationID        string                   `json:"relation_id"`
	Name              string                   `json:"name"`
	Composition       string                   `json:"composition"`
	PrimaryKeyFieldID string                   `json:"primary_key_field_id"`
	CreatedAtFieldID  *string                  `json:"created_at_field_id"`
	UpdatedAtFieldID  *string                  `json:"updated_at_field_id"`
	DeletedAtFieldID  *string                  `json:"deleted_at_field_id"`
	Fields            []queueReplaySchemaField `json:"fields"`
	Indexes           []queueReplaySchemaIndex `json:"indexes"`
}

type queueReplaySchemaField struct {
	FieldID          string          `json:"field_id"`
	Name             string          `json:"name"`
	Type             string          `json:"type"`
	PrimaryKey       bool            `json:"primary_key"`
	Nullable         bool            `json:"nullable"`
	Writable         bool            `json:"writable"`
	DecimalPrecision any             `json:"decimal_precision"`
	DecimalScale     any             `json:"decimal_scale"`
	DefaultWireJSON  json.RawMessage `json:"default_wire_json"`
}

type queueReplaySchemaIndex struct {
	IndexID  string   `json:"index_id"`
	Name     string   `json:"name"`
	FieldIDs []string `json:"field_ids"`
	Unique   bool     `json:"unique"`
}

type queueReplaySetupPayload struct {
	InitialSchema struct {
		Schema struct {
			Version uint64 `json:"version"`
			Hash    string `json:"hash"`
		} `json:"schema"`
		Tables []queueReplaySchemaTable `json:"tables"`
	} `json:"initial_schema"`
}

type queueReplayWorkloadPayload struct {
	Profile       string `json:"profile"`
	UserID        string `json:"user_id"`
	ClientID      string `json:"client_id"`
	TableID       string `json:"table_id"`
	AcceptedCount uint64 `json:"accepted_count"`
	RejectedCount uint64 `json:"rejected_count"`
}

func queueReplayWorkloads(scenario scenarios.Scenario) ([]queueReplayWorkload, error) {
	var setup queueReplaySetupPayload
	if json.Unmarshal(scenario.Model.Setup[0].Payload, &setup) != nil || setup.InitialSchema.Schema.Version == 0 || setup.InitialSchema.Schema.Hash == "" || len(setup.InitialSchema.Tables) != 1 {
		return nil, errors.New("React Native queue-replay initial schema is invalid")
	}
	current := queueReplaySchema{Version: setup.InitialSchema.Schema.Version, Hash: setup.InitialSchema.Schema.Hash, Tables: setup.InitialSchema.Tables}
	workloads := make([]queueReplayWorkload, 0, len(scenario.Steps))
	for index, step := range scenario.Steps {
		local, publish, push, next, err := queueReplayOperations(step, current, uint64(index*2+1))
		if err != nil {
			return nil, err
		}
		workloads = append(workloads, queueReplayWorkload{step: step, local: local, publish: publish, dropPush: push})
		current = next
	}
	return workloads, nil
}

func queueReplayOperations(step scenarios.Step, current queueReplaySchema, commitLSN uint64) ([]scenarios.Operation, scenarios.Operation, scenarios.Operation, queueReplaySchema, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Workload == nil || binding.Workload.AuthoredSchema.Version != current.Version || binding.Workload.AuthoredSchema.Hash != current.Hash {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay step %s schema is invalid", step.ID)
	}
	var payload queueReplayWorkloadPayload
	if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.Profile != "pending_mutations" || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID || payload.TableID == "" || payload.RejectedCount != 1 || payload.AcceptedCount != binding.Workload.RecordCount-1 || len(current.Tables) != 1 || len(binding.Workload.Targets) != 1 || binding.Workload.RecordCount == 0 || binding.Workload.BatchSize == 0 {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay step %s workload is invalid", step.ID)
	}
	table := current.Tables[0]
	if table.TableID != payload.TableID || table.PrimaryKeyFieldID != binding.Workload.Targets[0].PrimaryKeyFieldID || binding.Workload.Targets[0].TableID != table.TableID {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay step %s workload target is invalid", step.ID)
	}
	rejectedField, acceptedField, err := queueReplayMutationFields(table)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, err
	}
	local, err := queueReplayExpandWorkload(step)
	if err != nil || uint64(len(local)) != binding.Workload.RecordCount {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("expand React Native queue-replay workload %s: %w", step.ID, err)
	}
	wire := make([]map[string]any, 0, len(local))
	for ordinal, operation := range local {
		var value struct {
			AuthenticatedUserID string            `json:"authenticated_user_id"`
			ClientID            string            `json:"client_id"`
			MutationID          string            `json:"mutation_id"`
			TableID             string            `json:"table_id"`
			PK                  map[string]string `json:"pk"`
			AuthoredSchema      struct {
				Version uint64 `json:"version"`
				Hash    string `json:"hash"`
			} `json:"authored_schema"`
			Operation     string `json:"operation"`
			ClientVersion string `json:"client_version"`
			Columns       []struct {
				FieldID string `json:"field_id"`
				Value   string `json:"value"`
			} `json:"columns"`
		}
		if json.Unmarshal(operation.Payload, &value) != nil || value.AuthenticatedUserID != binding.UserID || value.ClientID != binding.ClientID || value.MutationID == "" || value.TableID != table.TableID || value.Operation != "insert" || value.ClientVersion != binding.Workload.ClientVersion || value.AuthoredSchema.Version != current.Version || value.AuthoredSchema.Hash != current.Hash || len(value.PK) != 1 || value.PK[table.PrimaryKeyFieldID] == "" {
			return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay local write %d is invalid", ordinal+1)
		}
		columns := make(map[string]any, len(value.Columns))
		for _, column := range value.Columns {
			if column.FieldID == "" || column.FieldID == table.PrimaryKeyFieldID {
				return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay local write %d field is invalid", ordinal+1)
			}
			if _, duplicate := columns[column.FieldID]; duplicate {
				return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay local write %d repeats a field", ordinal+1)
			}
			columns[column.FieldID] = column.Value
		}
		if len(columns) == 0 || ordinal+1 < len(local) && len(columns) != 1 || ordinal+1 == len(local) && len(columns) != 2 {
			return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay local write %d field count is invalid", ordinal+1)
		}
		if _, found := columns[acceptedField.FieldID]; !found {
			return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay local write %d lacks the accepted field", ordinal+1)
		}
		if ordinal+1 < len(local) {
			if _, found := columns[rejectedField.FieldID]; found {
				return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("React Native queue-replay local write %d contains the rejection field", ordinal+1)
			}
		} else if _, found := columns[rejectedField.FieldID]; !found {
			return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, errors.New("React Native queue-replay terminal local write lacks the rejection field")
		}
		wire = append(wire, map[string]any{"mutation_id": value.MutationID, "table": value.TableID, "pk": value.PK, "authored_schema": map[string]any{"version": value.AuthoredSchema.Version, "hash": value.AuthoredSchema.Hash}, "op": value.Operation, "client_version": value.ClientVersion, "columns": columns})
	}
	next, publish, err := queueReplayNextSchema(current, rejectedField.FieldID, current.Version+1)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, err
	}
	batchID := queueReplayUUID("batch", binding.UserID, binding.ClientID, current.Version, binding.Workload.RecordCount)
	push := scenarios.Operation{ContractOperation: "push", Name: "submit", Payload: queueReplayJSON(map[string]any{"authenticated_user_id": binding.UserID, "request": map[string]any{"client_id": binding.ClientID, "client_generation": 1, "batch_id": batchID, "schema": map[string]any{"version": next.Version, "hash": next.Hash}, "mutations": wire}, "delivery": "drop_after_server", "commit_lsn": strconv.FormatUint(commitLSN, 10), "end_lsn": strconv.FormatUint(commitLSN+1, 10)})}
	if err := scenarios.ValidateOperation(push); err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, queueReplaySchema{}, fmt.Errorf("validate React Native queue-replay push: %w", err)
	}
	return local, publish, push, next, nil
}

func queueReplayExpandWorkload(step scenarios.Step) ([]scenarios.Operation, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Workload == nil {
		return nil, fmt.Errorf("step %s has no native workload binding", step.ID)
	}
	parameters := binding.Workload
	kinds := make([]scenarios.NativeWorkloadMutationKind, 0, parameters.RecordCount)
	for _, kind := range parameters.MutationKinds {
		for count := uint64(0); count < kind.Count; count++ {
			kinds = append(kinds, kind)
		}
	}
	if len(kinds) != int(parameters.RecordCount) {
		return nil, fmt.Errorf("step %s workload mutation kinds do not cover record_count", step.ID)
	}
	operations := make([]scenarios.Operation, 0, parameters.RecordCount)
	for ordinal := uint64(0); ordinal < parameters.RecordCount; ordinal++ {
		target := parameters.Targets[ordinal%uint64(len(parameters.Targets))]
		kind := kinds[ordinal]
		fieldIDs := append([]string(nil), kind.FieldIDs...)
		sort.Strings(fieldIDs)
		columns := make([]map[string]string, 0, len(fieldIDs))
		for _, fieldID := range fieldIDs {
			columns = append(columns, map[string]string{"field_id": fieldID, "value": fmt.Sprintf("workload-%d-%06d", parameters.Seed, ordinal+1)})
		}
		payload, err := json.Marshal(map[string]any{"authenticated_user_id": binding.UserID, "client_id": binding.ClientID, "mutation_id": queueReplayNativeUUID(parameters.Seed, target, ordinal/parameters.BatchSize, ordinal%parameters.BatchSize), "table_id": target.TableID, "pk": map[string]string{target.PrimaryKeyFieldID: fmt.Sprintf("workload-%d-%s-%06d", parameters.Seed, target.ScopeID, ordinal+1)}, "authored_schema": map[string]any{"version": parameters.AuthoredSchema.Version, "hash": parameters.AuthoredSchema.Hash}, "operation": kind.Operation, "client_version": parameters.ClientVersion, "columns": columns})
		if err != nil {
			return nil, err
		}
		operation := scenarios.Operation{ContractOperation: "local", Name: "write", Payload: payload}
		if err := scenarios.ValidateOperation(operation); err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	encoded, err := json.Marshal(operations)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != parameters.Expectation.OperationDigest {
		return nil, fmt.Errorf("step %s generated operation digest does not match expectation", step.ID)
	}
	return operations, nil
}

func queueReplayNativeUUID(seed uint64, target scenarios.NativeWorkloadTarget, batchOrdinal, ordinalInBatch uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:native-workload:v1:%d:%s:%s:%d:%d", seed, target.ScopeID, target.TableID, batchOrdinal, ordinalInBatch)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func queueReplayMutationFields(table queueReplaySchemaTable) (queueReplaySchemaField, queueReplaySchemaField, error) {
	fields := make([]queueReplaySchemaField, 0, len(table.Fields))
	for _, field := range table.Fields {
		if !field.PrimaryKey && field.Writable && field.Type == "string" {
			fields = append(fields, field)
		}
	}
	sort.Slice(fields, func(left, right int) bool { return fields[left].FieldID < fields[right].FieldID })
	if len(fields) < 2 {
		return queueReplaySchemaField{}, queueReplaySchemaField{}, errors.New("React Native queue-replay table has fewer than two writable string fields")
	}
	return fields[0], fields[1], nil
}

func queueReplayNextSchema(current queueReplaySchema, removedField string, version uint64) (queueReplaySchema, scenarios.Operation, error) {
	if len(current.Tables) != 1 {
		return queueReplaySchema{}, scenarios.Operation{}, errors.New("React Native queue-replay schema has an unexpected table count")
	}
	table := current.Tables[0]
	addedField := "queue_value_" + strconv.FormatUint(version, 10)
	updated := make([]queueReplaySchemaField, 0, len(table.Fields))
	for _, field := range table.Fields {
		if field.FieldID != removedField {
			updated = append(updated, field)
		}
	}
	updated = append(updated, queueReplaySchemaField{FieldID: addedField, Name: addedField, Type: "string", Nullable: false, Writable: true, DefaultWireJSON: json.RawMessage(`""`)})
	table.Fields = updated
	tables := []map[string]any{queueReplayManifestTable(table)}
	bodyWithoutHash := queueReplayJSON(map[string]any{"parent_schema": map[string]any{"version": current.Version, "hash": current.Hash}, "schema_version": version, "transition_class": "class_4", "compatibility_floor": version, "tables": tables})
	canonical, err := jcs.Transform(bodyWithoutHash)
	if err != nil {
		return queueReplaySchema{}, scenarios.Operation{}, fmt.Errorf("canonicalize React Native queue-replay schema: %w", err)
	}
	digest := sha256.Sum256(append([]byte("synchro:v3:schema-manifest:v1\x00"), canonical...))
	hash := hex.EncodeToString(digest[:])
	body, err := jcs.Transform(queueReplayJSON(map[string]any{"parent_schema": map[string]any{"version": current.Version, "hash": current.Hash}, "schema_version": version, "schema_hash": hash, "transition_class": "class_4", "compatibility_floor": version, "tables": tables}))
	if err != nil {
		return queueReplaySchema{}, scenarios.Operation{}, fmt.Errorf("encode React Native queue-replay schema: %w", err)
	}
	next := queueReplaySchema{Version: version, Hash: hash, Tables: []queueReplaySchemaTable{table}}
	publish := scenarios.Operation{ContractOperation: "model", Name: "publish-schema", Payload: queueReplayJSON(map[string]any{"schema": map[string]any{"version": version, "hash": hash}, "body": string(body), "transition_class": "class_4", "compatibility_floor": version, "tables": []map[string]any{queueReplayProtocolTable(table)}, "affected_scopes": []string{}})}
	if err := scenarios.ValidateOperation(publish); err != nil {
		return queueReplaySchema{}, scenarios.Operation{}, fmt.Errorf("validate React Native queue-replay schema publication: %w", err)
	}
	return next, publish, nil
}

func queueReplayProtocolTable(table queueReplaySchemaTable) map[string]any {
	return map[string]any{"table_id": table.TableID, "relation_id": table.RelationID, "name": table.Name, "composition": table.Composition, "primary_key_field_id": table.PrimaryKeyFieldID, "created_at_field_id": table.CreatedAtFieldID, "updated_at_field_id": table.UpdatedAtFieldID, "deleted_at_field_id": table.DeletedAtFieldID, "fields": queueReplayProtocolFields(table.Fields), "indexes": queueReplayIndexes(table.Indexes)}
}

func queueReplayManifestTable(table queueReplaySchemaTable) map[string]any {
	fields := make([]map[string]any, 0, len(table.Fields))
	for _, field := range table.Fields {
		fields = append(fields, map[string]any{"field_id": field.FieldID, "name": field.Name, "type": field.Type, "nullable": field.Nullable, "writable": field.Writable})
	}
	sort.Slice(fields, func(left, right int) bool {
		return fields[left]["field_id"].(string) < fields[right]["field_id"].(string)
	})
	indexes := queueReplayIndexes(table.Indexes)
	sort.Slice(indexes, func(left, right int) bool {
		return indexes[left]["index_id"].(string) < indexes[right]["index_id"].(string)
	})
	return map[string]any{"table_id": table.TableID, "relation_id": table.RelationID, "name": table.Name, "composition": table.Composition, "primary_key_field_id": table.PrimaryKeyFieldID, "lifecycle": map[string]any{"created_at_field_id": table.CreatedAtFieldID, "updated_at_field_id": table.UpdatedAtFieldID, "deleted_at_field_id": table.DeletedAtFieldID}, "fields": fields, "indexes": indexes}
}

func queueReplayProtocolFields(values []queueReplaySchemaField) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, field := range values {
		var defaultValue any
		if len(field.DefaultWireJSON) != 0 && string(field.DefaultWireJSON) != "null" {
			_ = json.Unmarshal(field.DefaultWireJSON, &defaultValue)
		}
		result = append(result, map[string]any{"field_id": field.FieldID, "name": field.Name, "type": field.Type, "primary_key": field.PrimaryKey, "nullable": field.Nullable, "writable": field.Writable, "decimal_precision": field.DecimalPrecision, "decimal_scale": field.DecimalScale, "default_wire_json": defaultValue})
	}
	return result
}

func queueReplayIndexes(values []queueReplaySchemaIndex) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, index := range values {
		result = append(result, map[string]any{"index_id": index.IndexID, "name": index.Name, "field_ids": index.FieldIDs, "unique": index.Unique})
	}
	return result
}

func queueReplayUUID(kind, userID, clientID string, schemaVersion, ordinal uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:workload:%s:%s:%s:%d:%d", kind, userID, clientID, schemaVersion, ordinal)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func queueReplayJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
