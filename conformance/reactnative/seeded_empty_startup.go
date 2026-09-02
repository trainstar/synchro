package reactnative

import (
	"bytes"
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
	seededEmptyStartupScenarioPath = "conformance/scenarios/performance/seeded-empty-startup-001.json"
	seededEmptyStartupScenarioID   = "SCN-PERF-SEEDED-EMPTY-STARTUP-001"
	portableSeedAssetName          = "seed.db"
)

// SeededEmptyStartupCoordinatorConfig configures one React Native startup sidecar.
type SeededEmptyStartupCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Artifact   *blackbox.NativeArtifact
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// SeededEmptyStartupCoordinator executes all authored seeded and empty starts.
type SeededEmptyStartupCoordinator struct {
	config SeededEmptyStartupCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string
	database string

	clients    []seededEmptyStartupClient
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	authTokens map[string]string
	processes  map[string]actionProcessIdentity

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	nextSeq   uint64
	client    int
	stage     seededEmptyStartupStage
	result    SeededEmptyStartupCoordinatorResult
}

// SeededEmptyStartupCoordinatorResult contains the resolved native identities.
type SeededEmptyStartupCoordinatorResult struct {
	IdentityResolution []blackbox.NativeIdentityResolution
	StartupCount       int
}

type seededEmptyStartupStage uint8

const (
	seededEmptyStartupStageOpen seededEmptyStartupStage = iota
	seededEmptyStartupStageOpened
	seededEmptyStartupStageSynchronized
	seededEmptyStartupStageCaptured
)

type seededEmptyStartupClient struct {
	key                       string
	userID                    string
	clientID                  string
	startupStep               scenarios.Step
	assignmentStep            scenarios.Step
	artifactStep              *scenarios.Step
	connectScopeProjectionLen uint64
	pullScopeProjectionLen    uint64
}

type seededEmptyStartupRuntime struct {
	ClientKey        string  `json:"client_key"`
	Database         string  `json:"database_path"`
	ClientID         string  `json:"client_id"`
	SeedDatabasePath *string `json:"seed_database_path,omitempty"`
	ServerURL        string  `json:"server_url"`
	AuthToken        string  `json:"auth_token"`
}

type seededEmptyStartupCommand struct {
	SchemaVersion int                       `json:"schema_version"`
	Action        conformanceManifest       `json:"action"`
	Runtime       seededEmptyStartupRuntime `json:"runtime"`
}

type seededEmptyStartupResponse struct {
	SchemaVersion int                        `json:"schema_version"`
	Sequence      uint64                     `json:"sequence"`
	State         string                     `json:"state"`
	Command       *seededEmptyStartupCommand `json:"command"`
}

// LoadSeededEmptyStartupScenario loads only the authored startup scenario.
func LoadSeededEmptyStartupScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, seededEmptyStartupScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native seeded-empty-startup scenario: %w", err)
	}
	if err := ValidateSeededEmptyStartupScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateSeededEmptyStartupScenario rejects changes to the closed RN startup contract.
func ValidateSeededEmptyStartupScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != seededEmptyStartupScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native seeded-empty-startup scenario contract is invalid")
	}
	clients, err := seededEmptyStartupClients(scenario)
	if err != nil {
		return err
	}
	if len(clients) == 0 || len(scenario.NativeLifecycleBoundaries) != 0 {
		return errors.New("React Native seeded-empty-startup lifecycle contract is invalid")
	}
	assertions := map[string]bool{}
	for _, assertion := range scenario.Assertions {
		if assertion.Oracle.ExpectedSource != "authored-model" {
			continue
		}
		switch assertion.Predicate.ContractPredicate {
		case "wire-outcome", "performance-measurement":
			assertions[assertion.Predicate.ContractPredicate] = true
		}
	}
	if !assertions["wire-outcome"] || !assertions["performance-measurement"] {
		return errors.New("React Native seeded-empty-startup assertions are invalid")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		switch string(obligation.ObligationID) {
		case "OBL-PERF-SEEDED-EMPTY-STARTUP-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[string(obligation.ObligationID)]++
			}
		case "OBL-PERF-SEEDED-EMPTY-STARTUP-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[string(obligation.ObligationID)]++
			}
		case "OBL-PERF-SEEDED-EMPTY-STARTUP-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-SEEDED-EMPTY-STARTUP-001", "CTRL-SEED-001") {
				obligations[string(obligation.ObligationID)]++
			}
		}
	}
	if obligations["OBL-PERF-SEEDED-EMPTY-STARTUP-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-SEEDED-EMPTY-STARTUP-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-SEEDED-EMPTY-STARTUP-CONTROL-001"] != 1 {
		return errors.New("React Native seeded-empty-startup proof obligations are invalid")
	}
	return nil
}

// NewSeededEmptyStartupCoordinator creates an authenticated host-loopback listener.
func NewSeededEmptyStartupCoordinator(config SeededEmptyStartupCoordinatorConfig) (*SeededEmptyStartupCoordinator, error) {
	if err := ValidateSeededEmptyStartupScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native seeded-empty-startup coordinator platform must be ios or android")
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native seeded-empty-startup coordinator auth token is required")
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
		return nil, errors.New("create React Native seeded-empty-startup coordinator capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-seeded-empty-startup-")
		if err != nil {
			return nil, errors.New("create React Native seeded-empty-startup private database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native seeded-empty-startup database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native seeded-empty-startup coordinator")
	}
	clients, err := seededEmptyStartupClients(config.Scenario)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	coordinator := &SeededEmptyStartupCoordinator{
		config: config, listener: listener, token: token, adapter: adapter, database: database,
		clients: clients, identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), authTokens: make(map[string]string), processes: make(map[string]actionProcessIdentity), nextSeq: 1,
	}
	coordinator.server = &http.Server{Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return coordinator, nil
}

// Prepare installs and stages the authored workload before the app opens a client.
func (c *SeededEmptyStartupCoordinator) Prepare(ctx context.Context) error {
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
	if c.config.Controller == nil || c.config.Harness == nil || c.config.Artifact == nil {
		return errors.New("React Native seeded-empty-startup coordinator dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native seeded-empty-startup contract: %w", err)
	}
	for _, client := range c.clients {
		if client.artifactStep != nil {
			if _, err := c.config.Artifact.StageStep(ctx, client.artifactStep.Operation); err != nil {
				return fmt.Errorf("stage React Native seeded startup artifact for %s: %w", client.clientID, err)
			}
		}
		assignment, err := c.config.Controller.ApplyStep(ctx, client.assignmentStep.Operation)
		if err != nil || assignment.Disposition != "success" {
			return fmt.Errorf("assign React Native startup client %s: %w", client.clientID, nativeResultError(err, assignment.Disposition))
		}
		if c.config.AuthToken != "" {
			c.authTokens[client.key] = c.config.AuthToken
			continue
		}
		token, err := c.config.Harness.NativeBearerToken(ctx, client.userID, time.Now())
		if err != nil {
			return fmt.Errorf("mint React Native startup bearer token for %s: %w", client.clientID, err)
		}
		c.authTokens[client.key] = token
	}
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native seeded-empty-startup server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *SeededEmptyStartupCoordinator) Serve(ctx context.Context) error {
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

func (c *SeededEmptyStartupCoordinator) Handler() http.Handler { return c }

// URL returns the host-loopback URL. Detox consumes this URL on the host.
func (c *SeededEmptyStartupCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *SeededEmptyStartupCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// StageCount returns the exact number of commands that the Detox consumer must execute.
func (c *SeededEmptyStartupCoordinator) StageCount() int {
	if c == nil {
		return 0
	}
	return len(c.clients) * int(seededEmptyStartupStageCaptured)
}

func (c *SeededEmptyStartupCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *SeededEmptyStartupCoordinator) Result() (SeededEmptyStartupCoordinatorResult, error) {
	if c == nil {
		return SeededEmptyStartupCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return SeededEmptyStartupCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return SeededEmptyStartupCoordinatorResult{}, errors.New("React Native seeded-empty-startup coordinator has not completed")
	}
	return c.result, nil
}

func (c *SeededEmptyStartupCoordinator) Close(ctx context.Context) error {
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

func (c *SeededEmptyStartupCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
		c.failed = errors.New("React Native seeded-empty-startup exchange sequence is not monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native seeded-empty-startup exchange %d failed: %w", exchange.Sequence, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(exchange.Sequence)
	if err != nil {
		c.failed = err
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = errors.New("React Native seeded-empty-startup response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *SeededEmptyStartupCoordinator) acceptLocked(raw json.RawMessage) error {
	if c.stage == seededEmptyStartupStageOpen {
		if !isJSONNull(raw) {
			return errInvalidExchange
		}
		return nil
	}
	client := c.clients[c.client]
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return errInvalidExchange
	}
	switch c.stage {
	case seededEmptyStartupStageOpened:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.processes[client.key] = process
		return nil
	case seededEmptyStartupStageSynchronized:
		return c.validateSynchronized(client, envelope.Result)
	case seededEmptyStartupStageCaptured:
		return c.validateCapture(client, envelope.Result)
	default:
		return errInvalidExchange
	}
}

func (c *SeededEmptyStartupCoordinator) advanceLocked(sequence uint64) (seededEmptyStartupResponse, error) {
	response := seededEmptyStartupResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	client := c.clients[c.client]
	switch c.stage {
	case seededEmptyStartupStageOpen:
		response.Command = c.command(client, "open", map[string]any{"client_key": client.key, "database_mode": "reuse", "initialization": client.initialization(), "seed_step_id": client.seedStepID()})
		c.stage = seededEmptyStartupStageOpened
	case seededEmptyStartupStageOpened:
		response.Command = c.command(client, "synchronize-step", map[string]any{"client_key": client.key, "method": client.startupStep.NativeBinding.Method, "completion": client.startupStep.NativeBinding.Completion})
		c.stage = seededEmptyStartupStageSynchronized
	case seededEmptyStartupStageSynchronized:
		response.Command = c.command(client, "capture", map[string]any{"client_keys": []string{client.key}, "sources": []string{"request-trace"}})
		c.stage = seededEmptyStartupStageCaptured
	case seededEmptyStartupStageCaptured:
		c.client++
		if c.client == len(c.clients) {
			resolutions, err := c.resolveIdentities()
			if err != nil {
				return seededEmptyStartupResponse{}, err
			}
			c.result = SeededEmptyStartupCoordinatorResult{IdentityResolution: resolutions, StartupCount: len(c.clients)}
			c.completed = true
			response.State = "complete"
			return response, nil
		}
		next := c.clients[c.client]
		response.Command = c.command(next, "open", map[string]any{"client_key": next.key, "database_mode": "reuse", "initialization": next.initialization(), "seed_step_id": next.seedStepID()})
		c.stage = seededEmptyStartupStageOpened
	default:
		return seededEmptyStartupResponse{}, errInvalidExchange
	}
	return response, nil
}

func (c *SeededEmptyStartupCoordinator) validateSynchronized(client seededEmptyStartupClient, raw json.RawMessage) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "seeded-startup synchronized result"); err != nil {
		return err
	}
	var completion string
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || json.Unmarshal(members["completion"], &completion) != nil || completion != client.startupStep.NativeBinding.Completion ||
		validateReadyStatus(members["status"]) != nil || process != c.processes[client.key] {
		return errors.New("React Native seeded-empty-startup synchronized result is invalid")
	}
	return nil
}

func (c *SeededEmptyStartupCoordinator) validateCapture(client seededEmptyStartupClient, raw json.RawMessage) error {
	capture, err := decodeCapture(raw, []string{"request_trace"})
	if err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "seeded-startup capture result"); err != nil {
		return err
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != c.processes[client.key] {
		return errors.New("React Native seeded-empty-startup capture process changed")
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	if err := validateSeededEmptyStartupBootstrapTrace(trace, client.connectScopeProjectionLen, client.pullScopeProjectionLen); err != nil {
		return fmt.Errorf("React Native seeded-empty-startup %s trace is invalid: %w", client.clientID, err)
	}
	if err := validateSeededEmptyStartupWire(c.config.Scenario, client.startupStep.ID, trace.Observations[0]); err != nil {
		return err
	}
	return c.observeTraceIdentities(trace)
}

func validateSeededEmptyStartupBootstrapTrace(trace traceSnapshot, expectedConnectScopeCount, expectedPullScopeCount uint64) error {
	if expectedConnectScopeCount > expectedPullScopeCount {
		return errors.New("React Native seeded-empty-startup bootstrap scope projections are invalid")
	}
	expectedRebuildScopeCount := expectedPullScopeCount - expectedConnectScopeCount
	minimumObservationCount := expectedRebuildScopeCount + 2
	if trace.Overflowed || uint64(len(trace.Observations)) < minimumObservationCount || trace.SequenceCheckpoint != uint64(len(trace.Observations)) {
		operations := make([]string, len(trace.Observations))
		for index := range trace.Observations {
			operations[index] = trace.Observations[index].OperationClass
		}
		return fmt.Errorf("React Native seeded-empty-startup bootstrap trace observed operations=%v count=%d checkpoint=%d overflowed=%t, expected connect plus at least %d rebuild pages and pull with count>=%d checkpoint=count overflowed=false", operations, len(trace.Observations), trace.SequenceCheckpoint, trace.Overflowed, expectedRebuildScopeCount, minimumObservationCount)
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return err
	}
	if err := validateTraceOperation(trace.Observations[0], "connect"); err != nil {
		return fmt.Errorf("React Native seeded-empty-startup bootstrap connect trace is invalid: %w", err)
	}
	for index := 1; index < len(trace.Observations)-1; index++ {
		if err := validateTraceOperation(trace.Observations[index], "rebuild"); err != nil {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d trace is invalid: %w", index, err)
		}
	}
	pullIndex := len(trace.Observations) - 1
	if err := validateTraceOperation(trace.Observations[pullIndex], "pull"); err != nil {
		return fmt.Errorf("React Native seeded-empty-startup bootstrap pull trace is invalid: %w", err)
	}
	connectScopeCount, err := requestInteger(trace.Observations[0], "scope_count")
	if err != nil {
		return fmt.Errorf("React Native seeded-empty-startup bootstrap connect scope_count observed unavailable, expected %d: %w", expectedConnectScopeCount, err)
	}
	if connectScopeCount != expectedConnectScopeCount {
		return fmt.Errorf("React Native seeded-empty-startup bootstrap connect scope_count observed %d, expected %d", connectScopeCount, expectedConnectScopeCount)
	}
	if pullScopeCount, err := requestInteger(trace.Observations[pullIndex], "scope_count"); err != nil || pullScopeCount != expectedPullScopeCount {
		if err != nil {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap pull scope_count observed unavailable, expected %d: %w", expectedPullScopeCount, err)
		}
		return fmt.Errorf("React Native seeded-empty-startup bootstrap pull scope_count observed %d, expected %d", pullScopeCount, expectedPullScopeCount)
	}
	rebuiltScopes := make(map[string]struct {
		rebuildID string
		complete  bool
	}, expectedRebuildScopeCount)
	activeScope := ""
	for index := 1; index < pullIndex; index++ {
		rebuild, err := decodeRebuildResponseFacts(trace.Observations[index].RebuildResponseFacts)
		if err != nil {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d response facts are invalid", index)
		}
		scopeFingerprint, err := requestString(trace.Observations[index], "scope_fingerprint")
		if err != nil || *rebuild.ScopeFingerprint != scopeFingerprint {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d scope identity is invalid", index)
		}
		rebuildID, err := requestString(trace.Observations[index], "rebuild_id_fingerprint")
		if err != nil {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d identity is invalid", index)
		}
		state, found := rebuiltScopes[scopeFingerprint]
		if !found {
			if activeScope != "" {
				return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d scope changed before %q completed", index, activeScope)
			}
			state.rebuildID = rebuildID
			activeScope = scopeFingerprint
		} else if state.complete {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d scope identity has a page after finality", index)
		} else if activeScope != scopeFingerprint || state.rebuildID != rebuildID {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d continuation identity is invalid", index)
		}
		if *rebuild.HasMore {
			if !*rebuild.HasCursor || *rebuild.HasFinalScopeCursor || *rebuild.HasChecksum || rebuild.FinalScopeCursorFingerprint != nil {
				return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d intermediate page finality is invalid", index)
			}
		} else {
			if *rebuild.HasCursor || !*rebuild.HasFinalScopeCursor || !*rebuild.HasChecksum || rebuild.FinalScopeCursorFingerprint == nil {
				return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild %d final page finality is invalid", index)
			}
			state.complete = true
			activeScope = ""
		}
		rebuiltScopes[scopeFingerprint] = state
	}
	if activeScope != "" {
		return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild scope %q is incomplete", activeScope)
	}
	if uint64(len(rebuiltScopes)) != expectedRebuildScopeCount {
		return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild scope count observed %d, expected %d", len(rebuiltScopes), expectedRebuildScopeCount)
	}
	for scopeFingerprint, state := range rebuiltScopes {
		if !state.complete {
			return fmt.Errorf("React Native seeded-empty-startup bootstrap rebuild scope %q has no final page", scopeFingerprint)
		}
	}
	return nil
}

func (c *SeededEmptyStartupCoordinator) observeTraceIdentities(trace traceSnapshot) error {
	pull := trace.Observations[len(trace.Observations)-1]
	generation, err := requestInteger(pull, "client_generation")
	if err != nil || generation == 0 {
		return errors.New("React Native seeded-empty-startup client generation is invalid")
	}
	scopeSetVersion, err := requestInteger(pull, "scope_set_version")
	if err != nil || scopeSetVersion == 0 {
		return errors.New("React Native seeded-empty-startup scope-set version is invalid")
	}
	for _, alias := range c.identities {
		var value any
		switch alias.Kind {
		case "client-generation":
			value = generation
		case "scope-set-version":
			value = scopeSetVersion
		default:
			continue
		}
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return fmt.Errorf("encode React Native seeded-empty-startup alias %q: %w", alias.Alias, marshalErr)
		}
		if prior, found := c.runtimeIDs[alias.Alias]; found && !bytes.Equal(prior, encoded) {
			return fmt.Errorf("React Native seeded-empty-startup alias %q changed between clients", alias.Alias)
		}
		c.runtimeIDs[alias.Alias] = encoded
	}
	return nil
}

func (c *SeededEmptyStartupCoordinator) resolveIdentities() ([]blackbox.NativeIdentityResolution, error) {
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("React Native seeded-empty-startup alias %q has no runtime evidence", alias.Alias)
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

func (c *SeededEmptyStartupCoordinator) command(client seededEmptyStartupClient, name string, parameters map[string]any) *seededEmptyStartupCommand {
	runtime := seededEmptyStartupRuntime{ClientKey: client.key, Database: fmt.Sprintf("%s-%d", c.database, c.client), ClientID: client.clientID, ServerURL: c.adapter, AuthToken: c.authTokens[client.key]}
	if client.artifactStep != nil {
		seed := portableSeedAssetName
		runtime.SeedDatabasePath = &seed
	}
	steps := []conformanceStep{}
	if name == "synchronize-step" {
		steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: client.startupStep.Operation.ContractOperation, Name: client.startupStep.Operation.Name, Payload: copyRaw(client.startupStep.Operation.Payload)}})
	}
	actor := "client"
	if name == "capture" {
		actor = "observer"
	}
	return &seededEmptyStartupCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: runtime}
}

func (client seededEmptyStartupClient) initialization() string {
	if client.artifactStep != nil {
		return "seed"
	}
	return "empty"
}

func (client seededEmptyStartupClient) seedStepID() any {
	if client.artifactStep == nil {
		return nil
	}
	return string(client.artifactStep.ID)
}

func seededEmptyStartupClients(scenario scenarios.Scenario) ([]seededEmptyStartupClient, error) {
	type identity struct {
		UserID       string            `json:"user_id"`
		ClientID     string            `json:"client_id"`
		SeedReceipts map[string]string `json:"seed_receipts"`
		Assignments  []struct {
			ScopeID string `json:"scope_id"`
		} `json:"assignments"`
	}
	artifacts := make(map[string]scenarios.Step)
	assignments := make(map[string]scenarios.Step)
	pullScopeProjectionLens := make(map[string]uint64)
	clients := make([]seededEmptyStartupClient, 0)
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil || step.ExpectedOutcome.Disposition != "success" {
			return nil, errors.New("React Native seeded-empty-startup step binding is invalid")
		}
		var payload identity
		if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.UserID == "" || payload.ClientID == "" {
			return nil, errors.New("React Native seeded-empty-startup step identity is invalid")
		}
		switch step.NativeBinding.Kind {
		case "artifact":
			if scenarios.OperationKey(step.Operation) != "artifact/install-portable-seed" || step.NativeBinding.UserID != payload.UserID || step.NativeBinding.ClientID != payload.ClientID {
				return nil, errors.New("React Native seeded-empty-startup artifact binding is invalid")
			}
			if _, duplicate := artifacts[payload.ClientID]; duplicate {
				return nil, errors.New("React Native seeded-empty-startup artifact client is duplicated")
			}
			artifacts[payload.ClientID] = step
		case "controller":
			if scenarios.OperationKey(step.Operation) != "model/set-client-assignments" || len(payload.Assignments) == 0 {
				return nil, errors.New("React Native seeded-empty-startup assignment binding is invalid")
			}
			if _, duplicate := assignments[payload.ClientID]; duplicate {
				return nil, errors.New("React Native seeded-empty-startup assignment client is duplicated")
			}
			assignments[payload.ClientID] = step
			pullScopeProjectionLens[payload.ClientID] = uint64(len(payload.Assignments)) + 1
		case "public-call":
			if scenarios.OperationKey(step.Operation) != "connect/send" || step.NativeBinding.UserID != payload.UserID || step.NativeBinding.ClientID != payload.ClientID ||
				step.NativeBinding.Method == "" || step.NativeBinding.Completion == "" {
				return nil, errors.New("React Native seeded-empty-startup call binding is invalid")
			}
			assignment, found := assignments[payload.ClientID]
			if !found {
				return nil, errors.New("React Native seeded-empty-startup client has no assignment")
			}
			artifact, seeded := artifacts[payload.ClientID]
			if seeded && artifact.ID > step.ID {
				return nil, errors.New("React Native seeded-empty-startup artifact follows startup")
			}
			if seeded != (len(payload.SeedReceipts) != 0) {
				return nil, fmt.Errorf("React Native seeded-empty-startup seed receipt projection has artifact %t and receipt count %d, expected an artifact with receipts or no artifact with zero receipts", seeded, len(payload.SeedReceipts))
			}
			for _, current := range clients {
				if current.clientID == payload.ClientID {
					return nil, errors.New("React Native seeded-empty-startup startup client is duplicated")
				}
			}
			connectScopeProjectionLen := uint64(len(payload.SeedReceipts))
			pullScopeProjectionLen := pullScopeProjectionLens[payload.ClientID]
			if connectScopeProjectionLen > pullScopeProjectionLen {
				return nil, errors.New("React Native seeded-empty-startup seed receipt projection exceeds assigned scopes")
			}
			client := seededEmptyStartupClient{
				key: fmt.Sprintf("seeded-empty-startup-%d", len(clients)+1), userID: payload.UserID, clientID: payload.ClientID,
				startupStep: step, assignmentStep: assignment, connectScopeProjectionLen: connectScopeProjectionLen,
				pullScopeProjectionLen: pullScopeProjectionLen,
			}
			if seeded {
				artifactCopy := artifact
				client.artifactStep = &artifactCopy
			}
			clients = append(clients, client)
		default:
			return nil, errors.New("React Native seeded-empty-startup native binding kind is invalid")
		}
	}
	if len(clients) == 0 || len(artifacts) == 0 || len(artifacts) == len(clients) {
		return nil, errors.New("React Native seeded-empty-startup must contain seeded and empty clients")
	}
	return clients, nil
}

func validateSeededEmptyStartupWire(scenario scenarios.Scenario, stepID scenarios.StepID, observed transportObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != stepID {
			continue
		}
		if observed.OperationClass != "connect" || observed.StatusCode != expected.HTTPStatus || expected.ErrorCode != nil || expected.Retryable {
			return fmt.Errorf("React Native seeded-empty-startup wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("React Native seeded-empty-startup wire expectation %s is absent", stepID)
}
