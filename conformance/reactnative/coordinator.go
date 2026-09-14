package reactnative

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	warmConnectScenarioPath = "conformance/scenarios/performance/warm-connect-001.json"
	clientKey               = "client-a"
	userID                  = "user-a"
	clientID                = "client-a"
	defaultAppVersion       = "0.3.0"
	maximumExchangeBytes    = 1 << 20
)

var (
	errCoordinatorUnavailable = errors.New("React Native coordinator is unavailable")
	errInvalidExchange        = errors.New("React Native exchange request is invalid")
)

// CoordinatorConfig configures one authenticated React Native scenario sidecar.
type CoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// Coordinator is the loopback command sidecar for one React Native warm-connect run.
type Coordinator struct {
	config CoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string
	database string

	steps      map[scenarios.StepID]scenarios.Step
	expected   *scenarios.StateFacts
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	tableName  string
	primaryKey string

	mu          sync.Mutex
	prepared    bool
	closed      bool
	completed   bool
	failed      error
	stage       exchangeStage
	nextSeq     uint64
	bootstrap   *traceSnapshot
	finalResult *finalCapture
	process     *actionProcessIdentity
	result      CoordinatorResult
}

// CoordinatorResult contains the server projection and resolved native identities.
type CoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type exchangeStage uint8

const (
	stageOpen exchangeStage = iota
	stageBootstrapSynchronize
	stageBootstrapTrace
	stageStop
	stageWarmSynchronize
	stageFinalCapture
	stageApplicationRows
	stageComplete
)

type exchangeRequest struct {
	SchemaVersion int
	Sequence      uint64
	Result        json.RawMessage
}

type exchangeResponse struct {
	SchemaVersion int                 `json:"schema_version"`
	Sequence      uint64              `json:"sequence"`
	State         string              `json:"state"`
	Command       *conformanceCommand `json:"command"`
}

type conformanceCommand struct {
	SchemaVersion int                 `json:"schema_version"`
	Action        conformanceManifest `json:"action"`
	Runtime       conformanceRuntime  `json:"runtime"`
}

type conformanceManifest struct {
	Action conformanceAction `json:"action"`
	Steps  []conformanceStep `json:"steps"`
}

type conformanceAction struct {
	Actor      string         `json:"actor"`
	Command    string         `json:"command"`
	Parameters map[string]any `json:"parameters"`
}

type conformanceStep struct {
	Operation conformanceOperation `json:"operation"`
}

type conformanceOperation struct {
	ContractOperation string          `json:"contract_operation"`
	Name              string          `json:"name"`
	Payload           json.RawMessage `json:"payload"`
}

type conformanceRuntime struct {
	ClientKey string `json:"client_key"`
	Database  string `json:"database_path"`
	ClientID  string `json:"client_id"`
	ServerURL string `json:"server_url"`
	AuthToken string `json:"auth_token"`
	// The authored pull page size, when a scenario authors one. The client
	// default applies when absent, which matches the native runner openings.
	PullPageSize uint64 `json:"pull_page_size,omitempty"`
	// The push batch size the passing native fixtures configure, when a
	// scenario's workload requires it. The client default applies when absent.
	PushBatchSize uint64 `json:"push_batch_size,omitempty"`
}

type resultEnvelope struct {
	SchemaVersion int
	Outcome       string
	Result        json.RawMessage
	ErrorCode     *string
	// ErrorDetail carries the device-side message for a failed command. An
	// error code alone cannot name the cause, and the device is torn down
	// before its log can be read.
	ErrorDetail *string
}

type finalCapture struct {
	ClientState  json.RawMessage
	Pending      json.RawMessage
	Rejected     json.RawMessage
	Status       json.RawMessage
	Events       json.RawMessage
	Provenance   json.RawMessage
	Trace        json.RawMessage
	DurableProof json.RawMessage
	Rows         json.RawMessage
}

// LoadWarmConnectScenario loads only the authored warm-connect scenario.
func LoadWarmConnectScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, warmConnectScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native warm-connect scenario: %w", err)
	}
	if err := ValidateScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// NewCoordinator creates an authenticated loopback listener.
// Prepare must run before Serve when real black-box dependencies are supplied.
func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if err := ValidateScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native coordinator platform must be ios or android")
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native coordinator auth token is required")
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
		return nil, errors.New("create React Native coordinator capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseName()
		if err != nil {
			return nil, errors.New("create React Native private database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native coordinator database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native coordinator")
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	coordinator := &Coordinator{
		config:     config,
		listener:   listener,
		token:      token,
		adapter:    adapterURL,
		database:   database,
		steps:      steps,
		expected:   warmConnectExpectedState(config.Scenario),
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage),
		nextSeq:    1,
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

// Prepare installs the authored model, applies the authored assignment, and binds runtime identities.
func (c *Coordinator) Prepare(ctx context.Context) error {
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
			return errors.New("mint React Native adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if c.config.Controller != nil {
		if c.config.Harness == nil {
			return errors.New("React Native coordinator harness is unavailable")
		}
		if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
			return fmt.Errorf("install React Native warm-connect contract: %w", err)
		}
		assignment, err := c.config.Controller.ApplyStep(ctx, c.steps["STEP-PERF-WARM-CONNECT-ASSIGN-001"].Operation)
		if err != nil || assignment.Disposition != "success" {
			return fmt.Errorf("apply React Native warm-connect assignment: %w", nativeResultError(err, assignment.Disposition))
		}
		values, err := c.config.Controller.IdentityValues(c.identities)
		if err != nil {
			return fmt.Errorf("resolve React Native warm-connect runtime identities: %w", err)
		}
		for _, value := range values {
			c.runtimeIDs[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
			switch value.Alias {
			case "items-table":
				c.tableName = value.ApplicationIdentifier
			case "items-primary-key":
				c.primaryKey = value.ApplicationIdentifier
			}
		}
	}
	if c.config.Controller != nil && (c.tableName == "" || c.primaryKey == "") {
		return errors.New("React Native coordinator runtime application identities are unavailable")
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *Coordinator) Serve(ctx context.Context) error {
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
func (c *Coordinator) Handler() http.Handler { return c }

// URL returns the host-only loopback sidecar URL. Android must not rewrite this URL.
func (c *Coordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

// Token returns the capability required by the exchange endpoint.
func (c *Coordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// Completed reports whether the coordinator validated the final application row capture.
func (c *Coordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

// Result returns the validated final server and identity evidence.
func (c *Coordinator) Result() (CoordinatorResult, error) {
	if c == nil {
		return CoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return CoordinatorResult{}, c.failed
	}
	if !c.completed {
		return CoordinatorResult{}, errors.New("React Native coordinator has not completed")
	}
	return c.result, nil
}

// Close stops the sidecar. It does not close the externally owned black-box controller.
func (c *Coordinator) Close(ctx context.Context) error {
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

func (c *Coordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if request.Header.Get("Content-Type") != "application/json" {
		writeExchangeError(writer, http.StatusUnsupportedMediaType)
		return
	}
	if request.ContentLength > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusRequestEntityTooLarge)
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
	if c.closed || !c.prepared || c.failed != nil || c.completed {
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if exchange.Sequence != c.nextSeq {
		c.failed = errors.New("React Native exchange sequence is not monotonic")
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
		c.failed = errors.New("React Native exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *Coordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == stageOpen {
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
	case stageBootstrapSynchronize:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
		return nil
	case stageBootstrapTrace:
		return validateActionResult(envelope.Result, "synchronized")
	case stageStop:
		if err := validateActionResult(envelope.Result, "capture"); err != nil {
			return err
		}
		capture, err := decodeCapture(envelope.Result, []string{"request_trace", "durable_proof"})
		if err != nil {
			return err
		}
		trace, err := captureTraceFromRaw(capture.Trace)
		if err != nil {
			return err
		}
		if err := validateBootstrapTrace(trace); err != nil {
			return err
		}
		if err := validateBootstrapRebuildEvidence(capture.DurableProof, trace); err != nil {
			return err
		}
		c.bootstrap = &trace
		return nil
	case stageWarmSynchronize:
		if c.process == nil {
			return errInvalidExchange
		}
		return validateStoppedLifecycleResult(envelope.Result, *c.process)
	case stageFinalCapture:
		if err := validateActionResult(envelope.Result, "synchronized"); err != nil {
			return err
		}
		return nil
	case stageApplicationRows:
		if err := validateActionResult(envelope.Result, "capture"); err != nil {
			return err
		}
		capture, err := decodeCapture(envelope.Result, []string{
			"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof",
		})
		if err != nil {
			return err
		}
		if err := validateFinalCapture(c.config.Scenario, capture, c.bootstrap); err != nil {
			return err
		}
		c.finalResult = &capture
		return nil
	case stageComplete:
		if err := validateActionResult(envelope.Result, "capture"); err != nil {
			return err
		}
		rows, err := captureRows(envelope.Result)
		if err != nil {
			return err
		}
		if c.finalResult == nil {
			return errors.New("React Native final capture is unavailable")
		}
		c.finalResult.Rows = rows
		return nil
	default:
		return errInvalidExchange
	}
}

func (c *Coordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case stageOpen:
		response.Command = c.command("client", "open", map[string]any{
			"client_key": clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil,
		}, nil)
	case stageBootstrapSynchronize:
		response.Command = c.command("client", "synchronize-step", map[string]any{
			"client_key": clientKey, "method": "start", "completion": "idle",
		}, []scenarios.StepID{
			"STEP-PERF-WARM-CONNECT-BOOTSTRAP-CONNECT-001",
			"STEP-PERF-WARM-CONNECT-BASELINE-REBUILD-001",
			"STEP-PERF-WARM-CONNECT-BASELINE-ACK-001",
		})
	case stageBootstrapTrace:
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{clientKey}, "sources": []string{"request-trace", "durable-proof"},
			"durable_proof_identity": map[string]any{
				"table_name": c.tableName, "record_id": "bootstrap-absent-row",
			},
		}, nil)
	case stageStop:
		response.Command = c.command("client", "lifecycle", map[string]any{
			"client_key": clientKey, "operation": "stop",
		}, nil)
	case stageWarmSynchronize:
		if c.config.Controller == nil {
			return exchangeResponse{}, errors.New("React Native coordinator controller is unavailable")
		}
		committed, err := c.config.Controller.ApplyStep(ctx, c.steps["STEP-PERF-WARM-CONNECT-COMMIT-001"].Operation)
		if err != nil || committed.Disposition != "success" {
			return exchangeResponse{}, fmt.Errorf("commit React Native warm-connect source row: %w", nativeResultError(err, committed.Disposition))
		}
		materialized, err := c.config.Controller.ProcessStep(ctx, nil, c.steps["STEP-PERF-WARM-CONNECT-MATERIALIZE-001"].Operation)
		if err != nil || materialized.Disposition != "success" {
			return exchangeResponse{}, fmt.Errorf("materialize React Native warm-connect source row: %w", nativeResultError(err, materialized.Disposition))
		}
		response.Command = c.command("client", "synchronize-step", map[string]any{
			"client_key": clientKey, "method": "start", "completion": "idle",
		}, []scenarios.StepID{
			"STEP-PERF-WARM-CONNECT-001",
			"STEP-PERF-WARM-CONNECT-002",
		})
	case stageFinalCapture:
		response.Command = c.command("observer", "capture", map[string]any{
			"client_keys": []string{clientKey},
			"sources":     []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"},
		}, nil)
	case stageApplicationRows:
		if c.finalResult == nil {
			return exchangeResponse{}, errors.New("React Native final capture is unavailable")
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
	case stageComplete:
		if c.finalResult == nil {
			return exchangeResponse{}, errors.New("React Native final capture is unavailable")
		}
		if err := c.validateCompletionLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	}
	if c.stage != stageComplete {
		c.stage++
	}
	return response, nil
}

func (c *Coordinator) validateCompletionLocked(ctx context.Context) error {
	if c.config.Controller == nil {
		return errors.New("React Native coordinator controller is unavailable")
	}
	if c.finalResult == nil {
		return errors.New("React Native final capture is unavailable")
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
		return errors.New("React Native application row identity is invalid")
	}
	serverCaptures, err := c.config.Controller.Capture(ctx, []string{clientKey}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return fmt.Errorf("capture React Native server state: %w", nativeResultError(err, ""))
	}
	if err := validateServerState(*c.expected, serverCaptures[0].StateFacts); err != nil {
		return err
	}
	resolutions, err := c.resolveIdentities(metadata)
	if err != nil {
		return err
	}
	if err := validateClientStateAgainstModel(c.config.Scenario, c.finalResult, resolutions, c.tableName, c.primaryKey); err != nil {
		return err
	}
	c.result = CoordinatorResult{ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *Coordinator) resolveIdentities(metadata durableMetadata) ([]blackbox.NativeIdentityResolution, error) {
	if c.finalResult == nil || c.bootstrap == nil {
		return nil, errors.New("React Native identity evidence is incomplete")
	}
	clientState, err := decodeClientState(c.finalResult.ClientState)
	if err != nil {
		return nil, err
	}
	if len(clientState.ScopeStates) != 1 || len(clientState.ScopeRows) != 1 || clientState.Schema == nil {
		return nil, errors.New("React Native client identity state is incomplete")
	}
	trace, err := captureTraceFromRaw(c.finalResult.Trace)
	if err != nil {
		return nil, err
	}
	warm, err := warmTrace(trace, c.bootstrap)
	if err != nil {
		return nil, err
	}
	if len(c.bootstrap.Observations) != 3 {
		return nil, errors.New("React Native bootstrap trace is incomplete")
	}
	generation, err := requestInteger(c.bootstrap.Observations[1], "client_generation")
	if err != nil {
		return nil, err
	}
	scopeSetVersion, err := requestInteger(warm[0], "scope_set_version")
	if err != nil {
		return nil, err
	}
	if warmScopeSetVersion, err := requestInteger(warm[1], "scope_set_version"); err != nil || warmScopeSetVersion != scopeSetVersion {
		return nil, errors.New("React Native warm scope-set version is inconsistent")
	}
	rebuildID, err := completedRebuildID(c.finalResult.Events, clientState.ScopeStates[0].ScopeID)
	if err != nil {
		return nil, err
	}
	scopeChecksum, err := checksumDigest(clientState.ScopeStates[0].Checksum)
	if err != nil || scopeChecksum == nil {
		return nil, errors.New("React Native scope checksum identity is invalid")
	}
	runtime := make(map[string]json.RawMessage, len(c.identities))
	for alias, value := range map[string]any{
		"row-a-checksum":        clientState.ScopeRows[0].Checksum,
		"scope-a-checksum":      *scopeChecksum,
		"client-a-generation":   generation,
		"baseline-rebuild":      rebuildID,
		"row-a-version":         metadata.ServerVersion,
		"current-schema":        clientState.Schema,
		"scope-a":               clientState.ScopeStates[0].ScopeID,
		"scope-set-version-one": scopeSetVersion,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode React Native alias %q: %w", alias, err)
		}
		runtime[alias] = encoded
	}
	for alias, value := range c.runtimeIDs {
		runtime[alias] = append(json.RawMessage(nil), value...)
	}
	for _, alias := range warmConnectAliasNames {
		if len(runtime[alias]) == 0 {
			return nil, fmt.Errorf("React Native alias %q has no runtime evidence", alias)
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

func (c *Coordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		step := c.steps[id]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{
			ContractOperation: step.Operation.ContractOperation,
			Name:              step.Operation.Name,
			Payload:           append(json.RawMessage(nil), step.Operation.Payload...),
		}})
	}
	return &conformanceCommand{
		SchemaVersion: 1,
		Action:        conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps},
		Runtime:       conformanceRuntime{ClientKey: clientKey, Database: c.database, ClientID: clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken},
	}
}

func decodeExchangeRequest(body []byte) (exchangeRequest, error) {
	if len(body) == 0 || len(body) > maximumExchangeBytes {
		return exchangeRequest{}, errInvalidExchange
	}
	if err := jsonstrict.ValidateValue(body); err != nil {
		return exchangeRequest{}, errInvalidExchange
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(body, &members); err != nil || len(members) != 3 {
		return exchangeRequest{}, errInvalidExchange
	}
	for _, name := range []string{"schema_version", "sequence", "result"} {
		if _, ok := members[name]; !ok {
			return exchangeRequest{}, errInvalidExchange
		}
	}
	var version int
	var sequence uint64
	if json.Unmarshal(members["schema_version"], &version) != nil || version != 1 || json.Unmarshal(members["sequence"], &sequence) != nil || sequence == 0 {
		return exchangeRequest{}, errInvalidExchange
	}
	if err := validateBoundedJSON(members["result"], maximumExchangeBytes); err != nil {
		return exchangeRequest{}, errInvalidExchange
	}
	return exchangeRequest{SchemaVersion: version, Sequence: sequence, Result: append(json.RawMessage(nil), members["result"]...)}, nil
}

func decodeResultEnvelope(raw json.RawMessage) (resultEnvelope, error) {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		return resultEnvelope{}, errInvalidExchange
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 5 {
		return resultEnvelope{}, errInvalidExchange
	}
	for _, name := range []string{"schema_version", "outcome", "result", "error_code", "error_detail"} {
		if _, ok := members[name]; !ok {
			return resultEnvelope{}, errInvalidExchange
		}
	}
	var envelope resultEnvelope
	if json.Unmarshal(members["schema_version"], &envelope.SchemaVersion) != nil || envelope.SchemaVersion != 1 || json.Unmarshal(members["outcome"], &envelope.Outcome) != nil {
		return resultEnvelope{}, errInvalidExchange
	}
	if envelope.Outcome != "passed" && envelope.Outcome != "error" {
		return resultEnvelope{}, errInvalidExchange
	}
	envelope.Result = append(json.RawMessage(nil), members["result"]...)
	if json.Unmarshal(members["error_code"], &envelope.ErrorCode) != nil {
		return resultEnvelope{}, errInvalidExchange
	}
	if json.Unmarshal(members["error_detail"], &envelope.ErrorDetail) != nil {
		return resultEnvelope{}, errInvalidExchange
	}
	if envelope.Outcome == "passed" {
		if isJSONNull(envelope.Result) || envelope.ErrorCode != nil || envelope.ErrorDetail != nil {
			return resultEnvelope{}, errInvalidExchange
		}
	} else if !isJSONNull(envelope.Result) || envelope.ErrorCode == nil || !validConformanceError(*envelope.ErrorCode) {
		return resultEnvelope{}, errInvalidExchange
	}
	if err := validateBoundedJSON(envelope.Result, maximumExchangeBytes); err != nil {
		return resultEnvelope{}, errInvalidExchange
	}
	return envelope, nil
}

func validateBoundedJSON(raw []byte, maximum int) error {
	if len(raw) == 0 || len(raw) > maximum {
		return errors.New("JSON value exceeds its byte bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing data")
	}
	return visitBoundedJSON(value, 0)
}

func visitBoundedJSON(value any, depth int) error {
	if depth > 24 {
		return errors.New("JSON value exceeds its depth bound")
	}
	switch typed := value.(type) {
	case []any:
		if len(typed) > 512 {
			return errors.New("JSON array exceeds its item bound")
		}
		for _, child := range typed {
			if err := visitBoundedJSON(child, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if len(typed) > 256 {
			return errors.New("JSON object exceeds its key bound")
		}
		for key, child := range typed {
			if len(key) > 128 {
				return errors.New("JSON object key exceeds its byte bound")
			}
			if err := visitBoundedJSON(child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func validBearer(value, expected string) bool {
	if expected == "" || !strings.HasPrefix(value, "Bearer ") || strings.Contains(value[7:], " ") {
		return false
	}
	actual := []byte(value[7:])
	expectedBytes := []byte(expected)
	return len(actual) == len(expectedBytes) && subtle.ConstantTimeCompare(actual, expectedBytes) == 1
}

func writeExchangeError(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, `{"error":"invalid_request"}`)
}

func randomToken(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func randomDatabaseName() (string, error) {
	token, err := randomToken(12)
	if err != nil {
		return "", err
	}
	return "rn-warm-connect-" + token + ".db", nil
}

func validDatabaseName(value string) bool {
	if len(value) == 0 || len(value) > 128 || strings.Contains(value, "..") {
		return false
	}
	for index, character := range value {
		if index == 0 && !((character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9')) {
			return false
		}
		if !((character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || strings.ContainsRune("._-", character)) {
			return false
		}
	}
	return true
}

func nativeAdapterURL(value, platform string) (string, error) {
	if value == "" {
		return "", errors.New("React Native adapter URL is required")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("React Native adapter URL is invalid")
	}
	parsed.Path = ""
	if platform == "android" && (parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost" || parsed.Hostname() == "::1") {
		port := parsed.Port()
		if port == "" {
			return "", errors.New("React Native Android adapter URL has no port")
		}
		parsed.Host = net.JoinHostPort("10.0.2.2", port)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func isJSONNull(raw []byte) bool { return string(bytes.TrimSpace(raw)) == "null" }

func nativeResultError(err error, disposition string) error {
	if err != nil {
		return err
	}
	if disposition == "" {
		return errors.New("result is absent")
	}
	return fmt.Errorf("terminal disposition is %q", disposition)
}

func warmConnectExpectedState(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		value := scenario.Model.ExpectedState[index]
		if value.ID == "EXPECT-PERF-WARM-CONNECT-SEMANTIC-001" && value.StateFacts != nil {
			return value.StateFacts
		}
	}
	return nil
}
