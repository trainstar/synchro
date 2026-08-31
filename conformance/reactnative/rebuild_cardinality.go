package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	rebuildCardinalityScenarioPath = "conformance/scenarios/performance/rebuild-cardinality-001.json"
	rebuildCardinalityScenarioID   = "SCN-PERF-REBUILD-CARDINALITY-001"
)

var rebuildCardinalityAliasNames = []string{
	"client-generation-one",
	"current-schema",
	"scope-a",
	"items-table",
}

type rebuildCardinalityWorkload struct {
	Profile     string `json:"profile"`
	ScopeID     string `json:"scope_id"`
	RecordCount uint64 `json:"record_count"`
	PageSize    uint64 `json:"page_size"`
}

type rebuildCardinalitySchemaRef struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
}

type rebuildCardinalityControlPayload struct {
	UserID             string                      `json:"user_id"`
	ClientID           string                      `json:"client_id"`
	ClientGeneration   int64                       `json:"client_generation"`
	Schema             rebuildCardinalitySchemaRef `json:"schema"`
	ScopeID            string                      `json:"scope_id"`
	RebuildID          string                      `json:"rebuild_id"`
	Limit              uint64                      `json:"limit"`
	PageOrdinal        uint64                      `json:"page_ordinal"`
	RequestTokenSource string                      `json:"request_token_source"`
	CursorSource       string                      `json:"cursor_source"`
}

type rebuildCardinalityCommitPayload struct {
	Events []struct {
		Operation string `json:"operation"`
	} `json:"events"`
}

// RebuildCardinalityCoordinatorConfig configures one React Native sidecar.
type RebuildCardinalityCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
}

// RebuildCardinalityCoordinatorResult contains final server and identity evidence.
type RebuildCardinalityCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// RebuildCardinalityCoordinator drives every authored cardinality sample through RN.
type RebuildCardinalityCoordinator struct {
	config RebuildCardinalityCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string

	steps      []scenarios.Step
	workloads  []rebuildCardinalityWorkload
	expanded   [][]scenarios.Operation
	expected   *scenarios.StateFacts
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	authTokens map[string]string
	tableName  string

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	stage     rebuildCardinalityStage
	nextSeq   uint64
	current   int
	process   *actionProcessIdentity
	traces    []traceSnapshot
	result    RebuildCardinalityCoordinatorResult
}

type rebuildCardinalityStage uint8

const (
	rebuildCardinalityStageOpen rebuildCardinalityStage = iota
	rebuildCardinalityStageSynchronize
	rebuildCardinalityStageCapture
	rebuildCardinalityStageComplete
)

// LoadRebuildCardinalityScenario loads the authored cardinality contract.
func LoadRebuildCardinalityScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, rebuildCardinalityScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native rebuild-cardinality scenario: %w", err)
	}
	if err := ValidateRebuildCardinalityScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateRebuildCardinalityScenario rejects changes to the closed RN contract.
func ValidateRebuildCardinalityScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != rebuildCardinalityScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native rebuild-cardinality scenario contract is invalid")
	}
	if len(scenario.Steps) != 9 || len(scenario.NativeLifecycleBoundaries) != 0 ||
		len(scenario.NativeIdentityAliases) != len(rebuildCardinalityAliasNames) {
		return errors.New("React Native rebuild-cardinality scenario structure is invalid")
	}
	seenClients := make(map[string]struct{}, len(scenario.Steps))
	pageSize := uint64(0)
	for index, step := range scenario.Steps {
		wantID := scenarios.StepID(fmt.Sprintf("STEP-PERF-REBUILD-CARDINALITY-%03d", index+1))
		binding := step.NativeBinding
		if step.ID != wantID || step.Transport != "model" || scenarios.OperationKey(step.Operation) != "workload/prepare" ||
			binding == nil || binding.Kind != "workload" || binding.Workload == nil || binding.UserID == "" ||
			binding.ClientID == "" || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native rebuild-cardinality step %s binding is invalid", step.ID)
		}
		workload, err := decodeRebuildCardinalityWorkload(step)
		if err != nil {
			return err
		}
		if workload.Profile != "scope_cardinality" || workload.ScopeID != "scope-a" || workload.RecordCount == 0 ||
			workload.PageSize != 100 || workload.RecordCount != binding.Workload.RecordCount ||
			binding.Workload.BatchSize != workload.RecordCount || len(binding.Workload.Targets) != 1 ||
			binding.Workload.Targets[0].ScopeID != workload.ScopeID || binding.Workload.Targets[0].TableID != "items" ||
			binding.Workload.Targets[0].PrimaryKeyFieldID != "id" {
			return fmt.Errorf("React Native rebuild-cardinality step %s workload target is invalid", step.ID)
		}
		if len(binding.Workload.MutationKinds) != 1 || binding.Workload.MutationKinds[0].Operation != "insert" ||
			binding.Workload.MutationKinds[0].Count != workload.RecordCount || len(binding.Workload.MutationKinds[0].FieldIDs) != 1 ||
			binding.Workload.MutationKinds[0].FieldIDs[0] != "value" {
			return fmt.Errorf("React Native rebuild-cardinality step %s mutation binding is invalid", step.ID)
		}
		if _, duplicate := seenClients[binding.ClientID]; duplicate {
			return fmt.Errorf("React Native rebuild-cardinality client %s is not fresh", binding.ClientID)
		}
		seenClients[binding.ClientID] = struct{}{}
		if pageSize == 0 {
			pageSize = workload.PageSize
		} else if pageSize != workload.PageSize {
			return fmt.Errorf("React Native rebuild-cardinality step %s page size %d differs from %d", step.ID, workload.PageSize, pageSize)
		}
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native rebuild-cardinality identity alias is invalid")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return fmt.Errorf("React Native rebuild-cardinality identity alias %q is duplicated", alias.Alias)
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range rebuildCardinalityAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native rebuild-cardinality identity alias %q is absent", name)
		}
	}
	semantic, performance := false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-REBUILD-CARDINALITY-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-REBUILD-CARDINALITY-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !performance || rebuildCardinalityExpectedState(scenario) == nil {
		return errors.New("React Native rebuild-cardinality assertions are invalid")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-REBUILD-CARDINALITY-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-REBUILD-CARDINALITY-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-REBUILD-CARDINALITY-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-REBUILD-CARDINALITY-001", "CTRL-REBUILD-006") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-PERF-REBUILD-CARDINALITY-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-REBUILD-CARDINALITY-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-REBUILD-CARDINALITY-CONTROL-001"] != 1 {
		return errors.New("React Native rebuild-cardinality proof obligations are invalid")
	}
	return nil
}

func decodeRebuildCardinalityWorkload(step scenarios.Step) (rebuildCardinalityWorkload, error) {
	var workload rebuildCardinalityWorkload
	if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil {
		return rebuildCardinalityWorkload{}, fmt.Errorf("React Native rebuild-cardinality step %s workload payload is invalid: %w", step.ID, err)
	}
	return workload, nil
}

// NewRebuildCardinalityCoordinator creates an authenticated host-loopback sidecar.
func NewRebuildCardinalityCoordinator(config RebuildCardinalityCoordinatorConfig) (*RebuildCardinalityCoordinator, error) {
	if err := ValidateRebuildCardinalityScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native rebuild-cardinality coordinator platform must be ios or android")
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native rebuild-cardinality coordinator auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	adapter, err := nativeAdapterURL(serverURL, config.Platform)
	if err != nil {
		return nil, err
	}
	capability, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native rebuild-cardinality coordinator capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native rebuild-cardinality coordinator")
	}
	coordinator := &RebuildCardinalityCoordinator{
		config:     config,
		listener:   listener,
		token:      capability,
		adapter:    adapter,
		expected:   rebuildCardinalityExpectedState(config.Scenario),
		identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage),
		authTokens: make(map[string]string),
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

// Prepare installs the contract and derives every source workload operation.
func (c *RebuildCardinalityCoordinator) Prepare(ctx context.Context) error {
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
		return errors.New("React Native rebuild-cardinality coordinator dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native rebuild-cardinality contract: %w", err)
	}
	model, err := modelrunner.RunScenario(ctx, c.config.Scenario)
	if err != nil {
		return fmt.Errorf("derive React Native rebuild-cardinality source operations: %w", err)
	}
	if !model.Passed || len(model.Steps) != len(c.config.Scenario.Steps) {
		return errors.New("authored rebuild-cardinality model did not close all workload steps")
	}
	for index, step := range c.config.Scenario.Steps {
		if model.Steps[index].StepID != step.ID {
			return fmt.Errorf("React Native rebuild-cardinality model step %s is bound to %s", step.ID, model.Steps[index].StepID)
		}
		workload, err := decodeRebuildCardinalityWorkload(step)
		if err != nil {
			return err
		}
		c.steps = append(c.steps, step)
		c.workloads = append(c.workloads, workload)
		c.expanded = append(c.expanded, model.Steps[index].Expanded)
		if c.config.AuthToken != "" {
			c.authTokens[step.NativeBinding.ClientID] = c.config.AuthToken
			continue
		}
		token, err := c.config.Harness.NativeBearerToken(ctx, step.NativeBinding.UserID, time.Now())
		if err != nil {
			return fmt.Errorf("mint React Native rebuild-cardinality bearer token for %s: %w", step.NativeBinding.ClientID, err)
		}
		c.authTokens[step.NativeBinding.ClientID] = token
	}
	if err := c.bindServerIdentities(); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve runs the sidecar until it closes.
func (c *RebuildCardinalityCoordinator) Serve(ctx context.Context) error {
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

func (c *RebuildCardinalityCoordinator) Handler() http.Handler { return c }

func (c *RebuildCardinalityCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *RebuildCardinalityCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

func (c *RebuildCardinalityCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *RebuildCardinalityCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	return len(c.config.Scenario.Steps)*3 + 1
}

func (c *RebuildCardinalityCoordinator) Result() (RebuildCardinalityCoordinatorResult, error) {
	if c == nil {
		return RebuildCardinalityCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return RebuildCardinalityCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return RebuildCardinalityCoordinatorResult{}, errors.New("React Native rebuild-cardinality coordinator has not completed")
	}
	return c.result, nil
}

func (c *RebuildCardinalityCoordinator) Close(ctx context.Context) error {
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

func (c *RebuildCardinalityCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if c.closed || !c.prepared || c.failed != nil || c.completed || exchange.Sequence != c.nextSeq {
		c.failed = errors.New("React Native rebuild-cardinality exchange is unavailable or non-monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native rebuild-cardinality exchange %d failed: %w", exchange.Sequence, err)
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
		c.failed = errors.New("React Native rebuild-cardinality exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *RebuildCardinalityCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == rebuildCardinalityStageOpen {
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
	case rebuildCardinalityStageSynchronize:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
	case rebuildCardinalityStageCapture:
		if err := c.validateSynchronized(envelope.Result); err != nil {
			return err
		}
	case rebuildCardinalityStageComplete:
		capture, err := decodeCapture(envelope.Result, []string{
			"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof",
		})
		if err != nil {
			return err
		}
		if err := c.validateCapture(capture); err != nil {
			return err
		}
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *RebuildCardinalityCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	if c.stage == rebuildCardinalityStageComplete && c.current == len(c.steps) {
		if err := c.finish(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State, response.Command, c.completed = "complete", nil, true
		return response, nil
	}
	if c.current >= len(c.steps) {
		return exchangeResponse{}, fmt.Errorf("React Native rebuild-cardinality workload index %d is invalid", c.current)
	}
	step, workload := c.steps[c.current], c.workloads[c.current]
	clientID := step.NativeBinding.ClientID
	clientKey := "rebuild-cardinality-" + clientID
	switch c.stage {
	case rebuildCardinalityStageOpen:
		if err := c.executeSource(ctx, c.expanded[c.current], step, workload, c.priorRecordCount()); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command(clientKey, clientID, "client", "open", map[string]any{
			"client_key": clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil,
		})
		c.stage = rebuildCardinalityStageSynchronize
	case rebuildCardinalityStageSynchronize:
		response.Command = c.command(clientKey, clientID, "client", "synchronize-step", map[string]any{
			"client_key": clientKey, "method": "start", "completion": "idle",
		})
		c.stage = rebuildCardinalityStageCapture
	case rebuildCardinalityStageCapture:
		response.Command = c.command(clientKey, clientID, "observer", "capture", map[string]any{
			"client_keys": []string{clientKey},
			"sources":     []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"},
			"durable_proof_identity": map[string]any{
				"table_name": c.tableName, "record_id": "rebuild-cardinality-absent-row",
			},
		})
		c.stage = rebuildCardinalityStageComplete
	case rebuildCardinalityStageComplete:
		c.current++
		c.stage = rebuildCardinalityStageOpen
		return c.advanceLocked(ctx, sequence)
	}
	return response, nil
}

func (c *RebuildCardinalityCoordinator) command(key, clientID, actor, name string, parameters map[string]any) *conformanceCommand {
	return &conformanceCommand{
		SchemaVersion: 1,
		// The runner requires a steps array. A nil slice encodes as null, which
		// its strict decoder rejects.
		Action: conformanceManifest{
			Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters},
			Steps:  make([]conformanceStep, 0),
		},
		Runtime: conformanceRuntime{
			ClientKey: key, Database: "rn-rebuild-cardinality-" + clientID + ".db", ClientID: clientID,
			ServerURL: c.adapter, AuthToken: c.authTokens[clientID],
		},
	}
}

func (c *RebuildCardinalityCoordinator) priorRecordCount() uint64 {
	if c.current == 0 {
		return 0
	}
	return c.workloads[c.current-1].RecordCount
}

func (c *RebuildCardinalityCoordinator) executeSource(ctx context.Context, operations []scenarios.Operation, step scenarios.Step, workload rebuildCardinalityWorkload, prior uint64) error {
	if len(operations) == 0 {
		return fmt.Errorf("React Native rebuild-cardinality step %s source expansion is empty", step.ID)
	}
	pageCount := 0
	commitSeen, materializeSeen := false, false
	stageSeen, activateSeen := false, false
	beginSeen, requestSeen := false, false
	applySeen, finalizeSeen := false, false
	currentRebuildID := ""
	for _, operation := range operations {
		key := scenarios.OperationKey(operation)
		switch key {
		case "model/stage-registry-membership-generation":
			if stageSeen || commitSeen || materializeSeen || beginSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s membership stage is out of order", step.ID)
			}
			stageSeen = true
		case "model/activate-registry-membership-generation":
			if !stageSeen || activateSeen || commitSeen || materializeSeen || beginSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s membership activation is out of order", step.ID)
			}
			activateSeen = true
		case "model/commit-source-transaction":
			if commitSeen || materializeSeen || beginSeen || stageSeen && !activateSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s source commit is out of order", step.ID)
			}
			if err := validateRebuildCardinalityCommit(operation, prior, workload.RecordCount); err != nil {
				return err
			}
			result, err := c.config.Controller.ApplyStep(ctx, operation)
			if err != nil || result.Disposition != "success" {
				return fmt.Errorf("apply React Native rebuild-cardinality source commit for %s: %w", step.ID, nativeResultError(err, result.Disposition))
			}
			commitSeen = true
		case "process/materialize-source-transaction":
			if !commitSeen || materializeSeen || beginSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s materialization is out of order", step.ID)
			}
			result, err := c.config.Controller.ProcessStep(ctx, nil, operation)
			if err != nil || result.Disposition != "success" {
				return fmt.Errorf("materialize React Native rebuild-cardinality source for %s: %w", step.ID, nativeResultError(err, result.Disposition))
			}
			materializeSeen = true
		case "local/begin-rebuild":
			if !materializeSeen || beginSeen || pageCount != 0 {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild begin is out of order", step.ID)
			}
			payload, err := decodeRebuildCardinalityControl(operation)
			if err != nil {
				return err
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID ||
				payload.ScopeID != workload.ScopeID || payload.Limit != workload.PageSize || payload.RebuildID == "" ||
				payload.Schema.Version != int64(step.NativeBinding.Workload.AuthoredSchema.Version) ||
				payload.Schema.Hash != step.NativeBinding.Workload.AuthoredSchema.Hash {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild begin binding is invalid: user=%q client=%q scope=%q limit=%d rebuild=%q schema_version=%d schema_hash=%q", step.ID, payload.UserID, payload.ClientID, payload.ScopeID, payload.Limit, payload.RebuildID, payload.Schema.Version, payload.Schema.Hash)
			}
			currentRebuildID = payload.RebuildID
			beginSeen = true
		case "rebuild/request-page":
			if !beginSeen || finalizeSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild request is out of order", step.ID)
			}
			payload, err := decodeRebuildCardinalityControl(operation)
			if err != nil {
				return err
			}
			wantCursorSource := "none"
			if pageCount > 0 {
				wantCursorSource = "local_rebuild_continuation"
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID ||
				payload.ScopeID != workload.ScopeID || payload.Limit != workload.PageSize || payload.RebuildID != currentRebuildID ||
				payload.CursorSource != wantCursorSource {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild request binding is invalid: page=%d user=%q client=%q scope=%q limit=%d rebuild=%q cursor_source=%q want=%q", step.ID, pageCount+1, payload.UserID, payload.ClientID, payload.ScopeID, payload.Limit, payload.RebuildID, payload.CursorSource, wantCursorSource)
			}
			requestSeen = true
		case "local/apply-rebuild-page":
			if !beginSeen || !requestSeen || finalizeSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild apply is out of order", step.ID)
			}
			payload, err := decodeRebuildCardinalityControl(operation)
			if err != nil {
				return err
			}
			wantOrdinal := uint64(pageCount)*workload.PageSize + 1
			wantTokenSource := "none"
			if pageCount > 0 {
				wantTokenSource = "local_rebuild_continuation"
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID ||
				payload.ScopeID != workload.ScopeID || payload.RebuildID != currentRebuildID || payload.PageOrdinal != wantOrdinal ||
				payload.RequestTokenSource != wantTokenSource {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild apply binding is invalid: page=%d ordinal=%d want=%d token_source=%q want=%q", step.ID, pageCount+1, payload.PageOrdinal, wantOrdinal, payload.RequestTokenSource, wantTokenSource)
			}
			pageCount++
			requestSeen = false
			applySeen = true
		case "local/finalize-rebuild":
			if !beginSeen || !applySeen || requestSeen || finalizeSeen {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild finalize is out of order", step.ID)
			}
			payload, err := decodeRebuildCardinalityControl(operation)
			if err != nil {
				return err
			}
			if payload.UserID != step.NativeBinding.UserID || payload.ClientID != step.NativeBinding.ClientID ||
				payload.ScopeID != workload.ScopeID || payload.RebuildID != currentRebuildID {
				return fmt.Errorf("React Native rebuild-cardinality step %s rebuild finalize binding is invalid: user=%q client=%q scope=%q rebuild=%q", step.ID, payload.UserID, payload.ClientID, payload.ScopeID, payload.RebuildID)
			}
			finalizeSeen = true
		default:
			return fmt.Errorf("React Native rebuild-cardinality step %s operation %q is unsupported", step.ID, key)
		}
	}
	wantPages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	if stageSeen != activateSeen || !commitSeen || !materializeSeen || !beginSeen || !applySeen || !finalizeSeen ||
		requestSeen || pageCount != wantPages {
		return fmt.Errorf("React Native rebuild-cardinality step %s source expansion is incomplete: pages=%d want=%d staged=%t activated=%t committed=%t materialized=%t begun=%t applied=%t finalized=%t request_pending=%t", step.ID, pageCount, wantPages, stageSeen, activateSeen, commitSeen, materializeSeen, beginSeen, applySeen, finalizeSeen, requestSeen)
	}
	return nil
}

func validateRebuildCardinalityCommit(operation scenarios.Operation, prior, current uint64) error {
	var payload rebuildCardinalityCommitPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Events) == 0 {
		return errors.New("React Native rebuild-cardinality source commit payload is invalid")
	}
	wantCount, wantOperation := uint64(1), "update"
	if current > prior {
		wantCount, wantOperation = current-prior, "insert"
	}
	if uint64(len(payload.Events)) != wantCount {
		return fmt.Errorf("React Native rebuild-cardinality source event count %d, want %d", len(payload.Events), wantCount)
	}
	for _, event := range payload.Events {
		if event.Operation != wantOperation {
			return fmt.Errorf("React Native rebuild-cardinality source event operation %q, want %q", event.Operation, wantOperation)
		}
	}
	return nil
}

func decodeRebuildCardinalityControl(operation scenarios.Operation) (rebuildCardinalityControlPayload, error) {
	var payload rebuildCardinalityControlPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return rebuildCardinalityControlPayload{}, fmt.Errorf("decode React Native rebuild-cardinality %s payload: %w", scenarios.OperationKey(operation), err)
	}
	return payload, nil
}

func (c *RebuildCardinalityCoordinator) validateSynchronized(raw json.RawMessage) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "rebuild-cardinality synchronized result"); err != nil {
		return err
	}
	var completion string
	if err := json.Unmarshal(members["completion"], &completion); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality synchronization completion is invalid: %w", err)
	}
	if completion != "idle" {
		return fmt.Errorf("React Native rebuild-cardinality synchronization completion %q, want %q", completion, "idle")
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality synchronization status is invalid: %w", err)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil {
		return fmt.Errorf("React Native rebuild-cardinality synchronization process is invalid: %w", err)
	}
	if c.process == nil || process != *c.process {
		return fmt.Errorf("React Native rebuild-cardinality process identity changed: process_id=%q database_identity_fingerprint=%q", process.ProcessID, process.DatabaseIdentityFingerprint)
	}
	return nil
}

func (c *RebuildCardinalityCoordinator) validateCapture(capture finalCapture) error {
	if c.current >= len(c.steps) {
		return fmt.Errorf("React Native rebuild-cardinality capture workload index %d is invalid", c.current)
	}
	if err := validateEmptyArray(capture.Pending); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality pending mutations are invalid: %w", err)
	}
	if err := validateEmptyArray(capture.Rejected); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality rejected mutations are invalid: %w", err)
	}
	if err := validateReadyStatus(capture.Status); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality sync status is invalid: %w", err)
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	expected := c.expectedClient(c.steps[c.current].NativeBinding.ClientID)
	if expected == nil || expected.RowCount == nil || expected.ProvenanceCount == nil || expected.CheckpointCount == nil || expected.RebuildAttemptCount == nil {
		return fmt.Errorf("React Native rebuild-cardinality authored client state for %s is unavailable", c.steps[c.current].NativeBinding.ClientID)
	}
	if state.ApplicationRowCount != *expected.RowCount || state.ProvenanceCount != *expected.ProvenanceCount ||
		state.ScopeStateCount != *expected.CheckpointCount || state.ScopeRowCount != *expected.RowCount ||
		state.RowMetadataCount != *expected.RowCount || state.RebuildAttemptCount != 0 {
		return fmt.Errorf("React Native rebuild-cardinality client %s durable counts differ: application_rows=%d want=%d provenance=%d want=%d scope_states=%d want=%d scope_rows=%d want=%d row_metadata=%d want=%d active_rebuilds=%d want=0", c.steps[c.current].NativeBinding.ClientID, state.ApplicationRowCount, *expected.RowCount, state.ProvenanceCount, *expected.ProvenanceCount, state.ScopeStateCount, *expected.CheckpointCount, state.ScopeRowCount, *expected.RowCount, state.RowMetadataCount, *expected.RowCount, state.RebuildAttemptCount)
	}
	proof, err := decodeDurableProof(capture.DurableProof)
	if err != nil {
		return fmt.Errorf("React Native rebuild-cardinality client %s receipt proof is invalid: %w", c.steps[c.current].NativeBinding.ClientID, err)
	}
	if proof.RowMetadata != nil || len(proof.RebuildReceiptProofs) == 0 || uint64(len(state.RebuildAttempts)) != state.RebuildAttemptCount {
		return fmt.Errorf("React Native rebuild-cardinality client %s receipt proof detail is incomplete", c.steps[c.current].NativeBinding.ClientID)
	}
	attempts, err := rebuildAttemptFactCount(state.RebuildAttempts, proof.RebuildReceiptProofs)
	if err != nil {
		return fmt.Errorf("React Native rebuild-cardinality client %s rebuild attempt proof is invalid: %w", c.steps[c.current].NativeBinding.ClientID, err)
	}
	if attempts != *expected.RebuildAttemptCount {
		return fmt.Errorf("React Native rebuild-cardinality client %s rebuild attempt facts=%d want=%d", c.steps[c.current].NativeBinding.ClientID, attempts, *expected.RebuildAttemptCount)
	}
	wantPages := (c.workloads[c.current].RecordCount + c.workloads[c.current].PageSize - 1) / c.workloads[c.current].PageSize
	var pages, records uint64
	for _, receipt := range proof.RebuildReceiptProofs {
		if receipt.PageCount == 0 || receipt.PageCount > state.RebuildReceiptCount-pages ||
			!receipt.RequestChainValid || !receipt.RecordsInCanonicalOrder || !receipt.RowChecksumsValid ||
			!receipt.ScopeChecksumValid || !receipt.FinalChecksumMatches {
			return fmt.Errorf("React Native rebuild-cardinality client %s receipt proof detail is invalid", c.steps[c.current].NativeBinding.ClientID)
		}
		pages += receipt.PageCount
		records += receipt.ReturnedRecordCount
	}
	if pages != state.RebuildReceiptCount || pages != wantPages || records != c.workloads[c.current].RecordCount {
		return fmt.Errorf("React Native rebuild-cardinality client %s receipt pages=%d state=%d want=%d records=%d want=%d", c.steps[c.current].NativeBinding.ClientID, pages, state.RebuildReceiptCount, wantPages, records, c.workloads[c.current].RecordCount)
	}
	detailCount := state.ScopeRowCount
	if detailCount > 512 {
		detailCount = 512
	}
	if len(state.ScopeStates) != int(state.ScopeStateCount) || len(state.ScopeRows) != int(detailCount) {
		return fmt.Errorf("React Native rebuild-cardinality client %s durable details differ: scope_states=%d want=%d scope_rows=%d want=%d", c.steps[c.current].NativeBinding.ClientID, len(state.ScopeStates), state.ScopeStateCount, len(state.ScopeRows), detailCount)
	}
	if err := c.validateClientIdentityEvidence(state, capture); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality client %s identity evidence is invalid: %w", c.steps[c.current].NativeBinding.ClientID, err)
	}
	if len(capture.Provenance) == 0 || len(capture.Events) == 0 {
		return fmt.Errorf("React Native rebuild-cardinality client %s durable evidence is incomplete: provenance_records=%d event_records=%d", c.steps[c.current].NativeBinding.ClientID, len(capture.Provenance), len(capture.Events))
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	if err := validateRebuildCardinalityTrace(trace, c.workloads[c.current]); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality client %s trace is invalid: %w", c.steps[c.current].NativeBinding.ClientID, err)
	}
	c.traces = append(c.traces, trace)
	return nil
}

func (c *RebuildCardinalityCoordinator) validateClientIdentityEvidence(state inspectedClientState, capture finalCapture) error {
	var runtimeSchema clientSchema
	if json.Unmarshal(c.runtimeIDs["current-schema"], &runtimeSchema) != nil || runtimeSchema.Version == 0 || runtimeSchema.Hash == "" {
		return errors.New("server schema identity is invalid")
	}
	if state.Schema == nil || *state.Schema != runtimeSchema {
		return fmt.Errorf("schema runtime=%v observed=%v", runtimeSchema, state.Schema)
	}
	var runtimeScope string
	if json.Unmarshal(c.runtimeIDs["scope-a"], &runtimeScope) != nil || runtimeScope == "" {
		return errors.New("server scope identity is invalid")
	}
	for index, scope := range state.ScopeStates {
		if scope.ScopeID != runtimeScope {
			return fmt.Errorf("scope state %d scope_id=%q want=%q", index+1, scope.ScopeID, runtimeScope)
		}
	}
	for index, row := range state.ScopeRows {
		if row.ScopeID != runtimeScope || row.TableName != c.tableName {
			return fmt.Errorf("scope row %d scope_id=%q table_name=%q want_scope=%q want_table=%q", index+1, row.ScopeID, row.TableName, runtimeScope, c.tableName)
		}
	}
	var provenance []clientScopeRow
	if err := decodeStrictValue(capture.Provenance, &provenance); err != nil {
		return fmt.Errorf("provenance details are invalid: %w", err)
	}
	for index, row := range provenance {
		if row.ScopeID != runtimeScope || row.TableName != c.tableName {
			return fmt.Errorf("provenance row %d scope_id=%q table_name=%q want_scope=%q want_table=%q", index+1, row.ScopeID, row.TableName, runtimeScope, c.tableName)
		}
	}
	return nil
}

func validateRebuildCardinalityTrace(trace traceSnapshot, workload rebuildCardinalityWorkload) error {
	pages := int((workload.RecordCount + workload.PageSize - 1) / workload.PageSize)
	wantObservations := pages + 2
	if trace.Overflowed || len(trace.Observations) != wantObservations || trace.SequenceCheckpoint != uint64(len(trace.Observations)) {
		return fmt.Errorf("React Native rebuild-cardinality trace observations=%d want=%d checkpoint=%d want=%d overflowed=%t", len(trace.Observations), wantObservations, trace.SequenceCheckpoint, len(trace.Observations), trace.Overflowed)
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return err
	}
	if err := validateTraceOperation(trace.Observations[0], "connect"); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality connect trace is invalid: %w", err)
	}
	if err := validateTraceOperation(trace.Observations[len(trace.Observations)-1], "pull"); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality pull trace is invalid: %w", err)
	}
	for index := 0; index < pages; index++ {
		observation := trace.Observations[index+1]
		if err := validateTraceOperation(observation, "rebuild"); err != nil {
			return fmt.Errorf("React Native rebuild-cardinality rebuild page %d is invalid: %w", index+1, err)
		}
		limit, limitErr := requestInteger(observation, "limit")
		generation, generationErr := requestInteger(observation, "client_generation")
		scopeFingerprint, scopeErr := requestString(observation, "scope_fingerprint")
		_, rebuildErr := requestString(observation, "rebuild_id_fingerprint")
		cursor, cursorErr := requestStringOptional(observation, "cursor_fingerprint")
		facts, factsErr := decodeRebuildResponseFacts(observation.RebuildResponseFacts)
		remaining := workload.RecordCount - uint64(index)*workload.PageSize
		wantRecords := workload.PageSize
		if remaining < wantRecords {
			wantRecords = remaining
		}
		if limitErr != nil || limit != workload.PageSize || generationErr != nil || generation == 0 || scopeErr != nil || rebuildErr != nil || factsErr != nil || facts.RecordCount == nil || *facts.RecordCount != wantRecords || facts.ScopeFingerprint == nil || *facts.ScopeFingerprint != scopeFingerprint || cursorErr != nil || index == 0 && cursor != "" || index > 0 && cursor == "" {
			return fmt.Errorf("React Native rebuild-cardinality page %d facts generation=%d scope_fingerprint=%q limit=%d want=%d records=%d want=%d cursor=%q errors=%v/%v/%v/%v/%v", index+1, generation, scopeFingerprint, limit, workload.PageSize, valueOrZero(facts.RecordCount), wantRecords, cursor, generationErr, scopeErr, rebuildErr, factsErr, cursorErr)
		}
		terminal := index == pages-1
		if terminal && (*facts.HasMore || *facts.HasCursor || !*facts.HasFinalScopeCursor || !*facts.HasChecksum) {
			return fmt.Errorf("React Native rebuild-cardinality terminal page %d finality has_more=%t has_cursor=%t has_final_scope_cursor=%t has_checksum=%t", index+1, *facts.HasMore, *facts.HasCursor, *facts.HasFinalScopeCursor, *facts.HasChecksum)
		}
		if !terminal && (!*facts.HasMore || !*facts.HasCursor || *facts.HasFinalScopeCursor || *facts.HasChecksum) {
			return fmt.Errorf("React Native rebuild-cardinality intermediate page %d finality has_more=%t has_cursor=%t has_final_scope_cursor=%t has_checksum=%t", index+1, *facts.HasMore, *facts.HasCursor, *facts.HasFinalScopeCursor, *facts.HasChecksum)
		}
	}
	pull := trace.Observations[len(trace.Observations)-1]
	count, err := requestInteger(pull, "scope_count")
	if err != nil || count != 1 {
		return fmt.Errorf("React Native rebuild-cardinality final pull scope_count=%d want=1 error=%v", count, err)
	}
	return nil
}

func valueOrZero(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func (c *RebuildCardinalityCoordinator) bindServerIdentities() error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native rebuild-cardinality server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		if value.Alias == "items-table" {
			c.tableName = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" {
		return errors.New("React Native rebuild-cardinality table identity is unavailable")
	}
	for _, alias := range aliases {
		if len(c.runtimeIDs[alias.Alias]) == 0 {
			return fmt.Errorf("React Native rebuild-cardinality server identity %q is unavailable", alias.Alias)
		}
	}
	return nil
}

func (c *RebuildCardinalityCoordinator) finish(ctx context.Context) error {
	if len(c.traces) != len(c.steps) {
		return fmt.Errorf("React Native rebuild-cardinality trace count=%d want=%d", len(c.traces), len(c.steps))
	}
	var generation uint64
	for traceIndex, trace := range c.traces {
		for observationIndex, observation := range trace.Observations {
			value, err := requestInteger(observation, "client_generation")
			if err != nil || value == 0 {
				return fmt.Errorf("React Native rebuild-cardinality trace %d observation %d client_generation=%d error=%v", traceIndex+1, observationIndex+1, value, err)
			}
			if generation == 0 {
				generation = value
			} else if generation != value {
				return fmt.Errorf("React Native rebuild-cardinality client generation changed: first=%d observed=%d", generation, value)
			}
		}
	}
	encodedGeneration, err := json.Marshal(generation)
	if err != nil {
		return fmt.Errorf("encode React Native rebuild-cardinality client generation: %w", err)
	}
	c.runtimeIDs["client-generation-one"] = encodedGeneration
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return fmt.Errorf("React Native rebuild-cardinality alias %q has no runtime evidence", alias.Alias)
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
	resolutions, err := blackbox.ResolveNativeIdentityAliases(c.identities, observations)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(c.steps))
	for _, step := range c.steps {
		keys = append(keys, "rebuild-cardinality-"+step.NativeBinding.ClientID)
	}
	sort.Strings(keys)
	captures, err := c.config.Controller.Capture(ctx, keys, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return fmt.Errorf("capture React Native rebuild-cardinality server state: %w", nativeResultError(err, ""))
	}
	if err := validateServerState(*c.expected, captures[0].StateFacts); err != nil {
		return fmt.Errorf("React Native rebuild-cardinality server state differs from authored model: transactions=%d rows=%d scopes=%d rebuilds=%d: %w", valueOrZero(captures[0].StateFacts.TransactionCount), valueOrZero(captures[0].StateFacts.RowCount), valueOrZero(captures[0].StateFacts.ScopeCount), valueOrZero(captures[0].StateFacts.RebuildCount), err)
	}
	c.result = RebuildCardinalityCoordinatorResult{ServerFacts: captures[0].StateFacts, IdentityResolution: resolutions}
	return nil
}

func (c *RebuildCardinalityCoordinator) expectedClient(id string) *scenarios.ClientDurabilityFact {
	if c.expected == nil {
		return nil
	}
	for index := range c.expected.Clients {
		if c.expected.Clients[index].ClientID == id {
			return &c.expected.Clients[index]
		}
	}
	return nil
}

func rebuildCardinalityExpectedState(scenario scenarios.Scenario) *scenarios.StateFacts {
	for index := range scenario.Model.ExpectedState {
		value := scenario.Model.ExpectedState[index]
		if value.ID == "EXPECT-PERF-REBUILD-CARDINALITY-SEMANTIC-001" && value.StateFacts != nil {
			return value.StateFacts
		}
	}
	return nil
}
