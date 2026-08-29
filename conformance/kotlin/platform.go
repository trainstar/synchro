package kotlin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	forgedRebuildCursor            = "native-forged-rebuild-cursor"
	maximumProxiedPushRequestBytes = 16 << 20
)

// Client identifies one durable Android database and its authenticated owner.
type Client struct {
	Key         string
	UserID      string
	ClientID    string
	DatabaseKey string
}

// AuthTokenResolver returns the current token for one client.
type AuthTokenResolver func(context.Context, Client) (string, error)

// StepObservation records one client operation result.
type StepObservation struct {
	Disposition               string     `json:"disposition"`
	ErrorCode                 *string    `json:"error_code,omitempty"`
	Wire                      *WireFacts `json:"wire,omitempty"`
	Completion                string     `json:"completion,omitempty"`
	DurationNanoseconds       uint64     `json:"duration_nanoseconds,omitempty"`
	ProvenanceMaintenanceWork uint64     `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     int        `json:"replayed_mutation_count,omitempty"`
}

// WireFacts records bounded facts from one transport request.
type WireFacts struct {
	HTTPStatus            int     `json:"http_status"`
	ErrorCode             *string `json:"error_code,omitempty"`
	Retryable             bool    `json:"retryable"`
	MutationCount         *int    `json:"mutation_count,omitempty"`
	ReplayedMutationCount *int    `json:"replayed_mutation_count,omitempty"`
}

// CaptureFacts binds one requested source to durable client facts.
type CaptureFacts struct {
	Source                    string               `json:"source"`
	StateFacts                scenarios.StateFacts `json:"state_facts"`
	ProvenanceMaintenanceWork uint64               `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     int                  `json:"replayed_mutation_count,omitempty"`
}

// InstallRequest selects one direct client initialization mode.
type InstallRequest struct {
	Client         Client
	Initialization string
	SeedPath       string
}

// SynchronizeRequest groups authored request operations in one client call.
type SynchronizeRequest struct {
	Client     Client
	Method     string
	Operations []scenarios.Operation
}

// CallRequest starts or completes one paused public client call.
type CallRequest struct {
	Client     Client
	CallID     string
	Method     string
	Operations []scenarios.Operation
}

// AwaitRequest advances one paused public client call.
type AwaitRequest struct {
	Client    Client
	CallID    string
	Operation scenarios.Operation
}

// LifecycleRequest invokes one public client lifecycle operation.
type LifecycleRequest struct {
	Client    Client
	Operation string
}

// ClientCallResult describes one direct client call lifecycle state.
type ClientCallResult struct {
	CallID                    string            `json:"call_id"`
	State                     string            `json:"state"`
	Completion                string            `json:"completion,omitempty"`
	Steps                     []StepObservation `json:"steps,omitempty"`
	DurationNanoseconds       uint64            `json:"duration_nanoseconds,omitempty"`
	ProvenanceMaintenanceWork uint64            `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     int               `json:"replayed_mutation_count,omitempty"`
}

// SynchronizationResult describes one grouped synchronization call.
type SynchronizationResult struct {
	Completion                string            `json:"completion"`
	Steps                     []StepObservation `json:"steps"`
	DurationNanoseconds       uint64            `json:"duration_nanoseconds,omitempty"`
	ProvenanceMaintenanceWork uint64            `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     int               `json:"replayed_mutation_count,omitempty"`
	transportObservations     []TransportObservation
}

// Platform drives one or more real Android clients through Kotlin instrumentation.
type Platform struct {
	config Config

	mu        sync.Mutex
	installMu sync.Mutex
	closed    bool
	installed bool
	clients   map[string]*platformClient

	responseProxy            *httptest.Server
	temporaryUnavailablePush *scenarios.PushWireFaultTarget
}

type platformClient struct {
	mu sync.Mutex

	client                      Client
	session                     *Session
	processID                   string
	databaseIdentityFingerprint string
	terminated                  bool
	started                     bool
	restarted                   bool
	nextCall                    uint64
	selectors                   map[string]RowSelector
	maintenanceCursor           int64
	activeCall                  *pausedCall
	pendingLoss                 *pendingResponseLoss
}

type operationWindow struct {
	observations              []TransportObservation
	duration                  time.Duration
	provenanceMaintenanceWork uint64
	replayedMutations         int
}

type pausedCall struct {
	id                 string
	checkpoint         uint64
	observedCheckpoint uint64
	started            time.Time
	before             Result
	paused             bool
}

type pendingResponseLoss struct {
	batchID      string
	before       Result
	observations []TransportObservation
	started      time.Time
}

// NewPlatform creates a direct Android client platform.
func NewPlatform(config Config) (*Platform, error) {
	normalized, err := normalizePlatformConfig(config)
	if err != nil {
		return nil, err
	}
	platform := &Platform{config: normalized, clients: make(map[string]*platformClient)}
	if err := platform.startResponseProxy(); err != nil {
		return nil, err
	}
	return platform, nil
}

func (p *Platform) startResponseProxy() error {
	upstream, err := url.Parse(p.config.ServerURL)
	if err != nil {
		return errors.New("Kotlin Android response proxy upstream is invalid")
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if p.serveTemporaryUnavailablePush(response, request) {
			return
		}
		proxy.ServeHTTP(response, request)
	}))
	p.responseProxy = server
	p.config.ServerURL = server.URL
	return nil
}

func (p *Platform) serveTemporaryUnavailablePush(response http.ResponseWriter, request *http.Request) bool {
	if !strings.HasSuffix(request.URL.Path, "/sync/push") || !p.hasTemporaryUnavailablePush() {
		return false
	}
	target, err := proxiedPushTarget(request)
	if err != nil {
		return false
	}
	if !p.claimTemporaryUnavailablePush(target) {
		return false
	}
	injected := faults.NewTemporaryUnavailableResponse(request)
	defer injected.Body.Close()
	copyInjectedResponse(response, injected)
	return true
}

func proxiedPushTarget(request *http.Request) (scenarios.PushWireFaultTarget, error) {
	if request.Body == nil {
		return scenarios.PushWireFaultTarget{}, errors.New("Kotlin Android proxied push body is absent")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumProxiedPushRequestBytes+1))
	request.Body.Close()
	if err != nil || len(body) > maximumProxiedPushRequestBytes {
		return scenarios.PushWireFaultTarget{}, errors.New("Kotlin Android proxied push body is invalid")
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	if request.Header != nil {
		request.Header.Set("Content-Length", strconv.Itoa(len(body)))
	}
	var payload struct {
		ClientID string `json:"client_id"`
		BatchID  string `json:"batch_id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.ClientID == "" || payload.BatchID == "" {
		return scenarios.PushWireFaultTarget{}, errors.New("Kotlin Android proxied push target is invalid")
	}
	return scenarios.PushWireFaultTarget{ClientID: payload.ClientID, BatchID: payload.BatchID}, nil
}

func copyInjectedResponse(writer http.ResponseWriter, response *http.Response) {
	for name, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}

func (p *Platform) hasTemporaryUnavailablePush() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.temporaryUnavailablePush != nil
}

func (p *Platform) claimTemporaryUnavailablePush(target scenarios.PushWireFaultTarget) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	armed := p.temporaryUnavailablePush
	if armed == nil || armed.ClientID != target.ClientID || armed.BatchID != target.BatchID {
		return false
	}
	p.temporaryUnavailablePush = nil
	return true
}

func (p *Platform) armTemporaryUnavailablePush(operations []scenarios.Operation) (func(), bool, error) {
	target, enabled, err := temporaryUnavailablePushTargetForOperations(operations)
	if err != nil || !enabled {
		return nil, enabled, err
	}
	p.mu.Lock()
	if p.closed || p.temporaryUnavailablePush != nil {
		p.mu.Unlock()
		return nil, false, errors.New("Kotlin Android temporary-unavailable push fault is unavailable")
	}
	p.temporaryUnavailablePush = &target
	p.mu.Unlock()
	return func() { p.clearTemporaryUnavailablePush(target) }, true, nil
}

func (p *Platform) clearTemporaryUnavailablePush(target scenarios.PushWireFaultTarget) {
	p.mu.Lock()
	if armed := p.temporaryUnavailablePush; armed != nil && armed.ClientID == target.ClientID && armed.BatchID == target.BatchID {
		p.temporaryUnavailablePush = nil
	}
	p.mu.Unlock()
}

func temporaryUnavailablePushTargetForOperations(operations []scenarios.Operation) (scenarios.PushWireFaultTarget, bool, error) {
	var target scenarios.PushWireFaultTarget
	for _, operation := range operations {
		candidate, enabled, err := scenarios.TemporaryUnavailablePushTarget(operation)
		if err != nil {
			return scenarios.PushWireFaultTarget{}, false, err
		}
		if !enabled {
			continue
		}
		if target.ClientID != "" {
			return scenarios.PushWireFaultTarget{}, false, errors.New("Kotlin Android synchronization has multiple temporary-unavailable push faults")
		}
		target = candidate
	}
	return target, target.ClientID != "", nil
}

func normalizePlatformConfig(config Config) (Config, error) {
	if config.ApplicationAPKPath == "" || config.InstrumentationAPKPath == "" || config.ServerURL == "" || config.AuthToken == nil || config.Platform == "" || config.AppVersion == "" {
		return Config{}, errors.New("Kotlin Android platform configuration is incomplete")
	}
	if config.Platform != "android" || len(config.AppVersion) > 128 {
		return Config{}, errors.New("Kotlin Android platform configuration is invalid")
	}
	parsed, err := url.Parse(config.ServerURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return Config{}, errors.New("Kotlin Android platform server URL is invalid")
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		return Config{}, err
	}
	if normalized.PullPageSize == 0 {
		normalized.PullPageSize = 100
	}
	if normalized.PullPageSize < 1 || normalized.PullPageSize > 1000 {
		return Config{}, errors.New("Kotlin Android pull page size is invalid")
	}
	if normalized.PushBatchSize == 0 {
		normalized.PushBatchSize = 100
	}
	if normalized.PushBatchSize < 1 || normalized.PushBatchSize > 1000 {
		return Config{}, errors.New("Kotlin Android push batch size is invalid")
	}
	if normalized.TransportCapacity == 0 {
		normalized.TransportCapacity = 512
	}
	if normalized.TransportCapacity < 1 || normalized.TransportCapacity > 512 {
		return Config{}, errors.New("Kotlin Android transport capacity is invalid")
	}
	return normalized, nil
}

// Install starts one client with empty, current, or finalized seed initialization.
func (p *Platform) Install(ctx context.Context, request InstallRequest) error {
	if err := platformContext(ctx); err != nil {
		return err
	}
	if err := validateInstallRequest(request); err != nil {
		return err
	}
	client := request.Client
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("Kotlin Android platform is closed")
	}
	if _, found := p.clients[client.Key]; found {
		p.mu.Unlock()
		return errors.New("Kotlin Android client is already installed")
	}
	p.mu.Unlock()

	p.installMu.Lock()
	config := p.config
	if p.isInstalled() {
		config.ApplicationAPKPath = ""
		config.InstrumentationAPKPath = ""
	}
	session, err := StartSession(ctx, config)
	if err == nil && !p.isInstalled() {
		p.mu.Lock()
		p.installed = true
		p.mu.Unlock()
	}
	p.installMu.Unlock()
	if err != nil {
		return err
	}
	closeSession := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = session.Close(cleanup)
	}
	if err := configureAdapterReverse(ctx, session, p.config.ServerURL); err != nil {
		closeSession()
		return err
	}
	seedName := ""
	if request.Initialization == "seed" {
		seedName, err = session.StageSeed(ctx, client.DatabaseKey, request.SeedPath)
		if err != nil {
			closeSession()
			return err
		}
	}
	state := &platformClient{
		client:    client,
		session:   session,
		selectors: make(map[string]RowSelector),
	}
	databaseMode, err := databaseModeForInitialization(request.Initialization)
	if err != nil {
		closeSession()
		return err
	}
	if _, err := p.openClient(ctx, state, seedName, databaseMode); err != nil {
		closeSession()
		return err
	}
	if request.Initialization == "current" {
		if err := p.initializeCurrent(ctx, state); err != nil {
			closeSession()
			return err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		closeSession()
		return errors.New("Kotlin Android platform is closed")
	}
	if _, found := p.clients[client.Key]; found {
		closeSession()
		return errors.New("Kotlin Android client is already installed")
	}
	p.clients[client.Key] = state
	p.installed = true
	return nil
}

func (p *Platform) isInstalled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.installed
}

func validateInstallRequest(request InstallRequest) error {
	if err := validateClient(request.Client); err != nil {
		return err
	}
	switch request.Initialization {
	case "empty", "current":
		if request.SeedPath != "" {
			return errors.New("Kotlin Android empty initialization cannot use a seed")
		}
	case "seed":
		if request.SeedPath == "" {
			return errors.New("Kotlin Android seed initialization requires a seed")
		}
	default:
		return errors.New("Kotlin Android initialization is unsupported")
	}
	return nil
}

func databaseModeForInitialization(initialization string) (string, error) {
	switch initialization {
	case "empty", "current":
		return "create", nil
	case "seed":
		return "reuse", nil
	default:
		return "", errors.New("Kotlin Android initialization is unsupported")
	}
}

func (p *Platform) openClient(ctx context.Context, client *platformClient, seedName, databaseMode string) (Result, error) {
	token, err := p.config.AuthToken(ctx, client.client)
	if err != nil || token == "" || len(token) > 16384 {
		return Result{}, errors.New("resolve Kotlin Android client authentication failed")
	}
	databaseKey := androidDatabaseName(client.client.DatabaseKey)
	result, err := client.session.Execute(ctx, Request{
		Operation:         "open",
		DatabaseKey:       databaseKey,
		DatabaseMode:      databaseMode,
		ServerURL:         p.config.ServerURL,
		AuthToken:         token,
		ClientID:          client.client.ClientID,
		SeedDatabaseName:  seedName,
		Platform:          p.config.Platform,
		AppVersion:        p.config.AppVersion,
		PullPageSize:      p.config.PullPageSize,
		PushBatchSize:     p.config.PushBatchSize,
		TransportCapacity: p.config.TransportCapacity,
	})
	if err != nil {
		return Result{}, fmt.Errorf("open Kotlin Android client: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		return Result{}, errors.New("Kotlin Android client open did not return status")
	}
	if result.ProvenanceMaintenanceWorkCursor == nil || *result.ProvenanceMaintenanceWorkCursor < 0 {
		return Result{}, errors.New("Kotlin Android open did not return a valid maintenance cursor")
	}
	if client.processID != "" && *result.ProvenanceMaintenanceWorkCursor < client.maintenanceCursor {
		return Result{}, errors.New("Kotlin Android open moved the maintenance cursor backward")
	}
	client.processID = result.ProcessID
	client.databaseIdentityFingerprint = result.DatabaseIdentityFingerprint
	client.maintenanceCursor = *result.ProvenanceMaintenanceWorkCursor
	client.terminated = false
	client.started = false
	return result, nil
}

func (p *Platform) initializeCurrent(ctx context.Context, client *platformClient) error {
	completed, _, result, err := p.runPublicCall(ctx, client, "start")
	if err != nil {
		return fmt.Errorf("initialize current Kotlin Android database: %w", err)
	}
	if completed.Completion != "idle" {
		return errors.New("current Kotlin Android database initialization did not reach idle")
	}
	if err := client.advanceMaintenanceCursor(result); err != nil {
		return err
	}
	stopped, err := client.session.Execute(ctx, Request{Operation: "lifecycle", LifecycleOperation: "stop"})
	if err != nil || stopped.Status == nil || *stopped.Status == "" {
		return errors.New("stop current Kotlin Android database initialization failed")
	}
	if err := client.advanceMaintenanceCursor(stopped); err != nil {
		return err
	}
	client.started = false
	return nil
}

func androidDatabaseName(databaseKey string) string {
	digest := sha256.Sum256([]byte("synchro:android:application-database:v1\x00" + databaseKey))
	return hex.EncodeToString(digest[:]) + ".sqlite"
}

// ApplyStep executes one direct local client operation.
func (p *Platform) ApplyStep(ctx context.Context, client Client, operation scenarios.Operation) (StepObservation, error) {
	if err := platformContext(ctx); err != nil {
		return StepObservation{}, err
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return StepObservation{}, fmt.Errorf("Kotlin Android apply operation is invalid: %w", err)
	}
	if scenarios.OperationKey(operation) != "local/write" {
		return StepObservation{}, fmt.Errorf("Kotlin Android apply operation %s is unsupported", scenarios.OperationKey(operation))
	}
	state, err := p.clientFor(client)
	if err != nil {
		return StepObservation{}, err
	}
	action, selector, err := decodeLocalWrite(operation, client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("apply"); err != nil {
		return StepObservation{}, err
	}
	before, err := captureClientState(ctx, state)
	if err != nil {
		return StepObservation{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	result, err := state.session.Execute(ctx, Request{Operation: "local-action", LocalAction: &action})
	if err != nil {
		return StepObservation{}, fmt.Errorf("execute Kotlin Android local action: %w", err)
	}
	if result.RowsAffected == nil || *result.RowsAffected != 1 {
		return StepObservation{}, errors.New("Kotlin Android local action did not affect one row")
	}
	state.selectors[selectorKey(selector)] = selector
	window, err := p.completeWindow(ctx, state, checkpoint, started, before)
	if err != nil {
		return StepObservation{}, err
	}
	return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
}

// RequestStep runs one public client synchronization and returns its matching request fact.
func (p *Platform) RequestStep(ctx context.Context, client Client, operation scenarios.Operation) (StepObservation, error) {
	if err := platformContext(ctx); err != nil {
		return StepObservation{}, err
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return StepObservation{}, fmt.Errorf("Kotlin Android request operation is invalid: %w", err)
	}
	class, _, _, err := requestDispatch(operation)
	if err != nil {
		return StepObservation{}, err
	}
	method := "sync-now"
	if class == "connect" {
		method = "start"
	}
	completed, err := p.Synchronize(ctx, SynchronizeRequest{
		Client:     client,
		Method:     method,
		Operations: []scenarios.Operation{operation},
	})
	if err != nil {
		return StepObservation{}, err
	}
	if len(completed.Steps) != 1 {
		return StepObservation{}, errors.New("Kotlin Android request did not produce one transport observation")
	}
	observation := completed.Steps[0]
	observation.Completion = completed.Completion
	observation.DurationNanoseconds = completed.DurationNanoseconds
	observation.ProvenanceMaintenanceWork = completed.ProvenanceMaintenanceWork
	observation.ReplayedMutationCount = completed.ReplayedMutationCount
	return observation, nil
}

// Synchronize runs grouped transport operations through one public client call.
func (p *Platform) Synchronize(ctx context.Context, request SynchronizeRequest) (SynchronizationResult, error) {
	if err := platformContext(ctx); err != nil {
		return SynchronizationResult{}, err
	}
	if !validMethod(request.Method) {
		return SynchronizationResult{}, errors.New("Kotlin Android synchronization method is invalid")
	}
	dropBatchID, err := validateRequestOperations(request.Client, request.Operations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state, err := p.clientFor(request.Client)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("synchronization"); err != nil {
		return SynchronizationResult{}, err
	}
	releaseFault, faultArmed, err := p.armTemporaryUnavailablePush(request.Operations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if faultArmed {
		defer releaseFault()
	}
	if dropBatchID != "" {
		if faultArmed {
			return SynchronizationResult{}, errors.New("Kotlin Android response loss cannot combine with a temporary-unavailable push fault")
		}
		_, dropLast, _, dispatchErr := requestDispatch(request.Operations[len(request.Operations)-1])
		if dispatchErr != nil || !dropLast {
			return SynchronizationResult{}, errors.New("Kotlin Android response-loss request must end its public call")
		}
		return p.synchronizeWithResponseLoss(ctx, state, request.Method, request.Operations, dropBatchID)
	}

	before, err := captureClientState(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	completed, observations, _, err := p.runPublicCall(ctx, state, request.Method)
	if err != nil {
		return SynchronizationResult{}, err
	}
	mapped, err := mapTransportOperations(request.Operations, observations, before)
	if err != nil {
		return SynchronizationResult{}, err
	}
	window, err := p.completeWindow(ctx, state, checkpoint, started, before)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if state.restarted {
		window.replayedMutations = replayedMutationCount(window.observations)
		state.restarted = false
	}
	state.started = true
	return synchronizationResult(completed.Completion, mapped, window), nil
}

func (p *Platform) synchronizeWithResponseLoss(ctx context.Context, state *platformClient, method string, operations []scenarios.Operation, batchID string) (SynchronizationResult, error) {
	before, err := captureClientState(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	callID := p.nextCallID(state)
	operationClass, _, _, err := requestDispatch(operations[0])
	if err != nil {
		return SynchronizationResult{}, err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return SynchronizationResult{}, fmt.Errorf("arm Kotlin Android transport pause: %w", err)
	}
	begin, err := state.session.Execute(ctx, Request{Operation: "begin-call", CallID: callID, Method: method})
	if err != nil {
		return SynchronizationResult{}, fmt.Errorf("start Kotlin Android response-loss call: %w", err)
	}
	inFlight, err := clientCallResult(begin)
	if err != nil || inFlight.CallID != callID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return SynchronizationResult{}, errors.New("Kotlin Android response-loss call did not enter flight")
	}
	if err := waitForTransportObservation(ctx, state, checkpoint, operationClass); err != nil {
		return SynchronizationResult{}, err
	}
	lastState, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass})
	if err != nil {
		return SynchronizationResult{}, fmt.Errorf("await Kotlin Android transport pause: %w", err)
	}
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if len(observations) == 0 || observations[len(observations)-1].OperationClass != operationClass {
		return SynchronizationResult{}, errors.New("Kotlin Android response-loss transport observation is not the covered request")
	}
	mapped, err := mapTransportOperations(operations[:1], observations[len(observations)-1:], before)
	if err != nil {
		return SynchronizationResult{}, err
	}
	for index := 1; index < len(operations); index++ {
		stepCheckpoint := state.session.Checkpoint()
		operationClass, _, _, err = requestDispatch(operations[index])
		if err != nil {
			return SynchronizationResult{}, err
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("arm next Kotlin Android transport pause: %w", err)
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("resume Kotlin Android transport pause: %w", err)
		}
		if err := waitForTransportObservation(ctx, state, stepCheckpoint, operationClass); err != nil {
			return SynchronizationResult{}, err
		}
		lastState, err = state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass})
		if err != nil {
			return SynchronizationResult{}, fmt.Errorf("await next Kotlin Android transport pause: %w", err)
		}
		stepObservations, err := state.session.ObservationsAfter(stepCheckpoint)
		if err != nil {
			return SynchronizationResult{}, err
		}
		source, err := captureClientState(ctx, state)
		if err != nil {
			return SynchronizationResult{}, err
		}
		step, err := mapTransportOperations(operations[index:index+1], stepObservations, source)
		if err != nil {
			return SynchronizationResult{}, err
		}
		mapped = append(mapped, step[0])
	}
	observations, err = state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if len(observations) == 0 {
		return SynchronizationResult{}, errors.New("Kotlin Android response-loss call did not record a response")
	}
	last := observations[len(observations)-1]
	if last.StatusCode < 200 || last.StatusCode >= 300 {
		return SynchronizationResult{}, errors.New("Kotlin Android response loss requires a committed server response")
	}
	if method == "reset-schema-and-start" {
		state.selectors = make(map[string]RowSelector)
	}
	lostStep := &mapped[len(mapped)-1]
	if lostStep.Wire == nil {
		return SynchronizationResult{}, errors.New("Kotlin Android response loss has no wire observation")
	}
	lostStep.Wire.HTTPStatus = 0
	lostStep.Wire.ErrorCode = nil
	lostStep.Wire.Retryable = true
	window, err := state.windowFromResults(started, before, lastState, observations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if err := state.session.Kill(ctx); err != nil {
		return SynchronizationResult{}, fmt.Errorf("terminate Kotlin Android client after server response: %w", err)
	}
	if err := state.session.WaitForExit(ctx); err != nil {
		return SynchronizationResult{}, errors.New("Kotlin Android response-loss termination is not confirmed")
	}
	if err := state.session.Close(ctx); err != nil {
		return SynchronizationResult{}, err
	}
	state.session = nil
	state.terminated = true
	state.started = false
	state.pendingLoss = &pendingResponseLoss{
		batchID:      batchID,
		before:       before,
		observations: cloneObservations(observations),
		started:      started,
	}
	return synchronizationResult("blocked", mapped, window), nil
}

func waitForTransportObservation(ctx context.Context, state *platformClient, checkpoint uint64, operationClass string) error {
	for {
		if _, err := captureClientStateBatch(ctx, state, nil); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("wait for Kotlin Android transport observation %q: %w", operationClass, ctx.Err())
			}
			return fmt.Errorf("poll Kotlin Android transport observation: %w", err)
		}
		observations, err := state.session.ObservationsAfter(checkpoint)
		if err != nil {
			return err
		}
		for _, observation := range observations {
			if observation.OperationClass == operationClass {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Kotlin Android transport observation %q: %w", operationClass, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// BeginCall starts one public call and pauses after its first upstream response.
func (p *Platform) BeginCall(ctx context.Context, request CallRequest) (ClientCallResult, error) {
	if err := platformContext(ctx); err != nil {
		return ClientCallResult{}, err
	}
	if len(request.Operations) != 1 || !validCallID(request.CallID) || !validMethod(request.Method) {
		return ClientCallResult{}, errors.New("Kotlin Android begin-call request is invalid")
	}
	dropBatchID, err := validateRequestOperations(request.Client, request.Operations)
	if err != nil {
		return ClientCallResult{}, err
	}
	if dropBatchID != "" {
		return ClientCallResult{}, errors.New("Kotlin Android response loss requires grouped synchronization")
	}
	if _, enabled, err := temporaryUnavailablePushTargetForOperations(request.Operations); err != nil {
		return ClientCallResult{}, err
	} else if enabled {
		return ClientCallResult{}, errors.New("Kotlin Android temporary-unavailable push fault requires synchronous synchronization")
	}
	operationClass, _, _, err := requestDispatch(request.Operations[0])
	if err != nil {
		return ClientCallResult{}, err
	}
	state, err := p.clientFor(request.Client)
	if err != nil {
		return ClientCallResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("begin-call"); err != nil {
		return ClientCallResult{}, err
	}
	before, err := captureClientState(ctx, state)
	if err != nil {
		return ClientCallResult{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return ClientCallResult{}, fmt.Errorf("arm Kotlin Android transport pause: %w", err)
	}
	begin, err := state.session.Execute(ctx, Request{Operation: "begin-call", CallID: request.CallID, Method: request.Method})
	if err != nil {
		return ClientCallResult{}, fmt.Errorf("start paused Kotlin Android call: %w", err)
	}
	inFlight, err := clientCallResult(begin)
	if err != nil || inFlight.CallID != request.CallID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return ClientCallResult{}, errors.New("Kotlin Android paused call did not enter flight")
	}
	if err := waitForTransportObservation(ctx, state, checkpoint, operationClass); err != nil {
		return ClientCallResult{}, err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
		return ClientCallResult{}, fmt.Errorf("await Kotlin Android transport pause: %w", err)
	}
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return ClientCallResult{}, err
	}
	mapped, err := mapTransportOperations(request.Operations, observations, before)
	if err != nil {
		return ClientCallResult{}, err
	}
	state.activeCall = &pausedCall{
		id:                 request.CallID,
		checkpoint:         checkpoint,
		observedCheckpoint: state.session.Checkpoint(),
		started:            started,
		before:             before,
		paused:             true,
	}
	inFlight.Steps = mapped
	return inFlight, nil
}

// AwaitStep resumes one paused call and pauses after its next upstream response.
func (p *Platform) AwaitStep(ctx context.Context, request AwaitRequest) (StepObservation, error) {
	if err := platformContext(ctx); err != nil {
		return StepObservation{}, err
	}
	if !validCallID(request.CallID) {
		return StepObservation{}, errors.New("Kotlin Android await-step call ID is invalid")
	}
	dropBatchID, err := validateRequestOperations(request.Client, []scenarios.Operation{request.Operation})
	if err != nil {
		return StepObservation{}, err
	}
	if dropBatchID != "" {
		return StepObservation{}, errors.New("Kotlin Android response loss requires grouped synchronization")
	}
	if _, enabled, err := temporaryUnavailablePushTargetForOperations([]scenarios.Operation{request.Operation}); err != nil {
		return StepObservation{}, err
	} else if enabled {
		return StepObservation{}, errors.New("Kotlin Android temporary-unavailable push fault requires synchronous synchronization")
	}
	operationClass, _, _, err := requestDispatch(request.Operation)
	if err != nil {
		return StepObservation{}, err
	}
	state, err := p.clientFor(request.Client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	active := state.activeCall
	if state.terminated || state.session == nil || active == nil || active.id != request.CallID || !active.paused {
		return StepObservation{}, errors.New("Kotlin Android await-step has no matching paused call")
	}
	checkpoint := state.session.Checkpoint()
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return StepObservation{}, fmt.Errorf("arm next Kotlin Android transport pause: %w", err)
	}
	if operationUsesForgedRebuildCursor(request.Operation) {
		if _, err := state.session.Execute(ctx, Request{Operation: "override-rebuild-cursor", RebuildCursorOverride: forgedRebuildCursor}); err != nil {
			return StepObservation{}, fmt.Errorf("override paused Kotlin Android rebuild cursor: %w", err)
		}
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
		return StepObservation{}, fmt.Errorf("resume Kotlin Android transport pause: %w", err)
	}
	active.paused = false
	if err := waitForTransportObservation(ctx, state, checkpoint, operationClass); err != nil {
		return StepObservation{}, err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
		return StepObservation{}, fmt.Errorf("await next Kotlin Android transport pause: %w", err)
	}
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return StepObservation{}, err
	}
	source, err := captureClientState(ctx, state)
	if err != nil {
		return StepObservation{}, err
	}
	mapped, err := mapTransportOperations([]scenarios.Operation{request.Operation}, observations, source)
	if err != nil {
		return StepObservation{}, err
	}
	active.observedCheckpoint = state.session.Checkpoint()
	active.paused = true
	return mapped[0], nil
}

// AwaitCall resumes the final pause and waits for call completion.
func (p *Platform) AwaitCall(ctx context.Context, request CallRequest) (ClientCallResult, error) {
	if err := platformContext(ctx); err != nil {
		return ClientCallResult{}, err
	}
	if !validCallID(request.CallID) || request.Method != "" || len(request.Operations) != 0 {
		return ClientCallResult{}, errors.New("Kotlin Android await-call request is invalid")
	}
	state, err := p.clientFor(request.Client)
	if err != nil {
		return ClientCallResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	active := state.activeCall
	if state.terminated || state.session == nil || active == nil || active.id != request.CallID {
		return ClientCallResult{}, errors.New("Kotlin Android await-call has no matching active call")
	}
	if active.paused {
		if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
			return ClientCallResult{}, fmt.Errorf("resume final Kotlin Android transport pause: %w", err)
		}
		active.paused = false
	}
	result, err := state.session.Execute(ctx, Request{Operation: "await-call", CallID: request.CallID})
	if err != nil {
		return ClientCallResult{}, fmt.Errorf("await paused Kotlin Android call: %w", err)
	}
	completed, err := clientCallResult(result)
	if err != nil || completed.CallID != request.CallID || completed.State != "completed" || !validCompletion(completed.Completion) {
		return ClientCallResult{}, errors.New("Kotlin Android paused call did not complete")
	}
	uncovered, err := state.session.ObservationsAfter(active.observedCheckpoint)
	if err != nil {
		return ClientCallResult{}, err
	}
	if len(uncovered) != 0 {
		return ClientCallResult{}, errors.New("Kotlin Android paused call produced an uncovered transport request")
	}
	observations, err := state.session.ObservationsAfter(active.checkpoint)
	if err != nil {
		return ClientCallResult{}, err
	}
	window, err := p.completeWindow(ctx, state, active.checkpoint, active.started, active.before)
	if err != nil {
		return ClientCallResult{}, err
	}
	if state.restarted {
		window.replayedMutations = replayedMutationCount(observations)
		state.restarted = false
	}
	state.activeCall = nil
	state.started = true
	return clientCallResultWithWindow(completed, window), nil
}

// Lifecycle invokes one public client lifecycle operation.
func (p *Platform) Lifecycle(ctx context.Context, request LifecycleRequest) (StepObservation, error) {
	if err := platformContext(ctx); err != nil {
		return StepObservation{}, err
	}
	if !validLifecycle(request.Operation) {
		return StepObservation{}, errors.New("Kotlin Android lifecycle operation is unsupported")
	}
	state, err := p.clientFor(request.Client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("lifecycle"); err != nil {
		return StepObservation{}, err
	}
	before, err := captureClientState(ctx, state)
	if err != nil {
		return StepObservation{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	result, err := state.session.Execute(ctx, Request{Operation: "lifecycle", LifecycleOperation: request.Operation})
	if err != nil {
		return StepObservation{}, fmt.Errorf("run Kotlin Android lifecycle operation: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		return StepObservation{}, errors.New("Kotlin Android lifecycle operation did not return status")
	}
	window, err := p.completeWindow(ctx, state, checkpoint, started, before)
	if err != nil {
		return StepObservation{}, err
	}
	if request.Operation == "stop" {
		state.started = false
	}
	return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
}

// ProcessStep executes a supported client process operation.
func (p *Platform) ProcessStep(ctx context.Context, client Client, operation scenarios.Operation) (StepObservation, error) {
	if err := platformContext(ctx); err != nil {
		return StepObservation{}, err
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return StepObservation{}, fmt.Errorf("Kotlin Android process operation is invalid: %w", err)
	}
	if err := operationIdentityMatches(operation, client); err != nil {
		return StepObservation{}, err
	}
	state, err := p.clientFor(client)
	if err != nil {
		return StepObservation{}, err
	}
	switch scenarios.OperationKey(operation) {
	case "process/restart-client":
		state.mu.Lock()
		defer state.mu.Unlock()
		if err := state.available("restart"); err != nil {
			return StepObservation{}, err
		}
		started := time.Now()
		opened, err := p.restartClient(ctx, state)
		if err != nil {
			return StepObservation{}, err
		}
		after, err := captureClientState(ctx, state)
		if err != nil {
			return StepObservation{}, err
		}
		window, err := state.windowFromResults(started, opened, after, nil)
		if err != nil {
			return StepObservation{}, err
		}
		return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
	case "process/response-loss":
		batchID, err := responseLossBatch(operation, client)
		if err != nil {
			return StepObservation{}, err
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		loss := state.pendingLoss
		if !state.terminated || state.session != nil || loss == nil || loss.batchID != batchID {
			return StepObservation{}, errors.New("Kotlin Android response loss has no matching interrupted request")
		}
		started := time.Now()
		opened, err := p.relaunchExistingClient(ctx, state)
		if err != nil {
			return StepObservation{}, fmt.Errorf("relaunch Kotlin Android client after response loss: %w", err)
		}
		state.pendingLoss = nil
		state.restarted = true
		after, err := captureClientState(ctx, state)
		if err != nil {
			return StepObservation{}, err
		}
		window, err := state.windowFromResults(started, opened, after, nil)
		if err != nil {
			return StepObservation{}, err
		}
		return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
	default:
		return StepObservation{}, fmt.Errorf("Kotlin Android process operation %s is unsupported", scenarios.OperationKey(operation))
	}
}

func (p *Platform) restartClient(ctx context.Context, client *platformClient) (Result, error) {
	priorProcessID := client.processID
	priorFingerprint := client.databaseIdentityFingerprint
	oldSession := client.session
	if err := oldSession.Kill(ctx); err != nil {
		return Result{}, err
	}
	if err := oldSession.WaitForExit(ctx); err != nil {
		return Result{}, errors.New("Kotlin Android client termination is not confirmed")
	}
	if err := oldSession.Close(ctx); err != nil {
		return Result{}, err
	}
	client.session = nil
	client.terminated = true
	opened, err := p.relaunchExistingClient(ctx, client)
	if err != nil {
		return Result{}, fmt.Errorf("relaunch Kotlin Android client: %w", err)
	}
	if err := verifyRestartIdentity(priorProcessID, priorFingerprint, client.processID, client.databaseIdentityFingerprint); err != nil {
		closeKotlinSession(client.session)
		client.session = nil
		client.terminated = true
		return Result{}, err
	}
	client.restarted = true
	return opened, nil
}

func (p *Platform) relaunchExistingClient(ctx context.Context, client *platformClient) (Result, error) {
	priorProcessID := client.processID
	priorFingerprint := client.databaseIdentityFingerprint
	config := p.config
	config.ApplicationAPKPath = ""
	config.InstrumentationAPKPath = ""
	session, err := StartSession(ctx, config)
	if err != nil {
		return Result{}, err
	}
	if err := configureAdapterReverse(ctx, session, p.config.ServerURL); err != nil {
		closeKotlinSession(session)
		return Result{}, err
	}
	client.session = session
	opened, err := p.openClient(ctx, client, "", "existing")
	if err != nil {
		closeKotlinSession(session)
		client.session = nil
		client.terminated = true
		return Result{}, err
	}
	if err := verifyRestartIdentity(priorProcessID, priorFingerprint, client.processID, client.databaseIdentityFingerprint); err != nil {
		closeKotlinSession(session)
		client.session = nil
		client.terminated = true
		return Result{}, err
	}
	return opened, nil
}

func configureAdapterReverse(ctx context.Context, session *Session, serverURL string) error {
	port, required, err := adapterReversePort(serverURL)
	if err != nil {
		return err
	}
	if !required {
		return nil
	}
	if err := session.ReverseHostPort(ctx, port, port); err != nil {
		return fmt.Errorf("configure Kotlin Android adapter reverse: %w", err)
	}
	return nil
}

func adapterReversePort(serverURL string) (int, bool, error) {
	parsed, err := url.Parse(serverURL)
	if err != nil || parsed.Hostname() == "" {
		return 0, false, errors.New("Kotlin Android platform server URL is invalid")
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "localhost" && host != "127.0.0.1" && host != "::1" {
		return 0, false, nil
	}
	port := 0
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil || !validPort(port) {
			return 0, false, errors.New("Kotlin Android platform server port is invalid")
		}
	} else if parsed.Scheme == "http" {
		port = 80
	} else if parsed.Scheme == "https" {
		port = 443
	} else {
		return 0, false, errors.New("Kotlin Android platform server URL is invalid")
	}
	return port, true, nil
}

func verifyRestartIdentity(priorProcessID, priorFingerprint, processID, fingerprint string) error {
	if priorProcessID == "" || priorFingerprint == "" || processID == "" || fingerprint == "" || processID == priorProcessID {
		return errors.New("Kotlin Android relaunch did not create a distinct process")
	}
	if fingerprint != priorFingerprint {
		return errors.New("Kotlin Android relaunch changed the database identity")
	}
	return nil
}

func closeKotlinSession(session *Session) {
	if session == nil {
		return
	}
	cleanup, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_ = session.Close(cleanup)
}

func (p *Platform) runPublicCall(ctx context.Context, client *platformClient, method string) (ClientCallResult, []TransportObservation, Result, error) {
	callID := p.nextCallID(client)
	checkpoint := client.session.Checkpoint()
	inFlight, err := client.session.Execute(ctx, Request{Operation: "begin-call", CallID: callID, Method: method})
	if err != nil {
		return ClientCallResult{}, nil, Result{}, fmt.Errorf("start Kotlin Android public call: %w", err)
	}
	inFlightResult, err := clientCallResult(inFlight)
	if err != nil || inFlightResult.CallID != callID || inFlightResult.State != "in_flight" || inFlightResult.Completion != "" {
		return ClientCallResult{}, nil, Result{}, errors.New("Kotlin Android public call did not enter flight")
	}
	completed, err := client.session.Execute(ctx, Request{Operation: "await-call", CallID: callID})
	if err != nil {
		return ClientCallResult{}, nil, Result{}, fmt.Errorf("await Kotlin Android public call: %w", err)
	}
	completedResult, err := clientCallResult(completed)
	if err != nil || completedResult.CallID != callID || completedResult.State != "completed" || !validCompletion(completedResult.Completion) {
		return ClientCallResult{}, nil, Result{}, errors.New("Kotlin Android public call did not complete")
	}
	observations, err := client.session.ObservationsAfter(checkpoint)
	if err != nil {
		return ClientCallResult{}, nil, Result{}, err
	}
	return completedResult, observations, completed, nil
}

func (p *Platform) nextCallID(client *platformClient) string {
	client.nextCall++
	return "kotlin_call_" + strconv.FormatUint(client.nextCall, 10)
}

func mapTransportObservation(observation TransportObservation) (StepObservation, error) {
	if err := validateTransportObservation(observation); err != nil {
		return StepObservation{}, err
	}
	facts := &WireFacts{HTTPStatus: observation.StatusCode}
	if observation.ErrorCode != nil {
		facts.ErrorCode = clonePointer(observation.ErrorCode)
	}
	if observation.Retryable != nil {
		facts.Retryable = *observation.Retryable
	}
	if observation.RequestFacts != nil && observation.RequestFacts.MutationCount != nil {
		value := *observation.RequestFacts.MutationCount
		facts.MutationCount = &value
		facts.ReplayedMutationCount = &value
	}
	return StepObservation{Disposition: "success", Wire: facts}, nil
}

func mapTransportOperations(operations []scenarios.Operation, observations []TransportObservation, source Result) ([]StepObservation, error) {
	if len(operations) != len(observations) {
		return nil, errors.New("Kotlin Android transport observations do not close covered requests")
	}
	mapped := make([]StepObservation, len(operations))
	withinCallCheckpoints := make(map[string]string)
	for index := range operations {
		if err := validateOperationTransportFacts(operations[index], observations[index]); err != nil {
			return nil, err
		}
		if err := validateCursorSourceBinding(operations[index], observations[index], source, withinCallCheckpoints); err != nil {
			return nil, err
		}
		if observations[index].RebuildResponseFacts != nil && observations[index].RebuildResponseFacts.FinalScopeCursorFingerprint != nil {
			var payload struct {
				ScopeID string `json:"scope_id"`
			}
			if err := json.Unmarshal(operations[index].Payload, &payload); err != nil || payload.ScopeID == "" {
				return nil, errors.New("decode Kotlin Android authored rebuild scope failed")
			}
			if _, exists := withinCallCheckpoints[payload.ScopeID]; exists {
				return nil, errors.New("Kotlin Android public call produced multiple terminal rebuild cursors for one scope")
			}
			withinCallCheckpoints[payload.ScopeID] = *observations[index].RebuildResponseFacts.FinalScopeCursorFingerprint
		}
		observation, err := mapTransportObservation(observations[index])
		if err != nil {
			return nil, err
		}
		mapped[index] = observation
	}
	return mapped, nil
}

func validateOperationTransportFacts(operation scenarios.Operation, observation TransportObservation) error {
	if err := validateTransportObservation(observation); err != nil {
		return err
	}
	operationClass, _, _, err := requestDispatch(operation)
	if err != nil || observation.OperationClass != operationClass {
		return errors.New("Kotlin Android transport observation does not match the requested operation")
	}
	facts := observation.RequestFacts
	switch operationClass {
	case "connect":
		var payload struct {
			ProtocolVersion int               `json:"protocol_version"`
			KnownScopes     []json.RawMessage `json:"known_scopes"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || facts == nil || facts.ProtocolVersion == nil || *facts.ProtocolVersion != payload.ProtocolVersion || facts.ScopeCount == nil || *facts.ScopeCount != len(payload.KnownScopes) {
			return errors.New("Kotlin Android connect request facts do not match the authored operation")
		}
	case "pull":
		var payload struct {
			Scopes []struct {
				CursorSource string `json:"cursor_source"`
			} `json:"scopes"`
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil {
			return errors.New("decode Kotlin Android authored pull request facts failed")
		}
		expectedCursors := 0
		for _, scope := range payload.Scopes {
			if scope.CursorSource != "none" {
				expectedCursors++
			}
		}
		if facts == nil || facts.ScopeCount == nil || *facts.ScopeCount != len(payload.Scopes) || facts.Limit == nil || *facts.Limit != payload.Limit || len(observation.CursorFingerprints) != expectedCursors {
			return errors.New("Kotlin Android pull request facts do not match the authored operation")
		}
	case "push":
		var payload struct {
			Request struct {
				Mutations []json.RawMessage `json:"mutations"`
			} `json:"request"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || facts == nil || facts.MutationCount == nil || *facts.MutationCount != len(payload.Request.Mutations) {
			return errors.New("Kotlin Android push request facts do not match the authored operation")
		}
	case "rebuild":
		var payload struct {
			RebuildID    string `json:"rebuild_id"`
			CursorSource string `json:"cursor_source"`
			Limit        int    `json:"limit"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil {
			return errors.New("decode Kotlin Android authored rebuild request facts failed")
		}
		cursorPresent := payload.CursorSource != "none"
		if payload.RebuildID == "" || facts == nil || facts.RebuildIDFingerprint == nil {
			return errors.New("Kotlin Android rebuild identity facts are incomplete")
		}
		if facts.Limit == nil || *facts.Limit != payload.Limit {
			return errors.New("Kotlin Android rebuild limit does not match the authored operation")
		}
		if facts.CursorPresent == nil || *facts.CursorPresent != cursorPresent {
			return errors.New("Kotlin Android rebuild cursor presence does not match the authored operation")
		}
	}
	return nil
}

func validateCursorSourceBinding(operation scenarios.Operation, observation TransportObservation, source Result, withinCallCheckpoints map[string]string) error {
	switch operation.ContractOperation {
	case "pull":
		var payload struct {
			Scopes []struct {
				ScopeID      string `json:"scope_id"`
				CursorSource string `json:"cursor_source"`
			} `json:"scopes"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Scopes) == 0 {
			return errors.New("decode Kotlin Android authored pull cursor sources failed")
		}
		sourceKind := payload.Scopes[0].CursorSource
		for _, scope := range payload.Scopes {
			if scope.CursorSource != sourceKind {
				return errors.New("Kotlin Android authored pull cursor sources are mixed")
			}
		}
		var expected []string
		switch sourceKind {
		case "none":
		case "local_checkpoint":
			for _, scope := range payload.Scopes {
				if checkpoint, ok := withinCallCheckpoints[scope.ScopeID]; ok {
					expected = append(expected, checkpoint)
				}
			}
			if len(expected) != 0 {
				if len(expected) != len(payload.Scopes) {
					return errors.New("Kotlin Android within-call rebuild checkpoints do not cover authored pull scopes")
				}
			} else {
				states, err := androidCursorScopeStates(source.ScopeStates)
				if err != nil || len(states) != len(payload.Scopes) {
					return errors.New("Kotlin Android local checkpoint sources do not match authored pull scopes")
				}
				expected = make([]string, 0, len(states))
				for _, state := range states {
					if state.Cursor == nil || *state.Cursor == "" {
						return errors.New("Kotlin Android local checkpoint cursor is absent")
					}
					expected = append(expected, cursorFingerprint(*state.Cursor))
				}
			}
			sort.Strings(expected)
		default:
			return errors.New("Kotlin Android authored pull cursor source is unsupported")
		}
		if !reflect.DeepEqual(expected, observation.CursorFingerprints) {
			return errors.New("Kotlin Android pull cursor fingerprints do not match durable checkpoints")
		}
	case "rebuild":
		var payload struct {
			RebuildID    string `json:"rebuild_id"`
			CursorSource string `json:"cursor_source"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.RebuildID == "" {
			return errors.New("decode Kotlin Android authored rebuild cursor source failed")
		}
		facts := observation.RequestFacts
		if facts == nil || facts.CursorPresent == nil {
			return errors.New("Kotlin Android rebuild cursor facts are absent")
		}
		switch payload.CursorSource {
		case "none":
			if *facts.CursorPresent || facts.CursorFingerprint != nil {
				return errors.New("Kotlin Android rebuild request used an unexpected cursor")
			}
		case "local_rebuild_continuation":
			attempts, err := androidRebuildAttempts(source.RebuildAttempts)
			if err != nil {
				return err
			}
			var cursor string
			matches := 0
			for _, attempt := range attempts {
				if attempt.RebuildID == payload.RebuildID {
					matches++
					if attempt.Cursor != nil {
						cursor = *attempt.Cursor
					}
				}
			}
			if matches != 1 || cursor == "" || !*facts.CursorPresent || facts.CursorFingerprint == nil || *facts.CursorFingerprint != cursorFingerprint(cursor) {
				return errors.New("Kotlin Android rebuild cursor fingerprint does not match the durable continuation")
			}
		case "forged":
			if !*facts.CursorPresent || facts.CursorFingerprint == nil || *facts.CursorFingerprint != cursorFingerprint(forgedRebuildCursor) {
				return errors.New("Kotlin Android forged rebuild cursor fingerprint does not match the deterministic override")
			}
		default:
			return errors.New("Kotlin Android authored rebuild cursor source is unsupported")
		}
	}
	return nil
}

func cursorFingerprint(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func operationUsesForgedRebuildCursor(operation scenarios.Operation) bool {
	if operation.ContractOperation != "rebuild" {
		return false
	}
	var payload struct {
		CursorSource string `json:"cursor_source"`
	}
	return json.Unmarshal(operation.Payload, &payload) == nil && payload.CursorSource == "forged"
}

func clientCallResult(result Result) (ClientCallResult, error) {
	if result.CallID == nil || result.State == nil || *result.CallID == "" || *result.State == "" {
		return ClientCallResult{}, errors.New("Kotlin Android client call result is incomplete")
	}
	completion := ""
	if result.Completion != nil {
		completion = *result.Completion
	}
	return ClientCallResult{CallID: *result.CallID, State: *result.State, Completion: completion}, nil
}

func validCompletion(value string) bool {
	switch value {
	case "idle", "blocked", "error":
		return true
	default:
		return false
	}
}

func synchronizationResult(completion string, steps []StepObservation, window operationWindow) SynchronizationResult {
	result := SynchronizationResult{
		Completion:                completion,
		Steps:                     steps,
		ProvenanceMaintenanceWork: window.provenanceMaintenanceWork,
		ReplayedMutationCount:     window.replayedMutations,
		transportObservations:     cloneObservations(window.observations),
	}
	if window.duration > 0 {
		result.DurationNanoseconds = uint64(window.duration)
	}
	return result
}

func clientCallResultWithWindow(result ClientCallResult, window operationWindow) ClientCallResult {
	if window.duration > 0 {
		result.DurationNanoseconds = uint64(window.duration)
	}
	result.ProvenanceMaintenanceWork = window.provenanceMaintenanceWork
	result.ReplayedMutationCount = window.replayedMutations
	return result
}

func observationWithWindow(observation StepObservation, window operationWindow) StepObservation {
	if window.duration > 0 {
		observation.DurationNanoseconds = uint64(window.duration)
	}
	observation.ProvenanceMaintenanceWork = window.provenanceMaintenanceWork
	observation.ReplayedMutationCount = window.replayedMutations
	return observation
}

func (p *Platform) completeWindow(ctx context.Context, client *platformClient, checkpoint uint64, started time.Time, before Result) (operationWindow, error) {
	after, err := captureClientState(ctx, client)
	if err != nil {
		return operationWindow{}, err
	}
	observations, err := client.session.ObservationsAfter(checkpoint)
	if err != nil {
		return operationWindow{}, err
	}
	return client.windowFromResults(started, before, after, observations)
}

func (c *platformClient) windowFromResults(started time.Time, before, after Result, observations []TransportObservation) (operationWindow, error) {
	work, err := c.maintenanceWorkDelta(before, after)
	if err != nil {
		return operationWindow{}, err
	}
	return operationWindow{
		observations:              cloneObservations(observations),
		duration:                  time.Since(started),
		provenanceMaintenanceWork: work,
	}, nil
}

func (c *platformClient) maintenanceWorkDelta(before, after Result) (uint64, error) {
	beforeCursor, err := maintenanceCursor(before)
	if err != nil {
		return 0, err
	}
	afterCursor, err := maintenanceCursor(after)
	if err != nil {
		return 0, err
	}
	if beforeCursor < c.maintenanceCursor || afterCursor < beforeCursor {
		return 0, errors.New("Kotlin Android provenance maintenance cursor moved backward")
	}
	c.maintenanceCursor = afterCursor
	return uint64(afterCursor - beforeCursor), nil
}

func (c *platformClient) advanceMaintenanceCursor(result Result) error {
	cursor, err := maintenanceCursor(result)
	if err != nil {
		return err
	}
	if cursor < c.maintenanceCursor {
		return errors.New("Kotlin Android provenance maintenance cursor moved backward")
	}
	c.maintenanceCursor = cursor
	return nil
}

func maintenanceCursor(result Result) (int64, error) {
	if result.ProvenanceMaintenanceWorkCursor == nil || *result.ProvenanceMaintenanceWorkCursor < 0 {
		return 0, errors.New("Kotlin Android provenance maintenance cursor is unavailable")
	}
	return *result.ProvenanceMaintenanceWorkCursor, nil
}

func replayedMutationCount(observations []TransportObservation) int {
	count := 0
	for _, observation := range observations {
		if observation.OperationClass != "push" || observation.RequestFacts == nil || observation.RequestFacts.MutationCount == nil {
			continue
		}
		if *observation.RequestFacts.MutationCount > 0 {
			count += *observation.RequestFacts.MutationCount
		}
	}
	return count
}

// Capture reads strict durable facts from every requested client.
func (p *Platform) Capture(ctx context.Context, clients []Client, sources []string) ([]CaptureFacts, error) {
	if err := platformContext(ctx); err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, errors.New("Kotlin Android capture has no sources")
	}
	if len(clients) == 0 {
		return nil, errors.New("Kotlin Android capture has no clients")
	}
	if err := validateCaptureSources(sources); err != nil {
		return nil, err
	}
	results := make([]captureResult, 0, len(clients))
	seen := make(map[string]struct{}, len(clients))
	for _, client := range clients {
		if _, duplicate := seen[client.Key]; duplicate {
			return nil, fmt.Errorf("Kotlin Android capture client %q is duplicated", client.Key)
		}
		seen[client.Key] = struct{}{}
		state, err := p.clientFor(client)
		if err != nil {
			return nil, err
		}
		state.mu.Lock()
		if err := state.available("capture"); err != nil {
			state.mu.Unlock()
			return nil, err
		}
		result, err := captureClientState(ctx, state)
		if err == nil {
			results = append(results, captureResult{state: state, result: result})
		}
		state.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("capture Kotlin Android state: %w", err)
		}
	}

	observations := make([]CaptureFacts, 0, len(sources))
	for _, source := range sources {
		facts, err := captureFactsForSource(source, results)
		if err != nil {
			return nil, err
		}
		replayed := 0
		var maintenanceWork uint64
		for _, value := range results {
			replayed += replayedMutationCountFromResult(value.result)
			if value.result.ProvenanceMaintenanceWorkCursor == nil || *value.result.ProvenanceMaintenanceWorkCursor < 0 {
				return nil, errors.New("Kotlin Android capture maintenance cursor is invalid")
			}
			maintenanceWork += uint64(*value.result.ProvenanceMaintenanceWorkCursor)
		}
		observations = append(observations, CaptureFacts{
			Source:                    source,
			StateFacts:                facts,
			ProvenanceMaintenanceWork: maintenanceWork,
			ReplayedMutationCount:     replayed,
		})
	}
	return observations, nil
}

type captureResult struct {
	state  *platformClient
	result Result
}

func captureClientState(ctx context.Context, client *platformClient) (Result, error) {
	selectors, err := androidSelectors(client)
	if err != nil {
		return Result{}, err
	}
	if len(selectors) <= maximumSelectors {
		return captureClientStateBatch(ctx, client, selectors)
	}
	baseline, err := captureClientStateBatch(ctx, client, nil)
	if err != nil {
		return Result{}, err
	}
	if *baseline.ApplicationRowCount > maximumRows {
		return baseline, nil
	}
	rows, err := androidApplicationRows(baseline.ApplicationRows)
	if err != nil {
		return Result{}, err
	}
	if len(rows) == *baseline.ApplicationRowCount {
		return baseline, nil
	}
	baselineRows := append([]map[string]json.RawMessage(nil), rows...)
	for start := 0; start < len(selectors); start += maximumSelectors {
		end := min(start+maximumSelectors, len(selectors))
		captured, err := captureClientStateBatch(ctx, client, selectors[start:end])
		if err != nil {
			return Result{}, err
		}
		if !equalAndroidCaptureState(baseline, captured) {
			return Result{}, errors.New("Kotlin Android capture changed between selector batches")
		}
		extra, err := applicationRowsBeyondBaseline(baselineRows, captured.ApplicationRows)
		if err != nil {
			return Result{}, err
		}
		rows = append(rows, extra...)
	}
	if len(rows) != *baseline.ApplicationRowCount {
		return Result{}, errors.New("Kotlin Android selector batches did not cover application rows")
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		return Result{}, errors.New("encode Kotlin Android captured application rows failed")
	}
	baseline.ApplicationRows = encoded
	return baseline, nil
}

func captureClientStateBatch(ctx context.Context, client *platformClient, selectors []RowSelector) (Result, error) {
	result, err := client.session.Execute(ctx, Request{Operation: "capture", RowSelectors: &selectors})
	if err != nil {
		return Result{}, err
	}
	if err := validateCapturedClientState(result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func validateCapturedClientState(result Result) error {
	if result.Status == nil || *result.Status == "" || result.ApplicationRowCount == nil || result.MutationLedgerCount == nil || result.MutationOutcomeCount == nil || result.SealedBatchCount == nil || result.RejectedMutationCount == nil || result.ScopeStateCount == nil || result.ScopeRowCount == nil || result.ProvenanceCount == nil || result.RowMetadataCount == nil || result.RebuildAttemptCount == nil || result.RebuildReceiptCount == nil || result.ProvenanceMaintenanceWorkCursor == nil || *result.ProvenanceMaintenanceWorkCursor < 0 {
		return errors.New("Kotlin Android capture facts are incomplete")
	}
	if (*result.ApplicationRowCount <= maximumRows) != presentJSON(result.ApplicationRows) ||
		(*result.MutationLedgerCount <= maximumRecords) != presentJSON(result.RetainedMutations) ||
		(*result.RejectedMutationCount <= maximumRecords) != presentJSON(result.RejectedMutations) ||
		(*result.ScopeStateCount <= maximumRecords) != presentJSON(result.ScopeStates) ||
		(*result.ScopeRowCount <= maximumRecords) != presentJSON(result.ScopeRows) ||
		(*result.RowMetadataCount <= maximumRecords) != presentJSON(result.RowMetadata) ||
		(*result.RebuildAttemptCount <= maximumRecords) != presentJSON(result.RebuildAttempts) ||
		(*result.RebuildReceiptCount <= maximumRecords) != presentJSON(result.RebuildReceipts) ||
		(*result.RebuildReceiptCount <= maximumRecords) != presentJSON(result.RebuildReceiptProofs) {
		return errors.New("Kotlin Android capture detail bounds are inconsistent")
	}
	if *result.ScopeRowCount <= maximumRecords {
		rows, err := androidScopeRows(result.ScopeRows)
		if err != nil {
			return err
		}
		if len(rows) != *result.ScopeRowCount {
			return errors.New("Kotlin Android scope-row count does not match detail")
		}
	}
	if *result.RejectedMutationCount <= maximumRecords {
		values, err := androidOutcomeFacts(result.RejectedMutations)
		if err != nil {
			return err
		}
		if len(values) != *result.RejectedMutationCount {
			return errors.New("Kotlin Android rejected-mutation count does not match detail")
		}
	}
	if *result.ScopeStateCount <= maximumRecords {
		values, err := androidCheckpointFacts(result.ScopeStates)
		if err != nil {
			return err
		}
		if len(values) != *result.ScopeStateCount {
			return errors.New("Kotlin Android scope-state count does not match detail")
		}
	}
	if *result.RebuildAttemptCount <= maximumRecords {
		values, err := androidRebuildAttempts(result.RebuildAttempts)
		if err != nil {
			return err
		}
		if len(values) != *result.RebuildAttemptCount {
			return errors.New("Kotlin Android rebuild-attempt count does not match detail")
		}
	}
	if *result.RebuildReceiptCount <= maximumRecords {
		proofs, err := androidRebuildReceiptProofs(result.RebuildReceiptProofs)
		if err != nil {
			return err
		}
		pageCount := 0
		seen := make(map[string]struct{}, len(proofs))
		for _, proof := range proofs {
			if proof.PageCount > *result.RebuildReceiptCount-pageCount {
				return errors.New("Kotlin Android rebuild receipt proof is invalid")
			}
			if _, duplicate := seen[proof.RebuildIDFingerprint]; duplicate {
				return errors.New("Kotlin Android rebuild receipt proof is duplicated")
			}
			seen[proof.RebuildIDFingerprint] = struct{}{}
			pageCount += proof.PageCount
		}
		if pageCount != *result.RebuildReceiptCount {
			return errors.New("Kotlin Android rebuild receipt count does not match proof detail")
		}
	}
	return nil
}

func androidApplicationRows(raw json.RawMessage) ([]map[string]json.RawMessage, error) {
	var rows []map[string]json.RawMessage
	if err := decodeFactArray(raw, &rows, maximumRows); err != nil {
		return nil, errors.New("Kotlin Android application-row inspection is invalid")
	}
	return rows, nil
}

func equalAndroidCaptureState(left, right Result) bool {
	left.ApplicationRows = nil
	right.ApplicationRows = nil
	return reflect.DeepEqual(left, right)
}

func applicationRowsBeyondBaseline(baseline []map[string]json.RawMessage, raw json.RawMessage) ([]map[string]json.RawMessage, error) {
	captured, err := androidApplicationRows(raw)
	if err != nil {
		return nil, err
	}
	remaining := make(map[string]int, len(baseline))
	for _, row := range baseline {
		key, err := applicationRowKey(row)
		if err != nil {
			return nil, err
		}
		remaining[key]++
	}
	extra := make([]map[string]json.RawMessage, 0, len(captured))
	for _, row := range captured {
		key, err := applicationRowKey(row)
		if err != nil {
			return nil, err
		}
		if remaining[key] > 0 {
			remaining[key]--
			continue
		}
		extra = append(extra, row)
	}
	for _, count := range remaining {
		if count != 0 {
			return nil, errors.New("Kotlin Android selector batch omitted a baseline application row")
		}
	}
	return extra, nil
}

func applicationRowKey(row map[string]json.RawMessage) (string, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return "", errors.New("encode Kotlin Android application row identity failed")
	}
	return string(encoded), nil
}

func presentJSON(raw json.RawMessage) bool {
	return !absentJSON(raw) && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func androidCount(value *int) uint64 {
	return uint64(*value)
}

func androidSelectors(client *platformClient) ([]RowSelector, error) {
	values := make([]RowSelector, 0, len(client.selectors))
	for _, value := range client.selectors {
		values = append(values, value)
	}
	sort.Slice(values, func(left, right int) bool { return selectorKey(values[left]) < selectorKey(values[right]) })
	return values, nil
}

func selectorKey(value RowSelector) string {
	encoded, _ := json.Marshal(value.PrimaryKey)
	return value.TableName + "\x00" + value.PrimaryKeyField + "\x00" + string(encoded)
}

func captureFactsForSource(source string, values []captureResult) (scenarios.StateFacts, error) {
	var facts scenarios.StateFacts
	for _, value := range values {
		clientFacts, err := androidClientFactsForSource(source, value.state, value.result)
		if err != nil {
			return scenarios.StateFacts{}, err
		}
		if clientFacts != nil {
			facts.Clients = append(facts.Clients, *clientFacts)
		}
	}
	return facts, nil
}

func androidClientFactsForSource(source string, client *platformClient, result Result) (*scenarios.ClientDurabilityFact, error) {
	facts := scenarios.ClientDurabilityFact{UserID: client.client.UserID, ClientID: client.client.ClientID}
	if schema, err := androidSchemaFact(result.Schema); err != nil {
		return nil, err
	} else if schema != nil {
		facts.CurrentSchema = schema
	}
	switch source {
	case "application-rows":
		count := androidCount(result.ApplicationRowCount)
		facts.RowCount = &count
	case "pending-mutations":
		var queue []scenarios.QueuedMutationFact
		if presentJSON(result.RetainedMutations) {
			var err error
			queue, err = androidQueuedMutationFacts(result.RetainedMutations)
			if err != nil {
				return nil, err
			}
		}
		count := androidCount(result.MutationLedgerCount)
		facts.QueueCount = &count
		facts.Queue = queue
		sealedBatchCount := androidCount(result.SealedBatchCount)
		facts.SealedBatchCount = &sealedBatchCount
	case "rejected-mutations":
		var outcomes []scenarios.MutationOutcomeFact
		if presentJSON(result.RejectedMutations) {
			var err error
			outcomes, err = androidOutcomeFacts(result.RejectedMutations)
			if err != nil {
				return nil, err
			}
		}
		count := androidCount(result.MutationOutcomeCount)
		facts.OutcomeCount = &count
		facts.Outcomes = outcomes
	case "scope-state", "checkpoints":
		var checkpoints []scenarios.CheckpointFact
		if presentJSON(result.Checkpoints) {
			var err error
			checkpoints, err = androidCheckpointFacts(result.Checkpoints)
			if err != nil {
				return nil, err
			}
		}
		count := androidCount(result.ScopeStateCount)
		facts.CheckpointCount = &count
		facts.Checkpoints = checkpoints
	case "provenance":
		var provenance []scenarios.ProvenanceFact
		if presentJSON(result.Provenance) {
			var err error
			provenance, err = androidProvenanceFacts(result.Provenance)
			if err != nil {
				return nil, err
			}
		}
		count := androidCount(result.ProvenanceCount)
		facts.ProvenanceCount = &count
		facts.Provenance = provenance
	case "rebuild-state":
		count, err := androidRebuildAttemptFactCount(result)
		if err != nil {
			return nil, err
		}
		facts.RebuildAttemptCount = &count
	case "sync-status", "sync-events", "request-trace", "process-trace":
		return nil, nil
	default:
		return nil, fmt.Errorf("Kotlin Android capture source %q is unsupported", source)
	}
	return &facts, nil
}

func validateCaptureSources(sources []string) error {
	seen := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if _, duplicate := seen[source]; duplicate {
			return fmt.Errorf("Kotlin Android capture source %q is duplicated", source)
		}
		seen[source] = struct{}{}
		switch source {
		case "application-rows", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "scope-state", "checkpoints", "provenance", "rebuild-state", "request-trace", "process-trace":
		default:
			return fmt.Errorf("Kotlin Android capture source %q is unsupported", source)
		}
	}
	return nil
}

func androidSchemaFact(raw json.RawMessage) (*scenarios.SchemaFact, error) {
	if absentJSON(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var value struct {
		Version int64  `json:"version"`
		Hash    string `json:"hash"`
	}
	if err := decodeStrictFact(raw, &value); err != nil || value.Version <= 0 || !validLowerHexDigest(value.Hash) {
		return nil, errors.New("Kotlin Android schema inspection is invalid")
	}
	version := uint64(value.Version)
	return &scenarios.SchemaFact{Version: version, Hash: value.Hash}, nil
}

type scopeStateRecord struct {
	ScopeID       string  `json:"scope_id"`
	Cursor        *string `json:"cursor"`
	Checksum      *string `json:"checksum"`
	Generation    int64   `json:"generation"`
	LocalChecksum string  `json:"local_checksum"`
}

func androidCursorScopeStates(raw json.RawMessage) ([]scopeStateRecord, error) {
	var values []scopeStateRecord
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android scope-state cursor inspection is invalid")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ScopeID == "" || value.Generation < 0 || value.Cursor != nil && *value.Cursor == "" {
			return nil, errors.New("Kotlin Android scope-state cursor inspection is invalid")
		}
		if _, duplicate := seen[value.ScopeID]; duplicate {
			return nil, errors.New("Kotlin Android scope-state cursor inspection is duplicated")
		}
		seen[value.ScopeID] = struct{}{}
	}
	return values, nil
}

type scopeRowRecord struct {
	ScopeID    string `json:"scope_id"`
	TableName  string `json:"table_name"`
	RecordID   string `json:"record_id"`
	Checksum   string `json:"checksum"`
	Generation int64  `json:"generation"`
}

type retainedMutation struct {
	MutationID            string          `json:"mutation_id"`
	LocalOrder            int64           `json:"local_order"`
	TableID               string          `json:"table_id"`
	TableName             string          `json:"table_name"`
	RecordID              string          `json:"record_id"`
	PrimaryKeyFieldID     string          `json:"primary_key_field_id"`
	PrimaryKeyLogicalType string          `json:"primary_key_logical_type"`
	Operation             string          `json:"operation"`
	AuthoredSchema        schemaRef       `json:"authored_schema"`
	BaseVersion           *string         `json:"base_version"`
	ClientVersion         string          `json:"client_version"`
	Status                string          `json:"status"`
	AuthoredFields        []retainedField `json:"authored_fields"`
}

type retainedField struct {
	FieldID     string          `json:"field_id"`
	LogicalType string          `json:"logical_type"`
	Value       json.RawMessage `json:"value"`
}

type schemaRef struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
}

type rejectedMutation struct {
	MutationID    string          `json:"mutation_id"`
	TableName     string          `json:"table_name"`
	RecordID      string          `json:"record_id"`
	Status        string          `json:"status"`
	Code          string          `json:"code"`
	Message       json.RawMessage `json:"message"`
	ServerRow     json.RawMessage `json:"server_row"`
	ServerVersion json.RawMessage `json:"server_version"`
	Mutation      json.RawMessage `json:"mutation"`
	Rejection     json.RawMessage `json:"rejection"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
}

type provenanceRecord struct {
	TableName     string   `json:"table_name"`
	RecordID      string   `json:"record_id"`
	ScopeIDs      []string `json:"scope_ids"`
	ServerVersion *string  `json:"server_version"`
}

type rebuildAttemptRecord struct {
	ScopeID          string  `json:"scope_id"`
	RebuildID        string  `json:"rebuild_id"`
	ClientGeneration int64   `json:"client_generation"`
	SchemaVersion    int64   `json:"schema_version"`
	SchemaHash       string  `json:"schema_hash"`
	Generation       int64   `json:"generation"`
	Cursor           *string `json:"cursor"`
	PageLimit        int     `json:"page_limit"`
}

func androidScopeRows(raw json.RawMessage) ([]scopeRowRecord, error) {
	var values []scopeRowRecord
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android scope row inspection is invalid")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ScopeID == "" || value.TableName == "" || value.RecordID == "" || value.Checksum == "" || value.Generation < 0 {
			return nil, errors.New("Kotlin Android scope row inspection is invalid")
		}
		key := value.ScopeID + "\x00" + value.TableName + "\x00" + value.RecordID
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("Kotlin Android scope row inspection is duplicated")
		}
		seen[key] = struct{}{}
	}
	return values, nil
}

func androidCheckpointFacts(raw json.RawMessage) ([]scenarios.CheckpointFact, error) {
	var values []scopeStateRecord
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android checkpoint inspection is invalid")
	}
	result := make([]scenarios.CheckpointFact, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ScopeID == "" || value.Generation < 0 {
			return nil, errors.New("Kotlin Android checkpoint inspection is invalid")
		}
		if _, duplicate := seen[value.ScopeID]; duplicate {
			return nil, errors.New("Kotlin Android checkpoint inspection is duplicated")
		}
		seen[value.ScopeID] = struct{}{}
		checksum, err := androidChecksumDigest(value.Checksum)
		if err != nil {
			return nil, err
		}
		var localChecksum *string
		if value.LocalChecksum != "" {
			localChecksum, err = androidChecksumDigest(&value.LocalChecksum)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, scenarios.CheckpointFact{
			ScopeID:     value.ScopeID,
			HasCursor:   value.Cursor != nil,
			HasChecksum: checksum != nil,
			Checksum:    checksum,
			Verified:    checksum != nil && localChecksum != nil && *checksum == *localChecksum,
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ScopeID < result[right].ScopeID })
	return result, nil
}

func androidChecksumDigest(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	var object struct {
		Algorithm string `json:"algorithm"`
		Version   int    `json:"version"`
		Encoding  string `json:"encoding"`
		Digest    string `json:"digest"`
	}
	if err := decodeStrictFact([]byte(*value), &object); err != nil || object.Algorithm != "sha256" || object.Version != 1 || object.Encoding != "hex" || !validLowerHexDigest(object.Digest) {
		return nil, errors.New("Kotlin Android checksum inspection is invalid")
	}
	digest := object.Digest
	return &digest, nil
}

func androidQueuedMutationFacts(raw json.RawMessage) ([]scenarios.QueuedMutationFact, error) {
	var values []retainedMutation
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android queued mutation inspection is invalid")
	}
	result := make([]scenarios.QueuedMutationFact, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.MutationID == "" || value.LocalOrder < 0 || value.TableID == "" || value.RecordID == "" || value.AuthoredSchema.Version <= 0 || !validLowerHexDigest(value.AuthoredSchema.Hash) {
			return nil, errors.New("Kotlin Android queued mutation inspection is invalid")
		}
		if _, duplicate := seen[value.MutationID]; duplicate {
			return nil, errors.New("Kotlin Android queued mutation inspection is duplicated")
		}
		seen[value.MutationID] = struct{}{}
		identity, err := androidRecordIDWireJSON(value.RecordID, value.PrimaryKeyLogicalType)
		if err != nil {
			return nil, err
		}
		columns := make([]scenarios.FieldFact, 0, len(value.AuthoredFields))
		for _, field := range value.AuthoredFields {
			if field.FieldID == "" || field.LogicalType == "" || !strictJSONValue(field.Value) {
				return nil, errors.New("Kotlin Android queued mutation field is invalid")
			}
			columns = append(columns, scenarios.FieldFact{FieldID: field.FieldID, Type: field.LogicalType, WireJSON: string(field.Value)})
		}
		sort.Slice(columns, func(left, right int) bool { return columns[left].FieldID < columns[right].FieldID })
		result = append(result, scenarios.QueuedMutationFact{
			MutationID:        value.MutationID,
			TableID:           value.TableID,
			CanonicalWireJSON: identity,
			AuthoredSchema:    scenarios.SchemaFact{Version: uint64(value.AuthoredSchema.Version), Hash: value.AuthoredSchema.Hash},
			Operation:         value.Operation,
			BaseVersion:       clonePointer(value.BaseVersion),
			ClientVersion:     value.ClientVersion,
			AuthoredColumns:   columns,
			LocalOrder:        uint64(value.LocalOrder),
			Status:            value.Status,
		})
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].LocalOrder != result[right].LocalOrder {
			return result[left].LocalOrder < result[right].LocalOrder
		}
		return result[left].MutationID < result[right].MutationID
	})
	return result, nil
}

func androidOutcomeFacts(raw json.RawMessage) ([]scenarios.MutationOutcomeFact, error) {
	var values []rejectedMutation
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android rejected mutation inspection is invalid")
	}
	result := make([]scenarios.MutationOutcomeFact, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.MutationID == "" || value.Status == "" || value.Code == "" {
			return nil, errors.New("Kotlin Android rejected mutation inspection is invalid")
		}
		if _, duplicate := seen[value.MutationID]; duplicate {
			return nil, errors.New("Kotlin Android rejected mutation inspection is duplicated")
		}
		seen[value.MutationID] = struct{}{}
		result = append(result, scenarios.MutationOutcomeFact{MutationID: value.MutationID, State: value.Status, Reason: value.Code})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].MutationID < result[right].MutationID })
	return result, nil
}

func androidProvenanceFacts(raw json.RawMessage) ([]scenarios.ProvenanceFact, error) {
	var values []provenanceRecord
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android provenance inspection is invalid")
	}
	result := make([]scenarios.ProvenanceFact, 0, len(values))
	seenRecords := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.TableName == "" || value.RecordID == "" || value.ServerVersion == nil || *value.ServerVersion == "" || len(value.ScopeIDs) == 0 || len(value.ScopeIDs) > 128 {
			return nil, errors.New("Kotlin Android provenance inspection is invalid")
		}
		key := value.TableName + "\x00" + value.RecordID
		if _, duplicate := seenRecords[key]; duplicate {
			return nil, errors.New("Kotlin Android provenance inspection is duplicated")
		}
		seenRecords[key] = struct{}{}
		scopes := append([]string(nil), value.ScopeIDs...)
		seenScopes := make(map[string]struct{}, len(scopes))
		for _, scope := range scopes {
			if scope == "" {
				return nil, errors.New("Kotlin Android provenance scope is invalid")
			}
			if _, duplicate := seenScopes[scope]; duplicate {
				return nil, errors.New("Kotlin Android provenance scope is duplicated")
			}
			seenScopes[scope] = struct{}{}
		}
		canonical, err := json.Marshal(value.RecordID)
		if err != nil {
			return nil, errors.New("encode Kotlin Android provenance identity failed")
		}
		sort.Strings(scopes)
		result = append(result, scenarios.ProvenanceFact{
			TableID:           value.TableName,
			CanonicalWireJSON: string(canonical),
			Scopes:            scopes,
			Version:           *value.ServerVersion,
		})
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].TableID+"\x00"+result[left].CanonicalWireJSON < result[right].TableID+"\x00"+result[right].CanonicalWireJSON
	})
	return result, nil
}

func androidRebuildAttempts(raw json.RawMessage) ([]rebuildAttemptRecord, error) {
	var values []rebuildAttemptRecord
	if err := decodeFactArray(raw, &values, 512); err != nil {
		return nil, errors.New("Kotlin Android rebuild inspection is invalid")
	}
	for _, value := range values {
		if value.ScopeID == "" || value.RebuildID == "" || value.ClientGeneration < 0 || value.SchemaVersion <= 0 || !validLowerHexDigest(value.SchemaHash) || value.Generation < 0 || value.PageLimit < 1 || value.PageLimit > 1000 {
			return nil, errors.New("Kotlin Android rebuild inspection is invalid")
		}
	}
	return values, nil
}

func androidRebuildReceiptProofs(raw json.RawMessage) ([]rebuildReceiptProofRecord, error) {
	var values []rebuildReceiptProofRecord
	if err := decodeFactArray(raw, &values, maximumRecords); err != nil {
		return nil, errors.New("Kotlin Android rebuild receipt proof inspection is invalid")
	}
	for _, value := range values {
		if !validLowerHexDigest(value.RebuildIDFingerprint) || value.PageCount <= 0 || value.ReturnedRecordCount < 0 {
			return nil, errors.New("Kotlin Android rebuild receipt proof inspection is invalid")
		}
	}
	return values, nil
}

func androidRebuildAttemptFactCount(result Result) (uint64, error) {
	attempts, err := androidRebuildAttempts(result.RebuildAttempts)
	if err != nil {
		return 0, err
	}
	proofs, err := androidRebuildReceiptProofs(result.RebuildReceiptProofs)
	if err != nil {
		return 0, err
	}
	identities := make(map[string]struct{}, len(attempts)+len(proofs))
	for _, attempt := range attempts {
		identities[cursorFingerprint(attempt.RebuildID)] = struct{}{}
	}
	for _, proof := range proofs {
		identities[proof.RebuildIDFingerprint] = struct{}{}
	}
	return uint64(len(identities)), nil
}

func androidRecordIDWireJSON(recordID, logicalType string) (string, error) {
	switch logicalType {
	case "string", "decimal", "datetime", "date", "time", "json":
		value, err := json.Marshal(recordID)
		if err != nil {
			return "", errors.New("encode Kotlin Android record identity failed")
		}
		return string(value), nil
	case "int", "int64":
		parsed, err := strconv.ParseInt(recordID, 10, 64)
		if err != nil || strconv.FormatInt(parsed, 10) != recordID {
			return "", errors.New("Kotlin Android integer record identity is invalid")
		}
		return recordID, nil
	default:
		return "", errors.New("Kotlin Android primary-key type has no conformance identity mapping")
	}
}

func replayedMutationCountFromResult(result Result) int {
	if result.TransportObservations == nil {
		return 0
	}
	return replayedMutationCount(result.TransportObservations.Observations)
}

func decodeFactArray(raw json.RawMessage, target any, maximum int) error {
	if absentJSON(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("fact array is absent")
	}
	if !strictJSONValue(raw) || bytes.TrimSpace(raw)[0] != '[' {
		return errors.New("fact array is invalid")
	}
	if err := decodeStrictFact(raw, target); err != nil {
		return err
	}
	value := reflectSliceLength(target)
	if value > maximum {
		return errors.New("fact array is out of bounds")
	}
	return nil
}

func reflectSliceLength(target any) int {
	value := reflect.ValueOf(target)
	if value.IsValid() && value.Kind() == reflect.Pointer && !value.IsNil() {
		value = value.Elem()
		if value.Kind() == reflect.Slice {
			return value.Len()
		}
	}
	return 0
}

func decodeStrictFact(raw []byte, target any) error {
	if !strictJSONValue(raw) {
		return errors.New("strict fact JSON is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("strict fact JSON has trailing data")
	}
	return nil
}

func strictJSONValue(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	wrapped := make([]byte, 0, len(trimmed)+10)
	wrapped = append(wrapped, `{"value":`...)
	wrapped = append(wrapped, trimmed...)
	wrapped = append(wrapped, '}')
	return jsonstrict.ValidateValue(wrapped) == nil
}

func absentJSON(raw json.RawMessage) bool { return len(bytes.TrimSpace(raw)) == 0 }

func operationIdentityMatches(operation scenarios.Operation, client Client) error {
	var payload map[string]json.RawMessage
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return errors.New("Kotlin Android operation identity is invalid")
	}
	userField, clientField := "user_id", "client_id"
	if operation.ContractOperation == "local" || operation.ContractOperation == "process" {
		userField = "authenticated_user_id"
	}
	if operation.ContractOperation == "process" && operation.Name == "restart-client" {
		userField = "user_id"
	}
	if operation.ContractOperation == "push" {
		var authenticatedUser string
		if err := json.Unmarshal(payload["authenticated_user_id"], &authenticatedUser); err != nil || authenticatedUser != client.UserID {
			return errors.New("Kotlin Android operation identity does not match client")
		}
		var request map[string]json.RawMessage
		if err := jsonstrict.Decode(payload["request"], &request); err != nil {
			return errors.New("Kotlin Android push identity is invalid")
		}
		payload = request
		userField = ""
	}
	var userID, clientID string
	if userField != "" {
		if err := json.Unmarshal(payload[userField], &userID); err != nil || userID != client.UserID {
			return errors.New("Kotlin Android operation identity does not match client")
		}
	}
	if err := json.Unmarshal(payload[clientField], &clientID); err != nil || clientID != client.ClientID {
		return errors.New("Kotlin Android operation identity does not match client")
	}
	return nil
}

func requestDispatch(operation scenarios.Operation) (string, bool, string, error) {
	switch scenarios.OperationKey(operation) {
	case "connect/send":
		return "connect", false, "", nil
	case "pull/request-page":
		return "pull", false, "", nil
	case "rebuild/request-page":
		return "rebuild", false, "", nil
	case "push/submit":
		var payload struct {
			Request struct {
				BatchID string `json:"batch_id"`
			} `json:"request"`
			Delivery string `json:"delivery"`
		}
		if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.Request.BatchID == "" {
			return "", false, "", errors.New("decode Kotlin Android push request failed")
		}
		switch payload.Delivery {
		case "apply", "transport_failure":
			return "push", false, payload.Request.BatchID, nil
		case "drop_after_server":
			return "push", true, payload.Request.BatchID, nil
		default:
			return "", false, "", errors.New("Kotlin Android push delivery is unsupported")
		}
	default:
		return "", false, "", fmt.Errorf("Kotlin Android request operation %s is unsupported", scenarios.OperationKey(operation))
	}
}

func requestOperationClass(operation scenarios.Operation) (string, bool) {
	operationClass, _, _, err := requestDispatch(operation)
	return operationClass, err == nil
}

func validateRequestOperations(client Client, operations []scenarios.Operation) (string, error) {
	if len(operations) == 0 {
		return "", errors.New("Kotlin Android synchronization has no covered requests")
	}
	var dropBatchID string
	for _, operation := range operations {
		if err := scenarios.ValidateOperation(operation); err != nil {
			return "", fmt.Errorf("Kotlin Android request operation is invalid: %w", err)
		}
		if err := operationIdentityMatches(operation, client); err != nil {
			return "", err
		}
		_, drop, batchID, err := requestDispatch(operation)
		if err != nil {
			return "", err
		}
		if drop {
			if dropBatchID != "" {
				return "", errors.New("Kotlin Android synchronization has multiple response-loss requests")
			}
			dropBatchID = batchID
		}
	}
	return dropBatchID, nil
}

func responseLossBatch(operation scenarios.Operation, client Client) (string, error) {
	var payload struct {
		AuthenticatedUserID string `json:"authenticated_user_id"`
		ClientID            string `json:"client_id"`
		BatchID             string `json:"batch_id"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.AuthenticatedUserID != client.UserID || payload.ClientID != client.ClientID || payload.BatchID == "" {
		return "", errors.New("Kotlin Android response-loss identity is invalid")
	}
	return payload.BatchID, nil
}

func dispatchOperation(operation scenarios.Operation) string {
	switch {
	case scenarios.OperationKey(operation) == "local/write":
		return "apply"
	case func() bool { _, ok := requestOperationClass(operation); return ok }():
		return "request"
	case operation.ContractOperation == "process":
		return "process"
	default:
		return ""
	}
}

func validateClient(client Client) error {
	if client.Key == "" || client.UserID == "" || client.ClientID == "" || client.DatabaseKey == "" || len(client.DatabaseKey) > 128 || strings.ContainsAny(client.DatabaseKey, "/\\\x00\r\n") {
		return errors.New("Kotlin Android client identity is invalid")
	}
	return nil
}

func (p *Platform) clientFor(client Client) (*platformClient, error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("Kotlin Android platform is closed")
	}
	state, found := p.clients[client.Key]
	if !found || state.client.UserID != client.UserID || state.client.ClientID != client.ClientID || state.client.DatabaseKey != client.DatabaseKey {
		return nil, errors.New("Kotlin Android client is unavailable")
	}
	return state, nil
}

func (c *platformClient) available(operation string) error {
	if c.terminated || c.session == nil || c.pendingLoss != nil || c.activeCall != nil {
		return fmt.Errorf("Kotlin Android client is unavailable for %s", operation)
	}
	return nil
}

func selectorFromValues(table, field string, primary TypedValue) RowSelector {
	return RowSelector{TableName: table, PrimaryKeyField: field, PrimaryKey: primary}
}

func selectorKeyForValues(table, field string, primary TypedValue) string {
	return selectorKey(selectorFromValues(table, field, primary))
}

func selectorKeyFromRaw(table, field string, raw json.RawMessage) string {
	return table + "\x00" + field + "\x00" + string(raw)
}

func decodeLocalWrite(operation scenarios.Operation, client Client) (LocalAction, RowSelector, error) {
	var payload map[string]json.RawMessage
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return LocalAction{}, RowSelector{}, errors.New("decode Kotlin Android local write failed")
	}
	var userID, clientID, table, name string
	if json.Unmarshal(payload["authenticated_user_id"], &userID) != nil || json.Unmarshal(payload["client_id"], &clientID) != nil || userID != client.UserID || clientID != client.ClientID {
		return LocalAction{}, RowSelector{}, errors.New("Kotlin Android local write identity does not match client")
	}
	if json.Unmarshal(payload["table_id"], &table) != nil || table == "" || json.Unmarshal(payload["operation"], &name) != nil {
		return LocalAction{}, RowSelector{}, errors.New("Kotlin Android local write fields are invalid")
	}
	primaryField, primaryRaw, err := decodePrimaryKey(payload["pk"])
	if err != nil {
		return LocalAction{}, RowSelector{}, err
	}
	primary, err := typedValue(primaryRaw, false)
	if err != nil {
		return LocalAction{}, RowSelector{}, err
	}
	fields, err := decodeColumns(payload["columns"])
	if err != nil {
		return LocalAction{}, RowSelector{}, err
	}
	action := LocalAction{Operation: name, TableName: table, PrimaryKeyField: primaryField, PrimaryKey: primary, Fields: fields}
	if !validLocalAction(action) {
		return LocalAction{}, RowSelector{}, errors.New("Kotlin Android local write operation is invalid")
	}
	return action, selectorFromValues(table, primaryField, primary), nil
}

func decodePrimaryKey(raw json.RawMessage) (string, json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &object); err != nil || len(object) == 0 {
		return "", nil, errors.New("Kotlin Android local write primary key is invalid")
	}
	if len(object) == 2 {
		fieldRaw, hasField := object["field_id"]
		value, hasValue := object["value"]
		var field string
		if hasField && hasValue && json.Unmarshal(fieldRaw, &field) == nil && validAndroidName(field) == nil && strictJSONValue(value) {
			return field, append(json.RawMessage(nil), value...), nil
		}
	}
	if len(object) != 1 {
		return "", nil, errors.New("Kotlin Android local write primary key shape is unsupported")
	}
	for field, value := range object {
		if validAndroidName(field) != nil || !strictJSONValue(value) {
			return "", nil, errors.New("Kotlin Android local write primary key is invalid")
		}
		return field, append(json.RawMessage(nil), value...), nil
	}
	return "", nil, errors.New("Kotlin Android local write primary key is invalid")
}

func decodeColumns(raw json.RawMessage) (map[string]TypedValue, error) {
	if absentJSON(raw) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return map[string]TypedValue{}, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if trimmed[0] == '{' {
		var object map[string]json.RawMessage
		if err := jsonstrict.Decode(raw, &object); err != nil {
			return nil, errors.New("Kotlin Android local write columns are invalid")
		}
		result := make(map[string]TypedValue, len(object))
		for field, value := range object {
			if validAndroidName(field) != nil {
				return nil, errors.New("Kotlin Android local write column name is invalid")
			}
			decoded, err := typedValue(value, true)
			if err != nil {
				return nil, err
			}
			result[field] = decoded
		}
		return result, nil
	}
	var values []json.RawMessage
	if err := decodeStrictFact(raw, &values); err != nil {
		return nil, errors.New("Kotlin Android local write columns are invalid")
	}
	result := make(map[string]TypedValue, len(values))
	for _, value := range values {
		var object map[string]json.RawMessage
		if err := jsonstrict.Decode(value, &object); err != nil || len(object) != 2 {
			return nil, errors.New("Kotlin Android local write column shape is invalid")
		}
		fieldRaw, hasField := object["field_id"]
		fieldValue, hasValue := object["value"]
		var field string
		if !hasField || !hasValue || json.Unmarshal(fieldRaw, &field) != nil || validAndroidName(field) != nil {
			return nil, errors.New("Kotlin Android local write column is invalid")
		}
		if _, duplicate := result[field]; duplicate {
			return nil, errors.New("Kotlin Android local write column is duplicated")
		}
		decoded, err := typedValue(fieldValue, true)
		if err != nil {
			return nil, err
		}
		result[field] = decoded
	}
	return result, nil
}

func typedValue(raw json.RawMessage, allowNull bool) (TypedValue, error) {
	if !strictJSONValue(raw) {
		return TypedValue{}, errors.New("Kotlin Android typed value is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return TypedValue{}, errors.New("Kotlin Android typed value is invalid")
	}
	if object, ok := value.(map[string]any); ok {
		if len(object) != 2 {
			return TypedValue{}, errors.New("Kotlin Android typed object is invalid")
		}
		kind, ok := object["type"].(string)
		if !ok {
			return TypedValue{}, errors.New("Kotlin Android typed object type is invalid")
		}
		fieldValue, found := object["value"]
		if !found {
			return TypedValue{}, errors.New("Kotlin Android typed object value is missing")
		}
		decoded := TypedValue{Type: kind, Value: fieldValue}
		if !validTypedValue(decoded, allowNull) {
			return TypedValue{}, errors.New("Kotlin Android typed value is invalid")
		}
		return decoded, nil
	}
	var decoded TypedValue
	switch value := value.(type) {
	case nil:
		if !allowNull {
			return TypedValue{}, errors.New("Kotlin Android primary key is null")
		}
		decoded = TypedValue{Type: "null", Value: nil}
	case string:
		decoded = TypedValue{Type: "string", Value: value}
	case bool:
		decoded = TypedValue{Type: "boolean", Value: value}
	case json.Number:
		if strings.ContainsAny(string(value), ".eE") {
			parsed, err := strconv.ParseFloat(string(value), 64)
			if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
				return TypedValue{}, errors.New("Kotlin Android double value is invalid")
			}
			decoded = TypedValue{Type: "double", Value: parsed}
			break
		}
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return TypedValue{}, errors.New("Kotlin Android integer value is invalid")
		}
		decoded = TypedValue{Type: "integer", Value: parsed}
	default:
		return TypedValue{}, errors.New("Kotlin Android typed value is unsupported")
	}
	if !validTypedValue(decoded, allowNull) {
		return TypedValue{}, errors.New("Kotlin Android typed value is invalid")
	}
	return decoded, nil
}

func validAndroidName(value string) error {
	if !validName(value) {
		return errors.New("name is invalid")
	}
	return nil
}

func platformContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Kotlin Android platform context is required")
	}
	return ctx.Err()
}

func (p *Platform) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("Kotlin Android platform close context is required")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	proxy := p.responseProxy
	p.responseProxy = nil
	clients := make([]*platformClient, 0, len(p.clients))
	for _, client := range p.clients {
		clients = append(clients, client)
	}
	p.mu.Unlock()

	var failures []error
	for _, client := range clients {
		client.mu.Lock()
		session := client.session
		client.session = nil
		client.terminated = true
		client.mu.Unlock()
		if session != nil {
			if err := session.Close(ctx); err != nil {
				failures = append(failures, err)
			}
		}
	}
	if proxy != nil {
		proxy.Close()
	}
	return errors.Join(failures...)
}
