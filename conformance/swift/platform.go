package swift

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
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
	maximumMutatedResponseBytes    = 16 << 20
	maximumProxiedPushRequestBytes = 16 << 20
)

type rebuildProxyClientIDKey struct{}

// Client identifies one durable Swift client database.
type Client struct {
	Key         string `json:"key"`
	UserID      string `json:"user_id"`
	ClientID    string `json:"client_id"`
	DatabaseKey string `json:"database_key"`
}

// WireFacts records one client transport result.
type WireFacts struct {
	HTTPStatus int     `json:"http_status"`
	ErrorCode  *string `json:"error_code,omitempty"`
	Retryable  bool    `json:"retryable"`
}

// StepObservation records one direct Swift operation result.
type StepObservation struct {
	Disposition               string     `json:"disposition"`
	ErrorCode                 *string    `json:"error_code,omitempty"`
	Wire                      *WireFacts `json:"wire,omitempty"`
	Completion                string     `json:"completion,omitempty"`
	DurationNanoseconds       uint64     `json:"duration_nanoseconds,omitempty"`
	ProvenanceMaintenanceWork uint64     `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     uint64     `json:"replayed_mutation_count,omitempty"`
}

// RequestOperations are authored HTTP operations covered by one public call.
type RequestOperations []scenarios.Operation

// SynchronizationResult records one completed grouped public call.
type SynchronizationResult struct {
	Completion                string            `json:"completion"`
	Steps                     []StepObservation `json:"steps"`
	DurationNanoseconds       uint64            `json:"duration_nanoseconds,omitempty"`
	ProvenanceMaintenanceWork uint64            `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     uint64            `json:"replayed_mutation_count,omitempty"`
	transportObservations     []transportObservation
}

// CallResult records one paused or completed public call.
type CallResult struct {
	CallID                    string            `json:"call_id"`
	State                     string            `json:"state"`
	Completion                string            `json:"completion,omitempty"`
	Steps                     []StepObservation `json:"steps,omitempty"`
	DurationNanoseconds       uint64            `json:"duration_nanoseconds,omitempty"`
	ProvenanceMaintenanceWork uint64            `json:"provenance_maintenance_work,omitempty"`
	ReplayedMutationCount     uint64            `json:"replayed_mutation_count,omitempty"`
}

// CaptureFacts binds one requested source to durable Swift state.
type CaptureFacts struct {
	Source     string               `json:"source"`
	StateFacts scenarios.StateFacts `json:"state_facts"`
}

// Platform drives one direct Swift runner session for each installed client.
type Platform struct {
	config Config

	mu      sync.Mutex
	closed  bool
	clients map[string]*platformClient

	responseProxy *httptest.Server
	// temporaryUnavailableMisses records why an armed push fault did not apply.
	temporaryUnavailableMisses    []string
	temporaryUnavailablePush      *scenarios.PushWireFaultTarget
	rebuildCursorOverride         string
	rebuildCursorOverrideClientID string
	rebuildResponseCursors        map[string]string
}

type platformClient struct {
	mu sync.Mutex

	client       Client
	databasePath string
	session      *Session
	terminated   bool
	started      bool
	restarted    bool
	callSequence uint64
	selectors    map[string]runnerRowSelector
	pendingLoss  *pendingResponseLoss
	activeCall   *platformCall
}

type platformCall struct {
	id                 string
	checkpoint         uint64
	observedCheckpoint uint64
	started            time.Time
	before             runnerResult
	paused             bool
}

type pendingResponseLoss struct {
	batchID      string
	before       runnerResult
	observations []transportObservation
	started      time.Time
}

type operationWindow struct {
	observations              []transportObservation
	duration                  time.Duration
	provenanceMaintenanceWork uint64
	replayedMutationCount     uint64
}

// NewPlatform creates one direct macOS Swift platform.
func NewPlatform(config Config) (*Platform, error) {
	normalized, err := normalizePlatformConfig(config)
	if err != nil {
		return nil, err
	}
	platform := &Platform{config: normalized, clients: make(map[string]*platformClient), rebuildResponseCursors: make(map[string]string)}
	if err := platform.startResponseProxy(); err != nil {
		return nil, err
	}
	return platform, nil
}

func (p *Platform) startResponseProxy() error {
	upstream, err := url.Parse(p.config.ServerURL)
	if err != nil {
		return errors.New("Swift response proxy upstream is invalid")
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.ModifyResponse = p.modifyProxiedResponse
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if p.serveTemporaryUnavailablePush(response, request) {
			return
		}
		if strings.HasSuffix(request.URL.Path, "/sync/rebuild") && request.Body != nil {
			body, readErr := io.ReadAll(io.LimitReader(request.Body, maximumMutatedResponseBytes+1))
			request.Body.Close()
			if readErr != nil || len(body) > maximumMutatedResponseBytes {
				http.Error(response, "bounded rebuild request required", http.StatusBadRequest)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			var value struct {
				ClientID string `json:"client_id"`
			}
			if json.Unmarshal(body, &value) == nil && value.ClientID != "" {
				request = request.WithContext(context.WithValue(request.Context(), rebuildProxyClientIDKey{}, value.ClientID))
			}
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
		p.recordTemporaryUnavailableMiss("unreadable push target: " + err.Error())
		return false
	}
	if !p.claimTemporaryUnavailablePush(target) {
		p.recordTemporaryUnavailableMiss("no armed fault for client " + target.ClientID)
		return false
	}
	injected := faults.NewTemporaryUnavailableResponse(request)
	defer injected.Body.Close()
	copyInjectedResponse(response, injected)
	return true
}

func proxiedPushTarget(request *http.Request) (scenarios.PushWireFaultTarget, error) {
	if request.Body == nil {
		return scenarios.PushWireFaultTarget{}, errors.New("Swift proxied push body is absent")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumProxiedPushRequestBytes+1))
	request.Body.Close()
	if err != nil || len(body) > maximumProxiedPushRequestBytes {
		return scenarios.PushWireFaultTarget{}, errors.New("Swift proxied push body is invalid")
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
		return scenarios.PushWireFaultTarget{}, errors.New("Swift proxied push target is invalid")
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
	// The authored batch identity is an alias. The client mints its own batch
	// identity, so the armed identity never equals the observed one. The fault
	// is armed for one client across one call, so the client identity selects
	// the intended push.
	if armed == nil || armed.ClientID != target.ClientID {
		return false
	}
	p.temporaryUnavailablePush = nil
	return true
}

func (p *Platform) armTemporaryUnavailablePush(operations RequestOperations) (func(), bool, error) {
	target, enabled, err := temporaryUnavailablePushTargetForOperations(operations)
	if err != nil || !enabled {
		return nil, enabled, err
	}
	p.mu.Lock()
	if p.closed || p.temporaryUnavailablePush != nil {
		p.mu.Unlock()
		return nil, false, errors.New("Swift temporary-unavailable push fault is unavailable")
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

func temporaryUnavailablePushTargetForOperations(operations RequestOperations) (scenarios.PushWireFaultTarget, bool, error) {
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
			return scenarios.PushWireFaultTarget{}, false, errors.New("Swift synchronization has multiple temporary-unavailable push faults")
		}
		target = candidate
	}
	return target, target.ClientID != "", nil
}

func (p *Platform) modifyProxiedResponse(response *http.Response) error {
	if response.StatusCode != http.StatusOK || !strings.HasSuffix(response.Request.URL.Path, "/sync/rebuild") {
		return nil
	}
	p.mu.Lock()
	override := p.rebuildCursorOverride
	clientID, _ := response.Request.Context().Value(rebuildProxyClientIDKey{}).(string)
	if override != "" && clientID == p.rebuildCursorOverrideClientID {
		p.rebuildCursorOverride = ""
		p.rebuildCursorOverrideClientID = ""
	} else {
		override = ""
	}
	p.mu.Unlock()
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumMutatedResponseBytes+1))
	response.Body.Close()
	if err != nil || len(body) > maximumMutatedResponseBytes {
		return errors.New("read Swift rebuild response failed")
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(body, &value); err != nil {
		return errors.New("decode Swift rebuild response failed")
	}
	var cursor string
	if raw, found := value["cursor"]; found && string(raw) != "null" {
		if err := json.Unmarshal(raw, &cursor); err != nil || cursor == "" {
			return errors.New("Swift rebuild response cursor is invalid")
		}
	}
	proxied := body
	if override != "" {
		if cursor == "" {
			return errors.New("Swift rebuild cursor mutation target is invalid")
		}
		cursor = override
		value["cursor"], err = json.Marshal(override)
		if err != nil {
			return errors.New("encode Swift rebuild cursor mutation failed")
		}
		proxied, err = json.Marshal(value)
		if err != nil {
			return errors.New("encode Swift rebuild response mutation failed")
		}
	}
	if cursor != "" && clientID != "" {
		p.mu.Lock()
		p.rebuildResponseCursors[clientID] = cursorFingerprint(cursor)
		p.mu.Unlock()
	}
	response.Body = io.NopCloser(bytes.NewReader(proxied))
	response.ContentLength = int64(len(proxied))
	response.Header.Set("Content-Length", strconv.Itoa(len(proxied)))
	return nil
}

func (p *Platform) armRebuildCursorOverride(clientID, cursor string) error {
	if clientID == "" || cursor == "" || len(cursor) > 4096 {
		return errors.New("Swift rebuild cursor override is invalid")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.rebuildCursorOverride != "" {
		return errors.New("Swift rebuild cursor override is unavailable")
	}
	p.rebuildCursorOverride = cursor
	p.rebuildCursorOverrideClientID = clientID
	return nil
}

func (p *Platform) clearRebuildCursorOverride() {
	p.mu.Lock()
	p.rebuildCursorOverride = ""
	p.rebuildCursorOverrideClientID = ""
	p.mu.Unlock()
}

func normalizePlatformConfig(config Config) (Config, error) {
	if config.RunnerPath == "" || config.ApplicationDatabaseDirectory == "" || config.ServerURL == "" || config.AuthToken == nil || config.Platform == "" || config.AppVersion == "" {
		return Config{}, errors.New("Swift platform configuration is incomplete")
	}
	if config.Platform != "macos" || len(config.AppVersion) > 128 {
		return Config{}, errors.New("Swift platform supports only current macOS")
	}
	if config.PullPageSize < 0 || config.PullPageSize > 1000 {
		return Config{}, errors.New("Swift platform pull page size is invalid")
	}
	if config.PushBatchSize == 0 {
		config.PushBatchSize = 100
	}
	if config.PushBatchSize < 1 || config.PushBatchSize > 1000 {
		return Config{}, errors.New("Swift platform push batch size is invalid")
	}
	parsedURL, err := url.Parse(config.ServerURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return Config{}, errors.New("Swift platform server URL is invalid")
	}
	config, err = normalizeConfig(config)
	if err != nil {
		return Config{}, err
	}
	directory, err := prepareApplicationDatabaseDirectory(config.ApplicationDatabaseDirectory)
	if err != nil {
		return Config{}, err
	}
	config.ApplicationDatabaseDirectory = directory
	return config, nil
}

func prepareApplicationDatabaseDirectory(path string) (string, error) {
	directory, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("Swift platform database directory is invalid")
	}
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return "", errors.New("create Swift application database directory failed")
		}
		info, err = os.Lstat(directory)
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("Swift application database directory is not private")
	}
	return filepath.Clean(directory), nil
}

// Install starts one empty, current, or seeded durable client database.
func (p *Platform) Install(ctx context.Context, client Client, initialization, seedPath string) error {
	if err := p.context(ctx); err != nil {
		return err
	}
	if err := validateClient(client); err != nil {
		return err
	}
	seedPath, err := validateInstallation(initialization, seedPath)
	if err != nil {
		return err
	}

	p.mu.Lock()
	if _, found := p.clients[client.Key]; found {
		p.mu.Unlock()
		return errors.New("Swift platform client is already installed")
	}
	p.mu.Unlock()

	databasePath := p.databasePath(client.DatabaseKey)
	if err := requireAbsentDatabaseFamily(databasePath); err != nil {
		return err
	}
	state := &platformClient{
		client:       client,
		databasePath: databasePath,
		selectors:    make(map[string]runnerRowSelector),
	}
	if err := p.startClient(ctx, state, seedPath); err != nil {
		return err
	}
	if initialization == "current" {
		if err := p.initializeCurrent(ctx, state); err != nil {
			closeSession(state.session)
			return err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		closeSession(state.session)
		return errors.New("Swift platform is closed")
	}
	if _, found := p.clients[client.Key]; found {
		closeSession(state.session)
		return errors.New("Swift platform client is already installed")
	}
	p.clients[client.Key] = state
	return nil
}

func validateInstallation(initialization, seedPath string) (string, error) {
	switch initialization {
	case "empty", "current":
		if seedPath != "" {
			return "", errors.New("Swift non-seed initialization has a seed path")
		}
		return "", nil
	case "seed":
		if seedPath == "" {
			return "", errors.New("Swift seed initialization has no seed path")
		}
		return requireSeedPath(seedPath)
	default:
		return "", errors.New("Swift client initialization is unsupported")
	}
}

func validateClient(client Client) error {
	if client.Key == "" || client.UserID == "" || client.ClientID == "" || client.DatabaseKey == "" {
		return errors.New("Swift client identity is incomplete")
	}
	for _, value := range []string{client.Key, client.UserID, client.ClientID, client.DatabaseKey} {
		if len(value) > 256 || strings.ContainsAny(value, "\x00\r\n") {
			return errors.New("Swift client identity is invalid")
		}
	}
	return nil
}

func requireSeedPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("Swift seed path is invalid")
	}
	info, err := os.Lstat(absolute)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("Swift production seed is unavailable")
	}
	return filepath.Clean(absolute), nil
}

func (p *Platform) databasePath(databaseKey string) string {
	digest := sha256.Sum256([]byte(databaseKey))
	return filepath.Join(p.config.ApplicationDatabaseDirectory, hex.EncodeToString(digest[:])+".sqlite")
}

func requireAbsentDatabaseFamily(path string) error {
	for _, candidate := range []string{path, path + "-journal", path + "-wal", path + "-shm"} {
		if _, err := os.Lstat(candidate); err == nil {
			return errors.New("Swift application database already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("inspect Swift application database failed")
		}
	}
	return nil
}

func requireExistingDatabase(path string) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("Swift application database is unavailable")
	}
	return nil
}

func (p *Platform) startClient(ctx context.Context, state *platformClient, seedPath string) error {
	session, err := StartSession(ctx, Config{RunnerPath: p.config.RunnerPath})
	if err != nil {
		return err
	}
	token, err := p.config.AuthToken(ctx, state.client)
	if err != nil || token == "" || len(token) > 4096 {
		closeSession(session)
		return errors.New("resolve Swift client authentication failed")
	}
	result, err := session.Execute(ctx, Request{
		Operation:        "open",
		DatabasePath:     state.databasePath,
		ServerURL:        p.config.ServerURL,
		AuthToken:        token,
		ClientID:         state.client.ClientID,
		SeedDatabasePath: seedPath,
		Platform:         p.config.Platform,
		AppVersion:       p.config.AppVersion,
		PullPageSize:     p.config.PullPageSize,
		PushBatchSize:    p.config.PushBatchSize,
	})
	if err != nil {
		closeSession(session)
		return fmt.Errorf("open Swift runner client: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		closeSession(session)
		return errors.New("Swift runner open did not return status")
	}
	state.session = session
	state.terminated = false
	state.started = false
	return nil
}

func (p *Platform) initializeCurrent(ctx context.Context, state *platformClient) error {
	completed, observations, err := runCallToCompletion(ctx, state, "install_current", "start")
	if err != nil {
		return fmt.Errorf("initialize current Swift database: %w", err)
	}
	if completed.Completion != "idle" {
		outcomes := make([]string, 0, len(observations))
		for _, observation := range observations {
			outcome := fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode)
			if observation.ErrorCode != nil {
				outcome += ":" + *observation.ErrorCode
			}
			if facts := observation.RequestFacts; facts != nil {
				generation := "none"
				if facts.ClientGeneration != nil {
					generation = strconv.FormatInt(*facts.ClientGeneration, 10)
				}
				scopeSetVersion := "none"
				if facts.ScopeSetVersion != nil {
					scopeSetVersion = strconv.FormatInt(*facts.ScopeSetVersion, 10)
				}
				scopeCount := "none"
				if facts.ScopeCount != nil {
					scopeCount = strconv.Itoa(*facts.ScopeCount)
				}
				outcome += fmt.Sprintf(":generation=%s:schema=%d:scope-set=%s:scopes=%s", generation, facts.SchemaVersion, scopeSetVersion, scopeCount)
			}
			outcomes = append(outcomes, outcome)
		}
		return fmt.Errorf("current Swift database initialization reached %q after %v", completed.Completion, outcomes)
	}
	result, err := state.session.Execute(ctx, Request{Operation: "lifecycle", LifecycleOperation: "stop"})
	if err != nil {
		return fmt.Errorf("stop current Swift database initialization: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		return errors.New("Swift runner stop did not return status")
	}
	state.started = false
	return nil
}

func closeSession(session *Session) {
	if session == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = session.Close(ctx)
}

// ApplyStep executes one authored application write through public client SQL.
func (p *Platform) ApplyStep(ctx context.Context, client Client, operation scenarios.Operation) (StepObservation, error) {
	if err := p.context(ctx); err != nil {
		return StepObservation{}, err
	}
	if scenarios.OperationKey(operation) != "local/write" {
		return StepObservation{}, fmt.Errorf("Swift apply operation %q is unsupported", scenarios.OperationKey(operation))
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return StepObservation{}, fmt.Errorf("Swift apply operation is invalid: %w", err)
	}
	state, err := p.client(client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.terminated || state.session == nil || state.pendingLoss != nil {
		return StepObservation{}, errors.New("Swift client is unavailable for a local write")
	}
	action, selector, err := decodeLocalWrite(operation, client)
	if err != nil {
		return StepObservation{}, err
	}
	started := time.Now()
	result, err := state.session.Execute(ctx, Request{Operation: "local-action", LocalAction: &action})
	if err != nil {
		inspections := []runnerRowSelector{selector}
		fieldNames := make([]string, 0, len(action.Fields))
		for field := range action.Fields {
			fieldNames = append(fieldNames, field)
		}
		sort.Strings(fieldNames)
		for _, field := range fieldNames {
			inspection := selector
			inspection.PrimaryKeyField = field
			inspections = append(inspections, inspection)
		}
		for _, inspection := range inspections {
			_, inspectionErr := state.session.Execute(ctx, Request{Operation: "capture", RowSelectors: []runnerRowSelector{inspection}})
			if inspectionErr != nil {
				return StepObservation{}, fmt.Errorf("execute Swift local action on %s.%s: %w (application field %s inspection: %v)", action.TableName, action.PrimaryKeyField, err, inspection.PrimaryKeyField, inspectionErr)
			}
		}
		return StepObservation{}, fmt.Errorf("execute Swift local action with existing application fields on %s: %w", action.TableName, err)
	}
	if result.RowsAffected == nil || *result.RowsAffected != 1 {
		return StepObservation{}, errors.New("Swift local action did not affect one row")
	}
	state.selectors[selectorKey(selector)] = selector
	return StepObservation{Disposition: "success", DurationNanoseconds: uint64(time.Since(started))}, nil
}

// RequestStep executes one public synchronization and reports its matching request.
func (p *Platform) RequestStep(ctx context.Context, client Client, operation scenarios.Operation) (StepObservation, error) {
	if err := p.context(ctx); err != nil {
		return StepObservation{}, err
	}
	state, err := p.client(client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	method := "sync-now"
	if !state.started {
		method = "start"
	}
	result, err := p.synchronizeLocked(ctx, state, method, RequestOperations{operation})
	if err != nil {
		return StepObservation{}, err
	}
	observation := result.Steps[0]
	observation.Completion = result.Completion
	observation.DurationNanoseconds = result.DurationNanoseconds
	observation.ProvenanceMaintenanceWork = result.ProvenanceMaintenanceWork
	observation.ReplayedMutationCount = result.ReplayedMutationCount
	return observation, nil
}

// Synchronize executes one grouped public call to completion.
func (p *Platform) Synchronize(ctx context.Context, client Client, method string, operations RequestOperations) (SynchronizationResult, error) {
	if err := p.context(ctx); err != nil {
		return SynchronizationResult{}, err
	}
	state, err := p.client(client)
	if err != nil {
		return SynchronizationResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return p.synchronizeLocked(ctx, state, method, operations)
}

func (p *Platform) synchronizeLocked(ctx context.Context, state *platformClient, method string, operations RequestOperations) (SynchronizationResult, error) {
	if state.terminated || state.session == nil || state.pendingLoss != nil || state.activeCall != nil {
		return SynchronizationResult{}, errors.New("Swift client is unavailable for synchronization")
	}
	dropBatchID, err := validateRequestOperations(operations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if !validRunnerMethod(method) {
		return SynchronizationResult{}, errors.New("Swift synchronization method is invalid")
	}
	releaseFault, faultArmed, err := p.armTemporaryUnavailablePush(operations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if faultArmed {
		defer releaseFault()
	}
	if dropBatchID != "" {
		if faultArmed {
			return SynchronizationResult{}, errors.New("Swift response loss cannot combine with a temporary-unavailable push fault")
		}
		_, dropLast, _, _ := requestDispatch(operations[len(operations)-1])
		if !dropLast {
			return SynchronizationResult{}, errors.New("Swift response-loss request must end its public call")
		}
		return p.synchronizeWithResponseLoss(ctx, state, method, operations, dropBatchID)
	}

	before, err := captureRunner(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	started := time.Now()
	completed, observations, err := runCallToCompletion(ctx, state, p.nextCallID(state), method)
	if err != nil {
		return SynchronizationResult{}, err
	}
	after, err := captureRunner(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	mapped, err := mapTransportOperations(operations, observations, before)
	if err != nil {
		if after.RebuildAttemptCount != nil && after.RebuildReceiptCount != nil && after.ScopeStateCount != nil {
			return SynchronizationResult{}, fmt.Errorf("%w; scope count = %d, rebuild attempt count = %d, receipt count = %d", err, *after.ScopeStateCount, *after.RebuildAttemptCount, *after.RebuildReceiptCount)
		}
		return SynchronizationResult{}, err
	}
	window, err := windowFromResults(started, before, after, observations)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if state.restarted {
		window.replayedMutationCount = pushMutationCount(window.observations)
		state.restarted = false
	}
	state.started = true
	return synchronizationResult(completed.Completion, mapped, window), nil
}

func validateRequestOperations(operations RequestOperations) (string, error) {
	if len(operations) == 0 {
		return "", errors.New("Swift synchronization has no covered requests")
	}
	var dropBatchID string
	for _, operation := range operations {
		if err := scenarios.ValidateOperation(operation); err != nil {
			return "", fmt.Errorf("Swift request operation is invalid: %w", err)
		}
		_, drop, batchID, err := requestDispatch(operation)
		if err != nil {
			return "", err
		}
		if drop {
			if dropBatchID != "" {
				return "", errors.New("Swift synchronization has multiple response-loss requests")
			}
			dropBatchID = batchID
		}
	}
	return dropBatchID, nil
}

func (p *Platform) synchronizeWithResponseLoss(ctx context.Context, state *platformClient, method string, operations RequestOperations, batchID string) (SynchronizationResult, error) {
	before, err := captureRunner(ctx, state)
	if err != nil {
		return SynchronizationResult{}, err
	}
	initialCheckpoint := state.session.Checkpoint()
	started := time.Now()
	callID := p.nextCallID(state)
	operationClass, _, _, _ := requestDispatch(operations[0])
	if !state.started {
		if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: "connect"}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("arm Swift response-loss connect: %w", err)
		}
	} else if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return SynchronizationResult{}, fmt.Errorf("arm Swift response-loss transport: %w", err)
	}
	begin, err := state.session.Execute(ctx, Request{Operation: "begin-call", CallID: callID, Method: method})
	if err != nil {
		return SynchronizationResult{}, fmt.Errorf("start Swift public call: %w", err)
	}
	inFlight, err := runnerClientCallResult(begin)
	if err != nil || inFlight.CallID != callID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return SynchronizationResult{}, errors.New("Swift public call did not enter flight")
	}
	if !state.started {
		if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: "connect"}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("await Swift response-loss connect: %w", err)
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("arm Swift response-loss transport: %w", err)
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("resume Swift response-loss connect: %w", err)
		}
	}
	if err := waitForTransportObservation(ctx, state, initialCheckpoint, operationClass); err != nil {
		return SynchronizationResult{}, err
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
		after, captureErr := captureRunner(ctx, state)
		if captureErr == nil && after.Failure != nil {
			return SynchronizationResult{}, fmt.Errorf("await Swift transport pause: %w; operation = %s, code = %s, recovery = %s", err, after.Failure.Operation, after.Failure.Code, after.Failure.RecoveryAction)
		}
		if captureErr != nil {
			return SynchronizationResult{}, fmt.Errorf("await Swift transport pause: %w; diagnostic capture: %v", err, captureErr)
		}
		return SynchronizationResult{}, fmt.Errorf("await Swift transport pause: %w", err)
	}
	observations, err := state.session.ObservationsAfter(initialCheckpoint)
	if err != nil {
		return SynchronizationResult{}, err
	}
	if len(observations) == 0 || observations[len(observations)-1].OperationClass != operationClass {
		return SynchronizationResult{}, errors.New("Swift response-loss transport observation is not the covered request")
	}
	mapped, err := mapTransportOperations(operations[:1], observations[len(observations)-1:], before)
	if err != nil {
		return SynchronizationResult{}, err
	}
	for index := 1; index < len(operations); index++ {
		checkpoint := state.session.Checkpoint()
		operationClass, _, _, _ = requestDispatch(operations[index])
		if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("arm next Swift transport pause: %w", err)
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("resume Swift transport pause: %w", err)
		}
		if err := waitForTransportObservation(ctx, state, checkpoint, operationClass); err != nil {
			return SynchronizationResult{}, err
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
			return SynchronizationResult{}, fmt.Errorf("await next Swift transport pause: %w", err)
		}
		stepObservations, err := state.session.ObservationsAfter(checkpoint)
		if err != nil {
			return SynchronizationResult{}, err
		}
		source, err := captureRunner(ctx, state)
		if err != nil {
			return SynchronizationResult{}, err
		}
		step, err := mapTransportOperations(operations[index:index+1], stepObservations, source)
		if err != nil {
			return SynchronizationResult{}, err
		}
		mapped = append(mapped, step[0])
	}
	observations, err = state.session.ObservationsAfter(initialCheckpoint)
	if err != nil {
		return SynchronizationResult{}, err
	}
	last := observations[len(observations)-1]
	if last.StatusCode < 200 || last.StatusCode >= 300 {
		return SynchronizationResult{}, errors.New("Swift response loss requires a committed server response")
	}
	if method == "reset-schema-and-start" {
		state.selectors = make(map[string]runnerRowSelector)
	}
	if err := state.session.Kill(ctx); err != nil {
		return SynchronizationResult{}, fmt.Errorf("terminate Swift runner after server response: %w", err)
	}
	closeSession(state.session)
	state.session = nil
	state.terminated = true
	state.started = false
	state.pendingLoss = &pendingResponseLoss{
		batchID:      batchID,
		before:       before,
		observations: cloneTransportObservations(observations),
		started:      started,
	}
	window := operationWindow{observations: cloneTransportObservations(observations), duration: time.Since(started)}
	return synchronizationResult("blocked", mapped, window), nil
}

func waitForTransportObservation(ctx context.Context, state *platformClient, checkpoint uint64, operationClass string) error {
	for {
		if _, err := captureRunnerBatch(ctx, state, nil); err != nil {
			return fmt.Errorf("poll Swift response-loss transport: %w", err)
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
			return fmt.Errorf("wait for Swift response-loss transport: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func runCallToCompletion(ctx context.Context, state *platformClient, callID, method string) (callResult, []transportObservation, error) {
	if !validRunnerCallID(callID) || !validRunnerMethod(method) {
		return callResult{}, nil, errors.New("Swift public call is invalid")
	}
	checkpoint := state.session.Checkpoint()
	begin, err := state.session.Execute(ctx, Request{Operation: "begin-call", CallID: callID, Method: method})
	if err != nil {
		return callResult{}, nil, fmt.Errorf("start Swift public call: %w", err)
	}
	inFlight, err := runnerClientCallResult(begin)
	if err != nil || inFlight.CallID != callID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return callResult{}, nil, errors.New("Swift public call did not enter flight")
	}
	result, err := state.session.Execute(ctx, Request{Operation: "await-call", CallID: callID})
	if err != nil {
		return callResult{}, nil, fmt.Errorf("await Swift public call: %w", err)
	}
	completed, err := runnerClientCallResult(result)
	if err != nil || completed.CallID != callID || completed.State != "completed" || !validCompletion(completed.Completion) {
		return callResult{}, nil, errors.New("Swift public call did not complete")
	}
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return callResult{}, nil, err
	}
	return *completed, observations, nil
}

func (p *Platform) nextCallID(state *platformClient) string {
	state.callSequence++
	return "swift_call_" + strconv.FormatUint(state.callSequence, 10)
}

func mapTransportOperations(operations RequestOperations, observations []transportObservation, source runnerResult) ([]StepObservation, error) {
	if len(operations) != len(observations) {
		classes := make([]string, len(observations))
		for index, observation := range observations {
			classes[index] = observation.OperationClass
		}
		return nil, fmt.Errorf("Swift transport observations %v do not close %d covered requests", classes, len(operations))
	}
	mapped := make([]StepObservation, len(operations))
	withinCallCheckpoints := make(map[string]string)
	for index := range operations {
		operationClass, _, _, err := requestDispatch(operations[index])
		if err != nil || observations[index].OperationClass != operationClass {
			return nil, fmt.Errorf("Swift transport observation %d is %q, covered request is %q", index, observations[index].OperationClass, operationClass)
		}
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
				return nil, errors.New("decode Swift authored rebuild scope failed")
			}
			if _, exists := withinCallCheckpoints[payload.ScopeID]; exists {
				return nil, errors.New("Swift public call produced multiple terminal rebuild cursors for one scope")
			}
			withinCallCheckpoints[payload.ScopeID] = *observations[index].RebuildResponseFacts.FinalScopeCursorFingerprint
		}
		mapped[index], err = transportStepObservation(observations[index])
		if err != nil {
			return nil, err
		}
	}
	return mapped, nil
}

func synchronizationResult(completion string, steps []StepObservation, window operationWindow) SynchronizationResult {
	result := SynchronizationResult{
		Completion:                completion,
		Steps:                     steps,
		ProvenanceMaintenanceWork: window.provenanceMaintenanceWork,
		ReplayedMutationCount:     window.replayedMutationCount,
		transportObservations:     cloneTransportObservations(window.observations),
	}
	if window.duration > 0 {
		result.DurationNanoseconds = uint64(window.duration)
	}
	return result
}

// BeginCall starts one public call and pauses after its first upstream response.
func (p *Platform) BeginCall(ctx context.Context, client Client, callID, method string, operations RequestOperations) (CallResult, error) {
	if err := p.context(ctx); err != nil {
		return CallResult{}, err
	}
	if len(operations) != 1 {
		return CallResult{}, errors.New("Swift paused call requires one first covered request")
	}
	if _, err := validateRequestOperations(operations); err != nil {
		return CallResult{}, err
	}
	if _, enabled, err := temporaryUnavailablePushTargetForOperations(operations); err != nil {
		return CallResult{}, err
	} else if enabled {
		return CallResult{}, errors.New("Swift temporary-unavailable push fault requires synchronous synchronization")
	}
	if !validRunnerCallID(callID) || !validRunnerMethod(method) {
		return CallResult{}, errors.New("Swift begin-call request is invalid")
	}
	operationClass, drop, _, _ := requestDispatch(operations[0])
	if drop {
		return CallResult{}, errors.New("Swift response loss requires grouped synchronization")
	}
	state, err := p.client(client)
	if err != nil {
		return CallResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.terminated || state.session == nil || state.pendingLoss != nil || state.activeCall != nil {
		return CallResult{}, errors.New("Swift client is unavailable for begin-call")
	}
	before, err := captureRunner(ctx, state)
	if err != nil {
		return CallResult{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	pauseAfterConnect := !state.started && operationClass != "connect"
	firstPauseClass := operationClass
	if pauseAfterConnect {
		firstPauseClass = "connect"
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: firstPauseClass}); err != nil {
		return CallResult{}, fmt.Errorf("arm Swift transport pause: %w", err)
	}
	begin, err := state.session.Execute(ctx, Request{Operation: "begin-call", CallID: callID, Method: method})
	if err != nil {
		return CallResult{}, fmt.Errorf("start paused Swift call: %w", err)
	}
	inFlight, err := runnerClientCallResult(begin)
	if err != nil || inFlight.CallID != callID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return CallResult{}, errors.New("Swift paused call did not enter flight")
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: firstPauseClass}); err != nil {
		return CallResult{}, fmt.Errorf("await Swift transport pause: %w", err)
	}
	if pauseAfterConnect {
		connect, err := state.session.ObservationsAfter(checkpoint)
		if err != nil {
			return CallResult{}, err
		}
		if len(connect) != 1 || connect[0].OperationClass != "connect" || connect[0].StatusCode != http.StatusOK || connect[0].ErrorCode != nil || connect[0].Retryable {
			return CallResult{}, errors.New("Swift staged call setup connect did not succeed")
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
			return CallResult{}, fmt.Errorf("arm covered Swift transport pause: %w", err)
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
			return CallResult{}, fmt.Errorf("resume Swift staged call setup connect: %w", err)
		}
		if err := waitForTransportObservation(ctx, state, checkpoint, operationClass); err != nil {
			return CallResult{}, err
		}
		if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
			return CallResult{}, fmt.Errorf("await covered Swift transport pause: %w", err)
		}
	}
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return CallResult{}, err
	}
	covered := observations
	if pauseAfterConnect {
		if len(observations) != 2 {
			return CallResult{}, errors.New("Swift staged call setup produced unexpected transport")
		}
		covered = observations[1:]
	}
	mapped, err := mapTransportOperations(operations, covered, before)
	if err != nil {
		return CallResult{}, err
	}
	state.activeCall = &platformCall{
		id:                 callID,
		checkpoint:         checkpoint,
		observedCheckpoint: state.session.Checkpoint(),
		started:            started,
		before:             before,
		paused:             true,
	}
	return CallResult{CallID: callID, State: "in_flight", Steps: mapped}, nil
}

// AwaitStep resumes one paused call and pauses after its next upstream response.
func (p *Platform) AwaitStep(ctx context.Context, client Client, callID string, operation scenarios.Operation) (StepObservation, error) {
	if err := p.context(ctx); err != nil {
		return StepObservation{}, err
	}
	if _, err := validateRequestOperations(RequestOperations{operation}); err != nil {
		return StepObservation{}, err
	}
	if _, enabled, err := temporaryUnavailablePushTargetForOperations(RequestOperations{operation}); err != nil {
		return StepObservation{}, err
	} else if enabled {
		return StepObservation{}, errors.New("Swift temporary-unavailable push fault requires synchronous synchronization")
	}
	operationClass, drop, _, _ := requestDispatch(operation)
	if drop {
		return StepObservation{}, errors.New("Swift response loss requires grouped synchronization")
	}
	state, err := p.client(client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	active := state.activeCall
	if state.terminated || state.session == nil || active == nil || active.id != callID || !active.paused {
		return StepObservation{}, errors.New("Swift await-step has no matching paused call")
	}
	checkpoint := state.session.Checkpoint()
	var rebuildCursorSource, expectedRebuildCursor string
	if operationClass == "rebuild" {
		var payload struct {
			CursorSource string `json:"cursor_source"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil {
			return StepObservation{}, errors.New("decode paused Swift rebuild cursor source failed")
		}
		rebuildCursorSource = payload.CursorSource
		if rebuildCursorSource == "local_rebuild_continuation" {
			p.mu.Lock()
			expectedRebuildCursor = p.rebuildResponseCursors[client.ClientID]
			p.mu.Unlock()
			if expectedRebuildCursor == "" {
				return StepObservation{}, errors.New("Swift paused rebuild continuation has no preceding response cursor")
			}
		}
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return StepObservation{}, fmt.Errorf("arm next Swift transport pause: %w", err)
	}
	if operationUsesForgedRebuildCursor(operation) {
		if err := p.armRebuildCursorOverride(client.ClientID, forgedRebuildCursor); err != nil {
			return StepObservation{}, fmt.Errorf("override paused Swift rebuild cursor: %w", err)
		}
		defer p.clearRebuildCursorOverride()
	}
	if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
		return StepObservation{}, fmt.Errorf("resume Swift transport pause: %w", err)
	}
	active.paused = false
	if _, err := state.session.Execute(ctx, Request{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
		return StepObservation{}, fmt.Errorf("await next Swift transport pause: %w", err)
	}
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return StepObservation{}, err
	}
	var mapped []StepObservation
	if operationClass == "rebuild" && rebuildCursorSource == "local_rebuild_continuation" {
		if len(observations) != 1 || observations[0].RequestFacts == nil || observations[0].RequestFacts.CursorFingerprint == nil || *observations[0].RequestFacts.CursorFingerprint != expectedRebuildCursor {
			return StepObservation{}, errors.New("Swift paused rebuild request did not use the preceding response cursor")
		}
		if err := validateOperationTransportFacts(operation, observations[0]); err != nil {
			return StepObservation{}, err
		}
		observation, err := transportStepObservation(observations[0])
		if err != nil {
			return StepObservation{}, err
		}
		mapped = []StepObservation{observation}
		p.mu.Lock()
		if p.rebuildResponseCursors[client.ClientID] == expectedRebuildCursor {
			delete(p.rebuildResponseCursors, client.ClientID)
		}
		p.mu.Unlock()
	} else {
		source := runnerResult{}
		if operationClass != "rebuild" {
			source, err = captureRunner(ctx, state)
			if err != nil {
				return StepObservation{}, err
			}
		}
		mapped, err = mapTransportOperations(RequestOperations{operation}, observations, source)
		if err != nil {
			return StepObservation{}, err
		}
	}
	active.observedCheckpoint = state.session.Checkpoint()
	active.paused = true
	return mapped[0], nil
}

// AwaitCall resumes the final pause and waits for call completion.
func (p *Platform) AwaitCall(ctx context.Context, client Client, callID string) (CallResult, error) {
	if err := p.context(ctx); err != nil {
		return CallResult{}, err
	}
	state, err := p.client(client)
	if err != nil {
		return CallResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	active := state.activeCall
	if state.terminated || state.session == nil || active == nil || active.id != callID {
		return CallResult{}, errors.New("Swift await-call has no matching active call")
	}
	if active.paused {
		if _, err := state.session.Execute(ctx, Request{Operation: "resume-transport-pause"}); err != nil {
			return CallResult{}, fmt.Errorf("resume final Swift transport pause: %w", err)
		}
		active.paused = false
	}
	result, err := state.session.Execute(ctx, Request{Operation: "await-call", CallID: callID})
	if err != nil {
		return CallResult{}, fmt.Errorf("await paused Swift call: %w", err)
	}
	completed, err := runnerClientCallResult(result)
	if err != nil || completed.CallID != callID || completed.State != "completed" || !validCompletion(completed.Completion) {
		return CallResult{}, errors.New("Swift paused call did not complete")
	}
	uncovered, err := state.session.ObservationsAfter(active.observedCheckpoint)
	if err != nil {
		return CallResult{}, err
	}
	if len(uncovered) != 0 {
		return CallResult{}, errors.New("Swift paused call produced an uncovered transport request")
	}
	observations, err := state.session.ObservationsAfter(active.checkpoint)
	if err != nil {
		return CallResult{}, err
	}
	var window operationWindow
	if completed.Completion == "error" {
		window = operationWindow{observations: observations, duration: time.Since(active.started)}
	} else {
		after, err := captureRunner(ctx, state)
		if err != nil {
			return CallResult{}, err
		}
		window, err = windowFromResults(active.started, active.before, after, observations)
		if err != nil {
			return CallResult{}, err
		}
	}
	if state.restarted {
		window.replayedMutationCount = pushMutationCount(window.observations)
		state.restarted = false
	}
	state.activeCall = nil
	state.started = true
	return callResultWithWindow(*completed, window), nil
}

func callResultWithWindow(completed callResult, window operationWindow) CallResult {
	result := CallResult{
		CallID:                    completed.CallID,
		State:                     completed.State,
		Completion:                completed.Completion,
		ProvenanceMaintenanceWork: window.provenanceMaintenanceWork,
		ReplayedMutationCount:     window.replayedMutationCount,
	}
	if window.duration > 0 {
		result.DurationNanoseconds = uint64(window.duration)
	}
	return result
}

// Lifecycle invokes one public client lifecycle operation.
func (p *Platform) Lifecycle(ctx context.Context, client Client, operation string) (StepObservation, error) {
	if err := p.context(ctx); err != nil {
		return StepObservation{}, err
	}
	if !validRunnerLifecycle(operation) {
		return StepObservation{}, errors.New("Swift lifecycle operation is unsupported")
	}
	state, err := p.client(client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.terminated || state.session == nil || state.pendingLoss != nil || state.activeCall != nil {
		return StepObservation{}, errors.New("Swift client is unavailable for lifecycle operation")
	}
	before, err := captureRunner(ctx, state)
	if err != nil {
		return StepObservation{}, err
	}
	checkpoint := state.session.Checkpoint()
	started := time.Now()
	result, err := state.session.Execute(ctx, Request{Operation: "lifecycle", LifecycleOperation: operation})
	if err != nil {
		return StepObservation{}, fmt.Errorf("run Swift lifecycle operation: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		return StepObservation{}, errors.New("Swift lifecycle operation did not return status")
	}
	after, err := captureRunner(ctx, state)
	if err != nil {
		return StepObservation{}, err
	}
	window, err := p.completeWindow(state, checkpoint, started, before, after)
	if err != nil {
		return StepObservation{}, err
	}
	if operation == "stop" {
		state.started = false
	}
	return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
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
			AuthenticatedUserID string `json:"authenticated_user_id"`
			Request             struct {
				ClientID         string            `json:"client_id"`
				ClientGeneration int64             `json:"client_generation"`
				BatchID          string            `json:"batch_id"`
				Schema           schemaRef         `json:"schema"`
				Mutations        []json.RawMessage `json:"mutations"`
			} `json:"request"`
			Delivery  string `json:"delivery"`
			CommitLSN string `json:"commit_lsn"`
			EndLSN    string `json:"end_lsn"`
		}
		if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.Request.BatchID == "" {
			return "", false, "", errors.New("decode Swift push request failed")
		}
		switch payload.Delivery {
		case "apply":
			return "push", false, payload.Request.BatchID, nil
		case "drop_after_server":
			return "push", true, payload.Request.BatchID, nil
		case "transport_failure":
			return "push", false, payload.Request.BatchID, nil
		default:
			return "", false, "", errors.New("Swift push delivery is unsupported")
		}
	default:
		return "", false, "", fmt.Errorf("Swift request operation %q is unsupported", scenarios.OperationKey(operation))
	}
}

func validateOperationTransportFacts(operation scenarios.Operation, observation transportObservation) error {
	if err := validateTransportObservation(observation); err != nil {
		return err
	}
	if operation.ContractOperation != observation.OperationClass {
		return errors.New("Swift transport observation does not match the requested operation")
	}
	if observation.OperationClass == "push" {
		var payload struct {
			Request struct {
				Mutations []json.RawMessage `json:"mutations"`
			} `json:"request"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || observation.RequestFacts == nil || observation.RequestFacts.MutationCount == nil || *observation.RequestFacts.MutationCount != len(payload.Request.Mutations) {
			return errors.New("Swift push request mutation facts do not match the authored operation")
		}
	}
	return nil
}

func validateCursorSourceBinding(operation scenarios.Operation, observation transportObservation, source runnerResult, withinCallCheckpoints map[string]string) error {
	switch operation.ContractOperation {
	case "pull":
		var payload struct {
			Scopes []struct {
				ScopeID      string `json:"scope_id"`
				CursorSource string `json:"cursor_source"`
			} `json:"scopes"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Scopes) == 0 {
			return errors.New("decode Swift authored pull cursor sources failed")
		}
		sourceKind := payload.Scopes[0].CursorSource
		for _, scope := range payload.Scopes {
			if scope.CursorSource != sourceKind {
				return errors.New("Swift authored pull cursor sources are mixed")
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
					return errors.New("Swift within-call rebuild checkpoints do not cover authored pull scopes")
				}
			} else {
				if len(source.ScopeStates) != len(payload.Scopes) {
					return errors.New("Swift local checkpoint sources do not match authored pull scopes")
				}
				expected = make([]string, 0, len(source.ScopeStates))
				for _, state := range source.ScopeStates {
					if state.Cursor == nil || *state.Cursor == "" {
						return errors.New("Swift local checkpoint cursor is absent")
					}
					expected = append(expected, cursorFingerprint(*state.Cursor))
				}
			}
			sort.Strings(expected)
		default:
			return errors.New("Swift authored pull cursor source is unsupported")
		}
		if !equalStrings(expected, observation.CursorFingerprints) {
			return errors.New("Swift pull cursor fingerprints do not match durable checkpoints")
		}
	case "rebuild":
		var payload struct {
			RebuildID    string `json:"rebuild_id"`
			CursorSource string `json:"cursor_source"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.RebuildID == "" {
			return errors.New("decode Swift authored rebuild cursor source failed")
		}
		facts := observation.RequestFacts
		if facts == nil || facts.CursorPresent == nil {
			return errors.New("Swift rebuild cursor facts are absent")
		}
		switch payload.CursorSource {
		case "none":
			if *facts.CursorPresent || facts.CursorFingerprint != nil {
				return errors.New("Swift rebuild request used an unexpected cursor")
			}
		case "local_rebuild_continuation":
			var cursor string
			matches := 0
			rebuildFingerprint := cursorFingerprint(payload.RebuildID)
			if facts.RebuildIDFingerprint != nil {
				rebuildFingerprint = *facts.RebuildIDFingerprint
			}
			for _, attempt := range source.RebuildAttempts {
				if cursorFingerprint(attempt.RebuildID) == rebuildFingerprint {
					matches++
					if attempt.Cursor != nil {
						cursor = *attempt.Cursor
					}
				}
			}
			if matches != 1 || cursor == "" || !*facts.CursorPresent || facts.CursorFingerprint == nil || *facts.CursorFingerprint != cursorFingerprint(cursor) {
				return errors.New("Swift rebuild cursor fingerprint does not match the durable continuation")
			}
		case "forged":
			if !*facts.CursorPresent || facts.CursorFingerprint == nil || *facts.CursorFingerprint != cursorFingerprint(forgedRebuildCursor) {
				return errors.New("Swift forged rebuild cursor fingerprint does not match the deterministic override")
			}
		default:
			return errors.New("Swift authored rebuild cursor source is unsupported")
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

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func transportStepObservation(observation transportObservation) (StepObservation, error) {
	if err := validateTransportObservation(observation); err != nil {
		return StepObservation{}, err
	}
	wire := &WireFacts{
		HTTPStatus: observation.StatusCode,
		ErrorCode:  cloneOptionalString(observation.ErrorCode),
		Retryable:  observation.Retryable,
	}
	return StepObservation{Disposition: "success", Wire: wire}, nil
}

func validCompletion(value string) bool {
	switch value {
	case "idle", "blocked", "error":
		return true
	default:
		return false
	}
}

// ProcessStep executes a durable client restart or completes recorded response loss.
func (p *Platform) ProcessStep(ctx context.Context, client Client, operation scenarios.Operation) (StepObservation, error) {
	if err := p.context(ctx); err != nil {
		return StepObservation{}, err
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return StepObservation{}, fmt.Errorf("Swift process operation is invalid: %w", err)
	}
	state, err := p.client(client)
	if err != nil {
		return StepObservation{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	switch scenarios.OperationKey(operation) {
	case "process/restart-client":
		if state.terminated || state.session == nil || state.pendingLoss != nil || state.activeCall != nil {
			return StepObservation{}, errors.New("Swift client restart is unavailable")
		}
		started := time.Now()
		if err := state.session.Kill(ctx); err != nil {
			return StepObservation{}, err
		}
		closeSession(state.session)
		state.session = nil
		state.terminated = true
		if err := requireExistingDatabase(state.databasePath); err != nil {
			return StepObservation{}, err
		}
		if err := p.startClient(ctx, state, ""); err != nil {
			return StepObservation{}, fmt.Errorf("relaunch Swift runner: %w", err)
		}
		state.restarted = true
		after, err := captureRunner(ctx, state)
		if err != nil {
			return StepObservation{}, err
		}
		// A restart replaces the runner process, so its provenance maintenance
		// cursor starts again. The window measures the relaunched capture
		// against itself rather than comparing a cursor across a process
		// boundary it cannot span.
		window, err := windowFromResults(started, after, after, nil)
		if err != nil {
			return StepObservation{}, err
		}
		return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
	case "process/response-loss":
		batchID, err := responseLossBatch(operation, client)
		if err != nil {
			return StepObservation{}, err
		}
		loss := state.pendingLoss
		if !state.terminated || state.session != nil || loss == nil || loss.batchID != batchID {
			return StepObservation{}, errors.New("Swift response loss has no matching interrupted request")
		}
		if err := requireExistingDatabase(state.databasePath); err != nil {
			return StepObservation{}, err
		}
		if err := p.startClient(ctx, state, ""); err != nil {
			return StepObservation{}, fmt.Errorf("relaunch Swift runner after response loss: %w", err)
		}
		state.restarted = true
		state.pendingLoss = nil
		after, err := captureRunner(ctx, state)
		if err != nil {
			return StepObservation{}, err
		}
		window, err := windowFromResults(loss.started, loss.before, after, loss.observations)
		if err != nil {
			return StepObservation{}, err
		}
		return observationWithWindow(StepObservation{Disposition: "success"}, window), nil
	default:
		return StepObservation{}, fmt.Errorf("Swift process operation %q is unsupported", scenarios.OperationKey(operation))
	}
}

func responseLossBatch(operation scenarios.Operation, client Client) (string, error) {
	var payload struct {
		AuthenticatedUserID string `json:"authenticated_user_id"`
		ClientID            string `json:"client_id"`
		BatchID             string `json:"batch_id"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.AuthenticatedUserID != client.UserID || payload.ClientID != client.ClientID || payload.BatchID == "" {
		return "", errors.New("Swift response-loss identity is invalid")
	}
	return payload.BatchID, nil
}

// Capture reads durable public inspection facts for the requested clients.
func (p *Platform) Capture(ctx context.Context, clients []Client, sources []string) ([]CaptureFacts, error) {
	if err := p.context(ctx); err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, errors.New("Swift capture has no sources")
	}
	if len(clients) == 0 {
		return nil, errors.New("Swift capture has no clients")
	}
	seenSources := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source == "" {
			return nil, errors.New("Swift capture source is invalid")
		}
		if _, duplicate := seenSources[source]; duplicate {
			return nil, errors.New("Swift capture source is duplicated")
		}
		seenSources[source] = struct{}{}
	}

	results := make([]captureResult, 0, len(clients))
	seenClients := make(map[string]struct{}, len(clients))
	for _, client := range clients {
		if _, duplicate := seenClients[client.Key]; duplicate {
			return nil, errors.New("Swift capture client is duplicated")
		}
		seenClients[client.Key] = struct{}{}
		state, err := p.client(client)
		if err != nil {
			return nil, err
		}
		state.mu.Lock()
		if state.terminated || state.session == nil {
			state.mu.Unlock()
			return nil, errors.New("Swift capture client is unavailable")
		}
		result, err := captureRunner(ctx, state)
		state.mu.Unlock()
		if err != nil {
			return nil, err
		}
		results = append(results, captureResult{client: client, result: result})
	}

	facts := make([]CaptureFacts, 0, len(sources))
	for _, source := range sources {
		stateFacts, err := captureFactsForSource(source, results)
		if err != nil {
			return nil, err
		}
		facts = append(facts, CaptureFacts{Source: source, StateFacts: stateFacts})
	}
	return facts, nil
}

type captureResult struct {
	client Client
	result runnerResult
}

func captureRunner(ctx context.Context, state *platformClient) (runnerResult, error) {
	selectors := clientSelectors(state)
	if len(selectors) <= maximumRunnerSelectors {
		return captureRunnerBatch(ctx, state, selectors)
	}

	baseline, err := captureRunnerBatch(ctx, state, nil)
	if err != nil {
		return runnerResult{}, err
	}
	if *baseline.ApplicationRowCount > maximumRunnerRows || len(baseline.ApplicationRows) == *baseline.ApplicationRowCount {
		return baseline, nil
	}
	rows := append([]map[string]json.RawMessage(nil), baseline.ApplicationRows...)
	for start := 0; start < len(selectors); start += maximumRunnerSelectors {
		end := min(start+maximumRunnerSelectors, len(selectors))
		captured, err := captureRunnerBatch(ctx, state, selectors[start:end])
		if err != nil {
			return runnerResult{}, err
		}
		if !equalRunnerCaptureState(baseline, captured) {
			return runnerResult{}, errors.New("Swift runner capture changed between selector batches")
		}
		extra, err := applicationRowsBeyondBaseline(baseline.ApplicationRows, captured.ApplicationRows)
		if err != nil {
			return runnerResult{}, err
		}
		rows = append(rows, extra...)
	}
	if len(rows) != *baseline.ApplicationRowCount {
		return runnerResult{}, errors.New("Swift runner selector batches did not cover application rows")
	}
	baseline.ApplicationRows = rows
	return baseline, nil
}

func captureRunnerBatch(ctx context.Context, state *platformClient, selectors []runnerRowSelector) (runnerResult, error) {
	result, err := state.session.Execute(ctx, Request{Operation: "capture", RowSelectors: selectors})
	if err != nil {
		return runnerResult{}, fmt.Errorf("capture Swift runner state: %w", err)
	}
	if err := validateCaptureResult(result); err != nil {
		return runnerResult{}, err
	}
	return result, nil
}

func equalRunnerCaptureState(left, right runnerResult) bool {
	left.ApplicationRows = nil
	right.ApplicationRows = nil
	return reflect.DeepEqual(left, right)
}

func applicationRowsBeyondBaseline(baseline, captured []map[string]json.RawMessage) ([]map[string]json.RawMessage, error) {
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
			return nil, errors.New("Swift runner selector batch omitted a baseline application row")
		}
	}
	return extra, nil
}

func applicationRowKey(row map[string]json.RawMessage) (string, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return "", errors.New("encode Swift application row identity failed")
	}
	return string(encoded), nil
}

func clientSelectors(state *platformClient) []runnerRowSelector {
	values := make([]runnerRowSelector, 0, len(state.selectors))
	for _, selector := range state.selectors {
		selector.PrimaryKey = append(json.RawMessage(nil), selector.PrimaryKey...)
		values = append(values, selector)
	}
	sort.Slice(values, func(left, right int) bool { return selectorKey(values[left]) < selectorKey(values[right]) })
	return values
}

func selectorKey(selector runnerRowSelector) string {
	return selector.TableName + "\x00" + selector.PrimaryKeyField + "\x00" + string(selector.PrimaryKey)
}

func validateCaptureResult(result runnerResult) error {
	if result.Status == nil || *result.Status == "" || result.PendingChangeCount == nil || *result.PendingChangeCount < 0 || result.ApplicationRowCount == nil || result.MutationLedgerCount == nil || result.MutationOutcomeCount == nil || result.SealedBatchCount == nil || result.RejectedMutationCount == nil || result.ScopeStateCount == nil || result.ScopeRowCount == nil || result.ProvenanceCount == nil || result.RowMetadataCount == nil || result.RebuildAttemptCount == nil || result.RebuildReceiptCount == nil || result.ProvenanceMaintenanceWorkCursor == nil || *result.ProvenanceMaintenanceWorkCursor < 0 || result.Events == nil || result.ScopeStatesTruncated == nil || result.ScopeRowsTruncated == nil || result.RebuildAttemptsTruncated == nil || result.RebuildReceiptsTruncated == nil || result.RowMetadataTruncated == nil || result.CaptureOverflowed == nil {
		return errors.New("Swift runner capture facts are incomplete")
	}
	truncated := *result.ScopeStatesTruncated || *result.ScopeRowsTruncated || *result.RebuildAttemptsTruncated || *result.RebuildReceiptsTruncated || *result.RowMetadataTruncated
	if (*result.ApplicationRowCount <= maximumRunnerRows) != (result.ApplicationRows != nil) ||
		(*result.MutationLedgerCount <= maximumRunnerRecords) != (result.RetainedMutations != nil) ||
		(*result.RejectedMutationCount <= maximumRunnerRecords) != (result.RejectedMutations != nil) ||
		(*result.ScopeStateCount > maximumRunnerRecords) != *result.ScopeStatesTruncated ||
		(*result.ScopeStateCount <= maximumRunnerRecords) != (result.ScopeStates != nil) ||
		(*result.ScopeRowCount > maximumRunnerRecords) != *result.ScopeRowsTruncated ||
		(*result.ScopeRowCount <= maximumRunnerRecords) != (result.ScopeRows != nil) ||
		(*result.RowMetadataCount > maximumRunnerRecords) != *result.RowMetadataTruncated ||
		(*result.RowMetadataCount <= maximumRunnerRecords) != (result.RowMetadataRecords != nil) ||
		(*result.RebuildAttemptCount > maximumRunnerRecords) != *result.RebuildAttemptsTruncated ||
		(*result.RebuildAttemptCount <= maximumRunnerRecords) != (result.RebuildAttempts != nil) ||
		(*result.RebuildReceiptCount > maximumRunnerRecords) != *result.RebuildReceiptsTruncated ||
		(*result.RebuildReceiptCount <= maximumRunnerRecords) != (result.RebuildReceipts != nil) ||
		*result.CaptureOverflowed != truncated {
		return errors.New("Swift runner capture detail bounds are inconsistent")
	}
	if len(result.ScopeStates) != boundedDetailCount(*result.ScopeStateCount, maximumRunnerRecords) || len(result.ScopeRows) != boundedDetailCount(*result.ScopeRowCount, maximumRunnerRecords) || len(result.RejectedMutations) != boundedDetailCount(*result.RejectedMutationCount, maximumRunnerRecords) || len(result.RebuildAttempts) != boundedDetailCount(*result.RebuildAttemptCount, maximumRunnerRecords) || len(result.RowMetadataRecords) != boundedDetailCount(*result.RowMetadataCount, maximumRunnerRecords) {
		return errors.New("Swift runner capture counts do not match detail")
	}
	if result.RebuildReceipts != nil {
		pageCount := 0
		seenRebuilds := make(map[string]struct{}, len(result.RebuildReceipts))
		for _, receipt := range result.RebuildReceipts {
			if !validLowerHexDigest(receipt.RebuildIDFingerprint) || receipt.PageCount <= 0 || receipt.ReturnedRecordCount < 0 || receipt.PageCount > *result.RebuildReceiptCount-pageCount || len(receipt.RecordIdentitiesHex) != receipt.ReturnedRecordCount || len(receipt.ReceivedRowChecksums) != receipt.ReturnedRecordCount || len(receipt.ComputedRowChecksums) != receipt.ReturnedRecordCount || len(receipt.RequestChainExpected) != len(receipt.RequestChainObserved) {
				return errors.New("Swift runner rebuild receipt is invalid")
			}
			if _, duplicate := seenRebuilds[receipt.RebuildIDFingerprint]; duplicate {
				return errors.New("Swift runner rebuild receipt is duplicated")
			}
			seenRebuilds[receipt.RebuildIDFingerprint] = struct{}{}
			pageCount += receipt.PageCount
		}
		if pageCount != *result.RebuildReceiptCount {
			return errors.New("Swift runner rebuild receipt count does not match detail")
		}
	}
	seenScopes := make(map[string]struct{}, len(result.ScopeStates))
	for _, value := range result.ScopeStates {
		if value.ScopeID == "" || value.Generation < 0 {
			return errors.New("Swift runner scope-state fact is invalid")
		}
		if _, duplicate := seenScopes[value.ScopeID]; duplicate {
			return errors.New("Swift runner scope-state fact is duplicated")
		}
		seenScopes[value.ScopeID] = struct{}{}
		if _, err := swiftChecksumDigest(value.Checksum); err != nil {
			return err
		}
		if value.LocalChecksum != "" {
			if _, err := swiftChecksumDigest(pointerString(value.LocalChecksum)); err != nil {
				return err
			}
		}
	}
	seenEdges := make(map[string]struct{}, len(result.ScopeRows))
	metadata := make(map[string]struct{}, len(result.RowMetadataRecords))
	for _, value := range result.RowMetadataRecords {
		if value.TableName == "" || value.RecordID == "" || value.ServerVersion == "" {
			return errors.New("Swift runner row-metadata fact is invalid")
		}
		key := value.TableName + "\x00" + value.RecordID
		if _, duplicate := metadata[key]; duplicate {
			return errors.New("Swift runner row-metadata fact is duplicated")
		}
		metadata[key] = struct{}{}
	}
	for _, value := range result.ScopeRows {
		key := scopeRowKey(value)
		if value.ScopeID == "" || value.TableName == "" || value.RecordID == "" || value.Checksum == "" || value.Generation < 0 {
			return errors.New("Swift runner provenance fact is invalid")
		}
		if _, duplicate := seenEdges[key]; duplicate {
			return errors.New("Swift runner provenance fact is duplicated")
		}
		seenEdges[key] = struct{}{}
		if result.RowMetadataRecords != nil {
			if _, found := metadata[value.TableName+"\x00"+value.RecordID]; !found {
				return errors.New("Swift runner provenance metadata is incomplete")
			}
		}
	}
	if result.Schema != nil && (result.Schema.Version <= 0 || !schemaHashPattern.MatchString(result.Schema.Hash)) {
		return errors.New("Swift runner schema inspection is invalid")
	}
	return nil
}

func boundedDetailCount(count, maximum int) int {
	if count > maximum {
		return 0
	}
	return count
}

func captureFactsForSource(source string, values []captureResult) (scenarios.StateFacts, error) {
	var facts scenarios.StateFacts
	for _, value := range values {
		clientFacts, err := clientFactsForSource(source, value.client, value.result)
		if err != nil {
			return scenarios.StateFacts{}, err
		}
		if clientFacts != nil {
			facts.Clients = append(facts.Clients, *clientFacts)
		}
	}
	return facts, nil
}

func clientFactsForSource(source string, client Client, result runnerResult) (*scenarios.ClientDurabilityFact, error) {
	facts := scenarios.ClientDurabilityFact{UserID: client.UserID, ClientID: client.ClientID}
	switch source {
	case "application-rows":
		count := uint64(*result.ApplicationRowCount)
		facts.RowCount = &count
	case "pending-mutations":
		queue, err := queuedMutationFacts(result.RetainedMutations)
		if err != nil {
			return nil, err
		}
		count := uint64(*result.MutationLedgerCount)
		facts.QueueCount = &count
		facts.Queue = queue
		sealedBatchCount := uint64(*result.SealedBatchCount)
		facts.SealedBatchCount = &sealedBatchCount
	case "rejected-mutations":
		outcomes, err := outcomeFacts(result.RejectedMutations)
		if err != nil {
			return nil, err
		}
		count := uint64(*result.MutationOutcomeCount)
		facts.OutcomeCount = &count
		facts.Outcomes = outcomes
	case "scope-state", "checkpoints":
		checkpoints, err := checkpointFacts(result.ScopeStates)
		if err != nil {
			return nil, err
		}
		count := uint64(*result.ScopeStateCount)
		facts.CheckpointCount = &count
		facts.Checkpoints = checkpoints
	case "provenance":
		provenance, err := provenanceFacts(result.ScopeRows, result.RowMetadataRecords)
		if err != nil {
			return nil, err
		}
		count := uint64(*result.ProvenanceCount)
		facts.ProvenanceCount = &count
		facts.Provenance = provenance
	case "rebuild-state":
		count, err := rebuildAttemptFactCount(result)
		if err != nil {
			return nil, err
		}
		facts.RebuildAttemptCount = &count
	case "sync-status", "sync-events", "request-trace", "process-trace":
		return nil, nil
	default:
		return nil, fmt.Errorf("Swift capture source %q is unsupported", source)
	}
	if result.Schema != nil {
		version := uint64(result.Schema.Version)
		facts.CurrentSchema = &scenarios.SchemaFact{Version: version, Hash: result.Schema.Hash}
	}
	return &facts, nil
}

func rebuildAttemptFactCount(result runnerResult) (uint64, error) {
	if result.RebuildAttempts == nil || result.RebuildReceipts == nil {
		return 0, errors.New("Swift rebuild attempt detail is unavailable")
	}
	fingerprints := make(map[string]struct{}, len(result.RebuildAttempts)+len(result.RebuildReceipts))
	for _, attempt := range result.RebuildAttempts {
		if attempt.RebuildID == "" {
			return 0, errors.New("Swift rebuild attempt identity is invalid")
		}
		fingerprints[cursorFingerprint(attempt.RebuildID)] = struct{}{}
	}
	for _, receipt := range result.RebuildReceipts {
		if !validLowerHexDigest(receipt.RebuildIDFingerprint) {
			return 0, errors.New("Swift rebuild receipt identity is invalid")
		}
		fingerprints[receipt.RebuildIDFingerprint] = struct{}{}
	}
	return uint64(len(fingerprints)), nil
}

func checkpointFacts(values []scopeStateRecord) ([]scenarios.CheckpointFact, error) {
	result := make([]scenarios.CheckpointFact, 0, len(values))
	for _, value := range values {
		checksum, err := swiftChecksumDigest(value.Checksum)
		if err != nil {
			return nil, err
		}
		var localChecksum *string
		if value.LocalChecksum != "" {
			localChecksum, err = swiftChecksumDigest(pointerString(value.LocalChecksum))
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

func pointerString(value string) *string {
	return &value
}

func queuedMutationFacts(values []retainedMutation) ([]scenarios.QueuedMutationFact, error) {
	result := make([]scenarios.QueuedMutationFact, 0, len(values))
	for _, value := range values {
		if value.LocalOrder < 0 || value.AuthoredSchema.Version <= 0 || !schemaHashPattern.MatchString(value.AuthoredSchema.Hash) {
			return nil, errors.New("Swift runner queued mutation inspection is invalid")
		}
		identity, err := recordIDWireJSON(value.RecordID, value.PrimaryKeyLogicalType)
		if err != nil {
			return nil, err
		}
		columns := make([]scenarios.FieldFact, 0, len(value.AuthoredFields))
		for _, field := range value.AuthoredFields {
			if field.FieldID == "" || field.LogicalType == "" || !json.Valid(field.Value) {
				return nil, errors.New("Swift runner queued mutation field is invalid")
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
			BaseVersion:       cloneOptionalString(value.BaseVersion),
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

func outcomeFacts(values []retainedRejection) ([]scenarios.MutationOutcomeFact, error) {
	result := make([]scenarios.MutationOutcomeFact, 0, len(values))
	for _, value := range values {
		if value.Mutation.MutationID == "" || value.Rejection.MutationID != value.Mutation.MutationID || value.Rejection.Status == "" || value.Rejection.Code == "" {
			return nil, errors.New("Swift runner rejected mutation inspection is invalid")
		}
		result = append(result, scenarios.MutationOutcomeFact{MutationID: value.Mutation.MutationID, State: value.Rejection.Status, Reason: value.Rejection.Code})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].MutationID < result[right].MutationID })
	return result, nil
}

func provenanceFacts(rows []scopeRowRecord, metadata []rowMetadataRecord) ([]scenarios.ProvenanceFact, error) {
	type accumulated struct {
		table   string
		record  string
		version string
		scopes  []string
	}
	versions := make(map[string]string, len(metadata))
	for _, value := range metadata {
		versions[value.TableName+"\x00"+value.RecordID] = value.ServerVersion
	}
	grouped := make(map[string]*accumulated)
	for _, row := range rows {
		key := row.TableName + "\x00" + row.RecordID
		value := grouped[key]
		if value == nil {
			value = &accumulated{table: row.TableName, record: row.RecordID, version: versions[key]}
			grouped[key] = value
		}
		value.scopes = append(value.scopes, row.ScopeID)
	}
	result := make([]scenarios.ProvenanceFact, 0, len(grouped))
	for _, value := range grouped {
		canonical, err := json.Marshal(value.record)
		if err != nil || value.version == "" {
			return nil, errors.New("Swift runner provenance identity is invalid")
		}
		sort.Strings(value.scopes)
		result = append(result, scenarios.ProvenanceFact{
			TableID:           value.table,
			CanonicalWireJSON: string(canonical),
			Scopes:            value.scopes,
			Version:           value.version,
		})
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].TableID+"\x00"+result[left].CanonicalWireJSON < result[right].TableID+"\x00"+result[right].CanonicalWireJSON
	})
	return result, nil
}

func recordIDWireJSON(recordID, logicalType string) (string, error) {
	switch logicalType {
	case "string", "decimal", "datetime", "date", "time", "json":
		value, err := json.Marshal(recordID)
		if err != nil {
			return "", errors.New("encode Swift record identity failed")
		}
		return string(value), nil
	case "int", "int64":
		value, err := strconv.ParseInt(recordID, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != recordID {
			return "", errors.New("Swift integer record identity is invalid")
		}
		return recordID, nil
	default:
		return "", errors.New("Swift runner primary-key type has no conformance identity mapping")
	}
}

func decodeLocalWrite(operation scenarios.Operation, client Client) (runnerLocalAction, runnerRowSelector, error) {
	var payload map[string]json.RawMessage
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("decode Swift local write payload failed")
	}
	if !payloadStringEquals(payload, "authenticated_user_id", client.UserID) || !payloadStringEquals(payload, "client_id", client.ClientID) {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("Swift local write identity does not match client")
	}
	tableName, err := payloadString(payload, "table_id")
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	operationName, err := payloadString(payload, "operation")
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	primaryKeyField, primaryKey, err := decodePrimaryKey(payload["pk"])
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	fields, err := decodeColumns(payload["columns"])
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	action := runnerLocalAction{Operation: operationName, TableName: tableName, PrimaryKeyField: primaryKeyField, PrimaryKey: append(json.RawMessage(nil), primaryKey...), Fields: fields}
	if err := validateRunnerLocalAction(action); err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("Swift local write cannot map to runner action")
	}
	selector := runnerRowSelector{TableName: tableName, PrimaryKeyField: primaryKeyField, PrimaryKey: append(json.RawMessage(nil), primaryKey...)}
	return action, selector, nil
}

func payloadString(payload map[string]json.RawMessage, field string) (string, error) {
	raw, found := payload[field]
	if !found {
		return "", errors.New("Swift local write field is absent")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", errors.New("Swift local write field is invalid")
	}
	return value, nil
}

func payloadStringEquals(payload map[string]json.RawMessage, field, wanted string) bool {
	value, err := payloadString(payload, field)
	return err == nil && value == wanted
}

func decodePrimaryKey(raw json.RawMessage) (string, json.RawMessage, error) {
	if len(raw) == 0 {
		return "", nil, errors.New("Swift local write primary key is absent")
	}
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &object); err != nil {
		return "", nil, errors.New("Swift local write primary key is invalid")
	}
	if len(object) == 2 {
		fieldRaw, hasField := object["field_id"]
		value, hasValue := object["value"]
		if hasField && hasValue {
			var field string
			if err := json.Unmarshal(fieldRaw, &field); err != nil || field == "" || validateRunnerLocalJSONValue(value) != nil {
				return "", nil, errors.New("Swift local write primary key is invalid")
			}
			return field, append(json.RawMessage(nil), value...), nil
		}
	}
	if len(object) != 1 {
		return "", nil, errors.New("Swift local write primary key shape is unsupported")
	}
	for field, value := range object {
		if field == "" || validateRunnerLocalJSONValue(value) != nil {
			return "", nil, errors.New("Swift local write primary key is invalid")
		}
		return field, append(json.RawMessage(nil), value...), nil
	}
	return "", nil, errors.New("Swift local write primary key is invalid")
}

func decodeColumns(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "{") {
		var object map[string]json.RawMessage
		if err := jsonstrict.Decode(raw, &object); err != nil {
			return nil, errors.New("Swift local write columns are invalid")
		}
		fields := make(map[string]json.RawMessage, len(object))
		for field, value := range object {
			if field == "" || validateRunnerLocalJSONValue(value) != nil {
				return nil, errors.New("Swift local write column is invalid")
			}
			fields[field] = append(json.RawMessage(nil), value...)
		}
		return fields, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, errors.New("Swift local write columns are invalid")
	}
	fields := make(map[string]json.RawMessage, len(values))
	for _, value := range values {
		var object map[string]json.RawMessage
		if err := jsonstrict.Decode(value, &object); err != nil || len(object) != 2 {
			return nil, errors.New("Swift local write column shape is invalid")
		}
		fieldRaw, hasField := object["field_id"]
		fieldValue, hasValue := object["value"]
		if !hasField || !hasValue {
			return nil, errors.New("Swift local write column is incomplete")
		}
		var field string
		if err := json.Unmarshal(fieldRaw, &field); err != nil || field == "" || validateRunnerLocalJSONValue(fieldValue) != nil {
			return nil, errors.New("Swift local write column is invalid")
		}
		if _, duplicate := fields[field]; duplicate {
			return nil, errors.New("Swift local write column is duplicated")
		}
		fields[field] = append(json.RawMessage(nil), fieldValue...)
	}
	return fields, nil
}

func (p *Platform) completeWindow(state *platformClient, checkpoint uint64, started time.Time, before, after runnerResult) (operationWindow, error) {
	observations, err := state.session.ObservationsAfter(checkpoint)
	if err != nil {
		return operationWindow{}, err
	}
	return windowFromResults(started, before, after, observations)
}

func windowFromResults(started time.Time, before, after runnerResult, observations []transportObservation) (operationWindow, error) {
	work, err := provenanceMaintenanceDelta(before, after)
	if err != nil {
		return operationWindow{}, err
	}
	return operationWindow{
		observations:              cloneTransportObservations(observations),
		duration:                  time.Since(started),
		provenanceMaintenanceWork: work,
	}, nil
}

func provenanceMaintenanceDelta(before, after runnerResult) (uint64, error) {
	if before.ProvenanceMaintenanceWorkCursor == nil || after.ProvenanceMaintenanceWorkCursor == nil || *before.ProvenanceMaintenanceWorkCursor < 0 || *after.ProvenanceMaintenanceWorkCursor < 0 {
		return 0, errors.New("Swift provenance maintenance cursor is unavailable")
	}
	if *after.ProvenanceMaintenanceWorkCursor < *before.ProvenanceMaintenanceWorkCursor {
		return 0, errors.New("Swift provenance maintenance cursor moved backward")
	}
	return uint64(*after.ProvenanceMaintenanceWorkCursor - *before.ProvenanceMaintenanceWorkCursor), nil
}

func scopeRowKey(value scopeRowRecord) string {
	return value.ScopeID + "\x00" + value.TableName + "\x00" + value.RecordID
}

func pushMutationCount(observations []transportObservation) uint64 {
	var count uint64
	for _, observation := range observations {
		if observation.OperationClass == "push" && observation.RequestFacts != nil && observation.RequestFacts.MutationCount != nil && *observation.RequestFacts.MutationCount > 0 {
			count += uint64(*observation.RequestFacts.MutationCount)
		}
	}
	return count
}

func observationWithWindow(observation StepObservation, window operationWindow) StepObservation {
	if window.duration > 0 {
		observation.DurationNanoseconds = uint64(window.duration)
	}
	observation.ProvenanceMaintenanceWork = window.provenanceMaintenanceWork
	observation.ReplayedMutationCount = window.replayedMutationCount
	return observation
}

// Close stops all owned runners and retains their application databases.
func (p *Platform) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("Swift platform close context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
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
	for _, state := range p.clients {
		clients = append(clients, state)
	}
	p.mu.Unlock()
	var failures []error
	for _, state := range clients {
		state.mu.Lock()
		session := state.session
		state.session = nil
		state.mu.Unlock()
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

func (p *Platform) context(ctx context.Context) error {
	if p == nil {
		return errors.New("Swift platform is unavailable")
	}
	if ctx == nil {
		return errors.New("Swift platform context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return errors.New("Swift platform is closed")
	}
	return nil
}

func (p *Platform) client(client Client) (*platformClient, error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("Swift platform is closed")
	}
	state := p.clients[client.Key]
	if state == nil || state.client != client {
		return nil, errors.New("Swift platform client is unavailable")
	}
	return state, nil
}

// recordTemporaryUnavailableMiss records why an armed push fault did not apply
// to an observed push. A silent pass through hides an unfired authored fault.
func (p *Platform) recordTemporaryUnavailableMiss(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.temporaryUnavailableMisses) < 8 {
		p.temporaryUnavailableMisses = append(p.temporaryUnavailableMisses, reason)
	}
}

// TemporaryUnavailablePushMisses reports why an armed push fault did not apply.
func (p *Platform) TemporaryUnavailablePushMisses() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.temporaryUnavailableMisses...)
}
