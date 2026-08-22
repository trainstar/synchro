package nativeharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// SwiftAuthTokenResolver resolves the authenticated token for one native client.
// It must not return a token for a different client identity.
type SwiftAuthTokenResolver func(context.Context, scenarios.NativeClient) (string, error)

// SwiftSeedDatabasePathResolver resolves one staged production seed database.
// The returned path must identify a regular, finalized seed file.
type SwiftSeedDatabasePathResolver func(context.Context, scenarios.NativeClient, scenarios.StepID) (string, error)

// SwiftPlatformConfig configures the current macOS Swift runner.
type SwiftPlatformConfig struct {
	RunnerPath                   string
	ApplicationDatabaseDirectory string
	ServerURL                    string
	AuthToken                    SwiftAuthTokenResolver
	SeedDatabasePath             SwiftSeedDatabasePathResolver
	Platform                     string
	AppVersion                   string
}

// SwiftPlatform drives one current Swift runner for each declared client key.
type SwiftPlatform struct {
	config SwiftPlatformConfig

	mu         sync.Mutex
	closed     bool
	clients    map[string]*swiftPlatformClient
	lastWindow *swiftOperationWindow
}

type swiftPlatformClient struct {
	mu sync.Mutex

	client                      scenarios.NativeClient
	databasePath                string
	databaseIdentityFingerprint string
	process                     *runnerProcess
	processID                   string
	terminated                  bool
	activeCall                  *swiftPlatformCall
	lastWindow                  *swiftOperationWindow
	selectors                   map[string]runnerRowSelector
}

type swiftPlatformCall struct {
	id         scenarios.NativeCallID
	checkpoint uint64
	started    time.Time
	paused     bool
}

type swiftOperationWindow struct {
	clientKey    string
	observations []transportObservation
	duration     time.Duration
}

var _ Platform = (*SwiftPlatform)(nil)

// NewSwiftPlatform creates the current-only Swift platform capability.
func NewSwiftPlatform(config SwiftPlatformConfig) (*SwiftPlatform, error) {
	normalized, err := normalizeSwiftPlatformConfig(config)
	if err != nil {
		return nil, err
	}
	return &SwiftPlatform{
		config:  normalized,
		clients: make(map[string]*swiftPlatformClient),
	}, nil
}

func normalizeSwiftPlatformConfig(config SwiftPlatformConfig) (SwiftPlatformConfig, error) {
	if config.RunnerPath == "" || config.ApplicationDatabaseDirectory == "" || config.ServerURL == "" || config.AuthToken == nil || config.Platform == "" || config.AppVersion == "" {
		return SwiftPlatformConfig{}, errors.New("Swift platform configuration is incomplete")
	}
	if config.Platform != "macos" {
		return SwiftPlatformConfig{}, errors.New("Swift platform supports only current macOS")
	}
	if len(config.AppVersion) > 128 {
		return SwiftPlatformConfig{}, errors.New("Swift platform app version is invalid")
	}
	parsedURL, err := url.Parse(config.ServerURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" || parsedURL.User != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return SwiftPlatformConfig{}, errors.New("Swift platform server URL is invalid")
	}
	runnerPath, err := filepath.Abs(config.RunnerPath)
	if err != nil {
		return SwiftPlatformConfig{}, errors.New("Swift platform runner path is invalid")
	}
	runnerInfo, err := os.Lstat(runnerPath)
	if err != nil || !runnerInfo.Mode().IsRegular() || runnerInfo.Mode()&0o111 == 0 {
		return SwiftPlatformConfig{}, errors.New("Swift platform runner is unavailable")
	}
	databaseDirectory, err := prepareSwiftApplicationDatabaseDirectory(config.ApplicationDatabaseDirectory)
	if err != nil {
		return SwiftPlatformConfig{}, err
	}
	config.RunnerPath = runnerPath
	config.ApplicationDatabaseDirectory = databaseDirectory
	return config, nil
}

func prepareSwiftApplicationDatabaseDirectory(path string) (string, error) {
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

// Open starts one real Swift runner for a client key.
func (p *SwiftPlatform) Open(ctx context.Context, request OpenRequest) error {
	if err := swiftPlatformContext(ctx); err != nil {
		return err
	}
	if err := validateSwiftOpenRequest(request); err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("Swift platform is closed")
	}
	if _, exists := p.clients[request.ClientKey]; exists {
		p.mu.Unlock()
		return errors.New("Swift platform client is already open")
	}
	p.mu.Unlock()

	databasePath := p.databasePath(request.Client.DatabaseKey)
	if err := requireAbsentDatabaseFamily(databasePath); err != nil {
		return err
	}
	seedPath, err := p.seedPath(ctx, request)
	if err != nil {
		return err
	}
	client := &swiftPlatformClient{
		client:                      request.Client,
		databasePath:                databasePath,
		databaseIdentityFingerprint: swiftDatabaseFingerprint(databasePath),
		selectors:                   make(map[string]runnerRowSelector),
	}
	if err := p.startClientRunner(ctx, client, seedPath); err != nil {
		return err
	}

	if request.Initialization == "current" {
		if err := p.initializeCurrentDatabase(ctx, client); err != nil {
			closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = client.process.close(closeContext)
			return err
		}
		client.lastWindow = nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.process.close(closeContext)
		return errors.New("Swift platform is closed")
	}
	if _, exists := p.clients[request.ClientKey]; exists {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.process.close(closeContext)
		return errors.New("Swift platform client is already open")
	}
	p.clients[request.ClientKey] = client
	if request.Initialization == "current" {
		p.lastWindow = nil
	}
	return nil
}

func validateSwiftOpenRequest(request OpenRequest) error {
	if request.ClientKey == "" || request.Client.Key != request.ClientKey || request.Client.ClientID == "" || request.Client.DatabaseKey == "" {
		return errors.New("Swift platform open client is invalid")
	}
	switch request.Initialization {
	case "empty", "current":
		if request.DatabaseMode != "create" || request.SeedStepID != nil {
			return errors.New("Swift platform empty or current open is invalid")
		}
	case "seed":
		if request.DatabaseMode != "reuse" || request.SeedStepID == nil || *request.SeedStepID == "" {
			return errors.New("Swift platform seed open is invalid")
		}
	default:
		return errors.New("Swift platform initialization is unsupported")
	}
	return nil
}

func (p *SwiftPlatform) seedPath(ctx context.Context, request OpenRequest) (string, error) {
	if request.Initialization != "seed" {
		return "", nil
	}
	if p.config.SeedDatabasePath == nil {
		return "", errors.New("Swift platform staged seed resolver is unavailable")
	}
	path, err := p.config.SeedDatabasePath(ctx, request.Client, *request.SeedStepID)
	if err != nil || path == "" {
		return "", errors.New("resolve staged Swift production seed failed")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", errors.New("staged Swift production seed path is invalid")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("staged Swift production seed is unavailable")
	}
	return path, nil
}

func (p *SwiftPlatform) databasePath(databaseKey string) string {
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

func swiftDatabaseFingerprint(path string) string {
	digest := sha256.Sum256([]byte("synchro:swift:application-database:v1\x00" + path))
	return hex.EncodeToString(digest[:])
}

func (p *SwiftPlatform) startClientRunner(ctx context.Context, client *swiftPlatformClient, seedPath string) error {
	process, err := startRunnerProcess(ctx, p.config.RunnerPath)
	if err != nil {
		return err
	}
	processID, err := swiftRunnerProcessID(process)
	if err != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = process.close(closeContext)
		return err
	}
	token, err := p.config.AuthToken(ctx, client.client)
	if err != nil || token == "" {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = process.close(closeContext)
		return errors.New("resolve Swift client authentication failed")
	}
	result, err := process.send(ctx, runnerCommand{
		Operation:        "open",
		DatabasePath:     client.databasePath,
		ServerURL:        p.config.ServerURL,
		AuthToken:        token,
		ClientID:         client.client.ClientID,
		SeedDatabasePath: seedPath,
		Platform:         p.config.Platform,
		AppVersion:       p.config.AppVersion,
	})
	if err != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = process.close(closeContext)
		return fmt.Errorf("open Swift runner client: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = process.close(closeContext)
		return errors.New("Swift runner open did not return status")
	}
	client.process = process
	client.processID = processID
	client.terminated = false
	return nil
}

func swiftRunnerProcessID(process *runnerProcess) (string, error) {
	if process == nil {
		return "", errors.New("Swift runner process is unavailable")
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.command == nil || process.command.Process == nil || process.command.Process.Pid < 1 {
		return "", errors.New("Swift runner process identity is unavailable")
	}
	return "swift-runner:" + strconv.Itoa(process.command.Process.Pid), nil
}

func (p *SwiftPlatform) initializeCurrentDatabase(ctx context.Context, client *swiftPlatformClient) error {
	callID := scenarios.NativeCallID("open_" + client.databaseIdentityFingerprint[:24])
	completed, _, err := p.runPublicCall(ctx, client, callID, "start")
	if err != nil {
		return fmt.Errorf("initialize current Swift database: %w", err)
	}
	if completed.Completion != "idle" {
		return errors.New("current Swift database initialization did not reach idle")
	}
	result, err := client.process.send(ctx, runnerCommand{Operation: "lifecycle", LifecycleOperation: "stop"})
	if err != nil {
		return fmt.Errorf("stop current Swift database initialization: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		return errors.New("Swift runner stop did not return status")
	}
	return nil
}

// LocalAction executes one authored local write through the public runner.
func (p *SwiftPlatform) LocalAction(ctx context.Context, request LocalActionRequest) (nativeexecution.StepObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil {
		return nativeexecution.StepObservation{}, errors.New("Swift client is unavailable for a local action")
	}
	payload, selector, err := decodeSwiftLocalWrite(request.Operation, client.client)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	checkpoint := client.process.transportCheckpointValue()
	started := time.Now()
	result, err := client.process.send(ctx, runnerCommand{Operation: "local-action", LocalAction: &payload})
	if err != nil {
		return nativeexecution.StepObservation{}, fmt.Errorf("execute Swift local action: %w", err)
	}
	if result.RowsAffected == nil || *result.RowsAffected != 1 {
		return nativeexecution.StepObservation{}, errors.New("Swift local action did not affect one row")
	}
	observations, err := client.process.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	window := swiftOperationWindow{clientKey: request.ClientKey, observations: observations, duration: time.Since(started)}
	p.storeWindow(client, window)
	client.selectors[swiftSelectorKey(selector)] = selector
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func decodeSwiftLocalWrite(operation scenarios.Operation, client scenarios.NativeClient) (runnerLocalAction, runnerRowSelector, error) {
	if scenarios.OperationKey(operation) != "local/write" {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("Swift platform local operation is unsupported")
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("Swift local write payload is invalid")
	}
	var payload map[string]json.RawMessage
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("decode Swift local write payload failed")
	}
	if !swiftPayloadStringEquals(payload, "authenticated_user_id", client.UserID) || !swiftPayloadStringEquals(payload, "client_id", client.ClientID) {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("Swift local write identity does not match client")
	}
	tableName, err := swiftPayloadString(payload, "table_id")
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	operationName, err := swiftPayloadString(payload, "operation")
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	primaryKeyField, primaryKey, err := decodeSwiftPrimaryKey(payload["pk"])
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	fields, err := decodeSwiftColumns(payload["columns"])
	if err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, err
	}
	action := runnerLocalAction{
		Operation:       operationName,
		TableName:       tableName,
		PrimaryKeyField: primaryKeyField,
		PrimaryKey:      append(json.RawMessage(nil), primaryKey...),
		Fields:          fields,
	}
	if err := validateRunnerLocalAction(action); err != nil {
		return runnerLocalAction{}, runnerRowSelector{}, errors.New("Swift local write cannot map to runner action")
	}
	selector := runnerRowSelector{
		TableName:       tableName,
		PrimaryKeyField: primaryKeyField,
		PrimaryKey:      append(json.RawMessage(nil), primaryKey...),
	}
	return action, selector, nil
}

func swiftPayloadString(payload map[string]json.RawMessage, field string) (string, error) {
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

func swiftPayloadStringEquals(payload map[string]json.RawMessage, field, wanted string) bool {
	value, err := swiftPayloadString(payload, field)
	return err == nil && value == wanted
}

func decodeSwiftPrimaryKey(raw json.RawMessage) (string, json.RawMessage, error) {
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

func decodeSwiftColumns(raw json.RawMessage) (map[string]json.RawMessage, error) {
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

// Synchronize executes one public Swift synchronization call to completion.
func (p *SwiftPlatform) Synchronize(ctx context.Context, request SynchronizeRequest) (nativeexecution.SynchronizationResult, []nativeexecution.StepObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil {
		return nativeexecution.SynchronizationResult{}, nil, errors.New("Swift client is unavailable for synchronization")
	}
	if len(request.Steps) == 0 {
		return nativeexecution.SynchronizationResult{}, nil, errors.New("Swift synchronization has no covered steps")
	}
	callID := scenarios.NativeCallID("sync_" + client.databaseIdentityFingerprint[:24])
	completed, observations, err := p.runPublicCall(ctx, client, callID, request.Method)
	if err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	mapped, err := mapSwiftTransportSteps(request.Steps, observations)
	if err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	p.storeWindow(client, *client.lastWindow)
	return nativeexecution.SynchronizationResult{Completion: completed.Completion}, mapped, nil
}

func (p *SwiftPlatform) runPublicCall(ctx context.Context, client *swiftPlatformClient, callID scenarios.NativeCallID, method string) (nativeexecution.ClientCallResult, []transportObservation, error) {
	if !validRunnerCallID(string(callID)) || !validRunnerMethod(method) {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift public call is invalid")
	}
	checkpoint := client.process.transportCheckpointValue()
	started := time.Now()
	begin, err := client.process.send(ctx, runnerCommand{Operation: "begin-call", CallID: string(callID), Method: method})
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("start Swift public call: %w", err)
	}
	inFlight, err := runnerClientCallResult(begin)
	if err != nil || inFlight.CallID != callID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift public call did not enter flight")
	}
	completedResult, err := client.process.send(ctx, runnerCommand{Operation: "await-call", CallID: string(callID)})
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("await Swift public call: %w", err)
	}
	completed, err := runnerClientCallResult(completedResult)
	if err != nil || completed.CallID != callID || completed.State != "completed" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift public call did not complete")
	}
	observations, err := client.process.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client.lastWindow = &swiftOperationWindow{clientKey: client.client.Key, observations: observations, duration: time.Since(started)}
	return *completed, observations, nil
}

func mapSwiftTransportSteps(steps []StepRequest, observations []transportObservation) ([]nativeexecution.StepObservation, error) {
	if len(steps) != len(observations) {
		return nil, errors.New("Swift transport observations do not close covered steps")
	}
	mapped := make([]nativeexecution.StepObservation, len(steps))
	for index := range steps {
		observation, err := mapSwiftTransportStep(steps[index], observations[index])
		if err != nil {
			return nil, err
		}
		mapped[index] = observation
	}
	return mapped, nil
}

func mapSwiftTransportStep(step StepRequest, observation transportObservation) (nativeexecution.StepObservation, error) {
	class := transportClassForContractOperation(step.Operation.ContractOperation)
	if step.Transport != "http" || class == "" || observation.OperationClass != class {
		return nativeexecution.StepObservation{}, errors.New("Swift transport observation does not match the covered operation class")
	}
	if observation.StatusCode != 0 && (observation.StatusCode < 100 || observation.StatusCode > 599) {
		return nativeexecution.StepObservation{}, errors.New("Swift transport observation status is invalid")
	}
	disposition := "success"
	if observation.StatusCode == 0 || observation.StatusCode < 200 || observation.StatusCode >= 300 {
		disposition = "error"
	}
	if observation.StatusCode == 0 && !observation.Retryable || observation.StatusCode != 0 && observation.StatusCode < 300 && observation.StatusCode >= 200 && (observation.ErrorCode != nil || observation.Retryable) {
		return nativeexecution.StepObservation{}, errors.New("Swift transport observation failure facts are invalid")
	}
	if observation.StatusCode != 0 && (observation.StatusCode < 200 || observation.StatusCode >= 300) && (observation.ErrorCode == nil || !validTransportErrorCode(*observation.ErrorCode)) {
		return nativeexecution.StepObservation{}, errors.New("Swift transport observation does not expose a canonical failure code")
	}
	return nativeexecution.StepObservation{
		Disposition: disposition,
		Wire: &nativeexecution.WireObservation{
			HTTPStatus: observation.StatusCode,
			ErrorCode:  cloneOptionalString(observation.ErrorCode),
			Retryable:  observation.Retryable,
		},
	}, nil
}

// BeginCall starts one public call and waits at its first covered HTTP pause.
func (p *SwiftPlatform) BeginCall(ctx context.Context, request CallRequest) (nativeexecution.ClientCallResult, []nativeexecution.StepObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift client is unavailable for begin-call")
	}
	if len(request.Steps) != 1 {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift runner pause protocol requires one first covered step")
	}
	operationClass := transportClassForContractOperation(request.Steps[0].Operation.ContractOperation)
	if request.Steps[0].Transport != "http" || operationClass == "" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift runner cannot pause a non-HTTP covered step")
	}
	if !validRunnerCallID(string(request.CallID)) || !validRunnerMethod(request.Method) {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift begin-call request is invalid")
	}
	checkpoint := client.process.transportCheckpointValue()
	started := time.Now()
	if _, err := client.process.send(ctx, runnerCommand{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("arm Swift transport pause: %w", err)
	}
	begin, err := client.process.send(ctx, runnerCommand{Operation: "begin-call", CallID: string(request.CallID), Method: request.Method})
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("start paused Swift call: %w", err)
	}
	inFlight, err := runnerClientCallResult(begin)
	if err != nil || inFlight.CallID != request.CallID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Swift paused call did not enter flight")
	}
	if _, err := client.process.send(ctx, runnerCommand{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("await Swift transport pause: %w", err)
	}
	observations, err := client.process.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	mapped, err := mapSwiftTransportSteps(request.Steps, observations)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client.activeCall = &swiftPlatformCall{id: request.CallID, checkpoint: checkpoint, started: started, paused: true}
	return *inFlight, mapped, nil
}

// AwaitStep resumes a paused call and waits at the next covered HTTP pause.
func (p *SwiftPlatform) AwaitStep(ctx context.Context, request AwaitRequest) (nativeexecution.StepObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || request.CallID == nil || client.activeCall == nil || client.activeCall.id != *request.CallID || !client.activeCall.paused {
		return nativeexecution.StepObservation{}, errors.New("Swift await-step has no paused call")
	}
	operationClass := transportClassForContractOperation(request.Step.Operation.ContractOperation)
	if request.Step.Transport != "http" || operationClass == "" {
		return nativeexecution.StepObservation{}, errors.New("Swift runner cannot observe a non-HTTP step during a paused call")
	}
	checkpoint := client.process.transportCheckpointValue()
	if _, err := client.process.send(ctx, runnerCommand{Operation: "arm-transport-pause", TransportOperation: operationClass}); err != nil {
		return nativeexecution.StepObservation{}, fmt.Errorf("arm next Swift transport pause: %w", err)
	}
	if _, err := client.process.send(ctx, runnerCommand{Operation: "resume-transport-pause"}); err != nil {
		return nativeexecution.StepObservation{}, fmt.Errorf("resume Swift transport pause: %w", err)
	}
	client.activeCall.paused = false
	if _, err := client.process.send(ctx, runnerCommand{Operation: "await-transport-pause", TransportOperation: operationClass}); err != nil {
		return nativeexecution.StepObservation{}, fmt.Errorf("await next Swift transport pause: %w", err)
	}
	observations, err := client.process.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	mapped, err := mapSwiftTransportSteps([]StepRequest{request.Step}, observations)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.activeCall.paused = true
	return mapped[0], nil
}

// AwaitCall resumes a final pause, then waits for public call completion.
func (p *SwiftPlatform) AwaitCall(ctx context.Context, request CallRequest) (nativeexecution.ClientCallResult, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall == nil || client.activeCall.id != request.CallID {
		return nativeexecution.ClientCallResult{}, errors.New("Swift await-call has no active call")
	}
	active := client.activeCall
	if active.paused {
		if _, err := client.process.send(ctx, runnerCommand{Operation: "resume-transport-pause"}); err != nil {
			return nativeexecution.ClientCallResult{}, fmt.Errorf("resume final Swift transport pause: %w", err)
		}
		active.paused = false
	}
	completedResult, err := client.process.send(ctx, runnerCommand{Operation: "await-call", CallID: string(request.CallID)})
	if err != nil {
		return nativeexecution.ClientCallResult{}, fmt.Errorf("await paused Swift call: %w", err)
	}
	completed, err := runnerClientCallResult(completedResult)
	if err != nil || completed.CallID != request.CallID || completed.State != "completed" {
		return nativeexecution.ClientCallResult{}, errors.New("paused Swift call did not complete")
	}
	observations, err := client.process.transportObservationsAfter(active.checkpoint)
	if err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	p.storeWindow(client, swiftOperationWindow{clientKey: request.ClientKey, observations: observations, duration: time.Since(active.started)})
	client.activeCall = nil
	return *completed, nil
}

// Lifecycle invokes the public Swift lifecycle API.
func (p *SwiftPlatform) Lifecycle(ctx context.Context, request LifecycleRequest) error {
	if err := swiftPlatformContext(ctx); err != nil {
		return err
	}
	if !validRunnerLifecycle(request.Operation) {
		return errors.New("Swift lifecycle operation is unsupported")
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil {
		return errors.New("Swift client is unavailable for lifecycle operation")
	}
	checkpoint := client.process.transportCheckpointValue()
	started := time.Now()
	result, err := client.process.send(ctx, runnerCommand{Operation: "lifecycle", LifecycleOperation: request.Operation})
	if err != nil {
		return fmt.Errorf("run Swift lifecycle operation: %w", err)
	}
	if result.Status == nil || *result.Status == "" {
		return errors.New("Swift lifecycle operation did not return status")
	}
	observations, err := client.process.transportObservationsAfter(checkpoint)
	if err != nil {
		return err
	}
	p.storeWindow(client, swiftOperationWindow{clientKey: request.ClientKey, observations: observations, duration: time.Since(started)})
	return nil
}

// ProcessStep reports unsupported process controls instead of simulating success.
func (p *SwiftPlatform) ProcessStep(ctx context.Context, request StepRequest) (nativeexecution.StepObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	if request.ClientKey == nil {
		return nativeexecution.StepObservation{}, errors.New("Swift platform cannot execute a server process operation")
	}
	if _, err := p.client(*request.ClientKey); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	return nativeexecution.StepObservation{}, fmt.Errorf("Swift runner does not expose process operation %s", scenarios.OperationKey(request.Operation))
}

// ProcessBoundary terminates or relaunches a real Swift runner process.
func (p *SwiftPlatform) ProcessBoundary(ctx context.Context, request ProcessBoundaryRequest) (nativeexecution.ProcessBoundaryResult, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.ProcessBoundaryResult{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.ProcessBoundaryResult{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	switch request.Boundary {
	case "":
		return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift process boundary is invalid")
	case "queue-inserted", "queue-resolved", "push-reconciled", "pull-applied", "checksum-committed", "rebuild-page-applied", "provenance-pruned", "rebuild-finalized", "schema-progressed", "rejection-persisted":
	default:
		return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift process boundary is unsupported")
	}
	if request.AfterActionID == "" {
		return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift process boundary predecessor is invalid")
	}
	switch request.Operation {
	case "terminate":
		if client.terminated {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift runner process is already terminated")
		}
		if client.activeCall != nil {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift process termination cannot interrupt a call")
		}
		priorProcessID := client.processID
		if err := client.process.killSIGKILL(); err != nil {
			return nativeexecution.ProcessBoundaryResult{}, err
		}
		client.process = nil
		client.terminated = true
		return nativeexecution.ProcessBoundaryResult{
			ClientKey:                   request.ClientKey,
			Boundary:                    request.Boundary,
			AfterActionID:               string(request.AfterActionID),
			PriorProcessID:              priorProcessID,
			TerminationConfirmed:        true,
			DatabaseIdentityFingerprint: client.databaseIdentityFingerprint,
		}, nil
	case "relaunch":
		if !client.terminated {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift runner process is not terminated")
		}
		if err := requireExistingDatabase(client.databasePath); err != nil {
			return nativeexecution.ProcessBoundaryResult{}, err
		}
		priorProcessID := client.processID
		if err := p.startClientRunner(ctx, client, ""); err != nil {
			return nativeexecution.ProcessBoundaryResult{}, fmt.Errorf("relaunch Swift runner: %w", err)
		}
		if client.processID == priorProcessID {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift runner relaunch did not create a distinct process")
		}
		currentProcessID := client.processID
		return nativeexecution.ProcessBoundaryResult{
			ClientKey:                   request.ClientKey,
			Boundary:                    request.Boundary,
			AfterActionID:               string(request.AfterActionID),
			PriorProcessID:              priorProcessID,
			CurrentProcessID:            &currentProcessID,
			TerminationConfirmed:        true,
			DatabaseIdentityFingerprint: client.databaseIdentityFingerprint,
		}, nil
	default:
		return nativeexecution.ProcessBoundaryResult{}, errors.New("Swift process operation is unsupported")
	}
}

// Capture reads current runner inspection fields for each requested source.
func (p *SwiftPlatform) Capture(ctx context.Context, request CaptureRequest) ([]CaptureSourceObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nil, err
	}
	if len(request.Sources) == 0 {
		return nil, errors.New("Swift capture has no sources")
	}
	clients := make([]*swiftPlatformClient, 0, len(request.ClientKeys))
	for _, key := range request.ClientKeys {
		client, err := p.client(key)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	results := make([]swiftCaptureResult, 0, len(clients))
	for _, client := range clients {
		client.mu.Lock()
		if client.terminated {
			client.mu.Unlock()
			return nil, errors.New("Swift capture client is terminated")
		}
		result, err := client.process.send(ctx, runnerCommand{Operation: "capture", RowSelectors: swiftClientSelectors(client)})
		client.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("capture Swift runner state: %w", err)
		}
		results = append(results, swiftCaptureResult{client: client, result: result})
	}

	observations := make([]CaptureSourceObservation, 0, len(request.Sources))
	for _, source := range request.Sources {
		facts, err := swiftCaptureFactsForSource(source, results)
		if err != nil {
			return nil, err
		}
		observations = append(observations, CaptureSourceObservation{Source: source, StateFacts: facts})
	}
	return observations, nil
}

type swiftCaptureResult struct {
	client *swiftPlatformClient
	result runnerResult
}

func swiftClientSelectors(client *swiftPlatformClient) []runnerRowSelector {
	values := make([]runnerRowSelector, 0, len(client.selectors))
	for _, selector := range client.selectors {
		selector.PrimaryKey = append(json.RawMessage(nil), selector.PrimaryKey...)
		values = append(values, selector)
	}
	sort.Slice(values, func(left, right int) bool {
		return swiftSelectorKey(values[left]) < swiftSelectorKey(values[right])
	})
	return values
}

func swiftSelectorKey(selector runnerRowSelector) string {
	return selector.TableName + "\x00" + selector.PrimaryKeyField + "\x00" + string(selector.PrimaryKey)
}

func swiftCaptureFactsForSource(source string, values []swiftCaptureResult) (scenarios.StateFacts, error) {
	var facts scenarios.StateFacts
	for _, value := range values {
		clientFacts, err := swiftClientFactsForSource(source, value.client.client, value.result)
		if err != nil {
			return scenarios.StateFacts{}, err
		}
		if clientFacts != nil {
			facts.Clients = append(facts.Clients, *clientFacts)
		}
	}
	return facts, nil
}

func swiftClientFactsForSource(source string, client scenarios.NativeClient, result runnerResult) (*scenarios.ClientDurabilityFact, error) {
	facts := scenarios.ClientDurabilityFact{UserID: client.UserID, ClientID: client.ClientID}
	switch source {
	case "application-rows":
		count := uint64(len(uniqueSwiftScopeRows(result.ScopeRows)))
		facts.RowCount = &count
	case "pending-mutations":
		queue, err := swiftQueuedMutationFacts(result.RetainedMutations)
		if err != nil {
			return nil, err
		}
		count := uint64(len(queue))
		facts.QueueCount = &count
		facts.Queue = queue
	case "rejected-mutations":
		outcomes, err := swiftOutcomeFacts(result.RejectedMutations)
		if err != nil {
			return nil, err
		}
		count := uint64(len(outcomes))
		facts.OutcomeCount = &count
		facts.Outcomes = outcomes
	case "scope-state", "checkpoints":
		checkpoints, err := swiftCheckpointFacts(result.ScopeStates)
		if err != nil {
			return nil, err
		}
		count := uint64(len(checkpoints))
		facts.CheckpointCount = &count
		facts.Checkpoints = checkpoints
	case "provenance":
		count := uint64(len(uniqueSwiftScopeRows(result.ScopeRows)))
		facts.ProvenanceCount = &count
	case "rebuild-state":
		count := uint64(len(result.RebuildAttempts))
		facts.RebuildAttemptCount = &count
	case "sync-status", "sync-events", "request-trace", "process-trace":
		// These runner facts have no StateFacts representation.
		return nil, nil
	default:
		return nil, fmt.Errorf("Swift capture source %q is unsupported", source)
	}
	if result.Schema != nil {
		if result.Schema.Version <= 0 || !schemaHashPattern.MatchString(result.Schema.Hash) {
			return nil, errors.New("Swift runner schema inspection is invalid")
		}
		version := uint64(result.Schema.Version)
		facts.CurrentSchema = &scenarios.SchemaFact{Version: version, Hash: result.Schema.Hash}
	}
	return &facts, nil
}

func uniqueSwiftScopeRows(values []scopeRowRecord) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value.TableName+"\x00"+value.RecordID] = struct{}{}
	}
	return result
}

func swiftCheckpointFacts(values []scopeStateRecord) ([]scenarios.CheckpointFact, error) {
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
		verified := checksum != nil && localChecksum != nil && *checksum == *localChecksum
		result = append(result, scenarios.CheckpointFact{
			ScopeID:     value.ScopeID,
			HasCursor:   value.Cursor != nil,
			HasChecksum: checksum != nil,
			Checksum:    checksum,
			Verified:    verified,
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ScopeID < result[right].ScopeID })
	return result, nil
}

func pointerString(value string) *string {
	return &value
}

func swiftChecksumDigest(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	var object struct {
		Algorithm string `json:"algorithm"`
		Version   int    `json:"version"`
		Encoding  string `json:"encoding"`
		Digest    string `json:"digest"`
	}
	if err := json.Unmarshal([]byte(*value), &object); err != nil || object.Algorithm != "sha256" || object.Version != 1 || object.Encoding != "hex" || !validLowerHexDigest(object.Digest) {
		return nil, errors.New("Swift runner checksum inspection is invalid")
	}
	digest := object.Digest
	return &digest, nil
}

func swiftQueuedMutationFacts(values []retainedMutation) ([]scenarios.QueuedMutationFact, error) {
	result := make([]scenarios.QueuedMutationFact, 0, len(values))
	for _, value := range values {
		if value.LocalOrder < 0 || value.AuthoredSchema.Version <= 0 || !schemaHashPattern.MatchString(value.AuthoredSchema.Hash) {
			return nil, errors.New("Swift runner queued mutation inspection is invalid")
		}
		identity, err := swiftRecordIDWireJSON(value.RecordID, value.PrimaryKeyLogicalType)
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
		version := uint64(value.AuthoredSchema.Version)
		result = append(result, scenarios.QueuedMutationFact{
			MutationID:        value.MutationID,
			TableID:           value.TableID,
			CanonicalWireJSON: identity,
			AuthoredSchema:    scenarios.SchemaFact{Version: version, Hash: value.AuthoredSchema.Hash},
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

func swiftOutcomeFacts(values []retainedRejection) ([]scenarios.MutationOutcomeFact, error) {
	result := make([]scenarios.MutationOutcomeFact, 0, len(values))
	for _, value := range values {
		if value.Mutation.MutationID == "" || value.Rejection.MutationID != value.Mutation.MutationID || value.Rejection.Status == "" || value.Rejection.Code == "" {
			return nil, errors.New("Swift runner rejected mutation inspection is invalid")
		}
		result = append(result, scenarios.MutationOutcomeFact{
			MutationID: value.Mutation.MutationID,
			State:      value.Rejection.Status,
			Reason:     value.Rejection.Code,
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].MutationID < result[right].MutationID })
	return result, nil
}

func swiftRecordIDWireJSON(recordID, logicalType string) (string, error) {
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

func cloneOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// MeasureBudgets returns request counters from the most recent operation window.
func (p *SwiftPlatform) MeasureBudgets(ctx context.Context, request BudgetRequest) ([]nativeexecution.BudgetObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nil, err
	}
	window, err := p.latestWindow()
	if err != nil {
		return nil, err
	}
	measurement := swiftPerformanceMeasurement(window.observations)
	result := make([]nativeexecution.BudgetObservation, 0, len(request.Budgets))
	for _, budget := range request.Budgets {
		if budget.ID == "" || budget.Metric == "" || budget.Unit == "" {
			return nil, errors.New("Swift budget instruction is invalid")
		}
		result = append(result, nativeexecution.BudgetObservation{BudgetID: budget.ID, Measurement: measurement})
	}
	return result, nil
}

func swiftPerformanceMeasurement(observations []transportObservation) execution.PerformanceMeasurement {
	var measurement execution.PerformanceMeasurement
	for _, observation := range observations {
		switch observation.OperationClass {
		case "connect":
			measurement.RequestCounts.Connect++
		case "push":
			measurement.RequestCounts.Push++
		case "pull":
			measurement.RequestCounts.Pull++
		case "rebuild":
			measurement.RequestCounts.RebuildPage++
			if observation.StatusCode == 200 && observation.RebuildResponseFacts != nil {
				measurement.ReturnedRebuildPageCount++
			}
		case "schemas":
			measurement.RequestCounts.SchemaFetch++
		default:
			measurement.RequestCounts.Other++
		}
	}
	return measurement
}

// MeasureSample returns immediate metric values from the client operation window.
func (p *SwiftPlatform) MeasureSample(ctx context.Context, request SampleRequest) (nativeexecution.MeasurementSampleObservation, error) {
	if err := swiftPlatformContext(ctx); err != nil {
		return nativeexecution.MeasurementSampleObservation{}, err
	}
	if request.ClientKey == nil || *request.ClientKey == "" {
		return nativeexecution.MeasurementSampleObservation{}, errors.New("Swift metric sample has no client operation window")
	}
	client, err := p.client(*request.ClientKey)
	if err != nil {
		return nativeexecution.MeasurementSampleObservation{}, err
	}
	client.mu.Lock()
	window := cloneSwiftOperationWindow(client.lastWindow)
	client.mu.Unlock()
	if window == nil {
		return nativeexecution.MeasurementSampleObservation{}, errors.New("Swift metric sample has no completed operation window")
	}
	metricValues := make([]execution.MetricValue, 0, len(request.Measurement.Metrics))
	for _, metric := range request.Measurement.Metrics {
		value, err := swiftSampleMetric(metric.Name, *window)
		if err != nil {
			return nativeexecution.MeasurementSampleObservation{}, err
		}
		metricValues = append(metricValues, execution.MetricValue{MetricID: string(metric.ID), Value: value})
	}
	return nativeexecution.MeasurementSampleObservation{
		MeasurementID: request.Measurement.ID,
		StratumID:     request.Stratum.StratumID,
		SampleID:      request.SampleID,
		MetricValues:  metricValues,
	}, nil
}

func swiftSampleMetric(name string, window swiftOperationWindow) (float64, error) {
	measurement := swiftPerformanceMeasurement(window.observations)
	switch name {
	case "rebuild_page_count":
		return float64(measurement.RequestCounts.RebuildPage), nil
	case "rebuild_record_count", "rebuild_records_applied":
		var count int64
		for _, observation := range window.observations {
			if observation.OperationClass == "rebuild" && observation.StatusCode == 200 && observation.RebuildResponseFacts != nil {
				count += int64(observation.RebuildResponseFacts.RecordCount)
			}
		}
		return float64(count), nil
	case "schema_check_requests":
		return float64(measurement.RequestCounts.SchemaFetch), nil
	case "schema_check_duration":
		return swiftTransportDurationMilliseconds(window.observations, "schemas"), nil
	case "startup_sync_requests":
		return float64(swiftRequestCount(measurement)), nil
	case "startup_local_setup_duration", "queue_replay_duration", "rebuild_apply_duration":
		return float64(window.duration) / float64(time.Millisecond), nil
	default:
		return 0, fmt.Errorf("Swift runner does not expose metric %q", name)
	}
}

func swiftTransportDurationMilliseconds(observations []transportObservation, operationClass string) float64 {
	var duration uint64
	for _, observation := range observations {
		if observation.OperationClass == operationClass {
			duration += observation.DurationNanoseconds
		}
	}
	return float64(duration) / float64(time.Millisecond)
}

func swiftRequestCount(measurement execution.PerformanceMeasurement) int {
	return measurement.RequestCounts.Connect + measurement.RequestCounts.Push + measurement.RequestCounts.Pull + measurement.RequestCounts.RebuildPage + measurement.RequestCounts.SchemaFetch + measurement.RequestCounts.Other
}

func (p *SwiftPlatform) storeWindow(client *swiftPlatformClient, window swiftOperationWindow) {
	copy := cloneSwiftOperationWindow(&window)
	client.lastWindow = copy
	p.mu.Lock()
	p.lastWindow = copy
	p.mu.Unlock()
}

func (p *SwiftPlatform) latestWindow() (*swiftOperationWindow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastWindow == nil {
		return nil, errors.New("Swift budget measurement has no operation window")
	}
	return cloneSwiftOperationWindow(p.lastWindow), nil
}

func cloneSwiftOperationWindow(value *swiftOperationWindow) *swiftOperationWindow {
	if value == nil {
		return nil
	}
	copy := *value
	copy.observations = append([]transportObservation(nil), value.observations...)
	return &copy
}

// Close stops all owned Swift runner processes. It retains application databases.
func (p *SwiftPlatform) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("Swift platform close context is required")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	clients := make([]*swiftPlatformClient, 0, len(p.clients))
	for _, client := range p.clients {
		clients = append(clients, client)
	}
	p.mu.Unlock()
	var failures []error
	for _, client := range clients {
		client.mu.Lock()
		process := client.process
		client.process = nil
		client.mu.Unlock()
		if process != nil {
			if err := process.close(ctx); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (p *SwiftPlatform) client(key string) (*swiftPlatformClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("Swift platform is closed")
	}
	client, found := p.clients[key]
	if !found {
		return nil, errors.New("Swift platform client is unavailable")
	}
	return client, nil
}

func swiftPlatformContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Swift platform context is required")
	}
	return ctx.Err()
}
