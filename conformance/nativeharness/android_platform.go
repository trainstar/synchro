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
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// AndroidAuthTokenResolver resolves the token for one declared Android client.
type AndroidAuthTokenResolver func(context.Context, scenarios.NativeClient) (string, error)

// AndroidSeedDatabasePathResolver resolves one finalized production seed database.
type AndroidSeedDatabasePathResolver func(context.Context, scenarios.NativeClient, scenarios.StepID) (string, error)

// AndroidPlatformConfig configures real Android instrumentation execution.
type AndroidPlatformConfig struct {
	ADBPath                  string
	DeviceSerial             string
	ApplicationAPKPath       string
	InstrumentationAPKPath   string
	ApplicationID            string
	InstrumentationComponent string
	ServerURL                string
	AuthToken                AndroidAuthTokenResolver
	SeedDatabasePath         AndroidSeedDatabasePathResolver
	Platform                 string
	AppVersion               string
	PullPageSize             int
	TransportCapacity        int
}

// AndroidPlatform drives application-private SQLite through real instrumentation.
type AndroidPlatform struct {
	config AndroidPlatformConfig

	mu         sync.Mutex
	closed     bool
	installed  bool
	clients    map[string]*androidPlatformClient
	lastWindow *androidOperationWindow
}

type androidPlatformClient struct {
	mu sync.Mutex

	client                      scenarios.NativeClient
	databaseName                string
	databaseIdentityFingerprint string
	session                     *androidSession
	processID                   string
	terminated                  bool
	activeCall                  *androidPlatformCall
	lastWindow                  *androidOperationWindow
	selectors                   map[string]runnerRowSelector
}

type androidPlatformCall struct {
	id         scenarios.NativeCallID
	checkpoint uint64
	started    time.Time
	paused     bool
}

type androidOperationWindow struct {
	clientKey    string
	observations []transportObservation
	duration     time.Duration
}

var _ Platform = (*AndroidPlatform)(nil)

// NewAndroidPlatform creates an Android platform that fails closed when its
// device, artifacts, or transport prerequisites are unavailable.
func NewAndroidPlatform(config AndroidPlatformConfig) (*AndroidPlatform, error) {
	normalized, err := normalizeAndroidPlatformConfig(config)
	if err != nil {
		return nil, err
	}
	return &AndroidPlatform{config: normalized, clients: make(map[string]*androidPlatformClient)}, nil
}

func normalizeAndroidPlatformConfig(config AndroidPlatformConfig) (AndroidPlatformConfig, error) {
	if config.ADBPath == "" || config.DeviceSerial == "" || config.ApplicationAPKPath == "" || config.InstrumentationAPKPath == "" || config.ApplicationID == "" || config.InstrumentationComponent == "" || config.ServerURL == "" || config.AuthToken == nil || config.Platform == "" || config.AppVersion == "" {
		return AndroidPlatformConfig{}, errors.New("Android platform configuration is incomplete")
	}
	if config.Platform != "android" || len(config.ApplicationID) > 255 || len(config.InstrumentationComponent) > 512 || len(config.AppVersion) > 128 {
		return AndroidPlatformConfig{}, errors.New("Android platform configuration is invalid")
	}
	parsedURL, err := url.Parse(config.ServerURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" || parsedURL.User != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return AndroidPlatformConfig{}, errors.New("Android platform server URL is invalid")
	}
	adbPath, err := exec.LookPath(config.ADBPath)
	if err != nil {
		return AndroidPlatformConfig{}, errors.New("Android adb is unavailable")
	}
	applicationAPK, err := requireAndroidRegularFile(config.ApplicationAPKPath)
	if err != nil {
		return AndroidPlatformConfig{}, errors.New("Android application APK is unavailable")
	}
	instrumentationAPK, err := requireAndroidRegularFile(config.InstrumentationAPKPath)
	if err != nil {
		return AndroidPlatformConfig{}, errors.New("Android instrumentation APK is unavailable")
	}
	if config.PullPageSize == 0 {
		config.PullPageSize = 100
	}
	if config.PullPageSize < 1 || config.PullPageSize > 1000 {
		return AndroidPlatformConfig{}, errors.New("Android pull page size is invalid")
	}
	if config.TransportCapacity == 0 {
		config.TransportCapacity = 512
	}
	if config.TransportCapacity < 1 || config.TransportCapacity > 512 {
		return AndroidPlatformConfig{}, errors.New("Android transport capacity is invalid")
	}
	config.ADBPath = adbPath
	config.ApplicationAPKPath = applicationAPK
	config.InstrumentationAPKPath = instrumentationAPK
	return config, nil
}

func requireAndroidRegularFile(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(abs)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("file is unavailable")
	}
	return filepath.Clean(abs), nil
}

func (p *AndroidPlatform) sessionConfig() androidSessionConfig {
	return androidSessionConfig{
		ADBPath:                  p.config.ADBPath,
		DeviceSerial:             p.config.DeviceSerial,
		ApplicationID:            p.config.ApplicationID,
		InstrumentationComponent: p.config.InstrumentationComponent,
	}
}

// Open installs the instrumentation artifacts once, then creates one real client.
func (p *AndroidPlatform) Open(ctx context.Context, request OpenRequest) error {
	if err := androidPlatformContext(ctx); err != nil {
		return err
	}
	if err := validateAndroidOpenRequest(request); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("Android platform is closed")
	}
	if _, found := p.clients[request.ClientKey]; found {
		p.mu.Unlock()
		return errors.New("Android platform client is already open")
	}
	p.mu.Unlock()
	if err := p.install(ctx); err != nil {
		return err
	}

	client := &androidPlatformClient{
		client:       request.Client,
		databaseName: androidDatabaseName(request.Client.DatabaseKey),
		selectors:    make(map[string]runnerRowSelector),
	}
	seedName, err := p.stageSeed(ctx, request)
	if err != nil {
		return err
	}
	if err := p.startClientSession(ctx, client, request.DatabaseMode, seedName); err != nil {
		return err
	}
	if request.Initialization == "current" {
		if err := p.initializeCurrentDatabase(ctx, client); err != nil {
			closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = client.session.close(closeContext)
			return err
		}
		client.lastWindow = nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.session.close(closeContext)
		return errors.New("Android platform is closed")
	}
	if _, found := p.clients[request.ClientKey]; found {
		return errors.New("Android platform client is already open")
	}
	p.clients[request.ClientKey] = client
	if request.Initialization == "current" {
		p.lastWindow = nil
	}
	return nil
}

func validateAndroidOpenRequest(request OpenRequest) error {
	if request.ClientKey == "" || request.Client.Key != request.ClientKey || request.Client.ClientID == "" || request.Client.DatabaseKey == "" {
		return errors.New("Android platform open client is invalid")
	}
	switch request.Initialization {
	case "empty", "current":
		if request.DatabaseMode != "create" || request.SeedStepID != nil {
			return errors.New("Android empty or current open is invalid")
		}
	case "seed":
		if request.DatabaseMode != "reuse" || request.SeedStepID == nil || *request.SeedStepID == "" {
			return errors.New("Android seed open is invalid")
		}
	default:
		return errors.New("Android platform initialization is unsupported")
	}
	return nil
}

func androidDatabaseName(databaseKey string) string {
	digest := sha256.Sum256([]byte("synchro:android:application-database:v1\x00" + databaseKey))
	return hex.EncodeToString(digest[:]) + ".sqlite"
}

func (p *AndroidPlatform) install(ctx context.Context) error {
	p.mu.Lock()
	if p.installed {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	session := &androidSession{config: p.sessionConfig()}
	state, err := session.adb(ctx, "get-state")
	if err != nil || strings.TrimSpace(state) != "device" {
		return errors.New("Android device is unavailable")
	}
	if _, err := session.adb(ctx, "install", "-r", "-t", p.config.ApplicationAPKPath); err != nil {
		return errors.New("install Android application APK failed")
	}
	if _, err := session.adb(ctx, "install", "-r", "-t", p.config.InstrumentationAPKPath); err != nil {
		return errors.New("install Android instrumentation APK failed")
	}
	p.mu.Lock()
	p.installed = true
	p.mu.Unlock()
	return nil
}

func (p *AndroidPlatform) stageSeed(ctx context.Context, request OpenRequest) (string, error) {
	if request.Initialization != "seed" {
		return "", nil
	}
	if p.config.SeedDatabasePath == nil {
		return "", errors.New("Android staged seed resolver is unavailable")
	}
	path, err := p.config.SeedDatabasePath(ctx, request.Client, *request.SeedStepID)
	if err != nil || path == "" {
		return "", errors.New("resolve staged Android production seed failed")
	}
	path, err = requireAndroidRegularFile(path)
	if err != nil {
		return "", errors.New("staged Android production seed is unavailable")
	}
	digest := sha256.Sum256([]byte(request.Client.DatabaseKey + "\x00" + path))
	name := "synchro-seed-" + hex.EncodeToString(digest[:16]) + ".sqlite"
	remote := "/data/local/tmp/" + name
	session := &androidSession{config: p.sessionConfig()}
	if _, err := session.adb(ctx, "push", path, remote); err != nil {
		return "", errors.New("stage Android production seed failed")
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = session.adb(cleanupContext, "shell", "rm", "-f", remote)
	}()
	if _, err := session.adb(ctx, "shell", "run-as", p.config.ApplicationID, "mkdir", "-p", "files"); err != nil {
		return "", errors.New("prepare Android seed storage failed")
	}
	if _, err := session.adb(ctx, "shell", "run-as", p.config.ApplicationID, "cp", remote, "files/"+name); err != nil {
		return "", errors.New("copy Android production seed failed")
	}
	return name, nil
}

func (p *AndroidPlatform) startClientSession(ctx context.Context, client *androidPlatformClient, databaseMode, seedName string) error {
	session, err := startAndroidSession(ctx, p.sessionConfig())
	if err != nil {
		return err
	}
	token, err := p.config.AuthToken(ctx, client.client)
	if err != nil || token == "" || len(token) > 16384 {
		_ = session.close(context.Background())
		return errors.New("resolve Android client authentication failed")
	}
	command := androidSessionCommand("open")
	command["database_key"] = client.databaseName
	command["database_mode"] = databaseMode
	command["server_url"] = p.config.ServerURL
	command["auth_token"] = token
	command["client_id"] = client.client.ClientID
	command["platform"] = p.config.Platform
	command["app_version"] = p.config.AppVersion
	command["pull_page_size"] = p.config.PullPageSize
	command["transport_capacity"] = p.config.TransportCapacity
	if seedName != "" {
		command["seed_database_name"] = seedName
	}
	result, err := session.send(ctx, command)
	if err != nil {
		_ = session.close(context.Background())
		return fmt.Errorf("open Android instrumentation client: %w", err)
	}
	processID, err := androidSessionProcessID(result)
	if err != nil {
		_ = session.close(context.Background())
		return err
	}
	client.session = session
	client.processID = processID
	client.databaseIdentityFingerprint = result.DatabaseIdentityFingerprint
	client.terminated = false
	return nil
}

func (p *AndroidPlatform) initializeCurrentDatabase(ctx context.Context, client *androidPlatformClient) error {
	callID := scenarios.NativeCallID("open_" + client.databaseIdentityFingerprint[:24])
	completed, _, err := p.runPublicCall(ctx, client, callID, "start")
	if err != nil {
		return fmt.Errorf("initialize current Android database: %w", err)
	}
	if completed.Completion != "idle" {
		return errors.New("current Android database initialization did not reach idle")
	}
	result, err := client.session.send(ctx, func() map[string]any {
		command := androidSessionCommand("lifecycle")
		command["lifecycle_operation"] = "stop"
		return command
	}())
	if err != nil || result.Status == nil || *result.Status == "" {
		return errors.New("stop current Android database initialization failed")
	}
	return nil
}

// LocalAction executes one authorized application write in real Android SQLite.
func (p *AndroidPlatform) LocalAction(ctx context.Context, request LocalActionRequest) (nativeexecution.StepObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil || client.session == nil {
		return nativeexecution.StepObservation{}, errors.New("Android client is unavailable for a local action")
	}
	payload, selector, err := decodeSwiftLocalWrite(request.Operation, client.client)
	if err != nil {
		return nativeexecution.StepObservation{}, errors.New("Android local write payload is invalid")
	}
	action, err := androidLocalAction(payload)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	checkpoint := client.session.transportCheckpointValue()
	started := time.Now()
	command := androidSessionCommand("local-action")
	command["local_action"] = action
	result, err := client.session.send(ctx, command)
	if err != nil {
		return nativeexecution.StepObservation{}, fmt.Errorf("execute Android local action: %w", err)
	}
	if result.RowsAffected == nil || *result.RowsAffected != 1 {
		return nativeexecution.StepObservation{}, errors.New("Android local action did not affect one row")
	}
	observations, err := client.session.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	p.storeWindow(client, androidOperationWindow{clientKey: request.ClientKey, observations: observations, duration: time.Since(started)})
	client.selectors[androidSelectorKey(selector)] = selector
	return nativeexecution.StepObservation{Disposition: "success"}, nil
}

func androidLocalAction(action runnerLocalAction) (map[string]any, error) {
	primaryKey, err := androidSessionValue(action.PrimaryKey)
	if err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(action.Fields))
	for name, raw := range action.Fields {
		value, err := androidSessionValue(raw)
		if err != nil {
			return nil, err
		}
		fields[name] = value
	}
	return map[string]any{
		"operation":         action.Operation,
		"table_name":        action.TableName,
		"primary_key_field": action.PrimaryKeyField,
		"primary_key":       primaryKey,
		"fields":            fields,
	}, nil
}

// Synchronize executes a real public Kotlin synchronization call.
func (p *AndroidPlatform) Synchronize(ctx context.Context, request SynchronizeRequest) (nativeexecution.SynchronizationResult, []nativeexecution.StepObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil || client.session == nil || len(request.Steps) == 0 {
		return nativeexecution.SynchronizationResult{}, nil, errors.New("Android client is unavailable for synchronization")
	}
	callID := scenarios.NativeCallID("sync_" + client.databaseIdentityFingerprint[:24])
	completed, observations, err := p.runPublicCall(ctx, client, callID, request.Method)
	if err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	mapped, err := mapAndroidTransportSteps(request.Steps, observations)
	if err != nil {
		return nativeexecution.SynchronizationResult{}, nil, err
	}
	return nativeexecution.SynchronizationResult{Completion: completed.Completion}, mapped, nil
}

func (p *AndroidPlatform) runPublicCall(ctx context.Context, client *androidPlatformClient, callID scenarios.NativeCallID, method string) (nativeexecution.ClientCallResult, []transportObservation, error) {
	if !validRunnerCallID(string(callID)) || !validRunnerMethod(method) {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Android public call is invalid")
	}
	checkpoint := client.session.transportCheckpointValue()
	started := time.Now()
	begin := androidSessionCommand("begin-call")
	begin["call_id"] = string(callID)
	begin["method"] = method
	inFlightResult, err := client.session.send(ctx, begin)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("start Android public call: %w", err)
	}
	inFlight, err := androidClientCallResult(inFlightResult)
	if err != nil || inFlight.CallID != callID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Android public call did not enter flight")
	}
	await := androidSessionCommand("await-call")
	await["call_id"] = string(callID)
	completedResult, err := client.session.send(ctx, await)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, fmt.Errorf("await Android public call: %w", err)
	}
	completed, err := androidClientCallResult(completedResult)
	if err != nil || completed.CallID != callID || completed.State != "completed" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Android public call did not complete")
	}
	observations, err := client.session.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	p.storeWindow(client, androidOperationWindow{clientKey: client.client.Key, observations: observations, duration: time.Since(started)})
	return completed, observations, nil
}

func androidClientCallResult(result androidSessionResult) (nativeexecution.ClientCallResult, error) {
	if result.CallID == nil || result.State == nil || *result.CallID == "" || *result.State == "" {
		return nativeexecution.ClientCallResult{}, errors.New("Android client call result is incomplete")
	}
	completion := ""
	if result.Completion != nil {
		completion = *result.Completion
	}
	return nativeexecution.ClientCallResult{CallID: scenarios.NativeCallID(*result.CallID), State: *result.State, Completion: completion}, nil
}

func mapAndroidTransportSteps(steps []StepRequest, observations []transportObservation) ([]nativeexecution.StepObservation, error) {
	if len(steps) != len(observations) {
		return nil, errors.New("Android transport observations do not close covered steps")
	}
	result := make([]nativeexecution.StepObservation, len(steps))
	for index := range steps {
		class := transportClassForContractOperation(steps[index].Operation.ContractOperation)
		observation := observations[index]
		if steps[index].Transport != "http" || class == "" || observation.OperationClass != class {
			return nil, errors.New("Android transport observation does not match the covered operation")
		}
		if observation.StatusCode < 200 || observation.StatusCode >= 300 {
			return nil, errors.New("Android runner does not expose canonical failure facts for this transport result")
		}
		result[index] = nativeexecution.StepObservation{Disposition: "success", Wire: &nativeexecution.WireObservation{HTTPStatus: observation.StatusCode, Retryable: false}}
	}
	return result, nil
}

// BeginCall starts a public call and pauses it at the first actual HTTP request.
func (p *AndroidPlatform) BeginCall(ctx context.Context, request CallRequest) (nativeexecution.ClientCallResult, []nativeexecution.StepObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil || client.session == nil || len(request.Steps) != 1 {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Android client is unavailable for begin-call")
	}
	operationClass := transportClassForContractOperation(request.Steps[0].Operation.ContractOperation)
	if request.Steps[0].Transport != "http" || operationClass == "" || !validRunnerCallID(string(request.CallID)) || !validRunnerMethod(request.Method) {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Android begin-call request is invalid")
	}
	checkpoint := client.session.transportCheckpointValue()
	started := time.Now()
	if err := androidArmTransportPause(ctx, client.session, operationClass); err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	begin := androidSessionCommand("begin-call")
	begin["call_id"] = string(request.CallID)
	begin["method"] = request.Method
	result, err := client.session.send(ctx, begin)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	inFlight, err := androidClientCallResult(result)
	if err != nil || inFlight.CallID != request.CallID || inFlight.State != "in_flight" || inFlight.Completion != "" {
		return nativeexecution.ClientCallResult{}, nil, errors.New("Android paused call did not enter flight")
	}
	if err := androidAwaitTransportPause(ctx, client.session, operationClass); err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	observations, err := client.session.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	mapped, err := mapAndroidTransportSteps(request.Steps, observations)
	if err != nil {
		return nativeexecution.ClientCallResult{}, nil, err
	}
	client.activeCall = &androidPlatformCall{id: request.CallID, checkpoint: checkpoint, started: started, paused: true}
	return inFlight, mapped, nil
}

// AwaitStep resumes a real paused request and pauses the next covered request.
func (p *AndroidPlatform) AwaitStep(ctx context.Context, request AwaitRequest) (nativeexecution.StepObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.session == nil || request.CallID == nil || client.activeCall == nil || client.activeCall.id != *request.CallID || !client.activeCall.paused {
		return nativeexecution.StepObservation{}, errors.New("Android await-step has no paused call")
	}
	operationClass := transportClassForContractOperation(request.Step.Operation.ContractOperation)
	if request.Step.Transport != "http" || operationClass == "" {
		return nativeexecution.StepObservation{}, errors.New("Android await-step is invalid")
	}
	checkpoint := client.session.transportCheckpointValue()
	if err := androidArmTransportPause(ctx, client.session, operationClass); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	if err := androidResumeTransportPause(ctx, client.session); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.activeCall.paused = false
	if err := androidAwaitTransportPause(ctx, client.session, operationClass); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	observations, err := client.session.transportObservationsAfter(checkpoint)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	mapped, err := mapAndroidTransportSteps([]StepRequest{request.Step}, observations)
	if err != nil {
		return nativeexecution.StepObservation{}, err
	}
	client.activeCall.paused = true
	return mapped[0], nil
}

// AwaitCall resumes the final real HTTP pause and waits for the public call.
func (p *AndroidPlatform) AwaitCall(ctx context.Context, request CallRequest) (nativeexecution.ClientCallResult, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.session == nil || client.activeCall == nil || client.activeCall.id != request.CallID {
		return nativeexecution.ClientCallResult{}, errors.New("Android await-call has no active call")
	}
	active := client.activeCall
	if active.paused {
		if err := androidResumeTransportPause(ctx, client.session); err != nil {
			return nativeexecution.ClientCallResult{}, err
		}
		active.paused = false
	}
	command := androidSessionCommand("await-call")
	command["call_id"] = string(request.CallID)
	result, err := client.session.send(ctx, command)
	if err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	completed, err := androidClientCallResult(result)
	if err != nil || completed.CallID != request.CallID || completed.State != "completed" {
		return nativeexecution.ClientCallResult{}, errors.New("Android paused call did not complete")
	}
	observations, err := client.session.transportObservationsAfter(active.checkpoint)
	if err != nil {
		return nativeexecution.ClientCallResult{}, err
	}
	p.storeWindow(client, androidOperationWindow{clientKey: request.ClientKey, observations: observations, duration: time.Since(active.started)})
	client.activeCall = nil
	return completed, nil
}

func androidArmTransportPause(ctx context.Context, session *androidSession, operation string) error {
	command := androidSessionCommand("arm-transport-pause")
	command["transport_operation"] = operation
	if _, err := session.send(ctx, command); err != nil {
		return fmt.Errorf("arm Android transport pause: %w", err)
	}
	return nil
}

func androidAwaitTransportPause(ctx context.Context, session *androidSession, operation string) error {
	command := androidSessionCommand("await-transport-pause")
	command["transport_operation"] = operation
	if _, err := session.send(ctx, command); err != nil {
		return fmt.Errorf("await Android transport pause: %w", err)
	}
	return nil
}

func androidResumeTransportPause(ctx context.Context, session *androidSession) error {
	if _, err := session.send(ctx, androidSessionCommand("resume-transport-pause")); err != nil {
		return fmt.Errorf("resume Android transport pause: %w", err)
	}
	return nil
}

// Lifecycle invokes one public Kotlin lifecycle API.
func (p *AndroidPlatform) Lifecycle(ctx context.Context, request LifecycleRequest) error {
	if err := androidPlatformContext(ctx); err != nil {
		return err
	}
	if !validRunnerLifecycle(request.Operation) {
		return errors.New("Android lifecycle operation is unsupported")
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.terminated || client.activeCall != nil || client.session == nil {
		return errors.New("Android client is unavailable for lifecycle operation")
	}
	checkpoint := client.session.transportCheckpointValue()
	started := time.Now()
	command := androidSessionCommand("lifecycle")
	command["lifecycle_operation"] = request.Operation
	result, err := client.session.send(ctx, command)
	if err != nil || result.Status == nil || *result.Status == "" {
		return errors.New("Android lifecycle operation failed")
	}
	observations, err := client.session.transportObservationsAfter(checkpoint)
	if err != nil {
		return err
	}
	p.storeWindow(client, androidOperationWindow{clientKey: request.ClientKey, observations: observations, duration: time.Since(started)})
	return nil
}

// ProcessStep fails closed because the Android runner does not expose synthetic process controls.
func (p *AndroidPlatform) ProcessStep(ctx context.Context, request StepRequest) (nativeexecution.StepObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	if request.ClientKey == nil {
		return nativeexecution.StepObservation{}, errors.New("Android platform cannot execute a server process operation")
	}
	if _, err := p.client(*request.ClientKey); err != nil {
		return nativeexecution.StepObservation{}, err
	}
	return nativeexecution.StepObservation{}, fmt.Errorf("Android runner does not expose process operation %s", scenarios.OperationKey(request.Operation))
}

// ProcessBoundary kills and relaunches real instrumentation processes.
func (p *AndroidPlatform) ProcessBoundary(ctx context.Context, request ProcessBoundaryRequest) (nativeexecution.ProcessBoundaryResult, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.ProcessBoundaryResult{}, err
	}
	if request.AfterActionID == "" || !validAndroidBoundary(request.Boundary) {
		return nativeexecution.ProcessBoundaryResult{}, errors.New("Android process boundary is invalid")
	}
	client, err := p.client(request.ClientKey)
	if err != nil {
		return nativeexecution.ProcessBoundaryResult{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	switch request.Operation {
	case "terminate":
		if client.terminated || client.activeCall != nil || client.session == nil {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Android process termination is unavailable")
		}
		prior := client.processID
		if err := client.session.kill(ctx, prior); err != nil {
			return nativeexecution.ProcessBoundaryResult{}, err
		}
		waitContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := client.session.waitForExit(waitContext); err != nil {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Android process termination is not confirmed")
		}
		client.session = nil
		client.terminated = true
		return nativeexecution.ProcessBoundaryResult{ClientKey: request.ClientKey, Boundary: request.Boundary, AfterActionID: string(request.AfterActionID), PriorProcessID: prior, TerminationConfirmed: true, DatabaseIdentityFingerprint: client.databaseIdentityFingerprint}, nil
	case "relaunch":
		if !client.terminated {
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Android process is not terminated")
		}
		prior := client.processID
		fingerprint := client.databaseIdentityFingerprint
		if err := p.startClientSession(ctx, client, "existing", ""); err != nil {
			return nativeexecution.ProcessBoundaryResult{}, fmt.Errorf("relaunch Android instrumentation: %w", err)
		}
		if client.processID == prior || client.databaseIdentityFingerprint != fingerprint {
			closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = client.session.close(closeContext)
			client.session = nil
			client.terminated = true
			return nativeexecution.ProcessBoundaryResult{}, errors.New("Android relaunch did not create a distinct process")
		}
		return nativeexecution.ProcessBoundaryResult{ClientKey: request.ClientKey, Boundary: request.Boundary, AfterActionID: string(request.AfterActionID), PriorProcessID: prior, CurrentProcessID: &client.processID, TerminationConfirmed: true, DatabaseIdentityFingerprint: client.databaseIdentityFingerprint}, nil
	default:
		return nativeexecution.ProcessBoundaryResult{}, errors.New("Android process operation is unsupported")
	}
}

func validAndroidBoundary(value string) bool {
	switch value {
	case "queue-inserted", "queue-resolved", "push-reconciled", "pull-applied", "checksum-committed", "rebuild-page-applied", "provenance-pruned", "rebuild-finalized", "schema-progressed", "rejection-persisted":
		return true
	default:
		return false
	}
}

// Capture obtains bounded facts through Kotlin public inspection APIs.
func (p *AndroidPlatform) Capture(ctx context.Context, request CaptureRequest) ([]CaptureSourceObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nil, err
	}
	if len(request.Sources) == 0 {
		return nil, errors.New("Android capture has no sources")
	}
	results := make([]androidCaptureResult, 0, len(request.ClientKeys))
	for _, key := range request.ClientKeys {
		client, err := p.client(key)
		if err != nil {
			return nil, err
		}
		client.mu.Lock()
		if client.terminated || client.session == nil {
			client.mu.Unlock()
			return nil, errors.New("Android capture client is unavailable")
		}
		selectors, err := androidSelectors(client)
		if err == nil {
			result, sendErr := client.session.send(ctx, androidCaptureCommand(selectors))
			err = sendErr
			if err == nil {
				results = append(results, androidCaptureResult{client: client, result: result})
			}
		}
		client.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("capture Android runner state: %w", err)
		}
	}
	observations := make([]CaptureSourceObservation, 0, len(request.Sources))
	for _, source := range request.Sources {
		facts, err := androidCaptureFactsForSource(source, results)
		if err != nil {
			return nil, err
		}
		observations = append(observations, CaptureSourceObservation{Source: source, StateFacts: facts})
	}
	return observations, nil
}

type androidCaptureResult struct {
	client *androidPlatformClient
	result androidSessionResult
}

func androidCaptureCommand(selectors []map[string]any) map[string]any {
	command := androidSessionCommand("capture")
	command["row_selectors"] = selectors
	return command
}

func androidSelectors(client *androidPlatformClient) ([]map[string]any, error) {
	values := make([]runnerRowSelector, 0, len(client.selectors))
	for _, value := range client.selectors {
		value.PrimaryKey = append(json.RawMessage(nil), value.PrimaryKey...)
		values = append(values, value)
	}
	sort.Slice(values, func(left, right int) bool {
		return androidSelectorKey(values[left]) < androidSelectorKey(values[right])
	})
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		primaryKey, err := androidSessionValue(value.PrimaryKey)
		if err != nil {
			return nil, err
		}
		result = append(result, map[string]any{"table_name": value.TableName, "primary_key_field": value.PrimaryKeyField, "primary_key": primaryKey})
	}
	return result, nil
}

func androidSelectorKey(value runnerRowSelector) string {
	return value.TableName + "\x00" + value.PrimaryKeyField + "\x00" + string(value.PrimaryKey)
}

func androidCaptureFactsForSource(source string, values []androidCaptureResult) (scenarios.StateFacts, error) {
	var facts scenarios.StateFacts
	for _, value := range values {
		clientFacts, err := androidClientFactsForSource(source, value.client.client, value.result)
		if err != nil {
			return scenarios.StateFacts{}, err
		}
		if clientFacts != nil {
			facts.Clients = append(facts.Clients, *clientFacts)
		}
	}
	return facts, nil
}

func androidClientFactsForSource(source string, client scenarios.NativeClient, result androidSessionResult) (*scenarios.ClientDurabilityFact, error) {
	facts := scenarios.ClientDurabilityFact{UserID: client.UserID, ClientID: client.ClientID}
	switch source {
	case "application-rows":
		count := uint64(len(androidUniqueScopeRows(result.ScopeRows)))
		facts.RowCount = &count
	case "pending-mutations":
		queue, err := androidQueuedMutationFacts(result.RetainedMutations)
		if err != nil {
			return nil, err
		}
		count := uint64(len(queue))
		facts.QueueCount = &count
		facts.Queue = queue
	case "rejected-mutations":
		outcomes, err := androidOutcomeFacts(result.RejectedMutations)
		if err != nil {
			return nil, err
		}
		count := uint64(len(outcomes))
		facts.OutcomeCount = &count
		facts.Outcomes = outcomes
	case "scope-state", "checkpoints":
		checkpoints, err := androidCheckpointFacts(result.Checkpoints)
		if err != nil {
			return nil, err
		}
		count := uint64(len(checkpoints))
		facts.CheckpointCount = &count
		facts.Checkpoints = checkpoints
	case "provenance":
		count := uint64(len(result.Provenance))
		facts.ProvenanceCount = &count
	case "rebuild-state":
		count := uint64(len(result.RebuildAttempts))
		facts.RebuildAttemptCount = &count
	case "sync-status", "sync-events", "request-trace", "process-trace":
		return nil, nil
	default:
		return nil, fmt.Errorf("Android capture source %q is unsupported", source)
	}
	if result.Schema != nil {
		if result.Schema.Version <= 0 || !schemaHashPattern.MatchString(result.Schema.Hash) {
			return nil, errors.New("Android schema inspection is invalid")
		}
		version := uint64(result.Schema.Version)
		facts.CurrentSchema = &scenarios.SchemaFact{Version: version, Hash: result.Schema.Hash}
	}
	return &facts, nil
}

func androidUniqueScopeRows(values []scopeRowRecord) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value.TableName+"\x00"+value.RecordID] = struct{}{}
	}
	return result
}

func androidCheckpointFacts(values []scopeStateRecord) ([]scenarios.CheckpointFact, error) {
	result := make([]scenarios.CheckpointFact, 0, len(values))
	for _, value := range values {
		checksum, err := androidChecksumDigest(value.Checksum)
		if err != nil {
			return nil, err
		}
		var localChecksum *string
		if value.LocalChecksum != "" {
			localChecksum, err = androidChecksumDigest(pointerString(value.LocalChecksum))
			if err != nil {
				return nil, err
			}
		}
		result = append(result, scenarios.CheckpointFact{ScopeID: value.ScopeID, HasCursor: value.Cursor != nil, HasChecksum: checksum != nil, Checksum: checksum, Verified: checksum != nil && localChecksum != nil && *checksum == *localChecksum})
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
	if json.Unmarshal([]byte(*value), &object) != nil || object.Algorithm != "sha256" || object.Version != 1 || object.Encoding != "hex" || !validLowerHexDigest(object.Digest) {
		return nil, errors.New("Android checksum inspection is invalid")
	}
	digest := object.Digest
	return &digest, nil
}

func androidQueuedMutationFacts(values []retainedMutation) ([]scenarios.QueuedMutationFact, error) {
	result := make([]scenarios.QueuedMutationFact, 0, len(values))
	for _, value := range values {
		if value.LocalOrder < 0 || value.AuthoredSchema.Version <= 0 || !schemaHashPattern.MatchString(value.AuthoredSchema.Hash) {
			return nil, errors.New("Android queued mutation inspection is invalid")
		}
		identity, err := androidRecordIDWireJSON(value.RecordID, value.PrimaryKeyLogicalType)
		if err != nil {
			return nil, err
		}
		columns := make([]scenarios.FieldFact, 0, len(value.AuthoredFields))
		for _, field := range value.AuthoredFields {
			if field.FieldID == "" || field.LogicalType == "" || !json.Valid(field.Value) {
				return nil, errors.New("Android queued mutation field is invalid")
			}
			columns = append(columns, scenarios.FieldFact{FieldID: field.FieldID, Type: field.LogicalType, WireJSON: string(field.Value)})
		}
		sort.Slice(columns, func(left, right int) bool { return columns[left].FieldID < columns[right].FieldID })
		version := uint64(value.AuthoredSchema.Version)
		result = append(result, scenarios.QueuedMutationFact{MutationID: value.MutationID, TableID: value.TableID, CanonicalWireJSON: identity, AuthoredSchema: scenarios.SchemaFact{Version: version, Hash: value.AuthoredSchema.Hash}, Operation: value.Operation, BaseVersion: cloneOptionalString(value.BaseVersion), ClientVersion: value.ClientVersion, AuthoredColumns: columns, LocalOrder: uint64(value.LocalOrder), Status: value.Status})
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].LocalOrder != result[right].LocalOrder {
			return result[left].LocalOrder < result[right].LocalOrder
		}
		return result[left].MutationID < result[right].MutationID
	})
	return result, nil
}

func androidOutcomeFacts(values []androidRejectedMutation) ([]scenarios.MutationOutcomeFact, error) {
	result := make([]scenarios.MutationOutcomeFact, 0, len(values))
	for _, value := range values {
		if value.MutationID == "" || value.Status == "" || value.Code == "" {
			return nil, errors.New("Android rejected mutation inspection is invalid")
		}
		result = append(result, scenarios.MutationOutcomeFact{MutationID: value.MutationID, State: value.Status, Reason: value.Code})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].MutationID < result[right].MutationID })
	return result, nil
}

func androidRecordIDWireJSON(recordID, logicalType string) (string, error) {
	switch logicalType {
	case "string", "decimal", "datetime", "date", "time", "json":
		value, err := json.Marshal(recordID)
		if err != nil {
			return "", errors.New("encode Android record identity failed")
		}
		return string(value), nil
	case "int", "int64":
		parsed, err := strconv.ParseInt(recordID, 10, 64)
		if err != nil || strconv.FormatInt(parsed, 10) != recordID {
			return "", errors.New("Android integer record identity is invalid")
		}
		return recordID, nil
	default:
		return "", errors.New("Android primary-key type has no conformance identity mapping")
	}
}

// MeasureBudgets returns counters from the latest real operation window.
func (p *AndroidPlatform) MeasureBudgets(ctx context.Context, request BudgetRequest) ([]nativeexecution.BudgetObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nil, err
	}
	window, err := p.latestWindow()
	if err != nil {
		return nil, err
	}
	measurement := androidPerformanceMeasurement(window.observations)
	result := make([]nativeexecution.BudgetObservation, 0, len(request.Budgets))
	for _, budget := range request.Budgets {
		if budget.ID == "" || budget.Metric == "" || budget.Unit == "" {
			return nil, errors.New("Android budget instruction is invalid")
		}
		result = append(result, nativeexecution.BudgetObservation{BudgetID: budget.ID, Measurement: measurement})
	}
	return result, nil
}

func androidPerformanceMeasurement(observations []transportObservation) execution.PerformanceMeasurement {
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

// MeasureSample returns immediate values from the completed real operation window.
func (p *AndroidPlatform) MeasureSample(ctx context.Context, request SampleRequest) (nativeexecution.MeasurementSampleObservation, error) {
	if err := androidPlatformContext(ctx); err != nil {
		return nativeexecution.MeasurementSampleObservation{}, err
	}
	if request.ClientKey == nil || *request.ClientKey == "" {
		return nativeexecution.MeasurementSampleObservation{}, errors.New("Android metric sample has no client operation window")
	}
	client, err := p.client(*request.ClientKey)
	if err != nil {
		return nativeexecution.MeasurementSampleObservation{}, err
	}
	client.mu.Lock()
	window := cloneAndroidOperationWindow(client.lastWindow)
	client.mu.Unlock()
	if window == nil {
		return nativeexecution.MeasurementSampleObservation{}, errors.New("Android metric sample has no completed operation window")
	}
	values := make([]execution.MetricValue, 0, len(request.Measurement.Metrics))
	for _, metric := range request.Measurement.Metrics {
		value, err := androidSampleMetric(metric.Name, *window)
		if err != nil {
			return nativeexecution.MeasurementSampleObservation{}, err
		}
		values = append(values, execution.MetricValue{MetricID: string(metric.ID), Value: value})
	}
	return nativeexecution.MeasurementSampleObservation{MeasurementID: request.Measurement.ID, StratumID: request.Stratum.StratumID, SampleID: request.SampleID, MetricValues: values}, nil
}

func androidSampleMetric(name string, window androidOperationWindow) (float64, error) {
	measurement := androidPerformanceMeasurement(window.observations)
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
		return androidTransportDurationMilliseconds(window.observations, "schemas"), nil
	case "startup_sync_requests":
		return float64(androidRequestCount(measurement)), nil
	case "startup_local_setup_duration", "queue_replay_duration", "rebuild_apply_duration":
		return float64(window.duration) / float64(time.Millisecond), nil
	default:
		return 0, fmt.Errorf("Android runner does not expose metric %q", name)
	}
}

func androidTransportDurationMilliseconds(observations []transportObservation, operation string) float64 {
	var duration uint64
	for _, observation := range observations {
		if observation.OperationClass == operation {
			duration += observation.DurationNanoseconds
		}
	}
	return float64(duration) / float64(time.Millisecond)
}

func androidRequestCount(measurement execution.PerformanceMeasurement) int {
	return measurement.RequestCounts.Connect + measurement.RequestCounts.Push + measurement.RequestCounts.Pull + measurement.RequestCounts.RebuildPage + measurement.RequestCounts.SchemaFetch + measurement.RequestCounts.Other
}

func (p *AndroidPlatform) storeWindow(client *androidPlatformClient, window androidOperationWindow) {
	copy := cloneAndroidOperationWindow(&window)
	client.lastWindow = copy
	p.mu.Lock()
	p.lastWindow = copy
	p.mu.Unlock()
}

func (p *AndroidPlatform) latestWindow() (*androidOperationWindow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastWindow == nil {
		return nil, errors.New("Android budget measurement has no operation window")
	}
	return cloneAndroidOperationWindow(p.lastWindow), nil
}

func cloneAndroidOperationWindow(value *androidOperationWindow) *androidOperationWindow {
	if value == nil {
		return nil
	}
	copy := *value
	copy.observations = append([]transportObservation(nil), value.observations...)
	return &copy
}

// Close stops every owned instrumentation process and preserves application databases.
func (p *AndroidPlatform) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("Android platform close context is required")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	clients := make([]*androidPlatformClient, 0, len(p.clients))
	for _, client := range p.clients {
		clients = append(clients, client)
	}
	p.mu.Unlock()
	var failures []error
	for _, client := range clients {
		client.mu.Lock()
		session := client.session
		client.session = nil
		client.mu.Unlock()
		if session != nil {
			if err := session.close(ctx); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (p *AndroidPlatform) client(key string) (*androidPlatformClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("Android platform is closed")
	}
	client, found := p.clients[key]
	if !found {
		return nil, errors.New("Android platform client is unavailable")
	}
	return client, nil
}

func androidPlatformContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Android platform context is required")
	}
	return ctx.Err()
}
