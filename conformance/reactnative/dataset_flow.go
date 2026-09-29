package reactnative

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/dataset"
)

// DatasetCoordinator serves the authored dataset flow to one React Native app.
// The flow runs on the host. Each platform call becomes one device command.
type DatasetCoordinator struct {
	harness  *blackbox.Harness
	adapter  string
	token    string
	listener net.Listener
	server   *http.Server
	commands chan *conformanceCommand
	results  chan json.RawMessage
	done     chan error

	runtimes map[string]conformanceRuntime
	active   string
	started  bool

	mu       sync.Mutex
	nextSeq  uint64
	finished bool
	failed   error
}

var _ dataset.NativePlatform = (*DatasetCoordinator)(nil)

// NewDatasetCoordinator creates an authenticated loopback listener.
func NewDatasetCoordinator(harness *blackbox.Harness, platform string) (*DatasetCoordinator, error) {
	if harness == nil || (platform != "ios" && platform != "android") {
		return nil, errors.New("React Native dataset coordinator configuration is invalid")
	}
	adapter, err := nativeAdapterURL(harness.AdapterURL(), platform)
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native dataset coordinator capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native dataset coordinator")
	}
	coordinator := &DatasetCoordinator{
		harness: harness, adapter: adapter, token: token, listener: listener,
		commands: make(chan *conformanceCommand), results: make(chan json.RawMessage), done: make(chan error, 1),
		runtimes: map[string]conformanceRuntime{}, nextSeq: 1,
	}
	coordinator.server = &http.Server{Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	return coordinator, nil
}

// Start serves the device and runs the authored flow against source.
func (c *DatasetCoordinator) Start(ctx context.Context, source *sql.DB, logf func(string, ...any)) {
	go func() { _ = c.server.Serve(c.listener) }()
	go func() { c.done <- dataset.RunNativeFlow(ctx, source, c, logf) }()
}

// URL returns the loopback coordinator origin.
func (c *DatasetCoordinator) URL() string { return "http://" + c.listener.Addr().String() }

// Token returns the per-run bearer capability.
func (c *DatasetCoordinator) Token() string { return c.token }

// Result returns the flow outcome after the device reaches completion.
func (c *DatasetCoordinator) Result() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished {
		select {
		case err := <-c.done:
			c.finished, c.failed = true, err
		default:
			return errors.New("React Native dataset flow has not completed")
		}
	}
	return c.failed
}

// Close stops the listener and every open device connection.
func (c *DatasetCoordinator) Close() error {
	return c.server.Close()
}

func (c *DatasetCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/exchange" || request.Method != http.MethodPost {
		writeExchangeError(writer, http.StatusNotFound)
		return
	}
	if !validBearer(request.Header.Get("Authorization"), c.token) {
		writeExchangeError(writer, http.StatusUnauthorized)
		return
	}
	body, err := ioReadAll(request)
	if err != nil || len(body) > maximumExchangeBytes || request.Header.Get("Content-Type") != "application/json" {
		writeExchangeError(writer, http.StatusUnsupportedMediaType)
		return
	}
	exchange, err := decodeExchangeRequest(body)
	if err != nil {
		writeExchangeError(writer, http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || exchange.Sequence != c.nextSeq || (exchange.Sequence == 1) != isJSONNull(exchange.Result) {
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if exchange.Sequence > 1 {
		select {
		case c.results <- exchange.Result:
		case <-request.Context().Done():
			return
		}
	}
	response := exchangeResponse{SchemaVersion: 1, Sequence: exchange.Sequence, State: "command"}
	select {
	case response.Command = <-c.commands:
	case err := <-c.done:
		c.finished, c.failed = true, err
		if err != nil {
			writeExchangeError(writer, http.StatusUnprocessableEntity)
			return
		}
		response.State = "complete"
	case <-request.Context().Done():
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

// execute sends one command to the device and returns its passed result.
func (c *DatasetCoordinator) execute(ctx context.Context, key, actor, name string, parameters map[string]any, steps []conformanceStep) (json.RawMessage, error) {
	command := &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: c.runtimes[key]}
	select {
	case c.commands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var raw json.RawMessage
	select {
	case raw = <-c.results:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil {
		return nil, err
	}
	if envelope.Outcome != "passed" {
		detail := ""
		if envelope.ErrorDetail != nil {
			detail = *envelope.ErrorDetail
		}
		return nil, fmt.Errorf("React Native %s/%s failed: %s %s", actor, name, *envelope.ErrorCode, detail)
	}
	// The bridge holds one active client. A command for another client
	// closes the active one, and the reopened client is not started.
	if c.active != key {
		c.active, c.started = key, false
	}
	return envelope.Result, nil
}

// Open creates a fresh React Native client and synchronizes it to idle.
func (c *DatasetCoordinator) Open(ctx context.Context, key, user string) error {
	token, err := c.harness.NativeBearerToken(ctx, user, time.Now())
	if err != nil {
		return errors.New("mint React Native dataset adapter bearer token")
	}
	c.runtimes[key] = conformanceRuntime{ClientKey: key, Database: "rn-" + key + ".db", ClientID: key, ServerURL: c.adapter, AuthToken: token}
	raw, err := c.execute(ctx, key, "client", "open", map[string]any{"client_key": key, "database_mode": "create", "initialization": "empty", "seed_step_id": nil}, nil)
	if err != nil {
		return err
	}
	if _, err := validateOpenedResult(raw); err != nil {
		return err
	}
	return c.Synchronize(ctx, key)
}

// Write inserts one row through the public authored write path.
func (c *DatasetCoordinator) Write(ctx context.Context, key string, write dataset.LocalWrite) error {
	columns := make([]map[string]any, 0, len(write.Columns)+len(write.Support))
	for name, raw := range write.Columns {
		var value any = raw
		// A JavaScript number cannot hold every int64, so the bridge takes a tag.
		if integer, err := strconv.ParseInt(string(raw), 10, 64); err == nil && (integer > int64(warmConnectMaximumSafeInteger) || integer < -int64(warmConnectMaximumSafeInteger)) {
			value = map[string]string{"type": "int64", "value": string(raw)}
		}
		columns = append(columns, map[string]any{"field_id": name, "value": value})
	}
	for name, raw := range write.Support {
		columns = append(columns, map[string]any{"field_id": name, "value": raw, "support": true})
	}
	payload, err := json.Marshal(map[string]any{"table_id": write.Table, "pk": map[string]string{"id": write.ID}, "operation": "insert", "columns": columns})
	if err != nil {
		return err
	}
	steps := []conformanceStep{{Operation: conformanceOperation{ContractOperation: "local", Name: "write", Payload: payload}}}
	raw, err := c.execute(ctx, key, "client", "execute-step", map[string]any{"client_key": key}, steps)
	if err != nil {
		return err
	}
	var result struct {
		Kind         string `json:"kind"`
		RowsAffected int    `json:"rows_affected"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Kind != "local-action" || result.RowsAffected != 1 {
		return errors.New("React Native dataset write did not affect one row")
	}
	return nil
}

// Synchronize runs one public synchronization to idle. A retryable response,
// such as 503 capture_pending for a pull right after a push, ends the public
// call in native backoff. The adapter then waits for the retry time and asks
// again, as an application does.
func (c *DatasetCoordinator) Synchronize(ctx context.Context, key string) error {
	for attempt := 0; ; attempt++ {
		method := "start"
		if c.active == key && c.started {
			method = "sync-now"
		}
		raw, err := c.execute(ctx, key, "client", "synchronize-step", map[string]any{"client_key": key, "method": method, "completion": "idle"}, nil)
		if err != nil {
			return err
		}
		var result struct {
			Kind       string `json:"kind"`
			Completion string `json:"completion"`
			Status     struct {
				State   string          `json:"state"`
				RetryAt time.Time       `json:"retry_at"`
				Failure json.RawMessage `json:"failure"`
			} `json:"status"`
		}
		if err := json.Unmarshal(raw, &result); err != nil || result.Kind != "synchronized" {
			return fmt.Errorf("React Native dataset synchronization result is invalid: %s", raw)
		}
		c.started = true
		if result.Completion == "idle" {
			return nil
		}
		if result.Completion != "error" || result.Status.State != "backoff" || string(result.Status.Failure) != "null" || attempt == 4 {
			return fmt.Errorf("React Native dataset synchronization did not reach idle: %s", raw)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(result.Status.RetryAt)):
		}
	}
}

// Capture reads the selected local rows and the local application row count.
func (c *DatasetCoordinator) Capture(ctx context.Context, key string, selectors []dataset.RowRef) ([]dataset.LocalRow, int, error) {
	rowSelectors := make([]map[string]string, 0, len(selectors))
	for _, selector := range selectors {
		rowSelectors = append(rowSelectors, map[string]string{"table_name": selector.Table, "primary_key_field": "id", "primary_key": selector.ID})
	}
	raw, err := c.execute(ctx, key, "observer", "capture", map[string]any{"client_keys": []string{key}, "sources": []string{"application-rows", "application-row-storage-classes", "scope-state"}, "row_selectors": rowSelectors}, nil)
	if err != nil {
		return nil, 0, err
	}
	capture, err := decodeCapture(raw, []string{"application_rows", "application_row_storage_classes", "client_state"})
	if err != nil {
		return nil, 0, err
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return nil, 0, err
	}
	var values []map[string]json.RawMessage
	var storage []map[string]string
	if json.Unmarshal(capture.Rows, &values) != nil || json.Unmarshal(capture.Storage, &storage) != nil || len(storage) != len(values) {
		return nil, 0, errors.New("React Native dataset application rows or storage classes are invalid")
	}
	rows := make([]dataset.LocalRow, 0, len(values))
	for index := range values {
		rows = append(rows, dataset.LocalRow{Values: values[index], StorageClasses: storage[index]})
	}
	return rows, int(state.ApplicationRowCount), nil
}
