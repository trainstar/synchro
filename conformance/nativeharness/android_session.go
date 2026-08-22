package nativeharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	androidSessionMaximumLineBytes = 1 << 20
	androidSessionMaximumOutput    = 64 << 10
	androidSessionConnectTimeout   = 30 * time.Second
)

// androidSessionConfig contains only device facts needed to start one real
// instrumentation process.
type androidSessionConfig struct {
	ADBPath                  string
	DeviceSerial             string
	ApplicationID            string
	InstrumentationComponent string
}

// androidSession owns one adb-forwarded, host-controlled instrumentation socket.
// The session does not inspect the Android database directly.
type androidSession struct {
	config androidSessionConfig

	mu          sync.Mutex
	requestMu   sync.Mutex
	connection  net.Conn
	scanner     *bufio.Scanner
	command     *exec.Cmd
	done        chan error
	forwardPort int
	closed      bool

	transportCheckpoint   uint64
	transportObservations []transportObservation
}

type androidSessionResult struct {
	Status                      *string
	RowsAffected                *int
	PendingChangeCount          *int
	Schema                      *schemaRef
	ApplicationRows             []map[string]json.RawMessage
	RetainedMutations           []retainedMutation
	RejectedMutations           []androidRejectedMutation
	ScopeStates                 []scopeStateRecord
	ScopeRows                   []scopeRowRecord
	RowMetadata                 []rowMetadataRecord
	Checkpoints                 []scopeStateRecord
	Provenance                  []androidProvenanceRecord
	RebuildAttempts             []rebuildAttemptRecord
	RebuildReceipts             []androidRebuildReceiptRecord
	Events                      []eventRecord
	EventsOverflowed            bool
	Failure                     *runnerFailure
	TransportObservations       *transportObservationSnapshot
	CallID                      *string
	State                       *string
	Completion                  *string
	CallErrorCategory           *string
	ProcessID                   string
	DatabaseIdentityFingerprint string
}

type androidRejectedMutation struct {
	MutationID string `json:"mutation_id"`
	Status     string `json:"status"`
	Code       string `json:"code"`
}

type androidProvenanceRecord struct {
	TableName     string   `json:"table_name"`
	RecordID      string   `json:"record_id"`
	ScopeIDs      []string `json:"scope_ids"`
	ServerVersion *string  `json:"server_version"`
}

type androidRebuildReceiptRecord struct {
	ScopeID          string          `json:"scope_id"`
	RebuildID        string          `json:"rebuild_id"`
	RequestCursor    *string         `json:"request_cursor"`
	IsFinal          bool            `json:"is_final"`
	FinalScopeCursor *string         `json:"final_scope_cursor"`
	FinalChecksum    json.RawMessage `json:"final_checksum"`
}

func startAndroidSession(ctx context.Context, config androidSessionConfig) (*androidSession, error) {
	if ctx == nil {
		return nil, errors.New("Android session context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name, err := androidSocketName()
	if err != nil {
		return nil, err
	}
	session := &androidSession{config: config}
	port, err := session.createForward(ctx, name)
	if err != nil {
		return nil, err
	}
	session.forwardPort = port
	if err := session.startInstrumentation(name); err != nil {
		_ = session.close(context.Background())
		return nil, err
	}
	if err := session.connect(ctx, port); err != nil {
		_ = session.close(context.Background())
		return nil, err
	}
	return session, nil
}

func androidSocketName() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", errors.New("generate Android instrumentation socket name failed")
	}
	return "synchro-" + hex.EncodeToString(value), nil
}

func (s *androidSession) adb(ctx context.Context, arguments ...string) (string, error) {
	if ctx == nil {
		return "", errors.New("Android adb context is required")
	}
	arguments = append([]string{"-s", s.config.DeviceSerial}, arguments...)
	command := exec.CommandContext(ctx, s.config.ADBPath, arguments...)
	output := &boundedWriter{maximum: androidSessionMaximumOutput}
	command.Stdout = output
	command.Stderr = output
	if err := command.Run(); err != nil {
		return "", errors.New("Android adb command failed")
	}
	output.mu.Lock()
	defer output.mu.Unlock()
	return string(output.data), nil
}

func (s *androidSession) createForward(ctx context.Context, socketName string) (int, error) {
	output, err := s.adb(ctx, "forward", "tcp:0", "localabstract:"+socketName)
	if err != nil {
		return 0, errors.New("create Android adb forward failed")
	}
	port, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("Android adb forward did not return a local port")
	}
	return port, nil
}

func (s *androidSession) startInstrumentation(socketName string) error {
	arguments := []string{
		"-s", s.config.DeviceSerial,
		"shell", "am", "instrument", "-w", "-r",
		"-e", "synchro.native.socket", socketName,
		s.config.InstrumentationComponent,
	}
	command := exec.Command(s.config.ADBPath, arguments...)
	output := &boundedWriter{maximum: androidSessionMaximumOutput}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		return errors.New("start Android instrumentation failed")
	}
	s.mu.Lock()
	s.command = command
	s.done = make(chan error, 1)
	done := s.done
	s.mu.Unlock()
	go func() { done <- command.Wait() }()
	return nil
}

func (s *androidSession) connect(ctx context.Context, port int) error {
	deadline := time.Now().Add(androidSessionConnectTimeout)
	if contextDeadline, found := ctx.Deadline(); found && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	var last error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		connection, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if err == nil {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				_ = connection.Close()
				return errors.New("Android session is closed")
			}
			s.connection = connection
			s.scanner = bufio.NewScanner(connection)
			s.scanner.Buffer(make([]byte, 4096), androidSessionMaximumLineBytes)
			s.mu.Unlock()
			return nil
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	if last != nil {
		return errors.New("connect Android instrumentation socket failed")
	}
	return errors.New("Android instrumentation socket did not become ready")
}

func (s *androidSession) send(ctx context.Context, command map[string]any) (androidSessionResult, error) {
	if ctx == nil {
		return androidSessionResult{}, errors.New("Android session command context is required")
	}
	if err := ctx.Err(); err != nil {
		return androidSessionResult{}, err
	}
	s.requestMu.Lock()
	defer s.requestMu.Unlock()

	s.mu.Lock()
	if s.closed || s.connection == nil || s.scanner == nil {
		s.mu.Unlock()
		return androidSessionResult{}, errors.New("Android instrumentation session is unavailable")
	}
	connection := s.connection
	scanner := s.scanner
	s.mu.Unlock()

	command["schema_version"] = 1
	encoded, err := json.Marshal(command)
	if err != nil || len(encoded) > androidSessionMaximumLineBytes-1 || jsonstrict.ValidateValue(encoded) != nil {
		return androidSessionResult{}, errors.New("encode Android instrumentation command failed")
	}
	deadline := time.Now().Add(90 * time.Second)
	if contextDeadline, found := ctx.Deadline(); found && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return androidSessionResult{}, errors.New("set Android instrumentation deadline failed")
	}
	if _, err := connection.Write(append(encoded, '\n')); err != nil {
		return androidSessionResult{}, errors.New("write Android instrumentation command failed")
	}
	if !scanner.Scan() {
		return androidSessionResult{}, errors.New("read Android instrumentation response failed")
	}
	if err := scanner.Err(); err != nil {
		return androidSessionResult{}, errors.New("read Android instrumentation response failed")
	}
	result, err := decodeAndroidSessionResponse(append([]byte(nil), scanner.Bytes()...))
	if err != nil {
		return androidSessionResult{}, err
	}
	if err := s.acceptTransportObservations(result.TransportObservations); err != nil {
		return androidSessionResult{}, err
	}
	return result, nil
}

func decodeAndroidSessionResponse(data []byte) (androidSessionResult, error) {
	if len(data) == 0 || len(data) > androidSessionMaximumLineBytes || jsonstrict.ValidateValue(data) != nil {
		return androidSessionResult{}, errors.New("decode Android instrumentation response failed")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope) != 4 {
		return androidSessionResult{}, errors.New("Android instrumentation response envelope is invalid")
	}
	for _, name := range []string{"schema_version", "outcome", "result", "error_code"} {
		if _, found := envelope[name]; !found {
			return androidSessionResult{}, errors.New("Android instrumentation response envelope is incomplete")
		}
	}
	var schemaVersion int
	var outcome string
	if json.Unmarshal(envelope["schema_version"], &schemaVersion) != nil || schemaVersion != 1 || json.Unmarshal(envelope["outcome"], &outcome) != nil {
		return androidSessionResult{}, errors.New("Android instrumentation response envelope is invalid")
	}
	switch outcome {
	case "error":
		if string(envelope["result"]) != "null" {
			return androidSessionResult{}, errors.New("Android instrumentation error response is invalid")
		}
		var code string
		if json.Unmarshal(envelope["error_code"], &code) != nil || !validRunnerErrorCode(code) {
			return androidSessionResult{}, errors.New("Android instrumentation error response is invalid")
		}
		return androidSessionResult{}, &runnerCommandError{code: code}
	case "passed":
		if string(envelope["error_code"]) != "null" {
			return androidSessionResult{}, errors.New("Android instrumentation passed response is invalid")
		}
	default:
		return androidSessionResult{}, errors.New("Android instrumentation response outcome is invalid")
	}
	return decodeAndroidSessionResult(envelope["result"])
}

func decodeAndroidSessionResult(data []byte) (androidSessionResult, error) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return androidSessionResult{}, errors.New("decode Android instrumentation result failed")
	}
	allowed := map[string]struct{}{
		"status": {}, "rows_affected": {}, "pending_change_count": {}, "schema": {}, "application_rows": {},
		"retained_mutations": {}, "rejected_mutations": {}, "scope_states": {}, "scope_rows": {},
		"row_metadata": {}, "checkpoints": {}, "provenance": {}, "rebuild_attempts": {}, "rebuild_receipts": {},
		"events": {}, "events_overflowed": {}, "failure": {}, "transport_observations": {},
		"call_id": {}, "state": {}, "completion": {}, "call_error_category": {}, "process_id": {},
		"database_identity_fingerprint": {}, "transport_milestone": {},
	}
	for name := range members {
		if _, found := allowed[name]; !found {
			return androidSessionResult{}, errors.New("Android instrumentation result contains an unknown field")
		}
	}
	required := []string{"transport_observations", "process_id", "database_identity_fingerprint"}
	for _, name := range required {
		if _, found := members[name]; !found {
			return androidSessionResult{}, errors.New("Android instrumentation result is incomplete")
		}
	}
	var result androidSessionResult
	decode := func(name string, target any) error {
		raw, found := members[name]
		if !found {
			return nil
		}
		if err := json.Unmarshal(raw, target); err != nil {
			return errors.New("decode Android instrumentation result field failed")
		}
		return nil
	}
	if err := errors.Join(
		decode("status", &result.Status), decode("rows_affected", &result.RowsAffected),
		decode("pending_change_count", &result.PendingChangeCount), decode("schema", &result.Schema),
		decode("application_rows", &result.ApplicationRows), decode("retained_mutations", &result.RetainedMutations),
		decode("rejected_mutations", &result.RejectedMutations), decode("scope_states", &result.ScopeStates),
		decode("scope_rows", &result.ScopeRows), decode("row_metadata", &result.RowMetadata),
		decode("checkpoints", &result.Checkpoints), decode("provenance", &result.Provenance),
		decode("rebuild_attempts", &result.RebuildAttempts), decode("rebuild_receipts", &result.RebuildReceipts),
		decode("events", &result.Events), decode("events_overflowed", &result.EventsOverflowed), decode("failure", &result.Failure),
		decode("transport_observations", &result.TransportObservations), decode("call_id", &result.CallID),
		decode("state", &result.State), decode("completion", &result.Completion),
		decode("call_error_category", &result.CallErrorCategory), decode("process_id", &result.ProcessID),
		decode("database_identity_fingerprint", &result.DatabaseIdentityFingerprint),
	); err != nil {
		return androidSessionResult{}, err
	}
	if result.Status != nil && *result.Status == "" {
		return androidSessionResult{}, errors.New("Android instrumentation status is invalid")
	}
	if result.PendingChangeCount != nil && *result.PendingChangeCount < 0 {
		return androidSessionResult{}, errors.New("Android instrumentation pending count is invalid")
	}
	if result.ProcessID == "" || result.EventsOverflowed || !validLowerHexDigest(result.DatabaseIdentityFingerprint) {
		return androidSessionResult{}, errors.New("Android instrumentation result is invalid")
	}
	if result.RowsAffected != nil && *result.RowsAffected < 0 {
		return androidSessionResult{}, errors.New("Android instrumentation row count is invalid")
	}
	if err := validateAndroidSessionResult(result); err != nil {
		return androidSessionResult{}, err
	}
	return result, nil
}

func validateAndroidSessionResult(result androidSessionResult) error {
	if result.TransportObservations == nil || validateTransportObservationSnapshot(result.TransportObservations) != nil {
		return errors.New("Android instrumentation transport observations are invalid")
	}
	if len(result.ApplicationRows) > maximumRunnerRows || len(result.RetainedMutations) > maximumRunnerRecords || len(result.RejectedMutations) > maximumRunnerRecords || len(result.ScopeStates) > maximumRunnerRecords || len(result.ScopeRows) > maximumRunnerRecords || len(result.RowMetadata) > maximumRunnerRecords || len(result.Checkpoints) > maximumRunnerRecords || len(result.Provenance) > maximumRunnerRecords || len(result.RebuildAttempts) > maximumRunnerRecords || len(result.RebuildReceipts) > maximumRunnerRecords || len(result.Events) > maximumRunnerRecords {
		return errors.New("Android instrumentation result is out of bounds")
	}
	for _, row := range result.ApplicationRows {
		if len(row) > maximumRunnerFields {
			return errors.New("Android instrumentation application row is out of bounds")
		}
		for _, value := range row {
			if validateRunnerRawJSONValue(value) != nil {
				return errors.New("Android instrumentation application row is invalid")
			}
		}
	}
	for _, mutation := range result.RetainedMutations {
		if mutation.MutationID == "" || mutation.LocalOrder < 0 || len(mutation.AuthoredFields) > maximumRunnerFields || mutation.AuthoredSchema.Version <= 0 || !schemaHashPattern.MatchString(mutation.AuthoredSchema.Hash) {
			return errors.New("Android instrumentation retained mutation is invalid")
		}
	}
	for _, scope := range result.ScopeStates {
		if scope.ScopeID == "" || scope.Generation < 0 {
			return errors.New("Android instrumentation scope state is invalid")
		}
	}
	for _, row := range result.ScopeRows {
		if row.ScopeID == "" || row.TableName == "" || row.RecordID == "" || row.Checksum == "" || row.Generation < 0 {
			return errors.New("Android instrumentation scope row is invalid")
		}
	}
	for _, value := range result.RebuildAttempts {
		if value.ScopeID == "" || value.RebuildID == "" || value.ClientGeneration < 0 || value.SchemaVersion <= 0 || !schemaHashPattern.MatchString(value.SchemaHash) || value.Generation < 0 || value.PageLimit < 1 || value.PageLimit > 1000 {
			return errors.New("Android instrumentation rebuild attempt is invalid")
		}
	}
	return nil
}

func (s *androidSession) acceptTransportObservations(snapshot *transportObservationSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snapshot == nil || snapshot.SequenceCheckpoint < s.transportCheckpoint || len(snapshot.Observations) < len(s.transportObservations) {
		return errors.New("Android instrumentation transport observation range is invalid")
	}
	for index, observation := range s.transportObservations {
		if !reflect.DeepEqual(observation, snapshot.Observations[index]) {
			return errors.New("Android instrumentation transport observation changed")
		}
	}
	s.transportCheckpoint = snapshot.SequenceCheckpoint
	s.transportObservations = append([]transportObservation(nil), snapshot.Observations...)
	return nil
}

func (s *androidSession) transportCheckpointValue() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transportCheckpoint
}

func (s *androidSession) transportObservationsAfter(checkpoint uint64) ([]transportObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if checkpoint > s.transportCheckpoint {
		return nil, errors.New("Android instrumentation transport checkpoint is unavailable")
	}
	result := make([]transportObservation, 0, len(s.transportObservations))
	for _, observation := range s.transportObservations {
		if observation.Sequence > checkpoint {
			result = append(result, observation)
		}
	}
	return result, nil
}

func (s *androidSession) kill(ctx context.Context, processID string) error {
	if processID == "" {
		return errors.New("Android instrumentation process identity is unavailable")
	}
	if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "kill", "-9", processID); err != nil {
		return errors.New("kill Android instrumentation process failed")
	}
	if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "kill", "-0", processID); err == nil {
		return errors.New("Android instrumentation process termination is not confirmed")
	}
	return s.closeTransport(ctx)
}

func (s *androidSession) closeTransport(ctx context.Context) error {
	s.mu.Lock()
	connection := s.connection
	s.connection = nil
	s.scanner = nil
	port := s.forwardPort
	s.forwardPort = 0
	s.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
	if port != 0 {
		if _, err := s.adb(ctx, "forward", "--remove", "tcp:"+strconv.Itoa(port)); err != nil {
			return errors.New("remove Android adb forward failed")
		}
	}
	return nil
}

func (s *androidSession) close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("Android session close context is required")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	done := s.done
	s.mu.Unlock()
	transportErr := s.closeTransport(ctx)
	if done == nil {
		return transportErr
	}
	select {
	case <-done:
		return transportErr
	case <-ctx.Done():
		return errors.Join(transportErr, errors.New("Android instrumentation did not stop"))
	case <-time.After(10 * time.Second):
		stopContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, stopErr := s.adb(stopContext, "shell", "am", "force-stop", s.config.ApplicationID)
		return errors.Join(transportErr, stopErr)
	}
}

func (s *androidSession) waitForExit(ctx context.Context) error {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return errors.New("Android instrumentation process is unavailable")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func androidSessionValue(raw json.RawMessage) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if len(raw) == 0 || decoder.Decode(&value) != nil || requireDecoderEOF(decoder) != nil {
		return nil, errors.New("Android instrumentation typed value is invalid")
	}
	if object, ok := value.(map[string]any); ok {
		if len(object) == 2 {
			if kind, found := object["type"].(string); found {
				if _, found := object["value"]; !found {
					return nil, errors.New("Android instrumentation typed value is incomplete")
				}
				switch kind {
				case "null", "string", "bytes", "boolean", "integer", "double":
					return object, nil
				}
			}
		}
		return nil, errors.New("Android instrumentation typed object is unsupported")
	}
	switch value.(type) {
	case nil:
		return map[string]any{"type": "null", "value": nil}, nil
	case string:
		return map[string]any{"type": "string", "value": value}, nil
	case bool:
		return map[string]any{"type": "boolean", "value": value}, nil
	case json.Number:
		number := string(value.(json.Number))
		if strings.ContainsAny(number, ".eE") {
			decimal, err := strconv.ParseFloat(number, 64)
			if err != nil || math.IsNaN(decimal) || math.IsInf(decimal, 0) {
				return nil, errors.New("Android instrumentation double value is invalid")
			}
			return map[string]any{"type": "double", "value": decimal}, nil
		}
		integer, err := strconv.ParseInt(number, 10, 64)
		if err != nil {
			return nil, errors.New("Android instrumentation integer value is invalid")
		}
		return map[string]any{"type": "integer", "value": integer}, nil
	default:
		return nil, errors.New("Android instrumentation value is unsupported")
	}
}

func androidSessionCommand(operation string) map[string]any {
	return map[string]any{"operation": operation}
}

func androidSessionProcessID(result androidSessionResult) (string, error) {
	if _, err := strconv.ParseInt(result.ProcessID, 10, 32); err != nil {
		return "", errors.New("Android instrumentation process identity is invalid")
	}
	return result.ProcessID, nil
}
