package syntheticproof

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	syntheticExecutePath = "/v3/execute"
	syntheticSetupStepID = "__model_setup__"
	maximumSyntheticBody = int64(1 << 20)
)

var wireNormalizationSpec = blackbox.NormalizationSpec{DynamicFields: []string{"/request_id"}}

// SyntheticFault identifies one well-formed semantic defect.
type SyntheticFault string

const (
	SyntheticCompliant         SyntheticFault = ""
	SyntheticOmitMutation      SyntheticFault = "omitted-mutation-outcome"
	SyntheticConstantChecksum  SyntheticFault = "constant-checksum"
	SyntheticDuplicateDelivery SyntheticFault = "duplicate-delivery"
	SyntheticWrongScope        SyntheticFault = "wrong-scope-row"
	SyntheticReplayCorruption  SyntheticFault = "replay-corruption"
	SyntheticWrongStatus       SyntheticFault = "wrong-status"
)

// SyntheticOptions configures one loopback reference system.
type SyntheticOptions struct {
	Fault         SyntheticFault
	ExpectedToken string
}

// SyntheticSystem serves reference-model results through real loopback HTTP.
type SyntheticSystem struct {
	mu            sync.Mutex
	scenario      scenarios.Scenario
	executions    []syntheticExecution
	expectedToken string
	fault         SyntheticFault
	faultApplied  bool
	next          int
	requestCount  int
	cache         map[string]syntheticResponse
	listener      net.Listener
	server        *http.Server
	serveDone     chan error
	closed        bool
}

type syntheticExecution struct {
	stepID    string
	operation scenarios.Operation
	result    reference.StepResult
}

type wireRequest struct {
	SchemaVersion int                 `json:"schema_version"`
	ScenarioID    string              `json:"scenario_id"`
	StepID        string              `json:"step_id"`
	Operation     scenarios.Operation `json:"operation"`
}

type wireEnvelope struct {
	SchemaVersion int                  `json:"schema_version"`
	ScenarioID    string               `json:"scenario_id"`
	StepID        string               `json:"step_id"`
	OperationKey  string               `json:"operation_key"`
	RequestID     string               `json:"request_id"`
	OpaqueValue   string               `json:"opaque_value"`
	Result        reference.StepResult `json:"result"`
}

type syntheticResponse struct {
	status    int
	headers   http.Header
	body      []byte
	operation scenarios.Operation
}

// NewSyntheticSystem starts one isolated system on a dynamic loopback port.
func NewSyntheticSystem(ctx context.Context, scenario scenarios.Scenario, options SyntheticOptions) (*SyntheticSystem, error) {
	if ctx == nil {
		return nil, errors.New("synthetic system context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scenario.ID == "" || options.ExpectedToken == "" || !validSyntheticFault(options.Fault) {
		return nil, errors.New("synthetic system configuration is invalid")
	}
	isolated, err := scenarios.Clone(scenario)
	if err != nil {
		return nil, fmt.Errorf("isolate synthetic scenario: %w", err)
	}
	scenario = isolated
	modelResult, err := modelrunner.RunScenario(ctx, scenario)
	if err != nil {
		return nil, fmt.Errorf("execute synthetic reference model: %w", err)
	}
	if !modelResult.Passed || len(modelResult.Setup) != 1 || len(modelResult.Steps) != len(scenario.Steps) {
		return nil, errors.New("synthetic reference model did not complete the scenario")
	}
	executions := make([]syntheticExecution, 0, len(scenario.Steps)+1)
	executions = append(executions, syntheticExecution{
		stepID:    syntheticSetupStepID,
		operation: cloneSyntheticOperation(modelResult.Setup[0].Operation),
		result:    modelResult.Setup[0].Result,
	})
	for index, execution := range modelResult.Steps {
		executions = append(executions, syntheticExecution{
			stepID:    string(scenario.Steps[index].ID),
			operation: cloneSyntheticOperation(execution.Operation),
			result:    execution.Result,
		})
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for synthetic HTTP: %w", err)
	}
	system := &SyntheticSystem{
		scenario:      scenario,
		executions:    executions,
		expectedToken: options.ExpectedToken,
		fault:         options.Fault,
		cache:         make(map[string]syntheticResponse),
		listener:      listener,
		serveDone:     make(chan error, 1),
	}
	system.server = &http.Server{
		Handler:  system,
		ErrorLog: log.New(io.Discard, "", 0),
		ConnContext: func(connectionContext context.Context, _ net.Conn) context.Context {
			return connectionContext
		},
	}
	go func() {
		system.serveDone <- system.server.Serve(listener)
	}()
	return system, nil
}

// BaseURL returns the loopback origin used by the raw client.
func (s *SyntheticSystem) BaseURL() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return "http://" + s.listener.Addr().String()
}

// FaultApplied reports whether the configured system changed one response.
func (s *SyntheticSystem) FaultApplied() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faultApplied
}

// RequestCount returns the number of authenticated loopback requests.
func (s *SyntheticSystem) RequestCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestCount
}

// Close stops the loopback server and waits for its serving goroutine.
func (s *SyntheticSystem) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	closeErr := s.server.Close()
	serveErr := <-s.serveDone
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve synthetic HTTP: %w", serveErr)
	}
	return closeErr
}

func (s *SyntheticSystem) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != syntheticExecutePath || request.URL.RawQuery != "" {
		writeSyntheticError(writer, http.StatusNotFound)
		return
	}
	wantedAuthorization := "Bearer " + s.expectedToken
	providedAuthorization := request.Header.Get("Authorization")
	if len(providedAuthorization) != len(wantedAuthorization) || subtle.ConstantTimeCompare([]byte(providedAuthorization), []byte(wantedAuthorization)) != 1 {
		writeSyntheticError(writer, http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumSyntheticBody+1))
	closeErr := request.Body.Close()
	if err != nil || closeErr != nil || int64(len(body)) > maximumSyntheticBody {
		writeSyntheticError(writer, http.StatusBadRequest)
		return
	}
	var envelope wireRequest
	if err := blackbox.DecodeStrictResponse(body, &envelope); err != nil {
		writeSyntheticError(writer, http.StatusBadRequest)
		return
	}
	response, status := s.execute(envelope)
	if status != 0 {
		writeSyntheticError(writer, status)
		return
	}
	for name, values := range response.headers {
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.WriteHeader(response.status)
	_, _ = writer.Write(response.body)
}

func (s *SyntheticSystem) execute(request wireRequest) (syntheticResponse, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || request.SchemaVersion != 1 || request.ScenarioID != string(s.scenario.ID) || request.StepID == "" {
		return syntheticResponse{}, http.StatusBadRequest
	}
	s.requestCount++
	cacheKey := request.ScenarioID + "\x00" + request.StepID
	if cached, found := s.cache[cacheKey]; found {
		if !sameSyntheticOperation(cached.operation, request.Operation) {
			return syntheticResponse{}, http.StatusConflict
		}
		response := cloneSyntheticResponse(cached)
		if s.fault == SyntheticReplayCorruption && !s.faultApplied {
			var envelope wireEnvelope
			if blackbox.DecodeStrictResponse(response.body, &envelope) == nil {
				envelope.OpaqueValue += "-corrupt"
				if body, err := encodeWireEnvelope(envelope); err == nil {
					response.body = body
					s.faultApplied = true
				}
			}
		}
		return response, 0
	}
	if s.next >= len(s.executions) {
		return syntheticResponse{}, http.StatusConflict
	}
	expected := s.executions[s.next]
	if request.StepID != expected.stepID || !sameSyntheticOperation(request.Operation, expected.operation) {
		return syntheticResponse{}, http.StatusConflict
	}
	result, err := cloneSyntheticResult(expected.result)
	if err != nil {
		return syntheticResponse{}, http.StatusInternalServerError
	}
	if !s.faultApplied {
		s.applyResultFault(expected.operation, &result)
	}
	requestID, err := newSyntheticRequestID()
	if err != nil {
		return syntheticResponse{}, http.StatusInternalServerError
	}
	envelope := wireEnvelope{
		SchemaVersion: 1,
		ScenarioID:    request.ScenarioID,
		StepID:        request.StepID,
		OperationKey:  scenarios.OperationKey(expected.operation),
		RequestID:     requestID,
		OpaqueValue:   syntheticOpaqueValue(request.ScenarioID, request.StepID),
		Result:        result,
	}
	responseBody, err := encodeWireEnvelope(envelope)
	if err != nil {
		return syntheticResponse{}, http.StatusInternalServerError
	}
	status := outerHTTPStatus(result)
	if s.fault == SyntheticWrongStatus && !s.faultApplied && scenarios.OperationKey(expected.operation) == "push/submit" {
		if status == http.StatusOK {
			status = http.StatusCreated
		} else {
			status = http.StatusOK
		}
		s.faultApplied = true
	}
	response := syntheticResponse{
		status: status,
		headers: http.Header{
			"Content-Type":               []string{"application/json"},
			"X-Synchro-Protocol-Version": []string{"3"},
		},
		body:      responseBody,
		operation: cloneSyntheticOperation(expected.operation),
	}
	s.cache[cacheKey] = cloneSyntheticResponse(response)
	s.next++
	return response, 0
}

func (s *SyntheticSystem) applyResultFault(operation scenarios.Operation, result *reference.StepResult) {
	switch s.fault {
	case SyntheticOmitMutation:
		if scenarios.OperationKey(operation) == "push/submit" && result.Push != nil && len(result.Push.Mutations) > 0 {
			result.Push.Mutations = append([]reference.MutationObservation(nil), result.Push.Mutations[:len(result.Push.Mutations)-1]...)
			s.faultApplied = true
		}
	case SyntheticConstantChecksum:
		if scenarios.OperationKey(operation) == "pull/request-page" && result.Pull != nil && !result.Pull.HasMore {
			for index := range result.Pull.ScopeChecksums {
				if !result.Pull.ScopeChecksums[index].HasChecksum {
					continue
				}
				constant := reference.Checksum{}
				for byteIndex := range constant {
					constant[byteIndex] = 0xa5
				}
				if result.Pull.ScopeChecksums[index].Checksum == constant {
					constant[0]++
				}
				result.Pull.ScopeChecksums[index].Checksum = constant
				s.faultApplied = true
				break
			}
		}
	case SyntheticDuplicateDelivery:
		if scenarios.OperationKey(operation) == "pull/request-page" && result.Pull != nil && len(result.Pull.Changes) > 0 {
			result.Pull.Changes = append(result.Pull.Changes, result.Pull.Changes[0])
			s.faultApplied = true
		}
	case SyntheticWrongScope:
		if scenarios.OperationKey(operation) == "pull/request-page" && result.Pull != nil && len(result.Pull.Changes) > 0 {
			result.Pull.Changes[0].Scope = reference.ScopeID("synthetic-wrong-scope")
			s.faultApplied = true
		}
	}
}

func encodeWireEnvelope(envelope wireEnvelope) ([]byte, error) {
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	return blackbox.CanonicalResponseBytes(encoded)
}

func cloneSyntheticResult(source reference.StepResult) (reference.StepResult, error) {
	encoded, err := json.Marshal(source)
	if err != nil {
		return reference.StepResult{}, err
	}
	var result reference.StepResult
	if err := blackbox.DecodeStrictResponse(encoded, &result); err != nil {
		return reference.StepResult{}, err
	}
	return result, nil
}

func cloneSyntheticOperation(source scenarios.Operation) scenarios.Operation {
	result := source
	result.Payload = append(json.RawMessage(nil), source.Payload...)
	return result
}

func cloneSyntheticResponse(source syntheticResponse) syntheticResponse {
	return syntheticResponse{
		status:    source.status,
		headers:   source.headers.Clone(),
		body:      append([]byte(nil), source.body...),
		operation: cloneSyntheticOperation(source.operation),
	}
}

func sameSyntheticOperation(left, right scenarios.Operation) bool {
	if left.ContractOperation != right.ContractOperation || left.Name != right.Name {
		return false
	}
	var leftValue any
	leftErr := blackbox.DecodeStrictResponse(left.Payload, &leftValue)
	var rightValue any
	rightErr := blackbox.DecodeStrictResponse(right.Payload, &rightValue)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}

func newSyntheticRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func syntheticOpaqueValue(scenarioID, stepID string) string {
	digest := sha256.Sum256([]byte("synthetic-opaque\x00" + scenarioID + "\x00" + stepID))
	return hex.EncodeToString(digest[:])
}

func validSyntheticFault(fault SyntheticFault) bool {
	switch fault {
	case SyntheticCompliant, SyntheticOmitMutation, SyntheticConstantChecksum, SyntheticDuplicateDelivery, SyntheticWrongScope, SyntheticReplayCorruption, SyntheticWrongStatus:
		return true
	default:
		return false
	}
}

func writeSyntheticError(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Synchro-Protocol-Version", "3")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(`{"error":"synthetic_request_rejected"}`))
}

func syntheticRequestBody(scenarioID, stepID string, operation scenarios.Operation) ([]byte, error) {
	request := wireRequest{
		SchemaVersion: 1,
		ScenarioID:    scenarioID,
		StepID:        stepID,
		Operation:     cloneSyntheticOperation(operation),
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode synthetic HTTP request: %w", err)
	}
	if int64(len(encoded)) > maximumSyntheticBody {
		return nil, errors.New("synthetic HTTP request exceeds the body limit")
	}
	return encoded, nil
}
