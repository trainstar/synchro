package nativeharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"syscall"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	maximumRunnerLineBytes = 1 << 20
	maximumRunnerStderr    = 64 << 10
	maximumRunnerSelectors = 128
	maximumRunnerFields    = 256
	maximumRunnerRecords   = 512
	maximumRunnerRows      = 256
	maximumRunnerValueSize = 1 << 20
)

type runnerCommand struct {
	SchemaVersion      int                 `json:"schema_version"`
	Operation          string              `json:"operation"`
	DatabasePath       string              `json:"database_path,omitempty"`
	ServerURL          string              `json:"server_url,omitempty"`
	AuthToken          string              `json:"auth_token,omitempty"`
	ClientID           string              `json:"client_id,omitempty"`
	SeedDatabasePath   string              `json:"seed_database_path,omitempty"`
	Platform           string              `json:"platform,omitempty"`
	AppVersion         string              `json:"app_version,omitempty"`
	PullPageSize       int                 `json:"pull_page_size,omitempty"`
	TransportCapacity  int                 `json:"transport_capacity,omitempty"`
	LocalAction        *runnerLocalAction  `json:"local_action,omitempty"`
	LifecycleOperation string              `json:"lifecycle_operation,omitempty"`
	TransportOperation string              `json:"transport_operation,omitempty"`
	CallID             string              `json:"call_id,omitempty"`
	Method             string              `json:"method,omitempty"`
	RowSelectors       []runnerRowSelector `json:"row_selectors,omitempty"`
}

type runnerLocalAction struct {
	Operation       string                     `json:"operation"`
	TableName       string                     `json:"table_name"`
	PrimaryKeyField string                     `json:"primary_key_field"`
	PrimaryKey      json.RawMessage            `json:"primary_key"`
	Fields          map[string]json.RawMessage `json:"fields"`
}

type runnerRowSelector struct {
	TableName       string          `json:"table_name"`
	PrimaryKeyField string          `json:"primary_key_field"`
	PrimaryKey      json.RawMessage `json:"primary_key"`
}

type runnerResponse struct {
	SchemaVersion int           `json:"schema_version"`
	Outcome       string        `json:"outcome"`
	Result        *runnerResult `json:"result"`
	ErrorCode     *string       `json:"error_code"`
}

type runnerCommandError struct {
	code string
}

func (e *runnerCommandError) Error() string {
	return "runner command failed"
}

type runnerResult struct {
	Status                *string                       `json:"status"`
	RowsAffected          *int                          `json:"rows_affected"`
	PendingChangeCount    *int                          `json:"pending_change_count"`
	Schema                *schemaRef                    `json:"schema"`
	ApplicationRows       []map[string]json.RawMessage  `json:"application_rows"`
	RetainedMutations     []retainedMutation            `json:"retained_mutations"`
	RejectedMutations     []retainedRejection           `json:"rejected_mutations"`
	ScopeStates           []scopeStateRecord            `json:"scope_states"`
	ScopeRows             []scopeRowRecord              `json:"scope_rows"`
	RowMetadata           *rowMetadataRecord            `json:"row_metadata"`
	RowMetadataRecords    []rowMetadataRecord           `json:"row_metadata_records"`
	RebuildAttempts       []rebuildAttemptRecord        `json:"rebuild_attempts"`
	RebuildReceiptProofs  []rebuildReceiptProofRecord   `json:"rebuild_receipt_proofs"`
	Events                []eventRecord                 `json:"events"`
	Failure               *runnerFailure                `json:"failure"`
	TransportObservations *transportObservationSnapshot `json:"transport_observations"`
	CallID                *string                       `json:"call_id"`
	State                 *string                       `json:"state"`
	Completion            *string                       `json:"completion"`
	CallErrorCategory     *string                       `json:"call_error_category"`
}

type runnerFailure struct {
	Operation      string            `json:"operation"`
	Code           string            `json:"code"`
	Retryable      bool              `json:"retryable"`
	Message        string            `json:"message"`
	RecoveryAction string            `json:"recoveryAction"`
	Metadata       map[string]string `json:"metadata"`
}

func (f *runnerFailure) UnmarshalJSON(data []byte) error {
	var raw struct {
		Operation      *string            `json:"operation"`
		Code           *string            `json:"code"`
		Retryable      *bool              `json:"retryable"`
		Message        *string            `json:"message"`
		RecoveryAction *string            `json:"recoveryAction"`
		Metadata       *map[string]string `json:"metadata"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode runner failure failed")
	}
	if raw.Operation == nil || raw.Code == nil || raw.Retryable == nil || raw.Message == nil || raw.RecoveryAction == nil || raw.Metadata == nil {
		return errors.New("runner failure is incomplete")
	}
	if len(*raw.Operation) > 32 || len(*raw.Code) > 64 || len(*raw.Message) > 256 || len(*raw.RecoveryAction) > 32 || len(*raw.Metadata) > 8 {
		return errors.New("runner failure is out of bounds")
	}
	if len(*raw.Operation) == 0 || len(*raw.Code) == 0 || len(*raw.RecoveryAction) == 0 {
		return errors.New("runner failure contains an empty field")
	}
	if !validRunnerFailureOperation(*raw.Operation) || !validRunnerFailureCode(*raw.Code) || !validRunnerRecoveryAction(*raw.RecoveryAction) {
		return errors.New("runner failure contains an unknown value")
	}
	for key, value := range *raw.Metadata {
		if key == "" || len(key) > 64 || len(value) > 128 {
			return errors.New("runner failure metadata is out of bounds")
		}
	}
	f.Operation = *raw.Operation
	f.Code = *raw.Code
	f.Retryable = *raw.Retryable
	f.Message = *raw.Message
	f.RecoveryAction = *raw.RecoveryAction
	f.Metadata = *raw.Metadata
	return nil
}

func validRunnerFailureOperation(value string) bool {
	switch value {
	case "opening", "connecting", "schema", "pushing", "pulling", "rebuilding", "lifecycle", "database":
		return true
	default:
		return false
	}
}

func validRunnerFailureCode(value string) bool {
	switch value {
	case "auth_required", "client_retired", "idempotency_conflict", "invalid_request", "invalid_response", "invalid_schema_reference", "invalid_state_transition", "local_database", "schema_application_failed", "sync_integrity_failure", "unsupported_schema", "upgrade_required":
		return true
	default:
		return false
	}
}

func validRunnerRecoveryAction(value string) bool {
	switch value {
	case "retry", "schema_reset", "none":
		return true
	default:
		return false
	}
}

type transportObservation struct {
	Sequence                   uint64
	OperationClass             string
	StatusCode                 int
	ErrorCode                  *string
	Retryable                  bool
	DurationNanoseconds        uint64
	CursorFingerprints         []string
	CursorFingerprintsComplete *bool
	RequestFacts               *transportRequestFacts
	RebuildResponseFacts       *transportRebuildResponseFacts
	PullResponseFacts          *transportPullResponseFacts
}

type transportRequestFacts struct {
	ClientGeneration     *int64
	SchemaVersion        int64
	SchemaHash           string
	ProtocolVersion      *int
	ScopeSetVersion      *int64
	ScopeCount           *int
	Limit                *int
	RebuildIDFingerprint *string
	CursorFingerprint    *string
	CursorPresent        *bool
}

func (f *transportRequestFacts) UnmarshalJSON(data []byte) error {
	var raw struct {
		ClientGeneration     *int64  `json:"client_generation"`
		SchemaVersion        *int64  `json:"schema_version"`
		SchemaHash           *string `json:"schema_hash"`
		ProtocolVersion      *int    `json:"protocol_version"`
		ScopeSetVersion      *int64  `json:"scope_set_version"`
		ScopeCount           *int    `json:"scope_count"`
		Limit                *int    `json:"limit"`
		RebuildIDFingerprint *string `json:"rebuild_id_fingerprint"`
		CursorFingerprint    *string `json:"cursor_fingerprint"`
		CursorPresent        *bool   `json:"cursor_present"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode transport request facts failed")
	}
	if raw.SchemaVersion == nil || raw.SchemaHash == nil {
		return errors.New("transport request facts are incomplete")
	}
	f.ClientGeneration = raw.ClientGeneration
	f.SchemaVersion = *raw.SchemaVersion
	f.SchemaHash = *raw.SchemaHash
	f.ProtocolVersion = raw.ProtocolVersion
	f.ScopeSetVersion = raw.ScopeSetVersion
	f.ScopeCount = raw.ScopeCount
	f.Limit = raw.Limit
	f.RebuildIDFingerprint = raw.RebuildIDFingerprint
	f.CursorFingerprint = raw.CursorFingerprint
	f.CursorPresent = raw.CursorPresent
	return nil
}

type transportRebuildResponseFacts struct {
	RecordCount         int
	HasMore             bool
	HasCursor           bool
	HasFinalScopeCursor bool
	HasChecksum         bool
	ScopeMatchesRequest bool
}

func (f *transportRebuildResponseFacts) UnmarshalJSON(data []byte) error {
	var raw struct {
		RecordCount         *int  `json:"record_count"`
		HasMore             *bool `json:"has_more"`
		HasCursor           *bool `json:"has_cursor"`
		HasFinalScopeCursor *bool `json:"has_final_scope_cursor"`
		HasChecksum         *bool `json:"has_checksum"`
		ScopeMatchesRequest *bool `json:"scope_matches_request"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode transport rebuild response facts failed")
	}
	if raw.RecordCount == nil || raw.HasMore == nil || raw.HasCursor == nil || raw.HasFinalScopeCursor == nil || raw.HasChecksum == nil || raw.ScopeMatchesRequest == nil {
		return errors.New("transport rebuild response facts are incomplete")
	}
	f.RecordCount = *raw.RecordCount
	f.HasMore = *raw.HasMore
	f.HasCursor = *raw.HasCursor
	f.HasFinalScopeCursor = *raw.HasFinalScopeCursor
	f.HasChecksum = *raw.HasChecksum
	f.ScopeMatchesRequest = *raw.ScopeMatchesRequest
	return nil
}

type transportPullResponseFacts struct {
	ChangeCount       int
	HasMore           bool
	RebuildScopeCount int
	ChecksumCount     int
}

func (f *transportPullResponseFacts) UnmarshalJSON(data []byte) error {
	var raw struct {
		ChangeCount       *int  `json:"change_count"`
		HasMore           *bool `json:"has_more"`
		RebuildScopeCount *int  `json:"rebuild_scope_count"`
		ChecksumCount     *int  `json:"checksum_count"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode transport pull response facts failed")
	}
	if raw.ChangeCount == nil || raw.HasMore == nil || raw.RebuildScopeCount == nil || raw.ChecksumCount == nil {
		return errors.New("transport pull response facts are incomplete")
	}
	f.ChangeCount = *raw.ChangeCount
	f.HasMore = *raw.HasMore
	f.RebuildScopeCount = *raw.RebuildScopeCount
	f.ChecksumCount = *raw.ChecksumCount
	return nil
}

func (o *transportObservation) UnmarshalJSON(data []byte) error {
	var raw struct {
		Sequence                   *uint64                        `json:"sequence"`
		OperationClass             *string                        `json:"operation_class"`
		StatusCode                 *int                           `json:"status_code"`
		ErrorCode                  *string                        `json:"error_code"`
		Retryable                  *bool                          `json:"retryable"`
		DurationNanoseconds        *uint64                        `json:"duration_nanoseconds"`
		CursorFingerprints         *[]string                      `json:"cursor_fingerprints"`
		CursorFingerprintsComplete *bool                          `json:"cursor_fingerprints_complete"`
		RequestFacts               *transportRequestFacts         `json:"request_facts"`
		RebuildResponseFacts       *transportRebuildResponseFacts `json:"rebuild_response_facts"`
		PullResponseFacts          *transportPullResponseFacts    `json:"pull_response_facts"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode transport observation failed")
	}
	if raw.Sequence == nil || raw.OperationClass == nil || raw.StatusCode == nil || raw.Retryable == nil || raw.DurationNanoseconds == nil {
		return errors.New("transport observation is incomplete")
	}
	o.Sequence = *raw.Sequence
	o.OperationClass = *raw.OperationClass
	o.StatusCode = *raw.StatusCode
	o.ErrorCode = cloneOptionalString(raw.ErrorCode)
	o.Retryable = *raw.Retryable
	o.DurationNanoseconds = *raw.DurationNanoseconds
	if raw.CursorFingerprints != nil {
		o.CursorFingerprints = append([]string(nil), (*raw.CursorFingerprints)...)
	} else {
		o.CursorFingerprints = nil
	}
	o.CursorFingerprintsComplete = raw.CursorFingerprintsComplete
	o.RequestFacts = raw.RequestFacts
	o.RebuildResponseFacts = raw.RebuildResponseFacts
	o.PullResponseFacts = raw.PullResponseFacts
	return nil
}

type transportObservationSnapshot struct {
	Observations       []transportObservation
	Overflowed         bool
	SequenceCheckpoint uint64
}

func (s *transportObservationSnapshot) UnmarshalJSON(data []byte) error {
	var raw struct {
		Observations       *[]transportObservation `json:"observations"`
		Overflowed         *bool                   `json:"overflowed"`
		SequenceCheckpoint *uint64                 `json:"sequence_checkpoint"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode transport observation snapshot failed")
	}
	if raw.Observations == nil || raw.Overflowed == nil || raw.SequenceCheckpoint == nil {
		return errors.New("transport observation snapshot is incomplete")
	}
	s.Observations = append([]transportObservation(nil), (*raw.Observations)...)
	s.Overflowed = *raw.Overflowed
	s.SequenceCheckpoint = *raw.SequenceCheckpoint
	return nil
}

type scopeStateRecord struct {
	ScopeID       string  `json:"scope_id"`
	Cursor        *string `json:"cursor"`
	Checksum      *string `json:"checksum"`
	LocalChecksum string  `json:"local_checksum"`
	Generation    int64   `json:"generation"`
}

type scopeRowRecord struct {
	ScopeID    string `json:"scope_id"`
	TableName  string `json:"table_name"`
	RecordID   string `json:"record_id"`
	Checksum   string `json:"checksum"`
	Generation int64  `json:"generation"`
}

type rowMetadataRecord struct {
	TableName     string  `json:"table_name"`
	RecordID      string  `json:"record_id"`
	ServerVersion string  `json:"server_version"`
	RowChecksum   *string `json:"row_checksum"`
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

type rebuildReceiptProofRecord struct {
	RebuildIDFingerprint      string
	PageCount                 int
	ReturnedRecordCount       int
	RequestChainValid         bool
	RecordsInCanonicalOrder   bool
	RowChecksumsValid         bool
	ScopeChecksumValid        bool
	FinalChecksumMatchesLocal bool
}

func (p *rebuildReceiptProofRecord) UnmarshalJSON(data []byte) error {
	var raw struct {
		RebuildIDFingerprint      *string `json:"rebuild_id_fingerprint"`
		PageCount                 *int    `json:"page_count"`
		ReturnedRecordCount       *int    `json:"returned_record_count"`
		RequestChainValid         *bool   `json:"request_chain_valid"`
		RecordsInCanonicalOrder   *bool   `json:"records_in_canonical_order"`
		RowChecksumsValid         *bool   `json:"row_checksums_valid"`
		ScopeChecksumValid        *bool   `json:"scope_checksum_valid"`
		FinalChecksumMatchesLocal *bool   `json:"final_checksum_matches_local"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil || requireDecoderEOF(decoder) != nil {
		return errors.New("decode rebuild receipt proof failed")
	}
	if raw.RebuildIDFingerprint == nil || raw.PageCount == nil || raw.ReturnedRecordCount == nil || raw.RequestChainValid == nil || raw.RecordsInCanonicalOrder == nil || raw.RowChecksumsValid == nil || raw.ScopeChecksumValid == nil || raw.FinalChecksumMatchesLocal == nil {
		return errors.New("rebuild receipt proof is incomplete")
	}
	p.RebuildIDFingerprint = *raw.RebuildIDFingerprint
	p.PageCount = *raw.PageCount
	p.ReturnedRecordCount = *raw.ReturnedRecordCount
	p.RequestChainValid = *raw.RequestChainValid
	p.RecordsInCanonicalOrder = *raw.RecordsInCanonicalOrder
	p.RowChecksumsValid = *raw.RowChecksumsValid
	p.ScopeChecksumValid = *raw.ScopeChecksumValid
	p.FinalChecksumMatchesLocal = *raw.FinalChecksumMatchesLocal
	return nil
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

type retainedRejection struct {
	LocalOrder int64         `json:"local_order"`
	Mutation   wireMutation  `json:"mutation"`
	Rejection  wireRejection `json:"rejection"`
}

type wireMutation struct {
	MutationID     string                     `json:"mutation_id"`
	Table          string                     `json:"table"`
	Operation      string                     `json:"op"`
	PrimaryKey     map[string]json.RawMessage `json:"pk"`
	AuthoredSchema schemaRef                  `json:"authored_schema"`
	BaseVersion    *string                    `json:"base_version"`
	ClientVersion  string                     `json:"client_version"`
	Columns        map[string]json.RawMessage `json:"columns,omitempty"`
}

type wireRejection struct {
	MutationID           string                     `json:"mutation_id"`
	Table                string                     `json:"table"`
	PrimaryKey           map[string]json.RawMessage `json:"pk"`
	OutcomeSchema        schemaRef                  `json:"outcome_schema"`
	Status               string                     `json:"status"`
	Code                 string                     `json:"code"`
	Message              string                     `json:"message"`
	Retryable            *bool                      `json:"retryable"`
	ServerRow            map[string]json.RawMessage `json:"server_row"`
	RowChecksum          json.RawMessage            `json:"row_checksum"`
	ServerVersion        *string                    `json:"server_version"`
	AuthoredSchema       *schemaRef                 `json:"authored_schema"`
	CurrentSchema        *schemaRef                 `json:"current_schema"`
	IncompatibleFieldIDs []string                   `json:"incompatible_field_ids"`
}

type eventRecord struct {
	Type          string         `json:"type"`
	Status        *string        `json:"status"`
	MutationID    *string        `json:"mutation_id"`
	TableID       *string        `json:"table_id"`
	RejectionCode *string        `json:"rejection_code"`
	SourceSchema  *schemaRef     `json:"source_schema"`
	TargetSchema  *schemaRef     `json:"target_schema"`
	SchemaAction  *string        `json:"schema_action"`
	ScopeID       *string        `json:"scope_id"`
	RebuildID     *string        `json:"rebuild_id"`
	Failure       *runnerFailure `json:"failure"`
}

type runnerProcess struct {
	mu                    sync.Mutex
	requestMu             sync.Mutex
	waitOnce              sync.Once
	waitDone              chan error
	command               *exec.Cmd
	stdin                 io.WriteCloser
	scanner               *bufio.Scanner
	stderr                *boundedWriter
	transportCheckpoint   uint64
	transportObservations []transportObservation
}

func startRunnerProcess(ctx context.Context, path string) (*runnerProcess, error) {
	if ctx == nil {
		return nil, errors.New("runner process context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The caller context controls startup and commands, not the runner lifetime.
	command := exec.Command(path)
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, errors.New("create runner input failed")
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, errors.New("create runner output failed")
	}
	stderr := &boundedWriter{maximum: maximumRunnerStderr}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, errors.New("start runner process failed")
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), maximumRunnerLineBytes)
	return &runnerProcess{
		command: command,
		stdin:   stdin,
		scanner: scanner,
		stderr:  stderr,
	}, nil
}

func (p *runnerProcess) send(ctx context.Context, command runnerCommand) (runnerResult, error) {
	if ctx == nil {
		return runnerResult{}, errors.New("runner command context is required")
	}
	if err := ctx.Err(); err != nil {
		return runnerResult{}, err
	}
	p.requestMu.Lock()
	defer p.requestMu.Unlock()

	p.mu.Lock()
	if p.command == nil || p.stdin == nil || p.scanner == nil {
		p.mu.Unlock()
		return runnerResult{}, errors.New("runner process is unavailable")
	}
	stdin := p.stdin
	scanner := p.scanner
	p.mu.Unlock()

	command.SchemaVersion = 1
	if err := validateRunnerCommand(command); err != nil {
		return runnerResult{}, err
	}
	data, err := json.Marshal(command)
	if err != nil || len(data) > maximumRunnerLineBytes-1 {
		return runnerResult{}, errors.New("encode runner command failed")
	}
	if err := jsonstrict.ValidateValue(data); err != nil {
		return runnerResult{}, errors.New("encode runner command failed")
	}
	data = append(data, '\n')
	if err := writeRunnerCommand(ctx, stdin, data, p); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return runnerResult{}, err
		}
		return runnerResult{}, errors.New("write runner command failed")
	}
	if err := scanRunnerResponse(ctx, scanner, p); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return runnerResult{}, err
		}
		return runnerResult{}, errors.New("read runner response failed")
	}
	result, err := validateRunnerResponse(append([]byte(nil), scanner.Bytes()...))
	if err != nil {
		return runnerResult{}, err
	}
	if err := p.acceptTransportObservations(result.TransportObservations); err != nil {
		return runnerResult{}, err
	}
	return result, nil
}

func writeRunnerCommand(ctx context.Context, stdin io.Writer, data []byte, process *runnerProcess) error {
	completed := make(chan error, 1)
	go func() {
		_, err := stdin.Write(data)
		completed <- err
	}()
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		_ = process.killForCancellation()
		return ctx.Err()
	}
}

func scanRunnerResponse(ctx context.Context, scanner *bufio.Scanner, process *runnerProcess) error {
	completed := make(chan bool, 1)
	go func() { completed <- scanner.Scan() }()
	select {
	case ok := <-completed:
		if !ok {
			return errors.New("runner response is unavailable")
		}
		return nil
	case <-ctx.Done():
		_ = process.killForCancellation()
		return ctx.Err()
	}
}

func validateRunnerResponse(data []byte) (runnerResult, error) {
	if err := jsonstrict.ValidateValue(data); err != nil {
		return runnerResult{}, errors.New("decode runner response failed")
	}
	var response runnerResponse
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return runnerResult{}, errors.New("decode runner response failed")
	}
	if err := requireDecoderEOF(decoder); err != nil {
		return runnerResult{}, errors.New("decode runner response failed")
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil || len(members) != 4 {
		return runnerResult{}, errors.New("runner response envelope is invalid")
	}
	for _, member := range []string{"schema_version", "outcome", "result", "error_code"} {
		if _, found := members[member]; !found {
			return runnerResult{}, errors.New("runner response envelope is incomplete")
		}
	}
	if response.SchemaVersion != 1 {
		return runnerResult{}, errors.New("runner response schema is invalid")
	}
	switch response.Outcome {
	case "passed":
		if response.Result == nil || response.ErrorCode != nil {
			return runnerResult{}, errors.New("runner passed response is invalid")
		}
		if err := validateRunnerResult(*response.Result); err != nil {
			return runnerResult{}, err
		}
		if err := validateTransportObservationSnapshot(response.Result.TransportObservations); err != nil {
			return runnerResult{}, err
		}
		return *response.Result, nil
	case "error":
		if response.Result != nil || response.ErrorCode == nil || !validRunnerErrorCode(*response.ErrorCode) {
			return runnerResult{}, errors.New("runner error response is invalid")
		}
		return runnerResult{}, &runnerCommandError{code: *response.ErrorCode}
	default:
		return runnerResult{}, errors.New("runner response outcome is invalid")
	}
}

func validateRunnerCommand(command runnerCommand) error {
	if command.SchemaVersion != 1 || command.Operation == "" {
		return errors.New("runner command is invalid")
	}
	if len(command.DatabasePath) > 4096 || len(command.ServerURL) > 4096 || len(command.AuthToken) > 4096 || len(command.ClientID) > 4096 || len(command.SeedDatabasePath) > 4096 || len(command.Platform) > 128 || len(command.AppVersion) > 128 || len(command.CallID) > 128 || len(command.Method) > 128 || len(command.LifecycleOperation) > 64 || len(command.TransportOperation) > 64 {
		return errors.New("runner command is out of bounds")
	}
	if command.PullPageSize < 0 || command.PullPageSize > 1000 || command.TransportCapacity < 0 || command.TransportCapacity > 512 {
		return errors.New("runner command is out of bounds")
	}
	if command.LocalAction != nil {
		if err := validateRunnerLocalAction(*command.LocalAction); err != nil {
			return err
		}
	}
	if len(command.RowSelectors) > maximumRunnerSelectors {
		return errors.New("runner command contains too many row selectors")
	}
	for _, selector := range command.RowSelectors {
		if err := validateRunnerRowSelector(selector); err != nil {
			return err
		}
	}
	if command.Operation != "open" && runnerHasOpenFields(command) {
		return errors.New("runner command contains open-only fields")
	}
	switch command.Operation {
	case "open":
		if command.DatabasePath == "" || command.ServerURL == "" || command.AuthToken == "" || command.ClientID == "" || command.LocalAction != nil || command.LifecycleOperation != "" || command.TransportOperation != "" || command.CallID != "" || command.Method != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner open command is invalid")
		}
	case "local-action":
		if command.LocalAction == nil || command.LifecycleOperation != "" || command.TransportOperation != "" || command.CallID != "" || command.Method != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner local-action command is invalid")
		}
	case "begin-call":
		if !validRunnerCallID(command.CallID) || !validRunnerMethod(command.Method) || command.LocalAction != nil || command.LifecycleOperation != "" || command.TransportOperation != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner begin-call command is invalid")
		}
	case "await-call", "cancel-call":
		if !validRunnerCallID(command.CallID) || command.LocalAction != nil || command.LifecycleOperation != "" || command.TransportOperation != "" || command.Method != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner call command is invalid")
		}
	case "lifecycle":
		if !validRunnerLifecycle(command.LifecycleOperation) || command.LocalAction != nil || command.TransportOperation != "" || command.CallID != "" || command.Method != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner lifecycle command is invalid")
		}
	case "arm-transport-pause", "await-transport-pause":
		if !validRunnerTransportOperation(command.TransportOperation) || command.LocalAction != nil || command.LifecycleOperation != "" || command.CallID != "" || command.Method != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner transport pause command is invalid")
		}
	case "resume-transport-pause":
		if command.LocalAction != nil || command.LifecycleOperation != "" || command.TransportOperation != "" || command.CallID != "" || command.Method != "" || len(command.RowSelectors) != 0 {
			return errors.New("runner resume transport pause command is invalid")
		}
	case "capture":
		if command.LocalAction != nil || command.LifecycleOperation != "" || command.TransportOperation != "" || command.CallID != "" || command.Method != "" {
			return errors.New("runner capture command is invalid")
		}
	default:
		return errors.New("runner command operation is unknown")
	}
	return nil
}

func runnerHasOpenFields(command runnerCommand) bool {
	return command.DatabasePath != "" || command.ServerURL != "" || command.AuthToken != "" || command.ClientID != "" || command.SeedDatabasePath != "" || command.Platform != "" || command.AppVersion != "" || command.PullPageSize != 0 || command.TransportCapacity != 0
}

func validateRunnerLocalAction(action runnerLocalAction) error {
	if action.Operation != "insert" && action.Operation != "update" && action.Operation != "delete" {
		return errors.New("runner local-action operation is invalid")
	}
	if action.TableName == "" || action.PrimaryKeyField == "" || len(action.PrimaryKey) == 0 || action.Fields == nil || len(action.Fields) > maximumRunnerFields {
		return errors.New("runner local-action payload is invalid")
	}
	if err := validateRunnerLocalJSONValue(action.PrimaryKey); err != nil {
		return err
	}
	for name, value := range action.Fields {
		if name == "" || len(name) > 128 || len(value) == 0 {
			return errors.New("runner local-action field is invalid")
		}
		if err := validateRunnerLocalJSONValue(value); err != nil {
			return err
		}
	}
	if action.Operation == "update" && len(action.Fields) == 0 || action.Operation == "delete" && len(action.Fields) != 0 {
		return errors.New("runner local-action fields do not match operation")
	}
	return nil
}

func validateRunnerRowSelector(selector runnerRowSelector) error {
	if selector.TableName == "" || selector.PrimaryKeyField == "" || len(selector.PrimaryKey) == 0 {
		return errors.New("runner row selector is invalid")
	}
	return validateRunnerLocalJSONValue(selector.PrimaryKey)
}

func validateRunnerLocalJSONValue(value []byte) error {
	if len(value) > maximumRunnerValueSize {
		return errors.New("runner JSON value is too large")
	}
	if err := jsonstrict.ValidateValue(value); err != nil {
		return errors.New("runner JSON value is invalid")
	}
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "null" || trimmed == "true" || trimmed == "false" {
		return nil
	}
	if strings.HasPrefix(trimmed, "\"") {
		var decoded string
		if err := json.Unmarshal(value, &decoded); err != nil {
			return errors.New("runner JSON string is invalid")
		}
		return nil
	}
	if strings.HasPrefix(trimmed, "{") {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(value, &object); err != nil || len(object) != 2 {
			return errors.New("runner typed JSON value is invalid")
		}
		typeValue, ok := object["type"]
		valueValue, hasValue := object["value"]
		if !ok || !hasValue {
			return errors.New("runner typed JSON value is incomplete")
		}
		var typeName string
		if err := json.Unmarshal(typeValue, &typeName); err != nil {
			return errors.New("runner typed JSON value type is invalid")
		}
		switch typeName {
		case "null":
			if string(valueValue) != "null" {
				return errors.New("runner typed null value is invalid")
			}
		case "string", "bytes":
			var decoded string
			if err := json.Unmarshal(valueValue, &decoded); err != nil {
				return errors.New("runner typed string value is invalid")
			}
			if typeName == "bytes" && !validCanonicalBase64URL(decoded) {
				return errors.New("runner typed bytes value is invalid")
			}
		case "boolean":
			var decoded bool
			if err := json.Unmarshal(valueValue, &decoded); err != nil {
				return errors.New("runner typed boolean value is invalid")
			}
		case "integer":
			var decoded int64
			if err := json.Unmarshal(valueValue, &decoded); err != nil {
				return errors.New("runner typed integer value is invalid")
			}
		case "double":
			var decoded float64
			if err := json.Unmarshal(valueValue, &decoded); err != nil || math.IsNaN(decoded) || math.IsInf(decoded, 0) {
				return errors.New("runner typed double value is invalid")
			}
		default:
			return errors.New("runner typed JSON value type is unsupported")
		}
		return nil
	}
	var integer int64
	if json.Unmarshal(value, &integer) == nil {
		return nil
	}
	var number float64
	if json.Unmarshal(value, &number) == nil && !math.IsNaN(number) && !math.IsInf(number, 0) {
		return nil
	}
	return errors.New("runner JSON value must be a strict scalar or typed object")
}

func validateRunnerRawJSONValue(value []byte) error {
	if len(value) > maximumRunnerValueSize {
		return errors.New("runner JSON value is too large")
	}
	wrapped := make([]byte, 0, len(value)+10)
	wrapped = append(wrapped, `{"value":`...)
	wrapped = append(wrapped, value...)
	wrapped = append(wrapped, '}')
	if err := jsonstrict.ValidateValue(wrapped); err != nil {
		return errors.New("runner JSON value is invalid")
	}
	return nil
}

func validCanonicalBase64URL(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validRunnerMethod(method string) bool {
	switch method {
	case "start", "sync-now", "retry-after-error", "reset-schema-and-start":
		return true
	default:
		return false
	}
}

func validRunnerLifecycle(operation string) bool {
	switch operation {
	case "stop", "enter-background", "enter-foreground":
		return true
	default:
		return false
	}
}

func validRunnerTransportOperation(operation string) bool {
	switch operation {
	case "connect", "pull", "push", "checkpoint", "schemas", "rebuild", "other":
		return true
	default:
		return false
	}
}

func validRunnerCallID(value string) bool {
	if len(value) == 0 || len(value) > 128 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func validateRunnerResult(result runnerResult) error {
	if len(result.ApplicationRows) > maximumRunnerRows || len(result.RetainedMutations) > maximumRunnerRecords || len(result.RejectedMutations) > maximumRunnerRecords || len(result.ScopeStates) > maximumRunnerRecords || len(result.ScopeRows) > maximumRunnerRecords || len(result.RowMetadataRecords) > maximumRunnerRecords || len(result.RebuildAttempts) > maximumRunnerRecords || len(result.RebuildReceiptProofs) > maximumRunnerRecords || len(result.Events) > maximumRunnerRecords {
		return errors.New("runner result is out of bounds")
	}
	for _, row := range result.ApplicationRows {
		if len(row) > maximumRunnerFields {
			return errors.New("runner application row is out of bounds")
		}
		for _, value := range row {
			if err := validateRunnerRawJSONValue(value); err != nil {
				return err
			}
		}
	}
	for _, mutation := range result.RetainedMutations {
		if mutation.MutationID == "" || mutation.TableID == "" || mutation.RecordID == "" || mutation.PrimaryKeyFieldID == "" || mutation.PrimaryKeyLogicalType == "" || mutation.ClientVersion == "" || mutation.AuthoredSchema.Version <= 0 || !schemaHashPattern.MatchString(mutation.AuthoredSchema.Hash) || (mutation.Operation != "insert" && mutation.Operation != "update" && mutation.Operation != "delete") || !validRetainedMutationStatus(mutation.Status) {
			return errors.New("runner retained mutation fields are invalid")
		}
		if len(mutation.AuthoredFields) > maximumRunnerRecords {
			return errors.New("runner retained mutation is out of bounds")
		}
		seenFields := make(map[string]struct{}, len(mutation.AuthoredFields))
		for _, field := range mutation.AuthoredFields {
			if field.FieldID == "" || len(field.FieldID) > 128 || !validRunnerLogicalType(field.LogicalType) {
				return errors.New("runner retained mutation field metadata is invalid")
			}
			if _, duplicate := seenFields[field.FieldID]; duplicate {
				return errors.New("runner retained mutation field is duplicated")
			}
			seenFields[field.FieldID] = struct{}{}
			if err := validateRunnerLocalJSONValue(field.Value); err != nil {
				return err
			}
		}
	}
	for _, metadata := range result.RowMetadataRecords {
		if metadata.TableName == "" || metadata.RecordID == "" || metadata.ServerVersion == "" {
			return errors.New("runner row metadata is invalid")
		}
		if metadata.RowChecksum != nil {
			if _, err := swiftChecksumDigest(metadata.RowChecksum); err != nil {
				return err
			}
		}
	}
	return nil
}

func validRetainedMutationStatus(status string) bool {
	switch status {
	case "pending", "sealed", "server_rejected", "superseded_before_send", "cancelled_before_send", "blocked_by_predecessor":
		return true
	default:
		return false
	}
}

func validRunnerLogicalType(value string) bool {
	switch value {
	case "string", "int", "int64", "decimal", "float", "boolean", "datetime", "date", "time", "json", "bytes":
		return true
	default:
		return false
	}
}

func runnerClientCallResult(result runnerResult) (*nativeexecution.ClientCallResult, error) {
	if result.CallID == nil || result.State == nil || *result.CallID == "" || *result.State == "" {
		return nil, errors.New("runner client call result is incomplete")
	}
	completion := ""
	if result.Completion != nil {
		completion = *result.Completion
	}
	return &nativeexecution.ClientCallResult{
		CallID:     scenarios.NativeCallID(*result.CallID),
		State:      *result.State,
		Completion: completion,
	}, nil
}

func validateTransportObservationSnapshot(snapshot *transportObservationSnapshot) error {
	if snapshot == nil {
		return errors.New("runner transport observations are missing")
	}
	if snapshot.Overflowed {
		return errors.New("runner transport observations overflowed")
	}
	if len(snapshot.Observations) > maximumRunnerRecords {
		return errors.New("runner transport observations are out of bounds")
	}
	if snapshot.SequenceCheckpoint != uint64(len(snapshot.Observations)) {
		return errors.New("runner transport observation range is incomplete")
	}
	for index, observation := range snapshot.Observations {
		expectedSequence := uint64(index + 1)
		if observation.Sequence != expectedSequence {
			return errors.New("runner transport observation sequence is not contiguous")
		}
		if err := validateTransportObservation(observation); err != nil {
			return err
		}
	}
	return nil
}

func validateTransportObservation(observation transportObservation) error {
	if observation.Sequence == 0 {
		return errors.New("runner transport observation sequence is invalid")
	}
	switch observation.OperationClass {
	case "connect", "pull", "push", "checkpoint", "schemas", "rebuild", "other":
	default:
		return errors.New("runner transport observation operation class is unknown")
	}
	if observation.StatusCode != 0 && (observation.StatusCode < 100 || observation.StatusCode > 599) {
		return errors.New("runner transport observation status code is out of bounds")
	}
	if observation.DurationNanoseconds == 0 {
		return errors.New("runner transport observation duration is invalid")
	}
	if observation.StatusCode == 0 {
		if observation.ErrorCode != nil || !observation.Retryable {
			return errors.New("runner transport failure facts are invalid")
		}
	} else if observation.StatusCode >= 200 && observation.StatusCode < 300 {
		if observation.ErrorCode != nil || observation.Retryable {
			return errors.New("runner transport success facts are invalid")
		}
	} else {
		if observation.ErrorCode == nil || !validTransportErrorCode(*observation.ErrorCode) {
			return errors.New("runner transport failure code is invalid")
		}
		if !transportErrorRetryable(*observation.ErrorCode) && observation.Retryable {
			return errors.New("runner transport failure retryability is invalid")
		}
	}
	if err := validateTransportRequestAndResponseFacts(observation); err != nil {
		return err
	}
	if observation.OperationClass != "pull" {
		if observation.CursorFingerprints != nil || observation.CursorFingerprintsComplete != nil {
			return errors.New("runner transport cursor fingerprints are not pull evidence")
		}
		return nil
	}
	if observation.CursorFingerprints == nil || observation.CursorFingerprintsComplete == nil || !*observation.CursorFingerprintsComplete || len(observation.CursorFingerprints) > 16 {
		return errors.New("runner transport cursor fingerprints are incomplete")
	}
	previous := ""
	for _, fingerprint := range observation.CursorFingerprints {
		if !validLowerHexDigest(fingerprint) || fingerprint <= previous {
			return errors.New("runner transport cursor fingerprint is invalid")
		}
		previous = fingerprint
	}
	return nil
}

func validTransportErrorCode(code string) bool {
	switch code {
	case "invalid_request", "invalid_schema_reference", "auth_required", "idempotency_conflict", "client_retired", "client_generation_expired", "rebuild_restart_required", "schema_mismatch", "retry_later", "sync_integrity_failure", "capture_pending", "temporary_unavailable", "upgrade_required", "invalid_response":
		return true
	default:
		return false
	}
}

func transportErrorRetryable(code string) bool {
	switch code {
	case "retry_later", "capture_pending", "temporary_unavailable":
		return true
	default:
		return false
	}
}

func validateTransportRequestAndResponseFacts(observation transportObservation) error {
	facts := observation.RequestFacts
	switch observation.OperationClass {
	case "connect":
		if !validTransportRequestSchema(facts, true) || facts.ProtocolVersion == nil || *facts.ProtocolVersion != 3 || facts.ScopeSetVersion == nil || *facts.ScopeSetVersion < 0 || facts.ScopeCount == nil || *facts.ScopeCount < 0 || facts.Limit != nil || facts.RebuildIDFingerprint != nil || facts.CursorFingerprint != nil || facts.CursorPresent != nil {
			return errors.New("runner connect request facts are invalid")
		}
		if facts.ClientGeneration != nil && *facts.ClientGeneration <= 0 {
			return errors.New("runner connect generation fact is invalid")
		}
	case "pull":
		if !validTransportRequestCommon(facts) || facts.ProtocolVersion != nil || facts.ScopeSetVersion == nil || *facts.ScopeSetVersion < 0 || facts.ScopeCount == nil || *facts.ScopeCount <= 0 || facts.Limit == nil || *facts.Limit <= 0 || facts.RebuildIDFingerprint != nil || facts.CursorFingerprint != nil || facts.CursorPresent != nil {
			return errors.New("runner pull request facts are invalid")
		}
	case "rebuild":
		if !validTransportRequestCommon(facts) || facts.ProtocolVersion != nil || facts.ScopeSetVersion != nil || facts.ScopeCount != nil || facts.Limit == nil || *facts.Limit <= 0 || facts.RebuildIDFingerprint == nil || !validLowerHexDigest(*facts.RebuildIDFingerprint) || facts.CursorPresent == nil || *facts.CursorPresent != (facts.CursorFingerprint != nil) || facts.CursorFingerprint != nil && !validLowerHexDigest(*facts.CursorFingerprint) {
			return errors.New("runner rebuild request facts are invalid")
		}
	default:
		if facts != nil {
			return errors.New("runner transport request facts are attached to an unsupported operation")
		}
	}

	if observation.StatusCode == 200 && observation.OperationClass == "rebuild" {
		response := observation.RebuildResponseFacts
		if response == nil || response.RecordCount < 0 || response.RecordCount > 1000 || !response.ScopeMatchesRequest || observation.PullResponseFacts != nil {
			return errors.New("runner rebuild response facts are invalid")
		}
	} else if observation.RebuildResponseFacts != nil {
		return errors.New("runner rebuild response facts are attached to an unsupported response")
	}
	if observation.StatusCode == 200 && observation.OperationClass == "pull" {
		response := observation.PullResponseFacts
		if response == nil || response.ChangeCount < 0 || response.ChangeCount > 1000 || response.RebuildScopeCount < 0 || response.ChecksumCount < 0 {
			return errors.New("runner pull response facts are invalid")
		}
	} else if observation.PullResponseFacts != nil {
		return errors.New("runner pull response facts are attached to an unsupported response")
	}
	return nil
}

func validTransportRequestCommon(facts *transportRequestFacts) bool {
	return validTransportRequestSchema(facts, false) && facts.ClientGeneration != nil && *facts.ClientGeneration > 0
}

func validTransportRequestSchema(facts *transportRequestFacts, allowFresh bool) bool {
	if facts == nil {
		return false
	}
	if allowFresh && facts.SchemaVersion == 0 && facts.SchemaHash == "" {
		return true
	}
	return facts.SchemaVersion > 0 && schemaHashPattern.MatchString(facts.SchemaHash)
}

func (p *runnerProcess) acceptTransportObservations(snapshot *transportObservationSnapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acceptTransportObservationsLocked(snapshot)
}

func (p *runnerProcess) acceptTransportObservationsLocked(snapshot *transportObservationSnapshot) error {
	if snapshot == nil {
		return errors.New("runner transport observations are missing")
	}
	if snapshot.SequenceCheckpoint < p.transportCheckpoint {
		return errors.New("runner transport observation checkpoint moved backwards")
	}
	if len(snapshot.Observations) < len(p.transportObservations) {
		return errors.New("runner transport observation range was omitted")
	}
	for index, observation := range p.transportObservations {
		if !reflect.DeepEqual(observation, snapshot.Observations[index]) {
			return errors.New("runner transport observation checkpoint changed")
		}
	}
	p.transportCheckpoint = snapshot.SequenceCheckpoint
	p.transportObservations = append([]transportObservation(nil), snapshot.Observations...)
	return nil
}

func (p *runnerProcess) transportCheckpointValue() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.transportCheckpoint
}

func (p *runnerProcess) transportObservationsAfter(checkpoint uint64) ([]transportObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if checkpoint > p.transportCheckpoint {
		return nil, errors.New("runner transport observation checkpoint is unavailable")
	}
	result := make([]transportObservation, 0, len(p.transportObservations))
	for _, observation := range p.transportObservations {
		if observation.Sequence > checkpoint {
			result = append(result, observation)
		}
	}
	return result, nil
}

func validRunnerErrorCode(code string) bool {
	switch code {
	case "invalid_command", "execution_failed", "capture_query_failed", "capture_row_cardinality", "capture_inspection_failed":
		return true
	default:
		return false
	}
}

func validLowerHexDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	for _, character := range digest {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func transportClassForContractOperation(operation string) string {
	switch operation {
	case "connect", "pull", "push", "rebuild":
		return operation
	case "schema":
		return "schemas"
	default:
		return ""
	}
}

func cursorFingerprint(cursor string) string {
	digest := sha256.Sum256([]byte(cursor))
	return hex.EncodeToString(digest[:])
}

func runnerFailureCode(err error) string {
	var failure *runnerCommandError
	if errors.As(err, &failure) {
		return failure.code
	}
	return "transport_failed"
}

func requireDecoderEOF(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON response has trailing data")
	}
	return nil
}

func (p *runnerProcess) killSIGKILL() error {
	return p.stopProcess(syscall.SIGKILL, true)
}

func (p *runnerProcess) close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("runner process close context is required")
	}
	p.mu.Lock()
	if p.command == nil {
		p.mu.Unlock()
		return nil
	}
	command := p.command
	stdin := p.stdin
	p.command = nil
	p.stdin = nil
	p.scanner = nil
	p.mu.Unlock()
	if stdin != nil {
		_ = stdin.Close()
	}
	completed := p.waitCommand(command)
	select {
	case err := <-completed:
		if err != nil {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) {
				return errors.New("runner process close failed")
			}
		}
		return nil
	case <-ctx.Done():
		_ = command.Process.Kill()
		<-completed
		return ctx.Err()
	}
}

func (p *runnerProcess) killForCancellation() error {
	p.mu.Lock()
	if p.command == nil || p.command.Process == nil {
		p.mu.Unlock()
		return nil
	}
	command := p.command
	p.command = nil
	p.stdin = nil
	p.scanner = nil
	p.mu.Unlock()
	_ = command.Process.Kill()
	_ = <-p.waitCommand(command)
	return nil
}

func (p *runnerProcess) stopProcess(signal syscall.Signal, requireSignal bool) error {
	p.mu.Lock()
	if p.command == nil || p.command.Process == nil {
		p.mu.Unlock()
		return errors.New("runner process is unavailable")
	}
	command := p.command
	p.command = nil
	p.stdin = nil
	p.scanner = nil
	p.mu.Unlock()
	if err := command.Process.Signal(signal); err != nil {
		return errors.New("send signal to runner failed")
	}
	err := <-p.waitCommand(command)
	if !requireSignal {
		return nil
	}
	if err == nil {
		return errors.New("runner process did not report SIGKILL")
	}
	status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != signal {
		return errors.New("runner process boundary was not SIGKILL")
	}
	return nil
}

func (p *runnerProcess) waitCommand(command *exec.Cmd) <-chan error {
	p.waitOnce.Do(func() {
		p.waitDone = make(chan error, 1)
		go func() { p.waitDone <- command.Wait() }()
	})
	return p.waitDone
}

func (p *runnerProcess) stderrSize() int {
	if p == nil || p.stderr == nil {
		return 0
	}
	return p.stderr.size()
}

type boundedWriter struct {
	mu      sync.Mutex
	maximum int
	data    []byte
}

func (w *boundedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	available := w.maximum - len(w.data)
	if available > 0 {
		if available > len(data) {
			available = len(data)
		}
		w.data = append(w.data, data[:available]...)
	}
	return len(data), nil
}

func (w *boundedWriter) size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.data)
}
