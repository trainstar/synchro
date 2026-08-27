package kotlin

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	maximumRecords                = 512
	maximumRows                   = 256
	maximumFields                 = 256
	maximumSelectors              = 128
	maximumEncodedTypedValueBytes = 1_398_102
)

// Request is one Kotlin instrumentation command.
// Execute always sets SchemaVersion to the current value.
type Request struct {
	SchemaVersion         int            `json:"schema_version"`
	Operation             string         `json:"operation"`
	DatabaseKey           string         `json:"database_key,omitempty"`
	DatabaseMode          string         `json:"database_mode,omitempty"`
	ServerURL             string         `json:"server_url,omitempty"`
	AuthToken             string         `json:"auth_token,omitempty"`
	ClientID              string         `json:"client_id,omitempty"`
	SeedDatabaseName      string         `json:"seed_database_name,omitempty"`
	Platform              string         `json:"platform,omitempty"`
	AppVersion            string         `json:"app_version,omitempty"`
	PullPageSize          int            `json:"pull_page_size,omitempty"`
	PushBatchSize         int            `json:"push_batch_size,omitempty"`
	TransportCapacity     int            `json:"transport_capacity,omitempty"`
	LocalAction           *LocalAction   `json:"local_action,omitempty"`
	LifecycleOperation    string         `json:"lifecycle_operation,omitempty"`
	TransportOperation    string         `json:"transport_operation,omitempty"`
	RebuildCursorOverride string         `json:"rebuild_cursor_override,omitempty"`
	CallID                string         `json:"call_id,omitempty"`
	Method                string         `json:"method,omitempty"`
	RowSelectors          *[]RowSelector `json:"row_selectors,omitempty"`
}

// LocalAction is one direct application write through Kotlin instrumentation.
type LocalAction struct {
	Operation       string                `json:"operation"`
	TableName       string                `json:"table_name"`
	PrimaryKeyField string                `json:"primary_key_field"`
	PrimaryKey      TypedValue            `json:"primary_key"`
	Fields          map[string]TypedValue `json:"fields"`
}

// RowSelector identifies one application row for Kotlin inspection.
type RowSelector struct {
	TableName       string     `json:"table_name"`
	PrimaryKeyField string     `json:"primary_key_field"`
	PrimaryKey      TypedValue `json:"primary_key"`
}

// TypedValue matches the Kotlin instrumentation value envelope.
type TypedValue struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// Result contains one bounded Kotlin response.
// Inspection payloads stay raw until a semantic test requests their shape.
type Result struct {
	Status                          *string                       `json:"status"`
	RowsAffected                    *int                          `json:"rows_affected"`
	PendingChangeCount              *int                          `json:"pending_change_count"`
	ApplicationRowCount             *int                          `json:"application_row_count"`
	MutationLedgerCount             *int                          `json:"mutation_ledger_count"`
	MutationOutcomeCount            *int                          `json:"mutation_outcome_count"`
	SealedBatchCount                *int                          `json:"sealed_batch_count"`
	RejectedMutationCount           *int                          `json:"rejected_mutation_count"`
	ScopeStateCount                 *int                          `json:"scope_state_count"`
	ScopeRowCount                   *int                          `json:"scope_row_count"`
	ProvenanceCount                 *int                          `json:"provenance_count"`
	RowMetadataCount                *int                          `json:"row_metadata_count"`
	RebuildAttemptCount             *int                          `json:"rebuild_attempt_count"`
	RebuildReceiptCount             *int                          `json:"rebuild_receipt_count"`
	Schema                          json.RawMessage               `json:"schema"`
	ApplicationRows                 json.RawMessage               `json:"application_rows"`
	RetainedMutations               json.RawMessage               `json:"retained_mutations"`
	RejectedMutations               json.RawMessage               `json:"rejected_mutations"`
	ScopeStates                     json.RawMessage               `json:"scope_states"`
	ScopeRows                       json.RawMessage               `json:"scope_rows"`
	RowMetadata                     json.RawMessage               `json:"row_metadata"`
	Checkpoints                     json.RawMessage               `json:"checkpoints"`
	Provenance                      json.RawMessage               `json:"provenance"`
	RebuildAttempts                 json.RawMessage               `json:"rebuild_attempts"`
	RebuildReceipts                 json.RawMessage               `json:"rebuild_receipts"`
	RebuildReceiptProofs            json.RawMessage               `json:"rebuild_receipt_proofs"`
	ProvenanceMaintenanceWorkCursor *int64                        `json:"provenance_maintenance_work_cursor"`
	Events                          json.RawMessage               `json:"events"`
	EventsOverflowed                bool                          `json:"events_overflowed"`
	Failure                         json.RawMessage               `json:"failure"`
	TransportMilestone              json.RawMessage               `json:"transport_milestone"`
	TransportObservations           *TransportObservationSnapshot `json:"transport_observations"`
	CallID                          *string                       `json:"call_id"`
	State                           *string                       `json:"state"`
	Completion                      *string                       `json:"completion"`
	CallErrorCategory               *string                       `json:"call_error_category"`
	ProcessID                       string                        `json:"process_id"`
	DatabaseIdentityFingerprint     string                        `json:"database_identity_fingerprint"`
}

type TransportObservationSnapshot struct {
	Observations       []TransportObservation `json:"observations"`
	Overflowed         bool                   `json:"overflowed"`
	SequenceCheckpoint uint64                 `json:"sequence_checkpoint"`
}

type TransportObservation struct {
	Sequence                   uint64                         `json:"sequence"`
	OperationClass             string                         `json:"operation_class"`
	StatusCode                 int                            `json:"status_code"`
	ErrorCode                  *string                        `json:"error_code"`
	Retryable                  *bool                          `json:"retryable"`
	DurationNanoseconds        uint64                         `json:"duration_nanoseconds"`
	CursorFingerprints         []string                       `json:"cursor_fingerprints"`
	CursorFingerprintsComplete *bool                          `json:"cursor_fingerprints_complete"`
	RequestFacts               *TransportRequestFacts         `json:"request_facts"`
	RebuildResponseFacts       *TransportRebuildResponseFacts `json:"rebuild_response_facts"`
	PullResponseFacts          *TransportPullResponseFacts    `json:"pull_response_facts"`
}

type TransportRequestFacts struct {
	ClientGeneration     *int64  `json:"client_generation"`
	SchemaVersion        int64   `json:"schema_version"`
	SchemaHash           string  `json:"schema_hash"`
	ProtocolVersion      *int    `json:"protocol_version"`
	ScopeSetVersion      *int64  `json:"scope_set_version"`
	ScopeCount           *int    `json:"scope_count"`
	Limit                *int    `json:"limit"`
	ScopeFingerprint     *string `json:"scope_fingerprint"`
	RebuildIDFingerprint *string `json:"rebuild_id_fingerprint"`
	CursorFingerprint    *string `json:"cursor_fingerprint"`
	CursorPresent        *bool   `json:"cursor_present"`
	MutationCount        *int    `json:"mutation_count"`
}

type TransportRebuildResponseFacts struct {
	RecordCount                 int     `json:"record_count"`
	HasMore                     bool    `json:"has_more"`
	HasCursor                   bool    `json:"has_cursor"`
	HasFinalScopeCursor         bool    `json:"has_final_scope_cursor"`
	HasChecksum                 bool    `json:"has_checksum"`
	ScopeFingerprint            string  `json:"scope_fingerprint"`
	FinalScopeCursorFingerprint *string `json:"final_scope_cursor_fingerprint"`
}

type TransportPullResponseFacts struct {
	ChangeCount                     int      `json:"change_count"`
	HasMore                         bool     `json:"has_more"`
	RebuildScopeCount               int      `json:"rebuild_scope_count"`
	ChecksumCount                   int      `json:"checksum_count"`
	ScopeCursorFingerprints         []string `json:"scope_cursor_fingerprints"`
	ScopeCursorFingerprintsComplete bool     `json:"scope_cursor_fingerprints_complete"`
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
	if err := decodeStrict(data, &raw); err != nil {
		return errors.New("decode Kotlin rebuild receipt proof failed")
	}
	if raw.RebuildIDFingerprint == nil || raw.PageCount == nil || raw.ReturnedRecordCount == nil || raw.RequestChainValid == nil || raw.RecordsInCanonicalOrder == nil || raw.RowChecksumsValid == nil || raw.ScopeChecksumValid == nil || raw.FinalChecksumMatchesLocal == nil {
		return errors.New("Kotlin rebuild receipt proof is incomplete")
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

// CommandError reports one bounded Kotlin instrumentation error code.
type CommandError struct {
	Code string
}

func (e *CommandError) Error() string {
	return "Kotlin instrumentation command failed"
}

// Execute sends one strict newline-delimited request and validates its response.
func (s *Session) Execute(ctx context.Context, request Request) (Result, error) {
	if err := requireContext(ctx, "Kotlin command context is required"); err != nil {
		return Result{}, err
	}
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	request.SchemaVersion = 1
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) > MaximumMessageBytes || jsonstrict.ValidateValue(encoded) != nil {
		return Result{}, errors.New("encode Kotlin instrumentation command failed")
	}

	s.mu.Lock()
	if s.closed || s.connection == nil || s.scanner == nil {
		s.mu.Unlock()
		return Result{}, errors.New("Kotlin instrumentation session is unavailable")
	}
	connection := s.connection
	scanner := s.scanner
	s.mu.Unlock()
	deadline := time.Now().Add(requestTimeout)
	if contextDeadline, found := ctx.Deadline(); found && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return Result{}, errors.New("set Kotlin instrumentation deadline failed")
	}
	if err := writeAll(connection, append(encoded, '\n')); err != nil {
		return Result{}, errors.New("write Kotlin instrumentation command failed")
	}
	if !scanner.Scan() || scanner.Err() != nil {
		return Result{}, s.instrumentationFailure("read Kotlin instrumentation response failed")
	}
	result, err := DecodeResponse(append([]byte(nil), scanner.Bytes()...))
	if err != nil {
		return Result{}, err
	}
	if err := s.acceptResult(result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return errors.New("writer made no progress")
		}
		data = data[written:]
	}
	return nil
}

func validateRequest(request Request) error {
	if request.SchemaVersion != 1 || request.Operation == "" {
		return errors.New("Kotlin instrumentation command is invalid")
	}
	if len(request.DatabaseKey) > 128 || len(request.ServerURL) > 4096 || len(request.AuthToken) > 16384 || len(request.ClientID) > 128 || len(request.SeedDatabaseName) > 128 || len(request.Platform) > 128 || len(request.AppVersion) > 128 || len(request.LifecycleOperation) > 64 || len(request.TransportOperation) > 64 || len(request.RebuildCursorOverride) > 4096 || len(request.CallID) > 128 || len(request.Method) > 128 || request.PullPageSize < 0 || request.PullPageSize > 1000 || request.PushBatchSize < 0 || request.PushBatchSize > 1000 || request.TransportCapacity < 0 || request.TransportCapacity > 4096 {
		return errors.New("Kotlin instrumentation command is out of bounds")
	}
	if request.Operation != "open" && requestHasOpenFields(request) {
		return errors.New("Kotlin instrumentation command contains open-only fields")
	}
	if request.Operation != "override-rebuild-cursor" && request.RebuildCursorOverride != "" {
		return errors.New("Kotlin instrumentation command contains a rebuild cursor override")
	}
	switch request.Operation {
	case "open":
		return validateOpenRequest(request)
	case "local-action":
		if request.LocalAction == nil || hasControlFields(request) || !validLocalAction(*request.LocalAction) {
			return errors.New("Kotlin local action command is invalid")
		}
	case "begin-call":
		if request.LocalAction != nil || request.LifecycleOperation != "" || request.TransportOperation != "" || request.RowSelectors != nil || !validCallID(request.CallID) || !validMethod(request.Method) {
			return errors.New("Kotlin begin-call command is invalid")
		}
	case "await-call":
		if request.LocalAction != nil || request.LifecycleOperation != "" || request.TransportOperation != "" || request.RowSelectors != nil || !validCallID(request.CallID) || request.Method != "" {
			return errors.New("Kotlin await-call command is invalid")
		}
	case "lifecycle":
		if request.LocalAction != nil || request.TransportOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors != nil || !validLifecycle(request.LifecycleOperation) {
			return errors.New("Kotlin lifecycle command is invalid")
		}
	case "arm-transport-pause", "await-transport-pause":
		if request.LocalAction != nil || request.LifecycleOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors != nil || !validTransportOperation(request.TransportOperation) {
			return errors.New("Kotlin transport pause command is invalid")
		}
	case "resume-transport-pause":
		if request.LocalAction != nil || request.LifecycleOperation != "" || request.TransportOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors != nil {
			return errors.New("Kotlin transport resume command is invalid")
		}
	case "override-rebuild-cursor":
		if request.RebuildCursorOverride == "" || request.LocalAction != nil || request.LifecycleOperation != "" || request.TransportOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors != nil {
			return errors.New("Kotlin rebuild cursor override command is invalid")
		}
	case "capture":
		if request.LocalAction != nil || request.LifecycleOperation != "" || request.TransportOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors == nil || len(*request.RowSelectors) > maximumSelectors {
			return errors.New("Kotlin capture command is invalid")
		}
		for _, selector := range *request.RowSelectors {
			if !validName(selector.TableName) || reservedAndroidTable(selector.TableName) || !validName(selector.PrimaryKeyField) || !validTypedValue(selector.PrimaryKey, false) {
				return errors.New("Kotlin row selector is invalid")
			}
		}
	default:
		return errors.New("Kotlin instrumentation operation is unsupported")
	}
	return nil
}

func validateOpenRequest(request Request) error {
	if request.DatabaseKey == "" || strings.ContainsAny(request.DatabaseKey, "/\\") || request.ServerURL == "" || request.AuthToken == "" || request.ClientID == "" || request.LocalAction != nil || request.LifecycleOperation != "" || request.TransportOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors != nil {
		return errors.New("Kotlin open command is invalid")
	}
	if request.DatabaseMode != "create" && request.DatabaseMode != "reuse" && request.DatabaseMode != "existing" {
		return errors.New("Kotlin database mode is invalid")
	}
	if (request.DatabaseMode == "reuse") != (request.SeedDatabaseName != "") || strings.ContainsAny(request.SeedDatabaseName, "/\\") {
		return errors.New("Kotlin seed database mode is invalid")
	}
	parsed, err := url.Parse(request.ServerURL)
	if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("Kotlin server URL is invalid")
	}
	return nil
}

func requestHasOpenFields(request Request) bool {
	return request.DatabaseKey != "" || request.DatabaseMode != "" || request.ServerURL != "" || request.AuthToken != "" || request.ClientID != "" || request.SeedDatabaseName != "" || request.Platform != "" || request.AppVersion != "" || request.PullPageSize != 0 || request.PushBatchSize != 0 || request.TransportCapacity != 0
}

func hasControlFields(request Request) bool {
	return request.LifecycleOperation != "" || request.TransportOperation != "" || request.CallID != "" || request.Method != "" || request.RowSelectors != nil
}

func validLocalAction(action LocalAction) bool {
	if action.Operation != "insert" && action.Operation != "update" && action.Operation != "delete" || !validName(action.TableName) || reservedAndroidTable(action.TableName) || !validName(action.PrimaryKeyField) || !validTypedValue(action.PrimaryKey, false) || action.Fields == nil || len(action.Fields) > maximumFields {
		return false
	}
	if action.Operation == "update" && len(action.Fields) == 0 || action.Operation == "delete" && len(action.Fields) != 0 {
		return false
	}
	for name, value := range action.Fields {
		if !validName(name) || !validTypedValue(value, true) {
			return false
		}
	}
	if supplied, found := action.Fields[action.PrimaryKeyField]; found && !typedValuesEqual(supplied, action.PrimaryKey) {
		return false
	}
	return true
}

func validTypedValue(value TypedValue, allowNull bool) bool {
	content, kind, ok := typedPrimitiveContent(value.Value)
	if !ok {
		return false
	}
	switch value.Type {
	case "null":
		return allowNull && kind == "null"
	case "string":
		return kind == "string"
	case "bytes":
		return kind == "string" && len(content) <= maximumEncodedTypedValueBytes && canonicalBase64URL(content)
	case "boolean":
		return kind == "boolean"
	case "integer":
		if kind != "number" {
			return false
		}
		_, err := strconv.ParseInt(content, 10, 64)
		return err == nil
	case "double":
		if kind != "number" {
			return false
		}
		decoded, err := strconv.ParseFloat(content, 64)
		return err == nil && !math.IsNaN(decoded) && !math.IsInf(decoded, 0)
	default:
		return false
	}
}

func validName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	if value[0] != '_' && (value[0] < 'A' || value[0] > 'Z') && (value[0] < 'a' || value[0] > 'z') {
		return false
	}
	for _, character := range value[1:] {
		if character != '_' && (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func reservedAndroidTable(value string) bool {
	normalized := strings.ToLower(value)
	return strings.HasPrefix(normalized, "_synchro_") || strings.HasPrefix(normalized, "sqlite_")
}

func typedPrimitiveContent(value any) (string, string, bool) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", "", false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return "", "", false
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", "", false
	}
	switch value := decoded.(type) {
	case nil:
		return "null", "null", true
	case string:
		return value, "string", true
	case bool:
		return strconv.FormatBool(value), "boolean", true
	case json.Number:
		return string(value), "number", true
	default:
		return "", "", false
	}
}

func typedValuesEqual(left, right TypedValue) bool {
	leftContent, leftKind, leftOK := typedPrimitiveContent(left.Value)
	rightContent, rightKind, rightOK := typedPrimitiveContent(right.Value)
	if !leftOK || !rightOK || left.Type != right.Type || leftKind != rightKind {
		return false
	}
	switch left.Type {
	case "null":
		return leftKind == "null"
	case "string", "bytes", "boolean":
		return leftContent == rightContent
	case "integer":
		leftValue, leftErr := strconv.ParseInt(leftContent, 10, 64)
		rightValue, rightErr := strconv.ParseInt(rightContent, 10, 64)
		return leftErr == nil && rightErr == nil && leftValue == rightValue
	case "double":
		leftValue, leftErr := strconv.ParseFloat(leftContent, 64)
		rightValue, rightErr := strconv.ParseFloat(rightContent, 64)
		return leftErr == nil && rightErr == nil && leftValue == rightValue
	default:
		return false
	}
}

func canonicalBase64URL(value string) bool {
	if strings.ContainsRune(value, '=') {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validCallID(value string) bool {
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

func validMethod(value string) bool {
	switch value {
	case "start", "sync-now", "retry-after-error", "reset-schema-and-start":
		return true
	default:
		return false
	}
}

func validLifecycle(value string) bool {
	switch value {
	case "stop", "enter-background", "enter-foreground":
		return true
	default:
		return false
	}
}

func validTransportOperation(value string) bool {
	switch value {
	case "connect", "pull", "push", "checkpoint", "schemas", "rebuild", "other":
		return true
	default:
		return false
	}
}

// DecodeResponse validates one complete Kotlin response line.
func DecodeResponse(data []byte) (Result, error) {
	if len(data) == 0 || len(data) > MaximumMessageBytes || jsonstrict.ValidateValue(data) != nil {
		return Result{}, errors.New("decode Kotlin instrumentation response failed")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope) != 4 {
		return Result{}, errors.New("Kotlin instrumentation response envelope is invalid")
	}
	for _, name := range []string{"schema_version", "outcome", "result", "error_code"} {
		if _, found := envelope[name]; !found {
			return Result{}, errors.New("Kotlin instrumentation response envelope is incomplete")
		}
	}
	var schemaVersion int
	var outcome string
	if json.Unmarshal(envelope["schema_version"], &schemaVersion) != nil || schemaVersion != 1 || json.Unmarshal(envelope["outcome"], &outcome) != nil {
		return Result{}, errors.New("Kotlin instrumentation response envelope is invalid")
	}
	if outcome == "error" {
		var code string
		if !isJSONNull(envelope["result"]) || json.Unmarshal(envelope["error_code"], &code) != nil || !validCommandErrorCode(code) {
			return Result{}, errors.New("Kotlin instrumentation error response is invalid")
		}
		return Result{}, &CommandError{Code: code}
	}
	if outcome != "passed" || !isJSONNull(envelope["error_code"]) {
		return Result{}, errors.New("Kotlin instrumentation passed response is invalid")
	}
	return decodeResult(envelope["result"])
}

func isJSONNull(data []byte) bool {
	return string(bytes.TrimSpace(data)) == "null"
}

func validCommandErrorCode(value string) bool {
	switch value {
	case "invalid_command", "execution_failed", "capture_query_failed", "capture_row_cardinality", "capture_inspection_failed":
		return true
	default:
		return false
	}
}

func decodeResult(data []byte) (Result, error) {
	var result Result
	if err := decodeStrict(data, &result); err != nil {
		return Result{}, errors.New("decode Kotlin instrumentation result failed")
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return Result{}, errors.New("decode Kotlin instrumentation result failed")
	}
	for _, name := range []string{"transport_observations", "process_id", "database_identity_fingerprint"} {
		if _, found := members[name]; !found {
			return Result{}, errors.New("Kotlin instrumentation result is incomplete")
		}
	}
	if result.Status != nil && *result.Status == "" || result.RowsAffected != nil && *result.RowsAffected < 0 || result.PendingChangeCount != nil && *result.PendingChangeCount < 0 || !validProcessID(result.ProcessID) || !validLowerHexDigest(result.DatabaseIdentityFingerprint) || result.EventsOverflowed {
		return Result{}, errors.New("Kotlin instrumentation result is invalid")
	}
	for _, count := range []*int{result.ApplicationRowCount, result.MutationLedgerCount, result.MutationOutcomeCount, result.SealedBatchCount, result.RejectedMutationCount, result.ScopeStateCount, result.ScopeRowCount, result.ProvenanceCount, result.RowMetadataCount, result.RebuildAttemptCount, result.RebuildReceiptCount} {
		if count != nil && *count < 0 {
			return Result{}, errors.New("Kotlin instrumentation state count is invalid")
		}
	}
	if err := validateTransportSnapshot(result.TransportObservations); err != nil {
		return Result{}, err
	}
	return result, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON value has trailing data")
	}
	return nil
}

func (s *TransportObservationSnapshot) UnmarshalJSON(data []byte) error {
	var raw struct {
		Observations       *[]TransportObservation `json:"observations"`
		Overflowed         *bool                   `json:"overflowed"`
		SequenceCheckpoint *uint64                 `json:"sequence_checkpoint"`
	}
	if err := decodeStrict(data, &raw); err != nil || raw.Observations == nil || raw.Overflowed == nil || raw.SequenceCheckpoint == nil {
		return errors.New("decode Kotlin transport observation snapshot failed")
	}
	s.Observations = append([]TransportObservation(nil), (*raw.Observations)...)
	s.Overflowed = *raw.Overflowed
	s.SequenceCheckpoint = *raw.SequenceCheckpoint
	return nil
}

func (o *TransportObservation) UnmarshalJSON(data []byte) error {
	var raw struct {
		Sequence                   *uint64                        `json:"sequence"`
		OperationClass             *string                        `json:"operation_class"`
		StatusCode                 *int                           `json:"status_code"`
		ErrorCode                  json.RawMessage                `json:"error_code"`
		Retryable                  *bool                          `json:"retryable"`
		DurationNanoseconds        *uint64                        `json:"duration_nanoseconds"`
		CursorFingerprints         *[]string                      `json:"cursor_fingerprints"`
		CursorFingerprintsComplete *bool                          `json:"cursor_fingerprints_complete"`
		RequestFacts               *TransportRequestFacts         `json:"request_facts"`
		RebuildResponseFacts       *TransportRebuildResponseFacts `json:"rebuild_response_facts"`
		PullResponseFacts          *TransportPullResponseFacts    `json:"pull_response_facts"`
	}
	if err := decodeStrict(data, &raw); err != nil || raw.Sequence == nil || raw.OperationClass == nil || raw.StatusCode == nil || len(raw.ErrorCode) == 0 || raw.Retryable == nil || raw.DurationNanoseconds == nil {
		return errors.New("decode Kotlin transport observation failed")
	}
	var errorCode *string
	if !bytes.Equal(raw.ErrorCode, []byte("null")) {
		var decoded string
		if err := json.Unmarshal(raw.ErrorCode, &decoded); err != nil || decoded == "" || len(decoded) > 128 {
			return errors.New("decode Kotlin transport error code failed")
		}
		errorCode = &decoded
	}
	o.Sequence = *raw.Sequence
	o.OperationClass = *raw.OperationClass
	o.StatusCode = *raw.StatusCode
	o.ErrorCode = errorCode
	o.Retryable = clonePointer(raw.Retryable)
	o.DurationNanoseconds = *raw.DurationNanoseconds
	if raw.CursorFingerprints != nil {
		o.CursorFingerprints = append([]string(nil), (*raw.CursorFingerprints)...)
	}
	o.CursorFingerprintsComplete = clonePointer(raw.CursorFingerprintsComplete)
	o.RequestFacts = raw.RequestFacts
	o.RebuildResponseFacts = raw.RebuildResponseFacts
	o.PullResponseFacts = raw.PullResponseFacts
	return nil
}

func (f *TransportRequestFacts) UnmarshalJSON(data []byte) error {
	var raw struct {
		ClientGeneration     *int64  `json:"client_generation"`
		SchemaVersion        *int64  `json:"schema_version"`
		SchemaHash           *string `json:"schema_hash"`
		ProtocolVersion      *int    `json:"protocol_version"`
		ScopeSetVersion      *int64  `json:"scope_set_version"`
		ScopeCount           *int    `json:"scope_count"`
		Limit                *int    `json:"limit"`
		ScopeFingerprint     *string `json:"scope_fingerprint"`
		RebuildIDFingerprint *string `json:"rebuild_id_fingerprint"`
		CursorFingerprint    *string `json:"cursor_fingerprint"`
		CursorPresent        *bool   `json:"cursor_present"`
		MutationCount        *int    `json:"mutation_count"`
	}
	if err := decodeStrict(data, &raw); err != nil || raw.SchemaVersion == nil || raw.SchemaHash == nil {
		return errors.New("decode Kotlin transport request facts failed")
	}
	f.ClientGeneration = clonePointer(raw.ClientGeneration)
	f.SchemaVersion = *raw.SchemaVersion
	f.SchemaHash = *raw.SchemaHash
	f.ProtocolVersion = clonePointer(raw.ProtocolVersion)
	f.ScopeSetVersion = clonePointer(raw.ScopeSetVersion)
	f.ScopeCount = clonePointer(raw.ScopeCount)
	f.Limit = clonePointer(raw.Limit)
	f.ScopeFingerprint = clonePointer(raw.ScopeFingerprint)
	f.RebuildIDFingerprint = clonePointer(raw.RebuildIDFingerprint)
	f.CursorFingerprint = clonePointer(raw.CursorFingerprint)
	f.CursorPresent = clonePointer(raw.CursorPresent)
	f.MutationCount = clonePointer(raw.MutationCount)
	return nil
}

func (f *TransportRebuildResponseFacts) UnmarshalJSON(data []byte) error {
	var raw struct {
		RecordCount                 *int    `json:"record_count"`
		HasMore                     *bool   `json:"has_more"`
		HasCursor                   *bool   `json:"has_cursor"`
		HasFinalScopeCursor         *bool   `json:"has_final_scope_cursor"`
		HasChecksum                 *bool   `json:"has_checksum"`
		ScopeFingerprint            *string `json:"scope_fingerprint"`
		FinalScopeCursorFingerprint *string `json:"final_scope_cursor_fingerprint"`
	}
	if err := decodeStrict(data, &raw); err != nil || raw.RecordCount == nil || raw.HasMore == nil || raw.HasCursor == nil || raw.HasFinalScopeCursor == nil || raw.HasChecksum == nil || raw.ScopeFingerprint == nil {
		return errors.New("decode Kotlin rebuild response facts failed")
	}
	f.RecordCount = *raw.RecordCount
	f.HasMore = *raw.HasMore
	f.HasCursor = *raw.HasCursor
	f.HasFinalScopeCursor = *raw.HasFinalScopeCursor
	f.HasChecksum = *raw.HasChecksum
	f.ScopeFingerprint = *raw.ScopeFingerprint
	f.FinalScopeCursorFingerprint = clonePointer(raw.FinalScopeCursorFingerprint)
	return nil
}

func (f *TransportPullResponseFacts) UnmarshalJSON(data []byte) error {
	var raw struct {
		ChangeCount                     *int      `json:"change_count"`
		HasMore                         *bool     `json:"has_more"`
		RebuildScopeCount               *int      `json:"rebuild_scope_count"`
		ChecksumCount                   *int      `json:"checksum_count"`
		ScopeCursorFingerprints         *[]string `json:"scope_cursor_fingerprints"`
		ScopeCursorFingerprintsComplete *bool     `json:"scope_cursor_fingerprints_complete"`
	}
	if err := decodeStrict(data, &raw); err != nil || raw.ChangeCount == nil || raw.HasMore == nil || raw.RebuildScopeCount == nil || raw.ChecksumCount == nil || raw.ScopeCursorFingerprints == nil || raw.ScopeCursorFingerprintsComplete == nil {
		return errors.New("decode Kotlin pull response facts failed")
	}
	f.ChangeCount = *raw.ChangeCount
	f.HasMore = *raw.HasMore
	f.RebuildScopeCount = *raw.RebuildScopeCount
	f.ChecksumCount = *raw.ChecksumCount
	f.ScopeCursorFingerprints = append([]string(nil), (*raw.ScopeCursorFingerprints)...)
	f.ScopeCursorFingerprintsComplete = *raw.ScopeCursorFingerprintsComplete
	return nil
}

func validateTransportSnapshot(snapshot *TransportObservationSnapshot) error {
	if snapshot == nil || snapshot.Overflowed || len(snapshot.Observations) > maximumRecords || snapshot.SequenceCheckpoint != uint64(len(snapshot.Observations)) {
		return errors.New("Kotlin transport observation range is invalid")
	}
	for index, observation := range snapshot.Observations {
		if observation.Sequence != uint64(index+1) || validateTransportObservation(observation) != nil {
			return errors.New("Kotlin transport observation is invalid")
		}
	}
	return nil
}

func validateTransportObservation(observation TransportObservation) error {
	switch observation.OperationClass {
	case "connect", "pull", "push", "checkpoint", "schemas", "rebuild", "other":
	default:
		return errors.New("Kotlin transport operation is invalid")
	}
	if observation.DurationNanoseconds == 0 || observation.StatusCode != 0 && (observation.StatusCode < 100 || observation.StatusCode > 599) || len(observation.CursorFingerprints) > 16 {
		return errors.New("Kotlin transport observation is out of bounds")
	}
	if observation.Retryable == nil || observation.StatusCode == 0 && (observation.ErrorCode != nil || !*observation.Retryable) || observation.StatusCode >= 200 && observation.StatusCode < 300 && (observation.ErrorCode != nil || *observation.Retryable) {
		return errors.New("Kotlin transport outcome facts are invalid")
	}
	if observation.StatusCode != 0 && (observation.StatusCode < 200 || observation.StatusCode >= 300) {
		if observation.ErrorCode == nil || !validTransportErrorCode(*observation.ErrorCode) || *observation.Retryable && !transportErrorRetryable(*observation.ErrorCode) {
			return errors.New("Kotlin transport failure facts are invalid")
		}
	}
	if err := validateTransportRequestAndResponseFacts(observation); err != nil {
		return err
	}
	if observation.OperationClass != "pull" {
		if observation.CursorFingerprints != nil || observation.CursorFingerprintsComplete != nil {
			return errors.New("Kotlin transport cursor fingerprints are not pull evidence")
		}
		return nil
	}
	if observation.CursorFingerprints == nil || observation.CursorFingerprintsComplete == nil || !*observation.CursorFingerprintsComplete || !validCursorFingerprintSet(observation.CursorFingerprints) {
		return errors.New("Kotlin pull cursor fingerprints are incomplete")
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

func validateTransportRequestAndResponseFacts(observation TransportObservation) error {
	facts := observation.RequestFacts
	switch observation.OperationClass {
	case "connect":
		if !validTransportRequestSchema(facts, true) || facts.ProtocolVersion == nil || *facts.ProtocolVersion != 3 || facts.ScopeSetVersion == nil || *facts.ScopeSetVersion < 0 || facts.ScopeCount == nil || *facts.ScopeCount < 0 || facts.Limit != nil || facts.ScopeFingerprint != nil || facts.RebuildIDFingerprint != nil || facts.CursorFingerprint != nil || facts.CursorPresent != nil || facts.MutationCount != nil || facts.ClientGeneration != nil && *facts.ClientGeneration <= 0 {
			return errors.New("Kotlin connect request facts are invalid")
		}
	case "pull":
		if !validTransportRequestCommon(facts) || facts.ProtocolVersion != nil || facts.ScopeSetVersion == nil || *facts.ScopeSetVersion < 0 || facts.ScopeCount == nil || *facts.ScopeCount <= 0 || facts.Limit == nil || *facts.Limit <= 0 || facts.ScopeFingerprint != nil || facts.RebuildIDFingerprint != nil || facts.CursorFingerprint != nil || facts.CursorPresent != nil || facts.MutationCount != nil {
			return errors.New("Kotlin pull request facts are invalid")
		}
	case "push":
		if !validTransportRequestCommon(facts) || facts.ProtocolVersion != nil || facts.ScopeSetVersion != nil || facts.ScopeCount != nil || facts.Limit != nil || facts.ScopeFingerprint != nil || facts.RebuildIDFingerprint != nil || facts.CursorFingerprint != nil || facts.CursorPresent != nil || facts.MutationCount == nil || *facts.MutationCount <= 0 || *facts.MutationCount > 1000 {
			return errors.New("Kotlin push request facts are invalid")
		}
	case "rebuild":
		if !validTransportRequestCommon(facts) || facts.ProtocolVersion != nil || facts.ScopeSetVersion != nil || facts.ScopeCount != nil || facts.Limit == nil || *facts.Limit <= 0 || facts.ScopeFingerprint == nil || !validLowerHexDigest(*facts.ScopeFingerprint) || facts.RebuildIDFingerprint == nil || !validLowerHexDigest(*facts.RebuildIDFingerprint) || facts.CursorPresent == nil || *facts.CursorPresent != (facts.CursorFingerprint != nil) || facts.CursorFingerprint != nil && !validLowerHexDigest(*facts.CursorFingerprint) || facts.MutationCount != nil {
			return errors.New("Kotlin rebuild request facts are invalid")
		}
	default:
		if facts != nil {
			return errors.New("Kotlin request facts are attached to an unsupported operation")
		}
	}

	if observation.StatusCode == 200 && observation.OperationClass == "rebuild" {
		response := observation.RebuildResponseFacts
		if response == nil || response.RecordCount < 0 || response.RecordCount > 1000 || !validLowerHexDigest(response.ScopeFingerprint) || facts == nil || facts.ScopeFingerprint == nil || response.ScopeFingerprint != *facts.ScopeFingerprint || response.HasFinalScopeCursor != (response.FinalScopeCursorFingerprint != nil) || response.FinalScopeCursorFingerprint != nil && !validLowerHexDigest(*response.FinalScopeCursorFingerprint) || observation.PullResponseFacts != nil {
			return errors.New("Kotlin rebuild response facts are invalid")
		}
	} else if observation.RebuildResponseFacts != nil {
		return errors.New("Kotlin rebuild response facts are attached to an unsupported response")
	}
	if observation.StatusCode == 200 && observation.OperationClass == "pull" {
		response := observation.PullResponseFacts
		if response == nil || response.ChangeCount < 0 || response.ChangeCount > 1000 || response.RebuildScopeCount < 0 || response.ChecksumCount < 0 || !response.ScopeCursorFingerprintsComplete || !validCursorFingerprintSet(response.ScopeCursorFingerprints) {
			return errors.New("Kotlin pull response facts are invalid")
		}
	} else if observation.PullResponseFacts != nil {
		return errors.New("Kotlin pull response facts are attached to an unsupported response")
	}
	return nil
}

func validTransportRequestCommon(facts *TransportRequestFacts) bool {
	return validTransportRequestSchema(facts, false) && facts.ClientGeneration != nil && *facts.ClientGeneration > 0
}

func validTransportRequestSchema(facts *TransportRequestFacts, allowFresh bool) bool {
	if facts == nil {
		return false
	}
	if allowFresh && facts.SchemaVersion == 0 && facts.SchemaHash == "" {
		return true
	}
	return facts.SchemaVersion > 0 && validLowerHexDigest(facts.SchemaHash)
}

func validLowerHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validCursorFingerprintSet(fingerprints []string) bool {
	if len(fingerprints) > 16 {
		return false
	}
	previous := ""
	for _, fingerprint := range fingerprints {
		if !validLowerHexDigest(fingerprint) || fingerprint <= previous {
			return false
		}
		previous = fingerprint
	}
	return true
}

func (s *Session) acceptResult(result Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processID != "" && s.processID != result.ProcessID || s.databaseIdentityFingerprint != "" && s.databaseIdentityFingerprint != result.DatabaseIdentityFingerprint {
		return errors.New("Kotlin instrumentation process identity changed")
	}
	snapshot := result.TransportObservations
	if snapshot == nil {
		return errors.New("Kotlin transport observations are missing")
	}
	if snapshot.SequenceCheckpoint < s.transportCheckpoint || len(snapshot.Observations) < len(s.transportObservations) {
		return errors.New("Kotlin transport observation checkpoint moved backwards")
	}
	for index, observation := range s.transportObservations {
		if !reflect.DeepEqual(observation, snapshot.Observations[index]) {
			return errors.New("Kotlin transport observation checkpoint changed")
		}
	}
	s.processID = result.ProcessID
	s.databaseIdentityFingerprint = result.DatabaseIdentityFingerprint
	s.transportCheckpoint = snapshot.SequenceCheckpoint
	s.transportObservations = cloneObservations(snapshot.Observations)
	return nil
}

// Checkpoint returns the latest accepted transport observation checkpoint.
func (s *Session) Checkpoint() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transportCheckpoint
}

// ObservationsAfter returns accepted observations after one checkpoint.
func (s *Session) ObservationsAfter(checkpoint uint64) ([]TransportObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if checkpoint > s.transportCheckpoint {
		return nil, errors.New("Kotlin transport observation checkpoint is unavailable")
	}
	result := make([]TransportObservation, 0, len(s.transportObservations))
	for _, observation := range s.transportObservations {
		if observation.Sequence > checkpoint {
			result = append(result, cloneObservation(observation))
		}
	}
	return result, nil
}

func cloneObservations(values []TransportObservation) []TransportObservation {
	result := make([]TransportObservation, len(values))
	for index, value := range values {
		result[index] = cloneObservation(value)
	}
	return result
}

func cloneObservation(value TransportObservation) TransportObservation {
	copy := value
	copy.ErrorCode = clonePointer(value.ErrorCode)
	copy.Retryable = clonePointer(value.Retryable)
	copy.CursorFingerprints = append([]string(nil), value.CursorFingerprints...)
	copy.CursorFingerprintsComplete = clonePointer(value.CursorFingerprintsComplete)
	if value.RebuildResponseFacts != nil {
		response := *value.RebuildResponseFacts
		response.FinalScopeCursorFingerprint = clonePointer(response.FinalScopeCursorFingerprint)
		copy.RebuildResponseFacts = &response
	}
	if value.PullResponseFacts != nil {
		response := *value.PullResponseFacts
		response.ScopeCursorFingerprints = append([]string(nil), value.PullResponseFacts.ScopeCursorFingerprints...)
		copy.PullResponseFacts = &response
	}
	if value.RequestFacts != nil {
		facts := *value.RequestFacts
		facts.ClientGeneration = clonePointer(facts.ClientGeneration)
		facts.ProtocolVersion = clonePointer(facts.ProtocolVersion)
		facts.ScopeSetVersion = clonePointer(facts.ScopeSetVersion)
		facts.ScopeCount = clonePointer(facts.ScopeCount)
		facts.Limit = clonePointer(facts.Limit)
		facts.RebuildIDFingerprint = clonePointer(facts.RebuildIDFingerprint)
		facts.CursorFingerprint = clonePointer(facts.CursorFingerprint)
		facts.CursorPresent = clonePointer(facts.CursorPresent)
		facts.MutationCount = clonePointer(facts.MutationCount)
		copy.RequestFacts = &facts
	}
	return copy
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
