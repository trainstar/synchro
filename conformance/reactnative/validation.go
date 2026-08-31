package reactnative

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const warmConnectScenarioID = "SCN-PERF-WARM-CONNECT-001"

var warmConnectStepOrder = []scenarios.StepID{
	"STEP-PERF-WARM-CONNECT-ASSIGN-001",
	"STEP-PERF-WARM-CONNECT-BOOTSTRAP-CONNECT-001",
	"STEP-PERF-WARM-CONNECT-BASELINE-REBUILD-001",
	"STEP-PERF-WARM-CONNECT-BASELINE-BEGIN-001",
	"STEP-PERF-WARM-CONNECT-BASELINE-APPLY-001",
	"STEP-PERF-WARM-CONNECT-BASELINE-FINALIZE-001",
	"STEP-PERF-WARM-CONNECT-BASELINE-ACK-001",
	"STEP-PERF-WARM-CONNECT-BASELINE-ACK-APPLY-001",
	"STEP-PERF-WARM-CONNECT-COMMIT-001",
	"STEP-PERF-WARM-CONNECT-MATERIALIZE-001",
	"STEP-PERF-WARM-CONNECT-001",
	"STEP-PERF-WARM-CONNECT-002",
	"STEP-PERF-WARM-CONNECT-003",
}

var warmConnectAliasNames = []string{
	"row-a-checksum",
	"scope-a-checksum",
	"client-a-generation",
	"items-primary-key",
	"baseline-rebuild",
	"row-a-version",
	"current-schema",
	"scope-a",
	"scope-set-version-one",
	"items-table",
}

type traceSnapshot struct {
	Observations       []transportObservation `json:"observations"`
	Overflowed         bool                   `json:"overflowed"`
	SequenceCheckpoint uint64                 `json:"sequenceCheckpoint"`
}

type transportObservation struct {
	Sequence                   uint64          `json:"sequence"`
	OperationClass             string          `json:"operationClass"`
	StatusCode                 int             `json:"statusCode"`
	DurationNanoseconds        uint64          `json:"durationNanoseconds"`
	CursorFingerprints         []string        `json:"cursorFingerprints"`
	CursorFingerprintsComplete *bool           `json:"cursorFingerprintsComplete"`
	RequestFacts               json.RawMessage `json:"requestFacts"`
	RebuildResponseFacts       json.RawMessage `json:"rebuildResponseFacts"`
	PullResponseFacts          json.RawMessage `json:"pullResponseFacts"`
}

type rebuildResponseFacts struct {
	RecordCount                 *uint64 `json:"record_count"`
	HasMore                     *bool   `json:"has_more"`
	HasCursor                   *bool   `json:"has_cursor"`
	HasFinalScopeCursor         *bool   `json:"has_final_scope_cursor"`
	HasChecksum                 *bool   `json:"has_checksum"`
	ScopeFingerprint            *string `json:"scope_fingerprint"`
	FinalScopeCursorFingerprint *string `json:"final_scope_cursor_fingerprint"`
}

type pullResponseFacts struct {
	ChangeCount                     *uint64  `json:"change_count"`
	HasMore                         *bool    `json:"has_more"`
	RebuildScopeCount               *uint64  `json:"rebuild_scope_count"`
	ChecksumCount                   *uint64  `json:"checksum_count"`
	ScopeCursorFingerprints         []string `json:"scope_cursor_fingerprints"`
	ScopeCursorFingerprintsComplete *bool    `json:"scope_cursor_fingerprints_complete"`
}

type clientSchema struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type clientScopeState struct {
	ScopeID       string  `json:"scopeID"`
	Cursor        *string `json:"cursor"`
	Checksum      *string `json:"checksum"`
	LocalChecksum string  `json:"localChecksum"`
	Generation    uint64  `json:"generation"`
}

type clientScopeRow struct {
	ScopeID    string `json:"scopeID"`
	TableName  string `json:"tableName"`
	RecordID   string `json:"recordID"`
	Checksum   string `json:"checksum"`
	Generation uint64 `json:"generation"`
}

type rebuildAttempt struct {
	ScopeID          string  `json:"scopeID"`
	RebuildID        string  `json:"rebuildID"`
	ClientGeneration uint64  `json:"clientGeneration"`
	SchemaVersion    uint64  `json:"schemaVersion"`
	SchemaHash       string  `json:"schemaHash"`
	Generation       uint64  `json:"generation"`
	Cursor           *string `json:"cursor"`
	PageLimit        uint64  `json:"pageLimit"`
}

type inspectedClientState struct {
	Schema                          *clientSchema      `json:"schema"`
	ScopeStates                     []clientScopeState `json:"scopeStates"`
	ScopeRows                       []clientScopeRow   `json:"scopeRows"`
	RebuildAttempts                 []rebuildAttempt   `json:"rebuildAttempts"`
	ApplicationRowCount             uint64             `json:"applicationRowCount"`
	MutationLedgerCount             uint64             `json:"mutationLedgerCount"`
	MutationOutcomeCount            uint64             `json:"mutationOutcomeCount"`
	SealedBatchCount                uint64             `json:"sealedBatchCount"`
	RejectedMutationCount           uint64             `json:"rejectedMutationCount"`
	ScopeStateCount                 uint64             `json:"scopeStateCount"`
	ScopeRowCount                   uint64             `json:"scopeRowCount"`
	ProvenanceCount                 uint64             `json:"provenanceCount"`
	RowMetadataCount                uint64             `json:"rowMetadataCount"`
	RebuildAttemptCount             uint64             `json:"rebuildAttemptCount"`
	RebuildReceiptCount             uint64             `json:"rebuildReceiptCount"`
	ProvenanceMaintenanceWorkCursor string             `json:"provenanceMaintenanceWorkCursor"`
}

type durableMetadata struct {
	TableName     string  `json:"table_name"`
	RecordID      string  `json:"record_id"`
	ServerVersion string  `json:"server_version"`
	RowChecksum   *string `json:"row_checksum"`
}

type rebuildReceiptProof struct {
	RebuildIDFingerprint    string `json:"rebuild_id_fingerprint"`
	PageCount               uint64 `json:"page_count"`
	ReturnedRecordCount     uint64 `json:"returned_record_count"`
	RequestChainValid       bool   `json:"request_chain_valid"`
	RecordsInCanonicalOrder bool   `json:"records_in_canonical_order"`
	RowChecksumsValid       bool   `json:"row_checksums_valid"`
	ScopeChecksumValid      bool   `json:"scope_checksum_valid"`
	FinalChecksumMatches    bool   `json:"final_checksum_matches_local"`
}

type durableProof struct {
	RowMetadata          *durableMetadata      `json:"row_metadata"`
	RebuildReceiptProofs []rebuildReceiptProof `json:"rebuild_receipt_proofs"`
}

type syncStatus struct {
	State     string          `json:"state"`
	RetryAt   json.RawMessage `json:"retry_at"`
	Operation json.RawMessage `json:"operation"`
	Failure   json.RawMessage `json:"failure"`
}

type actionProcessIdentity struct {
	ProcessID                   string `json:"process_id"`
	DatabaseIdentityFingerprint string `json:"database_identity_fingerprint"`
}

// ValidateScenario rejects every scenario other than the closed warm-connect contract.
func ValidateScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != warmConnectScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native warm-connect scenario contract is invalid")
	}
	if len(scenario.Steps) != len(warmConnectStepOrder) {
		return errors.New("React Native warm-connect step set changed")
	}
	for index, step := range scenario.Steps {
		if step.ID != warmConnectStepOrder[index] {
			return errors.New("React Native warm-connect step order changed")
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 1 {
		return errors.New("React Native warm-connect lifecycle boundary changed")
	}
	boundary := scenario.NativeLifecycleBoundaries[0]
	if boundary.ID != "warm_bootstrap_stop" || boundary.Phase != "setup" ||
		boundary.AfterStepID != "STEP-PERF-WARM-CONNECT-BASELINE-ACK-APPLY-001" ||
		boundary.UserID != userID || boundary.ClientID != clientID || boundary.Method != "stop" {
		return errors.New("React Native warm-connect lifecycle boundary is invalid")
	}
	if len(scenario.NativeIdentityAliases) != len(warmConnectAliasNames) {
		return errors.New("React Native warm-connect identity alias set changed")
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native warm-connect identity alias is invalid")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return errors.New("React Native warm-connect identity alias is duplicated")
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range warmConnectAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native warm-connect identity alias %q is absent", name)
		}
	}
	semantic, performance := false, false
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-WARM-CONNECT-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "state-equality" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-WARM-CONNECT-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !performance || warmConnectExpectedState(scenario) == nil {
		return errors.New("React Native warm-connect semantic or performance assertion changed")
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-WARM-CONNECT-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-warm-connect-ios", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-WARM-CONNECT-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-warm-connect-android", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-WARM-CONNECT-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-rn-warm-connect-control", "FPL-PERF-WARM-CONNECT-001", "CTRL-SCOPE-001") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-PERF-WARM-CONNECT-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-WARM-CONNECT-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-WARM-CONNECT-CONTROL-001"] != 1 {
		return errors.New("React Native warm-connect proof obligations are invalid")
	}
	return nil
}

func proofTargetMatches(obligation scenarios.ProofObligation, proofType, supportCell, target, faultPlan, control string) bool {
	if obligation.ProofType != proofType || obligation.MakeTarget != target || len(obligation.Argv) != 2 ||
		obligation.Argv[0] != "make" || obligation.Argv[1] != target {
		return false
	}
	if supportCell == "" {
		if obligation.SupportCellID != nil {
			return false
		}
	} else if obligation.SupportCellID == nil || string(*obligation.SupportCellID) != supportCell {
		return false
	}
	if faultPlan == "" {
		if obligation.FaultPlanID != nil {
			return false
		}
	} else if obligation.FaultPlanID == nil || string(*obligation.FaultPlanID) != faultPlan {
		return false
	}
	if control == "" {
		return obligation.ControlID == nil
	}
	return obligation.ControlID != nil && string(*obligation.ControlID) == control
}

func validateActionResult(raw json.RawMessage, kind string) error {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		return errInvalidExchange
	}
	var value map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &value); err != nil {
		return errInvalidExchange
	}
	var actual string
	if json.Unmarshal(value["kind"], &actual) != nil || actual != kind {
		return errInvalidExchange
	}
	return nil
}

func validateOpenedResult(raw json.RawMessage) (actionProcessIdentity, error) {
	if err := validateActionResult(raw, "opened"); err != nil {
		return actionProcessIdentity{}, err
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 3 || validateSyncStatusShape(members["status"]) != nil {
		return actionProcessIdentity{}, errInvalidExchange
	}
	return decodeActionProcessIdentity(members["process"])
}

func validateStoppedLifecycleResult(raw json.RawMessage, expected actionProcessIdentity) error {
	if err := validateActionResult(raw, "lifecycle"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 4 {
		return errors.New("React Native stopped lifecycle result shape is invalid")
	}
	var operation string
	if json.Unmarshal(members["operation"], &operation) != nil || operation != "stop" {
		return errors.New("React Native stopped lifecycle operation is invalid")
	}
	var status syncStatus
	var statusMembers map[string]json.RawMessage
	if err := jsonstrict.Decode(members["status"], &statusMembers); err != nil || len(statusMembers) != 4 ||
		jsonstrict.Decode(members["status"], &status) != nil || status.State != "stopped" ||
		!isJSONNull(status.RetryAt) || !isJSONNull(status.Operation) || !isJSONNull(status.Failure) {
		return errors.New("React Native stopped lifecycle status is invalid")
	}
	actual, err := decodeActionProcessIdentity(members["process"])
	if err != nil {
		return errors.New("React Native stopped lifecycle process identity is invalid")
	}
	if actual != expected {
		return errors.New("React Native stopped lifecycle process identity changed")
	}
	return nil
}

func validateSyncStatusShape(raw json.RawMessage) error {
	var status syncStatus
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 4 || jsonstrict.Decode(raw, &status) != nil || status.State == "" || len(status.State) > 32 {
		return errInvalidExchange
	}
	return nil
}

func decodeActionProcessIdentity(raw json.RawMessage) (actionProcessIdentity, error) {
	var identity actionProcessIdentity
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 2 || jsonstrict.Decode(raw, &identity) != nil ||
		identity.ProcessID == "" || len(identity.ProcessID) > 256 || !validLowerHexDigest(identity.DatabaseIdentityFingerprint) {
		return actionProcessIdentity{}, errInvalidExchange
	}
	return identity, nil
}

func decodeCapture(result json.RawMessage, keys []string) (finalCapture, error) {
	if err := validateActionResult(result, "capture"); err != nil {
		return finalCapture{}, err
	}
	var resultMembers map[string]json.RawMessage
	if err := jsonstrict.Decode(result, &resultMembers); err != nil || len(resultMembers) != 3 {
		return finalCapture{}, errInvalidExchange
	}
	var captureMembers map[string]json.RawMessage
	if err := jsonstrict.Decode(resultMembers["capture"], &captureMembers); err != nil || len(captureMembers) != len(keys) {
		return finalCapture{}, errInvalidExchange
	}
	for _, key := range keys {
		if raw, found := captureMembers[key]; !found || validateBoundedJSON(raw, maximumExchangeBytes) != nil {
			return finalCapture{}, errInvalidExchange
		}
	}
	return finalCapture{
		ClientState:  copyRaw(captureMembers["client_state"]),
		Pending:      copyRaw(captureMembers["pending_mutations"]),
		Rejected:     copyRaw(captureMembers["rejected_mutations"]),
		Status:       copyRaw(captureMembers["sync_status"]),
		Events:       copyRaw(captureMembers["sync_events"]),
		Provenance:   copyRaw(captureMembers["provenance"]),
		Trace:        copyRaw(captureMembers["request_trace"]),
		DurableProof: copyRaw(captureMembers["durable_proof"]),
		Rows:         copyRaw(captureMembers["application_rows"]),
	}, nil
}

func captureRows(result json.RawMessage) (json.RawMessage, error) {
	capture, err := decodeCapture(result, []string{"application_rows"})
	if err != nil {
		return nil, err
	}
	if _, err := decodeRows(capture.Rows); err != nil {
		return nil, err
	}
	return capture.Rows, nil
}

func captureTrace(result json.RawMessage, keys []string) (traceSnapshot, error) {
	capture, err := decodeCapture(result, keys)
	if err != nil {
		return traceSnapshot{}, err
	}
	return captureTraceFromRaw(capture.Trace)
}

func captureTraceFromRaw(raw json.RawMessage) (traceSnapshot, error) {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		return traceSnapshot{}, errors.New("React Native request trace is invalid")
	}
	var trace traceSnapshot
	if err := jsonstrict.Decode(raw, &trace); err != nil || trace.Observations == nil {
		return traceSnapshot{}, errors.New("React Native request trace is invalid")
	}
	return trace, nil
}

func validateBootstrapTrace(trace traceSnapshot) error {
	if trace.Overflowed || len(trace.Observations) != 3 || trace.SequenceCheckpoint != 3 {
		return errors.New("React Native bootstrap trace is incomplete")
	}
	if err := validateTraceSequence(trace.Observations); err != nil {
		return err
	}
	for index, operation := range []string{"connect", "rebuild", "pull"} {
		if err := validateTraceOperation(trace.Observations[index], operation); err != nil {
			return fmt.Errorf("React Native bootstrap %s trace is invalid: %w", operation, err)
		}
	}
	if integer, err := requestInteger(trace.Observations[0], "scope_count"); err != nil || integer != 0 {
		return errors.New("React Native bootstrap connect scope projection is invalid")
	}
	if integer, err := requestInteger(trace.Observations[2], "scope_count"); err != nil || integer != 1 {
		return errors.New("React Native bootstrap pull scope projection is invalid")
	}
	rebuild, err := decodeRebuildResponseFacts(trace.Observations[1].RebuildResponseFacts)
	if err != nil || *rebuild.HasMore || *rebuild.HasCursor || !*rebuild.HasFinalScopeCursor || !*rebuild.HasChecksum || rebuild.FinalScopeCursorFingerprint == nil {
		return errors.New("React Native bootstrap rebuild response facts are invalid")
	}
	scopeFingerprint, err := requestString(trace.Observations[1], "scope_fingerprint")
	if err != nil || *rebuild.ScopeFingerprint != scopeFingerprint {
		return errors.New("React Native bootstrap rebuild scope identity is invalid")
	}
	return nil
}

func warmTrace(final traceSnapshot, bootstrap *traceSnapshot) ([]transportObservation, error) {
	if bootstrap == nil || final.Overflowed || len(bootstrap.Observations) != 3 ||
		len(final.Observations) != 5 || final.SequenceCheckpoint != 5 {
		return nil, errors.New("React Native warm trace is incomplete")
	}
	if err := validateTraceSequence(final.Observations); err != nil {
		return nil, err
	}
	for index, observation := range bootstrap.Observations {
		if !transportObservationsEqual(final.Observations[index], observation) {
			return nil, errors.New("React Native bootstrap trace changed after its checkpoint")
		}
	}
	warm := final.Observations[bootstrap.SequenceCheckpoint:]
	for index, operation := range []string{"connect", "pull"} {
		if err := validateTraceOperation(warm[index], operation); err != nil {
			return nil, fmt.Errorf("React Native warm %s trace is invalid: %w", operation, err)
		}
	}
	return warm, nil
}

func transportObservationsEqual(left, right transportObservation) bool {
	if left.Sequence != right.Sequence || left.OperationClass != right.OperationClass ||
		left.StatusCode != right.StatusCode || left.DurationNanoseconds != right.DurationNanoseconds ||
		!reflect.DeepEqual(left.CursorFingerprints, right.CursorFingerprints) ||
		!reflect.DeepEqual(left.CursorFingerprintsComplete, right.CursorFingerprintsComplete) {
		return false
	}
	return semanticRawJSONEqual(left.RequestFacts, right.RequestFacts) &&
		semanticRawJSONEqual(left.RebuildResponseFacts, right.RebuildResponseFacts) &&
		semanticRawJSONEqual(left.PullResponseFacts, right.PullResponseFacts)
}

func semanticRawJSONEqual(left, right json.RawMessage) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	var leftValue, rightValue any
	leftDecoder := json.NewDecoder(bytes.NewReader(left))
	leftDecoder.UseNumber()
	rightDecoder := json.NewDecoder(bytes.NewReader(right))
	rightDecoder.UseNumber()
	return leftDecoder.Decode(&leftValue) == nil && rightDecoder.Decode(&rightValue) == nil &&
		reflect.DeepEqual(leftValue, rightValue)
}

func validateTraceSequence(observations []transportObservation) error {
	for index, observation := range observations {
		if observation.Sequence != uint64(index+1) {
			return errors.New("React Native request trace has a gap")
		}
	}
	return nil
}

func validateTraceOperation(observation transportObservation, operation string) error {
	if observation.OperationClass != operation || observation.StatusCode != 200 ||
		observation.DurationNanoseconds == 0 || !hasJSONValue(observation.RequestFacts) {
		return errors.New("operation facts are absent or invalid")
	}
	if err := validateBoundedJSON(observation.RequestFacts, maximumExchangeBytes); err != nil {
		return err
	}
	if operation == "pull" {
		if observation.CursorFingerprints == nil || observation.CursorFingerprintsComplete == nil ||
			!*observation.CursorFingerprintsComplete || !validCursorFingerprintSet(observation.CursorFingerprints) {
			return errors.New("pull cursor fingerprints are incomplete")
		}
	} else if observation.CursorFingerprints != nil || observation.CursorFingerprintsComplete != nil {
		return errors.New("cursor fingerprints are not pull evidence")
	}
	switch operation {
	case "connect":
		if hasJSONValue(observation.RebuildResponseFacts) || hasJSONValue(observation.PullResponseFacts) {
			return errors.New("connect response facts are invalid")
		}
	case "rebuild":
		if hasJSONValue(observation.PullResponseFacts) {
			return errors.New("rebuild response facts are invalid")
		}
		if _, err := decodeRebuildResponseFacts(observation.RebuildResponseFacts); err != nil {
			return err
		}
	case "pull":
		if hasJSONValue(observation.RebuildResponseFacts) {
			return errors.New("pull response facts are invalid")
		}
		if _, err := decodePullResponseFacts(observation.PullResponseFacts); err != nil {
			return err
		}
	}
	return nil
}

func decodeRebuildResponseFacts(raw json.RawMessage) (rebuildResponseFacts, error) {
	if !hasJSONValue(raw) || validateBoundedJSON(raw, maximumExchangeBytes) != nil {
		return rebuildResponseFacts{}, errors.New("React Native rebuild response facts are absent or invalid")
	}
	var members map[string]json.RawMessage
	var facts rebuildResponseFacts
	if jsonstrict.Decode(raw, &members) != nil || jsonstrict.Decode(raw, &facts) != nil ||
		(len(members) != 6 && len(members) != 7) || members["record_count"] == nil ||
		members["has_more"] == nil || members["has_cursor"] == nil || members["has_final_scope_cursor"] == nil ||
		members["has_checksum"] == nil || members["scope_fingerprint"] == nil ||
		len(members) == 7 && members["final_scope_cursor_fingerprint"] == nil ||
		facts.RecordCount == nil || facts.HasMore == nil || facts.HasCursor == nil || facts.HasFinalScopeCursor == nil ||
		facts.HasChecksum == nil || facts.ScopeFingerprint == nil || *facts.RecordCount > 1000 ||
		!validLowerHexDigest(*facts.ScopeFingerprint) || *facts.HasFinalScopeCursor != (facts.FinalScopeCursorFingerprint != nil) ||
		facts.FinalScopeCursorFingerprint != nil && !validLowerHexDigest(*facts.FinalScopeCursorFingerprint) {
		return rebuildResponseFacts{}, errors.New("React Native rebuild response facts are invalid")
	}
	return facts, nil
}

func decodePullResponseFacts(raw json.RawMessage) (pullResponseFacts, error) {
	if !hasJSONValue(raw) || validateBoundedJSON(raw, maximumExchangeBytes) != nil {
		return pullResponseFacts{}, errors.New("React Native pull response facts are absent or invalid")
	}
	var members map[string]json.RawMessage
	var facts pullResponseFacts
	if jsonstrict.Decode(raw, &members) != nil || jsonstrict.Decode(raw, &facts) != nil || len(members) != 6 ||
		members["change_count"] == nil || members["has_more"] == nil || members["rebuild_scope_count"] == nil ||
		members["checksum_count"] == nil || members["scope_cursor_fingerprints"] == nil ||
		members["scope_cursor_fingerprints_complete"] == nil || facts.ChangeCount == nil || facts.HasMore == nil ||
		facts.RebuildScopeCount == nil || facts.ChecksumCount == nil || *facts.ChangeCount > 1000 ||
		*facts.RebuildScopeCount > 1000 || *facts.ChecksumCount > 1000 || facts.ScopeCursorFingerprints == nil ||
		facts.ScopeCursorFingerprintsComplete == nil || !*facts.ScopeCursorFingerprintsComplete ||
		!validCursorFingerprintSet(facts.ScopeCursorFingerprints) {
		return pullResponseFacts{}, errors.New("React Native pull response facts are invalid")
	}
	return facts, nil
}

func hasJSONValue(raw json.RawMessage) bool {
	return len(bytes.TrimSpace(raw)) != 0 && !isJSONNull(raw)
}

func requestInteger(observation transportObservation, name string) (uint64, error) {
	var facts map[string]json.RawMessage
	if jsonstrict.Decode(observation.RequestFacts, &facts) != nil {
		return 0, errors.New("React Native request facts are invalid")
	}
	raw, found := facts[name]
	if !found {
		return 0, fmt.Errorf("React Native request fact %q is absent", name)
	}
	var value uint64
	if json.Unmarshal(raw, &value) != nil {
		return 0, fmt.Errorf("React Native request fact %q is invalid", name)
	}
	return value, nil
}

func requestString(observation transportObservation, name string) (string, error) {
	var facts map[string]json.RawMessage
	if jsonstrict.Decode(observation.RequestFacts, &facts) != nil {
		return "", errors.New("React Native request facts are invalid")
	}
	raw, found := facts[name]
	if !found {
		return "", fmt.Errorf("React Native request fact %q is absent", name)
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || value == "" {
		return "", fmt.Errorf("React Native request fact %q is invalid", name)
	}
	return value, nil
}

func decodeClientState(raw json.RawMessage) (inspectedClientState, error) {
	var state inspectedClientState
	if err := jsonstrict.Decode(raw, &state); err != nil || state.Schema == nil {
		return inspectedClientState{}, errors.New("React Native client state is invalid")
	}
	if state.Schema.Version == 0 || len(state.Schema.Hash) != 64 ||
		state.ProvenanceMaintenanceWorkCursor == "" {
		return inspectedClientState{}, errors.New("React Native client state identity is invalid")
	}
	return state, nil
}

func durableRowMetadata(raw json.RawMessage) (durableMetadata, error) {
	proof, err := decodeDurableProof(raw)
	if err != nil || proof.RowMetadata == nil {
		return durableMetadata{}, errors.New("React Native durable row metadata is unavailable")
	}
	metadata := *proof.RowMetadata
	if metadata.TableName == "" || metadata.RecordID == "" || metadata.ServerVersion == "" || metadata.RowChecksum == nil || *metadata.RowChecksum == "" {
		return durableMetadata{}, errors.New("React Native durable row metadata is invalid")
	}
	return metadata, nil
}

func decodeDurableProof(raw json.RawMessage) (durableProof, error) {
	var proof durableProof
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 2 ||
		members["row_metadata"] == nil || members["rebuild_receipt_proofs"] == nil ||
		jsonstrict.Decode(raw, &proof) != nil {
		return durableProof{}, errors.New("React Native durable proof is invalid")
	}
	return proof, nil
}

func checksumDigest(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	var checksum struct {
		Algorithm string `json:"algorithm"`
		Version   int    `json:"version"`
		Encoding  string `json:"encoding"`
		Digest    string `json:"digest"`
	}
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode([]byte(*value), &members); err != nil || len(members) != 4 ||
		jsonstrict.Decode([]byte(*value), &checksum) != nil || checksum.Algorithm != "sha256" ||
		checksum.Version != 1 || checksum.Encoding != "hex" || !validLowerHexDigest(checksum.Digest) {
		return nil, errors.New("React Native checksum inspection is invalid")
	}
	digest := checksum.Digest
	return &digest, nil
}

func decodeRows(raw json.RawMessage) ([]map[string]json.RawMessage, error) {
	var rows []map[string]json.RawMessage
	if err := decodeStrictValue(raw, &rows); err != nil || rows == nil || len(rows) > 256 {
		return nil, errors.New("React Native application rows are invalid")
	}
	return rows, nil
}

func rowUsesRuntimePrimary(row map[string]json.RawMessage, primaryKey string, recordID string) bool {
	raw, found := row[primaryKey]
	if !found {
		return false
	}
	expected, err := json.Marshal(recordID)
	return err == nil && bytes.Equal(bytes.TrimSpace(raw), expected)
}

func completedRebuildID(raw json.RawMessage, scopeID string) (string, error) {
	var events []map[string]json.RawMessage
	if err := decodeStrictValue(raw, &events); err != nil || events == nil || len(events) > 256 {
		return "", errors.New("React Native event evidence is invalid")
	}
	matches := 0
	var rebuildID string
	observed := make([]string, 0, len(events))
	for _, event := range events {
		var kind string
		if json.Unmarshal(event["type"], &kind) != nil || kind != "rebuild_completed" {
			continue
		}
		var eventScope, id string
		if len(event) != 3 || json.Unmarshal(event["scope_id"], &eventScope) != nil ||
			json.Unmarshal(event["rebuild_id"], &id) != nil || eventScope == "" || id == "" {
			return "", errors.New("React Native rebuild event is invalid")
		}
		observed = append(observed, eventScope+"/"+id)
		if eventScope == scopeID {
			matches++
			rebuildID = id
		}
	}
	if matches != 1 {
		// The count alone cannot show whether the scope produced no completion
		// or several. Name the wanted scope and each completion observed.
		return "", fmt.Errorf(
			"React Native completed rebuild identity is ambiguous: scope %q matched %d completions; observed [%s]",
			scopeID, matches, strings.Join(observed, " "),
		)
	}
	return rebuildID, nil
}

func validateFinalCapture(scenario scenarios.Scenario, capture finalCapture, bootstrap *traceSnapshot) error {
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if err := validateFinalClientEvidence(scenario, state, capture); err != nil {
		return err
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil {
		return err
	}
	warm, err := warmTrace(trace, bootstrap)
	if err != nil {
		return err
	}
	return validateTransportIdentities(state, capture, *bootstrap, warm)
}

func validateTransportIdentities(state inspectedClientState, capture finalCapture, bootstrap traceSnapshot, warm []transportObservation) error {
	if len(state.ScopeStates) != 1 || len(warm) != 2 {
		return errors.New("React Native transport identity evidence is incomplete")
	}
	generation, err := requestInteger(bootstrap.Observations[1], "client_generation")
	if err != nil || generation == 0 {
		return errors.New("React Native bootstrap rebuild generation is invalid")
	}
	scopeSet, err := requestInteger(bootstrap.Observations[2], "scope_set_version")
	if err != nil {
		return errors.New("React Native bootstrap scope-set version is invalid")
	}
	requests := []transportObservation{bootstrap.Observations[1], bootstrap.Observations[2], warm[0], warm[1]}
	for _, request := range requests {
		actualGeneration, generationErr := requestInteger(request, "client_generation")
		version, versionErr := requestInteger(request, "schema_version")
		hash, hashErr := requestString(request, "schema_hash")
		if generationErr != nil || actualGeneration != generation || versionErr != nil || version != state.Schema.Version ||
			hashErr != nil || hash != state.Schema.Hash {
			return errors.New("React Native request schema or generation drifted")
		}
	}
	for _, request := range []transportObservation{bootstrap.Observations[2], warm[0], warm[1]} {
		actual, scopeErr := requestInteger(request, "scope_set_version")
		count, countErr := requestInteger(request, "scope_count")
		if scopeErr != nil || actual != scopeSet || countErr != nil || count != uint64(len(state.ScopeStates)) {
			return errors.New("React Native request scope projection drifted")
		}
	}
	rebuildID, err := completedRebuildID(capture.Events, state.ScopeStates[0].ScopeID)
	if err != nil {
		return err
	}
	fingerprint, err := requestString(bootstrap.Observations[1], "rebuild_id_fingerprint")
	if err != nil || fingerprint != hashFingerprint(rebuildID) {
		return errors.New("React Native rebuild identity drifted")
	}
	rebuild, err := decodeRebuildResponseFacts(bootstrap.Observations[1].RebuildResponseFacts)
	if err != nil || rebuild.FinalScopeCursorFingerprint == nil {
		return errors.New("React Native bootstrap cursor identity is absent")
	}
	bootstrapPull, err := decodePullResponseFacts(bootstrap.Observations[2].PullResponseFacts)
	if err != nil || *bootstrapPull.HasMore || len(bootstrapPull.ScopeCursorFingerprints) != 1 {
		return errors.New("React Native bootstrap pull cursor identity is absent")
	}
	warmPull, err := decodePullResponseFacts(warm[1].PullResponseFacts)
	if err != nil || *warmPull.HasMore || len(warmPull.ScopeCursorFingerprints) != 1 {
		return errors.New("React Native warm pull cursor identity is absent")
	}
	if !reflect.DeepEqual(bootstrap.Observations[2].CursorFingerprints, []string{*rebuild.FinalScopeCursorFingerprint}) {
		return errors.New("React Native bootstrap pull is not bound to the rebuild checkpoint")
	}
	if !reflect.DeepEqual(warm[1].CursorFingerprints, bootstrapPull.ScopeCursorFingerprints) {
		return errors.New("React Native warm pull is not bound to the bootstrap pull response")
	}
	if state.ScopeStates[0].Cursor == nil || !reflect.DeepEqual(
		warmPull.ScopeCursorFingerprints,
		[]string{hashFingerprint(*state.ScopeStates[0].Cursor)},
	) {
		return errors.New("React Native durable cursor is not bound to the warm pull response")
	}
	return nil
}

func validateFinalClientEvidence(scenario scenarios.Scenario, state inspectedClientState, capture finalCapture) error {
	expected := warmConnectExpectedState(scenario)
	if expected == nil || len(expected.Clients) != 1 {
		return errors.New("React Native authored client state is unavailable")
	}
	client := expected.Clients[0]
	if client.CurrentSchema == nil {
		return errors.New("React Native authored warm-connect schema is unavailable")
	}
	if client.QueueCount == nil || client.OutcomeCount == nil || client.SealedBatchCount == nil || client.RebuildAttemptCount == nil {
		return errors.New("React Native authored warm-connect durability counts are unavailable")
	}
	return validateFinalClientEvidenceForExpected(expected, state, capture)
}

func validateFinalClientEvidenceForExpected(expected *scenarios.StateFacts, state inspectedClientState, capture finalCapture) error {
	if expected == nil || len(expected.Clients) != 1 || len(expected.Rows) != 1 {
		return errors.New("React Native authored client state is unavailable")
	}
	client := expected.Clients[0]
	if client.RowCount == nil || client.CheckpointCount == nil || client.ProvenanceCount == nil || state.Schema == nil ||
		len(client.Checkpoints) != 1 || len(client.Provenance) != 1 || len(client.Provenance[0].Scopes) != 1 {
		return errors.New("React Native authored durability state is incomplete")
	}
	proof, err := decodeDurableProof(capture.DurableProof)
	if err != nil || len(proof.RebuildReceiptProofs) != 1 {
		return errors.New("React Native rebuild receipt proof is incomplete")
	}
	rebuildAttemptCount, err := rebuildAttemptFactCount(state.RebuildAttempts, proof.RebuildReceiptProofs)
	if err != nil {
		return err
	}
	counts := []struct {
		name     string
		actual   uint64
		expected uint64
	}{
		{"application rows", state.ApplicationRowCount, *client.RowCount},
		{"scope states", state.ScopeStateCount, *client.CheckpointCount},
		{"provenance rows", state.ProvenanceCount, *client.ProvenanceCount},
		{"active rebuild attempts", state.RebuildAttemptCount, uint64(len(state.RebuildAttempts))},
		{"rebuild receipts", state.RebuildReceiptCount, uint64(len(proof.RebuildReceiptProofs))},
		{"rejected mutations", state.RejectedMutationCount, 0},
		{"scope rows", state.ScopeRowCount, 1},
		{"row metadata", state.RowMetadataCount, 1},
		{"expected rebuild receipts", state.RebuildReceiptCount, 1},
		{"scope state details", uint64(len(state.ScopeStates)), 1},
		{"scope row details", uint64(len(state.ScopeRows)), 1},
		{"active rebuild attempt details", uint64(len(state.RebuildAttempts)), 0},
	}
	if client.QueueCount != nil {
		counts = append(counts, struct {
			name     string
			actual   uint64
			expected uint64
		}{"mutation ledger", state.MutationLedgerCount, *client.QueueCount})
	}
	if client.OutcomeCount != nil {
		counts = append(counts, struct {
			name     string
			actual   uint64
			expected uint64
		}{"mutation outcomes", state.MutationOutcomeCount, *client.OutcomeCount})
	}
	if client.SealedBatchCount != nil {
		counts = append(counts, struct {
			name     string
			actual   uint64
			expected uint64
		}{"sealed batches", state.SealedBatchCount, *client.SealedBatchCount})
	}
	if client.RebuildAttemptCount != nil {
		counts = append(counts, struct {
			name     string
			actual   uint64
			expected uint64
		}{"rebuild attempt facts", rebuildAttemptCount, *client.RebuildAttemptCount})
	}
	for _, count := range counts {
		if count.actual != count.expected {
			return fmt.Errorf("React Native durable %s = %d, want %d", count.name, count.actual, count.expected)
		}
	}
	metadata, err := durableRowMetadata(capture.DurableProof)
	if err != nil {
		return err
	}
	stateScope, stateRow := state.ScopeStates[0], state.ScopeRows[0]
	if stateScope.ScopeID == "" || stateScope.Cursor == nil || stateScope.Checksum == nil {
		return errors.New("React Native durable scope identity is incomplete")
	}
	scopeChecksum, scopeChecksumErr := checksumDigest(stateScope.Checksum)
	localChecksum, localChecksumErr := checksumDigest(&stateScope.LocalChecksum)
	rowChecksum, rowChecksumErr := checksumDigest(metadata.RowChecksum)
	if scopeChecksumErr != nil || localChecksumErr != nil || rowChecksumErr != nil {
		return errors.New("React Native durable checksum evidence is invalid")
	}
	if scopeChecksum == nil || localChecksum == nil || *localChecksum != *scopeChecksum {
		return errors.New("React Native durable scope checksum is inconsistent")
	}
	if stateRow.ScopeID != stateScope.ScopeID {
		return errors.New("React Native durable row scope is inconsistent")
	}
	if metadata.TableName != stateRow.TableName || metadata.RecordID != stateRow.RecordID {
		return errors.New("React Native durable row identity is inconsistent")
	}
	if rowChecksum == nil || *rowChecksum != stateRow.Checksum {
		return errors.New("React Native durable row checksum is inconsistent")
	}
	if client.Checkpoints[0].Checksum == nil {
		return errors.New("React Native durable scope state differs from the authored model")
	}
	if receipt := proof.RebuildReceiptProofs[0]; receipt.PageCount == 0 || receipt.ReturnedRecordCount != 0 ||
		!receipt.RequestChainValid || !receipt.RecordsInCanonicalOrder || !receipt.RowChecksumsValid ||
		!receipt.ScopeChecksumValid || receipt.FinalChecksumMatches {
		return errors.New("React Native rebuild receipt proof is invalid")
	}
	if err := validateEmptyArray(capture.Pending); err != nil {
		return errors.New("React Native pending mutations are not empty")
	}
	if err := validateEmptyArray(capture.Rejected); err != nil {
		return errors.New("React Native rejected mutations are not empty")
	}
	if err := validateReadyStatus(capture.Status); err != nil {
		return err
	}
	return validateProvenance(capture.Provenance, stateScope, stateRow)
}

func validateBootstrapRebuildEvidence(raw json.RawMessage, trace traceSnapshot) error {
	proof, err := decodeDurableProof(raw)
	if err != nil || proof.RowMetadata != nil || len(proof.RebuildReceiptProofs) != 1 {
		return errors.New("React Native bootstrap rebuild receipt proof is incomplete")
	}
	receipt := proof.RebuildReceiptProofs[0]
	fingerprint, err := requestString(trace.Observations[1], "rebuild_id_fingerprint")
	if err != nil || !validLowerHexDigest(receipt.RebuildIDFingerprint) || receipt.RebuildIDFingerprint != fingerprint ||
		receipt.PageCount == 0 || receipt.ReturnedRecordCount != 0 || !receipt.RequestChainValid ||
		!receipt.RecordsInCanonicalOrder || !receipt.RowChecksumsValid || !receipt.ScopeChecksumValid ||
		!receipt.FinalChecksumMatches {
		return errors.New("React Native bootstrap rebuild receipt proof is invalid")
	}
	return nil
}

func rebuildAttemptFactCount(attempts []rebuildAttempt, receipts []rebuildReceiptProof) (uint64, error) {
	identities := make(map[string]struct{}, len(attempts)+len(receipts))
	for _, attempt := range attempts {
		if attempt.RebuildID == "" {
			return 0, errors.New("React Native rebuild attempt identity is invalid")
		}
		identities[hashFingerprint(attempt.RebuildID)] = struct{}{}
	}
	for _, receipt := range receipts {
		if !validLowerHexDigest(receipt.RebuildIDFingerprint) {
			return 0, errors.New("React Native rebuild receipt identity is invalid")
		}
		identities[receipt.RebuildIDFingerprint] = struct{}{}
	}
	return uint64(len(identities)), nil
}

func validLowerHexDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
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

func validateEmptyArray(raw json.RawMessage) error {
	var values []json.RawMessage
	if err := decodeStrictValue(raw, &values); err != nil || len(values) != 0 {
		return errors.New("array is invalid")
	}
	return nil
}

func validateReadyStatus(raw json.RawMessage) error {
	var status syncStatus
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &members); err != nil || len(members) != 4 || jsonstrict.Decode(raw, &status) != nil ||
		status.State != "ready" || !isJSONNull(status.RetryAt) || !isJSONNull(status.Operation) || !isJSONNull(status.Failure) {
		return errors.New("React Native sync status is not ready")
	}
	return nil
}

func validateProvenance(raw json.RawMessage, scope clientScopeState, row clientScopeRow) error {
	var rows []clientScopeRow
	if err := decodeStrictValue(raw, &rows); err != nil || len(rows) != 1 {
		return errors.New("React Native provenance capture is invalid")
	}
	provenance := rows[0]
	if provenance.ScopeID != scope.ScopeID || provenance.TableName != row.TableName || provenance.RecordID != row.RecordID ||
		provenance.Checksum != row.Checksum || provenance.Generation != row.Generation {
		return errors.New("React Native provenance differs from durable state")
	}
	return nil
}

func validateClientStateAgainstModel(scenario scenarios.Scenario, capture *finalCapture, resolutions []blackbox.NativeIdentityResolution, tableName, primaryKey string) error {
	if capture == nil || tableName == "" || primaryKey == "" {
		return errors.New("React Native final application evidence is unavailable")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	metadata, err := durableRowMetadata(capture.DurableProof)
	if err != nil {
		return err
	}
	if len(resolutions) != len(warmConnectAliasNames) {
		return errors.New("React Native identity resolutions are incomplete")
	}
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(resolutions))
	for _, resolution := range resolutions {
		resolved[resolution.Alias] = resolution
	}
	expected := warmConnectExpectedState(scenario)
	if expected == nil || len(expected.Clients) != 1 || len(expected.Rows) != 1 || expected.Clients[0].CurrentSchema == nil ||
		len(expected.Clients[0].Provenance) != 1 || len(expected.Clients[0].Provenance[0].Scopes) != 1 ||
		len(expected.Clients[0].Checkpoints) != 1 || expected.Clients[0].Checkpoints[0].Checksum == nil {
		return errors.New("React Native authored schema identity is unavailable")
	}
	scopeChecksum, err := checksumDigest(state.ScopeStates[0].Checksum)
	if err != nil || scopeChecksum == nil {
		return errors.New("React Native scope checksum identity is invalid")
	}
	if state.ScopeRows[0].TableName != tableName || metadata.TableName != tableName {
		return errors.New("React Native durable table differs from its runtime application identity")
	}
	identityChecks := []struct {
		alias   string
		matches bool
	}{
		{"items-table", resolutionAuthoredStringMatches(resolved["items-table"], expected.Rows[0].TableID)},
		{"scope-a", resolutionStringMatches(resolved["scope-a"], expected.Clients[0].Provenance[0].Scopes[0], state.ScopeStates[0].ScopeID)},
		{"scope-a-checksum", resolutionStringMatches(resolved["scope-a-checksum"], *expected.Clients[0].Checkpoints[0].Checksum, *scopeChecksum)},
		{"row-a-checksum", resolutionStringMatches(resolved["row-a-checksum"], expected.Rows[0].Checksum, state.ScopeRows[0].Checksum)},
		{"row-a-version", resolutionStringMatches(resolved["row-a-version"], expected.Clients[0].Provenance[0].Version, metadata.ServerVersion)},
		{"current-schema", resolutionSchemaMatches(resolved["current-schema"], *expected.Clients[0].CurrentSchema, *state.Schema)},
	}
	for _, check := range identityChecks {
		if !check.matches {
			return fmt.Errorf("React Native alias %q differs from durable evidence", check.alias)
		}
	}
	rows, err := decodeRows(capture.Rows)
	if err != nil || len(rows) != 1 || !rowUsesRuntimePrimary(rows[0], primaryKey, metadata.RecordID) {
		return errors.New("React Native application row does not use its runtime primary key")
	}
	return validateFinalClientEvidence(scenario, state, *capture)
}

func resolutionStringMatches(resolution blackbox.NativeIdentityResolution, authored, runtime string) bool {
	var actualAuthored, actualRuntime string
	return json.Unmarshal(resolution.AuthoredValue, &actualAuthored) == nil && actualAuthored == authored &&
		json.Unmarshal(resolution.RuntimeValue, &actualRuntime) == nil && actualRuntime == runtime
}

func resolutionAuthoredStringMatches(resolution blackbox.NativeIdentityResolution, authored string) bool {
	var actual string
	return json.Unmarshal(resolution.AuthoredValue, &actual) == nil && actual == authored
}

func resolutionSchemaMatches(resolution blackbox.NativeIdentityResolution, authored scenarios.SchemaFact, runtime clientSchema) bool {
	var resolvedAuthored, resolvedRuntime clientSchema
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored.Version == authored.Version && resolvedAuthored.Hash == authored.Hash && resolvedRuntime == runtime
}

func validateServerState(expected scenarios.StateFacts, actual scenarios.StateFacts) error {
	serverExpected := scenarios.CloneStateFacts(expected)
	serverExpected.Clients = nil
	normalizedExpected, err := scenarios.NormalizeStateFacts(serverExpected)
	if err != nil {
		return fmt.Errorf("normalize React Native expected server state: %w", err)
	}
	normalizedActual, err := scenarios.NormalizeStateFacts(actual)
	if err != nil {
		return fmt.Errorf("normalize React Native actual server state: %w", err)
	}
	if !scenarios.StateFactsProjectionEqual(normalizedExpected, normalizedActual) {
		return errors.New("React Native server state differs from the authored model")
	}
	return nil
}

func hashFingerprint(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func copyRaw(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func decodeStrictValue(raw json.RawMessage, destination any) error {
	wrapped := make([]byte, 0, len(raw)+10)
	wrapped = append(wrapped, `{"value":`...)
	wrapped = append(wrapped, raw...)
	wrapped = append(wrapped, '}')
	var holder struct {
		Value json.RawMessage `json:"value"`
	}
	if err := jsonstrict.Decode(wrapped, &holder); err != nil {
		return err
	}
	return json.Unmarshal(holder.Value, destination)
}

func validConformanceError(value string) bool {
	switch value {
	case "invalid_command", "unavailable", "execution_failed", "capture_query_failed", "capture_row_cardinality", "capture_inspection_failed":
		return true
	default:
		return false
	}
}
