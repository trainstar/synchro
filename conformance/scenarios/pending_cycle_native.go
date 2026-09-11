package scenarios

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
)

// PendingCycleNativeTarget identifies the authored row after runtime binding.
type PendingCycleNativeTarget struct {
	TableName                   string
	PrimaryKeyField             string
	RecordID                    string
	ValueField                  string
	Value                       string
	UnprotectedAuthoredRecordID string
	UnprotectedRecordID         string
	UnprotectedValue            string
}

// PendingCycleNativeMutation records one inspectable local mutation.
type PendingCycleNativeMutation struct {
	Operation     string
	Status        string
	ClientVersion string
}

// PendingCycleNativeState records the bounded durable state for one inspection.
type PendingCycleNativeState struct {
	ProcessID                   string
	DatabaseIdentityFingerprint string
	ApplicationRowCount         int
	PendingChangeCount          int
	MutationLedgerCount         int
	MutationOutcomeCount        int
	RejectedMutationCount       int
	ScopeStateCount             int
	ScopeRowCount               int
	RowMetadataCount            int
	TargetRowPresent            bool
	TargetRowValue              string
	TargetMutations             []PendingCycleNativeMutation
	TargetServerVersion         string
	TargetRowChecksum           string
	ScopeID                     string
	ScopeCursor                 string
	ScopeChecksum               string
	LocalScopeChecksum          string
	TargetScopeRowPresent       bool
	TargetScopeRowChecksum      string
	UnprotectedRowPresent       bool
	UnprotectedRowValue         string
	UnprotectedScopeRowPresent  bool
	UnprotectedScopeRowChecksum string
}

// PendingCycleNativeEvidence binds each client transition to direct inspection.
type PendingCycleNativeEvidence struct {
	Target        PendingCycleNativeTarget
	UpdatedValue  string
	BeforeWrite   PendingCycleNativeState
	AfterWrite    PendingCycleNativeState
	AfterPush     PendingCycleNativeState
	BeforePull    PendingCycleNativeState
	AfterPull     PendingCycleNativeState
	AfterRestart  PendingCycleNativeState
	BeforeCleanup PendingCycleNativeState
	AfterCleanup  PendingCycleNativeState
	AfterUpdate   PendingCycleNativeState
	BeforeDelete  PendingCycleNativeState
	AfterDelete   PendingCycleNativeState
}

// ValidatePendingCycleNativeEvidence checks the native pending-cycle transitions.
// Swift, Kotlin, and React Native provide direct consumers of this semantic check.
func ValidatePendingCycleNativeEvidence(evidence PendingCycleNativeEvidence) error {
	target := evidence.Target
	if target.TableName == "" || target.PrimaryKeyField == "" || target.RecordID == "" || target.ValueField == "" || target.Value == "" ||
		target.UnprotectedAuthoredRecordID == "" || target.UnprotectedRecordID == "" || target.UnprotectedRecordID == target.RecordID ||
		target.UnprotectedValue == "" || evidence.UpdatedValue == "" || evidence.UpdatedValue == target.Value {
		return errors.New("pending-cycle runtime target is incomplete")
	}
	initialProcessStates := []PendingCycleNativeState{
		evidence.BeforeWrite,
		evidence.AfterWrite,
		evidence.AfterPush,
		evidence.BeforePull,
		evidence.AfterPull,
	}
	for _, state := range initialProcessStates {
		if state.ProcessID == "" || state.DatabaseIdentityFingerprint == "" || state.ProcessID != evidence.BeforeWrite.ProcessID || state.DatabaseIdentityFingerprint != evidence.BeforeWrite.DatabaseIdentityFingerprint {
			return errors.New("pending-cycle inspection did not stay in one native process and database")
		}
	}
	for _, state := range []PendingCycleNativeState{evidence.BeforeCleanup, evidence.AfterCleanup, evidence.AfterUpdate, evidence.BeforeDelete, evidence.AfterDelete} {
		if state.ProcessID != evidence.AfterRestart.ProcessID || state.DatabaseIdentityFingerprint != evidence.AfterRestart.DatabaseIdentityFingerprint {
			return errors.New("pending-cycle generated work did not stay in the restarted process and database")
		}
	}

	before := evidence.BeforeWrite
	if before.ApplicationRowCount != 0 || before.PendingChangeCount != 0 || before.MutationLedgerCount != 0 || before.MutationOutcomeCount != 0 || before.RejectedMutationCount != 0 || before.ScopeStateCount != 1 || before.ScopeRowCount != 0 || before.RowMetadataCount != 0 || before.TargetRowPresent || len(before.TargetMutations) != 0 || !pendingCycleUnprotectedStateAbsent(before) {
		return errors.New("pending-cycle pre-write state is not empty")
	}

	written := evidence.AfterWrite
	if written.ApplicationRowCount != 1 || written.PendingChangeCount != 1 || written.MutationLedgerCount != 1 || written.MutationOutcomeCount != 0 || written.RejectedMutationCount != 0 || written.ScopeStateCount != 1 || written.ScopeRowCount != 0 || written.RowMetadataCount != 0 || !written.TargetRowPresent || written.TargetRowValue != target.Value || len(written.TargetMutations) != 1 || !pendingCycleUnprotectedStateAbsent(written) {
		return errors.New("pending-cycle local SQLite write is not durable and queued")
	}
	if written.TargetMutations[0].Operation != "insert" || written.TargetMutations[0].Status != "pending" || written.TargetMutations[0].ClientVersion == "" {
		return errors.New("pending-cycle local SQLite mutation is not a pending insert")
	}

	pushed := evidence.AfterPush
	if pushed.ApplicationRowCount != 1 || pushed.PendingChangeCount != 0 || pushed.MutationLedgerCount != 1 || pushed.MutationOutcomeCount != 1 || pushed.RejectedMutationCount != 0 || pushed.ScopeStateCount != 1 || pushed.ScopeRowCount != 0 || pushed.RowMetadataCount != 1 || !pushed.TargetRowPresent || pushed.TargetRowValue != target.Value || len(pushed.TargetMutations) != 0 || pushed.TargetServerVersion == "" || pushed.TargetRowChecksum == "" || !pendingCycleUnprotectedStateAbsent(pushed) {
		return errors.New("pending-cycle accepted push state is incomplete")
	}
	if pushed.TargetServerVersion == written.TargetMutations[0].ClientVersion {
		return errors.New("pending-cycle accepted push reused the client version")
	}
	if !reflect.DeepEqual(pushed, evidence.BeforePull) {
		return errors.New("pending-cycle server materialization changed client state before pull")
	}

	pulled := evidence.AfterPull
	if pulled.ApplicationRowCount != 2 || pulled.PendingChangeCount != 0 || pulled.MutationLedgerCount != 1 || pulled.MutationOutcomeCount != 1 || pulled.RejectedMutationCount != 0 || pulled.ScopeStateCount != 1 || pulled.ScopeRowCount != 2 || pulled.RowMetadataCount != 2 || !pulled.TargetRowPresent || pulled.TargetRowValue != target.Value || !pulled.UnprotectedRowPresent || pulled.UnprotectedRowValue != target.UnprotectedValue || len(pulled.TargetMutations) != 0 {
		return errors.New("pending-cycle pull state contains an echo mutation or partial row")
	}
	if pulled.TargetServerVersion != pushed.TargetServerVersion || pulled.TargetRowChecksum != pushed.TargetRowChecksum || pulled.ScopeID == "" || pulled.ScopeCursor == "" || pulled.ScopeCursor == evidence.BeforePull.ScopeCursor || pulled.ScopeChecksum == "" || pulled.ScopeChecksum != pulled.LocalScopeChecksum || !pulled.TargetScopeRowPresent || pulled.TargetScopeRowChecksum != pulled.TargetRowChecksum || !pulled.UnprotectedScopeRowPresent || pulled.UnprotectedScopeRowChecksum == "" {
		return errors.New("pending-cycle row, cursor, version, and checksum did not advance atomically")
	}

	restarted := evidence.AfterRestart
	if restarted.ProcessID == "" || restarted.ProcessID == pulled.ProcessID || restarted.DatabaseIdentityFingerprint != pulled.DatabaseIdentityFingerprint {
		return errors.New("pending-cycle restart did not replace the process and retain the database")
	}
	normalizedRestarted := restarted
	normalizedRestarted.ProcessID = pulled.ProcessID
	if !reflect.DeepEqual(pulled, normalizedRestarted) {
		return errors.New("pending-cycle restart changed durable row, queue, or cursor state")
	}

	pendingUpdate := evidence.BeforeCleanup
	if pendingUpdate.ApplicationRowCount != 2 || pendingUpdate.PendingChangeCount != 1 || pendingUpdate.MutationLedgerCount != restarted.MutationLedgerCount+1 || pendingUpdate.MutationOutcomeCount != restarted.MutationOutcomeCount || pendingUpdate.RejectedMutationCount != 0 || pendingUpdate.TargetRowValue != evidence.UpdatedValue || !pendingUpdate.TargetRowPresent || !pendingUpdate.UnprotectedRowPresent || pendingUpdate.UnprotectedRowValue != target.UnprotectedValue || len(pendingUpdate.TargetMutations) != 1 || pendingUpdate.TargetMutations[0].Operation != "update" || pendingUpdate.TargetMutations[0].Status != "pending" || pendingUpdate.TargetMutations[0].ClientVersion == "" {
		return errors.New("pending-cycle update intent is not pending before cleanup")
	}
	expectedPendingUpdate := restarted
	expectedPendingUpdate.PendingChangeCount = 1
	expectedPendingUpdate.MutationLedgerCount++
	expectedPendingUpdate.TargetRowValue = evidence.UpdatedValue
	expectedPendingUpdate.TargetMutations = pendingUpdate.TargetMutations
	if !reflect.DeepEqual(expectedPendingUpdate, pendingUpdate) {
		return errors.New("pending-cycle pending update changed unrelated durable state")
	}

	cleaned := evidence.AfterCleanup
	expectedCleaned := pendingUpdate
	expectedCleaned.ApplicationRowCount--
	expectedCleaned.RowMetadataCount--
	expectedCleaned.ScopeRowCount = 0
	expectedCleaned.ScopeID = cleaned.ScopeID
	expectedCleaned.ScopeCursor = ""
	expectedCleaned.ScopeChecksum = ""
	expectedCleaned.LocalScopeChecksum = ""
	expectedCleaned.TargetScopeRowPresent = false
	expectedCleaned.TargetScopeRowChecksum = ""
	expectedCleaned.UnprotectedRowPresent = false
	expectedCleaned.UnprotectedRowValue = ""
	expectedCleaned.UnprotectedScopeRowPresent = false
	expectedCleaned.UnprotectedScopeRowChecksum = ""
	if cleaned.ScopeStateCount != 1 || cleaned.ScopeID == "" || cleaned.ScopeID == pendingUpdate.ScopeID || !reflect.DeepEqual(expectedCleaned, cleaned) {
		return errors.New("pending-cycle scope cleanup did not retain pending intent and remove unprotected cache state")
	}

	updated := evidence.AfterUpdate
	if updated.PendingChangeCount != 0 || updated.MutationLedgerCount != cleaned.MutationLedgerCount || updated.MutationOutcomeCount != cleaned.MutationOutcomeCount+1 || len(updated.TargetMutations) != 0 || updated.TargetServerVersion == "" || updated.TargetServerVersion == cleaned.TargetServerVersion || updated.TargetRowChecksum == "" || updated.TargetRowChecksum == cleaned.TargetRowChecksum || updated.ScopeID != cleaned.ScopeID || updated.ScopeCursor == "" || updated.ScopeChecksum == "" || updated.ScopeChecksum != updated.LocalScopeChecksum || updated.ScopeRowCount != 0 || updated.TargetScopeRowPresent {
		return errors.New("pending-cycle synchronized update outcome is incomplete")
	}
	expectedUpdated := cleaned
	expectedUpdated.PendingChangeCount = 0
	expectedUpdated.MutationOutcomeCount++
	expectedUpdated.TargetMutations = nil
	expectedUpdated.TargetServerVersion = updated.TargetServerVersion
	expectedUpdated.TargetRowChecksum = updated.TargetRowChecksum
	expectedUpdated.ScopeCursor = updated.ScopeCursor
	expectedUpdated.ScopeChecksum = updated.ScopeChecksum
	expectedUpdated.LocalScopeChecksum = updated.LocalScopeChecksum
	if !reflect.DeepEqual(expectedUpdated, updated) {
		return errors.New("pending-cycle synchronized update changed unrelated durable state")
	}

	pendingDelete := evidence.BeforeDelete
	if pendingDelete.ApplicationRowCount != 0 || pendingDelete.PendingChangeCount != 1 || pendingDelete.MutationLedgerCount != updated.MutationLedgerCount+1 || pendingDelete.MutationOutcomeCount != updated.MutationOutcomeCount || pendingDelete.TargetRowPresent || pendingDelete.TargetRowValue != "" || len(pendingDelete.TargetMutations) != 1 || pendingDelete.TargetMutations[0].Operation != "delete" || pendingDelete.TargetMutations[0].Status != "pending" || pendingDelete.TargetMutations[0].ClientVersion == "" {
		return errors.New("pending-cycle delete intent is not pending before synchronization")
	}
	expectedPendingDelete := updated
	expectedPendingDelete.ApplicationRowCount = 0
	expectedPendingDelete.PendingChangeCount = 1
	expectedPendingDelete.MutationLedgerCount++
	expectedPendingDelete.TargetRowPresent = false
	expectedPendingDelete.TargetRowValue = ""
	expectedPendingDelete.TargetMutations = pendingDelete.TargetMutations
	if !reflect.DeepEqual(expectedPendingDelete, pendingDelete) {
		return errors.New("pending-cycle pending delete changed unrelated durable state")
	}

	deleted := evidence.AfterDelete
	if deleted.PendingChangeCount != 0 || deleted.MutationLedgerCount != pendingDelete.MutationLedgerCount || deleted.MutationOutcomeCount != pendingDelete.MutationOutcomeCount+1 || len(deleted.TargetMutations) != 0 || deleted.TargetServerVersion == "" || deleted.TargetServerVersion == pendingDelete.TargetServerVersion || deleted.TargetRowChecksum == "" || deleted.TargetRowChecksum == pendingDelete.TargetRowChecksum || deleted.ScopeID != pendingDelete.ScopeID || deleted.ScopeCursor == "" || deleted.ScopeCursor == pendingDelete.ScopeCursor || deleted.ScopeChecksum == "" || deleted.ScopeChecksum != pendingDelete.ScopeChecksum || deleted.LocalScopeChecksum != pendingDelete.LocalScopeChecksum || deleted.ScopeRowCount != 0 || deleted.TargetScopeRowPresent {
		return errors.New("pending-cycle synchronized delete outcome is incomplete")
	}
	expectedDeleted := pendingDelete
	expectedDeleted.PendingChangeCount = 0
	expectedDeleted.MutationOutcomeCount++
	expectedDeleted.TargetMutations = nil
	expectedDeleted.TargetServerVersion = deleted.TargetServerVersion
	expectedDeleted.TargetRowChecksum = deleted.TargetRowChecksum
	expectedDeleted.ScopeCursor = deleted.ScopeCursor
	if !reflect.DeepEqual(expectedDeleted, deleted) {
		return errors.New("pending-cycle synchronized delete changed unrelated durable state")
	}
	return nil
}

func pendingCycleUnprotectedStateAbsent(state PendingCycleNativeState) bool {
	return !state.UnprotectedRowPresent && state.UnprotectedRowValue == "" && !state.UnprotectedScopeRowPresent && state.UnprotectedScopeRowChecksum == ""
}

// PendingCycleUnprotectedRowTarget binds the authored cache row to its runtime identity.
func PendingCycleUnprotectedRowTarget(operation Operation, aliases []NativeIdentityAlias, runtimeRecordID string) (string, string, error) {
	if OperationKey(operation) != "model/commit-source-transaction" || ValidateOperation(operation) != nil || runtimeRecordID == "" {
		return "", "", errors.New("pending-cycle unprotected source row is invalid")
	}
	var payload struct {
		Events []struct {
			Operation string `json:"operation"`
			After     *struct {
				Identity struct {
					SyncedRow *struct {
						PrimaryKeyFieldID string `json:"primary_key_field_id"`
						CanonicalWireJSON string `json:"canonical_wire_json"`
					} `json:"synced_row"`
				} `json:"identity"`
				Fields []struct {
					Field    string `json:"field"`
					WireJSON string `json:"wire_json"`
				} `json:"fields"`
			} `json:"after"`
		} `json:"events"`
	}
	if json.Unmarshal(operation.Payload, &payload) != nil || len(payload.Events) != 1 || payload.Events[0].Operation != "insert" || payload.Events[0].After == nil || payload.Events[0].After.Identity.SyncedRow == nil {
		return "", "", errors.New("pending-cycle unprotected source row is invalid")
	}
	var authoredRecordID string
	if json.Unmarshal([]byte(payload.Events[0].After.Identity.SyncedRow.CanonicalWireJSON), &authoredRecordID) != nil || authoredRecordID == "" {
		return "", "", errors.New("pending-cycle unprotected source identity is invalid")
	}
	alias, err := PendingCycleUnprotectedIdentityAlias(aliases)
	if err != nil {
		return "", "", err
	}
	var aliasValue string
	if json.Unmarshal(alias.Value, &aliasValue) != nil || aliasValue != authoredRecordID {
		return "", "", errors.New("pending-cycle unprotected alias differs from its source row")
	}
	primaryField := payload.Events[0].After.Identity.SyncedRow.PrimaryKeyFieldID
	if primaryField == "" {
		return "", "", errors.New("pending-cycle unprotected source primary field is absent")
	}
	value := ""
	for _, field := range payload.Events[0].After.Fields {
		if field.Field == primaryField {
			continue
		}
		var text string
		if json.Unmarshal([]byte(field.WireJSON), &text) != nil || text == "" {
			return "", "", errors.New("pending-cycle unprotected source value is invalid")
		}
		if value != "" {
			return "", "", errors.New("pending-cycle unprotected source value is ambiguous")
		}
		value = text
	}
	if value == "" {
		return "", "", errors.New("pending-cycle unprotected source value is absent")
	}
	return authoredRecordID, value, nil
}

// PendingCycleUnprotectedIdentityAlias returns the single authored cache-row alias.
func PendingCycleUnprotectedIdentityAlias(aliases []NativeIdentityAlias) (NativeIdentityAlias, error) {
	var result NativeIdentityAlias
	for _, alias := range aliases {
		if alias.Alias != "unprotected-row-primary-key" || alias.Kind != "primary-key" {
			continue
		}
		if result.Alias != "" {
			return NativeIdentityAlias{}, errors.New("pending-cycle unprotected source alias is duplicated")
		}
		result = alias
	}
	if result.Alias == "" {
		return NativeIdentityAlias{}, errors.New("pending-cycle unprotected source alias is unavailable")
	}
	return result, nil
}

// ValidatePendingCycleServerFacts proves that only the unprotected source row remains authoritative.
func ValidatePendingCycleServerFacts(facts StateFacts, target PendingCycleNativeTarget) error {
	if facts.RowCount == nil || *facts.RowCount != 1 || len(facts.Rows) != 1 || target.UnprotectedAuthoredRecordID == "" {
		return errors.New("pending-cycle final server row count is invalid")
	}
	row := facts.Rows[0]
	if row.CanonicalWireJSON != strconv.Quote(target.UnprotectedAuthoredRecordID) || row.Version == "" || row.Checksum == "" {
		return errors.New("pending-cycle unprotected server row is absent")
	}
	return nil
}

// PendingCycleCleanupAssignment moves the authored client to an empty scope.
func PendingCycleCleanupAssignment(userID, clientID string) (Operation, error) {
	payload, err := json.Marshal(map[string]any{
		"user_id":   userID,
		"client_id": clientID,
		"assignments": []map[string]string{
			{"scope_id": "scope-b"},
		},
	})
	if err != nil {
		return Operation{}, errors.New("encode pending-cycle cleanup assignment failed")
	}
	operation := Operation{ContractOperation: "model", Name: "set-client-assignments", Payload: payload}
	if err := ValidateOperation(operation); err != nil {
		return Operation{}, errors.New("pending-cycle cleanup assignment is invalid")
	}
	return operation, nil
}

// PendingCycleNativeCRUDStep binds one local SQL operation to its push and WAL materialization.
type PendingCycleNativeCRUDStep struct {
	LocalWrite      Operation
	ApplicationPush Operation
	Materialize     Operation
}

// PendingCycleSynchronizedCRUDOperation derives one generated native CRUD step.
func PendingCycleSynchronizedCRUDOperation(boundInsert, authoredPush, authoredMaterialize Operation, operation, runtimeValueField, currentValue, nextValue, baseVersion string) (PendingCycleNativeCRUDStep, error) {
	if OperationKey(boundInsert) != "local/write" || OperationKey(authoredPush) != "push/submit" || OperationKey(authoredMaterialize) != "process/materialize-source-transaction" || runtimeValueField == "" || baseVersion == "" || (operation != "update" && operation != "delete") || (operation == "update" && (currentValue == "" || nextValue == "" || currentValue == nextValue)) {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle synchronized CRUD source is invalid")
	}
	if ValidateOperation(boundInsert) != nil || ValidateOperation(authoredPush) != nil || ValidateOperation(authoredMaterialize) != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle synchronized CRUD source operation is invalid")
	}
	encodedString := func(value string) json.RawMessage {
		return json.RawMessage(strconv.Quote(value))
	}

	var localPayload map[string]json.RawMessage
	if err := json.Unmarshal(boundInsert.Payload, &localPayload); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("decode pending-cycle local CRUD source failed")
	}
	var sourceOperation string
	if json.Unmarshal(localPayload["operation"], &sourceOperation) != nil || sourceOperation != "insert" {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle local CRUD source is not an insert")
	}
	localPayload["operation"] = encodedString(operation)
	if operation == "update" {
		columns, err := json.Marshal(map[string]string{runtimeValueField: nextValue})
		if err != nil {
			return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle local update columns failed")
		}
		localPayload["columns"] = columns
	} else {
		localPayload["columns"] = json.RawMessage(`{}`)
	}
	localEncoded, err := json.Marshal(localPayload)
	if err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle local CRUD operation failed")
	}
	localWrite := boundInsert
	localWrite.Payload = localEncoded
	if err := ValidateOperation(localWrite); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle local CRUD operation is invalid")
	}

	var pushPayload map[string]json.RawMessage
	if err := json.Unmarshal(authoredPush.Payload, &pushPayload); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("decode pending-cycle application push failed")
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal(pushPayload["request"], &request); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("decode pending-cycle application push request failed")
	}
	var mutations []map[string]json.RawMessage
	if err := json.Unmarshal(request["mutations"], &mutations); err != nil || len(mutations) != 1 {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle application push mutation is invalid")
	}
	var sourcePushOperation string
	if json.Unmarshal(mutations[0]["op"], &sourcePushOperation) != nil || sourcePushOperation != "insert" {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle application push source is not an insert")
	}
	identities := map[string]struct {
		mutation string
		batch    string
		commit   string
		end      string
	}{
		"update": {mutation: "00000000-0000-4000-8000-000000004101", batch: "00000000-0000-4000-8000-000000004102", commit: "22", end: "23"},
		"delete": {mutation: "00000000-0000-4000-8000-000000004201", batch: "00000000-0000-4000-8000-000000004202", commit: "24", end: "25"},
	}[operation]
	mutations[0]["op"] = encodedString(operation)
	mutations[0]["mutation_id"] = encodedString(identities.mutation)
	mutations[0]["base_version"] = encodedString(baseVersion)
	request["batch_id"] = encodedString(identities.batch)
	pushPayload["commit_lsn"] = encodedString(identities.commit)
	pushPayload["end_lsn"] = encodedString(identities.end)
	if operation == "update" {
		var columns map[string]json.RawMessage
		if err := json.Unmarshal(mutations[0]["columns"], &columns); err != nil {
			return PendingCycleNativeCRUDStep{}, errors.New("decode pending-cycle application update columns failed")
		}
		matches := 0
		for field, raw := range columns {
			var value string
			if json.Unmarshal(raw, &value) == nil && value == currentValue {
				columns[field] = encodedString(nextValue)
				matches++
			}
		}
		if matches != 1 {
			return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle application update value field is ambiguous")
		}
		encodedColumns, err := json.Marshal(columns)
		if err != nil {
			return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle application update columns failed")
		}
		mutations[0]["columns"] = encodedColumns
	} else {
		delete(mutations[0], "columns")
	}
	encodedMutations, err := json.Marshal(mutations)
	if err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle application push mutations failed")
	}
	request["mutations"] = encodedMutations
	encodedRequest, err := json.Marshal(request)
	if err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle application push request failed")
	}
	pushPayload["request"] = encodedRequest
	pushEncoded, err := json.Marshal(pushPayload)
	if err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle application push failed")
	}
	applicationPush := authoredPush
	applicationPush.Payload = pushEncoded
	applicationPush.WireFault = nil
	if err := ValidateOperation(applicationPush); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle application push is invalid")
	}

	var materializePayload map[string]json.RawMessage
	if err := json.Unmarshal(authoredMaterialize.Payload, &materializePayload); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("decode pending-cycle materialization failed")
	}
	materializePayload["commit_lsn"] = encodedString(identities.commit)
	materializeEncoded, err := json.Marshal(materializePayload)
	if err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("encode pending-cycle materialization failed")
	}
	materialize := authoredMaterialize
	materialize.Payload = materializeEncoded
	if err := ValidateOperation(materialize); err != nil {
		return PendingCycleNativeCRUDStep{}, errors.New("pending-cycle materialization is invalid")
	}
	return PendingCycleNativeCRUDStep{LocalWrite: localWrite, ApplicationPush: applicationPush, Materialize: materialize}, nil
}

// PendingCycleTemporaryUnavailablePush marks a generated push for bounded native fault injection.
func PendingCycleTemporaryUnavailablePush(operation Operation) (Operation, error) {
	operation.WireFault = &WireFaultControl{Mode: wireFaultTemporaryUnavailable}
	if err := ValidateOperation(operation); err != nil {
		return Operation{}, errors.New("pending-cycle temporary-unavailable push is invalid")
	}
	return operation, nil
}
