package scenarios

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// NativeCRUDSchema identifies the current authored schema for direct client CRUD checks.
type NativeCRUDSchema struct {
	Version uint64
	Hash    string
	Tables  []NativeCRUDSchemaTable
}

// NativeCRUDSchemaTable identifies one registered synced table.
type NativeCRUDSchemaTable struct {
	TableID           string
	PrimaryKeyFieldID string
	Fields            []NativeCRUDSchemaField
}

// NativeCRUDSchemaField identifies one field needed by a generated local write.
type NativeCRUDSchemaField struct {
	FieldID    string
	Type       string
	PrimaryKey bool
	Writable   bool
}

// NativeCRUDInspection identifies an isolated client and server stream for generated CRUD checks.
type NativeCRUDInspection struct {
	ScopeID          string
	StreamGeneration string
}

// NativeCRUDPlanTarget identifies one generated row before runtime schema binding.
type NativeCRUDPlanTarget struct {
	TableID           string
	PrimaryKeyFieldID string
	RecordID          string
	ValueFieldID      string
	InitialMarker     string
	UpdatedMarker     string
	insertColumns     map[string]string
}

// NativeCRUDPlan derives local SQL, canonical push, and WAL operations for all registered tables.
type NativeCRUDPlan struct {
	schema           NativeCRUDSchema
	streamGeneration string
	userID           string
	clientID         string
	targets          []NativeCRUDPlanTarget
}

// NativeCRUDStep binds one local SQL phase to its canonical push and WAL materialization.
type NativeCRUDStep struct {
	Operation       string
	ClientVersion   string
	LocalWrites     []Operation
	ApplicationPush Operation
	Materialize     Operation
}

// NativeCRUDTarget identifies one generated row after runtime schema binding.
type NativeCRUDTarget struct {
	TableID         string
	TableName       string
	PrimaryKeyField string
	RecordID        string
	ValueField      string
	InitialValue    json.RawMessage
	UpdatedValue    json.RawMessage
}

// NativeCRUDMutation records one inspectable local mutation for a generated row.
type NativeCRUDMutation struct {
	Operation     string
	Status        string
	ClientVersion string
}

// NativeCRUDRowState records one generated row at one inspection barrier.
type NativeCRUDRowState struct {
	TableID       string
	Present       bool
	Value         json.RawMessage
	Mutation      *NativeCRUDMutation
	ServerVersion string
	RowChecksum   string
}

// NativeCRUDState records bounded durable client state at one inspection barrier.
type NativeCRUDState struct {
	ProcessID                   string
	DatabaseIdentityFingerprint string
	ApplicationRowCount         int
	PendingChangeCount          int
	MutationLedgerCount         int
	MutationOutcomeCount        int
	RejectedMutationCount       int
	RowMetadataCount            int
	Rows                        []NativeCRUDRowState
}

// NativeCRUDTransport records one request and response from a generated synchronization call.
type NativeCRUDTransport struct {
	OperationClass       string
	StatusCode           int
	ErrorCode            string
	Retryable            bool
	RetryablePresent     bool
	MutationCount        int
	MutationCountPresent bool
}

// NativeCRUDResponse records one generated synchronization call.
type NativeCRUDResponse struct {
	Operation  string
	Completion string
	Transport  []NativeCRUDTransport
}

// NativeCRUDEvidence records local, response, and restart state for generated CRUD.
type NativeCRUDEvidence struct {
	Targets             []NativeCRUDTarget
	Before              NativeCRUDState
	AfterInsertWrite    NativeCRUDState
	AfterInsertResponse NativeCRUDState
	AfterInsertRestart  NativeCRUDState
	AfterUpdateWrite    NativeCRUDState
	AfterUpdateResponse NativeCRUDState
	AfterUpdateRestart  NativeCRUDState
	AfterDeleteWrite    NativeCRUDState
	AfterDeleteResponse NativeCRUDState
	AfterDeleteRestart  NativeCRUDState
	Responses           []NativeCRUDResponse
}

// NativeCRUDInspectionForSetup selects an empty authored scope and the installed WAL stream.
func NativeCRUDInspectionForSetup(setup Operation, userID string) (NativeCRUDInspection, error) {
	if OperationKey(setup) != "model/install-current-contract" || userID == "" || ValidateOperation(setup) != nil {
		return NativeCRUDInspection{}, errors.New("native CRUD inspection setup is invalid")
	}
	var payload struct {
		Stream struct {
			StreamGeneration string `json:"stream_generation"`
		} `json:"stream"`
		EmptyScopes []struct {
			ScopeID string `json:"scope_id"`
		} `json:"empty_scopes"`
		Clients []struct {
			UserID           string   `json:"user_id"`
			AssignedScopeIDs []string `json:"assigned_scope_ids"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(setup.Payload, &payload); err != nil || payload.Stream.StreamGeneration == "" {
		return NativeCRUDInspection{}, errors.New("native CRUD inspection setup is invalid")
	}
	assigned := make(map[string]struct{})
	for _, client := range payload.Clients {
		if client.UserID != userID {
			continue
		}
		for _, scopeID := range client.AssignedScopeIDs {
			assigned[scopeID] = struct{}{}
		}
	}
	for _, scope := range payload.EmptyScopes {
		if scope.ScopeID == "" {
			return NativeCRUDInspection{}, errors.New("native CRUD inspection scope is invalid")
		}
		if _, used := assigned[scope.ScopeID]; !used {
			return NativeCRUDInspection{ScopeID: scope.ScopeID, StreamGeneration: payload.Stream.StreamGeneration}, nil
		}
	}
	return NativeCRUDInspection{}, errors.New("native CRUD inspection has no isolated empty scope")
}

// NativeCRUDInspectionAssignment moves the inspection user to one isolated authored scope.
func NativeCRUDInspectionAssignment(userID, clientID, scopeID string) (Operation, error) {
	if userID == "" || clientID == "" || scopeID == "" {
		return Operation{}, errors.New("native CRUD inspection assignment is invalid")
	}
	payload, err := json.Marshal(map[string]any{
		"user_id":     userID,
		"client_id":   clientID,
		"assignments": []map[string]string{{"scope_id": scopeID}},
	})
	if err != nil {
		return Operation{}, errors.New("encode native CRUD inspection assignment failed")
	}
	operation := Operation{ContractOperation: "model", Name: "set-client-assignments", Payload: payload}
	if err := ValidateOperation(operation); err != nil {
		return Operation{}, errors.New("native CRUD inspection assignment is invalid")
	}
	return operation, nil
}

// NewNativeCRUDPlan creates generated CRUD for every table in the supplied current schema.
func NewNativeCRUDPlan(schema NativeCRUDSchema, streamGeneration, userID, clientID string) (NativeCRUDPlan, error) {
	if schema.Version == 0 || schema.Hash == "" || len(schema.Tables) == 0 || streamGeneration == "" || userID == "" || clientID == "" {
		return NativeCRUDPlan{}, errors.New("native CRUD plan is incomplete")
	}
	tables := append([]NativeCRUDSchemaTable(nil), schema.Tables...)
	sort.Slice(tables, func(left, right int) bool { return tables[left].TableID < tables[right].TableID })
	seenTables := make(map[string]struct{}, len(tables))
	targets := make([]NativeCRUDPlanTarget, 0, len(tables))
	for tableOrdinal, table := range tables {
		if table.TableID == "" || table.PrimaryKeyFieldID == "" {
			return NativeCRUDPlan{}, errors.New("native CRUD table identity is incomplete")
		}
		if _, duplicate := seenTables[table.TableID]; duplicate {
			return NativeCRUDPlan{}, errors.New("native CRUD table identity is duplicated")
		}
		seenTables[table.TableID] = struct{}{}
		fields := append([]NativeCRUDSchemaField(nil), table.Fields...)
		sort.Slice(fields, func(left, right int) bool { return fields[left].FieldID < fields[right].FieldID })
		seenFields := make(map[string]struct{}, len(fields))
		primaryFound := false
		valueField := ""
		columns := make(map[string]string)
		for _, field := range fields {
			if field.FieldID == "" {
				return NativeCRUDPlan{}, errors.New("native CRUD field identity is incomplete")
			}
			if _, duplicate := seenFields[field.FieldID]; duplicate {
				return NativeCRUDPlan{}, errors.New("native CRUD field identity is duplicated")
			}
			seenFields[field.FieldID] = struct{}{}
			if field.FieldID == table.PrimaryKeyFieldID {
				primaryFound = field.PrimaryKey && field.Type == "string" && !field.Writable
				continue
			}
			if !field.Writable {
				continue
			}
			if field.Type != "string" {
				return NativeCRUDPlan{}, fmt.Errorf("native CRUD table %q has unsupported writable field type %q", table.TableID, field.Type)
			}
			marker := fmt.Sprintf("native-crud-%s-%s-insert", table.TableID, field.FieldID)
			columns[field.FieldID] = marker
			if valueField == "" {
				valueField = field.FieldID
			}
		}
		if !primaryFound || valueField == "" {
			return NativeCRUDPlan{}, fmt.Errorf("native CRUD table %q has no supported primary key and value field", table.TableID)
		}
		targets = append(targets, NativeCRUDPlanTarget{
			TableID:           table.TableID,
			PrimaryKeyFieldID: table.PrimaryKeyFieldID,
			RecordID:          fmt.Sprintf("native-crud-%03d-%s", tableOrdinal+1, table.TableID),
			ValueFieldID:      valueField,
			InitialMarker:     columns[valueField],
			UpdatedMarker:     fmt.Sprintf("native-crud-%s-%s-update", table.TableID, valueField),
			insertColumns:     columns,
		})
	}
	return NativeCRUDPlan{schema: schema, streamGeneration: streamGeneration, userID: userID, clientID: clientID, targets: targets}, nil
}

// Targets returns the generated authored rows in stable table order.
func (p NativeCRUDPlan) Targets() []NativeCRUDPlanTarget {
	return append([]NativeCRUDPlanTarget(nil), p.targets...)
}

// Step derives one generated create, update, or delete phase.
func (p NativeCRUDPlan) Step(operation string, baseVersions map[string]string, commitLSN uint64) (NativeCRUDStep, error) {
	if operation != "insert" && operation != "update" && operation != "delete" || commitLSN == 0 || len(p.targets) == 0 {
		return NativeCRUDStep{}, errors.New("native CRUD operation phase is invalid")
	}
	if operation == "insert" && len(baseVersions) != 0 || operation != "insert" && len(baseVersions) != len(p.targets) {
		return NativeCRUDStep{}, errors.New("native CRUD base-version set is invalid")
	}
	phaseOrdinal := map[string]int{"insert": 1, "update": 2, "delete": 3}[operation]
	clientVersion := fmt.Sprintf("2026-08-11T00:30:%02d.000000Z", phaseOrdinal)
	localWrites := make([]Operation, 0, len(p.targets))
	mutations := make([]map[string]any, 0, len(p.targets))
	for _, target := range p.targets {
		baseVersion := ""
		if operation != "insert" {
			baseVersion = baseVersions[target.TableID]
			if baseVersion == "" {
				return NativeCRUDStep{}, fmt.Errorf("native CRUD %s base version for table %q is absent", operation, target.TableID)
			}
		}
		columns := make(map[string]string)
		switch operation {
		case "insert":
			for fieldID, value := range target.insertColumns {
				columns[fieldID] = value
			}
		case "update":
			columns[target.ValueFieldID] = target.UpdatedMarker
		}
		mutationID := nativeCRUDUUID(p.userID, p.clientID, operation, target.TableID)
		localPayload := map[string]any{
			"authenticated_user_id": p.userID,
			"client_id":             p.clientID,
			"mutation_id":           mutationID,
			"table_id":              target.TableID,
			"pk":                    map[string]string{target.PrimaryKeyFieldID: target.RecordID},
			"authored_schema":       map[string]any{"version": p.schema.Version, "hash": p.schema.Hash},
			"operation":             operation,
			"client_version":        clientVersion,
		}
		mutation := map[string]any{
			"mutation_id":     mutationID,
			"table":           target.TableID,
			"pk":              map[string]string{target.PrimaryKeyFieldID: target.RecordID},
			"authored_schema": map[string]any{"version": p.schema.Version, "hash": p.schema.Hash},
			"op":              operation,
			"client_version":  clientVersion,
		}
		if operation != "delete" {
			localPayload["columns"] = columns
			mutation["columns"] = columns
		}
		if operation != "insert" {
			localPayload["base_version"] = baseVersion
			mutation["base_version"] = baseVersion
		}
		encoded, err := json.Marshal(localPayload)
		if err != nil {
			return NativeCRUDStep{}, errors.New("encode native CRUD local write failed")
		}
		local := Operation{ContractOperation: "local", Name: "write", Payload: encoded}
		if err := ValidateOperation(local); err != nil {
			return NativeCRUDStep{}, fmt.Errorf("native CRUD local %s for table %q is invalid: %w", operation, target.TableID, err)
		}
		localWrites = append(localWrites, local)
		mutations = append(mutations, mutation)
	}
	pushPayload, err := json.Marshal(map[string]any{
		"authenticated_user_id": p.userID,
		"request": map[string]any{
			"client_id":         p.clientID,
			"client_generation": 1,
			"batch_id":          nativeCRUDUUID(p.userID, p.clientID, operation, "batch"),
			"schema":            map[string]any{"version": p.schema.Version, "hash": p.schema.Hash},
			"mutations":         mutations,
		},
		"delivery":   "apply",
		"commit_lsn": strconv.FormatUint(commitLSN, 10),
		"end_lsn":    strconv.FormatUint(commitLSN+1, 10),
	})
	if err != nil {
		return NativeCRUDStep{}, errors.New("encode native CRUD application push failed")
	}
	push := Operation{ContractOperation: "push", Name: "submit", Payload: pushPayload}
	if err := ValidateOperation(push); err != nil {
		return NativeCRUDStep{}, fmt.Errorf("native CRUD application %s push is invalid: %w", operation, err)
	}
	materializePayload, err := json.Marshal(map[string]string{
		"stream_generation": p.streamGeneration,
		"commit_lsn":        strconv.FormatUint(commitLSN, 10),
	})
	if err != nil {
		return NativeCRUDStep{}, errors.New("encode native CRUD materialization failed")
	}
	materialize := Operation{ContractOperation: "process", Name: "materialize-source-transaction", Payload: materializePayload}
	if err := ValidateOperation(materialize); err != nil {
		return NativeCRUDStep{}, errors.New("native CRUD materialization is invalid")
	}
	return NativeCRUDStep{Operation: operation, ClientVersion: clientVersion, LocalWrites: localWrites, ApplicationPush: push, Materialize: materialize}, nil
}

// BindNativeCRUDTarget resolves one generated row through bound insert and update writes.
func BindNativeCRUDTarget(target NativeCRUDPlanTarget, boundInsert, boundUpdate Operation) (NativeCRUDTarget, error) {
	insert, err := decodeBoundNativeCRUDWrite(boundInsert, "insert", target.InitialMarker)
	if err != nil {
		return NativeCRUDTarget{}, err
	}
	update, err := decodeBoundNativeCRUDWrite(boundUpdate, "update", target.UpdatedMarker)
	if err != nil {
		return NativeCRUDTarget{}, err
	}
	if insert.tableName != update.tableName || insert.primaryKeyField != update.primaryKeyField || insert.recordID != update.recordID || insert.valueField != update.valueField {
		return NativeCRUDTarget{}, errors.New("native CRUD runtime target changed between insert and update")
	}
	return NativeCRUDTarget{
		TableID: target.TableID, TableName: insert.tableName, PrimaryKeyField: insert.primaryKeyField,
		RecordID: insert.recordID, ValueField: insert.valueField,
		InitialValue: append(json.RawMessage(nil), insert.value...), UpdatedValue: append(json.RawMessage(nil), update.value...),
	}, nil
}

type boundNativeCRUDWrite struct {
	tableName       string
	primaryKeyField string
	recordID        string
	valueField      string
	value           json.RawMessage
}

func decodeBoundNativeCRUDWrite(operation Operation, expectedOperation, marker string) (boundNativeCRUDWrite, error) {
	if OperationKey(operation) != "local/write" || ValidateOperation(operation) != nil || marker == "" {
		return boundNativeCRUDWrite{}, errors.New("bound native CRUD write is invalid")
	}
	var payload struct {
		TableID   string                     `json:"table_id"`
		PK        map[string]json.RawMessage `json:"pk"`
		Operation string                     `json:"operation"`
		Columns   map[string]json.RawMessage `json:"columns"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.TableID == "" || payload.Operation != expectedOperation || len(payload.PK) != 1 || len(payload.Columns) == 0 {
		return boundNativeCRUDWrite{}, errors.New("bound native CRUD write payload is invalid")
	}
	primaryKeyField := ""
	recordID := ""
	for field, raw := range payload.PK {
		if field == "" || json.Unmarshal(raw, &recordID) != nil || recordID == "" {
			return boundNativeCRUDWrite{}, errors.New("bound native CRUD primary key is invalid")
		}
		primaryKeyField = field
	}
	valueField := ""
	var value json.RawMessage
	for field, raw := range payload.Columns {
		if bytes.Contains(raw, []byte(marker)) {
			if valueField != "" {
				return boundNativeCRUDWrite{}, errors.New("bound native CRUD value field is ambiguous")
			}
			valueField = field
			value = append(json.RawMessage(nil), raw...)
		}
	}
	if valueField == "" || !json.Valid(value) {
		return boundNativeCRUDWrite{}, errors.New("bound native CRUD value field is absent")
	}
	return boundNativeCRUDWrite{tableName: payload.TableID, primaryKeyField: primaryKeyField, recordID: recordID, valueField: valueField, value: value}, nil
}

// NativeCRUDRestartOperation creates one direct native process restart.
func NativeCRUDRestartOperation(userID, clientID string) (Operation, error) {
	payload, err := json.Marshal(map[string]string{"user_id": userID, "client_id": clientID})
	if err != nil {
		return Operation{}, errors.New("encode native CRUD restart failed")
	}
	operation := Operation{ContractOperation: "process", Name: "restart-client", Payload: payload}
	if err := ValidateOperation(operation); err != nil {
		return Operation{}, errors.New("native CRUD restart is invalid")
	}
	return operation, nil
}

// NativeCRUDServerVersions returns one nonempty version for every expected table.
func NativeCRUDServerVersions(state NativeCRUDState) (map[string]string, error) {
	versions := make(map[string]string, len(state.Rows))
	for _, row := range state.Rows {
		if row.TableID == "" || row.ServerVersion == "" {
			return nil, errors.New("native CRUD server-version evidence is incomplete")
		}
		if _, duplicate := versions[row.TableID]; duplicate {
			return nil, errors.New("native CRUD server-version evidence is duplicated")
		}
		versions[row.TableID] = row.ServerVersion
	}
	if len(versions) == 0 {
		return nil, errors.New("native CRUD server-version evidence is absent")
	}
	return versions, nil
}

// ValidateNativeCRUDEvidence checks local SQL, canonical responses, versions, and restart durability.
func ValidateNativeCRUDEvidence(evidence NativeCRUDEvidence) error {
	if len(evidence.Targets) == 0 {
		return errors.New("native CRUD evidence has no registered tables")
	}
	tableIDs := make(map[string]struct{}, len(evidence.Targets))
	for _, target := range evidence.Targets {
		if target.TableID == "" || target.TableName == "" || target.PrimaryKeyField == "" || target.RecordID == "" || target.ValueField == "" || !json.Valid(target.InitialValue) || !json.Valid(target.UpdatedValue) || nativeCRUDJSONEqual(target.InitialValue, target.UpdatedValue) {
			return errors.New("native CRUD runtime target is incomplete")
		}
		if _, duplicate := tableIDs[target.TableID]; duplicate {
			return errors.New("native CRUD runtime target is duplicated")
		}
		tableIDs[target.TableID] = struct{}{}
	}
	states := []*NativeCRUDState{
		&evidence.Before, &evidence.AfterInsertWrite, &evidence.AfterInsertResponse, &evidence.AfterInsertRestart,
		&evidence.AfterUpdateWrite, &evidence.AfterUpdateResponse, &evidence.AfterUpdateRestart,
		&evidence.AfterDeleteWrite, &evidence.AfterDeleteResponse, &evidence.AfterDeleteRestart,
	}
	for _, state := range states {
		if err := validateNativeCRUDState(*state, evidence.Targets); err != nil {
			return err
		}
	}
	if err := validateNativeCRUDProcesses(evidence); err != nil {
		return err
	}
	count := len(evidence.Targets)
	if evidence.Before.PendingChangeCount != 0 || evidence.Before.RejectedMutationCount != 0 {
		return errors.New("native CRUD inspection client did not start with an empty queue")
	}
	for _, row := range evidence.Before.Rows {
		if row.Present || row.Mutation != nil || row.ServerVersion != "" || row.RowChecksum != "" {
			return errors.New("native CRUD generated row existed before local SQL")
		}
	}
	if err := validateNativeCRUDWrite(evidence.Before, evidence.AfterInsertWrite, evidence.Targets, "insert", count); err != nil {
		return err
	}
	if err := validateNativeCRUDResponseState(evidence.AfterInsertWrite, evidence.AfterInsertResponse, evidence.Targets, "insert", count); err != nil {
		return err
	}
	if !nativeCRUDDurableEqual(evidence.AfterInsertResponse, evidence.AfterInsertRestart) {
		return errors.New("native CRUD insert outcome changed after restart")
	}
	if err := validateNativeCRUDWrite(evidence.AfterInsertRestart, evidence.AfterUpdateWrite, evidence.Targets, "update", count); err != nil {
		return err
	}
	if err := validateNativeCRUDResponseState(evidence.AfterUpdateWrite, evidence.AfterUpdateResponse, evidence.Targets, "update", count); err != nil {
		return err
	}
	if !nativeCRUDDurableEqual(evidence.AfterUpdateResponse, evidence.AfterUpdateRestart) {
		return errors.New("native CRUD update outcome changed after restart")
	}
	if err := validateNativeCRUDWrite(evidence.AfterUpdateRestart, evidence.AfterDeleteWrite, evidence.Targets, "delete", count); err != nil {
		return err
	}
	if err := validateNativeCRUDResponseState(evidence.AfterDeleteWrite, evidence.AfterDeleteResponse, evidence.Targets, "delete", count); err != nil {
		return err
	}
	if !nativeCRUDDurableEqual(evidence.AfterDeleteResponse, evidence.AfterDeleteRestart) {
		return errors.New("native CRUD delete outcome changed after restart")
	}
	if len(evidence.Responses) != 3 {
		return errors.New("native CRUD response evidence is incomplete")
	}
	for index, operation := range []string{"insert", "update", "delete"} {
		if err := validateNativeCRUDResponse(evidence.Responses[index], operation, count); err != nil {
			return err
		}
	}
	return nil
}

func validateNativeCRUDState(state NativeCRUDState, targets []NativeCRUDTarget) error {
	if state.ProcessID == "" || state.DatabaseIdentityFingerprint == "" || state.ApplicationRowCount < 0 || state.PendingChangeCount < 0 || state.MutationLedgerCount < 0 || state.MutationOutcomeCount < 0 || state.RejectedMutationCount < 0 || state.RowMetadataCount < 0 || len(state.Rows) != len(targets) {
		return errors.New("native CRUD inspection state is incomplete")
	}
	for index, target := range targets {
		row := state.Rows[index]
		if row.TableID != target.TableID || row.Present != (len(row.Value) != 0) || len(row.Value) != 0 && !json.Valid(row.Value) {
			return errors.New("native CRUD row inspection is incomplete")
		}
		if row.Mutation != nil && (row.Mutation.Operation == "" || row.Mutation.Status == "" || row.Mutation.ClientVersion == "") {
			return errors.New("native CRUD mutation inspection is incomplete")
		}
	}
	return nil
}

func validateNativeCRUDProcesses(evidence NativeCRUDEvidence) error {
	groups := [][]NativeCRUDState{
		{evidence.Before, evidence.AfterInsertWrite, evidence.AfterInsertResponse},
		{evidence.AfterInsertRestart, evidence.AfterUpdateWrite, evidence.AfterUpdateResponse},
		{evidence.AfterUpdateRestart, evidence.AfterDeleteWrite, evidence.AfterDeleteResponse},
		{evidence.AfterDeleteRestart},
	}
	database := evidence.Before.DatabaseIdentityFingerprint
	priorProcess := ""
	for _, group := range groups {
		process := group[0].ProcessID
		if process == priorProcess {
			return errors.New("native CRUD restart did not replace the process")
		}
		for _, state := range group {
			if state.ProcessID != process || state.DatabaseIdentityFingerprint != database {
				return errors.New("native CRUD operation changed its process or database unexpectedly")
			}
		}
		priorProcess = process
	}
	return nil
}

func validateNativeCRUDWrite(before, after NativeCRUDState, targets []NativeCRUDTarget, operation string, count int) error {
	wantRows := before.ApplicationRowCount
	if operation == "insert" {
		wantRows += count
	} else if operation == "delete" {
		wantRows -= count
	}
	if after.ApplicationRowCount != wantRows || after.PendingChangeCount != before.PendingChangeCount+count || after.MutationLedgerCount != before.MutationLedgerCount+count || after.MutationOutcomeCount != before.MutationOutcomeCount || after.RejectedMutationCount != before.RejectedMutationCount || after.RowMetadataCount != before.RowMetadataCount {
		return fmt.Errorf("native CRUD local %s did not create one durable pending outcome per registered table", operation)
	}
	for index, target := range targets {
		prior := before.Rows[index]
		row := after.Rows[index]
		if row.Mutation == nil || row.Mutation.Operation != operation || row.Mutation.Status != "pending" || row.Mutation.ClientVersion == "" {
			return fmt.Errorf("native CRUD local %s is not pending for every registered table", operation)
		}
		switch operation {
		case "insert":
			if prior.Present || !row.Present || !nativeCRUDJSONEqual(row.Value, target.InitialValue) || row.ServerVersion != "" || row.RowChecksum != "" {
				return errors.New("native CRUD local insert row is invalid")
			}
		case "update":
			if !prior.Present || !row.Present || !nativeCRUDJSONEqual(row.Value, target.UpdatedValue) || row.ServerVersion != prior.ServerVersion || row.RowChecksum != prior.RowChecksum {
				return errors.New("native CRUD local update changed canonical metadata")
			}
		case "delete":
			if !prior.Present || row.Present || row.ServerVersion != prior.ServerVersion || row.RowChecksum != prior.RowChecksum {
				return errors.New("native CRUD local delete changed canonical metadata")
			}
		}
	}
	return nil
}

func validateNativeCRUDResponseState(before, after NativeCRUDState, targets []NativeCRUDTarget, operation string, count int) error {
	if after.ApplicationRowCount != before.ApplicationRowCount || after.PendingChangeCount != before.PendingChangeCount-count || after.MutationLedgerCount != before.MutationLedgerCount || after.MutationOutcomeCount != before.MutationOutcomeCount+count || after.RejectedMutationCount != before.RejectedMutationCount {
		return fmt.Errorf("native CRUD %s response did not retain one terminal outcome per registered table", operation)
	}
	wantMetadata := before.RowMetadataCount
	if operation == "insert" {
		wantMetadata += count
	}
	if after.RowMetadataCount != wantMetadata {
		return fmt.Errorf("native CRUD %s response metadata count is invalid", operation)
	}
	for index, target := range targets {
		prior := before.Rows[index]
		row := after.Rows[index]
		if row.Mutation != nil || row.Present != prior.Present || !nativeCRUDJSONEqual(row.Value, prior.Value) || row.ServerVersion == "" || row.ServerVersion == prior.ServerVersion || row.ServerVersion == prior.Mutation.ClientVersion || row.RowChecksum == "" || row.RowChecksum == prior.RowChecksum {
			return fmt.Errorf("native CRUD %s response is not canonical for every registered table", operation)
		}
		if operation == "delete" && row.Present || operation != "delete" && !row.Present || operation == "insert" && !nativeCRUDJSONEqual(row.Value, target.InitialValue) || operation == "update" && !nativeCRUDJSONEqual(row.Value, target.UpdatedValue) {
			return fmt.Errorf("native CRUD %s response row is invalid", operation)
		}
	}
	return nil
}

func validateNativeCRUDResponse(response NativeCRUDResponse, operation string, mutationCount int) error {
	if response.Operation != operation || response.Completion != "idle" || len(response.Transport) == 0 {
		return fmt.Errorf("native CRUD %s response is incomplete", operation)
	}
	pushes := 0
	for _, observation := range response.Transport {
		switch observation.OperationClass {
		case "connect", "pull", "push", "checkpoint", "schemas", "rebuild":
		default:
			return fmt.Errorf("native CRUD %s used a noncanonical application endpoint", operation)
		}
		if observation.OperationClass != "push" {
			continue
		}
		pushes++
		if observation.StatusCode != 200 || observation.ErrorCode != "" || !observation.RetryablePresent || observation.Retryable || !observation.MutationCountPresent || observation.MutationCount != mutationCount {
			return fmt.Errorf("native CRUD %s push response is invalid", operation)
		}
	}
	if pushes != 1 {
		return fmt.Errorf("native CRUD %s response has %d canonical pushes, want 1", operation, pushes)
	}
	return nil
}

func nativeCRUDDurableEqual(left, right NativeCRUDState) bool {
	left.ProcessID = ""
	right.ProcessID = ""
	return reflect.DeepEqual(left, right)
}

func nativeCRUDJSONEqual(left, right json.RawMessage) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == 0 && len(right) == 0
	}
	var compactLeft, compactRight bytes.Buffer
	if json.Compact(&compactLeft, left) != nil || json.Compact(&compactRight, right) != nil {
		return false
	}
	return bytes.Equal(compactLeft.Bytes(), compactRight.Bytes())
}

func nativeCRUDUUID(parts ...string) string {
	digest := sha256.Sum256([]byte("synchro:native-crud:v1:" + strings.Join(parts, ":")))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
