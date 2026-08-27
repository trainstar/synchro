package kotlin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/trainstar/synchro/conformance/blackbox"
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

// WarmConnectResult records the direct Kotlin Android pilot evidence.
type WarmConnectResult struct {
	BootstrapCall      SynchronizationResult
	WarmCall           SynchronizationResult
	ClientCaptures     []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type warmConnectSnapshot struct {
	result               Result
	schema               schemaRef
	scopeStates          []scopeStateRecord
	scopeRows            []scopeRowRecord
	rowMetadata          []rowMetadataRecord
	rebuildAttempts      []rebuildAttemptRecord
	rebuildReceiptProofs []rebuildReceiptProofRecord
}

type warmConnectApplicationIdentity struct {
	tableRuntimeValue   json.RawMessage
	primaryRuntimeValue json.RawMessage
	tableName           string
	primaryKeyName      string
	rows                []map[string]json.RawMessage
}

type rowMetadataRecord struct {
	TableName     string  `json:"table_name"`
	RecordID      string  `json:"record_id"`
	ServerVersion string  `json:"server_version"`
	RowChecksum   *string `json:"row_checksum"`
}

// RunWarmConnectScenario executes the binding-only Kotlin Android pilot.
func RunWarmConnectScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (WarmConnectResult, error) {
	steps, expected, err := validateWarmConnectScenario(scenario, client)
	if err != nil {
		return WarmConnectResult{}, err
	}
	if controller == nil || platform == nil {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect dependencies are unavailable")
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return WarmConnectResult{}, fmt.Errorf("install Kotlin Android warm-connect contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "empty"}); err != nil {
		return WarmConnectResult{}, fmt.Errorf("install Kotlin Android warm-connect client: %w", err)
	}

	assignment, err := controller.ApplyStep(ctx, steps["STEP-PERF-WARM-CONNECT-ASSIGN-001"].Operation)
	if err != nil || assignment.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("apply Kotlin Android warm-connect assignment: %w", warmConnectResultError(err, assignment.Disposition))
	}

	bootstrapIDs := []scenarios.StepID{
		"STEP-PERF-WARM-CONNECT-BOOTSTRAP-CONNECT-001",
		"STEP-PERF-WARM-CONNECT-BASELINE-REBUILD-001",
		"STEP-PERF-WARM-CONNECT-BASELINE-ACK-001",
	}
	bootstrap, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "start", Operations: warmConnectOperations(steps, bootstrapIDs)})
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("run Kotlin Android warm bootstrap: %w", err)
	}
	if err := validateWarmConnectCall(scenario, bootstrap, bootstrapIDs); err != nil {
		return WarmConnectResult{}, err
	}
	stopped, err := platform.Lifecycle(ctx, LifecycleRequest{Client: client, Operation: "stop"})
	if err != nil || stopped.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("stop Kotlin Android warm bootstrap: %w", warmConnectResultError(err, stopped.Disposition))
	}

	committed, err := controller.ApplyStep(ctx, steps["STEP-PERF-WARM-CONNECT-COMMIT-001"].Operation)
	if err != nil || committed.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("commit Kotlin Android warm-connect source row: %w", warmConnectResultError(err, committed.Disposition))
	}
	materialized, err := controller.ProcessStep(ctx, nil, steps["STEP-PERF-WARM-CONNECT-MATERIALIZE-001"].Operation)
	if err != nil || materialized.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("materialize Kotlin Android warm-connect source row: %w", warmConnectResultError(err, materialized.Disposition))
	}

	warmIDs := []scenarios.StepID{"STEP-PERF-WARM-CONNECT-001", "STEP-PERF-WARM-CONNECT-002"}
	warm, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "start", Operations: warmConnectOperations(steps, warmIDs)})
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("run Kotlin Android warm start: %w", err)
	}
	if err := validateWarmConnectCall(scenario, warm, warmIDs); err != nil {
		return WarmConnectResult{}, err
	}

	snapshot, err := platform.warmConnectSnapshot(ctx, client)
	if err != nil {
		return WarmConnectResult{}, err
	}
	clientCaptures, err := platform.Capture(ctx, []Client{client}, []string{
		"application-rows",
		"pending-mutations",
		"rejected-mutations",
		"checkpoints",
		"provenance",
		"rebuild-state",
	})
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("capture Kotlin Android warm-connect client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return WarmConnectResult{}, fmt.Errorf("capture Kotlin Android warm-connect server state: %w", warmConnectResultError(err, ""))
	}

	resolutions, err := resolveWarmConnectIdentities(controller, scenario.NativeIdentityAliases, bootstrap, warm, snapshot)
	if err != nil {
		return WarmConnectResult{}, err
	}
	if len(expected.Rows) != 1 {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect expected row is unavailable")
	}
	applicationValues, err := controller.IdentityValues(scenario.NativeIdentityAliases)
	if err != nil {
		return WarmConnectResult{}, err
	}
	applicationIdentity := warmConnectApplicationIdentity{}
	for _, value := range applicationValues {
		switch value.Alias {
		case "items-table":
			applicationIdentity.tableRuntimeValue = append(json.RawMessage(nil), value.RuntimeValue...)
			applicationIdentity.tableName = value.ApplicationIdentifier
		case "items-primary-key":
			applicationIdentity.primaryRuntimeValue = append(json.RawMessage(nil), value.RuntimeValue...)
			applicationIdentity.primaryKeyName = value.ApplicationIdentifier
		}
	}
	if applicationIdentity.tableName == "" || applicationIdentity.primaryKeyName == "" {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect application identity is incomplete")
	}
	if len(snapshot.rowMetadata) != 1 || snapshot.rowMetadata[0].RecordID == "" {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect runtime record identity is unavailable")
	}
	runtimePrimaryKey, err := json.Marshal(snapshot.rowMetadata[0].RecordID)
	if err != nil {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect runtime record identity is invalid")
	}
	primaryKey, err := typedValue(runtimePrimaryKey, false)
	if err != nil {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect expected primary key is invalid")
	}
	state, err := platform.clientFor(client)
	if err != nil {
		return WarmConnectResult{}, err
	}
	state.mu.Lock()
	if err := state.available("warm-connect application identity"); err != nil {
		state.mu.Unlock()
		return WarmConnectResult{}, err
	}
	selectors := []RowSelector{{TableName: applicationIdentity.tableName, PrimaryKeyField: applicationIdentity.primaryKeyName, PrimaryKey: primaryKey}}
	applicationResult, err := state.session.Execute(ctx, Request{Operation: "capture", RowSelectors: &selectors})
	state.mu.Unlock()
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("capture Kotlin Android warm-connect application identity: %w", err)
	}
	if decodeFactArray(applicationResult.ApplicationRows, &applicationIdentity.rows, maximumRows) != nil {
		return WarmConnectResult{}, errors.New("Kotlin Android warm-connect application identity rows are invalid")
	}
	if err := validateWarmConnectState(*expected, serverCaptures[0].StateFacts, clientCaptures, snapshot, applicationIdentity, resolutions); err != nil {
		return WarmConnectResult{}, err
	}
	return WarmConnectResult{
		BootstrapCall:      bootstrap,
		WarmCall:           warm,
		ClientCaptures:     clientCaptures,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: resolutions,
	}, nil
}

func validateWarmConnectScenario(scenario scenarios.Scenario, client Client) (map[scenarios.StepID]scenarios.Step, *scenarios.StateFacts, error) {
	if string(scenario.ID) != warmConnectScenarioID || len(scenario.Model.Setup) != 1 || scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return nil, nil, errors.New("Kotlin Android warm-connect scenario contract is invalid")
	}
	if len(scenario.Steps) != len(warmConnectStepOrder) {
		return nil, nil, errors.New("Kotlin Android warm-connect step set changed")
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for index, step := range scenario.Steps {
		if step.ID != warmConnectStepOrder[index] {
			return nil, nil, errors.New("Kotlin Android warm-connect step order changed")
		}
		steps[step.ID] = step
	}
	if len(scenario.NativeLifecycleBoundaries) != 1 {
		return nil, nil, errors.New("Kotlin Android warm-connect lifecycle boundary changed")
	}
	boundary := scenario.NativeLifecycleBoundaries[0]
	if boundary.AfterStepID != "STEP-PERF-WARM-CONNECT-BASELINE-ACK-APPLY-001" || boundary.Method != "stop" || boundary.UserID != client.UserID || boundary.ClientID != client.ClientID {
		return nil, nil, errors.New("Kotlin Android warm-connect lifecycle boundary is invalid")
	}
	if len(scenario.NativeIdentityAliases) != len(warmConnectAliasNames) {
		return nil, nil, errors.New("Kotlin Android warm-connect identity alias set changed")
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		aliases[alias.Alias] = struct{}{}
	}
	for _, alias := range warmConnectAliasNames {
		if _, found := aliases[alias]; !found {
			return nil, nil, fmt.Errorf("Kotlin Android warm-connect identity alias %q is absent", alias)
		}
	}
	for _, expectation := range scenario.Model.ExpectedState {
		if expectation.ID == "EXPECT-PERF-WARM-CONNECT-SEMANTIC-001" && expectation.StateFacts != nil {
			return steps, expectation.StateFacts, nil
		}
	}
	return nil, nil, errors.New("Kotlin Android warm-connect semantic expectation is absent")
}

func warmConnectOperations(steps map[scenarios.StepID]scenarios.Step, ids []scenarios.StepID) []scenarios.Operation {
	operations := make([]scenarios.Operation, len(ids))
	for index, id := range ids {
		operations[index] = steps[id].Operation
	}
	return operations
}

func validateWarmConnectCall(scenario scenarios.Scenario, result SynchronizationResult, ids []scenarios.StepID) error {
	if result.Completion != "idle" || result.DurationNanoseconds == 0 || len(result.Steps) != len(ids) || len(result.transportObservations) != len(ids) {
		return errors.New("Kotlin Android warm-connect public call did not close its declared operations")
	}
	wireByStep := make(map[scenarios.StepID]scenarios.WireExpectation, len(scenario.WireExpectations))
	for _, wire := range scenario.WireExpectations {
		wireByStep[wire.StepID] = wire
	}
	for index, id := range ids {
		wire, found := wireByStep[id]
		observed := result.Steps[index]
		if !found || observed.Disposition != "success" || observed.ErrorCode != nil || observed.Wire == nil {
			return fmt.Errorf("Kotlin Android warm-connect step %s has no successful wire evidence", id)
		}
		if observed.Wire.HTTPStatus != wire.HTTPStatus || !reflect.DeepEqual(observed.Wire.ErrorCode, wire.ErrorCode) || observed.Wire.Retryable != wire.Retryable {
			return fmt.Errorf("Kotlin Android warm-connect step %s differs from its wire expectation", id)
		}
	}
	return nil
}

func (p *Platform) warmConnectSnapshot(ctx context.Context, client Client) (warmConnectSnapshot, error) {
	state, err := p.clientFor(client)
	if err != nil {
		return warmConnectSnapshot{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("warm-connect inspection"); err != nil {
		return warmConnectSnapshot{}, err
	}
	result, err := captureClientState(ctx, state)
	if err != nil {
		return warmConnectSnapshot{}, err
	}
	return decodeWarmConnectSnapshot(result)
}

func decodeWarmConnectSnapshot(result Result) (warmConnectSnapshot, error) {
	var schema schemaRef
	if err := decodeStrictFact(result.Schema, &schema); err != nil || schema.Version <= 0 || !validLowerHexDigest(schema.Hash) {
		return warmConnectSnapshot{}, errors.New("Kotlin Android warm-connect schema inspection is invalid")
	}
	scopeStates, err := androidCursorScopeStates(result.ScopeStates)
	if err != nil {
		return warmConnectSnapshot{}, err
	}
	scopeRows, err := androidScopeRows(result.ScopeRows)
	if err != nil {
		return warmConnectSnapshot{}, err
	}
	var metadata []rowMetadataRecord
	if err := decodeFactArray(result.RowMetadata, &metadata, maximumRecords); err != nil {
		return warmConnectSnapshot{}, errors.New("Kotlin Android warm-connect row metadata inspection is invalid")
	}
	seenMetadata := make(map[string]struct{}, len(metadata))
	for _, value := range metadata {
		key := value.TableName + "\x00" + value.RecordID
		if value.TableName == "" || value.RecordID == "" || value.ServerVersion == "" || value.RowChecksum == nil {
			return warmConnectSnapshot{}, errors.New("Kotlin Android warm-connect row metadata inspection is invalid")
		}
		if _, duplicate := seenMetadata[key]; duplicate {
			return warmConnectSnapshot{}, errors.New("Kotlin Android warm-connect row metadata inspection is duplicated")
		}
		seenMetadata[key] = struct{}{}
	}
	attempts, err := androidRebuildAttempts(result.RebuildAttempts)
	if err != nil {
		return warmConnectSnapshot{}, err
	}
	proofs, err := androidRebuildReceiptProofs(result.RebuildReceiptProofs)
	if err != nil {
		return warmConnectSnapshot{}, err
	}
	return warmConnectSnapshot{
		result:               result,
		schema:               schema,
		scopeStates:          scopeStates,
		scopeRows:            scopeRows,
		rowMetadata:          metadata,
		rebuildAttempts:      attempts,
		rebuildReceiptProofs: proofs,
	}, nil
}

func resolveWarmConnectIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, bootstrap, warm SynchronizationResult, snapshot warmConnectSnapshot) ([]blackbox.NativeIdentityResolution, error) {
	if len(snapshot.scopeStates) != 1 || len(snapshot.scopeRows) != 1 || len(snapshot.rowMetadata) != 1 || len(snapshot.rebuildAttempts) != 0 || len(snapshot.rebuildReceiptProofs) != 1 || len(bootstrap.transportObservations) != 3 || bootstrap.transportObservations[1].RequestFacts == nil || bootstrap.transportObservations[1].RequestFacts.ClientGeneration == nil || len(warm.transportObservations) != 2 || warm.transportObservations[0].RequestFacts == nil {
		return nil, errors.New("Kotlin Android warm-connect identity state is incomplete")
	}
	scope := snapshot.scopeStates[0]
	row := snapshot.scopeRows[0]
	metadata := snapshot.rowMetadata[0]
	scopeChecksum, scopeChecksumErr := androidChecksumDigest(scope.Checksum)
	localChecksum, localChecksumErr := androidChecksumDigest(&scope.LocalChecksum)
	rowChecksum, rowChecksumErr := androidChecksumDigest(metadata.RowChecksum)
	if scopeChecksumErr != nil || localChecksumErr != nil || rowChecksumErr != nil {
		return nil, errors.New("Kotlin Android warm-connect checksum identity evidence is invalid")
	}
	if scopeChecksum == nil || localChecksum == nil || *localChecksum != *scopeChecksum || rowChecksum == nil || row.Checksum != *rowChecksum {
		return nil, errors.New("Kotlin Android warm-connect checksum identity evidence is inconsistent")
	}
	if row.ScopeID != scope.ScopeID || metadata.TableName != row.TableName || metadata.RecordID != row.RecordID {
		return nil, errors.New("Kotlin Android warm-connect row identity evidence is inconsistent")
	}

	runtime := make(map[string]json.RawMessage, len(aliases))
	controllerValues, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, err
	}
	for _, value := range controllerValues {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}
	rebuildID, err := completedWarmConnectRebuildID(snapshot.result.Events, scope.ScopeID)
	if err != nil {
		return nil, err
	}
	generated := map[string]any{
		"row-a-checksum":        *rowChecksum,
		"scope-a-checksum":      *scopeChecksum,
		"client-a-generation":   *bootstrap.transportObservations[1].RequestFacts.ClientGeneration,
		"baseline-rebuild":      rebuildID,
		"row-a-version":         metadata.ServerVersion,
		"scope-set-version-one": warm.transportObservations[0].RequestFacts.ScopeSetVersion,
	}
	for alias, value := range generated {
		if pointer, ok := value.(*int64); ok {
			if pointer == nil {
				return nil, fmt.Errorf("Kotlin Android warm-connect alias %q is absent", alias)
			}
			value = *pointer
		}
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return nil, fmt.Errorf("encode Kotlin Android warm-connect alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap.transportObservations, warm.transportObservations, snapshot); err != nil {
		return nil, err
	}
	for _, alias := range warmConnectAliasNames {
		if len(runtime[alias]) == 0 {
			return nil, fmt.Errorf("Kotlin Android warm-connect alias %q has no runtime evidence", alias)
		}
	}

	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range aliases {
		value := runtime[alias.Alias]
		for _, id := range alias.StepIDs {
			stepID := id
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &stepID, RuntimeValue: append(json.RawMessage(nil), value...)})
		}
		for _, id := range alias.ExpectationIDs {
			expectationID := id
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &expectationID, RuntimeValue: append(json.RawMessage(nil), value...)})
		}
	}
	return blackbox.ResolveNativeIdentityAliases(aliases, observations)
}

func completedWarmConnectRebuildID(raw json.RawMessage, scopeID string) (string, error) {
	var events []map[string]json.RawMessage
	if err := decodeFactArray(raw, &events, maximumRecords); err != nil {
		return "", errors.New("Kotlin Android warm-connect event evidence is invalid")
	}
	matches := 0
	var rebuildID string
	for _, event := range events {
		var eventType string
		if json.Unmarshal(event["type"], &eventType) != nil || eventType == "" {
			return "", errors.New("Kotlin Android warm-connect event evidence is invalid")
		}
		if eventType != "rebuild_completed" {
			continue
		}
		var eventScope, eventRebuild string
		if len(event) != 3 || json.Unmarshal(event["scope_id"], &eventScope) != nil || json.Unmarshal(event["rebuild_id"], &eventRebuild) != nil || eventScope == "" || eventRebuild == "" {
			return "", errors.New("Kotlin Android warm-connect completed rebuild event is invalid")
		}
		if eventScope == scopeID {
			matches++
			rebuildID = eventRebuild
		}
	}
	if matches != 1 {
		return "", errors.New("Kotlin Android warm-connect completed rebuild identity is ambiguous")
	}
	return rebuildID, nil
}

func validateWarmConnectTransportIdentities(runtime map[string]json.RawMessage, bootstrap, warm []TransportObservation, snapshot warmConnectSnapshot) error {
	if len(bootstrap) != 3 || len(warm) != 2 || len(snapshot.scopeStates) != 1 {
		return errors.New("Kotlin Android warm-connect transport identity evidence is incomplete")
	}
	if bootstrap[0].RequestFacts == nil || bootstrap[0].RequestFacts.ScopeCount == nil || *bootstrap[0].RequestFacts.ScopeCount != 0 {
		return errors.New("Kotlin Android warm-connect bootstrap scope projection is invalid")
	}
	for _, facts := range []*TransportRequestFacts{bootstrap[2].RequestFacts, warm[0].RequestFacts, warm[1].RequestFacts} {
		if facts == nil || facts.ScopeCount == nil || *facts.ScopeCount != len(snapshot.scopeStates) {
			return errors.New("Kotlin Android warm-connect request scope projection differs from durable state")
		}
	}
	var generation int64
	var scopeSetVersion int64
	var rebuildID string
	var schema schemaRef
	if json.Unmarshal(runtime["client-a-generation"], &generation) != nil || generation <= 0 ||
		json.Unmarshal(runtime["scope-set-version-one"], &scopeSetVersion) != nil || scopeSetVersion < 0 ||
		json.Unmarshal(runtime["baseline-rebuild"], &rebuildID) != nil || rebuildID == "" ||
		json.Unmarshal(runtime["current-schema"], &schema) != nil || schema.Version <= 0 || schema.Hash == "" {
		return errors.New("Kotlin Android warm-connect resolved transport identities are invalid")
	}
	requests := []*TransportRequestFacts{
		bootstrap[1].RequestFacts,
		bootstrap[2].RequestFacts,
		warm[0].RequestFacts,
		warm[1].RequestFacts,
	}
	for _, facts := range requests {
		if facts == nil || facts.ClientGeneration == nil || *facts.ClientGeneration != generation || facts.SchemaVersion != schema.Version || facts.SchemaHash != schema.Hash {
			return errors.New("Kotlin Android warm-connect request identity differs from durable client state")
		}
	}
	for _, facts := range []*TransportRequestFacts{bootstrap[2].RequestFacts, warm[0].RequestFacts, warm[1].RequestFacts} {
		if facts.ScopeSetVersion == nil || *facts.ScopeSetVersion != scopeSetVersion {
			return errors.New("Kotlin Android warm-connect scope-set identity is inconsistent")
		}
	}
	rebuildFingerprint := cursorFingerprint(rebuildID)
	if bootstrap[1].RequestFacts.RebuildIDFingerprint == nil || *bootstrap[1].RequestFacts.RebuildIDFingerprint != rebuildFingerprint {
		return errors.New("Kotlin Android warm-connect rebuild identity is inconsistent")
	}
	if snapshot.schema != schema {
		return errors.New("Kotlin Android warm-connect schema identity differs from durable state")
	}
	if len(snapshot.rebuildAttempts) != 0 || len(snapshot.rebuildReceiptProofs) != 1 {
		return errors.New("Kotlin Android warm-connect completed rebuild evidence is incomplete")
	}
	rebuild := bootstrap[1].RebuildResponseFacts
	bootstrapPull := bootstrap[2].PullResponseFacts
	warmPull := warm[1].PullResponseFacts
	if rebuild == nil || rebuild.HasMore || rebuild.HasCursor || !rebuild.HasFinalScopeCursor || !rebuild.HasChecksum || rebuild.FinalScopeCursorFingerprint == nil || bootstrapPull == nil || bootstrapPull.HasMore || !bootstrapPull.ScopeCursorFingerprintsComplete || len(bootstrapPull.ScopeCursorFingerprints) != 1 || warmPull == nil || warmPull.HasMore || !warmPull.ScopeCursorFingerprintsComplete || len(warmPull.ScopeCursorFingerprints) != 1 || snapshot.scopeStates[0].Cursor == nil || bootstrap[2].CursorFingerprintsComplete == nil || !*bootstrap[2].CursorFingerprintsComplete || warm[1].CursorFingerprintsComplete == nil || !*warm[1].CursorFingerprintsComplete {
		return errors.New("Kotlin Android warm-connect cursor identity evidence is incomplete")
	}
	if !reflect.DeepEqual(bootstrap[2].CursorFingerprints, []string{*rebuild.FinalScopeCursorFingerprint}) {
		return errors.New("Kotlin Android warm-connect bootstrap pull is not bound to the rebuild checkpoint")
	}
	if !reflect.DeepEqual(warm[1].CursorFingerprints, bootstrapPull.ScopeCursorFingerprints) {
		return errors.New("Kotlin Android warm-connect pull is not bound to the bootstrap pull response")
	}
	if !reflect.DeepEqual(warmPull.ScopeCursorFingerprints, []string{cursorFingerprint(*snapshot.scopeStates[0].Cursor)}) {
		return errors.New("Kotlin Android warm-connect durable cursor is not bound to the warm pull response")
	}
	proof := snapshot.rebuildReceiptProofs[0]
	if proof.RebuildIDFingerprint != rebuildFingerprint || proof.PageCount <= 0 || proof.ReturnedRecordCount != 0 || !proof.RequestChainValid || !proof.RecordsInCanonicalOrder || !proof.RowChecksumsValid || !proof.ScopeChecksumValid {
		return errors.New("Kotlin Android warm-connect completed rebuild evidence is invalid")
	}
	return nil
}

func validateWarmConnectState(expected, server scenarios.StateFacts, captures []CaptureFacts, snapshot warmConnectSnapshot, applicationIdentity warmConnectApplicationIdentity, resolutions []blackbox.NativeIdentityResolution) error {
	expectedScopes := append([]scenarios.ScopeFact(nil), expected.Scopes...)
	serverScopes := append([]scenarios.ScopeFact(nil), server.Scopes...)
	sort.Slice(expectedScopes, func(left, right int) bool { return expectedScopes[left].ScopeID < expectedScopes[right].ScopeID })
	sort.Slice(serverScopes, func(left, right int) bool { return serverScopes[left].ScopeID < serverScopes[right].ScopeID })
	if !reflect.DeepEqual(expected.TransactionCount, server.TransactionCount) ||
		!reflect.DeepEqual(expected.RowCount, server.RowCount) ||
		!reflect.DeepEqual(expected.ScopeCount, server.ScopeCount) ||
		!reflect.DeepEqual(expected.RebuildCount, server.RebuildCount) ||
		!reflect.DeepEqual(expected.Transactions, server.Transactions) ||
		!reflect.DeepEqual(expected.Rows, server.Rows) ||
		!reflect.DeepEqual(expectedScopes, serverScopes) {
		return errors.New("Kotlin Android warm-connect server state differs from the authored model")
	}
	if len(expected.Clients) != 1 || len(expected.Rows) != 1 {
		return errors.New("Kotlin Android warm-connect authored client state is invalid")
	}
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(resolutions))
	for _, resolution := range resolutions {
		resolved[resolution.Alias] = resolution
	}
	if err := validateWarmConnectSnapshot(expected.Clients[0], expected.Rows[0], snapshot, applicationIdentity, resolved); err != nil {
		return err
	}
	return validateWarmConnectCaptures(expected.Clients[0], captures, resolved)
}

func validateWarmConnectSnapshot(expected scenarios.ClientDurabilityFact, expectedRow scenarios.RowFact, snapshot warmConnectSnapshot, applicationIdentity warmConnectApplicationIdentity, resolved map[string]blackbox.NativeIdentityResolution) error {
	result := snapshot.result
	if result.ApplicationRowCount == nil || result.MutationLedgerCount == nil || result.MutationOutcomeCount == nil || result.SealedBatchCount == nil || result.ScopeStateCount == nil || result.ScopeRowCount == nil || result.ProvenanceCount == nil || result.RowMetadataCount == nil || result.RebuildAttemptCount == nil || result.RebuildReceiptCount == nil {
		return errors.New("Kotlin Android warm-connect durable counts are incomplete")
	}
	rebuildAttemptCount, err := androidRebuildAttemptFactCount(result)
	if err != nil {
		return err
	}
	if !equalWarmConnectCount(expected.RowCount, *result.ApplicationRowCount) ||
		!equalWarmConnectCount(expected.QueueCount, *result.MutationLedgerCount) ||
		!equalWarmConnectCount(expected.OutcomeCount, *result.MutationOutcomeCount) ||
		!equalWarmConnectCount(expected.SealedBatchCount, *result.SealedBatchCount) ||
		!equalWarmConnectCount(expected.CheckpointCount, *result.ScopeStateCount) ||
		!equalWarmConnectCount(expected.ProvenanceCount, *result.ProvenanceCount) ||
		expected.RebuildAttemptCount == nil || *expected.RebuildAttemptCount != rebuildAttemptCount ||
		*result.ScopeRowCount != 1 || *result.RowMetadataCount != 1 {
		return errors.New("Kotlin Android warm-connect durable counts differ from the authored model")
	}
	if len(expected.Provenance) != 1 || len(expected.Provenance[0].Scopes) != 1 || len(expected.Checkpoints) != 1 || expected.Checkpoints[0].Checksum == nil || expected.CurrentSchema == nil || len(snapshot.scopeStates) != 1 || snapshot.scopeStates[0].Checksum == nil || len(snapshot.scopeRows) != 1 || len(snapshot.rowMetadata) != 1 || len(snapshot.rebuildAttempts) != 0 || len(snapshot.rebuildReceiptProofs) != 1 {
		return errors.New("Kotlin Android warm-connect durable detail is incomplete")
	}
	scopeChecksum, err := androidChecksumDigest(snapshot.scopeStates[0].Checksum)
	if err != nil || scopeChecksum == nil {
		return errors.New("Kotlin Android warm-connect durable scope checksum is invalid")
	}
	if !warmConnectResolutionMatchesString(resolved["scope-a"], expected.Provenance[0].Scopes[0], snapshot.scopeStates[0].ScopeID) ||
		!warmConnectResolutionMatchesString(resolved["row-a-version"], expected.Provenance[0].Version, snapshot.rowMetadata[0].ServerVersion) ||
		!warmConnectResolutionMatchesString(resolved["row-a-checksum"], expectedRow.Checksum, snapshot.scopeRows[0].Checksum) ||
		!warmConnectResolutionMatchesString(resolved["scope-a-checksum"], *expected.Checkpoints[0].Checksum, *scopeChecksum) ||
		!warmConnectResolutionMatchesSchema(resolved["current-schema"], *expected.CurrentSchema, snapshot.schema) ||
		!warmConnectResolutionAuthoredMatchesString(resolved["items-table"], expectedRow.TableID) ||
		!warmConnectResolutionAuthoredMatchesString(resolved["items-table"], expected.Provenance[0].TableID) {
		return errors.New("Kotlin Android warm-connect durable identities differ from the authored model")
	}
	if err := validateWarmConnectApplicationIdentity(expectedRow, snapshot, applicationIdentity, resolved); err != nil {
		return err
	}
	var rebuildID string
	if json.Unmarshal(resolved["baseline-rebuild"].RuntimeValue, &rebuildID) != nil || rebuildID == "" || cursorFingerprint(rebuildID) != snapshot.rebuildReceiptProofs[0].RebuildIDFingerprint {
		return errors.New("Kotlin Android warm-connect rebuild identity differs from the authored model")
	}
	return nil
}

func validateWarmConnectApplicationIdentity(expectedRow scenarios.RowFact, snapshot warmConnectSnapshot, applicationIdentity warmConnectApplicationIdentity, resolved map[string]blackbox.NativeIdentityResolution) error {
	if len(applicationIdentity.rows) != 1 {
		return errors.New("Kotlin Android warm-connect runtime primary-key lookup did not return one row")
	}
	if len(snapshot.scopeRows) != 1 || len(snapshot.rowMetadata) != 1 {
		return errors.New("Kotlin Android warm-connect durable application identity is incomplete")
	}
	var authoredPrimary string
	var authoredRecordID string
	observedPrimary, found := applicationIdentity.rows[0][applicationIdentity.primaryKeyName]
	runtimeRecordID, runtimeRecordIDErr := json.Marshal(snapshot.rowMetadata[0].RecordID)
	expectedPrimary, expectedPrimaryErr := typedValue(runtimeRecordID, false)
	actualPrimary, actualPrimaryErr := typedValue(observedPrimary, false)
	if applicationIdentity.tableName != snapshot.scopeRows[0].TableName || applicationIdentity.tableName != snapshot.rowMetadata[0].TableName {
		return errors.New("Kotlin Android warm-connect application table differs from durable row state")
	}
	if !bytes.Equal(applicationIdentity.tableRuntimeValue, resolved["items-table"].RuntimeValue) || !bytes.Equal(applicationIdentity.primaryRuntimeValue, resolved["items-primary-key"].RuntimeValue) {
		return errors.New("Kotlin Android warm-connect application identity differs from its runtime alias")
	}
	if json.Unmarshal(resolved["items-primary-key"].AuthoredValue, &authoredPrimary) != nil || authoredPrimary == "" {
		return errors.New("Kotlin Android warm-connect authored primary-key identity is invalid")
	}
	if json.Unmarshal([]byte(expectedRow.CanonicalWireJSON), &authoredRecordID) != nil || authoredRecordID == "" {
		return errors.New("Kotlin Android warm-connect authored record identity is invalid")
	}
	if !found || runtimeRecordIDErr != nil || expectedPrimaryErr != nil || actualPrimaryErr != nil || !typedValuesEqual(expectedPrimary, actualPrimary) {
		return errors.New("Kotlin Android warm-connect application row does not use the runtime primary key")
	}
	return nil
}

func validateWarmConnectCaptures(expected scenarios.ClientDurabilityFact, captures []CaptureFacts, resolved map[string]blackbox.NativeIdentityResolution) error {
	if len(captures) != 6 || len(expected.Checkpoints) != 1 || expected.Checkpoints[0].Checksum == nil || len(expected.Provenance) != 1 || len(expected.Provenance[0].Scopes) != 1 {
		return errors.New("Kotlin Android warm-connect public capture set is incomplete")
	}
	seen := make(map[string]struct{}, len(captures))
	for _, capture := range captures {
		if _, duplicate := seen[capture.Source]; duplicate || len(capture.StateFacts.Clients) != 1 {
			return errors.New("Kotlin Android warm-connect public capture is invalid")
		}
		seen[capture.Source] = struct{}{}
		client := capture.StateFacts.Clients[0]
		if client.UserID != expected.UserID || client.ClientID != expected.ClientID || client.CurrentSchema == nil {
			return errors.New("Kotlin Android warm-connect public capture client identity is invalid")
		}
		var runtimeSchema schemaRef
		if json.Unmarshal(resolved["current-schema"].RuntimeValue, &runtimeSchema) != nil || client.CurrentSchema.Version != uint64(runtimeSchema.Version) || client.CurrentSchema.Hash != runtimeSchema.Hash {
			return errors.New("Kotlin Android warm-connect public capture schema is inconsistent")
		}
		switch capture.Source {
		case "application-rows":
			if !reflect.DeepEqual(client.RowCount, expected.RowCount) {
				return errors.New("Kotlin Android warm-connect application-row capture differs from the authored model")
			}
		case "pending-mutations":
			if !reflect.DeepEqual(client.QueueCount, expected.QueueCount) || !reflect.DeepEqual(client.SealedBatchCount, expected.SealedBatchCount) || len(client.Queue) != 0 {
				return errors.New("Kotlin Android warm-connect queue capture differs from the authored model")
			}
		case "rejected-mutations":
			if !reflect.DeepEqual(client.OutcomeCount, expected.OutcomeCount) || len(client.Outcomes) != 0 {
				return errors.New("Kotlin Android warm-connect outcome capture differs from the authored model")
			}
		case "checkpoints":
			if !reflect.DeepEqual(client.CheckpointCount, expected.CheckpointCount) || len(client.Checkpoints) != 1 || client.Checkpoints[0].Checksum == nil || !client.Checkpoints[0].Verified ||
				!warmConnectResolutionMatchesString(resolved["scope-a"], expected.Checkpoints[0].ScopeID, client.Checkpoints[0].ScopeID) ||
				!warmConnectResolutionMatchesString(resolved["scope-a-checksum"], *expected.Checkpoints[0].Checksum, *client.Checkpoints[0].Checksum) {
				return errors.New("Kotlin Android warm-connect checkpoint capture differs from the authored model")
			}
		case "provenance":
			if !reflect.DeepEqual(client.ProvenanceCount, expected.ProvenanceCount) || len(client.Provenance) != 1 || client.Provenance[0].TableID == "" || client.Provenance[0].CanonicalWireJSON == "" ||
				!warmConnectResolutionMatchesString(resolved["scope-a"], expected.Provenance[0].Scopes[0], client.Provenance[0].Scopes[0]) ||
				!warmConnectResolutionMatchesString(resolved["row-a-version"], expected.Provenance[0].Version, client.Provenance[0].Version) {
				return errors.New("Kotlin Android warm-connect provenance capture differs from the authored model")
			}
		case "rebuild-state":
			if !reflect.DeepEqual(client.RebuildAttemptCount, expected.RebuildAttemptCount) {
				return errors.New("Kotlin Android warm-connect rebuild capture differs from the authored model")
			}
		default:
			return errors.New("Kotlin Android warm-connect public capture source is unknown")
		}
	}
	return nil
}

func warmConnectResolutionMatchesString(resolution blackbox.NativeIdentityResolution, authored, runtime string) bool {
	var resolvedAuthored, resolvedRuntime string
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored == authored && resolvedRuntime == runtime
}

func warmConnectResolutionAuthoredMatchesString(resolution blackbox.NativeIdentityResolution, authored string) bool {
	var resolved string
	return json.Unmarshal(resolution.AuthoredValue, &resolved) == nil && resolved == authored
}

func warmConnectResolutionMatchesSchema(resolution blackbox.NativeIdentityResolution, authored scenarios.SchemaFact, runtime schemaRef) bool {
	var resolvedAuthored, resolvedRuntime schemaRef
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored.Version == int64(authored.Version) && resolvedAuthored.Hash == authored.Hash &&
		resolvedRuntime == runtime
}

func equalWarmConnectCount(expected *uint64, observed int) bool {
	return expected != nil && observed >= 0 && *expected == uint64(observed)
}

func warmConnectResultError(err error, disposition string) error {
	if err != nil {
		return err
	}
	if disposition == "" {
		return errors.New("result is absent")
	}
	return fmt.Errorf("terminal disposition is %q", disposition)
}
