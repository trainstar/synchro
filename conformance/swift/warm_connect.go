package swift

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

// WarmConnectResult records the direct Swift pilot evidence.
type WarmConnectResult struct {
	BootstrapCall      SynchronizationResult
	WarmCall           SynchronizationResult
	ClientCaptures     []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type warmConnectApplicationIdentity struct {
	tableRuntimeValue   json.RawMessage
	primaryRuntimeValue json.RawMessage
	tableName           string
	primaryKeyName      string
	rows                []map[string]json.RawMessage
}

// RunWarmConnectScenario executes the binding-only warm-connect pilot.
func RunWarmConnectScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (WarmConnectResult, error) {
	steps, expected, err := validateWarmConnectScenario(scenario, client)
	if err != nil {
		return WarmConnectResult{}, err
	}
	if controller == nil || platform == nil {
		return WarmConnectResult{}, errors.New("Swift warm-connect dependencies are unavailable")
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return WarmConnectResult{}, fmt.Errorf("install Swift warm-connect contract: %w", err)
	}
	if err := platform.Install(ctx, client, "empty", ""); err != nil {
		return WarmConnectResult{}, fmt.Errorf("install Swift warm-connect client: %w", err)
	}

	assignment, err := controller.ApplyStep(ctx, steps["STEP-PERF-WARM-CONNECT-ASSIGN-001"].Operation)
	if err != nil || assignment.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("apply Swift warm-connect assignment: %w", resultError(err, assignment.Disposition))
	}

	bootstrapIDs := []scenarios.StepID{
		"STEP-PERF-WARM-CONNECT-BOOTSTRAP-CONNECT-001",
		"STEP-PERF-WARM-CONNECT-BASELINE-REBUILD-001",
		"STEP-PERF-WARM-CONNECT-BASELINE-ACK-001",
	}
	bootstrap, err := platform.Synchronize(ctx, client, "start", warmConnectOperations(steps, bootstrapIDs))
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("run Swift warm bootstrap: %w", err)
	}
	if err := validateWarmConnectCall(scenario, bootstrap, bootstrapIDs); err != nil {
		return WarmConnectResult{}, err
	}
	stopped, err := platform.Lifecycle(ctx, client, "stop")
	if err != nil || stopped.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("stop Swift warm bootstrap: %w", resultError(err, stopped.Disposition))
	}

	committed, err := controller.ApplyStep(ctx, steps["STEP-PERF-WARM-CONNECT-COMMIT-001"].Operation)
	if err != nil || committed.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("commit Swift warm-connect source row: %w", resultError(err, committed.Disposition))
	}
	materialized, err := controller.ProcessStep(ctx, nil, steps["STEP-PERF-WARM-CONNECT-MATERIALIZE-001"].Operation)
	if err != nil || materialized.Disposition != "success" {
		return WarmConnectResult{}, fmt.Errorf("materialize Swift warm-connect source row: %w", resultError(err, materialized.Disposition))
	}

	warmIDs := []scenarios.StepID{"STEP-PERF-WARM-CONNECT-001", "STEP-PERF-WARM-CONNECT-002"}
	warm, err := platform.Synchronize(ctx, client, "start", warmConnectOperations(steps, warmIDs))
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("run Swift warm start: %w", err)
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
		return WarmConnectResult{}, fmt.Errorf("capture Swift warm-connect client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return WarmConnectResult{}, fmt.Errorf("capture Swift warm-connect server state: %w", resultError(err, ""))
	}

	resolutions, err := resolveWarmConnectIdentities(controller, scenario.NativeIdentityAliases, bootstrap, warm, snapshot)
	if err != nil {
		return WarmConnectResult{}, err
	}
	if len(expected.Rows) != 1 {
		return WarmConnectResult{}, errors.New("Swift warm-connect expected row is unavailable")
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
		return WarmConnectResult{}, errors.New("Swift warm-connect application identity is incomplete")
	}
	if len(snapshot.RowMetadataRecords) != 1 || snapshot.RowMetadataRecords[0].RecordID == "" {
		return WarmConnectResult{}, errors.New("Swift warm-connect runtime record identity is unavailable")
	}
	runtimePrimaryKey, err := json.Marshal(snapshot.RowMetadataRecords[0].RecordID)
	if err != nil {
		return WarmConnectResult{}, errors.New("Swift warm-connect runtime record identity is invalid")
	}
	state, err := platform.client(client)
	if err != nil {
		return WarmConnectResult{}, err
	}
	state.mu.Lock()
	if state.terminated || state.session == nil {
		state.mu.Unlock()
		return WarmConnectResult{}, errors.New("Swift warm-connect client is unavailable for application identity inspection")
	}
	applicationResult, err := state.session.Execute(ctx, Request{
		Operation: "capture",
		RowSelectors: []runnerRowSelector{{
			TableName:       applicationIdentity.tableName,
			PrimaryKeyField: applicationIdentity.primaryKeyName,
			PrimaryKey:      runtimePrimaryKey,
		}},
	})
	state.mu.Unlock()
	if err != nil {
		return WarmConnectResult{}, fmt.Errorf("capture Swift warm-connect application identity: %w", err)
	}
	applicationIdentity.rows = applicationResult.ApplicationRows
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
		return nil, nil, errors.New("Swift warm-connect scenario contract is invalid")
	}
	if len(scenario.Steps) != len(warmConnectStepOrder) {
		return nil, nil, errors.New("Swift warm-connect step set changed")
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for index, step := range scenario.Steps {
		if step.ID != warmConnectStepOrder[index] {
			return nil, nil, errors.New("Swift warm-connect step order changed")
		}
		steps[step.ID] = step
	}
	if len(scenario.NativeLifecycleBoundaries) != 1 {
		return nil, nil, errors.New("Swift warm-connect lifecycle boundary changed")
	}
	boundary := scenario.NativeLifecycleBoundaries[0]
	if boundary.AfterStepID != "STEP-PERF-WARM-CONNECT-BASELINE-ACK-APPLY-001" || boundary.Method != "stop" || boundary.UserID != client.UserID || boundary.ClientID != client.ClientID {
		return nil, nil, errors.New("Swift warm-connect lifecycle boundary is invalid")
	}
	if len(scenario.NativeIdentityAliases) != len(warmConnectAliasNames) {
		return nil, nil, errors.New("Swift warm-connect identity alias set changed")
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		aliases[alias.Alias] = struct{}{}
	}
	for _, alias := range warmConnectAliasNames {
		if _, found := aliases[alias]; !found {
			return nil, nil, fmt.Errorf("Swift warm-connect identity alias %q is absent", alias)
		}
	}
	for _, expectation := range scenario.Model.ExpectedState {
		if expectation.ID == "EXPECT-PERF-WARM-CONNECT-SEMANTIC-001" && expectation.StateFacts != nil {
			return steps, expectation.StateFacts, nil
		}
	}
	return nil, nil, errors.New("Swift warm-connect semantic expectation is absent")
}

func warmConnectOperations(steps map[scenarios.StepID]scenarios.Step, ids []scenarios.StepID) RequestOperations {
	operations := make(RequestOperations, len(ids))
	for index, id := range ids {
		operations[index] = steps[id].Operation
	}
	return operations
}

func validateWarmConnectCall(scenario scenarios.Scenario, result SynchronizationResult, ids []scenarios.StepID) error {
	if result.Completion != "idle" || result.DurationNanoseconds == 0 || len(result.Steps) != len(ids) || len(result.transportObservations) != len(ids) {
		return errors.New("Swift warm-connect public call did not close its declared operations")
	}
	wireByStep := make(map[scenarios.StepID]scenarios.WireExpectation, len(scenario.WireExpectations))
	for _, wire := range scenario.WireExpectations {
		wireByStep[wire.StepID] = wire
	}
	for index, id := range ids {
		wire, found := wireByStep[id]
		observed := result.Steps[index]
		if !found || observed.Disposition != "success" || observed.ErrorCode != nil || observed.Wire == nil {
			return fmt.Errorf("Swift warm-connect step %s has no successful wire evidence", id)
		}
		if observed.Wire.HTTPStatus != wire.HTTPStatus || !reflect.DeepEqual(observed.Wire.ErrorCode, wire.ErrorCode) || observed.Wire.Retryable != wire.Retryable {
			return fmt.Errorf("Swift warm-connect step %s differs from its wire expectation", id)
		}
	}
	return nil
}

func (p *Platform) warmConnectSnapshot(ctx context.Context, client Client) (runnerResult, error) {
	state, err := p.client(client)
	if err != nil {
		return runnerResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.terminated || state.session == nil {
		return runnerResult{}, errors.New("Swift warm-connect client is unavailable for inspection")
	}
	return captureRunner(ctx, state)
}

func resolveWarmConnectIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, bootstrap, warm SynchronizationResult, snapshot runnerResult) ([]blackbox.NativeIdentityResolution, error) {
	if len(snapshot.ScopeStates) != 1 || len(snapshot.ScopeRows) != 1 || len(snapshot.RowMetadataRecords) != 1 || len(snapshot.RebuildAttempts) != 0 || len(snapshot.RebuildReceiptProofs) != 1 || snapshot.Schema == nil || len(bootstrap.transportObservations) != 3 || bootstrap.transportObservations[1].RequestFacts == nil || bootstrap.transportObservations[1].RequestFacts.ClientGeneration == nil || len(warm.transportObservations) != 2 || warm.transportObservations[0].RequestFacts == nil {
		return nil, errors.New("Swift warm-connect identity state is incomplete")
	}
	scope := snapshot.ScopeStates[0]
	row := snapshot.ScopeRows[0]
	metadata := snapshot.RowMetadataRecords[0]
	scopeChecksum, scopeChecksumErr := swiftChecksumDigest(scope.Checksum)
	localChecksum, localChecksumErr := swiftChecksumDigest(pointerString(scope.LocalChecksum))
	rowChecksum, rowChecksumErr := swiftChecksumDigest(metadata.RowChecksum)
	if scopeChecksumErr != nil || localChecksumErr != nil || rowChecksumErr != nil {
		return nil, errors.New("Swift warm-connect checksum identity evidence is invalid")
	}
	scopeChecksumPresent := scopeChecksum != nil
	rowChecksumPresent := rowChecksum != nil
	if !scopeChecksumPresent || localChecksum == nil || *localChecksum != *scopeChecksum || !rowChecksumPresent || row.Checksum != *rowChecksum {
		return nil, fmt.Errorf("Swift warm-connect checksum identity evidence is inconsistent: scope present = %t, scope local match = %t, row present = %t, row metadata match = %t", scopeChecksumPresent, scopeChecksumPresent && localChecksum != nil && *localChecksum == *scopeChecksum, rowChecksumPresent, rowChecksumPresent && row.Checksum == *rowChecksum)
	}
	if row.ScopeID != scope.ScopeID || metadata.TableName != row.TableName || metadata.RecordID != row.RecordID {
		return nil, errors.New("Swift warm-connect row identity evidence is inconsistent")
	}

	runtime := make(map[string]json.RawMessage, len(aliases))
	controllerValues, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, err
	}
	for _, value := range controllerValues {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}
	var rebuildID string
	rebuildMatches := 0
	for _, event := range snapshot.Events {
		if event.Type != "rebuild_completed" {
			continue
		}
		if event.ScopeID == nil || event.RebuildID == nil || *event.ScopeID == "" || *event.RebuildID == "" {
			return nil, errors.New("Swift warm-connect completed rebuild event is invalid")
		}
		if *event.ScopeID == scope.ScopeID {
			rebuildMatches++
			rebuildID = *event.RebuildID
		}
	}
	if rebuildMatches != 1 {
		return nil, errors.New("Swift warm-connect completed rebuild identity is ambiguous")
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
				return nil, fmt.Errorf("Swift warm-connect alias %q is absent", alias)
			}
			value = *pointer
		}
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return nil, fmt.Errorf("encode Swift warm-connect alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap.transportObservations, warm.transportObservations, snapshot); err != nil {
		return nil, err
	}
	for _, alias := range warmConnectAliasNames {
		if len(runtime[alias]) == 0 {
			return nil, fmt.Errorf("Swift warm-connect alias %q has no runtime evidence", alias)
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

func validateWarmConnectTransportIdentities(runtime map[string]json.RawMessage, bootstrap, warm []transportObservation, snapshot runnerResult) error {
	if len(bootstrap) != 3 || len(warm) != 2 || len(snapshot.ScopeStates) != 1 {
		return errors.New("Swift warm-connect transport identity evidence is incomplete")
	}
	if bootstrap[0].RequestFacts == nil || bootstrap[0].RequestFacts.ScopeCount == nil || *bootstrap[0].RequestFacts.ScopeCount != 0 {
		return errors.New("Swift warm-connect bootstrap scope projection is invalid")
	}
	for _, facts := range []*transportRequestFacts{bootstrap[2].RequestFacts, warm[0].RequestFacts, warm[1].RequestFacts} {
		if facts == nil || facts.ScopeCount == nil || *facts.ScopeCount != len(snapshot.ScopeStates) {
			return errors.New("Swift warm-connect request scope projection differs from durable state")
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
		return errors.New("Swift warm-connect resolved transport identities are invalid")
	}
	requests := []*transportRequestFacts{
		bootstrap[1].RequestFacts,
		bootstrap[2].RequestFacts,
		warm[0].RequestFacts,
		warm[1].RequestFacts,
	}
	for _, facts := range requests {
		if facts == nil || facts.ClientGeneration == nil || *facts.ClientGeneration != generation || facts.SchemaVersion != schema.Version || facts.SchemaHash != schema.Hash {
			return errors.New("Swift warm-connect request identity differs from durable client state")
		}
	}
	for _, facts := range []*transportRequestFacts{bootstrap[2].RequestFacts, warm[0].RequestFacts, warm[1].RequestFacts} {
		if facts.ScopeSetVersion == nil || *facts.ScopeSetVersion != scopeSetVersion {
			return errors.New("Swift warm-connect scope-set identity is inconsistent")
		}
	}
	rebuildFingerprint := cursorFingerprint(rebuildID)
	if bootstrap[1].RequestFacts.RebuildIDFingerprint == nil || *bootstrap[1].RequestFacts.RebuildIDFingerprint != rebuildFingerprint {
		return errors.New("Swift warm-connect rebuild identity is inconsistent")
	}
	if snapshot.Schema == nil || snapshot.Schema.Version != schema.Version || snapshot.Schema.Hash != schema.Hash {
		return errors.New("Swift warm-connect schema identity differs from durable state")
	}
	if len(snapshot.RebuildAttempts) != 0 || len(snapshot.RebuildReceiptProofs) != 1 {
		return errors.New("Swift warm-connect completed rebuild evidence is incomplete")
	}
	rebuild := bootstrap[1].RebuildResponseFacts
	bootstrapPull := bootstrap[2].PullResponseFacts
	warmPull := warm[1].PullResponseFacts
	if rebuild == nil || rebuild.HasMore || rebuild.HasCursor || !rebuild.HasFinalScopeCursor || !rebuild.HasChecksum || rebuild.FinalScopeCursorFingerprint == nil || bootstrapPull == nil || bootstrapPull.HasMore || !bootstrapPull.ScopeCursorFingerprintsComplete || len(bootstrapPull.ScopeCursorFingerprints) != 1 || warmPull == nil || warmPull.HasMore || !warmPull.ScopeCursorFingerprintsComplete || len(warmPull.ScopeCursorFingerprints) != 1 || snapshot.ScopeStates[0].Cursor == nil || bootstrap[2].CursorFingerprintsComplete == nil || !*bootstrap[2].CursorFingerprintsComplete || warm[1].CursorFingerprintsComplete == nil || !*warm[1].CursorFingerprintsComplete {
		return errors.New("Swift warm-connect cursor identity evidence is incomplete")
	}
	if !reflect.DeepEqual(bootstrap[2].CursorFingerprints, []string{*rebuild.FinalScopeCursorFingerprint}) {
		return errors.New("Swift warm-connect bootstrap pull is not bound to the rebuild checkpoint")
	}
	if !reflect.DeepEqual(warm[1].CursorFingerprints, bootstrapPull.ScopeCursorFingerprints) {
		return errors.New("Swift warm-connect pull is not bound to the bootstrap pull response")
	}
	if !reflect.DeepEqual(warmPull.ScopeCursorFingerprints, []string{cursorFingerprint(*snapshot.ScopeStates[0].Cursor)}) {
		return errors.New("Swift warm-connect durable cursor is not bound to the warm pull response")
	}
	proof := snapshot.RebuildReceiptProofs[0]
	if proof.RebuildIDFingerprint != rebuildFingerprint || proof.PageCount <= 0 || proof.ReturnedRecordCount != 0 || !proof.RequestChainValid || !proof.RecordsInCanonicalOrder || !proof.RowChecksumsValid || !proof.ScopeChecksumValid {
		return errors.New("Swift warm-connect completed rebuild evidence is invalid")
	}
	return nil
}

func validateWarmConnectState(expected, server scenarios.StateFacts, captures []CaptureFacts, snapshot runnerResult, applicationIdentity warmConnectApplicationIdentity, resolutions []blackbox.NativeIdentityResolution) error {
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
		return fmt.Errorf("Swift warm-connect server state differs from the authored model: transaction count = %t, row count = %t, scope count = %t, rebuild count = %t, transactions = %t, rows = %t, scopes = %t", reflect.DeepEqual(expected.TransactionCount, server.TransactionCount), reflect.DeepEqual(expected.RowCount, server.RowCount), reflect.DeepEqual(expected.ScopeCount, server.ScopeCount), reflect.DeepEqual(expected.RebuildCount, server.RebuildCount), reflect.DeepEqual(expected.Transactions, server.Transactions), reflect.DeepEqual(expected.Rows, server.Rows), reflect.DeepEqual(expectedScopes, serverScopes))
	}
	if len(expected.Clients) != 1 || len(expected.Rows) != 1 {
		return errors.New("Swift warm-connect authored client state is invalid")
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

func validateWarmConnectSnapshot(expected scenarios.ClientDurabilityFact, expectedRow scenarios.RowFact, snapshot runnerResult, applicationIdentity warmConnectApplicationIdentity, resolved map[string]blackbox.NativeIdentityResolution) error {
	if snapshot.ApplicationRowCount == nil || snapshot.MutationLedgerCount == nil || snapshot.MutationOutcomeCount == nil || snapshot.SealedBatchCount == nil || snapshot.ScopeStateCount == nil || snapshot.ScopeRowCount == nil || snapshot.ProvenanceCount == nil || snapshot.RowMetadataCount == nil || snapshot.RebuildAttemptCount == nil || snapshot.RebuildReceiptCount == nil {
		return errors.New("Swift warm-connect durable counts are incomplete")
	}
	rebuildAttemptCount, err := rebuildAttemptFactCount(snapshot)
	if err != nil {
		return err
	}
	if !equalExpectedCount(expected.RowCount, *snapshot.ApplicationRowCount) ||
		!equalExpectedCount(expected.QueueCount, *snapshot.MutationLedgerCount) ||
		!equalExpectedCount(expected.OutcomeCount, *snapshot.MutationOutcomeCount) ||
		!equalExpectedCount(expected.SealedBatchCount, *snapshot.SealedBatchCount) ||
		!equalExpectedCount(expected.CheckpointCount, *snapshot.ScopeStateCount) ||
		!equalExpectedCount(expected.ProvenanceCount, *snapshot.ProvenanceCount) ||
		!equalExpectedCount(expected.RebuildAttemptCount, int(rebuildAttemptCount)) ||
		*snapshot.ScopeRowCount != 1 || *snapshot.RowMetadataCount != 1 {
		return errors.New("Swift warm-connect durable counts differ from the authored model")
	}
	if len(expected.Provenance) != 1 || len(expected.Provenance[0].Scopes) != 1 || len(expected.Checkpoints) != 1 || expected.Checkpoints[0].Checksum == nil || expected.CurrentSchema == nil ||
		len(snapshot.ScopeStates) != 1 || snapshot.ScopeStates[0].Checksum == nil || len(snapshot.ScopeRows) != 1 || len(snapshot.RowMetadataRecords) != 1 || len(snapshot.RebuildAttempts) != 0 || len(snapshot.RebuildReceiptProofs) != 1 || snapshot.Schema == nil {
		return errors.New("Swift warm-connect durable detail is incomplete")
	}
	scopeChecksum, err := swiftChecksumDigest(snapshot.ScopeStates[0].Checksum)
	if err != nil || scopeChecksum == nil {
		return errors.New("Swift warm-connect durable scope checksum is invalid")
	}
	if !resolutionMatchesString(resolved["scope-a"], expected.Provenance[0].Scopes[0], snapshot.ScopeStates[0].ScopeID) ||
		!resolutionMatchesString(resolved["row-a-version"], expected.Provenance[0].Version, snapshot.RowMetadataRecords[0].ServerVersion) ||
		!resolutionMatchesString(resolved["row-a-checksum"], expectedRow.Checksum, snapshot.ScopeRows[0].Checksum) ||
		!resolutionMatchesString(resolved["scope-a-checksum"], *expected.Checkpoints[0].Checksum, *scopeChecksum) ||
		!resolutionMatchesSchema(resolved["current-schema"], *expected.CurrentSchema, *snapshot.Schema) ||
		!resolutionAuthoredMatchesString(resolved["items-table"], expectedRow.TableID) ||
		!resolutionAuthoredMatchesString(resolved["items-table"], expected.Provenance[0].TableID) {
		return errors.New("Swift warm-connect durable identities differ from the authored model")
	}
	if err := validateWarmConnectApplicationIdentity(expectedRow, snapshot, applicationIdentity, resolved); err != nil {
		return err
	}
	var rebuildID string
	if json.Unmarshal(resolved["baseline-rebuild"].RuntimeValue, &rebuildID) != nil || rebuildID == "" || cursorFingerprint(rebuildID) != snapshot.RebuildReceiptProofs[0].RebuildIDFingerprint {
		return errors.New("Swift warm-connect rebuild identity differs from the authored model")
	}
	return nil
}

func validateWarmConnectApplicationIdentity(expectedRow scenarios.RowFact, snapshot runnerResult, applicationIdentity warmConnectApplicationIdentity, resolved map[string]blackbox.NativeIdentityResolution) error {
	if len(applicationIdentity.rows) != 1 || len(snapshot.ScopeRows) != 1 || len(snapshot.RowMetadataRecords) != 1 {
		return errors.New("Swift warm-connect application identities differ from the resolved runtime schema")
	}
	var authoredPrimary string
	var authoredRecordID string
	observedPrimary, found := applicationIdentity.rows[0][applicationIdentity.primaryKeyName]
	runtimeRecordID, runtimeRecordIDErr := json.Marshal(snapshot.RowMetadataRecords[0].RecordID)
	if applicationIdentity.tableName != snapshot.ScopeRows[0].TableName || applicationIdentity.tableName != snapshot.RowMetadataRecords[0].TableName ||
		!bytes.Equal(applicationIdentity.tableRuntimeValue, resolved["items-table"].RuntimeValue) || !bytes.Equal(applicationIdentity.primaryRuntimeValue, resolved["items-primary-key"].RuntimeValue) ||
		json.Unmarshal(resolved["items-primary-key"].AuthoredValue, &authoredPrimary) != nil || authoredPrimary == "" ||
		json.Unmarshal([]byte(expectedRow.CanonicalWireJSON), &authoredRecordID) != nil || authoredRecordID == "" || !found || runtimeRecordIDErr != nil || !bytes.Equal(bytes.TrimSpace(observedPrimary), runtimeRecordID) {
		return errors.New("Swift warm-connect application identities differ from the resolved runtime schema")
	}
	return nil
}

func validateWarmConnectCaptures(expected scenarios.ClientDurabilityFact, captures []CaptureFacts, resolved map[string]blackbox.NativeIdentityResolution) error {
	if len(captures) != 6 || len(expected.Checkpoints) != 1 || expected.Checkpoints[0].Checksum == nil || len(expected.Provenance) != 1 || len(expected.Provenance[0].Scopes) != 1 {
		return errors.New("Swift warm-connect public capture set is incomplete")
	}
	seen := make(map[string]struct{}, len(captures))
	for _, capture := range captures {
		if _, duplicate := seen[capture.Source]; duplicate || len(capture.StateFacts.Clients) != 1 {
			return errors.New("Swift warm-connect public capture is invalid")
		}
		seen[capture.Source] = struct{}{}
		client := capture.StateFacts.Clients[0]
		if client.UserID != expected.UserID || client.ClientID != expected.ClientID || client.CurrentSchema == nil {
			return errors.New("Swift warm-connect public capture client identity is invalid")
		}
		var runtimeSchema schemaRef
		if json.Unmarshal(resolved["current-schema"].RuntimeValue, &runtimeSchema) != nil || client.CurrentSchema.Version != uint64(runtimeSchema.Version) || client.CurrentSchema.Hash != runtimeSchema.Hash {
			return errors.New("Swift warm-connect public capture schema is inconsistent")
		}
		switch capture.Source {
		case "application-rows":
			if !reflect.DeepEqual(client.RowCount, expected.RowCount) {
				return errors.New("Swift warm-connect application-row capture differs from the authored model")
			}
		case "pending-mutations":
			if !reflect.DeepEqual(client.QueueCount, expected.QueueCount) || !reflect.DeepEqual(client.SealedBatchCount, expected.SealedBatchCount) || len(client.Queue) != 0 {
				return errors.New("Swift warm-connect queue capture differs from the authored model")
			}
		case "rejected-mutations":
			if !reflect.DeepEqual(client.OutcomeCount, expected.OutcomeCount) || len(client.Outcomes) != 0 {
				return errors.New("Swift warm-connect outcome capture differs from the authored model")
			}
		case "checkpoints":
			if !reflect.DeepEqual(client.CheckpointCount, expected.CheckpointCount) || len(client.Checkpoints) != 1 || client.Checkpoints[0].Checksum == nil || !client.Checkpoints[0].Verified ||
				!resolutionMatchesString(resolved["scope-a"], expected.Checkpoints[0].ScopeID, client.Checkpoints[0].ScopeID) ||
				!resolutionMatchesString(resolved["scope-a-checksum"], *expected.Checkpoints[0].Checksum, *client.Checkpoints[0].Checksum) {
				return errors.New("Swift warm-connect checkpoint capture differs from the authored model")
			}
		case "provenance":
			if !reflect.DeepEqual(client.ProvenanceCount, expected.ProvenanceCount) || len(client.Provenance) != 1 || client.Provenance[0].TableID == "" || client.Provenance[0].CanonicalWireJSON == "" ||
				!resolutionMatchesString(resolved["scope-a"], expected.Provenance[0].Scopes[0], client.Provenance[0].Scopes[0]) ||
				!resolutionMatchesString(resolved["row-a-version"], expected.Provenance[0].Version, client.Provenance[0].Version) {
				return errors.New("Swift warm-connect provenance capture differs from the authored model")
			}
		case "rebuild-state":
			if !reflect.DeepEqual(client.RebuildAttemptCount, expected.RebuildAttemptCount) {
				return errors.New("Swift warm-connect rebuild capture differs from the authored model")
			}
		default:
			return errors.New("Swift warm-connect public capture source is unknown")
		}
	}
	return nil
}

func resolutionMatchesString(resolution blackbox.NativeIdentityResolution, authored, runtime string) bool {
	var resolvedAuthored, resolvedRuntime string
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored == authored && resolvedRuntime == runtime
}

func resolutionAuthoredMatchesString(resolution blackbox.NativeIdentityResolution, authored string) bool {
	var resolved string
	return json.Unmarshal(resolution.AuthoredValue, &resolved) == nil && resolved == authored
}

func resolutionMatchesSchema(resolution blackbox.NativeIdentityResolution, authored scenarios.SchemaFact, runtime schemaRef) bool {
	var resolvedAuthored, resolvedRuntime schemaRef
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		resolvedAuthored.Version == int64(authored.Version) && resolvedAuthored.Hash == authored.Hash &&
		resolvedRuntime == runtime
}

func equalExpectedCount(expected *uint64, observed int) bool {
	return expected != nil && observed >= 0 && *expected == uint64(observed)
}

func resultError(err error, disposition string) error {
	if err != nil {
		return err
	}
	if disposition == "" {
		return errors.New("result is absent")
	}
	return fmt.Errorf("terminal disposition is %q", disposition)
}
