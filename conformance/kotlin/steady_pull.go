package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const steadyPullScenarioID = "SCN-PERF-STEADY-PULL-001"

var steadyPullAliasNames = []string{
	"client-generation-one",
	"current-schema",
	"scope-a",
	"scope-b",
	"baseline-rebuild",
	"scope-set-version-one",
	"items-table",
	"row-a-primary-key",
	"row-version-one",
	"row-a-checksum",
	"scope-a-checksum",
}

// SteadyPullResult records direct Kotlin Android evidence for steady-pull.
type SteadyPullResult struct {
	BaselineCall       SynchronizationResult
	MeasuredCall       SynchronizationResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type steadyPullIdentityEvidence struct {
	resolutions    []blackbox.NativeIdentityResolution
	tableName      string
	primaryKeyName string
}

// RunSteadyPullScenario executes the authored steady-pull flow through Kotlin Android.
func RunSteadyPullScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (SteadyPullResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, steadyPullScenarioID, 8)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if controller == nil || platform == nil {
		return SteadyPullResult{}, errors.New("Kotlin Android steady-pull dependencies are unavailable")
	}
	for _, id := range []string{"STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", "STEP-PERF-STEADY-PULL-001"} {
		if err := kotlinScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return SteadyPullResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Kotlin Android steady-pull contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "empty"}); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Kotlin Android steady-pull client: %w", err)
	}
	if _, err := kotlinScenarioOperation(steps, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", "rebuild/request-page"); err != nil {
		return SteadyPullResult{}, err
	}
	measuredPull, err := kotlinScenarioOperation(steps, "STEP-PERF-STEADY-PULL-001", "pull/request-page")
	if err != nil {
		return SteadyPullResult{}, err
	}
	baseline, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("run Kotlin Android steady-pull baseline: %w", err)
	}
	if !validateKotlinSteadyPullBaselineShape(baseline) {
		observed := make([]string, 0, len(baseline.transportObservations))
		for _, observation := range baseline.transportObservations {
			observed = append(observed, fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode))
		}
		// The completion alone cannot name why the client performed no
		// transport, so the client failure it recorded accompanies it.
		failure := "unavailable"
		if state, stateErr := platform.clientFor(client); stateErr == nil {
			state.mu.Lock()
			captured, captureErr := captureClientState(ctx, state)
			state.mu.Unlock()
			if captureErr == nil {
				if captured.Failure != nil {
					failure = fmt.Sprintf("%s/%s/%s", captured.Failure.Operation, captured.Failure.Code, captured.Failure.RecoveryAction)
				}
			}
		}
		return SteadyPullResult{}, fmt.Errorf("Kotlin Android steady-pull baseline produced %v, want connect, rebuild, and pull (completion %q, steps %d, failure %s)",
			observed, baseline.Completion, len(baseline.Steps), failure)
	}
	if err := validateKotlinSteadyPullBaselineWires(scenario, baseline); err != nil {
		return SteadyPullResult{}, err
	}

	commit, err := kotlinScenarioOperation(steps, "STEP-PERF-STEADY-PULL-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return SteadyPullResult{}, err
	}
	if result, err := controller.ApplyStep(ctx, commit); err != nil || result.Disposition != "success" {
		return SteadyPullResult{}, fmt.Errorf("commit Kotlin Android steady-pull source transaction: %w", kotlinResultError(err, result.Disposition))
	}
	materialize, err := kotlinScenarioOperation(steps, "STEP-PERF-STEADY-PULL-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return SteadyPullResult{}, err
	}
	if result, err := controller.ProcessStep(ctx, nil, materialize); err != nil || result.Disposition != "success" {
		return SteadyPullResult{}, fmt.Errorf("materialize Kotlin Android steady-pull source transaction: %w", kotlinResultError(err, result.Disposition))
	}
	measured, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "sync-now", Operations: []scenarios.Operation{measuredPull}})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("run Kotlin Android measured pull: %w", err)
	}
	if measured.Completion != "idle" || len(measured.Steps) != 1 || len(measured.transportObservations) != 1 || measured.transportObservations[0].StatusCode != 200 {
		return SteadyPullResult{}, errors.New("Kotlin Android measured pull did not complete successfully")
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-PERF-STEADY-PULL-001", "pull", measured); err != nil {
		return SteadyPullResult{}, err
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance", "rebuild-state"})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull client state: %w", err)
	}
	rawSnapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull identity state: %w", err)
	}
	snapshot, err := decodeWarmConnectSnapshot(rawSnapshot)
	if err != nil {
		return SteadyPullResult{}, err
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull server state: %w", kotlinResultError(err, ""))
	}
	actualClient, err := mergeKotlinCaptureFacts(clientFacts)
	if err != nil {
		return SteadyPullResult{}, err
	}
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-PERF-STEADY-PULL-SEMANTIC-001")
	if err != nil {
		return SteadyPullResult{}, err
	}
	evidence, err := resolveSteadyPullIdentities(controller, scenario.NativeIdentityAliases, baseline, measured, snapshot)
	if err != nil {
		return SteadyPullResult{}, err
	}
	applicationRow, err := captureSteadyPullApplicationRow(ctx, platform, client, evidence, snapshot)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if err := validateSteadyPullState(expected, serverCaptures[0].StateFacts, actualClient, snapshot, evidence, applicationRow); err != nil {
		return SteadyPullResult{}, err
	}
	return SteadyPullResult{BaselineCall: baseline, MeasuredCall: measured, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: evidence.resolutions}, nil
}

func resolveSteadyPullIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, baseline, measured SynchronizationResult, snapshot warmConnectSnapshot) (steadyPullIdentityEvidence, error) {
	if len(aliases) != len(steadyPullAliasNames) {
		return steadyPullIdentityEvidence{}, errors.New("Kotlin Android steady-pull identity alias set changed")
	}
	wanted := make(map[string]struct{}, len(steadyPullAliasNames))
	for _, alias := range steadyPullAliasNames {
		wanted[alias] = struct{}{}
	}
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return steadyPullIdentityEvidence{}, fmt.Errorf("Kotlin Android steady-pull identity alias %q is unexpected", alias.Alias)
		}
		if _, duplicate := seen[alias.Alias]; duplicate {
			return steadyPullIdentityEvidence{}, fmt.Errorf("Kotlin Android steady-pull identity alias %q is duplicated", alias.Alias)
		}
		seen[alias.Alias] = struct{}{}
	}
	if len(snapshot.scopeStates) != 1 || len(snapshot.scopeRows) != 1 || len(snapshot.rowMetadata) != 1 || len(snapshot.rebuildAttempts) != 0 || len(snapshot.rebuildReceiptProofs) != 1 {
		return steadyPullIdentityEvidence{}, errors.New("Kotlin Android steady-pull identity state is incomplete")
	}
	scope := snapshot.scopeStates[0]
	row := snapshot.scopeRows[0]
	metadata := snapshot.rowMetadata[0]
	scopeChecksum, scopeChecksumErr := androidChecksumDigest(scope.Checksum)
	localChecksum, localChecksumErr := androidChecksumDigest(&scope.LocalChecksum)
	rowChecksum, rowChecksumErr := androidChecksumDigest(metadata.RowChecksum)
	if scopeChecksumErr != nil || localChecksumErr != nil || rowChecksumErr != nil || scopeChecksum == nil || localChecksum == nil || rowChecksum == nil {
		return steadyPullIdentityEvidence{}, errors.New("Kotlin Android steady-pull checksum identity evidence is invalid")
	}
	if *scopeChecksum != *localChecksum || row.Checksum != *rowChecksum || row.ScopeID != scope.ScopeID || row.TableName != metadata.TableName || row.RecordID != metadata.RecordID {
		return steadyPullIdentityEvidence{}, errors.New("Kotlin Android steady-pull durable identity evidence is inconsistent")
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	identifiers := make(map[string]string)
	controllerValues, err := controller.IdentityValues(aliases)
	if err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	for _, value := range controllerValues {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
		identifiers[value.Alias] = value.ApplicationIdentifier
	}
	var runtimeScopeA, runtimeScopeB, runtimeRecord string
	var runtimeSchema schemaRef
	if json.Unmarshal(runtime["scope-a"], &runtimeScopeA) != nil || runtimeScopeA == "" || runtimeScopeA != scope.ScopeID || runtimeScopeA != row.ScopeID ||
		json.Unmarshal(runtime["scope-b"], &runtimeScopeB) != nil || runtimeScopeB == "" || runtimeScopeB == runtimeScopeA ||
		json.Unmarshal(runtime["row-a-primary-key"], &runtimeRecord) != nil || runtimeRecord == "" || runtimeRecord != row.RecordID || runtimeRecord != metadata.RecordID ||
		json.Unmarshal(runtime["current-schema"], &runtimeSchema) != nil || runtimeSchema != snapshot.schema ||
		identifiers["items-table"] == "" || identifiers["items-table"] != row.TableName || identifiers["items-table"] != metadata.TableName || identifiers["row-a-primary-key"] == "" {
		return steadyPullIdentityEvidence{}, errors.New("Kotlin Android steady-pull controller identities differ from durable state")
	}
	if len(baseline.transportObservations) < 3 || len(measured.transportObservations) != 1 || baseline.transportObservations[1].RequestFacts == nil || baseline.transportObservations[1].RequestFacts.ClientGeneration == nil || measured.transportObservations[0].RequestFacts == nil || measured.transportObservations[0].RequestFacts.ScopeSetVersion == nil {
		return steadyPullIdentityEvidence{}, errors.New("Kotlin Android steady-pull transport identity evidence is incomplete")
	}
	rebuildID, err := completedWarmConnectRebuildID(snapshot.result.Events, scope.ScopeID)
	if err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	generated := map[string]any{
		"client-generation-one": *baseline.transportObservations[1].RequestFacts.ClientGeneration,
		"baseline-rebuild":      rebuildID,
		"scope-set-version-one": *measured.transportObservations[0].RequestFacts.ScopeSetVersion,
		"row-version-one":       metadata.ServerVersion,
		"row-a-checksum":        *rowChecksum,
		"scope-a-checksum":      *scopeChecksum,
	}
	for alias, value := range generated {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return steadyPullIdentityEvidence{}, fmt.Errorf("encode Kotlin Android steady-pull alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	if err := validateSteadyPullTransportIdentities(runtime, baseline.transportObservations, measured.transportObservations, snapshot); err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	resolutions, err := resolveKotlinNativeIdentities(aliases, runtime)
	if err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	return steadyPullIdentityEvidence{resolutions: resolutions, tableName: identifiers["items-table"], primaryKeyName: identifiers["row-a-primary-key"]}, nil
}

func validateSteadyPullTransportIdentities(runtime map[string]json.RawMessage, baseline, measured []TransportObservation, snapshot warmConnectSnapshot) error {
	if len(baseline) < 3 || len(measured) != 1 || len(snapshot.scopeStates) != 1 || len(snapshot.rebuildReceiptProofs) != 1 {
		return errors.New("Kotlin Android steady-pull transport identity evidence is incomplete")
	}
	var generation, scopeSetVersion int64
	var rebuildID string
	var schema schemaRef
	if json.Unmarshal(runtime["client-generation-one"], &generation) != nil || generation <= 0 ||
		json.Unmarshal(runtime["scope-set-version-one"], &scopeSetVersion) != nil || scopeSetVersion < 0 ||
		json.Unmarshal(runtime["baseline-rebuild"], &rebuildID) != nil || rebuildID == "" ||
		json.Unmarshal(runtime["current-schema"], &schema) != nil || schema.Version <= 0 || schema.Hash == "" {
		return errors.New("Kotlin Android steady-pull resolved transport identities are invalid")
	}
	rebuilds := baseline[1 : len(baseline)-1]
	for index, observation := range rebuilds {
		facts := observation.RequestFacts
		response := observation.RebuildResponseFacts
		if observation.OperationClass != "rebuild" || facts == nil || facts.ClientGeneration == nil || *facts.ClientGeneration != generation || facts.SchemaVersion != schema.Version || facts.SchemaHash != schema.Hash || facts.RebuildIDFingerprint == nil || *facts.RebuildIDFingerprint != cursorFingerprint(rebuildID) || facts.ScopeFingerprint == nil || response == nil || response.ScopeFingerprint != *facts.ScopeFingerprint {
			return errors.New("Kotlin Android steady-pull rebuild identity is inconsistent")
		}
		terminal := index == len(rebuilds)-1
		if terminal != (!response.HasMore && !response.HasCursor && response.HasFinalScopeCursor && response.HasChecksum) {
			return errors.New("Kotlin Android steady-pull rebuild page finality is invalid")
		}
		if !terminal && (!response.HasMore || !response.HasCursor || response.HasFinalScopeCursor || response.HasChecksum) {
			return errors.New("Kotlin Android steady-pull intermediate rebuild page is invalid")
		}
	}
	proof := snapshot.rebuildReceiptProofs[0]
	if proof.RebuildIDFingerprint != cursorFingerprint(rebuildID) || proof.PageCount != len(rebuilds) || proof.ReturnedRecordCount != 0 || !proof.RequestChainValid || !proof.RecordsInCanonicalOrder || !proof.RowChecksumsValid || !proof.ScopeChecksumValid {
		return errors.New("Kotlin Android steady-pull completed rebuild evidence is invalid")
	}
	baselinePull := baseline[len(baseline)-1]
	measuredPull := measured[0]
	for _, observation := range []TransportObservation{baselinePull, measuredPull} {
		facts := observation.RequestFacts
		if observation.OperationClass != "pull" || facts == nil || facts.ClientGeneration == nil || *facts.ClientGeneration != generation || facts.SchemaVersion != schema.Version || facts.SchemaHash != schema.Hash || facts.ScopeSetVersion == nil || *facts.ScopeSetVersion != scopeSetVersion || facts.ScopeCount == nil || *facts.ScopeCount != 1 {
			return errors.New("Kotlin Android steady-pull request identity differs from durable state")
		}
	}
	if baselinePull.PullResponseFacts == nil || baselinePull.PullResponseFacts.HasMore || baselinePull.PullResponseFacts.ChangeCount != 0 || baselinePull.PullResponseFacts.RebuildScopeCount != 0 || !baselinePull.PullResponseFacts.ScopeCursorFingerprintsComplete || len(baselinePull.PullResponseFacts.ScopeCursorFingerprints) != 1 ||
		measuredPull.PullResponseFacts == nil || measuredPull.PullResponseFacts.HasMore || measuredPull.PullResponseFacts.ChangeCount != 1 || measuredPull.PullResponseFacts.RebuildScopeCount != 0 || measuredPull.PullResponseFacts.ChecksumCount != 1 || !measuredPull.PullResponseFacts.ScopeCursorFingerprintsComplete || len(measuredPull.PullResponseFacts.ScopeCursorFingerprints) != 1 ||
		measuredPull.CursorFingerprintsComplete == nil || !*measuredPull.CursorFingerprintsComplete || len(measuredPull.CursorFingerprints) != 1 || snapshot.scopeStates[0].Cursor == nil {
		return errors.New("Kotlin Android steady-pull cursor identity evidence is incomplete")
	}
	if !reflect.DeepEqual(measuredPull.CursorFingerprints, baselinePull.PullResponseFacts.ScopeCursorFingerprints) || !reflect.DeepEqual(measuredPull.PullResponseFacts.ScopeCursorFingerprints, []string{cursorFingerprint(*snapshot.scopeStates[0].Cursor)}) {
		return errors.New("Kotlin Android steady-pull cursor identity evidence is inconsistent")
	}
	return nil
}

func captureSteadyPullApplicationRow(ctx context.Context, platform *Platform, client Client, evidence steadyPullIdentityEvidence, snapshot warmConnectSnapshot) ([]map[string]json.RawMessage, error) {
	if len(snapshot.scopeRows) != 1 || evidence.tableName == "" || evidence.primaryKeyName == "" {
		return nil, errors.New("Kotlin Android steady-pull application identity is incomplete")
	}
	rawPrimary, err := json.Marshal(snapshot.scopeRows[0].RecordID)
	if err != nil {
		return nil, errors.New("Kotlin Android steady-pull application primary key is invalid")
	}
	primary, err := typedValue(rawPrimary, false)
	if err != nil {
		return nil, errors.New("Kotlin Android steady-pull application primary key is invalid")
	}
	state, err := platform.clientFor(client)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.available("steady-pull application identity"); err != nil {
		return nil, err
	}
	selectors := []RowSelector{{TableName: evidence.tableName, PrimaryKeyField: evidence.primaryKeyName, PrimaryKey: primary}}
	result, err := state.session.Execute(ctx, Request{Operation: "capture", RowSelectors: &selectors})
	if err != nil {
		return nil, fmt.Errorf("capture Kotlin Android steady-pull application row: %w", err)
	}
	var rows []map[string]json.RawMessage
	if err := decodeFactArray(result.ApplicationRows, &rows, maximumRows); err != nil || len(rows) != 1 {
		return nil, errors.New("Kotlin Android steady-pull application row is invalid")
	}
	return rows, nil
}

func validateSteadyPullState(expected, server, actualClient scenarios.StateFacts, snapshot warmConnectSnapshot, evidence steadyPullIdentityEvidence, applicationRows []map[string]json.RawMessage) error {
	serverExpected := scenarios.CloneStateFacts(expected)
	serverExpected.Clients = nil
	if err := validateKotlinStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Kotlin Android steady-pull server state differs from the authored model: %w", err)
	}
	if len(expected.Rows) != 1 || len(expected.Scopes) != 2 || len(expected.Clients) != 1 || len(actualClient.Clients) != 1 || len(snapshot.scopeRows) != 1 || len(snapshot.rowMetadata) != 1 || len(snapshot.scopeStates) != 1 || len(applicationRows) != 1 {
		return errors.New("Kotlin Android steady-pull semantic state is incomplete")
	}
	resolved, err := kotlinResolutionMap(evidence.resolutions)
	if err != nil || len(resolved) != len(steadyPullAliasNames) {
		return errors.New("Kotlin Android steady-pull identity resolution is incomplete")
	}
	wantClient := expected.Clients[0]
	gotClient := actualClient.Clients[0]
	if wantClient.UserID != gotClient.UserID || wantClient.ClientID != gotClient.ClientID || !reflect.DeepEqual(wantClient.RowCount, gotClient.RowCount) || !reflect.DeepEqual(wantClient.ProvenanceCount, gotClient.ProvenanceCount) || !reflect.DeepEqual(wantClient.CheckpointCount, gotClient.CheckpointCount) || len(wantClient.Provenance) != 1 || len(gotClient.Provenance) != 1 || len(wantClient.Checkpoints) != 1 || len(gotClient.Checkpoints) != 1 || gotClient.CurrentSchema == nil {
		return errors.New("Kotlin Android steady-pull client state shape differs from the authored model")
	}
	wantProvenance := wantClient.Provenance[0]
	gotProvenance := gotClient.Provenance[0]
	wantCheckpoint := wantClient.Checkpoints[0]
	gotCheckpoint := gotClient.Checkpoints[0]
	if len(wantProvenance.Scopes) != 1 || len(gotProvenance.Scopes) != 1 || wantCheckpoint.Checksum == nil || gotCheckpoint.Checksum == nil {
		return errors.New("Kotlin Android steady-pull client identity state is incomplete")
	}
	runtimeSchema := schemaRef{Version: int64(gotClient.CurrentSchema.Version), Hash: gotClient.CurrentSchema.Hash}
	if !kotlinResolutionAuthoredMatchesString(resolved["items-table"], wantProvenance.TableID) || gotProvenance.TableID != evidence.tableName ||
		!kotlinResolutionMatchesCanonicalString(resolved["row-a-primary-key"], wantProvenance.CanonicalWireJSON, gotProvenance.CanonicalWireJSON) ||
		!kotlinResolutionMatchesString(resolved["scope-a"], wantProvenance.Scopes[0], gotProvenance.Scopes[0]) ||
		!kotlinResolutionMatchesString(resolved["row-version-one"], wantProvenance.Version, gotProvenance.Version) ||
		!kotlinResolutionMatchesString(resolved["scope-a"], wantCheckpoint.ScopeID, gotCheckpoint.ScopeID) ||
		!kotlinResolutionMatchesString(resolved["scope-a-checksum"], *wantCheckpoint.Checksum, *gotCheckpoint.Checksum) ||
		!kotlinResolutionMatchesSchemaRuntime(resolved["current-schema"], runtimeSchema) {
		return errors.New("Kotlin Android steady-pull client identities differ from the authored model")
	}
	if wantCheckpoint.HasCursor != gotCheckpoint.HasCursor || wantCheckpoint.HasChecksum != gotCheckpoint.HasChecksum || wantCheckpoint.Verified != gotCheckpoint.Verified {
		return errors.New("Kotlin Android steady-pull checkpoint state differs from the authored model")
	}
	wantRow := expected.Rows[0]
	if !kotlinResolutionAuthoredMatchesString(resolved["items-table"], wantRow.TableID) ||
		!kotlinResolutionMatchesCanonicalString(resolved["row-a-primary-key"], wantRow.CanonicalWireJSON, gotProvenance.CanonicalWireJSON) ||
		!kotlinResolutionMatchesString(resolved["row-version-one"], wantRow.Version, snapshot.rowMetadata[0].ServerVersion) ||
		!kotlinResolutionMatchesString(resolved["row-a-checksum"], wantRow.Checksum, snapshot.scopeRows[0].Checksum) {
		return errors.New("Kotlin Android steady-pull row identities differ from the authored model")
	}
	primary, found := applicationRows[0][evidence.primaryKeyName]
	if !found || !kotlinResolutionMatchesCanonicalString(resolved["row-a-primary-key"], wantRow.CanonicalWireJSON, string(primary)) {
		return errors.New("Kotlin Android steady-pull application row differs from the resolved primary key")
	}
	for _, scope := range expected.Scopes {
		resolution, found := resolved[scope.ScopeID]
		if !found || !kotlinResolutionAuthoredMatchesString(resolution, scope.ScopeID) {
			return errors.New("Kotlin Android steady-pull scope identities differ from the authored model")
		}
	}
	return nil
}
