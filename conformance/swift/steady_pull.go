package swift

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

// SteadyPullResult records direct Swift evidence for the steady-pull scenario.
type SteadyPullResult struct {
	BaselineCall       SynchronizationResult
	MeasuredCall       SynchronizationResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type steadyPullIdentityEvidence struct {
	resolutions []blackbox.NativeIdentityResolution
	tableName   string
}

// RunSteadyPullScenario executes the authored steady-pull flow through Swift.
func RunSteadyPullScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (SteadyPullResult, error) {
	steps, err := swiftScenarioStepMap(scenario, steadyPullScenarioID, 8)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if controller == nil || platform == nil {
		return SteadyPullResult{}, errors.New("Swift steady-pull dependencies are unavailable")
	}
	for _, id := range []string{
		"STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001",
		"STEP-PERF-STEADY-PULL-001",
	} {
		if err := swiftScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return SteadyPullResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Swift steady-pull contract: %w", err)
	}
	if err := platform.Install(ctx, client, "empty", ""); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Swift steady-pull client: %w", err)
	}

	if _, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", "rebuild/request-page"); err != nil {
		return SteadyPullResult{}, err
	}
	measuredPull, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-001", "pull/request-page")
	if err != nil {
		return SteadyPullResult{}, err
	}
	baseline, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("run Swift steady-pull baseline: %w", err)
	}
	if !validateSwiftSteadyPullBaselineShape(baseline) {
		observed := make([]string, 0, len(baseline.transportObservations))
		for _, observation := range baseline.transportObservations {
			observed = append(observed, fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode))
		}
		return SteadyPullResult{}, fmt.Errorf("Swift steady-pull baseline produced %v, want connect, rebuild, and pull", observed)
	}
	if err := validateSwiftSteadyPullBaselineWires(scenario, baseline); err != nil {
		return SteadyPullResult{}, err
	}

	commit, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return SteadyPullResult{}, err
	}
	if _, err := controller.ApplyStep(ctx, commit); err != nil {
		return SteadyPullResult{}, fmt.Errorf("commit Swift steady-pull source transaction: %w", err)
	}
	materialize, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return SteadyPullResult{}, err
	}
	if _, err := controller.ProcessStep(ctx, nil, materialize); err != nil {
		return SteadyPullResult{}, fmt.Errorf("materialize Swift steady-pull source transaction: %w", err)
	}

	measured, err := platform.Synchronize(ctx, client, "sync-now", RequestOperations{measuredPull})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("run Swift measured pull: %w", err)
	}
	if measured.Completion != "idle" || len(measured.Steps) != 1 || len(measured.transportObservations) != 1 || measured.transportObservations[0].StatusCode != 200 {
		return SteadyPullResult{}, errors.New("Swift measured pull did not complete successfully")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-STEADY-PULL-001", "pull", measured); err != nil {
		return SteadyPullResult{}, err
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{
		"application-rows",
		"pending-mutations",
		"rejected-mutations",
		"checkpoints",
		"provenance",
		"rebuild-state",
	})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull client state: %w", err)
	}
	snapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull identity state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull server state: %w", err)
	}
	actualClient, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return SteadyPullResult{}, err
	}
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-PERF-STEADY-PULL-SEMANTIC-001")
	if err != nil {
		return SteadyPullResult{}, err
	}
	identityEvidence, err := resolveSteadyPullIdentities(controller, scenario.NativeIdentityAliases, baseline, measured, snapshot)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if err := validateSteadyPullState(expected, serverCaptures[0].StateFacts, actualClient, snapshot, identityEvidence); err != nil {
		return SteadyPullResult{}, err
	}
	return SteadyPullResult{
		BaselineCall:       baseline,
		MeasuredCall:       measured,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: identityEvidence.resolutions,
	}, nil
}

func resolveSteadyPullIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, baseline, measured SynchronizationResult, snapshot runnerResult) (steadyPullIdentityEvidence, error) {
	if len(aliases) != len(steadyPullAliasNames) {
		return steadyPullIdentityEvidence{}, errors.New("Swift steady-pull identity alias set changed")
	}
	wantedAliases := make(map[string]struct{}, len(steadyPullAliasNames))
	for _, alias := range steadyPullAliasNames {
		wantedAliases[alias] = struct{}{}
	}
	seenAliases := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if _, wanted := wantedAliases[alias.Alias]; !wanted {
			return steadyPullIdentityEvidence{}, fmt.Errorf("Swift steady-pull identity alias %q is unexpected", alias.Alias)
		}
		if _, duplicate := seenAliases[alias.Alias]; duplicate {
			return steadyPullIdentityEvidence{}, fmt.Errorf("Swift steady-pull identity alias %q is duplicated", alias.Alias)
		}
		seenAliases[alias.Alias] = struct{}{}
	}

	if len(snapshot.ScopeStates) != 1 || len(snapshot.ScopeRows) != 1 || len(snapshot.RowMetadataRecords) != 1 || len(snapshot.RebuildAttempts) != 0 || len(snapshot.RebuildReceipts) != 1 || snapshot.Schema == nil {
		return steadyPullIdentityEvidence{}, errors.New("Swift steady-pull identity state is incomplete")
	}
	scope := snapshot.ScopeStates[0]
	row := snapshot.ScopeRows[0]
	metadata := snapshot.RowMetadataRecords[0]
	scopeChecksum, scopeChecksumErr := swiftChecksumDigest(scope.Checksum)
	localChecksum, localChecksumErr := swiftChecksumDigest(pointerString(scope.LocalChecksum))
	rowChecksum, rowChecksumErr := swiftChecksumDigest(metadata.RowChecksum)
	if scopeChecksumErr != nil || localChecksumErr != nil || rowChecksumErr != nil || scopeChecksum == nil || localChecksum == nil || rowChecksum == nil {
		return steadyPullIdentityEvidence{}, errors.New("Swift steady-pull checksum identity evidence is invalid")
	}
	if *scopeChecksum != *localChecksum || row.Checksum != *rowChecksum || row.ScopeID != scope.ScopeID || row.TableName != metadata.TableName || row.RecordID != metadata.RecordID {
		return steadyPullIdentityEvidence{}, errors.New("Swift steady-pull durable identity evidence is inconsistent")
	}

	runtime := make(map[string]json.RawMessage, len(aliases))
	applicationIdentifiers := make(map[string]string)
	controllerValues, err := controller.IdentityValues(aliases)
	if err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	for _, value := range controllerValues {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
		applicationIdentifiers[value.Alias] = value.ApplicationIdentifier
	}
	var runtimeScopeA, runtimeScopeB, runtimeRecord string
	var runtimeSchema schemaRef
	if json.Unmarshal(runtime["scope-a"], &runtimeScopeA) != nil || runtimeScopeA == "" || runtimeScopeA != scope.ScopeID || runtimeScopeA != row.ScopeID ||
		json.Unmarshal(runtime["scope-b"], &runtimeScopeB) != nil || runtimeScopeB == "" || runtimeScopeB == runtimeScopeA ||
		json.Unmarshal(runtime["row-a-primary-key"], &runtimeRecord) != nil || runtimeRecord == "" || runtimeRecord != row.RecordID || runtimeRecord != metadata.RecordID ||
		json.Unmarshal(runtime["current-schema"], &runtimeSchema) != nil || runtimeSchema != *snapshot.Schema ||
		applicationIdentifiers["items-table"] == "" || applicationIdentifiers["items-table"] != row.TableName || applicationIdentifiers["items-table"] != metadata.TableName {
		return steadyPullIdentityEvidence{}, errors.New("Swift steady-pull controller identities differ from durable state")
	}

	rebuildID, err := completedSwiftRebuildID(snapshot.Events, scope.ScopeID)
	if err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	if len(baseline.transportObservations) < 3 || len(measured.transportObservations) != 1 || baseline.transportObservations[1].RequestFacts == nil || baseline.transportObservations[1].RequestFacts.ClientGeneration == nil || measured.transportObservations[0].RequestFacts == nil || measured.transportObservations[0].RequestFacts.ScopeSetVersion == nil {
		return steadyPullIdentityEvidence{}, errors.New("Swift steady-pull transport identity evidence is incomplete")
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
			return steadyPullIdentityEvidence{}, fmt.Errorf("encode Swift steady-pull alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	if err := validateSteadyPullTransportIdentities(runtime, baseline.transportObservations, measured.transportObservations, snapshot); err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	for _, alias := range steadyPullAliasNames {
		if len(runtime[alias]) == 0 {
			return steadyPullIdentityEvidence{}, fmt.Errorf("Swift steady-pull alias %q has no runtime evidence", alias)
		}
	}
	resolutions, err := resolveSwiftNativeIdentities(aliases, runtime)
	if err != nil {
		return steadyPullIdentityEvidence{}, err
	}
	return steadyPullIdentityEvidence{resolutions: resolutions, tableName: applicationIdentifiers["items-table"]}, nil
}

func validateSteadyPullTransportIdentities(runtime map[string]json.RawMessage, baseline, measured []transportObservation, snapshot runnerResult) error {
	if len(baseline) < 3 || len(measured) != 1 || len(snapshot.ScopeStates) != 1 || len(snapshot.RebuildReceipts) != 1 {
		return errors.New("Swift steady-pull transport identity evidence is incomplete")
	}
	var generation, scopeSetVersion int64
	var rebuildID string
	var schema schemaRef
	if json.Unmarshal(runtime["client-generation-one"], &generation) != nil || generation <= 0 ||
		json.Unmarshal(runtime["scope-set-version-one"], &scopeSetVersion) != nil || scopeSetVersion < 0 ||
		json.Unmarshal(runtime["baseline-rebuild"], &rebuildID) != nil || rebuildID == "" ||
		json.Unmarshal(runtime["current-schema"], &schema) != nil || schema.Version <= 0 || schema.Hash == "" {
		return errors.New("Swift steady-pull resolved transport identities are invalid")
	}
	rebuilds := baseline[1 : len(baseline)-1]
	for index, observation := range rebuilds {
		facts := observation.RequestFacts
		response := observation.RebuildResponseFacts
		if observation.OperationClass != "rebuild" || facts == nil || facts.ClientGeneration == nil || *facts.ClientGeneration != generation || facts.SchemaVersion != schema.Version || facts.SchemaHash != schema.Hash || facts.RebuildIDFingerprint == nil || *facts.RebuildIDFingerprint != cursorFingerprint(rebuildID) || facts.ScopeFingerprint == nil || response == nil || response.ScopeFingerprint != *facts.ScopeFingerprint {
			return errors.New("Swift steady-pull rebuild identity is inconsistent")
		}
		terminal := index == len(rebuilds)-1
		if terminal != (!response.HasMore && !response.HasCursor && response.HasFinalScopeCursor && response.HasChecksum) {
			return errors.New("Swift steady-pull rebuild page finality is invalid")
		}
		if !terminal && (!response.HasMore || !response.HasCursor || response.HasFinalScopeCursor || response.HasChecksum) {
			return errors.New("Swift steady-pull intermediate rebuild page is invalid")
		}
	}
	if !validateCompletedEmptyRebuildReceipt(snapshot.RebuildReceipts[0], rebuildID, len(rebuilds)) {
		return errors.New("Swift steady-pull completed rebuild evidence is invalid")
	}
	baselinePull := baseline[len(baseline)-1]
	measuredPull := measured[0]
	for _, observation := range []transportObservation{baselinePull, measuredPull} {
		facts := observation.RequestFacts
		if observation.OperationClass != "pull" || facts == nil || facts.ClientGeneration == nil || *facts.ClientGeneration != generation || facts.SchemaVersion != schema.Version || facts.SchemaHash != schema.Hash || facts.ScopeSetVersion == nil || *facts.ScopeSetVersion != scopeSetVersion || facts.ScopeCount == nil || *facts.ScopeCount != 1 {
			return errors.New("Swift steady-pull request identity differs from durable state")
		}
	}
	if baselinePull.PullResponseFacts == nil || baselinePull.PullResponseFacts.HasMore || baselinePull.PullResponseFacts.ChangeCount != 0 || baselinePull.PullResponseFacts.RebuildScopeCount != 0 || !baselinePull.PullResponseFacts.ScopeCursorFingerprintsComplete || len(baselinePull.PullResponseFacts.ScopeCursorFingerprints) != 1 ||
		measuredPull.PullResponseFacts == nil || measuredPull.PullResponseFacts.HasMore || measuredPull.PullResponseFacts.ChangeCount != 1 || measuredPull.PullResponseFacts.RebuildScopeCount != 0 || measuredPull.PullResponseFacts.ChecksumCount != 1 || !measuredPull.PullResponseFacts.ScopeCursorFingerprintsComplete || len(measuredPull.PullResponseFacts.ScopeCursorFingerprints) != 1 ||
		measuredPull.CursorFingerprintsComplete == nil || !*measuredPull.CursorFingerprintsComplete || len(measuredPull.CursorFingerprints) != 1 || snapshot.ScopeStates[0].Cursor == nil {
		return errors.New("Swift steady-pull cursor identity evidence is incomplete")
	}
	if !reflect.DeepEqual(measuredPull.CursorFingerprints, baselinePull.PullResponseFacts.ScopeCursorFingerprints) ||
		!reflect.DeepEqual(measuredPull.PullResponseFacts.ScopeCursorFingerprints, []string{cursorFingerprint(*snapshot.ScopeStates[0].Cursor)}) {
		return errors.New("Swift steady-pull cursor identity evidence is inconsistent")
	}
	return nil
}

func validateSteadyPullState(expected, server, actualClient scenarios.StateFacts, snapshot runnerResult, evidence steadyPullIdentityEvidence) error {
	serverExpected := scenarios.CloneStateFacts(expected)
	serverExpected.Clients = nil
	if err := validateSwiftStateProjection(serverExpected, server); err != nil {
		return fmt.Errorf("Swift steady-pull server state differs from the authored model: %w", err)
	}
	if len(expected.Rows) != 1 || len(expected.Scopes) != 2 || len(expected.Clients) != 1 || len(actualClient.Clients) != 1 || len(snapshot.ScopeRows) != 1 || len(snapshot.RowMetadataRecords) != 1 || len(snapshot.ScopeStates) != 1 {
		return errors.New("Swift steady-pull semantic state is incomplete")
	}
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(evidence.resolutions))
	for _, resolution := range evidence.resolutions {
		if _, duplicate := resolved[resolution.Alias]; duplicate {
			return errors.New("Swift steady-pull identity resolution is duplicated")
		}
		resolved[resolution.Alias] = resolution
	}
	if len(resolved) != len(steadyPullAliasNames) {
		return errors.New("Swift steady-pull identity resolution is incomplete")
	}
	wantClient := expected.Clients[0]
	gotClient := actualClient.Clients[0]
	if wantClient.UserID != gotClient.UserID || wantClient.ClientID != gotClient.ClientID ||
		!reflect.DeepEqual(wantClient.RowCount, gotClient.RowCount) ||
		!reflect.DeepEqual(wantClient.ProvenanceCount, gotClient.ProvenanceCount) ||
		!reflect.DeepEqual(wantClient.CheckpointCount, gotClient.CheckpointCount) ||
		len(wantClient.Provenance) != 1 || len(gotClient.Provenance) != 1 || len(wantClient.Checkpoints) != 1 || len(gotClient.Checkpoints) != 1 || gotClient.CurrentSchema == nil {
		return errors.New("Swift steady-pull client state shape differs from the authored model")
	}
	wantProvenance := wantClient.Provenance[0]
	gotProvenance := gotClient.Provenance[0]
	wantCheckpoint := wantClient.Checkpoints[0]
	gotCheckpoint := gotClient.Checkpoints[0]
	if len(wantProvenance.Scopes) != 1 || len(gotProvenance.Scopes) != 1 || wantCheckpoint.Checksum == nil || gotCheckpoint.Checksum == nil {
		return errors.New("Swift steady-pull client identity state is incomplete")
	}
	runtimeSchema := schemaRef{Version: int64(gotClient.CurrentSchema.Version), Hash: gotClient.CurrentSchema.Hash}
	if !resolutionAuthoredMatchesString(resolved["items-table"], wantProvenance.TableID) || gotProvenance.TableID != evidence.tableName ||
		!resolutionMatchesCanonicalString(resolved["row-a-primary-key"], wantProvenance.CanonicalWireJSON, gotProvenance.CanonicalWireJSON) ||
		!resolutionMatchesString(resolved["scope-a"], wantProvenance.Scopes[0], gotProvenance.Scopes[0]) ||
		!resolutionMatchesString(resolved["row-version-one"], wantProvenance.Version, gotProvenance.Version) ||
		!resolutionMatchesString(resolved["scope-a"], wantCheckpoint.ScopeID, gotCheckpoint.ScopeID) ||
		!resolutionMatchesString(resolved["scope-a-checksum"], *wantCheckpoint.Checksum, *gotCheckpoint.Checksum) ||
		!resolutionMatchesSchemaRuntime(resolved["current-schema"], runtimeSchema) {
		return errors.New("Swift steady-pull client identities differ from the authored model")
	}
	if wantCheckpoint.HasCursor != gotCheckpoint.HasCursor || wantCheckpoint.HasChecksum != gotCheckpoint.HasChecksum || wantCheckpoint.Verified != gotCheckpoint.Verified {
		return errors.New("Swift steady-pull checkpoint state differs from the authored model")
	}
	wantRow := expected.Rows[0]
	if !resolutionAuthoredMatchesString(resolved["items-table"], wantRow.TableID) ||
		!resolutionMatchesCanonicalString(resolved["row-a-primary-key"], wantRow.CanonicalWireJSON, gotProvenance.CanonicalWireJSON) ||
		!resolutionMatchesString(resolved["row-version-one"], wantRow.Version, snapshot.RowMetadataRecords[0].ServerVersion) ||
		!resolutionMatchesString(resolved["row-a-checksum"], wantRow.Checksum, snapshot.ScopeRows[0].Checksum) {
		return errors.New("Swift steady-pull row identities differ from the authored model")
	}
	for _, scope := range expected.Scopes {
		resolution, found := resolved[scope.ScopeID]
		if !found || !resolutionAuthoredMatchesString(resolution, scope.ScopeID) {
			return errors.New("Swift steady-pull scope identities differ from the authored model")
		}
	}
	return nil
}

func resolutionMatchesCanonicalString(resolution blackbox.NativeIdentityResolution, authoredCanonical, runtimeCanonical string) bool {
	var resolvedAuthored, resolvedRuntime, authored, runtime string
	return json.Unmarshal(resolution.AuthoredValue, &resolvedAuthored) == nil &&
		json.Unmarshal(resolution.RuntimeValue, &resolvedRuntime) == nil &&
		json.Unmarshal([]byte(authoredCanonical), &authored) == nil &&
		json.Unmarshal([]byte(runtimeCanonical), &runtime) == nil &&
		resolvedAuthored == authored && resolvedRuntime == runtime
}

func resolutionMatchesSchemaRuntime(resolution blackbox.NativeIdentityResolution, runtime schemaRef) bool {
	var resolved schemaRef
	return json.Unmarshal(resolution.RuntimeValue, &resolved) == nil && resolved == runtime
}

func mergeSwiftCaptureFacts(values []CaptureFacts) (scenarios.StateFacts, error) {
	parts := make([]scenarios.StateFacts, 0, len(values))
	for _, value := range values {
		parts = append(parts, value.StateFacts)
	}
	return mergeSwiftStateFacts(parts...)
}
