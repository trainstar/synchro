package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const scopeEmptyPullScenarioID = "SCN-SCOPE-EMPTY-PULL-001"

var scopeEmptyPullAliasNames = []string{
	"current-schema",
	"rows-table",
	"identity-scope",
	"granted-scope",
	"shared-row-primary-key",
	"granted-rebuild",
}

// ScopeEmptyPullResult records direct Swift evidence for one empty scope set pull.
type ScopeEmptyPullResult struct {
	Start              SynchronizationResult
	Sync               SynchronizationResult
	IdentityResolution []blackbox.NativeIdentityResolution
}

type scopeEmptyPullOperations struct {
	revoke, connect, firstPull, grant, commit, materialize, syncPull, rebuild scenarios.Operation
}

// scopeEmptyPullEvidence holds the runtime identities that the final state must match.
type scopeEmptyPullEvidence struct {
	grantedScope string
	rebuildID    string
	tableName    string
	primaryField string
	recordID     string
}

// RunScopeEmptyPullScenario executes the authored empty scope set pull flow through Swift.
func RunScopeEmptyPullScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (ScopeEmptyPullResult, error) {
	if controller == nil || platform == nil {
		return ScopeEmptyPullResult{}, errors.New("Swift scope-empty-pull dependencies are unavailable")
	}
	operations, err := loadScopeEmptyPullOperations(scenario, client)
	if err != nil {
		return ScopeEmptyPullResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return ScopeEmptyPullResult{}, fmt.Errorf("install Swift scope-empty-pull contract: %w", err)
	}
	if observed, applyErr := controller.ApplyStep(ctx, operations.revoke); applyErr != nil || observed.Disposition != "success" {
		return ScopeEmptyPullResult{}, fmt.Errorf("revoke Swift scope-empty-pull identity scope: %w", resultError(applyErr, observed.Disposition))
	}
	if err := platform.Install(ctx, client, "empty", ""); err != nil {
		return ScopeEmptyPullResult{}, fmt.Errorf("install Swift scope-empty-pull client: %w", err)
	}

	start, err := platform.Synchronize(ctx, client, "start", RequestOperations{operations.connect, operations.firstPull})
	if err != nil {
		return ScopeEmptyPullResult{}, fmt.Errorf("run Swift scope-empty-pull start: %w", err)
	}
	startVersion, err := validateScopeEmptyPullStart(scenario, operations, start)
	if err != nil {
		return ScopeEmptyPullResult{}, err
	}
	afterStart, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return ScopeEmptyPullResult{}, fmt.Errorf("capture Swift scope-empty-pull start state: %w", err)
	}
	if err := validateScopeEmptyPullStartState(afterStart); err != nil {
		return ScopeEmptyPullResult{}, err
	}

	if observed, applyErr := controller.ApplyStep(ctx, operations.grant); applyErr != nil || observed.Disposition != "success" {
		return ScopeEmptyPullResult{}, fmt.Errorf("grant Swift scope-empty-pull shared scope: %w", resultError(applyErr, observed.Disposition))
	}
	if observed, applyErr := controller.ApplyStep(ctx, operations.commit); applyErr != nil || observed.Disposition != "success" {
		return ScopeEmptyPullResult{}, fmt.Errorf("commit Swift scope-empty-pull row: %w", resultError(applyErr, observed.Disposition))
	}
	if observed, processErr := controller.ProcessStep(ctx, nil, operations.materialize); processErr != nil || observed.Disposition != "success" {
		return ScopeEmptyPullResult{}, fmt.Errorf("materialize Swift scope-empty-pull row: %w", resultError(processErr, observed.Disposition))
	}

	synced, err := platform.Synchronize(ctx, client, "sync-now", RequestOperations{operations.syncPull, operations.rebuild})
	if err != nil {
		return ScopeEmptyPullResult{}, fmt.Errorf("run Swift scope-empty-pull sync cycle: %w", err)
	}
	final, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return ScopeEmptyPullResult{}, fmt.Errorf("capture Swift scope-empty-pull final state: %w", err)
	}
	runtime, evidence, err := resolveScopeEmptyPullIdentities(controller, scenario.NativeIdentityAliases, final)
	if err != nil {
		return ScopeEmptyPullResult{}, err
	}
	if err := validateScopeEmptyPullSync(scenario, operations, synced, startVersion, evidence); err != nil {
		return ScopeEmptyPullResult{}, err
	}
	if err := validateScopeEmptyPullFinalState(final, synced, evidence); err != nil {
		return ScopeEmptyPullResult{}, err
	}
	resolutions, err := resolveSwiftNativeIdentities(scenario.NativeIdentityAliases, runtime)
	if err != nil {
		return ScopeEmptyPullResult{}, err
	}
	return ScopeEmptyPullResult{Start: start, Sync: synced, IdentityResolution: resolutions}, nil
}

func loadScopeEmptyPullOperations(scenario scenarios.Scenario, client Client) (scopeEmptyPullOperations, error) {
	steps, err := swiftScenarioStepMap(scenario, scopeEmptyPullScenarioID, 8)
	if err != nil {
		return scopeEmptyPullOperations{}, err
	}
	var operations scopeEmptyPullOperations
	for _, wanted := range []struct {
		id, key, callID, method string
		target                  *scenarios.Operation
	}{
		{"STEP-SCOPE-EMPTY-PULL-REVOKE-001", "model/set-client-assignments", "", "", &operations.revoke},
		{"STEP-SCOPE-EMPTY-PULL-CONNECT-001", "connect/send", "empty_scope_start", "start", &operations.connect},
		{"STEP-SCOPE-EMPTY-PULL-FIRST-PULL-001", "pull/request-page", "empty_scope_start", "start", &operations.firstPull},
		{"STEP-SCOPE-EMPTY-PULL-GRANT-001", "model/set-client-assignments", "", "", &operations.grant},
		{"STEP-SCOPE-EMPTY-PULL-COMMIT-001", "model/commit-source-transaction", "", "", &operations.commit},
		{"STEP-SCOPE-EMPTY-PULL-MATERIALIZE-001", "process/materialize-source-transaction", "", "", &operations.materialize},
		{"STEP-SCOPE-EMPTY-PULL-SYNC-PULL-001", "pull/request-page", "empty_scope_sync", "sync-now", &operations.syncPull},
		{"STEP-SCOPE-EMPTY-PULL-REBUILD-001", "rebuild/request-page", "empty_scope_sync", "sync-now", &operations.rebuild},
	} {
		operation, err := swiftScenarioOperation(steps, wanted.id, wanted.key)
		if err != nil {
			return scopeEmptyPullOperations{}, err
		}
		step := steps[scenarios.StepID(wanted.id)]
		binding := step.NativeBinding
		if step.ExpectedOutcome.Disposition != "success" {
			return scopeEmptyPullOperations{}, fmt.Errorf("Swift scope-empty-pull step %s does not expect success", wanted.id)
		}
		if wanted.callID == "" {
			if binding.Kind != "controller" {
				return scopeEmptyPullOperations{}, fmt.Errorf("Swift scope-empty-pull step %s is not a controller step", wanted.id)
			}
		} else {
			if err := swiftScenarioClient(step, client); err != nil {
				return scopeEmptyPullOperations{}, err
			}
			if binding.Kind != "public-call" || binding.CallID == nil || string(*binding.CallID) != wanted.callID || binding.Stage != "synchronous" || binding.Method != wanted.method || binding.Completion != "idle" {
				return scopeEmptyPullOperations{}, fmt.Errorf("Swift scope-empty-pull public call %s is invalid", wanted.id)
			}
		}
		*wanted.target = operation
	}
	return operations, nil
}

// validateScopeEmptyPullStart proves that the start call of a client with no
// scope connects and then pulls with an empty scopes map. It returns the scope
// set version of that pull.
func validateScopeEmptyPullStart(scenario scenarios.Scenario, operations scopeEmptyPullOperations, start SynchronizationResult) (int64, error) {
	observed := start.transportObservations
	if start.Completion != "idle" || !scopeEmptyPullClasses(observed, "connect", "pull") {
		return 0, fmt.Errorf("Swift scope-empty-pull start produced %v with completion %q, want connect then pull", scopeEmptyPullClassNames(observed), start.Completion)
	}
	if err := validateSwiftWireObservation(scenario, "STEP-SCOPE-EMPTY-PULL-CONNECT-001", observed[0]); err != nil {
		return 0, err
	}
	if err := validateSwiftWireObservation(scenario, "STEP-SCOPE-EMPTY-PULL-FIRST-PULL-001", observed[1]); err != nil {
		return 0, err
	}
	if facts := observed[0].RequestFacts; facts == nil || facts.ScopeCount == nil || *facts.ScopeCount != 0 {
		return 0, errors.New("Swift scope-empty-pull connect announced a known scope")
	}
	return validateScopeEmptyPullRequest(observed[1], operations.firstPull, 0)
}

// validateScopeEmptyPullSync proves that one normal cycle with no reconnect
// pulls with an empty scopes map and rebuilds the scope that the pull added.
func validateScopeEmptyPullSync(scenario scenarios.Scenario, operations scopeEmptyPullOperations, synced SynchronizationResult, startVersion int64, evidence scopeEmptyPullEvidence) error {
	observed := synced.transportObservations
	if synced.Completion != "idle" || !scopeEmptyPullClasses(observed, "pull", "rebuild") {
		return fmt.Errorf("Swift scope-empty-pull sync cycle produced %v with completion %q, want pull then rebuild", scopeEmptyPullClassNames(observed), synced.Completion)
	}
	if err := validateSwiftWireObservation(scenario, "STEP-SCOPE-EMPTY-PULL-SYNC-PULL-001", observed[0]); err != nil {
		return err
	}
	if err := validateSwiftWireObservation(scenario, "STEP-SCOPE-EMPTY-PULL-REBUILD-001", observed[1]); err != nil {
		return err
	}
	version, err := validateScopeEmptyPullRequest(observed[0], operations.syncPull, 1)
	if err != nil {
		return err
	}
	if version != startVersion {
		return fmt.Errorf("Swift scope-empty-pull sync pull scope set version = %d, want %d", version, startVersion)
	}
	limit, err := scopeEmptyPullAuthoredLimit(operations.rebuild)
	if err != nil {
		return err
	}
	request := observed[1].RequestFacts
	response := observed[1].RebuildResponseFacts
	if request == nil || request.ScopeFingerprint == nil || *request.ScopeFingerprint != cursorFingerprint(evidence.grantedScope) || request.RebuildIDFingerprint == nil || *request.RebuildIDFingerprint != cursorFingerprint(evidence.rebuildID) || request.CursorPresent == nil || *request.CursorPresent || request.Limit == nil || *request.Limit != limit {
		return errors.New("Swift scope-empty-pull rebuild request does not target the added scope")
	}
	if response == nil || response.RecordCount != 1 || response.HasMore || !response.HasFinalScopeCursor || !response.HasChecksum || response.ScopeFingerprint != cursorFingerprint(evidence.grantedScope) {
		return errors.New("Swift scope-empty-pull rebuild response is not one terminal page with the granted row")
	}
	return nil
}

func validateScopeEmptyPullRequest(observation transportObservation, authored scenarios.Operation, rebuildScopes int) (int64, error) {
	limit, err := scopeEmptyPullAuthoredLimit(authored)
	if err != nil {
		return 0, err
	}
	request := observation.RequestFacts
	if request == nil || request.ScopeCount == nil || *request.ScopeCount != 0 || request.ScopeSetVersion == nil || request.Limit == nil || *request.Limit != limit {
		return 0, errors.New("Swift scope-empty-pull pull does not carry an empty scopes map")
	}
	if observation.CursorFingerprints == nil || len(observation.CursorFingerprints) != 0 || observation.CursorFingerprintsComplete == nil || !*observation.CursorFingerprintsComplete {
		return 0, errors.New("Swift scope-empty-pull pull carries a cursor")
	}
	response := observation.PullResponseFacts
	if response == nil || response.ChangeCount != 0 || response.HasMore || response.RebuildScopeCount != rebuildScopes || len(response.ScopeCursorFingerprints) != 0 {
		return 0, fmt.Errorf("Swift scope-empty-pull pull response differs from an assignment reconciliation with %d added scopes", rebuildScopes)
	}
	return *request.ScopeSetVersion, nil
}

func validateScopeEmptyPullStartState(snapshot runnerResult) error {
	if snapshot.ScopeStateCount == nil || *snapshot.ScopeStateCount != 0 || len(snapshot.ScopeStates) != 0 || snapshot.ScopeRowCount == nil || *snapshot.ScopeRowCount != 0 || snapshot.ApplicationRowCount == nil || *snapshot.ApplicationRowCount != 0 {
		return errors.New("Swift scope-empty-pull client knows a scope or a row after start")
	}
	return nil
}

func validateScopeEmptyPullFinalState(final runnerResult, synced SynchronizationResult, evidence scopeEmptyPullEvidence) error {
	if len(final.ScopeStates) != 1 || final.ScopeStates[0].ScopeID != evidence.grantedScope || final.ScopeStates[0].Cursor == nil || *final.ScopeStates[0].Cursor == "" || final.ScopeStates[0].Checksum == nil {
		return errors.New("Swift scope-empty-pull client did not persist the granted scope checkpoint")
	}
	if len(synced.transportObservations) != 2 || synced.transportObservations[1].RebuildResponseFacts == nil || synced.transportObservations[1].RebuildResponseFacts.FinalScopeCursorFingerprint == nil || *synced.transportObservations[1].RebuildResponseFacts.FinalScopeCursorFingerprint != cursorFingerprint(*final.ScopeStates[0].Cursor) {
		return errors.New("Swift scope-empty-pull scope checkpoint is not the rebuild terminal cursor")
	}
	if len(final.ScopeRows) != 1 || final.ScopeRows[0].ScopeID != evidence.grantedScope || final.ScopeRows[0].TableName != evidence.tableName || final.ScopeRows[0].RecordID != evidence.recordID {
		return errors.New("Swift scope-empty-pull row provenance does not bind the granted scope")
	}
	if final.ApplicationRowCount == nil || *final.ApplicationRowCount != 1 || len(final.ApplicationRows) != 1 {
		return errors.New("Swift scope-empty-pull client did not materialize exactly one row")
	}
	var recordID string
	if encoded, found := final.ApplicationRows[0][evidence.primaryField]; !found || json.Unmarshal(encoded, &recordID) != nil || recordID != evidence.recordID {
		return errors.New("Swift scope-empty-pull materialized row is not the granted row")
	}
	if len(final.RebuildAttempts) != 0 {
		return errors.New("Swift scope-empty-pull rebuild did not complete")
	}
	return nil
}

func resolveScopeEmptyPullIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, final runnerResult) (map[string]json.RawMessage, scopeEmptyPullEvidence, error) {
	wanted := make(map[string]struct{}, len(scopeEmptyPullAliasNames))
	for _, name := range scopeEmptyPullAliasNames {
		wanted[name] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return nil, scopeEmptyPullEvidence{}, fmt.Errorf("Swift scope-empty-pull identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 || len(aliases) != len(scopeEmptyPullAliasNames) {
		return nil, scopeEmptyPullEvidence{}, errors.New("Swift scope-empty-pull identity alias set is incomplete")
	}
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, scopeEmptyPullEvidence{}, err
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	identifiers := make(map[string]string, len(values))
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
		identifiers[value.Alias] = value.ApplicationIdentifier
	}
	var evidence scopeEmptyPullEvidence
	if json.Unmarshal(runtime["granted-scope"], &evidence.grantedScope) != nil || evidence.grantedScope == "" || json.Unmarshal(runtime["shared-row-primary-key"], &evidence.recordID) != nil || evidence.recordID == "" {
		return nil, scopeEmptyPullEvidence{}, errors.New("Swift scope-empty-pull runtime scope or row identity is invalid")
	}
	evidence.tableName = identifiers["rows-table"]
	evidence.primaryField = identifiers["shared-row-primary-key"]
	if evidence.tableName == "" || evidence.primaryField == "" {
		return nil, scopeEmptyPullEvidence{}, errors.New("Swift scope-empty-pull application identity evidence is incomplete")
	}
	evidence.rebuildID, err = completedSwiftRebuildID(final.Events, evidence.grantedScope)
	if err != nil {
		return nil, scopeEmptyPullEvidence{}, err
	}
	encoded, err := json.Marshal(evidence.rebuildID)
	if err != nil {
		return nil, scopeEmptyPullEvidence{}, fmt.Errorf("encode Swift scope-empty-pull rebuild identity: %w", err)
	}
	runtime["granted-rebuild"] = encoded
	return runtime, evidence, nil
}

func scopeEmptyPullAuthoredLimit(operation scenarios.Operation) (int, error) {
	var payload struct {
		Limit int `json:"limit"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.Limit <= 0 {
		return 0, errors.New("decode Swift scope-empty-pull authored limit failed")
	}
	return payload.Limit, nil
}

func scopeEmptyPullClasses(observations []transportObservation, classes ...string) bool {
	if len(observations) != len(classes) {
		return false
	}
	for index, class := range classes {
		if observations[index].OperationClass != class {
			return false
		}
	}
	return true
}

func scopeEmptyPullClassNames(observations []transportObservation) []string {
	names := make([]string, len(observations))
	for index, observation := range observations {
		names[index] = observation.OperationClass
	}
	return names
}
