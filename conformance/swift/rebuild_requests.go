package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const rebuildRequestsScenarioID = "SCN-PERF-REBUILD-REQUESTS-001"

var rebuildRequestsAliasNames = []string{
	"current-schema",
	"client-generation-one",
	"scope-a",
	"rebuild-cycle",
	"scope-set-version-one",
	"items-table",
	"row-a-primary-key",
	"row-b-primary-key",
	"row-c-primary-key",
}

// RebuildRequestsResult records direct Swift evidence for the first-sync rebuild scenario.
type RebuildRequestsResult struct {
	Call               CallResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type rebuildRequestsIdentityEvidence struct {
	resolutions  []blackbox.NativeIdentityResolution
	runtime      map[string]json.RawMessage
	tableName    string
	primaryField string
}

// RunRebuildRequestsScenario executes the authored first-sync rebuild flow through Swift.
func RunRebuildRequestsScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (RebuildRequestsResult, error) {
	steps, err := swiftScenarioStepMap(scenario, rebuildRequestsScenarioID, 13)
	if err != nil {
		return RebuildRequestsResult{}, err
	}
	if controller == nil || platform == nil {
		return RebuildRequestsResult{}, errors.New("Swift rebuild-requests dependencies are unavailable")
	}
	if err := validateRebuildRequestsBindings(steps, client); err != nil {
		return RebuildRequestsResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("install Swift rebuild-requests contract: %w", err)
	}
	if err := platform.Install(ctx, client, "empty", ""); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("install Swift rebuild-requests client: %w", err)
	}

	initialCommit, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-INITIAL-COMMIT-001", "model/commit-source-transaction")
	if _, err := controller.ApplyStep(ctx, initialCommit); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("commit Swift rebuild-requests snapshot: %w", err)
	}
	initialMaterialize, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-INITIAL-MATERIALIZE-001", "process/materialize-source-transaction")
	if _, err := controller.ProcessStep(ctx, nil, initialMaterialize); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("materialize Swift rebuild-requests snapshot: %w", err)
	}
	assignment, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-ASSIGN-001", "model/set-client-assignments")
	if _, err := controller.ApplyStep(ctx, assignment); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("assign Swift rebuild-requests scope: %w", err)
	}

	connect, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-001", "connect/send")
	firstPage, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-003", "rebuild/request-page")
	finalPage, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-004", "rebuild/request-page")
	pull, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-002", "pull/request-page")
	callID := string(*steps[scenarios.StepID("STEP-PERF-REBUILD-REQUESTS-001")].NativeBinding.CallID)

	state, err := platform.client(client)
	if err != nil {
		return RebuildRequestsResult{}, err
	}
	state.mu.Lock()
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()

	begin, err := platform.BeginCall(ctx, client, callID, "start", RequestOperations{connect})
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("begin Swift rebuild-requests call: %w", err)
	}
	if begin.CallID != callID || begin.State != "in_flight" || begin.Completion != "" || len(begin.Steps) != 1 {
		return RebuildRequestsResult{}, errors.New("Swift rebuild-requests connect did not enter the staged call")
	}
	if err := validateRebuildRequestsStepWire(scenario, "STEP-PERF-REBUILD-REQUESTS-001", begin.Steps[0]); err != nil {
		return RebuildRequestsResult{}, err
	}

	firstResult, err := platform.AwaitStep(ctx, client, callID, firstPage)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("await Swift first rebuild page: %w", err)
	}
	if err := validateRebuildRequestsStepWire(scenario, "STEP-PERF-REBUILD-REQUESTS-003", firstResult); err != nil {
		return RebuildRequestsResult{}, err
	}
	firstPaused, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("capture Swift first rebuild pause: %w", err)
	}
	if err := validateRebuildRequestsFirstPause(firstPaused); err != nil {
		return RebuildRequestsResult{}, err
	}

	concurrentCommit, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-CONCURRENT-COMMIT-001", "model/commit-source-transaction")
	if _, err := controller.ApplyStep(ctx, concurrentCommit); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("commit Swift concurrent rebuild row: %w", err)
	}
	concurrentMaterialize, _ := swiftScenarioOperation(steps, "STEP-PERF-REBUILD-REQUESTS-CONCURRENT-MATERIALIZE-001", "process/materialize-source-transaction")
	if _, err := controller.ProcessStep(ctx, nil, concurrentMaterialize); err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("materialize Swift concurrent rebuild row: %w", err)
	}

	finalResult, err := platform.AwaitStep(ctx, client, callID, finalPage)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("await Swift final rebuild page: %w", err)
	}
	if err := validateRebuildRequestsStepWire(scenario, "STEP-PERF-REBUILD-REQUESTS-004", finalResult); err != nil {
		return RebuildRequestsResult{}, err
	}

	pullResult, err := platform.AwaitStep(ctx, client, callID, pull)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("await Swift post-rebuild pull: %w", err)
	}
	if err := validateRebuildRequestsStepWire(scenario, "STEP-PERF-REBUILD-REQUESTS-002", pullResult); err != nil {
		return RebuildRequestsResult{}, err
	}
	pullPaused, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("capture Swift post-rebuild pull pause: %w", err)
	}
	if err := validateRebuildRequestsPullPause(firstPaused, pullPaused); err != nil {
		return RebuildRequestsResult{}, err
	}

	completed, err := platform.AwaitCall(ctx, client, callID)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("complete Swift rebuild-requests call: %w", err)
	}
	if completed.CallID != callID || completed.State != "completed" || completed.Completion != "idle" {
		return RebuildRequestsResult{}, errors.New("Swift rebuild-requests call did not complete idle")
	}

	state.mu.Lock()
	transport, transportErr := state.session.ObservationsAfter(transportCheckpoint)
	state.mu.Unlock()
	if transportErr != nil {
		return RebuildRequestsResult{}, fmt.Errorf("capture Swift rebuild-requests transport: %w", transportErr)
	}
	if err := validateRebuildRequestsTransport(scenario, transport); err != nil {
		return RebuildRequestsResult{}, err
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
		return RebuildRequestsResult{}, fmt.Errorf("capture Swift rebuild-requests client state: %w", err)
	}
	finalSnapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return RebuildRequestsResult{}, fmt.Errorf("capture Swift rebuild-requests final state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return RebuildRequestsResult{}, fmt.Errorf("capture Swift rebuild-requests server state: %w", err)
	}
	evidence, err := resolveRebuildRequestsIdentities(controller, scenario.NativeIdentityAliases, transport, firstPaused, finalSnapshot)
	if err != nil {
		return RebuildRequestsResult{}, err
	}
	actualClient, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return RebuildRequestsResult{}, err
	}
	if err := validateRebuildRequestsState(serverCaptures[0].StateFacts, actualClient, pullPaused, finalSnapshot, transport, evidence); err != nil {
		return RebuildRequestsResult{}, err
	}

	return RebuildRequestsResult{
		Call:               completed,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: evidence.resolutions,
	}, nil
}

func validateRebuildRequestsBindings(steps map[scenarios.StepID]scenarios.Step, client Client) error {
	expected := []struct {
		id      string
		key     string
		kind    string
		stage   string
		method  string
		closing string
	}{
		{"STEP-PERF-REBUILD-REQUESTS-INITIAL-COMMIT-001", "model/commit-source-transaction", "controller", "", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-INITIAL-MATERIALIZE-001", "process/materialize-source-transaction", "controller", "", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-ASSIGN-001", "model/set-client-assignments", "controller", "", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-001", "connect/send", "public-call", "begin", "start", ""},
		{"STEP-PERF-REBUILD-REQUESTS-BEGIN-001", "local/begin-rebuild", "public-call", "await-step", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-003", "rebuild/request-page", "public-call", "await-step", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-CONCURRENT-COMMIT-001", "model/commit-source-transaction", "controller", "", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-CONCURRENT-MATERIALIZE-001", "process/materialize-source-transaction", "controller", "", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-APPLY-001", "local/apply-rebuild-page", "public-call", "await-step", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-004", "rebuild/request-page", "public-call", "await-step", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-APPLY-002", "local/apply-rebuild-page", "public-call", "await-step", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-FINALIZE-001", "local/finalize-rebuild", "public-call", "await-step", "", ""},
		{"STEP-PERF-REBUILD-REQUESTS-002", "pull/request-page", "public-call", "await-call", "", "idle"},
	}
	var callID scenarios.NativeCallID
	for _, wanted := range expected {
		step := steps[scenarios.StepID(wanted.id)]
		if _, err := swiftScenarioOperation(steps, wanted.id, wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || binding.Completion != wanted.closing || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Swift rebuild-requests binding %s is invalid", wanted.id)
		}
		if wanted.kind != "public-call" {
			continue
		}
		if err := swiftScenarioClient(step, client); err != nil {
			return err
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Swift rebuild-requests binding %s has no call identity", wanted.id)
		}
		if callID == "" {
			callID = *binding.CallID
		} else if callID != *binding.CallID {
			return errors.New("Swift rebuild-requests bindings do not share one public call")
		}
	}
	return nil
}

func validateRebuildRequestsStepWire(scenario scenarios.Scenario, stepID string, observed StepObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Swift rebuild-requests wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Swift rebuild-requests wire expectation %s is absent", stepID)
}

func validateRebuildRequestsFirstPause(snapshot runnerResult) error {
	if len(snapshot.RebuildAttempts) != 1 || len(snapshot.RebuildReceipts) != 0 || len(snapshot.ScopeRows) != 0 || len(snapshot.RowMetadataRecords) != 0 || snapshot.RebuildAttempts[0].Cursor != nil || snapshot.RebuildAttempts[0].PageLimit != 1 {
		return errors.New("Swift first rebuild page was not paused before local apply")
	}
	return nil
}

func validateRebuildRequestsPullPause(first, pull runnerResult) error {
	if len(pull.RebuildAttempts) != 0 || len(pull.RebuildReceipts) != 1 || len(pull.ScopeRows) != 2 || len(pull.RowMetadataRecords) != 2 || len(pull.ApplicationRows) != 2 {
		return errors.New("Swift two-page rebuild did not finalize before incremental pull")
	}
	receipt := pull.RebuildReceipts[0]
	if !validateRebuildRequestsReceipt(receipt, first.RebuildAttempts[0].RebuildID) {
		return errors.New("Swift two-page rebuild receipt is invalid")
	}
	return nil
}

func validateRebuildRequestsReceipt(receipt rebuildReceiptRecord, rebuildID string) bool {
	return receipt.RebuildIDFingerprint == cursorFingerprint(rebuildID) &&
		receipt.PageCount == 2 &&
		receipt.ReturnedRecordCount == 2 &&
		reflect.DeepEqual(receipt.RequestChainExpected, receipt.RequestChainObserved) &&
		receipt.RecordsInCanonicalOrder &&
		receipt.RowChecksumsValid &&
		receipt.ComputedScopeChecksum != nil &&
		receipt.FinalScopeChecksum != nil &&
		*receipt.ComputedScopeChecksum == *receipt.FinalScopeChecksum
}

func validateRebuildRequestsTransport(scenario scenarios.Scenario, observations []transportObservation) error {
	ids := []string{
		"STEP-PERF-REBUILD-REQUESTS-001",
		"STEP-PERF-REBUILD-REQUESTS-003",
		"STEP-PERF-REBUILD-REQUESTS-004",
		"STEP-PERF-REBUILD-REQUESTS-002",
	}
	classes := []string{"connect", "rebuild", "rebuild", "pull"}
	if len(observations) != len(ids) {
		return fmt.Errorf("Swift rebuild-requests transport count = %d, want %d", len(observations), len(ids))
	}
	for index := range observations {
		if observations[index].OperationClass != classes[index] {
			return errors.New("Swift rebuild-requests transport order differs from the authored call")
		}
		if err := validateSwiftWireObservation(scenario, ids[index], observations[index]); err != nil {
			return err
		}
	}
	first := observations[1]
	final := observations[2]
	pull := observations[3]
	if first.RequestFacts == nil || final.RequestFacts == nil || pull.RequestFacts == nil || first.RebuildResponseFacts == nil || final.RebuildResponseFacts == nil || pull.PullResponseFacts == nil {
		return errors.New("Swift rebuild-requests transport facts are incomplete")
	}
	firstRequest := first.RequestFacts
	finalRequest := final.RequestFacts
	firstResponse := first.RebuildResponseFacts
	finalResponse := final.RebuildResponseFacts
	if firstRequest.ClientGeneration == nil || finalRequest.ClientGeneration == nil || *firstRequest.ClientGeneration != *finalRequest.ClientGeneration || firstRequest.SchemaVersion != finalRequest.SchemaVersion || firstRequest.SchemaHash != finalRequest.SchemaHash || firstRequest.ScopeFingerprint == nil || finalRequest.ScopeFingerprint == nil || *firstRequest.ScopeFingerprint != *finalRequest.ScopeFingerprint || firstRequest.RebuildIDFingerprint == nil || finalRequest.RebuildIDFingerprint == nil || *firstRequest.RebuildIDFingerprint != *finalRequest.RebuildIDFingerprint || firstRequest.Limit == nil || finalRequest.Limit == nil || *firstRequest.Limit != 1 || *finalRequest.Limit != 1 {
		return errors.New("Swift rebuild-requests page identities are inconsistent")
	}
	if firstRequest.CursorPresent == nil || *firstRequest.CursorPresent || firstRequest.CursorFingerprint != nil || finalRequest.CursorPresent == nil || !*finalRequest.CursorPresent || finalRequest.CursorFingerprint == nil {
		return errors.New("Swift rebuild-requests continuation chain is invalid")
	}
	if firstResponse.RecordCount != 1 || !firstResponse.HasMore || !firstResponse.HasCursor || firstResponse.HasFinalScopeCursor || firstResponse.HasChecksum || firstResponse.FinalScopeCursorFingerprint != nil || firstResponse.ScopeFingerprint != *firstRequest.ScopeFingerprint {
		return errors.New("Swift first rebuild response is not an intermediate one-row page")
	}
	if finalResponse.RecordCount != 1 || finalResponse.HasMore || finalResponse.HasCursor || !finalResponse.HasFinalScopeCursor || !finalResponse.HasChecksum || finalResponse.FinalScopeCursorFingerprint == nil || finalResponse.ScopeFingerprint != *finalRequest.ScopeFingerprint {
		return errors.New("Swift final rebuild response is not a terminal one-row page")
	}
	if pull.RequestFacts.ClientGeneration == nil || *pull.RequestFacts.ClientGeneration != *firstRequest.ClientGeneration || pull.RequestFacts.SchemaVersion != firstRequest.SchemaVersion || pull.RequestFacts.SchemaHash != firstRequest.SchemaHash || pull.RequestFacts.ScopeSetVersion == nil || pull.RequestFacts.ScopeCount == nil || *pull.RequestFacts.ScopeCount != 1 || pull.RequestFacts.Limit == nil || *pull.RequestFacts.Limit != 1 || pull.CursorFingerprintsComplete == nil || !*pull.CursorFingerprintsComplete || len(pull.CursorFingerprints) != 1 || pull.CursorFingerprints[0] != *finalResponse.FinalScopeCursorFingerprint {
		return errors.New("Swift post-rebuild pull is not bound to the final rebuild cursor")
	}
	if pull.PullResponseFacts.ChangeCount != 1 || pull.PullResponseFacts.HasMore || pull.PullResponseFacts.RebuildScopeCount != 0 || pull.PullResponseFacts.ChecksumCount != 1 || !pull.PullResponseFacts.ScopeCursorFingerprintsComplete || len(pull.PullResponseFacts.ScopeCursorFingerprints) != 1 {
		return errors.New("Swift post-rebuild pull did not return the one concurrent row")
	}
	return nil
}

func resolveRebuildRequestsIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, transport []transportObservation, firstPaused, final runnerResult) (rebuildRequestsIdentityEvidence, error) {
	if len(aliases) != len(rebuildRequestsAliasNames) || len(transport) != 4 || len(firstPaused.RebuildAttempts) != 1 {
		return rebuildRequestsIdentityEvidence{}, errors.New("Swift rebuild-requests identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(rebuildRequestsAliasNames))
	for _, name := range rebuildRequestsAliasNames {
		wanted[name] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return rebuildRequestsIdentityEvidence{}, fmt.Errorf("Swift rebuild-requests identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return rebuildRequestsIdentityEvidence{}, errors.New("Swift rebuild-requests identity alias set is incomplete")
	}

	runtime := make(map[string]json.RawMessage, len(aliases))
	applicationIdentifiers := make(map[string]string)
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return rebuildRequestsIdentityEvidence{}, err
	}
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
		applicationIdentifiers[value.Alias] = value.ApplicationIdentifier
	}
	firstRequest := transport[1].RequestFacts
	pullRequest := transport[3].RequestFacts
	if firstRequest == nil || firstRequest.ClientGeneration == nil || pullRequest == nil || pullRequest.ScopeSetVersion == nil {
		return rebuildRequestsIdentityEvidence{}, errors.New("Swift rebuild-requests generated transport identities are absent")
	}
	generated := map[string]any{
		"client-generation-one": *firstRequest.ClientGeneration,
		"rebuild-cycle":         firstPaused.RebuildAttempts[0].RebuildID,
		"scope-set-version-one": *pullRequest.ScopeSetVersion,
	}
	for alias, value := range generated {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return rebuildRequestsIdentityEvidence{}, fmt.Errorf("encode Swift rebuild-requests alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	for _, alias := range rebuildRequestsAliasNames {
		if len(runtime[alias]) == 0 {
			return rebuildRequestsIdentityEvidence{}, fmt.Errorf("Swift rebuild-requests alias %q has no runtime evidence", alias)
		}
	}
	resolutions, err := resolveSwiftNativeIdentities(aliases, runtime)
	if err != nil {
		return rebuildRequestsIdentityEvidence{}, err
	}
	if final.Schema == nil || applicationIdentifiers["items-table"] == "" || applicationIdentifiers["row-a-primary-key"] == "" || applicationIdentifiers["row-a-primary-key"] != applicationIdentifiers["row-b-primary-key"] || applicationIdentifiers["row-a-primary-key"] != applicationIdentifiers["row-c-primary-key"] {
		return rebuildRequestsIdentityEvidence{}, errors.New("Swift rebuild-requests application identity evidence is incomplete")
	}
	return rebuildRequestsIdentityEvidence{
		resolutions:  resolutions,
		runtime:      runtime,
		tableName:    applicationIdentifiers["items-table"],
		primaryField: applicationIdentifiers["row-a-primary-key"],
	}, nil
}

func validateRebuildRequestsState(server, client scenarios.StateFacts, beforePull, final runnerResult, transport []transportObservation, evidence rebuildRequestsIdentityEvidence) error {
	if server.TransactionCount == nil || *server.TransactionCount != 2 || server.RowCount == nil || *server.RowCount != 3 || server.ScopeCount == nil || *server.ScopeCount != 2 || server.RebuildCount == nil || *server.RebuildCount != 1 || len(server.Transactions) != 2 || len(server.Rows) != 3 || len(server.Scopes) != 2 || len(server.Rebuilds) != 1 {
		return errors.New("Swift rebuild-requests server state is incomplete")
	}
	cardinalities := make([]uint64, 0, len(server.Scopes))
	for _, scope := range server.Scopes {
		cardinalities = append(cardinalities, scope.Cardinality)
	}
	sort.Slice(cardinalities, func(left, right int) bool { return cardinalities[left] < cardinalities[right] })
	rebuild := server.Rebuilds[0]
	if !reflect.DeepEqual(cardinalities, []uint64{0, 3}) || rebuild.UserID != "user-a" || rebuild.ClientID != "client-a" || rebuild.ScopeID != "scope-a" || rebuild.PageLimit != 1 || rebuild.StagedRowCount != 2 || rebuild.PageCount != 2 || !rebuild.HasContinuation || !rebuild.HasFinalCursor {
		return errors.New("Swift rebuild-requests server state differs from the authored flow")
	}
	if len(client.Clients) != 1 || client.Clients[0].RowCount == nil || *client.Clients[0].RowCount != 3 || client.Clients[0].ProvenanceCount == nil || *client.Clients[0].ProvenanceCount != 3 || client.Clients[0].CheckpointCount == nil || *client.Clients[0].CheckpointCount != 1 || client.Clients[0].RebuildAttemptCount == nil || *client.Clients[0].RebuildAttemptCount != 1 {
		return errors.New("Swift rebuild-requests durable client facts are incomplete")
	}
	if final.ApplicationRowCount == nil || *final.ApplicationRowCount != 3 || len(final.ApplicationRows) != 3 || len(final.ScopeStates) != 1 || len(final.ScopeRows) != 3 || len(final.RowMetadataRecords) != 3 || len(final.RebuildAttempts) != 0 || len(final.RebuildReceipts) != 1 {
		return errors.New("Swift rebuild-requests final client state is incomplete")
	}
	var rebuildID string
	if json.Unmarshal(evidence.runtime["rebuild-cycle"], &rebuildID) != nil || rebuildID == "" || len(beforePull.ApplicationRows) != 2 || len(beforePull.ScopeRows) != 2 || !validateRebuildRequestsReceipt(final.RebuildReceipts[0], rebuildID) {
		return errors.New("Swift rebuild-requests final receipt differs from its staged snapshot")
	}

	var runtimeScope string
	var runtimeSchema schemaRef
	if json.Unmarshal(evidence.runtime["scope-a"], &runtimeScope) != nil || runtimeScope == "" || json.Unmarshal(evidence.runtime["current-schema"], &runtimeSchema) != nil || final.Schema == nil || runtimeSchema != *final.Schema || final.ScopeStates[0].ScopeID != runtimeScope {
		return errors.New("Swift rebuild-requests durable scope or schema identity is inconsistent")
	}
	runtimeRecords := make(map[string]struct{}, 3)
	for _, alias := range []string{"row-a-primary-key", "row-b-primary-key", "row-c-primary-key"} {
		var value string
		if json.Unmarshal(evidence.runtime[alias], &value) != nil || value == "" {
			return errors.New("Swift rebuild-requests row identity evidence is invalid")
		}
		runtimeRecords[value] = struct{}{}
	}
	if len(runtimeRecords) != 3 {
		return errors.New("Swift rebuild-requests row identities are not distinct")
	}
	metadata := make(map[string]rowMetadataRecord, len(final.RowMetadataRecords))
	for _, value := range final.RowMetadataRecords {
		metadata[value.RecordID] = value
	}
	for _, row := range final.ScopeRows {
		meta, found := metadata[row.RecordID]
		rowChecksum, checksumErr := swiftChecksumDigest(meta.RowChecksum)
		if _, expected := runtimeRecords[row.RecordID]; !expected || !found || checksumErr != nil || rowChecksum == nil || row.ScopeID != runtimeScope || row.TableName != evidence.tableName || meta.TableName != evidence.tableName || row.Checksum != *rowChecksum {
			return errors.New("Swift rebuild-requests row provenance is inconsistent")
		}
	}
	applicationRecords := make(map[string]struct{}, len(final.ApplicationRows))
	for _, row := range final.ApplicationRows {
		encoded, found := row[evidence.primaryField]
		var recordID string
		if !found || json.Unmarshal(encoded, &recordID) != nil || recordID == "" {
			return errors.New("Swift rebuild-requests application row identity is invalid")
		}
		applicationRecords[recordID] = struct{}{}
	}
	if !reflect.DeepEqual(applicationRecords, runtimeRecords) {
		return errors.New("Swift rebuild-requests application rows do not match durable provenance")
	}
	storedChecksum, storedErr := swiftChecksumDigest(final.ScopeStates[0].Checksum)
	localChecksum, localErr := swiftChecksumDigest(pointerString(final.ScopeStates[0].LocalChecksum))
	if final.ScopeStates[0].Cursor == nil || storedErr != nil || localErr != nil || storedChecksum == nil || localChecksum == nil || *storedChecksum != *localChecksum || transport[3].PullResponseFacts == nil || len(transport[3].PullResponseFacts.ScopeCursorFingerprints) != 1 || transport[3].PullResponseFacts.ScopeCursorFingerprints[0] != cursorFingerprint(*final.ScopeStates[0].Cursor) {
		return errors.New("Swift rebuild-requests final checkpoint is not verified")
	}
	return nil
}
