package swift

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

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
	FaultCalls         []SynchronizationResult
	MeasuredCall       SynchronizationResult
	Restart            StepObservation
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type steadyPullIdentityEvidence struct {
	resolutions []blackbox.NativeIdentityResolution
	tableName   string
}

type steadyPullFault string

const (
	steadyPullMalformedTypedRow steadyPullFault = "malformed-typed-row"
	steadyPullRowDigest         steadyPullFault = "row-digest"
	steadyPullScopeDigest       steadyPullFault = "scope-digest"
	steadyPullTerminalMap       steadyPullFault = "terminal-map"
	steadyPullDuplicateEffect   steadyPullFault = "duplicate-effect"
	steadyPullCursorBeforeApply steadyPullFault = "cursor-before-apply"
	steadyPullResponseLimit                     = 16 << 20
)

var steadyPullFaults = []steadyPullFault{
	steadyPullMalformedTypedRow,
	steadyPullRowDigest,
	steadyPullScopeDigest,
	steadyPullTerminalMap,
	steadyPullDuplicateEffect,
	steadyPullCursorBeforeApply,
}

type steadyPullFaultProxy struct {
	mu      sync.Mutex
	armed   steadyPullFault
	applied steadyPullFault
	failure error
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
	if err := validateSwiftSteadyPullFaultPlans(scenario); err != nil {
		return SteadyPullResult{}, err
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
	faultProxy, closeFaultProxy, err := startSwiftSteadyPullFaultProxy(platform)
	if err != nil {
		return SteadyPullResult{}, err
	}
	defer closeFaultProxy()
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
	if !validateSwiftBaselineCallShape(baseline) {
		observed := make([]string, 0, len(baseline.transportObservations))
		for _, observation := range baseline.transportObservations {
			observed = append(observed, fmt.Sprintf("%s:%d", observation.OperationClass, observation.StatusCode))
		}
		return SteadyPullResult{}, fmt.Errorf("Swift steady-pull baseline produced %v, want connect, rebuild, and pull", observed)
	}
	if err := validateSwiftSteadyPullBaselineWires(scenario, baseline); err != nil {
		return SteadyPullResult{}, err
	}
	pristine, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull pristine state: %w", err)
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

	faultCalls := make([]SynchronizationResult, 0, len(steadyPullFaults))
	for index, fault := range steadyPullFaults {
		if err := faultProxy.arm(fault); err != nil {
			return SteadyPullResult{}, err
		}
		method := "retry-after-error"
		if index == 0 {
			method = "sync-now"
		}
		failed, callErr := platform.Synchronize(ctx, client, method, RequestOperations{measuredPull})
		if callErr != nil {
			return SteadyPullResult{}, fmt.Errorf("run Swift steady-pull %s fault: %w", fault, callErr)
		}
		if err := faultProxy.verify(fault); err != nil {
			return SteadyPullResult{}, err
		}
		afterFault, captureErr := platform.captureSnapshot(ctx, client)
		if captureErr != nil {
			return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull %s fault state: %w", fault, captureErr)
		}
		if err := validateSwiftSteadyPullFaultResult(fault, failed, afterFault); err != nil {
			return SteadyPullResult{}, err
		}
		if !equalSwiftSteadyPullDurableState(pristine, afterFault) {
			return SteadyPullResult{}, fmt.Errorf("Swift steady-pull %s fault changed durable rows or cursor state", fault)
		}
		faultCalls = append(faultCalls, failed)
	}

	measured, err := platform.Synchronize(ctx, client, "retry-after-error", RequestOperations{measuredPull})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("retry Swift measured pull: %w", err)
	}
	if measured.Completion != "idle" || len(measured.Steps) != 1 || len(measured.transportObservations) != 1 || measured.transportObservations[0].StatusCode != 200 {
		return SteadyPullResult{}, errors.New("Swift measured pull did not complete successfully")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-STEADY-PULL-001", "pull", measured); err != nil {
		return SteadyPullResult{}, err
	}
	beforeRestart, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull state before restart: %w", err)
	}
	restartOperation := scenarios.Operation{
		ContractOperation: "process",
		Name:              "restart-client",
		Payload:           queueJSON(map[string]any{"user_id": client.UserID, "client_id": client.ClientID}),
	}
	restart, err := platform.ProcessStep(ctx, client, restartOperation)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("restart Swift steady-pull client: %w", err)
	}
	if restart.Disposition != "success" {
		return SteadyPullResult{}, fmt.Errorf("restart Swift steady-pull client returned disposition %q", restart.Disposition)
	}
	afterRestart, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull state after restart: %w", err)
	}
	if !equalSwiftSteadyPullDurableState(beforeRestart, afterRestart) {
		return SteadyPullResult{}, errors.New("Swift steady-pull restart changed durable rows or cursor state")
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
	snapshot := afterRestart
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
		FaultCalls:         faultCalls,
		MeasuredCall:       measured,
		Restart:            restart,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: identityEvidence.resolutions,
	}, nil
}

func validateSwiftSteadyPullFaultPlans(scenario scenarios.Scenario) error {
	required := map[string]bool{
		"FPL-PERF-STEADY-PULL-CURSOR-002":    false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-003": false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-004": false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-005": false,
		"FPL-PERF-STEADY-PULL-INTEGRITY-006": false,
		"FPL-PERF-STEADY-PULL-PULL-005":      false,
	}
	for _, plan := range scenario.FaultPlans {
		if _, found := required[string(plan.ID)]; found {
			required[string(plan.ID)] = true
		}
	}
	for id, found := range required {
		if !found {
			return fmt.Errorf("Swift steady-pull fault plan %s is absent", id)
		}
	}
	return nil
}

func startSwiftSteadyPullFaultProxy(platform *Platform) (*steadyPullFaultProxy, func(), error) {
	platform.mu.Lock()
	if platform.closed {
		platform.mu.Unlock()
		return nil, nil, errors.New("Swift steady-pull platform is closed")
	}
	originalURL := platform.config.ServerURL
	upstream, err := url.Parse(originalURL)
	if err != nil || upstream.Scheme == "" || upstream.Host == "" {
		platform.mu.Unlock()
		return nil, nil, errors.New("Swift steady-pull proxy upstream is invalid")
	}
	fault := &steadyPullFaultProxy{}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.ModifyResponse = fault.modifyResponse
	server := httptest.NewServer(proxy)
	platform.config.ServerURL = server.URL
	platform.mu.Unlock()
	return fault, func() {
		platform.mu.Lock()
		if platform.config.ServerURL == server.URL {
			platform.config.ServerURL = originalURL
		}
		platform.mu.Unlock()
		server.Close()
	}, nil
}

func (p *steadyPullFaultProxy) arm(fault steadyPullFault) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.armed != "" || p.applied != "" || p.failure != nil {
		return errors.New("Swift steady-pull response fault is already active")
	}
	p.armed = fault
	return nil
}

func (p *steadyPullFaultProxy) verify(fault steadyPullFault) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		p.applied = ""
		p.failure = nil
	}()
	if p.failure != nil {
		return p.failure
	}
	if p.armed != "" || p.applied != fault {
		return fmt.Errorf("Swift steady-pull %s fault did not mutate one pull response", fault)
	}
	return nil
}

func (p *steadyPullFaultProxy) modifyResponse(response *http.Response) error {
	if response.StatusCode != http.StatusOK || response.Request == nil || !strings.HasSuffix(response.Request.URL.Path, "/sync/pull") {
		return nil
	}
	p.mu.Lock()
	fault := p.armed
	if fault == "" {
		p.mu.Unlock()
		return nil
	}
	p.armed = ""
	p.mu.Unlock()
	body, err := io.ReadAll(io.LimitReader(response.Body, steadyPullResponseLimit+1))
	_ = response.Body.Close()
	if err != nil || len(body) > steadyPullResponseLimit {
		err = errors.New("Swift steady-pull pull response exceeds the fault bound")
	} else {
		body, err = mutateSwiftSteadyPullResponse(body, fault)
	}
	if err != nil {
		p.mu.Lock()
		p.failure = err
		p.mu.Unlock()
		return err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
	p.mu.Lock()
	p.applied = fault
	p.mu.Unlock()
	return nil
}

func mutateSwiftSteadyPullResponse(body []byte, fault steadyPullFault) ([]byte, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, errors.New("decode Swift steady-pull pull response failed")
	}
	var changes []map[string]json.RawMessage
	if err := json.Unmarshal(response["changes"], &changes); err != nil || len(changes) != 1 {
		return nil, errors.New("Swift steady-pull fault requires one canonical change")
	}
	switch fault {
	case steadyPullMalformedTypedRow:
		if err := mutateSwiftSteadyPullTypedRow(changes[0]); err != nil {
			return nil, err
		}
	case steadyPullRowDigest:
		if err := mutateSwiftSteadyPullChecksum(changes[0], "row_checksum"); err != nil {
			return nil, err
		}
	case steadyPullScopeDigest:
		if err := mutateSwiftSteadyPullScopeDigest(response); err != nil {
			return nil, err
		}
	case steadyPullTerminalMap:
		if err := mutateSwiftSteadyPullTerminalMap(response); err != nil {
			return nil, err
		}
	case steadyPullDuplicateEffect:
		duplicate, err := cloneSwiftSteadyPullChange(changes[0])
		if err != nil {
			return nil, err
		}
		changes = append(changes, duplicate)
	case steadyPullCursorBeforeApply:
		later, err := cloneSwiftSteadyPullChange(changes[0])
		if err != nil {
			return nil, err
		}
		if err := mutateSwiftSteadyPullPrimaryKey(later); err != nil {
			return nil, err
		}
		changes = append(changes, later)
	default:
		return nil, errors.New("Swift steady-pull fault is unsupported")
	}
	if fault != steadyPullScopeDigest && fault != steadyPullTerminalMap {
		encoded, err := json.Marshal(changes)
		if err != nil {
			return nil, errors.New("encode Swift steady-pull changes failed")
		}
		response["changes"] = encoded
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, errors.New("encode Swift steady-pull pull response failed")
	}
	return encoded, nil
}

func mutateSwiftSteadyPullTypedRow(change map[string]json.RawMessage) error {
	var row map[string]json.RawMessage
	var primary map[string]json.RawMessage
	if json.Unmarshal(change["row"], &row) != nil || len(row) == 0 || json.Unmarshal(change["pk"], &primary) != nil {
		return errors.New("Swift steady-pull typed-row fault target is invalid")
	}
	fields := make([]string, 0, len(row))
	for field := range row {
		if _, isPrimary := primary[field]; !isPrimary {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return errors.New("Swift steady-pull typed-row fault has no writable field")
	}
	sort.Strings(fields)
	row[fields[0]] = json.RawMessage("1")
	encoded, err := json.Marshal(row)
	if err != nil {
		return errors.New("encode Swift steady-pull typed-row fault failed")
	}
	change["row"] = encoded
	return nil
}

func mutateSwiftSteadyPullChecksum(object map[string]json.RawMessage, member string) error {
	var checksum map[string]json.RawMessage
	if json.Unmarshal(object[member], &checksum) != nil || len(checksum) == 0 {
		return errors.New("Swift steady-pull row-digest fault target is invalid")
	}
	checksum["digest"] = json.RawMessage(`"0000000000000000000000000000000000000000000000000000000000000000"`)
	encoded, err := json.Marshal(checksum)
	if err != nil {
		return errors.New("encode Swift steady-pull row-digest fault failed")
	}
	object[member] = encoded
	return nil
}

func mutateSwiftSteadyPullScopeDigest(response map[string]json.RawMessage) error {
	var checksums map[string]json.RawMessage
	if json.Unmarshal(response["checksums"], &checksums) != nil || len(checksums) != 1 {
		return errors.New("Swift steady-pull scope-digest fault target is invalid")
	}
	for scope, raw := range checksums {
		var checksum map[string]json.RawMessage
		if json.Unmarshal(raw, &checksum) != nil {
			return errors.New("Swift steady-pull scope digest is invalid")
		}
		checksum["algorithm"] = json.RawMessage(`"sha1"`)
		checksums[scope], _ = json.Marshal(checksum)
	}
	response["checksums"], _ = json.Marshal(checksums)
	return nil
}

func mutateSwiftSteadyPullTerminalMap(response map[string]json.RawMessage) error {
	var checksums map[string]json.RawMessage
	if json.Unmarshal(response["checksums"], &checksums) != nil || len(checksums) != 1 {
		return errors.New("Swift steady-pull terminal-map fault target is invalid")
	}
	for _, checksum := range checksums {
		checksums["native-extra-scope"] = append(json.RawMessage(nil), checksum...)
		break
	}
	response["checksums"], _ = json.Marshal(checksums)
	return nil
}

func cloneSwiftSteadyPullChange(change map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	encoded, err := json.Marshal(change)
	if err != nil {
		return nil, errors.New("encode Swift steady-pull effect failed")
	}
	var clone map[string]json.RawMessage
	if json.Unmarshal(encoded, &clone) != nil {
		return nil, errors.New("clone Swift steady-pull effect failed")
	}
	return clone, nil
}

func mutateSwiftSteadyPullPrimaryKey(change map[string]json.RawMessage) error {
	var primary map[string]json.RawMessage
	var row map[string]json.RawMessage
	if json.Unmarshal(change["pk"], &primary) != nil || len(primary) != 1 || json.Unmarshal(change["row"], &row) != nil {
		return errors.New("Swift steady-pull cursor-before-apply fault target is invalid")
	}
	value, _ := json.Marshal("native-cursor-before-apply")
	for field := range primary {
		primary[field] = value
		if _, found := row[field]; found {
			row[field] = value
		}
	}
	change["pk"], _ = json.Marshal(primary)
	change["row"], _ = json.Marshal(row)
	return nil
}

func validateSwiftSteadyPullFaultResult(fault steadyPullFault, result SynchronizationResult, snapshot runnerResult) error {
	if result.Completion != "error" || len(result.Steps) != 1 || len(result.transportObservations) != 1 || result.transportObservations[0].OperationClass != "pull" || result.transportObservations[0].StatusCode != http.StatusOK {
		return fmt.Errorf("Swift steady-pull %s fault did not produce one failed pull call", fault)
	}
	failure := snapshot.Failure
	if failure == nil || failure.Operation != "pulling" || failure.Code != "invalid_response" || failure.Retryable || failure.RecoveryAction != "retry" {
		return fmt.Errorf("Swift steady-pull %s fault did not expose retry recovery evidence", fault)
	}
	return nil
}

func equalSwiftSteadyPullDurableState(left, right runnerResult) bool {
	normalize := func(value runnerResult) runnerResult {
		value.Status = nil
		value.RowsAffected = nil
		value.Events = nil
		value.Failure = nil
		value.TransportObservations = nil
		value.CallID = nil
		value.State = nil
		value.Completion = nil
		value.CallErrorCategory = nil
		value.ProcessID = ""
		value.ProvenanceMaintenanceWorkCursor = nil
		return value
	}
	return reflect.DeepEqual(normalize(left), normalize(right))
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
