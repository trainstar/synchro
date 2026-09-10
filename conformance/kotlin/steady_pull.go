package kotlin

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

// SteadyPullResult records direct Kotlin Android evidence for steady-pull.
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
	resolutions    []blackbox.NativeIdentityResolution
	tableName      string
	primaryKeyName string
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

// RunSteadyPullScenario executes the authored steady-pull flow through Kotlin Android.
func RunSteadyPullScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (SteadyPullResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, steadyPullScenarioID, 8)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if controller == nil || platform == nil {
		return SteadyPullResult{}, errors.New("Kotlin Android steady-pull dependencies are unavailable")
	}
	if err := validateKotlinSteadyPullFaultPlans(scenario); err != nil {
		return SteadyPullResult{}, err
	}
	for _, id := range []string{"STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", "STEP-PERF-STEADY-PULL-001"} {
		if err := kotlinScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return SteadyPullResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Kotlin Android steady-pull contract: %w", err)
	}
	faultProxy, closeFaultProxy, err := startKotlinSteadyPullFaultProxy(platform)
	if err != nil {
		return SteadyPullResult{}, err
	}
	defer closeFaultProxy()
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
	pristine, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull pristine state: %w", err)
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
	faultCalls := make([]SynchronizationResult, 0, len(steadyPullFaults))
	for index, fault := range steadyPullFaults {
		if err := faultProxy.arm(fault); err != nil {
			return SteadyPullResult{}, err
		}
		method := "retry-after-error"
		if index == 0 {
			method = "sync-now"
		}
		failed, callErr := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: method, Operations: []scenarios.Operation{measuredPull}})
		if callErr != nil {
			return SteadyPullResult{}, fmt.Errorf("run Kotlin Android steady-pull %s fault: %w", fault, callErr)
		}
		if err := faultProxy.verify(fault); err != nil {
			return SteadyPullResult{}, err
		}
		afterFault, captureErr := platform.scenarioSnapshot(ctx, client)
		if captureErr != nil {
			return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull %s fault state: %w", fault, captureErr)
		}
		if err := validateKotlinSteadyPullFaultResult(fault, failed, afterFault); err != nil {
			return SteadyPullResult{}, err
		}
		if !equalKotlinSteadyPullDurableState(pristine, afterFault) {
			return SteadyPullResult{}, fmt.Errorf("Kotlin Android steady-pull %s fault changed durable rows or cursor state", fault)
		}
		faultCalls = append(faultCalls, failed)
	}

	measured, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "retry-after-error", Operations: []scenarios.Operation{measuredPull}})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("retry Kotlin Android measured pull: %w", err)
	}
	if measured.Completion != "idle" || len(measured.Steps) != 1 || len(measured.transportObservations) != 1 || measured.transportObservations[0].StatusCode != 200 {
		return SteadyPullResult{}, errors.New("Kotlin Android measured pull did not complete successfully")
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-PERF-STEADY-PULL-001", "pull", measured); err != nil {
		return SteadyPullResult{}, err
	}
	beforeRestart, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull state before restart: %w", err)
	}
	restartOperation := scenarios.Operation{
		ContractOperation: "process",
		Name:              "restart-client",
		Payload:           queueJSON(map[string]any{"user_id": client.UserID, "client_id": client.ClientID}),
	}
	restart, err := platform.ProcessStep(ctx, client, restartOperation)
	if err != nil || restart.Disposition != "success" {
		return SteadyPullResult{}, fmt.Errorf("restart Kotlin Android steady-pull client: %w", kotlinResultError(err, restart.Disposition))
	}
	afterRestart, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull state after restart: %w", err)
	}
	if !equalKotlinSteadyPullDurableState(beforeRestart, afterRestart) {
		return SteadyPullResult{}, errors.New("Kotlin Android steady-pull restart changed durable rows or cursor state")
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance", "rebuild-state"})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Kotlin Android steady-pull client state: %w", err)
	}
	snapshot, err := decodeWarmConnectSnapshot(afterRestart)
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
	return SteadyPullResult{BaselineCall: baseline, FaultCalls: faultCalls, MeasuredCall: measured, Restart: restart, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: evidence.resolutions}, nil
}

func validateKotlinSteadyPullFaultPlans(scenario scenarios.Scenario) error {
	required := map[string]bool{
		"FPL-PERF-STEADY-PULL-CURSOR-002":    false,
		"FPL-PERF-STEADY-PULL-CURSOR-004":    false,
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
			return fmt.Errorf("Kotlin Android steady-pull fault plan %s is absent", id)
		}
	}
	return nil
}

func startKotlinSteadyPullFaultProxy(platform *Platform) (*steadyPullFaultProxy, func(), error) {
	platform.mu.Lock()
	if platform.closed {
		platform.mu.Unlock()
		return nil, nil, errors.New("Kotlin Android steady-pull platform is closed")
	}
	originalURL := platform.config.ServerURL
	upstream, err := url.Parse(originalURL)
	if err != nil || upstream.Scheme == "" || upstream.Host == "" {
		platform.mu.Unlock()
		return nil, nil, errors.New("Kotlin Android steady-pull proxy upstream is invalid")
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
		return errors.New("Kotlin Android steady-pull response fault is already active")
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
		return fmt.Errorf("Kotlin Android steady-pull %s fault did not mutate one pull response", fault)
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
		err = errors.New("Kotlin Android steady-pull pull response exceeds the fault bound")
	} else {
		body, err = mutateKotlinSteadyPullResponse(body, fault)
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

func mutateKotlinSteadyPullResponse(body []byte, fault steadyPullFault) ([]byte, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, errors.New("decode Kotlin Android steady-pull pull response failed")
	}
	var changes []map[string]json.RawMessage
	if err := json.Unmarshal(response["changes"], &changes); err != nil || len(changes) != 1 {
		return nil, errors.New("Kotlin Android steady-pull fault requires one canonical change")
	}
	switch fault {
	case steadyPullMalformedTypedRow:
		if err := mutateKotlinSteadyPullTypedRow(changes[0]); err != nil {
			return nil, err
		}
	case steadyPullRowDigest:
		if err := mutateKotlinSteadyPullChecksum(changes[0], "row_checksum"); err != nil {
			return nil, err
		}
	case steadyPullScopeDigest:
		if err := mutateKotlinSteadyPullScopeDigest(response); err != nil {
			return nil, err
		}
	case steadyPullTerminalMap:
		if err := mutateKotlinSteadyPullTerminalMap(response); err != nil {
			return nil, err
		}
	case steadyPullDuplicateEffect:
		duplicate, err := cloneKotlinSteadyPullChange(changes[0])
		if err != nil {
			return nil, err
		}
		changes = append(changes, duplicate)
	case steadyPullCursorBeforeApply:
		later, err := cloneKotlinSteadyPullChange(changes[0])
		if err != nil {
			return nil, err
		}
		if err := mutateKotlinSteadyPullPrimaryKey(later); err != nil {
			return nil, err
		}
		changes = append(changes, later)
	default:
		return nil, errors.New("Kotlin Android steady-pull fault is unsupported")
	}
	if fault != steadyPullScopeDigest && fault != steadyPullTerminalMap {
		encoded, err := json.Marshal(changes)
		if err != nil {
			return nil, errors.New("encode Kotlin Android steady-pull changes failed")
		}
		response["changes"] = encoded
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, errors.New("encode Kotlin Android steady-pull pull response failed")
	}
	return encoded, nil
}

func mutateKotlinSteadyPullTypedRow(change map[string]json.RawMessage) error {
	var row map[string]json.RawMessage
	var primary map[string]json.RawMessage
	if json.Unmarshal(change["row"], &row) != nil || len(row) == 0 || json.Unmarshal(change["pk"], &primary) != nil {
		return errors.New("Kotlin Android steady-pull typed-row fault target is invalid")
	}
	fields := make([]string, 0, len(row))
	for field := range row {
		if _, isPrimary := primary[field]; !isPrimary {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return errors.New("Kotlin Android steady-pull typed-row fault has no writable field")
	}
	sort.Strings(fields)
	row[fields[0]] = json.RawMessage("1")
	encoded, err := json.Marshal(row)
	if err != nil {
		return errors.New("encode Kotlin Android steady-pull typed-row fault failed")
	}
	change["row"] = encoded
	return nil
}

func mutateKotlinSteadyPullChecksum(object map[string]json.RawMessage, member string) error {
	var checksum map[string]json.RawMessage
	if json.Unmarshal(object[member], &checksum) != nil || len(checksum) == 0 {
		return errors.New("Kotlin Android steady-pull row-digest fault target is invalid")
	}
	checksum["digest"] = json.RawMessage(`"0000000000000000000000000000000000000000000000000000000000000000"`)
	encoded, err := json.Marshal(checksum)
	if err != nil {
		return errors.New("encode Kotlin Android steady-pull row-digest fault failed")
	}
	object[member] = encoded
	return nil
}

func mutateKotlinSteadyPullScopeDigest(response map[string]json.RawMessage) error {
	var checksums map[string]json.RawMessage
	if json.Unmarshal(response["checksums"], &checksums) != nil || len(checksums) != 1 {
		return errors.New("Kotlin Android steady-pull scope-digest fault target is invalid")
	}
	for scope, raw := range checksums {
		var checksum map[string]json.RawMessage
		if json.Unmarshal(raw, &checksum) != nil {
			return errors.New("Kotlin Android steady-pull scope digest is invalid")
		}
		checksum["algorithm"] = json.RawMessage(`"sha1"`)
		encoded, err := json.Marshal(checksum)
		if err != nil {
			return errors.New("encode Kotlin Android steady-pull scope-digest fault failed")
		}
		checksums[scope] = encoded
	}
	encoded, err := json.Marshal(checksums)
	if err != nil {
		return errors.New("encode Kotlin Android steady-pull scope checksum map failed")
	}
	response["checksums"] = encoded
	return nil
}

func mutateKotlinSteadyPullTerminalMap(response map[string]json.RawMessage) error {
	var checksums map[string]json.RawMessage
	if json.Unmarshal(response["checksums"], &checksums) != nil || len(checksums) != 1 {
		return errors.New("Kotlin Android steady-pull terminal-map fault target is invalid")
	}
	for _, checksum := range checksums {
		checksums["native-extra-scope"] = append(json.RawMessage(nil), checksum...)
		break
	}
	encoded, err := json.Marshal(checksums)
	if err != nil {
		return errors.New("encode Kotlin Android steady-pull terminal-map fault failed")
	}
	response["checksums"] = encoded
	return nil
}

func cloneKotlinSteadyPullChange(change map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	encoded, err := json.Marshal(change)
	if err != nil {
		return nil, errors.New("encode Kotlin Android steady-pull effect failed")
	}
	var clone map[string]json.RawMessage
	if json.Unmarshal(encoded, &clone) != nil {
		return nil, errors.New("clone Kotlin Android steady-pull effect failed")
	}
	return clone, nil
}

func mutateKotlinSteadyPullPrimaryKey(change map[string]json.RawMessage) error {
	var primary map[string]json.RawMessage
	var row map[string]json.RawMessage
	if json.Unmarshal(change["pk"], &primary) != nil || len(primary) != 1 || json.Unmarshal(change["row"], &row) != nil {
		return errors.New("Kotlin Android steady-pull cursor-before-apply fault target is invalid")
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

func validateKotlinSteadyPullFaultResult(fault steadyPullFault, result SynchronizationResult, snapshot Result) error {
	if result.Completion != "error" || len(result.Steps) != 1 || len(result.transportObservations) != 1 || result.transportObservations[0].OperationClass != "pull" || result.transportObservations[0].StatusCode != http.StatusOK {
		return fmt.Errorf("Kotlin Android steady-pull %s fault did not produce one failed pull call", fault)
	}
	failure := snapshot.Failure
	if failure == nil || failure.Operation != "pulling" || failure.Code != "invalid_response" || failure.Retryable || failure.RecoveryAction != "retry" {
		return fmt.Errorf("Kotlin Android steady-pull %s fault did not expose retry recovery evidence", fault)
	}
	return nil
}

func equalKotlinSteadyPullDurableState(left, right Result) bool {
	normalize := func(value Result) Result {
		value.Status = nil
		value.RowsAffected = nil
		value.Events = nil
		value.EventsOverflowed = false
		value.Failure = nil
		value.TransportMilestone = nil
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
