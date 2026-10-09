package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	schemaCheckScenarioPath = "conformance/scenarios/performance/schema-check-001.json"
	schemaCheckScenarioID   = "SCN-PERF-SCHEMA-CHECK-001"
)

var schemaCheckAliasNames = []string{
	"schema-v1",
	"schema-v2",
	"schema-v3",
	"schema-v4",
	"scope-user-a",
	"scope-user-b",
	"client-generation-one",
	"scope-set-version-one",
	"items-table",
	"items-primary-key",
}

var schemaCheckStepOrder = []scenarios.StepID{
	"STEP-PERF-SCHEMA-CHECK-001",
	"STEP-PERF-SCHEMA-CHECK-002",
	"STEP-PERF-SCHEMA-CHECK-003",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS1-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-COMMIT-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-MATERIALIZE-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-STAGE-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS1-ACTIVATE-001",
	"STEP-PERF-SCHEMA-CHECK-004",
	"STEP-PERF-SCHEMA-CHECK-005",
	"STEP-PERF-SCHEMA-CHECK-006",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS2-003",
	"STEP-PERF-SCHEMA-CHECK-PROOF-BASELINE-COMMIT-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-BASELINE-MATERIALIZE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-BOOTSTRAP-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-BOOTSTRAP-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-WRITE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-WRITE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-CONNECT-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-SEND-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-RESTART-001",
	"STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-MIGRATE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-CUT-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-RECOVER-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-PUSH-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-COMPLETE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-MIGRATE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-CUT-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-RECOVER-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M2-WRITE-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M1-REPLAY-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M2-REPLY-001",
	"STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-COMPLETE-001",
	"STEP-PERF-SCHEMA-CHECK-007",
	"STEP-PERF-SCHEMA-CHECK-008",
	"STEP-PERF-SCHEMA-CHECK-009",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-AFFECTED-003",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS3-UNAFFECTED-003",
	"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-001",
	"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-002",
	"STEP-PERF-SCHEMA-CHECK-BASELINE-CLASS4-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS3-PUBLISH-001",
	"STEP-PERF-SCHEMA-CHECK-010",
	"STEP-PERF-SCHEMA-CHECK-011",
	"STEP-PERF-SCHEMA-CHECK-012",
	"STEP-PERF-SCHEMA-CHECK-013",
	"STEP-PERF-SCHEMA-CHECK-014",
	"STEP-PERF-SCHEMA-CHECK-015",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-001",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-002",
	"STEP-PERF-SCHEMA-CHECK-PREWARM-CLASS4-003",
	"STEP-PERF-SCHEMA-CHECK-CLASS4-PUBLISH-001",
	"STEP-PERF-SCHEMA-CHECK-016",
	"STEP-PERF-SCHEMA-CHECK-017",
	"STEP-PERF-SCHEMA-CHECK-018",
}

// SchemaCheckCoordinatorConfig configures one authenticated schema-check sidecar.
type SchemaCheckCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
}

// SchemaCheckCoordinatorResult contains server evidence and resolved identities.
type SchemaCheckCoordinatorResult struct {
	ServerFacts         scenarios.StateFacts
	IdentityResolution  []blackbox.NativeIdentityResolution
	InterruptedCalls    map[string]scenarios.StepID
	CompletedProofCalls []string
}

type schemaCheckCall struct {
	step              scenarios.Step
	controllerSteps   []scenarios.Step
	clientKey         string
	sessionKey        string
	serverSchemaAlias string
	affectedScopes    []string
}

type schemaCheckWaiting uint8

const (
	schemaCheckWaitingNone schemaCheckWaiting = iota
	schemaCheckWaitingOpen
	schemaCheckWaitingBeforeCapture
	schemaCheckWaitingSync
	schemaCheckWaitingCapture
	schemaCheckWaitingLifecycle
)

// SchemaCheckCoordinator executes every authored schema dispatch call through React Native.
type SchemaCheckCoordinator struct {
	config SchemaCheckCoordinatorConfig

	listener net.Listener
	server   *http.Server
	token    string
	adapter  string

	calls                []schemaCheckCall
	boundaries           map[scenarios.StepID]scenarios.NativeLifecycleBoundary
	databases            map[string]bool
	captures             map[scenarios.StepID]finalCapture
	beforeCaptures       map[scenarios.StepID]finalCapture
	authTokens           map[string]string
	processes            map[string]actionProcessIdentity
	runtimeIDs           map[string]json.RawMessage
	tableName            string
	primaryKey           string
	proofPhase           int
	proofCommand         *conformanceCommand
	proofCaptures        map[string]finalCapture
	proofServers         map[string]scenarios.StateFacts
	proofInterrupted     map[string]scenarios.StepID
	proofCompleted       []string
	proofPaused          map[string]string
	proofHTTPBaseline    uint64
	proofPhysicalSchemas map[clientSchema][]physicalSchemaColumn
	proxyMu              sync.Mutex
	proxyHTTP            map[string]uint64
	proxyPushes          map[string][]schemaCheckPush
	proxyFailure         error
	proxyLoss            bool
	proxyLossAccepted    chan struct{}
	proxyLossRelease     chan struct{}
	proxyLossReleaseOnce sync.Once
	upstream             string

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	nextSeq   uint64
	current   int
	waiting   schemaCheckWaiting
	result    SchemaCheckCoordinatorResult
}

type schemaCheckPush struct {
	Request  []byte
	Response []byte
	Status   int
}

func schemaCheckIsProof(step scenarios.Step) bool {
	return strings.HasPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-")
}

// LoadSchemaCheckScenario loads only the authored schema-check scenario.
func LoadSchemaCheckScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, schemaCheckScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native schema-check scenario: %w", err)
	}
	if err := ValidateSchemaCheckScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidateSchemaCheckScenario rejects changes to the closed RN schema-check contract.
func ValidateSchemaCheckScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != schemaCheckScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native schema-check scenario contract is invalid")
	}
	if len(scenario.Steps) != len(schemaCheckStepOrder) {
		return fmt.Errorf("React Native schema-check steps=%d want=%d", len(scenario.Steps), len(schemaCheckStepOrder))
	}
	fixture := scenario.NativeLocalFixture
	if fixture == nil || fixture.TableName != "schema_proof_local" || fixture.ID != "sentinel" || fixture.Value != "preserve-local" {
		return errors.New("React Native schema proof local fixture differs from its create-only contract")
	}
	for index, step := range scenario.Steps {
		if step.ID != schemaCheckStepOrder[index] {
			return fmt.Errorf("React Native schema-check step index=%d id=%s want=%s", index, step.ID, schemaCheckStepOrder[index])
		}
	}
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		return err
	}
	// The authored scenario owns the call count. Derive it from the public
	// bindings rather than restating a number that can drift from the contract.
	publicSteps := 0
	for _, step := range scenario.Steps {
		if step.Transport == "http" {
			publicSteps++
		}
	}
	if len(scenario.WireExpectations) != publicSteps {
		return fmt.Errorf("React Native schema-check steps=%d calls=%d, want %d steps and %d calls",
			len(scenario.Steps), len(calls), len(schemaCheckStepOrder), publicSteps)
	}
	if err := schemaCheckLifecycleBoundaries(scenario, calls); err != nil {
		return err
	}
	if err := schemaCheckAliases(scenario.NativeIdentityAliases); err != nil {
		return err
	}
	if err := schemaCheckAssertions(scenario); err != nil {
		return err
	}
	if err := schemaCheckProofObligations(scenario); err != nil {
		return err
	}
	normalWindows := 0
	// The empty-database bootstrap precedes the 35 normal windows.
	for _, call := range calls {
		if !schemaCheckIsProof(call.step) && call.step.ID != "STEP-PERF-SCHEMA-CHECK-001" {
			normalWindows++
		}
	}
	if normalWindows != 35 {
		return fmt.Errorf("React Native normal schema windows=%d want=35", normalWindows)
	}
	plan, err := schemaCheckDispatchPlan(scenario)
	if err != nil {
		return err
	}
	strata, err := schemaCheckStrata(plan)
	if err != nil {
		return err
	}
	counts := make(map[string]uint64, len(plan.Strata))
	seenSamples := make(map[string]struct{})
	for _, call := range calls {
		step := call.step
		if schemaCheckIsProof(step) {
			if step.MeasurementSample != nil {
				return fmt.Errorf("proof step %s entered a measurement window", step.ID)
			}
			continue
		}
		wire, wireErr := schemaCheckWireExpectation(scenario, step.ID)
		if wireErr != nil {
			return wireErr
		}
		if step.NativeBinding.Completion != schemaCheckCompletion(wire) {
			return fmt.Errorf("React Native schema-check step %s completion=%q wire_action=%q status=%d", step.ID, step.NativeBinding.Completion, wire.Action, wire.HTTPStatus)
		}
		if step.MeasurementSample == nil {
			continue
		}
		sample := step.MeasurementSample
		if sample.MeasurementID != plan.MeasurementID || sample.SampleID == "" || sample.Operation.Family != "schema-check" {
			return fmt.Errorf("React Native schema-check measurement step %s is invalid", step.ID)
		}
		if _, duplicate := seenSamples[sample.SampleID]; duplicate {
			return fmt.Errorf("React Native schema-check sample %q is duplicated", sample.SampleID)
		}
		caseName, caseErr := schemaCheckCase(step)
		operationCase, operationCaseErr := schemaCheckMeasurementOperationCase(*sample)
		wantCase, found := strata[string(sample.StratumID)]
		if caseErr != nil || operationCaseErr != nil || !found || caseName != wantCase || operationCase != wantCase {
			return fmt.Errorf("React Native schema-check sample step=%s stratum=%q parameter_case=%q operation_case=%q want_case=%q parameter_error=%v operation_error=%v", step.ID, sample.StratumID, caseName, operationCase, wantCase, caseErr, operationCaseErr)
		}
		seenSamples[sample.SampleID] = struct{}{}
		counts[string(sample.StratumID)]++
	}
	for _, stratum := range plan.Strata {
		if counts[string(stratum.StratumID)] != plan.MinimumSampleCountPerStratum {
			return fmt.Errorf("React Native schema-check stratum %s samples=%d want=%d", stratum.StratumID, counts[string(stratum.StratumID)], plan.MinimumSampleCountPerStratum)
		}
	}
	return nil
}

func schemaCheckCalls(scenario scenarios.Scenario) ([]schemaCheckCall, error) {
	if len(scenario.Steps) == 0 {
		return nil, errors.New("React Native schema-check steps are absent")
	}
	calls := make([]schemaCheckCall, 0, len(scenario.WireExpectations))
	pending := make([]scenarios.Step, 0, 5)
	var setup struct {
		InitialSchema struct {
			Schema clientSchema `json:"schema"`
		} `json:"initial_schema"`
	}
	if len(scenario.Model.Setup) != 1 || json.Unmarshal(scenario.Model.Setup[0].Payload, &setup) != nil {
		return nil, errors.New("React Native schema-check initial schema is invalid")
	}
	serverSchema := ""
	for _, alias := range scenario.NativeIdentityAliases {
		var authored clientSchema
		if alias.Kind == "schema" && json.Unmarshal(alias.Value, &authored) == nil && authored == setup.InitialSchema.Schema {
			if serverSchema != "" {
				return nil, errors.New("React Native schema-check initial schema alias is ambiguous")
			}
			serverSchema = alias.Alias
		}
	}
	if serverSchema == "" {
		return nil, errors.New("React Native schema-check initial schema alias is absent")
	}
	var affectedScopes []string
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil || step.ExpectedOutcome.Disposition != "success" || scenarios.ValidateOperation(step.Operation) != nil {
			return nil, fmt.Errorf("React Native schema-check step %s is invalid", step.ID)
		}
		key := scenarios.OperationKey(step.Operation)
		if schemaCheckIsProof(step) && step.NativeBinding.Kind != "controller" {
			binding := step.NativeBinding
			lane := "committed"
			if strings.Contains(string(step.ID), "PROOF-PREPARED-") {
				lane = "prepared"
			}
			if binding.UserID != "user-a" || binding.ClientID != "client-schema-proof-"+lane {
				return nil, fmt.Errorf("React Native schema proof step %s client is invalid", step.ID)
			}
			if err := schemaCheckProofBinding(step); err != nil {
				return nil, err
			}
			calls = append(calls, schemaCheckCall{step: step, controllerSteps: append([]scenarios.Step(nil), pending...), clientKey: schemaCheckClientKey(binding.UserID, binding.ClientID), sessionKey: schemaCheckProofSession(step), serverSchemaAlias: serverSchema})
			pending = pending[:0]
			continue
		}
		switch key {
		case "connect/send":
			binding := step.NativeBinding
			if step.Transport != "http" || binding.Kind != "public-call" || binding.UserID == "" || binding.ClientID == "" ||
				binding.Stage != "synchronous" || binding.Method != "start" || binding.CallID == nil || *binding.CallID == "" {
				return nil, fmt.Errorf("React Native schema-check public step %s binding is invalid", step.ID)
			}
			var payload struct {
				UserID   string `json:"user_id"`
				ClientID string `json:"client_id"`
			}
			if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID {
				return nil, fmt.Errorf("React Native schema-check public step %s identity is invalid", step.ID)
			}
			calls = append(calls, schemaCheckCall{
				step:              step,
				controllerSteps:   append([]scenarios.Step(nil), pending...),
				clientKey:         schemaCheckClientKey(binding.UserID, binding.ClientID),
				sessionKey:        schemaCheckSessionKey(step.ID),
				serverSchemaAlias: serverSchema,
				affectedScopes:    append([]string(nil), affectedScopes...),
			})
			pending = pending[:0]
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/publish-schema":
			if step.NativeBinding.Kind != "controller" {
				return nil, fmt.Errorf("React Native schema-check controller step %s binding is invalid", step.ID)
			}
			pending = append(pending, step)
			if key == "model/publish-schema" {
				alias, err := schemaCheckPublishedSchemaAlias(scenario, step)
				if err != nil {
					return nil, err
				}
				serverSchema = alias
				var published struct {
					AffectedScopes []string `json:"affected_scopes"`
				}
				if json.Unmarshal(step.Operation.Payload, &published) != nil {
					return nil, errors.New("React Native schema-check affected scopes are invalid")
				}
				affectedScopes = published.AffectedScopes
			}
		case "process/materialize-source-transaction":
			if step.NativeBinding.Kind != "controller" {
				return nil, fmt.Errorf("React Native schema-check process step %s binding is invalid", step.ID)
			}
			pending = append(pending, step)
		default:
			return nil, fmt.Errorf("React Native schema-check step %s operation %q is unsupported", step.ID, key)
		}
	}
	if len(pending) != 0 {
		return nil, errors.New("React Native schema-check ends with unapplied controller steps")
	}
	return calls, nil
}

func schemaCheckPublishedSchemaAlias(scenario scenarios.Scenario, step scenarios.Step) (string, error) {
	var payload struct {
		Schema struct {
			Version uint64 `json:"version"`
			Hash    string `json:"hash"`
		} `json:"schema"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil {
		return "", fmt.Errorf("decode React Native schema-check published schema %s: %w", step.ID, err)
	}
	for _, alias := range scenario.NativeIdentityAliases {
		var authored clientSchema
		if alias.Kind == "schema" && json.Unmarshal(alias.Value, &authored) == nil && authored.Version == payload.Schema.Version && authored.Hash == payload.Schema.Hash {
			return alias.Alias, nil
		}
	}
	return "", fmt.Errorf("React Native schema-check published schema step %s has version=%d hash=%q", step.ID, payload.Schema.Version, payload.Schema.Hash)
}

func schemaCheckLifecycleBoundaries(scenario scenarios.Scenario, calls []schemaCheckCall) error {
	if len(scenario.NativeLifecycleBoundaries) != 20 {
		return fmt.Errorf("React Native schema-check lifecycle boundaries=%d want=20", len(scenario.NativeLifecycleBoundaries))
	}
	callByStep := make(map[scenarios.StepID]schemaCheckCall, len(calls))
	for _, call := range calls {
		callByStep[call.step.ID] = call
	}
	seen := make(map[scenarios.StepID]struct{}, len(scenario.NativeLifecycleBoundaries))
	for _, boundary := range scenario.NativeLifecycleBoundaries {
		call, found := callByStep[boundary.AfterStepID]
		if !found || boundary.ID == "" || boundary.Phase != "setup" || boundary.Method != "stop" ||
			boundary.UserID != call.step.NativeBinding.UserID || boundary.ClientID != call.step.NativeBinding.ClientID {
			return fmt.Errorf("React Native schema-check lifecycle boundary %q is invalid", boundary.ID)
		}
		if _, duplicate := seen[boundary.AfterStepID]; duplicate {
			return fmt.Errorf("React Native schema-check lifecycle step %s is duplicated", boundary.AfterStepID)
		}
		seen[boundary.AfterStepID] = struct{}{}
	}
	return nil
}

func schemaCheckAliases(aliases []scenarios.NativeIdentityAlias) error {
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if alias.Alias == "" {
			return errors.New("React Native schema-check alias is empty")
		}
		if _, duplicate := seen[alias.Alias]; duplicate {
			return fmt.Errorf("React Native schema-check alias %q is duplicated", alias.Alias)
		}
		seen[alias.Alias] = struct{}{}
	}
	for _, name := range schemaCheckAliasNames {
		if _, found := seen[name]; !found {
			return fmt.Errorf("React Native schema-check alias %q is absent", name)
		}
	}
	for _, name := range []string{"proof-prepared-row", "proof-committed-row"} {
		if _, found := seen[name]; !found {
			return fmt.Errorf("React Native schema proof alias %q is absent", name)
		}
	}
	return nil
}

func schemaCheckAssertions(scenario scenarios.Scenario) error {
	semantic, dispatch, performance := false, false, false
	for _, assertion := range scenario.Assertions {
		switch string(assertion.ID) {
		case "ASSERT-PERF-SCHEMA-CHECK-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-SCHEMA-CHECK-DISPATCH-001":
			dispatch = assertion.Predicate.ContractPredicate == "state-transition" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-SCHEMA-CHECK-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
	}
	if !semantic || !dispatch || !performance {
		return errors.New("React Native schema-check assertions are invalid")
	}
	return nil
}

func schemaCheckProofObligations(scenario scenarios.Scenario) error {
	matches := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-SCHEMA-CHECK-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") {
				matches[id]++
			}
		case "OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") {
				matches[id]++
			}
		case "OBL-PERF-SCHEMA-CHECK-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-SCHEMA-CHECK-001", "CTRL-SCHEMA-004") {
				matches[id]++
			}
		}
	}
	if matches["OBL-PERF-SCHEMA-CHECK-RN-IOS-CURRENT-001"] != 1 ||
		matches["OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001"] != 1 ||
		matches["OBL-PERF-SCHEMA-CHECK-CONTROL-001"] != 1 {
		return fmt.Errorf("React Native schema-check proof obligations=%v", matches)
	}
	return nil
}

func schemaCheckDispatchPlan(scenario scenarios.Scenario) (scenarios.SchemaDispatchMeasurementPlan, error) {
	for _, expected := range scenario.Model.ExpectedState {
		if expected.ID != "EXPECT-PERF-SCHEMA-CHECK-DISPATCH-001" {
			continue
		}
		var plan scenarios.SchemaDispatchMeasurementPlan
		if expected.Predicate.ContractPredicate != "state-transition" || expected.Predicate.Name != "schema-dispatch-observations-satisfied" ||
			json.Unmarshal(expected.Predicate.Payload, &plan) != nil || plan.MeasurementID != "MEAS-SCHEMA-CHECK-001" ||
			plan.MinimumSampleCountPerStratum == 0 || len(plan.Strata) != 6 {
			return scenarios.SchemaDispatchMeasurementPlan{}, errors.New("React Native schema-check dispatch plan is invalid")
		}
		return plan, nil
	}
	return scenarios.SchemaDispatchMeasurementPlan{}, errors.New("React Native schema-check dispatch plan is absent")
}

func schemaCheckStrata(plan scenarios.SchemaDispatchMeasurementPlan) (map[string]string, error) {
	strata := make(map[string]string, len(plan.Strata))
	for _, stratum := range plan.Strata {
		id := string(stratum.StratumID)
		if id == "" || stratum.SchemaCase == "" {
			return nil, fmt.Errorf("React Native schema-check stratum id=%q case=%q", id, stratum.SchemaCase)
		}
		if _, duplicate := strata[id]; duplicate {
			return nil, fmt.Errorf("React Native schema-check stratum %q is duplicated", id)
		}
		strata[id] = stratum.SchemaCase
	}
	return strata, nil
}

func schemaCheckWireExpectation(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.WireExpectation, error) {
	var result scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID == id {
			result = wire
			count++
		}
	}
	if count != 1 || result.ContractCase != "connect_success" || result.HTTPStatus != http.StatusOK || result.Retryable || result.ErrorCode != nil {
		return scenarios.WireExpectation{}, fmt.Errorf("React Native schema-check wire expectation %s count=%d", id, count)
	}
	return result, nil
}

func schemaCheckCompletion(wire scenarios.WireExpectation) string {
	if wire.Action == "unsupported" {
		return "error"
	}
	if wire.HTTPStatus >= http.StatusOK && wire.HTTPStatus < http.StatusMultipleChoices {
		return "idle"
	}
	if wire.Retryable || wire.HTTPStatus == 0 {
		return "blocked"
	}
	return "error"
}

// NewSchemaCheckCoordinator creates an authenticated loopback coordinator.
func NewSchemaCheckCoordinator(config SchemaCheckCoordinatorConfig) (*SchemaCheckCoordinator, error) {
	if err := ValidateSchemaCheckScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, fmt.Errorf("React Native schema-check platform=%q is invalid", config.Platform)
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native schema-check auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	adapter, err := nativeAdapterURL(serverURL, config.Platform)
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native schema-check capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native schema-check coordinator")
	}
	calls, err := schemaCheckCalls(config.Scenario)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	boundaries := make(map[scenarios.StepID]scenarios.NativeLifecycleBoundary, len(config.Scenario.NativeLifecycleBoundaries))
	for _, boundary := range config.Scenario.NativeLifecycleBoundaries {
		boundaries[boundary.AfterStepID] = boundary
	}
	coordinator := &SchemaCheckCoordinator{
		config: config, listener: listener, token: token, adapter: adapter, calls: calls, boundaries: boundaries,
		databases: make(map[string]bool), captures: make(map[scenarios.StepID]finalCapture), authTokens: make(map[string]string),
		processes: make(map[string]actionProcessIdentity), runtimeIDs: make(map[string]json.RawMessage), nextSeq: 1,
		beforeCaptures: make(map[scenarios.StepID]finalCapture),
		proofCaptures:  make(map[string]finalCapture), proofServers: make(map[string]scenarios.StateFacts), proofInterrupted: make(map[string]scenarios.StepID), proofPaused: make(map[string]string),
		proofPhysicalSchemas: make(map[clientSchema][]physicalSchemaColumn),
		proxyHTTP:            make(map[string]uint64), proxyPushes: make(map[string][]schemaCheckPush), upstream: serverURL,
		proxyLossAccepted: make(chan struct{}), proxyLossRelease: make(chan struct{}),
	}
	coordinator.adapter, err = nativeAdapterURL(coordinator.URL(), config.Platform)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	coordinator.server = &http.Server{
		Handler: coordinator, MaxHeaderBytes: 16 * 1024, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second,
	}
	return coordinator, nil
}

// Prepare installs the authored contract and mints one token for each client key.
func (c *SchemaCheckCoordinator) Prepare(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCoordinatorUnavailable
	}
	if c.prepared {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native schema-check dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native schema-check contract: %w", err)
	}
	for _, call := range c.calls {
		if _, found := c.authTokens[call.clientKey]; found {
			continue
		}
		if c.config.AuthToken != "" {
			c.authTokens[call.clientKey] = c.config.AuthToken
			continue
		}
		token, err := c.config.Harness.NativeBearerToken(ctx, call.step.NativeBinding.UserID, time.Now())
		if err != nil {
			return fmt.Errorf("mint React Native schema-check bearer token for %q: %w", call.clientKey, err)
		}
		c.authTokens[call.clientKey] = token
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	return nil
}

// Serve runs the sidecar until it closes.
func (c *SchemaCheckCoordinator) Serve(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	if err := c.Prepare(ctx); err != nil {
		return err
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(closeContext)
			cancel()
		case <-stop:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Handler returns the authenticated exchange handler.
func (c *SchemaCheckCoordinator) Handler() http.Handler { return c }

// URL returns the host-loopback sidecar URL.
func (c *SchemaCheckCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

// Token returns the exchange capability.
func (c *SchemaCheckCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

// ExchangeCount returns all commands plus the terminal exchange.
func (c *SchemaCheckCoordinator) ExchangeCount() int {
	if c == nil {
		return 0
	}
	count := 1
	for _, call := range c.calls {
		if schemaCheckIsProof(call.step) {
			count += schemaCheckProofCommandCount(call.step)
		} else {
			count += 4
			if _, stop := c.boundaries[call.step.ID]; stop {
				count++
			}
		}
	}
	return count
}

// Completed reports whether every authored call passed final validation.
func (c *SchemaCheckCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

// Result returns the verified server projection and native identity resolution.
func (c *SchemaCheckCoordinator) Result() (SchemaCheckCoordinatorResult, error) {
	if c == nil {
		return SchemaCheckCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return SchemaCheckCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return SchemaCheckCoordinatorResult{}, errors.New("React Native schema-check coordinator has not completed")
	}
	return c.result, nil
}

// Close stops the sidecar without closing the controller.
func (c *SchemaCheckCoordinator) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.proxyLossReleaseOnce.Do(func() { close(c.proxyLossRelease) })
	shutdownErr, listenerErr := c.server.Shutdown(ctx), c.listener.Close()
	if shutdownErr != nil {
		return shutdownErr
	}
	if listenerErr != nil && !errors.Is(listenerErr, net.ErrClosed) {
		return listenerErr
	}
	return nil
}

func (c *SchemaCheckCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if strings.HasPrefix(request.URL.Path, "/sync/") {
		c.proxyAdapter(writer, request)
		return
	}
	if request.URL.Path != "/exchange" {
		writeExchangeError(writer, http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writeExchangeError(writer, http.StatusMethodNotAllowed)
		return
	}
	if !validBearer(request.Header.Get("Authorization"), c.token) {
		writeExchangeError(writer, http.StatusUnauthorized)
		return
	}
	if request.Header.Get("Content-Type") != "application/json" || request.ContentLength > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusRequestEntityTooLarge)
		return
	}
	exchange, err := decodeExchangeRequest(body)
	if err != nil {
		writeExchangeError(writer, http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.prepared || c.failed != nil || c.completed || exchange.Sequence != c.nextSeq {
		c.failed = fmt.Errorf("React Native schema-check exchange closed=%t prepared=%t completed=%t sequence=%d want=%d", c.closed, c.prepared, c.completed, exchange.Sequence, c.nextSeq)
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native schema-check exchange=%d call=%d waiting=%d: %w", exchange.Sequence, c.current, c.waiting, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(request.Context(), exchange.Sequence)
	if err != nil {
		c.failed = fmt.Errorf("React Native schema-check exchange=%d call=%d waiting=%d: %w", exchange.Sequence, c.current, c.waiting, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = fmt.Errorf("React Native schema-check response bytes=%d error=%v", len(encoded), err)
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *SchemaCheckCoordinator) acceptLocked(raw json.RawMessage) error {
	if c.waiting == schemaCheckWaitingNone {
		if !isJSONNull(raw) {
			return fmt.Errorf("initial result=%s want=null", boundedRaw(raw))
		}
		return nil
	}
	if c.current >= len(c.calls) {
		return errors.New("React Native schema-check received a result after all calls")
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return fmt.Errorf("command outcome=%q error_code=%v error=%v", envelope.Outcome, envelope.ErrorCode, err)
	}
	call := c.calls[c.current]
	if schemaCheckIsProof(call.step) {
		return c.acceptProofLocked(call, envelope.Result)
	}
	switch c.waiting {
	case schemaCheckWaitingOpen:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return fmt.Errorf("open step %s: %w", call.step.ID, err)
		}
		c.processes[call.sessionKey] = process
	case schemaCheckWaitingSync:
		if err := c.validateSynchronized(call, envelope.Result); err != nil {
			return err
		}
	case schemaCheckWaitingBeforeCapture, schemaCheckWaitingCapture:
		capture, err := c.validateCapture(call, envelope.Result)
		if err != nil {
			return err
		}
		if c.waiting == schemaCheckWaitingBeforeCapture {
			c.beforeCaptures[call.step.ID] = capture
		} else {
			c.captures[call.step.ID] = capture
		}
	case schemaCheckWaitingLifecycle:
		process, found := c.processes[call.sessionKey]
		if !found {
			return fmt.Errorf("React Native schema-check lifecycle process for %s is absent", call.step.ID)
		}
		if err := validateStoppedLifecycleResult(envelope.Result, process); err != nil {
			return fmt.Errorf("lifecycle step %s: %w", call.step.ID, err)
		}
	default:
		return errInvalidExchange
	}
	return nil
}

func (c *SchemaCheckCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	if c.current < len(c.calls) && schemaCheckIsProof(c.calls[c.current].step) {
		return c.advanceProofLocked(ctx, sequence)
	}
	if c.waiting == schemaCheckWaitingCapture {
		if _, stop := c.boundaries[c.calls[c.current].step.ID]; stop {
			c.waiting = schemaCheckWaitingLifecycle
			response.Command = c.command(c.calls[c.current], "client", "lifecycle", map[string]any{
				"client_key": c.calls[c.current].sessionKey, "operation": "stop",
			}, nil)
			return response, nil
		}
		c.current++
		c.waiting = schemaCheckWaitingNone
	}
	if c.waiting == schemaCheckWaitingLifecycle {
		c.current++
		c.waiting = schemaCheckWaitingNone
	}
	if c.current == len(c.calls) {
		if err := c.finishLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		c.completed = true
		response.State = "complete"
		return response, nil
	}
	call := c.calls[c.current]
	if schemaCheckIsProof(call.step) {
		return c.advanceProofLocked(ctx, sequence)
	}
	switch c.waiting {
	case schemaCheckWaitingNone:
		if err := c.applyControllerSteps(ctx, call.controllerSteps); err != nil {
			return exchangeResponse{}, err
		}
		mode := "create"
		initialization := "empty"
		if c.databases[call.clientKey] {
			mode = "reuse"
			if call.step.NativeBinding.Initialization != "" {
				return exchangeResponse{}, fmt.Errorf("React Native schema-check step %s repeats client initialization", call.step.ID)
			}
		} else if call.step.NativeBinding.Initialization != "" {
			initialization = call.step.NativeBinding.Initialization
		}
		c.databases[call.clientKey] = true
		c.waiting = schemaCheckWaitingOpen
		response.Command = c.command(call, "client", "open", map[string]any{
			"client_key": call.sessionKey, "database_mode": mode, "initialization": initialization, "seed_step_id": nil,
		}, nil)
	case schemaCheckWaitingOpen, schemaCheckWaitingSync:
		if c.waiting == schemaCheckWaitingOpen {
			c.waiting = schemaCheckWaitingBeforeCapture
		} else {
			c.waiting = schemaCheckWaitingCapture
		}
		// The authored scenario has no record identity for a durable-proof capture.
		response.Command = c.command(call, "observer", "capture", map[string]any{
			"client_keys": []string{call.sessionKey},
			"sources":     []string{"scope-state", "sync-status", "sync-events", "request-trace"},
		}, nil)
	case schemaCheckWaitingBeforeCapture:
		c.waiting = schemaCheckWaitingSync
		response.Command = c.command(call, "client", "synchronize-step", map[string]any{
			"client_key": call.sessionKey, "method": call.step.NativeBinding.Method, "completion": call.step.NativeBinding.Completion,
		}, []scenarios.StepID{call.step.ID})
	default:
		return exchangeResponse{}, errInvalidExchange
	}
	return response, nil
}

func (c *SchemaCheckCoordinator) applyControllerSteps(ctx context.Context, steps []scenarios.Step) error {
	for _, step := range steps {
		var (
			result blackbox.NativeStepObservation
			err    error
		)
		switch scenarios.OperationKey(step.Operation) {
		case "process/materialize-source-transaction":
			result, err = c.config.Controller.ProcessStep(ctx, nil, step.Operation)
		case "model/commit-source-transaction", "model/stage-registry-membership-generation", "model/activate-registry-membership-generation", "model/publish-schema":
			result, err = c.config.Controller.ApplyStep(ctx, step.Operation)
		default:
			return fmt.Errorf("React Native schema-check controller step %s operation=%q", step.ID, scenarios.OperationKey(step.Operation))
		}
		if err != nil || result.Disposition != step.ExpectedOutcome.Disposition {
			return fmt.Errorf("React Native schema-check controller step %s disposition=%q want=%q error=%v", step.ID, result.Disposition, step.ExpectedOutcome.Disposition, err)
		}
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateSynchronized(call schemaCheckCall, raw json.RawMessage) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return fmt.Errorf("React Native schema-check synchronized step %s: %w", call.step.ID, err)
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "schema-check synchronized result"); err != nil {
		return err
	}
	var completion string
	if err := json.Unmarshal(members["completion"], &completion); err != nil || completion != call.step.NativeBinding.Completion {
		return fmt.Errorf("React Native schema-check step %s completion=%q want=%q decode_error=%v", call.step.ID, completion, call.step.NativeBinding.Completion, err)
	}
	wantProcess, found := c.processes[call.sessionKey]
	if !found {
		return fmt.Errorf("React Native schema-check synchronization process for %s is absent", call.step.ID)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != wantProcess {
		return fmt.Errorf("React Native schema-check step %s process=%+v want=%+v error=%v", call.step.ID, process, wantProcess, err)
	}
	if call.step.NativeBinding.Completion == "idle" {
		if err := validateReadyStatus(members["status"]); err != nil {
			return fmt.Errorf("React Native schema-check step %s idle status: %w", call.step.ID, err)
		}
		return nil
	}
	var status syncStatus
	if err := json.Unmarshal(members["status"], &status); err != nil || status.State != "error" || !isJSONNull(status.RetryAt) || !hasJSONValue(status.Failure) {
		return fmt.Errorf("React Native schema-check step %s error status=%s decode_error=%v", call.step.ID, boundedRaw(members["status"]), err)
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateCapture(call schemaCheckCall, raw json.RawMessage) (finalCapture, error) {
	capture, err := decodeCapture(raw, []string{"client_state", "sync_status", "sync_events", "request_trace"})
	if err != nil {
		return finalCapture{}, err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "schema-check capture result"); err != nil {
		return finalCapture{}, err
	}
	wantProcess, found := c.processes[call.sessionKey]
	if !found {
		return finalCapture{}, fmt.Errorf("React Native schema-check capture process for %s is absent", call.step.ID)
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil || process != wantProcess {
		return finalCapture{}, fmt.Errorf("React Native schema-check step %s capture process=%+v want=%+v error=%v", call.step.ID, process, wantProcess, err)
	}
	if c.waiting == schemaCheckWaitingBeforeCapture {
		var state inspectedClientState
		if json.Unmarshal(capture.ClientState, &state) != nil {
			return finalCapture{}, errors.New("React Native schema-check before capture is invalid")
		}
	} else {
		if _, err := decodeClientState(capture.ClientState); err != nil {
			return finalCapture{}, fmt.Errorf("React Native schema-check step %s client state: %w", call.step.ID, err)
		}
	}
	if _, err := captureTraceFromRaw(capture.Trace); err != nil {
		return finalCapture{}, fmt.Errorf("React Native schema-check step %s trace: %w", call.step.ID, err)
	}
	return capture, nil
}

func (c *SchemaCheckCoordinator) command(call schemaCheckCall, actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		if id != call.step.ID {
			continue
		}
		steps = append(steps, conformanceStep{Operation: conformanceOperation{
			ContractOperation: call.step.Operation.ContractOperation,
			Name:              call.step.Operation.Name,
			Payload:           copyRaw(call.step.Operation.Payload),
		}})
	}
	return &conformanceCommand{
		SchemaVersion: 1,
		Action: conformanceManifest{
			Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters},
			Steps:  steps,
		},
		Runtime: conformanceRuntime{
			ClientKey: call.sessionKey, Database: schemaCheckDatabase(call.step.NativeBinding.ClientID), ClientID: call.step.NativeBinding.ClientID,
			ServerURL: c.adapter, AuthToken: c.authTokens[call.clientKey],
		},
	}
}

func (c *SchemaCheckCoordinator) finishLocked(ctx context.Context) error {
	normal := 0
	for _, call := range c.calls {
		if !schemaCheckIsProof(call.step) {
			normal++
		}
	}
	if len(c.captures) != normal || len(c.beforeCaptures) != normal {
		return fmt.Errorf("React Native schema-check captures=%d want=%d", len(c.captures), len(c.calls))
	}
	server, err := c.captureServer(ctx)
	if err != nil {
		return err
	}
	if err := c.bindServerIdentities(); err != nil {
		return err
	}
	plan, err := schemaCheckDispatchPlan(c.config.Scenario)
	if err != nil {
		return err
	}
	counts := make(map[string]uint64, len(plan.Strata))
	for _, call := range c.calls {
		if schemaCheckIsProof(call.step) {
			continue
		}
		capture, found := c.captures[call.step.ID]
		if !found {
			return fmt.Errorf("React Native schema-check capture for %s is absent", call.step.ID)
		}
		if err := c.validateCallEvidence(call, capture); err != nil {
			return err
		}
		if call.step.MeasurementSample != nil {
			counts[string(call.step.MeasurementSample.StratumID)]++
		}
	}
	for _, stratum := range plan.Strata {
		if counts[string(stratum.StratumID)] != plan.MinimumSampleCountPerStratum {
			return fmt.Errorf("React Native schema-check executed stratum %s samples=%d want=%d", stratum.StratumID, counts[string(stratum.StratumID)], plan.MinimumSampleCountPerStratum)
		}
	}
	resolutions, err := c.resolveIdentities()
	if err != nil {
		return err
	}
	if len(c.proofInterrupted) != 3 || len(c.proofCompleted) != 4 {
		return errors.New("React Native schema proof call closure is incomplete")
	}
	c.result = SchemaCheckCoordinatorResult{ServerFacts: server, IdentityResolution: resolutions, InterruptedCalls: c.proofInterrupted, CompletedProofCalls: append([]string(nil), c.proofCompleted...)}
	return nil
}

func (c *SchemaCheckCoordinator) captureServer(ctx context.Context) (scenarios.StateFacts, error) {
	clients := c.uniqueClients()
	keys := make([]string, 0, len(clients))
	for _, call := range clients {
		key := call.clientKey
		keys = append(keys, key)
	}
	sort.Strings(keys)
	captures, err := c.config.Controller.Capture(ctx, keys, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("capture React Native schema-check server state captures=%d error=%v", len(captures), err)
	}
	return captures[0].StateFacts, nil
}

func (c *SchemaCheckCoordinator) bindServerIdentities() error {
	serverAliases := make([]scenarios.NativeIdentityAlias, 0, len(c.config.Scenario.NativeIdentityAliases))
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		switch alias.Kind {
		case "schema", "scope", "table", "primary-key":
			serverAliases = append(serverAliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(serverAliases)
	if err != nil {
		return fmt.Errorf("resolve React Native schema-check server identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		switch value.Alias {
		case "items-table":
			c.tableName = value.ApplicationIdentifier
		case "items-primary-key":
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" || c.primaryKey == "" {
		return fmt.Errorf("React Native schema-check server application identities observed table=%q primary_key=%q want=nonempty table and primary key", c.tableName, c.primaryKey)
	}
	generation, scopeSetVersion, err := c.observedClientIdentities()
	if err != nil {
		return err
	}
	for alias, value := range map[string]uint64{
		"client-generation-one": generation,
		"scope-set-version-one": scopeSetVersion,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode React Native schema-check server identity %q: %w", alias, err)
		}
		c.runtimeIDs[alias] = encoded
	}
	for _, call := range c.calls {
		if !schemaCheckIsProof(call.step) {
			continue
		}
		if scenarios.OperationKey(call.step.Operation) == "push/submit" {
			var authored struct {
				Request schemaProofPushRequest `json:"request"`
			}
			if json.Unmarshal(call.step.Operation.Payload, &authored) != nil {
				return errors.New("schema proof authored push identity is invalid")
			}
			index := 0
			if strings.Contains(string(call.step.ID), "M1-REPLAY") {
				index = 1
			}
			if strings.Contains(string(call.step.ID), "M2-REPLY") {
				index = 2
			}
			c.proxyMu.Lock()
			pushes := c.proxyPushes[call.step.NativeBinding.ClientID]
			if len(pushes) <= index {
				c.proxyMu.Unlock()
				return errors.New("schema proof actual push identity is absent")
			}
			push := pushes[index]
			c.proxyMu.Unlock()
			var actual schemaProofPushRequest
			if json.Unmarshal(push.Request, &actual) != nil || len(actual.Mutations) != len(authored.Request.Mutations) {
				return errors.New("schema proof actual push identities differ from the authored request")
			}
			for _, alias := range c.config.Scenario.NativeIdentityAliases {
				var value string
				if json.Unmarshal(alias.Value, &value) != nil {
					continue
				}
				if alias.Kind == "batch-id" && value == authored.Request.BatchID {
					c.runtimeIDs[alias.Alias], _ = json.Marshal(actual.BatchID)
				}
				if alias.Kind == "mutation-id" {
					for ordinal, mutationRaw := range authored.Request.Mutations {
						var mutation, observed schemaProofWireMutation
						_ = json.Unmarshal(mutationRaw, &mutation)
						_ = json.Unmarshal(actual.Mutations[ordinal], &observed)
						if value == mutation.MutationID {
							if prior, exists := c.runtimeIDs[alias.Alias]; exists {
								var identity string
								_ = json.Unmarshal(prior, &identity)
								if identity != observed.MutationID {
									return errors.New("schema proof runtime mutation identity changed")
								}
							}
							c.runtimeIDs[alias.Alias], _ = json.Marshal(observed.MutationID)
						}
					}
				}
			}
		}
		if scenarios.OperationKey(call.step.Operation) == "local/write" && !strings.Contains(string(call.step.ID), "M2-") {
			var write struct {
				BaseVersion string `json:"base_version"`
			}
			if json.Unmarshal(call.step.Operation.Payload, &write) != nil {
				return errors.New("schema proof authored local base is invalid")
			}
			lane := "COMMITTED"
			if call.step.NativeBinding.ClientID == "client-schema-proof-prepared" {
				lane = "PREPARED"
			}
			var proof durableProof
			if json.Unmarshal(c.proofCaptures[lane+"-S1-001"].DurableProof, &proof) != nil || proof.RowMetadata == nil {
				return errors.New("schema proof baseline version capture is absent")
			}
			for _, alias := range c.config.Scenario.NativeIdentityAliases {
				var value string
				if (alias.Kind == "row-version" || alias.Kind == "server-version") && json.Unmarshal(alias.Value, &value) == nil && value == write.BaseVersion {
					c.runtimeIDs[alias.Alias], _ = json.Marshal(proof.RowMetadata.ServerVersion)
				}
			}
		}
	}
	return nil
}

type schemaCheckClientIdentity struct {
	generation      uint64
	scopeSetVersion uint64
}

func (c *SchemaCheckCoordinator) observedClientIdentities() (uint64, uint64, error) {
	clients := c.uniqueClients()
	if len(clients) == 0 {
		return 0, 0, errors.New("React Native schema-check identity clients=0 want=positive")
	}
	observed := make(map[string]schemaCheckClientIdentity, len(clients))
	for _, call := range c.calls {
		if schemaCheckIsProof(call.step) {
			continue
		}
		capture, found := c.captures[call.step.ID]
		if !found {
			return 0, 0, fmt.Errorf("React Native schema-check identity capture step=%s observed=absent want=present", call.step.ID)
		}
		trace, err := captureTraceFromRaw(capture.Trace)
		if err != nil {
			return 0, 0, fmt.Errorf("React Native schema-check identity trace step=%s observed=invalid want=valid error=%v", call.step.ID, err)
		}
		before, found := c.beforeCaptures[call.step.ID]
		if !found {
			return 0, 0, fmt.Errorf("React Native schema-check identity before capture step=%s is absent", call.step.ID)
		}
		beforeTrace, err := captureTraceFromRaw(before.Trace)
		if err != nil {
			return 0, 0, err
		}
		trace, err = schemaCheckTraceWindow(beforeTrace, trace)
		if err != nil {
			return 0, 0, err
		}
		for _, observation := range trace.Observations {
			if observation.OperationClass != "pull" {
				continue
			}
			generation, generationErr := requestInteger(observation, "client_generation")
			scopeSetVersion, scopeSetErr := requestInteger(observation, "scope_set_version")
			if generationErr != nil || scopeSetErr != nil {
				return 0, 0, fmt.Errorf("React Native schema-check client=%q identity request=%s observed generation_error=%v scope_set_error=%v want=valid client_generation and scope_set_version", call.clientKey, boundedRaw(observation.RequestFacts), generationErr, scopeSetErr)
			}
			observed[call.clientKey] = schemaCheckClientIdentity{generation: generation, scopeSetVersion: scopeSetVersion}
		}
	}
	for _, call := range c.calls {
		if !schemaCheckIsProof(call.step) || !strings.HasSuffix(string(call.step.ID), "COMPLETE-001") {
			continue
		}
		trace, err := captureTraceFromRaw(c.proofCaptures[schemaCheckProofCaptureName(call.step)].Trace)
		if err != nil {
			return 0, 0, err
		}
		for _, observation := range trace.Observations {
			if observation.OperationClass != "pull" {
				continue
			}
			generation, generationErr := requestInteger(observation, "client_generation")
			scopeSetVersion, scopeSetErr := requestInteger(observation, "scope_set_version")
			if generationErr != nil || scopeSetErr != nil {
				return 0, 0, errors.New("schema proof final pull identity is incomplete")
			}
			observed[call.clientKey] = schemaCheckClientIdentity{generation: generation, scopeSetVersion: scopeSetVersion}
		}
	}
	if len(observed) != len(clients) {
		return 0, 0, fmt.Errorf("React Native schema-check observed identity clients=%d want=%d", len(observed), len(clients))
	}
	var expected schemaCheckClientIdentity
	for _, call := range clients {
		identity := observed[call.clientKey]
		if expected.generation == 0 && expected.scopeSetVersion == 0 {
			expected = identity
		}
		if identity.generation == 0 || identity.scopeSetVersion == 0 || identity != expected {
			return 0, 0, fmt.Errorf("React Native schema-check client=%q observed client_generation=%d scope_set_version=%d want positive shared client_generation=%d scope_set_version=%d", call.clientKey, identity.generation, identity.scopeSetVersion, expected.generation, expected.scopeSetVersion)
		}
	}
	return expected.generation, expected.scopeSetVersion, nil
}

func (c *SchemaCheckCoordinator) validateCallEvidence(call schemaCheckCall, capture finalCapture) error {
	wire, err := schemaCheckWireExpectation(c.config.Scenario, call.step.ID)
	if err != nil {
		return err
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return fmt.Errorf("React Native schema-check step %s client state: %w", call.step.ID, err)
	}
	wantSchema := call.serverSchemaAlias
	if wire.Action == "unsupported" {
		inputAlias, err := c.stepSchemaAlias(call.step)
		if err != nil {
			return err
		}
		wantSchema = inputAlias
	}
	runtimeSchema, err := c.runtimeSchema(wantSchema)
	if err != nil || state.Schema == nil || *state.Schema != runtimeSchema {
		return fmt.Errorf("React Native schema-check step %s client schema=%+v want_alias=%q want=%+v error=%v", call.step.ID, state.Schema, wantSchema, runtimeSchema, err)
	}
	scope, err := c.stepRuntimeScope(call.step)
	if err != nil || len(state.ScopeStates) != 1 || state.ScopeStates[0].ScopeID != scope {
		return fmt.Errorf("React Native schema-check step %s scopes=%+v want=%q error=%v", call.step.ID, state.ScopeStates, scope, err)
	}
	if call.step.NativeBinding.Completion == "idle" {
		if err := validateReadyStatus(capture.Status); err != nil {
			return fmt.Errorf("React Native schema-check step %s capture status: %w", call.step.ID, err)
		}
	} else {
		var status syncStatus
		if err := json.Unmarshal(capture.Status, &status); err != nil || status.State != "error" || !isJSONNull(status.RetryAt) || !hasJSONValue(status.Failure) {
			return fmt.Errorf("React Native schema-check step %s capture error status=%s decode_error=%v", call.step.ID, boundedRaw(capture.Status), err)
		}
		if wire.Action == "unsupported" {
			var failure struct {
				Operation      string `json:"operation"`
				Code           string `json:"code"`
				Retryable      bool   `json:"retryable"`
				RecoveryAction string `json:"recovery_action"`
			}
			if err := json.Unmarshal(status.Failure, &failure); err != nil || failure.Operation != "schema" || failure.Code != "unsupported_schema" || failure.Retryable || failure.RecoveryAction != "schema_reset" {
				return fmt.Errorf("React Native schema-check step %s did not persist unsupported_schema with schema_reset", call.step.ID)
			}
		}
	}
	target, err := c.runtimeSchema(call.serverSchemaAlias)
	if err != nil {
		return err
	}
	before, found := c.beforeCaptures[call.step.ID]
	if !found {
		return fmt.Errorf("React Native schema-check step %s before-window capture is absent", call.step.ID)
	}
	affected := false
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if alias.Kind != "scope" {
			continue
		}
		var runtime, authored string
		if json.Unmarshal(c.runtimeIDs[alias.Alias], &runtime) == nil && runtime == scope && json.Unmarshal(alias.Value, &authored) == nil {
			affected = wire.Action == "rebuild_local" && slices.Contains(call.affectedScopes, authored)
		}
	}
	if err := validateSchemaCheckDispatch(call, before, capture, wire.Action, target, scope, affected); err != nil {
		return fmt.Errorf("React Native schema-check step %s: %w", call.step.ID, err)
	}
	if wire.HTTPStatus != http.StatusOK || wire.ErrorCode != nil || wire.Retryable {
		return fmt.Errorf("React Native schema-check step %s wire status=%d code=%v retryable=%t", call.step.ID, wire.HTTPStatus, wire.ErrorCode, wire.Retryable)
	}
	return nil
}

func (c *SchemaCheckCoordinator) resolveIdentities() ([]blackbox.NativeIdentityResolution, error) {
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		value := c.runtimeIDs[alias.Alias]
		if len(value) == 0 {
			return nil, fmt.Errorf("React Native schema-check server alias %q is absent", alias.Alias)
		}
		for _, stepID := range alias.StepIDs {
			owner := stepID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &owner, RuntimeValue: value})
		}
		for _, expectationID := range alias.ExpectationIDs {
			owner := expectationID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &owner, RuntimeValue: value})
		}
	}
	return blackbox.ResolveNativeIdentityAliases(c.config.Scenario.NativeIdentityAliases, observations)
}

func (c *SchemaCheckCoordinator) stepSchemaAlias(step scenarios.Step) (string, error) {
	var payload struct {
		Schema clientSchema `json:"schema"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil {
		return "", fmt.Errorf("React Native schema-check step %s input schema is invalid", step.ID)
	}
	if payload.Schema.Version == 0 && payload.Schema.Hash == "" {
		return "", nil
	}
	if payload.Schema.Version == 0 || payload.Schema.Hash == "" {
		return "", fmt.Errorf("React Native schema-check step %s input schema is invalid", step.ID)
	}
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if alias.Kind != "schema" {
			continue
		}
		var authored clientSchema
		if json.Unmarshal(alias.Value, &authored) == nil && authored == payload.Schema {
			return alias.Alias, nil
		}
	}
	return "", fmt.Errorf("React Native schema-check step %s schema=%+v has no declared alias", step.ID, payload.Schema)
}

func (c *SchemaCheckCoordinator) stepRuntimeScope(step scenarios.Step) (string, error) {
	var payload struct {
		KnownScopes []struct {
			ScopeID string `json:"scope_id"`
		} `json:"known_scopes"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || len(payload.KnownScopes) > 1 {
		return "", fmt.Errorf("React Native schema-check step %s known scopes are invalid", step.ID)
	}
	if len(payload.KnownScopes) == 0 {
		if step.NativeBinding == nil {
			return "", errors.New("React Native schema-check client scope binding is absent")
		}
		scopes := make(map[string]bool)
		for _, candidate := range c.config.Scenario.Steps {
			if candidate.NativeBinding == nil || candidate.NativeBinding.Kind != "public-call" || candidate.NativeBinding.UserID != step.NativeBinding.UserID {
				continue
			}
			var authored struct {
				KnownScopes []struct {
					ScopeID string `json:"scope_id"`
				} `json:"known_scopes"`
			}
			if json.Unmarshal(candidate.Operation.Payload, &authored) != nil {
				return "", errors.New("React Native schema-check authored client scope is invalid")
			}
			for _, scope := range authored.KnownScopes {
				scopes[scope.ScopeID] = true
			}
		}
		if len(scopes) != 1 {
			return "", errors.New("React Native schema-check authored client scope is ambiguous")
		}
		for scope := range scopes {
			payload.KnownScopes = append(payload.KnownScopes, struct {
				ScopeID string `json:"scope_id"`
			}{ScopeID: scope})
		}
	}
	if payload.KnownScopes[0].ScopeID == "" {
		return "", errors.New("React Native schema-check authored client scope is empty")
	}
	for _, alias := range c.config.Scenario.NativeIdentityAliases {
		if alias.Kind != "scope" {
			continue
		}
		var authored string
		if json.Unmarshal(alias.Value, &authored) == nil && authored == payload.KnownScopes[0].ScopeID {
			var runtime string
			if err := json.Unmarshal(c.runtimeIDs[alias.Alias], &runtime); err != nil || runtime == "" {
				return "", fmt.Errorf("React Native schema-check scope alias %q value=%s error=%v", alias.Alias, boundedRaw(c.runtimeIDs[alias.Alias]), err)
			}
			return runtime, nil
		}
	}
	return "", fmt.Errorf("React Native schema-check step %s scope=%q has no declared alias", step.ID, payload.KnownScopes[0].ScopeID)
}

func (c *SchemaCheckCoordinator) runtimeSchema(alias string) (clientSchema, error) {
	var schema clientSchema
	if err := json.Unmarshal(c.runtimeIDs[alias], &schema); err != nil || schema.Version == 0 || schema.Hash == "" {
		return clientSchema{}, fmt.Errorf("React Native schema-check schema alias %q value=%s error=%v", alias, boundedRaw(c.runtimeIDs[alias]), err)
	}
	return schema, nil
}

func (c *SchemaCheckCoordinator) uniqueClients() []schemaCheckCall {
	clients := make(map[string]schemaCheckCall, len(c.calls))
	for _, call := range c.calls {
		clients[call.clientKey] = call
	}
	keys := make([]string, 0, len(clients))
	for key := range clients {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]schemaCheckCall, 0, len(keys))
	for _, key := range keys {
		result = append(result, clients[key])
	}
	return result
}

func schemaCheckCase(step scenarios.Step) (string, error) {
	if step.MeasurementSample == nil {
		return "", fmt.Errorf("React Native schema-check step %s has no measurement", step.ID)
	}
	var parameters struct {
		SchemaCase string `json:"schema_case"`
	}
	if err := json.Unmarshal(step.MeasurementSample.Parameters, &parameters); err != nil || parameters.SchemaCase == "" {
		return "", fmt.Errorf("React Native schema-check step %s measurement parameters are invalid", step.ID)
	}
	return parameters.SchemaCase, nil
}

func schemaCheckMeasurementOperationCase(sample scenarios.MeasurementSample) (string, error) {
	var value struct {
		SchemaCase string `json:"schema_case"`
	}
	if err := json.Unmarshal(sample.Operation.Value, &value); err != nil || value.SchemaCase == "" {
		return "", fmt.Errorf("React Native schema-check measurement operation %s has invalid schema case", sample.Operation.ID)
	}
	return value.SchemaCase, nil
}

func schemaCheckClientKey(userID, clientID string) string {
	return "schema-check-" + userID + "-" + clientID
}

func schemaCheckSessionKey(stepID scenarios.StepID) string {
	return "schema-check-" + strings.ToLower(strings.TrimPrefix(string(stepID), "STEP-PERF-SCHEMA-CHECK-"))
}

func schemaCheckDatabase(clientID string) string {
	return "rn-schema-check-" + clientID + ".db"
}

func schemaCheckProofSession(step scenarios.Step) string {
	suffix := strings.TrimPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-")
	lane := "committed"
	if strings.HasPrefix(suffix, "PREPARED-") {
		lane = "prepared"
	}
	epoch := "s1"
	switch suffix {
	case "COMMITTED-M1-RESTART-001":
		epoch = "loss-restarted"
	case "PREPARED-MIGRATE-001", "COMMITTED-MIGRATE-001":
		epoch = "migrate"
	case "PREPARED-CUT-001", "PREPARED-RECOVER-001", "PREPARED-PUSH-001", "PREPARED-COMPLETE-001", "COMMITTED-CUT-001", "COMMITTED-RECOVER-001", "COMMITTED-M2-WRITE-001", "COMMITTED-M1-REPLAY-001", "COMMITTED-M2-REPLY-001", "COMMITTED-COMPLETE-001":
		epoch = "recover"
	}
	return "schema-proof-" + lane + "-" + epoch
}

func schemaCheckProofActions(step scenarios.Step) []string {
	switch strings.TrimPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-") {
	case "PREPARED-BOOTSTRAP-001", "COMMITTED-BOOTSTRAP-001":
		return []string{"open", "synchronize-step", "capture", "stop"}
	case "PREPARED-WRITE-001", "COMMITTED-M1-WRITE-001", "COMMITTED-M2-WRITE-001":
		return []string{"execute-step", "capture"}
	case "COMMITTED-M1-CONNECT-001":
		return []string{"begin-call"}
	case "COMMITTED-M1-SEND-001":
		return []string{"capture"}
	case "COMMITTED-M1-RESTART-001", "PREPARED-CUT-001", "COMMITTED-CUT-001":
		return []string{"restart"}
	case "PREPARED-MIGRATE-001", "COMMITTED-MIGRATE-001":
		return []string{"open", "arm-checkpoint", "begin-call", "await-checkpoint", "capture"}
	case "PREPARED-RECOVER-001", "COMMITTED-RECOVER-001":
		return []string{"arm-checkpoint", "begin-call", "await-checkpoint", "capture"}
	case "PREPARED-PUSH-001", "COMMITTED-M2-REPLY-001":
		return []string{"arm-push", "resume", "await-push", "capture", "resume"}
	case "COMMITTED-M1-REPLAY-001":
		return []string{"arm-push", "resume", "await-push", "capture"}
	case "PREPARED-COMPLETE-001", "COMMITTED-COMPLETE-001":
		return []string{"await-call", "capture", "stop"}
	}
	return nil
}

func schemaCheckProofCommandCount(step scenarios.Step) int { return len(schemaCheckProofActions(step)) }

func schemaCheckProofBinding(step scenarios.Step) error {
	binding := step.NativeBinding
	suffix := strings.TrimPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-")
	operation, kind, stage, callID, checkpoint := "connect/send", "public-call", "begin", "", ""
	switch suffix {
	case "PREPARED-BOOTSTRAP-001":
		stage, callID = "synchronous", "schema_proof_prepared_s1"
	case "COMMITTED-BOOTSTRAP-001":
		stage, callID = "synchronous", "schema_proof_committed_s1"
	case "PREPARED-WRITE-001", "COMMITTED-M1-WRITE-001", "COMMITTED-M2-WRITE-001":
		operation, kind, stage = "local/write", "local-write", ""
	case "COMMITTED-M1-CONNECT-001":
		stage, callID = "synchronous", "schema_proof_committed_m1_initial"
	case "COMMITTED-M1-SEND-001":
		operation, stage, callID = "push/submit", "synchronous", "schema_proof_committed_m1_initial"
	case "COMMITTED-M1-RESTART-001", "PREPARED-CUT-001", "COMMITTED-CUT-001":
		operation, kind, stage = "process/restart-client", "process", ""
	case "PREPARED-MIGRATE-001":
		callID, checkpoint = "schema_proof_prepared_migrate", "migration_prepared"
	case "PREPARED-RECOVER-001":
		callID, checkpoint = "schema_proof_prepared_recover", "migration_committed"
	case "COMMITTED-MIGRATE-001":
		callID, checkpoint = "schema_proof_committed_migrate", "migration_committed"
	case "COMMITTED-RECOVER-001":
		callID, checkpoint = "schema_proof_committed_recover", "migration_committed"
	case "PREPARED-PUSH-001":
		operation, stage, callID = "push/submit", "await-step", "schema_proof_prepared_recover"
	case "COMMITTED-M1-REPLAY-001", "COMMITTED-M2-REPLY-001":
		operation, stage, callID = "push/submit", "await-step", "schema_proof_committed_recover"
	case "PREPARED-COMPLETE-001":
		operation, stage, callID = "pull/request-page", "await-call", "schema_proof_prepared_recover"
	case "COMMITTED-COMPLETE-001":
		operation, stage, callID = "pull/request-page", "await-call", "schema_proof_committed_recover"
	default:
		return fmt.Errorf("React Native schema proof step %s is not declared", step.ID)
	}
	actualCallID := ""
	if binding.CallID != nil {
		actualCallID = string(*binding.CallID)
	}
	if scenarios.OperationKey(step.Operation) != operation || binding.Kind != kind || binding.Stage != stage || actualCallID != callID || binding.Checkpoint != checkpoint {
		return fmt.Errorf("React Native schema proof step %s binding differs from the fixed contract", step.ID)
	}
	if stage == "begin" && binding.Method != "start" {
		return errors.New("schema proof checkpoint does not start its authored call")
	}
	return nil
}

func (c *SchemaCheckCoordinator) proofCallID(call schemaCheckCall) string {
	if call.step.NativeBinding.CallID != nil {
		return string(*call.step.NativeBinding.CallID)
	}
	return ""
}

func schemaCheckProofCaptureName(step scenarios.Step) string {
	suffix := strings.TrimPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-")
	switch suffix {
	case "PREPARED-BOOTSTRAP-001":
		return "PREPARED-S1-001"
	case "COMMITTED-BOOTSTRAP-001":
		return "COMMITTED-S1-001"
	case "PREPARED-WRITE-001":
		return "PREPARED-INTENT-001"
	case "COMMITTED-M1-WRITE-001":
		return "COMMITTED-M1-LOCAL-001"
	case "COMMITTED-M1-SEND-001":
		return "COMMITTED-M1-SEALED-001"
	case "PREPARED-MIGRATE-001":
		return "PREPARED-JOURNAL-001"
	case "COMMITTED-MIGRATE-001":
		return "COMMITTED-JOURNAL-001"
	case "PREPARED-RECOVER-001":
		return "PREPARED-RECOVERED-001"
	case "COMMITTED-RECOVER-001":
		return "COMMITTED-RECOVERED-001"
	case "COMMITTED-M2-WRITE-001":
		return "COMMITTED-M2-INTENT-001"
	case "COMMITTED-M1-REPLAY-001":
		return "COMMITTED-M1-PAUSED-001"
	case "COMMITTED-M2-REPLY-001":
		return "COMMITTED-M2-PAUSED-001"
	case "PREPARED-PUSH-001":
		return "PREPARED-PUSH-PAUSED-001"
	case "PREPARED-COMPLETE-001":
		return "PREPARED-FINAL-001"
	case "COMMITTED-COMPLETE-001":
		return "COMMITTED-FINAL-001"
	}
	return ""
}

func (c *SchemaCheckCoordinator) advanceProofLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	call := c.calls[c.current]
	actions := schemaCheckProofActions(call.step)
	if c.proofPhase == len(actions) {
		c.current++
		c.proofPhase = 0
		c.proofCommand = nil
		c.waiting = schemaCheckWaitingNone
		return c.advanceLocked(ctx, sequence)
	}
	if c.proofPhase == 0 {
		if err := c.applyControllerSteps(ctx, call.controllerSteps); err != nil {
			return exchangeResponse{}, err
		}
		if c.tableName == "" {
			var aliases []scenarios.NativeIdentityAlias
			for _, alias := range c.config.Scenario.NativeIdentityAliases {
				if slices.Contains([]string{"scope", "table", "primary-key"}, alias.Kind) || alias.Kind == "schema" && alias.Alias == "schema-v1" {
					aliases = append(aliases, alias)
				}
			}
			values, err := c.config.Controller.IdentityValues(aliases)
			if err != nil {
				return exchangeResponse{}, err
			}
			for _, value := range values {
				c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
				if value.Alias == "items-table" {
					c.tableName = value.ApplicationIdentifier
				}
				if value.Alias == "items-primary-key" {
					c.primaryKey = value.ApplicationIdentifier
				}
			}
		}
		for _, alias := range c.config.Scenario.NativeIdentityAliases {
			if alias.Kind != "schema" || alias.Alias != call.serverSchemaAlias {
				continue
			}
			values, err := c.config.Controller.IdentityValues([]scenarios.NativeIdentityAlias{alias})
			if err != nil || len(values) != 1 {
				return exchangeResponse{}, fmt.Errorf("resolve proof current schema: %v", err)
			}
			c.runtimeIDs[alias.Alias] = copyRaw(values[0].RuntimeValue)
		}
		if call.serverSchemaAlias != "" {
			if err := c.bindProofPhysicalSchema(call.serverSchemaAlias); err != nil {
				return exchangeResponse{}, err
			}
		}
	}
	action := actions[c.proofPhase]
	parameters := map[string]any{"client_key": call.sessionKey}
	actor, name := "client", action
	var stepIDs []scenarios.StepID
	suffix := strings.TrimPrefix(string(call.step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-")
	checkpoint := "migration_committed"
	if suffix == "PREPARED-MIGRATE-001" {
		checkpoint = "migration_prepared"
	}
	switch action {
	case "open", "restart":
		mode := "reuse"
		if strings.Contains(suffix, "BOOTSTRAP") {
			mode = "create"
		}
		parameters["database_mode"], parameters["initialization"], parameters["seed_step_id"] = mode, "empty", nil
		if mode == "create" {
			if c.config.Scenario.NativeLocalFixture == nil {
				return exchangeResponse{}, errors.New("schema proof local fixture is absent")
			}
			parameters["local_fixture"] = c.config.Scenario.NativeLocalFixture
		}
		if action == "restart" {
			name = "open"
			parameters["process_restart"] = true
			priorCall, target := "", ""
			switch suffix {
			case "COMMITTED-M1-RESTART-001":
				priorCall = "schema_proof_committed_m1_initial"
				target = "server_response_loss"
			case "PREPARED-CUT-001":
				priorCall = "schema_proof_prepared_migrate"
				target = "migration_prepared"
			case "COMMITTED-CUT-001":
				priorCall = "schema_proof_committed_migrate"
				target = "migration_committed"
			}
			if c.proofPaused[call.clientKey] != target {
				return exchangeResponse{}, errors.New("schema proof restart lacks its acknowledged pause")
			}
			c.proofInterrupted[priorCall] = call.step.ID
		}
	case "synchronize-step":
		parameters["method"], parameters["completion"] = "start", "idle"
		stepIDs = []scenarios.StepID{call.step.ID}
	case "execute-step":
		if suffix == "COMMITTED-M2-WRITE-001" && c.proofPaused[call.clientKey] != "migration_committed" {
			return exchangeResponse{}, errors.New("M2 write does not own a paused committed recovery")
		}
		write, err := c.config.Controller.ApplicationWrite(call.step.Operation)
		if err != nil {
			return exchangeResponse{}, err
		}
		call.step.Operation = write
		stepIDs = []scenarios.StepID{call.step.ID}
	case "begin-call":
		if suffix == "COMMITTED-M1-CONNECT-001" {
			c.proxyMu.Lock()
			c.proxyLoss = true
			c.proxyMu.Unlock()
		} else if strings.Contains(suffix, "RECOVER") {
			c.proxyMu.Lock()
			c.proofHTTPBaseline = c.proxyHTTP["*"]
			c.proxyMu.Unlock()
		}
		parameters["method"], parameters["call_id"] = "start", c.proofCallID(call)
		stepIDs = []scenarios.StepID{call.step.ID}
	case "await-call":
		parameters["call_id"], parameters["completion"] = c.proofCallID(call), "idle"
		stepIDs = []scenarios.StepID{call.step.ID}
	case "arm-checkpoint", "await-checkpoint", "arm-push", "await-push", "resume":
		actor, name = "observer", "transport-pause"
		operation := "resume"
		if strings.HasPrefix(action, "arm-") {
			operation = "arm"
		}
		if strings.HasPrefix(action, "await-") {
			operation = "await"
			parameters["timeout_ms"] = 60000
		}
		parameters["operation"] = operation
		if action != "resume" {
			target := checkpoint
			if strings.HasSuffix(action, "-push") {
				target = "push"
			}
			parameters["transport_operation"] = target
		}
		if suffix == "COMMITTED-M1-REPLAY-001" && c.proofPhase == 0 {
			facts, err := c.captureProofServer(ctx, call)
			if err != nil {
				return exchangeResponse{}, err
			}
			c.proofServers["COMMITTED-M1-SERVER-BEFORE-001"] = facts
		}
	case "capture":
		actor = "observer"
		if suffix == "COMMITTED-M1-SEND-001" || suffix == "PREPARED-PUSH-001" || suffix == "COMMITTED-M2-REPLY-001" {
			if suffix == "COMMITTED-M1-SEND-001" {
				select {
				case <-c.proxyLossAccepted:
				case <-ctx.Done():
					return exchangeResponse{}, ctx.Err()
				}
			}
			operation := call.step.Operation
			if suffix == "COMMITTED-M1-SEND-001" {
				var payload map[string]json.RawMessage
				if json.Unmarshal(operation.Payload, &payload) != nil {
					return exchangeResponse{}, errInvalidExchange
				}
				payload["delivery"] = []byte(`"apply"`)
				operation.Payload, _ = json.Marshal(payload)
			}
			if err := c.config.Controller.BindApplicationPush(operation); err != nil {
				return exchangeResponse{}, err
			}
		}
		if suffix == "COMMITTED-M1-SEND-001" {
			c.proofPaused[call.clientKey] = "server_response_loss"
			facts, err := c.captureProofServer(ctx, call)
			if err != nil {
				return exchangeResponse{}, err
			}
			c.proofServers["COMMITTED-M1-ACCEPTED-001"] = facts
		}
		selectors, err := c.proofSelectors(call)
		if err != nil {
			return exchangeResponse{}, err
		}
		parameters = map[string]any{"client_keys": []string{call.sessionKey}, "sources": []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "request-trace", "application-rows", "durable-proof"}, "row_selectors": selectors, "durable_proof_identity": map[string]any{"table_name": c.tableName, "record_id": selectors[0]["primary_key"]}}
	case "stop":
		name = "lifecycle"
		parameters["operation"] = "stop"
	default:
		return exchangeResponse{}, fmt.Errorf("schema proof step %s has no command", call.step.ID)
	}
	c.proofCommand = c.command(call, actor, name, parameters, stepIDs)
	c.waiting = schemaCheckWaitingSync
	return exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command", Command: c.proofCommand}, nil
}

func (c *SchemaCheckCoordinator) proofSelectors(call schemaCheckCall) ([]map[string]any, error) {
	alias := "proof-committed-row"
	if call.step.NativeBinding.ClientID == "client-schema-proof-prepared" {
		alias = "proof-prepared-row"
	}
	var record string
	if json.Unmarshal(c.runtimeIDs[alias], &record) != nil || record == "" {
		return nil, errors.New("schema proof runtime row identity is absent")
	}
	return []map[string]any{
		{"table_name": c.tableName, "primary_key_field": c.primaryKey, "primary_key": record},
		{"table_name": "schema_proof_local", "primary_key_field": "id", "primary_key": "sentinel"},
	}, nil
}

func (c *SchemaCheckCoordinator) acceptProofLocked(call schemaCheckCall, raw json.RawMessage) error {
	if c.proofCommand == nil {
		return errors.New("schema proof command is absent")
	}
	action := c.proofCommand.Action.Action
	var members map[string]json.RawMessage
	if json.Unmarshal(raw, &members) != nil {
		return errInvalidExchange
	}
	process, err := decodeActionProcessIdentity(members["process"])
	if err != nil {
		return err
	}
	if action.Command == "open" {
		if _, err := validateOpenedResult(raw); err != nil {
			return err
		}
		lane := "committed"
		otherLane := "prepared"
		if call.step.NativeBinding.ClientID == "client-schema-proof-prepared" {
			lane, otherLane = otherLane, lane
		}
		if original, found := c.processes["schema-proof-"+lane+"-s1"]; found && original.DatabaseIdentityFingerprint != process.DatabaseIdentityFingerprint {
			return errors.New("schema proof reuse changed its lane database")
		}
		if other, found := c.processes["schema-proof-"+otherLane+"-s1"]; found && other.DatabaseIdentityFingerprint == process.DatabaseIdentityFingerprint {
			return errors.New("schema proof lanes share one database")
		}
		if action.Parameters["process_restart"] == true {
			previousSession := "schema-proof-committed-s1"
			if strings.Contains(string(call.step.ID), "PREPARED-CUT") {
				previousSession = "schema-proof-prepared-migrate"
			}
			if strings.Contains(string(call.step.ID), "COMMITTED-CUT") {
				previousSession = "schema-proof-committed-migrate"
			}
			previous, found := c.processes[previousSession]
			if !found || previous.ProcessID == process.ProcessID || previous.DatabaseIdentityFingerprint != process.DatabaseIdentityFingerprint {
				return errors.New("schema proof cut did not replace the process with the same database")
			}
			delete(c.proofPaused, call.clientKey)
			if strings.Contains(string(call.step.ID), "M1-RESTART") {
				c.proxyLossReleaseOnce.Do(func() { close(c.proxyLossRelease) })
			}
		}
		c.processes[call.sessionKey] = process
	} else {
		if expected, found := c.processes[call.sessionKey]; !found || expected != process {
			return errors.New("schema proof command changed its client process or database")
		}
		switch action.Command {
		case "synchronize-step":
			if err := c.validateSynchronized(call, raw); err != nil {
				return err
			}
			c.proofCompleted = append(c.proofCompleted, c.proofCallID(call))
		case "begin-call":
			if validateActionResult(raw, "call-begun") != nil || string(members["state"]) != `"in_flight"` || string(members["call_id"]) != fmt.Sprintf("%q", c.proofCallID(call)) {
				return errors.New("schema proof call did not begin")
			}
		case "await-call":
			if validateActionResult(raw, "call-completed") != nil || string(members["state"]) != `"completed"` || string(members["call_id"]) != fmt.Sprintf("%q", c.proofCallID(call)) || string(members["completion"]) != `"idle"` || validateReadyStatus(members["status"]) != nil {
				return errors.New("schema proof call did not complete")
			}
			c.proofCompleted = append(c.proofCompleted, c.proofCallID(call))
		case "execute-step":
			if validateActionResult(raw, "local-action") != nil || string(members["rows_affected"]) != "1" {
				return errors.New("schema proof local write did not affect one row")
			}
		case "lifecycle":
			if err := validateStoppedLifecycleResult(raw, process); err != nil {
				return err
			}
		case "transport-pause":
			if validateActionResult(raw, "pause-control") != nil {
				return errors.New("schema proof pause was not acknowledged")
			}
			var operation string
			if json.Unmarshal(members["operation"], &operation) != nil || operation != action.Parameters["operation"] {
				return errors.New("schema proof pause operation differs")
			}
			if operation == "resume" {
				if !isJSONNull(members["target"]) {
					return errInvalidExchange
				}
				delete(c.proofPaused, call.clientKey)
			} else {
				var target string
				if json.Unmarshal(members["target"], &target) != nil || target != action.Parameters["transport_operation"] {
					return errors.New("schema proof pause target differs")
				}
				if operation == "await" {
					c.proofPaused[call.clientKey] = target
				}
			}
		case "capture":
			capture, err := decodeCapture(raw, []string{"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "request_trace", "application_rows", "durable_proof"})
			if err != nil {
				return err
			}
			name := schemaCheckProofCaptureName(call.step)
			if err := c.validateProofCapture(call, name, capture); err != nil {
				return fmt.Errorf("schema proof capture %s: %w", name, err)
			}
			c.proofCaptures[name] = capture
		}
	}
	c.proofPhase++
	return nil
}

func (c *SchemaCheckCoordinator) captureProofServer(ctx context.Context, call schemaCheckCall) (scenarios.StateFacts, error) {
	values, err := c.config.Controller.Capture(ctx, []string{call.clientKey}, []string{"server-state"})
	if err != nil || len(values) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("schema proof server capture failed: %v", err)
	}
	return values[0].StateFacts, nil
}

func (c *SchemaCheckCoordinator) proxyAdapter(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		c.recordSchemaProxyFailure(errors.New("schema proof HTTP request is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	var identity struct {
		ClientID string `json:"client_id"`
	}
	_ = json.Unmarshal(body, &identity)
	c.proxyMu.Lock()
	c.proxyHTTP[identity.ClientID]++
	c.proxyHTTP["*"]++
	loss := c.proxyLoss && identity.ClientID == "client-schema-proof-committed" && request.URL.Path == "/sync/push"
	if loss {
		c.proxyLoss = false
	}
	c.proxyMu.Unlock()
	upstream, err := http.NewRequestWithContext(request.Context(), request.Method, strings.TrimRight(c.upstream, "/")+request.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		c.recordSchemaProxyFailure(err)
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	for name, values := range request.Header {
		if !strings.EqualFold(name, "Host") {
			for _, value := range values {
				upstream.Header.Add(name, value)
			}
		}
	}
	response, err := (&http.Client{Timeout: 60 * time.Second}).Do(upstream)
	if err != nil {
		c.recordSchemaProxyFailure(err)
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maximumExchangeBytes+1))
	if err != nil || len(responseBody) > maximumExchangeBytes {
		c.recordSchemaProxyFailure(errors.New("schema proof HTTP response is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	if request.URL.Path == "/sync/push" && strings.HasPrefix(identity.ClientID, "client-schema-proof-") {
		c.proxyMu.Lock()
		c.proxyPushes[identity.ClientID] = append(c.proxyPushes[identity.ClientID], schemaCheckPush{Request: append([]byte(nil), body...), Response: append([]byte(nil), responseBody...), Status: response.StatusCode})
		c.proxyMu.Unlock()
	}
	if loss {
		if response.StatusCode != http.StatusOK {
			c.recordSchemaProxyFailure(errors.New("initial M1 did not receive a successful upstream response"))
		}
		close(c.proxyLossAccepted)
		<-c.proxyLossRelease
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
		return
	}
	for name, values := range response.Header {
		if !strings.EqualFold(name, "Content-Length") && !strings.EqualFold(name, "Transfer-Encoding") {
			for _, value := range values {
				writer.Header().Add(name, value)
			}
		}
	}
	writer.Header().Set("Content-Length", fmt.Sprint(len(responseBody)))
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(responseBody)
}

func (c *SchemaCheckCoordinator) recordSchemaProxyFailure(err error) {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.proxyFailure == nil {
		c.proxyFailure = err
	}
}

type schemaProofMutation struct {
	MutationID            string       `json:"mutationID"`
	LocalOrder            uint64       `json:"localOrder"`
	TableID               string       `json:"tableID"`
	TableName             string       `json:"tableName"`
	RecordID              string       `json:"recordID"`
	PrimaryKeyFieldID     string       `json:"primaryKeyFieldID"`
	PrimaryKeyLogicalType string       `json:"primaryKeyLogicalType"`
	Operation             string       `json:"operation"`
	AuthoredSchema        clientSchema `json:"authoredSchema"`
	BaseVersion           *string      `json:"baseVersion"`
	ClientVersion         string       `json:"clientVersion"`
	Status                string       `json:"status"`
	SourceKind            string       `json:"sourceKind"`
	DependsOnMutationID   *string      `json:"dependsOnMutationID"`
	NormalizedMutationID  *string      `json:"normalizedMutationID"`
	SealedBatchID         *string      `json:"sealedBatchID"`
	SealedOrdinal         *uint64      `json:"sealedOrdinal"`
	AuthoredFields        []struct {
		FieldID     string          `json:"fieldID"`
		LogicalType string          `json:"logicalType"`
		Value       json.RawMessage `json:"value"`
	} `json:"authoredFields"`
}

func schemaProofMutations(capture finalCapture) ([]schemaProofMutation, error) {
	var mutations []schemaProofMutation
	if decodeStrictValue(capture.Pending, &mutations) != nil || mutations == nil || len(mutations) > 512 {
		return nil, errors.New("schema proof mutation ledger is absent or incomplete")
	}
	seen := make(map[string]bool, len(mutations))
	previousOrder := uint64(0)
	for _, mutation := range mutations {
		if mutation.MutationID == "" || mutation.LocalOrder == 0 || mutation.ClientVersion == "" || mutation.TableID == "" || mutation.RecordID == "" || mutation.AuthoredSchema.Version == 0 || mutation.AuthoredFields == nil {
			return nil, errors.New("schema proof mutation identity is incomplete")
		}
		if seen[mutation.MutationID] || mutation.LocalOrder <= previousOrder {
			return nil, errors.New("schema proof mutation identity or local order is duplicated or changed")
		}
		seen[mutation.MutationID] = true
		previousOrder = mutation.LocalOrder
	}
	return mutations, nil
}

func schemaProofFindMutation(mutations []schemaProofMutation, id string) (schemaProofMutation, error) {
	for _, mutation := range mutations {
		if mutation.MutationID == id {
			return mutation, nil
		}
	}
	return schemaProofMutation{}, fmt.Errorf("schema proof retained mutation %q is absent", id)
}

func schemaProofSameIntent(a, b schemaProofMutation) bool {
	a.Status, b.Status = "", ""
	a.SealedBatchID, b.SealedBatchID = nil, nil
	a.SealedOrdinal, b.SealedOrdinal = nil, nil
	a.NormalizedMutationID, b.NormalizedMutationID = nil, nil
	return reflect.DeepEqual(a, b)
}

func schemaProofSameOriginal(a, b schemaProofMutation) bool {
	if b.Status != a.Status && (a.Status != "pending" || b.Status != "superseded_before_send") {
		return false
	}
	if a.NormalizedMutationID != nil && (b.NormalizedMutationID == nil || *a.NormalizedMutationID != *b.NormalizedMutationID) {
		return false
	}
	a.Status, b.Status = "", ""
	a.NormalizedMutationID, b.NormalizedMutationID = nil, nil
	return reflect.DeepEqual(a, b)
}

func (c *SchemaCheckCoordinator) validateProofCapture(call schemaCheckCall, name string, capture finalCapture) error {
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		return err
	}
	if err := state.requireCompleteMigrationCapture(); err != nil {
		return err
	}
	if err := state.requireCompleteAcceptedOutcomes(); err != nil {
		return err
	}
	mutations, err := schemaProofMutations(capture)
	if err != nil || uint64(len(mutations)+len(state.AcceptedMutationOutcomes)) != state.MutationLedgerCount || uint64(len(state.AcceptedMutationOutcomes)) != state.MutationOutcomeCount {
		return errors.New("schema proof mutation capture is incomplete")
	}
	trace, err := captureTraceFromRaw(capture.Trace)
	if err != nil || trace.Overflowed || trace.SequenceCheckpoint != uint64(len(trace.Observations)) || validateTraceSequence(trace.Observations) != nil {
		return errors.New("schema proof transport capture is incomplete")
	}
	var events []json.RawMessage
	var rejected []json.RawMessage
	if json.Unmarshal(capture.Events, &events) != nil || events == nil || len(events) >= 256 || json.Unmarshal(capture.Rejected, &rejected) != nil || rejected == nil || len(rejected) != 0 || state.RejectedMutationCount != 0 || state.ScopeStateCount != uint64(len(state.ScopeStates)) || state.ScopeRowCount != uint64(len(state.ScopeRows)) || state.ScopeStateCount != 1 || state.RebuildAttemptCount != uint64(len(state.RebuildAttempts)) {
		return errors.New("schema proof captured state is incomplete")
	}
	rows, err := decodeRows(capture.Rows)
	if err != nil || len(rows) != 2 || !semanticRawJSONEqual(rows[1]["id"], []byte(`"sentinel"`)) || !semanticRawJSONEqual(rows[1]["value"], []byte(`"preserve-local"`)) || len(rows[1]) != 2 {
		return errors.New("schema proof local sentinel changed or disappeared")
	}
	selectors, err := c.proofSelectors(call)
	if err != nil {
		return err
	}
	recordRaw, _ := json.Marshal(selectors[0]["primary_key"])
	if !semanticRawJSONEqual(rows[0][c.primaryKey], recordRaw) {
		return errors.New("schema proof capture selected the wrong synchronized row")
	}
	journal, err := decodeMigrationJournal(state.MigrationJournal)
	if err != nil {
		return err
	}
	c.proxyMu.Lock()
	proxyErr := c.proxyFailure
	traffic := c.proxyHTTP["*"]
	pushes := append([]schemaCheckPush(nil), c.proxyPushes[call.step.NativeBinding.ClientID]...)
	c.proxyMu.Unlock()
	if proxyErr != nil {
		return proxyErr
	}
	prepared := call.step.NativeBinding.ClientID == "client-schema-proof-prepared"
	lane := "COMMITTED"
	if prepared {
		lane = "PREPARED"
	}
	s1, err := c.runtimeSchema("schema-v1")
	if err != nil {
		return err
	}
	s2 := clientSchema{}
	if call.serverSchemaAlias == "schema-v2" {
		s2, err = c.runtimeSchema("schema-v2")
		if err != nil {
			return err
		}
	}
	wantSchema := s1
	if strings.Contains(name, "RECOVERED") || strings.Contains(name, "M2-") || strings.Contains(name, "FINAL") || strings.Contains(name, "PAUSED") || name == "COMMITTED-JOURNAL-001" {
		wantSchema = s2
	}
	if state.Schema == nil || *state.Schema != wantSchema {
		return errors.New("schema proof active schema differs from its cut")
	}
	columns, err := decodePhysicalSchema(state.PhysicalSchema)
	if err != nil {
		return err
	}
	if err := schemaProofPhysicalColumns(columns, c.proofPhysicalSchemas[wantSchema], c.tableName); err != nil {
		return err
	}
	wantValue := "42"
	if prepared {
		if name == "PREPARED-S1-001" {
			wantValue = "41"
		}
	} else if name != "COMMITTED-S1-001" && !strings.Contains(name, "M2-") && !strings.Contains(name, "M1-PAUSED") && !strings.Contains(name, "FINAL") {
		wantValue = "41"
	}
	valueRaw, _ := json.Marshal(wantValue)
	if !semanticRawJSONEqual(rows[0]["value"], valueRaw) {
		return errors.New("schema proof local intent row changed")
	}
	if strings.Contains(name, "M2-") || name == "COMMITTED-M1-PAUSED-001" || name == "COMMITTED-FINAL-001" {
		if !semanticRawJSONEqual(rows[0]["note"], []byte(`"later-note"`)) {
			return errors.New("schema proof later note changed")
		}
	}
	if strings.Contains(name, "JOURNAL") {
		if journal == nil || journal.Source != s1 || journal.Target != s2 || journal.Action != "replace" || journal.Stored["migration_plan_json"] == "" || journal.Stored["migration_plan_hash"] == "" {
			return errors.New("schema proof migration journal is incomplete")
		}
		if prepared && journal.Phase != "prepared" || !prepared && journal.Phase != "applied" && journal.Phase != "ddl_applied" {
			return errors.New("schema proof migration phase differs from its acknowledged cut")
		}
		baseline := c.proofCaptures["PREPARED-INTENT-001"]
		if !prepared {
			baseline = c.proofCaptures["COMMITTED-M1-SEALED-001"]
		}
		var source inspectedClientState
		if json.Unmarshal(baseline.ClientState, &source) != nil {
			return errors.New("schema proof source capture is absent")
		}
		if prepared && !reflect.DeepEqual(source.ScopeStates, state.ScopeStates) {
			return errors.New("prepared migration changed the active S1 cursor")
		}
		if len(source.ScopeStates) != 1 || source.ScopeStates[0].Cursor == nil || len(trace.Observations) == 0 {
			return errors.New("migration source cursor evidence is absent")
		}
		connect := trace.Observations[len(trace.Observations)-1]
		if connect.CursorFingerprintsComplete == nil || !*connect.CursorFingerprintsComplete || !slices.Equal(connect.CursorFingerprints, []string{hashFingerprint(*source.ScopeStates[0].Cursor)}) {
			return errors.New("migration connect did not present the actual S1 source cursor")
		}
		if err := c.validateProofActivation(state, journal, trace, !prepared, false); err != nil {
			return err
		}
		if !semanticRawJSONEqual(baseline.Pending, capture.Pending) {
			return errors.New("migration changed immutable queued intent")
		}
	} else if strings.Contains(name, "RECOVERED") {
		if traffic != c.proofHTTPBaseline || trace.SequenceCheckpoint != 0 {
			return errors.New("migration recovery sent HTTP before its capture")
		}
		before := c.proofCaptures[lane+"-JOURNAL-001"]
		var source inspectedClientState
		if json.Unmarshal(before.ClientState, &source) != nil {
			return errors.New("migration cut capture is absent")
		}
		priorJournal, err := decodeMigrationJournal(source.MigrationJournal)
		if err != nil || priorJournal == nil {
			return errors.New("migration cut journal is absent")
		}
		if err := c.validateProofActivation(state, priorJournal, trace, true, true); err != nil {
			return err
		}
		if !semanticRawJSONEqual(before.Pending, capture.Pending) {
			return errors.New("migration recovery changed queued intent")
		}
		if journal != nil && journal.Phase == "prepared" {
			return errors.New("migration recovery left a prepared journal")
		}
	} else if name == "COMMITTED-M1-SEALED-001" {
		if len(pushes) != 1 {
			return errors.New("M1 loss did not receive exactly one actual push")
		}
		if err := c.validateProofPush(pushes[0], s1, s1, nil); err != nil {
			return err
		}
		if err := schemaProofWireIntent(pushes[0], capture); err != nil {
			return err
		}
		previous, err := schemaProofMutations(c.proofCaptures["COMMITTED-M1-LOCAL-001"])
		if err != nil {
			return err
		}
		for _, original := range previous {
			current, err := schemaProofFindMutation(mutations, original.MutationID)
			if err != nil || !schemaProofSameIntent(original, current) || original.SourceKind != "normalized" && !schemaProofSameOriginal(original, current) {
				return errors.New("M1 sealing changed its immutable S1 local intent")
			}
		}
		sealed := false
		for _, mutation := range mutations {
			sealed = sealed || mutation.Status == "sealed" && mutation.SealedBatchID != nil && mutation.SealedOrdinal != nil
		}
		if !sealed || state.SealedBatchCount != 1 || state.MutationOutcomeCount != 0 {
			return errors.New("accepted M1 did not remain locally unresolved and sealed")
		}
	} else if name == "COMMITTED-M2-INTENT-001" {
		if c.proofPaused[call.clientKey] != "migration_committed" || traffic != c.proofHTTPBaseline {
			return errors.New("M2 write escaped its acknowledged recovery pause")
		}
		original, err := schemaProofMutations(c.proofCaptures["COMMITTED-RECOVERED-001"])
		if err != nil {
			return err
		}
		for _, previous := range original {
			current, err := schemaProofFindMutation(mutations, previous.MutationID)
			if err != nil || !reflect.DeepEqual(current, previous) {
				return errors.New("M2 write changed its unresolved predecessor")
			}
		}
		if len(mutations) != len(original)+1 {
			return errors.New("M2 did not add one real local intent")
		}
		later := mutations[len(mutations)-1]
		if later.AuthoredSchema != s2 || later.Status != "pending" || later.SourceKind == "normalized" || later.SealedBatchID != nil || later.LocalOrder <= original[len(original)-1].LocalOrder {
			return errors.New("M2 original intent is not an unsealed later S2 record")
		}
		var proof durableProof
		if json.Unmarshal(capture.DurableProof, &proof) != nil || proof.RowMetadata == nil || later.BaseVersion == nil || *later.BaseVersion != proof.RowMetadata.ServerVersion {
			return errors.New("M2 did not retain its actual local base")
		}
	} else if name == "COMMITTED-M1-PAUSED-001" {
		if c.proofPaused[call.clientKey] != "push" || len(pushes) != 2 {
			return errors.New("historical M1 replay lacks its first successful response pause")
		}
		if err := c.validateProofReplay(pushes[0], pushes[1], s1, s2); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		facts, err := c.captureProofServer(ctx, call)
		cancel()
		if err != nil {
			return err
		}
		before := c.proofServers["COMMITTED-M1-SERVER-BEFORE-001"]
		if !reflect.DeepEqual(before.Rows, facts.Rows) || !reflect.DeepEqual(before.MutationOutcomes, facts.MutationOutcomes) {
			return errors.New("historical M1 replay changed authoritative rows or outcome identities")
		}
		c.proofServers["COMMITTED-M1-SERVER-AFTER-001"] = facts
		if state.MutationOutcomeCount != 0 {
			return errors.New("M1 reconciled before its first response pause")
		}
		m1, err := schemaProofPushMutation(pushes[0])
		if err != nil {
			return err
		}
		if err := c.validateProofLaterIntent(c.proofCaptures["COMMITTED-M2-INTENT-001"], capture, false, "", m1.MutationID); err != nil {
			return err
		}
	} else if name == "COMMITTED-M2-PAUSED-001" {
		if c.proofPaused[call.clientKey] != "push" || len(pushes) != 3 || state.MutationOutcomeCount != 1 {
			return errors.New("M2 response pause did not follow validated M1 reconciliation")
		}
		version, err := schemaProofAcceptedVersion(pushes[0].Response)
		if err != nil {
			return err
		}
		m1, err := schemaProofPushMutation(pushes[0])
		if err != nil {
			return err
		}
		if err := schemaProofStoredOutcome(state, m1.MutationID, pushes[1].Response); err != nil {
			return err
		}
		if err := c.validateProofLaterIntent(c.proofCaptures["COMMITTED-M1-PAUSED-001"], capture, true, version, m1.MutationID); err != nil {
			return err
		}
		if err := c.validateProofPush(pushes[2], s2, s2, &version); err != nil {
			return err
		}
		if err := schemaProofWireIntent(pushes[2], capture); err != nil {
			return err
		}
		if err := schemaProofNamedM2Push(c.proofCaptures["COMMITTED-M1-PAUSED-001"], pushes[2]); err != nil {
			return err
		}
	} else if name == "PREPARED-PUSH-PAUSED-001" {
		if c.proofPaused[call.clientKey] != "push" || len(pushes) != 1 {
			return errors.New("prepared push has no actual response pause")
		}
		if err := c.validateProofPush(pushes[0], s2, s1, nil); err != nil {
			return err
		}
		if err := schemaProofWireIntent(pushes[0], capture); err != nil {
			return err
		}
	} else if strings.Contains(name, "FINAL") {
		wantOutcomes := uint64(2)
		if prepared {
			wantOutcomes = 1
		}
		if validateReadyStatus(capture.Status) != nil || state.SealedBatchCount != 0 || state.RejectedMutationCount != 0 || state.MutationOutcomeCount != wantOutcomes {
			return errors.New("schema proof did not finish ordinary reconciliation")
		}
		for _, mutation := range mutations {
			if mutation.Status == "sealed" || mutation.Status == "pending" || mutation.Status == "blocked_by_predecessor" {
				return errors.New("schema proof final queue has unresolved mutations")
			}
		}
		if len(pushes) == 0 {
			return errors.New("schema proof has no successful real push")
		}
		version, err := schemaProofAcceptedVersion(pushes[len(pushes)-1].Response)
		if err != nil {
			return err
		}
		var proof durableProof
		if json.Unmarshal(capture.DurableProof, &proof) != nil || proof.RowMetadata == nil || proof.RowMetadata.ServerVersion != version {
			return errors.New("schema proof final row metadata did not install its own accepted outcome version")
		}
		originalCapture := c.proofCaptures["COMMITTED-M1-LOCAL-001"]
		if prepared {
			originalCapture = c.proofCaptures["PREPARED-INTENT-001"]
		}
		originals, err := schemaProofMutations(originalCapture)
		if err != nil {
			return err
		}
		if !prepared {
			later, err := schemaProofMutations(c.proofCaptures["COMMITTED-M2-INTENT-001"])
			if err != nil {
				return err
			}
			originals = append(originals, later...)
		}
		for _, original := range originals {
			if original.SourceKind == "normalized" {
				continue
			}
			retained, err := schemaProofFindMutation(mutations, original.MutationID)
			if err != nil || !schemaProofSameOriginal(original, retained) {
				return errors.New("schema proof final capture lost or changed an original local record")
			}
			if retained.NormalizedMutationID == nil {
				return errors.New("schema proof final original has no accepted normalized identity")
			}
			expectedLink := original.NormalizedMutationID
			if expectedLink == nil {
				pausedName := "COMMITTED-M1-PAUSED-001"
				if prepared {
					pausedName = "PREPARED-PUSH-PAUSED-001"
				}
				paused, err := schemaProofMutations(c.proofCaptures[pausedName])
				if err != nil {
					return err
				}
				before, err := schemaProofFindMutation(paused, original.MutationID)
				if err != nil || !schemaProofSameOriginal(original, before) {
					return errors.New("schema proof normalized capture changed its original local record")
				}
				expectedLink = before.NormalizedMutationID
			}
			if expectedLink == nil || *expectedLink != *retained.NormalizedMutationID {
				return errors.New("schema proof final original changed its validated normalized identity")
			}
			matched := false
			for _, push := range pushes {
				wire, err := schemaProofPushMutation(push)
				if err != nil {
					return err
				}
				if wire.MutationID == *retained.NormalizedMutationID {
					if err := schemaProofStoredOutcome(state, wire.MutationID, push.Response); err != nil {
						return err
					}
					matched = true
				}
			}
			if !matched {
				return errors.New("schema proof final accepted record is not bound to its original local record")
			}
		}
		if prepared {
			if len(pushes) != 1 || c.validateProofPush(pushes[0], s2, s1, nil) != nil {
				return errors.New("prepared recovered push did not succeed under S2")
			}
		}
		if err := c.validateProofFinalTraffic(trace, s2, lane); err != nil {
			return err
		}
	}
	if name == "PREPARED-INTENT-001" || name == "COMMITTED-M1-LOCAL-001" || name == "COMMITTED-M2-INTENT-001" {
		expectedSchema := s1
		if name == "COMMITTED-M2-INTENT-001" {
			expectedSchema = s2
		}
		valueField, err := c.config.Controller.RuntimeFieldID("items", "value")
		if err != nil {
			return err
		}
		noteField := ""
		if expectedSchema == s2 {
			noteField, err = c.config.Controller.RuntimeFieldID("items", "note")
			if err != nil {
				return err
			}
		}
		checked := 0
		for _, mutation := range mutations {
			if mutation.AuthoredSchema != expectedSchema {
				continue
			}
			if mutation.Operation != "update" || mutation.TableName != c.tableName || mutation.RecordID != selectors[0]["primary_key"] || mutation.BaseVersion == nil {
				return errors.New("authored local mutation has the wrong row or base binding")
			}
			wantFields := 1
			if noteField != "" {
				wantFields++
			}
			if len(mutation.AuthoredFields) != wantFields {
				return errors.New("authored local mutation lost a field or captured an extra field")
			}
			for _, field := range mutation.AuthoredFields {
				if field.LogicalType != "string" || field.FieldID == valueField && !semanticRawJSONEqual(field.Value, valueRaw) || field.FieldID == noteField && !semanticRawJSONEqual(field.Value, []byte(`"later-note"`)) || field.FieldID != valueField && field.FieldID != noteField {
					return errors.New("authored local mutation differs from its real local write")
				}
			}
			checked++
		}
		if checked == 0 {
			return errors.New("authored local mutation is absent")
		}
	}
	return nil
}

func schemaProofWireIntent(push schemaCheckPush, capture finalCapture) error {
	var request schemaProofPushRequest
	if json.Unmarshal(push.Request, &request) != nil || len(request.Mutations) != 1 {
		return errors.New("actual push mutation is absent")
	}
	var wire schemaProofWireMutation
	if json.Unmarshal(request.Mutations[0], &wire) != nil {
		return errors.New("actual push mutation is invalid")
	}
	mutations, err := schemaProofMutations(capture)
	if err != nil {
		return err
	}
	sealed, err := schemaProofFindMutation(mutations, wire.MutationID)
	if err != nil || sealed.Status != "sealed" || sealed.SealedBatchID == nil || *sealed.SealedBatchID != request.BatchID || sealed.SealedOrdinal == nil || *sealed.SealedOrdinal != 0 || sealed.TableID != wire.Table || sealed.AuthoredSchema != wire.AuthoredSchema || !reflect.DeepEqual(sealed.BaseVersion, wire.BaseVersion) || sealed.ClientVersion != wire.ClientVersion || len(wire.Columns) != len(sealed.AuthoredFields) {
		return errors.New("actual outgoing bytes differ from their immutable sealed ledger binding")
	}
	record, _ := json.Marshal(sealed.RecordID)
	if len(wire.PK) != 1 || !semanticRawJSONEqual(wire.PK[sealed.PrimaryKeyFieldID], record) {
		return errors.New("actual push primary key differs from its sealed ledger binding")
	}
	for _, field := range sealed.AuthoredFields {
		if !semanticRawJSONEqual(wire.Columns[field.FieldID], field.Value) {
			return errors.New("actual outgoing field differs from its immutable sealed value")
		}
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateProofActivation(state inspectedClientState, journal *migrationJournalCapture, trace traceSnapshot, activated, recovery bool) error {
	var updates map[string]*string
	if json.Unmarshal([]byte(journal.Stored["scope_cursor_updates_json"]), &updates) != nil || len(updates) != 1 || len(state.ScopeStates) != 1 {
		return errors.New("migration journal replacement cursor is incomplete")
	}
	scope := state.ScopeStates[0]
	cursor, found := updates[scope.ScopeID]
	if !found || cursor == nil || *cursor == "" {
		return errors.New("migration journal has no actual replacement cursor")
	}
	if activated && (state.Schema == nil || *state.Schema != journal.Target || scope.Cursor == nil || *scope.Cursor != *cursor) {
		return errors.New("migration activation did not atomically install S2 and its issued cursor")
	}
	if !activated && (state.Schema == nil || *state.Schema != journal.Source || scope.Cursor != nil && *scope.Cursor == *cursor) {
		return errors.New("prepared migration installed its target before commit")
	}
	if recovery && (len(trace.Observations) != 0 || trace.SequenceCheckpoint != 0) || !recovery && len(trace.Observations) == 0 {
		return errors.New("migration cut omitted real connect evidence or recovery sent traffic")
	}
	if !recovery {
		for _, observation := range trace.Observations {
			if observation.OperationClass == "pull" || observation.OperationClass == "push" || observation.OperationClass == "rebuild" {
				return errors.New("migration cut sent target traffic before activation capture")
			}
		}
		connect := trace.Observations[len(trace.Observations)-1]
		facts, err := decodeConnectResponseFacts(connect.ConnectResponseFacts)
		if err != nil || validateTraceOperation(connect, "connect") != nil || facts.Action != "replace" || facts.SchemaVersion != journal.Target.Version || facts.SchemaHash != journal.Target.Hash || !facts.ScopeCursorUpdatesComplete {
			return errors.New("migration cut has no actual replacement connect response")
		}
		issued, found := facts.ScopeCursorUpdates[hashFingerprint(scope.ScopeID)]
		if !found || issued == nil || *issued != hashFingerprint(*cursor) {
			return errors.New("migration journal cursor differs from the actual response-issued replacement")
		}
		version, versionErr := requestInteger(connect, "schema_version")
		hash, hashErr := requestString(connect, "schema_hash")
		if versionErr != nil || hashErr != nil || version != journal.Source.Version || hash != journal.Source.Hash {
			return errors.New("migration connect did not present the captured S1 source schema")
		}
	}
	columns, err := decodePhysicalSchema(state.PhysicalSchema)
	if err != nil {
		return err
	}
	expected := journal.Source
	if activated {
		expected = journal.Target
	}
	return schemaProofPhysicalColumns(columns, c.proofPhysicalSchemas[expected], c.tableName)
}

func schemaProofPhysicalColumns(observed, expected []physicalSchemaColumn, table string) error {
	if len(expected) == 0 {
		return errors.New("authored synchronized physical schema is absent")
	}
	actual := make(map[string]physicalSchemaColumn)
	for _, column := range observed {
		if column.TableName == table {
			if _, duplicate := actual[column.Name]; duplicate {
				return errors.New("synchronized physical schema contains a duplicate subject column")
			}
			column.Type = strings.ToUpper(column.Type)
			actual[column.Name] = column
		}
	}
	if len(actual) != len(expected) {
		return errors.New("synchronized physical column set differs from the authored manifest")
	}
	for _, column := range expected {
		if actual[column.Name] != column {
			return errors.New("synchronized physical type, nullability, or primary-key position differs from the authored manifest")
		}
	}
	return nil
}

func (c *SchemaCheckCoordinator) bindProofPhysicalSchema(alias string) error {
	if c.tableName != "cf_items" || c.primaryKey != "id" {
		return errors.New("schema proof physical subject must bind items to cf_items with primary key id")
	}
	schema, err := c.runtimeSchema(alias)
	if err != nil {
		return err
	}
	if _, present := c.proofPhysicalSchemas[schema]; present {
		return nil
	}
	var manifest struct {
		Tables []struct {
			TableID string `json:"table_id"`
			Fields  []struct {
				FieldID    string `json:"field_id"`
				Type       string `json:"type"`
				Nullable   bool   `json:"nullable"`
				PrimaryKey bool   `json:"primary_key"`
			} `json:"fields"`
		} `json:"tables"`
	}
	writeSuffix := "PREPARED-WRITE-001"
	if alias == "schema-v1" {
		var setup struct {
			InitialSchema json.RawMessage `json:"initial_schema"`
		}
		if json.Unmarshal(c.config.Scenario.Model.Setup[0].Payload, &setup) != nil || json.Unmarshal(setup.InitialSchema, &manifest) != nil {
			return errors.New("authored S1 physical manifest is invalid")
		}
	} else if alias == "schema-v2" {
		writeSuffix = "COMMITTED-M2-WRITE-001"
		for _, step := range c.config.Scenario.Steps {
			if step.ID == "STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001" {
				if json.Unmarshal(step.Operation.Payload, &manifest) != nil {
					return errors.New("authored S2 physical manifest is invalid")
				}
			}
		}
	} else {
		return errors.New("schema proof physical manifest is not S1 or S2")
	}
	if len(manifest.Tables) != 1 || manifest.Tables[0].TableID != "items" || len(manifest.Tables[0].Fields) == 0 {
		return errors.New("schema proof authored table manifest is incomplete")
	}
	var authored, runtime struct {
		TableID string                     `json:"table_id"`
		PK      map[string]json.RawMessage `json:"pk"`
		Columns map[string]json.RawMessage `json:"columns"`
	}
	for _, step := range c.config.Scenario.Steps {
		if string(step.ID) == "STEP-PERF-SCHEMA-CHECK-PROOF-"+writeSuffix {
			if json.Unmarshal(step.Operation.Payload, &authored) != nil {
				return errors.New("schema proof authored physical field binding is invalid")
			}
			operation, err := c.config.Controller.ApplicationWrite(step.Operation)
			if err != nil {
				return err
			}
			if json.Unmarshal(operation.Payload, &runtime) != nil {
				return errors.New("schema proof runtime physical field binding is invalid")
			}
		}
	}
	if runtime.TableID != "cf_items" || len(runtime.PK) != 1 || len(runtime.PK["id"]) == 0 {
		return errors.New("schema proof runtime write has the wrong physical table or primary-key binding")
	}
	fieldCount := 2
	if alias == "schema-v2" {
		fieldCount = 3
	}
	if len(manifest.Tables[0].Fields) != fieldCount {
		return errors.New("schema proof authored physical field set is incomplete")
	}
	var expected []physicalSchemaColumn
	seen := make(map[string]bool)
	for _, field := range manifest.Tables[0].Fields {
		if seen[field.FieldID] || field.FieldID != "id" && field.FieldID != "value" && (alias != "schema-v2" || field.FieldID != "note") || field.PrimaryKey != (field.FieldID == "id") {
			return errors.New("schema proof authored physical field identity is invalid")
		}
		seen[field.FieldID] = true
		if field.Type != "string" {
			return errors.New("schema proof manifests must retain their authored string storage")
		}
		name, primaryPosition := c.primaryKey, uint64(1)
		if !field.PrimaryKey {
			name, primaryPosition = "", 0
			for runtimeName, value := range runtime.Columns {
				if semanticRawJSONEqual(value, authored.Columns[field.FieldID]) {
					if name != "" {
						return errors.New("schema proof runtime physical field binding is ambiguous")
					}
					name = runtimeName
				}
			}
			if name == "" {
				return errors.New("schema proof authored physical field has no runtime binding")
			}
			if name != field.FieldID {
				return errors.New("schema proof authored physical field must retain its physical name")
			}
		}
		expected = append(expected, physicalSchemaColumn{TableName: c.tableName, Name: name, Type: "TEXT", NotNull: !field.Nullable && !field.PrimaryKey, PrimaryKeyPosition: primaryPosition})
	}
	// conformance/blackbox/testdata/schema.sql prescribes these support columns for the cf_items fixture.
	expected = append(expected,
		physicalSchemaColumn{TableName: "cf_items", Name: "owner_id", Type: "TEXT", NotNull: true},
		physicalSchemaColumn{TableName: "cf_items", Name: "updated_at", Type: "TEXT", NotNull: true},
		physicalSchemaColumn{TableName: "cf_items", Name: "deleted_at", Type: "TEXT"},
	)
	if c.proofPhysicalSchemas == nil {
		c.proofPhysicalSchemas = make(map[clientSchema][]physicalSchemaColumn)
	}
	c.proofPhysicalSchemas[schema] = expected
	return nil
}

type schemaProofPushRequest struct {
	ClientID  string            `json:"client_id"`
	BatchID   string            `json:"batch_id"`
	Schema    clientSchema      `json:"schema"`
	Mutations []json.RawMessage `json:"mutations"`
}

type schemaProofWireMutation struct {
	MutationID     string                     `json:"mutation_id"`
	Table          string                     `json:"table"`
	PK             map[string]json.RawMessage `json:"pk"`
	AuthoredSchema clientSchema               `json:"authored_schema"`
	Operation      string                     `json:"op"`
	BaseVersion    *string                    `json:"base_version"`
	ClientVersion  string                     `json:"client_version"`
	Columns        map[string]json.RawMessage `json:"columns"`
}

func schemaProofPushMutation(push schemaCheckPush) (schemaProofWireMutation, error) {
	var request schemaProofPushRequest
	var mutation schemaProofWireMutation
	if json.Unmarshal(push.Request, &request) != nil || len(request.Mutations) != 1 || json.Unmarshal(request.Mutations[0], &mutation) != nil || mutation.MutationID == "" {
		return schemaProofWireMutation{}, errors.New("schema proof named actual push mutation is absent")
	}
	return mutation, nil
}

func schemaProofStoredOutcome(state inspectedClientState, id string, response []byte) error {
	if err := state.requireCompleteAcceptedOutcomes(); err != nil {
		return err
	}
	stored, present := state.AcceptedMutationOutcomes[id]
	outcome, err := schemaProofAccepted(response)
	if !present || err != nil || !semanticRawJSONEqual([]byte(stored), outcome) {
		return errors.New("schema proof named durable accepted outcome differs from its actual successful response")
	}
	return nil
}

func schemaProofNamedM2Push(before finalCapture, push schemaCheckPush) error {
	mutations, err := schemaProofMutations(before)
	if err != nil {
		return err
	}
	var original schemaProofMutation
	for _, mutation := range mutations {
		if mutation.SourceKind != "normalized" && mutation.LocalOrder > original.LocalOrder {
			original = mutation
		}
	}
	if original.NormalizedMutationID == nil {
		return errors.New("schema proof original M2 normalized identity is absent")
	}
	mutation, err := schemaProofPushMutation(push)
	if err != nil || mutation.MutationID != *original.NormalizedMutationID || mutation.AuthoredSchema != original.AuthoredSchema {
		return errors.New("third actual push does not name the validated normalized M2 identity")
	}
	return nil
}

func schemaProofAccepted(response []byte) (json.RawMessage, error) {
	var envelope struct {
		Accepted []json.RawMessage `json:"accepted"`
		Rejected []json.RawMessage `json:"rejected"`
	}
	if json.Unmarshal(response, &envelope) != nil || len(envelope.Accepted) != 1 || envelope.Rejected == nil || len(envelope.Rejected) != 0 {
		return nil, errors.New("schema proof push did not return one independent successful outcome")
	}
	return envelope.Accepted[0], nil
}

func schemaProofAcceptedVersion(response []byte) (string, error) {
	raw, err := schemaProofAccepted(response)
	if err != nil {
		return "", err
	}
	var outcome struct {
		ServerVersion string `json:"server_version"`
		Status        string `json:"status"`
	}
	if json.Unmarshal(raw, &outcome) != nil || outcome.ServerVersion == "" || outcome.Status != "applied" {
		return "", errors.New("schema proof accepted predecessor version is absent")
	}
	return outcome.ServerVersion, nil
}

func (c *SchemaCheckCoordinator) validateProofPush(push schemaCheckPush, envelopeSchema, authoredSchema clientSchema, base *string) error {
	var request schemaProofPushRequest
	if push.Status != http.StatusOK || json.Unmarshal(push.Request, &request) != nil || request.BatchID == "" || request.Schema != envelopeSchema || len(request.Mutations) != 1 {
		return errors.New("schema proof real push envelope differs from its current schema")
	}
	var mutation schemaProofWireMutation
	if json.Unmarshal(request.Mutations[0], &mutation) != nil || mutation.MutationID == "" || mutation.AuthoredSchema != authoredSchema || mutation.Operation != "update" || mutation.ClientVersion == "" || mutation.BaseVersion == nil || len(mutation.PK) != 1 || len(mutation.Columns) == 0 {
		return errors.New("schema proof real mutation binding is incomplete")
	}
	if base != nil && *mutation.BaseVersion != *base {
		return errors.New("M2 wire did not use its validated accepted predecessor base")
	}
	raw, err := schemaProofAccepted(push.Response)
	if err != nil {
		return err
	}
	var outcome struct {
		MutationID    string          `json:"mutation_id"`
		Status        string          `json:"status"`
		OutcomeSchema clientSchema    `json:"outcome_schema"`
		ServerVersion string          `json:"server_version"`
		RowChecksum   json.RawMessage `json:"row_checksum"`
	}
	if json.Unmarshal(raw, &outcome) != nil || outcome.MutationID != mutation.MutationID || outcome.Status != "applied" || outcome.OutcomeSchema != envelopeSchema || outcome.ServerVersion == "" || !hasJSONValue(outcome.RowChecksum) {
		return errors.New("schema proof actual push outcome differs from its own mutation and schema")
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateProofReplay(initial, replay schemaCheckPush, s1, s2 clientSchema) error {
	if err := c.validateProofPush(initial, s1, s1, nil); err != nil {
		return err
	}
	var original, current schemaProofPushRequest
	if json.Unmarshal(initial.Request, &original) != nil || json.Unmarshal(replay.Request, &current) != nil || replay.Status != http.StatusOK || len(current.Mutations) != len(original.Mutations) || len(current.Mutations) != 1 {
		return errors.New("M1 replay request is incomplete")
	}
	if original.BatchID == current.BatchID {
		if !bytes.Equal(initial.Request, replay.Request) {
			return errors.New("M1 same-batch replay changed original request bytes")
		}
	} else if current.Schema != s2 || current.ClientID != original.ClientID || !bytes.Equal(bytes.TrimSpace(original.Mutations[0]), bytes.TrimSpace(current.Mutations[0])) {
		return errors.New("M1 successor replay changed identity, authored S1 content, or order")
	}
	oldOutcome, err := schemaProofAccepted(initial.Response)
	if err != nil {
		return err
	}
	newOutcome, err := schemaProofAccepted(replay.Response)
	if err != nil || !semanticRawJSONEqual(oldOutcome, newOutcome) {
		return errors.New("M1 replay did not return its actual historical S1 outcome")
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateProofLaterIntent(before, after finalCapture, reconciled bool, acceptedVersion, predecessorID string) error {
	prior, err := schemaProofMutations(before)
	if err != nil {
		return err
	}
	current, err := schemaProofMutations(after)
	if err != nil {
		return err
	}
	var original schemaProofMutation
	for _, mutation := range prior {
		if mutation.SourceKind != "normalized" && mutation.AuthoredSchema.Version > 0 && (original.MutationID == "" || mutation.LocalOrder > original.LocalOrder) {
			original = mutation
		}
	}
	if original.MutationID == "" {
		return errors.New("original later local record is absent")
	}
	retained, err := schemaProofFindMutation(current, original.MutationID)
	if err != nil || !schemaProofSameOriginal(original, retained) {
		return errors.New("original M2 local record changed during predecessor reconciliation")
	}
	if retained.NormalizedMutationID == nil {
		return errors.New("M2 has no inspectable normalized successor")
	}
	normalized, err := schemaProofFindMutation(current, *retained.NormalizedMutationID)
	if err != nil || normalized.AuthoredSchema != original.AuthoredSchema || !reflect.DeepEqual(normalized.AuthoredFields, original.AuthoredFields) || normalized.ClientVersion != original.ClientVersion || normalized.Operation != original.Operation || normalized.RecordID != original.RecordID || normalized.SourceKind != "normalized" || normalized.LocalOrder <= original.LocalOrder {
		return errors.New("normalized M2 lost its original identity binding or authored fields")
	}
	if reconciled {
		if original.NormalizedMutationID == nil || *original.NormalizedMutationID != normalized.MutationID {
			return errors.New("M2 normalized identity changed after M1 reconciliation")
		}
		previous, err := schemaProofFindMutation(prior, normalized.MutationID)
		if err != nil {
			return err
		}
		allowed := previous
		allowed.BaseVersion = &acceptedVersion
		allowed.DependsOnMutationID = nil
		if previous.DependsOnMutationID == nil || *previous.DependsOnMutationID != predecessorID || !schemaProofSameIntent(allowed, normalized) || normalized.Status != "sealed" || normalized.SealedBatchID == nil || normalized.SealedOrdinal == nil || normalized.DependsOnMutationID != nil || normalized.BaseVersion == nil || *normalized.BaseVersion != acceptedVersion {
			return errors.New("M2 changed more than the validated unsealed base and dependency transition")
		}
	} else if normalized.BaseVersion == nil || original.BaseVersion == nil || *normalized.BaseVersion != *original.BaseVersion || normalized.SealedBatchID != nil || normalized.Status != "pending" || normalized.DependsOnMutationID == nil || *normalized.DependsOnMutationID != predecessorID {
		return errors.New("M2 normalized before M1 reconciliation with a substituted base or missing dependency")
	}
	return nil
}

func (c *SchemaCheckCoordinator) validateProofFinalTraffic(trace traceSnapshot, target clientSchema, lane string) error {
	if len(trace.Observations) < 2 {
		return errors.New("recovery connect expectations have no real later traffic")
	}
	connect := trace.Observations[0]
	facts, err := decodeConnectResponseFacts(connect.ConnectResponseFacts)
	if err != nil || validateTraceOperation(connect, "connect") != nil || facts.Action != "none" || facts.SchemaVersion != target.Version || facts.SchemaHash != target.Hash {
		return errors.New("recovery real connect did not present its installed S2 schema")
	}
	var recovered inspectedClientState
	if json.Unmarshal(c.proofCaptures[lane+"-RECOVERED-001"].ClientState, &recovered) != nil || len(recovered.ScopeStates) != 1 || recovered.ScopeStates[0].Cursor == nil {
		return errors.New("recovery activation cursor capture is absent")
	}
	installed := hashFingerprint(*recovered.ScopeStates[0].Cursor)
	if !slices.Equal(connect.CursorFingerprints, []string{installed}) {
		return errors.New("real recovery connect did not use its installed response-issued cursor")
	}
	for _, observation := range trace.Observations[1:] {
		if observation.OperationClass != "pull" {
			continue
		}
		version, versionErr := requestInteger(observation, "schema_version")
		hash, hashErr := requestString(observation, "schema_hash")
		if validateTraceOperation(observation, "pull") != nil || versionErr != nil || hashErr != nil || version != target.Version || hash != target.Hash || !slices.Equal(observation.CursorFingerprints, []string{installed}) {
			return errors.New("first target pull did not use the previously installed S2 replacement cursor")
		}
		return nil
	}
	return errors.New("schema proof omitted its first target pull")
}

func schemaCheckTraceWindow(before, after traceSnapshot) (traceSnapshot, error) {
	if before.Overflowed || after.Overflowed || before.SequenceCheckpoint != uint64(len(before.Observations)) || after.SequenceCheckpoint != uint64(len(after.Observations)) || after.SequenceCheckpoint < before.SequenceCheckpoint || validateTraceSequence(before.Observations) != nil || validateTraceSequence(after.Observations) != nil {
		return traceSnapshot{}, errors.New("schema dispatch transport window is incomplete")
	}
	for index, observation := range before.Observations {
		if !transportObservationsEqual(observation, after.Observations[index]) {
			return traceSnapshot{}, errors.New("schema dispatch transport prefix changed")
		}
	}
	return traceSnapshot{Observations: after.Observations[len(before.Observations):], SequenceCheckpoint: after.SequenceCheckpoint}, nil
}

func validateSchemaCheckDispatch(call schemaCheckCall, before, after finalCapture, action string, target clientSchema, scope string, affected bool) error {
	beforeTrace, err := captureTraceFromRaw(before.Trace)
	if err != nil {
		return err
	}
	afterTrace, err := captureTraceFromRaw(after.Trace)
	if err != nil {
		return err
	}
	window, err := schemaCheckTraceWindow(beforeTrace, afterTrace)
	if err != nil || len(window.Observations) == 0 {
		return errors.New("schema dispatch transport window is absent or invalid")
	}
	connect := window.Observations[0]
	facts, err := decodeConnectResponseFacts(connect.ConnectResponseFacts)
	if validateTraceOperation(connect, "connect") != nil || err != nil || !facts.AffectedScopesComplete || !facts.ScopeCursorUpdatesComplete || facts.Action != action || facts.SchemaVersion != target.Version || facts.SchemaHash != target.Hash {
		return errors.New("initial connect action or target schema differs from authored dispatch")
	}
	var beforeState inspectedClientState
	if json.Unmarshal(before.ClientState, &beforeState) != nil {
		return errors.New("schema dispatch source capture is invalid")
	}
	afterState, err := decodeClientState(after.ClientState)
	if err != nil {
		return err
	}
	for _, capture := range []inspectedClientState{beforeState, afterState} {
		if err := capture.requireCompleteMigrationCapture(); err != nil {
			return err
		}
		for _, truncated := range []*bool{capture.CaptureOverflowed, capture.ScopeStatesTruncated, capture.ScopeRowsTruncated, capture.RebuildAttemptsTruncated, capture.RebuildReceiptsTruncated, capture.RowMetadataTruncated} {
			if truncated != nil && *truncated {
				return errors.New("schema dispatch captured state is truncated")
			}
		}
		if capture.ApplicationRowCount > 256 {
			return errors.New("schema dispatch application capture is out of bounds")
		}
		for _, count := range []uint64{capture.MutationLedgerCount, capture.MutationOutcomeCount, capture.SealedBatchCount, capture.RejectedMutationCount, capture.ScopeStateCount, capture.ScopeRowCount, capture.ProvenanceCount, capture.RowMetadataCount, capture.RebuildAttemptCount, capture.RebuildReceiptCount} {
			if count > 512 {
				return errors.New("schema dispatch captured state is out of bounds")
			}
		}
	}
	source := clientSchema{}
	if beforeState.Schema != nil {
		source = *beforeState.Schema
	}
	var requestSchema struct {
		Version *uint64 `json:"schema_version"`
		Hash    *string `json:"schema_hash"`
	}
	if json.Unmarshal(connect.RequestFacts, &requestSchema) != nil || requestSchema.Version == nil || requestSchema.Hash == nil || *requestSchema.Version != source.Version || *requestSchema.Hash != source.Hash {
		return errors.New("connect did not present the captured source schema")
	}
	if len(beforeState.ScopeStates) > 1 || len(afterState.ScopeStates) != 1 || afterState.ScopeStates[0].ScopeID != scope {
		return errors.New("schema dispatch scope captures are incomplete")
	}
	var oldCursor *string
	if len(beforeState.ScopeStates) == 1 {
		if beforeState.ScopeStates[0].ScopeID != scope {
			return errors.New("schema dispatch source scope differs from authored scope")
		}
		oldCursor = beforeState.ScopeStates[0].Cursor
	}
	presented := []string{}
	if oldCursor != nil {
		presented = append(presented, hashFingerprint(*oldCursor))
	}
	if connect.CursorFingerprints == nil || connect.CursorFingerprintsComplete == nil || !*connect.CursorFingerprintsComplete || !slices.Equal(connect.CursorFingerprints, presented) {
		return errors.New("connect did not present the captured old cursor")
	}
	wantSchema := target
	// A final capture does not identify the schema activation cut.
	if action == "unsupported" {
		wantSchema = source
	}
	if *afterState.Schema != wantSchema {
		return errors.New("schema dispatch final schema differs from its accepted target")
	}
	var beforeEvents, afterEvents []json.RawMessage
	if json.Unmarshal(before.Events, &beforeEvents) != nil || json.Unmarshal(after.Events, &afterEvents) != nil || len(afterEvents) < len(beforeEvents) {
		return errors.New("schema dispatch event window is incomplete")
	}
	if len(beforeEvents) >= 256 || len(afterEvents) >= 256 {
		return errors.New("schema dispatch event ring is full")
	}
	for index, event := range beforeEvents {
		if !semanticRawJSONEqual(event, afterEvents[index]) {
			return errors.New("schema dispatch event prefix changed")
		}
	}
	schemaEvents := []string{}
	for _, raw := range afterEvents[len(beforeEvents):] {
		var event struct {
			Type   string        `json:"type"`
			Source *clientSchema `json:"source"`
			Target *clientSchema `json:"target"`
			Action *string       `json:"action"`
		}
		if json.Unmarshal(raw, &event) != nil {
			return errors.New("schema dispatch event is invalid")
		}
		if event.Type != "schema_applying" && event.Type != "schema_applied" {
			continue
		}
		if event.Source == nil || *event.Source != source || event.Target == nil || *event.Target != target || event.Action == nil || *event.Action != action {
			return errors.New("schema event source, target, or action differs from authored dispatch")
		}
		schemaEvents = append(schemaEvents, event.Type)
	}
	wantEvents := []string{}
	if action == "replace" || action == "rebuild_local" {
		wantEvents = []string{"schema_applying", "schema_applied"}
	}
	if !slices.Equal(schemaEvents, wantEvents) {
		return errors.New("schema dispatch event sequence differs from authored action")
	}
	scopeFingerprint := hashFingerprint(scope)
	wantAffected := []string{}
	if affected {
		wantAffected = append(wantAffected, scopeFingerprint)
	}
	if (action == "rebuild_local") != affected || !slices.Equal(facts.AffectedScopeFingerprints, wantAffected) {
		return errors.New("schema dispatch affected scopes differ from authored assignment")
	}
	for fingerprint := range facts.ScopeCursorUpdates {
		if fingerprint != scopeFingerprint {
			return errors.New("connect issued a cursor update for an unexpected scope")
		}
	}
	var parameters struct {
		SchemaCase string `json:"schema_case"`
	}
	if call.step.MeasurementSample != nil && json.Unmarshal(call.step.MeasurementSample.Parameters, &parameters) != nil {
		return errors.New("schema dispatch measurement case is invalid")
	}
	membershipRecovery := parameters.SchemaCase == "class_1"
	issued, hasUpdate := facts.ScopeCursorUpdates[scopeFingerprint]
	if affected && (!hasUpdate || issued != nil) || hasUpdate && issued == nil && !affected && !membershipRecovery {
		return errors.New("connect cursor reset differs from required affected rebuild")
	}
	if oldCursor != nil && source != target && !affected && action != "unsupported" && (!hasUpdate || issued == nil || *issued == presented[0]) {
		return errors.New("connect did not replace the historical unaffected cursor")
	}
	if action == "unsupported" {
		if len(window.Observations) != 1 || len(facts.ScopeCursorUpdates) != 0 {
			return errors.New("unsupported schema dispatch continued synchronization")
		}
		return nil
	}
	firstPull := -1
	var terminalBeforePull *string
	rebuilt := false
	for index, observation := range window.Observations[1:] {
		if observation.OperationClass != "pull" && observation.OperationClass != "rebuild" {
			continue
		}
		version, versionErr := requestInteger(observation, "schema_version")
		hash, hashErr := requestString(observation, "schema_hash")
		if versionErr != nil || hashErr != nil || version != target.Version || hash != target.Hash {
			return errors.New("pull or rebuild did not use the activated target schema")
		}
		if observation.OperationClass == "pull" && firstPull == -1 {
			firstPull = index + 1
		}
		if observation.OperationClass == "rebuild" {
			if oldCursor != nil && !affected && !membershipRecovery {
				return errors.New("unaffected schema dispatch caused an unnecessary rebuild")
			}
			fingerprint, err := requestString(observation, "scope_fingerprint")
			if err != nil || fingerprint != scopeFingerprint {
				return errors.New("schema rebuild scope differs from authored dispatch")
			}
			response, err := decodeRebuildResponseFacts(observation.RebuildResponseFacts)
			if observation.StatusCode == 200 && err == nil && response.HasFinalScopeCursor != nil && *response.HasFinalScopeCursor && response.FinalScopeCursorFingerprint != nil && response.ScopeFingerprint != nil && *response.ScopeFingerprint == scopeFingerprint {
				rebuilt = true
				if firstPull == -1 {
					terminalBeforePull = response.FinalScopeCursorFingerprint
				}
			}
		}
	}
	if firstPull == -1 || affected && !rebuilt {
		return errors.New("schema dispatch omitted its target pull or required affected rebuild")
	}
	pull := window.Observations[firstPull]
	if validateTraceOperation(pull, "pull") != nil {
		return errors.New("first target-schema pull cursor facts are incomplete")
	}
	wantCursors := presented
	if hasUpdate && issued != nil {
		wantCursors = []string{*issued}
	} else if affected || oldCursor == nil || hasUpdate && issued == nil {
		wantCursors = []string{}
		if terminalBeforePull != nil {
			wantCursors = []string{*terminalBeforePull}
		}
	} else if membershipRecovery && rebuilt {
		if terminalBeforePull != nil {
			wantCursors = []string{*terminalBeforePull}
		} else if len(pull.CursorFingerprints) == 0 {
			wantCursors = []string{}
		}
	}
	if !slices.Equal(pull.CursorFingerprints, wantCursors) {
		return errors.New("first target-schema pull did not use the response-issued replacement or rebuilt cursor")
	}
	return nil
}
