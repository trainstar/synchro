package scenarios

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

// VectorSetLookup provides the catalog boundary for scenario vector sets.
type VectorSetLookup interface {
	Has(contract.VectorSetID) bool
}

// wireCase describes the canonical wire result for one contract case.
type wireCase struct {
	status     int
	errorCode  string
	hasCode    bool
	retryable  bool
	operations []string
}

var wireCases = map[string]wireCase{
	"connect_success":           {status: 200, operations: []string{"connect"}},
	"push_success":              {status: 200, operations: []string{"push"}},
	"pull_success":              {status: 200, operations: []string{"pull"}},
	"rebuild_success":           {status: 200, operations: []string{"rebuild"}},
	"transport_failure":         {status: 0, retryable: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"invalid_request":           {status: 400, errorCode: "invalid_request", hasCode: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"invalid_schema_reference":  {status: 400, errorCode: "invalid_schema_reference", hasCode: true, operations: []string{"connect"}},
	"auth_required":             {status: 401, errorCode: "auth_required", hasCode: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"idempotency_conflict":      {status: 409, errorCode: "idempotency_conflict", hasCode: true, operations: []string{"push"}},
	"client_retired":            {status: 409, errorCode: "client_retired", hasCode: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"client_generation_expired": {status: 409, errorCode: "client_generation_expired", hasCode: true, operations: []string{"push", "pull", "rebuild"}},
	"rebuild_restart_required":  {status: 409, errorCode: "rebuild_restart_required", hasCode: true, operations: []string{"rebuild"}},
	"schema_mismatch":           {status: 422, errorCode: "schema_mismatch", hasCode: true, operations: []string{"push", "pull", "rebuild"}},
	"upgrade_required":          {status: 426, errorCode: "upgrade_required", hasCode: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"retry_later":               {status: 429, errorCode: "retry_later", hasCode: true, retryable: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"sync_integrity_failure":    {status: 500, errorCode: "sync_integrity_failure", hasCode: true, operations: []string{"connect", "push", "pull", "rebuild"}},
	"capture_pending":           {status: 503, errorCode: "capture_pending", hasCode: true, retryable: true, operations: []string{"pull", "rebuild"}},
	"temporary_unavailable":     {status: 503, errorCode: "temporary_unavailable", hasCode: true, retryable: true, operations: []string{"connect", "push", "pull", "rebuild"}},
}

var connectSchemaActions = stringSet([]string{
	"none",
	"replace",
	"rebuild_local",
	"unsupported",
})

var operationTransport = map[string]string{
	"connect":  "http",
	"push":     "http",
	"pull":     "http",
	"rebuild":  "http",
	"local":    "local",
	"artifact": "artifact",
	"process":  "process",
	"model":    "model",
	"workload": "model",
}

var predicateNames = map[string]map[string]struct{}{
	"state-equality":          {"state-equals-authored-model": {}, "state-unchanged": {}},
	"wire-outcome":            {"canonical-wire-outcome": {}},
	"state-transition":        {"legal-state-transition": {}, "schema-dispatch-observations-satisfied": {}},
	"artifact-integrity":      {"artifact-policy-satisfied": {}},
	"performance-measurement": {"performance-contract-satisfied": {}, "schema-dispatch-measurement-satisfied": {}},
}

var predicateByOracle = map[string]string{
	"model-state-equality": "state-equality",
	"wire-contract":        "wire-outcome",
	"state-transition":     "state-transition",
	"artifact-policy":      "artifact-integrity",
	"performance-budget":   "performance-measurement",
}

var proofTargetPolicy = map[string]map[string]struct{}{
	"reference-model":  {"test-conformance": {}},
	"server-black-box": {"test-blackbox": {}},
	"native-e2e": {
		"test-swift": {}, "test-kotlin": {}, "test-rn-e2e-ios": {}, "test-rn-e2e-android": {},
		"test-rn-warm-connect-ios": {}, "test-rn-warm-connect-android": {},
	},
	"fault-injection": {
		"test-blackbox": {}, "test-swift": {}, "test-kotlin": {}, "test-rn-e2e-ios": {}, "test-rn-e2e-android": {},
	},
	"negative-control": {"test-conformance": {}, "test-rn-warm-connect-control": {}},
}

type targetRule struct {
	component    string
	platform     string
	hasComponent bool
	hasPlatform  bool
}

var targetRules = map[string]targetRule{
	"test-conformance":    {},
	"test-blackbox":       {component: "postgresql-server", hasComponent: true},
	"test-swift":          {component: "swift-client", hasComponent: true},
	"test-kotlin":         {component: "kotlin-client", hasComponent: true},
	"test-rn-e2e-ios":     {component: "react-native-client", platform: "ios", hasComponent: true, hasPlatform: true},
	"test-rn-e2e-android": {component: "react-native-client", platform: "android", hasComponent: true, hasPlatform: true},
	"test-rn-warm-connect-ios": {
		component: "react-native-client", platform: "ios", hasComponent: true, hasPlatform: true,
	},
	"test-rn-warm-connect-android": {
		component: "react-native-client", platform: "android", hasComponent: true, hasPlatform: true,
	},
	"test-rn-warm-connect-control": {},
}

var targetRequiredRoles = map[string]map[string]struct{}{
	"test-conformance":    {"conformance-runner": {}},
	"test-blackbox":       {"pg-extension": {}, "adapter": {}},
	"test-swift":          {"pg-extension": {}, "adapter": {}, "swift-spm": {}},
	"test-kotlin":         {"pg-extension": {}, "adapter": {}, "kotlin-maven": {}},
	"test-rn-e2e-ios":     {"pg-extension": {}, "adapter": {}, "swift-spm": {}, "cocoapods": {}, "react-native-npm": {}},
	"test-rn-e2e-android": {"pg-extension": {}, "adapter": {}, "kotlin-maven": {}, "react-native-npm": {}},
	"test-rn-warm-connect-ios": {
		"pg-extension": {}, "adapter": {}, "swift-spm": {}, "cocoapods": {}, "react-native-npm": {},
	},
	"test-rn-warm-connect-android": {
		"pg-extension": {}, "adapter": {}, "kotlin-maven": {}, "react-native-npm": {},
	},
	"test-rn-warm-connect-control": {"conformance-runner": {}, "pg-extension": {}, "adapter": {}},
}

var targetAllowedRoles = map[string]map[string]struct{}{
	"test-blackbox":       {"pg-extension": {}, "pg-install-sql": {}, "adapter": {}, "seed-tool": {}, "portable-seed": {}},
	"test-swift":          {"pg-extension": {}, "adapter": {}, "seed-tool": {}, "swift-spm": {}, "cocoapods": {}, "portable-seed": {}},
	"test-kotlin":         {"pg-extension": {}, "adapter": {}, "seed-tool": {}, "kotlin-maven": {}, "portable-seed": {}},
	"test-rn-e2e-ios":     {"pg-extension": {}, "adapter": {}, "seed-tool": {}, "swift-spm": {}, "cocoapods": {}, "react-native-npm": {}, "portable-seed": {}},
	"test-rn-e2e-android": {"pg-extension": {}, "adapter": {}, "seed-tool": {}, "kotlin-maven": {}, "react-native-npm": {}, "portable-seed": {}},
	"test-rn-warm-connect-ios": {
		"pg-extension": {}, "adapter": {}, "seed-tool": {}, "swift-spm": {}, "cocoapods": {}, "react-native-npm": {}, "portable-seed": {},
	},
	"test-rn-warm-connect-android": {
		"pg-extension": {}, "adapter": {}, "seed-tool": {}, "kotlin-maven": {}, "react-native-npm": {}, "portable-seed": {},
	},
	"test-rn-warm-connect-control": {"conformance-runner": {}, "pg-extension": {}, "adapter": {}},
}

// Validate checks all semantic bindings in one scenario. It does not execute
// operations and does not depend on a production package or black-box runner.
func Validate(s Scenario, bundle *contract.Bundle) error {
	return ValidateWithVectors(s, bundle, nil)
}

// ValidateWithVectors checks one scenario against the contract and vector-set catalog.
func ValidateWithVectors(s Scenario, bundle *contract.Bundle, vectorSets VectorSetLookup) error {
	if bundle == nil {
		return errors.New("scenario semantic validation failed: contract bundle is nil")
	}

	v := scenarioValidator{scenario: s, bundle: bundle, vectorSets: vectorSets}
	v.validate()
	return joinScenarioErrors(v.errors)
}

// ValidateAll validates a selected scenario set and its cross-scenario closure.
func ValidateAll(scenarios []Scenario, bundle *contract.Bundle) error {
	return ValidateAllWithVectors(scenarios, bundle, nil)
}

// ValidateAllWithVectors validates a selected scenario set and its vector-set catalog.
func ValidateAllWithVectors(scenarios []Scenario, bundle *contract.Bundle, vectorSets VectorSetLookup) error {
	if bundle == nil {
		return errors.New("scenario semantic validation failed: contract bundle is nil")
	}

	var failures []error
	seenScenarioIDs := make(map[contract.ScenarioID]string, len(scenarios))
	selectedRequirementOwners := make(map[contract.RequirementID][]string)
	requirementNegativeOwners := make(map[contract.RequirementID][]string)
	controlNegativeOwners := make(map[contract.ControlID][]string)
	selectedProofKeys := make(map[string][]string)
	selectedIDs := make(map[contract.ScenarioID]struct{}, len(scenarios))

	for _, scenario := range scenarios {
		if previous, duplicate := seenScenarioIDs[scenario.ID]; duplicate {
			failures = append(failures, fmt.Errorf("duplicate scenario ID %q in %s and %s", scenario.ID, previous, scenario.ID))
		} else {
			seenScenarioIDs[scenario.ID] = string(scenario.ID)
		}
		selectedIDs[scenario.ID] = struct{}{}
		if err := ValidateWithVectors(scenario, bundle, vectorSets); err != nil {
			failures = append(failures, err)
		}
		for _, requirementID := range scenario.RequirementIDs {
			selectedRequirementOwners[requirementID] = append(selectedRequirementOwners[requirementID], string(scenario.ID))
		}
		for _, obligation := range scenario.ProofObligations {
			for _, requirementID := range obligation.RequirementIDs {
				key := fmt.Sprintf("%s|%s|%s", requirementID, obligation.ProofType, nullableSupportCell(obligation.SupportCellID))
				selectedProofKeys[key] = append(selectedProofKeys[key], fmt.Sprintf("%s/%s", scenario.ID, obligation.ObligationID))
			}
		}
		for _, obligation := range scenario.ProofObligations {
			if obligation.ProofType != "negative-control" || len(obligation.RequirementIDs) != 1 {
				continue
			}
			requirementID := obligation.RequirementIDs[0]
			owner := fmt.Sprintf("%s/%s", scenario.ID, obligation.ObligationID)
			requirementNegativeOwners[requirementID] = append(requirementNegativeOwners[requirementID], owner)
			if obligation.ControlID != nil {
				controlNegativeOwners[*obligation.ControlID] = append(controlNegativeOwners[*obligation.ControlID], owner)
			}
		}
	}

	for _, requirementID := range sortedRequirementKeys(selectedRequirementOwners) {
		owners := selectedRequirementOwners[requirementID]
		if len(owners) != 1 {
			failures = append(failures, fmt.Errorf("selected requirement %s is owned by %d scenarios: %s", requirementID, len(owners), sortedOwnerText(owners)))
		}
	}

	// Aggregate the proof keys across the selected scenarios. A requirement
	// remains atomic at the selected-set boundary, even when each scenario is
	// valid in isolation.
	for _, requirementID := range sortedRequirementKeys(selectedRequirementOwners) {
		requirement, known := requirementByID(bundle, requirementID)
		if !known {
			continue
		}
		requiredProofs := stringSet(requirement.RequiredProofTypes)
		expectedKeys := make(map[string]struct{})
		for _, proofType := range sortedStrings(requirement.RequiredProofTypes) {
			switch proofType {
			case "server-black-box":
				if contains(requirement.ApplicableComponents, "postgresql-server") {
					for _, cell := range requiredSupportCellsFor(bundle, "postgresql-server") {
						expectedKeys[fmt.Sprintf("%s|%s|%s", requirementID, proofType, cell)] = struct{}{}
					}
				}
			case "native-e2e":
				for _, component := range []string{"swift-client", "kotlin-client", "react-native-client"} {
					if !contains(requirement.ApplicableComponents, component) {
						continue
					}
					for _, cell := range semanticSupportCellsFor(bundle, component) {
						expectedKeys[fmt.Sprintf("%s|%s|%s", requirementID, proofType, cell)] = struct{}{}
					}
				}
			case "reference-model", "negative-control":
				expectedKeys[fmt.Sprintf("%s|%s|null", requirementID, proofType)] = struct{}{}
			case "fault-injection":
				// Fault-injection closure is checked after all authored keys are known.
			}
		}
		for key := range expectedKeys {
			owners := selectedProofKeys[key]
			if len(owners) != 1 {
				failures = append(failures, fmt.Errorf("selected proof key %s has %d owners: %s", key, len(owners), sortedOwnerText(owners)))
			}
		}
		faultInjectionOwners := 0
		for key, owners := range selectedProofKeysForRequirement(selectedProofKeys, requirementID) {
			parts := strings.Split(key, "|")
			if len(parts) < 3 {
				continue
			}
			proofType := parts[1]
			if proofType == "fault-injection" {
				faultInjectionOwners += len(owners)
				if len(owners) > 1 {
					failures = append(failures, fmt.Errorf("selected requirement %s has multiple fault-injection obligations: %s", requirementID, sortedOwnerText(owners)))
				}
				continue
			}
			if _, required := requiredProofs[proofType]; !required {
				failures = append(failures, fmt.Errorf("selected requirement %s has non-required proof type %s", requirementID, proofType))
			}
		}
		faultRequired := false
		if _, required := requiredProofs["fault-injection"]; required {
			faultRequired = true
		}
		postgresFaultCells := selectedPostgresFaultCells(selectedProofKeys, requirementID, bundle)
		if len(postgresFaultCells) > 0 {
			requiredPostgresCells := requiredSupportCellsFor(bundle, "postgresql-server")
			if !supportCellSetsEqual(postgresFaultCells, supportCellIDs(requiredPostgresCells)) || faultInjectionOwners != len(requiredPostgresCells) {
				failures = append(failures, fmt.Errorf("selected requirement %s PostgreSQL fault-injection obligations must exactly cover required extension architecture cells", requirementID))
			}
		} else if faultRequired && faultInjectionOwners != 1 {
			failures = append(failures, fmt.Errorf("selected requirement %s requires exactly one fault-injection obligation, found %d", requirementID, faultInjectionOwners))
		} else if !faultRequired && faultInjectionOwners > 1 {
			failures = append(failures, fmt.Errorf("selected requirement %s has multiple optional fault-injection obligations, found %d", requirementID, faultInjectionOwners))
		}
		catalogControlIDs := make([]contract.ControlID, 0, 1)
		for _, control := range bundle.Faults.Controls {
			if len(control.RequirementIDs) == 1 && control.RequirementIDs[0] == requirementID {
				catalogControlIDs = append(catalogControlIDs, control.ID)
			}
		}
		if len(catalogControlIDs) != 1 {
			failures = append(failures, fmt.Errorf("selected requirement %s must have exactly one catalog negative control, found %d", requirementID, len(catalogControlIDs)))
		} else if len(controlNegativeOwners[catalogControlIDs[0]]) != 1 {
			owners := controlNegativeOwners[catalogControlIDs[0]]
			failures = append(failures, fmt.Errorf("selected control %s must have exactly one owner, found %d: %s", catalogControlIDs[0], len(owners), sortedOwnerText(owners)))
		}
	}

	for _, requirement := range sortedRequirementKeys(requirementNegativeOwners) {
		owners := requirementNegativeOwners[requirement]
		if len(owners) != 1 {
			failures = append(failures, fmt.Errorf("negative-control requirement %s is owned by %d obligations: %s", requirement, len(owners), sortedOwnerText(owners)))
		}
	}
	for _, control := range sortedControlKeys(controlNegativeOwners) {
		owners := controlNegativeOwners[control]
		if len(owners) != 1 {
			failures = append(failures, fmt.Errorf("negative control %s is reused by %d obligations: %s", control, len(owners), sortedOwnerText(owners)))
		}
	}

	// A selected requirement must have one negative-control proof globally,
	// even when its scenario-local validation already reported a defect.
	for _, scenario := range scenarios {
		for _, requirementID := range uniqueRequirementIDs(scenario.RequirementIDs) {
			if len(requirementNegativeOwners[requirementID]) != 1 {
				failures = append(failures, fmt.Errorf("selected requirement %s does not have exactly one negative-control obligation", requirementID))
			}
		}
	}

	for _, budget := range bundle.Performance.Budgets {
		if _, selected := selectedIDs[budget.ScenarioID]; !selected {
			failures = append(failures, fmt.Errorf("performance budget %s belongs to absent scenario %s", budget.ID, budget.ScenarioID))
		}
	}
	for _, measurement := range bundle.Performance.RequiredMeasurements {
		if _, selected := selectedIDs[measurement.ScenarioID]; !selected {
			failures = append(failures, fmt.Errorf("required measurement %s belongs to absent scenario %s", measurement.ID, measurement.ScenarioID))
		}
	}

	return joinScenarioErrors(failures)
}

type scenarioValidator struct {
	scenario   Scenario
	bundle     *contract.Bundle
	vectorSets VectorSetLookup
	errors     []error

	steps        map[StepID]Step
	assertions   map[contract.AssertionID]Assertion
	expectations map[ExpectationID]ModelExpectation
	barriers     map[BarrierID]Barrier
	plans        map[contract.FaultPlanID]FaultPlan
	controls     map[contract.ControlID]NegativeControl
	obligations  map[contract.ObligationID]ProofObligation
	wireByStep   map[StepID]WireExpectation
}

func (v *scenarioValidator) add(format string, args ...any) {
	v.errors = append(v.errors, fmt.Errorf(format, args...))
}

func (v *scenarioValidator) validate() {
	v.indexAndCheckDuplicates()
	v.validateOperationsAndWire()
	v.validateAssertionsAndObligations()
	v.validateReplayAndBarriers()
	v.validateFaultsAndControls()
	v.validateNormativeReferences()
	v.validateTargetsAndArtifacts()
	v.validateRequiredProofs()
	v.validatePerformance()
	v.validateNativeProof()
	v.validateOwnership()
}

func (v *scenarioValidator) indexAndCheckDuplicates() {
	v.steps = make(map[StepID]Step, len(v.scenario.Steps))
	for _, step := range v.scenario.Steps {
		if _, exists := v.steps[step.ID]; exists {
			v.add("%s duplicate step ID %q", v.scenario.ID, step.ID)
		} else {
			v.steps[step.ID] = step
		}
	}
	v.assertions = make(map[contract.AssertionID]Assertion, len(v.scenario.Assertions))
	for _, assertion := range v.scenario.Assertions {
		if _, exists := v.assertions[assertion.ID]; exists {
			v.add("%s duplicate assertion ID %q", v.scenario.ID, assertion.ID)
		} else {
			v.assertions[assertion.ID] = assertion
		}
	}
	v.expectations = make(map[ExpectationID]ModelExpectation, len(v.scenario.Model.ExpectedState))
	for _, expectation := range v.scenario.Model.ExpectedState {
		if _, exists := v.expectations[expectation.ID]; exists {
			v.add("%s duplicate model expectation ID %q", v.scenario.ID, expectation.ID)
		} else {
			v.expectations[expectation.ID] = expectation
		}
	}
	v.barriers = make(map[BarrierID]Barrier, len(v.scenario.BarrierPlan.Barriers))
	for _, barrier := range v.scenario.BarrierPlan.Barriers {
		if _, exists := v.barriers[barrier.ID]; exists {
			v.add("%s duplicate barrier ID %q", v.scenario.ID, barrier.ID)
		} else {
			v.barriers[barrier.ID] = barrier
		}
	}
	v.plans = make(map[contract.FaultPlanID]FaultPlan, len(v.scenario.FaultPlans))
	for _, plan := range v.scenario.FaultPlans {
		if _, exists := v.plans[plan.ID]; exists {
			v.add("%s duplicate fault plan ID %q", v.scenario.ID, plan.ID)
		} else {
			v.plans[plan.ID] = plan
		}
	}
	v.controls = make(map[contract.ControlID]NegativeControl, len(v.scenario.NegativeControls))
	for _, control := range v.scenario.NegativeControls {
		if _, exists := v.controls[control.ControlID]; exists {
			v.add("%s duplicate negative control ID %q", v.scenario.ID, control.ControlID)
		} else {
			v.controls[control.ControlID] = control
		}
	}
	v.obligations = make(map[contract.ObligationID]ProofObligation, len(v.scenario.ProofObligations))
	for _, obligation := range v.scenario.ProofObligations {
		if _, exists := v.obligations[obligation.ObligationID]; exists {
			v.add("%s duplicate proof obligation ID %q", v.scenario.ID, obligation.ObligationID)
		} else {
			v.obligations[obligation.ObligationID] = obligation
		}
	}
	v.wireByStep = make(map[StepID]WireExpectation, len(v.scenario.WireExpectations))
	for _, wire := range v.scenario.WireExpectations {
		if _, exists := v.wireByStep[wire.StepID]; exists {
			v.add("%s duplicate wire expectation binding for step %q", v.scenario.ID, wire.StepID)
		} else {
			v.wireByStep[wire.StepID] = wire
		}
	}
	seenOwnership := make(map[string]struct{}, len(v.scenario.Ownership))
	for _, ownership := range v.scenario.Ownership {
		key := ownershipKey(ownership)
		if _, exists := seenOwnership[key]; exists {
			v.add("%s duplicate ownership tuple %q", v.scenario.ID, key)
		} else {
			seenOwnership[key] = struct{}{}
		}
	}
}

func (v *scenarioValidator) validateOperationsAndWire() {
	if len(v.scenario.Model.Setup) != 1 {
		v.add("%s model setup must contain exactly one operation", v.scenario.ID)
	}
	for index, operation := range v.scenario.Model.Setup {
		if operation.ContractOperation != "model" || operation.Name != "install-current-contract" {
			v.add("%s model setup operation %d has unknown operation %s/%s", v.scenario.ID, index, operation.ContractOperation, operation.Name)
			continue
		}
		if err := ValidateOperation(operation); err != nil {
			v.add("%s model setup operation %d is invalid: %v", v.scenario.ID, index, err)
		}
	}
	for _, step := range v.scenario.Steps {
		if err := ValidateOperation(step.Operation); err != nil {
			v.add("%s step %s has invalid operation: %v", v.scenario.ID, step.ID, err)
		}
		if expected, known := operationTransport[step.Operation.ContractOperation]; !known {
			v.add("%s step %s has unknown contract operation %q", v.scenario.ID, step.ID, step.Operation.ContractOperation)
		} else if step.Transport != expected {
			v.add("%s step %s transport %q does not match contract operation %q", v.scenario.ID, step.ID, step.Transport, step.Operation.ContractOperation)
		}
		if err := validateExpectedOutcome(step.ExpectedOutcome); err != nil {
			v.add("%s step %s has invalid expected outcome: %v", v.scenario.ID, step.ID, err)
		}
	}

	var httpStepIDs []string
	for _, step := range v.scenario.Steps {
		if step.Transport == "http" {
			httpStepIDs = append(httpStepIDs, string(step.ID))
		}
	}
	var wireStepIDs []string
	for _, wire := range v.scenario.WireExpectations {
		wireStepIDs = append(wireStepIDs, string(wire.StepID))
	}
	if !stringSetEqual(httpStepIDs, wireStepIDs) {
		v.add("%s HTTP steps and wire expectations are not an exact closure", v.scenario.ID)
	}
	for _, wire := range v.scenario.WireExpectations {
		step, stepExists := v.steps[wire.StepID]
		if !stepExists || step.Transport != "http" {
			v.add("%s wire expectation references non-HTTP step %s", v.scenario.ID, wire.StepID)
		}
		assertion, assertionExists := v.assertions[wire.AssertionID]
		if !assertionExists {
			v.add("%s wire expectation references unknown assertion %s", v.scenario.ID, wire.AssertionID)
		} else if assertion.Oracle.Kind != "wire-contract" {
			v.add("%s wire assertion %s must use the wire-contract oracle", v.scenario.ID, wire.AssertionID)
		}
		policy, known := wireCases[wire.ContractCase]
		if !known {
			v.add("%s wire expectation has unknown contract case %q", v.scenario.ID, wire.ContractCase)
			continue
		}
		actualCode, actualHasCode := "", wire.ErrorCode != nil
		if actualHasCode {
			actualCode = *wire.ErrorCode
		}
		if wire.HTTPStatus != policy.status || actualHasCode != policy.hasCode || actualCode != policy.errorCode || wire.Retryable != policy.retryable {
			v.add("%s wire expectation %s does not match canonical status, error code, and retryability", v.scenario.ID, wire.ContractCase)
		}
		if stepExists {
			if !contains(policy.operations, step.Operation.ContractOperation) {
				v.add("%s wire expectation %s is invalid for contract operation %s", v.scenario.ID, wire.ContractCase, step.Operation.ContractOperation)
			}
		}
		v.validateWireAction(step, stepExists, wire)
	}
}

func (v *scenarioValidator) validateWireAction(step Step, stepExists bool, wire WireExpectation) {
	if wire.Action == "" {
		return
	}
	if _, known := connectSchemaActions[wire.Action]; !known {
		v.add("%s wire expectation has unknown connect schema action %q", v.scenario.ID, wire.Action)
	}
	if !stepExists || step.Operation.ContractOperation != "connect" || wire.ContractCase != "connect_success" {
		v.add("%s wire expectation action %q requires a connect_success connect outcome", v.scenario.ID, wire.Action)
	}
}

func validateExpectedOutcome(outcome ExpectedOutcome) error {
	switch outcome.Disposition {
	case "success":
		if outcome.ErrorCode != nil {
			return errors.New("success cannot contain error_code")
		}
	case "error":
		if outcome.ErrorCode == nil {
			return errors.New("error requires error_code")
		}
		switch *outcome.ErrorCode {
		case "source_transaction_predecessor_pending", "source_transaction_poison_blocked":
		default:
			return fmt.Errorf("unknown error_code %q", *outcome.ErrorCode)
		}
	default:
		return fmt.Errorf("unknown disposition %q", outcome.Disposition)
	}
	return nil
}

func (v *scenarioValidator) validateAssertionsAndObligations() {
	if !stringSetEqual(v.scenario.ProofTypes, uniqueStrings(proofTypes(v.scenario.ProofObligations))) {
		v.add("%s proof types do not exactly match proof obligations", v.scenario.ID)
	}
	scenarioRequirements := stringSet(stringIDs(v.scenario.RequirementIDs))
	obligationRequirements := make(map[string]struct{})
	assertionRequirements := make(map[string]struct{})
	boundAssertions := make(map[contract.AssertionID]struct{})
	referencedExpectations := make(map[ExpectationID]struct{})
	proofKeys := make(map[string]contract.ObligationID)

	for _, obligation := range v.scenario.ProofObligations {
		for _, requirementID := range obligation.RequirementIDs {
			obligationRequirements[string(requirementID)] = struct{}{}
			if _, selected := scenarioRequirements[string(requirementID)]; !selected {
				v.add("%s obligation %s references requirement %s outside the scenario", v.scenario.ID, obligation.ObligationID, requirementID)
			}
			key := fmt.Sprintf("%s|%s|%s", requirementID, obligation.ProofType, nullableSupportCell(obligation.SupportCellID))
			if previous, duplicate := proofKeys[key]; duplicate {
				v.add("%s has duplicate obligation proof key %s in %s and %s", v.scenario.ID, key, previous, obligation.ObligationID)
			} else {
				proofKeys[key] = obligation.ObligationID
			}
		}
		assertedRequirements := make(map[string]struct{})
		for _, assertionID := range obligation.AssertionIDs {
			boundAssertions[assertionID] = struct{}{}
			assertion, exists := v.assertions[assertionID]
			if !exists {
				v.add("%s obligation %s references unknown assertion %s", v.scenario.ID, obligation.ObligationID, assertionID)
				continue
			}
			for _, requirementID := range assertion.RequirementIDs {
				assertedRequirements[string(requirementID)] = struct{}{}
			}
		}
		if !stringSetEqual(keysOfStringSet(assertedRequirements), stringIDs(obligation.RequirementIDs)) {
			v.add("%s obligation %s requirement IDs do not exactly match its assertions", v.scenario.ID, obligation.ObligationID)
		}
		if obligation.ProofType == "fault-injection" || obligation.ProofType == "negative-control" {
			if len(obligation.RequirementIDs) != 1 {
				v.add("%s obligation %s %s must own exactly one requirement", v.scenario.ID, obligation.ObligationID, obligation.ProofType)
			}
			if len(obligation.AssertionIDs) != 1 {
				v.add("%s obligation %s %s must own exactly one assertion", v.scenario.ID, obligation.ObligationID, obligation.ProofType)
			}
			if obligation.FaultPlanID == nil || obligation.ControlID == nil {
				v.add("%s obligation %s %s must bind one fault plan and one control", v.scenario.ID, obligation.ObligationID, obligation.ProofType)
			}
		} else if obligation.FaultPlanID != nil || obligation.ControlID != nil {
			v.add("%s obligation %s non-fault proof must bind null fault_plan_id and control_id", v.scenario.ID, obligation.ObligationID)
		}
	}
	if !stringSetEqual(stringIDs(v.scenario.RequirementIDs), keysOfStringSet(obligationRequirements)) {
		v.add("%s requirement IDs do not exactly match its proof obligations", v.scenario.ID)
	}

	for _, assertion := range v.scenario.Assertions {
		if _, bound := boundAssertions[assertion.ID]; !bound {
			v.add("%s assertion %s is not bound to a proof obligation", v.scenario.ID, assertion.ID)
		}
		for _, requirementID := range assertion.RequirementIDs {
			assertionRequirements[string(requirementID)] = struct{}{}
			if _, selected := scenarioRequirements[string(requirementID)]; !selected {
				v.add("%s assertion %s references requirement %s outside the scenario", v.scenario.ID, assertion.ID, requirementID)
			}
		}
		if expectedPredicate, known := predicateByOracle[assertion.Oracle.Kind]; !known {
			v.add("%s assertion %s has unknown oracle kind %q", v.scenario.ID, assertion.ID, assertion.Oracle.Kind)
		} else if assertion.Predicate.ContractPredicate != expectedPredicate {
			v.add("%s assertion %s contract predicate does not match oracle %s", v.scenario.ID, assertion.ID, assertion.Oracle.Kind)
		}
		if names, known := predicateNames[assertion.Predicate.ContractPredicate]; !known {
			v.add("%s assertion %s has unknown contract predicate %q", v.scenario.ID, assertion.ID, assertion.Predicate.ContractPredicate)
		} else if _, knownName := names[assertion.Predicate.Name]; !knownName {
			v.add("%s assertion %s has unknown predicate name %q", v.scenario.ID, assertion.ID, assertion.Predicate.Name)
		}
		if assertion.Oracle.ExpectedSource != "authored-model" {
			v.add("%s assertion %s expected source must be authored-model", v.scenario.ID, assertion.ID)
		}
		if assertion.Oracle.ObservedSource != "system-under-test" && assertion.Oracle.ObservedSource != "generated-artifact" {
			v.add("%s assertion %s has invalid observed source %q", v.scenario.ID, assertion.ID, assertion.Oracle.ObservedSource)
		}
		for _, expectationID := range assertion.ExpectationIDs {
			referencedExpectations[expectationID] = struct{}{}
			if _, exists := v.expectations[expectationID]; !exists {
				v.add("%s assertion %s references unknown model expectation %s", v.scenario.ID, assertion.ID, expectationID)
			}
		}
	}
	if !stringSetEqual(stringIDs(v.scenario.RequirementIDs), keysOfStringSet(assertionRequirements)) {
		v.add("%s requirement IDs do not exactly match its assertions", v.scenario.ID)
	}
	for expectationID := range v.expectations {
		if _, referenced := referencedExpectations[expectationID]; !referenced {
			v.add("%s model expectation %s is not bound to an assertion", v.scenario.ID, expectationID)
		}
	}
	for _, expectation := range v.scenario.Model.ExpectedState {
		if names, known := predicateNames[expectation.Predicate.ContractPredicate]; !known {
			v.add("%s model expectation %s has unknown contract predicate %q", v.scenario.ID, expectation.ID, expectation.Predicate.ContractPredicate)
		} else if _, knownName := names[expectation.Predicate.Name]; !knownName {
			v.add("%s model expectation %s has unknown predicate name %q", v.scenario.ID, expectation.ID, expectation.Predicate.Name)
		}
		if expectation.Predicate.Name == "state-equals-authored-model" {
			if expectation.StateFacts == nil {
				v.add("%s model expectation %s must contain authored state facts", v.scenario.ID, expectation.ID)
			}
		} else if expectation.StateFacts != nil {
			v.add("%s model expectation %s has state facts for a non-state predicate", v.scenario.ID, expectation.ID)
		}
	}
}

func (v *scenarioValidator) validateReplayAndBarriers() {
	if v.scenario.Replay.Mode == "randomized" && !v.scenario.Replay.SeedRequired {
		v.add("%s randomized replay must require a seed", v.scenario.ID)
	}
	if len(v.scenario.BarrierPlan.Barriers) > 0 && !v.scenario.Replay.BarrierTraceRequired {
		v.add("%s authored barriers require a barrier trace", v.scenario.ID)
	}
	orders := make([]int, 0, len(v.scenario.BarrierPlan.Barriers))
	for _, barrier := range v.scenario.BarrierPlan.Barriers {
		orders = append(orders, barrier.ReleaseOrder)
	}
	sort.Ints(orders)
	for index, order := range orders {
		want := index + 1
		if order != want {
			v.add("%s barrier release_order values must be unique and contiguous from 1", v.scenario.ID)
			break
		}
	}
}

func (v *scenarioValidator) validateFaultsAndControls() {
	assertionIDs := make(map[contract.AssertionID]struct{}, len(v.assertions))
	for id := range v.assertions {
		assertionIDs[id] = struct{}{}
	}
	barrierIDs := make(map[BarrierID]struct{}, len(v.barriers))
	for id := range v.barriers {
		barrierIDs[id] = struct{}{}
	}
	catalogFaults := make(map[contract.FaultID]struct{}, len(v.bundle.Faults.Faults))
	for _, fault := range v.bundle.Faults.Faults {
		catalogFaults[fault.ID] = struct{}{}
	}
	catalogControls := make(map[contract.ControlID]contract.Control, len(v.bundle.Faults.Controls))
	for _, control := range v.bundle.Faults.Controls {
		catalogControls[control.ID] = control
	}
	planReferences := make(map[contract.FaultPlanID]int)
	negativeObligationCounts := make(map[contract.ControlID]int)

	for _, plan := range v.scenario.FaultPlans {
		if _, exists := catalogFaults[plan.FaultID]; !exists {
			v.add("%s fault plan %s references unknown fault %s", v.scenario.ID, plan.ID, plan.FaultID)
		}
		catalogControl, controlExists := catalogControls[plan.ControlID]
		if !controlExists {
			v.add("%s fault plan %s references unknown catalog control %s", v.scenario.ID, plan.ID, plan.ControlID)
		} else {
			if catalogControl.FaultID != plan.FaultID {
				v.add("%s fault plan %s does not match catalog control fault", v.scenario.ID, plan.ID)
			}
			if len(catalogControl.RequirementIDs) != 1 || catalogControl.RequirementIDs[0] != plan.RequirementID {
				v.add("%s fault plan %s requirement does not match catalog control ownership", v.scenario.ID, plan.ID)
			}
			if !sameInjection(plan.Injection, catalogControl.Injection) {
				v.add("%s fault plan %s injection recipe does not match catalog control", v.scenario.ID, plan.ID)
			}
		}
		if _, selected := stringSet(stringIDs(v.scenario.RequirementIDs))[string(plan.RequirementID)]; !selected {
			v.add("%s fault plan %s references requirement %s outside the scenario", v.scenario.ID, plan.ID, plan.RequirementID)
		}
		if _, exists := barrierIDs[plan.BarrierID]; !exists {
			v.add("%s fault plan %s references unknown barrier %s", v.scenario.ID, plan.ID, plan.BarrierID)
		}
		for _, assertionID := range plan.ExpectedAssertionIDs {
			if _, exists := assertionIDs[assertionID]; !exists {
				v.add("%s fault plan %s references unknown assertion %s", v.scenario.ID, plan.ID, assertionID)
			}
		}
	}

	for _, obligation := range v.scenario.ProofObligations {
		if obligation.ProofType != "fault-injection" && obligation.ProofType != "negative-control" {
			continue
		}
		if obligation.FaultPlanID == nil || obligation.ControlID == nil || len(obligation.RequirementIDs) != 1 {
			continue
		}
		planReferences[*obligation.FaultPlanID]++
		if obligation.ProofType == "negative-control" {
			negativeObligationCounts[*obligation.ControlID]++
		}
		plan, planExists := v.plans[*obligation.FaultPlanID]
		control, controlExists := v.controls[*obligation.ControlID]
		if !planExists {
			v.add("%s obligation %s references unknown fault plan %s", v.scenario.ID, obligation.ObligationID, *obligation.FaultPlanID)
		}
		if !controlExists {
			v.add("%s obligation %s references unknown scenario control %s", v.scenario.ID, obligation.ObligationID, *obligation.ControlID)
		}
		if planExists && plan.ControlID != *obligation.ControlID {
			v.add("%s obligation %s fault plan does not exactly bind control %s", v.scenario.ID, obligation.ObligationID, *obligation.ControlID)
		}
		if planExists && !stringSetEqual(stringIDs(obligation.AssertionIDs), stringIDs(plan.ExpectedAssertionIDs)) {
			v.add("%s obligation %s assertions do not exactly match fault plan", v.scenario.ID, obligation.ObligationID)
		}
		if planExists && plan.RequirementID != obligation.RequirementIDs[0] {
			v.add("%s obligation %s fault plan does not exactly bind its requirement", v.scenario.ID, obligation.ObligationID)
		}
		if controlExists {
			if control.RequirementID != obligation.RequirementIDs[0] {
				v.add("%s obligation %s scenario control does not exactly bind its requirement", v.scenario.ID, obligation.ObligationID)
			}
			if !stringSetEqual(stringIDs(obligation.AssertionIDs), stringIDs(control.DetectedBy)) {
				v.add("%s obligation %s assertions do not exactly match scenario control", v.scenario.ID, obligation.ObligationID)
			}
		}
	}
	for _, plan := range v.scenario.FaultPlans {
		if planReferences[plan.ID] == 0 {
			v.add("%s fault plan %s is orphaned", v.scenario.ID, plan.ID)
		}
	}

	expectedControls := make(map[contract.ControlID]struct{})
	selectedRequirements := stringSet(stringIDs(v.scenario.RequirementIDs))
	for _, control := range v.bundle.Faults.Controls {
		if len(control.RequirementIDs) == 1 {
			if _, selected := selectedRequirements[string(control.RequirementIDs[0])]; selected {
				expectedControls[control.ID] = struct{}{}
			}
		}
	}
	actualControls := make([]string, 0, len(v.scenario.NegativeControls))
	for _, control := range v.scenario.NegativeControls {
		actualControls = append(actualControls, string(control.ControlID))
		catalogControl, exists := catalogControls[control.ControlID]
		if !exists {
			v.add("%s negative control %s is not in the catalog", v.scenario.ID, control.ControlID)
		} else {
			if len(catalogControl.RequirementIDs) != 1 || catalogControl.RequirementIDs[0] != control.RequirementID {
				v.add("%s negative control %s requirement does not match catalog ownership", v.scenario.ID, control.ControlID)
			}
			if catalogControl.FaultID != control.FaultID {
				v.add("%s negative control %s fault does not match catalog control", v.scenario.ID, control.ControlID)
			}
		}
		if _, selected := selectedRequirements[string(control.RequirementID)]; !selected {
			v.add("%s negative control %s references a requirement outside the scenario", v.scenario.ID, control.ControlID)
		}
		for _, artifactID := range control.SubjectArtifactInventoryIDs {
			artifact, known := artifactByID(v.bundle, artifactID)
			if !known {
				v.add("%s negative control %s references unknown subject artifact %s", v.scenario.ID, control.ControlID, artifactID)
			} else if artifact.Role == "conformance-runner" {
				v.add("%s negative control %s cannot use the conformance runner as its mutated subject", v.scenario.ID, control.ControlID)
			}
		}
		for _, assertionID := range control.DetectedBy {
			assertion, known := v.assertions[assertionID]
			if !known {
				v.add("%s negative control %s references unknown detection assertion %s", v.scenario.ID, control.ControlID, assertionID)
				continue
			}
			if !contains(stringIDs(assertion.DetectsControlIDs), string(control.ControlID)) {
				v.add("%s assertion %s does not reciprocally detect control %s", v.scenario.ID, assertionID, control.ControlID)
			}
			if !contains(stringIDs(assertion.RequirementIDs), string(control.RequirementID)) {
				v.add("%s assertion %s does not assert control requirement %s", v.scenario.ID, assertionID, control.RequirementID)
			}
		}
		plans := 0
		for _, plan := range v.scenario.FaultPlans {
			if plan.ControlID == control.ControlID {
				plans++
			}
		}
		if plans != 1 {
			v.add("%s negative control %s must have exactly one fault plan, found %d", v.scenario.ID, control.ControlID, plans)
		}
		if negativeObligationCounts[control.ControlID] != 1 {
			v.add("%s negative control %s must have exactly one negative-control obligation, found %d", v.scenario.ID, control.ControlID, negativeObligationCounts[control.ControlID])
		}
	}
	if !stringSetEqual(actualControls, keysOfControlSet(expectedControls)) {
		v.add("%s negative controls do not exactly match its selected requirements", v.scenario.ID)
	}
	for _, assertion := range v.scenario.Assertions {
		for _, controlID := range assertion.DetectsControlIDs {
			control, exists := v.controls[controlID]
			if !exists {
				v.add("%s assertion %s detects unknown control %s", v.scenario.ID, assertion.ID, controlID)
			} else if !contains(stringIDs(control.DetectedBy), string(assertion.ID)) {
				v.add("%s assertion %s detection of %s is not reciprocal", v.scenario.ID, assertion.ID, controlID)
			}
		}
	}
}

func (v *scenarioValidator) validateNormativeReferences() {
	required := make(map[string]struct{})
	seenRequirements := stringSet(stringIDs(v.scenario.RequirementIDs))
	for _, requirement := range v.bundle.Requirements.Requirements {
		if _, selected := seenRequirements[string(requirement.ID)]; !selected {
			continue
		}
		for _, reference := range requirement.NormativeReferences {
			required[normativeReferenceKey(reference)] = struct{}{}
		}
	}
	actual := make(map[string]struct{})
	for _, reference := range v.scenario.NormativeReferences {
		key := normativeReferenceKey(reference)
		if _, duplicate := actual[key]; duplicate {
			v.add("%s has duplicate normative reference %s", v.scenario.ID, key)
		} else {
			actual[key] = struct{}{}
		}
	}
	for key := range required {
		if _, present := actual[key]; !present {
			v.add("%s normative references omit mandatory requirement anchor %s", v.scenario.ID, key)
		}
	}
	if err := v.bundle.ValidateNormativeReferences(v.scenario.NormativeReferences); err != nil {
		v.add("%s normative references are not in the frozen contract snapshot: %v", v.scenario.ID, err)
	}
}

func (v *scenarioValidator) validateTargetsAndArtifacts() {
	artifactRoles := make(map[contract.ArtifactInventoryID]string, len(v.bundle.Artifacts.Artifacts))
	for _, artifact := range v.bundle.Artifacts.Artifacts {
		artifactRoles[artifact.ID] = artifact.Role
	}
	for _, obligation := range v.scenario.ProofObligations {
		for _, artifactID := range obligation.ArtifactInventoryIDs {
			if _, known := artifactRoles[artifactID]; !known {
				v.add("%s obligation %s references unknown artifact inventory %s", v.scenario.ID, obligation.ObligationID, artifactID)
			}
		}
		if !contains([]string{"make"}, firstArg(obligation.Argv)) || len(obligation.Argv) != 2 || obligation.Argv[1] != obligation.MakeTarget {
			v.add("%s obligation %s argv must be exactly [make, make_target]", v.scenario.ID, obligation.ObligationID)
		}
		targets, targetKnown := proofTargetPolicy[obligation.ProofType]
		if !targetKnown || !containsMapKey(targets, obligation.MakeTarget) {
			v.add("%s obligation %s target %s cannot prove %s", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget, obligation.ProofType)
		}
		if _, exists := v.scenario.makeTargets[obligation.MakeTarget]; !exists {
			v.add("%s obligation %s target %s is not defined by the repository Makefile", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget)
		}
		rule, ruleKnown := targetRules[obligation.MakeTarget]
		supportCell, hasSupport := supportByID(v.bundle, obligation.SupportCellID)
		if ruleKnown && rule.hasComponent == false && hasSupport {
			v.add("%s obligation %s target %s requires support_cell_id null", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget)
		}
		if ruleKnown && rule.hasComponent && (!hasSupport || supportCell.Component != rule.component) {
			v.add("%s obligation %s target %s requires a %s support cell", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget, rule.component)
		}
		if ruleKnown && rule.hasPlatform && (!hasSupport || supportCell.Platform != rule.platform) {
			v.add("%s obligation %s target %s requires platform %s", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget, rule.platform)
		}
		if obligation.SupportCellID != nil {
			if !hasSupport {
				v.add("%s obligation %s references unknown support cell %s", v.scenario.ID, obligation.ObligationID, *obligation.SupportCellID)
			} else if supportCell.Policy != "required" && !isSemanticSupportCell(v.bundle, *obligation.SupportCellID) {
				v.add("%s obligation %s references excluded or unauthorized support cell %s", v.scenario.ID, obligation.ObligationID, *obligation.SupportCellID)
			}
		}
		for _, requirementID := range obligation.RequirementIDs {
			requirement, known := requirementByID(v.bundle, requirementID)
			if !known {
				continue
			}
			if obligation.SupportCellID != nil && hasSupport && !contains(requirement.ApplicableComponents, supportCell.Component) {
				v.add("%s obligation %s uses support cell outside requirement %s applicability", v.scenario.ID, obligation.ObligationID, requirementID)
			}
		}
		roles := make(map[string]struct{})
		for _, artifactID := range obligation.ArtifactInventoryIDs {
			if role, known := artifactRoles[artifactID]; known {
				roles[role] = struct{}{}
			}
		}
		for role := range targetRequiredRoles[obligation.MakeTarget] {
			if _, present := roles[role]; !present {
				v.add("%s obligation %s target %s requires artifact role %s", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget, role)
			}
		}
		if obligation.ProofType == "reference-model" && !stringSetEqual(keysOfStringSet(roles), []string{"conformance-runner"}) {
			v.add("%s obligation %s reference-model proof requires only the independent conformance-runner artifact", v.scenario.ID, obligation.ObligationID)
		} else if obligation.MakeTarget != "test-conformance" {
			for role := range roles {
				if _, allowed := targetAllowedRoles[obligation.MakeTarget][role]; !allowed {
					v.add("%s obligation %s target %s does not permit artifact role %s", v.scenario.ID, obligation.ObligationID, obligation.MakeTarget, role)
				}
			}
		}
		if obligation.ProofType == "negative-control" {
			expected := map[contract.ArtifactInventoryID]struct{}{"ARTDEF-CONFORMANCE-RUNNER-001": {}}
			if len(obligation.RequirementIDs) == 1 {
				for _, control := range v.scenario.NegativeControls {
					if control.RequirementID == obligation.RequirementIDs[0] {
						for _, artifactID := range control.SubjectArtifactInventoryIDs {
							expected[artifactID] = struct{}{}
						}
					}
				}
			}
			if !stringSetEqual(stringIDs(obligation.ArtifactInventoryIDs), keysOfArtifactSet(expected)) {
				v.add("%s obligation %s negative-control artifacts must exactly bind the runner and mutated subjects", v.scenario.ID, obligation.ObligationID)
			}
		}
		v.validateVectorSets(obligation)
		if len(obligation.PerformanceBudgetIDs) > 0 || len(obligation.RequiredMeasurementIDs) > 0 {
			performanceAssertion := false
			for _, assertionID := range obligation.AssertionIDs {
				if assertion, known := v.assertions[assertionID]; known && assertion.Oracle.Kind == "performance-budget" {
					performanceAssertion = true
				}
			}
			if !performanceAssertion {
				v.add("%s obligation %s performance ownership requires a performance-budget assertion", v.scenario.ID, obligation.ObligationID)
			}
		}
	}
}

func (v *scenarioValidator) validateVectorSets(obligation ProofObligation) {
	seen := make(map[contract.VectorSetID]struct{}, len(obligation.RequiredVectorSetIDs))
	for _, vectorSetID := range obligation.RequiredVectorSetIDs {
		if _, duplicate := seen[vectorSetID]; duplicate {
			v.add("%s obligation %s has duplicate required vector set ID %s", v.scenario.ID, obligation.ObligationID, vectorSetID)
		} else {
			seen[vectorSetID] = struct{}{}
		}
		if v.vectorSets == nil {
			v.add("%s obligation %s requires vector-set catalog for nonempty required_vector_set_ids", v.scenario.ID, obligation.ObligationID)
		} else if !v.vectorSets.Has(vectorSetID) {
			v.add("%s obligation %s references unknown vector set %s", v.scenario.ID, obligation.ObligationID, vectorSetID)
		}
	}
}

func (v *scenarioValidator) validateRequiredProofs() {
	obligationsByKey := make(map[string]int)
	for _, obligation := range v.scenario.ProofObligations {
		for _, requirementID := range obligation.RequirementIDs {
			key := fmt.Sprintf("%s|%s|%s", requirementID, obligation.ProofType, nullableSupportCell(obligation.SupportCellID))
			obligationsByKey[key]++
		}
	}
	requiredSupportCells := make(map[string][]contract.SupportCellID)
	for _, cell := range v.bundle.Support.Cells {
		if cell.Policy != "required" {
			continue
		}
		requiredSupportCells[cell.Component] = append(requiredSupportCells[cell.Component], cell.ID)
	}
	semanticSupportCells := make(map[string][]contract.SupportCellID)
	for _, cellID := range v.bundle.Support.SemanticCorpusCellIDs {
		cell, known := supportCellByID(v.bundle, cellID)
		if known {
			semanticSupportCells[cell.Component] = append(semanticSupportCells[cell.Component], cell.ID)
		}
	}
	for _, requirementID := range uniqueRequirementIDs(v.scenario.RequirementIDs) {
		requirement, known := requirementByID(v.bundle, requirementID)
		if !known {
			v.add("%s references unknown requirement %s", v.scenario.ID, requirementID)
			continue
		}
		required := stringSet(requirement.RequiredProofTypes)
		for _, obligation := range v.scenario.ProofObligations {
			if !contains(stringIDs(obligation.RequirementIDs), string(requirementID)) {
				continue
			}
			if _, requiredProof := required[obligation.ProofType]; requiredProof || obligation.ProofType == "fault-injection" {
				continue
			}
			v.add("%s requirement %s has non-required proof type %s", v.scenario.ID, requirementID, obligation.ProofType)
		}
		faultInjectionCount := 0
		postgresFaultCells := make([]contract.SupportCellID, 0, 2)
		for _, obligation := range v.scenario.ProofObligations {
			if obligation.ProofType == "fault-injection" && contains(stringIDs(obligation.RequirementIDs), string(requirementID)) {
				faultInjectionCount++
				if obligation.SupportCellID != nil {
					cell, known := supportCellByID(v.bundle, *obligation.SupportCellID)
					if known && cell.Component == "postgresql-server" {
						postgresFaultCells = append(postgresFaultCells, *obligation.SupportCellID)
					}
				}
			}
		}
		_, faultRequired := required["fault-injection"]
		if len(postgresFaultCells) > 0 {
			requiredPostgresCells := requiredSupportCells["postgresql-server"]
			if !supportCellSetsEqual(postgresFaultCells, requiredPostgresCells) || faultInjectionCount != len(requiredPostgresCells) {
				v.add("%s requirement %s PostgreSQL fault-injection obligations must exactly cover required extension architecture cells", v.scenario.ID, requirementID)
			}
		} else if !faultRequired && faultInjectionCount > 1 {
			v.add("%s requirement %s has multiple optional fault-injection obligations, found %d", v.scenario.ID, requirementID, faultInjectionCount)
		}
		for _, proofType := range sortedStrings(requirement.RequiredProofTypes) {
			switch proofType {
			case "server-black-box":
				if contains(requirement.ApplicableComponents, "postgresql-server") {
					for _, cellID := range requiredSupportCells["postgresql-server"] {
						v.requireProofKey(requirementID, proofType, &cellID, obligationsByKey)
					}
				}
			case "native-e2e":
				for _, component := range []string{"swift-client", "kotlin-client", "react-native-client"} {
					if !contains(requirement.ApplicableComponents, component) {
						continue
					}
					for _, cellID := range semanticSupportCells[component] {
						v.requireProofKey(requirementID, proofType, &cellID, obligationsByKey)
					}
				}
			case "reference-model", "negative-control":
				v.requireProofKey(requirementID, proofType, nil, obligationsByKey)
			case "fault-injection":
				if len(postgresFaultCells) == 0 {
					v.requireSingletonProof(requirementID, proofType, obligationsByKey)
				}
			default:
				v.add("%s requirement %s has unknown required proof type %s", v.scenario.ID, requirementID, proofType)
			}
		}
	}
}

func supportCellByID(bundle *contract.Bundle, id contract.SupportCellID) (contract.SupportCell, bool) {
	for _, cell := range bundle.Support.Cells {
		if cell.ID == id {
			return cell, true
		}
	}
	return contract.SupportCell{}, false
}

func selectedPostgresFaultCells(selectedProofKeys map[string][]string, requirementID contract.RequirementID, bundle *contract.Bundle) []contract.SupportCellID {
	var result []contract.SupportCellID
	for key := range selectedProofKeysForRequirement(selectedProofKeys, requirementID) {
		parts := strings.Split(key, "|")
		if len(parts) != 3 || parts[1] != "fault-injection" {
			continue
		}
		id := contract.SupportCellID(parts[2])
		cell, known := supportCellByID(bundle, id)
		if known && cell.Component == "postgresql-server" {
			result = append(result, id)
		}
	}
	return result
}

func supportCellSetsEqual(left, right []contract.SupportCellID) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := make(map[contract.SupportCellID]struct{}, len(left))
	for _, id := range left {
		leftSet[id] = struct{}{}
	}
	if len(leftSet) != len(right) {
		return false
	}
	for _, id := range right {
		if _, found := leftSet[id]; !found {
			return false
		}
	}
	return true
}

func supportCellIDs(values []string) []contract.SupportCellID {
	result := make([]contract.SupportCellID, len(values))
	for index, value := range values {
		result[index] = contract.SupportCellID(value)
	}
	return result
}

func (v *scenarioValidator) requireSingletonProof(requirementID contract.RequirementID, proofType string, counts map[string]int) {
	count := 0
	prefix := fmt.Sprintf("%s|%s|", requirementID, proofType)
	for key, value := range counts {
		if strings.HasPrefix(key, prefix) {
			count += value
		}
	}
	if count != 1 {
		v.add("%s requirement %s requires exactly one singleton %s proof obligation, found %d", v.scenario.ID, requirementID, proofType, count)
	}
}

func (v *scenarioValidator) requireProofKey(requirementID contract.RequirementID, proofType string, supportID *contract.SupportCellID, counts map[string]int) {
	key := fmt.Sprintf("%s|%s|%s", requirementID, proofType, nullableSupportCell(supportID))
	if counts[key] != 1 {
		v.add("%s requirement %s requires exactly one %s proof obligation for support cell %s, found %d", v.scenario.ID, requirementID, proofType, nullableSupportCell(supportID), counts[key])
	}
}

func (v *scenarioValidator) validatePerformance() {
	budgets := make(map[contract.BudgetID]contract.PerformanceBudget, len(v.bundle.Performance.Budgets))
	measurements := make(map[contract.MeasurementID]contract.RequiredMeasurement, len(v.bundle.Performance.RequiredMeasurements))
	for _, item := range v.bundle.Performance.Budgets {
		budgets[item.ID] = item
	}
	for _, item := range v.bundle.Performance.RequiredMeasurements {
		measurements[item.ID] = item
	}
	supportCells := make(map[contract.SupportCellID]contract.SupportCell, len(v.bundle.Support.Cells))
	for _, cell := range v.bundle.Support.Cells {
		supportCells[cell.ID] = cell
	}
	artifactRoles := make(map[contract.ArtifactInventoryID]string, len(v.bundle.Artifacts.Artifacts))
	for _, artifact := range v.bundle.Artifacts.Artifacts {
		artifactRoles[artifact.ID] = artifact.Role
	}
	for _, item := range v.bundle.Performance.Budgets {
		if item.ScenarioID != v.scenario.ID {
			continue
		}
		v.validatePerformanceItem(string(item.ID), item.SupportCellIDs, item.ArtifactInventoryIDs, budgets, measurements, supportCells, artifactRoles, true)
	}
	for _, item := range v.bundle.Performance.RequiredMeasurements {
		if item.ScenarioID != v.scenario.ID {
			continue
		}
		v.validatePerformanceItem(string(item.ID), item.SupportCellIDs, item.ArtifactInventoryIDs, budgets, measurements, supportCells, artifactRoles, false)
	}
	v.validateMeasurementBindings(measurements)
	for _, obligation := range v.scenario.ProofObligations {
		for _, id := range obligation.PerformanceBudgetIDs {
			item, exists := budgets[id]
			if !exists {
				v.add("%s obligation %s references unknown performance budget %s", v.scenario.ID, obligation.ObligationID, id)
				continue
			}
			v.validateDeclaredPerformance(string(id), item.ScenarioID, item.SupportCellIDs, item.ArtifactInventoryIDs, obligation, supportCells, artifactRoles)
		}
		for _, id := range obligation.RequiredMeasurementIDs {
			item, exists := measurements[id]
			if !exists {
				v.add("%s obligation %s references unknown required measurement %s", v.scenario.ID, obligation.ObligationID, id)
				continue
			}
			v.validateDeclaredPerformance(string(id), item.ScenarioID, item.SupportCellIDs, item.ArtifactInventoryIDs, obligation, supportCells, artifactRoles)
		}
	}
}

func (v *scenarioValidator) validatePerformanceItem(id string, itemSupport []contract.SupportCellID, itemArtifacts []contract.ArtifactInventoryID, budgets map[contract.BudgetID]contract.PerformanceBudget, measurements map[contract.MeasurementID]contract.RequiredMeasurement, supportCells map[contract.SupportCellID]contract.SupportCell, artifactRoles map[contract.ArtifactInventoryID]string, isBudget bool) {
	for _, supportID := range itemSupport {
		if _, exists := supportCells[supportID]; !exists {
			v.add("%s performance item %s references unknown support cell %s", v.scenario.ID, id, supportID)
		}
		var owners []ProofObligation
		for _, obligation := range v.scenario.ProofObligations {
			if obligation.SupportCellID == nil || *obligation.SupportCellID != supportID {
				continue
			}
			if isBudget && contains(stringIDs(obligation.PerformanceBudgetIDs), id) {
				owners = append(owners, obligation)
			}
			if !isBudget && contains(stringIDs(obligation.RequiredMeasurementIDs), id) {
				owners = append(owners, obligation)
			}
		}
		if len(owners) != 1 {
			v.add("%s performance item %s must be declared by exactly one obligation for support cell %s, found %d", v.scenario.ID, id, supportID, len(owners))
			continue
		}
		obligation := owners[0]
		expectedArtifacts := performanceArtifactProjection(itemArtifacts, supportCells[supportID], artifactRoles)
		if !stringSetEqual(stringIDs(obligation.ArtifactInventoryIDs), expectedArtifacts) {
			v.add("%s performance item %s obligation %s artifacts do not exactly match support cell %s", v.scenario.ID, id, obligation.ObligationID, supportID)
		}
		if !obligationHasPerformanceAssertion(obligation, v.assertions) {
			v.add("%s performance item %s declaring obligation %s must own a performance-budget assertion", v.scenario.ID, id, obligation.ObligationID)
		}
	}
}

func (v *scenarioValidator) validateDeclaredPerformance(id string, itemScenario contract.ScenarioID, itemSupport []contract.SupportCellID, itemArtifacts []contract.ArtifactInventoryID, obligation ProofObligation, supportCells map[contract.SupportCellID]contract.SupportCell, artifactRoles map[contract.ArtifactInventoryID]string) {
	if itemScenario != v.scenario.ID {
		v.add("%s obligation %s declares %s authored for scenario %s", v.scenario.ID, obligation.ObligationID, id, itemScenario)
	}
	if obligation.SupportCellID == nil || !contains(stringIDs(itemSupport), string(*obligation.SupportCellID)) {
		v.add("%s %s obligation %s uses an unauthorized support cell %s", v.scenario.ID, id, obligation.ObligationID, nullableSupportCell(obligation.SupportCellID))
		return
	}
	expectedArtifacts := performanceArtifactProjection(itemArtifacts, supportCells[*obligation.SupportCellID], artifactRoles)
	if !stringSetEqual(stringIDs(obligation.ArtifactInventoryIDs), expectedArtifacts) {
		v.add("%s %s obligation %s artifacts do not exactly match its declared support cell", v.scenario.ID, id, obligation.ObligationID)
	}
}

func (v *scenarioValidator) validateOwnership() {
	expected := make(map[string]struct{})
	for _, obligation := range v.scenario.ProofObligations {
		for _, assertionID := range obligation.AssertionIDs {
			assertion, exists := v.assertions[assertionID]
			if !exists {
				continue
			}
			for _, requirementID := range assertion.RequirementIDs {
				ownership := Ownership{ScenarioID: v.scenario.ID, RequirementID: requirementID, ProofObligationID: obligation.ObligationID, AssertionID: assertionID, ProofType: obligation.ProofType, SupportCellID: obligation.SupportCellID}
				expected[ownershipKey(ownership)] = struct{}{}
			}
		}
	}
	actual := make(map[string]struct{}, len(v.scenario.Ownership))
	for _, ownership := range v.scenario.Ownership {
		actual[ownershipKey(ownership)] = struct{}{}
		if ownership.ScenarioID != v.scenario.ID {
			v.add("%s ownership tuple has wrong scenario ID %s", v.scenario.ID, ownership.ScenarioID)
		}
		if obligation, exists := v.obligations[ownership.ProofObligationID]; exists {
			if obligation.ProofType != ownership.ProofType || !sameSupportCell(obligation.SupportCellID, ownership.SupportCellID) {
				v.add("%s ownership tuple %s does not match its obligation", v.scenario.ID, ownershipKey(ownership))
			}
		}
	}
	if len(actual) != len(expected) || !stringSetEqual(keysOfStringSet(actual), keysOfStringSet(expected)) {
		v.add("%s ownership does not exactly enumerate obligation, assertion, and requirement tuples", v.scenario.ID)
	}
}

func sameInjection(actual InjectionRecipe, expected contract.FaultInjection) bool {
	return actual.Mechanism == expected.Mechanism && actual.Target == expected.Target && actual.Operator == expected.Operator && actual.Parameters.Scenario == expected.Parameters.Scenario && actual.Parameters.Defect == expected.Parameters.Defect && actual.Parameters.Precondition == expected.Parameters.Precondition
}

func sameSupportCell(left, right *contract.SupportCellID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func artifactByID(bundle *contract.Bundle, id contract.ArtifactInventoryID) (contract.ArtifactInventoryItem, bool) {
	for _, artifact := range bundle.Artifacts.Artifacts {
		if artifact.ID == id {
			return artifact, true
		}
	}
	return contract.ArtifactInventoryItem{}, false
}

func requirementByID(bundle *contract.Bundle, id contract.RequirementID) (contract.Requirement, bool) {
	for _, requirement := range bundle.Requirements.Requirements {
		if requirement.ID == id {
			return requirement, true
		}
	}
	return contract.Requirement{}, false
}

func supportByID(bundle *contract.Bundle, id *contract.SupportCellID) (contract.SupportCell, bool) {
	if id == nil {
		return contract.SupportCell{}, false
	}
	for _, cell := range bundle.Support.Cells {
		if cell.ID == *id {
			return cell, true
		}
	}
	return contract.SupportCell{}, false
}

func requiredSupportCellsFor(bundle *contract.Bundle, component string) []string {
	result := make([]string, 0)
	for _, cell := range bundle.Support.Cells {
		if cell.Policy == "required" && cell.Component == component {
			result = append(result, string(cell.ID))
		}
	}
	sort.Strings(result)
	return result
}

func semanticSupportCellsFor(bundle *contract.Bundle, component string) []string {
	result := make([]string, 0)
	for _, cellID := range bundle.Support.SemanticCorpusCellIDs {
		cell, known := supportCellByID(bundle, cellID)
		if known && cell.Component == component {
			result = append(result, string(cellID))
		}
	}
	sort.Strings(result)
	return result
}

func isSemanticSupportCell(bundle *contract.Bundle, id contract.SupportCellID) bool {
	for _, cellID := range bundle.Support.SemanticCorpusCellIDs {
		if cellID == id {
			return true
		}
	}
	return false
}

func selectedProofKeysForRequirement(keys map[string][]string, requirementID contract.RequirementID) map[string][]string {
	prefix := string(requirementID) + "|"
	result := make(map[string][]string)
	for key, owners := range keys {
		if strings.HasPrefix(key, prefix) {
			result[key] = owners
		}
	}
	return result
}

func performanceArtifactProjection(itemArtifacts []contract.ArtifactInventoryID, cell contract.SupportCell, roles map[contract.ArtifactInventoryID]string) []string {
	clientRoles := map[string]struct{}{"swift-spm": {}, "cocoapods": {}, "kotlin-maven": {}, "react-native-npm": {}}
	applicable := make(map[string]struct{})
	switch cell.Component {
	case "swift-client":
		applicable["swift-spm"] = struct{}{}
		applicable["cocoapods"] = struct{}{}
	case "kotlin-client":
		applicable["kotlin-maven"] = struct{}{}
	case "react-native-client":
		applicable["react-native-npm"] = struct{}{}
		if cell.Platform == "ios" {
			applicable["swift-spm"] = struct{}{}
			applicable["cocoapods"] = struct{}{}
		} else {
			applicable["kotlin-maven"] = struct{}{}
		}
	}
	result := make([]string, 0, len(itemArtifacts))
	for _, artifactID := range itemArtifacts {
		role := roles[artifactID]
		if _, isClient := clientRoles[role]; !isClient {
			result = append(result, string(artifactID))
		} else if _, applies := applicable[role]; applies {
			result = append(result, string(artifactID))
		}
	}
	return result
}

func obligationHasPerformanceAssertion(obligation ProofObligation, assertions map[contract.AssertionID]Assertion) bool {
	for _, assertionID := range obligation.AssertionIDs {
		if assertion, exists := assertions[assertionID]; exists && assertion.Oracle.Kind == "performance-budget" {
			return true
		}
	}
	return false
}

func ownershipKey(ownership Ownership) string {
	return strings.Join([]string{string(ownership.ScenarioID), string(ownership.RequirementID), string(ownership.ProofObligationID), string(ownership.AssertionID), ownership.ProofType, nullableSupportCell(ownership.SupportCellID)}, "|")
}

func normativeReferenceKey(reference contract.NormativeReference) string {
	return reference.Path + reference.Anchor
}

func nullableSupportCell(id *contract.SupportCellID) string {
	if id == nil {
		return "null"
	}
	return string(*id)
}

func proofTypes(obligations []ProofObligation) []string {
	result := make([]string, 0, len(obligations))
	for _, obligation := range obligations {
		result = append(result, obligation.ProofType)
	}
	return result
}

func firstArg(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	return argv[0]
}

func uniqueRequirementIDs(values []contract.RequirementID) []contract.RequirementID {
	seen := make(map[contract.RequirementID]struct{}, len(values))
	result := make([]contract.RequirementID, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func stringIDs[T ~string](values []T) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, string(value))
	}
	return result
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func stringSetEqual(left, right []string) bool {
	leftSet := stringSet(left)
	rightSet := stringSet(right)
	if len(leftSet) != len(rightSet) {
		return false
	}
	for value := range leftSet {
		if _, exists := rightSet[value]; !exists {
			return false
		}
	}
	return true
}

func keysOfStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	return result
}

func keysOfControlSet(values map[contract.ControlID]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, string(value))
	}
	return result
}

func keysOfArtifactSet(values map[contract.ArtifactInventoryID]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, string(value))
	}
	return result
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func sortedRequirementKeys(values map[contract.RequirementID][]string) []contract.RequirementID {
	result := make([]contract.RequirementID, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func sortedControlKeys(values map[contract.ControlID][]string) []contract.ControlID {
	result := make([]contract.ControlID, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func sortedOwnerText(values []string) string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return strings.Join(result, ", ")
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsMapKey(values map[string]struct{}, wanted string) bool {
	_, exists := values[wanted]
	return exists
}

func joinScenarioErrors(failures []error) error {
	filtered := make([]error, 0, len(failures))
	for _, failure := range failures {
		if failure != nil {
			filtered = append(filtered, failure)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	sort.SliceStable(filtered, func(left, right int) bool { return filtered[left].Error() < filtered[right].Error() })
	return fmt.Errorf("scenario semantic validation failed: %w", errors.Join(filtered...))
}
