package scenarios

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

var (
	nativeActionIDPattern  = regexp.MustCompile(`^NACT-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
	nativeClientKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
)

var nativeCommandsByActor = map[string]map[string]struct{}{
	"controller": {"install-model": {}, "apply-step": {}, "request-step": {}},
	"artifact":   {"stage-step": {}},
	"client":     {"open": {}, "execute-step": {}, "synchronize-step": {}, "begin-call": {}, "await-call": {}, "lifecycle": {}},
	"process":    {"execute-step": {}, "terminate": {}, "relaunch": {}},
	"observer":   {"await-step": {}, "capture": {}, "measure": {}},
}

var nativeCaptureSources = stringSet([]string{
	"application-rows",
	"pending-mutations",
	"rejected-mutations",
	"sync-status",
	"sync-events",
	"scope-state",
	"checkpoints",
	"provenance",
	"rebuild-state",
	"request-trace",
	"server-state",
	"artifact-state",
	"process-trace",
})

var nativeLocalCaptureSources = stringSet([]string{
	"application-rows",
	"pending-mutations",
	"rejected-mutations",
	"sync-status",
	"sync-events",
	"scope-state",
	"checkpoints",
	"provenance",
	"rebuild-state",
})

var nativeProcessBoundaries = stringSet([]string{
	"queue-inserted",
	"queue-resolved",
	"push-reconciled",
	"pull-applied",
	"checksum-committed",
	"rebuild-page-applied",
	"provenance-pruned",
	"rebuild-finalized",
	"schema-progressed",
	"rejection-persisted",
})

type nativeActionBinding struct {
	clientKey  *string
	clientKeys []string
	open       *NativeClientOpenParameters
	sync       *NativeSynchronizeParameters
	beginCall  *NativeBeginCallParameters
	awaitCall  *NativeAwaitCallParameters
	awaitStep  *NativeAwaitStepParameters
	boundary   *NativeProcessBoundaryParameters
	capture    *NativeCaptureParameters
	measure    *NativeMeasureParameters
}

type nativePlanValidator struct {
	parent          *scenarioValidator
	plan            NativeExecutionPlan
	clients         map[string]NativeClient
	actions         map[NativeActionID]int
	bindings        []nativeActionBinding
	coveredSteps    map[StepID][]NativeActionID
	captured        map[ExpectationID][]NativeActionID
	measuredBudgets map[contract.BudgetID][]NativeActionID
	measured        map[contract.MeasurementID][]NativeActionID
	stagedSteps     map[StepID]nativeStagedStep
	opened          map[string]bool
	alive           map[string]bool
	used            map[string]bool
	lastTermination map[string]nativeTermination
	calls           map[NativeCallID]string
	activeCalls     map[string]NativeCallID
	closedCalls     map[NativeCallID]bool
	stepIndexes     map[StepID]int
	lastStepIndex   int
}

type nativeStagedStep struct {
	clientKey string
	action    NativeActionID
	index     int
}

type nativeTermination struct {
	action   NativeActionID
	boundary string
}

func (v *scenarioValidator) validateNativeExecution() {
	if v.scenario.NativeExecution == nil {
		return
	}

	validator := nativePlanValidator{
		parent:          v,
		plan:            *v.scenario.NativeExecution,
		clients:         make(map[string]NativeClient),
		actions:         make(map[NativeActionID]int),
		bindings:        make([]nativeActionBinding, len(v.scenario.NativeExecution.Actions)),
		coveredSteps:    make(map[StepID][]NativeActionID),
		captured:        make(map[ExpectationID][]NativeActionID),
		measuredBudgets: make(map[contract.BudgetID][]NativeActionID),
		measured:        make(map[contract.MeasurementID][]NativeActionID),
		stagedSteps:     make(map[StepID]nativeStagedStep),
		opened:          make(map[string]bool),
		alive:           make(map[string]bool),
		used:            make(map[string]bool),
		lastTermination: make(map[string]nativeTermination),
		calls:           make(map[NativeCallID]string),
		activeCalls:     make(map[string]NativeCallID),
		closedCalls:     make(map[NativeCallID]bool),
		stepIndexes:     make(map[StepID]int, len(v.scenario.Steps)),
		lastStepIndex:   -1,
	}
	for index, step := range v.scenario.Steps {
		validator.stepIndexes[step.ID] = index
	}
	validator.validate()
}

func (v *nativePlanValidator) validate() {
	if v.plan.Version != 1 {
		v.parent.add("%s native_execution has unsupported version %d", v.parent.scenario.ID, v.plan.Version)
	}
	v.validateClients()
	v.validateActions()
	v.validateStepClosure()
	v.validateObservationClosure()
	v.validateClientClosure()
}

func (v *nativePlanValidator) validateClients() {
	if len(v.plan.Clients) == 0 {
		v.parent.add("%s native_execution must declare at least one client", v.parent.scenario.ID)
	}
	identities := make(map[string]string)
	databases := make(map[string]string)
	for _, client := range v.plan.Clients {
		if !nativeClientKeyPattern.MatchString(client.Key) {
			v.parent.add("%s native client key %q is invalid", v.parent.scenario.ID, client.Key)
		}
		if client.UserID == "" || client.ClientID == "" || !nativeClientKeyPattern.MatchString(client.DatabaseKey) {
			v.parent.add("%s native client %q has an incomplete identity or invalid database key", v.parent.scenario.ID, client.Key)
		}
		if _, duplicate := v.clients[client.Key]; duplicate {
			v.parent.add("%s duplicate native client key %q", v.parent.scenario.ID, client.Key)
		} else {
			v.clients[client.Key] = client
		}
		identity := client.UserID + "\x00" + client.ClientID
		if owner, duplicate := identities[identity]; duplicate {
			v.parent.add("%s native clients %q and %q share one client identity", v.parent.scenario.ID, owner, client.Key)
		} else {
			identities[identity] = client.Key
		}
		if owner, duplicate := databases[client.DatabaseKey]; duplicate {
			v.parent.add("%s native clients %q and %q share database key %q", v.parent.scenario.ID, owner, client.Key, client.DatabaseKey)
		} else {
			databases[client.DatabaseKey] = client.Key
		}
	}
}

func (v *nativePlanValidator) validateActions() {
	if len(v.plan.Actions) == 0 {
		v.parent.add("%s native_execution must contain at least one action", v.parent.scenario.ID)
	}
	for index, action := range v.plan.Actions {
		if !nativeActionIDPattern.MatchString(string(action.ID)) {
			v.parent.add("%s native action ID %q is invalid", v.parent.scenario.ID, action.ID)
		}
		if previous, duplicate := v.actions[action.ID]; duplicate {
			v.parent.add("%s duplicate native action ID %q at indexes %d and %d", v.parent.scenario.ID, action.ID, previous, index)
		} else {
			v.actions[action.ID] = index
		}
		if _, known := nativeCommandsByActor[action.Actor][action.Command]; !known {
			v.parent.add("%s native action %s has incompatible actor and command %s/%s", v.parent.scenario.ID, action.ID, action.Actor, action.Command)
		}
		if !contains([]string{"setup", "exercise", "verify", "cleanup"}, action.Phase) {
			v.parent.add("%s native action %s has invalid phase %q", v.parent.scenario.ID, action.ID, action.Phase)
		}

		binding, err := decodeNativeActionParameters(action)
		if err != nil {
			v.parent.add("%s native action %s parameters are invalid: %v", v.parent.scenario.ID, action.ID, err)
		}
		v.bindings[index] = binding
		v.validateActionCoverage(action, binding)
		v.validateActionOrdering(index, action, binding)
	}
	v.validatePlanOrder()
}

func (v *nativePlanValidator) validatePlanOrder() {
	installIndex := -1
	installCount := 0
	lastCoveredIndex := -1
	for index, action := range v.plan.Actions {
		if action.Actor == "controller" && action.Command == "install-model" {
			installCount++
			if installIndex == -1 {
				installIndex = index
			}
		}
		if len(action.CoversStepIDs) != 0 {
			lastCoveredIndex = index
		}
	}
	if installCount != 1 {
		v.parent.add("%s native_execution has %d install-model actions, want exactly one", v.parent.scenario.ID, installCount)
	}
	if installIndex >= 0 {
		for index, action := range v.plan.Actions {
			if len(action.CoversStepIDs) != 0 && index < installIndex {
				v.parent.add("%s native action %s covers a step before install-model", v.parent.scenario.ID, action.ID)
			}
		}
	}
	for index, action := range v.plan.Actions {
		if (action.Command == "capture" || action.Command == "measure") && index <= lastCoveredIndex {
			v.parent.add("%s native observation action %s occurs before all scenario steps complete", v.parent.scenario.ID, action.ID)
		}
	}
}

func (v *nativePlanValidator) validateActionCoverage(action NativeAction, binding nativeActionBinding) {
	wantsStep := nativeActionCoversStep(action.Actor, action.Command)
	groupedSynchronization := action.Actor == "client" && action.Command == "synchronize-step"
	if wantsStep && len(action.CoversStepIDs) == 0 {
		v.parent.add("%s native action %s must cover at least one scenario step", v.parent.scenario.ID, action.ID)
	} else if wantsStep && !groupedSynchronization && len(action.CoversStepIDs) != 1 {
		v.parent.add("%s native action %s must cover exactly one scenario step", v.parent.scenario.ID, action.ID)
	}
	if !wantsStep && len(action.CoversStepIDs) != 0 {
		v.parent.add("%s native action %s must not cover a scenario step", v.parent.scenario.ID, action.ID)
	}
	seen := make(map[StepID]struct{}, len(action.CoversStepIDs))
	for _, stepID := range action.CoversStepIDs {
		if _, duplicate := seen[stepID]; duplicate {
			v.parent.add("%s native action %s covers step %s more than once", v.parent.scenario.ID, action.ID, stepID)
			continue
		}
		seen[stepID] = struct{}{}
		step, known := v.parent.steps[stepID]
		if !known {
			v.parent.add("%s native action %s covers unknown step %s", v.parent.scenario.ID, action.ID, stepID)
			continue
		}
		stepIndex := v.stepIndexes[stepID]
		if stepIndex <= v.lastStepIndex {
			v.parent.add("%s native action %s covers step %s outside authored step order", v.parent.scenario.ID, action.ID, stepID)
		} else {
			v.lastStepIndex = stepIndex
		}
		v.coveredSteps[stepID] = append(v.coveredSteps[stepID], action.ID)
		if action.Phase != step.Phase {
			v.parent.add("%s native action %s phase %q does not match step %s phase %q", v.parent.scenario.ID, action.ID, action.Phase, stepID, step.Phase)
		}
		if !nativeCommandSupportsStep(action.Actor, action.Command, step) {
			v.parent.add("%s native action %s command %s/%s cannot execute step %s operation %s", v.parent.scenario.ID, action.ID, action.Actor, action.Command, stepID, OperationKey(step.Operation))
		}
		v.validateStepClientIdentity(action, binding, step)
		v.validateAwaitStepBinding(action, binding, step)
		v.validateSynchronizationCompletion(action, binding, step)
	}

	if len(action.CoversStepIDs) != 0 {
		return
	}
	switch action.Actor + "/" + action.Command {
	case "controller/install-model", "client/open":
		if action.Phase != "setup" {
			v.parent.add("%s native action %s must use setup phase", v.parent.scenario.ID, action.ID)
		}
	case "observer/capture", "observer/measure":
		if action.Phase != "verify" {
			v.parent.add("%s native action %s must use verify phase", v.parent.scenario.ID, action.ID)
		}
	default:
		if action.Phase == "verify" {
			v.parent.add("%s native action %s cannot use verify phase", v.parent.scenario.ID, action.ID)
		}
	}
}

func (v *nativePlanValidator) validateStepClientIdentity(action NativeAction, binding nativeActionBinding, step Step) {
	userID, clientID, hasIdentity, err := nativeOperationIdentity(step.Operation)
	if err != nil {
		v.parent.add("%s native action %s cannot resolve step %s client identity: %v", v.parent.scenario.ID, action.ID, step.ID, err)
		return
	}
	if binding.clientKey == nil {
		if hasIdentity && action.Actor == "process" {
			v.parent.add("%s native action %s must declare the client for step %s", v.parent.scenario.ID, action.ID, step.ID)
		}
		return
	}
	client, known := v.clients[*binding.clientKey]
	if !known {
		return
	}
	if !hasIdentity {
		v.parent.add("%s native action %s declares client %q for identity-free step %s", v.parent.scenario.ID, action.ID, *binding.clientKey, step.ID)
		return
	}
	if client.UserID != userID || client.ClientID != clientID {
		v.parent.add("%s native action %s client %q does not match step %s identity", v.parent.scenario.ID, action.ID, *binding.clientKey, step.ID)
	}
}

func (v *nativePlanValidator) validateAwaitStepBinding(action NativeAction, binding nativeActionBinding, step Step) {
	if binding.awaitStep == nil {
		return
	}
	if binding.awaitStep.CallID == nil {
		if step.Transport == "http" {
			v.parent.add("%s native action %s observes HTTP step %s without a call ID", v.parent.scenario.ID, action.ID, step.ID)
		}
		return
	}
	clientKey := binding.awaitStep.ClientKey
	active, found := v.activeCalls[clientKey]
	if !found || active != *binding.awaitStep.CallID {
		v.parent.add("%s native action %s observes inactive call %q for client %q", v.parent.scenario.ID, action.ID, *binding.awaitStep.CallID, clientKey)
	}
}

func (v *nativePlanValidator) validateActionOrdering(index int, action NativeAction, binding nativeActionBinding) {
	if binding.capture != nil {
		for _, expectationID := range binding.capture.ExpectationIDs {
			v.captured[expectationID] = append(v.captured[expectationID], action.ID)
		}
	}
	if binding.measure != nil {
		for _, budgetID := range binding.measure.PerformanceBudgetIDs {
			v.measuredBudgets[budgetID] = append(v.measuredBudgets[budgetID], action.ID)
		}
		for _, measurementID := range binding.measure.MeasurementIDs {
			v.measured[measurementID] = append(v.measured[measurementID], action.ID)
		}
	}
	if binding.clientKey != nil {
		if _, known := v.clients[*binding.clientKey]; !known {
			v.parent.add("%s native action %s references unknown client %q", v.parent.scenario.ID, action.ID, *binding.clientKey)
		}
	}
	for _, clientKey := range binding.clientKeys {
		if _, known := v.clients[clientKey]; !known {
			v.parent.add("%s native action %s references unknown client %q", v.parent.scenario.ID, action.ID, clientKey)
			continue
		}
		if !v.opened[clientKey] {
			v.parent.add("%s native action %s observes client %q before open", v.parent.scenario.ID, action.ID, clientKey)
		} else if !v.alive[clientKey] {
			v.parent.add("%s native action %s observes terminated client %q", v.parent.scenario.ID, action.ID, clientKey)
		}
	}
	if action.Actor == "controller" && action.Command == "apply-step" {
		v.validateWorkloadClientOrdering(action)
	}

	switch action.Actor + "/" + action.Command {
	case "artifact/stage-step":
		for _, stepID := range action.CoversStepIDs {
			v.stagedSteps[stepID] = nativeStagedStep{clientKey: dereferenceString(binding.clientKey), action: action.ID, index: index}
		}
		return
	case "client/open":
		v.validateOpenOrdering(index, action, binding)
		return
	case "client/begin-call":
		v.validateBeginCallOrdering(action, binding)
		return
	case "client/await-call":
		v.validateAwaitCallOrdering(action, binding)
		return
	case "process/terminate":
		v.validateTerminateOrdering(index, action, binding)
		return
	case "process/relaunch":
		v.validateRelaunchOrdering(action, binding)
		return
	}

	if binding.clientKey == nil {
		return
	}
	clientKey := *binding.clientKey
	if !v.opened[clientKey] {
		v.parent.add("%s native action %s uses client %q before open", v.parent.scenario.ID, action.ID, clientKey)
	} else if !v.alive[clientKey] {
		v.parent.add("%s native action %s uses terminated client %q", v.parent.scenario.ID, action.ID, clientKey)
	}
	if action.Actor != "observer" || action.Command == "await-step" {
		v.used[clientKey] = true
	}
}

func (v *nativePlanValidator) validateBeginCallOrdering(action NativeAction, binding nativeActionBinding) {
	if binding.beginCall == nil {
		return
	}
	clientKey := binding.beginCall.ClientKey
	if !v.opened[clientKey] {
		v.parent.add("%s native action %s begins call for client %q before open", v.parent.scenario.ID, action.ID, clientKey)
	} else if !v.alive[clientKey] {
		v.parent.add("%s native action %s begins call for terminated client %q", v.parent.scenario.ID, action.ID, clientKey)
	}
	if prior, duplicate := v.calls[binding.beginCall.CallID]; duplicate {
		v.parent.add("%s native call %q is already bound to client %q", v.parent.scenario.ID, binding.beginCall.CallID, prior)
	}
	if active, found := v.activeCalls[clientKey]; found {
		v.parent.add("%s native action %s begins call while call %q is active for client %q", v.parent.scenario.ID, action.ID, active, clientKey)
	}
	v.calls[binding.beginCall.CallID] = clientKey
	v.activeCalls[clientKey] = binding.beginCall.CallID
	v.used[clientKey] = true
}

func (v *nativePlanValidator) validateAwaitCallOrdering(action NativeAction, binding nativeActionBinding) {
	if binding.awaitCall == nil {
		return
	}
	clientKey := binding.awaitCall.ClientKey
	owner, known := v.calls[binding.awaitCall.CallID]
	active, activeFound := v.activeCalls[clientKey]
	if !known || owner != clientKey || !activeFound || active != binding.awaitCall.CallID || v.closedCalls[binding.awaitCall.CallID] {
		v.parent.add("%s native action %s awaits inactive call %q for client %q", v.parent.scenario.ID, action.ID, binding.awaitCall.CallID, clientKey)
		return
	}
	delete(v.activeCalls, clientKey)
	v.closedCalls[binding.awaitCall.CallID] = true
	v.used[clientKey] = true
}

func (v *nativePlanValidator) validateWorkloadClientOrdering(action NativeAction) {
	if len(action.CoversStepIDs) != 1 {
		return
	}
	step, found := v.parent.steps[action.CoversStepIDs[0]]
	if !found || step.Operation.ContractOperation != "workload" {
		return
	}
	clientKeys := make([]string, 0, len(v.clients))
	userID, clientID, hasIdentity, err := nativeOperationIdentity(step.Operation)
	if err != nil {
		v.parent.add("%s native action %s cannot resolve workload client identity: %v", v.parent.scenario.ID, action.ID, err)
		return
	}
	for key, client := range v.clients {
		if !hasIdentity || client.UserID == userID && client.ClientID == clientID {
			clientKeys = append(clientKeys, key)
		}
	}
	sort.Strings(clientKeys)
	if len(clientKeys) == 0 {
		v.parent.add("%s native action %s workload has no matching declared client", v.parent.scenario.ID, action.ID)
		return
	}
	for _, clientKey := range clientKeys {
		if !v.opened[clientKey] {
			v.parent.add("%s native action %s executes workload for client %q before open", v.parent.scenario.ID, action.ID, clientKey)
		} else if !v.alive[clientKey] {
			v.parent.add("%s native action %s executes workload for terminated client %q", v.parent.scenario.ID, action.ID, clientKey)
		}
		v.used[clientKey] = true
	}
}

func (v *nativePlanValidator) validateOpenOrdering(index int, action NativeAction, binding nativeActionBinding) {
	if binding.open == nil {
		return
	}
	clientKey := binding.open.ClientKey
	if v.opened[clientKey] {
		v.parent.add("%s native client %q opens more than once", v.parent.scenario.ID, clientKey)
		return
	}
	switch binding.open.DatabaseMode {
	case "create":
		if binding.open.SeedStepID != nil {
			v.parent.add("%s native action %s create open must not declare a seed step", v.parent.scenario.ID, action.ID)
		}
	case "reuse":
		if binding.open.SeedStepID == nil {
			v.parent.add("%s native action %s reuse open requires a seed step", v.parent.scenario.ID, action.ID)
		} else if staged, found := v.stagedSteps[*binding.open.SeedStepID]; !found || staged.index >= index {
			v.parent.add("%s native action %s seed step %s was not staged earlier", v.parent.scenario.ID, action.ID, *binding.open.SeedStepID)
		} else if staged.clientKey != clientKey {
			v.parent.add("%s native action %s seed step %s belongs to client %q", v.parent.scenario.ID, action.ID, *binding.open.SeedStepID, staged.clientKey)
		}
	default:
		v.parent.add("%s native action %s has invalid database mode %q", v.parent.scenario.ID, action.ID, binding.open.DatabaseMode)
	}
	if (binding.open.SeedStepID != nil) != (binding.open.Initialization == "seed") {
		v.parent.add("%s native action %s requires initialization seed exactly when seed_step_id is non-null", v.parent.scenario.ID, action.ID)
	}
	v.opened[clientKey] = true
	v.alive[clientKey] = true
}

func (v *nativePlanValidator) validateTerminateOrdering(index int, action NativeAction, binding nativeActionBinding) {
	if binding.boundary == nil {
		return
	}
	clientKey := binding.boundary.ClientKey
	if !v.opened[clientKey] {
		v.parent.add("%s native action %s terminates client %q before open", v.parent.scenario.ID, action.ID, clientKey)
	} else if !v.alive[clientKey] {
		v.parent.add("%s native action %s terminates client %q more than once", v.parent.scenario.ID, action.ID, clientKey)
	}
	if callID, active := v.activeCalls[clientKey]; active {
		v.parent.add("%s native action %s terminates client %q while call %q is active", v.parent.scenario.ID, action.ID, clientKey, callID)
	}
	afterIndex, found := v.actions[binding.boundary.AfterActionID]
	if !found || afterIndex >= index {
		v.parent.add("%s native action %s does not name an earlier boundary action %s", v.parent.scenario.ID, action.ID, binding.boundary.AfterActionID)
	} else if priorKey := v.bindings[afterIndex].clientKey; priorKey != nil && *priorKey != clientKey {
		v.parent.add("%s native action %s boundary action %s belongs to client %q", v.parent.scenario.ID, action.ID, binding.boundary.AfterActionID, *priorKey)
	}
	v.alive[clientKey] = false
	v.used[clientKey] = true
	v.lastTermination[clientKey] = nativeTermination{action: action.ID, boundary: binding.boundary.Boundary}
}

func (v *nativePlanValidator) validateRelaunchOrdering(action NativeAction, binding nativeActionBinding) {
	if binding.boundary == nil {
		return
	}
	clientKey := binding.boundary.ClientKey
	if !v.opened[clientKey] {
		v.parent.add("%s native action %s relaunches client %q before open", v.parent.scenario.ID, action.ID, clientKey)
	} else if v.alive[clientKey] {
		v.parent.add("%s native action %s relaunches active client %q", v.parent.scenario.ID, action.ID, clientKey)
	}
	termination, found := v.lastTermination[clientKey]
	if !found || termination.action != binding.boundary.AfterActionID || termination.boundary != binding.boundary.Boundary {
		v.parent.add("%s native action %s does not match the latest termination for client %q", v.parent.scenario.ID, action.ID, clientKey)
	}
	v.alive[clientKey] = true
	v.used[clientKey] = true
}

func (v *nativePlanValidator) validateStepClosure() {
	for _, step := range v.parent.scenario.Steps {
		actions := v.coveredSteps[step.ID]
		if len(actions) != 1 {
			v.parent.add("%s native step %s has %d covering actions, want exactly one", v.parent.scenario.ID, step.ID, len(actions))
		}
	}
}

func (v *nativePlanValidator) validateObservationClosure() {
	requiredExpectations := make(map[ExpectationID]struct{})
	requiredBudgets := make(map[contract.BudgetID]struct{})
	requiredMeasurements := make(map[contract.MeasurementID]struct{})
	for _, obligation := range v.parent.scenario.ProofObligations {
		if obligation.ProofType != "native-e2e" {
			continue
		}
		for _, assertionID := range obligation.AssertionIDs {
			assertion, found := v.parent.assertions[assertionID]
			if !found {
				continue
			}
			for _, expectationID := range assertion.ExpectationIDs {
				requiredExpectations[expectationID] = struct{}{}
			}
		}
		for _, measurementID := range obligation.RequiredMeasurementIDs {
			requiredMeasurements[measurementID] = struct{}{}
		}
		for _, budgetID := range obligation.PerformanceBudgetIDs {
			requiredBudgets[budgetID] = struct{}{}
		}
	}

	for _, expectationID := range sortedExpectationIDs(requiredExpectations, v.captured) {
		_, required := requiredExpectations[expectationID]
		count := len(v.captured[expectationID])
		if !required {
			v.parent.add("%s native capture includes extra expectation %s", v.parent.scenario.ID, expectationID)
		} else if count != 1 {
			v.parent.add("%s native expectation %s has %d capture actions, want exactly one", v.parent.scenario.ID, expectationID, count)
		}
	}
	for _, budgetID := range sortedBudgetIDs(requiredBudgets, v.measuredBudgets) {
		_, required := requiredBudgets[budgetID]
		count := len(v.measuredBudgets[budgetID])
		if !required {
			v.parent.add("%s native measure includes extra performance budget %s", v.parent.scenario.ID, budgetID)
		} else if count != 1 {
			v.parent.add("%s native performance budget %s has %d measure actions, want exactly one", v.parent.scenario.ID, budgetID, count)
		}
	}
	for _, measurementID := range sortedMeasurementIDs(requiredMeasurements, v.measured) {
		_, required := requiredMeasurements[measurementID]
		count := len(v.measured[measurementID])
		if !required {
			v.parent.add("%s native measure includes extra measurement %s", v.parent.scenario.ID, measurementID)
		} else if count != 1 {
			v.parent.add("%s native measurement %s has %d measure actions, want exactly one", v.parent.scenario.ID, measurementID, count)
		}
	}
}

func (v *nativePlanValidator) validateClientClosure() {
	for _, client := range v.plan.Clients {
		if !v.opened[client.Key] {
			v.parent.add("%s native client %q is never opened", v.parent.scenario.ID, client.Key)
		}
		if !v.used[client.Key] {
			v.parent.add("%s native client %q performs no client operation", v.parent.scenario.ID, client.Key)
		}
		if v.opened[client.Key] && !v.alive[client.Key] {
			v.parent.add("%s native client %q is not relaunched after termination", v.parent.scenario.ID, client.Key)
		}
	}
	for clientKey, callID := range v.activeCalls {
		v.parent.add("%s native call %q for client %q is not awaited", v.parent.scenario.ID, callID, clientKey)
	}
}

func decodeNativeActionParameters(action NativeAction) (nativeActionBinding, error) {
	var binding nativeActionBinding
	switch action.Actor + "/" + action.Command {
	case "controller/install-model", "controller/apply-step", "controller/request-step":
		var parameters struct{}
		return binding, decodeNativeParameterObject(action.Parameters, nil, &parameters)
	case "artifact/stage-step", "client/execute-step":
		var parameters NativeClientParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key"}, &parameters); err != nil {
			return binding, err
		}
		binding.clientKey = &parameters.ClientKey
	case "observer/await-step":
		var parameters NativeAwaitStepParameters
		var fields map[string]json.RawMessage
		if err := jsonstrict.Decode(action.Parameters, &fields); err != nil {
			return binding, err
		}
		required := []string{"client_key"}
		if _, found := fields["call_id"]; found {
			required = append(required, "call_id")
		}
		if err := decodeNativeParameterObject(action.Parameters, required, &parameters); err != nil {
			return binding, err
		}
		binding.clientKey = &parameters.ClientKey
		binding.awaitStep = &parameters
	case "client/open":
		var parameters NativeClientOpenParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key", "database_mode", "initialization", "seed_step_id"}, &parameters); err != nil {
			return binding, err
		}
		if !contains([]string{"empty", "current", "seed"}, parameters.Initialization) {
			return binding, fmt.Errorf("unknown initialization %q", parameters.Initialization)
		}
		binding.clientKey = &parameters.ClientKey
		binding.open = &parameters
	case "client/synchronize-step":
		var parameters NativeSynchronizeParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key", "method", "completion"}, &parameters); err != nil {
			return binding, err
		}
		if !contains([]string{"start", "sync-now", "retry-after-error", "reset-schema-and-start"}, parameters.Method) {
			return binding, fmt.Errorf("unknown synchronization method %q", parameters.Method)
		}
		if !contains([]string{"idle", "blocked", "error"}, parameters.Completion) {
			return binding, fmt.Errorf("unknown completion %q", parameters.Completion)
		}
		binding.clientKey = &parameters.ClientKey
		binding.sync = &parameters
	case "client/begin-call":
		var parameters NativeBeginCallParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key", "call_id", "method"}, &parameters); err != nil {
			return binding, err
		}
		if !nativeClientKeyPattern.MatchString(string(parameters.CallID)) {
			return binding, fmt.Errorf("invalid call ID %q", parameters.CallID)
		}
		if !contains([]string{"start", "sync-now", "retry-after-error", "reset-schema-and-start"}, parameters.Method) {
			return binding, fmt.Errorf("unknown synchronization method %q", parameters.Method)
		}
		binding.clientKey = &parameters.ClientKey
		binding.beginCall = &parameters
	case "client/await-call":
		var parameters NativeAwaitCallParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key", "call_id", "completion"}, &parameters); err != nil {
			return binding, err
		}
		if !nativeClientKeyPattern.MatchString(string(parameters.CallID)) {
			return binding, fmt.Errorf("invalid call ID %q", parameters.CallID)
		}
		if !contains([]string{"idle", "blocked", "error"}, parameters.Completion) {
			return binding, fmt.Errorf("unknown completion %q", parameters.Completion)
		}
		binding.clientKey = &parameters.ClientKey
		binding.awaitCall = &parameters
	case "client/lifecycle":
		var parameters NativeLifecycleParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key", "operation"}, &parameters); err != nil {
			return binding, err
		}
		if !contains([]string{"stop", "enter-background", "enter-foreground"}, parameters.Operation) {
			return binding, fmt.Errorf("unknown lifecycle operation %q", parameters.Operation)
		}
		binding.clientKey = &parameters.ClientKey
	case "process/execute-step":
		var parameters NativeProcessStepParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key"}, &parameters); err != nil {
			return binding, err
		}
		binding.clientKey = parameters.ClientKey
	case "process/terminate", "process/relaunch":
		var parameters NativeProcessBoundaryParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_key", "boundary", "after_action_id"}, &parameters); err != nil {
			return binding, err
		}
		if _, known := nativeProcessBoundaries[parameters.Boundary]; !known {
			return binding, fmt.Errorf("unknown process boundary %q", parameters.Boundary)
		}
		binding.clientKey = &parameters.ClientKey
		binding.boundary = &parameters
	case "observer/capture":
		var parameters NativeCaptureParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"client_keys", "sources", "expectation_ids"}, &parameters); err != nil {
			return binding, err
		}
		if err := validateNativeCaptureParameters(parameters); err != nil {
			return binding, err
		}
		binding.clientKeys = parameters.ClientKeys
		binding.capture = &parameters
	case "observer/measure":
		var parameters NativeMeasureParameters
		if err := decodeNativeParameterObject(action.Parameters, []string{"performance_budget_ids", "measurement_ids"}, &parameters); err != nil {
			return binding, err
		}
		if len(parameters.PerformanceBudgetIDs)+len(parameters.MeasurementIDs) == 0 || hasDuplicateBudgets(parameters.PerformanceBudgetIDs) || hasDuplicateMeasurements(parameters.MeasurementIDs) {
			return binding, errors.New("performance_budget_ids and measurement_ids must have a nonempty unique union")
		}
		binding.measure = &parameters
	default:
		return binding, fmt.Errorf("unsupported actor and command %s/%s", action.Actor, action.Command)
	}
	return binding, nil
}

func decodeNativeParameterObject(raw json.RawMessage, required []string, target any) error {
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &object); err != nil {
		return err
	}
	if err := validateObjectFields(object, operationFields{required: required}); err != nil {
		return err
	}
	return jsonstrict.Decode(raw, target)
}

func validateNativeCaptureParameters(parameters NativeCaptureParameters) error {
	if len(parameters.Sources) == 0 || len(parameters.ExpectationIDs) == 0 {
		return errors.New("sources and expectation_ids must be nonempty")
	}
	seenSources := make(map[string]struct{}, len(parameters.Sources))
	requiresClient := false
	for _, source := range parameters.Sources {
		if _, known := nativeCaptureSources[source]; !known {
			return fmt.Errorf("unknown capture source %q", source)
		}
		if _, duplicate := seenSources[source]; duplicate {
			return fmt.Errorf("duplicate capture source %q", source)
		}
		seenSources[source] = struct{}{}
		_, requiresLocalClient := nativeLocalCaptureSources[source]
		requiresClient = requiresClient || requiresLocalClient
	}
	if requiresClient && len(parameters.ClientKeys) == 0 {
		return errors.New("local capture sources require at least one client key")
	}
	seenClients := make(map[string]struct{}, len(parameters.ClientKeys))
	for _, clientKey := range parameters.ClientKeys {
		if _, duplicate := seenClients[clientKey]; duplicate {
			return fmt.Errorf("duplicate capture client %q", clientKey)
		}
		seenClients[clientKey] = struct{}{}
	}
	seenExpectations := make(map[ExpectationID]struct{}, len(parameters.ExpectationIDs))
	for _, expectationID := range parameters.ExpectationIDs {
		if _, duplicate := seenExpectations[expectationID]; duplicate {
			return fmt.Errorf("duplicate expectation ID %s", expectationID)
		}
		seenExpectations[expectationID] = struct{}{}
	}
	return nil
}

func nativeActionCoversStep(actor, command string) bool {
	switch actor + "/" + command {
	case "controller/apply-step", "controller/request-step", "artifact/stage-step", "client/execute-step", "client/synchronize-step", "client/begin-call", "process/execute-step", "observer/await-step":
		return true
	default:
		return false
	}
}

func nativeCommandSupportsStep(actor, command string, step Step) bool {
	switch actor + "/" + command {
	case "controller/apply-step":
		return step.Operation.ContractOperation == "model" || step.Operation.ContractOperation == "workload"
	case "controller/request-step":
		return step.Transport == "http"
	case "artifact/stage-step":
		return step.Operation.ContractOperation == "artifact"
	case "client/execute-step":
		return OperationKey(step.Operation) == "local/write"
	case "client/synchronize-step":
		return step.Transport == "http"
	case "client/begin-call":
		return step.Transport == "http"
	case "process/execute-step":
		return step.Operation.ContractOperation == "process"
	case "observer/await-step":
		return step.Transport == "http" || step.Operation.ContractOperation == "local" && step.Operation.Name != "write"
	default:
		return false
	}
}

func nativeOperationIdentity(operation Operation) (string, string, bool, error) {
	object, err := decodePayloadObject(operation.Payload)
	if err != nil {
		return "", "", false, err
	}
	switch operation.ContractOperation {
	case "artifact", "connect", "pull", "rebuild":
		return stringValue(object["user_id"]), stringValue(object["client_id"]), true, nil
	case "local":
		userField := "user_id"
		if operation.Name == "write" {
			userField = "authenticated_user_id"
		}
		return stringValue(object[userField]), stringValue(object["client_id"]), true, nil
	case "push":
		request, err := decodePayloadObject(object["request"])
		if err != nil {
			return "", "", false, err
		}
		return stringValue(object["authenticated_user_id"]), stringValue(request["client_id"]), true, nil
	case "process":
		if operation.Name == "response-loss" {
			return stringValue(object["authenticated_user_id"]), stringValue(object["client_id"]), true, nil
		}
		if operation.Name == "restart-client" {
			return stringValue(object["user_id"]), stringValue(object["client_id"]), true, nil
		}
	case "workload":
		userID := stringValue(object["user_id"])
		clientID := stringValue(object["client_id"])
		if userID != "" || clientID != "" {
			return userID, clientID, true, nil
		}
	}
	return "", "", false, nil
}

func (v *nativePlanValidator) validateSynchronizationCompletion(action NativeAction, binding nativeActionBinding, step Step) {
	if binding.sync == nil {
		return
	}
	wire, found := v.parent.wireByStep[step.ID]
	if !found {
		return
	}
	want := "error"
	if wire.HTTPStatus >= 200 && wire.HTTPStatus < 300 {
		want = "idle"
	} else if wire.Retryable || wire.HTTPStatus == 0 {
		want = "blocked"
	}
	if binding.sync.Completion != want {
		v.parent.add("%s native action %s completion %q does not match step %s terminal outcome %q", v.parent.scenario.ID, action.ID, binding.sync.Completion, step.ID, want)
	}
}

func sortedExpectationIDs(required map[ExpectationID]struct{}, actual map[ExpectationID][]NativeActionID) []ExpectationID {
	set := make(map[ExpectationID]struct{}, len(required)+len(actual))
	for id := range required {
		set[id] = struct{}{}
	}
	for id := range actual {
		set[id] = struct{}{}
	}
	values := make([]ExpectationID, 0, len(set))
	for id := range set {
		values = append(values, id)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func sortedMeasurementIDs(required map[contract.MeasurementID]struct{}, actual map[contract.MeasurementID][]NativeActionID) []contract.MeasurementID {
	set := make(map[contract.MeasurementID]struct{}, len(required)+len(actual))
	for id := range required {
		set[id] = struct{}{}
	}
	for id := range actual {
		set[id] = struct{}{}
	}
	values := make([]contract.MeasurementID, 0, len(set))
	for id := range set {
		values = append(values, id)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func sortedBudgetIDs(required map[contract.BudgetID]struct{}, actual map[contract.BudgetID][]NativeActionID) []contract.BudgetID {
	set := make(map[contract.BudgetID]struct{}, len(required)+len(actual))
	for id := range required {
		set[id] = struct{}{}
	}
	for id := range actual {
		set[id] = struct{}{}
	}
	values := make([]contract.BudgetID, 0, len(set))
	for id := range set {
		values = append(values, id)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func hasDuplicateBudgets(ids []contract.BudgetID) bool {
	seen := make(map[contract.BudgetID]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

func hasDuplicateMeasurements(ids []contract.MeasurementID) bool {
	seen := make(map[contract.MeasurementID]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

func dereferenceString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
