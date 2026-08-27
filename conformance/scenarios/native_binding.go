package scenarios

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const maxNativeIdentityInteger = int64(1<<53 - 1)

var nativePublicMethods = stringSet([]string{
	"retry-after-error",
	"reset-schema-and-start",
	"start",
	"sync-now",
})

var nativeLifecycleMethods = stringSet([]string{"stop"})

var nativeCallStages = stringSet([]string{
	"await-call",
	"await-step",
	"begin",
	"synchronous",
})

var nativeBindingTransports = map[string]map[string]struct{}{
	"artifact":    {"artifact": {}},
	"controller":  {"http": {}, "model": {}, "process": {}},
	"local-write": {"local": {}},
	"process":     {"process": {}},
	"public-call": {"http": {}, "local": {}, "process": {}},
}

var nativeIdentityKinds = stringSet([]string{
	"batch-id",
	"checksum",
	"client-generation",
	"cursor",
	"mutation-id",
	"primary-key",
	"rebuild-id",
	"row-version",
	"schema",
	"scope",
	"scope-set-version",
	"server-version",
	"table",
})

type nativeCallGroup struct {
	clientID    string
	userID      string
	method      string
	completion  string
	phase       string
	hasHTTP     bool
	stages      []string
	stepIndexes []int
}

func (v *scenarioValidator) validateNativeProof() {
	hasNativeObligation := false
	for _, obligation := range v.scenario.ProofObligations {
		if obligation.ProofType == "native-e2e" {
			hasNativeObligation = true
			break
		}
	}

	boundSteps := 0
	for _, step := range v.scenario.Steps {
		if step.NativeBinding != nil {
			boundSteps++
		}
	}
	if !hasNativeObligation {
		if v.scenario.NativeExecution != nil {
			v.add("%s has native_execution without a native-e2e proof obligation", v.scenario.ID)
		}
		if boundSteps != 0 {
			v.add("%s has native bindings without a native-e2e proof obligation", v.scenario.ID)
		}
		if len(v.scenario.NativeIdentityAliases) != 0 {
			v.add("%s has native identity aliases without native step bindings", v.scenario.ID)
		}
		if len(v.scenario.NativeLifecycleBoundaries) != 0 {
			v.add("%s has native lifecycle boundaries without native step bindings", v.scenario.ID)
		}
		return
	}
	if v.scenario.NativeExecution != nil {
		if boundSteps != 0 {
			v.add("%s must not mix native_execution with native step bindings", v.scenario.ID)
			return
		}
		if len(v.scenario.NativeIdentityAliases) != 0 {
			v.add("%s native_execution must not contain native identity aliases", v.scenario.ID)
		}
		if len(v.scenario.NativeLifecycleBoundaries) != 0 {
			v.add("%s native_execution must not contain native lifecycle boundaries", v.scenario.ID)
		}
		v.validateNativeExecution()
		return
	}
	if boundSteps != len(v.scenario.Steps) {
		v.add("%s native-e2e proof obligations require native_execution or native bindings on every step", v.scenario.ID)
	}
	if len(v.scenario.NativeIdentityAliases) == 0 {
		v.add("%s native step bindings require native identity aliases", v.scenario.ID)
	} else {
		v.validateNativeIdentityAliases()
	}
	v.validateNativeStepBindings()
	v.validateNativeLifecycleBoundaries()
}

func (v *scenarioValidator) validateNativeIdentityAliases() {
	aliases := make(map[string]struct{}, len(v.scenario.NativeIdentityAliases))
	values := make(map[string]string, len(v.scenario.NativeIdentityAliases))
	steps := make(map[StepID]struct{}, len(v.scenario.Steps))
	for _, step := range v.scenario.Steps {
		steps[step.ID] = struct{}{}
	}
	expectations := make(map[ExpectationID]struct{}, len(v.scenario.Model.ExpectedState))
	for _, expectation := range v.scenario.Model.ExpectedState {
		expectations[expectation.ID] = struct{}{}
	}
	for _, identity := range v.scenario.NativeIdentityAliases {
		if _, known := nativeIdentityKinds[identity.Kind]; !known {
			v.add("%s has unknown native identity kind %q", v.scenario.ID, identity.Kind)
		}
		if !nativeClientKeyPattern.MatchString(identity.Alias) {
			v.add("%s native identity kind %q has invalid alias %q", v.scenario.ID, identity.Kind, identity.Alias)
		}
		if _, duplicate := aliases[identity.Alias]; duplicate {
			v.add("%s repeats native identity alias %q", v.scenario.ID, identity.Alias)
		}
		aliases[identity.Alias] = struct{}{}

		value, err := canonicalNativeIdentityValue(identity.Value)
		if err != nil {
			v.add("%s native identity alias %q has invalid value: %v", v.scenario.ID, identity.Alias, err)
			continue
		}
		valueKey := identity.Kind + "\x00" + value
		if previous, duplicate := values[valueKey]; duplicate {
			v.add("%s native identity aliases %q and %q for kind %q share one authored value", v.scenario.ID, previous, identity.Alias, identity.Kind)
		} else {
			values[valueKey] = identity.Alias
		}

		if len(identity.StepIDs) == 0 && len(identity.ExpectationIDs) == 0 {
			v.add("%s native identity alias %q must bind at least one step or expectation", v.scenario.ID, identity.Alias)
		}
		seenSteps := make(map[StepID]struct{}, len(identity.StepIDs))
		for _, stepID := range identity.StepIDs {
			if _, duplicate := seenSteps[stepID]; duplicate {
				v.add("%s native identity alias %q repeats step %s", v.scenario.ID, identity.Alias, stepID)
			}
			seenSteps[stepID] = struct{}{}
			if _, found := steps[stepID]; !found {
				v.add("%s native identity alias %q references unknown step %s", v.scenario.ID, identity.Alias, stepID)
			}
		}
		seenExpectations := make(map[ExpectationID]struct{}, len(identity.ExpectationIDs))
		for _, expectationID := range identity.ExpectationIDs {
			if _, duplicate := seenExpectations[expectationID]; duplicate {
				v.add("%s native identity alias %q repeats expectation %s", v.scenario.ID, identity.Alias, expectationID)
			}
			seenExpectations[expectationID] = struct{}{}
			if _, found := expectations[expectationID]; !found {
				v.add("%s native identity alias %q references unknown expectation %s", v.scenario.ID, identity.Alias, expectationID)
			}
		}
	}
}

func (v *scenarioValidator) validateNativeLifecycleBoundaries() {
	stepIndexes := make(map[StepID]int, len(v.scenario.Steps))
	for index, step := range v.scenario.Steps {
		stepIndexes[step.ID] = index
	}
	ids := make(map[string]struct{}, len(v.scenario.NativeLifecycleBoundaries))
	placements := make(map[string]struct{}, len(v.scenario.NativeLifecycleBoundaries))
	for _, boundary := range v.scenario.NativeLifecycleBoundaries {
		if !nativeClientKeyPattern.MatchString(boundary.ID) {
			v.add("%s has invalid native lifecycle boundary ID %q", v.scenario.ID, boundary.ID)
		}
		if _, duplicate := ids[boundary.ID]; duplicate {
			v.add("%s repeats native lifecycle boundary ID %q", v.scenario.ID, boundary.ID)
		}
		ids[boundary.ID] = struct{}{}
		if _, known := nativeLifecycleMethods[boundary.Method]; !known {
			v.add("%s native lifecycle boundary %q has unknown method %q", v.scenario.ID, boundary.ID, boundary.Method)
		}
		index, found := stepIndexes[boundary.AfterStepID]
		if !found {
			v.add("%s native lifecycle boundary %q references unknown step %s", v.scenario.ID, boundary.ID, boundary.AfterStepID)
			continue
		}
		step := v.scenario.Steps[index]
		if boundary.Phase != step.Phase {
			v.add("%s native lifecycle boundary %q phase does not match step %s", v.scenario.ID, boundary.ID, step.ID)
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "public-call" || binding.UserID != boundary.UserID || binding.ClientID != boundary.ClientID {
			v.add("%s native lifecycle boundary %q must follow a public call for the same client", v.scenario.ID, boundary.ID)
			continue
		}
		if binding.Stage != "synchronous" && binding.Stage != "await-call" {
			v.add("%s native lifecycle boundary %q must follow the terminal step of call %q", v.scenario.ID, boundary.ID, dereferenceNativeCallID(binding.CallID))
		}
		for _, laterStep := range v.scenario.Steps[index+1:] {
			later := laterStep.NativeBinding
			if later != nil && later.Kind == "public-call" && dereferenceNativeCallID(later.CallID) == dereferenceNativeCallID(binding.CallID) {
				v.add("%s native lifecycle boundary %q must follow the terminal step of call %q", v.scenario.ID, boundary.ID, dereferenceNativeCallID(binding.CallID))
				break
			}
		}
		placement := string(boundary.AfterStepID) + "\x00" + boundary.UserID + "\x00" + boundary.ClientID
		if _, duplicate := placements[placement]; duplicate {
			v.add("%s repeats a native lifecycle boundary after step %s for client %q", v.scenario.ID, boundary.AfterStepID, boundary.ClientID)
		}
		placements[placement] = struct{}{}
	}
}

func canonicalNativeIdentityValue(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("value is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	if value == nil {
		return "", fmt.Errorf("value must not be null")
	}
	if err := validateNativeIdentityNumbers(value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

func validateNativeIdentityNumbers(value any) error {
	switch typed := value.(type) {
	case json.Number:
		integer, err := typed.Int64()
		if err != nil {
			return fmt.Errorf("number must be an integer")
		}
		if integer < -maxNativeIdentityInteger || integer > maxNativeIdentityInteger {
			return fmt.Errorf("integer exceeds exact JSON range")
		}
	case []any:
		for _, item := range typed {
			if err := validateNativeIdentityNumbers(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range typed {
			if err := validateNativeIdentityNumbers(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *scenarioValidator) validateNativeStepBindings() {
	calls := make(map[NativeCallID]nativeCallGroup)
	closedCalls := make(map[NativeCallID]struct{})
	var activeStagedCall NativeCallID
	var currentSynchronousCall NativeCallID

	for index, step := range v.scenario.Steps {
		binding := step.NativeBinding
		if binding == nil {
			if activeStagedCall != "" {
				v.add("%s native call %q is interrupted by an unbound step %s", v.scenario.ID, activeStagedCall, step.ID)
			}
			if currentSynchronousCall != "" {
				closedCalls[currentSynchronousCall] = struct{}{}
				currentSynchronousCall = ""
			}
			continue
		}
		transports, known := nativeBindingTransports[binding.Kind]
		if !known {
			v.add("%s step %s has unknown native binding kind %q", v.scenario.ID, step.ID, binding.Kind)
			continue
		}
		if _, permitted := transports[step.Transport]; !permitted {
			v.add("%s step %s native binding %q cannot own transport %q", v.scenario.ID, step.ID, binding.Kind, step.Transport)
		} else if !nativeBindingOwnsOperation(binding.Kind, step) {
			v.add("%s step %s native binding %q cannot own operation %q", v.scenario.ID, step.ID, binding.Kind, OperationKey(step.Operation))
		}
		v.validateNativeBindingShape(step, *binding)

		if binding.Kind != "public-call" {
			if activeStagedCall != "" && binding.Kind != "controller" {
				v.add("%s native call %q is interrupted by binding %q on step %s", v.scenario.ID, activeStagedCall, binding.Kind, step.ID)
			}
			if currentSynchronousCall != "" {
				closedCalls[currentSynchronousCall] = struct{}{}
				currentSynchronousCall = ""
			}
			continue
		}
		callID := dereferenceNativeCallID(binding.CallID)
		if callID == "" {
			continue
		}
		group, found := calls[callID]
		if !found {
			group = nativeCallGroup{
				clientID: binding.ClientID,
				userID:   binding.UserID,
				phase:    step.Phase,
			}
		} else if group.clientID != binding.ClientID || group.userID != binding.UserID {
			v.add("%s native call %q has inconsistent client, method, completion, or phase", v.scenario.ID, callID)
		} else if group.phase != step.Phase && (group.stages[0] == "synchronous" || binding.Stage == "synchronous") {
			v.add("%s synchronous native call %q crosses phase %q", v.scenario.ID, callID, step.Phase)
		}
		if binding.Method != "" {
			if group.method != "" && group.method != binding.Method {
				v.add("%s native call %q has inconsistent client, method, completion, or phase", v.scenario.ID, callID)
			}
			group.method = binding.Method
		}
		if binding.Completion != "" {
			if group.completion != "" && group.completion != binding.Completion {
				v.add("%s native call %q has inconsistent client, method, completion, or phase", v.scenario.ID, callID)
			}
			group.completion = binding.Completion
		}
		if step.Transport == "http" {
			group.hasHTTP = true
		}
		group.stages = append(group.stages, binding.Stage)
		group.stepIndexes = append(group.stepIndexes, index)
		calls[callID] = group

		switch binding.Stage {
		case "synchronous":
			if activeStagedCall != "" {
				v.add("%s native call %q overlaps active call %q", v.scenario.ID, callID, activeStagedCall)
			}
			if currentSynchronousCall != callID {
				if _, closed := closedCalls[callID]; closed {
					v.add("%s native call %q for client %q resumes after another call or binding", v.scenario.ID, callID, binding.ClientID)
				}
				if currentSynchronousCall != "" {
					closedCalls[currentSynchronousCall] = struct{}{}
				}
				currentSynchronousCall = callID
			}
		case "begin":
			if currentSynchronousCall != "" {
				closedCalls[currentSynchronousCall] = struct{}{}
				currentSynchronousCall = ""
			}
			if activeStagedCall != "" {
				v.add("%s native call %q overlaps active call %q", v.scenario.ID, callID, activeStagedCall)
			}
			if _, closed := closedCalls[callID]; closed {
				v.add("%s native call %q for client %q resumes after another call or binding", v.scenario.ID, callID, binding.ClientID)
			}
			activeStagedCall = callID
		case "await-step", "await-call":
			if currentSynchronousCall != "" {
				closedCalls[currentSynchronousCall] = struct{}{}
				currentSynchronousCall = ""
			}
			if activeStagedCall != callID {
				v.add("%s native call %q must begin before stage %q", v.scenario.ID, callID, binding.Stage)
			}
			if binding.Stage == "await-call" {
				closedCalls[callID] = struct{}{}
				activeStagedCall = ""
			}
		}
	}

	for callID, group := range calls {
		if !group.hasHTTP {
			v.add("%s native call %q covers no HTTP step", v.scenario.ID, callID)
			continue
		}
		v.validateNativeCallStages(callID, group)
		want, terminalStepID := v.nativeCallCompletion(group)
		if group.completion != want {
			v.add("%s native call %q completion %q does not match terminal step %s outcome %q", v.scenario.ID, callID, group.completion, terminalStepID, want)
		}
	}
}

func (v *scenarioValidator) validateNativeCallStages(callID NativeCallID, group nativeCallGroup) {
	if len(group.stages) == 0 {
		return
	}
	if group.stages[0] == "synchronous" {
		for _, stage := range group.stages {
			if stage != "synchronous" {
				v.add("%s native call %q mixes synchronous and staged execution", v.scenario.ID, callID)
				return
			}
		}
		return
	}
	if group.stages[0] != "begin" {
		v.add("%s native call %q must begin with stage %q", v.scenario.ID, callID, "begin")
	}
	if group.stages[len(group.stages)-1] != "await-call" {
		v.add("%s native call %q must end with one terminal await-call stage", v.scenario.ID, callID)
	}
	for index, stage := range group.stages {
		want := "await-step"
		if index == 0 {
			want = "begin"
		} else if index == len(group.stages)-1 {
			want = "await-call"
		}
		if stage != want {
			v.add("%s native call %q stage %d is %q, want %q", v.scenario.ID, callID, index+1, stage, want)
		}
	}
}

func nativeBindingOwnsOperation(kind string, step Step) bool {
	key := OperationKey(step.Operation)
	switch kind {
	case "controller":
		return step.Transport == "http" || step.Operation.ContractOperation == "model" || step.Operation.ContractOperation == "process" && key != "process/restart-client" && key != "process/response-loss"
	case "artifact":
		return step.Operation.ContractOperation == "artifact"
	case "local-write":
		return key == "local/write"
	case "process":
		return key == "process/restart-client"
	case "public-call":
		return step.Transport == "http" || step.Operation.ContractOperation == "local" && key != "local/write" || key == "process/response-loss"
	default:
		return false
	}
}

func (v *scenarioValidator) validateNativeBindingShape(step Step, binding NativeStepBinding) {
	hasClient := binding.UserID != "" && binding.ClientID != ""
	hasCall := binding.CallID != nil && *binding.CallID != ""
	if binding.Kind == "controller" {
		if binding.UserID != "" || binding.ClientID != "" || hasCall || binding.Stage != "" || binding.Method != "" || binding.Completion != "" {
			v.add("%s step %s controller native binding must not contain client call fields", v.scenario.ID, step.ID)
		}
	} else if !hasClient {
		v.add("%s step %s native binding %q requires user_id and client_id", v.scenario.ID, step.ID, binding.Kind)
	}

	if binding.Kind == "public-call" {
		if !hasCall || !nativeClientKeyPattern.MatchString(string(*binding.CallID)) {
			v.add("%s step %s public native binding has invalid call_id", v.scenario.ID, step.ID)
		}
		if _, known := nativeCallStages[binding.Stage]; !known {
			v.add("%s step %s public native binding has unknown stage %q", v.scenario.ID, step.ID, binding.Stage)
		}
		switch binding.Stage {
		case "synchronous":
			v.validateNativeCallMethod(step, binding.Method)
			v.validateNativeCallCompletion(step, binding.Completion)
		case "begin":
			v.validateNativeCallMethod(step, binding.Method)
			if binding.Completion != "" {
				v.add("%s step %s begin native binding must not declare completion", v.scenario.ID, step.ID)
			}
		case "await-step":
			if binding.Method != "" || binding.Completion != "" {
				v.add("%s step %s await-step native binding must not declare method or completion", v.scenario.ID, step.ID)
			}
		case "await-call":
			if binding.Method != "" {
				v.add("%s step %s await-call native binding must not declare method", v.scenario.ID, step.ID)
			}
			v.validateNativeCallCompletion(step, binding.Completion)
		}
	} else if hasCall || binding.Stage != "" || binding.Method != "" || binding.Completion != "" {
		v.add("%s step %s native binding %q must not contain public call fields", v.scenario.ID, step.ID, binding.Kind)
	}

	switch binding.Kind {
	case "artifact", "local-write", "process", "public-call":
		userID, clientID, hasIdentity, err := nativeOperationIdentity(step.Operation)
		if err != nil {
			v.add("%s step %s cannot resolve native client identity: %v", v.scenario.ID, step.ID, err)
		} else if !hasIdentity || userID != binding.UserID || clientID != binding.ClientID {
			v.add("%s step %s native binding client identity does not match the authored operation", v.scenario.ID, step.ID)
		}
	}
	if binding.Kind == "controller" && step.Operation.ContractOperation == "workload" {
		v.add("%s step %s native controller binding cannot execute a workload macro", v.scenario.ID, step.ID)
	}
}

func (v *scenarioValidator) validateNativeCallMethod(step Step, method string) {
	if _, known := nativePublicMethods[method]; !known {
		v.add("%s step %s public native binding has unknown method %q", v.scenario.ID, step.ID, method)
	}
}

func (v *scenarioValidator) validateNativeCallCompletion(step Step, completion string) {
	if !contains([]string{"idle", "blocked", "error"}, completion) {
		v.add("%s step %s public native binding has unknown completion %q", v.scenario.ID, step.ID, completion)
	}
}

func (v *scenarioValidator) nativeCallCompletion(group nativeCallGroup) (string, StepID) {
	if len(group.stages) != 0 && group.stages[0] != "synchronous" {
		index := group.stepIndexes[len(group.stepIndexes)-1]
		step := v.scenario.Steps[index]
		completion := "idle"
		if wire, found := v.wireByStep[step.ID]; found {
			completion = nativeCompletion(wire)
		}
		if step.ExpectedOutcome.Disposition == "error" {
			completion = "error"
		}
		return completion, step.ID
	}

	completion := "idle"
	var terminalStepID StepID
	failed := false
	for _, index := range group.stepIndexes {
		step := v.scenario.Steps[index]
		if failed {
			if OperationKey(step.Operation) != "process/response-loss" {
				v.add("%s native call has effect %s after terminal step %s", v.scenario.ID, step.ID, terminalStepID)
			}
			continue
		}
		terminalStepID = step.ID
		if wire, found := v.wireByStep[step.ID]; found {
			completion = nativeCompletion(wire)
		}
		if step.ExpectedOutcome.Disposition == "error" {
			completion = "error"
		}
		failed = completion != "idle"
	}
	return completion, terminalStepID
}

func nativeCompletion(wire WireExpectation) string {
	if wire.HTTPStatus >= 200 && wire.HTTPStatus < 300 {
		return "idle"
	}
	if wire.Retryable || wire.HTTPStatus == 0 {
		return "blocked"
	}
	return "error"
}

func dereferenceNativeCallID(value *NativeCallID) NativeCallID {
	if value == nil {
		return ""
	}
	return *value
}
