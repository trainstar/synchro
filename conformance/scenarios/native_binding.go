package scenarios

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxNativeIdentityInteger = int64(1<<53 - 1)
	maxNativeWorkloadRecords = uint64(1000)
	maxNativeWorkloadTargets = 8
	maxNativeWorkloadKinds   = 8
	maxNativeWorkloadSeed    = uint64(maxNativeIdentityInteger)
)

var nativePublicMethods = stringSet([]string{
	"retry-after-error",
	"reset-schema-and-start",
	"start",
	"sync-now",
})

var nativeLifecycleMethods = stringSet([]string{"stop"})

var nativeClientKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

var nativeCallStages = stringSet([]string{
	"await-call",
	"await-step",
	"begin",
	// observe opens a call the client starts on its own. The contract requires
	// the client to recover from some conditions without an application call,
	// so no method begins that work and the harness only observes it.
	"observe",
	"synchronous",
})

var nativeBindingTransports = map[string]map[string]struct{}{
	"artifact":    {"artifact": {}},
	"controller":  {"http": {}, "model": {}, "process": {}},
	"local-write": {"local": {}},
	"process":     {"process": {}},
	"public-call": {"http": {}, "local": {}, "process": {}},
	"workload":    {"model": {}},
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
	clientID      string
	userID        string
	method        string
	completion    string
	phase         string
	hasHTTP       bool
	stages        []string
	stepIndexes   []int
	checkpoint    string
	interruptedBy StepID
}

// SchemaProofBaselineWrite binds a proof row capture to its authored source insert.
// Swift and Kotlin use the returned operation only for row comparison.
func SchemaProofBaselineWrite(scenario Scenario, write Operation) (Operation, error) {
	var payload map[string]json.RawMessage
	var identity struct {
		TableID string                     `json:"table_id"`
		PK      map[string]json.RawMessage `json:"pk"`
	}
	if json.Unmarshal(write.Payload, &payload) != nil || json.Unmarshal(write.Payload, &identity) != nil || len(identity.PK) != 1 {
		return Operation{}, fmt.Errorf("proof baseline write identity is invalid")
	}
	var primaryField string
	var primary json.RawMessage
	for name, value := range identity.PK {
		primaryField, primary = name, value
	}
	for _, step := range scenario.Steps {
		if step.ID != "STEP-PERF-SCHEMA-CHECK-PROOF-BASELINE-COMMIT-001" {
			continue
		}
		var transaction struct {
			Events []struct {
				Operation string `json:"operation"`
				After     struct {
					Identity struct {
						Row struct {
							TableID   string `json:"table_id"`
							Canonical string `json:"canonical_wire_json"`
						} `json:"synced_row"`
					} `json:"identity"`
					Fields []struct {
						Field    string          `json:"field"`
						WireJSON json.RawMessage `json:"wire_json"`
					} `json:"fields"`
				} `json:"after"`
			} `json:"events"`
		}
		if json.Unmarshal(step.Operation.Payload, &transaction) != nil {
			return Operation{}, fmt.Errorf("proof baseline transaction is invalid")
		}
		for _, event := range transaction.Events {
			if event.Operation != "insert" || event.After.Identity.Row.TableID != identity.TableID {
				continue
			}
			var row json.RawMessage
			if json.Unmarshal([]byte(event.After.Identity.Row.Canonical), &row) != nil {
				continue
			}
			var expected, actual any
			if json.Unmarshal(primary, &expected) != nil || json.Unmarshal(row, &actual) != nil || !reflect.DeepEqual(expected, actual) {
				continue
			}
			columns := make(map[string]json.RawMessage)
			for _, field := range event.After.Fields {
				if field.Field != primaryField {
					var text string
					if json.Unmarshal(field.WireJSON, &text) != nil {
						return Operation{}, fmt.Errorf("proof baseline field wire JSON is invalid")
					}
					columns[field.Field] = json.RawMessage(text)
				}
			}
			if len(columns) == 0 {
				return Operation{}, fmt.Errorf("proof baseline fields are absent")
			}
			encoded, err := json.Marshal(columns)
			if err != nil {
				return Operation{}, err
			}
			payload["columns"] = encoded
			write.Payload, err = json.Marshal(payload)
			return write, err
		}
	}
	return Operation{}, fmt.Errorf("proof baseline source insert is absent")
}

func ValidateNativeLocalFixture(fixture *NativeLocalFixture) error {
	if fixture == nil {
		return nil
	}
	name := fixture.TableName
	if len(name) == 0 || len(name) > 128 || strings.HasPrefix(strings.ToLower(name), "_synchro_") || strings.HasPrefix(strings.ToLower(name), "sqlite_") {
		return fmt.Errorf("local fixture table name is invalid")
	}
	for index, character := range []byte(name) {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character == '_' || index > 0 && character >= '0' && character <= '9') {
			return fmt.Errorf("local fixture table name is invalid")
		}
	}
	if !utf8.ValidString(fixture.ID) || !utf8.ValidString(fixture.Value) || len(fixture.ID) == 0 || len(fixture.ID) > 256 || len(fixture.Value) == 0 || len(fixture.Value) > 1024 {
		return fmt.Errorf("local fixture values are invalid")
	}
	return nil
}

func (v *scenarioValidator) validateNativeProof() {
	if err := ValidateNativeLocalFixture(v.scenario.NativeLocalFixture); err != nil {
		v.add("%s: %v", v.scenario.ID, err)
	}
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
		if v.scenario.NativeLocalFixture != nil {
			v.add("%s local fixture requires native proof ownership", v.scenario.ID)
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
	if boundSteps != len(v.scenario.Steps) {
		v.add("%s native-e2e proof obligations require native bindings on every step", v.scenario.ID)
	}
	if len(v.scenario.NativeIdentityAliases) == 0 {
		v.add("%s native step bindings require native identity aliases", v.scenario.ID)
	} else {
		v.validateNativeIdentityAliases()
	}
	v.validateNativeWorkloadBindings()
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
			} else if v.steps[stepID].NativeBinding != nil && v.steps[stepID].NativeBinding.Kind == "workload" {
				v.add("%s native identity alias %q must not bind generated workload step %s", v.scenario.ID, identity.Alias, stepID)
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

func (v *scenarioValidator) validateNativeWorkloadBindings() {
	for _, step := range v.scenario.Steps {
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "workload" {
			continue
		}
		v.validateNativeWorkload(step, *binding)
	}
}

func (v *scenarioValidator) validateNativeWorkload(step Step, binding NativeStepBinding) {
	if binding.Workload == nil {
		v.add("%s step %s workload native binding requires workload parameters", v.scenario.ID, step.ID)
		return
	}
	parameters := binding.Workload
	if parameters.RecordCount == 0 || parameters.RecordCount > maxNativeWorkloadRecords {
		v.add("%s step %s workload record_count must be between 1 and %d", v.scenario.ID, step.ID, maxNativeWorkloadRecords)
	}
	if parameters.BatchSize == 0 || parameters.BatchSize > maxNativeWorkloadRecords || parameters.BatchSize > parameters.RecordCount {
		v.add("%s step %s workload batch_size must be between 1 and record_count", v.scenario.ID, step.ID)
	}
	if parameters.Seed == 0 || parameters.Seed > maxNativeWorkloadSeed {
		v.add("%s step %s workload seed must be nonzero and deterministic", v.scenario.ID, step.ID)
	}
	if parameters.AuthoredSchema.Version == 0 || !isNativeSHA256(parameters.AuthoredSchema.Hash) {
		v.add("%s step %s workload authored_schema is invalid", v.scenario.ID, step.ID)
	}
	if parameters.ClientVersion == "" {
		v.add("%s step %s workload client_version is required", v.scenario.ID, step.ID)
	}
	if len(parameters.Targets) == 0 || len(parameters.Targets) > maxNativeWorkloadTargets {
		v.add("%s step %s workload must have between 1 and %d targets", v.scenario.ID, step.ID, maxNativeWorkloadTargets)
	}
	if len(parameters.MutationKinds) == 0 || len(parameters.MutationKinds) > maxNativeWorkloadKinds {
		v.add("%s step %s workload must have between 1 and %d mutation kinds", v.scenario.ID, step.ID, maxNativeWorkloadKinds)
	}

	targets := make(map[string]struct{}, len(parameters.Targets))
	expectedScopes := make(map[string]uint64, len(parameters.Targets))
	for index, target := range parameters.Targets {
		if !nativeClientKeyPattern.MatchString(target.ScopeID) || !nativeClientKeyPattern.MatchString(target.TableID) || !nativeClientKeyPattern.MatchString(target.PrimaryKeyFieldID) {
			v.add("%s step %s workload target %d has invalid scope, table, or primary key field", v.scenario.ID, step.ID, index+1)
		}
		key := target.ScopeID + "\x00" + target.TableID + "\x00" + target.PrimaryKeyFieldID
		if _, duplicate := targets[key]; duplicate {
			v.add("%s step %s workload repeats target %q", v.scenario.ID, step.ID, key)
		}
		targets[key] = struct{}{}
	}
	if len(parameters.Targets) != 0 && parameters.RecordCount != 0 {
		for ordinal := uint64(0); ordinal < parameters.RecordCount; ordinal++ {
			target := parameters.Targets[ordinal%uint64(len(parameters.Targets))]
			expectedScopes[target.ScopeID]++
		}
	}

	mutationCount := uint64(0)
	for index, kind := range parameters.MutationKinds {
		if kind.Operation != "insert" {
			v.add("%s step %s workload mutation kind %d must be insert", v.scenario.ID, step.ID, index+1)
		}
		if kind.Count == 0 || kind.Count > maxNativeWorkloadRecords {
			v.add("%s step %s workload mutation kind %d count must be between 1 and %d", v.scenario.ID, step.ID, index+1, maxNativeWorkloadRecords)
		}
		fields := make(map[string]struct{}, len(kind.FieldIDs))
		if len(kind.FieldIDs) == 0 {
			v.add("%s step %s workload mutation kind %d requires writable fields", v.scenario.ID, step.ID, index+1)
		}
		for _, fieldID := range kind.FieldIDs {
			if !nativeClientKeyPattern.MatchString(fieldID) {
				v.add("%s step %s workload mutation kind %d has invalid field %q", v.scenario.ID, step.ID, index+1, fieldID)
			}
			if _, duplicate := fields[fieldID]; duplicate {
				v.add("%s step %s workload mutation kind %d repeats field %q", v.scenario.ID, step.ID, index+1, fieldID)
			}
			fields[fieldID] = struct{}{}
		}
		if kind.Count > maxNativeWorkloadRecords || mutationCount > maxNativeWorkloadRecords-kind.Count {
			v.add("%s step %s workload mutation counts exceed %d", v.scenario.ID, step.ID, maxNativeWorkloadRecords)
			continue
		}
		mutationCount += kind.Count
	}
	if mutationCount != parameters.RecordCount {
		v.add("%s step %s workload mutation counts total %d, want record_count %d", v.scenario.ID, step.ID, mutationCount, parameters.RecordCount)
	}

	expectation := parameters.Expectation
	if expectation.OperationCount != parameters.RecordCount {
		v.add("%s step %s workload expected operation_count %d, want record_count %d", v.scenario.ID, step.ID, expectation.OperationCount, parameters.RecordCount)
	}
	if parameters.BatchSize != 0 {
		wantBatches := (parameters.RecordCount + parameters.BatchSize - 1) / parameters.BatchSize
		if expectation.BatchCount != wantBatches {
			v.add("%s step %s workload expected batch_count %d, want %d", v.scenario.ID, step.ID, expectation.BatchCount, wantBatches)
		}
	}
	if !isNativeSHA256(expectation.OperationDigest) {
		v.add("%s step %s workload expected operation_digest is invalid", v.scenario.ID, step.ID)
	}
	actualScopes := make(map[string]uint64, len(expectation.PerScopeCardinalities))
	seenScopes := make(map[string]struct{}, len(expectation.PerScopeCardinalities))
	for _, cardinality := range expectation.PerScopeCardinalities {
		if !nativeClientKeyPattern.MatchString(cardinality.ScopeID) || cardinality.Cardinality == 0 || cardinality.Cardinality > maxNativeWorkloadRecords {
			v.add("%s step %s workload expected scope cardinality is invalid", v.scenario.ID, step.ID)
		}
		if _, duplicate := seenScopes[cardinality.ScopeID]; duplicate {
			v.add("%s step %s workload repeats expected scope %q", v.scenario.ID, step.ID, cardinality.ScopeID)
		}
		seenScopes[cardinality.ScopeID] = struct{}{}
		actualScopes[cardinality.ScopeID] = cardinality.Cardinality
	}
	if !nativeWorkloadScopeCardinalitiesEqual(expectedScopes, actualScopes) {
		v.add("%s step %s workload expected per-scope cardinalities do not close generated targets", v.scenario.ID, step.ID)
	}
}

func nativeWorkloadScopeCardinalitiesEqual(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for scopeID, cardinality := range left {
		if right[scopeID] != cardinality {
			return false
		}
	}
	return true
}

func isNativeSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
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
				group := calls[activeStagedCall]
				userID, clientID, _, identityErr := nativeOperationIdentity(step.Operation)
				sameClient := identityErr == nil && binding.UserID == group.userID && binding.ClientID == group.clientID && userID == group.userID && clientID == group.clientID
				adjacent := len(group.stepIndexes) == 1 && index == group.stepIndexes[0]+1
				if binding.Kind == "process" && OperationKey(step.Operation) == "process/restart-client" && group.checkpoint != "" && sameClient && adjacent {
					group.interruptedBy = step.ID
					calls[activeStagedCall] = group
					closedCalls[activeStagedCall] = struct{}{}
					activeStagedCall = ""
				} else if !(binding.Kind == "local-write" && sameClient && adjacent && group.checkpoint == "migration_committed" && activeStagedCall == "schema_proof_committed_recover") {
					v.add("%s native call %q is interrupted by binding %q on step %s", v.scenario.ID, activeStagedCall, binding.Kind, step.ID)
				}
			}
			if currentSynchronousCall != "" {
				group := calls[currentSynchronousCall]
				last := v.scenario.Steps[group.stepIndexes[len(group.stepIndexes)-1]]
				var delivery struct {
					Delivery string `json:"delivery"`
				}
				if binding.Kind == "process" && OperationKey(step.Operation) == "process/restart-client" && binding.UserID == group.userID && binding.ClientID == group.clientID && index == group.stepIndexes[len(group.stepIndexes)-1]+1 && OperationKey(last.Operation) == "push/submit" && json.Unmarshal(last.Operation.Payload, &delivery) == nil && delivery.Delivery == "drop_after_server" {
					group.interruptedBy = step.ID
					calls[currentSynchronousCall] = group
				}
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
		if binding.Checkpoint != "" {
			group.checkpoint = binding.Checkpoint
		}
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
		case "begin", "observe":
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
			if binding.Stage == "observe" && binding.Method != "" {
				v.add("%s native call %q is client-initiated and must declare no method", v.scenario.ID, callID)
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
		if group.interruptedBy != "" {
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
	// A staged call opens with begin when an application drives it, and with
	// observe when the client starts the work on its own.
	first := group.stages[0]
	if first != "begin" && first != "observe" {
		v.add("%s native call %q must begin with stage %q", v.scenario.ID, callID, "begin")
		first = "begin"
	}
	if group.stages[len(group.stages)-1] != "await-call" {
		v.add("%s native call %q must end with one terminal await-call stage", v.scenario.ID, callID)
	}
	for index, stage := range group.stages {
		want := "await-step"
		if index == 0 {
			want = first
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
	case "workload":
		return key == "workload/prepare"
	default:
		return false
	}
}

func (v *scenarioValidator) validateNativeBindingShape(step Step, binding NativeStepBinding) {
	if binding.Checkpoint != "" && (binding.Kind != "public-call" || binding.Stage != "begin" || binding.Checkpoint != "migration_prepared" && binding.Checkpoint != "migration_committed") {
		v.add("%s step %s checkpoint requires a bounded begin binding", v.scenario.ID, step.ID)
	}
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
	if binding.Kind != "workload" && binding.Workload != nil {
		v.add("%s step %s native binding %q must not contain workload parameters", v.scenario.ID, step.ID, binding.Kind)
	}

	switch binding.Kind {
	case "artifact", "local-write", "process", "public-call":
		userID, clientID, hasIdentity, err := nativeOperationIdentity(step.Operation)
		if err != nil {
			v.add("%s step %s cannot resolve native client identity: %v", v.scenario.ID, step.ID, err)
		} else if !hasIdentity || userID != binding.UserID || clientID != binding.ClientID {
			v.add("%s step %s native binding client identity does not match the authored operation", v.scenario.ID, step.ID)
		}
	case "workload":
		if OperationKey(step.Operation) != "workload/prepare" {
			v.add("%s step %s workload native binding requires workload/prepare", v.scenario.ID, step.ID)
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
	if wire.Action == "unsupported" {
		return "error"
	}
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
