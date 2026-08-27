package nativeexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/scenarios"
)

const maxNativeWorkloadSeed = uint64(1<<53 - 1)

// deriveNativeWorkload expands one bounded workload binding for BuildManifest.
// The native executor receives the result through ExecutionStep.ExpandedOperations.
func deriveNativeWorkload(step scenarios.Step) ([]scenarios.Operation, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Kind != "workload" || binding.Workload == nil {
		return nil, fmt.Errorf("step %s has no native workload binding", step.ID)
	}
	parameters := binding.Workload
	if parameters.RecordCount == 0 || parameters.RecordCount > 1000 {
		return nil, fmt.Errorf("step %s workload record_count is out of bounds", step.ID)
	}
	if parameters.BatchSize == 0 || parameters.BatchSize > parameters.RecordCount {
		return nil, fmt.Errorf("step %s workload batch_size is out of bounds", step.ID)
	}
	if parameters.Seed == 0 || parameters.Seed > maxNativeWorkloadSeed || parameters.AuthoredSchema.Version == 0 || !validSHA256(parameters.AuthoredSchema.Hash) || parameters.ClientVersion == "" {
		return nil, fmt.Errorf("step %s workload parameters are incomplete", step.ID)
	}
	if len(parameters.Targets) == 0 || len(parameters.Targets) > 8 || len(parameters.MutationKinds) == 0 || len(parameters.MutationKinds) > 8 {
		return nil, fmt.Errorf("step %s workload targets or mutation kinds are out of bounds", step.ID)
	}

	kindForOrdinal, err := nativeWorkloadKinds(parameters)
	if err != nil {
		return nil, fmt.Errorf("step %s workload mutation kinds: %w", step.ID, err)
	}
	operations := make([]scenarios.Operation, 0, parameters.RecordCount)
	scopeCardinalities := make(map[string]uint64, len(parameters.Targets))
	for ordinal := uint64(0); ordinal < parameters.RecordCount; ordinal++ {
		target := parameters.Targets[ordinal%uint64(len(parameters.Targets))]
		batchOrdinal := ordinal / parameters.BatchSize
		ordinalInBatch := ordinal % parameters.BatchSize
		kind := kindForOrdinal[ordinal]
		columns := make([]map[string]string, 0, len(kind.FieldIDs))
		fieldIDs := append([]string(nil), kind.FieldIDs...)
		sort.Strings(fieldIDs)
		for _, fieldID := range fieldIDs {
			columns = append(columns, map[string]string{
				"field_id": fieldID,
				"value":    fmt.Sprintf("workload-%d-%06d", parameters.Seed, ordinal+1),
			})
		}
		operation := scenarios.Operation{
			ContractOperation: "local",
			Name:              "write",
			Payload: mustMarshalNativeWorkload(map[string]any{
				"authenticated_user_id": binding.UserID,
				"client_id":             binding.ClientID,
				"mutation_id":           nativeWorkloadUUID(parameters.Seed, target, batchOrdinal, ordinalInBatch),
				"table_id":              target.TableID,
				"pk": map[string]string{
					target.PrimaryKeyFieldID: fmt.Sprintf("workload-%d-%s-%06d", parameters.Seed, target.ScopeID, ordinal+1),
				},
				"authored_schema": map[string]any{
					"version": parameters.AuthoredSchema.Version,
					"hash":    parameters.AuthoredSchema.Hash,
				},
				"operation":      kind.Operation,
				"client_version": parameters.ClientVersion,
				"columns":        columns,
			}),
		}
		if err := scenarios.ValidateOperation(operation); err != nil {
			return nil, fmt.Errorf("step %s generated local/write %d: %w", step.ID, ordinal+1, err)
		}
		operations = append(operations, operation)
		scopeCardinalities[target.ScopeID]++
	}
	if err := validateNativeWorkloadExpectation(step.ID, parameters, operations, scopeCardinalities); err != nil {
		return nil, err
	}
	return operations, nil
}

func nativeWorkloadKinds(parameters *scenarios.NativeWorkloadParameters) (map[uint64]scenarios.NativeWorkloadMutationKind, error) {
	result := make(map[uint64]scenarios.NativeWorkloadMutationKind, parameters.RecordCount)
	ordinal := uint64(0)
	for index, kind := range parameters.MutationKinds {
		if kind.Operation != "insert" || kind.Count == 0 || kind.Count > 1000 || len(kind.FieldIDs) == 0 || len(kind.FieldIDs) > 16 {
			return nil, fmt.Errorf("kind %d is invalid", index+1)
		}
		fields := make(map[string]struct{}, len(kind.FieldIDs))
		for _, fieldID := range kind.FieldIDs {
			if fieldID == "" {
				return nil, fmt.Errorf("kind %d has an empty field", index+1)
			}
			if _, duplicate := fields[fieldID]; duplicate {
				return nil, fmt.Errorf("kind %d repeats field %q", index+1, fieldID)
			}
			fields[fieldID] = struct{}{}
		}
		if kind.Count > parameters.RecordCount || ordinal > parameters.RecordCount-kind.Count {
			return nil, fmt.Errorf("kind counts exceed record_count")
		}
		for count := uint64(0); count < kind.Count; count++ {
			result[ordinal] = kind
			ordinal++
		}
	}
	if ordinal != parameters.RecordCount {
		return nil, fmt.Errorf("kind counts total %d, want %d", ordinal, parameters.RecordCount)
	}
	return result, nil
}

func validateNativeWorkloadExpectation(stepID scenarios.StepID, parameters *scenarios.NativeWorkloadParameters, operations []scenarios.Operation, scopeCardinalities map[string]uint64) error {
	if parameters.Expectation.OperationCount != uint64(len(operations)) {
		return fmt.Errorf("step %s generated operation count %d does not match expectation %d", stepID, len(operations), parameters.Expectation.OperationCount)
	}
	wantBatches := (parameters.RecordCount + parameters.BatchSize - 1) / parameters.BatchSize
	if parameters.Expectation.BatchCount != wantBatches {
		return fmt.Errorf("step %s generated batch count %d does not match expectation %d", stepID, wantBatches, parameters.Expectation.BatchCount)
	}
	encoded, err := json.Marshal(operations)
	if err != nil {
		return fmt.Errorf("step %s encode generated operations: %w", stepID, err)
	}
	digest := sha256.Sum256(encoded)
	actualDigest := hex.EncodeToString(digest[:])
	if parameters.Expectation.OperationDigest != actualDigest {
		return fmt.Errorf("step %s generated operation digest %s does not match expectation", stepID, actualDigest)
	}
	expectedScopes := make(map[string]uint64, len(parameters.Expectation.PerScopeCardinalities))
	for _, cardinality := range parameters.Expectation.PerScopeCardinalities {
		if _, duplicate := expectedScopes[cardinality.ScopeID]; duplicate {
			return fmt.Errorf("step %s expected scope %q is duplicated", stepID, cardinality.ScopeID)
		}
		expectedScopes[cardinality.ScopeID] = cardinality.Cardinality
	}
	if len(expectedScopes) != len(scopeCardinalities) {
		return fmt.Errorf("step %s generated scope cardinalities do not match expectation", stepID)
	}
	for scopeID, cardinality := range scopeCardinalities {
		if expectedScopes[scopeID] != cardinality {
			return fmt.Errorf("step %s generated scope %q cardinality does not match expectation", stepID, scopeID)
		}
	}
	return nil
}

func nativeWorkloadUUID(seed uint64, target scenarios.NativeWorkloadTarget, batchOrdinal, ordinalInBatch uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:native-workload:v1:%d:%s:%s:%d:%d", seed, target.ScopeID, target.TableID, batchOrdinal, ordinalInBatch)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func mustMarshalNativeWorkload(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
