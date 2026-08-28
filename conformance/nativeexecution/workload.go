package nativeexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/scenarios"
)

// deriveNativeWorkload expands one bounded workload binding for BuildManifest.
// The native executor receives the result through ExecutionStep.ExpandedOperations.
func deriveNativeWorkload(step scenarios.Step) ([]scenarios.Operation, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Kind != "workload" || binding.Workload == nil {
		return nil, fmt.Errorf("step %s has no native workload binding", step.ID)
	}
	parameters := binding.Workload
	kinds := make([]scenarios.NativeWorkloadMutationKind, 0, parameters.RecordCount)
	for _, kind := range parameters.MutationKinds {
		for count := uint64(0); count < kind.Count; count++ {
			kinds = append(kinds, kind)
		}
	}
	operations := make([]scenarios.Operation, 0, parameters.RecordCount)
	for ordinal := uint64(0); ordinal < parameters.RecordCount; ordinal++ {
		target := parameters.Targets[ordinal%uint64(len(parameters.Targets))]
		batchOrdinal := ordinal / parameters.BatchSize
		ordinalInBatch := ordinal % parameters.BatchSize
		kind := kinds[ordinal]
		columns := make([]map[string]string, 0, len(kind.FieldIDs))
		fieldIDs := append([]string(nil), kind.FieldIDs...)
		sort.Strings(fieldIDs)
		for _, fieldID := range fieldIDs {
			columns = append(columns, map[string]string{
				"field_id": fieldID,
				"value":    fmt.Sprintf("workload-%d-%06d", parameters.Seed, ordinal+1),
			})
		}
		payload, err := json.Marshal(map[string]any{
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
		})
		if err != nil {
			return nil, fmt.Errorf("step %s generated local/write %d payload: %w", step.ID, ordinal+1, err)
		}
		operation := scenarios.Operation{
			ContractOperation: "local",
			Name:              "write",
			Payload:           payload,
		}
		if err := scenarios.ValidateOperation(operation); err != nil {
			return nil, fmt.Errorf("step %s generated local/write %d: %w", step.ID, ordinal+1, err)
		}
		operations = append(operations, operation)
	}
	encoded, err := json.Marshal(operations)
	if err != nil {
		return nil, fmt.Errorf("step %s encode generated operations: %w", step.ID, err)
	}
	digest := sha256.Sum256(encoded)
	actualDigest := hex.EncodeToString(digest[:])
	if parameters.Expectation.OperationDigest != actualDigest {
		return nil, fmt.Errorf("step %s generated operation digest %s does not match expectation", step.ID, actualDigest)
	}
	return operations, nil
}

func nativeWorkloadUUID(seed uint64, target scenarios.NativeWorkloadTarget, batchOrdinal, ordinalInBatch uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:native-workload:v1:%d:%s:%s:%d:%d", seed, target.ScopeID, target.TableID, batchOrdinal, ordinalInBatch)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
