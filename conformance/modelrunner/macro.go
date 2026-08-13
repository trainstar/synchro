package modelrunner

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

type workloadExpansionPlan struct {
	Operations []scenarios.Operation
	Samples    []workloadSamplePlan
}

type workloadSamplePlan struct {
	Family                 WorkloadSampleFamily
	Boundary               WorkloadSampleBoundary
	Value                  uint64
	ExpandedOperationIndex int
	Expected               workloadSampleExpectation
}

type workloadSampleExpectation struct {
	ResultKind      reference.StepResultKind
	HTTPStatus      int
	HTTPCode        reference.HTTPCode
	ErrorCode       string
	PreserveState   bool
	BatchCount      uint64
	CheckBatchCount bool
}

// expandWorkload turns the only model-runner macro into a typed execution
// plan. The reference model remains the sole owner of state changes.
func expandWorkload(snapshot reference.StateSnapshot, operation scenarios.Operation) (workloadExpansionPlan, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return workloadExpansionPlan{}, fmt.Errorf("decode workload/prepare payload: %w", err)
	}
	var profile string
	if err := json.Unmarshal(payload["profile"], &profile); err != nil || profile == "" {
		return workloadExpansionPlan{}, errors.New("workload/prepare profile is required")
	}

	switch profile {
	case "scope_topology":
		operations, err := expandScopeTopologyWorkload(snapshot, payload)
		if err == nil && scopeTopologyProvenanceWorkload(snapshot, payload) {
			operations, err = appendScopeTopologyProvenanceOperations(snapshot, operations)
		}
		return workloadExpansionPlan{Operations: operations}, err
	case "scope_cardinality":
		operations, err := expandScopeCardinalityWorkload(snapshot, payload)
		return workloadExpansionPlan{Operations: operations}, err
	case "pending_mutations":
		operations, err := expandPendingMutationsWorkload(snapshot, payload)
		return workloadExpansionPlan{Operations: operations}, err
	case "configured_limits":
		return expandConfiguredLimitsWorkload(snapshot, payload)
	default:
		return workloadExpansionPlan{}, fmt.Errorf("workload/prepare profile %q is not closed", profile)
	}
}

func scopeTopologyProvenanceWorkload(snapshot reference.StateSnapshot, payload map[string]json.RawMessage) bool {
	var fanout uint64
	if err := json.Unmarshal(payload["scope_fanout"], &fanout); err != nil || fanout < 2 {
		return false
	}
	for _, entry := range snapshot.Clients {
		assigned := 0
		for _, assignment := range entry.Value.ScopeAssignments {
			if assignment.Assigned {
				assigned++
			}
		}
		if assigned >= 2 {
			return true
		}
	}
	return false
}

func appendScopeTopologyProvenanceOperations(snapshot reference.StateSnapshot, operations []scenarios.Operation) ([]scenarios.Operation, error) {
	client, state, err := cardinalityAssignedClient(snapshot)
	if err != nil {
		return nil, fmt.Errorf("scope topology provenance client: %w", err)
	}
	schema := cardinalitySchemaPayload(snapshot.CurrentSchema)
	for scopeIndex, scope := range []reference.ScopeID{"scope-a", "scope-b"} {
		assigned := false
		for _, assignment := range state.ScopeAssignments {
			if assignment.Scope == scope && assignment.Assigned {
				assigned = true
				break
			}
		}
		if !assigned {
			return nil, fmt.Errorf("scope topology provenance scope %q is not assigned", scope)
		}
		rebuildID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 9100+uint64(snapshot.Registry.CurrentGeneration)*2+uint64(scopeIndex))
		for _, item := range []struct {
			contract string
			name     string
			payload  map[string]any
		}{
			{"local", "begin-rebuild", map[string]any{"user_id": client.UserID, "client_id": client.ClientID, "client_generation": state.CurrentGeneration, "schema": schema, "scope_id": scope, "rebuild_id": rebuildID, "limit": 100}},
			{"rebuild", "request-page", map[string]any{"user_id": client.UserID, "client_id": client.ClientID, "client_generation": state.CurrentGeneration, "schema": schema, "scope_id": scope, "rebuild_id": rebuildID, "cursor_source": "none", "limit": 100}},
			{"local", "apply-rebuild-page", map[string]any{"user_id": client.UserID, "client_id": client.ClientID, "scope_id": scope, "rebuild_id": rebuildID, "page_ordinal": 1, "request_token_source": "none"}},
			{"local", "finalize-rebuild", map[string]any{"user_id": client.UserID, "client_id": client.ClientID, "scope_id": scope, "rebuild_id": rebuildID}},
		} {
			operation := scenarios.Operation{ContractOperation: item.contract, Name: item.name, Payload: mustJSON(item.payload)}
			if err := scenarios.ValidateOperation(operation); err != nil {
				return nil, fmt.Errorf("scope topology provenance operation %s/%s: %w", item.contract, item.name, err)
			}
			operations = append(operations, operation)
		}
	}
	return operations, nil
}

func requiredString(payload map[string]json.RawMessage, name string) (string, error) {
	value, ok := payload[name]
	if !ok {
		return "", fmt.Errorf("workload/prepare %s is required", name)
	}
	var decoded string
	if err := json.Unmarshal(value, &decoded); err != nil || decoded == "" {
		return "", fmt.Errorf("workload/prepare %s must be a nonempty string", name)
	}
	return decoded, nil
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func expectedOutcomeFailure(step scenarios.Step, execution OperationExecution, err error) *RunError {
	return &RunError{
		Kind:         RunErrorExpectedOutcome,
		StepID:       step.ID,
		OperationKey: execution.OperationKey,
		ExpectedCode: expectedCode(step.ExpectedOutcome),
		ActualCode:   ErrorCode(err),
		Err:          fmt.Errorf("expected outcome did not match the expanded operation result"),
	}
}
