package scenarios

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/trainstar/synchro/conformance/vectors"
)

// RebuildWorkloadInputs contains authored inputs, not predicted client or server results.
type RebuildWorkloadInputs struct {
	StepID     StepID
	Operations []Operation
}

// BuildRebuildWorkloadInputs supplies the Swift, Kotlin, and React Native
// rebuild-apply and rebuild-cardinality drivers without executing a sync model.
func BuildRebuildWorkloadInputs(scenario Scenario) ([]RebuildWorkloadInputs, error) {
	if len(scenario.Model.Setup) != 1 || OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return nil, errors.New("rebuild workload requires one contract installation")
	}
	if err := ValidateOperation(scenario.Model.Setup[0]); err != nil {
		return nil, fmt.Errorf("validate rebuild workload setup: %w", err)
	}
	var setup struct {
		InitialSchema struct {
			Schema SchemaFact `json:"schema"`
			Body   string     `json:"body"`
		} `json:"initial_schema"`
		InitialRegistry struct {
			Relations []struct {
				Relation               string   `json:"relation"`
				TableID                string   `json:"table_id"`
				RegistrationKind       string   `json:"registration_kind"`
				PrimaryKeyFieldID      string   `json:"primary_key_field_id"`
				PrimaryKeyPortableType string   `json:"primary_key_portable_type"`
				CapturedFieldIDs       []string `json:"captured_field_ids"`
			} `json:"relations"`
		} `json:"initial_registry"`
		Stream struct {
			Generation string `json:"stream_generation"`
		} `json:"stream"`
		Clients []struct {
			UserID           string   `json:"user_id"`
			ClientID         string   `json:"client_id"`
			Generation       uint64   `json:"client_generation"`
			AssignedScopeIDs []string `json:"assigned_scope_ids"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(scenario.Model.Setup[0].Payload, &setup); err != nil {
		return nil, fmt.Errorf("decode rebuild workload setup: %w", err)
	}
	manifest, err := vectors.ParseManifest(json.RawMessage(setup.InitialSchema.Body))
	if err != nil {
		return nil, fmt.Errorf("decode rebuild workload manifest: %w", err)
	}
	hash := manifest.Hash()
	if setup.InitialSchema.Schema.Version == 0 || hex.EncodeToString(hash[:]) != setup.InitialSchema.Schema.Hash ||
		setup.Stream.Generation == "" || len(scenario.Steps) == 0 {
		return nil, errors.New("rebuild workload setup binding is invalid")
	}

	inputs := make([]RebuildWorkloadInputs, 0, len(scenario.Steps))
	var priorCount uint64
	var priorValue, priorVersion string
	for index, step := range scenario.Steps {
		var workload struct {
			Profile     string `json:"profile"`
			ScopeID     string `json:"scope_id"`
			RecordCount uint64 `json:"record_count"`
			PageSize    uint64 `json:"page_size"`
		}
		if step.ExpectedOutcome.Disposition != "success" || step.ExpectedOutcome.ErrorCode != nil ||
			OperationKey(step.Operation) != "workload/prepare" || ValidateOperation(step.Operation) != nil ||
			json.Unmarshal(step.Operation.Payload, &workload) != nil || workload.Profile != "scope_cardinality" ||
			workload.ScopeID != "scope-a" || workload.PageSize != 100 ||
			!slices.Contains([]uint64{1, 101, 1000}, workload.RecordCount) || workload.RecordCount < priorCount {
			return nil, fmt.Errorf("rebuild workload step %s is invalid", step.ID)
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != "workload" || binding.Workload == nil {
			return nil, fmt.Errorf("rebuild workload step %s binding is absent", step.ID)
		}
		parameters := binding.Workload
		validator := scenarioValidator{}
		validator.validateNativeWorkload(step, *binding)
		if err := joinScenarioErrors(validator.errors); err != nil {
			return nil, fmt.Errorf("validate rebuild workload step %s: %w", step.ID, err)
		}
		if parameters.RecordCount != workload.RecordCount || len(parameters.Targets) != 1 ||
			parameters.AuthoredSchema != setup.InitialSchema.Schema {
			return nil, fmt.Errorf("rebuild workload step %s parameters differ from its inputs", step.ID)
		}
		target := parameters.Targets[0]
		if target.ScopeID != workload.ScopeID || target.TableID != "items" || target.PrimaryKeyFieldID != "id" {
			return nil, fmt.Errorf("rebuild workload step %s target is invalid", step.ID)
		}
		var relation string
		var capturedFields []string
		for _, candidate := range setup.InitialRegistry.Relations {
			if candidate.TableID != target.TableID {
				continue
			}
			if relation != "" || candidate.RegistrationKind != "synced" ||
				candidate.PrimaryKeyFieldID != target.PrimaryKeyFieldID || candidate.PrimaryKeyPortableType != "string" ||
				len(candidate.CapturedFieldIDs) != 2 || !slices.Contains(candidate.CapturedFieldIDs, "id") ||
				!slices.Contains(candidate.CapturedFieldIDs, "value") {
				return nil, fmt.Errorf("rebuild workload step %s relation is invalid", step.ID)
			}
			relation, capturedFields = candidate.Relation, candidate.CapturedFieldIDs
		}
		if relation == "" {
			return nil, fmt.Errorf("rebuild workload step %s relation is absent", step.ID)
		}
		var generation uint64
		for _, client := range setup.Clients {
			if client.UserID != binding.UserID || client.ClientID != binding.ClientID {
				continue
			}
			if generation != 0 || client.Generation == 0 || client.Generation > uint64(maxNativeIdentityInteger) ||
				!slices.Contains(client.AssignedScopeIDs, workload.ScopeID) {
				return nil, fmt.Errorf("rebuild workload step %s client binding is invalid", step.ID)
			}
			generation = client.Generation
		}
		if generation == 0 {
			return nil, fmt.Errorf("rebuild workload step %s client is absent", step.ID)
		}

		commit := uint64(index+1) * 10
		commitText := strconv.FormatUint(commit, 10)
		var events []any
		if workload.RecordCount > priorCount {
			for ordinal := priorCount + 1; ordinal <= workload.RecordCount; ordinal++ {
				value := fmt.Sprintf("cardinality-value-%06d-%010d", ordinal, commit)
				version := fmt.Sprintf("scope-cardinality-%010d-%06d", commit, ordinal)
				image, err := rebuildWorkloadImage(manifest, target, capturedFields, ordinal, value, version)
				if err != nil {
					return nil, fmt.Errorf("construct rebuild workload step %s row: %w", step.ID, err)
				}
				events = append(events, map[string]any{
					"event_ordinal": ordinal - priorCount, "relation": relation,
					"operation": "insert", "before": nil, "after": image,
				})
				priorValue, priorVersion = value, version
			}
		} else {
			before, err := rebuildWorkloadImage(manifest, target, capturedFields, priorCount, priorValue, priorVersion)
			if err != nil {
				return nil, fmt.Errorf("construct rebuild workload step %s previous row: %w", step.ID, err)
			}
			priorValue = fmt.Sprintf("cardinality-update-%010d", commit)
			priorVersion = fmt.Sprintf("scope-cardinality-%010d-update", commit)
			after, err := rebuildWorkloadImage(manifest, target, capturedFields, priorCount, priorValue, priorVersion)
			if err != nil {
				return nil, fmt.Errorf("construct rebuild workload step %s updated row: %w", step.ID, err)
			}
			events = []any{map[string]any{
				"event_ordinal": 1, "relation": relation, "operation": "update", "before": before, "after": after,
			}}
		}
		input := RebuildWorkloadInputs{StepID: step.ID}
		add := func(contract, name string, payload any) error {
			raw, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("encode rebuild workload %s/%s: %w", contract, name, err)
			}
			operation := Operation{ContractOperation: contract, Name: name, Payload: raw}
			if err := ValidateOperation(operation); err != nil {
				return fmt.Errorf("validate rebuild workload %s/%s: %w", contract, name, err)
			}
			input.Operations = append(input.Operations, operation)
			return nil
		}
		if err := add("model", "commit-source-transaction", map[string]any{
			"stream_generation": setup.Stream.Generation, "commit_lsn": commitText,
			"end_lsn": strconv.FormatUint(commit+1, 10), "events": events,
		}); err != nil {
			return nil, err
		}
		if err := add("process", "materialize-source-transaction", map[string]any{
			"stream_generation": setup.Stream.Generation, "commit_lsn": commitText,
		}); err != nil {
			return nil, err
		}
		rebuildID := fmt.Sprintf("00000000-0000-4000-8000-%012d", commit)
		if err := add("local", "begin-rebuild", map[string]any{
			"user_id": binding.UserID, "client_id": binding.ClientID, "client_generation": generation,
			"schema": parameters.AuthoredSchema, "scope_id": workload.ScopeID,
			"rebuild_id": rebuildID, "limit": workload.PageSize,
		}); err != nil {
			return nil, err
		}
		for ordinal := uint64(0); ordinal < workload.RecordCount; ordinal += workload.PageSize {
			cursor := "none"
			if ordinal != 0 {
				cursor = "local_rebuild_continuation"
			}
			if err := add("rebuild", "request-page", map[string]any{
				"user_id": binding.UserID, "client_id": binding.ClientID, "client_generation": generation,
				"schema": parameters.AuthoredSchema, "scope_id": workload.ScopeID,
				"rebuild_id": rebuildID, "cursor_source": cursor, "limit": workload.PageSize,
			}); err != nil {
				return nil, err
			}
			if err := add("local", "apply-rebuild-page", map[string]any{
				"user_id": binding.UserID, "client_id": binding.ClientID, "scope_id": workload.ScopeID,
				"rebuild_id": rebuildID, "page_ordinal": ordinal + 1, "request_token_source": cursor,
			}); err != nil {
				return nil, err
			}
		}
		if err := add("local", "finalize-rebuild", map[string]any{
			"user_id": binding.UserID, "client_id": binding.ClientID, "scope_id": workload.ScopeID, "rebuild_id": rebuildID,
		}); err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
		priorCount = workload.RecordCount
	}
	return inputs, nil
}

func rebuildWorkloadImage(manifest vectors.Manifest, target NativeWorkloadTarget, capturedFields []string, ordinal uint64, value, version string) (map[string]any, error) {
	pk, err := json.Marshal(fmt.Sprintf("cardinality-%06d", ordinal))
	if err != nil {
		return nil, err
	}
	identity, err := vectors.RowIdentity(manifest, target.TableID, pk)
	if err != nil {
		return nil, err
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	fields := make([]any, 0, len(capturedFields))
	row := vectors.Row{PK: pk}
	for _, fieldID := range capturedFields {
		wire := valueJSON
		if fieldID == target.PrimaryKeyFieldID {
			wire = pk
		}
		fields = append(fields, map[string]any{"field": fieldID, "type": "string", "wire_json": string(wire)})
		row.Fields = append(row.Fields, vectors.RowField{FieldID: fieldID, Value: wire})
	}
	digest, err := vectors.RowDigest(manifest, target.TableID, row, version)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"identity": map[string]any{
			"kind": "synced",
			"synced_row": map[string]any{
				"canonical_identity_bytes": string(identity), "table_id": target.TableID,
				"primary_key_field_id": target.PrimaryKeyFieldID, "portable_type": "string",
				"canonical_wire_json": string(pk),
			},
		},
		"fields": fields, "version": version, "checksum": hex.EncodeToString(digest[:]), "deleted": false,
	}, nil
}
