package modelrunner

import (
	"encoding/json"
	"fmt"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func deriveSchemaDispatchMeasurementSample(binding scenarios.MeasurementSample, execution OperationExecution) (SchemaDispatchMeasurementSampleExecution, error) {
	if binding.MeasurementID == "" || binding.StratumID == "" || binding.SampleID == "" {
		return SchemaDispatchMeasurementSampleExecution{}, fmt.Errorf("schema-dispatch measurement binding is incomplete")
	}
	if execution.OperationKey != "connect/send" {
		return SchemaDispatchMeasurementSampleExecution{}, fmt.Errorf("schema-dispatch measurement sample must execute connect/send")
	}
	if execution.Err != nil || execution.Result.Kind != reference.StepResultKindConnect || execution.Result.HTTP == nil || execution.Result.Connect == nil {
		return SchemaDispatchMeasurementSampleExecution{}, fmt.Errorf("schema-dispatch measurement sample has no executed connect observation")
	}
	return SchemaDispatchMeasurementSampleExecution{
		Binding: scenarios.MeasurementSample{
			MeasurementID: binding.MeasurementID,
			StratumID:     binding.StratumID,
			SampleID:      binding.SampleID,
			Parameters:    append(json.RawMessage(nil), binding.Parameters...),
		},
		RequestCount:   1,
		HTTPStatus:     execution.Result.HTTP.Status,
		Source:         execution.Result.Connect.Schema.Source,
		Target:         execution.Result.Connect.Schema.Target,
		Action:         execution.Result.Connect.Schema.Action,
		Reason:         execution.Result.Connect.Schema.Reason,
		AffectedScopes: append([]reference.ScopeID(nil), execution.Result.Connect.Schema.AffectedScopes...),
	}, nil
}

func schemaDispatchObservationsSatisfied(result Result, plan scenarios.SchemaDispatchMeasurementPlan) (bool, string) {
	observations, reason := collectSchemaDispatchObservations(result, plan)
	if reason != "" {
		return false, reason
	}
	if reason := schemaDispatchCoverageFailure(observations, plan); reason != "" {
		return false, reason
	}
	return true, "each bound schema-dispatch sample matched its executed semantic observation"
}

func schemaDispatchMeasurementSatisfied(result Result, plan scenarios.SchemaDispatchMeasurementPlan) (bool, string) {
	observations, reason := collectSchemaDispatchObservations(result, plan)
	if reason != "" {
		return false, reason
	}
	if reason := schemaDispatchCoverageFailure(observations, plan); reason != "" {
		return false, reason
	}
	for _, observation := range observations {
		if observation.execution.SchemaDispatchMeasurement.RequestCount != 1 {
			return false, "a schema-dispatch measurement did not derive one request from its executed connect operation"
		}
	}
	return true, "each bound schema-dispatch sample derived its request measurement from one executed connect operation"
}

type schemaDispatchObservation struct {
	execution    OperationExecution
	expectedCase string
	actualCase   string
}

func collectSchemaDispatchObservations(result Result, plan scenarios.SchemaDispatchMeasurementPlan) ([]schemaDispatchObservation, string) {
	if plan.MeasurementID == "" || plan.MinimumSampleCountPerStratum == 0 || len(plan.Strata) == 0 {
		return nil, "schema-dispatch measurement plan is incomplete"
	}
	expectedStrata := make(map[string]string, len(plan.Strata))
	for _, stratum := range plan.Strata {
		if stratum.StratumID == "" || stratum.SchemaCase == "" {
			return nil, "schema-dispatch measurement plan has an incomplete stratum"
		}
		key := string(stratum.StratumID)
		if _, duplicate := expectedStrata[key]; duplicate {
			return nil, "schema-dispatch measurement plan has a duplicate stratum"
		}
		expectedStrata[key] = stratum.SchemaCase
	}

	observations := make([]schemaDispatchObservation, 0)
	seenSamples := make(map[string]struct{})
	seenClients := make(map[reference.ClientKey]struct{})
	for _, execution := range result.Steps {
		measurement := execution.SchemaDispatchMeasurement
		if measurement == nil {
			continue
		}
		if measurement.Binding.MeasurementID != plan.MeasurementID {
			return nil, fmt.Sprintf("step %s binds another measurement", execution.StepID)
		}
		sampleKey := string(measurement.Binding.StratumID) + "|" + measurement.Binding.SampleID
		if _, duplicate := seenSamples[sampleKey]; duplicate {
			return nil, fmt.Sprintf("step %s duplicates a schema-dispatch measurement sample", execution.StepID)
		}
		seenSamples[sampleKey] = struct{}{}
		expectedCase, err := schemaCaseFromParameters(measurement.Binding.Parameters)
		if err != nil {
			return nil, fmt.Sprintf("step %s measurement parameters: %v", execution.StepID, err)
		}
		plannedCase, found := expectedStrata[string(measurement.Binding.StratumID)]
		if !found || plannedCase != expectedCase {
			return nil, fmt.Sprintf("step %s binds an unavailable schema stratum", execution.StepID)
		}
		actualCase, err := executedSchemaDispatchCase(execution)
		if err != nil {
			return nil, fmt.Sprintf("step %s schema-dispatch observation: %v", execution.StepID, err)
		}
		if actualCase != expectedCase {
			return nil, fmt.Sprintf("step %s executed schema case %q, want bound case %q", execution.StepID, actualCase, expectedCase)
		}
		client := execution.Result.Connect.Client
		if _, duplicate := seenClients[client]; duplicate {
			return nil, fmt.Sprintf("step %s reuses client %s/%s for a schema-dispatch sample", execution.StepID, client.UserID, client.ClientID)
		}
		seenClients[client] = struct{}{}
		observations = append(observations, schemaDispatchObservation{execution: execution, expectedCase: expectedCase, actualCase: actualCase})
	}
	if len(observations) == 0 {
		return nil, "schema-dispatch measurement plan has no executed samples"
	}
	return observations, ""
}

func schemaDispatchCoverageFailure(observations []schemaDispatchObservation, plan scenarios.SchemaDispatchMeasurementPlan) string {
	counts := make(map[string]uint64, len(plan.Strata))
	for _, observation := range observations {
		counts[string(observation.execution.SchemaDispatchMeasurement.Binding.StratumID)]++
	}
	for _, stratum := range plan.Strata {
		count := counts[string(stratum.StratumID)]
		if count < plan.MinimumSampleCountPerStratum {
			return fmt.Sprintf("schema stratum %q has %d executed observations, want at least %d", stratum.StratumID, count, plan.MinimumSampleCountPerStratum)
		}
	}
	return ""
}

func schemaCaseFromParameters(raw json.RawMessage) (string, error) {
	var parameters map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &parameters); err != nil {
		return "", fmt.Errorf("decode parameters: %w", err)
	}
	if len(parameters) != 1 {
		return "", fmt.Errorf("parameters do not contain exactly one member")
	}
	encoded, found := parameters["schema_case"]
	if !found {
		return "", fmt.Errorf("parameters have no schema_case")
	}
	var schemaCase string
	if err := json.Unmarshal(encoded, &schemaCase); err != nil || schemaCase == "" {
		return "", fmt.Errorf("schema_case is invalid")
	}
	return schemaCase, nil
}

func executedSchemaDispatchCase(execution OperationExecution) (string, error) {
	measurement := execution.SchemaDispatchMeasurement
	if measurement == nil || execution.Result.Connect == nil || execution.Result.HTTP == nil {
		return "", fmt.Errorf("executed connect observation is absent")
	}
	connect := execution.Result.Connect
	if execution.OperationKey != "connect/send" || execution.Err != nil || execution.Result.Kind != reference.StepResultKindConnect || execution.Result.HTTP.Status != 200 || execution.Result.HTTP.HasCode {
		return "", fmt.Errorf("executed operation is not a successful Protocol 3 connect")
	}
	if measurement.HTTPStatus != execution.Result.HTTP.Status || measurement.Source != connect.Schema.Source || measurement.Target != connect.Schema.Target || measurement.Action != connect.Schema.Action || measurement.Reason != connect.Schema.Reason || !sameScopeSet(measurement.AffectedScopes, connect.Schema.AffectedScopes) {
		return "", fmt.Errorf("derived measurement observation does not match the executed connect result")
	}
	if schemaResetRequested(execution.Operation.Payload) {
		return "", fmt.Errorf("explicit schema reset does not exercise ordinary schema dispatch")
	}
	if connect.Schema.Target != execution.Before.CurrentSchema {
		return "", fmt.Errorf("connect target is not the authoritative current schema")
	}
	if connect.Schema.Source == connect.Schema.Target {
		if connect.Schema.Action != reference.SchemaActionNone || connect.Schema.Reason != "" || len(connect.Schema.AffectedScopes) != 0 {
			return "", fmt.Errorf("exact-current schema dispatch is invalid")
		}
		divergent := clientMembershipDivergence(execution.Before, connect.Client)
		if len(divergent) == 0 {
			return "current", nil
		}
		if !hasRebuildCursorForAny(connect.ScopeCursors, divergent) {
			return "", fmt.Errorf("Class 1 membership dispatch did not invalidate its changed scope cursor")
		}
		return "class_1", nil
	}

	lineage, err := schemaDispatchLineage(execution.Before, connect.Schema.Source, connect.Schema.Target)
	if err != nil {
		return "", err
	}
	if lineage.hasClass4 {
		if connect.Schema.Action != reference.SchemaActionUnsupported || connect.Schema.Reason != reference.ReasonCode("incompatible_schema_transition") || len(connect.Schema.AffectedScopes) != 0 {
			return "", fmt.Errorf("Class 4 dispatch is invalid")
		}
		return "class_4", nil
	}
	if lineage.hasClass3 {
		affected := assignedLineageScopes(execution.Before, connect.Client, lineage.affectedScopes)
		if len(affected) == 0 {
			if connect.Schema.Action != reference.SchemaActionReplace || connect.Schema.Reason != "" || len(connect.Schema.AffectedScopes) != 0 {
				return "", fmt.Errorf("unaffected Class 3 dispatch is invalid")
			}
			return "class_3_unaffected", nil
		}
		if connect.Schema.Action != reference.SchemaActionRebuildLocal || connect.Schema.Reason != "" || !sameScopeSet(connect.Schema.AffectedScopes, affected) {
			return "", fmt.Errorf("affected Class 3 dispatch is invalid")
		}
		return "class_3_affected", nil
	}
	if lineage.hasClass2 {
		if connect.Schema.Action != reference.SchemaActionReplace || connect.Schema.Reason != "" || len(connect.Schema.AffectedScopes) != 0 {
			return "", fmt.Errorf("Class 2 dispatch is invalid")
		}
		return "class_2", nil
	}
	return "", fmt.Errorf("schema lineage does not contain a dispatch class")
}

func schemaResetRequested(payload json.RawMessage) bool {
	var request map[string]json.RawMessage
	if jsonstrict.Decode(payload, &request) != nil {
		return true
	}
	raw, found := request["schema_reset"]
	if !found {
		return true
	}
	var requested bool
	return json.Unmarshal(raw, &requested) != nil || requested
}

type schemaLineage struct {
	hasClass2      bool
	hasClass3      bool
	hasClass4      bool
	affectedScopes map[reference.ScopeID]struct{}
}

func schemaDispatchLineage(snapshot reference.StateSnapshot, source, target reference.SchemaRef) (schemaLineage, error) {
	if source == target || target != snapshot.CurrentSchema {
		return schemaLineage{}, fmt.Errorf("schema lineage endpoints are invalid")
	}
	manifests := make(map[reference.SchemaRef]reference.SchemaManifest, len(snapshot.Schemas))
	for _, entry := range snapshot.Schemas {
		manifests[entry.Key] = entry.Value
	}
	current := target
	result := schemaLineage{affectedScopes: make(map[reference.ScopeID]struct{})}
	for steps := 0; steps <= len(manifests); steps++ {
		if current == source {
			return result, nil
		}
		manifest, found := manifests[current]
		if !found || manifest.Parent == nil {
			return schemaLineage{}, fmt.Errorf("schema lineage is incomplete")
		}
		switch manifest.Class {
		case reference.SchemaClass2:
			result.hasClass2 = true
		case reference.SchemaClass3:
			result.hasClass3 = true
			for _, scope := range manifest.AffectedScopes {
				result.affectedScopes[scope] = struct{}{}
			}
		case reference.SchemaClass4:
			result.hasClass4 = true
		default:
			return schemaLineage{}, fmt.Errorf("schema lineage has an unsupported transition class")
		}
		current = *manifest.Parent
	}
	return schemaLineage{}, fmt.Errorf("schema lineage does not reach the source")
}

func clientMembershipDivergence(snapshot reference.StateSnapshot, clientKey reference.ClientKey) []reference.ScopeID {
	client, clientFound := snapshotClient(snapshot.Clients, clientKey)
	local, localFound := snapshotLocalClient(snapshot.ClientLocal, clientKey)
	if !clientFound || !localFound {
		return []reference.ScopeID{"missing-client-state"}
	}
	divergent := make([]reference.ScopeID, 0)
	for _, serverAssignment := range client.ScopeAssignments {
		if !serverAssignment.Assigned {
			continue
		}
		localAssignment, found := localAssignment(local, serverAssignment.Scope)
		if !found || !localAssignment.Assigned || serverAssignment.RebuildRequired || localAssignment.MembershipGeneration != serverAssignment.MembershipGeneration || localAssignment.RetentionGeneration != serverAssignment.RetentionGeneration {
			divergent = append(divergent, serverAssignment.Scope)
		}
	}
	return divergent
}

func hasRebuildCursorForAny(cursors []reference.ScopeCursorObservation, scopes []reference.ScopeID) bool {
	for _, cursor := range cursors {
		if cursor.Disposition != reference.CursorDispositionRebuildRequired {
			continue
		}
		for _, scope := range scopes {
			if cursor.Scope == scope {
				return true
			}
		}
	}
	return false
}

func assignedLineageScopes(snapshot reference.StateSnapshot, clientKey reference.ClientKey, lineageScopes map[reference.ScopeID]struct{}) []reference.ScopeID {
	client, found := snapshotClient(snapshot.Clients, clientKey)
	if !found {
		return nil
	}
	result := make([]reference.ScopeID, 0)
	for _, assignment := range client.ScopeAssignments {
		if assignment.Assigned {
			if _, affected := lineageScopes[assignment.Scope]; affected {
				result = append(result, assignment.Scope)
			}
		}
	}
	return result
}

func sameScopeSet(left, right []reference.ScopeID) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[reference.ScopeID]struct{}, len(left))
	for _, scope := range left {
		seen[scope] = struct{}{}
	}
	if len(seen) != len(left) {
		return false
	}
	seenRight := make(map[reference.ScopeID]struct{}, len(right))
	for _, scope := range right {
		if _, found := seen[scope]; !found {
			return false
		}
		if _, duplicate := seenRight[scope]; duplicate {
			return false
		}
		seenRight[scope] = struct{}{}
	}
	return true
}
