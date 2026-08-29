package modelrunner

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	cardinalityScope       = reference.ScopeID("scope-a")
	cardinalityPageSize    = uint64(100)
	cardinalityRelation    = reference.RelationID("public.items")
	cardinalityTable       = reference.TableID("items")
	cardinalityPrimaryKey  = reference.FieldID("id")
	cardinalityValueField  = reference.FieldID("value")
	cardinalityRowPrefix   = "cardinality-"
	cardinalityRebuildBase = "00000000-0000-4000-8000-"
)

type cardinalityRelationContext struct {
	Generation reference.RegistryGenerationState
	Definition reference.RelationDefinition
	Fields     []cardinalityFieldInfo
	Manifest   vectors.Manifest
}

type cardinalityFieldInfo struct {
	ID       reference.FieldID
	Type     reference.PortableType
	Nullable bool
}

// expandScopeCardinalityWorkload expands one cardinality sample into source,
// WAL, and immutable rebuild operations. It only reads the supplied snapshot.
func expandScopeCardinalityWorkload(snapshot reference.StateSnapshot, payload map[string]json.RawMessage) ([]scenarios.Operation, error) {
	return expandScopeCardinalityWorkloadForClient(snapshot, payload, nil)
}

func expandScopeCardinalityWorkloadForClient(snapshot reference.StateSnapshot, payload map[string]json.RawMessage, selectedClient *reference.ClientKey) ([]scenarios.Operation, error) {
	if payload == nil {
		return nil, errors.New("scope_cardinality workload payload is required")
	}
	profile, err := cardinalityRequiredString(payload, "profile")
	if err != nil {
		return nil, err
	}
	if profile != "scope_cardinality" {
		return nil, fmt.Errorf("workload/prepare profile %q is not scope_cardinality", profile)
	}
	scopeID, err := cardinalityRequiredString(payload, "scope_id")
	if err != nil {
		return nil, err
	}
	if reference.ScopeID(scopeID) != cardinalityScope {
		return nil, fmt.Errorf("scope_cardinality scope_id must be %q", cardinalityScope)
	}
	recordCount, err := cardinalityRequiredUint(payload, "record_count")
	if err != nil {
		return nil, err
	}
	if recordCount != 1 && recordCount != 101 && recordCount != 1000 {
		return nil, fmt.Errorf("scope_cardinality record_count %d is not a closed sample", recordCount)
	}
	pageSize, err := cardinalityRequiredUint(payload, "page_size")
	if err != nil {
		return nil, err
	}
	if pageSize != cardinalityPageSize {
		return nil, fmt.Errorf("scope_cardinality page_size must be %d", cardinalityPageSize)
	}
	if snapshot.ProtocolVersion != 3 {
		return nil, errors.New("scope_cardinality requires protocol version 3")
	}

	relation, err := cardinalityRelationInfo(snapshot)
	if err != nil {
		return nil, err
	}
	client, clientState, err := cardinalityAssignedClientFor(snapshot, selectedClient)
	if err != nil {
		return nil, err
	}
	scopeState, found := cardinalityScopeState(snapshot, cardinalityScope)
	if !found {
		return nil, fmt.Errorf("scope_cardinality scope %q is absent", cardinalityScope)
	}
	currentRows, currentCount, err := cardinalityCurrentRows(snapshot, scopeState, relation.Definition)
	if err != nil {
		return nil, err
	}
	if currentCount > recordCount {
		return nil, fmt.Errorf("scope_cardinality cannot reduce scope cardinality from %d to %d", currentCount, recordCount)
	}

	desiredRows, err := cardinalityDesiredRows(recordCount, relation.Definition)
	if err != nil {
		return nil, err
	}
	for ordinal := uint64(1); ordinal <= currentCount; ordinal++ {
		if _, exists := currentRows[desiredRows[ordinal-1]]; !exists {
			return nil, fmt.Errorf("scope_cardinality current row %d does not use the deterministic identity", ordinal)
		}
	}

	operations := make([]scenarios.Operation, 0, 8+int(recordCount)/100)
	if !cardinalityMembershipSupports(relation.Generation, relation.Definition.Relation, desiredRows, cardinalityScope) {
		if snapshot.Stream.Authority.GlobalMaterializationBoundary.StreamGeneration != snapshot.Stream.Authority.ActiveGeneration || snapshot.Stream.Authority.GlobalMaterializationBoundary.Kind != reference.PositionKindTransactionEnd {
			return nil, errors.New("scope_cardinality membership staging requires a completed transaction boundary")
		}
		stagePayload, err := cardinalityStagePayload(snapshot, relation, desiredRows, scopeState)
		if err != nil {
			return nil, err
		}
		operations, err = cardinalityAppendOperation(operations, "model", "stage-registry-membership-generation", stagePayload)
		if err != nil {
			return nil, err
		}
		operations, err = cardinalityAppendOperation(operations, "model", "activate-registry-membership-generation", map[string]any{
			"registry_generation": relation.Generation.Generation + 1,
		})
		if err != nil {
			return nil, err
		}
	}

	commitLSN, err := cardinalityNextCommitLSN(snapshot)
	if err != nil {
		return nil, err
	}
	events, err := cardinalitySourceEvents(snapshot, relation, currentRows, currentCount, recordCount, commitLSN)
	if err != nil {
		return nil, err
	}
	commitPayload := map[string]any{
		"stream_generation": string(snapshot.Stream.Authority.ActiveGeneration),
		"commit_lsn":        strconv.FormatUint(commitLSN, 10),
		"end_lsn":           strconv.FormatUint(commitLSN+1, 10),
		"events":            events,
	}
	operations, err = cardinalityAppendOperation(operations, "model", "commit-source-transaction", commitPayload)
	if err != nil {
		return nil, err
	}
	operations, err = cardinalityAppendOperation(operations, "process", "materialize-source-transaction", map[string]any{
		"stream_generation": string(snapshot.Stream.Authority.ActiveGeneration),
		"commit_lsn":        strconv.FormatUint(commitLSN, 10),
	})
	if err != nil {
		return nil, err
	}

	rebuildID := cardinalityRebuildID(commitLSN)
	schemaPayload := cardinalitySchemaPayload(snapshot.CurrentSchema)
	operations, err = cardinalityAppendOperation(operations, "local", "begin-rebuild", map[string]any{
		"user_id":           string(client.UserID),
		"client_id":         string(client.ClientID),
		"client_generation": clientState.CurrentGeneration,
		"schema":            schemaPayload,
		"scope_id":          string(cardinalityScope),
		"rebuild_id":        rebuildID,
		"limit":             cardinalityPageSize,
	})
	if err != nil {
		return nil, err
	}

	pageCount := (recordCount + cardinalityPageSize - 1) / cardinalityPageSize
	for pageIndex := uint64(0); pageIndex < pageCount; pageIndex++ {
		requestOrdinal := pageIndex*cardinalityPageSize + 1
		cursorSource := "none"
		requestTokenSource := "none"
		if pageIndex > 0 {
			cursorSource = "local_rebuild_continuation"
			requestTokenSource = "local_rebuild_continuation"
		}
		operations, err = cardinalityAppendOperation(operations, "rebuild", "request-page", map[string]any{
			"user_id":           string(client.UserID),
			"client_id":         string(client.ClientID),
			"client_generation": clientState.CurrentGeneration,
			"schema":            schemaPayload,
			"scope_id":          string(cardinalityScope),
			"rebuild_id":        rebuildID,
			"cursor_source":     cursorSource,
			"limit":             cardinalityPageSize,
		})
		if err != nil {
			return nil, err
		}
		operations, err = cardinalityAppendOperation(operations, "local", "apply-rebuild-page", map[string]any{
			"user_id":              string(client.UserID),
			"client_id":            string(client.ClientID),
			"scope_id":             string(cardinalityScope),
			"rebuild_id":           rebuildID,
			"page_ordinal":         requestOrdinal,
			"request_token_source": requestTokenSource,
		})
		if err != nil {
			return nil, err
		}
	}
	operations, err = cardinalityAppendOperation(operations, "local", "finalize-rebuild", map[string]any{
		"user_id":    string(client.UserID),
		"client_id":  string(client.ClientID),
		"scope_id":   string(cardinalityScope),
		"rebuild_id": rebuildID,
	})
	if err != nil {
		return nil, err
	}
	return operations, nil
}

func cardinalityRequiredString(payload map[string]json.RawMessage, name string) (string, error) {
	raw, found := payload[name]
	if !found {
		return "", fmt.Errorf("scope_cardinality %s is required", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", fmt.Errorf("scope_cardinality %s must be a nonempty string", name)
	}
	return value, nil
}

func cardinalityRequiredUint(payload map[string]json.RawMessage, name string) (uint64, error) {
	raw, found := payload[name]
	if !found {
		return 0, fmt.Errorf("scope_cardinality %s is required", name)
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("scope_cardinality %s must be an unsigned integer: %w", name, err)
	}
	return value, nil
}

func cardinalityRelationInfo(snapshot reference.StateSnapshot) (cardinalityRelationContext, error) {
	if snapshot.Stream.Authority.ActiveGeneration == "" {
		return cardinalityRelationContext{}, errors.New("scope_cardinality requires an active stream generation")
	}
	var generation reference.RegistryGenerationState
	foundGeneration := false
	for _, candidate := range snapshot.Registry.Generations {
		if candidate.Generation == snapshot.Registry.CurrentGeneration {
			generation = candidate
			foundGeneration = true
			break
		}
	}
	if !foundGeneration || !generation.Validated || generation.HasBootstrapStage {
		return cardinalityRelationContext{}, errors.New("scope_cardinality requires a validated active registry generation")
	}
	var definition reference.RelationDefinition
	foundRelation := false
	for _, registered := range generation.Relations {
		candidate := registered.Definition
		if candidate.RegistrationKind != reference.RegistrationKindSynced || !candidate.HasTableID {
			continue
		}
		if candidate.Relation == cardinalityRelation || candidate.TableID == cardinalityTable {
			definition = candidate
			foundRelation = true
			break
		}
	}
	if !foundRelation {
		return cardinalityRelationContext{}, errors.New("scope_cardinality requires a registered synced items relation")
	}
	if definition.PrimaryKeyPortableType != "string" || definition.PrimaryKeyFieldID == "" || len(definition.CapturedFieldIDs) == 0 {
		return cardinalityRelationContext{}, errors.New("scope_cardinality requires a string primary key and captured fields")
	}
	if definition.TableID != cardinalityTable || definition.PrimaryKeyFieldID != cardinalityPrimaryKey {
		return cardinalityRelationContext{}, errors.New("scope_cardinality relation does not match the closed items table")
	}
	manifest, foundSchema := cardinalitySchemaManifest(snapshot, snapshot.CurrentSchema)
	if !foundSchema {
		return cardinalityRelationContext{}, errors.New("scope_cardinality current schema manifest is absent")
	}
	_, vectorManifest, err := installedVectorManifest(snapshot.CurrentSchema, manifest)
	if err != nil {
		return cardinalityRelationContext{}, fmt.Errorf("scope_cardinality current schema manifest is invalid: %w", err)
	}
	fields := make([]cardinalityFieldInfo, 0, len(definition.CapturedFieldIDs))
	for _, fieldID := range definition.CapturedFieldIDs {
		foundField := false
		for _, table := range manifest.Tables {
			if table.ID != definition.TableID {
				continue
			}
			for _, field := range table.Fields {
				if field.ID == fieldID {
					fields = append(fields, cardinalityFieldInfo{ID: field.ID, Type: field.PortableType, Nullable: field.Nullable})
					foundField = true
					break
				}
			}
		}
		if !foundField {
			return cardinalityRelationContext{}, fmt.Errorf("scope_cardinality captured field %q is absent from the current schema", fieldID)
		}
	}
	return cardinalityRelationContext{Generation: generation, Definition: definition, Fields: fields, Manifest: vectorManifest}, nil
}

func cardinalitySchemaManifest(snapshot reference.StateSnapshot, ref reference.SchemaRef) (reference.SchemaManifest, bool) {
	for _, entry := range snapshot.Schemas {
		if entry.Key == ref {
			return entry.Value, true
		}
	}
	return reference.SchemaManifest{}, false
}

func cardinalityAssignedClient(snapshot reference.StateSnapshot) (reference.ClientKey, reference.ClientState, error) {
	return cardinalityAssignedClientFor(snapshot, nil)
}

func cardinalityAssignedClientFor(snapshot reference.StateSnapshot, selectedClient *reference.ClientKey) (reference.ClientKey, reference.ClientState, error) {
	type candidate struct {
		key   reference.ClientKey
		value reference.ClientState
	}
	candidates := make([]candidate, 0)
	for _, entry := range snapshot.Clients {
		if selectedClient != nil && entry.Key != *selectedClient {
			continue
		}
		for _, assignment := range entry.Value.ScopeAssignments {
			if assignment.Scope == cardinalityScope && assignment.Assigned {
				candidates = append(candidates, candidate{key: entry.Key, value: entry.Value})
				break
			}
		}
	}
	if len(candidates) == 0 {
		if selectedClient != nil {
			return reference.ClientKey{}, reference.ClientState{}, fmt.Errorf("scope_cardinality bound client %q/%q is not assigned", selectedClient.UserID, selectedClient.ClientID)
		}
		return reference.ClientKey{}, reference.ClientState{}, errors.New("scope_cardinality requires an assigned client")
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].key.UserID != candidates[right].key.UserID {
			return candidates[left].key.UserID < candidates[right].key.UserID
		}
		return candidates[left].key.ClientID < candidates[right].key.ClientID
	})
	selected := candidates[0]
	if selected.value.CurrentGeneration == 0 {
		return reference.ClientKey{}, reference.ClientState{}, errors.New("scope_cardinality client generation is invalid")
	}
	return selected.key, selected.value, nil
}

func cardinalityScopeState(snapshot reference.StateSnapshot, scope reference.ScopeID) (reference.ScopeState, bool) {
	for _, entry := range snapshot.Scopes {
		if entry.Key == scope {
			return entry.Value, true
		}
	}
	return reference.ScopeState{}, false
}

func cardinalityCurrentRows(snapshot reference.StateSnapshot, scope reference.ScopeState, definition reference.RelationDefinition) (map[reference.RowIdentity]reference.AuthoritativeRow, uint64, error) {
	rows := make(map[reference.RowIdentity]reference.AuthoritativeRow)
	for _, entry := range snapshot.Rows {
		if entry.Value.Identity != entry.Key {
			return nil, 0, errors.New("scope_cardinality row map key differs from row identity")
		}
		if entry.Value.Identity.TableID == definition.TableID && !entry.Value.Deleted {
			rows[entry.Key] = entry.Value
		}
	}
	included := make(map[reference.RowIdentity]struct{})
	for _, membership := range scope.Membership {
		if membership.Included {
			if membership.Row.TableID != definition.TableID {
				return nil, 0, errors.New("scope_cardinality scope contains a row from another table")
			}
			if _, found := rows[membership.Row]; !found {
				return nil, 0, errors.New("scope_cardinality scope membership has no live source row")
			}
			included[membership.Row] = struct{}{}
		}
	}
	if reference.Cardinality(len(included)) != scope.Cardinality {
		return nil, 0, fmt.Errorf("scope_cardinality scope cardinality %d does not match membership count %d", scope.Cardinality, len(included))
	}
	for row := range rows {
		if _, found := included[row]; !found {
			delete(rows, row)
		}
	}
	return rows, uint64(len(included)), nil
}

func cardinalityDesiredRows(count uint64, definition reference.RelationDefinition) ([]reference.RowIdentity, error) {
	rows := make([]reference.RowIdentity, 0, count)
	for ordinal := uint64(1); ordinal <= count; ordinal++ {
		row, err := cardinalityRowIdentity(definition, ordinal)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func cardinalityRowIdentity(definition reference.RelationDefinition, ordinal uint64) (reference.RowIdentity, error) {
	if definition.PrimaryKeyPortableType != "string" {
		return reference.RowIdentity{}, errors.New("scope_cardinality row identity requires a string primary key")
	}
	value := fmt.Sprintf("%s%06d", cardinalityRowPrefix, ordinal)
	wire, err := json.Marshal(value)
	if err != nil {
		return reference.RowIdentity{}, err
	}
	identity := append([]byte("synchro:v3:row-identity:v1\x00"), nil...)
	identity = cardinalityAppendText(identity, string(definition.TableID))
	identity = cardinalityAppendText(identity, string(definition.PrimaryKeyFieldID))
	identity = append(identity, 0x01, 0x01)
	identity = cardinalityAppendText(identity, value)
	return reference.RowIdentity{
		CanonicalIdentityBytes: string(identity),
		TableID:                definition.TableID,
		PrimaryKeyFieldID:      definition.PrimaryKeyFieldID,
		PortableType:           definition.PrimaryKeyPortableType,
		CanonicalWireJSON:      string(wire),
	}, nil
}

func cardinalityAppendText(destination []byte, value string) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len([]byte(value))))
	destination = append(destination, length[:]...)
	return append(destination, []byte(value)...)
}

func cardinalityMembershipSupports(generation reference.RegistryGenerationState, relation reference.RelationID, rows []reference.RowIdentity, scope reference.ScopeID) bool {
	if len(rows) == 0 {
		return true
	}
	foundRule := false
	for _, rule := range generation.ScopeRules {
		if rule.Relation != relation {
			continue
		}
		foundRule = true
		for _, row := range rows {
			foundEvaluation := false
			for _, evaluation := range rule.Evaluations {
				if evaluation.Row != row {
					continue
				}
				for _, candidate := range evaluation.Scopes {
					if candidate == scope {
						foundEvaluation = true
						break
					}
				}
				if foundEvaluation {
					break
				}
			}
			if !foundEvaluation {
				return false
			}
		}
	}
	return foundRule
}

func cardinalityStagePayload(snapshot reference.StateSnapshot, relation cardinalityRelationContext, rows []reference.RowIdentity, scope reference.ScopeState) (map[string]any, error) {
	boundary := snapshot.Stream.Authority.GlobalMaterializationBoundary
	rules := make([]any, 0, len(relation.Generation.ScopeRules)+1)
	replaced := false
	for _, rule := range relation.Generation.ScopeRules {
		if rule.Relation == relation.Definition.Relation {
			if replaced {
				continue
			}
			rules = append(rules, cardinalityScopeRulePayload(relation, rows, uint64(relation.Generation.Generation+1)))
			replaced = true
			continue
		}
		rules = append(rules, cardinalityExistingScopeRulePayload(rule))
	}
	if !replaced {
		rules = append(rules, cardinalityScopeRulePayload(relation, rows, uint64(relation.Generation.Generation+1)))
	}
	impacts := make([]any, 0, len(relation.Generation.DependencyImpacts))
	for _, impact := range relation.Generation.DependencyImpacts {
		capturedFields := make([]string, 0, len(impact.CapturedFieldIDs))
		for _, field := range impact.CapturedFieldIDs {
			capturedFields = append(capturedFields, string(field))
		}
		affectedRows := make([]any, 0, len(impact.AffectedRows))
		for _, row := range impact.AffectedRows {
			affectedRows = append(affectedRows, cardinalityRowPayload(row))
		}
		impacts = append(impacts, map[string]any{
			"dependency_impact_id": string(impact.ID),
			"relation":             string(impact.Relation),
			"function":             string(impact.Function),
			"captured_field_ids":   capturedFields,
			"positive_row_bound":   impact.PositiveRowBound,
			"affected_rows":        affectedRows,
			"requires_rebuild":     impact.RequiresRebuild,
		})
	}
	return map[string]any{
		"registry_generation":   relation.Generation.Generation + 1,
		"membership_generation": scope.MembershipGeneration + 1,
		"batch_size":            snapshot.ConfiguredLimits.BackfillBatchMaximum,
		"activation_boundary": map[string]any{
			"stream_generation": string(boundary.StreamGeneration),
			"kind":              string(boundary.Kind),
			"commit_lsn":        strconv.FormatUint(uint64(boundary.CommitLSN), 10),
		},
		"affected_scopes":    []string{string(cardinalityScope)},
		"scope_rules":        rules,
		"dependency_impacts": impacts,
	}, nil
}

func cardinalityScopeRulePayload(relation cardinalityRelationContext, rows []reference.RowIdentity, generation uint64) map[string]any {
	evaluations := make([]any, 0, len(rows))
	for _, row := range rows {
		evaluations = append(evaluations, map[string]any{
			"row":    cardinalityRowPayload(row),
			"scopes": []string{string(cardinalityScope)},
		})
	}
	return map[string]any{
		"scope_rule_id":         fmt.Sprintf("scope-cardinality-items-%d", generation),
		"relation":              string(relation.Definition.Relation),
		"membership_function":   string(relation.Definition.MembershipFunction),
		"positive_fanout_bound": relation.Definition.PositiveFanoutBound,
		"evaluations":           evaluations,
	}
}

func cardinalityExistingScopeRulePayload(rule reference.ScopeRule) map[string]any {
	evaluations := make([]any, 0, len(rule.Evaluations))
	for _, evaluation := range rule.Evaluations {
		scopes := make([]string, 0, len(evaluation.Scopes))
		for _, scope := range evaluation.Scopes {
			scopes = append(scopes, string(scope))
		}
		evaluations = append(evaluations, map[string]any{
			"row":    cardinalityRowPayload(evaluation.Row),
			"scopes": scopes,
		})
	}
	return map[string]any{
		"scope_rule_id":         string(rule.ID),
		"relation":              string(rule.Relation),
		"membership_function":   string(rule.MembershipFunction),
		"positive_fanout_bound": rule.PositiveFanoutBound,
		"evaluations":           evaluations,
	}
}

func cardinalityRowPayload(row reference.RowIdentity) map[string]any {
	return map[string]any{
		"canonical_identity_bytes": row.CanonicalIdentityBytes,
		"table_id":                 string(row.TableID),
		"primary_key_field_id":     string(row.PrimaryKeyFieldID),
		"portable_type":            string(row.PortableType),
		"canonical_wire_json":      row.CanonicalWireJSON,
	}
}

func cardinalitySourceEvents(snapshot reference.StateSnapshot, relation cardinalityRelationContext, currentRows map[reference.RowIdentity]reference.AuthoritativeRow, currentCount, target, commitLSN uint64) ([]any, error) {
	events := make([]any, 0)
	if currentCount < target {
		for ordinal := currentCount + 1; ordinal <= target; ordinal++ {
			row, err := cardinalityNewRow(relation, ordinal, commitLSN)
			if err != nil {
				return nil, err
			}
			events = append(events, map[string]any{
				"event_ordinal": ordinal - currentCount,
				"relation":      string(relation.Definition.Relation),
				"operation":     "insert",
				"before":        nil,
				"after":         cardinalityRegisteredImage(row),
			})
		}
		return events, nil
	}
	if currentCount == 0 {
		return nil, errors.New("scope_cardinality cannot update an empty scope")
	}
	row, err := cardinalityRowIdentity(relation.Definition, target)
	if err != nil {
		return nil, err
	}
	prior, found := currentRows[row]
	if !found {
		return nil, fmt.Errorf("scope_cardinality update row %d is absent", target)
	}
	updated, err := cardinalityUpdatedRow(relation, prior, commitLSN)
	if err != nil {
		return nil, err
	}
	return []any{map[string]any{
		"event_ordinal": 1,
		"relation":      string(relation.Definition.Relation),
		"operation":     "update",
		"before":        cardinalityRegisteredImage(prior),
		"after":         cardinalityRegisteredImage(updated),
	}}, nil
}

func cardinalityNewRow(relation cardinalityRelationContext, ordinal, commitLSN uint64) (reference.AuthoritativeRow, error) {
	identity, err := cardinalityRowIdentity(relation.Definition, ordinal)
	if err != nil {
		return reference.AuthoritativeRow{}, err
	}
	version := fmt.Sprintf("scope-cardinality-%010d-%06d", commitLSN, ordinal)
	fields := cardinalityGeneratedFields(relation.Fields, identity, ordinal, commitLSN)
	row := reference.AuthoritativeRow{Identity: identity, FieldValues: fields, Version: reference.RowVersion(version)}
	checksum, err := cardinalityRowChecksum(relation, row)
	if err != nil {
		return reference.AuthoritativeRow{}, err
	}
	row.Checksum = checksum
	return row, nil
}

func cardinalityUpdatedRow(relation cardinalityRelationContext, prior reference.AuthoritativeRow, commitLSN uint64) (reference.AuthoritativeRow, error) {
	fields := append([]reference.FieldValue(nil), prior.FieldValues...)
	updated := false
	for index := range fields {
		if fields[index].Field != cardinalityValueField || fields[index].Type != "string" {
			continue
		}
		wire, err := json.Marshal(fmt.Sprintf("cardinality-update-%010d", commitLSN))
		if err != nil {
			return reference.AuthoritativeRow{}, err
		}
		fields[index].WireJSON = string(wire)
		updated = true
		break
	}
	if !updated {
		for index := range fields {
			if fields[index].Field == relation.Definition.PrimaryKeyFieldID {
				continue
			}
			wire, err := json.Marshal(fmt.Sprintf("cardinality-update-%010d", commitLSN))
			if err != nil {
				return reference.AuthoritativeRow{}, err
			}
			fields[index].WireJSON = string(wire)
			updated = true
			break
		}
	}
	if !updated {
		return reference.AuthoritativeRow{}, errors.New("scope_cardinality relation has no writable captured field")
	}
	row := reference.AuthoritativeRow{
		Identity:    prior.Identity,
		FieldValues: fields,
		Version:     reference.RowVersion(fmt.Sprintf("scope-cardinality-%010d-update", commitLSN)),
		Deleted:     false,
	}
	checksum, err := cardinalityRowChecksum(relation, row)
	if err != nil {
		return reference.AuthoritativeRow{}, err
	}
	row.Checksum = checksum
	return row, nil
}

func cardinalityGeneratedFields(fields []cardinalityFieldInfo, identity reference.RowIdentity, ordinal, commitLSN uint64) []reference.FieldValue {
	result := make([]reference.FieldValue, 0, len(fields))
	for _, field := range fields {
		wire := identity.CanonicalWireJSON
		if field.ID != identity.PrimaryKeyFieldID {
			wire = cardinalityFieldWire(field, ordinal, commitLSN)
		}
		result = append(result, reference.FieldValue{Field: field.ID, Type: field.Type, WireJSON: wire})
	}
	return result
}

func cardinalityFieldWire(field cardinalityFieldInfo, ordinal, commitLSN uint64) string {
	var value any
	switch field.Type {
	case "string":
		value = fmt.Sprintf("cardinality-value-%06d-%010d", ordinal, commitLSN)
	case "int", "float":
		return strconv.FormatUint(ordinal, 10)
	case "int64":
		value = strconv.FormatUint(ordinal, 10)
	case "boolean":
		return strconv.FormatBool(ordinal%2 == 1)
	case "datetime":
		value = "2024-01-01T00:00:00.000000Z"
	case "date":
		value = "2024-01-01"
	case "time":
		value = "00:00:00.000000"
	case "decimal":
		value = fmt.Sprintf("%d.01", ordinal)
	default:
		if field.Nullable {
			return "null"
		}
		value = fmt.Sprintf("cardinality-value-%06d-%010d", ordinal, commitLSN)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(encoded)
}

func cardinalityRowChecksum(relation cardinalityRelationContext, row reference.AuthoritativeRow) (reference.Checksum, error) {
	fields := make([]vectors.RowField, 0, len(row.FieldValues))
	for _, field := range row.FieldValues {
		fields = append(fields, vectors.RowField{FieldID: string(field.Field), Value: json.RawMessage(field.WireJSON)})
	}
	digest, err := vectors.RowDigest(
		relation.Manifest,
		string(relation.Definition.TableID),
		vectors.Row{PK: json.RawMessage(row.Identity.CanonicalWireJSON), Fields: fields},
		string(row.Version),
	)
	if err != nil {
		return reference.Checksum{}, fmt.Errorf("derive scope_cardinality row checksum: %w", err)
	}
	var result reference.Checksum
	copy(result[:], digest[:])
	return result, nil
}

func cardinalityRegisteredImage(row reference.AuthoritativeRow) map[string]any {
	fields := make([]any, 0, len(row.FieldValues))
	for _, field := range row.FieldValues {
		fields = append(fields, map[string]any{
			"field":     string(field.Field),
			"type":      string(field.Type),
			"wire_json": field.WireJSON,
		})
	}
	return map[string]any{
		"identity": map[string]any{
			"kind": "synced",
			"synced_row": map[string]any{
				"canonical_identity_bytes": row.Identity.CanonicalIdentityBytes,
				"table_id":                 string(row.Identity.TableID),
				"primary_key_field_id":     string(row.Identity.PrimaryKeyFieldID),
				"portable_type":            string(row.Identity.PortableType),
				"canonical_wire_json":      row.Identity.CanonicalWireJSON,
			},
		},
		"fields":   fields,
		"version":  string(row.Version),
		"checksum": hex.EncodeToString(row.Checksum[:]),
		"deleted":  row.Deleted,
	}
}

func cardinalityNextCommitLSN(snapshot reference.StateSnapshot) (uint64, error) {
	maximum := uint64(0)
	for _, transaction := range snapshot.Stream.Transactions {
		if transaction.ReplayKey.StreamGeneration != snapshot.Stream.Authority.ActiveGeneration {
			continue
		}
		if uint64(transaction.ReplayKey.CommitLSN) > maximum {
			maximum = uint64(transaction.ReplayKey.CommitLSN)
		}
		if uint64(transaction.EndLSN) > maximum {
			maximum = uint64(transaction.EndLSN)
		}
	}
	boundary := snapshot.Stream.Authority.GlobalMaterializationBoundary
	if boundary.StreamGeneration == snapshot.Stream.Authority.ActiveGeneration && uint64(boundary.CommitLSN) > maximum {
		maximum = uint64(boundary.CommitLSN)
	}
	if maximum > ^uint64(0)-10 {
		return 0, errors.New("scope_cardinality deterministic LSN space is exhausted")
	}
	next := ((maximum / 10) + 1) * 10
	if next == 0 {
		return 0, errors.New("scope_cardinality deterministic LSN is invalid")
	}
	return next, nil
}

func cardinalityRebuildID(commitLSN uint64) string {
	return cardinalityRebuildBase + fmt.Sprintf("%012d", commitLSN)
}

func cardinalitySchemaPayload(schema reference.SchemaRef) map[string]any {
	return map[string]any{
		"version": schema.Version,
		"hash":    hex.EncodeToString(schema.Hash[:]),
	}
}

func cardinalityAppendOperation(operations []scenarios.Operation, contractOperation, name string, payload any) ([]scenarios.Operation, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %s/%s payload: %w", contractOperation, name, err)
	}
	operation := scenarios.Operation{ContractOperation: contractOperation, Name: name, Payload: encoded}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return nil, fmt.Errorf("validate expanded %s/%s operation: %w", contractOperation, name, err)
	}
	return append(operations, operation), nil
}
