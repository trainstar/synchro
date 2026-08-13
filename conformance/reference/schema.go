package reference

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type schemaReferencePayload struct {
	Version *uint64 `json:"version"`
	Hash    *string `json:"hash"`
}

type schemaFieldPayload struct {
	ID               *string       `json:"field_id"`
	Name             *string       `json:"name"`
	Type             *PortableType `json:"type"`
	PrimaryKey       *bool         `json:"primary_key"`
	Nullable         *bool         `json:"nullable"`
	Writable         *bool         `json:"writable"`
	DecimalPrecision *uint32       `json:"decimal_precision"`
	DecimalScale     *uint32       `json:"decimal_scale"`
	DefaultWireJSON  *string       `json:"default_wire_json"`
}

type schemaIndexPayload struct {
	ID     *string   `json:"index_id"`
	Name   *string   `json:"name"`
	Fields *[]string `json:"field_ids"`
	Unique *bool     `json:"unique"`
}

type schemaTablePayload struct {
	ID                *string               `json:"table_id"`
	Relation          *string               `json:"relation_id"`
	Name              *string               `json:"name"`
	Composition       *Composition          `json:"composition"`
	PrimaryKeyFieldID *string               `json:"primary_key_field_id"`
	CreatedFieldID    *string               `json:"created_at_field_id"`
	UpdatedFieldID    *string               `json:"updated_at_field_id"`
	DeletedFieldID    *string               `json:"deleted_at_field_id"`
	Fields            *[]schemaFieldPayload `json:"fields"`
	Indexes           *[]schemaIndexPayload `json:"indexes"`
}

type publishSchemaPayload struct {
	Schema             *schemaReferencePayload `json:"schema"`
	Body               *string                 `json:"body"`
	Class              *SchemaClass            `json:"transition_class"`
	CompatibilityFloor *uint64                 `json:"compatibility_floor"`
	Tables             *[]schemaTablePayload   `json:"tables"`
	AffectedScopes     *[]string               `json:"affected_scopes"`
}

type assignmentPayload struct {
	ScopeID *string `json:"scope_id"`
}

type setClientAssignmentsPayload struct {
	UserID      *string              `json:"user_id"`
	ClientID    *string              `json:"client_id"`
	Assignments *[]assignmentPayload `json:"assignments"`
}

type schemaLineageDecision struct {
	Action         SchemaAction
	Reason         ReasonCode
	AffectedScopes []ScopeID
}

func init() {
	registerOperation("model/publish-schema", publishSchema)
	registerOperation("model/set-client-assignments", setClientAssignments)
}

func publishSchema(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request publishSchemaPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode publish-schema payload: %w", err)
	}
	ref, fresh, err := decodeSchemaReference(request.Schema, false)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate publish-schema schema: %w", err)
	}
	if fresh {
		return StepResult{}, errors.New("publish-schema reference cannot be fresh")
	}
	manifest, affected, err := decodePublishedManifest(request)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate publish-schema payload: %w", err)
	}
	if manifest.Class == SchemaClass3 && len(affected) == 0 {
		return StepResult{}, errors.New("class_3 publication requires affected scopes")
	}
	if manifest.Class != SchemaClass3 && len(affected) != 0 {
		return StepResult{}, errors.New("only class_3 publication can declare affected scopes")
	}
	if _, exists := model.state.Schemas[ref]; exists {
		return StepResult{}, errors.New("published schema reference is immutable")
	}
	for existing := range model.state.Schemas {
		if existing.Version == ref.Version {
			return StepResult{}, errors.New("schema version is already published with another hash")
		}
	}

	prior := model.state.CurrentSchema
	if prior == (SchemaRef{}) {
		if len(model.state.Schemas) != 0 {
			return StepResult{}, errors.New("schema history has no current schema")
		}
		if manifest.Class != SchemaClassInitial {
			return StepResult{}, errors.New("first published schema must be initial")
		}
		if manifest.CompatibilityFloor != ref.Version {
			return StepResult{}, errors.New("initial schema compatibility floor must equal its version")
		}
	} else {
		parent, exists := model.state.Schemas[prior]
		if !exists {
			return StepResult{}, errors.New("current schema is absent from immutable history")
		}
		if manifest.Class == SchemaClassInitial {
			return StepResult{}, errors.New("only the first schema can be initial")
		}
		if ref.Version <= prior.Version {
			return StepResult{}, errors.New("schema version must increase monotonically")
		}
		parentRef := prior
		manifest.Parent = &parentRef
		switch manifest.Class {
		case SchemaClass2:
			if manifest.CompatibilityFloor != parent.CompatibilityFloor {
				return StepResult{}, errors.New("class_2 schema must retain the compatibility floor")
			}
		case SchemaClass3, SchemaClass4:
			if manifest.CompatibilityFloor != ref.Version {
				return StepResult{}, errors.New("class_3 and class_4 schemas reset the compatibility floor")
			}
		default:
			return StepResult{}, errors.New("schema transition class is unknown")
		}
	}

	if err := validateClass3AffectedScopes(model.state, manifest.Class, affected); err != nil {
		return StepResult{}, err
	}
	model.state.Schemas[ref] = manifest
	model.state.CurrentSchema = ref
	if manifest.Class == SchemaClass3 {
		if err := invalidateClass3Scopes(model, ref, affected); err != nil {
			return StepResult{}, err
		}
	}

	action, reason := schemaActionForPublishedClass(manifest.Class)
	return StepResult{
		Kind: StepResultKindSchema,
		Schema: &SchemaObservation{
			Source:         prior,
			Target:         ref,
			Action:         action,
			Reason:         reason,
			AffectedScopes: cloneScopeIDs(affected),
		},
	}, nil
}

func setClientAssignments(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request setClientAssignmentsPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode set-client-assignments payload: %w", err)
	}
	clientKey, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate set-client-assignments payload: %w", err)
	}
	if request.Assignments == nil {
		return StepResult{}, errors.New("validate set-client-assignments payload: assignments is required")
	}

	requestedScopes := make([]ScopeID, 0, len(*request.Assignments))
	for index, assignment := range *request.Assignments {
		if assignment.ScopeID == nil || *assignment.ScopeID == "" {
			return StepResult{}, fmt.Errorf("validate assignment %d: scope_id is required", index)
		}
		scope := ScopeID(*assignment.ScopeID)
		if containsScopeID(requestedScopes, scope) {
			return StepResult{}, fmt.Errorf("validate assignment %d: duplicate scope_id", index)
		}
		if _, exists := model.state.Scopes[scope]; !exists {
			return StepResult{}, fmt.Errorf("validate assignment %d: scope is not authoritative", index)
		}
		requestedScopes = append(requestedScopes, scope)
	}
	sortScopeIDs(requestedScopes)

	client := model.state.Clients[clientKey]
	if client.Retirement != nil {
		return StepResult{}, errors.New("cannot assign scopes to a retired client")
	}
	priorGeneration := client.CurrentGeneration
	priorScopeSetVersion := client.ScopeSetVersion
	assignments := make([]ScopeAssignment, 0, len(requestedScopes))
	for _, scopeID := range requestedScopes {
		scope := model.state.Scopes[scopeID]
		if scope.MembershipGeneration == 0 || scope.RetentionGeneration == 0 {
			return StepResult{}, fmt.Errorf("assigned scope %q has an invalid generation", scopeID)
		}
		assignment := ScopeAssignment{
			Scope:                scopeID,
			MembershipGeneration: scope.MembershipGeneration,
			RetentionGeneration:  scope.RetentionGeneration,
			Assigned:             true,
			RebuildRequired:      true,
		}
		if existingIndex, exists := findScopeAssignment(client.ScopeAssignments, scopeID); exists {
			existing := client.ScopeAssignments[existingIndex]
			if existing.MembershipGeneration == assignment.MembershipGeneration && existing.RetentionGeneration == assignment.RetentionGeneration {
				assignment.RebuildRequired = existing.RebuildRequired
			}
		}
		assignments = append(assignments, assignment)
	}

	setChanged := !sameAssignedScopeSet(client.ScopeAssignments, assignments)
	if setChanged {
		if uint64(client.ScopeSetVersion) >= maxProtocolCounter {
			return StepResult{}, errors.New("scope-set version allocation exceeds the protocol limit")
		}
		client.ScopeSetVersion++
	}
	client.ScopeAssignments = assignments
	client.Checkpoints = retainCurrentAssignmentCheckpoints(client.Checkpoints, assignments)
	model.state.Clients[clientKey] = client

	return StepResult{
		Kind: StepResultKindClient,
		Client: &ClientObservation{
			Client:               clientKey,
			PriorGeneration:      priorGeneration,
			NewGeneration:        client.CurrentGeneration,
			PriorScopeSetVersion: priorScopeSetVersion,
			NewScopeSetVersion:   client.ScopeSetVersion,
		},
	}, nil
}

func decodeSchemaReference(payload *schemaReferencePayload, allowFresh bool) (SchemaRef, bool, error) {
	if payload == nil || payload.Version == nil || payload.Hash == nil {
		return SchemaRef{}, false, errors.New("schema version and hash are required")
	}
	if *payload.Version > maxProtocolCounter {
		return SchemaRef{}, false, errors.New("schema version exceeds the protocol limit")
	}
	if *payload.Version == 0 && *payload.Hash == "" {
		if !allowFresh {
			return SchemaRef{}, true, errors.New("fresh schema reference is not allowed")
		}
		return SchemaRef{}, true, nil
	}
	if *payload.Version == 0 || *payload.Hash == "" {
		return SchemaRef{}, false, errors.New("schema reference is neither normal nor fresh")
	}
	if len(*payload.Hash) != 64 {
		return SchemaRef{}, false, errors.New("schema hash must have 64 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(*payload.Hash)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != *payload.Hash {
		return SchemaRef{}, false, errors.New("schema hash must have 64 lowercase hexadecimal characters")
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return SchemaRef{Version: *payload.Version, Hash: hash}, false, nil
}

func decodePublishedManifest(payload publishSchemaPayload) (SchemaManifest, []ScopeID, error) {
	if payload.Body == nil || *payload.Body == "" {
		return SchemaManifest{}, nil, errors.New("body is required")
	}
	if payload.Class == nil || !knownSchemaClass(*payload.Class) {
		return SchemaManifest{}, nil, errors.New("transition_class is required and must be known")
	}
	if payload.CompatibilityFloor == nil || *payload.CompatibilityFloor == 0 || *payload.CompatibilityFloor > maxProtocolCounter {
		return SchemaManifest{}, nil, errors.New("compatibility_floor is required and must be positive")
	}
	if payload.Tables == nil {
		return SchemaManifest{}, nil, errors.New("tables is required")
	}
	if payload.AffectedScopes == nil {
		return SchemaManifest{}, nil, errors.New("affected_scopes is required")
	}

	tables := make([]TableManifest, 0, len(*payload.Tables))
	for index, tablePayload := range *payload.Tables {
		table, err := decodeSchemaTable(tablePayload)
		if err != nil {
			return SchemaManifest{}, nil, fmt.Errorf("table %d: %w", index, err)
		}
		for _, existing := range tables {
			if existing.ID == table.ID {
				return SchemaManifest{}, nil, errors.New("duplicate table_id")
			}
		}
		tables = append(tables, table)
	}

	affected := make([]ScopeID, 0, len(*payload.AffectedScopes))
	for index, scope := range *payload.AffectedScopes {
		if scope == "" {
			return SchemaManifest{}, nil, fmt.Errorf("affected scope %d is empty", index)
		}
		scopeID := ScopeID(scope)
		if containsScopeID(affected, scopeID) {
			return SchemaManifest{}, nil, fmt.Errorf("affected scope %d is duplicated", index)
		}
		affected = append(affected, scopeID)
	}
	sortScopeIDs(affected)

	return SchemaManifest{
		Body:               []byte(*payload.Body),
		Class:              *payload.Class,
		CompatibilityFloor: *payload.CompatibilityFloor,
		Tables:             tables,
		AffectedScopes:     cloneScopeIDs(affected),
	}, affected, nil
}

func decodeSchemaTable(payload schemaTablePayload) (TableManifest, error) {
	if payload.ID == nil || *payload.ID == "" || payload.Relation == nil || *payload.Relation == "" || payload.Name == nil || *payload.Name == "" {
		return TableManifest{}, errors.New("table_id, relation_id, and name are required")
	}
	if payload.Composition == nil || !knownComposition(*payload.Composition) {
		return TableManifest{}, errors.New("composition is required and must be known")
	}
	if payload.PrimaryKeyFieldID == nil || *payload.PrimaryKeyFieldID == "" {
		return TableManifest{}, errors.New("primary_key_field_id is required")
	}
	if payload.Fields == nil || payload.Indexes == nil {
		return TableManifest{}, errors.New("fields and indexes are required")
	}

	table := TableManifest{
		ID:                TableID(*payload.ID),
		Relation:          RelationID(*payload.Relation),
		Name:              *payload.Name,
		Composition:       *payload.Composition,
		PrimaryKeyFieldID: FieldID(*payload.PrimaryKeyFieldID),
		Fields:            make([]FieldManifest, 0, len(*payload.Fields)),
		Indexes:           make([]IndexManifest, 0, len(*payload.Indexes)),
	}
	if payload.CreatedFieldID != nil {
		field := FieldID(*payload.CreatedFieldID)
		table.CreatedFieldID = &field
	}
	if payload.UpdatedFieldID != nil {
		field := FieldID(*payload.UpdatedFieldID)
		table.UpdatedFieldID = &field
	}
	if payload.DeletedFieldID != nil {
		field := FieldID(*payload.DeletedFieldID)
		table.DeletedFieldID = &field
	}

	primaryFound := false
	for index, fieldPayload := range *payload.Fields {
		field, err := decodeSchemaField(fieldPayload)
		if err != nil {
			return TableManifest{}, fmt.Errorf("field %d: %w", index, err)
		}
		for _, existing := range table.Fields {
			if existing.ID == field.ID {
				return TableManifest{}, errors.New("duplicate field_id")
			}
		}
		if field.ID == table.PrimaryKeyFieldID {
			primaryFound = true
			if !field.PrimaryKey {
				return TableManifest{}, errors.New("primary key field must declare primary_key")
			}
		}
		if field.PrimaryKey && field.ID != table.PrimaryKeyFieldID {
			return TableManifest{}, errors.New("only primary_key_field_id can declare primary_key")
		}
		table.Fields = append(table.Fields, field)
	}
	if !primaryFound {
		return TableManifest{}, errors.New("primary_key_field_id is absent from fields")
	}
	if err := validateLifecycleField(table, table.CreatedFieldID); err != nil {
		return TableManifest{}, fmt.Errorf("created_at_field_id: %w", err)
	}
	if err := validateLifecycleField(table, table.UpdatedFieldID); err != nil {
		return TableManifest{}, fmt.Errorf("updated_at_field_id: %w", err)
	}
	if err := validateLifecycleField(table, table.DeletedFieldID); err != nil {
		return TableManifest{}, fmt.Errorf("deleted_at_field_id: %w", err)
	}

	for index, indexPayload := range *payload.Indexes {
		indexManifest, err := decodeSchemaIndex(indexPayload, table.Fields)
		if err != nil {
			return TableManifest{}, fmt.Errorf("index %d: %w", index, err)
		}
		for _, existing := range table.Indexes {
			if existing.ID == indexManifest.ID {
				return TableManifest{}, errors.New("duplicate index_id")
			}
		}
		table.Indexes = append(table.Indexes, indexManifest)
	}
	return table, nil
}

func decodeSchemaField(payload schemaFieldPayload) (FieldManifest, error) {
	if payload.ID == nil || *payload.ID == "" || payload.Name == nil || *payload.Name == "" || payload.Type == nil {
		return FieldManifest{}, errors.New("field_id, name, and type are required")
	}
	if payload.PrimaryKey == nil || payload.Nullable == nil || payload.Writable == nil {
		return FieldManifest{}, errors.New("primary_key, nullable, and writable are required")
	}
	if !knownPortableType(*payload.Type) {
		return FieldManifest{}, errors.New("type is unknown")
	}
	field := FieldManifest{
		ID:              FieldID(*payload.ID),
		Name:            *payload.Name,
		PortableType:    *payload.Type,
		PrimaryKey:      *payload.PrimaryKey,
		Nullable:        *payload.Nullable,
		Writable:        *payload.Writable,
		DefaultWireJSON: cloneString(payload.DefaultWireJSON),
	}
	if *payload.Type == PortableType("decimal") {
		if payload.DecimalPrecision == nil || payload.DecimalScale == nil || *payload.DecimalPrecision == 0 || *payload.DecimalScale > *payload.DecimalPrecision {
			return FieldManifest{}, errors.New("decimal fields require valid precision and scale")
		}
		field.HasDecimalPrecision = true
		field.DecimalPrecision = *payload.DecimalPrecision
		field.HasDecimalScale = true
		field.DecimalScale = *payload.DecimalScale
	} else if payload.DecimalPrecision != nil || payload.DecimalScale != nil {
		return FieldManifest{}, errors.New("only decimal fields can declare precision or scale")
	}
	return field, nil
}

func decodeSchemaIndex(payload schemaIndexPayload, fields []FieldManifest) (IndexManifest, error) {
	if payload.ID == nil || *payload.ID == "" || payload.Name == nil || *payload.Name == "" || payload.Fields == nil || payload.Unique == nil {
		return IndexManifest{}, errors.New("index_id, name, field_ids, and unique are required")
	}
	if len(*payload.Fields) == 0 {
		return IndexManifest{}, errors.New("field_ids must not be empty")
	}
	index := IndexManifest{ID: IndexID(*payload.ID), Name: *payload.Name, Unique: *payload.Unique, Fields: make([]FieldID, 0, len(*payload.Fields))}
	for position, field := range *payload.Fields {
		if field == "" {
			return IndexManifest{}, fmt.Errorf("field_ids[%d] is empty", position)
		}
		fieldID := FieldID(field)
		if containsFieldID(index.Fields, fieldID) {
			return IndexManifest{}, fmt.Errorf("field_ids[%d] is duplicated", position)
		}
		if !manifestHasField(fields, fieldID) {
			return IndexManifest{}, fmt.Errorf("field_ids[%d] is not a table field", position)
		}
		index.Fields = append(index.Fields, fieldID)
	}
	return index, nil
}

func validateLifecycleField(table TableManifest, fieldID *FieldID) error {
	if fieldID == nil {
		return nil
	}
	for _, field := range table.Fields {
		if field.ID != *fieldID {
			continue
		}
		if field.PortableType != PortableType("datetime") || field.Writable {
			return errors.New("lifecycle field must be non-writable datetime")
		}
		return nil
	}
	return errors.New("lifecycle field is absent from fields")
}

func validateClass3AffectedScopes(state State, class SchemaClass, affected []ScopeID) error {
	if class != SchemaClass3 {
		return nil
	}
	for _, scope := range affected {
		stateScope, exists := state.Scopes[scope]
		if !exists {
			return fmt.Errorf("affected scope %q is not authoritative", scope)
		}
		if stateScope.MembershipGeneration == 0 || uint64(stateScope.MembershipGeneration) >= maxProtocolCounter {
			return fmt.Errorf("affected scope %q cannot allocate membership generation", scope)
		}
	}
	return nil
}

func invalidateClass3Scopes(model *Model, schema SchemaRef, affected []ScopeID) error {
	for _, scopeID := range affected {
		scope := model.state.Scopes[scopeID]
		scope.MembershipGeneration++
		scope.Schema = schema
		model.state.Scopes[scopeID] = scope

		for clientKey, client := range model.state.Clients {
			changed := false
			for index := range client.ScopeAssignments {
				assignment := &client.ScopeAssignments[index]
				if !assignment.Assigned || assignment.Scope != scopeID {
					continue
				}
				assignment.MembershipGeneration = scope.MembershipGeneration
				assignment.RetentionGeneration = scope.RetentionGeneration
				assignment.RebuildRequired = true
				changed = true
			}
			if !changed {
				continue
			}
			client.Checkpoints = retainCurrentAssignmentCheckpoints(client.Checkpoints, client.ScopeAssignments)
			if checkpointIndex, exists := findClientCheckpoint(client.Checkpoints, scopeID); exists {
				client.Checkpoints = append(client.Checkpoints[:checkpointIndex], client.Checkpoints[checkpointIndex+1:]...)
			}
			model.state.Clients[clientKey] = client
		}
	}
	return nil
}

func schemaActionForPublishedClass(class SchemaClass) (SchemaAction, ReasonCode) {
	switch class {
	case SchemaClassInitial, SchemaClass2:
		return SchemaActionReplace, ""
	case SchemaClass3:
		return SchemaActionRebuildLocal, ""
	case SchemaClass4:
		return SchemaActionUnsupported, ReasonCode("incompatible_schema_transition")
	default:
		return SchemaActionUnsupported, ReasonCode("unknown_schema_lineage")
	}
}

func resolveSchemaLineage(state State, source SchemaRef) schemaLineageDecision {
	if source == state.CurrentSchema {
		return schemaLineageDecision{Action: SchemaActionNone}
	}
	if _, exists := state.Schemas[source]; !exists {
		return schemaLineageDecision{Action: SchemaActionUnsupported, Reason: ReasonCode("unknown_schema_lineage")}
	}
	current := state.CurrentSchema
	affected := make([]ScopeID, 0)
	for steps := 0; steps <= len(state.Schemas); steps++ {
		if current == source {
			sortScopeIDs(affected)
			return schemaLineageDecision{Action: SchemaActionReplace, AffectedScopes: affected}
		}
		manifest, exists := state.Schemas[current]
		if !exists || manifest.Parent == nil {
			return schemaLineageDecision{Action: SchemaActionUnsupported, Reason: ReasonCode("unknown_schema_lineage")}
		}
		switch manifest.Class {
		case SchemaClass2:
		case SchemaClass3:
			for _, scope := range manifest.AffectedScopes {
				if !containsScopeID(affected, scope) {
					affected = append(affected, scope)
				}
			}
		case SchemaClass4:
			return schemaLineageDecision{Action: SchemaActionUnsupported, Reason: ReasonCode("incompatible_schema_transition")}
		default:
			return schemaLineageDecision{Action: SchemaActionUnsupported, Reason: ReasonCode("unknown_schema_lineage")}
		}
		current = *manifest.Parent
	}
	return schemaLineageDecision{Action: SchemaActionUnsupported, Reason: ReasonCode("unknown_schema_lineage")}
}

func appendSchemaJournal(local *ClientLocalState, source, target SchemaRef, manifest SchemaManifest, action SchemaAction, affected []ScopeID, hasCursorUpdates bool) error {
	if local == nil {
		return errors.New("local state is required")
	}
	if action != SchemaActionReplace && action != SchemaActionRebuildLocal {
		return errors.New("schema journal requires an inline schema action")
	}
	journalVersion := uint64(0)
	planVersion := uint64(0)
	for _, entry := range local.SchemaJournal {
		if entry.JournalVersion > journalVersion {
			journalVersion = entry.JournalVersion
		}
		if entry.MigrationPlanVersion > planVersion {
			planVersion = entry.MigrationPlanVersion
		}
	}
	if journalVersion >= maxProtocolCounter || planVersion >= maxProtocolCounter {
		return errors.New("schema journal allocation exceeds the protocol limit")
	}
	plan := []MigrationPlanOperation{
		{Kind: MigrationOperationUpdateSchemaMetadata},
		{Kind: MigrationOperationUpdateAssignment},
	}
	if hasCursorUpdates {
		plan = append(plan, MigrationPlanOperation{Kind: MigrationOperationUpdateCursor})
	}
	if action == SchemaActionRebuildLocal {
		plan = append(plan,
			MigrationPlanOperation{Kind: MigrationOperationUpdateProvenance},
			MigrationPlanOperation{Kind: MigrationOperationUpdateChecksum},
		)
	}
	local.SchemaJournal = append(local.SchemaJournal, SchemaJournalEntry{
		JournalVersion:         journalVersion + 1,
		MigrationPlanVersion:   planVersion + 1,
		SourceSchema:           source,
		TargetSchema:           target,
		VerifiedTargetManifest: cloneSchemaManifest(manifest),
		Action:                 action,
		AffectedScopes:         cloneScopeIDs(affected),
		MigrationPlan:          plan,
		Phase:                  MigrationPhaseApplied,
		Ordinal:                uint64(len(local.SchemaJournal) + 1),
	})
	return nil
}

func knownSchemaClass(class SchemaClass) bool {
	switch class {
	case SchemaClassInitial, SchemaClass2, SchemaClass3, SchemaClass4:
		return true
	default:
		return false
	}
}

func knownComposition(composition Composition) bool {
	return composition == CompositionSingleScope || composition == CompositionMultiScope
}

func knownPortableType(portableType PortableType) bool {
	switch portableType {
	case "string", "int", "int64", "decimal", "float", "boolean", "datetime", "date", "time", "json", "bytes":
		return true
	default:
		return false
	}
}

func containsScopeID(scopes []ScopeID, wanted ScopeID) bool {
	for _, scope := range scopes {
		if scope == wanted {
			return true
		}
	}
	return false
}

func containsFieldID(fields []FieldID, wanted FieldID) bool {
	for _, field := range fields {
		if field == wanted {
			return true
		}
	}
	return false
}

func manifestHasField(fields []FieldManifest, wanted FieldID) bool {
	for _, field := range fields {
		if field.ID == wanted {
			return true
		}
	}
	return false
}

func sortScopeIDs(scopes []ScopeID) {
	sort.Slice(scopes, func(left, right int) bool {
		return scopes[left] < scopes[right]
	})
}

func sameAssignedScopeSet(left, right []ScopeAssignment) bool {
	leftScopes := make([]ScopeID, 0, len(left))
	for _, assignment := range left {
		if assignment.Assigned {
			leftScopes = append(leftScopes, assignment.Scope)
		}
	}
	rightScopes := make([]ScopeID, 0, len(right))
	for _, assignment := range right {
		if assignment.Assigned {
			rightScopes = append(rightScopes, assignment.Scope)
		}
	}
	sortScopeIDs(leftScopes)
	sortScopeIDs(rightScopes)
	if len(leftScopes) != len(rightScopes) {
		return false
	}
	for index := range leftScopes {
		if leftScopes[index] != rightScopes[index] {
			return false
		}
	}
	return true
}

func retainCurrentAssignmentCheckpoints(checkpoints []ClientCheckpoint, assignments []ScopeAssignment) []ClientCheckpoint {
	retained := make([]ClientCheckpoint, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		assignmentIndex, exists := findScopeAssignment(assignments, checkpoint.Scope)
		if !exists || !assignments[assignmentIndex].Assigned || assignments[assignmentIndex].RebuildRequired {
			continue
		}
		retained = append(retained, checkpoint)
	}
	return retained
}
