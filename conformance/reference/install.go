package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/trainstar/synchro/conformance/vectors"
)

type installCurrentContractPayload struct {
	Installation     *installInstallationPayload  `json:"installation"`
	InitialSchema    *publishSchemaPayload        `json:"initial_schema"`
	InitialRegistry  *installRegistryPayload      `json:"initial_registry"`
	Stream           *installStreamPayload        `json:"stream"`
	EmptyScopes      *[]installScopePayload       `json:"empty_scopes"`
	Clients          *[]installClientPayload      `json:"clients"`
	WritePolicies    *[]installWritePolicyPayload `json:"write_policies"`
	ConfiguredLimits *installLimitsPayload        `json:"configured_limits"`
}

type installInstallationPayload struct {
	Installed                       *bool                       `json:"installed"`
	SchemaName                      *string                     `json:"schema_name"`
	ExtensionVersion                *string                     `json:"extension_version"`
	ProtocolVersion                 *int                        `json:"protocol_version"`
	MinimumClientRuntime            *int                        `json:"minimum_client_runtime"`
	StaleClientIntervalMilliseconds *uint64                     `json:"stale_client_interval_milliseconds"`
	Endpoints                       *[]string                   `json:"endpoints"`
	Capabilities                    *[]installCapabilityPayload `json:"capabilities"`
}

type installCapabilityPayload struct {
	ID      *string `json:"capability_id"`
	Enabled *bool   `json:"enabled"`
}

type installRegistryPayload struct {
	RegistryGeneration  *uint64                              `json:"registry_generation"`
	Relations           *[]installRelationPayload            `json:"relations"`
	CaptureDependencies *[]installCaptureDependencyPayload   `json:"capture_dependencies"`
	ScopeRules          *[]membershipScopeRulePayload        `json:"scope_rules"`
	DependencyImpacts   *[]membershipDependencyImpactPayload `json:"dependency_impacts"`
}

type installRelationPayload struct {
	Relation                   *string                 `json:"relation"`
	RegistrationKind           *RegistrationKind       `json:"registration_kind"`
	TableID                    nullableString          `json:"table_id"`
	Physical                   *installPhysicalPayload `json:"physical"`
	PrimaryKeyFieldID          nullableString          `json:"primary_key_field_id"`
	PrimaryKeyPhysicalColumn   nullableString          `json:"primary_key_physical_column"`
	PrimaryKeyPortableType     nullablePortableType    `json:"primary_key_portable_type"`
	CaptureKeyFieldIDs         *[]string               `json:"capture_key_field_ids"`
	CapturedFieldIDs           *[]string               `json:"captured_field_ids"`
	MembershipFunction         nullableString          `json:"membership_function"`
	PositiveFanoutBound        *uint64                 `json:"positive_fanout_bound"`
	DependencyImpactFunction   nullableString          `json:"dependency_impact_function"`
	DependencyCapturedFieldIDs *[]string               `json:"dependency_captured_field_ids"`
	PositiveDependencyRowBound nullableUint64          `json:"positive_dependency_row_bound"`
}

type installPhysicalPayload struct {
	Schema          *string          `json:"schema"`
	Name            *string          `json:"name"`
	OID             *uint32          `json:"oid"`
	ReplicaIdentity *ReplicaIdentity `json:"replica_identity"`
}

type installCaptureDependencyPayload struct {
	ID        *string `json:"capture_dependency_id"`
	Relation  *string `json:"relation"`
	DependsOn *string `json:"depends_on"`
}

type installStreamPayload struct {
	StreamGeneration *string `json:"stream_generation"`
	Database         *string `json:"database"`
	WorkerID         *string `json:"worker_id"`
	SlotID           *string `json:"slot_id"`
}

type installScopePayload struct {
	ScopeID              *string `json:"scope_id"`
	MembershipGeneration *uint64 `json:"membership_generation"`
	RetentionGeneration  *uint64 `json:"retention_generation"`
}

type installClientPayload struct {
	UserID                   *string                 `json:"user_id"`
	ClientID                 *string                 `json:"client_id"`
	ClientGeneration         *uint64                 `json:"client_generation"`
	ScopeSetVersion          *uint64                 `json:"scope_set_version"`
	AcceptedWriteEpoch       *uint64                 `json:"accepted_write_epoch"`
	LastCursorAcknowledgedAt *string                 `json:"last_cursor_acknowledged_at"`
	AssignedScopeIDs         *[]string               `json:"assigned_scope_ids"`
	LocalSchema              *schemaReferencePayload `json:"local_schema"`
	LocalLifecycle           *ClientLifecycle        `json:"local_lifecycle"`
}

type installWritePolicyPayload struct {
	UserID  *string `json:"user_id"`
	TableID *string `json:"table_id"`
	Allowed *bool   `json:"allowed"`
}

type installLimitsPayload struct {
	MaxScopeFanout         *uint64 `json:"max_scope_fanout"`
	MaxImpactRows          *uint64 `json:"max_impact_rows"`
	PullMaximum            *uint32 `json:"pull_maximum"`
	RebuildMaximum         *uint32 `json:"rebuild_maximum"`
	CompactionBatchMaximum *uint64 `json:"compaction_batch_maximum"`
	BackfillBatchMaximum   *uint64 `json:"backfill_batch_maximum"`
}

type nullableString struct {
	Set   bool
	Valid bool
	Value string
}

func (value *nullableString) UnmarshalJSON(data []byte) error {
	value.Set = true
	if string(data) == "null" {
		return nil
	}
	if err := json.Unmarshal(data, &value.Value); err != nil {
		return err
	}
	value.Valid = true
	return nil
}

type nullablePortableType struct {
	Set   bool
	Valid bool
	Value PortableType
}

func (value *nullablePortableType) UnmarshalJSON(data []byte) error {
	value.Set = true
	if string(data) == "null" {
		return nil
	}
	if err := json.Unmarshal(data, &value.Value); err != nil {
		return err
	}
	value.Valid = true
	return nil
}

type nullableUint64 struct {
	Set   bool
	Valid bool
	Value uint64
}

func (value *nullableUint64) UnmarshalJSON(data []byte) error {
	value.Set = true
	if string(data) == "null" {
		return nil
	}
	if err := json.Unmarshal(data, &value.Value); err != nil {
		return err
	}
	value.Valid = true
	return nil
}

func installCurrentContract(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request installCurrentContractPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode install-current-contract payload: %w", err)
	}
	if !stateIsUnconfigured(model.state) {
		return StepResult{}, errors.New("install-current-contract requires an empty protocol state")
	}
	limits, err := decodeInstallLimits(request.ConfiguredLimits)
	if err != nil {
		return StepResult{}, err
	}
	installation, err := decodeInstallCapabilities(request.Installation, model.clock.Now())
	if err != nil {
		return StepResult{}, err
	}
	if request.InitialSchema == nil || request.InitialSchema.Class == nil || *request.InitialSchema.Class != SchemaClassInitial {
		return StepResult{}, errors.New("initial_schema transition_class must be initial")
	}
	schema, fresh, err := decodeSchemaReference(request.InitialSchema.Schema, false)
	if err != nil || fresh {
		return StepResult{}, fmt.Errorf("validate initial_schema reference: %w", err)
	}
	manifest, affected, err := decodePublishedManifest(*request.InitialSchema)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate initial_schema: %w", err)
	}
	if len(affected) != 0 {
		return StepResult{}, errors.New("initial_schema affected_scopes must be empty")
	}
	stream, readiness, err := decodeInstallStream(request.Stream)
	if err != nil {
		return StepResult{}, err
	}
	registry, relations, err := decodeInstallRegistry(request.InitialRegistry, manifest, schema, stream, limits)
	if err != nil {
		return StepResult{}, err
	}
	scopes, err := decodeInstallScopes(request.EmptyScopes, schema, stream.Authority.ActiveGeneration)
	if err != nil {
		return StepResult{}, err
	}
	clients, local, err := decodeInstallClients(request.Clients, scopes, schema, model.clock.Now())
	if err != nil {
		return StepResult{}, err
	}
	policies, err := decodeInstallWritePolicies(request.WritePolicies, manifest)
	if err != nil {
		return StepResult{}, err
	}

	model.state.Schemas = map[SchemaRef]SchemaManifest{schema: manifest}
	model.state.CurrentSchema = schema
	model.state.Registry = registry
	model.state.Relations = relations
	model.state.Clients = clients
	model.state.ClientLocal = local
	model.state.Scopes = scopes
	model.state.Stream = stream
	model.state.Authorization = AuthorizationState{WritePolicies: policies}
	model.state.Installation = installation
	model.state.ConfiguredLimits = limits
	model.state.Readiness = readiness
	return StepResult{Kind: StepResultKindContractInstalled}, nil
}

func stateIsUnconfigured(state State) bool {
	return len(state.Schemas) == 0 && state.CurrentSchema == (SchemaRef{}) && len(state.Registry.Generations) == 0 &&
		len(state.Relations) == 0 && len(state.Clients) == 0 && len(state.Rows) == 0 && len(state.Scopes) == 0 &&
		state.Stream.Authority.ActiveGeneration == "" && len(state.Fences) == 0 && len(state.Projections) == 0 &&
		len(state.Batches) == 0 && len(state.Mutations) == 0 && len(state.Rebuilds) == 0 && len(state.ClientLocal) == 0 &&
		len(state.RetentionFloors) == 0 && len(state.Seed.Exports) == 0 && len(state.Seed.Records) == 0 && len(state.Events) == 0
}

func decodeInstallLimits(payload *installLimitsPayload) (ConfiguredLimits, error) {
	if payload == nil || payload.MaxScopeFanout == nil || payload.MaxImpactRows == nil || payload.PullMaximum == nil ||
		payload.RebuildMaximum == nil || payload.CompactionBatchMaximum == nil || payload.BackfillBatchMaximum == nil {
		return ConfiguredLimits{}, errors.New("configured_limits is incomplete")
	}
	limits := ConfiguredLimits{
		MaxScopeFanout: *payload.MaxScopeFanout, MaxImpactRows: *payload.MaxImpactRows,
		PullMaximum: *payload.PullMaximum, RebuildMaximum: *payload.RebuildMaximum,
		CompactionBatchMaximum: *payload.CompactionBatchMaximum, BackfillBatchMaximum: *payload.BackfillBatchMaximum,
	}
	if limits != (ConfiguredLimits{MaxScopeFanout: 8, MaxImpactRows: 1000, PullMaximum: 1000, RebuildMaximum: 1000, CompactionBatchMaximum: 10000, BackfillBatchMaximum: 1000}) {
		return ConfiguredLimits{}, errors.New("configured_limits do not equal the Protocol 3 release maxima")
	}
	return limits, nil
}

func decodeInstallCapabilities(payload *installInstallationPayload, now time.Time) (InstallationCapabilities, error) {
	if payload == nil || payload.Installed == nil || payload.SchemaName == nil || payload.ExtensionVersion == nil || payload.ProtocolVersion == nil ||
		payload.MinimumClientRuntime == nil || payload.StaleClientIntervalMilliseconds == nil || payload.Endpoints == nil || payload.Capabilities == nil {
		return InstallationCapabilities{}, errors.New("installation is incomplete")
	}
	if !*payload.Installed || *payload.SchemaName == "" || *payload.ExtensionVersion != "0.3.0" || *payload.ProtocolVersion != 3 ||
		*payload.MinimumClientRuntime != 3 || *payload.StaleClientIntervalMilliseconds == 0 {
		return InstallationCapabilities{}, errors.New("installation has invalid Protocol 3 capabilities")
	}
	endpoints := make([]Endpoint, 0, len(*payload.Endpoints))
	seenEndpoints := make(map[Endpoint]struct{})
	for _, text := range *payload.Endpoints {
		endpoint := Endpoint(text)
		if endpoint == "" {
			return InstallationCapabilities{}, errors.New("installation endpoint is empty")
		}
		if _, duplicate := seenEndpoints[endpoint]; duplicate {
			return InstallationCapabilities{}, errors.New("installation endpoint is duplicated")
		}
		seenEndpoints[endpoint] = struct{}{}
		endpoints = append(endpoints, endpoint)
	}
	capabilities := make([]InstallationCapability, 0, len(*payload.Capabilities))
	seenCapabilities := make(map[CapabilityID]struct{})
	checkedAt := now.Round(0).UTC()
	for _, item := range *payload.Capabilities {
		if item.ID == nil || *item.ID == "" || item.Enabled == nil {
			return InstallationCapabilities{}, errors.New("installation capability is incomplete")
		}
		id := CapabilityID(*item.ID)
		if _, duplicate := seenCapabilities[id]; duplicate {
			return InstallationCapabilities{}, errors.New("installation capability is duplicated")
		}
		seenCapabilities[id] = struct{}{}
		capabilities = append(capabilities, InstallationCapability{ID: id, Enabled: *item.Enabled, CheckedAt: &checkedAt})
	}
	return InstallationCapabilities{Installed: true, SchemaName: *payload.SchemaName, ExtensionVersion: *payload.ExtensionVersion,
		ProtocolVersion: 3, MinimumClientRuntime: 3, StaleClientIntervalMilliseconds: *payload.StaleClientIntervalMilliseconds,
		Endpoints: endpoints, Capabilities: capabilities}, nil
}

func decodeInstallStream(payload *installStreamPayload) (StreamState, ReadinessState, error) {
	if payload == nil || payload.StreamGeneration == nil || *payload.StreamGeneration == "" || payload.Database == nil || *payload.Database == "" ||
		payload.WorkerID == nil || *payload.WorkerID == "" || payload.SlotID == nil || *payload.SlotID == "" {
		return StreamState{}, ReadinessState{}, errors.New("stream is incomplete")
	}
	generation := StreamGeneration(*payload.StreamGeneration)
	boundary := StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart}
	database := DatabaseName(*payload.Database)
	worker := WorkerID(*payload.WorkerID)
	slot := SlotID(*payload.SlotID)
	stream := StreamState{Authority: StreamAuthority{ActiveGeneration: generation, GlobalMaterializationBoundary: boundary, HasActiveSlot: true, ActiveSlot: slot}}
	readiness := ReadinessState{ConfiguredDatabase: database, Workers: []WorkerReadiness{{ID: worker, Database: database, Running: true, RegistryGeneration: 1, MaterializedPosition: boundary}}, Slots: []SlotReadiness{{ID: slot, Database: database, Plugin: "pgoutput", Active: true}}}
	return stream, readiness, nil
}

func decodeInstallRegistry(payload *installRegistryPayload, manifest SchemaManifest, schema SchemaRef, stream StreamState, limits ConfiguredLimits) (RegistryState, map[RelationID]RelationState, error) {
	if payload == nil || payload.RegistryGeneration == nil || *payload.RegistryGeneration == 0 || payload.Relations == nil || payload.CaptureDependencies == nil || payload.ScopeRules == nil || payload.DependencyImpacts == nil {
		return RegistryState{}, nil, errors.New("initial_registry is incomplete")
	}
	definitions := make(map[RelationID]RelationDefinition)
	tableOwners := make(map[TableID]RelationID)
	physicalOwners := make(map[string]RelationID)
	for index, item := range *payload.Relations {
		definition, err := decodeInstallRelation(item, manifest, limits)
		if err != nil {
			return RegistryState{}, nil, fmt.Errorf("initial_registry relation %d: %w", index, err)
		}
		if _, duplicate := definitions[definition.Relation]; duplicate {
			return RegistryState{}, nil, errors.New("initial_registry relation identity is duplicated")
		}
		physicalKey := definition.Physical.Schema + "." + definition.Physical.Name
		if _, duplicate := physicalOwners[physicalKey]; duplicate {
			return RegistryState{}, nil, errors.New("initial_registry physical identity is duplicated")
		}
		if definition.HasTableID {
			if _, duplicate := tableOwners[definition.TableID]; duplicate {
				return RegistryState{}, nil, errors.New("initial_registry table identity is duplicated")
			}
			tableOwners[definition.TableID] = definition.Relation
		}
		definitions[definition.Relation] = definition
		physicalOwners[physicalKey] = definition.Relation
	}
	dependencies, err := decodeInstallCaptureDependencies(*payload.CaptureDependencies, definitions)
	if err != nil {
		return RegistryState{}, nil, err
	}
	generation := RegistryGenerationState{Generation: Generation(*payload.RegistryGeneration), ActivationBoundary: stream.Authority.GlobalMaterializationBoundary, Validated: true}
	for _, id := range sortedRelationDefinitionIDs(definitions) {
		generation.Relations = append(generation.Relations, RegistryRelation{Definition: definitions[id]})
	}
	generation.CaptureDependencies = dependencies
	rules, err := membershipDecodeScopeRules(*payload.ScopeRules, generation)
	if err != nil {
		return RegistryState{}, nil, fmt.Errorf("initial_registry scope_rules: %w", err)
	}
	impacts, err := membershipDecodeDependencyImpacts(*payload.DependencyImpacts, generation)
	if err != nil {
		return RegistryState{}, nil, fmt.Errorf("initial_registry dependency_impacts: %w", err)
	}
	if err := validateInstallRuleLimits(rules, impacts, limits); err != nil {
		return RegistryState{}, nil, err
	}
	generation.ScopeRules = rules
	generation.DependencyImpacts = impacts
	relations := make(map[RelationID]RelationState, len(definitions))
	for id, definition := range definitions {
		relations[id] = RelationState{Definition: definition}
	}
	for _, dependency := range dependencies {
		state := relations[dependency.Relation]
		state.CaptureDependencies = append(state.CaptureDependencies, dependency.ID)
		relations[dependency.Relation] = state
	}
	for _, rule := range rules {
		state := relations[rule.Relation]
		state.ScopeRules = append(state.ScopeRules, rule.ID)
		relations[rule.Relation] = state
	}
	for _, impact := range impacts {
		state := relations[impact.Relation]
		state.DependencyImpacts = append(state.DependencyImpacts, impact.ID)
		relations[impact.Relation] = state
	}
	_ = schema
	return RegistryState{CurrentGeneration: generation.Generation, Generations: []RegistryGenerationState{generation}}, relations, nil
}

func decodeInstallRelation(payload installRelationPayload, manifest SchemaManifest, limits ConfiguredLimits) (RelationDefinition, error) {
	if payload.Relation == nil || *payload.Relation == "" || payload.RegistrationKind == nil || payload.Physical == nil || payload.CaptureKeyFieldIDs == nil ||
		payload.CapturedFieldIDs == nil || len(*payload.CapturedFieldIDs) == 0 || payload.PositiveFanoutBound == nil || *payload.PositiveFanoutBound == 0 ||
		*payload.PositiveFanoutBound > limits.MaxScopeFanout || payload.DependencyCapturedFieldIDs == nil || !payload.TableID.Set || !payload.PrimaryKeyFieldID.Set ||
		!payload.PrimaryKeyPhysicalColumn.Set || !payload.PrimaryKeyPortableType.Set || !payload.MembershipFunction.Set || !payload.DependencyImpactFunction.Set || !payload.PositiveDependencyRowBound.Set {
		return RelationDefinition{}, errors.New("relation has an incomplete closed shape")
	}
	physical, err := decodeInstallPhysical(payload.Physical)
	if err != nil {
		return RelationDefinition{}, err
	}
	captured, err := uniqueFieldIDs(*payload.CapturedFieldIDs, true)
	if err != nil {
		return RelationDefinition{}, fmt.Errorf("captured_field_ids: %w", err)
	}
	captureKeys, err := uniqueFieldIDs(*payload.CaptureKeyFieldIDs, false)
	if err != nil {
		return RelationDefinition{}, fmt.Errorf("capture_key_field_ids: %w", err)
	}
	dependencyFields, err := uniqueFieldIDs(*payload.DependencyCapturedFieldIDs, false)
	if err != nil {
		return RelationDefinition{}, fmt.Errorf("dependency_captured_field_ids: %w", err)
	}
	definition := RelationDefinition{Relation: RelationID(*payload.Relation), RegistrationKind: *payload.RegistrationKind, Physical: physical,
		CaptureKeyFieldIDs: captureKeys, CapturedFieldIDs: captured, PositiveFanoutBound: *payload.PositiveFanoutBound, DependencyCapturedFieldIDs: dependencyFields}
	switch *payload.RegistrationKind {
	case RegistrationKindSynced:
		if !payload.TableID.Valid || !payload.PrimaryKeyFieldID.Valid || !payload.PrimaryKeyPhysicalColumn.Valid || !payload.PrimaryKeyPortableType.Valid || !payload.MembershipFunction.Valid || len(captureKeys) != 0 {
			return RelationDefinition{}, errors.New("synced relation has invalid table or primary-key bindings")
		}
		definition.HasTableID = true
		definition.TableID = TableID(payload.TableID.Value)
		definition.PrimaryKeyFieldID = FieldID(payload.PrimaryKeyFieldID.Value)
		definition.PrimaryKeyPhysicalColumn = payload.PrimaryKeyPhysicalColumn.Value
		definition.PrimaryKeyPortableType = payload.PrimaryKeyPortableType.Value
		definition.MembershipFunction = FunctionID(payload.MembershipFunction.Value)
		if err := validateInstallSyncedRelation(definition, manifest); err != nil {
			return RelationDefinition{}, err
		}
	case RegistrationKindCaptureDependency:
		if payload.TableID.Valid || payload.PrimaryKeyFieldID.Valid || payload.PrimaryKeyPhysicalColumn.Valid || payload.PrimaryKeyPortableType.Valid || payload.MembershipFunction.Valid || len(captureKeys) == 0 {
			return RelationDefinition{}, errors.New("capture_dependency relation has invalid null or capture-key bindings")
		}
	default:
		return RelationDefinition{}, errors.New("relation has unknown registration_kind")
	}
	if payload.DependencyImpactFunction.Valid != payload.PositiveDependencyRowBound.Valid {
		return RelationDefinition{}, errors.New("dependency impact function and bound must both be null or non-null")
	}
	if payload.DependencyImpactFunction.Valid {
		if payload.DependencyImpactFunction.Value == "" || payload.PositiveDependencyRowBound.Value == 0 || payload.PositiveDependencyRowBound.Value > limits.MaxImpactRows || len(dependencyFields) == 0 {
			return RelationDefinition{}, errors.New("dependency impact binding is invalid")
		}
		definition.DependencyImpactFunction = FunctionID(payload.DependencyImpactFunction.Value)
		definition.PositiveDependencyRowBound = payload.PositiveDependencyRowBound.Value
	} else if len(dependencyFields) != 0 {
		return RelationDefinition{}, errors.New("null dependency impact requires empty fields")
	}
	return definition, validateRelationDefinition(definition)
}

func decodeInstallPhysical(payload *installPhysicalPayload) (PhysicalRelation, error) {
	if payload.Schema == nil || *payload.Schema == "" || payload.Name == nil || *payload.Name == "" || payload.OID == nil || *payload.OID == 0 || payload.ReplicaIdentity == nil {
		return PhysicalRelation{}, errors.New("physical relation is incomplete")
	}
	switch *payload.ReplicaIdentity {
	case ReplicaIdentityDefault, ReplicaIdentityNothing, ReplicaIdentityFull, ReplicaIdentityIndex:
	default:
		return PhysicalRelation{}, errors.New("physical relation has unknown replica_identity")
	}
	return PhysicalRelation{Schema: *payload.Schema, Name: *payload.Name, OID: *payload.OID, ReplicaIdentity: *payload.ReplicaIdentity}, nil
}

func validateInstallSyncedRelation(definition RelationDefinition, manifest SchemaManifest) error {
	for _, table := range manifest.Tables {
		if table.ID != definition.TableID {
			continue
		}
		if table.Relation != definition.Relation || table.PrimaryKeyFieldID != definition.PrimaryKeyFieldID {
			return errors.New("synced relation does not agree with its schema table")
		}
		for _, field := range table.Fields {
			if field.ID == definition.PrimaryKeyFieldID && field.PrimaryKey && field.PortableType == definition.PrimaryKeyPortableType {
				return nil
			}
		}
		return errors.New("synced relation primary key does not agree with its schema field")
	}
	return errors.New("synced relation table is absent from initial_schema")
}

func uniqueFieldIDs(values []string, requireNonempty bool) ([]FieldID, error) {
	if requireNonempty && len(values) == 0 {
		return nil, errors.New("field list is empty")
	}
	result := make([]FieldID, 0, len(values))
	seen := make(map[FieldID]struct{})
	for _, value := range values {
		id := FieldID(value)
		if id == "" {
			return nil, errors.New("field ID is empty")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, errors.New("field ID is duplicated")
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}

func decodeInstallCaptureDependencies(payloads []installCaptureDependencyPayload, definitions map[RelationID]RelationDefinition) ([]CaptureDependency, error) {
	result := make([]CaptureDependency, 0, len(payloads))
	seen := make(map[CaptureDependencyID]struct{})
	edges := make(map[RelationID]RelationID)
	for _, payload := range payloads {
		if payload.ID == nil || *payload.ID == "" || payload.Relation == nil || payload.DependsOn == nil {
			return nil, errors.New("capture_dependency is incomplete")
		}
		id := CaptureDependencyID(*payload.ID)
		relation := RelationID(*payload.Relation)
		dependsOn := RelationID(*payload.DependsOn)
		if _, duplicate := seen[id]; duplicate || relation == dependsOn {
			return nil, errors.New("capture_dependency is duplicated or self-referential")
		}
		definition, relationFound := definitions[relation]
		dependency, dependencyFound := definitions[dependsOn]
		if !relationFound || definition.RegistrationKind != RegistrationKindCaptureDependency || !dependencyFound || dependency.RegistrationKind != RegistrationKindSynced {
			return nil, errors.New("capture_dependency relation kinds are invalid")
		}
		if existing, duplicate := edges[relation]; duplicate && existing == dependsOn {
			return nil, errors.New("capture_dependency edge is duplicated")
		}
		edges[relation] = dependsOn
		seen[id] = struct{}{}
		result = append(result, CaptureDependency{ID: id, Relation: relation, DependsOn: dependsOn})
	}
	return result, nil
}

func validateInstallRuleLimits(rules []ScopeRule, impacts []DependencyImpact, limits ConfiguredLimits) error {
	seenRules := make(map[ScopeRuleID]struct{})
	for _, rule := range rules {
		if _, duplicate := seenRules[rule.ID]; duplicate || rule.PositiveFanoutBound > limits.MaxScopeFanout {
			return errors.New("initial_registry scope rule is duplicated or exceeds configured limits")
		}
		seenRules[rule.ID] = struct{}{}
	}
	seenImpacts := make(map[DependencyImpactID]struct{})
	for _, impact := range impacts {
		if _, duplicate := seenImpacts[impact.ID]; duplicate || impact.PositiveRowBound > limits.MaxImpactRows {
			return errors.New("initial_registry dependency impact is duplicated or exceeds configured limits")
		}
		seenImpacts[impact.ID] = struct{}{}
	}
	return nil
}

func decodeInstallScopes(payloads *[]installScopePayload, schema SchemaRef, stream StreamGeneration) (map[ScopeID]ScopeState, error) {
	if payloads == nil {
		return nil, errors.New("empty_scopes is required")
	}
	result := make(map[ScopeID]ScopeState, len(*payloads))
	for _, payload := range *payloads {
		if payload.ScopeID == nil || *payload.ScopeID == "" || payload.MembershipGeneration == nil || *payload.MembershipGeneration == 0 || payload.RetentionGeneration == nil || *payload.RetentionGeneration == 0 {
			return nil, errors.New("empty scope is incomplete")
		}
		scope := ScopeID(*payload.ScopeID)
		if _, duplicate := result[scope]; duplicate {
			return nil, errors.New("empty scope is duplicated")
		}
		checksum, err := vectors.ScopeDigest(schema.Hash, string(scope), nil)
		if err != nil {
			return nil, fmt.Errorf("derive empty scope checksum: %w", err)
		}
		result[scope] = ScopeState{Schema: schema, MembershipGeneration: Generation(*payload.MembershipGeneration), RetentionGeneration: Generation(*payload.RetentionGeneration), StreamGeneration: stream, Checksum: checksum, HighWatermark: StreamPosition{StreamGeneration: stream, Kind: PositionKindGenerationStart}}
	}
	return result, nil
}

func decodeInstallClients(payloads *[]installClientPayload, scopes map[ScopeID]ScopeState, schema SchemaRef, now time.Time) (map[ClientKey]ClientState, map[ClientKey]ClientLocalState, error) {
	if payloads == nil {
		return nil, nil, errors.New("clients is required")
	}
	clients := make(map[ClientKey]ClientState, len(*payloads))
	locals := make(map[ClientKey]ClientLocalState, len(*payloads))
	createdAt := now.Round(0).UTC()
	for _, payload := range *payloads {
		key, err := decodeClientKey(payload.UserID, payload.ClientID)
		if err != nil {
			return nil, nil, err
		}
		if _, duplicate := clients[key]; duplicate || payload.ClientGeneration == nil || *payload.ClientGeneration == 0 || payload.ScopeSetVersion == nil || *payload.ScopeSetVersion == 0 || payload.AcceptedWriteEpoch == nil || payload.AssignedScopeIDs == nil || payload.LocalLifecycle == nil || *payload.LocalLifecycle != ClientLifecycleLocalReady {
			return nil, nil, errors.New("client is incomplete or duplicated")
		}
		localSchema, fresh, err := decodeSchemaReference(payload.LocalSchema, false)
		if err != nil || fresh || localSchema != schema {
			return nil, nil, errors.New("client local_schema does not equal initial_schema")
		}
		generation := ClientGenerationState{Generation: Generation(*payload.ClientGeneration), CreatedAt: &createdAt}
		if payload.LastCursorAcknowledgedAt != nil {
			parsed, err := time.Parse(time.RFC3339Nano, *payload.LastCursorAcknowledgedAt)
			if err != nil {
				return nil, nil, errors.New("client last_cursor_acknowledged_at is invalid")
			}
			parsed = parsed.Round(0).UTC()
			generation.LastCursorAcknowledgedAt = &parsed
		}
		serverAssignments := make([]ScopeAssignment, 0, len(*payload.AssignedScopeIDs))
		localAssignments := make([]LocalScopeAssignment, 0, len(*payload.AssignedScopeIDs))
		seenScopes := make(map[ScopeID]struct{})
		for _, text := range *payload.AssignedScopeIDs {
			scopeID := ScopeID(text)
			scope, found := scopes[scopeID]
			if !found {
				return nil, nil, errors.New("client assignment references unknown scope")
			}
			if _, duplicate := seenScopes[scopeID]; duplicate {
				return nil, nil, errors.New("client assignment is duplicated")
			}
			seenScopes[scopeID] = struct{}{}
			serverAssignments = append(serverAssignments, ScopeAssignment{Scope: scopeID, MembershipGeneration: scope.MembershipGeneration, RetentionGeneration: scope.RetentionGeneration, Assigned: true})
			localAssignments = append(localAssignments, LocalScopeAssignment{Scope: scopeID, MembershipGeneration: scope.MembershipGeneration, RetentionGeneration: scope.RetentionGeneration, Assigned: true})
		}
		sort.Slice(serverAssignments, func(i, j int) bool { return serverAssignments[i].Scope < serverAssignments[j].Scope })
		sort.Slice(localAssignments, func(i, j int) bool { return localAssignments[i].Scope < localAssignments[j].Scope })
		clients[key] = ClientState{CurrentGeneration: generation.Generation, Generations: []ClientGenerationState{generation}, ScopeSetVersion: ScopeSetVersion(*payload.ScopeSetVersion), ScopeAssignments: serverAssignments, AcceptedWriteEpoch: AcceptedWriteEpoch(*payload.AcceptedWriteEpoch)}
		locals[key] = ClientLocalState{ClientGeneration: generation.Generation, CurrentSchema: schema, AuthoritativeScopeSetVersion: ScopeSetVersion(*payload.ScopeSetVersion), ScopeAssignments: localAssignments, Lifecycle: ClientLifecycleState{State: *payload.LocalLifecycle, ChangedAt: &createdAt}}
	}
	return clients, locals, nil
}

func decodeInstallWritePolicies(payloads *[]installWritePolicyPayload, manifest SchemaManifest) ([]WritePolicyDecision, error) {
	if payloads == nil {
		return nil, errors.New("write_policies is required")
	}
	tables := make(map[TableID]struct{}, len(manifest.Tables))
	for _, table := range manifest.Tables {
		tables[table.ID] = struct{}{}
	}
	result := make([]WritePolicyDecision, 0, len(*payloads))
	seen := make(map[string]struct{})
	for _, payload := range *payloads {
		if payload.UserID == nil || *payload.UserID == "" || payload.TableID == nil || *payload.TableID == "" || payload.Allowed == nil {
			return nil, errors.New("write policy is incomplete")
		}
		table := TableID(*payload.TableID)
		if _, found := tables[table]; !found {
			return nil, errors.New("write policy references unknown table")
		}
		key := *payload.UserID + "\x00" + *payload.TableID
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("write policy is duplicated")
		}
		seen[key] = struct{}{}
		result = append(result, WritePolicyDecision{User: UserID(*payload.UserID), Table: table, Allowed: *payload.Allowed})
	}
	return result, nil
}

func sortedRelationDefinitionIDs(definitions map[RelationID]RelationDefinition) []RelationID {
	ids := make([]RelationID, 0, len(definitions))
	for id := range definitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
