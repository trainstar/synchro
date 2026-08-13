package modelrunner

import (
	"encoding/base64"
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

const topologyMaximumSafeInteger = uint64(9007199254740991)

type scopeTopologyRequest struct {
	Profile     string `json:"profile"`
	ScopeFanout uint64 `json:"scope_fanout"`
	ImpactRows  uint64 `json:"impact_rows"`
}

type scopeTopologyRegistry struct {
	stream          reference.StreamGeneration
	generation      reference.Generation
	relation        reference.RelationDefinition
	table           reference.TableManifest
	rule            reference.ScopeRule
	impact          reference.DependencyImpact
	captureRelation reference.RelationDefinition
	manifest        vectors.Manifest
}

type scopeTopologyRow struct {
	identity reference.RowIdentity
	fields   map[reference.FieldID]json.RawMessage
	version  string
	checksum [32]byte
}

// expandScopeTopologyWorkload expands one topology sample into closed model
// operations. It only reads the supplied snapshot.
func expandScopeTopologyWorkload(snapshot reference.StateSnapshot, payload map[string]json.RawMessage) ([]scenarios.Operation, error) {
	request, err := decodeScopeTopologyRequest(payload)
	if err != nil {
		return nil, err
	}
	registry, err := resolveScopeTopologyRegistry(snapshot, request)
	if err != nil {
		return nil, err
	}
	selectedScopes, err := selectScopeTopologyScopes(snapshot, request.ScopeFanout)
	if err != nil {
		return nil, err
	}
	markerCommitLSN, dataCommitLSN, err := scopeTopologyCommitLSNs(snapshot, registry.stream)
	if err != nil {
		return nil, err
	}
	membershipGeneration, err := nextTopologyMembershipGeneration(snapshot, selectedScopes)
	if err != nil {
		return nil, err
	}
	if uint64(registry.generation) >= topologyMaximumSafeInteger {
		return nil, errors.New("scope topology registry generation exceeds the portable integer range")
	}

	sample := uint64(registry.generation)
	rows, err := buildScopeTopologyRows(registry, sample, request.ImpactRows)
	if err != nil {
		return nil, err
	}
	missingRows, err := scopeTopologyMissingRows(snapshot, rows)
	if err != nil {
		return nil, err
	}
	impactRows := make([]reference.RowIdentity, len(rows))
	for index, row := range rows {
		impactRows[index] = row.identity
	}
	evaluations, err := buildScopeTopologyEvaluations(snapshot, registry, impactRows, selectedScopes)
	if err != nil {
		return nil, err
	}
	dataEvents, err := scopeTopologyDataEvents(registry, missingRows, sample)
	if err != nil {
		return nil, err
	}

	markerCommit := strconv.FormatUint(markerCommitLSN, 10)
	dataCommit := strconv.FormatUint(dataCommitLSN, 10)
	return []scenarios.Operation{
		{
			ContractOperation: "model",
			Name:              "commit-source-transaction",
			Payload: mustJSON(map[string]any{
				"stream_generation": registry.stream,
				"commit_lsn":        markerCommit,
				"end_lsn":           strconv.FormatUint(markerCommitLSN+1, 10),
				"events":            []any{},
			}),
		},
		{
			ContractOperation: "process",
			Name:              "materialize-source-transaction",
			Payload: mustJSON(map[string]any{
				"stream_generation": registry.stream,
				"commit_lsn":        markerCommit,
			}),
		},
		{
			ContractOperation: "model",
			Name:              "stage-registry-membership-generation",
			Payload: mustJSON(map[string]any{
				"registry_generation":   uint64(registry.generation) + 1,
				"membership_generation": membershipGeneration,
				"batch_size":            snapshot.ConfiguredLimits.BackfillBatchMaximum,
				"activation_boundary": map[string]any{
					"stream_generation": registry.stream,
					"kind":              "transaction_end",
					"commit_lsn":        markerCommit,
				},
				"affected_scopes": selectedScopeStrings(selectedScopes),
				"scope_rules": []any{map[string]any{
					"scope_rule_id":         registry.rule.ID,
					"relation":              registry.rule.Relation,
					"membership_function":   registry.rule.MembershipFunction,
					"positive_fanout_bound": registry.rule.PositiveFanoutBound,
					"evaluations":           evaluations,
				}},
				"dependency_impacts": []any{map[string]any{
					"dependency_impact_id": registry.impact.ID,
					"relation":             registry.impact.Relation,
					"function":             registry.impact.Function,
					"captured_field_ids":   fieldIDStrings(registry.impact.CapturedFieldIDs),
					"positive_row_bound":   registry.impact.PositiveRowBound,
					"affected_rows":        rowIdentityPayloads(impactRows),
					"requires_rebuild":     registry.impact.RequiresRebuild,
				}},
			}),
		},
		{
			ContractOperation: "model",
			Name:              "activate-registry-membership-generation",
			Payload: mustJSON(map[string]any{
				"registry_generation": uint64(registry.generation) + 1,
			}),
		},
		{
			ContractOperation: "model",
			Name:              "commit-source-transaction",
			Payload: mustJSON(map[string]any{
				"stream_generation": registry.stream,
				"commit_lsn":        dataCommit,
				"end_lsn":           strconv.FormatUint(dataCommitLSN+1, 10),
				"events":            dataEvents,
			}),
		},
		{
			ContractOperation: "process",
			Name:              "materialize-source-transaction",
			Payload: mustJSON(map[string]any{
				"stream_generation": registry.stream,
				"commit_lsn":        dataCommit,
			}),
		},
	}, nil
}

func decodeScopeTopologyRequest(payload map[string]json.RawMessage) (scopeTopologyRequest, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scopeTopologyRequest{}, fmt.Errorf("encode scope topology workload: %w", err)
	}
	var request scopeTopologyRequest
	if err := json.Unmarshal(encoded, &request); err != nil {
		return scopeTopologyRequest{}, fmt.Errorf("decode scope topology workload: %w", err)
	}
	if request.Profile != "scope_topology" {
		return scopeTopologyRequest{}, errors.New("scope topology workload profile is required")
	}
	if request.ScopeFanout == 0 {
		return scopeTopologyRequest{}, errors.New("scope topology scope_fanout must be positive")
	}
	if request.ImpactRows == 0 {
		return scopeTopologyRequest{}, errors.New("scope topology impact_rows must be positive")
	}
	return request, nil
}

func resolveScopeTopologyRegistry(snapshot reference.StateSnapshot, request scopeTopologyRequest) (scopeTopologyRegistry, error) {
	if snapshot.ProtocolVersion != 3 {
		return scopeTopologyRegistry{}, errors.New("scope topology requires protocol version 3")
	}
	if snapshot.Stream.Authority.ActiveGeneration == "" {
		return scopeTopologyRegistry{}, errors.New("scope topology requires an active stream generation")
	}
	if snapshot.Registry.CurrentGeneration == 0 {
		return scopeTopologyRegistry{}, errors.New("scope topology active registry generation is absent")
	}
	if snapshot.CurrentSchema == (reference.SchemaRef{}) {
		return scopeTopologyRegistry{}, errors.New("scope topology current schema is absent")
	}
	schema, found := findSchema(snapshot, snapshot.CurrentSchema)
	if !found {
		return scopeTopologyRegistry{}, errors.New("scope topology current schema data is absent")
	}
	_, manifest, err := installedVectorManifest(snapshot.CurrentSchema, schema.Value)
	if err != nil {
		return scopeTopologyRegistry{}, fmt.Errorf("scope topology schema data: %w", err)
	}
	if manifest.Hash() != snapshot.CurrentSchema.Hash {
		return scopeTopologyRegistry{}, errors.New("scope topology schema hash differs from the installed manifest")
	}

	var active *reference.RegistryGenerationState
	for index := range snapshot.Registry.Generations {
		generation := &snapshot.Registry.Generations[index]
		if generation.Generation == snapshot.Registry.CurrentGeneration {
			active = generation
			break
		}
	}
	if active == nil || !active.Validated || active.HasBootstrapStage {
		return scopeTopologyRegistry{}, errors.New("scope topology active registry generation is absent")
	}

	relations := make(map[reference.RelationID]reference.RelationDefinition, len(active.Relations))
	for _, registered := range active.Relations {
		definition := registered.Definition
		if definition.Relation == "" {
			return scopeTopologyRegistry{}, errors.New("scope topology registry has an empty relation")
		}
		if _, duplicate := relations[definition.Relation]; duplicate {
			return scopeTopologyRegistry{}, errors.New("scope topology registry relation is duplicated")
		}
		relations[definition.Relation] = definition
	}

	var rule *reference.ScopeRule
	for index := range active.ScopeRules {
		candidate := &active.ScopeRules[index]
		definition, exists := relations[candidate.Relation]
		if !exists || definition.RegistrationKind != reference.RegistrationKindSynced || !definition.HasTableID {
			continue
		}
		if rule != nil {
			return scopeTopologyRegistry{}, errors.New("scope topology requires exactly one synced scope rule")
		}
		rule = candidate
	}
	if rule == nil || rule.ID == "" || rule.MembershipFunction == "" || rule.PositiveFanoutBound == 0 {
		return scopeTopologyRegistry{}, errors.New("scope topology synced scope rule is absent or incomplete")
	}
	relation := relations[rule.Relation]
	if request.ScopeFanout > rule.PositiveFanoutBound || request.ScopeFanout > snapshot.ConfiguredLimits.MaxScopeFanout {
		return scopeTopologyRegistry{}, errors.New("scope topology scope_fanout exceeds the authoritative bound")
	}

	var table *reference.TableManifest
	for index := range schema.Value.Tables {
		candidate := &schema.Value.Tables[index]
		if candidate.ID == relation.TableID && candidate.Relation == relation.Relation {
			table = candidate
			break
		}
	}
	if table == nil || table.PrimaryKeyFieldID != relation.PrimaryKeyFieldID {
		return scopeTopologyRegistry{}, errors.New("scope topology schema table data is absent or inconsistent")
	}
	if _, found := topologyTableField(*table, relation.PrimaryKeyFieldID); !found {
		return scopeTopologyRegistry{}, errors.New("scope topology schema primary key field is absent")
	}

	var impact *reference.DependencyImpact
	var captureRelation reference.RelationDefinition
	for index := range active.DependencyImpacts {
		candidate := &active.DependencyImpacts[index]
		definition, exists := relations[candidate.Relation]
		if !exists || definition.RegistrationKind != reference.RegistrationKindCaptureDependency {
			continue
		}
		if impact != nil {
			return scopeTopologyRegistry{}, errors.New("scope topology requires exactly one capture dependency impact")
		}
		impact = candidate
		captureRelation = definition
	}
	if impact == nil || impact.ID == "" || impact.Function == "" || impact.PositiveRowBound == 0 || len(impact.CapturedFieldIDs) == 0 {
		return scopeTopologyRegistry{}, errors.New("scope topology dependency impact is absent or incomplete")
	}
	if request.ImpactRows > impact.PositiveRowBound || request.ImpactRows > snapshot.ConfiguredLimits.MaxImpactRows {
		return scopeTopologyRegistry{}, errors.New("scope topology impact_rows exceeds the authoritative bound")
	}
	if len(captureRelation.CapturedFieldIDs) == 0 || len(captureRelation.CaptureKeyFieldIDs) == 0 {
		return scopeTopologyRegistry{}, errors.New("scope topology capture dependency registration is incomplete")
	}

	return scopeTopologyRegistry{
		stream:          snapshot.Stream.Authority.ActiveGeneration,
		generation:      snapshot.Registry.CurrentGeneration,
		relation:        relation,
		table:           *table,
		rule:            *rule,
		impact:          *impact,
		captureRelation: captureRelation,
		manifest:        manifest,
	}, nil
}

func selectScopeTopologyScopes(snapshot reference.StateSnapshot, fanout uint64) ([]reference.ScopeID, error) {
	scopes := append([]reference.SnapshotEntry[reference.ScopeID, reference.ScopeState](nil), snapshot.Scopes...)
	if uint64(len(scopes)) < fanout {
		return nil, fmt.Errorf("scope topology requires %d authoritative scopes, found %d", fanout, len(scopes))
	}
	sort.Slice(scopes, func(left, right int) bool { return scopes[left].Key < scopes[right].Key })
	selected := make([]reference.ScopeID, 0, fanout)
	selectedSet := make(map[reference.ScopeID]struct{}, fanout)
	for index := uint64(0); index < fanout; index++ {
		candidate := scopes[index]
		if candidate.Key == "" || candidate.Value.Schema != snapshot.CurrentSchema || candidate.Value.StreamGeneration != snapshot.Stream.Authority.ActiveGeneration || candidate.Value.MembershipGeneration == 0 || candidate.Value.RetentionGeneration == 0 {
			return nil, fmt.Errorf("scope topology scope %q is not authoritative", candidate.Key)
		}
		if _, duplicate := selectedSet[candidate.Key]; duplicate {
			return nil, fmt.Errorf("scope topology scope %q is duplicated", candidate.Key)
		}
		selectedSet[candidate.Key] = struct{}{}
		selected = append(selected, candidate.Key)
	}
	sort.Slice(selected, func(left, right int) bool { return selected[left] < selected[right] })
	return selected, nil
}

func scopeTopologyCommitLSNs(snapshot reference.StateSnapshot, stream reference.StreamGeneration) (uint64, uint64, error) {
	maximum := uint64(0)
	for _, transaction := range snapshot.Stream.Transactions {
		if transaction.ReplayKey.StreamGeneration != stream {
			continue
		}
		if transaction.Lifecycle != reference.TransactionLifecycleMaterialized {
			return 0, 0, errors.New("scope topology cannot stage behind a pending source transaction")
		}
		if uint64(transaction.ReplayKey.CommitLSN) > maximum {
			maximum = uint64(transaction.ReplayKey.CommitLSN)
		}
	}
	boundary := snapshot.Stream.Authority.GlobalMaterializationBoundary
	if boundary.StreamGeneration != stream {
		return 0, 0, errors.New("scope topology materialization boundary is outside the active stream")
	}
	if uint64(boundary.CommitLSN) > maximum {
		maximum = uint64(boundary.CommitLSN)
	}
	if maximum > topologyMaximumSafeInteger-20 {
		return 0, 0, errors.New("scope topology commit LSN exceeds the portable integer range")
	}
	return maximum + 10, maximum + 20, nil
}

func nextTopologyMembershipGeneration(snapshot reference.StateSnapshot, scopes []reference.ScopeID) (uint64, error) {
	byID := make(map[reference.ScopeID]reference.ScopeState, len(snapshot.Scopes))
	for _, entry := range snapshot.Scopes {
		byID[entry.Key] = entry.Value
	}
	maximum := uint64(0)
	for _, scopeID := range scopes {
		scope, found := byID[scopeID]
		if !found || scope.MembershipGeneration == 0 {
			return 0, fmt.Errorf("scope topology scope %q is not authoritative", scopeID)
		}
		if uint64(scope.MembershipGeneration) > maximum {
			maximum = uint64(scope.MembershipGeneration)
		}
	}
	if maximum >= topologyMaximumSafeInteger {
		return 0, errors.New("scope topology membership generation exceeds the portable integer range")
	}
	return maximum + 1, nil
}

func buildScopeTopologyRows(registry scopeTopologyRegistry, sample, count uint64) ([]scopeTopologyRow, error) {
	rows := make([]scopeTopologyRow, 0, count)
	for ordinal := uint64(1); ordinal <= count; ordinal++ {
		row, err := buildScopeTopologyRow(registry, sample, ordinal)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func buildScopeTopologyRow(registry scopeTopologyRegistry, sample, ordinal uint64) (scopeTopologyRow, error) {
	primary, found := topologyTableField(registry.table, registry.relation.PrimaryKeyFieldID)
	if !found || primary.PortableType != registry.relation.PrimaryKeyPortableType {
		return scopeTopologyRow{}, errors.New("scope topology primary key schema data is inconsistent")
	}
	if primary.PortableType != "string" && primary.PortableType != "int" && primary.PortableType != "int64" {
		return scopeTopologyRow{}, errors.New("scope topology primary key type is not supported by WAL")
	}

	fields := make(map[reference.FieldID]json.RawMessage, len(registry.table.Fields))
	vectorFields := make([]vectors.RowField, 0, len(registry.table.Fields))
	var primaryValue json.RawMessage
	for _, field := range registry.table.Fields {
		var value json.RawMessage
		var err error
		value, err = scopeTopologyFieldValue(field, ordinal, field.ID == registry.relation.PrimaryKeyFieldID)
		if err != nil {
			return scopeTopologyRow{}, err
		}
		fields[field.ID] = value
		vectorFields = append(vectorFields, vectors.RowField{FieldID: string(field.ID), Value: value})
		if field.ID == registry.relation.PrimaryKeyFieldID {
			primaryValue = value
		}
	}
	if primaryValue == nil {
		return scopeTopologyRow{}, errors.New("scope topology primary key value is absent")
	}
	identity, err := scopeTopologyIdentity(registry, primaryValue)
	if err != nil {
		return scopeTopologyRow{}, err
	}
	version := scopeTopologyVersion(sample)
	checksum, err := vectors.RowDigest(registry.manifest, string(registry.table.ID), vectors.Row{PK: primaryValue, Fields: vectorFields}, version)
	if err != nil {
		return scopeTopologyRow{}, fmt.Errorf("derive scope topology row checksum: %w", err)
	}
	return scopeTopologyRow{identity: identity, fields: fields, version: version, checksum: checksum}, nil
}

func scopeTopologyMissingRows(snapshot reference.StateSnapshot, desired []scopeTopologyRow) ([]scopeTopologyRow, error) {
	existing := make(map[reference.RowIdentity]reference.AuthoritativeRow, len(snapshot.Rows))
	for _, entry := range snapshot.Rows {
		if entry.Key != entry.Value.Identity {
			return nil, errors.New("scope topology authoritative row identity is inconsistent")
		}
		existing[entry.Key] = entry.Value
	}
	missing := make([]scopeTopologyRow, 0, len(desired))
	for _, row := range desired {
		current, found := existing[row.identity]
		if !found {
			missing = append(missing, row)
			continue
		}
		if current.Deleted {
			return nil, errors.New("scope topology deterministic row is deleted")
		}
	}
	return missing, nil
}

func buildScopeTopologyEvaluations(snapshot reference.StateSnapshot, registry scopeTopologyRegistry, nextRows []reference.RowIdentity, selectedScopes []reference.ScopeID) ([]any, error) {
	type evaluation struct {
		row    reference.RowIdentity
		scopes []reference.ScopeID
	}
	currentScopes := make(map[reference.RowIdentity][]reference.ScopeID)
	for _, scope := range snapshot.Scopes {
		for _, membership := range scope.Value.Membership {
			if membership.Included {
				currentScopes[membership.Row] = append(currentScopes[membership.Row], scope.Key)
			}
		}
	}
	nextSet := make(map[reference.RowIdentity]struct{}, len(nextRows))
	for _, row := range nextRows {
		if _, duplicate := nextSet[row]; duplicate {
			return nil, errors.New("scope topology requested row is duplicated")
		}
		nextSet[row] = struct{}{}
	}
	evaluations := make([]evaluation, 0, len(snapshot.Rows)+len(nextRows))
	foundNext := make(map[reference.RowIdentity]struct{}, len(nextRows))
	for _, entry := range snapshot.Rows {
		if entry.Key.TableID != registry.table.ID || entry.Value.Deleted {
			continue
		}
		if entry.Key != entry.Value.Identity {
			return nil, errors.New("scope topology authoritative row identity is inconsistent")
		}
		scopes := uniqueSortedScopeIDs(currentScopes[entry.Key])
		if _, selected := nextSet[entry.Key]; selected {
			scopes = append([]reference.ScopeID(nil), selectedScopes...)
			foundNext[entry.Key] = struct{}{}
		}
		if uint64(len(scopes)) > registry.rule.PositiveFanoutBound {
			return nil, errors.New("scope topology existing membership exceeds the registered fanout bound")
		}
		evaluations = append(evaluations, evaluation{row: entry.Key, scopes: scopes})
	}
	for _, row := range nextRows {
		if _, found := foundNext[row]; found {
			continue
		}
		evaluations = append(evaluations, evaluation{row: row, scopes: append([]reference.ScopeID(nil), selectedScopes...)})
	}
	sort.Slice(evaluations, func(left, right int) bool {
		return lessScopeTopologyRowIdentity(evaluations[left].row, evaluations[right].row)
	})
	encoded := make([]any, 0, len(evaluations))
	for index, evaluation := range evaluations {
		if index > 0 && evaluation.row == evaluations[index-1].row {
			return nil, errors.New("scope topology membership evaluation row is duplicated")
		}
		encoded = append(encoded, map[string]any{
			"row":    rowIdentityPayload(evaluation.row),
			"scopes": selectedScopeStrings(evaluation.scopes),
		})
	}
	return encoded, nil
}

func scopeTopologyDataEvents(registry scopeTopologyRegistry, rows []scopeTopologyRow, sample uint64) ([]any, error) {
	events := make([]any, 0, len(rows)+1)
	for index, row := range rows {
		event, err := scopeTopologySyncedInsert(registry, row, uint64(index+1))
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	events = append(events, scopeTopologyDependencyInsert(registry, sample, uint64(len(rows)+1)))
	return events, nil
}

func scopeTopologySyncedInsert(registry scopeTopologyRegistry, row scopeTopologyRow, eventOrdinal uint64) (map[string]any, error) {
	fields := make([]any, 0, len(registry.relation.CapturedFieldIDs))
	for _, fieldID := range registry.relation.CapturedFieldIDs {
		field, found := topologyTableField(registry.table, fieldID)
		value, valueFound := row.fields[fieldID]
		if !found || !valueFound {
			return nil, fmt.Errorf("scope topology captured field %q is absent from the schema", fieldID)
		}
		fields = append(fields, map[string]any{
			"field":     fieldID,
			"type":      field.PortableType,
			"wire_json": string(value),
		})
	}
	return map[string]any{
		"event_ordinal": eventOrdinal,
		"relation":      registry.relation.Relation,
		"operation":     "insert",
		"before":        nil,
		"after": map[string]any{
			"identity": map[string]any{
				"kind":        "synced",
				"synced_row":  rowIdentityPayload(row.identity),
				"capture_key": nil,
			},
			"fields":   fields,
			"version":  row.version,
			"checksum": hex.EncodeToString(row.checksum[:]),
			"deleted":  false,
		},
	}, nil
}

func scopeTopologyDependencyInsert(registry scopeTopologyRegistry, sample, eventOrdinal uint64) map[string]any {
	fields := make([]any, 0, len(registry.captureRelation.CapturedFieldIDs))
	for _, fieldID := range registry.captureRelation.CapturedFieldIDs {
		fields = append(fields, map[string]any{
			"field":     fieldID,
			"type":      "string",
			"wire_json": strconv.Quote(fmt.Sprintf("scope-topology-impact-key-%06d-%s", sample, fieldID)),
		})
	}
	return map[string]any{
		"event_ordinal": eventOrdinal,
		"relation":      registry.captureRelation.Relation,
		"operation":     "insert",
		"before":        nil,
		"after": map[string]any{
			"identity": map[string]any{
				"kind":       "capture_dependency",
				"synced_row": nil,
				"capture_key": map[string]any{
					"canonical_key_bytes": fmt.Sprintf("scope-topology-impact-key-%06d", sample),
				},
			},
			"fields":   fields,
			"version":  fmt.Sprintf("scope-topology-impact-v%06d", sample),
			"checksum": nil,
			"deleted":  false,
		},
	}
}

func scopeTopologyIdentity(registry scopeTopologyRegistry, primaryValue json.RawMessage) (reference.RowIdentity, error) {
	identityBytes, err := vectors.RowIdentity(registry.manifest, string(registry.table.ID), primaryValue)
	if err != nil {
		return reference.RowIdentity{}, fmt.Errorf("derive scope topology row identity: %w", err)
	}
	return reference.RowIdentity{
		CanonicalIdentityBytes: string(identityBytes),
		TableID:                registry.table.ID,
		PrimaryKeyFieldID:      registry.relation.PrimaryKeyFieldID,
		PortableType:           registry.relation.PrimaryKeyPortableType,
		CanonicalWireJSON:      string(primaryValue),
	}, nil
}

func scopeTopologyFieldValue(field reference.FieldManifest, sample uint64, primary bool) (json.RawMessage, error) {
	if primary {
		switch field.PortableType {
		case "string":
			return json.RawMessage(strconv.Quote(fmt.Sprintf("scope-topology-row-%06d", sample))), nil
		case "int":
			return json.RawMessage(strconv.FormatUint(sample, 10)), nil
		case "int64":
			return json.RawMessage(strconv.Quote(strconv.FormatUint(sample, 10))), nil
		default:
			return nil, errors.New("scope topology primary key type is not supported by WAL")
		}
	}
	value := fmt.Sprintf("scope-topology-%06d-%s", sample, field.ID)
	switch field.PortableType {
	case "string":
		return json.RawMessage(strconv.Quote(value)), nil
	case "int":
		return json.RawMessage(strconv.FormatUint(sample, 10)), nil
	case "int64":
		return json.RawMessage(strconv.Quote(strconv.FormatUint(sample, 10))), nil
	case "decimal":
		return json.RawMessage(strconv.Quote(strconv.FormatUint(sample, 10))), nil
	case "float":
		return json.RawMessage(strconv.FormatUint(sample, 10)), nil
	case "boolean":
		if sample%2 == 0 {
			return json.RawMessage("false"), nil
		}
		return json.RawMessage("true"), nil
	case "datetime":
		return json.RawMessage(`"2024-01-01T00:00:00.000000Z"`), nil
	case "date":
		return json.RawMessage(`"2024-01-01"`), nil
	case "time":
		return json.RawMessage(`"00:00:00.000000"`), nil
	case "json":
		return json.RawMessage(strconv.Quote(fmt.Sprintf(`{"field":%q,"sample":%d}`, field.ID, sample))), nil
	case "bytes":
		return json.RawMessage(strconv.Quote(base64.RawURLEncoding.EncodeToString([]byte(value)))), nil
	default:
		return nil, fmt.Errorf("scope topology field %q has unsupported type %q", field.ID, field.PortableType)
	}
}

func scopeTopologyVersion(sample uint64) string {
	return fmt.Sprintf("scope-topology-v%06d", sample)
}

func topologyTableField(table reference.TableManifest, id reference.FieldID) (reference.FieldManifest, bool) {
	for _, field := range table.Fields {
		if field.ID == id {
			return field, true
		}
	}
	return reference.FieldManifest{}, false
}

func rowIdentityPayload(row reference.RowIdentity) map[string]any {
	return map[string]any{
		"canonical_identity_bytes": row.CanonicalIdentityBytes,
		"table_id":                 row.TableID,
		"primary_key_field_id":     row.PrimaryKeyFieldID,
		"portable_type":            row.PortableType,
		"canonical_wire_json":      row.CanonicalWireJSON,
	}
}

func rowIdentityPayloads(rows []reference.RowIdentity) []any {
	encoded := make([]any, 0, len(rows))
	for _, row := range rows {
		encoded = append(encoded, rowIdentityPayload(row))
	}
	return encoded
}

func selectedScopeStrings(scopes []reference.ScopeID) []string {
	encoded := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		encoded = append(encoded, string(scope))
	}
	return encoded
}

func fieldIDStrings(fields []reference.FieldID) []string {
	encoded := make([]string, 0, len(fields))
	for _, field := range fields {
		encoded = append(encoded, string(field))
	}
	return encoded
}

func uniqueSortedScopeIDs(scopes []reference.ScopeID) []reference.ScopeID {
	set := make(map[reference.ScopeID]struct{}, len(scopes))
	for _, scope := range scopes {
		set[scope] = struct{}{}
	}
	result := make([]reference.ScopeID, 0, len(set))
	for scope := range set {
		result = append(result, scope)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}

func lessScopeTopologyRowIdentity(left, right reference.RowIdentity) bool {
	if left.CanonicalIdentityBytes != right.CanonicalIdentityBytes {
		return left.CanonicalIdentityBytes < right.CanonicalIdentityBytes
	}
	if left.TableID != right.TableID {
		return left.TableID < right.TableID
	}
	if left.PrimaryKeyFieldID != right.PrimaryKeyFieldID {
		return left.PrimaryKeyFieldID < right.PrimaryKeyFieldID
	}
	if left.PortableType != right.PortableType {
		return left.PortableType < right.PortableType
	}
	return left.CanonicalWireJSON < right.CanonicalWireJSON
}
