package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type membershipActivationBoundaryPayload struct {
	StreamGeneration *StreamGeneration `json:"stream_generation"`
	Kind             *PositionKind     `json:"kind"`
	CommitLSN        *string           `json:"commit_lsn"`
}

type membershipEvaluationPayload struct {
	Row    *walRowIdentityPayload `json:"row"`
	Scopes *[]string              `json:"scopes"`
}

type membershipScopeRulePayload struct {
	ID                  *string                        `json:"scope_rule_id"`
	Relation            *string                        `json:"relation"`
	MembershipFunction  *string                        `json:"membership_function"`
	PositiveFanoutBound *uint64                        `json:"positive_fanout_bound"`
	Evaluations         *[]membershipEvaluationPayload `json:"evaluations"`
}

type membershipDependencyImpactPayload struct {
	ID               *string                  `json:"dependency_impact_id"`
	Relation         *string                  `json:"relation"`
	Function         *string                  `json:"function"`
	CapturedFieldIDs *[]string                `json:"captured_field_ids"`
	PositiveRowBound *uint64                  `json:"positive_row_bound"`
	AffectedRows     *[]walRowIdentityPayload `json:"affected_rows"`
	RequiresRebuild  *bool                    `json:"requires_rebuild"`
}

type stageRegistryMembershipGenerationPayload struct {
	RegistryGeneration   *uint64                              `json:"registry_generation"`
	MembershipGeneration *uint64                              `json:"membership_generation"`
	BatchSize            *uint64                              `json:"batch_size"`
	ActivationBoundary   *membershipActivationBoundaryPayload `json:"activation_boundary"`
	AffectedScopes       *[]string                            `json:"affected_scopes"`
	ScopeRules           *[]membershipScopeRulePayload        `json:"scope_rules"`
	DependencyImpacts    *[]membershipDependencyImpactPayload `json:"dependency_impacts"`
}

type activateRegistryMembershipGenerationPayload struct {
	RegistryGeneration *uint64 `json:"registry_generation"`
}

func init() {
	registerOperation("model/stage-registry-membership-generation", stageRegistryMembershipGeneration)
	registerOperation("model/activate-registry-membership-generation", activateRegistryMembershipGeneration)
}

type membershipInvalidLimitError struct{}

func (membershipInvalidLimitError) Error() string {
	return "invalid administrative limit"
}

func (membershipInvalidLimitError) ErrorCode() string {
	return "invalid_limit"
}

func stageRegistryMembershipGeneration(_ context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload stageRegistryMembershipGenerationPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode stage-registry-membership-generation payload: %w", err)
	}
	if payload.RegistryGeneration == nil || *payload.RegistryGeneration == 0 || *payload.RegistryGeneration > walMaximumSafeInteger {
		return StepResult{}, errors.New("registry_generation is required and must be a positive portable integer")
	}
	if payload.MembershipGeneration == nil || *payload.MembershipGeneration == 0 || *payload.MembershipGeneration > walMaximumSafeInteger {
		return StepResult{}, errors.New("membership_generation is required and must be a positive portable integer")
	}
	boundary, err := membershipDecodeActivationBoundary(payload.ActivationBoundary, model.state.Stream.Authority.ActiveGeneration)
	if err != nil {
		return StepResult{}, err
	}
	if boundary != model.state.Stream.Authority.GlobalMaterializationBoundary {
		return StepResult{}, errors.New("activation_boundary must equal the complete durable materialization boundary")
	}
	affectedScopes, err := membershipDecodeAffectedScopes(payload.AffectedScopes, model.state)
	if err != nil {
		return StepResult{}, err
	}
	if payload.ScopeRules == nil || payload.DependencyImpacts == nil {
		return StepResult{}, errors.New("scope_rules and dependency_impacts are required")
	}
	batchSize, err := membershipValidateAdministrativeLimits(payload.BatchSize, *payload.ScopeRules, *payload.DependencyImpacts, model.state.ConfiguredLimits)
	if err != nil {
		return StepResult{}, err
	}
	if _, exists := walRegistryGeneration(model.state.Registry, Generation(*payload.RegistryGeneration)); exists {
		return StepResult{}, errors.New("registry_generation is immutable and already exists")
	}
	if Generation(*payload.RegistryGeneration) <= model.state.Registry.CurrentGeneration {
		return StepResult{}, errors.New("registry_generation must increase")
	}

	active, found := walRegistryGeneration(model.state.Registry, model.state.Registry.CurrentGeneration)
	if !found || !active.Validated || active.HasBootstrapStage {
		return StepResult{}, errors.New("current registry generation is not an active validated generation")
	}
	rules, err := membershipDecodeScopeRules(*payload.ScopeRules, active)
	if err != nil {
		return StepResult{}, err
	}
	impacts, err := membershipDecodeDependencyImpacts(*payload.DependencyImpacts, active)
	if err != nil {
		return StepResult{}, err
	}
	candidate := cloneRegistryState(RegistryState{Generations: []RegistryGenerationState{active}}).Generations[0]
	candidate.Generation = Generation(*payload.RegistryGeneration)
	candidate.ActivationBoundary = boundary
	candidate.Validated = true
	candidate.ScopeRules = rules
	candidate.DependencyImpacts = impacts
	candidate.HasBootstrapStage = true
	candidate.BootstrapStage = CandidateProjectionStage{
		RegistryGeneration: candidate.Generation,
		Schema:             model.state.CurrentSchema,
		StreamGeneration:   model.state.Stream.Authority.ActiveGeneration,
		SnapshotBoundary:   boundary,
		ActivationBarrier:  boundary,
		Verified:           true,
		Rows:               []CandidateRowEntry{},
		Projections:        []CandidateProjectionEntry{},
		Fences:             []CandidateFenceEntry{},
		Scopes:             []CandidateScopeEntry{},
	}

	affectedSet := make(map[ScopeID]struct{}, len(affectedScopes))
	for _, scope := range affectedScopes {
		affectedSet[scope] = struct{}{}
		current := model.state.Scopes[scope]
		if Generation(*payload.MembershipGeneration) <= current.MembershipGeneration {
			return StepResult{}, fmt.Errorf("membership_generation does not increase for affected scope %q", scope)
		}
	}
	allCandidateMembership, batchCount, err := membershipCandidateSets(model.state, candidate, batchSize)
	if err != nil {
		return StepResult{}, err
	}
	if err := membershipValidateAffectedScopeIsolation(model.state, allCandidateMembership, affectedSet); err != nil {
		return StepResult{}, err
	}
	for _, scopeID := range affectedScopes {
		scope := model.state.Scopes[scopeID]
		scope.Schema = model.state.CurrentSchema
		scope.MembershipGeneration = Generation(*payload.MembershipGeneration)
		scope.StreamGeneration = model.state.Stream.Authority.ActiveGeneration
		scope.Membership = membershipEntries(allCandidateMembership[scopeID], scope.MembershipGeneration)
		scope.Effects = []ScopeEffect{}
		scope.HighWatermark = boundary
		if err := walRecomputeScopeState(model.state, scopeID, &scope); err != nil {
			return StepResult{}, fmt.Errorf("stage affected scope %q: %w", scopeID, err)
		}
		candidate.BootstrapStage.Scopes = append(candidate.BootstrapStage.Scopes, CandidateScopeEntry{Scope: scopeID, State: scope})
	}
	model.state.Registry.Generations = append(model.state.Registry.Generations, candidate)

	return StepResult{
		Kind: StepResultKindSchema,
		Schema: &SchemaObservation{
			Source:         model.state.CurrentSchema,
			Target:         model.state.CurrentSchema,
			Action:         SchemaActionNone,
			Reason:         "membership_generation_staged",
			AffectedScopes: cloneScopeIDs(affectedScopes),
			BatchSize:      batchSize,
			BatchCount:     batchCount,
		},
	}, nil
}

func activateRegistryMembershipGeneration(_ context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload activateRegistryMembershipGenerationPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode activate-registry-membership-generation payload: %w", err)
	}
	if payload.RegistryGeneration == nil || *payload.RegistryGeneration == 0 || *payload.RegistryGeneration > walMaximumSafeInteger {
		return StepResult{}, errors.New("registry_generation is required and must be a positive portable integer")
	}
	generation := Generation(*payload.RegistryGeneration)
	index := membershipRegistryGenerationIndex(model.state.Registry.Generations, generation)
	if index < 0 {
		return StepResult{}, errors.New("staged registry generation does not exist")
	}
	candidate := model.state.Registry.Generations[index]
	if !candidate.Validated || !candidate.HasBootstrapStage || !candidate.BootstrapStage.Verified {
		return StepResult{}, errors.New("registry generation has no complete verified candidate membership state")
	}
	if candidate.BootstrapStage.RegistryGeneration != generation || candidate.BootstrapStage.ActivationBarrier != candidate.ActivationBoundary {
		return StepResult{}, errors.New("candidate membership state has inconsistent generation or activation bindings")
	}
	if lessStreamPosition(model.state.Stream.Authority.GlobalMaterializationBoundary, candidate.ActivationBoundary) {
		return StepResult{}, errors.New("main WAL materialization has not reached the activation boundary")
	}
	if len(candidate.BootstrapStage.Scopes) == 0 {
		return StepResult{}, errors.New("candidate membership state has no affected scopes")
	}

	affectedSet := make(map[ScopeID]struct{}, len(candidate.BootstrapStage.Scopes))
	for _, entry := range candidate.BootstrapStage.Scopes {
		if entry.Scope == "" || entry.State.MembershipGeneration == 0 || entry.State.StreamGeneration != model.state.Stream.Authority.ActiveGeneration {
			return StepResult{}, errors.New("candidate membership scope is incomplete")
		}
		if _, duplicate := affectedSet[entry.Scope]; duplicate {
			return StepResult{}, errors.New("candidate membership state contains a duplicate scope")
		}
		affectedSet[entry.Scope] = struct{}{}
	}
	affectedScopes := walSortedScopeSet(affectedSet)
	for _, entry := range candidate.BootstrapStage.Scopes {
		model.state.Scopes[entry.Scope] = cloneScopeState(entry.State)
	}
	model.state.Registry.CurrentGeneration = generation
	model.state.Registry.Generations[index].HasBootstrapStage = false
	membershipInstallRelations(&model.state, candidate)
	membershipInvalidateAffectedClients(&model.state, affectedSet)
	membershipInvalidateAffectedRebuilds(&model.state, affectedSet)

	return StepResult{
		Kind: StepResultKindSchema,
		Schema: &SchemaObservation{
			Source:         model.state.CurrentSchema,
			Target:         model.state.CurrentSchema,
			Action:         SchemaActionRebuildLocal,
			Reason:         "membership_generation_activated",
			AffectedScopes: cloneScopeIDs(affectedScopes),
		},
	}, nil
}

func membershipDecodeActivationBoundary(payload *membershipActivationBoundaryPayload, active StreamGeneration) (StreamPosition, error) {
	if payload == nil || payload.StreamGeneration == nil || payload.Kind == nil || payload.CommitLSN == nil {
		return StreamPosition{}, errors.New("activation_boundary requires stream_generation, kind, and commit_lsn")
	}
	if *payload.StreamGeneration != active || *payload.Kind != PositionKindTransactionEnd {
		return StreamPosition{}, errors.New("activation_boundary must be a transaction end in the active stream generation")
	}
	commitLSN, err := walParseCanonicalUnsigned(*payload.CommitLSN, "activation_boundary.commit_lsn")
	if err != nil {
		return StreamPosition{}, err
	}
	return StreamPosition{StreamGeneration: active, Kind: PositionKindTransactionEnd, CommitLSN: CommitLSN(commitLSN)}, nil
}

func membershipDecodeAffectedScopes(payload *[]string, state State) ([]ScopeID, error) {
	if payload == nil || len(*payload) == 0 {
		return nil, errors.New("affected_scopes is required and must not be empty")
	}
	set := make(map[ScopeID]struct{}, len(*payload))
	for index, encoded := range *payload {
		scope := ScopeID(encoded)
		if scope == "" {
			return nil, fmt.Errorf("affected_scopes[%d] is empty", index)
		}
		if _, exists := state.Scopes[scope]; !exists {
			return nil, fmt.Errorf("affected scope %q is not authoritative", scope)
		}
		if _, duplicate := set[scope]; duplicate {
			return nil, fmt.Errorf("affected scope %q is duplicated", scope)
		}
		set[scope] = struct{}{}
	}
	return walSortedScopeSet(set), nil
}

func membershipValidateAdministrativeLimits(batchSize *uint64, rules []membershipScopeRulePayload, impacts []membershipDependencyImpactPayload, limits ConfiguredLimits) (uint64, error) {
	if batchSize == nil || *batchSize == 0 || *batchSize > limits.BackfillBatchMaximum {
		return 0, membershipInvalidLimitError{}
	}
	for _, rule := range rules {
		if rule.PositiveFanoutBound == nil || *rule.PositiveFanoutBound == 0 || *rule.PositiveFanoutBound > limits.MaxScopeFanout {
			return 0, membershipInvalidLimitError{}
		}
	}
	for _, impact := range impacts {
		if impact.PositiveRowBound == nil || *impact.PositiveRowBound == 0 || *impact.PositiveRowBound > limits.MaxImpactRows {
			return 0, membershipInvalidLimitError{}
		}
	}
	return *batchSize, nil
}

func membershipDecodeScopeRules(payloads []membershipScopeRulePayload, active RegistryGenerationState) ([]ScopeRule, error) {
	rules := make([]ScopeRule, 0, len(payloads))
	seen := make(map[ScopeRuleID]struct{}, len(payloads))
	for index, payload := range payloads {
		if payload.ID == nil || *payload.ID == "" || payload.Relation == nil || *payload.Relation == "" || payload.MembershipFunction == nil || *payload.MembershipFunction == "" || payload.PositiveFanoutBound == nil || *payload.PositiveFanoutBound == 0 || *payload.PositiveFanoutBound > walMaximumSafeInteger || payload.Evaluations == nil {
			return nil, fmt.Errorf("scope_rules[%d] is incomplete or has an invalid bound", index)
		}
		id := ScopeRuleID(*payload.ID)
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("scope_rules[%d] duplicates scope_rule_id", index)
		}
		seen[id] = struct{}{}
		relation := RelationID(*payload.Relation)
		definition, found := walRegistryRelation(active, relation)
		if !found || definition.RegistrationKind != RegistrationKindSynced {
			return nil, fmt.Errorf("scope_rules[%d] relation is not a synced registration", index)
		}
		evaluations := make([]MembershipEvaluation, 0, len(*payload.Evaluations))
		evaluationRows := make(map[RowIdentity]struct{}, len(*payload.Evaluations))
		for evaluationIndex, encoded := range *payload.Evaluations {
			if encoded.Row == nil || encoded.Scopes == nil {
				return nil, fmt.Errorf("scope_rules[%d].evaluations[%d] is incomplete", index, evaluationIndex)
			}
			row, err := walDecodeRowIdentity(*encoded.Row)
			if err != nil || row.TableID != definition.TableID {
				return nil, fmt.Errorf("scope_rules[%d].evaluations[%d] has an invalid row", index, evaluationIndex)
			}
			if _, duplicate := evaluationRows[row]; duplicate {
				return nil, fmt.Errorf("scope_rules[%d] duplicates a row evaluation", index)
			}
			evaluationRows[row] = struct{}{}
			scopeSet := make(map[ScopeID]struct{}, len(*encoded.Scopes))
			for scopeIndex, scopeText := range *encoded.Scopes {
				scope := ScopeID(scopeText)
				if scope == "" {
					return nil, fmt.Errorf("scope_rules[%d].evaluations[%d].scopes[%d] is empty", index, evaluationIndex, scopeIndex)
				}
				scopeSet[scope] = struct{}{}
			}
			if uint64(len(scopeSet)) > *payload.PositiveFanoutBound {
				return nil, fmt.Errorf("scope_rules[%d] evaluation exceeds its fanout bound", index)
			}
			evaluations = append(evaluations, MembershipEvaluation{Row: row, Scopes: walSortedScopeSet(scopeSet)})
		}
		rules = append(rules, ScopeRule{
			ID:                  id,
			Relation:            relation,
			MembershipFunction:  FunctionID(*payload.MembershipFunction),
			PositiveFanoutBound: *payload.PositiveFanoutBound,
			Evaluations:         evaluations,
		})
	}
	return rules, nil
}

func membershipDecodeDependencyImpacts(payloads []membershipDependencyImpactPayload, active RegistryGenerationState) ([]DependencyImpact, error) {
	impacts := make([]DependencyImpact, 0, len(payloads))
	seen := make(map[DependencyImpactID]struct{}, len(payloads))
	for index, payload := range payloads {
		if payload.ID == nil || *payload.ID == "" || payload.Relation == nil || *payload.Relation == "" || payload.Function == nil || *payload.Function == "" || payload.CapturedFieldIDs == nil || payload.PositiveRowBound == nil || *payload.PositiveRowBound == 0 || *payload.PositiveRowBound > walMaximumSafeInteger || payload.AffectedRows == nil || payload.RequiresRebuild == nil {
			return nil, fmt.Errorf("dependency_impacts[%d] is incomplete or has an invalid bound", index)
		}
		id := DependencyImpactID(*payload.ID)
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("dependency_impacts[%d] duplicates dependency_impact_id", index)
		}
		seen[id] = struct{}{}
		relation := RelationID(*payload.Relation)
		if _, found := walRegistryRelation(active, relation); !found {
			return nil, fmt.Errorf("dependency_impacts[%d] relation is not registered", index)
		}
		fieldSet := make(map[FieldID]struct{}, len(*payload.CapturedFieldIDs))
		for fieldIndex, fieldText := range *payload.CapturedFieldIDs {
			field := FieldID(fieldText)
			if field == "" {
				return nil, fmt.Errorf("dependency_impacts[%d].captured_field_ids[%d] is empty", index, fieldIndex)
			}
			if _, duplicate := fieldSet[field]; duplicate {
				return nil, fmt.Errorf("dependency_impacts[%d] duplicates a captured field", index)
			}
			fieldSet[field] = struct{}{}
		}
		fields := make([]FieldID, 0, len(fieldSet))
		for field := range fieldSet {
			fields = append(fields, field)
		}
		sort.Slice(fields, func(left, right int) bool { return fields[left] < fields[right] })
		rowSet := make(map[RowIdentity]struct{}, len(*payload.AffectedRows))
		for rowIndex, encoded := range *payload.AffectedRows {
			row, err := walDecodeRowIdentity(encoded)
			if err != nil || !walGenerationHasTable(active, row.TableID) {
				return nil, fmt.Errorf("dependency_impacts[%d].affected_rows[%d] is invalid", index, rowIndex)
			}
			rowSet[row] = struct{}{}
		}
		if uint64(len(rowSet)) > *payload.PositiveRowBound {
			return nil, fmt.Errorf("dependency_impacts[%d] exceeds its row bound", index)
		}
		rows := make([]RowIdentity, 0, len(rowSet))
		for row := range rowSet {
			rows = append(rows, row)
		}
		sort.Slice(rows, func(left, right int) bool { return lessRowIdentity(rows[left], rows[right]) })
		impacts = append(impacts, DependencyImpact{
			ID:               id,
			Relation:         relation,
			Function:         FunctionID(*payload.Function),
			CapturedFieldIDs: fields,
			PositiveRowBound: *payload.PositiveRowBound,
			AffectedRows:     rows,
			RequiresRebuild:  *payload.RequiresRebuild,
		})
	}
	return impacts, nil
}

func membershipCandidateSets(state State, candidate RegistryGenerationState, batchSize uint64) (map[ScopeID]map[RowIdentity]struct{}, uint64, error) {
	if batchSize == 0 {
		return nil, 0, membershipInvalidLimitError{}
	}
	result := make(map[ScopeID]map[RowIdentity]struct{})
	rows := make([]RowIdentity, 0, len(state.Rows))
	for identity, row := range state.Rows {
		if !row.Deleted {
			rows = append(rows, identity)
		}
	}
	sort.Slice(rows, func(left, right int) bool { return lessRowIdentity(rows[left], rows[right]) })
	var batchCount uint64
	for start := 0; start < len(rows); {
		remaining := uint64(len(rows) - start)
		batchLength := batchSize
		if remaining < batchLength {
			batchLength = remaining
		}
		end := start + int(batchLength)
		for _, row := range rows[start:end] {
			scopes, err := walEvaluateMembership(candidate, row)
			if err != nil {
				return nil, 0, fmt.Errorf("build candidate membership for row: %w", err)
			}
			for _, scope := range scopes {
				if result[scope] == nil {
					result[scope] = make(map[RowIdentity]struct{})
				}
				result[scope][row] = struct{}{}
			}
		}
		batchCount++
		start = end
	}
	return result, batchCount, nil
}

func membershipValidateAffectedScopeIsolation(state State, candidate map[ScopeID]map[RowIdentity]struct{}, affected map[ScopeID]struct{}) error {
	allScopes := make(map[ScopeID]struct{}, len(state.Scopes)+len(candidate))
	for scope := range state.Scopes {
		allScopes[scope] = struct{}{}
	}
	for scope := range candidate {
		allScopes[scope] = struct{}{}
	}
	for scope := range allScopes {
		if _, isAffected := affected[scope]; isAffected {
			continue
		}
		current := make(map[RowIdentity]struct{})
		for _, membership := range state.Scopes[scope].Membership {
			if membership.Included {
				current[membership.Row] = struct{}{}
			}
		}
		if !membershipEqualRowSets(current, candidate[scope]) {
			return fmt.Errorf("affected_scopes omits changed scope %q", scope)
		}
	}
	return nil
}

func membershipEqualRowSets(left, right map[RowIdentity]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for row := range left {
		if _, found := right[row]; !found {
			return false
		}
	}
	return true
}

func membershipEntries(rows map[RowIdentity]struct{}, generation Generation) []ScopeMembership {
	identities := make([]RowIdentity, 0, len(rows))
	for row := range rows {
		identities = append(identities, row)
	}
	sort.Slice(identities, func(left, right int) bool { return lessRowIdentity(identities[left], identities[right]) })
	result := make([]ScopeMembership, 0, len(identities))
	for _, row := range identities {
		result = append(result, ScopeMembership{Row: row, Generation: generation, Included: true})
	}
	return result
}

func membershipRegistryGenerationIndex(generations []RegistryGenerationState, wanted Generation) int {
	for index := range generations {
		if generations[index].Generation == wanted {
			return index
		}
	}
	return -1
}

func membershipInstallRelations(state *State, generation RegistryGenerationState) {
	installed := make(map[RelationID]RelationState, len(generation.Relations))
	for _, registered := range generation.Relations {
		installed[registered.Definition.Relation] = RelationState{Definition: cloneRelationDefinition(registered.Definition)}
	}
	for _, dependency := range generation.CaptureDependencies {
		relation := installed[dependency.Relation]
		relation.CaptureDependencies = append(relation.CaptureDependencies, dependency.ID)
		installed[dependency.Relation] = relation
	}
	for _, rule := range generation.ScopeRules {
		relation := installed[rule.Relation]
		relation.ScopeRules = append(relation.ScopeRules, rule.ID)
		installed[rule.Relation] = relation
	}
	for _, impact := range generation.DependencyImpacts {
		relation := installed[impact.Relation]
		relation.DependencyImpacts = append(relation.DependencyImpacts, impact.ID)
		installed[impact.Relation] = relation
	}
	state.Relations = installed
}

func membershipInvalidateAffectedClients(state *State, affected map[ScopeID]struct{}) {
	for clientKey, client := range state.Clients {
		changed := false
		for index := range client.ScopeAssignments {
			assignment := &client.ScopeAssignments[index]
			if !assignment.Assigned {
				continue
			}
			if _, found := affected[assignment.Scope]; !found {
				continue
			}
			scope := state.Scopes[assignment.Scope]
			assignment.MembershipGeneration = scope.MembershipGeneration
			assignment.RetentionGeneration = scope.RetentionGeneration
			assignment.RebuildRequired = true
			changed = true
		}
		if !changed {
			continue
		}
		retained := make([]ClientCheckpoint, 0, len(client.Checkpoints))
		for _, checkpoint := range client.Checkpoints {
			if _, invalidated := affected[checkpoint.Scope]; !invalidated {
				retained = append(retained, checkpoint)
			}
		}
		client.Checkpoints = retained
		state.Clients[clientKey] = client
	}
}

func membershipInvalidateAffectedRebuilds(state *State, affected map[ScopeID]struct{}) {
	for key, rebuild := range state.Rebuilds {
		if _, invalidated := affected[key.Scope]; !invalidated {
			continue
		}
		rebuild.Status = RebuildStatusInvalidated
		state.Rebuilds[key] = rebuild
	}
}
