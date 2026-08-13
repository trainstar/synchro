package reference

import (
	"bytes"
	"sort"
	"time"
)

func cloneState(in State) State {
	out := State{
		ProtocolVersion:  in.ProtocolVersion,
		CurrentSchema:    in.CurrentSchema,
		Registry:         cloneRegistryState(in.Registry),
		Stream:           cloneStreamState(in.Stream),
		Seed:             cloneSeedState(in.Seed),
		Authorization:    cloneAuthorizationState(in.Authorization),
		Installation:     cloneInstallationCapabilities(in.Installation),
		ConfiguredLimits: in.ConfiguredLimits,
		Readiness:        cloneReadinessState(in.Readiness),
		Events:           cloneModelEvents(in.Events),
	}
	out.Schemas = cloneRootMap(in.Schemas, cloneSchemaManifest)
	out.Relations = cloneRootMap(in.Relations, cloneRelationState)
	out.Clients = cloneRootMap(in.Clients, cloneClientState)
	out.Rows = cloneRootMap(in.Rows, cloneAuthoritativeRow)
	out.Scopes = cloneRootMap(in.Scopes, cloneScopeState)
	out.Fences = cloneRootMap(in.Fences, cloneVersionFence)
	out.Projections = cloneRootMap(in.Projections, cloneCapturedProjection)
	out.Batches = cloneRootMap(in.Batches, cloneBatchLedger)
	out.Mutations = cloneRootMap(in.Mutations, cloneMutationLedger)
	out.Rebuilds = cloneRootMap(in.Rebuilds, cloneRebuildSession)
	out.ClientLocal = cloneRootMap(in.ClientLocal, cloneClientLocalState)
	out.RetentionFloors = cloneRootMap(in.RetentionFloors, cloneRetentionFloor)
	initializeRootMaps(&out)
	return out
}

// initializeRootMaps creates writable clone-owned maps for every root map family.
func initializeRootMaps(state *State) {
	if state.Schemas == nil {
		state.Schemas = make(map[SchemaRef]SchemaManifest)
	}
	if state.Relations == nil {
		state.Relations = make(map[RelationID]RelationState)
	}
	if state.Clients == nil {
		state.Clients = make(map[ClientKey]ClientState)
	}
	if state.Rows == nil {
		state.Rows = make(map[RowIdentity]AuthoritativeRow)
	}
	if state.Scopes == nil {
		state.Scopes = make(map[ScopeID]ScopeState)
	}
	if state.Fences == nil {
		state.Fences = make(map[FenceID]VersionFence)
	}
	if state.Projections == nil {
		state.Projections = make(map[ProjectionKey]CapturedProjection)
	}
	if state.Batches == nil {
		state.Batches = make(map[BatchKey]BatchLedger)
	}
	if state.Mutations == nil {
		state.Mutations = make(map[MutationKey]MutationLedger)
	}
	if state.Rebuilds == nil {
		state.Rebuilds = make(map[RebuildKey]RebuildSession)
	}
	if state.ClientLocal == nil {
		state.ClientLocal = make(map[ClientKey]ClientLocalState)
	}
	if state.RetentionFloors == nil {
		state.RetentionFloors = make(map[ScopeID]RetentionFloor)
	}
}

func cloneRootMap[K comparable, V any](source map[K]V, clone func(V) V) map[K]V {
	if source == nil {
		return nil
	}
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = clone(value)
	}
	return result
}

func cloneBytes(source []byte) []byte {
	if source == nil {
		return nil
	}
	result := make([]byte, len(source))
	copy(result, source)
	return result
}

func cloneString(source *string) *string {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func cloneTime(source *time.Time) *time.Time {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func snapshotTime(source *time.Time) *time.Time {
	if source == nil {
		return nil
	}
	result := source.Round(0).UTC()
	return &result
}

func cloneSchemaManifest(source SchemaManifest) SchemaManifest {
	source.Body = cloneBytes(source.Body)
	if source.Parent != nil {
		parent := *source.Parent
		source.Parent = &parent
	}
	source.Tables = cloneTableManifests(source.Tables)
	source.AffectedScopes = cloneScopeIDs(source.AffectedScopes)
	return source
}

func cloneTableManifests(source []TableManifest) []TableManifest {
	if source == nil {
		return nil
	}
	result := make([]TableManifest, len(source))
	for index, value := range source {
		value.CreatedFieldID = cloneFieldID(value.CreatedFieldID)
		value.UpdatedFieldID = cloneFieldID(value.UpdatedFieldID)
		value.DeletedFieldID = cloneFieldID(value.DeletedFieldID)
		value.Fields = cloneFieldManifests(value.Fields)
		value.Indexes = cloneIndexManifests(value.Indexes)
		result[index] = value
	}
	return result
}

func cloneFieldManifests(source []FieldManifest) []FieldManifest {
	if source == nil {
		return nil
	}
	result := make([]FieldManifest, len(source))
	for index, value := range source {
		value.DefaultWireJSON = cloneString(value.DefaultWireJSON)
		result[index] = value
	}
	return result
}

func cloneIndexManifests(source []IndexManifest) []IndexManifest {
	if source == nil {
		return nil
	}
	result := make([]IndexManifest, len(source))
	for index, value := range source {
		value.Fields = cloneFieldIDs(value.Fields)
		result[index] = value
	}
	return result
}

func cloneRegistryState(source RegistryState) RegistryState {
	if source.Generations == nil {
		return source
	}
	result := make([]RegistryGenerationState, len(source.Generations))
	for index, value := range source.Generations {
		value.BootstrapStage = cloneCandidateProjectionStage(value.BootstrapStage)
		value.Relations = cloneRegistryRelations(value.Relations)
		value.CaptureDependencies = cloneCaptureDependencies(value.CaptureDependencies)
		value.ScopeRules = cloneScopeRules(value.ScopeRules)
		value.DependencyImpacts = cloneDependencyImpacts(value.DependencyImpacts)
		result[index] = value
	}
	source.Generations = result
	return source
}

func cloneRegistryRelations(source []RegistryRelation) []RegistryRelation {
	if source == nil {
		return nil
	}
	result := make([]RegistryRelation, len(source))
	for index, value := range source {
		value.Definition = cloneRelationDefinition(value.Definition)
		result[index] = value
	}
	return result
}

func cloneRelationDefinition(source RelationDefinition) RelationDefinition {
	source.CaptureKeyFieldIDs = cloneFieldIDs(source.CaptureKeyFieldIDs)
	source.CapturedFieldIDs = cloneFieldIDs(source.CapturedFieldIDs)
	source.DependencyCapturedFieldIDs = cloneFieldIDs(source.DependencyCapturedFieldIDs)
	return source
}

func cloneCaptureDependencies(source []CaptureDependency) []CaptureDependency {
	if source == nil {
		return nil
	}
	result := make([]CaptureDependency, len(source))
	copy(result, source)
	return result
}

func cloneScopeRules(source []ScopeRule) []ScopeRule {
	if source == nil {
		return nil
	}
	result := make([]ScopeRule, len(source))
	for index, value := range source {
		value.Evaluations = cloneMembershipEvaluations(value.Evaluations)
		result[index] = value
	}
	return result
}

func cloneMembershipEvaluations(source []MembershipEvaluation) []MembershipEvaluation {
	if source == nil {
		return nil
	}
	result := make([]MembershipEvaluation, len(source))
	for index, value := range source {
		value.Scopes = cloneScopeIDs(value.Scopes)
		result[index] = value
	}
	return result
}

func cloneDependencyImpacts(source []DependencyImpact) []DependencyImpact {
	if source == nil {
		return nil
	}
	result := make([]DependencyImpact, len(source))
	for index, value := range source {
		value.CapturedFieldIDs = cloneFieldIDs(value.CapturedFieldIDs)
		value.AffectedRows = cloneRowIdentities(value.AffectedRows)
		result[index] = value
	}
	return result
}

func cloneRelationState(source RelationState) RelationState {
	source.Definition = cloneRelationDefinition(source.Definition)
	source.CaptureDependencies = cloneCaptureDependencyIDs(source.CaptureDependencies)
	source.ScopeRules = cloneScopeRuleIDs(source.ScopeRules)
	source.DependencyImpacts = cloneDependencyImpactIDs(source.DependencyImpacts)
	return source
}

func cloneClientState(source ClientState) ClientState {
	source.Generations = cloneClientGenerationStates(source.Generations)
	if source.Retirement != nil {
		retirement := *source.Retirement
		retirement.RetiredAt = cloneTime(retirement.RetiredAt)
		source.Retirement = &retirement
	}
	source.ScopeAssignments = cloneScopeAssignments(source.ScopeAssignments)
	source.Checkpoints = cloneClientCheckpoints(source.Checkpoints)
	return source
}

func cloneClientGenerationStates(source []ClientGenerationState) []ClientGenerationState {
	if source == nil {
		return nil
	}
	result := make([]ClientGenerationState, len(source))
	for index, value := range source {
		value.CreatedAt = cloneTime(value.CreatedAt)
		value.LastCursorAcknowledgedAt = cloneTime(value.LastCursorAcknowledgedAt)
		value.ExpiresAt = cloneTime(value.ExpiresAt)
		result[index] = value
	}
	return result
}

func cloneScopeAssignments(source []ScopeAssignment) []ScopeAssignment {
	if source == nil {
		return nil
	}
	result := make([]ScopeAssignment, len(source))
	copy(result, source)
	return result
}

func cloneClientCheckpoints(source []ClientCheckpoint) []ClientCheckpoint {
	if source == nil {
		return nil
	}
	result := make([]ClientCheckpoint, len(source))
	copy(result, source)
	return result
}

func cloneAuthoritativeRow(source AuthoritativeRow) AuthoritativeRow {
	source.FieldValues = cloneFieldValues(source.FieldValues)
	source.DeletedAt = cloneTime(source.DeletedAt)
	source.DeleteReason = cloneString(source.DeleteReason)
	source.UpdatedAt = cloneTime(source.UpdatedAt)
	return source
}

func cloneFieldValues(source []FieldValue) []FieldValue {
	if source == nil {
		return nil
	}
	result := make([]FieldValue, len(source))
	copy(result, source)
	return result
}

func cloneScopeState(source ScopeState) ScopeState {
	source.Membership = cloneScopeMemberships(source.Membership)
	source.Effects = cloneScopeEffects(source.Effects)
	return source
}

func cloneScopeMemberships(source []ScopeMembership) []ScopeMembership {
	if source == nil {
		return nil
	}
	result := make([]ScopeMembership, len(source))
	copy(result, source)
	return result
}

func cloneScopeEffects(source []ScopeEffect) []ScopeEffect {
	if source == nil {
		return nil
	}
	result := make([]ScopeEffect, len(source))
	copy(result, source)
	return result
}

func cloneCandidateProjectionStage(source CandidateProjectionStage) CandidateProjectionStage {
	source.Rows = cloneCandidateRowEntries(source.Rows)
	source.Projections = cloneCandidateProjectionEntries(source.Projections)
	source.Fences = cloneCandidateFenceEntries(source.Fences)
	source.Scopes = cloneCandidateScopeEntries(source.Scopes)
	return source
}

func cloneCandidateRowEntries(source []CandidateRowEntry) []CandidateRowEntry {
	if source == nil {
		return nil
	}
	result := make([]CandidateRowEntry, len(source))
	for index, value := range source {
		value.Row = cloneAuthoritativeRow(value.Row)
		result[index] = value
	}
	return result
}

func cloneCandidateProjectionEntries(source []CandidateProjectionEntry) []CandidateProjectionEntry {
	if source == nil {
		return nil
	}
	result := make([]CandidateProjectionEntry, len(source))
	for index, value := range source {
		value.Projection = cloneCapturedProjection(value.Projection)
		result[index] = value
	}
	return result
}

func cloneCandidateFenceEntries(source []CandidateFenceEntry) []CandidateFenceEntry {
	if source == nil {
		return nil
	}
	result := make([]CandidateFenceEntry, len(source))
	for index, value := range source {
		value.Fence = cloneVersionFence(value.Fence)
		result[index] = value
	}
	return result
}

func cloneCandidateScopeEntries(source []CandidateScopeEntry) []CandidateScopeEntry {
	if source == nil {
		return nil
	}
	result := make([]CandidateScopeEntry, len(source))
	for index, value := range source {
		value.State = cloneScopeState(value.State)
		result[index] = value
	}
	return result
}

func cloneStreamState(source StreamState) StreamState {
	if source.Reset != nil {
		reset := *source.Reset
		reset.CandidateStage = cloneCandidateProjectionStage(reset.CandidateStage)
		source.Reset = &reset
	}
	source.SourceRows = cloneSourceRowEntries(source.SourceRows)
	source.Transactions = cloneStreamTransactions(source.Transactions)
	source.TransactionReplays = cloneTransactionReplayRecords(source.TransactionReplays)
	source.EventReplays = cloneEventReplayRecords(source.EventReplays)
	source.Materializations = cloneMaterializationRecords(source.Materializations)
	source.Acknowledgements = cloneSlotAcknowledgements(source.Acknowledgements)
	source.Poison = clonePoisonRecords(source.Poison)
	return source
}

func cloneSourceRowEntries(source []SourceRowEntry) []SourceRowEntry {
	if source == nil {
		return nil
	}
	result := make([]SourceRowEntry, len(source))
	for index, value := range source {
		value.Row = cloneAuthoritativeRow(value.Row)
		result[index] = value
	}
	return result
}

func cloneStreamTransactions(source []StreamTransaction) []StreamTransaction {
	if source == nil {
		return nil
	}
	result := make([]StreamTransaction, len(source))
	for index, value := range source {
		value.CommittedAt = cloneTime(value.CommittedAt)
		value.Events = cloneSourceEvents(value.Events)
		result[index] = value
	}
	return result
}

func cloneSourceEvents(source []SourceEvent) []SourceEvent {
	if source == nil {
		return nil
	}
	result := make([]SourceEvent, len(source))
	for index, value := range source {
		value.Before = cloneSourceImage(value.Before)
		value.After = cloneSourceImage(value.After)
		value.CapturedAt = cloneTime(value.CapturedAt)
		result[index] = value
	}
	return result
}

func cloneSourceImage(source SourceImage) SourceImage {
	source.Fields = cloneFieldValues(source.Fields)
	return source
}

func cloneTransactionReplayRecords(source []TransactionReplayRecord) []TransactionReplayRecord {
	if source == nil {
		return nil
	}
	result := make([]TransactionReplayRecord, len(source))
	copy(result, source)
	return result
}

func cloneEventReplayRecords(source []EventReplayRecord) []EventReplayRecord {
	if source == nil {
		return nil
	}
	result := make([]EventReplayRecord, len(source))
	copy(result, source)
	return result
}

func cloneMaterializationRecords(source []MaterializationRecord) []MaterializationRecord {
	if source == nil {
		return nil
	}
	result := make([]MaterializationRecord, len(source))
	copy(result, source)
	return result
}

func cloneSlotAcknowledgements(source []SlotAcknowledgement) []SlotAcknowledgement {
	if source == nil {
		return nil
	}
	result := make([]SlotAcknowledgement, len(source))
	for index, value := range source {
		value.AcknowledgedAt = cloneTime(value.AcknowledgedAt)
		result[index] = value
	}
	return result
}

func clonePoisonRecords(source []PoisonRecord) []PoisonRecord {
	if source == nil {
		return nil
	}
	result := make([]PoisonRecord, len(source))
	for index, value := range source {
		value.PoisonedAt = cloneTime(value.PoisonedAt)
		result[index] = value
	}
	return result
}

func cloneVersionFence(source VersionFence) VersionFence {
	return source
}

func cloneCapturedProjection(source CapturedProjection) CapturedProjection {
	source.Fields = cloneFieldValues(source.Fields)
	source.CapturedAt = cloneTime(source.CapturedAt)
	return source
}

func cloneBatchLedger(source BatchLedger) BatchLedger {
	source.SealedCanonicalRequest = cloneBytes(source.SealedCanonicalRequest)
	source.SealedCanonicalResponse = cloneBytes(source.SealedCanonicalResponse)
	source.Mutations = cloneMutationIDs(source.Mutations)
	source.Outcomes = cloneMutationOutcomes(source.Outcomes)
	source.ServerTime = cloneTime(source.ServerTime)
	source.CreatedAt = cloneTime(source.CreatedAt)
	source.CompletedAt = cloneTime(source.CompletedAt)
	source.SealedAt = cloneTime(source.SealedAt)
	return source
}

func cloneMutationLedger(source MutationLedger) MutationLedger {
	source.SealedCanonicalRequest = cloneBytes(source.SealedCanonicalRequest)
	source.SealedCanonicalResponse = cloneBytes(source.SealedCanonicalResponse)
	source.Outcome = cloneMutationOutcome(source.Outcome)
	source.ResolvedAt = cloneTime(source.ResolvedAt)
	return source
}

func cloneMutationOutcomes(source []MutationOutcome) []MutationOutcome {
	if source == nil {
		return nil
	}
	result := make([]MutationOutcome, len(source))
	for index, value := range source {
		result[index] = cloneMutationOutcome(value)
	}
	return result
}

func cloneMutationOutcome(source MutationOutcome) MutationOutcome {
	source.Response = cloneBytes(source.Response)
	return source
}

func cloneRebuildSession(source RebuildSession) RebuildSession {
	source.StagedRows = cloneRebuildStagedRows(source.StagedRows)
	source.CreatedAt = cloneTime(source.CreatedAt)
	source.ExpiresAt = cloneTime(source.ExpiresAt)
	source.Pages = cloneRebuildPages(source.Pages)
	return source
}

func cloneRebuildStagedRows(source []RebuildStagedRow) []RebuildStagedRow {
	if source == nil {
		return nil
	}
	result := make([]RebuildStagedRow, len(source))
	for index, value := range source {
		value.Row = cloneAuthoritativeRow(value.Row)
		value.StagedAt = cloneTime(value.StagedAt)
		result[index] = value
	}
	return result
}

func cloneRebuildPages(source []RebuildPage) []RebuildPage {
	if source == nil {
		return nil
	}
	result := make([]RebuildPage, len(source))
	for index, value := range source {
		value.Rows = cloneAuthoritativeRows(value.Rows)
		value.CanonicalResponse = cloneBytes(value.CanonicalResponse)
		result[index] = value
	}
	return result
}

func cloneAuthoritativeRows(source []AuthoritativeRow) []AuthoritativeRow {
	if source == nil {
		return nil
	}
	result := make([]AuthoritativeRow, len(source))
	for index, value := range source {
		result[index] = cloneAuthoritativeRow(value)
	}
	return result
}

func cloneClientLocalState(source ClientLocalState) ClientLocalState {
	source.ScopeAssignments = cloneLocalScopeAssignments(source.ScopeAssignments)
	source.ScopeCheckpoints = cloneLocalScopeCheckpoints(source.ScopeCheckpoints)
	if source.Backoff != nil {
		backoff := *source.Backoff
		backoff.NextEligibleAt = cloneTime(backoff.NextEligibleAt)
		source.Backoff = &backoff
	}
	source.Rows = cloneLocalRows(source.Rows)
	source.LocalOnlyRows = cloneLocalOnlyRows(source.LocalOnlyRows)
	source.Provenance = cloneLocalProvenance(source.Provenance)
	source.SeedReceipts = cloneLocalSeedReceipts(source.SeedReceipts)
	source.RebuildAttempts = cloneLocalRebuildAttempts(source.RebuildAttempts)
	source.SealedBatches = cloneLocalSealedBatches(source.SealedBatches)
	source.DurableQueue = cloneQueuedMutations(source.DurableQueue)
	source.Outcomes = cloneMutationOutcomes(source.Outcomes)
	source.SchemaJournal = cloneSchemaJournal(source.SchemaJournal)
	source.RebuildStaging = cloneLocalRebuildStages(source.RebuildStaging)
	if source.ErrorState != nil {
		errorState := *source.ErrorState
		errorState.At = cloneTime(errorState.At)
		source.ErrorState = &errorState
	}
	source.Lifecycle.ChangedAt = cloneTime(source.Lifecycle.ChangedAt)
	return source
}

func cloneLocalScopeAssignments(source []LocalScopeAssignment) []LocalScopeAssignment {
	if source == nil {
		return nil
	}
	result := make([]LocalScopeAssignment, len(source))
	copy(result, source)
	return result
}

func cloneLocalScopeCheckpoints(source []LocalScopeCheckpoint) []LocalScopeCheckpoint {
	if source == nil {
		return nil
	}
	result := make([]LocalScopeCheckpoint, len(source))
	copy(result, source)
	return result
}

func cloneLocalRows(source []LocalRow) []LocalRow {
	if source == nil {
		return nil
	}
	result := make([]LocalRow, len(source))
	for index, value := range source {
		value.Fields = cloneFieldValues(value.Fields)
		value.UpdatedAt = cloneTime(value.UpdatedAt)
		result[index] = value
	}
	return result
}

func cloneLocalOnlyRows(source []LocalOnlyRow) []LocalOnlyRow {
	if source == nil {
		return nil
	}
	result := make([]LocalOnlyRow, len(source))
	for index, value := range source {
		value.Fields = cloneFieldValues(value.Fields)
		value.UpdatedAt = cloneTime(value.UpdatedAt)
		result[index] = value
	}
	return result
}

func cloneLocalProvenance(source []LocalProvenance) []LocalProvenance {
	if source == nil {
		return nil
	}
	result := make([]LocalProvenance, len(source))
	for index, value := range source {
		value.Scopes = cloneScopeIDs(value.Scopes)
		result[index] = value
	}
	return result
}

func cloneLocalSeedReceipts(source []LocalSeedReceipt) []LocalSeedReceipt {
	if source == nil {
		return nil
	}
	result := make([]LocalSeedReceipt, len(source))
	copy(result, source)
	return result
}

func cloneLocalRebuildAttempts(source []LocalRebuildAttempt) []LocalRebuildAttempt {
	if source == nil {
		return nil
	}
	result := make([]LocalRebuildAttempt, len(source))
	for index, value := range source {
		value.AppliedPages = cloneAppliedRebuildPages(value.AppliedPages)
		result[index] = value
	}
	return result
}

func cloneAppliedRebuildPages(source []AppliedRebuildPage) []AppliedRebuildPage {
	if source == nil {
		return nil
	}
	result := make([]AppliedRebuildPage, len(source))
	for index, value := range source {
		value.AppliedAt = cloneTime(value.AppliedAt)
		result[index] = value
	}
	return result
}

func cloneQueuedMutations(source []QueuedMutation) []QueuedMutation {
	if source == nil {
		return nil
	}
	result := make([]QueuedMutation, len(source))
	for index, value := range source {
		value.AuthoredColumns = cloneFieldValues(value.AuthoredColumns)
		value.Request = cloneBytes(value.Request)
		value.QueuedAt = cloneTime(value.QueuedAt)
		result[index] = value
	}
	return result
}

func cloneLocalSealedBatches(source []LocalSealedBatch) []LocalSealedBatch {
	if source == nil {
		return nil
	}
	result := make([]LocalSealedBatch, len(source))
	for index, value := range source {
		value.Mutations = cloneMutationIDs(value.Mutations)
		value.CanonicalRequest = cloneBytes(value.CanonicalRequest)
		value.CanonicalResponse = cloneBytes(value.CanonicalResponse)
		value.SealedAt = cloneTime(value.SealedAt)
		value.ReconciledAt = cloneTime(value.ReconciledAt)
		result[index] = value
	}
	return result
}

func cloneSchemaJournal(source []SchemaJournalEntry) []SchemaJournalEntry {
	if source == nil {
		return nil
	}
	result := make([]SchemaJournalEntry, len(source))
	for index, value := range source {
		value.VerifiedTargetManifest = cloneSchemaManifest(value.VerifiedTargetManifest)
		value.AffectedScopes = cloneScopeIDs(value.AffectedScopes)
		value.MigrationPlan = cloneMigrationPlanOperations(value.MigrationPlan)
		result[index] = value
	}
	return result
}

func cloneMigrationPlanOperations(source []MigrationPlanOperation) []MigrationPlanOperation {
	if source == nil {
		return nil
	}
	result := make([]MigrationPlanOperation, len(source))
	copy(result, source)
	return result
}

func cloneLocalRebuildStages(source []LocalRebuildStage) []LocalRebuildStage {
	if source == nil {
		return nil
	}
	result := make([]LocalRebuildStage, len(source))
	for index, value := range source {
		value.Row.Fields = cloneFieldValues(value.Row.Fields)
		value.Row.UpdatedAt = cloneTime(value.Row.UpdatedAt)
		result[index] = value
	}
	return result
}

func cloneRetentionFloor(source RetentionFloor) RetentionFloor {
	source.ExpiresAt = cloneTime(source.ExpiresAt)
	return source
}

func cloneSeedState(source SeedState) SeedState {
	source.Exports = cloneSeedExports(source.Exports)
	source.Records = cloneSeedRecords(source.Records)
	return source
}

func cloneSeedExports(source []SeedExport) []SeedExport {
	if source == nil {
		return nil
	}
	result := make([]SeedExport, len(source))
	for index, value := range source {
		value.Scopes = cloneSeedScopeStates(value.Scopes)
		value.Pages = cloneSeedPageStates(value.Pages)
		value.CreatedAt = cloneTime(value.CreatedAt)
		result[index] = value
	}
	return result
}

func cloneSeedScopeStates(source []SeedScopeState) []SeedScopeState {
	if source == nil {
		return nil
	}
	result := make([]SeedScopeState, len(source))
	copy(result, source)
	return result
}

func cloneSeedPageStates(source []SeedPageState) []SeedPageState {
	if source == nil {
		return nil
	}
	result := make([]SeedPageState, len(source))
	copy(result, source)
	return result
}

func cloneSeedRecords(source []SeedRecord) []SeedRecord {
	if source == nil {
		return nil
	}
	result := make([]SeedRecord, len(source))
	for index, value := range source {
		value.Row = cloneAuthoritativeRow(value.Row)
		result[index] = value
	}
	return result
}

func cloneAuthorizationState(source AuthorizationState) AuthorizationState {
	if source.Roles != nil {
		result := make([]RoleCapabilities, len(source.Roles))
		for index, value := range source.Roles {
			value.Capabilities = cloneCapabilities(value.Capabilities)
			result[index] = value
		}
		source.Roles = result
	}
	source.WritePolicies = append([]WritePolicyDecision(nil), source.WritePolicies...)
	return source
}

func cloneInstallationCapabilities(source InstallationCapabilities) InstallationCapabilities {
	source.Endpoints = cloneEndpoints(source.Endpoints)
	if source.Capabilities == nil {
		return source
	}
	result := make([]InstallationCapability, len(source.Capabilities))
	for index, value := range source.Capabilities {
		value.CheckedAt = cloneTime(value.CheckedAt)
		result[index] = value
	}
	source.Capabilities = result
	return source
}

func cloneReadinessState(source ReadinessState) ReadinessState {
	source.Workers = cloneWorkerReadiness(source.Workers)
	source.Slots = cloneSlotReadiness(source.Slots)
	source.Limits = cloneReadinessLimits(source.Limits)
	source.Checks = cloneReadinessChecks(source.Checks)
	source.Reasons = cloneReasonCodes(source.Reasons)
	return source
}

func cloneWorkerReadiness(source []WorkerReadiness) []WorkerReadiness {
	if source == nil {
		return nil
	}
	result := make([]WorkerReadiness, len(source))
	for index, value := range source {
		value.HeartbeatAt = cloneTime(value.HeartbeatAt)
		result[index] = value
	}
	return result
}

func cloneSlotReadiness(source []SlotReadiness) []SlotReadiness {
	if source == nil {
		return nil
	}
	result := make([]SlotReadiness, len(source))
	copy(result, source)
	return result
}

func cloneReadinessLimits(source []ReadinessLimit) []ReadinessLimit {
	if source == nil {
		return nil
	}
	result := make([]ReadinessLimit, len(source))
	copy(result, source)
	return result
}

func cloneReadinessChecks(source []ReadinessCheck) []ReadinessCheck {
	if source == nil {
		return nil
	}
	result := make([]ReadinessCheck, len(source))
	for index, value := range source {
		value.CheckedAt = cloneTime(value.CheckedAt)
		result[index] = value
	}
	return result
}

func cloneModelEvents(source []ModelEvent) []ModelEvent {
	if source == nil {
		return nil
	}
	result := make([]ModelEvent, len(source))
	for index, value := range source {
		value.At = cloneTime(value.At)
		result[index] = value
	}
	return result
}

func cloneFieldIDs(source []FieldID) []FieldID {
	if source == nil {
		return nil
	}
	result := make([]FieldID, len(source))
	copy(result, source)
	return result
}

func cloneFieldID(source *FieldID) *FieldID {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func cloneCapabilities(source []Capability) []Capability {
	if source == nil {
		return nil
	}
	result := make([]Capability, len(source))
	copy(result, source)
	return result
}

func cloneEndpoints(source []Endpoint) []Endpoint {
	if source == nil {
		return nil
	}
	result := make([]Endpoint, len(source))
	copy(result, source)
	return result
}

func cloneScopeIDs(source []ScopeID) []ScopeID {
	if source == nil {
		return nil
	}
	result := make([]ScopeID, len(source))
	copy(result, source)
	return result
}

func cloneRowIdentities(source []RowIdentity) []RowIdentity {
	if source == nil {
		return nil
	}
	result := make([]RowIdentity, len(source))
	copy(result, source)
	return result
}

func cloneCaptureDependencyIDs(source []CaptureDependencyID) []CaptureDependencyID {
	if source == nil {
		return nil
	}
	result := make([]CaptureDependencyID, len(source))
	copy(result, source)
	return result
}

func cloneScopeRuleIDs(source []ScopeRuleID) []ScopeRuleID {
	if source == nil {
		return nil
	}
	result := make([]ScopeRuleID, len(source))
	copy(result, source)
	return result
}

func cloneDependencyImpactIDs(source []DependencyImpactID) []DependencyImpactID {
	if source == nil {
		return nil
	}
	result := make([]DependencyImpactID, len(source))
	copy(result, source)
	return result
}

func cloneMutationIDs(source []MutationID) []MutationID {
	if source == nil {
		return nil
	}
	result := make([]MutationID, len(source))
	copy(result, source)
	return result
}

func cloneReasonCodes(source []ReasonCode) []ReasonCode {
	if source == nil {
		return nil
	}
	result := make([]ReasonCode, len(source))
	copy(result, source)
	return result
}

func snapshotState(state State) StateSnapshot {
	cloned := cloneState(state)
	snapshot := StateSnapshot{
		ProtocolVersion:  cloned.ProtocolVersion,
		CurrentSchema:    cloned.CurrentSchema,
		Registry:         cloned.Registry,
		Stream:           cloned.Stream,
		Seed:             cloned.Seed,
		Authorization:    cloned.Authorization,
		Installation:     cloned.Installation,
		ConfiguredLimits: cloned.ConfiguredLimits,
		Readiness:        cloned.Readiness,
		Events:           cloned.Events,
		Schemas:          snapshotEntries(cloned.Schemas, lessSchemaRef),
		Relations:        snapshotEntries(cloned.Relations, lessRelationID),
		Clients:          snapshotEntries(cloned.Clients, lessClientKey),
		Rows:             snapshotEntries(cloned.Rows, lessRowIdentity),
		Scopes:           snapshotEntries(cloned.Scopes, lessScopeID),
		Fences:           snapshotEntries(cloned.Fences, lessFenceID),
		Projections:      snapshotEntries(cloned.Projections, lessProjectionKey),
		Batches:          snapshotEntries(cloned.Batches, lessBatchKey),
		Mutations:        snapshotEntries(cloned.Mutations, lessMutationKey),
		Rebuilds:         snapshotEntries(cloned.Rebuilds, lessRebuildKey),
		ClientLocal:      snapshotEntries(cloned.ClientLocal, lessClientKey),
		RetentionFloors:  snapshotEntries(cloned.RetentionFloors, lessScopeID),
	}
	normalizeSnapshot(&snapshot)
	return snapshot
}

func snapshotEntries[K comparable, V any](source map[K]V, less func(K, K) bool) []SnapshotEntry[K, V] {
	result := make([]SnapshotEntry[K, V], 0, len(source))
	for key, value := range source {
		result = append(result, SnapshotEntry[K, V]{Key: key, Value: value})
	}
	sort.Slice(result, func(left, right int) bool {
		return less(result[left].Key, result[right].Key)
	})
	return result
}

func normalizeSnapshot(snapshot *StateSnapshot) {
	for index := range snapshot.Schemas {
		normalizeSchemaManifest(&snapshot.Schemas[index].Value)
	}
	normalizeRegistryState(&snapshot.Registry)
	for index := range snapshot.Relations {
		normalizeRelationState(&snapshot.Relations[index].Value)
	}
	for index := range snapshot.Clients {
		normalizeClientState(&snapshot.Clients[index].Value)
	}
	for index := range snapshot.Rows {
		normalizeAuthoritativeRow(&snapshot.Rows[index].Value)
	}
	for index := range snapshot.Scopes {
		normalizeScopeState(&snapshot.Scopes[index].Value)
	}
	normalizeStreamState(&snapshot.Stream)
	for index := range snapshot.Projections {
		normalizeCapturedProjection(&snapshot.Projections[index].Value)
	}
	for index := range snapshot.Batches {
		normalizeBatchLedger(&snapshot.Batches[index].Value)
	}
	for index := range snapshot.Mutations {
		normalizeMutationLedger(&snapshot.Mutations[index].Value)
	}
	for index := range snapshot.Rebuilds {
		normalizeRebuildSession(&snapshot.Rebuilds[index].Value)
	}
	for index := range snapshot.ClientLocal {
		normalizeClientLocalState(&snapshot.ClientLocal[index].Value)
	}
	for index := range snapshot.RetentionFloors {
		normalizeRetentionFloor(&snapshot.RetentionFloors[index].Value)
	}
	normalizeSeedState(&snapshot.Seed)
	normalizeAuthorizationState(&snapshot.Authorization)
	normalizeInstallationCapabilities(&snapshot.Installation)
	normalizeReadinessState(&snapshot.Readiness)
	for index := range snapshot.Events {
		snapshot.Events[index].At = snapshotTime(snapshot.Events[index].At)
	}
}

func normalizeSchemaManifest(manifest *SchemaManifest) {
	sortScopeIDs(manifest.AffectedScopes)
	sort.Slice(manifest.Tables, func(left, right int) bool {
		return manifest.Tables[left].ID < manifest.Tables[right].ID
	})
	for index := range manifest.Tables {
		table := &manifest.Tables[index]
		sort.Slice(table.Fields, func(left, right int) bool {
			return table.Fields[left].ID < table.Fields[right].ID
		})
		sort.Slice(table.Indexes, func(left, right int) bool {
			return table.Indexes[left].ID < table.Indexes[right].ID
		})
	}
}

func normalizeRegistryState(state *RegistryState) {
	sort.Slice(state.Generations, func(left, right int) bool {
		return state.Generations[left].Generation < state.Generations[right].Generation
	})
	for index := range state.Generations {
		generation := &state.Generations[index]
		normalizeCandidateProjectionStage(&generation.BootstrapStage)
		sort.Slice(generation.Relations, func(left, right int) bool {
			return generation.Relations[left].Definition.Relation < generation.Relations[right].Definition.Relation
		})
		for relationIndex := range generation.Relations {
			normalizeRelationDefinition(&generation.Relations[relationIndex].Definition)
		}
		sort.Slice(generation.CaptureDependencies, func(left, right int) bool {
			return generation.CaptureDependencies[left].ID < generation.CaptureDependencies[right].ID
		})
		sort.Slice(generation.ScopeRules, func(left, right int) bool {
			return generation.ScopeRules[left].ID < generation.ScopeRules[right].ID
		})
		for ruleIndex := range generation.ScopeRules {
			evaluations := generation.ScopeRules[ruleIndex].Evaluations
			sort.Slice(evaluations, func(left, right int) bool {
				return lessRowIdentity(evaluations[left].Row, evaluations[right].Row)
			})
			for evaluationIndex := range evaluations {
				sort.Slice(evaluations[evaluationIndex].Scopes, func(left, right int) bool {
					return evaluations[evaluationIndex].Scopes[left] < evaluations[evaluationIndex].Scopes[right]
				})
			}
		}
		sort.Slice(generation.DependencyImpacts, func(left, right int) bool {
			return generation.DependencyImpacts[left].ID < generation.DependencyImpacts[right].ID
		})
		for impactIndex := range generation.DependencyImpacts {
			impact := &generation.DependencyImpacts[impactIndex]
			sort.Slice(impact.CapturedFieldIDs, func(left, right int) bool {
				return impact.CapturedFieldIDs[left] < impact.CapturedFieldIDs[right]
			})
			sort.Slice(impact.AffectedRows, func(left, right int) bool {
				return lessRowIdentity(impact.AffectedRows[left], impact.AffectedRows[right])
			})
		}
	}
}

func normalizeRelationDefinition(definition *RelationDefinition) {
	sort.Slice(definition.CaptureKeyFieldIDs, func(left, right int) bool {
		return definition.CaptureKeyFieldIDs[left] < definition.CaptureKeyFieldIDs[right]
	})
	sort.Slice(definition.CapturedFieldIDs, func(left, right int) bool {
		return definition.CapturedFieldIDs[left] < definition.CapturedFieldIDs[right]
	})
	sort.Slice(definition.DependencyCapturedFieldIDs, func(left, right int) bool {
		return definition.DependencyCapturedFieldIDs[left] < definition.DependencyCapturedFieldIDs[right]
	})
}

func normalizeRelationState(state *RelationState) {
	normalizeRelationDefinition(&state.Definition)
	sort.Slice(state.CaptureDependencies, func(left, right int) bool {
		return state.CaptureDependencies[left] < state.CaptureDependencies[right]
	})
	sort.Slice(state.ScopeRules, func(left, right int) bool {
		return state.ScopeRules[left] < state.ScopeRules[right]
	})
	sort.Slice(state.DependencyImpacts, func(left, right int) bool {
		return state.DependencyImpacts[left] < state.DependencyImpacts[right]
	})
}

func normalizeClientState(state *ClientState) {
	sort.Slice(state.Generations, func(left, right int) bool {
		return state.Generations[left].Generation < state.Generations[right].Generation
	})
	for index := range state.Generations {
		state.Generations[index].CreatedAt = snapshotTime(state.Generations[index].CreatedAt)
		state.Generations[index].LastCursorAcknowledgedAt = snapshotTime(state.Generations[index].LastCursorAcknowledgedAt)
		state.Generations[index].ExpiresAt = snapshotTime(state.Generations[index].ExpiresAt)
	}
	if state.Retirement != nil {
		state.Retirement.RetiredAt = snapshotTime(state.Retirement.RetiredAt)
	}
	sort.Slice(state.ScopeAssignments, func(left, right int) bool {
		return state.ScopeAssignments[left].Scope < state.ScopeAssignments[right].Scope
	})
	sort.Slice(state.Checkpoints, func(left, right int) bool {
		return state.Checkpoints[left].Scope < state.Checkpoints[right].Scope
	})
}

func normalizeAuthoritativeRow(row *AuthoritativeRow) {
	sort.Slice(row.FieldValues, func(left, right int) bool {
		return row.FieldValues[left].Field < row.FieldValues[right].Field
	})
	row.DeletedAt = snapshotTime(row.DeletedAt)
	row.UpdatedAt = snapshotTime(row.UpdatedAt)
}

func normalizeScopeState(state *ScopeState) {
	sort.Slice(state.Membership, func(left, right int) bool {
		return lessRowIdentity(state.Membership[left].Row, state.Membership[right].Row)
	})
	sort.Slice(state.Effects, func(left, right int) bool {
		if state.Effects[left].Position != state.Effects[right].Position {
			return lessStreamPosition(state.Effects[left].Position, state.Effects[right].Position)
		}
		leftRank := effectOperationRank(state.Effects[left].Operation)
		rightRank := effectOperationRank(state.Effects[right].Operation)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if state.Effects[left].Row != state.Effects[right].Row {
			return lessRowIdentity(state.Effects[left].Row, state.Effects[right].Row)
		}
		return lessEventReplayKey(state.Effects[left].SourceEvent, state.Effects[right].SourceEvent)
	})
}

func effectOperationRank(operation EffectOperation) uint8 {
	switch operation {
	case EffectOperationDelete:
		return EffectOperationDeleteRank
	case EffectOperationUpsert:
		return EffectOperationUpsertRank
	default:
		return EffectOperationUpsertRank + 1
	}
}

func normalizeCandidateProjectionStage(stage *CandidateProjectionStage) {
	sort.Slice(stage.Rows, func(left, right int) bool {
		return lessRowIdentity(stage.Rows[left].Identity, stage.Rows[right].Identity)
	})
	for index := range stage.Rows {
		normalizeAuthoritativeRow(&stage.Rows[index].Row)
	}
	sort.Slice(stage.Projections, func(left, right int) bool {
		return lessProjectionKey(stage.Projections[left].Key, stage.Projections[right].Key)
	})
	for index := range stage.Projections {
		normalizeCapturedProjection(&stage.Projections[index].Projection)
	}
	sort.Slice(stage.Fences, func(left, right int) bool {
		return lessFenceID(stage.Fences[left].ID, stage.Fences[right].ID)
	})
	sort.Slice(stage.Scopes, func(left, right int) bool {
		return lessScopeID(stage.Scopes[left].Scope, stage.Scopes[right].Scope)
	})
	for index := range stage.Scopes {
		normalizeScopeState(&stage.Scopes[index].State)
	}
}

func normalizeStreamState(state *StreamState) {
	if state.Reset != nil {
		normalizeCandidateProjectionStage(&state.Reset.CandidateStage)
	}
	sort.Slice(state.SourceRows, func(left, right int) bool {
		return lessRowIdentity(state.SourceRows[left].Identity, state.SourceRows[right].Identity)
	})
	for index := range state.SourceRows {
		normalizeAuthoritativeRow(&state.SourceRows[index].Row)
	}
	for transactionIndex := range state.Transactions {
		transaction := &state.Transactions[transactionIndex]
		transaction.CommittedAt = snapshotTime(transaction.CommittedAt)
		for eventIndex := range transaction.Events {
			normalizeSourceImage(&transaction.Events[eventIndex].Before)
			normalizeSourceImage(&transaction.Events[eventIndex].After)
			transaction.Events[eventIndex].CapturedAt = snapshotTime(transaction.Events[eventIndex].CapturedAt)
		}
	}
	sort.Slice(state.TransactionReplays, func(left, right int) bool {
		return lessTransactionReplayKey(state.TransactionReplays[left].Key, state.TransactionReplays[right].Key)
	})
	sort.Slice(state.EventReplays, func(left, right int) bool {
		return lessEventReplayKey(state.EventReplays[left].Key, state.EventReplays[right].Key)
	})
	sort.Slice(state.Materializations, func(left, right int) bool {
		return lessEventReplayKey(state.Materializations[left].Event, state.Materializations[right].Event)
	})
	sort.Slice(state.Acknowledgements, func(left, right int) bool {
		if state.Acknowledgements[left].StreamGeneration != state.Acknowledgements[right].StreamGeneration {
			return state.Acknowledgements[left].StreamGeneration < state.Acknowledgements[right].StreamGeneration
		}
		return state.Acknowledgements[left].EndLSN < state.Acknowledgements[right].EndLSN
	})
	for index := range state.Acknowledgements {
		state.Acknowledgements[index].AcknowledgedAt = snapshotTime(state.Acknowledgements[index].AcknowledgedAt)
	}
	sort.Slice(state.Poison, func(left, right int) bool {
		if state.Poison[left].Transaction != state.Poison[right].Transaction {
			return lessTransactionReplayKey(state.Poison[left].Transaction, state.Poison[right].Transaction)
		}
		if state.Poison[left].HasRelation != state.Poison[right].HasRelation {
			return !state.Poison[left].HasRelation
		}
		return state.Poison[left].Relation < state.Poison[right].Relation
	})
	for index := range state.Poison {
		state.Poison[index].PoisonedAt = snapshotTime(state.Poison[index].PoisonedAt)
	}
}

func normalizeSourceImage(image *SourceImage) {
	sort.Slice(image.Fields, func(left, right int) bool {
		return image.Fields[left].Field < image.Fields[right].Field
	})
}

func normalizeCapturedProjection(projection *CapturedProjection) {
	sort.Slice(projection.Fields, func(left, right int) bool {
		return projection.Fields[left].Field < projection.Fields[right].Field
	})
	projection.CapturedAt = snapshotTime(projection.CapturedAt)
}

func normalizeBatchLedger(ledger *BatchLedger) {
	ledger.ServerTime = snapshotTime(ledger.ServerTime)
	ledger.CreatedAt = snapshotTime(ledger.CreatedAt)
	ledger.CompletedAt = snapshotTime(ledger.CompletedAt)
	ledger.SealedAt = snapshotTime(ledger.SealedAt)
}

func normalizeMutationLedger(ledger *MutationLedger) {
	ledger.ResolvedAt = snapshotTime(ledger.ResolvedAt)
}

func normalizeRebuildSession(session *RebuildSession) {
	session.CreatedAt = snapshotTime(session.CreatedAt)
	session.ExpiresAt = snapshotTime(session.ExpiresAt)
	for index := range session.StagedRows {
		session.StagedRows[index].StagedAt = snapshotTime(session.StagedRows[index].StagedAt)
		normalizeAuthoritativeRow(&session.StagedRows[index].Row)
	}
	for pageIndex := range session.Pages {
		for rowIndex := range session.Pages[pageIndex].Rows {
			normalizeAuthoritativeRow(&session.Pages[pageIndex].Rows[rowIndex])
		}
	}
}

func normalizeClientLocalState(state *ClientLocalState) {
	sort.Slice(state.ScopeAssignments, func(left, right int) bool {
		return state.ScopeAssignments[left].Scope < state.ScopeAssignments[right].Scope
	})
	sort.Slice(state.ScopeCheckpoints, func(left, right int) bool {
		return state.ScopeCheckpoints[left].Scope < state.ScopeCheckpoints[right].Scope
	})
	if state.Backoff != nil {
		state.Backoff.NextEligibleAt = snapshotTime(state.Backoff.NextEligibleAt)
	}
	sort.Slice(state.Rows, func(left, right int) bool {
		return lessRowIdentity(state.Rows[left].Identity, state.Rows[right].Identity)
	})
	for index := range state.Rows {
		normalizeLocalRow(&state.Rows[index])
	}
	sort.Slice(state.LocalOnlyRows, func(left, right int) bool {
		return lessLocalOnlyRowKey(state.LocalOnlyRows[left].Key, state.LocalOnlyRows[right].Key)
	})
	for index := range state.LocalOnlyRows {
		normalizeLocalOnlyRow(&state.LocalOnlyRows[index])
	}
	sort.Slice(state.Provenance, func(left, right int) bool {
		return lessRowIdentity(state.Provenance[left].Row, state.Provenance[right].Row)
	})
	for index := range state.Provenance {
		sort.Slice(state.Provenance[index].Scopes, func(left, right int) bool {
			return state.Provenance[index].Scopes[left] < state.Provenance[index].Scopes[right]
		})
	}
	sort.Slice(state.SeedReceipts, func(left, right int) bool {
		return state.SeedReceipts[left].Scope < state.SeedReceipts[right].Scope
	})
	sort.Slice(state.RebuildAttempts, func(left, right int) bool {
		if state.RebuildAttempts[left].Scope != state.RebuildAttempts[right].Scope {
			return state.RebuildAttempts[left].Scope < state.RebuildAttempts[right].Scope
		}
		return state.RebuildAttempts[left].Rebuild < state.RebuildAttempts[right].Rebuild
	})
	for index := range state.RebuildAttempts {
		for pageIndex := range state.RebuildAttempts[index].AppliedPages {
			state.RebuildAttempts[index].AppliedPages[pageIndex].AppliedAt = snapshotTime(state.RebuildAttempts[index].AppliedPages[pageIndex].AppliedAt)
		}
	}
	for index := range state.DurableQueue {
		sort.Slice(state.DurableQueue[index].AuthoredColumns, func(left, right int) bool {
			return state.DurableQueue[index].AuthoredColumns[left].Field < state.DurableQueue[index].AuthoredColumns[right].Field
		})
		state.DurableQueue[index].QueuedAt = snapshotTime(state.DurableQueue[index].QueuedAt)
	}
	sort.Slice(state.SealedBatches, func(left, right int) bool {
		return state.SealedBatches[left].Batch < state.SealedBatches[right].Batch
	})
	for index := range state.SealedBatches {
		state.SealedBatches[index].SealedAt = snapshotTime(state.SealedBatches[index].SealedAt)
		state.SealedBatches[index].ReconciledAt = snapshotTime(state.SealedBatches[index].ReconciledAt)
	}
	for index := range state.RebuildStaging {
		normalizeLocalRow(&state.RebuildStaging[index].Row)
	}
	for index := range state.SchemaJournal {
		normalizeSchemaManifest(&state.SchemaJournal[index].VerifiedTargetManifest)
		sort.Slice(state.SchemaJournal[index].AffectedScopes, func(left, right int) bool {
			return state.SchemaJournal[index].AffectedScopes[left] < state.SchemaJournal[index].AffectedScopes[right]
		})
	}
	if state.ErrorState != nil {
		state.ErrorState.At = snapshotTime(state.ErrorState.At)
	}
	state.Lifecycle.ChangedAt = snapshotTime(state.Lifecycle.ChangedAt)
}

func normalizeLocalRow(row *LocalRow) {
	sort.Slice(row.Fields, func(left, right int) bool {
		return row.Fields[left].Field < row.Fields[right].Field
	})
	row.UpdatedAt = snapshotTime(row.UpdatedAt)
}

func normalizeLocalOnlyRow(row *LocalOnlyRow) {
	sort.Slice(row.Fields, func(left, right int) bool {
		return row.Fields[left].Field < row.Fields[right].Field
	})
	row.UpdatedAt = snapshotTime(row.UpdatedAt)
}

func normalizeRetentionFloor(floor *RetentionFloor) {
	floor.ExpiresAt = snapshotTime(floor.ExpiresAt)
}

func normalizeSeedState(state *SeedState) {
	sort.Slice(state.Exports, func(left, right int) bool {
		return state.Exports[left].ID < state.Exports[right].ID
	})
	for index := range state.Exports {
		sort.Slice(state.Exports[index].Scopes, func(left, right int) bool {
			return state.Exports[index].Scopes[left].Scope < state.Exports[index].Scopes[right].Scope
		})
		state.Exports[index].CreatedAt = snapshotTime(state.Exports[index].CreatedAt)
	}
	for index := range state.Records {
		normalizeAuthoritativeRow(&state.Records[index].Row)
	}
}

func normalizeAuthorizationState(state *AuthorizationState) {
	sort.Slice(state.Roles, func(left, right int) bool {
		return state.Roles[left].Role < state.Roles[right].Role
	})
	for index := range state.Roles {
		sort.Slice(state.Roles[index].Capabilities, func(left, right int) bool {
			return state.Roles[index].Capabilities[left] < state.Roles[index].Capabilities[right]
		})
	}
	sort.Slice(state.WritePolicies, func(left, right int) bool {
		if state.WritePolicies[left].User != state.WritePolicies[right].User {
			return state.WritePolicies[left].User < state.WritePolicies[right].User
		}
		return state.WritePolicies[left].Table < state.WritePolicies[right].Table
	})
}

func normalizeInstallationCapabilities(state *InstallationCapabilities) {
	sort.Slice(state.Endpoints, func(left, right int) bool {
		return state.Endpoints[left] < state.Endpoints[right]
	})
	sort.Slice(state.Capabilities, func(left, right int) bool {
		return state.Capabilities[left].ID < state.Capabilities[right].ID
	})
	for index := range state.Capabilities {
		state.Capabilities[index].CheckedAt = snapshotTime(state.Capabilities[index].CheckedAt)
	}
}

func normalizeReadinessState(state *ReadinessState) {
	sort.Slice(state.Workers, func(left, right int) bool {
		return state.Workers[left].ID < state.Workers[right].ID
	})
	for index := range state.Workers {
		state.Workers[index].HeartbeatAt = snapshotTime(state.Workers[index].HeartbeatAt)
	}
	sort.Slice(state.Slots, func(left, right int) bool {
		return state.Slots[left].ID < state.Slots[right].ID
	})
	sort.Slice(state.Limits, func(left, right int) bool {
		return state.Limits[left].ID < state.Limits[right].ID
	})
	sort.Slice(state.Checks, func(left, right int) bool {
		return state.Checks[left].ID < state.Checks[right].ID
	})
	for index := range state.Checks {
		state.Checks[index].CheckedAt = snapshotTime(state.Checks[index].CheckedAt)
	}
	sort.Slice(state.Reasons, func(left, right int) bool {
		return state.Reasons[left] < state.Reasons[right]
	})
}

func lessSchemaRef(left, right SchemaRef) bool {
	if left.Version != right.Version {
		return left.Version < right.Version
	}
	return bytes.Compare(left.Hash[:], right.Hash[:]) < 0
}

func lessRelationID(left, right RelationID) bool {
	return left < right
}

func lessScopeID(left, right ScopeID) bool {
	return left < right
}

func lessFenceID(left, right FenceID) bool {
	return left < right
}

func lessClientKey(left, right ClientKey) bool {
	if left.UserID != right.UserID {
		return left.UserID < right.UserID
	}
	return left.ClientID < right.ClientID
}

// lessRowIdentity orders canonical bytes first, then inspectable identity terms.
func lessRowIdentity(left, right RowIdentity) bool {
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

func lessStreamPosition(left, right StreamPosition) bool {
	if left.StreamGeneration != right.StreamGeneration {
		return left.StreamGeneration < right.StreamGeneration
	}
	leftClass := streamPositionClass(left.Kind)
	rightClass := streamPositionClass(right.Kind)
	if leftClass == 4 || rightClass == 4 {
		if leftClass != rightClass {
			return leftClass < rightClass
		}
		return lessMalformedStreamPosition(left, right)
	}
	if leftClass == 1 || rightClass == 1 {
		if leftClass != rightClass {
			return leftClass < rightClass
		}
		return lessMalformedStreamPosition(left, right)
	}
	if left.CommitLSN != right.CommitLSN {
		return left.CommitLSN < right.CommitLSN
	}
	if leftClass != rightClass {
		return leftClass < rightClass
	}
	if leftClass == 2 {
		if left.EventOrdinal != right.EventOrdinal {
			return left.EventOrdinal < right.EventOrdinal
		}
		if left.EffectOrdinal != right.EffectOrdinal {
			return left.EffectOrdinal < right.EffectOrdinal
		}
	}
	return lessMalformedStreamPosition(left, right)
}

func streamPositionClass(kind PositionKind) uint8 {
	switch kind {
	case PositionKindGenerationStart:
		return 1
	case PositionKindEffect:
		return 2
	case PositionKindTransactionEnd:
		return 3
	default:
		return 4
	}
}

func lessMalformedStreamPosition(left, right StreamPosition) bool {
	if left.CommitLSN != right.CommitLSN {
		return left.CommitLSN < right.CommitLSN
	}
	if left.EventOrdinal != right.EventOrdinal {
		return left.EventOrdinal < right.EventOrdinal
	}
	if left.EffectOrdinal != right.EffectOrdinal {
		return left.EffectOrdinal < right.EffectOrdinal
	}
	return left.Kind < right.Kind
}

func lessTransactionReplayKey(left, right TransactionReplayKey) bool {
	if left.StreamGeneration != right.StreamGeneration {
		return left.StreamGeneration < right.StreamGeneration
	}
	return left.CommitLSN < right.CommitLSN
}

func lessEventReplayKey(left, right EventReplayKey) bool {
	if left.Transaction != right.Transaction {
		return lessTransactionReplayKey(left.Transaction, right.Transaction)
	}
	return left.EventOrdinal < right.EventOrdinal
}

func lessProjectionKey(left, right ProjectionKey) bool {
	if left.Relation != right.Relation {
		return left.Relation < right.Relation
	}
	if left.Event != right.Event {
		return lessEventReplayKey(left.Event, right.Event)
	}
	return left.Image < right.Image
}

func lessBatchKey(left, right BatchKey) bool {
	if left.Client != right.Client {
		return lessClientKey(left.Client, right.Client)
	}
	return left.Batch < right.Batch
}

func lessMutationKey(left, right MutationKey) bool {
	if left.Client != right.Client {
		return lessClientKey(left.Client, right.Client)
	}
	return left.Mutation < right.Mutation
}

func lessRebuildKey(left, right RebuildKey) bool {
	if left.Client != right.Client {
		return lessClientKey(left.Client, right.Client)
	}
	if left.Scope != right.Scope {
		return left.Scope < right.Scope
	}
	return left.Rebuild < right.Rebuild
}

func lessSeedRecordKey(left, right SeedRecordKey) bool {
	if left.Export != right.Export {
		return left.Export < right.Export
	}
	if left.Scope != right.Scope {
		return left.Scope < right.Scope
	}
	return left.Ordinal < right.Ordinal
}

func lessLocalOnlyRowKey(left, right LocalOnlyRowKey) bool {
	if left.Table != right.Table {
		return left.Table < right.Table
	}
	return left.Row < right.Row
}
