package reference

import (
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

const (
	userA        UserID     = "00000000-0000-4000-8000-000000000001"
	userB        UserID     = "00000000-0000-4000-8000-000000000002"
	clientAID    ClientID   = "10000000-0000-4000-8000-000000000001"
	clientBID    ClientID   = "10000000-0000-4000-8000-000000000002"
	relationA    RelationID = "20000000-0000-4000-8000-000000000001"
	relationB    RelationID = "20000000-0000-4000-8000-000000000002"
	tableA       TableID    = "30000000-0000-4000-8000-000000000001"
	tableB       TableID    = "30000000-0000-4000-8000-000000000002"
	fieldA       FieldID    = "40000000-0000-4000-8000-000000000001"
	fieldB       FieldID    = "40000000-0000-4000-8000-000000000002"
	fieldCreated FieldID    = "40000000-0000-4000-8000-000000000003"
	fieldUpdated FieldID    = "40000000-0000-4000-8000-000000000004"
	fieldDeleted FieldID    = "40000000-0000-4000-8000-000000000005"
	scopeA       ScopeID    = "50000000-0000-4000-8000-000000000001"
	scopeB       ScopeID    = "50000000-0000-4000-8000-000000000002"
	scopeC       ScopeID    = "50000000-0000-4000-8000-000000000003"
	scopeD       ScopeID    = "50000000-0000-4000-8000-000000000004"
	batchA       BatchID    = "60000000-0000-4000-8000-000000000001"
	batchB       BatchID    = "60000000-0000-4000-8000-000000000002"
	mutationA    MutationID = "70000000-0000-4000-8000-000000000001"
	mutationB    MutationID = "70000000-0000-4000-8000-000000000002"
	rebuildA     RebuildID  = "80000000-0000-4000-8000-000000000001"
	rebuildB     RebuildID  = "80000000-0000-4000-8000-000000000002"
	exportA      ExportID   = "90000000-0000-4000-8000-000000000001"
	exportB      ExportID   = "90000000-0000-4000-8000-000000000002"
	fenceA       FenceID    = "a0000000-0000-4000-8000-000000000001"
	fenceB       FenceID    = "a0000000-0000-4000-8000-000000000002"
	fenceC       FenceID    = "a0000000-0000-4000-8000-000000000003"
	fenceD       FenceID    = "a0000000-0000-4000-8000-000000000004"
)

func TestSnapshotIsDeterministicAcrossRootMapInsertionOrders(t *testing.T) {
	forward := snapshotState(sampleState(false))
	reverse := snapshotState(sampleState(true))
	if !reflect.DeepEqual(forward, reverse) {
		t.Fatal("snapshot changed with root-map insertion order")
	}
}

func TestSnapshotOrdersSchemasCompositeKeysAndCanonicalRowBytes(t *testing.T) {
	var lowHash [32]byte
	var highHash [32]byte
	highHash[0] = 0xff
	versionTwo := lowHash
	versionTwo[31] = 1
	firstClient := ClientKey{UserID: userA, ClientID: clientBID}
	secondClient := ClientKey{UserID: userB, ClientID: clientAID}
	firstRow := canonicalStringRowIdentity(tableA, fieldA, "alpha")
	secondRow := canonicalStringRowIdentity(tableB, fieldA, "beta")

	snapshot := snapshotState(State{
		Schemas: map[SchemaRef]SchemaManifest{
			{Version: 1, Hash: highHash}:   {Body: []byte("high")},
			{Version: 2, Hash: versionTwo}: {Body: []byte("version-two")},
			{Version: 1, Hash: lowHash}:    {Body: []byte("low")},
		},
		Clients: map[ClientKey]ClientState{
			secondClient: {},
			firstClient:  {},
		},
		Rows: map[RowIdentity]AuthoritativeRow{
			secondRow: {Identity: secondRow, Version: "opaque-version-2"},
			firstRow:  {Identity: firstRow, Version: "opaque-version-1"},
		},
	})

	if got, want := snapshot.Schemas[0].Key, (SchemaRef{Version: 1, Hash: lowHash}); got != want {
		t.Fatal("schema hash ordering is not raw-byte ordering")
	}
	if got, want := snapshot.Schemas[1].Key, (SchemaRef{Version: 1, Hash: highHash}); got != want {
		t.Fatal("schema refs with equal versions are not ordered by hash")
	}
	if got, want := snapshot.Schemas[2].Key, (SchemaRef{Version: 2, Hash: versionTwo}); got != want {
		t.Fatal("schema version ordering is not numeric")
	}
	if got := snapshot.Clients[0].Key; got != firstClient {
		t.Fatal("client composite keys are not ordered component by component")
	}
	if got := snapshot.Clients[1].Key; got != secondClient {
		t.Fatal("client composite keys did not remain distinct")
	}
	if got := snapshot.Rows[0].Key; got != firstRow {
		t.Fatal("rows are not ordered by complete canonical identity bytes")
	}
	if got := snapshot.Rows[1].Key; got != secondRow {
		t.Fatal("canonical row identity bytes did not remain distinct")
	}
}

func TestLessStreamPositionOrdersTransactionEndAfterItsEffects(t *testing.T) {
	generation := StreamGeneration("stream-generation-1")
	start := StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart, CommitLSN: 99}
	effectOne := StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: 10, EventOrdinal: 1, EffectOrdinal: 1}
	effectTwo := StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: 10, EventOrdinal: 2, EffectOrdinal: 1}
	end := StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 10}
	laterEffect := StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: 11, EventOrdinal: 1, EffectOrdinal: 1}
	unknown := StreamPosition{StreamGeneration: generation, Kind: PositionKind("malformed"), CommitLSN: 1}

	if !lessStreamPosition(start, effectOne) {
		t.Fatal("generation start does not precede committed effects")
	}
	if !lessStreamPosition(effectOne, effectTwo) {
		t.Fatal("effects at one commit do not order by event ordinal")
	}
	if !lessStreamPosition(effectTwo, end) || lessStreamPosition(end, effectOne) {
		t.Fatal("transaction end does not follow every effect at its commit")
	}
	if !lessStreamPosition(end, laterEffect) {
		t.Fatal("transaction end does not precede later commits")
	}
	if lessStreamPosition(unknown, end) || !lessStreamPosition(end, unknown) {
		t.Fatal("malformed position kinds do not have a deterministic fallback order")
	}
}

func TestLessRowIdentityBreaksEqualCanonicalBytes(t *testing.T) {
	first := RowIdentity{CanonicalIdentityBytes: "same", TableID: tableA, PrimaryKeyFieldID: fieldA, PortableType: "string", CanonicalWireJSON: "\"a\""}
	second := RowIdentity{CanonicalIdentityBytes: "same", TableID: tableB, PrimaryKeyFieldID: fieldA, PortableType: "string", CanonicalWireJSON: "\"a\""}
	snapshot := snapshotState(State{Rows: map[RowIdentity]AuthoritativeRow{
		second: {Identity: second, Version: "opaque-version-2"},
		first:  {Identity: first, Version: "opaque-version-1"},
	}})
	if !lessRowIdentity(first, second) || lessRowIdentity(second, first) {
		t.Fatal("row identity comparator does not break canonical-byte ties")
	}
	if snapshot.Rows[0].Key != first || snapshot.Rows[1].Key != second {
		t.Fatal("row snapshot order collapsed distinct canonical-byte ties")
	}
}

func TestSnapshotPreservesSemanticSequenceOrder(t *testing.T) {
	snapshot := snapshotState(sampleState(false))
	if got := snapshot.Stream.Transactions[0].ReplayKey.CommitLSN; got != 20 {
		t.Fatal("stream transaction sequence changed")
	}
	if got := snapshot.Stream.Transactions[1].ReplayKey.CommitLSN; got != 10 {
		t.Fatal("stream transaction sequence changed")
	}
	end := snapshot.Stream.Transactions[0].End
	if end.StreamGeneration != "stream-generation-1" || end.Kind != PositionKindTransactionEnd || end.CommitLSN != 20 || end.EventOrdinal != 0 || end.EffectOrdinal != 0 {
		t.Fatal("transaction-end position contains event or effect ordinals")
	}
	if got := snapshot.Stream.Transactions[0].Events[0].ReplayKey.EventOrdinal; got != 3 {
		t.Fatal("stream event sequence changed")
	}
	if got := snapshot.Stream.Transactions[0].Events[1].ReplayKey.EventOrdinal; got != 2 {
		t.Fatal("stream event sequence changed")
	}
	batch := batchSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID}, batchA)
	if batch.Mutations[0] != mutationB || batch.Mutations[1] != mutationA {
		t.Fatal("batch mutation sequence changed")
	}
	if batch.Outcomes[0].Mutation != mutationB || batch.Outcomes[1].Mutation != mutationA {
		t.Fatal("batch outcome sequence changed")
	}
	rebuild := rebuildSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID}, scopeA, rebuildA)
	if rebuild.StagedRows[0].Ordinal != 2 || rebuild.StagedRows[1].Ordinal != 1 {
		t.Fatal("rebuild staging sequence changed")
	}
	if rebuild.Pages[0].Ordinal != 2 || rebuild.Pages[1].Ordinal != 1 {
		t.Fatal("rebuild page sequence changed")
	}
	local := clientLocalSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID})
	if local.DurableQueue[0].Mutation != mutationB || local.DurableQueue[1].Mutation != mutationA {
		t.Fatal("local queue sequence changed")
	}
	if local.Outcomes[0].Mutation != mutationB || local.Outcomes[1].Mutation != mutationA {
		t.Fatal("local outcome sequence changed")
	}
	if local.SchemaJournal[0].Ordinal != 2 || local.SchemaJournal[1].Ordinal != 1 {
		t.Fatal("schema journal sequence changed")
	}
	if local.SchemaJournal[0].MigrationPlan[0].Kind != MigrationOperationDropField || local.SchemaJournal[0].MigrationPlan[1].Kind != MigrationOperationAddField {
		t.Fatal("migration plan sequence changed")
	}
	if local.RebuildStaging[0].Ordinal != 2 || local.RebuildStaging[1].Ordinal != 1 {
		t.Fatal("local rebuild stage sequence changed")
	}
	if local.RebuildAttempts[0].AppliedPages[0].RequestPageOrdinal != 2 || local.RebuildAttempts[0].AppliedPages[1].RequestPageOrdinal != 1 {
		t.Fatal("local rebuild applied-page sequence changed")
	}
	if snapshot.Events[0].Ordinal != 2 || snapshot.Events[1].Ordinal != 1 {
		t.Fatal("model event sequence changed")
	}
	if snapshot.Seed.Records[0].Key.Export != exportB || snapshot.Seed.Records[1].Key.Export != exportA {
		t.Fatal("seed record sequence changed")
	}
	if snapshot.Seed.Exports[1].Pages[0].Scope != scopeB || snapshot.Seed.Exports[1].Pages[1].Scope != scopeA {
		t.Fatal("seed page sequence changed")
	}
}

func TestSnapshotRetainsProjectionImagesAndTransactionPoison(t *testing.T) {
	snapshot := snapshotState(sampleState(false))
	firstTransaction := TransactionReplayKey{StreamGeneration: "stream-generation-1", CommitLSN: 10}
	firstEvent := EventReplayKey{Transaction: firstTransaction, EventOrdinal: 1}
	before := false
	after := false
	for _, entry := range snapshot.Projections {
		if entry.Key.Relation != relationA || entry.Key.Event != firstEvent {
			continue
		}
		before = before || entry.Key.Image == ProjectionImageBefore
		after = after || entry.Key.Image == ProjectionImageAfter
	}
	if !before || !after {
		t.Fatal("one source event did not retain before and after projections")
	}
	if snapshot.Stream.Poison[0].Transaction != firstTransaction || snapshot.Stream.Poison[0].HasRelation {
		t.Fatal("poison is not normalized by transaction identity")
	}
	if snapshot.Stream.Poison[1].Transaction.CommitLSN != 20 || !snapshot.Stream.Poison[1].HasRelation || snapshot.Stream.Poison[1].Relation != relationA {
		t.Fatal("poison did not retain its optional logical relation")
	}
	foundCaptureFence := false
	for _, entry := range snapshot.Fences {
		if entry.Key != fenceB {
			continue
		}
		foundCaptureFence = true
		identity := entry.Value.NewRegisteredIdentity
		if identity.Kind != RegistrationKindCaptureDependency || identity.SyncedRow != (RowIdentity{}) || identity.CaptureKey.CanonicalKeyBytes == "" {
			t.Fatal("capture-dependency fence fabricated a synced row identity")
		}
		coverage := entry.Value.ResetBaselineCoverage
		if !entry.Value.HasResetBaselineCoverage || coverage.ResetID == "" || coverage.CandidateSlot == "" || coverage.TargetStreamGeneration != "stream-generation-2" {
			t.Fatal("reset-baseline fence coverage lost reset authority")
		}
	}
	if !foundCaptureFence {
		t.Fatal("capture-dependency fence is missing")
	}
}

func TestSnapshotRetainsCandidateProjectionStages(t *testing.T) {
	snapshot := snapshotState(sampleState(false))
	if len(snapshot.Rows) != 2 || len(snapshot.Projections) != 2 || len(snapshot.Fences) != 2 || len(snapshot.Scopes) != 2 || snapshot.Stream.Reset == nil || !snapshot.Stream.Reset.HasCandidateStage {
		t.Fatal("active state did not coexist with the reset candidate stage")
	}
	stage := snapshot.Stream.Reset.CandidateStage
	candidateRowA := canonicalStringRowIdentity(tableA, fieldA, "candidate-alpha")
	candidateRowB := canonicalStringRowIdentity(tableB, fieldA, "candidate-beta")
	if stage.RegistryGeneration != 3 || stage.Schema != schemaRef(2, 2) || stage.StreamGeneration != "stream-generation-2" || !stage.Verified || len(stage.Rows) != 2 || stage.Rows[0].Identity != candidateRowA || stage.Rows[1].Identity != candidateRowB || stage.Rows[0].Row.FieldValues[0].Field != fieldA || stage.Rows[0].Row.UpdatedAt == nil || stage.Rows[0].Row.UpdatedAt.Location() != time.UTC {
		t.Fatal("candidate rows were not retained and normalized")
	}
	if len(stage.Projections) != 2 || stage.Projections[0].Key.Relation != relationA || stage.Projections[1].Key.Relation != relationB || stage.Projections[0].Projection.Fields[0].Field != fieldA || stage.Projections[0].Projection.CapturedAt == nil || stage.Projections[0].Projection.CapturedAt.Location() != time.UTC {
		t.Fatal("candidate projections were not retained and normalized")
	}
	if len(stage.Fences) != 2 || stage.Fences[0].ID != fenceC || stage.Fences[1].ID != fenceD || len(stage.Scopes) != 2 || stage.Scopes[0].Scope != scopeC || stage.Scopes[1].Scope != scopeD || stage.Scopes[0].State.Membership[0].Row != candidateRowA || stage.Scopes[0].State.Effects[0].Row != candidateRowA || stage.Scopes[0].State.Checksum != (Checksum{0: 7}) {
		t.Fatal("candidate fences or scopes were not retained and normalized")
	}
	bootstrap := snapshot.Registry.Generations[2]
	if bootstrap.Generation != 3 || bootstrap.Validated || !bootstrap.HasBootstrapStage || bootstrap.BootstrapStage.RegistryGeneration != 3 || bootstrap.BootstrapStage.Verified || len(bootstrap.BootstrapStage.Rows) != 1 || bootstrap.BootstrapStage.Rows[0].Row.UpdatedAt == nil || bootstrap.BootstrapStage.Rows[0].Row.UpdatedAt.Location() != time.UTC || len(bootstrap.BootstrapStage.Projections) != 1 || bootstrap.BootstrapStage.Projections[0].Projection.CapturedAt == nil || bootstrap.BootstrapStage.Projections[0].Projection.CapturedAt.Location() != time.UTC || len(bootstrap.BootstrapStage.Fences) != 1 || len(bootstrap.BootstrapStage.Scopes) != 1 {
		t.Fatal("registry bootstrap candidate stage changed")
	}
}

func TestInactiveCandidateProjectionStagesRemainIsolated(t *testing.T) {
	state := sampleState(false)
	state.Stream.Reset.HasCandidateStage = false
	for index := range state.Registry.Generations {
		if state.Registry.Generations[index].Generation == 3 {
			state.Registry.Generations[index].HasBootstrapStage = false
		}
	}

	cloned := cloneState(state)
	baseline := snapshotState(cloned)
	if got := baseline.Stream.Reset.CandidateStage.Rows[0].Identity; got != canonicalStringRowIdentity(tableA, fieldA, "candidate-alpha") {
		t.Fatal("inactive reset candidate stage was not normalized")
	}

	state.Stream.Reset.CandidateStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	for index := range state.Registry.Generations {
		if state.Registry.Generations[index].Generation == 3 {
			state.Registry.Generations[index].BootstrapStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
		}
	}
	if got := snapshotState(cloned); !reflect.DeepEqual(got, baseline) {
		t.Fatal("inactive candidate stage changed through its input state")
	}

	first := snapshotState(cloned)
	second := snapshotState(cloned)
	first.Stream.Reset.CandidateStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	first.Registry.Generations[2].BootstrapStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	if !reflect.DeepEqual(second, baseline) {
		t.Fatal("inactive candidate stages share mutable snapshot state")
	}
}

func TestSnapshotNormalizesExpandedStateSets(t *testing.T) {
	snapshot := snapshotState(sampleState(false))
	if snapshot.Registry.Generations[1].ActivationBoundary.CommitLSN != 10 || !snapshot.Registry.Generations[1].Validated {
		t.Fatal("registry generation activation state changed")
	}
	if !snapshot.Registry.Generations[1].Relations[0].Definition.HasTableID || snapshot.Registry.Generations[1].Relations[0].Definition.TableID != tableA || snapshot.Registry.Generations[1].Relations[1].Definition.HasTableID {
		t.Fatal("relation registrations did not retain discriminated logical tables")
	}
	client := snapshot.Clients[0].Value
	if client.ScopeSetVersion != 2 || !client.ScopeAssignments[1].RebuildRequired || !client.Checkpoints[0].HasChecksum || client.Checkpoints[0].Checksum != (Checksum{}) || !client.Checkpoints[1].HasCursor || client.Checkpoints[1].Cursor != (OpaqueToken{}) {
		t.Fatal("server client scope state changed")
	}
	if client.Generations[0].Generation != 1 || client.Generations[1].Generation != 2 {
		t.Fatal("client generation history is not sorted")
	}
	if client.Generations[0].CreatedAt == nil || client.Generations[1].ExpiresAt == nil {
		t.Fatal("client generation history omitted issued or expiry time")
	}
	seed := snapshot.Seed.Exports[1]
	if len(seed.Scopes) != 2 || seed.Scopes[0].Scope != scopeA || seed.Scopes[0].MembershipGeneration != 1 || seed.Scopes[0].RetentionGeneration != 1 || seed.Scopes[0].Cardinality != 1 || seed.Scopes[1].Scope != scopeB || seed.Scopes[1].MembershipGeneration != 2 || seed.Scopes[1].RetentionGeneration != 2 || seed.Scopes[1].Cardinality != 2 {
		t.Fatal("per-scope seed declaration set is not sorted or complete")
	}
	if snapshot.Authorization.Roles[0].Role != "sync_runtime" || snapshot.Authorization.Roles[0].Capabilities[0] != "read" {
		t.Fatal("authorization capabilities are not sorted")
	}
	if snapshot.Installation.Endpoints[0] != "/sync/connect" || snapshot.Installation.Endpoints[1] != "/sync/pull" {
		t.Fatal("installation endpoints are not sorted")
	}
	if snapshot.Readiness.Checks[0].State != ReadinessCheckOK || snapshot.Readiness.Checks[1].State != ReadinessCheckUnknown {
		t.Fatal("readiness check states changed")
	}
	local := clientLocalSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID})
	if local.CurrentSchema != schemaRef(2, 2) || local.AuthoritativeScopeSetVersion != 2 || local.ScopeAssignments[0].Scope != scopeA || local.ScopeCheckpoints[0].Scope != scopeA || local.Backoff == nil || local.Backoff.NextEligibleAt == nil {
		t.Fatal("local durable synchronization state changed")
	}
	if len(local.SeedReceipts) != 2 {
		t.Fatal("local seed receipt set changed")
	}
	localReceipt := local.SeedReceipts[0]
	if localReceipt.Scope != scopeA || !localReceipt.HasReceipt || localReceipt.Receipt != (OpaqueToken{}) || localReceipt.ExportID != exportB || localReceipt.ExportManifestHash != schemaRef(2, 2).Hash || localReceipt.Schema != schemaRef(2, 2) || localReceipt.RegistryGeneration != 2 || localReceipt.MembershipGeneration != 1 || localReceipt.RetentionGeneration != 1 || localReceipt.StreamGeneration != "stream-generation-1" || localReceipt.SnapshotBoundary.CommitLSN != 20 || localReceipt.Cardinality != 1 || localReceipt.Checksum != (Checksum{}) || local.SeedReceipts[1].Scope != scopeB {
		t.Fatal("local seed receipts are not sorted or did not retain zero-token presence")
	}
	rebuild := rebuildSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID}, scopeA, rebuildA)
	if rebuild.ClientGeneration != 1 || !rebuild.HasContinuation || rebuild.Continuation != (OpaqueToken{}) || rebuild.NextRowOrdinal != 3 || !rebuild.Pages[0].HasToken || rebuild.Pages[0].Token != (OpaqueToken{}) || rebuild.Pages[1].HasToken {
		t.Fatal("rebuild token presence state changed")
	}
	if len(local.RebuildAttempts) != 2 {
		t.Fatal("local rebuild attempts changed")
	}
	attempt := local.RebuildAttempts[0]
	if attempt.Scope != scopeA || attempt.Rebuild != rebuildA || attempt.ClientGeneration != 1 || attempt.Schema != schemaRef(2, 2) || attempt.PageLimit != 100 || !attempt.HasContinuation || attempt.Continuation != (OpaqueToken{}) || attempt.Phase != LocalRebuildAttemptPhasePendingFinality || len(attempt.AppliedPages) != 2 || attempt.AppliedPages[0].RequestPageOrdinal != 2 || !attempt.AppliedPages[0].HasRequestToken || attempt.AppliedPages[0].AppliedAt == nil || attempt.AppliedPages[0].AppliedAt.Location() != time.UTC || attempt.AppliedPages[1].RequestPageOrdinal != 1 || !attempt.AppliedPages[1].HasRequestToken || attempt.AppliedPages[1].RequestToken != (OpaqueToken{}) || !attempt.HasPendingFinalResult || !attempt.PendingFinalResult.HasFinalCursor || attempt.PendingFinalResult.FinalCursor != (OpaqueToken{}) || attempt.PendingFinalResult.ScopeChecksum != (Checksum{0: 9}) || attempt.PendingFinalResult.Cardinality != 2 || local.RebuildAttempts[1].Scope != scopeB || local.RebuildAttempts[1].Rebuild != rebuildB || local.RebuildAttempts[1].Phase != LocalRebuildAttemptPhaseApplying || local.RebuildAttempts[1].HasContinuation || local.RebuildAttempts[1].Continuation != (OpaqueToken{}) || local.RebuildAttempts[1].AppliedPages[0].HasRequestToken || local.RebuildAttempts[1].AppliedPages[0].RequestToken != (OpaqueToken{}) || local.RebuildAttempts[1].HasPendingFinalResult {
		t.Fatal("local rebuild attempt state is not sorted or complete")
	}
	if !seed.Scopes[0].HasReceipt || seed.Scopes[0].Receipt != (OpaqueToken{}) || !seed.Pages[0].HasToken || seed.Pages[0].Token != (OpaqueToken{}) || seed.Pages[1].HasToken {
		t.Fatal("seed token presence state changed")
	}
	if snapshot.Stream.Authority.ActiveGeneration != "stream-generation-1" || !snapshot.Stream.Authority.HasActiveSlot || snapshot.Stream.Reset == nil || snapshot.Stream.Reset.TargetStreamGeneration != "stream-generation-2" {
		t.Fatal("stream authority or reset state changed")
	}
}

func TestEffectOperationRanks(t *testing.T) {
	if EffectOperationDeleteRank != 0 || EffectOperationUpsertRank != 1 {
		t.Fatal("effect operation ranks changed")
	}
	if effectOperationRank(EffectOperationDelete) != EffectOperationDeleteRank || effectOperationRank(EffectOperationUpsert) != EffectOperationUpsertRank {
		t.Fatal("effect operation rank mapping changed")
	}
}

func TestSnapshotRetainsTask6NestedState(t *testing.T) {
	snapshot := snapshotState(sampleState(false))
	firstRow := canonicalStringRowIdentity(tableA, fieldA, "alpha")
	secondRow := canonicalStringRowIdentity(tableB, fieldA, "beta")
	if len(snapshot.Stream.SourceRows) != 2 || snapshot.Stream.SourceRows[0].Identity != firstRow || snapshot.Stream.SourceRows[0].Row.Version != "source-version-a" || snapshot.Stream.SourceRows[1].Identity != secondRow || snapshot.Stream.SourceRows[1].Row.Version != "source-version-b" || snapshot.Rows[0].Value.Version == snapshot.Stream.SourceRows[0].Row.Version || snapshot.Stream.Authority.AcknowledgedEndLSN != 20 {
		t.Fatal("source rows or stream acknowledgement state changed")
	}
	transaction := snapshot.Stream.Transactions[0]
	if transaction.EndLSN != 20 || transaction.RegistryGeneration != 2 || transaction.Lifecycle != TransactionLifecyclePoisoned || transaction.Events[0].Operation != DMLOperationDelete || !transaction.Events[0].HasBefore || !transaction.Events[0].HasAfter || transaction.Events[0].Before.Fields[0].Field != fieldA || transaction.Events[0].After.Version != "source-version-deleted" || !transaction.Events[0].After.Deleted {
		t.Fatal("source transaction images changed")
	}
	if snapshot.Stream.TransactionReplays[0].EndLSN != 10 || snapshot.Stream.TransactionReplays[0].RegistryGeneration != 1 || !snapshot.Stream.TransactionReplays[0].Completed || snapshot.Stream.Acknowledgements[0].EndLSN != 10 || snapshot.Stream.Poison[0].Lifecycle != PoisonLifecycleRepaired || snapshot.Stream.Poison[1].Lifecycle != PoisonLifecycleActive {
		t.Fatal("stream replay, acknowledgement, or poison state changed")
	}
	generation := snapshot.Registry.Generations[1]
	if generation.ScopeRules[0].Evaluations[0].Row != firstRow || generation.ScopeRules[0].Evaluations[0].Scopes[0] != scopeA || generation.ScopeRules[0].Evaluations[0].Scopes[1] != scopeB || generation.DependencyImpacts[0].AffectedRows[0] != firstRow || generation.DependencyImpacts[0].AffectedRows[1] != secondRow {
		t.Fatal("membership evaluations or dependency rows are not normalized")
	}
	scope := snapshot.Scopes[0].Value
	if scope.Cardinality != 2 || scope.Effects[0].Operation != EffectOperationDelete || scope.Effects[0].SourceEvent.EventOrdinal != 1 || !scope.Effects[0].HasCapturedProjection || !scope.Effects[0].HasChecksum || scope.Effects[1].Operation != EffectOperationUpsert || scope.Effects[1].Version != "effect-version-b" {
		t.Fatal("pull-visible scope effects changed")
	}
	batch := batchSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID}, batchA)
	if batch.Fingerprint.Algorithm != "sha-256" || batch.Fingerprint.Version != 1 || batch.Fingerprint.Domain != "batch" || batch.Fingerprint.Digest != (Fingerprint{0: 1}) || batch.HTTPStatus != 200 || batch.ServerTime == nil || batch.ServerTime.Location() != time.UTC {
		t.Fatal("batch fingerprint or stored response state changed")
	}
	for _, projection := range snapshot.Projections {
		if projection.Key.Image == ProjectionImageBefore && projection.Value.Version != "projection-version-before" {
			t.Fatal("captured projection server version changed")
		}
	}
	local := clientLocalSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID})
	if local.ClientGeneration != 1 || !local.Rows[1].HasServerVersion || local.Rows[1].ServerVersion != "local-server-version-b" || !local.Rows[1].HasChecksum || local.Rows[1].Checksum != (Checksum{0: 20}) || local.DurableQueue[0].Mutation != mutationB || local.DurableQueue[0].Table != tableB || local.DurableQueue[0].AuthoredColumns[0].Field != fieldA || local.DurableQueue[0].AuthoredColumns[0].WireJSON != "\"a\"" || local.DurableQueue[1].Mutation != mutationA || !local.DurableQueue[1].HasBaseVersion || !local.DurableQueue[1].HasPredecessor || local.DurableQueue[1].Status != LocalMutationStatusSealed {
		t.Fatal("durable local mutation state changed")
	}
	if len(local.SealedBatches) != 2 || local.SealedBatches[0].Batch != batchA || local.SealedBatches[0].Mutations[0] != mutationA || local.SealedBatches[0].Mutations[1] != mutationB || !local.SealedBatches[0].HasCanonicalResponse || local.SealedBatches[0].State != LocalSealedBatchStateReconciled || local.SealedBatches[0].SealedAt == nil || local.SealedBatches[0].SealedAt.Location() != time.UTC || local.SealedBatches[1].Batch != batchB || local.SealedBatches[1].State != LocalSealedBatchStateResponseLost || local.ErrorState == nil || local.ErrorState.Reason != "transport" || !local.ErrorState.Retryable || local.ErrorState.At == nil || local.ErrorState.At.Location() != time.UTC {
		t.Fatal("sealed batch or local error state changed")
	}
	if local.SchemaJournal[0].JournalVersion != 2 || local.SchemaJournal[0].MigrationPlanVersion != 3 || local.SchemaJournal[0].MigrationPlan[2].Kind != MigrationOperationUpdateCursor || local.SchemaJournal[0].MigrationPlan[6].Kind != MigrationOperationUpdateSchemaMetadata || snapshot.Installation.MinimumClientRuntime != 3 || snapshot.Installation.StaleClientIntervalMilliseconds != 86400000 {
		t.Fatal("schema journal or deployment policy state changed")
	}
	rebuild := rebuildSnapshot(snapshot, ClientKey{UserID: userA, ClientID: clientAID}, scopeA, rebuildA)
	if rebuild.CreatedAt == nil || rebuild.CreatedAt.Location() != time.UTC || !rebuild.HasFinalCursor || rebuild.FinalCursor != (OpaqueToken{}) || string(rebuild.Pages[0].CanonicalResponse) != "rebuild-page-response-b" || !rebuild.Pages[0].HasContinuation || !rebuild.Pages[0].HasChecksum || !rebuild.Pages[1].HasFinalCursor || rebuild.Pages[1].FinalCursor != (OpaqueToken{}) || rebuild.Pages[1].Checksum != (Checksum{0: 19}) {
		t.Fatal("rebuild replay state changed")
	}
}

func TestCloneAndSnapshotsDoNotShareMutableState(t *testing.T) {
	state := sampleState(false)
	baseline := snapshotState(state)
	cloned := cloneState(state)
	firstSnapshot := snapshotState(state)
	secondSnapshot := snapshotState(state)

	mutateEveryStateFamily(&state)
	if got := snapshotState(cloned); !reflect.DeepEqual(got, baseline) {
		t.Fatal("cloned state changed when input state changed")
	}
	if !reflect.DeepEqual(firstSnapshot, baseline) {
		t.Fatal("first snapshot changed when input state changed")
	}
	if !reflect.DeepEqual(secondSnapshot, baseline) {
		t.Fatal("second snapshot changed when input state changed")
	}

	mutateSnapshotFamilies(&firstSnapshot)
	if !reflect.DeepEqual(secondSnapshot, baseline) {
		t.Fatal("snapshots share mutable state")
	}
	if got := snapshotState(cloned); !reflect.DeepEqual(got, baseline) {
		t.Fatal("snapshot mutation changed cloned state")
	}
	if later := snapshotState(cloned); !reflect.DeepEqual(later, baseline) {
		t.Fatal("later snapshot changed after another snapshot mutation")
	}
}

func TestSnapshotPreservesNilAndEmptySlices(t *testing.T) {
	empty := State{
		Events:        []ModelEvent{},
		Registry:      RegistryState{Generations: []RegistryGenerationState{{HasBootstrapStage: true, BootstrapStage: CandidateProjectionStage{Rows: []CandidateRowEntry{}, Projections: []CandidateProjectionEntry{}, Fences: []CandidateFenceEntry{}, Scopes: []CandidateScopeEntry{}}}}},
		Stream:        StreamState{Reset: &StreamReset{HasCandidateStage: true, CandidateStage: CandidateProjectionStage{Rows: []CandidateRowEntry{}, Projections: []CandidateProjectionEntry{}, Fences: []CandidateFenceEntry{}, Scopes: []CandidateScopeEntry{}}}, SourceRows: []SourceRowEntry{}, Transactions: []StreamTransaction{}},
		Seed:          SeedState{Exports: []SeedExport{{Scopes: []SeedScopeState{}, Pages: []SeedPageState{}}}, Records: []SeedRecord{}},
		Authorization: AuthorizationState{Roles: []RoleCapabilities{}},
		Installation:  InstallationCapabilities{Capabilities: []InstallationCapability{}},
		Readiness:     ReadinessState{Reasons: []ReasonCode{}},
		Batches:       map[BatchKey]BatchLedger{{}: {Mutations: []MutationID{}, Outcomes: []MutationOutcome{}}},
		ClientLocal:   map[ClientKey]ClientLocalState{{}: {ScopeAssignments: []LocalScopeAssignment{}, ScopeCheckpoints: []LocalScopeCheckpoint{}, SeedReceipts: []LocalSeedReceipt{}, RebuildAttempts: []LocalRebuildAttempt{{AppliedPages: []AppliedRebuildPage{}}}, SealedBatches: []LocalSealedBatch{{Mutations: []MutationID{}, CanonicalRequest: []byte{}, CanonicalResponse: []byte{}}}, DurableQueue: []QueuedMutation{{AuthoredColumns: []FieldValue{}, Request: []byte{}}}, SchemaJournal: []SchemaJournalEntry{{AffectedScopes: []ScopeID{}, MigrationPlan: []MigrationPlanOperation{}}}}},
	}

	clonedNil := cloneState(State{})
	if clonedNil.Events != nil || clonedNil.Registry.Generations != nil || clonedNil.Stream.SourceRows != nil || clonedNil.Stream.Transactions != nil {
		t.Fatal("clone changed nil slices to empty slices")
	}
	if clonedNil.Schemas == nil || clonedNil.Relations == nil || clonedNil.Clients == nil || clonedNil.Rows == nil || clonedNil.Scopes == nil || clonedNil.Fences == nil || clonedNil.Projections == nil || clonedNil.Batches == nil || clonedNil.Mutations == nil || clonedNil.Rebuilds == nil || clonedNil.ClientLocal == nil || clonedNil.RetentionFloors == nil {
		t.Fatal("clone did not initialize all nil root maps")
	}
	if cloneClientLocalState(ClientLocalState{}).SeedReceipts != nil || cloneClientLocalState(ClientLocalState{}).RebuildAttempts != nil || cloneClientLocalState(ClientLocalState{}).SealedBatches != nil {
		t.Fatal("clone changed nil local receipt or rebuild slices to empty slices")
	}
	nilAttemptState := cloneClientLocalState(ClientLocalState{RebuildAttempts: []LocalRebuildAttempt{{}}})
	if nilAttemptState.RebuildAttempts[0].AppliedPages != nil {
		t.Fatal("clone changed nil applied-page slices to empty slices")
	}
	nilCandidateStage := cloneCandidateProjectionStage(CandidateProjectionStage{})
	if nilCandidateStage.Rows != nil || nilCandidateStage.Projections != nil || nilCandidateStage.Fences != nil || nilCandidateStage.Scopes != nil {
		t.Fatal("clone changed nil candidate stage slices to empty slices")
	}
	nilCandidateSnapshot := snapshotState(State{Registry: RegistryState{Generations: []RegistryGenerationState{{HasBootstrapStage: true}}}, Stream: StreamState{Reset: &StreamReset{HasCandidateStage: true}}})
	if nilCandidateSnapshot.Registry.Generations[0].BootstrapStage.Rows != nil || nilCandidateSnapshot.Registry.Generations[0].BootstrapStage.Projections != nil || nilCandidateSnapshot.Registry.Generations[0].BootstrapStage.Fences != nil || nilCandidateSnapshot.Registry.Generations[0].BootstrapStage.Scopes != nil || nilCandidateSnapshot.Stream.Reset.CandidateStage.Rows != nil || nilCandidateSnapshot.Stream.Reset.CandidateStage.Projections != nil || nilCandidateSnapshot.Stream.Reset.CandidateStage.Fences != nil || nilCandidateSnapshot.Stream.Reset.CandidateStage.Scopes != nil {
		t.Fatal("snapshot changed nil candidate stage slices to empty slices")
	}

	snapshot := snapshotState(empty)
	if snapshot.Events == nil || snapshot.Registry.Generations == nil || snapshot.Stream.SourceRows == nil || snapshot.Stream.Transactions == nil {
		t.Fatal("snapshot changed empty slices to nil slices")
	}
	if snapshot.Seed.Exports == nil || snapshot.Authorization.Roles == nil || snapshot.Installation.Capabilities == nil {
		t.Fatal("snapshot did not preserve empty nested slices")
	}
	if snapshot.Seed.Exports[0].Scopes == nil || snapshot.Seed.Exports[0].Pages == nil || snapshot.Seed.Records == nil {
		t.Fatal("snapshot did not preserve empty seed slices")
	}
	if snapshot.ClientLocal[0].Value.ScopeAssignments == nil || snapshot.ClientLocal[0].Value.ScopeCheckpoints == nil || snapshot.ClientLocal[0].Value.SeedReceipts == nil || snapshot.ClientLocal[0].Value.RebuildAttempts == nil || snapshot.ClientLocal[0].Value.RebuildAttempts[0].AppliedPages == nil || snapshot.ClientLocal[0].Value.SealedBatches == nil || snapshot.ClientLocal[0].Value.SealedBatches[0].Mutations == nil || snapshot.ClientLocal[0].Value.SealedBatches[0].CanonicalRequest == nil || snapshot.ClientLocal[0].Value.SealedBatches[0].CanonicalResponse == nil || snapshot.ClientLocal[0].Value.DurableQueue == nil || snapshot.ClientLocal[0].Value.DurableQueue[0].AuthoredColumns == nil || snapshot.ClientLocal[0].Value.DurableQueue[0].Request == nil || snapshot.ClientLocal[0].Value.SchemaJournal[0].AffectedScopes == nil || snapshot.ClientLocal[0].Value.SchemaJournal[0].MigrationPlan == nil {
		t.Fatal("snapshot did not preserve empty local durable slices")
	}
	bootstrap := snapshot.Registry.Generations[0].BootstrapStage
	candidate := snapshot.Stream.Reset.CandidateStage
	if bootstrap.Rows == nil || bootstrap.Projections == nil || bootstrap.Fences == nil || bootstrap.Scopes == nil || candidate.Rows == nil || candidate.Projections == nil || candidate.Fences == nil || candidate.Scopes == nil {
		t.Fatal("snapshot did not preserve empty candidate stage slices")
	}
	if snapshot.Readiness.Reasons == nil || snapshot.Batches[0].Value.Mutations == nil || snapshot.Batches[0].Value.Outcomes == nil {
		t.Fatal("snapshot did not preserve empty ledger and readiness slices")
	}
}

func TestStateAndSnapshotRootContracts(t *testing.T) {
	stateType := reflect.TypeOf(State{})
	if _, found := stateType.FieldByName("Identities"); found {
		t.Fatal("state retained a noncontract identity root")
	}
	if typeContainsMap(reflect.TypeOf(StateSnapshot{}), make(map[reflect.Type]bool)) {
		t.Fatal("state snapshot contains a map")
	}
}

func typeContainsMap(typ reflect.Type, visited map[reflect.Type]bool) bool {
	if typ == nil || visited[typ] {
		return false
	}
	visited[typ] = true
	switch typ.Kind() {
	case reflect.Map:
		return true
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return typeContainsMap(typ.Elem(), visited)
	case reflect.Struct:
		for index := 0; index < typ.NumField(); index++ {
			if typeContainsMap(typ.Field(index).Type, visited) {
				return true
			}
		}
	}
	return false
}

func sampleState(reverse bool) State {
	firstTime := time.Date(2026, time.January, 2, 3, 4, 5, 6, time.FixedZone("sample", 3600))
	secondTime := firstTime.Add(time.Hour)
	defaultValue := "null"
	deleteReason := "deleted_for_test"
	createdField := fieldCreated
	updatedField := fieldUpdated
	deletedField := fieldDeleted
	firstRef := schemaRef(1, 1)
	secondRef := schemaRef(2, 2)
	firstClient := ClientKey{UserID: userA, ClientID: clientAID}
	secondClient := ClientKey{UserID: userB, ClientID: clientBID}
	firstRow := canonicalStringRowIdentity(tableA, fieldA, "alpha")
	secondRow := canonicalStringRowIdentity(tableB, fieldA, "beta")
	generation := StreamGeneration("stream-generation-1")
	firstTransaction := TransactionReplayKey{StreamGeneration: generation, CommitLSN: 10}
	secondTransaction := TransactionReplayKey{StreamGeneration: generation, CommitLSN: 20}
	firstEvent := EventReplayKey{Transaction: firstTransaction, EventOrdinal: 1}
	secondEvent := EventReplayKey{Transaction: secondTransaction, EventOrdinal: 2}
	thirdEvent := EventReplayKey{Transaction: secondTransaction, EventOrdinal: 3}
	firstPosition := StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: 10, EventOrdinal: 1, EffectOrdinal: 1}
	secondPosition := StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: 20, EventOrdinal: 2, EffectOrdinal: 1}
	thirdPosition := StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: 20, EventOrdinal: 3, EffectOrdinal: 1}
	firstToken := OpaqueToken{namespace: 1, sequence: 1}
	secondToken := OpaqueToken{namespace: 1, sequence: 2}
	firstRegisteredIdentity := RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: firstRow}
	secondRegisteredIdentity := RegisteredIdentity{Kind: RegistrationKindCaptureDependency, CaptureKey: CanonicalCaptureKey{CanonicalKeyBytes: "capture-key-beta"}}

	definitionA := RelationDefinition{
		Relation:                   relationA,
		RegistrationKind:           RegistrationKindSynced,
		HasTableID:                 true,
		TableID:                    tableA,
		Physical:                   PhysicalRelation{Schema: "app", Name: "orders", OID: 101, ReplicaIdentity: ReplicaIdentityDefault},
		PrimaryKeyFieldID:          fieldA,
		PrimaryKeyPhysicalColumn:   "order_id",
		PrimaryKeyPortableType:     "string",
		CapturedFieldIDs:           []FieldID{fieldB, fieldA},
		MembershipFunction:         "c0000000-0000-4000-8000-000000000001",
		PositiveFanoutBound:        1,
		DependencyImpactFunction:   "d0000000-0000-4000-8000-000000000001",
		DependencyCapturedFieldIDs: []FieldID{fieldB, fieldA},
		PositiveDependencyRowBound: 1,
	}
	definitionB := RelationDefinition{
		Relation:                   relationB,
		RegistrationKind:           RegistrationKindCaptureDependency,
		HasTableID:                 false,
		Physical:                   PhysicalRelation{Schema: "app", Name: "customers", OID: 102, ReplicaIdentity: ReplicaIdentityDefault},
		CaptureKeyFieldIDs:         []FieldID{fieldB, fieldA},
		CapturedFieldIDs:           []FieldID{fieldA},
		PositiveFanoutBound:        1,
		PositiveDependencyRowBound: 1,
	}
	journalManifest := SchemaManifest{Body: []byte("verified-target-manifest"), Class: SchemaClass2, CompatibilityFloor: 1}
	sourceRowA := sampleRow(firstRow, &secondTime, nil)
	sourceRowA.Version = "source-version-a"
	sourceRowA.Checksum = Checksum{0: 10}
	sourceRowB := sampleRow(secondRow, &secondTime, &deleteReason)
	sourceRowB.Version = "source-version-b"
	sourceRowB.Checksum = Checksum{0: 11}
	sourceBefore := SourceImage{Identity: firstRegisteredIdentity, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"before-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"before-a\""}}, Version: "source-version-before", HasChecksum: true, Checksum: Checksum{0: 12}}
	sourceAfter := SourceImage{Identity: firstRegisteredIdentity, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"after-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"after-a\""}}, Version: "source-version-after", HasChecksum: true, Checksum: Checksum{0: 13}}
	sourceDeleted := SourceImage{Identity: firstRegisteredIdentity, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"deleted-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"deleted-a\""}}, Version: "source-version-deleted", HasChecksum: true, Checksum: Checksum{0: 14}, Deleted: true}
	candidateGeneration := StreamGeneration("stream-generation-2")
	candidateFirstPosition := StreamPosition{StreamGeneration: candidateGeneration, Kind: PositionKindEffect, CommitLSN: 30, EventOrdinal: 1, EffectOrdinal: 1}
	candidateSecondPosition := StreamPosition{StreamGeneration: candidateGeneration, Kind: PositionKindEffect, CommitLSN: 30, EventOrdinal: 2, EffectOrdinal: 1}
	candidateBarrier := StreamPosition{StreamGeneration: candidateGeneration, Kind: PositionKindTransactionEnd, CommitLSN: 30}
	candidateFirstTime := secondTime.Add(time.Hour)
	candidateSecondTime := candidateFirstTime.Add(time.Hour)
	candidateRowA := canonicalStringRowIdentity(tableA, fieldA, "candidate-alpha")
	candidateRowB := canonicalStringRowIdentity(tableB, fieldA, "candidate-beta")
	candidateAuthoritativeA := sampleRow(candidateRowA, &candidateFirstTime, nil)
	candidateAuthoritativeA.Checksum = Checksum{0: 1}
	candidateAuthoritativeB := sampleRow(candidateRowB, &candidateSecondTime, &deleteReason)
	candidateAuthoritativeB.Checksum = Checksum{0: 2}
	candidateFirstEvent := EventReplayKey{Transaction: TransactionReplayKey{StreamGeneration: candidateGeneration, CommitLSN: 30}, EventOrdinal: 1}
	candidateSecondEvent := EventReplayKey{Transaction: TransactionReplayKey{StreamGeneration: candidateGeneration, CommitLSN: 30}, EventOrdinal: 2}
	candidateProjectionA := ProjectionKey{Relation: relationA, Event: candidateFirstEvent, Image: ProjectionImageAfter}
	candidateProjectionB := ProjectionKey{Relation: relationB, Event: candidateSecondEvent, Image: ProjectionImageBefore}
	candidateStage := CandidateProjectionStage{
		RegistryGeneration: 3,
		Schema:             secondRef,
		StreamGeneration:   candidateGeneration,
		SnapshotBoundary:   candidateFirstPosition,
		ActivationBarrier:  candidateBarrier,
		Verified:           true,
		Rows: []CandidateRowEntry{
			{Identity: candidateRowB, Row: candidateAuthoritativeB},
			{Identity: candidateRowA, Row: candidateAuthoritativeA},
		},
		Projections: []CandidateProjectionEntry{
			{Key: candidateProjectionB, Projection: CapturedProjection{Event: candidateSecondEvent, Image: ProjectionImageBefore, Row: candidateRowB, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"candidate-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"candidate-a\""}}, Checksum: Checksum{0: 2}, CapturedAt: &candidateSecondTime}},
			{Key: candidateProjectionA, Projection: CapturedProjection{Event: candidateFirstEvent, Image: ProjectionImageAfter, Row: candidateRowA, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"candidate-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"candidate-a\""}}, Checksum: Checksum{0: 1}, CapturedAt: &candidateFirstTime}},
		},
		Fences: []CandidateFenceEntry{
			{ID: fenceD, Fence: VersionFence{ID: fenceD, RegistrationKind: RegistrationKindCaptureDependency, Relation: relationB, Physical: definitionB.Physical, Operation: DMLOperationInsert, DMLOrdinal: 2, HasNewRegisteredIdentity: true, NewRegisteredIdentity: RegisteredIdentity{Kind: RegistrationKindCaptureDependency, CaptureKey: CanonicalCaptureKey{CanonicalKeyBytes: "candidate-capture-b"}}, RowVersion: "candidate-version-b", HasEventReplayKey: true, EventReplayKey: candidateSecondEvent, Coverage: FenceCoverageMaterialized}},
			{ID: fenceC, Fence: VersionFence{ID: fenceC, RegistrationKind: RegistrationKindSynced, Relation: relationA, Physical: definitionA.Physical, Operation: DMLOperationUpdate, DMLOrdinal: 1, HasNewRegisteredIdentity: true, NewRegisteredIdentity: RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: candidateRowA}, RowVersion: "candidate-version-a", HasEventReplayKey: true, EventReplayKey: candidateFirstEvent, Coverage: FenceCoverageMaterialized}},
		},
		Scopes: []CandidateScopeEntry{
			{Scope: scopeD, State: ScopeState{Schema: secondRef, MembershipGeneration: 3, RetentionGeneration: 3, StreamGeneration: candidateGeneration, Membership: []ScopeMembership{{Row: candidateRowB, Generation: 3, Included: true}, {Row: candidateRowA, Generation: 3, Included: true}}, Effects: []ScopeEffect{{Position: candidateSecondPosition, Row: candidateRowB, Operation: EffectOperationUpsert, Checksum: Checksum{0: 2}, HasChecksum: true}, {Position: candidateFirstPosition, Row: candidateRowA, Operation: EffectOperationUpsert, Checksum: Checksum{0: 1}, HasChecksum: true}}, Cardinality: 2, Checksum: Checksum{0: 4}, HighWatermark: candidateSecondPosition}},
			{Scope: scopeC, State: ScopeState{Schema: firstRef, MembershipGeneration: 3, RetentionGeneration: 2, StreamGeneration: candidateGeneration, Membership: []ScopeMembership{{Row: candidateRowB, Generation: 3}, {Row: candidateRowA, Generation: 3, Included: true}}, Effects: []ScopeEffect{{Position: candidateSecondPosition, Row: candidateRowB, Operation: EffectOperationDelete, Checksum: Checksum{0: 6}, HasChecksum: true}, {Position: candidateFirstPosition, Row: candidateRowA, Operation: EffectOperationUpsert, Checksum: Checksum{0: 5}, HasChecksum: true}}, Cardinality: 1, Checksum: Checksum{0: 7}, HighWatermark: candidateSecondPosition}},
		},
	}
	bootstrapGeneration := StreamGeneration("stream-generation-3")
	bootstrapPosition := StreamPosition{StreamGeneration: bootstrapGeneration, Kind: PositionKindEffect, CommitLSN: 40, EventOrdinal: 1, EffectOrdinal: 1}
	bootstrapBarrier := StreamPosition{StreamGeneration: bootstrapGeneration, Kind: PositionKindTransactionEnd, CommitLSN: 40}
	bootstrapTime := candidateSecondTime.Add(time.Hour)
	bootstrapRow := canonicalStringRowIdentity(tableA, fieldA, "bootstrap-alpha")
	bootstrapAuthoritativeRow := sampleRow(bootstrapRow, &bootstrapTime, nil)
	bootstrapAuthoritativeRow.Checksum = Checksum{0: 8}
	bootstrapEvent := EventReplayKey{Transaction: TransactionReplayKey{StreamGeneration: bootstrapGeneration, CommitLSN: 40}, EventOrdinal: 1}
	bootstrapStage := CandidateProjectionStage{
		RegistryGeneration: 3,
		Schema:             secondRef,
		StreamGeneration:   bootstrapGeneration,
		SnapshotBoundary:   bootstrapPosition,
		ActivationBarrier:  bootstrapBarrier,
		Verified:           false,
		Rows:               []CandidateRowEntry{{Identity: bootstrapRow, Row: bootstrapAuthoritativeRow}},
		Projections:        []CandidateProjectionEntry{{Key: ProjectionKey{Relation: relationA, Event: bootstrapEvent, Image: ProjectionImageAfter}, Projection: CapturedProjection{Event: bootstrapEvent, Image: ProjectionImageAfter, Row: bootstrapRow, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"bootstrap-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"bootstrap-a\""}}, Checksum: Checksum{0: 8}, CapturedAt: &bootstrapTime}}},
		Fences:             []CandidateFenceEntry{{ID: fenceC, Fence: VersionFence{ID: fenceC, RegistrationKind: RegistrationKindSynced, Relation: relationA, Physical: definitionA.Physical, Operation: DMLOperationInsert, DMLOrdinal: 1, HasNewRegisteredIdentity: true, NewRegisteredIdentity: RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: bootstrapRow}, RowVersion: "bootstrap-version", HasEventReplayKey: true, EventReplayKey: bootstrapEvent, Coverage: FenceCoveragePending}}},
		Scopes:             []CandidateScopeEntry{{Scope: scopeC, State: ScopeState{Schema: secondRef, MembershipGeneration: 3, RetentionGeneration: 3, StreamGeneration: bootstrapGeneration, Membership: []ScopeMembership{{Row: bootstrapRow, Generation: 3, Included: true}}, Effects: []ScopeEffect{{Position: bootstrapPosition, Row: bootstrapRow, Operation: EffectOperationUpsert, Checksum: Checksum{0: 8}, HasChecksum: true}}, Cardinality: 1, Checksum: Checksum{0: 8}, HighWatermark: bootstrapPosition}}},
	}

	state := State{
		ProtocolVersion: 3,
		CurrentSchema:   secondRef,
		Schemas:         make(map[SchemaRef]SchemaManifest),
		Relations:       make(map[RelationID]RelationState),
		Clients:         make(map[ClientKey]ClientState),
		Rows:            make(map[RowIdentity]AuthoritativeRow),
		Scopes:          make(map[ScopeID]ScopeState),
		Fences:          make(map[FenceID]VersionFence),
		Projections:     make(map[ProjectionKey]CapturedProjection),
		Batches:         make(map[BatchKey]BatchLedger),
		Mutations:       make(map[MutationKey]MutationLedger),
		Rebuilds:        make(map[RebuildKey]RebuildSession),
		ClientLocal:     make(map[ClientKey]ClientLocalState),
		RetentionFloors: make(map[ScopeID]RetentionFloor),
		Registry: RegistryState{CurrentGeneration: 2, Generations: []RegistryGenerationState{
			{Generation: 2, ActivationBoundary: StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 10}, Validated: true, Relations: []RegistryRelation{{Definition: definitionB}, {Definition: definitionA}}, CaptureDependencies: []CaptureDependency{{ID: "e0000000-0000-4000-8000-000000000002", Relation: relationB, DependsOn: relationA}, {ID: "e0000000-0000-4000-8000-000000000001", Relation: relationA}}, ScopeRules: []ScopeRule{{ID: "f0000000-0000-4000-8000-000000000001", Relation: relationA, MembershipFunction: definitionA.MembershipFunction, PositiveFanoutBound: 1, Evaluations: []MembershipEvaluation{{Row: secondRow, Scopes: []ScopeID{scopeB, scopeA}}, {Row: firstRow, Scopes: []ScopeID{scopeB, scopeA}}}}}, DependencyImpacts: []DependencyImpact{{ID: "01000000-0000-4000-8000-000000000001", Relation: relationB, Function: definitionA.DependencyImpactFunction, CapturedFieldIDs: []FieldID{fieldB, fieldA}, PositiveRowBound: 1, AffectedRows: []RowIdentity{secondRow, firstRow}}}},
			{Generation: 3, ActivationBoundary: bootstrapBarrier, HasBootstrapStage: true, BootstrapStage: bootstrapStage},
			{Generation: 1, ActivationBoundary: StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart, CommitLSN: 1}, Validated: true},
		}},
		Stream: StreamState{
			Authority:  StreamAuthority{ActiveGeneration: generation, GlobalMaterializationBoundary: firstPosition, AcknowledgedEndLSN: 20, HasActiveSlot: true, ActiveSlot: "05000000-0000-4000-8000-000000000001"},
			Reset:      &StreamReset{ID: "a1000000-0000-4000-8000-000000000001", CandidateSlot: "05000000-0000-4000-8000-000000000002", CandidateSlotPermanent: true, Database: "synchro", Plugin: "pgoutput", ConsistentPoint: 30, SnapshotBoundary: secondPosition, ActivationBarrier: StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 20}, TargetStreamGeneration: "stream-generation-2", Phase: StreamResetPhaseAwaitingActivation, HasCandidateStage: true, CandidateStage: candidateStage},
			SourceRows: []SourceRowEntry{{Identity: secondRow, Row: sourceRowB}, {Identity: firstRow, Row: sourceRowA}},
			Transactions: []StreamTransaction{
				{ReplayKey: secondTransaction, End: StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 20}, EndLSN: 20, RegistryGeneration: 2, Lifecycle: TransactionLifecyclePoisoned, CommittedAt: &secondTime, Events: []SourceEvent{{ReplayKey: thirdEvent, Position: thirdPosition, Relation: relationA, Operation: DMLOperationDelete, HasBefore: true, Before: sourceAfter, HasAfter: true, After: sourceDeleted, CapturedAt: &secondTime}, {ReplayKey: secondEvent, Position: secondPosition, Relation: relationA, Operation: DMLOperationUpdate, HasBefore: true, Before: sourceBefore, HasAfter: true, After: sourceAfter, CapturedAt: &secondTime}}},
				{ReplayKey: firstTransaction, End: StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 10}, EndLSN: 10, RegistryGeneration: 1, Lifecycle: TransactionLifecycleMaterialized, CommittedAt: &firstTime, Events: []SourceEvent{{ReplayKey: firstEvent, Position: firstPosition, Relation: relationA, Operation: DMLOperationInsert, HasAfter: true, After: sourceAfter, CapturedAt: &firstTime}}},
			},
			TransactionReplays: []TransactionReplayRecord{{Key: secondTransaction, RegistryGeneration: 2, EndLSN: 20, Completed: true}, {Key: firstTransaction, RegistryGeneration: 1, EndLSN: 10, Completed: true, Replayed: true}},
			EventReplays:       []EventReplayRecord{{Key: thirdEvent}, {Key: secondEvent}, {Key: firstEvent, Replayed: true}},
			Materializations:   []MaterializationRecord{{Event: thirdEvent}, {Event: secondEvent}, {Event: firstEvent, Materialized: true}},
			Acknowledgements:   []SlotAcknowledgement{{StreamGeneration: generation, EndLSN: 20, AcknowledgedAt: &secondTime}, {StreamGeneration: generation, EndLSN: 10, AcknowledgedAt: &firstTime}},
			Poison:             []PoisonRecord{{Transaction: secondTransaction, HasRelation: true, Relation: relationA, Reason: "poison", Lifecycle: PoisonLifecycleActive, PoisonedAt: &secondTime}, {Transaction: firstTransaction, Lifecycle: PoisonLifecycleRepaired, PoisonedAt: &firstTime}},
		},
		Seed: SeedState{
			Exports: []SeedExport{
				{ID: exportB, TransactionNonce: "nonce-b", Schema: secondRef, RegistryGeneration: 2, StreamGeneration: generation, SnapshotBoundary: secondPosition, ManifestHash: secondRef.Hash, Status: SeedExportStatusComplete, CreatedAt: &secondTime, Scopes: []SeedScopeState{{Scope: scopeB, MembershipGeneration: 2, RetentionGeneration: 2, Cardinality: 2, HasReceipt: true, Receipt: secondToken}, {Scope: scopeA, MembershipGeneration: 1, RetentionGeneration: 1, Cardinality: 1, HasReceipt: true}}, Pages: []SeedPageState{{Scope: scopeB, NextRowOrdinal: 2, PageLimit: 100, HasToken: true}, {Scope: scopeA, NextRowOrdinal: 1, PageLimit: 100}}},
				{ID: exportA, TransactionNonce: "nonce-a", Schema: firstRef, RegistryGeneration: 1, StreamGeneration: generation, SnapshotBoundary: firstPosition, ManifestHash: firstRef.Hash, Status: SeedExportStatusComplete, CreatedAt: &firstTime, Scopes: []SeedScopeState{{Scope: scopeA, MembershipGeneration: 1, RetentionGeneration: 1, Cardinality: 1, HasReceipt: true, Receipt: firstToken}}, Pages: []SeedPageState{{Scope: scopeA, NextRowOrdinal: 1, PageLimit: 100, HasToken: true, Token: firstToken}}},
			},
			Records: []SeedRecord{{Key: SeedRecordKey{Export: exportB, Scope: scopeB, Ordinal: 2}, Row: sampleRow(secondRow, &secondTime, &deleteReason)}, {Key: SeedRecordKey{Export: exportA, Scope: scopeA, Ordinal: 1}, Row: sampleRow(firstRow, &firstTime, nil)}},
		},
		Authorization: AuthorizationState{Roles: []RoleCapabilities{{Role: "sync_runtime", Capabilities: []Capability{"write", "read"}}, {Role: "sync_worker", Capabilities: []Capability{"capture", "read"}}}},
		Installation:  InstallationCapabilities{Installed: true, SchemaName: "synchro", ExtensionVersion: "0.3.0", ProtocolVersion: 3, MinimumClientRuntime: 3, StaleClientIntervalMilliseconds: 86400000, Endpoints: []Endpoint{"/sync/pull", "/sync/connect"}, Capabilities: []InstallationCapability{{ID: "03000000-0000-4000-8000-000000000002", CheckedAt: &secondTime}, {ID: "03000000-0000-4000-8000-000000000001", Enabled: true, CheckedAt: &firstTime}}},
		Readiness:     ReadinessState{ConfiguredDatabase: "synchro", Workers: []WorkerReadiness{{ID: "04000000-0000-4000-8000-000000000002", Database: "synchro", HeartbeatAt: &secondTime, RegistryGeneration: 2, MaterializedPosition: secondPosition}, {ID: "04000000-0000-4000-8000-000000000001", Database: "synchro", Running: true, HeartbeatAt: &firstTime, RegistryGeneration: 1, MaterializedPosition: firstPosition}}, Slots: []SlotReadiness{{ID: "05000000-0000-4000-8000-000000000002", Database: "synchro", Plugin: "pgoutput", AcknowledgedEndLSN: 20}, {ID: "05000000-0000-4000-8000-000000000001", Database: "synchro", Plugin: "pgoutput", Active: true, AcknowledgedEndLSN: 10}}, Limits: []ReadinessLimit{{ID: "06000000-0000-4000-8000-000000000002", Finite: true}, {ID: "06000000-0000-4000-8000-000000000001", Value: 1, Finite: true}}, Checks: []ReadinessCheck{{ID: "07000000-0000-4000-8000-000000000002", State: ReadinessCheckUnknown, NumericObservation: 2, CheckedAt: &secondTime}, {ID: "07000000-0000-4000-8000-000000000001", State: ReadinessCheckOK, Reason: "ok", NumericObservation: 1, CheckedAt: &firstTime}}, Reasons: []ReasonCode{"worker_missing", "limit_exceeded"}},
		Events:        []ModelEvent{{Ordinal: 2, Kind: ModelEventResponseLoss, At: &secondTime, HasClient: true, Client: secondClient, HasTransaction: true, Transaction: secondTransaction, Reason: "response_lost"}, {Ordinal: 1, Kind: ModelEventWorkerRestart, At: &firstTime, HasTransaction: true, Transaction: firstTransaction, Reason: "worker_restart"}},
	}

	insertPair(reverse, func() {
		state.Schemas[firstRef] = SchemaManifest{Body: []byte("schema-a"), Class: SchemaClassInitial, CompatibilityFloor: 1, Tables: []TableManifest{{ID: tableA, Relation: relationA, Composition: CompositionSingleScope, PrimaryKeyFieldID: fieldA, CreatedFieldID: &createdField, UpdatedFieldID: &updatedField, DeletedFieldID: &deletedField, Fields: []FieldManifest{{ID: fieldB, PortableType: "string", Nullable: true, Writable: true, DefaultWireJSON: &defaultValue}, {ID: fieldA, PortableType: "string", PrimaryKey: true}}, Indexes: []IndexManifest{{ID: "09000000-0000-4000-8000-000000000002", Fields: []FieldID{fieldB, fieldA}}, {ID: "09000000-0000-4000-8000-000000000001"}}}}}
	}, func() {
		state.Schemas[secondRef] = SchemaManifest{Body: []byte("schema-b"), Parent: &firstRef, Class: SchemaClass2, CompatibilityFloor: 1}
	})
	insertPair(reverse, func() {
		state.Relations[relationA] = RelationState{Definition: definitionA, CaptureDependencies: []CaptureDependencyID{"e0000000-0000-4000-8000-000000000002", "e0000000-0000-4000-8000-000000000001"}, ScopeRules: []ScopeRuleID{"f0000000-0000-4000-8000-000000000002", "f0000000-0000-4000-8000-000000000001"}, DependencyImpacts: []DependencyImpactID{"01000000-0000-4000-8000-000000000002", "01000000-0000-4000-8000-000000000001"}}
	}, func() { state.Relations[relationB] = RelationState{Definition: definitionB} })
	insertPair(reverse, func() {
		state.Clients[firstClient] = ClientState{CurrentGeneration: 1, Generations: []ClientGenerationState{{Generation: 2, CreatedAt: &secondTime, LastCursorAcknowledgedAt: &secondTime, ExpiresAt: &secondTime}, {Generation: 1, CreatedAt: &firstTime}}, ScopeSetVersion: 2, ScopeAssignments: []ScopeAssignment{{Scope: scopeB, RebuildRequired: true}, {Scope: scopeA, MembershipGeneration: 1, RetentionGeneration: 1, Assigned: true}}, Checkpoints: []ClientCheckpoint{{Scope: scopeB, Position: secondPosition, HasCursor: true}, {Scope: scopeA, Position: firstPosition, HasCursor: true, Cursor: firstToken, HasChecksum: true, Verified: true}}, AcceptedWriteEpoch: 1}
	}, func() {
		state.Clients[secondClient] = ClientState{CurrentGeneration: 2, Generations: []ClientGenerationState{{Generation: 2, CreatedAt: &secondTime, LastCursorAcknowledgedAt: &secondTime, ExpiresAt: &secondTime}}, Retirement: &PermanentRetirement{RetiredAt: &secondTime, Reason: "retired"}, ScopeSetVersion: 1, AcceptedWriteEpoch: 2}
	})
	insertPair(reverse, func() { state.Rows[firstRow] = sampleRow(firstRow, &firstTime, nil) }, func() { state.Rows[secondRow] = sampleRow(secondRow, &secondTime, &deleteReason) })
	insertPair(reverse, func() {
		state.Scopes[scopeA] = ScopeState{Schema: secondRef, MembershipGeneration: 2, RetentionGeneration: 2, StreamGeneration: generation, Membership: []ScopeMembership{{Row: secondRow}, {Row: firstRow, Generation: 1, Included: true}}, Effects: []ScopeEffect{{Position: secondPosition, Row: secondRow, SourceEvent: secondEvent, Operation: EffectOperationUpsert, Version: "effect-version-b", HasCapturedProjection: true, CapturedProjection: ProjectionKey{Relation: relationA, Event: secondEvent, Image: ProjectionImageAfter}, HasChecksum: true, Checksum: Checksum{0: 15}}, {Position: firstPosition, Row: firstRow, SourceEvent: firstEvent, Operation: EffectOperationDelete, Version: "effect-version-a", HasCapturedProjection: true, CapturedProjection: ProjectionKey{Relation: relationA, Event: firstEvent, Image: ProjectionImageBefore}, HasChecksum: true, Checksum: Checksum{0: 16}}}, Cardinality: 2, Checksum: Checksum{0: 17}, HighWatermark: secondPosition}
	}, func() {
		state.Scopes[scopeB] = ScopeState{Schema: firstRef, MembershipGeneration: 1, RetentionGeneration: 1, StreamGeneration: generation, HighWatermark: firstPosition}
	})
	insertPair(reverse, func() {
		state.Fences[fenceA] = VersionFence{ID: fenceA, RegistrationKind: RegistrationKindSynced, Relation: relationA, Physical: definitionA.Physical, Operation: DMLOperationUpdate, DMLOrdinal: 1, HasOldRegisteredIdentity: true, OldRegisteredIdentity: firstRegisteredIdentity, HasNewRegisteredIdentity: true, NewRegisteredIdentity: firstRegisteredIdentity, RowVersion: "opaque-version-a", HasEventReplayKey: true, EventReplayKey: firstEvent, HasMutationKey: true, MutationKey: MutationKey{Client: firstClient, Mutation: mutationA}, Coverage: FenceCoverageMaterialized}
	}, func() {
		state.Fences[fenceB] = VersionFence{ID: fenceB, RegistrationKind: RegistrationKindCaptureDependency, Relation: relationB, Physical: definitionB.Physical, Operation: DMLOperationInsert, DMLOrdinal: 2, HasNewRegisteredIdentity: true, NewRegisteredIdentity: secondRegisteredIdentity, RowVersion: "opaque-version-b", HasEventReplayKey: true, EventReplayKey: secondEvent, Coverage: FenceCoverageResetBaseline, HasResetBaselineCoverage: true, ResetBaselineCoverage: ResetBaselineCoverage{ResetID: "a1000000-0000-4000-8000-000000000001", CandidateSlot: "05000000-0000-4000-8000-000000000002", SnapshotBoundary: secondPosition, TargetStreamGeneration: "stream-generation-2"}}
	})
	insertPair(reverse, func() {
		state.Projections[ProjectionKey{Relation: relationA, Event: firstEvent, Image: ProjectionImageBefore}] = CapturedProjection{Event: firstEvent, Image: ProjectionImageBefore, Row: firstRow, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"b\""}, {Field: fieldA, Type: "string", WireJSON: "\"a\""}}, Version: "projection-version-before", CapturedAt: &firstTime}
	}, func() {
		state.Projections[ProjectionKey{Relation: relationA, Event: firstEvent, Image: ProjectionImageAfter}] = CapturedProjection{Event: firstEvent, Image: ProjectionImageAfter, Row: firstRow, Version: "projection-version-after", CapturedAt: &secondTime}
	})
	insertPair(reverse, func() {
		state.Batches[BatchKey{Client: firstClient, Batch: batchA}] = BatchLedger{Fingerprint: FingerprintRecord{Algorithm: "sha-256", Version: 1, Domain: "batch", Digest: Fingerprint{0: 1}}, SealedCanonicalRequest: []byte("request-a"), SealedCanonicalResponse: []byte("response-a"), Execution: BatchExecutionCompleted, Mutations: []MutationID{mutationB, mutationA}, Outcomes: []MutationOutcome{{Mutation: mutationB, State: MutationOutcomeConflict, Response: []byte("outcome-b")}, {Mutation: mutationA, State: MutationOutcomeApplied, Response: []byte("outcome-a")}}, HTTPStatus: 200, ServerTime: &secondTime, SealedAt: &firstTime}
	}, func() {
		state.Batches[BatchKey{Client: secondClient, Batch: batchB}] = BatchLedger{Fingerprint: FingerprintRecord{Algorithm: "sha-256", Version: 1, Domain: "batch", Digest: Fingerprint{0: 2}}, SealedCanonicalRequest: []byte("request-b"), Execution: BatchExecutionExecuting, HTTPStatus: 202, ServerTime: &secondTime, SealedAt: &secondTime}
	})
	insertPair(reverse, func() {
		state.Mutations[MutationKey{Client: firstClient, Mutation: mutationA}] = MutationLedger{Fingerprint: FingerprintRecord{Algorithm: "sha-256", Version: 1, Domain: "mutation", Digest: Fingerprint{0: 3}}, FirstBatch: batchA, RequestOrdinal: 2, SealedCanonicalRequest: []byte("mutation-request-a"), Outcome: MutationOutcome{Mutation: mutationA, State: MutationOutcomeApplied, Response: []byte("outcome-a")}, ResolvedAt: &firstTime}
	}, func() {
		state.Mutations[MutationKey{Client: secondClient, Mutation: mutationB}] = MutationLedger{Fingerprint: FingerprintRecord{Algorithm: "sha-256", Version: 1, Domain: "mutation", Digest: Fingerprint{0: 4}}, FirstBatch: batchB, RequestOrdinal: 1, SealedCanonicalResponse: []byte("mutation-response-b"), Outcome: MutationOutcome{Mutation: mutationB, State: MutationOutcomeRejectedTerminal}, ResolvedAt: &secondTime}
	})
	insertPair(reverse, func() {
		state.Rebuilds[RebuildKey{Client: firstClient, Scope: scopeA, Rebuild: rebuildA}] = RebuildSession{SessionID: "b1000000-0000-4000-8000-000000000001", ClientGeneration: 1, Scope: scopeA, Schema: secondRef, MembershipGeneration: 2, RetentionGeneration: 2, StreamGeneration: generation, SnapshotBoundary: firstPosition, PageLimit: 100, StagedRows: []RebuildStagedRow{{Row: sampleRow(secondRow, &secondTime, &deleteReason), Ordinal: 2, StagedAt: &secondTime}, {Row: sampleRow(firstRow, &firstTime, nil), Ordinal: 1, StagedAt: &firstTime}}, HasContinuation: true, NextRowOrdinal: 3, CreatedAt: &firstTime, ExpiresAt: &secondTime, AcceptedWriteEpoch: 1, Pages: []RebuildPage{{Ordinal: 2, Rows: []AuthoritativeRow{sampleRow(secondRow, &secondTime, nil)}, HasToken: true, CanonicalResponse: []byte("rebuild-page-response-b"), HasContinuation: true, HasChecksum: true, Checksum: Checksum{0: 18}}, {Ordinal: 1, Rows: []AuthoritativeRow{sampleRow(firstRow, &firstTime, nil)}, CanonicalResponse: []byte("rebuild-page-response-a"), HasFinalCursor: true, HasChecksum: true, Checksum: Checksum{0: 19}}}, HasFinalCursor: true}
	}, func() {
		state.Rebuilds[RebuildKey{Client: secondClient, Scope: scopeB, Rebuild: rebuildB}] = RebuildSession{SessionID: "b1000000-0000-4000-8000-000000000002", ClientGeneration: 2, Scope: scopeB, Schema: firstRef, MembershipGeneration: 1, RetentionGeneration: 1, StreamGeneration: generation, SnapshotBoundary: secondPosition, PageLimit: 100, CreatedAt: &secondTime, AcceptedWriteEpoch: 2}
	})
	insertPair(reverse, func() {
		state.ClientLocal[firstClient] = ClientLocalState{CurrentSchema: secondRef, AuthoritativeScopeSetVersion: 2, ScopeAssignments: []LocalScopeAssignment{{Scope: scopeB, RebuildRequired: true}, {Scope: scopeA, MembershipGeneration: 1, RetentionGeneration: 1, Assigned: true}}, ScopeCheckpoints: []LocalScopeCheckpoint{{Scope: scopeB, Position: secondPosition, HasCursor: true}, {Scope: scopeA, Position: firstPosition, HasCursor: true, Cursor: firstToken, HasChecksum: true, Verified: true}}, Backoff: &DurableBackoff{InterruptedLifecycle: ClientLifecyclePulling, Work: ResumableWorkIdentity{Kind: ResumableWorkPull, HasScope: true, Scope: scopeA}, Retry: RetryClassificationTransport, Attempt: 2, NextEligibleAt: &secondTime}, Rows: []LocalRow{{Identity: secondRow, Fields: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"b\""}, {Field: fieldA, Type: "string", WireJSON: "\"a\""}}, UpdatedAt: &secondTime}, {Identity: firstRow, UpdatedAt: &firstTime}}, LocalOnlyRows: []LocalOnlyRow{{Key: LocalOnlyRowKey{Table: tableB, Row: "b0000000-0000-4000-8000-000000000002"}, UpdatedAt: &secondTime}, {Key: LocalOnlyRowKey{Table: tableA, Row: "b0000000-0000-4000-8000-000000000001"}, UpdatedAt: &firstTime}}, Provenance: []LocalProvenance{{Row: secondRow, Scopes: []ScopeID{scopeB, scopeA}, Version: "opaque-version-b"}, {Row: firstRow, Version: "opaque-version-a"}}, DurableQueue: []QueuedMutation{{Mutation: mutationB, Request: []byte("queue-b"), QueuedAt: &secondTime}, {Mutation: mutationA, Request: []byte("queue-a"), QueuedAt: &firstTime}}, Outcomes: []MutationOutcome{{Mutation: mutationB, State: MutationOutcomeConflict}, {Mutation: mutationA, State: MutationOutcomeApplied}}, SchemaJournal: []SchemaJournalEntry{{SourceSchema: firstRef, TargetSchema: secondRef, VerifiedTargetManifest: journalManifest, Action: SchemaActionRebuildLocal, AffectedScopes: []ScopeID{scopeB, scopeA}, MigrationPlan: []MigrationPlanOperation{{Kind: MigrationOperationDropField, Table: tableA, Field: fieldB}, {Kind: MigrationOperationAddField, Table: tableA, Field: fieldCreated}}, Phase: MigrationPhaseApplying, Ordinal: 2}, {SourceSchema: firstRef, TargetSchema: firstRef, VerifiedTargetManifest: journalManifest, Action: SchemaActionNone, Phase: MigrationPhaseApplied, Ordinal: 1}}, RebuildStaging: []LocalRebuildStage{{Rebuild: rebuildA, Ordinal: 2, Row: LocalRow{Identity: secondRow, UpdatedAt: &secondTime}}, {Rebuild: rebuildA, Ordinal: 1, Row: LocalRow{Identity: firstRow, UpdatedAt: &firstTime}}}, Lifecycle: ClientLifecycleState{State: ClientLifecycleReady, ChangedAt: &firstTime}}
	}, func() {
		state.ClientLocal[secondClient] = ClientLocalState{Lifecycle: ClientLifecycleState{State: ClientLifecycleStopped, ChangedAt: &secondTime}}
	})
	local := state.ClientLocal[firstClient]
	local.ClientGeneration = 1
	local.Rows[0].HasServerVersion = true
	local.Rows[0].ServerVersion = "local-server-version-b"
	local.Rows[0].HasChecksum = true
	local.Rows[0].Checksum = Checksum{0: 20}
	local.DurableQueue[0].Table = tableB
	local.DurableQueue[0].Row = secondRow
	local.DurableQueue[0].AuthoredSchema = secondRef
	local.DurableQueue[0].Operation = DMLOperationUpdate
	local.DurableQueue[0].ClientVersion = "client-version-b"
	local.DurableQueue[0].AuthoredColumns = []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"b\""}, {Field: fieldA, Type: "string", WireJSON: "\"a\""}}
	local.DurableQueue[0].LocalOrder = 2
	local.DurableQueue[0].Status = LocalMutationStatusPending
	local.DurableQueue[1].Table = tableA
	local.DurableQueue[1].Row = firstRow
	local.DurableQueue[1].AuthoredSchema = firstRef
	local.DurableQueue[1].Operation = DMLOperationDelete
	local.DurableQueue[1].HasBaseVersion = true
	local.DurableQueue[1].BaseVersion = "base-version-a"
	local.DurableQueue[1].ClientVersion = "client-version-a"
	local.DurableQueue[1].AuthoredColumns = nil
	local.DurableQueue[1].LocalOrder = 1
	local.DurableQueue[1].HasPredecessor = true
	local.DurableQueue[1].Predecessor = mutationB
	local.DurableQueue[1].Status = LocalMutationStatusSealed
	local.LocalOnlyRows[0].Fields = []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"local-only-b\""}, {Field: fieldA, Type: "string", WireJSON: "\"local-only-a\""}}
	local.SeedReceipts = []LocalSeedReceipt{
		{Scope: scopeB, HasReceipt: true, Receipt: secondToken, ExportID: exportB, ExportManifestHash: secondRef.Hash, Schema: secondRef, RegistryGeneration: 2, MembershipGeneration: 2, RetentionGeneration: 2, StreamGeneration: generation, SnapshotBoundary: secondPosition, Cardinality: 2},
		{Scope: scopeA, HasReceipt: true, ExportID: exportB, ExportManifestHash: secondRef.Hash, Schema: secondRef, RegistryGeneration: 2, MembershipGeneration: 1, RetentionGeneration: 1, StreamGeneration: generation, SnapshotBoundary: secondPosition, Cardinality: 1},
	}
	local.RebuildAttempts = []LocalRebuildAttempt{
		{Rebuild: rebuildB, Scope: scopeB, ClientGeneration: 1, Schema: firstRef, PageLimit: 100, AppliedPages: []AppliedRebuildPage{{RequestPageOrdinal: 1, AppliedAt: &secondTime}}, Phase: LocalRebuildAttemptPhaseApplying},
		{Rebuild: rebuildA, Scope: scopeA, ClientGeneration: 1, Schema: secondRef, PageLimit: 100, HasContinuation: true, AppliedPages: []AppliedRebuildPage{{RequestPageOrdinal: 2, HasRequestToken: true, RequestToken: secondToken, AppliedAt: &secondTime}, {RequestPageOrdinal: 1, HasRequestToken: true, AppliedAt: &firstTime}}, HasPendingFinalResult: true, PendingFinalResult: PendingRebuildFinalResult{HasFinalCursor: true, ScopeChecksum: Checksum{0: 9}, Cardinality: 2}, Phase: LocalRebuildAttemptPhasePendingFinality},
	}
	local.SealedBatches = []LocalSealedBatch{
		{Batch: batchB, ClientGeneration: 1, Schema: secondRef, Mutations: []MutationID{mutationB, mutationA}, CanonicalRequest: []byte("local-batch-request-b"), Fingerprint: FingerprintRecord{Algorithm: "sha-256", Version: 1, Domain: "local-batch", Digest: Fingerprint{0: 5}}, State: LocalSealedBatchStateResponseLost, HTTPStatus: 503, SealedAt: &secondTime},
		{Batch: batchA, ClientGeneration: 1, Schema: firstRef, Mutations: []MutationID{mutationA, mutationB}, CanonicalRequest: []byte("local-batch-request-a"), Fingerprint: FingerprintRecord{Algorithm: "sha-256", Version: 1, Domain: "local-batch", Digest: Fingerprint{0: 6}}, State: LocalSealedBatchStateReconciled, HasCanonicalResponse: true, CanonicalResponse: []byte("local-batch-response-a"), HTTPStatus: 200, SealedAt: &firstTime, ReconciledAt: &secondTime},
	}
	local.ErrorState = &ClientErrorState{Reason: "transport", Retryable: true, At: &secondTime}
	local.SchemaJournal[0].JournalVersion = 2
	local.SchemaJournal[0].MigrationPlanVersion = 3
	local.SchemaJournal[0].MigrationPlan = append(local.SchemaJournal[0].MigrationPlan,
		MigrationPlanOperation{Kind: MigrationOperationUpdateCursor},
		MigrationPlanOperation{Kind: MigrationOperationUpdateAssignment},
		MigrationPlanOperation{Kind: MigrationOperationUpdateChecksum},
		MigrationPlanOperation{Kind: MigrationOperationUpdateProvenance},
		MigrationPlanOperation{Kind: MigrationOperationUpdateSchemaMetadata},
	)
	state.ClientLocal[firstClient] = local
	insertPair(reverse, func() {
		state.RetentionFloors[scopeA] = RetentionFloor{MembershipGeneration: 1, RetentionGeneration: 1, StreamGeneration: generation, Position: firstPosition, ExpiresAt: &firstTime}
	}, func() {
		state.RetentionFloors[scopeB] = RetentionFloor{MembershipGeneration: 2, RetentionGeneration: 2, StreamGeneration: generation, Position: secondPosition, ExpiresAt: &secondTime}
	})
	return state
}

func insertPair(reverse bool, first, second func()) {
	if reverse {
		second()
		first()
		return
	}
	first()
	second()
}

func sampleRow(identity RowIdentity, at *time.Time, deleteReason *string) AuthoritativeRow {
	row := AuthoritativeRow{Identity: identity, FieldValues: []FieldValue{{Field: fieldB, Type: "string", WireJSON: "\"b\""}, {Field: fieldA, Type: "string", WireJSON: "\"a\""}}, Version: "opaque-version", Deleted: deleteReason != nil, DeleteReason: deleteReason, UpdatedAt: at}
	if deleteReason != nil {
		row.DeletedAt = at
	}
	return row
}

func schemaRef(version uint64, hashByte byte) SchemaRef {
	var hash [32]byte
	hash[0] = hashByte
	return SchemaRef{Version: version, Hash: hash}
}

// canonicalStringRowIdentity writes the Task 4 row-identity framing for a string key.
func canonicalStringRowIdentity(table TableID, field FieldID, value string) RowIdentity {
	wire, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	bytes := append([]byte("synchro:v3:row-identity:v1"), 0)
	bytes = appendFramedText(bytes, string(table))
	bytes = appendFramedText(bytes, string(field))
	bytes = append(bytes, 0x01, 0x01)
	bytes = appendFramedText(bytes, value)
	return RowIdentity{CanonicalIdentityBytes: string(bytes), TableID: table, PrimaryKeyFieldID: field, PortableType: "string", CanonicalWireJSON: string(wire)}
}

func appendFramedText(destination []byte, value string) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	destination = append(destination, length[:]...)
	return append(destination, value...)
}

func mutateEveryStateFamily(state *State) {
	state.ProtocolVersion = 99
	state.CurrentSchema = schemaRef(1, 1)
	schema := state.Schemas[schemaRef(1, 1)]
	schema.Body[0] = 'X'
	*schema.Tables[0].Fields[0].DefaultWireJSON = "changed"
	*schema.Tables[0].CreatedFieldID = fieldB
	*schema.Tables[0].UpdatedFieldID = fieldA
	*schema.Tables[0].DeletedFieldID = fieldB
	state.Schemas[schemaRef(1, 1)] = schema
	state.Registry.Generations[0].Relations[0].Definition.CapturedFieldIDs[0] = fieldA
	state.Registry.Generations[1].BootstrapStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	state.Registry.Generations[1].BootstrapStage.Projections[0].Projection.Fields[0].WireJSON = "\"changed\""
	state.Registry.Generations[1].BootstrapStage.Scopes[0].State.Membership[0].Included = false
	state.Registry.Generations[1].BootstrapStage.Scopes[0].State.Effects[0].Operation = EffectOperationDelete
	relation := state.Relations[relationA]
	relation.Definition.DependencyCapturedFieldIDs[0] = fieldA
	state.Relations[relationA] = relation
	client := state.Clients[ClientKey{UserID: userB, ClientID: clientBID}]
	*client.Retirement.RetiredAt = client.Retirement.RetiredAt.Add(time.Hour)
	*client.Generations[0].CreatedAt = client.Generations[0].CreatedAt.Add(time.Hour)
	*client.Generations[0].LastCursorAcknowledgedAt = client.Generations[0].LastCursorAcknowledgedAt.Add(time.Hour)
	*client.Generations[0].ExpiresAt = client.Generations[0].ExpiresAt.Add(time.Hour)
	state.Clients[ClientKey{UserID: userB, ClientID: clientBID}] = client
	firstClient := state.Clients[ClientKey{UserID: userA, ClientID: clientAID}]
	firstClient.ScopeAssignments[0].RebuildRequired = false
	firstClient.Checkpoints[0].HasChecksum = true
	state.Clients[ClientKey{UserID: userA, ClientID: clientAID}] = firstClient
	row := state.Rows[canonicalStringRowIdentity(tableB, fieldA, "beta")]
	*row.DeleteReason = "changed"
	state.Rows[row.Identity] = row
	scope := state.Scopes[scopeA]
	scope.Membership[0].Included = !scope.Membership[0].Included
	state.Scopes[scopeA] = scope
	fence := state.Fences[fenceA]
	fence.Coverage = FenceCoveragePending
	state.Fences[fenceA] = fence
	projection := state.Projections[ProjectionKey{Relation: relationA, Event: EventReplayKey{Transaction: TransactionReplayKey{StreamGeneration: "stream-generation-1", CommitLSN: 10}, EventOrdinal: 1}, Image: ProjectionImageBefore}]
	projection.Fields[0].WireJSON = "\"changed\""
	state.Projections[ProjectionKey{Relation: relationA, Event: projection.Event, Image: projection.Image}] = projection
	batchKey := BatchKey{Client: ClientKey{UserID: userA, ClientID: clientAID}, Batch: batchA}
	batch := state.Batches[batchKey]
	batch.SealedCanonicalRequest[0] = 'X'
	*batch.ServerTime = batch.ServerTime.Add(time.Hour)
	state.Batches[batchKey] = batch
	mutationKey := MutationKey{Client: ClientKey{UserID: userA, ClientID: clientAID}, Mutation: mutationA}
	mutation := state.Mutations[mutationKey]
	mutation.SealedCanonicalRequest[0] = 'X'
	state.Mutations[mutationKey] = mutation
	rebuildKey := RebuildKey{Client: ClientKey{UserID: userA, ClientID: clientAID}, Scope: scopeA, Rebuild: rebuildA}
	rebuild := state.Rebuilds[rebuildKey]
	rebuild.StagedRows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	rebuild.Pages[0].Rows[0].FieldValues[0].WireJSON = "\"changed\""
	*rebuild.Pages[0].Rows[0].UpdatedAt = rebuild.Pages[0].Rows[0].UpdatedAt.Add(time.Hour)
	*rebuild.CreatedAt = rebuild.CreatedAt.Add(time.Hour)
	rebuild.Pages[0].CanonicalResponse[0] = 'X'
	rebuild.HasContinuation = false
	rebuild.NextRowOrdinal++
	rebuild.Pages[0].HasToken = false
	state.Rebuilds[rebuildKey] = rebuild
	localKey := ClientKey{UserID: userA, ClientID: clientAID}
	local := state.ClientLocal[localKey]
	local.ScopeAssignments[0].RebuildRequired = false
	local.ScopeCheckpoints[0].HasChecksum = true
	local.SeedReceipts[0].HasReceipt = false
	local.RebuildAttempts[0].AppliedPages[0].HasRequestToken = true
	*local.RebuildAttempts[0].AppliedPages[0].AppliedAt = local.RebuildAttempts[0].AppliedPages[0].AppliedAt.Add(time.Hour)
	local.RebuildAttempts[1].PendingFinalResult.Cardinality++
	local.LocalOnlyRows[0].Fields[0].WireJSON = "\"changed\""
	*local.LocalOnlyRows[0].UpdatedAt = local.LocalOnlyRows[0].UpdatedAt.Add(time.Hour)
	local.Rows[0].HasChecksum = false
	local.DurableQueue[0].AuthoredColumns[0].WireJSON = "\"changed\""
	*local.Backoff.NextEligibleAt = local.Backoff.NextEligibleAt.Add(time.Hour)
	local.DurableQueue[0].Request[0] = 'X'
	local.SealedBatches[0].CanonicalRequest[0] = 'X'
	*local.SealedBatches[0].SealedAt = local.SealedBatches[0].SealedAt.Add(time.Hour)
	local.SealedBatches[1].CanonicalResponse[0] = 'X'
	*local.ErrorState.At = local.ErrorState.At.Add(time.Hour)
	local.SchemaJournal[0].VerifiedTargetManifest.Body[0] = 'X'
	local.SchemaJournal[0].MigrationPlan[0].Field = fieldA
	state.ClientLocal[localKey] = local
	floor := state.RetentionFloors[scopeA]
	*floor.ExpiresAt = floor.ExpiresAt.Add(time.Hour)
	state.RetentionFloors[scopeA] = floor
	state.Seed.Exports[0].Scopes[0].Cardinality++
	state.Seed.Exports[0].Scopes[0].HasReceipt = false
	state.Seed.Exports[0].Pages[0].NextRowOrdinal++
	state.Seed.Exports[0].Pages[0].HasToken = false
	state.Seed.Records[0].Row.FieldValues[0].WireJSON = "\"changed\""
	state.Authorization.Roles[0].Capabilities[0] = "changed"
	state.Installation.Endpoints[0] = "/sync/changed"
	*state.Installation.Capabilities[0].CheckedAt = state.Installation.Capabilities[0].CheckedAt.Add(time.Hour)
	*state.Readiness.Workers[0].HeartbeatAt = state.Readiness.Workers[0].HeartbeatAt.Add(time.Hour)
	state.Readiness.Checks[0].State = ReadinessCheckFailed
	*state.Readiness.Checks[0].CheckedAt = state.Readiness.Checks[0].CheckedAt.Add(time.Hour)
	*state.Events[0].At = state.Events[0].At.Add(time.Hour)
	state.Stream.SourceRows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	*state.Stream.SourceRows[0].Row.UpdatedAt = state.Stream.SourceRows[0].Row.UpdatedAt.Add(time.Hour)
	state.Stream.Transactions[0].Events[0].Before.Fields[0].WireJSON = "\"changed\""
	state.Stream.Transactions[0].Events[0].After.Fields[0].WireJSON = "\"changed\""
	*state.Stream.Transactions[0].Events[0].CapturedAt = state.Stream.Transactions[0].Events[0].CapturedAt.Add(time.Hour)
	*state.Stream.Poison[0].PoisonedAt = state.Stream.Poison[0].PoisonedAt.Add(time.Hour)
	state.Stream.Reset.Phase = StreamResetPhaseActive
	state.Stream.Reset.CandidateStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	*state.Stream.Reset.CandidateStage.Rows[0].Row.UpdatedAt = state.Stream.Reset.CandidateStage.Rows[0].Row.UpdatedAt.Add(time.Hour)
	state.Stream.Reset.CandidateStage.Projections[0].Projection.Fields[0].WireJSON = "\"changed\""
	*state.Stream.Reset.CandidateStage.Projections[0].Projection.CapturedAt = state.Stream.Reset.CandidateStage.Projections[0].Projection.CapturedAt.Add(time.Hour)
	state.Stream.Reset.CandidateStage.Scopes[0].State.Membership[0].Included = false
	state.Stream.Reset.CandidateStage.Scopes[0].State.Effects[0].Operation = EffectOperationDelete
	fence = state.Fences[fenceB]
	fence.ResetBaselineCoverage.TargetStreamGeneration = "stream-generation-3"
	state.Fences[fenceB] = fence

	clear(state.Schemas)
	clear(state.Relations)
	clear(state.Clients)
	clear(state.Rows)
	clear(state.Scopes)
	clear(state.Fences)
	clear(state.Projections)
	clear(state.Batches)
	clear(state.Mutations)
	clear(state.Rebuilds)
	clear(state.ClientLocal)
	clear(state.RetentionFloors)
}

func mutateSnapshotFamilies(snapshot *StateSnapshot) {
	snapshot.Schemas[0].Value.Body[0] = 'X'
	snapshot.Stream.Transactions[0].Events[0].Before.Fields[0].WireJSON = "\"changed\""
	snapshot.Stream.Transactions[0].Events[0].After.Fields[0].WireJSON = "\"changed\""
	*snapshot.Stream.Transactions[0].Events[0].CapturedAt = snapshot.Stream.Transactions[0].Events[0].CapturedAt.Add(time.Hour)
	snapshot.Stream.SourceRows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	*snapshot.Stream.SourceRows[0].Row.UpdatedAt = snapshot.Stream.SourceRows[0].Row.UpdatedAt.Add(time.Hour)
	snapshot.Batches[0].Value.SealedCanonicalRequest[0] = 'X'
	*snapshot.Batches[0].Value.ServerTime = snapshot.Batches[0].Value.ServerTime.Add(time.Hour)
	snapshot.Mutations[0].Value.Outcome.Response[0] = 'X'
	snapshot.ClientLocal[0].Value.DurableQueue[0].Request[0] = 'X'
	snapshot.ClientLocal[0].Value.DurableQueue[0].AuthoredColumns[0].WireJSON = "\"changed\""
	snapshot.ClientLocal[0].Value.SealedBatches[0].CanonicalResponse[0] = 'X'
	*snapshot.ClientLocal[0].Value.SealedBatches[0].ReconciledAt = snapshot.ClientLocal[0].Value.SealedBatches[0].ReconciledAt.Add(time.Hour)
	snapshot.ClientLocal[0].Value.SealedBatches[1].CanonicalRequest[0] = 'X'
	*snapshot.ClientLocal[0].Value.ErrorState.At = snapshot.ClientLocal[0].Value.ErrorState.At.Add(time.Hour)
	snapshot.ClientLocal[0].Value.SchemaJournal[0].VerifiedTargetManifest.Body[0] = 'X'
	snapshot.ClientLocal[0].Value.RebuildAttempts[0].AppliedPages[0].HasRequestToken = false
	*snapshot.ClientLocal[0].Value.RebuildAttempts[0].AppliedPages[0].AppliedAt = snapshot.ClientLocal[0].Value.RebuildAttempts[0].AppliedPages[0].AppliedAt.Add(time.Hour)
	snapshot.ClientLocal[0].Value.RebuildAttempts[0].PendingFinalResult.Cardinality++
	snapshot.ClientLocal[0].Value.LocalOnlyRows[1].Fields[0].WireJSON = "\"changed\""
	*snapshot.ClientLocal[0].Value.LocalOnlyRows[1].UpdatedAt = snapshot.ClientLocal[0].Value.LocalOnlyRows[1].UpdatedAt.Add(time.Hour)
	snapshot.Rebuilds[0].Value.HasContinuation = false
	snapshot.Rebuilds[0].Value.NextRowOrdinal++
	snapshot.Rebuilds[0].Value.Pages[0].HasToken = false
	snapshot.Rebuilds[0].Value.Pages[0].Rows[0].FieldValues[0].WireJSON = "\"changed\""
	*snapshot.Rebuilds[0].Value.Pages[0].Rows[0].UpdatedAt = snapshot.Rebuilds[0].Value.Pages[0].Rows[0].UpdatedAt.Add(time.Hour)
	*snapshot.Rebuilds[0].Value.CreatedAt = snapshot.Rebuilds[0].Value.CreatedAt.Add(time.Hour)
	snapshot.Rebuilds[0].Value.Pages[0].CanonicalResponse[0] = 'X'
	snapshot.ClientLocal[0].Value.SeedReceipts[0].HasReceipt = false
	snapshot.Seed.Exports[0].Scopes[0].HasReceipt = false
	snapshot.Seed.Exports[0].Pages[0].NextRowOrdinal++
	snapshot.Seed.Exports[0].Pages[0].HasToken = false
	snapshot.Seed.Records[0].Row.FieldValues[0].WireJSON = "\"changed\""
	snapshot.Stream.Reset.CandidateStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	*snapshot.Stream.Reset.CandidateStage.Rows[0].Row.UpdatedAt = snapshot.Stream.Reset.CandidateStage.Rows[0].Row.UpdatedAt.Add(time.Hour)
	snapshot.Stream.Reset.CandidateStage.Projections[0].Projection.Fields[0].WireJSON = "\"changed\""
	*snapshot.Stream.Reset.CandidateStage.Projections[0].Projection.CapturedAt = snapshot.Stream.Reset.CandidateStage.Projections[0].Projection.CapturedAt.Add(time.Hour)
	snapshot.Stream.Reset.CandidateStage.Scopes[0].State.Membership[0].Included = false
	snapshot.Stream.Reset.CandidateStage.Scopes[0].State.Effects[0].Operation = EffectOperationDelete
	snapshot.Registry.Generations[2].BootstrapStage.Rows[0].Row.FieldValues[0].WireJSON = "\"changed\""
	snapshot.Registry.Generations[2].BootstrapStage.Projections[0].Projection.Fields[0].WireJSON = "\"changed\""
	snapshot.Registry.Generations[2].BootstrapStage.Scopes[0].State.Effects[0].Operation = EffectOperationDelete
	snapshot.Readiness.Checks[0].State = ReadinessCheckFailed
	*snapshot.Readiness.Checks[0].CheckedAt = snapshot.Readiness.Checks[0].CheckedAt.Add(time.Hour)
	*snapshot.Events[0].At = snapshot.Events[0].At.Add(time.Hour)
}

func batchSnapshot(snapshot StateSnapshot, client ClientKey, batchID BatchID) BatchLedger {
	for _, entry := range snapshot.Batches {
		if entry.Key == (BatchKey{Client: client, Batch: batchID}) {
			return entry.Value
		}
	}
	panic("missing batch snapshot")
}

func rebuildSnapshot(snapshot StateSnapshot, client ClientKey, scope ScopeID, rebuildID RebuildID) RebuildSession {
	for _, entry := range snapshot.Rebuilds {
		if entry.Key == (RebuildKey{Client: client, Scope: scope, Rebuild: rebuildID}) {
			return entry.Value
		}
	}
	panic("missing rebuild snapshot")
}

func clientLocalSnapshot(snapshot StateSnapshot, client ClientKey) ClientLocalState {
	for _, entry := range snapshot.ClientLocal {
		if entry.Key == client {
			return entry.Value
		}
	}
	panic("missing local snapshot")
}
