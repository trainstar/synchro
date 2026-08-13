package reference

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	walOpsStream             StreamGeneration   = "wal-ops-stream"
	walOpsSyncedRelation     RelationID         = "wal-ops-synced-relation"
	walOpsDependencyRelation RelationID         = "wal-ops-dependency-relation"
	walOpsTable              TableID            = "wal-ops-table"
	walOpsPrimaryField       FieldID            = "wal-ops-primary-field"
	walOpsValueField         FieldID            = "wal-ops-value-field"
	walOpsDependencyField    FieldID            = "wal-ops-dependency-field"
	walOpsScopeA             ScopeID            = "wal-ops-scope-a"
	walOpsScopeB             ScopeID            = "wal-ops-scope-b"
	walOpsScopeC             ScopeID            = "wal-ops-scope-c"
	walOpsWorker             WorkerID           = "wal-ops-worker"
	walOpsSlot               SlotID             = "wal-ops-slot"
	walOpsClientA            ClientID           = "wal-ops-client-a"
	walOpsClientB            ClientID           = "wal-ops-client-b"
	walOpsUser               UserID             = "wal-ops-user"
	walOpsRule               ScopeRuleID        = "wal-ops-rule"
	walOpsImpact             DependencyImpactID = "wal-ops-impact"
)

type walOpsClock struct {
	now time.Time
}

func (clock *walOpsClock) Now() time.Time {
	result := clock.now
	clock.now = clock.now.Add(time.Second)
	return result
}

func TestWALCommitOrderOrdinalsEmptyTransactionAndRegistryReplay(t *testing.T) {
	row := walOpsRow("alpha")
	generationOne := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	generationTwo := walOpsGeneration(2, walOpsTransactionEnd(15), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	state := walOpsState(generationOne, generationTwo)
	state.Registry.CurrentGeneration = 2
	model := walOpsModel(t, state)

	latePayload := walOpsCommitPayload("20", "21", []walSourceEventPayload{
		walOpsEvent(7, walOpsSyncedRelation, DMLOperationUpdate, walOpsSyncedImage(row, "v1", "alpha-v1", false), walOpsSyncedImage(row, "v2", "alpha-v2", false)),
	})
	earlyPayload := walOpsCommitPayload("10", "11", []walSourceEventPayload{
		walOpsEvent(2, walOpsSyncedRelation, DMLOperationInsert, walOpsNullImage(), walOpsSyncedImage(row, "v1", "alpha-v1", false)),
	})
	walOpsApply(t, model, "model", "commit-source-transaction", latePayload)
	walOpsApply(t, model, "model", "commit-source-transaction", earlyPayload)

	snapshot := model.Snapshot()
	if len(snapshot.Stream.Transactions) != 2 || snapshot.Stream.Transactions[0].ReplayKey.CommitLSN != 10 || snapshot.Stream.Transactions[1].ReplayKey.CommitLSN != 20 {
		t.Fatal("arrival order changed committed source order")
	}
	if snapshot.Stream.Transactions[0].RegistryGeneration != 1 || snapshot.Stream.Transactions[1].RegistryGeneration != 2 {
		t.Fatal("source transactions did not select registry generations by activation boundary")
	}
	if snapshot.Stream.Transactions[0].Events[0].ReplayKey.EventOrdinal != 2 || snapshot.Stream.Transactions[1].Events[0].ReplayKey.EventOrdinal != 7 {
		t.Fatal("source event ordinal gaps were renumbered")
	}
	if len(snapshot.Stream.SourceRows) != 1 || snapshot.Stream.SourceRows[0].Row.Version != "v2" {
		t.Fatal("late arrival overwrote the latest commit-ordered source row")
	}

	beforeBlocked := model.Snapshot()
	walOpsApplyError(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("20", nil))
	if afterBlocked := model.Snapshot(); !reflect.DeepEqual(afterBlocked, beforeBlocked) {
		t.Fatal("out-of-order materialization changed durable state")
	}
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("20", nil))

	emptyResult := walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload("30", "31", []walSourceEventPayload{}))
	if emptyResult.WAL == nil || emptyResult.WAL.RegistryGeneration != 2 {
		t.Fatal("empty source transaction omitted its selected registry generation")
	}
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("30", nil))
	snapshot = model.Snapshot()
	if snapshot.Stream.Authority.GlobalMaterializationBoundary != walOpsTransactionEnd(30) || len(snapshot.Stream.TransactionReplays) != 3 || len(snapshot.Stream.EventReplays) != 2 {
		t.Fatal("empty source transaction did not advance only transaction-level progress")
	}
	scope := walOpsScopeSnapshot(t, snapshot, walOpsScopeA)
	if len(scope.Effects) != 2 || scope.Effects[0].SourceEvent.EventOrdinal != 2 || scope.Effects[1].SourceEvent.EventOrdinal != 7 || scope.Effects[1].Version != "v2" {
		t.Fatal("pull effects do not preserve source event order and versions")
	}

	walOpsApply(t, model, "model", "commit-source-transaction", earlyPayload)
	transaction := walOpsTransactionSnapshot(t, model.Snapshot(), 10)
	if transaction.RegistryGeneration != 1 {
		t.Fatal("source transaction replay changed its retained registry generation")
	}
}

func TestWALMaterializationFailureRollsBackAllPartialProjectionState(t *testing.T) {
	rowA := walOpsRow("atomic-a")
	rowB := walOpsRow("atomic-b")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{rowA: {walOpsScopeA}}, nil)
	model := walOpsModel(t, walOpsState(generation))
	events := []walSourceEventPayload{
		walOpsEvent(1, walOpsSyncedRelation, DMLOperationInsert, walOpsNullImage(), walOpsSyncedImage(rowA, "a-v1", "atomic-a", false)),
		walOpsEvent(4, walOpsSyncedRelation, DMLOperationInsert, walOpsNullImage(), walOpsSyncedImage(rowB, "b-v1", "atomic-b", false)),
	}
	walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload("10", "11", events))
	committed := model.Snapshot()
	result := walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	if result.WAL == nil || result.WAL.Poison != WALPoisonStatePoisoned {
		t.Fatal("scope evaluation failure did not create poison")
	}

	after := model.Snapshot()
	if len(after.Rows) != 0 || len(after.Projections) != 0 || len(after.Stream.TransactionReplays) != 0 || len(after.Stream.EventReplays) != 0 || len(after.Stream.Materializations) != 0 {
		t.Fatal("failed transaction exposed partial projection or replay state")
	}
	if len(walOpsScopeSnapshot(t, after, walOpsScopeA).Effects) != 0 || after.Stream.Authority.GlobalMaterializationBoundary != committed.Stream.Authority.GlobalMaterializationBoundary {
		t.Fatal("failed transaction exposed a partial effect or progress update")
	}
	for _, fence := range after.Fences {
		if fence.Value.Coverage != FenceCoveragePending || fence.Value.HasEventReplayKey {
			t.Fatal("failed transaction partially covered a source fence")
		}
	}
	if len(after.Stream.Poison) != 1 || after.Stream.Poison[0].Reason != "scope_evaluation_failed" {
		t.Fatal("failed transaction omitted bounded quarantine state")
	}
}

func TestWALReplayBeforeAcknowledgementIsIdempotent(t *testing.T) {
	row := walOpsRow("replay")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	model := walOpsModel(t, walOpsState(generation))
	walOpsCommitInsert(t, model, "10", "11", 3, row, "replay-v1")
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	first := model.Snapshot()
	if first.Stream.Authority.AcknowledgedEndLSN != 0 {
		t.Fatal("materialization acknowledged the logical slot")
	}

	result := walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	if result.WAL == nil || result.WAL.PriorMaterialization != result.WAL.NewMaterialization || result.WAL.NewAcknowledgement != 0 {
		t.Fatal("materialization replay changed progress or acknowledgement")
	}
	second := model.Snapshot()
	if len(second.Rows) != len(first.Rows) || len(second.Projections) != len(first.Projections) || len(walOpsScopeSnapshot(t, second, walOpsScopeA).Effects) != len(walOpsScopeSnapshot(t, first, walOpsScopeA).Effects) || len(second.Fences) != len(first.Fences) {
		t.Fatal("materialization replay duplicated durable work")
	}
	if !second.Stream.TransactionReplays[0].Replayed || !second.Stream.EventReplays[0].Replayed {
		t.Fatal("materialization replay identity was not observed")
	}
	walOpsApply(t, model, "process", "acknowledge-contiguous-prefix", map[string]any{"stream_generation": walOpsStream})
	acknowledged := model.Snapshot()
	if acknowledged.Stream.Authority.AcknowledgedEndLSN != 11 || len(acknowledged.Stream.Acknowledgements) != 1 || acknowledged.Stream.Acknowledgements[0].EndLSN != 11 {
		t.Fatal("acknowledgement did not use the transaction end LSN")
	}
}

func TestWALAcknowledgementStopsAtBlockedContiguousPrefix(t *testing.T) {
	rowA := walOpsRow("ack-a")
	rowB := walOpsRow("ack-b")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{
		rowA: {walOpsScopeA},
		rowB: {walOpsScopeA},
	}, nil)
	model := walOpsModel(t, walOpsState(generation))
	walOpsCommitInsert(t, model, "10", "11", 0, rowA, "ack-a-v1")
	walOpsCommitInsert(t, model, "20", "21", 0, rowB, "ack-b-v1")
	failure := ReasonCode("decode_failed")
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", &failure))

	model.state.Stream.TransactionReplays = append(model.state.Stream.TransactionReplays, TransactionReplayRecord{
		Key:                TransactionReplayKey{StreamGeneration: walOpsStream, CommitLSN: 20},
		RegistryGeneration: 1,
		EndLSN:             21,
		Completed:          true,
	})
	for index := range model.state.Stream.Transactions {
		if model.state.Stream.Transactions[index].ReplayKey.CommitLSN == 20 {
			model.state.Stream.Transactions[index].Lifecycle = TransactionLifecycleMaterialized
		}
	}
	walOpsApply(t, model, "process", "acknowledge-contiguous-prefix", map[string]any{"stream_generation": walOpsStream})
	if model.Snapshot().Stream.Authority.AcknowledgedEndLSN != 0 {
		t.Fatal("acknowledgement advanced around an earlier poisoned transaction")
	}

	walOpsApply(t, model, "process", "repair-and-retry-source-transaction", map[string]any{"stream_generation": walOpsStream, "commit_lsn": "10"})
	walOpsApply(t, model, "process", "acknowledge-contiguous-prefix", map[string]any{"stream_generation": walOpsStream})
	if got := model.Snapshot().Stream.Authority.AcknowledgedEndLSN; got != 21 {
		t.Fatalf("contiguous acknowledgement = %d, want 21", got)
	}
}

func TestWALPoisonSurvivesRestartAndSameIdentityRepair(t *testing.T) {
	rowA := walOpsRow("poison-a")
	rowB := walOpsRow("poison-b")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{
		rowA: {walOpsScopeA},
		rowB: {walOpsScopeA},
	}, nil)
	model := walOpsModel(t, walOpsState(generation))
	walOpsCommitInsert(t, model, "10", "11", 0, rowA, "poison-a-v1")
	walOpsCommitInsert(t, model, "20", "21", 0, rowB, "poison-b-v1")
	failure := ReasonCode("decode_failed")
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", &failure))
	walOpsApplyError(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("20", nil))

	beforeRestart := model.Snapshot()
	result := walOpsApply(t, model, "process", "restart-wal-worker", map[string]any{"worker_id": walOpsWorker})
	afterRestart := model.Snapshot()
	if result.WAL == nil || result.WAL.Poison != WALPoisonStatePoisoned || !reflect.DeepEqual(afterRestart.Stream, beforeRestart.Stream) || !reflect.DeepEqual(afterRestart.Rows, beforeRestart.Rows) || !reflect.DeepEqual(afterRestart.Scopes, beforeRestart.Scopes) || !reflect.DeepEqual(afterRestart.Fences, beforeRestart.Fences) {
		t.Fatal("worker restart changed poison or materialized state")
	}
	if len(afterRestart.Events) != len(beforeRestart.Events)+1 || afterRestart.Events[len(afterRestart.Events)-1].Kind != ModelEventWorkerRestart {
		t.Fatal("worker restart omitted its bounded model event")
	}

	repair := walOpsApply(t, model, "process", "repair-and-retry-source-transaction", map[string]any{"stream_generation": walOpsStream, "commit_lsn": "10"})
	if repair.WAL == nil || repair.WAL.Poison != WALPoisonStateRepaired || repair.WAL.Transaction.CommitLSN != 10 {
		t.Fatal("repair did not retry the poisoned transaction identity")
	}
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("20", nil))
	afterRepair := model.Snapshot()
	if afterRepair.Stream.Poison[0].Lifecycle != PoisonLifecycleRepaired || len(afterRepair.Rows) != 2 || afterRepair.Stream.Authority.GlobalMaterializationBoundary.CommitLSN != 20 {
		t.Fatal("repair did not unblock later transaction processing")
	}
}

func TestWALTruncateCreatesBlockingPoisonWithoutFenceOrProjection(t *testing.T) {
	row := walOpsRow("truncate-rule")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {}}, nil)
	model := walOpsModel(t, walOpsState(generation))
	event := walOpsEvent(5, walOpsSyncedRelation, DMLOperation(walTruncateOperation), walOpsNullImage(), walOpsNullImage())
	walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload("10", "11", []walSourceEventPayload{event}))
	result := walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	snapshot := model.Snapshot()
	if result.WAL == nil || result.WAL.Poison != WALPoisonStatePoisoned || len(snapshot.Stream.Poison) != 1 || snapshot.Stream.Poison[0].Reason != "truncate_unsupported" {
		t.Fatal("truncate did not create bounded blocking poison")
	}
	if len(snapshot.Projections) != 0 || len(snapshot.Fences) != 0 || len(snapshot.Stream.TransactionReplays) != 0 {
		t.Fatal("truncate created partial materialization state")
	}
}

func TestWALRepeatedSameRowEventsProduceOneGreatestCausalEffect(t *testing.T) {
	row := walOpsRow("repeated")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	model := walOpsModel(t, walOpsState(generation))
	events := []walSourceEventPayload{
		walOpsEvent(2, walOpsSyncedRelation, DMLOperationInsert, walOpsNullImage(), walOpsSyncedImage(row, "repeat-v1", "repeat-one", false)),
		walOpsEvent(9, walOpsSyncedRelation, DMLOperationUpdate, walOpsSyncedImage(row, "repeat-v1", "repeat-one", false), walOpsSyncedImage(row, "repeat-v2", "repeat-two", false)),
	}
	walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload("10", "11", events))
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	snapshot := model.Snapshot()
	scope := walOpsScopeSnapshot(t, snapshot, walOpsScopeA)
	if len(scope.Effects) != 1 || scope.Effects[0].SourceEvent.EventOrdinal != 9 || scope.Effects[0].Position.EventOrdinal != 9 || scope.Effects[0].Position.EffectOrdinal != 0 || scope.Effects[0].Version != "repeat-v2" {
		t.Fatal("repeated same-row events did not collapse to the greatest causal event")
	}
	if scope.Cardinality != 1 || len(snapshot.Rows) != 1 || snapshot.Rows[0].Value.Version != "repeat-v2" || len(snapshot.Projections) != 3 || len(snapshot.Fences) != 2 {
		t.Fatal("repeated same-row materialization lost or duplicated transaction state")
	}
	for _, fence := range snapshot.Fences {
		if fence.Value.Coverage != FenceCoverageMaterialized || !fence.Value.HasEventReplayKey {
			t.Fatal("repeated same-row event did not cover each distinct fence")
		}
	}
	if EffectOperationDeleteRank != 0 || EffectOperationUpsertRank != 1 {
		t.Fatal("effect operation ranks differ from the closed ordering contract")
	}
}

func TestWALDependencyChangesEnterAndLeaveScopesUsingCurrentProjectedVersion(t *testing.T) {
	row := walOpsRow("dependency")
	impact := []DependencyImpact{{
		ID:               walOpsImpact,
		Relation:         walOpsDependencyRelation,
		Function:         "wal-ops-impact-function",
		CapturedFieldIDs: []FieldID{walOpsDependencyField},
		PositiveRowBound: 1,
		AffectedRows:     []RowIdentity{row},
	}}
	generationOne := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	generationTwo := walOpsGeneration(2, walOpsTransactionEnd(10), map[RowIdentity][]ScopeID{row: {walOpsScopeB}}, impact)
	generationThree := walOpsGeneration(3, walOpsTransactionEnd(20), map[RowIdentity][]ScopeID{row: {}}, impact)
	state := walOpsState(generationOne, generationTwo, generationThree)
	state.Registry.CurrentGeneration = 3
	model := walOpsModel(t, state)

	walOpsCommitInsert(t, model, "10", "11", 1, row, "dependency-v1")
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	dependencyInsert := walOpsEvent(5, walOpsDependencyRelation, DMLOperationInsert, walOpsNullImage(), walOpsDependencyImage("owner", "dependency-d1"))
	walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload("20", "21", []walSourceEventPayload{dependencyInsert}))
	result := walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("20", nil))
	if result.WAL == nil || !reflect.DeepEqual(result.WAL.AffectedScopes, []ScopeID{walOpsScopeA, walOpsScopeB}) {
		t.Fatal("dependency reassignment did not report both affected scopes")
	}

	dependencyUpdate := walOpsEvent(8, walOpsDependencyRelation, DMLOperationUpdate, walOpsDependencyImage("owner", "dependency-d1"), walOpsDependencyImage("owner", "dependency-d2"))
	walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload("30", "31", []walSourceEventPayload{dependencyUpdate}))
	walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("30", nil))
	snapshot := model.Snapshot()
	if len(snapshot.Rows) != 1 || snapshot.Rows[0].Value.Version != "dependency-v1" {
		t.Fatal("dependency-only membership work allocated or changed a synchronized row version")
	}
	scopeA := walOpsScopeSnapshot(t, snapshot, walOpsScopeA)
	scopeB := walOpsScopeSnapshot(t, snapshot, walOpsScopeB)
	if len(scopeA.Effects) != 2 || scopeA.Effects[1].Operation != EffectOperationDelete || scopeA.Effects[1].Version != "dependency-v1" || scopeA.Effects[1].SourceEvent.EventOrdinal != 5 {
		t.Fatal("dependency-driven scope leaving did not use the current projected row version")
	}
	if len(scopeB.Effects) != 2 || scopeB.Effects[0].Operation != EffectOperationUpsert || scopeB.Effects[0].Version != "dependency-v1" || scopeB.Effects[0].SourceEvent.EventOrdinal != 5 || scopeB.Effects[1].Operation != EffectOperationDelete || scopeB.Effects[1].Version != "dependency-v1" || scopeB.Effects[1].SourceEvent.EventOrdinal != 8 {
		t.Fatal("dependency-driven scope entering or leaving lost its causal event")
	}
	if scopeA.Cardinality != 0 || scopeB.Cardinality != 0 {
		t.Fatal("dependency-driven leaving retained stale membership")
	}
}

func TestWALMultiScopeEffectsCardinalityAndChecksumsRemainScopeLocal(t *testing.T) {
	row := walOpsRow("multi")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeB, walOpsScopeA}}, nil)
	model := walOpsModel(t, walOpsState(generation))
	result := walOpsCommitInsert(t, model, "10", "11", 6, row, "multi-v1")
	if result.WAL == nil || len(result.WAL.AffectedScopes) != 0 {
		t.Fatal("source commit created a pull-visible scope effect")
	}
	result = walOpsApply(t, model, "process", "materialize-source-transaction", walOpsMaterializePayload("10", nil))
	if result.WAL == nil || !reflect.DeepEqual(result.WAL.AffectedScopes, []ScopeID{walOpsScopeA, walOpsScopeB}) {
		t.Fatal("multi-scope materialization collapsed affected scopes")
	}
	snapshot := model.Snapshot()
	rowChecksum := snapshot.Rows[0].Value.Checksum
	for _, scopeID := range []ScopeID{walOpsScopeA, walOpsScopeB} {
		scope := walOpsScopeSnapshot(t, snapshot, scopeID)
		if scope.Cardinality != 1 || len(scope.Effects) != 1 || scope.Effects[0].Operation != EffectOperationUpsert || scope.Effects[0].SourceEvent.EventOrdinal != 6 {
			t.Fatalf("scope %q has incomplete multi-scope materialization", scopeID)
		}
		want := walOpsIndependentScopeChecksum(scope.Schema.Hash, scopeID, []walOpsDigestRow{{identity: row.CanonicalIdentityBytes, checksum: rowChecksum}})
		if scope.Checksum != want {
			t.Fatalf("scope %q checksum does not bind its exact row set", scopeID)
		}
	}
	if walOpsScopeSnapshot(t, snapshot, walOpsScopeA).Checksum == walOpsScopeSnapshot(t, snapshot, walOpsScopeB).Checksum {
		t.Fatal("scope checksums collapsed distinct scope identities")
	}
}

func TestMembershipGenerationStagesWithoutVisibilityAndActivatesOnlyAffectedScopes(t *testing.T) {
	rowA := walOpsRow("member-a")
	rowB := walOpsRow("member-b")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{
		rowA: {walOpsScopeA},
		rowB: {walOpsScopeB},
	}, nil)
	state := walOpsState(generation)
	state.Stream.Authority.GlobalMaterializationBoundary = walOpsTransactionEnd(10)
	state.Rows[rowA] = walOpsAuthoritativeRow(rowA, "member-a-v1", "member-a")
	state.Rows[rowB] = walOpsAuthoritativeRow(rowB, "member-b-v1", "member-b")
	walOpsInstallMembership(&state, walOpsScopeA, rowA)
	walOpsInstallMembership(&state, walOpsScopeB, rowB)
	clientKey := ClientKey{UserID: walOpsUser, ClientID: walOpsClientA}
	state.Clients[clientKey] = ClientState{
		CurrentGeneration: 1,
		ScopeSetVersion:   7,
		ScopeAssignments: []ScopeAssignment{
			{Scope: walOpsScopeA, MembershipGeneration: 1, RetentionGeneration: 1, Assigned: true},
			{Scope: walOpsScopeB, MembershipGeneration: 1, RetentionGeneration: 1, Assigned: true},
			{Scope: walOpsScopeC, MembershipGeneration: 1, RetentionGeneration: 1, Assigned: true},
		},
		Checkpoints: []ClientCheckpoint{
			{Scope: walOpsScopeA, Position: walOpsTransactionEnd(10), Verified: true},
			{Scope: walOpsScopeB, Position: walOpsTransactionEnd(10), Verified: true},
			{Scope: walOpsScopeC, Position: walOpsTransactionEnd(10), Verified: true},
		},
	}
	rebuildA := RebuildKey{Client: clientKey, Scope: walOpsScopeA, Rebuild: "wal-ops-rebuild-a"}
	rebuildB := RebuildKey{Client: clientKey, Scope: walOpsScopeB, Rebuild: "wal-ops-rebuild-b"}
	state.Rebuilds[rebuildA] = RebuildSession{Scope: walOpsScopeA, Status: RebuildStatusStaged}
	state.Rebuilds[rebuildB] = RebuildSession{Scope: walOpsScopeB, Status: RebuildStatusStaged}
	model := walOpsModel(t, state)
	beforeStage := model.Snapshot()

	stagePayload := walOpsStageMembershipPayload(2, 2, []ScopeID{walOpsScopeA}, map[RowIdentity][]ScopeID{
		rowA: {},
		rowB: {walOpsScopeB},
	}, nil)
	stageResult := walOpsApply(t, model, "model", "stage-registry-membership-generation", stagePayload)
	afterStage := model.Snapshot()
	if stageResult.Schema == nil || stageResult.Schema.Action != SchemaActionNone || afterStage.Registry.CurrentGeneration != 1 {
		t.Fatal("stage operation exposed the candidate generation as active")
	}
	if !reflect.DeepEqual(afterStage.Scopes, beforeStage.Scopes) || !reflect.DeepEqual(afterStage.Clients, beforeStage.Clients) || !reflect.DeepEqual(afterStage.Rebuilds, beforeStage.Rebuilds) {
		t.Fatal("stage operation exposed candidate membership before activation")
	}
	if len(afterStage.Registry.Generations) != 2 || !afterStage.Registry.Generations[1].HasBootstrapStage || !afterStage.Registry.Generations[1].BootstrapStage.Verified {
		t.Fatal("stage operation did not retain an isolated verified candidate")
	}

	activateResult := walOpsApply(t, model, "model", "activate-registry-membership-generation", map[string]any{"registry_generation": 2})
	after := model.Snapshot()
	if activateResult.Schema == nil || activateResult.Schema.Action != SchemaActionRebuildLocal || !reflect.DeepEqual(activateResult.Schema.AffectedScopes, []ScopeID{walOpsScopeA}) {
		t.Fatal("activation did not report exactly the affected scope")
	}
	if after.Registry.CurrentGeneration != 2 || len(after.Registry.Generations) != 2 || after.Registry.Generations[1].HasBootstrapStage {
		t.Fatal("activation did not retain old registry history and activate the candidate")
	}
	if scope := walOpsScopeSnapshot(t, after, walOpsScopeA); scope.MembershipGeneration != 2 || scope.Cardinality != 0 {
		t.Fatal("affected scope did not activate the staged membership generation")
	}
	if !reflect.DeepEqual(walOpsScopeSnapshot(t, after, walOpsScopeB), walOpsScopeSnapshot(t, beforeStage, walOpsScopeB)) || !reflect.DeepEqual(walOpsScopeSnapshot(t, after, walOpsScopeC), walOpsScopeSnapshot(t, beforeStage, walOpsScopeC)) {
		t.Fatal("membership activation changed an unrelated scope")
	}
	client := walOpsClientSnapshot(t, after, clientKey)
	if client.ScopeSetVersion != 7 || !client.ScopeAssignments[0].RebuildRequired || client.ScopeAssignments[0].MembershipGeneration != 2 || client.ScopeAssignments[1].RebuildRequired || client.ScopeAssignments[2].RebuildRequired {
		t.Fatal("membership activation invalidated the wrong client scope")
	}
	if len(client.Checkpoints) != 2 || client.Checkpoints[0].Scope != walOpsScopeB || client.Checkpoints[1].Scope != walOpsScopeC {
		t.Fatal("membership activation removed an unrelated client checkpoint")
	}
	if walOpsRebuildSnapshot(t, after, rebuildA).Status != RebuildStatusInvalidated || walOpsRebuildSnapshot(t, after, rebuildB).Status != RebuildStatusStaged {
		t.Fatal("membership activation invalidated an unrelated rebuild session")
	}
}

func TestMembershipGenerationAdministrativeLimits(t *testing.T) {
	for _, test := range []struct {
		name       string
		batchSize  uint64
		batchSet   bool
		fanout     uint64
		impactRows uint64
		valid      bool
	}{
		{name: "batch size one", batchSize: 1, batchSet: true, fanout: 1, impactRows: 1, valid: true},
		{name: "batch size maximum", batchSize: 1000, batchSet: true, fanout: 8, impactRows: 1000, valid: true},
		{name: "batch size is required", fanout: 8, impactRows: 1000},
		{name: "batch size exceeds maximum", batchSize: 1001, batchSet: true, fanout: 8, impactRows: 1000},
		{name: "scope fanout exceeds maximum", batchSize: 1, batchSet: true, fanout: 9, impactRows: 1},
		{name: "dependency impact exceeds maximum", batchSize: 1, batchSet: true, fanout: 1, impactRows: 1001},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := []RowIdentity{walOpsRow("boundary-a"), walOpsRow("boundary-b"), walOpsRow("boundary-c")}
			evaluations := make(map[RowIdentity][]ScopeID, len(rows))
			for _, row := range rows {
				evaluations[row] = []ScopeID{walOpsScopeA}
			}
			generation := walOpsGeneration(1, walOpsGenerationStart(), evaluations, nil)
			state := walOpsState(generation)
			state.Stream.Authority.GlobalMaterializationBoundary = walOpsTransactionEnd(10)
			for _, row := range rows {
				state.Rows[row] = walOpsAuthoritativeRow(row, "boundary-v1", string(row.CanonicalWireJSON[1:len(row.CanonicalWireJSON)-1]))
			}
			model := walOpsModel(t, state)
			payload := walOpsStageMembershipPayload(2, 2, []ScopeID{walOpsScopeA}, evaluations, []DependencyImpact{{
				ID:               walOpsImpact,
				Relation:         walOpsDependencyRelation,
				Function:         "wal-ops-impact-function",
				CapturedFieldIDs: []FieldID{walOpsDependencyField},
				PositiveRowBound: test.impactRows,
				AffectedRows:     []RowIdentity{rows[0]},
			}})
			if test.batchSet {
				payload.BatchSize = &test.batchSize
			} else {
				payload.BatchSize = nil
			}
			(*payload.ScopeRules)[0].PositiveFanoutBound = &test.fanout

			before := model.Snapshot()
			result, err := model.Apply(context.Background(), scenarios.Operation{
				ContractOperation: "model",
				Name:              "stage-registry-membership-generation",
				Payload:           walOpsMarshalPayload(t, payload),
			})
			if !test.valid {
				if err == nil {
					t.Fatal("stage operation accepted an invalid administrative limit")
				}
				var coded interface{ ErrorCode() string }
				if !errors.As(err, &coded) || coded.ErrorCode() != "invalid_limit" {
					t.Fatalf("stage operation error = %v, want invalid_limit", err)
				}
				if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatal("invalid administrative limit changed model state")
				}
				return
			}
			if err != nil {
				t.Fatalf("stage operation failed: %v", err)
			}
			wantedBatchCount := uint64(1)
			if test.batchSize == 1 {
				wantedBatchCount = 3
			}
			if result.Schema == nil || result.Schema.Reason != "membership_generation_staged" || result.Schema.BatchSize != test.batchSize || result.Schema.BatchCount != wantedBatchCount {
				t.Fatalf("stage observation = %#v", result.Schema)
			}
			stage := model.Snapshot().Registry.Generations[1].BootstrapStage
			if len(stage.Scopes) != 1 || stage.Scopes[0].State.Cardinality != 3 {
				t.Fatal("stage operation did not backfill all candidate rows")
			}
		})
	}
}

func TestWALAndMembershipPayloadsAreClosedAndRollbackStrictly(t *testing.T) {
	row := walOpsRow("strict")
	generation := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	validCommit := walOpsCommitPayload("10", "11", []walSourceEventPayload{
		walOpsEvent(0, walOpsSyncedRelation, DMLOperationInsert, walOpsNullImage(), walOpsSyncedImage(row, "strict-v1", "strict", false)),
	})

	mutations := map[string]func(map[string]any){
		"unknown root member": func(document map[string]any) {
			document["unknown"] = true
		},
		"unknown nested image member": func(document map[string]any) {
			events := document["events"].([]any)
			after := events[0].(map[string]any)["after"].(map[string]any)
			after["unknown"] = true
		},
		"omitted nullable image": func(document map[string]any) {
			events := document["events"].([]any)
			delete(events[0].(map[string]any), "before")
		},
		"invalid insert shape": func(document map[string]any) {
			events := document["events"].([]any)
			events[0].(map[string]any)["before"] = events[0].(map[string]any)["after"]
		},
		"noncanonical commit LSN": func(document map[string]any) {
			document["commit_lsn"] = "010"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			model := walOpsModel(t, walOpsState(generation))
			before := model.Snapshot()
			document := walOpsDecodeObject(t, validCommit)
			mutate(document)
			walOpsApplyError(t, model, "model", "commit-source-transaction", document)
			if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatal("invalid commit payload changed model state")
			}
		})
	}

	t.Run("unknown nested membership member", func(t *testing.T) {
		state := walOpsState(generation)
		state.Stream.Authority.GlobalMaterializationBoundary = walOpsTransactionEnd(10)
		state.Rows[row] = walOpsAuthoritativeRow(row, "strict-v1", "strict")
		walOpsInstallMembership(&state, walOpsScopeA, row)
		model := walOpsModel(t, state)
		before := model.Snapshot()
		payload := walOpsStageMembershipPayload(2, 2, []ScopeID{walOpsScopeA}, map[RowIdentity][]ScopeID{row: {}}, nil)
		document := walOpsDecodeObject(t, payload)
		rules := document["scope_rules"].([]any)
		rules[0].(map[string]any)["unknown"] = true
		walOpsApplyError(t, model, "model", "stage-registry-membership-generation", document)
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("invalid membership payload changed model state")
		}
	})
}

func TestWALNegativeControlsDetectRequiredMutants(t *testing.T) {
	row := walOpsRow("mutant")
	generationOne := walOpsGeneration(1, walOpsGenerationStart(), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	generationTwo := walOpsGeneration(2, walOpsTransactionEnd(15), map[RowIdentity][]ScopeID{row: {walOpsScopeA}}, nil)
	valid := StateSnapshot{
		Registry: RegistryState{CurrentGeneration: 2, Generations: []RegistryGenerationState{generationOne, generationTwo}},
		Stream: StreamState{
			Transactions: []StreamTransaction{
				{ReplayKey: TransactionReplayKey{StreamGeneration: walOpsStream, CommitLSN: 10}, RegistryGeneration: 1, Lifecycle: TransactionLifecycleMaterialized},
				{ReplayKey: TransactionReplayKey{StreamGeneration: walOpsStream, CommitLSN: 20}, RegistryGeneration: 2, Lifecycle: TransactionLifecycleCommitted},
			},
		},
		Scopes: []SnapshotEntry[ScopeID, ScopeState]{{
			Key: walOpsScopeA,
			Value: ScopeState{Effects: []ScopeEffect{{
				Position:    StreamPosition{StreamGeneration: walOpsStream, Kind: PositionKindEffect, CommitLSN: 10},
				Row:         row,
				SourceEvent: EventReplayKey{Transaction: TransactionReplayKey{StreamGeneration: walOpsStream, CommitLSN: 10}},
				Operation:   EffectOperationUpsert,
			}}},
		}},
	}
	if violations := walOpsWALOracleViolations(valid); len(violations) != 0 {
		t.Fatalf("valid WAL oracle fixture has violations: %v", violations)
	}

	t.Run("arrival ordering", func(t *testing.T) {
		mutant := valid
		mutant.Stream = cloneStreamState(valid.Stream)
		mutant.Stream.Transactions[0], mutant.Stream.Transactions[1] = mutant.Stream.Transactions[1], mutant.Stream.Transactions[0]
		walOpsRequireViolation(t, walOpsWALOracleViolations(mutant), "arrival_order")
	})
	t.Run("duplicate effects", func(t *testing.T) {
		mutant := valid
		mutant.Scopes = append([]SnapshotEntry[ScopeID, ScopeState](nil), valid.Scopes...)
		mutant.Scopes[0].Value = cloneScopeState(valid.Scopes[0].Value)
		mutant.Scopes[0].Value.Effects = append(mutant.Scopes[0].Value.Effects, mutant.Scopes[0].Value.Effects[0])
		walOpsRequireViolation(t, walOpsWALOracleViolations(mutant), "duplicate_effect")
	})
	t.Run("skipped poison", func(t *testing.T) {
		mutant := valid
		mutant.Stream = cloneStreamState(valid.Stream)
		mutant.Stream.Poison = []PoisonRecord{{
			Transaction: TransactionReplayKey{StreamGeneration: walOpsStream, CommitLSN: 10},
			Lifecycle:   PoisonLifecycleActive,
		}}
		mutant.Stream.Transactions[1].Lifecycle = TransactionLifecycleMaterialized
		walOpsRequireViolation(t, walOpsWALOracleViolations(mutant), "skipped_poison")
	})
	t.Run("current generation selection", func(t *testing.T) {
		mutant := valid
		mutant.Stream = cloneStreamState(valid.Stream)
		mutant.Stream.Transactions[0].RegistryGeneration = mutant.Registry.CurrentGeneration
		walOpsRequireViolation(t, walOpsWALOracleViolations(mutant), "registry_activation")
	})
	t.Run("global scope collapse", func(t *testing.T) {
		before := StateSnapshot{Scopes: []SnapshotEntry[ScopeID, ScopeState]{
			{Key: walOpsScopeA, Value: ScopeState{Cardinality: 1}},
			{Key: walOpsScopeB, Value: ScopeState{Cardinality: 1}},
		}}
		after := before
		after.Scopes = append([]SnapshotEntry[ScopeID, ScopeState](nil), before.Scopes...)
		after.Scopes[1].Value.Cardinality = 0
		violations := walOpsMembershipIsolationViolations(before, after, map[ScopeID]struct{}{walOpsScopeA: {}})
		walOpsRequireViolation(t, violations, "global_scope_collapse")
	})
}

type walOpsDigestRow struct {
	identity string
	checksum Checksum
}

func walOpsGeneration(generation Generation, activation StreamPosition, evaluations map[RowIdentity][]ScopeID, impacts []DependencyImpact) RegistryGenerationState {
	rows := make([]RowIdentity, 0, len(evaluations))
	for row := range evaluations {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(left, right int) bool { return lessRowIdentity(rows[left], rows[right]) })
	converted := make([]MembershipEvaluation, 0, len(rows))
	for _, row := range rows {
		scopes := append([]ScopeID(nil), evaluations[row]...)
		sort.Slice(scopes, func(left, right int) bool { return scopes[left] < scopes[right] })
		converted = append(converted, MembershipEvaluation{Row: row, Scopes: scopes})
	}
	return RegistryGenerationState{
		Generation:         generation,
		ActivationBoundary: activation,
		Validated:          true,
		Relations: []RegistryRelation{
			{Definition: walOpsSyncedDefinition()},
			{Definition: walOpsDependencyDefinition()},
		},
		CaptureDependencies: []CaptureDependency{{ID: "wal-ops-capture-dependency", Relation: walOpsDependencyRelation, DependsOn: walOpsSyncedRelation}},
		ScopeRules: []ScopeRule{{
			ID:                  walOpsRule,
			Relation:            walOpsSyncedRelation,
			MembershipFunction:  "wal-ops-membership-function",
			PositiveFanoutBound: 8,
			Evaluations:         converted,
		}},
		DependencyImpacts: append([]DependencyImpact(nil), impacts...),
	}
}

func walOpsSyncedDefinition() RelationDefinition {
	return RelationDefinition{
		Relation:                 walOpsSyncedRelation,
		RegistrationKind:         RegistrationKindSynced,
		HasTableID:               true,
		TableID:                  walOpsTable,
		Physical:                 PhysicalRelation{Schema: "app", Name: "wal_rows", OID: 101, ReplicaIdentity: ReplicaIdentityDefault},
		PrimaryKeyFieldID:        walOpsPrimaryField,
		PrimaryKeyPhysicalColumn: "id",
		PrimaryKeyPortableType:   "string",
		CapturedFieldIDs:         []FieldID{walOpsPrimaryField, walOpsValueField},
		MembershipFunction:       "wal-ops-membership-function",
		PositiveFanoutBound:      8,
	}
}

func walOpsDependencyDefinition() RelationDefinition {
	return RelationDefinition{
		Relation:                   walOpsDependencyRelation,
		RegistrationKind:           RegistrationKindCaptureDependency,
		Physical:                   PhysicalRelation{Schema: "app", Name: "wal_dependencies", OID: 102, ReplicaIdentity: ReplicaIdentityDefault},
		CaptureKeyFieldIDs:         []FieldID{walOpsDependencyField},
		CapturedFieldIDs:           []FieldID{walOpsDependencyField},
		PositiveFanoutBound:        8,
		DependencyImpactFunction:   "wal-ops-impact-function",
		DependencyCapturedFieldIDs: []FieldID{walOpsDependencyField},
		PositiveDependencyRowBound: 8,
	}
}

func walOpsState(generations ...RegistryGenerationState) State {
	schema := walOpsSchema()
	currentGeneration := Generation(1)
	if len(generations) != 0 {
		currentGeneration = generations[len(generations)-1].Generation
	}
	state := State{
		ProtocolVersion: 3,
		CurrentSchema:   schema,
		Schemas:         map[SchemaRef]SchemaManifest{schema: {Body: []byte("wal-ops-schema"), Class: SchemaClassInitial, CompatibilityFloor: 1}},
		Registry:        RegistryState{CurrentGeneration: currentGeneration, Generations: generations},
		Relations: map[RelationID]RelationState{
			walOpsSyncedRelation:     {Definition: walOpsSyncedDefinition(), ScopeRules: []ScopeRuleID{walOpsRule}},
			walOpsDependencyRelation: {Definition: walOpsDependencyDefinition(), DependencyImpacts: []DependencyImpactID{walOpsImpact}},
		},
		Rows:        make(map[RowIdentity]AuthoritativeRow),
		Scopes:      make(map[ScopeID]ScopeState),
		Fences:      make(map[FenceID]VersionFence),
		Projections: make(map[ProjectionKey]CapturedProjection),
		Clients:     make(map[ClientKey]ClientState),
		Rebuilds:    make(map[RebuildKey]RebuildSession),
		Stream: StreamState{Authority: StreamAuthority{
			ActiveGeneration:              walOpsStream,
			GlobalMaterializationBoundary: walOpsGenerationStart(),
			HasActiveSlot:                 true,
			ActiveSlot:                    walOpsSlot,
		}},
		ConfiguredLimits: ConfiguredLimits{
			MaxScopeFanout:         8,
			MaxImpactRows:          1000,
			PullMaximum:            1000,
			RebuildMaximum:         1000,
			CompactionBatchMaximum: 10000,
			BackfillBatchMaximum:   1000,
		},
		Readiness: ReadinessState{
			ConfiguredDatabase: "wal-ops-db",
			Workers: []WorkerReadiness{{
				ID:                   walOpsWorker,
				Database:             "wal-ops-db",
				Running:              true,
				RegistryGeneration:   currentGeneration,
				MaterializedPosition: walOpsGenerationStart(),
			}},
			Slots: []SlotReadiness{{ID: walOpsSlot, Database: "wal-ops-db", Plugin: "pgoutput", Active: true}},
		},
	}
	for _, scopeID := range []ScopeID{walOpsScopeA, walOpsScopeB, walOpsScopeC} {
		scope := ScopeState{
			Schema:               schema,
			MembershipGeneration: 1,
			RetentionGeneration:  1,
			StreamGeneration:     walOpsStream,
			Membership:           []ScopeMembership{},
			Effects:              []ScopeEffect{},
			HighWatermark:        walOpsGenerationStart(),
		}
		scope.Checksum = walOpsIndependentScopeChecksum(schema.Hash, scopeID, nil)
		state.Scopes[scopeID] = scope
	}
	return state
}

func walOpsSchema() SchemaRef {
	var hash [32]byte
	hash[0] = 1
	return SchemaRef{Version: 1, Hash: hash}
}

func walOpsGenerationStart() StreamPosition {
	return StreamPosition{StreamGeneration: walOpsStream, Kind: PositionKindGenerationStart}
}

func walOpsTransactionEnd(commitLSN CommitLSN) StreamPosition {
	return StreamPosition{StreamGeneration: walOpsStream, Kind: PositionKindTransactionEnd, CommitLSN: commitLSN}
}

func walOpsRow(value string) RowIdentity {
	return RowIdentity{
		CanonicalIdentityBytes: "wal-row-identity:" + value,
		TableID:                walOpsTable,
		PrimaryKeyFieldID:      walOpsPrimaryField,
		PortableType:           "string",
		CanonicalWireJSON:      `"` + value + `"`,
	}
}

func walOpsModel(t *testing.T, state State) *Model {
	t.Helper()
	model, err := New(Config{
		State: state,
		Clock: &walOpsClock{now: time.Date(2035, time.January, 2, 3, 4, 5, 0, time.UTC)},
		Seed:  701,
	})
	if err != nil {
		t.Fatalf("create WAL operations model: %v", err)
	}
	return model
}

func walOpsApply(t *testing.T, model *Model, contract, name string, payload any) StepResult {
	t.Helper()
	raw := walOpsMarshalPayload(t, payload)
	result, err := model.Apply(context.Background(), scenarios.Operation{
		ContractOperation: contract,
		Name:              name,
		Payload:           raw,
	})
	if err != nil {
		t.Fatalf("apply %s/%s: %v", contract, name, err)
	}
	return result
}

func walOpsApplyError(t *testing.T, model *Model, contract, name string, payload any) {
	t.Helper()
	raw := walOpsMarshalPayload(t, payload)
	if _, err := model.Apply(context.Background(), scenarios.Operation{ContractOperation: contract, Name: name, Payload: raw}); err == nil {
		t.Fatalf("apply %s/%s accepted invalid or blocked work", contract, name)
	}
}

func walOpsMarshalPayload(t *testing.T, payload any) json.RawMessage {
	t.Helper()
	if raw, ok := payload.(json.RawMessage); ok {
		return append(json.RawMessage(nil), raw...)
	}
	if text, ok := payload.(string); ok {
		return json.RawMessage(text)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal operation payload: %v", err)
	}
	return encoded
}

func walOpsCommitPayload(commitLSN, endLSN string, events []walSourceEventPayload) walCommitSourceTransactionPayload {
	return walCommitSourceTransactionPayload{
		StreamGeneration: walOpsStream,
		CommitLSN:        commitLSN,
		EndLSN:           endLSN,
		Events:           events,
	}
}

func walOpsMaterializePayload(commitLSN string, failureClass *ReasonCode) walMaterializeSourceTransactionPayload {
	return walMaterializeSourceTransactionPayload{
		StreamGeneration: walOpsStream,
		CommitLSN:        commitLSN,
		FailureClass:     failureClass,
	}
}

func walOpsEvent(ordinal uint64, relation RelationID, operation DMLOperation, before, after walNullableRegisteredImagePayload) walSourceEventPayload {
	return walSourceEventPayload{
		EventOrdinal: ordinal,
		Relation:     relation,
		Operation:    string(operation),
		Before:       before,
		After:        after,
	}
}

func walOpsNullImage() walNullableRegisteredImagePayload {
	return walNullableRegisteredImagePayload{Set: true}
}

func walOpsSyncedImage(row RowIdentity, version, value string, deleted bool) walNullableRegisteredImagePayload {
	checksum := walOpsRowChecksum(value, version)
	return walNullableRegisteredImagePayload{
		Set:   true,
		Valid: true,
		Value: walRegisteredImagePayload{
			Identity: walRegisteredIdentityPayload{
				Kind: RegistrationKindSynced,
				SyncedRow: &walRowIdentityPayload{
					CanonicalIdentityBytes: row.CanonicalIdentityBytes,
					TableID:                row.TableID,
					PrimaryKeyFieldID:      row.PrimaryKeyFieldID,
					PortableType:           row.PortableType,
					CanonicalWireJSON:      row.CanonicalWireJSON,
				},
			},
			Fields: []walFieldValuePayload{
				{Field: walOpsPrimaryField, Type: "string", WireJSON: row.CanonicalWireJSON},
				{Field: walOpsValueField, Type: "string", WireJSON: `"` + value + `"`},
			},
			Version:  RowVersion(version),
			Checksum: walNullableChecksumPayload{Set: true, Valid: true, Value: checksum},
			Deleted:  deleted,
		},
	}
}

func walOpsDependencyImage(key, version string) walNullableRegisteredImagePayload {
	return walNullableRegisteredImagePayload{
		Set:   true,
		Valid: true,
		Value: walRegisteredImagePayload{
			Identity: walRegisteredIdentityPayload{
				Kind:       RegistrationKindCaptureDependency,
				CaptureKey: &walCaptureKeyPayload{CanonicalKeyBytes: key},
			},
			Fields:   []walFieldValuePayload{{Field: walOpsDependencyField, Type: "string", WireJSON: `"` + key + `"`}},
			Version:  RowVersion(version),
			Checksum: walNullableChecksumPayload{Set: true},
		},
	}
}

func walOpsCommitInsert(t *testing.T, model *Model, commitLSN, endLSN string, ordinal uint64, row RowIdentity, version string) StepResult {
	t.Helper()
	value := strings.Trim(row.CanonicalWireJSON, `"`)
	event := walOpsEvent(ordinal, walOpsSyncedRelation, DMLOperationInsert, walOpsNullImage(), walOpsSyncedImage(row, version, value, false))
	return walOpsApply(t, model, "model", "commit-source-transaction", walOpsCommitPayload(commitLSN, endLSN, []walSourceEventPayload{event}))
}

func walOpsRowChecksum(value, version string) Checksum {
	return sha256.Sum256([]byte("wal-ops-row:" + value + ":" + version))
}

func walOpsAuthoritativeRow(row RowIdentity, version, value string) AuthoritativeRow {
	return AuthoritativeRow{
		Identity: row,
		FieldValues: []FieldValue{
			{Field: walOpsPrimaryField, Type: "string", WireJSON: row.CanonicalWireJSON},
			{Field: walOpsValueField, Type: "string", WireJSON: `"` + value + `"`},
		},
		Version:  RowVersion(version),
		Checksum: walOpsRowChecksum(value, version),
	}
}

func walOpsInstallMembership(state *State, scopeID ScopeID, rows ...RowIdentity) {
	scope := state.Scopes[scopeID]
	for _, row := range rows {
		scope.Membership = append(scope.Membership, ScopeMembership{Row: row, Generation: scope.MembershipGeneration, Included: true})
	}
	digestRows := make([]walOpsDigestRow, 0, len(rows))
	for _, row := range rows {
		digestRows = append(digestRows, walOpsDigestRow{identity: row.CanonicalIdentityBytes, checksum: state.Rows[row].Checksum})
	}
	scope.Cardinality = Cardinality(len(rows))
	scope.Checksum = walOpsIndependentScopeChecksum(scope.Schema.Hash, scopeID, digestRows)
	state.Scopes[scopeID] = scope
}

func walOpsIndependentScopeChecksum(schemaHash [32]byte, scope ScopeID, rows []walOpsDigestRow) Checksum {
	ordered := append([]walOpsDigestRow(nil), rows...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].identity < ordered[right].identity })
	preimage := append([]byte("synchro:v3:scope-digest:v1\x00"), schemaHash[:]...)
	preimage = walOpsAppendBlob(preimage, []byte(scope))
	preimage = walOpsAppendUint64(preimage, uint64(len(ordered)))
	for _, row := range ordered {
		preimage = walOpsAppendBlob(preimage, []byte(row.identity))
		preimage = append(preimage, row.checksum[:]...)
	}
	return sha256.Sum256(preimage)
}

func walOpsAppendBlob(destination, value []byte) []byte {
	destination = walOpsAppendUint64(destination, uint64(len(value)))
	return append(destination, value...)
}

func walOpsAppendUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func walOpsScopeSnapshot(t *testing.T, snapshot StateSnapshot, scope ScopeID) ScopeState {
	t.Helper()
	for _, entry := range snapshot.Scopes {
		if entry.Key == scope {
			return entry.Value
		}
	}
	t.Fatalf("scope %q is absent", scope)
	return ScopeState{}
}

func walOpsTransactionSnapshot(t *testing.T, snapshot StateSnapshot, commitLSN CommitLSN) StreamTransaction {
	t.Helper()
	for _, transaction := range snapshot.Stream.Transactions {
		if transaction.ReplayKey.StreamGeneration == walOpsStream && transaction.ReplayKey.CommitLSN == commitLSN {
			return transaction
		}
	}
	t.Fatalf("transaction %d is absent", commitLSN)
	return StreamTransaction{}
}

func walOpsClientSnapshot(t *testing.T, snapshot StateSnapshot, key ClientKey) ClientState {
	t.Helper()
	for _, entry := range snapshot.Clients {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("client %#v is absent", key)
	return ClientState{}
}

func walOpsRebuildSnapshot(t *testing.T, snapshot StateSnapshot, key RebuildKey) RebuildSession {
	t.Helper()
	for _, entry := range snapshot.Rebuilds {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("rebuild %#v is absent", key)
	return RebuildSession{}
}

func walOpsStageMembershipPayload(registryGeneration, membershipGeneration uint64, affected []ScopeID, evaluations map[RowIdentity][]ScopeID, impacts []DependencyImpact) stageRegistryMembershipGenerationPayload {
	rows := make([]RowIdentity, 0, len(evaluations))
	for row := range evaluations {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(left, right int) bool { return lessRowIdentity(rows[left], rows[right]) })
	evaluationPayloads := make([]membershipEvaluationPayload, 0, len(rows))
	for _, row := range rows {
		rowPayload := walRowIdentityPayload{
			CanonicalIdentityBytes: row.CanonicalIdentityBytes,
			TableID:                row.TableID,
			PrimaryKeyFieldID:      row.PrimaryKeyFieldID,
			PortableType:           row.PortableType,
			CanonicalWireJSON:      row.CanonicalWireJSON,
		}
		scopeValues := make([]string, 0, len(evaluations[row]))
		for _, scope := range evaluations[row] {
			scopeValues = append(scopeValues, string(scope))
		}
		evaluationPayloads = append(evaluationPayloads, membershipEvaluationPayload{Row: &rowPayload, Scopes: &scopeValues})
	}
	ruleID := string(walOpsRule)
	relation := string(walOpsSyncedRelation)
	function := "wal-ops-membership-function-v2"
	fanout := uint64(8)
	rules := []membershipScopeRulePayload{{
		ID:                  &ruleID,
		Relation:            &relation,
		MembershipFunction:  &function,
		PositiveFanoutBound: &fanout,
		Evaluations:         &evaluationPayloads,
	}}
	impactPayloads := make([]membershipDependencyImpactPayload, 0, len(impacts))
	for _, impact := range impacts {
		id := string(impact.ID)
		relationID := string(impact.Relation)
		functionID := string(impact.Function)
		captured := make([]string, 0, len(impact.CapturedFieldIDs))
		for _, field := range impact.CapturedFieldIDs {
			captured = append(captured, string(field))
		}
		rows := make([]walRowIdentityPayload, 0, len(impact.AffectedRows))
		for _, row := range impact.AffectedRows {
			rows = append(rows, walRowIdentityPayload{
				CanonicalIdentityBytes: row.CanonicalIdentityBytes,
				TableID:                row.TableID,
				PrimaryKeyFieldID:      row.PrimaryKeyFieldID,
				PortableType:           row.PortableType,
				CanonicalWireJSON:      row.CanonicalWireJSON,
			})
		}
		bound := impact.PositiveRowBound
		requiresRebuild := impact.RequiresRebuild
		impactPayloads = append(impactPayloads, membershipDependencyImpactPayload{
			ID:               &id,
			Relation:         &relationID,
			Function:         &functionID,
			CapturedFieldIDs: &captured,
			PositiveRowBound: &bound,
			AffectedRows:     &rows,
			RequiresRebuild:  &requiresRebuild,
		})
	}
	affectedValues := make([]string, 0, len(affected))
	for _, scope := range affected {
		affectedValues = append(affectedValues, string(scope))
	}
	stream := walOpsStream
	kind := PositionKindTransactionEnd
	commitLSN := "10"
	batchSize := uint64(1000)
	return stageRegistryMembershipGenerationPayload{
		RegistryGeneration:   &registryGeneration,
		MembershipGeneration: &membershipGeneration,
		BatchSize:            &batchSize,
		ActivationBoundary: &membershipActivationBoundaryPayload{
			StreamGeneration: &stream,
			Kind:             &kind,
			CommitLSN:        &commitLSN,
		},
		AffectedScopes:    &affectedValues,
		ScopeRules:        &rules,
		DependencyImpacts: &impactPayloads,
	}
}

func walOpsDecodeObject(t *testing.T, payload any) map[string]any {
	t.Helper()
	encoded := walOpsMarshalPayload(t, payload)
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode payload object: %v", err)
	}
	return document
}

func walOpsWALOracleViolations(snapshot StateSnapshot) []string {
	violations := make([]string, 0)
	for index := 1; index < len(snapshot.Stream.Transactions); index++ {
		left := snapshot.Stream.Transactions[index-1].ReplayKey
		right := snapshot.Stream.Transactions[index].ReplayKey
		if left.StreamGeneration > right.StreamGeneration || left.StreamGeneration == right.StreamGeneration && left.CommitLSN >= right.CommitLSN {
			violations = append(violations, "arrival_order")
		}
	}
	seenEffects := make(map[string]struct{})
	for _, scope := range snapshot.Scopes {
		for _, effect := range scope.Value.Effects {
			key := string(scope.Key) + "|" + effect.Row.CanonicalIdentityBytes + "|" + string(effect.Operation) + "|" + string(effect.SourceEvent.Transaction.StreamGeneration) + "|" + strconv.FormatUint(uint64(effect.SourceEvent.Transaction.CommitLSN), 10) + "|" + strconv.FormatUint(uint64(effect.SourceEvent.EventOrdinal), 10)
			if _, duplicate := seenEffects[key]; duplicate {
				violations = append(violations, "duplicate_effect")
			}
			seenEffects[key] = struct{}{}
		}
	}
	for _, poison := range snapshot.Stream.Poison {
		if poison.Lifecycle != PoisonLifecycleActive {
			continue
		}
		for _, transaction := range snapshot.Stream.Transactions {
			if transaction.ReplayKey.StreamGeneration == poison.Transaction.StreamGeneration && transaction.ReplayKey.CommitLSN > poison.Transaction.CommitLSN && transaction.Lifecycle == TransactionLifecycleMaterialized {
				violations = append(violations, "skipped_poison")
			}
		}
	}
	for _, transaction := range snapshot.Stream.Transactions {
		selected := walOpsIndependentRegistrySelection(snapshot.Registry, transaction.ReplayKey)
		if selected != transaction.RegistryGeneration {
			violations = append(violations, "registry_activation")
		}
	}
	return violations
}

func walOpsIndependentRegistrySelection(registry RegistryState, key TransactionReplayKey) Generation {
	var selected Generation
	var selectedBoundary StreamPosition
	for _, generation := range registry.Generations {
		if !generation.Validated || generation.HasBootstrapStage || generation.ActivationBoundary.StreamGeneration != key.StreamGeneration {
			continue
		}
		precedes := generation.ActivationBoundary.Kind == PositionKindGenerationStart || generation.ActivationBoundary.Kind == PositionKindTransactionEnd && generation.ActivationBoundary.CommitLSN < key.CommitLSN
		if !precedes {
			continue
		}
		if selected == 0 || selectedBoundary.Kind == PositionKindGenerationStart && generation.ActivationBoundary.Kind == PositionKindTransactionEnd || selectedBoundary.Kind == generation.ActivationBoundary.Kind && generation.ActivationBoundary.CommitLSN > selectedBoundary.CommitLSN || generation.ActivationBoundary == selectedBoundary && generation.Generation > selected {
			selected = generation.Generation
			selectedBoundary = generation.ActivationBoundary
		}
	}
	return selected
}

func walOpsMembershipIsolationViolations(before, after StateSnapshot, affected map[ScopeID]struct{}) []string {
	beforeScopes := make(map[ScopeID]ScopeState, len(before.Scopes))
	for _, entry := range before.Scopes {
		beforeScopes[entry.Key] = entry.Value
	}
	violations := make([]string, 0)
	for _, entry := range after.Scopes {
		if _, allowed := affected[entry.Key]; allowed {
			continue
		}
		if prior, found := beforeScopes[entry.Key]; !found || !reflect.DeepEqual(prior, entry.Value) {
			violations = append(violations, "global_scope_collapse")
		}
	}
	return violations
}

func walOpsRequireViolation(t *testing.T, violations []string, wanted string) {
	t.Helper()
	for _, violation := range violations {
		if violation == wanted {
			return
		}
	}
	t.Fatalf("negative control did not detect %q: %v", wanted, violations)
}
