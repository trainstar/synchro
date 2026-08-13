package reference

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	pushOpsUser          UserID           = "push-ops-user"
	pushOpsClient        ClientID         = "push-ops-client"
	pushOpsScope         ScopeID          = "push-ops-scope"
	pushOpsStream        StreamGeneration = "push-ops-stream"
	pushOpsTable         TableID          = "push-ops-table"
	pushOpsRelation      RelationID       = "push-ops-relation"
	pushOpsIDField       FieldID          = "push-ops-id"
	pushOpsValueField    FieldID          = "push-ops-value"
	pushOpsNoteField     FieldID          = "push-ops-note"
	pushOpsAmountField   FieldID          = "push-ops-amount"
	pushOpsCreatedField  FieldID          = "push-ops-created-at"
	pushOpsUpdatedField  FieldID          = "push-ops-updated-at"
	pushOpsDeletedField  FieldID          = "push-ops-deleted-at"
	pushOpsClientVersion ClientVersion    = "2032-01-02T03:04:05.000000Z"
)

var pushOpsFixtureTime = time.Date(2032, time.January, 2, 3, 4, 5, 0, time.UTC)

type pushOpsClock struct {
	now time.Time
}

func (clock *pushOpsClock) Now() time.Time {
	return clock.now
}

type pushOpsFieldSpec struct {
	ID        FieldID
	Name      string
	Type      PortableType
	Nullable  bool
	Writable  bool
	Precision *int
	Scale     *int
}

type pushOpsTableSpec struct {
	ID        TableID
	Relation  RelationID
	Name      string
	Primary   FieldID
	Fields    []pushOpsFieldSpec
	CreatedAt *FieldID
	UpdatedAt *FieldID
	DeletedAt *FieldID
}

func TestPushLocalCaptureNormalizationAndServerApplySuppression(t *testing.T) {
	t.Run("application capture covers insert update and delete", func(t *testing.T) {
		tests := []struct {
			name      string
			operation DMLOperation
			prepare   func(*State, SchemaRef, pushOpsTableSpec)
			columns   map[string]any
			base      *RowVersion
			wantBase  bool
		}{
			{
				name:      "insert",
				operation: DMLOperationInsert,
				columns:   map[string]any{string(pushOpsValueField): "inserted"},
			},
			{
				name:      "update",
				operation: DMLOperationUpdate,
				prepare: func(state *State, schema SchemaRef, table pushOpsTableSpec) {
					row := pushOpsInstallLiveRow(t, state, schema, table, "capture-update", "before", "capture-update-v1", false)
					pushOpsInstallLocalRow(t, state, pushOpsClientKey(), state.Stream.SourceRows[len(state.Stream.SourceRows)-1].Row)
					if row.CanonicalIdentityBytes == "" {
						t.Fatal("fixture row has no canonical identity")
					}
				},
				columns:  map[string]any{string(pushOpsValueField): "updated"},
				base:     pushOpsVersionPointer("capture-update-v1"),
				wantBase: true,
			},
			{
				name:      "delete",
				operation: DMLOperationDelete,
				prepare: func(state *State, schema SchemaRef, table pushOpsTableSpec) {
					pushOpsInstallLiveRow(t, state, schema, table, "capture-delete", "before", "capture-delete-v1", false)
					pushOpsInstallLocalRow(t, state, pushOpsClientKey(), state.Stream.SourceRows[len(state.Stream.SourceRows)-1].Row)
				},
				base:     pushOpsVersionPointer("capture-delete-v1"),
				wantBase: true,
			},
		}

		for index, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				state, clock, schema, table := pushOpsFixture(t, true, true)
				if test.prepare != nil {
					test.prepare(&state, schema, table)
				}
				model := pushOpsModel(t, state, clock)
				rowID := "capture-" + test.name
				result := pushOpsLocalWrite(t, model, pushOpsUUID(uint64(10+index)), table.ID, rowID, schema, test.operation, test.base, test.columns, "application")
				if result.Kind != StepResultKindLocal || result.Local == nil || result.Local.Status != LocalMutationStatusPending {
					t.Fatalf("local %s result = %#v", test.name, result)
				}
				local := pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
				queued := pushOpsQueuedMutation(t, local, MutationID(pushOpsUUID(uint64(10+index))))
				if queued.Operation != test.operation || queued.Status != LocalMutationStatusPending || queued.HasBaseVersion != test.wantBase {
					t.Fatalf("captured %s mutation = %#v", test.name, queued)
				}
				if test.wantBase && queued.BaseVersion != *test.base {
					t.Fatalf("captured %s base = %q, want %q", test.name, queued.BaseVersion, *test.base)
				}
				if test.operation == DMLOperationDelete {
					if queued.AuthoredColumns != nil {
						t.Fatal("captured delete retained authored columns")
					}
				} else if len(queued.AuthoredColumns) != 1 {
					t.Fatalf("captured %s has %d authored columns, want 1", test.name, len(queued.AuthoredColumns))
				}
			})
		}
	})

	t.Run("server apply changes local rows without a capture echo", func(t *testing.T) {
		state, clock, schema, table := pushOpsFixture(t, true, true)
		pushOpsInstallLiveRow(t, &state, schema, table, "server-apply", "before", "server-apply-v1", false)
		pushOpsInstallLocalRow(t, &state, pushOpsClientKey(), state.Stream.SourceRows[0].Row)
		model := pushOpsModel(t, state, clock)

		result := pushOpsLocalWrite(t, model, pushOpsUUID(20), table.ID, "server-apply", schema, DMLOperationUpdate, pushOpsVersionPointer("server-apply-v1"), map[string]any{string(pushOpsValueField): "authoritative"}, "server_apply")
		if result.Kind != StepResultKindLocal || result.Local == nil || result.Local.Status != LocalMutationStatusAccepted {
			t.Fatalf("server apply result = %#v", result)
		}
		local := pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
		if len(local.DurableQueue) != 0 || len(local.Outcomes) != 0 {
			t.Fatal("server apply created local mutation state")
		}
		row := pushOpsLocalRow(t, local, table.ID, "server-apply")
		if got := pushOpsLocalField(t, row, pushOpsValueField); got != `"authoritative"` {
			t.Fatalf("server-applied field = %s, want authoritative", got)
		}
	})

	t.Run("same-row pre-wire normalization retains every original record", func(t *testing.T) {
		tests := []struct {
			name               string
			first              DMLOperation
			second             DMLOperation
			prepare            func(*State, SchemaRef, pushOpsTableSpec)
			firstColumns       map[string]any
			secondColumns      map[string]any
			firstBase          *RowVersion
			wantOperation      DMLOperation
			wantCancelled      bool
			wantNormalizedBase RowVersion
		}{
			{
				name:          "insert-update",
				first:         DMLOperationInsert,
				second:        DMLOperationUpdate,
				firstColumns:  map[string]any{string(pushOpsValueField): "one"},
				secondColumns: map[string]any{string(pushOpsValueField): "two", string(pushOpsNoteField): "final"},
				wantOperation: DMLOperationInsert,
			},
			{
				name:          "insert-delete",
				first:         DMLOperationInsert,
				second:        DMLOperationDelete,
				firstColumns:  map[string]any{string(pushOpsValueField): "one"},
				wantCancelled: true,
			},
			{
				name:   "update-update",
				first:  DMLOperationUpdate,
				second: DMLOperationUpdate,
				prepare: func(state *State, schema SchemaRef, table pushOpsTableSpec) {
					pushOpsInstallLiveRow(t, state, schema, table, "normalize-update-update", "before", "normalize-base", false)
					pushOpsInstallLocalRow(t, state, pushOpsClientKey(), state.Stream.SourceRows[0].Row)
				},
				firstColumns:       map[string]any{string(pushOpsValueField): "one"},
				secondColumns:      map[string]any{string(pushOpsNoteField): "two"},
				firstBase:          pushOpsVersionPointer("normalize-base"),
				wantOperation:      DMLOperationUpdate,
				wantNormalizedBase: "normalize-base",
			},
			{
				name:   "update-delete",
				first:  DMLOperationUpdate,
				second: DMLOperationDelete,
				prepare: func(state *State, schema SchemaRef, table pushOpsTableSpec) {
					pushOpsInstallLiveRow(t, state, schema, table, "normalize-update-delete", "before", "normalize-delete-base", false)
					pushOpsInstallLocalRow(t, state, pushOpsClientKey(), state.Stream.SourceRows[0].Row)
				},
				firstColumns:       map[string]any{string(pushOpsValueField): "one"},
				firstBase:          pushOpsVersionPointer("normalize-delete-base"),
				wantOperation:      DMLOperationDelete,
				wantNormalizedBase: "normalize-delete-base",
			},
		}

		for index, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				state, clock, schema, table := pushOpsFixture(t, true, true)
				if test.prepare != nil {
					test.prepare(&state, schema, table)
				}
				model := pushOpsModel(t, state, clock)
				rowID := "normalize-" + test.name
				if test.first != DMLOperationInsert {
					rowID = "normalize-" + test.name
				}
				firstID := MutationID(pushOpsUUID(uint64(30 + index*10)))
				secondID := MutationID(pushOpsUUID(uint64(31 + index*10)))
				pushOpsLocalWrite(t, model, string(firstID), table.ID, rowID, schema, test.first, test.firstBase, test.firstColumns, "application")
				pushOpsLocalWrite(t, model, string(secondID), table.ID, rowID, schema, test.second, nil, test.secondColumns, "application")

				local := pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
				first := pushOpsQueuedMutation(t, local, firstID)
				second := pushOpsQueuedMutation(t, local, secondID)
				if first.Status != LocalMutationStatusSupersededBeforeSend {
					t.Fatalf("first %s record status = %q", test.name, first.Status)
				}
				if test.wantCancelled {
					if second.Status != LocalMutationStatusCancelledBeforeSend {
						t.Fatalf("insert-delete second record status = %q", second.Status)
					}
					if len(pushOpsPendingMutations(local)) != 0 {
						t.Fatal("insert-delete created a transient sendable mutation")
					}
					if len(model.Snapshot().Batches) != 0 || len(model.Snapshot().Mutations) != 0 || len(model.Snapshot().Stream.SourceRows) != 0 {
						t.Fatal("insert-delete created server state before a push")
					}
					pushOpsRequireLocalOutcome(t, local, firstID, "superseded_before_send")
					pushOpsRequireLocalOutcome(t, local, secondID, "cancelled_before_send")
					return
				}
				if second.Status != LocalMutationStatusSupersededBeforeSend {
					t.Fatalf("second %s record status = %q", test.name, second.Status)
				}
				normalized := pushOpsOnlyPendingMutation(t, local)
				if normalized.Operation != test.wantOperation || normalized.Mutation == firstID || normalized.Mutation == secondID || !normalized.HasPredecessor || normalized.Predecessor != secondID {
					t.Fatalf("normalized %s record = %#v", test.name, normalized)
				}
				if test.wantOperation == DMLOperationDelete {
					if normalized.AuthoredColumns != nil || len(normalized.AuthoredColumns) != 0 {
						t.Fatal("normalized delete contains columns")
					}
				}
				if test.wantNormalizedBase != "" && (!normalized.HasBaseVersion || normalized.BaseVersion != test.wantNormalizedBase) {
					t.Fatalf("normalized %s base = %q", test.name, normalized.BaseVersion)
				}
				if test.wantOperation == DMLOperationInsert && normalized.HasBaseVersion {
					t.Fatal("normalized insert fabricated a base version")
				}
				pushOpsRequireLocalOutcome(t, local, firstID, "superseded_before_send")
				pushOpsRequireLocalOutcome(t, local, secondID, "superseded_before_send")
			})
		}
	})
}

func TestPushSealedPredecessorBlocksWithoutFabricatedBase(t *testing.T) {
	state, clock, schema, table := pushOpsFixture(t, true, true)
	pushOpsInstallLiveRow(t, &state, schema, table, "sealed-chain", "base", "sealed-chain-v1", false)
	pushOpsInstallLocalRow(t, &state, pushOpsClientKey(), state.Stream.SourceRows[0].Row)
	model := pushOpsModel(t, state, clock)

	firstID := MutationID(pushOpsUUID(100))
	secondID := MutationID(pushOpsUUID(101))
	firstBatch := pushOpsUUID(102)
	firstColumns := map[string]any{string(pushOpsValueField): "first"}
	pushOpsLocalWrite(t, model, string(firstID), table.ID, "sealed-chain", schema, DMLOperationUpdate, pushOpsVersionPointer("sealed-chain-v1"), firstColumns, "application")
	firstRequest := pushOpsRequest(t, schema, firstBatch, pushOpsWireMutation(string(firstID), table.ID, "sealed-chain", schema, DMLOperationUpdate, pushOpsVersionPointer("sealed-chain-v1"), firstColumns))
	transport := pushOpsSubmit(t, model, firstRequest, "transport_failure", 100)
	pushOpsRequirePushFailure(t, transport, 503, pushHTTPTemporaryUnavailable, true)

	local := pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
	if first := pushOpsQueuedMutation(t, local, firstID); first.Status != LocalMutationStatusSealed {
		t.Fatalf("first sealed predecessor status = %q", first.Status)
	}
	pushOpsLocalWrite(t, model, string(secondID), table.ID, "sealed-chain", schema, DMLOperationUpdate, nil, map[string]any{string(pushOpsNoteField): "later"}, "application")
	local = pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
	second := pushOpsQueuedMutation(t, local, secondID)
	if second.Status != LocalMutationStatusPending || second.HasBaseVersion || !second.HasPredecessor || second.Predecessor != firstID {
		t.Fatalf("dependent successor = %#v", second)
	}

	invalidRequest := pushOpsRequest(t, schema, pushOpsUUID(103), pushOpsWireMutation(string(secondID), table.ID, "sealed-chain", schema, DMLOperationUpdate, nil, map[string]any{string(pushOpsNoteField): "later"}))
	beforeInvalid := model.Snapshot()
	invalid := pushOpsSubmit(t, model, invalidRequest, "apply", 101)
	pushOpsRequirePushFailure(t, invalid, 400, pushHTTPInvalidRequest, false)
	if afterInvalid := model.Snapshot(); !reflect.DeepEqual(afterInvalid, beforeInvalid) {
		t.Fatal("base-less dependent mutation changed state when submitted")
	}
	first := pushOpsSubmit(t, model, firstRequest, "apply", 100)
	pushOpsRequirePushSuccess(t, first, ReplayDispositionExecuted, 1)
	firstOutcome := pushOpsOnlyAcceptedOutcome(t, first)
	if firstOutcome.ServerVersion == nil {
		t.Fatal("accepted predecessor omitted its server version")
	}
	local = pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
	second = pushOpsQueuedMutation(t, local, secondID)
	if second.Status != LocalMutationStatusPending || !second.HasBaseVersion || second.BaseVersion != RowVersion(*firstOutcome.ServerVersion) {
		t.Fatalf("unsealed successor was not refreshed from accepted predecessor: %#v", second)
	}
	if second.Operation != DMLOperationUpdate || second.ClientVersion != pushOpsClientVersion || !reflect.DeepEqual(second.AuthoredColumns, []FieldValue{{Field: pushOpsNoteField, Type: "string", WireJSON: `"later"`}}) {
		t.Fatal("base refresh changed authored successor content")
	}

	secondBatch := pushOpsUUID(104)
	secondRequest := pushOpsRequest(t, schema, secondBatch, pushOpsWireMutation(string(secondID), table.ID, "sealed-chain", schema, DMLOperationUpdate, &second.BaseVersion, map[string]any{string(pushOpsNoteField): "later"}))
	secondTransport := pushOpsSubmit(t, model, secondRequest, "transport_failure", 101)
	pushOpsRequirePushFailure(t, secondTransport, 503, pushHTTPTemporaryUnavailable, true)
	sealedBeforeReplay := pushOpsSealedBatch(t, pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey()), BatchID(secondBatch))
	queuedBeforeReplay := pushOpsQueuedMutation(t, pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey()), secondID)

	replay := pushOpsSubmit(t, model, firstRequest, "apply", 100)
	pushOpsRequirePushSuccess(t, replay, ReplayDispositionReplayed, 1)
	if !bytes.Equal(replay.HTTP.Body, first.HTTP.Body) {
		t.Fatal("completed predecessor replay changed its canonical response")
	}
	local = pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
	if got := pushOpsSealedBatch(t, local, BatchID(secondBatch)); !reflect.DeepEqual(got, sealedBeforeReplay) {
		t.Fatal("sealed successor changed during predecessor replay")
	}
	if got := pushOpsQueuedMutation(t, local, secondID); !reflect.DeepEqual(got, queuedBeforeReplay) {
		t.Fatal("sealed successor intent changed during predecessor replay")
	}
}

func TestPushCASAbsentLiveDeletedMatrix(t *testing.T) {
	tests := []struct {
		name      string
		row       string
		version   RowVersion
		deleted   bool
		operation DMLOperation
		base      *RowVersion
		wantState MutationOutcomeState
		wantCode  ReasonCode
	}{
		{name: "absent-insert", operation: DMLOperationInsert, wantState: MutationOutcomeApplied},
		{name: "absent-update", operation: DMLOperationUpdate, base: pushOpsVersionPointer("absent-v1"), wantState: MutationOutcomeConflict, wantCode: "row_not_found"},
		{name: "absent-delete", operation: DMLOperationDelete, base: pushOpsVersionPointer("absent-v1"), wantState: MutationOutcomeConflict, wantCode: "row_not_found"},
		{name: "live-insert", row: "live", version: "live-v1", operation: DMLOperationInsert, wantState: MutationOutcomeConflict, wantCode: "row_already_exists"},
		{name: "live-update-equal", row: "live", version: "live-v1", operation: DMLOperationUpdate, base: pushOpsVersionPointer("live-v1"), wantState: MutationOutcomeApplied},
		{name: "live-update-stale", row: "live", version: "live-v1", operation: DMLOperationUpdate, base: pushOpsVersionPointer("live-stale"), wantState: MutationOutcomeConflict, wantCode: "version_conflict"},
		{name: "live-delete-equal", row: "live", version: "live-v1", operation: DMLOperationDelete, base: pushOpsVersionPointer("live-v1"), wantState: MutationOutcomeApplied},
		{name: "live-delete-stale", row: "live", version: "live-v1", operation: DMLOperationDelete, base: pushOpsVersionPointer("live-stale"), wantState: MutationOutcomeConflict, wantCode: "version_conflict"},
		{name: "deleted-insert", row: "deleted", version: "deleted-v1", deleted: true, operation: DMLOperationInsert, wantState: MutationOutcomeConflict, wantCode: "row_deleted"},
		{name: "deleted-update-equal", row: "deleted", version: "deleted-v1", deleted: true, operation: DMLOperationUpdate, base: pushOpsVersionPointer("deleted-v1"), wantState: MutationOutcomeConflict, wantCode: "row_deleted"},
		{name: "deleted-update-stale", row: "deleted", version: "deleted-v1", deleted: true, operation: DMLOperationUpdate, base: pushOpsVersionPointer("deleted-stale"), wantState: MutationOutcomeConflict, wantCode: "row_deleted"},
		{name: "deleted-delete-equal", row: "deleted", version: "deleted-v1", deleted: true, operation: DMLOperationDelete, base: pushOpsVersionPointer("deleted-v1"), wantState: MutationOutcomeConflict, wantCode: "row_deleted"},
		{name: "deleted-delete-stale", row: "deleted", version: "deleted-v1", deleted: true, operation: DMLOperationDelete, base: pushOpsVersionPointer("deleted-stale"), wantState: MutationOutcomeConflict, wantCode: "row_deleted"},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, clock, schema, table := pushOpsFixture(t, true, false)
			if test.row != "" {
				pushOpsInstallLiveRow(t, &state, schema, table, test.row, "before", test.version, test.deleted)
			}
			model := pushOpsModel(t, state, clock)
			columns := map[string]any(nil)
			if test.operation != DMLOperationDelete {
				columns = map[string]any{string(pushOpsValueField): "after"}
			}
			pk := test.row
			if pk == "" {
				pk = "absent"
			}
			mutationID := MutationID(pushOpsUUID(uint64(200 + index)))
			request := pushOpsRequest(t, schema, pushOpsUUID(uint64(300+index)), pushOpsWireMutation(string(mutationID), table.ID, pk, schema, test.operation, test.base, columns))
			result := pushOpsSubmit(t, model, request, "apply", uint64(400+index))
			pushOpsRequirePushSuccess(t, result, ReplayDispositionExecuted, 1)
			response := pushOpsPushResponse(t, result)
			if test.wantState == MutationOutcomeApplied {
				if len(response.Accepted) != 1 || len(response.Rejected) != 0 {
					t.Fatalf("matrix partition = accepted %d, rejected %d", len(response.Accepted), len(response.Rejected))
				}
			} else if len(response.Accepted) != 0 || len(response.Rejected) != 1 {
				t.Fatalf("matrix partition = accepted %d, rejected %d", len(response.Accepted), len(response.Rejected))
			}
			raw := pushOpsResponseOutcomeRaw(t, response, mutationID)
			outcome := pushOpsMutationOutcome(t, raw)
			if outcome.Status != string(test.wantState) {
				t.Fatalf("matrix status = %q, want %q", outcome.Status, test.wantState)
			}
			if test.wantCode == "" {
				if outcome.Code != nil {
					t.Fatalf("accepted matrix outcome code = %q", *outcome.Code)
				}
			} else if outcome.Code == nil || ReasonCode(*outcome.Code) != test.wantCode {
				t.Fatalf("matrix code = %v, want %q", outcome.Code, test.wantCode)
			}
		})
	}
}

func TestPushAcceptedSoftAndHardDeleteRetainCASFence(t *testing.T) {
	tests := []struct {
		name       string
		softDelete bool
	}{
		{name: "soft", softDelete: true},
		{name: "hard", softDelete: false},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, clock, schema, table := pushOpsFixture(t, test.softDelete, false)
			identity := pushOpsInstallLiveRow(t, &state, schema, table, "delete-target", "before", "delete-v1", false)
			model := pushOpsModel(t, state, clock)
			deleteID := MutationID(pushOpsUUID(uint64(500 + index*10)))
			deleteRequest := pushOpsRequest(t, schema, pushOpsUUID(uint64(510+index*10)), pushOpsWireMutation(string(deleteID), table.ID, "delete-target", schema, DMLOperationDelete, pushOpsVersionPointer("delete-v1"), nil))
			result := pushOpsSubmit(t, model, deleteRequest, "apply", uint64(600+index*10))
			pushOpsRequirePushSuccess(t, result, ReplayDispositionExecuted, 1)
			outcome := pushOpsOnlyAcceptedOutcome(t, result)
			if outcome.ServerVersion == nil || *outcome.ServerVersion == "" {
				t.Fatal("accepted delete omitted its opaque successor version")
			}
			deleteVersion := RowVersion(*outcome.ServerVersion)
			snapshot := model.Snapshot()
			if len(snapshot.Stream.Transactions) != 1 || len(snapshot.Stream.Transactions[0].Events) != 1 {
				t.Fatalf("delete source transaction = %#v", snapshot.Stream.Transactions)
			}
			event := snapshot.Stream.Transactions[0].Events[0]
			if !event.HasBefore || len(event.Before.Fields) != len(table.Fields) || !event.Before.HasChecksum || event.Before.Identity.SyncedRow != identity || event.Before.Version != "delete-v1" || event.Before.Deleted {
				t.Fatalf("delete source before image = %#v", event.Before)
			}
			if test.softDelete {
				if event.Operation != DMLOperationUpdate || !event.HasAfter || len(event.After.Fields) != len(table.Fields) || !event.After.HasChecksum || !event.After.Deleted || event.After.Version != deleteVersion {
					t.Fatalf("soft-delete source event = %#v", event)
				}
				if got := pushOpsSourceRow(t, snapshot, table.ID, "delete-target"); !got.Deleted || got.Version != deleteVersion || got.Identity != identity {
					t.Fatalf("soft-delete source row = %#v", got)
				}
			} else if event.Operation != DMLOperationDelete || event.HasAfter || len(snapshot.Stream.SourceRows) != 0 {
				t.Fatalf("hard-delete source event or row = event %#v, rows %#v", event, snapshot.Stream.SourceRows)
			}

			var successor VersionFence
			foundFence := false
			for _, entry := range snapshot.Fences {
				if entry.Value.HasMutationKey && entry.Value.MutationKey.Mutation == deleteID {
					successor = entry.Value
					foundFence = true
					break
				}
			}
			if !foundFence || successor.RowVersion != deleteVersion || successor.Coverage != FenceCoveragePending || !successor.HasEventReplayKey || !successor.HasOldRegisteredIdentity || successor.OldRegisteredIdentity.SyncedRow != identity {
				t.Fatalf("delete successor fence = %#v", successor)
			}
			if test.softDelete {
				if !successor.HasNewRegisteredIdentity || successor.NewRegisteredIdentity.SyncedRow != identity || successor.Operation != DMLOperationUpdate {
					t.Fatalf("soft-delete successor fence = %#v", successor)
				}
			} else if successor.HasNewRegisteredIdentity || successor.Operation != DMLOperationDelete {
				t.Fatalf("hard-delete successor fence = %#v", successor)
			}

			for later, operation := range []DMLOperation{DMLOperationInsert, DMLOperationUpdate, DMLOperationDelete} {
				mutationID := MutationID(pushOpsUUID(uint64(520 + index*10 + later)))
				var base *RowVersion
				if operation != DMLOperationInsert {
					base = &deleteVersion
				}
				columns := map[string]any(nil)
				if operation == DMLOperationInsert || operation == DMLOperationUpdate {
					columns = map[string]any{string(pushOpsValueField): "resurrection"}
				}
				request := pushOpsRequest(t, schema, pushOpsUUID(uint64(530+index*10+later)), pushOpsWireMutation(string(mutationID), table.ID, "delete-target", schema, operation, base, columns))
				laterResult := pushOpsSubmit(t, model, request, "apply", uint64(630+index*10+later))
				pushOpsRequirePushSuccess(t, laterResult, ReplayDispositionExecuted, 1)
				laterResponse := pushOpsPushResponse(t, laterResult)
				if len(laterResponse.Accepted) != 0 || len(laterResponse.Rejected) != 1 || pushOpsOutcomeCodes(t, laterResponse.Rejected)[0] != "row_deleted" {
					t.Fatalf("later %s outcome = %#v", operation, laterResponse)
				}
			}
		})
	}
}

func TestPushMixedCASOutcomesPartitionAndDurableEvidence(t *testing.T) {
	policyTable := pushOpsDefaultTable(TableID("push-ops-policy-table"), RelationID("push-ops-policy-relation"), true)
	table := pushOpsDefaultTable(pushOpsTable, pushOpsRelation, true)
	schema, currentManifest := pushOpsManifest(t, 1, nil, SchemaClassInitial, 1, table, policyTable)
	clock := &pushOpsClock{now: pushOpsFixtureTime}
	state := pushOpsState(schema, map[SchemaRef]SchemaManifest{schema: currentManifest}, []pushOpsTableSpec{table, policyTable}, false, clock.now)
	incompatibleTable := pushOpsWithReplacementField(table, pushOpsFieldSpec{ID: pushOpsValueField, Name: "value", Type: "int64", Nullable: false, Writable: true})
	incompatibleSchema, incompatibleManifest := pushOpsManifest(t, 3, &schema, SchemaClass2, 1, incompatibleTable)
	state.Schemas[incompatibleSchema] = incompatibleManifest
	state.Relations[policyTable.Relation] = RelationState{Definition: pushOpsRelationDefinition(policyTable, 401)}
	state.Registry.Generations[0].Relations = append(state.Registry.Generations[0].Relations, RegistryRelation{Definition: pushOpsRelationDefinition(policyTable, 401)})
	state.Authorization.WritePolicies = []WritePolicyDecision{{User: pushOpsUser, Table: policyTable.ID, Allowed: false}}
	pushOpsInstallLiveRow(t, &state, schema, table, "mixed-conflict", "before", "mixed-v1", false)
	model := pushOpsModel(t, state, clock)

	appliedID := MutationID(pushOpsUUID(700))
	conflictID := MutationID(pushOpsUUID(701))
	policyID := MutationID(pushOpsUUID(702))
	validationID := MutationID(pushOpsUUID(703))
	tableID := MutationID(pushOpsUUID(704))
	schemaID := MutationID(pushOpsUUID(705))
	mutations := []map[string]any{
		pushOpsWireMutation(string(appliedID), table.ID, "mixed-applied", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "applied"}),
		pushOpsWireMutation(string(conflictID), table.ID, "mixed-conflict", schema, DMLOperationUpdate, pushOpsVersionPointer("mixed-stale"), map[string]any{string(pushOpsValueField): "conflict"}),
		pushOpsWireMutation(string(policyID), policyTable.ID, "mixed-policy", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "policy"}),
		pushOpsWireMutation(string(validationID), table.ID, "mixed-conflict", schema, DMLOperationUpdate, pushOpsVersionPointer("mixed-v1"), map[string]any{string(pushOpsValueField): 7}),
		pushOpsWireMutation(string(tableID), TableID("push-ops-never-synced"), "mixed-table", schema, DMLOperationInsert, nil, map[string]any{"missing": "table"}),
		pushOpsWireMutation(string(schemaID), table.ID, "mixed-schema", incompatibleSchema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): 8}),
	}
	request := pushOpsRequest(t, schema, pushOpsUUID(710), mutations...)
	before := model.Snapshot()
	result := pushOpsSubmit(t, model, request, "apply", 800)
	pushOpsRequirePushSuccess(t, result, ReplayDispositionExecuted, len(mutations))
	response := pushOpsPushResponse(t, result)
	if got := pushOpsSortedOutcomeIDs(response.Accepted); !reflect.DeepEqual(got, []MutationID{appliedID}) {
		t.Fatalf("accepted order = %#v", got)
	}
	if got := pushOpsSortedOutcomeIDs(response.Rejected); !reflect.DeepEqual(got, []MutationID{conflictID, policyID, validationID, tableID, schemaID}) {
		t.Fatalf("rejected order = %#v", got)
	}
	if got := pushOpsOutcomeCodes(t, response.Rejected); !reflect.DeepEqual(got, []ReasonCode{"version_conflict", "policy_rejected", "validation_failed", "table_not_synced", "schema_incompatible"}) {
		t.Fatalf("rejected codes = %#v", got)
	}
	if len(result.Push.Mutations) != len(mutations) {
		t.Fatalf("typed mutation observations = %d, want %d", len(result.Push.Mutations), len(mutations))
	}
	for index, mutation := range result.Push.Mutations {
		if mutation.Mutation != MutationID(mutations[index]["mutation_id"].(string)) {
			t.Fatalf("typed mutation %d = %#v", index, mutation)
		}
	}
	accepted := pushOpsMutationOutcome(t, response.Accepted[0])
	if accepted.RowChecksum == nil || accepted.RowChecksum.Algorithm != "sha256" || accepted.RowChecksum.Version != 1 || accepted.RowChecksum.Encoding != "hex" || accepted.ServerVersion == nil || *accepted.ServerVersion == "" {
		t.Fatalf("accepted wire evidence = %#v", accepted)
	}
	if len(accepted.ServerRow) != len(table.Fields) || !reflect.DeepEqual(pushOpsMapKeys(accepted.ServerRow), func() []string {
		keys := make([]string, 0, len(table.Fields))
		for _, field := range table.Fields {
			keys = append(keys, string(field.ID))
		}
		sort.Strings(keys)
		return keys
	}()) {
		t.Fatalf("accepted server row fields = %#v", pushOpsMapKeys(accepted.ServerRow))
	}
	checksum := pushOpsWireOutcomeChecksum(t, accepted)
	if !pushOpsRowChecksumMatches(t, state.Schemas[schema], table.ID, accepted.ServerRow, accepted.PK[string(pushOpsIDField)], *accepted.ServerVersion, checksum) {
		t.Fatal("accepted row checksum does not cover the complete row and opaque version")
	}
	after := model.Snapshot()
	clientBefore := pushOpsClientSnapshot(t, before, pushOpsClientKey())
	clientAfter := pushOpsClientSnapshot(t, after, pushOpsClientKey())
	if clientAfter.AcceptedWriteEpoch != clientBefore.AcceptedWriteEpoch+1 {
		t.Fatalf("accepted-write epoch = %d, want %d", clientAfter.AcceptedWriteEpoch, clientBefore.AcceptedWriteEpoch+1)
	}
	if len(after.Stream.SourceRows) != len(before.Stream.SourceRows)+1 || len(after.Stream.Transactions) != len(before.Stream.Transactions)+1 || len(after.Stream.Transactions[len(after.Stream.Transactions)-1].Events) != 1 {
		t.Fatalf("source writes = rows %d transactions %d", len(after.Stream.SourceRows), len(after.Stream.Transactions))
	}
	if len(after.Scopes) != len(before.Scopes) || len(after.Scopes[0].Value.Effects) != 0 {
		t.Fatal("accepted push created pull-visible effects before WAL materialization")
	}
	if !reflect.DeepEqual(pushOpsSourceRow(t, after, table.ID, "mixed-conflict"), pushOpsSourceRow(t, before, table.ID, "mixed-conflict")) {
		t.Fatal("rejected mixed mutations changed the existing source row")
	}
	acceptedSource := pushOpsSourceRow(t, after, table.ID, "mixed-applied")
	if acceptedSource.Version != RowVersion(*accepted.ServerVersion) || acceptedSource.Checksum != checksum || acceptedSource.Deleted {
		t.Fatalf("accepted source row = %#v", acceptedSource)
	}
	ledger := pushOpsMutationSnapshot(t, after, MutationKey{Client: pushOpsClientKey(), Mutation: appliedID})
	if ledger.Fingerprint.Algorithm != "sha256" || ledger.Fingerprint.Version != 1 || ledger.Fingerprint.Domain != pushMutationDomain || ledger.FirstBatch != BatchID(pushOpsUUID(710)) || ledger.RequestOrdinal != 1 || ledger.Table != table.ID || ledger.Row != acceptedSource.Identity || ledger.Operation != DMLOperationInsert || ledger.AuthoredSchema != schema || ledger.SubmittedSchema != schema || ledger.OutcomeSchema != schema || len(ledger.SealedCanonicalRequest) == 0 || len(ledger.SealedCanonicalResponse) == 0 || ledger.Outcome.Mutation != appliedID || ledger.Outcome.State != MutationOutcomeApplied || ledger.Outcome.Reason != "" {
		t.Fatalf("typed mutation ledger = %#v", ledger)
	}
	if !bytes.Equal(ledger.SealedCanonicalResponse, pushOpsResponseOutcomeRaw(t, response, appliedID)) {
		t.Fatal("mutation ledger response is not the canonical accepted wire outcome")
	}
	for _, entry := range after.Fences {
		if entry.Value.HasMutationKey && entry.Value.MutationKey.Mutation == appliedID {
			if entry.Value.Coverage != FenceCoveragePending || entry.Value.RowVersion != RowVersion(*accepted.ServerVersion) || !entry.Value.HasEventReplayKey {
				t.Fatalf("accepted pending fence = %#v", entry.Value)
			}
			return
		}
	}
	t.Fatal("accepted mutation fence is absent")
}

func TestPushMalformedShapesAndDuplicateMutationIDsRollBack(t *testing.T) {
	tests := []struct {
		name    string
		request func(SchemaRef) []byte
	}{
		{
			name: "insert-base-shape",
			request: func(schema SchemaRef) []byte {
				mutation := pushOpsWireMutation(pushOpsUUID(900), pushOpsTable, "malformed-insert", schema, DMLOperationInsert, pushOpsVersionPointer("unexpected-base"), map[string]any{string(pushOpsValueField): "invalid"})
				return pushOpsRequest(t, schema, pushOpsUUID(901), mutation)
			},
		},
		{
			name: "delete-columns-shape",
			request: func(schema SchemaRef) []byte {
				mutation := pushOpsWireMutation(pushOpsUUID(902), pushOpsTable, "malformed-delete", schema, DMLOperationDelete, pushOpsVersionPointer("delete-base"), map[string]any{string(pushOpsValueField): "invalid"})
				return pushOpsRequest(t, schema, pushOpsUUID(903), mutation)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, clock, schema, _ := pushOpsFixture(t, true, false)
			model := pushOpsModel(t, state, clock)
			before := model.Snapshot()
			result := pushOpsSubmit(t, model, test.request(schema), "apply", 1000)
			pushOpsRequirePushFailure(t, result, 400, pushHTTPInvalidRequest, false)
			if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatal("malformed operation shape changed durable state")
			}
		})
	}

	state, clock, schema, table := pushOpsFixture(t, true, false)
	model := pushOpsModel(t, state, clock)
	mutation := pushOpsWireMutation(pushOpsUUID(910), table.ID, "duplicate", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "duplicate"})
	before := model.Snapshot()
	result := pushOpsSubmit(t, model, pushOpsRequest(t, schema, pushOpsUUID(911), mutation, mutation), "apply", 1010)
	pushOpsRequirePushFailure(t, result, 400, pushHTTPInvalidRequest, false)
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("duplicate mutation IDs changed durable state")
	}
}

func TestTask6PushCompletedBatchReplayIsExactAndImmutable(t *testing.T) {
	state, clock, schema, table := pushOpsFixture(t, true, true)
	laterSchema, laterManifest := pushOpsManifest(t, 2, &schema, SchemaClass2, 1, table)
	state.Schemas[laterSchema] = laterManifest
	model := pushOpsModel(t, state, clock)

	mutationID := MutationID(pushOpsUUID(1000))
	batchID := BatchID(pushOpsUUID(1001))
	columns := map[string]any{string(pushOpsValueField): "first"}
	pushOpsLocalWrite(t, model, string(mutationID), table.ID, "replay-target", schema, DMLOperationInsert, nil, columns, "application")
	request := pushOpsRequest(t, schema, string(batchID), pushOpsWireMutation(string(mutationID), table.ID, "replay-target", schema, DMLOperationInsert, nil, columns))
	first := pushOpsSubmit(t, model, request, "apply", 1000)
	pushOpsRequirePushSuccess(t, first, ReplayDispositionExecuted, 1)
	firstResponse := pushOpsPushResponse(t, first)
	if firstResponse.ServerTime != formatCanonicalTime(clock.now) {
		t.Fatalf("first server time = %q", firstResponse.ServerTime)
	}
	firstSnapshot := model.Snapshot()
	key := BatchKey{Client: pushOpsClientKey(), Batch: batchID}
	batchLedger := pushOpsBatchSnapshot(t, firstSnapshot, key)
	mutationLedger := pushOpsMutationSnapshot(t, firstSnapshot, MutationKey{Client: pushOpsClientKey(), Mutation: mutationID})
	pushOpsRequireTask6LedgerRecords(t, batchLedger, mutationLedger, schema, batchID, mutationID, table.ID, first.HTTP.Body, request, clock.now)

	clock.now = clock.now.Add(9 * time.Hour)
	pushOpsApply(t, model, "model", "expire-client-generation", map[string]any{
		"user_id":   string(pushOpsUser),
		"client_id": string(pushOpsClient),
	})
	model.state.CurrentSchema = laterSchema
	changed := pushOpsAuthoritativeRow(t, model.state.Schemas[schema], table, "replay-target", "changed", "changed-v2", false)
	model.state.Rows[changed.Identity] = changed
	for index := range model.state.Stream.SourceRows {
		if model.state.Stream.SourceRows[index].Identity == changed.Identity {
			model.state.Stream.SourceRows[index].Row = changed
		}
	}
	model.state.Authorization.WritePolicies = []WritePolicyDecision{{User: pushOpsUser, Table: table.ID, Allowed: false}}
	beforeReplay := model.Snapshot()

	replay := pushOpsSubmit(t, model, request, "apply", 1001)
	pushOpsRequirePushSuccess(t, replay, ReplayDispositionReplayed, 1)
	if !bytes.Equal(replay.HTTP.Body, first.HTTP.Body) {
		t.Fatal("completed replay changed the HTTP body")
	}
	if !reflect.DeepEqual(replay.Push.Mutations, first.Push.Mutations) {
		t.Fatalf("completed replay mutations = %#v, want %#v", replay.Push.Mutations, first.Push.Mutations)
	}
	if replayResponse := pushOpsPushResponse(t, replay); replayResponse.ServerTime != firstResponse.ServerTime {
		t.Fatalf("replayed server time = %q, want %q", replayResponse.ServerTime, firstResponse.ServerTime)
	}
	afterReplay := model.Snapshot()
	if len(afterReplay.Stream.SourceRows) != len(beforeReplay.Stream.SourceRows) || len(afterReplay.Stream.Transactions) != len(beforeReplay.Stream.Transactions) || len(afterReplay.Fences) != len(beforeReplay.Fences) || len(afterReplay.Batches) != len(beforeReplay.Batches) || len(afterReplay.Mutations) != len(beforeReplay.Mutations) {
		t.Fatalf("completed replay created server state: %#v", afterReplay)
	}
	if got := pushOpsClientSnapshot(t, afterReplay, pushOpsClientKey()).AcceptedWriteEpoch; got != pushOpsClientSnapshot(t, beforeReplay, pushOpsClientKey()).AcceptedWriteEpoch {
		t.Fatalf("completed replay changed accepted-write epoch from %d to %d", pushOpsClientSnapshot(t, beforeReplay, pushOpsClientKey()).AcceptedWriteEpoch, got)
	}
	if got := pushOpsBatchSnapshot(t, afterReplay, key); !reflect.DeepEqual(got, pushOpsBatchSnapshot(t, beforeReplay, key)) {
		t.Fatal("completed replay changed its batch ledger")
	}
	if got := pushOpsMutationSnapshot(t, afterReplay, MutationKey{Client: pushOpsClientKey(), Mutation: mutationID}); !reflect.DeepEqual(got, pushOpsMutationSnapshot(t, beforeReplay, MutationKey{Client: pushOpsClientKey(), Mutation: mutationID})) {
		t.Fatal("completed replay changed its mutation ledger")
	}
	beforeLocal := pushOpsLocalSnapshot(t, beforeReplay, pushOpsClientKey())
	afterLocal := pushOpsLocalSnapshot(t, afterReplay, pushOpsClientKey())
	if !reflect.DeepEqual(afterLocal.Outcomes, beforeLocal.Outcomes) {
		t.Fatal("completed replay created a local outcome")
	}
	if row := pushOpsSourceRow(t, afterReplay, table.ID, "replay-target"); row.Version != "changed-v2" || pushOpsFieldValue(t, row.FieldValues, pushOpsValueField) != `"changed"` {
		t.Fatalf("completed replay reevaluated changed authority: %#v", row)
	}
}

func TestTask6PushExpiredGenerationRejectsUnexecutedTransportFailureBatch(t *testing.T) {
	state, clock, schema, table := pushOpsFixture(t, true, true)
	model := pushOpsModel(t, state, clock)

	mutationID := MutationID(pushOpsUUID(1010))
	batchID := BatchID(pushOpsUUID(1011))
	columns := map[string]any{string(pushOpsValueField): "queued"}
	pushOpsLocalWrite(t, model, string(mutationID), table.ID, "expired-target", schema, DMLOperationInsert, nil, columns, "application")
	request := pushOpsRequest(t, schema, string(batchID), pushOpsWireMutation(string(mutationID), table.ID, "expired-target", schema, DMLOperationInsert, nil, columns))

	failed := pushOpsSubmit(t, model, request, "transport_failure", 1010)
	pushOpsRequirePushFailure(t, failed, 503, pushHTTPTemporaryUnavailable, true)
	clock.now = clock.now.Add(9 * time.Hour)
	pushOpsApply(t, model, "model", "expire-client-generation", map[string]any{
		"user_id":   string(pushOpsUser),
		"client_id": string(pushOpsClient),
	})

	retry := pushOpsSubmit(t, model, request, "apply", 1010)
	pushOpsRequirePushFailure(t, retry, 409, pushHTTPGenerationExpired, false)
	if len(model.Snapshot().Batches) != 0 || len(model.Snapshot().Mutations) != 0 {
		t.Fatal("expired generation retry created server ledgers")
	}
}

func TestTask6PushIdempotencyConflictsAndHistoricalMutationReplay(t *testing.T) {
	t.Run("changed normalized batch content conflicts without state change", func(t *testing.T) {
		state, clock, schema, table := pushOpsFixture(t, true, false)
		model := pushOpsModel(t, state, clock)
		mutationID := MutationID(pushOpsUUID(1010))
		batchID := BatchID(pushOpsUUID(1011))
		firstRequest := pushOpsRequest(t, schema, string(batchID), pushOpsWireMutation(string(mutationID), table.ID, "batch-conflict", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "one"}))
		pushOpsRequirePushSuccess(t, pushOpsSubmit(t, model, firstRequest, "apply", 1010), ReplayDispositionExecuted, 1)
		before := model.Snapshot()
		changedRequest := pushOpsRequest(t, schema, string(batchID), pushOpsWireMutation(string(mutationID), table.ID, "batch-conflict", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "two"}))
		result := pushOpsSubmit(t, model, changedRequest, "apply", 1011)
		pushOpsRequirePushFailure(t, result, 409, pushHTTPIdempotencyConflict, false)
		pushOpsRequireCanonicalErrorBody(t, result, pushHTTPIdempotencyConflict, false)
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("changed batch content changed state or created retry state")
		}
	})

	t.Run("equal mutation fingerprint returns the stored outcome in a new batch", func(t *testing.T) {
		state, clock, schema, table := pushOpsFixture(t, true, false)
		model := pushOpsModel(t, state, clock)
		mutationID := MutationID(pushOpsUUID(1020))
		firstBatch := BatchID(pushOpsUUID(1021))
		secondBatch := BatchID(pushOpsUUID(1022))
		mutation := pushOpsWireMutation(string(mutationID), table.ID, "mutation-replay", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "stored"})
		first := pushOpsSubmit(t, model, pushOpsRequest(t, schema, string(firstBatch), mutation), "apply", 1020)
		pushOpsRequirePushSuccess(t, first, ReplayDispositionExecuted, 1)
		before := model.Snapshot()
		stored := pushOpsMutationSnapshot(t, before, MutationKey{Client: pushOpsClientKey(), Mutation: mutationID})
		result := pushOpsSubmit(t, model, pushOpsRequest(t, schema, string(secondBatch), mutation), "apply", 1021)
		pushOpsRequirePushSuccess(t, result, ReplayDispositionExecuted, 1)
		response := pushOpsPushResponse(t, result)
		if got := pushOpsResponseOutcomeRaw(t, response, mutationID); !bytes.Equal(got, stored.Outcome.Response) {
			t.Fatalf("historical outcome = %s, want %s", got, stored.Outcome.Response)
		}
		outcome := pushOpsMutationOutcome(t, stored.Outcome.Response)
		if outcome.OutcomeSchema.Version != stored.OutcomeSchema.Version || outcome.OutcomeSchema.Hash != hex.EncodeToString(stored.OutcomeSchema.Hash[:]) {
			t.Fatalf("replayed outcome schema = %#v, want %#v", outcome.OutcomeSchema, stored.OutcomeSchema)
		}
		after := model.Snapshot()
		if !reflect.DeepEqual(after.Stream, before.Stream) || !reflect.DeepEqual(after.Rows, before.Rows) || !reflect.DeepEqual(after.Fences, before.Fences) || pushOpsClientSnapshot(t, after, pushOpsClientKey()).AcceptedWriteEpoch != pushOpsClientSnapshot(t, before, pushOpsClientKey()).AcceptedWriteEpoch {
			t.Fatal("historical mutation replay performed DML")
		}
		if len(after.Batches) != len(before.Batches)+1 || len(after.Mutations) != len(before.Mutations) {
			t.Fatalf("historical mutation replay ledgers = batches %d mutations %d", len(after.Batches), len(after.Mutations))
		}
	})

	t.Run("changed authored mutation content conflicts without a batch ledger", func(t *testing.T) {
		state, clock, schema, table := pushOpsFixture(t, true, false)
		model := pushOpsModel(t, state, clock)
		mutationID := MutationID(pushOpsUUID(1030))
		firstBatch := BatchID(pushOpsUUID(1031))
		firstMutation := pushOpsWireMutation(string(mutationID), table.ID, "mutation-conflict", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "one"})
		pushOpsRequirePushSuccess(t, pushOpsSubmit(t, model, pushOpsRequest(t, schema, string(firstBatch), firstMutation), "apply", 1030), ReplayDispositionExecuted, 1)
		before := model.Snapshot()
		changedMutation := pushOpsWireMutation(string(mutationID), table.ID, "mutation-conflict", schema, DMLOperationInsert, nil, map[string]any{string(pushOpsValueField): "two"})
		result := pushOpsSubmit(t, model, pushOpsRequest(t, schema, pushOpsUUID(1032), changedMutation), "apply", 1031)
		pushOpsRequirePushFailure(t, result, 409, pushHTTPIdempotencyConflict, false)
		pushOpsRequireCanonicalErrorBody(t, result, pushHTTPIdempotencyConflict, false)
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("changed mutation content created a batch ledger or changed state")
		}
	})
}

func TestTask6PushResponseLossRetainsAndReplaysSealedRequest(t *testing.T) {
	state, clock, schema, table := pushOpsFixture(t, true, true)
	model := pushOpsModel(t, state, clock)
	mutationID := MutationID(pushOpsUUID(1040))
	batchID := BatchID(pushOpsUUID(1041))
	columns := map[string]any{string(pushOpsValueField): "response-loss"}
	pushOpsLocalWrite(t, model, string(mutationID), table.ID, "response-loss", schema, DMLOperationInsert, nil, columns, "application")
	request := pushOpsRequest(t, schema, string(batchID), pushOpsWireMutation(string(mutationID), table.ID, "response-loss", schema, DMLOperationInsert, nil, columns))

	dropped := pushOpsSubmit(t, model, request, "drop_after_server", 1040)
	pushOpsRequirePushFailure(t, dropped, 503, pushHTTPTemporaryUnavailable, true)
	if dropped.Push.Replay != ReplayDispositionExecuted {
		t.Fatalf("dropped first delivery replay = %q", dropped.Push.Replay)
	}
	droppedSnapshot := model.Snapshot()
	key := BatchKey{Client: pushOpsClientKey(), Batch: batchID}
	if len(droppedSnapshot.Stream.SourceRows) != 1 || len(droppedSnapshot.Stream.Transactions) != 1 || len(droppedSnapshot.Fences) != 1 || len(droppedSnapshot.Batches) != 1 || len(droppedSnapshot.Mutations) != 1 || pushOpsClientSnapshot(t, droppedSnapshot, pushOpsClientKey()).AcceptedWriteEpoch != 1 {
		t.Fatalf("drop-after-server did not commit exactly once: %#v", droppedSnapshot)
	}
	droppedLocal := pushOpsLocalSnapshot(t, droppedSnapshot, pushOpsClientKey())
	droppedSealed := pushOpsSealedBatch(t, droppedLocal, batchID)
	if droppedSealed.State != LocalSealedBatchStateResponseLost || droppedSealed.HasCanonicalResponse || len(droppedSealed.CanonicalResponse) != 0 || !bytes.Equal(droppedSealed.CanonicalRequest, pushOpsBatchSnapshot(t, droppedSnapshot, key).SealedCanonicalRequest) {
		t.Fatalf("drop-after-server local sealed batch = %#v", droppedSealed)
	}
	if droppedLocal.Backoff == nil || droppedLocal.Backoff.Retry != RetryClassificationTransport || droppedLocal.Backoff.Attempt != 1 || !droppedLocal.Backoff.Work.HasBatch || droppedLocal.Backoff.Work.Batch != key || len(droppedLocal.Outcomes) != 0 {
		t.Fatalf("drop-after-server local retry state = %#v", droppedLocal)
	}

	responseLoss := pushOpsApply(t, model, "process", "response-loss", map[string]any{
		"authenticated_user_id": string(pushOpsUser),
		"client_id":             string(pushOpsClient),
		"batch_id":              string(batchID),
	})
	pushOpsRequirePushFailure(t, responseLoss, 503, pushHTTPTemporaryUnavailable, true)
	afterResponseLoss := model.Snapshot()
	if !reflect.DeepEqual(afterResponseLoss.Stream, droppedSnapshot.Stream) || !reflect.DeepEqual(afterResponseLoss.Fences, droppedSnapshot.Fences) || !reflect.DeepEqual(afterResponseLoss.Batches, droppedSnapshot.Batches) || !reflect.DeepEqual(afterResponseLoss.Mutations, droppedSnapshot.Mutations) || !reflect.DeepEqual(afterResponseLoss.Rows, droppedSnapshot.Rows) {
		t.Fatal("process/response-loss reconciled or changed server state")
	}
	lostLocal := pushOpsLocalSnapshot(t, afterResponseLoss, pushOpsClientKey())
	if lostLocal.Backoff == nil || lostLocal.Backoff.Attempt != 2 || lostLocal.Backoff.Retry != RetryClassificationTransport || len(lostLocal.Outcomes) != 0 || pushOpsSealedBatch(t, lostLocal, batchID).HasCanonicalResponse {
		t.Fatalf("process/response-loss did more than record loss and backoff: %#v", lostLocal)
	}

	replay := pushOpsSubmit(t, model, request, "apply", 1041)
	pushOpsRequirePushSuccess(t, replay, ReplayDispositionReplayed, 1)
	completed := pushOpsBatchSnapshot(t, model.Snapshot(), key)
	if !bytes.Equal(replay.HTTP.Body, completed.SealedCanonicalResponse) {
		t.Fatal("sealed request replay did not return the completed server response")
	}
	local := pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey())
	sealed := pushOpsSealedBatch(t, local, batchID)
	if sealed.State != LocalSealedBatchStateReconciled || !sealed.HasCanonicalResponse || !bytes.Equal(sealed.CanonicalResponse, completed.SealedCanonicalResponse) || !bytes.Equal(sealed.CanonicalRequest, droppedSealed.CanonicalRequest) || local.Backoff != nil || len(local.Outcomes) != 1 {
		t.Fatalf("sealed request replay did not reconcile once: %#v", local)
	}
	localAfterFirstReplay := local
	pushOpsRequirePushSuccess(t, pushOpsSubmit(t, model, request, "apply", 1042), ReplayDispositionReplayed, 1)
	if afterSecondReplay := pushOpsLocalSnapshot(t, model.Snapshot(), pushOpsClientKey()); !reflect.DeepEqual(afterSecondReplay, localAfterFirstReplay) {
		t.Fatal("a second completed replay reconciled the local batch again")
	}
}

func TestTask6PushTransportFailureSealsOnlyLocalRetryState(t *testing.T) {
	state, clock, schema, table := pushOpsFixture(t, true, true)
	model := pushOpsModel(t, state, clock)
	mutationID := MutationID(pushOpsUUID(1050))
	batchID := BatchID(pushOpsUUID(1051))
	columns := map[string]any{string(pushOpsValueField): "transport"}
	pushOpsLocalWrite(t, model, string(mutationID), table.ID, "transport-failure", schema, DMLOperationInsert, nil, columns, "application")
	request := pushOpsRequest(t, schema, string(batchID), pushOpsWireMutation(string(mutationID), table.ID, "transport-failure", schema, DMLOperationInsert, nil, columns))
	before := model.Snapshot()
	result := pushOpsSubmit(t, model, request, "transport_failure", 1050)
	pushOpsRequirePushFailure(t, result, 503, pushHTTPTemporaryUnavailable, true)
	after := model.Snapshot()
	if len(after.Stream.SourceRows) != len(before.Stream.SourceRows) || len(after.Stream.Transactions) != len(before.Stream.Transactions) || len(after.Fences) != len(before.Fences) || len(after.Batches) != len(before.Batches) || len(after.Mutations) != len(before.Mutations) || pushOpsClientSnapshot(t, after, pushOpsClientKey()).AcceptedWriteEpoch != pushOpsClientSnapshot(t, before, pushOpsClientKey()).AcceptedWriteEpoch {
		t.Fatal("transport failure reached server execution")
	}
	local := pushOpsLocalSnapshot(t, after, pushOpsClientKey())
	sealed := pushOpsSealedBatch(t, local, batchID)
	canonicalRequest, err := jcs.Transform(request)
	if err != nil {
		t.Fatalf("canonicalize transport request: %v", err)
	}
	if sealed.State != LocalSealedBatchStateResponseLost || !bytes.Equal(sealed.CanonicalRequest, canonicalRequest) || sealed.HasCanonicalResponse || len(local.Outcomes) != 0 {
		t.Fatalf("transport failure did not retain only the sealed request: %#v", local)
	}
	if local.Backoff == nil || local.Backoff.Retry != RetryClassificationTransport || local.Backoff.Attempt != 1 || !local.Backoff.Work.HasBatch || local.Backoff.Work.Batch != (BatchKey{Client: pushOpsClientKey(), Batch: batchID}) {
		t.Fatalf("transport failure retry state = %#v", local.Backoff)
	}
}

func pushOpsRequireTask6LedgerRecords(t *testing.T, batch BatchLedger, mutation MutationLedger, schema SchemaRef, batchID BatchID, mutationID MutationID, table TableID, response, request []byte, now time.Time) {
	t.Helper()
	canonicalRequest, err := jcs.Transform(request)
	if err != nil {
		t.Fatalf("canonicalize batch request: %v", err)
	}
	if batch.Fingerprint.Algorithm != "sha256" || batch.Fingerprint.Domain != pushBatchDomain || batch.Fingerprint.Version != 1 || !pushOpsNonzeroDigest(batch.Fingerprint.Digest) || batch.ProtocolVersion != supportedProtocolVersion || batch.ClientGeneration != 1 || batch.Schema != schema || !bytes.Equal(batch.SealedCanonicalRequest, canonicalRequest) || !bytes.Equal(batch.SealedCanonicalResponse, response) || batch.Execution != BatchExecutionCompleted || !reflect.DeepEqual(batch.Mutations, []MutationID{mutationID}) || batch.HTTPStatus != 200 || batch.ServerTime == nil || batch.CreatedAt == nil || batch.CompletedAt == nil || batch.SealedAt == nil || !batch.ServerTime.Equal(now) || !batch.CreatedAt.Equal(now) || !batch.CompletedAt.Equal(now) || !batch.SealedAt.Equal(now) {
		t.Fatalf("typed batch ledger = %#v", batch)
	}
	if len(batch.Outcomes) != 1 || batch.Outcomes[0].Mutation != mutationID || batch.Outcomes[0].State != MutationOutcomeApplied || batch.Outcomes[0].Reason != "" || !bytes.Equal(batch.Outcomes[0].Response, mutation.Outcome.Response) {
		t.Fatalf("typed batch outcome = %#v", batch.Outcomes)
	}
	if mutation.Fingerprint.Algorithm != "sha256" || mutation.Fingerprint.Domain != pushMutationDomain || mutation.Fingerprint.Version != 1 || !pushOpsNonzeroDigest(mutation.Fingerprint.Digest) || mutation.FirstBatch != batchID || mutation.RequestOrdinal != 1 || mutation.Table != table || mutation.Row.TableID != table || mutation.Row.PrimaryKeyFieldID != pushOpsIDField || mutation.Row.PortableType != "string" || mutation.Row.CanonicalIdentityBytes == "" || mutation.Row.CanonicalWireJSON != `"replay-target"` || mutation.Operation != DMLOperationInsert || mutation.AuthoredSchema != schema || mutation.SubmittedSchema != schema || mutation.OutcomeSchema != schema || !bytes.Equal(mutation.SealedCanonicalRequest, pushOpsMutationWireCanonical(t, request, 0)) || !bytes.Equal(mutation.SealedCanonicalResponse, mutation.Outcome.Response) || mutation.Outcome.Mutation != mutationID || mutation.Outcome.State != MutationOutcomeApplied || mutation.Outcome.Reason != "" || mutation.ResolvedAt == nil || !mutation.ResolvedAt.Equal(now) {
		t.Fatalf("typed mutation ledger = %#v", mutation)
	}
}

func pushOpsFieldValue(t *testing.T, values []FieldValue, field FieldID) string {
	t.Helper()
	for _, value := range values {
		if value.Field == field {
			return value.WireJSON
		}
	}
	t.Fatalf("field %q is absent", field)
	return ""
}

func pushOpsFixture(t *testing.T, softDelete, local bool) (State, *pushOpsClock, SchemaRef, pushOpsTableSpec) {
	t.Helper()
	table := pushOpsDefaultTable(pushOpsTable, pushOpsRelation, softDelete)
	schema, manifest := pushOpsManifest(t, 1, nil, SchemaClassInitial, 1, table)
	clock := &pushOpsClock{now: pushOpsFixtureTime}
	state := pushOpsState(schema, map[SchemaRef]SchemaManifest{schema: manifest}, []pushOpsTableSpec{table}, local, clock.now)
	return state, clock, schema, table
}

func pushOpsDefaultTable(table TableID, relation RelationID, softDelete bool) pushOpsTableSpec {
	precision := 6
	scale := 2
	created := pushOpsCreatedField
	updated := pushOpsUpdatedField
	fields := []pushOpsFieldSpec{
		{ID: pushOpsIDField, Name: "id", Type: "string", Nullable: false, Writable: false},
		{ID: pushOpsValueField, Name: "value", Type: "string", Nullable: false, Writable: true},
		{ID: pushOpsNoteField, Name: "note", Type: "string", Nullable: true, Writable: true},
		{ID: pushOpsAmountField, Name: "amount", Type: "decimal", Nullable: true, Writable: true, Precision: &precision, Scale: &scale},
		{ID: pushOpsCreatedField, Name: "created_at", Type: "datetime", Nullable: false, Writable: false},
		{ID: pushOpsUpdatedField, Name: "updated_at", Type: "datetime", Nullable: false, Writable: false},
	}
	result := pushOpsTableSpec{
		ID:        table,
		Relation:  relation,
		Name:      string(table),
		Primary:   pushOpsIDField,
		Fields:    fields,
		CreatedAt: &created,
		UpdatedAt: &updated,
	}
	if softDelete {
		deleted := pushOpsDeletedField
		result.DeletedAt = &deleted
		result.Fields = append(result.Fields, pushOpsFieldSpec{ID: pushOpsDeletedField, Name: "deleted_at", Type: "datetime", Nullable: true, Writable: false})
	}
	return result
}

func pushOpsCloneTable(table pushOpsTableSpec) pushOpsTableSpec {
	result := table
	result.Fields = append([]pushOpsFieldSpec(nil), table.Fields...)
	for index := range result.Fields {
		if table.Fields[index].Precision != nil {
			value := *table.Fields[index].Precision
			result.Fields[index].Precision = &value
		}
		if table.Fields[index].Scale != nil {
			value := *table.Fields[index].Scale
			result.Fields[index].Scale = &value
		}
	}
	result.CreatedAt = pushOpsCloneFieldID(table.CreatedAt)
	result.UpdatedAt = pushOpsCloneFieldID(table.UpdatedAt)
	result.DeletedAt = pushOpsCloneFieldID(table.DeletedAt)
	return result
}

func pushOpsCloneFieldID(value *FieldID) *FieldID {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func pushOpsManifest(t *testing.T, version uint64, parent *SchemaRef, class SchemaClass, compatibilityFloor uint64, tables ...pushOpsTableSpec) (SchemaRef, SchemaManifest) {
	t.Helper()
	document := map[string]any{
		"schema_version":      version,
		"parent_schema":       nil,
		"transition_class":    string(class),
		"compatibility_floor": compatibilityFloor,
		"tables":              pushOpsManifestTableDocuments(tables),
	}
	if parent != nil {
		document["parent_schema"] = pushOpsSchemaValue(*parent)
	}
	body := pushOpsCanonicalJSON(t, document)
	preimage := append([]byte("synchro:v3:schema-manifest:v1\x00"), body...)
	hash := sha256.Sum256(preimage)
	document["schema_hash"] = hex.EncodeToString(hash[:])
	canonical := pushOpsCanonicalJSON(t, document)
	parsed, err := vectors.ParseManifest(canonical)
	if err != nil {
		t.Fatalf("parse generated manifest: %v", err)
	}
	if parsed.Hash() != hash {
		t.Fatal("generated manifest hash differs from its frozen-vector parse")
	}
	ref := SchemaRef{Version: version, Hash: hash}
	return ref, SchemaManifest{
		Body:               canonical,
		Parent:             cloneSchemaReference(parent),
		Tables:             pushOpsTableManifests(tables),
		Class:              class,
		CompatibilityFloor: compatibilityFloor,
	}
}

func cloneSchemaReference(reference *SchemaRef) *SchemaRef {
	if reference == nil {
		return nil
	}
	result := *reference
	return &result
}

func pushOpsManifestTableDocuments(tables []pushOpsTableSpec) []any {
	sortedTables := append([]pushOpsTableSpec(nil), tables...)
	sort.Slice(sortedTables, func(left, right int) bool {
		return sortedTables[left].ID < sortedTables[right].ID
	})
	result := make([]any, 0, len(sortedTables))
	for _, table := range sortedTables {
		sortedFields := append([]pushOpsFieldSpec(nil), table.Fields...)
		sort.Slice(sortedFields, func(left, right int) bool {
			return sortedFields[left].ID < sortedFields[right].ID
		})
		fields := make([]any, 0, len(sortedFields))
		for _, field := range sortedFields {
			document := map[string]any{
				"field_id": string(field.ID),
				"name":     field.Name,
				"type":     string(field.Type),
				"nullable": field.Nullable,
				"writable": field.Writable,
			}
			if field.Precision != nil {
				document["precision"] = *field.Precision
			}
			if field.Scale != nil {
				document["scale"] = *field.Scale
			}
			fields = append(fields, document)
		}
		result = append(result, map[string]any{
			"table_id":             string(table.ID),
			"relation_id":          string(table.Relation),
			"name":                 table.Name,
			"composition":          string(CompositionSingleScope),
			"primary_key_field_id": string(table.Primary),
			"lifecycle": map[string]any{
				"created_at_field_id": pushOpsNullableFieldID(table.CreatedAt),
				"updated_at_field_id": pushOpsNullableFieldID(table.UpdatedAt),
				"deleted_at_field_id": pushOpsNullableFieldID(table.DeletedAt),
			},
			"fields":  fields,
			"indexes": []any{},
		})
	}
	return result
}

func pushOpsNullableFieldID(field *FieldID) any {
	if field == nil {
		return nil
	}
	return string(*field)
}

func pushOpsTableManifests(tables []pushOpsTableSpec) []TableManifest {
	result := make([]TableManifest, 0, len(tables))
	for _, table := range tables {
		manifest := TableManifest{
			ID:                table.ID,
			Relation:          table.Relation,
			Name:              table.Name,
			Composition:       CompositionSingleScope,
			PrimaryKeyFieldID: table.Primary,
			CreatedFieldID:    pushOpsCloneFieldID(table.CreatedAt),
			UpdatedFieldID:    pushOpsCloneFieldID(table.UpdatedAt),
			DeletedFieldID:    pushOpsCloneFieldID(table.DeletedAt),
		}
		for _, field := range table.Fields {
			entry := FieldManifest{
				ID:           field.ID,
				Name:         field.Name,
				PortableType: field.Type,
				PrimaryKey:   field.ID == table.Primary,
				Nullable:     field.Nullable,
				Writable:     field.Writable,
			}
			if field.Precision != nil {
				entry.HasDecimalPrecision = true
				entry.DecimalPrecision = uint32(*field.Precision)
			}
			if field.Scale != nil {
				entry.HasDecimalScale = true
				entry.DecimalScale = uint32(*field.Scale)
			}
			manifest.Fields = append(manifest.Fields, entry)
		}
		result = append(result, manifest)
	}
	return result
}

func pushOpsCanonicalJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal canonical JSON fixture: %v", err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		t.Fatalf("canonicalize JSON fixture: %v", err)
	}
	return canonical
}

func pushOpsState(current SchemaRef, schemas map[SchemaRef]SchemaManifest, activeTables []pushOpsTableSpec, local bool, now time.Time) State {
	client := pushOpsClientKey()
	registryRelations := make([]RegistryRelation, 0, len(activeTables))
	relations := make(map[RelationID]RelationState, len(activeTables))
	for index, table := range activeTables {
		definition := pushOpsRelationDefinition(table, uint32(400+index))
		registryRelations = append(registryRelations, RegistryRelation{Definition: definition})
		relations[table.Relation] = RelationState{Definition: definition}
	}
	assignments := []ScopeAssignment{{
		Scope:                pushOpsScope,
		MembershipGeneration: 1,
		RetentionGeneration:  1,
		Assigned:             true,
	}}
	state := State{
		ProtocolVersion: 3,
		Schemas:         schemas,
		CurrentSchema:   current,
		Registry: RegistryState{
			CurrentGeneration: 1,
			Generations: []RegistryGenerationState{{
				Generation: 1,
				Validated:  true,
				Relations:  registryRelations,
			}},
		},
		Relations: relations,
		Clients: map[ClientKey]ClientState{
			client: {
				CurrentGeneration: 1,
				Generations:       []ClientGenerationState{{Generation: 1, CreatedAt: timePointer(now)}},
				ScopeSetVersion:   1,
				ScopeAssignments:  assignments,
			},
		},
		Rows: make(map[RowIdentity]AuthoritativeRow),
		Scopes: map[ScopeID]ScopeState{
			pushOpsScope: {
				Schema:               current,
				MembershipGeneration: 1,
				RetentionGeneration:  1,
				StreamGeneration:     pushOpsStream,
				HighWatermark:        StreamPosition{StreamGeneration: pushOpsStream, Kind: PositionKindGenerationStart},
			},
		},
		Stream: StreamState{Authority: StreamAuthority{
			ActiveGeneration:              pushOpsStream,
			GlobalMaterializationBoundary: StreamPosition{StreamGeneration: pushOpsStream, Kind: PositionKindGenerationStart},
		}},
		Fences:      make(map[FenceID]VersionFence),
		Projections: make(map[ProjectionKey]CapturedProjection),
		Batches:     make(map[BatchKey]BatchLedger),
		Mutations:   make(map[MutationKey]MutationLedger),
		Rebuilds:    make(map[RebuildKey]RebuildSession),
		ClientLocal: make(map[ClientKey]ClientLocalState),
		Installation: InstallationCapabilities{
			ProtocolVersion:                 supportedProtocolVersion,
			MinimumClientRuntime:            supportedProtocolVersion,
			StaleClientIntervalMilliseconds: uint64(time.Hour / time.Millisecond),
		},
	}
	if local {
		state.ClientLocal[client] = ClientLocalState{
			ClientGeneration:             1,
			CurrentSchema:                current,
			AuthoritativeScopeSetVersion: 1,
			ScopeAssignments: []LocalScopeAssignment{{
				Scope:                pushOpsScope,
				MembershipGeneration: 1,
				RetentionGeneration:  1,
				Assigned:             true,
			}},
			ScopeCheckpoints: []LocalScopeCheckpoint{{Scope: pushOpsScope}},
			Lifecycle:        ClientLifecycleState{State: ClientLifecycleReady, ChangedAt: timePointer(now)},
		}
	}
	return state
}

func pushOpsRelationDefinition(table pushOpsTableSpec, oid uint32) RelationDefinition {
	fields := make([]FieldID, 0, len(table.Fields))
	primaryType := PortableType("")
	for _, field := range table.Fields {
		fields = append(fields, field.ID)
		if field.ID == table.Primary {
			primaryType = field.Type
		}
	}
	return RelationDefinition{
		Relation:                 table.Relation,
		RegistrationKind:         RegistrationKindSynced,
		HasTableID:               true,
		TableID:                  table.ID,
		Physical:                 PhysicalRelation{Schema: "app", Name: string(table.Relation), OID: oid, ReplicaIdentity: ReplicaIdentityDefault},
		PrimaryKeyFieldID:        table.Primary,
		PrimaryKeyPhysicalColumn: "id",
		PrimaryKeyPortableType:   primaryType,
		CapturedFieldIDs:         fields,
		MembershipFunction:       "push-ops-membership",
		PositiveFanoutBound:      8,
	}
}

func pushOpsInstallLiveRow(t *testing.T, state *State, schema SchemaRef, table pushOpsTableSpec, id, value string, version RowVersion, deleted bool) RowIdentity {
	t.Helper()
	manifest, found := state.Schemas[schema]
	if !found {
		t.Fatalf("schema %d is absent", schema.Version)
	}
	row := pushOpsAuthoritativeRow(t, manifest, table, id, value, version, deleted)
	state.Stream.SourceRows = append(state.Stream.SourceRows, SourceRowEntry{Identity: row.Identity, Row: row})
	state.Rows[row.Identity] = cloneAuthoritativeRow(row)
	return row.Identity
}

func pushOpsAuthoritativeRow(t *testing.T, schema SchemaManifest, table pushOpsTableSpec, id, value string, version RowVersion, deleted bool) AuthoritativeRow {
	t.Helper()
	manifest, err := vectors.ParseManifest(schema.Body)
	if err != nil {
		t.Fatalf("parse fixture manifest: %v", err)
	}
	pk, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("marshal fixture primary key: %v", err)
	}
	identityBytes, err := vectors.RowIdentity(manifest, string(table.ID), pk)
	if err != nil {
		t.Fatalf("derive fixture row identity: %v", err)
	}
	values := make([]FieldValue, 0, len(table.Fields))
	digestFields := make([]vectors.RowField, 0, len(table.Fields))
	for _, field := range table.Fields {
		wire := pushOpsFixtureFieldWire(t, table, field, id, value, deleted)
		values = append(values, FieldValue{Field: field.ID, Type: field.Type, WireJSON: wire})
		digestFields = append(digestFields, vectors.RowField{FieldID: string(field.ID), Value: json.RawMessage(wire)})
	}
	digest, err := vectors.RowDigest(manifest, string(table.ID), vectors.Row{PK: pk, Fields: digestFields}, string(version))
	if err != nil {
		t.Fatalf("derive fixture row digest: %v", err)
	}
	identity := RowIdentity{
		CanonicalIdentityBytes: string(identityBytes),
		TableID:                table.ID,
		PrimaryKeyFieldID:      table.Primary,
		PortableType:           pushOpsTableField(t, table, table.Primary).Type,
		CanonicalWireJSON:      string(pk),
	}
	row := AuthoritativeRow{
		Identity:     identity,
		FieldValues:  values,
		Version:      version,
		Checksum:     Checksum(digest),
		Deleted:      deleted,
		UpdatedAt:    timePointer(pushOpsFixtureTime),
		DeletedAt:    nil,
		DeleteReason: nil,
	}
	if deleted {
		row.DeletedAt = timePointer(pushOpsFixtureTime)
	}
	return row
}

func pushOpsFixtureFieldWire(t *testing.T, table pushOpsTableSpec, field pushOpsFieldSpec, id, value string, deleted bool) string {
	t.Helper()
	if field.ID == table.Primary {
		return pushOpsJSONString(t, id)
	}
	if table.CreatedAt != nil && field.ID == *table.CreatedAt || table.UpdatedAt != nil && field.ID == *table.UpdatedAt {
		return pushOpsJSONString(t, formatCanonicalTime(pushOpsFixtureTime))
	}
	if table.DeletedAt != nil && field.ID == *table.DeletedAt {
		if deleted {
			return pushOpsJSONString(t, formatCanonicalTime(pushOpsFixtureTime))
		}
		return "null"
	}
	switch field.ID {
	case pushOpsValueField:
		return pushOpsJSONString(t, value)
	case pushOpsNoteField:
		return pushOpsJSONString(t, "fixture note")
	case pushOpsAmountField:
		return `"12.34"`
	}
	if field.Nullable {
		return "null"
	}
	switch field.Type {
	case "string", "int64", "decimal", "datetime", "date", "time", "json", "bytes":
		return pushOpsJSONString(t, "fixture")
	case "int", "float":
		return "1"
	case "boolean":
		return "true"
	default:
		t.Fatalf("unsupported fixture field type %q", field.Type)
		return ""
	}
}

func pushOpsJSONString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal fixture JSON string: %v", err)
	}
	return string(encoded)
}

func pushOpsTableField(t *testing.T, table pushOpsTableSpec, wanted FieldID) pushOpsFieldSpec {
	t.Helper()
	for _, field := range table.Fields {
		if field.ID == wanted {
			return field
		}
	}
	t.Fatalf("table %q has no field %q", table.ID, wanted)
	return pushOpsFieldSpec{}
}

func pushOpsInstallLocalRow(t *testing.T, state *State, client ClientKey, row AuthoritativeRow) {
	t.Helper()
	local, found := state.ClientLocal[client]
	if !found {
		t.Fatalf("local client %#v is absent", client)
	}
	local.Rows = append(local.Rows, LocalRow{
		Identity:         row.Identity,
		Fields:           cloneFieldValues(row.FieldValues),
		Deleted:          row.Deleted,
		HasServerVersion: true,
		ServerVersion:    row.Version,
		HasChecksum:      true,
		Checksum:         row.Checksum,
		UpdatedAt:        cloneTime(row.UpdatedAt),
	})
	state.ClientLocal[client] = local
}

func pushOpsClientKey() ClientKey {
	return ClientKey{UserID: pushOpsUser, ClientID: pushOpsClient}
}

func pushOpsVersionPointer(value RowVersion) *RowVersion {
	return &value
}

func pushOpsUUID(sequence uint64) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", sequence)
}

func pushOpsModel(t *testing.T, state State, clock Clock) *Model {
	t.Helper()
	model, err := New(Config{State: state, Clock: clock, Seed: 731})
	if err != nil {
		t.Fatalf("create push operations model: %v", err)
	}
	return model
}

func pushOpsApply(t *testing.T, model *Model, contract, name string, payload any) StepResult {
	t.Helper()
	var raw []byte
	switch value := payload.(type) {
	case []byte:
		raw = append([]byte(nil), value...)
	case json.RawMessage:
		raw = append([]byte(nil), value...)
	case string:
		raw = []byte(value)
	default:
		var err error
		raw, err = json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s/%s payload: %v", contract, name, err)
		}
	}
	result, err := model.Apply(context.Background(), scenarios.Operation{ContractOperation: contract, Name: name, Payload: raw})
	if err != nil {
		t.Fatalf("apply %s/%s: %v", contract, name, err)
	}
	return result
}

func pushOpsLocalWrite(t *testing.T, model *Model, mutation string, table TableID, pk string, schema SchemaRef, operation DMLOperation, base *RowVersion, columns map[string]any, origin string) StepResult {
	t.Helper()
	payload := map[string]any{
		"authenticated_user_id": string(pushOpsUser),
		"client_id":             string(pushOpsClient),
		"mutation_id":           mutation,
		"table_id":              string(table),
		"pk":                    map[string]any{string(pushOpsIDField): pk},
		"authored_schema":       pushOpsSchemaValue(schema),
		"operation":             string(operation),
		"client_version":        string(pushOpsClientVersion),
		"origin":                origin,
	}
	if base != nil {
		payload["base_version"] = string(*base)
	}
	if columns != nil {
		payload["columns"] = columns
	}
	return pushOpsApply(t, model, "local", "write", payload)
}

func pushOpsSchemaValue(schema SchemaRef) map[string]any {
	return map[string]any{"version": schema.Version, "hash": hex.EncodeToString(schema.Hash[:])}
}

func pushOpsWireMutation(mutation string, table TableID, pk string, schema SchemaRef, operation DMLOperation, base *RowVersion, columns map[string]any) map[string]any {
	result := map[string]any{
		"mutation_id":     mutation,
		"table":           string(table),
		"pk":              map[string]any{string(pushOpsIDField): pk},
		"authored_schema": pushOpsSchemaValue(schema),
		"op":              string(operation),
		"client_version":  string(pushOpsClientVersion),
	}
	if base != nil {
		result["base_version"] = string(*base)
	}
	if columns != nil {
		result["columns"] = columns
	}
	return result
}

func pushOpsRequest(t *testing.T, schema SchemaRef, batch string, mutations ...map[string]any) []byte {
	t.Helper()
	values := make([]any, 0, len(mutations))
	for _, mutation := range mutations {
		values = append(values, mutation)
	}
	encoded, err := json.Marshal(map[string]any{
		"client_id":         string(pushOpsClient),
		"client_generation": 1,
		"batch_id":          batch,
		"schema":            pushOpsSchemaValue(schema),
		"mutations":         values,
	})
	if err != nil {
		t.Fatalf("marshal push request: %v", err)
	}
	return encoded
}

func pushOpsSubmit(t *testing.T, model *Model, request []byte, delivery string, commit uint64) StepResult {
	t.Helper()
	return pushOpsApply(t, model, "push", "submit", map[string]any{
		"authenticated_user_id": string(pushOpsUser),
		"request":               json.RawMessage(request),
		"delivery":              delivery,
		"commit_lsn":            fmt.Sprintf("%d", commit),
		"end_lsn":               fmt.Sprintf("%d", commit+1),
	})
}

func pushOpsRequirePushSuccess(t *testing.T, result StepResult, replay ReplayDisposition, mutations int) {
	t.Helper()
	if result.Kind != StepResultKindPush || result.HTTP == nil || result.Push == nil || result.HTTP.Status != 200 || result.HTTP.HasCode || result.Push.Replay != replay || len(result.Push.Mutations) != mutations {
		if result.HTTP == nil || result.Push == nil {
			t.Fatalf("push success has incomplete observations: %#v", result)
		}
		t.Fatalf("push success status=%d code=%q replay=%q mutations=%#v body=%s", result.HTTP.Status, result.HTTP.Code, result.Push.Replay, result.Push.Mutations, result.HTTP.Body)
	}
	canonical, err := jcs.Transform(result.HTTP.Body)
	if err != nil || !bytes.Equal(canonical, result.HTTP.Body) {
		t.Fatal("push success body is not canonical JSON")
	}
}

func pushOpsRequirePushFailure(t *testing.T, result StepResult, status int, code HTTPCode, retryable bool) {
	t.Helper()
	if result.Kind != StepResultKindPush || result.HTTP == nil || result.Push == nil || result.HTTP.Status != status || !result.HTTP.HasCode || result.HTTP.Code != code || result.HTTP.Retryable != retryable || len(result.Push.Mutations) != 0 {
		if result.HTTP == nil || result.Push == nil {
			t.Fatalf("push failure has incomplete observations: %#v", result)
		}
		t.Fatalf("push failure status=%d code=%q retryable=%t replay=%q mutations=%#v body=%s", result.HTTP.Status, result.HTTP.Code, result.HTTP.Retryable, result.Push.Replay, result.Push.Mutations, result.HTTP.Body)
	}
	if retryable {
		if !result.HTTP.HasRetryAfterMilliseconds || result.HTTP.RetryAfterMilliseconds != 1000 {
			t.Fatalf("retryable push failure retry-after = %#v", result.HTTP)
		}
	} else if result.HTTP.HasRetryAfterMilliseconds {
		t.Fatal("nonretryable push failure has retry-after")
	}
	var body pushErrorEnvelope
	if err := json.Unmarshal(result.HTTP.Body, &body); err != nil || body.Error.Code != string(code) || body.Error.Retryable != retryable {
		t.Fatalf("push failure body = %s", result.HTTP.Body)
	}
}

func pushOpsPushResponse(t *testing.T, result StepResult) pushResponse {
	t.Helper()
	if result.HTTP == nil {
		t.Fatal("push result has no HTTP observation")
	}
	var response pushResponse
	if err := json.Unmarshal(result.HTTP.Body, &response); err != nil {
		t.Fatalf("decode push response: %v", err)
	}
	return response
}

func pushOpsOnlyAcceptedOutcome(t *testing.T, result StepResult) pushOutcomeWire {
	t.Helper()
	response := pushOpsPushResponse(t, result)
	if len(response.Accepted) != 1 || len(response.Rejected) != 0 {
		t.Fatalf("push outcome partition = accepted %d, rejected %d", len(response.Accepted), len(response.Rejected))
	}
	var outcome pushOutcomeWire
	if err := json.Unmarshal(response.Accepted[0], &outcome); err != nil {
		t.Fatalf("decode accepted outcome: %v", err)
	}
	return outcome
}

func pushOpsClientSnapshot(t *testing.T, snapshot StateSnapshot, key ClientKey) ClientState {
	t.Helper()
	for _, entry := range snapshot.Clients {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("client %#v is absent from snapshot", key)
	return ClientState{}
}

func pushOpsLocalSnapshot(t *testing.T, snapshot StateSnapshot, key ClientKey) ClientLocalState {
	t.Helper()
	for _, entry := range snapshot.ClientLocal {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("local client %#v is absent from snapshot", key)
	return ClientLocalState{}
}

func pushOpsQueuedMutation(t *testing.T, local ClientLocalState, mutation MutationID) QueuedMutation {
	t.Helper()
	for _, queued := range local.DurableQueue {
		if queued.Mutation == mutation {
			return queued
		}
	}
	t.Fatalf("queued mutation %q is absent", mutation)
	return QueuedMutation{}
}

func pushOpsSealedBatch(t *testing.T, local ClientLocalState, batch BatchID) LocalSealedBatch {
	t.Helper()
	for _, sealed := range local.SealedBatches {
		if sealed.Batch == batch {
			return sealed
		}
	}
	t.Fatalf("sealed batch %q is absent", batch)
	return LocalSealedBatch{}
}

func pushOpsOnlyPendingMutation(t *testing.T, local ClientLocalState) QueuedMutation {
	t.Helper()
	pending := pushOpsPendingMutations(local)
	if len(pending) != 1 {
		t.Fatalf("pending mutation count = %d, want 1", len(pending))
	}
	return pending[0]
}

func pushOpsPendingMutations(local ClientLocalState) []QueuedMutation {
	result := make([]QueuedMutation, 0)
	for _, mutation := range local.DurableQueue {
		if mutation.Status == LocalMutationStatusPending {
			result = append(result, mutation)
		}
	}
	return result
}

func pushOpsRequireLocalOutcome(t *testing.T, local ClientLocalState, mutation MutationID, reason ReasonCode) {
	t.Helper()
	for _, outcome := range local.Outcomes {
		if outcome.Mutation == mutation && outcome.Reason == reason {
			return
		}
	}
	t.Fatalf("local outcome for %q with reason %q is absent", mutation, reason)
}

func pushOpsLocalRow(t *testing.T, local ClientLocalState, table TableID, pk string) LocalRow {
	t.Helper()
	for _, row := range local.Rows {
		if row.Identity.TableID == table && row.Identity.CanonicalWireJSON == pushOpsJSONString(t, pk) {
			return row
		}
	}
	t.Fatalf("local row %q/%q is absent", table, pk)
	return LocalRow{}
}

func pushOpsLocalField(t *testing.T, row LocalRow, field FieldID) string {
	t.Helper()
	for _, value := range row.Fields {
		if value.Field == field {
			return value.WireJSON
		}
	}
	t.Fatalf("local row has no field %q", field)
	return ""
}

func pushOpsSortedOutcomeIDs(raw []json.RawMessage) []MutationID {
	result := make([]MutationID, 0, len(raw))
	for _, value := range raw {
		var outcome pushOutcomeWire
		_ = json.Unmarshal(value, &outcome)
		result = append(result, MutationID(outcome.MutationID))
	}
	return result
}

func pushOpsSortedTableIDs(tables []pushOpsTableSpec) []TableID {
	result := make([]TableID, 0, len(tables))
	for _, table := range tables {
		result = append(result, table.ID)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}

func pushOpsRowChecksumMatches(t *testing.T, schema SchemaManifest, table TableID, row map[string]json.RawMessage, pk json.RawMessage, version string, checksum Checksum) bool {
	t.Helper()
	manifest, err := vectors.ParseManifest(schema.Body)
	if err != nil {
		t.Fatalf("parse checksum manifest: %v", err)
	}
	fields := make([]vectors.RowField, 0, len(row))
	for field, value := range row {
		fields = append(fields, vectors.RowField{FieldID: field, Value: value})
	}
	digest, err := vectors.RowDigest(manifest, string(table), vectors.Row{PK: pk, Fields: fields}, version)
	if err != nil {
		return false
	}
	return Checksum(digest) == checksum
}

func pushOpsMutationOutcome(t *testing.T, raw json.RawMessage) pushOutcomeWire {
	t.Helper()
	var outcome pushOutcomeWire
	if err := json.Unmarshal(raw, &outcome); err != nil {
		t.Fatalf("decode mutation outcome: %v", err)
	}
	return outcome
}

func pushOpsRequireDistinctSnapshotChange(t *testing.T, before, after StateSnapshot, message string) {
	t.Helper()
	if reflect.DeepEqual(before, after) {
		t.Fatal(message)
	}
}

func pushOpsRequestMutationRaw(t *testing.T, request []byte, index int) []byte {
	t.Helper()
	var document struct {
		Mutations []json.RawMessage `json:"mutations"`
	}
	if err := json.Unmarshal(request, &document); err != nil || index < 0 || index >= len(document.Mutations) {
		t.Fatalf("decode request mutation %d: %v", index, err)
	}
	return document.Mutations[index]
}

func pushOpsWireOutcomeChecksum(t *testing.T, outcome pushOutcomeWire) Checksum {
	t.Helper()
	if outcome.RowChecksum == nil {
		t.Fatal("outcome has no row checksum")
	}
	decoded, err := hex.DecodeString(outcome.RowChecksum.Digest)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("decode row checksum: %v", err)
	}
	var checksum Checksum
	copy(checksum[:], decoded)
	return checksum
}

func pushOpsErrorBody(code HTTPCode, retryable bool) []byte {
	encoded, err := json.Marshal(pushErrorEnvelope{Error: pushErrorWire{Code: string(code), Message: pushErrorMessage(code), Retryable: retryable}})
	if err != nil {
		panic(err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		panic(err)
	}
	return canonical
}

func pushOpsMutationWireCanonical(t *testing.T, request []byte, index int) []byte {
	t.Helper()
	canonical, err := jcs.Transform(pushOpsRequestMutationRaw(t, request, index))
	if err != nil {
		t.Fatalf("canonicalize mutation request: %v", err)
	}
	return canonical
}

func pushOpsStringSliceContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func pushOpsResponseOutcomeRaw(t *testing.T, response pushResponse, mutation MutationID) json.RawMessage {
	t.Helper()
	for _, outcomes := range [][]json.RawMessage{response.Accepted, response.Rejected} {
		for _, raw := range outcomes {
			if pushOpsMutationOutcome(t, raw).MutationID == string(mutation) {
				return raw
			}
		}
	}
	t.Fatalf("response outcome for %q is absent", mutation)
	return nil
}

func pushOpsRequireCanonicalErrorBody(t *testing.T, result StepResult, code HTTPCode, retryable bool) {
	t.Helper()
	if !bytes.Equal(result.HTTP.Body, pushOpsErrorBody(code, retryable)) {
		t.Fatalf("error body = %s, want %s", result.HTTP.Body, pushOpsErrorBody(code, retryable))
	}
}

func pushOpsBatchSnapshot(t *testing.T, snapshot StateSnapshot, key BatchKey) BatchLedger {
	t.Helper()
	for _, entry := range snapshot.Batches {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("batch %#v is absent", key)
	return BatchLedger{}
}

func pushOpsMutationSnapshot(t *testing.T, snapshot StateSnapshot, key MutationKey) MutationLedger {
	t.Helper()
	for _, entry := range snapshot.Mutations {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("mutation %#v is absent", key)
	return MutationLedger{}
}

func pushOpsSourceRow(t *testing.T, snapshot StateSnapshot, table TableID, pk string) AuthoritativeRow {
	t.Helper()
	for _, entry := range snapshot.Stream.SourceRows {
		if entry.Identity.TableID == table && entry.Identity.CanonicalWireJSON == pushOpsJSONString(t, pk) {
			return entry.Row
		}
	}
	t.Fatalf("source row %q/%q is absent", table, pk)
	return AuthoritativeRow{}
}

func pushOpsOutcomeCodes(t *testing.T, raw []json.RawMessage) []ReasonCode {
	t.Helper()
	result := make([]ReasonCode, 0, len(raw))
	for _, value := range raw {
		outcome := pushOpsMutationOutcome(t, value)
		if outcome.Code == nil {
			result = append(result, "")
			continue
		}
		result = append(result, ReasonCode(*outcome.Code))
	}
	return result
}

func pushOpsWithReplacementField(table pushOpsTableSpec, replacement pushOpsFieldSpec) pushOpsTableSpec {
	result := pushOpsCloneTable(table)
	for index := range result.Fields {
		if result.Fields[index].ID == replacement.ID {
			result.Fields[index] = replacement
			return result
		}
	}
	return result
}

func pushOpsWithoutField(table pushOpsTableSpec, fieldID FieldID) pushOpsTableSpec {
	result := pushOpsCloneTable(table)
	fields := make([]pushOpsFieldSpec, 0, len(result.Fields))
	for _, field := range result.Fields {
		if field.ID != fieldID {
			fields = append(fields, field)
		}
	}
	result.Fields = fields
	return result
}

func pushOpsTableFieldIDs(table pushOpsTableSpec) []FieldID {
	result := make([]FieldID, 0, len(table.Fields))
	for _, field := range table.Fields {
		result = append(result, field.ID)
	}
	return result
}

func pushOpsMapKeys(values map[string]json.RawMessage) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func pushOpsNonzeroDigest(digest [32]byte) bool {
	return digest != [32]byte{}
}

func pushOpsSameWithoutTimes(left, right BatchLedger) bool {
	left.ServerTime = nil
	left.CreatedAt = nil
	left.CompletedAt = nil
	left.SealedAt = nil
	right.ServerTime = nil
	right.CreatedAt = nil
	right.CompletedAt = nil
	right.SealedAt = nil
	return reflect.DeepEqual(left, right)
}

func pushOpsCanonicalResponseWithChecksum(t *testing.T, response pushResponse, mutation MutationID, digest string) ([]byte, []byte) {
	t.Helper()
	for _, group := range [][]json.RawMessage{response.Accepted, response.Rejected} {
		for index, raw := range group {
			outcome := pushOpsMutationOutcome(t, raw)
			if outcome.MutationID != string(mutation) {
				continue
			}
			if outcome.RowChecksum == nil {
				t.Fatal("cannot corrupt an outcome without a checksum")
			}
			var object map[string]any
			if err := json.Unmarshal(raw, &object); err != nil {
				t.Fatalf("decode outcome for corruption: %v", err)
			}
			checksum := object["row_checksum"].(map[string]any)
			checksum["digest"] = digest
			corruptedOutcome := pushOpsCanonicalJSON(t, object)
			if &group == nil || index < 0 {
				t.Fatal("unreachable outcome group")
			}
			for acceptedIndex, accepted := range response.Accepted {
				if bytes.Equal(accepted, raw) {
					response.Accepted[acceptedIndex] = corruptedOutcome
				}
			}
			for rejectedIndex, rejected := range response.Rejected {
				if bytes.Equal(rejected, raw) {
					response.Rejected[rejectedIndex] = corruptedOutcome
				}
			}
			return pushOpsCanonicalJSON(t, response), corruptedOutcome
		}
	}
	t.Fatalf("outcome %q is absent for checksum corruption", mutation)
	return nil, nil
}

func pushOpsJoinReasons(outcomes []MutationOutcome) string {
	parts := make([]string, 0, len(outcomes))
	for _, outcome := range outcomes {
		parts = append(parts, string(outcome.Reason))
	}
	return strings.Join(parts, ",")
}
