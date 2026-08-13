package reference

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

type mutableReferenceClock struct {
	now time.Time
}

func (c *mutableReferenceClock) Now() time.Time {
	return c.now
}

const (
	task6User   UserID   = "task6-user"
	task6Client ClientID = "task6-client"
	task6ScopeA ScopeID  = "task6-scope-a"
	task6ScopeB ScopeID  = "task6-scope-b"
)

func TestConnectSchemaDispatchAndReset(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.January, 2, 3, 4, 5, 0, time.UTC)}
	first := task6SchemaRef(1, 1)
	second := task6SchemaRef(2, 2)
	clientKey := ClientKey{UserID: task6User, ClientID: task6Client}

	t.Run("fresh", func(t *testing.T) {
		state := task6State(first)
		model := task6Model(t, state, clock)
		task6Apply(t, model, "model", "set-client-assignments", task6AssignmentsPayload(task6ScopeA))
		task6Apply(t, model, "local", "start-sync", task6ClientPayload())

		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(0, first, true, false, 0, "[]"))
		if result.Kind != StepResultKindConnect || result.HTTP == nil || result.HTTP.Status != 200 || result.Connect == nil {
			t.Fatal("fresh connect did not produce an HTTP connect result")
		}
		if got := result.Connect.Schema.Action; got != SchemaActionReplace {
			t.Fatalf("fresh schema action = %q, want %q", got, SchemaActionReplace)
		}
		if result.Connect.Generation != 1 || result.Connect.ScopeSetVersion != 1 {
			t.Fatal("fresh connect did not issue generation one with the authoritative assignment version")
		}
		snapshot := model.Snapshot()
		local := task6LocalSnapshot(t, snapshot, clientKey)
		if local.CurrentSchema != first || local.ClientGeneration != 1 || len(local.DurableQueue) != 0 || len(local.ScopeAssignments) != 1 || !local.ScopeAssignments[0].RebuildRequired {
			t.Fatal("fresh connect did not durably install the schema, generation, and null assignment state")
		}
		server := task6ClientSnapshot(t, snapshot, clientKey)
		if len(server.Checkpoints) != 0 || server.Generations[0].LastCursorAcknowledgedAt != nil {
			t.Fatal("connect acknowledged a cursor during fresh registration")
		}
	})

	t.Run("current", func(t *testing.T) {
		state := task6ExistingState(first, first, task6ScopeA)
		acknowledged := clock.now.Add(-time.Minute)
		server := state.Clients[clientKey]
		server.Checkpoints = []ClientCheckpoint{{Scope: task6ScopeA, Position: task6Position(5)}}
		server.Generations[0].LastCursorAcknowledgedAt = &acknowledged
		state.Clients[clientKey] = server
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA)))
		if result.Connect.Schema.Action != SchemaActionNone {
			t.Fatalf("current schema action = %q, want %q", result.Connect.Schema.Action, SchemaActionNone)
		}
		if len(result.Connect.Schema.AffectedScopes) != 0 {
			t.Fatal("current schema connect exposed affected scopes")
		}
		after := task6ClientSnapshot(t, model.Snapshot(), clientKey)
		if len(after.Checkpoints) != 1 || after.Checkpoints[0].Position != task6Position(5) || after.Generations[0].LastCursorAcknowledgedAt == nil || !after.Generations[0].LastCursorAcknowledgedAt.Equal(acknowledged) {
			t.Fatal("connect changed an acknowledged server checkpoint")
		}
	})

	t.Run("class 2", func(t *testing.T) {
		state := task6ExistingState(second, first, task6ScopeA)
		state.Schemas[first] = SchemaManifest{Body: []byte("initial"), Class: SchemaClassInitial, CompatibilityFloor: first.Version}
		parent := first
		state.Schemas[second] = SchemaManifest{Body: []byte("class-2"), Parent: &parent, Class: SchemaClass2, CompatibilityFloor: first.Version}
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA)))
		if result.Connect.Schema.Action != SchemaActionReplace {
			t.Fatalf("class 2 schema action = %q, want %q", result.Connect.Schema.Action, SchemaActionReplace)
		}
		local := task6LocalSnapshot(t, model.Snapshot(), clientKey)
		if local.CurrentSchema != second || len(local.SchemaJournal) != 1 || local.SchemaJournal[0].Action != SchemaActionReplace || local.SchemaJournal[0].Phase != MigrationPhaseApplied {
			t.Fatal("class 2 connect did not preserve typed applied migration metadata")
		}
	})

	t.Run("class 3 affected", func(t *testing.T) {
		state := task6ExistingState(second, first, task6ScopeA, task6ScopeB)
		state.Schemas[first] = SchemaManifest{Body: []byte("initial"), Class: SchemaClassInitial, CompatibilityFloor: first.Version}
		parent := first
		state.Schemas[second] = SchemaManifest{Body: []byte("class-3"), Parent: &parent, Class: SchemaClass3, CompatibilityFloor: second.Version, AffectedScopes: []ScopeID{task6ScopeA}}
		model := task6Model(t, state, clock)

		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA, task6ScopeB)))
		if result.Connect.Schema.Action != SchemaActionRebuildLocal || !reflect.DeepEqual(result.Connect.Schema.AffectedScopes, []ScopeID{task6ScopeA}) {
			t.Fatalf("class 3 affected dispatch = %#v", result.Connect.Schema)
		}
		local := task6LocalSnapshot(t, model.Snapshot(), clientKey)
		if local.Lifecycle.State != ClientLifecycleRebuilding || !task6LocalAssignment(t, local, task6ScopeA).RebuildRequired {
			t.Fatal("class 3 affected connect did not enter scope-local rebuild state")
		}
	})

	t.Run("class 3 unaffected", func(t *testing.T) {
		state := task6ExistingState(second, first, task6ScopeB)
		state.Schemas[first] = SchemaManifest{Body: []byte("initial"), Class: SchemaClassInitial, CompatibilityFloor: first.Version}
		parent := first
		state.Schemas[second] = SchemaManifest{Body: []byte("class-3"), Parent: &parent, Class: SchemaClass3, CompatibilityFloor: second.Version, AffectedScopes: []ScopeID{task6ScopeA}}
		model := task6Model(t, state, clock)

		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeB)))
		if result.Connect.Schema.Action != SchemaActionReplace || len(result.Connect.Schema.AffectedScopes) != 0 {
			t.Fatalf("class 3 unaffected dispatch = %#v", result.Connect.Schema)
		}
	})

	t.Run("class 4", func(t *testing.T) {
		state := task6ExistingState(second, first, task6ScopeA)
		state.Schemas[first] = SchemaManifest{Body: []byte("initial"), Class: SchemaClassInitial, CompatibilityFloor: first.Version}
		parent := first
		state.Schemas[second] = SchemaManifest{Body: []byte("class-4"), Parent: &parent, Class: SchemaClass4, CompatibilityFloor: second.Version}
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA)))
		if result.HTTP.Status != 200 || result.Connect.Schema.Action != SchemaActionUnsupported || result.Connect.Schema.Reason != ReasonCode("incompatible_schema_transition") {
			t.Fatalf("class 4 dispatch = %#v", result)
		}
		local := task6LocalSnapshot(t, model.Snapshot(), clientKey)
		if local.Lifecycle.State != ClientLifecycleError || local.ErrorState == nil || local.ErrorState.Reason != ReasonCode("incompatible_schema_transition") {
			t.Fatal("class 4 connect did not enter the typed upgrade error state")
		}
	})

	t.Run("explicit reset preserves local intent", func(t *testing.T) {
		state := task6ExistingState(second, first, task6ScopeA)
		state.Schemas[first] = SchemaManifest{Body: []byte("initial"), Class: SchemaClassInitial, CompatibilityFloor: first.Version}
		parent := first
		state.Schemas[second] = SchemaManifest{Body: []byte("class-4"), Parent: &parent, Class: SchemaClass4, CompatibilityFloor: second.Version}
		local := state.ClientLocal[clientKey]
		local.DurableQueue = []QueuedMutation{{Mutation: "queued", Status: LocalMutationStatusPending, Request: []byte("queued-request")}}
		local.SealedBatches = []LocalSealedBatch{{Batch: "sealed", ClientGeneration: 1, State: LocalSealedBatchStateResponseLost, CanonicalRequest: []byte("sealed-request")}}
		local.LocalOnlyRows = []LocalOnlyRow{{Key: LocalOnlyRowKey{Table: "local", Row: "row"}}}
		local.Outcomes = []MutationOutcome{{Mutation: "outcome", State: MutationOutcomeRejectedTerminal}}
		local.Backoff = &DurableBackoff{InterruptedLifecycle: ClientLifecyclePushing, Work: ResumableWorkIdentity{Kind: ResumableWorkPush}, Retry: RetryClassificationTransport, Attempt: 2, NextEligibleAt: task6TimePointer(clock.now.Add(time.Hour))}
		state.ClientLocal[clientKey] = local
		client := state.Clients[clientKey]
		client.Generations[0].CreatedAt = task6TimePointer(clock.now.Add(-time.Hour))
		state.Clients[clientKey] = client
		model := task6Model(t, state, clock)
		before := task6LocalSnapshot(t, model.Snapshot(), clientKey)

		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, true, 1, task6KnownScopesJSON(task6ScopeA)))
		if result.Connect.Schema.Action != SchemaActionRebuildLocal || !reflect.DeepEqual(result.Connect.Schema.AffectedScopes, []ScopeID{task6ScopeA}) {
			t.Fatalf("reset dispatch = %#v", result.Connect.Schema)
		}
		after := task6LocalSnapshot(t, model.Snapshot(), clientKey)
		if after.CurrentSchema != second || !task6LocalAssignment(t, after, task6ScopeA).RebuildRequired {
			t.Fatal("reset did not install the target schema and scope rebuild marker")
		}
		if !reflect.DeepEqual(after.DurableQueue, before.DurableQueue) || !reflect.DeepEqual(after.LocalOnlyRows, before.LocalOnlyRows) || !reflect.DeepEqual(after.Outcomes, before.Outcomes) || !reflect.DeepEqual(after.SealedBatches, before.SealedBatches) || !reflect.DeepEqual(after.Backoff, before.Backoff) {
			t.Fatal("reset cleared durable local intent or local-only state")
		}
	})
}

func TestClientGenerationExpiryRenewalAndRetirement(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.February, 3, 4, 5, 6, 0, time.UTC)}
	first := task6SchemaRef(1, 1)
	clientKey := ClientKey{UserID: task6User, ClientID: task6Client}

	t.Run("expiry uses created time until acknowledgement", func(t *testing.T) {
		state := task6ExistingState(first, first, task6ScopeA)
		client := state.Clients[clientKey]
		created := clock.now.Add(-2 * time.Hour)
		client.Generations[0].CreatedAt = &created
		state.Clients[clientKey] = client
		state.Installation.StaleClientIntervalMilliseconds = uint64(time.Hour / time.Millisecond)
		model := task6Model(t, state, clock)
		task6Apply(t, model, "model", "expire-client-generation", task6ClientPayload())
		if task6ClientSnapshot(t, model.Snapshot(), clientKey).Generations[0].ExpiresAt == nil {
			t.Fatal("generation did not expire from created_at before first acknowledgement")
		}
	})

	t.Run("acknowledgement time prevents early expiry", func(t *testing.T) {
		state := task6ExistingState(first, first, task6ScopeA)
		client := state.Clients[clientKey]
		created := clock.now.Add(-2 * time.Hour)
		acknowledged := clock.now.Add(-30 * time.Minute)
		client.Generations[0].CreatedAt = &created
		client.Generations[0].LastCursorAcknowledgedAt = &acknowledged
		state.Clients[clientKey] = client
		state.Installation.StaleClientIntervalMilliseconds = uint64(time.Hour / time.Millisecond)
		model := task6Model(t, state, clock)
		task6Apply(t, model, "model", "expire-client-generation", task6ClientPayload())
		if task6ClientSnapshot(t, model.Snapshot(), clientKey).Generations[0].ExpiresAt != nil {
			t.Fatal("generation expired from created_at after a newer acknowledgement")
		}
	})

	t.Run("renewal preserves durable queue", func(t *testing.T) {
		state := task6ExistingState(first, first, task6ScopeA)
		client := state.Clients[clientKey]
		created := clock.now.Add(-2 * time.Hour)
		client.Generations[0].CreatedAt = &created
		state.Clients[clientKey] = client
		state.Installation.StaleClientIntervalMilliseconds = uint64(time.Hour / time.Millisecond)
		local := state.ClientLocal[clientKey]
		local.DurableQueue = []QueuedMutation{{Mutation: "queued", Status: LocalMutationStatusPending, Request: []byte("queue")}}
		local.SealedBatches = []LocalSealedBatch{{Batch: "sealed", ClientGeneration: 1, State: LocalSealedBatchStateResponseLost, CanonicalRequest: []byte("batch")}}
		local.LocalOnlyRows = []LocalOnlyRow{{Key: LocalOnlyRowKey{Table: "local", Row: "row"}}}
		local.Outcomes = []MutationOutcome{{Mutation: "rejected", State: MutationOutcomeRejectedTerminal}}
		local.Backoff = &DurableBackoff{InterruptedLifecycle: ClientLifecyclePushing, Work: ResumableWorkIdentity{Kind: ResumableWorkPush}, Retry: RetryClassificationTransport, Attempt: 1, NextEligibleAt: task6TimePointer(clock.now.Add(time.Hour))}
		state.ClientLocal[clientKey] = local
		model := task6Model(t, state, clock)
		before := task6LocalSnapshot(t, model.Snapshot(), clientKey)
		task6Apply(t, model, "model", "expire-client-generation", task6ClientPayload())
		result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA)))
		if result.Connect.Generation != 2 {
			t.Fatalf("renewed generation = %d, want 2", result.Connect.Generation)
		}
		after := task6LocalSnapshot(t, model.Snapshot(), clientKey)
		if after.ClientGeneration != 2 || !task6LocalAssignment(t, after, task6ScopeA).RebuildRequired {
			t.Fatal("generation renewal did not install scoped rebuild state")
		}
		if !reflect.DeepEqual(after.DurableQueue, before.DurableQueue) || !reflect.DeepEqual(after.LocalOnlyRows, before.LocalOnlyRows) || !reflect.DeepEqual(after.Outcomes, before.Outcomes) || !reflect.DeepEqual(after.Backoff, before.Backoff) || len(after.SealedBatches) != len(before.SealedBatches) {
			t.Fatal("generation renewal did not preserve durable client records")
		}
		if after.SealedBatches[0].State != LocalSealedBatchStateAbandonedGeneration {
			t.Fatal("renewal did not retain the old sealed batch as abandoned generation history")
		}
	})

	t.Run("retirement rejects every later connect", func(t *testing.T) {
		state := task6ExistingState(first, first, task6ScopeA)
		model := task6Model(t, state, clock)
		task6Apply(t, model, "model", "retire-client", task6ClientPayload())
		for attempt := 0; attempt < 2; attempt++ {
			result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA)))
			if result.HTTP.Status != 409 || !result.HTTP.HasCode || result.HTTP.Code != HTTPCode("client_retired") {
				t.Fatalf("retired connect %d = %#v", attempt, result.HTTP)
			}
		}
	})
}

func TestConnectGateOrder(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.February, 4, 5, 6, 7, 0, time.UTC)}
	first := task6SchemaRef(1, 1)

	t.Run("protocol precedes retirement and schema dispatch", func(t *testing.T) {
		model := task6Model(t, task6ExistingState(first, first, task6ScopeA), clock)
		task6Apply(t, model, "model", "retire-client", task6ClientPayload())
		payload := `{"user_id":"task6-user","client_id":"task6-client","runtime_version":3,"protocol_version":2,"schema_reset":false,"schema":{"version":0,"hash":""},"scope_set_version":0,"known_scopes":[]}`
		result := task6Apply(t, model, "connect", "send", payload)
		if result.HTTP.Status != 426 || result.HTTP.Code != HTTPCode("upgrade_required") {
			t.Fatalf("protocol gate result = %#v", result.HTTP)
		}
	})

	t.Run("minimum runtime precedes retirement", func(t *testing.T) {
		model := task6Model(t, task6ExistingState(first, first, task6ScopeA), clock)
		task6Apply(t, model, "model", "retire-client", task6ClientPayload())
		payload := `{"user_id":"task6-user","client_id":"task6-client","runtime_version":2,"protocol_version":3,"schema_reset":false,"schema":{"version":0,"hash":""},"scope_set_version":0,"known_scopes":[]}`
		result := task6Apply(t, model, "connect", "send", payload)
		if result.HTTP.Status != 426 || result.HTTP.Code != HTTPCode("upgrade_required") {
			t.Fatalf("minimum-runtime gate result = %#v", result.HTTP)
		}
	})

	t.Run("retirement precedes fresh sentinel validation", func(t *testing.T) {
		model := task6Model(t, task6ExistingState(first, first, task6ScopeA), clock)
		task6Apply(t, model, "model", "retire-client", task6ClientPayload())
		payload := `{"user_id":"task6-user","client_id":"task6-client","runtime_version":3,"protocol_version":3,"schema_reset":false,"schema":{"version":0,"hash":""},"scope_set_version":0,"known_scopes":[]}`
		result := task6Apply(t, model, "connect", "send", payload)
		if result.HTTP.Status != 409 || result.HTTP.Code != HTTPCode("client_retired") {
			t.Fatalf("retirement gate result = %#v", result.HTTP)
		}
	})

	t.Run("known identity cannot use fresh sentinel", func(t *testing.T) {
		model := task6Model(t, task6ExistingState(first, first, task6ScopeA), clock)
		payload := `{"user_id":"task6-user","client_id":"task6-client","runtime_version":3,"protocol_version":3,"schema_reset":false,"schema":{"version":0,"hash":""},"scope_set_version":0,"known_scopes":[]}`
		result := task6Apply(t, model, "connect", "send", payload)
		if result.HTTP.Status != 400 || result.HTTP.Code != HTTPCode("invalid_schema_reference") {
			t.Fatalf("fresh sentinel gate result = %#v", result.HTTP)
		}
	})
}

func TestAssignmentDeltasPreserveQueue(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.March, 4, 5, 6, 7, 0, time.UTC)}
	first := task6SchemaRef(1, 1)
	clientKey := ClientKey{UserID: task6User, ClientID: task6Client}
	state := task6ExistingState(first, first, task6ScopeA)
	local := state.ClientLocal[clientKey]
	local.DurableQueue = []QueuedMutation{{Mutation: "queued", Status: LocalMutationStatusPending, Request: []byte("queue")}}
	state.ClientLocal[clientKey] = local
	model := task6Model(t, state, clock)
	before := task6LocalSnapshot(t, model.Snapshot(), clientKey).DurableQueue

	change := task6Apply(t, model, "model", "set-client-assignments", task6AssignmentsPayload(task6ScopeB))
	if change.Client.PriorScopeSetVersion != 1 || change.Client.NewScopeSetVersion != 2 {
		t.Fatalf("assignment scope-set version transition = %#v", change.Client)
	}
	result := task6Apply(t, model, "connect", "send", task6ConnectPayload(1, first, false, false, 1, task6KnownScopesJSON(task6ScopeA)))
	if !reflect.DeepEqual(result.Connect.AddedScopes, []ScopeID{task6ScopeB}) || !reflect.DeepEqual(result.Connect.RemovedScopes, []ScopeID{task6ScopeA}) {
		t.Fatalf("assignment delta = %#v", result.Connect)
	}
	after := task6LocalSnapshot(t, model.Snapshot(), clientKey)
	if _, found := findLocalScopeAssignment(after.ScopeAssignments, task6ScopeA); found {
		t.Fatal("removed scope remained in local assignment state")
	}
	if !task6LocalAssignment(t, after, task6ScopeB).RebuildRequired || !reflect.DeepEqual(after.DurableQueue, before) {
		t.Fatal("assignment delta cleared durable queue or failed to mark the new scope for rebuild")
	}
}

func TestPublishSchemaIsImmutable(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.March, 5, 6, 7, 8, 0, time.UTC)}
	first := task6SchemaRef(1, 1)
	second := task6SchemaRef(2, 2)
	model := task6Model(t, task6State(first), clock)
	payload := fmt.Sprintf(`{"schema":{"version":%d,"hash":"%s"},"body":"class-two-body","transition_class":"class_2","compatibility_floor":1,"tables":[],"affected_scopes":[]}`,
		second.Version,
		hex.EncodeToString(second.Hash[:]),
	)
	result := task6Apply(t, model, "model", "publish-schema", payload)
	if result.Schema.Action != SchemaActionReplace || result.Schema.Source != first || result.Schema.Target != second {
		t.Fatalf("published schema observation = %#v", result.Schema)
	}
	before := model.Snapshot()
	if _, err := model.Apply(context.Background(), scenarios.Operation{ContractOperation: "model", Name: "publish-schema", Payload: []byte(payload)}); err == nil {
		t.Fatal("schema publication overwrote an immutable record")
	}
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected immutable schema publication changed state")
	}
}

func TestLifecycleOperationsAreDurableAndClosed(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.April, 5, 6, 7, 8, 0, time.UTC)}
	first := task6SchemaRef(1, 1)
	clientKey := ClientKey{UserID: task6User, ClientID: task6Client}

	state := task6State(first)
	model := task6Model(t, state, clock)
	task6Apply(t, model, "local", "start-sync", task6ClientPayload())
	task6Apply(t, model, "local", "stop-sync", task6ClientPayload())
	if state := task6LocalSnapshot(t, model.Snapshot(), clientKey).Lifecycle.State; state != ClientLifecycleStopped {
		t.Fatalf("stop lifecycle = %q, want stopped", state)
	}
	task6Apply(t, model, "local", "start-sync", task6ClientPayload())
	if state := task6LocalSnapshot(t, model.Snapshot(), clientKey).Lifecycle.State; state != ClientLifecycleLocalReady {
		t.Fatalf("restart after stop lifecycle = %q, want local_ready", state)
	}

	backoffState := task6ExistingState(first, first, task6ScopeA)
	backoffLocal := backoffState.ClientLocal[clientKey]
	deadline := clock.now.Add(time.Hour)
	backoffLocal.Lifecycle = ClientLifecycleState{State: ClientLifecycleBackoff, ChangedAt: task6TimePointer(clock.now)}
	backoffLocal.Backoff = &DurableBackoff{InterruptedLifecycle: ClientLifecyclePushing, Work: ResumableWorkIdentity{Kind: ResumableWorkPush}, Retry: RetryClassificationTransport, Attempt: 3, NextEligibleAt: &deadline}
	backoffLocal.DurableQueue = []QueuedMutation{{Mutation: "queue", Status: LocalMutationStatusPending}}
	backoffState.ClientLocal[clientKey] = backoffLocal
	backoffModel := task6Model(t, backoffState, clock)
	before := backoffModel.Snapshot()
	task6Apply(t, backoffModel, "process", "restart-client", task6ClientPayload())
	after := backoffModel.Snapshot()
	localAfter := task6LocalSnapshot(t, after, clientKey)
	if localAfter.Lifecycle.State != ClientLifecycleLocalReady || !reflect.DeepEqual(localAfter.Backoff, task6LocalSnapshot(t, before, clientKey).Backoff) || !reflect.DeepEqual(localAfter.DurableQueue, task6LocalSnapshot(t, before, clientKey).DurableQueue) {
		t.Fatal("process restart did not retain backoff and queue in local_ready")
	}

	errorState := task6ExistingState(first, first, task6ScopeA)
	errorLocal := errorState.ClientLocal[clientKey]
	errorLocal.Lifecycle = ClientLifecycleState{State: ClientLifecycleError, ChangedAt: task6TimePointer(clock.now)}
	errorLocal.ErrorState = &ClientErrorState{Reason: "integrity", Retryable: false, At: task6TimePointer(clock.now)}
	errorState.ClientLocal[clientKey] = errorLocal
	errorModel := task6Model(t, errorState, clock)
	beforeRecovery := errorModel.Snapshot()
	if _, err := errorModel.applyHandler(context.Background(), scenarios.Operation{ContractOperation: "local", Name: "recover-error", Payload: []byte(`{"user_id":"task6-user","client_id":"task6-client","action":"retry"}`)}, recoverError); err == nil {
		t.Fatal("retry recovery accepted a non-retryable error")
	}
	if afterRecovery := errorModel.Snapshot(); !reflect.DeepEqual(afterRecovery, beforeRecovery) {
		t.Fatal("illegal recovery changed durable client state")
	}
	task6Apply(t, errorModel, "local", "recover-error", `{"user_id":"task6-user","client_id":"task6-client","action":"remediated"}`)
	if local := task6LocalSnapshot(t, errorModel.Snapshot(), clientKey); local.Lifecycle.State != ClientLifecycleLocalReady || local.ErrorState == nil || !local.ErrorState.Acknowledged {
		t.Fatal("explicit remediation did not acknowledge and recover the error")
	}

	states := []ClientLifecycle{
		ClientLifecycleUninitialized,
		ClientLifecycleLocalReady,
		ClientLifecycleConnecting,
		ClientLifecycleSchemaApplying,
		ClientLifecycleReady,
		ClientLifecyclePushing,
		ClientLifecyclePulling,
		ClientLifecycleRebuilding,
		ClientLifecycleBackoff,
		ClientLifecycleError,
		ClientLifecycleStopped,
	}
	for _, from := range states {
		for _, to := range states {
			want := task6ExpectedLifecycleEdge(from, to)
			if got := lifecycleTransitionAllowed(from, to); got != want {
				t.Fatalf("lifecycle edge %q -> %q = %t, want %t", from, to, got, want)
			}
		}
	}
	if lifecycleTransitionAllowed(ClientLifecycleStopped, ClientLifecyclePulling) || lifecycleTransitionAllowed(ClientLifecycleError, ClientLifecycleReady) || lifecycleTransitionAllowed(ClientLifecycleLocalReady, ClientLifecycleBackoff) {
		t.Fatal("representative illegal lifecycle edge was accepted")
	}
}

func TestScopeLocalCompaction(t *testing.T) {
	clock := &mutableReferenceClock{now: time.Date(2032, time.May, 6, 7, 8, 9, 0, time.UTC)}
	first := task6SchemaRef(1, 1)
	clientA := ClientKey{UserID: "client-a-user", ClientID: "client-a"}
	clientB := ClientKey{UserID: "client-b-user", ClientID: "client-b"}

	t.Run("minimum uses only the compacted scope", func(t *testing.T) {
		state := task6CompactionState(first)
		state.Scopes[task6ScopeA] = task6ScopeState(first, task6ScopeA, 1, 5, 10)
		state.Scopes[task6ScopeB] = task6ScopeState(first, task6ScopeB, 1, 1, 10)
		state.Clients[clientA] = task6RetentionClient(clock.now, task6ScopeA, 5)
		state.Clients[clientB] = task6RetentionClient(clock.now, task6ScopeA, 10)
		globalOnly := task6RetentionClient(clock.now, task6ScopeB, 1)
		state.Clients[ClientKey{UserID: "global-user", ClientID: "global-client"}] = globalOnly
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "model", "compact-scope", `{"scope_id":"task6-scope-a","batch_size":10000}`)
		if result.Retention.NewFloor.Position.CommitLSN != 5 || result.Retention.DeletedCount != 2 {
			t.Fatalf("scope-local compaction result = %#v", result.Retention)
		}
		floor := task6RetentionFloor(t, model.Snapshot(), task6ScopeA)
		if !cursorPositionAtOrAboveFloor(task6Position(5), floor) || cursorPositionAtOrAboveFloor(task6Position(1), floor) {
			t.Fatal("retention floor did not preserve floor-equal cursors and reject older cursors")
		}
	})

	t.Run("high-watermark fallback", func(t *testing.T) {
		state := task6CompactionState(first)
		state.Scopes[task6ScopeA] = task6ScopeState(first, task6ScopeA, 1, 5, 10)
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "model", "compact-scope", `{"scope_id":"task6-scope-a","batch_size":10000}`)
		if result.Retention.NewFloor.Position.CommitLSN != 10 || result.Retention.DeletedCount != 3 {
			t.Fatalf("high-watermark fallback = %#v", result.Retention)
		}
		if len(task6ScopeSnapshot(t, model.Snapshot(), task6ScopeA).Effects) != 0 {
			t.Fatal("high-watermark fallback did not delete all compactable scope effects")
		}
	})

	t.Run("lower bound limits deletion", func(t *testing.T) {
		state := task6CompactionState(first)
		state.Scopes[task6ScopeA] = task6ScopeState(first, task6ScopeA, 1, 5, 10)
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "model", "compact-scope", `{"scope_id":"task6-scope-a","batch_size":1}`)
		if result.Retention.NewFloor.Position.CommitLSN != 1 || result.Retention.DeletedCount != 1 {
			t.Fatalf("bounded compaction result = %#v", result.Retention)
		}
		effects := task6ScopeSnapshot(t, model.Snapshot(), task6ScopeA).Effects
		if len(effects) != 2 || effects[0].Position.CommitLSN != 5 || effects[1].Position.CommitLSN != 10 {
			t.Fatalf("bounded compaction effects = %#v", effects)
		}
	})

	t.Run("invalid upper bound preserves state", func(t *testing.T) {
		state := task6CompactionState(first)
		state.Scopes[task6ScopeA] = task6ScopeState(first, task6ScopeA, 1, 5, 10)
		model := task6Model(t, state, clock)
		before := model.Snapshot()
		op := scenarios.Operation{
			ContractOperation: "model",
			Name:              "compact-scope",
			Payload:           []byte(`{"scope_id":"task6-scope-a","batch_size":10001}`),
		}
		_, err := model.Apply(context.Background(), op)
		if err == nil {
			t.Fatal("compact-scope accepted a batch_size above the configured maximum")
		}
		var coded interface{ ErrorCode() string }
		if !errors.As(err, &coded) || coded.ErrorCode() != "invalid_limit" {
			t.Fatalf("invalid batch_size error code = %v", err)
		}
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("invalid batch_size changed model state")
		}
	})

	t.Run("active rebuild pins history", func(t *testing.T) {
		state := task6CompactionState(first)
		state.Scopes[task6ScopeA] = task6ScopeState(first, task6ScopeA, 1, 5, 10)
		state.Clients[clientA] = task6RetentionClient(clock.now, task6ScopeA, 10)
		expires := clock.now.Add(time.Hour)
		state.Rebuilds[RebuildKey{Client: clientA, Scope: task6ScopeA, Rebuild: "rebuild-pin"}] = RebuildSession{
			ClientGeneration:     1,
			Scope:                task6ScopeA,
			Schema:               first,
			MembershipGeneration: 1,
			RetentionGeneration:  1,
			StreamGeneration:     "task6-stream",
			SnapshotBoundary:     task6Position(5),
			ExpiresAt:            &expires,
			Status:               RebuildStatusStaged,
		}
		model := task6Model(t, state, clock)
		result := task6Apply(t, model, "model", "compact-scope", `{"scope_id":"task6-scope-a","batch_size":10000}`)
		if !result.Retention.Pinned || result.Retention.NewFloor.Position.CommitLSN != 5 || result.Retention.DeletedCount != 2 {
			t.Fatalf("rebuild pin compaction = %#v", result.Retention)
		}
	})
}

func TestOperationPayloadFailuresRollbackBeforeMutation(t *testing.T) {
	operations := []struct {
		contract string
		name     string
	}{
		{contract: "connect", name: "send"},
		{contract: "local", name: "start-sync"},
		{contract: "local", name: "stop-sync"},
		{contract: "local", name: "recover-error"},
		{contract: "process", name: "restart-client"},
		{contract: "model", name: "publish-schema"},
		{contract: "model", name: "set-client-assignments"},
		{contract: "model", name: "expire-client-generation"},
		{contract: "model", name: "compact-scope"},
		{contract: "model", name: "retire-client"},
	}
	clock := &mutableReferenceClock{now: time.Date(2032, time.June, 7, 8, 9, 10, 0, time.UTC)}
	for _, operation := range operations {
		t.Run(operation.contract+"/"+operation.name, func(t *testing.T) {
			model := task6Model(t, task6State(task6SchemaRef(1, 1)), clock)
			before := model.Snapshot()
			for _, payload := range []string{`{"unknown":true}`, `{"unknown":true,"unknown":false}`} {
				op := scenarios.Operation{ContractOperation: operation.contract, Name: operation.name, Payload: []byte(payload)}
				var err error
				if handler := removedTask6OperationHandler(operation.contract + "/" + operation.name); handler != nil {
					_, err = model.applyHandler(context.Background(), op, handler)
				} else {
					_, err = model.Apply(context.Background(), op)
				}
				if err == nil {
					t.Fatalf("%s accepted a strict-invalid payload", operation.name)
				}
				if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatalf("%s strict-invalid payload changed the model", operation.name)
				}
			}
		})
	}
}

func task6Model(t *testing.T, state State, clock Clock) *Model {
	t.Helper()
	model, err := New(Config{State: state, Clock: clock, Seed: 610})
	if err != nil {
		t.Fatalf("create model: %v", err)
	}
	return model
}

func task6State(current SchemaRef) State {
	return State{
		ProtocolVersion: 3,
		CurrentSchema:   current,
		Schemas: map[SchemaRef]SchemaManifest{
			current: {Body: []byte("initial"), Class: SchemaClassInitial, CompatibilityFloor: current.Version},
		},
		Scopes: map[ScopeID]ScopeState{
			task6ScopeA: task6ScopeState(current, task6ScopeA, 1, 5, 10),
			task6ScopeB: task6ScopeState(current, task6ScopeB, 1, 5, 10),
		},
		Clients:     make(map[ClientKey]ClientState),
		ClientLocal: make(map[ClientKey]ClientLocalState),
		Rebuilds:    make(map[RebuildKey]RebuildSession),
		Installation: InstallationCapabilities{
			ProtocolVersion:                 supportedProtocolVersion,
			MinimumClientRuntime:            3,
			StaleClientIntervalMilliseconds: uint64((24 * time.Hour) / time.Millisecond),
		},
	}
}

func task6CompactionState(current SchemaRef) State {
	state := task6State(current)
	state.ConfiguredLimits.CompactionBatchMaximum = 10000
	return state
}

func task6ExistingState(current, localSchema SchemaRef, scopes ...ScopeID) State {
	state := task6State(current)
	key := ClientKey{UserID: task6User, ClientID: task6Client}
	created := time.Date(2032, time.January, 1, 0, 0, 0, 0, time.UTC)
	assignments := make([]ScopeAssignment, 0, len(scopes))
	localAssignments := make([]LocalScopeAssignment, 0, len(scopes))
	for _, scopeID := range scopes {
		scope := state.Scopes[scopeID]
		assignments = append(assignments, ScopeAssignment{Scope: scopeID, MembershipGeneration: scope.MembershipGeneration, RetentionGeneration: scope.RetentionGeneration, Assigned: true})
		localAssignments = append(localAssignments, LocalScopeAssignment{Scope: scopeID, MembershipGeneration: 1, RetentionGeneration: 1, Assigned: true})
	}
	state.Clients[key] = ClientState{
		CurrentGeneration: 1,
		Generations:       []ClientGenerationState{{Generation: 1, CreatedAt: &created}},
		ScopeSetVersion:   1,
		ScopeAssignments:  assignments,
	}
	state.ClientLocal[key] = ClientLocalState{
		ClientGeneration:             1,
		CurrentSchema:                localSchema,
		AuthoritativeScopeSetVersion: 1,
		ScopeAssignments:             localAssignments,
		Lifecycle:                    ClientLifecycleState{State: ClientLifecycleLocalReady, ChangedAt: &created},
	}
	return state
}

func task6ScopeState(schema SchemaRef, scope ScopeID, generation Generation, middle, high uint64) ScopeState {
	effects := []ScopeEffect{
		{Position: task6Position(1), Operation: EffectOperationUpsert},
		{Position: task6Position(middle), Operation: EffectOperationUpsert},
		{Position: task6Position(high), Operation: EffectOperationUpsert},
	}
	return ScopeState{
		Schema:               schema,
		MembershipGeneration: generation,
		RetentionGeneration:  1,
		StreamGeneration:     "task6-stream",
		Effects:              effects,
		HighWatermark:        task6Position(high),
	}
}

func task6RetentionClient(created time.Time, scope ScopeID, position uint64) ClientState {
	return ClientState{
		CurrentGeneration: 1,
		Generations:       []ClientGenerationState{{Generation: 1, CreatedAt: task6TimePointer(created)}},
		ScopeSetVersion:   1,
		ScopeAssignments: []ScopeAssignment{{
			Scope:                scope,
			MembershipGeneration: 1,
			RetentionGeneration:  1,
			Assigned:             true,
		}},
		Checkpoints: []ClientCheckpoint{{Scope: scope, Position: task6Position(position)}},
	}
}

func task6Position(commit uint64) StreamPosition {
	return StreamPosition{StreamGeneration: "task6-stream", Kind: PositionKindEffect, CommitLSN: CommitLSN(commit), EventOrdinal: 1, EffectOrdinal: 1}
}

func task6SchemaRef(version uint64, marker byte) SchemaRef {
	var hash [32]byte
	hash[0] = marker
	return SchemaRef{Version: version, Hash: hash}
}

func task6ConnectPayload(generation uint64, schema SchemaRef, fresh, reset bool, scopeSetVersion uint64, knownScopes string) string {
	hash := hex.EncodeToString(schema.Hash[:])
	if fresh {
		schema = SchemaRef{}
		hash = ""
	}
	generationMember := ""
	if generation != 0 {
		generationMember = fmt.Sprintf(`,"client_generation":%d`, generation)
	}
	return fmt.Sprintf(`{"user_id":"%s","client_id":"%s","runtime_version":3,"protocol_version":3%s,"schema_reset":%t,"schema":{"version":%d,"hash":"%s"},"scope_set_version":%d,"known_scopes":%s}`,
		task6User,
		task6Client,
		generationMember,
		reset,
		schema.Version,
		hash,
		scopeSetVersion,
		knownScopes,
	)
}

func task6AssignmentsPayload(scopes ...ScopeID) string {
	value := `{"user_id":"task6-user","client_id":"task6-client","assignments":[`
	for index, scope := range scopes {
		if index != 0 {
			value += ","
		}
		value += fmt.Sprintf(`{"scope_id":"%s"}`, scope)
	}
	return value + `]}`
}

func task6KnownScopesJSON(scopes ...ScopeID) string {
	value := "["
	for index, scope := range scopes {
		if index != 0 {
			value += ","
		}
		value += fmt.Sprintf(`{"scope_id":"%s"}`, scope)
	}
	return value + "]"
}

func task6ClientPayload() string {
	return `{"user_id":"task6-user","client_id":"task6-client"}`
}

func task6Apply(t *testing.T, model *Model, contractOperation, name, payload string) StepResult {
	t.Helper()
	op := scenarios.Operation{
		ContractOperation: contractOperation,
		Name:              name,
		Payload:           []byte(payload),
	}
	var result StepResult
	var err error
	if handler := removedTask6OperationHandler(contractOperation + "/" + name); handler != nil {
		result, err = model.applyHandler(context.Background(), op, handler)
	} else {
		result, err = model.Apply(context.Background(), op)
	}
	if err != nil {
		t.Fatalf("apply %s/%s: %v", contractOperation, name, err)
	}
	return result
}

func removedTask6OperationHandler(key string) operationImplementation {
	switch key {
	case "local/start-sync":
		return startSync
	case "local/stop-sync":
		return stopSync
	case "local/recover-error":
		return recoverError
	case "model/retire-client":
		return retireClient
	default:
		return nil
	}
}

func task6ClientSnapshot(t *testing.T, snapshot StateSnapshot, key ClientKey) ClientState {
	t.Helper()
	for _, entry := range snapshot.Clients {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("client %v is absent from snapshot", key)
	return ClientState{}
}

func task6LocalSnapshot(t *testing.T, snapshot StateSnapshot, key ClientKey) ClientLocalState {
	t.Helper()
	for _, entry := range snapshot.ClientLocal {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("local client %v is absent from snapshot", key)
	return ClientLocalState{}
}

func task6ScopeSnapshot(t *testing.T, snapshot StateSnapshot, scope ScopeID) ScopeState {
	t.Helper()
	for _, entry := range snapshot.Scopes {
		if entry.Key == scope {
			return entry.Value
		}
	}
	t.Fatalf("scope %q is absent from snapshot", scope)
	return ScopeState{}
}

func task6RetentionFloor(t *testing.T, snapshot StateSnapshot, scope ScopeID) RetentionFloor {
	t.Helper()
	for _, entry := range snapshot.RetentionFloors {
		if entry.Key == scope {
			return entry.Value
		}
	}
	t.Fatalf("retention floor for %q is absent from snapshot", scope)
	return RetentionFloor{}
}

func task6LocalAssignment(t *testing.T, local ClientLocalState, scope ScopeID) LocalScopeAssignment {
	t.Helper()
	if index, found := findLocalScopeAssignment(local.ScopeAssignments, scope); found {
		return local.ScopeAssignments[index]
	}
	t.Fatalf("local assignment %q is absent", scope)
	return LocalScopeAssignment{}
}

func task6TimePointer(value time.Time) *time.Time {
	return &value
}

func task6ExpectedLifecycleEdge(from, to ClientLifecycle) bool {
	switch from {
	case ClientLifecycleUninitialized:
		return to == ClientLifecycleLocalReady || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleLocalReady:
		return to == ClientLifecycleConnecting || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleConnecting:
		return to == ClientLifecycleSchemaApplying || to == ClientLifecycleReady || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleSchemaApplying:
		return to == ClientLifecycleReady || to == ClientLifecycleRebuilding || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleReady:
		return to == ClientLifecycleConnecting || to == ClientLifecyclePushing || to == ClientLifecyclePulling || to == ClientLifecycleRebuilding || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecyclePushing:
		return to == ClientLifecyclePushing || to == ClientLifecycleReady || to == ClientLifecyclePulling || to == ClientLifecycleConnecting || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecyclePulling:
		return to == ClientLifecyclePulling || to == ClientLifecycleReady || to == ClientLifecycleRebuilding || to == ClientLifecycleConnecting || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleRebuilding:
		return to == ClientLifecycleRebuilding || to == ClientLifecycleReady || to == ClientLifecycleConnecting || to == ClientLifecycleBackoff || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleBackoff:
		return to == ClientLifecycleConnecting || to == ClientLifecyclePushing || to == ClientLifecyclePulling || to == ClientLifecycleRebuilding || to == ClientLifecycleError || to == ClientLifecycleStopped
	case ClientLifecycleError:
		return to == ClientLifecycleLocalReady || to == ClientLifecycleStopped
	case ClientLifecycleStopped:
		return to == ClientLifecycleLocalReady
	default:
		return false
	}
}
