package reference

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

type modelClock struct {
	nowCalls int
}

type observedDoneContext struct {
	context.Context
	doneObserved chan struct{}
	once         sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneObserved) })
	return c.Context.Done()
}

func (c *modelClock) Now() time.Time {
	c.nowCalls++
	return time.Time{}
}

func TestNewConstructsProtocolThreeModelWithoutReadingClock(t *testing.T) {
	clock := &modelClock{}
	initial := tokenFreeSampleState(false)
	model, err := New(Config{State: initial, Clock: clock, Seed: 17})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if clock.nowCalls != 0 {
		t.Fatal("New called Clock.Now")
	}
	if got, want := model.Snapshot(), snapshotState(initial); !reflect.DeepEqual(got, want) {
		t.Fatal("constructed model snapshot differs from initial state")
	}
}

func TestNewRejectsUnsupportedProtocolVersions(t *testing.T) {
	for _, version := range []int{0, 2} {
		t.Run("protocol_"+strconv.Itoa(version), func(t *testing.T) {
			_, err := New(Config{State: State{ProtocolVersion: version}, Clock: &modelClock{}})
			if !errors.Is(err, ErrInvalidInitialState) {
				t.Fatalf("New error = %v, want ErrInvalidInitialState", err)
			}
		})
	}
}

func TestNewRejectsNilAndTypedNilClocks(t *testing.T) {
	var typedNil *modelClock
	for name, clock := range map[string]Clock{
		"nil":       nil,
		"typed_nil": typedNil,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(Config{State: State{ProtocolVersion: 3}, Clock: clock})
			if err == nil {
				t.Fatal("New accepted a nil clock")
			}
		})
	}
}

func TestNewRejectsConfiguredTokenFamilies(t *testing.T) {
	firstClient := ClientKey{UserID: userA, ClientID: clientAID}
	firstRebuild := RebuildKey{Client: firstClient, Scope: scopeA, Rebuild: rebuildA}
	tests := map[string]func(*State){
		"server client checkpoint": func(state *State) {
			client := state.Clients[firstClient]
			client.Checkpoints[0].HasCursor = true
			state.Clients[firstClient] = client
		},
		"rebuild continuation": func(state *State) {
			rebuild := state.Rebuilds[firstRebuild]
			rebuild.HasContinuation = true
			state.Rebuilds[firstRebuild] = rebuild
		},
		"rebuild page": func(state *State) {
			rebuild := state.Rebuilds[firstRebuild]
			rebuild.Pages[0].HasToken = true
			state.Rebuilds[firstRebuild] = rebuild
		},
		"local scope checkpoint": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.ScopeCheckpoints[0].HasCursor = true
			state.ClientLocal[firstClient] = local
		},
		"local seed receipt": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.SeedReceipts[0].HasReceipt = true
			state.ClientLocal[firstClient] = local
		},
		"local rebuild continuation": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.RebuildAttempts[0].HasContinuation = true
			state.ClientLocal[firstClient] = local
		},
		"local applied page request": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.RebuildAttempts[0].AppliedPages[0].HasRequestToken = true
			state.ClientLocal[firstClient] = local
		},
		"local pending final cursor": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.RebuildAttempts[0].PendingFinalResult.HasFinalCursor = true
			state.ClientLocal[firstClient] = local
		},
		"export scope receipt": func(state *State) {
			state.Seed.Exports[0].Scopes[0].HasReceipt = true
		},
		"export page": func(state *State) {
			state.Seed.Exports[0].Pages[0].HasToken = true
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := tokenFreeSampleState(false)
			mutate(&state)
			assertInvalidInitialState(t, state)
		})
	}
}

func TestNewRejectsHiddenTokensWithoutPresence(t *testing.T) {
	firstClient := ClientKey{UserID: userA, ClientID: clientAID}
	firstRebuild := RebuildKey{Client: firstClient, Scope: scopeA, Rebuild: rebuildA}
	hiddenToken := OpaqueToken{namespace: 99, sequence: 1}
	tests := map[string]func(*State){
		"server checkpoint": func(state *State) {
			client := state.Clients[firstClient]
			client.Checkpoints[0].Cursor = hiddenToken
			state.Clients[firstClient] = client
		},
		"server rebuild continuation": func(state *State) {
			rebuild := state.Rebuilds[firstRebuild]
			rebuild.Continuation = hiddenToken
			state.Rebuilds[firstRebuild] = rebuild
		},
		"server rebuild page": func(state *State) {
			rebuild := state.Rebuilds[firstRebuild]
			rebuild.Pages[0].Token = hiddenToken
			state.Rebuilds[firstRebuild] = rebuild
		},
		"local checkpoint": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.ScopeCheckpoints[0].Cursor = hiddenToken
			state.ClientLocal[firstClient] = local
		},
		"local seed receipt": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.SeedReceipts[0].Receipt = hiddenToken
			state.ClientLocal[firstClient] = local
		},
		"local rebuild continuation": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.RebuildAttempts[0].Continuation = hiddenToken
			state.ClientLocal[firstClient] = local
		},
		"local applied page request": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.RebuildAttempts[0].AppliedPages[0].RequestToken = hiddenToken
			state.ClientLocal[firstClient] = local
		},
		"local pending final cursor": func(state *State) {
			local := state.ClientLocal[firstClient]
			local.RebuildAttempts[0].PendingFinalResult.FinalCursor = hiddenToken
			state.ClientLocal[firstClient] = local
		},
		"export scope receipt": func(state *State) {
			state.Seed.Exports[0].Scopes[0].Receipt = hiddenToken
		},
		"export page": func(state *State) {
			state.Seed.Exports[0].Pages[0].Token = hiddenToken
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := tokenFreeSampleState(false)
			mutate(&state)
			assertInvalidInitialState(t, state)
		})
	}
}

func TestNewRejectsInvalidAuthoritativeRows(t *testing.T) {
	firstIdentity := canonicalStringRowIdentity(tableA, fieldA, "alpha")
	tests := map[string]func(*State){
		"empty canonical identity": func(state *State) {
			row := state.Rows[firstIdentity]
			delete(state.Rows, firstIdentity)
			emptyIdentity := firstIdentity
			emptyIdentity.CanonicalIdentityBytes = ""
			row.Identity = emptyIdentity
			state.Rows[emptyIdentity] = row
		},
		"map key and value mismatch": func(state *State) {
			row := state.Rows[firstIdentity]
			row.Identity.TableID = tableB
			state.Rows[firstIdentity] = row
		},
		"duplicate canonical identity": func(state *State) {
			row := state.Rows[firstIdentity]
			duplicateIdentity := firstIdentity
			duplicateIdentity.TableID = tableB
			row.Identity = duplicateIdentity
			state.Rows[duplicateIdentity] = row
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := tokenFreeSampleState(false)
			mutate(&state)
			assertInvalidInitialState(t, state)
		})
	}
}

func TestNewRejectsInvalidRelationRegistrations(t *testing.T) {
	tests := map[string]func(*State){
		"relation map key mismatch": func(state *State) {
			relation := state.Relations[relationA]
			delete(state.Relations, relationA)
			state.Relations["other-relation"] = relation
		},
		"invalid synced table binding": func(state *State) {
			relation := state.Relations[relationA]
			relation.Definition.HasTableID = false
			state.Relations[relationA] = relation
		},
		"invalid capture table binding": func(state *State) {
			definition := &state.Registry.Generations[0].Relations[0].Definition
			definition.HasTableID = true
			definition.TableID = tableB
		},
		"unknown registration kind": func(state *State) {
			relation := state.Relations[relationB]
			relation.Definition.RegistrationKind = "unknown"
			state.Relations[relationB] = relation
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := tokenFreeSampleState(false)
			mutate(&state)
			assertInvalidInitialState(t, state)
		})
	}
}

func TestNewClonesConfigState(t *testing.T) {
	initial := tokenFreeSampleState(false)
	baseline := snapshotState(initial)
	config := Config{State: initial, Clock: &modelClock{}, Seed: 23}
	model, err := New(config)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	mutateEveryStateFamily(&config.State)
	if got := model.Snapshot(); !reflect.DeepEqual(got, baseline) {
		t.Fatal("Config state mutation changed model state")
	}
}

func TestSnapshotDoesNotAliasModelStateOrLaterSnapshots(t *testing.T) {
	initial := tokenFreeSampleState(false)
	baseline := snapshotState(initial)
	model, err := New(Config{State: initial, Clock: &modelClock{}})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	first := model.Snapshot()
	mutateSnapshotFamilies(&first)
	second := model.Snapshot()
	if !reflect.DeepEqual(second, baseline) {
		t.Fatal("snapshot mutation changed model state or a later snapshot")
	}
}

func TestSnapshotIsDeterministicForEquivalentModelStates(t *testing.T) {
	forward, err := New(Config{State: tokenFreeSampleState(false), Clock: &modelClock{}})
	if err != nil {
		t.Fatalf("New forward model: %v", err)
	}
	reverse, err := New(Config{State: tokenFreeSampleState(true), Clock: &modelClock{}})
	if err != nil {
		t.Fatalf("New reverse model: %v", err)
	}

	if !reflect.DeepEqual(forward.Snapshot(), reverse.Snapshot()) {
		t.Fatal("equivalent model snapshots differ by map insertion order")
	}
}

func TestApplyRejectsUnknownOperationWithoutMutation(t *testing.T) {
	model := newTestModel(t, 29)
	before := model.Snapshot()
	op := scenarios.Operation{ContractOperation: "unknown/class", Name: "unknown/name"}

	_, err := model.Apply(context.Background(), op)
	if !errors.Is(err, ErrUnregisteredOperation) {
		t.Fatalf("Apply error = %v, want ErrUnregisteredOperation", err)
	}
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("unknown operation changed model state")
	}
}

func TestApplyPreservesContextErrorsWithoutMutation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()

	for name, test := range map[string]struct {
		ctx  context.Context
		want error
	}{
		"canceled": {ctx: canceled, want: context.Canceled},
		"expired":  {ctx: expired, want: context.DeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			model := newTestModel(t, 31)
			before := model.Snapshot()
			_, err := model.Apply(test.ctx, scenarios.Operation{Name: "unknown"})
			if !errors.Is(err, test.want) {
				t.Fatalf("Apply error = %v, want %v", err, test.want)
			}
			if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatal("context error changed model state")
			}
		})
	}
}

func TestApplyWaitingForGateReturnsAfterCancellation(t *testing.T) {
	model := newTestModel(t, 35)
	<-model.gate
	defer model.releaseGate()

	base, cancel := context.WithCancel(context.Background())
	doneObserved := make(chan struct{})
	ctx := &observedDoneContext{Context: base, doneObserved: doneObserved}
	result := make(chan error, 1)
	go func() {
		_, err := model.Apply(ctx, scenarios.Operation{Name: "unknown"})
		result <- err
	}()
	<-doneObserved
	cancel()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Apply error = %v, want context.Canceled", err)
		}
	case <-timer.C:
		t.Fatal("Apply waited for gate release after context cancellation")
	}
}

func TestApplyRejectsNilContextWithoutMutation(t *testing.T) {
	model := newTestModel(t, 37)
	before := model.Snapshot()

	_, err := model.Apply(nil, scenarios.Operation{Name: "unknown"})
	if err == nil {
		t.Fatal("Apply accepted a nil context")
	}
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("nil context changed model state")
	}
}

func TestModelSeedCreatesReproducibleOpaqueTokenLabels(t *testing.T) {
	first := newTestModel(t, 41)
	second := newTestModel(t, 41)
	bindings := BindingSet{HasUser: true, User: userA, HasOrdinal: true, Ordinal: 2}

	firstToken := first.authority.Mint(string(TokenKindIncrementalCursor), bindings)
	secondToken := second.authority.Mint(string(TokenKindIncrementalCursor), bindings)
	if firstToken != secondToken {
		t.Fatal("equal model seeds did not create equal opaque token labels")
	}
}

func newTestModel(t *testing.T, seed int64) *Model {
	t.Helper()
	model, err := New(Config{State: tokenFreeSampleState(false), Clock: &modelClock{}, Seed: seed})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return model
}

func tokenFreeSampleState(reverse bool) State {
	state := sampleState(reverse)
	for key, client := range state.Clients {
		for index := range client.Checkpoints {
			client.Checkpoints[index].HasCursor = false
			client.Checkpoints[index].Cursor = OpaqueToken{}
		}
		state.Clients[key] = client
	}
	for key, rebuild := range state.Rebuilds {
		rebuild.HasContinuation = false
		rebuild.Continuation = OpaqueToken{}
		for index := range rebuild.Pages {
			rebuild.Pages[index].HasToken = false
			rebuild.Pages[index].Token = OpaqueToken{}
		}
		state.Rebuilds[key] = rebuild
	}
	for key, local := range state.ClientLocal {
		for index := range local.ScopeCheckpoints {
			local.ScopeCheckpoints[index].HasCursor = false
			local.ScopeCheckpoints[index].Cursor = OpaqueToken{}
		}
		for index := range local.SeedReceipts {
			local.SeedReceipts[index].HasReceipt = false
			local.SeedReceipts[index].Receipt = OpaqueToken{}
		}
		for attemptIndex := range local.RebuildAttempts {
			attempt := &local.RebuildAttempts[attemptIndex]
			attempt.HasContinuation = false
			attempt.Continuation = OpaqueToken{}
			for pageIndex := range attempt.AppliedPages {
				attempt.AppliedPages[pageIndex].HasRequestToken = false
				attempt.AppliedPages[pageIndex].RequestToken = OpaqueToken{}
			}
			attempt.PendingFinalResult.HasFinalCursor = false
			attempt.PendingFinalResult.FinalCursor = OpaqueToken{}
		}
		state.ClientLocal[key] = local
	}
	for exportIndex := range state.Seed.Exports {
		export := &state.Seed.Exports[exportIndex]
		for scopeIndex := range export.Scopes {
			export.Scopes[scopeIndex].HasReceipt = false
			export.Scopes[scopeIndex].Receipt = OpaqueToken{}
		}
		for pageIndex := range export.Pages {
			export.Pages[pageIndex].HasToken = false
			export.Pages[pageIndex].Token = OpaqueToken{}
		}
	}
	return state
}

func assertInvalidInitialState(t *testing.T, state State) {
	t.Helper()
	_, err := New(Config{State: state, Clock: &modelClock{}})
	if !errors.Is(err, ErrInvalidInitialState) {
		t.Fatalf("New error = %v, want ErrInvalidInitialState", err)
	}
}
