package reference

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestOperationRegistryIsClosedAndReturnsDefensiveCopies(t *testing.T) {
	expected := make(map[string]struct{})
	for _, key := range scenarios.OperationKeys() {
		class, found := scenarios.LookupOperationClass(key)
		if !found {
			t.Fatalf("scenario operation %q has no class", key)
		}
		if class == scenarios.OperationClassReference {
			expected[key] = struct{}{}
		}
	}
	assertOperationRegistry := func(t *testing.T, registry map[string]OperationHandler) {
		t.Helper()
		if len(registry) != len(expected) {
			t.Fatalf("operation registry has %d keys, want %d", len(registry), len(expected))
		}
		for key := range expected {
			if registry[key] == nil {
				t.Fatalf("operation registry has no handler for %q", key)
			}
		}
	}

	registry := OperationRegistry()
	assertOperationRegistry(t, registry)
	delete(registry, "model/install-current-contract")
	registry["unknown/operation"] = func(context.Context, *Model, json.RawMessage, ResolvedOperationInput) (StepResult, error) {
		return StepResult{}, nil
	}

	second := OperationRegistry()
	assertOperationRegistry(t, second)
	if second["unknown/operation"] != nil {
		t.Fatal("returned map mutation added an operation to the registry")
	}
}

func TestInstallCurrentContractRejectsIncompleteInputWithoutMutation(t *testing.T) {
	operation := scenarios.Operation{
		ContractOperation: "model",
		Name:              "install-current-contract",
		Payload:           json.RawMessage(`{}`),
	}
	configured := newTestModel(t, 60)
	configuredBefore := configured.Snapshot()
	if _, err := configured.Apply(context.Background(), operation); err == nil {
		t.Fatal("install-current-contract accepted configured protocol state")
	}
	if after := configured.Snapshot(); !reflect.DeepEqual(after, configuredBefore) {
		t.Fatal("rejected install changed configured protocol state")
	}

	invalidPayloads := map[string]json.RawMessage{
		"incomplete":      json.RawMessage(`{}`),
		"unknown field":   json.RawMessage(`{"unknown":true}`),
		"duplicate field": json.RawMessage(`{"unknown":1,"unknown":2}`),
		"non-object":      json.RawMessage(`[]`),
		"trailing value":  json.RawMessage(`{} {}`),
	}
	for name, payload := range invalidPayloads {
		t.Run(name, func(t *testing.T) {
			model, err := New(Config{State: State{ProtocolVersion: supportedProtocolVersion}, Clock: &modelClock{}, Seed: 61})
			if err != nil {
				t.Fatalf("create empty model: %v", err)
			}
			before := model.Snapshot()
			operation.Payload = payload
			if _, err := model.Apply(context.Background(), operation); err == nil {
				t.Fatal("Apply accepted an invalid install payload")
			}
			if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatal("invalid install payload changed model state")
			}
		})
	}
}

func TestDecodeStrictPayloadValidatesStructureAndTypedNumbers(t *testing.T) {
	type nestedPayload struct {
		Name string `json:"name"`
	}
	type typedPayload struct {
		Count  int64         `json:"count"`
		Exact  json.Number   `json:"exact"`
		Nested nestedPayload `json:"nested"`
	}

	var decoded typedPayload
	payload := json.RawMessage(`{"count":9223372036854775807,"exact":1e400,"nested":{"name":"ok"}}`)
	if err := decodeStrictPayload(payload, &decoded); err != nil {
		t.Fatalf("decodeStrictPayload returned error: %v", err)
	}
	if decoded.Count != 9223372036854775807 || decoded.Exact.String() != "1e400" || decoded.Nested.Name != "ok" {
		t.Fatal("strict typed decoding changed numeric or nested values")
	}

	invalidPayloads := [][]byte{
		[]byte(`{"count":1,"exact":2,"nested":{"name":"a","name":"b"}}`),
		[]byte(`{"count":1,"exact":2,"nested":{"name":"a","unknown":true}}`),
		[]byte(`{"count":1,"exact":2,"nested":{"name":"a"}} {}`),
		[]byte(`null`),
		{'{', '"', 'c', 'o', 'u', 'n', 't', '"', ':', '"', 0xff, '"', '}'},
	}
	for _, invalid := range invalidPayloads {
		var value typedPayload
		if err := decodeStrictPayload(json.RawMessage(invalid), &value); err == nil {
			t.Fatalf("decodeStrictPayload accepted invalid payload %q", invalid)
		}
	}
}

func TestPrivateHandlerRollsBackStateAndTokenMintOnError(t *testing.T) {
	seed := int64(62)
	model := newTestModel(t, seed)
	before := model.Snapshot()
	bindings := BindingSet{HasUser: true, User: userA}
	handlerError := errors.New("handler failed")
	var minted OpaqueToken

	_, err := model.applyHandler(context.Background(), scenarios.Operation{Name: "private"}, func(_ context.Context, working *Model, _ json.RawMessage) (StepResult, error) {
		working.state.Events = append(working.state.Events, ModelEvent{Ordinal: 999})
		minted = working.authority.Mint(string(TokenKindIncrementalCursor), bindings)
		return StepResult{}, handlerError
	})
	if !errors.Is(err, handlerError) {
		t.Fatalf("applyHandler error = %v, want handler error", err)
	}
	assertAtomicRollback(t, model, before, seed, minted, bindings)
}

func TestPrivateHandlerRollsBackStateAndTokenMintOnCancellation(t *testing.T) {
	seed := int64(63)
	model := newTestModel(t, seed)
	before := model.Snapshot()
	bindings := BindingSet{HasUser: true, User: userA}
	ctx, cancel := context.WithCancel(context.Background())
	var minted OpaqueToken

	_, err := model.applyHandler(ctx, scenarios.Operation{Name: "private"}, func(_ context.Context, working *Model, _ json.RawMessage) (StepResult, error) {
		working.state.Events = append(working.state.Events, ModelEvent{Ordinal: 999})
		minted = working.authority.Mint(string(TokenKindIncrementalCursor), bindings)
		cancel()
		return StepResult{Kind: StepResultKindContractInstalled}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("applyHandler error = %v, want context.Canceled", err)
	}
	assertAtomicRollback(t, model, before, seed, minted, bindings)
}

func TestPrivateHandlerCommitsStateAndTokenSequenceOnce(t *testing.T) {
	seed := int64(64)
	model := newTestModel(t, seed)
	before := model.Snapshot()
	bindings := BindingSet{HasUser: true, User: userA}
	var minted OpaqueToken

	result, err := model.applyHandler(context.Background(), scenarios.Operation{Name: "private"}, func(_ context.Context, working *Model, _ json.RawMessage) (StepResult, error) {
		working.state.Events = append(working.state.Events, ModelEvent{Ordinal: 999})
		minted = working.authority.Mint(string(TokenKindIncrementalCursor), bindings)
		return StepResult{Kind: StepResultKindContractInstalled}, nil
	})
	if err != nil {
		t.Fatalf("applyHandler returned error: %v", err)
	}
	if result.Kind != StepResultKindContractInstalled {
		t.Fatalf("result kind = %q, want %q", result.Kind, StepResultKindContractInstalled)
	}
	after := model.Snapshot()
	if len(after.Events) != len(before.Events)+1 {
		t.Fatal("successful handler did not commit state exactly once")
	}
	if got := model.authority.Validate(minted, string(TokenKindIncrementalCursor), bindings); got != TokenStatusValid {
		t.Fatalf("committed token status = %v, want %v", got, TokenStatusValid)
	}
	fresh := newTokenAuthority(seed)
	if want := fresh.Mint(string(TokenKindIncrementalCursor), bindings); minted != want {
		t.Fatal("successful handler did not commit the first token mint")
	}
	if got, want := model.authority.Mint(string(TokenKindIncrementalCursor), bindings), fresh.Mint(string(TokenKindIncrementalCursor), bindings); got != want {
		t.Fatal("successful handler advanced the token sequence more than once")
	}
}

func TestPrivateHandlerRollsBackInvalidResults(t *testing.T) {
	tests := map[string]StepResult{
		"unknown kind": {
			Kind: "unknown",
		},
		"contract with observation": {
			Kind: StepResultKindContractInstalled,
			Push: &PushObservation{},
		},
		"mismatched observation": {
			Kind: StepResultKindPush,
			Pull: &PullObservation{},
		},
		"multiple observations": {
			Kind: StepResultKindPush,
			Push: &PushObservation{},
			Pull: &PullObservation{},
		},
		"HTTP on non-endpoint": {
			Kind: StepResultKindWAL,
			HTTP: &HTTPObservation{},
			WAL:  &WALObservation{},
		},
	}

	for name, invalidResult := range tests {
		t.Run(name, func(t *testing.T) {
			seed := int64(65)
			model := newTestModel(t, seed)
			before := model.Snapshot()
			bindings := BindingSet{HasUser: true, User: userA}
			var minted OpaqueToken
			_, err := model.applyHandler(context.Background(), scenarios.Operation{Name: "private"}, func(_ context.Context, working *Model, _ json.RawMessage) (StepResult, error) {
				working.state.Events = append(working.state.Events, ModelEvent{Ordinal: 999})
				minted = working.authority.Mint(string(TokenKindIncrementalCursor), bindings)
				return invalidResult, nil
			})
			if err == nil {
				t.Fatal("applyHandler accepted an invalid result")
			}
			assertAtomicRollback(t, model, before, seed, minted, bindings)
		})
	}
}

func assertAtomicRollback(t *testing.T, model *Model, before StateSnapshot, seed int64, minted OpaqueToken, bindings BindingSet) {
	t.Helper()
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("failed handler changed model state")
	}
	if got := model.authority.Validate(minted, string(TokenKindIncrementalCursor), bindings); got != TokenStatusForged {
		t.Fatalf("rolled-back token status = %v, want %v", got, TokenStatusForged)
	}
	fresh := newTokenAuthority(seed)
	if got, want := model.authority.Mint(string(TokenKindIncrementalCursor), bindings), fresh.Mint(string(TokenKindIncrementalCursor), bindings); got != want {
		t.Fatal("failed handler changed the token sequence")
	}
}
