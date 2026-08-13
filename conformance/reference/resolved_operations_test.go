package reference

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestApplyResolvedRejectsInputsOutsideTheTwoResolvedOperations(t *testing.T) {
	model := newTestModel(t, 701)
	before := model.Snapshot()
	op := scenarios.Operation{ContractOperation: "connect", Name: "send", Payload: json.RawMessage(`{}`)}
	input := ResolvedOperationInput{SourceStep: &ResolvedStep{StepID: "STEP-REJECT-001"}}
	if _, err := model.ApplyResolved(context.Background(), op, input); err == nil {
		t.Fatal("ApplyResolved accepted resolved input for connect/send")
	}
	if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected resolved input changed model state")
	}
	for _, key := range []string{"local/recover-error", "local/start-sync", "local/stop-sync", "model/retire-client"} {
		parts := splitOperationKeyForTest(t, key)
		if _, err := model.Apply(context.Background(), scenarios.Operation{ContractOperation: parts[0], Name: parts[1], Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrUnregisteredOperation) {
			t.Fatalf("Apply %s error = %v, want ErrUnregisteredOperation", key, err)
		}
	}
}

func TestResolvedOperationInputIsDefensivelyCloned(t *testing.T) {
	model := newTestModel(t, 702)
	row := AuthoritativeRow{FieldValues: []FieldValue{{Field: fieldA, WireJSON: `"original"`}}}
	input := ResolvedOperationInput{
		SourceStep:   &ResolvedStep{Result: StepResult{Kind: StepResultKindPull, HTTP: &HTTPObservation{Body: []byte("original")}, Pull: &PullObservation{Changes: []PullChangeObservation{{Scope: scopeA}}}}},
		PortableSeed: &PortableSeedFixture{ArtifactBytes: []byte("artifact"), ManifestBytes: []byte("manifest"), PortableScopeIDs: []ScopeID{scopeA}, Scopes: []PortableSeedScopeFixture{{Scope: scopeA}}, Rows: []PortableSeedRowFixture{{Row: row}}},
	}
	result, err := model.applyResolvedHandler(context.Background(), scenarios.Operation{ContractOperation: "private", Name: "clone"}, input, func(_ context.Context, _ *Model, _ json.RawMessage, cloned ResolvedOperationInput) (StepResult, error) {
		cloned.SourceStep.Result.HTTP.Body[0] = 'X'
		cloned.SourceStep.Result.Pull.Changes[0].Scope = scopeB
		cloned.PortableSeed.ArtifactBytes[0] = 'X'
		cloned.PortableSeed.ManifestBytes[0] = 'X'
		cloned.PortableSeed.PortableScopeIDs[0] = scopeB
		cloned.PortableSeed.Scopes[0].Scope = scopeB
		cloned.PortableSeed.Rows[0].Row.FieldValues[0].WireJSON = `"changed"`
		return StepResult{Kind: StepResultKindContractInstalled}, nil
	})
	if err != nil || result.Kind != StepResultKindContractInstalled {
		t.Fatalf("applyResolvedHandler returned %#v, %v", result, err)
	}
	if string(input.SourceStep.Result.HTTP.Body) != "original" || input.SourceStep.Result.Pull.Changes[0].Scope != scopeA || string(input.PortableSeed.ArtifactBytes) != "artifact" || string(input.PortableSeed.ManifestBytes) != "manifest" || input.PortableSeed.PortableScopeIDs[0] != scopeA || input.PortableSeed.Scopes[0].Scope != scopeA || input.PortableSeed.Rows[0].Row.FieldValues[0].WireJSON != `"original"` {
		t.Fatal("handler mutation escaped the resolved-input defensive clone")
	}
}

func TestResolvedHandlerRollsBackStateAndTokenAuthority(t *testing.T) {
	seed := int64(703)
	model := newTestModel(t, seed)
	before := model.Snapshot()
	bindings := BindingSet{HasUser: true, User: userA}
	handlerError := errors.New("resolved handler failed")
	var minted OpaqueToken
	_, err := model.applyResolvedHandler(context.Background(), scenarios.Operation{ContractOperation: "private", Name: "rollback"}, ResolvedOperationInput{SourceStep: &ResolvedStep{StepID: "STEP-ROLLBACK-001"}}, func(_ context.Context, working *Model, _ json.RawMessage, input ResolvedOperationInput) (StepResult, error) {
		input.SourceStep.StepID = "changed"
		working.state.Events = append(working.state.Events, ModelEvent{Ordinal: 999})
		minted = working.authority.Mint(string(TokenKindIncrementalCursor), bindings)
		return StepResult{}, handlerError
	})
	if !errors.Is(err, handlerError) {
		t.Fatalf("applyResolvedHandler error = %v, want handler error", err)
	}
	assertAtomicRollback(t, model, before, seed, minted, bindings)
}

func TestLocalApplyPullPageAppliesCurrentProjectionAndInternalCursorAtomically(t *testing.T) {
	model, client, source, projection := preparedPullApply(t)
	result := applyResolvedTestOperation(t, model, "local", "apply-pull-page", localApplyPullPagePayload{UserID: string(client.UserID), ClientID: string(client.ClientID), SourceStepID: source.StepID}, ResolvedOperationInput{SourceStep: source})
	if result.Kind != StepResultKindLocal || result.Local == nil || result.Local.Client != client {
		t.Fatalf("local apply result = %#v", result)
	}
	local := model.state.ClientLocal[client]
	if len(local.Rows) != 1 || local.Rows[0].Identity != projection.Row || !reflect.DeepEqual(local.Rows[0].Fields, projection.Fields) || local.Rows[0].ServerVersion != projection.Version || local.Rows[0].Checksum != projection.Checksum {
		t.Fatalf("applied local row = %#v, want current captured projection", local.Rows)
	}
	if len(local.Provenance) != 1 || local.Provenance[0].Row != projection.Row || !reflect.DeepEqual(local.Provenance[0].Scopes, []ScopeID{scopeA}) || local.Provenance[0].Version != projection.Version {
		t.Fatalf("applied provenance = %#v", local.Provenance)
	}
	checkpoint, found := localCheckpointForScope(local.ScopeCheckpoints, scopeA)
	serverCheckpoint, serverFound := serverCheckpointForScope(model.state.Clients[client].Checkpoints, scopeA)
	if !found || !serverFound || !checkpoint.HasCursor || checkpoint.Cursor != serverCheckpoint.Cursor || !checkpoint.HasChecksum || !checkpoint.Verified || checkpoint.Checksum != model.state.Scopes[scopeA].Checksum {
		t.Fatalf("applied local checkpoint = %#v", checkpoint)
	}
	if result.HTTP != nil || result.Rebuild != nil || result.Pull != nil {
		t.Fatal("local apply exposed server cursor or pull result data")
	}
}

func TestLocalApplyPullPageRejectsMisbindingAndCurrentStateChanges(t *testing.T) {
	tests := map[string]func(*Model, *ResolvedStep){
		"source step ID":     func(_ *Model, source *ResolvedStep) { source.StepID = "STEP-OTHER-001" },
		"source operation":   func(_ *Model, source *ResolvedStep) { source.OperationKey = "push/submit" },
		"source result kind": func(_ *Model, source *ResolvedStep) { source.Result.Kind = StepResultKindPush },
		"projection version": func(model *Model, _ *ResolvedStep) {
			for key, projection := range model.state.Projections {
				projection.Version = "changed-version"
				model.state.Projections[key] = projection
				break
			}
		},
		"terminal checksum": func(model *Model, _ *ResolvedStep) {
			scope := model.state.Scopes[scopeA]
			scope.Checksum[0]++
			model.state.Scopes[scopeA] = scope
		},
		"issued cursor": func(model *Model, _ *ResolvedStep) {
			client := ClientKey{UserID: userA, ClientID: clientAID}
			state := model.state.Clients[client]
			state.Checkpoints[0].HasCursor = false
			state.Checkpoints[0].Cursor = OpaqueToken{}
			model.state.Clients[client] = state
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			model, client, source, _ := preparedPullApply(t)
			mutate(model, source)
			before := model.Snapshot()
			payload, err := json.Marshal(localApplyPullPagePayload{UserID: string(client.UserID), ClientID: string(client.ClientID), SourceStepID: "STEP-PULL-001"})
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}
			_, err = model.ApplyResolved(context.Background(), scenarios.Operation{ContractOperation: "local", Name: "apply-pull-page", Payload: payload}, ResolvedOperationInput{SourceStep: source})
			if err == nil {
				t.Fatal("ApplyResolved accepted a misbound or changed pull source")
			}
			if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatal("rejected pull apply changed local or server state")
			}
		})
	}
}

func TestInstallPortableSeedVerifiesAndInstallsWithoutAuthorityOrCheckpointGrant(t *testing.T) {
	state, clock, client, fixture := portableSeedTestFixture(t)
	authorizationBefore := cloneAuthorizationState(state.Authorization)
	serverBefore := cloneClientState(state.Clients[client])
	model, err := New(Config{State: state, Clock: clock, Seed: 704})
	if err != nil {
		t.Fatalf("create seed model: %v", err)
	}
	result := applyResolvedTestOperation(t, model, "artifact", "install-portable-seed", installPortableSeedPayload{UserID: string(client.UserID), ClientID: string(client.ClientID), PortableSeedArtifactID: portableSeedArtifactDefinitionID, SeedFixtureID: portableSeedFixtureID}, ResolvedOperationInput{PortableSeed: &fixture})
	if result.Kind != StepResultKindLocal || result.Local == nil {
		t.Fatalf("seed install result = %#v", result)
	}
	local := model.state.ClientLocal[client]
	if len(local.Rows) != portableSeedRowCount || len(local.Provenance) != portableSeedRowCount || len(local.SeedReceipts) != 1 {
		t.Fatalf("seed local counts = rows %d, provenance %d, receipts %d", len(local.Rows), len(local.Provenance), len(local.SeedReceipts))
	}
	if len(local.ScopeCheckpoints) != 0 {
		t.Fatal("seed install granted a local checkpoint or runtime cursor")
	}
	if !reflect.DeepEqual(model.state.Authorization, authorizationBefore) || !reflect.DeepEqual(model.state.Clients[client], serverBefore) {
		t.Fatal("seed install changed authorization or server assignments")
	}
	if len(model.state.Seed.Exports) != 0 || len(model.state.Seed.Records) != 0 {
		t.Fatal("seed install created a second server seed-state path")
	}
	receipt := local.SeedReceipts[0]
	bindings, found := tokenBindings(model.authority, receipt.Receipt)
	wantBindings := portableSeedReceiptBindings(fixture, fixture.Scopes[0], referenceNow(clock))
	if !found || bindings != canonicalizeBindings(wantBindings) || bindings.HasUser || bindings.HasClient {
		t.Fatalf("seed receipt bindings = %#v, want exact non-authorizing bindings", bindings)
	}
	if got := model.authority.Validate(receipt.Receipt, string(TokenKindSeedReceipt), wantBindings); got != TokenStatusValid {
		t.Fatalf("seed receipt status = %v, want valid", got)
	}
}

func TestInstallPortableSeedRejectsIntegrityCardinalityAndLineageDefectsAtomically(t *testing.T) {
	baseState, clock, client, baseFixture := portableSeedTestFixture(t)
	tests := map[string]func(*State, *PortableSeedFixture){
		"artifact digest":   func(_ *State, fixture *PortableSeedFixture) { fixture.ArtifactSHA256[0]++ },
		"manifest digest":   func(_ *State, fixture *PortableSeedFixture) { fixture.ManifestBytes[0]++ },
		"schema lineage":    func(_ *State, fixture *PortableSeedFixture) { fixture.Schema.Version++ },
		"registry lineage":  func(_ *State, fixture *PortableSeedFixture) { fixture.RegistryGeneration++ },
		"stream lineage":    func(_ *State, fixture *PortableSeedFixture) { fixture.StreamGeneration = "other-stream" },
		"scope generation":  func(_ *State, fixture *PortableSeedFixture) { fixture.Scopes[0].MembershipGeneration++ },
		"row ordinal":       func(_ *State, fixture *PortableSeedFixture) { fixture.Rows[1].Ordinal = 1 },
		"row checksum":      func(_ *State, fixture *PortableSeedFixture) { fixture.Rows[10].Row.Checksum[0]++ },
		"scope cardinality": func(_ *State, fixture *PortableSeedFixture) { fixture.Scopes[0].Cardinality-- },
		"scope digest":      func(_ *State, fixture *PortableSeedFixture) { fixture.Scopes[0].Checksum[0]++ },
		"nonempty target": func(state *State, _ *PortableSeedFixture) {
			local := state.ClientLocal[client]
			local.ScopeCheckpoints = []LocalScopeCheckpoint{{Scope: pushOpsScope}}
			state.ClientLocal[client] = local
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := cloneState(baseState)
			fixture := cloneResolvedOperationInput(ResolvedOperationInput{PortableSeed: &baseFixture}).PortableSeed
			mutate(&state, fixture)
			model, err := New(Config{State: state, Clock: clock, Seed: 705})
			if err != nil {
				t.Fatalf("create seed model: %v", err)
			}
			before := model.Snapshot()
			payload, err := json.Marshal(installPortableSeedPayload{UserID: string(client.UserID), ClientID: string(client.ClientID), PortableSeedArtifactID: portableSeedArtifactDefinitionID, SeedFixtureID: portableSeedFixtureID})
			if err != nil {
				t.Fatalf("marshal seed payload: %v", err)
			}
			if _, err := model.ApplyResolved(context.Background(), scenarios.Operation{ContractOperation: "artifact", Name: "install-portable-seed", Payload: payload}, ResolvedOperationInput{PortableSeed: fixture}); err == nil {
				t.Fatal("ApplyResolved accepted an invalid portable seed")
			}
			if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatal("invalid portable seed changed model state")
			}
			fresh := newTokenAuthority(705)
			bindings := BindingSet{HasScope: true, Scope: pushOpsScope}
			if got, want := model.authority.Mint(string(TokenKindSeedReceipt), bindings), fresh.Mint(string(TokenKindSeedReceipt), bindings); got != want {
				t.Fatal("invalid portable seed changed the receipt token sequence")
			}
		})
	}
}

func preparedPullApply(t *testing.T) (*Model, ClientKey, *ResolvedStep, CapturedProjection) {
	t.Helper()
	state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA})
	row := canonicalStringRowIdentity(tableA, fieldA, "pull-apply")
	addTestPullEffect(&state, scopeA, testEffectPosition(generation, 2, 1, 1), row, "pull-version", Checksum{0: 31})
	model, _ := newPullRebuildTestModel(t, state)
	installTestIncrementalCursor(t, model, client, scopeA, StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart})
	result := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
	if result.HTTP == nil || result.HTTP.Status != 200 || result.Pull == nil || result.Pull.HasMore {
		t.Fatalf("prepare pull result = %#v", result)
	}
	var projection CapturedProjection
	for _, value := range model.state.Projections {
		projection = value
		break
	}
	return model, client, &ResolvedStep{StepID: "STEP-PULL-001", OperationKey: "pull/request-page", Result: result}, projection
}

func portableSeedTestFixture(t *testing.T) (State, Clock, ClientKey, PortableSeedFixture) {
	t.Helper()
	state, clock, schema, table := pushOpsFixture(t, false, false)
	client := pushOpsClientKey()
	server := state.Clients[client]
	server.ScopeAssignments = nil
	server.Checkpoints = nil
	state.Clients[client] = server
	state.ClientLocal[client] = ClientLocalState{ClientGeneration: 1, CurrentSchema: schema, AuthoritativeScopeSetVersion: 1, Lifecycle: ClientLifecycleState{State: ClientLifecycleLocalReady, ChangedAt: timePointer(clock.now)}}
	state.Authorization = AuthorizationState{Roles: []RoleCapabilities{{Role: "existing-role", Capabilities: []Capability{"existing-capability"}}}, WritePolicies: []WritePolicyDecision{{User: client.UserID, Table: table.ID, Allowed: false}}}
	rows := make([]PortableSeedRowFixture, 0, portableSeedRowCount)
	digestRows := make([]rebuildDigestRow, 0, portableSeedRowCount)
	for ordinal := 1; ordinal <= portableSeedRowCount; ordinal++ {
		id := fmt.Sprintf("seed-%06d", ordinal)
		row := pushOpsAuthoritativeRow(t, state.Schemas[schema], table, id, fmt.Sprintf("value-%06d", ordinal), RowVersion(fmt.Sprintf("seed-version-%06d", ordinal)), false)
		rows = append(rows, PortableSeedRowFixture{Scope: pushOpsScope, Ordinal: uint64(ordinal), Row: row})
		digestRows = append(digestRows, rebuildDigestRow{Identity: row.Identity, Checksum: row.Checksum})
	}
	checksum, valid := referenceScopeChecksum(schema, pushOpsScope, digestRows)
	if !valid {
		t.Fatal("derive portable seed scope checksum failed")
	}
	artifact := []byte("deterministic portable seed artifact")
	manifest := []byte(`{"fixture_id":"SEEDFIX-PORTABLE-SHARED-1000-001"}`)
	fixture := PortableSeedFixture{
		FixtureID:            portableSeedFixtureID,
		ArtifactDefinitionID: portableSeedArtifactDefinitionID,
		ArtifactBytes:        artifact,
		ArtifactSHA256:       sha256.Sum256(artifact),
		ManifestBytes:        manifest,
		ManifestSHA256:       sha256.Sum256(manifest),
		ExportID:             "7c482c0c-3f5c-4e1c-a292-d05157577805",
		Schema:               schema,
		RegistryGeneration:   state.Registry.CurrentGeneration,
		StreamGeneration:     state.Stream.Authority.ActiveGeneration,
		SnapshotBoundary:     state.Stream.Authority.GlobalMaterializationBoundary,
		PortableScopeIDs:     []ScopeID{pushOpsScope},
		Scopes:               []PortableSeedScopeFixture{{Scope: pushOpsScope, MembershipGeneration: 1, RetentionGeneration: 1, Cardinality: portableSeedRowCount, Checksum: checksum}},
		Rows:                 rows,
	}
	return state, clock, client, fixture
}

func applyResolvedTestOperation(t *testing.T, model *Model, contractOperation, name string, payload any, input ResolvedOperationInput) StepResult {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal resolved operation payload: %v", err)
	}
	result, err := model.ApplyResolved(context.Background(), scenarios.Operation{ContractOperation: contractOperation, Name: name, Payload: encoded}, input)
	if err != nil {
		t.Fatalf("ApplyResolved %s/%s returned error: %v", contractOperation, name, err)
	}
	return result
}

func splitOperationKeyForTest(t *testing.T, key string) [2]string {
	t.Helper()
	for index := range key {
		if key[index] == '/' {
			return [2]string{key[:index], key[index+1:]}
		}
	}
	t.Fatalf("operation key %q has no separator", key)
	return [2]string{}
}
