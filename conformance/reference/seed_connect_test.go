package reference

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestConnectConvertsAssignedSeedReceipt(t *testing.T) {
	model, client, fixture := installedSeedConnectModel(t)
	authorization := cloneAuthorizationState(model.state.Authorization)
	beforeInstall := cloneClientState(model.state.Clients[client])
	installPortableSeedForConnect(t, model, client, fixture)
	if !reflect.DeepEqual(model.state.Authorization, authorization) || !reflect.DeepEqual(model.state.Clients[client], beforeInstall) {
		t.Fatal("seed installation granted authorization or assignment")
	}

	assignSeedConnectScope(t, model, client)
	serverBeforeConnect := cloneClientState(model.state.Clients[client])
	result := applyTestOperation(t, model, "connect", "send", seedConnectPayloadFor(client, fixture.Schema, true))
	if result.HTTP == nil || result.HTTP.Status != 200 || result.Connect == nil || !reflect.DeepEqual(result.Connect.AddedScopes, []ScopeID{pushOpsScope}) || len(result.Connect.ScopeCursors) != 1 || result.Connect.ScopeCursors[0].Scope != pushOpsScope || result.Connect.ScopeCursors[0].Disposition != CursorDispositionIssued {
		t.Fatalf("seed receipt connect result = %#v", result)
	}
	if !reflect.DeepEqual(model.state.Authorization, authorization) {
		t.Fatal("receipt conversion changed authorization")
	}
	server := model.state.Clients[client]
	if len(server.Checkpoints) != 0 || len(server.ScopeAssignments) != 1 || server.ScopeAssignments[0].Scope != pushOpsScope || !server.ScopeAssignments[0].Assigned || server.ScopeAssignments[0].RebuildRequired || server.CurrentGeneration != serverBeforeConnect.CurrentGeneration {
		t.Fatal("receipt conversion changed server authority beyond its existing assignment state")
	}
	local := model.state.ClientLocal[client]
	if len(local.Rows) != portableSeedRowCount || len(local.Provenance) != portableSeedRowCount || len(local.SeedReceipts) != 0 || len(local.ScopeAssignments) != 1 || local.ScopeAssignments[0].RebuildRequired {
		t.Fatal("valid receipt did not preserve the seed and install a ready assignment")
	}
	checkpoint, found := testLocalCheckpoint(local, pushOpsScope)
	if !found || !checkpoint.HasCursor || checkpoint.Position != fixture.SnapshotBoundary {
		t.Fatal("valid receipt did not become a local runtime cursor at the seed boundary")
	}
	assignment, found := scopeAssignmentFor(server, pushOpsScope)
	if !found {
		t.Fatal("assigned scope is absent after receipt conversion")
	}
	status, position := validateIncrementalCursor(model, client, assignment, model.state.Scopes[pushOpsScope], pushOpsScope, checkpoint.Cursor, referenceNow(model.clock))
	if status != TokenStatusValid || position != fixture.SnapshotBoundary {
		t.Fatalf("converted cursor = %v at %#v", status, position)
	}
}

func TestConnectConsumesStaleAssignedSeedReceipt(t *testing.T) {
	model, client, fixture := installedSeedConnectModel(t)
	installPortableSeedForConnect(t, model, client, fixture)
	assignSeedConnectScope(t, model, client)
	local := model.state.ClientLocal[client]
	local.SeedReceipts[0].Checksum[0]++
	model.state.ClientLocal[client] = local

	result := applyTestOperation(t, model, "connect", "send", seedConnectPayloadFor(client, fixture.Schema, true))
	if result.HTTP == nil || result.HTTP.Status != 200 || result.Connect == nil || !reflect.DeepEqual(result.Connect.AddedScopes, []ScopeID{pushOpsScope}) || len(result.Connect.ScopeCursors) != 1 || result.Connect.ScopeCursors[0].Disposition != CursorDispositionRebuildRequired {
		t.Fatalf("invalid receipt connect result = %#v", result)
	}
	local = model.state.ClientLocal[client]
	checkpoint, found := testLocalCheckpoint(local, pushOpsScope)
	if len(local.Rows) != portableSeedRowCount || len(local.Provenance) != portableSeedRowCount || len(local.SeedReceipts) != 0 || !found || checkpoint.HasCursor || !task6LocalAssignment(t, local, pushOpsScope).RebuildRequired {
		t.Fatal("invalid receipt did not become a consumed rebuild-required scope")
	}
}

func TestConnectCleansUnassignedSeedReceipt(t *testing.T) {
	model, client, fixture := installedSeedConnectModel(t)
	installPortableSeedForConnect(t, model, client, fixture)

	result := applyTestOperation(t, model, "connect", "send", seedConnectPayloadFor(client, fixture.Schema, true))
	if result.HTTP == nil || result.HTTP.Status != 200 || result.Connect == nil || !reflect.DeepEqual(result.Connect.RemovedScopes, []ScopeID{pushOpsScope}) || len(result.Connect.AddedScopes) != 0 || len(result.Connect.ScopeCursors) != 0 {
		t.Fatalf("unassigned receipt connect result = %#v", result)
	}
	local := model.state.ClientLocal[client]
	if len(model.state.Clients[client].ScopeAssignments) != 0 || len(local.ScopeAssignments) != 0 || len(local.Rows) != 0 || len(local.Provenance) != 0 || len(local.SeedReceipts) != 0 {
		t.Fatal("unassigned receipt created authority or retained seed-local scope data")
	}
}

func installedSeedConnectModel(t *testing.T) (*Model, ClientKey, PortableSeedFixture) {
	t.Helper()
	state, clock, client, fixture := portableSeedTestFixture(t)
	model, err := New(Config{State: state, Clock: clock, Seed: 812})
	if err != nil {
		t.Fatalf("create seed connect model: %v", err)
	}
	return model, client, fixture
}

func installPortableSeedForConnect(t *testing.T, model *Model, client ClientKey, fixture PortableSeedFixture) {
	t.Helper()
	payload, err := json.Marshal(installPortableSeedPayload{
		UserID: string(client.UserID), ClientID: string(client.ClientID),
		PortableSeedArtifactID: portableSeedArtifactDefinitionID, SeedFixtureID: portableSeedFixtureID,
	})
	if err != nil {
		t.Fatalf("marshal seed install payload: %v", err)
	}
	if _, err := model.ApplyResolved(context.Background(), scenarios.Operation{ContractOperation: "artifact", Name: "install-portable-seed", Payload: payload}, ResolvedOperationInput{PortableSeed: &fixture}); err != nil {
		t.Fatalf("install seed: %v", err)
	}
}

func assignSeedConnectScope(t *testing.T, model *Model, client ClientKey) {
	t.Helper()
	applyTestOperation(t, model, "model", "set-client-assignments", map[string]any{
		"user_id": string(client.UserID), "client_id": string(client.ClientID),
		"assignments": []map[string]string{{"scope_id": string(pushOpsScope)}},
	})
}

func seedConnectPayloadFor(client ClientKey, schema SchemaRef, withReceipt bool) map[string]any {
	payload := map[string]any{
		"user_id": string(client.UserID), "client_id": string(client.ClientID),
		"runtime_version": 3, "protocol_version": 3, "client_generation": 1,
		"schema_reset":      false,
		"schema":            map[string]any{"version": schema.Version, "hash": hex.EncodeToString(schema.Hash[:])},
		"scope_set_version": 1, "known_scopes": []any{},
	}
	if withReceipt {
		payload["seed_receipts"] = map[string]string{string(pushOpsScope): string(seedReceiptSourceLocal)}
	}
	return payload
}
