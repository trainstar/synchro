package reactnative

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateSchemaCheckScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateSchemaCheckScenario(loadSchemaCheckAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored schema-check scenario: %v", err)
	}
}

func TestValidateSchemaCheckScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"unsupported completion", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-PERF-SCHEMA-CHECK-016" {
					scenario.Steps[index].NativeBinding.Completion = "idle"
				}
			}
		}},
		{"controller operation", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001" {
					scenario.Steps[index].Operation.Name = "other"
				}
			}
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-schema-check-android"
				}
			}
		}},
		{"measurement case", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].MeasurementSample != nil {
					scenario.Steps[index].MeasurementSample.Parameters = json.RawMessage(`{"schema_case":"changed"}`)
					return
				}
			}
		}},
		{"measurement operation case", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].MeasurementSample != nil {
					scenario.Steps[index].MeasurementSample.Operation.Value = json.RawMessage(`{"schema_case":"changed"}`)
					return
				}
			}
		}},
		{"controller wire", func(scenario *scenarios.Scenario) {
			wire := scenario.WireExpectations[0]
			wire.StepID = "STEP-PERF-SCHEMA-CHECK-CLASS1-COMMIT-001"
			scenario.WireExpectations = append(scenario.WireExpectations, wire)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneSchemaCheckScenario(loadSchemaCheckAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateSchemaCheckScenario(scenario); err == nil {
				t.Fatalf("changed schema-check contract was accepted: error=%v", err)
			}
		})
	}
}

func TestSchemaCheckCallsCarryAllAuthoredDispatchSamples(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatalf("derive schema-check calls: %v", err)
	}
	// The authored scenario owns these counts. Derive them so the test cannot
	// drift from the contract it checks.
	publicSteps := 0
	for _, step := range scenario.Steps {
		if step.Transport == "http" {
			publicSteps++
		}
	}
	if len(scenario.WireExpectations) != publicSteps {
		t.Fatalf("schema-check calls=%d wires=%d, want %d public-call steps", len(calls), len(scenario.WireExpectations), publicSteps)
	}
	plan, err := schemaCheckDispatchPlan(scenario)
	if err != nil {
		t.Fatalf("read schema-check dispatch plan: %v", err)
	}
	samples := make(map[string]int, len(plan.Strata))
	measured := 0
	for _, call := range calls {
		if call.step.MeasurementSample != nil {
			measured++
			samples[string(call.step.MeasurementSample.StratumID)]++
		}
	}
	if measured != 18 {
		t.Fatalf("schema-check measured calls=%d want=18", measured)
	}
	for _, stratum := range plan.Strata {
		if samples[string(stratum.StratumID)] != 3 {
			t.Fatalf("schema-check stratum=%s samples=%d want=3", stratum.StratumID, samples[string(stratum.StratumID)])
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 20 {
		t.Fatalf("schema-check lifecycle boundaries=%d want=20", len(scenario.NativeLifecycleBoundaries))
	}
	publicCalls := make(map[scenarios.StepID]schemaCheckCall, len(calls))
	for _, call := range calls {
		publicCalls[call.step.ID] = call
	}
	for _, boundary := range scenario.NativeLifecycleBoundaries {
		call, found := publicCalls[boundary.AfterStepID]
		if !found || boundary.UserID != call.step.NativeBinding.UserID || boundary.ClientID != call.step.NativeBinding.ClientID {
			t.Fatalf("schema-check lifecycle boundary=%s step=%s is not bound to its public call", boundary.ID, boundary.AfterStepID)
		}
	}
}

func TestNewSchemaCheckCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: scenario, Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android schema-check coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android schema-check coordinator URL=%q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android schema-check adapter URL=%q", coordinator.adapter)
	}
	// Each call opens, captures before state, synchronizes, and captures final state, plus
	// one for each lifecycle boundary and one terminal exchange.
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatalf("derive schema-check calls: %v", err)
	}
	want := 1
	normal := 0
	for _, call := range calls {
		if schemaCheckIsProof(call.step) {
			want += schemaCheckProofCommandCount(call.step)
		} else {
			if call.step.ID != "STEP-PERF-SCHEMA-CHECK-001" {
				normal++
			}
			want += 4
		}
	}
	want += len(scenario.NativeLifecycleBoundaries) - 2
	if normal != 35 {
		t.Fatalf("normal schema-check windows=%d want=35", normal)
	}
	if coordinator.ExchangeCount() != want {
		t.Fatalf("schema-check exchange count=%d want=%d", coordinator.ExchangeCount(), want)
	}
}

func TestSchemaCheckCommandEncodesEmptyStepsAsArray(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command(coordinator.calls[0], "client", "open", map[string]any{"client_key": coordinator.calls[0].sessionKey}, nil)
	encoded, err := json.Marshal(command)
	if err != nil || command.Action.Steps == nil || len(command.Action.Steps) != 0 || !strings.Contains(string(encoded), `"steps":[]`) {
		t.Fatalf("schema-check empty command steps=%#v encoded=%s error=%v", command.Action.Steps, encoded, err)
	}
}

func TestSchemaCheckSynchronizationCommandsCarryAuthoredStartStep(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	for _, call := range coordinator.calls {
		if schemaCheckIsProof(call.step) {
			continue
		}
		command := coordinator.command(call, "client", "synchronize-step", map[string]any{
			"client_key": call.sessionKey, "method": call.step.NativeBinding.Method, "completion": call.step.NativeBinding.Completion,
		}, []scenarios.StepID{call.step.ID})
		method, methodOK := command.Action.Action.Parameters["method"].(string)
		if command.Action.Action.Actor != "client" || command.Action.Action.Command != "synchronize-step" || !methodOK || method != "start" {
			t.Fatalf("schema-check step=%s actor=%q command=%q method=%q method_ok=%t", call.step.ID, command.Action.Action.Actor, command.Action.Action.Command, method, methodOK)
		}
		if command.Action.Steps == nil || len(command.Action.Steps) != 1 {
			t.Fatalf("schema-check step=%s command steps=%#v", call.step.ID, command.Action.Steps)
		}
		operation := command.Action.Steps[0].Operation
		if operation.ContractOperation != call.step.Operation.ContractOperation || operation.Name != call.step.Operation.Name || !semanticRawJSONEqual(operation.Payload, call.step.Operation.Payload) {
			t.Fatalf("schema-check step=%s operation=%#v want=%#v", call.step.ID, operation, call.step.Operation)
		}
	}
}

func TestSchemaCheckCaptureOmitsUndeclaredDurableProof(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.waiting = schemaCheckWaitingSync
	response, err := coordinator.advanceLocked(context.Background(), 1)
	if err != nil || response.Command == nil {
		t.Fatalf("create schema-check capture command: command=%#v error=%v", response.Command, err)
	}
	parameters := response.Command.Action.Action.Parameters
	sources, ok := parameters["sources"].([]string)
	want := []string{"scope-state", "sync-status", "sync-events", "request-trace"}
	if !ok || !slices.Equal(sources, want) {
		t.Fatalf("schema-check capture sources=%#v want=%#v", parameters["sources"], want)
	}
	if _, found := parameters["durable_proof_identity"]; found {
		t.Fatalf("schema-check capture included an undeclared durable proof identity")
	}
}

func TestSchemaCheckDispatchRejectsWrongActionAndOldCursor(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatal(err)
	}
	var call schemaCheckCall
	for _, candidate := range calls {
		if candidate.step.ID == "STEP-PERF-SCHEMA-CHECK-007" {
			call = candidate
		}
	}
	wire, err := schemaCheckWireExpectation(scenario, call.step.ID)
	if err != nil {
		t.Fatal(err)
	}
	source := clientSchema{Version: 1, Hash: strings.Repeat("a", 64)}
	target := clientSchema{Version: 2, Hash: strings.Repeat("b", 64)}
	scope, oldCursor, finalCursor := "runtime-scope", "old-cursor", "later-pull-cursor"
	issued := hashFingerprint("issued-replacement")
	complete := true
	notTruncated := false
	encode := func(value any) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	newEvidence := func() (finalCapture, finalCapture) {
		connect := transportObservation{Sequence: 2, OperationClass: "connect", StatusCode: http.StatusOK, DurationNanoseconds: 1, CursorFingerprints: []string{hashFingerprint(oldCursor)}, CursorFingerprintsComplete: &complete,
			RequestFacts:         encode(map[string]any{"schema_version": source.Version, "schema_hash": source.Hash}),
			ConnectResponseFacts: encode(map[string]any{"action": wire.Action, "schema_version": target.Version, "schema_hash": target.Hash, "affected_scope_fingerprints": []string{}, "affected_scopes_complete": true, "scope_cursor_updates": map[string]*string{hashFingerprint(scope): &issued}, "scope_cursor_updates_complete": true}),
		}
		prior := connect
		prior.Sequence = 1
		pull := transportObservation{Sequence: 3, OperationClass: "pull", StatusCode: http.StatusOK, DurationNanoseconds: 1, CursorFingerprints: []string{issued}, CursorFingerprintsComplete: &complete,
			RequestFacts:      encode(map[string]any{"schema_version": target.Version, "schema_hash": target.Hash}),
			PullResponseFacts: encode(map[string]any{"change_count": 0, "has_more": false, "rebuild_scope_count": 0, "checksum_count": 1, "scope_cursor_fingerprints": []string{}, "scope_cursor_fingerprints_complete": true}),
		}
		before := finalCapture{ClientState: encode(inspectedClientState{Schema: &source, ScopeStates: []clientScopeState{{ScopeID: scope, Cursor: &oldCursor}}, ProvenanceMaintenanceWorkCursor: "0", MigrationJournal: []byte("null"), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte("[]"), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated}), Events: encode([]any{}), Trace: encode(traceSnapshot{Observations: []transportObservation{prior}, SequenceCheckpoint: 1})}
		after := finalCapture{ClientState: encode(inspectedClientState{Schema: &target, ScopeStates: []clientScopeState{{ScopeID: scope, Cursor: &finalCursor}}, ProvenanceMaintenanceWorkCursor: "0", MigrationJournal: []byte("null"), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte("[]"), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated}), Events: encode([]any{
			map[string]any{"type": "schema_applying", "source": source, "target": target, "action": wire.Action},
			map[string]any{"type": "schema_applied", "source": source, "target": target, "action": wire.Action},
		}), Trace: encode(traceSnapshot{Observations: []transportObservation{prior, connect, pull}, SequenceCheckpoint: 3})}
		return before, after
	}
	before, after := newEvidence()
	if err := validateSchemaCheckDispatch(call, before, after, wire.Action, target, scope, false); err != nil {
		t.Fatalf("valid replacement rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*traceSnapshot)
	}{
		{"wrong action", func(trace *traceSnapshot) {
			var facts map[string]any
			if err := json.Unmarshal(trace.Observations[1].ConnectResponseFacts, &facts); err != nil {
				t.Fatal(err)
			}
			facts["action"] = "none"
			trace.Observations[1].ConnectResponseFacts = encode(facts)
		}},
		{"old cursor on target pull", func(trace *traceSnapshot) {
			trace.Observations[2].CursorFingerprints = []string{hashFingerprint(oldCursor)}
		}},
		{"old token relabeled as replacement", func(trace *traceSnapshot) {
			var facts map[string]any
			if err := json.Unmarshal(trace.Observations[1].ConnectResponseFacts, &facts); err != nil {
				t.Fatal(err)
			}
			facts["scope_cursor_updates"] = map[string]string{hashFingerprint(scope): hashFingerprint(oldCursor)}
			trace.Observations[1].ConnectResponseFacts = encode(facts)
		}},
		{"prior connect cannot cover current call", func(trace *traceSnapshot) {
			trace.Observations = []transportObservation{trace.Observations[0], trace.Observations[2]}
			trace.Observations[1].Sequence = 2
			trace.SequenceCheckpoint = 2
		}},
		{"overflow", func(trace *traceSnapshot) { trace.Overflowed = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, after := newEvidence()
			trace, err := captureTraceFromRaw(after.Trace)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&trace)
			after.Trace = encode(trace)
			if err := validateSchemaCheckDispatch(call, before, after, wire.Action, target, scope, false); err == nil {
				t.Fatal("invalid schema dispatch passed")
			}
		})
	}
	for _, name := range []string{"missing migration journal", "truncated migration journal", "truncated physical schema", "missing aggregate overflow", "overall capture overflow", "full event ring"} {
		t.Run(name, func(t *testing.T) {
			before, after := newEvidence()
			var state inspectedClientState
			if err := json.Unmarshal(after.ClientState, &state); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "missing migration journal":
				state.MigrationJournal = nil
			case "truncated migration journal":
				state.MigrationJournalTruncated = &complete
			case "truncated physical schema":
				state.PhysicalSchemaTruncated = &complete
			case "overall capture overflow":
				state.CaptureOverflowed = &complete
			case "missing aggregate overflow":
				state.CaptureOverflowed = nil
			case "full event ring":
				var events []map[string]any
				if err := json.Unmarshal(after.Events, &events); err != nil {
					t.Fatal(err)
				}
				for len(events) < 256 {
					events = append(events, map[string]any{"type": "state_changed"})
				}
				after.Events = encode(events)
			}
			after.ClientState = encode(state)
			if err := validateSchemaCheckDispatch(call, before, after, wire.Action, target, scope, false); err == nil {
				t.Fatal("incomplete schema capture passed")
			}
		})
	}
}

func TestSchemaCheckFreshAuthoredStepsResolveExistingAliases(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	coordinator := &SchemaCheckCoordinator{config: SchemaCheckCoordinatorConfig{Scenario: scenario}, runtimeIDs: map[string]json.RawMessage{
		"scope-user-a": json.RawMessage(`"runtime-scope-a"`), "scope-user-b": json.RawMessage(`"runtime-scope-b"`),
	}}
	for _, step := range scenario.Steps {
		if step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" || schemaCheckIsProof(step) {
			continue
		}
		var payload struct {
			Schema clientSchema `json:"schema"`
		}
		if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.Schema.Version != 0 {
			continue
		}
		if alias, err := coordinator.stepSchemaAlias(step); err != nil || alias != "" {
			t.Fatalf("fresh schema alias=%q error=%v", alias, err)
		}
		if _, err := coordinator.stepRuntimeScope(step); err != nil {
			t.Fatalf("fresh authored scope: %v", err)
		}
	}
}

func TestMigrationCaptureCodecAcceptsNativeStoredShapesWithoutNormalizingPlans(t *testing.T) {
	if _, err := decodePhysicalSchema([]byte(`[],"extra":true`)); err == nil {
		t.Fatal("physical schema accepted trailing JSON")
	}
	encode := func(value any) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	base := map[string]string{"journal_version": "2", "migration_plan_version": "2", "target_manifest_json": "{}", "affected_scopes_json": "[]", "scope_cursor_updates_json": "{}", "migration_plan_json": " {\"native_operations\":[]} ", "migration_plan_hash": strings.Repeat("a", 64)}
	notTruncated := false
	schema := clientSchema{Version: 1, Hash: strings.Repeat("b", 64)}
	for _, native := range []string{"swift", "kotlin"} {
		t.Run(native, func(t *testing.T) {
			stored := map[string]string{}
			for key, text := range base {
				stored[key] = text
			}
			phase := "applied"
			if native == "swift" {
				stored["is_schema_reset"] = "0"
			} else {
				phase = "ddl_applied"
				stored["reset_materialization"] = "0"
				stored["target_tables_json"] = "[]"
			}
			journal := migrationJournalCapture{Source: clientSchema{}, Target: schema, Action: "replace", Phase: phase, Stored: stored}
			state := inspectedClientState{Schema: &schema, ProvenanceMaintenanceWorkCursor: "0", MigrationJournal: encode(journal), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte(`[{"table_name":"items","name":"id","type":"TEXT","not_null":false,"primary_key_position":1}]`), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated}
			decoded, err := decodeClientState(encode(state))
			if err != nil {
				t.Fatal(err)
			}
			inspection, err := decodeMigrationJournal(decoded.MigrationJournal)
			if err != nil || inspection == nil || inspection.Stored["migration_plan_json"] != stored["migration_plan_json"] {
				t.Fatalf("stored bytes changed: %v", err)
			}
			if err := decoded.requireCompleteMigrationCapture(); err != nil {
				t.Fatal(err)
			}
			legacy := state
			legacy.MigrationJournal = nil
			legacy.MigrationJournalTruncated = nil
			legacy.PhysicalSchema = nil
			legacy.PhysicalSchemaTruncated = nil
			legacy.CaptureOverflowed = nil
			if _, err := decodeClientState(encode(legacy)); err != nil {
				t.Fatal(err)
			}
			if err := legacy.requireCompleteMigrationCapture(); err == nil {
				t.Fatal("missing migration capture proved complete")
			}
			for _, test := range []struct {
				name   string
				mutate func(*inspectedClientState)
			}{
				{"missing flag", func(value *inspectedClientState) { value.MigrationJournalTruncated = nil }},
				{"null physical schema", func(value *inspectedClientState) { value.PhysicalSchema = []byte("null") }},
				{"wrong physical column type", func(value *inspectedClientState) {
					value.PhysicalSchema = []byte(`[{"table_name":"items","name":"id","type":"TEXT","not_null":1,"primary_key_position":1}]`)
				}},
				{"oversized stored bytes", func(value *inspectedClientState) {
					changed := journal
					changed.Stored = map[string]string{}
					for key, text := range stored {
						changed.Stored[key] = text
					}
					changed.Stored["migration_plan_json"] = strings.Repeat("x", 65_536)
					value.MigrationJournal = encode(changed)
				}},
			} {
				t.Run(test.name, func(t *testing.T) {
					changed := state
					test.mutate(&changed)
					if _, err := decodeClientState(encode(changed)); err == nil {
						t.Fatal("invalid migration capture decoded")
					}
				})
			}
		})
	}
}

func TestSchemaCheckIdentityEvidenceUsesCapturedPullRequests(t *testing.T) {
	trace := func(generation, scopeSetVersion uint64) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(traceSnapshot{Observations: []transportObservation{{
			Sequence:       1,
			OperationClass: "pull",
			RequestFacts: json.RawMessage(fmt.Sprintf(
				`{"client_generation":%d,"scope_set_version":%d}`,
				generation,
				scopeSetVersion,
			)),
		}}, SequenceCheckpoint: 1})
		if err != nil {
			t.Fatalf("encode schema-check identity trace: %v", err)
		}
		return raw
	}
	coordinator := &SchemaCheckCoordinator{
		calls: []schemaCheckCall{
			{step: scenarios.Step{ID: "step-a"}, clientKey: "client-a"},
			{step: scenarios.Step{ID: "step-b"}, clientKey: "client-b"},
		},
		captures: map[scenarios.StepID]finalCapture{
			"step-a": {Trace: trace(7, 9)},
			"step-b": {Trace: trace(7, 9)},
		},
		beforeCaptures: map[scenarios.StepID]finalCapture{
			"step-a": {Trace: json.RawMessage(`{"observations":[],"overflowed":false,"sequenceCheckpoint":0}`)},
			"step-b": {Trace: json.RawMessage(`{"observations":[],"overflowed":false,"sequenceCheckpoint":0}`)},
		},
	}
	generation, scopeSetVersion, err := coordinator.observedClientIdentities()
	if err != nil || generation != 7 || scopeSetVersion != 9 {
		t.Fatalf("schema-check observed identity generation=%d scope_set_version=%d want generation=7 scope_set_version=9 error=%v", generation, scopeSetVersion, err)
	}
	coordinator.captures["step-b"] = finalCapture{Trace: trace(8, 9)}
	if _, _, err := coordinator.observedClientIdentities(); err == nil || !strings.Contains(err.Error(), "observed") || !strings.Contains(err.Error(), "want") {
		t.Fatalf("schema-check inconsistent identity error=%v want observed and expected values", err)
	}
}

func TestSchemaCheckHasNoRawObserverRead(t *testing.T) {
	raw, err := os.ReadFile("schema_check.go")
	if err != nil {
		t.Fatalf("read schema-check coordinator source: %v", err)
	}
	for _, forbidden := range []string{"OpenObserver(", "synchro.sync_clients"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("schema-check coordinator observed forbidden raw read %q want controller and captured evidence only", forbidden)
		}
	}
}

func loadSchemaCheckAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadSchemaCheckScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored schema-check scenario: %v", err)
	}
	return scenario
}

func cloneSchemaCheckScenario(scenario scenarios.Scenario) scenarios.Scenario {
	raw, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(raw, &clone); err != nil {
		panic(err)
	}
	return clone
}

func TestSchemaProofProxyForwardsActualBytesAndRecordsUpstreamOutcome(t *testing.T) {
	body := []byte(`{ "client_id":"client-schema-proof-committed", "batch_id":"actual-batch", "mutations":[] }`)
	responseBody := `{"accepted":[],"rejected":[]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload json.RawMessage
		if json.NewDecoder(request.Body).Decode(&payload) != nil || string(payload) != string(body) {
			t.Error("proxy changed actual outgoing bytes")
		}
		if request.Header.Get("Authorization") != "Bearer actual-token" {
			t.Error("proxy changed resolved authorization")
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(responseBody))
	}))
	defer upstream.Close()
	coordinator := &SchemaCheckCoordinator{upstream: upstream.URL, proxyHTTP: make(map[string]uint64), proxyPushes: make(map[string][]schemaCheckPush)}
	request := httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer actual-token")
	writer := httptest.NewRecorder()
	coordinator.proxyAdapter(writer, request)
	pushes := coordinator.proxyPushes["client-schema-proof-committed"]
	if writer.Code != http.StatusOK || writer.Body.String() != responseBody || len(pushes) != 1 || string(pushes[0].Request) != string(body) || string(pushes[0].Response) != responseBody || pushes[0].Status != http.StatusOK || coordinator.proxyHTTP["*"] != 1 {
		t.Fatal("proxy did not record its actual HTTP exchange")
	}
}

func TestSchemaProofHistoricalReplayRejectsChangedIntentAndInventedOutcome(t *testing.T) {
	s1 := clientSchema{Version: 1, Hash: strings.Repeat("a", 64)}
	s2 := clientSchema{Version: 2, Hash: strings.Repeat("b", 64)}
	encode := func(value any) json.RawMessage {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	base := "baseline-version"
	mutation := schemaProofWireMutation{MutationID: "m1", Table: "items", PK: map[string]json.RawMessage{"id": []byte(`"row"`)}, AuthoredSchema: s1, Operation: "update", BaseVersion: &base, ClientVersion: "authored-client-version", Columns: map[string]json.RawMessage{"value": []byte(`"41"`)}}
	response := encode(map[string]any{"accepted": []any{map[string]any{"mutation_id": "m1", "status": "applied", "outcome_schema": s1, "server_version": "accepted-version", "row_checksum": map[string]any{"digest": strings.Repeat("c", 64)}}}, "rejected": []any{}})
	initial := schemaCheckPush{Request: encode(schemaProofPushRequest{ClientID: "client", BatchID: "initial", Schema: s1, Mutations: []json.RawMessage{encode(mutation)}}), Response: response, Status: http.StatusOK}
	newReplay := func() schemaCheckPush {
		return schemaCheckPush{Request: encode(schemaProofPushRequest{ClientID: "client", BatchID: "successor", Schema: s2, Mutations: []json.RawMessage{encode(mutation)}}), Response: append([]byte(nil), response...), Status: http.StatusOK}
	}
	coordinator := &SchemaCheckCoordinator{}
	if err := coordinator.validateProofReplay(initial, newReplay(), s1, s2); err != nil {
		t.Fatalf("current S2 successor rejected: %v", err)
	}
	for _, name := range []string{"M1 payload", "authored schema", "successor schema", "same batch rewritten", "invented S2 outcome", "HTTP failure"} {
		t.Run(name, func(t *testing.T) {
			replay := newReplay()
			var request schemaProofPushRequest
			_ = json.Unmarshal(replay.Request, &request)
			changed := mutation
			switch name {
			case "M1 payload":
				changed.Columns = map[string]json.RawMessage{"value": []byte(`"42"`)}
				request.Mutations[0] = encode(changed)
			case "authored schema":
				changed.AuthoredSchema = s2
				request.Mutations[0] = encode(changed)
			case "successor schema":
				request.Schema = s1
			case "same batch rewritten":
				request.BatchID = "initial"
			case "invented S2 outcome":
				replay.Response = []byte(strings.ReplaceAll(string(response), string(encode(s1)), string(encode(s2))))
			case "HTTP failure":
				replay.Status = http.StatusConflict
			}
			replay.Request = encode(request)
			if err := coordinator.validateProofReplay(initial, replay, s1, s2); err == nil {
				t.Fatal("changed historical replay passed")
			}
		})
	}
}

func TestSchemaProofActivationRequiresActualIssuedCursorWithTargetSchema(t *testing.T) {
	source := clientSchema{Version: 1, Hash: strings.Repeat("a", 64)}
	target := clientSchema{Version: 2, Hash: strings.Repeat("b", 64)}
	old, replacement := "old-token", "actual-server-token"
	complete, notTruncated := true, false
	journal := migrationJournalCapture{Source: source, Target: target, Stored: map[string]string{"scope_cursor_updates_json": `{"scope":"actual-server-token"}`}}
	columns := []physicalSchemaColumn{
		{TableName: "cf_items", Name: "id", Type: "TEXT", PrimaryKeyPosition: 1},
		{TableName: "cf_items", Name: "value", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "note", Type: "TEXT"},
		{TableName: "cf_items", Name: "owner_id", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "updated_at", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "deleted_at", Type: "TEXT"},
	}
	physicalSchema, err := json.Marshal(append(append([]physicalSchemaColumn(nil), columns...), physicalSchemaColumn{TableName: "cf_global_items", Name: "id", Type: "TEXT", PrimaryKeyPosition: 1}))
	if err != nil {
		t.Fatal(err)
	}
	state := inspectedClientState{Schema: &target, ScopeStates: []clientScopeState{{ScopeID: "scope", Cursor: &replacement}}, PhysicalSchema: physicalSchema}
	state.PhysicalSchemaTruncated, state.CaptureOverflowed = &notTruncated, &notTruncated
	issued := hashFingerprint(replacement)
	facts := map[string]any{"action": "replace", "schema_version": target.Version, "schema_hash": target.Hash, "affected_scope_fingerprints": []string{}, "affected_scopes_complete": true, "scope_cursor_updates": map[string]*string{hashFingerprint("scope"): &issued}, "scope_cursor_updates_complete": true}
	raw, _ := json.Marshal(facts)
	trace := traceSnapshot{Observations: []transportObservation{{Sequence: 1, OperationClass: "connect", StatusCode: http.StatusOK, DurationNanoseconds: 1, CursorFingerprints: []string{hashFingerprint(old)}, CursorFingerprintsComplete: &complete, RequestFacts: []byte(`{"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`), ConnectResponseFacts: raw}}, SequenceCheckpoint: 1}
	coordinator := &SchemaCheckCoordinator{tableName: "cf_items", primaryKey: "id", proofPhysicalSchemas: map[clientSchema][]physicalSchemaColumn{target: columns}}
	if err := coordinator.validateProofActivation(state, &journal, trace, true, false); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.validateProofActivation(state, &journal, traceSnapshot{}, true, true); err != nil {
		t.Fatalf("zero-HTTP recovery rejected: %v", err)
	}
	if err := coordinator.validateProofActivation(state, &journal, traceSnapshot{}, true, false); err == nil {
		t.Fatal("journal cut omitted actual connect trace")
	}
	for _, name := range []string{"old active cursor", "old active schema", "journal cursor substitution", "target DDL missing", "wrong value type", "wrong value nullability", "wrong primary key position", "unexpected synchronized column", "missing support column", "wrong support type", "wrong field binding", "duplicate subject column"} {
		t.Run(name, func(t *testing.T) {
			changed := state
			changed.ScopeStates = append([]clientScopeState(nil), state.ScopeStates...)
			changedJournal := journal
			changedJournal.Stored = map[string]string{"scope_cursor_updates_json": journal.Stored["scope_cursor_updates_json"]}
			switch name {
			case "old active cursor":
				changed.ScopeStates[0].Cursor = &old
			case "old active schema":
				changed.Schema = &source
			case "journal cursor substitution":
				changedJournal.Stored["scope_cursor_updates_json"] = `{"scope":"other-token"}`
			case "target DDL missing":
				changed.PhysicalSchema = []byte(`[]`)
			case "wrong value type":
				changed.PhysicalSchema = []byte(strings.Replace(string(state.PhysicalSchema), `"name":"value","type":"TEXT"`, `"name":"value","type":"INTEGER"`, 1))
			case "wrong value nullability":
				changed.PhysicalSchema = []byte(strings.Replace(string(state.PhysicalSchema), `"not_null":true`, `"not_null":false`, 1))
			case "wrong primary key position":
				changed.PhysicalSchema = []byte(strings.Replace(string(state.PhysicalSchema), `"primary_key_position":1`, `"primary_key_position":0`, 1))
			case "unexpected synchronized column":
				changed.PhysicalSchema = []byte(strings.TrimSuffix(string(state.PhysicalSchema), "]") + `,{"table_name":"cf_items","name":"extra","type":"TEXT","not_null":false,"primary_key_position":0}]`)
			case "missing support column":
				changed.PhysicalSchema, _ = json.Marshal(columns[:5])
			case "wrong support type":
				changed.PhysicalSchema = []byte(strings.Replace(string(state.PhysicalSchema), `"name":"updated_at","type":"TEXT"`, `"name":"updated_at","type":"INTEGER"`, 1))
			case "wrong field binding":
				changed.PhysicalSchema = []byte(strings.Replace(string(state.PhysicalSchema), `"name":"value"`, `"name":"physical_value"`, 1))
			case "duplicate subject column":
				changed.PhysicalSchema = []byte(strings.TrimSuffix(string(state.PhysicalSchema), "]") + `,{"table_name":"cf_items","name":"id","type":"TEXT","not_null":false,"primary_key_position":1}]`)
			}
			if err := coordinator.validateProofActivation(changed, &changedJournal, trace, true, false); err == nil {
				t.Fatal("invalid activation cut passed")
			}
		})
	}
	for _, binding := range []struct{ table, primary string }{{"items", "id"}, {"cf_items", "physical_id"}} {
		changed := *coordinator
		changed.tableName, changed.primaryKey = binding.table, binding.primary
		if err := changed.bindProofPhysicalSchema("schema-v2"); err == nil {
			t.Fatal("wrong proof subject binding passed")
		}
	}
}

func TestSchemaProofM2PermitsOnlyValidatedNormalizedBaseTransition(t *testing.T) {
	old, accepted, predecessor, normalizedID, batch := "local-base", "accepted-M1-base", "m1", "m2-normalized", "m2-batch"
	ordinal := uint64(0)
	original := schemaProofMutation{MutationID: "m2-local", LocalOrder: 3, TableID: "items", TableName: "items", RecordID: "row", PrimaryKeyFieldID: "id", PrimaryKeyLogicalType: "string", Operation: "update", AuthoredSchema: clientSchema{Version: 2, Hash: strings.Repeat("b", 64)}, BaseVersion: &old, ClientVersion: "client-version", Status: "superseded_before_send", SourceKind: "trigger", DependsOnMutationID: &predecessor, NormalizedMutationID: &normalizedID}
	_ = json.Unmarshal([]byte(`[{"fieldID":"note","logicalType":"string","value":"later-note"},{"fieldID":"value","logicalType":"string","value":"42"}]`), &original.AuthoredFields)
	normalized := original
	normalized.MutationID, normalized.LocalOrder, normalized.SourceKind, normalized.Status, normalized.NormalizedMutationID = normalizedID, 4, "normalized", "pending", nil
	sealed := normalized
	sealed.BaseVersion, sealed.DependsOnMutationID, sealed.Status, sealed.SealedBatchID, sealed.SealedOrdinal = &accepted, nil, "sealed", &batch, &ordinal
	capture := func(records []schemaProofMutation) finalCapture {
		raw, _ := json.Marshal(records)
		return finalCapture{Pending: raw}
	}
	before := capture([]schemaProofMutation{original, normalized})
	coordinator := &SchemaCheckCoordinator{}
	if err := coordinator.validateProofLaterIntent(before, capture([]schemaProofMutation{original, sealed}), true, accepted, predecessor); err != nil {
		t.Fatalf("documented normalization rejected: %v", err)
	}
	for _, name := range []string{"original base changed", "original status changed", "original seal changed", "original normalized link changed", "normalized base unchanged", "dependency retained", "local order changed", "identity changed", "original record removed", "authored fields changed"} {
		t.Run(name, func(t *testing.T) {
			local, current := original, sealed
			records := []schemaProofMutation{local, current}
			switch name {
			case "original base changed":
				records[0].BaseVersion = &accepted
			case "original status changed":
				records[0].Status = "cancelled_before_send"
			case "original seal changed":
				records[0].SealedBatchID = &batch
			case "original normalized link changed":
				changedLink := "other-normalized"
				records[0].NormalizedMutationID = &changedLink
			case "normalized base unchanged":
				records[1].BaseVersion = &old
			case "dependency retained":
				records[1].DependsOnMutationID = &predecessor
			case "local order changed":
				records[1].LocalOrder++
			case "identity changed":
				records[1].MutationID = "other"
			case "original record removed":
				records = records[1:]
			case "authored fields changed":
				records[1].AuthoredFields = append(records[1].AuthoredFields[:0:0], records[1].AuthoredFields...)
				records[1].AuthoredFields[0].Value = []byte(`"lost-note"`)
			}
			if err := coordinator.validateProofLaterIntent(before, capture(records), true, accepted, predecessor); err == nil {
				t.Fatal("invalid M2 transition passed")
			}
		})
	}
	wrongDependency := normalized
	other := "other-predecessor"
	wrongDependency.DependsOnMutationID = &other
	if err := coordinator.validateProofLaterIntent(before, capture([]schemaProofMutation{original, wrongDependency}), false, "", predecessor); err == nil {
		t.Fatal("M2 depended on a different predecessor")
	}
	push := schemaCheckPush{Request: []byte(`{"mutations":[{"mutation_id":"m2-normalized","authored_schema":{"version":2,"hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}]}`)}
	if err := schemaProofNamedM2Push(before, push); err != nil {
		t.Fatal(err)
	}
	push.Request = []byte(strings.Replace(string(push.Request), `"m2-normalized"`, `"other-sealed-record"`, 1))
	if err := schemaProofNamedM2Push(before, push); err == nil {
		t.Fatal("third push named an unrelated sealed record")
	}
}

func TestSchemaProofCutRequiresAcknowledgedPauseAndDifferentProcessOnSameDatabase(t *testing.T) {
	call := schemaCheckCall{step: scenarios.Step{ID: "STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-CUT-001", NativeBinding: &scenarios.NativeStepBinding{UserID: "user-a", ClientID: "client-schema-proof-prepared"}}, clientKey: "prepared", sessionKey: "schema-proof-prepared-recover"}
	previous := actionProcessIdentity{ProcessID: "ios-app:1", DatabaseIdentityFingerprint: strings.Repeat("a", 64)}
	newCoordinator := func() *SchemaCheckCoordinator {
		return &SchemaCheckCoordinator{calls: []schemaCheckCall{call}, tableName: "items", processes: map[string]actionProcessIdentity{"schema-proof-prepared-migrate": previous, "schema-proof-prepared-s1": previous}, proofPaused: make(map[string]string), proofInterrupted: make(map[string]scenarios.StepID)}
	}
	coordinator := newCoordinator()
	if _, err := coordinator.advanceProofLocked(context.Background(), 1); err == nil {
		t.Fatal("cut without a checkpoint acknowledgement passed")
	}
	coordinator.proofPaused[call.clientKey] = "migration_prepared"
	command, err := coordinator.advanceProofLocked(context.Background(), 1)
	if err != nil || command.Command.Action.Action.Parameters["process_restart"] != true || command.Command.Action.Action.Parameters["database_mode"] != "reuse" {
		t.Fatalf("acknowledged cut command=%+v error=%v", command.Command, err)
	}
	if _, present := command.Command.Action.Action.Parameters["local_fixture"]; present {
		t.Fatal("restart repeated fixture creation")
	}
	result := func(process actionProcessIdentity) json.RawMessage {
		raw, _ := json.Marshal(map[string]any{"kind": "opened", "status": map[string]any{"state": "stopped", "retry_at": nil, "operation": nil, "failure": nil}, "process": process})
		return raw
	}
	next := previous
	next.ProcessID = "ios-app:2"
	if err := coordinator.acceptProofLocked(call, result(next)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"same process", "new database", "other lane database"} {
		t.Run(name, func(t *testing.T) {
			coordinator := newCoordinator()
			coordinator.proofPaused[call.clientKey] = "migration_prepared"
			_, _ = coordinator.advanceProofLocked(context.Background(), 1)
			changed := next
			switch name {
			case "same process":
				changed = previous
			case "new database":
				changed.DatabaseIdentityFingerprint = strings.Repeat("b", 64)
			case "other lane database":
				coordinator.processes["schema-proof-committed-s1"] = next
			}
			if err := coordinator.acceptProofLocked(call, result(changed)); err == nil {
				t.Fatal("invalid process cut passed")
			}
		})
	}
}

func TestSchemaProofAcceptedOutcomesRequireCompleteNamedStoredEvidence(t *testing.T) {
	complete, truncated := false, true
	outcome := `{"mutation_id":"m1","status":"applied","outcome_schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"server_version":"accepted-version","server_row":{"value":"41"},"row_checksum":{"digest":"checksum"}}`
	response := []byte(`{"accepted":[` + outcome + `],"rejected":[]}`)
	newState := func() inspectedClientState {
		return inspectedClientState{AcceptedMutationOutcomes: map[string]string{"m1": outcome}, AcceptedMutationOutcomesTruncated: &complete}
	}
	if err := schemaProofStoredOutcome(newState(), "m1", response); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing map", "missing truncation", "truncated", "wrong map identity", "wrong outcome identity", "wrong historical schema", "changed accepted version", "record overflow", "byte overflow", "invalid stored JSON", "duplicate outcome member"} {
		t.Run(name, func(t *testing.T) {
			state := newState()
			switch name {
			case "missing map":
				state.AcceptedMutationOutcomes = nil
			case "missing truncation":
				state.AcceptedMutationOutcomesTruncated = nil
			case "truncated":
				state.AcceptedMutationOutcomesTruncated = &truncated
			case "wrong map identity":
				delete(state.AcceptedMutationOutcomes, "m1")
				state.AcceptedMutationOutcomes["other"] = strings.Replace(outcome, `"m1"`, `"other"`, 1)
			case "wrong outcome identity":
				state.AcceptedMutationOutcomes["m1"] = strings.Replace(outcome, `"m1"`, `"other"`, 1)
			case "wrong historical schema":
				state.AcceptedMutationOutcomes["m1"] = strings.Replace(outcome, `"version":1`, `"version":2`, 1)
			case "changed accepted version":
				state.AcceptedMutationOutcomes["m1"] = strings.Replace(outcome, `"accepted-version"`, `"later-version"`, 1)
			case "record overflow":
				for index := 0; index < 512; index++ {
					id := fmt.Sprintf("other-%d", index)
					state.AcceptedMutationOutcomes[id] = strings.Replace(outcome, `"m1"`, fmt.Sprintf("%q", id), 1)
				}
			case "byte overflow":
				state.AcceptedMutationOutcomes["m1"] = strings.TrimSuffix(outcome, "}") + `,"padding":"` + strings.Repeat("x", 65_536) + `"}`
			case "invalid stored JSON":
				state.AcceptedMutationOutcomes["m1"] = "not JSON"
			case "duplicate outcome member":
				state.AcceptedMutationOutcomes["m1"] = strings.TrimSuffix(outcome, "}") + `,"mutation_id":"m1"}`
			}
			if err := schemaProofStoredOutcome(state, "m1", response); err == nil {
				t.Fatal("incomplete or unrelated durable reconciliation passed")
			}
		})
	}
	raw := []byte(`{"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"provenanceMaintenanceWorkCursor":"0","accepted_mutation_outcomes":{"m1":1},"accepted_mutation_outcomes_truncated":false}`)
	if _, err := decodeClientState(raw); err == nil {
		t.Fatal("non-string stored outcome decoded")
	}
}
