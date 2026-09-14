package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRealNativeCaptureServerObservationSignals(t *testing.T) {
	if strings.TrimSpace(os.Getenv("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT")) == "" {
		t.Skip("the black-box environment is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		t.Fatalf("create native capture controller: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := controller.Close(closeContext); err != nil {
			t.Errorf("close native capture controller: %v", err)
		}
	})

	const (
		targetUserID        = "diagnostic-user"
		targetClientID      = "native-capture-client"
		decoyClientID       = "native-capture-client-decoy"
		decoyUserID         = "native-capture-user-decoy"
		targetBatchID       = "00000000-0000-4000-8000-000000000501"
		targetMutationID    = "00000000-0000-4000-8000-000000000502"
		targetMutationRow   = "00000000-0000-4000-8000-000000000503"
		targetMutationID2   = "00000000-0000-4000-8000-000000000512"
		targetMutationRow2  = "00000000-0000-4000-8000-000000000513"
		clientDecoyBatch    = "00000000-0000-4000-8000-000000000504"
		clientDecoyMutation = "00000000-0000-4000-8000-000000000505"
		clientDecoyRow      = "00000000-0000-4000-8000-000000000506"
		userDecoyBatch      = "00000000-0000-4000-8000-000000000507"
		userDecoyMutation   = "00000000-0000-4000-8000-000000000508"
		userDecoyRow        = "00000000-0000-4000-8000-000000000509"
		recordDecoyRow      = "00000000-0000-4000-8000-000000000510"
		tableDecoyRow       = "00000000-0000-4000-8000-000000000511"
		observationLimit    = 4096
	)
	scenario, err := scenarios.LoadFile(ctx, "../../..", "conformance/scenarios/performance/warm-connect-001.json")
	if err != nil {
		t.Fatalf("load native capture fixture: %v", err)
	}
	if len(scenario.Model.Setup) != 1 {
		t.Fatalf("native capture fixture setup count = %d, want 1", len(scenario.Model.Setup))
	}
	install := scenario.Model.Setup[0]
	var installPayload map[string]any
	if err := json.Unmarshal(install.Payload, &installPayload); err != nil {
		t.Fatalf("decode native capture installation: %v", err)
	}
	installPayload["clients"] = []any{map[string]any{
		"user_id":                     targetUserID,
		"client_id":                   targetClientID,
		"client_generation":           1,
		"scope_set_version":           1,
		"accepted_write_epoch":        0,
		"last_cursor_acknowledged_at": nil,
		"assigned_scope_ids":          []any{"scope-a"},
		"local_schema": map[string]any{
			"version": 1,
			"hash":    "721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44",
		},
		"local_lifecycle": "local_ready",
	}}
	policies, ok := installPayload["write_policies"].([]any)
	if !ok || len(policies) != 1 {
		t.Fatal("native capture fixture write policy is invalid")
	}
	policy, ok := policies[0].(map[string]any)
	if !ok {
		t.Fatal("native capture fixture write policy is not an object")
	}
	policy["user_id"] = targetUserID
	registry, ok := installPayload["initial_registry"].(map[string]any)
	if !ok {
		t.Fatal("native capture fixture registry is invalid")
	}
	rules, ok := registry["scope_rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatal("native capture fixture scope rules are invalid")
	}
	rule, ok := rules[0].(map[string]any)
	if !ok {
		t.Fatal("native capture fixture scope rule is not an object")
	}
	evaluations, ok := rule["evaluations"].([]any)
	if !ok || len(evaluations) != 1 {
		t.Fatal("native capture fixture scope evaluations are invalid")
	}
	evaluation, ok := evaluations[0].(map[string]any)
	if !ok {
		t.Fatal("native capture fixture scope evaluation is not an object")
	}
	evaluation["scopes"] = []any{"scope-a", "scope-b"}
	install.Payload, err = json.Marshal(installPayload)
	if err != nil {
		t.Fatalf("encode native capture installation: %v", err)
	}
	if err := controller.Install(ctx, install); err != nil {
		t.Fatalf("install native capture contract: %v", err)
	}

	var commit, materialize scenarios.Operation
	for _, step := range scenario.Steps {
		switch scenarios.OperationKey(step.Operation) {
		case "model/commit-source-transaction":
			commit = step.Operation
		case "process/materialize-source-transaction":
			materialize = step.Operation
		}
	}
	if scenarios.OperationKey(commit) != "model/commit-source-transaction" || scenarios.OperationKey(materialize) != "process/materialize-source-transaction" {
		t.Fatal("native capture fixture source operations are absent")
	}
	if observation, err := controller.ApplyStep(ctx, commit); err != nil || observation.Disposition != "success" {
		t.Fatalf("commit native capture source row: observation=%#v err=%v", observation, err)
	}
	if observation, err := controller.ProcessStep(ctx, nil, materialize); err != nil || observation.Disposition != "success" {
		t.Fatalf("materialize native capture source row: observation=%#v err=%v", observation, err)
	}

	targetToken, err := harness.NativeBearerToken(ctx, targetUserID, time.Now())
	if err != nil {
		t.Fatalf("sign native capture target token: %v", err)
	}
	targetClient := connectRealProtocolClient(t, ctx, harness, targetToken, targetClientID, "user:"+targetUserID)
	targetTable := requireRealTable(t, targetClient, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	clientDecoy := connectRealProtocolClient(t, ctx, harness, targetToken, decoyClientID, "user:"+targetUserID)
	pushNativeCaptureMutation(t, ctx, harness, targetToken, clientDecoy, requireRealTable(t, clientDecoy, "cf_items"), ownerField, targetUserID, clientDecoyBatch, clientDecoyMutation, clientDecoyRow)

	userDecoyToken, err := harness.NativeBearerToken(ctx, decoyUserID, time.Now())
	if err != nil {
		t.Fatalf("sign native capture user decoy token: %v", err)
	}
	userDecoy := connectRealProtocolClient(t, ctx, harness, userDecoyToken, targetClientID, "user:"+decoyUserID)
	pushNativeCaptureMutation(t, ctx, harness, userDecoyToken, userDecoy, requireRealTable(t, userDecoy, "cf_items"), ownerField, decoyUserID, userDecoyBatch, userDecoyMutation, userDecoyRow)

	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", recordDecoyRow, targetUserID, "native-capture-record-decoy"); err != nil {
		t.Fatalf("insert native capture record decoy: %v", err)
	}
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", tableDecoyRow, "native-capture-table-decoy"); err != nil {
		t.Fatalf("insert native capture table decoy: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", clientDecoyRow, userDecoyRow, recordDecoyRow)
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", tableDecoyRow)

	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open native capture database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	var relationID, runtimeRecord string
	if err := database.QueryRowContext(ctx, `
		SELECT captured.relation_id::text, captured.record_id
		FROM synchro.sync_captured_rows captured
		JOIN synchro.sync_registry registry
		  ON registry.registry_generation = captured.registry_generation
		 AND registry.relation_id = captured.relation_id
		JOIN public.cf_items source ON source.id::text = captured.record_id
		WHERE registry.table_name = 'cf_items'
		  AND source.owner_id = $1
		  AND source.value = 'steady-pull'
		  AND NOT captured.deleted`, targetUserID).Scan(&relationID, &runtimeRecord); err != nil {
		t.Fatalf("read native capture runtime row identity: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO synchro.sync_bucket_edges (relation_id, table_name, record_id, bucket_id, checksum)
		VALUES ($1::uuid, 'cf_items', $2, 'cf:global', decode(repeat('00', 32), 'hex'))`, relationID, runtimeRecord); err != nil {
		t.Fatalf("insert native capture second scope edge: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		WITH outcomes(mutation_id, primary_key_value, request_ordinal) AS (
			VALUES
				($3::uuid, to_jsonb($4::text), 1),
				($5::uuid, to_jsonb($6::text), 2)
		)
		INSERT INTO synchro.sync_push_mutations (
			user_id, client_id, mutation_id,
			fingerprint_algorithm, fingerprint_version, fingerprint_domain, fingerprint_digest,
			first_batch_id, request_ordinal,
			authored_schema_version, authored_schema_hash,
			submitted_schema_version, submitted_schema_hash,
			outcome_schema_version, outcome_schema_hash,
			table_id, primary_key_field_id, primary_key_type, primary_key_value,
			operation, outcome_status, rejection_code,
			sealed_canonical_request, sealed_canonical_response, completed_at
		)
		SELECT $1, $2, mutation_id,
		       'sha256', 1, 'synchro:v3:push-mutation-fingerprint:v1', decode(repeat('00', 32), 'hex'),
		       $7::uuid, request_ordinal,
		       1, repeat('a', 64),
		       1, repeat('a', 64),
		       1, repeat('a', 64),
		       $8, $9, 'string', primary_key_value,
		       'insert', 'applied', NULL,
		       decode('00', 'hex'), decode('00', 'hex'), now()
		FROM outcomes`, targetUserID, targetClientID, targetMutationID, targetMutationRow, targetMutationID2, targetMutationRow2,
		targetBatchID, targetTable.ID, targetTable.PrimaryKeyField); err != nil {
		t.Fatalf("insert native capture target mutation outcomes: %v", err)
	}

	captures, err := controller.Capture(ctx, nil, []string{"server-state"})
	if err != nil {
		t.Fatalf("capture native server observation: %v", err)
	}
	if len(captures) != 1 || captures[0].Source != "server-state" {
		t.Fatalf("native server capture = %#v", captures)
	}
	facts, err := scenarios.NormalizeStateFacts(captures[0].StateFacts)
	if err != nil {
		t.Fatalf("normalize native server observation: %v", err)
	}
	wantOutcomes := []scenarios.MutationOutcomeIdentityFact{
		{UserID: targetUserID, ClientID: targetClientID, MutationID: targetMutationID},
		{UserID: targetUserID, ClientID: targetClientID, MutationID: targetMutationID2},
	}
	if len(facts.MutationOutcomes) != len(wantOutcomes) || facts.MutationOutcomes[0] != wantOutcomes[0] || facts.MutationOutcomes[1] != wantOutcomes[1] {
		t.Fatalf("native mutation outcome identities = %#v, want %#v", facts.MutationOutcomes, wantOutcomes)
	}
	wantEdges := []scenarios.RowScopeEdgeFact{
		{TableID: "items", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-a"},
		{TableID: "items", CanonicalWireJSON: `"row-a"`, ScopeID: "scope-b"},
	}
	if len(facts.RowScopeEdges) != len(wantEdges) || facts.RowScopeEdges[0] != wantEdges[0] || facts.RowScopeEdges[1] != wantEdges[1] {
		t.Fatalf("native row scope edges = %#v, want %#v", facts.RowScopeEdges, wantEdges)
	}
	wantRow := scenarios.RowFact{
		TableID:           "items",
		CanonicalWireJSON: `"row-a"`,
		Version:           "v1",
		Checksum:          "153c8d456b46248d141c1168cb9668aa69fdf57a1f43be6c51f4b9a89fb758b8",
	}
	if facts.RowCount == nil || *facts.RowCount != 1 || len(facts.Rows) != 1 || facts.Rows[0] != wantRow {
		t.Fatalf("native captured rows = %#v, count=%v", facts.Rows, facts.RowCount)
	}
	if facts.ScopeCount == nil || *facts.ScopeCount != 2 || len(facts.Scopes) != 2 ||
		facts.Scopes[0].ScopeID != "scope-a" || facts.Scopes[0].MembershipGeneration != 1 || facts.Scopes[0].Cardinality != 1 ||
		len(facts.Scopes[0].EffectVersions) != 1 || facts.Scopes[0].EffectVersions[0] != "v1" ||
		facts.Scopes[1].ScopeID != "scope-b" || facts.Scopes[1].MembershipGeneration != 1 || facts.Scopes[1].Cardinality != 1 ||
		len(facts.Scopes[1].EffectVersions) != 1 || facts.Scopes[1].EffectVersions[0] != "v1" {
		t.Fatalf("native captured scopes = %#v, count=%v", facts.Scopes, facts.ScopeCount)
	}
	if facts.BatchCount == nil || *facts.BatchCount != 0 || facts.MutationCount == nil || *facts.MutationCount != 2 {
		t.Fatalf("native captured push counts = batches %v, mutations %v", facts.BatchCount, facts.MutationCount)
	}
	if facts.RebuildCount == nil || *facts.RebuildCount != 0 {
		t.Fatalf("native captured rebuild count = %v", facts.RebuildCount)
	}
	requireCaptureFailure := func(want string, exact bool) {
		t.Helper()
		failedCaptures, captureErr := controller.Capture(ctx, nil, []string{"server-state"})
		matched := captureErr != nil && strings.Contains(captureErr.Error(), want)
		if exact {
			matched = captureErr != nil && captureErr.Error() == want
		}
		if !matched || failedCaptures != nil {
			t.Fatalf("native fail-closed capture = %#v, %v, want %q", failedCaptures, captureErr, want)
		}
	}
	if _, err := database.ExecContext(ctx, `
		DELETE FROM synchro.sync_bucket_edges
		WHERE table_name = 'cf_items' AND record_id = $1 AND bucket_id = 'cf:global'`, runtimeRecord); err != nil {
		t.Fatalf("remove authored native capture edge: %v", err)
	}
	requireCaptureFailure("does not match its authored binding", false)
	if _, err := database.ExecContext(ctx, `
		INSERT INTO synchro.sync_bucket_edges (relation_id, table_name, record_id, bucket_id, checksum)
		VALUES ($1::uuid, 'cf_items', $2, 'cf:global', decode(repeat('00', 32), 'hex'))`, relationID, runtimeRecord); err != nil {
		t.Fatalf("restore authored native capture edge: %v", err)
	}

	const unboundRuntimeScope = "native-capture-runtime-scope-unbound"
	if _, err := database.ExecContext(ctx, `
		INSERT INTO synchro.sync_bucket_edges (relation_id, table_name, record_id, bucket_id, checksum)
		VALUES ($1::uuid, 'cf_items', $2, $3, decode(repeat('00', 32), 'hex'))`, relationID, runtimeRecord, unboundRuntimeScope); err != nil {
		t.Fatalf("insert unbound native capture edge: %v", err)
	}
	requireCaptureFailure("no authored binding", false)
	if _, err := database.ExecContext(ctx, `
		DELETE FROM synchro.sync_bucket_edges
		WHERE table_name = 'cf_items' AND record_id = $1 AND bucket_id = $2`, runtimeRecord, unboundRuntimeScope); err != nil {
		t.Fatalf("remove unbound native capture edge: %v", err)
	}

	if _, err := database.ExecContext(ctx, `
		INSERT INTO synchro.sync_push_mutations (
			user_id, client_id, mutation_id,
			fingerprint_algorithm, fingerprint_version, fingerprint_domain, fingerprint_digest,
			first_batch_id, request_ordinal,
			authored_schema_version, authored_schema_hash,
			submitted_schema_version, submitted_schema_hash,
			outcome_schema_version, outcome_schema_hash,
			table_id, primary_key_field_id, primary_key_type, primary_key_value,
			operation, outcome_status, rejection_code,
			sealed_canonical_request, sealed_canonical_response, completed_at
		)
		SELECT $1, $2,
		       ('00000000-0000-4000-9000-' || lpad(generated.ordinal::text, 12, '0'))::uuid,
		       'sha256', 1, 'synchro:v3:push-mutation-fingerprint:v1', decode(repeat('00', 32), 'hex'),
		       '00000000-0000-4000-8000-000000000514'::uuid, generated.ordinal,
		       1, repeat('a', 64),
		       1, repeat('a', 64),
		       1, repeat('a', 64),
		       $3, $4, 'string', to_jsonb($5::text),
		       'insert', 'applied', NULL,
		       decode('00', 'hex'), decode('00', 'hex'), now()
		FROM generate_series(1, $6::integer) AS generated(ordinal)`,
		targetUserID, targetClientID, targetTable.ID, targetTable.PrimaryKeyField, targetMutationRow, observationLimit-1); err != nil {
		t.Fatalf("insert native capture mutation observation overflow: %v", err)
	}
	requireCaptureFailure("native push mutation outcome identity observation limit exceeded", true)
}

func pushNativeCaptureMutation(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient, table realProtocolTable, ownerField, ownerID, batchID, mutationID, recordID string) {
	t.Helper()
	mutation := phase4InsertMutation(client, table, ownerField, mutationID, recordID, "native-capture-mutation")
	mutation["columns"].(map[string]any)[ownerField] = ownerID
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(client, batchID, []map[string]any{mutation}))
	if status != http.StatusOK || len(requireOutcomeList(t, response, "accepted")) != 1 || len(requireOutcomeList(t, response, "rejected")) != 0 {
		t.Fatalf("native capture mutation push status = %d, response = %#v", status, response)
	}
}
