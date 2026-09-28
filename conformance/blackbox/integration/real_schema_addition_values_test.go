package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestRealSchemaAdditionKeepsSourceValuesAcrossActivation proves the #200
// cutover. Registration records that the added field has no historical
// values. A later statement in the registration transaction and a later
// transaction then write the field before the worker publishes the
// generation. The recorded class must stay Class 2, and clients must receive
// both exact values.
func TestRealSchemaAdditionKeepsSourceValuesAcrossActivation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	createRealCheckpoint(t, ctx, harness)
	const (
		baselineID      = "00000000-0000-4000-a505-000000000001"
		laterID         = "00000000-0000-4000-a505-000000000002"
		registeredValue = "written after registration"
		laterValue      = "written before publication"
	)
	admin := openIssue49Admin(t, ctx, harness)
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO public.cf_items (id, owner_id, value)
		VALUES ($1, 'diagnostic-user', 'schema-addition-baseline')`, baselineID); err != nil {
		t.Fatalf("insert baseline row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", baselineID)
	var manifestsBefore int64
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_schema_manifest").Scan(&manifestsBefore); err != nil {
		t.Fatalf("count manifests before the addition: %v", err)
	}

	// The session that holds the WAL worker gate also runs the registration
	// transaction, so the gate cannot wait on its own registration.
	session, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire registration session: %v", err)
	}
	defer session.Close()
	if _, err := session.ExecContext(ctx, "SELECT pg_advisory_lock(2002873458::bigint)"); err != nil {
		t.Fatalf("acquire WAL worker gate: %v", err)
	}
	gateHeld := true
	defer func() {
		if gateHeld {
			if _, err := session.ExecContext(context.Background(), "SELECT pg_advisory_unlock(2002873458::bigint)"); err != nil {
				t.Errorf("release WAL worker gate: %v", err)
			}
		}
	}()
	transaction, err := session.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin registration transaction: %v", err)
	}
	defer transaction.Rollback()
	for step, statement := range []string{
		"ALTER TABLE public.cf_items ADD COLUMN added_value text",
		`WITH parent AS MATERIALIZED (
		     SELECT r.*
		     FROM synchro.sync_registry r
		     JOIN synchro.sync_registry_generations g ON g.generation = r.registry_generation
		     WHERE g.state = 'active' AND g.validated
		       AND r.physical_relation_oid = 'public.cf_items'::regclass
		 )
		 SELECT synchro.synchro_register_table(
		     format('%I.%I', physical_schema, physical_relation),
		     format('%I.%I', membership_function_schema, membership_function_name),
		     composition, pk_column, updated_at_col, deleted_at_col, push_policy,
		     exclude_columns, array_append(sync_columns, 'added_value'),
		     max_scope_fanout
		 )
		 FROM parent`,
	} {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			t.Fatalf("registration statement %d: %v", step+1, err)
		}
	}
	var generation int64
	var admittedRequirement sql.NullInt64
	if err := transaction.QueryRowContext(ctx, `
		SELECT generation, source_requirement
		FROM synchro.sync_registry_generations
		WHERE state = 'pending' AND validated`).Scan(&generation, &admittedRequirement); err != nil {
		t.Fatalf("observe admitted generation: %v", err)
	}
	if _, err := transaction.ExecContext(ctx,
		"UPDATE public.cf_items SET added_value = $2 WHERE id = $1", baselineID, registeredValue); err != nil {
		t.Fatalf("write the added field after registration: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit registration transaction: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO public.cf_items (id, owner_id, value, added_value)
		VALUES ($1, 'diagnostic-user', 'schema-addition-later', $2)`, laterID, laterValue); err != nil {
		t.Fatalf("write the added field before publication: %v", err)
	}
	if _, err := session.ExecContext(ctx, "SELECT pg_advisory_unlock(2002873458::bigint)"); err != nil {
		t.Fatalf("release WAL worker gate: %v", err)
	}
	gateHeld = false

	type capturedValue struct {
		generation int64
		value      sql.NullString
	}
	captured := func(recordID string) capturedValue {
		t.Helper()
		var observed capturedValue
		err := admin.QueryRowContext(ctx, `
			SELECT captured.registry_generation, captured.row_data ->> field.field_id::text
			FROM synchro.sync_captured_rows captured
			JOIN synchro.sync_registry_fields field
			  ON field.registry_generation = captured.registry_generation
			 AND field.relation_id = captured.relation_id
			 AND field.physical_column = 'added_value'
			WHERE captured.record_id = $1 AND NOT captured.deleted`, recordID).Scan(&observed.generation, &observed.value)
		if err != nil && err != sql.ErrNoRows {
			t.Fatalf("observe captured added field: %v", err)
		}
		return observed
	}
	deadline := time.Now().Add(30 * time.Second)
	for captured(laterID).generation != generation && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	var state string
	var recordedRequirement sql.NullInt64
	var transitionClass string
	var manifestsAfter int64
	if err := admin.QueryRowContext(ctx, `
		SELECT generation.state, generation.source_requirement,
		       (SELECT transition_class FROM synchro.sync_schema_manifest
		        ORDER BY schema_version DESC LIMIT 1),
		       (SELECT count(*) FROM synchro.sync_schema_manifest)
		FROM synchro.sync_registry_generations generation
		WHERE generation.generation = $1`, generation).Scan(
		&state, &recordedRequirement, &transitionClass, &manifestsAfter); err != nil {
		t.Fatalf("observe activated generation: %v", err)
	}
	baseline := captured(baselineID)
	later := captured(laterID)

	t.Run("assertion", func(t *testing.T) {
		if !admittedRequirement.Valid || admittedRequirement.Int64 != 0 ||
			recordedRequirement != admittedRequirement || state != "active" ||
			transitionClass != "class_2" || manifestsAfter != manifestsBefore+1 {
			t.Fatalf("recorded classification changed: admitted=%v recorded=%v state=%q class=%q manifests=%d/%d",
				admittedRequirement, recordedRequirement, state, transitionClass, manifestsBefore, manifestsAfter)
		}
		for _, check := range []struct {
			observed capturedValue
			value    string
		}{{baseline, registeredValue}, {later, laterValue}} {
			if check.observed.generation != generation || !check.observed.value.Valid || check.observed.value.String != check.value {
				t.Fatalf("captured added field = generation %d value %v, want generation %d value %q",
					check.observed.generation, check.observed.value, generation, check.value)
			}
		}

		var fieldID string
		if err := admin.QueryRowContext(ctx, `
			SELECT field.field_id::text
			FROM synchro.sync_registry_fields field
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = field.registry_generation
			 AND registry.relation_id = field.relation_id
			WHERE field.registry_generation = $1
			  AND registry.table_name = 'cf_items'
			  AND field.physical_column = 'added_value'`, generation).Scan(&fieldID); err != nil {
			t.Fatalf("observe added field identity: %v", err)
		}
		client := connectRealProtocolClient(t, ctx, harness, token, "schema-addition-values")
		records, _ := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-a505-000000000011")
		table := requireRealTable(t, client, "cf_items")
		want := map[string]string{baselineID: registeredValue, laterID: laterValue}
		for _, record := range records {
			pk, _ := record["pk"].(map[string]any)
			recordID, _ := pk[table.PrimaryKeyField].(string)
			expected, ok := want[recordID]
			row, _ := record["row"].(map[string]any)
			if record["table"] != table.ID || !ok {
				continue
			}
			if row[fieldID] != expected {
				t.Fatalf("client received added field %v for %s, want %q", row[fieldID], recordID, expected)
			}
			delete(want, recordID)
		}
		if len(want) != 0 {
			t.Fatalf("client rebuild omitted rows: %v", want)
		}
	})
}
