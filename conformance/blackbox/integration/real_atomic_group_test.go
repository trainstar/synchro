package integration

import (
	"context"
	"database/sql"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/vectors"
)

const realAtomicClientVersion = "2032-01-02T03:04:05.000000Z"

type realAtomicMutation struct {
	id          string
	table       realProtocolTable
	key         any
	op          string
	baseVersion string
	value       string
}

type realAtomicPush struct {
	status   int
	response map[string]any
	state    string
	changes  int64
}

// TestRealAtomicGroupAppliesAllOrNone proves SYNC-ATOMICITY-002 for
// SCN-ATOMIC-GROUP-001. Two failed atomic groups leave committed state and
// WAL unchanged and report the committed conflict row. A later group applies
// every mutation.
func TestRealAtomicGroupAppliesAllOrNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	parentField := installRealAtomicGroupTables(t, ctx, harness, admin)
	client := connectRealProtocolClient(t, ctx, harness, token, "atomic-group-client")
	parents := requireRealTable(t, client, "cf_atomic_parents")
	children := requireRealTable(t, client, "cf_atomic_children")
	manifest := loadMutationControlManifest(t, ctx, harness)
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO public.cf_atomic_parents (id, value) VALUES ('1', 'parent');
		INSERT INTO public.cf_atomic_children (id, parent_id, value) VALUES (1, '1', 'child')`); err != nil {
		t.Fatalf("commit atomic group source rows: %v", err)
	}
	parentVersion := loadReleaseRowVersion(t, ctx, admin, "cf_atomic_parents", "1")
	childVersion := loadReleaseRowVersion(t, ctx, admin, "cf_atomic_children", "1")
	committedParent := map[string]any{parents.PrimaryKeyField: "1", parents.ValueField: "parent"}
	committedChild := map[string]any{children.PrimaryKeyField: float64(1), parentField: "1", children.ValueField: "child"}
	failedMutationIDs := []string{
		"00000000-0000-4000-8000-00000000a011",
		"00000000-0000-4000-8000-00000000a012",
		"00000000-0000-4000-8000-00000000a013",
		"00000000-0000-4000-8000-00000000a021",
		"00000000-0000-4000-8000-00000000a022",
	}

	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		t.Fatalf("create atomic group WAL controller: %v", err)
	}
	resume, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		t.Fatalf("pause atomic group WAL materialization: %v", err)
	}
	defer func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelCleanup()
		if err := resume(cleanup); err != nil {
			t.Errorf("resume atomic group WAL materialization: %v", err)
		}
	}()
	before := observeRealAtomicGroupState(t, ctx, admin, client.ID, failedMutationIDs)
	rollbackGroup := []realAtomicMutation{
		{id: failedMutationIDs[0], table: parents, key: "row-b", op: "insert", value: "rollback-insert"},
		{id: failedMutationIDs[1], table: children, key: float64(1), op: "update", baseVersion: childVersion, value: "rollback-update"},
		{id: failedMutationIDs[2], table: parents, key: "1", op: "insert", value: "rollback-conflict"},
	}
	rollback := pushRealAtomicGroup(t, ctx, harness, admin, token, client, "00000000-0000-4000-8000-00000000a010", rollbackGroup)
	rollback.state = observeRealAtomicGroupState(t, ctx, admin, client.ID, failedMutationIDs)
	cascadeGroup := []realAtomicMutation{
		{id: failedMutationIDs[3], table: parents, key: "1", op: "delete", baseVersion: parentVersion},
		{id: failedMutationIDs[4], table: children, key: float64(1), op: "update", baseVersion: childVersion, value: "cascade-update"},
	}
	cascade := pushRealAtomicGroup(t, ctx, harness, admin, token, client, "00000000-0000-4000-8000-00000000a020", cascadeGroup)
	cascade.state = observeRealAtomicGroupState(t, ctx, admin, client.ID, failedMutationIDs)
	appliedGroup := []realAtomicMutation{
		{id: "00000000-0000-4000-8000-00000000a031", table: parents, key: "row-b", op: "insert", value: "applied-insert"},
		{id: "00000000-0000-4000-8000-00000000a032", table: children, key: float64(1), op: "update", baseVersion: childVersion, value: "applied-update"},
	}
	applied := pushRealAtomicGroup(t, ctx, harness, admin, token, client, "00000000-0000-4000-8000-00000000a030", appliedGroup)

	t.Run("assertion", func(t *testing.T) {
		requireRealFailedAtomicGroup(t, manifest, client, rollback, before, rollbackGroup, 2, "row_already_exists", committedParent, parentVersion)
	})
	t.Run("assertion", func(t *testing.T) {
		requireRealFailedAtomicGroup(t, manifest, client, cascade, before, cascadeGroup, 1, "row_deleted", committedChild, childVersion)
	})
	t.Run("assertion", func(t *testing.T) {
		if applied.status != http.StatusOK {
			t.Fatalf("applied atomic group status = %d, want 200: %#v", applied.status, applied.response)
		}
		if rejected := requireOutcomeList(t, applied.response, "rejected"); len(rejected) != 0 {
			t.Fatalf("applied atomic group rejected mutations: %#v", rejected)
		}
		accepted := requireOutcomeList(t, applied.response, "accepted")
		wantRows := []map[string]any{
			{parents.PrimaryKeyField: "row-b", parents.ValueField: "applied-insert"},
			{children.PrimaryKeyField: float64(1), parentField: "1", children.ValueField: "applied-update"},
		}
		if len(accepted) != len(appliedGroup) {
			t.Fatalf("applied atomic group accepted %d mutations, want %d: %#v", len(accepted), len(appliedGroup), accepted)
		}
		for index, outcome := range accepted {
			if outcome["mutation_id"] != appliedGroup[index].id || outcome["status"] != "applied" {
				t.Fatalf("applied atomic group outcome %d is invalid: %#v", index, outcome)
			}
			requireRealAtomicOutcomeRow(t, manifest, appliedGroup[index].table, outcome, wantRows[index])
		}
		if version := accepted[1]["server_version"]; version == childVersion {
			t.Fatalf("applied atomic group kept the committed child version: %#v", accepted[1])
		}
		if applied.changes != int64(len(appliedGroup)) {
			t.Fatalf("applied atomic group decoded %d WAL changes, want %d", applied.changes, len(appliedGroup))
		}
	})
}

// installRealAtomicGroupTables registers a parent table and a child table
// whose rows the parent deletes by cascade. It returns the child parent_id
// field identifier.
func installRealAtomicGroupTables(t *testing.T, ctx context.Context, harness *blackbox.Harness, admin *sql.DB) string {
	t.Helper()
	if _, err := admin.ExecContext(ctx, `
		CREATE TABLE public.cf_atomic_parents (
			id text PRIMARY KEY,
			value text NOT NULL
		);
		CREATE TABLE public.cf_atomic_children (
			id integer PRIMARY KEY,
			parent_id text NOT NULL REFERENCES public.cf_atomic_parents (id) ON DELETE CASCADE,
			value text NOT NULL
		);
		GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.cf_atomic_parents, public.cf_atomic_children TO synchro_owner;
		GRANT SELECT ON TABLE public.cf_atomic_parents, public.cf_atomic_children TO synchro_worker;
		ALTER TABLE public.cf_atomic_parents ENABLE ROW LEVEL SECURITY;
		ALTER TABLE public.cf_atomic_children ENABLE ROW LEVEL SECURITY;
		CREATE POLICY synchro_owner_all ON public.cf_atomic_parents
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE POLICY synchro_owner_all ON public.cf_atomic_children
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE FUNCTION public.cf_atomic_parents_membership(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END;
		CREATE FUNCTION public.cf_atomic_children_membership(p_id integer)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END;
		REVOKE ALL ON FUNCTION public.cf_atomic_parents_membership(text), public.cf_atomic_children_membership(integer) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.cf_atomic_parents_membership(text), public.cf_atomic_children_membership(integer)
			TO synchro_owner, synchro_worker;
		SELECT synchro.synchro_register_table(
			'public.cf_atomic_parents', 'public.cf_atomic_parents_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled');
		SELECT synchro.synchro_register_table(
			'public.cf_atomic_children', 'public.cf_atomic_children_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`); err != nil {
		t.Fatalf("register atomic group tables: %v", err)
	}
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, lastErr = fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_atomic_parents")
		if lastErr == nil {
			var child realSchemaTableReference
			child, lastErr = fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_atomic_children")
			if lastErr == nil {
				return requireRealSchemaField(t, child, "parent_id")
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("atomic group tables did not activate: %v; %s", lastErr, harness.FailureDiagnostics())
	return ""
}

// pushRealAtomicGroup submits one atomic group. It records the response and
// the decoded WAL change count of the push transaction without asserting them.
func pushRealAtomicGroup(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	admin *sql.DB,
	token string,
	client *realProtocolClient,
	batchID string,
	group []realAtomicMutation,
) realAtomicPush {
	t.Helper()
	mutations := make([]map[string]any, 0, len(group))
	for _, member := range group {
		mutation := map[string]any{
			"mutation_id":     member.id,
			"table":           member.table.ID,
			"pk":              map[string]any{member.table.PrimaryKeyField: member.key},
			"authored_schema": client.Schema,
			"op":              member.op,
			"client_version":  realAtomicClientVersion,
		}
		if member.baseVersion != "" {
			mutation["base_version"] = member.baseVersion
		}
		if member.op != "delete" {
			mutation["columns"] = map[string]any{member.table.ValueField: member.value}
		}
		mutations = append(mutations, mutation)
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"batch_id":          batchID,
		"schema":            client.Schema,
		"atomic":            true,
		"mutations":         mutations,
	})
	var changes int64
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_logical_slot_peek_binary_changes(
			(SELECT active_slot_name FROM synchro.sync_runtime_state WHERE singleton),
			NULL, 1000, 'proto_version', '1',
			'publication_names', current_setting('synchro.publication_name')
		)
		WHERE xid = (SELECT xmin FROM synchro.sync_push_batches WHERE batch_id = $1::uuid)
		  AND get_byte(data, 0) IN (68, 73, 84, 85)`, batchID,
	).Scan(&changes); err != nil {
		t.Fatalf("observe atomic group WAL changes: %v", err)
	}
	return realAtomicPush{status: status, response: response, changes: changes}
}

// observeRealAtomicGroupState returns the source rows, row versions, group
// fences, and accepted-write epoch as canonical JSON text.
func observeRealAtomicGroupState(t *testing.T, ctx context.Context, admin *sql.DB, clientID string, mutationIDs []string) string {
	t.Helper()
	var state string
	if err := admin.QueryRowContext(ctx, `
		SELECT jsonb_build_object(
			'parents', (SELECT jsonb_agg(to_jsonb(parent) ORDER BY parent.id) FROM public.cf_atomic_parents parent),
			'children', (SELECT jsonb_agg(to_jsonb(child) ORDER BY child.id) FROM public.cf_atomic_children child),
			'versions', (
				SELECT jsonb_agg(jsonb_build_object(
					'table', registry.table_name,
					'record', version.record_id,
					'version', version.row_version,
					'deleted', version.deleted
				) ORDER BY registry.table_name, version.record_id)
				FROM synchro.sync_row_versions version
				JOIN synchro.sync_registry registry ON registry.relation_id = version.relation_id
				JOIN synchro.sync_registry_generations generation
				  ON generation.generation = registry.registry_generation
				WHERE generation.state = 'active'
				  AND registry.table_name IN ('cf_atomic_parents', 'cf_atomic_children')
			),
			'fences', (SELECT count(*) FROM synchro.sync_write_fences WHERE mutation_id = ANY($2::text[])),
			'epoch', (
				SELECT accepted_write_epoch FROM synchro.sync_clients
				WHERE user_id = 'diagnostic-user' AND client_id = $1
			)
		)::text`,
		clientID, mutationIDs,
	).Scan(&state); err != nil {
		t.Fatalf("observe atomic group state: %v", err)
	}
	return state
}

// requireRealFailedAtomicGroup asserts the failed-group outcome partition, the
// committed conflict row, and unchanged durable state and WAL.
func requireRealFailedAtomicGroup(
	t *testing.T,
	manifest vectors.Manifest,
	client *realProtocolClient,
	push realAtomicPush,
	before string,
	group []realAtomicMutation,
	failingIndex int,
	failingCode string,
	committedRow map[string]any,
	committedVersion string,
) {
	t.Helper()
	if push.status != http.StatusOK {
		t.Fatalf("failed atomic group status = %d, want 200: %#v", push.status, push.response)
	}
	if accepted := requireOutcomeList(t, push.response, "accepted"); len(accepted) != 0 {
		t.Fatalf("failed atomic group accepted mutations: %#v", accepted)
	}
	rejected := requireOutcomeList(t, push.response, "rejected")
	if len(rejected) != len(group) {
		t.Fatalf("failed atomic group rejected %d mutations, want %d: %#v", len(rejected), len(group), rejected)
	}
	outcomeSchema := map[string]any{"version": float64(client.Schema["version"].(int64)), "hash": client.Schema["hash"]}
	for index, outcome := range rejected {
		member := group[index]
		pk := map[string]any{member.table.PrimaryKeyField: member.key}
		if index == failingIndex {
			if outcome["mutation_id"] != member.id || outcome["table"] != member.table.ID || !reflect.DeepEqual(outcome["pk"], pk) ||
				outcome["status"] != "conflict" || outcome["code"] != failingCode {
				t.Fatalf("failing atomic group outcome is invalid: %#v", outcome)
			}
			if outcome["server_version"] != committedVersion {
				t.Fatalf("failing atomic group outcome version = %#v, want committed %s", outcome["server_version"], committedVersion)
			}
			requireRealAtomicOutcomeRow(t, manifest, member.table, outcome, committedRow)
			continue
		}
		want := map[string]any{
			"mutation_id":    member.id,
			"table":          member.table.ID,
			"pk":             pk,
			"outcome_schema": outcomeSchema,
			"status":         "rejected_terminal",
			"code":           "atomic_batch_rejected",
			"message":        "atomic batch rejected",
		}
		if !reflect.DeepEqual(outcome, want) {
			t.Fatalf("atomic group outcome %d = %#v, want %#v", index, outcome, want)
		}
	}
	if push.state != before {
		t.Fatalf("failed atomic group changed durable state:\nbefore %s\nafter  %s", before, push.state)
	}
	if push.changes != 0 {
		t.Fatalf("failed atomic group decoded %d WAL changes, want 0", push.changes)
	}
}

// requireRealAtomicOutcomeRow asserts that an outcome carries the complete
// expected row and a checksum computed independently from that row.
func requireRealAtomicOutcomeRow(t *testing.T, manifest vectors.Manifest, table realProtocolTable, outcome map[string]any, wantRow map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(outcome["server_row"], wantRow) {
		t.Fatalf("atomic group outcome row = %#v, want %#v", outcome["server_row"], wantRow)
	}
	expected, _, err := independentlyComputeMutationControlDigests(manifest, "user:diagnostic-user", table, map[string]any{
		"pk":             outcome["pk"],
		"row":            outcome["server_row"],
		"server_version": outcome["server_version"],
	})
	if err != nil {
		t.Fatalf("compute atomic group row digest: %v", err)
	}
	if actual, ok := mutationControlChecksumDigest(outcome["row_checksum"]); !ok || actual != expected {
		t.Fatalf("atomic group outcome checksum = %#v, want digest %s", outcome["row_checksum"], expected)
	}
}
