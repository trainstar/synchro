package integration

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"
)

const rowStateRereadLock = 265002

// A push that misses the source row but locks a live version rereads the source row. The
// reread must not deadlock a direct writer that locked the source row first.
func TestRealPushRowStateRereadYieldsToDirectWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	client := connectRealProtocolClient(t, ctx, harness, token, "row-state-reread-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	var detectableWait bool
	if err := admin.QueryRowContext(ctx,
		"SELECT current_setting('deadlock_timeout')::interval >= interval '200 milliseconds'",
	).Scan(&detectableWait); err != nil || !detectableWait {
		t.Fatalf("deadlock_timeout is too short to observe a reread lock wait: %v", err)
	}
	recordID := "00000000-0000-4000-8e65-000000000001"
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO public.cf_items (id, owner_id, value)
		VALUES ($1, 'diagnostic-user', 'committed')`, recordID); err != nil {
		t.Fatalf("insert committed row: %v", err)
	}
	// The first read in each push transaction misses the row. This models a writer that
	// commits the row between the source read and the version read. Each later read waits
	// for the test lock, so the test can act while the push holds the version lock.
	if _, err := admin.ExecContext(ctx, `
		CREATE FUNCTION public.cf_items_reread_gate() RETURNS boolean
		LANGUAGE plpgsql VOLATILE AS $$
		BEGIN
			IF COALESCE(current_setting('test.cf_items_reread', true), '') = '' THEN
				PERFORM set_config('test.cf_items_reread', 'first', true);
				RETURN false;
			END IF;
			PERFORM pg_advisory_xact_lock_shared(265002);
			RETURN true;
		END
		$$;
		GRANT EXECUTE ON FUNCTION public.cf_items_reread_gate() TO synchro_owner;
		DROP POLICY synchro_owner_all ON public.cf_items;
		CREATE POLICY cf_items_reread_gate ON public.cf_items
			AS PERMISSIVE FOR ALL TO synchro_owner
			USING (public.cf_items_reread_gate()) WITH CHECK (true)`); err != nil {
		t.Fatalf("install reread gate policy: %v", err)
	}

	gate, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	if _, err := gate.ExecContext(ctx, "SELECT pg_advisory_lock($1)", rowStateRereadLock); err != nil {
		t.Fatalf("hold reread gate: %v", err)
	}
	gateHeld := true
	defer func() {
		if gateHeld {
			_, _ = gate.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", rowStateRereadLock)
		}
	}()

	type pushResult struct {
		status int
		body   map[string]any
		err    error
	}
	insert := func(batchID, mutationID string) map[string]any {
		return phase4PushPayload(client, batchID, []map[string]any{
			phase4InsertMutation(client, table, ownerField, mutationID, recordID, "pushed"),
		})
	}
	pushed := make(chan pushResult, 1)
	go func() {
		status, body, err := executeSyncRequest(ctx, harness.AdapterURL(), token, "/sync/push",
			insert("00000000-0000-4000-8e65-000000000011", "00000000-0000-4000-8e65-000000000012"))
		pushed <- pushResult{status: status, body: body, err: err}
	}()
	pushPID := waitForRowStateBackend(t, ctx, admin, pushed, `
		SELECT pid FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND objid = $1 AND NOT granted`, rowStateRereadLock)

	writer, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	var writerPID int
	if err := writer.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		_, err := writer.ExecContext(ctx, "UPDATE public.cf_items SET value = 'writer' WHERE id = $1", recordID)
		written <- err
	}()
	waitForRowStateBackend(t, ctx, admin, pushed, `
		SELECT $1::int WHERE $2::int = ANY (pg_catalog.pg_blocking_pids($1::int))`,
		writerPID, pushPID)

	if _, err := gate.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", rowStateRereadLock); err != nil {
		t.Fatalf("release reread gate: %v", err)
	}
	gateHeld = false
	// A blocking reread waits on the writer for deadlock_timeout before PostgreSQL aborts
	// either transaction. Both aborts produce the same 503, so the wait itself is the signal.
	var contended pushResult
	for received := false; !received; {
		select {
		case contended = <-pushed:
			received = true
		default:
			var waited bool
			if err := admin.QueryRowContext(ctx,
				"SELECT $2::int = ANY (pg_catalog.pg_blocking_pids($1::int))",
				pushPID, writerPID).Scan(&waited); err != nil {
				t.Fatal(err)
			}
			if waited {
				t.Fatal("push reread waited on the direct writer row lock")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	failure, _ := contended.body["error"].(map[string]any)
	if contended.err != nil || contended.status != http.StatusServiceUnavailable ||
		failure["code"] != "temporary_unavailable" || failure["retryable"] != true {
		t.Fatalf("contended reread push = status %d body %#v error %v, want retryable 503",
			contended.status, contended.body, contended.err)
	}
	if err := <-written; err != nil {
		t.Fatalf("direct writer failed behind the push: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit direct writer: %v", err)
	}

	var version string
	if err := admin.QueryRowContext(ctx, `
		SELECT version.row_version::text
		FROM synchro.sync_row_versions version
		JOIN synchro.sync_registry registry ON registry.relation_id = version.relation_id
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active' AND registry.table_name = 'cf_items'
		  AND version.record_id = $1`, recordID).Scan(&version); err != nil {
		t.Fatalf("read committed row version: %v", err)
	}
	status, body := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push",
		insert("00000000-0000-4000-8e65-000000000021", "00000000-0000-4000-8e65-000000000022"))
	rejected, _ := body["rejected"].([]any)
	if status != http.StatusOK || len(rejected) != 1 {
		t.Fatalf("uncontended reread push = status %d body %#v", status, body)
	}
	outcome, _ := rejected[0].(map[string]any)
	row, _ := outcome["server_row"].(map[string]any)
	if outcome["status"] != "conflict" || outcome["code"] != "row_already_exists" ||
		outcome["server_version"] != version || row[table.ValueField] != "writer" {
		t.Fatalf("uncontended reread outcome = %#v, want row_already_exists at version %s", outcome, version)
	}
}

func waitForRowStateBackend[T any](t *testing.T, ctx context.Context, admin *sql.DB, pushed <-chan T, query string, arguments ...any) int {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		var pid int
		err := admin.QueryRowContext(ctx, query, arguments...).Scan(&pid)
		if err == nil {
			return pid
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case result := <-pushed:
			t.Fatalf("push finished before the expected lock wait: %#v", result)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatalf("backend did not reach the expected lock wait: %s", query)
	return 0
}
