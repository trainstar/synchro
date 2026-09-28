package integration

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

// TestRealRegistrationsCommittedBeforeActivationActivateInOrder proves that
// two membership rule transitions from separate transactions activate when both
// commit before the worker activates the first one. Separate application
// migrations can commit in that order.
func TestRealRegistrationsCommittedBeforeActivationActivateInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		t.Fatalf("create native controller: %v", err)
	}

	// Empty relations change no scope, so each rule transition declares one authoritative scope.
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
		SELECT scope_id, runtime.stream_generation
		FROM synchro.sync_runtime_state runtime
		CROSS JOIN (VALUES ('user:ordering-first'), ('user:ordering-second')) AS scopes(scope_id)
		WHERE runtime.singleton`,
	); err != nil {
		t.Fatalf("create transition scopes: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE FUNCTION public.cf_items_ordering_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT 'user:ordering-' || (p.owner_id #>> '{}')
			FROM synchro_projection.cf_items AS p
			WHERE p.record_id = p_id::text AND NOT p.deleted;
		END;
		CREATE FUNCTION public.cf_document_notes_ordering_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT 'user:ordering-' || (n.author_id #>> '{}')
			FROM synchro_projection.cf_document_notes AS n
			WHERE n.record_id = p_id::text AND NOT n.deleted;
		END;
		REVOKE ALL ON FUNCTION public.cf_items_ordering_membership(uuid), public.cf_document_notes_ordering_membership(uuid) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.cf_items_ordering_membership(uuid), public.cf_document_notes_ordering_membership(uuid)
			TO synchro_owner, synchro_worker`,
	); err != nil {
		t.Fatalf("create ordering membership functions: %v", err)
	}

	resume, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		t.Fatalf("pause WAL materialization: %v", err)
	}
	paused := true
	defer func() {
		if paused {
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := resume(cleanupContext); err != nil {
				t.Errorf("resume WAL materialization: %v", err)
			}
		}
	}()
	register := func(executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}, table, function, scope string) {
		t.Helper()
		if _, err := executor.ExecContext(ctx, `
			SELECT synchro.synchro_register_table(
				$1, $2, 'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled',
				p_affected_scopes => ARRAY[$3]::text[]
			)`, table, function, scope,
		); err != nil {
			t.Fatalf("register %s before activation: %v", table, err)
		}
	}
	register(admin, "public.cf_items", "public.cf_items_ordering_membership", "user:ordering-first")
	// The second migration is still open when the worker starts to activate the first one.
	second, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second registration: %v", err)
	}
	defer second.Rollback()
	var secondPID int
	if err := second.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&secondPID); err != nil {
		t.Fatalf("observe second registration backend: %v", err)
	}
	register(second, "public.cf_document_notes", "public.cf_document_notes_ordering_membership", "user:ordering-second")
	if err := resume(ctx); err != nil {
		t.Fatalf("resume WAL materialization: %v", err)
	}
	paused = false
	// When the worker waits for the open migration, the first activation is in progress.
	workerWaited := false
	deadline := time.Now().Add(30 * time.Second)
	for !workerWaited && time.Now().Before(deadline) {
		if err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_stat_activity worker
				WHERE worker.backend_type = 'synchro WAL consumer'
				  AND $1 = ANY(pg_catalog.pg_blocking_pids(worker.pid))
			)`, secondPID,
		).Scan(&workerWaited); err != nil {
			t.Fatalf("observe worker wait: %v", err)
		}
		if !workerWaited {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("commit second registration: %v", err)
	}
	t.Logf("worker waited for the open second registration: %t", workerWaited)

	type registryState struct {
		pending, poison              int
		itemsFunction, notesFunction string
	}
	var state registryState
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if err := admin.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending'),
			       (SELECT count(*) FROM synchro.sync_wal_poison WHERE lifecycle = 'active'),
			       COALESCE((SELECT registry.membership_function_name::text
			                 FROM synchro.sync_registry registry
			                 JOIN synchro.sync_registry_generations generation
			                   ON generation.generation = registry.registry_generation AND generation.state = 'active'
			                 WHERE registry.physical_relation = 'cf_items'), ''),
			       COALESCE((SELECT registry.membership_function_name::text
			                 FROM synchro.sync_registry registry
			                 JOIN synchro.sync_registry_generations generation
			                   ON generation.generation = registry.registry_generation AND generation.state = 'active'
			                 WHERE registry.physical_relation = 'cf_document_notes'), '')`,
		).Scan(&state.pending, &state.poison, &state.itemsFunction, &state.notesFunction); err != nil {
			t.Fatalf("observe registry activation: %v", err)
		}
		if state.pending == 0 || state.poison != 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Each rule transition activates in the generation that its own transaction created.
	var stageScopes string
	if err := admin.QueryRowContext(ctx, `
		SELECT COALESCE(string_agg(stage.state || ':' || array_to_string(stage.affected_scopes, ','), ' '
		                           ORDER BY stage.registry_generation), '')
		FROM synchro.sync_registry_membership_stages stage
		WHERE EXISTS (
			SELECT 1 FROM unnest(stage.affected_scopes) AS scope(scope_id)
			WHERE scope_id LIKE 'user:ordering-%'
		)`,
	).Scan(&stageScopes); err != nil {
		t.Fatalf("observe membership stages: %v", err)
	}

	itemID := "00000000-0000-4000-8242-000000000001"
	documentID := "00000000-0000-4000-8242-000000000002"
	noteID := "00000000-0000-4000-8242-000000000003"
	t.Run("assertion", func(t *testing.T) {
		want := registryState{itemsFunction: "cf_items_ordering_membership", notesFunction: "cf_document_notes_ordering_membership"}
		if state != want {
			t.Fatalf("registrations committed before activation did not both activate: got %#v, want %#v; %s", state, want, harness.FailureDiagnostics())
		}
		if want := "activated:user:ordering-first activated:user:ordering-second"; stageScopes != want {
			t.Fatalf("membership stages = %q, want %q", stageScopes, want)
		}
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'item-owner', 'ordering')", []any{itemID}},
			{"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'document-owner', 'ordering')", []any{documentID}},
			{"INSERT INTO cf_document_notes (id, document_id, author_id, body) VALUES ($1, $2, 'note-author', 'ordering')", []any{noteID, documentID}},
		} {
			if err := harness.Source().ExecContext(ctx, statement.sql, statement.args...); err != nil {
				t.Fatalf("write after ordered activation: %v", err)
			}
		}
		for _, expected := range []struct{ table, recordID, bucket string }{
			{"cf_items", itemID, "user:ordering-item-owner"},
			{"cf_document_notes", noteID, "user:ordering-note-author"},
		} {
			var buckets []string
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				buckets, err = harness.Operator().ObserveMembershipBuckets(ctx, expected.table, expected.recordID)
				if err != nil {
					t.Fatalf("observe %s membership: %v", expected.table, err)
				}
				if slices.Equal(buckets, []string{expected.bucket}) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if !slices.Equal(buckets, []string{expected.bucket}) {
				t.Fatalf("%s membership = %v, want [%s]; %s", expected.table, buckets, expected.bucket, harness.FailureDiagnostics())
			}
		}
	})
}

// TestRealRegistrationDuringInitialSlotBindingActivatesOnce proves that a
// registration that commits after the replacement slot starts, and before the
// worker binds that slot, activates exactly once. Application migrations can
// run while a new worker creates and binds its first slot.
func TestRealRegistrationDuringInitialSlotBindingActivatesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	reinstall, err := harness.ReinstallExtension(ctx)
	if err != nil {
		t.Fatalf("reinstall extension: %v", err)
	}
	// The worker binds its new slot with a row lock on the progress row. This
	// table lock holds the worker between slot creation and binding, and it has
	// no transaction ID, so slot creation does not wait for it.
	binding, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin binding hold: %v", err)
	}
	defer binding.Rollback()
	var holderPID int
	var unbound bool
	if err := binding.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("observe binding hold backend: %v", err)
	}
	if _, err := binding.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_progress IN EXCLUSIVE MODE"); err != nil {
		t.Fatalf("hold worker slot binding: %v", err)
	}
	if err := binding.QueryRowContext(ctx,
		"SELECT active_slot_name IS NULL FROM synchro.sync_runtime_state WHERE singleton",
	).Scan(&unbound); err != nil || !unbound {
		t.Fatalf("replacement worker bound its slot before the binding hold: unbound=%t err=%v", unbound, err)
	}
	workerHeld := false
	deadline := time.Now().Add(60 * time.Second)
	for !workerHeld && time.Now().Before(deadline) {
		if err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)
			   AND EXISTS (
				SELECT 1 FROM pg_catalog.pg_stat_activity worker
				WHERE worker.backend_type = 'synchro WAL consumer'
				  AND $2 = ANY(pg_catalog.pg_blocking_pids(worker.pid))
			)`, harness.Names().ReplicationSlot, holderPID,
		).Scan(&workerHeld); err != nil {
			t.Fatalf("observe held slot binding: %v", err)
		}
		if !workerHeld {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !workerHeld {
		t.Fatalf("replacement worker did not create its slot and wait to bind it; %s", harness.FailureDiagnostics())
	}
	if err := harness.RestoreDiagnosticRegistrations(ctx); err != nil {
		t.Fatalf("register while the slot binding is held: %v", err)
	}
	var registered []int64
	rows, err := admin.QueryContext(ctx, "SELECT generation FROM synchro.sync_registry_generations WHERE state = 'pending' ORDER BY generation")
	if err != nil {
		t.Fatalf("observe committed registrations: %v", err)
	}
	for rows.Next() {
		var generation int64
		if err := rows.Scan(&generation); err != nil {
			t.Fatalf("read committed registration: %v", err)
		}
		registered = append(registered, generation)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil || len(registered) == 0 {
		t.Fatalf("committed registrations = %v, err %v", registered, err)
	}
	if err := binding.Commit(); err != nil {
		t.Fatalf("release worker slot binding: %v", err)
	}

	var pending int
	var poison string
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if err := admin.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending'),
			       COALESCE((SELECT string_agg(failure_class || ': ' || COALESCE(failure_detail, ''), '; ')
			                 FROM synchro.sync_wal_poison WHERE lifecycle = 'active'), '')`,
		).Scan(&pending, &poison); err != nil {
			t.Fatalf("observe registry activation: %v", err)
		}
		if pending == 0 || poison != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var activated int
	var active int64
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE state IN ('active', 'superseded') AND activation_commit_lsn IS NOT NULL),
		       COALESCE(max(generation) FILTER (WHERE state = 'active'), 0)
		FROM synchro.sync_registry_generations
		WHERE generation = ANY($1)`, registered,
	).Scan(&activated, &active); err != nil {
		t.Fatalf("observe activated registrations: %v", err)
	}

	itemID := "00000000-0000-4000-8243-000000000001"
	t.Run("assertion", func(t *testing.T) {
		if poison != "" || pending != 0 || activated != len(registered) || active != registered[len(registered)-1] {
			t.Fatalf("registration during slot binding: poison %q pending %d activated %d of %v active %d; %s",
				poison, pending, activated, registered, active, harness.FailureDiagnostics())
		}
		waitForReinstalledWorker(t, ctx, harness, reinstall, registered[0]-1)
		if err := harness.Source().ExecContext(ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'after-binding')", itemID,
		); err != nil {
			t.Fatalf("write after slot binding: %v", err)
		}
		var buckets []string
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if buckets, err = harness.Operator().ObserveMembershipBuckets(ctx, "cf_items", itemID); err != nil {
				t.Fatalf("observe cf_items membership: %v", err)
			}
			if slices.Equal(buckets, []string{"user:diagnostic-user"}) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !slices.Equal(buckets, []string{"user:diagnostic-user"}) {
			t.Fatalf("cf_items membership = %v, want [user:diagnostic-user]; %s", buckets, harness.FailureDiagnostics())
		}
	})
}
