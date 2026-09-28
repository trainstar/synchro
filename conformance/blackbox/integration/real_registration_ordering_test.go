package integration

import (
	"context"
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
	for _, registration := range []struct{ table, function, scope string }{
		{"public.cf_items", "public.cf_items_ordering_membership", "user:ordering-first"},
		{"public.cf_document_notes", "public.cf_document_notes_ordering_membership", "user:ordering-second"},
	} {
		if _, err := admin.ExecContext(ctx, `
			SELECT synchro.synchro_register_table(
				$1, $2, 'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled',
				p_affected_scopes => ARRAY[$3]::text[]
			)`, registration.table, registration.function, registration.scope,
		); err != nil {
			t.Fatalf("register %s before activation: %v", registration.table, err)
		}
	}
	var pendingWhilePaused int
	if err := admin.QueryRowContext(ctx,
		"SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending'",
	).Scan(&pendingWhilePaused); err != nil {
		t.Fatalf("observe pending registrations: %v", err)
	}
	if pendingWhilePaused < 2 {
		t.Fatalf("both registrations must commit before activation: pending generations = %d", pendingWhilePaused)
	}
	if err := resume(ctx); err != nil {
		t.Fatalf("resume WAL materialization: %v", err)
	}
	paused = false

	type registryState struct {
		pending, poison              int
		itemsFunction, notesFunction string
	}
	var state registryState
	deadline := time.Now().Add(60 * time.Second)
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

	itemID := "00000000-0000-4000-8242-000000000001"
	documentID := "00000000-0000-4000-8242-000000000002"
	noteID := "00000000-0000-4000-8242-000000000003"
	t.Run("assertion", func(t *testing.T) {
		want := registryState{itemsFunction: "cf_items_ordering_membership", notesFunction: "cf_document_notes_ordering_membership"}
		if state != want {
			t.Fatalf("registrations committed before activation did not both activate: got %#v, want %#v; %s", state, want, harness.FailureDiagnostics())
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
