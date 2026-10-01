package integration

import (
	"context"
	"database/sql"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

// TestRealScopeAssignmentPullReconciliation proves SYNC-SCOPE-007 for
// SCN-SCOPE-ASSIGNMENT-PULL-001. An application row change after connect
// changes the assignment function result, and the next pull reports it.
func TestRealScopeAssignmentPullReconciliation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	database := registerRealScopeAssignmentFunction(t, ctx, harness, 8)
	client := connectRealProtocolClient(t, ctx, harness, token, "scope-assignment-pull-client")
	const assignedScope = "cf:assigned-b"

	t.Run("assertion", func(t *testing.T) {
		setRealAssignedScopes(t, ctx, database, assignedScope)
		status, added := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, client.Scopes, 1))
		if status != http.StatusOK {
			t.Fatalf("assignment add pull status = %d, want 200: %#v", status, added)
		}
		if added["scope_set_version"] != float64(client.ScopeSetVersion+1) {
			t.Fatalf("assignment add did not advance scope_set_version exactly once: %#v", added)
		}
		wantAdd := map[string]any{
			"add":    []any{map[string]any{"id": assignedScope, "cursor": nil}},
			"remove": []any{},
		}
		if !reflect.DeepEqual(added["scope_updates"], wantAdd) {
			t.Fatalf("assignment add scope_updates = %#v, want %#v", added["scope_updates"], wantAdd)
		}
		rebuild, ok := added["rebuild"].([]any)
		if !ok || !slices.Contains(rebuild, any(assignedScope)) {
			t.Fatalf("assignment add omitted the added scope from rebuild: %#v", added)
		}

		setRealAssignedScopes(t, ctx, database)
		presented := map[string]any{assignedScope: map[string]any{"cursor": nil}}
		for scopeID, cursor := range client.Scopes {
			presented[scopeID] = cursor
		}
		payload := realPullPayload(client, presented, 1)
		payload["scope_set_version"] = client.ScopeSetVersion + 1
		status, removed := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", payload)
		if status != http.StatusOK {
			t.Fatalf("assignment remove pull status = %d, want 200: %#v", status, removed)
		}
		if removed["scope_set_version"] != float64(client.ScopeSetVersion+2) {
			t.Fatalf("assignment remove did not advance scope_set_version exactly once: %#v", removed)
		}
		wantRemove := map[string]any{"add": []any{}, "remove": []any{assignedScope}}
		if !reflect.DeepEqual(removed["scope_updates"], wantRemove) {
			t.Fatalf("assignment remove scope_updates = %#v, want %#v", removed["scope_updates"], wantRemove)
		}
	})
}

// TestRealScopeAssignmentBoundFailsClosed proves SYNC-SCOPE-008 for
// SCN-SCOPE-ASSIGNMENT-BOUND-001. A result above max_scopes fails the pull
// and changes no durable assignment state.
func TestRealScopeAssignmentBoundFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	database := registerRealScopeAssignmentFunction(t, ctx, harness, 2)
	client := connectRealProtocolClient(t, ctx, harness, token, "scope-assignment-bound-client")

	t.Run("assertion", func(t *testing.T) {
		setRealAssignedScopes(t, ctx, database, "cf:assigned-b", "cf:assigned-c", "cf:assigned-d")
		before := observeRealAssignmentState(t, ctx, database, client.ID)
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, client.Scopes, 1))
		requireRealProtocolError(t, status, response, http.StatusInternalServerError, "sync_integrity_failure")
		if after := observeRealAssignmentState(t, ctx, database, client.ID); after != before {
			t.Fatalf("assignment bound failure changed durable state:\nbefore %s\nafter  %s", before, after)
		}
	})
}

// registerRealScopeAssignmentFunction creates an application table and an
// assignment function that reads it, then registers the function. The
// function returns no scope until a test inserts rows.
func registerRealScopeAssignmentFunction(t *testing.T, ctx context.Context, harness *blackbox.Harness, maxScopes int) *sql.DB {
	t.Helper()
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open scope assignment database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE public.cf_scope_assignments (
			user_id text NOT NULL,
			scope_id text NOT NULL,
			PRIMARY KEY (user_id, scope_id)
		);
		GRANT SELECT ON public.cf_scope_assignments TO synchro_owner;
		CREATE FUNCTION public.cf_assigned_scopes(p_user_id text)
		RETURNS SETOF text
		LANGUAGE sql STABLE
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT scope_id FROM public.cf_scope_assignments WHERE user_id = p_user_id;
		END;
		REVOKE ALL ON FUNCTION public.cf_assigned_scopes(text) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.cf_assigned_scopes(text) TO synchro_owner`); err != nil {
		t.Fatalf("create scope assignment function: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		"SELECT synchro.synchro_register_assignment_function('public.cf_assigned_scopes', $1)",
		maxScopes,
	); err != nil {
		t.Fatalf("register scope assignment function: %v", err)
	}
	return database
}

// setRealAssignedScopes replaces the application rows that assign scopes to
// the diagnostic user.
func setRealAssignedScopes(t *testing.T, ctx context.Context, database *sql.DB, scopeIDs ...string) {
	t.Helper()
	if _, err := database.ExecContext(ctx, "DELETE FROM public.cf_scope_assignments WHERE user_id = 'diagnostic-user'"); err != nil {
		t.Fatalf("clear assigned scopes: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO public.cf_scope_assignments (user_id, scope_id)
		SELECT 'diagnostic-user', scope_id FROM unnest($1::text[]) AS assigned(scope_id)`,
		scopeIDs,
	); err != nil {
		t.Fatalf("insert assigned scopes: %v", err)
	}
}

// observeRealAssignmentState returns the durable assignment state of one
// diagnostic client as canonical JSON text.
func observeRealAssignmentState(t *testing.T, ctx context.Context, database *sql.DB, clientID string) string {
	t.Helper()
	var state string
	if err := database.QueryRowContext(ctx, `
		SELECT jsonb_build_object(
			'client', (
				SELECT to_jsonb(client) FROM synchro.sync_clients client
				WHERE client.user_id = 'diagnostic-user' AND client.client_id = $1
			),
			'history', (
				SELECT jsonb_agg(to_jsonb(history) ORDER BY history.scope_id, history.scope_set_version)
				FROM synchro.sync_client_scope_history history
				WHERE history.user_id = 'diagnostic-user' AND history.client_id = $1
			),
			'checkpoints', (
				SELECT jsonb_agg(to_jsonb(checkpoint) ORDER BY checkpoint.bucket_id)
				FROM synchro.sync_client_checkpoints checkpoint
				WHERE checkpoint.user_id = 'diagnostic-user' AND checkpoint.client_id = $1
			),
			'assigned_scope_state', (
				SELECT jsonb_agg(to_jsonb(state) ORDER BY state.scope_id)
				FROM synchro.sync_scope_state state
				WHERE state.scope_id LIKE 'cf:assigned-%'
			)
		)::text`,
		clientID,
	).Scan(&state); err != nil {
		t.Fatalf("observe durable assignment state: %v", err)
	}
	return state
}
