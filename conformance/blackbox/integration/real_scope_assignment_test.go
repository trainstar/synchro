package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

// TestRealLargeScopeAssignmentLifecycle checks complete assignment delivery,
// rebuild cursors, reconciliation, history, and isolation through HTTP.
func TestRealLargeScopeAssignmentLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	database := registerRealScopeAssignmentFunction(t, ctx, harness, 1000)
	const (
		clientID      = "large-assignment-client"
		otherUser     = "large-assignment-other"
		identityScope = "user:diagnostic-user"
		recordID      = "00000000-0000-4000-8c08-000000000001"
	)

	t.Run("assertion", func(t *testing.T) {
		if _, err := database.ExecContext(ctx, `
			CREATE OR REPLACE FUNCTION public.cf_assigned_scopes(p_user_id text)
			RETURNS SETOF text
			LANGUAGE sql STABLE
			SET search_path = pg_catalog, synchro
			BEGIN ATOMIC
				SELECT scope_id FROM public.cf_scope_assignments WHERE user_id = p_user_id
				UNION ALL
				SELECT scope_id
				FROM public.cf_scope_assignments
				WHERE user_id = p_user_id AND scope_id = 'cf:assigned-0001';
			END;
			SELECT synchro.synchro_register_assignment_function('public.cf_assigned_scopes');
			INSERT INTO public.cf_scope_assignments (user_id, scope_id)
			SELECT 'diagnostic-user', 'cf:assigned-' || lpad(scope_number::text, 4, '0')
			FROM generate_series(1, 1001) AS scope_number
			UNION ALL
			SELECT 'large-assignment-other', 'cf:assigned-other';
			SELECT synchro.synchro_grant_user_scope('diagnostic-user', 'cf:assigned-0001');
			SELECT synchro.synchro_grant_user_scope('diagnostic-user', 'cf:granted-only')`); err != nil {
			t.Fatalf("prepare large assignment fixture: %v", err)
		}
		observeScopeState := func() string {
			t.Helper()
			var state string
			if err := database.QueryRowContext(ctx, `
				SELECT COALESCE(jsonb_agg(to_jsonb(state) ORDER BY state.scope_id), '[]'::jsonb)::text
				FROM synchro.sync_scope_state state`).Scan(&state); err != nil {
				t.Fatalf("observe complete scope state: %v", err)
			}
			return state
		}
		before := observeRealAssignmentState(t, ctx, database, clientID)
		beforeScopes := observeScopeState()
		status, failed := postSync(t, ctx, harness.AdapterURL(), token, "/sync/connect", map[string]any{
			"client_id": clientID, "platform": "conformance", "app_version": "0.3.0", "protocol_version": 3,
			"schema": map[string]any{"version": 0, "hash": ""}, "scope_set_version": 0, "known_scopes": map[string]any{},
		})
		requireRealProtocolError(t, status, failed, http.StatusInternalServerError, "sync_integrity_failure")
		if after := observeRealAssignmentState(t, ctx, database, clientID); after != before {
			t.Fatalf("failed large connect changed assignment state:\nbefore=%s\nafter=%s", before, after)
		}
		if after := observeScopeState(); after != beforeScopes {
			t.Fatalf("failed large connect changed complete scope state:\nbefore=%s\nafter=%s", beforeScopes, after)
		}
		if _, err := database.ExecContext(ctx,
			"SELECT synchro.synchro_register_assignment_function('public.cf_assigned_scopes', NULL)"); err != nil {
			t.Fatalf("select unbounded assignment evaluation: %v", err)
		}
		if err := harness.Source().ExecContext(ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'large-assignment-before')", recordID); err != nil {
			t.Fatalf("insert large assignment owned row: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
		wantScopes := []string{"cf:global", "cf:granted-only", identityScope}
		for scopeNumber := 1; scopeNumber <= 1001; scopeNumber++ {
			wantScopes = append(wantScopes, fmt.Sprintf("cf:assigned-%04d", scopeNumber))
		}
		slices.Sort(wantScopes)
		client := connectRealProtocolClient(t, ctx, harness, token, clientID, slices.Clone(wantScopes)...)
		if len(client.Scopes) != 1004 {
			t.Fatalf("large assignment count = %d, want 1004", len(client.Scopes))
		}
		observeHistory := func(userID, clientID string) string {
			t.Helper()
			var history string
			if err := database.QueryRowContext(ctx, `
				SELECT COALESCE(jsonb_agg(to_jsonb(history)
				       ORDER BY history.client_generation, history.scope_id, history.scope_set_version), '[]'::jsonb)::text
				FROM synchro.sync_client_scope_history history
				WHERE history.user_id = $1 AND history.client_id = $2`, userID, clientID).Scan(&history); err != nil {
				t.Fatalf("observe ordered assignment history: %v", err)
			}
			return history
		}
		requireLatestHistory := func(wanted map[string]any) {
			t.Helper()
			var encoded string
			if err := database.QueryRowContext(ctx, `
				SELECT jsonb_object_agg(scope_id, jsonb_build_object(
				    'assigned', assigned, 'source', assignment_source,
				    'version', scope_set_version, 'rows', history_rows))::text
				FROM (
				    SELECT DISTINCT ON (scope_id) scope_id, assigned, assignment_source, scope_set_version,
				           count(*) OVER (PARTITION BY scope_id) AS history_rows
				    FROM synchro.sync_client_scope_history
				    WHERE user_id = 'diagnostic-user' AND client_id = $1 AND client_generation = $2
				    ORDER BY scope_id, scope_set_version DESC
				) latest`, client.ID, client.Generation).Scan(&encoded); err != nil {
				t.Fatalf("observe latest large assignment history: %v", err)
			}
			var latest map[string]any
			if err := json.Unmarshal([]byte(encoded), &latest); err != nil {
				t.Fatalf("decode latest large assignment history: %v", err)
			}
			if !reflect.DeepEqual(latest, wanted) {
				t.Fatalf("latest assignment history differs:\ngot=%v\nwant=%v", latest, wanted)
			}
		}
		wantHistory := make(map[string]any, len(wantScopes))
		for _, scopeID := range wantScopes {
			source := "assignment_rule"
			switch scopeID {
			case identityScope:
				source = "identity"
			case "cf:global":
				source = "shared"
			}
			wantHistory[scopeID] = map[string]any{
				"assigned": true, "source": source, "version": float64(client.ScopeSetVersion), "rows": float64(1),
			}
		}
		requireLatestHistory(wantHistory)
		table := requireRealTable(t, client, "cf_items")
		for index, scopeID := range wantScopes {
			rebuildID := fmt.Sprintf("00000000-0000-4000-8c08-%012d", index+100)
			records, cursor := rebuildRealScope(t, ctx, harness, token, client, scopeID, rebuildID)
			if cursor == "" {
				t.Fatalf("scope %s has no final rebuild cursor", scopeID)
			}
			if scopeID == identityScope {
				requireRebuildRecordVersion(t, records, table, recordID, "large-assignment-before")
			} else if len(records) != 0 {
				t.Fatalf("empty fixture scope %s returned %d records", scopeID, len(records))
			}
		}
		requireEmptyDelta := func(response map[string]any, key string) {
			t.Helper()
			wanted := map[string]any{"add": []any{}, "remove": []any{}}
			if !reflect.DeepEqual(response[key], wanted) {
				t.Fatalf("unchanged assignment returned %s: %#v", key, response[key])
			}
		}
		requireUnchangedPull := func() {
			t.Helper()
			history := observeHistory("diagnostic-user", client.ID)
			response := pullRealClient(t, ctx, harness, token, client)
			requireEmptyDelta(response, "scope_updates")
			if response["scope_set_version"] != float64(client.ScopeSetVersion) || len(requireRealChanges(t, response)) != 0 {
				t.Fatalf("unchanged cursor pull returned a version change or data: %#v", response)
			}
			if after := observeHistory("diagnostic-user", client.ID); after != history {
				t.Fatal("unchanged cursor pull changed assignment history")
			}
		}
		encodedPull, err := json.Marshal(realPullPayload(client, client.Scopes, 100))
		if err != nil {
			t.Fatalf("encode large cursor-bearing request: %v", err)
		}
		t.Logf("cursor-bearing pull request bytes=%d assigned scopes=%d", len(encodedPull), len(client.Scopes))
		requireUnchangedPull()
		requireUnchangedConnect := func(userID, signedToken string, current *realProtocolClient, expectedScopes []string, expectedHistory string) {
			t.Helper()
			if before := observeHistory(userID, current.ID); before != expectedHistory {
				t.Fatalf("assignment transition changed %s history", userID)
			}
			status, response := postSync(t, ctx, harness.AdapterURL(), signedToken, "/sync/connect", map[string]any{
				"client_id": current.ID, "client_generation": current.Generation, "platform": "conformance",
				"app_version": "0.3.0", "protocol_version": 3, "schema": current.Schema,
				"scope_set_version": current.ScopeSetVersion, "known_scopes": current.Scopes,
			})
			if status != http.StatusOK || response["client_generation"] != float64(current.Generation) ||
				response["scope_set_version"] != float64(current.ScopeSetVersion) {
				t.Fatalf("unchanged connect changed identity or version: status=%d response=%#v", status, response)
			}
			requireEmptyDelta(response, "scopes")
			wantedSchema := map[string]any{
				"version": float64(current.Schema["version"].(int64)), "hash": current.Schema["hash"], "action": "none",
			}
			if !reflect.DeepEqual(response["schema"], wantedSchema) {
				t.Fatalf("unchanged connect returned schema %v, want %v", response["schema"], wantedSchema)
			}
			if !reflect.DeepEqual(response["scope_cursor_updates"], map[string]any{}) {
				t.Fatalf("unchanged connect returned cursor updates: %#v", response["scope_cursor_updates"])
			}
			for _, forbidden := range []string{"schema_definition", "affected_scopes"} {
				if _, present := response[forbidden]; present {
					t.Fatalf("unchanged connect returned forbidden member %s", forbidden)
				}
			}
			var encodedScopes string
			if err := database.QueryRowContext(ctx,
				"SELECT to_jsonb(bucket_subs)::text FROM synchro.sync_clients WHERE user_id = $1 AND client_id = $2",
				userID, current.ID).Scan(&encodedScopes); err != nil {
				t.Fatalf("observe reconnected scope set: %v", err)
			}
			var assigned []string
			if err := json.Unmarshal([]byte(encodedScopes), &assigned); err != nil {
				t.Fatalf("decode reconnected scope set: %v", err)
			}
			slices.Sort(assigned)
			wanted := slices.Clone(expectedScopes)
			slices.Sort(wanted)
			if !slices.Equal(assigned, wanted) {
				t.Fatalf("reconnected scopes for %s = %v, want %v", userID, assigned, wanted)
			}
			if after := observeHistory(userID, current.ID); after != expectedHistory {
				t.Fatalf("unchanged connect changed %s history", userID)
			}
		}
		requireUnchangedConnect("diagnostic-user", token, client, wantScopes, observeHistory("diagnostic-user", client.ID))
		if err := harness.Source().ExecContext(ctx,
			"UPDATE cf_items SET value = 'large-assignment-after', updated_at = now() WHERE id = $1", recordID); err != nil {
			t.Fatalf("update large assignment owned row: %v", err)
		}
		waitForRealWALEffects(t, ctx, harness, "cf_items", 2, recordID)
		updated := pullRealClient(t, ctx, harness, token, client)
		requireEmptyDelta(updated, "scope_updates")
		changes := requireRealChanges(t, updated)
		if updated["scope_set_version"] != float64(client.ScopeSetVersion) || len(changes) != 1 {
			t.Fatalf("owned-row pull changed assignment or returned %d changes", len(changes))
		}
		change := requireRealPullChange(t, changes, identityScope, table, recordID, "large-assignment-after")
		if change["op"] != "upsert" {
			t.Fatalf("owned-row pull operation = %v, want upsert", change["op"])
		}
		otherToken, err := harness.NativeBearerToken(ctx, otherUser, time.Now())
		if err != nil {
			t.Fatalf("sign second assignment user token: %v", err)
		}
		otherScopes := []string{"cf:assigned-other", "cf:global", "user:" + otherUser}
		other := connectRealProtocolClient(t, ctx, harness, otherToken, "large-assignment-other-client", slices.Clone(otherScopes)...)
		otherHistory := observeHistory(otherUser, other.ID)
		status, denied := requestRealRebuildPage(t, ctx, harness, otherToken, other, "cf:assigned-0001",
			"00000000-0000-4000-8c08-000000002001", nil, 100)
		requireRealProtocolError(t, status, denied, http.StatusBadRequest, "invalid_request")
		requireScopeTransition := func(added, removed []string) {
			t.Helper()
			status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, client.Scopes, 100))
			if status != http.StatusOK || response["scope_set_version"] != float64(client.ScopeSetVersion+1) {
				t.Fatalf("scope transition did not advance its version once: status=%d response=%#v", status, response)
			}
			additions := make([]any, 0, len(added))
			rebuilds := make([]any, 0, len(added))
			for _, scopeID := range added {
				additions = append(additions, map[string]any{"id": scopeID, "cursor": nil})
				rebuilds = append(rebuilds, scopeID)
			}
			removals := make([]any, 0, len(removed))
			for _, scopeID := range removed {
				removals = append(removals, scopeID)
			}
			wanted := map[string]any{"add": additions, "remove": removals}
			if !reflect.DeepEqual(response["scope_updates"], wanted) || !reflect.DeepEqual(response["rebuild"], rebuilds) ||
				len(requireRealChanges(t, response)) != 0 {
				t.Fatalf("scope transition returned an incorrect delta, rebuild, or data: %#v", response)
			}
			client.ScopeSetVersion++
			for _, scopeID := range removed {
				delete(client.Scopes, scopeID)
			}
			for _, scopeID := range added {
				client.Scopes[scopeID] = map[string]any{"cursor": nil}
			}
			cursors, ok := response["scope_cursors"].(map[string]any)
			if !ok {
				t.Fatal("scope transition cursors are invalid")
			}
			for scopeID, rawCursor := range cursors {
				cursor, valid := rawCursor.(string)
				if _, assigned := client.Scopes[scopeID]; !assigned || !valid || cursor == "" || slices.Contains(added, scopeID) {
					t.Fatalf("scope transition returned an invalid cursor for %s", scopeID)
				}
				client.Scopes[scopeID] = map[string]any{"cursor": cursor}
			}
		}
		transaction, err := database.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin assignment reconciliation change: %v", err)
		}
		defer transaction.Rollback()
		if _, err := transaction.ExecContext(ctx,
			"DELETE FROM public.cf_scope_assignments WHERE user_id = 'diagnostic-user' AND scope_id IN ('cf:assigned-0001', 'cf:assigned-0002')"); err != nil {
			t.Fatalf("remove function assignments: %v", err)
		}
		if _, err := transaction.ExecContext(ctx,
			"INSERT INTO public.cf_scope_assignments (user_id, scope_id) VALUES ('diagnostic-user', 'cf:assigned-1002')"); err != nil {
			t.Fatalf("add function assignment: %v", err)
		}
		if err := transaction.Commit(); err != nil {
			t.Fatalf("commit assignment reconciliation change: %v", err)
		}
		requireScopeTransition([]string{"cf:assigned-1002"}, []string{"cf:assigned-0002"})
		records, cursor := rebuildRealScope(t, ctx, harness, token, client, "cf:assigned-1002",
			"00000000-0000-4000-8c08-000000002002")
		if len(records) != 0 || cursor == "" {
			t.Fatal("added assignment scope did not rebuild as empty with a final cursor")
		}
		wantHistory["cf:assigned-0002"] = map[string]any{
			"assigned": false, "source": "assignment_rule", "version": float64(client.ScopeSetVersion), "rows": float64(2),
		}
		wantHistory["cf:assigned-1002"] = map[string]any{
			"assigned": true, "source": "assignment_rule", "version": float64(client.ScopeSetVersion), "rows": float64(1),
		}
		requireLatestHistory(wantHistory)
		status, removed := requestRealRebuildPage(t, ctx, harness, token, client, "cf:assigned-0002",
			"00000000-0000-4000-8c08-000000002003", nil, 100)
		requireRealProtocolError(t, status, removed, http.StatusBadRequest, "invalid_request")
		requireUnchangedConnect(otherUser, otherToken, other, otherScopes, otherHistory)
		if _, err := database.ExecContext(ctx,
			"SELECT synchro.synchro_revoke_user_scope('diagnostic-user', 'cf:assigned-0001')"); err != nil {
			t.Fatalf("revoke retained overlap grant: %v", err)
		}
		requireScopeTransition(nil, []string{"cf:assigned-0001"})
		wantHistory["cf:assigned-0001"] = map[string]any{
			"assigned": false, "source": "assignment_rule", "version": float64(client.ScopeSetVersion), "rows": float64(2),
		}
		requireLatestHistory(wantHistory)
		requireUnchangedPull()
		requireUnchangedConnect(otherUser, otherToken, other, otherScopes, otherHistory)
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
