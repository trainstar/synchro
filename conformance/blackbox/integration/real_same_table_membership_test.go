package integration

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

// TestRealSameTableMembershipPropagatesSiblingChanges proves the self-impact
// contract. A note is visible to every author of a live note on the same
// document. A note insert, move, or delete therefore changes the visible rows of
// sibling notes. Expected row sets follow only from that membership rule.
func TestRealSameTableMembershipPropagatesSiblingChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	// An empty relation changes no scope, so the rule transition declares one authoritative scope.
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
		SELECT 'user:team-bootstrap', stream_generation
		FROM synchro.sync_runtime_state
		WHERE singleton`,
	); err != nil {
		t.Fatalf("create membership transition scope: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE FUNCTION public.cf_document_notes_team_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT DISTINCT 'user:' || (peer.author_id #>> '{}')
			FROM synchro_projection.cf_document_notes AS note
			JOIN synchro_projection.cf_document_notes AS peer
			  ON peer.document_id = note.document_id AND NOT peer.deleted
			WHERE note.record_id = p_id::text AND NOT note.deleted;
		END;
		REVOKE ALL ON FUNCTION public.cf_document_notes_team_membership(uuid) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.cf_document_notes_team_membership(uuid)
			TO synchro_owner, synchro_worker;
		SELECT synchro.synchro_register_table(
			'public.cf_document_notes',
			'public.cf_document_notes_team_membership',
			'multi_scope',
			'id', 'updated_at', 'deleted_at', 'enabled',
			p_affected_scopes => ARRAY['user:team-bootstrap']::text[]
		)`,
	); err != nil {
		t.Fatalf("register sibling-dependent membership function: %v", err)
	}
	waitForSameTableMembershipFunction(t, ctx, admin)

	const activeNoteFields = `
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation AND generation.state = 'active'
		JOIN synchro.sync_registry_fields field
		  ON field.registry_generation = registry.registry_generation
		 AND field.relation_id = registry.relation_id
		WHERE registry.physical_schema = 'public'
		  AND registry.physical_relation = 'cf_document_notes'`
	var tableID, documentFieldID string
	if err := admin.QueryRowContext(ctx,
		"SELECT registry.table_id::text, field.field_id::text"+activeNoteFields+" AND field.physical_column = 'document_id'",
	).Scan(&tableID, &documentFieldID); err != nil {
		t.Fatalf("load note registration identity: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(`
		CREATE FUNCTION public.cf_document_notes_team_impact(p_old_row jsonb, p_new_row jsonb)
		RETURNS SETOF synchro.synchro_row_ref
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT ROW('%[1]s'::uuid, 'string', pg_catalog.to_jsonb(peer.record_id))::synchro.synchro_row_ref
			FROM synchro_projection.cf_document_notes AS peer
			WHERE NOT peer.deleted
			  AND peer.document_id #>> '{}' IN (p_old_row ->> '%[2]s', p_new_row ->> '%[2]s');
		END;
		REVOKE ALL ON FUNCTION public.cf_document_notes_team_impact(jsonb, jsonb) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.cf_document_notes_team_impact(jsonb, jsonb)
			TO synchro_owner, synchro_worker`, tableID, documentFieldID),
	); err != nil {
		t.Fatalf("create sibling impact function: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		SELECT synchro.synchro_register_membership_dependency(
			'cf_document_notes', 'cf_document_notes', 'public.cf_document_notes_team_impact',
			(SELECT array_agg(field.field_id::text ORDER BY field.field_id)`+activeNoteFields+`
			   AND field.physical_column = ANY(ARRAY['id', 'document_id', 'author_id', 'deleted_at'])),
			1000
		)`,
	); err != nil {
		t.Fatalf("register self-impact declaration: %v", err)
	}
	waitForReleaseImpactRegistration(t, ctx, admin, "cf_document_notes_team_impact")

	documentOne := "00000000-0000-4000-8215-000000000001"
	documentTwo := "00000000-0000-4000-8215-000000000002"
	for _, documentID := range []string{documentOne, documentTwo} {
		if err := harness.Source().ExecContext(ctx,
			"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'team-owner', 'team document')", documentID,
		); err != nil {
			t.Fatalf("insert team document: %v", err)
		}
	}
	alice := newSameTableClient(t, ctx, harness, "team-alice", "00000000-0000-4000-8215-0000000000a")
	bob := newSameTableClient(t, ctx, harness, "team-bob", "00000000-0000-4000-8215-0000000000b")

	aliceOne := "00000000-0000-4000-8215-000000000011"
	aliceTwo := "00000000-0000-4000-8215-000000000012"
	bobOne := "00000000-0000-4000-8215-000000000021"
	insertNote := func(noteID, documentID, author string) {
		t.Helper()
		if err := harness.Source().ExecContext(ctx,
			"INSERT INTO cf_document_notes (id, document_id, author_id, body) VALUES ($1, $2, $3, 'team note')",
			noteID, documentID, author,
		); err != nil {
			t.Fatalf("insert team note: %v", err)
		}
	}
	steps := []struct {
		name  string
		write func()
		alice []string
		bob   []string
	}{
		{
			name: "initial",
			write: func() {
				insertNote(aliceOne, documentOne, "team-alice")
				insertNote(bobOne, documentTwo, "team-bob")
			},
			alice: []string{aliceOne},
			bob:   []string{bobOne},
		},
		{
			name:  "insert",
			write: func() { insertNote(aliceTwo, documentTwo, "team-alice") },
			alice: []string{aliceOne, aliceTwo, bobOne},
			bob:   []string{aliceTwo, bobOne},
		},
		{
			name: "move",
			write: func() {
				if err := harness.Source().ExecContext(ctx,
					"UPDATE cf_document_notes SET document_id = $2, updated_at = clock_timestamp() WHERE id = $1", bobOne, documentOne,
				); err != nil {
					t.Fatalf("move team note: %v", err)
				}
			},
			alice: []string{aliceOne, aliceTwo, bobOne},
			bob:   []string{aliceOne, bobOne},
		},
		{
			name: "delete",
			write: func() {
				if err := harness.Source().ExecContext(ctx, "DELETE FROM cf_document_notes WHERE id = $1", bobOne); err != nil {
					t.Fatalf("delete team note: %v", err)
				}
			},
			alice: []string{aliceOne, aliceTwo},
			bob:   []string{},
		},
	}
	for _, step := range steps {
		step.write()
		alice.waitForRows(t, ctx, harness, step.name, step.alice)
		bob.waitForRows(t, ctx, harness, step.name, step.bob)
	}
	for _, client := range []*sameTableClient{alice, bob} {
		acknowledgeRealClientCursors(t, ctx, harness, client.token, client.protocol)
	}
}

type sameTableClient struct {
	token    string
	scope    string
	protocol *realProtocolClient
	table    realProtocolTable
	rows     map[string]bool
}

func newSameTableClient(t *testing.T, ctx context.Context, harness *blackbox.Harness, userID, rebuildPrefix string) *sameTableClient {
	t.Helper()
	token, err := harness.NativeBearerToken(ctx, userID, time.Now())
	if err != nil {
		t.Fatalf("sign %s token: %v", userID, err)
	}
	scope := "user:" + userID
	protocol := connectRealProtocolClient(t, ctx, harness, token, userID+"-client", "cf:global", scope)
	rebuildRealScope(t, ctx, harness, token, protocol, "cf:global", rebuildPrefix+"01")
	if records, _ := rebuildRealScope(t, ctx, harness, token, protocol, scope, rebuildPrefix+"02"); len(records) != 0 {
		t.Fatalf("%s scope is not empty before team notes exist: %d records", userID, len(records))
	}
	return &sameTableClient{
		token:    token,
		scope:    scope,
		protocol: protocol,
		table:    requireRealTable(t, protocol, "cf_document_notes"),
		rows:     map[string]bool{},
	}
}

// waitForRows applies pulled note changes in the client's user scope and waits
// until the visible note set equals the expected set.
func (client *sameTableClient) waitForRows(t *testing.T, ctx context.Context, harness *blackbox.Harness, step string, want []string) {
	t.Helper()
	want = slices.Sorted(slices.Values(want))
	var got []string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, change := range requireRealChanges(t, pullRealClient(t, ctx, harness, client.token, client.protocol)) {
			if change["table"] != client.table.ID || change["scope"] != client.scope {
				continue
			}
			pk, _ := change["pk"].(map[string]any)
			recordID, ok := pk[client.table.PrimaryKeyField].(string)
			if !ok {
				t.Fatalf("team note change key is invalid: %#v", change)
			}
			switch change["op"] {
			case "upsert":
				client.rows[recordID] = true
			case "delete":
				delete(client.rows, recordID)
			default:
				t.Fatalf("team note change operation is invalid: %#v", change)
			}
		}
		got = got[:0]
		for recordID := range client.rows {
			got = append(got, recordID)
		}
		slices.Sort(got)
		if slices.Equal(got, want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s rows after %s = %v, want %v", client.scope, step, got, want)
}

func waitForSameTableMembershipFunction(t *testing.T, ctx context.Context, admin *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var active bool
		if err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_registry registry
				JOIN synchro.sync_registry_generations generation
				  ON generation.generation = registry.registry_generation
				WHERE generation.state = 'active'
				  AND registry.physical_relation = 'cf_document_notes'
				  AND registry.membership_function_name = 'cf_document_notes_team_membership'
			)`).Scan(&active); err != nil {
			t.Fatalf("observe sibling membership activation: %v", err)
		}
		if active {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("sibling-dependent membership function did not activate")
}
