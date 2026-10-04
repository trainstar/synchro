package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/release"
)

// TestRealExtensionUpdateFromBaseline updates each pinned released origin to
// the current version. Each update must equal a clean installation, keep the
// cross-table membership declarations, and accept a same-table declaration.
func TestRealExtensionUpdateFromBaseline(t *testing.T) {
	for _, origin := range realUpdateOrigins(t) {
		var wantSourceRequirement sql.NullInt64
		switch origin.version {
		case "0.3.1", "0.3.2":
		case "0.4.0-rc.1":
			wantSourceRequirement = sql.NullInt64{Int64: 2, Valid: true}
		default:
			t.Fatalf("missing fixture contract for update origin %s", origin.version)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		harness := provisionRealUpdateHarness(t, ctx, origin)
		token, err := harness.DiagnosticBearerToken(time.Now())
		if err != nil {
			t.Fatalf("sign extension update token: %v", err)
		}
		admin := openIssue49Admin(t, ctx, harness)
		originDependencies := activeMembershipDependencies(t, ctx, admin)

		t.Run("assertion", func(t *testing.T) {
			t.Logf("update origin %s", origin.version)
			beforeID := "00000000-0000-4000-8c07-000000000001"
			if err := harness.Source().ExecContext(
				ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
				beforeID,
				"diagnostic-user",
				"before-extension-update",
			); err != nil {
				t.Fatalf("insert source row before extension update: %v", err)
			}
			waitForRealWALRecords(t, ctx, harness, "cf_items", beforeID)

			// A client of another user syncs with the predecessor. Its rows stay
			// apart from the diagnostic-user rows that the retained generation
			// changes, so each later pull has an exact expected change set.
			const (
				clientUser      = "extension-update-user"
				clientUserScope = "user:" + clientUser
				clientID        = "extension-update-client"
				pushedID        = "00000000-0000-4000-8c07-000000000041"
				successorID     = "00000000-0000-4000-8c07-000000000046"
			)
			observeAssignmentRegistration := func() string {
				t.Helper()
				if origin.version != "0.4.0-rc.1" {
					return "null"
				}
				var registration string
				if err := admin.QueryRowContext(ctx, `
					SELECT to_jsonb(registration)::text
					FROM synchro.sync_assignment_function registration
					WHERE singleton`).Scan(&registration); err != nil {
					t.Fatalf("observe retained assignment registration: %v", err)
				}
				return registration
			}
			var predecessorRegistration string
			if origin.version == "0.4.0-rc.1" {
				database := registerRealScopeAssignmentFunction(t, ctx, harness, 8)
				if _, err := database.ExecContext(ctx,
					"INSERT INTO public.cf_scope_assignments (user_id, scope_id) VALUES ($1, 'cf:global')",
					clientUser); err != nil {
					t.Fatalf("insert predecessor client assignment: %v", err)
				}
				predecessorRegistration = observeAssignmentRegistration()
			}
			clientToken, err := harness.NativeBearerToken(ctx, clientUser, time.Now())
			if err != nil {
				t.Fatalf("sign extension update client token: %v", err)
			}
			if err := harness.Source().ExecContext(ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, 'predecessor-source')",
				pushedID, clientUser); err != nil {
				t.Fatalf("insert predecessor client row: %v", err)
			}
			waitForRealWALRecords(t, ctx, harness, "cf_items", pushedID)
			if err := harness.StartUpdateBaselineAdapter(ctx); err != nil {
				t.Fatalf("start the adapter on %s: %v; %s", origin.version, err, harness.FailureDiagnostics())
			}
			updateClient := connectRealProtocolClient(t, ctx, harness, clientToken, clientID, "cf:global", clientUserScope)
			rebuildRealScope(t, ctx, harness, clientToken, updateClient, "cf:global", "00000000-0000-4000-8c07-000000000044")
			clientRecords, _ := rebuildRealScope(t, ctx, harness, clientToken, updateClient, clientUserScope, "00000000-0000-4000-8c07-000000000045")
			clientTable := requireRealTable(t, updateClient, "cf_items")
			baseVersion := requireRebuildRecordVersion(t, clientRecords, clientTable, pushedID, "predecessor-source")
			// This pull stores the server checkpoints that the update must keep.
			acknowledgeRealClientCursors(t, ctx, harness, clientToken, updateClient)
			pushPayload := realPushPayload(realPushAttempt{
				client:        updateClient,
				batchID:       "00000000-0000-4000-8c07-000000000042",
				mutationID:    "00000000-0000-4000-8c07-000000000043",
				clientVersion: "2032-01-01T00:00:00.000000Z",
				value:         "predecessor-push",
			}, clientTable, pushedID, baseVersion)
			firstPush, firstPushBody := issue49RawSync(t, ctx, harness.AdapterURL(), clientToken, "/sync/push", pushPayload)
			pushOutcomes := requireOutcomeList(t, firstPushBody, "accepted")
			if firstPush.Status != http.StatusOK || len(pushOutcomes) != 1 || pushOutcomes[0]["status"] != "applied" ||
				len(requireOutcomeList(t, firstPushBody, "rejected")) != 0 {
				t.Fatalf("predecessor push on %s was not applied: status=%d body=%s", origin.version, firstPush.Status, firstPush.Body)
			}
			pushOutcome := pushOutcomes[0]
			assertOutcomeValue(t, pushOutcome, clientTable, "predecessor-push")
			// The source insert and the push each create one change of the row.
			waitForRealWALEffects(t, ctx, harness, "cf_items", 2, pushedID)
			pushedChanges, err := harness.Operator().ObserveWALRecordsForTable(ctx, "cf_items", []string{pushedID})
			if err != nil || len(pushedChanges.Records) != 2 || pushedChanges.Records[1].RowVersion != pushOutcome["server_version"] {
				t.Fatalf("predecessor WAL did not keep the push version %v: %#v, %v", pushOutcome["server_version"], pushedChanges, err)
			}
			if err := harness.StopUpdateBaselineAdapter(ctx); err != nil {
				t.Fatalf("stop the adapter on %s: %v", origin.version, err)
			}
			// The update must keep client, assignment, checkpoint, push ledger,
			// and row state.
			retainedClientState := func() string {
				t.Helper()
				var state string
				if err := admin.QueryRowContext(ctx, `
					SELECT jsonb_build_object(
					    'client', (SELECT jsonb_build_object(
					                   'generation', client_generation, 'scope_set_version', scope_set_version,
					                   'scopes', bucket_subs, 'write_epoch', accepted_write_epoch, 'active', is_active)
					               FROM synchro.sync_clients WHERE user_id = $1 AND client_id = $2),
					    'assignment_registration', $4::jsonb,
					    'assignment_history', (SELECT jsonb_agg(to_jsonb(history)
					                              ORDER BY history.client_generation, history.scope_id, history.scope_set_version)
					                           FROM synchro.sync_client_scope_history history
					                           WHERE history.user_id = $1 AND history.client_id = $2),
					    'checkpoints', (SELECT jsonb_agg(to_jsonb(checkpoint) ORDER BY checkpoint.bucket_id)
					                    FROM synchro.sync_client_checkpoints checkpoint
					                    WHERE checkpoint.user_id = $1 AND checkpoint.client_id = $2),
					    'batches', (SELECT jsonb_agg(to_jsonb(batch) ORDER BY batch.batch_id)
					                FROM synchro.sync_push_batches batch
					                WHERE batch.user_id = $1 AND batch.client_id = $2),
					    'mutations', (SELECT jsonb_agg(to_jsonb(mutation) ORDER BY mutation.mutation_id)
					                  FROM synchro.sync_push_mutations mutation
					                  WHERE mutation.user_id = $1 AND mutation.client_id = $2),
					    'versions', (SELECT jsonb_agg(to_jsonb(version) ORDER BY version.record_id)
					                 FROM synchro.sync_row_versions version WHERE version.record_id = $3),
					    'source', (SELECT jsonb_build_object('id', item.id, 'owner_id', item.owner_id, 'value', item.value,
					                                         'updated_at', item.updated_at, 'deleted_at', item.deleted_at)
					               FROM public.cf_items item WHERE item.id = $3::uuid)
					)::text`, clientUser, clientID, pushedID, observeAssignmentRegistration()).Scan(&state); err != nil {
					t.Fatalf("observe retained client state: %v", err)
				}
				return state
			}
			predecessorState := retainedClientState()

			// The worker gate keeps the validated field addition pending.
			// Legacy origins retain an unknown source requirement.
			// Rc.1 records requirement 2 because the field has a value at admission.
			// Both paths require bootstrap and Class 3 activation.
			const retainedValue = "retained-pending-value"
			session, err := admin.Conn(ctx)
			if err != nil {
				t.Fatalf("acquire retained-generation session: %v", err)
			}
			defer session.Close()
			if _, err := session.ExecContext(ctx, "SELECT pg_advisory_lock(2002873458::bigint)"); err != nil {
				t.Fatalf("acquire WAL worker gate: %v", err)
			}
			transaction, err := session.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin retained-generation registration: %v", err)
			}
			defer transaction.Rollback()
			if _, err := transaction.ExecContext(ctx, "ALTER TABLE public.cf_items ADD COLUMN retained_note text"); err != nil {
				t.Fatalf("add the retained field on %s: %v", origin.version, err)
			}
			if _, err := transaction.ExecContext(ctx,
				"UPDATE public.cf_items SET retained_note = $2 WHERE id = $1", beforeID, "retained-admission-value"); err != nil {
				t.Fatalf("write the retained field before registration: %v", err)
			}
			if _, err := transaction.ExecContext(ctx, `WITH parent AS MATERIALIZED (
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
				     exclude_columns, array_append(sync_columns, 'retained_note'),
				     max_scope_fanout
				 )
				 FROM parent`); err != nil {
				t.Fatalf("register the retained field on %s: %v", origin.version, err)
			}
			var retainedGeneration int64
			if err := transaction.QueryRowContext(ctx, `
				SELECT generation FROM synchro.sync_registry_generations
				WHERE state = 'pending' AND validated`).Scan(&retainedGeneration); err != nil {
				t.Fatalf("observe retained pending generation on %s: %v", origin.version, err)
			}
			if _, err := transaction.ExecContext(ctx,
				"UPDATE public.cf_items SET retained_note = $2 WHERE id = $1", beforeID, retainedValue); err != nil {
				t.Fatalf("write the retained field after registration: %v", err)
			}
			if err := transaction.Commit(); err != nil {
				t.Fatalf("commit retained-generation registration: %v", err)
			}

			update, err := harness.UpdateExtension(ctx)
			if err != nil {
				t.Fatalf("update extension from %s: %v", origin.version, err)
			}
			if update.VersionBeforeUpdate != origin.version || update.ReadyBeforeUpdate ||
				update.ExtensionObjectsStateBeforeUpdate == "ok" || update.VersionAfterUpdate != release.Version ||
				!update.WorkerStableBeforeUpdate {
				t.Fatalf(
					"extension update observation is invalid: before=%q ready=%t objects=%q after=%q worker_stable=%t",
					update.VersionBeforeUpdate,
					update.ReadyBeforeUpdate,
					update.ExtensionObjectsStateBeforeUpdate,
					update.VersionAfterUpdate,
					update.WorkerStableBeforeUpdate,
				)
			}
			var sourceRequirement sql.NullInt64
			if err := admin.QueryRowContext(ctx,
				"SELECT source_requirement FROM synchro.sync_registry_generations WHERE generation = $1",
				retainedGeneration).Scan(&sourceRequirement); err != nil {
				t.Fatalf("observe retained source requirement after the update: %v", err)
			}
			if sourceRequirement != wantSourceRequirement {
				t.Fatalf("retained source requirement from %s = %+v, want %+v", origin.version, sourceRequirement, wantSourceRequirement)
			}

			catalogs, err := harness.ObserveExtensionCatalogs(ctx)
			if err != nil {
				t.Fatalf("observe extension catalogs: %v", err)
			}
			if onlyUpdated, onlyClean := extensionCatalogDifference(catalogs.Updated, catalogs.Clean); len(onlyUpdated) != 0 || len(onlyClean) != 0 {
				t.Fatalf(
					"extension objects updated from %s differ from a clean installation: differences=%d\nonly updated:\n%s\nonly clean:\n%s",
					origin.version,
					len(onlyUpdated)+len(onlyClean),
					strings.Join(firstLines(onlyUpdated, 20), "\n"),
					strings.Join(firstLines(onlyClean, 20), "\n"),
				)
			}
			t.Logf("extension catalog snapshot lines: updated=%d clean=%d", len(catalogs.Updated), len(catalogs.Clean))

			// The same client continues with its predecessor identity and cursors.
			if state := retainedClientState(); state != predecessorState {
				t.Fatalf("update from %s changed retained client state:\nbefore=%s\nafter=%s", origin.version, predecessorState, state)
			}
			replay, _ := issue49RawSync(t, ctx, harness.AdapterURL(), clientToken, "/sync/push", pushPayload)
			if replay.Status != firstPush.Status || !bytes.Equal(replay.Body, firstPush.Body) {
				t.Fatalf("push replay after the update from %s changed its response:\nfirst=%d %s\nreplay=%d %s",
					origin.version, firstPush.Status, firstPush.Body, replay.Status, replay.Body)
			}
			if state := retainedClientState(); state != predecessorState {
				t.Fatalf("push replay after the update from %s changed durable state:\nbefore=%s\nafter=%s", origin.version, predecessorState, state)
			}
			if origin.version == "0.4.0-rc.1" {
				for _, call := range []string{
					"synchro.synchro_register_assignment_function(NULL)",
					"synchro.synchro_register_assignment_function(NULL, NULL)",
					"synchro.synchro_register_assignment_function(NULL, 0)",
				} {
					var returnedNull bool
					if err := admin.QueryRowContext(ctx, "SELECT "+call+" IS NULL").Scan(&returnedNull); err != nil {
						t.Fatalf("register a NULL function name after the update: %v", err)
					}
					if !returnedNull {
						t.Fatalf("NULL-name registration returned a non-NULL result: %s", call)
					}
					if after := observeAssignmentRegistration(); after != predecessorRegistration {
						t.Fatalf("NULL-name registration changed retained state: %s\nbefore=%s\nafter=%s", call, predecessorRegistration, after)
					}
				}
				var registered bool
				if err := admin.QueryRowContext(ctx, `
					SELECT synchro.synchro_register_assignment_function('public.cf_assigned_scopes', NULL) IS NOT NULL`).Scan(&registered); err != nil {
					t.Fatalf("register the retained function without a bound: %v", err)
				}
				if !registered {
					t.Fatal("unbounded registration returned a NULL result")
				}
				var unbounded, sameDefinition bool
				if err := admin.QueryRowContext(ctx, `
					SELECT max_scopes IS NULL,
					       (to_jsonb(registration) - 'max_scopes' - 'registered_at') =
					       ($1::jsonb - 'max_scopes' - 'registered_at')
					FROM synchro.sync_assignment_function registration
					WHERE singleton`, predecessorRegistration).Scan(&unbounded, &sameDefinition); err != nil {
					t.Fatalf("observe the retained unbounded registration: %v", err)
				}
				if !unbounded || !sameDefinition {
					t.Fatalf("unbounded registration changed its retained function: unbounded=%t same_definition=%t", unbounded, sameDefinition)
				}
			}
			requireOnlyChange := func(response map[string]any, recordID, value, version string) map[string]any {
				t.Helper()
				changes, ok := response["changes"].([]any)
				if !ok || len(changes) != 1 {
					t.Fatalf("pull after the update from %s returned %d changes, want only record %s: %v", origin.version, len(changes), recordID, response["changes"])
				}
				change, _ := changes[0].(map[string]any)
				pk, _ := change["pk"].(map[string]any)
				row, _ := change["row"].(map[string]any)
				if change["scope"] != clientUserScope || change["table"] != clientTable.ID || change["op"] != "upsert" ||
					len(pk) != 1 || pk[clientTable.PrimaryKeyField] != recordID || row[clientTable.ValueField] != value ||
					change["server_version"] != version {
					t.Fatalf("pull after the update from %s returned %v, want record %s value %s version %s", origin.version, change, recordID, value, version)
				}
				return change
			}
			pushedChange := requireOnlyChange(pullRealClient(t, ctx, harness, clientToken, updateClient),
				pushedID, "predecessor-push", pushedChanges.Records[1].RowVersion)
			if !reflect.DeepEqual(pushedChange["row"], pushOutcome["server_row"]) ||
				!reflect.DeepEqual(pushedChange["row_checksum"], pushOutcome["row_checksum"]) {
				t.Fatalf("pull after the update from %s differs from the push outcome: pull=%v push=%v", origin.version, pushedChange, pushOutcome)
			}
			if err := harness.Source().ExecContext(ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, 'successor-source')",
				successorID, clientUser); err != nil {
				t.Fatalf("insert successor client row: %v", err)
			}
			waitForRealWALRecords(t, ctx, harness, "cf_items", successorID)
			successorChanges, err := harness.Operator().ObserveWALRecordsForTable(ctx, "cf_items", []string{successorID})
			if err != nil || len(successorChanges.Records) != 1 {
				t.Fatalf("observe successor WAL change: %#v, %v", successorChanges, err)
			}
			requireOnlyChange(pullRealClient(t, ctx, harness, clientToken, updateClient),
				successorID, "successor-source", successorChanges.Records[0].RowVersion)
			acknowledgeRealClientCursors(t, ctx, harness, clientToken, updateClient)

			// Unknown legacy requirements and recorded requirement 2 both need
			// operator bootstrap before Class 3 activation.
			retainedGenerationState := func() (state, class string, pending int64) {
				t.Helper()
				if err := admin.QueryRowContext(ctx, `
					SELECT generation.state,
					       COALESCE((SELECT transition_class FROM synchro.sync_schema_manifest
					                 ORDER BY schema_version DESC LIMIT 1), ''),
					       (SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending')
					FROM synchro.sync_registry_generations generation
					WHERE generation.generation = $1`, retainedGeneration).Scan(&state, &class, &pending); err != nil {
					t.Fatalf("observe retained generation after the update: %v", err)
				}
				return state, class, pending
			}
			if state, _, pending := retainedGenerationState(); state != "pending" || pending != 1 {
				t.Fatalf("retained generation from %s activated without a bootstrap: state=%q pending=%d", origin.version, state, pending)
			}
			if _, err := harness.Operator().RunProjectionBootstrap(ctx, retainedGeneration); err != nil {
				t.Fatalf("bootstrap the retained generation from %s: %v; %s", origin.version, err, harness.FailureDiagnostics())
			}
			retainedState, retainedClass, pendingGenerations := retainedGenerationState()
			for deadline := time.Now().Add(30 * time.Second); retainedState == "pending" && time.Now().Before(deadline); {
				time.Sleep(50 * time.Millisecond)
				retainedState, retainedClass, pendingGenerations = retainedGenerationState()
			}
			if retainedState != "active" || retainedClass != "class_3" || pendingGenerations != 0 {
				t.Fatalf("retained generation from %s after the bootstrap: state=%q class=%q pending=%d, want active class_3 with no pending generation",
					origin.version, retainedState, retainedClass, pendingGenerations)
			}
			var retainedFieldID string
			if err := admin.QueryRowContext(ctx, `
				SELECT field.field_id::text
				FROM synchro.sync_registry_fields field
				JOIN synchro.sync_registry registry
				  ON registry.registry_generation = field.registry_generation
				 AND registry.relation_id = field.relation_id
				WHERE field.registry_generation = $1
				  AND registry.table_name = 'cf_items'
				  AND field.physical_column = 'retained_note'`, retainedGeneration).Scan(&retainedFieldID); err != nil {
				t.Fatalf("observe retained field identity: %v", err)
			}

			createNoteSiblingImpact(t, ctx, admin)
			if err := declareNoteSiblingImpact(ctx, admin, "id", "document_id", "author_id", "deleted_at"); err != nil {
				t.Fatalf("declare a same-table impact after the update from %s: %v", origin.version, err)
			}
			waitForReleaseImpactRegistration(t, ctx, admin, "cf_document_notes_team_impact")
			wantDependencies := append(slices.Clone(originDependencies), "cf_document_notes>cf_document_notes:cf_document_notes_team_impact")
			slices.Sort(wantDependencies)
			if got := activeMembershipDependencies(t, ctx, admin); !slices.Equal(got, wantDependencies) {
				t.Fatalf("membership declarations after the update from %s = %v, want %v", origin.version, got, wantDependencies)
			}

			afterID := "00000000-0000-4000-8c07-000000000002"
			if err := harness.Source().ExecContext(
				ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
				afterID,
				"diagnostic-user",
				"after-extension-update",
			); err != nil {
				t.Fatalf("insert source row after extension update: %v", err)
			}
			waitForRealWALRecords(t, ctx, harness, "cf_items", afterID)

			client := connectRealProtocolClient(t, ctx, harness, token, "extension-update-after")
			records, _ := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c07-000000000011")
			table := requireRealTable(t, client, "cf_items")
			requireRebuildRecordVersion(t, records, table, beforeID, "before-extension-update")
			requireRebuildRecordVersion(t, records, table, afterID, "after-extension-update")
			retainedTable := table
			retainedTable.ValueField = retainedFieldID
			requireRebuildRecordVersion(t, records, retainedTable, beforeID, retainedValue)
		})
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close extension update harness for %s: %v", origin.version, err)
		}
		closeCancel()
		cancel()
	}
}

// activeMembershipDependencies returns each active declaration as
// "dependency>target:impact function" in sorted order.
func activeMembershipDependencies(t *testing.T, ctx context.Context, admin *sql.DB) []string {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		SELECT source.physical_relation::text || '>' || target.physical_relation::text || ':' ||
		       dependency.impact_function_name::text
		FROM synchro.sync_membership_dependencies dependency
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = dependency.registry_generation AND generation.state = 'active'
		JOIN synchro.sync_registry source
		  ON source.registry_generation = dependency.registry_generation
		 AND source.relation_id = dependency.dependency_relation_id
		JOIN synchro.sync_registry target
		  ON target.registry_generation = dependency.registry_generation
		 AND target.relation_id = dependency.target_relation_id
		ORDER BY 1`)
	if err != nil {
		t.Fatalf("observe membership declarations: %v", err)
	}
	defer rows.Close()
	var declarations []string
	for rows.Next() {
		var declaration string
		if err := rows.Scan(&declaration); err != nil {
			t.Fatalf("read membership declaration: %v", err)
		}
		declarations = append(declarations, declaration)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read membership declarations: %v", err)
	}
	return declarations
}

// TestRealExtensionUpdateRepairsRetainedDecoderPoison proves SYNC-WAL-005 and
// SYNC-WAL-006 with a decoder poison that the update baseline creates. The
// baseline decoder rejects the Relation message that pgoutput resends after a
// valid column type change. The updated extension repairs that same source
// transaction on retry, and it then decodes another valid Relation refresh
// without a new poison or restart.
func TestRealExtensionUpdateRepairsRetainedDecoderPoison(t *testing.T) {
	// The published 0.3.1 decoder rejects a valid Relation refresh. A change of
	// the migration floor must reconsider this case, so another pinned version
	// is a setup failure and not a later wait for a poison that cannot occur.
	const affectedBaselineVersion = "0.3.1"
	if version := readUpdateBaselineVersion(t); version != affectedBaselineVersion {
		t.Fatalf("retained decoder poison requires update baseline %s, found %s", affectedBaselineVersion, version)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	baseline := realUpdateOrigins(t)[0]
	baselineVersion := baseline.version
	harness := provisionRealUpdateHarness(t, ctx, baseline)
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign retained-poison update token: %v", err)
	}
	const (
		prefixID  = "00000000-0000-4000-8c07-000000000021"
		poisonID  = "00000000-0000-4000-8c07-000000000022"
		laterID   = "00000000-0000-4000-8c07-000000000023"
		warmID    = "00000000-0000-4000-8c07-000000000024"
		changedID = "00000000-0000-4000-8c07-000000000025"
	)
	values := map[string]string{
		prefixID:  "retained-poison-prefix",
		poisonID:  "retained-poison-source",
		laterID:   "retained-poison-later",
		warmID:    "retained-poison-warm",
		changedID: "retained-poison-refresh",
	}
	insertSourceRow := func(t *testing.T, recordID string) {
		t.Helper()
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
			recordID,
			values[recordID],
		); err != nil {
			t.Fatalf("insert retained-poison source row: %v", err)
		}
	}
	// One source transaction changes the value column type and writes a row.
	// pgoutput then resends the cf_items Relation message before that row.
	commitTypeChange := func(t *testing.T, columnType, recordID string) {
		t.Helper()
		transaction, err := openIssue49Admin(t, ctx, harness).BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin value type change: %v", err)
		}
		defer transaction.Rollback()
		if _, err := transaction.ExecContext(ctx, "ALTER TABLE public.cf_items ALTER COLUMN value TYPE "+columnType); err != nil {
			t.Fatalf("change value column type: %v", err)
		}
		if _, err := transaction.ExecContext(
			ctx,
			"INSERT INTO public.cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
			recordID,
			values[recordID],
		); err != nil {
			t.Fatalf("insert row after value type change: %v", err)
		}
		if err := transaction.Commit(); err != nil {
			t.Fatalf("commit value type change: %v", err)
		}
	}

	insertSourceRow(t, prefixID)
	waitForRealWALRecords(t, ctx, harness, "cf_items", prefixID)
	prefix, err := harness.Operator().ObserveWALRecords(ctx, []string{prefixID})
	if err != nil || len(prefix.Records) != 1 || !prefix.ContiguousAcknowledged || !prefix.SlotMatchesAcknowledgement {
		t.Fatalf("establish exact baseline WAL prefix: observation=%#v err=%v", prefix, err)
	}
	prefixEndLSN := prefix.Records[0].EndLSN
	// The baseline decoder has cached the text column from the prefix row.
	commitTypeChange(t, "varchar(256)", poisonID)
	insertSourceRow(t, laterID)
	before := waitForIssue49Poison(t, ctx, harness, laterID)
	beforeAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, openIssue49Admin(t, ctx, harness), before.CommitLSN)

	t.Run("assertion", func(t *testing.T) {
		if before.FailureClass != "decode_failed" || before.CommitLSN == "" ||
			before.RelationID != "" || before.RelationIDMatchesRegistry || !before.AcknowledgementBlocked ||
			before.LaterRecordMaterialized || !before.LaterFencePending || !before.WorkerBlocked ||
			!before.ReadinessBlocked || !before.PoisonCheckFailed {
			t.Fatalf("baseline decoder did not persist a blocking decode poison: %#v", before)
		}
		if !beforeAcknowledgement.SlotMatchesProgress || !beforeAcknowledgement.ProgressAtOrBeforePoison ||
			!beforeAcknowledgement.SlotAtOrBeforePoison || beforeAcknowledgement.ProgressEndLSN != prefixEndLSN ||
			beforeAcknowledgement.SlotFlushLSN != prefixEndLSN {
			t.Fatalf("baseline slot advanced past the poisoned contiguous prefix: prefix=%s acknowledgement=%#v", prefixEndLSN, beforeAcknowledgement)
		}

		update, err := harness.ApplyExtensionUpdate(ctx)
		if err != nil {
			t.Fatalf("apply extension update over retained poison: %v", err)
		}
		if update.VersionBeforeUpdate != baselineVersion || update.VersionAfterUpdate != release.Version {
			t.Fatalf("retained-poison update versions are invalid: before=%q after=%q", update.VersionBeforeUpdate, update.VersionAfterUpdate)
		}
		afterUpdate := waitForIssue49Poison(t, ctx, harness, laterID)
		afterUpdateAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, openIssue49Admin(t, ctx, harness), afterUpdate.CommitLSN)
		if afterUpdate.FailureClass != before.FailureClass || afterUpdate.CommitLSN != before.CommitLSN ||
			afterUpdate.RelationID != before.RelationID || !afterUpdate.AcknowledgementBlocked ||
			afterUpdate.LaterRecordMaterialized || !afterUpdate.LaterFencePending ||
			afterUpdateAcknowledgement != beforeAcknowledgement {
			t.Fatalf("retained poison changed across the extension update: before=%#v after=%#v acknowledgement=%#v/%#v",
				before, afterUpdate, beforeAcknowledgement, afterUpdateAcknowledgement)
		}

		retried, err := harness.Operator().RetryWALPoison(ctx)
		if err != nil || !retried {
			t.Fatalf("request retained poison retry: requested=%t err=%v", retried, err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", poisonID, laterID)
		recovery, err := harness.Operator().ObserveWALPoisonRecovery(ctx, poisonID)
		if err != nil {
			t.Fatalf("observe retained poison recovery: %v", err)
		}
		if recovery.PoisonCount != 1 || recovery.FailureClass != "decode_failed" || recovery.Lifecycle != "repaired" ||
			recovery.AttemptCount != 2 || !recovery.RetryRequested || !recovery.Resolved || !recovery.SameCommitPosition {
			t.Fatalf("retained poison did not repair the same WAL identity: %#v", recovery)
		}
		recovered, err := harness.Operator().ObserveWALRecords(ctx, []string{poisonID, laterID})
		if err != nil || len(recovered.Records) != 2 || recovered.Records[0].RecordID != poisonID ||
			recovered.Records[1].RecordID != laterID || recovered.BlockingPoison || !recovered.ContiguousAcknowledged ||
			!recovered.SlotMatchesAcknowledgement || recovered.AcknowledgedEndLSN == "" ||
			recovered.AcknowledgedEndLSN != recovered.SlotConfirmedFlushLSN ||
			recovered.ProcessedEndLSN != recovered.AcknowledgedEndLSN {
			t.Fatalf("logical slot did not acknowledge the exact recovered contiguous end LSN: %#v, %v", recovered, err)
		}
		if err := harness.FinishExtensionUpdate(ctx); err != nil {
			t.Fatalf("finish extension update after poison repair: %v", err)
		}
		if retried, err := harness.Operator().RetryWALPoison(ctx); err != nil || retried {
			t.Fatalf("completed poison entered ordinary retry: requested=%t err=%v", retried, err)
		}

		// A restart loads current catalog metadata, so the repair alone does not
		// prove Relation refresh. The same warm worker must decode another change.
		insertSourceRow(t, warmID)
		waitForRealWALRecords(t, ctx, harness, "cf_items", warmID)
		restarts := harness.RestartCount()
		workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
		if err != nil {
			t.Fatalf("observe warm WAL worker: %v", err)
		}
		controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
		if err != nil {
			t.Fatalf("create retained-poison WAL controller: %v", err)
		}
		resumeWAL, err := controller.PauseWALMaterialization(ctx)
		if err != nil {
			t.Fatalf("pause WAL materialization before the Relation refresh: %v", err)
		}
		walPaused := true
		defer func() {
			if !walPaused {
				return
			}
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := resumeWAL(cleanupContext); err != nil {
				t.Errorf("resume WAL materialization during cleanup: %v", err)
			}
		}()
		commitTypeChange(t, "text", changedID)
		walPaused = false
		if err := resumeWAL(ctx); err != nil {
			t.Fatalf("resume WAL materialization after the Relation refresh: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", changedID)
		refreshed, err := harness.Operator().ObserveWALPoisonRecovery(ctx, poisonID)
		if err != nil {
			t.Fatalf("observe poison state after the Relation refresh: %v", err)
		}
		currentPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
		if err != nil || refreshed.PoisonCount != 1 || refreshed.Lifecycle != "repaired" ||
			harness.RestartCount() != restarts || currentPID != workerPID {
			t.Fatalf("valid Relation refresh required a new poison or restart: recovery=%#v restarts=%d/%d worker=%d/%d err=%v",
				refreshed, restarts, harness.RestartCount(), workerPID, currentPID, err)
		}

		client := connectRealProtocolClient(t, ctx, harness, token, "extension-update-retained-poison")
		records, _ := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c07-000000000031")
		table := requireRealTable(t, client, "cf_items")
		for _, recordID := range []string{prefixID, poisonID, laterID, warmID, changedID} {
			requireRebuildRecordVersion(t, records, table, recordID, values[recordID])
		}
	})
}

type realUpdateOrigin struct {
	version  string
	artifact string
}

// realUpdateOrigins returns the pinned baseline and each later pinned released
// origin with its published bundle. A missing bundle is a setup failure.
func realUpdateOrigins(t *testing.T) []realUpdateOrigin {
	t.Helper()
	baselineArtifact := os.Getenv("SYNCHRO_CONFORMANCE_UPDATE_BASELINE_EXTENSION_ARTIFACT")
	originArtifacts := os.Getenv("SYNCHRO_CONFORMANCE_UPDATE_ORIGIN_EXTENSION_ARTIFACTS")
	if baselineArtifact == "" || originArtifacts == "" {
		t.Fatal("extension update origin artifacts are unavailable")
	}
	origins := []realUpdateOrigin{{version: readUpdateBaselineVersion(t), artifact: baselineArtifact}}
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "extensions", "synchro-pg", "update-origins.json"))
	if err != nil {
		t.Fatalf("read extension update origins: %v", err)
	}
	var pins struct {
		Origins []struct {
			Version string `json:"version"`
		} `json:"origins"`
	}
	if err := json.Unmarshal(data, &pins); err != nil || len(pins.Origins) == 0 {
		t.Fatal("extension update origins are invalid")
	}
	entries, err := os.ReadDir(originArtifacts)
	if err != nil {
		t.Fatalf("read extension update origin artifacts: %v", err)
	}
	if len(entries) != len(pins.Origins) {
		t.Fatalf("extension update origin artifacts = %d, want %d pinned origins", len(entries), len(pins.Origins))
	}
	for _, pin := range pins.Origins {
		if !extensionVersionPattern.MatchString(pin.Version) {
			t.Fatal("extension update origin version is not in X.Y.Z or X.Y.Z-rc.N form")
		}
		origins = append(origins, realUpdateOrigin{version: pin.Version, artifact: filepath.Join(originArtifacts, pin.Version)})
	}
	return origins
}

// provisionRealUpdateHarness provisions an owned instance with the extension
// bundle of one update origin.
func provisionRealUpdateHarness(t *testing.T, ctx context.Context, origin realUpdateOrigin) *blackbox.Harness {
	t.Helper()
	if !*provision || !*install {
		t.Fatal("real proof requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load extension update environment: %v", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment:                     environment,
		UpdateBaselineExtensionArtifact: origin.artifact,
		UpdateBaselineExtensionVersion:  origin.version,
	})
	if err != nil {
		t.Fatalf("provision extension update harness for %s: %v", origin.version, err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close extension update harness: %v", err)
		}
	})
	return harness
}

// extensionCatalogDifference returns the lines that only one sorted list has.
// It counts repeated lines.
func extensionCatalogDifference(updated, clean []string) ([]string, []string) {
	var onlyUpdated, onlyClean []string
	updatedIndex, cleanIndex := 0, 0
	for updatedIndex < len(updated) && cleanIndex < len(clean) {
		switch {
		case updated[updatedIndex] == clean[cleanIndex]:
			updatedIndex++
			cleanIndex++
		case updated[updatedIndex] < clean[cleanIndex]:
			onlyUpdated = append(onlyUpdated, updated[updatedIndex])
			updatedIndex++
		default:
			onlyClean = append(onlyClean, clean[cleanIndex])
			cleanIndex++
		}
	}
	onlyUpdated = append(onlyUpdated, updated[updatedIndex:]...)
	onlyClean = append(onlyClean, clean[cleanIndex:]...)
	return onlyUpdated, onlyClean
}

func firstLines(lines []string, limit int) []string {
	if len(lines) > limit {
		return lines[:limit]
	}
	return lines
}

var extensionVersionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.([1-9][0-9]*))?$`)

func readUpdateBaselineVersion(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "extensions", "synchro-pg", "update-baseline.json"))
	if err != nil {
		t.Fatalf("read extension update baseline: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		t.Fatal("extension update baseline is not one JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatal("extension update baseline contains trailing data")
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "artifact_sha256,artifact_url,version" {
		t.Fatalf("extension update baseline keys = %v, want artifact_sha256, artifact_url, and version", keys)
	}
	var version string
	if err := json.Unmarshal(fields["version"], &version); err != nil || !extensionVersionPattern.MatchString(version) {
		t.Fatal("extension update baseline version is not in X.Y.Z or X.Y.Z-rc.N form")
	}
	return version
}

type extensionUpdatePath struct {
	source  string
	target  string
	hasPath bool
}

func readExtensionUpdatePaths(t *testing.T, ctx context.Context, database *sql.DB) []extensionUpdatePath {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT source, target, path IS NOT NULL
		FROM pg_catalog.pg_extension_update_paths('synchro_pg')`)
	if err != nil {
		t.Fatalf("observe extension update paths: %v", err)
	}
	defer rows.Close()
	var paths []extensionUpdatePath
	for rows.Next() {
		var path extensionUpdatePath
		if err := rows.Scan(&path.source, &path.target, &path.hasPath); err != nil {
			t.Fatalf("read extension update path: %v", err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read extension update paths: %v", err)
	}
	return paths
}

// extensionUpdatePathViolation returns an empty string when the update paths
// form one valid chain from the baseline to the current version.
func extensionUpdatePathViolation(paths []extensionUpdatePath, baseline, current string) string {
	baselineNumber, baselineValid := parseExtensionVersion(baseline)
	currentNumber, currentValid := parseExtensionVersion(current)
	if !baselineValid || !currentValid {
		return fmt.Sprintf("baseline %q or current version %q is not in X.Y.Z or X.Y.Z-rc.N form", baseline, current)
	}
	known := make(map[string]struct{})
	reachesCurrent := make(map[string]bool)
	for _, path := range paths {
		known[path.source] = struct{}{}
		known[path.target] = struct{}{}
		if path.target == current && path.hasPath {
			reachesCurrent[path.source] = true
		}
	}
	if current == baseline {
		if len(known) != 0 {
			return fmt.Sprintf("current version %s is the baseline, but %d versions are known", current, len(known))
		}
		return ""
	}
	if _, ok := known[baseline]; !ok {
		return fmt.Sprintf("baseline version %s is not known", baseline)
	}
	if _, ok := known[current]; !ok {
		return fmt.Sprintf("current version %s is not known", current)
	}
	versions := make([]string, 0, len(known))
	for version := range known {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	for _, version := range versions {
		number, valid := parseExtensionVersion(version)
		if !valid {
			return fmt.Sprintf("known version %q is not in X.Y.Z or X.Y.Z-rc.N form", version)
		}
		if compareExtensionVersions(number, baselineNumber) < 0 {
			return fmt.Sprintf("known version %s is below baseline %s", version, baseline)
		}
		if compareExtensionVersions(number, currentNumber) > 0 {
			return fmt.Sprintf("known version %s is above current version %s", version, current)
		}
		if compareExtensionVersions(number, currentNumber) < 0 && !reachesCurrent[version] {
			return fmt.Sprintf("known version %s has no update path to current version %s", version, current)
		}
	}
	return ""
}

// parseExtensionVersion returns major, minor, patch, release rank, and release
// candidate number. The rank orders a release after each of its candidates.
func parseExtensionVersion(version string) ([5]int, bool) {
	var number [5]int
	match := extensionVersionPattern.FindStringSubmatch(version)
	if match == nil {
		return number, false
	}
	if match[4] == "" {
		number[3] = 1
		match[4] = "0"
	}
	for index, slot := range []int{0, 1, 2, 4} {
		value, err := strconv.Atoi(match[index+1])
		if err != nil {
			return number, false
		}
		number[slot] = value
	}
	return number, true
}

func compareExtensionVersions(left, right [5]int) int {
	for index := range left {
		if left[index] != right[index] {
			if left[index] < right[index] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func TestExtensionUpdatePathViolation(t *testing.T) {
	tests := []struct {
		name      string
		paths     []extensionUpdatePath
		baseline  string
		current   string
		violation bool
	}{
		{
			name: "valid chain",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline: "0.3.1",
			current:  "0.3.2",
		},
		{
			name: "valid chain through release candidates",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2-rc.9", hasPath: true},
				{source: "0.3.1", target: "0.3.2-rc.10", hasPath: true},
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2-rc.9", target: "0.3.2-rc.10", hasPath: true},
				{source: "0.3.2-rc.9", target: "0.3.2", hasPath: true},
				{source: "0.3.2-rc.10", target: "0.3.2", hasPath: true},
			},
			baseline: "0.3.1",
			current:  "0.3.2",
		},
		{
			name: "release candidate above current release candidate",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2-rc.9", hasPath: true},
				{source: "0.3.1", target: "0.3.2-rc.10", hasPath: true},
				{source: "0.3.2-rc.9", target: "0.3.2-rc.10", hasPath: false},
				{source: "0.3.2-rc.10", target: "0.3.2-rc.9", hasPath: true},
			},
			baseline:  "0.3.1",
			current:   "0.3.2-rc.9",
			violation: true,
		},
		{
			name: "known version below baseline",
			paths: []extensionUpdatePath{
				{source: "0.3.0", target: "0.3.1", hasPath: true},
				{source: "0.3.0", target: "0.3.2", hasPath: true},
				{source: "0.3.1", target: "0.3.0", hasPath: false},
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2", target: "0.3.0", hasPath: false},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name: "missing path to current version",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: false},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name: "known version above current version",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.1", target: "0.3.10", hasPath: true},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
				{source: "0.3.2", target: "0.3.10", hasPath: true},
				{source: "0.3.10", target: "0.3.1", hasPath: false},
				{source: "0.3.10", target: "0.3.2", hasPath: true},
			},
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name:      "no versions after baseline",
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name:     "no versions at baseline",
			baseline: "0.3.1",
			current:  "0.3.1",
		},
		{
			name: "baseline version not known",
			paths: []extensionUpdatePath{
				{source: "0.3.2", target: "0.3.3", hasPath: true},
				{source: "0.3.3", target: "0.3.2", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.3",
			violation: true,
		},
		{
			name: "versions known at baseline",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.1",
			violation: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			violation := extensionUpdatePathViolation(test.paths, test.baseline, test.current)
			if (violation != "") != test.violation {
				t.Fatalf("violation = %q, want violation %t", violation, test.violation)
			}
		})
	}
}
