package integration

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealReleaseScopeMembershipDeterminism(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	anchorID := "00000000-0000-4000-8f01-000000000100"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		anchorID,
		"diagnostic-user",
		"release-scope-worker-anchor",
	); err != nil {
		t.Fatalf("insert scope worker anchor: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", anchorID)

	priorSchema := loadRealSchemaTableReference(t, ctx, harness, "cf_items")
	if err := harness.Operator().ConfigureCrossScopeTable(ctx); err != nil {
		t.Fatalf("configure deterministic cross-scope table: %v", err)
	}
	crossScopeConfigured := true
	t.Cleanup(func() {
		if !crossScopeConfigured {
			return
		}
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := harness.Operator().RestoreCrossScopeTable(cleanupContext); err != nil {
			t.Errorf("restore deterministic cross-scope table: %v", err)
		}
	})
	waitForReleaseSchemaChange(t, ctx, harness, priorSchema.Schema)
	crossScopeSchema := loadRealSchemaTableReference(t, ctx, harness, "cf_items")

	firstClient := connectRealProtocolClient(
		t, ctx, harness, token, "release-scope-determinism-one",
		"cf:dedup", "cf:global", "user:diagnostic-user",
	)
	secondClient := connectRealProtocolClient(
		t, ctx, harness, token, "release-scope-determinism-two",
		"cf:dedup", "cf:global", "user:diagnostic-user",
	)
	for index, client := range []*realProtocolClient{firstClient, secondClient} {
		rebuildRealScope(
			t, ctx, harness, token, client, "user:diagnostic-user",
			fmt.Sprintf("00000000-0000-4000-8f01-%012d", index*3+1),
		)
		rebuildRealScope(
			t, ctx, harness, token, client, "cf:global",
			fmt.Sprintf("00000000-0000-4000-8f01-%012d", index*3+2),
		)
		rebuildRealScope(
			t, ctx, harness, token, client, "cf:dedup",
			fmt.Sprintf("00000000-0000-4000-8f01-%012d", index*3+3),
		)
	}

	recordID := "00000000-0000-4000-8f01-000000000101"
	const value = "release-deterministic-membership"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		value,
	); err != nil {
		t.Fatalf("insert deterministic membership row: %v", err)
	}
	waitForRealWALEffects(t, ctx, harness, "cf_items", 2, recordID)

	const expectedMembership = "cf:dedup,user:diagnostic-user"
	for attempt := 0; attempt < 5; attempt++ {
		var actual string
		if err := admin.QueryRowContext(ctx, `
			SELECT COALESCE(string_agg(scope_id, ',' ORDER BY scope_id), '')
			FROM public.cf_items_cross_scope_membership($1::uuid) AS scope_id`,
			recordID,
		).Scan(&actual); err != nil {
			t.Fatalf("evaluate deterministic membership attempt %d: %v", attempt+1, err)
		}
		if actual != expectedMembership {
			t.Fatalf("membership evaluation attempt %d = %q, want %q", attempt+1, actual, expectedMembership)
		}
	}

	for index, client := range []*realProtocolClient{firstClient, secondClient} {
		response := pullRealClient(t, ctx, harness, token, client)
		changes := requireRealChanges(t, response)
		if len(changes) != 2 || response["has_more"] != false {
			t.Fatalf("deterministic pull %d = %#v", index+1, response)
		}
		table := requireRealTable(t, client, "cf_items")
		requireRealPullChange(t, changes, "cf:dedup", table, recordID, value)
		requireRealPullChange(t, changes, "user:diagnostic-user", table, recordID, value)
	}

	if err := harness.Operator().RestoreCrossScopeTable(ctx); err != nil {
		t.Fatalf("restore cross-scope registration before fail-closed control: %v", err)
	}
	crossScopeConfigured = false
	waitForReleaseSchemaChange(t, ctx, harness, crossScopeSchema.Schema)

	if _, err := admin.ExecContext(ctx, `
		CREATE FUNCTION public.release_scope_error_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT CASE
				WHEN (p.owner_id #>> '{}') = 'release-scope-error'
				THEN (1 / pg_catalog.length(pg_catalog.replace(p.owner_id #>> '{}', p.owner_id #>> '{}', '')))::text
				ELSE 'user:' || (p.owner_id #>> '{}')
			END
			FROM synchro_projection.cf_late_registration AS p
			WHERE p.record_id = p_id::text AND NOT p.deleted;
		END;
		REVOKE ALL ON FUNCTION public.release_scope_error_membership(uuid) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.release_scope_error_membership(uuid)
			TO synchro_owner, synchro_worker;
		SELECT synchro.synchro_register_table(
			'public.cf_late_registration',
			'public.release_scope_error_membership',
			'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled'
		)`,
	); err != nil {
		t.Fatalf("register fail-closed membership function: %v", err)
	}
	ensureReleaseLateRegistrationActive(t, ctx, harness, admin)

	errorRecordID := "00000000-0000-4000-8f01-000000000102"
	laterRecordID := "00000000-0000-4000-8f01-000000000103"
	var beforeFailureLSN, afterFailureLSN string
	if err := admin.QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&beforeFailureLSN); err != nil {
		t.Fatalf("observe WAL boundary before failed source transaction: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)",
		errorRecordID,
		"release-scope-error",
		"must-not-become-empty-membership",
	); err != nil {
		t.Fatalf("insert membership evaluation failure row: %v", err)
	}
	if err := admin.QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&afterFailureLSN); err != nil {
		t.Fatalf("observe WAL boundary after failed source transaction: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		laterRecordID,
		"diagnostic-user",
		"must-remain-after-membership-failure",
	); err != nil {
		t.Fatalf("insert row after membership evaluation failure: %v", err)
	}
	poison := waitForIssue49Poison(t, ctx, harness, laterRecordID)

	var poisonMatches, fencePending, acknowledgementBlocked, noEdges, noEffects bool
	if err := admin.QueryRowContext(ctx, `
		WITH late_relation AS (
			SELECT registry.relation_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active'
			  AND registry.physical_schema = 'public'
			  AND registry.physical_relation = 'cf_late_registration'
		), failed_fence AS (
			SELECT fence.*
			FROM synchro.sync_write_fences fence
			JOIN late_relation relation ON relation.relation_id = fence.relation_id
			WHERE fence.new_record_id = $1
		)
		SELECT poison.failure_class = 'scope_evaluation_failed'
		           AND poison.relation_id IS NOT DISTINCT FROM relation.relation_id
		           AND poison.commit_lsn > $2::pg_lsn
		           AND poison.commit_lsn <= $3::pg_lsn,
		       fence.coverage = 'pending'
		           AND fence.commit_lsn IS NULL
		           AND fence.event_ordinal IS NULL,
		       COALESCE(progress.acknowledged_end_lsn < poison.commit_lsn, true),
		       NOT EXISTS (
		           SELECT 1 FROM synchro.sync_bucket_edges edge
		           WHERE edge.relation_id = relation.relation_id AND edge.record_id = $1
		       ),
		       NOT EXISTS (
		           SELECT 1 FROM synchro.sync_changelog effect
		           WHERE effect.relation_id = relation.relation_id AND effect.record_id = $1
		       )
		FROM synchro.sync_wal_poison poison
		CROSS JOIN synchro.sync_wal_progress progress
		CROSS JOIN late_relation relation
		CROSS JOIN failed_fence fence
		WHERE poison.lifecycle = 'active' AND progress.singleton`,
		errorRecordID, beforeFailureLSN, afterFailureLSN,
	).Scan(
		&poisonMatches,
		&fencePending,
		&acknowledgementBlocked,
		&noEdges,
		&noEffects,
	); err != nil {
		t.Fatalf("observe fail-closed membership result: %v", err)
	}
	if poison.FailureClass != "scope_evaluation_failed" || !poison.WorkerBlocked ||
		!poison.LaterFencePending || poison.LaterRecordMaterialized ||
		!poisonMatches || !fencePending || !acknowledgementBlocked || !noEdges || !noEffects {
		t.Fatalf(
			"membership evaluation failure became visible as empty membership: poison=%#v matches=%t pending=%t blocked=%t no_edges=%t no_effects=%t",
			poison, poisonMatches, fencePending, acknowledgementBlocked, noEdges, noEffects,
		)
	}
}

func TestRealReleaseRegisteredFunctionContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	assertReleaseFunctionPrivileges(t, ctx, admin, "public.cf_items_membership(uuid)", "public.cf_items")
	if _, err := admin.ExecContext(ctx, `
		SELECT synchro.synchro_register_table(
			'public.cf_items',
			'public.cf_items_membership',
			'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled'
		)`,
	); err != nil {
		t.Fatalf("register valid deterministic membership function: %v", err)
	}

	var targetTableID string
	if err := admin.QueryRowContext(ctx, `
		SELECT registry.table_id::text
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active'
		  AND registry.physical_schema = 'public'
		  AND registry.physical_relation = 'cf_items'`,
	).Scan(&targetTableID); err != nil {
		t.Fatalf("load impact target table identity: %v", err)
	}

	if _, err := admin.ExecContext(ctx, fmt.Sprintf(`
		CREATE FUNCTION public.release_impact_valid(p_old_row jsonb, p_new_row jsonb)
		RETURNS SETOF synchro.synchro_row_ref
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT ROW('%[1]s'::uuid, 'string', pg_catalog.to_jsonb(item.record_id))::synchro.synchro_row_ref
			FROM synchro_projection.cf_items AS item
			WHERE NOT item.deleted
			  AND item.owner_id #>> '{}' IN (p_old_row ->> 'scope_key', p_new_row ->> 'scope_key');
		END;
	`, targetTableID)); err != nil {
		t.Fatalf("create valid deterministic impact function: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		REVOKE ALL ON FUNCTION public.release_impact_valid(jsonb, jsonb) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.release_impact_valid(jsonb, jsonb)
			TO synchro_owner, synchro_worker`,
	); err != nil {
		t.Fatalf("set valid deterministic impact function privileges: %v", err)
	}
	assertReleaseFunctionPrivileges(
		t, ctx, admin, "public.release_impact_valid(jsonb,jsonb)", "public.cf_item_impacts",
	)

	membershipRestrictions := []struct {
		name       string
		returns    string
		language   string
		volatility string
		security   string
		searchPath string
		definition string
	}{
		{
			name:     "language",
			language: "plpgsql",
			definition: `AS $function$
				BEGIN
					RETURN QUERY SELECT 'user:diagnostic-user'::text;
				END
				$function$`,
		},
		{name: "volatility", volatility: "VOLATILE"},
		{name: "security", security: "SECURITY DEFINER"},
		{name: "search_path", searchPath: "pg_catalog"},
		{name: "signature", returns: "text"},
		{
			name: "undeclared",
			definition: `BEGIN ATOMIC
				SELECT 'user:' || item.owner_id
				FROM public.cf_items AS item
				WHERE item.id = p_id AND item.deleted_at IS NULL;
				END`,
		},
	}
	for _, restriction := range membershipRestrictions {
		if _, err := admin.ExecContext(ctx, fmt.Sprintf(`
			CREATE FUNCTION public.release_membership_%s(p_id uuid)
			RETURNS %s
			LANGUAGE %s %s %s
			SET search_path = %s
			%s`,
			restriction.name,
			cmp.Or(restriction.returns, "SETOF text"),
			cmp.Or(restriction.language, "SQL"),
			cmp.Or(restriction.volatility, "STABLE"),
			cmp.Or(restriction.security, "SECURITY INVOKER"),
			cmp.Or(restriction.searchPath, "pg_catalog, synchro"),
			cmp.Or(restriction.definition, `BEGIN ATOMIC SELECT 'user:diagnostic-user'::text; END`),
		)); err != nil {
			t.Fatalf("create membership %s restriction control: %v", restriction.name, err)
		}
		signature := "public.release_membership_" + restriction.name + "(uuid)"
		if _, err := admin.ExecContext(
			ctx,
			"REVOKE ALL ON FUNCTION "+signature+" FROM PUBLIC; "+
				"GRANT EXECUTE ON FUNCTION "+signature+" TO synchro_owner, synchro_worker",
		); err != nil {
			t.Fatalf("set membership %s restriction privileges: %v", restriction.name, err)
		}
		assertReleaseFunctionPrivileges(t, ctx, admin, signature, "public.cf_items")
	}

	impactRestrictions := []struct {
		name       string
		arguments  string
		signature  string
		language   string
		volatility string
		security   string
		searchPath string
		definition string
	}{
		{
			name:     "language",
			language: "plpgsql",
			definition: fmt.Sprintf(`AS $function$
				BEGIN
					RETURN QUERY
					SELECT ROW('%s'::uuid, 'string', pg_catalog.to_jsonb(item.record_id))::synchro.synchro_row_ref
					FROM synchro_projection.cf_items AS item
					WHERE false;
				END
				$function$`, targetTableID),
		},
		{name: "volatility", volatility: "VOLATILE"},
		{name: "security", security: "SECURITY DEFINER"},
		{name: "search_path", searchPath: "pg_catalog"},
		{
			name:      "signature",
			arguments: "p_old_row jsonb",
			signature: "jsonb",
		},
		{
			name: "undeclared",
			definition: fmt.Sprintf(`BEGIN ATOMIC
				SELECT ROW('%s'::uuid, 'string', pg_catalog.to_jsonb(item.id::text))::synchro.synchro_row_ref
				FROM public.cf_items AS item
				WHERE false;
				END`, targetTableID),
		},
	}
	for _, restriction := range impactRestrictions {
		definition := restriction.definition
		if definition == "" {
			definition = fmt.Sprintf(`BEGIN ATOMIC
				SELECT ROW('%s'::uuid, 'string', pg_catalog.to_jsonb(item.record_id))::synchro.synchro_row_ref
				FROM synchro_projection.cf_items AS item WHERE false;
				END`, targetTableID)
		}
		if _, err := admin.ExecContext(ctx, fmt.Sprintf(`
			CREATE FUNCTION public.release_impact_%s(%s)
			RETURNS SETOF synchro.synchro_row_ref
			LANGUAGE %s %s %s
			SET search_path = %s
			%s`,
			restriction.name,
			cmp.Or(restriction.arguments, "p_old_row jsonb, p_new_row jsonb"),
			cmp.Or(restriction.language, "SQL"),
			cmp.Or(restriction.volatility, "STABLE"),
			cmp.Or(restriction.security, "SECURITY INVOKER"),
			cmp.Or(restriction.searchPath, "pg_catalog, synchro"),
			definition,
		)); err != nil {
			t.Fatalf("create impact %s restriction control: %v", restriction.name, err)
		}
		signature := "public.release_impact_" + restriction.name + "(" + cmp.Or(restriction.signature, "jsonb,jsonb") + ")"
		if _, err := admin.ExecContext(
			ctx,
			"REVOKE ALL ON FUNCTION "+signature+" FROM PUBLIC; "+
				"GRANT EXECUTE ON FUNCTION "+signature+" TO synchro_owner, synchro_worker",
		); err != nil {
			t.Fatalf("set impact %s restriction privileges: %v", restriction.name, err)
		}
		assertReleaseFunctionPrivileges(t, ctx, admin, signature, "public.cf_item_impacts")
	}

	if _, err := admin.ExecContext(ctx, `
		SELECT synchro.synchro_register_membership_dependency(
			'cf_item_impacts',
			'cf_items',
			'public.release_impact_valid',
			ARRAY['id', 'scope_key']::text[],
			1000
		)`,
	); err != nil {
		t.Fatalf("register valid deterministic impact function: %v", err)
	}
	waitForReleaseImpactRegistration(t, ctx, admin, "release_impact_valid")

	membershipCases := []struct {
		name      string
		function  string
		extraArgs string
	}{
		{"language", "public.release_membership_language", ""},
		{"volatility", "public.release_membership_volatility", ""},
		{"security", "public.release_membership_security", ""},
		{"search_path", "public.release_membership_search_path", ""},
		{"signature", "public.release_membership_signature", ""},
		{"unqualified_identity", "cf_items_membership", ""},
		{"undeclared_dependency", "public.release_membership_undeclared", ""},
		{"positive_bound", "public.cf_items_membership", ", p_max_scope_fanout => 0"},
	}
	for _, testCase := range membershipCases {
		t.Run("membership_"+testCase.name, func(t *testing.T) {
			before := loadReleaseRegistrySnapshot(t, ctx, admin)
			statement := fmt.Sprintf(`
				SELECT synchro.synchro_register_table(
					'public.cf_items',
					'%s',
					'single_scope',
					'id', 'updated_at', 'deleted_at', 'enabled'%s
				)`,
				testCase.function,
				testCase.extraArgs,
			)
			if _, err := admin.ExecContext(ctx, statement); err == nil {
				t.Fatalf("membership contract accepted %s", testCase.name)
			}
			after := loadReleaseRegistrySnapshot(t, ctx, admin)
			if before != after {
				t.Fatalf("membership rejection %s changed registry state: before=%#v after=%#v", testCase.name, before, after)
			}
		})
	}

	impactCases := []struct {
		name     string
		function string
		bound    int
	}{
		{"language", "public.release_impact_language", 1000},
		{"volatility", "public.release_impact_volatility", 1000},
		{"security", "public.release_impact_security", 1000},
		{"search_path", "public.release_impact_search_path", 1000},
		{"signature", "public.release_impact_signature", 1000},
		{"unqualified_identity", "release_impact_valid", 1000},
		{"undeclared_dependency", "public.release_impact_undeclared", 1000},
		{"positive_bound", "public.release_impact_valid", 0},
	}
	for _, testCase := range impactCases {
		t.Run("impact_"+testCase.name, func(t *testing.T) {
			before := loadReleaseRegistrySnapshot(t, ctx, admin)
			if _, err := admin.ExecContext(ctx, `
				SELECT synchro.synchro_register_membership_dependency(
					'cf_item_impacts',
					'cf_items',
					$1,
					ARRAY['id', 'scope_key']::text[],
					$2
				)`,
				testCase.function,
				testCase.bound,
			); err == nil {
				t.Fatalf("impact contract accepted %s", testCase.name)
			}
			after := loadReleaseRegistrySnapshot(t, ctx, admin)
			if before != after {
				t.Fatalf("impact rejection %s changed registry state: before=%#v after=%#v", testCase.name, before, after)
			}
		})
	}
}

func TestRealReleaseDependencyReassignmentRetainsVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	documentID := "00000000-0000-4000-8f03-000000000001"
	memberID := "00000000-0000-4000-8f03-000000000002"
	accessID := "00000000-0000-4000-8f03-000000000003"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, $2, $3)",
		documentID,
		"release-owner",
		"release dependency reassignment",
	); err != nil {
		t.Fatalf("insert dependency source document: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_members (id, document_id, member_id) VALUES ($1, $2, $3)",
		memberID,
		documentID,
		"release-member",
	); err != nil {
		t.Fatalf("insert dependency target member: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, $3)",
		accessID,
		documentID,
		"release-access-old",
	); err != nil {
		t.Fatalf("insert dependency ownership row: %v", err)
	}
	waitForMembershipBuckets(
		t,
		ctx,
		harness,
		memberID,
		[]string{"user:release-access-old", "user:release-member", "user:release-owner"},
	)
	sourceVersion := loadReleaseRowVersion(t, ctx, admin, "cf_document_members", memberID)
	baselineEvents := loadReleaseDependencyEvents(t, ctx, admin, accessID, "0/0")
	if len(baselineEvents) != 1 ||
		baselineEvents[0].BeforeOwner != "" ||
		baselineEvents[0].AfterOwner != "release-access-old" {
		t.Fatalf("initial captured dependency event is invalid: %#v", baselineEvents)
	}

	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		t.Fatalf("create dependency replay controller: %v", err)
	}
	resumeWAL, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		t.Fatalf("pause dependency WAL materialization: %v", err)
	}
	walPaused := true
	defer func() {
		if !walPaused {
			return
		}
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := resumeWAL(cleanupContext); err != nil {
			t.Errorf("resume dependency WAL materialization: %v", err)
		}
	}()

	if err := harness.Source().ExecContext(
		ctx,
		"UPDATE cf_document_access SET owner_id = $2 WHERE id = $1",
		accessID,
		"release-access-new",
	); err != nil {
		t.Fatalf("reassign dependency ownership: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"DELETE FROM cf_document_access WHERE id = $1",
		accessID,
	); err != nil {
		t.Fatalf("delete dependency ownership row: %v", err)
	}
	pausedBuckets, err := harness.Operator().ObserveMembershipBuckets(ctx, "cf_document_members", memberID)
	if err != nil {
		t.Fatalf("observe paused dependency membership: %v", err)
	}
	if !reflect.DeepEqual(
		pausedBuckets,
		[]string{"user:release-access-old", "user:release-member", "user:release-owner"},
	) {
		t.Fatalf("dependency membership changed while WAL materialization was paused: %v", pausedBuckets)
	}
	if err := resumeWAL(ctx); err != nil {
		t.Fatalf("resume dependency WAL materialization: %v", err)
	}
	walPaused = false
	waitForMembershipBuckets(
		t,
		ctx,
		harness,
		memberID,
		[]string{"user:release-member", "user:release-owner"},
	)
	events := loadReleaseDependencyEvents(t, ctx, admin, accessID, baselineEvents[0].CommitLSN)
	if len(events) != 2 ||
		events[0].BeforeOwner != "release-access-old" ||
		events[0].AfterOwner != "release-access-new" ||
		events[1].BeforeOwner != "release-access-new" ||
		events[1].AfterOwner != "" {
		t.Fatalf("captured dependency transaction images are invalid: %#v", events)
	}
	reassignment := loadReleaseDependencyEffects(t, ctx, admin, events[0], memberID)
	assertReleaseDependencyEffects(
		t,
		reassignment,
		sourceVersion,
		map[string]int16{
			"user:release-access-old": 3,
			"user:release-access-new": 1,
		},
	)
	revocation := loadReleaseDependencyEffects(t, ctx, admin, events[1], memberID)
	assertReleaseDependencyEffects(
		t,
		revocation,
		sourceVersion,
		map[string]int16{"user:release-access-new": 3},
	)
	if current := loadReleaseRowVersion(t, ctx, admin, "cf_document_members", memberID); current != sourceVersion {
		t.Fatalf("dependency delete changed target source version: before=%s after=%s", sourceVersion, current)
	}
}

func TestRealReleaseWALOrdinalGapsPreserveCommitOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	recordID := "00000000-0000-4000-8f04-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"release-ordinal-baseline",
	); err != nil {
		t.Fatalf("insert WAL ordinal baseline: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	baseline, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
	if err != nil || len(baseline.Records) != 1 {
		t.Fatalf("observe WAL ordinal baseline: observation=%#v err=%v", baseline, err)
	}

	later, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin later-committing lower-ordinal transaction: %v", err)
	}
	defer later.Rollback()
	// The source API accepts DML only. This update assigns an XID without a row event.
	assigned, err := later.ExecContext(
		ctx,
		"UPDATE cf_items SET value = value WHERE id = $1 AND pg_catalog.pg_current_xact_id() IS NULL",
		recordID,
	)
	if err != nil {
		_ = later.Rollback()
		t.Fatalf("assign later-committing transaction XID: %v", err)
	}
	rowsAffected, err := assigned.RowsAffected()
	if err != nil || rowsAffected != 0 {
		_ = later.Rollback()
		t.Fatalf("XID assignment emitted a row event: rows=%d err=%v", rowsAffected, err)
	}

	first, err := harness.Source().BeginTx(ctx)
	if err != nil {
		_ = later.Rollback()
		t.Fatalf("begin repeated-row ordinal-gap transaction: %v", err)
	}
	defer first.Rollback()
	for index := 0; index < 2; index++ {
		if _, err := first.ExecContext(
			ctx,
			"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
			fmt.Sprintf("00000000-0000-4000-8f04-%012d", index+101),
			fmt.Sprintf("release-ordinal-two-gap-%d", index),
		); err != nil {
			_ = first.Rollback()
			t.Fatalf("insert ordinal-two gap event %d: %v", index, err)
		}
	}
	if _, err := first.ExecContext(
		ctx,
		"UPDATE cf_items SET value = $2, updated_at = clock_timestamp() WHERE id = $1",
		recordID,
		"release-ordinal-two",
	); err != nil {
		_ = first.Rollback()
		t.Fatalf("update target at source ordinal two: %v", err)
	}
	for index := 0; index < 4; index++ {
		if _, err := first.ExecContext(
			ctx,
			"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
			fmt.Sprintf("00000000-0000-4000-8f04-%012d", index+103),
			fmt.Sprintf("release-ordinal-seven-gap-%d", index),
		); err != nil {
			_ = first.Rollback()
			t.Fatalf("insert ordinal-seven gap event %d: %v", index, err)
		}
	}
	if _, err := first.ExecContext(
		ctx,
		"UPDATE cf_items SET value = $2, updated_at = clock_timestamp() WHERE id = $1",
		recordID,
		"release-ordinal-seven",
	); err != nil {
		_ = first.Rollback()
		t.Fatalf("update target at source ordinal seven: %v", err)
	}
	if err := first.Commit(); err != nil {
		_ = later.Rollback()
		t.Fatalf("commit repeated-row ordinal-gap transaction: %v", err)
	}

	if _, err := later.ExecContext(
		ctx,
		"UPDATE cf_items SET value = $2, updated_at = clock_timestamp() WHERE id = $1",
		recordID,
		"release-later-ordinal-zero",
	); err != nil {
		_ = later.Rollback()
		t.Fatalf("update target in later transaction at source ordinal zero: %v", err)
	}
	if err := later.Commit(); err != nil {
		t.Fatalf("commit later lower-ordinal transaction: %v", err)
	}

	records := waitForReleaseWALRecordsAfter(t, ctx, admin, recordID, baseline.Records[0].CommitLSN, 2)
	if records[0].EventOrdinal != 7 || records[0].EffectOrdinal != 0 ||
		records[1].EventOrdinal != 0 || records[1].EffectOrdinal != 0 {
		t.Fatalf("consolidated and later source ordinals are invalid: %#v", records)
	}
	var ordered bool
	if err := admin.QueryRowContext(
		ctx,
		"SELECT $1::pg_lsn < $2::pg_lsn",
		records[0].CommitLSN,
		records[1].CommitLSN,
	).Scan(&ordered); err != nil {
		t.Fatalf("compare actual commit LSNs: %v", err)
	}
	if !ordered {
		t.Fatalf("target updates are not in commit-LSN order: %#v", records)
	}
	var startedInReverseCommitOrder bool
	if err := admin.QueryRowContext(ctx, `
		SELECT earlier.source_xid::text::bigint > later.source_xid::text::bigint
		FROM synchro.sync_wal_transactions earlier
		CROSS JOIN synchro.sync_wal_transactions later
		WHERE earlier.commit_lsn = $1::pg_lsn
		  AND later.commit_lsn = $2::pg_lsn`,
		records[0].CommitLSN,
		records[1].CommitLSN,
	).Scan(&startedInReverseCommitOrder); err != nil {
		t.Fatalf("compare source XID order with commit order: %v", err)
	}
	if !startedInReverseCommitOrder {
		t.Fatalf("source XID order did not oppose commit order: %#v", records)
	}
	assertReleaseTransactionOrdinals(t, ctx, admin, records[0], "0,1,2,3,4,5,6,7", 8)
	assertReleaseRepeatedRowProjections(
		t,
		ctx,
		admin,
		recordID,
		records[0],
		baseline.Records[0].RowVersion,
	)
	assertReleaseTransactionOrdinals(t, ctx, admin, records[1], "0", 1)

	finalState, err := harness.Operator().ObserveItemStateMatch(
		ctx,
		recordID,
		"release-later-ordinal-zero",
		records[1].RowVersion,
	)
	if err != nil {
		t.Fatalf("observe final WAL ordinal row: %v", err)
	}
	if !finalState.Live || !finalState.ValueMatches || !finalState.VersionMatches {
		t.Fatalf("later commit did not win over the earlier higher ordinal: %#v", finalState)
	}
}

type releaseRegistrySnapshot struct {
	GenerationCount int64
	RegistryCount   int64
	DependencyCount int64
	StageCount      int64
	Maximum         int64
}

type releaseDependencyEffect struct {
	BucketID      string
	Operation     int16
	EventOrdinal  int64
	EffectOrdinal int32
	RowVersion    string
}

type releaseDependencyEvent struct {
	StreamGeneration string
	CommitLSN        string
	EventOrdinal     int64
	BeforeOwner      string
	AfterOwner       string
}

type releaseWALRecord struct {
	CommitLSN          string
	EndLSN             string
	EventOrdinal       int64
	EffectOrdinal      int32
	RowVersion         string
	RegistryGeneration int64
}

type releaseProjectionImage struct {
	EventOrdinal int64
	ImageKind    string
	Value        string
	RowVersion   string
}

func waitForReleaseSchemaChange(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	priorSchema map[string]any,
) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		current, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_items")
		lastErr = err
		if err == nil && !sameRealSchemaReference(current.Schema, priorSchema) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(
		"wait for release schema change from %#v: last_error=%v; %s",
		priorSchema,
		lastErr,
		harness.FailureDiagnostics(),
	)
}

func assertReleaseFunctionPrivileges(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	signature string,
	relation string,
) {
	t.Helper()
	var valid bool
	if err := admin.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT procedure.proowner, procedure.proacl, namespace.oid AS schema_oid
			FROM pg_catalog.pg_proc procedure
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
			WHERE procedure.oid = pg_catalog.to_regprocedure($1)
		), roles AS (
			SELECT (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'synchro_owner') AS owner_oid,
			       (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'synchro_worker') AS worker_oid
		)
		SELECT EXISTS (
			SELECT 1
			FROM selected
			CROSS JOIN roles
			WHERE selected.proowner = (
				SELECT relation.relowner FROM pg_catalog.pg_class relation
				WHERE relation.oid = $2::regclass
			)
			  AND pg_catalog.has_schema_privilege(roles.owner_oid, selected.schema_oid, 'USAGE')
			  AND pg_catalog.has_schema_privilege(roles.worker_oid, selected.schema_oid, 'USAGE')
			  AND EXISTS (
				  SELECT 1
				  FROM pg_catalog.aclexplode(
					  COALESCE(selected.proacl, pg_catalog.acldefault('f', selected.proowner))
				  ) acl
				  WHERE acl.grantee = roles.owner_oid AND acl.privilege_type = 'EXECUTE'
			  )
			  AND EXISTS (
				  SELECT 1
				  FROM pg_catalog.aclexplode(
					  COALESCE(selected.proacl, pg_catalog.acldefault('f', selected.proowner))
				  ) acl
				  WHERE acl.grantee = roles.worker_oid AND acl.privilege_type = 'EXECUTE'
			  )
			  AND NOT EXISTS (
				  SELECT 1
				  FROM pg_catalog.aclexplode(
					  COALESCE(selected.proacl, pg_catalog.acldefault('f', selected.proowner))
				  ) acl
				  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'
			  )
		)`,
		signature,
		relation,
	).Scan(&valid); err != nil {
		t.Fatalf("inspect deterministic function privileges for %s: %v", signature, err)
	}
	if !valid {
		t.Fatalf("deterministic function privilege baseline is invalid for %s", signature)
	}
}

func loadReleaseRegistrySnapshot(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
) releaseRegistrySnapshot {
	t.Helper()
	var snapshot releaseRegistrySnapshot
	if err := admin.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM synchro.sync_registry_generations),
		       (SELECT count(*) FROM synchro.sync_registry),
		       (SELECT count(*) FROM synchro.sync_membership_dependencies),
		       (SELECT count(*) FROM synchro.sync_registry_membership_stages),
		       (SELECT max(generation) FROM synchro.sync_registry_generations)`,
	).Scan(
		&snapshot.GenerationCount,
		&snapshot.RegistryCount,
		&snapshot.DependencyCount,
		&snapshot.StageCount,
		&snapshot.Maximum,
	); err != nil {
		t.Fatalf("load registry atomicity snapshot: %v", err)
	}
	return snapshot
}

func waitForReleaseImpactRegistration(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	functionName string,
) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var active bool
		err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_membership_dependencies dependency
				JOIN synchro.sync_registry_generations generation
				  ON generation.generation = dependency.registry_generation
				WHERE generation.state = 'active'
				  AND dependency.impact_function_schema = 'public'
				  AND dependency.impact_function_name = $1
			)`,
			functionName,
		).Scan(&active)
		if err != nil {
			t.Fatalf("observe valid impact function activation: %v", err)
		}
		if active {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("valid impact function %s did not activate", functionName)
}

func ensureReleaseLateRegistrationActive(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	admin *sql.DB,
) {
	t.Helper()
	active := func() (bool, error) {
		var present bool
		err := admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_registry registry
				JOIN synchro.sync_registry_generations generation
				  ON generation.generation = registry.registry_generation
				WHERE generation.state = 'active'
				  AND registry.physical_schema = 'public'
				  AND registry.physical_relation = 'cf_late_registration'
			)`,
		).Scan(&present)
		return present, err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		present, err := active()
		if err != nil {
			t.Fatalf("observe fail-closed membership activation: %v", err)
		}
		if present {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	generation, err := harness.Operator().PendingLateSourceRegistryGeneration(ctx)
	if err != nil {
		t.Fatalf("load fail-closed membership generation: %v", err)
	}
	if _, err := harness.Operator().RunProjectionBootstrap(ctx, generation); err != nil {
		t.Fatalf("activate fail-closed membership function: %v; %s", err, harness.FailureDiagnostics())
	}
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		present, err := active()
		if err != nil {
			t.Fatalf("observe bootstrapped fail-closed membership activation: %v", err)
		}
		if present {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("fail-closed membership registration did not activate")
}

func loadReleaseRowVersion(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	tableName string,
	recordID string,
) string {
	t.Helper()
	var version string
	if err := admin.QueryRowContext(ctx, `
		SELECT version.row_version::text
		FROM synchro.sync_row_versions version
		JOIN synchro.sync_registry registry ON registry.relation_id = version.relation_id
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active'
		  AND registry.table_name = $1
		  AND version.record_id = $2`,
		tableName,
		recordID,
	).Scan(&version); err != nil {
		t.Fatalf("load retained source version for %s/%s: %v", tableName, recordID, err)
	}
	if !uuidPattern.MatchString(version) {
		t.Fatalf("retained source version is not an opaque UUID: %q", version)
	}
	return version
}

func loadReleaseDependencyEvents(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	sourceRecordID string,
	afterCommitLSN string,
) []releaseDependencyEvent {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		SELECT projection.stream_generation,
		       projection.commit_lsn::text,
		       projection.event_ordinal,
		       COALESCE(
		           max(projection.row_data ->> 'owner_id')
		               FILTER (WHERE projection.image_kind = 'before'),
		           ''
		       ),
		       COALESCE(
		           max(projection.row_data ->> 'owner_id')
		               FILTER (WHERE projection.image_kind = 'after'),
		           ''
		       )
		FROM synchro.sync_capture_dependency_projections projection
		JOIN synchro.sync_registry registry
		  ON registry.registry_generation = projection.registry_generation
		 AND registry.relation_id = projection.relation_id
		WHERE registry.physical_schema = 'public'
		  AND registry.physical_relation = 'cf_document_access'
		  AND projection.capture_key ->> 'id' = $1
		  AND projection.commit_lsn > $2::pg_lsn
		GROUP BY projection.stream_generation,
		         projection.commit_lsn,
		         projection.event_ordinal
		ORDER BY projection.commit_lsn, projection.event_ordinal`,
		sourceRecordID,
		afterCommitLSN,
	)
	if err != nil {
		t.Fatalf("load dependency source events: %v", err)
	}
	defer rows.Close()
	var events []releaseDependencyEvent
	for rows.Next() {
		var event releaseDependencyEvent
		if err := rows.Scan(
			&event.StreamGeneration,
			&event.CommitLSN,
			&event.EventOrdinal,
			&event.BeforeOwner,
			&event.AfterOwner,
		); err != nil {
			t.Fatalf("scan dependency source event: %v", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read dependency source events: %v", err)
	}
	return events
}

func loadReleaseDependencyEffects(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	sourceEvent releaseDependencyEvent,
	targetRecordID string,
) []releaseDependencyEffect {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		WITH target AS (
			SELECT registry.relation_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active'
			  AND registry.table_name = 'cf_document_members'
		)
		SELECT effect.bucket_id,
		       effect.operation,
		       effect.event_ordinal,
		       effect.effect_ordinal,
		       effect.row_version::text
		FROM synchro.sync_changelog effect
		JOIN target ON target.relation_id = effect.relation_id
		WHERE effect.stream_generation = $1
		  AND effect.commit_lsn = $2::pg_lsn
		  AND effect.event_ordinal = $3
		  AND effect.record_id = $4
		ORDER BY effect.effect_ordinal`,
		sourceEvent.StreamGeneration,
		sourceEvent.CommitLSN,
		sourceEvent.EventOrdinal,
		targetRecordID,
	)
	if err != nil {
		t.Fatalf("load dependency reassignment effects: %v", err)
	}
	defer rows.Close()
	var effects []releaseDependencyEffect
	for rows.Next() {
		var effect releaseDependencyEffect
		if err := rows.Scan(
			&effect.BucketID,
			&effect.Operation,
			&effect.EventOrdinal,
			&effect.EffectOrdinal,
			&effect.RowVersion,
		); err != nil {
			t.Fatalf("scan dependency reassignment effect: %v", err)
		}
		effects = append(effects, effect)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read dependency reassignment effects: %v", err)
	}
	return effects
}

func assertReleaseDependencyEffects(
	t *testing.T,
	effects []releaseDependencyEffect,
	sourceVersion string,
	expected map[string]int16,
) {
	t.Helper()
	if len(effects) != len(expected) {
		t.Fatalf("dependency effect count = %d, want %d: %#v", len(effects), len(expected), effects)
	}
	seen := make(map[string]int16, len(effects))
	for _, effect := range effects {
		if effect.RowVersion != sourceVersion {
			t.Fatalf("dependency effect changed target source version: %#v want=%s", effect, sourceVersion)
		}
		if effect.EffectOrdinal != 0 {
			t.Fatalf("single effect in scope did not start at ordinal zero: %#v", effects)
		}
		operation, ok := expected[effect.BucketID]
		if !ok || operation != effect.Operation {
			t.Fatalf("unexpected dependency effect: %#v expected=%#v", effect, expected)
		}
		seen[effect.BucketID] = effect.Operation
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatalf("dependency effects = %#v, want %#v", seen, expected)
	}
}

func waitForReleaseWALRecordsAfter(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	recordID string,
	baselineCommitLSN string,
	expected int,
) []releaseWALRecord {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var records []releaseWALRecord
	for time.Now().Before(deadline) {
		rows, err := admin.QueryContext(ctx, `
			SELECT change.commit_lsn::text,
			       transaction.end_lsn::text,
			       change.event_ordinal,
			       change.effect_ordinal,
			       change.row_version::text,
			       transaction.registry_generation
			FROM synchro.sync_changelog change
			JOIN synchro.sync_wal_transactions transaction
			  ON transaction.stream_generation = change.stream_generation
			 AND transaction.commit_lsn = change.commit_lsn
			WHERE change.table_name = 'cf_items'
			  AND change.record_id = $1
			  AND change.commit_lsn > $2::pg_lsn
			ORDER BY change.commit_lsn, change.event_ordinal, change.effect_ordinal`,
			recordID,
			baselineCommitLSN,
		)
		if err != nil {
			t.Fatalf("query repeated-row WAL records: %v", err)
		}
		records = nil
		for rows.Next() {
			var record releaseWALRecord
			if err := rows.Scan(
				&record.CommitLSN,
				&record.EndLSN,
				&record.EventOrdinal,
				&record.EffectOrdinal,
				&record.RowVersion,
				&record.RegistryGeneration,
			); err != nil {
				_ = rows.Close()
				t.Fatalf("scan repeated-row WAL record: %v", err)
			}
			records = append(records, record)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("read repeated-row WAL records: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close repeated-row WAL records: %v", err)
		}
		if len(records) > expected {
			t.Fatalf("repeated-row WAL record count = %d, want %d: %#v", len(records), expected, records)
		}
		if len(records) == expected {
			return records
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("wait for repeated-row WAL records: records=%#v", records)
	return nil
}

func assertReleaseRepeatedRowProjections(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	recordID string,
	consolidated releaseWALRecord,
	baselineVersion string,
) {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		WITH value_fields AS (
			SELECT field.registry_generation, field.relation_id, field.field_id::text
			FROM synchro.sync_registry_fields field
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = field.registry_generation
			 AND registry.relation_id = field.relation_id
			WHERE registry.table_name = 'cf_items'
			  AND field.physical_column = 'value'
		)
		SELECT projection.event_ordinal,
		       projection.image_kind,
		       projection.row_data ->> field.field_id,
		       projection.row_version::text
		FROM synchro.sync_captured_projections projection
		JOIN value_fields field
		  ON field.registry_generation = projection.registry_generation
		 AND field.relation_id = projection.relation_id
		WHERE projection.commit_lsn = $1::pg_lsn
		  AND projection.record_id = $2
		  AND projection.event_ordinal IN (2, 7)
		ORDER BY projection.event_ordinal,
		         CASE projection.image_kind WHEN 'before' THEN 0 ELSE 1 END`,
		consolidated.CommitLSN,
		recordID,
	)
	if err != nil {
		t.Fatalf("query repeated-row source projections: %v", err)
	}
	defer rows.Close()
	var images []releaseProjectionImage
	for rows.Next() {
		var image releaseProjectionImage
		if err := rows.Scan(
			&image.EventOrdinal,
			&image.ImageKind,
			&image.Value,
			&image.RowVersion,
		); err != nil {
			t.Fatalf("scan repeated-row source projection: %v", err)
		}
		images = append(images, image)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read repeated-row source projections: %v", err)
	}
	expected := []releaseProjectionImage{
		{EventOrdinal: 2, ImageKind: "before", Value: "release-ordinal-baseline"},
		{EventOrdinal: 2, ImageKind: "after", Value: "release-ordinal-two"},
		{EventOrdinal: 7, ImageKind: "before", Value: "release-ordinal-two"},
		{EventOrdinal: 7, ImageKind: "after", Value: "release-ordinal-seven"},
	}
	if len(images) != len(expected) {
		t.Fatalf("repeated-row source projection count = %d, want %d: %#v", len(images), len(expected), images)
	}
	for index := range expected {
		if images[index].EventOrdinal != expected[index].EventOrdinal ||
			images[index].ImageKind != expected[index].ImageKind ||
			images[index].Value != expected[index].Value ||
			!uuidPattern.MatchString(images[index].RowVersion) {
			t.Fatalf("repeated-row source projection %d = %#v, want %#v", index, images[index], expected[index])
		}
	}
	if images[0].RowVersion != baselineVersion {
		t.Fatalf("first repeated-row before image version = %s, want baseline %s", images[0].RowVersion, baselineVersion)
	}
	if images[1].RowVersion == images[3].RowVersion {
		t.Fatalf("same-transaction repeated updates reused a row version: %#v", images)
	}
	if images[3].RowVersion != consolidated.RowVersion {
		t.Fatalf(
			"consolidated ordinal-seven effect version = %s, want projection version %s",
			consolidated.RowVersion,
			images[3].RowVersion,
		)
	}
}

func assertReleaseTransactionOrdinals(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	record releaseWALRecord,
	expectedOrdinals string,
	expectedCount int64,
) {
	t.Helper()
	var eventCount int64
	var persistedCount int64
	var actualOrdinals string
	var targetProjectionMatches bool
	if err := admin.QueryRowContext(ctx, `
		SELECT transaction.event_count,
		       count(event.event_ordinal),
		       string_agg(event.event_ordinal::text, ',' ORDER BY event.event_ordinal),
		       EXISTS (
		           SELECT 1
		           FROM synchro.sync_captured_projections projection
		           JOIN synchro.sync_registry registry
		             ON registry.registry_generation = projection.registry_generation
		            AND registry.relation_id = projection.relation_id
		           WHERE projection.stream_generation = transaction.stream_generation
		             AND projection.commit_lsn = transaction.commit_lsn
		             AND projection.event_ordinal = $2
		             AND registry.table_name = 'cf_items'
		             AND projection.registry_generation = transaction.registry_generation
		       )
		FROM synchro.sync_wal_transactions transaction
		JOIN synchro.sync_wal_events event
		  ON event.stream_generation = transaction.stream_generation
		 AND event.commit_lsn = transaction.commit_lsn
		WHERE transaction.commit_lsn = $1::pg_lsn
		GROUP BY transaction.stream_generation,
		         transaction.commit_lsn,
		         transaction.event_count,
		         transaction.registry_generation`,
		record.CommitLSN,
		record.EventOrdinal,
	).Scan(
		&eventCount,
		&persistedCount,
		&actualOrdinals,
		&targetProjectionMatches,
	); err != nil {
		t.Fatalf("observe transaction source ordinals at %s: %v", record.CommitLSN, err)
	}
	if record.CommitLSN == "" || record.EndLSN == "" || record.RegistryGeneration <= 0 ||
		eventCount != expectedCount || persistedCount != expectedCount ||
		actualOrdinals != expectedOrdinals || !targetProjectionMatches ||
		!uuidPattern.MatchString(record.RowVersion) {
		t.Fatalf(
			"transaction ordinal evidence is incomplete: record=%#v event_count=%d persisted=%d ordinals=%q target_projection=%t",
			record, eventCount, persistedCount, actualOrdinals, targetProjectionMatches,
		)
	}
}
