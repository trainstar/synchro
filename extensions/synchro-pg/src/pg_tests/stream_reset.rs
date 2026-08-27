    fn configure_reset_test_slot(slot_name: &str) {
        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET active_slot_name = $1, updated_at = now()
             WHERE singleton",
            &[slot_name.into()],
        )
        .expect("configure reset test slot");
    }

    fn prepare_reset_for_test(candidate_slot: &str) -> serde_json::Value {
        Spi::connect_mut(|client| {
            crate::stream_reset::prepare_stream_reset_for_test(client, candidate_slot)
        })
        .expect("prepare test reset")
    }

    fn reset_id(value: &serde_json::Value) -> String {
        value["reset_id"]
            .as_str()
            .expect("reset identity")
            .to_string()
    }

    fn lock_and_stage_reset(reset_id: &str, candidate_slot: &str) {
        Spi::connect_mut(|client| {
            crate::stream_reset::lock_stream_reset_sources_for_test(client, reset_id)?;
            crate::stream_reset::stage_stream_reset_for_test(
                client,
                reset_id,
                candidate_slot,
                "0/10",
                "00000001-00000001-1",
            )?;
            Ok::<_, String>(())
        })
        .expect("lock and stage test reset");
    }

    #[pg_test]
    fn stream_reset_prepare_rejects_invalid_and_duplicate_candidates() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");

        let invalid = Spi::connect_mut(|client| {
            crate::stream_reset::prepare_stream_reset_for_test(client, "INVALID-SLOT")
        });
        assert!(invalid.is_err());

        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let duplicate = Spi::connect_mut(|client| {
            crate::stream_reset::prepare_stream_reset_for_test(client, "synchro_reset_other")
        });
        assert!(duplicate.is_err());

        let id = reset_id(&prepared);
        let aborted = Spi::connect_mut(|client| {
            crate::stream_reset::abort_stream_reset_for_test(client, &id)
        })
        .expect("abort prepared reset");
        assert_eq!(aborted["candidate_slot_name"], "synchro_reset_candidate");
    }

    #[pg_test]
    fn stream_reset_operator_can_lock_registered_sources() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);

        Spi::run("SET LOCAL ROLE synchro_operator").expect("select reset operator role");
        let locked: bool = Spi::get_one_with_args(
            "SELECT synchro.synchro_lock_stream_reset_sources($1::uuid)",
            &[id.as_str().into()],
        )
        .expect("lock sources as reset operator")
        .expect("reset operator lock result");
        Spi::run("SELECT pg_catalog.pg_advisory_unlock_all()")
            .expect("unlock reset operator session");
        Spi::run("RESET ROLE").expect("restore reset test role");
        assert!(locked);
    }

    #[pg_test]
    fn projection_bootstrap_runtime_reads_return_bounded_state() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");

        let active_stream: pgrx::JsonB = Spi::get_one(
            "SELECT synchro.synchro_projection_bootstrap_active_stream()",
        )
        .expect("read active projection bootstrap stream")
        .expect("active projection bootstrap stream");
        let stream_generation = active_stream.0["stream_generation"]
            .as_str()
            .expect("active stream generation")
            .to_string();
        assert_eq!(active_stream.0["active_slot_name"], "synchro_reset_old");

        let before_boundary: Option<bool> = Spi::get_one_with_args(
            "SELECT synchro.synchro_projection_bootstrap_main_boundary($1, '0/1')",
            &[stream_generation.as_str().into()],
        )
        .expect("read unset projection bootstrap boundary");
        assert_eq!(before_boundary, Some(false));
        Spi::run(
            "UPDATE synchro.sync_wal_progress
             SET materialized_commit_lsn = '0/1', materialized_end_lsn = '0/1'
             WHERE singleton",
        )
        .expect("set projection bootstrap boundary");
        let at_boundary: Option<bool> = Spi::get_one_with_args(
            "SELECT synchro.synchro_projection_bootstrap_main_boundary($1, '0/1')",
            &[stream_generation.as_str().into()],
        )
        .expect("read reached projection bootstrap boundary");
        assert_eq!(at_boundary, Some(true));

        let candidate_slot = "synchro_reset_candidate";
        let absent: Option<bool> = Spi::get_one_with_args(
            "SELECT synchro.synchro_projection_bootstrap_slot_absent($1)",
            &[candidate_slot.into()],
        )
        .expect("read absent projection bootstrap slot");
        assert_eq!(absent, Some(true));
        let slot_state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT synchro.synchro_projection_bootstrap_slot_drop_state($1)",
            &[candidate_slot.into()],
        )
        .expect("read absent projection bootstrap slot state")
        .expect("absent projection bootstrap slot state");
        assert_eq!(
            slot_state.0,
            serde_json::json!({"present": false, "active": false, "valid": true})
        );
        let aborted_slot: Option<String> = Spi::get_one(
            "SELECT synchro.synchro_projection_bootstrap_next_aborted_slot()",
        )
        .expect("read next aborted projection bootstrap slot");
        assert_eq!(aborted_slot, None);

        let interrupted: pgrx::JsonB = Spi::get_one(
            "SELECT synchro.synchro_projection_bootstrap_interrupted()",
        )
        .expect("read interrupted projection bootstrap")
        .expect("interrupted projection bootstrap state");
        assert_eq!(interrupted.0["present"], false);

        let invalid_slot_accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<bool>(
                "SELECT synchro.synchro_projection_bootstrap_slot_absent('INVALID-SLOT')",
            )
            .is_ok()
        }))
        .catch_others(|_| false)
        .execute();
        assert!(!invalid_slot_accepted, "invalid slot names must be rejected");
        let unknown_bootstrap_accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<bool>(
                "SELECT synchro.synchro_projection_bootstrap_is_activated(
                     '00000000-0000-4000-8000-000000000001'::uuid
                 )",
            )
            .is_ok()
        }))
        .catch_others(|_| false)
        .execute();
        assert!(
            !unknown_bootstrap_accepted,
            "unknown projection bootstraps must be rejected"
        );
    }

    #[pg_test]
    fn stream_reset_activation_installs_verified_baseline_atomically() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        register_client("u1", "c1");
        let record_id = "21000000-0000-4000-8000-000000000001";
        Spi::run_with_args(
            "INSERT INTO public.test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'reset baseline')",
            &[record_id.into()],
        )
        .expect("insert reset baseline source row");
        let checkpoint_count: i64 = Spi::get_one(
            "SELECT count(*) FROM synchro.sync_client_checkpoints
             WHERE user_id = 'u1' AND client_id = 'c1'",
        )
        .expect("count reset-invalidated checkpoints")
        .expect("reset-invalidated checkpoint count");
        assert!(checkpoint_count > 0);

        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);
        let target_generation = prepared["target_stream_generation"]
            .as_str()
            .expect("target stream generation")
            .to_string();
        lock_and_stage_reset(&id, "synchro_reset_candidate");

        Spi::run_with_args(
            "UPDATE synchro.sync_captured_rows
             SET row_data = jsonb_set(row_data, '{title}', to_jsonb('stale'::text))
             WHERE record_id = $1",
            &[record_id.into()],
        )
        .expect("mutate live projection before reset activation");

        let activated = Spi::connect_mut(|client| {
            crate::stream_reset::activate_stream_reset_for_test(client, &id)
        })
        .expect("activate staged reset");
        assert_eq!(activated["stream_generation"], target_generation);
        assert_eq!(activated["active_slot_name"], "synchro_reset_candidate");

        let state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'stream_generation', runtime.stream_generation,
                 'active_slot', runtime.active_slot_name,
                 'checkpoint_count', (SELECT count(*) FROM synchro.sync_client_checkpoints),
                 'event_count', (SELECT count(*) FROM synchro.sync_wal_events),
                 'effect_count', (SELECT count(*) FROM synchro.sync_changelog),
                 'captured_matches_stage', captured.row_data = staged.row_data,
                 'stale_key_absent', NOT captured.row_data ? 'title',
                 'source_reset_id', captured.source_reset_id,
                 'fence_coverage', fence.coverage,
                 'fence_reset_id', fence.reset_id,
                 'fence_commit_lsn', fence.commit_lsn,
                 'fence_event_ordinal', fence.event_ordinal
             )
             FROM synchro.sync_runtime_state runtime
             JOIN synchro.sync_captured_rows captured ON captured.record_id = $1
             JOIN synchro.sync_stream_reset_captured_rows staged
               ON staged.reset_id = captured.source_reset_id
              AND staged.relation_id = captured.relation_id
              AND staged.record_id = captured.record_id
             JOIN synchro.sync_write_fences fence ON fence.new_record_id = $1
             WHERE runtime.singleton",
            &[record_id.into()],
        )
        .expect("load activated reset state")
        .expect("activated reset state");
        assert_eq!(state.0["stream_generation"], target_generation);
        assert_eq!(state.0["active_slot"], "synchro_reset_candidate");
        assert_eq!(state.0["checkpoint_count"], 0);
        assert_eq!(state.0["event_count"], 0);
        assert_eq!(state.0["effect_count"], 0);
        assert_eq!(state.0["captured_matches_stage"], true);
        assert_eq!(state.0["stale_key_absent"], true);
        assert_eq!(state.0["source_reset_id"], id);
        assert_eq!(state.0["fence_coverage"], "reset_baseline");
        assert_eq!(state.0["fence_reset_id"], id);
        assert!(state.0["fence_commit_lsn"].is_null());
        assert!(state.0["fence_event_ordinal"].is_null());
    }

    #[pg_test]
    fn stream_reset_rejects_missing_fence_coverage() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        Spi::run(
            "INSERT INTO public.test_orders (
                 id, user_id, title
             ) VALUES (
                 '22000000-0000-4000-8000-000000000001', 'u1', 'coverage control'
             )",
        )
        .expect("insert reset coverage source row");
        let original_generation: String = Spi::get_one(
            "SELECT stream_generation FROM synchro.sync_runtime_state WHERE singleton",
        )
        .expect("load original stream generation")
        .expect("original stream generation");

        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);
        lock_and_stage_reset(&id, "synchro_reset_candidate");
        Spi::run_with_args(
            "DELETE FROM synchro.sync_stream_reset_fence_coverage
             WHERE reset_id = $1::uuid",
            &[id.as_str().into()],
        )
        .expect("remove staged fence coverage");

        let activation = Spi::connect_mut(|client| {
            crate::stream_reset::activate_stream_reset_for_test(client, &id)
        });
        assert!(activation.is_err());
        let current_generation: String = Spi::get_one(
            "SELECT stream_generation FROM synchro.sync_runtime_state WHERE singleton",
        )
        .expect("load unchanged stream generation")
        .expect("unchanged stream generation");
        assert_eq!(current_generation, original_generation);
    }

    #[pg_test]
    fn stream_reset_rejects_fence_added_after_staging() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        let record_id = "23000000-0000-4000-8000-000000000001";
        Spi::run_with_args(
            "INSERT INTO public.test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'late fence control')",
            &[record_id.into()],
        )
        .expect("insert late fence source row");

        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);
        lock_and_stage_reset(&id, "synchro_reset_candidate");
        Spi::run_with_args(
             "INSERT INTO synchro.sync_write_fences (
                  fence_id, transaction_xid, dml_ordinal, relation_id,
                  registration_kind, table_id,
                  physical_schema, physical_relation, physical_relation_oid,
                  operation, old_record_id, new_record_id, row_version
              )
              SELECT gen_random_uuid(), pg_current_xact_id(), 1, registry.relation_id,
                     registry.registration_kind, registry.table_id,
                     registry.physical_schema, registry.physical_relation,
                    registry.physical_relation_oid, 'insert', NULL, $1, gen_random_uuid()
             FROM synchro.sync_registry registry
             JOIN synchro.sync_registry_generations generation
               ON generation.generation = registry.registry_generation
             WHERE generation.state = 'active' AND registry.table_name = 'test_orders'",
            &[record_id.into()],
        )
        .expect("insert fence after reset staging");

        let activation = Spi::connect_mut(|client| {
            crate::stream_reset::activate_stream_reset_for_test(client, &id)
        });
        assert!(activation.is_err());
    }

    #[pg_test]
    fn stream_reset_fence_coverage_rejects_duplicates() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        Spi::run(
            "INSERT INTO public.test_orders (id, user_id, title)
             VALUES ('24000000-0000-4000-8000-000000000001', 'u1', 'duplicate control')",
        )
        .expect("insert duplicate coverage source row");

        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);
        lock_and_stage_reset(&id, "synchro_reset_candidate");
        let inserted: i64 = Spi::get_one_with_args(
            "WITH duplicate AS (
                 INSERT INTO synchro.sync_stream_reset_fence_coverage
                 SELECT * FROM synchro.sync_stream_reset_fence_coverage
                 WHERE reset_id = $1::uuid
                 ON CONFLICT DO NOTHING
                 RETURNING 1
             )
             SELECT count(*) FROM duplicate",
            &[id.as_str().into()],
        )
        .expect("count duplicate fence coverage")
        .expect("duplicate fence coverage count");
        assert_eq!(inserted, 0);
    }

    #[pg_test]
    fn stream_reset_abort_clears_stage_and_preserves_active_pointer() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);
        lock_and_stage_reset(&id, "synchro_reset_candidate");

        Spi::connect_mut(|client| {
            crate::stream_reset::abort_stream_reset_for_test(client, &id)
        })
        .expect("abort staged reset");
        let state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'lifecycle', reset.lifecycle,
                 'active_slot', runtime.active_slot_name,
                 'staged_rows', (
                     SELECT count(*) FROM synchro.sync_stream_reset_captured_rows
                     WHERE reset_id = $1::uuid
                 )
             )
             FROM synchro.sync_stream_resets reset
             CROSS JOIN synchro.sync_runtime_state runtime
             WHERE reset.reset_id = $1::uuid AND runtime.singleton",
            &[id.as_str().into()],
        )
        .expect("load aborted reset state")
        .expect("aborted reset state");
        assert_eq!(state.0["lifecycle"], "aborted");
        assert_eq!(state.0["active_slot"], "synchro_reset_old");
        assert_eq!(state.0["staged_rows"], 0);
    }

    #[pg_test]
    fn projection_bootstrap_abort_clears_catchup_boundary_and_stage() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        let bootstrap_id: String = Spi::get_one(
            "WITH active AS (
                 SELECT generation, stream_generation
                 FROM synchro.sync_registry_generations
                 WHERE state = 'active' AND validated
                 ORDER BY generation DESC LIMIT 1
             ), target AS (
                 INSERT INTO synchro.sync_registry_generations (
                     stream_generation, state, validated, parent_generation
                 )
                 SELECT stream_generation, 'pending', true, generation FROM active
                 RETURNING generation, stream_generation, parent_generation
             )
             INSERT INTO synchro.sync_stream_resets (
                 reset_id, operation_kind, source_stream_generation,
                 target_stream_generation, source_registry_generation,
                 target_registry_generation, old_slot_name, candidate_slot_name,
                 database_oid, database_name, plugin, consistent_point,
                 exported_snapshot_name, activation_barrier, lifecycle,
                 staged_row_count, staged_version_count, staged_edge_count,
                 staged_fence_count, staged_scope_count, baseline_staged_at
             )
             SELECT gen_random_uuid(), 'projection_bootstrap', stream_generation,
                    stream_generation, parent_generation, generation,
                    'synchro_reset_old', 'synchro_bootstrap_candidate',
                    database.oid, database.datname, 'pgoutput', '0/10',
                    'bootstrap-snapshot', '0/20', 'catching_up',
                    0, 0, 0, 0, 0, now()
             FROM target
             JOIN pg_catalog.pg_database database
               ON database.datname = pg_catalog.current_database()
             RETURNING reset_id::text",
        )
        .expect("create catching-up projection bootstrap")
        .expect("catching-up projection bootstrap identity");
        Spi::run_with_args(
            "INSERT INTO synchro.sync_stream_reset_scope_digests (
                 reset_id, scope_id, schema_hash, digest, row_count
             ) VALUES (
                 $1::uuid, 'abort-control', repeat('0', 64),
                 decode(repeat('00', 32), 'hex'), 0
             )",
            &[bootstrap_id.as_str().into()],
        )
        .expect("create projection bootstrap abort stage");

        let aborted: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT synchro.synchro_abort_projection_bootstrap($1::uuid)",
            &[bootstrap_id.as_str().into()],
        )
        .expect("abort catching-up projection bootstrap")
        .expect("projection bootstrap abort result");
        let state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'lifecycle', lifecycle,
                 'barrier_cleared', activation_barrier IS NULL,
                 'materialized_cleared', candidate_materialized_end_lsn IS NULL,
                 'acknowledged_cleared', candidate_acknowledged_end_lsn IS NULL,
                 'verified', candidate_verified,
                 'staged_scopes', (
                     SELECT count(*) FROM synchro.sync_stream_reset_scope_digests
                     WHERE reset_id = $1::uuid
                 )
             )
             FROM synchro.sync_stream_resets WHERE reset_id = $1::uuid",
            &[bootstrap_id.as_str().into()],
        )
        .expect("load aborted projection bootstrap")
        .expect("aborted projection bootstrap state");

        assert_eq!(aborted.0["candidate_slot_name"], "synchro_bootstrap_candidate");
        assert_eq!(state.0["lifecycle"], "aborted");
        assert_eq!(state.0["barrier_cleared"], true);
        assert_eq!(state.0["materialized_cleared"], true);
        assert_eq!(state.0["acknowledged_cleared"], true);
        assert_eq!(state.0["verified"], false);
        assert_eq!(state.0["staged_scopes"], 0);
    }

    #[pg_test]
    fn projection_bootstrap_activates_verified_stage_atomically() {
        let source_stream: String = Spi::get_one(
            "SELECT stream_generation FROM synchro.sync_runtime_state WHERE singleton",
        )
        .expect("load projection bootstrap source stream")
        .expect("projection bootstrap source stream");
        let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
            .expect("projection bootstrap fixture suffix query")
            .expect("projection bootstrap fixture suffix");
        let schema = format!("bootstrap_{suffix}");
        let table = "source";
        let view = format!("bootstrap_view_{suffix}");
        let membership = "membership";
        let policy = "bootstrap_policy";
        let record_id = "25000000-0000-4000-8000-000000000001";
        Spi::run(&format!(
            "CREATE SCHEMA {schema};
             GRANT USAGE ON SCHEMA {schema} TO synchro_owner, synchro_worker;
             CREATE TABLE {schema}.{table} (
                 id UUID PRIMARY KEY,
                 user_id TEXT NOT NULL,
                 title TEXT NOT NULL,
                 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 deleted_at TIMESTAMPTZ
             );
             INSERT INTO {schema}.{table} (id, user_id, title)
             VALUES ('{record_id}', 'u1', 'historical');
             SELECT synchro.synchro_prepare_projection_view(
                 '{schema}.{table}', '{view}',
                 ARRAY['id', 'user_id', 'title', 'updated_at', 'deleted_at']::text[]
             );
             CREATE FUNCTION {schema}.{membership}(p_key UUID)
             RETURNS SETOF text
             LANGUAGE sql
             STABLE
             SECURITY INVOKER
             SET search_path = pg_catalog, synchro
             BEGIN ATOMIC
                 SELECT 'user:' || (projection.user_id #>> '{{}}')
                 FROM synchro_projection.{view} projection
                 WHERE projection.record_id = p_key::text
                   AND NOT projection.deleted;
             END;
             REVOKE EXECUTE ON FUNCTION {schema}.{membership}(UUID) FROM PUBLIC;
             GRANT EXECUTE ON FUNCTION {schema}.{membership}(UUID)
                 TO synchro_owner, synchro_worker;
             GRANT SELECT, INSERT, UPDATE ON TABLE {schema}.{table} TO synchro_owner;
             ALTER TABLE {schema}.{table} ENABLE ROW LEVEL SECURITY;
             CREATE POLICY {policy} ON {schema}.{table}
                 AS PERMISSIVE FOR ALL TO synchro_owner
                 USING (true) WITH CHECK (true);
             SELECT synchro.synchro_register_table(
                 '{schema}.{table}', '{schema}.{membership}', 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'enabled'
             )"
        ))
        .expect("create projection bootstrap fixture");
        configure_reset_test_slot("synchro_reset_old");

        let pending: pgrx::JsonB = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT jsonb_build_object(
                 'generation', generation.generation,
                 'relation_id', registry.relation_id,
                 'state', generation.state,
                 'active_exposure', EXISTS (
                     SELECT 1
                     FROM synchro.sync_registry active_registry
                     JOIN synchro.sync_registry_generations active_generation
                       ON active_generation.generation = active_registry.registry_generation
                     WHERE active_generation.state = 'active'
                       AND active_registry.physical_relation_oid = '{schema}.{table}'::regclass
                 ),
                 'manifest_exposure', EXISTS (
                     SELECT 1
                     FROM synchro.sync_schema_manifest manifest
                     WHERE manifest.registry_generation = generation.generation
                 )
             )
             FROM synchro.sync_registry registry
             JOIN synchro.sync_registry_generations generation
               ON generation.generation = registry.registry_generation
             WHERE registry.physical_relation_oid = '{schema}.{table}'::regclass
             ORDER BY generation.generation DESC LIMIT 1"
        ))
        .expect("load pending projection bootstrap registration")
        .expect("pending projection bootstrap registration");
        let target_generation = pending.0["generation"]
            .as_i64()
            .expect("projection bootstrap target generation");
        let relation_id = pending.0["relation_id"]
            .as_str()
            .expect("projection bootstrap relation identity")
            .to_string();
        assert_eq!(pending.0["state"], "pending");
        assert_eq!(pending.0["active_exposure"], false);
        assert_eq!(pending.0["manifest_exposure"], false);

        let prepared = Spi::connect_mut(|client| {
            crate::stream_reset::prepare_projection_bootstrap_for_test(
                client,
                target_generation,
                "synchro_bootstrap_candidate",
            )
        })
        .expect("prepare projection bootstrap");
        let bootstrap_id = prepared["bootstrap_id"]
            .as_str()
            .expect("projection bootstrap identity")
            .to_string();
        Spi::connect_mut(|client| {
            crate::stream_reset::lock_stream_reset_sources_for_test(client, &bootstrap_id)?;
            crate::stream_reset::stage_projection_bootstrap_for_test(
                client,
                &bootstrap_id,
                "synchro_bootstrap_candidate",
                "0/10",
                "bootstrap-snapshot",
            )?;
            Ok::<_, String>(())
        })
        .expect("stage projection bootstrap");
        let staged: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'row_present', EXISTS (
                     SELECT 1 FROM synchro.sync_stream_reset_captured_rows
                     WHERE reset_id = $1::uuid AND relation_id = $2::uuid
                       AND record_id = $3
                 ),
                 'edge_present', EXISTS (
                     SELECT 1 FROM synchro.sync_stream_reset_membership_edges
                     WHERE reset_id = $1::uuid AND relation_id = $2::uuid
                       AND record_id = $3 AND scope_id = 'user:u1'
                 )
             )",
            &[
                bootstrap_id.as_str().into(),
                relation_id.as_str().into(),
                record_id.into(),
            ],
        )
        .expect("load projection bootstrap stage")
        .expect("projection bootstrap stage");
        assert_eq!(staged.0["row_present"], true);
        assert_eq!(staged.0["edge_present"], true);

        Spi::run(
            "INSERT INTO synchro.sync_wal_transactions (
                 stream_generation, commit_lsn, end_lsn, source_xid,
                 registry_generation, event_count, effect_count, content_hash,
                 commit_timestamp
             )
             SELECT runtime.stream_generation, '0/20', '0/30', '1'::xid,
                    progress.registry_generation, 0, 0,
                    decode(repeat('00', 32), 'hex'), now()
             FROM synchro.sync_runtime_state runtime
             CROSS JOIN synchro.sync_wal_progress progress
             WHERE runtime.singleton AND progress.singleton;
             UPDATE synchro.sync_wal_progress
             SET materialized_commit_lsn = '0/20', materialized_end_lsn = '0/30',
                 acknowledged_end_lsn = '0/30', updated_at = now()
             WHERE singleton",
        )
        .expect("create projection bootstrap activation boundary");
        let barrier = Spi::connect_mut(|client| {
            crate::stream_reset::request_projection_bootstrap_barrier_for_test(
                client,
                &bootstrap_id,
            )
        })
        .expect("request projection bootstrap barrier");
        assert_eq!(barrier["end_lsn"], "0/30");
        Spi::run_with_args(
            "UPDATE synchro.sync_stream_resets
             SET candidate_materialized_commit_lsn = '0/20',
                 candidate_materialized_end_lsn = '0/30',
                 candidate_acknowledged_end_lsn = '0/30',
                 candidate_verified = true, updated_at = now()
             WHERE reset_id = $1::uuid",
            &[bootstrap_id.as_str().into()],
        )
        .expect("verify projection bootstrap candidate boundary");

        Spi::run_with_args(
            "CREATE TEMP TABLE bootstrap_edge_backup ON COMMIT DROP AS
             SELECT * FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid AND relation_id = $2::uuid;
             DELETE FROM synchro.sync_stream_reset_membership_edges
             WHERE reset_id = $1::uuid AND relation_id = $2::uuid",
            &[
                bootstrap_id.as_str().into(),
                relation_id.as_str().into(),
            ],
        )
        .expect("remove required projection bootstrap edge");
        let partial_activation = Spi::connect_mut(|client| {
            crate::stream_reset::activate_projection_bootstrap_for_test(client, &bootstrap_id)
        });
        assert!(partial_activation.is_err());
        let still_pending: bool = Spi::get_one_with_args(
            "SELECT generation.state = 'pending'
                    AND NOT EXISTS (
                        SELECT 1 FROM synchro.sync_captured_rows
                        WHERE relation_id = $2::uuid
                    )
             FROM synchro.sync_registry_generations generation
             JOIN synchro.sync_stream_resets reset
               ON reset.target_registry_generation = generation.generation
             WHERE reset.reset_id = $1::uuid",
            &[
                bootstrap_id.as_str().into(),
                relation_id.as_str().into(),
            ],
        )
        .expect("check rejected partial projection bootstrap")
        .expect("rejected partial projection bootstrap state");
        assert!(still_pending);
        Spi::run(
            "INSERT INTO synchro.sync_stream_reset_membership_edges
             SELECT * FROM bootstrap_edge_backup",
        )
        .expect("restore required projection bootstrap edge");

        let activated = Spi::connect_mut(|client| {
            crate::stream_reset::activate_projection_bootstrap_for_test(client, &bootstrap_id)
        })
        .expect("activate verified projection bootstrap");
        assert_eq!(activated["registry_generation"], target_generation);
        assert_eq!(activated["activation_barrier"], "0/30");
        let state: pgrx::JsonB = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT jsonb_build_object(
                 'stream_unchanged', runtime.stream_generation = '{source_stream}',
                 'slot_unchanged', runtime.active_slot_name = 'synchro_reset_old',
                 'registry_active', generation.state = 'active',
                 'manifest_active', EXISTS (
                     SELECT 1 FROM synchro.sync_schema_manifest manifest
                     WHERE manifest.registry_generation = generation.generation
                 ),
                 'row_title', (
                     SELECT projection.title #>> '{{}}'
                     FROM synchro_projection.{view} projection
                     WHERE projection.record_id = '{record_id}'
                 ),
                 'edge_active', EXISTS (
                     SELECT 1 FROM synchro.sync_bucket_edges edge
                     WHERE edge.relation_id = '{relation_id}'::uuid
                       AND edge.record_id = '{record_id}'
                       AND edge.bucket_id = 'user:u1'
                 ),
                 'lifecycle', reset.lifecycle
             )
             FROM synchro.sync_stream_resets reset
             JOIN synchro.sync_registry_generations generation
               ON generation.generation = reset.target_registry_generation
             CROSS JOIN synchro.sync_runtime_state runtime
             WHERE reset.reset_id = '{bootstrap_id}'::uuid AND runtime.singleton"
        ))
        .expect("load activated projection bootstrap")
        .expect("activated projection bootstrap state");
        assert_eq!(state.0["stream_unchanged"], true);
        assert_eq!(state.0["slot_unchanged"], true);
        assert_eq!(state.0["registry_active"], true);
        assert_eq!(state.0["manifest_active"], true);
        assert_eq!(state.0["row_title"], "historical");
        assert_eq!(state.0["edge_active"], true);
        assert_eq!(state.0["lifecycle"], "activated");

        let cleaned: bool = Spi::get_one_with_args(
            "SELECT synchro.synchro_complete_projection_bootstrap_cleanup($1::uuid)",
            &[bootstrap_id.as_str().into()],
        )
        .expect("complete projection bootstrap cleanup")
        .expect("projection bootstrap cleanup result");
        assert!(cleaned);
    }

    #[pg_test]
    fn stream_reset_pointer_and_readiness_are_authoritative() {
        setup_test_tables();
        configure_reset_test_slot("synchro_reset_old");
        let selected = Spi::connect(|client| {
            crate::bgworker::effective_slot_name(client, "configured_bootstrap")
        })
        .expect("select durable reset slot");
        assert_eq!(selected, "synchro_reset_old");

        let _prepared = prepare_reset_for_test("synchro_reset_candidate");
        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load reset test database")
            .expect("reset test database");
        let detail = crate::health::load_readiness_status_with_configuration(
            crate::health::ReadinessConfiguration {
                database: Some(database),
                publication: Some("synchro_pub".to_string()),
                replication_slot: Some("synchro_reset_old".to_string()),
                worker_login: Some("missing_reset_worker".to_string()),
                max_heartbeat_age_seconds: 30,
                max_wal_lag_bytes: i32::MAX,
                max_wal_lag_seconds: 30,
            },
        )
        .detail();
        assert_eq!(detail["checks"]["stream_reset"]["state"], "failed");
        assert_eq!(detail["ready"], false);
    }

    #[pg_test]
    fn stream_reset_installs_capture_dependency_baseline() {
        setup_test_tables();
        let table = create_capture_dependency_table(false);
        register_capture_dependency_table(&table);
        let relation_id = active_capture_dependency_relation_id(&table);
        Spi::run(&format!(
            "INSERT INTO public.{table} (id, target_id, internal_note)
             VALUES (1, 7, 'not captured')"
        ))
        .expect("insert reset capture dependency source");
        configure_reset_test_slot("synchro_reset_old");

        let prepared = prepare_reset_for_test("synchro_reset_candidate");
        let id = reset_id(&prepared);
        lock_and_stage_reset(&id, "synchro_reset_candidate");
        let staged: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'row_data', row_data,
                 'deleted', deleted,
                 'fence_count', (
                     SELECT count(*)
                     FROM synchro.sync_stream_reset_fence_coverage coverage
                     WHERE coverage.reset_id = $1::uuid
                       AND coverage.relation_id = $2::uuid
                       AND coverage.registration_kind = 'capture_dependency'
                 )
             )
             FROM synchro.sync_stream_reset_capture_dependency_rows
             WHERE reset_id = $1::uuid
               AND relation_id = $2::uuid
               AND capture_key = '{\"id\": 1}'::jsonb",
            &[id.as_str().into(), relation_id.as_str().into()],
        )
        .expect("load staged reset capture dependency")
        .expect("staged reset capture dependency");
        assert_eq!(staged.0["row_data"], json!({"id": 1, "target_id": 7}));
        assert_eq!(staged.0["deleted"], json!(false));
        assert_eq!(staged.0["fence_count"], json!(1));

        Spi::connect_mut(|client| {
            crate::stream_reset::activate_stream_reset_for_test(client, &id)
        })
        .expect("activate capture dependency reset");
        let installed: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'row_data', row_data,
                 'source_reset_id', source_reset_id,
                 'fence_covered', EXISTS (
                     SELECT 1
                     FROM synchro.sync_write_fences fence
                     WHERE fence.relation_id = $2::uuid
                       AND fence.registration_kind = 'capture_dependency'
                       AND fence.coverage = 'reset_baseline'
                       AND fence.reset_id = $1::uuid
                 )
             )
             FROM synchro.sync_capture_dependency_rows
             WHERE relation_id = $2::uuid
               AND capture_key = '{\"id\": 1}'::jsonb",
            &[id.as_str().into(), relation_id.as_str().into()],
        )
        .expect("load installed reset capture dependency")
        .expect("installed reset capture dependency");
        assert_eq!(installed.0["row_data"], json!({"id": 1, "target_id": 7}));
        assert_eq!(installed.0["source_reset_id"], json!(id));
        assert_eq!(installed.0["fence_covered"], json!(true));
    }
