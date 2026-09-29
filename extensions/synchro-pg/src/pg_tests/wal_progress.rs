    const WAL_PROGRESS_HEARTBEAT_LIMIT: i32 = 3600;
    const WAL_PROGRESS_NO_BYTE_LIMIT: i32 = i32::MAX;

    fn assert_wal_progress_constraint_rejects(
        generation_start: Option<&str>,
        processed: Option<&str>,
        materialized: Option<(&str, &str)>,
        acknowledged: Option<&str>,
        constraint: &str,
    ) {
        let literal = |value: Option<&str>| {
            value.map_or_else(|| "NULL".to_string(), |lsn| format!("'{lsn}'::pg_lsn"))
        };
        let generation_start = literal(generation_start);
        let processed = literal(processed);
        let materialized_commit = literal(materialized.map(|(commit, _)| commit));
        let materialized_end = literal(materialized.map(|(_, end)| end));
        let acknowledged = literal(acknowledged);
        Spi::run(&format!(
            "DO $control$
             DECLARE
                 violated text;
             BEGIN
                 UPDATE synchro.sync_wal_progress
                 SET generation_start_lsn = {generation_start},
                     processed_end_lsn = {processed},
                     materialized_commit_lsn = {materialized_commit},
                     materialized_end_lsn = {materialized_end},
                     acknowledged_end_lsn = {acknowledged}
                 WHERE singleton;
                 RAISE EXCEPTION 'invalid WAL progress was accepted';
             EXCEPTION WHEN check_violation THEN
                 GET STACKED DIAGNOSTICS violated = CONSTRAINT_NAME;
                 IF violated IS DISTINCT FROM '{constraint}' THEN
                     RAISE EXCEPTION 'WAL progress violated constraint %', violated;
                 END IF;
             END
             $control$"
        ))
        .unwrap_or_else(|error| panic!("{constraint} did not reject the row: {error}"));
    }

    #[pg_test]
    fn wal_progress_rejects_processed_boundary_without_start() {
        assert_wal_progress_constraint_rejects(
            Some("0/10"),
            None,
            None,
            None,
            "sync_wal_progress_processed_present",
        );
        assert_wal_progress_constraint_rejects(
            None,
            Some("0/10"),
            None,
            None,
            "sync_wal_progress_processed_present",
        );
    }

    #[pg_test]
    fn wal_progress_rejects_processed_boundary_before_start() {
        assert_wal_progress_constraint_rejects(
            Some("0/20"),
            Some("0/10"),
            None,
            None,
            "sync_wal_progress_processed_after_start",
        );
    }

    #[pg_test]
    fn wal_progress_rejects_materialized_end_after_processed_boundary() {
        assert_wal_progress_constraint_rejects(
            Some("0/10"),
            Some("0/20"),
            Some(("0/28", "0/30")),
            None,
            "sync_wal_progress_materialized_processed",
        );
        assert_wal_progress_constraint_rejects(
            None,
            None,
            Some(("0/8", "0/10")),
            None,
            "sync_wal_progress_materialized_processed",
        );
    }

    #[pg_test]
    fn wal_progress_rejects_acknowledgement_outside_processed_range() {
        assert_wal_progress_constraint_rejects(
            Some("0/10"),
            Some("0/20"),
            None,
            Some("0/30"),
            "sync_wal_progress_acknowledged_processed",
        );
        assert_wal_progress_constraint_rejects(
            Some("0/10"),
            Some("0/20"),
            None,
            Some("0/8"),
            "sync_wal_progress_acknowledged_processed",
        );
        assert_wal_progress_constraint_rejects(
            None,
            None,
            None,
            Some("0/10"),
            "sync_wal_progress_acknowledged_processed",
        );
    }

    fn wal_progress_current_lsn() -> u64 {
        let value: String = Spi::get_one("SELECT pg_catalog.pg_current_wal_lsn()::text")
            .expect("read current WAL position")
            .expect("current WAL position");
        let value =
            crate::stream_position::parse_lsn(&value).expect("parse current WAL position");
        assert!(value > 16_384, "WAL position is too small for the fixture");
        value
    }

    fn wal_progress_set(
        generation_start: u64,
        acknowledged: Option<u64>,
        processed: u64,
        age_seconds: i32,
    ) {
        let generation_start = crate::stream_position::format_lsn(generation_start);
        let acknowledged = acknowledged.map(crate::stream_position::format_lsn);
        let processed = crate::stream_position::format_lsn(processed);
        Spi::run_with_args(
            "UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = $1::pg_lsn,
                 acknowledged_end_lsn = $2::pg_lsn,
                 processed_end_lsn = $3::pg_lsn,
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 updated_at = now() - ($4::integer * interval '1 second')
             WHERE singleton",
            &[
                generation_start.as_str().into(),
                acknowledged.as_deref().into(),
                processed.as_str().into(),
                age_seconds.into(),
            ],
        )
        .expect("set WAL progress fixture");
    }

    fn wal_progress_record(
        target: u64,
        row_limited: bool,
        heartbeat_limit_seconds: i32,
        max_wal_lag_bytes: i32,
    ) -> bool {
        Spi::connect_mut(|client| {
            crate::bgworker::record_idle_progress_for_test(
                client,
                target,
                row_limited,
                heartbeat_limit_seconds,
                max_wal_lag_bytes,
            )
        })
        .expect("record idle WAL progress")
    }

    fn wal_progress_row() -> String {
        Spi::get_one(
            "SELECT ROW(progress.ctid, progress.*)::text
             FROM synchro.sync_wal_progress progress
             WHERE progress.singleton",
        )
        .expect("read WAL progress row")
        .expect("WAL progress row")
    }

    fn wal_progress_processed() -> u64 {
        let value: String = Spi::get_one(
            "SELECT processed_end_lsn::text
             FROM synchro.sync_wal_progress
             WHERE singleton",
        )
        .expect("read processed WAL boundary")
        .expect("processed WAL boundary");
        crate::stream_position::parse_lsn(&value).expect("parse processed WAL boundary")
    }

    fn wal_progress_assert_skipped(target: u64, row_limited: bool, max_wal_lag_bytes: i32) {
        let before = wal_progress_row();
        let recorded = wal_progress_record(
            target,
            row_limited,
            WAL_PROGRESS_HEARTBEAT_LIMIT,
            max_wal_lag_bytes,
        );
        assert!(!recorded, "idle progress recorded a skipped target");
        assert_eq!(wal_progress_row(), before, "a skipped target changed WAL progress");
    }

    fn wal_progress_assert_recorded(target: u64, row_limited: bool, max_wal_lag_bytes: i32) {
        let recorded = wal_progress_record(
            target,
            row_limited,
            WAL_PROGRESS_HEARTBEAT_LIMIT,
            max_wal_lag_bytes,
        );
        assert!(recorded, "idle progress skipped a due target");
        assert_eq!(wal_progress_processed(), target);
    }

    #[pg_test]
    fn wal_progress_idle_retries_processed_after_acknowledgement() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 12_288, Some(base - 8_192), base - 4_096, 0);
        wal_progress_assert_recorded(base - 4_096, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_skips_target_that_is_not_due() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 0);
        wal_progress_assert_skipped(base, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_records_row_limited_target() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 0);
        wal_progress_assert_recorded(base, true, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_records_after_half_heartbeat_limit() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 3600);
        wal_progress_assert_recorded(base, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_records_after_half_wal_byte_limit() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 0);
        wal_progress_assert_recorded(base, false, 2);
    }

    #[pg_test]
    fn wal_progress_idle_skips_target_before_processed_boundary() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 12_288, Some(base - 8_192), base - 4_096, 3600);
        wal_progress_assert_skipped(base - 6_144, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_skips_while_stream_is_poisoned() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 3600);
        Spi::run(
            "INSERT INTO synchro.sync_wal_poison (
                 stream_generation, commit_lsn, failure_class, failure_detail, lifecycle
             )
             SELECT stream_generation, '0/1', 'decode_failed',
                    'WAL decoder rejected a replication message', 'active'
             FROM synchro.sync_runtime_state
             WHERE singleton",
        )
        .expect("create active idle progress poison");
        wal_progress_assert_skipped(base, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_stops_at_projection_bootstrap_barrier() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 3600);
        let consistent_point = crate::stream_position::format_lsn(base - 8_192);
        let barrier = crate::stream_position::format_lsn(base - 1);
        let bootstrap_id: String = Spi::get_one_with_args(
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
                    'synchro_wal_progress_old', 'synchro_wal_progress_candidate',
                    database.oid, database.datname, 'pgoutput', $1::pg_lsn,
                    'wal-progress-snapshot', $2::pg_lsn, 'catching_up',
                    0, 0, 0, 0, 0, now()
             FROM target
             JOIN pg_catalog.pg_database database
               ON database.datname = pg_catalog.current_database()
             RETURNING reset_id::text",
            &[consistent_point.as_str().into(), barrier.as_str().into()],
        )
        .expect("create catching-up idle progress bootstrap")
        .expect("catching-up idle progress bootstrap identity");

        wal_progress_assert_skipped(base, false, WAL_PROGRESS_NO_BYTE_LIMIT);

        Spi::run_with_args(
            "UPDATE synchro.sync_stream_resets
             SET activation_barrier = $2::pg_lsn, updated_at = now()
             WHERE reset_id = $1::uuid",
            &[
                bootstrap_id.as_str().into(),
                crate::stream_position::format_lsn(base).as_str().into(),
            ],
        )
        .expect("move idle progress bootstrap barrier to the target");
        wal_progress_assert_recorded(base, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_idle_skips_target_at_acknowledged_boundary() {
        let base = wal_progress_current_lsn();
        wal_progress_set(base - 8_192, Some(base - 4_096), base - 4_096, 3600);
        wal_progress_assert_skipped(base - 4_096, false, WAL_PROGRESS_NO_BYTE_LIMIT);
    }

    #[pg_test]
    fn wal_progress_order_rejects_commit_before_processed_boundary() {
        Spi::run(
            "UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = '0/10',
                 acknowledged_end_lsn = '0/30',
                 processed_end_lsn = '0/30',
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 updated_at = now()
             WHERE singleton",
        )
        .expect("set processed WAL order fixture");
        let xid: String = Spi::get_one("SELECT pg_current_xact_id()::text")
            .expect("read processed WAL order xid")
            .expect("processed WAL order xid");
        let xid: u32 = xid.parse().expect("parse processed WAL order xid");
        let transaction = |commit_lsn: u64| crate::wal_decoder::WalTransaction {
            xid,
            final_lsn: commit_lsn,
            commit_lsn,
            end_lsn: commit_lsn + 8,
            commit_timestamp: 0,
            events: Vec::new(),
            truncates: Vec::new(),
            messages: Vec::new(),
        };

        let before_boundary = Spi::connect_mut(|client| {
            crate::bgworker::materialize_transaction_for_test(client, &transaction(0x20))
        });
        assert_eq!(before_boundary, Err("validation_failed".to_string()));
        let unchanged: bool = Spi::get_one(
            "SELECT materialized_commit_lsn IS NULL
                    AND processed_end_lsn = '0/30'::pg_lsn
             FROM synchro.sync_wal_progress
             WHERE singleton",
        )
        .expect("read rejected processed WAL order state")
        .expect("rejected processed WAL order state");
        assert!(unchanged, "a rejected transaction changed WAL progress");

        let at_boundary = Spi::connect_mut(|client| {
            crate::bgworker::materialize_transaction_for_test(client, &transaction(0x30))
        });
        assert_eq!(at_boundary, Ok(()));
        let advanced: bool = Spi::get_one(
            "SELECT materialized_commit_lsn = '0/30'::pg_lsn
                    AND materialized_end_lsn = '0/38'::pg_lsn
                    AND processed_end_lsn = '0/38'::pg_lsn
                    AND acknowledged_end_lsn = '0/30'::pg_lsn
             FROM synchro.sync_wal_progress
             WHERE singleton",
        )
        .expect("read accepted processed WAL order state")
        .expect("accepted processed WAL order state");
        assert!(advanced, "a transaction at the processed boundary did not advance progress");
    }

    fn wal_progress_readiness_check(database: &str) -> Value {
        // The materialization progress check does not use the worker login.
        crate::health::load_readiness_status_with_configuration(
            crate::health::ReadinessConfiguration {
                database: Some(database.to_string()),
                publication: Some("synchro_pub".to_string()),
                worker_login: Some("synchro_wal_progress_worker".to_string()),
                max_heartbeat_age_seconds: 30,
                max_wal_lag_bytes: i32::MAX,
                max_wal_lag_seconds: 30,
            },
        )
        .detail()["checks"]["materialization_progress"]
            .clone()
    }

    #[pg_test]
    fn wal_progress_readiness_requires_acknowledged_processed_boundary() {
        // A pg_test transaction has a transaction ID before the test body runs.
        // PostgreSQL then rejects logical slot creation, so no runtime slot exists.
        // Without a runtime slot, a valid progress predicate gives an unknown slot check.
        let base = crate::stream_position::format_lsn(wal_progress_current_lsn());
        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load readiness test database")
            .expect("readiness test database");

        Spi::run_with_args(
            "UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = $1::pg_lsn,
                 processed_end_lsn = $1::pg_lsn + 8::numeric,
                 acknowledged_end_lsn = NULL,
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 updated_at = now()
             WHERE singleton",
            &[base.as_str().into()],
        )
        .expect("set unacknowledged processed boundary");
        let unacknowledged = wal_progress_readiness_check(&database);

        Spi::run_with_args(
            "UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = $1::pg_lsn,
                 processed_end_lsn = $1::pg_lsn,
                 acknowledged_end_lsn = NULL,
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 updated_at = now()
             WHERE singleton",
            &[base.as_str().into()],
        )
        .expect("set bound processed boundary");
        let bound = wal_progress_readiness_check(&database);

        Spi::run_with_args(
            "INSERT INTO synchro.sync_wal_transactions (
                 stream_generation, commit_lsn, end_lsn, source_xid,
                 registry_generation, event_count, effect_count, content_hash,
                 commit_timestamp
             )
             SELECT runtime.stream_generation, '0/A', '0/B', '1'::xid,
                    progress.registry_generation, 0, 0,
                    pg_catalog.decode(repeat('00', 32), 'hex'), now()
             FROM synchro.sync_runtime_state runtime
             CROSS JOIN synchro.sync_wal_progress progress
             WHERE runtime.singleton AND progress.singleton;
             UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = '0/1',
                 materialized_commit_lsn = '0/A',
                 materialized_end_lsn = '0/B',
                 processed_end_lsn = $1::pg_lsn,
                 acknowledged_end_lsn = $1::pg_lsn,
                 updated_at = now()
             WHERE singleton",
            &[base.as_str().into()],
        )
        .expect("set idle acknowledged processed boundary");
        let idle_acknowledged = wal_progress_readiness_check(&database);

        assert_eq!(unacknowledged["state"], "failed", "{unacknowledged}");
        assert_eq!(
            unacknowledged["reason"], "materialization_progress_invalid",
            "{unacknowledged}"
        );
        for passed in [&bound, &idle_acknowledged] {
            assert_eq!(passed["state"], "unknown", "{passed}");
            assert_eq!(passed["reason"], "slot_acknowledgement_unknown", "{passed}");
        }
    }
