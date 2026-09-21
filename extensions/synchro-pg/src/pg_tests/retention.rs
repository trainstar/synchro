    #[pg_test]
    fn test_compact_deactivates_stale() {
        setup_test_tables();
        for client_id in ["c1", "c2", "c3"] {
            register_client("u1", client_id);
        }

        // Set client's last_sync_at to 30 days ago.
        Spi::run_with_args(
            "UPDATE sync_clients
             SET created_at = now() - interval '30 days',
                 last_sync_at = now() - interval '30 days' \
             WHERE user_id = $1",
            &["u1".into()],
        )
        .unwrap();

        for expected in [2, 1, 0] {
            let response: pgrx::JsonB = Spi::get_one("SELECT synchro_compact('7 days', 2)")
                .expect("compact one stale-client batch")
                .expect("stale-client batch response");
            assert_eq!(response.0["deactivated_clients"], expected);
        }
    }

    #[pg_test]
    fn test_compact_keeps_recently_acknowledged_client_active() {
        setup_test_tables();
        register_client("u1", "c1");
        Spi::run(
            "UPDATE sync_clients
             SET last_sync_at = now() - interval '30 days',
                 last_acknowledged_at = now()
             WHERE user_id = 'u1' AND client_id = 'c1'",
        )
        .unwrap();

        let response: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        assert_eq!(response.unwrap().0["deactivated_clients"].as_i64(), Some(0));
        let active: Option<bool> = Spi::get_one(
            "SELECT is_active FROM sync_clients WHERE user_id = 'u1' AND client_id = 'c1'",
        )
        .unwrap();
        assert_eq!(active, Some(true));
    }

    #[pg_test]
    fn test_compact_default_stale_threshold_is_thirty_days() {
        setup_test_tables();
        register_client("u1", "c1");
        Spi::run(
            "UPDATE sync_clients
             SET created_at = now() - interval '8 days',
                 last_sync_at = now() - interval '8 days',
                 last_acknowledged_at = NULL
             WHERE user_id = 'u1' AND client_id = 'c1'",
        )
        .unwrap();

        let response: pgrx::JsonB = Spi::get_one("SELECT synchro_compact()")
            .unwrap()
            .expect("default compaction response");
        let active: bool = Spi::get_one(
            "SELECT is_active
             FROM sync_clients
             WHERE user_id = 'u1' AND client_id = 'c1'",
        )
        .unwrap()
        .expect("default compaction client state");

        assert_eq!(response.0["deactivated_clients"], 0);
        assert!(active);
    }

    #[pg_test]
    fn test_compact_deactivates_marked_retention_client() {
        setup_test_tables();
        register_client("u1", "expired-by-injected-clock");
        register_client("u1", "active-at-injected-clock");
        let marked: bool = Spi::get_one(
            "SELECT synchro_inject_client_retention_expiry('u1', 'expired-by-injected-clock')",
        )
        .expect("mark retention client")
        .expect("retention client mark result");

        let response: pgrx::JsonB = Spi::get_one("SELECT synchro_compact('30 days', 10000)")
            .unwrap()
            .expect("compaction response with marked retention client");
        let active_clients: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT client_id
                     FROM sync_clients
                     WHERE user_id = 'u1' AND is_active
                     ORDER BY client_id",
                    None,
                    &[],
                )
                .expect("read marked retention client state")
                .map(|row| {
                    row.get_by_name::<String, &str>("client_id")
                        .expect("read marked retention client ID")
                        .expect("marked retention client ID")
                })
                .collect()
        });

        assert!(marked);
        assert_eq!(response.0["deactivated_clients"], 1);
        assert_eq!(active_clients, vec!["active-at-injected-clock"]);
    }

    #[pg_test]
    fn test_compact_keeps_future_expiry_client_active() {
        setup_test_tables();
        register_client("u1", "future-expiry");
        Spi::run(
            "UPDATE sync_clients
             SET generation_expires_at = pg_catalog.statement_timestamp() + interval '1 hour'
             WHERE user_id = 'u1' AND client_id = 'future-expiry'",
        )
        .expect("set future client expiry");

        let response: pgrx::JsonB = Spi::get_one("SELECT synchro_compact('30 days', 10000)")
            .unwrap()
            .expect("compaction response with future expiry client");
        let active: Option<bool> = Spi::get_one(
            "SELECT is_active
             FROM sync_clients
             WHERE user_id = 'u1' AND client_id = 'future-expiry'",
        )
        .unwrap();

        assert_eq!(response.0["deactivated_clients"].as_i64(), Some(0));
        assert_eq!(active, Some(true));
    }

    #[pg_test]
    fn test_expire_retention_rejects_empty_identity() {
        setup_test_tables();
        register_client("u1", "c1");

        let accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<bool>(
                "SELECT synchro_inject_client_retention_expiry('', 'c1')",
            )
            .is_ok()
        }))
        .catch_others(|_| false)
        .execute();
        let null_accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<bool>(
                "SELECT synchro_inject_client_retention_expiry(NULL, 'c1')",
            )
            .is_ok()
        }))
        .catch_others(|_| false)
        .execute();
        let active: bool = Spi::get_one(
            "SELECT is_active FROM sync_clients WHERE user_id = 'u1' AND client_id = 'c1'",
        )
        .expect("read retention client after rejected expiry")
        .expect("retention client after rejected expiry");

        assert!(!accepted, "empty retention identity must be rejected");
        assert!(!null_accepted, "null retention identity must be rejected");
        assert!(active);
    }

    #[pg_test]
    fn test_compact_rejects_zero_stale_threshold_without_mutation() {
        assert_rejected_compaction_preserves_state(
            Some("0 seconds"),
            "b1000000-0000-4000-8000-000000000001",
        );
    }

    #[pg_test]
    fn test_compact_rejects_negative_stale_threshold_without_mutation() {
        assert_rejected_compaction_preserves_state(
            Some("-1 second"),
            "b1000000-0000-4000-8000-000000000002",
        );
    }

    #[pg_test]
    fn test_compact_rejects_infinite_stale_threshold_without_mutation() {
        assert_rejected_compaction_preserves_state(
            Some("infinity"),
            "b1000000-0000-4000-8000-000000000003",
        );
    }

    #[pg_test]
    fn test_compact_rejects_malformed_stale_threshold_without_mutation() {
        assert_rejected_compaction_preserves_state(
            Some("not an interval"),
            "b1000000-0000-4000-8000-000000000004",
        );
    }

    #[pg_test]
    fn test_compact_rejects_unsafe_stale_threshold_without_mutation() {
        assert_rejected_compaction_preserves_state(
            Some("1000000 years"),
            "b1000000-0000-4000-8000-000000000005",
        );
    }

    #[pg_test]
    fn test_compact_rejects_null_stale_threshold_without_mutation() {
        assert_rejected_compaction_preserves_state(
            None,
            "b1000000-0000-4000-8000-000000000007",
        );
    }

    #[pg_test]
    fn test_compact_rejects_oversized_batch_without_mutation() {
        setup_test_tables();
        register_client("u1", "c1");
        let record_id = "b1000000-0000-4000-8000-000000000006";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'retained after limit rejection')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", record_id, 1);

        let accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<pgrx::JsonB>("SELECT synchro_compact('7 days', 10001)").is_ok()
        }))
        .catch_others(|_| false)
        .execute();

        let state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'active', (
                     SELECT is_active
                     FROM sync_clients
                     WHERE user_id = 'u1' AND client_id = 'c1'
                 ),
                 'effect_count', (
                     SELECT count(*)
                     FROM sync_changelog
                     WHERE record_id = $1
                 )
             )",
            &[record_id.into()],
        )
        .unwrap()
        .expect("state after rejected compaction limit");

        assert!(!accepted, "batch size above 10000 must be rejected");
        assert_eq!(state.0["active"], true);
        assert_eq!(state.0["effect_count"], 1);
    }

    #[pg_test]
    fn test_compact_deletes_below_safe() {
        setup_test_tables();
        let first = "e1000000-0000-0000-0000-000000000001";
        let second = "e1000000-0000-0000-0000-000000000002";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name) VALUES
             ($1::uuid, 'first'), ($2::uuid, 'second')",
            &[first.into(), second.into()],
        )
        .unwrap();
        insert_changelog("global", "test_products", first, 1);
        insert_changelog("global", "test_products", second, 1);

        let before: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_changelog").unwrap();

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        let deleted = resp["deleted_entries"].as_i64().unwrap_or(0);
        // With no active clients, all entries should be deleted.
        assert!(deleted >= before.unwrap_or(0));
    }

    #[pg_test]
    fn test_compact_deletes_at_most_requested_batch_size() {
        setup_test_tables();
        let first = "e1100000-0000-0000-0000-000000000001";
        let second = "e1100000-0000-0000-0000-000000000002";
        let third = "e1100000-0000-0000-0000-000000000003";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name) VALUES
             ($1::uuid, 'first'), ($2::uuid, 'second'), ($3::uuid, 'third')",
            &[first.into(), second.into(), third.into()],
        )
        .unwrap();
        for record_id in [first, second, third] {
            insert_changelog("global", "test_products", record_id, 1);
        }

        let response: pgrx::JsonB = Spi::get_one("SELECT synchro_compact('7 days', 2)")
            .unwrap()
            .expect("bounded compaction response");
        let remaining: i64 = Spi::get_one(
            "SELECT count(*)
             FROM sync_changelog
             WHERE record_id IN (
                 'e1100000-0000-0000-0000-000000000001',
                 'e1100000-0000-0000-0000-000000000002',
                 'e1100000-0000-0000-0000-000000000003'
             )",
        )
        .unwrap()
        .expect("remaining bounded compaction effects");

        assert_eq!(response.0["deleted_entries"].as_i64(), Some(2));
        assert_eq!(remaining, 1);
    }

    #[pg_test]
    fn test_compact_updates_scope_floors_in_one_set() {
        setup_test_tables();
        let mut measurements = Vec::new();
        for scope_count in [1, 10] {
            let mut scopes = Vec::new();
            for index in 0..scope_count {
                let record_id = format!("e1200000-0000-4000-8000-{:012}", scope_count * 100 + index);
                let owner = format!("compact-{scope_count}-{index}");
                let scope = format!("user:{owner}");
                Spi::run_with_args(
                    "INSERT INTO test_orders (id, user_id, title)
                     VALUES ($1::uuid, $2, 'scope floor batch')",
                    &[record_id.as_str().into(), owner.as_str().into()],
                )
                .expect("insert compaction source");
                insert_changelog(&scope, "test_orders", &record_id, 1);
                scopes.push(scope);
            }
            let expected: pgrx::JsonB = Spi::get_one_with_args(
                "SELECT jsonb_object_agg(
                     bucket_id, jsonb_build_array(commit_lsn::text, event_ordinal, effect_ordinal)
                 )
                 FROM sync_changelog WHERE bucket_id = ANY($1)",
                &[scopes.clone().into()],
            )
            .expect("read expected compaction floors")
            .expect("expected compaction floors");
            let (response, queries) = query_counts::measure(1, || {
                Spi::get_one::<pgrx::JsonB>("SELECT synchro_compact('30 days', 100)")
                    .expect("compact scope set")
                    .expect("compacted scope set response")
            });
            assert_eq!(response.0["deleted_entries"], scope_count);
            let actual: pgrx::JsonB = Spi::get_one_with_args(
                "SELECT jsonb_object_agg(
                     scope_id, jsonb_build_array(
                         floor_commit_lsn::text, floor_event_ordinal, floor_effect_ordinal
                     )
                 )
                 FROM sync_scope_state
                 WHERE scope_id = ANY($1) AND floor_position_kind = 'effect'",
                &[scopes.into()],
            )
            .expect("read advanced compaction floors")
            .expect("advanced compaction floors");
            assert_eq!(actual.0, expected.0);
            assert!(queries > 0);
            measurements.push(queries);
        }
        assert_eq!(
            measurements[0], measurements[1],
            "compaction query count must not grow with scopes in one batch"
        );
    }

    #[pg_test]
    fn test_compact_rejects_invalid_effect_position_atomically() {
        setup_test_tables();
        let record_id = "e1300000-0000-4000-8000-000000000001";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name) VALUES ($1::uuid, 'invalid position')",
            &[record_id.into()],
        )
        .expect("insert invalid compaction position source");
        insert_changelog("global", "test_products", record_id, 1);
        Spi::run_with_args(
            "UPDATE sync_changelog SET effect_ordinal = -1 WHERE record_id = $1",
            &[record_id.into()],
        )
        .expect("corrupt compaction position");
        Spi::run(
            "DO $test$
             DECLARE rejected boolean := false;
             BEGIN
                 BEGIN
                     PERFORM synchro_compact('30 days', 100);
                 EXCEPTION WHEN OTHERS THEN
                     rejected := true;
                 END;
                 IF NOT rejected THEN
                     RAISE EXCEPTION 'invalid compaction position was accepted';
                 END IF;
             END
             $test$",
        )
        .expect("reject invalid position");
        let retained: i64 = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_changelog WHERE record_id = $1",
            &[record_id.into()],
        )
        .expect("count retained invalid effect")
        .expect("retained invalid effect count");
        let floor: String = Spi::get_one(
            "SELECT floor_position_kind FROM sync_scope_state WHERE scope_id = 'global'",
        )
        .expect("read floor after rejection")
        .expect("floor after rejection");
        assert_eq!(retained, 1);
        assert_eq!(floor, "generation_start");
    }

    #[pg_test]
    fn test_compact_floor_failure_rolls_back_effect_deletion() {
        setup_test_tables();
        let record_id = "e1400000-0000-4000-8000-000000000001";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name) VALUES ($1::uuid, 'floor failure')",
            &[record_id.into()],
        )
        .expect("insert floor failure source");
        insert_changelog("global", "test_products", record_id, 1);
        Spi::run(
            "CREATE FUNCTION pg_temp.reject_compaction_floor() RETURNS trigger
             LANGUAGE plpgsql AS $function$
             BEGIN
                 RAISE EXCEPTION 'injected floor failure' USING ERRCODE = 'check_violation';
             END
             $function$;
             CREATE TRIGGER reject_compaction_floor
             BEFORE UPDATE OF floor_position_kind ON synchro.sync_scope_state
             FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_compaction_floor();
             DO $test$
             DECLARE rejected boolean := false;
             BEGIN
                 BEGIN
                     PERFORM synchro_compact('30 days', 100);
                 EXCEPTION WHEN check_violation THEN
                     rejected := true;
                 END;
                 IF NOT rejected THEN
                     RAISE EXCEPTION 'compaction ignored the floor write failure';
                 END IF;
             END
             $test$",
        )
        .expect("surface the injected floor failure");
        let state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'effects', (SELECT count(*) FROM sync_changelog WHERE record_id = $1),
                 'floor', (SELECT floor_position_kind FROM sync_scope_state WHERE scope_id = 'global')
             )",
            &[record_id.into()],
        )
        .expect("read compaction rollback state")
        .expect("compaction rollback state");
        assert_eq!(state.0, json!({"effects": 1, "floor": "generation_start"}));
    }

    #[pg_test]
    fn test_compact_preserves_above_safe() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert entries.
        insert_changelog("user:u1", "test_orders", "preserve-1", 1);
        insert_changelog("user:u1", "test_orders", "preserve-2", 1);

        // Client has never pulled and seeded bucket checkpoints remain at 0.
        // safe_seq should be 0, so nothing gets deleted.
        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        let deleted = resp["deleted_entries"].as_i64().unwrap_or(0);
        assert_eq!(
            deleted, 0,
            "no entries should be deleted when active client at checkpoint 0"
        );
    }

    #[pg_test]
    fn test_compact_uses_typed_scope_checkpoints() {
        setup_test_tables();
        register_client("u1", "c1");

        let first = "c0010000-0000-0000-0000-000000000001";
        let second = "c0010000-0000-0000-0000-000000000002";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ($1::uuid, 'u1', 'first'), ($2::uuid, 'u1', 'second')",
            &[first.into(), second.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", first, 1);
        insert_changelog("user:u1", "test_orders", second, 1);
        Spi::run(
            "UPDATE sync_client_checkpoints checkpoint
             SET position_kind = 'transaction_end',
                 commit_lsn = progress.materialized_commit_lsn,
                 event_ordinal = NULL,
                 effect_ordinal = NULL
             FROM sync_wal_progress progress
             WHERE checkpoint.user_id = 'u1'
               AND checkpoint.client_id = 'c1'
               AND checkpoint.bucket_id = 'user:u1'
               AND progress.singleton = true",
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["deleted_entries"].as_i64(), Some(2));
    }

    #[pg_test]
    fn test_compaction_makes_cursor_below_retention_floor_stale() {
        setup_test_tables();
        register_client("u1", "c1");
        let stale_cursor = issued_scope_cursor("u1", "c1", "user:u1", 0);
        let record_id = "e3000000-0000-0000-0000-000000000001";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'retained')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", record_id, 1);
        Spi::run(
            "UPDATE sync_client_checkpoints checkpoint
             SET position_kind = 'transaction_end',
                 commit_lsn = progress.materialized_commit_lsn,
                 event_ordinal = NULL,
                 effect_ordinal = NULL
             FROM sync_wal_progress progress
             WHERE checkpoint.user_id = 'u1'
               AND checkpoint.client_id = 'c1'
               AND checkpoint.bucket_id = 'user:u1'
               AND progress.singleton = true",
        )
        .unwrap();

        let response: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        assert_eq!(response.unwrap().0["deleted_entries"].as_i64(), Some(1));
        let parsed = Spi::connect(|client| {
            let context = test_scope_cursor_context(client, "u1", "c1", "user:u1");
            crate::cursor_token::parse_scope_cursor(client, &context, &stale_cursor)
        })
        .unwrap();
        assert!(matches!(
            parsed,
            crate::cursor_token::ParsedScopeCursor::Stale
        ));
    }

    #[pg_test]
    fn test_compact_no_active_clients() {
        setup_test_tables();

        let record_id = "e2000000-0000-0000-0000-000000000001";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name) VALUES ($1::uuid, 'no clients')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("global", "test_products", record_id, 1);

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        // With no clients, all entries should be compactable.
        assert!(resp["deleted_entries"].as_i64().unwrap_or(0) >= 1);
    }

    #[pg_test]
    fn test_compact_pins_history_after_active_rebuild_boundary() {
        setup_test_tables();
        register_client("u1", "c1");
        let first = "b2000000-0000-0000-0000-000000000001";
        let second = "b2000000-0000-0000-0000-000000000002";
        let after_boundary = "b2000000-0000-0000-0000-000000000003";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ($1::uuid, 'u1', 'first staged row'),
             ($2::uuid, 'u1', 'second staged row')",
            &[first.into(), second.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", first, 1);
        insert_changelog("user:u1", "test_orders", second, 1);
        insert_edge("test_orders", first, "user:u1");
        insert_edge("test_orders", second, "user:u1");

        let first_page = rebuild_client("u1", "c1", "user:u1", None, 1);
        assert_eq!(first_page["has_more"].as_bool(), Some(true), "{first_page}");
        let continuation = first_page["cursor"]
            .as_str()
            .expect("active rebuild continuation")
            .to_string();

        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'after rebuild boundary')",
            &[after_boundary.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", after_boundary, 1);
        insert_edge("test_orders", after_boundary, "user:u1");
        Spi::run(
            "UPDATE sync_client_checkpoints checkpoint
             SET position_kind = 'transaction_end',
                 commit_lsn = progress.materialized_commit_lsn,
                 event_ordinal = NULL,
                 effect_ordinal = NULL
             FROM sync_wal_progress progress
             WHERE checkpoint.user_id = 'u1'
               AND checkpoint.client_id = 'c1'
               AND checkpoint.bucket_id = 'user:u1'
               AND progress.singleton = true",
        )
        .unwrap();

        let response: pgrx::JsonB = Spi::get_one("SELECT synchro_compact('7 days', 10000)")
            .unwrap()
            .expect("compaction response with active rebuild");
        let remaining: i64 = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_changelog WHERE record_id = $1",
            &[after_boundary.into()],
        )
        .unwrap()
        .expect("post-boundary history count");
        assert_eq!(remaining, 1, "compaction must retain history after the rebuild boundary");
        assert!(response.0["deleted_entries"].as_i64().unwrap_or(0) >= 1);

        let final_page = rebuild_client(
            "u1",
            "c1",
            "user:u1",
            Some(&continuation),
            1,
        );
        assert_eq!(final_page["error"], serde_json::Value::Null, "{final_page}");
        assert_eq!(final_page["has_more"].as_bool(), Some(false), "{final_page}");
        assert_eq!(final_page["records"].as_array().map(Vec::len), Some(1));
        let final_cursor = final_page["final_scope_cursor"]
            .as_str()
            .expect("active rebuild final cursor");
        let parsed = Spi::connect(|client| {
            let context = test_scope_cursor_context(client, "u1", "c1", "user:u1");
            crate::cursor_token::parse_scope_cursor(client, &context, final_cursor)
        })
        .unwrap();
        assert!(matches!(
            parsed,
            crate::cursor_token::ParsedScopeCursor::Current(_)
        ));
    }

    fn assert_rejected_compaction_preserves_state(
        threshold: Option<&str>,
        record_id: &str,
    ) {
        setup_test_tables();
        register_client("u1", "c1");
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'retained after rejection')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", record_id, 1);
        let before: i64 = Spi::get_one_with_args(
            "SELECT count(*)
             FROM sync_changelog
             WHERE record_id = $1",
            &[record_id.into()],
        )
        .unwrap()
        .expect("retained effect count before rejected compaction");

        let accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one_with_args::<pgrx::JsonB>(
                "SELECT synchro_compact($1, 10000)",
                &[threshold.into()],
            )
            .is_ok()
        }))
        .catch_others(|_| false)
        .execute();

        let retained: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'active', (
                     SELECT is_active
                     FROM sync_clients
                     WHERE user_id = 'u1' AND client_id = 'c1'
                 ),
                 'effect_count', (
                     SELECT count(*)
                     FROM sync_changelog
                     WHERE record_id = $1
                 )
             )",
            &[record_id.into()],
        )
        .unwrap()
        .expect("state after rejected compaction");

        assert!(!accepted, "invalid stale compaction input must be rejected");
        assert_eq!(retained.0["active"], true);
        assert_eq!(retained.0["effect_count"], before);
    }
