    #[pg_test]
    fn test_compact_deactivates_stale() {
        setup_test_tables();
        register_client("u1", "c1");

        // Set client's last_sync_at to 30 days ago.
        Spi::run_with_args(
            "UPDATE sync_clients
             SET created_at = now() - interval '30 days',
                 last_sync_at = now() - interval '30 days' \
             WHERE user_id = $1 AND client_id = $2",
            &["u1".into(), "c1".into()],
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        assert!(resp["deactivated_clients"].as_i64().unwrap() >= 1);
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
    fn test_compact_rejects_zero_stale_threshold_without_mutation() {
        assert_rejected_stale_threshold_preserves_state(
            "0 seconds",
            "b1000000-0000-4000-8000-000000000001",
        );
    }

    #[pg_test]
    fn test_compact_rejects_negative_stale_threshold_without_mutation() {
        assert_rejected_stale_threshold_preserves_state(
            "-1 second",
            "b1000000-0000-4000-8000-000000000002",
        );
    }

    #[pg_test]
    fn test_compact_rejects_infinite_stale_threshold_without_mutation() {
        assert_rejected_stale_threshold_preserves_state(
            "infinity",
            "b1000000-0000-4000-8000-000000000003",
        );
    }

    #[pg_test]
    fn test_compact_rejects_malformed_stale_threshold_without_mutation() {
        assert_rejected_stale_threshold_preserves_state(
            "not an interval",
            "b1000000-0000-4000-8000-000000000004",
        );
    }

    #[pg_test]
    fn test_compact_rejects_unsafe_stale_threshold_without_mutation() {
        assert_rejected_stale_threshold_preserves_state(
            "1000000 years",
            "b1000000-0000-4000-8000-000000000005",
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

    fn assert_rejected_stale_threshold_preserves_state(threshold: &str, record_id: &str) {
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

        assert!(!accepted, "invalid stale threshold must be rejected");
        assert_eq!(retained.0["active"], true);
        assert_eq!(retained.0["effect_count"], before);
    }
