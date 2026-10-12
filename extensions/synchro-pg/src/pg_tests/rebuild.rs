    #[pg_test]
    fn test_rebuild_rejects_empty_identity() {
        let response: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2::jsonb)",
            &["".into(), "{}".into()],
        )
        .unwrap();
        let response = response.unwrap().0;

        assert_eq!(response["error"]["code"].as_str(), Some("auth_required"));
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_rebuild_returns_final_cursor_and_checksum() {
        setup_test_tables();
        register_shared_scope("global", true);
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_products (id, name, price)
             VALUES ('33333333-3333-3333-3333-333333333333', 'Push Up', 0)",
        )
        .unwrap();
        insert_edge(
            "test_products",
            "33333333-3333-3333-3333-333333333333",
            "global",
        );
        insert_changelog(
            "global",
            "test_products",
            "33333333-3333-3333-3333-333333333333",
            1,
        );

        let resp = rebuild_client("user1", "client1", "global", None, 100);

        assert_eq!(resp["scope"].as_str(), Some("global"), "{resp}");
        assert_eq!(resp["has_more"].as_bool(), Some(false));
        assert!(resp["final_scope_cursor"].as_str().is_some());
        assert_eq!(resp["checksum"]["algorithm"].as_str(), Some("sha256"));
        assert!(resp["cursor"].is_null());

        let records = resp["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0]["table"].as_str(),
            Some(table_id("test_products").as_str())
        );
        assert_eq!(
            records[0]["row"][field_id("test_products", "name")].as_str(),
            Some("Push Up")
        );
        let expected_version =
            current_row_version("test_products", "33333333-3333-3333-3333-333333333333");
        assert_eq!(
            records[0]["server_version"].as_str(),
            Some(expected_version.as_str())
        );
    }

    #[pg_test]
    fn test_rebuild_missing_row_version_fails_closed() {
        setup_test_tables();
        register_shared_scope("global", true);
        register_client("user1", "client1");
        register_client("user1", "client2");
        let record_id = "34343434-3434-3434-3434-343434343434";

        Spi::run_with_args(
            "INSERT INTO test_products (id, name, price)
             VALUES ($1::uuid, 'Missing version', 0)",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_products", record_id, "global");
        insert_changelog("global", "test_products", record_id, 1);
        let baseline = rebuild_client("user1", "client1", "global", None, 100);
        assert!(baseline["error"].is_null(), "{baseline}");
        let records = baseline["records"].as_array().expect("baseline records");
        assert_eq!(records.len(), 1, "{baseline}");
        assert_eq!(
            records[0]["server_version"].as_str(),
            Some(current_row_version("test_products", record_id).as_str())
        );

        // Rebuild takes each version from the scope edge and the captured row.
        let removed: Option<i64> = Spi::get_one_with_args(
            "WITH removed AS (
                 UPDATE sync_bucket_edges SET row_version = NULL
                 WHERE record_id = $1 AND bucket_id = 'global'
                 RETURNING 1
             )
             SELECT count(*) FROM removed",
            &[record_id.into()],
        )
        .unwrap();
        assert_eq!(removed, Some(1));

        // A second client stages a new session. The first client's session
        // would reuse its staged snapshot.
        let response = rebuild_client("user1", "client2", "global", None, 100);
        assert_eq!(
            response["error"]["code"].as_str(),
            Some("sync_integrity_failure")
        );
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_rebuild_checks_each_row_digest_from_one_generation() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u1", "c2");
        let valid_id = "bde20000-0000-0000-0000-000000000001";
        let corrupted_id = "bde20000-0000-0000-0000-000000000002";
        for (record_id, title) in [(valid_id, "Valid digest"), (corrupted_id, "Corrupted digest")] {
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[record_id.into(), title.into()],
            )
            .unwrap();
            insert_edge("test_orders", record_id, "user:u1");
            insert_changelog("user:u1", "test_orders", record_id, 1);
        }

        let baseline = rebuild_client("u1", "c1", "user:u1", None, 100);
        assert!(baseline["error"].is_null(), "{baseline}");
        let records = baseline["records"].as_array().expect("baseline records");
        assert_eq!(records.len(), 2, "{baseline}");
        let primary_key_field_id = field_id("test_orders", "id");
        assert_eq!(records[0]["pk"][&primary_key_field_id].as_str(), Some(valid_id));
        assert_eq!(records[1]["pk"][&primary_key_field_id].as_str(), Some(corrupted_id));
        assert_eq!(baseline["has_more"].as_bool(), Some(false));
        assert!(baseline["final_scope_cursor"].as_str().is_some());
        assert_eq!(baseline["checksum"]["algorithm"].as_str(), Some("sha256"));
        assert_ne!(
            records[1]["row_checksum"]["digest"].as_str(),
            Some("a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5")
        );

        // Preserve all source fields and the first row's checksum.
        let source_state_query =
            "SELECT jsonb_build_object(
                 'generations', count(DISTINCT captured.registry_generation),
                 'rows', jsonb_agg(jsonb_build_object(
                     'record_id', edge.record_id,
                     'captured', to_jsonb(captured) -
                         CASE WHEN edge.record_id = $2 THEN 'checksum' ELSE '' END,
                     'edge', to_jsonb(edge) -
                         CASE WHEN edge.record_id = $2 THEN 'checksum' ELSE '' END
                 ) ORDER BY edge.relation_id, edge.record_id)
             )
             FROM sync_bucket_edges edge
             JOIN sync_captured_rows captured
               ON captured.relation_id = edge.relation_id
              AND captured.record_id = edge.record_id
             WHERE edge.table_name = 'test_orders'
               AND edge.bucket_id = 'user:u1'
               AND edge.record_id IN ($1, $2)";
        let before: pgrx::JsonB = Spi::get_one_with_args(
            source_state_query,
            &[valid_id.into(), corrupted_id.into()],
        )
        .unwrap()
        .expect("source rows before corruption");
        assert_eq!(before.0["generations"].as_i64(), Some(1));
        assert_eq!(before.0["rows"].as_array().unwrap().len(), 2);
        assert_eq!(before.0["rows"][0]["record_id"].as_str(), Some(valid_id));
        assert_eq!(before.0["rows"][1]["record_id"].as_str(), Some(corrupted_id));

        // Match both checksums so per-row digest verification must reject them.
        let changed: pgrx::JsonB = Spi::get_one_with_args(
            "WITH captured_changed AS (
                 UPDATE sync_captured_rows captured
                 SET checksum = decode(repeat('a5', 32), 'hex')
                 FROM sync_bucket_edges edge
                 WHERE edge.table_name = 'test_orders'
                   AND edge.bucket_id = 'user:u1'
                   AND edge.record_id = $1
                   AND captured.relation_id = edge.relation_id
                   AND captured.record_id = edge.record_id
                 RETURNING captured.record_id
             ), edge_changed AS (
                 UPDATE sync_bucket_edges
                 SET checksum = decode(repeat('a5', 32), 'hex')
                 WHERE table_name = 'test_orders'
                   AND bucket_id = 'user:u1'
                   AND record_id = $1
                 RETURNING record_id
             )
             SELECT jsonb_build_object(
                 'captured', (SELECT jsonb_agg(record_id) FROM captured_changed),
                 'edges', (SELECT jsonb_agg(record_id) FROM edge_changed)
             )",
            &[corrupted_id.into()],
        )
        .unwrap()
        .expect("corrupted row counts");
        assert_eq!(changed.0["captured"], json!([corrupted_id]));
        assert_eq!(changed.0["edges"], json!([corrupted_id]));
        let after: pgrx::JsonB = Spi::get_one_with_args(
            source_state_query,
            &[valid_id.into(), corrupted_id.into()],
        )
        .unwrap()
        .expect("source rows after corruption");
        assert_eq!(after.0, before.0);

        // The second client requires a fresh snapshot after corruption.
        let response = rebuild_client("u1", "c2", "user:u1", None, 100);
        assert_eq!(
            response["error"]["code"].as_str(),
            Some("sync_integrity_failure"),
            "{response}"
        );
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
        let staged_sessions: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_rebuild_sessions
             WHERE user_id = 'u1' AND client_id = 'c2' AND scope_id = 'user:u1'",
        )
        .unwrap();
        assert_eq!(staged_sessions, Some(0));
    }

    #[pg_test]
    fn test_rebuild_cursor_pagination() {
        setup_test_tables();
        register_client("u1", "c1");

        for i in 1..=3 {
            let id = format!("b000000{i}-0000-0000-0000-000000000000");
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[id.as_str().into(), format!("Rebuild {i}").as_str().into()],
            )
            .unwrap();
            insert_edge("test_orders", &id, "user:u1");
            insert_changelog("user:u1", "test_orders", &id, 1);
        }

        let first = rebuild_client("u1", "c1", "user:u1", None, 2);
        assert_eq!(first["has_more"].as_bool(), Some(true), "{first}");
        let cursor = first["cursor"].as_str().unwrap();
        assert!(!cursor.is_empty());

        let second = rebuild_client("u1", "c1", "user:u1", Some(cursor), 2);
        assert!(!second["records"].as_array().unwrap().is_empty());
    }

    #[pg_test]
    fn test_rebuild_verify_only_key_accepts_existing_cursor() {
        let cursor = paginated_rebuild_cursor();

        Spi::run(
            "UPDATE sync_token_keys
             SET state = 'verify_only'
             WHERE purpose = 'rebuild_cursor' AND state = 'active';
             INSERT INTO sync_token_keys (key_id, purpose, secret, state)
             VALUES (
                 'rebuild-cursor-v2', 'rebuild_cursor',
                 '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                 'active'
             )",
        )
        .unwrap();

        let second = rebuild_client("u1", "c1", "user:u1", Some(&cursor), 2);
        assert!(second["error"].is_null(), "{second}");
        assert!(!second["records"].as_array().unwrap().is_empty());
    }

    #[pg_test]
    fn test_rebuild_retired_key_rejects_existing_cursor() {
        let cursor = paginated_rebuild_cursor();

        Spi::run(
            "UPDATE sync_token_keys
             SET state = 'retired', retired_at = now()
             WHERE purpose = 'rebuild_cursor' AND state = 'active'",
        )
        .unwrap();

        let response = rebuild_client("u1", "c1", "user:u1", Some(&cursor), 2);
        assert_eq!(response["error"]["code"].as_str(), Some("invalid_request"));
    }

    #[pg_test]
    fn test_rebuild_rejects_noncanonical_cursor_payload() {
        let cursor = paginated_rebuild_cursor();
        let mut parts = cursor.split('.');
        assert_eq!(parts.next(), Some("v3"));
        assert_eq!(parts.next(), Some("rebuild"));
        let payload_segment = parts.next().expect("rebuild payload");
        assert!(parts.next().is_some());
        assert!(parts.next().is_none());

        use base64::Engine;
        use hmac::Mac;

        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload_segment)
            .expect("encoded rebuild payload");
        let noncanonical_payload = format!("{} ", String::from_utf8(payload).unwrap());
        let noncanonical_segment = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(noncanonical_payload.as_bytes());
        let secret: String = Spi::get_one(
            "SELECT secret FROM sync_token_keys
             WHERE purpose = 'rebuild_cursor' AND state = 'active'",
        )
        .unwrap()
        .expect("active rebuild key");
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
            .expect("rebuild key supports HMAC");
        mac.update(format!("v3.rebuild.{noncanonical_segment}").as_bytes());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(mac.finalize().into_bytes());
        let noncanonical_cursor = format!("v3.rebuild.{noncanonical_segment}.{signature}");

        let response = rebuild_client("u1", "c1", "user:u1", Some(&noncanonical_cursor), 2);
        assert_eq!(response["error"]["code"].as_str(), Some("invalid_request"));
    }

    fn paginated_rebuild_cursor() -> String {
        setup_test_tables();
        register_client("u1", "c1");
        for i in 1..=3 {
            let id = format!("b000000{i}-0000-0000-0000-000000000000");
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[id.as_str().into(), format!("Rebuild {i}").as_str().into()],
            )
            .unwrap();
            insert_edge("test_orders", &id, "user:u1");
            insert_changelog("user:u1", "test_orders", &id, 1);
        }
        let first = rebuild_client("u1", "c1", "user:u1", None, 2);
        assert_eq!(first["has_more"].as_bool(), Some(true), "{first}");
        first["cursor"]
            .as_str()
            .expect("paginated rebuild cursor")
            .to_string()
    }

    #[pg_test]
    fn test_rebuild_rejects_membership_of_soft_deleted_row() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u1", "c2");
        let live = "bde10000-0000-0000-0000-000000000001";
        let deleted = "bde10000-1111-1111-1111-111111111111";
        for (record_id, title) in [(live, "Live"), (deleted, "Deleted")] {
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[record_id.into(), title.into()],
            )
            .unwrap();
            insert_changelog("user:u1", "test_orders", record_id, 1);
            insert_edge("test_orders", record_id, "user:u1");
        }
        let primary_key_field_id = field_id("test_orders", "id");
        let rebuilt_keys = |response: &Value| -> Vec<String> {
            let mut keys: Vec<String> = response["records"]
                .as_array()
                .unwrap_or_else(|| panic!("{response}"))
                .iter()
                .map(|record| {
                    record["pk"][&primary_key_field_id]
                        .as_str()
                        .unwrap_or_else(|| panic!("{response}"))
                        .to_string()
                })
                .collect();
            keys.sort();
            keys
        };
        let baseline = rebuild_client("u1", "c1", "user:u1", None, 100);
        assert_eq!(rebuilt_keys(&baseline), vec![live, deleted], "{baseline}");

        // Keep the edge consistent with the captured tombstone, so only the
        // tombstone guard can reject the rebuild.
        Spi::run_with_args(
            "UPDATE test_orders SET deleted_at = now() WHERE id = $1::uuid",
            &[deleted.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", deleted, 3);
        let aligned: Option<i64> = Spi::get_one_with_args(
            "WITH aligned AS (
                 UPDATE sync_bucket_edges edge
                 SET checksum = captured.checksum, row_version = captured.row_version
                 FROM sync_captured_rows captured
                 WHERE edge.bucket_id = 'user:u1'
                   AND edge.record_id = $1
                   AND captured.relation_id = edge.relation_id
                   AND captured.record_id = edge.record_id
                   AND captured.deleted
                 RETURNING 1
             )
             SELECT count(*) FROM aligned",
            &[deleted.into()],
        )
        .unwrap();
        assert_eq!(aligned, Some(1));

        // A second client stages a new session from the changed state.
        let response = rebuild_client("u1", "c2", "user:u1", None, 100);
        assert_eq!(
            response["error"]["code"].as_str(),
            Some("sync_integrity_failure"),
            "{response}"
        );
    }

    #[pg_test]
    fn test_rebuild_final_scope_cursor_is_not_acknowledged() {
        setup_pull_fixtures();

        let resp = rebuild_client("u1", "c1", "user:u1", None, 1000);
        assert_eq!(resp["has_more"].as_bool(), Some(false), "{resp}");
        let final_scope_cursor = resp["final_scope_cursor"].as_str().unwrap();

        let stored: Option<String> = Spi::get_one_with_args(
            "SELECT position_kind FROM sync_client_checkpoints
             WHERE user_id = $1 AND client_id = $2 AND bucket_id = 'user:u1'",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        Spi::connect(|client| {
            let context = test_scope_cursor_context(client, "u1", "c1", "user:u1");
            match crate::cursor_token::parse_scope_cursor(client, &context, final_scope_cursor)
                .expect("final scope cursor should decode for rebuilt scope")
            {
                crate::cursor_token::ParsedScopeCursor::Current(_) => {
                    Ok::<(), pgrx::spi::Error>(())
                }
                crate::cursor_token::ParsedScopeCursor::Stale => {
                    panic!("rebuilt final scope cursor must not be stale")
                }
            }
        })
        .unwrap();
        assert_eq!(stored.as_deref(), Some("generation_start"));
    }

    #[pg_test]
    fn test_rebuild_preserves_unrelated_scope_checkpoint() {
        setup_test_tables();
        register_shared_scope("global", true);
        register_client("u1", "c1");
        Spi::run(
            "UPDATE sync_client_checkpoints
             SET position_kind = 'transaction_end', commit_lsn = '0/30',
                 event_ordinal = NULL, effect_ordinal = NULL,
                 updated_at = '2026-07-18T13:59:01Z'::timestamptz
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'global'",
        )
        .unwrap();
        let before: String = Spi::get_one(
            "SELECT to_jsonb(checkpoint)::text
             FROM sync_client_checkpoints checkpoint
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'global'",
        )
        .unwrap()
        .expect("unrelated checkpoint before rebuild");

        let response = rebuild_client("u1", "c1", "user:u1", None, 1000);
        assert_eq!(response["has_more"].as_bool(), Some(false), "{response}");
        let after: String = Spi::get_one(
            "SELECT to_jsonb(checkpoint)::text
             FROM sync_client_checkpoints checkpoint
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'global'",
        )
        .unwrap()
        .expect("unrelated checkpoint after rebuild");

        assert_eq!(after, before);
    }

    #[pg_test]
    fn test_rebuild_unsubscribed_errors() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = rebuild_client("u1", "c1", "team:other", None, 100);
        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
    }

    #[pg_test]
    fn test_rebuild_generation_precedes_schema_mismatch() {
        setup_test_tables();
        register_client("u1", "c1");
        let (schema_version, schema_hash) = latest_schema_ref();
        let response: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2::jsonb)",
            &[
                "u1".into(),
                json!({
                    "client_id": "c1",
                    "client_generation": 2,
                    "schema": { "version": schema_version + 1, "hash": schema_hash },
                    "scope": "user:u1",
                    "rebuild_id": test_uuid("rebuild-generation-precedence"),
                    "cursor": null,
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let response = response.unwrap().0;

        assert_eq!(
            response["error"]["code"].as_str(),
            Some("client_generation_expired")
        );
        assert_eq!(response["error"]["current_client_generation"], 1);
    }
