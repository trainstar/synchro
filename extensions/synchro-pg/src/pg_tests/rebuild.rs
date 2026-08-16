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
        let record_id = "34343434-3434-3434-3434-343434343434";

        Spi::run_with_args(
            "INSERT INTO test_products (id, name, price)
             VALUES ($1::uuid, 'Missing version', 0)",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_products", record_id, "global");
        Spi::run_with_args(
            "DELETE FROM sync_row_versions
             WHERE record_id = $1
               AND relation_id = (
                   SELECT r.relation_id
                   FROM sync_registry r
                   JOIN sync_registry_generations g
                     ON g.generation = r.registry_generation
                   WHERE g.state = 'active' AND r.table_name = 'test_products'
               )",
            &[record_id.into()],
        )
        .unwrap();

        let response = rebuild_client("user1", "client1", "global", None, 100);
        assert_eq!(
            response["error"]["code"].as_str(),
            Some("sync_integrity_failure")
        );
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
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
    fn test_rebuild_filters_soft_deleted() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, deleted_at) VALUES
             ('bde10000-1111-1111-1111-111111111111', 'u1', 'Deleted', now())",
        )
        .unwrap();
        let resp = rebuild_client("u1", "c1", "user:u1", None, 100);
        let deleted = resp["records"]
            .as_array()
            .unwrap_or_else(|| panic!("{resp}"))
            .iter()
            .any(|record| {
                record["pk"]["id"].as_str() == Some("bde10000-1111-1111-1111-111111111111")
            });
        assert!(!deleted);
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
