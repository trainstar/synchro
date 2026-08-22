    #[pg_test]
    fn test_pull_rejects_empty_identity() {
        let response: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &["".into(), "{}".into()],
        )
        .unwrap();
        let response = response.unwrap().0;

        assert_eq!(response["error"]["code"].as_str(), Some("auth_required"));
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_pull_returns_upsert_and_scope_cursor() {
        setup_test_tables();
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
            "INSERT INTO test_orders (id, user_id, title, amount)
             VALUES ('11111111-1111-1111-1111-111111111111', 'user1', 'Morning Run', 10)",
        )
        .unwrap();
        insert_edge(
            "test_orders",
            "11111111-1111-1111-1111-111111111111",
            "user:user1",
        );
        insert_changelog(
            "user:user1",
            "test_orders",
            "11111111-1111-1111-1111-111111111111",
            1,
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "client_generation": 1,
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": {
                        "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 0)
                    },
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0]["scope"].as_str(), Some("user:user1"));
        assert_eq!(
            changes[0]["table"].as_str(),
            Some(table_id("test_orders").as_str())
        );
        assert_eq!(changes[0]["op"].as_str(), Some("upsert"));
        assert_eq!(
            changes[0]["row"][field_id("test_orders", "title")].as_str(),
            Some("Morning Run")
        );
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
        assert!(resp["scope_cursors"]["user:user1"].as_str().is_some());
        assert_eq!(resp["rebuild"].as_array().unwrap().len(), 0);
        assert_eq!(
            resp["checksums"]["user:user1"]["algorithm"].as_str(),
            Some("sha256")
        );
    }

    #[pg_test]
    fn test_synced_projection_serializes_json_values_as_canonical_text() {
        setup_portable_type_contract_table();
        let record_id = "10101010-1010-4010-8010-101010101010";
        Spi::run_with_args(
            "INSERT INTO test_portable_type_contract (
                 id, user_id, label, col_json, col_text_array, col_int_array
             ) VALUES (
                 $1::uuid, 'projection-user', 'portable JSON',
                 '{\"b\":2,\"a\":1}'::jsonb, ARRAY['alpha', 'beta'], ARRAY[1, 2]
             )",
            &[record_id.into()],
        )
        .unwrap();

        let row_data = Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let table = registry
                .iter()
                .find(|table| table.table_name == "test_portable_type_contract")
                .expect("portable type registration");
            assert!(
                table
                    .fields
                    .iter()
                    .find(|field| field.physical_column == "col_json")
                    .expect("native JSON field")
                    .native_json
            );
            assert!(
                table
                    .fields
                    .iter()
                    .filter(|field| {
                        matches!(
                            field.physical_column.as_str(),
                            "col_text_array" | "col_int_array"
                        )
                    })
                    .all(|field| !field.native_json)
            );
            let projection = crate::pull::synced_row_projection_sql(table, "source");
            let query = format!(
                "SELECT {projection} AS row_data
                 FROM test_portable_type_contract source
                 WHERE source.id = $1::uuid"
            );
            let mut row_data = client
                .select(&query, None, &[record_id.into()])?
                .first()
                .get_by_name::<pgrx::JsonB, &str>("row_data")?
                .expect("projected row")
                .0;
            crate::pull::canonicalize_synced_row_data(table, &mut row_data)
                .expect("canonical projected row");
            Ok::<_, pgrx::spi::Error>(row_data)
        })
        .unwrap();

        assert_eq!(
            row_data[field_id("test_portable_type_contract", "col_json")].as_str(),
            Some("{\"a\":1,\"b\":2}")
        );
        assert_eq!(
            row_data[field_id("test_portable_type_contract", "col_text_array")].as_str(),
            Some("[\"alpha\",\"beta\"]")
        );
        assert_eq!(
            row_data[field_id("test_portable_type_contract", "col_int_array")].as_str(),
            Some("[1,2]")
        );
    }

    #[pg_test]
    fn test_pull_reads_immutable_captured_projection() {
        setup_test_tables();
        register_client("u1", "c1");
        let record_id = "13131313-1313-1313-1313-131313131313";

        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'captured title')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, "user:u1");
        insert_changelog("user:u1", "test_orders", record_id, 1);
        let captured_version: Option<String> = Spi::get_one_with_args(
            "SELECT row_version::text
             FROM sync_captured_projections
             WHERE record_id = $1 AND image_kind = 'after'",
            &[record_id.into()],
        )
        .unwrap();

        Spi::run_with_args(
            "UPDATE test_orders SET title = 'later live title' WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap();

        let response = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );
        let change = &response["changes"][0];
        assert_eq!(
            change["row"][field_id("test_orders", "title")].as_str(),
            Some("captured title")
        );
        assert_eq!(
            change["server_version"].as_str(),
            captured_version.as_deref()
        );
    }

    #[pg_test]
    fn test_pull_missing_projection_fails_without_progress() {
        setup_test_tables();
        register_client("u1", "c1");
        let record_id = "14141414-1414-1414-1414-141414141414";

        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'missing projection')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, "user:u1");
        insert_changelog("user:u1", "test_orders", record_id, 1);
        Spi::run_with_args(
            "DELETE FROM sync_captured_projections WHERE record_id = $1",
            &[record_id.into()],
        )
        .unwrap();

        let response = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );
        assert_eq!(
            response["error"]["code"].as_str(),
            Some("sync_integrity_failure")
        );
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
        assert!(response.get("changes").is_none());
        assert!(response.get("scope_cursors").is_none());
        assert!(response.get("checksums").is_none());
        let checkpoint: Option<String> = Spi::get_one(
            "SELECT position_kind
             FROM sync_client_checkpoints
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'user:u1'",
        )
        .unwrap();
        assert_eq!(checkpoint.as_deref(), Some("generation_start"));
    }

    #[pg_test]
    fn test_bucket_checksum_rejects_relation_table_mismatch() {
        setup_test_tables();
        let record_id = "15151515-1515-1515-1515-151515151515";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'checksum-user', 'relation mismatch')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, "user:checksum-user");
        Spi::run_with_args(
            "UPDATE sync_bucket_edges
             SET relation_id = 'ffffffff-ffff-4fff-8fff-ffffffffffff'::uuid
             WHERE table_name = 'test_orders' AND record_id = $1",
            &[record_id.into()],
        )
        .unwrap();

        let result = Spi::connect(|client| {
            crate::pull::compute_bucket_checksums(client, &["user:checksum-user".to_string()])
        });
        assert!(result.is_err(), "checksum calculation must bind table to relation");
    }

    #[pg_test]
    fn test_pull_delete_includes_tombstone_row() {
        setup_test_tables();
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
            "INSERT INTO test_orders (id, user_id, title, updated_at, deleted_at)
             VALUES (
                '12121212-1212-1212-1212-121212121212',
                'user1',
                'Soft Deleted',
                '2026-01-04T00:00:00Z'::timestamptz,
                '2026-01-04T00:00:00Z'::timestamptz
             )",
        )
        .unwrap();
        insert_changelog(
            "user:user1",
            "test_orders",
            "12121212-1212-1212-1212-121212121212",
            3,
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "client_generation": 1,
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": {
                        "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 0)
                    },
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0]["op"].as_str(), Some("delete"));
        assert_eq!(
            changes[0]["row"][field_id("test_orders", "deleted_at")].as_str(),
            Some("2026-01-04T00:00:00.000000Z")
        );
    }

    #[pg_test]
    fn test_pull_requests_rebuild_for_scope_without_cursor() {
        setup_test_tables();
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
            "INSERT INTO test_orders (id, user_id, title, amount)
             VALUES ('22222222-2222-2222-2222-222222222222', 'user1', 'Needs Rebuild', 11)",
        )
        .unwrap();
        insert_edge(
            "test_orders",
            "22222222-2222-2222-2222-222222222222",
            "user:user1",
        );
        insert_changelog(
            "user:user1",
            "test_orders",
            "22222222-2222-2222-2222-222222222222",
            1,
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "client_generation": 1,
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": {
                        "user:user1": { "cursor": null }
                    },
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["changes"].as_array().unwrap().len(), 0);
        assert_eq!(resp["rebuild"].as_array().unwrap().len(), 1);
        assert_eq!(resp["rebuild"][0].as_str(), Some("user:user1"));
    }

    #[pg_test]
    fn test_pull_deduplication() {
        setup_pull_fixtures();
        Spi::run(
            "UPDATE test_orders SET title = 'deduplicated'
             WHERE id = 'a1111111-1111-1111-1111-111111111111'",
        )
        .unwrap();
        insert_changelog(
            "user:u1",
            "test_orders",
            "a1111111-1111-1111-1111-111111111111",
            2,
        );

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );

        let primary_key_field_id = field_id("test_orders", "id");
        let hits = resp["changes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|change| {
                change["pk"][&primary_key_field_id].as_str()
                    == Some("a1111111-1111-1111-1111-111111111111")
            })
            .count();
        assert_eq!(hits, 1);
    }

    #[pg_test]
    fn test_pull_pagination_has_more() {
        setup_pull_fixtures();

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            1,
        );
        assert_eq!(resp["has_more"].as_bool(), Some(true));
    }

    #[pg_test]
    fn test_pull_applies_limit_after_each_scope_is_eligible() {
        setup_test_tables();
        register_shared_scope("global", false);
        register_client("u1", "c1");

        let user_ids = [
            "d1000000-0000-0000-0000-000000000001",
            "d1000000-0000-0000-0000-000000000002",
            "d1000000-0000-0000-0000-000000000003",
        ];
        for (index, record_id) in user_ids.iter().enumerate() {
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[
                    (*record_id).into(),
                    format!("old user change {index}").as_str().into(),
                ],
            )
            .unwrap();
            insert_changelog("user:u1", "test_orders", record_id, 1);
            insert_edge("test_orders", record_id, "user:u1");
        }
        let user_checkpoint: i64 =
            Spi::get_one("SELECT MAX(seq) FROM sync_changelog WHERE bucket_id = 'user:u1'")
                .unwrap()
                .expect("user checkpoint sequence");

        let product_id = "d2000000-0000-0000-0000-000000000001";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name) VALUES ($1::uuid, 'eligible product')",
            &[product_id.into()],
        )
        .unwrap();
        insert_changelog("global", "test_products", product_id, 1);
        insert_edge("test_products", product_id, "global");

        let response = pull_client(
            "u1",
            "c1",
            1,
            json!({
                "user:u1": scope_cursor_ref("u1", "c1", "user:u1", user_checkpoint),
                "global": scope_cursor_ref("u1", "c1", "global", 0)
            }),
            1,
        );

        assert_eq!(response["changes"].as_array().unwrap().len(), 1);
        assert_eq!(response["changes"][0]["scope"].as_str(), Some("global"));
        assert_eq!(response["has_more"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_pull_deduplicates_through_boundary_before_limit() {
        setup_test_tables();
        register_client("u1", "c1");
        let record_id = "d3000000-0000-0000-0000-000000000001";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', 'first')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", record_id, 1);
        for title in ["second", "final"] {
            Spi::run_with_args(
                "UPDATE test_orders SET title = $2 WHERE id = $1::uuid",
                &[record_id.into(), title.into()],
            )
            .unwrap();
            insert_changelog("user:u1", "test_orders", record_id, 2);
        }
        insert_edge("test_orders", record_id, "user:u1");

        let response = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            1,
        );

        assert_eq!(response["changes"].as_array().unwrap().len(), 1);
        assert_eq!(
            response["changes"][0]["row"][field_id("test_orders", "title")].as_str(),
            Some("final")
        );
        assert_eq!(response["has_more"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_pull_rejects_future_scope_set_version() {
        setup_test_tables();
        register_client("u1", "c1");

        let response = pull_client(
            "u1",
            "c1",
            2,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );

        assert_eq!(response["error"]["code"].as_str(), Some("invalid_request"));
        assert!(response.get("changes").is_none());
    }

    #[pg_test]
    fn test_pull_exclude_columns_stripped() {
        setup_pull_fixtures();

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );

        for change in resp["changes"].as_array().unwrap() {
            if change["table"].as_str() == Some(table_id("test_orders").as_str()) {
                assert_eq!(change["row"].as_object().map(|row| row.len()), Some(7));
            }
        }
    }

    #[pg_test]
    fn test_pull_sync_columns_strip_non_synced_fields() {
        setup_sync_columns_table();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_sync_columns_items (id, user_id, title, search_vector, internal_notes)
             VALUES (
                '44444444-4444-4444-4444-444444444444',
                'u1',
                'Projection test',
                'fts data',
                'server secret'
             )",
        )
        .unwrap();
        insert_edge(
            "test_sync_columns_items",
            "44444444-4444-4444-4444-444444444444",
            "user:u1",
        );
        insert_changelog(
            "user:u1",
            "test_sync_columns_items",
            "44444444-4444-4444-4444-444444444444",
            1,
        );

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );

        let change = resp["changes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|change| {
                change["table"].as_str() == Some(table_id("test_sync_columns_items").as_str())
            })
            .expect("test_sync_columns_items should be present in pull response");
        let row = &change["row"];
        assert_eq!(
            row[field_id("test_sync_columns_items", "title")].as_str(),
            Some("Projection test")
        );
        let expected_fields = ["id", "user_id", "title", "updated_at", "deleted_at"]
            .map(|column| field_id("test_sync_columns_items", column));
        let row = row.as_object().expect("logical row object");
        assert_eq!(row.len(), expected_fields.len());
        assert!(row.keys().all(|key| expected_fields.contains(key)));
    }

    #[pg_test]
    fn test_pull_bucket_isolation() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u2", "c2");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ('a00000a1-1111-1111-1111-111111111111', 'u1', 'User1 Order'),
             ('a00000a2-2222-2222-2222-222222222222', 'u2', 'User2 Order')",
        )
        .unwrap();

        insert_changelog(
            "user:u1",
            "test_orders",
            "a00000a1-1111-1111-1111-111111111111",
            1,
        );
        insert_changelog(
            "user:u2",
            "test_orders",
            "a00000a2-2222-2222-2222-222222222222",
            1,
        );
        insert_edge(
            "test_orders",
            "a00000a1-1111-1111-1111-111111111111",
            "user:u1",
        );
        insert_edge(
            "test_orders",
            "a00000a2-2222-2222-2222-222222222222",
            "user:u2",
        );

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );

        for change in resp["changes"].as_array().unwrap() {
            let primary_key_field_id = primary_key_field_id("test_orders");
            assert_ne!(
                change["pk"][primary_key_field_id].as_str(),
                Some("a00000a2-2222-2222-2222-222222222222")
            );
        }
    }

    #[pg_test]
    fn test_pull_scope_updates_added() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = pull_client("u1", "c1", 0, json!({}), 100);
        let added = resp["scope_updates"]["add"].as_array().unwrap();
        assert!(
            added
                .iter()
                .any(|scope| scope["id"].as_str() == Some("user:u1")),
            "user:u1 should be present in scope updates"
        );
    }

    #[pg_test]
    fn test_pull_scope_updates_removed() {
        setup_test_tables();
        register_shared_scope("shared:public", false);
        register_client("u1", "c1");

        let scopes = client_scope_ids("u1", "c1")
            .into_iter()
            .map(|scope_id| {
                let cursor = issued_scope_cursor("u1", "c1", &scope_id, 0);
                (scope_id, json!({ "cursor": cursor }))
            })
            .collect::<serde_json::Map<String, Value>>();
        Spi::run_with_args(
            "SELECT synchro_unregister_shared_scope($1)",
            &["shared:public".into()],
        )
        .unwrap();

        let resp = pull_client("u1", "c1", 1, Value::Object(scopes), 100);

        assert_eq!(resp["scope_set_version"].as_i64(), Some(2));
        assert_eq!(resp["scope_updates"]["add"], json!([]));
        assert_eq!(resp["scope_updates"]["remove"], json!(["shared:public"]));
    }

    #[pg_test]
    fn test_pull_rejects_scope_without_assignment_history() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = pull_client(
            "u1",
            "c1",
            0,
            json!({ "team:old": scope_cursor_ref("u1", "c1", "team:old", 0) }),
            100,
        );
        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
    }

    #[pg_test]
    fn test_pull_scope_updates_unchanged() {
        setup_test_tables();
        register_client("u1", "c1");

        let scopes = client_scope_ids("u1", "c1")
            .into_iter()
            .map(|scope_id| {
                let cursor = issued_scope_cursor("u1", "c1", &scope_id, 0);
                (scope_id, json!({ "cursor": cursor }))
            })
            .collect::<serde_json::Map<String, Value>>();
        let resp = pull_client("u1", "c1", 1, Value::Object(scopes), 100);

        assert_eq!(resp["scope_updates"]["add"].as_array().unwrap().len(), 0);
        assert_eq!(resp["scope_updates"]["remove"].as_array().unwrap().len(), 0);
        assert!(resp["scope_cursors"]["user:u1"].as_str().is_some());
    }

    #[pg_test]
    fn test_client_not_found_returns_jsonb() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "nonexistent_user".into(),
                json!({
                    "client_id": "nonexistent_client",
                    "client_generation": 1,
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 0,
                    "scopes": {},
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
        assert_eq!(resp["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_pull_generation_precedes_schema_mismatch() {
        setup_test_tables();
        register_client("u1", "c1");
        let (schema_version, schema_hash) = latest_schema_ref();
        let response: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "u1".into(),
                json!({
                    "client_id": "c1",
                    "client_generation": 2,
                    "schema": { "version": schema_version + 1, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": { "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) },
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
