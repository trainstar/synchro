    #[pg_test]
    fn test_push_cas_matrix_has_all_protocol_three_conflicts() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);

        let absent_insert = "10000000-0000-4000-8000-000000000001";
        let absent_update = "10000000-0000-4000-8000-000000000002";
        let absent_delete = "10000000-0000-4000-8000-000000000003";
        let live_insert = "10000000-0000-4000-8000-000000000004";
        let live_update = "10000000-0000-4000-8000-000000000005";
        let live_delete = "10000000-0000-4000-8000-000000000006";
        let stale_update = "10000000-0000-4000-8000-000000000007";
        let stale_delete = "10000000-0000-4000-8000-000000000008";
        let deleted_insert = "10000000-0000-4000-8000-000000000009";
        let deleted_update = "10000000-0000-4000-8000-00000000000a";
        let deleted_delete = "10000000-0000-4000-8000-00000000000b";

        insert_live_order(live_insert, user_id, "live insert");
        let live_update_version = insert_live_order(live_update, user_id, "live update");
        let live_delete_version = insert_live_order(live_delete, user_id, "live delete");
        insert_live_order(stale_update, user_id, "stale update");
        insert_live_order(stale_delete, user_id, "stale delete");
        let deleted_version = insert_deleted_order(deleted_insert, user_id, "deleted insert");
        insert_deleted_order(deleted_update, user_id, "deleted update");
        insert_deleted_order(deleted_delete, user_id, "deleted delete");

        let response = push_client(
            user_id,
            client_id,
            "cas-matrix",
            vec![
                push_mutation(
                    user_id,
                    client_id,
                    "cas-absent-insert",
                    "test_orders",
                    "insert",
                    absent_insert,
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("created"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-absent-update",
                    "test_orders",
                    "update",
                    absent_update,
                    Some("missing-version"),
                    Some(&[("title", json!("missing"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-absent-delete",
                    "test_orders",
                    "delete",
                    absent_delete,
                    Some("missing-version"),
                    None,
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-live-insert",
                    "test_orders",
                    "insert",
                    live_insert,
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("duplicate"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-live-update",
                    "test_orders",
                    "update",
                    live_update,
                    Some(live_update_version.as_str()),
                    Some(&[("title", json!("updated"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-live-delete",
                    "test_orders",
                    "delete",
                    live_delete,
                    Some(live_delete_version.as_str()),
                    None,
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-stale-update",
                    "test_orders",
                    "update",
                    stale_update,
                    Some("stale-version"),
                    Some(&[("title", json!("stale"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-stale-delete",
                    "test_orders",
                    "delete",
                    stale_delete,
                    Some("stale-version"),
                    None,
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-deleted-insert",
                    "test_orders",
                    "insert",
                    deleted_insert,
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("revive"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-deleted-update",
                    "test_orders",
                    "update",
                    deleted_update,
                    Some(deleted_version.as_str()),
                    Some(&[("title", json!("revive"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "cas-deleted-delete",
                    "test_orders",
                    "delete",
                    deleted_delete,
                    Some(deleted_version.as_str()),
                    None,
                ),
            ],
        );

        let accepted = response.json["accepted"]
            .as_array()
            .expect("accepted mutation array");
        let rejected = response.json["rejected"]
            .as_array()
            .expect("rejected mutation array");
        let accepted_ids = accepted
            .iter()
            .map(|outcome| outcome["mutation_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            accepted_ids,
            vec![
                mutation_id(user_id, client_id, "cas-absent-insert"),
                mutation_id(user_id, client_id, "cas-live-update"),
                mutation_id(user_id, client_id, "cas-live-delete"),
            ]
        );
        assert!(accepted
            .iter()
            .all(|outcome| outcome["status"] == "applied"));

        let rejected_ids = rejected
            .iter()
            .map(|outcome| outcome["mutation_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            rejected_ids,
            vec![
                mutation_id(user_id, client_id, "cas-absent-update"),
                mutation_id(user_id, client_id, "cas-absent-delete"),
                mutation_id(user_id, client_id, "cas-live-insert"),
                mutation_id(user_id, client_id, "cas-stale-update"),
                mutation_id(user_id, client_id, "cas-stale-delete"),
                mutation_id(user_id, client_id, "cas-deleted-insert"),
                mutation_id(user_id, client_id, "cas-deleted-update"),
                mutation_id(user_id, client_id, "cas-deleted-delete"),
            ]
        );
        let codes = rejected
            .iter()
            .map(|outcome| outcome["code"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                "row_not_found",
                "row_not_found",
                "row_already_exists",
                "version_conflict",
                "version_conflict",
                "row_deleted",
                "row_deleted",
                "row_deleted",
            ]
        );
    }

    #[pg_test]
    fn test_push_accepted_writes_return_fenced_canonical_outcomes() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let order_id = "20000000-0000-4000-8000-000000000001";

        let inserted = push_client(
            user_id,
            client_id,
            "accepted-insert",
            vec![push_mutation(
                user_id,
                client_id,
                "accepted-insert",
                "test_orders",
                "insert",
                order_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("Inserted"))]),
            )],
        );
        let inserted = &inserted.json["accepted"][0];
        let insert_version = inserted["server_version"]
            .as_str()
            .expect("insert server version")
            .to_string();
        assert!(!insert_version.is_empty());
        assert_eq!(
            inserted["server_row"][field_id("test_orders", "title")].as_str(),
            Some("Inserted")
        );
        assert_row_outcome_matches_source(inserted, "test_orders", order_id);
        assert_matching_fence(
            mutation_id(user_id, client_id, "accepted-insert").as_str(),
            insert_version.as_str(),
        );

        let updated = push_client(
            user_id,
            client_id,
            "accepted-update",
            vec![push_mutation(
                user_id,
                client_id,
                "accepted-update",
                "test_orders",
                "update",
                order_id,
                Some(insert_version.as_str()),
                Some(&[("title", json!("Updated"))]),
            )],
        );
        let updated = &updated.json["accepted"][0];
        let update_version = updated["server_version"]
            .as_str()
            .expect("update server version")
            .to_string();
        assert!(!update_version.is_empty());
        assert_ne!(update_version, insert_version);
        assert_eq!(
            updated["server_row"][field_id("test_orders", "title")].as_str(),
            Some("Updated")
        );
        assert_row_outcome_matches_source(updated, "test_orders", order_id);
        assert_matching_fence(
            mutation_id(user_id, client_id, "accepted-update").as_str(),
            update_version.as_str(),
        );

        let deleted = push_client(
            user_id,
            client_id,
            "accepted-soft-delete",
            vec![push_mutation(
                user_id,
                client_id,
                "accepted-soft-delete",
                "test_orders",
                "delete",
                order_id,
                Some(update_version.as_str()),
                None,
            )],
        );
        let deleted = &deleted.json["accepted"][0];
        let delete_version = deleted["server_version"]
            .as_str()
            .expect("soft-delete server version")
            .to_string();
        assert!(!delete_version.is_empty());
        assert_ne!(delete_version, update_version);
        assert!(deleted["server_row"][field_id("test_orders", "deleted_at")]
            .as_str()
            .is_some());
        assert_row_outcome_matches_source(deleted, "test_orders", order_id);
        assert_matching_fence(
            mutation_id(user_id, client_id, "accepted-soft-delete").as_str(),
            delete_version.as_str(),
        );
        let soft_deleted: Option<bool> = Spi::get_one_with_args(
            "SELECT deleted_at IS NOT NULL FROM test_orders WHERE id = $1::uuid",
            &[order_id.into()],
        )
        .unwrap();
        assert_eq!(soft_deleted, Some(true));

        let bare_id = "20000000-0000-4000-8000-000000000002";
        Spi::run_with_args(
            "INSERT INTO test_bare_items (id, name) VALUES ($1::uuid, 'bare')",
            &[bare_id.into()],
        )
        .unwrap();
        let bare_version = current_row_version("test_bare_items", bare_id);
        let hard_deleted = push_client(
            user_id,
            client_id,
            "accepted-hard-delete",
            vec![push_mutation(
                user_id,
                client_id,
                "accepted-hard-delete",
                "test_bare_items",
                "delete",
                bare_id,
                Some(bare_version.as_str()),
                None,
            )],
        );
        let hard_deleted = &hard_deleted.json["accepted"][0];
        let hard_delete_version = hard_deleted["server_version"]
            .as_str()
            .expect("hard-delete server version")
            .to_string();
        assert!(!hard_delete_version.is_empty());
        assert_ne!(hard_delete_version, bare_version);
        assert!(hard_deleted.get("server_row").is_none());
        assert!(hard_deleted.get("row_checksum").is_none());
        assert_matching_fence(
            mutation_id(user_id, client_id, "accepted-hard-delete").as_str(),
            hard_delete_version.as_str(),
        );
        let hard_deleted_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM test_bare_items WHERE id = $1::uuid",
            &[bare_id.into()],
        )
        .unwrap();
        assert_eq!(hard_deleted_count, Some(0));
    }

    #[pg_test]
    fn test_push_ignores_client_time_and_cas_selects_one_winner() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let update_wins = "21000000-0000-4000-8000-000000000001";
        let delete_wins = "21000000-0000-4000-8000-000000000002";
        let equal_time = "21000000-0000-4000-8000-000000000003";
        insert_live_order(update_wins, user_id, "update wins");
        insert_live_order(delete_wins, user_id, "delete wins");
        insert_live_order(equal_time, user_id, "equal time");
        Spi::run(
            "UPDATE test_orders
             SET updated_at = '2026-07-18T13:59:01Z'::timestamptz
             WHERE id IN (
                 '21000000-0000-4000-8000-000000000001'::uuid,
                 '21000000-0000-4000-8000-000000000002'::uuid,
                 '21000000-0000-4000-8000-000000000003'::uuid
             )",
        )
        .unwrap();
        let update_version = current_row_version("test_orders", update_wins);
        let delete_version = current_row_version("test_orders", delete_wins);
        let equal_version = current_row_version("test_orders", equal_time);

        let mut past_update = push_mutation(
            user_id,
            client_id,
            "time-past-update",
            "test_orders",
            "update",
            update_wins,
            Some(update_version.as_str()),
            Some(&[("title", json!("past accepted"))]),
        );
        past_update["client_version"] = json!("2000-01-01T00:00:00.000000Z");
        let mut future_delete_loses = push_mutation(
            user_id,
            client_id,
            "time-future-delete-loses",
            "test_orders",
            "delete",
            update_wins,
            Some(update_version.as_str()),
            None,
        );
        future_delete_loses["client_version"] = json!("2099-01-01T00:00:00.000000Z");
        let mut future_delete = push_mutation(
            user_id,
            client_id,
            "time-future-delete",
            "test_orders",
            "delete",
            delete_wins,
            Some(delete_version.as_str()),
            None,
        );
        future_delete["client_version"] = json!("2099-01-01T00:00:00.000000Z");
        let mut equal_update_loses = push_mutation(
            user_id,
            client_id,
            "time-equal-update-loses",
            "test_orders",
            "update",
            delete_wins,
            Some(delete_version.as_str()),
            Some(&[("title", json!("equal loses"))]),
        );
        equal_update_loses["client_version"] = json!(TEST_CLIENT_VERSION);
        let equal_update = push_mutation(
            user_id,
            client_id,
            "time-equal-update",
            "test_orders",
            "update",
            equal_time,
            Some(equal_version.as_str()),
            Some(&[("title", json!("equal accepted"))]),
        );

        let response = push_client(
            user_id,
            client_id,
            "time-and-cas",
            vec![
                past_update,
                future_delete_loses,
                future_delete,
                equal_update_loses,
                equal_update,
            ],
        );
        let accepted_ids = response.json["accepted"]
            .as_array()
            .expect("accepted mutation array")
            .iter()
            .map(|outcome| outcome["mutation_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            accepted_ids,
            vec![
                mutation_id(user_id, client_id, "time-past-update"),
                mutation_id(user_id, client_id, "time-future-delete"),
                mutation_id(user_id, client_id, "time-equal-update"),
            ]
        );
        let rejected = response.json["rejected"]
            .as_array()
            .expect("rejected mutation array");
        assert_eq!(rejected.len(), 2);
        assert_eq!(
            rejected
                .iter()
                .map(|outcome| outcome["code"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["version_conflict", "row_deleted"]
        );
        assert_eq!(
            rejected
                .iter()
                .map(|outcome| outcome["mutation_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                mutation_id(user_id, client_id, "time-future-delete-loses"),
                mutation_id(user_id, client_id, "time-equal-update-loses"),
            ]
        );
    }

    #[pg_test]
    fn test_push_mixed_outcomes_keep_partition_order_and_one_epoch() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);

        Spi::run("ALTER TABLE test_orders ADD COLUMN legacy_note TEXT").unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_orders',
                 $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'enabled',
                 ARRAY['internal_notes']
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();
        let legacy_schema = schema_ref_value();
        let legacy_field_id = field_id("test_orders", "legacy_note");

        Spi::run("ALTER TABLE test_orders DROP COLUMN legacy_note").unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_orders',
                 $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'enabled',
                 ARRAY['internal_notes']
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();

        let conflict_id = "80000000-0000-4000-8000-000000000001";
        insert_live_order(conflict_id, user_id, "existing");
        let mut never_synced_pk = serde_json::Map::new();
        never_synced_pk.insert(
            test_uuid("mixed-never-synced-pk"),
            Value::String("80000000-0000-4000-8000-000000000006".to_string()),
        );
        let mut never_synced_columns = serde_json::Map::new();
        never_synced_columns.insert(test_uuid("mixed-never-synced-field"), json!("never"));
        let never_synced = json!({
            "mutation_id": mutation_id(user_id, client_id, "mixed-never-synced"),
            "table": test_uuid("mixed-never-synced-table"),
            "pk": never_synced_pk,
            "authored_schema": schema_ref_value(),
            "op": "insert",
            "client_version": TEST_CLIENT_VERSION,
            "columns": never_synced_columns,
        });
        let mut incompatible = push_mutation(
            user_id,
            client_id,
            "mixed-schema-incompatible",
            "test_orders",
            "insert",
            "80000000-0000-4000-8000-000000000007",
            None,
            Some(&[("user_id", json!(user_id))]),
        );
        incompatible["authored_schema"] = legacy_schema;
        let mut incompatible_columns = serde_json::Map::new();
        incompatible_columns.insert(legacy_field_id, json!("preserved authored value"));
        incompatible["columns"] = Value::Object(incompatible_columns);
        let incompatible_intent = incompatible.clone();
        let incompatible_mutation_id = incompatible["mutation_id"]
            .as_str()
            .expect("schema-incompatible mutation ID")
            .to_string();
        let submitted_schema = schema_ref_value();

        let epoch_before = accepted_write_epoch(user_id, client_id);
        let response = push_client(
            user_id,
            client_id,
            "mixed-outcomes",
            vec![
                push_mutation(
                    user_id,
                    client_id,
                    "mixed-applied-first",
                    "test_orders",
                    "insert",
                    "80000000-0000-4000-8000-000000000002",
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("first"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "mixed-conflict",
                    "test_orders",
                    "insert",
                    conflict_id,
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("conflict"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "mixed-policy",
                    "test_products",
                    "insert",
                    "80000000-0000-4000-8000-000000000003",
                    None,
                    Some(&[("name", json!("read only"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "mixed-applied-second",
                    "test_orders",
                    "insert",
                    "80000000-0000-4000-8000-000000000004",
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("second"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "mixed-validation",
                    "test_orders",
                    "insert",
                    "80000000-0000-4000-8000-000000000005",
                    None,
                    Some(&[
                        ("user_id", json!(user_id)),
                        ("updated_at", json!(TEST_CLIENT_VERSION)),
                    ]),
                ),
                never_synced,
                incompatible,
            ],
        );

        let accepted = response.json["accepted"]
            .as_array()
            .expect("accepted mutation array");
        let rejected = response.json["rejected"]
            .as_array()
            .expect("rejected mutation array");
        let accepted_ids = accepted
            .iter()
            .map(|outcome| outcome["mutation_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            accepted_ids,
            vec![
                mutation_id(user_id, client_id, "mixed-applied-first"),
                mutation_id(user_id, client_id, "mixed-applied-second"),
            ]
        );
        let rejected_ids = rejected
            .iter()
            .map(|outcome| outcome["mutation_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            rejected_ids,
            vec![
                mutation_id(user_id, client_id, "mixed-conflict"),
                mutation_id(user_id, client_id, "mixed-policy"),
                mutation_id(user_id, client_id, "mixed-validation"),
                mutation_id(user_id, client_id, "mixed-never-synced"),
                mutation_id(user_id, client_id, "mixed-schema-incompatible"),
            ]
        );
        let rejected_codes = rejected
            .iter()
            .map(|outcome| outcome["code"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rejected_codes,
            vec![
                "row_already_exists",
                "policy_rejected",
                "validation_failed",
                "table_not_synced",
                "schema_incompatible",
            ]
        );
        assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before + 1);

        let ledger: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'request_ordinal', request_ordinal,
                 'authored_schema', jsonb_build_object(
                     'version', authored_schema_version,
                     'hash', authored_schema_hash
                 ),
                 'submitted_schema', jsonb_build_object(
                     'version', submitted_schema_version,
                     'hash', submitted_schema_hash
                 ),
                 'outcome_schema', jsonb_build_object(
                     'version', outcome_schema_version,
                     'hash', outcome_schema_hash
                 ),
                 'outcome_status', outcome_status,
                 'rejection_code', rejection_code,
                 'sealed_request', convert_from(sealed_canonical_request, 'UTF8')::jsonb,
                 'sealed_response', convert_from(sealed_canonical_response, 'UTF8')::jsonb
             )
             FROM sync_push_mutations
             WHERE user_id = $1 AND client_id = $2 AND mutation_id = $3::uuid",
            &[
                user_id.into(),
                client_id.into(),
                incompatible_mutation_id.clone().into(),
            ],
        )
        .unwrap();
        let ledger = ledger.expect("schema-incompatible mutation ledger").0;
        let incompatible_outcome = rejected
            .iter()
            .find(|outcome| {
                outcome["mutation_id"].as_str() == Some(incompatible_mutation_id.as_str())
            })
            .expect("schema-incompatible outcome");
        assert_eq!(ledger["request_ordinal"], json!(7));
        assert_eq!(ledger["authored_schema"], incompatible_intent["authored_schema"]);
        assert_eq!(ledger["submitted_schema"], submitted_schema);
        assert_eq!(ledger["outcome_schema"], submitted_schema);
        assert_eq!(ledger["outcome_status"], json!("rejected_terminal"));
        assert_eq!(ledger["rejection_code"], json!("schema_incompatible"));
        assert_eq!(ledger["sealed_request"], incompatible_intent);
        assert_eq!(ledger["sealed_response"], *incompatible_outcome);
    }

    #[pg_test]
    fn test_push_conflict_ledger_persists_its_conflict_code() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let record_id = "90000000-0000-4000-8000-000000000001";
        let mutation_label = "ledger-conflict";
        register_client(user_id, client_id);
        insert_live_order(record_id, user_id, "existing");

        let response = push_client(
            user_id,
            client_id,
            "ledger-conflict",
            vec![push_mutation(
                user_id,
                client_id,
                mutation_label,
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("duplicate"))]),
            )],
        );
        assert_eq!(response.json["rejected"][0]["status"], "conflict");
        assert_eq!(response.json["rejected"][0]["code"], "row_already_exists");

        let stored_status: Option<String> = Spi::get_one_with_args(
            "SELECT outcome_status
             FROM sync_push_mutations
             WHERE user_id = $1 AND client_id = $2 AND mutation_id = $3::uuid",
            &[
                user_id.into(),
                client_id.into(),
                mutation_id(user_id, client_id, mutation_label).into(),
            ],
        )
        .unwrap();
        let stored_code: Option<String> = Spi::get_one_with_args(
            "SELECT rejection_code
             FROM sync_push_mutations
             WHERE user_id = $1 AND client_id = $2 AND mutation_id = $3::uuid",
            &[
                user_id.into(),
                client_id.into(),
                mutation_id(user_id, client_id, mutation_label).into(),
            ],
        )
        .unwrap();
        assert_eq!(stored_status.as_deref(), Some("conflict"));
        assert_eq!(stored_code.as_deref(), Some("row_already_exists"));
    }

    #[pg_test]
    fn test_push_domain_failure_keeps_valid_mutation() {
        setup_test_tables();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_checked_orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                title TEXT NOT NULL,
                units INTEGER NOT NULL CHECK (units > 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_checked_orders',
                 $$SELECT ARRAY['user:' || user_id] FROM test_checked_orders WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'enabled'
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();

        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let valid_id = "b0000000-0000-4000-8000-000000000001";
        let invalid_id = "b0000000-0000-4000-8000-000000000002";
        let epoch_before = accepted_write_epoch(user_id, client_id);
        let response = push_client(
            user_id,
            client_id,
            "domain-failure",
            vec![
                push_mutation(
                    user_id,
                    client_id,
                    "domain-valid",
                    "test_orders",
                    "insert",
                    valid_id,
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!("valid"))]),
                ),
                push_mutation(
                    user_id,
                    client_id,
                    "domain-invalid",
                    "test_checked_orders",
                    "insert",
                    invalid_id,
                    None,
                    Some(&[
                        ("user_id", json!(user_id)),
                        ("title", json!("invalid")),
                        ("units", json!(0)),
                    ]),
                ),
            ],
        );
        assert_eq!(response.json["accepted"][0]["status"], "applied");
        assert_eq!(response.json["rejected"][0]["status"], "rejected_terminal");
        assert_eq!(response.json["rejected"][0]["code"], "validation_failed");
        let valid_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM test_orders WHERE id = $1::uuid",
            &[valid_id.into()],
        )
        .unwrap();
        let invalid_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM test_checked_orders WHERE id = $1::uuid",
            &[invalid_id.into()],
        )
        .unwrap();
        assert_eq!(valid_count, Some(1));
        assert_eq!(invalid_count, Some(0));
        assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before + 1);
    }

    #[pg_test]
    fn test_push_json_source_field_returns_valid_row_checksum() {
        setup_test_tables();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_json_orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                document JSONB NOT NULL,
                optional_document JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_json_orders',
                 $$SELECT ARRAY['user:' || user_id] FROM test_json_orders WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'enabled'
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();

        let user_id = "u1";
        let client_id = "c1";
        let record_id = "c0000000-0000-4000-8000-000000000001";
        let document = r#"{"active":true,"tags":["a","b"]}"#;
        register_client(user_id, client_id);
        let response = push_client(
            user_id,
            client_id,
            "json-checksum",
            vec![push_mutation(
                user_id,
                client_id,
                "json-checksum",
                "test_json_orders",
                "insert",
                record_id,
                None,
                Some(&[
                    ("user_id", json!(user_id)),
                    ("document", json!(document)),
                    ("optional_document", serde_json::Value::Null),
                ]),
            )],
        );
        let outcome = &response.json["accepted"][0];
        assert_eq!(outcome["status"], "applied");
        assert_eq!(
            outcome["server_row"][field_id("test_json_orders", "document")].as_str(),
            Some(document)
        );
        assert!(outcome["server_row"][field_id("test_json_orders", "optional_document")].is_null());
        assert_checksum_object(outcome);
        assert_row_outcome_matches_source(outcome, "test_json_orders", record_id);
    }

    #[pg_test]
    fn test_push_invalid_shapes_and_bounds_create_no_ledgers() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let valid = push_mutation(
            user_id,
            client_id,
            "invalid-shape",
            "test_orders",
            "insert",
            "d0000000-0000-4000-8000-000000000001",
            None,
            Some(&[("user_id", json!(user_id)), ("title", json!("shape"))]),
        );

        let mut missing_base = valid.clone();
        missing_base["op"] = json!("update");
        let malformed = execute_push(
            user_id,
            &push_request(
                user_id,
                client_id,
                "invalid-missing-base",
                vec![missing_base],
            ),
        );
        assert_eq!(
            malformed.json["error"]["code"].as_str(),
            Some("invalid_request")
        );

        let duplicate = execute_push(
            user_id,
            &push_request(
                user_id,
                client_id,
                "invalid-duplicate-id",
                vec![valid.clone(), valid.clone()],
            ),
        );
        assert_eq!(
            duplicate.json["error"]["code"].as_str(),
            Some("invalid_request")
        );

        let mut unknown_member = push_request(
            user_id,
            client_id,
            "invalid-unknown-member",
            vec![valid.clone()],
        );
        unknown_member["unexpected"] = json!(true);
        let unknown_member = execute_push(user_id, &unknown_member);
        assert_eq!(
            unknown_member.json["error"]["code"].as_str(),
            Some("invalid_request")
        );

        let mut upsert = valid.clone();
        upsert["op"] = json!("upsert");
        let upsert = execute_push(
            user_id,
            &push_request(user_id, client_id, "invalid-upsert", vec![upsert]),
        );
        assert_eq!(
            upsert.json["error"]["code"].as_str(),
            Some("invalid_request")
        );

        let too_many = execute_push(
            user_id,
            &push_request(
                user_id,
                client_id,
                "invalid-count",
                vec![valid.clone(); 1_001],
            ),
        );
        assert_eq!(
            too_many.json["error"]["code"].as_str(),
            Some("invalid_request")
        );

        let oversized_title = "x".repeat(66_000);
        let oversized = push_mutation(
            user_id,
            client_id,
            "invalid-normalized-size",
            "test_orders",
            "insert",
            "d0000000-0000-4000-8000-000000000002",
            None,
            Some(&[
                ("user_id", json!(user_id)),
                ("title", json!(oversized_title)),
            ]),
        );
        let oversized = execute_push(
            user_id,
            &push_request(
                user_id,
                client_id,
                "invalid-normalized-size",
                vec![oversized],
            ),
        );
        assert_eq!(
            oversized.json["error"]["code"].as_str(),
            Some("invalid_request")
        );
        assert_eq!(push_ledger_counts(user_id, client_id), (0, 0));
        let source_count: Option<i64> = Spi::get_one("SELECT count(*) FROM test_orders").unwrap();
        assert_eq!(source_count, Some(0));
    }

    #[pg_test]
    fn test_push_keeps_rls_context_without_value_leaks() {
        setup_test_tables();
        let user_id = "rls-user";
        let client_id = "c1";
        let secret_title = "rls-secret-value";
        register_client(user_id, client_id);
        Spi::run("ALTER TABLE test_orders ENABLE ROW LEVEL SECURITY").unwrap();
        Spi::run("ALTER TABLE test_orders FORCE ROW LEVEL SECURITY").unwrap();
        Spi::run("DROP POLICY synchro_test_owner_all ON test_orders").unwrap();
        Spi::run(
            "CREATE POLICY test_push_rls ON test_orders
             TO synchro_owner
             USING (user_id = current_setting('app.user_id', true))
             WITH CHECK (user_id = current_setting('app.user_id', true))",
        )
        .unwrap();
        Spi::run(
            "ALTER TABLE test_orders
             ADD CONSTRAINT test_push_rls_secret CHECK (title <> 'rls-secret-value')",
        )
        .unwrap();

        let allowed = push_client(
            user_id,
            client_id,
            "rls-allowed",
            vec![push_mutation(
                user_id,
                client_id,
                "rls-allowed",
                "test_orders",
                "insert",
                "e0000000-0000-4000-8000-000000000001",
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("allowed"))]),
            )],
        );
        assert_eq!(allowed.json["accepted"][0]["status"], "applied");

        let rejected = push_client(
            user_id,
            client_id,
            "rls-rejected",
            vec![push_mutation(
                user_id,
                client_id,
                "rls-rejected",
                "test_orders",
                "insert",
                "e0000000-0000-4000-8000-000000000002",
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!(secret_title))]),
            )],
        );
        assert_eq!(rejected.json["rejected"][0]["status"], "rejected_terminal");
        assert_eq!(rejected.json["rejected"][0]["code"], "validation_failed");
        assert!(!rejected.raw.contains(user_id));
        assert!(!rejected.raw.contains(secret_title));

        Spi::run("ALTER TABLE test_orders DROP CONSTRAINT test_push_rls_secret").unwrap();
        Spi::run("DROP POLICY test_push_rls ON test_orders").unwrap();
        Spi::run(
            "CREATE POLICY synchro_test_owner_all ON test_orders
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true)",
        )
        .unwrap();
        Spi::run("ALTER TABLE test_orders DISABLE ROW LEVEL SECURITY").unwrap();
    }

    #[pg_test]
    fn test_push_policy_cannot_repair_invalid_authored_value() {
        setup_test_tables();
        let user_id = "policy-user";
        let client_id = "c1";
        let record_id = "e0000000-0000-4000-8000-000000000003";
        register_client(user_id, client_id);
        Spi::run(
            "CREATE FUNCTION synchro_write_protect(TEXT, TEXT, TEXT, JSONB)
             RETURNS JSONB
             LANGUAGE sql
             IMMUTABLE
             STRICT
             AS $policy$
                 SELECT COALESCE(
                     jsonb_object_agg(
                         key,
                         CASE WHEN jsonb_typeof(value) = 'number'
                              THEN to_jsonb('repaired'::text)
                              ELSE value
                         END
                     ),
                     '{}'::jsonb
                 )
                 FROM jsonb_each($4)
             $policy$",
        )
        .unwrap();

        let response = push_client(
            user_id,
            client_id,
            "policy-invalid-authored",
            vec![push_mutation(
                user_id,
                client_id,
                "policy-invalid-authored",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!(42))]),
            )],
        );
        assert_eq!(response.json["rejected"][0]["status"], "rejected_terminal");
        assert_eq!(response.json["rejected"][0]["code"], "validation_failed");
        let source_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM test_orders WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap();
        assert_eq!(source_count, Some(0));
        Spi::run("DROP FUNCTION synchro_write_protect(TEXT, TEXT, TEXT, JSONB)").unwrap();
    }

    #[pg_test]
    fn test_push_schema_mismatch_creates_no_ledger() {
        setup_test_tables();
        register_client("u1", "c1");
        let mut request = push_request(
            "u1",
            "c1",
            "schema-mismatch",
            vec![push_mutation(
                "u1",
                "c1",
                "schema-mismatch",
                "test_orders",
                "insert",
                "f0000000-0000-4000-8000-000000000001",
                None,
                Some(&[("user_id", json!("u1")), ("title", json!("mismatch"))]),
            )],
        );
        request["schema"] = json!({
            "version": latest_schema_ref().0 + 1,
            "hash": "1111111111111111111111111111111111111111111111111111111111111111"
        });

        let response = execute_push("u1", &request);
        assert_eq!(
            response.json["error"]["code"].as_str(),
            Some("schema_mismatch")
        );
        assert_eq!(response.json["error"]["retryable"].as_bool(), Some(false));
        assert_eq!(push_ledger_counts("u1", "c1"), (0, 0));
    }
