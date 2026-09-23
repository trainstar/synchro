    use std::collections::HashMap;

    use synchro_core::change::ChangeOperation;

    use crate::registry::TableRegistration;
    use crate::wal_decoder::{
        RelationKey, TupleImage, TupleValue, WalEvent, WalLogicalMessage, WalTransaction,
    };

    #[pg_test]
    fn test_push_preserves_transaction_wide_fence_ordinals() {
        setup_test_tables();
        let user_id = "ordinal-user";
        let client_id = "c1";
        let direct_id = "e0000000-0000-4000-8000-000000000004";
        let first_push_id = "e0000000-0000-4000-8000-000000000005";
        let second_push_id = "e0000000-0000-4000-8000-000000000006";
        register_client(user_id, client_id);
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, $2, 'direct')",
            &[direct_id.into(), user_id.into()],
        )
        .unwrap();

        for (label, record_id) in [
            ("ordinal-first", first_push_id),
            ("ordinal-second", second_push_id),
        ] {
            let response = push_client(
                user_id,
                client_id,
                label,
                vec![push_mutation(
                    (user_id, client_id),
                    label,
                    "test_orders",
                    "insert",
                    record_id,
                    None,
                    Some(&[("user_id", json!(user_id)), ("title", json!(label))]),
                )],
            );
            assert_eq!(response.json["accepted"][0]["status"], "applied");
        }

        let ordered: Option<bool> = Spi::get_one_with_args(
            "SELECT direct.dml_ordinal < first_push.dml_ordinal
                    AND first_push.dml_ordinal < second_push.dml_ordinal
             FROM sync_write_fences direct
             CROSS JOIN sync_write_fences first_push
             CROSS JOIN sync_write_fences second_push
             WHERE direct.new_record_id = $1
               AND first_push.new_record_id = $2
               AND second_push.new_record_id = $3",
            &[
                direct_id.into(),
                first_push_id.into(),
                second_push_id.into(),
            ],
        )
        .unwrap();
        assert_eq!(ordered, Some(true));
    }

    #[pg_test]
    fn test_backfill_bucket_edges_populates_existing_rows() {
        setup_test_tables();
        Spi::run(
            "INSERT INTO test_products (id, name, price)
             VALUES ('13131313-1313-1313-1313-131313131313', 'Backfill Product', 12)",
        )
        .unwrap();
        insert_changelog(
            "global",
            "test_products",
            "13131313-1313-1313-1313-131313131313",
            1,
        );

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_backfill_bucket_edges()").unwrap();
        let resp = resp.unwrap().0;
        assert!(resp["edges"].as_i64().unwrap_or(0) > 0);
        assert_eq!(resp["batch_size"], 1_000);

        let edge_count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_bucket_edges
             WHERE table_name = 'test_products'
               AND record_id = '13131313-1313-1313-1313-131313131313'
               AND bucket_id = 'global'",
        )
        .unwrap();
        assert_eq!(edge_count, Some(1));

        let row_version: Option<String> = Spi::get_one(
            "SELECT row_version::text
             FROM sync_bucket_edges
             WHERE table_name = 'test_products'
               AND record_id = '13131313-1313-1313-1313-131313131313'
               AND bucket_id = 'global'",
        )
        .unwrap();
        assert!(row_version.is_some());
    }

    #[pg_test]
    fn test_backfill_bucket_edges_enforces_batch_boundaries() {
        setup_test_tables();
        for (record_id, sequence) in [
            ("14141414-1414-1414-1414-141414141414", 1),
            ("15151515-1515-1515-1515-151515151515", 2),
        ] {
            Spi::run_with_args(
                "INSERT INTO test_products (id, name, price)
                 VALUES ($1::uuid, 'Backfill Boundary Product', $2)",
                &[record_id.into(), sequence.into()],
            )
            .unwrap();
            insert_changelog("global", "test_products", record_id, sequence);
        }

        let lower: pgrx::JsonB = Spi::get_one(
            "SELECT synchro_backfill_bucket_edges('test_products', 1)",
        )
        .unwrap()
        .expect("lower backfill boundary response");
        assert_eq!(lower.0["batch_size"], 1);
        assert_eq!(lower.0["batch_count"], 2);

        let upper: pgrx::JsonB = Spi::get_one(
            "SELECT synchro_backfill_bucket_edges('test_products', 1000)",
        )
        .unwrap()
        .expect("upper backfill boundary response");
        assert_eq!(upper.0["batch_size"], 1_000);
        assert_eq!(upper.0["batch_count"], 1);

        let accepted = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<pgrx::JsonB>(
                "SELECT synchro_backfill_bucket_edges('test_products', 1001)",
            )
            .is_ok()
        }))
        .catch_others(|_| false)
        .execute();
        assert!(!accepted, "batch size above 1000 must be rejected");
    }

    #[pg_test]
    fn test_reevaluation_projection_batch_boundary() {
        setup_test_tables();
        let projections: pgrx::JsonB = Spi::get_one(
            "WITH context AS (
                 SELECT runtime.stream_generation, registry.relation_id,
                        registry.registry_generation
                 FROM sync_runtime_state runtime
                 JOIN sync_registry_generations generation
                   ON generation.stream_generation = runtime.stream_generation
                  AND generation.state = 'active'
                 JOIN sync_registry registry
                   ON registry.registry_generation = generation.generation
                  AND registry.table_name = 'test_orders'
                 WHERE runtime.singleton
             ), inserted AS (
                 INSERT INTO sync_captured_rows (
                     relation_id, record_id, row_data, row_version, checksum, deleted,
                     source_stream_generation, source_commit_lsn, source_event_ordinal,
                     registry_generation
                 )
                 SELECT context.relation_id,
                        '00000000-0000-4000-8001-' || lpad(series::text, 12, '0'),
                        jsonb_build_object('sequence', series),
                        ('00000000-0000-4000-8002-' || lpad(series::text, 12, '0'))::uuid,
                        decode(lpad(to_hex(series), 64, '0'), 'hex'), false,
                        context.stream_generation, '0/10'::pg_lsn, 0,
                        context.registry_generation
                 FROM context
                 CROSS JOIN generate_series(1, 501) AS series
                 RETURNING relation_id, record_id, registry_generation,
                           row_version, checksum, deleted
             )
             SELECT jsonb_agg(jsonb_build_object(
                        'event_ordinal', 7,
                        'relation_id', relation_id,
                        'registry_generation', registry_generation,
                        'record_id', record_id,
                        'row_version', row_version,
                        'checksum_hex', encode(checksum, 'hex'),
                        'deleted', deleted
                    ) ORDER BY record_id)
             FROM inserted",
        )
        .unwrap()
        .expect("reevaluation projection batch inputs");
        let mut projections = projections
            .0
            .as_array()
            .expect("projection input array")
            .clone();
        let stream_generation: String = Spi::get_one(
            "SELECT stream_generation FROM sync_runtime_state WHERE singleton",
        )
        .unwrap()
        .expect("stream generation");

        Spi::connect_mut(|client| {
            for (field, value) in [
                ("registry_generation", json!(-1)),
                (
                    "row_version",
                    json!("ffffffff-ffff-4fff-8fff-ffffffffffff"),
                ),
                ("checksum_hex", json!("f".repeat(64))),
                ("deleted", json!(true)),
            ] {
                let mut invalid = projections[..2].to_vec();
                invalid[1][field] = value;
                assert!(crate::bgworker::persist_reevaluation_projection_batch(
                    client,
                    &stream_generation,
                    0x20,
                    invalid,
                )
                .is_err());
            }
            assert!(crate::bgworker::persist_reevaluation_projection_batch(
                client,
                &stream_generation,
                0x20,
                projections.clone(),
            )
            .is_err());
            let inserted_before_valid_batches = client
                .select(
                    "SELECT count(*)::bigint AS count
                     FROM sync_captured_projections
                     WHERE commit_lsn = '0/20'::pg_lsn AND event_ordinal = 7",
                    None,
                    &[],
                )?
                .first()
                .get_by_name::<i64, &str>("count")?;
            assert_eq!(inserted_before_valid_batches, Some(0));
            let final_batch = projections.split_off(500);
            crate::bgworker::persist_reevaluation_projection_batch(
                client,
                &stream_generation,
                0x20,
                projections,
            )
            .expect("first reevaluation projection batch");
            crate::bgworker::persist_reevaluation_projection_batch(
                client,
                &stream_generation,
                0x20,
                final_batch,
            )
            .expect("second reevaluation projection batch");
            Ok::<_, pgrx::spi::Error>(())
        })
        .unwrap();

        let counts: pgrx::JsonB = Spi::get_one(
            "SELECT jsonb_build_object(
                 'rows', count(*),
                 'matches', bool_and(
                     projection.row_data = captured.row_data
                     AND projection.row_version = captured.row_version
                     AND projection.checksum = captured.checksum
                     AND projection.deleted = captured.deleted
                     AND projection.registry_generation = captured.registry_generation
                 )
             )
             FROM sync_captured_projections projection
             JOIN sync_captured_rows captured
               ON captured.relation_id = projection.relation_id
              AND captured.record_id = projection.record_id
             WHERE projection.commit_lsn = '0/20'::pg_lsn
               AND projection.event_ordinal = 7",
        )
        .unwrap()
        .expect("reevaluation projection batch counts");
        assert_eq!(counts.0["rows"], json!(501));
        assert_eq!(counts.0["matches"], json!(true));
    }

    #[pg_test]
    fn test_backfill_digest_failure_does_not_replace_live_edges() {
        setup_test_tables();
        let record_id = "15151515-1515-1515-1515-151515151515";
        Spi::run(&format!(
            "INSERT INTO test_products (id, name, price)
             VALUES ('{record_id}', 'Atomic Product', 12)"
        ))
        .unwrap();
        insert_changelog("global", "test_products", record_id, 1);
        insert_edge("test_products", record_id, "global");
        let generation_before = backfill_scope_generation("global");

        Spi::run_with_args(
            "UPDATE sync_captured_rows
             SET row_data = jsonb_set(row_data, '{name}', to_jsonb('corrupted'::text))
             WHERE record_id = $1",
            &[record_id.into()],
        )
        .unwrap();
        Spi::run(
            "DO $test$
             DECLARE failed BOOLEAN := false;
             BEGIN
                 BEGIN
                     PERFORM synchro_backfill_bucket_edges();
                 EXCEPTION WHEN OTHERS THEN
                     failed := true;
                 END;
                 IF NOT failed THEN
                     RAISE EXCEPTION 'backfill unexpectedly succeeded';
                 END IF;
             END
             $test$",
        )
        .unwrap();

        let edge_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*)
             FROM sync_bucket_edges
             WHERE table_name = 'test_products' AND record_id = $1 AND bucket_id = 'global'",
            &[record_id.into()],
        )
        .unwrap();
        assert_eq!(edge_count, Some(1));
        assert_eq!(backfill_scope_generation("global"), generation_before);
    }

    #[pg_test]
    fn test_backfill_invalidates_changed_scope_cursors() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u2", "c2");
        let record_id = "16161616-1616-1616-1616-161616161616";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'u1', 'Moved')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", record_id, 1);
        insert_edge("test_orders", record_id, "user:u1");
        let old_cursor = scope_cursor_ref("u1", "c1", "user:u1", 0);
        let old_generation = backfill_scope_generation("user:u1");

        Spi::run_with_args(
            "UPDATE test_orders SET user_id = 'u2' WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:u2", "test_orders", record_id, 2);

        let response: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_backfill_bucket_edges()").unwrap();
        let response = response.unwrap().0;
        assert_eq!(
            response["affected_scopes"].as_array().unwrap(),
            &vec![json!("user:u1"), json!("user:u2")]
        );
        assert!(backfill_scope_generation("user:u1") > old_generation);
        assert!(backfill_scope_generation("user:u2") > 0);

        let pull = pull_client("u1", "c1", 1, json!({ "user:u1": old_cursor }), 100);
        assert_eq!(pull["changes"].as_array().unwrap().len(), 0);
        assert_eq!(pull["rebuild"].as_array().unwrap(), &vec![json!("user:u1")]);
    }

    #[pg_test]
    fn test_backfill_preserves_unrelated_scope_generation() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u2", "c2");
        register_client("u3", "c3");
        let changed_id = "17171717-1717-1717-1717-171717171717";
        let unchanged_id = "18181818-1818-1818-1818-181818181818";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES
                 ($1::uuid, 'u1', 'Moved'),
                 ($2::uuid, 'u3', 'Stable')",
            &[changed_id.into(), unchanged_id.into()],
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", changed_id, 1);
        insert_changelog("user:u3", "test_orders", unchanged_id, 1);
        insert_edge("test_orders", changed_id, "user:u1");
        insert_edge("test_orders", unchanged_id, "user:u3");
        let unchanged_generation = backfill_scope_generation("user:u3");

        Spi::run_with_args(
            "UPDATE test_orders SET user_id = 'u2' WHERE id = $1::uuid",
            &[changed_id.into()],
        )
        .unwrap();
        insert_changelog("user:u2", "test_orders", changed_id, 2);
        let _: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_backfill_bucket_edges()").unwrap();

        assert_eq!(
            backfill_scope_generation("user:u3"),
            unchanged_generation
        );
        assert_eq!(
            backfill_edge_count("test_orders", unchanged_id, "user:u3"),
            1
        );
        assert_eq!(
            backfill_edge_count("test_orders", changed_id, "user:u1"),
            0
        );
        assert_eq!(
            backfill_edge_count("test_orders", changed_id, "user:u2"),
            1
        );
    }

    #[pg_test]
    fn test_backfill_atomically_replaces_old_and_new_edges() {
        setup_test_tables();
        let record_id = "19191919-1919-1919-1919-191919191919";
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES
                 ($1::uuid, 'before', 'Replacement')",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:before", "test_orders", record_id, 1);
        insert_edge("test_orders", record_id, "user:before");
        Spi::run_with_args(
            "UPDATE test_orders SET user_id = 'after' WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap();
        insert_changelog("user:after", "test_orders", record_id, 2);

        let response: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_backfill_bucket_edges()").unwrap();
        assert_eq!(response.unwrap().0["edges"].as_i64(), Some(1));
        assert_eq!(
            backfill_edge_count("test_orders", record_id, "user:before"),
            0
        );
        assert_eq!(
            backfill_edge_count("test_orders", record_id, "user:after"),
            1
        );
        let installed = Spi::connect(|client| {
            let row = client
                .select(
                    "SELECT row_version::text AS row_version, encode(checksum, 'hex') AS checksum
                     FROM sync_bucket_edges
                     WHERE table_name = 'test_orders' AND record_id = $1 AND bucket_id = 'user:after'",
                    None,
                    &[record_id.into()],
                )?
                .first();
            Ok::<_, pgrx::spi::Error>((
                row.get_by_name::<String, &str>("row_version")?,
                row.get_by_name::<String, &str>("checksum")?,
            ))
        })
        .unwrap();
        assert!(installed.0.is_some());
        assert_eq!(installed.1.as_deref().map(str::len), Some(64));
    }

    #[pg_test]
    fn test_pull_malformed_typed_effect_fails_without_progress() {
        setup_pull_fixtures();
        Spi::run(
            "UPDATE sync_changelog
             SET effect_ordinal = NULL
             WHERE bucket_id = 'user:u1'
               AND seq = (SELECT MIN(seq) FROM sync_changelog WHERE bucket_id = 'user:u1');
             UPDATE sync_wal_progress
             SET materialized_commit_lsn = NULL, materialized_end_lsn = NULL
             WHERE singleton = true",
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
        let checkpoint: Option<String> = Spi::get_one(
            "SELECT position_kind FROM sync_client_checkpoints
             WHERE user_id = 'u1' AND client_id = 'c1' AND bucket_id = 'user:u1'",
        )
        .unwrap();
        assert_eq!(checkpoint.as_deref(), Some("generation_start"));
    }

    #[pg_test]
    fn test_pull_ignores_malformed_effect_beyond_boundary() {
        setup_pull_fixtures();
        Spi::run(
            "INSERT INTO sync_changelog (
                 bucket_id, table_name, record_id, operation, stream_generation,
                 commit_lsn, event_ordinal, effect_ordinal, relation_id, row_version
             )
             SELECT 'user:u1', 'test_orders',
                    'f0000000-0000-4000-8000-000000000001', 1,
                    runtime.stream_generation, 'FFFFFFFF/FFFFFFFE'::pg_lsn,
                    NULL, 0, registry.relation_id, gen_random_uuid()
             FROM sync_runtime_state runtime
             JOIN sync_registry registry ON registry.table_name = 'test_orders'
             JOIN sync_registry_generations generation
               ON generation.generation = registry.registry_generation
              AND generation.state = 'active'
             WHERE runtime.singleton",
        )
        .unwrap();

        let response = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("u1", "c1", "user:u1", 0) }),
            100,
        );

        assert!(response.get("error").is_none());
        assert!(!response["changes"].as_array().unwrap().is_empty());
    }

    #[pg_test]
    fn test_direct_write_advances_opaque_version() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at) VALUES
             ('00c5e571-1111-1111-1111-111111111111', 'u1', 'GUC Test',
              '2025-06-15T12:00:00.000Z')",
        )
        .unwrap();
        let first_version =
            current_row_version("test_orders", "00c5e571-1111-1111-1111-111111111111");
        Spi::run(
            "UPDATE test_orders
             SET title = 'Direct update'
             WHERE id = '00c5e571-1111-1111-1111-111111111111'",
        )
        .unwrap();
        let second_version =
            current_row_version("test_orders", "00c5e571-1111-1111-1111-111111111111");
        assert_ne!(second_version, first_version);
    }

    #[pg_test]
    fn worker_runtime_identity_rejects_recreated_state() {
        setup_test_tables();
        let original = Spi::connect(|client| {
            let row = client
                .select(
                    "SELECT stream_generation, active_slot_name::text AS active_slot_name
                     FROM synchro.sync_runtime_state WHERE singleton",
                    None,
                    &[],
                )?
                .first();
            Ok::<_, pgrx::spi::Error>(
                (
                    row.get_by_name::<String, &str>("stream_generation")?,
                    row.get_by_name::<String, &str>("active_slot_name")?,
                ),
            )
        })
        .expect("load original runtime identity");
        let original_generation = original.0.expect("original stream generation");
        let original_slot = original.1;
        let expected_slot = "synchro_worker_identity";
        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET active_slot_name = $1, updated_at = now()
             WHERE singleton",
            &[expected_slot.into()],
        )
        .expect("set worker identity slot");

        let (identity, _) = Spi::connect(|client| {
            crate::bgworker::capture_worker_runtime_identity(client, "unused")
        })
        .expect("capture worker runtime identity");
        let unchanged = Spi::connect(|client| {
            crate::bgworker::validate_worker_runtime_identity(client, &identity)
        });
        assert!(unchanged.is_ok(), "unchanged runtime identity must validate");

        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET stream_generation = $1, updated_at = now()
             WHERE singleton",
            &["recreated-worker-runtime".into()],
        )
        .expect("replace runtime stream generation");
        let recreated = Spi::connect(|client| {
            crate::bgworker::validate_worker_runtime_identity(client, &identity)
        });
        assert!(recreated.is_err(), "recreated runtime state must invalidate identity");

        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET stream_generation = $1, active_slot_name = NULL, updated_at = now()
             WHERE singleton",
            &[original_generation.clone().into()],
        )
        .expect("prepare unbound worker runtime");
        let startup_identity = Spi::connect(|client| {
            crate::bgworker::capture_worker_startup_identity(client, expected_slot)
        })
        .expect("capture startup worker runtime identity");
        let startup_unchanged = Spi::connect(|client| {
            crate::bgworker::validate_worker_startup_identity(client, &startup_identity)
        });
        assert!(
            startup_unchanged.is_ok(),
            "unbound startup runtime identity must validate"
        );
        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET stream_generation = $1, updated_at = now()
             WHERE singleton",
            &["recreated-startup-runtime".into()],
        )
        .expect("replace startup runtime stream generation");
        let startup_recreated = Spi::connect(|client| {
            crate::bgworker::validate_worker_startup_identity(client, &startup_identity)
        });
        assert!(
            startup_recreated.is_err(),
            "recreated startup runtime state must invalidate identity"
        );

        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET stream_generation = $1, active_slot_name = $2, updated_at = now()
             WHERE singleton",
            &[original_generation.into(), original_slot.into()],
        )
        .expect("restore runtime identity");
    }

    #[pg_test]
    fn worker_slot_binding_selects_reuse_replace_or_fail() {
        let unbound = crate::bgworker::WorkerStartupIdentity {
            runtime: crate::bgworker::WorkerRuntimeIdentity {
                stream_generation: "unbound-generation".to_string(),
                slot_name: "unbound-slot".to_string(),
            },
            active_slot_is_unbound: true,
        };
        let bound = crate::bgworker::WorkerStartupIdentity {
            runtime: crate::bgworker::WorkerRuntimeIdentity {
                stream_generation: "bound-generation".to_string(),
                slot_name: "bound-slot".to_string(),
            },
            active_slot_is_unbound: false,
        };

        assert_eq!(
            crate::bgworker::slot_binding_decision(
                &unbound,
                crate::bgworker::ExistingWorkerSlot::Inactive,
            ),
            crate::bgworker::SlotBindingDecision::Replace,
        );
        assert_eq!(
            crate::bgworker::slot_binding_decision(
                &bound,
                crate::bgworker::ExistingWorkerSlot::Inactive,
            ),
            crate::bgworker::SlotBindingDecision::Reuse,
        );
        assert_eq!(
            crate::bgworker::slot_binding_decision(
                &unbound,
                crate::bgworker::ExistingWorkerSlot::Active,
            ),
            crate::bgworker::SlotBindingDecision::Fail,
        );
    }

    fn reset_runtime_for_unbound_registration_test() {
        Spi::run(
            "UPDATE synchro.sync_runtime_state
             SET active_slot_name = NULL,
                 active_publication_name = NULL,
                 active_publication_oid = NULL,
                 updated_at = now()
             WHERE singleton;
             UPDATE synchro.sync_wal_progress
             SET generation_start_lsn = NULL,
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 acknowledged_end_lsn = NULL,
                 updated_at = now()
             WHERE singleton",
        )
        .expect("reset runtime for unbound registration test");
    }

    fn bind_test_runtime_after_registration(slot: &str) -> i64 {
        Spi::run_with_args(
            "UPDATE synchro.sync_runtime_state
             SET active_slot_name = $1,
                  updated_at = now()
             WHERE singleton",
            &[slot.into()],
        )
        .expect("bind unbound registration test runtime");
        Spi::get_one_with_args(
            "SELECT count(*)
             FROM synchro.sync_registry_activation_requests request
             JOIN synchro.sync_registry_generations generation
               ON generation.generation = request.registry_generation
              AND generation.state = 'pending'
             WHERE request.emitted_at IS NOT NULL",
            &[],
        )
        .expect("read unbound registration activation requests")
        .expect("unbound registration activation message count")
    }

    fn create_and_register_unbound_test_table(table: &str) {
        Spi::run(&format!(
            "CREATE TABLE {table} (
                 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                 user_id TEXT NOT NULL,
                 title TEXT NOT NULL DEFAULT '',
                 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 deleted_at TIMESTAMPTZ
             )"
        ))
        .expect("create unbound registration test table");
        Spi::run(&format!(
            "SELECT synchro.synchro_prepare_projection_view(
                 'public.{table}', '{table}', ARRAY['user_id']
             );
             SELECT tests.register_test_table(
                 '{table}',
                 $$SELECT 'user:' || (user_id #>> '{{}}') FROM synchro_projection.{table} WHERE record_id = p_key::text$$,
                 'single_scope'
             )"
        ))
        .expect("register unbound registration test table");
    }

    #[pg_test]
    fn registration_committed_before_initial_slot_binding_is_replayed() {
        setup_test_tables();
        reset_runtime_for_unbound_registration_test();
        create_and_register_unbound_test_table("test_prebound_registration");

        let activation_messages = bind_test_runtime_after_registration("synchro_prebound_test");
        reset_runtime_for_unbound_registration_test();

        assert_eq!(
            activation_messages, 1,
            "registration activation was lost before initial slot binding"
        );
    }

    #[pg_test]
    fn repeated_unbound_reinstalls_replay_every_pending_registration() {
        setup_test_tables();
        for (index, table) in [
            "test_reinstall_registration_one",
            "test_reinstall_registration_two",
            "test_reinstall_registration_three",
        ]
        .into_iter()
        .enumerate()
        {
            reset_runtime_for_unbound_registration_test();
            create_and_register_unbound_test_table(table);
            let slot = format!("synchro_reinstall_test_{index}");
            let activation_messages = bind_test_runtime_after_registration(&slot);
            assert_eq!(
                activation_messages,
                i64::try_from(index + 1).expect("activation count fits"),
                "pending registration {table} was lost during repeated reinstall"
            );
        }
        reset_runtime_for_unbound_registration_test();
    }

    fn backfill_scope_generation(scope_id: &str) -> i64 {
        Spi::get_one_with_args(
            "SELECT membership_generation
             FROM sync_scope_state
             WHERE scope_id = $1",
            &[scope_id.into()],
        )
        .unwrap()
        .expect("backfill scope generation")
    }

    fn backfill_edge_count(table_name: &str, record_id: &str, bucket_id: &str) -> i64 {
        Spi::get_one_with_args(
            "SELECT count(*)
             FROM sync_bucket_edges
             WHERE table_name = $1 AND record_id = $2 AND bucket_id = $3",
            &[table_name.into(), record_id.into(), bucket_id.into()],
        )
        .unwrap()
        .expect("backfill edge count")
    }

    #[pg_test]
    fn backfill_membership_spi_query_is_batched() {
        setup_test_tables();
        let record_ids = vec![
            "1a1a1a1a-1a1a-1a1a-1a1a-1a1a1a1a1a1a".to_string(),
            "1b1b1b1b-1b1b-1b1b-1b1b-1b1b1b1b1b1b".to_string(),
        ];
        for record_id in &record_ids {
            Spi::run_with_args(
                "INSERT INTO test_products (id, name, price)
                 VALUES ($1::uuid, 'Bounded membership', 12)",
                &[record_id.as_str().into()],
            )
            .expect("insert bounded membership product");
            insert_changelog("global", "test_products", record_id, 1);
        }

        let registration = Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            Ok::<_, pgrx::spi::Error>(
                registry
                    .into_iter()
                    .find(|registration| registration.table_name == "test_products")
                    .expect("product membership registration"),
            )
        })
        .expect("load product membership registration");
        let query = crate::materialize::membership_batch_query(&registration)
            .expect("build bounded membership query");
        let (memberships, query_count) = query_counts::measure(0, || {
            Spi::connect(|client| {
                crate::materialize::resolve_membership_batch(client, &registration, &record_ids)
            })
            .expect("resolve bounded membership batch")
        });
        let response: pgrx::JsonB = Spi::get_one(
            "SELECT synchro_backfill_bucket_edges('test_products', 1000)",
        )
        .expect("run bounded membership backfill")
        .expect("bounded membership backfill response");

        assert_eq!(query.matches("jsonb_to_recordset").count(), 1);
        assert_eq!(query.matches("CROSS JOIN LATERAL").count(), 1);
        assert_eq!(query_count, 1);
        assert_eq!(memberships.len(), 2);
        assert!(memberships.values().all(|scopes| scopes == &["global"]));
        assert_eq!(response.0["records"], json!(2));
        assert_eq!(response.0["edges"], json!(2));
    }

    fn wal_counting_image(registration: &TableRegistration, record_id: &str) -> TupleImage {
        registration
            .fields
            .iter()
            .map(|field| {
                let value = match field.physical_column.as_str() {
                    "id" => TupleValue::Text(record_id.as_bytes().to_vec()),
                    "user_id" => TupleValue::Text(b"u1".to_vec()),
                    "title" => TupleValue::Text(b"WAL query count".to_vec()),
                    "amount" => TupleValue::Text(b"0".to_vec()),
                    "created_at" | "updated_at" => {
                        TupleValue::Text(b"2000-01-01 00:00:00+00".to_vec())
                    }
                    "deleted_at" => TupleValue::Null,
                    column => panic!("unexpected test_orders synced column {column}"),
                };
                (field.physical_column.clone(), value)
            })
            .collect()
    }

    fn wal_counting_transaction(
        registration: &TableRegistration,
        commit_lsn: u64,
        start: u32,
        count: u32,
    ) -> WalTransaction {
        let xid: String = Spi::get_one("SELECT pg_current_xact_id()::text")
            .expect("read WAL counting transaction xid")
            .expect("WAL counting transaction xid");
        let xid = xid.parse().expect("parse WAL counting transaction xid");
        let relation = RelationKey::new(
            registration.physical_schema.clone(),
            registration.physical_relation.clone(),
            registration.physical_relation_oid,
        );
        let mut events = Vec::new();
        let mut messages = Vec::new();
        for offset in 0..count {
            let sequence = start + offset;
            let record_id = format!("31000000-0000-4000-8000-{sequence:012x}");
            let fence_id = format!("32000000-0000-4000-8000-{sequence:012x}");
            let row_version = format!("33000000-0000-4000-8000-{sequence:012x}");
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title)
                 VALUES ($1::uuid, 'u1', 'WAL query count')",
                &[record_id.as_str().into()],
            )
            .expect("insert WAL counting source row");
            Spi::run_with_args(
                "INSERT INTO synchro.sync_write_fences (
                     fence_id, transaction_xid, dml_ordinal, relation_id,
                     registration_kind, table_id,
                     physical_schema, physical_relation, physical_relation_oid,
                     operation, old_record_id, new_record_id, row_version
                 ) VALUES (
                     $1::uuid, pg_current_xact_id(), $2, $3::uuid,
                     'synced', $4::uuid, $5, $6, $7::oid,
                     'insert', NULL, $8, $9::uuid
                 )",
                &[
                    fence_id.as_str().into(),
                    i64::from(offset + 1).into(),
                    registration.relation_id.as_str().into(),
                    registration.table_id.as_str().into(),
                    registration.physical_schema.as_str().into(),
                    registration.physical_relation.as_str().into(),
                    i64::from(registration.physical_relation_oid).into(),
                    record_id.as_str().into(),
                    row_version.as_str().into(),
                ],
            )
            .expect("insert WAL counting fence");
            events.push(WalEvent {
                operation: ChangeOperation::Insert,
                relation: relation.clone(),
                event_ordinal: u64::from(offset),
                before: None,
                after: Some(wal_counting_image(registration, &record_id)),
            });
            messages.push(WalLogicalMessage {
                prefix: "synchro_fence".to_string(),
                content: serde_json::to_vec(&json!({
                    "fence_id": fence_id,
                    "dml_ordinal": offset + 1,
                    "registration_kind": "synced",
                    "relation_id": registration.relation_id,
                    "table_id": registration.table_id,
                    "physical_schema": registration.physical_schema,
                    "physical_relation": registration.physical_relation,
                    "physical_relation_oid": registration.physical_relation_oid,
                    "operation": "insert",
                    "old_record_id": serde_json::Value::Null,
                    "new_record_id": record_id,
                    "old_capture_key": serde_json::Value::Null,
                    "new_capture_key": serde_json::Value::Null,
                    "row_version": row_version,
                }))
                .expect("encode WAL counting fence"),
                message_lsn: commit_lsn,
            });
        }
        WalTransaction {
            xid,
            final_lsn: commit_lsn,
            commit_lsn,
            end_lsn: commit_lsn + 1,
            commit_timestamp: 0,
            events,
            truncates: Vec::new(),
            messages,
        }
    }

    #[pg_test]
    fn wal_materialization_query_count_is_bounded() {
        setup_test_tables();
        let registration = Spi::connect(|client| {
            crate::registry::load_registry_from_client(client).map(|registry| {
                registry
                    .into_iter()
                    .find(|registration| registration.table_name == "test_orders")
                    .expect("orders WAL counting registration")
            })
        })
        .expect("load orders WAL counting registration");
        let mut measurements = HashMap::new();
        for (start, count, commit_lsn) in [(1, 10, 0x100u64), (100, 100, 0x200), (1_000, 501, 0x300)] {
            let transaction = wal_counting_transaction(&registration, commit_lsn, start, count);
            let (result, queries) = query_counts::measure(0, || {
                Spi::connect_mut(|client| {
                    crate::bgworker::materialize_transaction_for_test(client, &transaction)
                })
            });
            result.expect("materialize WAL counting transaction");
            assert!(queries > 0, "WAL query measurement observed no work");
            let durable: pgrx::JsonB = Spi::get_one_with_args(
                "SELECT jsonb_build_object(
                     'events', (SELECT count(*) FROM synchro.sync_wal_events WHERE commit_lsn = $1::pg_lsn),
                     'projections', (SELECT count(*) FROM synchro.sync_captured_projections WHERE commit_lsn = $1::pg_lsn),
                     'rows', (SELECT count(*) FROM synchro.sync_captured_rows WHERE source_commit_lsn = $1::pg_lsn),
                     'effects', (SELECT count(*) FROM synchro.sync_changelog WHERE commit_lsn = $1::pg_lsn)
                 )",
                &[crate::stream_position::format_lsn(commit_lsn).as_str().into()],
            )
            .expect("read measured WAL results")
            .expect("measured WAL results");
            assert_eq!(
                durable.0,
                json!({"events": count, "projections": count, "rows": count, "effects": count}),
            );
            measurements.insert(count, queries);
        }
        let below_boundary = measurements[&10];
        assert_eq!(measurements[&100], below_boundary);
        // Two input pages must use at most twice the work of one complete page.
        assert!(
            measurements[&501] > below_boundary
                && measurements[&501] <= 2 * below_boundary,
            "WAL materialization query count grew beyond its JSONB batch boundary: {measurements:?}"
        );
    }
