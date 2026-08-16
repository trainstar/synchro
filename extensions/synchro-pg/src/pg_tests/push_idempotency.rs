    #[pg_test]
    fn test_push_rejects_empty_identity() {
        let response = execute_push("", &json!({}));

        assert_eq!(response.json["error"]["code"].as_str(), Some("auth_required"));
        assert_eq!(response.json["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_push_completed_batch_replays_exact_text() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let record_id = "30000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);

        let request = push_request(
            user_id,
            client_id,
            "completed-replay",
            vec![push_mutation(
                user_id,
                client_id,
                "completed-replay",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("replay"))]),
            )],
        );
        let first = execute_push(user_id, &request);
        assert_eq!(first.json["accepted"][0]["status"], "applied");
        let source_before = source_wire_record("test_orders", record_id);
        let fence_count_before: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_write_fences WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        let ledgers_before = push_ledger_counts(user_id, client_id);
        let epoch_before = accepted_write_epoch(user_id, client_id);

        let replay = execute_push(user_id, &request);
        assert_eq!(replay.raw, first.raw);
        assert_eq!(source_wire_record("test_orders", record_id), source_before);
        let fence_count_after: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_write_fences WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        assert_eq!(fence_count_after, fence_count_before);
        assert_eq!(push_ledger_counts(user_id, client_id), ledgers_before);
        assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before);
    }

    #[pg_test]
    fn test_push_same_batch_changed_content_is_idempotency_conflict() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let record_id = "40000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);

        let first = push_client(
            user_id,
            client_id,
            "same-batch",
            vec![push_mutation(
                user_id,
                client_id,
                "same-batch",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("first"))]),
            )],
        );
        assert_eq!(first.json["accepted"][0]["status"], "applied");
        let source_before = source_wire_record("test_orders", record_id);
        let ledgers_before = push_ledger_counts(user_id, client_id);

        let changed = push_client(
            user_id,
            client_id,
            "same-batch",
            vec![push_mutation(
                user_id,
                client_id,
                "same-batch",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("changed"))]),
            )],
        );
        assert_eq!(
            changed.json["error"]["code"].as_str(),
            Some("idempotency_conflict")
        );
        assert_eq!(source_wire_record("test_orders", record_id), source_before);
        assert_eq!(push_ledger_counts(user_id, client_id), ledgers_before);
    }

    #[pg_test]
    fn test_push_reuses_equal_mutation_without_dml_or_epoch_change() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let record_id = "50000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);

        let first = push_client(
            user_id,
            client_id,
            "mutation-first",
            vec![push_mutation(
                user_id,
                client_id,
                "reused-mutation",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("reused"))]),
            )],
        );
        let first_outcome = first.json["accepted"][0].clone();
        let source_before = source_wire_record("test_orders", record_id);
        let fence_count_before: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_write_fences WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        let ledgers_before = push_ledger_counts(user_id, client_id);
        let epoch_before = accepted_write_epoch(user_id, client_id);

        let replayed = push_client(
            user_id,
            client_id,
            "mutation-second",
            vec![push_mutation(
                user_id,
                client_id,
                "reused-mutation",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("reused"))]),
            )],
        );
        assert_eq!(replayed.json["accepted"][0], first_outcome);
        let sealed_outcome: Option<Vec<u8>> = Spi::get_one_with_args(
            "SELECT sealed_canonical_response
             FROM sync_push_mutations
             WHERE user_id = $1 AND client_id = $2 AND mutation_id = $3::uuid",
            &[
                user_id.into(),
                client_id.into(),
                mutation_id(user_id, client_id, "reused-mutation").into(),
            ],
        )
        .unwrap();
        let replayed_outcome =
            serde_json_canonicalizer::to_vec(&replayed.json["accepted"][0]).unwrap();
        assert_eq!(sealed_outcome.as_deref(), Some(replayed_outcome.as_slice()));
        assert_eq!(source_wire_record("test_orders", record_id), source_before);
        let fence_count_after: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_write_fences WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        assert_eq!(fence_count_after, fence_count_before);
        assert_eq!(
            push_ledger_counts(user_id, client_id),
            (ledgers_before.0 + 1, ledgers_before.1)
        );
        assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before);
    }

    #[pg_test]
    fn test_push_changed_mutation_identity_creates_no_new_batch() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let record_id = "60000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);

        let first = push_client(
            user_id,
            client_id,
            "changed-mutation-first",
            vec![push_mutation(
                user_id,
                client_id,
                "changed-mutation",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("original"))]),
            )],
        );
        assert_eq!(first.json["accepted"][0]["status"], "applied");
        let source_before = source_wire_record("test_orders", record_id);
        let ledgers_before = push_ledger_counts(user_id, client_id);
        let epoch_before = accepted_write_epoch(user_id, client_id);

        let changed = push_client(
            user_id,
            client_id,
            "changed-mutation-second",
            vec![push_mutation(
                user_id,
                client_id,
                "changed-mutation",
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("changed"))]),
            )],
        );
        assert_eq!(
            changed.json["error"]["code"].as_str(),
            Some("idempotency_conflict")
        );
        assert_eq!(source_wire_record("test_orders", record_id), source_before);
        assert_eq!(push_ledger_counts(user_id, client_id), ledgers_before);
        assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before);
    }

    #[pg_test]
    fn test_push_stale_generation_creates_no_ledger() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        Spi::run_with_args(
            "UPDATE sync_clients SET client_generation = 2
             WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();

        let request = push_request_with_generation(
            user_id,
            client_id,
            "stale-generation",
            1,
            vec![push_mutation(
                user_id,
                client_id,
                "stale-generation",
                "test_orders",
                "insert",
                "70000000-0000-4000-8000-000000000001",
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("stale"))]),
            )],
        );
        let stale = execute_push(user_id, &request);
        assert_eq!(
            stale.json["error"]["code"].as_str(),
            Some("client_generation_expired")
        );
        assert_eq!(stale.json["error"]["current_client_generation"], 2);
        assert_eq!(push_ledger_counts(user_id, client_id), (0, 0));
    }

    #[pg_test]
    fn test_push_ledger_deletion_requires_permanent_retirement() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let batch_label = "ledger-retirement";
        let mutation_label = "ledger-retirement";
        let record_id = "a0000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);
        let response = push_client(
            user_id,
            client_id,
            batch_label,
            vec![push_mutation(
                user_id,
                client_id,
                mutation_label,
                "test_orders",
                "insert",
                record_id,
                None,
                Some(&[("user_id", json!(user_id)), ("title", json!("ledger"))]),
            )],
        );
        assert_eq!(response.json["accepted"][0]["status"], "applied");
        let stored_batch_id = batch_id(user_id, client_id, batch_label);
        let stored_mutation_id = mutation_id(user_id, client_id, mutation_label);

        Spi::run(
            "DO $test$
             BEGIN
                 BEGIN
                     UPDATE sync_push_batches SET completed_at = completed_at
                     WHERE user_id = 'u1' AND client_id = 'c1';
                     RAISE EXCEPTION 'batch update was not rejected';
                 EXCEPTION WHEN SQLSTATE '55000' THEN NULL;
                 END;
                 BEGIN
                     UPDATE sync_push_mutations SET completed_at = completed_at
                     WHERE user_id = 'u1' AND client_id = 'c1';
                     RAISE EXCEPTION 'mutation update was not rejected';
                 EXCEPTION WHEN SQLSTATE '55000' THEN NULL;
                 END;
                 BEGIN
                     DELETE FROM sync_push_batches
                     WHERE user_id = 'u1' AND client_id = 'c1';
                     RAISE EXCEPTION 'batch deletion was not rejected';
                 EXCEPTION WHEN SQLSTATE '55000' THEN NULL;
                 END;
                 BEGIN
                     DELETE FROM sync_push_mutations
                     WHERE user_id = 'u1' AND client_id = 'c1';
                     RAISE EXCEPTION 'mutation deletion was not rejected';
                 EXCEPTION WHEN SQLSTATE '55000' THEN NULL;
                 END;
             END
             $test$",
        )
        .unwrap();

        Spi::run_with_args(
            "INSERT INTO sync_client_retirements (user_id, client_id) VALUES ($1, $2)",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        Spi::run(
            "DO $test$
             BEGIN
                 BEGIN
                     DELETE FROM sync_client_retirements
                     WHERE user_id = 'u1' AND client_id = 'c1';
                     RAISE EXCEPTION 'retirement deletion was not rejected';
                 EXCEPTION WHEN SQLSTATE '55000' THEN NULL;
                 END;
             END
             $test$",
        )
        .unwrap();

        Spi::run_with_args(
            "DELETE FROM sync_push_mutations
             WHERE user_id = $1 AND client_id = $2 AND mutation_id = $3::uuid",
            &[
                user_id.into(),
                client_id.into(),
                stored_mutation_id.as_str().into(),
            ],
        )
        .unwrap();
        Spi::run_with_args(
            "DELETE FROM sync_push_batches
             WHERE user_id = $1 AND client_id = $2 AND batch_id = $3::uuid",
            &[
                user_id.into(),
                client_id.into(),
                stored_batch_id.as_str().into(),
            ],
        )
        .unwrap();
        assert_eq!(push_ledger_counts(user_id, client_id), (0, 0));
    }
