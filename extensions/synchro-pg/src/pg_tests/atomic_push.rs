    fn atomic_push_request(
        user_id: &str,
        client_id: &str,
        batch_label: &str,
        mutations: Vec<Value>,
    ) -> Value {
        let mut request = push_request(user_id, client_id, batch_label, mutations);
        request["atomic"] = json!(true);
        request
    }

    fn order_insert(user_id: &str, label: &str, record_id: &str) -> Value {
        push_mutation(
            (user_id, "c1"),
            label,
            "test_orders",
            "insert",
            record_id,
            None,
            Some(&[("user_id", json!(user_id)), ("title", json!(label))]),
        )
    }

    fn source_order_count(record_ids: &[&str]) -> i64 {
        Spi::get_one_with_args(
            "SELECT count(*) FROM test_orders WHERE id = ANY($1::uuid[])",
            &[record_ids
                .iter()
                .map(|record_id| record_id.to_string())
                .collect::<Vec<_>>()
                .into()],
        )
        .unwrap()
        .expect("source order count")
    }

    fn group_fence_count(mutations: &[Value]) -> i64 {
        let mutation_ids = mutations
            .iter()
            .map(|mutation| mutation["mutation_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        Spi::get_one_with_args(
            "SELECT count(*) FROM sync_write_fences WHERE mutation_id = ANY($1)",
            &[mutation_ids.into()],
        )
        .unwrap()
        .expect("atomic group fence count")
    }

    /// Asserts the C6 failed-group partition and returns the failing outcome.
    fn assert_failed_group(
        response: &PushResult,
        mutations: &[Value],
        failing_index: usize,
        failing_status: &str,
        failing_code: &str,
    ) -> Value {
        assert_eq!(response.json["accepted"], json!([]));
        let rejected = response.json["rejected"]
            .as_array()
            .expect("failed atomic group rejected array");
        assert_eq!(rejected.len(), mutations.len());
        for (index, (outcome, mutation)) in rejected.iter().zip(mutations).enumerate() {
            assert_eq!(outcome["mutation_id"], mutation["mutation_id"]);
            assert_eq!(outcome["table"], mutation["table"]);
            assert_eq!(outcome["pk"], mutation["pk"]);
            if index == failing_index {
                assert_eq!(outcome["status"], failing_status);
                assert_eq!(outcome["code"], failing_code);
                continue;
            }
            let mut expected = serde_json::Map::new();
            for member in ["mutation_id", "table", "pk"] {
                expected.insert(member.to_string(), mutation[member].clone());
            }
            expected.insert("outcome_schema".into(), schema_ref_value());
            expected.insert("status".into(), json!("rejected_terminal"));
            expected.insert("code".into(), json!("atomic_batch_rejected"));
            expected.insert("message".into(), json!("atomic batch rejected"));
            assert_eq!(*outcome, Value::Object(expected));
        }
        rejected[failing_index].clone()
    }

    fn assert_group_ledger(user_id: &str, client_id: &str, response: &PushResult) {
        let rejected = response.json["rejected"]
            .as_array()
            .expect("failed atomic group rejected array");
        for (index, outcome) in rejected.iter().enumerate() {
            let ledger: Option<pgrx::JsonB> = Spi::get_one_with_args(
                "SELECT jsonb_build_object(
                     'request_ordinal', request_ordinal,
                     'outcome_status', outcome_status,
                     'rejection_code', rejection_code,
                     'sealed_response', convert_from(sealed_canonical_response, 'UTF8')::jsonb
                 )
                 FROM sync_push_mutations
                 WHERE user_id = $1 AND client_id = $2 AND mutation_id = $3::uuid",
                &[
                    user_id.into(),
                    client_id.into(),
                    outcome["mutation_id"].as_str().unwrap().into(),
                ],
            )
            .unwrap();
            let ledger = ledger.expect("atomic group mutation ledger").0;
            assert_eq!(ledger["request_ordinal"], json!(index + 1));
            assert_eq!(ledger["outcome_status"], outcome["status"]);
            assert_eq!(ledger["rejection_code"], outcome["code"]);
            assert_eq!(ledger["sealed_response"], *outcome);
        }
    }

    #[pg_test]
    fn test_atomic_push_applies_every_mutation() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let first = "a1000000-0000-4000-8000-000000000001";
        let second = "a1000000-0000-4000-8000-000000000002";
        let epoch_before = accepted_write_epoch(user_id, client_id);

        let response = execute_push(
            user_id,
            &atomic_push_request(
                user_id,
                client_id,
                "atomic-all-applied",
                vec![
                    order_insert(user_id, "atomic-applied-first", first),
                    order_insert(user_id, "atomic-applied-second", second),
                ],
            ),
        );

        assert_eq!(response.json["rejected"], json!([]));
        let accepted = response.json["accepted"]
            .as_array()
            .expect("atomic accepted array");
        assert_eq!(accepted.len(), 2);
        for (outcome, record_id) in accepted.iter().zip([first, second]) {
            assert_eq!(outcome["status"], "applied");
            assert_row_outcome_matches_source(outcome, "test_orders", record_id);
        }
        assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before + 1);
    }

    #[pg_test]
    fn test_atomic_push_conflict_rolls_back_at_every_position() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);

        for position in 0..3 {
            let record_ids = (1..=3)
                .map(|index| format!("a2000000-0000-4000-8000-00000000{position}00{index}"))
                .collect::<Vec<_>>();
            let conflict_id = &record_ids[position];
            insert_live_order(conflict_id, user_id, "committed");
            let mutations = record_ids
                .iter()
                .enumerate()
                .map(|(index, record_id)| {
                    order_insert(
                        user_id,
                        &format!("atomic-position-{position}-{index}"),
                        record_id,
                    )
                })
                .collect::<Vec<_>>();
            let epoch_before = accepted_write_epoch(user_id, client_id);

            let response = execute_push(
                user_id,
                &atomic_push_request(
                    user_id,
                    client_id,
                    &format!("atomic-position-{position}"),
                    mutations.clone(),
                ),
            );

            let failing = assert_failed_group(
                &response,
                &mutations,
                position,
                "conflict",
                "row_already_exists",
            );
            assert_row_outcome_matches_source(&failing, "test_orders", conflict_id);
            let others = record_ids
                .iter()
                .filter(|record_id| *record_id != conflict_id)
                .map(String::as_str)
                .collect::<Vec<_>>();
            assert_eq!(source_order_count(&others), 0);
            assert_eq!(group_fence_count(&mutations), 0);
            assert_eq!(accepted_write_epoch(user_id, client_id), epoch_before);
            assert_group_ledger(user_id, client_id, &response);
        }
    }

    #[pg_test]
    fn test_atomic_push_terminal_failure_rolls_back_the_group() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let first = "a3000000-0000-4000-8000-000000000001";
        let last = "a3000000-0000-4000-8000-000000000003";
        let mutations = vec![
            order_insert(user_id, "atomic-terminal-first", first),
            push_mutation(
                (user_id, client_id),
                "atomic-terminal-policy",
                "test_products",
                "insert",
                "a3000000-0000-4000-8000-000000000002",
                None,
                Some(&[("name", json!("read only"))]),
            ),
            order_insert(user_id, "atomic-terminal-last", last),
        ];

        let response = execute_push(
            user_id,
            &atomic_push_request(user_id, client_id, "atomic-terminal", mutations.clone()),
        );

        let failing = assert_failed_group(
            &response,
            &mutations,
            1,
            "rejected_terminal",
            "policy_rejected",
        );
        assert_eq!(
            failing["message"],
            "authenticated write policy rejected the mutation"
        );
        assert_eq!(source_order_count(&[first, last]), 0);
        assert_eq!(group_fence_count(&mutations), 0);
        assert_group_ledger(user_id, client_id, &response);
    }

    #[pg_test]
    fn test_atomic_push_conflict_rereads_a_cascaded_row_after_rollback() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        let parent_id = "a4000000-0000-4000-8000-000000000001";
        let child_id = "a4000000-0000-4000-8000-000000000002";
        Spi::run(
            "CREATE TABLE public.test_atomic_parents (
                 id uuid PRIMARY KEY,
                 user_id text NOT NULL,
                 title text NOT NULL DEFAULT ''
             );
             CREATE TABLE public.test_atomic_children (
                 id uuid PRIMARY KEY,
                 parent_id uuid NOT NULL REFERENCES public.test_atomic_parents (id) ON DELETE CASCADE,
                 user_id text NOT NULL,
                 title text NOT NULL DEFAULT ''
             )",
        )
        .expect("create atomic cascade tables");
        for table in ["test_atomic_parents", "test_atomic_children"] {
            Spi::run(&format!(
                "SELECT synchro.synchro_prepare_projection_view(
                    'public.{table}', '{table}', ARRAY['user_id']
                 );
                 SELECT tests.register_test_table(
                    '{table}',
                    $$SELECT 'user:' || (user_id #>> '{{}}') FROM synchro_projection.{table} WHERE record_id = p_key::text$$,
                    'single_scope',
                    'id', 'updated_at', 'deleted_at', 'enabled'
                 )"
            ))
            .expect("register atomic cascade table");
        }
        activate_pending_registry_for_test();
        register_client(user_id, client_id);
        Spi::run_with_args(
            "INSERT INTO public.test_atomic_parents (id, user_id) VALUES ($1::uuid, $3);
             INSERT INTO public.test_atomic_children (id, parent_id, user_id, title)
             VALUES ($2::uuid, $1::uuid, $3, 'committed')",
            &[parent_id.into(), child_id.into(), user_id.into()],
        )
        .expect("insert atomic cascade rows");
        let parent_version = current_row_version("test_atomic_parents", parent_id);
        let child_version = current_row_version("test_atomic_children", child_id);
        let mutations = vec![
            push_mutation(
                (user_id, client_id),
                "atomic-cascade-parent-delete",
                "test_atomic_parents",
                "delete",
                parent_id,
                Some(&parent_version),
                None,
            ),
            push_mutation(
                (user_id, client_id),
                "atomic-cascade-child-update",
                "test_atomic_children",
                "update",
                child_id,
                Some(&child_version),
                Some(&[("title", json!("cascade"))]),
            ),
        ];

        let response = execute_push(
            user_id,
            &atomic_push_request(user_id, client_id, "atomic-cascade", mutations.clone()),
        );

        let failing = assert_failed_group(&response, &mutations, 1, "conflict", "row_deleted");
        assert_row_outcome_matches_source(&failing, "test_atomic_children", child_id);
        assert_eq!(failing["server_version"], json!(child_version));
        assert_eq!(
            current_row_version("test_atomic_parents", parent_id),
            parent_version
        );
        assert_eq!(group_fence_count(&mutations), 0);
    }

    #[pg_test]
    fn test_atomic_push_replays_a_failed_group_exactly() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let conflict_id = "a5000000-0000-4000-8000-000000000002";
        insert_live_order(conflict_id, user_id, "committed");
        let request = atomic_push_request(
            user_id,
            client_id,
            "atomic-replay",
            vec![
                order_insert(
                    user_id,
                    "atomic-replay-first",
                    "a5000000-0000-4000-8000-000000000001",
                ),
                order_insert(user_id, "atomic-replay-conflict", conflict_id),
            ],
        );

        let first = execute_push(user_id, &request);
        let ledgers = push_ledger_counts(user_id, client_id);
        let replay = execute_push(user_id, &request);

        assert_eq!(first.json["rejected"][1]["code"], "row_already_exists");
        assert_eq!(replay.raw, first.raw);
        assert_eq!(push_ledger_counts(user_id, client_id), ledgers);
    }

    #[pg_test]
    fn test_non_atomic_push_rejects_atomic_batch_rejected_replay() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let rejected_id = "a5100000-0000-4000-8000-000000000001";
        let conflict_id = "a5100000-0000-4000-8000-000000000002";
        let fresh_id = "a5100000-0000-4000-8000-000000000003";
        insert_live_order(conflict_id, user_id, "committed");
        let rejected = order_insert(user_id, "atomic-non-atomic-replay-rejected", rejected_id);
        let conflict = order_insert(user_id, "atomic-non-atomic-replay-conflict", conflict_id);
        let atomic = atomic_push_request(
            user_id,
            client_id,
            "atomic-non-atomic-replay",
            vec![rejected.clone(), conflict],
        );

        let atomic_response = execute_push(user_id, &atomic);
        assert_eq!(
            atomic_response.json["rejected"][0]["code"].as_str(),
            Some("atomic_batch_rejected")
        );
        let ledgers = push_ledger_counts(user_id, client_id);

        let replay = execute_push(
            user_id,
            &push_request(
                user_id,
                client_id,
                "non-atomic-atomic-rejection-replay",
                vec![
                    order_insert(user_id, "atomic-non-atomic-replay-fresh", fresh_id),
                    rejected,
                ],
            ),
        );

        assert_eq!(
            replay.json["error"]["code"].as_str(),
            Some("invalid_request")
        );
        assert_eq!(push_ledger_counts(user_id, client_id), ledgers);
        assert_eq!(source_order_count(&[rejected_id, fresh_id]), 0);
    }

    #[pg_test]
    fn test_atomic_push_invalid_requests_create_no_ledger() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let stored = order_insert(
            user_id,
            "atomic-stored",
            "a6000000-0000-4000-8000-000000000001",
        );
        push_client(
            user_id,
            client_id,
            "atomic-stored-batch",
            vec![stored.clone()],
        );
        let fresh_id = "a6000000-0000-4000-8000-000000000002";
        let fresh = order_insert(user_id, "atomic-fresh", fresh_id);
        let mut not_atomic = push_request(user_id, client_id, "atomic-false", vec![fresh.clone()]);
        not_atomic["atomic"] = json!(false);
        let mut duplicate_row = fresh.clone();
        duplicate_row["mutation_id"] = json!(mutation_id(user_id, client_id, "atomic-duplicate"));
        let ledgers = push_ledger_counts(user_id, client_id);

        for request in [
            atomic_push_request(
                user_id,
                client_id,
                "atomic-reuse",
                vec![fresh.clone(), stored],
            ),
            atomic_push_request(
                user_id,
                client_id,
                "atomic-duplicate-row",
                vec![fresh, duplicate_row],
            ),
            not_atomic,
        ] {
            let response = execute_push(user_id, &request);
            assert_eq!(response.json["error"]["code"], "invalid_request");
        }
        assert_eq!(push_ledger_counts(user_id, client_id), ledgers);
        assert_eq!(source_order_count(&[fresh_id]), 0);
    }

    #[pg_test]
    fn test_atomic_push_rollback_restores_fence_and_mutation_settings() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let conflict_id = "a7000000-0000-4000-8000-000000000003";
        insert_live_order(conflict_id, user_id, "committed");
        let settings = || {
            Spi::get_one::<pgrx::JsonB>(
                "SELECT jsonb_build_object(
                     'dml_ordinal', current_setting('synchro.dml_ordinal', true),
                     'mutation_id', current_setting('synchro.mutation_id', true)
                 )",
            )
            .unwrap()
            .expect("push settings")
            .0
        };
        let before = settings();

        let response = execute_push(
            user_id,
            &atomic_push_request(
                user_id,
                client_id,
                "atomic-settings",
                vec![
                    order_insert(
                        user_id,
                        "atomic-settings-first",
                        "a7000000-0000-4000-8000-000000000001",
                    ),
                    order_insert(
                        user_id,
                        "atomic-settings-second",
                        "a7000000-0000-4000-8000-000000000002",
                    ),
                    order_insert(user_id, "atomic-settings-conflict", conflict_id),
                ],
            ),
        );

        assert_eq!(response.json["rejected"][2]["code"], "row_already_exists");
        let after = settings();
        assert_eq!(after["dml_ordinal"], before["dml_ordinal"]);
        assert_eq!(after["mutation_id"], "");
    }

    #[pg_test]
    fn test_atomic_push_error_releases_the_subtransaction() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        Spi::run(&format!(
            "CREATE FUNCTION synchro.synchro_write_protect(TEXT, TEXT, TEXT, JSONB)
             RETURNS JSONB
             LANGUAGE plpgsql
             AS $policy$
             BEGIN
                 IF $4 ->> '{title}' = 'raise' THEN
                     RAISE EXCEPTION 'atomic policy failure';
                 END IF;
                 RETURN $4;
             END
             $policy$",
            title = field_id("test_orders", "title"),
        ))
        .unwrap();
        let first = "a8000000-0000-4000-8000-000000000001";
        let mut raising = order_insert(
            user_id,
            "atomic-error-raise",
            "a8000000-0000-4000-8000-000000000002",
        );
        raising["columns"] = logical_columns(
            "test_orders",
            &[("user_id", json!(user_id)), ("title", json!("raise"))],
        );
        let request = atomic_push_request(
            user_id,
            client_id,
            "atomic-error",
            vec![order_insert(user_id, "atomic-error-first", first), raising],
        );
        Spi::run_with_args(
            "SELECT set_config('tests.atomic_user', $1, true),
                    set_config('tests.atomic_request', $2, true)",
            &[user_id.into(), request.to_string().into()],
        )
        .unwrap();
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };

        // A catching caller must roll back its own subtransaction, not the push subtransaction.
        Spi::run(
            "DO $caller$
             BEGIN
                 PERFORM synchro_push(
                     current_setting('tests.atomic_user'),
                     current_setting('tests.atomic_request')::jsonb
                 );
             EXCEPTION WHEN OTHERS THEN
                 PERFORM set_config('tests.atomic_error', SQLERRM, true);
             END
             $caller$",
        )
        .unwrap();
        let error: Option<String> =
            Spi::get_one("SELECT current_setting('tests.atomic_error', true)").unwrap();

        assert_eq!(error.as_deref(), Some("atomic policy failure"));
        assert_eq!(
            unsafe { pg_sys::GetCurrentTransactionNestLevel() },
            nest_level
        );
        assert_eq!(push_ledger_counts(user_id, client_id), (0, 0));
        assert_eq!(source_order_count(&[first]), 0);

        Spi::run("DROP FUNCTION synchro.synchro_write_protect(TEXT, TEXT, TEXT, JSONB)").unwrap();
        let response = execute_push(
            user_id,
            &atomic_push_request(
                user_id,
                client_id,
                "atomic-after-error",
                vec![order_insert(user_id, "atomic-after-error", first)],
            ),
        );
        assert_eq!(response.json["accepted"][0]["status"], "applied");
        assert_eq!(source_order_count(&[first]), 1);
    }

    fn deferred_write_applied(record_id: &str) -> bool {
        Spi::get_one_with_args(
            "SELECT amount = 42 FROM test_orders WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap()
        .expect("deferred write source row")
    }

    fn write_fence_mutations(record_id: &str) -> Vec<Option<String>> {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT mutation_id FROM sync_write_fences
                     WHERE transaction_xid = pg_current_xact_id()
                       AND operation = 'update'
                       AND new_record_id = $1
                     ORDER BY dml_ordinal",
                    None,
                    &[record_id.into()],
                )
                .unwrap()
                .map(|row| row.get_by_name::<String, &str>("mutation_id").unwrap())
                .collect()
        })
    }

    /// A push unit ends at its constraint check. Each accepted outcome reports the row state after
    /// that check, and the last mutation of the unit owns each write that the check causes.
    #[pg_test]
    fn test_push_unit_outcomes_include_deferred_trigger_writes() {
        setup_test_tables();
        let user_id = "u1";
        let client_id = "c1";
        register_client(user_id, client_id);
        let atomic_other = "a9000000-0000-4000-8000-000000000001";
        let single_other = "a9000000-0000-4000-8000-000000000002";
        insert_live_order(atomic_other, user_id, "atomic-other");
        insert_live_order(single_other, user_id, "single-other");
        Spi::run(
            "CREATE FUNCTION public.test_orders_deferred_write() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 UPDATE public.test_orders SET amount = 42 WHERE id = NEW.id;
                 UPDATE public.test_orders SET title = 'deferred-written'
                 WHERE id = split_part(NEW.title, ':', 2)::uuid;
                 RETURN NULL;
             END;
             $$;
             CREATE CONSTRAINT TRIGGER test_orders_deferred_write
             AFTER INSERT ON public.test_orders
             DEFERRABLE INITIALLY DEFERRED
             FOR EACH ROW WHEN (NEW.title LIKE 'deferred:%')
             EXECUTE FUNCTION public.test_orders_deferred_write()",
        )
        .unwrap();

        let atomic_first = "a9000000-0000-4000-8000-000000000011";
        let atomic_last = "a9000000-0000-4000-8000-000000000012";
        let atomic_mutations = vec![
            order_insert(user_id, &format!("deferred:{atomic_other}"), atomic_first),
            order_insert(user_id, "atomic-last", atomic_last),
        ];
        let atomic_request = atomic_push_request(
            user_id,
            client_id,
            "atomic-deferred-write",
            atomic_mutations.clone(),
        );
        let atomic = execute_push(user_id, &atomic_request);

        assert_eq!(atomic.json["rejected"], json!([]));
        let accepted = atomic.json["accepted"].as_array().expect("atomic accepted");
        assert!(deferred_write_applied(atomic_first));
        for (outcome, record_id) in accepted.iter().zip([atomic_first, atomic_last]) {
            assert_row_outcome_matches_source(outcome, "test_orders", record_id);
        }
        let atomic_last_id = atomic_mutations[1]["mutation_id"].as_str().unwrap();
        for record_id in [atomic_first, atomic_other] {
            assert_eq!(
                write_fence_mutations(record_id),
                vec![Some(atomic_last_id.to_string())]
            );
        }
        let replay = execute_push(user_id, &atomic_request);
        assert_eq!(replay.raw, atomic.raw);

        let single_record = "a9000000-0000-4000-8000-000000000021";
        let single_mutation = order_insert(user_id, &format!("deferred:{single_other}"), single_record);
        let single = execute_push(
            user_id,
            &push_request(
                user_id,
                client_id,
                "single-deferred-write",
                vec![single_mutation.clone()],
            ),
        );

        let outcome = &single.json["accepted"][0];
        assert!(deferred_write_applied(single_record));
        assert_row_outcome_matches_source(outcome, "test_orders", single_record);
        let single_id = single_mutation["mutation_id"].as_str().unwrap();
        for record_id in [single_record, single_other] {
            assert_eq!(
                write_fence_mutations(record_id),
                vec![Some(single_id.to_string())]
            );
        }
    }
