    #[pg_test]
    fn test_register_table_basic() {
        setup_test_tables();
        let count: Option<i64> = Spi::get_one(
            "SELECT count(*)
             FROM sync_registry r
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'active' AND r.table_name = 'test_orders'",
        )
        .unwrap();
        assert_eq!(count, Some(1));
    }

    #[pg_test]
    fn test_registration_waits_for_source_write_gate() {
        run_source_gated_registration(
            "CREATE TABLE test_registration_gate (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             )",
            "SELECT tests.register_legacy_test_table(
                 'test_registration_gate',
                 $$SELECT ARRAY['global'] FROM test_registration_gate WHERE id = $1::uuid$$,
                 'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
            "DROP TABLE test_registration_gate CASCADE",
        );
    }

    #[pg_test]
    fn test_capture_registration_waits_for_source_gate() {
        run_source_gated_registration(
            "CREATE TABLE test_capture_registration_gate (
                  id UUID PRIMARY KEY,
                  value TEXT NOT NULL
              );
              GRANT SELECT ON TABLE test_capture_registration_gate TO synchro_owner;
              ALTER TABLE test_capture_registration_gate ENABLE ROW LEVEL SECURITY;
              CREATE POLICY test_capture_registration_gate_policy
                  ON test_capture_registration_gate
                  AS PERMISSIVE FOR ALL TO synchro_owner
                  USING (true) WITH CHECK (true)",
            "SELECT synchro_register_capture_dependency(
                 'public.test_capture_registration_gate', ARRAY['id'], ARRAY['value']
             )",
            "DROP TABLE test_capture_registration_gate CASCADE",
        );
    }

    #[pg_test]
    fn test_backfill_waits_for_progress_before_checkpoint_lock() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("install dblink extension");
        let connection_string: String = Spi::get_one(
            "SELECT format(
                        'host=%L port=%s dbname=%I user=%I',
                        current_setting('unix_socket_directories'),
                        current_setting('port'),
                        current_database(),
                        current_user
                    )",
        )
        .unwrap()
        .expect("dblink connection string");
        let pull_name = "synchro_backfill_pull";
        let backfill_name = "synchro_backfill_contender";
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[pull_name.into(), connection_string.as_str().into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[backfill_name.into(), connection_string.as_str().into()],
        )
        .unwrap();

        dblink_exec(pull_name, "SET lock_timeout = '5s'");
        dblink_exec(backfill_name, "SET statement_timeout = '5s'");
        dblink_exec(pull_name, "BEGIN");
        dblink_exec(
            pull_name,
            "LOCK TABLE synchro.sync_wal_progress IN SHARE MODE",
        );
        dblink_exec(backfill_name, "BEGIN");
        let backfill_pid: i32 = dblink_query(backfill_name, "SELECT pg_backend_pid()")
            .parse()
            .expect("parse backfill PID");
        let sent: i32 = Spi::get_one_with_args(
            "SELECT public.dblink_send_query($1, $2)",
            &[
                backfill_name.into(),
                "SELECT synchro_backfill_bucket_edges(NULL)".into(),
            ],
        )
        .unwrap()
        .expect("send backfill query");

        let mut waiting_for_progress = false;
        for _ in 0..1000 {
            waiting_for_progress = Spi::get_one_with_args(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE pid = $1
                       AND relation = 'synchro.sync_wal_progress'::regclass
                       AND mode = 'ShareRowExclusiveLock'
                       AND NOT granted
                 )",
                &[i64::from(backfill_pid).into()],
            )
            .unwrap()
            .unwrap_or(false);
            if waiting_for_progress {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let checkpoint_locked = if waiting_for_progress {
            Spi::get_one_with_args(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE pid = $1
                       AND relation = 'synchro.sync_client_checkpoints'::regclass
                       AND mode = 'ShareRowExclusiveLock'
                       AND granted
                 )",
                &[i64::from(backfill_pid).into()],
            )
            .unwrap()
            .unwrap_or(false)
        } else {
            false
        };

        if waiting_for_progress {
            dblink_exec(
                pull_name,
                "LOCK TABLE synchro.sync_client_checkpoints IN ROW EXCLUSIVE MODE",
            );
        }
        dblink_exec(pull_name, "COMMIT");
        let result = dblink_get_result(backfill_name);
        Spi::run_with_args(
            "SELECT result
             FROM public.dblink_get_result($1) AS result_row(result text)",
            &[backfill_name.into()],
        )
        .unwrap();
        dblink_exec(backfill_name, "ROLLBACK");
        Spi::run_with_args(
            "SELECT public.dblink_disconnect($1)",
            &[pull_name.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT public.dblink_disconnect($1)",
            &[backfill_name.into()],
        )
        .unwrap();

        assert_eq!(sent, 1);
        assert!(waiting_for_progress, "backfill did not wait for progress");
        assert!(!checkpoint_locked, "backfill locked checkpoints before progress");
        assert!(!result.starts_with("ERROR"), "backfill failed: {result}");
    }

    fn run_source_gated_registration(
        create_table: &str,
        registration: &str,
        drop_table: &str,
    ) {
        Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("install dblink extension");
        let connection_string: String = Spi::get_one(
            "SELECT format(
                        'host=%L port=%s dbname=%I user=%I',
                        current_setting('unix_socket_directories'),
                        current_setting('port'),
                        current_database(),
                        current_user
                    )",
        )
        .unwrap()
        .expect("dblink connection string");
        let driver_name = "synchro_registration_gate_driver";
        let contender_name = "synchro_registration_gate_contender";
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[driver_name.into(), connection_string.as_str().into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[contender_name.into(), connection_string.as_str().into()],
        )
        .unwrap();

        dblink_exec(
            driver_name,
            create_table,
        );
        dblink_exec(driver_name, "BEGIN");
        dblink_query(
            driver_name,
            &format!(
                "SELECT pg_catalog.pg_advisory_xact_lock({})",
                crate::SOURCE_WRITE_GATE_LOCK_KEY
            ),
        );
        dblink_exec(contender_name, "BEGIN");
        let contender_pid: i32 = dblink_query(contender_name, "SELECT pg_backend_pid()")
            .parse()
            .expect("parse registration contender PID");
        let sent: i32 = Spi::get_one_with_args(
            "SELECT public.dblink_send_query($1, $2)",
            &[contender_name.into(), registration.into()],
        )
        .unwrap()
        .expect("send source-gated registration");
        assert_eq!(sent, 1);

        let mut waiting = false;
        for _ in 0..1000 {
            let waiting_row: Option<bool> = Spi::get_one_with_args(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE pid = $1 AND locktype = 'advisory' AND NOT granted
                 )",
                &[i64::from(contender_pid).into()],
            )
            .unwrap();
            if waiting_row == Some(true) {
                waiting = true;
                break;
            }
        }
        assert!(waiting, "registration did not wait for the source write gate");
        dblink_exec(driver_name, "COMMIT");
        let result = dblink_get_result(contender_name);
        assert!(!result.starts_with("ERROR"), "source-gated registration failed: {result}");
        Spi::run_with_args(
            "SELECT result
             FROM public.dblink_get_result($1) AS result_row(result text)",
            &[contender_name.into()],
        )
        .unwrap();
        dblink_exec(contender_name, "ROLLBACK");

        dblink_exec(driver_name, drop_table);
        Spi::run_with_args(
            "SELECT public.dblink_disconnect($1)",
            &[driver_name.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT public.dblink_disconnect($1)",
            &[contender_name.into()],
        )
        .unwrap();
    }

    #[pg_test]
    fn test_register_table_allocates_logical_identities() {
        setup_test_tables();
        let valid: Option<bool> = Spi::get_one(
            "SELECT r.relation_id IS NOT NULL
                    AND r.table_id IS NOT NULL
                    AND r.primary_key_field_id = pk.field_id
                    AND pk.physical_column = r.pk_column
                    AND pk.primary_key
                    AND NOT pk.nullable
                    AND NOT pk.writable
                    AND count(fields.field_id) = cardinality(r.sync_columns)
             FROM sync_registry r
             JOIN sync_registry_generations g
               ON g.generation = r.registry_generation
             JOIN sync_registry_fields pk
               ON pk.registry_generation = r.registry_generation
              AND pk.relation_id = r.relation_id
              AND pk.field_id = r.primary_key_field_id
             JOIN sync_registry_fields fields
               ON fields.registry_generation = r.registry_generation
              AND fields.relation_id = r.relation_id
             WHERE g.state = 'active' AND r.table_name = 'test_orders'
             GROUP BY r.relation_id, r.table_id, r.primary_key_field_id,
                      r.pk_column, r.sync_columns, pk.field_id,
                      pk.physical_column, pk.primary_key, pk.nullable, pk.writable",
        )
        .unwrap();
        assert_eq!(valid, Some(true));
    }

    #[pg_test]
    fn test_reconfiguration_preserves_logical_identities() {
        setup_test_tables();
        Spi::run(
            "CREATE TEMP TABLE prior_logical_ids AS
             SELECT r.relation_id, r.table_id, r.primary_key_field_id,
                    array_agg(f.field_id ORDER BY f.physical_column) AS field_ids
             FROM sync_registry r
             JOIN sync_registry_generations g
               ON g.generation = r.registry_generation
             JOIN sync_registry_fields f
               ON f.registry_generation = r.registry_generation
              AND f.relation_id = r.relation_id
             WHERE g.state = 'active' AND r.table_name = 'test_orders'
             GROUP BY r.relation_id, r.table_id, r.primary_key_field_id",
        )
        .unwrap();

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                'test_orders',
                $$SELECT ARRAY['alternate:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                'single_scope',
                'id', 'updated_at', 'deleted_at', 'enabled',
                ARRAY['internal_notes']
            )",
        )
        .unwrap();

        let preserved: Option<bool> = Spi::get_one(
            "SELECT prior.relation_id = current.relation_id
                    AND prior.table_id = current.table_id
                    AND prior.primary_key_field_id = current.primary_key_field_id
                    AND prior.field_ids = current.field_ids
             FROM prior_logical_ids prior
             CROSS JOIN LATERAL (
                 SELECT r.relation_id, r.table_id, r.primary_key_field_id,
                        array_agg(f.field_id ORDER BY f.physical_column) AS field_ids
                 FROM sync_registry r
                 JOIN sync_registry_generations g
                   ON g.generation = r.registry_generation
                 JOIN sync_registry_fields f
                   ON f.registry_generation = r.registry_generation
                  AND f.relation_id = r.relation_id
                 WHERE g.state = 'pending' AND r.table_name = 'test_orders'
                 GROUP BY r.relation_id, r.table_id, r.primary_key_field_id
             ) current",
        )
        .unwrap();
        assert_eq!(preserved, Some(true));
    }

    #[pg_test]
    fn test_additive_field_gets_new_identity() {
        setup_test_tables();
        Spi::run(
            "CREATE TEMP TABLE prior_field_ids AS
             SELECT f.physical_column, f.field_id
             FROM sync_registry r
             JOIN sync_registry_generations g
               ON g.generation = r.registry_generation
             JOIN sync_registry_fields f
               ON f.registry_generation = r.registry_generation
              AND f.relation_id = r.relation_id
             WHERE g.state = 'active' AND r.table_name = 'test_orders'",
        )
        .unwrap();
        Spi::run("ALTER TABLE test_orders ADD COLUMN summary TEXT").unwrap();
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

        let retained_count: Option<i64> = Spi::get_one(
            "SELECT count(*)
             FROM prior_field_ids prior
             JOIN sync_registry_fields current
               ON current.physical_column = prior.physical_column
              AND current.field_id = prior.field_id
             JOIN sync_registry r
               ON r.registry_generation = current.registry_generation
              AND r.relation_id = current.relation_id
             JOIN sync_registry_generations g
               ON g.generation = r.registry_generation
             WHERE g.state = 'pending' AND r.table_name = 'test_orders'",
        )
        .unwrap();
        let prior_count: Option<i64> =
            Spi::get_one("SELECT count(*) FROM prior_field_ids").unwrap();
        assert_eq!(retained_count, prior_count);
        let added: Option<bool> = Spi::get_one(
            "SELECT current.field_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM prior_field_ids prior
                        WHERE prior.field_id = current.field_id
                    )
             FROM sync_registry_fields current
             JOIN sync_registry r
               ON r.registry_generation = current.registry_generation
              AND r.relation_id = current.relation_id
             JOIN sync_registry_generations g
               ON g.generation = r.registry_generation
             WHERE g.state = 'pending'
               AND r.table_name = 'test_orders'
               AND current.physical_column = 'summary'",
        )
        .unwrap();
        assert_eq!(added, Some(true));
    }

    #[pg_test]
    fn test_activation_load_accepts_retired_column_shape() {
        setup_test_tables();
        let active: i64 = Spi::get_one(
            "SELECT generation FROM sync_registry_generations WHERE state = 'active'",
        )
        .unwrap()
        .expect("active registry generation");
        let steady = Spi::connect(|client| {
            crate::registry::load_registry_generation_for_worker(client, active)
        })
        .unwrap();
        assert!(steady
            .iter()
            .any(|registration| registration.table_name == "test_orders"));

        Spi::run("ALTER TABLE test_orders DROP COLUMN title").unwrap();
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
        let pending: i64 = Spi::get_one(
            "SELECT generation FROM sync_registry_generations
             WHERE state = 'pending' AND validated
             ORDER BY generation DESC LIMIT 1",
        )
        .unwrap()
        .expect("pending registry generation");

        let prior = Spi::connect(|client| {
            crate::registry::load_registry_generation_for_activation(client, active, pending)
        })
        .unwrap();
        let worker = Spi::connect(|client| {
            crate::registry::load_registry_generation_for_worker(client, active)
        })
        .unwrap();
        let pending_registry = Spi::connect(|client| {
            crate::registry::load_registry_generation_from_client(client, pending)
        })
        .unwrap();
        let prior_orders = prior
            .iter()
            .find(|registration| registration.table_name == "test_orders")
            .expect("prior orders registration");
        let worker_orders = worker
            .iter()
            .find(|registration| registration.table_name == "test_orders")
            .expect("worker orders registration");
        let pending_orders = pending_registry
            .iter()
            .find(|registration| registration.table_name == "test_orders")
            .expect("pending orders registration");
        assert!(prior_orders.sync_columns.iter().any(|column| column == "title"));
        assert!(worker_orders.sync_columns.iter().any(|column| column == "title"));
        assert!(!pending_orders.sync_columns.iter().any(|column| column == "title"));
    }

    #[pg_test]
    fn test_primary_key_change_allocates_new_logical_identities() {
        setup_test_tables();
        Spi::run(
            "CREATE TABLE test_key_change (
                 id UUID PRIMARY KEY,
                 alternate_id TEXT NOT NULL UNIQUE,
                 value TEXT NOT NULL
             )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_key_change',
                 $$SELECT ARRAY['global'] FROM test_key_change WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        Spi::run(
            "CREATE TEMP TABLE prior_key_ids AS
             SELECT relation_id, table_id
             FROM sync_registry r
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'pending' AND r.table_name = 'test_key_change'",
        )
        .unwrap();
        Spi::run("ALTER TABLE test_key_change DROP CONSTRAINT test_key_change_pkey").unwrap();
        Spi::run("ALTER TABLE test_key_change ADD PRIMARY KEY (alternate_id)").unwrap();
        Spi::run("DROP TRIGGER synchro_primary_key_guard ON test_key_change").unwrap();
        Spi::run("DROP TRIGGER synchro_capture_fence ON test_key_change").unwrap();
        Spi::run("DROP TRIGGER synchro_capture_truncate_guard ON test_key_change").unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_key_change',
                 $$SELECT ARRAY['global'] FROM test_key_change WHERE alternate_id = $1::text$$,
                 'single_scope',
                 'alternate_id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();

        let replaced: Option<bool> = Spi::get_one(
            "SELECT prior.relation_id <> current.relation_id
                    AND prior.table_id <> current.table_id
             FROM prior_key_ids prior
             CROSS JOIN LATERAL (
                 SELECT r.relation_id, r.table_id
                 FROM sync_registry r
                 JOIN sync_registry_generations g
                   ON g.generation = r.registry_generation
                 WHERE g.state = 'pending'
                   AND r.table_name = 'test_key_change'
                 ORDER BY g.generation DESC
                 LIMIT 1
             ) current",
        )
        .unwrap();
        assert_eq!(replaced, Some(true));
    }

    #[pg_test]
    fn test_recreated_relation_does_not_inherit_logical_identities() {
        Spi::run(
            "CREATE TABLE test_recreated_identity (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_recreated_identity',
                 $$SELECT ARRAY['global'] FROM test_recreated_identity WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        Spi::run(
            "CREATE TEMP TABLE prior_recreated_ids AS
             SELECT relation_id, table_id
             FROM sync_registry r
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'pending' AND r.table_name = 'test_recreated_identity'",
        )
        .unwrap();
        Spi::run("DROP TABLE test_recreated_identity CASCADE").unwrap();
        Spi::run(
            "CREATE TABLE test_recreated_identity (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_recreated_identity',
                 $$SELECT ARRAY['global'] FROM test_recreated_identity WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();

        let replaced: Option<bool> = Spi::get_one(
            "SELECT prior.relation_id <> current.relation_id
                    AND prior.table_id <> current.table_id
             FROM prior_recreated_ids prior
             CROSS JOIN LATERAL (
                 SELECT r.relation_id, r.table_id
                 FROM sync_registry r
                 JOIN sync_registry_generations g
                   ON g.generation = r.registry_generation
                 WHERE g.state = 'pending'
                   AND r.table_name = 'test_recreated_identity'
                 ORDER BY g.generation DESC
                 LIMIT 1
             ) current",
        )
        .unwrap();
        assert_eq!(replaced, Some(true));
    }

    #[pg_test]
    fn test_unregister_does_not_delete_logical_identity_ledger() {
        setup_test_tables();
        let before: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_logical_ids").unwrap();
        Spi::run("SELECT synchro_unregister_table('test_orders')").unwrap();
        let after: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_logical_ids").unwrap();
        assert_eq!(after, before);
    }

    #[pg_test]
    fn test_register_table_creates_publication() {
        setup_test_tables();
        let in_pub: Option<bool> = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_publication_tables
                WHERE pubname = 'synchro_pub' AND tablename = 'test_orders'
            )",
        )
        .unwrap();
        assert_eq!(in_pub, Some(true));
    }

    #[pg_test]
    fn test_register_table_schema_manifest() {
        setup_test_tables();
        let version: Option<i64> =
            Spi::get_one("SELECT MAX(schema_version) FROM sync_schema_manifest").unwrap();
        assert!(version.unwrap_or(0) > 0);
    }

    #[pg_test]
    fn test_schema_manifest_publishes_empty_active_generation() {
        Spi::connect_mut(crate::schema::publish_schema_manifest).unwrap();

        let published: Option<bool> = Spi::get_one(
            "SELECT m.registry_generation = g.generation
                    AND (m.canonical_manifest_body::jsonb -> 'tables') = '[]'::jsonb
             FROM sync_schema_manifest m
             JOIN sync_registry_generations g
               ON g.generation = m.registry_generation
             WHERE g.state = 'active' AND g.validated
             ORDER BY m.schema_version DESC
             LIMIT 1",
        )
        .unwrap();
        assert_eq!(published, Some(true));
    }

    #[pg_test]
    fn test_registry_activation_publishes_schema_manifest() {
        Spi::run(
            "CREATE TABLE test_activation_manifest (
                 id UUID PRIMARY KEY,
                 name TEXT NOT NULL
             )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_activation_manifest',
                 $$SELECT ARRAY['global'] FROM test_activation_manifest WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        let pending: Option<i64> = Spi::get_one(
            "SELECT generation FROM sync_registry_generations
             WHERE state = 'pending' ORDER BY generation DESC LIMIT 1",
        )
        .unwrap();
        let pending = pending.expect("pending registry generation");
        let before: Option<i64> =
            Spi::get_one("SELECT max(schema_version) FROM sync_schema_manifest").unwrap();

        Spi::connect_mut(|client| {
            client.update(
                "UPDATE sync_registry_generations
                 SET state = 'superseded'
                 WHERE state = 'active'",
                None,
                &[],
            )?;
            client.update(
                "UPDATE sync_registry_generations
                 SET state = 'active', activated_at = now(),
                     activation_commit_lsn = '0/1'::pg_lsn,
                     activation_end_lsn = '0/1'::pg_lsn
                 WHERE generation = $1",
                None,
                &[pending.into()],
            )?;
            crate::schema::publish_schema_manifest(client)
        })
        .unwrap();

        let after: Option<i64> =
            Spi::get_one("SELECT max(schema_version) FROM sync_schema_manifest").unwrap();
        assert!(after.unwrap_or(0) > before.unwrap_or(0));
        let manifest: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest = manifest.unwrap().0;
        assert!(manifest["manifest"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| table["name"] == "test_activation_manifest"));
    }

    #[pg_test]
    fn test_schema_manifest_stores_canonical_body_and_domain_hash() {
        setup_test_tables();
        let stored: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT jsonb_build_object(
                 'version', schema_version,
                 'hash', schema_hash,
                 'body', canonical_manifest_body,
                 'class', transition_class,
                 'parent_version', parent_schema_version,
                 'parent_hash', parent_schema_hash,
                 'floor', compatibility_floor
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap();
        let stored = stored.unwrap().0;
        let body = stored["body"].as_str().unwrap();
        let mut hasher = Sha256::new();
        hasher.update(b"synchro:v3:schema-manifest:v1\0");
        hasher.update(body.as_bytes());
        assert_eq!(
            stored["hash"].as_str().unwrap(),
            format!("{:x}", hasher.finalize())
        );
        let body = serde_json::from_str::<Value>(body).unwrap();
        assert_eq!(body["schema_version"], stored["version"]);
        assert_eq!(body["transition_class"], stored["class"]);
        assert_eq!(body["compatibility_floor"], stored["floor"]);
        assert_eq!(body["parent_schema"]["version"], stored["parent_version"]);
        assert_eq!(body["parent_schema"]["hash"], stored["parent_hash"]);
    }

    #[pg_test]
    fn test_published_schema_manifest_is_immutable() {
        setup_test_tables();
        let update = std::panic::catch_unwind(|| {
            Spi::run(
                "UPDATE sync_schema_manifest
                 SET canonical_manifest_body = '{}'
                 WHERE schema_version = (SELECT max(schema_version) FROM sync_schema_manifest)",
            )
            .unwrap();
        });
        assert!(update.is_err());
    }

    #[pg_test]
    fn test_schema_manifest_history_keeps_original_body() {
        setup_test_tables();
        Spi::run(
            "CREATE TEMP TABLE prior_manifest AS
             SELECT schema_version, schema_hash, canonical_manifest_body,
                    compatibility_floor
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap();
        Spi::run("ALTER TABLE test_orders ADD COLUMN optional_note TEXT").unwrap();
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

        let valid: Option<bool> = Spi::get_one(
            "SELECT prior.schema_hash = stored.schema_hash
                    AND prior.canonical_manifest_body = stored.canonical_manifest_body
                    AND latest.transition_class = 'class_2'
                    AND latest.parent_schema_version = prior.schema_version
                    AND latest.parent_schema_hash = prior.schema_hash
                    AND latest.compatibility_floor = prior.compatibility_floor
             FROM prior_manifest prior
             JOIN sync_schema_manifest stored
               ON stored.schema_version = prior.schema_version
             CROSS JOIN LATERAL (
                 SELECT * FROM sync_schema_manifest
                 ORDER BY schema_version DESC
                 LIMIT 1
             ) latest",
        )
        .unwrap();
        assert_eq!(valid, Some(true));
    }

    #[pg_test]
    fn test_added_empty_table_is_class_2_without_bootstrap() {
        setup_test_tables();
        register_client("empty-table-user", "empty-table-client");
        Spi::run(
            "CREATE TABLE test_empty_added_manifest_boundary (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             );
             INSERT INTO test_empty_added_manifest_boundary (id, value)
             VALUES ('a1000000-0000-4000-8000-000000000101', 'existing')",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_empty_added_manifest_boundary',
                 $$SELECT ARRAY['global'] FROM test_empty_added_manifest_boundary WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();
        let boundary_class: String = Spi::get_one(
            "SELECT transition_class
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("empty added-table boundary manifest");
        assert_eq!(boundary_class, "class_3");
        Spi::run(
            "CREATE TABLE test_empty_added_manifest (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             )",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_empty_added_manifest',
                 $$SELECT ARRAY['global'] FROM test_empty_added_manifest WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        let generation: i64 = Spi::get_one(
            "SELECT generation
             FROM sync_registry_generations
             WHERE state = 'pending' AND validated
             ORDER BY generation DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("empty added-table generation");
        let requires_bootstrap = Spi::connect(|client| {
            crate::schema::generation_requires_projection_bootstrap(client, generation)
        })
        .unwrap();

        activate_pending_registry_for_test();

        let transition: pgrx::JsonB = Spi::get_one(
            "SELECT jsonb_build_object(
                 'metadata_class', transition_class,
                 'body_class', canonical_manifest_body::jsonb ->> 'transition_class',
                 'affected_scopes', affected_scopes,
                 'compatibility_floor', compatibility_floor,
                 'parent_schema_version', parent_schema_version
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("empty added-table manifest transition");

        assert!(!requires_bootstrap);
        assert_eq!(transition.0["metadata_class"], "class_2");
        assert_eq!(transition.0["body_class"], "class_2");
        assert_eq!(transition.0["affected_scopes"], json!([]));
        assert_eq!(
            transition.0["compatibility_floor"],
            transition.0["parent_schema_version"]
        );
    }

    #[pg_test]
    fn test_added_empty_table_with_stale_stats_is_class_2() {
        setup_test_tables();
        register_client("stale-statistics-user", "stale-statistics-client");
        Spi::run(
            "CREATE TABLE test_empty_stale_statistics_manifest (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             );
             INSERT INTO test_empty_stale_statistics_manifest (id, value)
             VALUES ('a1000000-0000-4000-8000-000000000102', 'stale');
             ANALYZE test_empty_stale_statistics_manifest;
             DELETE FROM test_empty_stale_statistics_manifest",
        )
        .unwrap();
        let has_stale_estimate: Option<bool> = Spi::get_one(
            "SELECT reltuples > 0
             FROM pg_catalog.pg_class
             WHERE oid = 'test_empty_stale_statistics_manifest'::regclass",
        )
        .unwrap();
        assert_eq!(has_stale_estimate, Some(true));
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_empty_stale_statistics_manifest',
                 $$SELECT ARRAY['global'] FROM test_empty_stale_statistics_manifest WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        let generation: i64 = Spi::get_one(
            "SELECT generation
             FROM sync_registry_generations
             WHERE state = 'pending' AND validated
             ORDER BY generation DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("stale-statistics generation");
        let requires_bootstrap = Spi::connect(|client| {
            crate::schema::generation_requires_projection_bootstrap(client, generation)
        })
        .unwrap();
        let pending_body = Spi::connect(|client| {
            let pending = crate::schema::prepare_pending_manifest(client, generation)
                .expect("prepare stale-statistics manifest")
                .expect("stale-statistics pending manifest");
            Ok::<_, spi::Error>(
                serde_json::from_str::<Value>(&pending.canonical_body)
                    .expect("decode stale-statistics pending manifest"),
            )
        })
        .unwrap();

        activate_pending_registry_for_test();

        let transition: pgrx::JsonB = Spi::get_one(
            "SELECT jsonb_build_object(
                 'metadata_class', transition_class,
                 'body_class', canonical_manifest_body::jsonb ->> 'transition_class',
                 'affected_scopes', affected_scopes
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("stale-statistics manifest transition");

        assert!(!requires_bootstrap);
        assert_eq!(pending_body["transition_class"], "class_2");
        assert_eq!(transition.0["metadata_class"], "class_2");
        assert_eq!(transition.0["body_class"], "class_2");
        assert_eq!(transition.0["affected_scopes"], json!([]));
    }

    #[pg_test]
    fn test_nonempty_nullable_relaxation_is_class_2_without_bootstrap() {
        setup_test_tables();
        register_client("nullable-user", "nullable-client");
        let prior_floor: i64 = Spi::get_one(
            "SELECT compatibility_floor
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("nullable relaxation prior compatibility floor");
        Spi::run(
            "INSERT INTO test_orders (user_id, title)
             VALUES ('nullable-user', 'existing');
             ALTER TABLE test_orders ALTER COLUMN title DROP NOT NULL",
        )
        .unwrap();
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
        let generation: i64 = Spi::get_one(
            "SELECT generation
             FROM sync_registry_generations
             WHERE state = 'pending' AND validated
             ORDER BY generation DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("nullable relaxation generation");
        let requires_bootstrap = Spi::connect(|client| {
            crate::schema::generation_requires_projection_bootstrap(client, generation)
        })
        .unwrap();
        let pending_body = Spi::connect(|client| {
            let pending = crate::schema::prepare_pending_manifest(client, generation)
                .expect("prepare nullable relaxation manifest")
                .expect("nullable relaxation pending manifest");
            Ok::<_, spi::Error>(
                serde_json::from_str::<Value>(&pending.canonical_body)
                    .expect("decode nullable relaxation pending manifest"),
            )
        })
        .unwrap();

        activate_pending_registry_for_test();

        let transition: pgrx::JsonB = Spi::get_one(
            "SELECT jsonb_build_object(
                 'metadata_class', transition_class,
                 'body_class', canonical_manifest_body::jsonb ->> 'transition_class',
                 'affected_scopes', affected_scopes,
                 'compatibility_floor', compatibility_floor,
                 'parent_schema_version', parent_schema_version
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("nullable relaxation manifest transition");

        assert!(!requires_bootstrap);
        assert_eq!(pending_body["transition_class"], "class_2");
        assert_eq!(pending_body["compatibility_floor"], prior_floor);
        assert_eq!(transition.0["metadata_class"], "class_2");
        assert_eq!(transition.0["body_class"], "class_2");
        assert_eq!(transition.0["affected_scopes"], json!([]));
        assert_eq!(transition.0["compatibility_floor"], prior_floor);
    }

    #[pg_test]
    fn test_class_2_migrates_current_digests() {
        setup_test_tables();
        let user_id = "class-2-digest-user";
        let client_id = "class-2-digest-client";
        let scope_id = "user:class-2-digest-user";
        let record_id = "c2000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, $2, 'retained class 2 row')",
            &[record_id.into(), user_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, scope_id);
        insert_changelog(scope_id, "test_orders", record_id, 1);

        let prior_identity = Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let table = registry
                .iter()
                .find(|table| table.table_name == "test_orders")
                .expect("prior class 2 table");
            Ok::<_, spi::Error>(
                crate::pull::typed_primary_key_bytes(table, record_id)
                    .expect("prior class 2 row identity"),
            )
        })
        .unwrap();
        let prior_state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'relation_id', captured.relation_id::text,
                 'record_id', captured.record_id,
                 'row_version', captured.row_version::text,
                 'deleted', captured.deleted,
                 'source_stream_generation', captured.source_stream_generation,
                 'source_commit_lsn', captured.source_commit_lsn::text,
                 'source_event_ordinal', captured.source_event_ordinal,
                 'source_reset_id', captured.source_reset_id::text,
                 'registry_generation', captured.registry_generation,
                 'row_checksum', encode(captured.checksum, 'hex'),
                 'edge_table', edge.table_name,
                 'edge_bucket', edge.bucket_id,
                 'edge_row_version', edge.row_version::text,
                 'edge_checksum', encode(edge.checksum, 'hex'),
                 'membership_generation', state.membership_generation,
                 'retention_generation', state.retention_generation
             )
             FROM sync_captured_rows captured
             JOIN sync_bucket_edges edge
               ON edge.relation_id = captured.relation_id
              AND edge.record_id = captured.record_id
             JOIN sync_scope_state state ON state.scope_id = edge.bucket_id
             WHERE captured.record_id = $1 AND edge.bucket_id = $2",
            &[record_id.into(), scope_id.into()],
        )
        .unwrap()
        .expect("prior class 2 materialization state");
        assert_eq!(
            prior_state.0["row_checksum"], prior_state.0["edge_checksum"],
            "prior row and edge digests must match"
        );
        let prior_projection: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                     'registry_generation', projection.registry_generation,
                     'checksum', encode(projection.checksum, 'hex'),
                     'row_data', projection.row_data
                 )
              FROM sync_captured_projections projection
              JOIN sync_registry registry
                ON registry.registry_generation = (
                    SELECT generation FROM sync_registry_generations
                    WHERE state = 'active' ORDER BY generation DESC LIMIT 1
                )
               AND registry.relation_id = projection.relation_id
              WHERE registry.table_name = 'test_orders'
                AND projection.record_id = $1
                AND projection.image_kind = 'after'",
            &[record_id.into()],
        )
        .unwrap()
        .expect("prior class 2 captured projection");

        Spi::run("ALTER TABLE test_orders ADD COLUMN optional_class_2_note TEXT").unwrap();
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
        let target_generation: i64 = Spi::get_one(
            "SELECT generation
             FROM sync_registry_generations
             WHERE state = 'pending' AND validated
             ORDER BY generation DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("class 2 digest target generation");
        let requires_bootstrap = Spi::connect(|client| {
            crate::schema::generation_requires_projection_bootstrap(client, target_generation)
        })
        .unwrap();
        assert!(!requires_bootstrap);

        activate_pending_registry_for_test();

        let (schema_version, schema_hash) = latest_schema_ref();
        let transition: String = Spi::get_one_with_args(
            "SELECT transition_class
             FROM sync_schema_manifest
             WHERE schema_version = $1 AND schema_hash = $2",
            &[schema_version.into(), schema_hash.as_str().into()],
        )
        .unwrap()
        .expect("class 2 digest transition");
        assert_eq!(transition, "class_2");

        let current_state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'relation_id', captured.relation_id::text,
                 'record_id', captured.record_id,
                 'row_version', captured.row_version::text,
                 'deleted', captured.deleted,
                 'source_stream_generation', captured.source_stream_generation,
                 'source_commit_lsn', captured.source_commit_lsn::text,
                 'source_event_ordinal', captured.source_event_ordinal,
                 'source_reset_id', captured.source_reset_id::text,
                 'registry_generation', captured.registry_generation,
                 'row_checksum', encode(captured.checksum, 'hex'),
                 'row_data', captured.row_data,
                 'edge_table', edge.table_name,
                 'edge_bucket', edge.bucket_id,
                 'edge_row_version', edge.row_version::text,
                 'edge_checksum', encode(edge.checksum, 'hex'),
                 'membership_generation', state.membership_generation,
                 'retention_generation', state.retention_generation
             )
             FROM sync_captured_rows captured
             JOIN sync_bucket_edges edge
               ON edge.relation_id = captured.relation_id
              AND edge.record_id = captured.record_id
             JOIN sync_scope_state state ON state.scope_id = edge.bucket_id
             WHERE captured.record_id = $1 AND edge.bucket_id = $2",
            &[record_id.into(), scope_id.into()],
        )
        .unwrap()
        .expect("current class 2 materialization state");
        for key in [
            "relation_id",
            "record_id",
            "row_version",
            "deleted",
            "source_stream_generation",
            "source_commit_lsn",
            "source_event_ordinal",
            "source_reset_id",
            "edge_table",
            "edge_bucket",
            "edge_row_version",
            "membership_generation",
            "retention_generation",
        ] {
            assert_eq!(current_state.0[key], prior_state.0[key], "changed {key}");
        }
        assert_eq!(current_state.0["registry_generation"], target_generation);
        assert_ne!(
            current_state.0["row_checksum"], prior_state.0["row_checksum"],
            "the child schema hash must change the row digest"
        );
        assert_eq!(
            current_state.0["row_checksum"], current_state.0["edge_checksum"],
            "current row and edge digests must match"
        );

        let optional_field_id: String = Spi::get_one(
            "SELECT field.field_id::text
             FROM sync_registry_fields field
             JOIN sync_registry registry
               ON registry.registry_generation = field.registry_generation
              AND registry.relation_id = field.relation_id
             JOIN sync_registry_generations generation
               ON generation.generation = registry.registry_generation
             WHERE generation.state = 'active'
               AND registry.table_name = 'test_orders'
               AND field.physical_column = 'optional_class_2_note'",
        )
        .unwrap()
        .expect("optional class 2 field identity");
        assert!(current_state.0["row_data"][&optional_field_id].is_null());
        let current_projection: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                     'registry_generation', projection.registry_generation,
                     'checksum', encode(projection.checksum, 'hex'),
                     'row_data', projection.row_data
                 )
              FROM sync_captured_projections projection
              WHERE projection.record_id = $1 AND projection.image_kind = 'after'",
            &[record_id.into()],
        )
        .unwrap()
        .expect("current class 2 captured projection");
        assert_eq!(
            current_projection.0["registry_generation"],
            target_generation
        );
        assert_ne!(
            current_projection.0["checksum"],
            prior_projection.0["checksum"],
            "historical projection digest must use the child schema"
        );
        assert_eq!(
            current_projection.0["checksum"],
            current_state.0["row_checksum"]
        );
        assert!(current_projection.0["row_data"][&optional_field_id].is_null());

        let (current_identity, row_digest, scope_digest) = Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let table = registry
                .iter()
                .find(|table| table.table_name == "test_orders")
                .expect("current class 2 table");
            let row = client
                .select(
                    "SELECT row_data, row_version::text AS row_version
                     FROM sync_captured_rows
                     WHERE relation_id = $1::uuid AND record_id = $2",
                    None,
                    &[table.relation_id.as_str().into(), record_id.into()],
                )?
                .first();
            let row_data = row
                .get_by_name::<pgrx::JsonB, &str>("row_data")?
                .expect("current class 2 row data");
            let row_version = row
                .get_by_name::<String, &str>("row_version")?
                .expect("current class 2 row version");
            let identity = crate::pull::typed_primary_key_bytes(table, record_id)
                .expect("current class 2 row identity");
            let row_digest = crate::pull::synced_row_digest(
                client,
                table,
                &row_data.0,
                record_id,
                &row_version,
            )
            .expect("current class 2 production row digest");
            let row_identity = synchro_core::checksum::RowIdentity::from_bytes(identity.clone())
                .expect("current class 2 scope row identity");
            let schema_hash = synchro_core::checksum::SchemaHash::from_lower_hex(&schema_hash)
                .expect("current class 2 schema hash");
            let scope_digest = synchro_core::checksum::scope_digest(
                schema_hash,
                scope_id,
                &[synchro_core::checksum::ScopeDigestEntry::new(
                    row_identity,
                    row_digest,
                )],
            )
            .expect("current class 2 production scope digest");
            Ok::<_, spi::Error>((identity, row_digest, scope_digest))
        })
        .unwrap();
        assert_eq!(current_identity, prior_identity);
        assert_eq!(
            current_state.0["row_checksum"],
            row_digest.to_lower_hex(),
            "stored row digest must use the current production schema binding"
        );

        let terminal = Spi::connect_mut(|client| {
            Ok::<_, spi::Error>(
                crate::pull::compute_bucket_checksums(client, &[scope_id.to_string()])
                    .expect("current class 2 terminal scope digest"),
            )
        })
        .unwrap();
        assert_eq!(
            terminal[scope_id].digest(),
            scope_digest,
            "terminal scope inputs must contain the migrated row digest"
        );

        let pull = pull_client(
            user_id,
            client_id,
            1,
            json!({ (scope_id): { "cursor": null } }),
            100,
        );
        assert_eq!(pull["has_more"], false, "{pull}");
        assert_eq!(
            pull["checksums"][scope_id]["digest"],
            scope_digest.to_lower_hex(),
            "terminal pull must publish the current scope digest"
        );

        let rebuild = rebuild_client(user_id, client_id, scope_id, None, 100);
        assert!(rebuild.get("error").is_none(), "{rebuild}");
        assert_eq!(rebuild["records"].as_array().unwrap().len(), 1, "{rebuild}");
        assert_eq!(
            rebuild["records"][0]["row_checksum"]["digest"],
            row_digest.to_lower_hex()
        );
        assert_eq!(rebuild["checksum"]["digest"], scope_digest.to_lower_hex());
        assert!(rebuild["records"][0]["row"][optional_field_id].is_null());
    }

    #[pg_test]
    fn test_class_4_field_removal_keeps_rebuild_valid() {
        setup_test_tables();
        let user_id = "class-4-digest-user";
        let client_id = "class-4-digest-client";
        let scope_id = "user:class-4-digest-user";
        let record_id = "c4000000-0000-4000-8000-000000000001";
        register_client(user_id, client_id);
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, $2, 'retired class 4 field')",
            &[record_id.into(), user_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, scope_id);
        insert_changelog(scope_id, "test_orders", record_id, 1);

        let retired_field_id: String = Spi::get_one(
            "SELECT field.field_id::text
             FROM sync_registry_fields field
             JOIN sync_registry_generations generation
               ON generation.generation = field.registry_generation
             JOIN sync_registry registry
               ON registry.registry_generation = field.registry_generation
              AND registry.relation_id = field.relation_id
             WHERE generation.state = 'active'
               AND registry.table_name = 'test_orders'
               AND field.physical_column = 'title'",
        )
        .unwrap()
        .expect("retired class 4 field identity");

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 p_table_name := 'test_orders',
                 p_bucket_sql := $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                 p_composition := 'single_scope',
                 p_pk_column := 'id',
                 p_updated_at_col := 'updated_at',
                 p_deleted_at_col := 'deleted_at',
                 p_push_policy := 'enabled',
                 p_sync_columns := ARRAY[
                     'id', 'user_id', 'amount', 'created_at', 'updated_at', 'deleted_at'
                 ]
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();

        let (schema_version, schema_hash) = latest_schema_ref();
        let transition: String = Spi::get_one_with_args(
            "SELECT transition_class
             FROM sync_schema_manifest
             WHERE schema_version = $1 AND schema_hash = $2",
            &[schema_version.into(), schema_hash.as_str().into()],
        )
        .unwrap()
        .expect("class 4 field removal transition");
        assert_eq!(transition, "class_4");

        let migrated: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'row_data', captured.row_data,
                 'row_checksum', encode(captured.checksum, 'hex'),
                 'edge_checksum', encode(edge.checksum, 'hex'),
                 'current_generation', generation.generation,
                 'row_generation', captured.registry_generation
             )
             FROM sync_captured_rows captured
             JOIN sync_bucket_edges edge
               ON edge.relation_id = captured.relation_id
              AND edge.record_id = captured.record_id
             CROSS JOIN LATERAL (
                 SELECT generation
                 FROM sync_registry_generations
                 WHERE state = 'active'
             ) generation
             WHERE captured.record_id = $1 AND edge.bucket_id = $2",
            &[record_id.into(), scope_id.into()],
        )
        .unwrap()
        .expect("migrated class 4 projection");
        assert!(migrated.0["row_data"].get(&retired_field_id).is_none());
        assert_eq!(migrated.0["row_checksum"], migrated.0["edge_checksum"]);
        assert_eq!(migrated.0["row_generation"], migrated.0["current_generation"]);

        let rebuilt = rebuild_client(user_id, client_id, scope_id, None, 100);
        assert!(rebuilt.get("error").is_none(), "{rebuilt}");
        assert_eq!(rebuilt["records"].as_array().map(Vec::len), Some(1));
        assert!(rebuilt["records"][0]["row"]
            .get(&retired_field_id)
            .is_none());
    }

    #[pg_test]
    fn test_class_4_table_removal_retires_live_projection() {
        setup_test_tables();
        let user_id = "class-4-table-user";
        let client_id = "class-4-table-client";
        let scope_id = "user:class-4-table-user";
        let record_id = "c4000000-0000-4000-8000-000000000002";
        let initial = register_client(user_id, client_id);
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, $2, 'retired class 4 table')",
            &[record_id.into(), user_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, scope_id);
        insert_changelog(scope_id, "test_orders", record_id, 1);

        Spi::run("SELECT synchro_unregister_table('test_orders')").unwrap();
        activate_pending_registry_for_test();

        let live_state: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT jsonb_build_object(
                 'rows', (SELECT count(*) FROM sync_captured_rows WHERE record_id = $1),
                 'edges', (SELECT count(*) FROM sync_bucket_edges WHERE record_id = $1)
             )",
            &[record_id.into()],
        )
        .unwrap()
        .expect("retired class 4 live projection state");
        assert_eq!(live_state.0["rows"], 0);
        assert_eq!(live_state.0["edges"], 0);

        let reset = connect_client(
            user_id,
            json!({
                "client_id": client_id,
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema_reset": true,
                "schema": {
                    "version": initial["schema"]["version"],
                    "hash": initial["schema"]["hash"]
                },
                "scope_set_version": 1,
                "known_scopes": { (scope_id): { "cursor": null } }
            }),
        );
        assert_eq!(reset["schema"]["action"], "rebuild_local");
        let rebuilt = rebuild_client(user_id, client_id, scope_id, None, 100);
        assert!(rebuilt.get("error").is_none(), "{rebuilt}");
        assert!(rebuilt["records"].as_array().is_some_and(Vec::is_empty));
    }

    #[pg_test]
    fn test_class_4_live_type_change_requires_bootstrap() {
        setup_test_tables();
        let record_id = "c4000000-0000-4000-8000-000000000003";
        register_client("class-4-type-user", "class-4-type-client");
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'class-4-type-user', '42')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, "user:class-4-type-user");
        insert_changelog("user:class-4-type-user", "test_orders", record_id, 1);
        Spi::run(
            "ALTER TABLE test_orders ALTER COLUMN title DROP DEFAULT;
             ALTER TABLE test_orders ALTER COLUMN title TYPE bigint USING title::bigint",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_orders',
                 $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                 'single_scope', 'id', 'updated_at', 'deleted_at', 'enabled',
                 ARRAY['internal_notes']
             )",
        )
        .unwrap();

        let result = std::panic::catch_unwind(activate_pending_registry_for_test);
        assert!(result.is_err(), "live Class 4 type change must require bootstrap");
    }

    #[pg_test]
    fn test_class_4_historical_type_change_requires_bootstrap() {
        setup_test_tables();
        let record_id = "c4000000-0000-4000-8000-000000000005";
        register_client("class-4-history-user", "class-4-history-client");
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'class-4-history-user', '42')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_orders", record_id, "user:class-4-history-user");
        insert_changelog("user:class-4-history-user", "test_orders", record_id, 1);
        Spi::run("SET LOCAL session_replication_role = replica").unwrap();
        Spi::run_with_args(
            "DELETE FROM test_orders WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap();
        Spi::run("SET LOCAL session_replication_role = origin").unwrap();
        Spi::run_with_args(
            "DELETE FROM sync_bucket_edges WHERE record_id = $1",
            &[record_id.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "DELETE FROM sync_captured_rows WHERE record_id = $1",
            &[record_id.into()],
        )
        .unwrap();
        let historical_count: i64 = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_captured_projections WHERE record_id = $1",
            &[record_id.into()],
        )
        .unwrap()
        .expect("historical Class 4 projection count");
        assert!(historical_count > 0);

        Spi::run(
            "ALTER TABLE test_orders ALTER COLUMN title DROP DEFAULT;
             ALTER TABLE test_orders ALTER COLUMN title TYPE bigint USING title::bigint",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_orders',
                 $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                 'single_scope', 'id', 'updated_at', 'deleted_at', 'enabled',
                 ARRAY['internal_notes']
             )",
        )
        .unwrap();

        let result = std::panic::catch_unwind(activate_pending_registry_for_test);
        assert!(
            result.is_err(),
            "historical Class 4 type change must require bootstrap"
        );
    }

    #[pg_test]
    fn test_class_4_live_lifecycle_change_is_rejected() {
        setup_test_tables();
        let record_id = "c4000000-0000-4000-8000-000000000004";
        register_client("class-4-lifecycle-user", "class-4-lifecycle-client");
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ($1::uuid, 'class-4-lifecycle-user', 'retained')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge(
            "test_orders",
            record_id,
            "user:class-4-lifecycle-user",
        );
        insert_changelog(
            "user:class-4-lifecycle-user",
            "test_orders",
            record_id,
            1,
        );
        Spi::run("ALTER TABLE test_orders DROP COLUMN deleted_at").unwrap();
        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "SELECT tests.register_legacy_test_table(
                     'test_orders',
                     $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                     'single_scope', 'id', 'updated_at', '', 'enabled',
                     ARRAY['internal_notes']
                 )",
            )
            .unwrap();
        });
        assert!(
            result.is_err(),
            "live Class 4 lifecycle change must be rejected"
        );
    }

    #[pg_test]
    fn test_added_nonempty_table_is_class_3_with_bootstrap() {
        setup_test_tables();
        register_client("nonempty-table-user", "nonempty-table-client");
        Spi::run(
            "CREATE TABLE test_nonempty_added_manifest (
                 id UUID PRIMARY KEY,
                 value TEXT NOT NULL
             );
             INSERT INTO test_nonempty_added_manifest (id, value)
             VALUES ('a1000000-0000-4000-8000-000000000001', 'existing')",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 'test_nonempty_added_manifest',
                 $$SELECT ARRAY['global'] FROM test_nonempty_added_manifest WHERE id = $1::uuid$$,
                 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )",
        )
        .unwrap();
        let generation: i64 = Spi::get_one(
            "SELECT generation
             FROM sync_registry_generations
             WHERE state = 'pending' AND validated
             ORDER BY generation DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("nonempty added-table generation");
        let requires_bootstrap = Spi::connect(|client| {
            crate::schema::generation_requires_projection_bootstrap(client, generation)
        })
        .unwrap();
        let pending_body = Spi::connect(|client| {
            let pending = crate::schema::prepare_pending_manifest(client, generation)
                .expect("prepare nonempty added-table manifest")
                .expect("nonempty added-table pending manifest");
            Ok::<_, spi::Error>(
                serde_json::from_str::<Value>(&pending.canonical_body)
                    .expect("decode nonempty added-table pending manifest"),
            )
        })
        .unwrap();
        let mut client_scopes: Vec<String> = Spi::get_one(
            "SELECT bucket_subs
             FROM sync_clients
             WHERE user_id = 'nonempty-table-user'
               AND client_id = 'nonempty-table-client'",
        )
        .unwrap()
        .expect("nonempty added-table client scopes");
        client_scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        activate_pending_registry_for_test();

        let transition: pgrx::JsonB = Spi::get_one(
            "SELECT jsonb_build_object(
                 'metadata_class', transition_class,
                 'body_class', canonical_manifest_body::jsonb ->> 'transition_class',
                 'affected_scopes', affected_scopes,
                 'compatibility_floor', compatibility_floor,
                 'schema_version', schema_version
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap()
        .expect("nonempty added-table manifest transition");

        assert!(requires_bootstrap);
        assert_eq!(pending_body["transition_class"], "class_3");
        assert_eq!(transition.0["metadata_class"], "class_3");
        assert_eq!(transition.0["body_class"], "class_3");
        assert_eq!(transition.0["affected_scopes"], json!(client_scopes));
        assert_eq!(
            transition.0["compatibility_floor"],
            transition.0["schema_version"]
        );
    }

    #[pg_test]
    fn test_publication_catalog_detects_for_all_tables() {
        Spi::run("CREATE PUBLICATION test_for_all_publication FOR ALL TABLES").unwrap();
        let is_for_all = Spi::connect(|client| {
            crate::registry::publication_is_for_all_tables(client, "test_for_all_publication")
        })
        .unwrap();
        assert!(is_for_all);
    }

    #[pg_test]
    fn test_register_table_rejects_conflicting_column_selection() {
        setup_test_tables();

        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "SELECT tests.register_legacy_test_table(
                    'test_orders',
                    $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                    'single_scope',
                    'id',
                    'updated_at',
                    'deleted_at',
                    'enabled',
                    ARRAY['internal_notes'],
                    ARRAY['id', 'user_id', 'title', 'updated_at', 'deleted_at']
                )",
            )
            .unwrap();
        });

        assert!(
            result.is_err(),
            "registration should reject simultaneous sync_columns and exclude_columns"
        );
    }

    #[pg_test]
    fn test_unregister_table_for_all_publication() {
        setup_test_tables();
        Spi::run("SELECT synchro_unregister_table('test_orders')").unwrap();

        let in_registry: Option<bool> = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1
                FROM sync_registry r
                JOIN sync_registry_generations g ON g.generation = r.registry_generation
                WHERE g.state = 'pending' AND r.table_name = 'test_orders'
            )",
        )
        .unwrap();
        assert_eq!(in_registry, Some(false));

        let still_active: Option<bool> = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1
                FROM sync_registry r
                JOIN sync_registry_generations g ON g.generation = r.registry_generation
                WHERE g.state = 'active' AND r.table_name = 'test_orders'
            )",
        )
        .unwrap();
        assert_eq!(still_active, Some(true));
    }

    #[pg_test]
    fn test_unregister_stages_immutable_generation() {
        setup_test_tables();
        let record_id = "17171717-1717-1717-1717-171717171717";
        Spi::run_with_args(
            "INSERT INTO test_bare_items (id, name) VALUES ($1::uuid, 'edge test row')",
            &[record_id.into()],
        )
        .unwrap();
        insert_edge("test_bare_items", record_id, "global");

        Spi::run("SELECT synchro_unregister_table('test_bare_items')").unwrap();

        let reg_count: Option<i64> = Spi::get_one(
            "SELECT count(*)
             FROM sync_registry r
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'pending' AND r.table_name = 'test_bare_items'",
        )
        .unwrap();
        assert_eq!(reg_count, Some(0));

        let edge_count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_bucket_edges WHERE table_name = 'test_bare_items'",
        )
        .unwrap();
        assert_eq!(edge_count, Some(1));
    }

    #[pg_test]
    fn test_unregister_removes_capture_and_activates_complete_schema() {
        setup_test_tables();
        Spi::run("SELECT synchro_unregister_table('test_orders')").unwrap();

        let controls: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT jsonb_build_object(
                 'orders_publication', EXISTS (
                     SELECT 1 FROM pg_publication_tables
                     WHERE pubname = 'synchro_pub' AND tablename = 'test_orders'
                 ),
                 'orders_triggers', (
                     SELECT count(*) FROM pg_trigger
                     WHERE tgrelid = 'test_orders'::regclass
                       AND tgname IN ('synchro_primary_key_guard', 'synchro_capture_fence')
                 ),
                 'products_publication', EXISTS (
                     SELECT 1 FROM pg_publication_tables
                     WHERE pubname = 'synchro_pub' AND tablename = 'test_products'
                 ),
                 'products_triggers', (
                     SELECT count(*) FROM pg_trigger
                     WHERE tgrelid = 'test_products'::regclass
                       AND tgname IN ('synchro_primary_key_guard', 'synchro_capture_fence')
                 )
             )",
        )
        .unwrap();
        let controls = controls.unwrap().0;
        assert_eq!(controls["orders_publication"], true);
        assert_eq!(controls["orders_triggers"], 2);
        assert_eq!(controls["products_publication"], true);
        assert_eq!(controls["products_triggers"], 2);

        let active: i64 = Spi::get_one(
            "SELECT max(generation) FROM sync_registry_generations WHERE state = 'active'",
        )
        .unwrap()
        .expect("active generation");
        let pending: i64 = Spi::get_one(
            "SELECT max(generation) FROM sync_registry_generations
             WHERE state = 'pending' AND validated",
        )
        .unwrap()
        .expect("pending generation");
        let prior = Spi::connect(|client| {
            crate::registry::load_registry_generation_for_activation(client, active, pending)
        })
        .unwrap();
        assert!(prior.iter().any(|entry| entry.table_name == "test_orders"));
        assert!(prior.iter().any(|entry| entry.table_name == "test_products"));

        Spi::run(
            "INSERT INTO test_orders (user_id, title)
             VALUES ('after-unregister', 'captured before activation')",
        )
        .unwrap();
        let pending_fences: i64 = Spi::get_one(
            "SELECT count(*)
             FROM sync_write_fences fence
             JOIN sync_registry registry
               ON registry.registry_generation = (
                      SELECT generation FROM sync_registry_generations WHERE state = 'active'
                  )
              AND registry.relation_id = fence.relation_id
             WHERE registry.table_name = 'test_orders'",
        )
        .unwrap()
        .expect("pending unregister fence count");
        assert_eq!(pending_fences, 1);
        activate_pending_registry_for_test();

        let retired_controls: pgrx::JsonB = Spi::get_one(
            "SELECT jsonb_build_object(
                 'publication', EXISTS (
                     SELECT 1 FROM pg_publication_tables
                     WHERE pubname = 'synchro_pub' AND tablename = 'test_orders'
                 ),
                 'triggers', (
                     SELECT count(*) FROM pg_trigger
                     WHERE tgrelid = 'test_orders'::regclass
                       AND tgname IN ('synchro_primary_key_guard', 'synchro_capture_fence')
                 )
             )",
        )
        .unwrap()
        .expect("retired capture controls");
        assert_eq!(retired_controls.0["publication"], false);
        assert_eq!(retired_controls.0["triggers"], 0);

        let active_registry = crate::registry::load_registry().unwrap();
        assert!(!active_registry
            .iter()
            .any(|entry| entry.table_name == "test_orders"));
        assert!(active_registry
            .iter()
            .any(|entry| entry.table_name == "test_products"));
        let manifest: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT canonical_manifest_body::jsonb
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap();
        let manifest = manifest.unwrap();
        let tables = manifest.0["tables"].as_array().unwrap();
        assert!(!tables.iter().any(|table| table["name"] == "test_orders"));
        assert!(tables.iter().any(|table| table["name"] == "test_products"));
    }

    #[pg_test]
    fn test_register_client_bucket_subs() {
        setup_test_tables();
        let resp = register_client("user1", "client1");
        let server_time = resp.get("server_time").and_then(|v| v.as_str()).unwrap();
        let parsed = chrono::DateTime::parse_from_rfc3339(server_time).unwrap();
        assert_eq!(
            server_time,
            parsed.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
        );
        assert!(
            resp["schema"]
                .get("version")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                > 0
        );
        let added_scopes = resp["scopes"]["add"].as_array().unwrap();
        assert!(added_scopes
            .iter()
            .any(|scope| scope["id"].as_str() == Some("user:user1")));

        // Verify bucket_subs in sync_clients.
        let subs: Option<Vec<String>> = Spi::get_one_with_args(
            "SELECT bucket_subs FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &["user1".into(), "client1".into()],
        )
        .unwrap();
        let subs = subs.unwrap();
        assert!(subs.contains(&"user:user1".to_string()));
        assert_eq!(subs.len(), 1);
    }

    #[pg_test]
    fn test_connect_includes_registered_shared_scopes() {
        setup_test_tables();
        register_shared_scope("catalog", true);

        let first = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": {
                    "version": 0,
                    "hash": ""
                },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(first["scope_set_version"].as_i64(), Some(1));
        let first_added_scopes = first["scopes"]["add"].as_array().unwrap();
        let first_added_ids: Vec<&str> = first_added_scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();
        assert!(first_added_ids.contains(&"user:user1"));
        assert!(first_added_ids.contains(&"catalog"));

        register_shared_scope("runtime-only", false);

        let (schema_version, schema_hash) = latest_schema_ref();
        let second = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "client_generation": 1,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 1),
                    "catalog": scope_cursor_ref("user1", "client1", "catalog", 1)
                }
            }),
        );

        assert_eq!(second["scope_set_version"].as_i64(), Some(2));
        let second_added_scopes = second["scopes"]["add"].as_array().unwrap();
        let second_added_ids: Vec<&str> = second_added_scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();
        assert!(second_added_ids.contains(&"runtime-only"));
    }

    #[pg_test]
    fn test_connect_rejects_empty_identity() {
        let response = connect_client("", json!({}));

        assert_eq!(response["error"]["code"].as_str(), Some("auth_required"));
        assert_eq!(response["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_connect_returns_replace_and_scope_adds_for_fresh_client() {
        setup_test_tables();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": {
                    "version": 0,
                    "hash": ""
                },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(resp["protocol_version"].as_u64(), Some(3));
        assert_eq!(resp["client_generation"].as_i64(), Some(1));
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
        assert_eq!(resp["schema"]["action"].as_str(), Some("replace"));
        assert!(resp.get("schema_definition").is_some());
        assert_eq!(resp["scope_cursor_updates"], json!({}));

        let added_scopes = resp["scopes"]["add"].as_array().unwrap();
        assert_eq!(added_scopes.len(), 1);
        let added_ids: Vec<&str> = added_scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();
        assert!(added_ids.contains(&"user:user1"));
        assert_eq!(resp["scopes"]["remove"].as_array().unwrap().len(), 0);
    }

    #[pg_test]
    fn test_connect_returns_none_when_schema_and_scopes_match() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 1)
                }
            }),
        );

        assert_eq!(resp["schema"]["action"].as_str(), Some("none"));
        assert!(resp.get("schema_definition").is_none());
        assert_eq!(resp["scopes"]["add"].as_array().unwrap().len(), 0);
        assert_eq!(resp["scopes"]["remove"].as_array().unwrap().len(), 0);
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
    }

    #[pg_test]
    fn test_connect_server_only_change_keeps_schema_none() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        Spi::run(
            "ALTER TABLE test_orders
             ADD CONSTRAINT test_orders_ship_address_len
             CHECK (char_length(coalesce(title, '')) >= 0)",
        )
        .unwrap();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 1)
                }
            }),
        );

        assert_eq!(resp["schema"]["action"].as_str(), Some("none"));
        assert!(resp.get("schema_definition").is_none());
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
    }

    #[pg_test]
    fn test_connect_rejects_client_invented_scope() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 1),
                    "client:invented": scope_cursor_ref("user1", "client1", "client:invented", 1)
                }
            }),
        );

        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
        let client_exists: Option<bool> = Spi::get_one_with_args(
            "SELECT EXISTS (
                 SELECT 1 FROM sync_clients WHERE user_id = $1 AND client_id = $2
             )",
            &["user1".into(), "client1".into()],
        )
        .unwrap();
        assert_eq!(client_exists, Some(false));
    }

    #[pg_test]
    fn test_connect_rejects_unsupported_protocol_version() {
        setup_test_tables();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 99,
                "schema": {
                    "version": 0,
                    "hash": ""
                },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(resp["error"]["code"].as_str(), Some("upgrade_required"));
        assert_eq!(resp["error"]["retryable"].as_bool(), Some(false));
        assert_eq!(resp["error"]["required_protocol_version"].as_u64(), Some(3));
        assert_eq!(
            resp["error"]["received_protocol_version"].as_u64(),
            Some(99)
        );
    }

    #[pg_test]
    fn test_connect_rejects_unknown_members_and_invalid_semver() {
        setup_test_tables();
        let unknown_member = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {},
                "extra": true
            }),
        );
        assert_eq!(unknown_member["error"]["code"], "invalid_request");

        let invalid_semver = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "v1.0.0",
                "protocol_version": 3,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );
        assert_eq!(invalid_semver["error"]["code"], "invalid_request");
    }

    #[pg_test]
    fn test_connect_rejects_fresh_sentinel_for_durable_identity() {
        setup_test_tables();
        register_client("user1", "client1");

        let response = register_client("user1", "client1");
        assert_eq!(
            response["error"]["code"].as_str(),
            Some("invalid_schema_reference")
        );
        assert_eq!(
            response["error"]["received_schema"],
            json!({
                "version": 0,
                "hash": ""
            })
        );
    }

    #[pg_test]
    fn test_connect_rejects_irreversibly_retired_identity() {
        setup_test_tables();
        let first = register_client("user1", "client1");
        let schema = first["schema"].clone();
        Spi::run(
            "INSERT INTO sync_client_retirements (user_id, client_id)
             VALUES ('user1', 'client1')",
        )
        .unwrap();

        let response = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": schema["version"], "hash": schema["hash"] },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": { "cursor": null } }
            }),
        );
        assert_eq!(response["error"]["code"], "client_retired");

        let mutation = std::panic::catch_unwind(|| {
            Spi::run(
                "DELETE FROM sync_client_retirements
                 WHERE user_id = 'user1' AND client_id = 'client1'",
            )
            .unwrap();
        });
        assert!(mutation.is_err());
    }

    #[pg_test]
    fn test_connect_renews_expired_generation_and_nulls_scopes() {
        setup_test_tables();
        let first = register_client("user1", "client1");
        let schema = first["schema"].clone();
        Spi::run(
            "UPDATE sync_clients
             SET generation_created_at = now() - interval '31 days',
                 generation_expires_at = now()
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap();

        let renewed = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": schema["version"], "hash": schema["hash"] },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": scope_cursor_ref("user1", "client1", "user:user1", 1) }
            }),
        );
        assert_eq!(renewed["client_generation"], 2);
        assert_eq!(renewed["schema"]["action"], "none");
        assert_eq!(renewed["scope_cursor_updates"]["user:user1"], Value::Null);

        let stale = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": schema["version"], "hash": schema["hash"] },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": { "cursor": null } }
            }),
        );
        assert_eq!(stale["error"]["code"], "invalid_request");
        let generation: Option<i64> = Spi::get_one(
            "SELECT client_generation FROM sync_clients
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap();
        assert_eq!(generation, Some(2));
    }

    #[pg_test]
    fn test_connect_dispatches_unknown_schema_and_explicit_reset() {
        setup_test_tables();
        let first = register_client("user1", "client1");
        let unknown_schema = json!({
            "version": 99,
            "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });
        let unsupported = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": unknown_schema,
                "scope_set_version": 1,
                "known_scopes": { "user:user1": { "cursor": null } }
            }),
        );
        assert_eq!(unsupported["schema"]["action"], "unsupported");
        assert_eq!(unsupported["schema"]["reason"], "unknown_schema_lineage");
        assert!(unsupported.get("schema_definition").is_none());
        assert!(unsupported.get("affected_scopes").is_none());

        let reset = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema_reset": true,
                "schema": {
                    "version": 99,
                    "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": { "cursor": null } }
            }),
        );
        assert_eq!(reset["schema"]["action"], "rebuild_local");
        assert_eq!(reset["affected_scopes"], json!(["user:user1"]));
        assert_eq!(reset["scope_cursor_updates"]["user:user1"], Value::Null);
        assert_eq!(
            reset["schema_definition"]["schema_hash"],
            first["schema"]["hash"]
        );
    }

    #[pg_test]
    fn test_connect_dispatches_stored_schema_lineage() {
        setup_test_tables();
        let initial = register_client("user1", "client1");
        let initial_schema = initial["schema"].clone();
        let initial_cursor = scope_cursor_ref("user1", "client1", "user:user1", 1);

        Spi::run("ALTER TABLE test_orders ADD COLUMN optional_note TEXT").unwrap();
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
        let class_2_schema = latest_schema_ref();
        let class_2 = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": {
                    "version": initial_schema["version"],
                    "hash": initial_schema["hash"]
                },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": initial_cursor }
            }),
        );
        assert_eq!(class_2["schema"]["action"], "replace");
        assert!(class_2["scope_cursor_updates"]["user:user1"]
            .as_str()
            .is_some());
        assert!(class_2.get("affected_scopes").is_none());
        let class_2_cursor = scope_cursor_ref("user1", "client1", "user:user1", 1);

        Spi::run(
            "ALTER TABLE test_orders
             ADD COLUMN required_note TEXT NOT NULL DEFAULT ''",
        )
        .unwrap();
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
        let class_3_schema = latest_schema_ref();
        let class_3 = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": class_2_schema.0, "hash": class_2_schema.1 },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": class_2_cursor }
            }),
        );
        assert_eq!(class_3["schema"]["action"], "rebuild_local");
        assert_eq!(class_3["affected_scopes"], json!(["user:user1"]));
        assert_eq!(class_3["scope_cursor_updates"]["user:user1"], Value::Null);

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                 p_table_name := 'test_orders',
                 p_bucket_sql := $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                 p_composition := 'single_scope',
                 p_pk_column := 'id',
                 p_updated_at_col := 'updated_at',
                 p_deleted_at_col := 'deleted_at',
                 p_push_policy := 'enabled',
                 p_sync_columns := ARRAY[
                     'id', 'user_id', 'title', 'amount', 'created_at',
                     'updated_at', 'deleted_at'
                 ]
             )",
        )
        .unwrap();
        activate_pending_registry_for_test();
        let class_4 = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": class_3_schema.0, "hash": class_3_schema.1 },
                "scope_set_version": 1,
                "known_scopes": { "user:user1": { "cursor": null } }
            }),
        );
        assert_eq!(class_4["schema"]["action"], "unsupported");
        assert_eq!(
            class_4["schema"]["reason"],
            "incompatible_schema_transition"
        );
        assert!(class_4.get("schema_definition").is_none());
    }

    #[pg_test]
    fn test_connect_rejects_forged_cursor_without_state_change() {
        setup_test_tables();
        let initial = register_client("user1", "client1");
        let initial_schema = initial["schema"].clone();
        let initial_cursor = scope_cursor_ref("user1", "client1", "user:user1", 0);

        Spi::run("ALTER TABLE test_orders ADD COLUMN optional_note TEXT").unwrap();
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

        let client_before: String = Spi::get_one(
            "SELECT to_jsonb(client)::text
             FROM sync_clients AS client
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap()
        .expect("client state before forged cursor");
        let history_before: String = Spi::get_one(
            "SELECT COALESCE(
                        jsonb_agg(to_jsonb(history) ORDER BY history.scope_set_version),
                        '[]'::jsonb
                    )::text
             FROM sync_client_scope_history AS history
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap()
        .expect("assignment history before forged cursor");
        let checkpoints_before: String = Spi::get_one(
            "SELECT COALESCE(
                        jsonb_agg(to_jsonb(checkpoint) ORDER BY checkpoint.bucket_id),
                        '[]'::jsonb
                    )::text
             FROM sync_client_checkpoints AS checkpoint
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap()
        .expect("checkpoints before forged cursor");

        let cursor = initial_cursor["cursor"]
            .as_str()
            .expect("historical cursor")
            .to_string();
        let response = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "client_generation": 1,
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": initial_schema,
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": { "cursor": format!("{cursor}forged") }
                }
            }),
        );

        assert_eq!(response["error"]["code"].as_str(), Some("invalid_request"));

        let client_after: String = Spi::get_one(
            "SELECT to_jsonb(client)::text
             FROM sync_clients AS client
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap()
        .expect("client state after forged cursor");
        let history_after: String = Spi::get_one(
            "SELECT COALESCE(
                        jsonb_agg(to_jsonb(history) ORDER BY history.scope_set_version),
                        '[]'::jsonb
                    )::text
             FROM sync_client_scope_history AS history
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap()
        .expect("assignment history after forged cursor");
        let checkpoints_after: String = Spi::get_one(
            "SELECT COALESCE(
                        jsonb_agg(to_jsonb(checkpoint) ORDER BY checkpoint.bucket_id),
                        '[]'::jsonb
                    )::text
             FROM sync_client_checkpoints AS checkpoint
             WHERE user_id = 'user1' AND client_id = 'client1'",
        )
        .unwrap()
        .expect("checkpoints after forged cursor");

        assert_eq!(client_after, client_before);
        assert_eq!(history_after, history_before);
        assert_eq!(checkpoints_after, checkpoints_before);
    }

    #[pg_test]
    fn test_connect_serializes_with_pull_generation_renewal() {
        run_connect_generation_renewal_race(false);
    }

    #[pg_test]
    fn test_connect_serializes_with_rebuild_generation_renewal() {
        run_connect_generation_renewal_race(true);
    }

    fn sql_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn dblink_exec(connection: &str, query: &str) -> String {
        Spi::get_one_with_args(
            "SELECT public.dblink_exec($1, $2)",
            &[connection.into(), query.into()],
        )
        .unwrap()
        .expect("dblink command result")
    }

    fn dblink_query(connection: &str, query: &str) -> String {
        Spi::get_one_with_args(
            "SELECT result
             FROM public.dblink($1, $2) AS result_row(result text)",
            &[connection.into(), query.into()],
        )
        .unwrap()
        .expect("dblink query result")
    }

    fn dblink_get_result(connection: &str) -> String {
        Spi::get_one_with_args(
            "SELECT result
             FROM public.dblink_get_result($1) AS result_row(result text)",
            &[connection.into()],
        )
        .unwrap()
        .expect("dblink asynchronous query result")
    }

    fn run_connect_generation_renewal_race(rebuild: bool) {
        let user_id = if rebuild {
            "concurrent-rebuild-user"
        } else {
            "concurrent-pull-user"
        };
        let client_id = "concurrent-client";
        let rebuild_id = if rebuild {
            "b0000000-0000-4000-8000-000000000004"
        } else {
            "b0000000-0000-4000-8000-000000000005"
        };
        Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("install dblink extension");
        let connection_string: String = Spi::get_one(
            "SELECT format(
                        'host=%L port=%s dbname=%I user=%I',
                        current_setting('unix_socket_directories'),
                        current_setting('port'),
                        current_database(),
                        current_user
                    )",
        )
        .unwrap()
        .expect("dblink connection string");
        let driver_name = if rebuild {
            "synchro_rebuild_driver"
        } else {
            "synchro_pull_driver"
        };
        let contender_name = if rebuild {
            "synchro_rebuild_contender"
        } else {
            "synchro_pull_contender"
        };
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[driver_name.into(), connection_string.as_str().into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[contender_name.into(), connection_string.as_str().into()],
        )
        .unwrap();

        let user = sql_literal(user_id);
        let client = sql_literal(client_id);
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_rebuild_staged_rows
                 WHERE session_id IN (
                     SELECT session_id FROM sync_rebuild_sessions
                     WHERE user_id = {user} AND client_id = {client}
                 )"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_rebuild_sessions
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_client_scope_history
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_client_checkpoints
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_clients
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );

        let initial_request = serde_json::to_string(&json!({
            "client_id": client_id,
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "schema": { "version": 0, "hash": "" },
            "scope_set_version": 0,
            "known_scopes": {}
        }))
        .expect("encode initial connect request");
        let initial_text = dblink_query(
            driver_name,
            &format!(
                "SELECT synchro_connect({}, {}::jsonb)::text",
                sql_literal(user_id),
                sql_literal(&initial_request),
            ),
        );
        let initial: Value = serde_json::from_str(&initial_text).expect("decode initial connect");
        let schema = json!({
            "version": initial["schema"]["version"],
            "hash": initial["schema"]["hash"]
        });
        dblink_exec(
            driver_name,
            &format!(
                "UPDATE sync_clients
                 SET generation_created_at = now() - interval '31 days',
                     generation_expires_at = now()
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(driver_name, "BEGIN");
        let _ = dblink_query(
            driver_name,
            &format!(
                "SELECT pg_advisory_xact_lock(
                     hashtextextended(jsonb_build_array({user}::text, {client}::text)::text, 0)
                 )"
            ),
        );
        let connect_request = serde_json::to_string(&json!({
            "client_id": client_id,
            "client_generation": 1,
            "platform": "test",
            "app_version": "1.0.0",
            "protocol_version": 3,
            "schema": schema.clone(),
            "scope_set_version": 1,
            "known_scopes": {
                format!("user:{user_id}"): { "cursor": null }
            }
        }))
        .expect("encode renewal connect request");
        let renewal_text = dblink_query(
            driver_name,
            &format!(
                "SELECT synchro_connect({}, {}::jsonb)::text",
                sql_literal(user_id),
                sql_literal(&connect_request),
            ),
        );
        let renewal: Value = serde_json::from_str(&renewal_text).expect("decode renewal connect");
        assert_eq!(renewal["client_generation"].as_i64(), Some(2));

        let contender_request = if rebuild {
            json!({
                "client_id": client_id,
                "client_generation": 1,
                "schema": schema,
                "scope": format!("user:{user_id}"),
                "rebuild_id": rebuild_id,
                "cursor": null,
                "limit": 100
            })
        } else {
            json!({
                "client_id": client_id,
                "client_generation": 1,
                "schema": schema,
                "scope_set_version": 1,
                "scopes": {
                    format!("user:{user_id}"): { "cursor": null }
                },
                "limit": 100
            })
        };
        let contender_request = serde_json::to_string(&contender_request)
            .expect("encode concurrent contender request");
        let contender_pid: i32 = dblink_query(contender_name, "SELECT pg_backend_pid()")
            .parse()
            .expect("parse contender PID");
        let function = if rebuild {
            "synchro_rebuild"
        } else {
            "synchro_pull"
        };
        let contender_sql = format!(
            "SELECT {function}({}, {}::jsonb)::text",
            sql_literal(user_id),
            sql_literal(&contender_request),
        );
        let sent: i32 = Spi::get_one_with_args(
            "SELECT public.dblink_send_query($1, $2)",
            &[contender_name.into(), contender_sql.as_str().into()],
        )
        .unwrap()
        .expect("send blocked contender query");
        assert_eq!(sent, 1);
        let mut waiting = false;
        for _ in 0..1000 {
            let waiting_row: Option<bool> = Spi::get_one_with_args(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE pid = $1 AND locktype = 'advisory' AND NOT granted
                 )",
                &[i64::from(contender_pid).into()],
            )
            .unwrap();
            if waiting_row == Some(true) {
                waiting = true;
                break;
            }
        }
        dblink_exec(driver_name, "COMMIT");
        let contender_text = dblink_get_result(contender_name);

        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_rebuild_staged_rows
                 WHERE session_id IN (
                     SELECT session_id FROM sync_rebuild_sessions
                     WHERE user_id = {user} AND client_id = {client}
                 )"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_rebuild_sessions
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_client_scope_history
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_client_checkpoints
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        dblink_exec(
            driver_name,
            &format!(
                "DELETE FROM sync_clients
                 WHERE user_id = {user} AND client_id = {client}"
            ),
        );
        Spi::run_with_args(
            "SELECT public.dblink_disconnect($1)",
            &[driver_name.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT public.dblink_disconnect($1)",
            &[contender_name.into()],
        )
        .unwrap();
        assert!(waiting, "contender did not wait for the shared client lock");
        let contender: Value = serde_json::from_str(&contender_text).expect("decode contender");
        assert_eq!(
            contender["error"]["code"].as_str(),
            Some("client_generation_expired")
        );
        assert_eq!(
            contender["error"]["current_client_generation"].as_i64(),
            Some(2)
        );
    }

    #[pg_test]
    fn test_schema_returns_columns() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let resp = resp.unwrap().0;

        let tables = resp["manifest"]["tables"].as_array().unwrap();
        assert!(!tables.is_empty());
        let has_fields = tables.iter().any(|t| {
            t["fields"]
                .as_array()
                .map(|c| !c.is_empty())
                .unwrap_or(false)
        });
        assert!(has_fields, "schema should include field definitions");
    }

    #[pg_test]
    fn test_schema_manifest_validates_against_core_contract() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let resp = resp.unwrap().0;

        assert!(resp.get("schema_version").is_some());
        assert!(resp.get("schema_hash").is_some());

        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(resp["manifest"].clone()).unwrap();
        manifest.validate().unwrap();

        let orders = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_orders")
            .expect("test_orders should be present in schema manifest");
        assert!(!orders.table_id.is_empty());
        assert!(!orders.relation_id.is_empty());
        assert!(!orders.primary_key_field_id.is_empty());
        assert!(orders.lifecycle.updated_at_field_id.is_some());
        assert!(orders.lifecycle.deleted_at_field_id.is_some());

        let columns = &orders.fields;
        assert!(columns.iter().all(|column| !column.field_id.is_empty()));
        assert!(columns
            .iter()
            .any(|column| { column.name == "user_id" && column.type_name == "string" }));
        assert!(columns
            .iter()
            .any(|column| { column.name == "updated_at" && column.type_name == "datetime" }));
        assert!(
            columns.iter().all(|column| column.name != "internal_notes"),
            "exclude_columns must not leak into the portable manifest"
        );
        assert!(columns.iter().all(|column| {
            synchro_core::contract::is_canonical_portable_type_name(&column.type_name)
        }));
    }

    #[pg_test]
    fn test_contract_info_reports_canonical_versions() {
        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_contract_info()").unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(
            resp["sql_contract_version"].as_i64(),
            Some(crate::client::SQL_CONTRACT_VERSION as i64)
        );
        assert_eq!(
            resp["protocol_version"].as_i64(),
            Some(crate::client::PROTOCOL_VERSION as i64)
        );
        assert!(resp["extension_version"].as_str().is_some());
    }

    #[pg_test]
    fn test_schema_surfaces_use_sync_columns_as_canonical_shape() {
        setup_sync_columns_table();

        let manifest_resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest_resp = manifest_resp.unwrap().0;
        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(manifest_resp["manifest"].clone()).unwrap();
        manifest.validate().unwrap();

        let table = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_sync_columns_items")
            .expect("portable schema manifest should include test_sync_columns_items");
        let columns = &table.fields;
        assert!(columns.iter().any(|column| column.name == "title"));
        assert!(columns.iter().all(|column| column.name != "search_vector"));
        assert!(columns.iter().all(|column| column.name != "internal_notes"));

        let indexes = &table.indexes;
        assert!(
            indexes.is_empty(),
            "physical indexes are not implicit client-owned indexes"
        );
    }

    #[pg_test]
    fn test_schema_surfaces_omit_missing_timestamp_columns() {
        setup_test_tables();

        let manifest_resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest_resp = manifest_resp.unwrap().0;
        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(manifest_resp["manifest"].clone()).unwrap();
        let bare_items = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_bare_items")
            .expect("portable schema should include test_bare_items");
        assert!(bare_items.lifecycle.updated_at_field_id.is_none());
        assert!(bare_items.lifecycle.deleted_at_field_id.is_none());
    }

    #[pg_test]
    fn test_schema_manifest_emits_canonical_portable_types() {
        setup_portable_type_contract_table();

        let manifest_resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest_resp = manifest_resp.unwrap().0;
        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(manifest_resp["manifest"].clone()).unwrap();
        manifest.validate().unwrap();

        let table = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_portable_type_contract")
            .expect("portable schema manifest should include test_portable_type_contract");
        let columns = &table.fields;
        let types: std::collections::HashMap<_, _> = columns
            .iter()
            .map(|column| (column.name.as_str(), column.type_name.as_str()))
            .collect();

        assert_eq!(types.get("id"), Some(&"string"));
        assert_eq!(types.get("user_id"), Some(&"string"));
        assert_eq!(types.get("label"), Some(&"string"));
        assert_eq!(types.get("col_smallint"), Some(&"int"));
        assert_eq!(types.get("col_integer"), Some(&"int"));
        assert_eq!(types.get("col_bigint"), Some(&"int64"));
        assert_eq!(types.get("col_numeric"), Some(&"decimal"));
        assert_eq!(types.get("col_real"), Some(&"float"));
        assert_eq!(types.get("col_double"), Some(&"float"));
        assert_eq!(types.get("col_timestamp"), Some(&"datetime"));
        assert_eq!(types.get("col_interval"), Some(&"string"));
        assert_eq!(types.get("col_json"), Some(&"json"));
        assert_eq!(types.get("col_blob"), Some(&"bytes"));
        assert_eq!(types.get("col_text_array"), Some(&"json"));
        assert_eq!(types.get("col_int_array"), Some(&"json"));
        assert_eq!(types.get("col_inet"), Some(&"string"));
        assert_eq!(types.get("col_point"), Some(&"string"));
        assert_eq!(types.get("col_int4range"), Some(&"string"));
        assert_eq!(types.get("updated_at"), Some(&"datetime"));
        assert_eq!(types.get("deleted_at"), Some(&"datetime"));
        assert!(columns.iter().all(|column| {
            synchro_core::contract::is_canonical_portable_type_name(&column.type_name)
        }));
    }

    #[pg_test]
    fn test_schema_manifest_hash_ignores_bucket_sql_only_changes() {
        setup_test_tables();

        let before: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let before = before.unwrap().0;

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                'test_orders',
                $$SELECT ARRAY['alt:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                'single_scope',
                'id', 'updated_at', 'deleted_at', 'enabled',
                ARRAY['internal_notes']
            )",
        )
        .unwrap();

        let after: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let after = after.unwrap().0;

        assert_eq!(before["schema_version"], after["schema_version"]);
        assert_eq!(before["schema_hash"], after["schema_hash"]);
    }

    #[pg_test]
    fn test_debug_returns_state() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> =
            Spi::get_one_with_args("SELECT synchro_debug($1, $2)", &["u1".into(), "c1".into()])
                .unwrap();
        let resp = resp.unwrap().0;

        assert!(resp.get("client").is_some());
        assert!(resp.get("buckets").is_some());
        let server_time = resp.get("server_time").and_then(|v| v.as_str()).unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(server_time).is_ok());
    }

    #[pg_test]
    fn test_tables_returns_registry() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_tables()").unwrap();
        let resp = resp.unwrap().0;

        let tables = resp["tables"].as_array().unwrap();
        assert!(!tables.is_empty());
        let has_orders = tables
            .iter()
            .any(|t| t["table_name"].as_str() == Some("test_orders"));
        assert!(has_orders, "tables should include test_orders");
    }

    #[pg_test]
    fn test_connect_publishes_initial_empty_manifest() {
        let response = connect_client(
            "empty_user",
            json!({
                "client_id": "empty_client",
                "platform": "test",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(response["schema"]["action"], "replace");
        assert_eq!(response["schema_definition"]["tables"], json!([]));
        let count: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_schema_manifest").unwrap();
        assert_eq!(count, Some(1));
    }

    fn stage_orders_transition(drop_title: bool, add_headline: bool) -> i64 {
        if drop_title {
            Spi::run("ALTER TABLE test_orders DROP COLUMN title").unwrap();
        }
        if add_headline {
            Spi::run("ALTER TABLE test_orders ADD COLUMN headline TEXT").unwrap();
        }
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
        let staged: Option<i64> = Spi::get_one(
            "SELECT max(g.generation)
             FROM sync_registry_generations g
             JOIN sync_registry r ON r.registry_generation = g.generation
             WHERE g.state = 'pending' AND r.table_name = 'test_orders'",
        )
        .unwrap();
        staged.expect("staged transition generation")
    }

    fn staged_generation_requires_bootstrap(generation: i64) -> bool {
        Spi::connect(|client| {
            crate::schema::generation_requires_projection_bootstrap(client, generation)
        })
        .expect("classify staged generation")
    }

    // Issue #43: manifest publication refuses a class 4 reshape over retained
    // rows, so bootstrap preparation must accept the same generation and
    // produce the pending class 4 manifest the activation publishes.
    #[pg_test]
    fn test_class_4_reshape_over_rows_requires_projection_bootstrap() {
        setup_test_tables();
        Spi::run("SELECT synchro_schema_manifest()").unwrap();
        Spi::run("INSERT INTO test_orders (user_id, title) VALUES ('user-a', 'kept')").unwrap();
        let staged = stage_orders_transition(true, true);
        assert!(staged_generation_requires_bootstrap(staged));
        let published: Option<i64> =
            Spi::get_one("SELECT max(schema_version) FROM sync_schema_manifest").unwrap();
        let pending = Spi::connect(|client| {
            crate::schema::prepare_pending_manifest(client, staged)
                .map_err(pgrx::spi::Error::CursorNotFound)
        })
        .expect("prepare pending class 4 manifest")
        .expect("pending class 4 manifest is present");
        assert_eq!(pending.version, published.expect("published manifest") + 1);
    }

    #[pg_test]
    fn test_class_4_field_removal_over_rows_keeps_wal_activation() {
        setup_test_tables();
        Spi::run("SELECT synchro_schema_manifest()").unwrap();
        Spi::run("INSERT INTO test_orders (user_id, title) VALUES ('user-a', 'kept')").unwrap();
        let staged = stage_orders_transition(true, false);
        assert!(!staged_generation_requires_bootstrap(staged));
    }

    #[pg_test]
    fn test_class_4_reshape_over_empty_relation_keeps_wal_activation() {
        setup_test_tables();
        Spi::run("SELECT synchro_schema_manifest()").unwrap();
        let staged = stage_orders_transition(true, true);
        assert!(!staged_generation_requires_bootstrap(staged));
    }

    fn loaded_orders_registration_validates(active_generation: i64) -> Result<(), pgrx::spi::Error> {
        Spi::connect(|client| {
            let registration = crate::registry::active_registration_for_logical_name(
                client,
                active_generation,
                "test_orders",
            )?
            .expect("active orders registration");
            crate::registry::validate_loaded_registration(client, &registration)
        })
    }

    fn active_orders_generation() -> i64 {
        let active: Option<i64> = Spi::get_one(
            "SELECT max(g.generation)
             FROM sync_registry_generations g
             JOIN sync_registry r ON r.registry_generation = g.generation
             WHERE g.state = 'active' AND r.table_name = 'test_orders'",
        )
        .unwrap();
        active.expect("active orders generation")
    }

    // Issue #43: the staged class 4 window leaves the active registration
    // behind the live catalog by design, and the loader tolerates exactly
    // that window while activation waits on the operator bootstrap.
    #[pg_test]
    fn test_loaded_registration_tolerates_staged_reshape_window() {
        setup_test_tables();
        Spi::run("SELECT synchro_schema_manifest()").unwrap();
        Spi::run("INSERT INTO test_orders (user_id, title) VALUES ('user-a', 'kept')").unwrap();
        let active = active_orders_generation();
        stage_orders_transition(true, true);
        loaded_orders_registration_validates(active).expect("staged window validation");
    }

    #[pg_test(error = "registered synced column metadata has drifted")]
    fn test_loaded_registration_rejects_unstaged_catalog_drift() {
        setup_test_tables();
        let active = active_orders_generation();
        Spi::run("ALTER TABLE test_orders ADD COLUMN rogue TEXT").unwrap();
        loaded_orders_registration_validates(active).expect("unstaged drift must abort");
    }

    // Issue #43: membership activation clears affected rebuild state while
    // sessions are live, so the child immutability trigger accepts the same
    // staged-generation authorization as the session trigger.
    #[pg_test]
    fn test_membership_activation_clears_live_rebuild_state() {
        setup_test_tables();
        Spi::run("SELECT synchro_schema_manifest()").unwrap();
        Spi::run("INSERT INTO test_orders (user_id, title) VALUES ('user-a', 'kept')").unwrap();
        let staged = stage_orders_transition(true, true);
        connect_client(
            "user-a",
            json!({
                "client_id": "client-a",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );
        Spi::run(
            "INSERT INTO sync_rebuild_sessions (
                 user_id, client_id, rebuild_id, scope_id, client_generation,
                 schema_version, schema_hash, stream_generation,
                 membership_generation, retention_generation,
                 boundary_position_kind, accepted_write_epoch, page_limit,
                 snapshot_checksum, staged_row_count
             ) VALUES (
                 'user-a', 'client-a', '44444444-4444-4444-4444-444444444444',
                 'user:user-a', 1, 1, repeat('a', 64), 'sg-1', 1, 1,
                 'generation_start', 1, 10, decode(repeat('ab', 32), 'hex'), 0
             )",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO sync_rebuild_pages (session_id, next_row_ordinal, response)
             SELECT session_id, 0, '{}'::jsonb
             FROM sync_rebuild_sessions
             WHERE rebuild_id = '44444444-4444-4444-4444-444444444444'",
        )
        .unwrap();
        let active = active_orders_generation();
        Spi::run(&format!(
            "INSERT INTO sync_stream_resets (
                 reset_id, operation_kind, source_stream_generation,
                 target_stream_generation, source_registry_generation,
                 target_registry_generation, old_slot_name, candidate_slot_name,
                 database_oid, database_name, plugin, lifecycle,
                 consistent_point, exported_snapshot_name, activation_barrier,
                 baseline_staged_at, staged_row_count, staged_version_count,
                 staged_edge_count, staged_fence_count, staged_scope_count
             ) VALUES (
                 '55555555-5555-5555-5555-555555555555', 'projection_bootstrap',
                 'sg-1', 'sg-1', {active}, {staged}, 'old_slot', 'cand_slot',
                 1, current_database(), 'pgoutput', 'catching_up',
                 '0/10', 'snap_test', '0/20', now(), 0, 0, 0, 0, 0
             )"
        ))
        .unwrap();
        Spi::run(
            "SELECT set_config(
                 'synchro.stream_reset_id',
                 '55555555-5555-5555-5555-555555555555', true
             )",
        )
        .unwrap();
        Spi::connect_mut(|client| {
            crate::materialize::invalidate_affected_membership_generation(
                client,
                &["user:user-a".to_string()],
                staged,
            )
        })
        .expect("membership activation clears live rebuild state");
        let remaining: Option<i64> =
            Spi::get_one("SELECT count(*) FROM sync_rebuild_sessions").unwrap();
        assert_eq!(remaining, Some(0));
        let pages: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_rebuild_pages").unwrap();
        assert_eq!(pages, Some(0));
    }
