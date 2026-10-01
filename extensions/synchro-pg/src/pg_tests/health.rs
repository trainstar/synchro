    #[pg_test]
    fn dedicated_worker_login_validation_uses_deployment_role() {
        Spi::run(
            "DROP ROLE IF EXISTS synchro_test_worker_login;
             CREATE ROLE synchro_test_worker_login
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO synchro_test_worker_login",
        )
        .expect("provision test worker login");

        let validation = Spi::connect(|client| {
            crate::health::validate_worker_login(client, "synchro_test_worker_login")
        });

        Spi::run(
            "REVOKE synchro_worker FROM synchro_test_worker_login;
             DROP ROLE synchro_test_worker_login",
        )
        .expect("remove test worker login");

        assert!(validation.expect("validate test worker login").is_valid());
    }

    #[pg_test]
    fn default_gucs_are_healthy_and_fail_closed() {
        let slot = "synchro_slot";
        Spi::run(
            "DROP ROLE IF EXISTS synchro_health_worker;
             CREATE ROLE synchro_health_worker
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO synchro_health_worker;
             DROP PUBLICATION IF EXISTS synchro_pub",
        )
        .expect("provision health test identity");
        setup_test_tables();
        Spi::run_with_args(
            "WITH slot AS (
                 SELECT slot_name, confirmed_flush_lsn
                 FROM pg_catalog.pg_replication_slots
                 WHERE slot_name = $1
             ), runtime_update AS (
                 UPDATE synchro.sync_runtime_state runtime
                 SET active_slot_name = slot.slot_name, updated_at = now()
                 FROM slot
                 WHERE runtime.singleton
             )
             UPDATE synchro.sync_wal_progress progress
             SET generation_start_lsn = slot.confirmed_flush_lsn,
                 processed_end_lsn = slot.confirmed_flush_lsn,
                 materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 acknowledged_end_lsn = NULL,
                 updated_at = now()
             FROM slot
             WHERE progress.singleton",
            &[slot.into()],
        )
        .expect("set default health test slot");
        Spi::run(
            "INSERT INTO synchro.sync_wal_worker_state (
                 worker_id, database_oid, database_name, worker_login_oid,
                 backend_pid, state, registry_generation,
                 materialized_commit_lsn, materialized_end_lsn,
                 wal_observed_at, heartbeat_at, updated_at
              )
              SELECT 'synchro_wal_consumer', database.oid, database.datname,
                     worker_role.oid, pg_backend_pid(), 'running',
                     progress.registry_generation,
                     progress.materialized_commit_lsn, progress.materialized_end_lsn,
                     now(), now(), now()
              FROM pg_catalog.pg_database database
             CROSS JOIN pg_catalog.pg_roles worker_role
             CROSS JOIN synchro.sync_wal_progress progress
             WHERE database.datname = current_database()
               AND worker_role.rolname = 'synchro_health_worker'
               AND progress.singleton",
        )
        .expect("create complete health state");

        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load health test database")
            .expect("health test database");
        let mut configuration = crate::health::ReadinessConfiguration::configured();
        configuration.database = Some(database);
        configuration.worker_login = Some("synchro_health_worker".to_string());

        let publication: String = Spi::get_one("SHOW synchro.publication_name")
            .expect("show default publication GUC")
            .expect("default publication GUC");
        let replication_slot: String = Spi::get_one("SHOW synchro.replication_slot")
            .expect("show default replication slot GUC")
            .expect("default replication slot GUC");
        let limit_defaults_visible: bool = Spi::get_one(
            "SELECT current_setting('synchro.max_worker_heartbeat_age_seconds') = '30'
                    AND current_setting('synchro.max_wal_lag_bytes') = '67108864'
                    AND current_setting('synchro.max_wal_lag_seconds') = '30'",
        )
        .expect("load default health limit GUCs")
        .expect("default health limit comparison");
        let guc_defaults_visible = publication == "synchro_pub"
            && replication_slot == "synchro_slot"
            && limit_defaults_visible;
        let default_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        // This cluster has no replication slot, so it cannot reach a complete
        // ready state. Each check that a rejection below targets starts ok, so
        // each rejection is attributable to its own change. The real-server
        // readiness test owns the slot and materialization-progress checks.
        let targeted_checks_start_ok = ["capture_triggers", "publication", "heartbeat", "poison"]
            .into_iter()
            .all(|check| default_detail["checks"][check]["state"].as_str() == Some("ok"))
            && default_detail["checks"]["wal_byte_lag"]["state"].as_str() != Some("failed");

        Spi::run(
            "ALTER TABLE public.test_orders DISABLE TRIGGER synchro_capture_fence",
        )
        .expect("disable required health test trigger");
        let trigger_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let disabled_trigger_rejected = !trigger_detail["ready"].as_bool().unwrap_or(true)
            && trigger_detail["checks"]["capture_triggers"]["state"].as_str()
                == Some("failed");
        Spi::run("ALTER TABLE public.test_orders ENABLE TRIGGER synchro_capture_fence")
            .expect("restore required health test trigger");

        Spi::run(
            "CREATE TABLE public.synchro_health_unregistered (id UUID PRIMARY KEY);
             ALTER PUBLICATION synchro_pub ADD TABLE public.synchro_health_unregistered",
        )
        .expect("add unexpected publication relation");
        let publication_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let extra_publication_relation_rejected =
            !publication_detail["ready"].as_bool().unwrap_or(true)
                && publication_detail["checks"]["publication"]["state"].as_str()
                    == Some("failed");
        Spi::run(
            "ALTER PUBLICATION synchro_pub DROP TABLE public.synchro_health_unregistered;
             DROP TABLE public.synchro_health_unregistered",
        )
        .expect("remove unexpected publication relation");

        Spi::run(
            "UPDATE synchro.sync_wal_worker_state
             SET heartbeat_at = now() - interval '2 minutes'
             WHERE worker_id = 'synchro_wal_consumer'",
        )
        .expect("make health test heartbeat stale");
        let heartbeat_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let stale_heartbeat_rejected = !heartbeat_detail["ready"].as_bool().unwrap_or(true)
            && heartbeat_detail["checks"]["heartbeat"]["state"].as_str()
                == Some("failed");
        Spi::run(
            "UPDATE synchro.sync_wal_worker_state
             SET heartbeat_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
        )
        .expect("restore health test heartbeat");

        Spi::run(
            "UPDATE synchro.sync_wal_worker_state
             SET oldest_unmaterialized_commit_timestamp = now() - interval '2 minutes',
                 wal_observed_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
        )
        .expect("set oldest unmaterialized commit observation");
        let lag_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let oldest_commit_age_reported = lag_detail["observations"]["wal_lag_seconds"]
            .as_f64()
            .is_some_and(|age| age >= 120.0);
        Spi::run(
            "UPDATE synchro.sync_wal_worker_state
             SET oldest_unmaterialized_commit_timestamp = NULL,
                 wal_observed_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
        )
        .expect("clear oldest unmaterialized commit observation");

        Spi::run(
            "DO $control$
             BEGIN
                 INSERT INTO synchro.sync_wal_poison (
                     stream_generation, commit_lsn, failure_class, failure_detail
                 )
                 SELECT stream_generation, '0/1', 'unrecognized_class', 'bounded detail'
                 FROM synchro.sync_runtime_state WHERE singleton;
                 RAISE EXCEPTION 'unknown poison failure class was accepted';
             EXCEPTION WHEN check_violation THEN
                 NULL;
             END
             $control$",
        )
        .expect("reject an unrecognized durable poison failure class");
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM synchro.sync_wal_poison")
                .expect("read poison count after rejection"),
            Some(0)
        );

        Spi::run(
            "INSERT INTO synchro.sync_wal_poison (
                 stream_generation, commit_lsn, failure_class, failure_detail, lifecycle
              )
              SELECT stream_generation, '0/1', 'decode_failed', 'WAL decoder rejected a replication message', 'active'
             FROM synchro.sync_runtime_state WHERE singleton",
        )
        .expect("create blocking health test poison");
        let poison_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let poison_rejected = !poison_detail["ready"].as_bool().unwrap_or(true)
            && poison_detail["checks"]["poison"]["state"].as_str() == Some("failed")
            && poison_detail["observations"]["poison"]["failure_class"].as_str()
                == Some("decode_failed")
            && poison_detail["observations"]["poison"]["failure_detail"].as_str()
                == Some("WAL decoder rejected a replication message");
        Spi::run("DELETE FROM synchro.sync_wal_poison WHERE lifecycle = 'active'")
            .expect("remove blocking health test poison");

        let mut invalid_limit = configuration.clone();
        invalid_limit.max_wal_lag_bytes = 0;
        let invalid_limit_detail =
            crate::health::load_readiness_status_with_configuration(invalid_limit).detail();
        let invalid_limit_rejected = !invalid_limit_detail["ready"].as_bool().unwrap_or(true)
            && invalid_limit_detail["checks"]["wal_byte_lag"]["state"].as_str()
                == Some("failed")
            && invalid_limit_detail["checks"]["wal_byte_lag"]["reason"].as_str()
                == Some("invalid_limit");


        let bounded_detail = poison_detail.to_string().len() < 4096;

        Spi::run("DELETE FROM synchro.sync_wal_worker_state WHERE worker_id = 'synchro_wal_consumer'")
            .expect("remove health test worker state");
        Spi::run(
            "REVOKE synchro_worker FROM synchro_health_worker;
             DROP ROLE synchro_health_worker",
        )
        .expect("remove health test identity");

        assert!(guc_defaults_visible);
        assert!(
            targeted_checks_start_ok,
            "targeted health checks do not start ok: {default_detail}"
        );
        assert!(disabled_trigger_rejected);
        assert!(extra_publication_relation_rejected);
        assert!(stale_heartbeat_rejected);
        assert!(oldest_commit_age_reported);
        assert!(poison_rejected);
        assert!(invalid_limit_rejected);
        assert!(bounded_detail, "detailed health exposed unbounded or sensitive state");
    }

    #[pg_test]
    fn reset_health_uses_runtime_slot() {
        let slot = "synchro_reset_health_candidate";
        Spi::run(
            "DROP ROLE IF EXISTS synchro_reset_health_worker;
             CREATE ROLE synchro_reset_health_worker
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO synchro_reset_health_worker",
        )
        .expect("provision reset health worker");
        setup_test_tables();
        Spi::run_with_args(
             "UPDATE synchro.sync_runtime_state
               SET active_slot_name = $1, updated_at = now()
               WHERE singleton;
              INSERT INTO synchro.sync_wal_worker_state (
                  worker_id, database_oid, database_name, worker_login_oid,
                  backend_pid, state, registry_generation,
                  materialized_commit_lsn, materialized_end_lsn,
                  wal_observed_at, heartbeat_at, updated_at
              )
              SELECT 'synchro_wal_consumer', database.oid, database.datname,
                     worker_role.oid, pg_backend_pid(), 'running',
                     progress.registry_generation,
                     progress.materialized_commit_lsn, progress.materialized_end_lsn,
                     now(), now(), now()
              FROM pg_catalog.pg_database database
              CROSS JOIN pg_catalog.pg_roles worker_role
              CROSS JOIN synchro.sync_wal_progress progress
              WHERE database.datname = current_database()
                AND worker_role.rolname = 'synchro_reset_health_worker'
                AND progress.singleton",
            &[slot.into()],
        )
        .expect("install reset health state");
        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load reset health database")
            .expect("reset health database");
        let detail = crate::health::load_readiness_status_with_configuration(
            crate::health::ReadinessConfiguration {
                database: Some(database),
                publication: Some("synchro_pub".to_string()),
                worker_login: Some("synchro_reset_health_worker".to_string()),
                max_heartbeat_age_seconds: 30,
                max_wal_lag_bytes: i32::MAX,
                max_wal_lag_seconds: 30,
            },
        )
        .detail();
        let runtime_slot_is_observed = detail["observations"]["active_slot_name"] == slot;

        Spi::run(
            "DELETE FROM synchro.sync_wal_worker_state
              WHERE worker_id = 'synchro_wal_consumer';
              REVOKE synchro_worker FROM synchro_reset_health_worker;
              DROP ROLE synchro_reset_health_worker",
        )
        .expect("remove reset health state");

        assert_eq!(detail["ready"], false);
        assert_eq!(detail["checks"]["replication_slot"]["state"], "failed");
        assert!(runtime_slot_is_observed);
    }

    #[pg_test]
    fn prior_generation_poison_does_not_block_current() {
        let current_generation: String = Spi::get_one(
            "SELECT stream_generation FROM synchro.sync_runtime_state WHERE singleton",
        )
        .expect("load current poison generation")
        .expect("current poison generation");
        let prior_generation = format!("{current_generation}-prior");
        Spi::run(
            "CREATE ROLE synchro_poison_scope_worker
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO synchro_poison_scope_worker",
        )
        .expect("provision poison scope worker");
        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load poison scope database")
            .expect("poison scope database");
        let configuration = crate::health::ReadinessConfiguration {
            database: Some(database),
            publication: Some("synchro_pub".to_string()),
            worker_login: Some("synchro_poison_scope_worker".to_string()),
            max_heartbeat_age_seconds: 30,
            max_wal_lag_bytes: i32::MAX,
            max_wal_lag_seconds: 30,
        };
        Spi::run_with_args(
            "INSERT INTO synchro.sync_wal_poison (
                 stream_generation, commit_lsn, failure_class, failure_detail, lifecycle
             ) VALUES ($1, '0/1', 'validation_failed', 'WAL validation failed', 'active')",
            &[prior_generation.as_str().into()],
        )
        .expect("create prior generation poison");

        let prior_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        assert_eq!(prior_detail["checks"]["poison"]["state"], "ok");
        assert_eq!(
            crate::bgworker::active_poison_state(&current_generation)
                .expect("read current poison state"),
            (false, false)
        );
        let retired = Spi::connect_mut(|client| {
            crate::bgworker::retire_prior_generation_poison(client, &current_generation)
        })
        .expect("retire prior generation poison");
        assert_eq!(retired, 1);
        let prior_retired: Option<bool> = Spi::get_one_with_args(
            "SELECT lifecycle = 'reset' AND resolved_at IS NOT NULL
             FROM synchro.sync_wal_poison
             WHERE stream_generation = $1",
            &[prior_generation.as_str().into()],
        )
        .expect("read retired poison lifecycle");
        assert_eq!(prior_retired, Some(true));

        Spi::run_with_args(
            "INSERT INTO synchro.sync_wal_poison (
                 stream_generation, commit_lsn, failure_class, failure_detail, lifecycle
             ) VALUES ($1, '0/2', 'validation_failed', 'WAL validation failed', 'active')",
            &[current_generation.as_str().into()],
        )
        .expect("create current generation poison");
        let current_detail = crate::health::load_readiness_status_with_configuration(configuration).detail();
        assert_eq!(current_detail["checks"]["poison"]["state"], "failed");
        assert_eq!(
            crate::bgworker::active_poison_state(&current_generation)
                .expect("read current poison block"),
            (true, false)
        );
        Spi::run_with_args(
            "UPDATE synchro.sync_wal_poison
             SET lifecycle = 'reset', resolved_at = now()
             WHERE lifecycle = 'active' AND stream_generation = $1",
            &[current_generation.as_str().into()],
        )
        .expect("retire current poison test state");
        Spi::run(
            "REVOKE synchro_worker FROM synchro_poison_scope_worker;
             DROP ROLE synchro_poison_scope_worker",
        )
        .expect("remove poison scope worker");
    }

    #[pg_test]
    fn unpublished_generated_field_fails_publication_readiness() {
        Spi::run(
            "DROP ROLE IF EXISTS synchro_generated_health_worker;
             CREATE ROLE synchro_generated_health_worker
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO synchro_generated_health_worker;
             DROP PUBLICATION IF EXISTS synchro_pub",
        )
        .expect("provision generated column health test identity");
        setup_test_tables();
        set_test_publication_generated_columns("stored");
        create_generated_column_fixture();
        Spi::run(&format!(
            "SELECT {}",
            generated_fixture_registration("ARRAY['value_upper']")
        ))
        .expect("register published stored generated field");
        activate_pending_registry_for_test();

        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load generated column health test database")
            .expect("generated column health test database");
        let mut configuration = crate::health::ReadinessConfiguration::configured();
        configuration.database = Some(database);
        configuration.worker_login = Some("synchro_generated_health_worker".to_string());

        let published_detail =
            crate::health::load_readiness_status_with_configuration(configuration.clone())
                .detail();
        set_test_publication_generated_columns("none");
        let unpublished_detail =
            crate::health::load_readiness_status_with_configuration(configuration).detail();

        Spi::run(
            "REVOKE synchro_worker FROM synchro_generated_health_worker;
             DROP ROLE synchro_generated_health_worker",
        )
        .expect("remove generated column health test identity");

        assert_eq!(
            published_detail["checks"]["publication"]["state"].as_str(),
            Some("ok")
        );
        assert_eq!(
            unpublished_detail["checks"]["publication"]["state"].as_str(),
            Some("failed")
        );
        assert_eq!(
            unpublished_detail["checks"]["publication"]["reason"].as_str(),
            Some("publication_mismatch")
        );
    }

    fn filtered_publication_member_fails_publication_readiness(member: &str) {
        Spi::run(
            "DROP ROLE IF EXISTS synchro_filter_health_worker;
             CREATE ROLE synchro_filter_health_worker
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO synchro_filter_health_worker;
             DROP PUBLICATION IF EXISTS synchro_pub",
        )
        .expect("provision publication filter health test identity");
        setup_test_tables();

        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load publication filter health test database")
            .expect("publication filter health test database");
        let mut configuration = crate::health::ReadinessConfiguration::configured();
        configuration.database = Some(database);
        configuration.worker_login = Some("synchro_filter_health_worker".to_string());

        let exact_detail =
            crate::health::load_readiness_status_with_configuration(configuration.clone())
                .detail();
        replace_orders_publication_member(member);
        let filtered_detail =
            crate::health::load_readiness_status_with_configuration(configuration).detail();

        Spi::run(
            "REVOKE synchro_worker FROM synchro_filter_health_worker;
             DROP ROLE synchro_filter_health_worker",
        )
        .expect("remove publication filter health test identity");

        assert_eq!(
            exact_detail["checks"]["publication"]["state"].as_str(),
            Some("ok")
        );
        assert_eq!(
            filtered_detail["checks"]["publication"]["state"].as_str(),
            Some("failed")
        );
        assert_eq!(
            filtered_detail["checks"]["publication"]["reason"].as_str(),
            Some("publication_mismatch")
        );
    }

    #[pg_test]
    fn publication_member_column_list_fails_publication_readiness() {
        filtered_publication_member_fails_publication_readiness(ORDERS_COLUMN_LIST_MEMBER);
    }

    #[pg_test]
    fn publication_member_row_filter_fails_publication_readiness() {
        filtered_publication_member_fails_publication_readiness(ORDERS_ROW_FILTER_MEMBER);
    }

    #[pg_test]
    fn public_readiness_is_generic_and_fail_closed() {
        let readiness: pgrx::JsonB = Spi::get_one("SELECT synchro.synchro_readiness()")
            .expect("query public readiness")
            .expect("public readiness result");
        let object = readiness.0.as_object().expect("public readiness object");
        assert_eq!(object.len(), 1);
        assert_eq!(object.get("ready").and_then(Value::as_bool), Some(false));
    }

    #[pg_test]
    fn stale_build_fingerprint_fails_health() {
        let current: String = Spi::get_one(
            "SELECT installed_fingerprint FROM synchro.sync_extension_build WHERE singleton",
        )
        .expect("load installed fingerprint")
        .expect("installed fingerprint");
        let stale = "0".repeat(64);
        assert_ne!(current, stale);

        Spi::run_with_args(
            "UPDATE synchro.sync_extension_build
             SET installed_fingerprint = $1
             WHERE singleton",
            &[stale.as_str().into()],
        )
        .expect("record stale fingerprint");

        let detail: pgrx::JsonB = Spi::get_one("SELECT synchro.synchro_health_detail()")
            .expect("load health detail")
            .expect("health detail");
        let contract: pgrx::JsonB = Spi::get_one("SELECT synchro.synchro_contract_info()")
            .expect("load contract info")
            .expect("contract info");

        Spi::run_with_args(
            "UPDATE synchro.sync_extension_build
             SET installed_fingerprint = $1
             WHERE singleton",
            &[current.as_str().into()],
        )
        .expect("restore installed fingerprint");

        assert_eq!(
            detail.0["checks"]["extension_objects_stale"]["state"],
            "failed"
        );
        assert_eq!(
            detail.0["observations"]["extension_objects"]["library_fingerprint"],
            crate::build_fingerprint::library_fingerprint()
        );
        assert_eq!(
            detail.0["observations"]["extension_objects"]["installed_fingerprint"],
            stale
        );
        assert_eq!(contract.0["extension_objects_current"], false);
        assert_eq!(
            contract.0["library_build_fingerprint"],
            crate::build_fingerprint::library_fingerprint()
        );
        assert_eq!(contract.0["installed_build_fingerprint"], stale);
    }

    const POSITION_ORDER_WORKER: &str = "synchro_position_order_worker";
    const POSITION_ORDER_MATERIALIZED: Option<(&str, &str)> = Some(("0/2800", "0/3000"));

    fn position_order_check(check: &str) -> Value {
        let database: String = Spi::get_one("SELECT current_database()::text")
            .expect("load position order database")
            .expect("position order database");
        crate::health::load_readiness_status_with_configuration(
            crate::health::ReadinessConfiguration {
                database: Some(database),
                publication: Some("synchro_pub".to_string()),
                worker_login: Some(POSITION_ORDER_WORKER.to_string()),
                max_heartbeat_age_seconds: 30,
                max_wal_lag_bytes: i32::MAX,
                max_wal_lag_seconds: 30,
            },
        )
        .detail()["checks"][check]
            .clone()
    }

    fn position_order_generation() -> i64 {
        Spi::get_one("SELECT registry_generation FROM synchro.sync_wal_progress WHERE singleton")
            .expect("load position order registry generation")
            .expect("position order registry generation")
    }

    fn set_position_order_progress(
        registry_generation: i64,
        materialized: Option<(&str, &str)>,
        acknowledged: Option<&str>,
    ) {
        Spi::run_with_args(
            "UPDATE synchro.sync_wal_progress
             SET registry_generation = $1,
                 generation_start_lsn = '0/1000',
                 materialized_commit_lsn = $2::pg_lsn,
                 materialized_end_lsn = $3::pg_lsn,
                 acknowledged_end_lsn = $4::pg_lsn,
                 processed_end_lsn = '0/3000',
                 updated_at = now()
             WHERE singleton",
            &[
                registry_generation.into(),
                materialized.map(|(commit, _)| commit).into(),
                materialized.map(|(_, end)| end).into(),
                acknowledged.into(),
            ],
        )
        .expect("set position order progress");
    }

    /// Returns the active, a prior, and a later registry generation.
    fn setup_position_order_worker() -> (i64, i64, i64) {
        Spi::run(&format!(
            "CREATE ROLE {POSITION_ORDER_WORKER}
                 LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
             GRANT synchro_worker TO {POSITION_ORDER_WORKER}"
        ))
        .expect("provision position order worker");
        setup_test_tables();
        let active = position_order_generation();
        let prior: i64 = Spi::get_one_with_args(
            "SELECT max(generation) FROM synchro.sync_registry_generations WHERE generation < $1",
            &[active.into()],
        )
        .expect("load prior registry generation")
        .expect("prior registry generation");
        let later: i64 = Spi::get_one(
            "INSERT INTO synchro.sync_registry_generations (
                 stream_generation, state, validated, parent_generation
             )
             SELECT stream_generation, 'pending', true, generation
             FROM synchro.sync_registry_generations
             WHERE state = 'active'
             RETURNING generation",
        )
        .expect("create later registry generation")
        .expect("later registry generation");
        (active, prior, later)
    }

    fn position_order_worker_check(
        progress_generation: i64,
        progress_materialized: Option<(&str, &str)>,
        worker_state: &str,
        worker_generation: i64,
        worker_materialized: Option<(&str, &str)>,
    ) -> Value {
        set_position_order_progress(progress_generation, progress_materialized, Some("0/2000"));
        Spi::run_with_args(
            "INSERT INTO synchro.sync_wal_worker_state (
                 worker_id, database_oid, database_name, worker_login_oid,
                 backend_pid, state, registry_generation,
                 materialized_commit_lsn, materialized_end_lsn,
                 wal_observed_at, heartbeat_at, updated_at
             )
             SELECT 'synchro_wal_consumer', database.oid, database.datname,
                    worker_role.oid, pg_backend_pid(), $1, $2,
                    $3::pg_lsn, $4::pg_lsn, now(), now(), now()
             FROM pg_catalog.pg_database database
             CROSS JOIN pg_catalog.pg_roles worker_role
             WHERE database.datname = current_database()
               AND worker_role.rolname = $5
             ON CONFLICT (worker_id) DO UPDATE
             SET state = EXCLUDED.state,
                 registry_generation = EXCLUDED.registry_generation,
                 materialized_commit_lsn = EXCLUDED.materialized_commit_lsn,
                 materialized_end_lsn = EXCLUDED.materialized_end_lsn",
            &[
                worker_state.into(),
                worker_generation.into(),
                worker_materialized.map(|(commit, _)| commit).into(),
                worker_materialized.map(|(_, end)| end).into(),
                POSITION_ORDER_WORKER.into(),
            ],
        )
        .expect("set position order worker copy");
        position_order_check("worker")
    }

    #[pg_test]
    fn readiness_progress_accepts_acknowledgement_before_processed_end() {
        let generation = position_order_generation();
        Spi::run_with_args(
            "INSERT INTO synchro.sync_wal_transactions (
                 stream_generation, commit_lsn, end_lsn, source_xid,
                 registry_generation, event_count, effect_count, content_hash,
                 content_hash_format, commit_timestamp
             )
             SELECT runtime.stream_generation, boundary.commit_lsn, boundary.end_lsn,
                    '1'::xid, $1, 0, 0, pg_catalog.decode(repeat('00', 32), 'hex'), 2, now()
             FROM synchro.sync_runtime_state runtime
             CROSS JOIN (
                 VALUES ('0/1800'::pg_lsn, '0/2000'::pg_lsn),
                        ('0/2800'::pg_lsn, '0/3000'::pg_lsn)
             ) AS boundary (commit_lsn, end_lsn)
             WHERE runtime.singleton",
            &[generation.into()],
        )
        .expect("create position order transactions");

        // The CHECK constraint sync_wal_progress_acknowledged_processed rejects
        // an acknowledgement outside [generation_start_lsn, processed_end_lsn].
        // The generation start is the lowest acknowledgement that it permits.
        for (label, acknowledged) in [
            ("NULL acknowledgement", None),
            ("acknowledgement at the generation start", Some("0/1000")),
            ("acknowledgement before the processed end", Some("0/2000")),
        ] {
            set_position_order_progress(generation, POSITION_ORDER_MATERIALIZED, acknowledged);
            let check = position_order_check("materialization_progress");
            // No runtime slot exists, so valid progress gives an unknown slot check.
            assert_eq!(check["state"], "unknown", "{label}: {check}");
            assert_eq!(
                check["reason"], "slot_acknowledgement_unknown",
                "{label}: {check}"
            );
        }
    }

    #[pg_test]
    fn readiness_worker_accepts_heartbeat_copy_behind_progress() {
        let (active, prior, _) = setup_position_order_worker();
        for (label, worker_generation, worker_materialized) in [
            (
                "copy equal to progress",
                active,
                POSITION_ORDER_MATERIALIZED,
            ),
            (
                "LSN copy behind progress",
                active,
                Some(("0/1800", "0/2000")),
            ),
            ("NULL LSN copy", active, None),
            (
                "generation copy behind progress",
                prior,
                POSITION_ORDER_MATERIALIZED,
            ),
        ] {
            let check = position_order_worker_check(
                active,
                POSITION_ORDER_MATERIALIZED,
                "running",
                worker_generation,
                worker_materialized,
            );
            assert_eq!(check["state"], "ok", "{label}: {check}");
        }
    }

    #[pg_test]
    fn readiness_worker_rejects_copy_that_a_worker_never_commits() {
        let (active, prior, later) = setup_position_order_worker();
        let baseline = position_order_worker_check(
            active,
            POSITION_ORDER_MATERIALIZED,
            "running",
            active,
            POSITION_ORDER_MATERIALIZED,
        );
        assert_eq!(baseline["state"], "ok", "baseline: {baseline}");

        for (
            label,
            progress_generation,
            progress_materialized,
            state,
            worker_generation,
            worker_materialized,
        ) in [
            (
                "commit copy after progress",
                active,
                POSITION_ORDER_MATERIALIZED,
                "running",
                active,
                Some(("0/2900", "0/3000")),
            ),
            (
                "end copy after progress",
                active,
                POSITION_ORDER_MATERIALIZED,
                "running",
                active,
                Some(("0/2800", "0/3800")),
            ),
            (
                "generation copy after progress",
                active,
                POSITION_ORDER_MATERIALIZED,
                "running",
                later,
                POSITION_ORDER_MATERIALIZED,
            ),
            (
                "LSN copy without a progress position",
                active,
                None,
                "running",
                active,
                POSITION_ORDER_MATERIALIZED,
            ),
            (
                "progress generation that is not active",
                prior,
                POSITION_ORDER_MATERIALIZED,
                "running",
                prior,
                POSITION_ORDER_MATERIALIZED,
            ),
            (
                "blocked worker",
                active,
                POSITION_ORDER_MATERIALIZED,
                "blocked",
                active,
                POSITION_ORDER_MATERIALIZED,
            ),
        ] {
            let check = position_order_worker_check(
                progress_generation,
                progress_materialized,
                state,
                worker_generation,
                worker_materialized,
            );
            assert_eq!(check["state"], "failed", "{label}: {check}");
            assert_eq!(check["reason"], "worker_state_invalid", "{label}: {check}");
        }
    }
