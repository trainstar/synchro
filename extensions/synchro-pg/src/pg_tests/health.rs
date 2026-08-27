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
    fn readiness_rejects_incomplete_capture_health() {
        let backend_pid: i32 = Spi::get_one("SELECT pg_backend_pid()")
            .expect("load health test backend PID")
            .expect("health test backend PID");
        let slot = format!("synchro_health_missing_{backend_pid}");

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
            "UPDATE synchro.sync_runtime_state
             SET active_slot_name = $1, updated_at = now()
             WHERE singleton",
            &[slot.as_str().into()],
        )
        .expect("set active health test slot");
        Spi::run(
            "INSERT INTO synchro.sync_wal_worker_state (
                 worker_id, database_oid, database_name, worker_login_oid,
                 backend_pid, state, registry_generation,
                 materialized_commit_lsn, materialized_end_lsn,
                 heartbeat_at, updated_at
             )
             SELECT 'synchro_wal_consumer', database.oid, database.datname,
                    worker_role.oid, pg_backend_pid(), 'running',
                    progress.registry_generation,
                    progress.materialized_commit_lsn, progress.materialized_end_lsn,
                    now(), now()
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
        let configuration = crate::health::ReadinessConfiguration {
            database: Some(database),
            publication: Some("synchro_pub".to_string()),
            replication_slot: Some(slot.clone()),
            worker_login: Some("synchro_health_worker".to_string()),
            max_heartbeat_age_seconds: 30,
            max_wal_lag_bytes: i32::MAX,
            max_wal_lag_seconds: 30,
        };

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
            "INSERT INTO synchro.sync_wal_poison (
                 stream_generation, commit_lsn, failure_class, lifecycle
             )
             SELECT stream_generation, '0/1', 'decode_failed', 'active'
             FROM synchro.sync_runtime_state WHERE singleton",
        )
        .expect("create blocking health test poison");
        let poison_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let poison_rejected = !poison_detail["ready"].as_bool().unwrap_or(true)
            && poison_detail["checks"]["poison"]["state"].as_str() == Some("failed");
        Spi::run("DELETE FROM synchro.sync_wal_poison WHERE lifecycle = 'active'")
            .expect("remove blocking health test poison");

        let mut invalid_limit = configuration.clone();
        invalid_limit.max_wal_lag_bytes = 0;
        let invalid_limit_detail =
            crate::health::load_readiness_status_with_configuration(invalid_limit).detail();
        let invalid_limit_rejected = !invalid_limit_detail["ready"].as_bool().unwrap_or(true)
            && invalid_limit_detail["checks"]["wal_byte_lag"]["state"].as_str()
                == Some("failed");

        let missing_slot_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let missing_slot_rejected = !missing_slot_detail["ready"].as_bool().unwrap_or(true)
            && missing_slot_detail["checks"]["replication_slot"]["state"].as_str()
                == Some("failed")
            && missing_slot_detail["checks"]["wal_byte_lag"]["state"].as_str()
                == Some("unknown");

        Spi::run(
            "INSERT INTO synchro.sync_wal_transactions (
                 stream_generation, commit_lsn, end_lsn, source_xid,
                 registry_generation, event_count, effect_count, content_hash,
                 commit_timestamp
             )
             SELECT runtime.stream_generation, '0/A', '0/B', '1'::xid,
                    progress.registry_generation, 0, 0,
                    pg_catalog.decode(repeat('00', 32), 'hex'), now()
             FROM synchro.sync_runtime_state runtime
             CROSS JOIN synchro.sync_wal_progress progress
             WHERE runtime.singleton AND progress.singleton;
             UPDATE synchro.sync_wal_progress
             SET materialized_commit_lsn = '0/A',
                 materialized_end_lsn = '0/B',
                 acknowledged_end_lsn = NULL,
                 updated_at = now()
             WHERE singleton;
             UPDATE synchro.sync_wal_worker_state
             SET materialized_commit_lsn = '0/A',
                 materialized_end_lsn = '0/B',
                 heartbeat_at = now(),
                 updated_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
        )
        .expect("create nonacknowledged health test progress");
        let progress_detail = crate::health::load_readiness_status_with_configuration(
            configuration.clone(),
        )
        .detail();
        let nonacknowledged_progress_rejected =
            !progress_detail["ready"].as_bool().unwrap_or(true)
                && progress_detail["checks"]["materialization_progress"]["state"].as_str()
                    == Some("failed");
        Spi::run(
            "DELETE FROM synchro.sync_wal_transactions WHERE commit_lsn = '0/A';
             UPDATE synchro.sync_wal_progress
             SET materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 acknowledged_end_lsn = NULL,
                 updated_at = now()
             WHERE singleton;
             UPDATE synchro.sync_wal_worker_state
             SET materialized_commit_lsn = NULL,
                 materialized_end_lsn = NULL,
                 heartbeat_at = now(),
                 updated_at = now()
             WHERE worker_id = 'synchro_wal_consumer'",
        )
        .expect("restore health test progress");

        let bounded_detail = missing_slot_detail.to_string().len() < 4096;

        Spi::run("DELETE FROM synchro.sync_wal_worker_state WHERE worker_id = 'synchro_wal_consumer'")
            .expect("remove health test worker state");
        Spi::run(
            "REVOKE synchro_worker FROM synchro_health_worker;
             DROP ROLE synchro_health_worker",
        )
        .expect("remove health test identity");

        assert!(disabled_trigger_rejected);
        assert!(extra_publication_relation_rejected);
        assert!(stale_heartbeat_rejected);
        assert!(oldest_commit_age_reported);
        assert!(poison_rejected);
        assert!(invalid_limit_rejected);
        assert!(missing_slot_rejected);
        assert!(nonacknowledged_progress_rejected);
        assert!(bounded_detail, "detailed health exposed unbounded or sensitive state");
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
            replication_slot: Some("synchro_poison_scope_slot".to_string()),
            worker_login: Some("synchro_poison_scope_worker".to_string()),
            max_heartbeat_age_seconds: 30,
            max_wal_lag_bytes: i32::MAX,
            max_wal_lag_seconds: 30,
        };
        Spi::run_with_args(
            "INSERT INTO synchro.sync_wal_poison (
                 stream_generation, commit_lsn, failure_class, lifecycle
             ) VALUES ($1, '0/1', 'validation_failed', 'active')",
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
                 stream_generation, commit_lsn, failure_class, lifecycle
             ) VALUES ($1, '0/2', 'validation_failed', 'active')",
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
    fn public_readiness_is_generic_and_fail_closed_without_limits() {
        let readiness: pgrx::JsonB = Spi::get_one("SELECT synchro.synchro_readiness()")
            .expect("query public readiness")
            .expect("public readiness result");
        let object = readiness.0.as_object().expect("public readiness object");
        assert_eq!(object.len(), 1);
        assert_eq!(object.get("ready").and_then(Value::as_bool), Some(false));
    }
