fn has_function_privilege(role: &str, function: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT pg_catalog.has_function_privilege($1, $2, 'EXECUTE')",
        &[role.into(), function.into()],
    )
    .expect("function privilege query")
    .expect("function privilege result")
}

struct RegistrationFixture {
    table: String,
    function: String,
    push_policy: &'static str,
}

fn registration_fixture(
    enable_rls: bool,
    push_policy: &'static str,
    grant_update: bool,
) -> RegistrationFixture {
    let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
        .expect("registration fixture suffix query")
        .expect("registration fixture suffix");
    let table = format!("synchro_authorization_{suffix}");
    let function = format!("sa_membership_{suffix}");
    let policy = format!("synchro_authorization_policy_{suffix}");

    Spi::run(&format!(
        "CREATE TABLE public.{table} (
             id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         CREATE FUNCTION public.{function}(p_key UUID)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT 'registration'::text;
         END;
         REVOKE EXECUTE ON FUNCTION public.{function}(UUID) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION public.{function}(UUID)
             TO synchro_owner, synchro_worker;
         GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;"
    ))
    .expect("create registration fixture");

    let privileges = match (push_policy, grant_update) {
        ("read_only", _) => "SELECT",
        ("enabled", true) => "SELECT, INSERT, UPDATE",
        ("enabled", false) => "SELECT, INSERT",
        _ => panic!("invalid registration fixture push policy"),
    };
    Spi::run(&format!(
        "GRANT {privileges} ON TABLE public.{table} TO synchro_owner"
    ))
    .expect("grant registration fixture relation privileges");

    if enable_rls {
        Spi::run(&format!(
            "ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;
             CREATE POLICY {policy} ON public.{table}
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true)"
        ))
        .expect("create registration fixture row-level security policy");
    }

    RegistrationFixture {
        table,
        function,
        push_policy,
    }
}

fn register_fixture(fixture: &RegistrationFixture) -> Result<(), pgrx::spi::Error> {
    Spi::run(&format!(
        "SELECT synchro.synchro_register_table(
             'public.{table}',
             'public.{function}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', '{push_policy}'
         )",
        table = fixture.table,
        function = fixture.function,
        push_policy = fixture.push_policy,
    ))
}

fn reject_fixture_registration(fixture: &RegistrationFixture) -> Result<(), pgrx::spi::Error> {
    Spi::run(&format!(
        "DO $test$
         DECLARE
             rejected boolean := false;
         BEGIN
             BEGIN
                 PERFORM synchro.synchro_register_table(
                     'public.{table}',
                     'public.{function}',
                     'single_scope',
                     'id', 'updated_at', 'deleted_at', '{push_policy}'
                 );
             EXCEPTION WHEN OTHERS THEN
                 rejected := true;
             END;
             IF NOT rejected THEN
                 RAISE EXCEPTION 'registration unexpectedly succeeded';
             END IF;
         END
         $test$",
        table = fixture.table,
        function = fixture.function,
        push_policy = fixture.push_policy,
    ))
}

fn fixture_registry_count(fixture: &RegistrationFixture) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)
         FROM synchro.sync_registry
         WHERE physical_relation_oid = 'public.{table}'::pg_catalog.regclass",
        table = fixture.table,
    ))
    .expect("registration fixture registry count query")
    .expect("registration fixture registry count")
}

fn cleanup_registration_fixture(fixture: &RegistrationFixture, registered: bool) {
    if registered {
        Spi::run(&format!(
            "SELECT synchro.synchro_unregister_table('{table}')",
            table = fixture.table,
        ))
        .expect("unregister registration fixture");
    }
    Spi::run(&format!(
        "DROP TABLE IF EXISTS public.{} CASCADE",
        fixture.table
    ))
    .expect("drop registration fixture table");
    Spi::run(&format!(
        "DROP FUNCTION IF EXISTS public.{}(UUID)",
        fixture.function
    ))
    .expect("drop registration fixture function");
}

#[pg_test]
fn seed_and_operator_have_only_declared_function_grants() {
    assert!(has_function_privilege(
        "synchro_seed",
        "synchro.synchro_schema_manifest()"
    ));
    assert!(has_function_privilege(
        "synchro_seed",
        "synchro.synchro_portable_seed_manifest(integer)"
    ));
    assert!(has_function_privilege(
        "synchro_seed",
        "synchro.synchro_portable_seed_scope(text,text,text,bigint,integer)"
    ));
    let can_read_token_keys: Option<bool> = Spi::get_one(
        "SELECT pg_catalog.has_table_privilege(
             'synchro_seed', 'synchro.sync_token_keys', 'SELECT'
         )",
    )
    .expect("seed token key privilege query");
    assert_eq!(can_read_token_keys, Some(false));
    assert!(!has_function_privilege(
        "synchro_seed",
        "synchro.synchro_readiness()"
    ));
    assert!(has_function_privilege(
        "synchro_operator",
        "synchro.synchro_debug(text,text)"
    ));
    assert!(has_function_privilege(
        "synchro_operator",
        "synchro.synchro_expire_retention_client(text,text)"
    ));
    assert!(!has_function_privilege(
        "synchro_adapter",
        "synchro.synchro_expire_retention_client(text,text)"
    ));
    assert!(!has_function_privilege(
        "synchro_worker",
        "synchro.synchro_expire_retention_client(text,text)"
    ));
    assert!(!has_function_privilege(
        "synchro_operator",
        "synchro.synchro_connect(text,jsonb)"
    ));
}

#[pg_test]
fn registration_functions_have_required_security_and_grants() {
    let protected: Option<bool> = Spi::get_one(
        "WITH registration_functions AS (
             SELECT procedure.oid,
                    procedure.prosecdef,
                    procedure.proowner = owner_role.oid AS owned_by_synchro_owner,
                    COALESCE(procedure.proconfig, ARRAY[]::text[])
                        @> ARRAY['search_path=pg_catalog, synchro'] AS fixed_path
             FROM pg_catalog.pg_proc procedure
             JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
             CROSS JOIN (
                 SELECT oid
                 FROM pg_catalog.pg_roles
                 WHERE rolname = 'synchro_owner'
             ) AS owner_role
             WHERE namespace.nspname = 'synchro'
               AND procedure.proname IN (
                   'synchro_register_table',
                   'synchro_register_membership_dependency',
                   'synchro_unregister_table',
                   'synchro_backfill_bucket_edges'
               )
         )
         SELECT count(*) = 4
                AND bool_and(prosecdef)
                AND bool_and(owned_by_synchro_owner)
                AND bool_and(fixed_path)
                AND bool_and(
                    pg_catalog.has_function_privilege(
                        'synchro_operator', oid, 'EXECUTE'
                    )
                )
         FROM registration_functions",
    )
    .expect("registration function authorization query");
    assert_eq!(protected, Some(true));
    assert!(has_function_privilege(
        "synchro_operator",
        "synchro.synchro_primary_key_guard()"
    ));
    assert!(has_function_privilege(
        "synchro_operator",
        "synchro.synchro_capture_fence()"
    ));
}

#[pg_test]
fn projection_bootstrap_functions_are_operator_only() {
    let protected: Option<bool> = Spi::get_one(
        "WITH bootstrap_functions AS (
             SELECT procedure.oid, procedure.prosecdef,
                    procedure.proowner = owner_role.oid AS owned_by_synchro_owner,
                    COALESCE(procedure.proconfig, ARRAY[]::text[])
                        @> ARRAY['search_path=pg_catalog, synchro'] AS fixed_path
             FROM pg_catalog.pg_proc procedure
             JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
             CROSS JOIN (
                 SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'synchro_owner'
             ) owner_role
             WHERE namespace.nspname = 'synchro'
               AND procedure.proname IN (
                   'synchro_prepare_projection_bootstrap',
                   'synchro_stage_projection_bootstrap',
                   'synchro_emit_projection_bootstrap_barrier',
                   'synchro_request_projection_bootstrap_barrier',
                   'synchro_activate_projection_bootstrap',
                   'synchro_projection_bootstrap_status',
                   'synchro_abort_projection_bootstrap',
                   'synchro_complete_projection_bootstrap_cleanup'
               )
         )
         SELECT count(*) = 8
                AND bool_and(prosecdef)
                AND bool_and(owned_by_synchro_owner)
                AND bool_and(fixed_path)
                AND bool_and(pg_catalog.has_function_privilege(
                    'synchro_operator', oid, 'EXECUTE'
                ))
                AND bool_and(NOT pg_catalog.has_function_privilege(
                    'synchro_adapter', oid, 'EXECUTE'
                ))
                AND bool_and(NOT pg_catalog.has_function_privilege(
                    'synchro_worker', oid, 'EXECUTE'
                ))
         FROM bootstrap_functions",
    )
    .expect("projection bootstrap function authorization query");
    assert_eq!(protected, Some(true));
}

#[pg_test]
fn projection_bootstrap_runtime_reads_are_worker_only() {
    let protected: Option<bool> = Spi::get_one(
        "WITH runtime_read_functions AS (
             SELECT procedure.oid, procedure.prosecdef,
                    procedure.proowner = owner_role.oid AS owned_by_synchro_owner,
                    COALESCE(procedure.proconfig, ARRAY[]::text[])
                        @> ARRAY['search_path=pg_catalog, synchro'] AS fixed_path
             FROM pg_catalog.pg_proc procedure
             JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
             CROSS JOIN (
                 SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'synchro_owner'
             ) owner_role
             WHERE namespace.nspname = 'synchro'
                AND procedure.proname IN (
                    'synchro_projection_bootstrap_active_stream',
                     'synchro_projection_bootstrap_main_boundary',
                     'synchro_projection_bootstrap_slot_absent',
                     'synchro_projection_bootstrap_next_aborted_slot',
                    'synchro_projection_bootstrap_is_activated',
                    'synchro_projection_bootstrap_interrupted'
                )
          )
          SELECT count(*) = 6
                AND bool_and(prosecdef)
                AND bool_and(owned_by_synchro_owner)
                AND bool_and(fixed_path)
                AND bool_and(pg_catalog.has_function_privilege(
                    'synchro_worker', oid, 'EXECUTE'
                ))
                AND bool_and(NOT pg_catalog.has_function_privilege(
                    'synchro_operator', oid, 'EXECUTE'
                ))
                AND bool_and(NOT pg_catalog.has_function_privilege(
                    'synchro_adapter', oid, 'EXECUTE'
                ))
         FROM runtime_read_functions",
    )
    .expect("projection bootstrap runtime read authorization query");
    assert_eq!(protected, Some(true));
}

#[pg_test]
fn operator_can_inspect_candidate_slot_only() {
    let candidate_slot = "synchro_operator_candidate";
    assert!(has_function_privilege(
        "synchro_operator",
        "synchro.synchro_projection_bootstrap_slot_drop_state(text)"
    ));
    assert!(has_function_privilege(
        "synchro_worker",
        "synchro.synchro_projection_bootstrap_slot_drop_state(text)"
    ));
    assert!(!has_function_privilege(
        "synchro_adapter",
        "synchro.synchro_projection_bootstrap_slot_drop_state(text)"
    ));
    Spi::run("SET LOCAL ROLE synchro_operator").expect("select operator role");
    let slot_state: pgrx::JsonB = Spi::get_one_with_args(
        "SELECT synchro.synchro_projection_bootstrap_slot_drop_state($1)",
        &[candidate_slot.into()],
    )
    .expect("inspect candidate slot as operator")
    .expect("candidate slot state");
    let active_stream_allowed = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
        Spi::get_one::<pgrx::JsonB>("SELECT synchro.synchro_projection_bootstrap_active_stream()")
            .is_ok()
    }))
    .catch_others(|_| false)
    .execute();
    Spi::run("RESET ROLE").expect("restore test role");

    assert_eq!(
        slot_state.0,
        serde_json::json!({"present": false, "active": false, "valid": true})
    );
    assert!(!active_stream_allowed);
}

#[pg_test]
fn worker_has_projection_bootstrap_table_privileges() {
    let authorized: Option<bool> = Spi::get_one(
        "WITH required(table_name, privilege) AS (
             VALUES
                 ('synchro.sync_stream_resets', 'SELECT'),
                 ('synchro.sync_stream_resets', 'UPDATE'),
                 ('synchro.sync_registry_membership_stages', 'SELECT'),
                 ('synchro.sync_registry_membership_stages', 'UPDATE'),
                 ('synchro.sync_projection_bootstrap_transactions', 'SELECT'),
                 ('synchro.sync_projection_bootstrap_transactions', 'INSERT'),
                 ('synchro.sync_projection_bootstrap_transactions', 'UPDATE'),
                 ('synchro.sync_projection_bootstrap_events', 'SELECT'),
                 ('synchro.sync_projection_bootstrap_events', 'INSERT'),
                 ('synchro.sync_stream_reset_row_versions', 'SELECT'),
                 ('synchro.sync_stream_reset_row_versions', 'INSERT'),
                 ('synchro.sync_stream_reset_row_versions', 'UPDATE'),
                 ('synchro.sync_stream_reset_captured_rows', 'SELECT'),
                 ('synchro.sync_stream_reset_captured_rows', 'INSERT'),
                 ('synchro.sync_stream_reset_captured_rows', 'UPDATE'),
                 ('synchro.sync_stream_reset_captured_rows', 'DELETE'),
                 ('synchro.sync_stream_reset_capture_dependency_rows', 'SELECT'),
                 ('synchro.sync_stream_reset_capture_dependency_rows', 'INSERT'),
                 ('synchro.sync_stream_reset_capture_dependency_rows', 'UPDATE'),
                 ('synchro.sync_stream_reset_capture_dependency_rows', 'DELETE'),
                 ('synchro.sync_stream_reset_membership_edges', 'SELECT'),
                 ('synchro.sync_stream_reset_membership_edges', 'INSERT'),
                 ('synchro.sync_stream_reset_membership_edges', 'DELETE'),
                 ('synchro.sync_stream_reset_scope_digests', 'SELECT'),
                 ('synchro.sync_stream_reset_scope_digests', 'INSERT'),
                 ('synchro.sync_stream_reset_scope_digests', 'DELETE'),
                 ('synchro.sync_stream_reset_fence_coverage', 'SELECT')
         )
         SELECT bool_and(pg_catalog.has_table_privilege(
             'synchro_worker', table_name, privilege
         ))
         FROM required",
    )
    .expect("projection bootstrap worker table authorization query");
    assert_eq!(authorized, Some(true));
}

#[pg_test]
fn registration_accepts_required_relation_and_membership_policy() {
    let fixture = registration_fixture(true, "enabled", true);
    let registration = register_fixture(&fixture);
    let registered = registration.is_ok();
    let count = fixture_registry_count(&fixture);
    let logical_name: Option<String> = Spi::get_one(&format!(
        "SELECT table_name FROM synchro.sync_registry
         WHERE physical_relation_oid = 'public.{}'::pg_catalog.regclass",
        fixture.table
    ))
    .expect("registration fixture logical table query");
    cleanup_registration_fixture(&fixture, registered);

    assert!(
        registered,
        "valid controlled registration failed: {registration:?}"
    );
    assert_eq!(count, 1);
    assert_eq!(logical_name.as_deref(), Some(fixture.table.as_str()));
}

#[pg_test]
fn registration_membership_change_stages_atomic_replacement() {
    let fixture = registration_fixture(true, "enabled", true);
    register_fixture(&fixture).expect("register membership replacement fixture");
    activate_pending_registry_for_test();
    Spi::run(&format!(
        "SELECT synchro.synchro_register_table(
             'public.{table}', 'public.{function}', 'multi_scope',
             'id', 'updated_at', 'deleted_at', '{push_policy}'
         )",
        table = fixture.table,
        function = fixture.function,
        push_policy = fixture.push_policy,
    ))
    .expect("stage membership replacement registration");

    let pending: pgrx::JsonB = Spi::get_one::<pgrx::JsonB>(&format!(
        "SELECT jsonb_build_object(
             'generation_state', generation.state,
             'stage_state', stage.state,
             'source_is_parent', stage.source_registry_generation = generation.parent_generation,
             'targets_relation', registry.relation_id = ANY(stage.target_relation_ids)
         )
         FROM synchro.sync_registry registry
         JOIN synchro.sync_registry_generations generation
           ON generation.generation = registry.registry_generation
         JOIN synchro.sync_registry_membership_stages stage
           ON stage.registry_generation = generation.generation
         WHERE registry.physical_relation_oid = 'public.{table}'::regclass
         ORDER BY generation.generation DESC LIMIT 1",
        table = fixture.table,
    ))
    .expect("membership replacement stage query")
    .expect("membership replacement stage");
    assert_eq!(pending.0["generation_state"], "pending");
    assert_eq!(pending.0["stage_state"], "pending");
    assert_eq!(pending.0["source_is_parent"], true);
    assert_eq!(pending.0["targets_relation"], true);
}

#[pg_test]
fn registration_rejects_disabled_rls_without_side_effects() {
    let fixture = registration_fixture(false, "enabled", true);
    let registration = reject_fixture_registration(&fixture);
    let count = fixture_registry_count(&fixture);
    cleanup_registration_fixture(&fixture, false);

    assert!(registration.is_ok());
    assert_eq!(count, 0);
}

#[pg_test]
fn registration_rejects_missing_owner_privilege_without_effects() {
    let fixture = registration_fixture(true, "enabled", false);
    let registration = reject_fixture_registration(&fixture);
    let count = fixture_registry_count(&fixture);
    cleanup_registration_fixture(&fixture, false);

    assert!(registration.is_ok());
    assert_eq!(count, 0);
}

#[pg_test]
fn registration_rejects_extra_read_only_dml_grant_without_effects() {
    let fixture = registration_fixture(true, "read_only", true);
    Spi::run(&format!(
        "GRANT INSERT ON TABLE public.{} TO synchro_owner",
        fixture.table
    ))
    .expect("grant extra read-only fixture privilege");
    let registration = reject_fixture_registration(&fixture);
    let count = fixture_registry_count(&fixture);
    cleanup_registration_fixture(&fixture, false);

    assert!(registration.is_ok());
    assert_eq!(count, 0);
}

#[pg_test]
fn registration_rejects_unqualified_physical_relation() {
    let fixture = registration_fixture(true, "enabled", true);
    let registration = Spi::run(&format!(
        "DO $test$
         DECLARE
             rejected boolean := false;
         BEGIN
             BEGIN
                 PERFORM synchro.synchro_register_table(
                     '{table}', 'public.{function}', 'single_scope',
                     'id', 'updated_at', 'deleted_at', '{push_policy}'
                 );
             EXCEPTION WHEN OTHERS THEN
                 rejected := true;
             END;
             IF NOT rejected THEN
                 RAISE EXCEPTION 'unqualified relation registration unexpectedly succeeded';
             END IF;
         END
         $test$",
        table = fixture.table,
        function = fixture.function,
        push_policy = fixture.push_policy,
    ));
    let count = fixture_registry_count(&fixture);
    cleanup_registration_fixture(&fixture, false);

    assert!(registration.is_ok());
    assert_eq!(count, 0);
}

#[pg_test]
fn registration_rejects_nondatetime_lifecycle_field() {
    let fixture = registration_fixture(true, "enabled", true);
    Spi::run(&format!(
        "ALTER TABLE public.{} ALTER COLUMN updated_at TYPE TEXT USING updated_at::text",
        fixture.table
    ))
    .expect("change lifecycle field type");
    let registration = reject_fixture_registration(&fixture);
    let count = fixture_registry_count(&fixture);
    cleanup_registration_fixture(&fixture, false);

    assert!(registration.is_ok());
    assert_eq!(count, 0);
}

#[pg_test]
fn fixed_group_roles_have_only_negative_role_attributes() {
    let valid: Option<bool> = Spi::get_one(
        "SELECT count(*) = 6
                AND bool_and(NOT rolcanlogin)
                AND bool_and(NOT rolreplication)
                AND bool_and(NOT rolsuper)
                AND bool_and(NOT rolcreatedb)
                AND bool_and(NOT rolcreaterole)
                AND bool_and(NOT rolbypassrls)
         FROM pg_catalog.pg_roles
         WHERE rolname IN (
             'synchro_owner',
             'synchro_adapter',
             'synchro_seed',
             'synchro_monitor',
             'synchro_operator',
             'synchro_worker'
         )",
    )
    .expect("fixed group role attribute query");
    assert_eq!(valid, Some(true));
}
