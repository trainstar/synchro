const ASSIGNMENT_SIGNATURE: &str = "p_user_id TEXT";
const ASSIGNMENT_ATTRIBUTES: &str =
    "RETURNS SETOF TEXT LANGUAGE sql STABLE SET search_path = pg_catalog, synchro";
const ASSIGNMENT_BODY: &str = "BEGIN ATOMIC
    SELECT scope_id FROM public.assignment_members WHERE user_id = p_user_id;
END";

fn create_assignment_members() {
    Spi::run("CREATE TABLE public.assignment_members (user_id TEXT NOT NULL, scope_id TEXT)")
        .expect("create assignment members");
}

/// Creates a non-superuser role that owns and registers an assignment
/// function, so a test can observe the privileges of the function owner.
fn create_assignment_owner(role: &str) {
    Spi::run(&format!(
        "CREATE ROLE {role} NOLOGIN NOSUPERUSER;
         GRANT synchro_operator TO {role}"
    ))
    .expect("create assignment function owner");
}

fn transfer_assignment_function(name: &str, role: &str) {
    Spi::run(&format!(
        "ALTER FUNCTION public.{name}(TEXT) OWNER TO {role}"
    ))
    .expect("transfer assignment function ownership");
}

fn as_assignment_owner<T>(role: &str, action: impl FnOnce() -> T) -> T {
    Spi::run(&format!("SET LOCAL ROLE {role}")).expect("select assignment owner role");
    let result = action();
    Spi::run("RESET ROLE").expect("restore test role");
    result
}

fn create_assignment_function(name: &str, signature: &str, attributes: &str, body: &str) {
    Spi::run(&format!(
        "CREATE FUNCTION public.{name}({signature}) {attributes} {body}"
    ))
    .expect("create assignment function");
}

fn restrict_assignment_function(name: &str, argument_types: &str) {
    Spi::run(&format!(
        "REVOKE EXECUTE ON FUNCTION public.{name}({argument_types}) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION public.{name}({argument_types}) TO synchro_owner"
    ))
    .expect("restrict assignment function");
}

fn create_valid_assignment_function(name: &str) {
    create_assignment_function(
        name,
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function(name, "TEXT");
}

fn register_assignment_function(name: &str, max_scopes: i32) {
    Spi::run_with_args(
        "SELECT synchro.synchro_register_assignment_function($1, $2)",
        &[format!("public.{name}").into(), max_scopes.into()],
    )
    .expect("register assignment function");
}

fn assert_assignment_registration_rejected(name: &str, max_scopes: i32) {
    reject_assignment_registration(name, max_scopes);
    assert_eq!(assignment_registration(), None);
}

fn reject_assignment_registration(name: &str, max_scopes: i32) {
    Spi::run(&format!(
        "DO $test$
         DECLARE
             rejected boolean := false;
         BEGIN
             BEGIN
                 PERFORM synchro.synchro_register_assignment_function(
                     'public.{name}', {max_scopes}
                 );
             EXCEPTION WHEN OTHERS THEN
                 rejected := true;
             END;
             IF NOT rejected THEN
                 RAISE EXCEPTION 'assignment registration unexpectedly succeeded';
             END IF;
         END
         $test$"
    ))
    .expect("reject assignment registration");
}

/// Returns the stored registration and compares its digest with the digest of
/// the definition text under the evaluation search path.
fn assignment_registration() -> Option<Value> {
    let search_path: String = Spi::get_one("SELECT current_setting('search_path')")
        .expect("load test search path")
        .expect("test search path");
    Spi::run("SELECT set_config('search_path', 'pg_catalog, synchro, pg_temp', true)")
        .expect("set evaluation search path");
    let registration = Spi::get_one::<pgrx::JsonB>(
        "SELECT (
             SELECT jsonb_build_object(
                 'function', (
                     SELECT format(
                         '%I.%I(%s)', namespace.nspname, procedure.proname,
                         pg_get_function_identity_arguments(procedure.oid)
                     )
                     FROM pg_catalog.pg_proc procedure
                     JOIN pg_catalog.pg_namespace namespace
                       ON namespace.oid = procedure.pronamespace
                     WHERE procedure.oid = function_oid
                 ),
                 'function_schema', function_schema,
                 'function_name', function_name,
                 'max_scopes', max_scopes,
                 'definition_matches', definition_sha256 = encode(
                     sha256(convert_to(pg_get_functiondef(function_oid), 'UTF8')),
                     'hex'
                 )
             )
             FROM synchro.sync_assignment_function
         )",
    )
    .expect("load assignment registration")
    .map(|registration| registration.0);
    Spi::run_with_args(
        "SELECT set_config('search_path', $1, true)",
        &[search_path.into()],
    )
    .expect("restore test search path");
    registration
}

fn insert_assignment_members(rows: &[(&str, Option<&str>)]) {
    for (user_id, scope_id) in rows {
        Spi::run_with_args(
            "INSERT INTO public.assignment_members (user_id, scope_id) VALUES ($1, $2)",
            &[(*user_id).into(), (*scope_id).into()],
        )
        .expect("insert assignment member");
    }
}

fn assert_connect_rejected(user_id: &str, client_id: &str) {
    let request = json!({
        "client_id": client_id,
        "platform": "test",
        "app_version": "1.0.0",
        "protocol_version": 3,
        "schema": { "version": 0, "hash": "" },
        "scope_set_version": 0,
        "known_scopes": {}
    });
    Spi::run(&format!(
        "DO $test$
         DECLARE
             rejected boolean := false;
         BEGIN
             BEGIN
                 PERFORM synchro.synchro_connect('{user_id}', '{request}'::jsonb);
             EXCEPTION WHEN OTHERS THEN
                 rejected := true;
             END;
             IF NOT rejected THEN
                 RAISE EXCEPTION 'connect unexpectedly succeeded';
             END IF;
         END
         $test$"
    ))
    .expect("reject connect");
    let clients = Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM synchro.sync_clients WHERE user_id = $1",
        &[user_id.into()],
    )
    .expect("count rejected clients");
    assert_eq!(clients, Some(0));
}

fn assignment_sources(user_id: &str, client_id: &str) -> Value {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT jsonb_object_agg(scope_id, assignment_source)
         FROM synchro.sync_client_scope_history
         WHERE user_id = $1 AND client_id = $2 AND assigned",
        &[user_id.into(), client_id.into()],
    )
    .expect("load assignment sources")
    .expect("assignment sources")
    .0
}

#[pg_test]
fn assignment_registration_stores_valid_function() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");

    Spi::run("SELECT synchro.synchro_register_assignment_function('public.assigned_scopes')")
        .expect("register assignment function with default bound");

    assert_eq!(
        assignment_registration(),
        Some(json!({
            "function": "public.assigned_scopes(p_user_id text)",
            "function_schema": "public",
            "function_name": "assigned_scopes",
            "max_scopes": 1000,
            "definition_matches": true
        }))
    );
}

#[pg_test]
fn assignment_registration_replaces_earlier_registration() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    create_valid_assignment_function("replacement_scopes");
    register_assignment_function("assigned_scopes", 1000);

    register_assignment_function("replacement_scopes", 5);

    assert_eq!(
        assignment_registration(),
        Some(json!({
            "function": "public.replacement_scopes(p_user_id text)",
            "function_schema": "public",
            "function_name": "replacement_scopes",
            "max_scopes": 5,
            "definition_matches": true
        }))
    );
}

#[pg_test]
fn assignment_registration_rejects_out_of_range_bounds() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");

    assert_assignment_registration_rejected("assigned_scopes", 0);
    assert_assignment_registration_rejected("assigned_scopes", 1001);
}

#[pg_test]
fn assignment_registration_rejects_function_that_is_not_plain() {
    Spi::run(
        "CREATE AGGREGATE public.assigned_scopes(TEXT) (SFUNC = pg_catalog.textcat, STYPE = TEXT);
         REVOKE EXECUTE ON FUNCTION public.assigned_scopes(TEXT) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION public.assigned_scopes(TEXT) TO synchro_owner",
    )
    .expect("create aggregate assignment function");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_second_argument() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        "p_user_id TEXT, p_extra TEXT",
        ASSIGNMENT_ATTRIBUTES,
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function("assigned_scopes", "TEXT, TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_non_text_argument() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        "p_user_id VARCHAR",
        ASSIGNMENT_ATTRIBUTES,
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function("assigned_scopes", "VARCHAR");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_argument_default() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        "p_user_id TEXT DEFAULT ''",
        ASSIGNMENT_ATTRIBUTES,
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_variadic_argument() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        "VARIADIC p_user_ids TEXT[]",
        ASSIGNMENT_ATTRIBUTES,
        "BEGIN ATOMIC
             SELECT scope_id FROM public.assignment_members WHERE user_id = p_user_ids[1];
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT[]");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_single_value_result() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        "RETURNS TEXT LANGUAGE sql STABLE SET search_path = pg_catalog, synchro",
        "BEGIN ATOMIC
             SELECT min(scope_id) FROM public.assignment_members WHERE user_id = p_user_id;
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_unparsed_sql_body() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        "AS $body$
             SELECT scope_id FROM public.assignment_members WHERE user_id = p_user_id
         $body$",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_procedural_language() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        "RETURNS SETOF TEXT LANGUAGE plpgsql STABLE SET search_path = pg_catalog, synchro",
        "AS $body$
         BEGIN
             RETURN QUERY
             SELECT scope_id FROM public.assignment_members WHERE user_id = p_user_id;
         END
         $body$",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_volatile_function() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        "RETURNS SETOF TEXT LANGUAGE sql VOLATILE SET search_path = pg_catalog, synchro",
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_security_definer() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        "RETURNS SETOF TEXT LANGUAGE sql STABLE SECURITY DEFINER
         SET search_path = pg_catalog, synchro",
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_other_search_path() {
    create_assignment_members();
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        "RETURNS SETOF TEXT LANGUAGE sql STABLE SET search_path = public, pg_catalog",
        ASSIGNMENT_BODY,
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_function_owned_by_another_role() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    Spi::run(
        "CREATE ROLE synchro_assignment_other_owner NOLOGIN;
         ALTER FUNCTION public.assigned_scopes(TEXT) OWNER TO synchro_assignment_other_owner",
    )
    .expect("transfer assignment function ownership");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_missing_owner_execute() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    Spi::run("REVOKE EXECUTE ON FUNCTION public.assigned_scopes(TEXT) FROM synchro_owner")
        .expect("revoke assignment function execute");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_public_execute() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    Spi::run("GRANT EXECUTE ON FUNCTION public.assigned_scopes(TEXT) TO PUBLIC")
        .expect("grant public assignment function execute");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_call_outside_pg_catalog() {
    create_assignment_members();
    Spi::run(
        "CREATE FUNCTION public.assignment_user_key(p_user_id TEXT) RETURNS TEXT
         LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, synchro
         RETURN p_user_id",
    )
    .expect("create assignment helper function");
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        "BEGIN ATOMIC
             SELECT scope_id FROM public.assignment_members
             WHERE user_id = public.assignment_user_key(p_user_id);
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_synchro_relation() {
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        "BEGIN ATOMIC
             SELECT scope_id FROM synchro.sync_user_scopes WHERE user_id = p_user_id;
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");

    assert_assignment_registration_rejected("assigned_scopes", 1000);
}

#[pg_test]
fn assignment_registration_rejects_relation_without_owner_select() {
    let owner = "synchro_assignment_select_owner";
    create_assignment_owner(owner);
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    transfer_assignment_function("assigned_scopes", owner);

    as_assignment_owner(owner, || reject_assignment_registration("assigned_scopes", 1000));
    let rejected = assignment_registration();
    Spi::run(&format!("GRANT SELECT ON public.assignment_members TO {owner}"))
        .expect("grant assignment relation select");
    as_assignment_owner(owner, || register_assignment_function("assigned_scopes", 1000));

    assert_eq!(rejected, None);
    assert!(assignment_registration().is_some());
}

#[pg_test]
fn assignment_registration_rejects_schema_without_owner_usage() {
    let owner = "synchro_assignment_usage_owner";
    create_assignment_owner(owner);
    Spi::run(&format!(
        "CREATE SCHEMA assignment_private;
         CREATE TABLE assignment_private.assignment_members (
             user_id TEXT NOT NULL,
             scope_id TEXT
         );
         GRANT SELECT ON assignment_private.assignment_members TO {owner}"
    ))
    .expect("create private assignment members");
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        "BEGIN ATOMIC
             SELECT scope_id FROM assignment_private.assignment_members
             WHERE user_id = p_user_id;
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");
    transfer_assignment_function("assigned_scopes", owner);

    as_assignment_owner(owner, || reject_assignment_registration("assigned_scopes", 1000));
    let rejected = assignment_registration();
    Spi::run(&format!("GRANT USAGE ON SCHEMA assignment_private TO {owner}"))
        .expect("grant private assignment schema usage");
    as_assignment_owner(owner, || register_assignment_function("assigned_scopes", 1000));

    assert_eq!(rejected, None);
    assert!(assignment_registration().is_some());
}

#[pg_test]
fn assignment_evaluation_runs_as_function_owner() {
    let owner = "synchro_assignment_evaluation_owner";
    setup_test_tables();
    create_assignment_owner(owner);
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        "BEGIN ATOMIC
             SELECT 'role:' || CURRENT_USER::text;
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");
    transfer_assignment_function("assigned_scopes", owner);
    as_assignment_owner(owner, || register_assignment_function("assigned_scopes", 1000));

    let response = register_client("u1", "c1");

    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        client_scope_ids("u1", "c1"),
        vec![format!("role:{owner}"), "user:u1".to_string()]
    );
}

#[pg_test]
fn assignment_evaluation_query_to_xml_cannot_read_synchro() {
    let owner = "synchro_assignment_query_owner";
    setup_test_tables();
    create_assignment_owner(owner);
    create_assignment_function(
        "assigned_scopes",
        ASSIGNMENT_SIGNATURE,
        ASSIGNMENT_ATTRIBUTES,
        "BEGIN ATOMIC
             SELECT 'team:alpha'::text
             WHERE pg_catalog.query_to_xml(
                 'SELECT count(*) FROM synchro.sync_clients', false, true, ''
             ) IS NOT NULL;
         END",
    );
    restrict_assignment_function("assigned_scopes", "TEXT");
    transfer_assignment_function("assigned_scopes", owner);
    as_assignment_owner(owner, || register_assignment_function("assigned_scopes", 1000));

    assert!(assignment_registration().is_some());
    assert_connect_rejected("u1", "c1");
}

#[pg_test]
fn assignment_results_join_authoritative_scopes() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    register_shared_scope("catalog", false);
    Spi::run_with_args(
        "SELECT synchro_grant_user_scope($1, $2)",
        &["u1".into(), "team:alpha".into()],
    )
    .unwrap();
    insert_assignment_members(&[
        ("u1", Some("team:beta")),
        ("u1", Some("team:beta")),
        ("u1", Some("team:alpha")),
        ("u1", Some("catalog")),
        ("u2", Some("team:gamma")),
    ]);

    let response = register_client("u1", "c1");

    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        client_scope_ids("u1", "c1"),
        vec!["catalog", "team:alpha", "team:beta", "user:u1"]
    );
    assert_eq!(
        assignment_sources("u1", "c1"),
        json!({
            "catalog": "shared",
            "team:alpha": "assignment_rule",
            "team:beta": "assignment_rule",
            "user:u1": "identity"
        })
    );
}

#[pg_test]
fn assignment_result_with_identity_prefix_fails_connect() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    insert_assignment_members(&[("u1", Some("team:alpha")), ("u1", Some("user:u2"))]);

    assert_connect_rejected("u1", "c1");
}

#[pg_test]
fn assignment_result_that_is_blank_fails_connect() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    insert_assignment_members(&[("u1", Some("team:alpha")), ("u1", Some("  "))]);

    assert_connect_rejected("u1", "c1");
}

#[pg_test]
fn assignment_result_that_is_null_fails_connect() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    insert_assignment_members(&[("u1", Some("team:alpha")), ("u1", None)]);

    assert_connect_rejected("u1", "c1");
}

#[pg_test]
fn assignment_results_above_bound_fail_connect() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 2);
    insert_assignment_members(&[
        ("u1", Some("team:alpha")),
        ("u1", Some("team:alpha")),
        ("u1", Some("team:beta")),
        ("u2", Some("team:alpha")),
        ("u2", Some("team:beta")),
        ("u2", Some("team:gamma")),
    ]);

    let at_bound = register_client("u1", "c1");
    assert!(at_bound.get("error").is_none(), "{at_bound}");
    assert_eq!(
        client_scope_ids("u1", "c1"),
        vec!["team:alpha", "team:beta", "user:u1"]
    );
    assert_connect_rejected("u2", "c2");
}

#[pg_test]
fn changed_assignment_definition_fails_connect() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    insert_assignment_members(&[("u1", Some("team:alpha"))]);
    Spi::run(
        "CREATE OR REPLACE FUNCTION public.assigned_scopes(p_user_id TEXT)
         RETURNS SETOF TEXT LANGUAGE sql STABLE SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT scope_id FROM public.assignment_members WHERE user_id <> p_user_id;
         END",
    )
    .expect("replace assignment function definition");

    assert_connect_rejected("u1", "c1");
}

#[pg_test]
fn dropped_assignment_function_fails_connect() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    Spi::run("DROP FUNCTION public.assigned_scopes(TEXT)").expect("drop assignment function");

    assert_connect_rejected("u1", "c1");
}

#[pg_test]
fn unregistered_assignment_function_assigns_nothing() {
    setup_test_tables();
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    insert_assignment_members(&[("u1", Some("team:alpha"))]);

    Spi::run("SELECT synchro.synchro_unregister_assignment_function()")
        .expect("unregister assignment function");
    let response = register_client("u1", "c1");

    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(assignment_registration(), None);
    assert_eq!(client_scope_ids("u1", "c1"), vec!["user:u1"]);
}

#[pg_test]
fn assignment_unregistration_ignores_temporary_shadow_table() {
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");
    register_assignment_function("assigned_scopes", 1000);
    Spi::run(
        "CREATE TABLE public.assignment_shadow_runs (role_name TEXT NOT NULL);
         GRANT INSERT ON public.assignment_shadow_runs TO PUBLIC",
    )
    .expect("create shadow trigger log");

    Spi::run("SET LOCAL ROLE synchro_operator").expect("select operator role");
    Spi::run(
        "CREATE TEMP TABLE sync_assignment_function (singleton BOOLEAN);
         GRANT ALL ON pg_temp.sync_assignment_function TO PUBLIC;
         CREATE FUNCTION pg_temp.record_assignment_shadow() RETURNS trigger
         LANGUAGE plpgsql AS $shadow$
         BEGIN
             INSERT INTO public.assignment_shadow_runs (role_name) VALUES (current_user);
             RETURN NULL;
         END
         $shadow$;
         CREATE TRIGGER record_assignment_shadow
         BEFORE DELETE ON pg_temp.sync_assignment_function
         FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.record_assignment_shadow()",
    )
    .expect("create temporary shadow table");
    Spi::run("SELECT synchro.synchro_unregister_assignment_function()")
        .expect("unregister assignment function");
    Spi::run("RESET ROLE").expect("restore test role");

    let shadow_roles = Spi::get_one::<Vec<String>>(
        "SELECT COALESCE(array_agg(role_name ORDER BY role_name), ARRAY[]::text[])
         FROM public.assignment_shadow_runs",
    )
    .expect("load shadow trigger roles");
    let registrations =
        Spi::get_one::<i64>("SELECT count(*) FROM synchro.sync_assignment_function")
            .expect("count assignment registrations");
    assert_eq!(shadow_roles, Some(Vec::new()));
    assert_eq!(registrations, Some(0));
}

#[pg_test]
fn assignment_health_fails_for_changed_or_missing_definition() {
    Spi::run(
        "DROP ROLE IF EXISTS synchro_assignment_health_worker;
         CREATE ROLE synchro_assignment_health_worker
             LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
         GRANT synchro_worker TO synchro_assignment_health_worker",
    )
    .expect("provision assignment health worker");
    let database: String = Spi::get_one("SELECT current_database()::text")
        .expect("load assignment health database")
        .expect("assignment health database");
    let mut configuration = crate::health::ReadinessConfiguration::configured();
    configuration.database = Some(database);
    configuration.worker_login = Some("synchro_assignment_health_worker".to_string());
    let assignment_check = || {
        crate::health::load_readiness_status_with_configuration(configuration.clone()).detail()
            ["checks"]["assignment_function"]
            .clone()
    };
    let ok = json!({ "state": "ok", "reason": "ok" });
    let drifted = json!({ "state": "failed", "reason": "assignment_function_drifted" });
    create_assignment_members();
    create_valid_assignment_function("assigned_scopes");

    let unregistered = assignment_check();
    register_assignment_function("assigned_scopes", 1000);
    let registered = assignment_check();
    Spi::run(
        "CREATE OR REPLACE FUNCTION public.assigned_scopes(p_user_id TEXT)
         RETURNS SETOF TEXT LANGUAGE sql STABLE SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT scope_id FROM public.assignment_members WHERE user_id <> p_user_id;
         END",
    )
    .expect("replace assignment function definition");
    let changed = assignment_check();
    register_assignment_function("assigned_scopes", 1000);
    let reregistered = assignment_check();
    Spi::run("DROP FUNCTION public.assigned_scopes(TEXT)").expect("drop assignment function");
    let missing = assignment_check();

    assert_eq!(unregistered, ok);
    assert_eq!(registered, ok);
    assert_eq!(changed, drifted);
    assert_eq!(reregistered, ok);
    assert_eq!(missing, drifted);
}
