struct MembershipDependencyFixture {
    source_table: String,
    target_table: String,
    source_membership: String,
    target_membership: String,
    impact_function: String,
    source_relation_id: String,
    target_relation_id: String,
    target_table_id: String,
    dependency_field_id: String,
}

fn membership_dependency_fixture() -> MembershipDependencyFixture {
    let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
        .expect("membership fixture suffix query")
        .expect("membership fixture suffix");
    let source_table = format!("md_source_{suffix}");
    let target_table = format!("md_target_{suffix}");
    let source_membership = format!("md_source_membership_{suffix}");
    let target_membership = format!("md_target_membership_{suffix}");
    let impact_function = format!("md_impact_{suffix}");
    let source_policy = format!("md_source_policy_{suffix}");
    let target_policy = format!("md_target_policy_{suffix}");

    Spi::run(&format!(
        "CREATE TABLE public.{source_table} (
             id INTEGER PRIMARY KEY,
             target_id INTEGER NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         CREATE TABLE public.{target_table} (
             id INTEGER PRIMARY KEY,
             label TEXT NOT NULL DEFAULT '',
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );"
    ))
    .expect("create membership fixture tables");
    Spi::run(&format!(
        "SELECT synchro.synchro_prepare_projection_view(
             'public.{source_table}', '{source_table}',
             ARRAY['id', 'target_id', 'updated_at', 'deleted_at']::text[]
         );
         SELECT synchro.synchro_prepare_projection_view(
             'public.{target_table}', '{target_table}',
             ARRAY['id', 'label', 'updated_at', 'deleted_at']::text[]
         );
         CREATE FUNCTION public.{source_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT 'source-scope'::text;
         END;
         CREATE FUNCTION public.{target_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT 'target-scope'::text;
         END;
         REVOKE EXECUTE ON FUNCTION public.{source_membership}(INTEGER) FROM PUBLIC;
         REVOKE EXECUTE ON FUNCTION public.{target_membership}(INTEGER) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION public.{source_membership}(INTEGER)
             TO synchro_owner, synchro_worker;
         GRANT EXECUTE ON FUNCTION public.{target_membership}(INTEGER)
             TO synchro_owner, synchro_worker;
         GRANT SELECT, INSERT, UPDATE ON TABLE public.{source_table} TO synchro_owner;
         GRANT SELECT, INSERT, UPDATE ON TABLE public.{target_table} TO synchro_owner;
         ALTER TABLE public.{source_table} ENABLE ROW LEVEL SECURITY;
         ALTER TABLE public.{target_table} ENABLE ROW LEVEL SECURITY;
         CREATE POLICY {source_policy} ON public.{source_table}
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true);
         CREATE POLICY {target_policy} ON public.{target_table}
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true);"
    ))
    .expect("create membership fixture functions and policy");

    Spi::run(&format!(
        "SELECT synchro.synchro_register_table(
             'public.{source_table}',
             'public.{source_membership}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled'
         );
         SELECT synchro.synchro_register_table(
             'public.{target_table}',
             'public.{target_membership}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled'
         );"
    ))
    .expect("register membership fixture relations");
    activate_pending_registry_for_test();

    let (source_relation_id, target_relation_id, target_table_id, dependency_field_id) =
        Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let source = registry
                .iter()
                .find(|registration| registration.physical_relation == source_table)
                .expect("registered membership source relation");
            let target = registry
                .iter()
                .find(|registration| registration.physical_relation == target_table)
                .expect("registered membership target relation");
            let dependency_field_id = source
                .fields
                .iter()
                .find(|field| field.physical_column == "target_id")
                .map(|field| field.field_id.clone())
                .expect("registered membership dependency field");
            Ok::<_, pgrx::spi::Error>((
                source.relation_id.clone(),
                target.relation_id.clone(),
                target.table_id.clone(),
                dependency_field_id,
            ))
        })
        .expect("load membership fixture registry");

    MembershipDependencyFixture {
        source_table,
        target_table,
        source_membership,
        target_membership,
        impact_function,
        source_relation_id,
        target_relation_id,
        target_table_id,
        dependency_field_id,
    }
}

fn create_impact_function(
    fixture: &MembershipDependencyFixture,
    body: &str,
    revoke_public: bool,
    grant_worker: bool,
) {
    Spi::run(&format!(
        "CREATE FUNCTION public.{function}(old_row JSONB, new_row JSONB)
         RETURNS SETOF synchro.synchro_row_ref
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
         {body};
         END",
        function = fixture.impact_function,
    ))
    .expect("create impact function");
    if revoke_public {
        Spi::run(&format!(
            "REVOKE EXECUTE ON FUNCTION public.{}(JSONB, JSONB) FROM PUBLIC",
            fixture.impact_function
        ))
        .expect("revoke impact function public execute");
    }
    Spi::run(&format!(
        "GRANT EXECUTE ON FUNCTION public.{}(JSONB, JSONB) TO synchro_owner",
        fixture.impact_function
    ))
    .expect("grant impact function owner execute");
    if grant_worker {
        Spi::run(&format!(
            "GRANT EXECUTE ON FUNCTION public.{}(JSONB, JSONB) TO synchro_worker",
            fixture.impact_function
        ))
        .expect("grant impact function worker execute");
    }
}

fn enable_dependent_target_membership(fixture: &MembershipDependencyFixture) {
    ensure_authoritative_scopes(&["dependent-scope", "target-scope"]);
    Spi::run(&format!(
        "CREATE OR REPLACE FUNCTION public.{target_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT CASE WHEN EXISTS (
                 SELECT 1
                 FROM synchro_projection.{source_table} projection
                 WHERE projection.target_id #>> '{{}}' = p_key::text
                   AND NOT projection.deleted
             ) THEN 'dependent-scope'::text ELSE 'target-scope'::text END;
         END;
         SELECT synchro.synchro_register_table(
             'public.{target_table}',
             'public.{target_membership}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             p_affected_scopes => ARRAY['dependent-scope', 'target-scope']::text[]
         )",
        target_membership = fixture.target_membership,
        source_table = fixture.source_table,
        target_table = fixture.target_table,
    ))
    .expect("enable dependent target membership");
}

fn ensure_authoritative_scopes(scopes: &[&str]) {
    Spi::connect_mut(|client| {
        client.update(
            "INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
             SELECT scope_id, runtime.stream_generation
             FROM unnest($1::text[]) scope(scope_id)
             CROSS JOIN synchro.sync_runtime_state runtime
             WHERE runtime.singleton
             ON CONFLICT (scope_id) DO NOTHING",
            None,
            &[scopes.to_vec().into()],
        )?;
        Ok::<_, pgrx::spi::Error>(())
    })
    .expect("create authoritative membership fixture scopes");
}

fn target_row_expression(fixture: &MembershipDependencyFixture, value: &str) -> String {
    format!(
        "ROW('{}'::uuid, 'int', to_jsonb({value}))::synchro.synchro_row_ref",
        fixture.target_table_id
    )
}

fn register_dependency(fixture: &MembershipDependencyFixture, max_impact_rows: i32) {
    Spi::run(&format!(
        "SELECT synchro.synchro_register_membership_dependency(
             '{source_table}',
             '{target_table}',
             'public.{impact_function}',
             ARRAY['{field_id}']::text[],
             {max_impact_rows}
         )",
        source_table = fixture.source_table,
        target_table = fixture.target_table,
        impact_function = fixture.impact_function,
        field_id = fixture.dependency_field_id,
    ))
    .expect("register membership dependency");
}

fn reject_dependency_registration(fixture: &MembershipDependencyFixture) -> Result<(), pgrx::spi::Error> {
    Spi::run(&format!(
        "DO $test$
         DECLARE
             rejected boolean := false;
         BEGIN
             BEGIN
                 PERFORM synchro.synchro_register_membership_dependency(
                     '{source_table}',
                     '{target_table}',
                     'public.{impact_function}',
                     ARRAY['{field_id}']::text[],
                     2
                 );
             EXCEPTION WHEN OTHERS THEN
                 rejected := true;
             END;
             IF NOT rejected THEN
                 RAISE EXCEPTION 'membership dependency unexpectedly succeeded';
             END IF;
         END
         $test$",
        source_table = fixture.source_table,
        target_table = fixture.target_table,
        impact_function = fixture.impact_function,
        field_id = fixture.dependency_field_id,
    ))
}

fn pending_dependency_count(fixture: &MembershipDependencyFixture) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)
         FROM synchro.sync_membership_dependencies dependency
         JOIN synchro.sync_registry_generations generation
           ON generation.generation = dependency.registry_generation
         WHERE generation.state = 'pending'
           AND dependency.dependency_relation_id = '{source_relation_id}'::uuid
           AND dependency.target_relation_id = '{target_relation_id}'::uuid",
        source_relation_id = fixture.source_relation_id,
        target_relation_id = fixture.target_relation_id,
    ))
    .expect("pending membership dependency count query")
    .expect("pending membership dependency count")
}

fn active_dependency(
    fixture: &MembershipDependencyFixture,
) -> (crate::registry::TableRegistration, crate::registry::MembershipDependency) {
    Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)?;
        let target = registry
            .iter()
            .find(|registration| registration.relation_id == fixture.target_relation_id)
            .cloned()
            .expect("active membership target relation");
        let dependencies = crate::registry::load_membership_dependencies_from_client(
            client,
            target.registry_generation,
            &registry,
        )?;
        let dependency = dependencies
            .into_iter()
            .find(|dependency| {
                dependency.dependency_relation_id == fixture.source_relation_id
                    && dependency.target_relation_id == fixture.target_relation_id
            })
            .expect("active membership dependency");
        Ok::<_, pgrx::spi::Error>((target, dependency))
    })
    .expect("load active membership dependency")
}

fn resolve_fixture_impacts(
    fixture: &MembershipDependencyFixture,
    old_row: Option<&serde_json::Value>,
    new_row: Option<&serde_json::Value>,
) -> Result<Vec<String>, ()> {
    let (target, dependency) = active_dependency(fixture);
    Spi::connect(|client| {
        let impacts = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            crate::bucketing::resolve_dependency_impacts(
                client,
                &dependency,
                &target,
                old_row,
                new_row,
            )
            .map_err(|_| ())
        }))
        .catch_others(|_| Err(()))
        .execute();
        Ok::<_, pgrx::spi::Error>(impacts)
    })
    .expect("resolve membership dependency impacts")
}

fn cleanup_membership_fixture(fixture: &MembershipDependencyFixture) {
    Spi::run(&format!(
        "SELECT synchro.synchro_unregister_table('{}');
         SELECT synchro.synchro_unregister_table('{}')",
        fixture.target_table, fixture.source_table
    ))
    .expect("unregister membership fixture relations");
    activate_pending_registry_for_test();
    Spi::run(&format!(
        "DROP TABLE IF EXISTS public.{source_table} CASCADE;
         DROP TABLE IF EXISTS public.{target_table} CASCADE;
         DROP FUNCTION IF EXISTS public.{source_membership}(INTEGER);
         DROP FUNCTION IF EXISTS public.{target_membership}(INTEGER);
         DROP FUNCTION IF EXISTS public.{impact_function}(JSONB, JSONB)",
        source_table = fixture.source_table,
        target_table = fixture.target_table,
        source_membership = fixture.source_membership,
        target_membership = fixture.target_membership,
        impact_function = fixture.impact_function,
    ))
    .expect("drop membership fixture objects");
}

fn create_capture_dependency_table(populated: bool) -> String {
    let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
        .expect("capture dependency suffix query")
        .expect("capture dependency suffix");
    let table = format!("capture_dependency_{suffix}");
    let policy = format!("capture_dependency_policy_{suffix}");
    Spi::run(&format!(
        "CREATE TABLE public.{table} (
             id INTEGER PRIMARY KEY,
             target_id INTEGER NOT NULL,
             internal_note TEXT
         );
         GRANT SELECT ON TABLE public.{table} TO synchro_owner;
         ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;
         CREATE POLICY {policy} ON public.{table}
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true);"
    ))
    .expect("create capture dependency table");
    if populated {
        Spi::run(&format!(
            "INSERT INTO public.{table} (id, target_id, internal_note)
             VALUES (1, 7, 'not captured')"
        ))
        .expect("populate capture dependency table");
    }
    table
}

fn register_capture_dependency_table(table: &str) {
    Spi::run(&format!(
        "SELECT synchro.synchro_register_capture_dependency(
             'public.{table}', ARRAY['id']::text[], ARRAY['target_id']::text[]
         )"
    ))
    .expect("register capture dependency table");
    activate_pending_registry_for_test();
}

#[pg_test]
fn capture_dependency_rejects_unqualified_physical_relation() {
    let table = create_capture_dependency_table(false);
    let registration = Spi::run(&format!(
        "DO $test$
         DECLARE
             rejected boolean := false;
         BEGIN
             BEGIN
                 PERFORM synchro.synchro_register_capture_dependency(
                     '{table}', ARRAY['id']::text[], ARRAY['target_id']::text[]
                 );
             EXCEPTION WHEN OTHERS THEN
                 rejected := true;
             END;
             IF NOT rejected THEN
                 RAISE EXCEPTION 'unqualified capture dependency registration unexpectedly succeeded';
             END IF;
         END
         $test$"
    ));
    let registrations: Option<i64> = Spi::get_one(&format!(
        "SELECT count(*) FROM synchro.sync_registry WHERE physical_relation = '{table}'"
    ))
    .expect("capture dependency registration count");

    assert!(registration.is_ok());
    assert_eq!(registrations, Some(0));
}

fn active_capture_dependency_relation_id(table: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT registry.relation_id::text
         FROM synchro.sync_registry registry
         JOIN synchro.sync_registry_generations generation
           ON generation.generation = registry.registry_generation
         WHERE generation.state = 'active'
           AND registry.registration_kind = 'capture_dependency'
           AND registry.physical_relation = '{table}'"
    ))
    .expect("capture dependency relation query")
    .expect("active capture dependency relation")
}

#[pg_test]
fn capture_dependency_registration_is_internal_and_fenced() {
    let table = create_capture_dependency_table(false);
    register_capture_dependency_table(&table);
    let relation_id = active_capture_dependency_relation_id(&table);

    let registration: pgrx::JsonB = Spi::get_one_with_args(
        "SELECT jsonb_build_object(
             'kind', registration_kind,
             'table_id_absent', table_id IS NULL,
             'field_count', (
                 SELECT count(*)
                 FROM synchro.sync_registry_fields field
                 WHERE field.registry_generation = registry.registry_generation
                   AND field.relation_id = registry.relation_id
             ),
             'capture_fields', (
                 SELECT jsonb_object_agg(physical_column, capture_key)
                 FROM synchro.sync_capture_dependency_fields field
                 WHERE field.registry_generation = registry.registry_generation
                   AND field.relation_id = registry.relation_id
             )
         )
         FROM synchro.sync_registry registry
         JOIN synchro.sync_registry_generations generation
           ON generation.generation = registry.registry_generation
         WHERE generation.state = 'active' AND registry.relation_id = $1::uuid",
        &[relation_id.as_str().into()],
    )
    .expect("capture dependency registration query")
    .expect("capture dependency registration");
    assert_eq!(registration.0["kind"], json!("capture_dependency"));
    assert_eq!(registration.0["table_id_absent"], json!(true));
    assert_eq!(registration.0["field_count"], json!(0));
    assert_eq!(
        registration.0["capture_fields"],
        json!({"id": true, "target_id": false})
    );

    let client_surface_count: i64 = Spi::get_one_with_args(
        "SELECT count(*)
         FROM jsonb_array_elements(
             synchro.synchro_schema_manifest()->'manifest'->'tables'
         ) table_definition
         WHERE table_definition->>'relation_id' = $1",
        &[relation_id.as_str().into()],
    )
    .expect("capture dependency manifest query")
    .expect("capture dependency manifest count");
    assert_eq!(client_surface_count, 0);

    Spi::run(&format!(
        "INSERT INTO public.{table} (id, target_id, internal_note)
         VALUES (1, 7, 'not captured')"
    ))
    .expect("insert capture dependency source row");
    let fence: pgrx::JsonB = Spi::get_one_with_args(
        "SELECT jsonb_build_object(
             'kind', registration_kind,
             'table_id_absent', table_id IS NULL,
             'record_ids_absent', old_record_id IS NULL AND new_record_id IS NULL,
             'old_key', old_capture_key,
             'new_key', new_capture_key,
             'operation', operation
         )
         FROM synchro.sync_write_fences
         WHERE relation_id = $1::uuid",
        &[relation_id.as_str().into()],
    )
    .expect("capture dependency fence query")
    .expect("capture dependency fence");
    assert_eq!(fence.0["kind"], json!("capture_dependency"));
    assert_eq!(fence.0["table_id_absent"], json!(true));
    assert_eq!(fence.0["record_ids_absent"], json!(true));
    assert_eq!(fence.0["old_key"], serde_json::Value::Null);
    assert_eq!(fence.0["new_key"], json!({"id": 1}));
    assert_eq!(fence.0["operation"], json!("insert"));

    let direct_effect_count: i64 = Spi::get_one_with_args(
        "SELECT count(*) FROM synchro.sync_changelog WHERE relation_id = $1::uuid",
        &[relation_id.as_str().into()],
    )
    .expect("capture dependency direct effect query")
    .expect("capture dependency direct effect count");
    assert_eq!(direct_effect_count, 0);
}

#[pg_test]
fn capture_dependency_nonempty_stays_pending() {
    let table = create_capture_dependency_table(true);
    Spi::run(&format!(
        "SELECT synchro.synchro_register_capture_dependency(
             'public.{table}', ARRAY['id']::text[], ARRAY['target_id']::text[]
         )"
    ))
    .expect("stage nonempty capture dependency registration");

    let state: pgrx::JsonB = Spi::get_one::<pgrx::JsonB>(&format!(
        "SELECT jsonb_build_object(
             'generation', generation.generation,
             'state', generation.state,
             'validated', generation.validated,
             'active_exposure', EXISTS (
                 SELECT 1
                 FROM synchro.sync_registry active_registry
                 JOIN synchro.sync_registry_generations active_generation
                   ON active_generation.generation = active_registry.registry_generation
                 WHERE active_generation.state = 'active'
                   AND active_registry.physical_relation_oid = 'public.{table}'::regclass
             ),
             'trigger_count', (
                 SELECT count(*)
                 FROM pg_catalog.pg_trigger
                 WHERE tgrelid = 'public.{table}'::regclass AND NOT tgisinternal
             )
         )
         FROM synchro.sync_registry registry
         JOIN synchro.sync_registry_generations generation
           ON generation.generation = registry.registry_generation
         WHERE registry.physical_relation_oid = 'public.{table}'::regclass
         ORDER BY generation.generation DESC
         LIMIT 1"
    ))
    .expect("nonempty capture dependency state query")
    .expect("nonempty capture dependency state");
    let generation = state.0["generation"]
        .as_i64()
        .expect("pending capture dependency generation");
    let requires_bootstrap = Spi::connect(|client| {
        crate::schema::generation_requires_projection_bootstrap(client, generation)
    })
    .expect("classify nonempty capture dependency generation");

    assert_eq!(state.0["state"], json!("pending"));
    assert_eq!(state.0["validated"], json!(true));
    assert_eq!(state.0["active_exposure"], json!(false));
    assert!(state.0["trigger_count"].as_i64().unwrap_or(0) > 0);
    assert!(requires_bootstrap);

    Spi::run(&format!(
        "DO $control$
         BEGIN
             UPDATE synchro.sync_registry_generations
             SET state = 'active', validated = false, activated_at = clock_timestamp()
             WHERE generation = {generation};
             RAISE EXCEPTION 'unvalidated registry activation was accepted';
         EXCEPTION WHEN check_violation THEN
             NULL;
         END
         $control$"
    ))
    .expect("reject unvalidated registry activation");
    let preserved: pgrx::JsonB = Spi::get_one_with_args(
        "SELECT jsonb_build_object('state', state, 'validated', validated)
         FROM synchro.sync_registry_generations WHERE generation = $1",
        &[generation.into()],
    )
    .expect("read rejected registry activation")
    .expect("registry generation remains present");
    assert_eq!(preserved.0, json!({"state": "pending", "validated": true}));
}

#[pg_test]
fn membership_dependency_resolves_old_and_new_target_rows() {
    let fixture = membership_dependency_fixture();
    let old_row = json!({"target_id": 7});
    let new_row = json!({"target_id": 3});
    let body = format!(
        "SELECT {} WHERE old_row ? 'target_id'
         UNION ALL
         SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(old_row ->> 'target_id')::integer"),
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();
    enable_dependent_target_membership(&fixture);
    activate_pending_registry_for_test();

    let (target, dependency) = active_dependency(&fixture);
    let impacts = resolve_fixture_impacts(&fixture, Some(&old_row), Some(&new_row));
    cleanup_membership_fixture(&fixture);

    assert_eq!(target.table_id, fixture.target_table_id);
    assert_eq!(dependency.target_table_id, fixture.target_table_id);
    assert_eq!(dependency.target_relation_id, fixture.target_relation_id);
    assert_eq!(impacts, Ok(vec!["3".to_string(), "7".to_string()]));
}

/// Returns the role that owns a registered function.
fn registered_function_owner(signature: &str) -> String {
    Spi::get_one_with_args::<String>(
        "SELECT pg_catalog.pg_get_userbyid(proowner)::text
         FROM pg_catalog.pg_proc
         WHERE oid = $1::regprocedure",
        &[signature.into()],
    )
    .expect("registered function owner query")
    .expect("registered function owner")
}

#[pg_test]
fn membership_dependency_impact_runs_as_function_owner() {
    let fixture = membership_dependency_fixture();
    let body = target_row_expression(
        &fixture,
        "(SELECT role.oid::integer FROM pg_catalog.pg_roles role
          WHERE role.rolname = CURRENT_USER)",
    );
    create_impact_function(&fixture, &format!("SELECT {body}"), true, true);
    register_dependency(&fixture, 1);
    activate_pending_registry_for_test();
    let owner_oid = Spi::get_one_with_args::<i64>(
        "SELECT proowner::bigint FROM pg_catalog.pg_proc WHERE oid = $1::regprocedure",
        &[format!("public.{}(jsonb, jsonb)", fixture.impact_function).into()],
    )
    .expect("impact function owner query")
    .expect("impact function owner");

    Spi::run("SET LOCAL ROLE synchro_worker").expect("select worker role");
    let impacts = resolve_fixture_impacts(&fixture, None, None);
    Spi::run("RESET ROLE").expect("restore test role");
    cleanup_membership_fixture(&fixture);

    assert_eq!(impacts, Ok(vec![owner_oid.to_string()]));
}

#[pg_test]
fn membership_dependency_fingerprint_is_independent_of_search_path() {
    let fixture = membership_dependency_fixture();
    let body = format!(
        "SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 1);
    activate_pending_registry_for_test();

    Spi::run("SET LOCAL search_path = pg_catalog, public")
        .expect("set alternate fingerprint search path");
    let (_, dependency) = active_dependency(&fixture);
    let search_path = Spi::get_one::<String>("SELECT current_setting('search_path')")
        .expect("read restored fingerprint search path")
        .expect("restored fingerprint search path");
    Spi::run("SET LOCAL search_path = pg_catalog, synchro")
        .expect("restore membership test search path");
    cleanup_membership_fixture(&fixture);

    assert_eq!(dependency.target_relation_id, fixture.target_relation_id);
    assert_eq!(search_path, "pg_catalog, public");
}

#[pg_test]
fn membership_reregistration_preserves_declared_dependencies() {
    let fixture = membership_dependency_fixture();
    let body = format!(
        "SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 1);
    activate_pending_registry_for_test();

    enable_dependent_target_membership(&fixture);
    let pending_dependencies = pending_dependency_count(&fixture);
    activate_pending_registry_for_test();
    let (_, dependency) = active_dependency(&fixture);
    cleanup_membership_fixture(&fixture);

    assert_eq!(pending_dependencies, 1);
    assert_eq!(dependency.dependency_relation_id, fixture.source_relation_id);
    assert_eq!(dependency.target_relation_id, fixture.target_relation_id);
}

#[pg_test]
fn membership_dependency_activation_replaces_existing_edges() {
    let fixture = membership_dependency_fixture();
    Spi::run(&format!(
        "INSERT INTO public.{target_table} (id, label) VALUES (7, 'target');
         INSERT INTO public.{source_table} (id, target_id) VALUES (1, 7)",
        target_table = fixture.target_table,
        source_table = fixture.source_table,
    ))
    .expect("insert membership activation source rows");
    insert_changelog(
        "target-scope",
        &fixture.target_table,
        "7",
        1,
    );
    insert_changelog(
        "source-scope",
        &fixture.source_table,
        "1",
        1,
    );
    insert_edge(
        &fixture.target_table,
        "7",
        "target-scope",
    );
    insert_edge(
        &fixture.source_table,
        "1",
        "source-scope",
    );

    let body = format!(
        "SELECT {} WHERE old_row ? 'target_id'
         UNION ALL
         SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(old_row ->> 'target_id')::integer"),
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();
    enable_dependent_target_membership(&fixture);
    activate_pending_registry_for_test();

    let edges: Vec<String> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT bucket_id
             FROM synchro.sync_bucket_edges
             WHERE relation_id = $1::uuid AND record_id = '7'
             ORDER BY bucket_id",
            None,
            &[fixture.target_relation_id.as_str().into()],
        )?;
        rows.into_iter()
            .map(|row| {
                Ok::<_, pgrx::spi::Error>(
                    row.get_by_name::<String, &str>("bucket_id")?
                        .expect("membership edge bucket"),
                )
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .expect("load activated membership edges");
    let generations: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_object_agg(scope_id, membership_generation)
         FROM synchro.sync_scope_state
         WHERE scope_id IN ('target-scope', 'dependent-scope', 'source-scope')",
    )
    .expect("load activated membership generations")
    .expect("activated membership generations");
    let stage: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_build_object(
             'state', state,
             'verified', verified,
             'records', staged_record_count,
             'edges', staged_edge_count,
             'affected_scopes', affected_scopes
         )
         FROM synchro.sync_registry_membership_stages
         ORDER BY registry_generation DESC LIMIT 1",
    )
    .expect("load membership activation stage")
    .expect("membership activation stage");
    cleanup_membership_fixture(&fixture);

    assert_eq!(edges, vec!["dependent-scope"]);
    assert_eq!(generations.0["target-scope"], json!(2));
    assert_eq!(generations.0["dependent-scope"], json!(2));
    assert_eq!(generations.0["source-scope"], json!(1));
    assert_eq!(stage.0["state"], json!("activated"));
    assert_eq!(stage.0["verified"], json!(true));
    assert_eq!(stage.0["records"], json!(1));
    assert_eq!(stage.0["edges"], json!(1));
    assert_eq!(
        stage.0["affected_scopes"],
        json!(["dependent-scope", "target-scope"])
    );
}

#[pg_test]
fn empty_membership_rule_activation_uses_exact_declared_scopes() {
    let fixture = membership_dependency_fixture();
    ensure_authoritative_scopes(&["target-scope", "unrelated-scope"]);
    let before: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_object_agg(scope_id, membership_generation ORDER BY scope_id)
         FROM synchro.sync_scope_state
         WHERE scope_id IN ('target-scope', 'unrelated-scope')",
    )
    .expect("load membership generations before empty activation")
    .expect("membership generations before empty activation");

    Spi::run(&format!(
        "CREATE OR REPLACE FUNCTION public.{target_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT pg_catalog.concat('target-', 'scope')::text;
         END;
         SELECT synchro.synchro_register_table(
             'public.{target_table}',
             'public.{target_membership}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             p_affected_scopes => ARRAY['target-scope']::text[]
         )",
        target_membership = fixture.target_membership,
        target_table = fixture.target_table,
    ))
    .expect("stage empty membership rule activation");
    let pending_scopes: Vec<String> = Spi::get_one(
        "SELECT affected_scopes
         FROM synchro.sync_registry_membership_stages
         WHERE state = 'pending'
         ORDER BY registry_generation DESC
         LIMIT 1",
    )
    .expect("load pending empty membership affected scopes")
    .expect("pending empty membership affected scopes");

    activate_pending_registry_for_test();
    let after: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_object_agg(scope_id, membership_generation ORDER BY scope_id)
         FROM synchro.sync_scope_state
         WHERE scope_id IN ('target-scope', 'unrelated-scope')",
    )
    .expect("load membership generations after empty activation")
    .expect("membership generations after empty activation");
    let stage: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_build_object(
             'state', state,
             'records', staged_record_count,
             'edges', staged_edge_count,
             'affected_scopes', affected_scopes
         )
         FROM synchro.sync_registry_membership_stages
         ORDER BY registry_generation DESC
         LIMIT 1",
    )
    .expect("load activated empty membership stage")
    .expect("activated empty membership stage");
    cleanup_membership_fixture(&fixture);

    assert_eq!(before.0["target-scope"], json!(1));
    assert_eq!(before.0["unrelated-scope"], json!(1));
    assert_eq!(pending_scopes, vec!["target-scope"]);
    assert_eq!(after.0["target-scope"], json!(2));
    assert_eq!(after.0["unrelated-scope"], json!(1));
    assert_eq!(stage.0["state"], json!("activated"));
    assert_eq!(stage.0["records"], json!(0));
    assert_eq!(stage.0["edges"], json!(0));
    assert_eq!(stage.0["affected_scopes"], json!(["target-scope"]));
}

#[pg_test]
fn empty_membership_rule_activation_rejects_missing_declaration() {
    let fixture = membership_dependency_fixture();
    ensure_authoritative_scopes(&["target-scope"]);
    Spi::run(&format!(
        "CREATE OR REPLACE FUNCTION public.{target_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT pg_catalog.concat('target-', 'scope')::text;
         END;
         SELECT synchro.synchro_register_table(
             'public.{target_table}',
             'public.{target_membership}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled'
         )",
        target_membership = fixture.target_membership,
        target_table = fixture.target_table,
    ))
    .expect("stage empty membership rule without declaration");

    let activation = std::panic::catch_unwind(activate_pending_registry_for_test);

    assert!(activation.is_err());
}

#[pg_test]
fn membership_rule_activation_rejects_omitted_changed_scope() {
    let fixture = membership_dependency_fixture();
    Spi::run(&format!(
        "INSERT INTO public.{target_table} (id, label) VALUES (7, 'target');
         INSERT INTO public.{source_table} (id, target_id) VALUES (1, 7)",
        target_table = fixture.target_table,
        source_table = fixture.source_table,
    ))
    .expect("insert omitted-scope activation rows");
    insert_changelog("target-scope", &fixture.target_table, "7", 1);
    insert_changelog("source-scope", &fixture.source_table, "1", 1);
    insert_edge(&fixture.target_table, "7", "target-scope");
    insert_edge(&fixture.source_table, "1", "source-scope");
    ensure_authoritative_scopes(&["dependent-scope"]);

    // The membership function below reads the source projection, so the registry
    // requires a declared impact dependency before it accepts the registration.
    let impact_body = format!(
        "SELECT {} WHERE old_row ? 'target_id'
         UNION ALL
         SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(old_row ->> 'target_id')::integer"),
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &impact_body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();

    Spi::run(&format!(
        "CREATE OR REPLACE FUNCTION public.{target_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT CASE WHEN EXISTS (
                 SELECT 1
                 FROM synchro_projection.{source_table} projection
                 WHERE projection.target_id #>> '{{}}' = p_key::text
                   AND NOT projection.deleted
             ) THEN 'dependent-scope'::text ELSE 'target-scope'::text END;
         END;
         SELECT synchro.synchro_register_table(
             'public.{target_table}',
             'public.{target_membership}',
             'single_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             p_affected_scopes => ARRAY['target-scope']::text[]
         )",
        target_membership = fixture.target_membership,
        source_table = fixture.source_table,
        target_table = fixture.target_table,
    ))
    .expect("stage membership rule with omitted changed scope");

    let activation = std::panic::catch_unwind(activate_pending_registry_for_test);

    assert!(activation.is_err());
}

#[pg_test]
fn membership_dependency_rejects_public_impact_acl() {
    let fixture = membership_dependency_fixture();
    let body = format!("SELECT {}", target_row_expression(&fixture, "1"));
    create_impact_function(&fixture, &body, false, true);
    let registration = reject_dependency_registration(&fixture);
    let pending_count = pending_dependency_count(&fixture);
    cleanup_membership_fixture(&fixture);

    assert!(registration.is_ok());
    assert_eq!(pending_count, 0);
}

#[pg_test]
fn membership_dependency_rejects_duplicate_impact_rows() {
    let fixture = membership_dependency_fixture();
    let row = target_row_expression(&fixture, "7");
    let body = format!("SELECT {row} UNION ALL SELECT {row}");
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();

    let impacts = resolve_fixture_impacts(&fixture, None, None);
    cleanup_membership_fixture(&fixture);

    assert_eq!(impacts, Err(()));
}

#[pg_test]
fn membership_dependency_rejects_positive_row_bound_overflow() {
    let fixture = membership_dependency_fixture();
    let body = format!(
        "SELECT {} UNION ALL SELECT {}",
        target_row_expression(&fixture, "1"),
        target_row_expression(&fixture, "2"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 1);
    activate_pending_registry_for_test();

    let (_, dependency) = active_dependency(&fixture);
    let query = crate::bucketing::dependency_impact_query(
        &dependency.impact_function,
        dependency
            .max_impact_rows
            .checked_add(1)
            .expect("positive impact row limit"),
    );
    let materialized_rows = Spi::connect(|client| {
        let rows = client.select(
            &query,
            None,
            &[None::<pgrx::JsonB>.into(), None::<pgrx::JsonB>.into()],
        )?;
        Ok::<_, pgrx::spi::Error>(rows.into_iter().count())
    })
    .expect("materialize bounded dependency impacts");
    let impacts = resolve_fixture_impacts(&fixture, None, None);
    cleanup_membership_fixture(&fixture);

    assert!(query.ends_with("LIMIT 2"));
    assert_eq!(materialized_rows, 2);
    assert_eq!(impacts, Err(()));
}

#[pg_test]
fn membership_function_limits_rows_before_rust_rejection() {
    let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
        .expect("membership limit suffix query")
        .expect("membership limit suffix");
    let table = format!("membership_limit_{suffix}");
    let function = format!("membership_limit_function_{suffix}");
    let policy = format!("membership_limit_policy_{suffix}");

    Spi::run(&format!(
        "CREATE TABLE public.{table} (
             id INTEGER PRIMARY KEY,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         CREATE FUNCTION tests.{function}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT scope_id::text
             FROM pg_catalog.generate_series(1, 1000) AS generated(scope_id);
         END;
         REVOKE EXECUTE ON FUNCTION tests.{function}(INTEGER) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION tests.{function}(INTEGER)
             TO synchro_owner, synchro_worker;
         GRANT SELECT, INSERT, UPDATE ON TABLE public.{table} TO synchro_owner;
         ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;
         CREATE POLICY {policy} ON public.{table}
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true);"
    ))
    .expect("create membership limit fixture");
    Spi::run(&format!(
        "SELECT synchro.synchro_register_table(
             'public.{table}',
             'tests.{function}',
             'multi_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             '{{}}'::text[], '{{}}'::text[], 1
         )"
    ))
    .expect("register membership limit fixture");
    activate_pending_registry_for_test();

    let registration = Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)?;
        Ok::<_, pgrx::spi::Error>(registry
            .iter()
            .find(|registration| {
                registration.physical_schema == "public" && registration.physical_relation == table
            })
            .cloned()
            .expect("registered membership limit fixture"))
    })
    .expect("load registered membership limit fixture");
    let resolution = Spi::connect(|client| {
        let result = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            crate::materialize::resolve_membership_batch(client, &registration, &["1".to_string()])
                .map_err(|_| ())
        }))
        .catch_others(|_| Err(()))
        .execute();
        Ok::<_, pgrx::spi::Error>(result)
    })
    .expect("resolve registered membership limit fixture");
    let materialized_rows = Spi::connect(|client| {
        let rows = client.select(
            &crate::materialize::membership_batch_query(&registration)
                .expect("bounded membership query"),
            None,
            &[pgrx::JsonB(json!([{"record_id": "1"}])).into()],
        )?;
        Ok::<_, pgrx::spi::Error>(rows.into_iter().count())
    })
    .expect("materialize bounded registered membership results");

    Spi::run(&format!(
        "SELECT synchro.synchro_unregister_table('{table}')"
    ))
    .expect("unregister membership limit fixture");
    activate_pending_registry_for_test();
    Spi::run(&format!(
        "DROP FUNCTION tests.{function}(INTEGER);
         DROP TABLE public.{table};"
    ))
    .expect("drop membership limit fixture");

    assert_eq!(resolution, Err(()));
    assert_eq!(materialized_rows, 2);
}

struct RegisteredMembershipFixture {
    table: String,
    function: String,
    registration: crate::registry::TableRegistration,
}

/// Registers one relation whose membership function has the given body.
fn registered_membership_fixture(body: &str) -> RegisteredMembershipFixture {
    let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
        .expect("membership owner suffix query")
        .expect("membership owner suffix");
    let table = format!("membership_owner_{suffix}");
    let function = format!("membership_owner_function_{suffix}");
    let policy = format!("membership_owner_policy_{suffix}");

    Spi::run(&format!(
        "CREATE TABLE public.{table} (
             id INTEGER PRIMARY KEY,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         CREATE FUNCTION tests.{function}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             {body};
         END;
         REVOKE EXECUTE ON FUNCTION tests.{function}(INTEGER) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION tests.{function}(INTEGER)
             TO synchro_owner, synchro_worker;
         GRANT SELECT, INSERT, UPDATE ON TABLE public.{table} TO synchro_owner;
         ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;
         CREATE POLICY {policy} ON public.{table}
             AS PERMISSIVE FOR ALL TO synchro_owner
             USING (true) WITH CHECK (true);"
    ))
    .expect("create membership owner fixture");
    Spi::run(&format!(
        "SELECT synchro.synchro_register_table(
             'public.{table}',
             'tests.{function}',
             'multi_scope',
             'id', 'updated_at', 'deleted_at', 'enabled',
             '{{}}'::text[], '{{}}'::text[], 1
         )"
    ))
    .expect("register membership owner fixture");
    activate_pending_registry_for_test();
    let registration = Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)?;
        Ok::<_, pgrx::spi::Error>(registry
            .iter()
            .find(|registration| {
                registration.physical_schema == "public" && registration.physical_relation == table
            })
            .cloned()
            .expect("registered membership owner fixture"))
    })
    .expect("load registered membership owner fixture");
    RegisteredMembershipFixture {
        table,
        function,
        registration,
    }
}

fn drop_registered_membership_fixture(fixture: &RegisteredMembershipFixture) {
    Spi::run(&format!(
        "SELECT synchro.synchro_unregister_table('{}')",
        fixture.table
    ))
    .expect("unregister membership owner fixture");
    activate_pending_registry_for_test();
    Spi::run(&format!(
        "DROP FUNCTION tests.{}(INTEGER);
         DROP TABLE public.{};",
        fixture.function, fixture.table
    ))
    .expect("drop membership owner fixture");
}

/// Resolves one record as `synchro_worker` and catches an evaluation error,
/// as the worker does.
fn resolve_membership_as_worker(
    fixture: &RegisteredMembershipFixture,
) -> Result<std::collections::HashMap<String, Vec<String>>, String> {
    Spi::run("SET LOCAL ROLE synchro_worker").expect("select worker role");
    Spi::connect(|client| {
        Ok::<_, pgrx::spi::Error>(crate::materialize::resolve_membership_batch(
            client,
            &fixture.registration,
            &["1".to_string()],
        ))
    })
    .expect("resolve registered membership owner fixture")
}

#[pg_test]
fn membership_function_runs_as_function_owner() {
    let fixture = registered_membership_fixture("SELECT 'role:' || CURRENT_USER::text");
    let owner = registered_function_owner(&format!("tests.{}(integer)", fixture.function));

    let resolution = resolve_membership_as_worker(&fixture);
    Spi::run("RESET ROLE").expect("restore test role");
    drop_registered_membership_fixture(&fixture);

    assert_ne!(owner, "synchro_worker");
    assert_eq!(
        resolution,
        Ok(std::collections::HashMap::from([(
            "1".to_string(),
            vec![format!("role:{owner}")],
        )]))
    );
}

#[pg_test]
fn membership_evaluation_error_restores_caller_identity() {
    let fixture = registered_membership_fixture("SELECT (p_key / 0)::text");
    let search_path: String = Spi::get_one("SELECT current_setting('search_path')")
        .expect("caller search path query")
        .expect("caller search path");

    let resolution = resolve_membership_as_worker(&fixture);
    let caller: String = Spi::get_one("SELECT current_user::text")
        .expect("caller identity query")
        .expect("caller identity");
    let restored_path: String = Spi::get_one("SELECT current_setting('search_path')")
        .expect("restored search path query")
        .expect("restored search path");
    Spi::run("RESET ROLE").expect("restore test role");
    drop_registered_membership_fixture(&fixture);

    assert!(resolution.is_err());
    assert_eq!(caller, "synchro_worker");
    assert_eq!(restored_path, search_path);
}

#[pg_test]
fn projection_view_grants_select_to_relation_owner() {
    let owner = "synchro_projection_view_owner";
    Spi::run(&format!(
        "CREATE ROLE {owner} NOLOGIN NOSUPERUSER;
         GRANT synchro_operator TO {owner};
         CREATE TABLE public.projection_owner_items (
             id INTEGER PRIMARY KEY,
             label TEXT NOT NULL
         );
         ALTER TABLE public.projection_owner_items OWNER TO {owner}"
    ))
    .expect("create projection view owner fixture");
    let prepare_as_owner = || {
        Spi::run(&format!("SET LOCAL ROLE {owner}")).expect("select relation owner role");
        Spi::run(
            "SELECT synchro.synchro_prepare_projection_view(
                 'public.projection_owner_items', 'projection_owner_items',
                 ARRAY['id', 'label']::text[]
             )",
        )
        .expect("prepare projection view as relation owner");
        Spi::run("RESET ROLE").expect("restore test role");
    };
    let owner_can_read = || {
        Spi::get_one_with_args::<bool>(
            "SELECT has_table_privilege(
                 $1, 'synchro_projection.projection_owner_items', 'SELECT'
             )",
            &[owner.into()],
        )
        .expect("projection view owner privilege query")
        .expect("projection view owner privilege")
    };

    prepare_as_owner();
    let created = owner_can_read();
    Spi::run(&format!(
        "REVOKE SELECT ON synchro_projection.projection_owner_items FROM {owner}"
    ))
    .expect("revoke projection view select");
    let revoked = owner_can_read();
    prepare_as_owner();
    let prepared_again = owner_can_read();

    assert!(created);
    assert!(!revoked);
    assert!(prepared_again);
}

#[pg_test]
fn projection_view_name_collision_grants_no_select() {
    let victim = "synchro_projection_victim_owner";
    let intruder = "synchro_projection_intruder_owner";
    Spi::run(&format!(
        "CREATE ROLE {victim} NOLOGIN NOSUPERUSER;
         CREATE ROLE {intruder} NOLOGIN NOSUPERUSER;
         GRANT synchro_operator TO {victim}, {intruder};
         CREATE TABLE public.projection_victim_items (
             id INTEGER PRIMARY KEY,
             label TEXT NOT NULL
         );
         CREATE TABLE public.projection_decoy_items (
             id INTEGER PRIMARY KEY,
             label TEXT NOT NULL
         );
         ALTER TABLE public.projection_victim_items OWNER TO {victim};
         ALTER TABLE public.projection_decoy_items OWNER TO {intruder}"
    ))
    .expect("create projection view collision fixture");
    Spi::run(&format!("SET LOCAL ROLE {victim}")).expect("select victim role");
    Spi::run(
        "SELECT synchro.synchro_prepare_projection_view(
             'public.projection_victim_items', 'projection_victim_items',
             ARRAY['id', 'label']::text[]
         )",
    )
    .expect("prepare victim projection view");
    Spi::run("RESET ROLE").expect("restore test role");

    Spi::run(&format!(
        "SET LOCAL ROLE {intruder};
         SELECT set_config('synchro_test.projection_rejected', 'false', true);
         DO $test$
         BEGIN
             PERFORM synchro.synchro_prepare_projection_view(
                 'public.projection_decoy_items', 'projection_victim_items',
                 ARRAY['id', 'label']::text[]
             );
         EXCEPTION WHEN OTHERS THEN
             PERFORM set_config('synchro_test.projection_rejected', 'true', true);
         END
         $test$;
         RESET ROLE"
    ))
    .expect("attempt projection view name collision");
    let rejected = Spi::get_one::<String>(
        "SELECT current_setting('synchro_test.projection_rejected')",
    )
    .expect("projection collision result query")
    .expect("projection collision result");
    let intruder_can_read = Spi::get_one_with_args::<bool>(
        "SELECT has_table_privilege(
             $1, 'synchro_projection.projection_victim_items', 'SELECT'
         )",
        &[intruder.into()],
    )
    .expect("intruder projection privilege query")
    .expect("intruder projection privilege");

    assert!(!intruder_can_read);
    assert_eq!(rejected, "true");
}

#[pg_test]
fn projection_view_preparation_waits_for_registry_writer() {
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
    let suffix: String = Spi::get_one("SELECT replace(gen_random_uuid()::text, '-', '')")
        .expect("projection lock order suffix query")
        .expect("projection lock order suffix");
    let writer = "synchro_projection_lock_writer";
    let preparer = "synchro_projection_lock_preparer";
    let view = format!("pl_view_{suffix}");
    let writer_table = format!("pl_writer_{suffix}");
    let preparer_table = format!("pl_preparer_{suffix}");
    for (connection, table) in [(writer, &writer_table), (preparer, &preparer_table)] {
        Spi::run_with_args(
            "SELECT public.dblink_connect($1, $2)",
            &[connection.into(), connection_string.as_str().into()],
        )
        .unwrap();
        dblink_exec(connection, "BEGIN");
        dblink_exec(
            connection,
            &format!(
                "CREATE TABLE public.{table} (
                     id UUID PRIMARY KEY,
                     label TEXT NOT NULL DEFAULT '',
                     updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                     deleted_at TIMESTAMPTZ
                 );
                 CREATE FUNCTION public.{table}_scope(p_key UUID)
                 RETURNS SETOF text
                 LANGUAGE sql
                 STABLE
                 SECURITY INVOKER
                 SET search_path = pg_catalog, synchro
                 BEGIN ATOMIC
                     SELECT 'global'::text;
                 END;
                 REVOKE EXECUTE ON FUNCTION public.{table}_scope(UUID) FROM PUBLIC;
                 GRANT EXECUTE ON FUNCTION public.{table}_scope(UUID)
                     TO synchro_owner, synchro_worker;
                 GRANT SELECT ON TABLE public.{table} TO synchro_owner;
                 ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;
                 CREATE POLICY {table}_policy ON public.{table}
                     AS PERMISSIVE FOR ALL TO synchro_owner
                     USING (true) WITH CHECK (true)"
            ),
        );
    }
    let prepare = |table: &str| {
        format!(
            "SELECT synchro.synchro_prepare_projection_view(
                 'public.{table}', '{view}', ARRAY['label']::text[]
             )::text"
        )
    };

    dblink_query(
        writer,
        &format!(
            "SELECT synchro.synchro_register_table(
                 'public.{writer_table}', 'public.{writer_table}_scope', 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'read_only'
             )::text"
        ),
    );
    let writer_pid: i32 = dblink_query(writer, "SELECT pg_backend_pid()")
        .parse()
        .expect("parse registry writer PID");
    let preparer_pid: i32 = dblink_query(preparer, "SELECT pg_backend_pid()")
        .parse()
        .expect("parse projection preparer PID");
    let sent: i32 = Spi::get_one_with_args(
        "SELECT public.dblink_send_query($1, $2)",
        &[preparer.into(), prepare(&preparer_table).as_str().into()],
    )
    .unwrap()
    .expect("send projection preparation");

    let mut waiting = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        waiting = Spi::get_one_with_args(
            "SELECT EXISTS (
                        SELECT 1 FROM pg_locks
                        WHERE pid = $1 AND locktype = 'advisory' AND NOT granted
                    )
                    AND $2 = ANY (pg_catalog.pg_blocking_pids($1))",
            &[preparer_pid.into(), writer_pid.into()],
        )
        .unwrap()
        .unwrap_or(false);
        let busy: Option<i32> =
            Spi::get_one_with_args("SELECT public.dblink_is_busy($1)", &[preparer.into()])
                .unwrap();
        if waiting || busy == Some(0) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // A preparer that wrote the view before the registry lock would block
    // this same-name preparation while it waits for the writer.
    let writer_view = waiting.then(|| dblink_query(writer, &prepare(&writer_table)));
    dblink_exec(writer, "ROLLBACK");
    let preparer_view: Option<String> = Spi::get_one_with_args(
        "SELECT result
         FROM public.dblink_get_result($1, false) AS result_row(result text)",
        &[preparer.into()],
    )
    .unwrap();
    let preparer_error: String = Spi::get_one_with_args(
        "SELECT public.dblink_error_message($1)",
        &[preparer.into()],
    )
    .unwrap()
    .unwrap_or_else(|| "error-status-missing".to_string());
    Spi::run_with_args(
        "SELECT result
         FROM public.dblink_get_result($1, false) AS result_row(result text)",
        &[preparer.into()],
    )
    .unwrap();
    dblink_exec(preparer, "ROLLBACK");
    for connection in [writer, preparer] {
        Spi::run_with_args("SELECT public.dblink_disconnect($1)", &[connection.into()])
            .unwrap();
    }

    assert_eq!(sent, 1);
    assert!(waiting, "projection preparation did not wait for the registry writer");
    let expected_view = format!("synchro_projection.{view}");
    assert!(writer_view.is_some_and(|result| result.contains(&expected_view)));
    assert_eq!(preparer_error, "OK");
    assert!(preparer_view.is_some_and(|result| result.contains(&expected_view)));
}

#[pg_test]
fn projection_view_reads_only_its_bound_relation_after_rename() {
    Spi::run(
        "CREATE TABLE public.projection_rename_items (
             id UUID PRIMARY KEY,
             owner_id TEXT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         SELECT synchro.synchro_prepare_projection_view(
             'public.projection_rename_items', 'projection_rename_items',
             ARRAY['owner_id']::text[]
         );
         ALTER TABLE public.projection_rename_items RENAME TO projection_rename_decoy;
         CREATE TABLE public.projection_rename_items (
             id UUID PRIMARY KEY,
             owner_id TEXT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         SELECT tests.register_test_table(
             'projection_rename_items', $$SELECT 'global'$$, 'single_scope',
             'id', 'updated_at', 'deleted_at', 'read_only'
         )",
    )
    .expect("create renamed projection relation fixture");
    activate_pending_registry_for_test();
    Spi::run(
        "INSERT INTO synchro.sync_captured_rows (
             relation_id, record_id, row_data, row_version, checksum, deleted,
             source_stream_generation, source_commit_lsn, source_event_ordinal,
             registry_generation
         )
         SELECT registry.relation_id, '00000000-0000-4000-8b47-000000000001',
                jsonb_build_object('owner_id', 'new-owner'),
                '00000000-0000-4000-8b47-000000000002'::uuid,
                decode(repeat('ab', 32), 'hex'), false,
                runtime.stream_generation, '0/10'::pg_lsn, 0,
                registry.registry_generation
         FROM synchro.sync_runtime_state runtime
         JOIN synchro.sync_registry_generations generation
           ON generation.stream_generation = runtime.stream_generation
          AND generation.state = 'active'
         JOIN synchro.sync_registry registry
           ON registry.registry_generation = generation.generation
          AND registry.table_name = 'projection_rename_items'
         WHERE runtime.singleton",
    )
    .expect("capture a row of the new relation");
    let new_relation_rows = Spi::get_one::<i64>(
        "SELECT count(*) FROM synchro.sync_current_projections
         WHERE record_id = '00000000-0000-4000-8b47-000000000001'",
    )
    .expect("new relation projection query")
    .expect("new relation projection count");
    let old_view_rows =
        Spi::get_one::<i64>("SELECT count(*) FROM synchro_projection.projection_rename_items")
            .expect("old projection view query")
            .expect("old projection view count");

    assert_eq!(new_relation_rows, 1);
    assert_eq!(old_view_rows, 0);
}

#[pg_test]
fn projection_view_update_rebuild_matches_prepared_definition() {
    Spi::run(
        "CREATE TABLE public.projection_rebuild_items (
             id INTEGER PRIMARY KEY,
             owner_id TEXT NOT NULL,
             \"Label\" TEXT NOT NULL
         );
         SELECT synchro.synchro_prepare_projection_view(
             'public.projection_rebuild_items', 'projection_rebuild_items',
             ARRAY['owner_id', 'Label']::text[]
         )",
    )
    .expect("create projection rebuild fixture");
    let definition = || {
        Spi::get_one::<String>(
            "SELECT pg_get_viewdef('synchro_projection.projection_rebuild_items'::regclass, true)",
        )
        .expect("projection view definition query")
        .expect("projection view definition")
    };
    let prepared = definition();
    Spi::run(
        "CREATE OR REPLACE VIEW synchro_projection.projection_rebuild_items
         WITH (security_barrier = true) AS
         SELECT projection.record_id, projection.capture_key, projection.deleted,
                NULL::jsonb AS \"Label\", NULL::jsonb AS owner_id
         FROM synchro.sync_current_projections projection
         WHERE false",
    )
    .expect("replace projection view with a prior definition");
    let prior = definition();
    let script = include_str!("../../sql/synchro_pg--0.3.2--0.4.0-rc.1.sql");
    let rebuild = &script[script
        .find("DO $rebuild$")
        .expect("update script rebuilds projection views")..];
    let rebuild = &rebuild[..rebuild
        .find("\n$rebuild$;")
        .map(|end| end + "\n$rebuild$;".len())
        .expect("update script ends the projection view rebuild")];
    Spi::run(rebuild).expect("rebuild projection views as the update does");
    let rebuilt = definition();

    assert_ne!(prior, prepared);
    assert_eq!(rebuilt, prepared);
}

#[pg_test]
fn projection_view_rejects_reader_without_source_access() {
    let former = "synchro_projection_former_owner";
    let current = "synchro_projection_current_owner";
    Spi::run(&format!(
        "CREATE ROLE {former} NOLOGIN NOSUPERUSER;
         CREATE ROLE {current} NOLOGIN NOSUPERUSER;
         GRANT synchro_operator TO {former}, {current};
         DO $grant$ BEGIN
             EXECUTE format('GRANT CREATE ON DATABASE %I TO {current}', current_database());
         END $grant$;
         CREATE TABLE public.projection_reader_items (
             id UUID PRIMARY KEY,
             owner_id TEXT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             deleted_at TIMESTAMPTZ
         );
         ALTER TABLE public.projection_reader_items OWNER TO {former};
         SET LOCAL ROLE {former};
         SELECT synchro.synchro_prepare_projection_view(
             'public.projection_reader_items', 'projection_reader_items',
             ARRAY['owner_id']::text[]
         );
         RESET ROLE;
         ALTER TABLE public.projection_reader_items OWNER TO {current};
         REVOKE ALL ON public.projection_reader_items FROM {former};
         GRANT SELECT ON public.projection_reader_items TO synchro_owner, synchro_worker;
         ALTER TABLE public.projection_reader_items ENABLE ROW LEVEL SECURITY;
         CREATE POLICY projection_reader_items_owner ON public.projection_reader_items
             AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
         CREATE FUNCTION public.projection_reader_membership(p_key UUID)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT 'global'::text;
         END;
         ALTER FUNCTION public.projection_reader_membership(UUID) OWNER TO {current};
         REVOKE EXECUTE ON FUNCTION public.projection_reader_membership(UUID) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION public.projection_reader_membership(UUID)
             TO synchro_owner, synchro_worker;
         SET LOCAL ROLE {current};
         SELECT synchro.synchro_register_table(
             'public.projection_reader_items', 'public.projection_reader_membership',
             'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only'
         );
         SELECT synchro.synchro_prepare_projection_view(
             'public.projection_reader_items', 'projection_reader_items',
             ARRAY['owner_id']::text[]
         );
         RESET ROLE"
    ))
    .expect("create transferred projection relation fixture");
    activate_pending_registry_for_test();
    Spi::run(
        "INSERT INTO synchro.sync_captured_rows (
             relation_id, record_id, row_data, row_version, checksum, deleted,
             source_stream_generation, source_commit_lsn, source_event_ordinal,
             registry_generation
         )
         SELECT registry.relation_id, '00000000-0000-4000-8b47-000000000011',
                jsonb_build_object('owner_id', 'current-owner'),
                '00000000-0000-4000-8b47-000000000012'::uuid,
                decode(repeat('cd', 32), 'hex'), false,
                runtime.stream_generation, '0/10'::pg_lsn, 0,
                registry.registry_generation
         FROM synchro.sync_runtime_state runtime
         JOIN synchro.sync_registry_generations generation
           ON generation.stream_generation = runtime.stream_generation
          AND generation.state = 'active'
         JOIN synchro.sync_registry registry
           ON registry.registry_generation = generation.generation
          AND registry.table_name = 'projection_reader_items'
         WHERE runtime.singleton",
    )
    .expect("capture a row of the transferred relation");
    let rows_as = |role: &str| {
        Spi::run(&format!("SET LOCAL ROLE {role}")).expect("select projection reader role");
        let rows = Spi::get_one::<i64>(
            "SELECT count(*) FROM synchro_projection.projection_reader_items",
        )
        .expect("projection reader query")
        .expect("projection reader count");
        Spi::run("RESET ROLE").expect("restore test role");
        rows
    };
    Spi::run(&format!(
        "SET LOCAL ROLE {former};
         SELECT set_config('synchro_test.projection_reader', 'none', true);
         DO $test$
         DECLARE visible_rows bigint;
         BEGIN
             SELECT count(*) INTO visible_rows FROM synchro_projection.projection_reader_items;
             PERFORM set_config('synchro_test.projection_reader', 'rows:' || visible_rows, true);
         EXCEPTION WHEN insufficient_privilege THEN
             PERFORM set_config('synchro_test.projection_reader', 'rejected', true);
         END
         $test$;
         RESET ROLE"
    ))
    .expect("read projection view as former owner");
    let former_result = Spi::get_one::<String>(
        "SELECT current_setting('synchro_test.projection_reader')",
    )
    .expect("former owner result query")
    .expect("former owner result");

    assert_eq!(former_result, "rejected");
    assert_eq!(rows_as(current), 1);
    assert_eq!(rows_as("synchro_worker"), 1);
    assert_eq!(rows_as("synchro_owner"), 1);
}

#[pg_test]
fn registration_rejects_owner_without_projection_source_access() {
    let reader = "synchro_projection_source_reader";
    Spi::run(&format!(
        "CREATE ROLE {reader} NOLOGIN NOSUPERUSER;
         GRANT synchro_operator TO {reader};
         DO $grant$ BEGIN
             EXECUTE format('GRANT CREATE ON DATABASE %I TO {reader}', current_database());
         END $grant$"
    ))
    .expect("create projection source reader");
    let fixture = membership_dependency_fixture();
    let impact = format!(
        "SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &impact, true, true);
    register_dependency(&fixture, 1);
    activate_pending_registry_for_test();
    ensure_authoritative_scopes(&["dependent-scope", "target-scope"]);
    Spi::run(&format!(
        "DO $transfer$ BEGIN
             EXECUTE (
                 SELECT format('ALTER PUBLICATION %I OWNER TO {reader}', publication.pubname)
                 FROM pg_catalog.pg_publication publication
             );
         END $transfer$;
         ALTER TABLE public.{source_table} OWNER TO {reader};
         ALTER TABLE public.{target_table} OWNER TO {reader};
         ALTER FUNCTION public.{source_membership}(INTEGER) OWNER TO {reader};
         ALTER FUNCTION public.{target_membership}(INTEGER) OWNER TO {reader};
         ALTER FUNCTION public.{impact_function}(JSONB, JSONB) OWNER TO {reader};
         GRANT SELECT ON synchro_projection.{source_table} TO {reader};
         CREATE OR REPLACE FUNCTION public.{target_membership}(p_key INTEGER)
         RETURNS SETOF text
         LANGUAGE sql
         STABLE
         SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT CASE WHEN EXISTS (
                 SELECT 1
                 FROM synchro_projection.{source_table} projection
                 WHERE projection.target_id #>> '{{}}' = p_key::text
                   AND NOT projection.deleted
             ) THEN 'dependent-scope'::text ELSE 'target-scope'::text END;
         END;
         REVOKE SELECT ON public.{source_table} FROM {reader}",
        source_table = fixture.source_table,
        target_table = fixture.target_table,
        source_membership = fixture.source_membership,
        target_membership = fixture.target_membership,
        impact_function = fixture.impact_function,
    ))
    .expect("transfer projection source fixture to operator");
    let register = || {
        Spi::run(&format!(
            "SET LOCAL ROLE {reader};
             DO $test$
             BEGIN
                 PERFORM synchro.synchro_register_table(
                     'public.{target_table}',
                     'public.{target_membership}',
                     'single_scope',
                     'id', 'updated_at', 'deleted_at', 'enabled',
                     p_affected_scopes => ARRAY['dependent-scope', 'target-scope']::text[]
                 );
                 PERFORM set_config('synchro_test.projection_source_registered', 'registered', true);
             EXCEPTION WHEN OTHERS THEN
                 PERFORM set_config('synchro_test.projection_source_registered', SQLERRM, true);
             END
             $test$;
             RESET ROLE",
            target_table = fixture.target_table,
            target_membership = fixture.target_membership,
        ))
        .expect("attempt projection source registration");
        Spi::get_one::<String>("SELECT current_setting('synchro_test.projection_source_registered')")
            .expect("projection source registration result query")
            .expect("projection source registration result")
    };

    let without_source_access = register();
    Spi::run(&format!(
        "GRANT SELECT ON public.{} TO {reader}",
        fixture.source_table
    ))
    .expect("grant projection source access");
    let with_source_access = register();

    assert_ne!(without_source_access, "registered");
    assert_eq!(with_source_access, "registered");
}

#[pg_test]
fn membership_test_schema_enforces_production_validation() {
    // Registration rejects each dependency that the catalog shows. It cannot see
    // what a catalog function reads, so it accepts a session setting that the
    // contract forbids. That acceptance does not make the function supported.
    for case in [
        "valid",
        "unparsed",
        "search_path",
        "live_table",
        "undeclared_field",
        "application_function",
        "session_setting",
    ] {
        let fixture = registration_fixture(true, "enabled", true);
        let table = &fixture.table;
        let function = &fixture.function;
        Spi::run(&format!(
            "ALTER TABLE public.{table} ADD COLUMN private_note TEXT;
             SELECT synchro.synchro_prepare_projection_view(
                 'public.{table}', '{table}', ARRAY['id', 'private_note']
             );
             ALTER FUNCTION public.{function}(UUID) SET SCHEMA tests"
        ))
        .expect("prepare membership validation fixture");
        let definition = match case {
            "unparsed" => format!(
                "SET search_path = pg_catalog, synchro
                 AS $$SELECT 'registration' FROM synchro_projection.{table}
                      WHERE record_id = p_key::text$$"
            ),
            "search_path" => format!(
                "SET search_path = pg_catalog, public
                 BEGIN ATOMIC
                     SELECT 'registration' FROM synchro_projection.{table}
                     WHERE record_id = p_key::text;
                 END"
            ),
            "live_table" => format!(
                "SET search_path = pg_catalog, synchro
                 BEGIN ATOMIC
                     SELECT 'registration' FROM public.{table} WHERE id = p_key;
                 END"
            ),
            "undeclared_field" => format!(
                "SET search_path = pg_catalog, synchro
                 BEGIN ATOMIC
                     SELECT private_note #>> '{{}}' FROM synchro_projection.{table}
                     WHERE record_id = p_key::text;
                 END"
            ),
            "application_function" => {
                Spi::run(&format!(
                    "CREATE FUNCTION tests.{function}_scope() RETURNS text
                     LANGUAGE SQL IMMUTABLE RETURN 'registration'"
                ))
                .expect("create application scope function");
                format!(
                    "SET search_path = pg_catalog, synchro
                     BEGIN ATOMIC
                         SELECT tests.{function}_scope() FROM synchro_projection.{table}
                         WHERE record_id = p_key::text;
                     END"
                )
            }
            "session_setting" => format!(
                "SET search_path = pg_catalog, synchro
                 BEGIN ATOMIC
                     SELECT pg_catalog.current_setting('synchro_test.scope', true)
                     FROM synchro_projection.{table}
                     WHERE record_id = p_key::text;
                 END"
            ),
            "valid" => format!(
                "SET search_path = pg_catalog, synchro
                 BEGIN ATOMIC
                     SELECT 'registration' FROM synchro_projection.{table}
                     WHERE record_id = p_key::text;
                 END"
            ),
            _ => unreachable!(),
        };
        Spi::run(&format!(
            "CREATE OR REPLACE FUNCTION tests.{function}(p_key UUID)
             RETURNS SETOF text LANGUAGE SQL STABLE SECURITY INVOKER {definition}"
        ))
        .expect("define membership validation case");
        let registration = format!(
            "synchro.synchro_register_table(
                 'public.{table}', 'tests.{function}', 'single_scope',
                 'id', 'updated_at', 'deleted_at', 'enabled', ARRAY['private_note']
             )"
        );
        if matches!(case, "valid" | "session_setting") {
            Spi::run(&format!("SELECT {registration}"))
                .expect("register production-valid test membership");
            assert_eq!(fixture_registry_count(&fixture), 1);
            Spi::run(&format!("SELECT synchro.synchro_unregister_table('{table}')"))
                .expect("unregister valid membership fixture");
        } else {
            Spi::run(&format!(
                "DO $test$
                 DECLARE rejected boolean := false;
                 BEGIN
                     BEGIN
                         PERFORM {registration};
                     EXCEPTION WHEN OTHERS THEN
                         rejected := true;
                     END;
                     IF NOT rejected THEN
                         RAISE EXCEPTION 'invalid membership accepted: {case}';
                     END IF;
                 END
                 $test$"
            ))
            .expect("reject invalid test membership");
            assert_eq!(fixture_registry_count(&fixture), 0, "{case}");
        }
        Spi::run(&format!(
            "DROP FUNCTION tests.{function}(UUID);
             DROP FUNCTION IF EXISTS tests.{function}_scope();
             DROP TABLE public.{table}"
        ))
        .expect("remove membership validation fixture");
    }
}

#[pg_test]
fn membership_uses_captured_values_instead_of_live_rows() {
    setup_test_tables();
    let record_id = "d7000000-0000-4000-8000-000000000001";
    Spi::run_with_args(
        "INSERT INTO test_orders (id, user_id) VALUES ($1::uuid, 'captured-owner')",
        &[record_id.into()],
    )
    .expect("insert projection membership source");
    insert_changelog("user:captured-owner", "test_orders", record_id, 1);
    Spi::run_with_args(
        "UPDATE test_orders SET user_id = 'later-owner' WHERE id = $1::uuid",
        &[record_id.into()],
    )
    .expect("change live membership source");
    let scopes = Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)
            .map_err(|error| error.to_string())?;
        let registration = registry
            .iter()
            .find(|registration| registration.table_name == "test_orders")
            .expect("projection membership registration");
        crate::materialize::resolve_membership_batch(client, registration, &[record_id.to_string()])
    })
    .expect("resolve captured membership");
    assert_eq!(scopes[record_id], vec!["user:captured-owner"]);
}

#[pg_test]
fn membership_accepts_empty_string_primary_key() {
    Spi::run(
        "CREATE TABLE test_empty_string_pk (
             id TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         SELECT synchro.synchro_prepare_projection_view(
             'public.test_empty_string_pk', 'test_empty_string_pk', ARRAY['id']
         );
         SELECT tests.register_test_table(
             'test_empty_string_pk',
             $$SELECT 'global' FROM synchro_projection.test_empty_string_pk WHERE record_id = p_key::text$$,
             'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only'
         )",
    )
    .expect("register empty string primary-key fixture");
    activate_pending_registry_for_test();
    let user_id = "empty-key-user";
    let client_id = "empty-key-client";
    register_shared_scope("global", false);
    register_client(user_id, client_id);
    Spi::run("INSERT INTO test_empty_string_pk (id, value) VALUES ('', 'empty key')")
        .expect("insert empty string primary key");
    insert_edge("test_empty_string_pk", "", "global");
    insert_changelog("global", "test_empty_string_pk", "", 1);
    let scopes = Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)
            .map_err(|error| error.to_string())?;
        let registration = registry
            .iter()
            .find(|registration| registration.physical_relation == "test_empty_string_pk")
            .expect("empty string primary-key registration");
        crate::materialize::resolve_membership_batch(
            client,
            registration,
            &["absent".to_string(), "".to_string()],
        )
    })
    .expect("resolve empty string primary-key membership");

    assert!(scopes["absent"].is_empty());
    assert_eq!(scopes[""], vec!["global"]);
    let response = pull_client(
        user_id,
        client_id,
        1,
        json!({"global": scope_cursor_ref(user_id, client_id, "global", 0)}),
        100,
    );
    assert!(response.get("error").is_none(), "{response}");
    let changes = response["changes"].as_array().expect("empty-key pull changes");
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0]["pk"].get(field_id("test_empty_string_pk", "id")),
        Some(&json!(""))
    );
    assert_eq!(
        changes[0]["row"].get(field_id("test_empty_string_pk", "value")),
        Some(&json!("empty key"))
    );
    let backfill: pgrx::JsonB = Spi::get_one(
        "SELECT synchro_backfill_bucket_edges('test_empty_string_pk', 1)",
    )
    .expect("backfill the empty primary key")
    .expect("empty primary-key backfill response");
    assert_eq!(backfill.0["records"], 1);
    assert_eq!(backfill.0["edges"], 1);

    configure_reset_test_slot("synchro_empty_key_old");
    let prepared = prepare_reset_for_test("synchro_empty_key_candidate");
    let id = reset_id(&prepared);
    lock_and_stage_reset(&id, "synchro_empty_key_candidate");
    let staged: i64 = Spi::get_one_with_args(
        "SELECT count(*) FROM synchro.sync_stream_reset_membership_edges
         WHERE reset_id = $1::uuid AND table_name = 'test_empty_string_pk'
           AND record_id = '' AND scope_id = 'global'",
        &[id.as_str().into()],
    )
    .expect("read the staged empty primary key")
    .expect("staged empty primary-key count");
    assert_eq!(staged, 1);
    Spi::connect_mut(|client| crate::stream_reset::abort_stream_reset_for_test(client, &id))
        .expect("abort empty primary-key reset");
}

#[pg_test]
fn membership_function_fails_closed_when_query_limit_overflows() {
    setup_test_tables();
    let mut registration = crate::registry::load_registry()
        .expect("load membership overflow registry")
        .pop()
        .expect("membership overflow registration");
    registration.max_scope_fanout = i32::MAX;
    let resolution = Spi::connect(|client| {
        crate::materialize::resolve_membership_batch(client, &registration, &["1".to_string()])
    });
    assert!(resolution.is_err());
}

/// Pull at `pull.rs:1124` and rebuild at `rebuild.rs:673` both recompute a
/// captured digest under the active registration and reject a mismatch. A
/// membership activation must therefore leave every captured digest valid under
/// the registration that those readers use.
#[pg_test]
fn membership_activation_keeps_captured_digests_valid() {
    let fixture = membership_dependency_fixture();
    Spi::run(&format!(
        "INSERT INTO public.{target_table} (id, label) VALUES (7, 'target');
         INSERT INTO public.{source_table} (id, target_id) VALUES (1, 7)",
        target_table = fixture.target_table,
        source_table = fixture.source_table,
    ))
    .expect("insert membership digest source rows");
    insert_changelog("target-scope", &fixture.target_table, "7", 1);
    insert_edge(&fixture.target_table, "7", "target-scope");

    let body = format!(
        "SELECT {} WHERE old_row ? 'target_id'
         UNION ALL
         SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(old_row ->> 'target_id')::integer"),
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();
    enable_dependent_target_membership(&fixture);
    activate_pending_registry_for_test();

    let stored: Vec<u8> = Spi::get_one_with_args(
        "SELECT checksum FROM synchro.sync_captured_rows
         WHERE relation_id = $1::uuid AND record_id = '7'",
        &[fixture.target_relation_id.as_str().into()],
    )
    .expect("load stored captured digest")
    .expect("stored captured digest");
    let row_data: pgrx::JsonB = Spi::get_one_with_args(
        "SELECT row_data FROM synchro.sync_captured_rows
         WHERE relation_id = $1::uuid AND record_id = '7'",
        &[fixture.target_relation_id.as_str().into()],
    )
    .expect("load captured row data")
    .expect("captured row data");
    let row_version: String = Spi::get_one_with_args(
        "SELECT row_version::text FROM synchro.sync_captured_rows
         WHERE relation_id = $1::uuid AND record_id = '7'",
        &[fixture.target_relation_id.as_str().into()],
    )
    .expect("load captured row version")
    .expect("captured row version");
    let captured_generation: i64 = Spi::get_one_with_args(
        "SELECT registry_generation FROM synchro.sync_captured_rows
         WHERE relation_id = $1::uuid AND record_id = '7'",
        &[fixture.target_relation_id.as_str().into()],
    )
    .expect("load captured generation")
    .expect("captured generation");
    let active_generation: i64 = Spi::get_one(
        "SELECT generation FROM synchro.sync_registry_generations
         WHERE state = 'active' ORDER BY generation DESC LIMIT 1",
    )
    .expect("load active generation")
    .expect("active generation");

    let target_table = fixture.target_table.clone();
    let computed = Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)?;
        let table = registry
            .iter()
            .find(|table| table.table_name == target_table)
            .expect("active target registration");
        Ok::<_, spi::Error>(
            crate::pull::synced_row_digest(client, table, &row_data.0, "7", &row_version)
                .expect("recompute captured digest under the active registration")
                .as_bytes()
                .to_vec(),
        )
    })
    .expect("recompute captured digest");
    cleanup_membership_fixture(&fixture);

    assert_eq!(
        computed, stored,
        "membership activation must leave the captured digest valid under the active registration"
    );
    assert_eq!(
        captured_generation, active_generation,
        "membership activation must leave the captured generation on the active registration"
    );
}

/// The authored provenance scenario captures rows between membership
/// activations. A row captured after one activation must keep a digest that
/// recomputes under the registration a later activation makes active.
#[pg_test]
fn membership_activation_keeps_digests_for_later_rows() {
    let fixture = membership_dependency_fixture();
    Spi::run(&format!(
        "INSERT INTO public.{target_table} (id, label) VALUES (7, 'target');
         INSERT INTO public.{source_table} (id, target_id) VALUES (1, 7)",
        target_table = fixture.target_table,
        source_table = fixture.source_table,
    ))
    .expect("insert first membership digest source rows");
    insert_changelog("target-scope", &fixture.target_table, "7", 1);
    insert_edge(&fixture.target_table, "7", "target-scope");

    let body = format!(
        "SELECT {} WHERE old_row ? 'target_id'
         UNION ALL
         SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(old_row ->> 'target_id')::integer"),
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();

    // Capture a second row under the registration the first activation made
    // active, exactly as the scenario commits data after each activation.
    Spi::run(&format!(
        "INSERT INTO public.{target_table} (id, label) VALUES (8, 'later')",
        target_table = fixture.target_table,
    ))
    .expect("insert later membership digest source row");
    insert_changelog("target-scope", &fixture.target_table, "8", 1);
    insert_edge(&fixture.target_table, "8", "target-scope");

    enable_dependent_target_membership(&fixture);
    activate_pending_registry_for_test();

    let active_generation: i64 = Spi::get_one(
        "SELECT generation FROM synchro.sync_registry_generations
         WHERE state = 'active' ORDER BY generation DESC LIMIT 1",
    )
    .expect("load active generation")
    .expect("active generation");
    let target_table = fixture.target_table.clone();
    let relation_id = fixture.target_relation_id.clone();

    let mut divergences: Vec<String> = Vec::new();
    for record_id in ["7", "8"] {
        let stored: Vec<u8> = Spi::get_one_with_args(
            "SELECT checksum FROM synchro.sync_captured_rows
             WHERE relation_id = $1::uuid AND record_id = $2",
            &[relation_id.as_str().into(), record_id.into()],
        )
        .expect("load stored captured digest")
        .expect("stored captured digest");
        let row_data: pgrx::JsonB = Spi::get_one_with_args(
            "SELECT row_data FROM synchro.sync_captured_rows
             WHERE relation_id = $1::uuid AND record_id = $2",
            &[relation_id.as_str().into(), record_id.into()],
        )
        .expect("load captured row data")
        .expect("captured row data");
        let row_version: String = Spi::get_one_with_args(
            "SELECT row_version::text FROM synchro.sync_captured_rows
             WHERE relation_id = $1::uuid AND record_id = $2",
            &[relation_id.as_str().into(), record_id.into()],
        )
        .expect("load captured row version")
        .expect("captured row version");
        let captured_generation: i64 = Spi::get_one_with_args(
            "SELECT registry_generation FROM synchro.sync_captured_rows
             WHERE relation_id = $1::uuid AND record_id = $2",
            &[relation_id.as_str().into(), record_id.into()],
        )
        .expect("load captured generation")
        .expect("captured generation");
        let owned_record = record_id.to_string();
        let owned_table = target_table.clone();
        let computed = Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let table = registry
                .iter()
                .find(|table| table.table_name == owned_table)
                .expect("active target registration");
            Ok::<_, spi::Error>(
                crate::pull::synced_row_digest(
                    client,
                    table,
                    &row_data.0,
                    &owned_record,
                    &row_version,
                )
                .expect("recompute captured digest under the active registration")
                .as_bytes()
                .to_vec(),
            )
        })
        .expect("recompute captured digest");
        if computed != stored || captured_generation != active_generation {
            divergences.push(format!(
                "record {record_id} captured generation {captured_generation} active generation {active_generation} stored {} computed {}",
                stored.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                computed.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            ));
        }
    }
    cleanup_membership_fixture(&fixture);

    assert!(
        divergences.is_empty(),
        "membership activation must keep every captured digest valid: {}",
        divergences.join("; ")
    );
}

/// A membership activation changes scope membership. It must not change the
/// published table shape. This test reports the exact manifest difference so a
/// change is visible as evidence instead of as a version number.
#[pg_test]
fn membership_activation_keeps_the_published_manifest() {
    let fixture = membership_dependency_fixture();
    let before: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_build_object(
             'version', schema_version,
             'transition', transition_class,
             'tables', (canonical_manifest_body::jsonb) -> 'tables')
         FROM synchro.sync_schema_manifest ORDER BY schema_version DESC LIMIT 1",
    )
    .expect("load manifest before activation")
    .expect("manifest before activation");

    let body = format!(
        "SELECT {} WHERE old_row ? 'target_id'
         UNION ALL
         SELECT {} WHERE new_row ? 'target_id'",
        target_row_expression(&fixture, "(old_row ->> 'target_id')::integer"),
        target_row_expression(&fixture, "(new_row ->> 'target_id')::integer"),
    );
    create_impact_function(&fixture, &body, true, true);
    register_dependency(&fixture, 2);
    activate_pending_registry_for_test();
    enable_dependent_target_membership(&fixture);
    activate_pending_registry_for_test();

    let after: pgrx::JsonB = Spi::get_one(
        "SELECT jsonb_build_object(
             'version', schema_version,
             'transition', transition_class,
             'tables', (canonical_manifest_body::jsonb) -> 'tables')
         FROM synchro.sync_schema_manifest ORDER BY schema_version DESC LIMIT 1",
    )
    .expect("load manifest after activation")
    .expect("manifest after activation");
    cleanup_membership_fixture(&fixture);

    assert_eq!(
        before.0["tables"], after.0["tables"],
        "membership activation changed the published table shape: before version {} after version {}",
        before.0["version"], after.0["version"]
    );
}
