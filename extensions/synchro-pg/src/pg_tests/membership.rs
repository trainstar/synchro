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
         GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
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
             'id', 'updated_at', 'deleted_at', 'enabled'
         )",
        target_membership = fixture.target_membership,
        source_table = fixture.source_table,
        target_table = fixture.target_table,
    ))
    .expect("enable dependent target membership");
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
         GRANT USAGE ON SCHEMA tests TO synchro_owner, synchro_worker;
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
            crate::bucketing::resolve_membership(client, &registration, "1").map_err(|_| ())
        }))
        .catch_others(|_| Err(()))
        .execute();
        Ok::<_, pgrx::spi::Error>(result)
    })
    .expect("resolve registered membership limit fixture");
    let materialized_rows = Spi::connect(|client| {
        let result_limit = registration
            .max_scope_fanout
            .checked_add(1)
            .expect("positive test scope fanout limit");
        let rows = client.select(
            &crate::bucketing::membership_query(
                &registration.membership_function,
                &registration.pk_type,
                result_limit,
            ),
            None,
            &["1".into()],
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

#[pg_test]
fn membership_accepts_empty_string_primary_key() {
    Spi::run(
        "CREATE TABLE test_empty_string_pk (
             id TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         SELECT tests.register_legacy_test_table(
             'test_empty_string_pk',
             $$SELECT ARRAY['global'] FROM test_empty_string_pk WHERE id = $1::text$$,
             'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only'
         )",
    )
    .expect("register empty string primary-key fixture");
    activate_pending_registry_for_test();
    Spi::run("INSERT INTO test_empty_string_pk (id, value) VALUES ('', 'empty key')")
        .expect("insert empty string primary key");
    let scopes = Spi::connect(|client| {
        let registry = crate::registry::load_registry_from_client(client)?;
        let registration = registry
            .iter()
            .find(|registration| registration.physical_relation == "test_empty_string_pk")
            .expect("empty string primary-key registration");
        crate::bucketing::resolve_membership(client, registration, "")
    })
    .expect("resolve empty string primary-key membership");

    assert_eq!(scopes, vec!["global"]);
}

#[pg_test]
fn membership_function_fails_closed_when_query_limit_overflows() {
    let function = crate::registry::RegisteredFunction {
        oid: 0,
        schema: "tests".to_string(),
        name: "unreachable_membership_function".to_string(),
    };
    let resolution = Spi::connect(|client| {
        let result = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            crate::bucketing::resolve_registered_membership(
                client,
                &function,
                "integer",
                "1",
                i32::MAX,
            )
            .map_err(|_| ())
        }))
        .catch_others(|_| Err(()))
        .execute();
        Ok::<_, pgrx::spi::Error>(result)
    })
    .expect("resolve overflowed membership limit");

    assert_eq!(resolution, Err(()));
}
