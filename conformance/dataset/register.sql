-- Registration for the synthetic training dataset.
-- Run after schema.sql and CREATE EXTENSION synchro_pg.
--
-- Scopes:
--   catalog      shared portable scope for global equipment and exercises
--   org:{id}     explicitly granted organization scope
--   user:{id}    the default private scope of each user

SELECT synchro.synchro_register_shared_scope('catalog', true);

SELECT synchro.synchro_prepare_projection_view('public.organizations', 'organizations', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.organization_members', 'organization_members', ARRAY['organization_id', 'user_id']);
SELECT synchro.synchro_prepare_projection_view('public.equipment', 'equipment', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.exercises', 'exercises', ARRAY['organization_id']);
SELECT synchro.synchro_prepare_projection_view('public.exercise_equipment', 'exercise_equipment', ARRAY['exercise_id']);
SELECT synchro.synchro_prepare_projection_view('public.programs', 'programs', ARRAY['organization_id', 'owner_id', 'visibility']);
SELECT synchro.synchro_prepare_projection_view('public.workouts', 'workouts', ARRAY['program_id']);
SELECT synchro.synchro_prepare_projection_view('public.workout_exercises', 'workout_exercises', ARRAY['program_id']);
SELECT synchro.synchro_prepare_projection_view('public.exercise_sets', 'exercise_sets', ARRAY['program_id']);
SELECT synchro.synchro_prepare_projection_view('public.workout_media', 'workout_media', ARRAY['owner_id']);

CREATE FUNCTION public.organizations_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'org:' || o.record_id
    FROM synchro_projection.organizations AS o
    WHERE o.record_id = p_id::text AND NOT o.deleted;
END;

CREATE FUNCTION public.organization_members_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT scope.bucket
    FROM synchro_projection.organization_members AS m
    CROSS JOIN LATERAL (VALUES
        ('org:' || (m.organization_id #>> '{}')),
        ('user:' || (m.user_id #>> '{}'))
    ) AS scope(bucket)
    WHERE m.record_id = p_id::text AND NOT m.deleted;
END;

CREATE FUNCTION public.equipment_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'catalog'::text
    FROM synchro_projection.equipment AS e
    WHERE e.record_id = p_id::text AND NOT e.deleted;
END;

CREATE FUNCTION public.exercises_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT COALESCE('org:' || (e.organization_id #>> '{}'), 'catalog')
    FROM synchro_projection.exercises AS e
    WHERE e.record_id = p_id::text AND NOT e.deleted;
END;

CREATE FUNCTION public.exercise_equipment_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT COALESCE('org:' || (e.organization_id #>> '{}'), 'catalog')
    FROM synchro_projection.exercise_equipment AS link
    JOIN synchro_projection.exercises AS e
      ON e.record_id = link.exercise_id #>> '{}' AND NOT e.deleted
    WHERE link.record_id = p_id::text AND NOT link.deleted;
END;

CREATE FUNCTION public.programs_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT scope.bucket
    FROM synchro_projection.programs AS p
    CROSS JOIN LATERAL (VALUES
        ('user:' || (p.owner_id #>> '{}')),
        (CASE WHEN p.visibility #>> '{}' = 'organization' THEN 'org:' || (p.organization_id #>> '{}') END)
    ) AS scope(bucket)
    WHERE p.record_id = p_id::text AND NOT p.deleted AND scope.bucket IS NOT NULL;
END;

CREATE FUNCTION public.workouts_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT scope.bucket
    FROM synchro_projection.workouts AS child
    JOIN synchro_projection.programs AS p
      ON p.record_id = child.program_id #>> '{}' AND NOT p.deleted
    CROSS JOIN LATERAL (VALUES
        ('user:' || (p.owner_id #>> '{}')),
        (CASE WHEN p.visibility #>> '{}' = 'organization' THEN 'org:' || (p.organization_id #>> '{}') END)
    ) AS scope(bucket)
    WHERE child.record_id = p_id::text AND NOT child.deleted AND scope.bucket IS NOT NULL;
END;

CREATE FUNCTION public.workout_exercises_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT scope.bucket
    FROM synchro_projection.workout_exercises AS child
    JOIN synchro_projection.programs AS p
      ON p.record_id = child.program_id #>> '{}' AND NOT p.deleted
    CROSS JOIN LATERAL (VALUES
        ('user:' || (p.owner_id #>> '{}')),
        (CASE WHEN p.visibility #>> '{}' = 'organization' THEN 'org:' || (p.organization_id #>> '{}') END)
    ) AS scope(bucket)
    WHERE child.record_id = p_id::text AND NOT child.deleted AND scope.bucket IS NOT NULL;
END;

CREATE FUNCTION public.exercise_sets_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT scope.bucket
    FROM synchro_projection.exercise_sets AS child
    JOIN synchro_projection.programs AS p
      ON p.record_id = child.program_id #>> '{}' AND NOT p.deleted
    CROSS JOIN LATERAL (VALUES
        ('user:' || (p.owner_id #>> '{}')),
        (CASE WHEN p.visibility #>> '{}' = 'organization' THEN 'org:' || (p.organization_id #>> '{}') END)
    ) AS scope(bucket)
    WHERE child.record_id = p_id::text AND NOT child.deleted AND scope.bucket IS NOT NULL;
END;

CREATE FUNCTION public.workout_media_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (m.owner_id #>> '{}')
    FROM synchro_projection.workout_media AS m
    WHERE m.record_id = p_id::text AND NOT m.deleted;
END;

-- Registration of a joined membership function needs its dependency first.
-- Each joined relation starts with this empty membership function.
CREATE FUNCTION public.dataset_bootstrap_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'dataset:bootstrap'::text WHERE p_id IS NULL AND false;
END;

REVOKE ALL ON FUNCTION
    public.organizations_membership(uuid), public.organization_members_membership(uuid),
    public.equipment_membership(uuid), public.exercises_membership(uuid),
    public.exercise_equipment_membership(uuid), public.programs_membership(uuid),
    public.workouts_membership(uuid), public.workout_exercises_membership(uuid),
    public.exercise_sets_membership(uuid), public.workout_media_membership(uuid),
    public.dataset_bootstrap_membership(uuid)
FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
    public.organizations_membership(uuid), public.organization_members_membership(uuid),
    public.equipment_membership(uuid), public.exercises_membership(uuid),
    public.exercise_equipment_membership(uuid), public.programs_membership(uuid),
    public.workouts_membership(uuid), public.workout_exercises_membership(uuid),
    public.exercise_sets_membership(uuid), public.workout_media_membership(uuid),
    public.dataset_bootstrap_membership(uuid)
TO synchro_owner, synchro_worker;

GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
GRANT SELECT ON TABLE public.organizations, public.organization_members, public.equipment,
    public.exercises, public.exercise_equipment TO synchro_owner;
GRANT SELECT, INSERT, UPDATE ON TABLE public.programs, public.workouts, public.workout_exercises,
    public.exercise_sets, public.workout_media TO synchro_owner;
GRANT SELECT ON TABLE public.organizations, public.organization_members, public.equipment,
    public.exercises, public.exercise_equipment, public.programs, public.workouts,
    public.workout_exercises, public.exercise_sets, public.workout_media TO synchro_worker;

-- A push writes only rows that belong to its authenticated user. A missing
-- identity context permits trusted server maintenance.
DO $rls$
DECLARE
    relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'organizations', 'organization_members', 'equipment', 'exercises', 'exercise_equipment'
    ]
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', relation_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY synchro_owner_read ON public.%I AS PERMISSIVE FOR SELECT TO synchro_owner USING (true)',
            relation_name
        );
    END LOOP;
    FOREACH relation_name IN ARRAY ARRAY[
        'programs', 'workouts', 'workout_exercises', 'exercise_sets', 'workout_media'
    ]
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', relation_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY synchro_owner_access ON public.%I AS PERMISSIVE FOR ALL TO synchro_owner
             USING (NULLIF(current_setting(''synchro.user_id'', true), '''') IS NULL
                    OR owner_id = current_setting(''synchro.user_id'', true))
             WITH CHECK (NULLIF(current_setting(''synchro.user_id'', true), '''') IS NULL
                    OR owner_id = current_setting(''synchro.user_id'', true))',
            relation_name
        );
    END LOOP;
END
$rls$;

SELECT synchro.synchro_register_table('public.organizations', 'public.organizations_membership',
    'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only');
SELECT synchro.synchro_register_table('public.organization_members', 'public.organization_members_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'read_only');
SELECT synchro.synchro_register_table('public.equipment', 'public.equipment_membership',
    'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only');
SELECT synchro.synchro_register_table('public.exercises', 'public.exercises_membership',
    'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only', ARRAY['search_vector']);
SELECT synchro.synchro_register_table('public.programs', 'public.programs_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled');
SELECT synchro.synchro_register_table('public.workout_media', 'public.workout_media_membership',
    'single_scope', 'id', 'updated_at', 'deleted_at', 'enabled');
SELECT synchro.synchro_register_table('public.exercise_equipment', 'public.dataset_bootstrap_membership',
    'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only');
SELECT synchro.synchro_register_table('public.workouts', 'public.dataset_bootstrap_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled', ARRAY['duration_minutes']);
SELECT synchro.synchro_register_table('public.workout_exercises', 'public.dataset_bootstrap_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled');
SELECT synchro.synchro_register_table('public.exercise_sets', 'public.dataset_bootstrap_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled');

-- Each dependency re-evaluates the child rows of a changed parent.
DO $dependencies$
DECLARE
    dependency record;
    dependency_field_ids text[];
    current_generation bigint;
    target_table_id uuid;
    source_primary_field_id text;
BEGIN
    FOR dependency IN
        SELECT *
        FROM (VALUES
            ('exercises', 'exercise_equipment', 'dataset_exercises_equipment_impact', 'exercise_id',
             ARRAY['id', 'organization_id', 'deleted_at']::text[]),
            ('programs', 'workouts', 'dataset_programs_workouts_impact', 'program_id',
             ARRAY['id', 'organization_id', 'owner_id', 'visibility', 'deleted_at']::text[]),
            ('programs', 'workout_exercises', 'dataset_programs_workout_exercises_impact', 'program_id',
             ARRAY['id', 'organization_id', 'owner_id', 'visibility', 'deleted_at']::text[]),
            ('programs', 'exercise_sets', 'dataset_programs_exercise_sets_impact', 'program_id',
             ARRAY['id', 'organization_id', 'owner_id', 'visibility', 'deleted_at']::text[])
        ) AS configured(source_relation, target_relation, function_name, target_foreign_key, dependency_columns)
    LOOP
        SELECT generation INTO STRICT current_generation
        FROM synchro.sync_registry_generations
        WHERE state IN ('active', 'pending') AND validated
        ORDER BY generation DESC
        LIMIT 1;

        SELECT registry.table_id INTO STRICT target_table_id
        FROM synchro.sync_registry AS registry
        WHERE registry.registry_generation = current_generation
          AND registry.physical_schema = 'public'
          AND registry.physical_relation = dependency.target_relation;

        SELECT field.field_id::text INTO STRICT source_primary_field_id
        FROM synchro.sync_registry_fields AS field
        JOIN synchro.sync_registry AS registry
          ON registry.registry_generation = field.registry_generation
         AND registry.relation_id = field.relation_id
        WHERE registry.registry_generation = current_generation
          AND registry.physical_schema = 'public'
          AND registry.physical_relation = dependency.source_relation
          AND field.physical_column = 'id';

        SELECT pg_catalog.array_agg(field.field_id::text ORDER BY field.field_id) INTO dependency_field_ids
        FROM synchro.sync_registry_fields AS field
        JOIN synchro.sync_registry AS registry
          ON registry.registry_generation = field.registry_generation
         AND registry.relation_id = field.relation_id
        WHERE registry.registry_generation = current_generation
          AND registry.physical_schema = 'public'
          AND registry.physical_relation = dependency.source_relation
          AND field.physical_column = ANY(dependency.dependency_columns);

        IF pg_catalog.cardinality(dependency_field_ids) <> pg_catalog.cardinality(dependency.dependency_columns) THEN
            RAISE EXCEPTION 'dataset dependency field identity is incomplete';
        END IF;

        EXECUTE pg_catalog.format(
            'CREATE FUNCTION public.%I(p_old_row jsonb, p_new_row jsonb)
             RETURNS SETOF synchro.synchro_row_ref
             LANGUAGE SQL STABLE SECURITY INVOKER
             SET search_path = pg_catalog, synchro
             BEGIN ATOMIC
                 SELECT ROW(%L::uuid, ''string'', pg_catalog.to_jsonb(projected.record_id))::synchro.synchro_row_ref
                 FROM synchro_projection.%I AS projected
                 WHERE NOT projected.deleted
                   AND projected.%I #>> ''{}'' IN (p_old_row ->> %L, p_new_row ->> %L);
             END',
            dependency.function_name, target_table_id, dependency.target_relation,
            dependency.target_foreign_key, source_primary_field_id, source_primary_field_id
        );
        EXECUTE pg_catalog.format('REVOKE EXECUTE ON FUNCTION public.%I(jsonb, jsonb) FROM PUBLIC', dependency.function_name);
        EXECUTE pg_catalog.format(
            'GRANT EXECUTE ON FUNCTION public.%I(jsonb, jsonb) TO synchro_owner, synchro_worker',
            dependency.function_name
        );
        PERFORM synchro.synchro_register_membership_dependency(
            dependency.source_relation, dependency.target_relation,
            'public.' || dependency.function_name, dependency_field_ids, 1000
        );
    END LOOP;
END
$dependencies$;

INSERT INTO synchro.sync_scope_state (scope_id, stream_generation)
SELECT 'dataset:bootstrap', stream_generation
FROM synchro.sync_runtime_state
WHERE singleton
ON CONFLICT (scope_id) DO NOTHING;

SELECT synchro.synchro_register_table('public.exercise_equipment', 'public.exercise_equipment_membership',
    'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only',
    p_affected_scopes => ARRAY['dataset:bootstrap']::text[]);
SELECT synchro.synchro_register_table('public.workouts', 'public.workouts_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled', ARRAY['duration_minutes'],
    p_affected_scopes => ARRAY['dataset:bootstrap']::text[]);
SELECT synchro.synchro_register_table('public.workout_exercises', 'public.workout_exercises_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled',
    p_affected_scopes => ARRAY['dataset:bootstrap']::text[]);
SELECT synchro.synchro_register_table('public.exercise_sets', 'public.exercise_sets_membership',
    'multi_scope', 'id', 'updated_at', 'deleted_at', 'enabled',
    p_affected_scopes => ARRAY['dataset:bootstrap']::text[]);

DELETE FROM synchro.sync_scope_state WHERE scope_id = 'dataset:bootstrap';
