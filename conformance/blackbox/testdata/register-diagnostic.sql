-- Independent public registration setup for protocol 3 verification.
-- Run only after schema.sql and CREATE EXTENSION synchro_pg.

SELECT synchro.synchro_register_shared_scope('cf:global', true);

SELECT synchro.synchro_prepare_projection_view('public.cf_global_items', 'cf_global_items', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_items', 'cf_items', ARRAY['owner_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_documents', 'cf_documents', ARRAY['id', 'owner_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_document_members', 'cf_document_members', ARRAY['document_id', 'member_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_document_access', 'cf_document_access', ARRAY['document_id', 'owner_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_document_notes', 'cf_document_notes', ARRAY['author_id', 'document_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_schema_queue', 'cf_schema_queue', ARRAY['owner_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_decode_trap', 'cf_decode_trap', ARRAY['owner_id']);
SELECT synchro.synchro_prepare_projection_view('public.cf_late_registration', 'cf_late_registration', ARRAY['owner_id']);

CREATE OR REPLACE FUNCTION public.cf_global_items_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'cf:global'::text
    FROM synchro_projection.cf_global_items AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_items_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.owner_id #>> '{}')
    FROM synchro_projection.cf_items AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_items_cross_scope_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT bucket
    FROM synchro_projection.cf_items AS p
    CROSS JOIN LATERAL (
        VALUES
            ('cf:dedup'::text),
            ('user:' || (p.owner_id #>> '{}'))
    ) AS memberships(bucket)
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_documents_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.owner_id #>> '{}')
    FROM synchro_projection.cf_documents AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_document_members_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (member.member_id #>> '{}')
    FROM synchro_projection.cf_document_members AS member
    WHERE member.record_id = p_id::text AND NOT member.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_document_notes_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (n.author_id #>> '{}')
    FROM synchro_projection.cf_document_notes AS n
    WHERE n.record_id = p_id::text AND NOT n.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_schema_queue_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.owner_id #>> '{}')
    FROM synchro_projection.cf_schema_queue AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_decode_trap_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.owner_id #>> '{}')
    FROM synchro_projection.cf_decode_trap AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.cf_late_registration_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.owner_id #>> '{}')
    FROM synchro_projection.cf_late_registration AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;

REVOKE ALL ON FUNCTION public.cf_global_items_membership(uuid), public.cf_items_membership(uuid), public.cf_items_cross_scope_membership(uuid), public.cf_documents_membership(uuid), public.cf_document_members_membership(uuid), public.cf_document_notes_membership(uuid), public.cf_schema_queue_membership(uuid), public.cf_decode_trap_membership(uuid), public.cf_late_registration_membership(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.cf_global_items_membership(uuid), public.cf_items_membership(uuid), public.cf_items_cross_scope_membership(uuid), public.cf_documents_membership(uuid), public.cf_document_members_membership(uuid), public.cf_document_notes_membership(uuid), public.cf_schema_queue_membership(uuid), public.cf_decode_trap_membership(uuid), public.cf_late_registration_membership(uuid) TO synchro_owner, synchro_worker;
GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
GRANT SELECT ON TABLE public.cf_global_items TO synchro_owner;
GRANT SELECT ON TABLE public.cf_document_access TO synchro_owner;
GRANT SELECT ON TABLE public.cf_item_impacts TO synchro_owner;
GRANT SELECT, INSERT, UPDATE ON TABLE public.cf_items, public.cf_documents, public.cf_document_members, public.cf_document_notes, public.cf_schema_queue, public.cf_decode_trap, public.cf_late_registration TO synchro_owner;
GRANT SELECT ON TABLE public.cf_global_items, public.cf_items, public.cf_documents, public.cf_document_members, public.cf_document_access, public.cf_document_notes, public.cf_schema_queue, public.cf_decode_trap, public.cf_late_registration, public.cf_item_impacts TO synchro_worker;

DO $rls$
DECLARE
    relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'cf_global_items', 'cf_items', 'cf_documents', 'cf_document_members',
        'cf_document_access', 'cf_document_notes', 'cf_schema_queue',
        'cf_decode_trap', 'cf_late_registration', 'cf_item_impacts'
    ]
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', relation_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY synchro_owner_all ON public.%I AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true)',
            relation_name
        );
    END LOOP;
END
$rls$;

SELECT synchro.synchro_register_table(
    'public.cf_global_items',
    'public.cf_global_items_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro.synchro_register_table(
    'public.cf_items',
    'public.cf_items_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro.synchro_register_table(
    'public.cf_documents',
    'public.cf_documents_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro.synchro_register_table(
    'public.cf_document_members',
    'public.cf_document_members_membership',
    'multi_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro.synchro_register_table(
    'public.cf_document_notes',
    'public.cf_document_notes_membership',
    'multi_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro.synchro_register_table(
    'public.cf_schema_queue',
    'public.cf_schema_queue_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro.synchro_register_table(
    'public.cf_decode_trap',
    'public.cf_decode_trap_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro.synchro_register_capture_dependency(
    'public.cf_document_access',
    ARRAY['id']::text[],
    ARRAY['document_id', 'owner_id']::text[]
);

SELECT synchro.synchro_register_capture_dependency(
    'public.cf_item_impacts',
    ARRAY['id']::text[],
    ARRAY['scope_key']::text[]
);

DO $dependencies$
DECLARE
    dependency record;
    dependency_field_ids text[];
    function_body text;
    current_generation bigint;
    target_table_id uuid;
    source_primary_field_id text;
BEGIN
    FOR dependency IN
        SELECT *
        FROM (VALUES
            ('cf_documents', 'cf_document_members', 'cf_documents_members_impact'),
            ('cf_documents', 'cf_document_notes', 'cf_documents_notes_impact')
        ) AS configured(source_relation, target_relation, function_name)
    LOOP
        SELECT generation
        INTO STRICT current_generation
        FROM synchro.sync_registry_generations
        WHERE state IN ('active', 'pending') AND validated
        ORDER BY generation DESC
        LIMIT 1;

        SELECT registry.table_id
        INTO STRICT target_table_id
        FROM synchro.sync_registry AS registry
        WHERE registry.registry_generation = current_generation
          AND registry.physical_schema = 'public'
          AND registry.physical_relation = dependency.target_relation;

        SELECT field.field_id::text
        INTO STRICT source_primary_field_id
        FROM synchro.sync_registry_fields AS field
        JOIN synchro.sync_registry AS registry
          ON registry.registry_generation = field.registry_generation
         AND registry.relation_id = field.relation_id
        WHERE registry.registry_generation = current_generation
          AND registry.physical_schema = 'public'
          AND registry.physical_relation = dependency.source_relation
          AND field.physical_column = 'id';

        SELECT pg_catalog.array_agg(field.field_id::text ORDER BY field.field_id)
        INTO dependency_field_ids
        FROM synchro.sync_registry_fields AS field
        JOIN synchro.sync_registry AS registry
          ON registry.registry_generation = field.registry_generation
         AND registry.relation_id = field.relation_id
        WHERE registry.registry_generation = current_generation
          AND registry.physical_schema = 'public'
          AND registry.physical_relation = dependency.source_relation
          AND field.physical_column = ANY(ARRAY['id', 'owner_id', 'deleted_at']::text[]);

        IF pg_catalog.cardinality(dependency_field_ids) <> 3 THEN
            RAISE EXCEPTION 'dependency field identity is incomplete';
        END IF;

        function_body := pg_catalog.format(
            'CREATE OR REPLACE FUNCTION public.%I(p_old_row jsonb, p_new_row jsonb)
             RETURNS SETOF synchro.synchro_row_ref
             LANGUAGE SQL STABLE SECURITY INVOKER
             SET search_path = pg_catalog, synchro
             BEGIN ATOMIC
                 SELECT ROW(%L::uuid, ''string'', pg_catalog.to_jsonb(projected.record_id))::synchro.synchro_row_ref
                 FROM synchro_projection.%I AS projected
                 WHERE NOT projected.deleted
                   AND projected.document_id #>> ''{}'' IN (
                       p_old_row ->> %L, p_new_row ->> %L
                   );
             END',
            dependency.function_name,
            target_table_id,
            dependency.target_relation,
            source_primary_field_id,
            source_primary_field_id
        );
        EXECUTE function_body;
        EXECUTE pg_catalog.format(
            'REVOKE EXECUTE ON FUNCTION public.%I(jsonb, jsonb) FROM PUBLIC',
            dependency.function_name
        );
        EXECUTE pg_catalog.format(
            'GRANT EXECUTE ON FUNCTION public.%I(jsonb, jsonb) TO synchro_owner, synchro_worker',
            dependency.function_name
        );

        PERFORM synchro.synchro_register_membership_dependency(
            dependency.source_relation,
            dependency.target_relation,
            'public.' || dependency.function_name,
            dependency_field_ids,
            1000
        );
    END LOOP;
END
$dependencies$;

DO $capture_dependency$
DECLARE
    current_generation bigint;
    target_table_id uuid;
    function_body text;
BEGIN
    SELECT generation
    INTO STRICT current_generation
    FROM synchro.sync_registry_generations
    WHERE state IN ('active', 'pending') AND validated
    ORDER BY generation DESC
    LIMIT 1;

    SELECT registry.table_id
    INTO STRICT target_table_id
    FROM synchro.sync_registry AS registry
    WHERE registry.registry_generation = current_generation
      AND registry.physical_schema = 'public'
      AND registry.physical_relation = 'cf_document_members';

    function_body := pg_catalog.format(
        'CREATE OR REPLACE FUNCTION public.cf_document_access_impact(p_old_row jsonb, p_new_row jsonb)
         RETURNS SETOF synchro.synchro_row_ref
         LANGUAGE SQL STABLE SECURITY INVOKER
         SET search_path = pg_catalog, synchro
         BEGIN ATOMIC
             SELECT ROW(%L::uuid, ''string'', pg_catalog.to_jsonb(projected.record_id))::synchro.synchro_row_ref
             FROM synchro_projection.cf_document_members AS projected
             WHERE NOT projected.deleted
               AND projected.document_id #>> ''{}'' IN (
                   p_old_row ->> ''document_id'', p_new_row ->> ''document_id''
               );
         END',
        target_table_id
    );
    EXECUTE function_body;
    REVOKE EXECUTE ON FUNCTION public.cf_document_access_impact(jsonb, jsonb) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION public.cf_document_access_impact(jsonb, jsonb)
        TO synchro_owner, synchro_worker;

    PERFORM synchro.synchro_register_membership_dependency(
        'cf_document_access',
        'cf_document_members',
        'public.cf_document_access_impact',
        ARRAY['id', 'document_id', 'owner_id']::text[],
        1000
    );
END
$capture_dependency$;

CREATE OR REPLACE FUNCTION public.cf_document_members_membership_v2(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT DISTINCT bucket
    FROM (
        SELECT 'user:' || (d.owner_id #>> '{}') AS bucket
        FROM synchro_projection.cf_document_members AS m
        JOIN synchro_projection.cf_documents AS d
          ON d.record_id = m.document_id #>> '{}' AND NOT d.deleted
        WHERE m.record_id = p_id::text AND NOT m.deleted
        UNION
        SELECT 'user:' || (member.member_id #>> '{}')
        FROM synchro_projection.cf_document_members AS member
        WHERE member.record_id = p_id::text AND NOT member.deleted
        UNION
        SELECT 'user:' || (access.owner_id #>> '{}')
        FROM synchro_projection.cf_document_members AS member
        JOIN synchro_projection.cf_document_access AS access
          ON access.document_id #>> '{}' = member.document_id #>> '{}'
         AND NOT access.deleted
        WHERE member.record_id = p_id::text AND NOT member.deleted
    ) AS memberships;
END;

REVOKE EXECUTE ON FUNCTION public.cf_document_members_membership_v2(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.cf_document_members_membership_v2(uuid)
    TO synchro_owner, synchro_worker;

SELECT synchro.synchro_register_table(
    'public.cf_document_members',
    'public.cf_document_members_membership_v2',
    'multi_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

CREATE OR REPLACE FUNCTION public.cf_document_notes_membership_v2(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT DISTINCT bucket
    FROM (
        SELECT 'user:' || (d.owner_id #>> '{}') AS bucket
        FROM synchro_projection.cf_document_notes AS n
        JOIN synchro_projection.cf_documents AS d
          ON d.record_id = n.document_id #>> '{}' AND NOT d.deleted
        WHERE n.record_id = p_id::text AND NOT n.deleted
        UNION
        SELECT 'user:' || (n.author_id #>> '{}')
        FROM synchro_projection.cf_document_notes AS n
        WHERE n.record_id = p_id::text AND NOT n.deleted
    ) AS memberships;
END;

REVOKE EXECUTE ON FUNCTION public.cf_document_notes_membership_v2(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.cf_document_notes_membership_v2(uuid)
    TO synchro_owner, synchro_worker;

SELECT synchro.synchro_register_table(
    'public.cf_document_notes',
    'public.cf_document_notes_membership_v2',
    'multi_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);
