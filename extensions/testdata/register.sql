-- Register all seed tables for sync.
-- Run after schema.sql and CREATE EXTENSION synchro_pg.

-- =========================================================================
-- Reference tables: global bucket, read-only
-- =========================================================================

SELECT synchro.synchro_register_shared_scope('global', true);

SELECT synchro.synchro_prepare_projection_view('public.regions', 'regions', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.nations', 'nations', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.suppliers', 'suppliers', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.parts', 'parts', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.part_suppliers', 'part_suppliers', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.categories', 'categories', ARRAY['id']);
SELECT synchro.synchro_prepare_projection_view('public.customers', 'customers', ARRAY['id', 'user_id']);
SELECT synchro.synchro_prepare_projection_view('public.orders', 'orders', ARRAY['customer_id', 'id', 'user_id']);
SELECT synchro.synchro_prepare_projection_view('public.line_items', 'line_items', ARRAY['order_id']);
SELECT synchro.synchro_prepare_projection_view('public.documents', 'documents', ARRAY['id', 'owner_id']);
SELECT synchro.synchro_prepare_projection_view('public.document_members', 'document_members', ARRAY['document_id', 'user_id']);
SELECT synchro.synchro_prepare_projection_view('public.document_comments', 'document_comments', ARRAY['author_id', 'document_id']);
SELECT synchro.synchro_prepare_projection_view('public.type_zoo', 'type_zoo', ARRAY['user_id']);

CREATE OR REPLACE FUNCTION public.test_regions_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'global'::text
    FROM synchro_projection.regions AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_nations_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'global'::text
    FROM synchro_projection.nations AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_suppliers_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'global'::text
    FROM synchro_projection.suppliers AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_parts_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'global'::text
    FROM synchro_projection.parts AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_part_suppliers_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'global'::text
    FROM synchro_projection.part_suppliers AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_categories_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'global'::text
    FROM synchro_projection.categories AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_customers_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.user_id #>> '{}')
    FROM synchro_projection.customers AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_orders_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (o.user_id #>> '{}')
    FROM synchro_projection.orders AS o
    WHERE o.record_id = p_id::text AND NOT o.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_line_items_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (o.user_id #>> '{}')
    FROM synchro_projection.line_items AS li
    JOIN synchro_projection.orders AS o
      ON o.record_id = li.order_id #>> '{}' AND NOT o.deleted
    WHERE li.record_id = p_id::text AND NOT li.deleted;
END;

CREATE OR REPLACE FUNCTION public.test_line_items_bootstrap_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'bootstrap'::text
    FROM synchro_projection.line_items AS li
    WHERE li.record_id = p_id::text AND false;
END;
CREATE OR REPLACE FUNCTION public.test_documents_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.owner_id #>> '{}')
    FROM synchro_projection.documents AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_document_members_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT DISTINCT 'user:' || (member.user_id #>> '{}')
    FROM synchro_projection.document_members AS target
    JOIN synchro_projection.document_members AS member
      ON member.document_id = target.document_id AND NOT member.deleted
    WHERE target.record_id = p_id::text AND NOT target.deleted;
END;
CREATE OR REPLACE FUNCTION public.test_document_comments_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT DISTINCT bucket
    FROM (
        SELECT 'user:' || (d.owner_id #>> '{}') AS bucket
        FROM synchro_projection.document_comments AS dc
        JOIN synchro_projection.documents AS d
          ON d.record_id = dc.document_id #>> '{}' AND NOT d.deleted
        WHERE dc.record_id = p_id::text AND NOT dc.deleted
        UNION
        SELECT 'user:' || (dc.author_id #>> '{}')
        FROM synchro_projection.document_comments AS dc
        WHERE dc.record_id = p_id::text AND NOT dc.deleted
    ) AS memberships;
END;
CREATE OR REPLACE FUNCTION public.test_document_comments_bootstrap_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'bootstrap'::text
    FROM synchro_projection.document_comments AS dc
    WHERE dc.record_id = p_id::text AND false;
END;
CREATE OR REPLACE FUNCTION public.test_type_zoo_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (p.user_id #>> '{}')
    FROM synchro_projection.type_zoo AS p
    WHERE p.record_id = p_id::text AND NOT p.deleted;
END;

REVOKE ALL ON FUNCTION public.test_regions_membership(uuid), public.test_nations_membership(uuid), public.test_suppliers_membership(uuid), public.test_parts_membership(uuid), public.test_part_suppliers_membership(uuid), public.test_categories_membership(uuid), public.test_customers_membership(uuid), public.test_orders_membership(uuid), public.test_line_items_membership(uuid), public.test_line_items_bootstrap_membership(uuid), public.test_documents_membership(uuid), public.test_document_members_membership(uuid), public.test_document_comments_membership(uuid), public.test_document_comments_bootstrap_membership(uuid), public.test_type_zoo_membership(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.test_regions_membership(uuid), public.test_nations_membership(uuid), public.test_suppliers_membership(uuid), public.test_parts_membership(uuid), public.test_part_suppliers_membership(uuid), public.test_categories_membership(uuid), public.test_customers_membership(uuid), public.test_orders_membership(uuid), public.test_line_items_membership(uuid), public.test_line_items_bootstrap_membership(uuid), public.test_documents_membership(uuid), public.test_document_members_membership(uuid), public.test_document_comments_membership(uuid), public.test_document_comments_bootstrap_membership(uuid), public.test_type_zoo_membership(uuid) TO synchro_owner, synchro_worker;
GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
GRANT SELECT ON TABLE public.regions, public.nations, public.suppliers, public.parts, public.part_suppliers, public.categories TO synchro_owner;
GRANT SELECT, INSERT, UPDATE ON TABLE public.customers, public.orders, public.line_items, public.documents, public.document_members, public.document_comments, public.type_zoo TO synchro_owner;
GRANT SELECT ON TABLE public.regions, public.nations, public.suppliers, public.parts, public.part_suppliers, public.categories, public.customers, public.orders, public.line_items, public.documents, public.document_members, public.document_comments, public.type_zoo TO synchro_worker;

DO $rls$
DECLARE
    relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'regions', 'nations', 'suppliers', 'parts', 'part_suppliers', 'categories',
        'customers', 'orders', 'line_items', 'documents', 'document_members',
        'document_comments', 'type_zoo'
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
    'public.regions',
    'public.test_regions_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro.synchro_register_table(
    'public.nations',
    'public.test_nations_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro.synchro_register_table(
    'public.suppliers',
    'public.test_suppliers_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro.synchro_register_table(
    'public.parts',
    'public.test_parts_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro.synchro_register_table(
    'public.part_suppliers',
    'public.test_part_suppliers_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro.synchro_register_table(
    'public.categories',
    'public.test_categories_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'read_only'
);

-- =========================================================================
-- User-owned tables: single-owner bucket via FK chain
-- =========================================================================

-- Direct ownership: bucket is user:{user_id}
SELECT synchro.synchro_register_table(
    'public.customers',
    'public.test_customers_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled',
    ARRAY['internal_notes']
);

-- Parent chain (1 level): orders -> customers.user_id
SELECT synchro.synchro_register_table(
    'public.orders',
    'public.test_orders_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- Parent chain (2 levels): line_items -> orders -> customers.user_id
SELECT synchro.synchro_register_table(
    'public.line_items',
    'public.test_line_items_bootstrap_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- =========================================================================
-- Collaboration tables: shared ownership, multi-bucket
-- =========================================================================

-- Documents: owned by owner_id
SELECT synchro.synchro_register_table(
    'public.documents',
    'public.test_documents_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- Document members: each member gets the document in their bucket.
-- This creates multi-bucket membership (the document appears in every
-- member's bucket).
SELECT synchro.synchro_register_table(
    'public.document_members',
    'public.test_document_members_membership',
    'multi_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- Document comments: visible to the document owner AND the comment author.
-- Multiple ownership paths (two different FK chains to user).
SELECT synchro.synchro_register_table(
    'public.document_comments',
    'public.test_document_comments_bootstrap_membership',
    'multi_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- =========================================================================
-- Type zoo: user-owned for push/pull testing
-- =========================================================================

SELECT synchro.synchro_register_table(
    'public.type_zoo',
    'public.test_type_zoo_membership',
    'single_scope',
    'id', 'updated_at', 'deleted_at', 'enabled'
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
            ('orders', 'line_items', 'test_orders_line_items_impact', 'order_id', ARRAY['id', 'user_id', 'deleted_at']::text[], 'test_line_items_membership', 'single_scope'),
            ('documents', 'document_comments', 'test_documents_comments_impact', 'document_id', ARRAY['id', 'owner_id', 'deleted_at']::text[], 'test_document_comments_membership', 'multi_scope')
        ) AS configured(source_relation, target_relation, function_name, target_foreign_key, dependency_columns, final_membership_function, composition)
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
          AND field.physical_column = ANY(dependency.dependency_columns);

        IF pg_catalog.cardinality(dependency_field_ids) <> pg_catalog.cardinality(dependency.dependency_columns) THEN
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
                   AND projected.%I #>> ''{}'' IN (
                       p_old_row ->> %L, p_new_row ->> %L
                   );
             END',
            dependency.function_name,
            target_table_id,
            dependency.target_relation,
            dependency.target_foreign_key,
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

        PERFORM synchro.synchro_register_table(
            'public.' || dependency.target_relation,
            'public.' || dependency.final_membership_function,
            dependency.composition,
            'id', 'updated_at', 'deleted_at', 'enabled'
        );
    END LOOP;
END
$dependencies$;
