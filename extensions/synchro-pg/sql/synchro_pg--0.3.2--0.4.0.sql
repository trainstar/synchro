-- Update synchro_pg from 0.3.2 to 0.4.0.
-- Stop if psql runs this file. ALTER EXTENSION removes the \echo line.
\echo Use "ALTER EXTENSION synchro_pg UPDATE TO '0.4.0'" to load this file. \quit

-- The WAL worker reads the tables that this update alters, in its own lock
-- order, during a poll and during startup after the restart that precedes the
-- update. The exclusive worker gate waits for those worker transactions and
-- holds the next one until the update commits, so they cannot deadlock.
SELECT pg_catalog.pg_advisory_xact_lock(2002873458);

-- Same-relation impact declarations are valid. A clean installation names the
-- remaining table check without a numeric suffix.
ALTER TABLE synchro.sync_membership_dependencies
    DROP CONSTRAINT sync_membership_dependencies_check;
ALTER TABLE synchro.sync_membership_dependencies
    RENAME CONSTRAINT sync_membership_dependencies_check1 TO sync_membership_dependencies_check;

-- Validation records whether a registry edge needs source values. Existing
-- rows keep NULL, which is unknown evidence. A pending generation with NULL
-- needs a verified bootstrap before activation.
ALTER TABLE synchro.sync_registry_generations
    ADD COLUMN source_requirement SMALLINT CHECK (source_requirement IN (0, 1, 2));

-- Format 2 transaction fingerprints bind the row boundary of each logical
-- message. Records from earlier releases keep format 1, which the worker
-- accepts only for those records.
ALTER TABLE synchro.sync_wal_transactions
    ADD COLUMN content_hash_format SMALLINT NOT NULL DEFAULT 1
        CHECK (content_hash_format IN (1, 2));
ALTER TABLE synchro.sync_wal_transactions
    ALTER COLUMN content_hash_format DROP DEFAULT;

CREATE TABLE synchro.sync_assignment_function (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    function_oid OID NOT NULL,
    function_schema TEXT NOT NULL,
    function_name TEXT NOT NULL,
    max_scopes INTEGER NOT NULL CHECK (max_scopes BETWEEN 1 AND 1000),
    definition_sha256 TEXT NOT NULL CHECK (definition_sha256 ~ '^[0-9a-f]{64}$'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE synchro.sync_assignment_function OWNER TO synchro_owner;
REVOKE ALL ON TABLE synchro.sync_assignment_function FROM PUBLIC;

CREATE FUNCTION synchro.synchro_register_assignment_function(
    "p_function" TEXT,
    "p_max_scopes" INT DEFAULT 1000
) RETURNS void
STRICT
LANGUAGE c
SECURITY DEFINER
SET search_path = pg_catalog, synchro, pg_temp
AS 'MODULE_PATHNAME', 'synchro_register_assignment_function_wrapper';
ALTER FUNCTION synchro.synchro_register_assignment_function(TEXT, INT) OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_register_assignment_function(TEXT, INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION synchro.synchro_register_assignment_function(TEXT, INT)
    TO synchro_operator;

CREATE FUNCTION synchro.synchro_unregister_assignment_function() RETURNS void
STRICT
LANGUAGE c
SECURITY DEFINER
SET search_path = pg_catalog, synchro, pg_temp
AS 'MODULE_PATHNAME', 'synchro_unregister_assignment_function_wrapper';
ALTER FUNCTION synchro.synchro_unregister_assignment_function() OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_unregister_assignment_function() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION synchro.synchro_unregister_assignment_function() TO synchro_operator;

ALTER TABLE synchro.sync_push_mutations DROP CONSTRAINT sync_push_mutations_check;
ALTER TABLE synchro.sync_push_mutations ADD CONSTRAINT sync_push_mutations_outcome_code_check CHECK (
        (outcome_status = 'applied' AND rejection_code IS NULL)
        OR
        (outcome_status = 'conflict'
         AND rejection_code IS NOT NULL
         AND rejection_code IN (
             'version_conflict', 'row_already_exists', 'row_deleted', 'row_not_found'
         ))
        OR
        (outcome_status = 'rejected_terminal'
         AND rejection_code IS NOT NULL
         AND rejection_code IN (
             'schema_incompatible', 'table_not_synced', 'policy_rejected', 'validation_failed',
             'atomic_batch_rejected'
         ))
    );

-- A projection view reads rows by relation oid, so a later relation with the
-- same name cannot supply rows to the view.
CREATE OR REPLACE VIEW synchro.sync_current_projections
WITH (security_barrier = true) AS
WITH reset_context AS (
    SELECT CASE
               WHEN reset_setting ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                   THEN reset_setting::uuid
               ELSE NULL
           END AS reset_id,
           CASE
               WHEN registry_setting ~ '^[1-9][0-9]*$'
                   THEN registry_setting::bigint
               ELSE NULL
           END AS registry_generation
    FROM (
        SELECT current_setting('synchro.stream_reset_staging_id', true) AS reset_setting,
               current_setting('synchro.stream_reset_staging_registry_generation', true)
                   AS registry_setting
    ) value
), projections AS (
    SELECT captured.relation_id,
           captured.record_id,
           NULL::jsonb AS capture_key,
           captured.row_data,
           captured.deleted
    FROM synchro.sync_captured_rows captured
    CROSS JOIN reset_context context
    WHERE context.reset_id IS NULL
    UNION ALL
    SELECT captured.relation_id,
           captured.record_id,
           NULL::jsonb AS capture_key,
           captured.row_data,
           captured.deleted
    FROM synchro.sync_stream_reset_captured_rows captured
    CROSS JOIN reset_context context
    WHERE captured.reset_id = context.reset_id
    UNION ALL
    SELECT captured.relation_id,
           NULL::text AS record_id,
           captured.capture_key,
           captured.row_data,
           captured.deleted
    FROM synchro.sync_capture_dependency_rows captured
    CROSS JOIN reset_context context
    WHERE context.reset_id IS NULL
    UNION ALL
    SELECT captured.relation_id,
           NULL::text AS record_id,
           captured.capture_key,
           captured.row_data,
           captured.deleted
    FROM synchro.sync_stream_reset_capture_dependency_rows captured
    CROSS JOIN reset_context context
    WHERE captured.reset_id = context.reset_id
)
SELECT registry.registry_generation,
       registry.relation_id,
       registry.registration_kind,
       registry.table_id,
       registry.table_name,
       registry.physical_schema,
       registry.physical_relation,
       captured.record_id,
       captured.capture_key,
       captured.row_data,
       captured.deleted,
       registry.physical_relation_oid
FROM synchro.sync_wal_progress progress
CROSS JOIN reset_context context
JOIN synchro.sync_registry registry
  ON registry.registry_generation = COALESCE(context.registry_generation, progress.registry_generation)
JOIN projections captured
  ON captured.relation_id = registry.relation_id
WHERE progress.singleton;

-- Each reader runs this check as itself, so a projection view returns rows
-- only to a role that can read the source relation.
CREATE FUNCTION synchro.synchro_assert_projection_reader(p_relation OID)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, synchro, pg_temp
AS $$
BEGIN
    IF pg_catalog.has_table_privilege(p_relation, 'SELECT') IS NOT TRUE THEN
        RAISE EXCEPTION 'projection view reader cannot read the source relation'
            USING ERRCODE = '42501';
    END IF;
    RETURN true;
END;
$$;
ALTER FUNCTION synchro.synchro_assert_projection_reader(OID) OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_assert_projection_reader(OID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION synchro.synchro_assert_projection_reader(OID)
    TO synchro_operator, synchro_worker;

-- An extension script can replace only an object that the extension owns, so
-- each projection view is an extension member only while it is replaced.
DO $rebuild$
DECLARE
    projection RECORD;
BEGIN
    FOR projection IN
        SELECT view.view_oid::pg_catalog.regclass AS view_name,
               view.physical_relation_oid,
               (
                   SELECT pg_catalog.string_agg(pg_catalog.format(
                       'CASE WHEN projection.registration_kind = ''synced'' THEN '
                           || 'projection.row_data -> ('
                           || 'SELECT field.field_id::text '
                           || 'FROM synchro.sync_registry_fields field '
                           || 'WHERE field.registry_generation = projection.registry_generation '
                           || 'AND field.relation_id = projection.relation_id '
                           || 'AND field.physical_column = %1$L'
                           || ') ELSE projection.row_data -> %1$L END AS %1$I',
                       projected.column_name
                   ), ', ' ORDER BY projected.ordinal)
                   FROM pg_catalog.unnest(view.projected_columns)
                       WITH ORDINALITY AS projected(column_name, ordinal)
               ) AS expressions
        FROM synchro.sync_projection_views AS view
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER EXTENSION synchro_pg ADD VIEW %s',
            projection.view_name
        );
        EXECUTE pg_catalog.format(
            'CREATE OR REPLACE VIEW %s WITH (security_barrier = true) AS '
                || 'SELECT projection.record_id, projection.capture_key, projection.deleted, %s '
                || 'FROM synchro.sync_current_projections projection '
                || 'WHERE projection.physical_relation_oid = %3$s::oid '
                || 'AND synchro.synchro_assert_projection_reader(%3$s::oid)',
            projection.view_name,
            projection.expressions,
            projection.physical_relation_oid
        );
        EXECUTE pg_catalog.format(
            'ALTER EXTENSION synchro_pg DROP VIEW %s',
            projection.view_name
        );
    END LOOP;
END
$rebuild$;

-- Registered functions now run as their owner, and each membership and
-- impact function owner is the relation owner.
DO $grant$
DECLARE
    projection RECORD;
BEGIN
    FOR projection IN
        SELECT view.view_oid::pg_catalog.regclass AS view_name,
               relation.relowner
        FROM synchro.sync_projection_views AS view
        JOIN pg_catalog.pg_class AS relation
          ON relation.oid = view.physical_relation_oid
    LOOP
        EXECUTE pg_catalog.format(
            'GRANT SELECT ON %s TO %I',
            projection.view_name,
            pg_catalog.pg_get_userbyid(projection.relowner)
        );
    END LOOP;
END
$grant$;

DROP FUNCTION synchro.synchro_execute_push_dml(TEXT, JSONB, TEXT);

CREATE FUNCTION synchro.synchro_execute_push_dml(
    p_sql TEXT,
    p_data JSONB,
    p_record_id TEXT,
    p_push_unit BOOLEAN
)
RETURNS TABLE (applied BOOLEAN, validation_failed BOOLEAN, policy_rejected BOOLEAN)
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
    v_rollback_unapplied BOOLEAN := false;
BEGIN
    applied := false;
    validation_failed := false;
    policy_rejected := false;
    BEGIN
        IF p_push_unit THEN
            SET CONSTRAINTS ALL DEFERRED;
        END IF;
        EXECUTE p_sql INTO applied USING p_data, p_record_id;
        applied := COALESCE(applied, false);
        IF NOT applied THEN
            -- A trigger can write other rows and then skip the target row. The raise rolls
            -- back those writes, because push rejects a mutation that applies no row.
            v_rollback_unapplied := true;
            RAISE EXCEPTION 'push source DML applied no row';
        END IF;
        IF p_push_unit THEN
            SET CONSTRAINTS ALL IMMEDIATE;
        END IF;
    EXCEPTION
        WHEN data_exception OR integrity_constraint_violation THEN
            applied := false;
            validation_failed := true;
        WHEN insufficient_privilege THEN
            applied := false;
            policy_rejected := true;
        WHEN raise_exception THEN
            IF NOT v_rollback_unapplied THEN
                RAISE;
            END IF;
    END;
    RETURN NEXT;
END;
$$;
ALTER FUNCTION synchro.synchro_execute_push_dml(TEXT, JSONB, TEXT, BOOLEAN)
    SECURITY DEFINER;
ALTER FUNCTION synchro.synchro_execute_push_dml(TEXT, JSONB, TEXT, BOOLEAN)
    SET search_path = pg_catalog, synchro, pg_temp;
ALTER FUNCTION synchro.synchro_execute_push_dml(TEXT, JSONB, TEXT, BOOLEAN)
    OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_execute_push_dml(TEXT, JSONB, TEXT, BOOLEAN)
    FROM PUBLIC;

CREATE FUNCTION synchro.synchro_check_push_constraints()
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
    SET CONSTRAINTS ALL IMMEDIATE;
    RETURN true;
EXCEPTION
    WHEN data_exception OR integrity_constraint_violation THEN
        RETURN false;
END;
$$;
ALTER FUNCTION synchro.synchro_check_push_constraints() SECURITY DEFINER;
ALTER FUNCTION synchro.synchro_check_push_constraints()
    SET search_path = pg_catalog, synchro, pg_temp;
ALTER FUNCTION synchro.synchro_check_push_constraints() OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_check_push_constraints() FROM PUBLIC;
-- A caller's temporary relation must not shadow an extension relation in a
-- privileged function, so pg_temp is last in each function path.
DO $search_path$
DECLARE
    object_record RECORD;
BEGIN
    FOR object_record IN
        SELECT namespace.nspname, procedure.proname,
               pg_catalog.pg_get_function_identity_arguments(procedure.oid) AS arguments
        FROM pg_catalog.pg_proc procedure
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
        JOIN pg_catalog.pg_depend dependency
          ON dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
         AND dependency.objid = procedure.oid
         AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
         AND dependency.deptype = 'e'
        JOIN pg_catalog.pg_extension extension
          ON extension.oid = dependency.refobjid
         AND extension.extname = 'synchro_pg'
        WHERE namespace.nspname = 'synchro'
          AND procedure.prokind = 'f'
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER FUNCTION %I.%I(%s) SET search_path = pg_catalog, synchro, pg_temp',
            object_record.nspname, object_record.proname, object_record.arguments
        );
    END LOOP;
END
$search_path$;

-- The processed boundary records WAL that the worker processed without a source transaction.
ALTER TABLE synchro.sync_wal_progress ADD COLUMN processed_end_lsn PG_LSN;
UPDATE synchro.sync_wal_progress
SET processed_end_lsn = COALESCE(materialized_end_lsn, generation_start_lsn)
WHERE singleton;
DO $progress_acknowledgement_check$
DECLARE
    constraint_name name;
BEGIN
    SELECT constraint_row.conname
    INTO STRICT constraint_name
    FROM pg_catalog.pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'synchro.sync_wal_progress'::pg_catalog.regclass
      AND constraint_row.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_row.oid) LIKE '%acknowledged_end_lsn%'
      AND pg_catalog.pg_get_constraintdef(constraint_row.oid) LIKE '%materialized_end_lsn%';
    EXECUTE pg_catalog.format(
        'ALTER TABLE synchro.sync_wal_progress DROP CONSTRAINT %I',
        constraint_name
    );
END
$progress_acknowledgement_check$;
ALTER TABLE synchro.sync_wal_progress
    ADD CONSTRAINT sync_wal_progress_processed_present
        CHECK ((generation_start_lsn IS NULL) = (processed_end_lsn IS NULL)),
    ADD CONSTRAINT sync_wal_progress_processed_after_start
        CHECK (processed_end_lsn IS NULL OR processed_end_lsn >= generation_start_lsn),
    ADD CONSTRAINT sync_wal_progress_materialized_processed
        CHECK (materialized_end_lsn IS NULL
               OR (processed_end_lsn IS NOT NULL AND materialized_end_lsn <= processed_end_lsn)),
    ADD CONSTRAINT sync_wal_progress_acknowledged_processed
        CHECK (acknowledged_end_lsn IS NULL
               OR (processed_end_lsn IS NOT NULL
                   AND acknowledged_end_lsn >= generation_start_lsn
                   AND acknowledged_end_lsn <= processed_end_lsn));

GRANT SELECT ON synchro.sync_extension_build TO synchro_worker;

UPDATE synchro.sync_extension_build
SET installed_fingerprint = synchro.synchro_build_fingerprint(),
    installed_at = pg_catalog.now()
WHERE singleton;
