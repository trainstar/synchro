-- Update synchro_pg from 0.3.2 to 0.4.0.
-- Stop if psql runs this file. ALTER EXTENSION removes the \echo line.
\echo Use "ALTER EXTENSION synchro_pg UPDATE TO '0.4.0'" to load this file. \quit

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
SET search_path = pg_catalog, synchro
AS 'MODULE_PATHNAME', 'synchro_register_assignment_function_wrapper';
ALTER FUNCTION synchro.synchro_register_assignment_function(TEXT, INT) OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_register_assignment_function(TEXT, INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION synchro.synchro_register_assignment_function(TEXT, INT)
    TO synchro_operator;

CREATE FUNCTION synchro.synchro_unregister_assignment_function() RETURNS void
STRICT
LANGUAGE c
SECURITY DEFINER
SET search_path = pg_catalog, synchro
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

UPDATE synchro.sync_extension_build
SET installed_fingerprint = synchro.synchro_build_fingerprint(),
    installed_at = pg_catalog.now()
WHERE singleton;
