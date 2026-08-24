use std::ffi::CString;

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

mod bgworker;
mod bucketing;
mod client;
mod compaction;
mod cursor_token;
mod health;
mod materialize;
mod portable_seed;
mod pull;
mod push;
mod rebuild;
mod rebuild_token;
mod registry;
mod schema;
mod seed_token;
mod stream_position;
mod stream_reset;
mod wal_decoder;

pgrx::pg_module_magic!();

pub(crate) const SOURCE_WRITE_GATE_LOCK_KEY: i64 = 0x7372_6365;
pub(crate) const WAL_WORKER_GATE_LOCK_KEY: i64 = 0x7761_6c72;
pub(crate) const STREAM_RESET_OPERATION_LOCK_KEY: i64 = 0x7273_746f;
pub(crate) const MEMBERSHIP_BACKFILL_LOCK_KEY: i64 = 0x6d65_6d62;

// ---------------------------------------------------------------------------
// Infrastructure tables (included in generated extension SQL)
// ---------------------------------------------------------------------------

pgrx::extension_sql!(
    r#"
CREATE TABLE IF NOT EXISTS sync_runtime_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    stream_generation TEXT NOT NULL,
    cursor_secret TEXT NOT NULL,
    active_slot_name NAME,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO sync_runtime_state (singleton, stream_generation, cursor_secret)
VALUES (
    true,
    gen_random_uuid(),
    replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', '')
)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE sync_token_keys (
    key_id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL CHECK (purpose IN (
        'incremental_cursor', 'rebuild_cursor', 'seed_page', 'seed_continuation'
    )),
    secret TEXT NOT NULL CHECK (length(secret) >= 64),
    state TEXT NOT NULL CHECK (state IN ('active', 'verify_only', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at TIMESTAMPTZ,
    CHECK ((state = 'retired') = (retired_at IS NOT NULL))
);
CREATE UNIQUE INDEX sync_token_keys_one_active_purpose
    ON sync_token_keys (purpose) WHERE state = 'active';
INSERT INTO sync_token_keys (key_id, purpose, secret, state)
SELECT purpose || '-v1', purpose,
       replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', ''),
       'active'
FROM unnest(ARRAY[
    'incremental_cursor', 'rebuild_cursor', 'seed_page', 'seed_continuation'
]) AS purpose;

CREATE TABLE IF NOT EXISTS sync_registry_generations (
    generation BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_generation TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'active', 'superseded')),
    validated BOOLEAN NOT NULL,
    activation_commit_lsn PG_LSN,
    activation_end_lsn PG_LSN,
    parent_generation BIGINT REFERENCES sync_registry_generations(generation),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at TIMESTAMPTZ,
    CHECK (
        (state = 'pending' AND activation_commit_lsn IS NULL AND activation_end_lsn IS NULL AND activated_at IS NULL)
        OR (state IN ('active', 'superseded') AND validated AND activated_at IS NOT NULL)
    ),
    CHECK (activation_commit_lsn IS NULL OR activation_end_lsn >= activation_commit_lsn)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_registry_one_active
    ON sync_registry_generations ((state)) WHERE state = 'active';
INSERT INTO sync_registry_generations (
    stream_generation,
    state,
    validated,
    activated_at
)
SELECT stream_generation, 'active', true, now()
FROM sync_runtime_state
WHERE singleton = true
  AND NOT EXISTS (SELECT 1 FROM sync_registry_generations);

CREATE TABLE IF NOT EXISTS sync_logical_ids (
    logical_id UUID PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('relation', 'table', 'field')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_registry (
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation) ON DELETE CASCADE,
    relation_id UUID NOT NULL REFERENCES sync_logical_ids(logical_id),
    registration_kind TEXT NOT NULL CHECK (registration_kind IN ('synced', 'capture_dependency')),
    table_id UUID REFERENCES sync_logical_ids(logical_id),
    primary_key_field_id UUID REFERENCES sync_logical_ids(logical_id),
    table_name TEXT NOT NULL,
    physical_schema NAME NOT NULL,
    physical_relation NAME NOT NULL,
    physical_relation_oid OID NOT NULL,
    replica_identity "char" NOT NULL CHECK (replica_identity = 'd'),
    composition TEXT CHECK (composition IN ('single_scope', 'multi_scope')),
    membership_function_oid OID,
    membership_function_schema NAME,
    membership_function_name NAME,
    membership_function_fingerprint BYTEA,
    max_scope_fanout INTEGER CHECK (max_scope_fanout > 0),
    pk_column TEXT NOT NULL DEFAULT 'id',
    pk_type TEXT NOT NULL DEFAULT 'uuid',
    pk_portable_type TEXT NOT NULL CHECK (pk_portable_type IN ('string', 'int', 'int64')),
    capture_key_columns TEXT[] NOT NULL DEFAULT '{}',
    updated_at_col TEXT NOT NULL DEFAULT 'updated_at',
    deleted_at_col TEXT NOT NULL DEFAULT 'deleted_at',
    push_policy TEXT NOT NULL DEFAULT 'enabled',
    sync_columns TEXT[] NOT NULL DEFAULT '{}',
    exclude_columns TEXT[] NOT NULL DEFAULT '{}',
    has_updated_at BOOLEAN NOT NULL DEFAULT true,
    has_deleted_at BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (registry_generation, relation_id),
    UNIQUE (registry_generation, table_id),
    UNIQUE (registry_generation, table_name),
    UNIQUE (registry_generation, physical_schema, physical_relation),
    UNIQUE (registry_generation, physical_relation_oid),
    CHECK (
        (registration_kind = 'synced'
         AND table_id IS NOT NULL
         AND primary_key_field_id IS NOT NULL
         AND composition IS NOT NULL
         AND membership_function_oid IS NOT NULL
         AND membership_function_schema IS NOT NULL
         AND membership_function_name IS NOT NULL
         AND octet_length(membership_function_fingerprint) = 32
         AND max_scope_fanout IS NOT NULL
         AND cardinality(capture_key_columns) = 1
         AND capture_key_columns[1] = pk_column)
        OR
        (registration_kind = 'capture_dependency'
         AND table_id IS NULL
         AND primary_key_field_id IS NULL
         AND composition IS NULL
         AND membership_function_oid IS NULL
         AND membership_function_schema IS NULL
         AND membership_function_name IS NULL
         AND membership_function_fingerprint IS NULL
         AND max_scope_fanout IS NULL
         AND push_policy = 'read_only'
         AND cardinality(sync_columns) = 0
         AND cardinality(capture_key_columns) > 0)
    )
);

CREATE TABLE IF NOT EXISTS sync_registry_fields (
    registry_generation BIGINT NOT NULL,
    relation_id UUID NOT NULL,
    field_id UUID NOT NULL REFERENCES sync_logical_ids(logical_id),
    physical_column NAME NOT NULL,
    portable_type TEXT NOT NULL,
    native_json BOOLEAN NOT NULL,
    decimal_precision INTEGER,
    decimal_scale INTEGER,
    nullable BOOLEAN NOT NULL,
    writable BOOLEAN NOT NULL,
    primary_key BOOLEAN NOT NULL,
    PRIMARY KEY (registry_generation, relation_id, field_id),
    UNIQUE (registry_generation, relation_id, physical_column),
    FOREIGN KEY (registry_generation, relation_id)
        REFERENCES sync_registry(registry_generation, relation_id) ON DELETE CASCADE,
    CHECK (NOT primary_key OR (NOT nullable AND NOT writable)),
    CHECK (NOT native_json OR portable_type = 'json'),
    CHECK (
        (portable_type = 'decimal' AND decimal_precision > 0 AND decimal_scale >= 0 AND decimal_scale <= decimal_precision)
        OR (portable_type <> 'decimal' AND decimal_precision IS NULL AND decimal_scale IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS sync_capture_dependency_fields (
    registry_generation BIGINT NOT NULL,
    relation_id UUID NOT NULL,
    physical_column NAME NOT NULL,
    portable_type TEXT NOT NULL,
    nullable BOOLEAN NOT NULL,
    capture_key BOOLEAN NOT NULL,
    PRIMARY KEY (registry_generation, relation_id, physical_column),
    FOREIGN KEY (registry_generation, relation_id)
        REFERENCES sync_registry(registry_generation, relation_id) ON DELETE CASCADE
);

CREATE TABLE sync_projection_views (
    physical_relation_oid OID PRIMARY KEY,
    physical_schema NAME NOT NULL,
    physical_relation NAME NOT NULL,
    view_oid OID NOT NULL UNIQUE,
    view_name NAME NOT NULL UNIQUE,
    projected_columns NAME[] NOT NULL CHECK (cardinality(projected_columns) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (physical_schema, physical_relation)
);

CREATE TABLE sync_membership_limits (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    max_scope_fanout INTEGER NOT NULL CHECK (max_scope_fanout > 0),
    max_impact_rows INTEGER NOT NULL CHECK (max_impact_rows > 0)
);
INSERT INTO sync_membership_limits (singleton, max_scope_fanout, max_impact_rows)
VALUES (true, 8, 1000);

CREATE TYPE synchro_row_ref AS (
    table_id UUID,
    pk_type TEXT,
    pk_value JSONB
);

CREATE TABLE sync_membership_dependencies (
    registry_generation BIGINT NOT NULL
        REFERENCES sync_registry_generations(generation) ON DELETE CASCADE,
    dependency_id UUID NOT NULL,
    dependency_relation_id UUID NOT NULL,
    dependency_registration_kind TEXT NOT NULL
        CHECK (dependency_registration_kind IN ('synced', 'capture_dependency')),
    target_relation_id UUID NOT NULL,
    impact_function_oid OID NOT NULL,
    impact_function_schema NAME NOT NULL,
    impact_function_name NAME NOT NULL,
    impact_function_fingerprint BYTEA NOT NULL
        CHECK (octet_length(impact_function_fingerprint) = 32),
    max_impact_rows INTEGER NOT NULL CHECK (max_impact_rows > 0),
    dependency_field_ids TEXT[] NOT NULL DEFAULT '{}',
    dependency_columns TEXT[] NOT NULL CHECK (cardinality(dependency_columns) > 0),
    PRIMARY KEY (registry_generation, dependency_id),
    UNIQUE (registry_generation, dependency_relation_id, target_relation_id),
    FOREIGN KEY (registry_generation, dependency_relation_id)
        REFERENCES sync_registry(registry_generation, relation_id) ON DELETE CASCADE,
    FOREIGN KEY (registry_generation, target_relation_id)
        REFERENCES sync_registry(registry_generation, relation_id) ON DELETE CASCADE,
    CHECK (dependency_relation_id <> target_relation_id),
    CHECK (
        (dependency_registration_kind = 'synced'
         AND cardinality(dependency_field_ids) > 0)
        OR
        (dependency_registration_kind = 'capture_dependency'
         AND cardinality(dependency_field_ids) = 0)
    )
);

CREATE TABLE sync_registry_membership_stages (
    registry_generation BIGINT PRIMARY KEY
        REFERENCES sync_registry_generations(generation) ON DELETE CASCADE,
    source_registry_generation BIGINT NOT NULL
        REFERENCES sync_registry_generations(generation),
    target_relation_ids UUID[] NOT NULL CHECK (cardinality(target_relation_ids) > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'activated')),
    stream_generation TEXT,
    activation_commit_lsn PG_LSN,
    activation_end_lsn PG_LSN,
    staged_record_count BIGINT CHECK (staged_record_count >= 0),
    staged_edge_count BIGINT CHECK (staged_edge_count >= 0),
    affected_scopes TEXT[],
    verified BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at TIMESTAMPTZ,
    CHECK (source_registry_generation < registry_generation),
    CHECK (
        (state = 'pending'
         AND stream_generation IS NULL
         AND activation_commit_lsn IS NULL
         AND activation_end_lsn IS NULL
         AND staged_record_count IS NULL
         AND staged_edge_count IS NULL
         AND affected_scopes IS NULL
         AND NOT verified
         AND activated_at IS NULL)
        OR
        (state = 'activated'
         AND stream_generation IS NOT NULL
         AND activation_commit_lsn IS NOT NULL
         AND activation_end_lsn >= activation_commit_lsn
         AND staged_record_count IS NOT NULL
         AND staged_edge_count IS NOT NULL
         AND affected_scopes IS NOT NULL
         AND verified
         AND activated_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS sync_changelog (
    seq BIGSERIAL PRIMARY KEY,
    bucket_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation SMALLINT NOT NULL,
    stream_generation TEXT,
    commit_lsn PG_LSN,
    event_ordinal BIGINT,
    effect_ordinal INTEGER,
    relation_id UUID,
    row_version UUID,
    projection_image TEXT CHECK (projection_image IN ('before', 'after')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sync_changelog_bucket_seq ON sync_changelog (bucket_id, seq);
CREATE INDEX IF NOT EXISTS idx_sync_changelog_record ON sync_changelog (table_name, record_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_changelog_stream_position
    ON sync_changelog (bucket_id, stream_generation, commit_lsn, event_ordinal, effect_ordinal)
    WHERE stream_generation IS NOT NULL;

CREATE TABLE IF NOT EXISTS sync_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT,
    platform TEXT NOT NULL DEFAULT '',
    app_version TEXT NOT NULL DEFAULT '',
    bucket_subs TEXT[] NOT NULL DEFAULT '{}',
    scope_set_version BIGINT NOT NULL DEFAULT 1,
    client_generation BIGINT NOT NULL DEFAULT 1,
    accepted_write_epoch BIGINT NOT NULL DEFAULT 1,
    generation_created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    generation_expires_at TIMESTAMPTZ,
    last_acknowledged_at TIMESTAMPTZ,
    last_sync_at TIMESTAMPTZ,
    last_pull_at TIMESTAMPTZ,
    last_push_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, client_id),
    CHECK (scope_set_version > 0 AND scope_set_version <= 9007199254740991),
    CHECK (client_generation > 0 AND client_generation <= 9007199254740991),
    CHECK (accepted_write_epoch > 0 AND accepted_write_epoch <= 9007199254740991),
    CHECK (generation_expires_at IS NULL OR generation_expires_at >= generation_created_at),
    CHECK (last_acknowledged_at IS NULL OR last_acknowledged_at >= generation_created_at)
);
ALTER TABLE sync_clients
    ADD COLUMN IF NOT EXISTS scope_set_version BIGINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_sync_clients_user_id ON sync_clients (user_id);

CREATE TABLE IF NOT EXISTS sync_client_retirements (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    retirement_id UUID NOT NULL DEFAULT gen_random_uuid(),
    retired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, client_id),
    UNIQUE (retirement_id)
);

CREATE TABLE sync_client_scope_history (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    client_generation BIGINT NOT NULL CHECK (client_generation > 0),
    scope_id TEXT NOT NULL,
    scope_set_version BIGINT NOT NULL CHECK (scope_set_version > 0),
    assigned BOOLEAN NOT NULL,
    assignment_source TEXT NOT NULL CHECK (assignment_source IN ('identity', 'shared', 'assignment_rule')),
    membership_generation BIGINT NOT NULL CHECK (membership_generation > 0),
    retention_generation BIGINT NOT NULL CHECK (retention_generation > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, client_id, client_generation, scope_id, scope_set_version),
    FOREIGN KEY (user_id, client_id) REFERENCES sync_clients (user_id, client_id) ON DELETE RESTRICT
);
CREATE INDEX sync_client_scope_history_lookup
    ON sync_client_scope_history (user_id, client_id, client_generation, scope_id, scope_set_version DESC);

CREATE OR REPLACE FUNCTION synchro_reject_client_retirement_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'client retirement markers are irreversible'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER synchro_client_retirement_irreversible
BEFORE UPDATE OR DELETE ON sync_client_retirements
FOR EACH ROW EXECUTE FUNCTION synchro_reject_client_retirement_mutation();

-- Protocol 3 push identity and outcome ledgers.  These rows are retained until
-- the scoped client has an irreversible retirement marker.
CREATE TABLE IF NOT EXISTS sync_push_batches (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    batch_id UUID NOT NULL,
    protocol_version INTEGER NOT NULL CHECK (protocol_version = 3),
    client_generation BIGINT NOT NULL,
    request_schema_version BIGINT NOT NULL,
    request_schema_hash TEXT NOT NULL CHECK (request_schema_hash ~ '^[0-9a-f]{64}$'),
    fingerprint_algorithm TEXT NOT NULL CHECK (fingerprint_algorithm = 'sha256'),
    fingerprint_version BIGINT NOT NULL CHECK (fingerprint_version = 1),
    fingerprint_domain TEXT NOT NULL CHECK (fingerprint_domain = 'synchro:v3:push-batch-fingerprint:v1'),
    fingerprint_digest BYTEA NOT NULL CHECK (octet_length(fingerprint_digest) = 32),
    sealed_canonical_request BYTEA NOT NULL,
    execution_state TEXT NOT NULL CHECK (execution_state IN ('executing', 'completed')),
    http_status INTEGER,
    sealed_canonical_response BYTEA,
    server_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (user_id, client_id, batch_id),
    CHECK (
        (execution_state = 'executing'
         AND http_status IS NULL
         AND sealed_canonical_response IS NULL
         AND server_time IS NULL
         AND completed_at IS NULL)
        OR
        (execution_state = 'completed'
         AND http_status = 200
         AND sealed_canonical_response IS NOT NULL
         AND server_time IS NOT NULL
         AND completed_at IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_sync_push_batches_client
    ON sync_push_batches (user_id, client_id, created_at);

CREATE TABLE IF NOT EXISTS sync_push_mutations (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    mutation_id UUID NOT NULL,
    fingerprint_algorithm TEXT NOT NULL CHECK (fingerprint_algorithm = 'sha256'),
    fingerprint_version BIGINT NOT NULL CHECK (fingerprint_version = 1),
    fingerprint_domain TEXT NOT NULL CHECK (fingerprint_domain = 'synchro:v3:push-mutation-fingerprint:v1'),
    fingerprint_digest BYTEA NOT NULL CHECK (octet_length(fingerprint_digest) = 32),
    first_batch_id UUID NOT NULL,
    request_ordinal INTEGER NOT NULL CHECK (request_ordinal > 0),
    authored_schema_version BIGINT NOT NULL,
    authored_schema_hash TEXT NOT NULL CHECK (authored_schema_hash ~ '^[0-9a-f]{64}$'),
    submitted_schema_version BIGINT NOT NULL,
    submitted_schema_hash TEXT NOT NULL CHECK (submitted_schema_hash ~ '^[0-9a-f]{64}$'),
    outcome_schema_version BIGINT NOT NULL,
    outcome_schema_hash TEXT NOT NULL CHECK (outcome_schema_hash ~ '^[0-9a-f]{64}$'),
    table_id TEXT NOT NULL,
    primary_key_field_id TEXT NOT NULL,
    primary_key_type TEXT NOT NULL,
    primary_key_value JSONB NOT NULL,
    row_identity BYTEA,
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    outcome_status TEXT NOT NULL CHECK (outcome_status IN ('applied', 'conflict', 'rejected_terminal')),
    rejection_code TEXT,
    sealed_canonical_request BYTEA NOT NULL,
    sealed_canonical_response BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, client_id, mutation_id),
    CHECK (
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
             'schema_incompatible', 'table_not_synced', 'policy_rejected', 'validation_failed'
         ))
    )
);
CREATE INDEX IF NOT EXISTS idx_sync_push_mutations_batch
    ON sync_push_mutations (user_id, client_id, first_batch_id, request_ordinal);

CREATE OR REPLACE FUNCTION synchro_reject_push_batch_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.execution_state = 'executing'
       AND NEW.execution_state = 'completed'
       AND NEW.user_id IS NOT DISTINCT FROM OLD.user_id
       AND NEW.client_id IS NOT DISTINCT FROM OLD.client_id
       AND NEW.batch_id IS NOT DISTINCT FROM OLD.batch_id
       AND NEW.protocol_version IS NOT DISTINCT FROM OLD.protocol_version
       AND NEW.client_generation IS NOT DISTINCT FROM OLD.client_generation
       AND NEW.request_schema_version IS NOT DISTINCT FROM OLD.request_schema_version
       AND NEW.request_schema_hash IS NOT DISTINCT FROM OLD.request_schema_hash
       AND NEW.fingerprint_algorithm IS NOT DISTINCT FROM OLD.fingerprint_algorithm
       AND NEW.fingerprint_version IS NOT DISTINCT FROM OLD.fingerprint_version
       AND NEW.fingerprint_domain IS NOT DISTINCT FROM OLD.fingerprint_domain
       AND NEW.fingerprint_digest IS NOT DISTINCT FROM OLD.fingerprint_digest
       AND NEW.sealed_canonical_request IS NOT DISTINCT FROM OLD.sealed_canonical_request
       AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1 FROM sync_client_retirements
           WHERE user_id = OLD.user_id AND client_id = OLD.client_id
       ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'push ledger rows are immutable outside client retirement'
        USING ERRCODE = '55000';
END;
$$;

CREATE OR REPLACE FUNCTION synchro_reject_push_mutation_ledger_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1 FROM sync_client_retirements
           WHERE user_id = OLD.user_id AND client_id = OLD.client_id
       ) THEN
        RETURN OLD;
    END IF;
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1
           FROM sync_registry_membership_stages stage
           WHERE stage.registry_generation::text = NULLIF(
                     current_setting('synchro.membership_activation_generation', true), ''
                 )
             AND stage.state = 'pending'
       ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'push ledger rows are immutable outside client retirement'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER synchro_push_batches_immutable
BEFORE UPDATE OR DELETE ON sync_push_batches
FOR EACH ROW EXECUTE FUNCTION synchro_reject_push_batch_mutation();

CREATE TRIGGER synchro_push_mutations_immutable
BEFORE UPDATE OR DELETE ON sync_push_mutations
FOR EACH ROW EXECUTE FUNCTION synchro_reject_push_mutation_ledger_mutation();

CREATE OR REPLACE FUNCTION synchro_execute_push_dml(
    p_sql TEXT,
    p_data JSONB,
    p_record_id TEXT
)
RETURNS TABLE (applied BOOLEAN, validation_failed BOOLEAN)
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
    applied := false;
    validation_failed := false;
    BEGIN
        EXECUTE p_sql INTO applied USING p_data, p_record_id;
        applied := COALESCE(applied, false);
    EXCEPTION
        WHEN data_exception OR integrity_constraint_violation THEN
            applied := false;
            validation_failed := true;
    END;
    RETURN NEXT;
END;
$$;

CREATE TABLE IF NOT EXISTS sync_shared_scopes (
    scope_id TEXT PRIMARY KEY,
    portable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_scope_state (
    scope_id TEXT PRIMARY KEY,
    stream_generation TEXT NOT NULL,
    membership_generation BIGINT NOT NULL DEFAULT 1 CHECK (membership_generation > 0),
    retention_generation BIGINT NOT NULL DEFAULT 1 CHECK (retention_generation > 0),
    floor_position_kind TEXT NOT NULL DEFAULT 'generation_start'
        CHECK (floor_position_kind IN ('generation_start', 'effect', 'transaction_end')),
    floor_commit_lsn PG_LSN,
    floor_event_ordinal BIGINT,
    floor_effect_ordinal INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (floor_position_kind = 'generation_start' AND floor_commit_lsn IS NULL AND floor_event_ordinal IS NULL AND floor_effect_ordinal IS NULL)
        OR (floor_position_kind = 'effect' AND floor_commit_lsn IS NOT NULL AND floor_event_ordinal >= 0 AND floor_effect_ordinal >= 0)
        OR (floor_position_kind = 'transaction_end' AND floor_commit_lsn IS NOT NULL AND floor_event_ordinal IS NULL AND floor_effect_ordinal IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS sync_bucket_edges (
    relation_id UUID NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
    row_version UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, record_id, bucket_id)
);
CREATE INDEX IF NOT EXISTS idx_sync_bucket_edges_bucket ON sync_bucket_edges (bucket_id, table_name, record_id);

CREATE TABLE IF NOT EXISTS sync_scope_digest_cache (
    scope_id TEXT PRIMARY KEY,
    edge_change_xid XID8 NOT NULL,
    schema_hash BYTEA CHECK (schema_hash IS NULL OR octet_length(schema_hash) = 32),
    digest BYTEA CHECK (digest IS NULL OR octet_length(digest) = 32),
    CHECK ((schema_hash IS NULL) = (digest IS NULL))
);

CREATE OR REPLACE FUNCTION sync_lock_scope_digest_boundary()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, synchro
AS $$
BEGIN
    LOCK TABLE synchro.sync_wal_progress IN ROW EXCLUSIVE MODE;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION sync_invalidate_scope_digest()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, synchro
AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        INSERT INTO synchro.sync_scope_digest_cache AS cache (scope_id, edge_change_xid)
        VALUES (OLD.bucket_id, pg_current_xact_id())
        ON CONFLICT (scope_id) DO UPDATE
        SET edge_change_xid = EXCLUDED.edge_change_xid,
            schema_hash = NULL,
            digest = NULL
        WHERE cache.edge_change_xid <> EXCLUDED.edge_change_xid
           OR cache.digest IS NOT NULL;
    END IF;

    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.bucket_id IS DISTINCT FROM OLD.bucket_id) THEN
        INSERT INTO synchro.sync_scope_digest_cache AS cache (scope_id, edge_change_xid)
        VALUES (NEW.bucket_id, pg_current_xact_id())
        ON CONFLICT (scope_id) DO UPDATE
        SET edge_change_xid = EXCLUDED.edge_change_xid,
            schema_hash = NULL,
            digest = NULL
        WHERE cache.edge_change_xid <> EXCLUDED.edge_change_xid
           OR cache.digest IS NOT NULL;
    END IF;

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS sync_lock_scope_digest_boundary ON sync_bucket_edges;
CREATE TRIGGER sync_lock_scope_digest_boundary
BEFORE INSERT OR UPDATE OR DELETE ON sync_bucket_edges
FOR EACH STATEMENT EXECUTE FUNCTION sync_lock_scope_digest_boundary();

DROP TRIGGER IF EXISTS sync_invalidate_scope_digest ON sync_bucket_edges;
CREATE TRIGGER sync_invalidate_scope_digest
AFTER INSERT OR UPDATE OR DELETE ON sync_bucket_edges
FOR EACH ROW EXECUTE FUNCTION sync_invalidate_scope_digest();

CREATE TABLE IF NOT EXISTS sync_rule_failures (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation SMALLINT NOT NULL,
    error_text TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sync_rule_failures_created ON sync_rule_failures (created_at);

CREATE TABLE IF NOT EXISTS sync_stream_resets (
    reset_id UUID PRIMARY KEY,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('stream_reset', 'projection_bootstrap')),
    source_stream_generation TEXT NOT NULL,
    target_stream_generation TEXT NOT NULL,
    source_registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    target_registry_generation BIGINT REFERENCES sync_registry_generations(generation),
    old_slot_name NAME NOT NULL,
    candidate_slot_name NAME NOT NULL,
    database_oid OID NOT NULL,
    database_name NAME NOT NULL,
    plugin TEXT NOT NULL CHECK (plugin = 'pgoutput'),
    consistent_point PG_LSN,
    exported_snapshot_name TEXT,
    snapshot_before_xid XID8,
    snapshot_after_xid XID8,
    snapshot_before_nonce UUID,
    snapshot_after_nonce UUID,
    activation_barrier PG_LSN,
    target_schema_version BIGINT,
    target_schema_hash TEXT,
    target_canonical_manifest_body TEXT,
    candidate_materialized_commit_lsn PG_LSN,
    candidate_materialized_end_lsn PG_LSN,
    candidate_acknowledged_end_lsn PG_LSN,
    candidate_verified BOOLEAN NOT NULL DEFAULT false,
    affected_scopes TEXT[],
    lifecycle TEXT NOT NULL CHECK (lifecycle IN (
        'preparing', 'baseline_staged', 'catching_up', 'activated', 'aborted', 'cleanup_complete'
    )),
    staged_row_count BIGINT CHECK (staged_row_count >= 0),
    staged_version_count BIGINT CHECK (staged_version_count >= 0),
    staged_edge_count BIGINT CHECK (staged_edge_count >= 0),
    staged_fence_count BIGINT CHECK (staged_fence_count >= 0),
    staged_scope_count BIGINT CHECK (staged_scope_count >= 0),
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    baseline_staged_at TIMESTAMPTZ,
    activated_at TIMESTAMPTZ,
    aborted_at TIMESTAMPTZ,
    cleanup_completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (operation_kind = 'stream_reset'
         AND source_stream_generation <> target_stream_generation
         AND target_registry_generation IS NULL
         AND target_schema_version IS NULL
         AND target_schema_hash IS NULL
         AND target_canonical_manifest_body IS NULL)
        OR
        (operation_kind = 'projection_bootstrap'
         AND source_stream_generation = target_stream_generation
         AND target_registry_generation IS NOT NULL
         AND target_registry_generation <> source_registry_generation
         AND (
             (target_schema_version IS NULL
              AND target_schema_hash IS NULL
              AND target_canonical_manifest_body IS NULL)
             OR
             (target_schema_version IS NOT NULL
              AND target_schema_hash ~ '^[0-9a-f]{64}$'
              AND target_canonical_manifest_body IS NOT NULL)
         ))
    ),
    CHECK (old_slot_name <> candidate_slot_name),
    CHECK (length(old_slot_name::text) BETWEEN 1 AND 63),
    CHECK (length(candidate_slot_name::text) BETWEEN 1 AND 63),
    CHECK (candidate_slot_name::text ~ '^[a-z0-9_]+$'),
    CHECK (exported_snapshot_name IS NULL OR (
        length(exported_snapshot_name) BETWEEN 1 AND 128
        AND exported_snapshot_name ~ '^[A-Za-z0-9_-]+$'
    )),
    CHECK ((consistent_point IS NULL) = (exported_snapshot_name IS NULL)),
    CHECK (
        (lifecycle = 'preparing'
         AND consistent_point IS NULL
         AND activation_barrier IS NULL
         AND baseline_staged_at IS NULL
         AND activated_at IS NULL
         AND aborted_at IS NULL
         AND cleanup_completed_at IS NULL)
        OR (lifecycle = 'baseline_staged'
            AND consistent_point IS NOT NULL
            AND activation_barrier IS NULL
            AND baseline_staged_at IS NOT NULL
         AND activated_at IS NULL
         AND aborted_at IS NULL
         AND cleanup_completed_at IS NULL)
        OR (lifecycle = 'catching_up'
            AND operation_kind = 'projection_bootstrap'
            AND consistent_point IS NOT NULL
            AND activation_barrier IS NOT NULL
            AND baseline_staged_at IS NOT NULL
            AND activated_at IS NULL
            AND aborted_at IS NULL
            AND cleanup_completed_at IS NULL)
        OR (lifecycle = 'activated'
            AND consistent_point IS NOT NULL
            AND activation_barrier IS NOT NULL
            AND (
                (operation_kind = 'stream_reset' AND activation_barrier = consistent_point)
                OR
                (operation_kind = 'projection_bootstrap'
                 AND activation_barrier > consistent_point
                 AND candidate_materialized_end_lsn = activation_barrier
                 AND candidate_acknowledged_end_lsn = activation_barrier
                 AND candidate_verified)
            )
            AND baseline_staged_at IS NOT NULL
            AND activated_at IS NOT NULL
            AND aborted_at IS NULL
            AND cleanup_completed_at IS NULL)
        OR (lifecycle = 'aborted'
            AND activation_barrier IS NULL
            AND activated_at IS NULL
            AND aborted_at IS NOT NULL
            AND cleanup_completed_at IS NULL)
        OR (lifecycle = 'cleanup_complete'
            AND consistent_point IS NOT NULL
            AND activation_barrier IS NOT NULL
            AND (
                (operation_kind = 'stream_reset' AND activation_barrier = consistent_point)
                OR
                (operation_kind = 'projection_bootstrap'
                 AND activation_barrier > consistent_point
                 AND candidate_materialized_end_lsn = activation_barrier
                 AND candidate_acknowledged_end_lsn = activation_barrier
                 AND candidate_verified)
            )
            AND baseline_staged_at IS NOT NULL
            AND activated_at IS NOT NULL
            AND aborted_at IS NULL
            AND cleanup_completed_at IS NOT NULL)
    ),
    CHECK (
        (lifecycle = 'preparing' AND staged_row_count IS NULL
         AND staged_version_count IS NULL AND staged_edge_count IS NULL
         AND staged_fence_count IS NULL AND staged_scope_count IS NULL)
        OR lifecycle = 'aborted'
        OR (lifecycle IN ('baseline_staged', 'catching_up', 'activated', 'cleanup_complete')
            AND staged_row_count IS NOT NULL
            AND staged_version_count IS NOT NULL
            AND staged_edge_count IS NOT NULL
            AND staged_fence_count IS NOT NULL
            AND staged_scope_count IS NOT NULL)
    ),
    CHECK ((candidate_materialized_commit_lsn IS NULL) = (candidate_materialized_end_lsn IS NULL)),
    CHECK (candidate_materialized_commit_lsn IS NULL OR candidate_materialized_end_lsn >= candidate_materialized_commit_lsn),
    CHECK (candidate_acknowledged_end_lsn IS NULL OR consistent_point IS NOT NULL),
    CHECK (candidate_acknowledged_end_lsn IS NULL OR candidate_acknowledged_end_lsn >= consistent_point),
    CHECK (candidate_acknowledged_end_lsn IS NULL OR candidate_materialized_end_lsn IS NULL OR candidate_acknowledged_end_lsn <= candidate_materialized_end_lsn)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_stream_resets_one_nonterminal
    ON sync_stream_resets ((true))
    WHERE lifecycle IN ('preparing', 'baseline_staged', 'catching_up', 'activated');

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_projection_bootstrap_generation
    ON sync_stream_resets (target_registry_generation)
    WHERE operation_kind = 'projection_bootstrap'
      AND lifecycle IN ('preparing', 'baseline_staged', 'catching_up', 'activated');

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_stream_reset_target_generation
    ON sync_stream_resets (target_stream_generation)
    WHERE operation_kind = 'stream_reset';

CREATE TABLE IF NOT EXISTS sync_stream_reset_snapshot_markers (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    phase TEXT NOT NULL CHECK (phase IN ('before', 'after')),
    marker_xid XID8 NOT NULL,
    marker_nonce UUID NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, phase),
    UNIQUE (reset_id, marker_xid)
);

CREATE TABLE IF NOT EXISTS sync_projection_bootstrap_transactions (
    bootstrap_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    commit_lsn PG_LSN NOT NULL,
    end_lsn PG_LSN NOT NULL,
    source_xid XID NOT NULL,
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    event_count BIGINT NOT NULL CHECK (event_count >= 0),
    content_hash BYTEA NOT NULL CHECK (octet_length(content_hash) = 32),
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replay_count BIGINT NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    PRIMARY KEY (bootstrap_id, commit_lsn),
    UNIQUE (bootstrap_id, end_lsn),
    CHECK (end_lsn >= commit_lsn)
);

CREATE TABLE IF NOT EXISTS sync_write_fences (
    fence_id UUID PRIMARY KEY,
    transaction_xid XID8 NOT NULL,
    dml_ordinal BIGINT NOT NULL CHECK (dml_ordinal > 0),
    relation_id UUID NOT NULL,
    registration_kind TEXT NOT NULL CHECK (registration_kind IN ('synced', 'capture_dependency')),
    table_id UUID REFERENCES sync_logical_ids(logical_id),
    physical_schema NAME NOT NULL,
    physical_relation NAME NOT NULL,
    physical_relation_oid OID NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    old_record_id TEXT,
    new_record_id TEXT,
    old_capture_key JSONB,
    new_capture_key JSONB,
    row_version UUID NOT NULL,
    mutation_id TEXT,
    user_id TEXT,
    client_id TEXT,
    coverage TEXT NOT NULL DEFAULT 'pending' CHECK (coverage IN (
        'pending', 'materialized', 'reset_baseline',
        'projection_bootstrap_baseline', 'projection_bootstrap'
    )),
    stream_generation TEXT,
    commit_lsn PG_LSN,
    event_ordinal BIGINT,
    reset_id UUID REFERENCES sync_stream_resets(reset_id),
    reset_slot_name NAME,
    reset_consistent_point PG_LSN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    materialized_at TIMESTAMPTZ,
    CHECK (
        (registration_kind = 'synced'
         AND table_id IS NOT NULL
         AND old_capture_key IS NULL
         AND new_capture_key IS NULL
         AND (operation = 'insert') = (old_record_id IS NULL)
         AND (operation = 'delete') = (new_record_id IS NULL))
        OR
        (registration_kind = 'capture_dependency'
         AND table_id IS NULL
         AND old_record_id IS NULL
         AND new_record_id IS NULL
         AND (operation = 'insert') = (old_capture_key IS NULL)
         AND (operation = 'delete') = (new_capture_key IS NULL)
         AND (old_capture_key IS NULL OR jsonb_typeof(old_capture_key) = 'object')
         AND (new_capture_key IS NULL OR jsonb_typeof(new_capture_key) = 'object'))
    ),
    CHECK (
        (coverage = 'pending'
         AND stream_generation IS NULL
         AND commit_lsn IS NULL
         AND event_ordinal IS NULL
         AND materialized_at IS NULL
         AND reset_id IS NULL
         AND reset_slot_name IS NULL
         AND reset_consistent_point IS NULL)
        OR (coverage = 'materialized'
            AND stream_generation IS NOT NULL
            AND commit_lsn IS NOT NULL
            AND event_ordinal >= 0
            AND materialized_at IS NOT NULL
            AND reset_id IS NULL
            AND reset_slot_name IS NULL
            AND reset_consistent_point IS NULL)
        OR (coverage = 'reset_baseline'
            AND stream_generation IS NOT NULL
            AND commit_lsn IS NULL
            AND event_ordinal IS NULL
            AND materialized_at IS NOT NULL
            AND reset_id IS NOT NULL
            AND reset_slot_name IS NOT NULL
            AND reset_consistent_point IS NOT NULL)
        OR (coverage = 'projection_bootstrap'
            AND stream_generation IS NOT NULL
            AND commit_lsn IS NOT NULL
            AND event_ordinal >= 0
            AND materialized_at IS NOT NULL
            AND reset_id IS NOT NULL
            AND reset_slot_name IS NOT NULL
            AND reset_consistent_point IS NOT NULL)
        OR (coverage = 'projection_bootstrap_baseline'
            AND stream_generation IS NOT NULL
            AND commit_lsn IS NULL
            AND event_ordinal IS NULL
            AND materialized_at IS NOT NULL
            AND reset_id IS NOT NULL
            AND reset_slot_name IS NOT NULL
            AND reset_consistent_point IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_sync_write_fences_pending
    ON sync_write_fences (relation_id, created_at) WHERE coverage = 'pending';

CREATE TABLE IF NOT EXISTS sync_projection_bootstrap_events (
    bootstrap_id UUID NOT NULL,
    commit_lsn PG_LSN NOT NULL,
    event_ordinal BIGINT NOT NULL CHECK (event_ordinal >= 0),
    relation_id UUID NOT NULL,
    registration_kind TEXT NOT NULL CHECK (registration_kind IN ('synced', 'capture_dependency')),
    physical_schema NAME NOT NULL,
    physical_relation NAME NOT NULL,
    physical_relation_oid OID NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    fence_id UUID NOT NULL REFERENCES sync_write_fences(fence_id),
    PRIMARY KEY (bootstrap_id, commit_lsn, event_ordinal),
    UNIQUE (bootstrap_id, fence_id),
    FOREIGN KEY (bootstrap_id, commit_lsn)
        REFERENCES sync_projection_bootstrap_transactions(bootstrap_id, commit_lsn)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sync_row_versions (
    relation_id UUID NOT NULL,
    record_id TEXT NOT NULL,
    row_version UUID NOT NULL,
    fence_id UUID REFERENCES sync_write_fences(fence_id),
    reset_id UUID REFERENCES sync_stream_resets(reset_id),
    deleted BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relation_id, record_id),
    CHECK (num_nonnulls(fence_id, reset_id) = 1)
);

CREATE TABLE IF NOT EXISTS sync_wal_transactions (
    stream_generation TEXT NOT NULL,
    commit_lsn PG_LSN NOT NULL,
    end_lsn PG_LSN NOT NULL,
    source_xid XID NOT NULL,
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    event_count BIGINT NOT NULL CHECK (event_count >= 0),
    effect_count BIGINT NOT NULL CHECK (effect_count >= 0),
    content_hash BYTEA NOT NULL CHECK (octet_length(content_hash) = 32),
    commit_timestamp TIMESTAMPTZ NOT NULL,
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replay_count BIGINT NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    PRIMARY KEY (stream_generation, commit_lsn),
    UNIQUE (stream_generation, end_lsn),
    CHECK (end_lsn >= commit_lsn)
);

CREATE TABLE IF NOT EXISTS sync_wal_events (
    stream_generation TEXT NOT NULL,
    commit_lsn PG_LSN NOT NULL,
    event_ordinal BIGINT NOT NULL CHECK (event_ordinal >= 0),
    relation_id UUID NOT NULL,
    registration_kind TEXT NOT NULL CHECK (registration_kind IN ('synced', 'capture_dependency')),
    physical_schema NAME NOT NULL,
    physical_relation NAME NOT NULL,
    physical_relation_oid OID NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    fence_id UUID NOT NULL REFERENCES sync_write_fences(fence_id),
    PRIMARY KEY (stream_generation, commit_lsn, event_ordinal),
    UNIQUE (fence_id),
    FOREIGN KEY (stream_generation, commit_lsn)
        REFERENCES sync_wal_transactions(stream_generation, commit_lsn) ON DELETE CASCADE
);
ALTER TABLE sync_changelog
    ADD CONSTRAINT sync_changelog_source_event_fk
    FOREIGN KEY (stream_generation, commit_lsn, event_ordinal)
    REFERENCES sync_wal_events(stream_generation, commit_lsn, event_ordinal);

CREATE TABLE IF NOT EXISTS sync_captured_rows (
    relation_id UUID NOT NULL,
    record_id TEXT NOT NULL,
    row_data JSONB NOT NULL,
    row_version UUID NOT NULL,
    checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
    deleted BOOLEAN NOT NULL,
    source_stream_generation TEXT NOT NULL,
    source_commit_lsn PG_LSN,
    source_event_ordinal BIGINT,
    source_reset_id UUID REFERENCES sync_stream_resets(reset_id),
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relation_id, record_id),
    CHECK (
        (source_reset_id IS NULL
         AND source_commit_lsn IS NOT NULL
         AND source_event_ordinal >= 0)
        OR (source_reset_id IS NOT NULL
            AND source_commit_lsn IS NULL
            AND source_event_ordinal IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS sync_capture_dependency_rows (
    relation_id UUID NOT NULL,
    capture_key JSONB NOT NULL CHECK (jsonb_typeof(capture_key) = 'object'),
    row_data JSONB NOT NULL CHECK (jsonb_typeof(row_data) = 'object'),
    deleted BOOLEAN NOT NULL,
    source_stream_generation TEXT NOT NULL,
    source_commit_lsn PG_LSN,
    source_event_ordinal BIGINT,
    source_reset_id UUID REFERENCES sync_stream_resets(reset_id),
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relation_id, capture_key),
    CHECK (
        (source_reset_id IS NULL
         AND source_commit_lsn IS NOT NULL
         AND source_event_ordinal >= 0)
        OR (source_reset_id IS NOT NULL
            AND source_commit_lsn IS NULL
            AND source_event_ordinal IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS sync_capture_dependency_projections (
    stream_generation TEXT NOT NULL,
    commit_lsn PG_LSN NOT NULL,
    event_ordinal BIGINT NOT NULL CHECK (event_ordinal >= 0),
    relation_id UUID NOT NULL,
    image_kind TEXT NOT NULL CHECK (image_kind IN ('before', 'after')),
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    capture_key JSONB NOT NULL CHECK (jsonb_typeof(capture_key) = 'object'),
    row_data JSONB NOT NULL CHECK (jsonb_typeof(row_data) = 'object'),
    deleted BOOLEAN NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        stream_generation,
        commit_lsn,
        event_ordinal,
        relation_id,
        image_kind
    )
);
CREATE INDEX IF NOT EXISTS idx_sync_capture_dependency_projections_record
    ON sync_capture_dependency_projections (relation_id, capture_key, commit_lsn, event_ordinal);

CREATE TABLE IF NOT EXISTS sync_stream_reset_row_versions (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    relation_id UUID NOT NULL,
    record_id TEXT NOT NULL,
    row_version UUID NOT NULL,
    fence_id UUID REFERENCES sync_write_fences(fence_id),
    source_reset_id UUID REFERENCES sync_stream_resets(reset_id),
    deleted BOOLEAN NOT NULL,
    baseline_generated BOOLEAN NOT NULL,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, relation_id, record_id),
    CHECK (num_nonnulls(fence_id, source_reset_id) = 1),
    CHECK (NOT baseline_generated OR source_reset_id = reset_id)
);

CREATE TABLE IF NOT EXISTS sync_stream_reset_captured_rows (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    relation_id UUID NOT NULL,
    record_id TEXT NOT NULL,
    row_data JSONB NOT NULL CHECK (jsonb_typeof(row_data) = 'object'),
    row_version UUID NOT NULL,
    checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
    deleted BOOLEAN NOT NULL,
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, relation_id, record_id),
    FOREIGN KEY (reset_id, relation_id, record_id)
        REFERENCES sync_stream_reset_row_versions(reset_id, relation_id, record_id)
);

CREATE TABLE IF NOT EXISTS sync_stream_reset_capture_dependency_rows (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    relation_id UUID NOT NULL,
    capture_key JSONB NOT NULL CHECK (jsonb_typeof(capture_key) = 'object'),
    row_data JSONB NOT NULL CHECK (jsonb_typeof(row_data) = 'object'),
    deleted BOOLEAN NOT NULL,
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, relation_id, capture_key)
);

CREATE TABLE IF NOT EXISTS sync_stream_reset_membership_edges (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    relation_id UUID NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    scope_id TEXT NOT NULL CHECK (scope_id <> ''),
    checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
    row_version UUID NOT NULL,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, table_name, record_id, scope_id),
    FOREIGN KEY (reset_id, relation_id, record_id)
        REFERENCES sync_stream_reset_captured_rows(reset_id, relation_id, record_id)
);

CREATE TABLE IF NOT EXISTS sync_stream_reset_fence_coverage (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    fence_id UUID NOT NULL REFERENCES sync_write_fences(fence_id),
    relation_id UUID NOT NULL,
    registration_kind TEXT NOT NULL CHECK (registration_kind IN ('synced', 'capture_dependency')),
    table_id UUID REFERENCES sync_logical_ids(logical_id),
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    old_record_id TEXT,
    new_record_id TEXT,
    old_capture_key JSONB,
    new_capture_key JSONB,
    row_version UUID NOT NULL,
    candidate_slot_name NAME NOT NULL,
    consistent_point PG_LSN NOT NULL,
    target_stream_generation TEXT NOT NULL,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, fence_id),
    CHECK (
        (registration_kind = 'synced'
         AND table_id IS NOT NULL
         AND old_capture_key IS NULL
         AND new_capture_key IS NULL
         AND (operation = 'insert') = (old_record_id IS NULL)
         AND (operation = 'delete') = (new_record_id IS NULL))
        OR
        (registration_kind = 'capture_dependency'
         AND table_id IS NULL
         AND old_record_id IS NULL
         AND new_record_id IS NULL
         AND (operation = 'insert') = (old_capture_key IS NULL)
         AND (operation = 'delete') = (new_capture_key IS NULL)
         AND (old_capture_key IS NULL OR jsonb_typeof(old_capture_key) = 'object')
         AND (new_capture_key IS NULL OR jsonb_typeof(new_capture_key) = 'object'))
    )
);

CREATE TABLE IF NOT EXISTS sync_stream_reset_scope_digests (
    reset_id UUID NOT NULL REFERENCES sync_stream_resets(reset_id),
    scope_id TEXT NOT NULL CHECK (scope_id <> ''),
    schema_hash TEXT NOT NULL CHECK (schema_hash ~ '^[0-9a-f]{64}$'),
    digest BYTEA NOT NULL CHECK (octet_length(digest) = 32),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reset_id, scope_id)
);

CREATE TABLE IF NOT EXISTS sync_captured_projections (
    stream_generation TEXT NOT NULL,
    commit_lsn PG_LSN NOT NULL,
    event_ordinal BIGINT NOT NULL,
    relation_id UUID NOT NULL,
    image_kind TEXT NOT NULL CHECK (image_kind IN ('before', 'after')),
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    record_id TEXT NOT NULL,
    row_data JSONB NOT NULL,
    row_version UUID NOT NULL,
    checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
    deleted BOOLEAN NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        stream_generation,
        commit_lsn,
        event_ordinal,
        relation_id,
        image_kind,
        record_id
    )
);
CREATE INDEX IF NOT EXISTS idx_sync_captured_projections_record
    ON sync_captured_projections (relation_id, record_id, commit_lsn, event_ordinal);

CREATE TABLE IF NOT EXISTS sync_wal_progress (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    stream_generation TEXT NOT NULL,
    generation_start_lsn PG_LSN,
    materialized_commit_lsn PG_LSN,
    materialized_end_lsn PG_LSN,
    acknowledged_end_lsn PG_LSN,
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((materialized_commit_lsn IS NULL) = (materialized_end_lsn IS NULL)),
    CHECK (materialized_commit_lsn IS NULL OR materialized_end_lsn >= materialized_commit_lsn),
    CHECK (acknowledged_end_lsn IS NULL OR materialized_end_lsn IS NOT NULL AND acknowledged_end_lsn <= materialized_end_lsn)
);
INSERT INTO sync_wal_progress (singleton, stream_generation, registry_generation)
SELECT true, rs.stream_generation, rg.generation
FROM sync_runtime_state rs
JOIN sync_registry_generations rg ON rg.stream_generation = rs.stream_generation AND rg.state = 'active'
WHERE rs.singleton = true
ON CONFLICT (singleton) DO NOTHING;

CREATE VIEW sync_current_projections
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
    FROM sync_captured_rows captured
    CROSS JOIN reset_context context
    WHERE context.reset_id IS NULL
    UNION ALL
    SELECT captured.relation_id,
           captured.record_id,
           NULL::jsonb AS capture_key,
           captured.row_data,
           captured.deleted
    FROM sync_stream_reset_captured_rows captured
    CROSS JOIN reset_context context
    WHERE captured.reset_id = context.reset_id
    UNION ALL
    SELECT captured.relation_id,
           NULL::text AS record_id,
           captured.capture_key,
           captured.row_data,
           captured.deleted
    FROM sync_capture_dependency_rows captured
    CROSS JOIN reset_context context
    WHERE context.reset_id IS NULL
    UNION ALL
    SELECT captured.relation_id,
           NULL::text AS record_id,
           captured.capture_key,
           captured.row_data,
           captured.deleted
    FROM sync_stream_reset_capture_dependency_rows captured
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
       captured.deleted
FROM sync_wal_progress progress
CROSS JOIN reset_context context
JOIN sync_registry registry
  ON registry.registry_generation = COALESCE(context.registry_generation, progress.registry_generation)
JOIN projections captured
  ON captured.relation_id = registry.relation_id
WHERE progress.singleton;

CREATE TABLE IF NOT EXISTS sync_wal_poison (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_generation TEXT NOT NULL,
    commit_lsn PG_LSN NOT NULL,
    failure_class TEXT NOT NULL CHECK (failure_class IN (
        'decode_failed',
        'validation_failed',
        'fence_correlation_failed',
        'materialization_failed',
        'projection_write_failed',
        'scope_evaluation_failed',
        'transaction_commit_failed',
        'truncate_unsupported',
        'registered_relation_drift'
    )),
    relation_id UUID,
    lifecycle TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle IN ('active', 'repaired', 'reset')),
    poisoned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    retry_requested_at TIMESTAMPTZ,
    attempt_count BIGINT NOT NULL DEFAULT 1 CHECK (attempt_count > 0),
    CHECK ((lifecycle = 'active') = (resolved_at IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_wal_one_active_poison
    ON sync_wal_poison ((lifecycle)) WHERE lifecycle = 'active';

CREATE TABLE IF NOT EXISTS sync_wal_worker_state (
    worker_id TEXT PRIMARY KEY,
    database_oid OID NOT NULL,
    database_name NAME NOT NULL,
    worker_login_oid OID NOT NULL,
    backend_pid INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('starting', 'running', 'blocked', 'stopped')),
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    materialized_commit_lsn PG_LSN,
    materialized_end_lsn PG_LSN,
    oldest_unmaterialized_commit_timestamp TIMESTAMPTZ,
    wal_observed_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_schema_manifest (
    schema_version BIGINT PRIMARY KEY,
    schema_hash TEXT NOT NULL CHECK (schema_hash ~ '^[0-9a-f]{64}$'),
    canonical_manifest_body TEXT NOT NULL,
    parent_schema_version BIGINT,
    parent_schema_hash TEXT,
    transition_class TEXT NOT NULL CHECK (transition_class IN ('initial', 'class_2', 'class_3', 'class_4')),
    compatibility_floor BIGINT NOT NULL CHECK (compatibility_floor > 0),
    affected_scopes TEXT[] NOT NULL DEFAULT '{}',
    registry_generation BIGINT NOT NULL REFERENCES sync_registry_generations(generation),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (schema_version > 0),
    CHECK ((parent_schema_version IS NULL) = (parent_schema_hash IS NULL)),
    CHECK ((transition_class = 'initial') = (parent_schema_version IS NULL)),
    CHECK (parent_schema_version IS NULL OR parent_schema_version < schema_version),
    CHECK (parent_schema_hash IS NULL OR parent_schema_hash ~ '^[0-9a-f]{64}$'),
    CHECK (compatibility_floor <= schema_version),
    CHECK (transition_class = 'class_3' OR cardinality(affected_scopes) = 0),
    UNIQUE (schema_version, schema_hash),
    FOREIGN KEY (parent_schema_version, parent_schema_hash)
        REFERENCES sync_schema_manifest(schema_version, schema_hash)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_schema_manifest_hash ON sync_schema_manifest (schema_hash);

CREATE OR REPLACE FUNCTION synchro_reject_manifest_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'published schema manifests are immutable'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER synchro_schema_manifest_immutable
BEFORE UPDATE OR DELETE ON sync_schema_manifest
FOR EACH ROW EXECUTE FUNCTION synchro_reject_manifest_mutation();

CREATE TABLE IF NOT EXISTS sync_client_checkpoints (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    stream_generation TEXT NOT NULL,
    position_kind TEXT NOT NULL CHECK (position_kind IN ('generation_start', 'effect', 'transaction_end')),
    commit_lsn PG_LSN,
    event_ordinal BIGINT,
    effect_ordinal INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, client_id, bucket_id),
    CHECK (
        (position_kind = 'generation_start' AND commit_lsn IS NULL AND event_ordinal IS NULL AND effect_ordinal IS NULL)
        OR (position_kind = 'effect' AND commit_lsn IS NOT NULL AND event_ordinal >= 0 AND effect_ordinal >= 0)
        OR (position_kind = 'transaction_end' AND commit_lsn IS NOT NULL AND event_ordinal IS NULL AND effect_ordinal IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS sync_rebuild_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    rebuild_id UUID NOT NULL,
    scope_id TEXT NOT NULL,
    client_generation BIGINT NOT NULL CHECK (client_generation > 0),
    schema_version BIGINT NOT NULL CHECK (schema_version > 0),
    schema_hash TEXT NOT NULL CHECK (schema_hash ~ '^[0-9a-f]{64}$'),
    stream_generation TEXT NOT NULL,
    membership_generation BIGINT NOT NULL CHECK (membership_generation > 0),
    retention_generation BIGINT NOT NULL CHECK (retention_generation > 0),
    boundary_position_kind TEXT NOT NULL
        CHECK (boundary_position_kind IN ('generation_start', 'transaction_end')),
    boundary_commit_lsn PG_LSN,
    boundary_event_ordinal BIGINT,
    boundary_effect_ordinal INTEGER,
    accepted_write_epoch BIGINT NOT NULL CHECK (accepted_write_epoch > 0),
    page_limit BIGINT NOT NULL CHECK (page_limit > 0),
    snapshot_checksum BYTEA NOT NULL CHECK (octet_length(snapshot_checksum) = 32),
    staged_row_count BIGINT NOT NULL CHECK (staged_row_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours'),
    UNIQUE (user_id, client_id, rebuild_id),
    FOREIGN KEY (user_id, client_id)
        REFERENCES sync_clients (user_id, client_id) ON DELETE RESTRICT,
    CHECK (expires_at = created_at + interval '24 hours'),
    CHECK (
        (boundary_position_kind = 'generation_start'
         AND boundary_commit_lsn IS NULL
         AND boundary_event_ordinal IS NULL
         AND boundary_effect_ordinal IS NULL)
        OR
        (boundary_position_kind = 'transaction_end'
         AND boundary_commit_lsn IS NOT NULL
         AND boundary_event_ordinal IS NULL
         AND boundary_effect_ordinal IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_sync_rebuild_sessions_expiry
    ON sync_rebuild_sessions (expires_at);

CREATE TABLE IF NOT EXISTS sync_rebuild_staged_rows (
    session_id UUID NOT NULL
        REFERENCES sync_rebuild_sessions (session_id) ON DELETE RESTRICT,
    row_ordinal BIGINT NOT NULL CHECK (row_ordinal >= 0),
    table_id TEXT NOT NULL,
    row_identity BYTEA NOT NULL CHECK (octet_length(row_identity) > 0),
    primary_key JSONB NOT NULL CHECK (jsonb_typeof(primary_key) = 'object'),
    row_data JSONB NOT NULL CHECK (jsonb_typeof(row_data) = 'object'),
    row_checksum BYTEA NOT NULL CHECK (octet_length(row_checksum) = 32),
    server_version TEXT NOT NULL CHECK (server_version <> ''),
    PRIMARY KEY (session_id, row_ordinal),
    UNIQUE (session_id, row_identity)
);

CREATE TABLE IF NOT EXISTS sync_rebuild_pages (
    session_id UUID NOT NULL
        REFERENCES sync_rebuild_sessions (session_id) ON DELETE RESTRICT,
    next_row_ordinal BIGINT NOT NULL CHECK (next_row_ordinal >= 0),
    response JSONB NOT NULL,
    PRIMARY KEY (session_id, next_row_ordinal)
);

CREATE OR REPLACE FUNCTION synchro_reject_rebuild_session_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1
           FROM sync_stream_resets reset
           WHERE reset.reset_id::text = NULLIF(current_setting('synchro.stream_reset_id', true), '')
             AND reset.lifecycle = 'baseline_staged'
       ) THEN
        RETURN OLD;
    END IF;
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1
           FROM sync_registry_membership_stages stage
           WHERE stage.registry_generation::text = NULLIF(
                     current_setting('synchro.membership_activation_generation', true), ''
                 )
             AND stage.state = 'pending'
       ) THEN
        RETURN OLD;
    END IF;
    IF TG_OP = 'DELETE'
       AND (
           OLD.expires_at <= now()
           OR EXISTS (
               SELECT 1
               FROM sync_client_retirements retirement
               WHERE retirement.user_id = OLD.user_id
                 AND retirement.client_id = OLD.client_id
           )
       ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'rebuild sessions are immutable before expiry or client retirement'
        USING ERRCODE = '55000';
END;
$$;

CREATE OR REPLACE FUNCTION synchro_reject_rebuild_child_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1
           FROM sync_stream_resets reset
           WHERE reset.reset_id::text = NULLIF(current_setting('synchro.stream_reset_id', true), '')
             AND reset.lifecycle = 'baseline_staged'
       ) THEN
        RETURN OLD;
    END IF;
    IF TG_OP = 'DELETE'
       AND EXISTS (
           SELECT 1
           FROM sync_rebuild_sessions session
           WHERE session.session_id = OLD.session_id
             AND (
                 session.expires_at <= now()
                 OR EXISTS (
                     SELECT 1
                     FROM sync_client_retirements retirement
                     WHERE retirement.user_id = session.user_id
                       AND retirement.client_id = session.client_id
                 )
             )
       ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'rebuild stages and pages are immutable before expiry or client retirement'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER synchro_rebuild_sessions_immutable
BEFORE UPDATE OR DELETE ON sync_rebuild_sessions
FOR EACH ROW EXECUTE FUNCTION synchro_reject_rebuild_session_mutation();
CREATE TRIGGER synchro_rebuild_staged_rows_immutable
BEFORE UPDATE OR DELETE ON sync_rebuild_staged_rows
FOR EACH ROW EXECUTE FUNCTION synchro_reject_rebuild_child_mutation();
CREATE TRIGGER synchro_rebuild_pages_immutable
BEFORE UPDATE OR DELETE ON sync_rebuild_pages
FOR EACH ROW EXECUTE FUNCTION synchro_reject_rebuild_child_mutation();

CREATE OR REPLACE FUNCTION synchro_primary_key_guard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, synchro
AS $$
BEGIN
    IF to_jsonb(OLD) -> TG_ARGV[0] IS DISTINCT FROM to_jsonb(NEW) -> TG_ARGV[0] THEN
        RAISE EXCEPTION 'registered primary key cannot change'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION synchro_capture_fence()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, synchro
AS $$
DECLARE
    v_fence_id UUID := gen_random_uuid();
    v_row_version UUID := gen_random_uuid();
    v_xid XID8 := pg_current_xact_id();
    v_ordinal BIGINT;
    v_registration_kind TEXT;
    v_table_id UUID;
    v_key_columns TEXT[];
    v_old_record_id TEXT;
    v_new_record_id TEXT;
    v_old_capture_key JSONB;
    v_new_capture_key JSONB;
    v_mutation_id TEXT;
    v_user_id TEXT;
    v_client_id TEXT;
    v_deleted BOOLEAN;
    v_version_rows INTEGER;
    v_message JSONB;
BEGIN
    IF TG_NARGS <> 5 THEN
        RAISE EXCEPTION 'capture fence trigger arguments are invalid'
            USING ERRCODE = '22023';
    END IF;
    v_registration_kind := TG_ARGV[1];
    IF v_registration_kind NOT IN ('synced', 'capture_dependency') THEN
        RAISE EXCEPTION 'capture fence registration kind is invalid'
            USING ERRCODE = '22023';
    END IF;
    SELECT array_agg(key_column ORDER BY ordinal)
      INTO v_key_columns
      FROM jsonb_array_elements_text(TG_ARGV[3]::jsonb)
           WITH ORDINALITY AS key_columns(key_column, ordinal);
    IF v_key_columns IS NULL
       OR cardinality(v_key_columns) <> 1
       OR v_key_columns[1] = '' THEN
        RAISE EXCEPTION 'capture fence key metadata is invalid'
            USING ERRCODE = '22023';
    END IF;
    IF v_registration_kind = 'synced' THEN
        IF TG_ARGV[2] = '' THEN
            RAISE EXCEPTION 'synced capture fence table identity is missing'
                USING ERRCODE = '22023';
        END IF;
        v_table_id := TG_ARGV[2]::uuid;
    ELSIF TG_ARGV[2] <> '' THEN
        RAISE EXCEPTION 'capture dependency fence must not include table identity'
            USING ERRCODE = '22023';
    END IF;
    PERFORM pg_advisory_xact_lock_shared(1936876389::bigint);
    PERFORM pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended('synchro:relation:' || TG_ARGV[0], 0)
    );
    IF TG_OP <> 'INSERT' THEN
        SELECT jsonb_object_agg(key_column, row_data -> key_column)
          INTO v_old_capture_key
          FROM unnest(v_key_columns) AS key_columns(key_column)
          CROSS JOIN LATERAL (SELECT to_jsonb(OLD) AS row_data) AS old_row;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        SELECT jsonb_object_agg(key_column, row_data -> key_column)
          INTO v_new_capture_key
          FROM unnest(v_key_columns) AS key_columns(key_column)
          CROSS JOIN LATERAL (SELECT to_jsonb(NEW) AS row_data) AS new_row;
    END IF;
    IF v_registration_kind = 'synced' THEN
        IF v_old_capture_key IS NOT NULL THEN
            v_old_record_id := v_old_capture_key ->> v_key_columns[1];
        END IF;
        IF v_new_capture_key IS NOT NULL THEN
            v_new_record_id := v_new_capture_key ->> v_key_columns[1];
        END IF;
        v_old_capture_key := NULL;
        v_new_capture_key := NULL;
    END IF;

    v_ordinal := COALESCE(
        NULLIF(current_setting('synchro.dml_ordinal', true), '')::bigint,
        0
    ) + 1;
    PERFORM set_config('synchro.dml_ordinal', v_ordinal::text, true);

    IF v_registration_kind = 'synced' THEN
        v_mutation_id := NULLIF(current_setting('synchro.mutation_id', true), '');
        v_user_id := NULLIF(current_setting('synchro.user_id', true), '');
        v_client_id := NULLIF(current_setting('synchro.client_id', true), '');
    END IF;
    v_deleted := TG_OP = 'DELETE';
    IF v_registration_kind = 'synced' AND TG_OP <> 'DELETE' AND TG_ARGV[4] <> '' THEN
        v_deleted := COALESCE(
            (to_jsonb(NEW) -> TG_ARGV[4]) <> 'null'::jsonb,
            false
        );
    END IF;

    INSERT INTO sync_write_fences (
        fence_id,
        transaction_xid,
        dml_ordinal,
        relation_id,
        registration_kind,
        table_id,
        physical_schema,
        physical_relation,
        physical_relation_oid,
        operation,
        old_record_id,
        new_record_id,
        old_capture_key,
        new_capture_key,
        row_version,
        mutation_id,
        user_id,
        client_id
    ) VALUES (
        v_fence_id,
        v_xid,
        v_ordinal,
        TG_ARGV[0]::uuid,
        v_registration_kind,
        v_table_id,
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        TG_RELID,
        lower(TG_OP),
        v_old_record_id,
        v_new_record_id,
        v_old_capture_key,
        v_new_capture_key,
        v_row_version,
        v_mutation_id,
        v_user_id,
        v_client_id
    );

    IF v_registration_kind = 'synced' THEN
        INSERT INTO sync_row_versions (
            relation_id,
            record_id,
            row_version,
            fence_id,
            reset_id,
            deleted,
            updated_at
        ) VALUES (
            TG_ARGV[0]::uuid,
            COALESCE(v_new_record_id, v_old_record_id),
            v_row_version,
            v_fence_id,
            NULL,
            v_deleted,
            now()
        )
        ON CONFLICT (relation_id, record_id) DO UPDATE SET
            row_version = EXCLUDED.row_version,
            fence_id = EXCLUDED.fence_id,
            reset_id = NULL,
            deleted = EXCLUDED.deleted,
            updated_at = now()
        WHERE NOT (sync_row_versions.deleted AND NOT EXCLUDED.deleted);

        GET DIAGNOSTICS v_version_rows = ROW_COUNT;
        IF v_version_rows <> 1 THEN
            RAISE EXCEPTION 'registered row identity is deleted'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    v_message := jsonb_build_object(
        'fence_id', v_fence_id,
        'dml_ordinal', v_ordinal,
        'registration_kind', v_registration_kind,
        'relation_id', TG_ARGV[0]::uuid,
        'physical_schema', TG_TABLE_SCHEMA,
        'physical_relation', TG_TABLE_NAME,
        'physical_relation_oid', TG_RELID::bigint,
        'operation', lower(TG_OP),
        'row_version', v_row_version
    );
    IF v_registration_kind = 'synced' THEN
        v_message := v_message || jsonb_build_object(
            'table_id', v_table_id,
            'old_record_id', v_old_record_id,
            'new_record_id', v_new_record_id
        );
    ELSE
        v_message := v_message || jsonb_build_object(
            'old_capture_key', v_old_capture_key,
            'new_capture_key', v_new_capture_key
        );
    END IF;
    PERFORM pg_logical_emit_message(
        true,
        'synchro_fence',
        convert_to(v_message::text, 'UTF8')
    );

    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE OR REPLACE FUNCTION synchro_capture_truncate_guard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, synchro
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(1936876389::bigint);
    PERFORM pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended('synchro:relation:' || TG_ARGV[0], 0)
    );
    RETURN NULL;
END;
$$;
"#,
    name = "create_infrastructure_tables",
    bootstrap
);

pgrx::extension_sql!(
    r#"
DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'synchro_owner') THEN
        CREATE ROLE synchro_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'synchro_adapter') THEN
        CREATE ROLE synchro_adapter NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'synchro_seed') THEN
        CREATE ROLE synchro_seed NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'synchro_monitor') THEN
        CREATE ROLE synchro_monitor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'synchro_operator') THEN
        CREATE ROLE synchro_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'synchro_worker') THEN
        CREATE ROLE synchro_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    END IF;

    ALTER ROLE synchro_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    ALTER ROLE synchro_adapter NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    ALTER ROLE synchro_seed NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    ALTER ROLE synchro_monitor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    ALTER ROLE synchro_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    ALTER ROLE synchro_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
END
$roles$;

CREATE SCHEMA IF NOT EXISTS synchro_projection;
ALTER SCHEMA synchro_projection OWNER TO synchro_owner;
REVOKE ALL ON SCHEMA synchro_projection FROM PUBLIC;

DO $ownership$
DECLARE
    object_record record;
    object_kind text;
    object_identity text;
BEGIN
    FOR object_record IN
        SELECT class.relkind, namespace.nspname, class.relname
        FROM pg_catalog.pg_class class
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = 'synchro'
          AND class.relkind IN ('r', 'p', 'v', 'm', 'f')
    LOOP
        object_kind := CASE object_record.relkind
            WHEN 'v' THEN 'VIEW'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'f' THEN 'FOREIGN TABLE'
            ELSE 'TABLE'
        END;
        object_identity := pg_catalog.format('%I.%I', object_record.nspname, object_record.relname);
        EXECUTE pg_catalog.format('ALTER %s %s OWNER TO synchro_owner', object_kind, object_identity);
    END LOOP;

    ALTER TYPE synchro.synchro_row_ref OWNER TO synchro_owner;

    FOR object_record IN
        SELECT namespace.nspname, procedure.proname,
               pg_catalog.pg_get_function_identity_arguments(procedure.oid) AS arguments
        FROM pg_catalog.pg_proc procedure
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = 'synchro'
          AND procedure.prokind = 'f'
    LOOP
        object_identity := pg_catalog.format(
            '%I.%I(%s)', object_record.nspname, object_record.proname, object_record.arguments
        );
        EXECUTE pg_catalog.format('ALTER FUNCTION %s OWNER TO synchro_owner', object_identity);
        EXECUTE pg_catalog.format('ALTER FUNCTION %s SECURITY DEFINER', object_identity);
        EXECUTE pg_catalog.format(
            'ALTER FUNCTION %s SET search_path = pg_catalog, synchro', object_identity
        );
    END LOOP;
END
$ownership$;

ALTER SCHEMA synchro OWNER TO synchro_owner;

REVOKE ALL ON SCHEMA synchro FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA synchro FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA synchro FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA synchro FROM PUBLIC;
REVOKE USAGE ON TYPE synchro.synchro_row_ref FROM PUBLIC;

GRANT USAGE ON SCHEMA synchro
    TO synchro_adapter, synchro_seed, synchro_monitor, synchro_operator, synchro_worker;
GRANT USAGE ON SCHEMA synchro_projection TO synchro_owner, synchro_operator, synchro_worker;
GRANT USAGE ON TYPE synchro.synchro_row_ref TO synchro_operator;

DO $function_grants$
DECLARE
    function_record record;
    object_identity text;
    grantee text;
BEGIN
    FOR function_record IN
        SELECT namespace.nspname, procedure.proname,
               pg_catalog.pg_get_function_identity_arguments(procedure.oid) AS arguments
        FROM pg_catalog.pg_proc procedure
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = 'synchro'
          AND procedure.prokind = 'f'
    LOOP
        grantee := CASE
            WHEN function_record.proname IN (
                'synchro_contract_info', 'synchro_connect', 'synchro_pull', 'synchro_push',
                'synchro_rebuild', 'synchro_schema_manifest', 'synchro_tables', 'synchro_readiness'
            ) THEN 'synchro_adapter'
            WHEN function_record.proname IN (
                'synchro_portable_seed_manifest', 'synchro_portable_seed_scope'
            ) THEN 'synchro_seed'
            WHEN function_record.proname IN ('synchro_readiness', 'synchro_health_detail')
            THEN 'synchro_monitor'
            WHEN function_record.proname IN (
                'synchro_register_table', 'synchro_register_capture_dependency',
                'synchro_prepare_projection_view',
                'synchro_register_membership_dependency',
                'synchro_unregister_table', 'synchro_register_shared_scope',
                'synchro_unregister_shared_scope', 'synchro_backfill_bucket_edges',
                 'synchro_compact', 'synchro_retry_wal_poison', 'synchro_health_detail',
                 'synchro_debug', 'synchro_primary_key_guard', 'synchro_capture_fence',
                 'synchro_prepare_stream_reset', 'synchro_lock_stream_reset_sources',
                 'synchro_mark_stream_reset_snapshot',
                 'synchro_stage_stream_reset', 'synchro_activate_stream_reset',
                 'synchro_abort_stream_reset', 'synchro_complete_stream_reset_cleanup',
                 'synchro_prepare_projection_bootstrap',
                 'synchro_stage_projection_bootstrap',
                 'synchro_emit_projection_bootstrap_barrier',
                 'synchro_request_projection_bootstrap_barrier',
                 'synchro_activate_projection_bootstrap',
                 'synchro_projection_bootstrap_status',
                 'synchro_abort_projection_bootstrap',
                 'synchro_complete_projection_bootstrap_cleanup'
             ) THEN 'synchro_operator'
            ELSE NULL
        END;
        IF grantee IS NOT NULL THEN
            object_identity := pg_catalog.format(
                '%I.%I(%s)', function_record.nspname, function_record.proname,
                function_record.arguments
            );
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %I', object_identity, grantee);
        END IF;
        IF function_record.proname = 'synchro_schema_manifest' THEN
            object_identity := pg_catalog.format(
                '%I.%I(%s)', function_record.nspname, function_record.proname,
                function_record.arguments
            );
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO synchro_seed', object_identity);
        END IF;
        IF function_record.proname = 'synchro_readiness' THEN
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO synchro_monitor', object_identity);
        END IF;
        IF function_record.proname = 'synchro_health_detail' THEN
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO synchro_operator', object_identity);
        END IF;
    END LOOP;
END
$function_grants$;

GRANT SELECT, UPDATE ON synchro.sync_runtime_state TO synchro_worker;
GRANT SELECT, UPDATE ON synchro.sync_registry_generations TO synchro_worker;
GRANT SELECT ON synchro.sync_logical_ids, synchro.sync_registry,
    synchro.sync_registry_fields, synchro.sync_capture_dependency_fields,
    synchro.sync_projection_views,
    synchro.sync_membership_dependencies,
    synchro.sync_membership_limits, synchro.sync_row_versions TO synchro_worker;
GRANT SELECT, UPDATE ON synchro.sync_write_fences TO synchro_worker;
GRANT SELECT, INSERT, UPDATE ON synchro.sync_wal_transactions,
    synchro.sync_wal_events, synchro.sync_wal_progress,
    synchro.sync_wal_poison, synchro.sync_wal_worker_state TO synchro_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON synchro.sync_captured_rows,
    synchro.sync_capture_dependency_rows, synchro.sync_bucket_edges TO synchro_worker;
GRANT SELECT, INSERT, UPDATE ON synchro.sync_captured_projections TO synchro_worker;
GRANT SELECT, INSERT ON synchro.sync_capture_dependency_projections TO synchro_worker;
GRANT SELECT ON synchro.sync_current_projections TO synchro_worker;
GRANT SELECT ON synchro.sync_clients, synchro.sync_client_checkpoints,
    synchro.sync_shared_scopes TO synchro_worker;
GRANT DELETE ON synchro.sync_client_checkpoints TO synchro_worker;
GRANT SELECT, INSERT, UPDATE ON synchro.sync_scope_state,
    synchro.sync_schema_manifest TO synchro_worker;
GRANT SELECT, INSERT ON synchro.sync_changelog TO synchro_worker;
GRANT SELECT, UPDATE ON synchro.sync_stream_resets TO synchro_worker;
GRANT SELECT, UPDATE ON synchro.sync_registry_membership_stages TO synchro_worker;
GRANT SELECT, INSERT, UPDATE ON synchro.sync_projection_bootstrap_transactions TO synchro_worker;
GRANT SELECT, INSERT ON synchro.sync_projection_bootstrap_events TO synchro_worker;
GRANT SELECT, INSERT, UPDATE ON synchro.sync_stream_reset_row_versions TO synchro_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON synchro.sync_stream_reset_captured_rows,
    synchro.sync_stream_reset_capture_dependency_rows TO synchro_worker;
GRANT SELECT, INSERT, DELETE ON synchro.sync_stream_reset_membership_edges,
    synchro.sync_stream_reset_scope_digests TO synchro_worker;
GRANT SELECT ON synchro.sync_stream_reset_fence_coverage TO synchro_worker;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA synchro TO synchro_worker;

ALTER DEFAULT PRIVILEGES FOR ROLE synchro_owner IN SCHEMA synchro REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE synchro_owner IN SCHEMA synchro REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE synchro_owner IN SCHEMA synchro REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE synchro_owner IN SCHEMA synchro REVOKE USAGE ON TYPES FROM PUBLIC;
"#,
    name = "apply_security_policy",
    finalize
);

// ---------------------------------------------------------------------------
// GUC settings (readable from all modules via crate::*)
// ---------------------------------------------------------------------------

/// Name of the logical replication slot. Defaults to "synchro_slot" when NULL.
pub(crate) static REPLICATION_SLOT_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Name of the WAL publication. Defaults to "synchro_pub" when NULL.
pub(crate) static PUBLICATION_NAME_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Database the WAL background worker should connect to.
pub(crate) static DATABASE_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Dedicated deployment login used by the WAL background worker.
pub(crate) static WORKER_LOGIN_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Whether to auto-start the WAL background worker on server boot.
pub(crate) static AUTO_START_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Maximum accepted age of the WAL worker heartbeat, in seconds.
pub(crate) static MAX_WORKER_HEARTBEAT_AGE_SECONDS_GUC: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Maximum accepted difference between current WAL and acknowledged WAL, in bytes.
pub(crate) static MAX_WAL_LAG_BYTES_GUC: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Maximum accepted age of the oldest unmaterialized registered write, in seconds.
pub(crate) static MAX_WAL_LAG_SECONDS_GUC: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) fn configured_worker_login() -> Option<String> {
    WORKER_LOGIN_GUC
        .get()
        .and_then(|value| value.to_str().ok().map(String::from))
        .filter(|value| !value.is_empty())
}

/// Extension initialization. Called when the shared library is loaded.
///
/// Registers all GUCs and conditionally starts the WAL background worker.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"synchro.replication_slot",
        c"Name of the logical replication slot used by synchro.",
        c"Name of the logical replication slot used by the synchro WAL consumer. Defaults to synchro_slot.",
        &REPLICATION_SLOT_GUC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"synchro.publication_name",
        c"Name of the WAL publication used by synchro.",
        c"Name of the PostgreSQL publication used by the synchro WAL consumer. Defaults to synchro_pub.",
        &PUBLICATION_NAME_GUC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"synchro.database",
        c"Database name for the WAL background worker.",
        c"Database the WAL consumer connects to. Defaults to postgres.",
        &DATABASE_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"synchro.worker_login",
        c"Dedicated login for the WAL background worker.",
        c"Deployment-provisioned LOGIN REPLICATION role used by the WAL background worker.",
        &WORKER_LOGIN_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"synchro.auto_start",
        c"Whether to auto-start the synchro WAL background worker.",
        c"When true, the WAL consumer background worker starts on server boot.",
        &AUTO_START_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"synchro.max_worker_heartbeat_age_seconds",
        c"Maximum accepted WAL worker heartbeat age.",
        c"Readiness fails when the worker heartbeat age exceeds this positive seconds limit.",
        &MAX_WORKER_HEARTBEAT_AGE_SECONDS_GUC,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"synchro.max_wal_lag_bytes",
        c"Maximum accepted WAL byte lag.",
        c"Readiness fails when current WAL exceeds acknowledged WAL by more than this positive byte limit.",
        &MAX_WAL_LAG_BYTES_GUC,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"synchro.max_wal_lag_seconds",
        c"Maximum accepted WAL time lag.",
        c"Readiness fails when the oldest unmaterialized registered write exceeds this positive seconds limit.",
        &MAX_WAL_LAG_SECONDS_GUC,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );

    let in_shared_preload = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    if AUTO_START_GUC.get() && in_shared_preload {
        bgworker::register_bgworker();
    }
}

// ---------------------------------------------------------------------------
// Test helpers and integration tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use serde_json::json;
    use serde_json::Value;
    use sha2::{Digest, Sha256};

    include!("pg_tests/order_cursor.rs");
    include!("pg_tests/integrity.rs");
    include!("pg_tests/wal_pipeline.rs");
    include!("pg_tests/pull.rs");
    include!("pg_tests/rebuild.rs");
    include!("pg_tests/push_idempotency.rs");
    include!("pg_tests/conflicts.rs");
    include!("pg_tests/membership.rs");
    include!("pg_tests/schema.rs");
    include!("pg_tests/retention.rs");
    include!("pg_tests/authorization.rs");
    include!("pg_tests/portable_seed.rs");
    include!("pg_tests/health.rs");
    include!("pg_tests/stream_reset.rs");

    // -----------------------------------------------------------------------
    // Shared test setup
    // -----------------------------------------------------------------------

    #[pg_extern]
    #[allow(clippy::too_many_arguments)]
    fn register_legacy_test_table(
        p_table_name: &str,
        p_bucket_sql: &str,
        p_composition: &str,
        p_pk_column: default!(&str, "'id'"),
        p_updated_at_col: default!(&str, "'updated_at'"),
        p_deleted_at_col: default!(&str, "'deleted_at'"),
        p_push_policy: default!(&str, "'enabled'"),
        p_exclude_columns: default!(Vec<String>, "'{}'"),
        p_sync_columns: default!(Vec<String>, "'{}'"),
    ) {
        let function_suffix = format!("{:x}", Sha256::digest(p_bucket_sql.as_bytes()));
        let function_name = format!("legacy_membership_{}", &function_suffix[..16]);
        let physical_table_name = format!("public.{p_table_name}");
        Spi::connect_mut(|client| {
            let primary_key_type = client
                .select(
                    "SELECT pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS sql_type
                     FROM pg_catalog.pg_attribute attribute
                     WHERE attribute.attrelid = pg_catalog.to_regclass($1)
                       AND attribute.attname = $2
                       AND attribute.attnum > 0
                       AND NOT attribute.attisdropped",
                    None,
                    &[p_table_name.into(), p_pk_column.into()],
                )?
                .first()
                .get_by_name::<String, &str>("sql_type")?
                .expect("legacy test primary-key type");
            let has_deleted_at = client
                .select(
                    "SELECT EXISTS (
                         SELECT 1
                         FROM pg_catalog.pg_attribute attribute
                         WHERE attribute.attrelid = pg_catalog.to_regclass($1)
                           AND attribute.attname = $2
                           AND attribute.attnum > 0
                           AND NOT attribute.attisdropped
                     ) AS has_deleted_at",
                    None,
                    &[p_table_name.into(), p_deleted_at_col.into()],
                )?
                .first()
                .get_by_name::<bool, &str>("has_deleted_at")?
                .expect("legacy test deleted_at state");
            let relation_privileges = match (p_push_policy, has_deleted_at) {
                ("read_only", _) => "SELECT",
                ("enabled", true) => "SELECT, INSERT, UPDATE",
                ("enabled", false) => "SELECT, INSERT, UPDATE, DELETE",
                _ => pgrx::error!("invalid legacy test push policy"),
            };
            let body = format!(
                "SELECT unnest(COALESCE(scope_ids, ARRAY[]::text[])) FROM ({p_bucket_sql}) AS membership(scope_ids)"
            );
            let ddl = client
                .select(
                    "SELECT pg_catalog.format(
                         'CREATE OR REPLACE FUNCTION tests.%I(p_key %s) RETURNS SETOF text LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, public AS %L',
                         $1, $2, $3
                     ) AS ddl",
                    None,
                    &[
                        function_name.as_str().into(),
                        primary_key_type.as_str().into(),
                        body.as_str().into(),
                    ],
            )?
            .first()
            .get_by_name::<String, &str>("ddl")?
            .expect("legacy test membership DDL");
            client.update(&ddl, None, &[])?;
            let revoke_function_public = client
                .select(
                    "SELECT pg_catalog.format(
                         'REVOKE EXECUTE ON FUNCTION tests.%I(%s) FROM PUBLIC',
                         $1, $2
                     ) AS ddl",
                    None,
                    &[
                        function_name.as_str().into(),
                        primary_key_type.as_str().into(),
                    ],
                )?
                .first()
                .get_by_name::<String, &str>("ddl")?
                .expect("legacy test membership public revoke DDL");
            client.update(&revoke_function_public, None, &[])?;
            let grant_function = client
                .select(
                    "SELECT pg_catalog.format(
                         'GRANT EXECUTE ON FUNCTION tests.%I(%s) TO synchro_owner, synchro_worker',
                         $1, $2
                     ) AS ddl",
                    None,
                    &[
                        function_name.as_str().into(),
                        primary_key_type.as_str().into(),
                    ],
                )?
                .first()
                .get_by_name::<String, &str>("ddl")?
                .expect("legacy test membership grant DDL");
            client.update(&grant_function, None, &[])?;
            client.update(
                "GRANT USAGE ON SCHEMA tests TO synchro_owner, synchro_worker",
                None,
                &[],
            )?;
            let revoke = client
                .select(
                    "SELECT pg_catalog.format(
                         'REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE %s FROM synchro_owner',
                         pg_catalog.to_regclass($1)
                      ) AS ddl",
                    None,
                    &[p_table_name.into()],
                )?
                .first()
                .get_by_name::<String, &str>("ddl")?
                .expect("legacy test relation revoke DDL");
            client.update(&revoke, None, &[])?;
            let grant = client
                .select(
                    "SELECT pg_catalog.format(
                         'GRANT %s ON TABLE %s TO synchro_owner',
                         $2, pg_catalog.to_regclass($1)
                      ) AS ddl",
                    None,
                    &[p_table_name.into(), relation_privileges.into()],
                )?
                .first()
                .get_by_name::<String, &str>("ddl")?
                .expect("legacy test relation grant DDL");
            client.update(&grant, None, &[])?;
            let rls = client
                .select(
                    "SELECT pg_catalog.format(
                         'ALTER TABLE %s ENABLE ROW LEVEL SECURITY; \
                          DROP POLICY IF EXISTS synchro_test_owner_all ON %s; \
                          CREATE POLICY synchro_test_owner_all ON %s \
                          AS PERMISSIVE FOR ALL TO synchro_owner \
                          USING (true) WITH CHECK (true)',
                         pg_catalog.to_regclass($1),
                         pg_catalog.to_regclass($1),
                         pg_catalog.to_regclass($1)
                     ) AS ddl",
                    None,
                    &[p_table_name.into()],
                )?
                .first()
                .get_by_name::<String, &str>("ddl")?
                .expect("legacy test owner RLS policy DDL");
            client.update(&rls, None, &[])?;
            let function_identity = format!("tests.{function_name}");
            client.update(
                "SELECT synchro.synchro_register_table(
                     $1, $2, $3, $4, $5, $6, $7, $8, $9
                 )",
                None,
                &[
                    physical_table_name.as_str().into(),
                    function_identity.as_str().into(),
                    p_composition.into(),
                    p_pk_column.into(),
                    p_updated_at_col.into(),
                    p_deleted_at_col.into(),
                    p_push_policy.into(),
                    p_exclude_columns.clone().into(),
                    p_sync_columns.clone().into(),
                ],
            )?;
            Ok::<_, pgrx::spi::Error>(())
        })
        .expect("register legacy test table");
    }

    fn activate_pending_registry_for_test() {
        Spi::connect_mut(|client| {
            let active_generation = client
                .select(
                    "SELECT generation FROM sync_registry_generations WHERE state = 'active'",
                    None,
                    &[],
                )?
                .first()
                .get_by_name::<i64, &str>("generation")?
                .expect("active test registry generation");
            let generation_rows = client.select(
                "SELECT generation
                 FROM sync_registry_generations
                 WHERE state = 'pending' AND validated
                 ORDER BY generation",
                None,
                &[],
            )?;
            let mut generations = Vec::with_capacity(generation_rows.len());
            for row in generation_rows {
                generations.push(
                    row.get_by_name::<i64, &str>("generation")?
                        .expect("pending test registry generation"),
                );
            }
            let Some(generation) = generations.last().copied() else {
                return Ok::<_, spi::Error>(());
            };
            let stream_generation = client
                .select(
                    "SELECT stream_generation FROM sync_runtime_state WHERE singleton",
                    None,
                    &[],
                )?
                .first()
                .get_by_name::<String, &str>("stream_generation")?
                .expect("test stream generation");
            crate::registry::remove_retired_capture_configuration(
                client,
                active_generation,
                generation,
            )?;
            let mut source_generation = active_generation;
            for target_generation in generations {
                crate::materialize::activate_staged_membership_generation(
                    client,
                    source_generation,
                    target_generation,
                    &stream_generation,
                    "0/1",
                    "0/2",
                )
                .expect("activate staged test membership generation");
                source_generation = target_generation;
            }
            crate::registry::load_registry_generation_from_client(client, generation)?;
            client.update(
                "UPDATE sync_registry_generations
                 SET state = 'superseded', activated_at = COALESCE(activated_at, now())
                 WHERE state IN ('active', 'pending') AND generation <> $1",
                None,
                &[generation.into()],
            )?;
            client.update(
                "UPDATE sync_registry_generations
                 SET state = 'active', activated_at = now()
                 WHERE generation = $1 AND state = 'pending' AND validated",
                None,
                &[generation.into()],
            )?;
            client.update(
                "UPDATE sync_wal_progress
                 SET registry_generation = $1, updated_at = now()
                 WHERE singleton",
                None,
                &[generation.into()],
            )?;

            crate::schema::publish_schema_manifest(client)?;
            Ok::<_, spi::Error>(())
        })
        .unwrap();
    }

    /// Create test tables and register them for sync.
    fn setup_test_tables() {
        // orders: standard table with timestamps, user_id for bucketing
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                amount NUMERIC(15,2) DEFAULT 0,
                internal_notes TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        // products: read-only reference data
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_products (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL DEFAULT '',
                price NUMERIC(15,2) DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        // bare_items: no timestamps (no updated_at, no deleted_at)
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_bare_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL DEFAULT ''
            )",
        )
        .unwrap();

        // Register tables for sync. Bucket SQL must cast $1 to the PK type
        // explicitly because SPI prepared statements pass text parameters and
        // PG does not implicit-cast text to uuid.
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                'test_orders',
                $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                'single_scope',
                'id', 'updated_at', 'deleted_at', 'enabled',
                ARRAY['internal_notes']
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                'test_products',
                $$SELECT ARRAY['global'] FROM test_products WHERE id = $1::uuid$$,
                'single_scope',
                'id', 'updated_at', 'deleted_at', 'read_only'
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                'test_bare_items',
                $$SELECT ARRAY['global'] FROM test_bare_items WHERE id = $1::uuid$$,
                'single_scope',
                'id', 'updated_at', 'deleted_at', 'enabled'
            )",
        )
        .unwrap();
        activate_pending_registry_for_test();
    }

    fn setup_sync_columns_table() {
        setup_test_tables();

        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_sync_columns_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                search_vector TEXT DEFAULT '',
                internal_notes TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE INDEX IF NOT EXISTS idx_test_sync_columns_items_title
             ON test_sync_columns_items (title)",
        )
        .unwrap();
        Spi::run(
            "CREATE INDEX IF NOT EXISTS idx_test_sync_columns_items_search_vector
             ON test_sync_columns_items (search_vector)",
        )
        .unwrap();
        Spi::run(
            "SELECT tests.register_legacy_test_table(
                'test_sync_columns_items',
                $$SELECT ARRAY['user:' || user_id] FROM test_sync_columns_items WHERE id = $1::uuid$$,
                'single_scope',
                'id',
                'updated_at',
                'deleted_at',
                'enabled',
                ARRAY[]::text[],
                ARRAY['id', 'user_id', 'title', 'updated_at', 'deleted_at']
            )",
        )
        .unwrap();
        activate_pending_registry_for_test();
    }

    fn setup_portable_type_contract_table() {
        setup_test_tables();

        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_portable_type_contract (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                col_smallint SMALLINT,
                col_integer INTEGER,
                col_bigint BIGINT,
                col_numeric NUMERIC(5,1),
                col_real REAL,
                col_double DOUBLE PRECISION,
                col_timestamp TIMESTAMPTZ,
                col_interval INTERVAL,
                col_json JSONB,
                col_blob BYTEA,
                col_text_array TEXT[],
                col_int_array INTEGER[],
                col_inet INET,
                col_point POINT,
                col_int4range INT4RANGE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT tests.register_legacy_test_table(
                p_table_name := 'test_portable_type_contract',
                p_bucket_sql := $$SELECT ARRAY['user:' || user_id] FROM test_portable_type_contract WHERE id = $1::uuid$$,
                p_composition := 'single_scope',
                p_pk_column := 'id',
                p_updated_at_col := 'updated_at',
                p_deleted_at_col := 'deleted_at',
                p_push_policy := 'enabled',
                p_sync_columns := ARRAY[
                    'id',
                    'user_id',
                    'label',
                    'col_smallint',
                    'col_integer',
                    'col_bigint',
                    'col_numeric',
                    'col_real',
                    'col_double',
                    'col_timestamp',
                    'col_interval',
                    'col_json',
                    'col_blob',
                    'col_text_array',
                    'col_int_array',
                    'col_inet',
                    'col_point',
                    'col_int4range',
                    'updated_at',
                    'deleted_at'
                ]
            )",
        )
        .unwrap();
        activate_pending_registry_for_test();
    }

    /// Register a test client and return the raw JSONB response.
    fn register_client(user_id: &str, client_id: &str) -> Value {
        connect_client(
            user_id,
            json!({
                "client_id": client_id,
                "platform": "test",
                "app_version": "1.0.0",
                "protocol_version": 3,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        )
    }

    /// Execute a canonical connect request and return the raw JSONB response.
    fn connect_client(user_id: &str, request: Value) -> Value {
        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_connect($1, $2::jsonb)",
            &[user_id.into(), request.to_string().into()],
        )
        .unwrap();
        row.unwrap().0
    }

    fn register_shared_scope(scope_id: &str, portable: bool) {
        Spi::run_with_args(
            "SELECT synchro_register_shared_scope($1, $2)",
            &[scope_id.into(), portable.into()],
        )
        .unwrap();
    }

    fn test_uuid(seed: &str) -> String {
        let digest = Sha256::digest(seed.as_bytes());
        let hex = format!("{digest:x}");
        format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    }

    const TEST_CLIENT_VERSION: &str = "2026-07-18T13:59:01.000000Z";

    fn table_id(table_name: &str) -> String {
        Spi::get_one_with_args(
            "SELECT r.table_id::text
             FROM sync_registry r
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'active' AND r.table_name = $1",
            &[table_name.into()],
        )
        .unwrap()
        .expect("active logical table identity")
    }

    fn primary_key_field_id(table_name: &str) -> String {
        Spi::get_one_with_args(
            "SELECT r.primary_key_field_id::text
             FROM sync_registry r
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'active' AND r.table_name = $1",
            &[table_name.into()],
        )
        .unwrap()
        .expect("active logical primary-key identity")
    }

    fn schema_ref_value() -> Value {
        let (schema_version, schema_hash) = latest_schema_ref();
        json!({ "version": schema_version, "hash": schema_hash })
    }

    fn client_generation(user_id: &str, client_id: &str) -> i64 {
        Spi::get_one_with_args(
            "SELECT client_generation
             FROM sync_clients
             WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap()
        .expect("registered client generation")
    }

    fn batch_id(user_id: &str, client_id: &str, label: &str) -> String {
        test_uuid(&format!("batch:{user_id}:{client_id}:{label}"))
    }

    fn mutation_id(user_id: &str, client_id: &str, label: &str) -> String {
        test_uuid(&format!("mutation:{user_id}:{client_id}:{label}"))
    }

    fn logical_columns(table_name: &str, columns: &[(&str, Value)]) -> Value {
        let mut fields = serde_json::Map::new();
        for (physical_column, value) in columns {
            fields.insert(field_id(table_name, physical_column), value.clone());
        }
        Value::Object(fields)
    }

    fn push_mutation(
        user_id: &str,
        client_id: &str,
        mutation_label: &str,
        table_name: &str,
        operation: &str,
        record_id: &str,
        base_version: Option<&str>,
        columns: Option<&[(&str, Value)]>,
    ) -> Value {
        let mut pk = serde_json::Map::new();
        pk.insert(
            primary_key_field_id(table_name),
            Value::String(record_id.to_string()),
        );
        let mut mutation = json!({
            "mutation_id": mutation_id(user_id, client_id, mutation_label),
            "table": table_id(table_name),
            "pk": pk,
            "authored_schema": schema_ref_value(),
            "op": operation,
            "client_version": TEST_CLIENT_VERSION,
        });
        if let Some(base_version) = base_version {
            mutation["base_version"] = Value::String(base_version.to_string());
        }
        if let Some(columns) = columns {
            mutation["columns"] = logical_columns(table_name, columns);
        }
        mutation
    }

    fn push_request_with_generation(
        user_id: &str,
        client_id: &str,
        batch_label: &str,
        generation: i64,
        mutations: Vec<Value>,
    ) -> Value {
        json!({
            "client_id": client_id,
            "client_generation": generation,
            "batch_id": batch_id(user_id, client_id, batch_label),
            "schema": schema_ref_value(),
            "mutations": mutations,
        })
    }

    fn push_request(
        user_id: &str,
        client_id: &str,
        batch_label: &str,
        mutations: Vec<Value>,
    ) -> Value {
        push_request_with_generation(
            user_id,
            client_id,
            batch_label,
            client_generation(user_id, client_id),
            mutations,
        )
    }

    struct PushResult {
        raw: String,
        json: Value,
    }

    fn execute_push(user_id: &str, request: &Value) -> PushResult {
        let response: Option<String> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[user_id.into(), request.to_string().into()],
        )
        .unwrap();
        let raw = response.expect("push must return canonical text");
        let json = serde_json::from_str(&raw).expect("push text must contain JSON");
        PushResult { raw, json }
    }

    fn push_client(
        user_id: &str,
        client_id: &str,
        batch_label: &str,
        mutations: Vec<Value>,
    ) -> PushResult {
        let request = push_request(user_id, client_id, batch_label, mutations);
        execute_push(user_id, &request)
    }

    fn pull_client(
        user_id: &str,
        client_id: &str,
        scope_set_version: i64,
        scopes: Value,
        limit: i32,
    ) -> Value {
        let (schema_version, schema_hash) = latest_schema_ref();
        let request = json!({
            "client_id": client_id,
            "client_generation": 1,
            "schema": { "version": schema_version, "hash": schema_hash },
            "scope_set_version": scope_set_version,
            "scopes": scopes,
            "limit": limit,
        });

        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[user_id.into(), request.to_string().into()],
        )
        .unwrap();
        row.unwrap().0
    }

    fn rebuild_client(
        user_id: &str,
        client_id: &str,
        scope: &str,
        cursor: Option<&str>,
        limit: i32,
    ) -> Value {
        let (schema_version, schema_hash) = latest_schema_ref();
        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2::jsonb)",
            &[
                user_id.into(),
                json!({
                    "client_id": client_id,
                    "client_generation": 1,
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope": scope,
                    "rebuild_id": test_uuid(&format!("rebuild:{user_id}:{client_id}:{scope}")),
                    "cursor": cursor,
                    "limit": limit
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        row.unwrap().0
    }

    fn client_scope_ids(user_id: &str, client_id: &str) -> Vec<String> {
        Spi::get_one_with_args(
            "SELECT bucket_subs FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap()
        .unwrap_or_default()
    }

    /// Return the latest schema version and hash persisted by the extension.
    fn latest_schema_ref() -> (i64, String) {
        let row: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT jsonb_build_object(
                'version', schema_version,
                'hash', schema_hash
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap();
        let row = row.expect("schema manifest row");
        let version = row.0["version"].as_i64().unwrap_or(0);
        let hash = row.0["hash"].as_str().unwrap_or_default().to_string();
        (version, hash)
    }

    fn field_id(table_name: &str, physical_column: &str) -> String {
        Spi::get_one_with_args(
            "SELECT f.field_id::text
             FROM sync_registry_fields f
             JOIN sync_registry r
               ON r.registry_generation = f.registry_generation
              AND r.relation_id = f.relation_id
             JOIN sync_registry_generations g
               ON g.generation = r.registry_generation
             WHERE g.state = 'active'
               AND r.table_name = $1
               AND f.physical_column = $2",
            &[table_name.into(), physical_column.into()],
        )
        .unwrap()
        .expect("logical field identity")
    }

    fn accepted_write_epoch(user_id: &str, client_id: &str) -> i64 {
        Spi::get_one_with_args(
            "SELECT accepted_write_epoch
             FROM sync_clients
             WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap()
        .expect("accepted write epoch")
    }

    fn push_ledger_counts(user_id: &str, client_id: &str) -> (i64, i64) {
        let batches: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_push_batches
             WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        let mutations: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_push_mutations
             WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        (
            batches.expect("push batch count"),
            mutations.expect("push mutation count"),
        )
    }

    fn source_wire_record(table_name: &str, record_id: &str) -> Value {
        Spi::connect(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let record = crate::pull::hydrate_records(client, table_name, &[record_id], &registry)
                .expect("hydrate source row for push outcome")
                .into_iter()
                .next()
                .expect("source row for push outcome");
            Ok::<Value, spi::Error>(record)
        })
        .unwrap()
    }

    fn assert_checksum_object(outcome: &Value) {
        let checksum = outcome
            .get("row_checksum")
            .expect("row-bearing outcome checksum");
        synchro_core::checksum::ChecksumObject::from_json(&checksum.to_string())
            .expect("valid row checksum object");
    }

    fn assert_row_outcome_matches_source(outcome: &Value, table_name: &str, record_id: &str) {
        assert_checksum_object(outcome);
        let source = source_wire_record(table_name, record_id);
        assert_eq!(outcome["server_row"], source["data"]);
        assert_eq!(outcome["row_checksum"], source["row_checksum"]);
        assert_eq!(outcome["server_version"], source["server_version"]);
    }

    fn assert_matching_fence(mutation_id: &str, server_version: &str) {
        let fence_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_write_fences WHERE mutation_id = $1",
            &[mutation_id.into()],
        )
        .unwrap();
        assert_eq!(fence_count, Some(1));
        let fence_version: Option<String> = Spi::get_one_with_args(
            "SELECT row_version::text FROM sync_write_fences WHERE mutation_id = $1",
            &[mutation_id.into()],
        )
        .unwrap();
        assert_eq!(fence_version.as_deref(), Some(server_version));
    }

    fn insert_live_order(record_id: &str, user_id: &str, title: &str) -> String {
        Spi::run_with_args(
            "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, $2, $3)",
            &[record_id.into(), user_id.into(), title.into()],
        )
        .unwrap();
        current_row_version("test_orders", record_id)
    }

    fn insert_deleted_order(record_id: &str, user_id: &str, title: &str) -> String {
        insert_live_order(record_id, user_id, title);
        Spi::run_with_args(
            "UPDATE test_orders SET deleted_at = now() WHERE id = $1::uuid",
            &[record_id.into()],
        )
        .unwrap();
        current_row_version("test_orders", record_id)
    }

    fn current_row_version(table_name: &str, record_id: &str) -> String {
        Spi::get_one_with_args(
            "SELECT v.row_version::text
             FROM sync_row_versions v
             JOIN sync_registry r ON r.relation_id = v.relation_id
             JOIN sync_registry_generations g ON g.generation = r.registry_generation
             WHERE g.state = 'active'
               AND r.table_name = $1
               AND v.record_id = $2",
            &[table_name.into(), record_id.into()],
        )
        .unwrap()
        .expect("authoritative row version")
    }

    fn issued_scope_cursor(
        user_id: &str,
        client_id: &str,
        scope_id: &str,
        checkpoint: i64,
    ) -> String {
        let position = if checkpoint == 0 {
            crate::stream_position::StreamPosition::GenerationStart
        } else {
            crate::stream_position::StreamPosition::effect(&format!("0/{checkpoint:08X}"), 0, 0)
                .expect("test effect position")
        };
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO sync_scope_state (scope_id, stream_generation)
                 SELECT $1, stream_generation FROM sync_runtime_state WHERE singleton = true
                 ON CONFLICT (scope_id) DO NOTHING",
                None,
                &[scope_id.into()],
            )?;
            let context = test_scope_cursor_context(client, user_id, client_id, scope_id);
            Ok::<_, pgrx::spi::Error>(
                crate::cursor_token::issue_scope_cursor(client, &context, &position)
                    .expect("issue scope cursor"),
            )
        })
        .expect("prepare scope cursor")
    }

    fn test_scope_cursor_context(
        client: &pgrx::spi::SpiClient<'_>,
        user_id: &str,
        client_id: &str,
        scope_id: &str,
    ) -> crate::cursor_token::ScopeCursorContext {
        let schema_hash = client
            .select(
                "SELECT schema_hash FROM sync_schema_manifest
                 ORDER BY schema_version DESC LIMIT 1",
                None,
                &[],
            )
            .expect("load test schema hash")
            .first()
            .get_by_name::<String, &str>("schema_hash")
            .expect("read test schema hash")
            .expect("test schema hash");
        crate::cursor_token::ScopeCursorContext::new(user_id, client_id, 1, scope_id, &schema_hash)
            .expect("build test scope cursor context")
    }

    fn scope_cursor_ref(user_id: &str, client_id: &str, scope_id: &str, checkpoint: i64) -> Value {
        json!({ "cursor": issued_scope_cursor(user_id, client_id, scope_id, checkpoint) })
    }

    /// Insert a changelog entry directly for test fixtures.
    fn insert_changelog(bucket_id: &str, table_name: &str, record_id: &str, operation: i16) {
        Spi::connect_mut(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let registration = registry
                .iter()
                .find(|table| table.table_name == table_name)
                .cloned();
            let captured = registration.as_ref().and_then(|_| {
                crate::pull::hydrate_records(client, table_name, &[record_id], &registry)
                    .ok()?
                    .into_iter()
                    .next()
            });
            let rows = client.update(
                "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation)
                 VALUES ($1, $2, $3, $4)
                 RETURNING seq",
                None,
                &[
                    bucket_id.into(),
                    table_name.into(),
                    record_id.into(),
                    operation.into(),
                ],
            )?;
            let seq = rows
                .first()
                .get_by_name::<i64, &str>("seq")?
                .expect("test changelog sequence");
            let Some(registration) = registration else {
                return Ok::<_, spi::Error>(());
            };
            let Some(captured) = captured else {
                return Ok::<_, spi::Error>(());
            };
            let row_data = captured["data"].clone();
            let checksum = synchro_core::checksum::Sha256Digest::from_lower_hex(
                captured["row_checksum"]["digest"]
                    .as_str()
                    .expect("captured row checksum digest"),
            )
            .expect("captured row checksum object")
            .as_bytes()
            .to_vec();
            let deleted = captured
                .get("deleted_at")
                .and_then(|value| value.as_str())
                .is_some();
            let row_version = captured["server_version"]
                .as_str()
                .expect("test row server version")
                .to_string();
            let commit_lsn = format!("0/{seq:08X}");
            let stream_generation: String = client
                .select(
                    "SELECT stream_generation FROM sync_runtime_state WHERE singleton = true",
                    None,
                    &[],
                )?
                .first()
                .get_by_name::<String, &str>("stream_generation")?
                .expect("test stream generation");
            let fence = client
                .select(
                    "SELECT version.fence_id::text AS fence_id, fence.operation
                     FROM sync_row_versions version
                     JOIN sync_write_fences fence ON fence.fence_id = version.fence_id
                     WHERE version.relation_id = $1::uuid AND version.record_id = $2",
                    None,
                    &[registration.relation_id.as_str().into(), record_id.into()],
                )?
                .first();
            let fence_id = fence
                .get_by_name::<String, &str>("fence_id")?
                .expect("test changelog fence identity");
            let source_operation = fence
                .get_by_name::<String, &str>("operation")?
                .expect("test changelog source operation");
            client.update(
                "INSERT INTO sync_scope_state (scope_id, stream_generation)
                 VALUES ($1, $2)
                 ON CONFLICT (scope_id) DO NOTHING",
                None,
                &[bucket_id.into(), stream_generation.as_str().into()],
            )?;
            client.update(
                "INSERT INTO sync_wal_transactions (
                     stream_generation, commit_lsn, end_lsn, source_xid,
                     registry_generation, event_count, effect_count, content_hash,
                     commit_timestamp
                 ) VALUES (
                     $1, $2::pg_lsn, $2::pg_lsn, $3::xid,
                     $4, 1, 1, decode(repeat('00', 32), 'hex'), now()
                 )",
                None,
                &[
                    stream_generation.as_str().into(),
                    commit_lsn.as_str().into(),
                    seq.to_string().as_str().into(),
                    registration.registry_generation.into(),
                ],
            )?;
            client.update(
                "INSERT INTO sync_wal_events (
                      stream_generation, commit_lsn, event_ordinal, relation_id,
                      registration_kind, physical_schema, physical_relation,
                      physical_relation_oid, operation, fence_id
                  ) VALUES (
                      $1, $2::pg_lsn, 0, $3::uuid, 'synced', $4, $5, $6::oid, $7, $8::uuid
                  )",
                None,
                &[
                    stream_generation.as_str().into(),
                    commit_lsn.as_str().into(),
                    registration.relation_id.as_str().into(),
                    registration.physical_schema.as_str().into(),
                    registration.physical_relation.as_str().into(),
                    i64::from(registration.physical_relation_oid).into(),
                    source_operation.as_str().into(),
                    fence_id.as_str().into(),
                ],
            )?;
            client.update(
                "INSERT INTO sync_captured_projections (
                     stream_generation, commit_lsn, event_ordinal, relation_id,
                     image_kind, registry_generation, record_id, row_data,
                     row_version, checksum, deleted
                 ) VALUES (
                     $1, $2::pg_lsn, 0, $3::uuid,
                     'after', $4, $5, $6, $7::uuid, $8, $9
                  )",
                None,
                &[
                    stream_generation.as_str().into(),
                    commit_lsn.as_str().into(),
                    registration.relation_id.as_str().into(),
                    registration.registry_generation.into(),
                    record_id.into(),
                    pgrx::JsonB(row_data.clone()).into(),
                    row_version.as_str().into(),
                    checksum.clone().into(),
                    deleted.into(),
                ],
            )?;
            client.update(
                "INSERT INTO sync_captured_rows (
                     relation_id, record_id, row_data, row_version, checksum, deleted,
                     source_stream_generation, source_commit_lsn, source_event_ordinal,
                     registry_generation
                 ) VALUES (
                     $1::uuid, $2, $3, $4::uuid, $5, $6, $7, $8::pg_lsn, 0, $9
                 )
                 ON CONFLICT (relation_id, record_id) DO UPDATE
                 SET row_data = EXCLUDED.row_data,
                     row_version = EXCLUDED.row_version,
                     checksum = EXCLUDED.checksum,
                     deleted = EXCLUDED.deleted,
                      source_stream_generation = EXCLUDED.source_stream_generation,
                      source_commit_lsn = EXCLUDED.source_commit_lsn,
                      source_event_ordinal = EXCLUDED.source_event_ordinal,
                      source_reset_id = NULL,
                      registry_generation = EXCLUDED.registry_generation,
                     updated_at = now()",
                None,
                &[
                    registration.relation_id.as_str().into(),
                    record_id.into(),
                    pgrx::JsonB(row_data).into(),
                    row_version.as_str().into(),
                    checksum.clone().into(),
                    deleted.into(),
                    stream_generation.as_str().into(),
                    commit_lsn.as_str().into(),
                    registration.registry_generation.into(),
                ],
            )?;
            client.update(
                "UPDATE sync_bucket_edges
                 SET row_version = $3::uuid, checksum = $4, updated_at = now()
                 WHERE relation_id = $1::uuid AND record_id = $2",
                None,
                &[
                    registration.relation_id.as_str().into(),
                    record_id.into(),
                    row_version.as_str().into(),
                    checksum.into(),
                ],
            )?;
            client.update(
                "UPDATE sync_changelog
                 SET stream_generation = $2,
                     commit_lsn = $3::pg_lsn,
                     event_ordinal = 0,
                     effect_ordinal = 0,
                     relation_id = $4::uuid,
                     row_version = $5::uuid,
                     projection_image = 'after'
                 WHERE seq = $1",
                None,
                &[
                    seq.into(),
                    stream_generation.as_str().into(),
                    commit_lsn.as_str().into(),
                    registration.relation_id.as_str().into(),
                    row_version.as_str().into(),
                ],
            )?;
            client.update(
                "UPDATE sync_wal_progress
                 SET stream_generation = $1,
                     materialized_commit_lsn = $2::pg_lsn,
                     materialized_end_lsn = $2::pg_lsn,
                     updated_at = now()
                 WHERE singleton = true",
                None,
                &[
                    stream_generation.as_str().into(),
                    commit_lsn.as_str().into(),
                ],
            )?;
            Ok::<_, spi::Error>(())
        })
        .unwrap();
    }

    /// Insert a bucket edge directly for test fixtures.
    fn insert_edge(table_name: &str, record_id: &str, bucket_id: &str) {
        Spi::connect_mut(|client| {
            let registry = crate::registry::load_registry_from_client(client)?;
            let table = registry
                .iter()
                .find(|table| table.table_name == table_name)
                .expect("edge test table registration");
            let record = crate::pull::hydrate_records(client, table_name, &[record_id], &registry)
                .expect("hydrate edge test row")
                .into_iter()
                .next()
                .expect("edge test row");
            let checksum = synchro_core::checksum::Sha256Digest::from_lower_hex(
                record["row_checksum"]["digest"]
                    .as_str()
                    .expect("edge test row checksum"),
            )
            .expect("edge test checksum object")
            .as_bytes()
            .to_vec();
            let row_version = record["server_version"]
                .as_str()
                .expect("edge test row version");
            client.update(
                "INSERT INTO sync_bucket_edges (
                     relation_id, table_name, record_id, bucket_id, checksum, row_version
                  ) VALUES ($1::uuid, $2, $3, $4, $5, $6::uuid)
                  ON CONFLICT DO NOTHING",
                None,
                &[
                    table.relation_id.as_str().into(),
                    table_name.into(),
                    record_id.into(),
                    bucket_id.into(),
                    checksum.into(),
                    row_version.into(),
                ],
            )?;
            Ok::<_, spi::Error>(())
        })
        .unwrap();
    }

    fn setup_pull_fixtures() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, internal_notes) VALUES
             ('a1111111-1111-1111-1111-111111111111', 'u1', 'Order 1', 'secret1'),
             ('a2222222-2222-2222-2222-222222222222', 'u1', 'Order 2', 'secret2')",
        )
        .unwrap();

        insert_changelog(
            "user:u1",
            "test_orders",
            "a1111111-1111-1111-1111-111111111111",
            1,
        );
        insert_changelog(
            "user:u1",
            "test_orders",
            "a2222222-2222-2222-2222-222222222222",
            1,
        );

        insert_edge(
            "test_orders",
            "a1111111-1111-1111-1111-111111111111",
            "user:u1",
        );
        insert_edge(
            "test_orders",
            "a2222222-2222-2222-2222-222222222222",
            "user:u1",
        );
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // The extension registers a Postmaster-context GUC (synchro.auto_start)
        // and a background worker, both of which require the shared library to
        // be loaded at server startup, not via CREATE EXTENSION.
        vec![
            "shared_preload_libraries = 'synchro_pg'",
            "synchro.auto_start = off",
            "wal_level = logical",
            "max_replication_slots = 8",
            "search_path = 'public, synchro, pg_catalog'",
        ]
    }
}
