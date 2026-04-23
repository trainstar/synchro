-- synchro_pg extension: infrastructure tables
-- All tables created by CREATE EXTENSION synchro_pg

-- Table registry: stores registered table configurations and bucket SQL.
-- New table (replaces Go-side Registry struct).
CREATE TABLE IF NOT EXISTS sync_registry (
    table_name TEXT PRIMARY KEY,
    bucket_sql TEXT NOT NULL,
    pk_column TEXT NOT NULL DEFAULT 'id',
    updated_at_col TEXT NOT NULL DEFAULT 'updated_at',
    deleted_at_col TEXT NOT NULL DEFAULT 'deleted_at',
    push_policy TEXT NOT NULL DEFAULT 'enabled',
    sync_columns TEXT[] NOT NULL DEFAULT '{}',
    exclude_columns TEXT[] NOT NULL DEFAULT '{}',
    has_updated_at BOOLEAN NOT NULL DEFAULT true,
    has_deleted_at BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE sync_registry
    ADD COLUMN IF NOT EXISTS sync_columns TEXT[] NOT NULL DEFAULT '{}';

-- Changelog table: ordered log of all changes by bucket.
CREATE TABLE IF NOT EXISTS sync_changelog (
    seq BIGSERIAL PRIMARY KEY,
    bucket_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_changelog_bucket_seq
    ON sync_changelog (bucket_id, seq);

CREATE INDEX IF NOT EXISTS idx_sync_changelog_record
    ON sync_changelog (table_name, record_id);

-- Client registration table.
CREATE TABLE IF NOT EXISTS sync_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT,
    platform TEXT NOT NULL DEFAULT '',
    app_version TEXT NOT NULL DEFAULT '',
    bucket_subs TEXT[] NOT NULL DEFAULT '{}',
    scope_set_version BIGINT NOT NULL DEFAULT 1,
    last_sync_at TIMESTAMPTZ,
    last_pull_at TIMESTAMPTZ,
    last_push_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, client_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_clients_user_id
    ON sync_clients (user_id);

CREATE TABLE IF NOT EXISTS sync_shared_scopes (
    scope_id TEXT PRIMARY KEY,
    portable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bucket edge membership index.
CREATE TABLE IF NOT EXISTS sync_bucket_edges (
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    checksum INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, record_id, bucket_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_bucket_edges_bucket
    ON sync_bucket_edges (bucket_id, table_name, record_id);

-- Rule failure log for operational debugging.
CREATE TABLE IF NOT EXISTS sync_rule_failures (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation SMALLINT NOT NULL,
    error_text TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_rule_failures_created
    ON sync_rule_failures (created_at);

-- Schema version/hash manifest.
CREATE TABLE IF NOT EXISTS sync_schema_manifest (
    schema_version BIGINT PRIMARY KEY,
    schema_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_schema_manifest_hash
    ON sync_schema_manifest (schema_hash);

-- Per-bucket checkpoints for each client.
CREATE TABLE IF NOT EXISTS sync_client_checkpoints (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    checkpoint BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, client_id, bucket_id)
);
