-- Independent diagnostic source schema.
-- This file never creates or mutates Synchro internal state.

CREATE TABLE cf_global_items (
    id UUID PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE cf_items (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_items_owner_id_idx ON cf_items (owner_id);

CREATE TABLE cf_documents (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    title TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_documents_owner_id_idx ON cf_documents (owner_id);

CREATE TABLE cf_document_members (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES cf_documents (id),
    member_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ,
    UNIQUE (document_id, member_id)
);

CREATE INDEX cf_document_members_document_id_idx ON cf_document_members (document_id);
CREATE INDEX cf_document_members_member_id_idx ON cf_document_members (member_id);

CREATE TABLE cf_document_notes (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES cf_documents (id),
    author_id TEXT NOT NULL,
    body TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_document_notes_document_id_idx ON cf_document_notes (document_id);

CREATE TABLE cf_schema_queue (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    authored_mutation JSONB NOT NULL,
    legacy_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_schema_queue_owner_id_idx ON cf_schema_queue (owner_id);

CREATE TABLE cf_decode_trap (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    unsupported_value POINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_decode_trap_owner_id_idx ON cf_decode_trap (owner_id);

CREATE TABLE cf_late_registration (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_late_registration_owner_id_idx ON cf_late_registration (owner_id);
