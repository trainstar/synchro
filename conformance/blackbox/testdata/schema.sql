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
    -- The authored corpus never authors ownership. A push materializes it
    -- from the extension's push identity context, issue #42.
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX cf_items_owner_id_idx ON cf_items (owner_id);

-- Capture-dependency source. The extension requires one plain primary-key
-- column for the capture key and at least one captured column that the key does
-- not contain, so the authored canonical key and the captured value hold
-- separate columns.
CREATE TABLE cf_item_impacts (
    id TEXT PRIMARY KEY,
    scope_key TEXT NOT NULL
);

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

CREATE TABLE cf_document_access (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES cf_documents (id),
    owner_id TEXT NOT NULL
);

CREATE INDEX cf_document_access_document_id_idx ON cf_document_access (document_id);

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
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    authored_mutation JSONB NOT NULL,
    -- The authored registration declares default "" for this field, so the
    -- realization carries the same default.
    legacy_value TEXT NOT NULL DEFAULT '',
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
