-- Run this file as synchro_quickstart_owner in the empty synchro_quickstart
-- database after the extension, roles, and worker configuration are ready.

\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE public.notes (
    id uuid PRIMARY KEY,
    owner_id text NOT NULL,
    body text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);

ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;

CREATE POLICY synchro_quickstart_notes_owner
    ON public.notes
    AS PERMISSIVE
    FOR ALL
    TO synchro_owner
    USING (
        NULLIF(current_setting('synchro.user_id', true), '') IS NULL
        OR owner_id = current_setting('synchro.user_id', true)
    )
    WITH CHECK (
        NULLIF(current_setting('synchro.user_id', true), '') IS NULL
        OR owner_id = current_setting('synchro.user_id', true)
    );

GRANT SELECT, INSERT, UPDATE ON public.notes TO synchro_owner;
GRANT SELECT ON public.notes TO synchro_worker;

SELECT synchro.synchro_prepare_projection_view(
    'public.notes',
    'notes',
    ARRAY['id', 'owner_id', 'body', 'updated_at', 'deleted_at']::text[]
);

CREATE FUNCTION public.notes_membership(p_id uuid)
RETURNS SETOF text
LANGUAGE SQL
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, synchro
BEGIN ATOMIC
    SELECT 'user:' || (note.owner_id #>> '{}')
    FROM synchro_projection.notes AS note
    WHERE note.record_id = p_id::text
      AND NOT note.deleted;
END;

REVOKE EXECUTE ON FUNCTION public.notes_membership(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.notes_membership(uuid)
    TO synchro_owner, synchro_worker;

SELECT synchro.synchro_register_table(
    'public.notes',
    'public.notes_membership',
    'single_scope',
    'id',
    'updated_at',
    'deleted_at',
    'enabled'
);

COMMIT;
