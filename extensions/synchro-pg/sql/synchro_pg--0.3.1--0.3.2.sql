-- Update synchro_pg from 0.3.1 to 0.3.2.
-- Stop if psql runs this file. ALTER EXTENSION removes the \echo line.
\echo Use "ALTER EXTENSION synchro_pg UPDATE TO '0.3.2'" to load this file. \quit

CREATE FUNCTION synchro.synchro_capture_fence_record()
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

ALTER FUNCTION synchro.synchro_capture_fence_record() OWNER TO synchro_owner;
REVOKE EXECUTE ON FUNCTION synchro.synchro_capture_fence_record() FROM PUBLIC;

CREATE OR REPLACE FUNCTION synchro.synchro_capture_fence()
RETURNS trigger
LANGUAGE c
SECURITY DEFINER
SET search_path = pg_catalog, synchro
AS 'MODULE_PATHNAME', 'synchro_capture_fence_wrapper';

UPDATE synchro.sync_extension_build
SET installed_fingerprint = synchro.synchro_build_fingerprint(),
    installed_at = pg_catalog.now()
WHERE singleton;
