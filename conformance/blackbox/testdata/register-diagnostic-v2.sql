-- Independent public registration setup for protocol 2 diagnostics.
-- Run only after schema.sql and CREATE EXTENSION synchro_pg.

SELECT synchro_register_shared_scope('cf:global', true);

SELECT synchro_register_table(
    'cf_global_items',
    $$SELECT ARRAY['cf:global'] FROM cf_global_items WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro_register_table(
    'cf_items',
    $$SELECT ARRAY['user:' || owner_id] FROM cf_items WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro_register_table(
    'cf_documents',
    $$SELECT ARRAY['user:' || owner_id] FROM cf_documents WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro_register_table(
    'cf_document_members',
    $$SELECT ARRAY[
        'user:' || document.owner_id,
        'user:' || member.member_id
    ]
      FROM cf_document_members AS member
      JOIN cf_documents AS document ON document.id = member.document_id
      WHERE member.id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro_register_table(
    'cf_document_notes',
    $$SELECT ARRAY[
        'user:' || document.owner_id,
        'user:' || note.author_id
    ]
      FROM cf_document_notes AS note
      JOIN cf_documents AS document ON document.id = note.document_id
      WHERE note.id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro_register_table(
    'cf_schema_queue',
    $$SELECT ARRAY['user:' || owner_id] FROM cf_schema_queue WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

SELECT synchro_register_table(
    'cf_decode_trap',
    $$SELECT ARRAY['user:' || owner_id] FROM cf_decode_trap WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);
