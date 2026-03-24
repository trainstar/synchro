-- Register all seed tables for sync.
-- Run after schema.sql and CREATE EXTENSION synchro_pg.

-- =========================================================================
-- Reference tables: global bucket, read-only
-- =========================================================================

SELECT synchro_register_shared_scope('global', true);

SELECT synchro_register_table(
    'regions',
    $$SELECT ARRAY['global'] FROM regions WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro_register_table(
    'nations',
    $$SELECT ARRAY['global'] FROM nations WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro_register_table(
    'suppliers',
    $$SELECT ARRAY['global'] FROM suppliers WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro_register_table(
    'parts',
    $$SELECT ARRAY['global'] FROM parts WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro_register_table(
    'part_suppliers',
    $$SELECT ARRAY['global'] FROM part_suppliers WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

SELECT synchro_register_table(
    'categories',
    $$SELECT ARRAY['global'] FROM categories WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'read_only'
);

-- =========================================================================
-- User-owned tables: single-owner bucket via FK chain
-- =========================================================================

-- Direct ownership: bucket is user:{user_id}
SELECT synchro_register_table(
    'customers',
    $$SELECT ARRAY['user:' || user_id] FROM customers WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled',
    ARRAY['internal_notes']
);

-- Parent chain (1 level): orders -> customers.user_id
SELECT synchro_register_table(
    'orders',
    $$SELECT ARRAY['user:' || c.user_id]
      FROM orders o
      JOIN customers c ON c.id = o.customer_id
      WHERE o.id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- Parent chain (2 levels): line_items -> orders -> customers.user_id
SELECT synchro_register_table(
    'line_items',
    $$SELECT ARRAY['user:' || c.user_id]
      FROM line_items li
      JOIN orders o ON o.id = li.order_id
      JOIN customers c ON c.id = o.customer_id
      WHERE li.id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- =========================================================================
-- Collaboration tables: shared ownership, multi-bucket
-- =========================================================================

-- Documents: owned by owner_id
SELECT synchro_register_table(
    'documents',
    $$SELECT ARRAY['user:' || owner_id] FROM documents WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- Document members: each member gets the document in their bucket.
-- This creates multi-bucket membership (the document appears in every
-- member's bucket).
SELECT synchro_register_table(
    'document_members',
    $$SELECT array_agg('user:' || user_id)
      FROM document_members
      WHERE document_id = (
          SELECT document_id FROM document_members WHERE id = $1::uuid
      )$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- Document comments: visible to the document owner AND the comment author.
-- Multiple ownership paths (two different FK chains to user).
SELECT synchro_register_table(
    'document_comments',
    $$SELECT array_agg(DISTINCT bucket) FROM (
          SELECT 'user:' || d.owner_id AS bucket
          FROM document_comments dc
          JOIN documents d ON d.id = dc.document_id
          WHERE dc.id = $1::uuid
          UNION
          SELECT 'user:' || dc.author_id AS bucket
          FROM document_comments dc
          WHERE dc.id = $1::uuid
      ) sub$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);

-- =========================================================================
-- Type zoo: user-owned for push/pull testing
-- =========================================================================

SELECT synchro_register_table(
    'type_zoo',
    $$SELECT ARRAY['user:' || user_id] FROM type_zoo WHERE id = $1::uuid$$,
    'id', 'updated_at', 'deleted_at', 'enabled'
);
