-- Northwind seed data for Synchro test schema.
-- Idempotent — safe to run multiple times.

-- Products (server-managed catalog, PushPolicyDisabled)
INSERT INTO products (id, name, unit_price, units_in_stock, discontinued)
VALUES
    ('00000000-0000-0000-0000-seed00000001', 'Widget A', 9.99, 100, false),
    ('00000000-0000-0000-0000-seed00000002', 'Widget B', 19.99, 50, false),
    ('00000000-0000-0000-0000-seed00000003', 'Gadget C', 49.99, 25, false),
    ('00000000-0000-0000-0000-seed00000004', 'Gizmo D', 4.99, 200, false),
    ('00000000-0000-0000-0000-seed00000005', 'Thingamajig E', 99.99, 0, true)
ON CONFLICT (id) DO NOTHING;

-- Categories (global read + owner write)
INSERT INTO categories (id, user_id, name, description, search_tags)
VALUES
    ('00000000-0000-0000-0000-seed00000010', NULL, 'Electronics', 'Electronic devices and accessories', '{electronics,tech}'),
    ('00000000-0000-0000-0000-seed00000011', NULL, 'Office Supplies', 'Pens, paper, and desk accessories', '{office,supplies}'),
    ('00000000-0000-0000-0000-seed00000012', NULL, 'Hardware', 'Tools and building materials', '{hardware,tools}')
ON CONFLICT (id) DO NOTHING;
