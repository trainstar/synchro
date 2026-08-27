INSERT INTO regions (
    id, name, description, created_at, updated_at
) VALUES (
    '10000000-0000-0000-0000-000000000001',
    'North Test',
    'Seed region',
    '2026-01-01T00:00:00.000000Z',
    '2026-01-01T00:00:00.000000Z'
);

INSERT INTO nations (
    id, region_id, name, iso_code, metadata, created_at, updated_at
) VALUES (
    '10000000-0000-0000-0000-000000000002',
    '10000000-0000-0000-0000-000000000001',
    'Testland',
    'TL',
    '{"source":"seed"}',
    '2026-01-01T00:00:01.000000Z',
    '2026-01-01T00:00:01.000000Z'
);

INSERT INTO suppliers (
    id, nation_id, name, address, phone, website, rating, is_active, tags, created_at, updated_at
) VALUES (
    '10000000-0000-0000-0000-000000000003',
    '10000000-0000-0000-0000-000000000002',
    'Seed Supplier',
    '1 Seed Way',
    '555-0100',
    'https://example.test/supplier',
    4.5,
    true,
    ARRAY['seed'],
    '2026-01-01T00:00:02.000000Z',
    '2026-01-01T00:00:02.000000Z'
);

INSERT INTO parts (
    id, name, manufacturer, brand, part_type, size_cm, weight_kg, retail_price,
    description, specifications, tags, created_at, updated_at
) VALUES (
    '10000000-0000-0000-0000-000000000004',
    'Seed Part',
    'Test Manufacturing',
    'SeedBrand',
    'component',
    10,
    1.25,
    19.99,
    'Seeded part for bootstrap tests',
    '{"color":"blue"}',
    ARRAY['seed'],
    '2026-01-01T00:00:03.000000Z',
    '2026-01-01T00:00:03.000000Z'
);

INSERT INTO part_suppliers (
    id, part_id, supplier_id, available_quantity, supply_cost, lead_time_days,
    notes, created_at, updated_at
) VALUES (
    '10000000-0000-0000-0000-000000000005',
    '10000000-0000-0000-0000-000000000004',
    '10000000-0000-0000-0000-000000000003',
    50,
    9.99,
    7,
    'Seeded relationship',
    '2026-01-01T00:00:04.000000Z',
    '2026-01-01T00:00:04.000000Z'
);

INSERT INTO categories (
    id, name, sort_order, metadata, created_at, updated_at
) VALUES (
    '10000000-0000-0000-0000-000000000006',
    'Seed Category',
    1,
    '{"source":"seed"}',
    '2026-01-01T00:00:05.000000Z',
    '2026-01-01T00:00:05.000000Z'
);
