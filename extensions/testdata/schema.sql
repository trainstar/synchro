-- Test seed schema: TPC-H inspired with modernized types.
-- PostgreSQL specific. Other databases get their own DDL from the same
-- logical schema.

-- =========================================================================
-- Reference tables (global bucket, read-only)
-- =========================================================================

CREATE TABLE IF NOT EXISTS regions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS nations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    region_id UUID NOT NULL REFERENCES regions(id),
    name TEXT NOT NULL,
    iso_code CHAR(2),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS suppliers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nation_id UUID NOT NULL REFERENCES nations(id),
    name TEXT NOT NULL,
    address TEXT,
    phone TEXT,
    website TEXT,
    rating NUMERIC(3,2),
    is_active BOOLEAN NOT NULL DEFAULT true,
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS parts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    manufacturer TEXT,
    brand TEXT,
    part_type TEXT,
    size_cm INTEGER,
    weight_kg NUMERIC(10,3),
    retail_price NUMERIC(15,2),
    description TEXT,
    specifications JSONB DEFAULT '{}',
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS part_suppliers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    part_id UUID NOT NULL REFERENCES parts(id),
    supplier_id UUID NOT NULL REFERENCES suppliers(id),
    available_quantity INTEGER NOT NULL DEFAULT 0,
    supply_cost NUMERIC(15,2) NOT NULL,
    lead_time_days SMALLINT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    UNIQUE (part_id, supplier_id)
);

CREATE TABLE IF NOT EXISTS categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id UUID REFERENCES categories(id),
    name TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

-- =========================================================================
-- User-owned tables (user bucket, read-write)
-- =========================================================================

CREATE TABLE IF NOT EXISTS customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    address JSONB DEFAULT '{}',
    nation_id UUID REFERENCES nations(id),
    balance NUMERIC(15,2) NOT NULL DEFAULT 0,
    market_segment TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    preferences JSONB DEFAULT '{}',
    internal_notes TEXT DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_customers_user_id ON customers(user_id);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL DEFAULT 'pending',
    total_price NUMERIC(15,2) NOT NULL DEFAULT 0,
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    priority TEXT DEFAULT 'normal',
    ship_date DATE,
    ship_address JSONB,
    order_comment TEXT,
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);

CREATE TABLE IF NOT EXISTS line_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(id),
    part_id UUID REFERENCES parts(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(15,2) NOT NULL,
    discount NUMERIC(5,2) NOT NULL DEFAULT 0,
    tax NUMERIC(5,2) NOT NULL DEFAULT 0,
    line_status TEXT NOT NULL DEFAULT 'open',
    ship_date DATE,
    receipt_date DATE,
    ship_instructions TEXT,
    line_comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_line_items_order_id ON line_items(order_id);

-- =========================================================================
-- Collaboration tables (shared ownership, multi-bucket)
-- =========================================================================

CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT DEFAULT '',
    doc_type TEXT NOT NULL DEFAULT 'note',
    is_public BOOLEAN NOT NULL DEFAULT false,
    version_number INTEGER NOT NULL DEFAULT 1,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_documents_owner_id ON documents(owner_id);

CREATE TABLE IF NOT EXISTS document_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id),
    user_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer',
    invited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    UNIQUE (document_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_document_members_user_id ON document_members(user_id);
CREATE INDEX IF NOT EXISTS idx_document_members_document_id ON document_members(document_id);

CREATE TABLE IF NOT EXISTS document_comments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id),
    author_id TEXT NOT NULL,
    parent_comment_id UUID REFERENCES document_comments(id),
    body TEXT NOT NULL,
    is_resolved BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_document_comments_document_id ON document_comments(document_id);
CREATE INDEX IF NOT EXISTS idx_document_comments_author_id ON document_comments(author_id);

-- =========================================================================
-- Type zoo (one column per PG type for round-trip validation)
-- =========================================================================

CREATE TABLE IF NOT EXISTS type_zoo (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    col_text TEXT,
    col_varchar VARCHAR(255),
    col_char CHAR(10),
    col_boolean BOOLEAN,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_numeric NUMERIC(20,6),
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_date DATE,
    col_timestamptz TIMESTAMPTZ,
    col_timestamp TIMESTAMP,
    col_interval INTERVAL,
    col_jsonb JSONB,
    col_json JSON,
    col_text_array TEXT[],
    col_int_array INTEGER[],
    col_bytea BYTEA,
    col_inet INET,
    col_cidr CIDR,
    col_macaddr MACADDR,
    col_uuid UUID,
    col_point POINT,
    col_int4range INT4RANGE,
    col_tstzrange TSTZRANGE,
    col_xml XML,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);
