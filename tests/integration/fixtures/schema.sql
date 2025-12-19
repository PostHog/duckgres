-- Test Schema for Duckgres PostgreSQL Compatibility Tests
-- This schema is loaded into both PostgreSQL and Duckgres for side-by-side testing

-- Basic types table (using DuckDB-compatible types)
CREATE TABLE types_test (
    id INTEGER PRIMARY KEY,
    bool_col BOOLEAN,
    int2_col SMALLINT,
    int4_col INTEGER,
    int8_col BIGINT,
    float4_col REAL,
    float8_col DOUBLE PRECISION,
    numeric_col NUMERIC(12, 4),
    text_col TEXT,
    varchar_col VARCHAR(255),
    char_col CHAR(10),
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ,
    interval_col INTERVAL,
    uuid_col UUID,
    bytea_col BYTEA
);

-- Simple table for basic queries
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    age INTEGER,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    score NUMERIC(10, 2)
);

-- Products table
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    price NUMERIC(10, 2),
    stock INTEGER,
    created_at TIMESTAMP,
    tags TEXT[]
);

-- Orders table (for JOINs)
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    total_amount NUMERIC(12, 2),
    status TEXT,
    order_date TIMESTAMP
);

-- Order items for more complex JOINs
CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10, 2)
);

-- Categories with self-reference for recursive CTEs
CREATE TABLE categories (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id INTEGER,
    level INTEGER
);

-- Time series data for window functions
CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    product_id INTEGER,
    sale_date DATE,
    amount NUMERIC(12, 2),
    region TEXT
);

-- Events table for date/time tests
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    event_name TEXT,
    event_date DATE,
    event_time TIME,
    event_timestamp TIMESTAMP,
    event_timestamptz TIMESTAMPTZ,
    duration INTERVAL
);

-- JSON test table (simplified - no JSON columns for DuckDB compatibility)
CREATE TABLE json_data (
    id INTEGER PRIMARY KEY,
    data TEXT,
    attributes TEXT
);

-- Table with NULLs for NULL handling tests
CREATE TABLE nullable_test (
    id INTEGER PRIMARY KEY,
    val1 INTEGER,
    val2 TEXT,
    val3 BOOLEAN,
    val4 NUMERIC(10, 2)
);

-- Empty table for edge case testing
CREATE TABLE empty_table (
    id INTEGER PRIMARY KEY,
    name TEXT
);

-- Table for aggregate tests
CREATE TABLE metrics (
    id INTEGER PRIMARY KEY,
    category TEXT,
    metric_name TEXT,
    value NUMERIC(12, 4),
    recorded_at TIMESTAMP
);

-- Table for text search tests
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    title TEXT,
    body TEXT,
    author TEXT
);

-- Table for array tests
CREATE TABLE array_test (
    id INTEGER PRIMARY KEY,
    int_array INTEGER[],
    text_array TEXT[],
    nested_array INTEGER[][]
);

-- View for view tests
CREATE VIEW active_users AS
SELECT id, name, email, age, score
FROM users
WHERE active = true;

-- View with aggregation
CREATE VIEW user_stats AS
SELECT
    active,
    COUNT(*) as user_count,
    AVG(age) as avg_age,
    AVG(score) as avg_score
FROM users
GROUP BY active;

-- View for JOINs
CREATE VIEW order_details AS
SELECT
    o.id as order_id,
    u.name as user_name,
    p.name as product_name,
    o.quantity,
    o.total_amount,
    o.status,
    o.order_date
FROM orders o
LEFT JOIN users u ON o.user_id = u.id
LEFT JOIN products p ON o.product_id = p.id;

-- Schema for schema tests
CREATE SCHEMA IF NOT EXISTS test_schema;

CREATE TABLE test_schema.schema_test (
    id INTEGER PRIMARY KEY,
    value TEXT
);
