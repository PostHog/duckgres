#!/bin/bash
# Seed script for DuckLake catalog with sample data stored in MinIO
#
# This script connects to a running Duckgres server with DuckLake configured
# (PostgreSQL metadata + MinIO object storage) and creates tables with sample
# data. DuckLake automatically stores the data as Parquet files in MinIO.
#
# Prerequisites:
#   - docker compose up -d (PostgreSQL + MinIO running)
#   - ./duckgres --config duckgres.yaml (Duckgres server running)
#
# Usage:
#   ./scripts/seed_ducklake.sh [options]
#
# Options:
#   --host HOST       Server host (default: 127.0.0.1)
#   --port PORT       Server port (default: 5432)
#   --user USER       Username (default: postgres)
#   --password PASS   Password (default: postgres)
#   --clean           Drop existing tables before seeding

set -e

# Default configuration
HOST="127.0.0.1"
PORT="5432"
USER="postgres"
PASSWORD="postgres"
CLEAN=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --password)
            PASSWORD="$2"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Seeds the DuckLake catalog with sample data stored as Parquet in MinIO."
            echo ""
            echo "Options:"
            echo "  --host HOST       Server host (default: 127.0.0.1)"
            echo "  --port PORT       Server port (default: 5432)"
            echo "  --user USER       Username (default: postgres)"
            echo "  --password PASS   Password (default: postgres)"
            echo "  --clean           Drop existing tables before seeding"
            echo ""
            echo "Prerequisites:"
            echo "  1. Start infrastructure: docker compose up -d"
            echo "  2. Start Duckgres: ./duckgres --config duckgres.yaml"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Connection string
CONN="host=$HOST port=$PORT user=$USER sslmode=require"

echo "=== DuckLake Catalog Seed Script ==="
echo "Seeding DuckLake with Parquet data in MinIO"
echo ""
echo "Connecting to $HOST:$PORT as $USER..."

# Function to run SQL commands
run_sql() {
    PGPASSWORD="$PASSWORD" psql "$CONN" -t -c "$1" 2>&1
}

# Function to run SQL from heredoc
run_sql_script() {
    PGPASSWORD="$PASSWORD" psql "$CONN" -t 2>&1
}

# Check connection
echo "Checking connection..."
if ! run_sql "SELECT 1" > /dev/null 2>&1; then
    echo "ERROR: Failed to connect to server at $HOST:$PORT"
    echo ""
    echo "Make sure:"
    echo "  1. Docker services are running: docker compose up -d"
    echo "  2. Duckgres is running: ./duckgres --config duckgres.yaml"
    exit 1
fi
echo "Connected successfully!"
echo ""

# Check if DuckLake catalog is attached
echo "Checking DuckLake catalog..."
if ! run_sql "SHOW ALL TABLES" 2>&1 | grep -q "ducklake"; then
    echo "ERROR: DuckLake catalog is not attached."
    echo ""
    echo "Make sure duckgres.yaml has DuckLake configured with:"
    echo "  - metadata_store pointing to PostgreSQL"
    echo "  - object_store pointing to MinIO (s3://ducklake/data/)"
    echo "  - S3 credentials configured"
    exit 1
fi
echo "DuckLake catalog is available!"
echo ""

# Clean existing tables if requested
if [ "$CLEAN" = true ]; then
    echo "Cleaning existing tables..."
    run_sql "DROP TABLE IF EXISTS ducklake.main.order_items" || true
    run_sql "DROP TABLE IF EXISTS ducklake.main.orders" || true
    run_sql "DROP TABLE IF EXISTS ducklake.main.products" || true
    run_sql "DROP TABLE IF EXISTS ducklake.main.customers" || true
    run_sql "DROP TABLE IF EXISTS ducklake.main.categories" || true
    run_sql "DROP TABLE IF EXISTS ducklake.main.events" || true
    run_sql "DROP TABLE IF EXISTS ducklake.main.page_views" || true
    echo "Done!"
    echo ""
fi

echo "=== Creating Tables in DuckLake Catalog ==="
echo "(Data will be stored as Parquet files in MinIO)"
echo ""

# Create categories table
echo "Creating ducklake.main.categories..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.categories (
    id INTEGER,
    name VARCHAR,
    description VARCHAR,
    created_at TIMESTAMP
);
EOF

# Create products table
echo "Creating ducklake.main.products..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.products (
    id INTEGER,
    name VARCHAR,
    description VARCHAR,
    category_id INTEGER,
    price DOUBLE,
    stock_quantity INTEGER,
    sku VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
EOF

# Create customers table
echo "Creating ducklake.main.customers..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.customers (
    id INTEGER,
    email VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    company VARCHAR,
    phone VARCHAR,
    country VARCHAR,
    city VARCHAR,
    created_at TIMESTAMP,
    last_login TIMESTAMP
);
EOF

# Create orders table
echo "Creating ducklake.main.orders..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.orders (
    id INTEGER,
    customer_id INTEGER,
    order_date TIMESTAMP,
    status VARCHAR,
    total_amount DOUBLE,
    shipping_address VARCHAR,
    notes VARCHAR
);
EOF

# Create order_items table
echo "Creating ducklake.main.order_items..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.order_items (
    id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DOUBLE,
    discount_percent DOUBLE
);
EOF

# Create events table (analytics-style)
echo "Creating ducklake.main.events..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.events (
    id INTEGER,
    event_name VARCHAR,
    distinct_id VARCHAR,
    properties VARCHAR,
    event_timestamp TIMESTAMP,
    session_id VARCHAR,
    page_url VARCHAR
);
EOF

# Create page_views table
echo "Creating ducklake.main.page_views..."
run_sql_script <<'EOF'
CREATE TABLE IF NOT EXISTS ducklake.main.page_views (
    id INTEGER,
    visitor_id VARCHAR,
    page_path VARCHAR,
    referrer VARCHAR,
    user_agent VARCHAR,
    duration_seconds INTEGER,
    view_timestamp TIMESTAMP
);
EOF

echo ""
echo "=== Inserting Data into DuckLake ==="
echo "(This creates Parquet files in MinIO)"
echo ""

# Seed categories
echo "Inserting categories..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.categories (id, name, description, created_at) VALUES
    (1, 'Electronics', 'Electronic devices and accessories', '2024-01-01 00:00:00'),
    (2, 'Clothing', 'Apparel and fashion items', '2024-01-01 00:00:00'),
    (3, 'Books', 'Physical and digital books', '2024-01-01 00:00:00'),
    (4, 'Home & Garden', 'Home improvement and garden supplies', '2024-01-01 00:00:00'),
    (5, 'Sports', 'Sports equipment and accessories', '2024-01-01 00:00:00');
EOF

# Seed products
echo "Inserting products..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.products (id, name, description, category_id, price, stock_quantity, sku, is_active, created_at, updated_at) VALUES
    (1, 'Wireless Mouse', 'Ergonomic wireless mouse with USB receiver', 1, 29.99, 150, 'ELEC-001', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (2, 'Mechanical Keyboard', 'RGB mechanical keyboard with Cherry MX switches', 1, 149.99, 75, 'ELEC-002', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (3, 'USB-C Hub', '7-in-1 USB-C hub with HDMI and SD card reader', 1, 59.99, 200, 'ELEC-003', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (4, '27" Monitor', '4K IPS display with HDR support', 1, 449.99, 30, 'ELEC-004', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (5, 'Cotton T-Shirt', 'Premium cotton crew neck t-shirt', 2, 24.99, 500, 'CLTH-001', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (6, 'Denim Jeans', 'Classic fit denim jeans', 2, 69.99, 300, 'CLTH-002', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (7, 'Running Shoes', 'Lightweight running shoes with cushioning', 5, 129.99, 100, 'SPRT-001', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (8, 'Yoga Mat', 'Non-slip yoga mat with carrying strap', 5, 39.99, 250, 'SPRT-002', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (9, 'Programming in Go', 'Comprehensive guide to Go programming', 3, 49.99, 80, 'BOOK-001', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (10, 'Data Engineering Handbook', 'Modern data engineering practices', 3, 59.99, 60, 'BOOK-002', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (11, 'Garden Tool Set', '5-piece stainless steel garden tools', 4, 44.99, 120, 'HOME-001', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (12, 'LED Desk Lamp', 'Adjustable LED lamp with USB charging', 4, 34.99, 180, 'HOME-002', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (13, 'Bluetooth Headphones', 'Noise-canceling over-ear headphones', 1, 199.99, 90, 'ELEC-005', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (14, 'Laptop Stand', 'Adjustable aluminum laptop stand', 1, 49.99, 160, 'ELEC-006', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    (15, 'Webcam HD', '1080p webcam with built-in microphone', 1, 79.99, 110, 'ELEC-007', true, '2024-01-01 00:00:00', '2024-01-01 00:00:00');
EOF

# Seed customers
echo "Inserting customers..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.customers (id, email, first_name, last_name, company, phone, country, city, created_at, last_login) VALUES
    (1, 'alice@example.com', 'Alice', 'Johnson', 'Tech Corp', '+1-555-0101', 'USA', 'San Francisco', '2024-01-01 00:00:00', '2024-01-20 10:00:00'),
    (2, 'bob@example.com', 'Bob', 'Smith', 'Data Inc', '+1-555-0102', 'USA', 'New York', '2024-01-02 00:00:00', '2024-01-24 12:30:00'),
    (3, 'carol@example.com', 'Carol', 'Williams', 'Analytics Ltd', '+44-20-7123-4567', 'UK', 'London', '2024-01-03 00:00:00', '2024-01-17 09:15:00'),
    (4, 'david@example.com', 'David', 'Brown', 'StartupXYZ', '+1-555-0104', 'USA', 'Austin', '2024-01-04 00:00:00', '2024-01-18 16:20:00'),
    (5, 'eva@example.com', 'Eva', 'Martinez', 'Cloud Systems', '+49-30-12345678', 'Germany', 'Berlin', '2024-01-05 00:00:00', '2024-01-19 11:00:00'),
    (6, 'frank@example.com', 'Frank', 'Garcia', 'DevOps Pro', '+1-555-0106', 'USA', 'Seattle', '2024-01-06 00:00:00', '2024-01-21 08:45:00'),
    (7, 'grace@example.com', 'Grace', 'Lee', 'AI Research', '+81-3-1234-5678', 'Japan', 'Tokyo', '2024-01-07 00:00:00', '2024-01-22 15:10:00'),
    (8, 'henry@example.com', 'Henry', 'Wilson', 'FinTech Co', '+1-555-0108', 'USA', 'Chicago', '2024-01-08 00:00:00', '2024-01-23 10:00:00'),
    (9, 'iris@example.com', 'Iris', 'Taylor', 'E-commerce Plus', '+33-1-23-45-67-89', 'France', 'Paris', '2024-01-09 00:00:00', '2024-01-25 09:45:00'),
    (10, 'jack@example.com', 'Jack', 'Anderson', 'SaaS Solutions', '+1-555-0110', 'Canada', 'Toronto', '2024-01-10 00:00:00', '2024-01-26 14:00:00');
EOF

# Seed orders
echo "Inserting orders..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.orders (id, customer_id, order_date, status, total_amount, shipping_address, notes) VALUES
    (1, 1, '2024-01-15 10:30:00', 'completed', 179.98, '123 Tech Street, San Francisco, CA 94102', NULL),
    (2, 2, '2024-01-16 14:45:00', 'completed', 449.99, '456 Data Ave, New York, NY 10001', NULL),
    (3, 3, '2024-01-17 09:15:00', 'shipped', 94.98, '789 Analytics Rd, London, UK', 'Express shipping'),
    (4, 4, '2024-01-18 16:20:00', 'processing', 269.97, '321 Startup Blvd, Austin, TX 78701', NULL),
    (5, 5, '2024-01-19 11:00:00', 'completed', 59.99, '654 Cloud Way, Berlin, Germany', NULL),
    (6, 1, '2024-01-20 13:30:00', 'shipped', 199.99, '123 Tech Street, San Francisco, CA 94102', 'Gift wrap requested'),
    (7, 6, '2024-01-21 08:45:00', 'pending', 129.99, '987 DevOps Lane, Seattle, WA 98101', NULL),
    (8, 7, '2024-01-22 15:10:00', 'completed', 109.98, '246 AI Street, Tokyo, Japan', NULL),
    (9, 8, '2024-01-23 10:00:00', 'processing', 329.97, '135 FinTech Plaza, Chicago, IL 60601', 'Priority order'),
    (10, 2, '2024-01-24 12:30:00', 'completed', 79.99, '456 Data Ave, New York, NY 10001', NULL),
    (11, 9, '2024-01-25 09:45:00', 'shipped', 174.98, '864 Commerce St, Paris, France', NULL),
    (12, 10, '2024-01-26 14:00:00', 'pending', 239.98, '753 SaaS Road, Toronto, Canada', NULL);
EOF

# Seed order items
echo "Inserting order items..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.order_items (id, order_id, product_id, quantity, unit_price, discount_percent) VALUES
    (1, 1, 1, 1, 29.99, 0),
    (2, 1, 2, 1, 149.99, 0),
    (3, 2, 4, 1, 449.99, 0),
    (4, 3, 5, 2, 24.99, 0),
    (5, 3, 8, 1, 39.99, 5),
    (6, 4, 7, 1, 129.99, 0),
    (7, 4, 6, 2, 69.99, 0),
    (8, 5, 3, 1, 59.99, 0),
    (9, 6, 13, 1, 199.99, 0),
    (10, 7, 7, 1, 129.99, 0),
    (11, 8, 9, 1, 49.99, 0),
    (12, 8, 10, 1, 59.99, 0),
    (13, 9, 2, 1, 149.99, 10),
    (14, 9, 14, 2, 49.99, 0),
    (15, 9, 1, 1, 29.99, 0),
    (16, 10, 15, 1, 79.99, 0),
    (17, 11, 12, 2, 34.99, 0),
    (18, 11, 11, 1, 44.99, 5),
    (19, 12, 13, 1, 199.99, 10),
    (20, 12, 8, 1, 39.99, 0);
EOF

# Seed events (analytics data)
echo "Inserting events..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.events (id, event_name, distinct_id, properties, event_timestamp, session_id, page_url) VALUES
    (1, 'page_view', 'user_001', '{"page": "/home", "referrer": "google.com"}', '2024-01-20 10:00:00', 'sess_abc123', 'https://example.com/home'),
    (2, 'button_click', 'user_001', '{"button": "signup", "variant": "blue"}', '2024-01-20 10:01:30', 'sess_abc123', 'https://example.com/home'),
    (3, 'signup', 'user_001', '{"method": "email", "plan": "free"}', '2024-01-20 10:05:00', 'sess_abc123', 'https://example.com/signup'),
    (4, 'page_view', 'user_002', '{"page": "/products", "category": "electronics"}', '2024-01-20 11:15:00', 'sess_def456', 'https://example.com/products'),
    (5, 'add_to_cart', 'user_002', '{"product_id": 1, "quantity": 1}', '2024-01-20 11:20:00', 'sess_def456', 'https://example.com/products/1'),
    (6, 'checkout_started', 'user_002', '{"cart_value": 29.99}', '2024-01-20 11:25:00', 'sess_def456', 'https://example.com/checkout'),
    (7, 'purchase', 'user_002', '{"order_id": 123, "total": 29.99}', '2024-01-20 11:30:00', 'sess_def456', 'https://example.com/checkout/success'),
    (8, 'page_view', 'user_003', '{"page": "/blog"}', '2024-01-20 12:00:00', 'sess_ghi789', 'https://example.com/blog'),
    (9, 'page_view', 'user_003', '{"page": "/blog/post-1"}', '2024-01-20 12:02:00', 'sess_ghi789', 'https://example.com/blog/post-1'),
    (10, 'share', 'user_003', '{"platform": "twitter", "content": "blog-post-1"}', '2024-01-20 12:10:00', 'sess_ghi789', 'https://example.com/blog/post-1'),
    (11, 'search', 'user_004', '{"query": "wireless mouse", "results": 5}', '2024-01-20 14:00:00', 'sess_jkl012', 'https://example.com/search'),
    (12, 'page_view', 'user_004', '{"page": "/products/1"}', '2024-01-20 14:01:00', 'sess_jkl012', 'https://example.com/products/1'),
    (13, 'feature_flag', 'user_005', '{"flag": "new_checkout", "enabled": true}', '2024-01-20 15:30:00', 'sess_mno345', 'https://example.com/checkout'),
    (14, 'error', 'user_005', '{"type": "payment_failed", "code": "card_declined"}', '2024-01-20 15:35:00', 'sess_mno345', 'https://example.com/checkout'),
    (15, 'support_ticket', 'user_005', '{"category": "billing", "priority": "high"}', '2024-01-20 15:40:00', 'sess_mno345', 'https://example.com/support');
EOF

# Seed page_views
echo "Inserting page views..."
run_sql_script <<'EOF'
INSERT INTO ducklake.main.page_views (id, visitor_id, page_path, referrer, user_agent, duration_seconds, view_timestamp) VALUES
    (1, 'v_001', '/', 'https://google.com', 'Mozilla/5.0 Chrome/120.0', 45, '2024-01-20 08:00:00'),
    (2, 'v_001', '/products', NULL, 'Mozilla/5.0 Chrome/120.0', 120, '2024-01-20 08:01:00'),
    (3, 'v_001', '/products/electronics', NULL, 'Mozilla/5.0 Chrome/120.0', 90, '2024-01-20 08:03:00'),
    (4, 'v_002', '/', 'https://twitter.com', 'Mozilla/5.0 Safari/17.0', 30, '2024-01-20 09:15:00'),
    (5, 'v_002', '/about', NULL, 'Mozilla/5.0 Safari/17.0', 60, '2024-01-20 09:16:00'),
    (6, 'v_003', '/blog', 'https://linkedin.com', 'Mozilla/5.0 Firefox/121.0', 180, '2024-01-20 10:30:00'),
    (7, 'v_003', '/blog/data-engineering-guide', NULL, 'Mozilla/5.0 Firefox/121.0', 420, '2024-01-20 10:33:00'),
    (8, 'v_004', '/pricing', 'https://google.com', 'Mozilla/5.0 Chrome/120.0', 90, '2024-01-20 11:00:00'),
    (9, 'v_004', '/signup', NULL, 'Mozilla/5.0 Chrome/120.0', 150, '2024-01-20 11:02:00'),
    (10, 'v_005', '/', NULL, 'Mozilla/5.0 Edge/120.0', 20, '2024-01-20 12:00:00'),
    (11, 'v_006', '/products', 'https://bing.com', 'Mozilla/5.0 Chrome/120.0', 75, '2024-01-20 13:45:00'),
    (12, 'v_006', '/cart', NULL, 'Mozilla/5.0 Chrome/120.0', 60, '2024-01-20 13:47:00'),
    (13, 'v_006', '/checkout', NULL, 'Mozilla/5.0 Chrome/120.0', 120, '2024-01-20 13:48:00'),
    (14, 'v_007', '/docs', 'https://github.com', 'Mozilla/5.0 Chrome/120.0', 300, '2024-01-20 14:30:00'),
    (15, 'v_007', '/docs/getting-started', NULL, 'Mozilla/5.0 Chrome/120.0', 480, '2024-01-20 14:35:00');
EOF

echo ""
echo "=== Verifying Data ==="

echo ""
echo "Table row counts in DuckLake catalog:"
echo "--------------------------------------"
run_sql "SELECT 'categories' as table_name, COUNT(*) as rows FROM ducklake.main.categories"
run_sql "SELECT 'products' as table_name, COUNT(*) as rows FROM ducklake.main.products"
run_sql "SELECT 'customers' as table_name, COUNT(*) as rows FROM ducklake.main.customers"
run_sql "SELECT 'orders' as table_name, COUNT(*) as rows FROM ducklake.main.orders"
run_sql "SELECT 'order_items' as table_name, COUNT(*) as rows FROM ducklake.main.order_items"
run_sql "SELECT 'events' as table_name, COUNT(*) as rows FROM ducklake.main.events"
run_sql "SELECT 'page_views' as table_name, COUNT(*) as rows FROM ducklake.main.page_views"

echo ""
echo "=== Sample Queries ==="
echo ""
echo "Top 5 products by price:"
run_sql "SELECT name, price FROM ducklake.main.products ORDER BY price DESC LIMIT 5"

echo ""
echo "Orders by status:"
run_sql "SELECT status, COUNT(*) as count FROM ducklake.main.orders GROUP BY status ORDER BY count DESC"

echo ""
echo "Event types:"
run_sql "SELECT event_name, COUNT(*) as count FROM ducklake.main.events GROUP BY event_name ORDER BY count DESC"

echo ""
echo "=== Seed Complete! ==="
echo ""
echo "Data is now stored in DuckLake with:"
echo "  - Metadata in PostgreSQL (localhost:5433)"
echo "  - Parquet files in MinIO (localhost:9000, bucket: ducklake)"
echo ""
echo "View MinIO console at: http://localhost:9001"
echo "  Username: minioadmin"
echo "  Password: minioadmin"
echo ""
echo "Sample queries to try:"
echo "  SELECT * FROM ducklake.main.products LIMIT 5;"
echo "  SELECT * FROM ducklake.main.orders WHERE status = 'completed';"
echo "  SELECT c.first_name, c.last_name, COUNT(o.id) as order_count"
echo "    FROM ducklake.main.customers c"
echo "    JOIN ducklake.main.orders o ON c.id = o.customer_id"
echo "    GROUP BY c.first_name, c.last_name;"
