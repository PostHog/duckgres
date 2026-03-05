#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PGWIRE_DSN="${DUCKGRES_PERF_PGWIRE_DSN:-}"
DATASET_VERSION="${DUCKGRES_PERF_DATASET_VERSION:-}"
MANIFEST_TABLE="${DUCKGRES_PERF_DATASET_MANIFEST_TABLE:-ducklake.main.dataset_manifest}"
GENERATOR_VERSION="${DUCKGRES_PERF_GENERATOR_VERSION:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
PERF_PASSWORD="${DUCKGRES_PERF_PASSWORD:-perfpass}"
ALLOW_OVERWRITE="${DUCKGRES_PERF_ALLOW_OVERWRITE:-0}"

CATEGORY_COUNT="${DUCKGRES_PERF_CATEGORIES_COUNT:-10}"
PRODUCT_COUNT="${DUCKGRES_PERF_PRODUCTS_COUNT:-10000}"
CUSTOMER_COUNT="${DUCKGRES_PERF_CUSTOMERS_COUNT:-100000}"
ORDER_COUNT="${DUCKGRES_PERF_ORDERS_COUNT:-1000000}"
ITEMS_PER_ORDER="${DUCKGRES_PERF_ITEMS_PER_ORDER:-3}"
EVENT_COUNT="${DUCKGRES_PERF_EVENTS_COUNT:-2000000}"
PAGE_VIEW_COUNT="${DUCKGRES_PERF_PAGE_VIEWS_COUNT:-2000000}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command missing: $1"
    exit 1
  fi
}

require_count() {
  if [[ ! "$1" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid positive integer: $1"
    exit 1
  fi
}

run_sql() {
  local sql="$1"
  PGPASSWORD="$PERF_PASSWORD" psql "$PGWIRE_DSN" -v ON_ERROR_STOP=1 -qc "$sql"
}

query_scalar() {
  local sql="$1"
  PGPASSWORD="$PERF_PASSWORD" psql "$PGWIRE_DSN" -v ON_ERROR_STOP=1 -Atqc "$sql" | tr -d '[:space:]'
}

if [[ -z "$PGWIRE_DSN" ]]; then
  echo "DUCKGRES_PERF_PGWIRE_DSN is required"
  exit 1
fi
if [[ -z "$DATASET_VERSION" ]]; then
  echo "DUCKGRES_PERF_DATASET_VERSION is required"
  exit 1
fi
require_cmd psql

for count in \
  "$CATEGORY_COUNT" "$PRODUCT_COUNT" "$CUSTOMER_COUNT" "$ORDER_COUNT" \
  "$ITEMS_PER_ORDER" "$EVENT_COUNT" "$PAGE_VIEW_COUNT"; do
  require_count "$count"
done

escaped_dataset_version="${DATASET_VERSION//\'/\'\'}"
escaped_generator_version="${GENERATOR_VERSION//\'/\'\'}"

cat <<CONFIG
Bootstrapping DuckLake frozen dataset
  dataset_version:    $DATASET_VERSION
  generator_version:  $GENERATOR_VERSION
  manifest_table:     $MANIFEST_TABLE
  categories:         $CATEGORY_COUNT
  products:           $PRODUCT_COUNT
  customers:          $CUSTOMER_COUNT
  orders:             $ORDER_COUNT
  items/order:        $ITEMS_PER_ORDER
  events:             $EVENT_COUNT
  page_views:         $PAGE_VIEW_COUNT
CONFIG

run_sql "CREATE TABLE IF NOT EXISTS ${MANIFEST_TABLE} (dataset_version VARCHAR, created_at TIMESTAMP, generator_version VARCHAR, categories_rows BIGINT, products_rows BIGINT, customers_rows BIGINT, orders_rows BIGINT, order_items_rows BIGINT, events_rows BIGINT, page_views_rows BIGINT);"

existing_version_count="$(query_scalar "SELECT COUNT(*) FROM ${MANIFEST_TABLE} WHERE dataset_version = '${escaped_dataset_version}';")"
if [[ "$existing_version_count" =~ ^[0-9]+$ ]] && [[ "$existing_version_count" -gt 0 ]] && [[ "$ALLOW_OVERWRITE" != "1" ]]; then
  echo "dataset_version=$DATASET_VERSION already exists in ${MANIFEST_TABLE}; set DUCKGRES_PERF_ALLOW_OVERWRITE=1 to replace"
  exit 1
fi

run_sql "
CREATE TABLE IF NOT EXISTS ducklake.main.categories (
  id INTEGER,
  name VARCHAR,
  description VARCHAR,
  created_at TIMESTAMP
);
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
CREATE TABLE IF NOT EXISTS ducklake.main.orders (
  id INTEGER,
  customer_id INTEGER,
  order_date TIMESTAMP,
  status VARCHAR,
  total_amount DOUBLE,
  shipping_address VARCHAR,
  notes VARCHAR
);
CREATE TABLE IF NOT EXISTS ducklake.main.order_items (
  id INTEGER,
  order_id INTEGER,
  product_id INTEGER,
  quantity INTEGER,
  unit_price DOUBLE,
  discount_percent DOUBLE
);
CREATE TABLE IF NOT EXISTS ducklake.main.events (
  id INTEGER,
  event_name VARCHAR,
  distinct_id VARCHAR,
  properties VARCHAR,
  event_timestamp TIMESTAMP,
  session_id VARCHAR,
  page_url VARCHAR
);
CREATE TABLE IF NOT EXISTS ducklake.main.page_views (
  id INTEGER,
  visitor_id VARCHAR,
  page_path VARCHAR,
  referrer VARCHAR,
  user_agent VARCHAR,
  duration_seconds INTEGER,
  view_timestamp TIMESTAMP
);
"

# The frozen snapshot is regenerated deterministically before publishing manifest metadata.
run_sql "
DELETE FROM ducklake.main.order_items;
DELETE FROM ducklake.main.orders;
DELETE FROM ducklake.main.products;
DELETE FROM ducklake.main.customers;
DELETE FROM ducklake.main.categories;
DELETE FROM ducklake.main.events;
DELETE FROM ducklake.main.page_views;
"

run_sql "
INSERT INTO ducklake.main.categories (id, name, description, created_at)
SELECT i, 'Category ' || i, 'Synthetic category ' || i, TIMESTAMP '2024-01-01 00:00:00' + ((i - 1) * INTERVAL '1 day')
FROM range(1, ${CATEGORY_COUNT} + 1) AS t(i);
"

run_sql "
INSERT INTO ducklake.main.products (id, name, description, category_id, price, stock_quantity, sku, is_active, created_at, updated_at)
SELECT
  i,
  'Product ' || i,
  'Synthetic product ' || i,
  ((i - 1) % ${CATEGORY_COUNT}) + 1,
  ROUND(5 + ((i * 17) % 20000) / 100.0, 2),
  5 + (i % 500),
  'SKU-' || LPAD(CAST(i AS VARCHAR), 8, '0'),
  TRUE,
  TIMESTAMP '2024-01-01 00:00:00' + ((i % 90) * INTERVAL '1 day'),
  TIMESTAMP '2024-01-01 00:00:00' + ((i % 120) * INTERVAL '1 day')
FROM range(1, ${PRODUCT_COUNT} + 1) AS t(i);
"

run_sql "
INSERT INTO ducklake.main.customers (id, email, first_name, last_name, company, phone, country, city, created_at, last_login)
SELECT
  i,
  'customer' || i || '@example.com',
  'First' || i,
  'Last' || i,
  'Company ' || ((i % 2000) + 1),
  '+1-555-' || LPAD(CAST((i % 10000) AS VARCHAR), 4, '0'),
  CASE (i % 5)
    WHEN 0 THEN 'US'
    WHEN 1 THEN 'CA'
    WHEN 2 THEN 'GB'
    WHEN 3 THEN 'DE'
    ELSE 'AU'
  END,
  CASE (i % 6)
    WHEN 0 THEN 'San Francisco'
    WHEN 1 THEN 'New York'
    WHEN 2 THEN 'Toronto'
    WHEN 3 THEN 'London'
    WHEN 4 THEN 'Berlin'
    ELSE 'Sydney'
  END,
  TIMESTAMP '2024-01-01 00:00:00' + ((i % 180) * INTERVAL '1 day'),
  TIMESTAMP '2024-07-01 00:00:00' + ((i % 180) * INTERVAL '1 day')
FROM range(1, ${CUSTOMER_COUNT} + 1) AS t(i);
"

run_sql "
INSERT INTO ducklake.main.orders (id, customer_id, order_date, status, total_amount, shipping_address, notes)
SELECT
  i,
  ((i - 1) % ${CUSTOMER_COUNT}) + 1,
  TIMESTAMP '2025-01-01 00:00:00' + ((i % 365) * INTERVAL '1 day'),
  CASE (i % 4)
    WHEN 0 THEN 'completed'
    WHEN 1 THEN 'pending'
    WHEN 2 THEN 'processing'
    ELSE 'cancelled'
  END,
  ROUND(25 + ((i * 13) % 50000) / 10.0, 2),
  'Address ' || i,
  CASE WHEN (i % 10) = 0 THEN NULL ELSE 'Order note ' || i END
FROM range(1, ${ORDER_COUNT} + 1) AS t(i);
"

run_sql "
INSERT INTO ducklake.main.order_items (id, order_id, product_id, quantity, unit_price, discount_percent)
SELECT
  ((o.i - 1) * ${ITEMS_PER_ORDER}) + li.j,
  o.i,
  (((o.i * 31) + li.j - 1) % ${PRODUCT_COUNT}) + 1,
  ((li.j - 1) % 4) + 1,
  ROUND(5 + (((o.i + li.j) * 19) % 25000) / 100.0, 2),
  CAST(((o.i + li.j) % 20) AS DOUBLE)
FROM range(1, ${ORDER_COUNT} + 1) AS o(i)
CROSS JOIN range(1, ${ITEMS_PER_ORDER} + 1) AS li(j);
"

run_sql "
INSERT INTO ducklake.main.events (id, event_name, distinct_id, properties, event_timestamp, session_id, page_url)
SELECT
  i,
  CASE (i % 5)
    WHEN 0 THEN 'pageview'
    WHEN 1 THEN 'product_view'
    WHEN 2 THEN 'add_to_cart'
    WHEN 3 THEN 'checkout_started'
    ELSE 'purchase'
  END,
  'cust-' || (((i - 1) % ${CUSTOMER_COUNT}) + 1),
  'source=frozen-bootstrap',
  TIMESTAMP '2025-01-01 00:00:00' + ((i % 86400) * INTERVAL '1 second'),
  'sess-' || (((i - 1) % 500000) + 1),
  '/category/' || (((i - 1) % ${CATEGORY_COUNT}) + 1)
FROM range(1, ${EVENT_COUNT} + 1) AS t(i);
"

run_sql "
INSERT INTO ducklake.main.page_views (id, visitor_id, page_path, referrer, user_agent, duration_seconds, view_timestamp)
SELECT
  i,
  'visitor-' || (((i - 1) % 200000) + 1),
  '/product/' || (((i - 1) % ${PRODUCT_COUNT}) + 1),
  CASE WHEN (i % 3) = 0 THEN 'https://search.example.com' ELSE 'https://app.example.com' END,
  CASE WHEN (i % 2) = 0 THEN 'Mozilla/5.0' ELSE 'DuckgresPerf/1.0' END,
  5 + (i % 600),
  TIMESTAMP '2025-01-01 00:00:00' + ((i % 86400) * INTERVAL '1 second')
FROM range(1, ${PAGE_VIEW_COUNT} + 1) AS t(i);
"

run_sql "DELETE FROM ${MANIFEST_TABLE} WHERE dataset_version = '${escaped_dataset_version}';"
run_sql "
INSERT INTO ${MANIFEST_TABLE} (
  dataset_version,
  created_at,
  generator_version,
  categories_rows,
  products_rows,
  customers_rows,
  orders_rows,
  order_items_rows,
  events_rows,
  page_views_rows
)
SELECT
  '${escaped_dataset_version}',
  CURRENT_TIMESTAMP,
  '${escaped_generator_version}',
  (SELECT COUNT(*) FROM ducklake.main.categories),
  (SELECT COUNT(*) FROM ducklake.main.products),
  (SELECT COUNT(*) FROM ducklake.main.customers),
  (SELECT COUNT(*) FROM ducklake.main.orders),
  (SELECT COUNT(*) FROM ducklake.main.order_items),
  (SELECT COUNT(*) FROM ducklake.main.events),
  (SELECT COUNT(*) FROM ducklake.main.page_views);
"

echo "Published manifest row:"
run_sql "SELECT dataset_version, created_at, generator_version, orders_rows, order_items_rows FROM ${MANIFEST_TABLE} WHERE dataset_version = '${escaped_dataset_version}';"

echo "Frozen dataset bootstrap complete for dataset_version=$DATASET_VERSION"
