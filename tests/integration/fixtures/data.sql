-- Test Data for Duckgres PostgreSQL Compatibility Tests

-- Types test data (without JSON column which was removed)
INSERT INTO types_test VALUES
(1, true, 1, 100, 1000000000, 1.5, 2.5, 123.4567, 'hello', 'world', 'test      ',
 '2024-01-15', '12:30:45', '2024-01-15 12:30:45', '2024-01-15 12:30:45+00',
 '1 day 2 hours', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\x48656c6c6f'),
(2, false, -1, -100, -1000000000, -1.5, -2.5, -123.4567, '', '', '          ',
 '2020-12-31', '23:59:59', '2020-12-31 23:59:59', '2020-12-31 23:59:59+00',
 '1 year 2 months', 'b1ffcd00-0d1c-5fa9-cc7e-7cc0ce491b22', '\x00'),
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Users data
INSERT INTO users (id, name, email, age, active, created_at, score) VALUES
(1, 'Alice', 'alice@example.com', 30, true, '2024-01-01 10:00:00', 95.50),
(2, 'Bob', 'bob@example.com', 25, true, '2024-01-02 11:00:00', 82.25),
(3, 'Charlie', 'charlie@example.com', 35, false, '2024-01-03 12:00:00', 78.00),
(4, 'Diana', 'diana@example.com', 28, true, '2024-01-04 13:00:00', 91.75),
(5, 'Eve', 'eve@example.com', 32, true, '2024-01-05 14:00:00', 88.50),
(6, 'Frank', 'frank@example.com', 45, false, '2024-01-06 15:00:00', 65.00),
(7, 'Grace', 'grace@example.com', 22, true, '2024-01-07 16:00:00', 99.00),
(8, 'Henry', 'henry@example.com', 38, true, '2024-01-08 17:00:00', 72.50),
(9, 'Ivy', 'ivy@example.com', 27, false, '2024-01-09 18:00:00', 85.25),
(10, 'Jack', 'jack@example.com', 33, true, '2024-01-10 19:00:00', 90.00);

-- Products data
INSERT INTO products (id, name, category, price, stock, created_at, tags) VALUES
(1, 'Laptop', 'Electronics', 999.99, 50, '2024-01-01 00:00:00', ARRAY['computer', 'portable']),
(2, 'Mouse', 'Electronics', 29.99, 200, '2024-01-02 00:00:00', ARRAY['accessory', 'input']),
(3, 'Keyboard', 'Electronics', 79.99, 150, '2024-01-03 00:00:00', ARRAY['accessory', 'input']),
(4, 'Monitor', 'Electronics', 299.99, 75, '2024-01-04 00:00:00', ARRAY['display', 'computer']),
(5, 'Desk', 'Furniture', 199.99, 30, '2024-01-05 00:00:00', ARRAY['office', 'workspace']),
(6, 'Chair', 'Furniture', 149.99, 45, '2024-01-06 00:00:00', ARRAY['office', 'seating']),
(7, 'Notebook', 'Office', 9.99, 500, '2024-01-07 00:00:00', ARRAY['paper', 'writing']),
(8, 'Pen Set', 'Office', 14.99, 300, '2024-01-08 00:00:00', ARRAY['writing', 'set']),
(9, 'Headphones', 'Electronics', 149.99, 100, '2024-01-09 00:00:00', ARRAY['audio', 'accessory']),
(10, 'Webcam', 'Electronics', 89.99, 80, '2024-01-10 00:00:00', ARRAY['video', 'accessory']);

-- Orders data
INSERT INTO orders (id, user_id, product_id, quantity, total_amount, status, order_date) VALUES
(1, 1, 1, 1, 999.99, 'completed', '2024-01-15 10:00:00'),
(2, 1, 2, 2, 59.98, 'completed', '2024-01-15 10:05:00'),
(3, 2, 3, 1, 79.99, 'pending', '2024-01-16 11:00:00'),
(4, 2, 4, 1, 299.99, 'shipped', '2024-01-16 11:30:00'),
(5, 3, 5, 2, 399.98, 'completed', '2024-01-17 12:00:00'),
(6, 4, 6, 1, 149.99, 'cancelled', '2024-01-18 13:00:00'),
(7, 5, 7, 10, 99.90, 'completed', '2024-01-19 14:00:00'),
(8, 5, 8, 5, 74.95, 'completed', '2024-01-19 14:30:00'),
(9, 6, 9, 1, 149.99, 'pending', '2024-01-20 15:00:00'),
(10, 7, 10, 2, 179.98, 'shipped', '2024-01-21 16:00:00'),
(11, 8, 1, 1, 999.99, 'completed', '2024-01-22 17:00:00'),
(12, 9, 2, 3, 89.97, 'completed', '2024-01-23 18:00:00'),
(13, 10, 3, 2, 159.98, 'pending', '2024-01-24 19:00:00'),
(14, 1, 4, 1, 299.99, 'shipped', '2024-01-25 10:00:00'),
(15, 2, 5, 1, 199.99, 'completed', '2024-01-26 11:00:00');

-- Order items data
INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1, 999.99),
(2, 2, 2, 2, 29.99),
(3, 3, 3, 1, 79.99),
(4, 4, 4, 1, 299.99),
(5, 5, 5, 2, 199.99),
(6, 6, 6, 1, 149.99),
(7, 7, 7, 10, 9.99),
(8, 8, 8, 5, 14.99),
(9, 9, 9, 1, 149.99),
(10, 10, 10, 2, 89.99);

-- Categories (hierarchical data for recursive CTEs)
INSERT INTO categories (id, name, parent_id, level) VALUES
(1, 'All Products', NULL, 0),
(2, 'Electronics', 1, 1),
(3, 'Furniture', 1, 1),
(4, 'Office Supplies', 1, 1),
(5, 'Computers', 2, 2),
(6, 'Accessories', 2, 2),
(7, 'Audio', 2, 2),
(8, 'Desks', 3, 2),
(9, 'Chairs', 3, 2),
(10, 'Paper Products', 4, 2),
(11, 'Writing Tools', 4, 2),
(12, 'Laptops', 5, 3),
(13, 'Monitors', 5, 3),
(14, 'Mice', 6, 3),
(15, 'Keyboards', 6, 3);

-- Sales data (time series)
INSERT INTO sales (id, product_id, sale_date, amount, region) VALUES
(1, 1, '2024-01-01', 2999.97, 'North'),
(2, 2, '2024-01-01', 149.95, 'South'),
(3, 1, '2024-01-02', 1999.98, 'East'),
(4, 3, '2024-01-02', 239.97, 'West'),
(5, 1, '2024-01-03', 999.99, 'North'),
(6, 2, '2024-01-03', 89.97, 'South'),
(7, 4, '2024-01-04', 599.98, 'East'),
(8, 5, '2024-01-04', 399.98, 'West'),
(9, 1, '2024-01-05', 3999.96, 'North'),
(10, 3, '2024-01-05', 159.98, 'South'),
(11, 2, '2024-01-06', 59.98, 'East'),
(12, 6, '2024-01-06', 449.97, 'West'),
(13, 1, '2024-01-07', 999.99, 'North'),
(14, 4, '2024-01-07', 899.97, 'South'),
(15, 5, '2024-01-08', 199.99, 'East'),
(16, 7, '2024-01-08', 99.90, 'West'),
(17, 1, '2024-01-09', 1999.98, 'North'),
(18, 8, '2024-01-09', 44.97, 'South'),
(19, 9, '2024-01-10', 299.98, 'East'),
(20, 10, '2024-01-10', 269.97, 'West');

-- Events data
INSERT INTO events (id, event_name, event_date, event_time, event_timestamp, event_timestamptz, duration) VALUES
(1, 'Conference', '2024-06-15', '09:00:00', '2024-06-15 09:00:00', '2024-06-15 09:00:00+00', '8 hours'),
(2, 'Meeting', '2024-06-16', '14:30:00', '2024-06-16 14:30:00', '2024-06-16 14:30:00+00', '1 hour 30 minutes'),
(3, 'Workshop', '2024-06-17', '10:00:00', '2024-06-17 10:00:00', '2024-06-17 10:00:00+00', '4 hours'),
(4, 'Webinar', '2024-06-18', '16:00:00', '2024-06-18 16:00:00', '2024-06-18 16:00:00+00', '2 hours'),
(5, 'Hackathon', '2024-06-20', '08:00:00', '2024-06-20 08:00:00', '2024-06-20 08:00:00+00', '48 hours');

-- JSON data
INSERT INTO json_data (id, data, attributes) VALUES
(1, '{"name": "item1", "value": 100, "tags": ["a", "b"]}', '{"color": "red", "size": "large"}'),
(2, '{"name": "item2", "value": 200, "tags": ["c"]}', '{"color": "blue", "size": "medium"}'),
(3, '{"name": "item3", "value": 300, "nested": {"inner": "data"}}', '{"color": "green", "size": "small"}'),
(4, '[1, 2, 3, 4, 5]', '{}'),
(5, '"simple string"', 'null');

-- Nullable test data
INSERT INTO nullable_test (id, val1, val2, val3, val4) VALUES
(1, 10, 'ten', true, 10.00),
(2, NULL, 'null val1', true, 20.00),
(3, 30, NULL, false, 30.00),
(4, 40, 'forty', NULL, 40.00),
(5, 50, 'fifty', true, NULL),
(6, NULL, NULL, NULL, NULL),
(7, 70, 'seventy', true, 70.00);

-- Metrics data
INSERT INTO metrics (id, category, metric_name, value, recorded_at) VALUES
(1, 'performance', 'cpu_usage', 45.5, '2024-01-15 10:00:00'),
(2, 'performance', 'memory_usage', 62.3, '2024-01-15 10:00:00'),
(3, 'performance', 'cpu_usage', 48.2, '2024-01-15 11:00:00'),
(4, 'performance', 'memory_usage', 65.1, '2024-01-15 11:00:00'),
(5, 'traffic', 'requests_per_sec', 1250.0, '2024-01-15 10:00:00'),
(6, 'traffic', 'requests_per_sec', 1380.0, '2024-01-15 11:00:00'),
(7, 'errors', 'error_rate', 0.5, '2024-01-15 10:00:00'),
(8, 'errors', 'error_rate', 0.3, '2024-01-15 11:00:00'),
(9, 'performance', 'cpu_usage', 52.1, '2024-01-15 12:00:00'),
(10, 'traffic', 'requests_per_sec', 1420.0, '2024-01-15 12:00:00');

-- Documents data
INSERT INTO documents (id, title, body, author) VALUES
(1, 'Introduction to SQL', 'SQL is a powerful query language for databases.', 'Alice'),
(2, 'Advanced Queries', 'Learn about JOINs, CTEs, and window functions.', 'Bob'),
(3, 'Database Design', 'Good database design is essential for performance.', 'Charlie'),
(4, 'PostgreSQL Tips', 'PostgreSQL offers many advanced features.', 'Alice'),
(5, 'DuckDB Overview', 'DuckDB is an in-process analytical database.', 'Diana');

-- Array test data
INSERT INTO array_test (id, int_array, text_array, nested_array) VALUES
(1, ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'], ARRAY[[1, 2], [3, 4]]),
(2, ARRAY[4, 5, 6, 7], ARRAY['hello', 'world'], ARRAY[[5, 6], [7, 8]]),
(3, ARRAY[]::INTEGER[], ARRAY[]::TEXT[], NULL),
(4, NULL, ARRAY['single'], ARRAY[[9]]);

-- Schema test data
INSERT INTO test_schema.schema_test (id, value) VALUES
(1, 'schema value 1'),
(2, 'schema value 2');
