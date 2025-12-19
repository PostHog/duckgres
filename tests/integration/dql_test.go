package integration

import (
	"testing"
)

// TestDQLBasicSelect tests basic SELECT statements
func TestDQLBasicSelect(t *testing.T) {
	tests := []QueryTest{
		// Literal values
		{Name: "select_integer", Query: "SELECT 1"},
		{Name: "select_multiple_integers", Query: "SELECT 1, 2, 3"},
		{Name: "select_string", Query: "SELECT 'hello'"},
		{Name: "select_null", Query: "SELECT NULL"},
		{Name: "select_boolean_true", Query: "SELECT TRUE"},
		{Name: "select_boolean_false", Query: "SELECT FALSE"},
		{Name: "select_float", Query: "SELECT 3.14159"},
		{Name: "select_negative", Query: "SELECT -42"},

		// Expressions
		{Name: "select_add", Query: "SELECT 1 + 1"},
		{Name: "select_subtract", Query: "SELECT 10 - 3"},
		{Name: "select_multiply", Query: "SELECT 6 * 7"},
		{Name: "select_divide_integer", Query: "SELECT 10 / 3"},
		{Name: "select_divide_float", Query: "SELECT 10.0 / 3"},
		{Name: "select_modulo", Query: "SELECT 10 % 3"},
		{Name: "select_concat", Query: "SELECT 'hello' || ' ' || 'world'"},
		{Name: "select_complex_expr", Query: "SELECT (1 + 2) * 3 - 4 / 2"},

		// Aliases
		{Name: "select_alias_as", Query: "SELECT 1 AS num"},
		{Name: "select_alias_implicit", Query: "SELECT 1 num"},
		{Name: "select_alias_quoted", Query: "SELECT 1 AS \"Column Name\""},
		{Name: "select_multiple_aliases", Query: "SELECT 1 AS a, 2 AS b, 3 AS c"},

		// FROM clause - basic
		{Name: "select_star", Query: "SELECT * FROM users LIMIT 5"},
		{Name: "select_columns", Query: "SELECT id, name FROM users LIMIT 5"},
		{Name: "select_table_alias", Query: "SELECT u.* FROM users u LIMIT 5"},
		{Name: "select_table_as_alias", Query: "SELECT u.id, u.name FROM users AS u LIMIT 5"},
		{Name: "select_qualified", Query: "SELECT users.id, users.name FROM users LIMIT 5"},

		// Table expressions
		{Name: "select_from_values", Query: "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"},
		{Name: "select_table_command", Query: "TABLE users LIMIT 5", Skip: SkipUnsupportedByDuckDB},
	}
	runQueryTests(t, tests)
}

// TestDQLWhere tests WHERE clause variations
func TestDQLWhere(t *testing.T) {
	tests := []QueryTest{
		// Comparison operators
		{Name: "where_equals", Query: "SELECT * FROM users WHERE id = 1"},
		{Name: "where_not_equals", Query: "SELECT * FROM users WHERE id != 1 LIMIT 5"},
		{Name: "where_not_equals_alt", Query: "SELECT * FROM users WHERE id <> 1 LIMIT 5"},
		{Name: "where_less_than", Query: "SELECT * FROM users WHERE id < 5"},
		{Name: "where_less_equal", Query: "SELECT * FROM users WHERE id <= 5"},
		{Name: "where_greater_than", Query: "SELECT * FROM users WHERE id > 5"},
		{Name: "where_greater_equal", Query: "SELECT * FROM users WHERE id >= 5"},

		// NULL handling
		{Name: "where_is_null", Query: "SELECT * FROM nullable_test WHERE val1 IS NULL"},
		{Name: "where_is_not_null", Query: "SELECT * FROM nullable_test WHERE val1 IS NOT NULL"},
		{Name: "where_is_distinct_from", Query: "SELECT * FROM nullable_test WHERE val1 IS DISTINCT FROM 10"},
		{Name: "where_is_not_distinct_from", Query: "SELECT * FROM nullable_test WHERE val1 IS NOT DISTINCT FROM 10"},

		// Boolean operators
		{Name: "where_and", Query: "SELECT * FROM users WHERE active = true AND age > 25 LIMIT 5"},
		{Name: "where_or", Query: "SELECT * FROM users WHERE age < 25 OR age > 35 LIMIT 5"},
		{Name: "where_not", Query: "SELECT * FROM users WHERE NOT active"},
		{Name: "where_complex_boolean", Query: "SELECT * FROM users WHERE active = true AND (age < 25 OR age > 35) LIMIT 5"},

		// IN operator
		{Name: "where_in_list", Query: "SELECT * FROM users WHERE id IN (1, 2, 3)"},
		{Name: "where_not_in_list", Query: "SELECT * FROM users WHERE id NOT IN (1, 2, 3) LIMIT 5"},
		{Name: "where_in_subquery", Query: "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed')"},

		// BETWEEN
		{Name: "where_between", Query: "SELECT * FROM users WHERE age BETWEEN 25 AND 35"},
		{Name: "where_not_between", Query: "SELECT * FROM users WHERE age NOT BETWEEN 25 AND 35"},

		// LIKE and pattern matching
		{Name: "where_like_prefix", Query: "SELECT * FROM users WHERE name LIKE 'A%'"},
		{Name: "where_like_suffix", Query: "SELECT * FROM users WHERE email LIKE '%@example.com'"},
		{Name: "where_like_contains", Query: "SELECT * FROM users WHERE name LIKE '%a%'"},
		{Name: "where_like_single_char", Query: "SELECT * FROM documents WHERE title LIKE 'D_ckDB%'"},
		{Name: "where_ilike", Query: "SELECT * FROM users WHERE name ILIKE 'ALICE'"},
		{Name: "where_not_like", Query: "SELECT * FROM users WHERE name NOT LIKE 'A%' LIMIT 5"},
		{Name: "where_similar_to", Query: "SELECT * FROM users WHERE name SIMILAR TO '(Alice|Bob)'", Skip: SkipUnsupportedByDuckDB},

		// Regex
		{Name: "where_regex_match", Query: "SELECT * FROM users WHERE name ~ '^A'"},
		{Name: "where_regex_match_case_insensitive", Query: "SELECT * FROM users WHERE name ~* '^a'"},
		{Name: "where_regex_not_match", Query: "SELECT * FROM users WHERE name !~ '^A' LIMIT 5"},

		// ANY/ALL
		{Name: "where_equals_any_array", Query: "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])"},
		{Name: "where_greater_any_array", Query: "SELECT * FROM users WHERE age > ANY(ARRAY[25, 30])"},
		{Name: "where_equals_all_array", Query: "SELECT * FROM users WHERE id = ALL(ARRAY[1])"},
	}
	runQueryTests(t, tests)
}

// TestDQLOrderBy tests ORDER BY clause variations
func TestDQLOrderBy(t *testing.T) {
	tests := []QueryTest{
		{Name: "order_by_single", Query: "SELECT * FROM users ORDER BY name LIMIT 5"},
		{Name: "order_by_asc", Query: "SELECT * FROM users ORDER BY name ASC LIMIT 5"},
		{Name: "order_by_desc", Query: "SELECT * FROM users ORDER BY name DESC LIMIT 5"},
		{Name: "order_by_nulls_first", Query: "SELECT * FROM nullable_test ORDER BY val1 NULLS FIRST"},
		{Name: "order_by_nulls_last", Query: "SELECT * FROM nullable_test ORDER BY val1 NULLS LAST"},
		{Name: "order_by_multiple", Query: "SELECT * FROM users ORDER BY active, name LIMIT 5"},
		{Name: "order_by_mixed", Query: "SELECT * FROM users ORDER BY active DESC, name ASC LIMIT 5"},
		{Name: "order_by_position", Query: "SELECT id, name FROM users ORDER BY 2 LIMIT 5"},
		{Name: "order_by_alias", Query: "SELECT id, name AS n FROM users ORDER BY n LIMIT 5"},
		{Name: "order_by_expression", Query: "SELECT *, age * 2 AS double_age FROM users ORDER BY double_age LIMIT 5"},
	}
	runQueryTests(t, tests)
}

// TestDQLLimitOffset tests LIMIT and OFFSET variations
func TestDQLLimitOffset(t *testing.T) {
	tests := []QueryTest{
		{Name: "limit_only", Query: "SELECT * FROM users ORDER BY id LIMIT 5"},
		{Name: "limit_offset", Query: "SELECT * FROM users ORDER BY id LIMIT 5 OFFSET 3"},
		{Name: "offset_only", Query: "SELECT * FROM users ORDER BY id OFFSET 5"},
		{Name: "limit_all", Query: "SELECT * FROM users ORDER BY id LIMIT ALL"},
		{Name: "fetch_first", Query: "SELECT * FROM users ORDER BY id FETCH FIRST 5 ROWS ONLY"},
		{Name: "offset_fetch", Query: "SELECT * FROM users ORDER BY id OFFSET 3 ROWS FETCH NEXT 5 ROWS ONLY"},
	}
	runQueryTests(t, tests)
}

// TestDQLDistinct tests DISTINCT variations
func TestDQLDistinct(t *testing.T) {
	tests := []QueryTest{
		{Name: "distinct_single", Query: "SELECT DISTINCT active FROM users"},
		{Name: "distinct_multiple", Query: "SELECT DISTINCT active, (age / 10 * 10) AS age_group FROM users"},
		{Name: "distinct_on", Query: "SELECT DISTINCT ON (active) active, name, age FROM users ORDER BY active, age DESC"},
		{Name: "select_all", Query: "SELECT ALL active FROM users"},
	}
	runQueryTests(t, tests)
}

// TestDQLGroupBy tests GROUP BY and HAVING
func TestDQLGroupBy(t *testing.T) {
	tests := []QueryTest{
		// Basic GROUP BY
		{Name: "group_by_count", Query: "SELECT active, COUNT(*) FROM users GROUP BY active"},
		{Name: "group_by_sum", Query: "SELECT active, SUM(age) FROM users GROUP BY active"},
		{Name: "group_by_avg", Query: "SELECT active, AVG(age) FROM users GROUP BY active"},
		{Name: "group_by_min_max", Query: "SELECT active, MIN(age), MAX(age) FROM users GROUP BY active"},
		{Name: "group_by_multiple", Query: "SELECT active, (age / 10 * 10) AS age_group, COUNT(*) FROM users GROUP BY active, age / 10 * 10"},

		// HAVING
		{Name: "having_count", Query: "SELECT category, COUNT(*) AS cnt FROM products GROUP BY category HAVING COUNT(*) > 3"},
		{Name: "having_sum", Query: "SELECT category, SUM(price) AS total FROM products GROUP BY category HAVING SUM(price) > 500"},

		// GROUP BY with expressions
		{Name: "group_by_expression", Query: "SELECT age / 10 * 10 AS decade, COUNT(*) FROM users GROUP BY age / 10 * 10"},
		{Name: "group_by_extract", Query: "SELECT EXTRACT(month FROM order_date) AS month, COUNT(*) FROM orders GROUP BY EXTRACT(month FROM order_date)"},

		// GROUPING SETS, CUBE, ROLLUP
		{Name: "grouping_sets", Query: "SELECT active, (age / 10 * 10) AS decade, COUNT(*) FROM users GROUP BY GROUPING SETS ((active), ((age / 10 * 10)), ())"},
		{Name: "rollup", Query: "SELECT category, name, SUM(stock) FROM products GROUP BY ROLLUP (category, name)"},
		{Name: "cube", Query: "SELECT category, name, SUM(stock) FROM products GROUP BY CUBE (category, name)"},
	}
	runQueryTests(t, tests)
}

// TestDQLJoins tests JOIN variations
func TestDQLJoins(t *testing.T) {
	tests := []QueryTest{
		// INNER JOIN
		{Name: "inner_join_on", Query: "SELECT u.name, o.id AS order_id FROM users u JOIN orders o ON u.id = o.user_id LIMIT 10"},
		{Name: "inner_join_explicit", Query: "SELECT u.name, o.id FROM users u INNER JOIN orders o ON u.id = o.user_id LIMIT 10"},
		{Name: "inner_join_using", Query: "SELECT * FROM orders o JOIN order_items oi USING (order_id)", Skip: "order_id column names don't match"},

		// LEFT JOIN
		{Name: "left_join", Query: "SELECT u.name, o.id AS order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id"},
		{Name: "left_outer_join", Query: "SELECT u.name, o.id FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id"},

		// RIGHT JOIN
		{Name: "right_join", Query: "SELECT u.name, o.id FROM users u RIGHT JOIN orders o ON u.id = o.user_id"},
		{Name: "right_outer_join", Query: "SELECT u.name, o.id FROM users u RIGHT OUTER JOIN orders o ON u.id = o.user_id"},

		// FULL OUTER JOIN
		{Name: "full_join", Query: "SELECT u.name, o.id FROM users u FULL JOIN orders o ON u.id = o.user_id"},
		{Name: "full_outer_join", Query: "SELECT u.name, o.id FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id"},

		// CROSS JOIN
		{Name: "cross_join", Query: "SELECT u.name, p.name AS product FROM users u CROSS JOIN products p LIMIT 20"},
		{Name: "cross_join_implicit", Query: "SELECT u.name, p.name AS product FROM users u, products p LIMIT 20"},

		// Multiple JOINs
		{Name: "multiple_joins", Query: `
			SELECT u.name, p.name AS product, o.quantity
			FROM users u
			JOIN orders o ON u.id = o.user_id
			JOIN products p ON o.product_id = p.id
			LIMIT 10
		`},

		// Self JOIN
		{Name: "self_join", Query: `
			SELECT c1.name AS category, c2.name AS parent
			FROM categories c1
			LEFT JOIN categories c2 ON c1.parent_id = c2.id
			LIMIT 10
		`},

		// LATERAL JOIN
		{Name: "lateral_join", Query: `
			SELECT u.name, latest.order_date
			FROM users u
			CROSS JOIN LATERAL (
				SELECT order_date FROM orders WHERE user_id = u.id ORDER BY order_date DESC LIMIT 1
			) latest
		`},
		{Name: "lateral_left_join", Query: `
			SELECT u.name, latest.order_date
			FROM users u
			LEFT JOIN LATERAL (
				SELECT order_date FROM orders WHERE user_id = u.id ORDER BY order_date DESC LIMIT 1
			) latest ON true
		`},

		// NATURAL JOIN
		{Name: "natural_join", Query: "SELECT * FROM orders NATURAL JOIN order_items LIMIT 5", Skip: "column name mismatch"},
	}
	runQueryTests(t, tests)
}

// TestDQLSubqueries tests subquery variations
func TestDQLSubqueries(t *testing.T) {
	tests := []QueryTest{
		// Scalar subquery
		{Name: "scalar_subquery_select", Query: "SELECT *, (SELECT MAX(age) FROM users) AS max_age FROM users LIMIT 5"},
		{Name: "scalar_subquery_where", Query: "SELECT * FROM users WHERE age = (SELECT MAX(age) FROM users)"},

		// Table subquery
		{Name: "table_subquery", Query: "SELECT * FROM (SELECT * FROM users WHERE active = true) sub LIMIT 5"},
		{Name: "table_subquery_aggregation", Query: "SELECT * FROM (SELECT active, COUNT(*) AS cnt FROM users GROUP BY active) sub WHERE cnt > 3"},

		// Correlated subquery
		{Name: "correlated_subquery", Query: `
			SELECT * FROM users u
			WHERE age > (SELECT AVG(age) FROM users WHERE active = u.active)
		`},

		// EXISTS
		{Name: "exists_subquery", Query: "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)"},
		{Name: "not_exists_subquery", Query: "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)"},

		// IN with subquery
		{Name: "in_subquery", Query: "SELECT * FROM products WHERE id IN (SELECT product_id FROM orders)"},
		{Name: "not_in_subquery", Query: "SELECT * FROM products WHERE id NOT IN (SELECT product_id FROM orders WHERE product_id IS NOT NULL)"},
	}
	runQueryTests(t, tests)
}

// TestDQLCTEs tests Common Table Expressions
func TestDQLCTEs(t *testing.T) {
	tests := []QueryTest{
		// Simple CTE
		{Name: "cte_simple", Query: `
			WITH active_users AS (
				SELECT * FROM users WHERE active = true
			)
			SELECT * FROM active_users LIMIT 5
		`},

		// Multiple CTEs
		{Name: "cte_multiple", Query: `
			WITH
				active_users AS (SELECT * FROM users WHERE active = true),
				recent_orders AS (SELECT * FROM orders WHERE order_date > '2024-01-20')
			SELECT u.name, COUNT(o.id) AS order_count
			FROM active_users u
			LEFT JOIN recent_orders o ON u.id = o.user_id
			GROUP BY u.name
		`},

		// CTE referencing another CTE
		{Name: "cte_chained", Query: `
			WITH
				raw AS (SELECT * FROM users),
				filtered AS (SELECT * FROM raw WHERE active = true),
				counted AS (SELECT COUNT(*) AS cnt FROM filtered)
			SELECT * FROM counted
		`},

		// Recursive CTE
		{Name: "cte_recursive", Query: `
			WITH RECURSIVE tree AS (
				SELECT id, name, parent_id, level
				FROM categories WHERE parent_id IS NULL
				UNION ALL
				SELECT c.id, c.name, c.parent_id, c.level
				FROM categories c
				JOIN tree t ON c.parent_id = t.id
			)
			SELECT * FROM tree ORDER BY id
		`},

		// CTE with column aliases
		{Name: "cte_column_aliases", Query: `
			WITH cte(a, b, c) AS (SELECT 1, 2, 3)
			SELECT * FROM cte
		`},

		// CTE with aggregation
		{Name: "cte_with_aggregation", Query: `
			WITH category_totals AS (
				SELECT category, SUM(price) AS total_price, COUNT(*) AS product_count
				FROM products
				GROUP BY category
			)
			SELECT * FROM category_totals ORDER BY total_price DESC
		`},
	}
	runQueryTests(t, tests)
}

// TestDQLSetOperations tests UNION, INTERSECT, EXCEPT
func TestDQLSetOperations(t *testing.T) {
	tests := []QueryTest{
		// UNION
		{Name: "union", Query: "SELECT name FROM users WHERE active = true UNION SELECT name FROM users WHERE age > 30"},
		{Name: "union_all", Query: "SELECT name FROM users WHERE active = true UNION ALL SELECT name FROM users WHERE age > 30"},
		{Name: "union_distinct", Query: "SELECT name FROM users WHERE active = true UNION DISTINCT SELECT name FROM users WHERE age > 30"},

		// INTERSECT
		{Name: "intersect", Query: "SELECT name FROM users WHERE active = true INTERSECT SELECT name FROM users WHERE age > 25"},
		{Name: "intersect_all", Query: "SELECT name FROM users WHERE active = true INTERSECT ALL SELECT name FROM users WHERE age > 25"},

		// EXCEPT
		{Name: "except", Query: "SELECT name FROM users WHERE active = true EXCEPT SELECT name FROM users WHERE age > 35"},
		{Name: "except_all", Query: "SELECT name FROM users WHERE active = true EXCEPT ALL SELECT name FROM users WHERE age > 35"},

		// Multiple set operations
		{Name: "multiple_union", Query: `
			SELECT category FROM products WHERE price < 50
			UNION
			SELECT category FROM products WHERE price BETWEEN 50 AND 200
			UNION
			SELECT category FROM products WHERE price > 200
		`},
		{Name: "mixed_set_ops", Query: `
			(SELECT name FROM users WHERE active = true
			 UNION
			 SELECT name FROM users WHERE age < 30)
			INTERSECT
			SELECT name FROM users WHERE score > 80
		`},
	}
	runQueryTests(t, tests)
}

// TestDQLWindowFunctions tests window functions
func TestDQLWindowFunctions(t *testing.T) {
	tests := []QueryTest{
		// ROW_NUMBER, RANK, DENSE_RANK
		{Name: "row_number", Query: "SELECT *, ROW_NUMBER() OVER (ORDER BY age) FROM users"},
		{Name: "rank", Query: "SELECT *, RANK() OVER (ORDER BY score DESC) FROM users"},
		{Name: "dense_rank", Query: "SELECT *, DENSE_RANK() OVER (ORDER BY score DESC) FROM users"},
		{Name: "ntile", Query: "SELECT *, NTILE(4) OVER (ORDER BY score DESC) FROM users"},

		// Partitioned windows
		{Name: "row_number_partitioned", Query: "SELECT *, ROW_NUMBER() OVER (PARTITION BY active ORDER BY score DESC) FROM users"},
		{Name: "rank_partitioned", Query: "SELECT *, RANK() OVER (PARTITION BY active ORDER BY score DESC) FROM users"},

		// Aggregate window functions
		{Name: "sum_window", Query: "SELECT *, SUM(amount) OVER (ORDER BY sale_date) AS running_sum FROM sales"},
		{Name: "avg_window", Query: "SELECT *, AVG(amount) OVER (ORDER BY sale_date) AS running_avg FROM sales"},
		{Name: "count_partitioned", Query: "SELECT *, COUNT(*) OVER (PARTITION BY region) AS region_count FROM sales"},

		// Window frame specification
		{Name: "window_rows_unbounded", Query: "SELECT *, SUM(amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales"},
		{Name: "window_rows_range", Query: "SELECT *, AVG(amount) OVER (ORDER BY sale_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM sales"},
		{Name: "window_rows_all", Query: "SELECT *, SUM(amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM sales"},

		// LAG, LEAD
		{Name: "lag", Query: "SELECT *, LAG(amount) OVER (ORDER BY sale_date) AS prev_amount FROM sales"},
		{Name: "lag_offset", Query: "SELECT *, LAG(amount, 2) OVER (ORDER BY sale_date) AS prev_2_amount FROM sales"},
		{Name: "lag_default", Query: "SELECT *, LAG(amount, 1, 0) OVER (ORDER BY sale_date) AS prev_amount FROM sales"},
		{Name: "lead", Query: "SELECT *, LEAD(amount) OVER (ORDER BY sale_date) AS next_amount FROM sales"},

		// FIRST_VALUE, LAST_VALUE, NTH_VALUE
		{Name: "first_value", Query: "SELECT *, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY sale_date) FROM sales"},
		{Name: "last_value", Query: "SELECT *, LAST_VALUE(amount) OVER (PARTITION BY region ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM sales"},
		{Name: "nth_value", Query: "SELECT *, NTH_VALUE(amount, 2) OVER (ORDER BY sale_date) FROM sales"},

		// Named windows
		{Name: "named_window", Query: `
			SELECT *,
				SUM(amount) OVER w AS sum,
				AVG(amount) OVER w AS avg,
				COUNT(*) OVER w AS cnt
			FROM sales
			WINDOW w AS (PARTITION BY region ORDER BY sale_date)
		`},

		// PERCENT_RANK, CUME_DIST
		{Name: "percent_rank", Query: "SELECT *, PERCENT_RANK() OVER (ORDER BY score) FROM users"},
		{Name: "cume_dist", Query: "SELECT *, CUME_DIST() OVER (ORDER BY score) FROM users"},
	}
	runQueryTests(t, tests)
}

// TestDQLValues tests VALUES clause
func TestDQLValues(t *testing.T) {
	tests := []QueryTest{
		{Name: "values_basic", Query: "VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{Name: "values_as_table", Query: "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"},
		{Name: "values_in_cte", Query: "WITH data(id, val) AS (VALUES (1, 10), (2, 20)) SELECT * FROM data"},
	}
	runQueryTests(t, tests)
}

// TestDQLCaseSensitivity tests case sensitivity
func TestDQLCaseSensitivity(t *testing.T) {
	tests := []QueryTest{
		{Name: "keyword_case", Query: "SELECT id, name FROM users WHERE active = TRUE ORDER BY id LIMIT 5"},
		{Name: "keyword_mixed_case", Query: "SeLeCt Id, NaMe FrOm UsErS WhErE AcTiVe = TrUe OrDeR By Id LiMiT 5"},
		{Name: "identifier_case", Query: "SELECT ID, NAME FROM USERS WHERE ACTIVE = true ORDER BY ID LIMIT 5"},
	}
	runQueryTests(t, tests)
}

// TestDQLComplexQueries tests complex real-world queries
func TestDQLComplexQueries(t *testing.T) {
	opts := DefaultCompareOptions()
	opts.IgnoreRowOrder = true

	tests := []QueryTest{
		{
			Name: "analytics_query",
			Query: `
				WITH monthly_sales AS (
					SELECT
						EXTRACT(month FROM sale_date) AS month,
						region,
						SUM(amount) AS total_sales
					FROM sales
					GROUP BY EXTRACT(month FROM sale_date), region
				)
				SELECT
					month,
					region,
					total_sales,
					SUM(total_sales) OVER (PARTITION BY region ORDER BY month) AS cumulative_sales,
					RANK() OVER (PARTITION BY month ORDER BY total_sales DESC) AS rank_in_month
				FROM monthly_sales
				ORDER BY month, region
			`,
		},
		{
			Name: "multi_join_aggregation",
			Query: `
				SELECT
					u.name AS customer,
					COUNT(DISTINCT o.id) AS order_count,
					SUM(o.total_amount) AS total_spent,
					AVG(o.total_amount) AS avg_order_value,
					STRING_AGG(DISTINCT o.status, ', ' ORDER BY o.status) AS statuses
				FROM users u
				LEFT JOIN orders o ON u.id = o.user_id
				GROUP BY u.id, u.name
				HAVING COUNT(o.id) > 0
				ORDER BY total_spent DESC
			`,
			Options: opts,
		},
		{
			Name: "hierarchical_query",
			Query: `
				WITH RECURSIVE category_path AS (
					SELECT id, name, parent_id, name::TEXT AS path, 1 AS depth
					FROM categories WHERE parent_id IS NULL
					UNION ALL
					SELECT c.id, c.name, c.parent_id, cp.path || ' > ' || c.name, cp.depth + 1
					FROM categories c
					JOIN category_path cp ON c.parent_id = cp.id
				)
				SELECT * FROM category_path ORDER BY path
			`,
		},
		{
			Name: "correlated_update_style",
			Query: `
				SELECT u.*,
					(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count,
					(SELECT SUM(total_amount) FROM orders o WHERE o.user_id = u.id) AS total_spent,
					(SELECT MAX(order_date) FROM orders o WHERE o.user_id = u.id) AS last_order
				FROM users u
				ORDER BY u.id
			`,
		},
	}
	runQueryTests(t, tests)
}
