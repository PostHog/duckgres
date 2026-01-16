package integration

import (
	"testing"
)

// TestFunctionsString tests string functions
func TestFunctionsString(t *testing.T) {
	tests := []QueryTest{
		// Length functions
		{Name: "length", Query: "SELECT length('hello')"},
		{Name: "char_length", Query: "SELECT char_length('hello')"},
		{Name: "character_length", Query: "SELECT character_length('hello')"},
		{Name: "octet_length", Query: "SELECT octet_length('hello')"},
		{Name: "bit_length", Query: "SELECT bit_length('hello')"},

		// Case conversion
		{Name: "lower", Query: "SELECT lower('HELLO')"},
		{Name: "upper", Query: "SELECT upper('hello')"},
		{Name: "initcap", Query: "SELECT initcap('hello world')"},

		// Trimming
		{Name: "trim_both", Query: "SELECT trim('  hello  ')"},
		{Name: "ltrim", Query: "SELECT ltrim('  hello')"},
		{Name: "rtrim", Query: "SELECT rtrim('hello  ')"},
		{Name: "btrim", Query: "SELECT btrim('xxhelloxx', 'x')"},
		{Name: "trim_leading", Query: "SELECT trim(leading 'x' from 'xxxhello')"},
		{Name: "trim_trailing", Query: "SELECT trim(trailing 'x' from 'helloxxx')"},
		{Name: "trim_both_char", Query: "SELECT trim(both 'x' from 'xxxhelloxxx')"},

		// Padding
		{Name: "lpad_space", Query: "SELECT lpad('hello', 10)"},
		{Name: "lpad_char", Query: "SELECT lpad('hello', 10, 'xy')"},
		{Name: "rpad_space", Query: "SELECT rpad('hello', 10)"},
		{Name: "rpad_char", Query: "SELECT rpad('hello', 10, 'xy')"},

		// Substring
		{Name: "substring_from_for", Query: "SELECT substring('hello world' from 1 for 5)"},
		{Name: "substring_positional", Query: "SELECT substring('hello world', 1, 5)"},
		{Name: "substr", Query: "SELECT substr('hello world', 1, 5)"},
		{Name: "left", Query: "SELECT left('hello', 3)"},
		{Name: "right", Query: "SELECT right('hello', 3)"},

		// Position/Search
		{Name: "position", Query: "SELECT position('lo' in 'hello')"},
		{Name: "strpos", Query: "SELECT strpos('hello', 'lo')"},

		// Replace
		{Name: "replace", Query: "SELECT replace('hello world', 'world', 'there')"},
		{Name: "overlay", Query: "SELECT overlay('hello' placing 'XX' from 2 for 3)"},
		{Name: "translate", Query: "SELECT translate('hello', 'el', 'ip')"},

		// Concatenation
		{Name: "concat", Query: "SELECT concat('hello', ' ', 'world')"},
		{Name: "concat_ws", Query: "SELECT concat_ws(', ', 'a', 'b', 'c')"},
		{Name: "concat_operator", Query: "SELECT 'hello' || ' ' || 'world'"},

		// Split/Join
		{Name: "string_to_array", Query: "SELECT string_to_array('a,b,c', ',')"},
		{Name: "array_to_string", Query: "SELECT array_to_string(ARRAY['a', 'b', 'c'], ',')"},
		{Name: "split_part", Query: "SELECT split_part('a,b,c', ',', 2)"},

		// Formatting
		{Name: "format_s", Query: "SELECT format('Hello %s', 'world')"},
		{Name: "format_I", Query: "SELECT format('Column: %I', 'my_column')", Skip: SkipPostgresSpecific}, // %I identifier quoting not supported by DuckDB printf()

		// Regular expressions
		{Name: "regexp_replace", Query: "SELECT regexp_replace('hello', 'l+', 'L')"},
		{Name: "regexp_matches", Query: "SELECT regexp_matches('hello', 'l+')"},

		// Misc
		{Name: "reverse", Query: "SELECT reverse('hello')"},
		{Name: "repeat", Query: "SELECT repeat('ab', 3)"},
		{Name: "ascii", Query: "SELECT ascii('A')"},
		{Name: "chr", Query: "SELECT chr(65)"},
		{Name: "md5", Query: "SELECT md5('hello')"},
		{Name: "quote_literal", Query: "SELECT quote_literal('hello')"},
		{Name: "quote_ident", Query: "SELECT quote_ident('column')"},
		{Name: "quote_nullable_null", Query: "SELECT quote_nullable(NULL)"},
		{Name: "quote_nullable_value", Query: "SELECT quote_nullable('hello')"},

		// String aggregation
		{Name: "string_agg", Query: "SELECT string_agg(name, ', ' ORDER BY name) FROM users WHERE id <= 3"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsNumeric tests numeric functions
func TestFunctionsNumeric(t *testing.T) {
	tests := []QueryTest{
		// Basic arithmetic
		{Name: "abs", Query: "SELECT abs(-5)"},
		{Name: "sign_negative", Query: "SELECT sign(-5)"},
		{Name: "sign_positive", Query: "SELECT sign(5)"},
		{Name: "sign_zero", Query: "SELECT sign(0)"},

		// Rounding
		{Name: "ceil", Query: "SELECT ceil(4.3)"},
		{Name: "ceiling", Query: "SELECT ceiling(4.3)"},
		{Name: "floor", Query: "SELECT floor(4.7)"},
		{Name: "round", Query: "SELECT round(4.567)"},
		{Name: "round_precision", Query: "SELECT round(4.567, 2)"},
		{Name: "trunc", Query: "SELECT trunc(4.567)"},
		{Name: "trunc_precision", Query: "SELECT trunc(4.567, 2)"},

		// Division
		{Name: "div", Query: "SELECT div(10, 3)"},
		{Name: "mod", Query: "SELECT mod(10, 3)"},
		{Name: "modulo_operator", Query: "SELECT 10 % 3"},

		// Power and roots
		{Name: "power", Query: "SELECT power(2, 10)"},
		{Name: "sqrt", Query: "SELECT sqrt(16)"},
		{Name: "cbrt", Query: "SELECT cbrt(27)"},
		{Name: "exp", Query: "SELECT round(exp(1)::NUMERIC, 5)"},
		{Name: "ln", Query: "SELECT round(ln(10)::NUMERIC, 5)"},
		{Name: "log10", Query: "SELECT log(100)"},
		{Name: "log_base", Query: "SELECT log(2, 8)"},

		// Trigonometric
		{Name: "sin", Query: "SELECT round(sin(0)::NUMERIC, 10)"},
		{Name: "cos", Query: "SELECT round(cos(0)::NUMERIC, 10)"},
		{Name: "tan", Query: "SELECT round(tan(0)::NUMERIC, 10)"},
		{Name: "asin", Query: "SELECT round(asin(0)::NUMERIC, 10)"},
		{Name: "acos", Query: "SELECT round(acos(1)::NUMERIC, 10)"},
		{Name: "atan", Query: "SELECT round(atan(0)::NUMERIC, 10)"},
		{Name: "atan2", Query: "SELECT round(atan2(1, 1)::NUMERIC, 5)"},
		{Name: "degrees", Query: "SELECT round(degrees(3.14159)::NUMERIC, 2)"},
		{Name: "radians", Query: "SELECT round(radians(180)::NUMERIC, 5)"},
		{Name: "pi", Query: "SELECT round(pi()::NUMERIC, 5)"},

		// Random
		{Name: "random_range", Query: "SELECT random() >= 0 AND random() < 1"},

		// Comparison
		{Name: "greatest", Query: "SELECT greatest(1, 5, 3, 9, 2)"},
		{Name: "least", Query: "SELECT least(1, 5, 3, 9, 2)"},

		// Width bucket
		{Name: "width_bucket", Query: "SELECT width_bucket(5, 0, 10, 5)"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsDateTime tests date/time functions
func TestFunctionsDateTime(t *testing.T) {
	tests := []QueryTest{
		// Current date/time (test that they work, not specific values)
		{Name: "current_date_not_null", Query: "SELECT current_date IS NOT NULL"},
		{Name: "current_time_not_null", Query: "SELECT current_time IS NOT NULL"},
		{Name: "current_timestamp_not_null", Query: "SELECT current_timestamp IS NOT NULL"},
		{Name: "now_not_null", Query: "SELECT now() IS NOT NULL"},

		// Extract
		{Name: "extract_year", Query: "SELECT extract(year from DATE '2024-06-15')"},
		{Name: "extract_month", Query: "SELECT extract(month from DATE '2024-06-15')"},
		{Name: "extract_day", Query: "SELECT extract(day from DATE '2024-06-15')"},
		{Name: "extract_hour", Query: "SELECT extract(hour from TIMESTAMP '2024-06-15 14:30:45')"},
		{Name: "extract_minute", Query: "SELECT extract(minute from TIMESTAMP '2024-06-15 14:30:45')"},
		{Name: "extract_second", Query: "SELECT extract(second from TIMESTAMP '2024-06-15 14:30:45')"},
		{Name: "extract_dow", Query: "SELECT extract(dow from DATE '2024-06-15')"},
		{Name: "extract_doy", Query: "SELECT extract(doy from DATE '2024-06-15')"},
		{Name: "extract_week", Query: "SELECT extract(week from DATE '2024-06-15')"},
		{Name: "extract_quarter", Query: "SELECT extract(quarter from DATE '2024-06-15')"},
		{Name: "extract_epoch", Query: "SELECT extract(epoch from TIMESTAMP '2024-01-01 00:00:00')"},

		// Date part
		{Name: "date_part_year", Query: "SELECT date_part('year', DATE '2024-06-15')"},
		{Name: "date_part_month", Query: "SELECT date_part('month', DATE '2024-06-15')"},

		// Date trunc
		{Name: "date_trunc_year", Query: "SELECT date_trunc('year', TIMESTAMP '2024-06-15 14:30:45')"},
		{Name: "date_trunc_month", Query: "SELECT date_trunc('month', TIMESTAMP '2024-06-15 14:30:45')"},
		{Name: "date_trunc_day", Query: "SELECT date_trunc('day', TIMESTAMP '2024-06-15 14:30:45')"},
		{Name: "date_trunc_hour", Query: "SELECT date_trunc('hour', TIMESTAMP '2024-06-15 14:30:45')"},

		// Age
		{Name: "age_two_args", Query: "SELECT age(TIMESTAMP '2024-06-15', TIMESTAMP '2020-01-01')"},

		// Formatting
		{Name: "to_char_date", Query: "SELECT to_char(DATE '2024-06-15', 'YYYY-MM-DD')"},
		{Name: "to_char_day_name", Query: "SELECT to_char(DATE '2024-06-15', 'Day')"},
		{Name: "to_char_number", Query: "SELECT to_char(1234.56, '9999.99')"},
		{Name: "to_date", Query: "SELECT to_date('2024-06-15', 'YYYY-MM-DD')"},
		{Name: "to_timestamp", Query: "SELECT to_timestamp('2024-06-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS')"},

		// Make functions
		{Name: "make_date", Query: "SELECT make_date(2024, 6, 15)"},
		{Name: "make_time", Query: "SELECT make_time(14, 30, 45)"},
		{Name: "make_timestamp", Query: "SELECT make_timestamp(2024, 6, 15, 14, 30, 45)"},

		// Misc
		{Name: "isfinite_date", Query: "SELECT isfinite(DATE '2024-06-15')"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsAggregate tests aggregate functions
func TestFunctionsAggregate(t *testing.T) {
	tests := []QueryTest{
		// Basic aggregates
		{Name: "count_star", Query: "SELECT count(*) FROM users"},
		{Name: "count_column", Query: "SELECT count(name) FROM users"},
		{Name: "count_distinct", Query: "SELECT count(DISTINCT active) FROM users"},
		{Name: "sum", Query: "SELECT sum(age) FROM users"},
		{Name: "avg", Query: "SELECT avg(age) FROM users"},
		{Name: "min", Query: "SELECT min(age) FROM users"},
		{Name: "max", Query: "SELECT max(age) FROM users"},

		// Statistical aggregates
		{Name: "stddev", Query: "SELECT round(stddev(age)::NUMERIC, 2) FROM users"},
		{Name: "stddev_pop", Query: "SELECT round(stddev_pop(age)::NUMERIC, 2) FROM users"},
		{Name: "stddev_samp", Query: "SELECT round(stddev_samp(age)::NUMERIC, 2) FROM users"},
		{Name: "variance", Query: "SELECT round(variance(age)::NUMERIC, 2) FROM users"},
		{Name: "var_pop", Query: "SELECT round(var_pop(age)::NUMERIC, 2) FROM users"},
		{Name: "var_samp", Query: "SELECT round(var_samp(age)::NUMERIC, 2) FROM users"},

		// Boolean aggregates
		{Name: "bool_and", Query: "SELECT bool_and(active) FROM users"},
		{Name: "bool_or", Query: "SELECT bool_or(active) FROM users"},
		{Name: "every", Query: "SELECT every(active) FROM users"},

		// Bit aggregates
		{Name: "bit_and", Query: "SELECT bit_and(id) FROM users WHERE id <= 3"},
		{Name: "bit_or", Query: "SELECT bit_or(id) FROM users WHERE id <= 3"},

		// Array aggregates
		{Name: "array_agg", Query: "SELECT array_agg(name ORDER BY name) FROM users WHERE id <= 3"},
		{Name: "array_agg_distinct", Query: "SELECT array_agg(DISTINCT active) FROM users"},

		// String aggregates
		{Name: "string_agg_simple", Query: "SELECT string_agg(name, ',') FROM users WHERE id <= 3"},
		{Name: "string_agg_ordered", Query: "SELECT string_agg(name, ',' ORDER BY name) FROM users WHERE id <= 3"},

		// JSON aggregates
		{Name: "json_agg", Query: "SELECT json_agg(name) FROM users WHERE id <= 3"},
		{Name: "json_object_agg", Query: "SELECT json_object_agg(name, age) FROM users WHERE id <= 3"},

		// FILTER clause
		{Name: "count_filter", Query: "SELECT count(*) FILTER (WHERE active = true) FROM users"},
		{Name: "sum_filter", Query: "SELECT sum(age) FILTER (WHERE active = true) FROM users"},

		// Ordered-set aggregates
		{Name: "percentile_cont", Query: "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY age) FROM users"},
		{Name: "percentile_disc", Query: "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY age) FROM users"},
		{Name: "mode", Query: "SELECT mode() WITHIN GROUP (ORDER BY active) FROM users"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsJSON tests JSON functions
func TestFunctionsJSON(t *testing.T) {
	tests := []QueryTest{
		// Construction
		{Name: "json_build_object", Query: "SELECT json_build_object('a', 1, 'b', 2)"},
		{Name: "json_build_array", Query: "SELECT json_build_array(1, 2, 3)"},
		{Name: "to_json", Query: "SELECT to_json('hello')"},
		{Name: "to_jsonb", Query: "SELECT to_jsonb('hello')"},
		{Name: "array_to_json", Query: "SELECT array_to_json(ARRAY[1, 2, 3])"},

		// Extraction
		{Name: "json_extract_path", Query: "SELECT json_extract_path('{\"a\": {\"b\": 1}}'::JSON, 'a', 'b')"},
		{Name: "json_extract_path_text", Query: "SELECT json_extract_path_text('{\"a\": {\"b\": 1}}'::JSON, 'a', 'b')"},

		// Querying
		{Name: "json_typeof_object", Query: "SELECT json_typeof('{\"a\": 1}'::JSON)"},
		{Name: "json_typeof_array", Query: "SELECT json_typeof('[1, 2]'::JSON)"},
		{Name: "json_typeof_string", Query: "SELECT json_typeof('\"hello\"'::JSON)"},
		{Name: "json_typeof_number", Query: "SELECT json_typeof('42'::JSON)"},
		{Name: "json_array_length", Query: "SELECT json_array_length('[1, 2, 3]'::JSON)"},
		{Name: "jsonb_array_length", Query: "SELECT jsonb_array_length('[1, 2, 3]'::JSONB)"},

		// JSONB modification
		{Name: "jsonb_set", Query: "SELECT jsonb_set('{\"a\": 1}'::JSONB, '{a}', '2')"},
		{Name: "jsonb_concat", Query: "SELECT '{\"a\": 1}'::JSONB || '{\"b\": 2}'::JSONB"},
		{Name: "jsonb_delete_key", Query: "SELECT '{\"a\": 1, \"b\": 2}'::JSONB - 'a'"},

		// Expansion functions
		{Name: "json_each", Query: "SELECT * FROM json_each('{\"a\": 1, \"b\": 2}'::JSON)"},
		{Name: "jsonb_each", Query: "SELECT * FROM jsonb_each('{\"a\": 1, \"b\": 2}'::JSONB)"},
		{Name: "json_each_text", Query: "SELECT * FROM json_each_text('{\"a\": 1, \"b\": 2}'::JSON)"},
		{Name: "json_array_elements", Query: "SELECT * FROM json_array_elements('[1, 2, 3]'::JSON)"},
		{Name: "jsonb_array_elements", Query: "SELECT * FROM jsonb_array_elements('[1, 2, 3]'::JSONB)"},
		{Name: "json_array_elements_text", Query: "SELECT * FROM json_array_elements_text('[\"a\", \"b\"]'::JSON)"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsArray tests array functions
func TestFunctionsArray(t *testing.T) {
	tests := []QueryTest{
		// Information
		{Name: "array_length", Query: "SELECT array_length(ARRAY[1, 2, 3], 1)"},
		{Name: "array_ndims", Query: "SELECT array_ndims(ARRAY[[1, 2], [3, 4]])"},
		{Name: "cardinality", Query: "SELECT cardinality(ARRAY[1, 2, 3])"},

		// Search
		{Name: "array_position", Query: "SELECT array_position(ARRAY['a', 'b', 'c'], 'b')"},

		// Modification
		{Name: "array_append", Query: "SELECT array_append(ARRAY[1, 2], 3)"},
		{Name: "array_prepend", Query: "SELECT array_prepend(0, ARRAY[1, 2])"},
		{Name: "array_cat", Query: "SELECT array_cat(ARRAY[1, 2], ARRAY[3, 4])"},
		{Name: "array_remove", Query: "SELECT array_remove(ARRAY[1, 2, 1], 1)"},
		{Name: "array_replace", Query: "SELECT array_replace(ARRAY[1, 2, 1], 1, 10)"},

		// Transformation
		{Name: "unnest", Query: "SELECT unnest(ARRAY[1, 2, 3])"},
		{Name: "array_to_string", Query: "SELECT array_to_string(ARRAY[1, 2, 3], ',')"},
		{Name: "string_to_array", Query: "SELECT string_to_array('1,2,3', ',')"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsConditional tests conditional functions
func TestFunctionsConditional(t *testing.T) {
	tests := []QueryTest{
		// CASE
		{Name: "case_when", Query: "SELECT CASE WHEN 1 > 0 THEN 'positive' WHEN 1 < 0 THEN 'negative' ELSE 'zero' END"},
		{Name: "case_simple", Query: "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"},
		{Name: "case_in_select", Query: "SELECT name, CASE WHEN active THEN 'active' ELSE 'inactive' END AS status FROM users LIMIT 5"},

		// COALESCE
		{Name: "coalesce_null", Query: "SELECT COALESCE(NULL, 'default')"},
		{Name: "coalesce_value", Query: "SELECT COALESCE('value', 'default')"},
		{Name: "coalesce_multiple", Query: "SELECT COALESCE(NULL, NULL, 'third')"},

		// NULLIF
		{Name: "nullif_equal", Query: "SELECT NULLIF(1, 1)"},
		{Name: "nullif_different", Query: "SELECT NULLIF(1, 2)"},

		// GREATEST/LEAST
		{Name: "greatest", Query: "SELECT GREATEST(1, 5, 3)"},
		{Name: "least", Query: "SELECT LEAST(1, 5, 3)"},
	}
	runQueryTests(t, tests)
}

// TestFunctionsMisc tests miscellaneous functions
func TestFunctionsMisc(t *testing.T) {
	tests := []QueryTest{
		// Type info
		{Name: "pg_typeof", Query: "SELECT pg_typeof(1)"},
		{Name: "pg_typeof_text", Query: "SELECT pg_typeof('hello')"},

		// Generate series
		{Name: "generate_series_int", Query: "SELECT * FROM generate_series(1, 5)"},
		{Name: "generate_series_step", Query: "SELECT * FROM generate_series(1, 10, 2)"},
		{Name: "generate_series_date", Query: "SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-05', INTERVAL '1 day')"},

		// System info
		{Name: "current_database", Query: "SELECT current_database() IS NOT NULL"},
		{Name: "current_schema", Query: "SELECT current_schema() IS NOT NULL"},
		{Name: "current_user", Query: "SELECT current_user IS NOT NULL"},
		{Name: "session_user", Query: "SELECT session_user IS NOT NULL"},

		// Version
		{Name: "version", Query: "SELECT version() IS NOT NULL"},
	}
	runQueryTests(t, tests)
}
