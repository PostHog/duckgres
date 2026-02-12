package integration

import (
	"testing"
)

// TestTypesNumeric tests numeric type handling
func TestTypesNumeric(t *testing.T) {
	tests := []QueryTest{
		// Integer types
		{Name: "smallint_literal", Query: "SELECT 1::SMALLINT"},
		{Name: "integer_literal", Query: "SELECT 42::INTEGER"},
		{Name: "bigint_literal", Query: "SELECT 9223372036854775807::BIGINT"},
		{Name: "int2_alias", Query: "SELECT 1::INT2"},
		{Name: "int4_alias", Query: "SELECT 1::INT4"},
		{Name: "int8_alias", Query: "SELECT 1::INT8"},

		// Floating point
		{Name: "real_literal", Query: "SELECT 3.14::REAL"},
		{Name: "double_precision", Query: "SELECT 3.14159265358979::DOUBLE PRECISION"},
		{Name: "float4_alias", Query: "SELECT 1.5::FLOAT4"},
		{Name: "float8_alias", Query: "SELECT 1.5::FLOAT8"},

		// Numeric/Decimal
		{Name: "numeric_literal", Query: "SELECT 123.456::NUMERIC"},
		{Name: "numeric_precision", Query: "SELECT 123.456::NUMERIC(10, 2)"},
		{Name: "decimal_alias", Query: "SELECT 123.456::DECIMAL(10, 2)"},

		// Special float values
		{Name: "float_nan", Query: "SELECT 'NaN'::FLOAT"},
		{Name: "float_infinity", Query: "SELECT 'Infinity'::FLOAT"},
		{Name: "float_neg_infinity", Query: "SELECT '-Infinity'::FLOAT"},

		// Integer operations
		{Name: "integer_add", Query: "SELECT 2147483647 + 1::BIGINT"},
		{Name: "integer_multiply", Query: "SELECT 1000000 * 1000000::BIGINT"},
		{Name: "integer_divide", Query: "SELECT 10 / 3"},
		{Name: "integer_modulo", Query: "SELECT 17 % 5"},

		// From table
		{Name: "select_int_columns", Query: "SELECT int2_col, int4_col, int8_col FROM types_test WHERE id = 1"},
		{Name: "select_float_columns", Query: "SELECT float4_col, float8_col FROM types_test WHERE id = 1"},
		{Name: "select_numeric_column", Query: "SELECT numeric_col FROM types_test WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesCharacter tests character/string type handling
func TestTypesCharacter(t *testing.T) {
	tests := []QueryTest{
		// Basic types
		{Name: "text_literal", Query: "SELECT 'hello'::TEXT"},
		{Name: "varchar_literal", Query: "SELECT 'hello'::VARCHAR"},
		{Name: "varchar_limit", Query: "SELECT 'hello'::VARCHAR(10)"},
		{Name: "char_fixed", Query: "SELECT 'hi'::CHAR(10)"},
		{Name: "character_varying", Query: "SELECT 'hello'::CHARACTER VARYING(100)"},

		// Empty and whitespace
		{Name: "empty_string", Query: "SELECT ''::TEXT"},
		{Name: "whitespace_string", Query: "SELECT '   '::TEXT"},

		// Special characters
		{Name: "string_with_quotes", Query: "SELECT 'it''s a test'::TEXT"},
		{Name: "string_with_newline", Query: "SELECT E'line1\\nline2'::TEXT"},
		{Name: "string_with_tab", Query: "SELECT E'col1\\tcol2'::TEXT"},

		// Unicode
		{Name: "unicode_string", Query: "SELECT 'Hello, \u4e16\u754c'::TEXT"},
		{Name: "emoji_string", Query: "SELECT '\U0001f600\U0001f389'::TEXT"},

		// From table
		{Name: "select_text_column", Query: "SELECT text_col FROM types_test WHERE id = 1"},
		{Name: "select_varchar_column", Query: "SELECT varchar_col FROM types_test WHERE id = 1"},
		{Name: "select_char_column", Query: "SELECT char_col FROM types_test WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesDateTime tests date/time type handling
func TestTypesDateTime(t *testing.T) {
	tests := []QueryTest{
		// Date
		{Name: "date_literal", Query: "SELECT DATE '2024-01-15'"},
		{Name: "date_cast", Query: "SELECT '2024-01-15'::DATE"},
		{Name: "current_date", Query: "SELECT CURRENT_DATE IS NOT NULL"},

		// Time
		{Name: "time_literal", Query: "SELECT TIME '12:30:45'"},
		{Name: "time_cast", Query: "SELECT '12:30:45'::TIME"},
		{Name: "time_with_microseconds", Query: "SELECT TIME '12:30:45.123456'"},

		// Timestamp
		{Name: "timestamp_literal", Query: "SELECT TIMESTAMP '2024-01-15 12:30:45'"},
		{Name: "timestamp_cast", Query: "SELECT '2024-01-15 12:30:45'::TIMESTAMP"},
		{Name: "timestamp_with_microseconds", Query: "SELECT TIMESTAMP '2024-01-15 12:30:45.123456'"},

		// Timestamp with timezone
		{Name: "timestamptz_literal", Query: "SELECT TIMESTAMPTZ '2024-01-15 12:30:45+00'"},
		{Name: "timestamptz_cast", Query: "SELECT '2024-01-15 12:30:45+00'::TIMESTAMPTZ"},
		{Name: "now_function", Query: "SELECT NOW() IS NOT NULL"},

		// Interval
		{Name: "interval_day", Query: "SELECT INTERVAL '1 day'"},
		{Name: "interval_hour_minute", Query: "SELECT INTERVAL '1 hour 30 minutes'"},
		{Name: "interval_complex", Query: "SELECT INTERVAL '1 year 2 months 3 days'"},
		{Name: "interval_cast", Query: "SELECT '1 day'::INTERVAL"},

		// Date arithmetic
		{Name: "date_add_interval", Query: "SELECT DATE '2024-01-15' + INTERVAL '1 month'"},
		{Name: "date_subtract_interval", Query: "SELECT DATE '2024-01-15' - INTERVAL '1 day'"},
		{Name: "date_add_integer", Query: "SELECT DATE '2024-01-15' + 7"},
		{Name: "date_difference", Query: "SELECT DATE '2024-01-15' - DATE '2024-01-01'"},

		// From table
		{Name: "select_date_column", Query: "SELECT date_col FROM types_test WHERE id = 1"},
		{Name: "select_time_column", Query: "SELECT time_col FROM types_test WHERE id = 1"},
		{Name: "select_timestamp_column", Query: "SELECT timestamp_col FROM types_test WHERE id = 1"},
		{Name: "select_timestamptz_column", Query: "SELECT timestamptz_col FROM types_test WHERE id = 1"},
		{Name: "select_interval_column", Query: "SELECT interval_col FROM types_test WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesBoolean tests boolean type handling
func TestTypesBoolean(t *testing.T) {
	tests := []QueryTest{
		// Literals
		{Name: "boolean_true", Query: "SELECT TRUE"},
		{Name: "boolean_false", Query: "SELECT FALSE"},

		// Cast from string
		{Name: "boolean_from_true_string", Query: "SELECT 'true'::BOOLEAN"},
		{Name: "boolean_from_false_string", Query: "SELECT 'false'::BOOLEAN"},
		{Name: "boolean_from_t", Query: "SELECT 't'::BOOLEAN"},
		{Name: "boolean_from_f", Query: "SELECT 'f'::BOOLEAN"},
		{Name: "boolean_from_yes", Query: "SELECT 'yes'::BOOLEAN"},
		{Name: "boolean_from_no", Query: "SELECT 'no'::BOOLEAN"},
		{Name: "boolean_from_1", Query: "SELECT '1'::BOOLEAN"},
		{Name: "boolean_from_0", Query: "SELECT '0'::BOOLEAN"},

		// Boolean operations
		{Name: "boolean_and", Query: "SELECT TRUE AND FALSE"},
		{Name: "boolean_or", Query: "SELECT TRUE OR FALSE"},
		{Name: "boolean_not", Query: "SELECT NOT TRUE"},

		// From table
		{Name: "select_boolean_column", Query: "SELECT bool_col FROM types_test WHERE id = 1"},
		{Name: "filter_by_boolean", Query: "SELECT COUNT(*) FROM users WHERE active = true"},
	}
	runQueryTests(t, tests)
}

// TestTypesBinary tests binary type handling
func TestTypesBinary(t *testing.T) {
	tests := []QueryTest{
		// Bytea literals
		{Name: "bytea_hex", Query: "SELECT '\\x48656c6c6f'::BYTEA"},
		{Name: "bytea_escape", Query: "SELECT '\\000\\001\\002'::BYTEA", Skip: SkipDifferentBehavior},

		// Encode/decode
		{Name: "decode_hex", Query: "SELECT decode('48656c6c6f', 'hex')"},
		{Name: "encode_hex", Query: "SELECT encode('hello'::BYTEA, 'hex')"},
		{Name: "encode_base64", Query: "SELECT encode('hello'::BYTEA, 'base64')"},
		{Name: "decode_base64", Query: "SELECT decode('aGVsbG8=', 'base64')"},

		// From table
		{Name: "select_bytea_column", Query: "SELECT bytea_col FROM types_test WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesUUID tests UUID type handling
func TestTypesUUID(t *testing.T) {
	tests := []QueryTest{
		{Name: "uuid_literal", Query: "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID"},
		{Name: "gen_random_uuid", Query: "SELECT gen_random_uuid() IS NOT NULL"},
		{Name: "uuid_comparison", Query: "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID"},

		// From table
		{Name: "select_uuid_column", Query: "SELECT uuid_col FROM types_test WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesJSON tests JSON type handling
func TestTypesJSON(t *testing.T) {
	tests := []QueryTest{
		// JSON literals
		{Name: "json_object", Query: "SELECT '{\"a\": 1}'::JSON"},
		{Name: "json_array", Query: "SELECT '[1, 2, 3]'::JSON"},
		{Name: "json_nested", Query: "SELECT '{\"a\": {\"b\": 1}}'::JSON"},
		{Name: "json_string", Query: "SELECT '\"hello\"'::JSON"},
		{Name: "json_number", Query: "SELECT '42'::JSON"},
		{Name: "json_null", Query: "SELECT 'null'::JSON"},

		// JSONB (may be stored as JSON)
		{Name: "jsonb_object", Query: "SELECT '{\"a\": 1}'::JSONB"},
		{Name: "jsonb_array", Query: "SELECT '[1, 2, 3]'::JSONB"},

		// JSON access operators
		{Name: "json_arrow", Query: "SELECT '{\"a\": 1}'::JSON -> 'a'"},
		{Name: "json_double_arrow", Query: "SELECT '{\"a\": 1}'::JSON ->> 'a'"},
		{Name: "json_array_access", Query: "SELECT '[1, 2, 3]'::JSON -> 0"},
		{Name: "json_path", Query: "SELECT '{\"a\": {\"b\": 1}}'::JSON #> '{a,b}'", Skip: SkipUnsupportedByDuckDB},
		{Name: "json_path_text", Query: "SELECT '{\"a\": {\"b\": 1}}'::JSON #>> '{a,b}'", Skip: SkipUnsupportedByDuckDB},

		// JSONB operators
		{Name: "jsonb_contains", Query: "SELECT '{\"a\": 1, \"b\": 2}'::JSONB @> '{\"a\": 1}'::JSONB"},
		{Name: "jsonb_contained", Query: "SELECT '{\"a\": 1}'::JSONB <@ '{\"a\": 1, \"b\": 2}'::JSONB"},
		{Name: "jsonb_exists", Query: "SELECT '{\"a\": 1}'::JSONB ? 'a'"},

		// From table
		{Name: "select_json_data", Query: "SELECT data FROM json_data WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesArray tests array type handling
func TestTypesArray(t *testing.T) {
	tests := []QueryTest{
		// Array literals
		{Name: "int_array", Query: "SELECT ARRAY[1, 2, 3]"},
		{Name: "text_array", Query: "SELECT ARRAY['a', 'b', 'c']"},
		{Name: "nested_array", Query: "SELECT ARRAY[[1, 2], [3, 4]]"},
		{Name: "array_cast", Query: "SELECT '{1,2,3}'::INTEGER[]"},

		// Array access
		{Name: "array_subscript", Query: "SELECT (ARRAY[1, 2, 3])[1]"},
		{Name: "array_slice", Query: "SELECT (ARRAY[1, 2, 3, 4, 5])[2:4]"},

		// Array operators
		{Name: "array_concat", Query: "SELECT ARRAY[1, 2] || ARRAY[3, 4]"},
		{Name: "array_append", Query: "SELECT ARRAY[1, 2] || 3"},
		{Name: "array_prepend", Query: "SELECT 0 || ARRAY[1, 2]"},
		{Name: "array_contains", Query: "SELECT ARRAY[1, 2, 3] @> ARRAY[1]"},
		{Name: "array_contained", Query: "SELECT ARRAY[1] <@ ARRAY[1, 2, 3]"},
		{Name: "array_overlap", Query: "SELECT ARRAY[1, 2] && ARRAY[2, 3]"},

		// ANY/ALL with arrays
		{Name: "any_array", Query: "SELECT 2 = ANY(ARRAY[1, 2, 3])"},
		{Name: "all_array", Query: "SELECT 1 = ALL(ARRAY[1, 1, 1])"},

		// From table
		{Name: "select_array_column", Query: "SELECT int_array FROM array_test WHERE id = 1"},
		{Name: "select_text_array", Query: "SELECT text_array FROM array_test WHERE id = 1"},
	}
	runQueryTests(t, tests)
}

// TestTypesNullHandling tests NULL handling across types
func TestTypesNullHandling(t *testing.T) {
	tests := []QueryTest{
		// NULL comparisons
		{Name: "null_equals", Query: "SELECT NULL = NULL"},
		{Name: "null_is_null", Query: "SELECT NULL IS NULL"},
		{Name: "null_is_not_null", Query: "SELECT NULL IS NOT NULL"},
		{Name: "null_is_distinct_from_null", Query: "SELECT NULL IS DISTINCT FROM NULL"},
		{Name: "null_is_not_distinct_from_null", Query: "SELECT NULL IS NOT DISTINCT FROM NULL"},

		// NULL in expressions
		{Name: "null_arithmetic", Query: "SELECT 1 + NULL"},
		{Name: "null_concat", Query: "SELECT 'hello' || NULL"},
		{Name: "null_and_true", Query: "SELECT NULL AND TRUE"},
		{Name: "null_or_true", Query: "SELECT NULL OR TRUE"},

		// COALESCE
		{Name: "coalesce_null", Query: "SELECT COALESCE(NULL, 'default')"},
		{Name: "coalesce_value", Query: "SELECT COALESCE('value', 'default')"},
		{Name: "coalesce_multiple", Query: "SELECT COALESCE(NULL, NULL, 'third')"},

		// NULLIF
		{Name: "nullif_equal", Query: "SELECT NULLIF(1, 1)"},
		{Name: "nullif_different", Query: "SELECT NULLIF(1, 2)"},

		// From table with NULLs
		{Name: "select_nullable", Query: "SELECT * FROM nullable_test WHERE val1 IS NULL"},
		{Name: "count_nulls", Query: "SELECT COUNT(*), COUNT(val1), COUNT(val2) FROM nullable_test"},
	}
	runQueryTests(t, tests)
}

// TestTypesCasting tests type casting
func TestTypesCasting(t *testing.T) {
	tests := []QueryTest{
		// CAST syntax
		{Name: "cast_int_to_text", Query: "SELECT CAST(123 AS TEXT)"},
		{Name: "cast_text_to_int", Query: "SELECT CAST('123' AS INTEGER)"},
		{Name: "cast_float_to_int", Query: "SELECT CAST(3.7 AS INTEGER)"},
		{Name: "cast_bool_to_int", Query: "SELECT CAST(TRUE AS INTEGER)"},

		// :: syntax
		{Name: "colon_int_to_text", Query: "SELECT 123::TEXT"},
		{Name: "colon_text_to_int", Query: "SELECT '123'::INTEGER"},
		{Name: "colon_timestamp", Query: "SELECT '2024-01-15 12:30:45'::TIMESTAMP"},

		// Implicit casting
		{Name: "implicit_int_to_float", Query: "SELECT 1 + 1.5"},
		{Name: "implicit_concat", Query: "SELECT 'count: ' || 5"},
	}
	runQueryTests(t, tests)
}

// TestTypesUnsupported tests types that are mapped or unsupported
func TestTypesUnsupported(t *testing.T) {
	tests := []QueryTest{
		// Network types (mapped to TEXT in Duckgres)
		{Name: "inet_type", Query: "SELECT '192.168.1.1'::INET", Skip: SkipNetworkType},
		{Name: "cidr_type", Query: "SELECT '192.168.1.0/24'::CIDR", Skip: SkipNetworkType},
		{Name: "macaddr_type", Query: "SELECT '08:00:2b:01:02:03'::MACADDR", Skip: SkipNetworkType},

		// Geometric types
		{Name: "point_type", Query: "SELECT point(1, 2)", Skip: SkipGeometricType},
		{Name: "line_type", Query: "SELECT line(point(0,0), point(1,1))", Skip: SkipGeometricType},
		{Name: "box_type", Query: "SELECT box(point(0,0), point(1,1))", Skip: SkipGeometricType},

		// Range types
		{Name: "int4range_type", Query: "SELECT '[1,10)'::int4range", Skip: SkipRangeType},
		{Name: "daterange_type", Query: "SELECT '[2024-01-01,2024-12-31)'::daterange", Skip: SkipRangeType},

		// Text search types
		{Name: "tsvector_type", Query: "SELECT 'hello world'::tsvector", Skip: SkipTextSearch},
		{Name: "tsquery_type", Query: "SELECT 'hello & world'::tsquery", Skip: SkipTextSearch},

		// Money type (has locale issues)
		{Name: "money_type", Query: "SELECT '12.34'::money", Skip: "money type not well supported"},
	}
	runQueryTests(t, tests)
}
