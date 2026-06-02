package icebergmeta

import (
	"regexp"
	"strings"
)

// canonicalDecimalRE matches an Iceberg decimal type like "decimal(10,2)".
var canonicalDecimalRE = regexp.MustCompile(`^decimal\(\s*\d+\s*,\s*\d+\s*\)$`)

// CanonicalPGType maps an Iceberg field type to a canonical PostgreSQL type name
// (the value reported as information_schema.columns.data_type).
//
// This is the single source of truth for the Iceberg→PostgreSQL type contract.
// Primitive Iceberg types arrive as plain strings (e.g. "long", "decimal(10,2)");
// nested types (struct/list/map) arrive as JSON objects, which the REST loader
// encodes as a JSON string. Per the compatibility policy, all nested/complex
// types — and the Iceberg v3 "variant" type — are presented as "jsonb" so that
// JDBC/SQLAlchemy clients receive a type they understand.
//
// Reference (Iceberg primitive type -> PostgreSQL):
//
//	boolean       -> boolean
//	int           -> integer
//	long          -> bigint
//	float         -> real
//	double        -> double precision
//	decimal(p,s)  -> numeric   (precision/scale carried separately)
//	date          -> date
//	time          -> time without time zone
//	timestamp     -> timestamp without time zone
//	timestamptz   -> timestamp with time zone
//	string        -> text
//	uuid          -> uuid
//	binary/fixed  -> bytea
//	struct/list/map/variant -> jsonb
func CanonicalPGType(icebergType string) string {
	t := strings.ToLower(strings.TrimSpace(icebergType))
	if t == "" {
		return ""
	}

	// Nested/complex types are encoded by the REST loader as a JSON object.
	if strings.HasPrefix(t, "{") {
		return "jsonb"
	}

	if canonicalDecimalRE.MatchString(t) {
		return "numeric"
	}

	// fixed[L] (fixed-length binary) and any fixed(...) spelling.
	if strings.HasPrefix(t, "fixed") {
		return "bytea"
	}

	switch t {
	case "boolean", "bool":
		return "boolean"
	case "int", "integer":
		return "integer"
	case "long", "bigint":
		return "bigint"
	case "float":
		return "real"
	case "double":
		return "double precision"
	case "date":
		return "date"
	case "time":
		return "time without time zone"
	case "timestamp", "timestamp_ns":
		return "timestamp without time zone"
	case "timestamptz", "timestamptz_ns", "timestamp_tz":
		return "timestamp with time zone"
	case "string":
		return "text"
	case "uuid":
		return "uuid"
	case "binary":
		return "bytea"
	case "struct", "list", "map", "variant":
		// Bare nested type names (some catalogs report these without the JSON body).
		return "jsonb"
	default:
		// Unknown types fall back to their lowercased name; the columns view's
		// type CASE leaves unrecognized names as-is.
		return t
	}
}
