package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// FunctionTransform converts PostgreSQL function calls to DuckDB equivalents.
// Based on sqlglot's DuckDB dialect function mappings.
type FunctionTransform struct{}

func NewFunctionTransform() *FunctionTransform {
	return &FunctionTransform{}
}

func (t *FunctionTransform) Name() string {
	return "functions"
}

// Simple function name mappings (PostgreSQL -> DuckDB)
// These are direct renames where the semantics are identical or close enough
var functionNameMapping = map[string]string{
	// Array functions
	"array_length":  "len",
	"array_upper":   "len", // approximate - array_upper returns upper bound
	"array_cat":     "list_concat",
	"array_append":  "list_append",
	"array_prepend": "list_prepend",
	"array_remove":  "list_filter", // needs special handling
	"array_position": "list_position",
	"unnest":        "unnest", // same name, but semantics differ slightly

	// String functions
	"strpos":       "strpos",    // same
	"substr":       "substr",    // same
	"substring":    "substring", // same
	"btrim":        "trim",
	"ltrim":        "ltrim", // same
	"rtrim":        "rtrim", // same
	"lpad":         "lpad",  // same
	"rpad":         "rpad",  // same
	"chr":          "chr",   // same
	"ascii":        "ascii", // same
	"repeat":       "repeat", // same
	"reverse":      "reverse", // same
	"split_part":   "split_part", // same
	"string_to_array": "string_split",
	"array_to_string": "array_to_string", // DuckDB has this

	// Math functions
	"log":    "log10", // PostgreSQL log() is base 10, ln() is natural
	"cbrt":   "cbrt",  // same
	"div":    "//",    // integer division - needs operator transform
	"mod":    "mod",   // same (also %)
	"trunc":  "trunc", // same
	"width_bucket": "width_bucket", // same

	// Date/time functions
	"date_part":   "date_part",   // same
	"date_trunc":  "date_trunc",  // same but different behavior
	"extract":     "extract",     // same
	"age":         "age",         // DuckDB has this
	"now":         "now",         // same (also current_timestamp)
	"current_date": "current_date",
	"current_time": "current_time",
	"current_timestamp": "current_timestamp",
	"localtime":   "current_time",
	"localtimestamp": "current_timestamp",
	"timeofday":   "current_timestamp", // approximate
	"make_date":   "make_date",   // same
	"make_time":   "make_time",   // same
	"make_timestamp": "make_timestamp", // same
	"to_timestamp": "to_timestamp", // same for unix timestamp
	"to_date":     "strptime",    // needs format handling
	"to_char":     "strftime",    // needs format handling

	// Aggregate functions
	"string_agg":  "string_agg",  // same
	"array_agg":   "list",        // DuckDB uses list() for array aggregation
	"bool_and":    "bool_and",    // same
	"bool_or":     "bool_or",     // same
	"bit_and":     "bit_and",     // same
	"bit_or":      "bit_or",      // same
	"every":       "bool_and",    // EVERY is alias for BOOL_AND

	// JSON functions (PostgreSQL -> DuckDB)
	"json_build_object":  "json_object",
	"jsonb_build_object": "json_object",
	"json_build_array":   "json_array",
	"jsonb_build_array":  "json_array",
	"json_agg":           "json_group_array",
	"jsonb_agg":          "json_group_array",
	"json_object_agg":    "json_group_object",
	"jsonb_object_agg":   "json_group_object",
	"json_array_length":  "json_array_length",
	"jsonb_array_length": "json_array_length",
	"json_typeof":        "json_type",
	"jsonb_typeof":       "json_type",
	"json_extract_path":  "json_extract",
	"jsonb_extract_path": "json_extract",
	"json_extract_path_text": "json_extract_string",
	"jsonb_extract_path_text": "json_extract_string",

	// Type conversion
	"to_number": "cast", // needs special handling
	"to_json":   "to_json",
	"to_jsonb":  "to_json",

	// Regex functions
	"regexp_match":   "regexp_extract",    // returns first match
	"regexp_matches": "regexp_extract_all", // returns all matches
	"regexp_replace": "regexp_replace",    // same
	"regexp_split_to_array": "regexp_split_to_array",
	"regexp_split_to_table": "regexp_split_to_table",

	// Misc functions
	"generate_series": "generate_series", // DuckDB has this now
	"coalesce":        "coalesce",        // same
	"nullif":          "nullif",          // same
	"greatest":        "greatest",        // same
	"least":           "least",           // same
	"md5":             "md5",             // same
	"sha256":          "sha256",          // same (DuckDB extension)
	"encode":          "encode",          // same
	"decode":          "decode",          // same
	"pg_typeof":       "typeof",          // DuckDB equivalent

	// Information functions (mostly stubs or approximations)
	"current_database": "current_database",
	"current_schema":   "current_schema",
	"current_schemas":  "current_schemas",
	"current_user":     "current_user",
	"session_user":     "current_user",
	"user":             "current_user",
}

// FunctionNames returns all PostgreSQL function names that have DuckDB mappings.
func FunctionNames() map[string]string {
	return functionNameMapping
}

// Functions that need special transformation (not just renaming)
var specialFunctions = map[string]bool{
	"to_char":           true, // format string conversion
	"to_date":           true, // format string conversion
	"to_timestamp":      true, // may need format handling
	"date_trunc":        true, // week handling differs
	"extract":           true, // some parts differ
	"regexp_matches":    true, // return type differs
	"array_agg":         true, // becomes list()
	"string_to_array":   true, // argument order
	"generate_series":   true, // may need range() for some cases
}

func (t *FunctionTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		if fc := node.GetFuncCall(); fc != nil {
			if t.transformFuncCall(fc) {
				changed = true
			}
		}
		return true
	})

	return changed, nil
}

func (t *FunctionTransform) transformFuncCall(fc *pg_query.FuncCall) bool {
	if fc == nil || len(fc.Funcname) == 0 {
		return false
	}

	// Get function name (last element, ignoring schema prefix)
	var funcName string
	var funcNameIdx int
	for i, name := range fc.Funcname {
		if str := name.GetString_(); str != nil {
			funcName = strings.ToLower(str.Sval)
			funcNameIdx = i
		}
	}

	if funcName == "" {
		return false
	}

	// Check for simple name mapping
	if newName, ok := functionNameMapping[funcName]; ok {
		if str := fc.Funcname[funcNameIdx].GetString_(); str != nil {
			str.Sval = newName
		}

		// array_upper(arr, dim) and array_length(arr, dim) take a dimension
		// parameter that DuckDB's len() doesn't accept. Strip the second arg.
		if (funcName == "array_upper" || funcName == "array_length") && len(fc.Args) == 2 {
			fc.Args = fc.Args[:1]
		}

		// Remove schema prefix if present, BUT preserve pg_catalog for functions
		// using SQL syntax (COERCE_SQL_SYNTAX) like EXTRACT(field FROM source)
		// because the deparser only outputs the special syntax with the prefix.
		// However, only preserve the prefix when the function name stays the same.
		// If we rename the function (like btrim -> trim), the deparser won't recognize
		// it as SQL syntax anymore, so we must strip the prefix.
		preservePrefix := fc.Funcformat == pg_query.CoercionForm_COERCE_SQL_SYNTAX && funcName == newName
		if len(fc.Funcname) > 1 && !preservePrefix {
			if first := fc.Funcname[0].GetString_(); first != nil {
				schema := strings.ToLower(first.Sval)
				if schema == "pg_catalog" || schema == "public" {
					fc.Funcname = fc.Funcname[1:]
				}
			}
		}

		return true
	}

	// Handle special transformations
	if specialFunctions[funcName] {
		return t.handleSpecialFunction(fc, funcName, funcNameIdx)
	}

	return false
}

func (t *FunctionTransform) handleSpecialFunction(fc *pg_query.FuncCall, funcName string, funcNameIdx int) bool {
	switch funcName {
	case "array_agg":
		// PostgreSQL array_agg() -> DuckDB list()
		if str := fc.Funcname[funcNameIdx].GetString_(); str != nil {
			str.Sval = "list"
		}
		if len(fc.Funcname) > 1 {
			fc.Funcname = fc.Funcname[funcNameIdx:]
		}
		return true

	case "string_to_array":
		// PostgreSQL string_to_array(string, delimiter)
		// -> DuckDB string_split(string, delimiter)
		if str := fc.Funcname[funcNameIdx].GetString_(); str != nil {
			str.Sval = "string_split"
		}
		if len(fc.Funcname) > 1 {
			fc.Funcname = fc.Funcname[funcNameIdx:]
		}
		return true

	case "regexp_matches":
		// PostgreSQL regexp_matches returns setof text[]
		// DuckDB regexp_extract_all returns list
		// For simple cases, this mapping works
		if str := fc.Funcname[funcNameIdx].GetString_(); str != nil {
			str.Sval = "regexp_extract_all"
		}
		if len(fc.Funcname) > 1 {
			fc.Funcname = fc.Funcname[funcNameIdx:]
		}
		return true

	// For now, leave other special functions as-is
	// They may work or need more complex transformation
	default:
		return false
	}
}
