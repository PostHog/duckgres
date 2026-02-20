package transform

import (
	"fmt"
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
	"timeofday":       "current_timestamp", // approximate
	"clock_timestamp":  "now",              // wall-clock time (close enough)
	"make_date":   "make_date",   // same
	"make_time":   "make_time",   // same
	"make_timestamp": "make_timestamp", // same

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
	"json_object_keys":  "json_keys",
	"jsonb_object_keys": "json_keys",

	// Type conversion
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

// SpecialFunctionNames returns all function names that have special (non-rename) transformations.
func SpecialFunctionNames() map[string]bool {
	return specialFunctions
}

// Functions that need special transformation (not just renaming)
var specialFunctions = map[string]bool{
	"to_char":        true, // format string conversion PG→strftime/format
	"to_date":        true, // format string conversion PG→strptime
	"to_timestamp":   true, // format string conversion for 2-arg form
	"regexp_matches": true, // return type differs
	"array_agg":      true, // becomes list()
	"string_to_array": true, // argument order
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

	case "to_char":
		return t.handleToChar(fc, funcNameIdx)

	case "to_date":
		return t.handleToDate(fc, funcNameIdx)

	case "to_timestamp":
		return t.handleToTimestamp(fc, funcNameIdx)

	default:
		return false
	}
}

// handleToChar converts PostgreSQL to_char() to DuckDB equivalents.
// to_char(timestamp, format) → strftime(timestamp, converted_format)
// to_char(number, format) → format(converted_format, number)
func (t *FunctionTransform) handleToChar(fc *pg_query.FuncCall, funcNameIdx int) bool {
	if len(fc.Args) != 2 {
		return false
	}

	// Try to get the format string (second argument)
	formatStr := extractStringConstant(fc.Args[1])
	if formatStr == "" {
		// Can't determine format at parse time — default to strftime for timestamps
		renameFuncAndStripPrefix(fc, funcNameIdx, "strftime")
		return true
	}

	if isDateFormat(formatStr) {
		// Date/time formatting: to_char(ts, fmt) → strftime(ts, converted_fmt)
		converted := pgDateFormatToStrftime(formatStr)
		setStringConstant(fc.Args[1], converted)
		renameFuncAndStripPrefix(fc, funcNameIdx, "strftime")
	} else {
		// Number formatting: to_char(num, fmt) → format(converted_fmt, num)
		converted := pgNumberFormatToDuckDB(formatStr)
		setStringConstant(fc.Args[1], converted)
		renameFuncAndStripPrefix(fc, funcNameIdx, "format")
		// Swap args: PG to_char(value, fmt) → DuckDB format(fmt, value)
		fc.Args[0], fc.Args[1] = fc.Args[1], fc.Args[0]
	}
	return true
}

// handleToDate converts PostgreSQL to_date(text, format) to strptime with converted format.
func (t *FunctionTransform) handleToDate(fc *pg_query.FuncCall, funcNameIdx int) bool {
	if len(fc.Args) != 2 {
		return false
	}
	formatStr := extractStringConstant(fc.Args[1])
	if formatStr != "" {
		setStringConstant(fc.Args[1], pgDateFormatToStrftime(formatStr))
	}
	renameFuncAndStripPrefix(fc, funcNameIdx, "strptime")
	return true
}

// handleToTimestamp converts 2-arg PostgreSQL to_timestamp(text, format) to strptime.
// 1-arg to_timestamp(epoch) is left unchanged (DuckDB handles it natively).
func (t *FunctionTransform) handleToTimestamp(fc *pg_query.FuncCall, funcNameIdx int) bool {
	if len(fc.Args) != 2 {
		// 1-arg form: to_timestamp(epoch) — DuckDB handles natively
		return false
	}
	formatStr := extractStringConstant(fc.Args[1])
	if formatStr != "" {
		setStringConstant(fc.Args[1], pgDateFormatToStrftime(formatStr))
	}
	renameFuncAndStripPrefix(fc, funcNameIdx, "strptime")
	return true
}

// renameFuncAndStripPrefix renames a function and removes any pg_catalog/public schema prefix.
func renameFuncAndStripPrefix(fc *pg_query.FuncCall, funcNameIdx int, newName string) {
	if str := fc.Funcname[funcNameIdx].GetString_(); str != nil {
		str.Sval = newName
	}
	if len(fc.Funcname) > 1 {
		fc.Funcname = fc.Funcname[funcNameIdx:]
	}
}

// extractStringConstant extracts the string value from an AST node if it's a string constant.
func extractStringConstant(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if ac := node.GetAConst(); ac != nil {
		if sval := ac.GetSval(); sval != nil {
			return sval.Sval
		}
	}
	return ""
}

// setStringConstant sets the string value of an AST node that is a string constant.
func setStringConstant(node *pg_query.Node, value string) {
	if node == nil {
		return
	}
	if ac := node.GetAConst(); ac != nil {
		if sval := ac.GetSval(); sval != nil {
			sval.Sval = value
		}
	}
}

// isDateFormat checks if a PG format string is a date/time format (vs number format).
func isDateFormat(format string) bool {
	upper := strings.ToUpper(format)
	dateSpecifiers := []string{
		"YYYY", "YY", "DD", "HH", "SS", "MON", "MONTH", "DAY", "DY",
		"TZ", "AM", "PM", "A.M.", "P.M.", "DDD", "IW", "WW", "IYYY",
	}
	for _, s := range dateSpecifiers {
		if strings.Contains(upper, s) {
			return true
		}
	}
	// MI is ambiguous (minute in date, minus-sign-after in number).
	// If there are other date specifiers, it's already detected above.
	// If MI appears alone with no digit placeholders (9, 0), treat as date.
	if strings.Contains(upper, "MI") && !strings.ContainsAny(upper, "90") {
		return true
	}
	return false
}

// pgDateFormatToStrftime converts PostgreSQL date/time format specifiers to strftime/strptime format.
// Processes left-to-right with longest-match-first to avoid ambiguity.
func pgDateFormatToStrftime(pgFormat string) string {
	type spec struct {
		pg, sf string
	}
	// Ordered by length descending, then alphabetically for same length.
	specifiers := []spec{
		// 5 chars
		{"MONTH", "%B"}, {"month", "%B"}, {"Month", "%B"},
		// 4 chars
		{"A.M.", "%p"}, {"P.M.", "%p"}, {"a.m.", "%p"}, {"p.m.", "%p"},
		{"YYYY", "%Y"}, {"yyyy", "%Y"},
		{"IYYY", "%G"}, {"iyyy", "%G"},
		{"HH24", "%H"}, {"hh24", "%H"},
		{"HH12", "%I"}, {"hh12", "%I"},
		{"SSSS", ""}, {"ssss", ""},
		// 3 chars
		{"MON", "%b"}, {"Mon", "%b"}, {"mon", "%b"},
		{"DAY", "%A"}, {"Day", "%A"}, {"day", "%A"},
		{"DDD", "%j"}, {"ddd", "%j"},
		// 2 chars
		{"HH", "%I"}, {"hh", "%I"},
		{"YY", "%y"}, {"yy", "%y"},
		{"MM", "%m"},
		{"MI", "%M"}, {"mi", "%M"},
		{"DD", "%d"}, {"dd", "%d"},
		{"SS", "%S"}, {"ss", "%S"},
		{"MS", "%g"}, {"ms", "%g"},
		{"US", "%f"}, {"us", "%f"},
		{"AM", "%p"}, {"PM", "%p"}, {"am", "%p"}, {"pm", "%p"},
		{"TZ", "%Z"}, {"tz", "%Z"},
		{"OF", "%z"}, {"of", "%z"},
		{"IW", "%V"}, {"iw", "%V"},
		{"WW", "%U"}, {"ww", "%U"},
		{"DY", "%a"}, {"Dy", "%a"}, {"dy", "%a"},
		{"FM", ""}, {"fm", ""},
		{"CC", ""}, {"cc", ""},
		// Note: Single-char specifiers D (day of week), W (week of month),
		// Q (quarter), J (Julian day) are intentionally omitted — they're
		// too aggressive and can match stray letters in literal text.
	}

	var result strings.Builder
	i := 0
	for i < len(pgFormat) {
		// Handle quoted literal text
		if pgFormat[i] == '"' {
			i++
			for i < len(pgFormat) && pgFormat[i] != '"' {
				result.WriteByte(pgFormat[i])
				i++
			}
			if i < len(pgFormat) {
				i++ // skip closing quote
			}
			continue
		}

		matched := false
		for _, s := range specifiers {
			if i+len(s.pg) <= len(pgFormat) && pgFormat[i:i+len(s.pg)] == s.pg {
				result.WriteString(s.sf)
				i += len(s.pg)
				matched = true
				break
			}
		}
		if !matched {
			result.WriteByte(pgFormat[i])
			i++
		}
	}
	return result.String()
}

// pgNumberFormatToDuckDB converts PostgreSQL number format specifiers to DuckDB format() syntax.
// PG format: '999,999.99' → DuckDB: '{:,.2f}'
func pgNumberFormatToDuckDB(pgFormat string) string {
	upper := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(pgFormat, "FM", ""), "fm", ""))

	hasComma := strings.ContainsAny(upper, ",G")

	// Count decimal places (digits after . or D)
	decimalPlaces := 0
	dotIdx := strings.IndexAny(upper, ".D")
	if dotIdx >= 0 {
		for i := dotIdx + 1; i < len(upper); i++ {
			if upper[i] == '9' || upper[i] == '0' {
				decimalPlaces++
			}
		}
	}

	commaFlag := ""
	if hasComma {
		commaFlag = ","
	}

	prefix := ""
	if strings.Contains(upper, "$") || strings.Contains(upper, "L") {
		prefix = "$"
	}

	// Check for EEEE (scientific notation)
	if strings.Contains(upper, "EEEE") {
		return fmt.Sprintf("%s{:%s.%de}", prefix, commaFlag, decimalPlaces)
	}

	return fmt.Sprintf("%s{:%s.%df}", prefix, commaFlag, decimalPlaces)
}
