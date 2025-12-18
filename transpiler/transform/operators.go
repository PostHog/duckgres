package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// OperatorTransform converts PostgreSQL operators to DuckDB equivalents.
// Handles JSON operators, regex operators, and other PostgreSQL-specific operators.
type OperatorTransform struct{}

func NewOperatorTransform() *OperatorTransform {
	return &OperatorTransform{}
}

func (t *OperatorTransform) Name() string {
	return "operators"
}

func (t *OperatorTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		if aexpr := node.GetAExpr(); aexpr != nil {
			if t.transformOperator(aexpr) {
				changed = true
			}
		}
		return true
	})

	return changed, nil
}

func (t *OperatorTransform) transformOperator(aexpr *pg_query.A_Expr) bool {
	if aexpr == nil || len(aexpr.Name) == 0 {
		return false
	}

	// Get operator name
	var opName string
	for _, name := range aexpr.Name {
		if str := name.GetString_(); str != nil {
			opName = str.Sval
			break
		}
	}

	if opName == "" {
		return false
	}

	switch opName {
	// JSON operators
	case "->":
		// PostgreSQL: json -> key returns json
		// DuckDB: json_extract(json, '$.key') or json->'key'
		// DuckDB supports -> operator for JSON, so this works as-is
		return false

	case "->>":
		// PostgreSQL: json ->> key returns text
		// DuckDB: json_extract_string(json, '$.key') or json->>'key'
		// DuckDB supports ->> operator, so this works as-is
		return false

	case "#>":
		// PostgreSQL: json #> path returns json (path is text[])
		// DuckDB: Need to convert to json_extract with path
		// This is complex - for now, leave as-is and it will error
		return false

	case "#>>":
		// PostgreSQL: json #>> path returns text
		// DuckDB: Need to convert to json_extract_string with path
		return false

	case "@>":
		// PostgreSQL: jsonb @> jsonb (contains)
		// DuckDB: json_contains or different syntax
		// PostgreSQL: array @> array (contains)
		// DuckDB: list_has_all or similar
		return false

	case "<@":
		// PostgreSQL: jsonb <@ jsonb (is contained by)
		// PostgreSQL: array <@ array (is contained by)
		return false

	case "?":
		// PostgreSQL: jsonb ? key (key exists)
		// DuckDB: json_exists or different approach
		return false

	case "?|":
		// PostgreSQL: jsonb ?| text[] (any key exists)
		return false

	case "?&":
		// PostgreSQL: jsonb ?& text[] (all keys exist)
		return false

	// Regex operators - DuckDB uses function syntax instead
	case "~":
		// PostgreSQL: text ~ pattern (regex match, case sensitive)
		// DuckDB: regexp_matches(text, pattern)
		return t.convertRegexOperator(aexpr, "regexp_matches", false)

	case "~*":
		// PostgreSQL: text ~* pattern (regex match, case insensitive)
		// DuckDB: regexp_matches(text, pattern, 'i')
		return t.convertRegexOperator(aexpr, "regexp_matches", true)

	case "!~":
		// PostgreSQL: text !~ pattern (regex no match, case sensitive)
		// DuckDB: NOT regexp_matches(text, pattern)
		return t.convertRegexOperator(aexpr, "regexp_matches", false)

	case "!~*":
		// PostgreSQL: text !~* pattern (regex no match, case insensitive)
		// DuckDB: NOT regexp_matches(text, pattern, 'i')
		return t.convertRegexOperator(aexpr, "regexp_matches", true)

	// String pattern matching
	case "~~":
		// PostgreSQL: text ~~ pattern (LIKE)
		// This is the internal representation of LIKE
		// DuckDB supports LIKE, so no change needed
		return false

	case "~~*":
		// PostgreSQL: text ~~* pattern (ILIKE)
		// DuckDB supports ILIKE, so no change needed
		return false

	case "!~~":
		// PostgreSQL: text !~~ pattern (NOT LIKE)
		return false

	case "!~~*":
		// PostgreSQL: text !~~* pattern (NOT ILIKE)
		return false

	// Array operators
	case "&&":
		// PostgreSQL: array && array (overlap)
		// DuckDB: list_has_any or similar
		// For now, leave as-is
		return false

	case "||":
		// PostgreSQL: text || text (concatenation) - same in DuckDB
		// PostgreSQL: array || array (concatenation)
		// DuckDB: list_concat for arrays
		// Hard to distinguish without type info, leave as-is
		return false
	}

	return false
}

// convertRegexOperator converts PostgreSQL regex operators to DuckDB function calls
// Note: This is complex because we need to replace the entire A_Expr node
// For now, we just mark it as needing conversion - the actual conversion
// would require restructuring the AST which is more involved
func (t *OperatorTransform) convertRegexOperator(aexpr *pg_query.A_Expr, funcName string, caseInsensitive bool) bool {
	// For now, just strip the pg_catalog schema prefix if present
	// The actual operator -> function conversion is complex and would
	// require replacing the A_Expr with a FuncCall node

	if len(aexpr.Name) > 1 {
		// Check for OPERATOR(pg_catalog.~) syntax
		if first := aexpr.Name[0].GetString_(); first != nil {
			if strings.ToLower(first.Sval) == "pg_catalog" {
				// Remove the pg_catalog prefix
				aexpr.Name = aexpr.Name[1:]
				return true
			}
		}
	}

	return false
}

// OperatorMappingNote documents the operator mappings for reference:
//
// JSON Operators (PostgreSQL -> DuckDB):
//   -> : Same (extract JSON object field)
//   ->> : Same (extract JSON object field as text)
//   #> : Not directly supported (use json_extract with path)
//   #>> : Not directly supported (use json_extract_string with path)
//   @> : json_contains() or manual check
//   <@ : Reverse of @>
//   ? : json_exists()
//   ?| : Manual check with OR
//   ?& : Manual check with AND
//
// Regex Operators (PostgreSQL -> DuckDB):
//   ~ : regexp_matches(text, pattern)
//   ~* : regexp_matches(text, pattern, 'i')
//   !~ : NOT regexp_matches(text, pattern)
//   !~* : NOT regexp_matches(text, pattern, 'i')
//
// Array Operators:
//   && : list_has_any()
//   @> : list_has_all()
//   <@ : Reverse containment
//   || : list_concat() for arrays, || for strings
