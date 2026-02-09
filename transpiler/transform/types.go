package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// TypeMappingTransform converts PostgreSQL types to DuckDB equivalents.
// Based on sqlglot's DuckDB dialect TYPE_MAPPING.
type TypeMappingTransform struct{}

func NewTypeMappingTransform() *TypeMappingTransform {
	return &TypeMappingTransform{}
}

func (t *TypeMappingTransform) Name() string {
	return "type_mapping"
}

// PostgreSQL type -> DuckDB type mappings
// Based on sqlglot's DuckDB dialect
var typeMapping = map[string]string{
	// Character types - DuckDB prefers TEXT
	"bpchar":   "text",
	"char":     "text",
	"nchar":    "text",
	"nvarchar": "text",
	// Note: varchar is kept as-is since DuckDB supports it

	// Binary types
	"binary":     "blob",
	"varbinary":  "blob",
	"bytea":      "blob",
	"rowversion": "blob",

	// JSON types
	"jsonb": "json",

	// Numeric types
	"money": "decimal(19,4)", // PostgreSQL money -> decimal

	// Date/time types
	"datetime":     "timestamp",
	"timestampntz": "timestamp",

	// Boolean
	"bool": "boolean",

	// Integer aliases
	"int2": "smallint",
	"int4": "integer",
	"int8": "bigint",

	// Floating point
	"float4": "real",
	"float8": "double",

	// Network types (not supported in DuckDB, convert to text)
	"inet":    "text",
	"cidr":    "text",
	"macaddr": "text",

	// Full-text search types (not supported in DuckDB)
	"tsvector": "text",
	"tsquery":  "text",

	// Geometric types (limited support in DuckDB)
	"point":   "text",
	"line":    "text",
	"lseg":    "text",
	"box":     "text",
	"path":    "text",
	"polygon": "text",
	"circle":  "text",

	// Range types (not directly supported)
	"int4range":   "text",
	"int8range":   "text",
	"numrange":    "text",
	"tsrange":     "text",
	"tstzrange":   "text",
	"daterange":   "text",
	"int4multirange": "text",
	"int8multirange": "text",
}

func (t *TypeMappingTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		switch n := node.Node.(type) {
		case *pg_query.Node_TypeCast:
			if n.TypeCast != nil && n.TypeCast.TypeName != nil {
				if t.transformTypeName(n.TypeCast.TypeName) {
					changed = true
				}
			}
		case *pg_query.Node_ColumnDef:
			if n.ColumnDef != nil && n.ColumnDef.TypeName != nil {
				if t.transformTypeName(n.ColumnDef.TypeName) {
					changed = true
				}
			}
		case *pg_query.Node_FuncCall:
			// CAST function calls have type as second argument
			// but are handled differently in the AST (via TypeCast nodes)
			// so no action needed here
		}
		return true
	})

	return changed, nil
}

// transformTypeName converts PostgreSQL type names to DuckDB equivalents
func (t *TypeMappingTransform) transformTypeName(typeName *pg_query.TypeName) bool {
	if typeName == nil || len(typeName.Names) == 0 {
		return false
	}

	// Get the type name (last element, ignoring schema prefix)
	var typeStr string
	var typeNameNode *pg_query.Node
	for _, name := range typeName.Names {
		if str := name.GetString_(); str != nil {
			typeStr = strings.ToLower(str.Sval)
			typeNameNode = name
		}
	}

	if typeStr == "" {
		return false
	}

	// Check if we have a mapping
	if newType, ok := typeMapping[typeStr]; ok {
		// Handle complex types like decimal(19,4)
		if strings.Contains(newType, "(") {
			// Parse the type and precision
			baseName := strings.Split(newType, "(")[0]
			if str := typeNameNode.GetString_(); str != nil {
				str.Sval = baseName
			}
			// For now, we just change the base type name
			// Precision handling would need more complex AST manipulation
		} else {
			if str := typeNameNode.GetString_(); str != nil {
				str.Sval = newType
			}
		}

		// Remove pg_catalog schema prefix if present
		if len(typeName.Names) > 1 {
			if first := typeName.Names[0].GetString_(); first != nil {
				if strings.ToLower(first.Sval) == "pg_catalog" {
					typeName.Names = typeName.Names[1:]
				}
			}
		}

		return true
	}

	// Even without a type mapping, strip pg_catalog schema prefix.
	// PostgreSQL parser qualifies built-in types as pg_catalog.typename
	// (e.g., pg_catalog.json) but DuckDB rejects the pg_catalog prefix.
	if len(typeName.Names) > 1 {
		if first := typeName.Names[0].GetString_(); first != nil {
			if strings.ToLower(first.Sval) == "pg_catalog" {
				typeName.Names = typeName.Names[1:]
				return true
			}
		}
	}

	return false
}
