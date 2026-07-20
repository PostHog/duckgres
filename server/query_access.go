package server

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/transform"
)

// QueryAccessPolicy is a fail-closed SQL policy for a project-scoped user.
// A nil policy means the internal/root principal and remains unrestricted.
type QueryAccessPolicy struct {
	ReadOnly         bool
	AllowedSchemas   []string
	AllowedRelations []string
}

// QueryAccessError is returned when a project-scoped principal attempts an
// operation or relation outside its policy.
type QueryAccessError struct {
	Reason string
}

func (e *QueryAccessError) Error() string {
	return "permission denied: " + e.Reason
}

var dangerousReadFunctions = map[string]struct{}{
	"current_setting":       {},
	"duckdb_secrets":        {},
	"duckdb_settings":       {},
	"getenv":                {},
	"glob":                  {},
	"http_get":              {},
	"http_post":             {},
	"nextval":               {},
	"parquet_file_metadata": {},
	"parquet_metadata":      {},
	"parquet_schema":        {},
	"query":                 {},
	"query_table":           {},
	"read_blob":             {},
	"read_csv":              {},
	"read_csv_auto":         {},
	"read_json":             {},
	"read_json_auto":        {},
	"read_ndjson":           {},
	"read_ndjson_auto":      {},
	"read_parquet":          {},
	"set_config":            {},
	"setval":                {},
	"sniff_csv":             {},
	"st_read":               {},
	"which_secret":          {},
	"write_file":            {},
}

var unqualifiedMetadataRelations = map[string]struct{}{
	"pg_attribute": {},
	"pg_class":     {},
	"pg_database":  {},
	"pg_namespace": {},
	"pg_roles":     {},
	"pg_tables":    {},
	"pg_views":     {},
}

var allowedShowVariables = map[string]struct{}{
	"application_name":              {},
	"client_encoding":               {},
	"datestyle":                     {},
	"default_transaction_isolation": {},
	"integer_datetimes":             {},
	"search_path":                   {},
	"server_version":                {},
	"server_version_num":            {},
	"standard_conforming_strings":   {},
	"timezone":                      {},
	"transaction_isolation":         {},
	"transaction_read_only":         {},
}

// Authorize verifies that query is read-only and every persistent relation is
// owned by the project. Native DuckDB fallback is deliberately unavailable to
// scoped users because an unparsed statement cannot be authorized safely.
func (p *QueryAccessPolicy) Authorize(query string) error {
	if p == nil {
		return nil
	}
	tree, err := pg_query.Parse(query)
	if err != nil {
		return &QueryAccessError{Reason: "project connections only accept PostgreSQL-compatible read queries"}
	}

	cteNames := make(map[string]struct{})
	transform.WalkFunc(tree, func(node *pg_query.Node) bool {
		if cte := node.GetCommonTableExpr(); cte != nil {
			cteNames[strings.ToLower(cte.Ctename)] = struct{}{}
		}
		return true
	})

	allowedSchemas := normalizedSet(p.AllowedSchemas)
	allowedRelations := normalizedSet(p.AllowedRelations)
	for _, raw := range tree.Stmts {
		if raw == nil || raw.Stmt == nil {
			continue
		}
		if err := authorizeStatementNode(raw.Stmt); err != nil {
			return err
		}
	}

	var denied error
	transform.WalkFunc(tree, func(node *pg_query.Node) bool {
		if err := authorizeReadNode(node); err != nil {
			denied = err
			return false
		}
		if rv := node.GetRangeVar(); rv != nil {
			if err := authorizeRangeVar(rv, allowedSchemas, allowedRelations, cteNames); err != nil {
				denied = err
				return false
			}
		}
		if fc := node.GetFuncCall(); fc != nil {
			name := functionName(fc)
			if dangerousFunction(name) {
				denied = &QueryAccessError{Reason: fmt.Sprintf("function %q is unavailable to project connections", name)}
				return false
			}
		}
		return true
	})
	return denied
}

func dangerousFunction(name string) bool {
	if _, dangerous := dangerousReadFunctions[name]; dangerous {
		return true
	}
	return strings.HasPrefix(name, "read_") ||
		strings.HasPrefix(name, "http_") ||
		strings.HasPrefix(name, "mysql_") ||
		strings.HasPrefix(name, "postgres_") ||
		strings.HasPrefix(name, "sqlite_") ||
		strings.HasSuffix(name, "_scan")
}

func authorizeReadNode(node *pg_query.Node) error {
	if selectStmt := node.GetSelectStmt(); selectStmt != nil && selectStmt.IntoClause != nil {
		return &QueryAccessError{Reason: "project connections are read-only"}
	}
	switch node.Node.(type) {
	case *pg_query.Node_InsertStmt, *pg_query.Node_UpdateStmt, *pg_query.Node_DeleteStmt, *pg_query.Node_MergeStmt:
		return &QueryAccessError{Reason: "project connections are read-only"}
	default:
		return nil
	}
}

func authorizeStatementNode(node *pg_query.Node) error {
	if show := node.GetVariableShowStmt(); show != nil {
		if _, allowed := allowedShowVariables[strings.ToLower(show.Name)]; allowed {
			return nil
		}
		return &QueryAccessError{Reason: fmt.Sprintf("setting %q is unavailable to project connections", show.Name)}
	}
	switch node.Node.(type) {
	case *pg_query.Node_SelectStmt,
		*pg_query.Node_TransactionStmt,
		*pg_query.Node_FetchStmt,
		*pg_query.Node_ClosePortalStmt:
		return nil
	default:
		return &QueryAccessError{Reason: "project connections are read-only"}
	}
}

func authorizeRangeVar(rv *pg_query.RangeVar, allowedSchemas, allowedRelations, cteNames map[string]struct{}) error {
	catalog := strings.ToLower(rv.Catalogname)
	schema := strings.ToLower(rv.Schemaname)
	relation := strings.ToLower(rv.Relname)
	if catalog != "" && catalog != "ducklake" {
		return &QueryAccessError{Reason: fmt.Sprintf("catalog %q is not available to this project", rv.Catalogname)}
	}
	if schema == "information_schema" || schema == "pg_catalog" {
		return nil
	}
	if schema == "" {
		if _, ok := cteNames[relation]; ok {
			return nil
		}
		if _, ok := unqualifiedMetadataRelations[relation]; ok {
			return nil
		}
		return &QueryAccessError{Reason: fmt.Sprintf("relation %q must be schema-qualified", rv.Relname)}
	}
	if _, ok := allowedSchemas[schema]; ok {
		return nil
	}
	qualified := schema + "." + relation
	if _, ok := allowedRelations[qualified]; ok {
		return nil
	}
	return &QueryAccessError{Reason: fmt.Sprintf("relation %q is not available to this project", qualified)}
}

func functionName(call *pg_query.FuncCall) string {
	if call == nil || len(call.Funcname) == 0 {
		return ""
	}
	last := call.Funcname[len(call.Funcname)-1].GetString_()
	if last == nil {
		return ""
	}
	return strings.ToLower(last.Sval)
}

func normalizedSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[strings.ToLower(strings.TrimSpace(value))] = struct{}{}
	}
	return result
}

// NormalizeQueryAccessPolicy makes policy snapshots deterministic for tests,
// logging, and cross-protocol conversion.
func NormalizeQueryAccessPolicy(policy QueryAccessPolicy) QueryAccessPolicy {
	policy.AllowedSchemas = append([]string(nil), policy.AllowedSchemas...)
	policy.AllowedRelations = append([]string(nil), policy.AllowedRelations...)
	sort.Strings(policy.AllowedSchemas)
	sort.Strings(policy.AllowedRelations)
	return policy
}
