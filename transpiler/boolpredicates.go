package transpiler

import (
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/transform"
)

// Matches a true/false literal adjacent to =, != or <> with any interleaving
// of whitespace and closing/opening parens between them, so parenthesized
// literals like "flag = ( ( true ) )" are gated too. False positives are safe
// no-ops: the AST transform only rewrites real boolean comparisons (parens
// produce no AST node).
var booleanPredicatePattern = regexp.MustCompile(`(?is)(?:\btrue\b|\bfalse\b)[\s)]*(?:=|!=|<>)|(?:=|!=|<>)[\s(]*(?:\btrue\b|\bfalse\b)`)

func NeedsBooleanPredicateRewrite(sql string) bool {
	return booleanPredicatePattern.MatchString(sql)
}

func RewriteBooleanPredicates(sql string) (string, bool, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" || !NeedsBooleanPredicateRewrite(sql) {
		return sql, false, nil
	}

	tree, err := pg_query.Parse(sql)
	if err != nil {
		return sql, false, nil
	}

	result := &transform.Result{}
	changed, err := transform.NewBooleanPredicateTransform().Transform(tree, result)
	if err != nil || !changed {
		return sql, false, err
	}

	rewritten, err := pg_query.Deparse(tree)
	if err != nil {
		return sql, false, nil
	}

	return rewritten, true, nil
}
