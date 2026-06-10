package usersecrets

import "strings"

// RedactForLog returns a version of query safe to write to logs, traces,
// query logs, and pg_stat_activity. For CREATE SECRET statements (any
// persistence, including multi-statement strings whose head is secret DDL)
// everything after the statement head is dropped — the option list carries
// credential material, and on a multi-statement string the trailing
// statements are dropped along with it. DROP variants carry only a name and
// pass through unchanged, as does every non-secret statement.
//
// This is driven by the same tokenizer as Classify, so the redactor can never
// be out of sync with the interceptor: any whitespace/comment arrangement
// Classify accepts is redacted, and the non-matching fast path is two short
// case-folded keyword comparisons with no allocation.
func RedactForLog(query string) string {
	st, headEnd, ok := parseSecretDDLHead(query)
	if !ok || st.Kind != KindCreate {
		return query
	}
	return strings.TrimSpace(query[:headEnd]) + " (…redacted)"
}
