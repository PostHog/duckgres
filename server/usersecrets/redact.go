package usersecrets

import "strings"

// redactedPlaceholder replaces a whole query whose credential material cannot
// be safely located and stripped in place (a CREATE SECRET that is not the
// statement head of the string).
const redactedPlaceholder = "(…redacted)"

// RedactForLog returns a version of query safe to write to logs, traces,
// query logs, and pg_stat_activity. CREATE SECRET option lists carry
// credential material and must never reach a log sink.
//
// The fast path: when the statement head is a CREATE SECRET, everything after
// the head is dropped (the option list and, for a multi-statement string, the
// trailing statements). DROP variants carry only a name and pass through, as
// does every non-secret single statement.
//
// The hardened path guards against secret DDL that is NOT the statement head,
// e.g. "SELECT 1; CREATE PERSISTENT SECRET foo (...)" or
// "BEGIN; CREATE SECRET ...". Such a string does not classify at its head, so
// the head-only check would leak it verbatim. We therefore split on top-level
// semicolons and, if ANY segment classifies as a CREATE SECRET, replace the
// entire query with a fixed placeholder. Over-redaction is harmless here —
// false positives only cost log fidelity, never credential exposure.
//
// This is driven by the same tokenizer as Classify, so the redactor can never
// be out of sync with the interceptor: any whitespace/comment arrangement
// Classify accepts is redacted, and the non-matching fast path is two short
// case-folded keyword comparisons with no allocation.
func RedactForLog(query string) string {
	if st, headEnd, ok := parseSecretDDLHead(query); ok && st.Kind == KindCreate {
		return strings.TrimSpace(query[:headEnd]) + " " + redactedPlaceholder
	}

	// Head is not a CREATE SECRET. If the query is a single top-level
	// statement, there is nothing more to check — the fast path already
	// handled it (and DROP / non-secret pass through unchanged). Only when
	// there are multiple top-level statements must we scan for secret DDL
	// hiding behind a leading statement.
	if !hasTrailingStatement(query) {
		return query
	}
	for _, seg := range splitTopLevel(query) {
		if st, _, ok := parseSecretDDLHead(seg); ok && st.Kind == KindCreate {
			return redactedPlaceholder
		}
	}
	return query
}
