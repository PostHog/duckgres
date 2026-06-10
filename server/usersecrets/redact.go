package usersecrets

import "strings"

// createSecretPrefixes are the statement shapes that can carry secret
// material (key ids, tokens, passwords) in their option list. DROP variants
// carry only a name and need no redaction.
var createSecretPrefixes = []string{
	"CREATE SECRET",
	"CREATE PERSISTENT SECRET",
	"CREATE TEMPORARY SECRET",
	"CREATE TEMP SECRET",
	"CREATE OR REPLACE SECRET",
	"CREATE OR REPLACE PERSISTENT SECRET",
	"CREATE OR REPLACE TEMPORARY SECRET",
	"CREATE OR REPLACE TEMP SECRET",
}

// RedactForLog returns a version of query safe to write to logs, traces,
// query logs, and pg_stat_activity. For CREATE SECRET statements the option
// list (which contains credential material) is dropped, keeping only the
// statement head and secret name. All other statements pass through
// unchanged. This is intentionally looser than Classify: any statement that
// merely looks like CREATE ... SECRET gets redacted — a false positive costs
// one uninformative log line, a false negative leaks a credential.
func RedactForLog(query string) string {
	head, ok := skipCommentsAndSpace(query)
	if !ok {
		return query
	}
	// Only the statement head matters for matching; don't uppercase a
	// potentially huge query just to reject it. 64 bytes comfortably covers
	// the longest prefix plus the word-boundary byte.
	probe := head
	if len(probe) > 64 {
		probe = probe[:64]
	}
	upper := strings.ToUpper(probe)
	for _, prefix := range createSecretPrefixes {
		if !strings.HasPrefix(upper, prefix) {
			continue
		}
		if len(upper) > len(prefix) && isIdentChar(upper[len(prefix)]) {
			continue // e.g. "CREATE SECRETIVE_TABLE"
		}
		// Keep everything up to the option list. The name (if any) sits
		// between the prefix and the first '(' and is safe to log.
		if idx := strings.IndexByte(head, '('); idx >= 0 {
			return strings.TrimSpace(head[:idx]) + " (…redacted)"
		}
		return strings.TrimSpace(head)
	}
	return query
}
