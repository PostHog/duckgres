// Package usersecrets implements the building blocks of the per-user
// persistent secret manager: classification of DuckDB secret DDL statements
// and authenticated encryption for storing them in the config store.
//
// In the multi-tenant control plane, a worker pod's DuckDB secrets die with
// the pod, so "CREATE PERSISTENT SECRET" is intercepted at the control plane,
// stored encrypted in the Postgres config store keyed by (org, user, name),
// and replayed onto the worker at session creation.
package usersecrets

import (
	"strings"
)

// Kind classifies a SQL statement with respect to secret DDL.
type Kind int

const (
	// KindNone means the statement is not secret DDL (or is one this package
	// declines to handle, e.g. part of a multi-statement string).
	KindNone Kind = iota
	// KindCreate is CREATE [OR REPLACE] [PERSISTENT|TEMPORARY] SECRET.
	KindCreate
	// KindDrop is DROP [PERSISTENT|TEMPORARY] SECRET.
	KindDrop
)

// Statement is the classification result for a secret DDL statement.
type Statement struct {
	Kind Kind
	// Name is the secret name, lowercased (DuckDB stores secret names
	// case-insensitively). Empty for an unnamed CREATE SECRET.
	Name string
	// Persistent is true when the PERSISTENT keyword is present.
	Persistent bool
	// Temporary is true when the TEMPORARY keyword is present.
	Temporary bool
	// IfNotExists is true for CREATE ... IF NOT EXISTS. The persistence layer
	// must honor it: when the secret is already stored, DuckDB no-ops on the
	// live session, so overwriting the stored statement would silently
	// diverge the two.
	IfNotExists bool
	// MultiStatement is true when the secret DDL is followed by further
	// statements ("CREATE PERSISTENT SECRET ...; SELECT 1"). Persistence side
	// effects must map 1:1 to a statement, so callers must not persist these —
	// and must not let persistent variants silently fall through either,
	// because the statement would execute, never persist, and then be wiped
	// at the next session.
	MultiStatement bool
}

// Reserved secret names/prefixes. These belong to duckgres itself: the
// system-issued catalog secrets, DuckDB's default names for unnamed secrets,
// and a duckgres_ prefix reserved for future system use.
var reservedNames = map[string]struct{}{
	"ducklake_s3":   {},
	"iceberg_sigv4": {},
	"iceberg_oauth": {},
}

var reservedPrefixes = []string{"__default_", "duckgres_"}

// IsReservedName reports whether a secret name is reserved for system use.
func IsReservedName(name string) bool {
	name = strings.ToLower(name)
	if _, ok := reservedNames[name]; ok {
		return true
	}
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// Classify inspects a SQL statement and reports whether it is DuckDB secret
// DDL. It is deliberately conservative: anything it cannot confidently parse
// as secret DDL classifies as KindNone, which makes the caller fall back to
// today's plain passthrough behavior. Multi-statement strings classify with
// MultiStatement set so callers can reject (not silently drop) persistent
// variants.
func Classify(query string) Statement {
	st, _, ok := parseSecretDDLHead(query)
	if !ok {
		return Statement{}
	}
	st.MultiStatement = hasTrailingStatement(query)
	return st
}

// ContainsPersistentSecretDDL reports whether any top-level statement in query
// is CREATE/DROP PERSISTENT SECRET. Unlike Classify (which inspects only the
// statement head), this scans every top-level statement, so a persistent-secret
// DDL hidden behind a leading statement ("SELECT 1; CREATE PERSISTENT SECRET
// ...") is still caught. Used to reject persistent-secret DDL on the Flight SQL
// ingress, where it would execute but never persist.
func ContainsPersistentSecretDDL(query string) bool {
	if st, _, ok := parseSecretDDLHead(query); ok && st.Persistent {
		return true
	}
	if !hasTrailingStatement(query) {
		return false
	}
	for _, seg := range splitTopLevel(query) {
		if st, _, ok := parseSecretDDLHead(seg); ok && st.Persistent {
			return true
		}
	}
	return false
}

// parseSecretDDLHead parses the statement head (through the optional secret
// name) and returns the classification plus the byte offset just past the
// head. The fast path for non-secret statements is two short case-folded
// keyword comparisons — no allocation — so this is safe on hot paths.
func parseSecretDDLHead(query string) (Statement, int, bool) {
	rest, ok := skipCommentsAndSpace(query)
	if !ok {
		return Statement{}, 0, false
	}
	toks := newTokenizer(rest)
	headStart := len(query) - len(rest)

	var st Statement
	switch {
	case toks.eatKeyword("CREATE"):
		st.Kind = KindCreate
		if toks.eatKeyword("OR") {
			if !toks.eatKeyword("REPLACE") {
				return Statement{}, 0, false
			}
		}
	case toks.eatKeyword("DROP"):
		st.Kind = KindDrop
	default:
		return Statement{}, 0, false
	}

	if toks.eatKeyword("PERSISTENT") {
		st.Persistent = true
	} else if toks.eatKeyword("TEMPORARY") || toks.eatKeyword("TEMP") {
		st.Temporary = true
	}

	if !toks.eatKeyword("SECRET") {
		return Statement{}, 0, false
	}

	if st.Kind == KindCreate {
		if toks.eatKeyword("IF") {
			if !toks.eatKeyword("NOT") || !toks.eatKeyword("EXISTS") {
				return Statement{}, 0, false
			}
			st.IfNotExists = true
		}
	} else {
		if toks.eatKeyword("IF") {
			if !toks.eatKeyword("EXISTS") {
				return Statement{}, 0, false
			}
		}
	}

	// Optional secret name. For CREATE the next token may instead be the
	// opening paren of the option list (unnamed secret).
	if name, ok := toks.eatIdentifier(); ok {
		st.Name = strings.ToLower(name)
	}

	return st, headStart + toks.i, true
}

// skipCommentsAndSpace removes leading whitespace, line comments (--) and
// block comments (/* */, nested) from s. Returns ok=false on an unterminated
// block comment.
func skipCommentsAndSpace(s string) (string, bool) {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "--"):
			idx := strings.IndexByte(s, '\n')
			if idx < 0 {
				return "", true
			}
			s = s[idx+1:]
		case strings.HasPrefix(s, "/*"):
			depth := 1
			i := 2
			for i < len(s) && depth > 0 {
				switch {
				case strings.HasPrefix(s[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(s[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			if depth > 0 {
				return "", false
			}
			s = s[i:]
		default:
			return s, true
		}
	}
}

// hasTrailingStatement reports whether query contains a top-level semicolon
// (outside string literals, quoted identifiers, and comments) followed by
// anything other than whitespace/comments.
func hasTrailingStatement(query string) bool {
	i := 0
	for i < len(query) {
		c := query[i]
		switch {
		case c == '\'':
			i = skipQuoted(query, i, '\'')
		case c == '"':
			i = skipQuoted(query, i, '"')
		case c == '-' && strings.HasPrefix(query[i:], "--"):
			idx := strings.IndexByte(query[i:], '\n')
			if idx < 0 {
				return false
			}
			i += idx + 1
		case c == '/' && strings.HasPrefix(query[i:], "/*"):
			depth := 1
			i += 2
			for i < len(query) && depth > 0 {
				switch {
				case strings.HasPrefix(query[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(query[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
		case c == ';':
			rest, ok := skipCommentsAndSpace(query[i+1:])
			if !ok {
				return true
			}
			return strings.TrimSpace(rest) != ""
		default:
			i++
		}
	}
	return false
}

// splitTopLevel splits query on top-level semicolons (outside string literals,
// quoted identifiers, and comments), using the same scanning rules as
// hasTrailingStatement so it stays in sync with the rest of this package. The
// returned segments do NOT include the separating semicolons; a trailing empty
// segment (query ending in ';') is omitted.
func splitTopLevel(query string) []string {
	var segments []string
	start := 0
	i := 0
	for i < len(query) {
		c := query[i]
		switch {
		case c == '\'':
			i = skipQuoted(query, i, '\'')
		case c == '"':
			i = skipQuoted(query, i, '"')
		case c == '-' && strings.HasPrefix(query[i:], "--"):
			idx := strings.IndexByte(query[i:], '\n')
			if idx < 0 {
				i = len(query)
			} else {
				i += idx + 1
			}
		case c == '/' && strings.HasPrefix(query[i:], "/*"):
			depth := 1
			i += 2
			for i < len(query) && depth > 0 {
				switch {
				case strings.HasPrefix(query[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(query[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
		case c == ';':
			segments = append(segments, query[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	if start < len(query) {
		segments = append(segments, query[start:])
	}
	return segments
}

// skipQuoted returns the index just past a quoted region starting at start
// (where query[start] == quote). Doubled quotes are escapes.
func skipQuoted(query string, start int, quote byte) int {
	i := start + 1
	for i < len(query) {
		if query[i] == quote {
			if i+1 < len(query) && query[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

type tokenizer struct {
	s string
	i int
}

func newTokenizer(s string) *tokenizer { return &tokenizer{s: s} }

func (t *tokenizer) skipSpace() {
	rest, ok := skipCommentsAndSpace(t.s[t.i:])
	if !ok {
		t.i = len(t.s)
		return
	}
	t.i = len(t.s) - len(rest)
}

// eatKeyword consumes the given keyword (case-insensitive, word-bounded).
func (t *tokenizer) eatKeyword(kw string) bool {
	t.skipSpace()
	if len(t.s)-t.i < len(kw) {
		return false
	}
	if !strings.EqualFold(t.s[t.i:t.i+len(kw)], kw) {
		return false
	}
	end := t.i + len(kw)
	if end < len(t.s) && isIdentChar(t.s[end]) {
		return false
	}
	t.i = end
	return true
}

// eatIdentifier consumes a bare or double-quoted identifier.
func (t *tokenizer) eatIdentifier() (string, bool) {
	t.skipSpace()
	if t.i >= len(t.s) {
		return "", false
	}
	if t.s[t.i] == '"' {
		end := skipQuoted(t.s, t.i, '"')
		if end <= t.i+1 || t.s[end-1] != '"' {
			return "", false
		}
		name := strings.ReplaceAll(t.s[t.i+1:end-1], `""`, `"`)
		if name == "" {
			return "", false
		}
		t.i = end
		return name, true
	}
	if !isIdentStart(t.s[t.i]) {
		return "", false
	}
	start := t.i
	for t.i < len(t.s) && isIdentChar(t.s[t.i]) {
		t.i++
	}
	return t.s[start:t.i], true
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}
