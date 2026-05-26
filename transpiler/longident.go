package transpiler

import (
	"strconv"
	"strings"
)

// maxIdentifierBytes is PostgreSQL's identifier length limit (NAMEDATALEN - 1).
//
// libpg_query (the C parser behind pg_query.Parse) inherits PostgreSQL's
// scanner, which silently truncates any identifier longer than this to its
// first 63 bytes. DuckDB has no such limit, so a Parse -> Deparse round trip
// renames e.g. a 65-character table to its 63-character prefix and the
// subsequent lookup fails with "Table with name ... does not exist".
//
// We work around this by swapping over-long identifiers for short, unique
// placeholders before parsing and restoring them after deparsing, so the
// truncation never touches a real name.
const maxIdentifierBytes = 63

// identReplacement records one long identifier that was swapped for a short
// placeholder before parsing, so it can be restored after deparsing.
type identReplacement struct {
	placeholder string // short, parse-safe stand-in (a bare lowercase identifier)
	original    string // verbatim source token, including surrounding double quotes if quoted
}

// protectLongIdentifiers scans sql and replaces every identifier whose value
// exceeds maxIdentifierBytes with a unique short placeholder. It returns the
// rewritten SQL together with the replacements needed to undo the swap.
//
// The scan is SQL-aware: it skips string literals (single-quoted, E-strings,
// dollar-quoted), comments, and parameter placeholders so that only genuine
// identifiers (bare or double-quoted) are considered. Identical source tokens
// reuse the same placeholder.
//
// When sql contains no over-long identifier, the input is returned unchanged
// and the replacement slice is nil, making the common path a no-op.
func protectLongIdentifiers(sql string) (string, []identReplacement) {
	// An identifier can only exceed the limit if the whole string does.
	if len(sql) <= maxIdentifierBytes {
		return sql, nil
	}

	// Choose a placeholder prefix that does not already occur in the input so
	// that restoration (a plain string replace) cannot collide with existing
	// text. The trailing underscore on each placeholder (added below) keeps
	// placeholders from being prefixes of one another (e.g. "..._1_" is not a
	// substring of "..._10_").
	prefix := "dglongident_"
	for strings.Contains(sql, prefix) {
		prefix = "_" + prefix
	}

	var b strings.Builder
	b.Grow(len(sql))
	var repls []identReplacement
	seen := make(map[string]string) // original token -> placeholder
	counter := 0

	placeholderFor := func(orig string) string {
		if ph, ok := seen[orig]; ok {
			return ph
		}
		ph := prefix + strconv.Itoa(counter) + "_"
		counter++
		seen[orig] = ph
		repls = append(repls, identReplacement{placeholder: ph, original: orig})
		return ph
	}

	n := len(sql)
	i := 0
	for i < n {
		ch := sql[i]
		switch {
		case ch == '-' && i+1 < n && sql[i+1] == '-':
			// Line comment: skip to end of line.
			j := i + 2
			for j < n && sql[j] != '\n' {
				j++
			}
			b.WriteString(sql[i:j])
			i = j

		case ch == '/' && i+1 < n && sql[i+1] == '*':
			// Block comment (nestable in PostgreSQL).
			depth := 1
			j := i + 2
			for j < n && depth > 0 {
				if j+1 < n && sql[j] == '/' && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if j+1 < n && sql[j] == '*' && sql[j+1] == '/' {
					depth--
					j += 2
					continue
				}
				j++
			}
			b.WriteString(sql[i:j])
			i = j

		case ch == '\'':
			// Standard string literal ('' is an escaped quote).
			j := consumeQuoted(sql, i, false)
			b.WriteString(sql[i:j])
			i = j

		case ch == '$':
			// Dollar-quoted string ($tag$...$tag$) or parameter ($1).
			if tag, ok := dollarQuoteTag(sql, i); ok {
				delim := "$" + tag + "$"
				rest := sql[i+len(delim):]
				if end := strings.Index(rest, delim); end >= 0 {
					stop := i + len(delim) + end + len(delim)
					b.WriteString(sql[i:stop])
					i = stop
				} else {
					// Unterminated: treat the remainder as opaque.
					b.WriteString(sql[i:])
					i = n
				}
			} else {
				b.WriteByte(ch)
				i++
			}

		case ch == '"':
			// Double-quoted identifier ("" is an escaped quote).
			end, value := consumeQuotedIdent(sql, i)
			tok := sql[i:end]
			if len(value) > maxIdentifierBytes {
				b.WriteString(placeholderFor(tok))
			} else {
				b.WriteString(tok)
			}
			i = end

		case isIdentStart(ch):
			j := i + 1
			for j < n && isIdentCont(sql[j]) {
				j++
			}
			tok := sql[i:j]
			// "E'...'" / "e'...'" introduce a backslash-escaped string. Emit the
			// introducer, then consume the following string with backslash escapes.
			if (tok == "E" || tok == "e") && j < n && sql[j] == '\'' {
				b.WriteString(tok)
				end := consumeQuoted(sql, j, true)
				b.WriteString(sql[j:end])
				i = end
				continue
			}
			if len(tok) > maxIdentifierBytes {
				b.WriteString(placeholderFor(tok))
			} else {
				b.WriteString(tok)
			}
			i = j

		default:
			b.WriteByte(ch)
			i++
		}
	}

	if len(repls) == 0 {
		return sql, nil
	}
	return b.String(), repls
}

// restoreLongIdentifiers reverses protectLongIdentifiers, substituting each
// placeholder in sql back to its original token text. It is a no-op when repls
// is empty.
//
// Restoration only replaces a placeholder where it stands alone as a token,
// i.e. where it is not flanked by identifier characters. This matters because a
// transform may derive a NEW identifier from a protected one — e.g. the
// writable-CTE transform builds a temp table name "_cte_<placeholder>_<hex>".
// Such a derived name embeds the placeholder inside a larger token; replacing it
// there would splice the original (possibly quoted) text into the middle of an
// identifier and produce invalid SQL. Leaving it as the (still unique, still
// valid) placeholder keeps the generated name self-consistent across the
// CREATE/SELECT/DROP statements that reference it.
func restoreLongIdentifiers(sql string, repls []identReplacement) string {
	if len(repls) == 0 {
		return sql
	}
	for _, r := range repls {
		sql = replaceStandaloneToken(sql, r.placeholder, r.original)
	}
	return sql
}

// replaceStandaloneToken replaces every occurrence of tok in s with repl, but
// only where tok is not adjacent to an identifier-continuation byte on either
// side (so it is a whole token rather than a substring of a larger identifier).
// A surrounding double quote counts as a boundary, so a deparser that quotes the
// placeholder ("tok") still matches.
func replaceStandaloneToken(s, tok, repl string) string {
	if tok == "" {
		return s
	}
	var b strings.Builder
	for {
		idx := strings.Index(s, tok)
		if idx < 0 {
			b.WriteString(s)
			break
		}
		after := idx + len(tok)
		beforeOK := idx == 0 || !isIdentCont(s[idx-1])
		afterOK := after >= len(s) || !isIdentCont(s[after])
		if beforeOK && afterOK {
			b.WriteString(s[:idx])
			b.WriteString(repl)
		} else {
			// Embedded in a larger identifier — leave it untouched.
			b.WriteString(s[:after])
		}
		s = s[after:]
	}
	return b.String()
}

// IdentReplacement is the exported form of identReplacement, returned by
// ProtectLongIdentifiers for callers outside this package (e.g. the server's
// multi-statement path) that parse and deparse SQL directly.
type IdentReplacement = identReplacement

// ProtectLongIdentifiers exposes protectLongIdentifiers to other packages.
func ProtectLongIdentifiers(sql string) (string, []IdentReplacement) {
	return protectLongIdentifiers(sql)
}

// RestoreLongIdentifiers exposes restoreLongIdentifiers to other packages.
func RestoreLongIdentifiers(sql string, repls []IdentReplacement) string {
	return restoreLongIdentifiers(sql, repls)
}

// restoreLongIdentifiersAll applies restoreLongIdentifiers to each statement in
// the slice, returning a new slice (or the input unchanged when repls is empty).
func restoreLongIdentifiersAll(stmts []string, repls []identReplacement) []string {
	if len(repls) == 0 || len(stmts) == 0 {
		return stmts
	}
	out := make([]string, len(stmts))
	for i, s := range stmts {
		out[i] = restoreLongIdentifiers(s, repls)
	}
	return out
}

// consumeQuoted returns the index just past a single-quoted string that starts
// at the single quote sql[start]. A pair of adjacent single quotes is an
// embedded quote, not a terminator. When escBackslash is true (E-strings), a
// backslash escapes the next byte.
func consumeQuoted(sql string, start int, escBackslash bool) int {
	n := len(sql)
	j := start + 1
	for j < n {
		c := sql[j]
		if c == '\\' && escBackslash {
			j += 2
			continue
		}
		if c == '\'' {
			if j+1 < n && sql[j+1] == '\'' {
				j += 2
				continue
			}
			return j + 1
		}
		j++
	}
	return n // unterminated
}

// consumeQuotedIdent parses a double-quoted identifier starting at
// sql[start] == '"'. It returns the index just past the closing quote and the
// decoded identifier value (with "" collapsed to a single ").
func consumeQuotedIdent(sql string, start int) (end int, value string) {
	n := len(sql)
	var v strings.Builder
	j := start + 1
	for j < n {
		if sql[j] == '"' {
			if j+1 < n && sql[j+1] == '"' {
				v.WriteByte('"')
				j += 2
				continue
			}
			j++ // consume closing quote
			break
		}
		v.WriteByte(sql[j])
		j++
	}
	return j, v.String()
}

// dollarQuoteTag reports whether sql[start] == '$' begins a dollar-quote
// opening delimiter ($$ or $tag$) and, if so, returns the tag (possibly empty).
// A "$" followed by a digit (a parameter like $1) is not a dollar quote.
func dollarQuoteTag(sql string, start int) (tag string, ok bool) {
	n := len(sql)
	j := start + 1
	if j < n && sql[j] == '$' {
		return "", true // "$$"
	}
	// The tag follows identifier rules but, unlike a normal identifier, it
	// cannot contain a dollar sign - the next '$' closes the opening delimiter.
	if j >= n || !isIdentStart(sql[j]) {
		return "", false
	}
	j++
	for j < n && isIdentCont(sql[j]) && sql[j] != '$' {
		j++
	}
	if j < n && sql[j] == '$' {
		return sql[start+1 : j], true
	}
	return "", false
}

// isIdentStart reports whether b can begin an SQL identifier. Bytes >= 0x80
// (multibyte/Unicode identifier characters) are permitted.
func isIdentStart(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		b >= 0x80
}

// isIdentCont reports whether b can continue an SQL identifier.
func isIdentCont(b byte) bool {
	return isIdentStart(b) ||
		(b >= '0' && b <= '9') ||
		b == '$'
}
