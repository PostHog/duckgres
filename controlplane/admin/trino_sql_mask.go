//go:build kubernetes

package admin

import (
	"strings"
	"unicode/utf8"
)

const (
	trinoMaskedQueryMaxRunes    = 2000
	trinoMaskedQueryUnavailable = "(query text unavailable)"
)

// maskTrinoSQLLiterals returns query with every literal value replaced by `?`
// and every comment removed, so the shape of a statement can be shown to the
// tenant's own users without the values they filtered on.
//
// It is a lexer, not a parser: it only has to find where literals, comments
// and quoted identifiers start and end. Input it cannot lex (an unterminated
// string, comment or quoted identifier) returns a fixed placeholder, because
// a wrong guess about where a literal ends would print part of it.
func maskTrinoSQLLiterals(query string) string {
	var out strings.Builder
	out.Grow(len(query))
	lastWasSpace := true // drops leading whitespace
	emit := func(text string) {
		out.WriteString(text)
		lastWasSpace = false
	}
	space := func() {
		if !lastWasSpace {
			out.WriteByte(' ')
			lastWasSpace = true
		}
	}

	n := len(query)
	for i := 0; i < n; {
		c := query[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			space()
			i++
		case c == '-' && i+1 < n && query[i+1] == '-':
			for i < n && query[i] != '\n' {
				i++
			}
			space()
		case c == '/' && i+1 < n && query[i+1] == '*':
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return trinoMaskedQueryUnavailable
			}
			i += end + 4
			space()
		case c == '\'':
			next, ok := skipSQLQuoted(query, i, '\'')
			if !ok {
				return trinoMaskedQueryUnavailable
			}
			emit("?")
			i = next
		case c == '"' || c == '`':
			next, ok := skipSQLQuoted(query, i, c)
			if !ok {
				return trinoMaskedQueryUnavailable
			}
			emit(query[i:next])
			i = next
		case isSQLDigit(c) || (c == '.' && i+1 < n && isSQLDigit(query[i+1])):
			j := i + 1
			for j < n && isSQLNumberByte(query, j) {
				j++
			}
			emit("?")
			i = j
		case isSQLIdentStart(c):
			j := i + 1
			for j < n && isSQLIdentByte(query[j]) {
				j++
			}
			word := query[i:j]
			// X'..' and U&'..' are literals that carry a prefix.
			literalStart := -1
			switch {
			case (word == "X" || word == "x") && j < n && query[j] == '\'':
				literalStart = j
			case (word == "U" || word == "u") && j+1 < n && query[j] == '&' && query[j+1] == '\'':
				literalStart = j + 1
			}
			if literalStart < 0 {
				emit(word)
				i = j
				continue
			}
			next, ok := skipSQLQuoted(query, literalStart, '\'')
			if !ok {
				return trinoMaskedQueryUnavailable
			}
			emit("?")
			i = next
		default:
			emit(query[i : i+1])
			i++
		}
	}

	masked := strings.TrimRight(out.String(), " ")
	if utf8.RuneCountInString(masked) > trinoMaskedQueryMaxRunes {
		masked = string([]rune(masked)[:trinoMaskedQueryMaxRunes]) + "…"
	}
	return masked
}

// skipSQLQuoted returns the index after the closing quote of the token that
// opens at start. A doubled quote is an escaped quote, not the end.
func skipSQLQuoted(s string, start int, quote byte) (int, bool) {
	for i := start + 1; i < len(s); i++ {
		if s[i] != quote {
			continue
		}
		if i+1 < len(s) && s[i+1] == quote {
			i++
			continue
		}
		return i + 1, true
	}
	return 0, false
}

func isSQLDigit(c byte) bool { return c >= '0' && c <= '9' }

// Bytes at or above 0x80 belong to a multi-byte UTF-8 character. Treating
// them as identifier bytes keeps the character whole.
func isSQLIdentStart(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isSQLIdentByte(c byte) bool {
	return isSQLIdentStart(c) || isSQLDigit(c) || c == '@'
}

// isSQLNumberByte reports whether s[i] continues a numeric literal: digits,
// a decimal point, hex and exponent letters, digit separators, and a sign
// directly after an exponent marker.
func isSQLNumberByte(s string, i int) bool {
	c := s[i]
	if isSQLDigit(c) || c == '.' || c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return true
	}
	return (c == '+' || c == '-') && (s[i-1] == 'e' || s[i-1] == 'E')
}
