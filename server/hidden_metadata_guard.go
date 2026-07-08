package server

import (
	"strings"
	"unicode"
)

const (
	hiddenDuckLakeMetadataCatalogPrefix      = "__ducklake_metadata_"
	HiddenDuckLakeMetadataCatalogAccessError = "direct access to DuckLake metadata catalogs or dynamic SQL functions is not allowed"
)

// QueryReferencesHiddenDuckLakeMetadataCatalog reports whether user-authored SQL
// names DuckLake's hidden metadata catalog, or uses a dynamic SQL function that
// could construct such a reference. It intentionally ignores comments, but it
// does inspect string literals because DuckDB can execute SQL passed to dynamic
// functions such as query().
func QueryReferencesHiddenDuckLakeMetadataCatalog(query string) bool {
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			literal, next := readSQLSingleQuotedString(query, i+1)
			if containsHiddenDuckLakeMetadataCatalogPrefix(literal) {
				return true
			}
			i = next
		case '"':
			ident, next := readSQLDoubleQuotedIdentifier(query, i+1)
			if hasHiddenDuckLakeMetadataCatalogPrefix(ident) {
				return true
			}
			if isDynamicDuckDBSQLFunctionCall(ident, query, next) {
				return true
			}
			i = next
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i = skipSQLLineComment(query, i+2)
			} else {
				i++
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = skipSQLBlockComment(query, i+2)
			} else {
				i++
			}
		case '$':
			if tag, ok := readSQLDollarQuoteTag(query, i); ok {
				literal, next := readSQLDollarQuotedString(query, i+len(tag), tag)
				if containsHiddenDuckLakeMetadataCatalogPrefix(literal) {
					return true
				}
				i = next
			} else {
				i++
			}
		default:
			if isSQLIdentifierStart(rune(query[i])) {
				start := i
				i++
				for i < len(query) && isSQLIdentifierContinue(rune(query[i])) {
					i++
				}
				if hasHiddenDuckLakeMetadataCatalogPrefix(query[start:i]) {
					return true
				}
				if isDynamicDuckDBSQLFunctionCall(query[start:i], query, i) {
					return true
				}
			} else {
				i++
			}
		}
	}
	return false
}

func hasHiddenDuckLakeMetadataCatalogPrefix(identifier string) bool {
	return strings.HasPrefix(strings.ToLower(identifier), hiddenDuckLakeMetadataCatalogPrefix)
}

func containsHiddenDuckLakeMetadataCatalogPrefix(text string) bool {
	return strings.Contains(strings.ToLower(text), hiddenDuckLakeMetadataCatalogPrefix)
}

func isDynamicDuckDBSQLFunctionCall(identifier, query string, i int) bool {
	if !strings.EqualFold(identifier, "query") && !strings.EqualFold(identifier, "query_table") {
		return false
	}
	next := nextSQLNonWhitespaceOrComment(query, i)
	return next < len(query) && query[next] == '('
}

func nextSQLNonWhitespaceOrComment(query string, i int) int {
	for i < len(query) {
		switch {
		case unicode.IsSpace(rune(query[i])):
			i++
		case i+1 < len(query) && query[i] == '-' && query[i+1] == '-':
			i = skipSQLLineComment(query, i+2)
		case i+1 < len(query) && query[i] == '/' && query[i+1] == '*':
			i = skipSQLBlockComment(query, i+2)
		default:
			return i
		}
	}
	return len(query)
}

func readSQLSingleQuotedString(query string, i int) (string, int) {
	var b strings.Builder
	for i < len(query) {
		if query[i] == '\'' {
			if i+1 < len(query) && query[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			return b.String(), i + 1
		}
		b.WriteByte(query[i])
		i++
	}
	return b.String(), len(query)
}

func readSQLDoubleQuotedIdentifier(query string, i int) (string, int) {
	var b strings.Builder
	for i < len(query) {
		if query[i] == '"' {
			if i+1 < len(query) && query[i+1] == '"' {
				b.WriteByte('"')
				i += 2
				continue
			}
			return b.String(), i + 1
		}
		b.WriteByte(query[i])
		i++
	}
	return b.String(), len(query)
}

func skipSQLLineComment(query string, i int) int {
	for i < len(query) && query[i] != '\n' && query[i] != '\r' {
		i++
	}
	return i
}

func skipSQLBlockComment(query string, i int) int {
	for i+1 < len(query) {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(query)
}

func readSQLDollarQuoteTag(query string, i int) (string, bool) {
	if i >= len(query) || query[i] != '$' {
		return "", false
	}
	for j := i + 1; j < len(query); j++ {
		if query[j] == '$' {
			return query[i : j+1], true
		}
		if query[j] != '_' && !unicode.IsLetter(rune(query[j])) && !unicode.IsDigit(rune(query[j])) {
			return "", false
		}
	}
	return "", false
}

func readSQLDollarQuotedString(query string, i int, tag string) (string, int) {
	if tag == "" {
		return "", i
	}
	if end := strings.Index(query[i:], tag); end >= 0 {
		return query[i : i+end], i + end + len(tag)
	}
	return query[i:], len(query)
}

func isSQLIdentifierStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isSQLIdentifierContinue(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
