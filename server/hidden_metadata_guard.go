package server

import (
	"strings"
	"unicode"
)

const (
	hiddenDuckLakeMetadataCatalogPrefix      = "__ducklake_metadata_"
	HiddenDuckLakeMetadataCatalogAccessError = "direct access to DuckLake metadata catalogs is not allowed"
)

// QueryReferencesHiddenDuckLakeMetadataCatalog reports whether user-authored SQL
// names DuckLake's hidden metadata catalog. It intentionally ignores comments
// and string literals so diagnostic text does not trip the access guard.
func QueryReferencesHiddenDuckLakeMetadataCatalog(query string) bool {
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			i = skipSQLSingleQuotedString(query, i+1)
		case '"':
			ident, next := readSQLDoubleQuotedIdentifier(query, i+1)
			if hasHiddenDuckLakeMetadataCatalogPrefix(ident) {
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
				i = skipSQLDollarQuotedString(query, i+len(tag), tag)
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

func skipSQLSingleQuotedString(query string, i int) int {
	for i < len(query) {
		if query[i] == '\'' {
			if i+1 < len(query) && query[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(query)
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

func skipSQLDollarQuotedString(query string, i int, tag string) int {
	if tag == "" {
		return i
	}
	if end := strings.Index(query[i:], tag); end >= 0 {
		return i + end + len(tag)
	}
	return len(query)
}

func isSQLIdentifierStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isSQLIdentifierContinue(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
