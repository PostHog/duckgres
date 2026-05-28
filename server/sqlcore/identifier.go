package sqlcore

import "strings"

// ParseQualifiedIdentifier splits a one-or-more-part SQL identifier, preserving
// quoted identifier case and lowercasing unquoted identifiers.
func ParseQualifiedIdentifier(ref string) ([]string, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, false
	}

	var parts []string
	inQuotes := false
	partStart := 0
	for i := 0; i < len(ref); i++ {
		ch := ref[i]
		if ch == '"' {
			if inQuotes && i+1 < len(ref) && ref[i+1] == '"' {
				i++
				continue
			}
			inQuotes = !inQuotes
			continue
		}
		if ch == '.' && !inQuotes {
			part, ok := parseIdentifierPart(ref[partStart:i])
			if !ok {
				return nil, false
			}
			parts = append(parts, part)
			partStart = i + 1
		}
	}
	if inQuotes {
		return nil, false
	}

	part, ok := parseIdentifierPart(ref[partStart:])
	if !ok {
		return nil, false
	}
	parts = append(parts, part)
	return parts, true
}

// CatalogFromSearchPath returns the catalog from the first search_path entry,
// or "" when the first entry is an unqualified schema.
func CatalogFromSearchPath(searchPath string) string {
	entry := firstSearchPathEntry(searchPath)
	if entry == "" {
		return ""
	}
	parts, ok := ParseQualifiedIdentifier(entry)
	if !ok || len(parts) < 2 {
		return ""
	}
	return parts[0]
}

func QuoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func firstSearchPathEntry(searchPath string) string {
	searchPath = strings.TrimSpace(searchPath)
	inQuotes := false
	for i := 0; i < len(searchPath); i++ {
		ch := searchPath[i]
		if ch == '"' {
			if inQuotes && i+1 < len(searchPath) && searchPath[i+1] == '"' {
				i++
				continue
			}
			inQuotes = !inQuotes
			continue
		}
		if ch == ',' && !inQuotes {
			return strings.TrimSpace(searchPath[:i])
		}
	}
	return searchPath
}

func parseIdentifierPart(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	if s[0] != '"' {
		if strings.ContainsAny(s, "\" \t\n\r\f") {
			return "", false
		}
		return strings.ToLower(s), true
	}
	if len(s) < 2 || s[len(s)-1] != '"' {
		return "", false
	}

	var ident strings.Builder
	for i := 1; i < len(s)-1; i++ {
		if s[i] == '"' {
			if i+1 >= len(s)-1 || s[i+1] != '"' {
				return "", false
			}
			ident.WriteByte('"')
			i++
			continue
		}
		ident.WriteByte(s[i])
	}
	return ident.String(), true
}
