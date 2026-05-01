package sqlcore

import "strings"

// IsEmptyQuery checks if a query contains only semicolons, whitespace, and/or
// SQL comments. PostgreSQL returns EmptyQueryResponse for queries like ";",
// "-- ping", "/* */", etc.
func IsEmptyQuery(query string) bool {
	stripped := StripLeadingComments(query)
	for _, r := range stripped {
		if r != ';' && r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}

// StripLeadingComments removes leading SQL comments from a query.
// Handles both block comments /* ... */ and line comments -- ...
func StripLeadingComments(query string) string {
	for {
		query = strings.TrimSpace(query)
		if strings.HasPrefix(query, "/*") {
			end := strings.Index(query, "*/")
			if end == -1 {
				return query
			}
			query = query[end+2:]
			continue
		}
		if strings.HasPrefix(query, "--") {
			nl := strings.IndexByte(query, '\n')
			if nl == -1 {
				return ""
			}
			query = query[nl+1:]
			continue
		}
		return query
	}
}
