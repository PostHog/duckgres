package server

import (
	"regexp"
	"strings"
)

// ParseStartupOptions parses the Postgres startup-message `options` parameter,
// which carries `-c name=value` GUC settings (also accepted: `-cname=value` and
// `--name=value`), e.g. `-c search_path=ducklake.main`. libpq's `options`
// connection keyword, the PGOPTIONS env var, and pgjdbc's `currentSchema` all
// arrive here. Values may contain backslash-escaped spaces. Returns a map of
// setting name -> value (later settings win on duplicate names).
func ParseStartupOptions(options string) map[string]string {
	out := map[string]string{}
	if strings.TrimSpace(options) == "" {
		return out
	}
	tokens := splitStartupOptions(options)
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		var kv string
		switch {
		case strings.HasPrefix(tok, "-c"):
			kv = tok[2:]
			if kv == "" && i+1 < len(tokens) { // space-separated form: `-c name=value`
				i++
				kv = tokens[i]
			}
		case strings.HasPrefix(tok, "--"):
			kv = tok[2:]
		default:
			continue
		}
		if eq := strings.IndexByte(kv, '='); eq > 0 {
			name := strings.TrimSpace(kv[:eq])
			val := strings.TrimSpace(kv[eq+1:])
			if name != "" {
				out[name] = val
			}
		}
	}
	return out
}

// splitStartupOptions splits on unescaped whitespace, honoring Postgres'
// backslash escaping of spaces within a value (e.g. `search_path=a,\ b`).
func splitStartupOptions(s string) []string {
	var tokens []string
	var b strings.Builder
	escaped := false
	for _, r := range s {
		switch {
		case escaped:
			b.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == ' ' || r == '\t':
			if b.Len() > 0 {
				tokens = append(tokens, b.String())
				b.Reset()
			}
		default:
			b.WriteRune(r)
		}
	}
	if b.Len() > 0 {
		tokens = append(tokens, b.String())
	}
	return tokens
}

// safeSearchPathPattern permits a comma-separated list of (optionally
// double-quoted, optionally catalog-qualified) identifiers plus surrounding
// whitespace. It deliberately excludes single quotes, semicolons, and
// parentheses so the value cannot break out of the single-quoted SET statement
// it is embedded in.
//
// In the class `["\- ]`, `\-` is an ESCAPED LITERAL hyphen (RE2), not a range —
// so the allowed punctuation is exactly `.` `,` `"` `-` and space. `'` and `;`
// are NOT admitted (locked down in TestSanitizeSearchPath). Keep `\-` escaped if
// you edit this: an unescaped `-` between class members would form a range.
var safeSearchPathPattern = regexp.MustCompile(`^[A-Za-z0-9_.,"\- ]+$`)

// SanitizeSearchPath validates a client-supplied search_path so it can be
// safely embedded in `SET search_path = '<value>'`. Returns (trimmed, true)
// when the value is a plausible, injection-safe search_path; ("", false)
// otherwise (callers should then fall back to the default search_path).
func SanitizeSearchPath(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" || len(s) > 512 {
		return "", false
	}
	if !safeSearchPathPattern.MatchString(s) {
		return "", false
	}
	return s, true
}
