package wire

import "regexp"

// passwordPattern matches password=<value> or password: <value> with quoted
// or unquoted values.
var passwordPattern = regexp.MustCompile(`(?i)(password\s*[=:]\s*)("[^"]*"|[^\s"]+)`)

// RedactSecrets replaces password=<value> (and password: <value>) patterns
// with password=[REDACTED] for safe logging and error reporting. It handles
// both quoted and unquoted values. It does not currently redact other secret
// types (tokens, keys).
func RedactSecrets(s string) string {
	return passwordPattern.ReplaceAllString(s, "${1}[REDACTED]")
}
