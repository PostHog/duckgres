package core

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

var envTemplatePattern = regexp.MustCompile(`\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}`)

// ResolveEnvTemplates replaces ${env:NAME} placeholders without exposing values in errors.
func ResolveEnvTemplates(text string) (string, error) {
	var missing []string
	out := envTemplatePattern.ReplaceAllStringFunc(text, func(match string) string {
		parts := envTemplatePattern.FindStringSubmatch(match)
		key := parts[1]
		value := os.Getenv(key)
		if value == "" {
			missing = append(missing, key)
			return match
		}
		return value
	})
	if len(missing) != 0 {
		return "", fmt.Errorf("missing required env template value(s): %s", strings.Join(missing, ", "))
	}
	return out, nil
}
