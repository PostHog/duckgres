package provision

import (
	"encoding/json"
	"regexp"
	"strings"
)

const RedactedValue = "<redacted>"

var (
	authorizationPattern = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)(bearer\s+)?([^&\s",}]+)`)
	sensitiveTextPattern = regexp.MustCompile(`(?i)(password|secret|token|authorization)(["'=:\s]+)([^&\s",}]+)`)
)

func RedactForArtifact(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return RedactedValue
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return RedactedValue
	}
	return redactValue(decoded)
}

func redactHTTPBody(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err == nil {
		redacted, err := json.Marshal(RedactForArtifact(decoded))
		if err == nil {
			return string(redacted)
		}
	}
	return redactSensitiveText(string(raw))
}

func redactValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, value := range typed {
			if isSensitiveKey(k) {
				out[k] = RedactedValue
				continue
			}
			out[k] = redactValue(value)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, value := range typed {
			out[i] = redactValue(value)
		}
		return out
	case string:
		return redactSensitiveText(typed)
	default:
		return typed
	}
}

func isSensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	for _, marker := range []string{"password", "secret", "token", "authorization"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func redactSensitiveText(value string) string {
	value = authorizationPattern.ReplaceAllString(value, `${1}${2}`+RedactedValue)
	return sensitiveTextPattern.ReplaceAllString(value, `${1}${2}`+RedactedValue)
}
