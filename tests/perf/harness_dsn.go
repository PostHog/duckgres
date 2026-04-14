package perf

import (
	"fmt"
	"net/url"
	"strings"
)

func pgwireDSNForHarness(dsn, password string) (string, error) {
	if password == "" {
		return dsn, nil
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse pgwire dsn: %w", err)
		}
		if parsed.User != nil {
			if _, hasPassword := parsed.User.Password(); hasPassword {
				return dsn, nil
			}
			parsed.User = url.UserPassword(parsed.User.Username(), password)
			return parsed.String(), nil
		}
		parsed.User = url.UserPassword("", password)
		return parsed.String(), nil
	}
	if strings.Contains(dsn, "password=") {
		return dsn, nil
	}
	escaped := strings.ReplaceAll(password, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return fmt.Sprintf("%s password='%s'", dsn, escaped), nil
}
