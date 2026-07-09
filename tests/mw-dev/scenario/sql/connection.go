package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type ConnectionConfig struct {
	OrgID           string
	SNISuffix       string
	HostAddr        string
	Port            int
	Database        string
	Username        string
	Password        string
	SSLMode         string
	ConnectTimeout  int
	ApplicationName string
}

func (c ConnectionConfig) DSN() (string, error) {
	if c.OrgID == "" {
		return "", classified(ErrorClassConfig, fmt.Errorf("org id is required for SQL connection"))
	}
	if c.SNISuffix == "" {
		return "", classified(ErrorClassConfig, fmt.Errorf("SNI suffix is required for SQL connection"))
	}
	if !strings.HasPrefix(c.SNISuffix, ".") {
		return "", classified(ErrorClassConfig, fmt.Errorf("SNI suffix must start with a dot"))
	}
	if c.HostAddr == "" {
		return "", classified(ErrorClassConfig, fmt.Errorf("PG host address is required for SQL connection"))
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.Database == "" {
		c.Database = "ducklake"
	}
	if c.Username == "" {
		c.Username = "root"
	}
	if c.SSLMode == "" {
		c.SSLMode = "require"
	}

	values := [][2]string{
		{"host", c.OrgID + c.SNISuffix},
		{"hostaddr", c.HostAddr},
		{"port", strconv.Itoa(c.Port)},
		{"user", c.Username},
		{"password", c.Password},
		{"dbname", c.Database},
		{"sslmode", c.SSLMode},
	}
	if c.ConnectTimeout > 0 {
		values = append(values, [2]string{"connect_timeout", strconv.Itoa(c.ConnectTimeout)})
	}
	if c.ApplicationName != "" {
		values = append(values, [2]string{"application_name", c.ApplicationName})
	}

	parts := make([]string, 0, len(values))
	for _, kv := range values {
		parts = append(parts, kv[0]+"="+quoteConninfoValue(kv[1]))
	}
	return strings.Join(parts, " "), nil
}

func quoteConninfoValue(value string) string {
	if value == "" {
		return "''"
	}
	if !strings.ContainsAny(value, " \t\n\r'\\") {
		return value
	}
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}
