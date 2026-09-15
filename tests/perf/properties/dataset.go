// Package properties prepares the generated properties dataset for performance runs.
package properties

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// File identifies a Parquet object selected for a run.
type File struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

// Dataset selects the Parquet directory and its current object inventory.
type Dataset struct {
	Prefix string
	Files  []File
	SHA256 string
}

func s3URL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.Path == "" || strings.ContainsAny(u.Path, "\x00\r\n*?[]") {
		return nil, fmt.Errorf("expected plain s3://bucket/prefix/ URI")
	}
	return u, nil
}
func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

// SetupSQL registers the supported JSON/STRUCT projection for Duckgres.
func (m *Dataset) SetupSQL() string {
	u, _ := s3URL(m.Prefix)
	files := make([]string, len(m.Files))
	for i, f := range m.Files {
		files[i] = literal("s3://" + u.Host + "/" + f.Key)
	}
	var b strings.Builder
	b.WriteString("CREATE SCHEMA IF NOT EXISTS properties_perf;\nBEGIN TRANSACTION;\n")
	b.WriteString("DROP TABLE IF EXISTS properties_perf.events_supported;\nCREATE TABLE properties_perf.events_supported (event VARCHAR, timestamp TIMESTAMPTZ, properties VARCHAR, properties_typed STRUCT(\"$browser\" VARCHAR));\n")
	fmt.Fprintf(&b, "CALL ducklake_add_data_files('ducklake', 'events_supported', [%s], schema => 'properties_perf', ignore_extra_columns => true);\n", strings.Join(files, ", "))
	b.WriteString("COMMIT;\n")
	return b.String()
}

// AthenaSQL exposes only fields supported by the Athena Parquet reader.
func (m *Dataset) AthenaSQL(table string) (string, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(table) {
		return "", fmt.Errorf("athena table must be a lowercase SQL identifier")
	}
	return fmt.Sprintf("CREATE EXTERNAL TABLE %s (event string, `timestamp` timestamp, properties string, properties_typed struct<`$browser`:string>) STORED AS PARQUET LOCATION %s;\n", table, literal(m.Prefix)), nil
}
