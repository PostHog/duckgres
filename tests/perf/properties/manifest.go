// Package properties validates immutable derived-property fixtures before registration.
package properties

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

var requiredChecks = []string{"source_browser_types_all_rows", "original_schema", "original_values_sampled", "derived_values_sampled", "physical_shredding_every_file", "whole_day_row_count", "output_date_bounds", "source_inventory_unchanged"}
var etagPattern = regexp.MustCompile(`^"[a-fA-F0-9]{32}(-[1-9][0-9]*)?"$`)

// File identifies an immutable object in the published inventory.
type File struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

// Manifest retains the checked selection and its provenance digest.
type Manifest struct {
	FormatVersion int    `json:"format_version"`
	Status        string `json:"status"`
	CompletedAt   string `json:"completed_at"`
	Config        struct {
		DestinationPrefix string `json:"destination_prefix"`
		Start             string `json:"start"`
		End               string `json:"end"`
	} `json:"config"`
	Coverage struct {
		Rows            int64            `json:"rows"`
		BrowserTypes    map[string]int64 `json:"browser_types"`
		BrowserNullRows int64            `json:"browser_null_rows"`
		ChromeRows      int64            `json:"chrome_rows"`
	} `json:"coverage"`
	Schema     [][]string `json:"schema"`
	Checks     []string   `json:"checks"`
	Validation struct {
		Mode         string `json:"mode"`
		SampledFiles int64  `json:"sampled_files"`
		SampledRows  int64  `json:"sampled_rows"`
	} `json:"validation"`
	Writer struct {
		Shredding  string `json:"shredding"`
		DuckDB     string `json:"duckdb"`
		CoreCommit string `json:"core_commit"`
		CorePatch  string `json:"core_patch_sha256"`
	} `json:"writer"`
	Files  []File    `json:"outputs"`
	Start  time.Time `json:"-"`
	End    time.Time `json:"-"`
	Rows   int64     `json:"-"`
	SHA256 string    `json:"-"`
}

func Load(name string) (*Manifest, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}
func Parse(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode fixture manifest: %w", err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	m.SHA256 = hex.EncodeToString(digest[:])
	m.Rows = m.Coverage.Rows
	return &m, nil
}
func (m *Manifest) validate() error {
	fail := func(s string) error { return fmt.Errorf("invalid properties fixture: %s", s) }
	if m.FormatVersion != 3 || m.Status != "complete" {
		return fail("requires complete format version 3")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.CompletedAt); err != nil {
		return fail("completion timestamp")
	}
	var err error
	m.Start, err = time.Parse(time.RFC3339, m.Config.Start)
	if err != nil {
		return fail("start timestamp")
	}
	m.End, err = time.Parse(time.RFC3339, m.Config.End)
	if err != nil {
		return fail("end timestamp")
	}
	_, offset := m.Start.Zone()
	_, endOffset := m.End.Zone()
	if offset != 0 || endOffset != 0 || m.Start.Hour() != 0 || m.Start.Minute() != 0 || m.Start.Second() != 0 || m.Start.Nanosecond() != 0 || m.End.Sub(m.Start) != 24*time.Hour {
		return fail("requires exactly one UTC day")
	}
	for _, check := range requiredChecks {
		found := false
		for _, got := range m.Checks {
			found = found || check == got
		}
		if !found {
			return fail("missing validation check " + check)
		}
	}
	if m.Validation.Mode != "sampled_values" || m.Validation.SampledFiles <= 0 || m.Validation.SampledFiles > int64(len(m.Files)) || m.Validation.SampledRows <= 0 || m.Validation.SampledRows > m.Coverage.Rows {
		return fail("sampled validation coverage")
	}
	total := int64(0)
	for kind, n := range m.Coverage.BrowserTypes {
		if (kind != "MISSING" && kind != "NULL" && kind != "VARCHAR") || n < 0 || n > m.Coverage.Rows-total {
			return fail("browser type coverage")
		}
		total += n
	}
	if total != m.Coverage.Rows || total <= 0 || m.Coverage.BrowserNullRows != m.Coverage.BrowserTypes["MISSING"]+m.Coverage.BrowserTypes["NULL"] || m.Coverage.ChromeRows < 0 || m.Coverage.ChromeRows > m.Coverage.BrowserTypes["VARCHAR"] {
		return fail("row coverage")
	}
	fields := map[string]string{}
	for _, field := range m.Schema {
		if len(field) != 2 || field[0] == "" || fields[field[0]] != "" {
			return fail("original schema")
		}
		fields[field[0]] = field[1]
	}
	if fields["event"] != "VARCHAR" || fields["properties"] != "VARCHAR" || fields["timestamp"] != "TIMESTAMP WITH TIME ZONE" {
		return fail("original schema required columns")
	}
	if m.Writer.Shredding != `STRUCT("$browser" VARCHAR)` || m.Writer.DuckDB == "" || !validHex(m.Writer.CoreCommit, 40) || !validHex(m.Writer.CorePatch, 64) {
		return fail("writer provenance and shredding")
	}
	u, err := s3URL(m.Config.DestinationPrefix)
	if err != nil || !strings.HasSuffix(m.Config.DestinationPrefix, "/") {
		return fail("destination prefix")
	}
	prefix := strings.TrimPrefix(u.Path, "/") + "data/"
	seen := map[string]bool{}
	if len(m.Files) == 0 {
		return fail("empty inventory")
	}
	for _, f := range m.Files {
		if !strings.HasPrefix(f.Key, prefix) || path.Clean(f.Key) != f.Key || strings.ContainsAny(f.Key, "\x00\r\n*?[]#%") || !strings.HasSuffix(f.Key, ".parquet") || f.Size <= 0 || !etagPattern.MatchString(f.ETag) || seen[f.Key] {
			return fail("output inventory")
		}
		seen[f.Key] = true
	}
	return nil
}
func validHex(value string, length int) bool {
	_, err := hex.DecodeString(value)
	return len(value) == length && err == nil
}
func s3URL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.Path == "" {
		return nil, fmt.Errorf("expected plain s3://bucket/key URI")
	}
	return u, nil
}
func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

// SetupSQL registers both logical projections from exactly the same objects.
func (m *Manifest) SetupSQL() string {
	u, _ := s3URL(m.Config.DestinationPrefix)
	files := make([]string, len(m.Files))
	for i, f := range m.Files {
		files[i] = literal("s3://" + u.Host + "/" + f.Key)
	}
	var b strings.Builder
	b.WriteString("CREATE SCHEMA IF NOT EXISTS properties_perf;\nBEGIN TRANSACTION;\n")
	for _, table := range []string{"events_supported", "events_variant"} {
		extra := ""
		if table == "events_variant" {
			extra = ", properties_variant VARIANT"
		}
		fmt.Fprintf(&b, "DROP TABLE IF EXISTS properties_perf.%s;\nCREATE TABLE properties_perf.%s (event VARCHAR, timestamp TIMESTAMPTZ, properties VARCHAR, properties_typed STRUCT(\"$browser\" VARCHAR)%s);\n", table, table, extra)
		fmt.Fprintf(&b, "CALL ducklake_add_data_files('ducklake', '%s', [%s], schema => 'properties_perf', ignore_extra_columns => true);\n", table, strings.Join(files, ", "))
	}
	b.WriteString("COMMIT;\n")
	b.WriteString(m.ValidationSQL())
	return b.String()
}

// ValidationSQL performs whole-fixture checks outside timed query execution.
func (m *Manifest) ValidationSQL() string {
	var b strings.Builder
	for _, table := range []string{"events_supported", "events_variant"} {
		fmt.Fprintf(&b, `SELECT CASE WHEN count(*) = %d
 AND count(*) FILTER (WHERE timestamp IS NULL OR timestamp < TIMESTAMPTZ %s OR timestamp >= TIMESTAMPTZ %s) = 0
 AND count(*) FILTER (WHERE coalesce(json_type(properties, '$."$browser"'), 'MISSING') NOT IN ('MISSING','NULL','VARCHAR')) = 0
 AND count(*) FILTER (WHERE coalesce(json_type(properties, '$."$browser"'), 'MISSING') IN ('MISSING','NULL')) = %d
 AND count(*) FILTER (WHERE json_extract_string(properties, '$."$browser"') = 'Chrome') = %d
 THEN true ELSE error('properties fixture coverage mismatch') END FROM properties_perf.%s;
`, m.Rows, literal(m.Config.Start), literal(m.Config.End), m.Coverage.BrowserNullRows, m.Coverage.ChromeRows, table)
	}
	return b.String()
}

// AthenaSQL uses only supported logical fields. Verify the exact prefix inventory first.
func (m *Manifest) AthenaSQL(table string) (string, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(table) {
		return "", fmt.Errorf("athena table must be a lowercase SQL identifier")
	}
	return fmt.Sprintf("CREATE EXTERNAL TABLE %s (event string, `timestamp` timestamp, properties string, properties_typed struct<`$browser`:string>) STORED AS PARQUET LOCATION %s;\n", table, literal(m.Config.DestinationPrefix+"data/")), nil
}
