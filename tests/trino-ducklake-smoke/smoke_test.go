package trino_ducklake_smoke

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	integration "github.com/posthog/duckgres/tests/integration"
)

const (
	trinoImage             = "trinodb/trino:483@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"
	brikkConnectorVersion  = "483-0.2.0"
	brikkConnectorSHA256   = "b8967c47b940c82d357c657c743b703aec7d9523a3ed2213b356ab7424729450"
	trinoStatementEndpoint = "http://127.0.0.1:38080/v1/statement"
	testTimeZone           = "UTC"
)

type trinoError struct {
	Message string `json:"message"`
}

type trinoResponse struct {
	Data    [][]any     `json:"data"`
	NextURI string      `json:"nextUri"`
	Error   *trinoError `json:"error"`
}

type versionArtifact struct {
	TrinoImage               string `json:"trino_image"`
	TrinoVersion             string `json:"trino_version"`
	BrikkConnectorVersion    string `json:"brikk_connector_version"`
	BrikkConnectorSHA256     string `json:"brikk_connector_sha256"`
	DuckDBVersion            string `json:"duckdb_version"`
	DuckLakeExtensionVersion string `json:"ducklake_extension_version"`
	DuckLakeCatalogVersion   string `json:"ducklake_catalog_version"`
}

func TestTrinoDuckLakeSmoke(t *testing.T) {
	setDuckLakeTestTimeZone(t)
	root := repositoryRoot(t)
	composeFile := filepath.Join(root, "tests", "integration", "docker-compose.yml")
	composeUp(t, composeFile, "ducklake-metadata", "minio")
	compose(t, composeFile, "run", "--rm", "--no-deps", "minio-init")
	compose(t, composeFile, "run", "--rm", "--no-deps", "trino-metadata-reader-init")

	cfg := integration.DefaultConfig()
	cfg.SkipPostgres = true
	harness, err := integration.NewTestHarness(cfg)
	if err != nil {
		t.Fatalf("seed DuckLake through Duckgres: %v", err)
	}
	t.Cleanup(func() { _ = harness.Close() })

	// Run after seeding so grants cover the DuckLake metadata tables created by DuckDB.
	compose(t, composeFile, "run", "--rm", "--no-deps", "trino-metadata-reader-init")
	assertMetadataReaderIsReadOnly(t)
	assertS3ReaderCannotWrite(t, composeFile)
	composeUp(t, composeFile, "trino")

	trino := newTrinoClient(30 * time.Second)
	assertTrinoUTC(t, trino)
	assertDiscovery(t, trino)
	assertRepresentativeTypes(t, trino)
	assertQueriesMatchDuckgres(t, harness.DuckgresDB, trino)
	assertChecksumsMatchDuckgres(t, harness.DuckgresDB, trino)
	writeVersionArtifact(t, harness.DuckgresDB, trino)
}

// setDuckLakeTestTimeZone is inherited by the Duckgres child worker that
// materializes DuckLake partitions. Keep it test-scoped: production server
// timezone policy is outside this local interoperability test.
func setDuckLakeTestTimeZone(t *testing.T) {
	t.Helper()
	t.Setenv("TZ", testTimeZone)
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("determine repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func composeUp(t *testing.T, composeFile string, services ...string) {
	t.Helper()
	args := append([]string{"up", "-d", "--wait"}, services...)
	compose(t, composeFile, args...)
}

func compose(t *testing.T, composeFile string, args ...string) {
	t.Helper()
	cmdArgs := append([]string{"compose", "-f", composeFile}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(cmdArgs, " "), err, output)
	}
	if len(output) != 0 {
		t.Logf("docker %s:\n%s", strings.Join(cmdArgs, " "), output)
	}
}

func assertMetadataReaderIsReadOnly(t *testing.T) {
	t.Helper()
	db, err := sql.Open("postgres", "host=127.0.0.1 port=35433 user=trino_reader password=trino-reader dbname=ducklake sslmode=disable")
	if err != nil {
		t.Fatalf("open Trino metadata reader: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		t.Fatalf("connect Trino metadata reader: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM public.ducklake_metadata").Scan(&count); err != nil {
		t.Fatalf("Trino metadata reader cannot read DuckLake metadata: %v", err)
	}
	if _, err := db.Exec("INSERT INTO public.ducklake_metadata(key, value) VALUES ('smoke_write_probe', 'blocked')"); err == nil {
		t.Fatal("Trino metadata reader unexpectedly wrote DuckLake metadata")
	}
}

func assertS3ReaderCannotWrite(t *testing.T, composeFile string) {
	t.Helper()
	cmdArgs := []string{
		"compose", "-f", composeFile, "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "minio-init", "-ec",
		"mc alias set reader http://minio:9000 trino-reader trino-reader && " +
			"printf blocked | mc pipe reader/ducklake/data/trino-smoke-write-probe",
	}
	cmd := exec.Command("docker", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Trino S3 reader unexpectedly wrote an object:\n%s", output)
	}
}

func assertDiscovery(t *testing.T, trino *trinoClient) {
	t.Helper()
	schemas := trino.query(t, "SHOW SCHEMAS FROM ducklake")
	if !containsCell(schemas, "main") {
		t.Fatalf("DuckLake main schema not discovered: %v", schemas)
	}
	tables := trino.query(t, "SHOW TABLES FROM ducklake.main")
	for _, table := range []string{"users", "products", "orders", "types_test"} {
		if !containsCell(tables, table) {
			t.Fatalf("DuckLake table %q not discovered: %v", table, tables)
		}
	}
}

func assertRepresentativeTypes(t *testing.T, trino *trinoClient) {
	t.Helper()
	rows := trino.query(t, "DESCRIBE ducklake.main.types_test")
	actual := make(map[string]string, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		actual[fmt.Sprint(row[0])] = strings.ToLower(fmt.Sprint(row[1]))
	}
	for column, wantType := range map[string]string{
		"bool_col":      "boolean",
		"int4_col":      "integer",
		"numeric_col":   "decimal(12,4)",
		"date_col":      "date",
		"timestamp_col": "timestamp(6)",
		"uuid_col":      "uuid",
		"bytea_col":     "varbinary",
	} {
		if got := actual[column]; got != wantType {
			t.Errorf("type for %s = %q, want %q (all types: %v)", column, got, wantType, actual)
		}
	}
}

func assertQueriesMatchDuckgres(t *testing.T, duckgres *sql.DB, trino *trinoClient) {
	t.Helper()
	queries := []string{
		"SELECT id, name FROM ducklake.main.users WHERE active = true ORDER BY id",
		"SELECT COUNT(*), SUM(id * age) FROM ducklake.main.users WHERE active = true",
		"SELECT u.name, p.name, o.quantity FROM ducklake.main.orders o JOIN ducklake.main.users u ON u.id = o.user_id JOIN ducklake.main.products p ON p.id = o.product_id WHERE o.status = 'completed' ORDER BY o.id",
		"SELECT COUNT(*) FROM ducklake.main.types_test WHERE bool_col = true AND int4_col = 100 AND numeric_col = 123.4567 AND uuid_col = UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
		"SELECT COUNT(*) FROM ducklake.main.types_test WHERE date_col = DATE '2024-01-15' AND timestamp_col = TIMESTAMP '2024-01-15 12:30:45'",
	}
	for _, query := range queries {
		query := query
		t.Run(query, func(t *testing.T) {
			want := duckgresRows(t, duckgres, query)
			got := canonicalTrinoRows(trino.query(t, query))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Trino result differs from Duckgres\nquery: %s\nTrino: %v\nDuckgres: %v", query, got, want)
			}
		})
	}
}

func assertChecksumsMatchDuckgres(t *testing.T, duckgres *sql.DB, trino *trinoClient) {
	t.Helper()
	for _, query := range []string{
		"SELECT id, name, active, age FROM ducklake.main.users ORDER BY id",
		"SELECT id, user_id, product_id, quantity, status FROM ducklake.main.orders ORDER BY id",
	} {
		want := checksumRows(duckgresRows(t, duckgres, query))
		got := checksumRows(canonicalTrinoRows(trino.query(t, query)))
		if got != want {
			t.Fatalf("Trino checksum differs from Duckgres\\nquery: %s\\nTrino: %s\\nDuckgres: %s", query, got, want)
		}
	}
}

func checksumRows(rows [][]string) string {
	hash := sha256.New()
	for _, row := range rows {
		_, _ = io.WriteString(hash, strings.Join(row, "\\x1f"))
		_, _ = io.WriteString(hash, "\\x1e")
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func duckgresRows(t *testing.T, db *sql.DB, query string) [][]string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Duckgres query %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Duckgres columns: %v", err)
	}
	var result [][]string
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatalf("Duckgres scan: %v", err)
		}
		row := make([]string, len(values))
		for i, value := range values {
			row[i] = canonicalValue(value)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Duckgres rows: %v", err)
	}
	return result
}

func canonicalTrinoRows(rows [][]any) [][]string {
	result := make([][]string, len(rows))
	for i, values := range rows {
		result[i] = make([]string, len(values))
		for j, value := range values {
			result[i][j] = canonicalValue(value)
		}
	}
	return result
}

func canonicalValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "<NULL>"
	case []byte:
		return string(v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func containsCell(rows [][]any, want string) bool {
	for _, row := range rows {
		for _, value := range row {
			if fmt.Sprint(value) == want {
				return true
			}
		}
	}
	return false
}

type trinoClient struct {
	endpoint   string
	httpClient *http.Client
	timeZone   string
}

func newTrinoClient(timeout time.Duration) *trinoClient {
	return &trinoClient{
		endpoint:   trinoStatementEndpoint,
		httpClient: &http.Client{Timeout: timeout},
		timeZone:   testTimeZone,
	}
}

func (c *trinoClient) setHeaders(req *http.Request) {
	req.Header.Set("X-Trino-User", "smoke")
	req.Header.Set("X-Trino-Catalog", "ducklake")
	req.Header.Set("X-Trino-Schema", "main")
	req.Header.Set("X-Trino-Time-Zone", c.timeZone)
}

func (c *trinoClient) query(t *testing.T, statement string) [][]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, c.endpoint, strings.NewReader(statement))
	if err != nil {
		t.Fatalf("create Trino request: %v", err)
	}
	c.setHeaders(req)
	response := c.do(t, req)
	var rows [][]any
	for {
		rows = append(rows, response.Data...)
		if response.Error != nil {
			t.Fatalf("Trino query %q: %s", statement, response.Error.Message)
		}
		if response.NextURI == "" {
			return rows
		}
		next, err := http.NewRequest(http.MethodGet, response.NextURI, nil)
		if err != nil {
			t.Fatalf("create Trino next request: %v", err)
		}
		c.setHeaders(next)
		response = c.do(t, next)
	}
}

func (c *trinoClient) do(t *testing.T, req *http.Request) trinoResponse {
	t.Helper()
	response, err := c.httpClient.Do(req)
	if err != nil {
		t.Fatalf("Trino request %s %s: %v", req.Method, req.URL, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("Trino request %s %s: status %s: %s", req.Method, req.URL, response.Status, body)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	var decoded trinoResponse
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode Trino response: %v", err)
	}
	return decoded
}

func writeVersionArtifact(t *testing.T, duckgres *sql.DB, trino *trinoClient) {
	t.Helper()
	artifact := versionArtifact{
		TrinoImage:               trinoImage,
		TrinoVersion:             singleTrinoValue(t, trino, "SELECT version()"),
		BrikkConnectorVersion:    brikkConnectorVersion,
		BrikkConnectorSHA256:     brikkConnectorSHA256,
		DuckDBVersion:            singleDuckgresValue(t, duckgres, "SELECT library_version FROM pragma_version()"),
		DuckLakeExtensionVersion: singleDuckgresValue(t, duckgres, "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake' AND loaded"),
		DuckLakeCatalogVersion:   singleMetadataValue(t, "SELECT value FROM public.ducklake_metadata WHERE key = 'version' AND scope IS NULL"),
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatalf("encode version artifact: %v", err)
	}
	dir := os.Getenv("TRINO_DUCKLAKE_SMOKE_ARTIFACT_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	path := filepath.Join(dir, "trino-ducklake-smoke-versions.json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write version artifact: %v", err)
	}
	t.Logf("version artifact: %s\n%s", path, bytes.TrimSpace(data))
}

func singleDuckgresValue(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var value string
	if err := db.QueryRow(query).Scan(&value); err != nil {
		t.Fatalf("Duckgres version query %q: %v", query, err)
	}
	return value
}

func singleMetadataValue(t *testing.T, query string) string {
	t.Helper()
	db, err := sql.Open("postgres", "host=127.0.0.1 port=35433 user=trino_reader password=trino-reader dbname=ducklake sslmode=disable")
	if err != nil {
		t.Fatalf("open Trino metadata reader for version artifact: %v", err)
	}
	defer func() { _ = db.Close() }()
	var value string
	if err := db.QueryRow(query).Scan(&value); err != nil {
		t.Fatalf("DuckLake metadata version query %q: %v", query, err)
	}
	return value
}

func singleTrinoValue(t *testing.T, trino *trinoClient, query string) string {
	t.Helper()
	rows := trino.query(t, query)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("Trino version query %q returned %v", query, rows)
	}
	return canonicalValue(rows[0][0])
}

func assertTrinoUTC(t *testing.T, trino *trinoClient) {
	t.Helper()
	if got := singleTrinoValue(t, trino, "SELECT current_timezone()"); got != testTimeZone {
		t.Fatalf("Trino timezone = %q, want %s", got, testTimeZone)
	}
}

func TestPinnedVersionsAreComplete(t *testing.T) {
	if !strings.HasPrefix(trinoImage, "trinodb/trino:") || !strings.Contains(trinoImage, "@sha256:") {
		t.Fatalf("Trino image must be version-pinned, got %q", trinoImage)
	}
	if brikkConnectorVersion == "" || len(brikkConnectorSHA256) != 64 {
		t.Fatalf("Brikk connector version/checksum must be pinned: version=%q checksum=%q", brikkConnectorVersion, brikkConnectorSHA256)
	}
	checksum := strings.ToLower(brikkConnectorSHA256)
	if checksum != brikkConnectorSHA256 || strings.Trim(checksum, "0123456789abcdef") != "" {
		t.Fatalf("Brikk connector checksum is not lowercase hexadecimal: %q", brikkConnectorSHA256)
	}
}

func TestCanonicalTrinoRows(t *testing.T) {
	got := canonicalTrinoRows([][]any{{json.Number("1"), "Alice", nil}})
	want := [][]string{{"1", "Alice", "<NULL>"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonicalTrinoRows() = %v, want %v", got, want)
	}
}

func TestTrinoClientUsesUTC(t *testing.T) {
	client := newTrinoClient(time.Second)
	req, err := http.NewRequest(http.MethodPost, trinoStatementEndpoint, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	client.setHeaders(req)
	if got := req.Header.Get("X-Trino-Time-Zone"); got != "UTC" {
		t.Fatalf("X-Trino-Time-Zone = %q, want UTC", got)
	}
}
