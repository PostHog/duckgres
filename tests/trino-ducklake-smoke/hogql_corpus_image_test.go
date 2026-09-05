package trino_ducklake_smoke

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

const corpusDiagnosticLimit = 25

var corpusSchemaTrinoTypes = map[string]struct{}{
	"array(varchar)": {},
	"bigint":         {},
	"boolean":        {},
	"date":           {},
	"decimal(38,10)": {},
	"double":         {},
	"json":           {},
	"timestamp(6)":   {},
	"uuid":           {},
	"varchar":        {},
}

type corpusAnalysisFailure struct {
	queryID          string
	queryHash        string
	errorName        string
	errorFingerprint string
}

type corpusAnalysisReport struct {
	Total    int                           `json:"total"`
	Passed   int                           `json:"passed"`
	Failures []corpusAnalysisReportFailure `json:"failures"`
}

type corpusAnalysisReportFailure struct {
	QueryID          string `json:"queryId"`
	QueryHash        string `json:"querySha256"`
	ErrorName        string `json:"errorName"`
	ErrorFingerprint string `json:"errorFingerprint"`
}

type corpusAnalysisJob struct {
	queryID    string
	queryHash  string
	query      string
	bindings   map[string]any
	generation int64
}

type corpusSchemaDocument struct {
	Namespaces []corpusSchemaNamespace `json:"namespaces"`
	Tables     []corpusSchemaTable     `json:"tables"`
}

type corpusSchemaNamespace struct {
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`
}

type corpusSchemaTable struct {
	Catalog string               `json:"catalog"`
	Schema  string               `json:"schema"`
	Name    string               `json:"name"`
	Columns []corpusSchemaColumn `json:"columns"`
}

type corpusSchemaColumn struct {
	Name          string `json:"name"`
	TrinoTypeName string `json:"trinoTypeName"`
}

func TestHogQLCorpusImageAnalyze(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("private corpus validation runs only in a local environment")
	}
	image := requiredCorpusEnvironment(t, "TRINO_HOGQL_SMOKE_IMAGE")
	corpusDirectory := requiredCorpusEnvironment(t, "TRINO_HOGQL_CORPUS_DIR")
	snapshotPath := requiredCorpusEnvironment(t, "TRINO_HOGQL_CORPUS_SNAPSHOT")
	image = resolveImmutableImageReference(t, image)
	setDuckLakeTestTimeZone(t)

	snapshot := readCorpusSnapshot(t, snapshotPath)
	semanticCatalog := newSemanticCatalogServer(t, hogQLCatalogToken, snapshot)
	for _, additionalSnapshot := range readAdditionalCorpusSnapshots(t) {
		semanticCatalog.publish(t, additionalSnapshot)
	}
	semanticCatalog.setAvailable(true)
	server := startHostVisibleServer(t, semanticCatalog)
	configDirectory := t.TempDir()
	tokenPath := filepath.Join(configDirectory, "catalog-token")
	writeTestFile(t, tokenPath, hogQLCatalogToken+"\n")
	configPath := filepath.Join(configDirectory, "config.properties")
	writeTestFile(t, configPath, hogQLTrinoConfig(server.Listener.Addr().(*net.TCPAddr).Port, "1m", "30m"))
	schemaDocument := readCorpusSchemaDocument(t)
	catalogDirectory := writeCorpusCatalogs(t, schemaDocument)

	baseURL := startHogQLTrinoContainer(t, image, configPath, tokenPath, catalogDirectory, false)
	client := &imageTrinoClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 2 * time.Minute},
		catalog:    snapshot.Catalog.Value,
		schema:     corpusEnvironment("TRINO_HOGQL_CORPUS_SCHEMA", "public"),
	}
	waitForTrino(t, client)
	seedCorpusMemoryCatalog(t, client, schemaDocument)
	waitForHogQLSuccess(t, client, hogQLRequest("SELECT 1", snapshot.Generation, nil))
	waitForHogQLSuccess(t, client, corpusHogQLRequest("SELECT convertCurrency('USD', 'USD', 100)", snapshot.Generation, nil))

	bindings := readCorpusBindings(t)
	queryPaths := corpusQueryPaths(t, corpusDirectory)
	jobs := make([]corpusAnalysisJob, 0, len(queryPaths))
	for _, queryPath := range queryPaths {
		queryID := strings.TrimSuffix(filepath.Base(queryPath), filepath.Ext(queryPath))
		query, err := os.ReadFile(queryPath)
		if err != nil {
			t.Fatalf("read corpus query %s: %v", queryID, err)
		}
		queryHash := fmt.Sprintf("%x", sha256.Sum256(query))
		jobs = append(jobs, corpusAnalysisJob{
			queryID:    queryID,
			queryHash:  queryHash,
			query:      string(query),
			bindings:   bindings[queryID],
			generation: corpusGeneration(t, snapshot.Generation, bindings[queryID]),
		})
	}

	workerCount := corpusWorkerCount(t, len(jobs))
	jobChannel := make(chan corpusAnalysisJob)
	resultChannel := make(chan *corpusAnalysisFailure, len(jobs))
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobChannel {
				outcome, transportError := executeCorpusHogQL(client, corpusHogQLRequest(job.query, job.generation, job.bindings))
				if transportError != nil {
					failure := corpusFailure(job.queryID, job.queryHash, "TRANSPORT_ERROR", transportError.Error())
					resultChannel <- &failure
					continue
				}
				if outcome.Error != nil {
					failure := corpusFailure(job.queryID, job.queryHash, outcome.Error.ErrorName, outcome.Error.Message)
					resultChannel <- &failure
					continue
				}
				if len(outcome.Rows) == 0 {
					failure := corpusFailure(job.queryID, job.queryHash, "EMPTY_EXPLAIN", "native EXPLAIN returned no rows")
					resultChannel <- &failure
					continue
				}
				resultChannel <- nil
			}
		}()
	}
	go func() {
		for _, job := range jobs {
			jobChannel <- job
		}
		close(jobChannel)
		workers.Wait()
		close(resultChannel)
	}()

	failures := make([]corpusAnalysisFailure, 0)
	for failure := range resultChannel {
		if failure != nil {
			failures = append(failures, *failure)
		}
	}
	sort.Slice(failures, func(left, right int) bool { return failures[left].queryID < failures[right].queryID })
	writeCorpusReport(t, len(queryPaths), failures)

	if len(failures) == 0 {
		t.Logf("native HogQL analysis and EXPLAIN passed for %d/%d hash-addressed corpus queries", len(queryPaths), len(queryPaths))
		return
	}

	errorCounts := make(map[string]int)
	for _, failure := range failures {
		errorCounts[failure.errorName]++
	}
	errorNames := make([]string, 0, len(errorCounts))
	for errorName := range errorCounts {
		errorNames = append(errorNames, errorName)
	}
	sort.Strings(errorNames)
	for _, errorName := range errorNames {
		t.Logf("corpus analysis error %s: %d", errorName, errorCounts[errorName])
	}
	for index, failure := range failures {
		if index == corpusDiagnosticLimit {
			t.Logf("omitting %d additional hash-only diagnostics", len(failures)-corpusDiagnosticLimit)
			break
		}
		t.Logf("corpus analysis failure id=%s query_sha256=%s error=%s fingerprint=%s", failure.queryID, failure.queryHash, failure.errorName, failure.errorFingerprint)
	}
	t.Fatalf("native HogQL analysis and EXPLAIN passed for %d/%d hash-addressed corpus queries", len(queryPaths)-len(failures), len(queryPaths))
}

func requiredCorpusEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Skipf("set %s to run target-schema corpus validation", name)
	}
	return value
}

func corpusEnvironment(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func readCorpusSnapshot(t *testing.T, path string) *hogqlcatalog.HogQLSemanticCatalogSnapshot {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open corpus semantic catalog snapshot: %v", err)
	}
	defer func() { _ = file.Close() }()
	snapshot, err := hogqlcatalog.DecodeSnapshot(file)
	if err != nil {
		t.Fatalf("decode corpus semantic catalog snapshot: %v", err)
	}
	return snapshot
}

func readAdditionalCorpusSnapshots(t *testing.T) []*hogqlcatalog.HogQLSemanticCatalogSnapshot {
	t.Helper()
	rawPaths := strings.TrimSpace(os.Getenv("TRINO_HOGQL_CORPUS_ADDITIONAL_SNAPSHOTS"))
	if rawPaths == "" {
		return nil
	}
	paths := strings.Split(rawPaths, ",")
	snapshots := make([]*hogqlcatalog.HogQLSemanticCatalogSnapshot, 0, len(paths))
	for _, path := range paths {
		snapshots = append(snapshots, readCorpusSnapshot(t, strings.TrimSpace(path)))
	}
	sort.Slice(snapshots, func(left, right int) bool { return snapshots[left].Generation < snapshots[right].Generation })
	return snapshots
}

func writeCorpusCatalogs(t *testing.T, schemaDocument *corpusSchemaDocument) string {
	t.Helper()
	aliases := strings.Split(requiredCorpusEnvironment(t, "TRINO_HOGQL_CORPUS_CATALOG_ALIASES"), ",")
	connector := corpusEnvironment("TRINO_HOGQL_CORPUS_CONNECTOR", "postgresql")
	var properties string
	switch connector {
	case "memory":
		properties = "connector.name=memory\n"
	case "postgresql":
		jdbcURL := requiredCorpusEnvironment(t, "TRINO_HOGQL_CORPUS_JDBC_URL")
		jdbcUser := requiredCorpusEnvironment(t, "TRINO_HOGQL_CORPUS_JDBC_USER")
		jdbcPassword := requiredCorpusEnvironment(t, "TRINO_HOGQL_CORPUS_JDBC_PASSWORD")
		for name, value := range map[string]string{
			"TRINO_HOGQL_CORPUS_JDBC_URL":      jdbcURL,
			"TRINO_HOGQL_CORPUS_JDBC_USER":     jdbcUser,
			"TRINO_HOGQL_CORPUS_JDBC_PASSWORD": jdbcPassword,
		} {
			if strings.ContainsAny(value, "\r\n") {
				t.Fatalf("%s contains a line break", name)
			}
		}
		properties = fmt.Sprintf("connector.name=postgresql\nconnection-url=%s\nconnection-user=%s\nconnection-password=%s\n", jdbcURL, jdbcUser, jdbcPassword)
	default:
		t.Fatalf("unsupported TRINO_HOGQL_CORPUS_CONNECTOR %q", connector)
	}
	if connector == "memory" && schemaDocument != nil {
		for _, namespace := range schemaDocument.Namespaces {
			aliases = append(aliases, namespace.Catalog)
		}
		for _, table := range schemaDocument.Tables {
			aliases = append(aliases, table.Catalog)
		}
	}

	directory := t.TempDir()
	seen := make(map[string]struct{})
	for _, rawAlias := range aliases {
		alias := strings.TrimSpace(rawAlias)
		if !validCatalogAlias(alias) {
			t.Fatalf("invalid corpus catalog alias %q", alias)
		}
		if _, exists := seen[alias]; exists {
			continue
		}
		seen[alias] = struct{}{}
		writeTestFile(t, filepath.Join(directory, alias+".properties"), properties)
	}
	return directory
}

func readCorpusSchemaDocument(t *testing.T) *corpusSchemaDocument {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("TRINO_HOGQL_CORPUS_SCHEMA_DOCUMENT"))
	if path == "" {
		return nil
	}
	if corpusEnvironment("TRINO_HOGQL_CORPUS_CONNECTOR", "postgresql") != "memory" {
		t.Fatal("TRINO_HOGQL_CORPUS_SCHEMA_DOCUMENT requires the ephemeral memory connector")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat corpus schema document: %v", err)
	}
	if info.Size() > 16*1024*1024 {
		t.Fatalf("corpus schema document is %d bytes, limit is 16777216", info.Size())
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus schema document: %v", err)
	}
	var document corpusSchemaDocument
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode corpus schema document: %v", err)
	}
	if len(document.Namespaces) > 10_000 {
		t.Fatalf("corpus schema document contains %d namespaces, expected at most 10000", len(document.Namespaces))
	}
	if len(document.Tables) == 0 || len(document.Tables) > 10_000 {
		t.Fatalf("corpus schema document contains %d tables, expected 1..10000", len(document.Tables))
	}
	return &document
}

func seedCorpusMemoryCatalog(t *testing.T, client *imageTrinoClient, document *corpusSchemaDocument) {
	t.Helper()
	if document == nil {
		return
	}

	createdSchemas := make(map[string]struct{})
	for namespaceIndex, namespace := range document.Namespaces {
		validateCorpusNamespace(t, namespace, fmt.Sprintf("namespace %d", namespaceIndex+1))
		qualifiedSchema := quoteCorpusIdentifier(namespace.Catalog) + "." + quoteCorpusIdentifier(namespace.Schema)
		canonicalSchema := strings.ToLower(qualifiedSchema)
		if _, exists := createdSchemas[canonicalSchema]; exists {
			continue
		}
		requireCorpusSetupSuccess(t, client, namespaceIndex+1, "CREATE SCHEMA IF NOT EXISTS "+qualifiedSchema)
		createdSchemas[canonicalSchema] = struct{}{}
	}

	createdTables := make(map[string]struct{}, len(document.Tables))
	for tableIndex, table := range document.Tables {
		validateCorpusNamespace(t, corpusSchemaNamespace{Catalog: table.Catalog, Schema: table.Schema}, fmt.Sprintf("table %d", tableIndex+1))
		for fieldName, identifier := range map[string]string{"catalog": table.Catalog, "schema": table.Schema, "table": table.Name} {
			if !validCorpusSchemaIdentifier(identifier) {
				t.Fatalf("corpus schema table %d has invalid %s identifier", tableIndex+1, fieldName)
			}
		}
		qualifiedSchema := quoteCorpusIdentifier(table.Catalog) + "." + quoteCorpusIdentifier(table.Schema)
		canonicalSchema := strings.ToLower(qualifiedSchema)
		if _, exists := createdSchemas[canonicalSchema]; !exists {
			requireCorpusSetupSuccess(t, client, len(createdSchemas)+1, "CREATE SCHEMA IF NOT EXISTS "+qualifiedSchema)
			createdSchemas[canonicalSchema] = struct{}{}
		}
		qualifiedTable := qualifiedSchema + "." + quoteCorpusIdentifier(table.Name)
		canonicalTable := strings.ToLower(qualifiedTable)
		if _, exists := createdTables[canonicalTable]; exists {
			t.Fatalf("corpus schema document contains duplicate table %d", tableIndex+1)
		}
		createdTables[canonicalTable] = struct{}{}
		if len(table.Columns) == 0 || len(table.Columns) > 10_000 {
			t.Fatalf("corpus schema table %d contains %d columns, expected 1..10000", tableIndex+1, len(table.Columns))
		}
		columnDefinitions := make([]string, 0, len(table.Columns))
		columnNames := make(map[string]struct{}, len(table.Columns))
		for columnIndex, column := range table.Columns {
			if !validCorpusSchemaIdentifier(column.Name) {
				t.Fatalf("corpus schema table %d column %d has an invalid identifier", tableIndex+1, columnIndex+1)
			}
			canonicalName := strings.ToLower(column.Name)
			if _, exists := columnNames[canonicalName]; exists {
				t.Fatalf("corpus schema table %d has duplicate column %d", tableIndex+1, columnIndex+1)
			}
			columnNames[canonicalName] = struct{}{}
			if _, allowed := corpusSchemaTrinoTypes[column.TrinoTypeName]; !allowed {
				t.Fatalf("corpus schema table %d column %d has unsupported type", tableIndex+1, columnIndex+1)
			}
			columnDefinitions = append(columnDefinitions, quoteCorpusIdentifier(column.Name)+" "+column.TrinoTypeName)
		}
		requireCorpusSetupSuccess(t, client, tableIndex+1, "CREATE TABLE "+qualifiedTable+" ("+strings.Join(columnDefinitions, ", ")+")")
	}
	t.Logf("created %d private synthetic namespaces and %d tables in ephemeral memory catalogs", len(createdSchemas), len(document.Tables))
}

func validateCorpusNamespace(t *testing.T, namespace corpusSchemaNamespace, location string) {
	t.Helper()
	if !validCatalogAlias(namespace.Catalog) {
		t.Fatalf("corpus schema %s has invalid catalog identifier", location)
	}
	if !validCorpusSchemaIdentifier(namespace.Schema) {
		t.Fatalf("corpus schema %s has invalid schema identifier", location)
	}
}

func validCorpusSchemaIdentifier(identifier string) bool {
	if identifier == "" || len(identifier) > 255 {
		return false
	}
	for _, character := range identifier {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func quoteCorpusIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func requireCorpusSetupSuccess(t *testing.T, client *imageTrinoClient, statementIndex int, statement string) {
	t.Helper()
	outcome, transportError := client.executeSQL(statement)
	if transportError != nil {
		t.Fatalf("execute corpus setup statement %d: transport error fingerprint=%x", statementIndex, sha256.Sum256([]byte(transportError.Error())))
	}
	if outcome.Error != nil {
		t.Fatalf("execute corpus setup statement %d: error=%s fingerprint=%x", statementIndex, outcome.Error.ErrorName, sha256.Sum256([]byte(outcome.Error.Message)))
	}
}

func validCatalogAlias(alias string) bool {
	if alias == "" {
		return false
	}
	for index, character := range alias {
		if character == '_' || character >= 'a' && character <= 'z' || index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

func readCorpusBindings(t *testing.T) map[string]map[string]any {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("TRINO_HOGQL_CORPUS_BINDINGS"))
	if path == "" {
		return map[string]map[string]any{}
	}
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus bindings: %v", err)
	}
	var bindings map[string]map[string]any
	if err := json.Unmarshal(document, &bindings); err != nil {
		t.Fatalf("decode corpus bindings: %v", err)
	}
	return bindings
}

func corpusQueryPaths(t *testing.T, directory string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(directory, "q[0-9][0-9][0-9][0-9].sql"))
	if err != nil {
		t.Fatalf("enumerate corpus queries: %v", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatal("corpus directory contains no hash-addressed query files")
	}
	if rawExpected := strings.TrimSpace(os.Getenv("TRINO_HOGQL_CORPUS_EXPECTED_COUNT")); rawExpected != "" {
		expected, err := strconv.Atoi(rawExpected)
		if err != nil || expected < 1 {
			t.Fatalf("invalid TRINO_HOGQL_CORPUS_EXPECTED_COUNT %q", rawExpected)
		}
		if len(paths) != expected {
			t.Fatalf("corpus query count = %d, want %d", len(paths), expected)
		}
	}
	return paths
}

func corpusWorkerCount(t *testing.T, queryCount int) int {
	t.Helper()
	workerCount := 8
	if rawWorkerCount := strings.TrimSpace(os.Getenv("TRINO_HOGQL_CORPUS_WORKERS")); rawWorkerCount != "" {
		parsed, err := strconv.Atoi(rawWorkerCount)
		if err != nil || parsed < 1 || parsed > 16 {
			t.Fatalf("invalid TRINO_HOGQL_CORPUS_WORKERS %q", rawWorkerCount)
		}
		workerCount = parsed
	}
	if workerCount > queryCount {
		return queryCount
	}
	return workerCount
}

func executeCorpusHogQL(client *imageTrinoClient, request []byte) (imageQueryOutcome, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		outcome, err := client.executeHogQL(request)
		if err != nil || outcome.Error == nil || !retryableCorpusError(outcome.Error.ErrorName) || time.Now().After(deadline) {
			return outcome, err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func retryableCorpusError(errorName string) bool {
	return errorName == "HOGQL_CATALOG_NOT_READY" || errorName == "HOGQL_COMPILATION_QUEUE_FULL"
}

func writeCorpusReport(t *testing.T, total int, failures []corpusAnalysisFailure) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("TRINO_HOGQL_CORPUS_REPORT"))
	if path == "" {
		return
	}
	reportFailures := make([]corpusAnalysisReportFailure, 0, len(failures))
	for _, failure := range failures {
		reportFailures = append(reportFailures, corpusAnalysisReportFailure{
			QueryID:          failure.queryID,
			QueryHash:        failure.queryHash,
			ErrorName:        failure.errorName,
			ErrorFingerprint: failure.errorFingerprint,
		})
	}
	report := corpusAnalysisReport{Total: total, Passed: total - len(failures), Failures: reportFailures}
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("encode corpus report: %v", err)
	}
	payload = append(payload, '\n')
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write corpus report: %v", err)
	}
}

func corpusGeneration(t *testing.T, fallback int64, bindings map[string]any) int64 {
	t.Helper()
	rawGeneration, exists := bindings["catalogGeneration"]
	if !exists {
		return fallback
	}
	generation, ok := rawGeneration.(float64)
	if !ok || generation < 1 || generation != float64(int64(generation)) {
		t.Fatal("corpus catalogGeneration binding must be a positive integer")
	}
	return int64(generation)
}

func corpusHogQLRequest(query string, generation int64, bindings map[string]any) []byte {
	request := map[string]any{
		"query":             query,
		"protocolVersion":   1,
		"languageVersion":   "1.0.0",
		"parameters":        map[string]any{},
		"variables":         map[string]any{},
		"filters":           map[string]any{},
		"modifiers":         map[string]any{},
		"catalogGeneration": generation,
		"explain":           map[string]any{"type": "LOGICAL", "format": "TEXT"},
	}
	for _, scope := range []string{"parameters", "variables", "filters", "modifiers"} {
		if value, exists := bindings[scope]; exists {
			request[scope] = value
		}
	}
	payload, err := json.Marshal(request)
	if err != nil {
		panic(err)
	}
	return payload
}

func corpusFailure(queryID, queryHash, errorName, message string) corpusAnalysisFailure {
	return corpusAnalysisFailure{
		queryID:          queryID,
		queryHash:        queryHash,
		errorName:        errorName,
		errorFingerprint: fmt.Sprintf("%x", sha256.Sum256([]byte(errorName+":"+message))),
	}
}
