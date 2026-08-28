package trino_ducklake_smoke

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

const (
	hogQLStatementPath = "/v1/hogql"
	trinoStatementPath = "/v1/statement"
	hogQLCatalogToken  = "synthetic-hogql-catalog-token"
)

type imageQueryError struct {
	Message   string `json:"message"`
	ErrorName string `json:"errorName"`
}

type imageQueryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type imageQueryResponse struct {
	Columns []imageQueryColumn `json:"columns"`
	Data    [][]any            `json:"data"`
	NextURI string             `json:"nextUri"`
	Error   *imageQueryError   `json:"error"`
}

type imageQueryOutcome struct {
	Columns []imageQueryColumn
	Rows    [][]any
	Error   *imageQueryError
	Pages   int
}

type imageTrinoClient struct {
	baseURL    string
	httpClient *http.Client
}

type semanticCatalogRequest struct {
	Generation int64
	Authorized bool
	Exact      bool
}

type semanticCatalogServer struct {
	mu        sync.Mutex
	token     string
	available bool
	latest    int64
	snapshots map[int64][]byte
	requests  []semanticCatalogRequest
}

func TestHogQLExactImageSmoke(t *testing.T) {
	image := os.Getenv("TRINO_HOGQL_SMOKE_IMAGE")
	if image == "" {
		t.Skip("set TRINO_HOGQL_SMOKE_IMAGE to an immutable image digest or local image ID")
	}
	image = resolveImmutableImageReference(t, image)
	setDuckLakeTestTimeZone(t)

	semanticCatalog := newSemanticCatalogServer(t, hogQLCatalogToken, hogQLSnapshot(t, 1))
	server := startHostVisibleServer(t, semanticCatalog)
	configDir := t.TempDir()
	tokenPath := filepath.Join(configDir, "catalog-token")
	writeTestFile(t, tokenPath, hogQLCatalogToken+"\n")
	configPath := filepath.Join(configDir, "config.properties")
	writeTestFile(t, configPath, hogQLTrinoConfig(server.Listener.Addr().(*net.TCPAddr).Port))

	baseURL := startHogQLTrinoContainer(t, image, configPath, tokenPath)
	client := &imageTrinoClient{baseURL: baseURL, httpClient: &http.Client{Timeout: 2 * time.Minute}}
	waitForTrino(t, client)
	seedHogQLMemoryFixture(t, client)

	standardRows := requireSQLSuccess(t, client, "SELECT e.event, CAST(CAST(json_parse(e.properties) AS map(varchar, json))['plan'] AS varchar) AS plan, p.id FROM memory.default.events e LEFT JOIN memory.default.persons p ON e.person_id = p.id ORDER BY e.event")
	cold := requireHogQLError(t, client, hogQLRequest("SELECT events.event, events.properties.plan, events.person.id FROM events ORDER BY events.event", 0, nil))
	if cold.ErrorName != "HOGQL_CATALOG_NOT_READY" {
		t.Fatalf("cold HogQL error = %s, want HOGQL_CATALOG_NOT_READY: %s", cold.ErrorName, cold.Message)
	}

	semanticCatalog.setAvailable(true)
	hogQLRows := waitForHogQLSuccess(t, client, hogQLRequest("SELECT events.event, events.properties.plan, events.person.id FROM events ORDER BY events.event", 0, nil))
	if !reflect.DeepEqual(canonicalTrinoRows(hogQLRows.Rows), canonicalTrinoRows(standardRows.Rows)) {
		t.Fatalf("HogQL result differs from standard SQL\nHogQL: %v\nSQL: %v", hogQLRows.Rows, standardRows.Rows)
	}
	if !reflect.DeepEqual(hogQLRows.Columns, standardRows.Columns) {
		t.Fatalf("HogQL schema differs from standard SQL\nHogQL: %v\nSQL: %v", hogQLRows.Columns, standardRows.Columns)
	}
	assertExactImageDifferentialCorpus(t, client)

	semanticCatalog.publish(t, hogQLSnapshot(t, 2))
	pinnedRows := waitForHogQLSuccess(t, client, hogQLRequest("SELECT events.event FROM events ORDER BY events.event", 2, nil))
	if len(pinnedRows.Rows) != 3 {
		t.Fatalf("generation-pinned query returned %d rows, want 3", len(pinnedRows.Rows))
	}
	if !semanticCatalog.requestedGeneration(2) {
		t.Fatal("Trino did not request the exact catalog generation")
	}

	time.Sleep(125 * time.Millisecond)
	waitForHogQLSuccess(t, client, hogQLRequest("SELECT events.event FROM events ORDER BY events.event", 0, nil))
	semanticCatalog.waitForLatestGeneration(t, 2)
	time.Sleep(25 * time.Millisecond)
	semanticCatalog.setAvailable(false)
	time.Sleep(125 * time.Millisecond)
	requireHogQLSuccess(t, client, hogQLRequest("SELECT events.event FROM events ORDER BY events.event", 0, nil))
	time.Sleep(750 * time.Millisecond)
	outage := requireHogQLError(t, client, hogQLRequest("SELECT events.event FROM events ORDER BY events.event", 0, nil))
	if outage.ErrorName != "HOGQL_CATALOG_NOT_READY" {
		t.Fatalf("expired catalog error = %s, want HOGQL_CATALOG_NOT_READY: %s", outage.ErrorName, outage.Message)
	}
	semanticCatalog.setAvailable(true)
	waitForHogQLSuccess(t, client, hogQLRequest("SELECT events.event FROM events ORDER BY events.event", 0, nil))

	unsupported := requireHogQLError(t, client, hogQLRequest("SELECT unsupportedHogQlFunction(event) FROM events", 0, nil))
	if unsupported.ErrorName != "HOGQL_RESOLUTION_ERROR" {
		t.Fatalf("unsupported function error = %s, want HOGQL_RESOLUTION_ERROR: %s", unsupported.ErrorName, unsupported.Message)
	}
	explain := requireHogQLSuccess(t, client, hogQLRequest(
		"SELECT events.event FROM events WHERE events.properties.plan = 'pro'",
		0,
		map[string]any{"type": "LOGICAL", "format": "TEXT"}))
	if len(explain.Rows) == 0 || len(explain.Rows[0]) == 0 || !strings.Contains(fmt.Sprint(explain.Rows[0][0]), "Scan") {
		t.Fatalf("HogQL EXPLAIN returned an unexpected plan: %v", explain.Rows)
	}

	requireSQLSuccess(t, client, "CREATE TABLE memory.default.hogql_paging AS SELECT (major - 1) * 100 + minor AS id, repeat('x', 4096) AS payload FROM UNNEST(sequence(1, 120)) AS first(major) CROSS JOIN UNNEST(sequence(1, 100)) AS second(minor)")
	paged := requireHogQLSuccess(t, client, hogQLRequest("SELECT id, payload FROM memory.default.hogql_paging ORDER BY id", 0, nil))
	if len(paged.Rows) != 12000 || paged.Pages < 2 {
		t.Fatalf("paged HogQL query returned rows=%d pages=%d, want 12000 rows across multiple pages", len(paged.Rows), paged.Pages)
	}

	cancelResponse, err := client.startHogQL(hogQLRequest("SELECT * FROM tpch.sf1000.lineitem", 0, nil))
	if err != nil {
		t.Fatalf("start cancellable HogQL query: %v", err)
	}
	if cancelResponse.Error != nil {
		t.Fatalf("cancellable HogQL query failed before cancellation: %s", cancelResponse.Error.Message)
	}
	if cancelResponse.NextURI == "" {
		t.Fatal("cancellable HogQL query completed before returning a cancellation URI")
	}
	if err := client.cancel(cancelResponse.NextURI); err != nil {
		t.Fatalf("cancel HogQL query: %v", err)
	}

}

func newSemanticCatalogServer(t *testing.T, token string, snapshot *hogqlcatalog.HogQLSemanticCatalogSnapshot) *semanticCatalogServer {
	t.Helper()
	payload := validatedSnapshotJSON(t, snapshot)
	return &semanticCatalogServer{
		token:     token,
		latest:    snapshot.Generation,
		snapshots: map[int64][]byte{snapshot.Generation: payload},
	}
}

func (s *semanticCatalogServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != "/v1/hogql/compatibility/semantic-catalog" {
		http.NotFound(writer, request)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	authorized := request.Header.Get("X-Duckgres-Internal-Secret") == s.token
	generation := s.latest
	rawGeneration := request.URL.Query().Get("generation")
	if rawGeneration != "" {
		parsed, err := strconv.ParseInt(rawGeneration, 10, 64)
		if err != nil {
			http.Error(writer, "invalid generation", http.StatusBadRequest)
			return
		}
		generation = parsed
	}
	s.requests = append(s.requests, semanticCatalogRequest{Generation: generation, Authorized: authorized, Exact: rawGeneration != ""})
	if !authorized {
		writeSemanticCatalogError(writer, http.StatusUnauthorized)
		return
	}
	if !s.available {
		writeSemanticCatalogError(writer, http.StatusServiceUnavailable)
		return
	}
	payload, exists := s.snapshots[generation]
	if !exists {
		writeSemanticCatalogError(writer, http.StatusNotFound)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("ETag", fmt.Sprintf(`"hogql-%d"`, generation))
	_, _ = writer.Write(payload)
}

func (s *semanticCatalogServer) publish(t *testing.T, snapshot *hogqlcatalog.HogQLSemanticCatalogSnapshot) {
	t.Helper()
	payload := validatedSnapshotJSON(t, snapshot)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[snapshot.Generation] = payload
	s.latest = snapshot.Generation
}

func (s *semanticCatalogServer) setAvailable(available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = available
}

func (s *semanticCatalogServer) requestedGeneration(generation int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, request := range s.requests {
		if request.Authorized && request.Exact && request.Generation == generation {
			return true
		}
	}
	return false
}

func (s *semanticCatalogServer) waitForLatestGeneration(t *testing.T, generation int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		requested := false
		for _, request := range s.requests {
			if request.Authorized && !request.Exact && request.Generation == generation {
				requested = true
				break
			}
		}
		s.mu.Unlock()
		if requested {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Trino did not request latest catalog generation %d", generation)
}

func writeSemanticCatalogError(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, `{"code":"HOGQL_CATALOG_UNAVAILABLE","message":"semantic catalog unavailable"}`)
}

func startHostVisibleServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen for semantic catalog: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func validatedSnapshotJSON(t *testing.T, snapshot *hogqlcatalog.HogQLSemanticCatalogSnapshot) []byte {
	t.Helper()
	store := hogqlcatalog.NewMemoryStore()
	if err := store.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("validate semantic snapshot: %v", err)
	}
	validated, err := store.Latest(context.Background(), snapshot.Catalog)
	if err != nil {
		t.Fatalf("read validated semantic snapshot: %v", err)
	}
	payload, err := json.Marshal(validated)
	if err != nil {
		t.Fatalf("encode semantic snapshot: %v", err)
	}
	return payload
}

func hogQLSnapshot(t *testing.T, generation int64) *hogqlcatalog.HogQLSemanticCatalogSnapshot {
	t.Helper()
	catalog := hogqlcatalog.PhysicalIdentifier{Value: "memory"}
	schema := hogqlcatalog.PhysicalIdentifier{Value: "default"}
	propertyRecipe := &hogqlcatalog.ExpressionRecipe{
		Kind: hogqlcatalog.ExpressionRecipeOperator,
		Operator: &hogqlcatalog.OperatorRecipe{
			Operator: hogqlcatalog.SemanticOperatorJSONObjectLookup,
			Arguments: []hogqlcatalog.ExpressionRecipe{
				{
					Kind:              hogqlcatalog.ExpressionRecipeArgumentReference,
					ArgumentReference: &hogqlcatalog.ArgumentReferenceRecipe{Argument: hogqlcatalog.ExpressionArgumentPropertySource},
				},
				{
					Kind:              hogqlcatalog.ExpressionRecipeArgumentReference,
					ArgumentReference: &hogqlcatalog.ArgumentReferenceRecipe{Argument: hogqlcatalog.ExpressionArgumentPropertyKey},
				},
			},
		},
	}
	properties := func() hogqlcatalog.PropertyDefinition {
		return hogqlcatalog.PropertyDefinition{
			Name:               "properties",
			SourceField:        "properties",
			Storage:            hogqlcatalog.PropertyStorageJSONObject,
			LogicalType:        hogqlcatalog.LogicalTypeString,
			Nullable:           true,
			KeyTypeSignature:   "varchar",
			ValueTypeSignature: "varchar",
			LookupRecipe:       propertyRecipe,
		}
	}
	return &hogqlcatalog.HogQLSemanticCatalogSnapshot{
		ProtocolVersion: hogqlcatalog.SnapshotProtocolVersion,
		SchemaVersion:   hogqlcatalog.SnapshotSchemaVersion,
		LanguageVersion: "1.0.0",
		Catalog:         catalog,
		Generation:      generation,
		LogicalTables: []hogqlcatalog.LogicalTableDefinition{
			{
				Name: "events",
				PhysicalTable: hogqlcatalog.PhysicalQualifiedName{
					Catalog: catalog,
					Schema:  schema,
					Table:   hogqlcatalog.PhysicalIdentifier{Value: "events"},
				},
				Fields: []hogqlcatalog.LogicalFieldDefinition{
					{Name: "event", PhysicalColumn: hogqlcatalog.PhysicalIdentifier{Value: "event"}, TrinoTypeSignature: "varchar", LogicalType: hogqlcatalog.LogicalTypeString, StarVisible: true},
					{Name: "person_id", PhysicalColumn: hogqlcatalog.PhysicalIdentifier{Value: "person_id"}, TrinoTypeSignature: "varchar", LogicalType: hogqlcatalog.LogicalTypeString, StarVisible: true},
					{Name: "properties", PhysicalColumn: hogqlcatalog.PhysicalIdentifier{Value: "properties"}, TrinoTypeSignature: "varchar", LogicalType: hogqlcatalog.LogicalTypeString, Nullable: true, StarVisible: true},
				},
				Properties: []hogqlcatalog.PropertyDefinition{properties()},
				Relationships: []hogqlcatalog.RelationshipDefinition{{
					Name:        "person",
					TargetTable: "persons",
					Cardinality: hogqlcatalog.RelationshipCardinalityManyToOne,
					JoinKeys:    []hogqlcatalog.JoinKey{{SourceField: "person_id", TargetField: "id"}},
				}},
			},
			{
				Name: "persons",
				PhysicalTable: hogqlcatalog.PhysicalQualifiedName{
					Catalog: catalog,
					Schema:  schema,
					Table:   hogqlcatalog.PhysicalIdentifier{Value: "persons"},
				},
				Fields: []hogqlcatalog.LogicalFieldDefinition{
					{Name: "id", PhysicalColumn: hogqlcatalog.PhysicalIdentifier{Value: "id"}, TrinoTypeSignature: "varchar", LogicalType: hogqlcatalog.LogicalTypeString, StarVisible: true},
					{Name: "properties", PhysicalColumn: hogqlcatalog.PhysicalIdentifier{Value: "properties"}, TrinoTypeSignature: "varchar", LogicalType: hogqlcatalog.LogicalTypeString, Nullable: true, StarVisible: true},
				},
				Properties:    []hogqlcatalog.PropertyDefinition{properties()},
				Relationships: []hogqlcatalog.RelationshipDefinition{},
			},
		},
		ExpressionFields:  []hogqlcatalog.ExpressionFieldDefinition{},
		VirtualTables:     []hogqlcatalog.VirtualTableDefinition{},
		SavedQueries:      []hogqlcatalog.SavedQueryReference{},
		MaterializedViews: []hogqlcatalog.MaterializedViewReference{},
		Functions:         []hogqlcatalog.FunctionCapabilityDefinition{},
		ModifierDefaults:  []hogqlcatalog.SemanticModifierDefault{},
	}
}

func hogQLTrinoConfig(semanticCatalogPort int) string {
	return fmt.Sprintf(`coordinator=true
node-scheduler.include-coordinator=true
discovery.uri=http://localhost:8080
catalog.management=static
hogql.enabled=true
hogql.compilation-threads=2
hogql.compilation-queue-capacity=8
hogql.semantic-catalog.uri=http://host.docker.internal:%d
hogql.semantic-catalog.authentication-token-file=/run/secrets/hogql-catalog-token
hogql.semantic-catalog.refresh-after=100ms
hogql.semantic-catalog.expire-after=750ms
hogql.semantic-catalog.failure-backoff=25ms
hogql.semantic-catalog.request-timeout=1s
`, semanticCatalogPort)
}

func resolveImmutableImageReference(t *testing.T, image string) string {
	t.Helper()
	if immutableImageReference(image) {
		return image
	}
	output, err := exec.Command("docker", "image", "inspect", "--format", "{{.Id}}", image).CombinedOutput()
	if err != nil {
		t.Fatalf("resolve TRINO_HOGQL_SMOKE_IMAGE %q: %v\n%s", image, err, output)
	}
	resolved := strings.TrimSpace(string(output))
	if !immutableImageReference(resolved) {
		t.Fatalf("docker resolved TRINO_HOGQL_SMOKE_IMAGE %q to invalid image ID %q", image, resolved)
	}
	t.Logf("exact Trino image: %s (%s)", image, resolved)
	return resolved
}

func immutableImageReference(image string) bool {
	digest := ""
	if strings.HasPrefix(image, "sha256:") {
		digest = strings.TrimPrefix(image, "sha256:")
	} else if index := strings.LastIndex(image, "@sha256:"); index >= 0 {
		digest = image[index+len("@sha256:"):]
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32
}

func startHogQLTrinoContainer(t *testing.T, image, configPath, tokenPath string) string {
	t.Helper()
	port := reserveTCPPort(t)
	name := fmt.Sprintf("duckgres-hogql-smoke-%d-%d", os.Getpid(), time.Now().UnixNano())
	args := []string{
		"run", "--detach", "--rm", "--name", name,
		"--publish", fmt.Sprintf("127.0.0.1:%d:8080", port),
		"--add-host", "host.docker.internal:host-gateway",
		"--volume", configPath + ":/etc/trino/config.properties:ro",
		"--volume", tokenPath + ":/run/secrets/hogql-catalog-token:ro",
		image,
	}
	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("start exact Trino image: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Logf("Trino container logs:\n%s", logs)
		}
		_, _ = exec.Command("docker", "rm", "--force", name).CombinedOutput()
	})
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve Trino port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release Trino port reservation: %v", err)
	}
	return port
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", filepath.Base(path), err)
	}
}

func waitForTrino(t *testing.T, client *imageTrinoClient) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for time.Now().Before(deadline) {
		outcome, err := client.executeSQL("SELECT 1")
		if err == nil && outcome.Error == nil {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = errors.New(outcome.Error.Message)
		}
		<-ticker.C
	}
	t.Fatalf("Trino image did not become ready: %v", lastErr)
}

func seedHogQLMemoryFixture(t *testing.T, client *imageTrinoClient) {
	t.Helper()
	for _, statement := range []string{
		"CREATE TABLE memory.default.persons (id varchar, properties varchar)",
		"CREATE TABLE memory.default.events (event varchar, person_id varchar, properties varchar)",
		`INSERT INTO memory.default.persons VALUES ('person-1', '{"role":"admin"}'), ('person-2', '{"role":"member"}')`,
		`INSERT INTO memory.default.events VALUES ('purchase', 'person-2', '{"plan":"free"}'), ('signup', 'person-1', '{"plan":"pro"}'), ('view', NULL, '{"plan":"anonymous"}')`,
	} {
		requireSQLSuccess(t, client, statement)
	}
}

func assertExactImageDifferentialCorpus(t *testing.T, client *imageTrinoClient) {
	t.Helper()
	queries := []struct {
		name     string
		hogQL    string
		trinoSQL string
	}{
		{
			name:     "null case and cast",
			hogQL:    "SELECT CAST(1 AS INTEGER) AS value, CASE WHEN NULL IS NULL THEN 'yes' ELSE 'no' END AS marker",
			trinoSQL: "SELECT CAST(1 AS integer) AS value, CASE WHEN NULL IS NULL THEN 'yes' ELSE 'no' END AS marker",
		},
		{
			name:     "collections and functions",
			hogQL:    "SELECT abs(-2), coalesce(NULL, 'fallback'), if(true, 1, 2), arrayDistinct([1, 1, 2]), arraySort([2, 1]), arrayFlatten([[1], [2]]), arrayStringConcat(['a', 'b'])",
			trinoSQL: "SELECT abs(-2), coalesce(NULL, 'fallback'), if(true, 1, 2), array_distinct(ARRAY[1, 1, 2]), array_sort(ARRAY[2, 1]), flatten(ARRAY[ARRAY[1], ARRAY[2]]), array_join(ARRAY['a', 'b'], '')",
		},
		{
			name:     "grouping and windows",
			hogQL:    "SELECT event, count(), rank() OVER (ORDER BY event) FROM events GROUP BY event ORDER BY event",
			trinoSQL: "SELECT event, count(), rank() OVER (ORDER BY event) FROM memory.default.events GROUP BY event ORDER BY event",
		},
		{
			name:     "set operations",
			hogQL:    "SELECT 2 AS value UNION SELECT 3",
			trinoSQL: "SELECT 2 AS value UNION SELECT 3",
		},
		{
			name:     "native values",
			hogQL:    "SELECT value FROM (VALUES (2), (1), (2)) AS numbers(value) ORDER BY value",
			trinoSQL: "SELECT value FROM (VALUES (2), (1), (2)) AS numbers(value) ORDER BY value",
		},
		{
			name:     "temporal rewrites",
			hogQL:    "SELECT addDays(CAST('2024-01-15 12:34:56' AS Timestamp), 2), subtractDays(CAST('2024-01-15 12:34:56' AS Timestamp), 3), addMonths(CAST('2024-01-15 12:34:56' AS Timestamp), 1), subtractMonths(CAST('2024-01-15 12:34:56' AS Timestamp), 2), subtractYears(CAST('2024-01-15 12:34:56' AS Timestamp), 1), toStartOfDay(CAST('2024-01-15 12:34:56' AS Timestamp)), toStartOfHour(CAST('2024-01-15 12:34:56' AS Timestamp)), toStartOfMonth(CAST('2024-01-15 12:34:56' AS Timestamp)), toStartOfWeek(CAST('2024-01-15 12:34:56' AS Timestamp), 1), toDayOfMonth(CAST('2024-01-15' AS Date)), toDayOfWeek(CAST('2024-01-15' AS Date)), toMonth(CAST('2024-01-15' AS Date)), toYear(CAST('2024-01-15' AS Date)), toLastDayOfMonth(CAST('2024-01-15' AS Date)), formatDateTime(CAST('2024-01-15 12:34:56' AS Timestamp), '%Y-%m-%d'), parseDateTime('2024-01-15', '%Y-%m-%d'), parseDateTimeBestEffort('2024-01-15 12:34:56')",
			trinoSQL: "SELECT date_add('day', 2, CAST('2024-01-15 12:34:56' AS timestamp(0))), date_add('day', -3, CAST('2024-01-15 12:34:56' AS timestamp(0))), date_add('month', 1, CAST('2024-01-15 12:34:56' AS timestamp(0))), date_add('month', -2, CAST('2024-01-15 12:34:56' AS timestamp(0))), date_add('year', -1, CAST('2024-01-15 12:34:56' AS timestamp(0))), date_trunc('day', CAST('2024-01-15 12:34:56' AS timestamp(0))), date_trunc('hour', CAST('2024-01-15 12:34:56' AS timestamp(0))), date_trunc('month', CAST('2024-01-15 12:34:56' AS timestamp(0))), date_trunc('week', CAST('2024-01-15 12:34:56' AS timestamp(0))), day(CAST('2024-01-15' AS date)), day_of_week(CAST('2024-01-15' AS date)), month(CAST('2024-01-15' AS date)), year(CAST('2024-01-15' AS date)), last_day_of_month(CAST('2024-01-15' AS date)), date_format(CAST('2024-01-15 12:34:56' AS timestamp(0)), '%Y-%m-%d'), date_parse('2024-01-15', '%Y-%m-%d'), TRY_CAST('2024-01-15 12:34:56' AS timestamp(3))",
		},
		{
			name:     "JSON regex and string rewrites",
			hogQL:    `SELECT JSONExtractString(payload, 'name'), JSONExtractInt(payload, 'items', 0), JSONExtractFloat(payload, 'score'), JSONExtractBool(payload, 'active'), JSONExtractUInt(payload, 'items', 1), JSONExtractRaw(payload, 'object'), JSONLength(payload, 'items'), JSONHas(payload, 'object'), JSONExtractKeys(payload, 'object'), extract(sample, '([a-z]+)'), extractAll(sample, '([a-z]+)'), match(sample, '^[a-z]+'), replaceRegexpAll(sample, '[0-9]', 'x'), replaceRegexpOne(sample, '[0-9]+', 'x'), splitByString('123', sample), substringUTF8(sample, 2, 3), position(sample, '123') FROM (VALUES ('{"name":"Ada","items":[2,3],"active":true,"score":1.5,"object":{"k":7}}', 'abc123abc')) AS t(payload, sample)`,
			trinoSQL: `SELECT coalesce(json_extract_scalar(payload, '$["name"]'), ''), coalesce(TRY_CAST(json_extract_scalar(payload, '$["items"][0]') AS bigint), 0), coalesce(TRY_CAST(json_extract_scalar(payload, '$["score"]') AS double), 0E0), coalesce(TRY_CAST(json_extract_scalar(payload, '$["active"]') AS boolean), false), coalesce(TRY_CAST(json_extract_scalar(payload, '$["items"][1]') AS bigint), 0), coalesce(json_format(json_extract(payload, '$["object"]')), ''), coalesce(json_size(payload, '$["items"]'), 0), json_extract(payload, '$["object"]') IS NOT NULL, map_keys(coalesce(TRY_CAST(json_extract(payload, '$["object"]') AS map(varchar, json)), CAST(map(ARRAY[], ARRAY[]) AS map(varchar, json)))), coalesce(regexp_extract(sample, '([a-z]+)', 1), ''), regexp_extract_all(sample, '([a-z]+)', 1), regexp_like(sample, '^[a-z]+'), regexp_replace(sample, '[0-9]', 'x'), regexp_replace(sample, '(?s)^(.*?)(([0-9]+))', '$1x'), split(sample, '123'), substring(sample, 2, 3), strpos(sample, '123') FROM (VALUES ('{"name":"Ada","items":[2,3],"active":true,"score":1.5,"object":{"k":7}}', 'abc123abc')) AS t(payload, sample)`,
		},
		{
			name:     "collection and lambda rewrites",
			hogQL:    "SELECT arrayElement([1, 2, 3], -1), arrayFilter(x -> x > 1, [1, 2, 3]), arrayFirst(x -> x > 1, [1, 2, 3]), arrayMap(x -> x + 1, [1, 2, 3]), arraySum([1, 2, 3]), arrayMin([3, 1, 2]), arraySlice([1, 2, 3, 4], 2, 2), arrayEnumerate(['a', 'b']), range(2, 5), tupleElement(tuple('x', 7), 2), splitByChar(',', 'a,b'), has([1, 2], 2), hasAny([1, 2], [2, 3]), map('a', 1, 'b', 2)['b'], mapUpdate(map('a', 1), map('a', 2))['a']",
			trinoSQL: "SELECT element_at(ARRAY[1, 2, 3], -1), filter(ARRAY[1, 2, 3], x -> x > 1), element_at(filter(ARRAY[1, 2, 3], x -> x > 1), 1), transform(ARRAY[1, 2, 3], x -> x + 1), reduce(ARRAY[1, 2, 3], 0, (total, item) -> total + item, total -> total), array_min(ARRAY[3, 1, 2]), slice(ARRAY[1, 2, 3, 4], 2, 2), sequence(1, cardinality(ARRAY['a', 'b'])), sequence(2, 5 - 1), ROW('x', 7)[2], split('a,b', ','), coalesce(contains(ARRAY[1, 2], 2), false), arrays_overlap(ARRAY[1, 2], ARRAY[2, 3]), map(ARRAY['a', 'b'], ARRAY[1, 2])['b'], map_concat(map(ARRAY['a'], ARRAY[1]), map(ARRAY['a'], ARRAY[2]))['a']",
		},
		{
			name:     "aggregate rewrites",
			hogQL:    "SELECT countIf(active), sumIf(value, active), minIf(value, active), maxIf(value, active), avgIf(value, active), uniqExactIf(value, active), countDistinct(value), argMaxIf(value, weight, active), argMinIf(value, weight, active), quantile(0.5)(value), quantileIf(0.5)(value, active) FROM (VALUES (1, true, 10), (1, false, 20), (2, true, 30), (3, true, 40)) AS t(value, active, weight)",
			trinoSQL: "SELECT count(*) FILTER (WHERE active), sum(value) FILTER (WHERE active), min(value) FILTER (WHERE active), max(value) FILTER (WHERE active), avg(value) FILTER (WHERE active), count(DISTINCT value) FILTER (WHERE active), count(DISTINCT value), max_by(value, weight) FILTER (WHERE active), min_by(value, weight) FILTER (WHERE active), approx_percentile(value, 0.5), approx_percentile(value, 0.5) FILTER (WHERE active) FROM (VALUES (1, true, 10), (1, false, 20), (2, true, 30), (3, true, 40)) AS t(value, active, weight)",
		},
		{
			name:     "numeric conversion and operator rewrites",
			hogQL:    "SELECT toFloatOrZero('bad'), toFloatOrDefault('bad', 1), toDecimal('1.25', 2), intDiv(-5, 2), toInt('4'), toIntOrZero('bad'), _toInt16('3'), toUUID('00000000-0000-0000-0000-000000000001'), roundBankers(1.25, 1), plus(4, 2), minus(4, 2), multiply(4, 2), divide(5, 2), greater(4, 2), greaterOrEquals(4, 4), lessOrEquals(2, 4), notEquals(1, 2), and(true, true, false), or(false, false, true), not(false), empty(''), notEmpty([1]), empty(mapFromArrays(['key'], [1])), empty(CAST(NULL AS Array(Int64)))",
			trinoSQL: "SELECT coalesce(TRY_CAST('bad' AS double), 0E0), coalesce(TRY_CAST('bad' AS double), CAST(1 AS double)), TRY_CAST('1.25' AS decimal(18, 2)), CAST(-5 AS bigint) / CAST(2 AS bigint) - if(CAST(-5 AS bigint) % CAST(2 AS bigint) <> 0 AND (CAST(-5 AS bigint) < 0 AND CAST(2 AS bigint) > 0 OR CAST(-5 AS bigint) > 0 AND CAST(2 AS bigint) < 0), 1, 0), CAST('4' AS bigint), coalesce(TRY_CAST('bad' AS bigint), 0), CAST('3' AS smallint), TRY_CAST('00000000-0000-0000-0000-000000000001' AS uuid), round(DOUBLE '1.25', 1), 4 + 2, 4 - 2, 4 * 2, 5 / 2, 4 > 2, 4 >= 4, 2 <= 4, 1 <> 2, (true AND true) AND false, (false OR false) OR true, NOT false, coalesce(length(CAST('' AS varchar)), 0) = 0, coalesce(cardinality(ARRAY[1]), 0) > 0, coalesce(cardinality(map(ARRAY['key'], ARRAY[1])), 0) = 0, coalesce(cardinality(CAST(NULL AS array(bigint))), 0) = 0",
		},
		{
			name:     "arrayJoin lowering",
			hogQL:    "SELECT id, arrayJoin(values_array) AS value FROM (VALUES (2, [3, 1]), (1, [2])) AS t(id, values_array) ORDER BY id, value",
			trinoSQL: "SELECT id, value FROM (VALUES (2, ARRAY[3, 1]), (1, ARRAY[2])) AS t(id, values_array) CROSS JOIN UNNEST(values_array) AS u(value) ORDER BY id, value",
		},
		{
			name:     "nested LIMIT BY lowering",
			hogQL:    "SELECT nested.* FROM (SELECT category, value FROM (VALUES ('a', 2), ('a', 1), ('b', 4), ('b', 3)) AS t(category, value) ORDER BY value DESC LIMIT 1 BY category) AS nested ORDER BY category",
			trinoSQL: "SELECT category, value FROM (SELECT category, value, row_number() OVER (PARTITION BY category ORDER BY value DESC) AS row_number FROM (VALUES ('a', 2), ('a', 1), ('b', 4), ('b', 3)) AS t(category, value)) WHERE row_number <= 1 ORDER BY category",
		},
	}
	for _, query := range queries {
		query := query
		t.Run(query.name, func(t *testing.T) {
			hogQL := requireHogQLSuccess(t, client, hogQLRequest(query.hogQL, 0, nil))
			trinoSQL := requireSQLSuccess(t, client, query.trinoSQL)
			if !reflect.DeepEqual(hogQL.Columns, trinoSQL.Columns) {
				t.Fatalf("schema differs\nHogQL: %v\nSQL: %v", hogQL.Columns, trinoSQL.Columns)
			}
			if !reflect.DeepEqual(canonicalTrinoRows(hogQL.Rows), canonicalTrinoRows(trinoSQL.Rows)) {
				t.Fatalf("rows differ\nHogQL: %v\nSQL: %v", hogQL.Rows, trinoSQL.Rows)
			}
		})
	}
}

func hogQLRequest(query string, generation int64, explain map[string]any) []byte {
	request := map[string]any{
		"query":           query,
		"protocolVersion": 1,
		"languageVersion": "1.0.0",
		"parameters":      map[string]any{},
		"variables":       map[string]any{},
		"filters":         map[string]any{},
		"modifiers":       map[string]any{},
	}
	if generation > 0 {
		request["catalogGeneration"] = generation
	}
	if explain != nil {
		request["explain"] = explain
	}
	payload, err := json.Marshal(request)
	if err != nil {
		panic(err)
	}
	return payload
}

func (c *imageTrinoClient) executeSQL(statement string) (imageQueryOutcome, error) {
	first, err := c.start(trinoStatementPath, "text/plain", []byte(statement))
	if err != nil {
		return imageQueryOutcome{}, err
	}
	return c.finish(first)
}

func (c *imageTrinoClient) executeHogQL(payload []byte) (imageQueryOutcome, error) {
	first, err := c.startHogQL(payload)
	if err != nil {
		return imageQueryOutcome{}, err
	}
	return c.finish(first)
}

func (c *imageTrinoClient) startHogQL(payload []byte) (imageQueryResponse, error) {
	return c.start(hogQLStatementPath, "application/json", payload)
}

func (c *imageTrinoClient) start(path, contentType string, body []byte) (imageQueryResponse, error) {
	request, err := http.NewRequest(http.MethodPost, c.baseURL+path, strings.NewReader(string(body)))
	if err != nil {
		return imageQueryResponse{}, err
	}
	request.Header.Set("Content-Type", contentType)
	c.setHeaders(request)
	return c.do(request)
}

func (c *imageTrinoClient) finish(response imageQueryResponse) (imageQueryOutcome, error) {
	outcome := imageQueryOutcome{Pages: 1}
	for {
		if len(response.Columns) > 0 {
			outcome.Columns = response.Columns
		}
		outcome.Rows = append(outcome.Rows, response.Data...)
		if response.Error != nil {
			outcome.Error = response.Error
			return outcome, nil
		}
		if response.NextURI == "" {
			return outcome, nil
		}
		request, err := http.NewRequest(http.MethodGet, response.NextURI, nil)
		if err != nil {
			return imageQueryOutcome{}, err
		}
		c.setHeaders(request)
		response, err = c.do(request)
		if err != nil {
			return imageQueryOutcome{}, err
		}
		outcome.Pages++
	}
}

func (c *imageTrinoClient) cancel(nextURI string) error {
	request, err := http.NewRequest(http.MethodDelete, nextURI, nil)
	if err != nil {
		return err
	}
	c.setHeaders(request)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("cancellation status %s: %s", response.Status, body)
	}
	return nil
}

func (c *imageTrinoClient) setHeaders(request *http.Request) {
	request.Header.Set("X-Trino-User", "hogql-smoke")
	request.Header.Set("X-Trino-Catalog", "memory")
	request.Header.Set("X-Trino-Schema", "default")
	request.Header.Set("X-Trino-Time-Zone", "UTC")
}

func (c *imageTrinoClient) do(request *http.Request) (imageQueryResponse, error) {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return imageQueryResponse{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return imageQueryResponse{}, fmt.Errorf("Trino request %s %s returned %s: %s", request.Method, request.URL, response.Status, body)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	var decoded imageQueryResponse
	if err := decoder.Decode(&decoded); err != nil {
		return imageQueryResponse{}, err
	}
	return decoded, nil
}

func requireSQLSuccess(t *testing.T, client *imageTrinoClient, statement string) imageQueryOutcome {
	t.Helper()
	outcome, err := client.executeSQL(statement)
	return requireImageQuerySuccess(t, outcome, err)
}

func requireHogQLSuccess(t *testing.T, client *imageTrinoClient, request []byte) imageQueryOutcome {
	t.Helper()
	outcome, err := client.executeHogQL(request)
	return requireImageQuerySuccess(t, outcome, err)
}

func requireHogQLError(t *testing.T, client *imageTrinoClient, request []byte) *imageQueryError {
	t.Helper()
	outcome, err := client.executeHogQL(request)
	return requireImageQueryError(t, outcome, err)
}

func requireImageQuerySuccess(t *testing.T, outcome imageQueryOutcome, err error) imageQueryOutcome {
	t.Helper()
	if err != nil {
		t.Fatalf("execute Trino query: %v", err)
	}
	if outcome.Error != nil {
		t.Fatalf("Trino query failed with %s: %s", outcome.Error.ErrorName, outcome.Error.Message)
	}
	return outcome
}

func requireImageQueryError(t *testing.T, outcome imageQueryOutcome, err error) *imageQueryError {
	t.Helper()
	if err != nil {
		t.Fatalf("execute Trino query: %v", err)
	}
	if outcome.Error == nil {
		t.Fatalf("Trino query unexpectedly succeeded with rows %v", outcome.Rows)
	}
	return outcome.Error
}

func waitForHogQLSuccess(t *testing.T, client *imageTrinoClient, request []byte) imageQueryOutcome {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var last imageQueryOutcome
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = client.executeHogQL(request)
		if lastErr == nil && last.Error == nil {
			return last
		}
		if lastErr == nil && last.Error.ErrorName != "HOGQL_CATALOG_NOT_READY" {
			t.Fatalf("HogQL query failed while waiting for catalog readiness with %s: %s", last.Error.ErrorName, last.Error.Message)
		}
		<-ticker.C
	}
	t.Fatalf("HogQL query did not become ready: transport=%v query=%v", lastErr, last.Error)
	return imageQueryOutcome{}
}
