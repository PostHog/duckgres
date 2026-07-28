package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/querymeta"
	"github.com/posthog/duckgres/server/usersecrets"
)

func decodeQueryMetadata(t *testing.T, encoded string) querymeta.Metadata {
	t.Helper()
	var meta querymeta.Metadata
	if err := json.Unmarshal([]byte(encoded), &meta); err != nil {
		t.Fatalf("decode query metadata %q: %v", encoded, err)
	}
	return meta
}

// TestQueryLogCarriesAccessMetadata is the end of the RBAC-signal path: what a
// statement reads and writes, and the access class it implies, must land on the
// logged event so a candidate policy can be evaluated against real traffic
// before it denies anything.
func TestQueryLogCarriesAccessMetadata(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.Metadata = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsData

	sql := "INSERT INTO main.daily SELECT id FROM main.events"
	start := time.Unix(1700000000, 0).UTC()
	scope := c.beginQueryMetrics(start)
	scope.queryText = sql
	c.markExecStarted()
	c.logQuery(start, sql, "", "INSERT", 0, 5, "", "", "simple")
	c.finishQueryMetrics(scope)

	if len(exec.entries) != 2 {
		t.Fatalf("expected start + terminal, got %d", len(exec.entries))
	}
	for i, entry := range exec.entries {
		if !entry.MetadataComplete {
			t.Fatalf("entry %d: extraction should be complete for parseable SQL", i)
		}
		if !strings.Contains(entry.AccessKinds, "write") || !strings.Contains(entry.AccessKinds, "read") {
			t.Fatalf("entry %d: access kinds = %q, want read and write", i, entry.AccessKinds)
		}
		meta := decodeQueryMetadata(t, entry.QueryMetadata)
		if len(meta.WriteRelations) != 1 || meta.WriteRelations[0].Raw != "main.daily" {
			t.Fatalf("entry %d: write relations = %v", i, meta.WriteRelations)
		}
		if len(meta.ReadRelations) != 1 || meta.ReadRelations[0].Raw != "main.events" {
			t.Fatalf("entry %d: read relations = %v", i, meta.ReadRelations)
		}
	}
	// Start and terminal describe the same statement, so they must agree.
	if exec.entries[0].AccessKinds != exec.entries[1].AccessKinds {
		t.Fatalf("start and terminal disagree on access: %q vs %q",
			exec.entries[0].AccessKinds, exec.entries[1].AccessKinds)
	}
}

// TestQueryLogMetadataMarksIncompleteExtraction: a statement the PostgreSQL
// parser cannot read must be logged as incomplete, never as "touched nothing".
// This is the property any future authorization gate depends on.
func TestQueryLogMetadataMarksIncompleteExtraction(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.Metadata = true

	sql := "PIVOT main.events ON kind USING sum(v)"
	start := time.Unix(1700000000, 0).UTC()
	scope := c.beginQueryMetrics(start)
	scope.queryText = sql
	c.logQuery(start, sql, "", "SELECT", 0, 0, "", "", "simple")
	c.finishQueryMetrics(scope)

	entry := exec.entries[len(exec.entries)-1]
	if entry.MetadataComplete {
		t.Fatal("unparseable SQL must be logged as incomplete extraction")
	}
	meta := decodeQueryMetadata(t, entry.QueryMetadata)
	if meta.IncompleteReason == "" {
		t.Fatal("an incomplete extraction must record why")
	}
}

// TestQueryLogMetadataNeverParsesSecrets: extraction runs on the redacted text,
// so credential material never reaches the parser or any derived column.
func TestQueryLogMetadataNeverParsesSecrets(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.Metadata = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsAll

	raw := "CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 'topsecret')"
	start := time.Unix(1700000000, 0).UTC()
	scope := c.beginQueryMetrics(start)
	scope.queryText = usersecrets.RedactForLog(raw)
	c.markExecStarted()
	c.logQuery(start, raw, "", "CREATE", 0, 0, "", "", "simple")
	c.finishQueryMetrics(scope)

	for i, entry := range exec.entries {
		if strings.Contains(entry.QueryMetadata, "topsecret") || strings.Contains(entry.QueryMetadata, "AKIAEXAMPLE") {
			t.Fatalf("entry %d leaked credential material into query_metadata: %s", i, entry.QueryMetadata)
		}
	}
	// Redacted secret DDL no longer parses as PostgreSQL, so it lands in the
	// lexical fallback — which must still call it admin-class access rather
	// than shrugging.
	if kinds := exec.entries[0].AccessKinds; !strings.Contains(kinds, "admin") {
		t.Fatalf("secret DDL must classify as admin access, got %q", kinds)
	}
}

func TestQueryLogMetadataDisabled(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.Metadata = false

	start := time.Unix(1700000000, 0).UTC()
	scope := c.beginQueryMetrics(start)
	scope.queryText = "SELECT * FROM main.events"
	c.logQuery(start, "SELECT * FROM main.events", "", "SELECT", 1, 0, "", "", "simple")
	c.finishQueryMetrics(scope)

	entry := exec.entries[len(exec.entries)-1]
	if entry.AccessKinds != "" {
		t.Fatalf("extraction is off; access kinds should be empty, got %q", entry.AccessKinds)
	}
}

// TestQueryMetadataComputedOncePerStatement: the start and terminal events
// share one extraction rather than parsing the same SQL twice.
func TestQueryMetadataComputedOncePerStatement(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	c.server.cfg.QueryLog.Metadata = true

	scope := c.beginQueryMetrics(time.Now())
	defer c.finishQueryMetrics(scope)
	scope.queryText = "SELECT * FROM main.events"

	first := c.queryMetadata(scope)
	if !scope.metadataDone {
		t.Fatal("extraction should be memoized on the scope")
	}
	// Mutating the cached value proves the second call does not re-extract.
	scope.metadata.QueryKind = "sentinel"
	if second := c.queryMetadata(scope); second.QueryKind != "sentinel" {
		t.Fatalf("second call re-extracted instead of reusing the scope value: %+v", second)
	}
	if first.QueryKind != querymeta.KindSelect {
		t.Fatalf("first extraction kind = %q", first.QueryKind)
	}
}

func TestTruncateQueryMetadataReplacesOversizedBlob(t *testing.T) {
	small := `{"complete":true}`
	if got := truncateQueryMetadata(small); got != small {
		t.Fatalf("small blob should pass through, got %q", got)
	}

	oversized := `{"x":"` + strings.Repeat("y", maxQueryMetadataLength) + `"}`
	got := truncateQueryMetadata(oversized)
	if len(got) >= len(oversized) {
		t.Fatal("oversized blob should be replaced")
	}
	// The replacement must stay valid JSON: a truncated fragment would be
	// unparseable, and a consumer might read the fragment as the whole truth.
	meta := decodeQueryMetadata(t, got)
	if meta.Complete {
		t.Fatal("the oversized marker must report incompleteness")
	}
}

func TestExtractQueryMetadataCaches(t *testing.T) {
	sql := "SELECT * FROM main.cache_probe_" + time.Now().Format("150405.000000000")
	first := extractQueryMetadata(sql)
	second := extractQueryMetadata(sql)
	if first.QueryKind != second.QueryKind || len(first.ReadRelations) != len(second.ReadRelations) {
		t.Fatalf("cached extraction differs: %+v vs %+v", first, second)
	}
	if queryMetadataCache == nil {
		t.Skip("cache unavailable")
	}
	if _, ok := queryMetadataCache.Get(sql); !ok {
		t.Fatal("extraction should be memoized by statement text")
	}
}
