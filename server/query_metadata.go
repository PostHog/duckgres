package server

import (
	"encoding/json"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/posthog/duckgres/server/querymeta"
)

// maxQueryMetadataLength bounds the JSON blob stored per event. The query log
// lives in the tenant's metadata Postgres — the same database every DuckLake
// postgres_scan hits on the query hot path — so an unbounded column here is a
// latency problem for every query, not just a storage one.
const maxQueryMetadataLength = 8192

// queryMetadataCacheSize is the per-process extraction cache. Client workloads
// are highly repetitive (prepared statements, dbt models, BI tools re-issuing
// the same SQL), so the cache turns a parse into a map lookup for the traffic
// that actually dominates. Entries are small: the extracted Metadata, never the
// AST.
const queryMetadataCacheSize = 2048

var queryMetadataCache = newQueryMetadataCache()

func newQueryMetadataCache() *lru.Cache[string, querymeta.Metadata] {
	cache, err := lru.New[string, querymeta.Metadata](queryMetadataCacheSize)
	if err != nil {
		return nil
	}
	return cache
}

// extractQueryMetadata reports what a statement touches, memoized by exact SQL
// text.
//
// The caller passes the REDACTED statement, so the parser never sees credential
// material and no derivative of it can reach the log. That is stronger than
// skipping secret DDL: redacted secret DDL no longer parses as PostgreSQL, so
// it lands in the lexical fallback and still classifies as admin access.
func extractQueryMetadata(redactedSQL string) querymeta.Metadata {
	if redactedSQL == "" {
		return querymeta.Metadata{}
	}
	if queryMetadataCache != nil {
		if meta, ok := queryMetadataCache.Get(redactedSQL); ok {
			return meta
		}
	}
	meta := querymeta.Extract(redactedSQL)
	if queryMetadataCache != nil {
		queryMetadataCache.Add(redactedSQL, meta)
	}
	return meta
}

// queryMetadataColumns renders extraction output for the query log: the JSON
// blob, the flattened access-kind list, and the completeness flag.
func queryMetadataColumns(meta querymeta.Metadata) (encoded, accessKinds string, complete bool) {
	kinds := make([]string, 0, len(meta.AccessKinds))
	for _, kind := range meta.AccessKinds {
		kinds = append(kinds, string(kind))
	}
	accessKinds = strings.Join(kinds, ",")

	if blob, err := json.Marshal(meta); err == nil {
		encoded = string(blob)
	}
	return encoded, accessKinds, meta.Complete
}

// truncateQueryMetadata caps the stored JSON. Truncation would make the blob
// unparseable, so an oversized payload is replaced with a marker that keeps the
// row honest rather than storing a fragment a consumer might misread.
func truncateQueryMetadata(encoded string) string {
	if len(encoded) <= maxQueryMetadataLength {
		return encoded
	}
	return `{"complete":false,"incomplete_reason":"oversized"}`
}

// queryMetadata returns the extraction for this scope's statement, computing it
// at most once per statement. Extraction is synchronous rather than deferred to
// a background enricher because the QueryStart event is emitted before the
// statement runs, and because a future authorization gate reads the same
// Metadata before deciding whether the statement may run at all.
func (c *clientConn) queryMetadata(scope *queryMetricsScope) querymeta.Metadata {
	if scope == nil {
		return querymeta.Metadata{}
	}
	if scope.metadataDone {
		return scope.metadata
	}
	scope.metadataDone = true
	if c.server == nil || !c.server.cfg.QueryLog.Metadata || scope.queryText == "" {
		return scope.metadata
	}
	scope.metadata = extractQueryMetadata(scope.queryText)
	return scope.metadata
}
