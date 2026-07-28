package duckdbservice

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/posthog/duckgres/server/wire"
)

// queryIDFromContext returns the control plane's per-statement query ID from an
// incoming worker RPC, or "" when the caller predates the header.
//
// This is the worker end of the correlation chain. The pairing that matters for
// triage is a QueryStart row with no terminal — a statement whose worker died
// mid-flight, which by definition cannot log its own ending. Stamping the same
// ID on worker-side logs is what turns that unpaired row into an answer: the
// pod's last words about the statement carry the ID the query log is missing.
func queryIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(wire.QueryIDMetadataKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// withQueryIDAttr appends the query ID to a slog attribute list when present,
// so log lines stay clean on paths that have no ID.
func withQueryIDAttr(attrs []any, queryID string) []any {
	if queryID == "" {
		return attrs
	}
	return append(attrs, "query_id", queryID)
}
