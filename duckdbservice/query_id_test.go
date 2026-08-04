package duckdbservice

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/posthog/duckgres/server/wire"
)

func TestQueryIDFromContext(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		wire.QueryIDMetadataKey, "019fa916-0918-76dd-bc75-591a7ec4e8fa",
	))
	if got := queryIDFromContext(ctx); got != "019fa916-0918-76dd-bc75-591a7ec4e8fa" {
		t.Fatalf("query id = %q", got)
	}

	// A control plane that predates the header must not break the worker.
	if got := queryIDFromContext(context.Background()); got != "" {
		t.Fatalf("missing metadata should yield an empty id, got %q", got)
	}
	empty := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-other", "v"))
	if got := queryIDFromContext(empty); got != "" {
		t.Fatalf("missing header should yield an empty id, got %q", got)
	}
}

func TestWithQueryIDAttr(t *testing.T) {
	base := []any{"session", "abc"}
	if got := withQueryIDAttr(base, ""); len(got) != len(base) {
		t.Fatalf("an absent id must not add an attribute, got %v", got)
	}
	got := withQueryIDAttr(base, "qid")
	if len(got) != len(base)+2 || got[len(got)-2] != "query_id" || got[len(got)-1] != "qid" {
		t.Fatalf("query id attribute not appended: %v", got)
	}
}

// TestSessionCurrentQueryID covers the value the progress monitor reads from
// another goroutine while the RPC path writes it.
func TestSessionCurrentQueryID(t *testing.T) {
	var session Session
	if got := session.CurrentQueryID(); got != "" {
		t.Fatalf("an idle session has no query id, got %q", got)
	}
	session.setCurrentQueryID("qid-1")
	if got := session.CurrentQueryID(); got != "qid-1" {
		t.Fatalf("query id = %q, want qid-1", got)
	}
	session.setCurrentQueryID("")
	if got := session.CurrentQueryID(); got != "" {
		t.Fatalf("a finished statement must clear the id, got %q", got)
	}

	var nilSession *Session
	nilSession.setCurrentQueryID("x")
	if got := nilSession.CurrentQueryID(); got != "" {
		t.Fatalf("nil session must be safe, got %q", got)
	}
}
