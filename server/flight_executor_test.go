package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestFlightExecutorWithSessionAddsOwnerEpochHeader(t *testing.T) {
	exec := NewFlightExecutorFromClient(nil, "session-1")
	exec.SetOwnerEpoch(7)
	exec.SetControlMetadata(17, "cp-live:boot-a", 7)

	ctx := exec.withSession(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get("x-duckgres-session"); len(got) != 1 || got[0] != "session-1" {
		t.Fatalf("unexpected session metadata: %#v", got)
	}
	if got := md.Get("x-duckgres-owner-epoch"); len(got) != 1 || got[0] != "7" {
		t.Fatalf("unexpected owner epoch metadata: %#v", got)
	}
	if got := md.Get("x-duckgres-worker-id"); len(got) != 1 || got[0] != "17" {
		t.Fatalf("unexpected worker id metadata: %#v", got)
	}
	if got := md.Get("x-duckgres-cp-instance-id"); len(got) != 1 || got[0] != "cp-live:boot-a" {
		t.Fatalf("unexpected cp instance metadata: %#v", got)
	}
}
