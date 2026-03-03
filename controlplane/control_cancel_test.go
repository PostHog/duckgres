package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
)

func TestCreateSessionWithRegisteredCancel_CancelQueryCancelsWait(t *testing.T) {
	srv := &server.Server{}
	server.InitMinimalServer(srv, server.Config{}, nil)

	key := server.BackendKey{Pid: 1234, SecretKey: 5678}

	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		_, _, err := createSessionWithRegisteredCancel(
			srv,
			200*time.Millisecond,
			key,
			func(ctx context.Context) (int32, *server.FlightExecutor, error) {
				close(started)
				<-ctx.Done()
				return 0, nil, ctx.Err()
			},
		)
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("create function did not start")
	}

	if !srv.CancelQuery(key) {
		t.Fatal("expected CancelQuery to find registered query")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected context cancellation error")
		}
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("createSessionWithRegisteredCancel did not return after cancel")
	}

	if srv.CancelQuery(key) {
		t.Fatal("expected query to be unregistered after return")
	}
}
