//go:build !kubernetes

package controlplane

import (
	"context"
	"errors"
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
			return
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

func TestSessionCreationErrorResponse(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(context.Canceled)
		if code != "57014" {
			t.Fatalf("code = %q, want 57014", code)
		}
		if message != "canceling authentication due to user request" {
			t.Fatalf("message = %q", message)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(context.DeadlineExceeded)
		if code != "53300" {
			t.Fatalf("code = %q, want 53300", code)
		}
		if message != "timed out waiting for an available worker" {
			t.Fatalf("message = %q", message)
		}
	})

	t.Run("generic error", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(errors.New("worker activation failed"))
		if code != "58000" {
			t.Fatalf("code = %q, want 58000", code)
		}
		if message != "failed to create session: worker activation failed" {
			t.Fatalf("message = %q", message)
		}
	})
}
