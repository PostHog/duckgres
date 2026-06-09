//go:build !kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
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
			func(ctx context.Context) (int32, *flightclient.FlightExecutor, error) {
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

	t.Run("too many connections", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(ErrTooManyConnections)
		if code != "53300" {
			t.Fatalf("code = %q, want 53300", code)
		}
		if message != "too many connections" {
			t.Fatalf("message = %q", message)
		}
	})

	t.Run("warm capacity exhausted", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(NewWorkerCapacityExhaustedError(45 * time.Second))
		if code != "53300" {
			t.Fatalf("code = %q, want 53300", code)
		}
		want := "no warm Duckgres worker is currently available; retry in about 45 seconds"
		if message != want {
			t.Fatalf("message = %q, want %q", message, want)
		}
	})

	for _, tt := range []struct {
		name    string
		reason  configstore.WorkerClaimMissReason
		message string
	}{
		{
			name:    "org capacity exhausted",
			reason:  configstore.WorkerClaimMissReasonOrgCap,
			message: "your organization has reached its maximum number of concurrent Duckgres workers and they are all busy; retry once a query finishes",
		},
		{
			name:    "global capacity exhausted",
			reason:  configstore.WorkerClaimMissReasonGlobalCap,
			message: "Duckgres worker capacity is currently exhausted; retry later",
		},
		{
			name:    "control plane shutting down",
			reason:  configstore.WorkerClaimMissReasonShuttingDown,
			message: "Duckgres control plane is shutting down; retry later",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, message := sessionCreationErrorResponse(NewWorkerCapacityExhaustedErrorForReason(tt.reason, 45*time.Second))
			if code != "53300" {
				t.Fatalf("code = %q, want 53300", code)
			}
			if message != tt.message {
				t.Fatalf("message = %q, want %q", message, tt.message)
			}
		})
	}

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

func TestWarmCapacityMissPolicyForKnownReasons(t *testing.T) {
	for _, tt := range []struct {
		name                string
		reason              configstore.WorkerClaimMissReason
		policyReason        configstore.WorkerClaimMissReason
		recordDynamicDemand bool
	}{
		{name: "none defaults to no_idle", reason: configstore.WorkerClaimMissReasonNone, policyReason: configstore.WorkerClaimMissReasonNoIdle, recordDynamicDemand: true},
		{name: "no_idle", reason: configstore.WorkerClaimMissReasonNoIdle, policyReason: configstore.WorkerClaimMissReasonNoIdle, recordDynamicDemand: true},
		{name: "org_cap", reason: configstore.WorkerClaimMissReasonOrgCap, policyReason: configstore.WorkerClaimMissReasonOrgCap, recordDynamicDemand: false},
		{name: "global_cap", reason: configstore.WorkerClaimMissReasonGlobalCap, policyReason: configstore.WorkerClaimMissReasonGlobalCap, recordDynamicDemand: false},
		{name: "shutting_down", reason: configstore.WorkerClaimMissReasonShuttingDown, policyReason: configstore.WorkerClaimMissReasonShuttingDown, recordDynamicDemand: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := capacityMissPolicyForReason(tt.reason)
			if policy.reason != tt.policyReason {
				t.Fatalf("policy reason = %q, want %q", policy.reason, tt.policyReason)
			}
			if policy.recordDynamicDemand != tt.recordDynamicDemand {
				t.Fatalf("recordDynamicDemand = %v, want %v", policy.recordDynamicDemand, tt.recordDynamicDemand)
			}
		})
	}
}
