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
	"github.com/posthog/duckgres/server/observe"
)

func TestCreateSessionWithRegisteredCancel_CancelQueryCancelsWait(t *testing.T) {
	srv := &server.Server{}
	server.InitMinimalServer(srv, server.Config{}, nil)

	key := server.BackendKey{Pid: 1234, SecretKey: 5678}

	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		_, _, err := createSessionWithRegisteredCancel(
			context.Background(),
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

func TestCreateSessionWithRegisteredCancelPreservesCanceledSuccessForCleanup(t *testing.T) {
	srv := &server.Server{}
	server.InitMinimalServer(srv, server.Config{}, nil)

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	wantExecutor := &flightclient.FlightExecutor{}
	pid, executor, err := createSessionWithRegisteredCancel(
		parent,
		srv,
		time.Second,
		server.BackendKey{Pid: 4321, SecretKey: 8765},
		func(context.Context) (int32, *flightclient.FlightExecutor, error) {
			// Model a grant committing concurrently with cancellation. The helper
			// must surface cancellation without discarding the session identity
			// the caller needs to destroy the raced session.
			return 4321, wantExecutor, nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if pid != 4321 || executor != wantExecutor {
		t.Fatalf("canceled success = pid %d, executor %p; want pid 4321, executor %p", pid, executor, wantExecutor)
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

	t.Run("session manager draining", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(ErrSessionManagerDraining)
		if code != "57P03" {
			t.Fatalf("code = %q, want 57P03", code)
		}
		if message != "control plane is draining, retry shortly" {
			t.Fatalf("message = %q", message)
		}
	})

	t.Run("request exceeds org vcpu limit", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(&configstore.OrgConnectionAdmissionRejectedError{
			Reason:         configstore.OrgConnectionAdmissionRejectedOrgVCPU,
			RequestedVCPUs: 4,
			MaximumVCPUs:   2,
		})
		if code != "53400" {
			t.Fatalf("code = %q, want 53400", code)
		}
		want := "requested worker requires 4 vCPUs, exceeding the organization limit of 2 vCPUs; request a smaller worker or raise the limit"
		if message != want {
			t.Fatalf("message = %q, want %q", message, want)
		}
	})

	t.Run("worker capacity exhausted", func(t *testing.T) {
		code, message := sessionCreationErrorResponse(NewWorkerCapacityExhaustedError(45 * time.Second))
		if code != "53300" {
			t.Fatalf("code = %q, want 53300", code)
		}
		want := "no Duckgres worker is currently available; one is being spawned, retry in about 45 seconds"
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

func TestControlPlaneSessionStartResult(t *testing.T) {
	tests := []struct {
		name             string
		err              error
		wantOutcome      string
		wantFailureClass string
	}{
		{name: "success", wantOutcome: "success", wantFailureClass: observe.SessionStartFailureNone},
		{name: "canceled", err: context.Canceled, wantOutcome: "canceled", wantFailureClass: observe.SessionStartFailureCanceled},
		{name: "deadline", err: context.DeadlineExceeded, wantOutcome: "timeout", wantFailureClass: observe.SessionStartFailureCapacity},
		{name: "queue timeout", err: ErrTooManyConnections, wantOutcome: "timeout", wantFailureClass: observe.SessionStartFailureCapacity},
		{name: "draining", err: ErrSessionManagerDraining, wantOutcome: "draining", wantFailureClass: observe.SessionStartFailureLifecycle},
		{name: "worker capacity", err: NewWorkerCapacityExhaustedError(time.Second), wantOutcome: "capacity", wantFailureClass: observe.SessionStartFailureCapacity},
		{
			name: "admission hard rejection",
			err: &configstore.OrgConnectionAdmissionRejectedError{
				Reason:         configstore.OrgConnectionAdmissionRejectedOrgVCPU,
				RequestedVCPUs: 4,
				MaximumVCPUs:   2,
			},
			wantOutcome:      "capacity",
			wantFailureClass: observe.SessionStartFailureClient,
		},
		{name: "generic error", err: errors.New("bootstrap failed"), wantOutcome: "error", wantFailureClass: observe.SessionStartFailureWorker},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutcome, gotFailureClass := controlPlaneSessionStartResult(tt.err)
			if gotOutcome != tt.wantOutcome || gotFailureClass != tt.wantFailureClass {
				t.Fatalf("controlPlaneSessionStartResult(%v) = (%q, %q), want (%q, %q)",
					tt.err, gotOutcome, gotFailureClass, tt.wantOutcome, tt.wantFailureClass)
			}
		})
	}
}

func TestControlPlaneSessionStartOperationResultPrefersContext(t *testing.T) {
	operationErr := errors.New("metadata initialization failed")
	tests := []struct {
		name             string
		contextErr       error
		draining         bool
		wantOutcome      string
		wantFailureClass string
	}{
		{name: "client canceled", contextErr: context.Canceled, wantOutcome: "canceled", wantFailureClass: observe.SessionStartFailureCanceled},
		{name: "operation timed out", contextErr: context.DeadlineExceeded, wantOutcome: "timeout", wantFailureClass: observe.SessionStartFailureMetadataStore},
		{name: "ordinary metadata error", wantOutcome: "error", wantFailureClass: observe.SessionStartFailureMetadataStore},
		{name: "draining wins", contextErr: context.Canceled, draining: true, wantOutcome: "draining", wantFailureClass: observe.SessionStartFailureLifecycle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutcome, gotFailureClass := controlPlaneSessionStartOperationResult(
				operationErr,
				tt.contextErr,
				tt.draining,
				observe.SessionStartFailureMetadataStore,
			)
			if gotOutcome != tt.wantOutcome || gotFailureClass != tt.wantFailureClass {
				t.Fatalf("operation result = (%q, %q), want (%q, %q)",
					gotOutcome, gotFailureClass, tt.wantOutcome, tt.wantFailureClass)
			}
		})
	}
}

func TestMissingOrgStackFailureClass(t *testing.T) {
	tests := []struct {
		name      string
		state     string
		orgExists bool
		want      string
	}{
		{name: "missing org", orgExists: false, want: observe.SessionStartFailureClient},
		{name: "legacy ready state", orgExists: true, state: "", want: observe.SessionStartFailureControlPlane},
		{name: "ready warehouse", orgExists: true, state: string(configstore.ManagedWarehouseStateReady), want: observe.SessionStartFailureControlPlane},
		{name: "failed provisioning", orgExists: true, state: string(configstore.ManagedWarehouseStateFailed), want: observe.SessionStartFailureLifecycle},
		{name: "deleting", orgExists: true, state: string(configstore.ManagedWarehouseStateDeleting), want: observe.SessionStartFailureLifecycle},
		{name: "resharding", orgExists: true, state: string(configstore.ManagedWarehouseStateResharding), want: observe.SessionStartFailureLifecycle},
		{name: "pending", orgExists: true, state: string(configstore.ManagedWarehouseStatePending), want: observe.SessionStartFailureLifecycle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := missingOrgStackFailureClass(tt.state, tt.orgExists); got != tt.want {
				t.Fatalf("missingOrgStackFailureClass(%q, %t) = %q, want %q", tt.state, tt.orgExists, got, tt.want)
			}
		})
	}
}

func TestBeginSessionDrainMarksControlPlaneDraining(t *testing.T) {
	sessions := NewSessionManager(nil, nil)
	cp := &ControlPlane{sessions: sessions}
	if cp.isDraining() {
		t.Fatal("new control plane unexpectedly reports draining")
	}

	cp.beginSessionDrain()

	if !cp.isDraining() {
		t.Fatal("beginSessionDrain must make subsequent connection checks fail closed")
	}
	if !sessions.lifecycle.isClosed() {
		t.Fatal("beginSessionDrain must close the session manager lifecycle")
	}
}

func TestControlPlanePreReadyLifecycleLinearizesWithDrain(t *testing.T) {
	t.Run("drain wins", func(t *testing.T) {
		cp := &ControlPlane{}
		ctx, finish, err := cp.beginPreReadyHandshake(context.Background())
		if err != nil {
			t.Fatalf("begin pre-ready handshake: %v", err)
		}

		cp.beginSessionDrain()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("drain did not cancel the pre-ready handshake")
		}
		if !finish() {
			t.Fatal("pre-ready finish must report that drain linearized first")
		}

		if _, _, err := cp.beginPreReadyHandshake(context.Background()); !errors.Is(err, ErrSessionManagerDraining) {
			t.Fatalf("begin after drain error = %v, want %v", err, ErrSessionManagerDraining)
		}
	})

	t.Run("handshake wins", func(t *testing.T) {
		cp := &ControlPlane{}
		_, finish, err := cp.beginPreReadyHandshake(context.Background())
		if err != nil {
			t.Fatalf("begin pre-ready handshake: %v", err)
		}
		if finish() {
			t.Fatal("pre-ready finish unexpectedly reported drain")
		}

		cp.beginSessionDrain()
		if finish() {
			t.Fatal("repeated finish changed the original linearization result")
		}
	})
}

func TestBeginUpgradeDrainClosesPreReadyLifecycleSynchronously(t *testing.T) {
	cp := &ControlPlane{}
	ctx, finish, err := cp.beginPreReadyHandshake(context.Background())
	if err != nil {
		t.Fatalf("begin pre-ready handshake: %v", err)
	}

	cp.beginUpgradeDrain()
	if !cp.upgradeDraining.Load() {
		t.Fatal("upgrade drain flag was not set")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("upgrade drain did not cancel the pre-ready handshake")
	}
	if !finish() {
		t.Fatal("pre-ready finish must report that upgrade drain linearized first")
	}
}

func TestCapacityMissPolicyForKnownReasons(t *testing.T) {
	for _, tt := range []struct {
		name         string
		reason       configstore.WorkerClaimMissReason
		policyReason configstore.WorkerClaimMissReason
	}{
		{name: "none defaults to no_idle", reason: configstore.WorkerClaimMissReasonNone, policyReason: configstore.WorkerClaimMissReasonNoIdle},
		{name: "no_idle", reason: configstore.WorkerClaimMissReasonNoIdle, policyReason: configstore.WorkerClaimMissReasonNoIdle},
		{name: "org_cap", reason: configstore.WorkerClaimMissReasonOrgCap, policyReason: configstore.WorkerClaimMissReasonOrgCap},
		{name: "shutting_down", reason: configstore.WorkerClaimMissReasonShuttingDown, policyReason: configstore.WorkerClaimMissReasonShuttingDown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := capacityMissPolicyForReason(tt.reason)
			if policy.reason != tt.policyReason {
				t.Fatalf("policy reason = %q, want %q", policy.reason, tt.policyReason)
			}
		})
	}
}
