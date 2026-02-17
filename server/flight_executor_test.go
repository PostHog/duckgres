package server

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
)

func TestFlightExecutorMarkDead_QueryContext(t *testing.T) {
	// A dead executor should return ErrWorkerDead without touching the client.
	e := &FlightExecutor{} // client is nil — would panic if accessed
	e.MarkDead()

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrWorkerDead) {
		t.Fatalf("expected ErrWorkerDead, got %v", err)
	}
}

func TestFlightExecutorMarkDead_ExecContext(t *testing.T) {
	e := &FlightExecutor{}
	e.MarkDead()

	_, err := e.ExecContext(context.Background(), "SET x = 1")
	if !errors.Is(err, ErrWorkerDead) {
		t.Fatalf("expected ErrWorkerDead, got %v", err)
	}
}

func TestFlightExecutorMarkDeadIdempotent(t *testing.T) {
	e := &FlightExecutor{}
	e.MarkDead()
	e.MarkDead() // should not panic

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrWorkerDead) {
		t.Fatalf("expected ErrWorkerDead after double MarkDead, got %v", err)
	}
}

func TestRecoverClientPanic_NilPointer(t *testing.T) {
	var err error
	func() {
		defer recoverClientPanic(&err)
		// Simulate the nil pointer dereference that arrow-go causes
		var i *int
		_ = *i //nolint:govet
	}()

	if err == nil {
		t.Fatal("expected error from recovered nil pointer panic")
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash message, got: %v", err)
	}
}

func TestRecoverClientPanic_NonNilPointerRePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected re-panic for non-nil-pointer panic")
		}
		// Should be the original string panic, not a wrapped error
		if s, ok := r.(string); !ok || s != "some other panic" {
			t.Fatalf("expected original panic value, got: %v", r)
		}
	}()

	var err error
	func() {
		defer recoverClientPanic(&err)
		panic("some other panic")
	}()

	t.Fatal("should not reach here — panic should propagate")
}

func TestRecoverClientPanic_RuntimeErrorRePanics(t *testing.T) {
	// runtime.Error that is NOT a nil pointer should re-panic
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected re-panic for non-nil-pointer runtime error")
		}
		if re, ok := r.(runtime.Error); !ok {
			t.Fatalf("expected runtime.Error, got %T: %v", r, r)
		} else if strings.Contains(re.Error(), "nil pointer") {
			t.Fatal("this test should use a non-nil-pointer runtime error")
		}
	}()

	var err error
	func() {
		defer recoverClientPanic(&err)
		// Index out of range is a runtime.Error but not a nil pointer
		s := []int{}
		_ = s[1] //nolint:govet
	}()

	t.Fatal("should not reach here — runtime error should re-panic")
}

func TestFlightExecutorNilClient_QueryContextRecovers(t *testing.T) {
	// Simulate the exact scenario: executor with a nil client (as if Close()
	// nilled it out). QueryContext should recover the panic, not crash.
	e := &FlightExecutor{
		client: nil, // simulates closed client
	}

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error from nil client")
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash recovery message, got: %v", err)
	}
}

func TestFlightExecutorNilClient_ExecContextRecovers(t *testing.T) {
	e := &FlightExecutor{
		client: nil,
	}

	_, err := e.ExecContext(context.Background(), "SET x = 1")
	if err == nil {
		t.Fatal("expected error from nil client")
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash recovery message, got: %v", err)
	}
}
