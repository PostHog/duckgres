package duckdbservice

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mockDoActionStream implements flight.FlightService_DoActionServer for testing.
type mockDoActionStream struct {
	grpc.ServerStream
	results []*flight.Result
}

func (m *mockDoActionStream) Send(r *flight.Result) error {
	m.results = append(m.results, r)
	return nil
}

func (m *mockDoActionStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockDoActionStream) SendHeader(metadata.MD) error { return nil }
func (m *mockDoActionStream) SetTrailer(metadata.MD)       {}

func TestHealthCheckBlocksUntilWarmup(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
	}
	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	// Health check in a goroutine — should block until warmup completes
	done := make(chan error, 1)
	go func() {
		done <- handler.doHealthCheck(stream)
	}()

	// Verify it hasn't returned after 100ms
	select {
	case <-done:
		t.Fatal("health check returned before warmup completed")
	case <-time.After(100 * time.Millisecond):
		// good, still blocking
	}

	// Simulate warmup completing
	close(pool.warmupDone)

	// Now it should return quickly
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("health check returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("health check didn't return after warmup completed")
	}

	// Verify the response was sent
	if len(stream.results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(stream.results))
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(stream.results[0].Body, &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["healthy"] != true {
		t.Errorf("expected healthy=true, got %v", resp["healthy"])
	}
}

func TestHealthCheckReturnsImmediatelyAfterWarmup(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
	}
	// Warmup already completed
	close(pool.warmupDone)

	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	done := make(chan error, 1)
	go func() {
		done <- handler.doHealthCheck(stream)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("health check returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("health check blocked even though warmup was already done")
	}
}
