package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
		done <- handler.doHealthCheck([]byte(`{}`), stream)
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
		done <- handler.doHealthCheck([]byte(`{}`), stream)
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

func TestHealthCheckRejectsMissingOwnerEpochInSharedWarmMode(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDone:     make(chan struct{}),
		startTime:      time.Now(),
		sharedWarmMode: true,
		ownerEpoch:     5,
		ownerCPInstanceID: "cp-live:boot-a",
		workerID:          17,
	}
	close(pool.warmupDone)

	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	err := handler.doHealthCheck([]byte(`{}`), stream)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestCreateSessionRequiresActivationForSharedWarmMode(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDone:     make(chan struct{}),
		startTime:      time.Now(),
		cfg:            server.Config{},
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	body, err := json.Marshal(map[string]any{
		"username": "alice",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	err = handler.doCreateSession(body, stream)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestActivateTenantRejectsDifferentTenantAfterActivation(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDone:     make(chan struct{}),
		startTime:      time.Now(),
		cfg:            server.Config{},
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	pool.createDBConnection = func(server.Config, chan struct{}, string, time.Time, string) (*sql.DB, error) {
		return &sql.DB{}, nil
	}
	pool.activateDBConnection = func(*sql.DB, server.Config, chan struct{}, string) error {
		return nil
	}
	pool.activateTenantFunc = pool.activateTenant

	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	firstBody, err := json.Marshal(ActivationPayload{
		OrgID: "analytics",
	})
	if err != nil {
		t.Fatalf("marshal first request: %v", err)
	}
	if err := handler.doActivateTenant(firstBody, stream); err != nil {
		t.Fatalf("first activation: %v", err)
	}

	secondBody, err := json.Marshal(ActivationPayload{
		OrgID: "billing",
	})
	if err != nil {
		t.Fatalf("marshal second request: %v", err)
	}

	err = handler.doActivateTenant(secondBody, stream)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestCreateSessionRejectsStaleOwnerEpochInSharedWarmMode(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDone:     make(chan struct{}),
		startTime:      time.Now(),
		cfg:            server.Config{},
		sharedWarmMode: true,
		ownerEpoch:     4,
		activation: &activatedTenantRuntime{
			payload: ActivationPayload{
				WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 4},
				OrgID:                 "analytics",
			},
		},
	}
	close(pool.warmupDone)

	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	body, err := json.Marshal(server.WorkerCreateSessionPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch: 3,
		},
		Username: "alice",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	err = handler.doCreateSession(body, stream)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestSessionFromContextRejectsStaleOwnerEpoch(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDone:     make(chan struct{}),
		startTime:      time.Now(),
		sharedWarmMode: true,
		ownerEpoch:     5,
	}
	close(pool.warmupDone)
	pool.sessions["session-1"] = &Session{ID: "session-1"}

	handler := &FlightSQLHandler{pool: pool}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-duckgres-session", "session-1",
		"x-duckgres-owner-epoch", "4",
		"x-duckgres-worker-id", "17",
		"x-duckgres-cp-instance-id", "cp-live:boot-a",
	))

	_, err := handler.sessionFromContext(ctx)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestSessionFromContextRejectsMismatchedControlIdentity(t *testing.T) {
	pool := &SessionPool{
		sessions:           make(map[string]*Session),
		stopRefresh:        make(map[string]func()),
		warmupDone:         make(chan struct{}),
		startTime:          time.Now(),
		sharedWarmMode:     true,
		ownerEpoch:         5,
		ownerCPInstanceID:  "cp-live:boot-a",
		workerID:           17,
	}
	close(pool.warmupDone)
	pool.sessions["session-1"] = &Session{ID: "session-1"}

	handler := &FlightSQLHandler{pool: pool}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-duckgres-session", "session-1",
		"x-duckgres-owner-epoch", "5",
		"x-duckgres-worker-id", "18",
		"x-duckgres-cp-instance-id", "cp-other:boot-b",
	))

	_, err := handler.sessionFromContext(ctx)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}
