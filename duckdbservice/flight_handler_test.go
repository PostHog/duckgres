package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// mockDoActionStream implements flight.FlightService_DoActionServer for testing.
type mockDoActionStream struct {
	grpc.ServerStream
	ctx     context.Context
	results []*flight.Result
	sendErr error
}

func (m *mockDoActionStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *mockDoActionStream) Send(r *flight.Result) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.results = append(m.results, r)
	return nil
}

func (m *mockDoActionStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockDoActionStream) SendHeader(metadata.MD) error { return nil }
func (m *mockDoActionStream) SetTrailer(metadata.MD)       {}

type testEndTransactionRequest struct {
	transactionID []byte
	action        flightsql.EndTransactionRequestType
}

func (r testEndTransactionRequest) GetTransactionId() []byte {
	return r.transactionID
}

func (r testEndTransactionRequest) GetAction() flightsql.EndTransactionRequestType {
	return r.action
}

type testPreparedStatementQuery struct {
	handle []byte
}

func (q testPreparedStatementQuery) GetPreparedStatementHandle() []byte {
	return q.handle
}

type testGetDBSchemas struct {
	catalog *string
	pattern *string
}

func (g testGetDBSchemas) GetCatalog() *string {
	return g.catalog
}

func (g testGetDBSchemas) GetDBSchemaFilterPattern() *string {
	return g.pattern
}

type testGetTables struct{}

func (testGetTables) GetCatalog() *string {
	return nil
}

func (testGetTables) GetDBSchemaFilterPattern() *string {
	return nil
}

func (testGetTables) GetTableNameFilterPattern() *string {
	return nil
}

func (testGetTables) GetTableTypes() []string {
	return nil
}

func (testGetTables) GetIncludeSchema() bool {
	return false
}

type testStatementQueryTicket struct {
	handle []byte
}

func (t testStatementQueryTicket) GetStatementHandle() []byte {
	return t.handle
}

type testStatementUpdate struct {
	query         string
	transactionID []byte
}

func (u testStatementUpdate) GetQuery() string {
	return u.query
}

func (u testStatementUpdate) GetTransactionId() []byte {
	return u.transactionID
}

type testStatementQuery struct {
	query         string
	transactionID []byte
}

func (q testStatementQuery) GetQuery() string {
	return q.query
}

func (q testStatementQuery) GetTransactionId() []byte {
	return q.transactionID
}

func TestStatementFlightInfoAllowsNextQueryBeforePriorDoGet(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:             "session-1",
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	if _, err := handler.GetFlightInfoStatement(ctx, testStatementQuery{query: ""}, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("first GetFlightInfoStatement: %v", err)
	}

	if _, err := handler.GetFlightInfoStatement(ctx, testStatementQuery{query: ""}, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("second GetFlightInfoStatement should not be blocked by pending DoGet: %v", err)
	}

	var handleIDs []string
	session.mu.RLock()
	for id := range session.queries {
		handleIDs = append(handleIDs, id)
	}
	session.mu.RUnlock()
	if len(handleIDs) != 2 {
		t.Fatalf("expected two query handles, got %d", len(handleIDs))
	}

	for _, handleID := range handleIDs {
		_, ch, err := handler.DoGetStatement(ctx, testStatementQueryTicket{handle: []byte(handleID)})
		if err != nil {
			t.Fatalf("DoGetStatement %s: %v", handleID, err)
		}
		for chunk := range ch {
			if chunk.Err != nil {
				t.Fatalf("DoGetStatement stream error: %v", chunk.Err)
			}
			if chunk.Data != nil {
				chunk.Data.Release()
			}
		}
	}
}

func TestStatementFlightInfoNonEmptyAllowsNextQueryBeforePriorDoGet(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	if _, err := handler.GetFlightInfoStatement(ctx, testStatementQuery{query: "SELECT 1"}, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("first GetFlightInfoStatement: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 1 {
		t.Fatalf("active drain work=%d want 1 before DoGet", got)
	}

	if _, err := handler.GetFlightInfoStatement(ctx, testStatementQuery{query: "SELECT 2"}, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("second GetFlightInfoStatement should not be blocked by pending DoGet: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 2 {
		t.Fatalf("active drain work=%d want 2 before DoGet", got)
	}

	var handleIDs []string
	session.mu.RLock()
	for id := range session.queries {
		handleIDs = append(handleIDs, id)
	}
	session.mu.RUnlock()
	if len(handleIDs) != 2 {
		t.Fatalf("expected two query handles, got %d", len(handleIDs))
	}

	for _, handleID := range handleIDs {
		_, ch, err := handler.DoGetStatement(ctx, testStatementQueryTicket{handle: []byte(handleID)})
		if err != nil {
			t.Fatalf("DoGetStatement %s: %v", handleID, err)
		}
		for chunk := range ch {
			if chunk.Err != nil {
				t.Fatalf("DoGetStatement stream error: %v", chunk.Err)
			}
			if chunk.Data != nil {
				chunk.Data.Release()
			}
		}
	}
	if got := pool.ActiveDrainWork(); got != 0 {
		t.Fatalf("active drain work=%d want 0 after DoGet", got)
	}
}

func TestHealthCheckBlocksUntilWarmup(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
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

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
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

func TestHealthCheckReportsDraining(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
	}
	close(pool.warmupDone)
	pool.BeginDrain()

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	stream := &mockDoActionStream{}

	if err := handler.doHealthCheck([]byte(`{}`), stream); err != nil {
		t.Fatalf("health check returned error: %v", err)
	}
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
	if resp["draining"] != true {
		t.Errorf("expected draining=true, got %v", resp["draining"])
	}
}

func TestHealthCheckAcceptsMismatchedEpochInSharedWarmMode(t *testing.T) {
	pool := &SessionPool{
		sessions:          make(map[string]*Session),
		stopRefresh:       make(map[string]func()),
		warmupDone:        make(chan struct{}),
		startTime:         time.Now(),
		sharedWarmMode:    true,
		ownerEpoch:        5,
		ownerCPInstanceID: "cp-live:boot-a",
		workerID:          17,
	}
	close(pool.warmupDone)

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	stream := &mockDoActionStream{}

	// Health checks no longer validate epoch — a fresh CP with epoch 0 should
	// be able to health-check workers activated by a previous CP. Ownership is
	// serialized by the config store, not by worker-side epoch checks.
	err := handler.doHealthCheck([]byte(`{}`), stream)
	if err != nil {
		t.Fatalf("health check should succeed regardless of epoch mismatch, got %v", err)
	}
}

func TestCreateSessionRejectsWhileDraining(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
		cfg:         server.Config{Users: map[string]string{"alice": "pass"}},
	}
	close(pool.warmupDone)
	pool.BeginDrain()

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	stream := &mockDoActionStream{}

	body, err := json.Marshal(server.WorkerCreateSessionPayload{
		Username: "alice",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	err = handler.doCreateSession(body, stream)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while draining, got %v", err)
	}
}

func TestCreateSessionSendFailureDestroysSession(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
		cfg:         server.Config{Users: map[string]string{"alice": "pass"}},
		duckLakeSem: make(chan struct{}, 1),
	}
	close(pool.warmupDone)
	pool.createDBPair = func(server.Config, chan struct{}, string, time.Time, string) (*DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return PairFromMain(db), nil
	}

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	stream := &mockDoActionStream{sendErr: errors.New("send failed")}

	body, err := json.Marshal(server.WorkerCreateSessionPayload{Username: "alice"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	if err := handler.doCreateSession(body, stream); err == nil {
		t.Fatal("expected send failure")
	}
	if got := len(pool.sessions); got != 0 {
		t.Fatalf("expected failed response to destroy created session, got %d sessions", got)
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

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
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

	pool.createDBPair = func(server.Config, chan struct{}, string, time.Time, string) (*DuckDBPair, error) {
		return PairFromMain(&sql.DB{}), nil
	}
	pool.activateDBConnection = func(*sql.DB, server.Config, chan struct{}, string) error {
		return nil
	}
	pool.activateTenantFunc = pool.activateTenant

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	stream := &mockDoActionStream{}

	firstBody, err := json.Marshal(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   1,
			CPInstanceID: "cp-live:boot-a",
			WorkerID:     17,
		},
		OrgID: "analytics",
	})
	if err != nil {
		t.Fatalf("marshal first request: %v", err)
	}
	if err := handler.doActivateTenant(firstBody, stream); err != nil {
		t.Fatalf("first activation: %v", err)
	}

	secondBody, err := json.Marshal(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   2,
			CPInstanceID: "cp-live:boot-a",
			WorkerID:     17,
		},
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

func TestCreateSessionAcceptsMismatchedEpochInSharedWarmMode(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDone:     make(chan struct{}),
		startTime:      time.Now(),
		cfg:            server.Config{Users: map[string]string{"alice": "pass"}},
		sharedWarmMode: true,
		ownerEpoch:     4,
		duckLakeSem:    make(chan struct{}, 1),
		activation: &activatedTenantRuntime{
			payload: ActivationPayload{
				WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 4},
				OrgID:                 "analytics",
			},
		},
	}
	close(pool.warmupDone)
	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return PairFromMain(db), nil
	}

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	stream := &mockDoActionStream{}

	// Session creation with a mismatched epoch should now succeed past the
	// validateControlMetadata check. Epoch/CP-instance validation is no longer
	// enforced on the worker side.
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
	if err != nil {
		t.Fatalf("create session should succeed regardless of epoch mismatch, got %v", err)
	}
}

func TestSessionFromContextAcceptsMismatchedEpoch(t *testing.T) {
	pool := &SessionPool{
		sessions:          make(map[string]*Session),
		stopRefresh:       make(map[string]func()),
		warmupDone:        make(chan struct{}),
		startTime:         time.Now(),
		sharedWarmMode:    true,
		ownerEpoch:        5,
		ownerCPInstanceID: "cp-live:boot-a",
		workerID:          17,
	}
	close(pool.warmupDone)
	pool.sessions["session-1"] = &Session{ID: "session-1"}

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-duckgres-session", "session-1",
		"x-duckgres-owner-epoch", "4",
		"x-duckgres-worker-id", "17",
		"x-duckgres-cp-instance-id", "cp-live:boot-a",
	))

	// Epoch mismatches are no longer rejected — ownership is serialized
	// by the config store, not worker-side epoch checks.
	_, err := handler.sessionFromContext(ctx)
	if err != nil {
		t.Fatalf("expected mismatched epoch to be accepted, got %v", err)
	}
}

func TestSessionFromContextRejectsMismatchedControlIdentity(t *testing.T) {
	pool := &SessionPool{
		sessions:          make(map[string]*Session),
		stopRefresh:       make(map[string]func()),
		warmupDone:        make(chan struct{}),
		startTime:         time.Now(),
		sharedWarmMode:    true,
		ownerEpoch:        5,
		ownerCPInstanceID: "cp-live:boot-a",
		workerID:          17,
	}
	close(pool.warmupDone)
	pool.sessions["session-1"] = &Session{ID: "session-1"}

	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
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

func TestEndTransactionKeepsDrainOpenForOpenTransaction(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:       "session-1",
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))

	txnID, err := handler.BeginTransaction(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	pool.BeginDrain()
	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("expected open transaction to keep drain active")
	}

	if err := handler.EndTransaction(ctx, testEndTransactionRequest{
		transactionID: txnID,
		action:        flightsql.EndTransactionRollback,
	}); err != nil {
		t.Fatalf("rollback during drain: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected drain to complete after transaction finalization")
	}
}

func TestEndTransactionRollbackReleasesAbandonedStatementOperation(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	txnID, err := handler.BeginTransaction(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := handler.GetFlightInfoStatement(ctx, testStatementQuery{query: "SELECT 1", transactionID: txnID}, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get in-transaction flight info: %v", err)
	}

	if err := handler.EndTransaction(ctx, testEndTransactionRequest{
		transactionID: txnID,
		action:        flightsql.EndTransactionRollback,
	}); err != nil {
		session.mu.Lock()
		ttx := session.txns[string(txnID)]
		if ttx != nil {
			delete(session.txns, string(txnID))
			delete(session.txnOwner, string(txnID))
		}
		session.mu.Unlock()
		if ttx != nil && ttx.tx != nil {
			_ = session.rollbackTx(ttx.tx)
		}
		if ttx != nil && ttx.finishDrain != nil {
			ttx.finishDrain()
		}
		t.Fatalf("rollback should clean up abandoned in-transaction statement: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 0 {
		t.Fatalf("active drain work=%d want 0 after rollback", got)
	}
}

func TestEndTransactionRollbackReleasesAbandonedMetadataOperation(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	txnID, err := handler.BeginTransaction(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := handler.GetFlightInfoSchemas(ctx, testGetDBSchemas{}, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get metadata flight info: %v", err)
	}

	if err := handler.EndTransaction(ctx, testEndTransactionRequest{
		transactionID: txnID,
		action:        flightsql.EndTransactionRollback,
	}); err != nil {
		session.mu.Lock()
		ttx := session.txns[string(txnID)]
		if ttx != nil {
			delete(session.txns, string(txnID))
			delete(session.txnOwner, string(txnID))
		}
		session.mu.Unlock()
		if ttx != nil && ttx.tx != nil {
			_ = session.rollbackTx(ttx.tx)
		}
		if ttx != nil && ttx.finishDrain != nil {
			ttx.finishDrain()
		}
		t.Fatalf("rollback should clean up abandoned in-transaction metadata: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 0 {
		t.Fatalf("active drain work=%d want 0 after rollback", got)
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	if len(session.metadataDrains) != 0 {
		t.Fatalf("metadata drains were not consumed: %v", session.metadataDrains)
	}
}

func TestEndTransactionNotFoundDoesNotConsumeMetadataOperation(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	txnID, err := handler.BeginTransaction(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() {
		var rollbackTxs []*sql.Tx
		var releaseDrains []func()
		session.mu.Lock()
		for id, ttx := range session.txns {
			if ttx.tx != nil {
				rollbackTxs = append(rollbackTxs, ttx.tx)
			}
			if ttx.finishDrain != nil {
				releaseDrains = append(releaseDrains, ttx.finishDrain)
				ttx.finishDrain = nil
			}
			delete(session.txns, id)
			delete(session.txnOwner, id)
		}
		for key, drain := range session.metadataDrains {
			releaseDrains = appendDrainTokenFunc(releaseDrains, drain)
			delete(session.metadataDrains, key)
		}
		session.mu.Unlock()
		for _, tx := range rollbackTxs {
			_ = session.rollbackTx(tx)
		}
		for _, release := range releaseDrains {
			releaseDrainFunc(release)
		}
	}()
	cmd := testGetDBSchemas{}
	if _, err := handler.GetFlightInfoSchemas(ctx, cmd, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get metadata flight info: %v", err)
	}

	err = handler.EndTransaction(ctx, testEndTransactionRequest{
		transactionID: []byte("txn-does-not-exist"),
		action:        flightsql.EndTransactionRollback,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected bogus transaction to be rejected as not found, got %v", err)
	}
	session.mu.RLock()
	metadataDrainCount := len(session.metadataDrains)
	session.mu.RUnlock()
	if metadataDrainCount != 1 {
		t.Fatalf("bogus rollback consumed metadata drains, got %d entries", metadataDrainCount)
	}

	_, ch, err := handler.DoGetDBSchemas(ctx, cmd)
	if err != nil {
		t.Fatalf("metadata DoGet should still consume the original continuation: %v", err)
	}
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("metadata stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
	if err := handler.EndTransaction(ctx, testEndTransactionRequest{
		transactionID: txnID,
		action:        flightsql.EndTransactionRollback,
	}); err != nil {
		t.Fatalf("rollback real transaction: %v", err)
	}
}

func TestEndTransactionNotFoundDoesNotDeleteLivePreparedHandle(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID: "session-1",
		queries: map[string]*QueryHandle{
			"prep-1": {
				Query:    "SELECT 1",
				Schema:   arrow.NewSchema(nil, nil),
				TxnID:    "txn-1",
				Prepared: true,
			},
		},
		metadataDrains: make(map[string]drainToken),
		txns: map[string]*trackedTx{
			"txn-1": {},
		},
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	err := handler.EndTransaction(ctx, testEndTransactionRequest{
		transactionID: []byte("txn-1"),
		action:        flightsql.EndTransactionRollback,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected EndTransaction to report missing transaction body, got %v", err)
	}
	session.mu.RLock()
	_, stillPresent := session.queries["prep-1"]
	session.mu.RUnlock()
	if !stillPresent {
		t.Fatal("busy EndTransaction deleted a live prepared handle")
	}
}

func TestRawSQLTransactionKeepsDrainOpenUntilCommit(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "BEGIN"}); err != nil {
		t.Fatalf("raw BEGIN: %v", err)
	}

	pool.BeginDrain()
	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("expected raw SQL transaction to keep drain active")
	}

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "COMMIT"}); err != nil {
		t.Fatalf("raw COMMIT during drain: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected raw SQL transaction drain token to release on COMMIT")
	}
}

func TestRawSQLTransactionKeepsDrainOpenAfterFailedCommit(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "BEGIN"}); err != nil {
		t.Fatalf("raw BEGIN: %v", err)
	}
	pool.BeginDrain()

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "COMMIT INVALID"}); err == nil {
		t.Fatal("expected invalid COMMIT to fail")
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("expected failed COMMIT to keep raw SQL transaction drain active")
	}

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "ROLLBACK"}); err != nil {
		t.Fatalf("raw ROLLBACK during drain: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected raw SQL transaction drain token to release on ROLLBACK")
	}
}

func TestPreparedStatementDoGetContinuesAfterDrainStarts(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID: "session-1",
		queries: map[string]*QueryHandle{
			"prep-1": {Query: "", Schema: arrow.NewSchema(nil, nil)},
		},
		txns: make(map[string]*trackedTx),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))
	cmd := testPreparedStatementQuery{handle: []byte("prep-1")}

	if _, err := handler.GetFlightInfoPreparedStatement(ctx, cmd, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get prepared flight info: %v", err)
	}
	pool.BeginDrain()

	_, ch, err := handler.DoGetPreparedStatement(ctx, cmd)
	if err != nil {
		t.Fatalf("prepared DoGet should continue after drain starts: %v", err)
	}
	for range ch {
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected prepared continuation to release drain work")
	}
}

func TestClosePreparedStatementReleasesAbandonedOperation(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID: "session-1",
		queries: map[string]*QueryHandle{
			"prep-1": {Query: "", Schema: arrow.NewSchema(nil, nil), Prepared: true},
		},
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))
	cmd := testPreparedStatementQuery{handle: []byte("prep-1")}

	if _, err := handler.GetFlightInfoPreparedStatement(ctx, cmd, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get prepared flight info: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 1 {
		t.Fatalf("active drain work=%d want 1 before close", got)
	}

	if err := handler.ClosePreparedStatement(ctx, cmd); err != nil {
		t.Fatalf("close prepared should clean up abandoned pending operation: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 0 {
		t.Fatalf("active drain work=%d want 0 after close", got)
	}
}

func TestDoGetPreparedStatementRequiresPendingFlightInfo(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID: "session-1",
		queries: map[string]*QueryHandle{
			"prep-1": {Query: "", Schema: arrow.NewSchema(nil, nil), Prepared: true},
		},
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))
	cmd := testPreparedStatementQuery{handle: []byte("prep-1")}

	if _, _, err := handler.DoGetPreparedStatement(ctx, cmd); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected DoGet without GetFlightInfo handoff to fail, got %v", err)
	}
}

func TestDoGetStatementEmptyQueryDeletesHandle(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID: "session-1",
		queries: map[string]*QueryHandle{
			"query-1": {Query: "", Schema: arrow.NewSchema(nil, nil)},
		},
		txns: make(map[string]*trackedTx),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))

	_, ch, err := handler.DoGetStatement(ctx, testStatementQueryTicket{handle: []byte("query-1")})
	if err != nil {
		t.Fatalf("DoGetStatement: %v", err)
	}
	for range ch {
	}

	session := pool.sessions["session-1"]
	session.mu.RLock()
	_, ok := session.queries["query-1"]
	session.mu.RUnlock()
	if ok {
		t.Fatal("expected empty query handle to be deleted")
	}
}

func TestDoGetStatementConsumesHandleBeforeStreaming(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:   "session-1",
		Conn: conn,
		queries: map[string]*QueryHandle{
			"query-1": {Query: "SELECT 1", Schema: arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)},
		},
		txns: make(map[string]*trackedTx),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))

	_, ch, err := handler.DoGetStatement(ctx, testStatementQueryTicket{handle: []byte("query-1")})
	if err != nil {
		t.Fatalf("first DoGetStatement: %v", err)
	}
	if _, _, err := handler.DoGetStatement(ctx, testStatementQueryTicket{handle: []byte("query-1")}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected duplicate DoGetStatement to be rejected, got %v", err)
	}
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("first stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
}

func TestDoGetStatementKeepsRawSQLTransactionDrainOpenBeforeStreamingStarts(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("begin raw transaction: %v", err)
	}

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin raw transaction drain work: %v", err)
	}
	session := &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
		sqlTxDrain:     finishDrain,
	}
	session.sqlTxActive.Store(true)
	session.sqlTxLastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())
	session.queries["query-1"] = &QueryHandle{
		Query:     "SELECT 1",
		Schema:    arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil),
		createdAt: time.Now(),
	}
	pool.sessions[session.ID] = session
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	_, ch, err := handler.DoGetStatement(ctx, testStatementQueryTicket{handle: []byte("query-1")})
	if err != nil {
		t.Fatalf("DoGetStatement: %v", err)
	}

	pool.BeginDrain()
	pool.reapIdle(time.Now())
	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("accepted DoGet must keep raw SQL transaction drain work open before its goroutine starts streaming")
	}

	for chunk := range ch {
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
}

func TestMetadataDoGetContinuesAfterDrainStarts(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:       "session-1",
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))
	cmd := testGetDBSchemas{}

	if _, err := handler.GetFlightInfoSchemas(ctx, cmd, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get schemas flight info: %v", err)
	}
	pool.BeginDrain()

	_, ch, err := handler.DoGetDBSchemas(ctx, cmd)
	if err != nil {
		t.Fatalf("metadata DoGet should continue after drain starts: %v", err)
	}
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("metadata stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
}

func TestMetadataGetFlightInfoAllowsMultiplePendingDescriptors(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))
	firstPattern := "%"
	secondPattern := "main"
	first := testGetDBSchemas{pattern: &firstPattern}
	second := testGetDBSchemas{pattern: &secondPattern}

	if _, err := handler.GetFlightInfoSchemas(ctx, first, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("first GetFlightInfoSchemas: %v", err)
	}
	if _, err := handler.GetFlightInfoSchemas(ctx, second, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("second GetFlightInfoSchemas should not be blocked by pending metadata DoGet: %v", err)
	}

	_, ch, err := handler.DoGetDBSchemas(ctx, first)
	if err != nil {
		t.Fatalf("first DoGetDBSchemas: %v", err)
	}
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("first stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}

	_, ch, err = handler.DoGetDBSchemas(ctx, second)
	if err != nil {
		t.Fatalf("second DoGetDBSchemas: %v", err)
	}
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("second stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
	session := pool.sessions["session-1"]
	session.mu.RLock()
	defer session.mu.RUnlock()
	if len(session.metadataDrains) != 0 {
		t.Fatalf("metadata drains were not consumed: %v", session.metadataDrains)
	}
}

func TestMetadataDoGetRequiresPendingFlightInfo(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:             "session-1",
		Conn:           conn,
		queries:        make(map[string]*QueryHandle),
		metadataDrains: make(map[string]drainToken),
		txns:           make(map[string]*trackedTx),
		txnOwner:       make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))

	if _, _, err := handler.DoGetDBSchemas(ctx, testGetDBSchemas{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected metadata DoGet without GetFlightInfo handoff to fail, got %v", err)
	}
}

func TestTablesMetadataDoGetContinuesAfterDrainStarts(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	pool.sessions["session-1"] = &Session{
		ID:       "session-1",
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "session-1"))
	cmd := testGetTables{}

	if _, err := handler.GetFlightInfoTables(ctx, cmd, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("get tables flight info: %v", err)
	}
	pool.BeginDrain()

	_, ch, err := handler.DoGetTables(ctx, cmd)
	if err != nil {
		t.Fatalf("table metadata DoGet should continue after drain starts: %v", err)
	}
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("table metadata stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
		}
	}
}

func TestSendStreamChunkReturnsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := make(chan flight.StreamChunk)

	if sendStreamChunk(ctx, ch, flight.StreamChunk{}) {
		t.Fatal("expected send to fail after context cancellation")
	}
}
