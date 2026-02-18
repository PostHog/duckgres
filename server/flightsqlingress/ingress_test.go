package flightsqlingress

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type testExecResult struct {
	affected int64
	err      error
}

type testServerTransportStream struct {
	header  metadata.MD
	trailer metadata.MD
}

func (s *testServerTransportStream) Method() string {
	return "/duckgres.test/CloseSession"
}

func (s *testServerTransportStream) SetHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *testServerTransportStream) SendHeader(md metadata.MD) error {
	return s.SetHeader(md)
}

func (s *testServerTransportStream) SetTrailer(md metadata.MD) error {
	s.trailer = metadata.Join(s.trailer, md)
	return nil
}

func (r testExecResult) RowsAffected() (int64, error) {
	return r.affected, r.err
}

func metricCounterValue(t *testing.T, metricName string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_COUNTER {
			t.Fatalf("metric %q is not a counter", metricName)
		}
		var total float64
		for _, metric := range fam.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
		return total
	}
	t.Fatalf("metric %q not found", metricName)
	return 0
}

func authContextForPeer(addr net.Addr, username, password string) context.Context {
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	base := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
	return metadata.NewIncomingContext(base, metadata.Pairs("authorization", "Basic "+token))
}

func testFlightHandlerWithStoreAndRateLimiter(t *testing.T, users map[string]string, rateLimiter *server.RateLimiter) *ControlPlaneFlightSQLHandler {
	t.Helper()
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		sessions:      make(map[string]*flightClientSession),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return 1234, nil, nil
		},
		destroySessionFn: func(int32) {},
	}
	h, err := NewControlPlaneFlightSQLHandler(store, users)
	if err != nil {
		t.Fatalf("NewControlPlaneFlightSQLHandler returned error: %v", err)
	}
	h.rateLimiter = rateLimiter
	return h
}

func TestParseBasicCredentials(t *testing.T) {
	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	user, pass, err := parseBasicCredentials("Basic " + token)
	if err != nil {
		t.Fatalf("parseBasicCredentials returned error: %v", err)
	}
	if user != "postgres" {
		t.Fatalf("expected username postgres, got %q", user)
	}
	if pass != "postgres" {
		t.Fatalf("expected password postgres, got %q", pass)
	}
}

func TestParseBasicCredentialsInvalid(t *testing.T) {
	tests := []string{
		"",
		"Bearer token",
		"Basic !!!",
		"Basic " + base64.StdEncoding.EncodeToString([]byte("nousersep")),
	}

	for _, input := range tests {
		if _, _, err := parseBasicCredentials(input); err == nil {
			t.Fatalf("expected parseBasicCredentials(%q) to fail", input)
		}
	}
}

func TestSupportsLimit(t *testing.T) {
	if !supportsLimit("SELECT 1") {
		t.Fatalf("expected SELECT to support LIMIT")
	}
	if supportsLimit("SHOW TABLES") {
		t.Fatalf("expected SHOW to not support LIMIT")
	}
}

func TestRowsAffectedOrErrorPropagatesRowsAffectedError(t *testing.T) {
	_, err := rowsAffectedOrError(testExecResult{err: errors.New("not available")})
	if err == nil {
		t.Fatalf("expected rowsAffectedOrError to return an error")
	}
}

func TestRowsAffectedOrErrorReturnsAffectedCount(t *testing.T) {
	affected, err := rowsAffectedOrError(testExecResult{affected: 42})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if affected != 42 {
		t.Fatalf("expected affected=42, got %d", affected)
	}
}

func TestFlightAuthSessionKeyStableAcrossPeerPorts(t *testing.T) {
	ctx1 := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 40000},
	})
	ctx2 := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 40001},
	})

	key1 := flightAuthSessionKey(ctx1, "postgres")
	key2 := flightAuthSessionKey(ctx2, "postgres")

	if key1 != key2 {
		t.Fatalf("expected stable key across peer ports, got %q vs %q", key1, key2)
	}
	if strings.Contains(key1, ":40000") || strings.Contains(key2, ":40001") {
		t.Fatalf("session key should not include peer source port: %q / %q", key1, key2)
	}
}

func TestFlightAuthSessionKeyDoesNotTrustMetadataClientOverride(t *testing.T) {
	base := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 45555},
	})
	ctx := metadata.NewIncomingContext(base, metadata.Pairs("x-duckgres-client-id", "worker-a"))

	key := flightAuthSessionKey(ctx, "postgres")
	if strings.Contains(key, "worker-a") {
		t.Fatalf("session key should ignore untrusted metadata client id: %q", key)
	}
	if strings.Contains(key, "45555") {
		t.Fatalf("session key should not include peer source port: %q", key)
	}
}

func TestSessionFromContextAcceptsServerIssuedSessionTokenWithoutBasicAuth(t *testing.T) {
	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "issued-token"))
	got, err := h.sessionFromContext(ctx)
	if err != nil {
		t.Fatalf("expected token-only auth to succeed, got %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil session")
	}
	if got != s {
		t.Fatalf("expected existing token session to be reused")
	}
}

func TestSessionFromContextRejectsUnknownSessionTokenEvenWithBasicAuth(t *testing.T) {
	store := &flightAuthSessionStore{
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return 9876, nil, nil
		},
		destroySessionFn: func(int32) {},
		sessions:         make(map[string]*flightClientSession),
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-duckgres-session", "missing-token",
			"authorization", "Basic "+token,
		),
	)

	if _, err := h.sessionFromContext(ctx); err == nil {
		t.Fatalf("expected unknown session token to be rejected")
	}
}

func TestSessionFromContextAcceptsServerIssuedSessionTokenWithBasicAuth(t *testing.T) {
	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-duckgres-session", "issued-token",
			"authorization", "Basic "+token,
		),
	)

	got, err := h.sessionFromContext(ctx)
	if err != nil {
		t.Fatalf("expected token+basic auth to succeed, got error: %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil session")
	}
	if got.username != "postgres" {
		t.Fatalf("expected postgres session, got %q", got.username)
	}
}

func TestSessionFromContextTokenPathDoesNotClearRateLimiterFailures(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.47"), Port: 30004}
	rateLimiter := server.NewRateLimiter(server.RateLimitConfig{
		MaxFailedAttempts:   2,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 100,
	})
	rateLimiter.RecordFailedAuth(addr)

	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
	}
	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}
	h.rateLimiter = rateLimiter

	base := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
	ctx := metadata.NewIncomingContext(base, metadata.Pairs("x-duckgres-session", "issued-token"))
	if _, err := h.sessionFromContext(ctx); err != nil {
		t.Fatalf("token-only auth failed: %v", err)
	}

	_, err = h.sessionFromContext(authContextForPeer(addr, "postgres", "wrong"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated error for bad password, got %v", err)
	}
	if !rateLimiter.IsBanned(addr) {
		t.Fatalf("expected prior failure + new failure to ban; token-only path should not clear failures")
	}
}

func TestSessionFromContextWithoutTokenCreatesDistinctSessions(t *testing.T) {
	var createCalls atomic.Int32
	store := &flightAuthSessionStore{
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return createCalls.Add(1), nil, nil
		},
		destroySessionFn: func(int32) {},
		sessions:         make(map[string]*flightClientSession),
		byKey:            make(map[string]string),
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	base := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 45555},
	})
	ctx := metadata.NewIncomingContext(base, metadata.Pairs("authorization", "Basic "+token))

	s1, err := h.sessionFromContext(ctx)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	s2, err := h.sessionFromContext(ctx)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	if s1 == nil || s2 == nil {
		t.Fatalf("expected non-nil sessions")
	}
	if s1 == s2 {
		t.Fatalf("expected distinct sessions without session token")
	}
	if createCalls.Load() != 2 {
		t.Fatalf("expected two independent session creations, got %d", createCalls.Load())
	}
}

func TestFlightAuthSessionStoreGetExistingByKeyConcurrentStaleEntry(t *testing.T) {
	store := &flightAuthSessionStore{
		sessions: make(map[string]*flightClientSession),
		byKey: map[string]string{
			"stale-key": "missing-token",
		},
	}

	const workers = 24
	const iterations = 1000

	start := make(chan struct{})
	errCh := make(chan string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				if _, ok := store.getExistingByKey("stale-key"); ok {
					select {
					case errCh <- "expected stale key lookup to miss":
					default:
					}
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	if msg, ok := <-errCh; ok {
		t.Fatalf("%s", msg)
	}

	store.mu.RLock()
	_, stillPresent := store.byKey["stale-key"]
	store.mu.RUnlock()
	if stillPresent {
		t.Fatalf("expected stale key mapping to be pruned")
	}
}

func TestSessionFromContextRejectsExpiredSessionToken(t *testing.T) {
	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	s.tokenIssuedAt.Store(time.Now().Add(-2 * time.Hour).UnixNano())

	store := &flightAuthSessionStore{
		tokenTTL: time.Hour,
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-duckgres-session", "issued-token",
			"authorization", "Basic "+token,
		),
	)

	if _, err := h.sessionFromContext(ctx); err == nil {
		t.Fatalf("expected expired session token to be rejected")
	}
}

func TestSessionFromContextRejectsTokenUserMismatch(t *testing.T) {
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": newFlightClientSession(1234, "postgres", nil),
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{
		"postgres": "postgres",
		"alice":    "alice",
	})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	token := base64.StdEncoding.EncodeToString([]byte("alice:alice"))
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-duckgres-session", "issued-token",
			"authorization", "Basic "+token,
		),
	)

	if _, err := h.sessionFromContext(ctx); err == nil {
		t.Fatalf("expected token/user mismatch to be rejected")
	}
}

func TestFlightSessionTokenLifecycleIssueValidateRevokeExpiryMatrix(t *testing.T) {
	const bootstrapKey = "bootstrap|postgres|nonce"

	destroyedPIDs := make([]int32, 0, 2)
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		handleIdleTTL: time.Minute,
		tokenTTL:      time.Hour,
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return 1234, nil, nil
		},
		destroySessionFn: func(pid int32) {
			destroyedPIDs = append(destroyedPIDs, pid)
		},
		sessions: make(map[string]*flightClientSession),
		byKey:    make(map[string]string),
	}

	var issued *flightClientSession

	t.Run("issue", func(t *testing.T) {
		var err error
		issued, err = store.GetOrCreate(context.Background(), bootstrapKey, "postgres")
		if err != nil {
			t.Fatalf("GetOrCreate returned error: %v", err)
		}
		if issued == nil {
			t.Fatalf("expected non-nil issued session")
		}
		if strings.TrimSpace(issued.token) == "" {
			t.Fatalf("expected non-empty issued token")
		}
		if issued.tokenIssuedAt.Load() == 0 {
			t.Fatalf("expected tokenIssuedAt to be set during issuance")
		}
		store.mu.RLock()
		mappedToken := store.byKey[bootstrapKey]
		store.mu.RUnlock()
		if mappedToken != issued.token {
			t.Fatalf("expected bootstrap key to map to issued token")
		}
	})

	t.Run("validate", func(t *testing.T) {
		got, ok := store.GetByToken(issued.token)
		if !ok {
			t.Fatalf("expected issued token to validate")
		}
		if got != issued {
			t.Fatalf("expected validated session to match issued session")
		}
	})

	t.Run("revoke", func(t *testing.T) {
		issued.lastUsed.Store(time.Now().Add(-2 * time.Hour).UnixNano())

		if reaped := store.ReapIdleNow(); reaped != 1 {
			t.Fatalf("expected revoke path to reap one idle session, got %d", reaped)
		}
		if _, ok := store.GetByToken(issued.token); ok {
			t.Fatalf("expected revoked token to fail validation")
		}
		store.mu.RLock()
		_, stillMapped := store.byKey[bootstrapKey]
		store.mu.RUnlock()
		if stillMapped {
			t.Fatalf("expected revoke path to prune bootstrap key mapping")
		}
		if len(destroyedPIDs) != 1 || destroyedPIDs[0] != issued.pid {
			t.Fatalf("expected revoke path to destroy session pid %d, got %v", issued.pid, destroyedPIDs)
		}

		h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
		if err != nil {
			t.Fatalf("failed to construct handler: %v", err)
		}
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", issued.token))
		if _, err := h.sessionFromContext(ctx); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected revoked token auth to return unauthenticated, got %v", err)
		}
	})

	t.Run("expiry", func(t *testing.T) {
		expiredSession := newFlightClientSession(4321, "postgres", nil)
		expiredSession.token = "expired-token"
		expiredSession.tokenIssuedAt.Store(time.Now().Add(-2 * time.Hour).UnixNano())

		expiredDestroyed := make([]int32, 0, 1)
		expiredStore := &flightAuthSessionStore{
			tokenTTL: time.Hour,
			sessions: map[string]*flightClientSession{
				"expired-token": expiredSession,
			},
			byKey: map[string]string{
				"bootstrap|postgres|expired": "expired-token",
			},
			destroySessionFn: func(pid int32) {
				expiredDestroyed = append(expiredDestroyed, pid)
			},
		}

		if _, ok := expiredStore.GetByToken("expired-token"); ok {
			t.Fatalf("expected expired token validation to fail")
		}
		if len(expiredDestroyed) != 1 || expiredDestroyed[0] != expiredSession.pid {
			t.Fatalf("expected expiry path to destroy session pid %d, got %v", expiredSession.pid, expiredDestroyed)
		}
		expiredStore.mu.RLock()
		_, stillMapped := expiredStore.byKey["bootstrap|postgres|expired"]
		expiredStore.mu.RUnlock()
		if stillMapped {
			t.Fatalf("expected expiry path to prune bootstrap key mapping")
		}
	})
}

func TestCloseSessionRevokesTokenAndDestroysWorker(t *testing.T) {
	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	s.tokenIssuedAt.Store(time.Now().UnixNano())

	var destroyed []int32
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
		byKey: map[string]string{
			"bootstrap|postgres|nonce": "issued-token",
		},
		destroySessionFn: func(pid int32) {
			destroyed = append(destroyed, pid)
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-duckgres-session", "issued-token",
			"authorization", "Basic "+token,
		),
	)

	res, err := h.CloseSession(ctx, &flight.CloseSessionRequest{})
	if err != nil {
		t.Fatalf("CloseSession returned error: %v", err)
	}
	if res.GetStatus() != flight.CloseSessionResultClosed {
		t.Fatalf("expected close status CLOSED, got %s", res.GetStatus())
	}
	if len(destroyed) != 1 || destroyed[0] != 1234 {
		t.Fatalf("expected session pid 1234 to be destroyed, got %v", destroyed)
	}
	if _, ok := store.GetByToken("issued-token"); ok {
		t.Fatalf("expected closed token to be invalid")
	}

	store.mu.RLock()
	_, stillMapped := store.byKey["bootstrap|postgres|nonce"]
	store.mu.RUnlock()
	if stillMapped {
		t.Fatalf("expected close path to prune bootstrap key mapping")
	}
}

func TestCloseSessionMissingTokenDoesNotBootstrap(t *testing.T) {
	var createCalls atomic.Int32
	store := &flightAuthSessionStore{
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			createCalls.Add(1)
			return 1234, nil, nil
		},
		destroySessionFn: func(int32) {},
		sessions:         make(map[string]*flightClientSession),
		byKey:            make(map[string]string),
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	ctx := authContextForPeer(&net.TCPAddr{IP: net.ParseIP("203.0.113.50"), Port: 30005}, "postgres", "postgres")
	_, err = h.CloseSession(ctx, &flight.CloseSessionRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated for missing session token, got %v", err)
	}
	if createCalls.Load() != 0 {
		t.Fatalf("expected CloseSession to not create bootstrap sessions, got %d create calls", createCalls.Load())
	}
}

func TestCloseSessionTokenOnlyRevokesTokenAndDoesNotBootstrap(t *testing.T) {
	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	s.tokenIssuedAt.Store(time.Now().UnixNano())

	var createCalls atomic.Int32
	var destroyed []int32
	store := &flightAuthSessionStore{
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			createCalls.Add(1)
			return 9876, nil, nil
		},
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
		byKey: map[string]string{
			"bootstrap|postgres|nonce": "issued-token",
		},
		destroySessionFn: func(pid int32) {
			destroyed = append(destroyed, pid)
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "issued-token"))
	res, err := h.CloseSession(ctx, &flight.CloseSessionRequest{})
	if err != nil {
		t.Fatalf("CloseSession returned error: %v", err)
	}
	if res.GetStatus() != flight.CloseSessionResultClosed {
		t.Fatalf("expected close status CLOSED, got %s", res.GetStatus())
	}
	if createCalls.Load() != 0 {
		t.Fatalf("expected token-only close to avoid bootstrap session creation, got %d create calls", createCalls.Load())
	}
	if len(destroyed) != 1 || destroyed[0] != 1234 {
		t.Fatalf("expected session pid 1234 to be destroyed, got %v", destroyed)
	}
	if _, ok := store.GetByToken("issued-token"); ok {
		t.Fatalf("expected closed token to be invalid")
	}
}

func TestCloseSessionDoesNotReissueSessionTokenMetadata(t *testing.T) {
	s := newFlightClientSession(1234, "postgres", nil)
	s.token = "issued-token"
	s.tokenIssuedAt.Store(time.Now().UnixNano())
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": s,
		},
		destroySessionFn: func(int32) {},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	transport := &testServerTransportStream{}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "issued-token"))
	ctx = grpc.NewContextWithServerTransportStream(ctx, transport)

	if _, err := h.CloseSession(ctx, &flight.CloseSessionRequest{}); err != nil {
		t.Fatalf("CloseSession returned error: %v", err)
	}

	if got := transport.header.Get(defaultFlightSessionHeaderKey); len(got) > 0 {
		t.Fatalf("expected close-session response header to omit %q, got %v", defaultFlightSessionHeaderKey, got)
	}
	if got := transport.trailer.Get(defaultFlightSessionHeaderKey); len(got) > 0 {
		t.Fatalf("expected close-session response trailer to omit %q, got %v", defaultFlightSessionHeaderKey, got)
	}
}

func TestSupportsReadOnlySchemaInference(t *testing.T) {
	if !supportsReadOnlySchemaInference("SELECT * FROM t") {
		t.Fatalf("SELECT should be schema-inference safe")
	}
	if supportsReadOnlySchemaInference("INSERT INTO t VALUES (1)") {
		t.Fatalf("INSERT should not be schema-inference safe")
	}
	if supportsReadOnlySchemaInference("UPDATE t SET a = 1") {
		t.Fatalf("UPDATE should not be schema-inference safe")
	}
}

func TestNewControlPlaneFlightSQLHandlerReturnsError(t *testing.T) {
	h, err := NewControlPlaneFlightSQLHandler(nil, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("expected handler constructor to return nil error, got %v", err)
	}
	if h == nil {
		t.Fatalf("expected non-nil handler")
	}
}

func TestSessionFromContextInvalidCredentialsIncrementsAuthFailureMetric(t *testing.T) {
	h := testFlightHandlerWithStoreAndRateLimiter(t, map[string]string{"postgres": "postgres"}, nil)
	ctx := authContextForPeer(&net.TCPAddr{IP: net.ParseIP("203.0.113.44"), Port: 30001}, "postgres", "wrong")

	before := metricCounterValue(t, "duckgres_auth_failures_total")
	_, err := h.sessionFromContext(ctx)
	after := metricCounterValue(t, "duckgres_auth_failures_total")

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated error, got %v", err)
	}
	if after-before != 1 {
		t.Fatalf("expected duckgres_auth_failures_total delta 1, got %.0f", after-before)
	}
}

func TestSessionFromContextRateLimitedRejectsAndIncrementsMetric(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.45"), Port: 30002}
	rateLimiter := server.NewRateLimiter(server.RateLimitConfig{
		MaxFailedAttempts:   1,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 100,
	})
	rateLimiter.RecordFailedAuth(addr)

	h := testFlightHandlerWithStoreAndRateLimiter(t, map[string]string{"postgres": "postgres"}, rateLimiter)
	ctx := authContextForPeer(addr, "postgres", "postgres")

	before := metricCounterValue(t, "duckgres_rate_limit_rejects_total")
	_, err := h.sessionFromContext(ctx)
	after := metricCounterValue(t, "duckgres_rate_limit_rejects_total")

	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected resource exhausted error, got %v", err)
	}
	if after-before != 1 {
		t.Fatalf("expected duckgres_rate_limit_rejects_total delta 1, got %.0f", after-before)
	}
}

func TestSessionFromContextFailedAndSuccessfulAuthUpdateRateLimiter(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.46"), Port: 30003}
	rateLimiter := server.NewRateLimiter(server.RateLimitConfig{
		MaxFailedAttempts:   2,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 100,
	})
	h := testFlightHandlerWithStoreAndRateLimiter(t, map[string]string{"postgres": "postgres"}, rateLimiter)

	_, err := h.sessionFromContext(authContextForPeer(addr, "postgres", "wrong"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated error for bad password, got %v", err)
	}

	s, err := h.sessionFromContext(authContextForPeer(addr, "postgres", "postgres"))
	if err != nil {
		t.Fatalf("expected successful auth, got %v", err)
	}
	if s == nil {
		t.Fatalf("expected non-nil session")
	}

	_, err = h.sessionFromContext(authContextForPeer(addr, "postgres", "wrong"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated error for bad password, got %v", err)
	}
	if rateLimiter.IsBanned(addr) {
		t.Fatalf("expected successful auth to clear prior failures before next bad password")
	}
}

func TestFlightAuthSessionStoreReapHookReceivesTrigger(t *testing.T) {
	stale := newFlightClientSession(1234, "postgres", nil)
	stale.lastUsed.Store(time.Now().Add(-1 * time.Hour).UnixNano())

	trigger := ""
	reapedCount := 0
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		sessions: map[string]*flightClientSession{
			"stale": stale,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		hooks: Hooks{
			OnSessionsReaped: func(t string, count int) {
				trigger = t
				reapedCount = count
			},
		},
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return 0, nil, fmt.Errorf("not used")
		},
		destroySessionFn: func(int32) {},
	}

	if got := store.ReapIdleNow(); got != 1 {
		t.Fatalf("expected one reaped session, got %d", got)
	}
	if trigger != ReapTriggerForced {
		t.Fatalf("expected forced trigger, got %q", trigger)
	}
	if reapedCount != 1 {
		t.Fatalf("expected hook count 1, got %d", reapedCount)
	}
}

func TestFlightAuthSessionStoreReapKeepsSessionWithFreshHandle(t *testing.T) {
	cs := newFlightClientSession(1234, "postgres", nil)
	cs.lastUsed.Store(time.Now().Add(-1 * time.Hour).UnixNano())
	cs.addQuery("prep-1", &flightQueryHandle{
		Query:    "SELECT 1",
		LastUsed: time.Now(),
	})

	destroyed := make([]int32, 0, 1)
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		sessions: map[string]*flightClientSession{
			"session": cs,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return 0, nil, fmt.Errorf("not used")
		},
		destroySessionFn: func(pid int32) {
			destroyed = append(destroyed, pid)
		},
	}

	reaped := store.ReapIdleNow()
	if reaped != 0 {
		t.Fatalf("expected no reaped sessions while handle is fresh, got %d", reaped)
	}
	if len(destroyed) != 0 {
		t.Fatalf("expected no destroyed sessions, got %v", destroyed)
	}
}

func TestFlightAuthSessionStoreReapStaleHandleAllowsSessionReap(t *testing.T) {
	cs := newFlightClientSession(1234, "postgres", nil)
	cs.lastUsed.Store(time.Now().Add(-1 * time.Hour).UnixNano())
	cs.addQuery("prep-1", &flightQueryHandle{
		Query: "SELECT 1",
	})
	cs.mu.Lock()
	cs.queries["prep-1"].LastUsed = time.Now().Add(-1 * time.Hour)
	cs.mu.Unlock()

	destroyed := make([]int32, 0, 1)
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		sessions: map[string]*flightClientSession{
			"session": cs,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		createSessionFn: func(context.Context, string) (int32, *server.FlightExecutor, error) {
			return 0, nil, fmt.Errorf("not used")
		},
		destroySessionFn: func(pid int32) {
			destroyed = append(destroyed, pid)
		},
	}

	reaped := store.ReapIdleNow()
	if reaped != 1 {
		t.Fatalf("expected one reaped session, got %d", reaped)
	}
	if len(destroyed) != 1 || destroyed[0] != 1234 {
		t.Fatalf("expected session 1234 to be destroyed, got %v", destroyed)
	}
}
