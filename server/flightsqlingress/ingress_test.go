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

	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

var errTestMaxWorkersReached = errors.New("max workers reached")

type testExecResult struct {
	affected int64
	err      error
}

func (r testExecResult) RowsAffected() (int64, error) {
	return r.affected, r.err
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

func TestSessionFromContextRejectsServerIssuedSessionTokenWithoutBasicAuth(t *testing.T) {
	store := &flightAuthSessionStore{
		sessions: map[string]*flightClientSession{
			"issued-token": newFlightClientSession(1234, "postgres", nil),
		},
	}

	h, err := NewControlPlaneFlightSQLHandler(store, map[string]string{"postgres": "postgres"})
	if err != nil {
		t.Fatalf("failed to construct handler: %v", err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", "issued-token"))
	if _, err := h.sessionFromContext(ctx); err == nil {
		t.Fatalf("expected token-only auth to be rejected")
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

func TestFlightAuthSessionStoreRetriesAfterForcedReapOnMaxWorkers(t *testing.T) {
	stale := newFlightClientSession(9001, "postgres", nil)
	stale.lastUsed.Store(time.Now().Add(-1 * time.Hour).UnixNano())

	createCalls := 0
	destroyed := make([]int32, 0, 2)
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		isMaxWorkerFn: func(err error) bool { return errors.Is(err, errTestMaxWorkersReached) },
		sessions: map[string]*flightClientSession{
			"stale-session": stale,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	store.createSessionFn = func(context.Context, string) (int32, *server.FlightExecutor, error) {
		createCalls++
		if createCalls == 1 {
			return 0, nil, fmt.Errorf("acquire worker: %w", errTestMaxWorkersReached)
		}
		return 9002, nil, nil
	}
	store.destroySessionFn = func(pid int32) {
		destroyed = append(destroyed, pid)
	}

	session, err := store.GetOrCreate(context.Background(), "new", "postgres")
	if err != nil {
		t.Fatalf("expected GetOrCreate retry to succeed, got error %v", err)
	}
	if session == nil {
		t.Fatalf("expected non-nil session after retry")
	}
	if createCalls != 2 {
		t.Fatalf("expected 2 create attempts, got %d", createCalls)
	}
	if len(destroyed) != 1 || destroyed[0] != 9001 {
		t.Fatalf("expected stale session to be reaped before retry, got destroyed=%v", destroyed)
	}
}

func TestFlightAuthSessionStoreRetryHookEvents(t *testing.T) {
	outcomes := make([]string, 0, 2)
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		isMaxWorkerFn: func(err error) bool { return errors.Is(err, errTestMaxWorkersReached) },
		sessions:      make(map[string]*flightClientSession),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		hooks: Hooks{
			OnMaxWorkersRetry: func(outcome string) {
				outcomes = append(outcomes, outcome)
			},
		},
	}

	createCalls := 0
	store.createSessionFn = func(context.Context, string) (int32, *server.FlightExecutor, error) {
		createCalls++
		if createCalls == 1 {
			return 0, nil, fmt.Errorf("acquire worker: %w", errTestMaxWorkersReached)
		}
		return 1234, nil, nil
	}
	store.destroySessionFn = func(int32) {}

	if _, err := store.GetOrCreate(context.Background(), "k", "postgres"); err != nil {
		t.Fatalf("expected retry path to succeed, got %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("expected 2 retry outcomes, got %d (%v)", len(outcomes), outcomes)
	}
	if outcomes[0] != MaxWorkersRetryAttempted || outcomes[1] != MaxWorkersRetrySucceeded {
		t.Fatalf("unexpected retry outcomes: %v", outcomes)
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

func TestFlightAuthSessionStoreConcurrentCreateReusesExistingAfterMaxWorkers(t *testing.T) {
	store := &flightAuthSessionStore{
		idleTTL:       time.Minute,
		reapInterval:  time.Hour,
		handleIdleTTL: time.Minute,
		isMaxWorkerFn: func(err error) bool { return errors.Is(err, errTestMaxWorkersReached) },
		sessions:      make(map[string]*flightClientSession),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	var createCalls atomic.Int32
	firstCreateStarted := make(chan struct{})
	releaseFirstCreate := make(chan struct{})
	store.createSessionFn = func(context.Context, string) (int32, *server.FlightExecutor, error) {
		callNum := createCalls.Add(1)
		if callNum == 1 {
			close(firstCreateStarted)
			<-releaseFirstCreate
			return 1001, nil, nil
		}
		return 0, nil, fmt.Errorf("acquire worker: %w", errTestMaxWorkersReached)
	}
	store.destroySessionFn = func(int32) {}

	var wg sync.WaitGroup
	wg.Add(2)

	var s1, s2 *flightClientSession
	var err1, err2 error

	go func() {
		defer wg.Done()
		s1, err1 = store.GetOrCreate(context.Background(), "shared-key", "postgres")
	}()

	<-firstCreateStarted
	go func() {
		defer wg.Done()
		s2, err2 = store.GetOrCreate(context.Background(), "shared-key", "postgres")
	}()

	close(releaseFirstCreate)
	wg.Wait()

	if err1 != nil {
		t.Fatalf("first GetOrCreate failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second GetOrCreate failed: %v", err2)
	}
	if s1 == nil || s2 == nil {
		t.Fatalf("expected non-nil sessions, got s1=%v s2=%v", s1, s2)
	}
	if s1 != s2 {
		t.Fatalf("expected both callers to share the same session pointer")
	}
	if createCalls.Load() < 1 {
		t.Fatalf("expected at least 1 create attempt, got %d", createCalls.Load())
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
