package controlplane

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

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

func TestFlightAuthSessionKeyMetadataClientOverride(t *testing.T) {
	base := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 45555},
	})
	ctx := metadata.NewIncomingContext(base, metadata.Pairs("x-duckgres-client-id", "worker-a"))

	key := flightAuthSessionKey(ctx, "postgres")
	if !strings.Contains(key, "worker-a") {
		t.Fatalf("expected session key to include metadata client id, got %q", key)
	}
	if strings.Contains(key, "45555") {
		t.Fatalf("session key should ignore peer source port when client id is provided: %q", key)
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
		sessions: map[string]*flightClientSession{
			"stale-session": stale,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	store.createSessionFn = func(context.Context, string) (int32, *server.FlightExecutor, error) {
		createCalls++
		if createCalls == 1 {
			return 0, nil, fmt.Errorf("acquire worker: %w", ErrMaxWorkersReached)
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
