//go:build !kubernetes

package controlplane

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

type remoteAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c remoteAddrConn) RemoteAddr() net.Addr {
	return c.remote
}

type fakeControlPlaneQueryLogSink struct {
	stops atomic.Int32
}

func (s *fakeControlPlaneQueryLogSink) Log(server.QueryLogEntry) {}

func (s *fakeControlPlaneQueryLogSink) Stop() {
	s.stops.Add(1)
}

func (s *fakeControlPlaneQueryLogSink) StopContext(context.Context) error {
	s.Stop()
	return nil
}

func TestStopQueryLoggerStopsGenericQueryLogSink(t *testing.T) {
	srv := &server.Server{}
	server.InitMinimalServer(srv, server.Config{}, nil)
	sink := &fakeControlPlaneQueryLogSink{}
	server.SetQueryLogSink(srv, sink)

	cp := &ControlPlane{srv: srv}
	cp.stopQueryLogger()

	if got := sink.stops.Load(); got != 1 {
		t.Fatalf("expected generic query log sink to stop once, got %d", got)
	}
}

func TestReadStartupFromRaw_SSLRequest(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	go func() {
		// Send SSLRequest: length=8, version=80877103
		_ = binary.Write(client, binary.BigEndian, int32(8))
		_ = binary.Write(client, binary.BigEndian, uint32(80877103))
	}()

	result, err := readStartupFromRaw(server)
	if err != nil {
		t.Fatalf("readStartupFromRaw() error = %v", err)
	}
	if !result.sslRequest {
		t.Error("should detect SSL request")
	}
}

func TestHandleConnectionNonSSLStartupDoesNotRecordFailedAuth(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("198.51.100.10"), Port: 54321}
	rateLimiter := server.NewRateLimiter(server.RateLimitConfig{
		MaxFailedAttempts:   2,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 10,
		MaxConnections:      10,
	})
	cp := &ControlPlane{rateLimiter: rateLimiter}

	for range 2 {
		client, serverConn := net.Pipe()
		done := make(chan struct{})
		go func() {
			cp.handleConnection(remoteAddrConn{Conn: serverConn, remote: addr})
			close(done)
		}()

		// Protocol version 3.0 without a preceding SSLRequest, equivalent to
		// a client using sslmode=disable.
		if err := binary.Write(client, binary.BigEndian, int32(8)); err != nil {
			t.Fatalf("write startup length: %v", err)
		}
		if err := binary.Write(client, binary.BigEndian, uint32(196608)); err != nil {
			t.Fatalf("write startup protocol: %v", err)
		}
		_, _ = io.Copy(io.Discard, client)
		_ = client.Close()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("handleConnection did not return")
		}
	}

	if rateLimiter.IsBanned(addr) {
		t.Fatal("non-SSL startup rejections should not ban the source address")
	}
}

func TestReadStartupFromRaw_GSSENCRequest(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	go func() {
		// Send GSSENCRequest: length=8, version=80877104
		_ = binary.Write(client, binary.BigEndian, int32(8))
		_ = binary.Write(client, binary.BigEndian, uint32(80877104))

		// Read the 'N' response
		buf := make([]byte, 1)
		n, err := client.Read(buf)
		if err != nil {
			t.Errorf("expected 'N' response, got error: %v", err)
			return
		}
		if n != 1 || buf[0] != 'N' {
			t.Errorf("expected 'N' response, got %q", buf[:n])
			return
		}

		// Follow up with SSLRequest
		_ = binary.Write(client, binary.BigEndian, int32(8))
		_ = binary.Write(client, binary.BigEndian, uint32(80877103))
	}()

	result, err := readStartupFromRaw(server)
	if err != nil {
		t.Fatalf("readStartupFromRaw() error = %v", err)
	}
	if !result.sslRequest {
		t.Error("after GSSENCRequest decline, should detect follow-up SSL request")
	}
}

func TestReadStartupFromRaw_CancelRequest(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	go func() {
		// Cancel request: length=16, version=80877102, pid=123, key=456
		_ = binary.Write(client, binary.BigEndian, int32(16))
		_ = binary.Write(client, binary.BigEndian, uint32(80877102))
		_ = binary.Write(client, binary.BigEndian, uint32(123))
		_ = binary.Write(client, binary.BigEndian, uint32(456))
	}()

	result, err := readStartupFromRaw(server)
	if err != nil {
		t.Fatalf("readStartupFromRaw() error = %v", err)
	}
	if !result.cancelRequest {
		t.Error("should detect cancel request")
	}
	if result.cancelPid != 123 {
		t.Errorf("cancelPid = %d, want 123", result.cancelPid)
	}
	if result.cancelSecretKey != 456 {
		t.Errorf("cancelSecretKey = %d, want 456", result.cancelSecretKey)
	}
}

func TestReadStartupFromRaw_UnknownProtocol(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	go func() {
		// Unknown protocol version
		_ = binary.Write(client, binary.BigEndian, int32(8))
		_ = binary.Write(client, binary.BigEndian, uint32(99999))
	}()

	_, err := readStartupFromRaw(server)
	if err == nil {
		t.Fatal("expected error for unknown protocol version")
		return
	}
}

func TestReadStartupFromRaw_EOF(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	// Close immediately — should get io.EOF
	_ = client.Close()

	_, err := readStartupFromRaw(server)
	if err == nil {
		t.Fatal("expected error on EOF")
		return
	}
}

func TestReadStartupFromRaw_StartupTimeout(t *testing.T) {
	// Simulates a client that connects but never sends data.
	// The startup read deadline (set in handleConnection) should prevent
	// readStartupFromRaw from blocking forever.
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	// Set a short read deadline to simulate the startup timeout
	_ = server.SetReadDeadline(time.Now().Add(50 * time.Millisecond))

	_, err := readStartupFromRaw(server)
	if err == nil {
		t.Fatal("expected timeout error")
		return
	}
	if !isTimeoutErr(err) {
		t.Fatalf("expected timeout error, got: %v (%T)", err, err)
	}
}

func isTimeoutErr(err error) bool {
	for err != nil {
		if te, ok := err.(interface{ Timeout() bool }); ok && te.Timeout() {
			return true
		}
		if uw, ok := err.(interface{ Unwrap() error }); ok {
			err = uw.Unwrap()
		} else if uw, ok := err.(interface{ Unwrap() []error }); ok {
			for _, e := range uw.Unwrap() {
				if isTimeoutErr(e) {
					return true
				}
			}
			return false
		} else {
			return false
		}
	}
	return false
}

// Verify that io.EOF from a closed connection is not misidentified as a timeout.
func TestReadStartupFromRaw_EOFNotTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	_ = client.Close()

	_, err := readStartupFromRaw(server)
	if err == nil {
		t.Fatal("expected error on closed connection")
		return
	}
	if isTimeoutErr(err) {
		t.Fatal("io.EOF should not be reported as a timeout")
	}
	if err != io.EOF && !isWrappedEOF(err) {
		t.Fatalf("expected io.EOF, got: %v", err)
	}
}

func isWrappedEOF(err error) bool {
	for err != nil {
		if err == io.EOF {
			return true
		}
		if uw, ok := err.(interface{ Unwrap() error }); ok {
			err = uw.Unwrap()
		} else {
			return false
		}
	}
	return false
}

func TestClientSuppliedWorkerGUCs(t *testing.T) {
	on := K8sConfig{AllowClientWorkerProfile: true}
	if !clientSuppliedWorkerGUCs(on, map[string]string{"duckgres.worker_cpu": "4"}) {
		t.Fatal("cpu GUC must count")
	}
	if !clientSuppliedWorkerGUCs(on, map[string]string{"duckgres.worker_memory": "8Gi"}) {
		t.Fatal("memory GUC must count")
	}
	if !clientSuppliedWorkerGUCs(on, map[string]string{"duckgres.worker_ttl": "5m"}) {
		t.Fatal("ttl GUC must count")
	}
	if clientSuppliedWorkerGUCs(on, map[string]string{"search_path": "x"}) {
		t.Fatal("unrelated options must not count")
	}
	if clientSuppliedWorkerGUCs(on, nil) {
		t.Fatal("no options must not count")
	}
	if clientSuppliedWorkerGUCs(on, map[string]string{"duckgres.worker_cpu": "   "}) {
		t.Fatal("blank GUC value must not count")
	}
	// Gate off: client GUCs are ignored everywhere, so they must not bypass
	// the tier either.
	off := K8sConfig{AllowClientWorkerProfile: false}
	if clientSuppliedWorkerGUCs(off, map[string]string{"duckgres.worker_cpu": "4"}) {
		t.Fatal("gated-off client GUCs must not count")
	}
}

// TestUseExploratoryTierExclusions pins every condition that keeps a
// connection off the exploratory tier. The passthrough exclusion is
// load-bearing beyond efficiency: server's executeQueryDirect (the
// passthrough-only execution path) carries no tier hooks, so a passthrough
// connection must never start on the exploratory worker.
func TestUseExploratoryTierExclusions(t *testing.T) {
	profile := &WorkerProfile{CPU: "1", Memory: "2Gi"}
	remote := &ControlPlane{
		isRemoteBackend: true,
		cfg:             ControlPlaneConfig{K8s: K8sConfig{AllowClientWorkerProfile: true}},
	}

	if !remote.useExploratoryTier(profile, false, nil) {
		t.Fatal("a plain remote-backend connection must use the exploratory tier")
	}
	if remote.useExploratoryTier(profile, true, nil) {
		t.Fatal("passthrough users must be excluded from the exploratory tier")
	}
	if remote.useExploratoryTier(nil, false, nil) {
		t.Fatal("a nil exploratory profile (tier off/half-configured) must degrade to today's behavior")
	}
	if remote.useExploratoryTier(profile, false, map[string]string{"duckgres.worker_cpu": "4"}) {
		t.Fatal("a client-supplied worker shape must bypass the exploratory tier")
	}

	local := &ControlPlane{isRemoteBackend: false, cfg: remote.cfg}
	if local.useExploratoryTier(profile, false, nil) {
		t.Fatal("non-remote backends have no worker pods to size")
	}
}

// The real config store must satisfy ConfigStoreInterface, including the
// OrgUserSessionQueryAccess accessor the exploratory switcher's post-escalation
// disabled re-check calls. Asserted here because the !kubernetes build never
// assigns the concrete store to the interface.
var _ ConfigStoreInterface = (*configstore.ConfigStore)(nil)

// TestLazyActivationCatalogAssumption pins the invariant the lazy (deferred
// worker acquisition) connect path leans on: with the DuckLake catalog
// attached — the ONLY attachment a multitenant session can succeed with —
// catalog resolution has exactly one successful outcome, the DuckLake catalog
// itself. That is what lets the connect path stamp the transpiler's backend
// profile and catalog USE rewriting before any worker exists, and refuse an
// unavailable catalog at CONNECT instead of deferring it into a
// first-statement fatal. If resolveEffectiveCatalog ever admits a second
// successful outcome, the lazy path must probe a worker instead of assuming.
func TestLazyActivationCatalogAssumption(t *testing.T) {
	got, ok := resolveEffectiveCatalog("", true)
	if !ok || got != physicalDuckLakeCatalog {
		t.Fatalf("default catalog resolution = (%q, %v), want (%q, true)", got, ok, physicalDuckLakeCatalog)
	}
	got, ok = resolveEffectiveCatalog(physicalDuckLakeCatalog, true)
	if !ok || got != physicalDuckLakeCatalog {
		t.Fatalf("explicit catalog resolution = (%q, %v), want (%q, true)", got, ok, physicalDuckLakeCatalog)
	}
	for _, requested := range []string{"memory", "postgres", "ducklake2", "MAIN"} {
		if got, ok := resolveEffectiveCatalog(requested, true); ok {
			t.Fatalf("resolveEffectiveCatalog(%q, true) = (%q, true); the lazy connect path assumes only %q can succeed",
				requested, got, physicalDuckLakeCatalog)
		}
	}
}

// TestWorkerProfileLogRendering covers the nil (pool-default shape) case of the
// activation log helpers — a nil profile is the default shape, not an empty one.
func TestWorkerProfileLogRendering(t *testing.T) {
	if workerProfileCPU(nil) != "default" || workerProfileMemory(nil) != "default" {
		t.Fatal("a nil worker profile must render as the default shape")
	}
	p := &WorkerProfile{CPU: "2", Memory: "8Gi"}
	if workerProfileCPU(p) != "2" || workerProfileMemory(p) != "8Gi" {
		t.Fatalf("profile rendering = %q/%q, want 2/8Gi", workerProfileCPU(p), workerProfileMemory(p))
	}
}

// A user disabled during the switcher's destroy→create window is rejected by
// the post-escalation re-check, and escalation failure is connection-fatal — so
// the client sees this error text. Keep it identical to the connect-time 28000
// message: whichever gate catches a disabled user, the explanation is the same.
func TestEscalationDisabledErrorMatchesConnectTimeMessage(t *testing.T) {
	if errEscalationUserDisabled.Error() != disabledUserMessage {
		t.Fatalf("escalation disabled error %q must match the connect-time message %q",
			errEscalationUserDisabled.Error(), disabledUserMessage)
	}
}
