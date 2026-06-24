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
