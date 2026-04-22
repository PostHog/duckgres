//go:build !kubernetes

package controlplane

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

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
