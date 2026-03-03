package controlplane

import (
	"encoding/binary"
	"net"
	"testing"
)

func startupPacket(protocolVersion uint32) []byte {
	pkt := make([]byte, 8)
	binary.BigEndian.PutUint32(pkt[0:4], uint32(len(pkt)))
	binary.BigEndian.PutUint32(pkt[4:8], protocolVersion)
	return pkt
}

func TestReadStartupWithGSSFallback(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	errCh := make(chan error, 1)
	go func() {
		// Send GSSENCRequest first.
		if _, err := clientConn.Write(startupPacket(80877104)); err != nil {
			errCh <- err
			return
		}

		// Server should reject GSS with a single 'N' byte.
		resp := make([]byte, 1)
		if _, err := clientConn.Read(resp); err != nil {
			errCh <- err
			return
		}
		if resp[0] != 'N' {
			errCh <- net.InvalidAddrError("expected GSS rejection byte 'N'")
			return
		}

		// Continue negotiation on the same connection with SSLRequest.
		if _, err := clientConn.Write(startupPacket(80877103)); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	params, err := readStartupWithGSSFallback(serverConn)
	if err != nil {
		t.Fatalf("readStartupWithGSSFallback returned error: %v", err)
	}
	if !params.sslRequest {
		t.Fatalf("expected sslRequest=true after GSS fallback, got %+v", params)
	}
	if params.gssRequest {
		t.Fatalf("expected gssRequest=false final result, got %+v", params)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("client side negotiation failed: %v", err)
	}
}
