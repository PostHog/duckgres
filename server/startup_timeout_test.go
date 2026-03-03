package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestHandleStartup_GSSENCStallTimesOut(t *testing.T) {
	client, serverConn := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = serverConn.Close() }()

	originalTimeout := startupReadTimeout
	startupReadTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		startupReadTimeout = originalTimeout
	})

	clientErr := make(chan error, 1)
	go func() {
		if err := binary.Write(client, binary.BigEndian, int32(8)); err != nil {
			clientErr <- err
			return
		}
		if err := binary.Write(client, binary.BigEndian, uint32(80877104)); err != nil {
			clientErr <- err
			return
		}

		var buf [1]byte
		if _, err := io.ReadFull(client, buf[:]); err != nil {
			clientErr <- err
			return
		}
		if buf[0] != 'N' {
			clientErr <- errors.New("expected GSS decline byte 'N'")
			return
		}

		clientErr <- nil
	}()

	c := &clientConn{
		server: &Server{},
		conn:   serverConn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
	}

	err := c.handleStartup()
	if !isTimeoutErr(err) {
		t.Fatalf("expected timeout error, got: %v", err)
	}

	if err := <-clientErr; err != nil {
		t.Fatalf("client setup failed: %v", err)
	}
}

func TestHandleConnectionIsolated_GSSENCStallReturns(t *testing.T) {
	client, serverConn := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = serverConn.Close() }()

	originalTimeout := startupReadTimeout
	startupReadTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		startupReadTimeout = originalTimeout
	})

	clientErr := make(chan error, 1)
	go func() {
		if err := binary.Write(client, binary.BigEndian, int32(8)); err != nil {
			clientErr <- err
			return
		}
		if err := binary.Write(client, binary.BigEndian, uint32(80877104)); err != nil {
			clientErr <- err
			return
		}

		var buf [1]byte
		if _, err := io.ReadFull(client, buf[:]); err != nil {
			clientErr <- err
			return
		}
		if buf[0] != 'N' {
			clientErr <- errors.New("expected GSS decline byte 'N'")
			return
		}

		clientErr <- nil
	}()

	s := &Server{rateLimiter: NewRateLimiter(DefaultRateLimitConfig())}
	done := make(chan struct{})
	go func() {
		s.handleConnectionIsolated(serverConn, serverConn.RemoteAddr())
		close(done)
	}()

	select {
	case <-done:
		// expected once startup read deadline fires
	case <-time.After(750 * time.Millisecond):
		t.Fatal("handleConnectionIsolated did not return after startup timeout")
	}

	if err := <-clientErr; err != nil {
		t.Fatalf("client setup failed: %v", err)
	}
}

func isTimeoutErr(err error) bool {
	for err != nil {
		if te, ok := err.(interface{ Timeout() bool }); ok && te.Timeout() {
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
