package controlplane

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
)

func TestPreReadyDisconnectWatcherCancelsContextWhenClientCloses(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	ctx, watcher := startPreReadyDisconnectWatcher(context.Background(), serverConn, bufio.NewReader(serverConn))
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("watch context error = %v, want context.Canceled", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("client close did not cancel the pre-ready context")
	}

	result := watcher.Stop()
	if !result.ClientCanceled {
		t.Fatalf("watch result = %+v, want client cancellation", result)
	}
	if !errors.Is(result.Err, io.EOF) {
		t.Fatalf("watch error = %v, want io.EOF", result.Err)
	}
}

func TestPreReadyDisconnectWatcherStopLeavesReaderUsable(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	reader := bufio.NewReader(serverConn)
	ctx, watcher := startPreReadyDisconnectWatcher(context.Background(), serverConn, reader)
	result := watcher.Stop()
	if result.ClientCanceled || result.Err != nil {
		t.Fatalf("watch result = %+v, want clean caller stop", result)
	}
	if second := watcher.Stop(); second != result {
		t.Fatalf("second Stop result = %+v, want %+v", second, result)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stopping watcher did not release its derived context")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte{'Q'})
		writeDone <- err
	}()

	got, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("read after stopping watcher: %v", err)
	}
	if got != 'Q' {
		t.Fatalf("read byte = %q, want Q", got)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write after stopping watcher: %v", err)
	}
}

func TestPreReadyDisconnectWatcherStopLeavesTLSReaderUsable(t *testing.T) {
	clientConn, serverConn := newPreReadyTLSConnPair(t)
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	reader := bufio.NewReader(serverConn)
	_, watcher := startPreReadyDisconnectWatcher(context.Background(), serverConn, reader)
	if result := watcher.Stop(); result.ClientCanceled || result.Err != nil {
		t.Fatalf("watch result = %+v, want clean caller stop", result)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte{'Q'})
		writeDone <- err
	}()
	got, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("TLS read after stopping watcher: %v", err)
	}
	if got != 'Q' {
		t.Fatalf("TLS read byte = %q, want Q", got)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("TLS write after stopping watcher: %v", err)
	}
}

func TestPreReadyDisconnectWatcherReportsUnexpectedFrontendData(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	reader := bufio.NewReader(serverConn)
	ctx, watcher := startPreReadyDisconnectWatcher(context.Background(), serverConn, reader)
	writeDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte{'X'})
		writeDone <- err
	}()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("unexpected frontend data did not cancel the pre-ready context")
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write unexpected frontend data: %v", err)
	}

	result := watcher.Stop()
	if !result.ClientCanceled {
		t.Fatalf("watch result = %+v, want client cancellation", result)
	}
	if !errors.Is(result.Err, errPreReadyFrontendData) {
		t.Fatalf("watch error = %v, want %v", result.Err, errPreReadyFrontendData)
	}
	got, err := reader.Peek(1)
	if err != nil {
		t.Fatalf("peek preserved frontend data: %v", err)
	}
	if got[0] != 'X' {
		t.Fatalf("preserved frontend byte = %q, want X", got[0])
	}
}

func TestPreReadyDisconnectWatcherPreservesClientCancellationAfterCreateSuccess(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	ctx, watcher := startPreReadyDisconnectWatcher(context.Background(), serverConn, bufio.NewReader(serverConn))
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("client close did not cancel the pre-ready context")
	}

	// Model a session-creation implementation that committed successfully just
	// as the client disappeared. The caller can use this durable result to tear
	// down that successful session instead of returning it to a vanished client.
	createSucceeded := true
	result := watcher.Stop()
	if createSucceeded && !result.ClientCanceled {
		t.Fatalf("watch result = %+v, caller cannot detect cancellation-after-success", result)
	}
}

func newPreReadyTLSConnPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := server.EnsureCertificates(certFile, keyFile); err != nil {
		t.Fatalf("create test certificate: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load test certificate: %v", err)
	}

	rawClient, rawServer := net.Pipe()
	clientConn := tls.Client(rawClient, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // self-signed test peer
	serverConn := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}})
	serverHandshake := make(chan error, 1)
	go func() { serverHandshake <- serverConn.Handshake() }()
	if err := clientConn.Handshake(); err != nil {
		t.Fatalf("client TLS handshake: %v", err)
	}
	if err := <-serverHandshake; err != nil {
		t.Fatalf("server TLS handshake: %v", err)
	}
	return clientConn, serverConn
}
