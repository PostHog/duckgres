package controlplane

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// unixSocketPair creates a connected pair of Unix domain sockets for testing.
func unixSocketPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	dir := shortTempDir(t)
	sockPath := filepath.Join(dir, "pair.sock")

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	acceptCh := make(chan net.Conn, 1)
	go func() {
		conn, _ := ln.Accept()
		acceptCh <- conn
	}()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	server := <-acceptCh
	return conn.(*net.UnixConn), server.(*net.UnixConn)
}

func TestSendRecvFDsSingle(t *testing.T) {
	client, server := unixSocketPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	// Create a temp file to pass
	f, err := os.CreateTemp("", "fd-test-*")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("write: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- sendFDs(server, []*os.File{f})
	}()

	files, err := recvFDs(client, 1)
	if err != nil {
		t.Fatalf("recvFDs: %v", err)
	}
	defer func() {
		for _, rf := range files {
			_ = rf.Close()
		}
	}()

	if sendErr := <-errCh; sendErr != nil {
		t.Fatalf("sendFDs: %v", sendErr)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 FD, got %d", len(files))
	}

	// Read from the received FD to verify it points to the same file
	buf := make([]byte, 10)
	n, _ := files[0].ReadAt(buf, 0)
	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[:n]))
	}
}

func TestSendRecvFDsMultiple(t *testing.T) {
	dir := shortTempDir(t)
	client, server := unixSocketPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	// Create 5 Unix socket listeners to pass (simulating pre-bound sockets)
	const count = 5
	var files []*os.File
	for i := 0; i < count; i++ {
		path := filepath.Join(dir, fmt.Sprintf("sock-%d.sock", i))
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		defer func() { _ = ln.Close() }()
		f, err := ln.(*net.UnixListener).File()
		if err != nil {
			t.Fatalf("File %d: %v", i, err)
		}
		defer func() { _ = f.Close() }()
		files = append(files, f)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- sendFDs(server, files)
	}()

	received, err := recvFDs(client, count)
	if err != nil {
		t.Fatalf("recvFDs: %v", err)
	}
	defer func() {
		for _, rf := range received {
			_ = rf.Close()
		}
	}()

	if sendErr := <-errCh; sendErr != nil {
		t.Fatalf("sendFDs: %v", sendErr)
	}

	if len(received) != count {
		t.Fatalf("expected %d FDs, got %d", count, len(received))
	}

	// Verify each received FD can be used as a listener
	for i, rf := range received {
		ln, err := net.FileListener(rf)
		if err != nil {
			t.Fatalf("FileListener %d: %v", i, err)
		}
		_ = ln.Close()
	}
}

func TestSendRecvFDsBackwardCompat(t *testing.T) {
	// Verify that recvFDs(uc, 1) works with the old single-FD sendFD
	client, server := unixSocketPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	f, err := os.CreateTemp("", "fd-test-*")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	errCh := make(chan error, 1)
	go func() {
		// Use the old single-FD sender
		errCh <- sendFD(server, f)
	}()

	// Use the new multi-FD receiver with expectedCount=1
	files, err := recvFDs(client, 1)
	if err != nil {
		t.Fatalf("recvFDs: %v", err)
	}
	defer func() {
		for _, rf := range files {
			_ = rf.Close()
		}
	}()

	if sendErr := <-errCh; sendErr != nil {
		t.Fatalf("sendFD: %v", sendErr)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 FD, got %d", len(files))
	}
}

// TestHandoverProtocolWithPrebound simulates the full handover protocol between
// an old CP (sending pre-bound sockets) and a new CP (receiving them).
func TestHandoverProtocolWithPrebound(t *testing.T) {
	dir := shortTempDir(t)
	handoverSock := filepath.Join(dir, "handover.sock")

	// Create a TCP listener to simulate the PG listener
	pgLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen TCP: %v", err)
	}
	defer func() { _ = pgLn.Close() }()
	pgAddr := pgLn.Addr().String()

	// Create pre-bound Unix sockets (simulating old CP's pool)
	const preboundCount = 3
	var preboundSockets []*preboundSocket
	for i := 0; i < preboundCount; i++ {
		path := filepath.Join(dir, fmt.Sprintf("worker-%d.sock", i))
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatalf("listen worker %d: %v", i, err)
		}
		ln.(*net.UnixListener).SetUnlinkOnClose(false)
		preboundSockets = append(preboundSockets, &preboundSocket{
			socketPath: path,
			listener:   ln,
		})
	}
	defer func() {
		for _, ps := range preboundSockets {
			_ = ps.listener.Close()
		}
	}()

	// Start handover listener (simulating old CP)
	handoverLn, err := net.Listen("unix", handoverSock)
	if err != nil {
		t.Fatalf("listen handover: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		conn, err := handoverLn.Accept()
		if err != nil {
			errCh <- fmt.Errorf("accept: %w", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = handoverLn.Close()

		decoder := json.NewDecoder(conn)
		encoder := json.NewEncoder(conn)

		// Read handover_request
		var req handoverMsg
		if err := decoder.Decode(&req); err != nil {
			errCh <- fmt.Errorf("decode req: %w", err)
			return
		}
		if req.Type != "handover_request" {
			errCh <- fmt.Errorf("unexpected type: %s", req.Type)
			return
		}

		// Collect pre-bound socket FDs
		var paths []string
		var preboundFiles []*os.File
		for _, ps := range preboundSockets {
			f, err := ps.listener.(*net.UnixListener).File()
			if err != nil {
				errCh <- fmt.Errorf("File: %w", err)
				return
			}
			defer func() { _ = f.Close() }()
			paths = append(paths, ps.socketPath)
			preboundFiles = append(preboundFiles, f)
		}

		// Send ack with paths
		if err := encoder.Encode(handoverMsg{Type: "handover_ack", PreboundPaths: paths}); err != nil {
			errCh <- fmt.Errorf("encode ack: %w", err)
			return
		}

		// Send PG listener FD + pre-bound FDs
		tcpLn := pgLn.(*net.TCPListener)
		pgFile, err := tcpLn.File()
		if err != nil {
			errCh <- fmt.Errorf("pgFile: %w", err)
			return
		}
		defer func() { _ = pgFile.Close() }()

		uc := conn.(*net.UnixConn)
		allFiles := make([]*os.File, 0, 1+len(preboundFiles))
		allFiles = append(allFiles, pgFile)
		allFiles = append(allFiles, preboundFiles...)

		if err := sendFDs(uc, allFiles); err != nil {
			errCh <- fmt.Errorf("sendFDs: %w", err)
			return
		}

		// Read handover_complete
		var complete handoverMsg
		if err := decoder.Decode(&complete); err != nil {
			errCh <- fmt.Errorf("decode complete: %w", err)
			return
		}
		if complete.Type != "handover_complete" {
			errCh <- fmt.Errorf("unexpected type: %s", complete.Type)
			return
		}

		errCh <- nil
	}()

	// New CP side: receiveHandover
	tcpLn, prebound, err := receiveHandover(handoverSock, dir)
	if err != nil {
		t.Fatalf("receiveHandover: %v", err)
	}
	defer func() { _ = tcpLn.Close() }()

	if oldErr := <-errCh; oldErr != nil {
		t.Fatalf("old CP: %v", oldErr)
	}

	// Verify TCP listener
	if tcpLn == nil {
		t.Fatal("TCP listener is nil")
	}
	if tcpLn.Addr().String() != pgAddr {
		t.Fatalf("expected addr %s, got %s", pgAddr, tcpLn.Addr())
	}

	// Verify pre-bound sockets
	if len(prebound) != preboundCount {
		t.Fatalf("expected %d pre-bound sockets, got %d", preboundCount, len(prebound))
	}
	for i, ps := range prebound {
		expectedPath := filepath.Join(dir, fmt.Sprintf("worker-%d.sock", i))
		if ps.socketPath != expectedPath {
			t.Errorf("prebound[%d]: expected path %s, got %s", i, expectedPath, ps.socketPath)
		}
		if ps.listener == nil {
			t.Errorf("prebound[%d]: listener is nil", i)
		}
	}

	// Clean up received pre-bound sockets
	for _, ps := range prebound {
		if ps.listener != nil {
			_ = ps.listener.Close()
		}
	}
}

// TestHandoverProtocolWithoutPrebound verifies backward compatibility when
// the old CP does not send pre-bound sockets (old code without this feature).
func TestHandoverProtocolWithoutPrebound(t *testing.T) {
	dir := shortTempDir(t)
	handoverSock := filepath.Join(dir, "handover.sock")

	pgLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen TCP: %v", err)
	}
	defer func() { _ = pgLn.Close() }()

	handoverLn, err := net.Listen("unix", handoverSock)
	if err != nil {
		t.Fatalf("listen handover: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		conn, err := handoverLn.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = handoverLn.Close()

		decoder := json.NewDecoder(conn)
		encoder := json.NewEncoder(conn)

		var req handoverMsg
		if err := decoder.Decode(&req); err != nil {
			errCh <- err
			return
		}

		// Send ack WITHOUT pre-bound paths (old CP behavior)
		if err := encoder.Encode(handoverMsg{Type: "handover_ack"}); err != nil {
			errCh <- err
			return
		}

		// Send only PG listener FD
		tcpLn := pgLn.(*net.TCPListener)
		pgFile, err := tcpLn.File()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = pgFile.Close() }()

		uc := conn.(*net.UnixConn)
		if err := sendFDs(uc, []*os.File{pgFile}); err != nil {
			errCh <- err
			return
		}

		var complete handoverMsg
		if err := decoder.Decode(&complete); err != nil {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	tcpLn, prebound, err := receiveHandover(handoverSock, dir)
	if err != nil {
		t.Fatalf("receiveHandover: %v", err)
	}
	defer func() { _ = tcpLn.Close() }()

	if oldErr := <-errCh; oldErr != nil {
		t.Fatalf("old CP: %v", oldErr)
	}

	if tcpLn == nil {
		t.Fatal("TCP listener is nil")
	}

	// No pre-bound sockets from old CP
	if len(prebound) != 0 {
		t.Fatalf("expected 0 pre-bound sockets, got %d", len(prebound))
	}
}

// TestHandoverNoPreboundReadOnlyDirFails verifies that when no pre-bound
// sockets are received and the socket directory is read-only, the handover
// correctly fails with a writability error (preserving backward compat behavior).
func TestHandoverNoPreboundReadOnlyDirFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping: running as root (chmod ineffective)")
	}

	dir := shortTempDir(t)
	handoverSock := filepath.Join(dir, "handover.sock")

	// Create a separate read-only directory as the "socket dir"
	roDir := filepath.Join(dir, "sockets")
	if err := os.MkdirAll(roDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(roDir, 0555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(roDir, 0755) })

	// Verify it's actually read-only
	testFile := filepath.Join(roDir, ".test-write")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err == nil {
		_ = os.Remove(testFile)
		t.Skip("directory is still writable despite chmod 0555")
	}

	pgLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen TCP: %v", err)
	}
	defer func() { _ = pgLn.Close() }()

	handoverLn, err := net.Listen("unix", handoverSock)
	if err != nil {
		t.Fatalf("listen handover: %v", err)
	}

	go func() {
		conn, err := handoverLn.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = handoverLn.Close()

		decoder := json.NewDecoder(conn)
		encoder := json.NewEncoder(conn)

		var req handoverMsg
		_ = decoder.Decode(&req)

		// Old CP: no pre-bound sockets
		_ = encoder.Encode(handoverMsg{Type: "handover_ack"})

		tcpLn := pgLn.(*net.TCPListener)
		pgFile, _ := tcpLn.File()
		defer func() { _ = pgFile.Close() }()

		uc := conn.(*net.UnixConn)
		_ = sendFDs(uc, []*os.File{pgFile})

		// New CP will NOT send handover_complete (it fails writability check).
		// The Decode will return an error when conn is closed.
		var complete handoverMsg
		_ = decoder.Decode(&complete)
	}()

	// receiveHandover should fail because no prebound sockets and dir is read-only
	_, _, err = receiveHandover(handoverSock, roDir)
	if err == nil {
		t.Fatal("expected error due to read-only socket dir")
	}
	if !strings.Contains(err.Error(), "socket dir not writable") {
		t.Fatalf("expected 'socket dir not writable' error, got: %v", err)
	}
}

// TestHandoverPreboundBypassesWritabilityCheck verifies that when pre-bound
// sockets are received, the writability check is skipped — the core fix for
// EROFS under systemd ProtectSystem=strict.
func TestHandoverPreboundBypassesWritabilityCheck(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping: running as root (chmod ineffective)")
	}

	dir := shortTempDir(t)
	handoverSock := filepath.Join(dir, "handover.sock")

	// Create a read-only "socket dir"
	roDir := filepath.Join(dir, "sockets")
	if err := os.MkdirAll(roDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	pgLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen TCP: %v", err)
	}
	defer func() { _ = pgLn.Close() }()

	// Create a pre-bound socket in roDir BEFORE making it read-only
	preboundPath := filepath.Join(roDir, "worker-0.sock")
	preboundLn, err := net.Listen("unix", preboundPath)
	if err != nil {
		t.Fatalf("listen prebound: %v", err)
	}
	preboundLn.(*net.UnixListener).SetUnlinkOnClose(false)
	defer func() { _ = preboundLn.Close() }()

	// NOW make the directory read-only
	if err := os.Chmod(roDir, 0555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(roDir, 0755) })

	// Verify it's actually read-only
	testFile := filepath.Join(roDir, ".test-write")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err == nil {
		_ = os.Remove(testFile)
		t.Skip("directory is still writable despite chmod 0555")
	}

	handoverLn, err := net.Listen("unix", handoverSock)
	if err != nil {
		t.Fatalf("listen handover: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		conn, err := handoverLn.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = handoverLn.Close()

		decoder := json.NewDecoder(conn)
		encoder := json.NewEncoder(conn)

		var req handoverMsg
		if err := decoder.Decode(&req); err != nil {
			errCh <- err
			return
		}

		// Send ack WITH pre-bound path
		preboundFile, err := preboundLn.(*net.UnixListener).File()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = preboundFile.Close() }()

		if err := encoder.Encode(handoverMsg{
			Type:          "handover_ack",
			PreboundPaths: []string{preboundPath},
		}); err != nil {
			errCh <- err
			return
		}

		// Send PG listener FD + pre-bound socket FD
		tcpLn := pgLn.(*net.TCPListener)
		pgFile, err := tcpLn.File()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = pgFile.Close() }()

		uc := conn.(*net.UnixConn)
		if err := sendFDs(uc, []*os.File{pgFile, preboundFile}); err != nil {
			errCh <- err
			return
		}

		var complete handoverMsg
		if err := decoder.Decode(&complete); err != nil {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	// receiveHandover should succeed even though roDir is read-only,
	// because we received a pre-bound socket.
	tcpLn, prebound, err := receiveHandover(handoverSock, roDir)
	if err != nil {
		t.Fatalf("receiveHandover should succeed with prebound sockets even under EROFS: %v", err)
	}
	defer func() { _ = tcpLn.Close() }()

	if oldErr := <-errCh; oldErr != nil {
		t.Fatalf("old CP: %v", oldErr)
	}

	if len(prebound) != 1 {
		t.Fatalf("expected 1 pre-bound socket, got %d", len(prebound))
	}
	if prebound[0].socketPath != preboundPath {
		t.Errorf("expected path %s, got %s", preboundPath, prebound[0].socketPath)
	}

	// Clean up
	for _, ps := range prebound {
		_ = ps.listener.Close()
	}
}
