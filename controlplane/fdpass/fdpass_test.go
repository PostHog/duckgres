package fdpass

import (
	"net"
	"os"
	"testing"
)

func TestSendRecvFD(t *testing.T) {
	// Create a socket pair for FD passing
	sender, receiver, err := SocketPair()
	if err != nil {
		t.Fatalf("SocketPair: %v", err)
	}
	defer func() { _ = sender.Close() }()
	defer func() { _ = receiver.Close() }()

	// Create a temp file to pass
	tmp, err := os.CreateTemp("", "fdpass-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	// Write some data
	msg := "hello from fd passing"
	if _, err := tmp.WriteString(msg); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := tmp.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Send the FD
	if err := SendFile(sender, tmp); err != nil {
		t.Fatalf("SendFile: %v", err)
	}
	_ = tmp.Close()

	// Receive the FD
	received, err := RecvFile(receiver, "received")
	if err != nil {
		t.Fatalf("RecvFile: %v", err)
	}
	defer func() { _ = received.Close() }()

	// Read data from received FD to verify it works
	if _, err := received.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	buf := make([]byte, len(msg))
	n, err := received.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != msg {
		t.Errorf("got %q, want %q", string(buf[:n]), msg)
	}
}

func TestSendRecvTCPConn(t *testing.T) {
	// Create a socket pair for FD passing
	sender, receiver, err := SocketPair()
	if err != nil {
		t.Fatalf("SocketPair: %v", err)
	}
	defer func() { _ = sender.Close() }()
	defer func() { _ = receiver.Close() }()

	// Create a TCP listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Connect a TCP client
	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// Accept the server side
	serverConn, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	// Get the FD from the server-side TCP connection
	tcpConn := serverConn.(*net.TCPConn)
	file, err := tcpConn.File()
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	_ = serverConn.Close() // Close the original; file has a dup'd FD

	// Send the TCP FD
	if err := SendFile(sender, file); err != nil {
		t.Fatalf("SendFile: %v", err)
	}
	_ = file.Close()

	// Receive the TCP FD in the "worker"
	recvFile, err := RecvFile(receiver, "tcp-conn")
	if err != nil {
		t.Fatalf("RecvFile: %v", err)
	}

	// Reconstruct the connection from the received FD
	fc, err := net.FileConn(recvFile)
	if err != nil {
		t.Fatalf("FileConn: %v", err)
	}
	_ = recvFile.Close()
	defer func() { _ = fc.Close() }()

	// Write from the reconstructed connection, read from client
	msg := "hello via fd passing"
	if _, err := fc.Write([]byte(msg)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	buf := make([]byte, len(msg))
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != msg {
		t.Errorf("got %q, want %q", string(buf[:n]), msg)
	}

	_ = clientConn.Close()
}
