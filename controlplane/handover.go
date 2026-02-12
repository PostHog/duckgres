package controlplane

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"syscall"
	"time"
)

// Handover protocol messages
type handoverMsg struct {
	Type string `json:"type"`
}

// startHandoverListener starts listening for handover requests from a new control plane.
// When a new CP connects, the old CP will transfer the PG listener FD.
func (cp *ControlPlane) startHandoverListener() {
	if cp.cfg.HandoverSocket == "" {
		return
	}

	// Clean up old socket
	_ = os.Remove(cp.cfg.HandoverSocket)

	ln, err := net.Listen("unix", cp.cfg.HandoverSocket)
	if err != nil {
		slog.Error("Failed to start handover listener.", "error", err)
		return
	}

	slog.Info("Handover listener started.", "socket", cp.cfg.HandoverSocket)

	go func() {
		defer func() { _ = ln.Close() }()
		defer func() { _ = os.Remove(cp.cfg.HandoverSocket) }()

		for {
			conn, err := ln.Accept()
			if err != nil {
				cp.closeMu.Lock()
				closed := cp.closed
				cp.closeMu.Unlock()
				if closed {
					return
				}
				slog.Error("Handover accept error.", "error", err)
				continue
			}

			// Handle handover in a goroutine (only one at a time is expected)
			go cp.handleHandoverRequest(conn, ln)
			return // Only handle one handover
		}
	}()
}

// handleHandoverRequest processes an incoming handover request from a new control plane.
func (cp *ControlPlane) handleHandoverRequest(conn net.Conn, handoverLn net.Listener) {
	handoverOK := false
	defer func() {
		_ = conn.Close()
		_ = handoverLn.Close()
		if !handoverOK {
			// Handover failed — recover so the old CP keeps serving and
			// a future SIGUSR1 can retry.
			slog.Warn("Handover failed, recovering.")
			cp.recoverFromFailedReload()
			cp.startHandoverListener()
		}
	}()

	// Deadline prevents the old CP from hanging forever if the new process
	// crashes mid-protocol before sending handover_complete.
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Read handover request
	var req handoverMsg
	if err := decoder.Decode(&req); err != nil {
		slog.Error("Failed to read handover request.", "error", err)
		return
	}

	if req.Type != "handover_request" {
		slog.Error("Unexpected handover message type.", "type", req.Type)
		return
	}

	slog.Info("Received handover request, preparing transfer...")

	// Send ack
	if err := encoder.Encode(handoverMsg{Type: "handover_ack"}); err != nil {
		slog.Error("Failed to send handover ack.", "error", err)
		return
	}

	// Pass PG listener FD via SCM_RIGHTS
	tcpLn, ok := cp.pgListener.(*net.TCPListener)
	if !ok {
		slog.Error("PG listener is not TCP, cannot handover.")
		return
	}

	file, err := tcpLn.File()
	if err != nil {
		slog.Error("Failed to get listener FD.", "error", err)
		return
	}
	defer func() { _ = file.Close() }()

	uc, ok := conn.(*net.UnixConn)
	if !ok {
		slog.Error("Handover connection is not Unix.")
		return
	}

	if err := sendFD(uc, file); err != nil {
		slog.Error("Failed to send PG listener FD.", "error", err)
		return
	}
	slog.Info("PG listener FD sent to new control plane.")

	// Wait for handover_complete
	var complete handoverMsg
	if err := decoder.Decode(&complete); err != nil {
		slog.Error("Failed to read handover complete.", "error", err)
		return
	}

	if complete.Type != "handover_complete" {
		slog.Error("Unexpected handover message type.", "type", complete.Type)
		return
	}

	handoverOK = true

	// Stop accepting new connections immediately. The new CP has its own
	// listener FD copy (from SCM_RIGHTS), so closing our copy doesn't
	// affect the underlying socket — the new CP can still accept on it.
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()
	_ = cp.pgListener.Close()

	// Wait for in-flight connections to finish (with timeout)
	drainDone := make(chan struct{})
	go func() {
		cp.wg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		slog.Info("All connections drained after handover.")
	case <-time.After(5 * time.Minute):
		slog.Warn("Handover drain timeout after 5 minutes, forcing exit.")
	}

	// Shut down workers
	cp.pool.ShutdownAll()

	slog.Info("Old control plane exiting after handover.")
	os.Exit(0)
}

// receiveHandover connects to an existing control plane's handover socket,
// receives the PG listener FD, and takes over.
func receiveHandover(handoverSocket string) (*net.TCPListener, error) {
	conn, err := net.Dial("unix", handoverSocket)
	if err != nil {
		return nil, fmt.Errorf("connect handover socket: %w", err)
	}
	defer func() { _ = conn.Close() }()

	encoder := json.NewEncoder(conn)

	// Send handover request
	if err := encoder.Encode(handoverMsg{Type: "handover_request"}); err != nil {
		return nil, fmt.Errorf("send handover request: %w", err)
	}

	// Read ack. We must NOT use json.NewDecoder here because
	// its buffered reads would consume the data byte from the next SCM_RIGHTS
	// FD send, silently discarding the ancillary file descriptor.
	var ack handoverMsg
	if err := readJSONLine(conn, &ack); err != nil {
		return nil, fmt.Errorf("read handover ack: %w", err)
	}

	if ack.Type != "handover_ack" {
		return nil, fmt.Errorf("unexpected handover message: %s", ack.Type)
	}

	// Receive PG listener FD
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("handover connection is not Unix")
	}

	file, err := recvFD(uc)
	if err != nil {
		return nil, fmt.Errorf("receive PG listener FD: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Reconstruct listener from FD
	ln, err := net.FileListener(file)
	if err != nil {
		return nil, fmt.Errorf("FileListener PG: %w", err)
	}

	tcpLn, ok := ln.(*net.TCPListener)
	if !ok {
		_ = ln.Close()
		return nil, fmt.Errorf("PG listener is not TCP")
	}

	// Notify systemd of our PID BEFORE telling the old CP we're done.
	// The old CP exits on handover_complete, so systemd must know our PID
	// first — otherwise it sees the old MAINPID die and kills the cgroup.
	if err := sdNotify(fmt.Sprintf("MAINPID=%d\nREADY=1", os.Getpid())); err != nil {
		slog.Warn("sd_notify MAINPID+READY failed.", "error", err)
	}

	// Send handover complete
	if err := encoder.Encode(handoverMsg{Type: "handover_complete"}); err != nil {
		_ = tcpLn.Close()
		return nil, fmt.Errorf("send handover complete: %w", err)
	}

	slog.Info("Handover received: got PG listener.")
	return tcpLn, nil
}

// sendFD sends a file descriptor over a Unix socket using SCM_RIGHTS.
func sendFD(uc *net.UnixConn, file *os.File) error {
	rights := syscall.UnixRights(int(file.Fd()))
	_, _, err := uc.WriteMsgUnix([]byte{0}, rights, nil)
	return err
}

// recvFD receives a file descriptor from a Unix socket using SCM_RIGHTS.
func recvFD(uc *net.UnixConn) (*os.File, error) {
	buf := make([]byte, 1)
	oob := make([]byte, 64)
	_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, err
	}
	scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, fmt.Errorf("parse control message: %w", err)
	}
	for _, scm := range scms {
		fds, err := syscall.ParseUnixRights(&scm)
		if err != nil {
			continue
		}
		if len(fds) > 0 {
			return os.NewFile(uintptr(fds[0]), "received-fd"), nil
		}
	}
	return nil, fmt.Errorf("no file descriptor received")
}

// readJSONLine reads one newline-terminated JSON message from conn without
// buffering past the newline. This is critical when the same connection also
// carries SCM_RIGHTS file descriptors — a buffered reader (like json.Decoder)
// would consume the 1-byte data payload of the next FD send via conn.Read(),
// which silently discards the accompanying ancillary data (the actual FD).
func readJSONLine(conn net.Conn, v any) error {
	var buf []byte
	one := make([]byte, 1)
	for {
		n, err := conn.Read(one)
		if n > 0 {
			buf = append(buf, one[0])
			if one[0] == '\n' {
				return json.Unmarshal(buf, v)
			}
		}
		if err != nil {
			return err
		}
	}
}
