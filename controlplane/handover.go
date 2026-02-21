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
	Type          string   `json:"type"`
	PID           int      `json:"pid,omitempty"`
	PreboundPaths []string `json:"prebound_paths,omitempty"`
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

	cp.closeMu.Lock()
	cp.handoverLn = ln
	cp.closeMu.Unlock()

	slog.Info("Handover listener started.", "socket", cp.cfg.HandoverSocket)

	go func() {
		defer func() { _ = ln.Close() }()

		for {
			conn, err := ln.Accept()
			if err != nil {
				cp.closeMu.Lock()
				closed := cp.closed
				superseded := cp.handoverLn != ln
				cp.closeMu.Unlock()
				if closed {
					_ = os.Remove(cp.cfg.HandoverSocket)
					return
				}
				if superseded {
					// A new listener owns the socket path now; don't remove it.
					return
				}
				slog.Error("Handover accept error.", "error", err)
				continue
			}

			// Socket file is no longer needed — the handover request handler
			// will start a new listener (with a fresh socket) if recovery
			// is needed, and startHandoverListener always removes stale files.
			_ = os.Remove(cp.cfg.HandoverSocket)

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
			// a future SIGUSR1 can retry. Use the atomic CAS inside
			// recoverFromFailedReload to avoid racing with the cmd.Wait
			// goroutine in selfExec (which also attempts recovery when
			// the child process exits).
			slog.Warn("Handover failed, recovering.")
			if cp.recoverFromFailedReload() {
				cp.startHandoverListener()
				cp.recoverAfterFailedReload()
			}
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

	// Collect available pre-bound sockets to pass to the new CP.
	// This allows the new CP to work even when the socket directory is
	// read-only (EROFS under systemd ProtectSystem=strict).
	preboundSockets := cp.pool.TakeAllPrebound()
	var preboundPaths []string
	var preboundFiles []*os.File
	for _, ps := range preboundSockets {
		f, err := ps.listener.(*net.UnixListener).File()
		if err != nil {
			slog.Warn("Failed to get pre-bound socket FD, skipping.", "path", ps.socketPath, "error", err)
			continue
		}
		preboundPaths = append(preboundPaths, ps.socketPath)
		preboundFiles = append(preboundFiles, f)
	}
	defer func() {
		for _, f := range preboundFiles {
			_ = f.Close()
		}
	}()

	// Send ack with pre-bound socket paths so the new CP knows what to expect
	ack := handoverMsg{Type: "handover_ack", PreboundPaths: preboundPaths}
	if err := encoder.Encode(ack); err != nil {
		slog.Error("Failed to send handover ack.", "error", err)
		return
	}

	// Pass PG listener FD via SCM_RIGHTS
	tcpLn, ok := cp.pgListener.(*net.TCPListener)
	if !ok {
		slog.Error("PG listener is not TCP, cannot handover.")
		return
	}

	pgFile, err := tcpLn.File()
	if err != nil {
		slog.Error("Failed to get listener FD.", "error", err)
		return
	}
	defer func() { _ = pgFile.Close() }()

	uc, ok := conn.(*net.UnixConn)
	if !ok {
		slog.Error("Handover connection is not Unix.")
		return
	}

	// Send PG listener FD + all pre-bound socket FDs in a single SCM_RIGHTS message.
	// The first FD is always the PG listener; subsequent FDs match preboundPaths order.
	allFiles := make([]*os.File, 0, 1+len(preboundFiles))
	allFiles = append(allFiles, pgFile)
	allFiles = append(allFiles, preboundFiles...)

	if err := sendFDs(uc, allFiles); err != nil {
		slog.Error("Failed to send FDs.", "error", err)
		return
	}
	slog.Info("PG listener + pre-bound socket FDs sent to new control plane.", "prebound_count", len(preboundFiles))

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
	cp.handoverDraining.Store(true)

	// Belt and suspenders: also send MAINPID from the old CP (which is the
	// currently trusted main PID). This eliminates the race where systemd
	// sees our PID die before processing the new CP's sd_notify MAINPID.
	// When we send it, systemd updates the tracked PID before we exit.
	if complete.PID > 0 {
		if err := sdNotify(fmt.Sprintf("MAINPID=%d", complete.PID)); err != nil {
			slog.Warn("sd_notify MAINPID (from old CP) failed.", "error", err)
		}
	}

	// Clear reloading flag so the timeout-based recovery in selfExecDetached
	// doesn't fire during a long drain.
	cp.reloading.Store(false)

	// Shut down Flight ingress and ACME now that the new CP has confirmed
	// handover_complete. The new CP will bind these ports after
	// receiveHandover returns. Shutting down here (instead of in the
	// SIGUSR1 handler) keeps these services available during the entire
	// handover protocol + pre-warm, minimizing downtime.
	if cp.flight != nil {
		cp.flight.Shutdown()
		cp.flight = nil
	}
	if cp.acmeManager != nil {
		if err := cp.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown during handover.", "error", err)
		}
	}

	// Stop accepting new connections immediately. The new CP has its own
	// listener FD copy (from SCM_RIGHTS), so closing our copy doesn't
	// affect the underlying socket — the new CP can still accept on it.
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()
	_ = cp.pgListener.Close()

	// Close the original pre-bound socket listeners. Their FDs were dup'd
	// and sent to the new CP via SCM_RIGHTS, so the new CP has its own
	// copies. Closing here prevents an FD leak during the (potentially
	// hours-long) drain.
	for _, ps := range preboundSockets {
		_ = ps.listener.Close()
	}

	// Wait for in-flight connections to finish (with timeout)
	drainDone := make(chan struct{})
	go func() {
		cp.wg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		slog.Info("All connections drained after handover.")
	case <-time.After(cp.cfg.HandoverDrainTimeout):
		slog.Warn("Handover drain timeout, forcing exit.", "timeout", cp.cfg.HandoverDrainTimeout)
	}

	// Shut down workers
	cp.pool.ShutdownAll()

	// Give systemd time to process the MAINPID notification before we exit.
	// Without this delay there is a small window where systemd could see our
	// PID die before it has updated the tracked main PID, causing it to
	// briefly consider the service failed and tear down the RuntimeDirectory
	// bind mount inside the ProtectSystem=strict mount namespace.
	// Only sleep when running under systemd — no point delaying in tests or
	// manual runs where there is no NOTIFY_SOCKET.
	if os.Getenv("NOTIFY_SOCKET") != "" {
		time.Sleep(2 * time.Second)
	}

	slog.Info("Old control plane exiting after handover.")
	os.Exit(0)
}

// receiveHandover connects to an existing control plane's handover socket,
// receives the PG listener FD and any pre-bound worker socket FDs, and takes
// over. When no pre-bound sockets are received (old CP without this feature),
// socketDir is validated for writability before completing the handover.
func receiveHandover(handoverSocket, socketDir string) (*net.TCPListener, []*preboundSocket, error) {
	conn, err := net.Dial("unix", handoverSocket)
	if err != nil {
		return nil, nil, fmt.Errorf("connect handover socket: %w", err)
	}
	defer func() { _ = conn.Close() }()

	encoder := json.NewEncoder(conn)

	// Send handover request
	if err := encoder.Encode(handoverMsg{Type: "handover_request"}); err != nil {
		return nil, nil, fmt.Errorf("send handover request: %w", err)
	}

	// Read ack. We must NOT use json.NewDecoder here because
	// its buffered reads would consume the data byte from the next SCM_RIGHTS
	// FD send, silently discarding the ancillary file descriptor.
	var ack handoverMsg
	if err := readJSONLine(conn, &ack); err != nil {
		return nil, nil, fmt.Errorf("read handover ack: %w", err)
	}

	if ack.Type != "handover_ack" {
		return nil, nil, fmt.Errorf("unexpected handover message: %s", ack.Type)
	}

	// Receive FDs via SCM_RIGHTS. The first FD is always the PG listener;
	// subsequent FDs are pre-bound worker sockets matching ack.PreboundPaths.
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, nil, fmt.Errorf("handover connection is not Unix")
	}

	expectedFDs := 1 + len(ack.PreboundPaths)
	files, err := recvFDs(uc, expectedFDs)
	if err != nil {
		return nil, nil, fmt.Errorf("receive handover FDs: %w", err)
	}
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	if len(files) < 1 {
		return nil, nil, fmt.Errorf("no PG listener FD received")
	}

	// First FD is the PG listener.
	ln, err := net.FileListener(files[0])
	if err != nil {
		return nil, nil, fmt.Errorf("FileListener PG: %w", err)
	}

	tcpLn, ok := ln.(*net.TCPListener)
	if !ok {
		_ = ln.Close()
		return nil, nil, fmt.Errorf("PG listener is not TCP")
	}

	// Reconstruct pre-bound sockets from remaining FDs.
	var prebound []*preboundSocket
	for i := 1; i < len(files) && i-1 < len(ack.PreboundPaths); i++ {
		pln, pErr := net.FileListener(files[i])
		if pErr != nil {
			slog.Warn("Failed to reconstruct pre-bound socket from FD.", "path", ack.PreboundPaths[i-1], "error", pErr)
			continue
		}
		ul, ulOK := pln.(*net.UnixListener)
		if !ulOK {
			_ = pln.Close()
			slog.Warn("Pre-bound FD is not a Unix listener.", "path", ack.PreboundPaths[i-1])
			continue
		}
		ul.SetUnlinkOnClose(false)
		prebound = append(prebound, &preboundSocket{
			socketPath: ack.PreboundPaths[i-1],
			listener:   ul,
		})
	}

	// When pre-bound sockets were received, the socket directory doesn't
	// need to be writable — the whole point of passing FDs is to work
	// around EROFS under systemd ProtectSystem=strict.
	// When no pre-bound sockets were received (backward compat with old CP),
	// fall back to the writability check.
	if len(prebound) == 0 {
		if err := checkSocketDirWritable(socketDir); err != nil {
			_ = tcpLn.Close()
			return nil, nil, fmt.Errorf("socket dir not writable, aborting handover: %w", err)
		}
	}

	// Notify systemd of our PID BEFORE telling the old CP we're done.
	// The old CP exits on handover_complete, so systemd must know our PID
	// first — otherwise it sees the old MAINPID die and kills the cgroup.
	if err := sdNotify(fmt.Sprintf("MAINPID=%d\nREADY=1", os.Getpid())); err != nil {
		slog.Warn("sd_notify MAINPID+READY failed.", "error", err)
	}

	// Send handover complete (include our PID so the old CP can also send
	// MAINPID to systemd as the currently trusted main process).
	if err := encoder.Encode(handoverMsg{Type: "handover_complete", PID: os.Getpid()}); err != nil {
		_ = tcpLn.Close()
		for _, ps := range prebound {
			_ = ps.listener.Close()
		}
		return nil, nil, fmt.Errorf("send handover complete: %w", err)
	}

	slog.Info("Handover received.", "prebound_count", len(prebound))
	return tcpLn, prebound, nil
}

// maxFDsPerMsg is the maximum number of file descriptors that can be sent in a
// single SCM_RIGHTS message. The Linux kernel enforces this via SCM_MAX_FD
// (include/net/scm.h) and returns EINVAL if exceeded. macOS has a similar
// limit. We batch sends/receives to stay within this limit.
const maxFDsPerMsg = 253

// sendFD sends a single file descriptor over a Unix socket using SCM_RIGHTS.
func sendFD(uc *net.UnixConn, file *os.File) error {
	rights := syscall.UnixRights(int(file.Fd()))
	_, _, err := uc.WriteMsgUnix([]byte{0}, rights, nil)
	return err
}

// sendFDs sends multiple file descriptors over a Unix socket using SCM_RIGHTS.
// When more than maxFDsPerMsg FDs need to be sent, they are batched into
// multiple sendmsg calls to avoid the kernel's SCM_MAX_FD EINVAL.
func sendFDs(uc *net.UnixConn, files []*os.File) error {
	for i := 0; i < len(files); i += maxFDsPerMsg {
		end := i + maxFDsPerMsg
		if end > len(files) {
			end = len(files)
		}
		batch := files[i:end]
		fds := make([]int, len(batch))
		for j, f := range batch {
			fds[j] = int(f.Fd())
		}
		rights := syscall.UnixRights(fds...)
		if _, _, err := uc.WriteMsgUnix([]byte{0}, rights, nil); err != nil {
			return err
		}
	}
	return nil
}

// recvFDs receives multiple file descriptors from a Unix socket via SCM_RIGHTS.
// When the sender batches FDs across multiple sendmsg calls (due to the
// SCM_MAX_FD limit), this function loops ReadMsgUnix until all expectedCount
// FDs have been accumulated.
func recvFDs(uc *net.UnixConn, expectedCount int) ([]*os.File, error) {
	if expectedCount <= 0 {
		return nil, fmt.Errorf("expectedCount must be positive, got %d", expectedCount)
	}

	buf := make([]byte, 1)
	// Size the OOB buffer for the max batch size (maxFDsPerMsg), not the full
	// expectedCount, since each ReadMsgUnix receives at most one batch.
	oobSize := maxFDsPerMsg
	if expectedCount < oobSize {
		oobSize = expectedCount
	}
	oob := make([]byte, syscall.CmsgSpace(oobSize*4))

	closeFDs := func(files []*os.File) {
		for _, f := range files {
			_ = f.Close()
		}
	}

	var files []*os.File
	for len(files) < expectedCount {
		prevCount := len(files)
		_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
		if err != nil {
			closeFDs(files)
			return nil, err
		}
		scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
		if err != nil {
			closeFDs(files)
			return nil, fmt.Errorf("parse control message: %w", err)
		}
		for _, scm := range scms {
			fds, err := syscall.ParseUnixRights(&scm)
			if err != nil {
				continue
			}
			for _, fd := range fds {
				files = append(files, os.NewFile(uintptr(fd), fmt.Sprintf("received-fd-%d", len(files))))
			}
		}
		if len(files) == prevCount {
			closeFDs(files)
			return nil, fmt.Errorf("received message without file descriptors")
		}
	}
	return files, nil
}

// checkSocketDirWritable verifies that the socket directory is writable by
// creating and removing both a temporary file and a Unix socket. This catches
// cases where ProtectSystem=strict's ReadWritePaths bind mount has been lost
// or remounted RO (e.g., after a handover race with systemd's RuntimeDirectory cleanup).
func checkSocketDirWritable(dir string) error {
	// Test regular file creation
	f, err := os.CreateTemp(dir, ".writable-check-*")
	if err != nil {
		return fmt.Errorf("cannot write to socket directory %s: %w", dir, err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)

	// Test Unix socket binding specifically (catches different EROFS/EPERM cases)
	socketPath := fmt.Sprintf("%s/.socket-check-%d.sock", dir, os.Getpid())
	_ = os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("cannot bind unix socket in %s: %w", dir, err)
	}
	_ = l.Close()
	_ = os.Remove(socketPath)

	return nil
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
