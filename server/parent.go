package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// ChildProcess represents a spawned child worker process
type ChildProcess struct {
	PID        int
	Cmd        *exec.Cmd
	Username   string
	RemoteAddr string
	BackendKey BackendKey
	StartTime  time.Time
	done       chan struct{} // Closed when process exits
	exitCode   int           // Exit code after process exits
}

// ChildTracker manages spawned child worker processes
type ChildTracker struct {
	mu       sync.RWMutex
	children map[int]*ChildProcess // Keyed by PID
}

// NewChildTracker creates a new child tracker
func NewChildTracker() *ChildTracker {
	return &ChildTracker{
		children: make(map[int]*ChildProcess),
	}
}

// Add registers a new child process
func (ct *ChildTracker) Add(child *ChildProcess) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.children[child.PID] = child
	slog.Debug("Child process registered", "pid", child.PID, "username", child.Username, "remote_addr", child.RemoteAddr)
}

// Remove unregisters a child process by PID
func (ct *ChildTracker) Remove(pid int) *ChildProcess {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	child, ok := ct.children[pid]
	if ok {
		delete(ct.children, pid)
		slog.Debug("Child process unregistered", "pid", pid)
	}
	return child
}

// Get returns a child process by PID
func (ct *ChildTracker) Get(pid int) *ChildProcess {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.children[pid]
}

// FindByBackendKey finds a child process by its backend key (for cancel requests)
func (ct *ChildTracker) FindByBackendKey(key BackendKey) *ChildProcess {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	for _, child := range ct.children {
		if child.BackendKey.Pid == key.Pid && child.BackendKey.SecretKey == key.SecretKey {
			return child
		}
	}
	return nil
}

// Count returns the number of active child processes
func (ct *ChildTracker) Count() int {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return len(ct.children)
}

// SignalAll sends a signal to all child processes
func (ct *ChildTracker) SignalAll(sig syscall.Signal) {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	for pid, child := range ct.children {
		if child.Cmd != nil && child.Cmd.Process != nil {
			if err := child.Cmd.Process.Signal(sig); err != nil {
				slog.Warn("Failed to signal child process", "pid", pid, "signal", sig, "error", err)
			} else {
				slog.Debug("Sent signal to child process", "pid", pid, "signal", sig)
			}
		}
	}
}

// WaitAll returns a channel that is closed when all children have exited.
// Caller should call this after SignalAll to wait for graceful shutdown.
//
// NOTE: This method creates a new goroutine each time it's called. It captures
// a snapshot of current children at call time - children added after the call
// won't be waited on. For typical shutdown scenarios, call this once after SignalAll.
func (ct *ChildTracker) WaitAll() <-chan struct{} {
	done := make(chan struct{})

	go func() {
		ct.mu.RLock()
		children := make([]*ChildProcess, 0, len(ct.children))
		for _, child := range ct.children {
			children = append(children, child)
		}
		ct.mu.RUnlock()

		for _, child := range children {
			<-child.done // Wait for each child to exit
		}
		close(done)
	}()

	return done
}

// spawnChildForTLS spawns a child worker process to handle TLS handshake and the rest of the connection.
// This is called after the parent has received an SSL request and sent 'S'.
// The TCP connection file descriptor is passed to the child via ExtraFiles.
// Returns the spawned child process or an error.
func (s *Server) spawnChildForTLS(conn net.Conn) (*ChildProcess, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("connection is not TCP")
	}

	// Get a file descriptor for the TCP connection
	file, err := tcpConn.File()
	if err != nil {
		return nil, fmt.Errorf("failed to get file descriptor: %w", err)
	}
	// Note: tcpConn.File() duplicates the FD, so we need to close the original
	// after starting the child. The file will be passed to the child.

	remoteAddr := conn.RemoteAddr().String()

	// Generate backend key for this connection (used for cancel requests)
	backendSecretKey := generateSecretKey()

	// Prepare child configuration
	// Note: Username is not known yet - child will read it from the startup message after TLS
	childCfg := ChildConfig{
		RemoteAddr:       remoteAddr,
		DataDir:          s.cfg.DataDir,
		Extensions:       s.cfg.Extensions,
		IdleTimeout:      int64(s.cfg.IdleTimeout),
		TLSCertFile:      s.cfg.TLSCertFile,
		TLSKeyFile:       s.cfg.TLSKeyFile,
		DuckLake:         s.cfg.DuckLake,
		Users:            s.cfg.Users, // Pass all users - child will look up after getting username
		BackendSecretKey: backendSecretKey,
		ServerStartTime:  processStartTime.UnixNano(),
	}

	configJSON, err := json.Marshal(childCfg)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to marshal child config: %w", err)
	}

	// Spawn child process
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "DUCKGRES_CHILD_MODE=1")
	cmd.ExtraFiles = []*os.File{file} // Will be FD 3 in child

	// Pass config via stdin (more secure than env var for passwords)
	// Create a pipe for stdin
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Inherit stdout/stderr for logging
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		_ = file.Close()
		_ = stdinPipe.Close()
		return nil, fmt.Errorf("failed to start child process: %w", err)
	}

	// Write config to child's stdin and close the pipe
	_, err = stdinPipe.Write(configJSON)
	_ = stdinPipe.Close() // Always close, even on error
	if err != nil {
		// Child is already started, we need to kill it
		_ = cmd.Process.Kill()
		_ = file.Close()
		return nil, fmt.Errorf("failed to write config to child stdin: %w", err)
	}

	// Close the file in parent - child has its own copy
	_ = file.Close()

	// Close original connection in parent - child will use the duplicated FD
	_ = conn.Close()

	child := &ChildProcess{
		PID:        cmd.Process.Pid,
		Cmd:        cmd,
		Username:   "", // Will be set by child - for now unknown
		RemoteAddr: remoteAddr,
		BackendKey: BackendKey{
			Pid:       int32(cmd.Process.Pid),
			SecretKey: backendSecretKey,
		},
		StartTime: time.Now(),
		done:      make(chan struct{}),
	}

	slog.Info("Spawned child worker process",
		"pid", child.PID,
		"remote_addr", remoteAddr,
		"backend_key_pid", child.BackendKey.Pid,
	)

	return child, nil
}

// monitorChild waits for a child process to exit and handles cleanup.
// This should be called in a goroutine after spawnChild.
func (s *Server) monitorChild(child *ChildProcess) {
	// Wait for the child process to exit
	err := child.Cmd.Wait()

	// Get exit code
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = ExitError
		}
	}
	child.exitCode = exitCode

	duration := time.Since(child.StartTime)

	// Handle auth failure exit code
	if exitCode == ExitAuthFailure {
		// Record failed auth for rate limiting
		// We need to parse the remote address to get a net.Addr
		addr, err := net.ResolveTCPAddr("tcp", child.RemoteAddr)
		if err == nil {
			banned := s.rateLimiter.RecordFailedAuth(addr)
			if banned {
				slog.Warn("IP banned after child auth failure", "remote_addr", child.RemoteAddr)
			}
		}
		slog.Warn("Child process exited with auth failure",
			"pid", child.PID,
			"username", child.Username,
			"remote_addr", child.RemoteAddr,
			"duration", duration,
		)
	} else if exitCode != ExitSuccess {
		slog.Error("Child process exited with error",
			"pid", child.PID,
			"username", child.Username,
			"remote_addr", child.RemoteAddr,
			"exit_code", exitCode,
			"duration", duration,
		)
	} else {
		slog.Info("Child process exited cleanly",
			"pid", child.PID,
			"username", child.Username,
			"remote_addr", child.RemoteAddr,
			"duration", duration,
		)
	}

	// Remove from tracker
	s.childTracker.Remove(child.PID)

	// Signal that this child is done
	close(child.done)
}

// CancelQueryBySignal sends SIGUSR1 to a child process to cancel its current query.
// Returns true if the signal was sent successfully.
func (s *Server) CancelQueryBySignal(key BackendKey) bool {
	if s.childTracker == nil {
		return false
	}

	child := s.childTracker.FindByBackendKey(key)
	if child == nil {
		slog.Debug("No child process found for backend key", "pid", key.Pid, "secret_key", key.SecretKey)
		return false
	}

	if child.Cmd == nil || child.Cmd.Process == nil {
		return false
	}

	err := child.Cmd.Process.Signal(syscall.SIGUSR1)
	if err != nil {
		slog.Warn("Failed to send cancel signal to child", "pid", child.PID, "error", err)
		return false
	}

	slog.Info("Sent cancel signal to child process", "pid", child.PID, "username", child.Username)
	queryCancellationsCounter.Inc()
	return true
}
