//go:build linux || darwin

package controlplane_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/server"
)

// Package-level state set by TestMain.
var (
	binaryPath string
	certFile   string
	keyFile    string
)

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "cp-test-binary-*")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Build binary
	binaryPath = filepath.Join(tmpDir, "duckgres")
	projectRoot := findProjectRoot()
	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to build binary: %v", err)
	}

	// Generate TLS certs
	certFile = filepath.Join(tmpDir, "server.crt")
	keyFile = filepath.Join(tmpDir, "server.key")
	if err := server.EnsureCertificates(certFile, keyFile); err != nil {
		log.Fatalf("Failed to generate certs: %v", err)
	}

	os.Exit(m.Run())
}

func findProjectRoot() string {
	// Walk up from the test file to find go.mod
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			log.Fatal("Could not find project root (go.mod)")
		}
		dir = parent
	}
}

// ---------------------------------------------------------------------------
// syncBuffer — thread-safe byte buffer for capturing subprocess logs
// ---------------------------------------------------------------------------

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (sb *syncBuffer) Write(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Write(p)
}

func (sb *syncBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.String()
}

// ---------------------------------------------------------------------------
// cpHarness — per-test control plane harness
// ---------------------------------------------------------------------------

type cpHarness struct {
	cmd        *exec.Cmd
	port       int
	socketDir  string
	configFile string
	logBuf     *syncBuffer
}

type cpOpts struct {
	handoverSocket string
}

func defaultOpts() cpOpts {
	return cpOpts{}
}

func startControlPlane(t *testing.T, opts cpOpts) *cpHarness {
	t.Helper()


	port := freePort(t)

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	// Unix socket paths have a max length (~104 bytes on macOS). t.TempDir()
	// paths are too long, so use a short /tmp path for sockets.
	socketDir, err := os.MkdirTemp("/tmp", "cp-")
	if err != nil {
		t.Fatalf("Failed to create socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	// Write YAML config
	configFile := filepath.Join(tmpDir, "duckgres.yaml")
	configContent := fmt.Sprintf(`host: 127.0.0.1
port: %d
data_dir: %s
tls:
  cert: %s
  key: %s
users:
  testuser: testpass
`, port, dataDir, certFile, keyFile)
	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	handoverSocket := opts.handoverSocket
	if handoverSocket == "" {
		handoverSocket = filepath.Join(socketDir, "handover.sock")
	}

	logBuf := &syncBuffer{}

	args := []string{
		"--config", configFile,
		"--mode", "control-plane",
		"--socket-dir", socketDir,
		"--handover-socket", handoverSocket,
	}

	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = logBuf
	cmd.Stderr = logBuf
	// Clean environment to avoid inheriting DUCKGRES_ env vars
	cmd.Env = []string{
		"HOME=" + os.Getenv("HOME"),
		"PATH=" + os.Getenv("PATH"),
	}
	// Put CP in its own process group so cleanup can kill the entire tree
	// (CP + selfExec'd new CPs + all workers) with a single signal.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start control plane: %v", err)
	}

	h := &cpHarness{
		cmd:        cmd,
		port:       port,
		socketDir:  socketDir,
		configFile: configFile,
		logBuf:     logBuf,
	}

	t.Cleanup(func() { h.cleanup(t) })

	// Wait for CP to be ready
	if err := h.waitForLog("Control plane listening.", 30*time.Second); err != nil {
		t.Fatalf("Control plane did not start in time.\nLogs:\n%s", logBuf.String())
	}

	return h
}

func (h *cpHarness) openConn(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass sslmode=require connect_timeout=10", h.port)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (h *cpHarness) sendSignal(sig syscall.Signal) error {
	if h.cmd.Process == nil {
		return fmt.Errorf("process not running")
	}
	return syscall.Kill(h.cmd.Process.Pid, sig)
}

func (h *cpHarness) waitForLog(substr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(h.logBuf.String(), substr) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("log %q not found after %v", substr, timeout)
}


var newCPPidRe = regexp.MustCompile(`New control plane spawned\.\s.*pid=(\d+)`)

// parseNewCPPid extracts the new CP PID from the logs after a handover.
func (h *cpHarness) parseNewCPPid() (int, error) {
	matches := newCPPidRe.FindStringSubmatch(h.logBuf.String())
	if len(matches) < 2 {
		return 0, fmt.Errorf("could not find new CP PID in logs")
	}
	pid, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid PID %q: %w", matches[1], err)
	}
	return pid, nil
}

func (h *cpHarness) cleanup(t *testing.T) {
	t.Helper()

	if h.cmd.Process == nil {
		return
	}

	// The CP was started with Setpgid: true, so it and all descendants
	// (selfExec'd new CPs, all workers) share a process group. Killing
	// the group ensures no orphan workers survive to hold pipe FDs open
	// (which would block cmd.Wait() forever).
	pgid := h.cmd.Process.Pid

	// Try graceful shutdown first
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		_ = h.cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(5 * time.Second):
	}

	// Force kill entire process group
	_ = syscall.Kill(-pgid, syscall.SIGKILL)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

// doHandover sends SIGUSR1, waits for the new CP to take over, and tracks
// the new PID for cleanup. Returns the new CP's PID.
func (h *cpHarness) doHandover(t *testing.T) int {
	t.Helper()

	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Wait for the new CP to confirm it has taken over
	if err := h.waitForLog("Handover complete, took over PG listener.", 30*time.Second); err != nil {
		t.Fatalf("Handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	newPid, err := h.parseNewCPPid()
	if err != nil {
		t.Fatalf("Failed to parse new CP PID: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	return newPid
}

// freePort allocates a free TCP port by briefly listening on :0.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to allocate free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestControlPlaneBasic(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	db := h.openConn(t)
	var result int
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if result != 1 {
		t.Fatalf("Expected 1, got %d", result)
	}
}

func TestHandoverPreservesActiveQuery(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	db := h.openConn(t)
	// Warm up connection
	var warmup int
	if err := db.QueryRow("SELECT 1").Scan(&warmup); err != nil {
		t.Fatalf("Warmup query failed: %v", err)
	}

	// Start a long-running query in a goroutine
	type queryResult struct {
		count int64
		err   error
	}
	resultCh := make(chan queryResult, 1)
	go func() {
		var count int64
		err := db.QueryRow("SELECT count(*) FROM range(50000000)").Scan(&count)
		resultCh <- queryResult{count, err}
	}()

	// Give the query a moment to start executing
	time.Sleep(500 * time.Millisecond)

	// Trigger handover
	h.doHandover(t)

	// The long-running query must complete successfully.
	// The old CP keeps serving existing connections until they disconnect.
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("Long-running query failed during handover: %v", res.err)
		}
		if res.count != 50000000 {
			t.Fatalf("Expected count 50000000, got %d", res.count)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Long-running query timed out")
	}

	// The same connection should still work after the query completes —
	// the old CP waits for all connections to finish before exiting.
	var postHandover int
	if err := db.QueryRow("SELECT 42").Scan(&postHandover); err != nil {
		t.Fatalf("Post-handover query on same connection failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if postHandover != 42 {
		t.Fatalf("Expected 42, got %d", postHandover)
	}
}

func TestHandoverNewConnections(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Verify initial connectivity
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-handover query failed: %v", err)
	}
	_ = db.Close()

	// Trigger handover
	h.doHandover(t)

	// New connections must work immediately — the new CP spawns fresh workers
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 42").Scan(&v); err != nil {
		t.Fatalf("Post-handover query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 42 {
		t.Fatalf("Expected 42, got %d", v)
	}
}

func TestHandoverNewConnectionsDuringTransition(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Establish initial connection
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-handover query failed: %v", err)
	}
	_ = db.Close()

	// Send SIGUSR1
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// While handover is in progress, try connecting repeatedly.
	// Some connections may fail during transition, but at least some should succeed.
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass sslmode=require connect_timeout=10", h.port)
	var successes, failures int
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		tmpDB, err := sql.Open("postgres", dsn)
		if err != nil {
			failures++
		} else {
			err = tmpDB.QueryRow("SELECT 1").Scan(&v)
			_ = tmpDB.Close()
			if err == nil {
				successes++
			} else {
				failures++
			}
		}
		time.Sleep(200 * time.Millisecond)

		// Stop once the new CP has taken over and we've confirmed connectivity
		if strings.Contains(h.logBuf.String(), "Handover complete, took over PG listener.") && successes > 0 {
			break
		}
	}

	t.Logf("During transition: %d successes, %d failures", successes, failures)
	if successes == 0 {
		t.Fatalf("No connections succeeded during handover transition.\nLogs:\n%s", h.logBuf.String())
	}
}

func TestHandoverConcurrentConnections(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass sslmode=require connect_timeout=10", h.port)
	const numConns = 8
	dbs := make([]*sql.DB, numConns)
	for i := range dbs {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatalf("Failed to open connection %d: %v", i, err)
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		dbs[i] = db
		// Warm up each connection
		var v int
		if err := dbs[i].QueryRow("SELECT 1").Scan(&v); err != nil {
			t.Fatalf("Failed to warm up connection %d: %v", i, err)
		}
	}
	t.Cleanup(func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	})

	// Start concurrent queries on all connections
	var wg sync.WaitGroup
	errors := make([]error, numConns)
	for i := range dbs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var count int64
			if err := dbs[idx].QueryRow("SELECT count(*) FROM range(10000000)").Scan(&count); err != nil {
				errors[idx] = fmt.Errorf("conn %d: %w", idx, err)
				return
			}
		}(i)
	}

	// Give queries a moment to start
	time.Sleep(300 * time.Millisecond)

	// Send SIGUSR1 to trigger handover
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Wait for all query goroutines to finish
	wg.Wait()

	// Wait for the new CP to confirm it has taken over
	if err := h.waitForLog("Handover complete, took over PG listener.", 60*time.Second); err != nil {
		t.Fatalf("Handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// Count errors from the concurrent queries
	var errCount int
	for _, err := range errors {
		if err != nil {
			errCount++
			t.Logf("Connection error: %v", err)
		}
	}

	// The old CP waits for all connections to finish before exiting,
	// so all active queries should complete without error.
	if errCount > 0 {
		t.Fatalf("%d/%d connections failed (expected 0).\nLogs:\n%s",
			errCount, numConns, h.logBuf.String())
	}
	t.Logf("Concurrent handover: all %d connections completed without error", numConns)
}

func TestDoubleUSR1Ignored(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Verify connectivity first
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-test query failed: %v", err)
	}
	_ = db.Close()

	// Send first SIGUSR1
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send first SIGUSR1: %v", err)
	}

	// Immediately send second SIGUSR1. The old CP may exit before
	// this signal is delivered, so don't fail if the signal can't be sent.
	time.Sleep(100 * time.Millisecond)
	_ = h.sendSignal(syscall.SIGUSR1)

	// Wait for the new CP to confirm it has taken over
	if err := h.waitForLog("Handover complete, took over PG listener.", 30*time.Second); err != nil {
		t.Fatalf("Handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// Verify exactly one handover happened
	handoverCount := strings.Count(h.logBuf.String(), "Handover complete, took over PG listener.")
	if handoverCount != 1 {
		t.Fatalf("Expected exactly 1 handover, got %d.\nLogs:\n%s", handoverCount, h.logBuf.String())
	}

	// Service should work after handover
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Post-handover query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
}

// TestHandoverChildCrashRecovery verifies that when the self-exec'd child
// process dies before connecting to the handover socket, the old control
// plane recovers from the RELOADING state and continues serving.
//
// This is a regression test for a bug where selfExec()'s cmd.Wait goroutine
// didn't trigger recovery, leaving the old CP stuck in "reloading" state
// and causing systemd to log "Reload operation timed out" indefinitely.
func TestHandoverChildCrashRecovery(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Verify initial connectivity
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-test query failed: %v", err)
	}
	_ = db.Close()

	// Save the original config, then corrupt it so the self-exec'd child
	// fails during startup (before reaching the handover socket).
	// The old CP already loaded its config, so this doesn't affect it.
	origConfig, err := os.ReadFile(h.configFile)
	if err != nil {
		t.Fatalf("Failed to read config file: %v", err)
	}
	if err := os.WriteFile(h.configFile, []byte("invalid yaml: ["), 0644); err != nil {
		t.Fatalf("Failed to corrupt config: %v", err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(h.configFile, origConfig, 0644)
	})

	// Send SIGUSR1 — the child will fail to parse config and exit
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// The old CP must detect the child failure and recover
	if err := h.waitForLog("Child process exited before completing handover, recovering.", 15*time.Second); err != nil {
		t.Fatalf("Recovery did not happen.\nLogs:\n%s", h.logBuf.String())
	}

	// The old CP should still accept connections after recovery
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 42").Scan(&v); err != nil {
		t.Fatalf("Post-recovery query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 42 {
		t.Fatalf("Expected 42, got %d", v)
	}
}

// TestHandoverChildCrashThenRetry verifies that after recovering from a
// failed handover (child crash), a subsequent SIGUSR1 successfully completes
// a full handover. This ensures the handover listener is properly restarted.
func TestHandoverChildCrashThenRetry(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Verify initial connectivity
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-test query failed: %v", err)
	}
	_ = db.Close()

	// Save original config, then corrupt it to make the first reload fail
	origConfig, err := os.ReadFile(h.configFile)
	if err != nil {
		t.Fatalf("Failed to read config file: %v", err)
	}
	if err := os.WriteFile(h.configFile, []byte("invalid yaml: ["), 0644); err != nil {
		t.Fatalf("Failed to corrupt config: %v", err)
	}

	// First SIGUSR1 — child crashes, old CP recovers
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send first SIGUSR1: %v", err)
	}
	if err := h.waitForLog("Child process exited before completing handover, recovering.", 15*time.Second); err != nil {
		t.Fatalf("First recovery did not happen.\nLogs:\n%s", h.logBuf.String())
	}

	// Verify old CP still works
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Post-recovery query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	_ = db2.Close()

	// Restore valid config
	if err := os.WriteFile(h.configFile, origConfig, 0644); err != nil {
		t.Fatalf("Failed to restore config: %v", err)
	}

	// Second SIGUSR1 — this time the handover should succeed
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send second SIGUSR1: %v", err)
	}
	if err := h.waitForLog("Handover complete, took over PG listener.", 30*time.Second); err != nil {
		t.Fatalf("Second handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// New CP should serve connections
	db3 := h.openConn(t)
	if err := db3.QueryRow("SELECT 42").Scan(&v); err != nil {
		t.Fatalf("Post-handover query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 42 {
		t.Fatalf("Expected 42, got %d", v)
	}
}
