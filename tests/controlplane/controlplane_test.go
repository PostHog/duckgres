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
	flightPort int
	socketDir  string
	configFile string
	logBuf     *syncBuffer
}

type cpOpts struct {
	workerCount    int
	handoverSocket string
}

func defaultOpts() cpOpts {
	return cpOpts{workerCount: 2}
}

func startControlPlane(t *testing.T, opts cpOpts) *cpHarness {
	t.Helper()

	if opts.workerCount == 0 {
		opts.workerCount = 2
	}

	port := freePort(t)
	flightPort := freePort(t)

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
		"--worker-count", strconv.Itoa(opts.workerCount),
		"--socket-dir", socketDir,
		"--handover-socket", handoverSocket,
		"--flight-port", strconv.Itoa(flightPort),
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
		flightPort: flightPort,
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

// waitForLogCount waits until the given substring appears at least `count` times.
func (h *cpHarness) waitForLogCount(substr string, count int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Count(h.logBuf.String(), substr) >= count {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("log %q appeared %d times (want %d) after %v",
		substr, strings.Count(h.logBuf.String(), substr), count, timeout)
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
		// Safety net: don't block forever even if Wait() is stuck.
		// This shouldn't happen since SIGKILL to the group kills all
		// pipe holders, but guard against edge cases.
	}
}


// doHandover sends SIGUSR1, waits for the new CP to take over, and tracks
// the new PID for cleanup. Returns the new CP's PID.
//
// We wait for "Handover complete, took over listener and workers." which is
// logged by the NEW CP after it receives listener FDs and worker info.
// We cannot rely on the old CP's exit log because os.Exit(0) may bypass
// the slog handler's buffered flush.
func (h *cpHarness) doHandover(t *testing.T) int {
	t.Helper()

	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Wait for the new CP to confirm it has taken over
	if err := h.waitForLog("Handover complete, took over listener and workers.", 30*time.Second); err != nil {
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

	// The long-running query must complete successfully
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
	// graceful replace keeps old workers alive until clients disconnect.
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

	// Wait for the new CP's graceful replace to complete so new workers are ready
	if err := h.waitForLogCount("Graceful replace: worker replaced.", 2, 120*time.Second); err != nil {
		t.Logf("Warning: graceful replace may not have completed: %v", err)
	}

	// New connections must work after handover settles
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
		if strings.Contains(h.logBuf.String(), "Handover complete, took over listener and workers.") && successes > 0 {
			break
		}
	}

	t.Logf("During transition: %d successes, %d failures", successes, failures)
	if successes == 0 {
		t.Fatalf("No connections succeeded during handover transition.\nLogs:\n%s", h.logBuf.String())
	}

}

func TestRollingUpdatePreservesActiveQuery(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	db := h.openConn(t)
	// Warm up
	var warmup int
	if err := db.QueryRow("SELECT 1").Scan(&warmup); err != nil {
		t.Fatalf("Warmup query failed: %v", err)
	}

	// Start a long-running query
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

	// Give the query a moment to start
	time.Sleep(500 * time.Millisecond)

	// Send SIGUSR2 for rolling update
	if err := h.sendSignal(syscall.SIGUSR2); err != nil {
		t.Fatalf("Failed to send SIGUSR2: %v", err)
	}

	// The query must complete successfully — drain waits for active connections
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("Long-running query failed during rolling update: %v", res.err)
		}
		if res.count != 50000000 {
			t.Fatalf("Expected count 50000000, got %d", res.count)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("Long-running query timed out during rolling update")
	}

	// Close the connection so drain can complete quickly (drain waits for
	// all sessions to end, not just queries).
	_ = db.Close()

	// Wait for rolling update to finish
	if err := h.waitForLogCount("Rolling update: worker replaced.", 2, 120*time.Second); err != nil {
		t.Logf("Warning: rolling update may not have fully completed: %v", err)
	}

	// New connections should work after rolling update
	db2 := h.openConn(t)
	var v int
	if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Post-rolling-update query failed: %v\nLogs:\n%s", err, h.logBuf.String())
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
			// Run a single long-ish query per connection
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

	// Close all connections so retiring workers can detect 0 active connections
	for _, db := range dbs {
		_ = db.Close()
	}

	// Wait for the new CP to confirm it has taken over
	if err := h.waitForLog("Handover complete, took over listener and workers.", 60*time.Second); err != nil {
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

	// With graceful replace, old workers stay alive until clients disconnect,
	// so all active queries should complete without error.
	if errCount > 0 {
		t.Fatalf("%d/%d connections failed (expected 0 with graceful replace).\nLogs:\n%s",
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
	// this signal is delivered (listener closes immediately after
	// handover), so don't fail if the signal can't be sent.
	time.Sleep(100 * time.Millisecond)
	_ = h.sendSignal(syscall.SIGUSR1)

	// Wait for the new CP to confirm it has taken over
	if err := h.waitForLog("Handover complete, took over listener and workers.", 30*time.Second); err != nil {
		t.Fatalf("Handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// Verify exactly one handover happened (the second SIGUSR1 was either
	// ignored by the reloading guard or arrived after the old CP exited).
	handoverCount := strings.Count(h.logBuf.String(), "Handover complete, took over listener and workers.")
	if handoverCount != 1 {
		t.Fatalf("Expected exactly 1 handover, got %d.\nLogs:\n%s", handoverCount, h.logBuf.String())
	}

	// Service should work after handover completes
	// Wait for graceful replace to finish so new workers are ready
	if err := h.waitForLogCount("Graceful replace: worker replaced.", 2, 120*time.Second); err != nil {
		t.Logf("Warning: graceful replace may not have completed: %v", err)
	}

	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Post-handover query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
}

func TestUSR2DuringRollingUpdate(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Verify connectivity
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-test query failed: %v", err)
	}
	_ = db.Close()

	// Send SIGUSR1 to start handover (which triggers a post-handover rolling update)
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Wait for post-handover graceful replace to start
	if err := h.waitForLog("Post-handover: starting graceful replace", 30*time.Second); err != nil {
		t.Fatalf("Graceful replace did not start.\nLogs:\n%s", h.logBuf.String())
	}

	// Track new CP for cleanup and send it SIGUSR2 while graceful replace is in progress.
	// GracefulReplace uses the same rollingUpdate flag, so SIGUSR2 is rejected.
	newPid, err := h.parseNewCPPid()
	if err != nil {
		t.Fatalf("Could not get new CP PID: %v", err)
	}
	if err := syscall.Kill(newPid, syscall.SIGUSR2); err != nil {
		t.Fatalf("Failed to send SIGUSR2 to new CP: %v", err)
	}

	// The concurrent rolling update should be rejected
	if err := h.waitForLog("Rolling update already in progress, skipping.", 10*time.Second); err != nil {
		t.Fatalf("Concurrent rolling update was not rejected.\nLogs:\n%s", h.logBuf.String())
	}

	// Wait for the graceful replace to finish
	if err := h.waitForLogCount("Graceful replace: worker replaced.", 2, 120*time.Second); err != nil {
		t.Logf("Warning: graceful replace may not have fully completed: %v", err)
	}

	// Service should still work
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Post-test query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
}

func TestHandoverThenNormalOperation(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Verify connectivity
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Pre-handover query failed: %v", err)
	}
	_ = db.Close()

	// Do a full handover
	newPid := h.doHandover(t)

	// Wait for graceful replace to complete (all workers replaced)
	if err := h.waitForLogCount("Graceful replace: worker replaced.", 2, 120*time.Second); err != nil {
		t.Fatalf("Post-handover graceful replace did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// Verify new connections work
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 42").Scan(&v); err != nil {
		t.Fatalf("Post-handover query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 42 {
		t.Fatalf("Expected 42, got %d", v)
	}
	_ = db2.Close()

	// Send SIGUSR2 to the new CP — a standalone rolling update should also work
	if err := syscall.Kill(newPid, syscall.SIGUSR2); err != nil {
		t.Fatalf("Failed to send SIGUSR2 to new CP: %v", err)
	}

	// Wait for the SIGUSR2 rolling update to complete (2 workers replaced).
	// These are "Rolling update:" logs (not "Graceful replace:").
	if err := h.waitForLogCount("Rolling update: worker replaced.", 2, 120*time.Second); err != nil {
		t.Fatalf("SIGUSR2 rolling update did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// Verify connections still work after rolling update
	db3 := h.openConn(t)
	if err := db3.QueryRow("SELECT 99").Scan(&v); err != nil {
		t.Fatalf("Post-rolling-update query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 99 {
		t.Fatalf("Expected 99, got %d", v)
	}
}

func TestHandoverOldWorkerRetiresOnDisconnect(t *testing.T) {
	h := startControlPlane(t, defaultOpts())

	// Open a connection and run a query to warm it up
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Warmup query failed: %v", err)
	}

	// Trigger handover — old workers become retiring, new workers spawned
	h.doHandover(t)

	// Wait for graceful replace to finish spawning new workers
	if err := h.waitForLogCount("Graceful replace: worker replaced.", 2, 120*time.Second); err != nil {
		t.Fatalf("Graceful replace did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// The existing connection should still work on the retiring old worker
	if err := db.QueryRow("SELECT 42").Scan(&v); err != nil {
		t.Fatalf("Query on retiring worker failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 42 {
		t.Fatalf("Expected 42, got %d", v)
	}

	// Close the connection — this should cause the retiring worker to have 0 active connections
	_ = db.Close()

	// The retiring worker should detect 0 connections and shut down
	if err := h.waitForLog("Retiring worker has no active connections, shutting down.", 30*time.Second); err != nil {
		t.Fatalf("Old worker did not retire after disconnect.\nLogs:\n%s", h.logBuf.String())
	}

	// New connections should work on the new workers
	db2 := h.openConn(t)
	if err := db2.QueryRow("SELECT 99").Scan(&v); err != nil {
		t.Fatalf("Post-retirement query failed: %v\nLogs:\n%s", err, h.logBuf.String())
	}
	if v != 99 {
		t.Fatalf("Expected 99, got %d", v)
	}
}
