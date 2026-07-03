package crashhandler

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"testing"
)

// The tests re-execute the test binary with CRASHHANDLER_TEST_MODE set; TestMain
// dispatches to a crash trigger that must never return. The parent asserts on
// the child's exit status and stderr.
func TestMain(m *testing.M) {
	switch os.Getenv("CRASHHANDLER_TEST_MODE") {
	case "":
		os.Exit(m.Run())
	case "c-thread-segv":
		// SIGSEGV on a C-created thread (the incident class: a DuckDB
		// TaskScheduler thread faulting inside an extension).
		TriggerCThreadSegfault()
	case "cgo-call-segv":
		// SIGSEGV inside a C call made from a Go goroutine (a DuckDB call
		// crashing under cgo).
		TriggerCgoCallSegfault()
	case "go-nil-deref":
		// A plain Go nil dereference must still produce a normal Go panic,
		// not a native crash report.
		var p *int
		_ = *p //nolint:govet
	}
	// Every mode above must have killed the process.
	os.Exit(97)
}

// runCrashChild re-runs the test binary in the given crash mode and returns
// (stderr, waitStatus).
func runCrashChild(t *testing.T, mode string) (string, syscall.WaitStatus) {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "CRASHHANDLER_TEST_MODE="+mode, "GOTRACEBACK=single")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("child in mode %q exited cleanly; expected it to die (stderr: %s)", mode, stderr.String())
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("child in mode %q: unexpected error type %T: %v", mode, err, err)
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("child in mode %q: no wait status: %v", mode, err)
	}
	if ws.Exited() && ws.ExitStatus() == 97 {
		t.Fatalf("child in mode %q survived its crash trigger (exit 97); handler swallowed the signal? stderr: %s", mode, stderr.String())
	}
	return stderr.String(), ws
}

func TestInstalled(t *testing.T) {
	if !Installed() {
		t.Fatal("native crash handler not installed at process start")
	}
}

// TestCThreadSegfaultDiesLoudly is the regression test for the production
// incident: a SIGSEGV on a non-Go thread must (a) kill the process instead of
// wedging in the Go runtime's badsignal path, and (b) leave a native backtrace
// on stderr.
func TestCThreadSegfaultDiesLoudly(t *testing.T) {
	stderr, ws := runCrashChild(t, "c-thread-segv")
	if !ws.Signaled() {
		t.Fatalf("child not killed by a signal (status %v); stderr: %s", ws, stderr)
	}
	if !strings.Contains(stderr, Marker) {
		t.Fatalf("stderr missing crash marker %q:\n%s", Marker, stderr)
	}
	if !strings.Contains(stderr, "SIGSEGV") {
		t.Fatalf("stderr does not name the signal:\n%s", stderr)
	}
	// backtrace_symbols_fd's frame format differs per platform
	// ("binary[0xADDR]" on glibc, "N binary 0xADDR symbol + off" on macOS)
	// and the unwind depth through a signal frame varies (glibc on
	// linux/amd64 can produce as few as two frames), so require just one
	// frame address plus the fault-address line the handler prints itself.
	if !regexp.MustCompile(`0x[0-9a-f]{4,}`).MatchString(stderr) {
		t.Fatalf("stderr does not contain a backtrace frame address:\n%s", stderr)
	}
	if !strings.Contains(stderr, "fault addr 0x") {
		t.Fatalf("stderr does not report the fault address:\n%s", stderr)
	}
}

// TestCgoCallSegfaultDiesLoudly covers a fault inside a cgo call running on a
// Go-created thread (e.g. DuckDB crashing during a query executed via
// duckdb-go).
func TestCgoCallSegfaultDiesLoudly(t *testing.T) {
	stderr, ws := runCrashChild(t, "cgo-call-segv")
	if ws.Exited() {
		t.Fatalf("child exited (%d) instead of dying on a signal; stderr: %s", ws.ExitStatus(), stderr)
	}
	if !strings.Contains(stderr, Marker) {
		t.Fatalf("stderr missing crash marker %q:\n%s", Marker, stderr)
	}
}

// TestGoNilDerefStillPanics pins the chaining contract: the native handler
// must NOT hijack SIGSEGV raised by Go code — those keep producing ordinary
// Go panics with goroutine tracebacks.
func TestGoNilDerefStillPanics(t *testing.T) {
	stderr, _ := runCrashChild(t, "go-nil-deref")
	if strings.Contains(stderr, Marker) {
		t.Fatalf("native handler hijacked a Go nil dereference:\n%s", stderr)
	}
	if !strings.Contains(stderr, "panic: runtime error") {
		t.Fatalf("expected a normal Go panic on stderr:\n%s", stderr)
	}
}
