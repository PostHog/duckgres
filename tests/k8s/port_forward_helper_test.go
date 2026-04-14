package k8s_test

import (
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"
)

type portForwardState struct {
	mu    sync.Mutex
	port  int
	cmd   *exec.Cmd
	start func() (int, *exec.Cmd, error)
	wait  func(int, time.Duration) error
	close func(*exec.Cmd)
}

func newPortForwardState(
	start func() (int, *exec.Cmd, error),
	wait func(int, time.Duration) error,
	closeFn func(*exec.Cmd),
) *portForwardState {
	return &portForwardState{
		start: start,
		wait:  wait,
		close: closeFn,
	}
}

func (p *portForwardState) currentPort() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.port
}

func (p *portForwardState) closeCurrent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
}

func (p *portForwardState) restart(timeout time.Duration) error {
	return p.restartIfStale(0, timeout)
}

func (p *portForwardState) restartIfStale(stalePort int, timeout time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if stalePort != 0 && p.port != 0 && p.port != stalePort {
		if err := p.wait(p.port, 2*time.Second); err == nil {
			return nil
		}
	}

	p.closeLocked()

	nextPort, nextCmd, err := p.start()
	if err != nil {
		return err
	}
	if err := p.wait(nextPort, timeout); err != nil {
		p.close(nextCmd)
		return err
	}

	p.port = nextPort
	p.cmd = nextCmd
	return nil
}

func (p *portForwardState) closeLocked() {
	if p.cmd == nil {
		return
	}
	p.close(p.cmd)
	p.cmd = nil
}

func TestPortForwardStateRestartIfStaleSkipsHealthyReplacement(t *testing.T) {
	startCalls := 0
	state := newPortForwardState(
		func() (int, *exec.Cmd, error) {
			startCalls++
			return 3333, &exec.Cmd{}, nil
		},
		func(port int, timeout time.Duration) error {
			if port == 2222 {
				return nil
			}
			return fmt.Errorf("port %d unreachable", port)
		},
		func(*exec.Cmd) {},
	)
	state.port = 2222

	if err := state.restartIfStale(1111, 30*time.Second); err != nil {
		t.Fatalf("restartIfStale returned error: %v", err)
	}
	if startCalls != 0 {
		t.Fatalf("start called %d times, want 0", startCalls)
	}
	if got := state.currentPort(); got != 2222 {
		t.Fatalf("currentPort() = %d, want 2222", got)
	}
}

func TestPortForwardStateRestartIfStaleReplacesUnhealthyPort(t *testing.T) {
	startCalls := 0
	state := newPortForwardState(
		func() (int, *exec.Cmd, error) {
			startCalls++
			return 3333, &exec.Cmd{}, nil
		},
		func(port int, timeout time.Duration) error {
			if port == 3333 {
				return nil
			}
			return fmt.Errorf("port %d unreachable", port)
		},
		func(*exec.Cmd) {},
	)
	state.port = 1111
	state.cmd = &exec.Cmd{}

	if err := state.restartIfStale(1111, 30*time.Second); err != nil {
		t.Fatalf("restartIfStale returned error: %v", err)
	}
	if startCalls != 1 {
		t.Fatalf("start called %d times, want 1", startCalls)
	}
	if got := state.currentPort(); got != 3333 {
		t.Fatalf("currentPort() = %d, want 3333", got)
	}
}

func TestPortForwardStateRestartUsesDefaultStalePort(t *testing.T) {
	startCalls := 0
	state := newPortForwardState(
		func() (int, *exec.Cmd, error) {
			startCalls++
			return 4444, &exec.Cmd{}, nil
		},
		func(port int, timeout time.Duration) error {
			if port == 4444 {
				return nil
			}
			return fmt.Errorf("port %d unreachable", port)
		},
		func(*exec.Cmd) {},
	)

	if err := state.restart(30 * time.Second); err != nil {
		t.Fatalf("restart returned error: %v", err)
	}
	if startCalls != 1 {
		t.Fatalf("start called %d times, want 1", startCalls)
	}
	if got := state.currentPort(); got != 4444 {
		t.Fatalf("currentPort() = %d, want 4444", got)
	}
}

func TestPortForwardStateCloseCurrentClearsCommand(t *testing.T) {
	closed := 0
	state := newPortForwardState(
		func() (int, *exec.Cmd, error) { return 0, nil, nil },
		func(int, time.Duration) error { return nil },
		func(*exec.Cmd) { closed++ },
	)
	state.cmd = &exec.Cmd{}

	state.closeCurrent()

	if closed != 1 {
		t.Fatalf("close called %d times, want 1", closed)
	}
	if state.cmd != nil {
		t.Fatal("expected cmd to be cleared")
	}
}
