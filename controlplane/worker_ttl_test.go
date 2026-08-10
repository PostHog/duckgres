package controlplane

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

// ttlFakePool is a WorkerPool that also implements the worker-TTL capability,
// recording SetWorkerTTL calls.
type ttlFakePool struct {
	workers  map[int]*ManagedWorker
	setCalls []int
	setTTLs  []time.Duration
}

func (p *ttlFakePool) AcquireWorker(context.Context, *WorkerProfile) (*ManagedWorker, error) {
	return nil, errors.New("not implemented")
}

func (p *ttlFakePool) ReleaseWorker(int) {}

func (p *ttlFakePool) RetireWorker(int) {}

func (p *ttlFakePool) RetireWorkerIfNoSessions(int) bool { return false }

func (p *ttlFakePool) Worker(id int) (*ManagedWorker, bool) {
	w, ok := p.workers[id]
	return w, ok
}

func (p *ttlFakePool) SpawnMinWorkers(int) error { return nil }

func (p *ttlFakePool) HealthCheckLoop(context.Context, time.Duration, WorkerCrashHandler, ProgressHandler) {
}

func (p *ttlFakePool) SetMaxWorkers(int) {}

func (p *ttlFakePool) ShutdownAll() {}

func (p *ttlFakePool) SetWorkerTTL(id int, ttl time.Duration) bool {
	if _, ok := p.workers[id]; !ok {
		return false
	}
	p.setCalls = append(p.setCalls, id)
	p.setTTLs = append(p.setTTLs, ttl)
	return true
}

func (p *ttlFakePool) WorkerTTL(id int) (time.Duration, bool) {
	w, ok := p.workers[id]
	if !ok {
		return 0, false
	}
	return w.profile.TTL, true
}

// ttlLessFakePool is a WorkerPool WITHOUT the worker-TTL capability (the
// process-backend shape).
type ttlLessFakePool struct{}

func (p *ttlLessFakePool) AcquireWorker(context.Context, *WorkerProfile) (*ManagedWorker, error) {
	return nil, errors.New("not implemented")
}

func (p *ttlLessFakePool) ReleaseWorker(int) {}

func (p *ttlLessFakePool) RetireWorker(int) {}

func (p *ttlLessFakePool) RetireWorkerIfNoSessions(int) bool { return false }

func (p *ttlLessFakePool) Worker(int) (*ManagedWorker, bool) { return nil, false }

func (p *ttlLessFakePool) SpawnMinWorkers(int) error { return nil }

func (p *ttlLessFakePool) HealthCheckLoop(context.Context, time.Duration, WorkerCrashHandler, ProgressHandler) {
}

func (p *ttlLessFakePool) SetMaxWorkers(int) {}

func (p *ttlLessFakePool) ShutdownAll() {}

// TestSessionManagerSetWorkerTTLForPID asserts the session→worker routing of a
// duckgres.worker_ttl override: the pool is asked to stamp the TTL on the
// worker bound to the session's pid.
func TestSessionManagerSetWorkerTTLForPID(t *testing.T) {
	pool := &ttlFakePool{workers: map[int]*ManagedWorker{5: {ID: 5}}}
	sm := NewSessionManager(pool, nil)
	sm.sessions[1001] = &ManagedSession{WorkerID: 5}

	if err := sm.SetWorkerTTLForPID(1001, 20*time.Minute); err != nil {
		t.Fatalf("SetWorkerTTLForPID: %v", err)
	}
	if len(pool.setCalls) != 1 || pool.setCalls[0] != 5 || pool.setTTLs[0] != 20*time.Minute {
		t.Fatalf("pool calls = %v/%v, want worker 5 with 20m", pool.setCalls, pool.setTTLs)
	}

	// No session for the pid: an error, and the pool is untouched.
	if err := sm.SetWorkerTTLForPID(1002, time.Minute); err == nil {
		t.Fatal("SetWorkerTTLForPID with unknown pid: nil error, want failure")
	}
	if len(pool.setCalls) != 1 {
		t.Fatalf("unknown pid reached the pool: calls = %v", pool.setCalls)
	}

	// Session exists but the worker is gone (raced with retirement): an error.
	sm.sessions[1003] = &ManagedSession{WorkerID: 99}
	if err := sm.SetWorkerTTLForPID(1003, time.Minute); err == nil {
		t.Fatal("SetWorkerTTLForPID with missing worker: nil error, want failure")
	}
}

// TestSessionManagerSetWorkerTTLPoolWithoutCapability asserts the process
// backend shape: a pool without the TTL capability makes the apply a no-op
// success (the hook is only installed for the remote backend anyway, so this
// is defensive).
func TestSessionManagerSetWorkerTTLPoolWithoutCapability(t *testing.T) {
	sm := NewSessionManager(&ttlLessFakePool{}, nil)
	sm.sessions[1001] = &ManagedSession{WorkerID: 5}
	if err := sm.SetWorkerTTLForPID(1001, 20*time.Minute); err != nil {
		t.Fatalf("SetWorkerTTLForPID on a TTL-less pool: %v, want no-op success", err)
	}
	if _, ok := sm.WorkerTTLForPID(1001); ok {
		t.Fatal("WorkerTTLForPID on a TTL-less pool: ok=true, want false")
	}
}

// TestSessionManagerWorkerTTLForPID asserts the SHOW-facing read path reports
// the bound worker's current pool-side TTL.
func TestSessionManagerWorkerTTLForPID(t *testing.T) {
	pool := &ttlFakePool{workers: map[int]*ManagedWorker{
		5: {ID: 5, profile: WorkerProfile{CPU: "8", Memory: "16Gi", TTL: 7 * time.Minute}},
	}}
	sm := NewSessionManager(pool, nil)
	sm.sessions[1001] = &ManagedSession{WorkerID: 5}

	ttl, ok := sm.WorkerTTLForPID(1001)
	if !ok || ttl != 7*time.Minute {
		t.Fatalf("WorkerTTLForPID = %s, %v; want 7m, true", ttl, ok)
	}
	if _, ok := sm.WorkerTTLForPID(1002); ok {
		t.Fatal("WorkerTTLForPID with unknown pid: ok=true, want false")
	}
}

// TestWorkerTTLControlForGateDisabled asserts the mid-session override honors
// the same trust boundary as the duckgres.worker_* startup options: with
// AllowClientWorkerProfile off the apply is rejected with 22023 and never
// reaches the pool.
func TestWorkerTTLControlForGateDisabled(t *testing.T) {
	pool := &ttlFakePool{workers: map[int]*ManagedWorker{5: {ID: 5}}}
	sm := NewSessionManager(pool, nil)
	sm.sessions[1001] = &ManagedSession{WorkerID: 5}

	cp := &ControlPlane{}
	cp.cfg.K8s.AllowClientWorkerProfile = false
	ctl := cp.workerTTLControlFor(sm, 1001, nil, slog.Default())

	if _, err := ctl.Apply(context.Background(), 20*time.Minute); err == nil {
		t.Fatal("Apply with the gate off: nil error, want 22023 rejection")
	} else {
		var coded interface{ SQLState() string }
		if !errors.As(err, &coded) || coded.SQLState() != "22023" {
			t.Fatalf("Apply error = %v, want SQLSTATE 22023", err)
		}
	}
	if len(pool.setCalls) != 0 {
		t.Fatalf("gated apply reached the pool: calls = %v", pool.setCalls)
	}
}

// TestWorkerTTLControlForClamps asserts the apply honors the deployment's
// WorkerMaxTTL ceiling exactly like the startup option: the value stamped on
// the worker AND the value reported back to the session are the clamped one.
func TestWorkerTTLControlForClamps(t *testing.T) {
	pool := &ttlFakePool{workers: map[int]*ManagedWorker{5: {ID: 5}}}
	sm := NewSessionManager(pool, nil)
	sm.sessions[1001] = &ManagedSession{WorkerID: 5}

	cp := &ControlPlane{}
	cp.cfg.K8s.AllowClientWorkerProfile = true
	cp.cfg.K8s.WorkerMaxTTL = time.Hour
	ctl := cp.workerTTLControlFor(sm, 1001, nil, slog.Default())

	applied, err := ctl.Apply(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if applied != time.Hour {
		t.Fatalf("Apply returned %s, want the clamped 1h", applied)
	}
	if len(pool.setTTLs) != 1 || pool.setTTLs[0] != time.Hour {
		t.Fatalf("pool TTLs = %v, want [1h]", pool.setTTLs)
	}

	// Within the ceiling: applied as-is.
	if _, err := ctl.Apply(context.Background(), 20*time.Minute); err != nil {
		t.Fatalf("Apply(20m): %v", err)
	}
	if pool.setTTLs[1] != 20*time.Minute {
		t.Fatalf("pool TTLs = %v, want [1h 20m]", pool.setTTLs)
	}
}

// TestSessionWorkerTTLBaseline pins the connect-time baseline SHOW falls back
// to: the session profile's TTL when one was resolved (sized / org default /
// exploratory), else the deployment default TTL, else the built-in 1m.
func TestSessionWorkerTTLBaseline(t *testing.T) {
	var k K8sConfig
	if got := sessionWorkerTTLBaseline(nil, k); got != defaultWorkerTTL {
		t.Fatalf("baseline(nil profile) = %s, want built-in %s", got, defaultWorkerTTL)
	}
	k.WorkerDefaultTTL = 70 * time.Minute
	if got := sessionWorkerTTLBaseline(nil, k); got != 70*time.Minute {
		t.Fatalf("baseline(nil profile, deployment default) = %s, want 70m", got)
	}
	p := &WorkerProfile{CPU: "8", Memory: "16Gi", TTL: 48 * time.Hour}
	if got := sessionWorkerTTLBaseline(p, k); got != 48*time.Hour {
		t.Fatalf("baseline(concrete profile) = %s, want 48h", got)
	}
}
