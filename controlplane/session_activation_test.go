package controlplane

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeActivationSessions is a scripted activationSessions recording the calls
// the acquisition makes and, critically, their order — the ordering
// (create → gauge → identity → finish, destroy exactly once on failure) is what
// the unit tests exist to pin.
type fakeActivationSessions struct {
	createFn func(ctx context.Context, memoryLimit string, threads int, profile *WorkerProfile) (*flightclient.FlightExecutor, error)

	createCalls   []*WorkerProfile
	createLimits  []string
	createThreads []int
	destroyCalls  int
	connClosers   int
	order         []string
}

func (f *fakeActivationSessions) CreateSession(ctx context.Context, _ string, _ int32, memoryLimit string, threads int, profile *WorkerProfile) (int32, *flightclient.FlightExecutor, error) {
	f.createCalls = append(f.createCalls, profile)
	f.createLimits = append(f.createLimits, memoryLimit)
	f.createThreads = append(f.createThreads, threads)
	f.order = append(f.order, "create")
	if f.createFn != nil {
		exec, err := f.createFn(ctx, memoryLimit, threads, profile)
		return 1000, exec, err
	}
	return 1000, &flightclient.FlightExecutor{}, nil
}

func (f *fakeActivationSessions) DestroySession(int32) {
	f.destroyCalls++
	f.order = append(f.order, "destroy")
}
func (f *fakeActivationSessions) SetConnCloser(int32, io.Closer) { f.connClosers++ }
func (f *fakeActivationSessions) WorkerIDForPID(int32) int       { return 42 }
func (f *fakeActivationSessions) WorkerPodNameForPID(int32) string {
	return "duckgres-worker-42"
}
func (f *fakeActivationSessions) SessionCount() int { return 1 }

// newActivationRequest builds a request whose post-create wiring succeeds, so a
// test only has to script the part it is about.
func newActivationRequest(t *testing.T, sessions *fakeActivationSessions) sessionActivationRequest {
	t.Helper()
	var srv server.Server
	server.InitMinimalServer(&srv, server.Config{}, nil)
	return sessionActivationRequest{
		sessions:           sessions,
		srv:                &srv,
		backendKey:         server.BackendKey{Pid: 1000, SecretKey: 7},
		pid:                1000,
		orgID:              "org-1",
		username:           "root",
		connCloser:         io.NopCloser(nil),
		exploratoryProfile: &WorkerProfile{CPU: "1", Memory: "2Gi"},
		standardProfile:    &WorkerProfile{CPU: "8", Memory: "16Gi"},
		baseClog:           slog.Default(),
		finish: func(context.Context, sessionAcquisition) (sessionMetadataResult, *sessionInitError, error) {
			return sessionMetadataResult{duckLakeAttached: true, effectiveCatalog: physicalDuckLakeCatalog}, nil, nil
		},
	}
}

func activationTestCP() *ControlPlane {
	return &ControlPlane{
		isRemoteBackend: true,
		cfg:             ControlPlaneConfig{WorkerQueueTimeout: 5 * time.Second},
	}
}

// TestActivateConnectionSessionProfileSelection is the whole point of the
// pinned flag: a pinning FIRST statement must acquire the escalation target
// directly, in one acquire, instead of taking the small worker and escalating
// off it one statement later.
func TestActivateConnectionSessionProfileSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pinned  bool
		wantCPU string
	}{
		{"unpinned takes the exploratory shape", false, "1"},
		{"pinned takes the escalation target", true, "8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sessions := &fakeActivationSessions{}
			req := newActivationRequest(t, sessions)
			req.pinned = tc.pinned

			res, err := activationTestCP().activateConnectionSession(context.Background(), req)
			if err != nil {
				t.Fatalf("activateConnectionSession: %v", err)
			}
			if len(sessions.createCalls) != 1 {
				t.Fatalf("CreateSession calls = %d, want exactly one", len(sessions.createCalls))
			}
			if got := sessions.createCalls[0]; got == nil || got.CPU != tc.wantCPU {
				t.Fatalf("acquired profile = %+v, want CPU %s", got, tc.wantCPU)
			}
			// The caller stamps the billing size off res.profile, so it has to be
			// the shape actually acquired, not the requested tier.
			if res.profile == nil || res.profile.CPU != tc.wantCPU {
				t.Fatalf("result profile = %+v, want CPU %s", res.profile, tc.wantCPU)
			}
			if !res.sessionCreated {
				t.Fatal("sessionCreated false after a successful acquire; the connection's teardown would leak the session")
			}
			if res.workerID != 42 || res.workerPod != "duckgres-worker-42" {
				t.Fatalf("worker identity = %d/%q", res.workerID, res.workerPod)
			}
			if sessions.destroyCalls != 0 {
				t.Fatalf("successful acquire destroyed the session %d times", sessions.destroyCalls)
			}
		})
	}
}

// TestActivateConnectionSessionFailedCreateDoesNotClaimASession pins the
// bookkeeping the caller depends on: a create that never produced a session
// must leave sessionCreated false, or the connection's teardown calls
// DestroySession on an unknown pid and logs a spurious WARN.
func TestActivateConnectionSessionFailedCreateDoesNotClaimASession(t *testing.T) {
	sessions := &fakeActivationSessions{
		createFn: func(context.Context, string, int, *WorkerProfile) (*flightclient.FlightExecutor, error) {
			return nil, errors.New("no worker")
		},
	}
	req := newActivationRequest(t, sessions)

	res, err := activationTestCP().activateConnectionSession(context.Background(), req)
	if err == nil {
		t.Fatal("failed create returned nil error")
	}
	if res.sessionCreated {
		t.Fatal("sessionCreated true after a create that produced no session")
	}
	if sessions.destroyCalls != 0 {
		t.Fatalf("destroyed a session that was never created (%d calls)", sessions.destroyCalls)
	}
}

// TestActivateConnectionSessionRacedCreateIsDestroyed covers the other half:
// CreateSession can commit at the same instant its context is canceled, handing
// back BOTH an executor and an error. That session must be torn down here (and
// exactly once — sessionCreated stays false so the caller does not repeat it).
func TestActivateConnectionSessionRacedCreateIsDestroyed(t *testing.T) {
	sessions := &fakeActivationSessions{
		createFn: func(context.Context, string, int, *WorkerProfile) (*flightclient.FlightExecutor, error) {
			return &flightclient.FlightExecutor{}, context.Canceled
		},
	}
	req := newActivationRequest(t, sessions)

	res, err := activationTestCP().activateConnectionSession(context.Background(), req)
	if err == nil {
		t.Fatal("raced create returned nil error")
	}
	if sessions.destroyCalls != 1 {
		t.Fatalf("raced session destroyed %d times, want exactly 1", sessions.destroyCalls)
	}
	if res.sessionCreated {
		t.Fatal("sessionCreated true for a raced create already destroyed here; the caller would destroy it twice")
	}
}

// TestActivateConnectionSessionClassifiesFailures asserts activation failures
// carry the SAME SQLSTATE + client message the eager connect path would have
// produced, rather than leaving the server package to guess from error text.
func TestActivateConnectionSessionClassifiesFailures(t *testing.T) {
	cases := []struct {
		name     string
		build    func(req *sessionActivationRequest, sessions *fakeActivationSessions)
		wantCode string
		wantMsg  string
	}{
		{
			name: "cancel",
			build: func(_ *sessionActivationRequest, s *fakeActivationSessions) {
				s.createFn = func(context.Context, string, int, *WorkerProfile) (*flightclient.FlightExecutor, error) {
					return nil, context.Canceled
				}
			},
			wantCode: "57014",
			// NOT the connect path's "canceling authentication…": a lazy
			// activation is cancelled against an in-flight STATEMENT, long after
			// authentication finished.
			wantMsg: "canceling statement due to user request",
		},
		{
			name: "queue timeout",
			build: func(_ *sessionActivationRequest, s *fakeActivationSessions) {
				s.createFn = func(context.Context, string, int, *WorkerProfile) (*flightclient.FlightExecutor, error) {
					return nil, context.DeadlineExceeded
				}
			},
			wantCode: "53300",
			wantMsg:  "timed out waiting for an available worker",
		},
		{
			name: "too many connections",
			build: func(_ *sessionActivationRequest, s *fakeActivationSessions) {
				s.createFn = func(context.Context, string, int, *WorkerProfile) (*flightclient.FlightExecutor, error) {
					return nil, ErrTooManyConnections
				}
			},
			wantCode: "53300",
			wantMsg:  "too many connections",
		},
		{
			name: "draining",
			build: func(_ *sessionActivationRequest, s *fakeActivationSessions) {
				s.createFn = func(context.Context, string, int, *WorkerProfile) (*flightclient.FlightExecutor, error) {
					return nil, ErrSessionManagerDraining
				}
			},
			wantCode: "57P03",
			wantMsg:  "control plane is draining, retry shortly",
		},
		{
			name: "catalog init keeps its own code",
			build: func(req *sessionActivationRequest, _ *fakeActivationSessions) {
				req.finish = func(context.Context, sessionAcquisition) (sessionMetadataResult, *sessionInitError, error) {
					return sessionMetadataResult{}, &sessionInitError{
						code: "3D000", message: `database "nope" does not exist",`,
					}, nil
				}
			},
			wantCode: "3D000",
			wantMsg:  `database "nope" does not exist",`,
		},
		{
			name: "disabled user",
			build: func(req *sessionActivationRequest, _ *fakeActivationSessions) {
				req.finish = func(context.Context, sessionAcquisition) (sessionMetadataResult, *sessionInitError, error) {
					return sessionMetadataResult{}, nil, errEscalationUserDisabled
				}
			},
			wantCode: "28000",
			wantMsg:  disabledUserMessage,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sessions := &fakeActivationSessions{}
			req := newActivationRequest(t, sessions)
			tc.build(&req, sessions)

			res, err := activationTestCP().activateConnectionSession(context.Background(), req)
			if err == nil {
				t.Fatal("want an error")
			}
			var acq *server.SessionAcquireError
			if !errors.As(err, &acq) {
				t.Fatalf("error %v is not a *server.SessionAcquireError; the client would get a guessed SQLSTATE", err)
			}
			if acq.Code != tc.wantCode {
				t.Fatalf("SQLSTATE = %q, want %q", acq.Code, tc.wantCode)
			}
			if acq.Message != tc.wantMsg {
				t.Fatalf("client message = %q, want %q", acq.Message, tc.wantMsg)
			}
			// A post-create failure is destroyed inside the acquisition, so the
			// caller must not repeat it.
			if res.sessionCreated {
				t.Fatal("sessionCreated true on a failure path")
			}
		})
	}
}

// TestActivateConnectionSessionCancelAbortsAcquire asserts a CancelRequest can
// abort a slow first-statement acquire, the parity with the eager connect path
// that createSessionWithRegisteredCancel provides: the acquisition registers
// its backend key, so cancelling that key unblocks a create that is waiting on
// a cold worker spawn.
func TestActivateConnectionSessionCancelAbortsAcquire(t *testing.T) {
	var srv server.Server
	server.InitMinimalServer(&srv, server.Config{}, nil)
	key := server.BackendKey{Pid: 1000, SecretKey: 7}

	started := make(chan struct{})
	sessions := &fakeActivationSessions{
		createFn: func(ctx context.Context, _ string, _ int, _ *WorkerProfile) (*flightclient.FlightExecutor, error) {
			close(started)
			<-ctx.Done() // a cold spawn: blocks until cancelled or timed out
			return nil, ctx.Err()
		},
	}
	req := newActivationRequest(t, sessions)
	req.srv = &srv
	req.backendKey = key

	cp := activationTestCP()
	cp.cfg.WorkerQueueTimeout = 30 * time.Second // long enough that only the cancel can end this

	done := make(chan error, 1)
	go func() {
		_, err := cp.activateConnectionSession(context.Background(), req)
		done <- err
	}()

	<-started
	// The same call path a CancelRequest takes.
	srv.CancelQuery(key)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled acquire returned nil error")
		}
		var acq *server.SessionAcquireError
		if !errors.As(err, &acq) || acq.Code != "57014" {
			t.Fatalf("cancelled acquire = %v, want a SessionAcquireError with 57014", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CancelRequest did not abort the acquire; a client cannot cancel a slow first statement")
	}
}

// TestActivationCancelMessageDivergesFromConnectPath pins the deliberate
// wording split: the eager connect path cancels during AUTHENTICATION, a lazy
// activation cancels an in-flight STATEMENT. Same SQLSTATE, different truth —
// re-unifying them would put "canceling authentication" in a client's log
// minutes after it authenticated.
func TestActivationCancelMessageDivergesFromConnectPath(t *testing.T) {
	acq := newSessionAcquireError(context.Canceled)
	if acq.Code != "57014" || acq.Message != "canceling statement due to user request" {
		t.Fatalf("activation cancel = %q/%q, want 57014/canceling statement due to user request", acq.Code, acq.Message)
	}
	if _, connectMsg := sessionCreationErrorResponse(context.Canceled); connectMsg != "canceling authentication due to user request" {
		t.Fatalf("connect-path cancel wording changed to %q; only the activation branch may be re-worded", connectMsg)
	}
	// Every other classification is passed through verbatim.
	if got := newSessionAcquireError(ErrSessionManagerDraining); got.Message != "control plane is draining, retry shortly" {
		t.Fatalf("draining message = %q, want the shared classification verbatim", got.Message)
	}
}

// TestActivationMetricsCarryOrgLabel pins the org label added for per-tenant
// slicing (which tenant is eating cold-spawn waits / hitting its cap).
func TestActivationMetricsCarryOrgLabel(t *testing.T) {
	sessions := &fakeActivationSessions{}
	req := newActivationRequest(t, sessions)
	req.orgID = "org-metric-label"

	before := testutil.ToFloat64(sessionActivationTotal.WithLabelValues(req.orgID, string(sessionActivationSuccess)))
	if _, err := activationTestCP().activateConnectionSession(context.Background(), req); err != nil {
		t.Fatalf("activateConnectionSession: %v", err)
	}
	if got := testutil.ToFloat64(sessionActivationTotal.WithLabelValues(req.orgID, string(sessionActivationSuccess))); got != before+1 {
		t.Fatalf("duckgres_session_activation_total{org=%q,outcome=success} = %v, want %v", req.orgID, got, before+1)
	}
	if got := testutil.CollectAndCount(sessionActivationDuration, "duckgres_session_activation_duration_seconds"); got == 0 {
		t.Fatal("duckgres_session_activation_duration_seconds collected no series; the org-labelled histogram never observed")
	}
}

// TestActivationOutcomeForCode pins the bounded metric label set.
func TestActivationOutcomeForCode(t *testing.T) {
	for code, want := range map[string]sessionActivationOutcome{
		"":      sessionActivationSuccess,
		"57014": sessionActivationCanceled,
		"53300": sessionActivationCapacity,
		"57P03": sessionActivationDraining,
		// 28000 is the disabled-user re-check, broken out of the generic error
		// bucket by the shared server.AcquisitionFailureOutcome helper.
		"28000": sessionActivationDisabled,
		"53400": sessionActivationError,
		"3D000": sessionActivationError,
		"58000": sessionActivationError,
	} {
		if got := activationOutcomeForCode(code); got != want {
			t.Fatalf("activationOutcomeForCode(%q) = %q, want %q", code, got, want)
		}
	}
}
