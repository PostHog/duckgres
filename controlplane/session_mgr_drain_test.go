//go:build !kubernetes

package controlplane

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type recordingWorkerPool struct {
	events *[]string
}

func (p *recordingWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	return nil, errors.New("not implemented")
}

func (p *recordingWorkerPool) ReleaseWorker(id int) {
	*p.events = append(*p.events, "pool ReleaseWorker")
}

func (p *recordingWorkerPool) RetireWorker(id int) {}

func (p *recordingWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	return false
}

func (p *recordingWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	return nil, false
}

func (p *recordingWorkerPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *recordingWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *recordingWorkerPool) SetMaxWorkers(n int) {}

func (p *recordingWorkerPool) ShutdownAll() {}

type blockingReleaseWorkerPool struct {
	mu             sync.Mutex
	events         []string
	releaseEntered chan struct{}
	allowRelease   chan struct{}
	releaseOnce    sync.Once
}

func (p *blockingReleaseWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	return nil, errors.New("not implemented")
}

func (p *blockingReleaseWorkerPool) ReleaseWorker(id int) {
	p.appendEvent("pool ReleaseWorker")
	p.releaseOnce.Do(func() { close(p.releaseEntered) })
	<-p.allowRelease
}

func (p *blockingReleaseWorkerPool) RetireWorker(id int) {}

func (p *blockingReleaseWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	return false
}

func (p *blockingReleaseWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	return nil, false
}

func (p *blockingReleaseWorkerPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *blockingReleaseWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *blockingReleaseWorkerPool) SetMaxWorkers(n int) {}

func (p *blockingReleaseWorkerPool) ShutdownAll() {}

func (p *blockingReleaseWorkerPool) appendEvent(event string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
}

func (p *blockingReleaseWorkerPool) snapshotEvents() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

type recordingConnectionLease struct {
	events *[]string
}

func (l *recordingConnectionLease) Release(ctx context.Context) error {
	*l.events = append(*l.events, "lease Release")
	return nil
}

type blockingReleaseConnectionLease struct {
	pool *blockingReleaseWorkerPool
}

func (l *blockingReleaseConnectionLease) Release(ctx context.Context) error {
	l.pool.appendEvent("lease Release")
	return nil
}

type flakyReleaseConnectionLease struct {
	failures int32
	attempts atomic.Int32
}

func (l *flakyReleaseConnectionLease) Release(ctx context.Context) error {
	attempt := l.attempts.Add(1)
	if attempt <= l.failures {
		return errors.New("transient release failure")
	}
	return nil
}

type blockingCrashConnectionLease struct {
	entered  chan struct{}
	allow    chan struct{}
	released atomic.Bool
	once     sync.Once
}

func (l *blockingCrashConnectionLease) Release(ctx context.Context) error {
	l.once.Do(func() { close(l.entered) })
	<-l.allow
	l.released.Store(true)
	return nil
}

type observingConnectionLimiter struct {
	firstRead  chan int
	readAgain  chan struct{}
	secondRead chan int
}

func (l *observingConnectionLimiter) Acquire(ctx context.Context, pid int32, protocol string, maxConnections func() int) (connectionLease, error) {
	first := maxConnections()
	l.firstRead <- first

	select {
	case <-l.readAgain:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	l.secondRead <- maxConnections()
	return nil, nil
}

type runtimeLimiterTestStore struct {
	mu          sync.Mutex
	tryMaxes    []int
	cancels     int
	firstTry    chan struct{}
	leaseID     string
	queuedEntry *configstore.OrgConnectionQueueEntry
}

func (s *runtimeLimiterTestStore) EnqueueOrgConnectionRequest(entry *configstore.OrgConnectionQueueEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entryCopy := *entry
	s.queuedEntry = &entryCopy
	return nil
}

func (s *runtimeLimiterTestStore) TryAcquireOrgConnectionLease(requestID string, maxConnections int, now time.Time) (*configstore.OrgConnectionLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tryMaxes = append(s.tryMaxes, maxConnections)
	if len(s.tryMaxes) == 1 {
		close(s.firstTry)
		return nil, nil
	}
	return &configstore.OrgConnectionLease{LeaseID: s.leaseID, RequestID: requestID}, nil
}

func (s *runtimeLimiterTestStore) ReleaseOrgConnectionLease(leaseID string) error {
	return nil
}

func (s *runtimeLimiterTestStore) CancelOrgConnectionRequest(requestID string, canceledAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancels++
	return nil
}

func (s *runtimeLimiterTestStore) snapshot() ([]int, int, *configstore.OrgConnectionQueueEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tryMaxes := append([]int(nil), s.tryMaxes...)
	return tryMaxes, s.cancels, s.queuedEntry
}

type blockingCreateSessionPool struct {
	worker *ManagedWorker
}

func (p *blockingCreateSessionPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	return p.worker, nil
}

func (p *blockingCreateSessionPool) ReleaseWorker(id int) {}

func (p *blockingCreateSessionPool) RetireWorker(id int) {}

func (p *blockingCreateSessionPool) RetireWorkerIfNoSessions(id int) bool {
	return false
}

func (p *blockingCreateSessionPool) Worker(id int) (*ManagedWorker, bool) {
	return p.worker, p.worker != nil && p.worker.ID == id
}

func (p *blockingCreateSessionPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *blockingCreateSessionPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *blockingCreateSessionPool) SetMaxWorkers(n int) {}

func (p *blockingCreateSessionPool) ShutdownAll() {}

type cancelAwareWorkerPool struct {
	entered          chan struct{}
	reconnectEntered chan struct{}
	once             sync.Once
	reconnectOnce    sync.Once
}

func (p *cancelAwareWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	p.once.Do(func() { close(p.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (p *cancelAwareWorkerPool) ReleaseWorker(id int) {}

func (p *cancelAwareWorkerPool) RetireWorker(id int) {}

func (p *cancelAwareWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	return false
}

func (p *cancelAwareWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	return nil, false
}

func (p *cancelAwareWorkerPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *cancelAwareWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *cancelAwareWorkerPool) SetMaxWorkers(n int) {}

func (p *cancelAwareWorkerPool) ShutdownAll() {}

func (p *cancelAwareWorkerPool) ReconnectFlightWorker(ctx context.Context, workerID int, ownerEpoch int64) (*ManagedWorker, error) {
	p.reconnectOnce.Do(func() {
		if p.reconnectEntered != nil {
			close(p.reconnectEntered)
		}
	})
	<-ctx.Done()
	return nil, ctx.Err()
}

type countingConnectionLease struct {
	releases atomic.Int32
}

func (l *countingConnectionLease) Release(ctx context.Context) error {
	l.releases.Add(1)
	return nil
}

type blockingCreateSessionLimiter struct {
	lease connectionLease
}

func (l *blockingCreateSessionLimiter) Acquire(ctx context.Context, pid int32, protocol string, maxConnections func() int) (connectionLease, error) {
	return l.lease, nil
}

type blockingAcquireLimiter struct {
	entered chan struct{}
	release chan struct{}
	lease   connectionLease
	once    sync.Once
}

func (l *blockingAcquireLimiter) Acquire(ctx context.Context, pid int32, protocol string, maxConnections func() int) (connectionLease, error) {
	l.once.Do(func() { close(l.entered) })
	select {
	case <-l.release:
		return l.lease, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type blockingCreateSessionFlightClient struct {
	createStarted chan struct{}
	allowCreate   chan struct{}
	startOnce     sync.Once
	destroyCalls  atomic.Int32
}

func (c *blockingCreateSessionFlightClient) DoAction(ctx context.Context, action *flight.Action, opts ...grpc.CallOption) (flight.FlightService_DoActionClient, error) {
	switch action.Type {
	case "CreateSession":
		return &blockingCreateSessionActionClient{ctx: ctx, client: c}, nil
	case "DestroySession":
		c.destroyCalls.Add(1)
		return &eofActionClient{ctx: ctx}, nil
	default:
		return nil, errors.New("unexpected action: " + action.Type)
	}
}

func (c *blockingCreateSessionFlightClient) Authenticate(context.Context, ...grpc.CallOption) error {
	return nil
}

func (c *blockingCreateSessionFlightClient) AuthenticateBasicToken(ctx context.Context, username string, password string, opts ...grpc.CallOption) (context.Context, error) {
	return ctx, nil
}

func (c *blockingCreateSessionFlightClient) CancelFlightInfo(ctx context.Context, request *flight.CancelFlightInfoRequest, opts ...grpc.CallOption) (*flight.CancelFlightInfoResult, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) Close() error { return nil }

func (c *blockingCreateSessionFlightClient) RenewFlightEndpoint(ctx context.Context, request *flight.RenewFlightEndpointRequest, opts ...grpc.CallOption) (*flight.FlightEndpoint, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) SetSessionOptions(ctx context.Context, request *flight.SetSessionOptionsRequest, opts ...grpc.CallOption) (*flight.SetSessionOptionsResult, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) GetSessionOptions(ctx context.Context, request *flight.GetSessionOptionsRequest, opts ...grpc.CallOption) (*flight.GetSessionOptionsResult, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) CloseSession(ctx context.Context, request *flight.CloseSessionRequest, opts ...grpc.CallOption) (*flight.CloseSessionResult, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) Handshake(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_HandshakeClient, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) ListFlights(ctx context.Context, in *flight.Criteria, opts ...grpc.CallOption) (flight.FlightService_ListFlightsClient, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) PollFlightInfo(ctx context.Context, in *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) GetSchema(ctx context.Context, in *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) DoGet(ctx context.Context, in *flight.Ticket, opts ...grpc.CallOption) (flight.FlightService_DoGetClient, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) DoPut(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_DoPutClient, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) DoExchange(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_DoExchangeClient, error) {
	return nil, errors.New("not implemented")
}

func (c *blockingCreateSessionFlightClient) ListActions(ctx context.Context, in *flight.Empty, opts ...grpc.CallOption) (flight.FlightService_ListActionsClient, error) {
	return nil, errors.New("not implemented")
}

type blockingCreateSessionActionClient struct {
	ctx    context.Context
	client *blockingCreateSessionFlightClient
	sent   bool
}

func (c *blockingCreateSessionActionClient) Recv() (*flight.Result, error) {
	if c.sent {
		return nil, io.EOF
	}
	c.client.startOnce.Do(func() { close(c.client.createStarted) })
	select {
	case <-c.client.allowCreate:
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
	c.sent = true
	return &flight.Result{Body: []byte(`{"session_token":"session-1"}`)}, nil
}

func (c *blockingCreateSessionActionClient) Header() (metadata.MD, error) { return nil, nil }
func (c *blockingCreateSessionActionClient) Trailer() metadata.MD         { return nil }
func (c *blockingCreateSessionActionClient) CloseSend() error             { return nil }
func (c *blockingCreateSessionActionClient) Context() context.Context     { return c.ctx }
func (c *blockingCreateSessionActionClient) SendMsg(m any) error          { return nil }
func (c *blockingCreateSessionActionClient) RecvMsg(m any) error          { return nil }

type eofActionClient struct {
	ctx context.Context
}

func (c *eofActionClient) Recv() (*flight.Result, error) { return nil, io.EOF }
func (c *eofActionClient) Header() (metadata.MD, error)  { return nil, nil }
func (c *eofActionClient) Trailer() metadata.MD          { return nil }
func (c *eofActionClient) CloseSend() error              { return nil }
func (c *eofActionClient) Context() context.Context      { return c.ctx }
func (c *eofActionClient) SendMsg(m any) error           { return nil }
func (c *eofActionClient) RecvMsg(m any) error           { return nil }

func TestDestroySession_ReleasesLeaseAfterWorkerRelease(t *testing.T) {
	events := []string{}
	pool := &recordingWorkerPool{events: &events}
	sm := NewSessionManager(pool, nil)

	pid := int32(1010)
	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:      pid,
		WorkerID: 7,
		lease:    &recordingConnectionLease{events: &events},
	}
	sm.byWorker[7] = []int32{pid}
	sm.mu.Unlock()

	sm.DestroySession(pid)

	want := []string{"pool ReleaseWorker", "lease Release"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("expected event order %v, got %v", want, events)
	}
}

func TestDestroySessionRetriesTransientLeaseReleaseFailure(t *testing.T) {
	pool := &recordingWorkerPool{events: &[]string{}}
	sm := NewSessionManager(pool, nil)
	lease := &flakyReleaseConnectionLease{failures: 2}

	pid := int32(1010)
	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:      pid,
		WorkerID: 7,
		lease:    lease,
	}
	sm.byWorker[7] = []int32{pid}
	sm.mu.Unlock()

	sm.DestroySession(pid)

	if got := lease.attempts.Load(); got != 3 {
		t.Fatalf("expected lease release to retry until third attempt succeeds, got %d attempts", got)
	}
}

func TestDestroyAllSessionsWaitsForInFlightDestroySessionCleanup(t *testing.T) {
	pool := &blockingReleaseWorkerPool{
		releaseEntered: make(chan struct{}),
		allowRelease:   make(chan struct{}),
	}
	sm := NewSessionManager(pool, nil)

	pid := int32(1010)
	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:      pid,
		WorkerID: 7,
		lease:    &blockingReleaseConnectionLease{pool: pool},
	}
	sm.byWorker[7] = []int32{pid}
	sm.mu.Unlock()

	destroySessionDone := make(chan struct{})
	go func() {
		sm.DestroySession(pid)
		close(destroySessionDone)
	}()

	select {
	case <-pool.releaseEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ReleaseWorker to block")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected session to be removed before ReleaseWorker blocks, got %d", got)
	}

	destroyAllDone := make(chan struct{})
	go func() {
		sm.DestroyAllSessions()
		close(destroyAllDone)
	}()

	select {
	case <-destroyAllDone:
		t.Fatal("DestroyAllSessions returned while DestroySession cleanup was still blocked")
	case <-time.After(25 * time.Millisecond):
	}

	close(pool.allowRelease)

	select {
	case <-destroySessionDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroySession to finish")
	}
	select {
	case <-destroyAllDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroyAllSessions to finish")
	}

	want := []string{"pool ReleaseWorker", "lease Release"}
	if got := pool.snapshotEvents(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("expected event order %v, got %v", want, got)
	}
}

func TestDestroyAllSessionsWaitsForInFlightWorkerCrashLeaseRelease(t *testing.T) {
	lease := &blockingCrashConnectionLease{
		entered: make(chan struct{}),
		allow:   make(chan struct{}),
	}
	sm := NewSessionManager(&FlightWorkerPool{workers: make(map[int]*ManagedWorker)}, nil)

	pid := int32(1010)
	sm.mu.Lock()
	sm.sessions[pid] = &ManagedSession{
		PID:      pid,
		WorkerID: 7,
		lease:    lease,
	}
	sm.byWorker[7] = []int32{pid}
	sm.mu.Unlock()

	crashDone := make(chan struct{})
	go func() {
		sm.OnWorkerCrash(7, func(pid int32) {})
		close(crashDone)
	}()

	select {
	case <-lease.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker crash lease release to block")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected session to be removed before lease release blocks, got %d", got)
	}

	destroyAllDone := make(chan struct{})
	go func() {
		sm.DestroyAllSessions()
		close(destroyAllDone)
	}()

	select {
	case <-destroyAllDone:
		t.Fatal("DestroyAllSessions returned while worker crash lease release was still blocked")
	case <-time.After(25 * time.Millisecond):
	}

	close(lease.allow)

	select {
	case <-crashDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnWorkerCrash to finish")
	}
	select {
	case <-destroyAllDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroyAllSessions to finish")
	}
	if !lease.released.Load() {
		t.Fatal("expected lease to be released")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected no sessions after crash cleanup, got %d", got)
	}
}

func TestDestroyAllSessions_ReleasesAllLeasesAndClearsSessions(t *testing.T) {
	events := []string{}
	pool := &recordingWorkerPool{events: &events}
	sm := NewSessionManager(pool, nil)

	sm.mu.Lock()
	sm.sessions[1010] = &ManagedSession{
		PID:      1010,
		WorkerID: 7,
		lease:    &recordingConnectionLease{events: &events},
	}
	sm.sessions[1011] = &ManagedSession{
		PID:      1011,
		WorkerID: 8,
		lease:    &recordingConnectionLease{events: &events},
	}
	sm.byWorker[7] = []int32{1010}
	sm.byWorker[8] = []int32{1011}
	sm.mu.Unlock()

	sm.DestroyAllSessions()

	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected all sessions destroyed, got %d", got)
	}
	if got := countEvents(events, "lease Release"); got != 2 {
		t.Fatalf("expected two lease releases, got %d events=%v", got, events)
	}
	if got := countEvents(events, "pool ReleaseWorker"); got != 2 {
		t.Fatalf("expected two worker releases, got %d events=%v", got, events)
	}
}

func TestDestroyAllSessionsRejectsInFlightCreateBeforeRegistration(t *testing.T) {
	flightClient := &blockingCreateSessionFlightClient{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
	}
	lease := &countingConnectionLease{}
	worker := &ManagedWorker{
		ID:     7,
		client: &flightsql.Client{Client: flightClient},
		done:   make(chan struct{}),
	}
	pool := &blockingCreateSessionPool{worker: worker}
	sm := NewSessionManager(pool, nil)
	sm.SetConnectionLimiter(&blockingCreateSessionLimiter{lease: lease})

	createErr := make(chan error, 1)
	go func() {
		_, _, err := sm.CreateSessionWithProtocol(context.Background(), "root", "", 1010, "", 0, "postgres")
		createErr <- err
	}()

	select {
	case <-flightClient.createStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CreateSession RPC to start")
	}

	destroyDone := make(chan struct{})
	go func() {
		sm.DestroyAllSessions()
		close(destroyDone)
	}()
	waitForSessionManagerDraining(t, sm)
	close(flightClient.allowCreate)

	select {
	case err := <-createErr:
		if err == nil {
			t.Fatal("expected in-flight CreateSession to fail once draining starts")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CreateSession to return")
	}
	select {
	case <-destroyDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroyAllSessions to return")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected no sessions registered after drain, got %d", got)
	}
	if got := lease.releases.Load(); got != 1 {
		t.Fatalf("expected acquired lease to be released once, got %d", got)
	}
}

func TestDestroyAllSessionsWaitsForCreateBlockedInLimiterAcquire(t *testing.T) {
	lease := &countingConnectionLease{}
	limiter := &blockingAcquireLimiter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		lease:   lease,
	}
	sm := NewSessionManager(&acquireErrorPool{err: errors.New("worker should not be acquired")}, nil)
	sm.SetConnectionLimiter(limiter)

	createErr := make(chan error, 1)
	go func() {
		_, _, err := sm.CreateSessionWithProtocol(context.Background(), "root", "", 1010, "", 0, "postgres")
		createErr <- err
	}()

	select {
	case <-limiter.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for limiter acquire")
	}

	destroyDone := make(chan struct{})
	go func() {
		sm.DestroyAllSessions()
		close(destroyDone)
	}()
	waitForSessionManagerDraining(t, sm)

	select {
	case err := <-createErr:
		if err == nil {
			t.Fatal("expected CreateSession to fail when drain cancels limiter Acquire")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CreateSession to return")
	}
	select {
	case <-destroyDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroyAllSessions to return")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected no sessions registered after drain, got %d", got)
	}
	if got := lease.releases.Load(); got != 0 {
		t.Fatalf("expected no lease release before limiter returned a lease, got %d", got)
	}
}

func TestDestroyAllSessionsCancelsCreateBlockedInAcquireWorker(t *testing.T) {
	lease := &countingConnectionLease{}
	pool := &cancelAwareWorkerPool{entered: make(chan struct{})}
	sm := NewSessionManager(pool, nil)
	sm.SetConnectionLimiter(&blockingCreateSessionLimiter{lease: lease})

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()

	createErr := make(chan error, 1)
	go func() {
		_, _, err := sm.CreateSessionWithProtocol(callerCtx, "root", "", 1010, "", 0, "postgres")
		createErr <- err
	}()

	select {
	case <-pool.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for AcquireWorker")
	}

	destroyDone := make(chan struct{})
	go func() {
		sm.DestroyAllSessions()
		close(destroyDone)
	}()
	waitForSessionManagerDraining(t, sm)

	select {
	case err := <-createErr:
		if err == nil {
			t.Fatal("expected CreateSession to fail when drain cancels AcquireWorker")
		}
	case <-time.After(100 * time.Millisecond):
		cancelCaller()
		select {
		case <-createErr:
		case <-time.After(time.Second):
			t.Fatal("timed out cleaning up blocked CreateSession")
		}
		select {
		case <-destroyDone:
		case <-time.After(time.Second):
			t.Fatal("timed out cleaning up blocked DestroyAllSessions")
		}
		t.Fatal("DestroyAllSessions did not cancel CreateSession blocked in AcquireWorker")
	}

	select {
	case <-destroyDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroyAllSessions to return")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected no sessions registered after drain, got %d", got)
	}
	if got := lease.releases.Load(); got != 1 {
		t.Fatalf("expected acquired lease to be released once, got %d", got)
	}
}

func TestDestroyAllSessionsCancelsReconnectBlockedInReconnectWorker(t *testing.T) {
	lease := &countingConnectionLease{}
	pool := &cancelAwareWorkerPool{reconnectEntered: make(chan struct{})}
	sm := NewSessionManager(pool, nil)
	sm.SetConnectionLimiter(&blockingCreateSessionLimiter{lease: lease})

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()

	reconnectErr := make(chan error, 1)
	go func() {
		_, _, err := sm.ReconnectFlightSession(callerCtx, "root", 7, 1)
		reconnectErr <- err
	}()

	select {
	case <-pool.reconnectEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ReconnectFlightWorker")
	}

	destroyDone := make(chan struct{})
	go func() {
		sm.DestroyAllSessions()
		close(destroyDone)
	}()
	waitForSessionManagerDraining(t, sm)

	select {
	case err := <-reconnectErr:
		if err == nil {
			t.Fatal("expected ReconnectFlightSession to fail when drain cancels reconnect")
		}
	case <-time.After(100 * time.Millisecond):
		cancelCaller()
		select {
		case <-reconnectErr:
		case <-time.After(time.Second):
			t.Fatal("timed out cleaning up blocked ReconnectFlightSession")
		}
		select {
		case <-destroyDone:
		case <-time.After(time.Second):
			t.Fatal("timed out cleaning up blocked DestroyAllSessions")
		}
		t.Fatal("DestroyAllSessions did not cancel ReconnectFlightSession blocked in ReconnectFlightWorker")
	}

	select {
	case <-destroyDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DestroyAllSessions to return")
	}
	if got := sm.SessionCount(); got != 0 {
		t.Fatalf("expected no sessions registered after drain, got %d", got)
	}
	if got := lease.releases.Load(); got != 1 {
		t.Fatalf("expected acquired lease to be released once, got %d", got)
	}
}

func waitForSessionManagerDraining(t *testing.T, sm *SessionManager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sm.lifecycle.isClosed() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for session manager to start draining")
}

func countEvents(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}

func TestSessionManager_RuntimeLimiterObservesDynamicLimitWhileQueued(t *testing.T) {
	sm := NewSessionManager(nil, nil)
	sm.SetMaxConnections(1)

	limiter := &observingConnectionLimiter{
		firstRead:  make(chan int, 1),
		readAgain:  make(chan struct{}),
		secondRead: make(chan int, 1),
	}
	sm.SetConnectionLimiter(limiter)

	acquired := make(chan error, 1)
	go func() {
		_, err := sm.acquireConnectionSlot(context.Background(), 1001, "postgres")
		acquired <- err
	}()

	select {
	case got := <-limiter.firstRead:
		if got != 1 {
			t.Fatalf("expected first max connection read to see 1, got %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first max connection read")
	}

	sm.SetMaxConnections(2)
	close(limiter.readAgain)

	select {
	case got := <-limiter.secondRead:
		if got != 2 {
			t.Fatalf("expected queued limiter read to observe updated max connections 2, got %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second max connection read")
	}

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("expected limiter acquire to succeed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire")
	}
}

func TestRuntimeOrgConnectionLimiterKeepsQueuedLeaseWhenLimitBecomesUnlimited(t *testing.T) {
	store := &runtimeLimiterTestStore{
		firstTry: make(chan struct{}),
		leaseID:  "lease-1",
	}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
		orgID:        "org-1",
		cpInstanceID: "cp-1",
		queueTTL:     time.Second,
		pollInterval: time.Millisecond,
		now:          time.Now,
		newID: func() (string, error) {
			return "request-1", nil
		},
	}

	var maxConnections atomic.Int32
	maxConnections.Store(1)
	acquired := make(chan connectionLease, 1)
	acquireErr := make(chan error, 1)
	go func() {
		lease, err := limiter.Acquire(context.Background(), 1001, "postgres", func() int {
			return int(maxConnections.Load())
		})
		acquired <- lease
		acquireErr <- err
	}()

	select {
	case <-store.firstTry:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first lease attempt")
	}
	maxConnections.Store(0)

	var lease connectionLease
	select {
	case lease = <-acquired:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for limiter acquire")
	}
	if lease == nil {
		t.Fatal("expected limiter to return a durable lease when limit becomes unlimited")
	}

	select {
	case err := <-acquireErr:
		if err != nil {
			t.Fatalf("expected limiter acquire to succeed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for limiter acquire error")
	}

	tryMaxes, cancels, queuedEntry := store.snapshot()
	if queuedEntry == nil {
		t.Fatal("expected request to be queued")
	}
	if len(tryMaxes) != 2 {
		t.Fatalf("expected two lease attempts, got %d (%v)", len(tryMaxes), tryMaxes)
	}
	if tryMaxes[0] != 1 || tryMaxes[1] != 0 {
		t.Fatalf("expected lease attempts with max connections [1 0], got %v", tryMaxes)
	}
	if cancels != 0 {
		t.Fatalf("expected queued request not to be canceled, got %d cancels", cancels)
	}
}
