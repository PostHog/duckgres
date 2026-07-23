//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type recordingOrgRouterPool struct {
	events        *[]string
	shutdownCount *atomic.Int32
}

func (p *recordingOrgRouterPool) AcquireWorker(ctx context.Context, _ *WorkerProfile) (*ManagedWorker, error) {
	return nil, errors.New("not implemented")
}

func (p *recordingOrgRouterPool) ReleaseWorker(id int) {
	*p.events = append(*p.events, "pool ReleaseWorker")
}

func (p *recordingOrgRouterPool) RetireWorker(id int) {}

func (p *recordingOrgRouterPool) RetireWorkerIfNoSessions(id int) bool {
	return false
}

func (p *recordingOrgRouterPool) Worker(id int) (*ManagedWorker, bool) {
	return nil, false
}

func (p *recordingOrgRouterPool) SpawnMinWorkers(count int) error {
	return nil
}

func (p *recordingOrgRouterPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *recordingOrgRouterPool) SetMaxWorkers(n int) {}

func (p *recordingOrgRouterPool) ShutdownAll() {
	if p.events != nil {
		*p.events = append(*p.events, "pool ShutdownAll")
	}
	if p.shutdownCount != nil {
		p.shutdownCount.Add(1)
	}
}

type recordingOrgRouterLease struct {
	events *[]string
}

func (l *recordingOrgRouterLease) Release(ctx context.Context) error {
	*l.events = append(*l.events, "lease Release")
	return nil
}

type blockingOrgRouterLease struct {
	started chan struct{}
	release chan struct{}
}

func (l *blockingOrgRouterLease) Release(context.Context) error {
	l.started <- struct{}{}
	<-l.release
	return nil
}

func TestOrgRouterBeginDrainStopsCreationWithoutDestroyingEstablishedSessions(t *testing.T) {
	first := NewSessionManager(nil, nil)
	second := NewSessionManager(nil, nil)
	first.sessions[1010] = &ManagedSession{PID: 1010}
	second.sessions[2020] = &ManagedSession{PID: 2020}

	router := &OrgRouter{
		orgs: map[string]*OrgStack{
			"first":  {Sessions: first},
			"second": {Sessions: second},
		},
	}

	router.BeginDrain()

	for name, sessions := range map[string]*SessionManager{"first": first, "second": second} {
		if !sessions.lifecycle.isClosed() {
			t.Fatalf("expected %s org session manager to stop creation", name)
		}
		if got := sessions.SessionCount(); got != 1 {
			t.Fatalf("expected %s org established session to survive BeginDrain, got %d", name, got)
		}
	}

	late := NewSessionManager(nil, nil)
	router.publishOrgStack("late", &OrgStack{Sessions: late})
	if !late.lifecycle.isClosed() {
		t.Fatal("expected an org stack published after BeginDrain to start drained")
	}
}

func TestOrgRouterDestroyOrgStackDrainsSessionsBeforePoolShutdownAndReleasesSessionLeases(t *testing.T) {
	events := []string{}
	pool := &recordingOrgRouterPool{events: &events}
	sessions := NewSessionManager(pool, nil)
	reclaimer := &recordingAdmissionReclaimer{}

	sessions.mu.Lock()
	sessions.sessions[1010] = &ManagedSession{
		PID:      1010,
		WorkerID: 7,
		lease:    &recordingOrgRouterLease{events: &events},
	}
	sessions.byWorker[7] = []int32{1010}
	sessions.mu.Unlock()

	tr := &OrgRouter{
		admissionReclaimer: reclaimer,
		orgs: map[string]*OrgStack{
			"analytics": {
				Pool:     pool,
				Sessions: sessions,
				cancel: func() {
					events = append(events, "stack cancel")
				},
			},
		},
	}

	tr.DestroyOrgStack("analytics")

	want := []string{"stack cancel", "pool ReleaseWorker", "lease Release", "pool ShutdownAll"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("expected destroy order %v, got %v", want, events)
	}
	if got := sessions.SessionCount(); got != 0 {
		t.Fatalf("expected sessions drained, got %d", got)
	}
	if _, ok := tr.orgs["analytics"]; ok {
		t.Fatal("expected org stack to be removed")
	}
	if _, drainCalls := reclaimer.snapshot(); drainCalls != 0 {
		t.Fatalf("DestroyOrgStack drained the CP-wide admission reclaimer %d times, want 0", drainCalls)
	}
}

func TestOrgRouterShutdownAllDrainsAdmissionReclaimerAfterAllSessions(t *testing.T) {
	events := []string{}
	firstPool := &recordingOrgRouterPool{events: &events}
	secondPool := &recordingOrgRouterPool{events: &events}
	firstSessions := NewSessionManager(firstPool, nil)
	secondSessions := NewSessionManager(secondPool, nil)

	addSession := func(sm *SessionManager, pid int32, workerID int) {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		sm.sessions[pid] = &ManagedSession{
			PID: pid, WorkerID: workerID, lease: &recordingOrgRouterLease{events: &events},
		}
		sm.byWorker[workerID] = []int32{pid}
	}
	addSession(firstSessions, 1010, 7)
	addSession(secondSessions, 2020, 8)

	allSessionsDrained := false
	reclaimer := &recordingAdmissionReclaimer{onDrain: func() {
		allSessionsDrained = firstSessions.SessionCount() == 0 && secondSessions.SessionCount() == 0
		events = append(events, "admission DrainAndClose")
	}}
	router := &OrgRouter{
		orgs: map[string]*OrgStack{
			"first":  {Pool: firstPool, Sessions: firstSessions, cancel: func() {}},
			"second": {Pool: secondPool, Sessions: secondSessions, cancel: func() {}},
		},
		admissionReclaimer: reclaimer,
	}

	router.ShutdownAll()
	router.ShutdownAll()

	if !allSessionsDrained {
		t.Fatal("admission reclaimer drained before every org session manager completed cleanup")
	}
	_, drainCalls := reclaimer.snapshot()
	if drainCalls != 1 {
		t.Fatalf("admission reclaimer drain calls = %d, want 1", drainCalls)
	}
}

func TestOrgRouterShutdownAllWaitsForConcurrentOrgDestroyBeforeDrainingReclaimer(t *testing.T) {
	events := []string{}
	pool := &recordingOrgRouterPool{events: &events}
	lease := &blockingOrgRouterLease{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	sessions := NewSessionManager(pool, nil)
	sessions.mu.Lock()
	sessions.sessions[1010] = &ManagedSession{PID: 1010, WorkerID: 7, lease: lease}
	sessions.byWorker[7] = []int32{1010}
	sessions.mu.Unlock()

	drainStarted := make(chan struct{}, 1)
	reclaimer := &recordingAdmissionReclaimer{onDrain: func() {
		drainStarted <- struct{}{}
	}}
	router := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {Pool: pool, Sessions: sessions, cancel: func() {}},
		},
		admissionReclaimer: reclaimer,
	}

	destroyDone := make(chan struct{})
	go func() {
		router.DestroyOrgStack("analytics")
		close(destroyDone)
	}()
	select {
	case <-lease.started:
	case <-time.After(time.Second):
		t.Fatal("org destroy did not reach lease release")
	}

	shutdownDone := make(chan struct{})
	go func() {
		router.ShutdownAll()
		close(shutdownDone)
	}()
	prematureDrain := false
	select {
	case <-drainStarted:
		prematureDrain = true
	case <-time.After(50 * time.Millisecond):
	}

	close(lease.release)
	select {
	case <-destroyDone:
	case <-time.After(time.Second):
		t.Fatal("org destroy did not finish after lease release")
	}
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("router shutdown did not finish after org destroy")
	}
	if prematureDrain {
		t.Fatal("admission reclaimer drained while a detached org stack was still releasing leases")
	}
}

func TestOrgRouterSerializesCreationBehindDestroyTeardown(t *testing.T) {
	events := []string{}
	pool := &recordingOrgRouterPool{events: &events}
	lease := &blockingOrgRouterLease{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	sessions := NewSessionManager(pool, nil)
	sessions.mu.Lock()
	sessions.sessions[1010] = &ManagedSession{PID: 1010, WorkerID: 7, lease: lease}
	sessions.byWorker[7] = []int32{1010}
	sessions.mu.Unlock()
	router := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {Pool: pool, Sessions: sessions},
		},
	}

	destroyDone := make(chan struct{})
	go func() {
		router.DestroyOrgStack("analytics")
		close(destroyDone)
	}()
	select {
	case <-lease.started:
	case <-time.After(time.Second):
		t.Fatal("org destroy did not reach lease cleanup")
	}

	type beginResult struct {
		finish func()
		err    error
	}
	creationResult := make(chan beginResult, 1)
	go func() {
		finish, err := router.beginOrgStackCreation("analytics")
		creationResult <- beginResult{finish: finish, err: err}
	}()
	select {
	case result := <-creationResult:
		if result.finish != nil {
			result.finish()
		}
		t.Fatalf("replacement creation started before old stack teardown finished: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(lease.release)
	select {
	case <-destroyDone:
	case <-time.After(time.Second):
		t.Fatal("org destroy did not finish after lease cleanup")
	}
	select {
	case result := <-creationResult:
		if result.err != nil {
			t.Fatalf("replacement creation after teardown: %v", result.err)
		}
		if result.finish == nil {
			t.Fatal("replacement creation did not acquire the org mutation slot")
		}
		result.finish()
	case <-time.After(time.Second):
		t.Fatal("replacement creation did not resume after old stack teardown")
	}
}

func TestOrgRouterShutdownDoesNotTeardownStackOwnedByConcurrentDestroy(t *testing.T) {
	var shutdownCount atomic.Int32
	pool := &recordingOrgRouterPool{shutdownCount: &shutdownCount}
	stack := &OrgStack{Pool: pool}
	router := &OrgRouter{
		orgs: map[string]*OrgStack{"analytics": stack},
	}
	finishDestroy, err := router.beginOrgStackMutation("analytics")
	if err != nil {
		t.Fatalf("begin destroy mutation: %v", err)
	}

	shutdownDone := make(chan struct{})
	go func() {
		router.ShutdownAll()
		close(shutdownDone)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		router.mu.RLock()
		terminal := router.terminal
		router.mu.RUnlock()
		if terminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("router shutdown did not become terminal")
		}
		time.Sleep(time.Millisecond)
	}

	router.mu.Lock()
	delete(router.orgs, "analytics")
	router.mu.Unlock()
	shutdownOrgStack(stack)
	finishDestroy()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("router shutdown did not finish after destroy mutation")
	}
	if got := shutdownCount.Load(); got != 1 {
		t.Fatalf("org pool shutdown count = %d, want exactly one teardown owner", got)
	}
}

func TestOrgRouterShutdownAllWaitsForInflightCreateAndRejectsLatePublication(t *testing.T) {
	events := []string{}
	pool := &recordingOrgRouterPool{events: &events}
	sessions := NewSessionManager(pool, nil)
	drainStarted := make(chan struct{}, 1)
	reclaimer := &recordingAdmissionReclaimer{onDrain: func() {
		drainStarted <- struct{}{}
	}}
	router := &OrgRouter{
		orgs:               make(map[string]*OrgStack),
		admissionReclaimer: reclaimer,
	}

	finishCreate, ok := router.beginOrgStackOperation()
	if !ok {
		t.Fatal("expected stack creation to register before shutdown")
	}

	shutdownDone := make(chan struct{})
	go func() {
		router.ShutdownAll()
		close(shutdownDone)
	}()

	terminalObserved := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		router.mu.RLock()
		terminalObserved = router.terminal
		router.mu.RUnlock()
		if terminalObserved {
			break
		}
		time.Sleep(time.Millisecond)
	}
	prematureDrain := false
	select {
	case <-drainStarted:
		prematureDrain = true
	case <-time.After(50 * time.Millisecond):
	}

	late := &OrgStack{
		Pool:     pool,
		Sessions: sessions,
		cancel: func() {
			events = append(events, "stack cancel")
		},
	}
	publishResult := router.publishOrgStack("late", late)
	if publishResult != orgStackPublishAccepted {
		shutdownUnpublishedOrgStack(late)
	}
	finishCreate()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("router shutdown did not finish after in-flight creation completed")
	}

	if !terminalObserved {
		t.Fatal("router shutdown did not enter its terminal state")
	}
	if prematureDrain {
		t.Fatal("admission reclaimer drained while an org stack creation was still in flight")
	}
	if publishResult != orgStackPublishRejectedTerminal {
		t.Fatalf("late org stack publish result = %v, want terminal rejection", publishResult)
	}
	if _, ok := router.StackForOrg("late"); ok {
		t.Fatal("late org stack remained visible after shutdown")
	}
	wantEvents := []string{"stack cancel"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("late org stack cleanup events = %v, want %v", events, wantEvents)
	}
	if !sessions.lifecycle.isClosed() {
		t.Fatal("late unpublished stack session lifecycle remained open")
	}
}

func TestOrgRouterPublishOrgStackIsInsertOnlyWithoutOrgScopedLoserCleanup(t *testing.T) {
	type candidate struct {
		stack  *OrgStack
		events *[]string
	}

	newCandidate := func() candidate {
		events := []string{}
		pool := &recordingOrgRouterPool{events: &events}
		sessions := NewSessionManager(pool, nil)

		return candidate{
			stack: &OrgStack{
				Pool:       pool,
				Sessions:   sessions,
				Rebalancer: NewMemoryRebalancer(1, 1, nil, false),
				cancel: func() {
					events = append(events, "stack cancel")
				},
			},
			events: &events,
		}
	}

	router := &OrgRouter{orgs: make(map[string]*OrgStack)}
	candidates := []candidate{newCandidate(), newCandidate()}
	start := make(chan struct{})
	publishResults := make(chan orgStackPublishResult, len(candidates))
	for _, candidate := range candidates {
		candidate := candidate
		go func() {
			<-start
			result := router.publishOrgStack("analytics", candidate.stack)
			if result != orgStackPublishAccepted {
				shutdownUnpublishedOrgStack(candidate.stack)
			}
			publishResults <- result
		}()
	}
	close(start)

	publishedCount := 0
	for range candidates {
		if result := <-publishResults; result == orgStackPublishAccepted {
			publishedCount++
		} else if result != orgStackPublishRejectedDuplicate {
			t.Fatalf("concurrent publish result = %v, want accepted or duplicate rejection", result)
		}
	}
	if publishedCount != 1 {
		t.Fatalf("published stack count = %d, want 1", publishedCount)
	}

	winner, ok := router.StackForOrg("analytics")
	if !ok {
		t.Fatal("published org stack is missing")
	}
	if len(router.orgs) != 1 {
		t.Fatalf("router org stack count = %d, want 1", len(router.orgs))
	}

	wantLoserEvents := []string{"stack cancel"}
	for i, candidate := range candidates {
		if candidate.stack == winner {
			if len(*candidate.events) != 0 {
				t.Fatalf("winner %d cleanup events = %v, want none", i, *candidate.events)
			}
			select {
			case <-candidate.stack.Rebalancer.stopDebounce:
				t.Fatalf("winner %d rebalancer was stopped", i)
			default:
			}
			continue
		}
		if !reflect.DeepEqual(*candidate.events, wantLoserEvents) {
			t.Fatalf("loser %d cleanup events = %v, want %v", i, *candidate.events, wantLoserEvents)
		}
		if !candidate.stack.Sessions.lifecycle.isClosed() {
			t.Fatalf("loser %d session lifecycle remained open", i)
		}
		select {
		case <-candidate.stack.Rebalancer.stopDebounce:
		default:
			t.Fatalf("loser %d rebalancer was not stopped", i)
		}
	}
}

func TestOrgRouterPublishCandidateRejectsReadyConfigSupersededDuringConstruction(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	ready := &configstore.OrgConfig{
		Name:       "analytics",
		MaxWorkers: 2,
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			Image: "posthog/duckgres:ready",
		},
	}
	provisioning := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateProvisioning,
		},
	}
	store := newTestConfigStoreWithSnapshot(&configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": provisioning},
	})
	router := &OrgRouter{
		orgs:        make(map[string]*OrgStack),
		configStore: store,
	}
	candidate := &OrgStack{
		Config: ready,
		Pool:   NewOrgReservedPool(sharedPool, "analytics", ready.MaxWorkers, ready.Warehouse.Image, nil),
	}

	result, err := router.publishOrgStackCandidate("analytics", candidate)
	if err == nil {
		t.Fatal("candidate authorized by superseded ready config was not rejected")
	}
	if result != orgStackPublishRejectedConfig {
		t.Fatalf("candidate publish result = %v, want config rejection", result)
	}
	if _, ok := router.StackForOrg("analytics"); ok {
		t.Fatal("candidate authorized by superseded ready config was published")
	}
}

func TestOrgRouterPublishCandidateRefreshesReadyConfigSupersededDuringConstruction(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	readyV1 := &configstore.OrgConfig{
		Name:       "analytics",
		MaxWorkers: 2,
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			Image: "posthog/duckgres:v1",
		},
	}
	readyV2 := &configstore.OrgConfig{
		Name:       "analytics",
		MaxWorkers: 7,
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			Image: "posthog/duckgres:v2",
		},
	}
	store := newTestConfigStoreWithSnapshot(&configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": readyV2},
	})
	pool := NewOrgReservedPool(sharedPool, "analytics", readyV1.MaxWorkers, readyV1.Warehouse.Image, nil)
	candidate := &OrgStack{Config: readyV1, Pool: pool}
	router := &OrgRouter{
		orgs:        make(map[string]*OrgStack),
		configStore: store,
	}

	result, err := router.publishOrgStackCandidate("analytics", candidate)
	if err != nil {
		t.Fatalf("publish candidate: %v", err)
	}
	if result != orgStackPublishAccepted {
		t.Fatalf("candidate publish result = %v, want accepted", result)
	}
	if candidate.Config != readyV2 {
		t.Fatal("candidate did not adopt the latest ready config before publication")
	}
	if pool.maxWorkers != readyV2.MaxWorkers {
		t.Fatalf("candidate max workers = %d, want %d", pool.maxWorkers, readyV2.MaxWorkers)
	}
	if pool.image != readyV2.Warehouse.Image {
		t.Fatalf("candidate image = %q, want %q", pool.image, readyV2.Warehouse.Image)
	}
	if got, ok := router.StackForOrg("analytics"); !ok || got != candidate {
		t.Fatal("latest-ready candidate was not published")
	}
	router.DestroyOrgStack("analytics")
}

func TestOrgRouterPublishCandidateRejectsUnexpectedPoolType(t *testing.T) {
	ready := &configstore.OrgConfig{Name: "analytics"}
	store := newTestConfigStoreWithSnapshot(&configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": ready},
	})
	router := &OrgRouter{
		orgs:        make(map[string]*OrgStack),
		configStore: store,
	}
	candidate := &OrgStack{Config: ready, Pool: &recordingOrgRouterPool{}}

	result, err := router.publishOrgStackCandidate("analytics", candidate)
	if err == nil {
		t.Fatal("candidate with unexpected pool type was not rejected")
	}
	if result != orgStackPublishRejectedConfig {
		t.Fatalf("candidate publish result = %v, want config rejection", result)
	}
	if _, ok := router.StackForOrg("analytics"); ok {
		t.Fatal("candidate with unexpected pool type was published")
	}
}

func TestOrgRouterSerializesConcurrentCreationBeforeBuildingDuplicatePool(t *testing.T) {
	router := &OrgRouter{orgs: make(map[string]*OrgStack)}
	finishFirst, err := router.beginOrgStackCreation("analytics")
	if err != nil {
		t.Fatalf("begin first creation: %v", err)
	}

	type beginResult struct {
		finish func()
		err    error
	}
	secondResult := make(chan beginResult, 1)
	go func() {
		finish, err := router.beginOrgStackCreation("analytics")
		secondResult <- beginResult{finish: finish, err: err}
	}()
	select {
	case result := <-secondResult:
		if result.finish != nil {
			result.finish()
		}
		t.Fatalf("second same-org creation was not serialized: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}

	router.mu.Lock()
	router.orgs["analytics"] = &OrgStack{}
	router.mu.Unlock()
	finishFirst()

	select {
	case result := <-secondResult:
		if result.finish != nil {
			result.finish()
			t.Fatal("second creation acquired ownership after the first published")
		}
		if result.err == nil {
			t.Fatal("second creation did not report the published org stack")
		}
	case <-time.After(time.Second):
		t.Fatal("second creation did not resume after the first finished")
	}
}

func TestOrgRouterHandleConfigChangeStaleRemovalKeepsStackRequiredByLatestSnapshot(t *testing.T) {
	org := &configstore.OrgConfig{Name: "analytics"}
	latest := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": org},
	}
	store := newTestConfigStoreWithSnapshot(latest)
	var shutdownCount atomic.Int32
	stack := &OrgStack{
		Config: org,
		Pool:   &recordingOrgRouterPool{shutdownCount: &shutdownCount},
	}
	router := &OrgRouter{
		orgs:        map[string]*OrgStack{"analytics": stack},
		configStore: store,
	}

	// Model an older removal callback that resumes after ConfigStore has already
	// published a newer snapshot in which the org should remain available.
	router.HandleConfigChange(latest, &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{}})

	got, ok := router.StackForOrg("analytics")
	if !ok || got != stack {
		t.Fatal("stale removal callback destroyed the stack required by the latest snapshot")
	}
	if got := shutdownCount.Load(); got != 0 {
		t.Fatalf("stale removal callback shut down the current stack %d times, want 0", got)
	}
}

func TestOrgRouterHandleConfigChangeStaleReadyDoesNotCreateAgainstLatestSnapshot(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	staleReady := &configstore.OrgConfig{Name: "analytics"}
	latestDeleting := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateDeleting,
		},
	}
	store := newTestConfigStoreWithSnapshot(&configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": latestDeleting},
	})
	router := &OrgRouter{
		orgs:        make(map[string]*OrgStack),
		configStore: store,
		sharedPool:  sharedPool,
	}
	t.Cleanup(router.ShutdownAll)

	// Model an older ready callback that resumes after the latest snapshot has
	// moved the warehouse into deletion.
	router.HandleConfigChange(
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{}},
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": staleReady}},
	)

	_, created := router.StackForOrg("analytics")
	if created {
		t.Fatal("stale ready callback created a stack rejected by the latest snapshot")
	}
}

func TestOrgRouterHandleConfigChangeDeletingDestroysExistingStackExactlyOnce(t *testing.T) {
	ready := &configstore.OrgConfig{
		Name:      "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{State: configstore.ManagedWarehouseStateReady},
	}
	deleting := &configstore.OrgConfig{
		Name:      "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{State: configstore.ManagedWarehouseStateDeleting},
	}
	latest := &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": deleting}}
	store := newTestConfigStoreWithSnapshot(latest)
	var shutdownCount atomic.Int32
	router := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {
				Config: ready,
				Pool:   &recordingOrgRouterPool{shutdownCount: &shutdownCount},
			},
		},
		configStore: store,
	}

	router.HandleConfigChange(
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": ready}},
		latest,
	)

	if _, ok := router.StackForOrg("analytics"); ok {
		t.Fatal("deleting warehouse retained its org stack")
	}
	if got := shutdownCount.Load(); got != 1 {
		t.Fatalf("deleting warehouse shut down stack %d times, want 1", got)
	}
}

func TestOrgRouterHandleConfigChangeReconcilesAfterRemovalBecomesStaleInFlight(t *testing.T) {
	key := configstore.OrgUserKey{OrgID: "analytics", Username: "reader"}
	org := &configstore.OrgConfig{Name: "analytics"}
	beforeRemoval := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": org},
		OrgUserAccess: map[configstore.OrgUserKey]configstore.OrgUserAccessConfig{
			key: {Mode: configstore.OrgUserAccessModeProjectReader},
		},
	}
	removed := &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{}}
	restored := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{"analytics": org},
	}
	store := newTestConfigStoreWithSnapshot(removed)
	var shutdownCount atomic.Int32
	stack := &OrgStack{
		Config: org,
		Pool:   &recordingOrgRouterPool{shutdownCount: &shutdownCount},
	}
	router := &OrgRouter{
		orgs:        map[string]*OrgStack{"analytics": stack},
		configStore: store,
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	releaseBarrier := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(releaseBarrier)
	router.setProjectReaderChangeHandler(func(string, string) {
		close(entered)
		<-release
	})
	staleDone := make(chan struct{})
	go func() {
		router.HandleConfigChange(beforeRemoval, removed)
		close(staleDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stale removal callback did not reach the barrier")
	}

	setTestConfigStoreSnapshot(store, restored)
	latestDone := make(chan struct{})
	go func() {
		router.HandleConfigChange(removed, restored)
		close(latestDone)
	}()
	select {
	case <-latestDone:
	case <-time.After(time.Second):
		t.Fatal("latest restore callback did not finish")
	}
	if got, ok := router.StackForOrg("analytics"); !ok || got != stack {
		t.Fatal("latest restore callback did not preserve the current stack")
	}

	releaseBarrier()
	select {
	case <-staleDone:
	case <-time.After(time.Second):
		t.Fatal("stale removal callback did not finish")
	}

	got, ok := router.StackForOrg("analytics")
	if !ok || got != stack {
		t.Fatal("in-flight stale removal destroyed the stack restored by the latest snapshot")
	}
	if got := shutdownCount.Load(); got != 0 {
		t.Fatalf("in-flight stale removal shut down the current stack %d times, want 0", got)
	}
}

func TestOrgRouterHandleConfigChangeReconcilesLatestConfigAfterCreateBecomesStaleInFlight(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	key := configstore.OrgUserKey{OrgID: "analytics", Username: "reader"}
	v1 := &configstore.OrgConfig{
		Name:       "analytics",
		MaxWorkers: 2,
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			Image: "posthog/duckgres:v1",
		},
	}
	v2 := &configstore.OrgConfig{
		Name:       "analytics",
		MaxWorkers: 7,
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			Image: "posthog/duckgres:v2",
		},
	}
	userAccess := map[configstore.OrgUserKey]configstore.OrgUserAccessConfig{
		key: {Mode: configstore.OrgUserAccessModeProjectReader},
	}
	beforeCreate := &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{}}
	readyV1 := &configstore.Snapshot{
		Orgs:          map[string]*configstore.OrgConfig{"analytics": v1},
		OrgUserAccess: userAccess,
	}
	readyV2 := &configstore.Snapshot{
		Orgs:          map[string]*configstore.OrgConfig{"analytics": v2},
		OrgUserAccess: userAccess,
	}
	store := newTestConfigStoreWithSnapshot(readyV1)
	router := &OrgRouter{
		orgs:        make(map[string]*OrgStack),
		configStore: store,
		sharedPool:  sharedPool,
	}
	t.Cleanup(router.ShutdownAll)

	entered := make(chan struct{})
	release := make(chan struct{})
	releaseBarrier := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(releaseBarrier)
	router.setProjectReaderChangeHandler(func(string, string) {
		close(entered)
		<-release
	})
	staleDone := make(chan struct{})
	go func() {
		router.HandleConfigChange(beforeCreate, readyV1)
		close(staleDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stale create callback did not reach the barrier")
	}

	setTestConfigStoreSnapshot(store, readyV2)
	latestDone := make(chan struct{})
	go func() {
		router.HandleConfigChange(readyV1, readyV2)
		close(latestDone)
	}()
	select {
	case <-latestDone:
	case <-time.After(time.Second):
		t.Fatal("latest ready callback did not finish")
	}
	latestStack, ok := router.StackForOrg("analytics")
	if !ok || latestStack.Config != v2 {
		t.Fatal("latest ready callback did not publish the v2 stack")
	}

	releaseBarrier()
	select {
	case <-staleDone:
	case <-time.After(time.Second):
		t.Fatal("stale create callback did not finish")
	}

	stack, ok := router.StackForOrg("analytics")
	if !ok {
		t.Fatal("latest ready snapshot did not result in an org stack")
	}
	if stack != latestStack {
		t.Fatal("stale callback replaced the generation published by the latest callback")
	}
	if stack.Config != v2 {
		t.Fatal("in-flight stale create did not publish the latest org config")
	}
	pool, ok := stack.Pool.(*OrgReservedPool)
	if !ok {
		t.Fatalf("org stack pool type = %T, want *OrgReservedPool", stack.Pool)
	}
	if pool.maxWorkers != v2.MaxWorkers {
		t.Fatalf("org stack max workers = %d, want %d", pool.maxWorkers, v2.MaxWorkers)
	}
	if pool.image != v2.Warehouse.Image {
		t.Fatalf("org stack image = %q, want %q", pool.image, v2.Warehouse.Image)
	}
}

func TestLatestOrgStackStateMatchesWarehouseLifecycle(t *testing.T) {
	orgWithState := func(state configstore.ManagedWarehouseProvisioningState) *configstore.OrgConfig {
		return &configstore.OrgConfig{
			Name:      "analytics",
			Warehouse: &configstore.ManagedWarehouseConfig{State: state},
		}
	}
	tests := []struct {
		name     string
		snapshot *configstore.Snapshot
		want     orgStackReconcileState
	}{
		{name: "snapshot unavailable", want: orgStackPreserve},
		{name: "org removed", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{}}, want: orgStackEnsureAbsent},
		{name: "legacy org", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": {Name: "analytics"}}}, want: orgStackEnsurePresent},
		{name: "ready", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStateReady)}}, want: orgStackEnsurePresent},
		{name: "deleting", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStateDeleting)}}, want: orgStackEnsureAbsent},
		{name: "deleted", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStateDeleted)}}, want: orgStackEnsureAbsent},
		{name: "pending", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStatePending)}}, want: orgStackPreserve},
		{name: "provisioning", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStateProvisioning)}}, want: orgStackPreserve},
		{name: "failed", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStateFailed)}}, want: orgStackPreserve},
		{name: "resharding", snapshot: &configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": orgWithState(configstore.ManagedWarehouseStateResharding)}}, want: orgStackPreserve},
	}

	router := &OrgRouter{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := router.latestOrgStackState("analytics", tt.snapshot)
			if got != tt.want {
				t.Fatalf("latestOrgStackState = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChangedProjectReaderUsersDetectsNamespaceAndCredentialChanges(t *testing.T) {
	teamID := int64(2)
	key := configstore.OrgUserKey{OrgID: "org-a", Username: "posthog_team_2"}
	old := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"org-a": {Teams: []configstore.OrgTeamConfig{{TeamID: teamID, SchemaName: "team_2"}}},
		},
		OrgUserAccess: map[configstore.OrgUserKey]configstore.OrgUserAccessConfig{
			key: {Mode: configstore.OrgUserAccessModeProjectReader, TeamID: &teamID},
		},
		OrgUserPassword: map[configstore.OrgUserKey]string{key: "old-hash"},
		OrgUserDisabled: map[configstore.OrgUserKey]bool{},
	}
	updatedNamespace := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"org-a": {Teams: []configstore.OrgTeamConfig{{TeamID: teamID, SchemaName: "renamed"}}},
		},
		OrgUserAccess:   old.OrgUserAccess,
		OrgUserPassword: old.OrgUserPassword,
		OrgUserDisabled: old.OrgUserDisabled,
	}
	if _, ok := changedProjectReaderUsers(old, updatedNamespace)[key]; !ok {
		t.Fatal("namespace change did not invalidate project reader")
	}

	updatedPassword := *old
	updatedPassword.OrgUserPassword = map[configstore.OrgUserKey]string{key: "new-hash"}
	if _, ok := changedProjectReaderUsers(old, &updatedPassword)[key]; !ok {
		t.Fatal("credential change did not invalidate project reader")
	}
}

func TestOrgRouterHandleConfigChangeNotifiesFlightSessionRevocation(t *testing.T) {
	teamID := int64(42)
	key := configstore.OrgUserKey{OrgID: "analytics", Username: "reader"}
	old := &configstore.Snapshot{
		Orgs:            map[string]*configstore.OrgConfig{"analytics": {Teams: []configstore.OrgTeamConfig{{TeamID: teamID, Enabled: true}}}},
		OrgUserPassword: map[configstore.OrgUserKey]string{key: "old"},
		OrgUserDisabled: map[configstore.OrgUserKey]bool{},
		OrgUserAccess: map[configstore.OrgUserKey]configstore.OrgUserAccessConfig{
			key: {Mode: configstore.OrgUserAccessModeProjectReader, TeamID: &teamID},
		},
	}
	updated := *old
	updated.OrgUserPassword = map[configstore.OrgUserKey]string{key: "new"}

	var revokedOrg, revokedUser string
	router := &OrgRouter{orgs: map[string]*OrgStack{
		"analytics": {Config: old.Orgs["analytics"]},
	}}
	router.setProjectReaderChangeHandler(func(orgID, username string) {
		revokedOrg, revokedUser = orgID, username
	})
	router.HandleConfigChange(old, &updated)

	if revokedOrg != "analytics" || revokedUser != "reader" {
		t.Fatalf("revoked user = %s/%s, want analytics/reader", revokedOrg, revokedUser)
	}
}

func TestOrgRouterHandleConfigChangeRefreshesRuntimeOnlyUpdates(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	pool := NewOrgReservedPool(sharedPool, "analytics", 2, sharedPool.workerImage, nil)

	oldTC := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "old-metadata.internal",
			},
		},
	}
	newTC := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "new-metadata.internal",
			},
		},
	}

	tr := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {
				Config: oldTC,
				Pool:   pool,
			},
		},
		baseCfg:   K8sWorkerPoolConfig{},
		globalCfg: ControlPlaneConfig{},
	}

	tr.HandleConfigChange(
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": oldTC}},
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": newTC}},
	)

	if got := tr.orgs["analytics"].Config.Warehouse.MetadataStore.Endpoint; got != "new-metadata.internal" {
		t.Fatalf("expected runtime-only update to refresh stack config, got %q", got)
	}
}

func TestOrgRouterHandleConfigChangeRefreshesOrgWorkerImage(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	pool := NewOrgReservedPool(sharedPool, "analytics", 2, "posthog/duckgres:v1.0.0", nil)

	oldTC := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			Image: "posthog/duckgres:v1.0.0",
		},
	}
	newTC := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			Image: "posthog/duckgres:v1.1.0",
		},
	}

	tr := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {
				Config: oldTC,
				Pool:   pool,
			},
		},
		baseCfg: K8sWorkerPoolConfig{WorkerImage: "posthog/duckgres:v1.0.0"},
	}

	tr.HandleConfigChange(
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": oldTC}},
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": newTC}},
	)

	if got := pool.image; got != "posthog/duckgres:v1.1.0" {
		t.Fatalf("expected org reserved pool image to refresh, got %q", got)
	}
}

func TestOrgRouterHandleConfigChangeRefreshesDefaultWorkerMinHotIdle(t *testing.T) {
	pool := &recordingFloorConfigPool{}

	oldTC := &configstore.OrgConfig{Name: "analytics", DefaultWorkerMinHotIdle: 1}
	newTC := &configstore.OrgConfig{Name: "analytics", DefaultWorkerMinHotIdle: 3}

	tr := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {
				Config: oldTC,
				Pool:   pool,
			},
		},
		baseCfg: K8sWorkerPoolConfig{},
	}

	tr.HandleConfigChange(
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": oldTC}},
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": newTC}},
	)

	if pool.floorUpdates != 0 {
		t.Fatalf("retention-only floor should not update pool state, got %d calls", pool.floorUpdates)
	}
}

type recordingFloorConfigPool struct {
	floorUpdates int
}

func (p *recordingFloorConfigPool) AcquireWorker(ctx context.Context, profile *WorkerProfile) (*ManagedWorker, error) {
	return nil, nil
}

func (p *recordingFloorConfigPool) ReleaseWorker(id int) {}

func (p *recordingFloorConfigPool) RetireWorker(id int) {}

func (p *recordingFloorConfigPool) RetireWorkerIfNoSessions(id int) bool { return false }

func (p *recordingFloorConfigPool) Worker(id int) (*ManagedWorker, bool) { return nil, false }

func (p *recordingFloorConfigPool) SpawnMinWorkers(count int) error { return nil }

func (p *recordingFloorConfigPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
}

func (p *recordingFloorConfigPool) SetMaxWorkers(n int) {}

func (p *recordingFloorConfigPool) ShutdownAll() {}

func (p *recordingFloorConfigPool) SetDefaultWorkerMinHotIdle(n int) {
	p.floorUpdates++
}

func TestOrgRouterCreateOrgStackActivatesUsingLatestSnapshotThroughSharedWorkerActivator(t *testing.T) {
	sharedPool, cs := newTestK8sPool(t, 10)
	sharedPool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	sharedPool.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		sharedPool.mu.Lock()
		sharedPool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		sharedPool.mu.Unlock()
		return nil
	}
	sharedPool.workers[1] = &ManagedWorker{ID: 1, done: make(chan struct{})}

	for name, value := range map[string]string{
		"analytics-metadata-old": "old-password",
		"analytics-metadata-new": "new-password",
	} {
		_, err := cs.CoreV1().Secrets("tenant-a").Create(context.Background(), newStringSecret("tenant-a", name, "dsn", value), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create secret %s: %v", name, err)
		}
	}

	oldOrg := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
			},
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "old-metadata.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-old",
				Key:       "dsn",
			},
		},
	}
	newOrg := &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"bob": "ignored",
		},
		Warehouse: &configstore.ManagedWarehouseConfig{
			State: configstore.ManagedWarehouseStateReady,
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
			},
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "new-metadata.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-new",
				Key:       "dsn",
			},
		},
	}

	store := newTestConfigStoreWithSnapshot(&configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"analytics": oldOrg,
		},
	})
	reclaimer := &recordingAdmissionReclaimer{}

	tr := &OrgRouter{
		orgs:               make(map[string]*OrgStack),
		configStore:        store,
		baseCfg:            K8sWorkerPoolConfig{},
		sharedPool:         sharedPool,
		globalCfg:          ControlPlaneConfig{},
		admissionReclaimer: reclaimer,
	}

	var captured TenantActivationPayload
	sharedPool.activateTenantFunc = func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
		captured = payload
		return nil
	}

	stack, err := tr.createOrgStack(oldOrg)
	if err != nil {
		t.Fatalf("createOrgStack: %v", err)
	}
	limiter, ok := stack.Sessions.limiter.(*runtimeOrgConnectionLimiter)
	if !ok {
		t.Fatalf("org stack limiter type = %T, want *runtimeOrgConnectionLimiter", stack.Sessions.limiter)
	}
	if limiter.reclaimer != reclaimer {
		t.Fatalf("org stack limiter reclaimer = %p, want shared router reclaimer %p", limiter.reclaimer, reclaimer)
	}

	setTestConfigStoreSnapshot(store, &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"analytics": newOrg,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := stack.Pool.AcquireWorker(ctx, nil)
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", got)
	}
	if captured.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", captured.OrgID)
	}
	if len(captured.Usernames) != 1 || captured.Usernames[0] != "bob" {
		t.Fatalf("expected latest usernames from router snapshot, got %#v", captured.Usernames)
	}
	if got := captured.DuckLake.MetadataStore; got != "postgres:host=new-metadata.internal port=5432 user=ducklake_user password=new-password dbname=ducklake_metadata" {
		t.Fatalf("expected latest warehouse runtime from router snapshot, got %q", got)
	}
}

func newStringSecret(namespace, name, key, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		StringData: map[string]string{
			key: value,
		},
	}
}

func newTestConfigStoreWithSnapshot(snapshot *configstore.Snapshot) *configstore.ConfigStore {
	store := &configstore.ConfigStore{}
	setTestConfigStoreSnapshot(store, snapshot)
	return store
}

func setTestConfigStoreSnapshot(store *configstore.ConfigStore, snapshot *configstore.Snapshot) {
	field := reflect.ValueOf(store).Elem().FieldByName("snapshot")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(snapshot))
}

func TestWorkerImageForOrg(t *testing.T) {
	fallback := "posthog/duckgres:v1.0.0"

	tests := []struct {
		name     string
		org      *configstore.OrgConfig
		expected string
	}{
		{
			name:     "nil org returns fallback",
			org:      nil,
			expected: fallback,
		},
		{
			name: "nil warehouse returns fallback",
			org: &configstore.OrgConfig{
				Name:      "analytics",
				Warehouse: nil,
			},
			expected: fallback,
		},
		{
			name: "empty image returns fallback",
			org: &configstore.OrgConfig{
				Name: "analytics",
				Warehouse: &configstore.ManagedWarehouseConfig{
					Image: "",
				},
			},
			expected: fallback,
		},
		{
			name: "custom image is returned",
			org: &configstore.OrgConfig{
				Name: "analytics",
				Warehouse: &configstore.ManagedWarehouseConfig{
					Image: "posthog/duckgres:v1.2.0-canary",
				},
			},
			expected: "posthog/duckgres:v1.2.0-canary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workerImageForOrg(tt.org, fallback); got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
