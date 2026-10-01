//go:build kubernetes

package controlplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	"github.com/prometheus/client_golang/prometheus"
)

func TestTrinoRetirementLogRequiresDurableTransition(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	instance := harness.placeInstance(t, harness.store.order[0], trinopool.PhaseRetiring, "RETIRING")
	harness.kube.absent = true
	harness.store.failAdvance = true
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	_, err := harness.operator.deleteResources(context.Background(), *instance)
	if err == nil {
		t.Fatal("expected the durable transition to fail")
	}
	if strings.Contains(logs.String(), "Trino pool instance retired.") || strings.Contains(logs.String(), `"phase":"RETIRED"`) {
		t.Fatal("reported retirement before its durable transition succeeded")
	}
}

type trinoPoolFailingListStore struct{ *fakePoolStore }

func (s trinoPoolFailingListStore) ListTrinoPoolInstances(context.Context, string) ([]configstore.TrinoPoolInstance, error) {
	return nil, errors.New("inventory unavailable")
}

func TestTrinoPoolSnapshotFreshnessTracksReadNotLaterEffects(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	instance := harness.placeInstance(t, harness.store.order[0], trinopool.PhaseDraining, "DRAINING")
	instance.PhaseChangedAt = time.Now().Add(-time.Minute)
	metrics := newTrinoPoolMetrics(prometheus.NewRegistry())
	observation := metrics.begin("pool-a")
	defer observation.end()
	ctx := context.WithValue(context.Background(), trinoPoolObservationKey{}, observation)
	previousInventory := []configstore.TrinoPoolInstance{{InstanceID: "previous-member", Phase: "SERVING", BlueprintSnapshot: `{"namespace":"trino-compute"}`, CoordinatorDeploymentName: "previous-coordinator", WorkerDeploymentName: "previous-worker"}}
	observation.snapshot(previousInventory, "trino-compute", 2, time.Unix(1000, 0))
	harness.operator.store = trinoPoolFailingListStore{harness.store}
	if err := harness.operator.reconcileOnce(ctx); err == nil {
		t.Fatal("expected inventory error")
	}
	if got := gaugeVecLabelValue(t, metrics.snapshotAt, "pool-a"); got != 1000 {
		t.Fatalf("failed read refreshed snapshot: %v", got)
	}
	if got := gaugeVecLabelValue(t, metrics.servingInstances, "pool-a", "previous-member", "trino-compute", "previous-coordinator", "previous-worker"); got != 1 {
		t.Fatalf("failed read changed serving inventory: %v", got)
	}
	harness.operator.store = harness.store
	harness.store.failAdvance = true
	if err := harness.operator.reconcileOnce(ctx); err == nil {
		t.Fatal("expected phase transition error")
	}
	if got := gaugeVecLabelValue(t, metrics.snapshotAt, "pool-a"); got <= 1000 {
		t.Fatalf("successful read did not refresh snapshot: %v", got)
	}
	if got := gaugeVecLabelValue(t, metrics.members, "pool-a", "DRAINING"); got != 1 {
		t.Fatalf("snapshot lost blocked member: %v", got)
	}
}

func TestTrinoPoolRetirementWaitAndConfirmedTransitions(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	instance := harness.placeInstance(t, harness.store.order[0], trinopool.PhaseRetiring, "RETIRING")
	metrics := newTrinoPoolMetrics(prometheus.NewRegistry())
	observation := metrics.begin("pool-a")
	defer observation.end()
	ctx := context.WithValue(context.Background(), trinoPoolObservationKey{}, observation)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	progressed, err := harness.operator.deleteResources(ctx, *instance)
	if err != nil || progressed {
		t.Fatalf("deletion wait = %v, %v", progressed, err)
	}
	if !strings.Contains(logs.String(), `"reason":"kubernetes_resources"`) {
		t.Fatalf("missing deletion wait: %s", logs.String())
	}
	if strings.Contains(logs.String(), `"active_queries"`) {
		t.Fatal("deletion wait invents an obligation observation")
	}
	harness.kube.absent = true
	_, err = harness.operator.deleteResources(ctx, *instance)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), `"phase":"RETIRED"`) {
		t.Fatalf("missing confirmed retirement: %s", logs.String())
	}
}

func TestTrinoPoolDrainTransitionsLogOnlyCommittedState(t *testing.T) {
	for _, tc := range []struct {
		name         string
		phase        trinopool.Phase
		gatewayPhase string
		apply        func(*operatorHarness, configstore.TrinoPoolInstance) error
	}{
		{"drain", trinopool.PhaseServing, "ACTIVE", func(h *operatorHarness, i configstore.TrinoPoolInstance) error {
			return h.operator.beginDrain(context.Background(), trinopool.Plan{InstanceID: i.InstanceID})
		}},
		{"seal", trinopool.PhaseDraining, "DRAINING", func(h *operatorHarness, i configstore.TrinoPoolInstance) error {
			_, err := h.operator.sealWhenDrained(context.Background(), i)
			return err
		}},
		{"retire", trinopool.PhaseSealed, "SEALED", func(h *operatorHarness, i configstore.TrinoPoolInstance) error {
			return h.operator.claimRetirement(context.Background(), i)
		}},
	} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failure=%v", tc.name, fail), func(t *testing.T) {
				h := newOperatorHarness(t)
				h.tick(t, 20)
				i := h.placeInstance(t, h.store.order[0], tc.phase, tc.gatewayPhase)
				h.store.failAdvance = fail
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previous) })
				err := tc.apply(h, *i)
				if (err != nil) != fail {
					t.Fatalf("transition error = %v, failure expected %v", err, fail)
				}
				logged := strings.Contains(logs.String(), "Trino pool member phase advanced.")
				if logged == fail {
					t.Fatalf("committed-state log = %v, failure = %v: %s", logged, fail, logs.String())
				}
			})
		}
	}
}

func TestTrinoPoolObservationSnapshotAndOwnership(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newTrinoPoolMetrics(registry)
	old := metrics.begin("pool-a")
	other := metrics.begin("pool-b")
	now := time.Unix(1000, 0)
	instances := []configstore.TrinoPoolInstance{
		{InstanceID: "member-a", Phase: "DRAINING", PhaseChangedAt: now.Add(-time.Minute)},
		{InstanceID: "member-b", Phase: "DRAINING", PhaseChangedAt: now.Add(time.Minute)},
		{InstanceID: "member-c", Phase: "SERVING"},
		{InstanceID: "member-d", Phase: "future-value"},
	}
	old.snapshot(instances, "trino-compute", 2, now)
	other.snapshot(nil, "trino-compute", 2, now)
	if got := gaugeVecLabelValue(t, metrics.members, "pool-a", "DRAINING"); got != 2 {
		t.Fatalf("draining = %v", got)
	}
	if got := gaugeVecLabelValue(t, metrics.oldestDrain, "pool-a"); got != 60 {
		t.Fatalf("oldest drain = %v", got)
	}
	if got := gaugeVecLabelValue(t, metrics.members, "pool-a", "UNKNOWN"); got != 1 {
		t.Fatalf("unknown phase = %v", got)
	}
	next := metrics.begin("pool-a")
	next.snapshot(nil, "trino-compute", 2, now.Add(time.Minute))
	old.snapshot(instances, "trino-compute", 2, now.Add(time.Hour))
	old.end()
	if got := gaugeVecLabelValue(t, metrics.snapshotAt, "pool-a"); got != 1060 {
		t.Fatalf("stale term changed current snapshot: %v", got)
	}
	next.end()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "pool" && label.GetValue() == "pool-a" {
					t.Fatal("ended term retained a gauge")
				}
			}
		}
	}
	if got := gaugeVecLabelValue(t, metrics.snapshotAt, "pool-b"); got != 1000 {
		t.Fatalf("cleanup affected another pool: %v", got)
	}
	other.end()
}

func TestTrinoPoolServingInventoryMetricsFollowSnapshotAndTerm(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newTrinoPoolMetrics(registry)
	old := metrics.begin("pool-a")
	now := time.Unix(1000, 0)
	instances := []configstore.TrinoPoolInstance{
		{InstanceID: "member-a", Phase: "SERVING", BlueprintSnapshot: `{"namespace":"pinned-compute"}`, CoordinatorDeploymentName: "member-a-coordinator", WorkerDeploymentName: "member-a-worker"},
		{InstanceID: "member-b", Phase: "DRAINING", CoordinatorDeploymentName: "member-b-coordinator", WorkerDeploymentName: "member-b-worker"},
		{InstanceID: "member-c", Phase: "PREPARING"},
	}
	old.snapshot(instances, "trino-compute", 3, now)
	if got := gaugeVecLabelValue(t, metrics.minServing, "pool-a", "trino-compute"); got != 3 {
		t.Fatalf("configured serving floor = %v", got)
	}
	if got := gaugeVecLabelValue(t, metrics.servingInstances, "pool-a", "member-a", "pinned-compute", "member-a-coordinator", "member-a-worker"); got != 1 {
		t.Fatalf("serving deployment identity = %v", got)
	}
	assertInventory := func(want int) {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, family := range families {
			if family.GetName() == "duckgres_trino_pool_serving_instance_info" {
				count = len(family.Metric)
				for _, metric := range family.Metric {
					for _, label := range metric.Label {
						if label.GetName() == "namespace" || label.GetName() == "instance" {
							t.Fatalf("workload metric conflicts with scrape identity: %s", label.GetName())
						}
					}
				}
			}
		}
		if count != want {
			t.Fatalf("serving inventory series = %d, want %d", count, want)
		}
	}
	assertInventory(1)
	instances[0].Phase = "DRAINING"
	old.snapshot(instances, "trino-compute", 2, now.Add(time.Minute))
	assertInventory(0)
	if got := gaugeVecLabelValue(t, metrics.minServing, "pool-a", "trino-compute"); got != 2 {
		t.Fatalf("zero-serving snapshot lost configured floor: %v", got)
	}
	next := metrics.begin("pool-a")
	next.snapshot(nil, "trino-compute", 4, now.Add(2*time.Minute))
	instances[0].Phase = "SERVING"
	old.snapshot(instances, "old-namespace", 9, now.Add(time.Hour))
	old.end()
	assertInventory(0)
	if got := gaugeVecLabelValue(t, metrics.minServing, "pool-a", "trino-compute"); got != 4 {
		t.Fatalf("stale term changed configured floor: %v", got)
	}
	next.snapshot(instances, "trino-compute", 4, now.Add(3*time.Minute))
	assertInventory(1)
	next.end()
	families, err := registry.Gather()
	if err != nil || len(families) != 0 {
		t.Fatalf("ended term retained metrics: %v, %v", families, err)
	}
}

func TestTrinoPoolCancellationRemovesGaugesBeforeRunReturns(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newTrinoPoolMetrics(registry)
	ctx, cancel := context.WithCancel(context.Background())
	ctx, finish := metrics.beginTerm(ctx, "pool-a")
	defer finish()
	serving := []configstore.TrinoPoolInstance{{InstanceID: "member-a", Phase: "SERVING", CoordinatorDeploymentName: "member-a-coordinator", WorkerDeploymentName: "member-a-worker"}}
	poolObservation(ctx).snapshot(serving, "trino-compute", 2, time.Now())
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		metrics.mu.Lock()
		owner := metrics.owners["pool-a"]
		metrics.mu.Unlock()
		if owner == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled term still owns gauges before Run cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 {
		t.Fatal("canceled term still exports gauges")
	}
	poolObservation(ctx).snapshot(serving, "trino-compute", 2, time.Now())
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 {
		t.Fatal("canceled term recreated its gauges")
	}
}

func TestTrinoPoolConfiguredMetricsOutliveAuthorityButNotConfiguration(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newTrinoPoolMetrics(registry)
	operators := []*trinoPoolOperator{
		{config: trinoPoolConfig{PublicID: "pool-a", Namespace: "compute-a"}, operatorEnabled: true},
		{config: trinoPoolConfig{PublicID: "pool-b", Namespace: "compute-b"}, operatorEnabled: true},
		{config: trinoPoolConfig{PublicID: "paused", Namespace: "compute-c"}},
	}
	cleanup := metrics.configurePools(operators)
	a, b := metrics.begin("pool-a"), metrics.begin("pool-b")
	a.snapshot(nil, "compute-a", 3, time.Unix(1000, 0))
	b.snapshot(nil, "compute-b", 2, time.Unix(1000, 0))
	a.end()
	for _, pool := range []struct{ id, namespace string }{{"pool-a", "compute-a"}, {"pool-b", "compute-b"}} {
		if got := gaugeVecLabelValue(t, metrics.configured, pool.id, pool.namespace); got != 1 {
			t.Fatalf("authority loss hid configured %s: %v", pool.id, got)
		}
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "duckgres_trino_pool_configured" && len(family.Metric) != 2 {
			t.Fatal("paused operator exported expected active pool")
		}
	}
	if got := gaugeVecLabelValue(t, metrics.snapshotAt, "pool-b"); got != 1000 {
		t.Fatalf("other pool lost its authority snapshot: %v", got)
	}
	b.end()
	nextCleanup := metrics.configurePools(operators[:1])
	cleanup()
	if got := gaugeVecLabelValue(t, metrics.configured, "pool-a", "compute-a"); got != 1 {
		t.Fatalf("previous configuration cleanup erased current configuration: %v", got)
	}
	nextCleanup()
	families, err = registry.Gather()
	if err != nil || len(families) != 0 {
		t.Fatalf("configuration shutdown retained metrics: %v, %v", families, err)
	}
}

func TestTrinoPoolDrainProgressIsBoundedAndPruned(t *testing.T) {
	metrics := newTrinoPoolMetrics(prometheus.NewRegistry())
	observation := metrics.begin("pool-a")
	defer observation.end()
	now := time.Unix(1000, 0)
	instance := configstore.TrinoPoolInstance{InstanceID: "member-a", Phase: "DRAINING", PhaseChangedAt: now.Add(-time.Hour)}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	state := trinogateway.Obligations{ActiveQueries: 1, PendingRequests: 2, OpenTransactions: 3}
	observation.waiting(logger, instance, 7, "gateway_obligations", state, now)
	state.ActiveQueries = 2
	observation.waiting(logger, instance, 7, "gateway_obligations", state, now.Add(time.Second))
	if got := strings.Count(logs.String(), "Trino pool member is waiting."); got != 1 {
		t.Fatalf("rapid changes emitted %d logs", got)
	}
	observation.waiting(logger, instance, 7, "gateway_obligations", state, now.Add(30*time.Second))
	observation.waiting(logger, instance, 7, "gateway_obligations", state, now.Add(4*time.Minute))
	if got := strings.Count(logs.String(), "Trino pool member is waiting."); got != 2 {
		t.Fatalf("unchanged poll emitted %d logs", got)
	}
	observation.waiting(logger, instance, 7, "gateway_obligations", state, now.Add(6*time.Minute))
	if got := strings.Count(logs.String(), "Trino pool member is waiting."); got != 3 {
		t.Fatalf("missing heartbeat: %d logs", got)
	}
	for _, want := range []string{`"pending_requests":2`, `"open_transactions":3`, `"active_queries":1`, `"phase_age_seconds":3600`, `"epoch":7`, `"ready_to_seal":false`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %s: %s", want, logs.String())
		}
	}
	observation.snapshot(nil, "trino-compute", 2, now)
	observation.waiting(logger, instance, 7, "gateway_obligations", state, now.Add(6*time.Minute+time.Second))
	if got := strings.Count(logs.String(), "Trino pool member is waiting."); got != 4 {
		t.Fatalf("departed member cache not pruned: %d", got)
	}
}

func TestTrinoPoolErrorReasonsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{nil, ""},
		{fmt.Errorf("wait: %w", errTrinoPoolBackoff), ""},
		{errors.Join(errTrinoPoolBackoff, errors.New("private arbitrary error")), "internal"},
		{errors.Join(errTrinoPoolBackoff, context.DeadlineExceeded), "timeout"},
		{errors.Join(errors.New("other"), configstore.ErrTrinoPoolConflict), "fenced"},
		{trinogateway.ErrServingFloor, "gateway_refused"},
		{trinogateway.ErrUnavailable, "gateway_unavailable"},
	} {
		if got := trinoPoolFailureReason(tc.err); got != tc.want {
			t.Errorf("reason = %q, want %q", got, tc.want)
		}
	}
	metrics := newTrinoPoolMetrics(prometheus.NewRegistry())
	observation := metrics.begin("pool-a")
	defer observation.end()
	observation.failure(errTrinoPoolBackoff)
	observation.failure(errors.Join(errTrinoPoolBackoff, context.DeadlineExceeded))
	if got := counterVecLabelValue(t, metrics.failures, "pool-a", "timeout"); got != 1 {
		t.Fatalf("mixed backoff failure counter = %v", got)
	}
}

func TestTrinoPoolBlockedPollLogsWithoutSealing(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	instance := harness.placeInstance(t, harness.store.order[0], trinopool.PhaseDraining, "DRAINING")
	harness.gateway.obligations[instance.InstanceID] = trinogateway.Obligations{ActiveQueries: 1}
	metrics := newTrinoPoolMetrics(prometheus.NewRegistry())
	observation := metrics.begin("pool-a")
	defer observation.end()
	ctx := context.WithValue(context.Background(), trinoPoolObservationKey{}, observation)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for range 3 {
		progressed, err := harness.operator.sealWhenDrained(ctx, *instance)
		if err != nil || progressed {
			t.Fatalf("blocked poll = %v, %v", progressed, err)
		}
	}
	if got := strings.Count(logs.String(), "Trino pool member is waiting."); got != 1 {
		t.Fatalf("blocked logs = %d: %s", got, logs.String())
	}
	if countCalls(harness.gateway.calls, "seal:") != 0 {
		t.Fatal("observability sealed an occupied member")
	}
}
