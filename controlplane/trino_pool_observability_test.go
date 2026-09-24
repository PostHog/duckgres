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
	observation.snapshot(nil, time.Unix(1000, 0))
	harness.operator.store = trinoPoolFailingListStore{harness.store}
	if err := harness.operator.reconcileOnce(ctx); err == nil {
		t.Fatal("expected inventory error")
	}
	if got := gaugeVecLabelValue(t, metrics.snapshotAt, "pool-a"); got != 1000 {
		t.Fatalf("failed read refreshed snapshot: %v", got)
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
	old.snapshot(instances, now)
	other.snapshot(nil, now)
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
	next.snapshot(nil, now.Add(time.Minute))
	old.snapshot(instances, now.Add(time.Hour))
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

func TestTrinoPoolCancellationRemovesGaugesBeforeRunReturns(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newTrinoPoolMetrics(registry)
	ctx, cancel := context.WithCancel(context.Background())
	ctx, finish := metrics.beginTerm(ctx, "pool-a")
	defer finish()
	poolObservation(ctx).snapshot(nil, time.Now())
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
	poolObservation(ctx).snapshot(nil, time.Now())
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 {
		t.Fatal("canceled term recreated its gauges")
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
	observation.snapshot(nil, now)
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
