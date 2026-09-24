//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	trinoPoolProgressLogInterval = 30 * time.Second
	trinoPoolProgressHeartbeat   = 5 * time.Minute
)

var trinoPoolMetrics = newTrinoPoolMetrics(prometheus.DefaultRegisterer)

type trinoPoolObservationKey struct{}

type trinoPoolMetricSet struct {
	mu          sync.Mutex
	owners      map[string]*trinoPoolTelemetry
	members     *prometheus.GaugeVec
	oldestDrain *prometheus.GaugeVec
	snapshotAt  *prometheus.GaugeVec
	failures    *prometheus.CounterVec
}

func newTrinoPoolMetrics(reg prometheus.Registerer) *trinoPoolMetricSet {
	m := &trinoPoolMetricSet{
		owners: make(map[string]*trinoPoolTelemetry),
		members: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "duckgres_trino_pool_members", Help: "Durable pool instance rows by phase at the last successful snapshot.",
		}, []string{"pool", "phase"}),
		oldestDrain: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "duckgres_trino_pool_oldest_drain_seconds", Help: "Oldest durable DRAINING phase age at the last successful snapshot.",
		}, []string{"pool"}),
		snapshotAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "duckgres_trino_pool_snapshot_timestamp_seconds", Help: "Time of the last successful durable instance snapshot in this operator term.",
		}, []string{"pool"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "duckgres_trino_pool_reconcile_failures_total", Help: "Failed pool reconciliations by bounded reason; excludes pure backoff waits.",
		}, []string{"pool", "reason"}),
	}
	reg.MustRegister(m.members, m.oldestDrain, m.snapshotAt, m.failures)
	return m
}

type trinoPoolWaitState struct {
	reason      string
	phase       string
	obligations trinogateway.Obligations
}

type trinoPoolWaitLog struct {
	state trinoPoolWaitState
	at    time.Time
}

type trinoPoolTelemetry struct {
	metrics *trinoPoolMetricSet
	pool    string
	waits   map[string]trinoPoolWaitLog
}

func (m *trinoPoolMetricSet) beginTerm(ctx context.Context, pool string) (context.Context, func()) {
	observation := m.begin(pool)
	stop := context.AfterFunc(ctx, observation.end)
	return context.WithValue(ctx, trinoPoolObservationKey{}, observation), func() {
		stop()
		observation.end()
	}
}

// Each Run owns its observation token. An older Run cannot erase a newer term.
func (m *trinoPoolMetricSet) begin(pool string) *trinoPoolTelemetry {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clear(pool)
	o := &trinoPoolTelemetry{metrics: m, pool: pool, waits: make(map[string]trinoPoolWaitLog)}
	m.owners[pool] = o
	return o
}

func (m *trinoPoolMetricSet) clear(pool string) {
	m.members.DeletePartialMatch(prometheus.Labels{"pool": pool})
	m.oldestDrain.DeleteLabelValues(pool)
	m.snapshotAt.DeleteLabelValues(pool)
}

func (o *trinoPoolTelemetry) end() {
	o.metrics.mu.Lock()
	defer o.metrics.mu.Unlock()
	if o.metrics.owners[o.pool] != o {
		return
	}
	o.metrics.clear(o.pool)
	delete(o.metrics.owners, o.pool)
}

func poolObservation(ctx context.Context) *trinoPoolTelemetry {
	o, _ := ctx.Value(trinoPoolObservationKey{}).(*trinoPoolTelemetry)
	return o
}

// Snapshot data precedes this tick's effects. It is not Gateway obligation data.
func (o *trinoPoolTelemetry) snapshot(instances []configstore.TrinoPoolInstance, now time.Time) {
	if o == nil {
		return
	}
	o.metrics.mu.Lock()
	defer o.metrics.mu.Unlock()
	if o.metrics.owners[o.pool] != o {
		return
	}
	counts := map[string]int{
		"PENDING": 0, "CREATING": 0, "PREPARING": 0, "VALIDATING": 0,
		"ADMITTED": 0, "SERVING": 0, "DRAINING": 0, "SEALED": 0,
		"RETIRING": 0, "RETIRED": 0, "SUSPECT": 0, "LOST": 0,
		"FAILURE_RETIRED": 0, "FAILED_PREPARING": 0, "UNKNOWN": 0,
	}
	live := make(map[string]bool)
	var oldest float64
	for _, instance := range instances {
		phase := instance.Phase
		if _, known := counts[phase]; !known {
			phase = "UNKNOWN"
		}
		counts[phase]++
		if !trinopool.Phase(instance.Phase).Terminal() {
			live[instance.InstanceID] = true
		}
		if phase == "DRAINING" {
			oldest = max(oldest, trinoPoolPhaseAge(instance, now))
		}
	}
	for phase, count := range counts {
		o.metrics.members.WithLabelValues(o.pool, phase).Set(float64(count))
	}
	for id := range o.waits {
		if !live[id] {
			delete(o.waits, id)
		}
	}
	o.metrics.oldestDrain.WithLabelValues(o.pool).Set(oldest)
	o.metrics.snapshotAt.WithLabelValues(o.pool).Set(float64(now.Unix()))
}

func trinoPoolPhaseAge(instance configstore.TrinoPoolInstance, now time.Time) float64 {
	if instance.PhaseChangedAt.IsZero() {
		return 0
	}
	return max(0, now.Sub(instance.PhaseChangedAt).Seconds())
}

func (o *trinoPoolTelemetry) waiting(logger *slog.Logger, instance configstore.TrinoPoolInstance, epoch int64, reason string, obligations trinogateway.Obligations, now time.Time) {
	if o == nil {
		return
	}
	o.metrics.mu.Lock()
	defer o.metrics.mu.Unlock()
	if o.metrics.owners[o.pool] != o {
		return
	}
	state := trinoPoolWaitState{reason: reason, phase: instance.Phase, obligations: obligations}
	previous, seen := o.waits[instance.InstanceID]
	if seen && (now.Sub(previous.at) < trinoPoolProgressLogInterval ||
		(state == previous.state && now.Sub(previous.at) < trinoPoolProgressHeartbeat)) {
		return
	}
	o.waits[instance.InstanceID] = trinoPoolWaitLog{state: state, at: now}
	attrs := []any{
		"pool", o.pool, "instance", instance.InstanceID, "epoch", epoch,
		"phase", instance.Phase, "phase_age_seconds", trinoPoolPhaseAge(instance, now), "reason", reason,
	}
	if reason == "gateway_obligations" {
		attrs = append(attrs, "pending_requests", obligations.PendingRequests, "open_transactions", obligations.OpenTransactions,
			"active_queries", obligations.ActiveQueries, "ready_to_seal", obligations.ReadyToSeal, "drained", obligations.Drained)
	}
	logger.Info("Trino pool member is waiting.", attrs...)
}

func (o *trinoPoolTelemetry) failure(err error) {
	if o == nil {
		return
	}
	reason := trinoPoolFailureReason(err)
	if reason == "" {
		return
	}
	o.metrics.mu.Lock()
	defer o.metrics.mu.Unlock()
	if o.metrics.owners[o.pool] == o {
		o.metrics.failures.WithLabelValues(o.pool, reason).Inc()
	}
}

func trinoPoolFailureReason(err error) string {
	if err == nil || trinoPoolOnlyBackoff(err) {
		return ""
	}
	if errors.Is(err, configstore.ErrTrinoPoolConflict) || errors.Is(err, trinogateway.ErrStaleEpoch) {
		return "fenced"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, trinogateway.ErrUnavailable) {
		return "gateway_unavailable"
	}
	var gatewayError *trinogateway.Error
	if errors.As(err, &gatewayError) {
		return "gateway_refused"
	}
	if errors.Is(err, trinogateway.ErrServingFloor) {
		return "gateway_refused"
	}
	return "internal"
}

func trinoPoolOnlyBackoff(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !trinoPoolOnlyBackoff(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return trinoPoolOnlyBackoff(wrapped.Unwrap())
	}
	return err == errTrinoPoolBackoff
}

func (o *trinoPoolOperator) logDrainTransition(instance configstore.TrinoPoolInstance, phase string) {
	slog.Info("Trino pool member phase advanced.", "pool", o.config.PublicID,
		"instance", instance.InstanceID, "epoch", o.lease.Epoch,
		"from_phase", instance.Phase, "phase", phase,
		"previous_phase_age_seconds", trinoPoolPhaseAge(instance, time.Now()))
}
