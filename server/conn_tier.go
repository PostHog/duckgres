package server

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WorkerSwitcher swaps a connection's backing worker/session: the control
// plane destroys the current (stateless, exploratory) session and creates one
// on a normal-size worker, returning the new executor + worker identity.
type WorkerSwitcher func(ctx context.Context, reason string) (exec QueryExecutor, workerID int, workerPod string, err error)

const (
	escalateReasonState     = "state"
	escalateReasonOOM       = "oom"
	escalateReasonHeuristic = "heuristic"
)

var exploratoryEscalationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_exploratory_escalations_total",
	Help: "Connections escalated off the exploratory small worker, by reason (state|oom|heuristic).",
}, []string{"reason"})

// escalateWorker moves the connection from the exploratory small worker to a
// normal-size worker. Sticky: once pinned, later calls are no-ops. On failure
// the connection's previous session is already gone (the control-plane switcher
// destroys it before acquiring the target worker), so callers MUST treat a
// failed escalation as connection-fatal: surface the error to the client and
// terminate the connection. The query-path integration implements that
// termination.
func (c *clientConn) escalateWorker(ctx context.Context, reason string) error {
	if !c.onExploratoryWorker || c.workerSwitcher == nil {
		return nil
	}
	exec, workerID, workerPod, err := c.workerSwitcher(ctx, reason)
	if err != nil {
		return err
	}
	c.executor = exec
	c.workerID = workerID
	c.workerPod = workerPod
	c.onExploratoryWorker = false
	exploratoryEscalationsTotal.WithLabelValues(reason).Inc()
	c.logger().Info("Escalated connection off exploratory worker.", "reason", reason, "worker", workerID, "worker_pod", workerPod)
	// The `duckgres.s3_cache` bypass is worker-side state, and the new session
	// starts on the cache proxy — re-assert it or the connection silently
	// starts reading cached mid-flight. On failure the session state is reset
	// to match the transport the worker is actually in (SHOW must never lie)
	// and the statement fails, rather than a benchmark quietly going cached.
	if err := c.reapplyS3CacheAfterWorkerSwitch(ctx); err != nil {
		c.s3CacheOff = false
		return err
	}
	return nil
}
