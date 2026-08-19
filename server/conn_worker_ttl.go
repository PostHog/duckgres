package server

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WorkerTTLGUCName is the duckgres-namespaced session GUC controlling how long
// the session's worker stays hot-idle (warm, reusable) after its last session
// ends, on the remote/k8s backend:
//
//	SET duckgres.worker_ttl = '20m'
//
// It is the mid-session form of the `-c duckgres.worker_ttl=...` startup
// option (controlplane/worker_profile.go), for clients that cannot set startup
// options. Used as the SHOW result column label.
const WorkerTTLGUCName = "duckgres.worker_ttl"

// workerTTLApplyTimeout bounds the control-plane apply hook. The apply is an
// in-memory pool mutation (no worker RPC), so this is generous.
const workerTTLApplyTimeout = 5 * time.Second

// defaultWorkerTTLFallback is what SHOW reports when nothing better is known
// (standalone / process backend with no override): the control plane's
// built-in hot-idle TTL default (defaultWorkerTTL, 1m), mirrored here because
// the import direction is controlplane -> server.
const defaultWorkerTTLFallback = time.Minute

// WorkerTTLControl is the optional control-plane capability behind the
// `duckgres.worker_ttl` session GUC, installed on remote/k8s connections. It
// is how the connection layer reaches the bound worker's pool-side profile —
// the hot-idle TTL lives in the control plane's worker pool, not in the
// worker process, so unlike duckgres.s3_cache this is NOT an executor
// capability. Connections without it (standalone, process backend) get
// session-state-only SET/SHOW, which is correct because those deployments
// have no hot-idle worker TTL to override.
type WorkerTTLControl struct {
	// Baseline is the TTL resolved at connect time (startup GUC > org default
	// > deployment default > built-in 1m). RESET restores it on the worker,
	// and SHOW falls back to it when no worker is bound yet.
	Baseline time.Duration

	// Apply overrides the bound worker's hot-idle TTL, returning the value
	// actually applied (the control plane may clamp to WorkerMaxTTL). A
	// returned *transform.CodedError preserves its SQLSTATE to the client;
	// any other error surfaces as XX000.
	Apply func(ctx context.Context, ttl time.Duration) (applied time.Duration, err error)

	// Current reports the TTL the bound worker would park with NOW (ok=false
	// when no worker is bound — a lazily activated connection before its
	// first engine statement). It beats Baseline for SHOW because a reused
	// hot-idle worker can carry a previous request's TTL.
	Current func() (ttl time.Duration, ok bool)
}

// effectiveWorkerTTL resolves the value SHOW reports: the session override
// wins, then the bound worker's current TTL, then the connect-time baseline,
// then the built-in default.
func (c *clientConn) effectiveWorkerTTL() time.Duration {
	if c.workerTTLOverride != nil {
		return *c.workerTTLOverride
	}
	if c.workerTTLCtl != nil {
		if c.workerTTLCtl.Current != nil {
			if cur, ok := c.workerTTLCtl.Current(); ok {
				return cur
			}
		}
		if c.workerTTLCtl.Baseline > 0 {
			return c.workerTTLCtl.Baseline
		}
	}
	return defaultWorkerTTLFallback
}

// workerTTLValue is the SHOW-facing rendering of the session state.
func (c *clientConn) workerTTLValue() string {
	return c.effectiveWorkerTTL().String()
}

// applyWorkerTTLSetting applies an already-normalized `duckgres.worker_ttl`
// value (a canonical Go duration, or "" = reset to the connect-time baseline)
// to the session. When the effective state changes and the control plane
// installed a WorkerTTLControl, the apply hook runs FIRST and the session
// state is only updated on success — a SET that failed to take effect on the
// worker must error, not leave SHOW claiming a TTL the worker won't park
// with. Callers pass validated values only (transform.NormalizeWorkerTTL,
// rejecting anything else with 22023 before this is reached).
func (c *clientConn) applyWorkerTTLSetting(value string) error {
	var override *time.Duration
	if value != "" {
		d, err := time.ParseDuration(value)
		if err != nil {
			// Unreachable: every SET path validates via NormalizeWorkerTTL
			// first. Defensive so a future caller cannot store unparseable
			// state.
			return err
		}
		override = &d
	}
	// No-op when the effective state does not change (a redundant SET must
	// not re-invoke the control plane, mirroring applyS3CacheSetting).
	if (override == nil) == (c.workerTTLOverride == nil) &&
		(override == nil || *override == *c.workerTTLOverride) {
		return nil
	}
	if c.workerTTLCtl != nil && c.workerTTLCtl.Apply != nil {
		target := c.workerTTLCtl.Baseline
		if override != nil {
			target = *override
		}
		c.ensureConnectionContext()
		ctx, cancel := context.WithTimeout(c.ctx, workerTTLApplyTimeout)
		defer cancel()
		applied, err := c.workerTTLCtl.Apply(ctx, target)
		if err != nil {
			return fmt.Errorf("failed to apply %s: %w", WorkerTTLGUCName, err)
		}
		// Store the value the worker ACTUALLY got (the hook may have clamped
		// it), so SHOW never reports a TTL the worker won't park with.
		if override != nil {
			*override = applied
		}
		c.logger().Info("Set duckgres.worker_ttl.", "ttl", applied.String())
	}
	c.workerTTLOverride = override
	return nil
}

// reapplyWorkerTTLAfterWorkerSwitch re-asserts this session's TTL override on
// a freshly acquired worker after a tier escalation. The override is pool-side
// per-worker state, and the escalated worker's profile carries the TTL
// resolved at connect time — without the re-apply the connection's warm
// retention would silently revert the moment it escalated. On failure the
// caller resets the session override (the new worker parks with its own
// baseline TTL, so session state must match). No-op when the session never
// overrode the TTL.
func (c *clientConn) reapplyWorkerTTLAfterWorkerSwitch(ctx context.Context) error {
	if c.workerTTLOverride == nil {
		return nil
	}
	if c.workerTTLCtl == nil || c.workerTTLCtl.Apply == nil {
		return nil
	}
	applyCtx, cancel := context.WithTimeout(ctx, workerTTLApplyTimeout)
	defer cancel()
	applied, err := c.workerTTLCtl.Apply(applyCtx, *c.workerTTLOverride)
	if err != nil {
		return fmt.Errorf("failed to re-apply %s on the new worker: %w", WorkerTTLGUCName, err)
	}
	*c.workerTTLOverride = applied
	c.logger().Info("Re-applied duckgres.worker_ttl after worker switch.", "ttl", applied.String())
	return nil
}

// workerTTLApplyErrorSQLState picks the client-facing SQLSTATE for a failed
// SET duckgres.worker_ttl: a coded rejection from the control plane (the
// AllowClientWorkerProfile gate's 22023) keeps its code; anything else is an
// internal apply failure (XX000).
func workerTTLApplyErrorSQLState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return "XX000"
}
