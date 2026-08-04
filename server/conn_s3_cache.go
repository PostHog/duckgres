package server

import (
	"context"
	"fmt"
	"time"

	"github.com/posthog/duckgres/transpiler/transform"
)

// s3CacheGUCName is the fully-qualified name of the duckgres-namespaced
// session GUC controlling whether the session's DuckLake S3 traffic is served
// through the node-local cache proxy (remote/k8s workers). Used as the SHOW
// result column label.
const s3CacheGUCName = "duckgres.s3_cache"

// s3CacheApplyTimeout bounds the worker RPC that swaps the S3 secret
// transport. The swap is a single CREATE OR REPLACE SECRET on the worker's
// control connection, but it briefly takes the worker's DuckLake semaphore,
// so allow for a concurrent attach/refresh to finish first.
const s3CacheApplyTimeout = 30 * time.Second

// S3CacheControl is the optional session-executor capability behind the
// `duckgres.s3_cache` GUC: swapping the worker-side S3 secret between the
// cache-proxy transport (enabled, the default) and the org's native HTTPS
// transport (bypassed). flightclient.FlightExecutor implements it for
// remote/k8s workers. Executors that don't implement it (standalone,
// in-process) get session-state-only SET/SHOW, which is correct because those
// deployments never route S3 traffic through a cache proxy — there is nothing
// to bypass.
type S3CacheControl interface {
	SetS3CacheEnabled(ctx context.Context, enabled bool) error
}

// S3CacheEnabled reports the session's `duckgres.s3_cache` state. Defaults to
// true: the cache proxy serves all S3 traffic unless this session explicitly
// opted out.
func (c *clientConn) S3CacheEnabled() bool {
	return !c.s3CacheOff
}

// s3CacheValue is the SHOW-facing rendering of the session state.
func (c *clientConn) s3CacheValue() string {
	if c.s3CacheOff {
		return transform.S3CacheOff
	}
	return transform.S3CacheOn
}

// applyS3CacheSetting applies an already-normalized `duckgres.s3_cache` value
// ("on", "off", or "" = reset to the default "on") to the session. When the
// effective state changes and the session executor supports per-session S3
// cache control, the worker swap RPC runs FIRST and the session state is only
// updated on success — a SET that failed to take effect on the worker must
// error, not leave SHOW claiming a transport the worker isn't using. Callers
// pass validated values only (transform.NormalizeS3Cache, rejecting anything
// else with 22023 before this is reached).
func (c *clientConn) applyS3CacheSetting(value string) error {
	enabled := value != transform.S3CacheOff
	if enabled != c.S3CacheEnabled() {
		if ctrl, ok := c.executor.(S3CacheControl); ok {
			c.ensureConnectionContext()
			ctx, cancel := context.WithTimeout(c.ctx, s3CacheApplyTimeout)
			defer cancel()
			if err := ctrl.SetS3CacheEnabled(ctx, enabled); err != nil {
				return fmt.Errorf("failed to apply %s: %w", s3CacheGUCName, err)
			}
		}
		c.logger().Info("Set duckgres.s3_cache.", "enabled", enabled)
	}
	c.s3CacheOff = !enabled
	return nil
}

// applyStartupS3Cache validates and applies a `-c duckgres.s3_cache=…`
// startup-option value. An invalid value returns the 22023 error for the
// caller to reject the connection with (mirroring duckgres.query_source and
// the duckgres.worker_* startup options); a worker-apply failure returns that
// error so the caller can refuse the connection rather than start a session
// whose cache state doesn't match the requested option.
func (c *clientConn) applyStartupS3Cache(raw string) error {
	norm, err := transform.NormalizeS3Cache(raw)
	if err != nil {
		return err
	}
	return c.applyS3CacheSetting(norm)
}

// reapplyS3CacheAfterWorkerSwitch re-asserts this session's `duckgres.s3_cache`
// state on a freshly acquired worker after a tier escalation. The bypass lives
// in the WORKER's `ducklake_s3` secret, not in session state, and a new session
// always starts on the cache-proxy transport (CreateSession restores it before
// the session starts, so a bypass never leaks into the org's next session).
// Without this re-apply, a connection that asked for `off` would silently start
// reading through the cache the moment it escalated — exactly the failure mode
// applyS3CacheSetting exists to prevent, and one applyS3CacheSetting itself
// cannot fix here because its "did the effective state change?" guard sees no
// change. No-op when the session never bypassed the cache, or when the executor
// has no cache to bypass.
func (c *clientConn) reapplyS3CacheAfterWorkerSwitch(ctx context.Context) error {
	if !c.s3CacheOff {
		return nil
	}
	ctrl, ok := c.executor.(S3CacheControl)
	if !ok {
		return nil
	}
	applyCtx, cancel := context.WithTimeout(ctx, s3CacheApplyTimeout)
	defer cancel()
	if err := ctrl.SetS3CacheEnabled(applyCtx, false); err != nil {
		return fmt.Errorf("failed to re-apply %s on the new worker: %w", s3CacheGUCName, err)
	}
	c.logger().Info("Re-applied duckgres.s3_cache after worker switch.", "enabled", false)
	return nil
}

// S3CacheGUCName is the startup-option / GUC name, exported for the control
// plane's startup-option parsing.
const S3CacheGUCName = s3CacheGUCName

// ValidateS3CacheOption validates a `-c duckgres.s3_cache=…` startup-option
// value without applying it. The control plane calls this BEFORE acquiring a
// worker so a bad option is rejected (FATAL 22023) without spending a spawn.
func ValidateS3CacheOption(raw string) error {
	_, err := transform.NormalizeS3Cache(raw)
	return err
}

// ApplyConnectionS3CacheOption applies a pre-validated `-c duckgres.s3_cache=…`
// startup option on a control-plane connection whose session executor is
// already bound. A returned error means the worker could not swap the S3
// transport — the caller should refuse the connection (FATAL) rather than
// start a session whose cache state doesn't match the requested option (a
// benchmark connecting with s3_cache=off must never silently run cached).
func ApplyConnectionS3CacheOption(cc *clientConn, raw string) error {
	return cc.applyStartupS3Cache(raw)
}
