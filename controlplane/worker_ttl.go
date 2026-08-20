package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/transpiler/transform"
)

// errWorkerTTLOverrideDisabled rejects a mid-session `SET duckgres.worker_ttl`
// when the deployment does not trust client-supplied worker settings
// (DUCKGRES_K8S_ALLOW_CLIENT_WORKER_PROFILE off) — the same trust boundary the
// duckgres.worker_* startup options have, except a startup option is silently
// ignored there while a SET must not pretend it took effect.
var errWorkerTTLOverrideDisabled = &transform.CodedError{
	Code:    "22023", // invalid_parameter_value
	Message: "duckgres.worker_ttl overrides are not enabled on this server",
}

// sessionWorkerTTLBaseline is the connect-time TTL a session's SHOW falls back
// to and RESET restores: the session profile's TTL when one was resolved
// (startup GUC / org default / exploratory tier), else the deployment default
// TTL, else the built-in 1m — the same chain resolveWorkerProfile applies.
func sessionWorkerTTLBaseline(profile *WorkerProfile, k K8sConfig) time.Duration {
	if profile != nil && profile.TTL > 0 {
		return profile.TTL
	}
	return effectiveDefaultWorkerTTL(k.WorkerDefaultTTL)
}

// workerTTLControlFor builds the per-connection server.WorkerTTLControl behind
// the mid-session `duckgres.worker_ttl` GUC. Apply updates the bound worker's
// pool-side hot-idle TTL (gated on AllowClientWorkerProfile, clamped to
// WorkerMaxTTL — both exactly like the startup option); Current lets SHOW
// report the TTL the bound worker would actually park with (a reused hot-idle
// worker can carry a previous request's TTL, which beats the baseline).
func (cp *ControlPlane) workerTTLControlFor(sessions *SessionManager, pid int32, initialProfile *WorkerProfile, clog *slog.Logger) *server.WorkerTTLControl {
	return &server.WorkerTTLControl{
		Baseline: sessionWorkerTTLBaseline(initialProfile, cp.cfg.K8s),
		Apply: func(_ context.Context, ttl time.Duration) (time.Duration, error) {
			if !cp.cfg.K8s.AllowClientWorkerProfile {
				return 0, errWorkerTTLOverrideDisabled
			}
			applied := ttl
			if max := cp.cfg.K8s.WorkerMaxTTL; max > 0 && ttl > max {
				clog.Warn("Clamped duckgres.worker_ttl override to the deployment maximum.",
					"requested", ttl.String(), "max", max.String())
				applied = max
			}
			if err := sessions.SetWorkerTTLForPID(pid, applied); err != nil {
				return 0, err
			}
			return applied, nil
		},
		Current: func() (time.Duration, bool) {
			ttl, ok := sessions.WorkerTTLForPID(pid)
			if !ok {
				return 0, false
			}
			if ttl <= 0 {
				// A default-profile worker carries TTL 0 = "the deployment
				// default applies at reap time"; resolve it for SHOW.
				ttl = effectiveDefaultWorkerTTL(cp.cfg.K8s.WorkerDefaultTTL)
			}
			return ttl, true
		},
	}
}
