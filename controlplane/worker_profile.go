package controlplane

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Startup-option GUC names a client uses to select its worker size + idle TTL.
// Parsed from the libpq `options` parameter exactly like search_path (control.go).
const (
	gucWorkerCPU    = "duckgres.worker_cpu"
	gucWorkerMemory = "duckgres.worker_memory"
	gucWorkerTTL    = "duckgres.worker_ttl"
)

// Built-in defaults when a request omits a field (or client sizing is gated off)
// and the deployment did not configure its own default size/TTL.
const (
	defaultWorkerCPU    = "8"
	defaultWorkerMemory = "16Gi"
	defaultWorkerTTL    = 20 * time.Minute
)

// resolveWorkerProfile turns the client's worker_cpu / worker_memory / worker_ttl
// startup options into a *WorkerProfile (a worker size + hot-idle TTL), or nil.
//
//   - (nil, nil, nil)  => the DEFAULT profile: gate off, or no sizing GUC set.
//     nil is the sentinel that matches the neutral/default worker pool (the warm
//     pool spawns neutral workers with no profile), so default traffic reuses
//     warm workers exactly as before. The default worker SIZE then comes from the
//     pool-global WorkerCPURequest/MemoryRequest, not from this profile.
//   - (profile, warns, nil) => the client explicitly requested a size/ttl. cpu/mem
//     default to the pool-global request (else built-in 8/16Gi) for any field the
//     client omitted, clamped to [min,max]; ttl to [0,WorkerMaxTTL], default 20m.
//   - (nil, nil, err) => an unparseable/negative value (rejects the connection).
//
// IMPORTANT: a no-sizing request MUST return nil, not a concrete-default profile.
// Returning concrete here regresses warm-pool reuse — the concrete key (e.g.
// "8|16Gi|false") never matches a neutral warm worker's key ("||false"), so the
// request can neither reuse a warm worker nor be served by warm replenishment
// (which spawns neutral workers), and fails with "no warm worker available".
// Concrete sizing only becomes fully functional once the warm pool is removed
// (see docs/design/worker-ttl-pool.md) — until then it is opt-in via the gate.
func (cp *ControlPlane) resolveWorkerProfile(opts map[string]string) (*WorkerProfile, []string, error) {
	k := cp.cfg.K8s
	if !k.AllowClientWorkerProfile {
		return nil, nil, nil
	}

	rawCPU := strings.TrimSpace(opts[gucWorkerCPU])
	rawMem := strings.TrimSpace(opts[gucWorkerMemory])
	rawTTL := strings.TrimSpace(opts[gucWorkerTTL])
	if rawCPU == "" && rawMem == "" && rawTTL == "" {
		return nil, nil, nil // no client sizing -> default (nil) profile
	}

	cpu := firstNonEmpty(strings.TrimSpace(k.WorkerCPURequest), defaultWorkerCPU)
	mem := firstNonEmpty(strings.TrimSpace(k.WorkerMemoryRequest), defaultWorkerMemory)
	ttl := defaultWorkerTTL

	var warns []string
	if rawCPU != "" {
		v, warn, err := sizeField(gucWorkerCPU, rawCPU, k.WorkerProfileMinCPU, k.WorkerProfileMaxCPU)
		if err != nil {
			return nil, nil, err
		}
		cpu = v
		if warn != "" {
			warns = append(warns, warn)
		}
	}
	if rawMem != "" {
		v, warn, err := sizeField(gucWorkerMemory, rawMem, k.WorkerProfileMinMemory, k.WorkerProfileMaxMemory)
		if err != nil {
			return nil, nil, err
		}
		mem = v
		if warn != "" {
			warns = append(warns, warn)
		}
	}
	if rawTTL != "" {
		v, warn, err := ttlField(rawTTL, k.WorkerMaxTTL)
		if err != nil {
			return nil, nil, err
		}
		ttl = v
		if warn != "" {
			warns = append(warns, warn)
		}
	}

	// Normalize the sizes so reuse-matching is canonical ("16Gi" == "16384Mi").
	cpuN, _, err := sizeField(gucWorkerCPU, cpu, "", "")
	if err != nil {
		return nil, nil, fmt.Errorf("invalid default worker cpu: %w", err)
	}
	memN, _, err := sizeField(gucWorkerMemory, mem, "", "")
	if err != nil {
		return nil, nil, fmt.Errorf("invalid default worker memory: %w", err)
	}

	return &WorkerProfile{CPU: cpuN, Memory: memN, TTL: ttl}, warns, nil
}

// sizeField normalizes a client-supplied size and, when min/max are set, bounds
// it into [min,max], returning a human-readable warning if it was capped. A
// negative/zero or unparseable value errors.
func sizeField(name, raw, min, max string) (normalized, warn string, err error) {
	if raw == "" {
		return "", "", nil
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return "", "", fmt.Errorf("%s: invalid quantity %q", name, raw)
	}
	if q.Sign() <= 0 {
		return "", "", fmt.Errorf("%s: quantity %q must be positive", name, raw)
	}
	capped := false
	if min != "" {
		if lo, e := resource.ParseQuantity(min); e == nil && q.Cmp(lo) < 0 {
			q, capped = lo, true
		}
	}
	if max != "" {
		if hi, e := resource.ParseQuantity(max); e == nil && q.Cmp(hi) > 0 {
			q, capped = hi, true
		}
	}
	if capped {
		warn = fmt.Sprintf("%s %q clamped to %q", name, raw, q.String())
	}
	return q.String(), warn, nil
}

// ttlField parses a worker TTL (Go duration) and clamps it to [0,maxTTL] when
// maxTTL > 0. ttl=0 means "retire as soon as the last query finishes".
func ttlField(raw string, maxTTL time.Duration) (time.Duration, string, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, "", fmt.Errorf("%s: invalid duration %q", gucWorkerTTL, raw)
	}
	if d < 0 {
		return 0, "", fmt.Errorf("%s: duration %q must be >= 0", gucWorkerTTL, raw)
	}
	if maxTTL > 0 && d > maxTTL {
		return maxTTL, fmt.Sprintf("%s %q clamped to %q", gucWorkerTTL, raw, maxTTL.String()), nil
	}
	return d, "", nil
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
