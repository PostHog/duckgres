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

// orgWorkerProfileDefaults carries an org's operator-set default worker
// profile (config store columns default_worker_cpu/memory/ttl) into profile
// resolution. All fields are raw strings as stored; empty = unset. CPU/Memory
// are k8s resource quantities, TTL is a Go duration string (e.g. "75m").
type orgWorkerProfileDefaults struct {
	CPU    string
	Memory string
	TTL    string
}

// resolveWorkerProfile turns the client's worker_cpu / worker_memory /
// worker_ttl startup options plus the org's operator-set default worker
// profile into a *WorkerProfile (a worker size + hot-idle TTL), or nil.
//
// Precedence, per field (cpu, memory, ttl independently):
//  1. client GUC — only when AllowClientWorkerProfile is on; validated and
//     clamped to WorkerProfileMin/Max + WorkerMaxTTL exactly as before.
//  2. org default — applies REGARDLESS of AllowClientWorkerProfile: that gate
//     is about trusting client-supplied startup options, while org defaults
//     are operator config set via the authenticated admin API. They are
//     validated/normalized but NOT clamped to WorkerProfileMin/Max (TTL is
//     still bounded by WorkerMaxTTL). A field that fails validation is
//     ignored with a warning — a bad config-store row must not break
//     connections.
//  3. pool-global WorkerCPURequest/MemoryRequest (else built-in 8/16Gi); TTL
//     defaults to 20m.
//
// Returns:
//   - (nil, warns, false, nil)  => the DEFAULT profile: no client sizing and no
//     (valid) org default. nil is the sentinel for the default worker shape;
//     its size comes from the pool-global WorkerCPURequest/MemoryRequest (else
//     BestEffort), and it reuses a hot-idle default-shape worker for the org
//     (MatchKey "|").
//   - (profile, warns, orgApplied, nil) => a concrete shape was requested.
//     orgApplied reports whether any org-default field shaped the result (i.e.
//     was set, valid, and not overridden by a client GUC) so the session-setup
//     path can log which tenants run on an org default.
//   - (nil, nil, false, err) => an unparseable/negative client value (rejects
//     the connection). Org-default values never produce an error.
//
// A no-sizing request returns nil (the default shape) rather than a concrete
// profile, so default traffic and a hot-idle default-shape worker share the same
// MatchKey ("|") and reuse each other. A sized request spawns/reuses a worker of
// its exact cpu/memory shape on demand (see docs/design/worker-ttl-pool.md).
func (cp *ControlPlane) resolveWorkerProfile(opts map[string]string, org orgWorkerProfileDefaults) (*WorkerProfile, []string, bool, error) {
	k := cp.cfg.K8s

	// Validate/normalize the org defaults first so they can act as the base
	// layer underneath any client GUCs. Invalid fields warn and fall back as
	// if unset.
	var warns []string
	orgCPU, orgMem := "", ""
	var orgTTL time.Duration
	orgTTLSet := false
	if raw := strings.TrimSpace(org.CPU); raw != "" {
		if v, _, err := sizeField("org default_worker_cpu", raw, "", ""); err != nil {
			warns = append(warns, fmt.Sprintf("ignoring invalid org default worker cpu: %v", err))
		} else {
			orgCPU = v
		}
	}
	if raw := strings.TrimSpace(org.Memory); raw != "" {
		if v, _, err := sizeField("org default_worker_memory", raw, "", ""); err != nil {
			warns = append(warns, fmt.Sprintf("ignoring invalid org default worker memory: %v", err))
		} else {
			orgMem = v
		}
	}
	if raw := strings.TrimSpace(org.TTL); raw != "" {
		if v, warn, err := ttlField("org default_worker_ttl", raw, k.WorkerMaxTTL); err != nil {
			warns = append(warns, fmt.Sprintf("ignoring invalid org default worker ttl: %v", err))
		} else {
			orgTTL, orgTTLSet = v, true
			if warn != "" {
				warns = append(warns, warn)
			}
		}
	}

	// Client startup options are only read when the deployment opts in; with
	// the gate off even garbage GUC values are ignored, never an error.
	rawCPU, rawMem, rawTTL := "", "", ""
	if k.AllowClientWorkerProfile {
		rawCPU = strings.TrimSpace(opts[gucWorkerCPU])
		rawMem = strings.TrimSpace(opts[gucWorkerMemory])
		rawTTL = strings.TrimSpace(opts[gucWorkerTTL])
	}

	if rawCPU == "" && rawMem == "" && rawTTL == "" && orgCPU == "" && orgMem == "" && !orgTTLSet {
		return nil, warns, false, nil // no sizing anywhere -> default (nil) profile
	}

	// Base layer per field: org default, else pool-global request, else built-in.
	cpu := firstNonEmpty(orgCPU, firstNonEmpty(strings.TrimSpace(k.WorkerCPURequest), defaultWorkerCPU))
	mem := firstNonEmpty(orgMem, firstNonEmpty(strings.TrimSpace(k.WorkerMemoryRequest), defaultWorkerMemory))
	ttl := defaultWorkerTTL
	if orgTTLSet {
		ttl = orgTTL
	}

	if rawCPU != "" {
		v, warn, err := sizeField(gucWorkerCPU, rawCPU, k.WorkerProfileMinCPU, k.WorkerProfileMaxCPU)
		if err != nil {
			return nil, nil, false, err
		}
		cpu = v
		if warn != "" {
			warns = append(warns, warn)
		}
	}
	if rawMem != "" {
		v, warn, err := sizeField(gucWorkerMemory, rawMem, k.WorkerProfileMinMemory, k.WorkerProfileMaxMemory)
		if err != nil {
			return nil, nil, false, err
		}
		mem = v
		if warn != "" {
			warns = append(warns, warn)
		}
	}
	if rawTTL != "" {
		v, warn, err := ttlField(gucWorkerTTL, rawTTL, k.WorkerMaxTTL)
		if err != nil {
			return nil, nil, false, err
		}
		ttl = v
		if warn != "" {
			warns = append(warns, warn)
		}
	}

	// An org default "applied" when a valid org field survived into the result
	// (i.e. the client did not override that same field).
	orgApplied := (orgCPU != "" && rawCPU == "") || (orgMem != "" && rawMem == "") || (orgTTLSet && rawTTL == "")

	// Normalize the sizes so reuse-matching is canonical ("16Gi" == "16384Mi").
	cpuN, _, err := sizeField(gucWorkerCPU, cpu, "", "")
	if err != nil {
		return nil, nil, false, fmt.Errorf("invalid default worker cpu: %w", err)
	}
	memN, _, err := sizeField(gucWorkerMemory, mem, "", "")
	if err != nil {
		return nil, nil, false, fmt.Errorf("invalid default worker memory: %w", err)
	}

	return &WorkerProfile{CPU: cpuN, Memory: memN, TTL: ttl}, warns, orgApplied, nil
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
func ttlField(name, raw string, maxTTL time.Duration) (time.Duration, string, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, "", fmt.Errorf("%s: invalid duration %q", name, raw)
	}
	if d < 0 {
		return 0, "", fmt.Errorf("%s: duration %q must be >= 0", name, raw)
	}
	if maxTTL > 0 && d > maxTTL {
		return maxTTL, fmt.Sprintf("%s %q clamped to %q", name, raw, maxTTL.String()), nil
	}
	return d, "", nil
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
