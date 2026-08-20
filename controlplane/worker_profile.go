package controlplane

import (
	"errors"
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
// defaultWorkerTTL is the SINGLE built-in TTL: it backs both sized-but-no-ttl
// requests here and default-shape workers in the janitor
// (effectiveDefaultWorkerTTL), so "no TTL anywhere" means 1m regardless of
// how the worker was requested. Kept short so an idle one-session worker pod
// (and the r6gd node behind it) is reclaimed quickly by default; deployments
// that want longer warm reuse set DUCKGRES_K8S_WORKER_DEFAULT_TTL, and a
// specific connection/org can opt up via duckgres.worker_ttl / default_worker_ttl.
const (
	defaultWorkerCPU    = "8"
	defaultWorkerMemory = "16Gi"
	defaultWorkerTTL    = 1 * time.Minute
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
//     defaults to the deployment WorkerDefaultTTL (else built-in 1m).
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
	return resolveWorkerProfileWithConfig(cp.cfg.K8s, opts, org)
}

func resolveWorkerProfileWithConfig(k K8sConfig, opts map[string]string, org orgWorkerProfileDefaults) (*WorkerProfile, []string, bool, error) {
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
	// TTL base layer mirrors cpu/mem: org default, else the deployment default
	// TTL (DUCKGRES_K8S_WORKER_DEFAULT_TTL — the same knob that governs
	// default-shape workers via the janitor), else the built-in 1m.
	ttl := defaultWorkerTTL
	if k.WorkerDefaultTTL > 0 {
		ttl = k.WorkerDefaultTTL
	}
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

// effectiveDefaultWorkerTTL resolves the hot-idle retention floor: the
// operator default TTL (DUCKGRES_K8S_WORKER_DEFAULT_TTL →
// K8sConfig.WorkerDefaultTTL) when set, otherwise the single built-in
// defaultWorkerTTL (1m — the same fallback sized-but-no-ttl requests get at
// profile resolution, so there is exactly ONE default TTL however a worker
// came to have no explicit one). The full per-request precedence is:
// client GUC > org default > deployment default TTL > built-in 1m.
func effectiveDefaultWorkerTTL(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return defaultWorkerTTL
}

func requestedWorkerVCPUs(profile *WorkerProfile, workerCPURequest string) (int, error) {
	cpu := strings.TrimSpace(workerCPURequest)
	if profile != nil && strings.TrimSpace(profile.CPU) != "" {
		cpu = strings.TrimSpace(profile.CPU)
	}
	if cpu == "" {
		cpu = defaultWorkerCPU
	}
	q, err := resource.ParseQuantity(cpu)
	if err != nil {
		return 0, fmt.Errorf("invalid worker cpu quantity %q: %w", cpu, err)
	}
	millis := q.MilliValue()
	if millis <= 0 {
		return 0, fmt.Errorf("worker cpu quantity %q must be positive", cpu)
	}
	return int((millis + 999) / 1000), nil
}

// useExploratoryTier decides whether a connection starts on the small
// exploratory worker rather than directly on its target shape. Every exclusion
// here is a case where starting small is a guaranteed-wasted acquire +
// escalate cycle, not a policy preference:
//
//   - Non-remote backends have no per-org worker pods to size at all.
//   - explProfile == nil means the tier is off or half-configured
//     (exploratoryWorkerProfile degrades to nil), so behave exactly as today.
//   - The client explicitly asked for a shape via duckgres.worker_* — give it
//     that shape from the first statement.
//   - Passthrough users speak raw DuckDB SQL, which pg_query cannot parse, so
//     classifyStatementTier pins on their FIRST statement no matter what it is.
//     This exclusion is also what keeps server's executeQueryDirect — the
//     passthrough-only execution path, which carries no tier hooks —
//     unreachable with onExploratoryWorker set.
func (cp *ControlPlane) useExploratoryTier(explProfile *WorkerProfile, passthroughUser bool, startupOptions map[string]string) bool {
	return cp.isRemoteBackend && explProfile != nil && !passthroughUser &&
		!clientSuppliedWorkerGUCs(cp.cfg.K8s, startupOptions)
}

// clientSuppliedWorkerGUCs reports whether the client's startup options carry
// an explicit worker sizing (any duckgres.worker_* GUC, honored only when the
// deployment trusts client sizing). Such connections bypass the exploratory
// tier: the client asked for a specific shape, so give it that shape from the
// first statement instead of starting small and escalating.
//
// Gated on AllowClientWorkerProfile for the same reason resolveWorkerProfile
// is: with the gate off the GUCs are ignored everywhere, so they must not
// silently steer tier selection either.
func clientSuppliedWorkerGUCs(k K8sConfig, opts map[string]string) bool {
	if !k.AllowClientWorkerProfile {
		return false
	}
	return strings.TrimSpace(opts[gucWorkerCPU]) != "" ||
		strings.TrimSpace(opts[gucWorkerMemory]) != "" ||
		strings.TrimSpace(opts[gucWorkerTTL]) != ""
}

// disabledUserMessage is the client-facing text for the per-user kill switch.
// Shared by the connect-time 28000 rejection and the tier-escalation re-check
// so a disabled user gets the same explanation whichever gate catches it.
const disabledUserMessage = "this account is disabled; contact your administrator"

// errEscalationUserDisabled aborts a tier escalation whose user was disabled
// during the switcher's destroy→create window — the one window in which a
// connection is invisible to the per-user disable fan-out (it holds neither a
// session nor a registered conn-closer). Escalation failure is
// connection-fatal, so this text reaches the client.
var errEscalationUserDisabled = errors.New(disabledUserMessage)

// defaultExploratoryWorkerTTL keeps an org's exploratory worker parked
// hot-idle for two days after its last connection — the "warm pod for every
// recently-active team" retention from the tier design.
const defaultExploratoryWorkerTTL = 48 * time.Hour

// exploratoryWorkerProfile resolves the deployment's exploratory small-worker
// shape. Returns nil when the tier is disabled OR unusable (missing/invalid
// size) — a half-configured tier must degrade to today's behavior, never to a
// BestEffort pod. Sizes are normalized so MatchKey-based reuse is canonical.
func exploratoryWorkerProfile(k K8sConfig) (*WorkerProfile, []string) {
	if !k.ExploratoryTierEnabled {
		return nil, nil
	}
	var warns []string
	if strings.TrimSpace(k.ExploratoryWorkerCPU) == "" || strings.TrimSpace(k.ExploratoryWorkerMemory) == "" {
		return nil, append(warns, "exploratory tier enabled but DUCKGRES_EXPLORATORY_WORKER_CPU/MEMORY not both set; tier disabled")
	}
	cpu, _, err := sizeField("exploratory worker cpu", k.ExploratoryWorkerCPU, "", "")
	if err != nil {
		return nil, append(warns, fmt.Sprintf("invalid exploratory worker cpu; tier disabled: %v", err))
	}
	mem, _, err := sizeField("exploratory worker memory", k.ExploratoryWorkerMemory, "", "")
	if err != nil {
		return nil, append(warns, fmt.Sprintf("invalid exploratory worker memory; tier disabled: %v", err))
	}
	ttl := defaultExploratoryWorkerTTL
	if k.ExploratoryWorkerTTL > 0 {
		ttl = k.ExploratoryWorkerTTL
	}
	return &WorkerProfile{CPU: cpu, Memory: mem, TTL: ttl}, warns
}
