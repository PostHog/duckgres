package controlplane

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Startup-option GUC names a client uses to select its worker shape. Parsed from
// the libpq `options` parameter exactly like search_path (see control.go).
const (
	gucColocate     = "duckgres.colocate"
	gucWorkerCPU    = "duckgres.worker_cpu"
	gucWorkerMemory = "duckgres.worker_memory"
	gucWorkerTier   = "duckgres.worker_tier"
)

// resolveWorkerProfile turns the parsed startup-option GUCs into a *WorkerProfile
// under this deployment's gate/clamp/tier policy.
//
// Returns:
//   - (nil, warns, nil)  => the default exclusive profile (gate off, no GUCs, or a
//     selection that normalizes to the default). nil is the sentinel that matches
//     legacy/default workers, so callers thread it straight through unchanged.
//   - (profile, warns, nil) => a non-default profile to schedule.
//   - (nil, nil, err) => reject the connection (unknown tier, disallowed
//     colocate=false, or an unparseable/non-positive size). Clamping never errors;
//     an out-of-range size is capped and reported via warns.
func (cp *ControlPlane) resolveWorkerProfile(opts map[string]string) (*WorkerProfile, []string, error) {
	k := cp.cfg.K8s
	if !k.AllowClientWorkerProfile {
		return nil, nil, nil
	}

	rawColocate := strings.TrimSpace(opts[gucColocate])
	rawCPU := strings.TrimSpace(opts[gucWorkerCPU])
	rawMem := strings.TrimSpace(opts[gucWorkerMemory])
	rawTier := strings.TrimSpace(opts[gucWorkerTier])

	if rawColocate == "" && rawCPU == "" && rawMem == "" && rawTier == "" {
		return nil, nil, nil // no selection -> default exclusive profile
	}

	// Tier alias expands first; explicit inline GUCs override its fields below.
	var (
		tierColocate     *bool
		tierCPU, tierMem string
	)
	if rawTier != "" {
		spec, ok := k.WorkerTiers[rawTier]
		if !ok {
			return nil, nil, fmt.Errorf("unknown worker tier %q", rawTier)
		}
		tierColocate, tierCPU, tierMem = spec.Colocate, spec.CPU, spec.Memory
	}

	// colocate: inline GUC > tier > default(false)
	colocate := false
	if tierColocate != nil {
		colocate = *tierColocate
	}
	if rawColocate != "" {
		b, err := strconv.ParseBool(rawColocate)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid %s %q (want true or false)", gucColocate, rawColocate)
		}
		colocate = b
	}

	// A full exclusive node is the expensive button; gate it independently.
	if !colocate && !k.AllowClientExclusiveNode {
		return nil, nil, fmt.Errorf("client-selected exclusive workers (colocate=false) are not permitted on this deployment")
	}

	// size: inline GUC > tier > colocate default. colocate=false with no size stays
	// empty -> the pool-global request (today's 46/360) applies.
	cpu := firstNonEmpty(rawCPU, tierCPU)
	mem := firstNonEmpty(rawMem, tierMem)
	if cpu == "" && mem == "" && colocate {
		cpu = k.ColocatedWorkerCPURequest
		mem = k.ColocatedWorkerMemoryRequest
	}

	// Normalize both sizes; clamp only COLOCATED sizes. The clamp range is the
	// bin-pack safety bound — it would wrongly cap an exclusive full-node request
	// (46/360 is well above it), and exclusive is already gated + quota'd.
	var warns []string
	cpu, warn, err := sizeField(gucWorkerCPU, cpu, k.WorkerProfileMinCPU, k.WorkerProfileMaxCPU, colocate)
	if err != nil {
		return nil, nil, err
	}
	if warn != "" {
		warns = append(warns, warn)
	}
	mem, warn, err = sizeField(gucWorkerMemory, mem, k.WorkerProfileMinMemory, k.WorkerProfileMaxMemory, colocate)
	if err != nil {
		return nil, nil, err
	}
	if warn != "" {
		warns = append(warns, warn)
	}

	// Guaranteed-QoS safety: a colocated (bin-packed) pod must carry explicit
	// non-empty resources, or it would schedule BestEffort and be first to OOM
	// under bin-packing. The colocate defaults guarantee this; this guards a
	// misconfigured deployment (colocated defaults unset).
	if colocate && (cpu == "" || mem == "") {
		return nil, nil, fmt.Errorf("colocated workers require both cpu and memory; set %s/%s or configure colocated defaults", gucWorkerCPU, gucWorkerMemory)
	}

	// Normalizes to the default exclusive profile -> return the nil sentinel so it
	// matches legacy/default workers rather than forming a distinct key.
	if cpu == "" && mem == "" && !colocate {
		return nil, warns, nil
	}

	profile := &WorkerProfile{CPU: cpu, Memory: mem, Colocate: colocate}
	if colocate {
		profile.NodeSelector = parseNodeSelectorJSON(k.ColocatedWorkerNodeSelector)
	}
	return profile, warns, nil
}

// sizeField normalizes a client-supplied size and, when clamp is set, bounds it
// into [min,max], returning a human-readable warning if it was capped. An empty
// raw value passes through unchanged (=> pool-global request).
func sizeField(name, raw, min, max string, clamp bool) (normalized, warn string, err error) {
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
	if clamp {
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
	}
	return q.String(), warn, nil
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

// parseNodeSelectorJSON parses a JSON object string into a nodeSelector map.
// Returns nil on empty/invalid input (the spawn path then falls back to the
// pool-global selector); the value never affects worker matching.
func parseNodeSelectorJSON(s string) map[string]string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil || len(m) == 0 {
		return nil
	}
	return m
}
