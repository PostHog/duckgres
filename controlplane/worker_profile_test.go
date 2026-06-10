package controlplane

import (
	"strings"
	"testing"
	"time"
)

// noOrgDefaults is the zero org default profile — single-tenant deployments
// and orgs with no default_worker_* columns set.
var noOrgDefaults = orgWorkerProfileDefaults{}

// newSizingTestCP builds a ControlPlane whose K8s config enables client worker
// sizing with a prod-shaped clamp policy and explicit defaults.
func newSizingTestCP() *ControlPlane {
	return &ControlPlane{cfg: ControlPlaneConfig{K8s: K8sConfig{
		AllowClientWorkerProfile: true,
		WorkerCPURequest:         "8",
		WorkerMemoryRequest:      "16Gi",
		WorkerProfileMinCPU:      "1",
		WorkerProfileMaxCPU:      "16",
		WorkerProfileMinMemory:   "4Gi",
		WorkerProfileMaxMemory:   "64Gi",
		WorkerMaxTTL:             time.Hour,
	}}}
}

func TestResolveWorkerProfileSizing(t *testing.T) {
	cp := newSizingTestCP()
	tests := []struct {
		name     string
		opts     map[string]string
		wantNil  bool   // expect the default (nil) profile — matches the neutral warm pool
		wantKey  string // CPU|Memory (only when !wantNil)
		wantTTL  time.Duration
		wantErr  bool
		wantWarn bool
	}{
		{name: "no opts -> default (nil)", opts: map[string]string{}, wantNil: true},
		{name: "unrelated opts -> default (nil)", opts: map[string]string{"search_path": "iceberg.public"}, wantNil: true},
		{name: "client cpu+mem", opts: map[string]string{gucWorkerCPU: "4", gucWorkerMemory: "32Gi"}, wantKey: "4|32Gi", wantTTL: defaultWorkerTTL},
		{name: "client ttl only -> concrete w/ default size", opts: map[string]string{gucWorkerTTL: "5m"}, wantKey: "8|16Gi", wantTTL: 5 * time.Minute},
		{name: "cpu over max -> clamp+warn", opts: map[string]string{gucWorkerCPU: "64"}, wantKey: "16|16Gi", wantTTL: defaultWorkerTTL, wantWarn: true},
		{name: "mem under min -> clamp+warn", opts: map[string]string{gucWorkerMemory: "1Gi"}, wantKey: "8|4Gi", wantTTL: defaultWorkerTTL, wantWarn: true},
		{name: "ttl over max -> clamp+warn", opts: map[string]string{gucWorkerTTL: "24h"}, wantKey: "8|16Gi", wantTTL: time.Hour, wantWarn: true},
		{name: "cpu normalized", opts: map[string]string{gucWorkerCPU: "4000m"}, wantKey: "4|16Gi", wantTTL: defaultWorkerTTL},
		{name: "zero cpu -> error", opts: map[string]string{gucWorkerCPU: "0"}, wantErr: true},
		{name: "bad mem -> error", opts: map[string]string{gucWorkerMemory: "lots"}, wantErr: true},
		{name: "bad ttl -> error", opts: map[string]string{gucWorkerTTL: "soon"}, wantErr: true},
		{name: "negative ttl -> error", opts: map[string]string{gucWorkerTTL: "-5m"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, warns, orgApplied, err := cp.resolveWorkerProfile(tt.opts, noOrgDefaults)
			if orgApplied {
				t.Fatal("orgApplied must be false without org defaults")
			}
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got profile=%v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("expected nil (default) profile, got %s", got.MatchKey())
				}
				return
			}
			if got == nil {
				t.Fatalf("expected concrete profile %q, got nil", tt.wantKey)
			}
			if got.MatchKey() != tt.wantKey {
				t.Fatalf("MatchKey = %q, want %q", got.MatchKey(), tt.wantKey)
			}
			if got.TTL != tt.wantTTL {
				t.Fatalf("TTL = %s, want %s", got.TTL, tt.wantTTL)
			}
			if tt.wantWarn && len(warns) == 0 {
				t.Fatal("expected a clamp warning, got none")
			}
			if !tt.wantWarn && len(warns) != 0 {
				t.Fatalf("unexpected warnings: %v", warns)
			}
		})
	}
}

// Gate off: every GUC is ignored and the default (nil) profile is returned (never
// an error, even for garbage values) — preserving warm-pool reuse.
func TestResolveWorkerProfileSizing_GateOff(t *testing.T) {
	cp := newSizingTestCP()
	cp.cfg.K8s.AllowClientWorkerProfile = false
	got, warns, orgApplied, err := cp.resolveWorkerProfile(map[string]string{gucWorkerCPU: "garbage", gucWorkerTTL: "nope"}, noOrgDefaults)
	if err != nil {
		t.Fatalf("gate off must never error, got %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("gate off must not warn, got %v", warns)
	}
	if got != nil {
		t.Fatalf("gate off must return the default (nil) profile, got %s", got.MatchKey())
	}
	if orgApplied {
		t.Fatal("orgApplied must be false without org defaults")
	}
}

// A no-sizing request must return the nil default sentinel so it matches the
// neutral warm pool (regression guard: returning a concrete-default profile here
// broke worker acquisition on a warm-pool-enabled deploy).
func TestResolveWorkerProfileSizing_NoSizingIsNil(t *testing.T) {
	cp := newSizingTestCP()
	got, _, _, err := cp.resolveWorkerProfile(map[string]string{}, noOrgDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("no-sizing request must return nil (default), got %s", got.MatchKey())
	}
}

// When the client DOES set sizing and no default size is configured, the built-in
// 8/16Gi/20m applies for the omitted fields.
func TestResolveWorkerProfileSizing_BuiltinDefaults(t *testing.T) {
	cp := &ControlPlane{cfg: ControlPlaneConfig{K8s: K8sConfig{AllowClientWorkerProfile: true}}}
	got, _, _, err := cp.resolveWorkerProfile(map[string]string{gucWorkerTTL: "1m"}, noOrgDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.CPU != defaultWorkerCPU || got.Memory != defaultWorkerMemory || got.TTL != time.Minute {
		t.Fatalf("built-in defaults = %v, want %s/%s/1m", got, defaultWorkerCPU, defaultWorkerMemory)
	}
}

// Org default worker profile (operator-set via the admin API): when the client
// sends no sizing GUCs, the org default produces a concrete profile; partially
// set org defaults fall back per-field to the pool-global/built-in defaults;
// client GUCs override per-field; an org default never errors.
func TestResolveWorkerProfileOrgDefaults(t *testing.T) {
	tests := []struct {
		name           string
		opts           map[string]string
		org            orgWorkerProfileDefaults
		wantNil        bool
		wantKey        string
		wantTTL        time.Duration
		wantOrgApplied bool
		wantWarn       string // substring expected in a warning, "" = no warnings expected
	}{
		{
			name:           "org default all three fields",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{CPU: "2", Memory: "8Gi", TTL: "10m"},
			wantKey:        "2|8Gi",
			wantTTL:        10 * time.Minute,
			wantOrgApplied: true,
		},
		{
			name:           "org cpu only -> mem/ttl fall back to pool-global/built-in",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{CPU: "2"},
			wantKey:        "2|16Gi",
			wantTTL:        defaultWorkerTTL,
			wantOrgApplied: true,
		},
		{
			name:           "org ttl only -> size falls back to pool-global",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{TTL: "45m"},
			wantKey:        "8|16Gi",
			wantTTL:        45 * time.Minute,
			wantOrgApplied: true,
		},
		{
			name:           "org default normalized (4000m -> 4)",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{CPU: "4000m", Memory: "8192Mi"},
			wantKey:        "4|8Gi",
			wantTTL:        defaultWorkerTTL,
			wantOrgApplied: true,
		},
		{
			name: "org default NOT clamped to WorkerProfileMax (operator config)",
			opts: map[string]string{},
			// Above the client clamp band (max cpu 16 / max mem 64Gi).
			org:            orgWorkerProfileDefaults{CPU: "46", Memory: "360Gi"},
			wantKey:        "46|360Gi",
			wantTTL:        defaultWorkerTTL,
			wantOrgApplied: true,
		},
		{
			name:           "org ttl clamped by WorkerMaxTTL",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{TTL: "24h"},
			wantKey:        "8|16Gi",
			wantTTL:        time.Hour, // WorkerMaxTTL in newSizingTestCP
			wantOrgApplied: true,
			wantWarn:       "clamped",
		},
		{
			name:           "client cpu overrides org cpu; org mem+ttl survive",
			opts:           map[string]string{gucWorkerCPU: "4"},
			org:            orgWorkerProfileDefaults{CPU: "2", Memory: "8Gi", TTL: "10m"},
			wantKey:        "4|8Gi",
			wantTTL:        10 * time.Minute,
			wantOrgApplied: true,
		},
		{
			name:           "client sets all -> org default fully overridden",
			opts:           map[string]string{gucWorkerCPU: "4", gucWorkerMemory: "32Gi", gucWorkerTTL: "5m"},
			org:            orgWorkerProfileDefaults{CPU: "2", Memory: "8Gi", TTL: "10m"},
			wantKey:        "4|32Gi",
			wantTTL:        5 * time.Minute,
			wantOrgApplied: false,
		},
		{
			name:           "invalid org cpu ignored with warning -> nil profile",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{CPU: "lots"},
			wantNil:        true,
			wantOrgApplied: false,
			wantWarn:       "org default worker cpu",
		},
		{
			name:           "negative org ttl ignored with warning -> nil profile",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{TTL: "-5m"},
			wantNil:        true,
			wantOrgApplied: false,
			wantWarn:       "org default worker ttl",
		},
		{
			name:           "invalid org mem ignored, valid org cpu still applies",
			opts:           map[string]string{},
			org:            orgWorkerProfileDefaults{CPU: "2", Memory: "garbage"},
			wantKey:        "2|16Gi",
			wantTTL:        defaultWorkerTTL,
			wantOrgApplied: true,
			wantWarn:       "org default worker memory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := newSizingTestCP()
			got, warns, orgApplied, err := cp.resolveWorkerProfile(tt.opts, tt.org)
			if err != nil {
				t.Fatalf("org defaults must never error the connection, got %v", err)
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("expected nil (default) profile, got %s", got.MatchKey())
				}
			} else {
				if got == nil {
					t.Fatalf("expected concrete profile %q, got nil", tt.wantKey)
				}
				if got.MatchKey() != tt.wantKey {
					t.Fatalf("MatchKey = %q, want %q", got.MatchKey(), tt.wantKey)
				}
				if got.TTL != tt.wantTTL {
					t.Fatalf("TTL = %s, want %s", got.TTL, tt.wantTTL)
				}
			}
			if orgApplied != tt.wantOrgApplied {
				t.Fatalf("orgApplied = %v, want %v", orgApplied, tt.wantOrgApplied)
			}
			if tt.wantWarn == "" {
				if len(warns) != 0 {
					t.Fatalf("unexpected warnings: %v", warns)
				}
			} else {
				found := false
				for _, w := range warns {
					if strings.Contains(w, tt.wantWarn) {
						found = true
					}
				}
				if !found {
					t.Fatalf("warnings %v missing %q", warns, tt.wantWarn)
				}
			}
		})
	}
}

// Org defaults apply even when AllowClientWorkerProfile is off: the gate is
// about trusting client startup options, not operator config. Client GUCs are
// still ignored (even garbage ones), so the org default is the whole profile.
func TestResolveWorkerProfileOrgDefaults_GateOff(t *testing.T) {
	cp := newSizingTestCP()
	cp.cfg.K8s.AllowClientWorkerProfile = false
	got, warns, orgApplied, err := cp.resolveWorkerProfile(
		map[string]string{gucWorkerCPU: "4", gucWorkerTTL: "garbage"},
		orgWorkerProfileDefaults{CPU: "2", Memory: "8Gi", TTL: "10m"},
	)
	if err != nil {
		t.Fatalf("gate off must never error, got %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if got == nil {
		t.Fatal("org default must apply with the client gate off, got nil profile")
	}
	if got.MatchKey() != "2|8Gi" || got.TTL != 10*time.Minute {
		t.Fatalf("profile = %s/%s, want 2|8Gi/10m (client GUCs must be ignored)", got.MatchKey(), got.TTL)
	}
	if !orgApplied {
		t.Fatal("orgApplied must be true when the org default shaped the profile")
	}
}

// An org default with no pool-global request configured falls back to the
// built-in 8/16Gi for unset size fields (mirrors the client path).
func TestResolveWorkerProfileOrgDefaults_BuiltinFallback(t *testing.T) {
	cp := &ControlPlane{cfg: ControlPlaneConfig{K8s: K8sConfig{}}}
	got, _, orgApplied, err := cp.resolveWorkerProfile(map[string]string{}, orgWorkerProfileDefaults{TTL: "30m"})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.CPU != defaultWorkerCPU || got.Memory != defaultWorkerMemory || got.TTL != 30*time.Minute {
		t.Fatalf("profile = %v, want %s/%s/30m", got, defaultWorkerCPU, defaultWorkerMemory)
	}
	if !orgApplied {
		t.Fatal("orgApplied must be true")
	}
}

func TestResolveWorkerProfileSizedNoTTLUsesWorkerDefaultTTL(t *testing.T) {
	// DUCKGRES_K8S_WORKER_DEFAULT_TTL is THE default TTL: a sized request that
	// omits duckgres.worker_ttl gets the deployment default, not the built-in
	// 20m. Precedence stays client GUC > org default > deployment default.
	cp := newSizingTestCP()
	cp.cfg.K8s.WorkerDefaultTTL = 70 * time.Minute

	p, _, _, err := cp.resolveWorkerProfile(map[string]string{"duckgres.worker_cpu": "4"}, orgWorkerProfileDefaults{})
	if err != nil || p == nil {
		t.Fatalf("sized request failed: p=%v err=%v", p, err)
	}
	if p.TTL != 70*time.Minute {
		t.Fatalf("sized-no-ttl TTL = %v, want 70m (deployment default)", p.TTL)
	}

	// Client GUC still wins over the deployment default.
	p, _, _, err = cp.resolveWorkerProfile(map[string]string{"duckgres.worker_cpu": "4", "duckgres.worker_ttl": "10m"}, orgWorkerProfileDefaults{})
	if err != nil || p == nil {
		t.Fatalf("sized+ttl request failed: p=%v err=%v", p, err)
	}
	if p.TTL != 10*time.Minute {
		t.Fatalf("client ttl = %v, want 10m (GUC beats deployment default)", p.TTL)
	}

	// Org default beats the deployment default too.
	p, _, _, err = cp.resolveWorkerProfile(map[string]string{"duckgres.worker_cpu": "4"}, orgWorkerProfileDefaults{TTL: "30m"})
	if err != nil || p == nil {
		t.Fatalf("sized+orgttl request failed: p=%v err=%v", p, err)
	}
	if p.TTL != 30*time.Minute {
		t.Fatalf("org ttl = %v, want 30m (org default beats deployment default)", p.TTL)
	}
}
