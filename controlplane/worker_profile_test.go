package controlplane

import (
	"testing"
	"time"
)

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
		wantNil  bool // expect the default (nil) profile — matches the neutral warm pool
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
			got, warns, err := cp.resolveWorkerProfile(tt.opts)
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
	got, warns, err := cp.resolveWorkerProfile(map[string]string{gucWorkerCPU: "garbage", gucWorkerTTL: "nope"})
	if err != nil {
		t.Fatalf("gate off must never error, got %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("gate off must not warn, got %v", warns)
	}
	if got != nil {
		t.Fatalf("gate off must return the default (nil) profile, got %s", got.MatchKey())
	}
}

// A no-sizing request must return the nil default sentinel so it matches the
// neutral warm pool (regression guard: returning a concrete-default profile here
// broke worker acquisition on a warm-pool-enabled deploy).
func TestResolveWorkerProfileSizing_NoSizingIsNil(t *testing.T) {
	cp := newSizingTestCP()
	got, _, err := cp.resolveWorkerProfile(map[string]string{})
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
	got, _, err := cp.resolveWorkerProfile(map[string]string{gucWorkerTTL: "1m"})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.CPU != defaultWorkerCPU || got.Memory != defaultWorkerMemory || got.TTL != time.Minute {
		t.Fatalf("built-in defaults = %v, want %s/%s/1m", got, defaultWorkerCPU, defaultWorkerMemory)
	}
}
