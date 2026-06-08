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
		wantKey  string // CPU|Memory|Colocate
		wantTTL  time.Duration
		wantErr  bool
		wantWarn bool
	}{
		{name: "no opts -> defaults", opts: map[string]string{}, wantKey: "8|16Gi|false", wantTTL: defaultWorkerTTL},
		{name: "unrelated opts -> defaults", opts: map[string]string{"search_path": "iceberg.public"}, wantKey: "8|16Gi|false", wantTTL: defaultWorkerTTL},
		{name: "client cpu+mem", opts: map[string]string{gucWorkerCPU: "4", gucWorkerMemory: "32Gi"}, wantKey: "4|32Gi|false", wantTTL: defaultWorkerTTL},
		{name: "client ttl", opts: map[string]string{gucWorkerTTL: "5m"}, wantKey: "8|16Gi|false", wantTTL: 5 * time.Minute},
		{name: "cpu over max -> clamp+warn", opts: map[string]string{gucWorkerCPU: "64"}, wantKey: "16|16Gi|false", wantTTL: defaultWorkerTTL, wantWarn: true},
		{name: "mem under min -> clamp+warn", opts: map[string]string{gucWorkerMemory: "1Gi"}, wantKey: "8|4Gi|false", wantTTL: defaultWorkerTTL, wantWarn: true},
		{name: "ttl over max -> clamp+warn", opts: map[string]string{gucWorkerTTL: "24h"}, wantKey: "8|16Gi|false", wantTTL: time.Hour, wantWarn: true},
		{name: "cpu normalized", opts: map[string]string{gucWorkerCPU: "4000m"}, wantKey: "4|16Gi|false", wantTTL: defaultWorkerTTL},
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
			if got == nil {
				t.Fatal("expected a concrete profile, got nil")
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

// Gate off: every GUC is ignored and the deployment defaults are returned (never
// an error, even for garbage values).
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
	if got == nil || got.MatchKey() != "8|16Gi|false" || got.TTL != defaultWorkerTTL {
		t.Fatalf("gate off must return defaults, got %v / ttl=%v", got, got.TTL)
	}
}

// With no configured default size, the built-in 8/16Gi/20m applies.
func TestResolveWorkerProfileSizing_BuiltinDefaults(t *testing.T) {
	cp := &ControlPlane{cfg: ControlPlaneConfig{K8s: K8sConfig{AllowClientWorkerProfile: true}}}
	got, _, err := cp.resolveWorkerProfile(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if got.CPU != defaultWorkerCPU || got.Memory != defaultWorkerMemory || got.TTL != defaultWorkerTTL {
		t.Fatalf("built-in defaults = %s/%s/%s, want %s/%s/%s", got.CPU, got.Memory, got.TTL, defaultWorkerCPU, defaultWorkerMemory, defaultWorkerTTL)
	}
}
