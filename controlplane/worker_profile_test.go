package controlplane

import "testing"

// wpBoolPtr is a small helper for tier specs.
func wpBoolPtr(b bool) *bool { return &b }

// newProfileTestCP builds a ControlPlane whose K8s config enables the
// connection-string worker-profile feature with the prod-shaped policy.
func newProfileTestCP() *ControlPlane {
	return &ControlPlane{cfg: ControlPlaneConfig{K8s: K8sConfig{
		AllowClientWorkerProfile:     true,
		AllowClientExclusiveNode:     false,
		ColocatedWorkerCPURequest:    "4",
		ColocatedWorkerMemoryRequest: "16Gi",
		ColocatedWorkerNodeSelector:  `{"posthog.com/nodepool":"duckgres-workers-colocated"}`,
		WorkerProfileMinCPU:          "1",
		WorkerProfileMaxCPU:          "16",
		WorkerProfileMinMemory:       "4Gi",
		WorkerProfileMaxMemory:       "64Gi",
		WorkerTiers: map[string]WorkerProfileSpec{
			"backfill": {CPU: "4", Memory: "16Gi", Colocate: wpBoolPtr(true)},
			"heavy":    {Colocate: wpBoolPtr(false)},
		},
	}}}
}

func TestResolveWorkerProfile(t *testing.T) {
	tests := []struct {
		name     string
		opts     map[string]string
		wantNil  bool // expect the default (nil) profile
		wantKey  string
		wantErr  bool
		wantWarn bool
	}{
		{name: "no opts -> default", opts: map[string]string{}, wantNil: true},
		{name: "unrelated opts -> default", opts: map[string]string{"search_path": "iceberg.public"}, wantNil: true},
		{name: "colocate true, no size -> defaults 4/16", opts: map[string]string{gucColocate: "true"}, wantKey: "4|16Gi|true"},
		{name: "inline size colocated", opts: map[string]string{gucColocate: "true", gucWorkerCPU: "8", gucWorkerMemory: "48Gi"}, wantKey: "8|48Gi|true"},
		{name: "tier alias backfill", opts: map[string]string{gucWorkerTier: "backfill"}, wantKey: "4|16Gi|true"},
		{name: "unknown tier -> error", opts: map[string]string{gucWorkerTier: "nope"}, wantErr: true},
		{name: "explicit colocate=false disallowed -> error", opts: map[string]string{gucColocate: "false"}, wantErr: true},
		{name: "tier heavy (colocate=false) disallowed -> error", opts: map[string]string{gucWorkerTier: "heavy"}, wantErr: true},
		{name: "invalid colocate -> error", opts: map[string]string{gucColocate: "maybe"}, wantErr: true},
		{name: "cpu over max -> clamp+warn", opts: map[string]string{gucColocate: "true", gucWorkerCPU: "64", gucWorkerMemory: "16Gi"}, wantKey: "16|16Gi|true", wantWarn: true},
		{name: "mem under min -> clamp+warn", opts: map[string]string{gucColocate: "true", gucWorkerCPU: "4", gucWorkerMemory: "1Gi"}, wantKey: "4|4Gi|true", wantWarn: true},
		{name: "zero cpu -> error", opts: map[string]string{gucColocate: "true", gucWorkerCPU: "0", gucWorkerMemory: "16Gi"}, wantErr: true},
		{name: "inline overrides tier", opts: map[string]string{gucWorkerTier: "backfill", gucWorkerCPU: "8", gucWorkerMemory: "48Gi"}, wantKey: "8|48Gi|true"},
	}

	cp := newProfileTestCP()
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
				t.Fatalf("expected profile %q, got nil (default)", tt.wantKey)
			}
			if got.MatchKey() != tt.wantKey {
				t.Fatalf("MatchKey = %q, want %q", got.MatchKey(), tt.wantKey)
			}
			if tt.wantWarn && len(warns) == 0 {
				t.Fatalf("expected a clamp warning, got none")
			}
			if !tt.wantWarn && len(warns) != 0 {
				t.Fatalf("unexpected warnings: %v", warns)
			}
		})
	}
}

// Gate off: every GUC is ignored and the default profile is returned.
func TestResolveWorkerProfile_GateOff(t *testing.T) {
	cp := newProfileTestCP()
	cp.cfg.K8s.AllowClientWorkerProfile = false
	got, _, err := cp.resolveWorkerProfile(map[string]string{gucColocate: "true", gucWorkerTier: "nope"})
	if err != nil {
		t.Fatalf("gate off must never error, got %v", err)
	}
	if got != nil {
		t.Fatalf("gate off must return the default (nil) profile, got %s", got.MatchKey())
	}
}

// Explicit colocate=false is honored when the deployment allows it, and a colocated
// node selector is attached only to colocated profiles.
func TestResolveWorkerProfile_ExclusiveAllowed(t *testing.T) {
	cp := newProfileTestCP()
	cp.cfg.K8s.AllowClientExclusiveNode = true

	got, _, err := cp.resolveWorkerProfile(map[string]string{gucColocate: "false", gucWorkerCPU: "32", gucWorkerMemory: "256Gi"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || got.Colocate {
		t.Fatalf("expected a non-colocated profile, got %v", got)
	}
	if got.MatchKey() != "32|256Gi|false" {
		t.Fatalf("MatchKey = %q, want %q", got.MatchKey(), "32|256Gi|false")
	}
}

// The tier alias and its inline equivalent must produce identical match keys so a
// worker reserved one way is reusable the other way.
func TestResolveWorkerProfile_TierEqualsInline(t *testing.T) {
	cp := newProfileTestCP()
	viaTier, _, err := cp.resolveWorkerProfile(map[string]string{gucWorkerTier: "backfill"})
	if err != nil {
		t.Fatal(err)
	}
	viaInline, _, err := cp.resolveWorkerProfile(map[string]string{gucColocate: "true", gucWorkerCPU: "4", gucWorkerMemory: "16Gi"})
	if err != nil {
		t.Fatal(err)
	}
	if !viaTier.Equal(viaInline) {
		t.Fatalf("tier %q != inline %q", viaTier.MatchKey(), viaInline.MatchKey())
	}
}

// Quantity normalization collapses equivalent spellings so they match.
func TestResolveWorkerProfile_Normalization(t *testing.T) {
	cp := newProfileTestCP()
	a, _, err := cp.resolveWorkerProfile(map[string]string{gucColocate: "true", gucWorkerCPU: "4000m", gucWorkerMemory: "16Gi"})
	if err != nil {
		t.Fatal(err)
	}
	if a.MatchKey() != "4|16Gi|true" {
		t.Fatalf("MatchKey = %q, want %q (4000m should normalize to 4)", a.MatchKey(), "4|16Gi|true")
	}
}

// MatchKey/Equal must be nil-safe and treat nil as the default profile.
func TestWorkerProfileMatchKeyNilSafe(t *testing.T) {
	var nilProfile *WorkerProfile
	if nilProfile.MatchKey() != "||false" {
		t.Fatalf("nil MatchKey = %q, want %q", nilProfile.MatchKey(), "||false")
	}
	def := &WorkerProfile{}
	if !nilProfile.Equal(def) || !def.Equal(nilProfile) {
		t.Fatalf("nil profile must equal the zero/default profile")
	}
	colo := &WorkerProfile{CPU: "4", Memory: "16Gi", Colocate: true}
	if nilProfile.Equal(colo) {
		t.Fatalf("nil (default) profile must not equal a colocated profile")
	}
}
