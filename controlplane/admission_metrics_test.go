package controlplane

import "testing"

func TestSessionAdmissionReasonsValue(t *testing.T) {
	tests := []struct {
		name    string
		reasons []string
		want    string
	}{
		{name: "none", want: "none"},
		{name: "single non-vCPU reason", reasons: []string{"fifo"}, want: "fifo"},
		{name: "repeated non-vCPU reason", reasons: []string{"fifo", "fifo"}, want: "fifo"},
		{name: "mixed non-vCPU reasons", reasons: []string{"fifo", "resharding"}, want: "mixed"},
		{name: "org vCPU takes precedence", reasons: []string{"fifo", "org_vcpu", "resharding"}, want: "org_vcpu"},
		{name: "user vCPU takes precedence", reasons: []string{"fifo", "user_vcpu", "store_error"}, want: "user_vcpu"},
		{name: "both vCPU limits", reasons: []string{"org_vcpu", "fifo", "user_vcpu"}, want: "org_user_vcpu"},
		{name: "both vCPU limits in reverse order", reasons: []string{"user_vcpu", "resharding", "org_vcpu"}, want: "org_user_vcpu"},
		{name: "unknown reason is bounded", reasons: []string{"future_dynamic_reason"}, want: "store_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sessionAdmissionReasons
			for _, reason := range tt.reasons {
				got.add(reason)
			}
			if value := got.value(); value != tt.want {
				t.Fatalf("value() = %q, want %q", value, tt.want)
			}
		})
	}
}
