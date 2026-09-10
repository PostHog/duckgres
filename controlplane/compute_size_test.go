package controlplane

import "testing"

func TestParseK8sCPUMillicores(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"8", 8000},
		{"8000m", 8000},
		{"500m", 500},
		{"2", 2000},
		{"0.5", 500},
		{"1.5", 1500},
		{"garbage", 0},
		{"-1", 0},
	}
	for _, tt := range tests {
		if got := parseK8sCPUMillicores(tt.in); got != tt.want {
			t.Errorf("parseK8sCPUMillicores(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
