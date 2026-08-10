package server

import (
	"errors"
	"testing"
	"time"
)

// Client-controlled idle timeout is a connect-time escape hatch from the
// control-plane default. It is deliberately bounded by an operator cap: a
// client must never be able to pin a worker indefinitely by requesting an
// unbounded, zero, or negative duration.
func TestValidateClientIdleTimeoutOption(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		max     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "valid below cap", raw: "1m", max: 5 * time.Minute, want: time.Minute},
		{name: "valid at cap", raw: "5m", max: 5 * time.Minute, want: 5 * time.Minute},
		{name: "valid whitespace", raw: " 90s ", max: 5 * time.Minute, want: 90 * time.Second},
		{name: "feature disabled by zero cap", raw: "1m", max: 0, wantErr: true},
		{name: "feature disabled by negative cap", raw: "1m", max: -time.Second, wantErr: true},
		{name: "over cap", raw: "6m", max: 5 * time.Minute, wantErr: true},
		{name: "zero rejected", raw: "0s", max: 5 * time.Minute, wantErr: true},
		{name: "negative rejected", raw: "-1s", max: 5 * time.Minute, wantErr: true},
		{name: "invalid duration rejected", raw: "forever", max: 5 * time.Minute, wantErr: true},
		{name: "empty rejected", raw: "", max: 5 * time.Minute, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateClientIdleTimeoutOption(tt.raw, tt.max)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ValidateClientIdleTimeoutOption(%q, %s) = %s, nil error; want SQLSTATE 22023 rejection", tt.raw, tt.max, got)
				}
				var coded interface{ SQLState() string }
				if !errors.As(err, &coded) || coded.SQLState() != "22023" {
					t.Fatalf("ValidateClientIdleTimeoutOption(%q, %s) error = %v; want SQLSTATE 22023", tt.raw, tt.max, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateClientIdleTimeoutOption(%q, %s): %v", tt.raw, tt.max, err)
			}
			if got != tt.want {
				t.Fatalf("ValidateClientIdleTimeoutOption(%q, %s) = %s, want %s", tt.raw, tt.max, got, tt.want)
			}
		})
	}
}
