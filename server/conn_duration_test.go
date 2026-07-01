package server

import (
	"testing"
	"time"
)

// CloseConnectionMetrics must return the elapsed connection lifetime measured
// from backendStart so the control plane can log duration_ms. The histogram
// side effect is covered in server/observe; here we pin the returned duration
// (the value the disconnect log reports).
func TestCloseConnectionMetricsReturnsLifetime(t *testing.T) {
	cc := &clientConn{orgID: "close-metrics-org", backendStart: time.Now().Add(-250 * time.Millisecond)}

	dur := CloseConnectionMetrics(cc)

	if dur < 250*time.Millisecond {
		t.Fatalf("duration = %v, want >= 250ms", dur)
	}
	if dur > 5*time.Second {
		t.Fatalf("duration = %v, implausibly large (backendStart not honoured?)", dur)
	}
}
