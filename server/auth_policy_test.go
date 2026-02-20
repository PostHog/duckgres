package server

import (
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterMetricValue(t *testing.T, metricName string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_COUNTER {
			t.Fatalf("metric %q is not a counter", metricName)
		}
		var total float64
		for _, metric := range fam.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
		return total
	}
	t.Fatalf("metric %q not found", metricName)
	return 0
}

func TestValidateUserPassword(t *testing.T) {
	users := map[string]string{"postgres": "postgres"}

	if !ValidateUserPassword(users, "postgres", "postgres") {
		t.Fatalf("expected valid credentials to pass")
	}
	if ValidateUserPassword(users, "postgres", "wrong") {
		t.Fatalf("expected wrong password to fail")
	}
	if ValidateUserPassword(users, "unknown", "postgres") {
		t.Fatalf("expected unknown user to fail")
	}
}

func TestRecordFailedAuthAttemptIncrementsMetricAndBans(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 41000}
	rl := NewRateLimiter(RateLimitConfig{
		MaxFailedAttempts:   1,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 10,
	})

	before := counterMetricValue(t, "duckgres_auth_failures_total")
	banned := RecordFailedAuthAttempt(rl, addr)
	after := counterMetricValue(t, "duckgres_auth_failures_total")

	if !banned {
		t.Fatalf("expected failed auth attempt to ban when threshold is 1")
	}
	if after-before != 1 {
		t.Fatalf("expected duckgres_auth_failures_total delta 1, got %.0f", after-before)
	}
}

func TestBeginRateLimitedAuthAttemptRejectsBannedAndIncrementsMetric(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.11"), Port: 41001}
	rl := NewRateLimiter(RateLimitConfig{
		MaxFailedAttempts:   1,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 10,
	})
	rl.RecordFailedAuth(addr)

	before := counterMetricValue(t, "duckgres_rate_limit_rejects_total")
	release, reason := BeginRateLimitedAuthAttempt(rl, addr)
	release()
	after := counterMetricValue(t, "duckgres_rate_limit_rejects_total")

	if reason == "" {
		t.Fatalf("expected non-empty rejection reason for banned client")
	}
	if after-before != 1 {
		t.Fatalf("expected duckgres_rate_limit_rejects_total delta 1, got %.0f", after-before)
	}
}

func TestBeginRateLimitedAuthAttemptRegistersAndReleases(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("203.0.113.12"), Port: 41002}
	rl := NewRateLimiter(RateLimitConfig{
		MaxFailedAttempts:   5,
		FailedAttemptWindow: time.Minute,
		BanDuration:         time.Hour,
		MaxConnectionsPerIP: 1,
	})

	release1, reason1 := BeginRateLimitedAuthAttempt(rl, addr)
	if reason1 != "" {
		t.Fatalf("unexpected first attempt rejection: %q", reason1)
	}

	release2, reason2 := BeginRateLimitedAuthAttempt(rl, addr)
	release2()
	if reason2 == "" {
		t.Fatalf("expected second concurrent attempt to be rejected")
	}

	release1()

	release3, reason3 := BeginRateLimitedAuthAttempt(rl, addr)
	release3()
	if reason3 != "" {
		t.Fatalf("expected third attempt to succeed after release, got %q", reason3)
	}
}
