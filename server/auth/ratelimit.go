// Package auth holds duckgres' connection rate-limiting and password
// validation policy. It has no dependency on github.com/duckdb/duckdb-go,
// so the control plane can use it without linking libduckdb.
package auth

import (
	"net"
	"sync"
	"time"
)

// RateLimitConfig configures rate limiting behavior.
type RateLimitConfig struct {
	// MaxFailedAttempts is the maximum number of failed auth attempts before banning.
	MaxFailedAttempts int
	// FailedAttemptWindow is the time window for counting failed attempts.
	FailedAttemptWindow time.Duration
	// BanDuration is how long to ban an IP after exceeding max failed attempts.
	BanDuration time.Duration
	// MaxConnectionsPerIP is the max concurrent connections from a single IP (0 = unlimited).
	MaxConnectionsPerIP int
	// MaxConnections is the total max concurrent connections (0 = unlimited).
	MaxConnections int
}

// DefaultRateLimitConfig returns sensible defaults for rate limiting.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		MaxFailedAttempts:   5,
		FailedAttemptWindow: 5 * time.Minute,
		BanDuration:         15 * time.Minute,
		MaxConnectionsPerIP: 500,
		MaxConnections:      1024,
	}
}

// ipRecord tracks connection and authentication attempts from an IP.
type ipRecord struct {
	failedAttempts []time.Time // timestamps of failed auth attempts
	bannedUntil    time.Time   // when the ban expires (zero if not banned)
	activeConns    int         // current active connections from this IP
}

// RateLimiter tracks and limits connections per IP.
type RateLimiter struct {
	mu               sync.Mutex
	config           RateLimitConfig
	records          map[string]*ipRecord
	totalActiveConns int
}

// NewRateLimiter creates a new rate limiter with the given config.
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	rl := &RateLimiter{
		config:  cfg,
		records: make(map[string]*ipRecord),
	}
	go rl.cleanupLoop()
	return rl
}

// extractIP gets the IP address from a net.Addr (strips port).
func extractIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// CheckConnection checks if a connection from the given address should be allowed.
// Returns an error message if the connection should be rejected, or empty string if allowed.
func (rl *RateLimiter) CheckConnection(addr net.Addr) string {
	ip := extractIP(addr)
	if ip == "" {
		return ""
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.config.MaxConnections > 0 && rl.totalActiveConns >= rl.config.MaxConnections {
		return "too many concurrent connections"
	}

	record := rl.getOrCreateRecord(ip)

	if !record.bannedUntil.IsZero() && time.Now().Before(record.bannedUntil) {
		remaining := time.Until(record.bannedUntil).Round(time.Second)
		return "too many failed authentication attempts, try again in " + remaining.String()
	}

	if rl.config.MaxConnectionsPerIP > 0 && record.activeConns >= rl.config.MaxConnectionsPerIP {
		return "too many connections from your IP address"
	}

	return ""
}

// RegisterConnection records a new connection from the given address.
// Returns true if the connection is allowed, false otherwise.
func (rl *RateLimiter) RegisterConnection(addr net.Addr) bool {
	ip := extractIP(addr)
	if ip == "" {
		return true
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.config.MaxConnections > 0 && rl.totalActiveConns >= rl.config.MaxConnections {
		return false
	}

	record := rl.getOrCreateRecord(ip)

	if !record.bannedUntil.IsZero() && time.Now().Before(record.bannedUntil) {
		return false
	}

	if rl.config.MaxConnectionsPerIP > 0 && record.activeConns >= rl.config.MaxConnectionsPerIP {
		return false
	}

	record.activeConns++
	rl.totalActiveConns++
	return true
}

// UnregisterConnection decrements the active connection count for an IP.
func (rl *RateLimiter) UnregisterConnection(addr net.Addr) {
	ip := extractIP(addr)
	if ip == "" {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.totalActiveConns--
	if rl.totalActiveConns < 0 {
		rl.totalActiveConns = 0
	}

	if record, ok := rl.records[ip]; ok {
		record.activeConns--
		if record.activeConns < 0 {
			record.activeConns = 0
		}
	}
}

// RecordFailedAuth records a failed authentication attempt.
// Returns true if the IP is now banned.
func (rl *RateLimiter) RecordFailedAuth(addr net.Addr) bool {
	ip := extractIP(addr)
	if ip == "" {
		return false
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	record := rl.getOrCreateRecord(ip)
	now := time.Now()

	record.failedAttempts = append(record.failedAttempts, now)

	windowStart := now.Add(-rl.config.FailedAttemptWindow)
	recentAttempts := 0
	for _, t := range record.failedAttempts {
		if t.After(windowStart) {
			recentAttempts++
		}
	}

	if recentAttempts >= rl.config.MaxFailedAttempts {
		// Three cases: never banned (gauge++), already banned (no change —
		// already counted), or expired ban that cleanup hasn't cleared yet
		// (no change — still counted, will be decremented when cleanup runs).
		alreadyCounted := !record.bannedUntil.IsZero()
		record.bannedUntil = now.Add(rl.config.BanDuration)
		if !alreadyCounted {
			rateLimitedIPsGauge.Inc()
		}
		return true
	}

	return false
}

// RecordSuccessfulAuth clears failed attempts for an IP after successful auth.
func (rl *RateLimiter) RecordSuccessfulAuth(addr net.Addr) {
	ip := extractIP(addr)
	if ip == "" {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if record, ok := rl.records[ip]; ok {
		record.failedAttempts = nil
		if !record.bannedUntil.IsZero() {
			rateLimitedIPsGauge.Dec()
		}
		record.bannedUntil = time.Time{}
	}
}

// IsBanned checks if an IP is currently banned.
func (rl *RateLimiter) IsBanned(addr net.Addr) bool {
	ip := extractIP(addr)
	if ip == "" {
		return false
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	record, ok := rl.records[ip]
	if !ok {
		return false
	}

	return !record.bannedUntil.IsZero() && time.Now().Before(record.bannedUntil)
}

// getOrCreateRecord gets or creates a record for an IP (must hold lock).
func (rl *RateLimiter) getOrCreateRecord(ip string) *ipRecord {
	record, ok := rl.records[ip]
	if !ok {
		record = &ipRecord{}
		rl.records[ip] = record
	}
	return record
}

// cleanupLoop periodically cleans up expired records.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.cleanup()
	}
}

// cleanup removes expired records to prevent memory growth.
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-rl.config.FailedAttemptWindow)

	for ip, record := range rl.records {
		var validAttempts []time.Time
		for _, t := range record.failedAttempts {
			if t.After(windowStart) {
				validAttempts = append(validAttempts, t)
			}
		}
		record.failedAttempts = validAttempts

		if !record.bannedUntil.IsZero() && now.After(record.bannedUntil) {
			record.bannedUntil = time.Time{}
			rateLimitedIPsGauge.Dec()
		}

		if len(record.failedAttempts) == 0 &&
			record.bannedUntil.IsZero() &&
			record.activeConns == 0 {
			delete(rl.records, ip)
		}
	}
}
