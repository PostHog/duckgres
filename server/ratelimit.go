package server

import (
	"net"
	"sync"
	"time"
)

// RateLimitConfig configures rate limiting behavior
type RateLimitConfig struct {
	// MaxFailedAttempts is the maximum number of failed auth attempts before banning
	MaxFailedAttempts int
	// FailedAttemptWindow is the time window for counting failed attempts
	FailedAttemptWindow time.Duration
	// BanDuration is how long to ban an IP after exceeding max failed attempts
	BanDuration time.Duration
	// MaxConnectionsPerIP is the max concurrent connections from a single IP (0 = unlimited)
	MaxConnectionsPerIP int
}

// DefaultRateLimitConfig returns sensible defaults for rate limiting
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		MaxFailedAttempts:   5,
		FailedAttemptWindow: 5 * time.Minute,
		BanDuration:         15 * time.Minute,
		MaxConnectionsPerIP: 100,
	}
}

// ipRecord tracks connection and authentication attempts from an IP
type ipRecord struct {
	failedAttempts []time.Time // timestamps of failed auth attempts
	bannedUntil    time.Time   // when the ban expires (zero if not banned)
	activeConns    int         // current active connections from this IP
}

// RateLimiter tracks and limits connections per IP
type RateLimiter struct {
	mu      sync.Mutex
	config  RateLimitConfig
	records map[string]*ipRecord
}

// NewRateLimiter creates a new rate limiter with the given config
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	rl := &RateLimiter{
		config:  cfg,
		records: make(map[string]*ipRecord),
	}
	// Start cleanup goroutine
	go rl.cleanupLoop()
	return rl
}

// extractIP gets the IP address from a net.Addr (strips port)
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

// CheckConnection checks if a connection from the given address should be allowed
// Returns an error message if the connection should be rejected, or empty string if allowed
func (rl *RateLimiter) CheckConnection(addr net.Addr) string {
	ip := extractIP(addr)
	if ip == "" {
		return ""
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	record := rl.getOrCreateRecord(ip)

	// Check if IP is banned
	if !record.bannedUntil.IsZero() && time.Now().Before(record.bannedUntil) {
		remaining := time.Until(record.bannedUntil).Round(time.Second)
		return "too many failed authentication attempts, try again in " + remaining.String()
	}

	// Check concurrent connection limit
	if rl.config.MaxConnectionsPerIP > 0 && record.activeConns >= rl.config.MaxConnectionsPerIP {
		return "too many connections from your IP address"
	}

	return ""
}

// RegisterConnection records a new connection from the given address
// Returns true if the connection is allowed, false otherwise
func (rl *RateLimiter) RegisterConnection(addr net.Addr) bool {
	ip := extractIP(addr)
	if ip == "" {
		return true
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	record := rl.getOrCreateRecord(ip)

	// Check if banned
	if !record.bannedUntil.IsZero() && time.Now().Before(record.bannedUntil) {
		return false
	}

	// Check concurrent connection limit
	if rl.config.MaxConnectionsPerIP > 0 && record.activeConns >= rl.config.MaxConnectionsPerIP {
		return false
	}

	record.activeConns++
	return true
}

// UnregisterConnection decrements the active connection count for an IP
func (rl *RateLimiter) UnregisterConnection(addr net.Addr) {
	ip := extractIP(addr)
	if ip == "" {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if record, ok := rl.records[ip]; ok {
		record.activeConns--
		if record.activeConns < 0 {
			record.activeConns = 0
		}
	}
}

// RecordFailedAuth records a failed authentication attempt
// Returns true if the IP is now banned
func (rl *RateLimiter) RecordFailedAuth(addr net.Addr) bool {
	ip := extractIP(addr)
	if ip == "" {
		return false
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	record := rl.getOrCreateRecord(ip)
	now := time.Now()

	// Add this failed attempt
	record.failedAttempts = append(record.failedAttempts, now)

	// Count recent failed attempts within the window
	windowStart := now.Add(-rl.config.FailedAttemptWindow)
	recentAttempts := 0
	for _, t := range record.failedAttempts {
		if t.After(windowStart) {
			recentAttempts++
		}
	}

	// Ban if exceeded threshold
	if recentAttempts >= rl.config.MaxFailedAttempts {
		record.bannedUntil = now.Add(rl.config.BanDuration)
		return true
	}

	return false
}

// RecordSuccessfulAuth clears failed attempts for an IP after successful auth
func (rl *RateLimiter) RecordSuccessfulAuth(addr net.Addr) {
	ip := extractIP(addr)
	if ip == "" {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if record, ok := rl.records[ip]; ok {
		record.failedAttempts = nil
		record.bannedUntil = time.Time{}
	}
}

// IsBanned checks if an IP is currently banned
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

// getOrCreateRecord gets or creates a record for an IP (must hold lock)
func (rl *RateLimiter) getOrCreateRecord(ip string) *ipRecord {
	record, ok := rl.records[ip]
	if !ok {
		record = &ipRecord{}
		rl.records[ip] = record
	}
	return record
}

// cleanupLoop periodically cleans up expired records
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.cleanup()
	}
}

// cleanup removes expired records to prevent memory growth
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-rl.config.FailedAttemptWindow)

	for ip, record := range rl.records {
		// Remove expired failed attempts
		var validAttempts []time.Time
		for _, t := range record.failedAttempts {
			if t.After(windowStart) {
				validAttempts = append(validAttempts, t)
			}
		}
		record.failedAttempts = validAttempts

		// Clear expired bans
		if !record.bannedUntil.IsZero() && now.After(record.bannedUntil) {
			record.bannedUntil = time.Time{}
		}

		// Remove record if it's empty and has no active connections
		if len(record.failedAttempts) == 0 &&
			record.bannedUntil.IsZero() &&
			record.activeConns == 0 {
			delete(rl.records, ip)
		}
	}
}
