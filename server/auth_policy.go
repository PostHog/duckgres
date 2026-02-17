package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"net"
)

const invalidPasswordSentinel = "__duckgres_invalid_password_sentinel__"

// BeginRateLimitedAuthAttempt enforces rate-limit policy before an auth attempt.
// The returned release function must be called once the attempt is complete.
func BeginRateLimitedAuthAttempt(rateLimiter *RateLimiter, remoteAddr net.Addr) (release func(), rejectReason string) {
	release = func() {}
	if rateLimiter == nil {
		return release, ""
	}

	if msg := rateLimiter.CheckConnection(remoteAddr); msg != "" {
		rateLimitRejectsCounter.Inc()
		return release, msg
	}
	if !rateLimiter.RegisterConnection(remoteAddr) {
		rateLimitRejectsCounter.Inc()
		if msg := rateLimiter.CheckConnection(remoteAddr); msg != "" {
			return release, msg
		}
		return release, "too many connections from your IP address"
	}

	return func() {
		rateLimiter.UnregisterConnection(remoteAddr)
	}, ""
}

// RecordFailedAuthAttempt records auth telemetry and updates rate-limit state.
// Returns true when this failure causes the source IP to be banned.
func RecordFailedAuthAttempt(rateLimiter *RateLimiter, remoteAddr net.Addr) bool {
	authFailuresCounter.Inc()
	if rateLimiter == nil {
		return false
	}
	return rateLimiter.RecordFailedAuth(remoteAddr)
}

// RecordSuccessfulAuthAttempt clears failure tracking after successful auth.
func RecordSuccessfulAuthAttempt(rateLimiter *RateLimiter, remoteAddr net.Addr) {
	if rateLimiter == nil {
		return
	}
	rateLimiter.RecordSuccessfulAuth(remoteAddr)
}

// ValidateUserPassword validates username/password without leaking user existence
// via credential-compare timing differences.
func ValidateUserPassword(users map[string]string, username, password string) bool {
	expectedPassword, userFound := users[username]
	if !userFound {
		expectedPassword = invalidPasswordSentinel
	}

	givenHash := sha256.Sum256([]byte(password))
	expectedHash := sha256.Sum256([]byte(expectedPassword))
	passwordMatches := subtle.ConstantTimeCompare(givenHash[:], expectedHash[:]) == 1
	return userFound && passwordMatches
}
