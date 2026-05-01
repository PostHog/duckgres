package server

import "github.com/posthog/duckgres/server/auth"

// Type aliases and re-exports kept here so existing references to
// server.RateLimiter / server.RateLimitConfig / server.NewRateLimiter etc.
// continue to compile after the rate-limit and auth-policy code moved into
// server/auth. New code should import server/auth and use auth.X directly.

type (
	RateLimiter     = auth.RateLimiter
	RateLimitConfig = auth.RateLimitConfig
)

var (
	NewRateLimiter             = auth.NewRateLimiter
	DefaultRateLimitConfig     = auth.DefaultRateLimitConfig
	BeginRateLimitedAuthAttempt = auth.BeginRateLimitedAuthAttempt
	RecordFailedAuthAttempt    = auth.RecordFailedAuthAttempt
	RecordSuccessfulAuthAttempt = auth.RecordSuccessfulAuthAttempt
	ValidateUserPassword       = auth.ValidateUserPassword
)
