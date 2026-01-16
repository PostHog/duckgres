package server

import (
	"context"
	"fmt"
	"time"
)

// RetryWithBackoff executes a function with exponential backoff on failure.
// Used for handling transient SSL/network errors when querying DuckLake catalog.
// Backoff schedule: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s...
func RetryWithBackoff(ctx context.Context, maxRetries int, fn func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		// Check if context was cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Exponential backoff: 100ms * 2^i, max 5 seconds
		delay := time.Duration(100<<uint(i)) * time.Millisecond
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("after %d retries: %w", maxRetries, err)
}

// RetryWithBackoffNoContext is a convenience wrapper for RetryWithBackoff
// that doesn't require a context (uses background context).
func RetryWithBackoffNoContext(maxRetries int, fn func() error) error {
	return RetryWithBackoff(context.Background(), maxRetries, fn)
}
