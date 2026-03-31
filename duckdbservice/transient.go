package duckdbservice

import (
	"log/slog"
	"strings"
	"time"
)

// isTransientDuckLakeError returns true if the error message indicates a
// transient failure that is likely to succeed on retry. This covers DNS
// resolution failures and TCP connection errors to the DuckLake metadata store.
func isTransientDuckLakeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "name resolution") ||
		strings.Contains(msg, "could not translate host name") ||
		strings.Contains(msg, "could not connect to server") ||
		strings.Contains(msg, "Connection refused") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "server closed the connection unexpectedly") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable")
}

const (
	transientMaxRetries     = 3
	transientInitialBackoff = 250 * time.Millisecond
)

// retryOnTransient calls fn up to transientMaxRetries additional times when it
// returns a transient DuckLake error, using exponential backoff.
func retryOnTransient[T any](fn func() (T, error)) (T, error) {
	result, err := fn()
	if err == nil || !isTransientDuckLakeError(err) {
		return result, err
	}

	backoff := transientInitialBackoff
	for attempt := 1; attempt <= transientMaxRetries; attempt++ {
		slog.Warn("Transient DuckLake error, retrying.",
			"attempt", attempt, "max_retries", transientMaxRetries,
			"backoff", backoff, "error", err)

		time.Sleep(backoff)
		backoff *= 2

		result, err = fn()
		if err == nil || !isTransientDuckLakeError(err) {
			if err == nil {
				slog.Info("DuckLake retry succeeded.", "attempt", attempt)
			}
			return result, err
		}
	}

	slog.Error("DuckLake retries exhausted.", "attempts", transientMaxRetries+1, "error", err)
	return result, err
}
