package server

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

// retryOnTransientAttach retries a DuckLake ATTACH operation on transient errors.
func retryOnTransientAttach(fn func() error) error {
	err := fn()
	if err == nil || !isTransientDuckLakeError(err) {
		return err
	}

	backoff := transientInitialBackoff
	for attempt := 1; attempt <= transientMaxRetries; attempt++ {
		slog.Warn("Transient DuckLake error during attach, retrying.",
			"attempt", attempt, "max_retries", transientMaxRetries,
			"backoff", backoff, "error", err)

		time.Sleep(backoff)
		backoff *= 2

		err = fn()
		if err == nil || !isTransientDuckLakeError(err) {
			if err == nil {
				slog.Info("DuckLake attach retry succeeded.", "attempt", attempt)
			}
			return err
		}
	}

	slog.Error("DuckLake attach retries exhausted.", "attempts", transientMaxRetries+1, "error", err)
	return err
}
