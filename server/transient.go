package server

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
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
		strings.Contains(msg, "SSL connection has been closed unexpectedly") ||
		strings.Contains(msg, "Current transaction is aborted") ||
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

// RecoverAbortedTransaction rolls back and retries once when a DuckLake-backed
// connection is stuck in "Current transaction is aborted" state. This is only
// safe when the caller owns the transaction lifecycle (autocommit / no active
// user transaction). Callers should pass canRollback=false for explicit user
// transactions so the original error is surfaced unchanged.
func RecoverAbortedTransaction[T any](
	err error,
	canRollback bool,
	rollback func() error,
	retry func() (T, error),
) (T, error, bool) {
	var zero T
	if err == nil || !canRollback || !isTransactionAborted(err) {
		return zero, err, false
	}

	slog.Warn("DuckLake connection hit aborted transaction state; issuing ROLLBACK before retry.", "error", err)
	if rollbackErr := rollback(); rollbackErr != nil {
		return zero, fmt.Errorf("DuckLake aborted transaction recovery rollback failed: %w (original error: %v)", rollbackErr, err), true
	}

	result, retryErr := retry()
	if retryErr == nil {
		slog.Info("DuckLake aborted transaction recovery succeeded.")
	} else {
		slog.Warn("DuckLake aborted transaction recovery retry failed.", "error", retryErr)
	}
	return result, retryErr, true
}

func recoverAbortedTransaction[T any](
	err error,
	canRollback bool,
	rollback func() error,
	retry func() (T, error),
) (T, error, bool) {
	return RecoverAbortedTransaction(err, canRollback, rollback, retry)
}

const (
	conflictMaxRetries     = 5
	conflictInitialBackoff = 50 * time.Millisecond
	conflictMaxBackoff     = 2 * time.Second
)

// retryOnConflict retries fn on DuckLake transaction conflicts with
// exponential backoff and jitter (50-100% of backoff interval).
// Only used for autocommit queries — user-managed transactions propagate
// the error since the entire transaction is invalid after a conflict.
func retryOnConflict[T any](fn func() (T, error)) (T, error) {
	var lastErr error
	backoff := conflictInitialBackoff
	for attempt := 1; attempt <= conflictMaxRetries; attempt++ {
		ducklakeConflictRetriesTotal.Inc()

		jittered := time.Duration(float64(backoff) * (0.5 + rand.Float64()*0.5))
		slog.Warn("DuckLake transaction conflict, retrying.",
			"attempt", attempt, "max_retries", conflictMaxRetries,
			"backoff", jittered)

		time.Sleep(jittered)

		result, err := fn()
		if err == nil {
			ducklakeConflictRetrySuccessesTotal.Inc()
			slog.Info("DuckLake conflict retry succeeded.", "attempt", attempt)
			return result, nil
		}
		lastErr = err
		if !isDuckLakeTransactionConflict(err) {
			return result, err
		}

		backoff *= 2
		if backoff > conflictMaxBackoff {
			backoff = conflictMaxBackoff
		}
	}

	ducklakeConflictRetriesExhaustedTotal.Inc()
	var zero T
	slog.Error("DuckLake conflict retries exhausted.", "attempts", conflictMaxRetries, "error", lastErr)
	return zero, fmt.Errorf("DuckLake transaction conflict: retries exhausted after %d attempts: %w", conflictMaxRetries, lastErr)
}
