//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
)

// Durable operations around external effects.
//
// Every Gateway mutation is preceded by a recorded intent and followed by a
// recorded outcome. That is what makes a LOST response recoverable: the next
// attempt - possibly by a different leader - reads the operation back under the
// same identity instead of guessing whether the effect happened.
//
// The Gateway has its own replay guard, so a repeated identical call is already
// safe there. What it cannot do is tell THIS controller what a previous leader
// attempted, which is why the record lives here too.

// trinoPoolOperationStore is the durable-operation surface the operator uses.
type trinoPoolOperationStore interface {
	BeginTrinoPoolOperation(context.Context, configstore.TrinoPoolLease, configstore.TrinoPoolOperationSpec) (configstore.TrinoPoolOperation, error)
	RecordTrinoPoolOperationStep(ctx context.Context, lease configstore.TrinoPoolLease, operationID, stepID, payloadHash, outcome, result string) (configstore.TrinoPoolOperationStep, error)
	FinishTrinoPoolOperation(ctx context.Context, lease configstore.TrinoPoolLease, operationID, phase, lastError string) error
	UpdateTrinoPoolOperation(ctx context.Context, lease configstore.TrinoPoolLease, operationID string, updates map[string]any) error
}

// Durable retry pacing. A failing external call is retried on a schedule the
// DATABASE holds, not the leader's memory: a restart or a leadership move would
// otherwise reset every backoff to zero and turn a persistent failure into a
// hot loop against the Gateway.
const (
	trinoPoolRetryBase = 500 * time.Millisecond
	trinoPoolRetryMax  = 30 * time.Second
)

// errTrinoPoolBackoff means "not yet" - the operation has a recorded next
// attempt in the future. It is not a failure: nothing was attempted, and the
// reconcile loop treats it as a quiet no-op rather than an error to alert on.
var errTrinoPoolBackoff = errors.New("trino pool operation is waiting for its next attempt")

// trinoPoolRetryDelay is full jitter over an exponential backoff: every attempt
// waits a random duration up to the exponential bound, so several controllers
// retrying the same class of failure do not synchronize into bursts.
func trinoPoolRetryDelay(attempts int64) time.Duration {
	bound := trinoPoolRetryBase
	for i := int64(0); i < attempts && bound < trinoPoolRetryMax; i++ {
		bound *= 2
	}
	if bound > trinoPoolRetryMax {
		bound = trinoPoolRetryMax
	}
	return time.Duration(rand.Int64N(int64(bound)) + int64(trinoPoolRetryBase))
}

// Recorded step outcomes.
const (
	trinoPoolStepOK      = configstore.TrinoPoolStepOutcomeOK
	trinoPoolStepUnknown = configstore.TrinoPoolStepOutcomeUnknown
	trinoPoolStepFailed  = configstore.TrinoPoolStepOutcomeFailed
)

// runDurableStep records the intent, performs the effect, and records what came
// back.
//
// The three outcomes are deliberately distinct:
//
//   - OK: the Gateway answered. The recorded result is the answer.
//   - FAILED: the Gateway REFUSED. That is a decision; retrying cannot change
//     it, and recording it as unknown would invite exactly that retry.
//   - UNKNOWN: no answer arrived. The effect may or may not have happened, so
//     the step stays open and the next attempt resolves it by read-back under
//     the same identity rather than by repeating blind.
func (o *trinoPoolOperator) runDurableStep(
	ctx context.Context,
	operation configstore.TrinoPoolOperationSpec,
	stepID string,
	payload any,
	effect func(context.Context) (string, error),
) error {
	if o.operations == nil {
		// Durable recording is unavailable (tests, or a store that does not
		// implement it). The effect still runs: refusing to act because the
		// journal is missing would be a worse failure than acting unrecorded.
		_, err := effect(ctx)
		return err
	}

	recordedOperation, err := o.operations.BeginTrinoPoolOperation(ctx, o.lease, operation)
	if err != nil {
		return o.dropAuthority(fmt.Errorf("record intent for %s: %w", operation.OperationID, err))
	}
	if recordedOperation.NextAttemptAt != nil && time.Now().UTC().Before(*recordedOperation.NextAttemptAt) {
		// A previous attempt failed and the wait it earned has not elapsed.
		// Retrying now would hammer whatever refused it, and the schedule is
		// durable precisely so a restart cannot skip it.
		return fmt.Errorf("%w: %s until %s", errTrinoPoolBackoff,
			operation.OperationID, recordedOperation.NextAttemptAt.Format(time.RFC3339))
	}

	hash := trinoPoolPayloadHash(payload)
	recorded, err := o.operations.RecordTrinoPoolOperationStep(ctx, o.lease,
		operation.OperationID, stepID, hash, trinoPoolStepUnknown, "{}")
	if err != nil {
		if errors.Is(err, configstore.ErrTrinoPoolIntentChanged) {
			// The same step id was already recorded with different content.
			// Performing this effect would apply an intent nobody recorded.
			return fmt.Errorf("step %s of %s was recorded with different content: %w",
				stepID, operation.OperationID, err)
		}
		return o.dropAuthority(fmt.Errorf("record step %s: %w", stepID, err))
	}
	if recorded.Replayed && recorded.Outcome == trinoPoolStepOK {
		// A previous attempt - possibly by another leader - already completed
		// this step. Repeating the effect is unnecessary; the Gateway would
		// replay it anyway, but not calling at all is cheaper and clearer.
		slog.Debug("Trino pool step already completed.",
			"pool", o.config.PublicID, "operation", operation.OperationID, "step", stepID)
		return nil
	}

	result, effectErr := effect(ctx)
	outcome := trinoPoolStepOK
	if effectErr != nil {
		outcome = trinoPoolStepUnknown
		if trinoPoolDecided(effectErr) {
			outcome = trinoPoolStepFailed
		}
	}
	if _, err := o.operations.RecordTrinoPoolOperationStep(ctx, o.lease,
		operation.OperationID, stepID, hash, outcome, result); err != nil &&
		!errors.Is(err, configstore.ErrTrinoPoolIntentChanged) {
		slog.Warn("Trino pool step outcome could not be recorded.",
			"pool", o.config.PublicID, "operation", operation.OperationID, "step", stepID, "error", err)
	}
	o.recordAttempt(ctx, operation.OperationID, recordedOperation.Attempts, effectErr)
	return effectErr
}

// recordAttempt persists the retry schedule, or closes the operation when the
// effect succeeded.
//
// Both halves matter. Without the schedule, `attempts` and `next_attempt_at`
// stay untouched and every failing call is retried on every tick forever; with
// no terminal marker, the operations table only grows and nothing can
// distinguish work in flight from work that finished.
func (o *trinoPoolOperator) recordAttempt(ctx context.Context, operationID string, attempts int64, effectErr error) {
	if effectErr == nil {
		if err := o.operations.FinishTrinoPoolOperation(ctx, o.lease, operationID, "completed", ""); err != nil &&
			!errors.Is(err, configstore.ErrTrinoPoolConflict) {
			slog.Warn("Trino pool operation could not be closed.",
				"pool", o.config.PublicID, "operation", operationID, "error", err)
		}
		return
	}
	next := time.Now().UTC().Add(trinoPoolRetryDelay(attempts))
	if err := o.operations.UpdateTrinoPoolOperation(ctx, o.lease, operationID, map[string]any{
		"attempts":        attempts + 1,
		"next_attempt_at": next,
		"last_error":      effectErr.Error(),
	}); err != nil && !errors.Is(err, configstore.ErrTrinoPoolConflict) {
		slog.Warn("Trino pool retry schedule could not be recorded.",
			"pool", o.config.PublicID, "operation", operationID, "error", err)
	}
}

// trinoPoolDecided reports whether the Gateway made a decision, as opposed to
// never answering. A decision is terminal for the step; an unanswered call is
// not, and the difference is what keeps a retry loop from hiding a refusal.
func trinoPoolDecided(err error) bool {
	var gatewayError *trinogateway.Error
	return errors.As(err, &gatewayError)
}

// trinoPoolPayloadHash identifies a step's intent, so a replay with different
// content is recognizable as a conflict rather than applied silently.
func trinoPoolPayloadHash(payload any) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%#v", payload)))
	return hex.EncodeToString(digest[:])
}
