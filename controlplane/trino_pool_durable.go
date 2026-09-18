//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

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

	if _, err := o.operations.BeginTrinoPoolOperation(ctx, o.lease, operation); err != nil {
		return o.dropAuthority(fmt.Errorf("record intent for %s: %w", operation.OperationID, err))
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
	return effectErr
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
