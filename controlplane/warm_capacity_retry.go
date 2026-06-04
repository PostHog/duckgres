package controlplane

import (
	"errors"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// isRetryableWarmMiss reports whether a worker-acquire error is a transient
// no-idle warm-worker miss. Org/global-cap and shutdown misses are not retried.
func isRetryableWarmMiss(err error) bool {
	var capErr *WarmCapacityExhaustedError
	if !errors.As(err, &capErr) {
		return false
	}
	return capErr.missReason() == configstore.WorkerClaimMissReasonNoIdle
}
