package controlplane

import (
	"fmt"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type warmCapacityClientMessageKind int

const (
	warmCapacityMessageNoIdle warmCapacityClientMessageKind = iota
	warmCapacityMessageOrgCap
	warmCapacityMessageGlobalCap
	warmCapacityMessageShuttingDown
	warmCapacityMessageGeneric
)

type warmCapacityMissPolicy struct {
	reason              configstore.WorkerClaimMissReason
	recordDynamicDemand bool
	messageKind         warmCapacityClientMessageKind
}

func warmCapacityMissPolicyForReason(reason configstore.WorkerClaimMissReason) warmCapacityMissPolicy {
	switch reason {
	case configstore.WorkerClaimMissReasonNone, configstore.WorkerClaimMissReasonNoIdle:
		return warmCapacityMissPolicy{
			reason:              configstore.WorkerClaimMissReasonNoIdle,
			recordDynamicDemand: true,
			messageKind:         warmCapacityMessageNoIdle,
		}
	case configstore.WorkerClaimMissReasonOrgCap:
		return warmCapacityMissPolicy{reason: reason, messageKind: warmCapacityMessageOrgCap}
	case configstore.WorkerClaimMissReasonGlobalCap:
		return warmCapacityMissPolicy{reason: reason, messageKind: warmCapacityMessageGlobalCap}
	case configstore.WorkerClaimMissReasonShuttingDown:
		return warmCapacityMissPolicy{reason: reason, messageKind: warmCapacityMessageShuttingDown}
	default:
		return warmCapacityMissPolicy{reason: reason, messageKind: warmCapacityMessageGeneric}
	}
}

func (p warmCapacityMissPolicy) errorString(retryAfter time.Duration) string {
	switch p.messageKind {
	case warmCapacityMessageNoIdle:
		return fmt.Sprintf("warm worker capacity exhausted; retry in about %s", normalizedWarmCapacityRetryAfter(retryAfter).Round(time.Second))
	case warmCapacityMessageOrgCap:
		return "warm worker capacity exhausted for organization"
	case warmCapacityMessageGlobalCap:
		return "warm worker capacity exhausted by global pool limit"
	case warmCapacityMessageShuttingDown:
		return "warm worker capacity unavailable while control plane is shutting down"
	default:
		return "warm worker capacity exhausted"
	}
}

func (p warmCapacityMissPolicy) sqlMessage(retryAfter time.Duration) string {
	switch p.messageKind {
	case warmCapacityMessageNoIdle:
		return fmt.Sprintf("no warm Duckgres worker is currently available; retry in about %d seconds", warmCapacityRetrySeconds(retryAfter))
	case warmCapacityMessageOrgCap:
		return "Duckgres worker capacity for this organization is currently exhausted; retry later"
	case warmCapacityMessageGlobalCap:
		return "Duckgres worker capacity is currently exhausted; retry later"
	case warmCapacityMessageShuttingDown:
		return "Duckgres control plane is shutting down; retry later"
	default:
		return "Duckgres worker capacity is currently unavailable; retry later"
	}
}

func normalizedWarmCapacityRetryAfter(retryAfter time.Duration) time.Duration {
	if retryAfter <= 0 {
		return DefaultWarmCapacityRetryAfter
	}
	return retryAfter
}

func warmCapacityRetrySeconds(retryAfter time.Duration) int {
	return int((normalizedWarmCapacityRetryAfter(retryAfter) + time.Second - 1) / time.Second)
}
