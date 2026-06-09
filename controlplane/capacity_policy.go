package controlplane

import (
	"fmt"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type capacityClientMessageKind int

const (
	capacityMessageNoIdle capacityClientMessageKind = iota
	capacityMessageOrgCap
	capacityMessageGlobalCap
	capacityMessageShuttingDown
	capacityMessageGeneric
)

type capacityMissPolicy struct {
	reason              configstore.WorkerClaimMissReason
	recordDynamicDemand bool
	messageKind         capacityClientMessageKind
}

func capacityMissPolicyForReason(reason configstore.WorkerClaimMissReason) capacityMissPolicy {
	switch reason {
	case configstore.WorkerClaimMissReasonNone, configstore.WorkerClaimMissReasonNoIdle:
		return capacityMissPolicy{
			reason:              configstore.WorkerClaimMissReasonNoIdle,
			recordDynamicDemand: true,
			messageKind:         capacityMessageNoIdle,
		}
	case configstore.WorkerClaimMissReasonOrgCap:
		return capacityMissPolicy{reason: reason, messageKind: capacityMessageOrgCap}
	case configstore.WorkerClaimMissReasonGlobalCap:
		return capacityMissPolicy{reason: reason, messageKind: capacityMessageGlobalCap}
	case configstore.WorkerClaimMissReasonShuttingDown:
		return capacityMissPolicy{reason: reason, messageKind: capacityMessageShuttingDown}
	default:
		return capacityMissPolicy{reason: reason, messageKind: capacityMessageGeneric}
	}
}

func (p capacityMissPolicy) errorString(retryAfter time.Duration) string {
	switch p.messageKind {
	case capacityMessageNoIdle:
		return fmt.Sprintf("warm worker capacity exhausted; retry in about %s", normalizedCapacityRetryAfter(retryAfter).Round(time.Second))
	case capacityMessageOrgCap:
		return "warm worker capacity exhausted for organization"
	case capacityMessageGlobalCap:
		return "warm worker capacity exhausted by global pool limit"
	case capacityMessageShuttingDown:
		return "warm worker capacity unavailable while control plane is shutting down"
	default:
		return "warm worker capacity exhausted"
	}
}

func (p capacityMissPolicy) sqlMessage(retryAfter time.Duration) string {
	switch p.messageKind {
	case capacityMessageNoIdle:
		return fmt.Sprintf("no warm Duckgres worker is currently available; retry in about %d seconds", capacityRetrySeconds(retryAfter))
	case capacityMessageOrgCap:
		return "your organization has reached its maximum number of concurrent Duckgres workers and they are all busy; retry once a query finishes"
	case capacityMessageGlobalCap:
		return "Duckgres worker capacity is currently exhausted; retry later"
	case capacityMessageShuttingDown:
		return "Duckgres control plane is shutting down; retry later"
	default:
		return "Duckgres worker capacity is currently unavailable; retry later"
	}
}

func normalizedCapacityRetryAfter(retryAfter time.Duration) time.Duration {
	if retryAfter <= 0 {
		return DefaultWorkerSpawnRetryAfter
	}
	return retryAfter
}

func capacityRetrySeconds(retryAfter time.Duration) int {
	return int((normalizedCapacityRetryAfter(retryAfter) + time.Second - 1) / time.Second)
}
