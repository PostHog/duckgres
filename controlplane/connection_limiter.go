package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const defaultOrgConnectionPollInterval = 100 * time.Millisecond

var errRuntimeOrgConnectionExactAcquisitionUnsupported = errors.New("org connection store does not support exact-ref lease acquisition")

type connectionLease interface {
	Release(context.Context) error
}

type connectionAdmissionRequest struct {
	PID            int32
	Username       string
	Protocol       string
	RequestedVCPUs int
}

type connectionLimiter interface {
	Acquire(ctx context.Context, request connectionAdmissionRequest, limits func(string) configstore.OrgResourceLimits) (connectionLease, error)
}

type runtimeOrgConnectionStore interface {
	EnqueueOrgConnectionRequest(entry *configstore.OrgConnectionQueueEntry) error
}

type runtimeOrgConnectionLimitLookupStore interface {
	TryAcquireOrgConnectionLeaseWithLimitLookupForRef(ref configstore.OrgConnectionAdmissionRef, limits func(string) configstore.OrgResourceLimits, now time.Time) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionTryStore interface {
	TryAcquireOrgConnectionLeaseForRef(ref configstore.OrgConnectionAdmissionRef, limits configstore.OrgResourceLimits, now time.Time) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionEnqueueContextStore interface {
	EnqueueOrgConnectionRequestContext(ctx context.Context, entry *configstore.OrgConnectionQueueEntry) error
}

// runtimeOrgConnectionScheduleAndClaimStore is the optional authoritative
// scheduler handshake. Concrete stores that implement it own both global
// admission evaluation and claiming the caller's request. Compatibility stores
// may fall back to the exact-ref TryAcquire interfaces above; ID-only acquisition
// is deliberately unsupported because it cannot fence a reused request ID.
type runtimeOrgConnectionScheduleAndClaimStore interface {
	ScheduleAndClaimOrgConnectionLeaseForRef(ref configstore.OrgConnectionAdmissionRef) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionScheduleAndClaimContextStore interface {
	ScheduleAndClaimOrgConnectionLeaseForRefContext(ctx context.Context, ref configstore.OrgConnectionAdmissionRef) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionScheduleAndClaimEvaluationContextStore interface {
	ScheduleAndClaimOrgConnectionLeaseForRefWithEvaluationContext(ctx context.Context, ref configstore.OrgConnectionAdmissionRef) (*configstore.OrgConnectionLease, configstore.OrgConnectionAdmissionEvaluation, error)
}

type runtimeOrgConnectionLimiter struct {
	store        runtimeOrgConnectionStore
	reclaimer    admissionReclaimer
	orgID        string
	cpInstanceID string
	queueTTL     time.Duration
	pollInterval time.Duration
	now          func() time.Time
	newID        func() (string, error)
}

// NewRuntimeOrgConnectionLimiter builds the runtime vCPU admission gate. The
// optional form preserves source compatibility for older callers, but Acquire
// deliberately fails closed unless the control-plane-wide reclaimer is
// supplied; durable admission must never start without reserved cleanup
// ownership.
func NewRuntimeOrgConnectionLimiter(store runtimeOrgConnectionStore, orgID, cpInstanceID string, queueTTL time.Duration, reclaimers ...admissionReclaimer) connectionLimiter {
	if queueTTL <= 0 {
		queueTTL = 60 * time.Second
	}
	var reclaimer admissionReclaimer
	if len(reclaimers) > 0 {
		reclaimer = reclaimers[0]
	}
	return &runtimeOrgConnectionLimiter{
		store:        store,
		reclaimer:    reclaimer,
		orgID:        orgID,
		cpInstanceID: cpInstanceID,
		queueTTL:     queueTTL,
		pollInterval: defaultOrgConnectionPollInterval,
		now:          time.Now,
		newID:        randomConnectionRequestID,
	}
}

func (l *runtimeOrgConnectionLimiter) Acquire(ctx context.Context, request connectionAdmissionRequest, limits func(string) configstore.OrgResourceLimits) (connectionLease, error) {
	if l == nil || l.store == nil || limits == nil {
		return nil, nil
	}
	if l.reclaimer == nil {
		return nil, fmt.Errorf("org connection admission reclaimer is required")
	}
	requestID, err := l.newID()
	if err != nil {
		return nil, err
	}
	ref := configstore.OrgConnectionAdmissionRef{
		RequestID:    requestID,
		OrgID:        l.orgID,
		CPInstanceID: l.cpInstanceID,
	}
	enqueuedAt := l.now()
	expiresAt := enqueuedAt.Add(l.queueTTL)
	entry := &configstore.OrgConnectionQueueEntry{
		RequestID:      requestID,
		OrgID:          l.orgID,
		Username:       request.Username,
		CPInstanceID:   l.cpInstanceID,
		PID:            request.PID,
		Protocol:       request.Protocol,
		RequestedVCPUs: request.RequestedVCPUs,
		EnqueuedAt:     enqueuedAt,
		ExpiresAt:      expiresAt,
	}
	if err := ctx.Err(); err != nil {
		return nil, runtimeAdmissionContextError(err)
	}
	reservation, err := l.reclaimer.Reserve(ref)
	if err != nil {
		switch {
		case errors.Is(err, ErrAdmissionReclaimerFull):
			return nil, ErrTooManyConnections
		case errors.Is(err, ErrAdmissionReclaimerClosed):
			return nil, ErrSessionManagerDraining
		default:
			return nil, err
		}
	}
	cleanupArmed := true
	defer func() {
		if cleanupArmed {
			reservation.Reclaim(admissionReclaimCauseAcquireAbandoned)
		}
	}()
	var enqueueErr error
	if contextStore, ok := l.store.(runtimeOrgConnectionEnqueueContextStore); ok {
		enqueueErr = contextStore.EnqueueOrgConnectionRequestContext(ctx, entry)
	} else {
		enqueueErr = l.store.EnqueueOrgConnectionRequest(entry)
	}
	if enqueueErr != nil {
		if ctx.Err() != nil {
			return nil, runtimeAdmissionContextError(ctx.Err())
		}
		return nil, enqueueErr
	}

	waitStarted := time.Now()
	outcome := "error"
	var reasons sessionAdmissionReasons
	sessionAdmissionQueueDepthGauge.WithLabelValues(l.orgID).Inc()
	defer func() {
		sessionAdmissionQueueDepthGauge.WithLabelValues(l.orgID).Dec()
		observeSessionAdmissionTerminal(l.orgID, outcome, reasons.value(), time.Since(waitStarted))
	}()

	for {
		if err := ctx.Err(); err != nil {
			outcome = sessionAdmissionContextOutcome(err)
			return nil, runtimeAdmissionContextError(err)
		}
		now := l.now()
		if !now.Before(expiresAt) {
			outcome = "timeout"
			return nil, ErrTooManyConnections
		}

		var lease *configstore.OrgConnectionLease
		var evaluation configstore.OrgConnectionAdmissionEvaluation
		var err error
		if schedulerStore, ok := l.store.(runtimeOrgConnectionScheduleAndClaimEvaluationContextStore); ok {
			lease, evaluation, err = schedulerStore.ScheduleAndClaimOrgConnectionLeaseForRefWithEvaluationContext(ctx, ref)
		} else if schedulerStore, ok := l.store.(runtimeOrgConnectionScheduleAndClaimContextStore); ok {
			lease, err = schedulerStore.ScheduleAndClaimOrgConnectionLeaseForRefContext(ctx, ref)
		} else if schedulerStore, ok := l.store.(runtimeOrgConnectionScheduleAndClaimStore); ok {
			lease, err = schedulerStore.ScheduleAndClaimOrgConnectionLeaseForRef(ref)
		} else if lookupStore, ok := l.store.(runtimeOrgConnectionLimitLookupStore); ok {
			lease, err = lookupStore.TryAcquireOrgConnectionLeaseWithLimitLookupForRef(ref, limits, now)
		} else if tryStore, ok := l.store.(runtimeOrgConnectionTryStore); ok {
			lease, err = tryStore.TryAcquireOrgConnectionLeaseForRef(ref, limits(request.Username), now)
		} else {
			return nil, errRuntimeOrgConnectionExactAcquisitionUnsupported
		}
		if contextErr := runtimeAdmissionContextCause(ctx, err); contextErr != nil {
			// A completed blocking evaluation may race with cancellation. Retain
			// that real blocker, but never reinterpret an interrupted DB call as
			// a store failure on the logical request.
			if err == nil && evaluation.Decision != "canceled" && evaluation.Decision != "timeout" && evaluation.Decision != "error" {
				if evaluation.Decision == "waiting" && (evaluation.Reason == "" || evaluation.Reason == "none") {
					evaluation.Reason = "fifo"
				}
				reasons.add(evaluation.Reason)
			}
			outcome = sessionAdmissionContextOutcome(contextErr)
			return nil, runtimeAdmissionContextError(contextErr)
		}
		if evaluation.Decision == "waiting" && (evaluation.Reason == "" || evaluation.Reason == "none") {
			evaluation.Reason = "fifo"
		}
		reasons.add(evaluation.Reason)
		if err != nil {
			var rejection *configstore.OrgConnectionAdmissionRejectedError
			if errors.As(err, &rejection) {
				outcome = "rejected"
				reasons.add(sessionAdmissionRejectionReason(rejection))
				return nil, err
			}
			reasons.add("store_error")
			return nil, err
		}
		if lease != nil {
			outcome = "granted"
			sessionAdmissionActiveVCPUsGauge.WithLabelValues(l.orgID).Add(float64(request.RequestedVCPUs))
			cleanupArmed = false
			return &runtimeOrgConnectionLease{
				reservation:    reservation,
				orgID:          l.orgID,
				requestedVCPUs: request.RequestedVCPUs,
			}, nil
		}

		wait := l.pollInterval
		if remaining := expiresAt.Sub(now); remaining > 0 && remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			outcome = sessionAdmissionContextOutcome(ctx.Err())
			return nil, runtimeAdmissionContextError(ctx.Err())
		case <-timer.C:
		}
	}
}

func sessionAdmissionContextOutcome(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "canceled"
}

func runtimeAdmissionContextCause(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func sessionAdmissionRejectionReason(rejection *configstore.OrgConnectionAdmissionRejectedError) string {
	if rejection == nil {
		return "store_error"
	}
	switch rejection.Reason {
	case configstore.OrgConnectionAdmissionRejectedOrgVCPU:
		return "org_vcpu"
	case configstore.OrgConnectionAdmissionRejectedUserVCPU:
		return "user_vcpu"
	default:
		return "store_error"
	}
}

func runtimeAdmissionContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTooManyConnections
	}
	return err
}

type runtimeOrgConnectionLease struct {
	reservation    AdmissionReclaimReservation
	orgID          string
	requestedVCPUs int
	released       sync.Once
}

func (l *runtimeOrgConnectionLease) Release(context.Context) error {
	if l == nil || l.reservation == nil {
		return nil
	}
	l.released.Do(func() {
		l.reservation.Reclaim(admissionReclaimCauseLeaseRelease)
		sessionAdmissionActiveVCPUsGauge.WithLabelValues(l.orgID).Sub(float64(l.requestedVCPUs))
	})
	return nil
}

func randomConnectionRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate org connection request id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
