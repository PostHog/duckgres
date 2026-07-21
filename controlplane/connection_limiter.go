package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const (
	defaultOrgConnectionPollInterval = 100 * time.Millisecond
	orgConnectionCleanupTimeout      = 5 * time.Second
)

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
	TryAcquireOrgConnectionLease(requestID string, limits configstore.OrgResourceLimits, now time.Time) (*configstore.OrgConnectionLease, error)
	ReleaseOrgConnectionLease(leaseID string) error
	CancelOrgConnectionRequest(requestID string, canceledAt time.Time) error
}

type runtimeOrgConnectionLimitLookupStore interface {
	TryAcquireOrgConnectionLeaseWithLimitLookup(requestID string, limits func(string) configstore.OrgResourceLimits, now time.Time) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionContextStore interface {
	EnqueueOrgConnectionRequestContext(ctx context.Context, entry *configstore.OrgConnectionQueueEntry) error
	CancelOrgConnectionRequestContext(ctx context.Context, requestID string, canceledAt time.Time) error
	ReleaseOrgConnectionLeaseContext(ctx context.Context, leaseID string) error
}

// runtimeOrgConnectionScheduleAndClaimStore is the optional authoritative
// scheduler handshake. Concrete stores that implement it own both global
// admission evaluation and claiming the caller's request; older stores and
// test fakes continue through the legacy TryAcquire paths below.
type runtimeOrgConnectionScheduleAndClaimStore interface {
	ScheduleAndClaimOrgConnectionLease(requestID, cpInstanceID string) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionScheduleAndClaimContextStore interface {
	ScheduleAndClaimOrgConnectionLeaseContext(ctx context.Context, requestID, cpInstanceID string) (*configstore.OrgConnectionLease, error)
}

type runtimeOrgConnectionLimiter struct {
	store        runtimeOrgConnectionStore
	orgID        string
	cpInstanceID string
	queueTTL     time.Duration
	pollInterval time.Duration
	now          func() time.Time
	newID        func() (string, error)
}

func NewRuntimeOrgConnectionLimiter(store runtimeOrgConnectionStore, orgID, cpInstanceID string, queueTTL time.Duration) connectionLimiter {
	if queueTTL <= 0 {
		queueTTL = 60 * time.Second
	}
	return &runtimeOrgConnectionLimiter{
		store:        store,
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
	requestID, err := l.newID()
	if err != nil {
		return nil, err
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
	var enqueueErr error
	if contextStore, ok := l.store.(runtimeOrgConnectionContextStore); ok {
		enqueueErr = contextStore.EnqueueOrgConnectionRequestContext(ctx, entry)
	} else {
		enqueueErr = l.store.EnqueueOrgConnectionRequest(entry)
	}
	if enqueueErr != nil {
		// A canceled database call can be ambiguous at the network boundary.
		// Reclaim by request ID with a fresh bounded context; durable expiry is
		// the fallback if cleanup cannot acquire the org lock in time.
		if cancelErr := l.cancelRequest(requestID, l.now()); cancelErr != nil {
			slog.Warn("Failed to cancel errored org connection enqueue.", "org", l.orgID, "request_id", requestID, "error", cancelErr)
		}
		if ctx.Err() != nil {
			return nil, runtimeAdmissionContextError(ctx.Err())
		}
		return nil, enqueueErr
	}

	for {
		now := l.now()
		if !now.Before(expiresAt) {
			if err := l.cancelRequest(requestID, now); err != nil {
				slog.Warn("Failed to cancel expired org connection request.", "org", l.orgID, "request_id", requestID, "error", err)
			}
			return nil, ErrTooManyConnections
		}

		var lease *configstore.OrgConnectionLease
		var err error
		if schedulerStore, ok := l.store.(runtimeOrgConnectionScheduleAndClaimContextStore); ok {
			lease, err = schedulerStore.ScheduleAndClaimOrgConnectionLeaseContext(ctx, requestID, l.cpInstanceID)
		} else if schedulerStore, ok := l.store.(runtimeOrgConnectionScheduleAndClaimStore); ok {
			lease, err = schedulerStore.ScheduleAndClaimOrgConnectionLease(requestID, l.cpInstanceID)
		} else if lookupStore, ok := l.store.(runtimeOrgConnectionLimitLookupStore); ok {
			lease, err = lookupStore.TryAcquireOrgConnectionLeaseWithLimitLookup(requestID, limits, now)
		} else {
			lease, err = l.store.TryAcquireOrgConnectionLease(requestID, limits(request.Username), now)
		}
		if ctx.Err() != nil {
			if cancelErr := l.cancelRequest(requestID, l.now()); cancelErr != nil {
				slog.Warn("Failed to cancel interrupted org connection request.", "org", l.orgID, "request_id", requestID, "error", cancelErr)
			}
			return nil, runtimeAdmissionContextError(ctx.Err())
		}
		if err != nil {
			if cancelErr := l.cancelRequest(requestID, l.now()); cancelErr != nil {
				slog.Warn("Failed to cancel errored org connection request.", "org", l.orgID, "request_id", requestID, "error", cancelErr)
			}
			return nil, err
		}
		if lease != nil {
			return &runtimeOrgConnectionLease{store: l.store, leaseID: lease.LeaseID}, nil
		}

		wait := l.pollInterval
		if remaining := expiresAt.Sub(now); remaining > 0 && remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			if err := l.cancelRequest(requestID, l.now()); err != nil {
				slog.Warn("Failed to cancel interrupted org connection request.", "org", l.orgID, "request_id", requestID, "error", err)
			}
			return nil, runtimeAdmissionContextError(ctx.Err())
		case <-timer.C:
		}
	}
}

func runtimeAdmissionContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTooManyConnections
	}
	return err
}

func (l *runtimeOrgConnectionLimiter) cancelRequest(requestID string, canceledAt time.Time) error {
	if contextStore, ok := l.store.(runtimeOrgConnectionContextStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), orgConnectionCleanupTimeout)
		defer cancel()
		return contextStore.CancelOrgConnectionRequestContext(ctx, requestID, canceledAt)
	}
	return l.store.CancelOrgConnectionRequest(requestID, canceledAt)
}

type runtimeOrgConnectionLease struct {
	store   runtimeOrgConnectionStore
	leaseID string
}

func (l *runtimeOrgConnectionLease) Release(ctx context.Context) error {
	if l == nil || l.store == nil {
		return nil
	}
	if contextStore, ok := l.store.(runtimeOrgConnectionContextStore); ok {
		return contextStore.ReleaseOrgConnectionLeaseContext(ctx, l.leaseID)
	}
	return l.store.ReleaseOrgConnectionLease(l.leaseID)
}

func randomConnectionRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate org connection request id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
