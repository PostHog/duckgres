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

const defaultOrgConnectionPollInterval = 100 * time.Millisecond

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
	if err := l.store.EnqueueOrgConnectionRequest(&configstore.OrgConnectionQueueEntry{
		RequestID:      requestID,
		OrgID:          l.orgID,
		Username:       request.Username,
		CPInstanceID:   l.cpInstanceID,
		PID:            request.PID,
		Protocol:       request.Protocol,
		RequestedVCPUs: request.RequestedVCPUs,
		EnqueuedAt:     enqueuedAt,
		ExpiresAt:      expiresAt,
	}); err != nil {
		return nil, err
	}

	for {
		now := l.now()
		if !now.Before(expiresAt) {
			if err := l.store.CancelOrgConnectionRequest(requestID, now); err != nil {
				slog.Warn("Failed to cancel expired org connection request.", "org", l.orgID, "request_id", requestID, "error", err)
			}
			return nil, ErrTooManyConnections
		}

		var lease *configstore.OrgConnectionLease
		var err error
		if lookupStore, ok := l.store.(runtimeOrgConnectionLimitLookupStore); ok {
			lease, err = lookupStore.TryAcquireOrgConnectionLeaseWithLimitLookup(requestID, limits, now)
		} else {
			lease, err = l.store.TryAcquireOrgConnectionLease(requestID, limits(request.Username), now)
		}
		if err != nil {
			if cancelErr := l.store.CancelOrgConnectionRequest(requestID, l.now()); cancelErr != nil {
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
			if err := l.store.CancelOrgConnectionRequest(requestID, l.now()); err != nil {
				slog.Warn("Failed to cancel interrupted org connection request.", "org", l.orgID, "request_id", requestID, "error", err)
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, ErrTooManyConnections
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type runtimeOrgConnectionLease struct {
	store   runtimeOrgConnectionStore
	leaseID string
}

func (l *runtimeOrgConnectionLease) Release(context.Context) error {
	if l == nil || l.store == nil {
		return nil
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
