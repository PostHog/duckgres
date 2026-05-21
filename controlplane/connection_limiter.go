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

type connectionLimiter interface {
	Acquire(ctx context.Context, pid int32, protocol string, maxConnections func() int) (connectionLease, error)
}

type runtimeOrgConnectionStore interface {
	EnqueueOrgConnectionRequest(entry *configstore.OrgConnectionQueueEntry) error
	TryAcquireOrgConnectionLease(requestID string, maxConnections int, now time.Time) (*configstore.OrgConnectionLease, error)
	ReleaseOrgConnectionLease(leaseID string) error
	CancelOrgConnectionRequest(requestID string, canceledAt time.Time) error
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

func (l *runtimeOrgConnectionLimiter) Acquire(ctx context.Context, pid int32, protocol string, maxConnections func() int) (connectionLease, error) {
	if l == nil || l.store == nil || maxConnections == nil {
		return nil, nil
	}
	requestID, err := l.newID()
	if err != nil {
		return nil, err
	}
	enqueuedAt := l.now()
	expiresAt := enqueuedAt.Add(l.queueTTL)
	if err := l.store.EnqueueOrgConnectionRequest(&configstore.OrgConnectionQueueEntry{
		RequestID:    requestID,
		OrgID:        l.orgID,
		CPInstanceID: l.cpInstanceID,
		PID:          pid,
		Protocol:     protocol,
		EnqueuedAt:   enqueuedAt,
		ExpiresAt:    expiresAt,
	}); err != nil {
		return nil, err
	}

	for {
		now := l.now()
		currentMaxConnections := maxConnections()
		if !now.Before(expiresAt) {
			if err := l.store.CancelOrgConnectionRequest(requestID, now); err != nil {
				slog.Warn("Failed to cancel expired org connection request.", "org", l.orgID, "request_id", requestID, "error", err)
			}
			return nil, ErrTooManyConnections
		}

		lease, err := l.store.TryAcquireOrgConnectionLease(requestID, currentMaxConnections, now)
		if err != nil {
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
