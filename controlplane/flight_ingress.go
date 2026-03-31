package controlplane

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightsqlingress"
)

type FlightIngressConfig = flightsqlingress.Config

type FlightIngress = flightsqlingress.FlightIngress

// NewFlightIngress creates a control-plane Flight SQL ingress listener.
func NewFlightIngress(host string, port int, tlsConfig *tls.Config, validator flightsqlingress.CredentialValidator, provider flightsqlingress.SessionProvider, rateLimiter *server.RateLimiter, cfg FlightIngressConfig) (*FlightIngress, error) {
	return flightsqlingress.NewFlightIngress(host, port, tlsConfig, validator, provider, cfg, flightsqlingress.Options{
		RateLimiter: rateLimiter,
		Hooks: flightsqlingress.Hooks{
			OnSessionCountChanged: observeFlightAuthSessions,
			OnSessionsReaped:      observeFlightSessionsReaped,
		},
	})
}

// flightSessionProvider wraps a SessionManager and labels sessions as "flight".
type flightSessionProvider struct {
	sm *SessionManager
}

func (p *flightSessionProvider) CreateSession(ctx context.Context, username string, pid int32, memoryLimit string, threads int) (int32, *server.FlightExecutor, error) {
	workerPID, executor, err := p.sm.CreateSession(ctx, username, pid, memoryLimit, threads)
	if err != nil {
		return 0, nil, err
	}
	p.sm.SetProtocol(workerPID, "flight")
	return workerPID, executor, nil
}

func (p *flightSessionProvider) DestroySession(pid int32) {
	p.sm.DestroySession(pid)
}

type flightOwnedSession struct {
	orgID    string
	sessions *SessionManager
}

// orgRoutedSessionProvider routes Flight SQL session operations to the correct
// org's SessionManager based on the username→org mapping resolved during auth.
type orgRoutedSessionProvider struct {
	orgRouter   OrgRouterInterface
	configStore ConfigStoreInterface

	mu         sync.RWMutex
	pidSession map[int32]flightOwnedSession // pid → owning session manager
	userOrg    map[string]string            // username → orgID (populated during auth)
}

func (p *orgRoutedSessionProvider) CreateSession(ctx context.Context, username string, pid int32, memoryLimit string, threads int) (int32, *server.FlightExecutor, error) {
	p.mu.RLock()
	orgID := p.userOrg[username]
	p.mu.RUnlock()

	_, sessions, _, ok := p.orgRouter.StackForOrg(orgID)
	if !ok {
		slog.Warn("Flight SQL session: no org stack for user.", "username", username, "org", orgID)
		return 0, nil, fmt.Errorf("no org configured for user %q", username)
	}

	// SessionManager.resolveSessionLimits handles rebalancer defaults,
	// so pass memoryLimit/threads through as-is.
	workerPID, executor, err := sessions.CreateSession(ctx, username, pid, memoryLimit, threads)
	if err != nil {
		return 0, nil, err
	}

	sessions.SetProtocol(workerPID, "flight")

	p.mu.Lock()
	p.pidSession[workerPID] = flightOwnedSession{orgID: orgID, sessions: sessions}
	p.mu.Unlock()

	return workerPID, executor, nil
}

func (p *orgRoutedSessionProvider) DestroySession(pid int32) {
	p.mu.RLock()
	owned, ok := p.pidSession[pid]
	p.mu.RUnlock()
	if !ok {
		slog.Warn("Flight SQL destroy: no session manager for pid.", "pid", pid)
		return
	}

	owned.sessions.DestroySession(pid)

	p.mu.Lock()
	delete(p.pidSession, pid)
	p.mu.Unlock()
}

func (p *orgRoutedSessionProvider) DurableSessionMetadata(pid int32, username string) (flightsqlingress.DurableSessionMetadata, error) {
	p.mu.RLock()
	owned, ok := p.pidSession[pid]
	p.mu.RUnlock()
	if !ok {
		return flightsqlingress.DurableSessionMetadata{}, fmt.Errorf("no session manager for pid %d", pid)
	}
	workerID := owned.sessions.WorkerIDForPID(pid)
	if workerID < 0 {
		return flightsqlingress.DurableSessionMetadata{}, fmt.Errorf("worker not found for pid %d", pid)
	}
	worker, ok := owned.sessions.pool.Worker(workerID)
	if !ok {
		return flightsqlingress.DurableSessionMetadata{}, fmt.Errorf("worker %d not found for pid %d", workerID, pid)
	}
	return flightsqlingress.DurableSessionMetadata{
		Username:     username,
		OrgID:        owned.orgID,
		WorkerID:     workerID,
		OwnerEpoch:   worker.OwnerEpoch(),
		CPInstanceID: worker.OwnerCPInstanceID(),
	}, nil
}

func (p *orgRoutedSessionProvider) ReconnectSession(ctx context.Context, record flightsqlingress.DurableSessionRecord) (int32, *server.FlightExecutor, error) {
	_, sessions, _, ok := p.orgRouter.StackForOrg(record.OrgID)
	if !ok {
		return 0, nil, fmt.Errorf("no org stack for org %q", record.OrgID)
	}

	pid, executor, err := sessions.ReconnectFlightSession(ctx, record.Username, record.WorkerID, record.OwnerEpoch)
	if err != nil {
		if errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
			return 0, nil, flightsqlingress.MarkDurableReconnectTerminal(err)
		}
		return 0, nil, err
	}

	p.mu.Lock()
	p.pidSession[pid] = flightOwnedSession{orgID: record.OrgID, sessions: sessions}
	p.mu.Unlock()
	return pid, executor, nil
}

func (p *orgRoutedSessionProvider) DurableSessionStore() flightsqlingress.DurableSessionStore {
	if p == nil || p.configStore == nil {
		return nil
	}
	return &configStoreFlightSessionStore{store: p.configStore}
}

type configStoreFlightSessionStore struct {
	store ConfigStoreInterface
}

func (s *configStoreFlightSessionStore) UpsertSession(record flightsqlingress.DurableSessionRecord) error {
	return s.store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: record.SessionToken,
		Username:     record.Username,
		OrgID:        record.OrgID,
		WorkerID:     record.WorkerID,
		OwnerEpoch:   record.OwnerEpoch,
		CPInstanceID: record.CPInstanceID,
		State:        configstore.FlightSessionState(record.State),
		ExpiresAt:    record.ExpiresAt,
		LastSeenAt:   record.LastSeenAt,
	})
}

func (s *configStoreFlightSessionStore) GetSession(sessionToken string) (*flightsqlingress.DurableSessionRecord, error) {
	record, err := s.store.GetFlightSessionRecord(sessionToken)
	if err != nil || record == nil {
		return nil, err
	}
	return &flightsqlingress.DurableSessionRecord{
		SessionToken: record.SessionToken,
		Username:     record.Username,
		OrgID:        record.OrgID,
		WorkerID:     record.WorkerID,
		OwnerEpoch:   record.OwnerEpoch,
		CPInstanceID: record.CPInstanceID,
		State:        flightsqlingress.DurableSessionState(record.State),
		ExpiresAt:    record.ExpiresAt,
		LastSeenAt:   record.LastSeenAt,
	}, nil
}

func (s *configStoreFlightSessionStore) TouchSession(sessionToken string, lastSeenAt time.Time) error {
	return s.store.TouchFlightSessionRecord(sessionToken, lastSeenAt)
}

func (s *configStoreFlightSessionStore) CloseSession(sessionToken string, closedAt time.Time) error {
	return s.store.CloseFlightSessionRecord(sessionToken, closedAt)
}
