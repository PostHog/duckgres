package controlplane

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
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

func NewFlightIngressFromListener(listener net.Listener, tlsConfig *tls.Config, validator flightsqlingress.CredentialValidator, provider flightsqlingress.SessionProvider, rateLimiter *server.RateLimiter, cfg FlightIngressConfig) (*FlightIngress, error) {
	return flightsqlingress.NewFlightIngressFromListener(listener, tlsConfig, validator, provider, cfg, flightsqlingress.Options{
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

func (p *flightSessionProvider) CreateSession(ctx context.Context, username string, pid int32, memoryLimit string, threads int) (int32, *flightclient.FlightExecutor, error) {
	workerPID, executor, err := p.sm.CreateSessionWithProtocol(ctx, username, pid, memoryLimit, threads, "flight", nil)
	if err != nil {
		return 0, nil, err
	}
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
// org's SessionManager. The org is derived from the connection's managed
// hostname (SNI) — the same immutable per-connection identity that auth uses —
// re-resolved at session-create time via resolveOrg. There is deliberately NO
// username→org map: a username is only unique within an org, so a shared map
// keyed by username collides when two tenants share a username (the auth result
// for one connection could be overwritten by a concurrent connection's).
type orgRoutedSessionProvider struct {
	orgRouter   OrgRouterInterface
	configStore ConfigStoreInterface
	// resolveOrg resolves the org for a session from the request context's SNI.
	// Injected so it can be stubbed in tests; production wires it to
	// ControlPlane.flightOrgFromContext.
	resolveOrg func(ctx context.Context) (orgID string, ok bool)

	mu         sync.RWMutex
	pidSession map[int32]flightOwnedSession // pid → owning session manager
}

func (p *orgRoutedSessionProvider) SessionOrgID(pid int32) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	owned, ok := p.pidSession[pid]
	return owned.orgID, ok
}

func (p *orgRoutedSessionProvider) QueryAccessPolicy(ctx context.Context, username string) *server.QueryAccessPolicy {
	orgID, ok := p.resolveOrg(ctx)
	if !ok || orgID == "" {
		// Session creation will reject the same unresolved SNI. Returning an
		// empty policy here keeps this helper independently fail-closed.
		return &server.QueryAccessPolicy{ReadOnly: true}
	}
	policy, _, valid := p.queryAccessPolicyForOrg(orgID, username)
	if !valid {
		return &server.QueryAccessPolicy{ReadOnly: true}
	}
	return policy
}

func (p *orgRoutedSessionProvider) queryAccessPolicyForOrg(orgID, username string) (*server.QueryAccessPolicy, string, bool) {
	store, ok := p.configStore.(interface {
		OrgUserSessionQueryAccess(orgID, username string) (*configstore.OrgUserQueryAccess, string, bool)
	})
	if !ok {
		return nil, "", false
	}
	access, revision, valid := store.OrgUserSessionQueryAccess(orgID, username)
	if !valid || access == nil {
		return nil, revision, valid
	}
	return &server.QueryAccessPolicy{
		ReadOnly:         access.ReadOnly,
		AllowedSchemas:   access.AllowedSchemas,
		AllowedRelations: access.AllowedRelations,
	}, revision, true
}

func (p *orgRoutedSessionProvider) CreateSession(ctx context.Context, username string, pid int32, memoryLimit string, threads int) (int32, *flightclient.FlightExecutor, error) {
	// Bind the session to the org of THIS connection's managed hostname, not a
	// shared username lookup. Fail closed if the SNI no longer resolves an org.
	if p.resolveOrg == nil {
		return 0, nil, fmt.Errorf("flight session provider misconfigured: no org resolver")
	}
	orgID, ok := p.resolveOrg(ctx)
	if !ok || orgID == "" {
		slog.Warn("Flight SQL session: could not resolve org from connection SNI.", "username", username)
		return 0, nil, fmt.Errorf("could not resolve organization for flight session")
	}

	if p.orgResharding(orgID) {
		slog.Info("Flight SQL session rejected during metadata-store reshard.", "org", orgID)
		return 0, nil, fmt.Errorf("metadata-store reshard in progress for your organization, please retry shortly")
	}

	_, sessions, _, ok := p.orgRouter.StackForOrg(orgID)
	if !ok {
		slog.Warn("Flight SQL session: no org stack for org.", "username", username, "org", orgID)
		return 0, nil, fmt.Errorf("no org stack for org %q", orgID)
	}

	// SessionManager.resolveSessionLimits handles rebalancer defaults,
	// so pass memoryLimit/threads through as-is.
	workerPID, executor, err := sessions.CreateSessionWithProtocol(ctx, username, pid, memoryLimit, threads, "flight", nil)
	if err != nil {
		return 0, nil, err
	}

	p.mu.Lock()
	p.pidSession[workerPID] = flightOwnedSession{orgID: orgID, sessions: sessions}
	p.mu.Unlock()

	return workerPID, executor, nil
}

// orgResharding reports whether the org's warehouse is mid-reshard (from the
// polled config snapshot). Create/reconnect are refused then — the org's live
// sessions drain and new work waits for the cutover.
func (p *orgRoutedSessionProvider) orgResharding(orgID string) bool {
	if p.configStore == nil {
		return false
	}
	state, ok := p.configStore.OrgWarehouseStatus(orgID)
	return ok && state == string(configstore.ManagedWarehouseStateResharding)
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
	_, accessRevision, valid := p.queryAccessPolicyForOrg(owned.orgID, username)
	if !valid {
		return flightsqlingress.DurableSessionMetadata{}, fmt.Errorf("user access changed before Flight session persistence")
	}
	return flightsqlingress.DurableSessionMetadata{
		Username:       username,
		OrgID:          owned.orgID,
		WorkerID:       workerID,
		OwnerEpoch:     worker.OwnerEpoch(),
		CPInstanceID:   worker.OwnerCPInstanceID(),
		AccessRevision: accessRevision,
	}, nil
}

func (p *orgRoutedSessionProvider) ReconnectSession(ctx context.Context, record flightsqlingress.DurableSessionRecord) (int32, *flightclient.FlightExecutor, error) {
	currentPolicy, accessRevision, valid := p.queryAccessPolicyForOrg(record.OrgID, record.Username)
	if !record.AccessPolicyRecorded || record.AccessRevision == "" || !valid ||
		accessRevision != record.AccessRevision || !sameQueryAccessPolicy(currentPolicy, record.QueryAccessPolicy) {
		return 0, nil, flightsqlingress.MarkDurableReconnectTerminal(
			fmt.Errorf("flight session access changed; authenticate again"))
	}
	if p.orgResharding(record.OrgID) {
		// Terminal, not retry-later: the reshard runner drains parked flight
		// sessions; letting a reconnect re-acquire a worker mid-reshard would
		// re-pin the source catalog.
		return 0, nil, flightsqlingress.MarkDurableReconnectTerminal(
			fmt.Errorf("metadata-store reshard in progress for your organization, please retry shortly"))
	}
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

func sameQueryAccessPolicy(a, b *server.QueryAccessPolicy) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	normalizedA := server.NormalizeQueryAccessPolicy(*a)
	normalizedB := server.NormalizeQueryAccessPolicy(*b)
	return normalizedA.ReadOnly == normalizedB.ReadOnly &&
		slices.Equal(normalizedA.AllowedSchemas, normalizedB.AllowedSchemas) &&
		slices.Equal(normalizedA.AllowedRelations, normalizedB.AllowedRelations)
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
	var accessReadOnly bool
	var allowedSchemas, allowedRelations []string
	if record.QueryAccessPolicy != nil {
		accessReadOnly = record.QueryAccessPolicy.ReadOnly
		allowedSchemas = record.QueryAccessPolicy.AllowedSchemas
		allowedRelations = record.QueryAccessPolicy.AllowedRelations
	}
	return s.store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken:         record.SessionToken,
		Username:             record.Username,
		OrgID:                record.OrgID,
		WorkerID:             record.WorkerID,
		PID:                  record.PID,
		OwnerEpoch:           record.OwnerEpoch,
		CPInstanceID:         record.CPInstanceID,
		State:                configstore.FlightSessionState(record.State),
		ExpiresAt:            record.ExpiresAt,
		LastSeenAt:           record.LastSeenAt,
		AccessPolicyRecorded: record.AccessPolicyRecorded,
		AccessRevision:       record.AccessRevision,
		AccessReadOnly:       accessReadOnly,
		AllowedSchemas:       allowedSchemas,
		AllowedRelations:     allowedRelations,
	})
}

func (s *configStoreFlightSessionStore) GetSession(sessionToken string) (*flightsqlingress.DurableSessionRecord, error) {
	record, err := s.store.GetFlightSessionRecord(sessionToken)
	if err != nil || record == nil {
		return nil, err
	}
	var queryAccessPolicy *server.QueryAccessPolicy
	if record.AccessPolicyRecorded && (record.AccessReadOnly || len(record.AllowedSchemas) > 0 || len(record.AllowedRelations) > 0) {
		queryAccessPolicy = &server.QueryAccessPolicy{
			ReadOnly:         record.AccessReadOnly,
			AllowedSchemas:   record.AllowedSchemas,
			AllowedRelations: record.AllowedRelations,
		}
	}
	return &flightsqlingress.DurableSessionRecord{
		SessionToken:         record.SessionToken,
		Username:             record.Username,
		OrgID:                record.OrgID,
		WorkerID:             record.WorkerID,
		PID:                  record.PID,
		OwnerEpoch:           record.OwnerEpoch,
		CPInstanceID:         record.CPInstanceID,
		State:                flightsqlingress.DurableSessionState(record.State),
		ExpiresAt:            record.ExpiresAt,
		LastSeenAt:           record.LastSeenAt,
		AccessPolicyRecorded: record.AccessPolicyRecorded,
		AccessRevision:       record.AccessRevision,
		QueryAccessPolicy:    queryAccessPolicy,
	}, nil
}

func (s *configStoreFlightSessionStore) TouchSession(sessionToken string, lastSeenAt time.Time) error {
	return s.store.TouchFlightSessionRecord(sessionToken, lastSeenAt)
}

func (s *configStoreFlightSessionStore) CloseSession(sessionToken string, closedAt time.Time) error {
	return s.store.CloseFlightSessionRecord(sessionToken, closedAt)
}

func (s *configStoreFlightSessionStore) CloseSessionIfReconnectTargetUnchanged(stale flightsqlingress.DurableSessionRecord, closedAt time.Time) (bool, error) {
	return s.store.CloseFlightSessionRecordIfReconnectTargetUnchanged(configstore.FlightSessionRecord{
		SessionToken: stale.SessionToken,
		Username:     stale.Username,
		OrgID:        stale.OrgID,
		WorkerID:     stale.WorkerID,
		PID:          stale.PID,
		OwnerEpoch:   stale.OwnerEpoch,
		CPInstanceID: stale.CPInstanceID,
		State:        configstore.FlightSessionState(stale.State),
		ExpiresAt:    stale.ExpiresAt,
		LastSeenAt:   stale.LastSeenAt,
	}, closedAt)
}
