package controlplane

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"

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

// teamRoutedSessionProvider routes Flight SQL session operations to the correct
// team's SessionManager based on the username→team mapping in the config store.
type teamRoutedSessionProvider struct {
	teamRouter TeamRouterInterface

	mu         sync.RWMutex
	pidSession map[int32]*SessionManager // pid → owning session manager
}

func (p *teamRoutedSessionProvider) CreateSession(ctx context.Context, username string, pid int32, memoryLimit string, threads int) (int32, *server.FlightExecutor, error) {
	_, sessions, _, ok := p.teamRouter.StackForUser(username)
	if !ok {
		slog.Warn("Flight SQL session: no team stack for user.", "username", username)
		return 0, nil, fmt.Errorf("no team configured for user %q", username)
	}

	// SessionManager.resolveSessionLimits handles rebalancer defaults,
	// so pass memoryLimit/threads through as-is.
	workerPID, executor, err := sessions.CreateSession(ctx, username, pid, memoryLimit, threads)
	if err != nil {
		return 0, nil, err
	}

	sessions.SetProtocol(workerPID, "flight")

	p.mu.Lock()
	p.pidSession[workerPID] = sessions
	p.mu.Unlock()

	return workerPID, executor, nil
}

func (p *teamRoutedSessionProvider) DestroySession(pid int32) {
	p.mu.RLock()
	sm, ok := p.pidSession[pid]
	p.mu.RUnlock()
	if !ok {
		slog.Warn("Flight SQL destroy: no session manager for pid.", "pid", pid)
		return
	}

	sm.DestroySession(pid)

	p.mu.Lock()
	delete(p.pidSession, pid)
	p.mu.Unlock()
}
