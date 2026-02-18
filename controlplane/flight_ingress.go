package controlplane

import (
	"crypto/tls"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightsqlingress"
)

type FlightIngressConfig = flightsqlingress.Config

type FlightIngress = flightsqlingress.FlightIngress

// NewFlightIngress creates a control-plane Flight SQL ingress listener.
func NewFlightIngress(host string, port int, tlsConfig *tls.Config, users map[string]string, sm *SessionManager, rateLimiter *server.RateLimiter, cfg FlightIngressConfig) (*FlightIngress, error) {
	return flightsqlingress.NewFlightIngress(host, port, tlsConfig, users, sm, cfg, flightsqlingress.Options{
		RateLimiter: rateLimiter,
		Hooks: flightsqlingress.Hooks{
			OnSessionCountChanged: observeFlightAuthSessions,
			OnSessionsReaped:      observeFlightSessionsReaped,
		},
	})
}
