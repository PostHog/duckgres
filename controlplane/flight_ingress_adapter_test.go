//go:build !kubernetes

package controlplane

import (
	"testing"

	"github.com/posthog/duckgres/server/flightsqlingress"
)

func TestNewFlightIngressAdapterValidation(t *testing.T) {
	_, err := NewFlightIngress("127.0.0.1", 0, nil, &flightsqlingress.MapCredentialValidator{}, nil, nil, FlightIngressConfig{})
	if err == nil {
		t.Fatalf("expected validation error for invalid port")
		return
	}
}
