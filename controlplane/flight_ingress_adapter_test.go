package controlplane

import "testing"

func TestNewFlightIngressAdapterValidation(t *testing.T) {
	_, err := NewFlightIngress("127.0.0.1", 0, nil, map[string]string{}, nil, nil, FlightIngressConfig{})
	if err == nil {
		t.Fatalf("expected validation error for invalid port")
	}
}
