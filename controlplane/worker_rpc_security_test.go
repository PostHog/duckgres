//go:build kubernetes

package controlplane

import "testing"

func TestWorkerTLSBearerCredsRequireTransportSecurity(t *testing.T) {
	creds := &workerTLSBearerCreds{token: "test-token"}
	if !creds.RequireTransportSecurity() {
		t.Fatal("expected worker TLS bearer credentials to require transport security")
	}
}
