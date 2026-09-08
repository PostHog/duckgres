//go:build kubernetes

package controlplane

import "testing"

func TestTrinoLegacyConsoleIdentityKeepsProvisionerID(t *testing.T) {
	cell := trinoCell{ID: "stored-cell", CoordinatorURL: "https://coordinator.example.test", TLSServerName: "tls.example.test", ClientURL: "https://client.example.test"}
	console := cell.consoleCell()
	if console.ID != "legacy" || console.StoredID != "stored-cell" {
		t.Fatalf("console identity: %+v", console)
	}
	if cell.ID != "stored-cell" || console.CoordinatorURL != cell.CoordinatorURL || console.TLSServerName != cell.TLSServerName || console.ClientURL != cell.ClientURL {
		t.Fatal("alias changed storage identity or connection configuration")
	}
}
