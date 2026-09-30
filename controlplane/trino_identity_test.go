//go:build kubernetes

package controlplane

import "testing"

func TestTrinoPoolConsoleIdentityKeepsProvisionerID(t *testing.T) {
	cell := trinoCell{ID: "registered:cell-test", PublicID: "cell-test", ClientURL: "https://client.example.test"}
	console := cell.consoleCell()
	if console.ID != "cell-test" || console.StoredID != "registered:cell-test" {
		t.Fatalf("console identity: %+v", console)
	}
	if cell.ID != "registered:cell-test" || console.ClientURL != cell.ClientURL {
		t.Fatal("alias changed storage identity or connection configuration")
	}
}
