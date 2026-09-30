//go:build kubernetes

package admin

import "testing"

func TestTrinoPoolConnectionRequiresExplicitClientEndpoint(t *testing.T) {
	cell := TrinoCell{ID: "pool-a", StoredID: "registered:pool-a"}
	if cell.connectionFor("tenant-a") != nil || cell.ServiceCredentialConnection("tenant-a", "svc_test") != nil {
		t.Fatal("a missing client endpoint must not advertise an implicit coordinator")
	}
	cell.ClientURL = "https://gateway.example.test"
	connection := cell.connectionFor("tenant-a")
	if connection == nil || connection.Host != "gateway.example.test" || connection.Port != 443 {
		t.Fatalf("explicit pool endpoint was not preserved: %+v", connection)
	}
}
