//go:build kubernetes

package controlplane

import "testing"

func TestMakeControlPlaneInstanceID_StaysKubernetesSafe(t *testing.T) {
	t.Parallel()

	id := makeControlPlaneInstanceID(
		"duckgres-control-plane-7fb9dd69c6-dcgzw",
		"14cd8dd9eb353e609c7a4387a594a418",
	)

	if len(id) > 63 {
		t.Fatalf("expected cp_instance_id length <= 63, got %d (%q)", len(id), id)
	}
	if id == "duckgres-control-plane-7fb9dd69c6-dcgzw:14cd8dd9eb353e609c7a4387a594a418" {
		t.Fatalf("expected shortened cp_instance_id, got original %q", id)
	}
	if id != "duckgres-control-plane-7fb9dd69c6-dcgzw-14cd8dd9eb353e60" {
		t.Fatalf("unexpected cp_instance_id %q", id)
	}
}
