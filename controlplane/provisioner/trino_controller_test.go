//go:build kubernetes

package provisioner

import "testing"

func TestTrinoControllerRejectsTypedNilProvisioner(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("typed nil provisioner must be rejected before reconciliation")
		}
	}()
	var p *TrinoProvisioner
	new(Controller).WithTrinoProvisioner(p)
}
