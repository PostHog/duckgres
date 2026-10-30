package netlisten

import (
	"net"
	"os"
	"os/exec"
	"testing"
)

// TestModuleDefaultDisablesMultipathTCP guards the `godebug multipathtcp=0`
// directive in go.mod.
//
// Since Go 1.24 the net package opens listeners with a Multipath TCP socket
// (IPPROTO_MPTCP) whenever the kernel supports MPTCP: defaultMPTCPEnabledListen
// is true. The go.mod directive pins the pre-1.24 default for the whole module,
// so every binary and test built from this repository gets plain TCP listeners
// without anyone having to remember a deployment-time environment variable.
//
// Deleting the directive turns this test red; that is the point of it.
func TestModuleDefaultDisablesMultipathTCP(t *testing.T) {
	if (&net.ListenConfig{}).MultipathTCP() {
		t.Fatal("a zero net.ListenConfig reports MultipathTCP()==true: the `godebug multipathtcp=0` directive is missing from go.mod, so listeners in this module would be IPPROTO_MPTCP sockets on kernels that support MPTCP")
	}
}

// TestConfigDisablesMultipathTCP covers the explicit override this package
// exists to provide.
func TestConfigDisablesMultipathTCP(t *testing.T) {
	if Config().MultipathTCP() {
		t.Fatal("Config() reports MultipathTCP()==true; call sites using it would bind MPTCP sockets")
	}
}

// TestConfigDisablesMultipathTCPUnderGODEBUGOverride proves the explicit
// SetMultipathTCP(false) survives a GODEBUG environment variable that would
// otherwise re-enable MPTCP.
//
// This runs in a child process because the net package reads GODEBUG once per
// setting per process, so a t.Setenv in this process cannot influence a value
// that earlier tests already forced.
func TestConfigDisablesMultipathTCPUnderGODEBUGOverride(t *testing.T) {
	if os.Getenv("NETLISTEN_GODEBUG_PROBE") == "1" {
		if Config().MultipathTCP() {
			t.Fatalf("GODEBUG=%s re-enabled MPTCP on a Config() listener; an explicit SetMultipathTCP(false) must win over GODEBUG", os.Getenv("GODEBUG"))
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestConfigDisablesMultipathTCPUnderGODEBUGOverride$", "-test.v")
	cmd.Env = append(os.Environ(), "NETLISTEN_GODEBUG_PROBE=1", "GODEBUG=multipathtcp=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child process with GODEBUG=multipathtcp=1 failed: %v\n%s", err, out)
	}
}
