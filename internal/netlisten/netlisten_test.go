package netlisten

import (
	"net"
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

// TestConfigDisablesMultipathTCPUnderGODEBUGOverride is the test that makes the
// helper load-bearing rather than decorative.
//
// The module default already keeps MPTCP off, so deleting SetMultipathTCP(false)
// from Config() would not turn TestConfigDisablesMultipathTCP red. GODEBUG puts
// the net package back on the MPTCP path, leaving the explicit call as the only
// thing holding it; the GODEBUG values that do that for listeners are "1"
// (everywhere) and "2" (listeners only), see mptcpStatusListen.get in
// net/dial.go.
//
// The control assertion is not decoration either: if the override silently fails
// to apply, Config() is false for the boring reason and this test would pass
// without the helper doing anything. Setting GODEBUG through t.Setenv is enough
// because internal/godebug re-reads its settings when GODEBUG changes, which
// TestGODEBUGOverrideTakesEffect checks first.
func TestConfigDisablesMultipathTCPUnderGODEBUGOverride(t *testing.T) {
	for _, mode := range []string{"1", "2"} {
		t.Run("multipathtcp="+mode, func(t *testing.T) {
			t.Setenv("GODEBUG", "multipathtcp="+mode)

			if !(&net.ListenConfig{}).MultipathTCP() {
				t.Fatalf("GODEBUG=multipathtcp=%s did not take effect: a zero net.ListenConfig still reports MultipathTCP()==false, so this test could not tell a working SetMultipathTCP(false) from a missing one", mode)
			}
			if Config().MultipathTCP() {
				t.Fatalf("GODEBUG=multipathtcp=%s re-enabled MPTCP on a Config() listener; an explicit SetMultipathTCP(false) must win over GODEBUG", mode)
			}
		})
	}
}

// TestGODEBUGOverrideTakesEffect pins the assumption the test above relies on:
// changing GODEBUG at runtime is visible to the net package. If that ever stops
// holding, the override test would need a child process to stay honest.
func TestGODEBUGOverrideTakesEffect(t *testing.T) {
	if (&net.ListenConfig{}).MultipathTCP() {
		t.Fatal("precondition failed: MPTCP is on before the override, so the module default is not in force")
	}

	t.Setenv("GODEBUG", "multipathtcp=1")

	if !(&net.ListenConfig{}).MultipathTCP() {
		t.Fatal("setting GODEBUG=multipathtcp=1 at runtime did not reach the net package; TestConfigDisablesMultipathTCPUnderGODEBUGOverride would then be testing nothing")
	}
}
