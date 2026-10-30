package netlisten

import (
	"net"
	"os"
	"os/exec"
	"strings"
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
// otherwise re-enable MPTCP on listeners.
//
// This runs in a child process because the net package reads GODEBUG once per
// setting per process, so a t.Setenv in this process cannot influence a value
// that earlier tests already forced.
//
// Two details make the difference between a real test and a green one:
//
//   - The child asserts the override took effect (a zero ListenConfig must
//     report MultipathTCP()==true) before it checks Config(). Without that
//     control the test passes when the override silently fails to apply, which
//     is exactly what happens if an inherited GODEBUG entry shadows ours.
//   - Inherited GODEBUG entries are dropped rather than appended to. The runtime
//     reads the first GODEBUG= entry in the environment while os.Getenv returns
//     the last, so appending ours would leave a CI-provided GODEBUG in force and
//     the whole test inert.
//
// Both "1" (everywhere) and "2" (listeners only) enable MPTCP on listeners; see
// mptcpStatusListen.get in net/dial.go.
func TestConfigDisablesMultipathTCPUnderGODEBUGOverride(t *testing.T) {
	if os.Getenv("NETLISTEN_GODEBUG_PROBE") == "1" {
		if !(&net.ListenConfig{}).MultipathTCP() {
			t.Fatalf("GODEBUG=%q did not enable MPTCP on a zero net.ListenConfig, so this test cannot tell a working SetMultipathTCP(false) from a missing one", os.Getenv("GODEBUG"))
		}
		if Config().MultipathTCP() {
			t.Fatalf("GODEBUG=%q re-enabled MPTCP on a Config() listener; an explicit SetMultipathTCP(false) must win over GODEBUG", os.Getenv("GODEBUG"))
		}

		return
	}

	for _, mode := range []string{"1", "2"} {
		t.Run("multipathtcp="+mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestConfigDisablesMultipathTCPUnderGODEBUGOverride$", "-test.v")
			for _, kv := range os.Environ() {
				if strings.HasPrefix(kv, "GODEBUG=") {
					continue
				}
				cmd.Env = append(cmd.Env, kv)
			}
			cmd.Env = append(cmd.Env, "NETLISTEN_GODEBUG_PROBE=1", "GODEBUG=multipathtcp="+mode)

			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child process with GODEBUG=multipathtcp=%s failed: %v\n%s", mode, err, out)
			}
		})
	}
}
