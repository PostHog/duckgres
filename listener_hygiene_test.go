package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// tcpNetworks are the net package networks that end up as TCP sockets, and
// therefore the ones that can silently become IPPROTO_MPTCP sockets on kernels
// built with CONFIG_MPTCP.
//
// internal/netlisten.Config pins MultipathTCP off for exactly this reason. The
// go.mod `godebug multipathtcp=0` directive covers the whole module already;
// this test stops a future listener from depending on it, because the directive
// is one deleted line away from being reverted and nothing else would notice.
var tcpNetworks = map[string]bool{"tcp": true, "tcp4": true, "tcp6": true}

// listenerAllowlist holds call sites that create TCP listeners without going
// through internal/netlisten. Every entry needs a reason that the test itself
// validates, so an entry cannot outlive the arrangement it describes.
var listenerAllowlist = map[string]string{
	// tableflip owns the client-facing PG listener's lifecycle (it inherits the
	// FD across zero-downtime restarts), so the socket is bound by the
	// *tableflip.Upgrader rather than by us. The MPTCP setting still applies:
	// it travels in tableflip.Options.ListenConfig, which
	// TestUpgraderReceivesNetlistenConfig checks below.
	"controlplane/control.go": "binds the PG listener through tableflip.Options.ListenConfig",
}

func TestTCPListenersUseNetlisten(t *testing.T) {
	tracked := trackedGoFiles(t)
	var violations []string

	for _, path := range tracked {
		if strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, "internal/netlisten/") {
			continue
		}
		// Test harnesses bind loopback ports to stand up a fake upstream; they
		// never serve a customer and are not part of a released binary.
		if strings.HasPrefix(path, "tests/") {
			continue
		}
		if _, allowed := listenerAllowlist[path]; allowed {
			continue
		}

		for _, call := range tcpListenCalls(t, path) {
			violations = append(violations, fmt.Sprintf("%s: %s", path, call))
		}
	}

	if len(violations) > 0 {
		t.Fatalf("TCP listeners must be created with internal/netlisten so MultipathTCP stays off (see internal/netlisten):\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestUpgraderReceivesNetlistenConfig validates the one allowlist entry above:
// the tableflip upgrader must be handed a listen config that disables MPTCP,
// otherwise the client-facing PG listener binds an IPPROTO_MPTCP socket.
func TestUpgraderReceivesNetlistenConfig(t *testing.T) {
	const path = "controlplane/control.go"
	src := readFile(t, path)
	f := parseFile(t, path, src)

	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Options" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "tableflip" {
			return true
		}
		found = true

		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "ListenConfig" {
				continue
			}
			if got := exprText(src, kv.Value); !strings.Contains(got, "netlisten.Config()") {
				t.Errorf("tableflip.Options.ListenConfig = %s, want netlisten.Config(): the PG listener would bind an MPTCP socket", got)
			}
			return true
		}
		t.Error("tableflip.Options does not set ListenConfig, so the PG listener falls back to the net package default and binds an IPPROTO_MPTCP socket on kernels with MPTCP")

		return true
	})

	if !found {
		t.Fatalf("%s no longer builds a tableflip.Options literal; update listenerAllowlist and this test", path)
	}
}

// tcpListenCalls returns a description of every listener-creating call in path
// that would bind a TCP socket using the net package default.
func tcpListenCalls(t *testing.T, path string) []string {
	t.Helper()

	src := readFile(t, path)
	f := parseFile(t, path, src)

	var calls []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "netlisten" {
			return true
		}

		var networkArgIdx int
		switch sel.Sel.Name {
		case "Listen", "ListenTCP", "ListenPacket":
			networkArgIdx = 0
		default:
			return true
		}

		network, isLiteral := stringLiteral(call.Args, networkArgIdx)
		if isLiteral && !tcpNetworks[network] {
			// unix/unixpacket/udp and friends: not TCP, nothing to pin.
			return true
		}

		description := exprText(src, call)
		if !isLiteral {
			description += " (network argument is not a literal; may resolve to a TCP network)"
		}
		calls = append(calls, description)

		return true
	})

	return calls
}

func stringLiteral(args []ast.Expr, idx int) (string, bool) {
	if idx >= len(args) {
		return "", false
	}
	lit, ok := args[idx].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}

	return value, true
}

func exprText(src []byte, node ast.Node) string {
	start := node.Pos() - 1
	end := node.End() - 1
	if start < 0 || int(end) > len(src) {
		return ""
	}

	return strings.Join(strings.Fields(string(src[start:end])), " ")
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()

	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return src
}

func trackedGoFiles(t *testing.T) []string {
	t.Helper()

	out, err := exec.Command("git", "ls-files", "-z", "*.go").Output()
	if err != nil {
		t.Fatalf("list tracked files: %v", err)
	}

	var files []string
	for _, file := range bytes.Split(out, []byte{0}) {
		if len(file) == 0 {
			continue
		}
		files = append(files, string(file))
	}

	return files
}

func parseFile(t *testing.T, path string, src []byte) *ast.File {
	t.Helper()

	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	return f
}
