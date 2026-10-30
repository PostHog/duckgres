package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// tcpNetworks are the net package networks that end up as TCP sockets, and
// therefore the ones that can silently become IPPROTO_MPTCP sockets on kernels
// built with CONFIG_MPTCP.
//
// internal/netlisten pins MultipathTCP off for exactly this reason. go.mod's
// `godebug multipathtcp=0` covers the module default, but only until something
// sets GODEBUG in the deployment environment: an explicit SetMultipathTCP(false)
// survives that, a build-time default does not. That is why new listeners have to
// go through the helper rather than rely on the directive.
var tcpNetworks = map[string]bool{"tcp": true, "tcp4": true, "tcp6": true}

// upgraderListenerFile is the one file where a TCP listener is bound by a
// dependency rather than by us: tableflip owns the client-facing PG listener's
// lifecycle (it inherits the FD across a zero-downtime restart), so the socket is
// bound by the *tableflip.Upgrader. The MPTCP setting still applies, because it
// travels in tableflip.Options.ListenConfig — TestUpgraderReceivesNetlistenConfig
// checks that below. Only that one call is exempted: the rest of the file is
// still scanned, so a new direct net.Listen there is caught.
const upgraderListenerFile = "controlplane/control.go"

// skippedDirs are not scanned: no first-party Go listeners live there, and
// walking them is slow or unreliable.
var skippedDirs = map[string]bool{".git": true, "vendor": true, "node_modules": true}

func TestTCPListenersUseNetlisten(t *testing.T) {
	var violations []string

	for _, path := range goFiles(t) {
		// Test harnesses bind loopback ports to stand up a fake upstream; they
		// never serve a customer and are not part of a released binary.
		if strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, "internal/netlisten/") || strings.HasPrefix(path, "tests/") {
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

// TestUpgraderReceivesNetlistenConfig validates the exemption above: the
// tableflip upgrader must be handed a listen config that disables MPTCP,
// otherwise the client-facing PG listener binds an IPPROTO_MPTCP socket.
func TestUpgraderReceivesNetlistenConfig(t *testing.T) {
	src := readFile(t, upgraderListenerFile)
	f := parseFile(t, upgraderListenerFile, src)

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
			// Exactly the helper's config, not something that merely mentions it:
			// a wrapper could call netlisten.Config() and then re-enable MPTCP.
			if got := exprText(src, kv.Value); got != "netlisten.Config()" {
				t.Errorf("tableflip.Options.ListenConfig = %s, want exactly netlisten.Config(): the PG listener would bind an MPTCP socket", got)
			}

			return true
		}
		t.Error("tableflip.Options does not set ListenConfig, so the PG listener falls back to the net package default and binds an IPPROTO_MPTCP socket on kernels with MPTCP")

		return true
	})

	if !found {
		t.Fatalf("%s no longer builds a tableflip.Options literal; update upgraderListenerFile and this test", upgraderListenerFile)
	}
}

// tcpListenCalls returns a description of every listener-creating call in path
// that would bind a TCP socket using the net package default.
//
// This is a syntactic check, so it can be evaded: a dot-import, a function value
// (f := net.Listen), or an aliased import hides the package from it. It catches
// the shapes people actually write, which is the point; the go.mod directive and
// code review cover the rest.
func tcpListenCalls(t *testing.T, path string) []string {
	t.Helper()

	src := readFile(t, path)
	f := parseFile(t, path, src)
	configVars := netlistenConfigVars(f)

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
		// netlisten.Listen(...), netlisten.Config().Listen(...), or the same
		// config kept in a variable.
		if isNetlisten(sel.X) || isNetlistenConfigCall(sel.X) {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && configVars[id.Name] {
			return true
		}
		if path == upgraderListenerFile && isUpgrader(sel.X) && sel.Sel.Name == "Listen" {
			return true
		}

		var networkArgIdx int
		switch sel.Sel.Name {
		case "Listen":
			networkArgIdx = listenNetworkArgIdx(sel.X, call.Args)
		case "ListenTCP":
			networkArgIdx = 0
		case "ListenAndServe", "ListenAndServeTLS":
			// http.ListenAndServe* bind a TCP socket internally, with no way to
			// pass a listen config, so they are always on the net package
			// default. The rule is limited to the package-qualified form: a
			// method with the same name may be ours, like server.Server's, which
			// binds through netlisten itself.
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "http" {
				calls = append(calls, exprText(src, call))
			}

			return true
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

// listenNetworkArgIdx picks the argument that carries the network name. Choosing
// by argument count alone gets tls.Listen("tcp", addr, config) wrong, which is a
// three-argument call whose network is still first.
func listenNetworkArgIdx(recv ast.Expr, args []ast.Expr) int {
	// net.Listen(network, addr), tls.Listen(network, addr, config).
	if id, ok := recv.(*ast.Ident); ok && (id.Name == "net" || id.Name == "tls") {
		return 0
	}
	// (*net.ListenConfig).Listen(ctx, network, addr).
	if len(args) == 3 {
		return 1
	}

	return 0
}

// netlistenConfigVars collects identifiers assigned from netlisten.Config(), so
// the two-step form is not reported as a violation:
//
//	lc := netlisten.Config()
//	lc.Listen(ctx, "tcp", addr)
func netlistenConfigVars(f *ast.File) map[string]bool {
	vars := map[string]bool{}

	ast.Inspect(f, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != len(assign.Rhs) {
			return true
		}
		for i, rhs := range assign.Rhs {
			if !isNetlistenConfigCall(rhs) {
				continue
			}
			if id, ok := assign.Lhs[i].(*ast.Ident); ok {
				vars[id.Name] = true
			}
		}

		return true
	})

	return vars
}

func isNetlistenConfigCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == "Config" && isNetlisten(sel.X)
}

// isNetlisten reports whether expr is the internal/netlisten package.
func isNetlisten(expr ast.Expr) bool {
	pkg, ok := expr.(*ast.Ident)

	return ok && pkg.Name == "netlisten"
}

// isUpgrader reports whether expr is the tableflip upgrader variable.
func isUpgrader(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)

	return ok && ident.Name == "upg"
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

func parseFile(t *testing.T, path string, src []byte) *ast.File {
	t.Helper()

	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	return f
}

// goFiles walks the working tree rather than asking git: this test also runs in
// build stages that have no .git directory, where shelling out to git fails.
func goFiles(t *testing.T) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skippedDirs[d.Name()] {
				return filepath.SkipDir
			}

			return nil
		}
		if strings.HasSuffix(path, ".go") {
			files = append(files, filepath.ToSlash(path))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk for go files: %v", err)
	}

	return files
}
