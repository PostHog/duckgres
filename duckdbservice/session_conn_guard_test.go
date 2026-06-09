package duckdbservice

import (
	"context"
	"database/sql"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestFlightHandlersDoNotUseSessionConnDirectly(t *testing.T) {
	files := []string{
		"flight_handler.go",
		"copy_from_stdin.go",
	}

	for _, file := range files {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, use := range directSessionConnUses(fset, parsed) {
			t.Errorf("%s uses session Conn directly; use Session conn helpers", use)
		}
	}
}

func TestSessionConnGuardCatchesSessionAliases(t *testing.T) {
	const src = `package duckdbservice

func bad(session *Session) {
	s := session
	_ = s.Conn
}
`
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "alias.go", src, 0)
	if err != nil {
		t.Fatalf("parse alias fixture: %v", err)
	}
	uses := directSessionConnUses(fset, parsed)
	if len(uses) == 0 {
		t.Fatal("expected guard to catch Conn access through a session alias")
	}
}

func directSessionConnUses(fset *token.FileSet, parsed *ast.File) []string {
	var uses []string
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		sessionNames := map[string]bool{"session": true}
		if fn.Type.Params != nil {
			for _, field := range fn.Type.Params.List {
				if !isSessionType(field.Type) {
					continue
				}
				for _, name := range field.Names {
					sessionNames[name.Name] = true
				}
			}
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if i >= len(n.Rhs) {
						continue
					}
					name, ok := lhs.(*ast.Ident)
					if ok && isSessionExpr(n.Rhs[i], sessionNames) {
						sessionNames[name.Name] = true
					}
				}
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if i < len(n.Values) && isSessionExpr(n.Values[i], sessionNames) {
						sessionNames[name.Name] = true
					}
				}
			case *ast.SelectorExpr:
				if n.Sel.Name != "Conn" {
					return true
				}
				if ident, ok := n.X.(*ast.Ident); ok && sessionNames[ident.Name] {
					pos := fset.Position(n.Pos())
					uses = append(uses, fmt.Sprintf("%s:%d", pos.Filename, pos.Line))
				}
			}
			return true
		})
	}
	return uses
}

func isSessionExpr(expr ast.Expr, sessionNames map[string]bool) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && sessionNames[ident.Name]
}

func isSessionType(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name == "Session"
	case *ast.StarExpr:
		return isSessionType(t.X)
	default:
		return false
	}
}

func TestSessionQueryRowsHoldsConnLockUntilClose(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	session := &Session{Conn: conn}
	rows, closeRows, err := session.queryRows(context.Background(), nil, "SELECT 1")
	if err != nil {
		t.Fatalf("queryRows: %v", err)
	}
	if rows == nil {
		t.Fatal("queryRows returned nil rows")
	}
	if session.connMu.TryLock() {
		session.connMu.Unlock()
		t.Fatal("queryRows released connMu before rows were closed")
	}
	if err := closeRows(); err != nil {
		t.Fatalf("closeRows: %v", err)
	}
	if !session.connMu.TryLock() {
		t.Fatal("queryRows did not release connMu after closeRows")
	}
	session.connMu.Unlock()
}

func TestSessionTxQueryRowsHoldsConnLockUntilClose(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	session := &Session{Conn: conn}
	tx, err := session.beginTx(context.Background())
	if err != nil {
		t.Fatalf("beginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, closeRows, err := session.queryRows(context.Background(), tx, "SELECT 1")
	if err != nil {
		t.Fatalf("queryRows tx: %v", err)
	}
	if rows == nil {
		t.Fatal("queryRows returned nil rows")
	}
	if session.connMu.TryLock() {
		session.connMu.Unlock()
		t.Fatal("tx queryRows released connMu before rows were closed")
	}
	if err := closeRows(); err != nil {
		t.Fatalf("closeRows: %v", err)
	}
	if !session.connMu.TryLock() {
		t.Fatal("tx queryRows did not release connMu after closeRows")
	}
	session.connMu.Unlock()
}
