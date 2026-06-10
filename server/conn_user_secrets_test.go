package server

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeUserSecretMgr records PutSecret/DeleteSecret calls.
type fakeUserSecretMgr struct {
	readyErr       error
	putErr         error
	delErr         error
	delExist       bool
	putCalls       []string // "org/user/name"
	putStmts       []string
	putIfNotExists []bool
	delCalls       []string
}

func (m *fakeUserSecretMgr) Ready() error { return m.readyErr }
func (m *fakeUserSecretMgr) PutSecret(_ context.Context, orgID, username, name, stmt string, ifNotExists bool) error {
	m.putCalls = append(m.putCalls, orgID+"/"+username+"/"+name)
	m.putStmts = append(m.putStmts, stmt)
	m.putIfNotExists = append(m.putIfNotExists, ifNotExists)
	return m.putErr
}
func (m *fakeUserSecretMgr) DeleteSecret(_ context.Context, orgID, username, name string) (bool, error) {
	m.delCalls = append(m.delCalls, orgID+"/"+username+"/"+name)
	return m.delExist, m.delErr
}

func newUserSecretTestConn(t *testing.T, mgr UserSecretManager, exec *lifecycleExecutor) (*clientConn, func()) {
	t.Helper()
	c, cleanup := newLifecycleClientConn(t)
	c.server.cfg.UserSecrets = mgr
	c.orgID = "org1"
	c.username = "alice"
	c.executor = exec
	return c, cleanup
}

func TestExecUserSecretDDLCreatePersists(t *testing.T) {
	mgr := &fakeUserSecretMgr{}
	exec := &lifecycleExecutor{execResult: emptyExecResult{}}
	c, cleanup := newUserSecretTestConn(t, mgr, exec)
	defer cleanup()

	stmt := "CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET 's')"
	handled, tag, secErr := c.execUserSecretDDL(stmt)
	if !handled || secErr != nil {
		t.Fatalf("handled=%v secErr=%v, want handled with no error", handled, secErr)
	}
	if tag != "CREATE" {
		t.Errorf("tag = %q, want CREATE", tag)
	}
	if exec.execCalls.Load() != 1 {
		t.Errorf("executor calls = %d, want 1 (DuckDB must validate before persisting)", exec.execCalls.Load())
	}
	if len(mgr.putCalls) != 1 || mgr.putCalls[0] != "org1/alice/my_s3" {
		t.Errorf("putCalls = %v, want [org1/alice/my_s3]", mgr.putCalls)
	}
	if mgr.putStmts[0] != stmt {
		t.Errorf("stored statement = %q, want original text", mgr.putStmts[0])
	}
}

func TestExecUserSecretDDLNotHandledCases(t *testing.T) {
	tests := []struct {
		name  string
		mgr   UserSecretManager
		query string
	}{
		{"nil manager", nil, "CREATE PERSISTENT SECRET s (TYPE s3)"},
		{"temporary create", &fakeUserSecretMgr{}, "CREATE TEMPORARY SECRET s (TYPE s3)"},
		{"plain create stays session-scoped", &fakeUserSecretMgr{}, "CREATE SECRET s (TYPE s3)"},
		{"drop temporary", &fakeUserSecretMgr{}, "DROP TEMPORARY SECRET s"},
		{"non-secret statement", &fakeUserSecretMgr{}, "SELECT 1"},
		{"unnamed drop", &fakeUserSecretMgr{}, "DROP SECRET"},
		{"plain drop in multi-statement batch keeps passthrough", &fakeUserSecretMgr{}, "DROP SECRET s; SELECT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &lifecycleExecutor{execResult: emptyExecResult{}}
			c, cleanup := newUserSecretTestConn(t, tt.mgr, exec)
			defer cleanup()
			handled, _, _ := c.execUserSecretDDL(tt.query)
			if handled {
				t.Errorf("execUserSecretDDL(%q) handled=true, want passthrough", tt.query)
			}
			if exec.execCalls.Load() != 0 {
				t.Errorf("executor was called for a passthrough statement")
			}
		})
	}
}

func TestExecUserSecretDDLRejections(t *testing.T) {
	tests := []struct {
		name     string
		mgr      *fakeUserSecretMgr
		query    string
		wantCode string
	}{
		{"not enabled", &fakeUserSecretMgr{readyErr: errors.New("no key")}, "CREATE PERSISTENT SECRET s (TYPE s3)", "0A000"},
		{"unnamed persistent", &fakeUserSecretMgr{}, "CREATE PERSISTENT SECRET (TYPE s3)", "0A000"},
		{"reserved name", &fakeUserSecretMgr{}, "CREATE PERSISTENT SECRET ducklake_s3 (TYPE s3)", "42939"},
		{"oversized statement", &fakeUserSecretMgr{}, "CREATE PERSISTENT SECRET big (TYPE s3, SECRET '" + strings.Repeat("x", maxUserSecretStatementLen) + "')", "54000"},
		// A persistent variant in a multi-statement batch must be rejected,
		// not silently fall through: it would execute, never persist, and be
		// wiped at the next session.
		{"multi-statement create", &fakeUserSecretMgr{}, "CREATE PERSISTENT SECRET s (TYPE s3); SELECT 1", "0A000"},
		{"multi-statement drop", &fakeUserSecretMgr{}, "DROP PERSISTENT SECRET s; SELECT 1", "0A000"},
		// Bound parameters can never be replayed; reject rather than execute
		// unbound or persist placeholder text.
		{"parameterized statement", &fakeUserSecretMgr{}, "CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID $1, SECRET $2)", "0A000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &lifecycleExecutor{execResult: emptyExecResult{}}
			c, cleanup := newUserSecretTestConn(t, tt.mgr, exec)
			defer cleanup()
			handled, _, secErr := c.execUserSecretDDL(tt.query)
			if !handled || secErr == nil {
				t.Fatalf("handled=%v secErr=%v, want handled error", handled, secErr)
			}
			if secErr.code != tt.wantCode {
				t.Errorf("code = %q, want %q (msg: %s)", secErr.code, tt.wantCode, secErr.msg)
			}
			if exec.execCalls.Load() != 0 {
				t.Errorf("rejected statement must not reach the executor")
			}
			if len(tt.mgr.putCalls) != 0 {
				t.Errorf("rejected statement must not be persisted")
			}
		})
	}
}

func TestExecUserSecretDDLExecErrorNotPersisted(t *testing.T) {
	mgr := &fakeUserSecretMgr{}
	exec := &lifecycleExecutor{execErr: errors.New("Invalid Input Error: bad type")}
	c, cleanup := newUserSecretTestConn(t, mgr, exec)
	defer cleanup()

	handled, _, secErr := c.execUserSecretDDL("CREATE PERSISTENT SECRET s (TYPE wat)")
	if !handled || secErr == nil {
		t.Fatalf("want handled error, got handled=%v secErr=%v", handled, secErr)
	}
	if len(mgr.putCalls) != 0 {
		t.Errorf("statement that failed on DuckDB must not be persisted")
	}
}

func TestExecUserSecretDDLPutFailureSurfaces(t *testing.T) {
	mgr := &fakeUserSecretMgr{putErr: errors.New("config store down")}
	exec := &lifecycleExecutor{execResult: emptyExecResult{}}
	c, cleanup := newUserSecretTestConn(t, mgr, exec)
	defer cleanup()

	handled, _, secErr := c.execUserSecretDDL("CREATE PERSISTENT SECRET s (TYPE s3)")
	if !handled || secErr == nil {
		t.Fatalf("want handled error when persistence fails, got handled=%v secErr=%v", handled, secErr)
	}
	if secErr.code != "58000" {
		t.Errorf("code = %q, want 58000", secErr.code)
	}
	if !strings.Contains(secErr.msg, "NOT survive") {
		t.Errorf("message %q must warn the secret did not persist", secErr.msg)
	}
}

func TestExecUserSecretDDLDropDeletesFromStore(t *testing.T) {
	mgr := &fakeUserSecretMgr{delExist: true}
	exec := &lifecycleExecutor{execResult: emptyExecResult{}}
	c, cleanup := newUserSecretTestConn(t, mgr, exec)
	defer cleanup()

	for _, q := range []string{"DROP PERSISTENT SECRET my_s3", "DROP SECRET IF EXISTS my_s3"} {
		mgr.delCalls = nil
		handled, tag, secErr := c.execUserSecretDDL(q)
		if !handled || secErr != nil {
			t.Fatalf("%q: handled=%v secErr=%v", q, handled, secErr)
		}
		if tag != "DROP" {
			t.Errorf("%q: tag = %q, want DROP", q, tag)
		}
		if len(mgr.delCalls) != 1 || mgr.delCalls[0] != "org1/alice/my_s3" {
			t.Errorf("%q: delCalls = %v, want [org1/alice/my_s3]", q, mgr.delCalls)
		}
	}
}

// A stored secret whose replay failed doesn't exist on the session; DROP must
// still be able to remove it from the store — but ONLY when the session-side
// failure is DuckDB's not-found error. Any other failure must surface and
// leave the store untouched: a false "DROP succeeded" is fatal for a
// credential revocation.
func TestExecUserSecretDDLDropFallsBackToStore(t *testing.T) {
	// DuckDB's actual message for dropping a missing secret.
	notFound := errors.New("Invalid Input Error: Failed to remove non-existent secret with name 'broken_one'")

	mgr := &fakeUserSecretMgr{delExist: true}
	exec := &lifecycleExecutor{execErr: notFound}
	c, cleanup := newUserSecretTestConn(t, mgr, exec)
	defer cleanup()

	handled, tag, secErr := c.execUserSecretDDL("DROP PERSISTENT SECRET broken_one")
	if !handled || secErr != nil {
		t.Fatalf("handled=%v secErr=%v, want store-backed success", handled, secErr)
	}
	if tag != "DROP" {
		t.Errorf("tag = %q, want DROP", tag)
	}

	// When the store has nothing either, the DuckDB error wins.
	mgr2 := &fakeUserSecretMgr{delExist: false}
	c2, cleanup2 := newUserSecretTestConn(t, mgr2, exec)
	defer cleanup2()
	handled, _, secErr = c2.execUserSecretDDL("DROP PERSISTENT SECRET nope")
	if !handled || secErr == nil {
		t.Fatalf("handled=%v secErr=%v, want the DuckDB error surfaced", handled, secErr)
	}
}

// Any session-side DROP failure other than not-found (cancellation, RPC
// error, aborted transaction, the multiple-storages ambiguity error) must NOT
// delete the stored secret and must surface the error.
func TestExecUserSecretDDLDropOtherErrorsDoNotTouchStore(t *testing.T) {
	for _, execErr := range []error{
		errors.New("rpc error: code = Unavailable desc = connection reset"),
		errors.New("Invalid Input Error: Ambiguity found for secret name 'dup', secret occurs in multiple storages"),
		errors.New("current transaction is aborted"),
	} {
		mgr := &fakeUserSecretMgr{delExist: true}
		exec := &lifecycleExecutor{execErr: execErr}
		c, cleanup := newUserSecretTestConn(t, mgr, exec)
		handled, _, secErr := c.execUserSecretDDL("DROP PERSISTENT SECRET my_s3")
		if !handled || secErr == nil {
			t.Errorf("%v: handled=%v secErr=%v, want the error surfaced", execErr, handled, secErr)
		}
		if len(mgr.delCalls) != 0 {
			t.Errorf("%v: store delete was attempted on a non-not-found exec error", execErr)
		}
		cleanup()
	}
}

// IF NOT EXISTS must flow through to the store so an already-stored secret is
// not silently replaced while DuckDB no-ops the live session.
func TestExecUserSecretDDLIfNotExistsFlag(t *testing.T) {
	mgr := &fakeUserSecretMgr{}
	exec := &lifecycleExecutor{execResult: emptyExecResult{}}
	c, cleanup := newUserSecretTestConn(t, mgr, exec)
	defer cleanup()

	if handled, _, secErr := c.execUserSecretDDL("CREATE PERSISTENT SECRET IF NOT EXISTS foo (TYPE s3)"); !handled || secErr != nil {
		t.Fatalf("handled=%v secErr=%v", handled, secErr)
	}
	if handled, _, secErr := c.execUserSecretDDL("CREATE PERSISTENT SECRET bar (TYPE s3)"); !handled || secErr != nil {
		t.Fatalf("handled=%v secErr=%v", handled, secErr)
	}
	want := []bool{true, false}
	if len(mgr.putIfNotExists) != 2 || mgr.putIfNotExists[0] != want[0] || mgr.putIfNotExists[1] != want[1] {
		t.Errorf("putIfNotExists = %v, want %v", mgr.putIfNotExists, want)
	}
}
