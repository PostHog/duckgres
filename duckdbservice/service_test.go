package duckdbservice

import (
	"context"
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestReapIdleRawSQLRollbackUsesBoundedContext(t *testing.T) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "service.go", nil, 0)
	if err != nil {
		t.Fatalf("parse service.go: %v", err)
	}

	var found bool
	ast.Inspect(parsed, func(node ast.Node) bool {
		fn, ok := node.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "reapIdle" {
			return true
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "ExecContext" {
				return true
			}
			recv, ok := sel.X.(*ast.SelectorExpr)
			if !ok || recv.Sel.Name != "Conn" {
				return true
			}
			recvIdent, ok := recv.X.(*ast.Ident)
			if !ok || recvIdent.Name != "s" {
				return true
			}
			query, ok := call.Args[1].(*ast.BasicLit)
			if !ok || query.Value != `"ROLLBACK"` {
				return true
			}
			found = true
			if isContextBackgroundCall(call.Args[0]) {
				pos := fset.Position(call.Args[0].Pos())
				t.Errorf("idle raw SQL rollback uses unbounded context at %s", pos)
			}
			return true
		})
		return false
	})
	if !found {
		t.Fatal("did not find idle raw SQL rollback ExecContext in reapIdle")
	}
}

func isContextBackgroundCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Background" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "context"
}

// With MaxSessions=1 (how the control plane spawns remote worker pods), a worker
// serves exactly one client query session — a second concurrent CreateSession is
// rejected rather than silently overcommitting the pod's resources. Internal
// control/maintenance connections (controlDB/warmupDB) are not counted sessions
// and are unaffected.
func TestCreateSessionRejectsSecondSessionWhenMaxIsOne(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		maxSessions: 1,
	}
	pool.sessions["existing"] = &Session{ID: "existing"}

	_, err := pool.CreateSession("u", "", 0)
	if err == nil {
		t.Fatal("expected second CreateSession to be rejected at MaxSessions=1")
	}
	if !strings.Contains(err.Error(), "max sessions reached") {
		t.Fatalf("expected 'max sessions reached' error, got %v", err)
	}
}

func TestSessionAllowsOverlappingProtocolOperations(t *testing.T) {
	s := &Session{}
	finish, ok := s.beginOperation()
	if !ok {
		t.Fatal("expected first operation to start")
	}
	defer finish()

	finish2, ok := s.beginOperation()
	if !ok {
		t.Fatal("expected overlapping protocol operation to be allowed")
	}
	finish2()
}

func TestSessionOperationFinishIsIdempotent(t *testing.T) {
	s := &Session{}
	finish, ok := s.beginOperation()
	if !ok {
		t.Fatal("expected first operation to start")
	}
	finish()
	finish()

	if finish2, ok := s.beginOperation(); !ok {
		t.Fatal("expected session to still allow operations after idempotent finish")
	} else {
		finish2()
	}
}

func TestReapAbandonedQueryHandleDeletesHandle(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:      "session-1",
		queries: make(map[string]*QueryHandle),
		txns:    make(map[string]*trackedTx),
	}
	finishOperation, ok := session.beginOperation()
	if !ok {
		t.Fatal("expected operation to start")
	}
	session.queries["query-1"] = &QueryHandle{
		Query:           "SELECT 1",
		createdAt:       time.Now().Add(-handleIdleTimeout - time.Minute),
		finishOperation: finishOperation,
	}
	pool.sessions[session.ID] = session

	pool.reapIdle(time.Now())

	session.mu.RLock()
	_, stillPresent := session.queries["query-1"]
	session.mu.RUnlock()
	if stillPresent {
		t.Fatal("expected abandoned query handle to be deleted")
	}
}

type exitPanic struct {
	code int
}

func TestInitSearchPath(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("fallback when user schema does not exist", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// "nonexistent_user" is not a schema — should fall back to 'main' without error
		initSearchPath(conn, "nonexistent_user")

		var searchPath string
		if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('search_path')").Scan(&searchPath); err != nil {
			t.Fatalf("failed to query search_path: %v", err)
		}
		if searchPath != "main,memory.main" {
			t.Errorf("expected search_path 'main,memory.main', got %q", searchPath)
		}
	})

	t.Run("includes user schema when it exists", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// Create a schema matching the username
		if _, err := conn.ExecContext(context.Background(), "CREATE SCHEMA myuser"); err != nil {
			t.Fatalf("failed to create schema: %v", err)
		}

		initSearchPath(conn, "myuser")

		var searchPath string
		if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('search_path')").Scan(&searchPath); err != nil {
			t.Fatalf("failed to query search_path: %v", err)
		}
		if searchPath != "myuser,main,memory.main" {
			t.Errorf("expected search_path 'myuser,main,memory.main', got %q", searchPath)
		}
	})
}

func TestCleanupSessionState(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("no temp objects returns true", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if ok := cleanupSessionState(conn); !ok {
			t.Errorf("cleanupSessionState() = false on a clean connection, want true")
		}
	})

	t.Run("drops user-created temp tables and views", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if _, err := conn.ExecContext(context.Background(), "CREATE TEMP TABLE t1 (x INTEGER)"); err != nil {
			t.Fatalf("create temp table: %v", err)
		}
		if _, err := conn.ExecContext(context.Background(), "CREATE TEMP VIEW v1 AS SELECT 1 AS x"); err != nil {
			t.Fatalf("create temp view: %v", err)
		}

		// cleanupSessionState may also DROP IF EXISTS many DuckDB-managed system
		// views via the temp schema (most are in other schemas, so the DROPs are
		// no-ops). Only assert that the user-created objects are gone.
		_ = cleanupSessionState(conn)

		var n int
		if err := conn.QueryRowContext(context.Background(),
			"SELECT count(*) FROM duckdb_tables() WHERE temporary = true AND table_name = 't1'",
		).Scan(&n); err != nil {
			t.Fatalf("count t1: %v", err)
		}
		if n != 0 {
			t.Errorf("user temp table t1 not dropped (remaining = %d)", n)
		}

		if err := conn.QueryRowContext(context.Background(),
			"SELECT count(*) FROM duckdb_views() WHERE temporary = true AND view_name = 'v1'",
		).Scan(&n); err != nil {
			t.Fatalf("count v1: %v", err)
		}
		if n != 0 {
			t.Errorf("user temp view v1 not dropped (remaining = %d)", n)
		}
	})

	t.Run("rollback clears aborted transaction state", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// Open a txn and leave it dangling — cleanup should ROLLBACK before
		// running its own statements. Without the rollback, the SELECT against
		// duckdb_tables() would surface the dangling txn's aborted state.
		if _, err := conn.ExecContext(context.Background(), "BEGIN TRANSACTION"); err != nil {
			t.Fatalf("begin txn: %v", err)
		}
		if _, err := conn.ExecContext(context.Background(), "CREATE TEMP TABLE leak (x INTEGER)"); err != nil {
			t.Fatalf("create temp inside txn: %v", err)
		}

		if ok := cleanupSessionState(conn); !ok {
			t.Errorf("cleanupSessionState() = false after rollback path, want true")
		}

		// After cleanup we should be out of the txn — a fresh statement should succeed.
		if _, err := conn.ExecContext(context.Background(), "SELECT 1"); err != nil {
			t.Errorf("post-cleanup SELECT failed: %v", err)
		}
	})
}

func TestDestroySessionClusterModeDiscardsConn(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDB:       db,
		sharedWarmMode: true,
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}

	// Create a temp macro on this conn — current cleanupSessionState does NOT
	// drop temp macros (only tables/views), so without conn discard a macro
	// would leak via the pool to the next session that reuses the conn.
	if _, err := conn.ExecContext(context.Background(),
		"CREATE OR REPLACE TEMP MACRO leak_check() AS 'leaked'",
	); err != nil {
		t.Fatalf("create temp macro: %v", err)
	}

	const token = "tok-cluster"
	pool.sessions[token] = &Session{
		ID:       token,
		DB:       db,
		Conn:     conn,
		Username: "test_user",
	}

	if err := pool.DestroySession(token); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}

	// Open a fresh conn from the same pool and verify the macro is gone.
	// In cluster mode the prior conn was evicted via evictConnFromPool, so
	// this fresh conn opens a new DuckDB connection that never had the
	// macro defined.
	fresh, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire fresh conn: %v", err)
	}
	defer func() { _ = fresh.Close() }()

	var v string
	err = fresh.QueryRowContext(context.Background(), "SELECT leak_check()").Scan(&v)
	if err == nil {
		t.Errorf("temp macro leaked across sessions: got value %q, expected error", v)
	}
}

func TestDestroySessionStandaloneModeRunsCleanup(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDB:       db,
		sharedWarmMode: false, // standalone mode — cleanup path runs
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}

	// Create a user-level temp table — this is what the standalone cleanup
	// path is responsible for scrubbing before returning the conn to the pool.
	if _, err := conn.ExecContext(context.Background(),
		"CREATE TEMP TABLE standalone_leak (x INTEGER)",
	); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	const token = "tok-standalone"
	pool.sessions[token] = &Session{
		ID:       token,
		DB:       db,
		Conn:     conn,
		Username: "test_user",
	}

	if err := pool.DestroySession(token); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}

	// In standalone mode the cleanup ran and dropped the temp table,
	// then the conn was returned to the pool (since cleanup succeeded).
	// A fresh conn shouldn't see standalone_leak regardless of which
	// driver conn it gets.
	fresh, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire fresh conn: %v", err)
	}
	defer func() { _ = fresh.Close() }()

	var n int
	if err := fresh.QueryRowContext(context.Background(),
		"SELECT count(*) FROM duckdb_tables() WHERE table_name = 'standalone_leak'",
	).Scan(&n); err != nil {
		t.Fatalf("count standalone_leak: %v", err)
	}
	if n != 0 {
		t.Errorf("standalone_leak survived cleanup: count=%d", n)
	}
}

func TestRunExitsWhenBundledExtensionBootstrapFails(t *testing.T) {
	prevBootstrap := bootstrapBundledExtensions
	prevExit := exitProcess
	defer func() {
		bootstrapBundledExtensions = prevBootstrap
		exitProcess = prevExit
	}()

	bootstrapBundledExtensions = func(string) error {
		return errors.New("boom")
	}

	exitCode := -1
	exitProcess = func(code int) {
		exitCode = code
		panic(exitPanic{code: code})
	}

	defer func() {
		r := recover()
		p, ok := r.(exitPanic)
		if !ok {
			t.Fatalf("expected exit panic, got %v", r)
		}
		if p.code != 1 {
			t.Fatalf("expected exit code 1, got %d", p.code)
		}
		if exitCode != 1 {
			t.Fatalf("expected exitProcess to be called with 1, got %d", exitCode)
		}
	}()

	Run(ServiceConfig{})
	t.Fatal("expected Run to exit")
}

func TestSessionPoolDrainWaitsForActiveWorkAndRejectsNewWork(t *testing.T) {
	pool := &SessionPool{}

	finish, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin tracked work: %v", err)
	}
	if got := pool.ActiveDrainWork(); got != 1 {
		t.Fatalf("expected one active drain work item, got %d", got)
	}

	pool.BeginDrain()
	if !pool.IsDraining() {
		t.Fatal("expected pool to be draining")
	}

	if _, err := pool.beginDrainWork(false); !errors.Is(err, ErrWorkerDraining) {
		t.Fatalf("expected new work to be rejected while draining, got %v", err)
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("expected drain wait to time out while active work is still running")
	}

	finish()

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected drain wait to complete after active work finishes")
	}
	if got := pool.ActiveDrainWork(); got != 0 {
		t.Fatalf("expected no active drain work, got %d", got)
	}
}

func TestSessionPoolRejectsContinuationAfterDrainReachesZero(t *testing.T) {
	pool := &SessionPool{}

	pool.BeginDrain()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected drain to complete with no active work")
	}

	if _, err := pool.beginDrainWork(true); !errors.Is(err, ErrWorkerDraining) {
		t.Fatalf("expected continuation to be rejected after drain completed, got %v", err)
	}
}

func TestDestroySessionReleasesPendingQueryDrainWork(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}

	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin pending query drain work: %v", err)
	}
	pool.sessions["session-1"] = &Session{
		ID:      "session-1",
		queries: map[string]*QueryHandle{"query-1": {Query: "SELECT 1", finishDrain: finishDrain}},
		txns:    make(map[string]*trackedTx),
	}

	pool.BeginDrain()
	if err := pool.DestroySession("session-1"); err != nil {
		t.Fatalf("destroy session: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected destroying the session to release pending query drain work")
	}
}

func TestReapIdleTransactionsReleasesDrainWork(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin transaction drain work: %v", err)
	}
	ttx := &trackedTx{finishDrain: finishDrain}
	ttx.lastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())
	pool.sessions["session-1"] = &Session{
		ID:       "session-1",
		queries:  make(map[string]*QueryHandle),
		txns:     map[string]*trackedTx{"txn-1": ttx},
		txnOwner: map[string]string{"txn-1": "alice"},
	}

	pool.BeginDrain()
	pool.reapIdle(time.Now())

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected idle transaction reaper to release transaction drain work")
	}
}

func TestReapIdleTransactionsKeepsDrainWorkWhenTransactionActive(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin transaction drain work: %v", err)
	}
	ttx := &trackedTx{finishDrain: finishDrain}
	ttx.lastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())
	ttx.activeWork.Store(1)
	session := &Session{
		ID:       "session-1",
		queries:  make(map[string]*QueryHandle),
		txns:     map[string]*trackedTx{"txn-1": ttx},
		txnOwner: map[string]string{"txn-1": "alice"},
	}
	pool.sessions[session.ID] = session

	pool.BeginDrain()
	pool.reapIdle(time.Now())
	assertSessionMutexUnlocked(t, session)

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("active Flight SQL transaction must keep drain work open")
	}
	session.mu.RLock()
	_, stillTracked := session.txns["txn-1"]
	session.mu.RUnlock()
	if !stillTracked {
		t.Fatal("active Flight SQL transaction should not be reaped")
	}
	releaseDrainFunc(finishDrain)
}

func TestReapIdleRawSQLTransactionReleasesDrainWork(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("begin raw transaction: %v", err)
	}

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin raw transaction drain work: %v", err)
	}
	session := &Session{
		ID:         "session-1",
		Conn:       conn,
		queries:    make(map[string]*QueryHandle),
		txns:       make(map[string]*trackedTx),
		txnOwner:   make(map[string]string),
		sqlTxDrain: finishDrain,
	}
	session.sqlTxActive.Store(true)
	session.lastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())
	pool.sessions[session.ID] = session

	pool.BeginDrain()
	pool.reapIdle(time.Now())

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("expected idle raw SQL transaction reaper to release drain work")
	}
	if session.sqlTxActive.Load() {
		t.Fatal("expected idle raw SQL transaction reaper to clear sqlTxActive")
	}
	if session.sqlTxDrain != nil {
		t.Fatal("expected idle raw SQL transaction reaper to clear sqlTxDrain")
	}
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err == nil {
		t.Fatal("expected COMMIT after idle reaper rollback to fail")
	}
}

func TestReapIdleRawSQLTransactionKeepsDrainWorkWhenConnWorkActive(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin raw transaction drain work: %v", err)
	}
	session := &Session{
		ID:         "session-1",
		queries:    make(map[string]*QueryHandle),
		txns:       make(map[string]*trackedTx),
		txnOwner:   make(map[string]string),
		sqlTxDrain: finishDrain,
	}
	session.sqlTxActive.Store(true)
	session.sqlTxLastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())
	pool.sessions[session.ID] = session

	endWork, ok := session.beginConnWork()
	if !ok {
		t.Fatal("beginConnWork failed")
	}
	defer endWork()
	if session.progress.queryActive.Load() {
		t.Fatal("conn work guard must not mutate queryActive")
	}
	session.sqlTxLastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())

	pool.BeginDrain()
	pool.reapIdle(time.Now())
	assertSessionMutexUnlocked(t, session)

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("active raw SQL transaction connection work must keep drain work open")
	}
	if !session.sqlTxActive.Load() {
		t.Fatal("active raw SQL transaction connection work must keep sqlTxActive")
	}
	if session.sqlTxDrain == nil {
		t.Fatal("active raw SQL transaction connection work must keep sqlTxDrain")
	}
	releaseDrainFunc(session.sqlTxDrain)
}

func TestReapIdleRawSQLTransactionKeepsDrainWorkWhenRollbackCannotRun(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin raw transaction drain work: %v", err)
	}
	session := &Session{
		ID:         "session-1",
		queries:    make(map[string]*QueryHandle),
		txns:       make(map[string]*trackedTx),
		txnOwner:   make(map[string]string),
		sqlTxDrain: finishDrain,
	}
	session.sqlTxActive.Store(true)
	session.sqlTxLastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())
	pool.sessions[session.ID] = session

	pool.BeginDrain()
	pool.reapIdle(time.Now())
	assertSessionMutexUnlocked(t, session)

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if pool.WaitForDrain(shortCtx) {
		t.Fatal("failed raw SQL transaction rollback must keep drain work open")
	}
	if !session.sqlTxActive.Load() {
		t.Fatal("failed raw SQL transaction rollback must keep sqlTxActive")
	}
	if session.sqlTxDrain == nil {
		t.Fatal("failed raw SQL transaction rollback must keep sqlTxDrain")
	}
	releaseDrainFunc(session.sqlTxDrain)
}

func assertSessionMutexUnlocked(t *testing.T, session *Session) {
	t.Helper()
	unlocked := make(chan struct{})
	go func() {
		session.mu.Lock()
		close(unlocked)
		session.mu.Unlock()
	}()
	select {
	case <-unlocked:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("session mutex remained locked after reapIdle")
	}
}

func TestDestroySessionPreventsLateQueryHandleDrainLeak(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin query drain work: %v", err)
	}

	if err := pool.DestroySession(session.ID); err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if addQueryHandle(session, "query-1", &QueryHandle{Query: "SELECT 1", finishDrain: finishDrain, createdAt: time.Now()}) {
		t.Fatal("late query handle registration should fail after DestroySession")
	}
	releaseDrainFunc(finishDrain)

	pool.BeginDrain()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("late query handle registration leaked drain work")
	}
}

func TestDestroySessionPreventsLateSQLTransactionDrainLeak(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin transaction drain work: %v", err)
	}

	if err := pool.DestroySession(session.ID); err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if session.setSQLTransactionDrain(finishDrain) {
		t.Fatal("late raw SQL transaction drain registration should fail after DestroySession")
	}
	releaseDrainFunc(finishDrain)

	pool.BeginDrain()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("late raw SQL transaction drain registration leaked drain work")
	}
}

func TestDestroySessionPreventsLateTrackedTransactionDrainLeak(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	finishDrain, err := pool.beginDrainWork(false)
	if err != nil {
		t.Fatalf("begin transaction drain work: %v", err)
	}

	if err := pool.DestroySession(session.ID); err != nil {
		t.Fatalf("destroy session: %v", err)
	}
	if addTrackedTransaction(session, "txn-1", &trackedTx{finishDrain: finishDrain}) {
		t.Fatal("late tracked transaction registration should fail after DestroySession")
	}
	releaseDrainFunc(finishDrain)

	pool.BeginDrain()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !pool.WaitForDrain(waitCtx) {
		t.Fatal("late tracked transaction registration leaked drain work")
	}
}

func TestDestroySessionWaitsForActiveConnectionWorkBeforeCleanup(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		DB:       db,
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	session.connMu.Lock()

	done := make(chan error, 1)
	go func() {
		done <- pool.DestroySession(session.ID)
	}()

	select {
	case err := <-done:
		t.Fatalf("DestroySession returned before active connection work finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	session.connMu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DestroySession: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DestroySession did not finish after active connection work ended")
	}
}

func TestDestroySessionWaitsForAcceptedConnectionWorkBeforeCleanup(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		DB:       db,
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	endWork, ok := session.beginConnWork()
	if !ok {
		t.Fatal("beginConnWork failed")
	}

	done := make(chan error, 1)
	go func() {
		done <- pool.DestroySession(session.ID)
	}()

	select {
	case err := <-done:
		t.Fatalf("DestroySession returned before accepted connection work finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	endWork()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DestroySession: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DestroySession did not finish after accepted connection work ended")
	}
}

func TestCloseAllWaitsForActiveConnectionWorkBeforeClose(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		DB:       db,
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	session.connMu.Lock()

	done := make(chan struct{})
	go func() {
		pool.CloseAll()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("CloseAll returned before active connection work finished")
	case <-time.After(50 * time.Millisecond):
	}

	session.connMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CloseAll did not finish after active connection work ended")
	}
}

func TestCloseAllWaitsForAcceptedConnectionWorkBeforeClose(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	session := &Session{
		ID:       "session-1",
		DB:       db,
		Conn:     conn,
		queries:  make(map[string]*QueryHandle),
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions[session.ID] = session
	endWork, ok := session.beginConnWork()
	if !ok {
		t.Fatal("beginConnWork failed")
	}

	done := make(chan struct{})
	go func() {
		pool.CloseAll()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("CloseAll returned before accepted connection work finished")
	case <-time.After(50 * time.Millisecond):
	}

	endWork()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CloseAll did not finish after accepted connection work ended")
	}
}

// A GetFlightInfo whose matching DoGet never arrives must not hold the drain
// open forever: the reaper releases drain tokens stranded on stale ad-hoc query
// handles while leaving fresh handles intact.
func TestReapIdleReleasesAbandonedHandleDrains(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
	}
	mustToken := func() func() {
		f, err := pool.beginDrainWork(false)
		if err != nil {
			t.Fatalf("begin drain work: %v", err)
		}
		return f
	}
	stale := time.Now().Add(-handleIdleTimeout - time.Minute)
	fresh := time.Now()

	staleAdhoc := mustToken()
	freshAdhoc := mustToken()

	sess := &Session{
		ID: "s1",
		queries: map[string]*QueryHandle{
			"query-1": {Query: "SELECT 1", createdAt: stale, finishDrain: staleAdhoc},
			"query-2": {Query: "SELECT 2", createdAt: fresh, finishDrain: freshAdhoc},
		},
		txns:     make(map[string]*trackedTx),
		txnOwner: make(map[string]string),
	}
	pool.sessions["s1"] = sess

	if got := pool.ActiveDrainWork(); got != 2 {
		t.Fatalf("activeWork=%d want 2 before reap", got)
	}

	pool.reapIdle(time.Now())

	if got := pool.ActiveDrainWork(); got != 1 {
		t.Fatalf("activeWork=%d want 1 (only the fresh ad-hoc handle should remain)", got)
	}

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if _, ok := sess.queries["query-1"]; ok {
		t.Error("stale ad-hoc handle query-1 was not reaped")
	}
	if _, ok := sess.queries["query-2"]; !ok {
		t.Error("fresh ad-hoc handle query-2 was wrongly reaped")
	}
}
