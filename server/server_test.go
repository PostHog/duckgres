package server

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockRefreshExecer struct {
	mu            sync.Mutex
	secretCalls   int
	rollbackCalls int
	secretErrFn   func(callNum int) error
}

type mockDuckLakeExecer struct {
	mu        sync.Mutex
	errs      []error
	queries   []string
	execCalls int
}

func (m *mockDuckLakeExecer) Exec(query string, _ ...any) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, query)
	m.execCalls++
	if len(m.errs) == 0 {
		return nil, nil
	}
	err := m.errs[0]
	m.errs = m.errs[1:]
	return nil, err
}

func (m *mockDuckLakeExecer) calls() (int, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	queries := append([]string(nil), m.queries...)
	return m.execCalls, queries
}

func (m *mockRefreshExecer) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	trimmed := strings.TrimSpace(strings.ToUpper(query))
	if trimmed == "ROLLBACK" {
		m.rollbackCalls++
		return nil, nil
	}

	m.secretCalls++
	if m.secretErrFn != nil {
		if err := m.secretErrFn(m.secretCalls); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (m *mockRefreshExecer) calls() (secretCalls, rollbackCalls int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.secretCalls, m.rollbackCalls
}

func waitForCalls(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for expected refresh calls")
}

func TestParseExtensionName(t *testing.T) {
	tests := []struct {
		input       string
		wantName    string
		wantInstall string
	}{
		{"ducklake", "ducklake", "ducklake"},
		{"httpfs", "httpfs", "httpfs"},
		{"cache_httpfs FROM community", "cache_httpfs", "cache_httpfs FROM community"},
		{"cache_httpfs from community", "cache_httpfs", "cache_httpfs from community"},
		{"cache_httpfs FROM COMMUNITY", "cache_httpfs", "cache_httpfs FROM COMMUNITY"},
		{"my_ext FROM my_repo", "my_ext", "my_ext FROM my_repo"},
		{"ext  FROM  source", "ext", "ext  FROM  source"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			name, installCmd := parseExtensionName(tt.input)
			if name != tt.wantName {
				t.Errorf("parseExtensionName(%q) name = %q, want %q", tt.input, name, tt.wantName)
			}
			if installCmd != tt.wantInstall {
				t.Errorf("parseExtensionName(%q) installCmd = %q, want %q", tt.input, installCmd, tt.wantInstall)
			}
		})
	}
}

func TestNeedsCredentialRefresh(t *testing.T) {
	tests := []struct {
		name string
		cfg  DuckLakeConfig
		want bool
	}{
		{
			"credential_chain with object store",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/", S3Provider: "credential_chain"},
			true,
		},
		{
			"implicit credential_chain (no access key, no provider)",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/"},
			true,
		},
		{
			"config provider with explicit credentials",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/", S3Provider: "config", S3AccessKey: "key", S3SecretKey: "secret"},
			false,
		},
		{
			"implicit config provider (access key set)",
			DuckLakeConfig{ObjectStore: "s3://bucket/path/", S3AccessKey: "key", S3SecretKey: "secret"},
			false,
		},
		{
			"no object store",
			DuckLakeConfig{MetadataStore: "postgres:..."},
			false,
		},
		{
			"empty config",
			DuckLakeConfig{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := needsCredentialRefresh(tt.cfg)
			if got != tt.want {
				t.Errorf("needsCredentialRefresh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildDuckLakePreAttachStatements(t *testing.T) {
	tests := []struct {
		name string
		cfg  DuckLakeConfig
		want []string
	}{
		{
			name: "default disables metadata tls cache",
			cfg: DuckLakeConfig{
				DisableMetadataThreadLocalCache: true,
			},
			want: []string{"SET GLOBAL pg_pool_enable_thread_local_cache = false"},
		},
		{
			name: "disable metadata tls cache",
			cfg: DuckLakeConfig{
				DisableMetadataThreadLocalCache: true,
			},
			want: []string{"SET GLOBAL pg_pool_enable_thread_local_cache = false"},
		},
		{
			name: "explicitly keep metadata tls cache enabled",
			cfg: DuckLakeConfig{
				DisableMetadataThreadLocalCache: false,
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDuckLakePreAttachStatements(tt.cfg)
			if len(got) != len(tt.want) {
				t.Fatalf("statement count = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("statement[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestIsMissingDuckLakePoolSettingError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "matches duckdb text",
			err:  errors.New("Catalog Error: unrecognized configuration parameter \"pg_pool_enable_thread_local_cache\""),
			want: true,
		},
		{
			name: "different error",
			err:  errors.New("permission denied"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMissingDuckLakePoolSettingError(tt.err); got != tt.want {
				t.Fatalf("isMissingDuckLakePoolSettingError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestApplyDuckLakePreAttachSettingsWith_IgnoresUnsupportedSettingWhenLoaderFails(t *testing.T) {
	execer := &mockDuckLakeExecer{
		errs: []error{
			errors.New("Catalog Error: unrecognized configuration parameter \"pg_pool_enable_thread_local_cache\""),
		},
	}

	loadCalls := 0
	err := applyDuckLakePreAttachSettingsWith(execer, func() error {
		loadCalls++
		return errors.New("offline")
	}, DuckLakeConfig{DisableMetadataThreadLocalCache: true})
	if err != nil {
		t.Fatalf("expected unsupported pre-attach setting to be best-effort, got %v", err)
	}
	if loadCalls != 1 {
		t.Fatalf("expected one load attempt, got %d", loadCalls)
	}
	if calls, _ := execer.calls(); calls != 1 {
		t.Fatalf("expected one exec attempt, got %d", calls)
	}
}

func TestApplyDuckLakePreAttachSettingsWith_IgnoresUnsupportedSettingAfterLoad(t *testing.T) {
	execer := &mockDuckLakeExecer{
		errs: []error{
			errors.New("Catalog Error: unrecognized configuration parameter \"pg_pool_enable_thread_local_cache\""),
			errors.New("Catalog Error: unrecognized configuration parameter \"pg_pool_enable_thread_local_cache\""),
		},
	}

	loadCalls := 0
	err := applyDuckLakePreAttachSettingsWith(execer, func() error {
		loadCalls++
		return nil
	}, DuckLakeConfig{DisableMetadataThreadLocalCache: true})
	if err != nil {
		t.Fatalf("expected unsupported pre-attach setting to be ignored after load retry, got %v", err)
	}
	if loadCalls != 1 {
		t.Fatalf("expected one load attempt, got %d", loadCalls)
	}
	if calls, _ := execer.calls(); calls != 2 {
		t.Fatalf("expected two exec attempts, got %d", calls)
	}
}

func TestConfigureDuckLakeMetadataPoolRunsExpectedStatement(t *testing.T) {
	execer := &mockDuckLakeExecer{}
	configureDuckLakeMetadataPool(execer)

	calls, queries := execer.calls()
	if calls != 1 {
		t.Fatalf("expected one pool-config exec, got %d", calls)
	}
	if len(queries) != 1 || !strings.Contains(queries[0], "postgres_configure_pool") {
		t.Fatalf("expected postgres_configure_pool statement, got %v", queries)
	}
}

func TestStartCredentialRefresh_NoOpForStaticCredentials(t *testing.T) {
	// Static credentials should return a no-op stop function immediately
	stop := StartCredentialRefresh(nil, DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "config",
		S3AccessKey: "key",
		S3SecretKey: "secret",
	})
	stop() // Should not panic
	stop() // Calling twice should be safe (sync.Once)
}

func TestStartCredentialRefresh_NoOpForNoObjectStore(t *testing.T) {
	stop := StartCredentialRefresh(nil, DuckLakeConfig{})
	stop() // Should not panic
}

func TestStartCredentialRefresh_StopsCleanly(t *testing.T) {
	// Open a real DuckDB connection to test that the refresh goroutine works
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Install httpfs extension so CREATE SECRET works
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skip("httpfs extension not available:", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skip("httpfs extension not loadable:", err)
	}

	// Use credential_chain config to trigger refresh
	cfg := DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "credential_chain",
	}

	stop := StartCredentialRefresh(db, cfg)

	// Give the goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Stop should not hang or panic
	stop()
}

func TestStartCredentialRefresh_WorksWithPinnedConn(t *testing.T) {
	// Simulate the worker path: MaxOpenConns(1) with the sole connection
	// pinned via db.Conn(). Passing the pinned *sql.Conn must not deadlock.
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skip("httpfs extension not available:", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skip("httpfs extension not loadable:", err)
	}

	// Pin the pool's only connection — exactly what the worker does.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	cfg := DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "credential_chain",
	}

	stop := StartCredentialRefresh(conn, cfg)

	// Give the goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Stop should not hang or panic
	stop()
}

func TestStartCredentialRefresh_DoesNotRollbackDuringActiveTransaction(t *testing.T) {
	oldInterval := credentialRefreshInterval
	credentialRefreshInterval = 5 * time.Millisecond
	defer func() { credentialRefreshInterval = oldInterval }()

	execer := &mockRefreshExecer{
		secretErrFn: func(_ int) error {
			return errors.New("TransactionContext Error: Current transaction is aborted (please ROLLBACK)")
		},
	}

	var txnActive atomic.Bool
	txnActive.Store(true)

	stop := StartCredentialRefresh(execer, DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "credential_chain",
	}, txnActive.Load)

	waitForCalls(t, 250*time.Millisecond, func() bool {
		secretCalls, _ := execer.calls()
		return secretCalls > 0
	})

	stop()

	_, rollbackCalls := execer.calls()
	if rollbackCalls != 0 {
		t.Fatalf("expected no rollback while transaction is active, got %d", rollbackCalls)
	}
}

func TestNewRejectsUnsupportedACMEDNSProvider(t *testing.T) {
	_, err := New(Config{
		ACMEDomain:      "test.us.duckgres.com",
		ACMEDNSProvider: "cloudflare",
	})
	if err == nil {
		t.Fatal("expected error for unsupported ACME DNS provider")
	}
	if !strings.Contains(err.Error(), "unsupported ACME DNS provider") {
		t.Fatalf("expected unsupported provider error, got: %v", err)
	}
}

func TestNewRejectsACMEDNSProviderWithoutDomain(t *testing.T) {
	_, err := New(Config{
		ACMEDNSProvider: "route53",
	})
	if err == nil {
		t.Fatal("expected error when ACME DNS provider is set without ACME domain")
	}
	if !strings.Contains(err.Error(), "ACME DNS provider requires ACME domain") {
		t.Fatalf("expected missing domain error, got: %v", err)
	}
}

func TestStartCredentialRefresh_RollbackAndRetryWhenNoActiveTransaction(t *testing.T) {
	oldInterval := credentialRefreshInterval
	credentialRefreshInterval = 5 * time.Millisecond
	defer func() { credentialRefreshInterval = oldInterval }()

	execer := &mockRefreshExecer{
		secretErrFn: func(callNum int) error {
			if callNum == 1 {
				return errors.New("TransactionContext Error: Current transaction is aborted (please ROLLBACK)")
			}
			return nil
		},
	}

	stop := StartCredentialRefresh(execer, DuckLakeConfig{
		ObjectStore: "s3://bucket/path/",
		S3Provider:  "credential_chain",
	}, func() bool { return false })

	waitForCalls(t, 250*time.Millisecond, func() bool {
		secretCalls, rollbackCalls := execer.calls()
		return secretCalls >= 2 && rollbackCalls >= 1
	})

	stop()

	secretCalls, rollbackCalls := execer.calls()
	if rollbackCalls == 0 {
		t.Fatalf("expected rollback before retry when no transaction is active, got %d", rollbackCalls)
	}
	if secretCalls < 2 {
		t.Fatalf("expected retry after rollback, got %d secret executions", secretCalls)
	}
}

func TestOpenBaseDBInMemoryByDefault(t *testing.T) {
	cfg := Config{}
	db, err := openBaseDB(cfg, "testuser")
	if err != nil {
		t.Fatalf("openBaseDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	var dbName string
	err = db.QueryRow("SELECT current_database()").Scan(&dbName)
	if err != nil {
		t.Fatalf("failed to query current_database(): %v", err)
	}
	if dbName != "memory" {
		t.Fatalf("expected in-memory database (current_database()='memory'), got %q", dbName)
	}
}

func TestOpenBaseDBFilePersistence(t *testing.T) {
	dataDir := t.TempDir()
	cfg := Config{
		FilePersistence: true,
		DataDir:         dataDir,
	}
	db, err := openBaseDB(cfg, "alice")
	if err != nil {
		t.Fatalf("openBaseDB failed: %v", err)
	}

	// Write data
	if _, err := db.Exec("CREATE TABLE test_persist (id INTEGER)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test_persist VALUES (42)"); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_ = db.Close()

	// Reopen the same file and verify data survives
	db2, err := openBaseDB(cfg, "alice")
	if err != nil {
		t.Fatalf("openBaseDB (reopen) failed: %v", err)
	}
	defer func() { _ = db2.Close() }()

	var val int
	err = db2.QueryRow("SELECT id FROM test_persist").Scan(&val)
	if err != nil {
		t.Fatalf("failed to read persisted data: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected persisted value 42, got %d", val)
	}
}

func TestOpenBaseDBFilePersistenceFallsBackWithoutDataDir(t *testing.T) {
	cfg := Config{
		FilePersistence: true,
		// DataDir intentionally empty
	}
	db, err := openBaseDB(cfg, "testuser")
	if err != nil {
		t.Fatalf("openBaseDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	var dbName string
	err = db.QueryRow("SELECT current_database()").Scan(&dbName)
	if err != nil {
		t.Fatalf("failed to query current_database(): %v", err)
	}
	if dbName != "memory" {
		t.Fatalf("expected fallback to in-memory when DataDir is empty, got %q", dbName)
	}
}

func TestOpenBaseDBFilePersistenceFallsBackWithoutUsername(t *testing.T) {
	cfg := Config{
		FilePersistence: true,
		DataDir:         t.TempDir(),
	}
	db, err := openBaseDB(cfg, "")
	if err != nil {
		t.Fatalf("openBaseDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	var dbName string
	err = db.QueryRow("SELECT current_database()").Scan(&dbName)
	if err != nil {
		t.Fatalf("failed to query current_database(): %v", err)
	}
	if dbName != "memory" {
		t.Fatalf("expected fallback to in-memory when username is empty, got %q", dbName)
	}
}

func TestHasCacheHTTPFS(t *testing.T) {
	tests := []struct {
		name       string
		extensions []string
		want       bool
	}{
		{"present bare", []string{"ducklake", "cache_httpfs"}, true},
		{"present with source", []string{"ducklake", "cache_httpfs FROM community"}, true},
		{"absent", []string{"ducklake", "httpfs"}, false},
		{"empty list", []string{}, false},
		{"nil list", nil, false},
		{"similar name", []string{"not_cache_httpfs"}, false},
		{"substring match", []string{"cache_httpfs_v2 FROM community"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCacheHTTPFS(tt.extensions)
			if got != tt.want {
				t.Errorf("hasCacheHTTPFS(%v) = %v, want %v", tt.extensions, got, tt.want)
			}
		})
	}
}

func TestOpenBaseDBFilePersistenceRejectsPathTraversal(t *testing.T) {
	dataDir := t.TempDir()
	cfg := Config{
		FilePersistence: true,
		DataDir:         dataDir,
	}

	cases := []struct {
		name     string
		username string
	}{
		{"parent directory", "../etc/evil"},
		{"slash in name", "foo/bar"},
		{"backslash dot-dot", "..\\windows"},
		{"dot-dot between names", "alice/../bob"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := openBaseDB(cfg, tc.username)
			if err == nil {
				_ = db.Close()
				t.Fatalf("expected error for username %q, got nil", tc.username)
			}
			if !strings.Contains(err.Error(), "invalid username for file persistence") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestOpenBaseDBCreatesDataDir(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "deep", "nested", "dir")

	cfg := Config{
		FilePersistence: true,
		DataDir:         nested,
	}
	db, err := openBaseDB(cfg, "testuser")
	if err != nil {
		t.Fatalf("openBaseDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	info, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("data directory was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected directory, got file")
	}
}

func TestFileDBPoolRefCounting(t *testing.T) {
	dataDir := t.TempDir()
	s := &Server{
		cfg: Config{
			FilePersistence: true,
			DataDir:         dataDir,
		},
		fileDBs:     make(map[string]*fileDBEntry),
		duckLakeSem: make(chan struct{}, 1),
	}

	// First acquire
	db1, err := s.acquireFileDB("alice", false)
	if err != nil {
		t.Fatalf("first acquireFileDB failed: %v", err)
	}

	// Second acquire should return the same *sql.DB
	db2, err := s.acquireFileDB("alice", false)
	if err != nil {
		t.Fatalf("second acquireFileDB failed: %v", err)
	}
	if db1 != db2 {
		t.Fatal("expected same *sql.DB for same user, got different instances")
	}

	// Check ref count is 2
	s.fileDBsMu.Lock()
	refs := s.fileDBs["alice"].refs
	s.fileDBsMu.Unlock()
	if refs != 2 {
		t.Fatalf("expected refs=2, got %d", refs)
	}

	// Release one — DB should still be open
	s.releaseFileDB("alice")
	s.fileDBsMu.Lock()
	entry, exists := s.fileDBs["alice"]
	s.fileDBsMu.Unlock()
	if !exists {
		t.Fatal("entry removed too early (refs should be 1)")
	}
	if entry.refs != 1 {
		t.Fatalf("expected refs=1, got %d", entry.refs)
	}

	// DB should still work
	if err := db1.Ping(); err != nil {
		t.Fatalf("db should still be usable: %v", err)
	}

	// Release last — DB should be closed and removed
	s.releaseFileDB("alice")
	s.fileDBsMu.Lock()
	_, exists = s.fileDBs["alice"]
	s.fileDBsMu.Unlock()
	if exists {
		t.Fatal("entry should be removed after last release")
	}
}
