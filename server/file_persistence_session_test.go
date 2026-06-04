package server

import (
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/lib/pq"
)

func TestFilePersistenceStandaloneConnectionInitializesSession(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")
	if err := EnsureCertificates(certFile, keyFile); err != nil {
		t.Fatalf("generate certs: %v", err)
	}

	port := freeTCPPort(t)
	srv, err := New(Config{
		Host:            "127.0.0.1",
		Port:            port,
		DataDir:         tmpDir,
		FilePersistence: true,
		TLSCertFile:     certFile,
		TLSKeyFile:      keyFile,
		Users:           map[string]string{"testuser": "testpass"},
		RateLimit: RateLimitConfig{
			MaxConnections: 100,
		},
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	waitForTCP(t, port)

	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require connect_timeout=5", port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open postgres connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("ping file-persistence session: %v", err)
	}

	var currentDB string
	if err := db.QueryRow("SELECT current_database()").Scan(&currentDB); err != nil {
		_ = db.Close()
		t.Fatalf("query current_database(): %v", err)
	}
	if currentDB != "test" {
		_ = db.Close()
		t.Fatalf("current_database() = %q, want %q", currentDB, "test")
	}

	if _, err := db.Exec("CREATE TABLE fp_session_probe (id INTEGER)"); err != nil {
		_ = db.Close()
		t.Fatalf("create persisted table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO fp_session_probe VALUES (42)"); err != nil {
		_ = db.Close()
		t.Fatalf("insert persisted row: %v", err)
	}
	_ = db.Close()

	db2, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open second postgres connection: %v", err)
	}

	var id int
	if err := db2.QueryRow("SELECT id FROM fp_session_probe").Scan(&id); err != nil {
		_ = db2.Close()
		t.Fatalf("query persisted row on second connection: %v", err)
	}
	if id != 42 {
		_ = db2.Close()
		t.Fatalf("persisted row id = %d, want 42", id)
	}
	_ = db2.Close()

	_ = srv.Close()
	if err := <-errCh; err != nil {
		t.Fatalf("server returned error: %v", err)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitForTCP(t *testing.T, port int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("server did not listen on port %d: %v", port, lastErr)
}
