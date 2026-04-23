package duckdbservice

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

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
