package duckdbservice

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type recordingRequiredExtensionExecer struct {
	queries []string
	err     error
}

func (e *recordingRequiredExtensionExecer) Exec(query string, _ ...any) (sql.Result, error) {
	e.queries = append(e.queries, query)
	return nil, e.err
}

func TestLoadWorkerRequiredExtensionsLoadsPostgresScanner(t *testing.T) {
	execer := &recordingRequiredExtensionExecer{}

	if err := loadWorkerRequiredExtensions(execer); err != nil {
		t.Fatalf("loadWorkerRequiredExtensions: %v", err)
	}
	if len(execer.queries) != 1 || execer.queries[0] != "LOAD postgres_scanner" {
		t.Fatalf("queries = %q, want [LOAD postgres_scanner]", execer.queries)
	}
}

func TestLoadWorkerRequiredExtensionsFailsClosed(t *testing.T) {
	execer := &recordingRequiredExtensionExecer{err: errors.New("extension unavailable")}

	err := loadWorkerRequiredExtensions(execer)
	if err == nil {
		t.Fatal("expected required extension load to fail")
	}
	if !strings.Contains(err.Error(), "postgres_scanner") {
		t.Fatalf("error = %q, want postgres_scanner context", err)
	}
}
