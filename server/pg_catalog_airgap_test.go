package server

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// warnCollector captures WARN records emitted during initPgCatalog so the test
// can assert no macro/view failed to register.
type warnCollector struct {
	mu   sync.Mutex
	msgs []string
}

func (w *warnCollector) Enabled(_ context.Context, level slog.Level) bool { return level >= slog.LevelWarn }
func (w *warnCollector) Handle(_ context.Context, r slog.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	line := r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.String(); return true })
	w.msgs = append(w.msgs, line)
	return nil
}
func (w *warnCollector) WithAttrs([]slog.Attr) slog.Handler { return w }
func (w *warnCollector) WithGroup(string) slog.Handler      { return w }

// Worker pods run initPgCatalog during warmup in an environment where DuckDB
// extension downloads either fail or — worse — hang for minutes (the egress
// policy silently drops the autoinstall fetch, which goes over plain HTTP port
// 80). A single catalog statement that needs a non-statically-loaded extension
// therefore stalls worker warmup past the control plane's connect budget and
// the worker is reaped (the inet_server_addr CAST(NULL AS INET) incident).
// Run the full catalog init with autoinstall/autoload disabled and require
// that every statement registers: no compatibility object may depend on an
// extension that isn't statically linked into the binary.
func TestInitPgCatalogIsAirgapSafe(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("SET autoinstall_known_extensions=false"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET autoload_known_extensions=false"); err != nil {
		t.Fatal(err)
	}

	collector := &warnCollector{}
	prev := slog.Default()
	slog.SetDefault(slog.New(collector))
	defer slog.SetDefault(prev)

	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}

	for _, m := range collector.msgs {
		if strings.Contains(m, "Failed to register") || strings.Contains(m, "extension") {
			t.Errorf("catalog statement requires a non-static extension (would hang worker warmup): %s", m)
		}
	}

	// The shadowing macro must actually win over DuckDB's wrong-typed builtin —
	// a registration failure silently falls back to the builtin, so assert the
	// macro's VARCHAR-typed NULL is what resolves.
	var typ string
	if err := db.QueryRow("SELECT typeof(inet_server_addr())").Scan(&typ); err != nil {
		t.Fatalf("inet_server_addr: %v", err)
	}
	if typ != "VARCHAR" {
		t.Errorf("inet_server_addr typeof = %q, want VARCHAR (macro not registered; builtin fallback)", typ)
	}
}
