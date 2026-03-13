package server

import (
	"os"
	"sync"
	"testing"
	"time"
)

func makeTestEntry(query string) QueryLogEntry {
	return QueryLogEntry{
		EventTime:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		QueryDurationMs: 42,
		Type:            "QueryFinish",
		Query:           query,
		QueryKind:       "Select",
		NormalizedHash:  12345,
		ResultRows:      10,
		UserName:        "testuser",
		CurrentDatabase: "testdb",
		ClientAddress:   "127.0.0.1",
		ClientPort:      5432,
		ApplicationName: "test",
		PID:             1001,
		WorkerID:        0,
		Protocol:        "simple",
	}
}

func TestWAL_AppendAndReadAll(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	entries := []QueryLogEntry{
		makeTestEntry("SELECT 1"),
		makeTestEntry("SELECT 2"),
		makeTestEntry("INSERT INTO t VALUES (1)"),
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	got, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(entries) {
		t.Fatalf("ReadAll: got %d entries, want %d", len(got), len(entries))
	}

	for i, e := range got {
		if e.Query != entries[i].Query {
			t.Errorf("entry %d: got query %q, want %q", i, e.Query, entries[i].Query)
		}
		if e.QueryDurationMs != entries[i].QueryDurationMs {
			t.Errorf("entry %d: got duration %d, want %d", i, e.QueryDurationMs, entries[i].QueryDurationMs)
		}
		if e.UserName != entries[i].UserName {
			t.Errorf("entry %d: got username %q, want %q", i, e.UserName, entries[i].UserName)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWAL_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write entries
	for i := 0; i < 5; i++ {
		if err := w.Append(makeTestEntry("SELECT " + string(rune('A'+i)))); err != nil {
			t.Fatal(err)
		}
	}

	// Close without "flushing" (simulating crash — entries stay on disk)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify recovery
	w2, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("crash recovery: got %d entries, want 5", len(got))
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(makeTestEntry("SELECT 1")); err != nil {
		t.Fatal(err)
	}

	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}

	got, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("after truncate: got %d entries, want 0", len(got))
	}

	// Can still append after truncate
	if err := w.Append(makeTestEntry("SELECT 2")); err != nil {
		t.Fatal(err)
	}

	got, err = w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("after truncate+append: got %d entries, want 1", len(got))
	}
}

func TestWAL_MaxSizeBackpressure(t *testing.T) {
	dir := t.TempDir()
	// Very small max size
	w, err := newWAL(dir, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// First append should succeed
	if err := w.Append(makeTestEntry("S")); err != nil {
		t.Fatal(err)
	}

	// Keep appending until we hit the size limit
	var hitLimit bool
	for i := 0; i < 100; i++ {
		if err := w.Append(makeTestEntry("SELECT " + string(rune('0'+i)))); err != nil {
			hitLimit = true
			break
		}
	}
	if !hitLimit {
		t.Fatal("expected size limit error, but none occurred")
	}
}

func TestWAL_CorruptedRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write 3 valid entries
	for i := 0; i < 3; i++ {
		if err := w.Append(makeTestEntry("SELECT valid")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the file by appending garbage
	f, err := os.OpenFile(dir+"/query_log.wal", os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Write a valid-looking header but corrupt payload
	f.Write([]byte{0, 0, 0, 10, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Write([]byte("corrupted!"))
	f.Close()

	// Reopen and read — should get the 3 valid entries
	w2, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("corrupted WAL: got %d valid entries, want 3", len(got))
	}
}

func TestWAL_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const goroutines = 10
	const entriesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				if err := w.Append(makeTestEntry("SELECT concurrent")); err != nil {
					t.Errorf("goroutine %d, entry %d: %v", id, i, err)
				}
			}
		}(g)
	}
	wg.Wait()

	got, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	expected := goroutines * entriesPerGoroutine
	if len(got) != expected {
		t.Fatalf("concurrent append: got %d entries, want %d", len(got), expected)
	}
}

func TestWAL_GroupCommit(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const count = 20
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			if err := w.Append(makeTestEntry("SELECT group")); err != nil {
				t.Errorf("group commit entry %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	got, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != count {
		t.Fatalf("group commit: got %d entries, want %d", len(got), count)
	}
}

func TestWAL_TranspiledQueryField(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	transpiled := "SELECT * FROM pg_class_full"
	entry := makeTestEntry("SELECT * FROM pg_catalog.pg_class")
	entry.TranspiledQuery = &transpiled
	entry.IsTranspiled = true

	if err := w.Append(entry); err != nil {
		t.Fatal(err)
	}

	got, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if got[0].TranspiledQuery == nil {
		t.Fatal("TranspiledQuery is nil, expected non-nil")
	}
	if *got[0].TranspiledQuery != transpiled {
		t.Errorf("TranspiledQuery = %q, want %q", *got[0].TranspiledQuery, transpiled)
	}
	if !got[0].IsTranspiled {
		t.Error("IsTranspiled = false, want true")
	}
}
