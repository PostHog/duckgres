package duckdbservice

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// The whole point of the classifier is that Internal/Fatal are instance-killing
// while the superficially similar failure classes (OOM, transaction conflict)
// are NOT — those must keep retrying in place. Retiring a worker on a DuckLake
// commit conflict would turn ordinary write contention into pod churn.
func TestIsInstanceFatalErrorTypedClasses(t *testing.T) {
	fatal := []error{
		&duckdb.Error{Type: duckdb.ErrorTypeInternal, Msg: "Calling GetValueInternal on a value that is NULL"},
		&duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "anything"},
	}
	for _, err := range fatal {
		if !isInstanceFatalError(err) {
			t.Errorf("expected instance-fatal: %v", err)
		}
	}

	notFatal := []error{
		nil,
		errors.New("some ordinary failure"),
		&duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate"},
		&duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "Transaction conflict"},
		&duckdb.Error{Type: duckdb.ErrorTypeCatalog, Msg: "Catalog Error: Table does not exist"},
		&duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: No space left on device"},
	}
	for _, err := range notFatal {
		if isInstanceFatalError(err) {
			t.Errorf("must NOT be instance-fatal (would churn workers): %v", err)
		}
	}
}

// The typed error can reach us wrapped (database/sql, fmt.Errorf) or already
// flattened to text by a relay. Both must still classify.
func TestIsInstanceFatalErrorWrappedAndTextFallback(t *testing.T) {
	wrapped := fmt.Errorf("failed to execute update: %w",
		&duckdb.Error{Type: duckdb.ErrorTypeInternal, Msg: "boom"})
	if !isInstanceFatalError(wrapped) {
		t.Error("wrapped typed InternalException must classify as instance-fatal")
	}

	// The exact production signature, flattened: the DuckLake commit fatal.
	commitFatal := errors.New("INTERNAL Error: Calling GetValueInternal on a value that is NULL\n" +
		"Failed to commit DuckLake transaction.")
	if !isInstanceFatalError(commitFatal) {
		t.Error("flattened DuckLake commit fatal must classify as instance-fatal")
	}

	// The downstream wrapper every subsequent statement sees.
	invalidated := errors.New("FATAL Error: Failed to create view '_pyducklake_tmp_append': " +
		"Failed: database has been invalidated because of a previous fatal error. " +
		"The database must be restarted prior to being used again.")
	if !isInstanceFatalError(invalidated) {
		t.Error("invalidated-database wrapper must classify as instance-fatal")
	}
}

// Invalidation is permanent until the process restarts, so the flag must never
// clear — otherwise a poisoned worker could be handed back into rotation by a
// later healthy-looking probe.
func TestInstanceInvalidationIsStickyAndRecordsReason(t *testing.T) {
	pool := &SessionPool{}
	if pool.InstanceInvalidated() {
		t.Fatal("fresh pool must not report invalidated")
	}

	pool.noteInstanceError(errors.New("ordinary failure"))
	if pool.InstanceInvalidated() {
		t.Fatal("a non-fatal error must not invalidate the instance")
	}

	pool.noteInstanceError(&duckdb.Error{Type: duckdb.ErrorTypeInternal, Msg: "GetValueInternal on NULL"})
	if !pool.InstanceInvalidated() {
		t.Fatal("expected instance to be invalidated")
	}
	if got := pool.InstanceInvalidReason(); got != "GetValueInternal on NULL" {
		t.Errorf("expected the originating error as the reason, got %q", got)
	}

	// Nothing clears it, including later benign activity.
	pool.noteInstanceError(nil)
	pool.noteInstanceError(errors.New("ordinary failure"))
	if !pool.InstanceInvalidated() {
		t.Fatal("invalidation must be sticky")
	}
	if got := pool.InstanceInvalidReason(); got != "GetValueInternal on NULL" {
		t.Errorf("reason must keep the ORIGINAL fatal, got %q", got)
	}
}

// The probe is what catches a fatal thrown on a session that has since been
// destroyed — the case that leaves a poisoned pod sitting hot-idle. A probe
// that merely times out or fails for an unrelated reason must NOT retire a
// healthy worker.
func TestProbeInstanceLivenessOnlyFlagsFatalErrors(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	pool := &SessionPool{}
	pool.probeInstanceLiveness(db)
	if pool.InstanceInvalidated() {
		t.Fatal("a healthy instance must survive the probe")
	}

	// A closed handle fails the probe with a driver/connection error, not an
	// engine fatal — it must not be mistaken for invalidation.
	closed, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	_ = closed.Close()
	pool.probeInstanceLiveness(closed)
	if pool.InstanceInvalidated() {
		t.Fatal("a non-fatal probe failure must not invalidate the instance")
	}
}

// The control plane acts on what the health check reports, so the wire fields
// are the contract. Before this, the health check never executed SQL and always
// reported healthy=true, which is exactly why a poisoned instance stayed
// schedulable.
func TestHealthCheckReportsInstanceInvalidated(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
	}
	close(pool.warmupDone)
	handler := &FlightSQLHandler{pool: pool, alloc: memory.DefaultAllocator}

	decode := func(t *testing.T) map[string]interface{} {
		t.Helper()
		stream := &mockDoActionStream{}
		if err := handler.doHealthCheck([]byte(`{}`), stream); err != nil {
			t.Fatalf("health check: %v", err)
		}
		if len(stream.results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(stream.results))
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(stream.results[0].Body, &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return resp
	}

	resp := decode(t)
	if resp["healthy"] != true {
		t.Errorf("healthy worker must report healthy=true, got %v", resp["healthy"])
	}
	if resp["instance_invalidated"] != false {
		t.Errorf("expected instance_invalidated=false, got %v", resp["instance_invalidated"])
	}

	pool.noteInstanceError(&duckdb.Error{Type: duckdb.ErrorTypeInternal, Msg: "GetValueInternal on NULL"})

	resp = decode(t)
	if resp["instance_invalidated"] != true {
		t.Errorf("expected instance_invalidated=true, got %v", resp["instance_invalidated"])
	}
	// healthy=false as well, so a CP that predates the flag still refuses to
	// reuse the worker through the generic gate.
	if resp["healthy"] != false {
		t.Errorf("an invalidated instance must report healthy=false, got %v", resp["healthy"])
	}
	if resp["instance_invalid_reason"] != "GetValueInternal on NULL" {
		t.Errorf("expected the reason on the wire, got %v", resp["instance_invalid_reason"])
	}
}
