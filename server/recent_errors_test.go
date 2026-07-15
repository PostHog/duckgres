package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRecentErrorRingNewestFirstAndEviction(t *testing.T) {
	r := newRecentErrorRing(3)
	base := time.Unix(1700000000, 0)
	for i := 0; i < 5; i++ { // 5 adds into a cap-3 ring: 0,1 evicted
		r.add(RecentError{SQLState: "S", Message: "m", Time: base.Add(time.Duration(i) * time.Second), OrgID: string(rune('a' + i))})
	}
	got := r.snapshot(0) // all retained
	if len(got) != 3 {
		t.Fatalf("snapshot len = %d, want 3 (ring cap)", len(got))
	}
	// Newest first: last added (i=4, org 'e') is [0]; oldest retained (i=2, 'c') is [2].
	if got[0].OrgID != "e" || got[1].OrgID != "d" || got[2].OrgID != "c" {
		t.Fatalf("order = %s,%s,%s, want e,d,c (newest first, 0/1 evicted)", got[0].OrgID, got[1].OrgID, got[2].OrgID)
	}
	// limit clamps.
	if l := r.snapshot(2); len(l) != 2 || l[0].OrgID != "e" {
		t.Fatalf("snapshot(2) = %v, want 2 newest starting 'e'", l)
	}
}

func TestRecentErrorRingNilSafe(t *testing.T) {
	var r *recentErrorRing
	r.add(RecentError{}) // must not panic
	if got := r.snapshot(10); got != nil {
		t.Fatalf("nil ring snapshot = %v, want nil", got)
	}
	var s *Server
	s.recordRecentError(RecentError{}) // nil server must not panic
	if got := s.RecentErrors(10); got != nil {
		t.Fatalf("nil server RecentErrors = %v, want nil", got)
	}
}

// The load-bearing invariant: a failed CREATE SECRET must NOT leak its credential
// into the Errors-page ring — neither via the stored query nor the error message
// (engine errors echo the offending SQL). logQueryError redacts both at capture.
func TestLogQueryErrorCapturesRedactedSecret(t *testing.T) {
	installFakeQueryTracker(t)
	s := &Server{recentErrors: newRecentErrorRing(0)}
	c := &clientConn{server: s, orgID: "acme", username: "root", pid: 1000, workerID: 42, ctx: context.Background()}

	const cred = "topsecretAKIA"
	query := "CREATE SECRET s (TYPE S3, KEY_ID 'AKIAEXAMPLE', SECRET '" + cred + "')"
	// DuckDB echoes the offending SQL (incl. the literal) in its error text.
	engineErr := errors.New("Parser Error: syntax error at or near \"" + cred + "\"\nLINE 1: " + query)

	c.logQueryError(query, engineErr)

	got := s.RecentErrors(10)
	if len(got) != 1 {
		t.Fatalf("ring len = %d, want 1", len(got))
	}
	e := got[0]
	if strings.Contains(e.Query, cred) {
		t.Errorf("stored Query leaks credential: %q", e.Query)
	}
	if strings.Contains(e.Message, cred) {
		t.Errorf("stored Message leaks credential: %q", e.Message)
	}
	if e.OrgID != "acme" || e.Username != "root" || e.WorkerID != 42 || e.PID != 1000 {
		t.Errorf("metadata not captured: %+v", e)
	}
	// A syntax error is user-attributable (42601); the point of this test is the
	// redaction above — just assert classification was captured.
	if e.Category == "" || e.SQLState == "" {
		t.Errorf("classification missing: category=%q sqlstate=%q", e.Category, e.SQLState)
	}
}

// A non-secret query is stored verbatim (redaction is a no-op) so triage keeps
// full fidelity for ordinary errors.
func TestLogQueryErrorCapturesPlainQuery(t *testing.T) {
	installFakeQueryTracker(t)
	s := &Server{recentErrors: newRecentErrorRing(0)}
	c := &clientConn{server: s, orgID: "acme", username: "root", ctx: context.Background()}

	c.logQueryError("SELECT * FROM nope", errors.New("Catalog Error: Table with name nope does not exist!"))

	got := s.RecentErrors(10)
	if len(got) != 1 || got[0].Query != "SELECT * FROM nope" {
		t.Fatalf("plain query not captured verbatim: %+v", got)
	}
	if got[0].Category != "user" {
		t.Errorf("category = %q, want user (Catalog Error)", got[0].Category)
	}
}

func TestLogQueryErrorBoundsRetainedText(t *testing.T) {
	installFakeQueryTracker(t)
	s := &Server{recentErrors: newRecentErrorRing(0)}
	c := &clientConn{server: s, orgID: "acme", username: "root", ctx: context.Background()}

	query := "SELECT " + strings.Repeat("q", maxQueryLength) + " query-tail-marker"
	message := "engine error: " + strings.Repeat("e", maxQueryLength) + " error-tail-marker"
	c.logQueryError(query, errors.New(message))

	got := s.RecentErrors(10)
	if len(got) != 1 {
		t.Fatalf("ring len = %d, want 1", len(got))
	}
	if len(got[0].Query) > maxQueryLength || strings.Contains(got[0].Query, "tail-marker") {
		t.Errorf("retained query was not bounded: length=%d", len(got[0].Query))
	}
	if len(got[0].Message) > maxQueryLength || strings.Contains(got[0].Message, "tail-marker") {
		t.Errorf("retained error was not bounded: length=%d", len(got[0].Message))
	}
}
