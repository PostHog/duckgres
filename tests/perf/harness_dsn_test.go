package perf

import "testing"

func TestPGWireDSNForHarnessAddsKeywordPasswordWhenMissing(t *testing.T) {
	got, err := pgwireDSNForHarness("host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require", "secret")
	if err != nil {
		t.Fatalf("pgwireDSNForHarness returned error: %v", err)
	}
	want := "host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require password='secret'"
	if got != want {
		t.Fatalf("dsn = %q, want %q", got, want)
	}
}

func TestPGWireDSNForHarnessPreservesExistingKeywordPassword(t *testing.T) {
	original := "host=127.0.0.1 port=5432 user=perfuser password='from-dsn' dbname=test sslmode=require"
	got, err := pgwireDSNForHarness(original, "secret")
	if err != nil {
		t.Fatalf("pgwireDSNForHarness returned error: %v", err)
	}
	if got != original {
		t.Fatalf("dsn = %q, want original %q", got, original)
	}
}

func TestPGWireDSNForHarnessAddsURLPasswordWhenMissing(t *testing.T) {
	got, err := pgwireDSNForHarness("postgres://perfuser@127.0.0.1:5432/test?sslmode=require", "secret")
	if err != nil {
		t.Fatalf("pgwireDSNForHarness returned error: %v", err)
	}
	want := "postgres://perfuser:secret@127.0.0.1:5432/test?sslmode=require"
	if got != want {
		t.Fatalf("dsn = %q, want %q", got, want)
	}
}

func TestPGWireDSNForHarnessPreservesExistingURLPassword(t *testing.T) {
	original := "postgres://perfuser:from-dsn@127.0.0.1:5432/test?sslmode=require"
	got, err := pgwireDSNForHarness(original, "secret")
	if err != nil {
		t.Fatalf("pgwireDSNForHarness returned error: %v", err)
	}
	if got != original {
		t.Fatalf("dsn = %q, want original %q", got, original)
	}
}

func TestPGWireDSNForHarnessLeavesDSNUnchangedWithoutPassword(t *testing.T) {
	original := "host=127.0.0.1 port=5432 user=perfuser dbname=test sslmode=require"
	got, err := pgwireDSNForHarness(original, "")
	if err != nil {
		t.Fatalf("pgwireDSNForHarness returned error: %v", err)
	}
	if got != original {
		t.Fatalf("dsn = %q, want original %q", got, original)
	}
}
