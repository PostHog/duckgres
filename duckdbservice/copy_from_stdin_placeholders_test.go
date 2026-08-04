package duckdbservice

import (
	"testing"

	"github.com/posthog/duckgres/server/flightclient"
)

func TestSubstituteCopyFromStdinPlaceholdersIncludesSpoolSize(t *testing.T) {
	template := "SELECT * FROM read_postgres_binary('" +
		flightclient.CopyFromStdinPathPlaceholder +
		"', buffer_size = " + flightclient.CopyFromStdinSizePlaceholder + ")"

	got := substituteCopyFromStdinPlaceholders(template, "/tmp/copy.bin", 210500021)
	want := "SELECT * FROM read_postgres_binary('/tmp/copy.bin', buffer_size = 210500021)"
	if got != want {
		t.Fatalf("substituteCopyFromStdinPlaceholders() = %q, want %q", got, want)
	}
}
