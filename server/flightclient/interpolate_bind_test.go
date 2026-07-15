package flightclient

import (
	"errors"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/sqlcore"
)

// literalSource models the compact portal-side append-only contract used by
// Flight interpolation. The values are already SQL literals so this test can
// isolate placeholder scanning from type decoding.
type literalSource struct {
	literals []string
	errAt    int
}

func (s literalSource) BindParameterCount() int { return len(s.literals) }

func (s literalSource) AppendBindParameterLiteral(dst *strings.Builder, index int) error {
	if index == s.errAt {
		return errors.New("malformed binary value")
	}
	dst.WriteString(s.literals[index])
	return nil
}

var _ sqlcore.SQLLiteralAppender = literalSource{}

func TestInterpolateBoundArgsStreamsLiteralsIntoFinalSQL(t *testing.T) {
	source := literalSource{literals: []string{"'o''brien'", "NULL", "42"}, errAt: -1}
	got, err := interpolateBoundArgs("SELECT ?, $3, $1, '?' /* $2 */", source)
	if err != nil {
		t.Fatalf("interpolateBoundArgs() error = %v", err)
	}
	const want = "SELECT 'o''brien', 42, 'o''brien', '?' /* $2 */"
	if got != want {
		t.Fatalf("interpolateBoundArgs() = %q, want %q", got, want)
	}
}

func TestInterpolateBoundArgsPropagatesLiteralDecodeError(t *testing.T) {
	_, err := interpolateBoundArgs("SELECT ?", literalSource{literals: []string{"ignored"}, errAt: 0})
	if err == nil || !strings.Contains(err.Error(), "malformed binary value") {
		t.Fatalf("interpolateBoundArgs() error = %v, want literal decode error", err)
	}
}

func BenchmarkInterpolateBoundArgs27000Params(b *testing.B) {
	const paramCount = 27_000
	literals := make([]string, paramCount)
	placeholders := make([]string, paramCount)
	for i := range literals {
		literals[i] = "'value'"
		placeholders[i] = "?"
	}
	query := "INSERT INTO t VALUES (" + strings.Join(placeholders, ",") + ")"
	source := literalSource{literals: literals, errAt: -1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := interpolateBoundArgs(query, source); err != nil {
			b.Fatal(err)
		}
	}
}
