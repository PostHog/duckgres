package trino

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
	trinoclient "github.com/trinodb/trino-go-client/trino"
)

type fakeResultExecutor struct {
	fakeExecutor
	results [][]any
	reads   int
	readErr error
}

func (f *fakeResultExecutor) ReadResult(context.Context, string, []any) (resultDigest, error) {
	f.reads++
	if f.readErr != nil {
		return resultDigest{}, f.readErr
	}
	result := resultDigest{columns: 2, rows: map[[32]byte]int64{}}
	for _, row := range f.results {
		if err := result.add(row); err != nil {
			return resultDigest{}, err
		}
	}
	return result, nil
}

func TestHoglakeValidationBeforeTimingAndCachedBySQLAndArgs(t *testing.T) {
	target := &fakeResultExecutor{results: [][]any{{int64(1), "a"}, {int64(2), "b"}}}
	reference := &fakeResultExecutor{results: [][]any{{int64(2), "b"}, {int64(1), "a"}}}
	driver := &Driver{exec: target, reference: reference}
	query := core.Query{QueryID: "q", PGWireSQL: "SELECT x, y FROM posthog.events WHERE x > ?"}
	for _, args := range [][]any{{1}, {1}, {2}} {
		if _, err := driver.Execute(context.Background(), query, args); err != nil {
			t.Fatal(err)
		}
	}
	if target.reads != 2 || reference.reads != 2 || len(target.queries) != 3 {
		t.Fatalf("reads=%d/%d executions=%d", target.reads, reference.reads, len(target.queries))
	}
}

func TestHoglakeMismatchBlocksTimingAndIsNotCached(t *testing.T) {
	target := &fakeResultExecutor{results: [][]any{{"private-value", int64(1)}, {"private-value", int64(1)}}}
	reference := &fakeResultExecutor{results: [][]any{{"private-value", int64(1)}}}
	driver := &Driver{exec: target, reference: reference}
	query := core.Query{QueryID: "q", PGWireSQL: "SELECT x, y FROM posthog.events"}
	for range 2 {
		_, err := driver.Execute(context.Background(), query, nil)
		if err == nil || !strings.Contains(err.Error(), "result mismatch") || strings.Contains(err.Error(), "private-value") {
			t.Fatalf("error=%v", err)
		}
	}
	if len(target.queries) != 0 || target.reads != 2 {
		t.Fatalf("executions=%d reads=%d", len(target.queries), target.reads)
	}
}

func TestHoglakeValidationFailureRedactsUnderlyingValues(t *testing.T) {
	target := &fakeResultExecutor{readErr: errors.New("secret query result")}
	driver := &Driver{exec: target, reference: &fakeResultExecutor{}}
	_, err := driver.Execute(context.Background(), core.Query{QueryID: "q", PGWireSQL: "SELECT 1"}, nil)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error=%v", err)
	}
}

func TestResultDigestNormalizesTimeAndPreservesNullAndMultiplicity(t *testing.T) {
	a := resultDigest{rows: map[[32]byte]int64{}}
	b := resultDigest{rows: map[[32]byte]int64{}}
	instant := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := a.add([]any{instant, nil, float64(0)}); err != nil {
		t.Fatal(err)
	}
	if err := b.add([]any{instant.In(time.FixedZone("offset", 3600)), nil, float64(0)}); err != nil {
		t.Fatal(err)
	}
	if !a.equal(b) {
		t.Fatal("equivalent times differ")
	}
	if err := b.add([]any{instant, nil, float64(0)}); err != nil {
		t.Fatal(err)
	}
	if a.equal(b) {
		t.Fatal("duplicate rows ignored")
	}
}

func TestHoglakeRequiresDistinctReferenceCatalog(t *testing.T) {
	for _, ref := range []string{"", "hoglake"} {
		_, err := New(context.Background(), ConnectionConfig{Protocol: core.ProtocolTrinoHoglake, Catalog: "hoglake", ReferenceCatalog: ref})
		if err == nil || !strings.Contains(err.Error(), "reference catalog") {
			t.Fatalf("reference=%q error=%v", ref, err)
		}
	}
}

func TestValidationRetainsSafeTrinoErrorCode(t *testing.T) {
	target := &fakeResultExecutor{readErr: &trinoclient.ErrQueryFailed{StatusCode: 200, Reason: &trinoclient.ErrTrino{Message: "secret result", ErrorName: "NOT_SUPPORTED", ErrorType: "USER_ERROR", ErrorCode: 13}}}
	driver := &Driver{exec: target, reference: &fakeResultExecutor{}}
	_, err := driver.Execute(context.Background(), core.Query{QueryID: "q", PGWireSQL: "SELECT 1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "NOT_SUPPORTED") || !strings.Contains(err.Error(), "code=13") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error=%v", err)
	}
}

func TestCatalogIdentityRequiresHoglakeAndDucklake(t *testing.T) {
	for _, referenceConnector := range []string{"hoglake", "ducklake"} {
		lookup := func(_ context.Context, catalog string) (string, error) {
			if catalog == "target" {
				return "hoglake", nil
			}
			return referenceConnector, nil
		}
		err := validateCatalogs(context.Background(), lookup, "target", "reference")
		if (err == nil) != (referenceConnector == "ducklake") {
			t.Fatalf("connector=%s error=%v", referenceConnector, err)
		}
	}
}
