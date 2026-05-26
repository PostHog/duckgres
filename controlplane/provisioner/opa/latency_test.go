package opa

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/open-policy-agent/opa/rego"
)

// LatencyBudget is the per-decision budget for the canonical AccessCatalog
// evaluation against a 1000-org bundle. Anything above this is the symptom
// of a non-O(1) policy (linear scan over user_catalogs, etc.) and fails
// the build.
//
// The plan calls out 30-40 OPA decisions per Trino query, with low-tens of
// thousands of orgs. 1ms per decision keeps the OPA portion of query
// authorization under ~40ms, which is acceptable next to query planning.
// If this number changes, update the plan reference too.
const LatencyBudget = 1 * time.Millisecond

// largeFixture builds a UserCatalogs with the requested number of orgs.
// Each user owns exactly one catalog named org_<i>_iceberg.
func largeFixture(orgs int) UserCatalogs {
	uc := make(UserCatalogs, orgs+1)
	for i := 0; i < orgs; i++ {
		user := fmt.Sprintf("%d", i)
		uc[user] = map[string]bool{fmt.Sprintf("org_%d_iceberg", i): true}
	}
	// Admin owns everything (matches Stream A's planned bundle).
	admin := make(map[string]bool, orgs)
	for i := 0; i < orgs; i++ {
		admin[fmt.Sprintf("org_%d_iceberg", i)] = true
	}
	uc[AdminPrincipal] = admin
	return uc
}

// preparedLargeBundle compiles the policy against a 1000-org dataset. We
// pre-prepare once and reuse across iterations; the bundle is what
// production looks like (OPA loads the bundle once, then runs many
// decisions against it).
func preparedLargeBundle(b interface{ Fatalf(string, ...interface{}) }, orgs int) rego.PreparedEvalQuery {
	ctx := context.Background()
	data, err := buildDataDocument(largeFixture(orgs))
	if err != nil {
		b.Fatalf("buildDataDocument: %v", err)
	}
	q, err := rego.New(
		rego.Query("data.trino.allow"),
		rego.Module("policy.rego", string(policyRego)),
		rego.Data(data),
	).PrepareForEval(ctx)
	if err != nil {
		b.Fatalf("PrepareForEval: %v", err)
	}
	return q
}

// BenchmarkAccessCatalog1000Orgs is the canonical decision benchmark for
// CI. Run with `go test -bench=BenchmarkAccessCatalog1000Orgs`. Used by
// TestAccessCatalogLatencyBudget below as the CI gate.
func BenchmarkAccessCatalog1000Orgs(b *testing.B) {
	q := preparedLargeBundle(b, 1000)
	input := buildInput("777", "AccessCatalog", catalogResource("org_777_iceberg"))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := q.Eval(ctx, rego.EvalInput(input))
		if err != nil {
			b.Fatalf("Eval: %v", err)
		}
		if len(rs) == 0 || rs[0].Expressions[0].Value != true {
			b.Fatal("expected allow=true")
		}
	}
}

// TestAccessCatalogLatencyBudget is the CI gate. It runs a representative
// decision against a 1000-org bundle, measures the wall-clock time across
// 200 iterations, and fails if the p50 exceeds LatencyBudget. We deliber-
// ately measure p50 (not mean), because mean is skewed by occasional GC
// pauses and Go scheduler latency in test environments.
//
// This is the test the plan calls out as the "reject naive Rego"
// enforcement. If you're editing policy.rego, run this test and confirm
// the new policy stays under budget BEFORE pushing.
func TestAccessCatalogLatencyBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency benchmark in -short mode")
	}

	q := preparedLargeBundle(t, 1000)
	input := buildInput("777", "AccessCatalog", catalogResource("org_777_iceberg"))
	ctx := context.Background()

	// Warm up. The first eval allocates topdown state; we want to
	// measure steady-state latency, not warmup.
	for i := 0; i < 50; i++ {
		if _, err := q.Eval(ctx, rego.EvalInput(input)); err != nil {
			t.Fatalf("warmup eval: %v", err)
		}
	}

	const iters = 200
	durations := make([]time.Duration, iters)
	for i := 0; i < iters; i++ {
		start := time.Now()
		rs, err := q.Eval(ctx, rego.EvalInput(input))
		durations[i] = time.Since(start)
		if err != nil {
			t.Fatalf("eval %d: %v", i, err)
		}
		if len(rs) == 0 || rs[0].Expressions[0].Value != true {
			t.Fatalf("eval %d: want allow=true, got %v", i, rs)
		}
	}

	p50 := percentile(durations, 50)
	p99 := percentile(durations, 99)
	t.Logf("AccessCatalog @1000 orgs: p50=%s p99=%s budget=%s", p50, p99, LatencyBudget)
	if p50 > LatencyBudget {
		t.Fatalf("AccessCatalog @1000 orgs p50=%s exceeds budget %s -- policy is likely doing a linear scan over user_catalogs", p50, LatencyBudget)
	}
	// p99 budget is more lenient because of GC; allow 5x.
	if p99 > 5*LatencyBudget {
		t.Fatalf("AccessCatalog @1000 orgs p99=%s exceeds 5x budget (%s)", p99, 5*LatencyBudget)
	}
}

// TestNegativeDecisionLatency: verify that the deny path is also fast.
// A linear-scan policy would be slow on deny too (it scans all orgs for a
// match and finds none). The deny path is hot in practice -- users
// running SHOW CATALOGS see one FilterCatalogs decision per non-owned
// catalog, and a customer cluster with N orgs has N-1 of these per call.
func TestNegativeDecisionLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency benchmark in -short mode")
	}
	q := preparedLargeBundle(t, 1000)
	// user 777 trying to access catalog 42 -- should deny.
	input := buildInput("777", "AccessCatalog", catalogResource("org_42_iceberg"))
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if _, err := q.Eval(ctx, rego.EvalInput(input)); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	const iters = 200
	durations := make([]time.Duration, iters)
	for i := 0; i < iters; i++ {
		start := time.Now()
		rs, err := q.Eval(ctx, rego.EvalInput(input))
		durations[i] = time.Since(start)
		if err != nil {
			t.Fatalf("eval %d: %v", i, err)
		}
		// Deny: either no result or allow=false.
		if len(rs) > 0 && len(rs[0].Expressions) > 0 {
			if v, _ := rs[0].Expressions[0].Value.(bool); v {
				t.Fatalf("eval %d: expected deny, got allow", i)
			}
		}
	}
	p50 := percentile(durations, 50)
	t.Logf("AccessCatalog (deny) @1000 orgs: p50=%s", p50)
	if p50 > LatencyBudget {
		t.Fatalf("deny p50=%s exceeds budget %s", p50, LatencyBudget)
	}
}

// percentile returns the p-th percentile of d (0 < p < 100). Uses
// nearest-rank for simplicity; not exact but consistent across runs.
func percentile(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	// Copy + sort.
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	insertionSort(sorted)
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// insertionSort is overkill for 200 elements, but avoids pulling in sort
// for a single call.
func insertionSort(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j-1] > d[j]; j-- {
			d[j-1], d[j] = d[j], d[j-1]
		}
	}
}
