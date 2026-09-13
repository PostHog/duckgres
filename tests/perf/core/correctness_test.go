package core

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type resultDriver struct {
	protocol    Protocol
	results     map[string][][]*string
	events      *[]string
	readErr     error
	queryErrors map[string]error
}

func (d *resultDriver) Protocol() Protocol { return d.protocol }
func (d *resultDriver) Close() error       { return nil }
func (d *resultDriver) Execute(_ context.Context, q Query, _ []any) (ExecutionResult, error) {
	*d.events = append(*d.events, "time:"+q.QueryID)
	return ExecutionResult{Rows: 1}, nil
}
func (d *resultDriver) ReadResults(_ context.Context, q Query, _ []any) ([][]*string, error) {
	*d.events = append(*d.events, "read:"+q.QueryID)
	if err := d.queryErrors[q.QueryID]; err != nil {
		return nil, err
	}
	return d.results[q.QueryID], d.readErr
}
func cell(s string) *string { return &s }
func TestCorrectnessGateBeforeAnyTiming(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    *string
		err      error
		wantFail bool
	}{
		{"equal", cell("Chrome"), nil, false}, {"different key", cell("chrome"), nil, true}, {"SQL null", nil, nil, true}, {"read failure", nil, errors.New("reader failed"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []string
			queries := []Query{{QueryID: "json", IntentID: "browser", Representation: "json"}, {QueryID: "struct", IntentID: "browser", Representation: "struct"}}
			d := &resultDriver{protocol: ProtocolPGWire, events: &events, results: map[string][][]*string{"json": {{cell("Chrome"), cell("10")}}, "struct": {{tc.value, cell("10")}}}, readErr: tc.err}
			r := NewQueryRunner(RunnerConfig{Catalog: Catalog{Targets: []Protocol{ProtocolPGWire}, Queries: queries, WarmupIterations: 1, MeasureIterations: 1}, Drivers: map[Protocol]ProtocolDriver{ProtocolPGWire: d}})
			_, err := r.Run(context.Background())
			if (err != nil) != tc.wantFail {
				t.Fatalf("error=%v", err)
			}
			if tc.wantFail {
				for _, e := range events {
					if strings.HasPrefix(e, "time:") {
						t.Fatalf("timing after failed gate: %v", events)
					}
				}
			} else if len(events) != 6 || events[0] != "read:json" || events[1] != "read:struct" {
				t.Fatalf("events %v", events)
			}
		})
	}
}
func TestCorrectnessGateComparesCountsAndEngines(t *testing.T) {
	var events []string
	q := Query{QueryID: "json", IntentID: "browser", Representation: "json"}
	a := &resultDriver{protocol: ProtocolPGWire, events: &events, results: map[string][][]*string{"json": {{cell("null"), cell("2")}}}}
	b := &resultDriver{protocol: ProtocolTrino, events: &events, results: map[string][][]*string{"json": {{cell("null"), cell("3")}}}}
	_, err := NewQueryRunner(RunnerConfig{Catalog: Catalog{Targets: []Protocol{ProtocolPGWire, ProtocolTrino}, Queries: []Query{q}, MeasureIterations: 1}, Drivers: map[Protocol]ProtocolDriver{ProtocolPGWire: a, ProtocolTrino: b}}).Run(context.Background())
	if err == nil {
		t.Fatal("same row count with different aggregate counts passed")
	}
	for _, e := range events {
		if strings.HasPrefix(e, "time:") {
			t.Fatal(events)
		}
	}
}
func TestCorrectnessRequiresJSONBaseline(t *testing.T) {
	var events []string
	d := &resultDriver{events: &events}
	_, err := NewQueryRunner(RunnerConfig{Catalog: Catalog{Targets: []Protocol{ProtocolPGWire}, Queries: []Query{{QueryID: "struct", IntentID: "browser", Representation: "struct"}}}, Drivers: map[Protocol]ProtocolDriver{ProtocolPGWire: d}}).Run(context.Background())
	if err == nil {
		t.Fatal("missing JSON baseline accepted")
	}
}
func TestRepresentationDialectAndRouting(t *testing.T) {
	q := Query{Representation: "json", PGWireSQL: "SELECT json_extract_string(properties, '$.\"$browser\"')", Targets: []Protocol{ProtocolPGWire}}
	sql, err := q.SQLFor(ProtocolTrino)
	if err != nil || !strings.Contains(sql, "json_extract_scalar") {
		t.Fatalf("sql=%q error=%v", sql, err)
	}
	if querySupportsProtocol(q, ProtocolTrino) || !querySupportsProtocol(q, ProtocolPGWire) {
		t.Fatal("query targets ignored")
	}
}

func TestValidationOnlyJSONNeverRunsTimed(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolPGWire, ProtocolTrino, ProtocolTrinoCached} {
		t.Run(string(protocol), func(t *testing.T) {
			var events []string
			queries := []Query{{QueryID: "json", IntentID: "browser", Representation: "json", ValidationOnly: true}, {QueryID: "variant", IntentID: "browser", Representation: "variant"}}
			values := [][]*string{{cell("Chrome"), cell("10")}}
			d := &resultDriver{protocol: protocol, events: &events, results: map[string][][]*string{"json": values, "variant": values}}
			summary, err := NewQueryRunner(RunnerConfig{Catalog: Catalog{Targets: []Protocol{protocol}, Queries: queries, WarmupIterations: 1, MeasureIterations: 2}, Drivers: map[Protocol]ProtocolDriver{protocol: d}}).Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"read:json", "read:variant", "time:variant", "time:variant", "time:variant"}
			if strings.Join(events, ",") != strings.Join(want, ",") {
				t.Fatalf("events=%v", events)
			}
			if summary.TotalQueries != 2 || summary.WarmupQueries != 1 {
				t.Fatalf("summary includes validation: %+v", summary)
			}
		})
	}
}

func TestUnsupportedTrinoVariantFailsBeforeTiming(t *testing.T) {
	var events []string
	d := &resultDriver{protocol: ProtocolTrino, events: &events, results: map[string][][]*string{"json": {{cell("Chrome"), cell("1")}}}, queryErrors: map[string]error{"variant": errors.New("unsupported variant reader")}}
	queries := []Query{{QueryID: "json", IntentID: "browser", Representation: "json", ValidationOnly: true}, {QueryID: "variant", IntentID: "browser", Representation: "variant"}}
	summary, err := NewQueryRunner(RunnerConfig{Catalog: Catalog{Targets: []Protocol{ProtocolTrino}, Queries: queries, WarmupIterations: 1, MeasureIterations: 1}, Drivers: map[Protocol]ProtocolDriver{ProtocolTrino: d}}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported variant reader") {
		t.Fatalf("unsupported reader must fail: %v", err)
	}
	if strings.Join(events, ",") != "read:json,read:variant" || summary.TotalQueries != 0 || summary.WarmupQueries != 0 {
		t.Fatalf("unsupported reader fell back or timed: %v %+v", events, summary)
	}
}
