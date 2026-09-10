//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/analytics"
)

type trinoUsageFakeCoordinator struct {
	queries []admin.TrinoQuery
	err     error
}

func (f *trinoUsageFakeCoordinator) Queries(context.Context) ([]admin.TrinoQuery, error) {
	return f.queries, f.err
}

func (f *trinoUsageFakeCoordinator) Query(context.Context, string) (*admin.TrinoQuery, error) {
	return nil, nil
}

func (f *trinoUsageFakeCoordinator) KillQuery(context.Context, string, string) error { return nil }

func (f *trinoUsageFakeCoordinator) Nodes(context.Context) (admin.TrinoNodeInventory, error) {
	return admin.TrinoNodeInventory{}, nil
}

func (f *trinoUsageFakeCoordinator) ServerInfo(context.Context) (*admin.TrinoServerInfo, error) {
	return nil, nil
}

type trinoUsageFakeOrgs struct{ orgs []configstore.TrinoEnabledOrg }

func (f trinoUsageFakeOrgs) ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error) {
	return f.orgs, nil
}

func (f trinoUsageFakeOrgs) GetManagedWarehouseTrino(string) (*configstore.ManagedWarehouseTrino, error) {
	return nil, nil
}

type trinoUsageEvent struct {
	event string
	org   string
	props map[string]any
}

type trinoUsageFakeTracker struct{ events []trinoUsageEvent }

func (f *trinoUsageFakeTracker) Capture(event, org string, props map[string]any) {
	f.events = append(f.events, trinoUsageEvent{event: event, org: org, props: props})
}

func (f *trinoUsageFakeTracker) Close() {}

func TestTrinoUsageCollectorCapturesTerminalQueriesOnce(t *testing.T) {
	tracker := &trinoUsageFakeTracker{}
	analytics.SetDefault(tracker)
	t.Cleanup(func() { analytics.SetDefault(nil) })

	coordinator := &trinoUsageFakeCoordinator{queries: []admin.TrinoQuery{
		{QueryID: "finished", State: "FINISHED", Principal: "tenant-db", Query: "SELECT private_value", ElapsedMS: 1200, QueuedMS: 200, CPUMS: 400, PhysicalInputBytes: 12, PeakMemoryBytes: 34, ProcessedInputRows: 56, Source: "sql-editor", ResourceGroup: "global.tenant"},
		{QueryID: "failed", State: "FAILED", Principal: "tenant-db", ElapsedMS: 800, CPUMS: 100, ErrorType: "USER_ERROR", ErrorCode: "SYNTAX_ERROR"},
		{QueryID: "running", State: "RUNNING", Principal: "tenant-db"},
		{QueryID: "operator", State: "FINISHED", Principal: "__observer"},
	}}
	collector := newTrinoUsageCollector(coordinator, trinoUsageFakeOrgs{orgs: []configstore.TrinoEnabledOrg{{OrgID: "org-a", DatabaseName: "tenant-db"}}}, func(org, user string) int64 {
		if org != "org-a" || user != "tenant-db" {
			t.Fatalf("team lookup = (%q, %q)", org, user)
		}
		return 42
	})

	collector.collect(context.Background())
	collector.collect(context.Background())

	if len(tracker.events) != 2 {
		t.Fatalf("event count = %d, want 2", len(tracker.events))
	}
	completed, failed := tracker.events[0], tracker.events[1]
	if completed.event != "query_completed" || completed.org != "org-a" {
		t.Errorf("completed = %#v", completed)
	}
	if got := completed.props["execution_engine"]; got != "trino" {
		t.Errorf("execution_engine = %v, want trino", got)
	}
	if got := completed.props["cpu_seconds"]; got != 0.4 {
		t.Errorf("cpu_seconds = %v, want 0.4", got)
	}
	if got := completed.props["team_id"]; got != int64(42) {
		t.Errorf("team_id = %v, want 42", got)
	}
	if _, ok := completed.props["query"]; ok {
		t.Error("query text must not be captured")
	}
	if failed.event != "query_failed" || failed.props["error_code"] != "SYNTAX_ERROR" {
		t.Errorf("failed = %#v", failed)
	}
}
