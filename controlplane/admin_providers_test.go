//go:build kubernetes

package controlplane

import (
	"reflect"
	"testing"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestAggregateWorkerFleetPreservesExistingOrgAgnosticContract(t *testing.T) {
	stats := []configstore.WorkerLifecycleStats{
		{Image: "duckgres:v1", State: configstore.WorkerStateHot, Binding: "org_bound", Org: "org-a", Count: 2, CPUCores: 4, MemoryBytes: 8},
		{Image: "duckgres:v1", State: configstore.WorkerStateHot, Binding: "org_bound", Org: "org-b", Count: 3, CPUCores: 6, MemoryBytes: 12},
		{Image: "duckgres:v2", State: configstore.WorkerStateHotIdle, Binding: "org_bound", Org: "org-a", Count: 1, CPUCores: 2, MemoryBytes: 4},
	}

	want := []admin.FleetStat{
		{Image: "duckgres:v1", State: "hot", Binding: "org_bound", Count: 5, CPUCores: 10, MemoryBytes: 20},
		{Image: "duckgres:v2", State: "hot_idle", Binding: "org_bound", Count: 1, CPUCores: 2, MemoryBytes: 4},
	}
	if got := aggregateWorkerFleet(stats); !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregateWorkerFleet() = %#v, want %#v", got, want)
	}
}
