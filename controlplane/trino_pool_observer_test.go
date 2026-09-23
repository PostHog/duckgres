//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/trinopool"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

type fakePoolInstanceLister struct {
	instances []configstore.TrinoPoolInstance
	err       error
	poolIDs   []string
}

func (f *fakePoolInstanceLister) ListTrinoPoolInstances(_ context.Context, poolID string) ([]configstore.TrinoPoolInstance, error) {
	f.poolIDs = append(f.poolIDs, poolID)
	return f.instances, f.err
}

type fakePoolMember struct {
	queries []admin.TrinoQuery
	err     error
	nodes   admin.TrinoNodeInventory
	killed  []string
}

func (m *fakePoolMember) Queries(context.Context) ([]admin.TrinoQuery, error) {
	return m.queries, m.err
}
func (m *fakePoolMember) Query(_ context.Context, id string) (*admin.TrinoQuery, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, q := range m.queries {
		if q.QueryID == id {
			q := q
			return &q, nil
		}
	}
	return nil, errors.New("not found")
}
func (m *fakePoolMember) KillQuery(_ context.Context, id, _ string) error {
	m.killed = append(m.killed, id)
	return nil
}
func (m *fakePoolMember) Nodes(context.Context) (admin.TrinoNodeInventory, error) {
	return m.nodes, m.err
}
func (m *fakePoolMember) ServerInfo(context.Context) (*admin.TrinoServerInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &admin.TrinoServerInfo{Version: "471", Coordinator: true}, nil
}

func poolObserverFixture(instances []configstore.TrinoPoolInstance, members map[string]*fakePoolMember) (*trinoPoolObserver, *fakePoolInstanceLister, map[string]string) {
	lister := &fakePoolInstanceLister{instances: instances}
	endpoints := map[string]string{}
	o := newTrinoPoolObserver("registered:cell-001", "trino-cells", 8080, lister, func() (string, string) { return "u", "p" })
	o.newClient = func(baseURL string, _ admin.TrinoCredentialSource) admin.TrinoCoordinatorClient {
		for id, member := range members {
			if baseURL == "http://"+id+".trino-cells.svc.cluster.local:8080" {
				endpoints[id] = baseURL
				return member
			}
		}
		return &fakePoolMember{err: errors.New("unexpected endpoint " + baseURL)}
	}
	return o, lister, endpoints
}

func instance(id string, phase trinopool.Phase) configstore.TrinoPoolInstance {
	return configstore.TrinoPoolInstance{InstanceID: id, Phase: string(phase)}
}

// The pool observer queries every live instance's own Service and unions the
// result, so a pooled org's queries are visible to the console and metered.
func TestTrinoPoolObserverUnionsLiveMembers(t *testing.T) {
	members := map[string]*fakePoolMember{
		"cell-a": {queries: []admin.TrinoQuery{{QueryID: "qa"}}},
		"cell-b": {queries: []admin.TrinoQuery{{QueryID: "qb"}}},
		"cell-c": {queries: []admin.TrinoQuery{{QueryID: "qc"}}},
	}
	o, lister, endpoints := poolObserverFixture([]configstore.TrinoPoolInstance{
		instance("cell-a", trinopool.PhaseServing),
		instance("cell-b", trinopool.PhaseDraining),
		instance("cell-c", trinopool.PhaseCreating), // no coordinator yet: not observed
		instance("cell-d", trinopool.PhaseRetired),  // gone: not observed
	}, members)

	queries, err := o.Queries(context.Background())
	if err != nil {
		t.Fatalf("Queries: %v", err)
	}
	var ids []string
	for _, q := range queries {
		ids = append(ids, q.QueryID)
	}
	sort.Strings(ids)
	if len(ids) != 2 || ids[0] != "qa" || ids[1] != "qb" {
		t.Fatalf("queries = %v, want qa and qb (serving + draining only)", ids)
	}
	if len(endpoints) != 2 {
		t.Fatalf("dialled %v, want only the two observed instances", endpoints)
	}
	if lister.poolIDs[0] != "registered:cell-001" {
		t.Fatalf("listed pool %q, want the stored pool id", lister.poolIDs[0])
	}
}

// One unreachable member must not hide the rest; only a pool where every
// member fails is an error.
func TestTrinoPoolObserverToleratesPartialFailure(t *testing.T) {
	members := map[string]*fakePoolMember{
		"cell-a": {err: errors.New("connection refused")},
		"cell-b": {queries: []admin.TrinoQuery{{QueryID: "qb"}}},
	}
	o, _, _ := poolObserverFixture([]configstore.TrinoPoolInstance{
		instance("cell-a", trinopool.PhaseServing),
		instance("cell-b", trinopool.PhaseServing),
	}, members)
	queries, err := o.Queries(context.Background())
	if err != nil || len(queries) != 1 || queries[0].QueryID != "qb" {
		t.Fatalf("Queries = %v, %v; want qb despite cell-a failing", queries, err)
	}

	members["cell-b"].err = errors.New("connection refused")
	if _, err := o.Queries(context.Background()); err == nil {
		t.Fatal("Queries succeeded with every member failing")
	}
}

// With no running member the console gets a named reason, not a transport
// error against an empty URL (#1216).
func TestTrinoPoolObserverWithoutMembers(t *testing.T) {
	o, _, _ := poolObserverFixture(nil, nil)
	if queries, err := o.Queries(context.Background()); err != nil || len(queries) != 0 {
		t.Fatalf("Queries = %v, %v; want empty and no error", queries, err)
	}
	if _, err := o.ServerInfo(context.Background()); !errors.Is(err, errTrinoPoolNoMembers) {
		t.Fatalf("ServerInfo err = %v, want errTrinoPoolNoMembers", err)
	}
	if _, err := o.Nodes(context.Background()); !errors.Is(err, errTrinoPoolNoMembers) {
		t.Fatalf("Nodes err = %v, want errTrinoPoolNoMembers", err)
	}
}

// A kill goes only to the member that holds the query.
func TestTrinoPoolObserverKillsOnOwningMember(t *testing.T) {
	members := map[string]*fakePoolMember{
		"cell-a": {queries: []admin.TrinoQuery{{QueryID: "qa"}}},
		"cell-b": {queries: []admin.TrinoQuery{{QueryID: "qb"}}},
	}
	o, _, _ := poolObserverFixture([]configstore.TrinoPoolInstance{
		instance("cell-a", trinopool.PhaseServing),
		instance("cell-b", trinopool.PhaseServing),
	}, members)
	if err := o.KillQuery(context.Background(), "qb", "operator kill"); err != nil {
		t.Fatalf("KillQuery: %v", err)
	}
	if len(members["cell-a"].killed) != 0 || len(members["cell-b"].killed) != 1 {
		t.Fatalf("kills a=%v b=%v, want only cell-b", members["cell-a"].killed, members["cell-b"].killed)
	}
	if err := o.KillQuery(context.Background(), "missing", "x"); err == nil {
		t.Fatal("KillQuery of an unknown query succeeded")
	}
}

// A retired instance's client is dropped, so the cache cannot grow without
// bound as the pool replaces members.
func TestTrinoPoolObserverDropsRetiredClients(t *testing.T) {
	members := map[string]*fakePoolMember{"cell-a": {}, "cell-b": {}}
	o, lister, _ := poolObserverFixture([]configstore.TrinoPoolInstance{
		instance("cell-a", trinopool.PhaseServing),
		instance("cell-b", trinopool.PhaseServing),
	}, members)
	if _, err := o.Queries(context.Background()); err != nil {
		t.Fatal(err)
	}
	lister.instances = []configstore.TrinoPoolInstance{instance("cell-b", trinopool.PhaseServing), instance("cell-a", trinopool.PhaseRetired)}
	if _, err := o.Queries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := o.clients["cell-a"]; ok || len(o.clients) != 1 {
		t.Fatalf("clients = %v, want only cell-b", o.clients)
	}
}

type poolObserverWiringStore struct {
	*fleetBootstrapStore
	fakePoolInstanceLister
}

// The wired observer for a pooled cell is the pool observer, for the console
// and for usage metering alike; a fixed cell keeps its coordinator client.
func TestPooledCellWiresThePoolObserver(t *testing.T) {
	t.Setenv(envTrinoFilesystemCacheEnabled, "false")
	store := &poolObserverWiringStore{fleetBootstrapStore: &fleetBootstrapStore{initialized: map[string]bool{}}}
	kc := kubefake.NewClientset()
	ducklings := func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }

	pooled := trinoCell{ID: registeredTrinoCellPrefix + "cell-001", PublicID: "cell-001", Namespace: "trino-cells", Mode: trinoPoolModeShared, PoolCoordinatorPort: 8080}
	wire, err := buildTrinoCellWiring(store, kc, ducklings, pooled)
	if err != nil {
		t.Fatalf("wire pooled cell: %v", err)
	}
	observer, ok := wire.Console.Observer.(*trinoPoolObserver)
	if !ok {
		t.Fatalf("console observer is %T, want *trinoPoolObserver", wire.Console.Observer)
	}
	if observer.poolID != pooled.ID || observer.namespace != "trino-cells" || observer.port != 8080 {
		t.Fatalf("observer = %s/%s:%d, want the pool's stored id, namespace and port", observer.poolID, observer.namespace, observer.port)
	}
	if len(wire.Observers) != 1 || wire.Observers[0] != wire.Console.Observer {
		t.Fatalf("usage observers = %v, want exactly the pool observer", wire.Observers)
	}

	fixed := trinoCell{ID: "cell-legacy", Namespace: "legacy", CoordinatorURL: "https://legacy.example.test"}
	wire, err = buildTrinoCellWiring(store, kc, ducklings, fixed)
	if err != nil {
		t.Fatalf("wire fixed cell: %v", err)
	}
	if _, ok := wire.Console.Observer.(*trinoPoolObserver); ok {
		t.Fatal("a fixed cell was given the pool observer")
	}
}
