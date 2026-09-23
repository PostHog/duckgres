//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// errTrinoPoolNoMembers means the pool has no instance with a running
// coordinator to observe. The console reports it as unavailable with this
// reason instead of a transport error against an empty URL.
var errTrinoPoolNoMembers = errors.New("trino pool has no running member to observe")

// trinoPoolInstanceLister is the one config-store read the observer needs.
type trinoPoolInstanceLister interface {
	ListTrinoPoolInstances(ctx context.Context, poolID string) ([]configstore.TrinoPoolInstance, error)
}

// trinoPoolObservedPhases are the phases whose coordinator is up and can hold
// queries. DRAINING and SEALED stay observed so a query that started before a
// drain is still seen to finish (usage metering), and SUSPECT stays observed
// because a failing probe is not proof that the process is gone. Nothing
// before ADMITTED has served a tenant.
var trinoPoolObservedPhases = map[string]bool{
	string(trinopool.PhaseAdmitted): true,
	string(trinopool.PhaseServing):  true,
	string(trinopool.PhaseDraining): true,
	string(trinopool.PhaseSealed):   true,
	string(trinopool.PhaseSuspect):  true,
}

// trinoPoolObserver is the console and usage observer for a shared pool.
//
// A pool has no fixed coordinator: its instances are replaced over time. So
// instead of one client built at startup from a URL the pool does not have,
// the observer reads the pool's current instances from the config store on
// every call and asks each instance's own Service. Any control-plane replica
// can do this; it does not need the pool operator's lease.
type trinoPoolObserver struct {
	poolID    string
	namespace string
	port      int32
	instances trinoPoolInstanceLister
	creds     admin.TrinoCredentialSource
	newClient func(baseURL string, creds admin.TrinoCredentialSource) admin.TrinoCoordinatorClient

	mu      sync.Mutex
	clients map[string]admin.TrinoCoordinatorClient
}

func newTrinoPoolObserver(poolID, namespace string, port int32, instances trinoPoolInstanceLister, creds admin.TrinoCredentialSource) *trinoPoolObserver {
	return &trinoPoolObserver{
		poolID:    poolID,
		namespace: namespace,
		port:      port,
		instances: instances,
		creds:     creds,
		newClient: admin.NewTrinoPoolMemberClient,
		clients:   make(map[string]admin.TrinoCoordinatorClient),
	}
}

type trinoPoolObservedMember struct {
	instanceID string
	serving    bool
	client     admin.TrinoCoordinatorClient
}

// members returns the observable instances, serving ones first, in a stable
// order. Clients are cached per instance and dropped once it leaves the set.
func (o *trinoPoolObserver) members(ctx context.Context) ([]trinoPoolObservedMember, error) {
	instances, err := o.instances.ListTrinoPoolInstances(ctx, o.poolID)
	if err != nil {
		return nil, fmt.Errorf("list trino pool instances: %w", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	live := make(map[string]bool)
	var members []trinoPoolObservedMember
	for _, instance := range instances {
		if !trinoPoolObservedPhases[instance.Phase] {
			continue
		}
		live[instance.InstanceID] = true
		client, ok := o.clients[instance.InstanceID]
		if !ok {
			// Same address the pool operator probes: the instance's own Service.
			endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", instance.InstanceID, o.namespace, o.port)
			client = o.newClient(endpoint, o.creds)
			o.clients[instance.InstanceID] = client
		}
		members = append(members, trinoPoolObservedMember{
			instanceID: instance.InstanceID,
			serving:    instance.Phase == string(trinopool.PhaseServing),
			client:     client,
		})
	}
	for id := range o.clients {
		if !live[id] {
			delete(o.clients, id)
		}
	}
	sort.SliceStable(members, func(i, j int) bool {
		if members[i].serving != members[j].serving {
			return members[i].serving
		}
		return members[i].instanceID < members[j].instanceID
	})
	return members, nil
}

// Queries is the union across members. One unreachable member does not hide
// the others' queries; only when every member fails is the call an error.
func (o *trinoPoolObserver) Queries(ctx context.Context) ([]admin.TrinoQuery, error) {
	members, err := o.members(ctx)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	var all []admin.TrinoQuery
	var firstErr error
	succeeded := 0
	for _, member := range members {
		queries, err := member.client.Queries(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("pool member %s: %w", member.instanceID, err)
			}
			continue
		}
		succeeded++
		all = append(all, queries...)
	}
	if succeeded == 0 {
		return nil, firstErr
	}
	return all, nil
}

// Query asks each member until one holds the query. A member that does not
// know it answers not-found, which is the answer only if nobody else has it.
func (o *trinoPoolObserver) Query(ctx context.Context, queryID string) (*admin.TrinoQuery, error) {
	members, err := o.members(ctx)
	if err != nil {
		return nil, err
	}
	var lastErr error = errTrinoPoolNoMembers
	for _, member := range members {
		query, err := member.client.Query(ctx, queryID)
		if err == nil {
			return query, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// KillQuery goes to the member that holds the query. Trino query ids are
// generated per coordinator, so at most one member can kill a given id.
func (o *trinoPoolObserver) KillQuery(ctx context.Context, queryID, message string) error {
	members, err := o.members(ctx)
	if err != nil {
		return err
	}
	var lastErr error = errTrinoPoolNoMembers
	for _, member := range members {
		if _, err := member.client.Query(ctx, queryID); err != nil {
			lastErr = err
			continue
		}
		return member.client.KillQuery(ctx, queryID, message)
	}
	return lastErr
}

// Nodes is the union of every member's inventory. The source is reported
// only when all members answered from the same one; a mixed inventory keeps
// the first member's source, which is the least-detailed claim the console
// can make about the rows it received.
func (o *trinoPoolObserver) Nodes(ctx context.Context) (admin.TrinoNodeInventory, error) {
	members, err := o.members(ctx)
	if err != nil {
		return admin.TrinoNodeInventory{}, err
	}
	if len(members) == 0 {
		return admin.TrinoNodeInventory{}, errTrinoPoolNoMembers
	}
	var inventory admin.TrinoNodeInventory
	var firstErr error
	succeeded := 0
	for _, member := range members {
		nodes, err := member.client.Nodes(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("pool member %s: %w", member.instanceID, err)
			}
			continue
		}
		if succeeded == 0 {
			inventory.Source = nodes.Source
		}
		succeeded++
		inventory.Nodes = append(inventory.Nodes, nodes.Nodes...)
	}
	if succeeded == 0 {
		return admin.TrinoNodeInventory{}, firstErr
	}
	return inventory, nil
}

// ServerInfo answers from the first member that responds, serving members
// first. All members run the pool's one release, so any answer describes it.
func (o *trinoPoolObserver) ServerInfo(ctx context.Context) (*admin.TrinoServerInfo, error) {
	members, err := o.members(ctx)
	if err != nil {
		return nil, err
	}
	var lastErr error = errTrinoPoolNoMembers
	for _, member := range members {
		info, err := member.client.ServerInfo(ctx)
		if err == nil {
			return info, nil
		}
		lastErr = err
	}
	return nil, lastErr
}
