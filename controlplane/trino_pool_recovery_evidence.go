//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

const trinoPoolRecoveryInventoryInterval = 30 * time.Second

type trinoPoolRecoveryEvidenceKey struct {
	namespace string
	request   configstore.TrinoPoolRecovery
}

type trinoPoolRecoveryInventory struct {
	eligible    map[trinoPoolRecoveryEvidenceKey]bool
	uids        map[string]bool
	err         error
	nextAttempt time.Time
}

// Evidence is local to one leadership term and bounded by active recoveries.
// A verified absent Kubernetes UID cannot identify a subsequently created pod.
type trinoPoolRecoveryEvidence struct {
	active    map[trinoPoolRecoveryEvidenceKey]bool
	absent    map[trinoPoolRecoveryEvidenceKey]bool
	inventory map[string]trinoPoolRecoveryInventory
}

func (e *trinoPoolRecoveryEvidence) prepare(instances []configstore.TrinoPoolInstance, requests map[string]configstore.TrinoPoolRecovery) {
	e.active = make(map[trinoPoolRecoveryEvidenceKey]bool)
	namespaces := make(map[string]bool)
	for _, instance := range instances {
		if request, ok := requests[instance.InstanceID]; ok && !trinopool.Phase(instance.Phase).Terminal() {
			key := trinoPoolRecoveryEvidenceKey{namespace: instanceNamespace(instance), request: request}
			e.active[key] = true
			namespaces[key.namespace] = true
		}
	}
	for key := range e.absent {
		if !e.active[key] {
			delete(e.absent, key)
		}
	}
	for namespace := range e.inventory {
		if !namespaces[namespace] {
			delete(e.inventory, namespace)
		}
	}
}

func (o *trinoPoolOperator) recoveryPodAbsent(ctx context.Context, instance configstore.TrinoPoolInstance, request configstore.TrinoPoolRecovery) (bool, error) {
	key := trinoPoolRecoveryEvidenceKey{namespace: instanceNamespace(instance), request: request}
	if key.namespace == "" || request.PodUID == "" {
		return false, errors.New("recovery absence requires the pinned namespace and admitted pod UID")
	}
	e := &o.recoveryEvidence
	if e.absent[key] {
		return true, nil
	}
	now := time.Now()
	inventory, found := e.inventory[key.namespace]
	if !found || !now.Before(inventory.nextAttempt) {
		// Capture eligible requests before enumeration. A later request cannot use an earlier snapshot.
		eligible := map[trinoPoolRecoveryEvidenceKey]bool{key: true}
		for candidate := range e.active {
			if candidate.namespace == key.namespace {
				eligible[candidate] = true
			}
		}
		uids, err := o.kube(o.lease.Epoch).NamespacePodUIDs(ctx, key.namespace)
		if err == nil && uids == nil {
			err = errors.New("recovery namespace inventory is incomplete")
		}
		inventory = trinoPoolRecoveryInventory{eligible: eligible, uids: uids, err: err, nextAttempt: now.Add(trinoPoolRecoveryInventoryInterval)}
		if e.inventory == nil {
			e.inventory = make(map[string]trinoPoolRecoveryInventory)
		}
		e.inventory[key.namespace] = inventory
	}
	if inventory.err != nil {
		return false, inventory.err
	}
	if !inventory.eligible[key] {
		return false, errors.New("recovery awaits a namespace inventory taken after its authorization")
	}
	if inventory.uids[request.PodUID] {
		return false, nil
	}
	if e.absent == nil {
		e.absent = make(map[trinoPoolRecoveryEvidenceKey]bool)
	}
	e.absent[key] = true
	return true, nil
}
