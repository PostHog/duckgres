//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

type trinoPoolNodeReplacementStore interface {
	RecordTrinoPoolNodeReplacement(context.Context, configstore.TrinoPoolLease, string, trinopool.NodeReplacementEvidence) error
}

var errTrinoCandidateNodeReplacement = errors.New("candidate node requires replacement before admission")
var errTrinoCandidateNodeRetired = errors.New("candidate retired before admission because its node requires replacement")

func definitiveTrinoNodePlacement(err error) bool {
	return errors.Is(err, errTrinoCandidateNodeReplacement) || errors.Is(err, errTrinoUnschedulableNode) || errors.Is(err, errTrinoNodePlacementInvalid)
}

// protectPoolNodes visits all live instances before any instance advances.
// A busy candidate cannot starve the protection of existing serving pods.
func (o *trinoPoolOperator) protectPoolNodes(ctx context.Context, instances []configstore.TrinoPoolInstance) error {
	o.nodeProtectionErrors = make(map[string]error)
	if o.nodeGuard == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	o.nodeGuard.reset()
	o.nodeGuard.epoch = o.lease.Epoch
	store, ok := o.store.(trinoPoolNodeReplacementStore)
	if !ok {
		return errors.New("node disruption handling requires durable replacement storage")
	}
	var failures []error
	start := int(o.nodeProtectionCursor % uint64(max(1, len(instances))))
	o.nodeProtectionCursor++
	for offset := range instances {
		index := (start + offset) % len(instances)
		instance := &instances[index]
		switch trinopool.Phase(instance.Phase) {
		case trinopool.PhasePending, trinopool.PhaseRetiring, trinopool.PhaseRetired, trinopool.PhaseFailureRetired, trinopool.PhaseFailedPreparing, trinopool.PhaseLost:
			continue
		}
		evidence, err := o.nodeGuard.inspect(ctx, inventoryOf(*instance))
		if errors.Is(err, errTrinoPodProtectionIncomplete) && (instance.Phase == string(trinopool.PhaseCreating) || instance.Phase == string(trinopool.PhasePreparing)) {
			// Pending pods are normal convergence, not failed protection of serving work.
			continue
		}
		if errors.Is(err, errTrinoUnschedulableNode) && instance.NodeReplacementEvidence != nil {
			err = nil
		}
		if err == nil && len(evidence) > 0 {
			err = o.dropAuthority(store.RecordTrinoPoolNodeReplacement(ctx, o.lease, instance.InstanceID, evidence[0]))
			if instance.NodeReplacementEvidence == nil {
				if err == nil {
					encoded, _ := json.Marshal(evidence[0])
					value := string(encoded)
					instance.NodeReplacementEvidence = &value
					slog.Info("Trino pool requested voluntary node replacement.", "pool", o.config.PublicID, "instance", instance.InstanceID, "node_uid", evidence[0].NodeUID, "nodeclaim_uid", evidence[0].NodeClaimUID, "reason", evidence[0].Reason)
				}
			}
			if err == nil {
				for _, node := range evidence {
					if err = o.nodeGuard.cordon(ctx, node, o.config.PoolID, instance.InstanceID); err != nil {
						break
					}
				}
			}
		}
		if err != nil {
			o.nodeProtectionErrors[instance.InstanceID] = err
			failures = append(failures, fmt.Errorf("protect instance %s from node disruption: %w", instance.InstanceID, err))
			if o.fenced {
				return errors.Join(failures...)
			}
		}
	}
	return errors.Join(failures...)
}

func (o *trinoPoolOperator) candidateNodeCheck(ctx context.Context, instance configstore.TrinoPoolInstance) error {
	if o.nodeGuard == nil {
		return nil
	}
	if instance.NodeReplacementEvidence != nil {
		return errTrinoCandidateNodeReplacement
	}
	o.nodeGuard.reset()
	o.nodeGuard.epoch = o.lease.Epoch
	evidence, err := o.nodeGuard.inspect(ctx, inventoryOf(instance))
	if err != nil {
		return err
	}
	if len(evidence) != 0 || instance.NodeReplacementEvidence != nil {
		return errTrinoCandidateNodeReplacement
	}
	return nil
}

// nodeSafeAdmission resolves committed admission before inspecting new infrastructure facts.
// A failed fresh check cannot erase an admission whose response was lost.
func (o *trinoPoolOperator) nodeSafeAdmission(ctx context.Context, instance configstore.TrinoPoolInstance, request trinogateway.AdmitMemberRequest) (trinogateway.Member, error) {
	if o.nodeGuard != nil {
		current, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
		if err != nil {
			return trinogateway.Member{}, err
		}
		if current.Incarnation != instance.GatewayIncarnation || current.PodUID != instance.CoordinatorPodUID || current.BootID != instance.CoordinatorBootID {
			return trinogateway.Member{}, errors.New("admission read-back identity changed")
		}
		if current.Phase == "ACTIVE" {
			return o.gateway.AdmitMember(ctx, o.config.RoutingGroup, instance.InstanceID, request)
		}
		if current.Phase == "RETIRING" || current.Phase == "RETIRED" {
			return trinogateway.Member{}, errTrinoCandidateNodeRetired
		}
		if err := o.candidateNodeCheck(ctx, instance); err != nil {
			if definitiveTrinoNodePlacement(err) {
				_, retireErr := o.gateway.RetireMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{Step: o.step(instance.InstanceID, "retire"), ExpectedGeneration: current.Generation})
				if retireErr != nil {
					return trinogateway.Member{}, retireErr
				}
				return trinogateway.Member{}, errTrinoCandidateNodeRetired
			}
			return trinogateway.Member{}, err
		}
	}
	return o.gateway.AdmitMember(ctx, o.config.RoutingGroup, instance.InstanceID, request)
}
