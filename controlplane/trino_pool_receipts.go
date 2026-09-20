//go:build kubernetes

package controlplane

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
)

// Receipts persisted alongside an instance.
//
// A receipt is EVIDENCE, kept so a later reader can answer "why was this
// admitted" or "on whose authority was this deleted" without re-deriving it.
// They are stored as JSON documents rather than scattered columns because they
// are read by people, not by queries.

// storedValidationReceipt is the durable form of a candidate validation.
type storedValidationReceipt struct {
	NodeID          string   `json:"nodeId"`
	ProcessID       string   `json:"processId"`
	CoordinatorID   string   `json:"coordinatorId"`
	AppliedRevision int64    `json:"appliedRevision"`
	AuthRevision    string   `json:"authRevision"`
	ReadyWorkers    int      `json:"readyWorkers"`
	Checks          []string `json:"checks"`
	CertificateHash string   `json:"certificateHash"`
	ObservedAt      string   `json:"observedAt"`
	// Unacknowledged names security components that did not report what they
	// loaded. It is persisted so an operator reading the receipt later can see
	// exactly which part of the authorization state was never acknowledged,
	// rather than inferring it from the absence of a check.
	Unacknowledged []string `json:"unacknowledged,omitempty"`
}

func marshalValidationReceipt(validation trinoPoolValidation) (string, error) {
	encoded, err := json.Marshal(storedValidationReceipt{
		NodeID:          validation.NodeID,
		ProcessID:       validation.ProcessID,
		CoordinatorID:   validation.CoordinatorID,
		AppliedRevision: validation.AppliedRevision,
		AuthRevision:    validation.AuthRevision,
		ReadyWorkers:    validation.ReadyWorkers,
		Checks:          validation.Checks,
		CertificateHash: validation.CertificateHash,
		ObservedAt:      nowUTC().Format(time.RFC3339),
		Unacknowledged:  validation.Unacknowledged,
	})
	if err != nil {
		return "", fmt.Errorf("encode validation receipt: %w", err)
	}
	return string(encoded), nil
}

func unmarshalValidationReceipt(document string) (trinoPoolValidation, error) {
	if document == "" || document == "{}" {
		return trinoPoolValidation{}, fmt.Errorf("no validation receipt was recorded")
	}
	var stored storedValidationReceipt
	if err := json.Unmarshal([]byte(document), &stored); err != nil {
		return trinoPoolValidation{}, err
	}
	if stored.CertificateHash == "" || stored.ProcessID == "" {
		return trinoPoolValidation{}, fmt.Errorf("the recorded validation receipt is incomplete")
	}
	return trinoPoolValidation{
		NodeID:          stored.NodeID,
		ProcessID:       stored.ProcessID,
		CoordinatorID:   stored.CoordinatorID,
		AppliedRevision: stored.AppliedRevision,
		AuthRevision:    stored.AuthRevision,
		ReadyWorkers:    stored.ReadyWorkers,
		Checks:          stored.Checks,
		CertificateHash: stored.CertificateHash,
		Unacknowledged:  stored.Unacknowledged,
	}, nil
}

// storedRetirementReceipt records the Gateway's irreversible claim. It is
// written BEFORE any resource is deleted, so an interrupted deletion can always
// be resumed with proof that the claim existed.
type storedRetirementReceipt struct {
	Incarnation    string `json:"incarnation"`
	Phase          string `json:"phase"`
	Generation     int64  `json:"generation"`
	RetirementKind string `json:"retirementKind"`
	ClaimedAt      string `json:"claimedAt"`
}

func marshalRetirementReceipt(member trinogateway.Member) (string, error) {
	encoded, err := json.Marshal(storedRetirementReceipt{
		Incarnation:    member.Incarnation,
		Phase:          member.Phase,
		Generation:     member.Generation,
		RetirementKind: member.RetirementKind,
		ClaimedAt:      nowUTC().Format(time.RFC3339),
	})
	if err != nil {
		return "", fmt.Errorf("encode retirement receipt: %w", err)
	}
	return string(encoded), nil
}

// instanceNamespace reports where an instance's objects live. The namespace is
// the blueprint's and is pinned per instance, so a pool that is later moved
// cannot make an old instance's delete target the wrong namespace.
func instanceNamespace(instance configstore.TrinoPoolInstance) string {
	var snapshot struct {
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal([]byte(instance.BlueprintSnapshot), &snapshot); err != nil {
		return ""
	}
	return snapshot.Namespace
}

// nowUTC exists so tests can observe stable timestamps without reaching for a
// global clock.
var nowUTC = func() time.Time { return time.Now().UTC() }
