package configstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TrinoClusterBootstrap is a minimal durable sentinel: the existence of
// a row for a namespace means "the customer-Trino cluster credentials
// for this namespace have been generated at least once."
//
// Why only a sentinel (and not the SecretRefs / credential material):
//
//	The three cluster credentials (internal-communication shared secret,
//	__admin_provisioner password + hash, OPA bundle bearer token) are
//	machine-generated random values written as K8s Secrets in the
//	trino-customer namespace. The K8s Secret is the single source of
//	truth for each value — exactly like the worker RPC secret
//	(controlplane/worker_rpc_security.go) and unlike the env-var or
//	externally-sourced secrets. The Secret NAMES are a fixed naming
//	contract (constants in the provisioner), so there are no refs worth
//	persisting.
//
//	The one thing the K8s Secrets cannot encode is "have we EVER
//	bootstrapped." That bit is load-bearing for exactly one credential:
//	the internal-communication shared secret is env-projected into
//	long-lived Trino pods, so if it goes missing post-bootstrap the
//	provisioner must FAIL LOUD (a regenerated value would split-brain
//	the running cluster) rather than treat the absence as a fresh
//	first-boot and generate a new one. This sentinel is what lets the
//	provisioner tell "never bootstrapped → generate" from "bootstrapped
//	then a Secret was deleted → fail loud."
//
//	Storing only this bit (not the values, not even refs) keeps the
//	configstore out of the credential-coordination business entirely:
//	there is no two-store authority split, no advisory lock, and no
//	orphan state — the K8s API's Create/AlreadyExists is the only
//	serialization primitive needed (see provisioner.ensureClusterSecrets).
//
// Namespace primary key: one customer-Trino cluster per namespace;
// future multi-cluster deployments get one sentinel row each.
type TrinoClusterBootstrap struct {
	Namespace      string    `gorm:"primaryKey;size:255" json:"namespace"`
	BootstrappedAt time.Time `json:"bootstrapped_at"`
}

func (TrinoClusterBootstrap) TableName() string { return "duckgres_trino_cluster_bootstrap" }

// IsTrinoClusterBootstrapped reports whether the cluster credentials for
// the namespace have been generated at least once (i.e. the sentinel
// row exists). A read error is returned verbatim so callers can treat
// it as transient and retry rather than mis-classify a DB blip as
// "never bootstrapped" (which would risk regenerating a live secret).
func (cs *ConfigStore) IsTrinoClusterBootstrapped(ctx context.Context, namespace string) (bool, error) {
	if namespace == "" {
		return false, errors.New("IsTrinoClusterBootstrapped: namespace is required")
	}
	var row TrinoClusterBootstrap
	err := cs.db.WithContext(ctx).Where("namespace = ?", namespace).First(&row).Error
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("read trino cluster bootstrap sentinel: %w", err)
}

// MarkTrinoClusterBootstrapped records that the cluster credentials have
// been generated. Idempotent: a row that already exists is left
// untouched (ON CONFLICT DO NOTHING), so concurrent replicas finishing
// their first reconcile can both call this safely without a lock and
// without clobbering the original BootstrappedAt.
//
// Callers MUST only invoke this after confirming all three K8s Secrets
// are present and valid — the sentinel is the gate that flips the
// missing-Secret response from "generate" to "fail loud", so setting it
// prematurely (before the Secrets exist) would wedge the next reconcile
// into fail-loud against Secrets that were never created.
func (cs *ConfigStore) MarkTrinoClusterBootstrapped(ctx context.Context, namespace string) error {
	if namespace == "" {
		return errors.New("MarkTrinoClusterBootstrapped: namespace is required")
	}
	row := TrinoClusterBootstrap{Namespace: namespace, BootstrappedAt: time.Now().UTC()}
	if err := cs.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&row).Error; err != nil {
		return fmt.Errorf("mark trino cluster bootstrapped: %w", err)
	}
	return nil
}
