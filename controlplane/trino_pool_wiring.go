//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// Startup wiring for the shared Trino pool.
//
// The operator is constructed at startup and attached to the EXISTING janitor
// leader lease, exactly like the reshard reconciler and the usage collectors.
// With the feature disabled this returns no operators and nothing changes; with
// the pool enabled but the operator disabled, the loops run in a read-only mode
// that keeps the durable desired state in sync and touches nothing external.
//
// Wiring failures are fatal, for the same reason the existing Trino branch is
// fatal: silently skipping would leave an operator believing a pool is being
// reconciled while nothing is.

// buildTrinoPoolOperators constructs one operator per shared-pool cell.
func buildTrinoPoolOperators(
	store *configstore.ConfigStore,
	fleet trinoFleet,
	controlPlaneID string,
) ([]*trinoPoolOperator, error) {
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		return nil, err
	}
	if len(configs) == 0 {
		return nil, nil
	}
	operatorEnabled := trinoPoolOperatorEnabled()
	gateway, err := buildTrinoPoolGateway()
	if err != nil {
		return nil, err
	}
	if operatorEnabled && gateway == nil {
		// Without a Gateway the operator could create compute but never admit,
		// drain or retire it. Creating unadmittable instances is worse than
		// refusing to start.
		return nil, fmt.Errorf("%s is enabled but %s is not configured", envTrinoPoolOperatorEnabled, envTrinoPoolGatewayURL)
	}

	// The owner identity must be unique per PROCESS, not merely per control
	// plane: two processes sharing an identity would both satisfy the fence's
	// owner check and the epoch would stop distinguishing them.
	owner := trinoPoolOwnerIdentity(controlPlaneID)

	operators := make([]*trinoPoolOperator, 0, len(configs))
	for _, config := range configs {
		// Credentials and the Kubernetes client are taken from THIS pool's own
		// cell. Picking the first entry of a map meant a pool could be certified
		// with another cell's observer credential, chosen by Go's randomized map
		// order - a different answer on different boots.
		wire := fleet.byStoredID(config.PoolID)
		if wire == nil {
			return nil, fmt.Errorf("shared Trino pool %s has no wired cell", config.PublicID)
		}
		if wire.Kubernetes == nil {
			return nil, fmt.Errorf("shared Trino pool %s has no Kubernetes client", config.PublicID)
		}
		if wire.Provisioner == nil {
			return nil, fmt.Errorf("shared Trino pool %s has no operational credential", config.PublicID)
		}
		clientset := wire.Kubernetes
		observerCredential := wire.Provisioner.ObserverCredential

		operator := &trinoPoolOperator{
			config:          config,
			store:           store,
			gateway:         gateway,
			owner:           owner,
			operatorEnabled: operatorEnabled,
			newInstanceID:   newTrinoPoolInstanceID,
			tenants:         store,
			publications:    store,
			operations:      store,
		}
		// Desired state is published from the ConfigMap the chart projects the
		// registry and blueprint from, read through the Kubernetes API.
		//
		// The mounted copies are how this process learned the pool exists, and
		// that is all they are trusted for. A projected volume is refreshed per
		// pod on the kubelet's own schedule - and not at all under a subPath
		// mount - so two replicas can hold different contents indefinitely and
		// an idle pod can hold a configuration the cluster replaced hours ago.
		// Publishing from the API object means every replica derives desired
		// state from one value, and re-reading it before each publication is
		// what bounds the staleness window to a single read.
		configSource, err := newTrinoPoolAPIConfigReader(clientset, config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("configure the desired-state source for pool %s: %w", config.PublicID, err)
		}
		publicID := config.PublicID
		operator.resolveConfig = func() (trinoPoolConfig, error) {
			return resolveTrinoPoolConfigByID(context.Background(), configSource, publicID)
		}
		// The Kubernetes effects follow the CURRENT configuration, because a new
		// blueprint may name different pool-shared resources, and those are the
		// objects the effects refuse to touch.
		operator.kube = func(epoch int64) trinoPoolKube {
			shared := trinopool.BlueprintSharedResources{}
			if operator.config.Blueprint != nil {
				shared = operator.config.Blueprint.SharedResources
			}
			return newTrinoPoolEffects(clientset, operator.config.Namespace, shared, epoch)
		}
		// Pooled coordinators are reached directly on their in-cluster Service
		// over plain HTTP: TLS terminates at the Gateway. The probe declares the
		// forwarded HTTPS hop rather than relaxing authentication, so a
		// coordinator that rejects credentials over unforwarded HTTP keeps
		// rejecting them.
		client := newRolloutHTTPClient("")
		operator.validate = func(ctx context.Context, endpoint string, observed trinoPoolObservation, expected trinoPoolExpectation) (trinoPoolValidation, error) {
			return validateTrinoPoolCandidate(ctx, client, endpoint, observerCredential, observed, expected)
		}
		operator.identity = func(ctx context.Context, endpoint string) (string, error) {
			return probeProcessIdentity(ctx, client, endpoint, observerCredential, true)
		}
		// What a publication receipt asserts about ONE member, read from that
		// member rather than assumed from what was published.
		operator.acknowledgement = func(ctx context.Context, endpoint string, expected trinoPoolProjectionRevisions, catalogRevision int64) (trinoPoolAcknowledgement, error) {
			return probeMemberAcknowledgement(ctx, client, endpoint, observerCredential, expected, catalogRevision)
		}
		// The projection is produced by THIS cell's provisioner: it serves the
		// bundle the candidate's OPA pulls and writes the Secret the candidate
		// mounts its password and group files from.
		provisionerForProjection := wire.Provisioner
		operator.projection = func() trinoPoolProjectionRevisions {
			password, group := provisionerForProjection.PublishedAuthRevisions()
			return trinoPoolProjectionRevisions{
				Policy:   provisionerForProjection.PublishedPolicyRevision(),
				Password: password,
				Group:    group,
			}
		}

		// The lease this process currently holds over the pool.
		//
		// It is an atomic pointer because the two sides run on different
		// goroutines: the operator's leader loop writes it, and the
		// provisioner's per-cell reconcile goroutines read it on every catalog
		// publication and every projection. A plain captured variable was a
		// data race, and the value it raced on decides whether a write is
		// fenced at all.
		authority := &atomic.Pointer[configstore.TrinoPoolLease]{}

		// The projection fence. A pooled cell's authorization and
		// authentication projections are accepted by the control plane's own
		// durable record, and only by a process running the image the
		// deployment currently wants: an older binary that won the lease would
		// otherwise publish its own older rules under a newer revision, which a
		// counter cannot detect.
		//
		// Both inputs are rendered by the chart from one image helper: this
		// process's own startup environment, and the pool ConfigMap, which is
		// re-read immediately before every publication and carries the desired
		// publisher image alongside the desired configuration.
		producer := newTrinoPoolProducerIdentity()
		// Serving is fenced by the same record, on EVERY replica - not just the
		// one holding the authority. A replica whose projection has been
		// replaced must stop handing it to coordinators, and it is precisely
		// the replica that does not know it is behind.
		accepted := &trinoPoolAcceptedProjection{store: store, poolID: config.PoolID}
		if wire.BundleHandler != nil {
			wire.BundleHandler.AcceptedRevision = accepted.digest
		}
		// Candidate admission compares against the same durable record, for the
		// same reason: what THIS process published is not evidence about what
		// the pool accepts.
		operator.acceptedProjection = func() string {
			digest, known := accepted.digest()
			if !known {
				return ""
			}
			return digest
		}
		provisionerForProjection.SetProjectionFence(&trinoPoolProjectionFence{
			store:        store,
			publicID:     config.PublicID,
			authority:    authority,
			configSource: configSource,
			producer:     producer,
			accepted:     accepted,
		})

		// With the Gateway's admission restriction on, a warehouse is not
		// queryable until its publication barrier commits - so it must not read
		// as Ready before then. The durable publication record is the answer;
		// the Gateway's own state is authoritative for it and is what wrote it.
		if config.Pool.TenantAdmission {
			poolID := config.PoolID
			wire.Provisioner.SetTenantAdmissionGate(func(orgID string) (bool, string) {
				publication, err := store.GetTrinoPoolPublication(context.Background(), poolID, orgID)
				if err != nil {
					return false, "the pool's publication state is unreadable"
				}
				return trinoPoolTenantIsAdmitted(publication)
			})
		}

		// The fenced catalog writer, if this deployment has moved the cell off
		// the coordinator-mediated path. Its fence is the pool authority, so it
		// is claimed and installed when the operator wins the lease - never at
		// startup, where every replica would claim it.
		//
		// It publishes under the SAME lease the projection fence uses.
		writer, err := buildTrinoPoolCatalogWriter(config.PoolID, store,
			func() (configstore.TrinoPoolLease, bool) {
				lease := authority.Load()
				if lease == nil {
					return configstore.TrinoPoolLease{}, false
				}
				return *lease, true
			},
			// Node inventory still comes from a live coordinator: the catalog
			// store knows nothing about cluster membership. For a pooled cell
			// that is whichever instance the operator is currently validating,
			// so the bridge is given no static node client and the provisioner's
			// readiness check reads the pool's instances instead.
			nil)
		if err != nil {
			return nil, fmt.Errorf("configure catalog writer for pool %s: %w", config.PublicID, err)
		}
		if writer != nil {
			provisionerForCell := wire.Provisioner
			// The admission gate's revision is read from the catalog store
			// itself, so a checkpoint that failed after a committed catalog is
			// recovered rather than waiting for a mutation that will never come.
			operator.catalogWatermark = writer.PublishedRevision
			operator.installWriter = func(ctx context.Context, lease configstore.TrinoPoolLease) error {
				held := lease
				authority.Store(&held)
				if err := writer.ClaimWriter(ctx); err != nil {
					authority.Store(nil)
					return err
				}
				provisionerForCell.SetCatalogClient(writer)
				slog.Info("Shared Trino pool catalog writer claimed.",
					"pool", config.PublicID, "epoch", lease.Epoch)
				return nil
			}
			// When the leadership term ends - cancelled or fenced - the writer
			// stops being able to publish. Leaving the lease behind would let a
			// superseded process keep issuing writes that are only refused at
			// the store's own fence, one failed catalog publication at a time.
			operator.releaseWriter = func() { authority.Store(nil) }
		}
		operators = append(operators, operator)

		slog.Info("Shared Trino pool configured.",
			"pool", config.PublicID, "namespace", config.Namespace,
			"desired", config.Spec.DesiredInstances, "minServing", config.Spec.MinServing,
			"operator", operatorEnabled, "frozen", config.Frozen, "reason", config.FrozenReason)
	}
	return operators, nil
}

// buildTrinoPoolGateway builds the pooled-lifecycle client from the existing
// managed-gateway configuration. It returns (nil, nil) when no Gateway is
// configured, which is valid while the operator is disabled.
func buildTrinoPoolGateway() (trinoPoolGateway, error) {
	endpoint := strings.TrimSpace(os.Getenv(envTrinoPoolGatewayURL))
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL"))
	}
	if endpoint == "" {
		return nil, nil
	}
	token, err := readTrinoPoolGatewayToken()
	if err != nil {
		return nil, err
	}
	client, err := trinogateway.NewClient(trinogateway.Config{
		BaseURL:       endpoint,
		AdminToken:    token,
		APIUsername:   os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME"),
		TLSServerName: strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_SERVER_NAME")),
	})
	if err != nil {
		return nil, fmt.Errorf("configure shared-pool Gateway client: %w", err)
	}
	return client, nil
}

// readTrinoPoolGatewayToken reuses the existing rollout capability token file.
// The pooled protocol authenticates with the Gateway's existing admin
// credential and introduces no new secret.
func readTrinoPoolGatewayToken() (string, error) {
	path := strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE"))
	if path == "" {
		return "", fmt.Errorf("a shared-pool Gateway is configured but DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE is unset")
	}
	token, err := readRolloutSecretFile(path, 4096)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(token)), nil
}

// attachTrinoPoolOperators registers the reconcile loops under the janitor
// leader lease. One control plane reconciles a pool at a time; the authority
// epoch is what makes that safe rather than merely likely.
func attachTrinoPoolOperators(leader *JanitorLeaderManager, operators []*trinoPoolOperator) {
	if leader == nil {
		return
	}
	for _, operator := range operators {
		leader.AttachLeaderLoop(operator.Run)
	}
}

// trinoPoolOwnerIdentity makes the fence owner unique per process. The control
// plane instance id survives a restart, so two processes could otherwise both
// match the recorded owner and the epoch alone would decide - which is exactly
// the ambiguity the owner check exists to remove.
func trinoPoolOwnerIdentity(controlPlaneID string) string {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		// Fall back to the pid: still per-process on this node, and better than
		// a shared constant.
		return fmt.Sprintf("%s.pid-%d", controlPlaneID, os.Getpid())
	}
	return controlPlaneID + "." + hex.EncodeToString(buffer)
}

// trinoPoolTenantIsAdmitted answers the Ready gate: may this warehouse be
// reported as queryable?
//
// The question is whether it has EVER been admitted and still is - not whether
// a barrier happens to be open right now. Adding a login opens a new attempt,
// and the publication's state leaves `admitted` while that attempt runs, so a
// state-only reading would flap a warehouse that has been serving for weeks
// back to Provisioning because somebody created a user. The committed target
// revision is the durable evidence that the Gateway has dispatched for it.
//
// A revoked tenant is not serving, one that never committed a barrier was never
// dispatchable, and a missing record is not evidence of anything - all three
// wait.
func trinoPoolTenantIsAdmitted(publication *configstore.TrinoPoolPublication) (bool, string) {
	if publication == nil {
		return false, "waiting for the pool to publish this warehouse"
	}
	if publication.State == configstore.TrinoPublicationRevoked {
		return false, "this warehouse's admission was revoked"
	}
	if publication.AdmittedTargetRevision == "" {
		return false, "waiting for the pool to admit this warehouse: " + publication.State
	}
	return true, ""
}
