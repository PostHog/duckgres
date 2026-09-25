package configstore

import (
	"errors"
	"time"

	"github.com/posthog/duckgres/controlplane/trinopool"
)

// Durable state of the shared Trino compute pool. Migration 000041 owns the
// physical schema. Everything here is inert unless a registered cell declares
// shared-pool mode and the feature flag is on.

// ErrTrinoPoolConflict is returned when a fenced write loses: a superseded
// authority epoch, a failed phase CAS, or a watermark that would regress. It is
// never a reason to retry with a fresher epoch read from the database — the
// caller has lost leadership and must stop writing.
var ErrTrinoPoolConflict = errors.New("trino pool conflict")

// ErrTrinoPoolIntentChanged is returned when an operation or step id is reused
// with different content. That is a caller bug, not a replay, and applying it
// would perform an effect nobody recorded an intent for.
var ErrTrinoPoolIntentChanged = errors.New("trino pool operation was replayed with different content")

// ErrTrinoPoolStaleGeneration is returned when a desired specification would
// move the recorded generation BACKWARDS.
//
// It is deliberately NOT an ErrTrinoPoolConflict: losing a fence means this
// process is no longer the authority and must stop, whereas this means the
// authority is holding a specification the store considers older. Conflating
// the two ended the leadership term on every tick over what is a configuration
// problem, and handed the pool to a replica that would do exactly the same.
var ErrTrinoPoolStaleGeneration = errors.New("trino pool desired generation is behind the published generation")

// API modes. `legacy` keeps today's fixed blue/green behavior for the cell.
const (
	TrinoPoolAPIModeLegacy = "legacy"
	TrinoPoolAPIModeShared = "shared-pool"
)

// Operation kinds.
const (
	TrinoPoolOperationReplace = "replace"
	TrinoPoolOperationScaleUp = "scale_up"
	TrinoPoolOperationRepair  = "repair"
	TrinoPoolOperationRetire  = "retire"
	TrinoPoolOperationPublish = "publish"
)

// Publication states. Desired, published and admitted are distinct facts; a
// single boolean cannot express "enabled but not yet allowed to query".
const (
	TrinoPublicationPending   = "pending"
	TrinoPublicationPublished = "published"
	TrinoPublicationAdmitting = "admitting"
	TrinoPublicationAdmitted  = "admitted"
	TrinoPublicationFailed    = "failed"
	// TrinoPublicationRevoked is a tenant whose admission was withdrawn. The
	// row is KEPT: a deleted row would read as "never published" and the next
	// tick would republish the binding of a tenant that is meant to be gone.
	TrinoPublicationRevoked = "revoked"
)

// TrinoPool is the desired specification plus the operator's runtime state.
type TrinoPool struct {
	PoolID                 string `gorm:"primaryKey;column:pool_id"`
	PublicID               string `gorm:"column:public_id"`
	APIMode                string `gorm:"column:api_mode"`
	DesiredReleaseID       string `gorm:"column:desired_release_id"`
	DesiredBlueprintDigest string `gorm:"column:desired_blueprint_digest"`
	DesiredInstances       int    `gorm:"column:desired_instances"`
	MinServing             int    `gorm:"column:min_serving"`
	MaxSurge               int    `gorm:"column:max_surge"`
	MaxRepair              int    `gorm:"column:max_repair"`
	// DesiredGeneration orders desired-state CONTENT independently of who
	// wrote it, so a leader carrying an older configuration cannot publish it
	// over a newer one just because it legitimately holds the fence.
	DesiredGeneration   int64     `gorm:"column:desired_generation"`
	AuthorityEpoch      int64     `gorm:"column:authority_epoch"`
	AuthorityOwner      string    `gorm:"column:authority_owner"`
	PublicationRevision int64     `gorm:"column:publication_revision"`
	AdmittedRevision    int64     `gorm:"column:admitted_revision"`
	Frozen              bool      `gorm:"column:frozen"`
	FrozenReason        string    `gorm:"column:frozen_reason"`
	CreatedAt           time.Time `gorm:"column:created_at"`
	UpdatedAt           time.Time `gorm:"column:updated_at"`
}

func (TrinoPool) TableName() string { return "duckgres_trino_pools" }

// TrinoPoolSpec is the desired configuration as resolved from the registry and
// the blueprint. It deliberately carries no runtime state: applying it must not
// disturb the authority epoch, the freeze flag, or anything an operator owns.
type TrinoPoolSpec struct {
	// Generation is the ordering of this desired content. It must increase
	// whenever the spec changes; a publication that does not advance it is
	// refused as stale.
	Generation             int64
	PoolID                 string
	PublicID               string
	APIMode                string
	DesiredReleaseID       string
	DesiredBlueprintDigest string
	DesiredInstances       int
	MinServing             int
	MaxSurge               int
	MaxRepair              int
}

// TrinoPoolLease is proof of current authority over a pool. Every fenced write
// takes one; a write whose epoch no longer matches the stored epoch is refused.
type TrinoPoolLease struct {
	PoolID string
	Owner  string
	Epoch  int64
}

// TrinoPoolInstance is one immutable compute instance.
type TrinoPoolInstance struct {
	InstanceID        string    `gorm:"primaryKey;column:instance_id"`
	PoolID            string    `gorm:"column:pool_id"`
	ReleaseID         string    `gorm:"column:release_id"`
	SpecDigest        string    `gorm:"column:spec_digest"`
	BlueprintSnapshot string    `gorm:"column:blueprint_snapshot;type:jsonb"`
	Phase             string    `gorm:"column:phase"`
	PhaseChangedAt    time.Time `gorm:"column:phase_changed_at"`
	OwnerEpoch        int64     `gorm:"column:owner_epoch"`
	Repair            bool      `gorm:"column:repair"`
	// RepairFor names the failed instance this one replaces. The Gateway
	// charges an activation to the repair budget only when it is set; without
	// it a repair spends the single planned surge instead.
	RepairFor                 string `gorm:"column:repair_for"`
	FailureReason             string `gorm:"column:failure_reason"`
	CoordinatorDeploymentName string `gorm:"column:coordinator_deployment_name"`
	CoordinatorDeploymentUID  string `gorm:"column:coordinator_deployment_uid"`
	WorkerDeploymentName      string `gorm:"column:worker_deployment_name"`
	WorkerDeploymentUID       string `gorm:"column:worker_deployment_uid"`
	ServiceName               string `gorm:"column:service_name"`
	ServiceUID                string `gorm:"column:service_uid"`
	ConfigMapName             string `gorm:"column:config_map_name"`
	ConfigMapUID              string `gorm:"column:config_map_uid"`
	WorkerConfigMapName       string `gorm:"column:worker_config_map_name"`
	WorkerConfigMapUID        string `gorm:"column:worker_config_map_uid"`
	CoordinatorPodUID         string `gorm:"column:coordinator_pod_uid"`
	CoordinatorNodeID         string `gorm:"column:coordinator_node_id"`
	// CoordinatorID is the coordinator identity the GATEWAY observed when the
	// member registered. It is a distinct value from the node id, and a loss
	// claim has to carry both exactly as the Gateway recorded them, or the
	// evidence is refused and the member keeps its live slot forever.
	CoordinatorID string `gorm:"column:coordinator_id"`
	// CoordinatorContainerID is the container instance that hosted the admitted
	// process. Termination records name a container instance, so this is what
	// ties one to the process the Gateway admitted rather than to some earlier
	// restart of the same pod.
	CoordinatorContainerID string `gorm:"column:coordinator_container_id"`
	CoordinatorBootID      string `gorm:"column:coordinator_boot_id"`
	EndpointURL            string `gorm:"column:endpoint_url"`
	// TLSServerName is retained on the row for the fixed-cell path only. A
	// pooled instance is reached over plain in-cluster HTTP and has no
	// certificate of its own, so the pool never sets it.
	TLSServerName          string     `gorm:"column:tls_server_name"`
	GatewayIncarnation     string     `gorm:"column:gateway_incarnation"`
	GatewayBackendName     string     `gorm:"column:gateway_backend_name"`
	GatewayState           string     `gorm:"column:gateway_state"`
	GatewayGeneration      int64      `gorm:"column:gateway_generation"`
	AppliedCatalogRevision int64      `gorm:"column:applied_catalog_revision"`
	ValidationReceipt      string     `gorm:"column:validation_receipt;type:jsonb"`
	ValidatedAt            *time.Time `gorm:"column:validated_at"`
	RetirementReceipt      string     `gorm:"column:retirement_receipt;type:jsonb"`
	LastError              string     `gorm:"column:last_error"`
	CreatedAt              time.Time  `gorm:"column:created_at"`
	UpdatedAt              time.Time  `gorm:"column:updated_at"`
}

func (TrinoPoolInstance) TableName() string { return "duckgres_trino_pool_instances" }

// View projects the row onto the planner's read-only input.
func (i TrinoPoolInstance) View() trinopool.InstanceView {
	return trinopool.InstanceView{
		ID:        i.InstanceID,
		Phase:     trinopool.Phase(i.Phase),
		ReleaseID: i.ReleaseID,
		Repair:    i.Repair,
		RepairFor: i.RepairFor,
		CreatedAt: i.CreatedAt.UnixNano(),
	}
}

// TrinoPoolInstanceSpec is the immutable identity of a new instance.
type TrinoPoolInstanceSpec struct {
	InstanceID        string
	PoolID            string
	ReleaseID         string
	SpecDigest        string
	BlueprintSnapshot string
	Phase             trinopool.Phase
	Repair            bool
	RepairFor         string
	EndpointURL       string
}

// TrinoPoolOperation is a durable reconcile intent that outlives the leader.
type TrinoPoolOperation struct {
	OperationID   string     `gorm:"primaryKey;column:operation_id"`
	PoolID        string     `gorm:"column:pool_id"`
	InstanceID    string     `gorm:"column:instance_id"`
	Kind          string     `gorm:"column:kind"`
	IntentHash    string     `gorm:"column:intent_hash"`
	OwnerEpoch    int64      `gorm:"column:owner_epoch"`
	Step          string     `gorm:"column:step"`
	Phase         string     `gorm:"column:phase"`
	Receipts      string     `gorm:"column:receipts;type:jsonb"`
	LastError     string     `gorm:"column:last_error"`
	Attempts      int64      `gorm:"column:attempts"`
	NextAttemptAt *time.Time `gorm:"column:next_attempt_at"`
	CreatedAt     time.Time  `gorm:"column:created_at"`
	UpdatedAt     time.Time  `gorm:"column:updated_at"`
	TerminalAt    *time.Time `gorm:"column:terminal_at"`

	// Replayed reports that this call found an existing identical operation
	// rather than creating one. It is not a column.
	Replayed bool `gorm:"-"`
}

func (TrinoPoolOperation) TableName() string { return "duckgres_trino_pool_operations" }

// TrinoPoolOperationSpec is the immutable intent of an operation.
type TrinoPoolOperationSpec struct {
	OperationID string
	PoolID      string
	InstanceID  string
	Kind        string
	IntentHash  string
}

// TrinoPoolOperationStep is one idempotent step inside an operation. Scoping
// replay identity to the step is what lets a resumed operation re-run only the
// step that was interrupted.
type TrinoPoolOperationStep struct {
	OperationID string    `gorm:"primaryKey;column:operation_id"`
	StepID      string    `gorm:"primaryKey;column:step_id"`
	PayloadHash string    `gorm:"column:payload_hash"`
	Outcome     string    `gorm:"column:outcome"`
	Result      string    `gorm:"column:result;type:jsonb"`
	RecordedAt  time.Time `gorm:"column:recorded_at"`

	Replayed bool `gorm:"-"`
}

func (TrinoPoolOperationStep) TableName() string { return "duckgres_trino_pool_operation_steps" }

// Recorded step outcomes. They are defined here, next to the row they are
// written into, because the store itself has to distinguish them: an UNKNOWN
// step may be completed by a later attempt, a decided one never is.
const (
	// TrinoPoolStepOutcomeUnknown is recorded BEFORE the external effect. It
	// means "this may or may not have happened", which is the only honest
	// answer to a lost response.
	TrinoPoolStepOutcomeUnknown = "UNKNOWN"
	// TrinoPoolStepOutcomeOK is a completed effect, and its result is the
	// answer a later attempt reads back instead of repeating the call.
	TrinoPoolStepOutcomeOK = "OK"
	// TrinoPoolStepOutcomeFailed is a REFUSAL. Retrying cannot change it.
	TrinoPoolStepOutcomeFailed = "FAILED"
)

// TrinoPoolPublication is one warehouse's publication state on a pool.
type TrinoPoolPublication struct {
	PoolID                 string `gorm:"primaryKey;column:pool_id"`
	OrgID                  string `gorm:"primaryKey;column:org_id"`
	DesiredRevision        int64  `gorm:"column:desired_revision"`
	PublishedRevision      int64  `gorm:"column:published_revision"`
	AdmittedRevision       int64  `gorm:"column:admitted_revision"`
	PublicationID          string `gorm:"column:publication_id"`
	PublicationOperationID string `gorm:"column:publication_operation_id"`
	// PrincipalRevision is the binding (the tenant's principal set) last
	// published to the Gateway, and TargetRevision the configuration revision
	// the open barrier requires every serving member to acknowledge.
	// AdmittedTargetRevision is the last one that actually committed.
	//
	// They are durable rather than remembered in the leader's memory: a
	// restart or a leadership move would otherwise either republish blindly or
	// assume an admission that never happened.
	PrincipalRevision      string `gorm:"column:principal_revision"`
	TargetRevision         string `gorm:"column:target_revision"`
	AdmittedTargetRevision string `gorm:"column:admitted_target_revision"`
	// Attempt is a monotone occurrence counter. It is part of every durable
	// step identity this tenant's barrier and revocations use, so a reopened
	// barrier - or a second revocation after the tenant was re-enabled - is a
	// new operation rather than a replay that returns the first one's outcome.
	Attempt int64 `gorm:"column:attempt"`
	// PendingIntent names the request the open occurrence stands for while its
	// outcome is unknown: "principals", "revoke", or empty for none.
	//
	// A lost response is not a finished request. Until the Gateway gives a
	// definite answer, the next pass reissues THAT occurrence's step identity
	// rather than minting a new one, so a request still executing at the
	// Gateway cannot commit after a newer desired intent has already been
	// checkpointed here.
	PendingIntent string `gorm:"column:pending_intent"`
	// PendingPayload is that request's body, so the reissue is byte-identical
	// to the original: the same step identity carrying the same bytes is an
	// ordinary replay, which is far easier to reason about than sending a new
	// body under an old identity and reading the refusal as a success.
	//
	// It carries principal identifiers and the revision naming them - never a
	// password or a hash.
	PendingPayload string `gorm:"column:pending_payload;type:jsonb"`
	// Attempts and NextAttemptAt are this tenant's own durable backoff. The
	// driver takes one tenant at a time, so without them a permanently failing
	// warehouse is retried every tick and starves every tenant behind it.
	Attempts       int64      `gorm:"column:attempts"`
	NextAttemptAt  *time.Time `gorm:"column:next_attempt_at"`
	State          string     `gorm:"column:state"`
	GatewayReceipt string     `gorm:"column:gateway_receipt;type:jsonb"`
	LastError      string     `gorm:"column:last_error"`
	CreatedAt      time.Time  `gorm:"column:created_at"`
	UpdatedAt      time.Time  `gorm:"column:updated_at"`
}

func (TrinoPoolPublication) TableName() string { return "duckgres_trino_pool_publications" }

// TrinoPoolProjection is the accepted authorization-projection watermark. A
// replica consults it before serving an OPA bundle and refuses to serve
// anything older, so a stale replica never produces a regressing body.
type TrinoPoolProjection struct {
	PoolID           string    `gorm:"primaryKey;column:pool_id"`
	AuthorityEpoch   int64     `gorm:"column:authority_epoch"`
	AcceptedRevision int64     `gorm:"column:accepted_revision"`
	AcceptedDigest   string    `gorm:"column:accepted_digest"`
	UpdatedAt        time.Time `gorm:"column:updated_at"`
}

func (TrinoPoolProjection) TableName() string { return "duckgres_trino_pool_projection" }
