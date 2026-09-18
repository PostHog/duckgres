package trinogateway

// Wire types for the Gateway pooled-member protocol v1.
//
// These mirror the Java records in PoolStore (PoolState, Member, Obligations,
// Publication, TenantAdmission, FailureReceipt, OperationHistory) and the field
// names PoolLifecycleService actually reads out of each request body. They are
// pinned by decoding fixtures serialized by the real Java records, not by a
// hand-written copy of a design document.
//
// Two details that are easy to get wrong and fail only at runtime:
//
//   - The Gateway computes each mutation's payload hash ITSELF, as a SHA-256
//     over the canonicalized request body. A client must not send a payloadHash
//     field; the only exception is openPublication, where payloadHash is an
//     explicit publication input.
//   - Admission is ONE call, POST .../admit, carrying a nested receipt. There
//     is no separate certificate or activate route.

// Checks duckgres performs against a candidate. The Gateway records the list
// verbatim and does not claim to have performed any of them; it independently
// verifies the live process identity from the member's own endpoint.
const (
	CheckImage                 = "image"
	CheckWorkers               = "workers"
	CheckCatalogRevision       = "catalog-revision"
	CheckAuthRevision          = "auth-revision"
	CheckOperationalConnection = "operational-connection"
)

// Step is the idempotency envelope every mutation carries. The Gateway derives
// the payload hash from the whole body, so replaying an identical body resolves
// to the recorded result and a changed body under the same step is a conflict.
type Step struct {
	OperationID     string `json:"operationId"`
	StepID          string `json:"stepId"`
	ControllerEpoch int64  `json:"controllerEpoch"`
	// OwnerIdentity names the controller PROCESS holding the pool. The Gateway
	// records it (`owner_identity = coalesce(:owner, owner_identity)`) and
	// refuses an equal epoch presented by a different owner, so leaving it out
	// left the recorded owner NULL and reduced the fence to the epoch alone -
	// which admits an equal epoch from any other controller. It is part of the
	// authority envelope, which the Gateway strips before it hashes the
	// payload, so sending it cannot turn a replay into a conflict.
	OwnerIdentity string `json:"ownerIdentity,omitempty"`
}

// PoolState is PoolStore.PoolState.
type PoolState struct {
	ProtocolVersion        int              `json:"protocolVersion"`
	PoolID                 string           `json:"poolId"`
	APIMode                string           `json:"apiMode"`
	ControllerEpoch        int64            `json:"controllerEpoch"`
	MembershipGeneration   int64            `json:"membershipGeneration"`
	MinServing             int              `json:"minServing"`
	DesiredMembers         int              `json:"desiredMembers"`
	MaxSurge               int              `json:"maxSurge"`
	MaxRepair              int              `json:"maxRepair"`
	DesiredRevision        string           `json:"desiredRevision"`
	AdmittedRevision       string           `json:"admittedRevision"`
	TenantAdmissionEnabled bool             `json:"tenantAdmissionEnabled"`
	Counts                 map[string]int64 `json:"counts"`
	ServingMembers         int64            `json:"servingMembers"`
	LiveMembers            int64            `json:"liveMembers"`
	SurgeInUse             int64            `json:"surgeInUse"`
	RepairInUse            int64            `json:"repairInUse"`
	OpenPublications       int64            `json:"openPublications"`
	Blocked                []string         `json:"blocked"`
	Replayed               bool             `json:"replayed"`
}

// ConfigurePoolRequest is the PUT body. desiredMembers defaults to minServing
// on the Gateway side when absent, so it is always sent explicitly here.
type ConfigurePoolRequest struct {
	Step
	APIMode                string `json:"apiMode"`
	MinServing             int    `json:"minServing"`
	DesiredMembers         int    `json:"desiredMembers"`
	MaxSurge               int    `json:"maxSurge"`
	MaxRepair              int    `json:"maxRepair"`
	DesiredRevision        string `json:"desiredRevision,omitempty"`
	TenantAdmissionEnabled bool   `json:"tenantAdmissionEnabled"`
}

// Member is PoolStore.Member.
type Member struct {
	ProtocolVersion      int    `json:"protocolVersion"`
	PoolID               string `json:"poolId"`
	InstanceID           string `json:"instanceId"`
	Incarnation          string `json:"incarnation"`
	BackendName          string `json:"backendName"`
	URL                  string `json:"url"`
	ExternalURL          string `json:"externalUrl"`
	Phase                string `json:"phase"`
	Generation           int64  `json:"generation"`
	ControllerEpoch      int64  `json:"controllerEpoch"`
	PodUID               string `json:"podUid"`
	BootID               string `json:"bootId"`
	NodeID               string `json:"nodeId"`
	CoordinatorID        string `json:"coordinatorId"`
	ConfigRevision       string `json:"configRevision"`
	CertifiedRevision    string `json:"certifiedRevision"`
	AuthRevision         string `json:"authRevision"`
	Repair               bool   `json:"repair"`
	RepairFor            string `json:"repairFor"`
	RetirementKind       string `json:"retirementKind"`
	PendingRequests      int64  `json:"pendingRequests"`
	OpenTransactions     int64  `json:"openTransactions"`
	ActiveQueries        int64  `json:"activeQueries"`
	ReadyToSeal          bool   `json:"readyToSeal"`
	Drained              bool   `json:"drained"`
	Eligible             bool   `json:"eligible"`
	MembershipGeneration int64  `json:"membershipGeneration"`
	Replayed             bool   `json:"replayed"`
}

// Obligations is PoolStore.Obligations, returned by its own endpoint.
//
// Drain completion is read from HERE, never from a Member response: Go decodes
// an absent JSON field as zero, so a Member that happens to omit the counters
// would look like a safely drained member. This record always carries them.
type Obligations struct {
	ProtocolVersion  int    `json:"protocolVersion"`
	InstanceID       string `json:"instanceId"`
	Incarnation      string `json:"incarnation"`
	Phase            string `json:"phase"`
	Generation       int64  `json:"generation"`
	PendingRequests  int64  `json:"pendingRequests"`
	OpenTransactions int64  `json:"openTransactions"`
	ActiveQueries    int64  `json:"activeQueries"`
	ReadyToSeal      bool   `json:"readyToSeal"`
	Drained          bool   `json:"drained"`
}

// Outstanding reports the work still pinned to the member.
func (o Obligations) Outstanding() int64 {
	return o.PendingRequests + o.OpenTransactions + o.ActiveQueries
}

// RegisterMemberRequest creates an unroutable PREPARING member.
//
// backendName must already exist as a Gateway backend registration in this
// routing group: the Gateway takes the endpoint from that record rather than
// trusting a caller-supplied URL, and observes the coordinator's process
// identity itself. url is optional and, when sent, must match exactly.
type RegisterMemberRequest struct {
	Step
	InstanceID     string `json:"instanceId"`
	BackendName    string `json:"backendName"`
	URL            string `json:"url,omitempty"`
	PodUID         string `json:"podUid"`
	BootID         string `json:"bootId"`
	ConfigRevision string `json:"configRevision"`
	// RepairFor charges the member to the repair budget instead of the single
	// planned surge, and names the failed instance it replaces.
	RepairFor string `json:"repairFor,omitempty"`
}

// ValidationReceipt is the nested receipt of an admission. Every string field is
// required by the Gateway: an empty value is rejected as a validation error.
type ValidationReceipt struct {
	CertificateHash string   `json:"certificateHash"`
	ConfigRevision  string   `json:"configRevision"`
	AuthRevision    string   `json:"authRevision"`
	PodUID          string   `json:"podUid"`
	BootID          string   `json:"bootId"`
	NodeID          string   `json:"nodeId"`
	CoordinatorID   string   `json:"coordinatorId"`
	ReadyWorkers    int      `json:"readyWorkers"`
	Checks          []string `json:"checks"`
}

// AdmitMemberRequest is the single certified-activation call.
type AdmitMemberRequest struct {
	Step
	ExpectedGeneration int64             `json:"expectedGeneration"`
	Receipt            ValidationReceipt `json:"receipt"`
}

// MemberStepRequest is the envelope for drain, seal, retire and retired.
type MemberStepRequest struct {
	Step
	ExpectedGeneration int64 `json:"expectedGeneration"`
	// ResourcesAbsent is the operator's assertion on `retired`. The Gateway
	// records it and never infers resource deletion for itself.
	ResourcesAbsent bool `json:"resourcesAbsent,omitempty"`
}

// SuspectMemberRequest excludes a member from new admissions. The reason is
// required and is recorded.
type SuspectMemberRequest struct {
	Step
	ExpectedGeneration int64  `json:"expectedGeneration"`
	Reason             string `json:"reason"`
}

// LostMemberRequest claims a member's process terminated. Evidence is
// mandatory: a probe timeout is not death, and a partitioned but possibly live
// process needs an explicit destructive authorization instead.
type LostMemberRequest struct {
	Step
	ExpectedGeneration       int64            `json:"expectedGeneration"`
	Evidence                 string           `json:"evidence"`
	Termination              TerminationProof `json:"termination"`
	DestructiveAuthorization bool             `json:"destructiveAuthorization,omitempty"`
	Reason                   string           `json:"reason,omitempty"`
}

// TerminationProof binds a loss claim to one exact incarnation.
type TerminationProof struct {
	PodUID        string `json:"podUid"`
	BootID        string `json:"bootId"`
	NodeID        string `json:"nodeId"`
	CoordinatorID string `json:"coordinatorId"`
	Source        string `json:"source"`
	ObservedAt    string `json:"observedAt,omitempty"`
}

// Evidence values for a loss claim.
const (
	EvidenceProcessTerminated   = "PROCESS_TERMINATED"
	EvidenceDestructiveOverride = "DESTRUCTIVE_OVERRIDE"
)

// FailureReceipt is PoolStore.FailureReceipt: a failed member's preserved
// obligations, never reported as a successful drain.
type FailureReceipt struct {
	ProtocolVersion int    `json:"protocolVersion"`
	PoolID          string `json:"poolId"`
	InstanceID      string `json:"instanceId"`
	Incarnation     string `json:"incarnation"`
	Evidence        string `json:"evidence"`
	// Detail is the Gateway's free-form evidence record, kept as decoded JSON
	// so a future field cannot silently change its meaning here.
	Detail                  map[string]any `json:"detail"`
	OutstandingAdmissions   int64          `json:"outstandingAdmissions"`
	OutstandingTransactions int64          `json:"outstandingTransactions"`
	OutstandingQueries      int64          `json:"outstandingQueries"`
	RecordedAt              string         `json:"recordedAt"`
}

// OperationStep is one recorded step outcome.
type OperationStep struct {
	StepID          string         `json:"stepId"`
	PayloadHash     string         `json:"payloadHash"`
	ControllerEpoch int64          `json:"controllerEpoch"`
	Outcome         string         `json:"outcome"`
	RecordedAt      string         `json:"recordedAt"`
	Result          map[string]any `json:"result"`
}

// OperationHistory is the read-back that resolves a lost response.
type OperationHistory struct {
	ProtocolVersion int             `json:"protocolVersion"`
	OperationID     string          `json:"operationId"`
	Steps           []OperationStep `json:"steps"`
}

// Step returns the recorded outcome of one step, if it was reached.
func (o OperationHistory) Step(stepID string) (OperationStep, bool) {
	for _, step := range o.Steps {
		if step.StepID == stepID {
			return step, true
		}
	}
	return OperationStep{}, false
}

// OpenPublicationRequest opens the tenant publication barrier. payloadHash is
// an explicit input here, unlike the guard hash the Gateway computes itself.
type OpenPublicationRequest struct {
	Step
	PublicationID                string `json:"publicationId"`
	Tenant                       string `json:"tenant"`
	TargetRevision               string `json:"targetRevision"`
	ExpectedMembershipGeneration int64  `json:"expectedMembershipGeneration"`
	PayloadHash                  string `json:"payloadHash"`
}

// PublicationReceiptRequest records one member's application-loaded state.
type PublicationReceiptRequest struct {
	Step
	InstanceID      string `json:"instanceId"`
	PodUID          string `json:"podUid"`
	BootID          string `json:"bootId"`
	AppliedRevision string `json:"appliedRevision"`
	AuthFingerprint string `json:"authFingerprint"`
}

// CommitPublicationRequest closes the barrier and opens the tenant gate.
type CommitPublicationRequest struct {
	Step
	ExpectedMembershipGeneration int64 `json:"expectedMembershipGeneration"`
}

// PublicationReceipt is one recorded member acknowledgement.
type PublicationReceipt struct {
	InstanceID      string `json:"instanceId"`
	Incarnation     string `json:"incarnation"`
	PodUID          string `json:"podUid"`
	BootID          string `json:"bootId"`
	AppliedRevision string `json:"appliedRevision"`
	AuthFingerprint string `json:"authFingerprint"`
}

// Publication is PoolStore.Publication.
type Publication struct {
	ProtocolVersion      int                  `json:"protocolVersion"`
	PublicationID        string               `json:"publicationId"`
	PoolID               string               `json:"poolId"`
	Tenant               string               `json:"tenant"`
	TargetRevision       string               `json:"targetRevision"`
	MembershipGeneration int64                `json:"membershipGeneration"`
	Phase                string               `json:"phase"`
	RequiredMembers      []string             `json:"requiredMembers"`
	Receipts             []PublicationReceipt `json:"receipts"`
	MissingMembers       []string             `json:"missingMembers"`
	TenantState          string               `json:"tenantState"`
	AdmittedRevision     string               `json:"admittedRevision"`
	Replayed             bool                 `json:"replayed"`
}

// TenantAdmission is PoolStore.TenantAdmission.
type TenantAdmission struct {
	ProtocolVersion  int    `json:"protocolVersion"`
	PoolID           string `json:"poolId"`
	Tenant           string `json:"tenant"`
	State            string `json:"state"`
	AdmittedRevision string `json:"admittedRevision"`
	PublicationID    string `json:"publicationId"`
	// PrincipalRevision and Principals are the authoritative binding the
	// Gateway's admission restriction keys on.
	PrincipalRevision string   `json:"principalRevision"`
	Principals        []string `json:"principals"`
	Replayed          bool     `json:"replayed"`
}

// PublishPrincipalsRequest publishes a tenant's authoritative principal set.
//
// The Gateway cannot derive these strings. A tenant's logins are one flat
// namespace produced by the controller's own projection - a root login that
// carries no separator at all, plus qualified per-user names - and they are not
// a function of the tenant identifier. Deriving them from the shape of a name
// would refuse legitimate root logins and could bind a principal to the wrong
// tenant, so the controller states them.
//
// The set is replaced whole: a login removed here stops being admitted.
type PublishPrincipalsRequest struct {
	Step
	Revision   string   `json:"revision"`
	Principals []string `json:"principals"`
}

// RevokeTenantRequest closes a tenant's admission gate. Revocation is not
// additive publication: it takes effect for new work immediately.
type RevokeTenantRequest struct {
	Step
	Reason string `json:"reason"`
}
