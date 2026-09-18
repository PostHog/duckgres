package trinogateway

// Wire types for the Gateway pooled-member protocol v1. The JSON tags are the
// cross-repo contract: a typo here fails closed only at runtime, so they are
// asserted field by field in the client tests.

// Checks duckgres performs before asking the Gateway to activate a member. The
// Gateway records them verbatim and does NOT claim to have performed them.
//
// There is deliberately no auth-revision check: the coordinator acknowledges
// CATALOG revisions only, and nothing in Trino exposes an application-loaded
// version for the password/group providers or OPA. Claiming one here would put
// an unverifiable assertion into the Gateway's record.
const (
	CheckImage                 = "image"
	CheckWorkers               = "workers"
	CheckCatalogRevision       = "catalog-revision"
	CheckOperationalConnection = "operational-connection"
)

// Step is the idempotency envelope every mutation carries.
type Step struct {
	OperationID     string `json:"operationId"`
	StepID          string `json:"stepId"`
	ControllerEpoch int64  `json:"controllerEpoch"`
}

// Pool is the Gateway's view of a pool.
type Pool struct {
	ProtocolVersion        int            `json:"protocolVersion"`
	PoolID                 string         `json:"poolId"`
	APIMode                string         `json:"apiMode"`
	ControllerEpoch        int64          `json:"controllerEpoch"`
	MembershipGeneration   int64          `json:"membershipGeneration"`
	MinServing             int            `json:"minServing"`
	MaxSurge               int            `json:"maxSurge"`
	RepairBudget           int            `json:"repairBudget"`
	DesiredRevision        string         `json:"desiredRevision"`
	AdmittedRevision       string         `json:"admittedRevision"`
	TenantAdmissionEnabled bool           `json:"tenantAdmissionEnabled"`
	Counts                 map[string]int `json:"counts"`
	ServingMembers         int            `json:"servingMembers"`
	LiveMembers            int            `json:"liveMembers"`
	SurgeInUse             int            `json:"surgeInUse"`
	RepairInUse            int            `json:"repairInUse"`
	OpenPublications       int            `json:"openPublications"`
	Blocked                []string       `json:"blocked"`
}

// UpdatePoolRequest configures the pool. It never lowers the controller epoch.
type UpdatePoolRequest struct {
	ControllerEpoch        int64  `json:"controllerEpoch"`
	APIMode                string `json:"apiMode"`
	MinServing             int    `json:"minServing"`
	MaxSurge               int    `json:"maxSurge"`
	RepairBudget           int    `json:"repairBudget"`
	DesiredRevision        string `json:"desiredRevision"`
	TenantAdmissionEnabled bool   `json:"tenantAdmissionEnabled"`
}

// Member is the shape every member response shares.
type Member struct {
	ProtocolVersion      int    `json:"protocolVersion"`
	PoolID               string `json:"poolId"`
	InstanceID           string `json:"instanceId"`
	Incarnation          string `json:"incarnation"`
	BackendName          string `json:"backendName"`
	Phase                string `json:"phase"`
	Generation           int64  `json:"generation"`
	ControllerEpoch      int64  `json:"controllerEpoch"`
	PodUID               string `json:"podUid"`
	BootID               string `json:"bootId"`
	NodeID               string `json:"nodeId"`
	CoordinatorID        string `json:"coordinatorId"`
	ConfigRevision       string `json:"configRevision"`
	CertifiedRevision    string `json:"certifiedRevision"`
	PendingRequests      int    `json:"pendingRequests"`
	OpenTransactions     int    `json:"openTransactions"`
	ActiveQueries        int    `json:"activeQueries"`
	ReadyToSeal          bool   `json:"readyToSeal"`
	Eligible             bool   `json:"eligible"`
	RetirementKind       string `json:"retirementKind"`
	MembershipGeneration int64  `json:"membershipGeneration"`
	// Replayed marks a recorded result returned for an identical step rather
	// than a fresh mutation.
	Replayed bool `json:"replayed"`
}

// Obligations reports what still pins a member. Drain completion is read from
// here; it is never inferred from a timer.
func (m Member) Obligations() int {
	return m.PendingRequests + m.OpenTransactions + m.ActiveQueries
}

// RegisterMemberRequest creates a PREPARING member. Registration never creates
// an eligible ACTIVE member.
type RegisterMemberRequest struct {
	Step
	InstanceID     string `json:"instanceId"`
	BackendName    string `json:"backendName"`
	URL            string `json:"url"`
	ExternalURL    string `json:"externalUrl,omitempty"`
	PodUID         string `json:"podUid"`
	BootID         string `json:"bootId"`
	ConfigRevision string `json:"configRevision"`
}

// MemberStepRequest is the envelope for activate/drain/seal/retire/retired.
type MemberStepRequest struct {
	Step
	ExpectedGeneration int64 `json:"expectedGeneration"`
	// RepairFor charges the activation to the repair budget instead of the
	// single planned surge.
	RepairFor string `json:"repairFor,omitempty"`
	// ResourcesAbsent is the operator's assertion on `retired`. The Gateway
	// records it and never infers resource deletion for itself.
	ResourcesAbsent bool   `json:"resourcesAbsent,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// CertificateRequest carries a duckgres-performed validation receipt bound to
// the exact process identity it was observed against. A restart or a config
// change invalidates it.
type CertificateRequest struct {
	Step
	ExpectedGeneration int64    `json:"expectedGeneration"`
	ConfigRevision     string   `json:"configRevision"`
	PodUID             string   `json:"podUid"`
	BootID             string   `json:"bootId"`
	NodeID             string   `json:"nodeId"`
	CoordinatorID      string   `json:"coordinatorId"`
	ReadyWorkers       int      `json:"readyWorkers"`
	Checks             []string `json:"checks"`
	CertificateHash    string   `json:"certificateHash"`
}

// LostRequest claims a member's process terminated. Evidence is mandatory: a
// probe timeout is not death, and a partitioned but possibly live process needs
// an explicit destructive authorization instead.
type LostRequest struct {
	Step
	ExpectedGeneration       int64            `json:"expectedGeneration"`
	Evidence                 string           `json:"evidence"`
	DestructiveAuthorization bool             `json:"destructiveAuthorization,omitempty"`
	Termination              TerminationProof `json:"termination"`
}

// TerminationProof binds the loss claim to one exact incarnation.
type TerminationProof struct {
	PodUID        string `json:"podUid"`
	BootID        string `json:"bootId"`
	NodeID        string `json:"nodeId"`
	CoordinatorID string `json:"coordinatorId"`
	Source        string `json:"source"`
	ObservedAt    string `json:"observedAt"`
}

// Evidence values for a loss claim.
const (
	EvidenceProcessTerminated   = "PROCESS_TERMINATED"
	EvidenceDestructiveOverride = "DESTRUCTIVE_OVERRIDE"
)

// Operation is the recorded step history, used to resolve a lost response.
type Operation struct {
	ProtocolVersion int             `json:"protocolVersion"`
	OperationID     string          `json:"operationId"`
	Steps           []OperationStep `json:"steps"`
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

// Step returns the recorded outcome of one step, if it was reached.
func (o Operation) Step(stepID string) (OperationStep, bool) {
	for _, step := range o.Steps {
		if step.StepID == stepID {
			return step, true
		}
	}
	return OperationStep{}, false
}

// OpenPublicationRequest opens the publication barrier for one tenant.
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
	AuthFingerprint string `json:"authFingerprint,omitempty"`
}

// CommitPublicationRequest closes the barrier and opens the tenant gate.
type CommitPublicationRequest struct {
	Step
	ExpectedMembershipGeneration int64 `json:"expectedMembershipGeneration"`
}

// Publication is the Gateway's view of a barrier.
type Publication struct {
	ProtocolVersion      int      `json:"protocolVersion"`
	PublicationID        string   `json:"publicationId"`
	PoolID               string   `json:"poolId"`
	Tenant               string   `json:"tenant"`
	TargetRevision       string   `json:"targetRevision"`
	Phase                string   `json:"phase"`
	MembershipGeneration int64    `json:"membershipGeneration"`
	RequiredMembers      []string `json:"requiredMembers"`
	ReceivedReceipts     []string `json:"receivedReceipts"`
	Replayed             bool     `json:"replayed"`
}
