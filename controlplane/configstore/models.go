package configstore

import "time"

// Org represents a tenant with per-org resource limits.
//
// HostnameAlias decouples the SNI hostname prefix from database_name so an org
// can be reachable at <alias>.<managed-suffix> while clients still connect
// with dbname=<database_name>. *string + sparse-unique index: NULL means "no
// alias", multiple orgs can share the NULL state, but any non-NULL alias must
// be unique across orgs (Postgres ignores NULL in UNIQUE).
type Org struct {
	Name          string  `gorm:"primaryKey;size:255" json:"name"`
	DatabaseName  string  `gorm:"size:255;uniqueIndex" json:"database_name"`
	HostnameAlias *string `gorm:"size:255;uniqueIndex" json:"hostname_alias"`
	MaxWorkers    int     `gorm:"default:0" json:"max_workers"`
	MaxVCPUs      int     `gorm:"column:max_vcpus;default:0" json:"max_vcpus"`
	// DefaultWorkerCPU/Memory/TTL are the org's operator-set default worker
	// profile: the pod shape (k8s resource quantities, e.g. "2"/"8Gi") and
	// hot-idle TTL (Go duration string, e.g. "75m" — stored as a string for
	// human editability) applied to connections that don't size themselves via
	// the duckgres.worker_* startup options. Empty = unset. Versioned SQL
	// migrations add these columns.
	DefaultWorkerCPU        string `gorm:"size:32" json:"default_worker_cpu"`
	DefaultWorkerMemory     string `gorm:"size:32" json:"default_worker_memory"`
	DefaultWorkerTTL        string `gorm:"size:32" json:"default_worker_ttl"`
	DefaultWorkerMinHotIdle int    `gorm:"default:0" json:"default_worker_min_hot_idle"`
	// DefaultTeamID links the org to its default PostHog team (a team id, kept
	// as a string). It is a prerequisite for pull-based compute billing —
	// usage buckets are keyed by team_id = the org's default team. NULLABLE /
	// optional everywhere: NULL means "unset" (existing orgs are backfilled
	// separately, a follow-up makes it required). *string so the column is a
	// nullable VARCHAR; callers must tolerate an empty/absent value.
	DefaultTeamID *string           `gorm:"size:255" json:"default_team_id,omitempty"`
	Users         []OrgUser         `gorm:"foreignKey:OrgID;references:Name" json:"users,omitempty"`
	Warehouse     *ManagedWarehouse `gorm:"foreignKey:OrgID;references:Name;constraint:OnDelete:CASCADE" json:"warehouse,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

func (Org) TableName() string { return "duckgres_orgs" }

// OrgUser maps a username to an org with credentials.
//
// Passthrough flips a per-user flag that bypasses the PostgreSQL compatibility
// layer (SQL transpiler + pg_catalog initialization) and forwards SQL straight
// to DuckDB. Used by clients that already speak DuckDB SQL natively. Scoped to
// (org_id, username) so the same login name can be passthrough in one tenant
// and not in another.
type OrgUser struct {
	OrgID       string `gorm:"primaryKey;size:255" json:"org_id"`
	Username    string `gorm:"primaryKey;size:255" json:"username"`
	Password    string `gorm:"size:255;not null" json:"-"`
	Passthrough bool   `gorm:"not null;default:false" json:"passthrough"`
	// Disabled is the per-user kill switch: when true the user is refused at
	// connect time (PG wire + Flight SQL). Toggling it on also tears down the
	// user's live sessions (see admin disable endpoint).
	Disabled  bool      `gorm:"not null;default:false" json:"disabled"`
	MaxVCPUs  int       `gorm:"column:max_vcpus;default:0" json:"max_vcpus"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (OrgUser) TableName() string { return "duckgres_org_users" }

// Operator is one admin-console operator and the role they resolve to. Rows
// are authoritative access-control data (losing them locks every operator out),
// not rebuildable runtime state — so they live in the goose-migrated config
// schema (see migration 000006_create_operators.sql), alongside the other
// duckgres_-prefixed config tables, and are managed via the admin API's
// Admin → Operators section. AuthMiddleware resolves each SSO request's role
// from this table per-request (see admin.RoleResolver); the break-glass
// internal-secret path is independent and always grants admin.
type Operator struct {
	Email     string    `gorm:"primaryKey;size:255" json:"email"`
	Role      string    `gorm:"size:16;not null" json:"role"` // "admin" | "viewer"
	AddedBy   string    `gorm:"size:255" json:"added_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (Operator) TableName() string { return "duckgres_operators" }

// OrgUserSecret is one customer-set persistent DuckDB secret, scoped to
// (org, user) and replayed onto the user's worker at session creation. The
// row stores the AES-GCM-sealed CREATE SECRET statement (see
// server/usersecrets); the config store never sees plaintext credential
// material. Rows are written/deleted inline when the control plane intercepts
// CREATE/DROP PERSISTENT SECRET and read directly (not via the snapshot
// poller) at session creation, so a secret set through one CP replica is
// immediately visible to all replicas.
type OrgUserSecret struct {
	OrgID      string    `gorm:"primaryKey;size:255" json:"org_id"`
	Username   string    `gorm:"primaryKey;size:255" json:"username"`
	SecretName string    `gorm:"primaryKey;size:255" json:"secret_name"`
	Ciphertext []byte    `gorm:"not null" json:"-"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (OrgUserSecret) TableName() string { return "duckgres_org_user_secrets" }

// ManagedWarehouseProvisioningState is an open string used for warehouse lifecycle status.
// The constants below are the canonical values used by current tooling, but callers may
// persist other states while provisioning workflows evolve.
type ManagedWarehouseProvisioningState string

const (
	ManagedWarehouseStatePending      ManagedWarehouseProvisioningState = "pending"
	ManagedWarehouseStateProvisioning ManagedWarehouseProvisioningState = "provisioning"
	ManagedWarehouseStateReady        ManagedWarehouseProvisioningState = "ready"
	ManagedWarehouseStateFailed       ManagedWarehouseProvisioningState = "failed"
	ManagedWarehouseStateDeleting     ManagedWarehouseProvisioningState = "deleting"
	ManagedWarehouseStateDeleted      ManagedWarehouseProvisioningState = "deleted"
)

// SecretRef identifies a secret key without storing secret material in the config store.
type SecretRef struct {
	Namespace string `gorm:"size:255" json:"namespace"`
	Name      string `gorm:"size:255" json:"name"`
	Key       string `gorm:"size:255" json:"key"`
}

// ManagedWarehouseDatabase stores primary warehouse DB metadata for an org.
// Only Endpoint/Port are consumed (the curated provision connection response);
// the warehouse DB name/user/region are not part of any provisioning or worker
// path and were dropped.
type ManagedWarehouseDatabase struct {
	Endpoint string `gorm:"size:512" json:"endpoint"`
	Port     int    `json:"port"`
}

// Metadata-store kinds, stored verbatim in ManagedWarehouseMetadataStore.Kind
// and mirrored onto the Duckling CR's spec.metadataStore.type. The control
// plane provisions two of these:
//
//   - "cnpg-shard": the per-tenant Postgres backend on the shared
//     CloudNativePG shard.
//   - "external": a pre-existing Postgres (e.g. RDS/Aurora), referenced by
//     endpoint + an AWS Secrets Manager secret for the password. Backs a
//     DuckLake catalog.
const (
	MetadataStoreKindCnpgShard = "cnpg-shard"
	MetadataStoreKindExternal  = "external"
)

// ManagedWarehouseMetadataStore stores org-scoped DuckLake metadata DB info.
type ManagedWarehouseMetadataStore struct {
	Kind         string `gorm:"size:64" json:"kind"`
	Endpoint     string `gorm:"size:512" json:"endpoint"`
	Port         int    `json:"port"`
	DatabaseName string `gorm:"size:255" json:"database_name"`
	Username     string `gorm:"size:255" json:"username"`

	// PasswordAWSSecret is the AWS Secrets Manager secret NAME that holds the
	// metadata DB password. Only meaningful when Kind == "external": it's
	// passed through to the Duckling CR's spec.metadataStore.external.
	// passwordAwsSecret, where the composition resolves it (via ESO) into the
	// status password the worker activator reads. Empty for cnpg-shard
	// (which mints its own credentials).
	PasswordAWSSecret string `gorm:"size:255" json:"password_aws_secret,omitempty"`
}

// ManagedWarehouseDataStore captures the org's object-store provisioning
// intent — the shape the Duckling CR's spec.dataStore takes. Distinct from
// ManagedWarehouseS3 (the resolved, activation-time object-store config):
// this records what to ask the composition for.
//
//   - Kind "s3bucket" (default): the composition provisions a fresh per-org
//     bucket. BucketName/Region are ignored.
//   - Kind "external": reuse an existing bucket (BucketName required); the
//     composition provisions no bucket.
type ManagedWarehouseDataStore struct {
	Kind       string `gorm:"size:32" json:"kind"`
	BucketName string `gorm:"size:255" json:"bucket_name,omitempty"`
	Region     string `gorm:"size:64" json:"region,omitempty"`
}

// ManagedWarehousePgBouncer captures per-org opt-in state for the per-Duckling
// PgBouncer pooler provisioned by the Crossplane composition. When Enabled is
// true, the provisioner controller sets spec.metadataStore.pgbouncer.enabled
// on the Duckling CR at creation time; worker DSN routing through the pooler
// is driven by status.metadataStore.pgbouncerEndpoint (populated by the
// composition once the pooler Service is up).
type ManagedWarehousePgBouncer struct {
	Enabled bool `gorm:"default:false" json:"enabled"`
}

// ManagedWarehouseS3 stores object-store metadata for an org's warehouse.
type ManagedWarehouseS3 struct {
	Provider            string `gorm:"size:64" json:"provider"`
	Region              string `gorm:"size:64" json:"region"`
	Bucket              string `gorm:"size:255" json:"bucket"`
	PathPrefix          string `gorm:"size:1024" json:"path_prefix"`
	Endpoint            string `gorm:"size:512" json:"endpoint"`
	UseSSL              bool   `gorm:"default:false" json:"use_ssl"`
	URLStyle            string `gorm:"size:16" json:"url_style"`
	DeltaCatalogEnabled bool   `gorm:"default:true" json:"delta_catalog_enabled"`
	DeltaCatalogPath    string `gorm:"size:1024" json:"delta_catalog_path"`
}

// ManagedWarehouseWorkerIdentity stores org-scoped worker identity metadata.
// The Namespace + IAM role ARN are consumed at activation; the worker
// ServiceAccount name is computed (not read from config), so it was dropped.
type ManagedWarehouseWorkerIdentity struct {
	Namespace  string `gorm:"size:255" json:"namespace"`
	IAMRoleARN string `gorm:"size:512" json:"iam_role_arn"`
}

// ManagedWarehouseDuckLake captures whether the org's DuckLake catalog is
// enabled. Decoupled from the metadata-store type: a duckling may run
// DuckLake on any metadata backend (cnpg / external). The DuckLake catalog
// lives in the metadata Postgres — the per-tenant database for cnpg-shard,
// or the metadata database for external.
//
// For ducklings created before this field existed the column is absent/false;
// the worker activator does NOT key off it directly — it reads the Duckling
// CR's spec.ducklake.enabled (present/absent) so legacy ducklings keep their
// implied behavior (external ⇒ DuckLake, cnpg ⇒ none).
type ManagedWarehouseDuckLake struct {
	Enabled bool `gorm:"default:false" json:"enabled"`
}

// ManagedWarehouse is the config-store source of truth for an org's managed warehouse metadata.
type ManagedWarehouse struct {
	OrgID string `gorm:"primaryKey;size:255" json:"org_id"`

	Image           string `gorm:"size:512" json:"image"`
	DuckLakeVersion string `gorm:"size:32" json:"ducklake_version"`

	// DucklingName is THE authoritative k8s Duckling CR name — nothing in the
	// control plane derives or re-derives it. On warehouse create it defaults
	// to the org ID verbatim (org IDs are validated as lowercase DNS-1123
	// labels at the provisioning endpoint).
	DucklingName string `gorm:"size:255;not null" json:"duckling_name"`

	WarehouseDatabase ManagedWarehouseDatabase       `gorm:"embedded;embeddedPrefix:warehouse_database_" json:"warehouse_database"`
	MetadataStore     ManagedWarehouseMetadataStore  `gorm:"embedded;embeddedPrefix:metadata_store_" json:"metadata_store"`
	DataStore         ManagedWarehouseDataStore      `gorm:"embedded;embeddedPrefix:data_store_" json:"data_store"`
	PgBouncer         ManagedWarehousePgBouncer      `gorm:"embedded;embeddedPrefix:pgbouncer_" json:"pgbouncer"`
	S3                ManagedWarehouseS3             `gorm:"embedded;embeddedPrefix:s3_" json:"s3"`
	DuckLake          ManagedWarehouseDuckLake       `gorm:"embedded;embeddedPrefix:ducklake_" json:"ducklake"`
	WorkerIdentity    ManagedWarehouseWorkerIdentity `gorm:"embedded;embeddedPrefix:worker_identity_" json:"worker_identity"`

	WarehouseDatabaseCredentials SecretRef `gorm:"embedded;embeddedPrefix:warehouse_database_credentials_" json:"warehouse_database_credentials"`
	MetadataStoreCredentials     SecretRef `gorm:"embedded;embeddedPrefix:metadata_store_credentials_" json:"metadata_store_credentials"`
	S3Credentials                SecretRef `gorm:"embedded;embeddedPrefix:s3_credentials_" json:"s3_credentials"`
	RuntimeConfig                SecretRef `gorm:"embedded;embeddedPrefix:runtime_config_" json:"runtime_config"`

	// Top-level State/StatusMessage are the rolled-up provisioning status; the
	// per-component *State fields below drive readiness. The provisioner only
	// ever writes the top-level status_message, so per-component status-message
	// columns were dropped. warehouse_database has no provisioning sub-state, so
	// its *State was dropped too.
	State              ManagedWarehouseProvisioningState `gorm:"size:32" json:"state"`
	StatusMessage      string                            `gorm:"size:1024" json:"status_message"`
	MetadataStoreState ManagedWarehouseProvisioningState `gorm:"size:32" json:"metadata_store_state"`
	S3State            ManagedWarehouseProvisioningState `gorm:"size:32" json:"s3_state"`
	IdentityState      ManagedWarehouseProvisioningState `gorm:"size:32" json:"identity_state"`
	SecretsState       ManagedWarehouseProvisioningState `gorm:"size:32" json:"secrets_state"`

	ProvisioningStartedAt *time.Time `json:"provisioning_started_at"`
	ReadyAt               *time.Time `json:"ready_at"`
	FailedAt              *time.Time `json:"failed_at"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

func (ManagedWarehouse) TableName() string { return "duckgres_managed_warehouses" }

// NOTE: the cluster-wide singleton config tables (global_config,
// ducklake_config, rate_limit_config, query_log_config) were removed — they
// were seeded and served by the admin API but never read to drive runtime
// behavior. Effective config comes from CLI flags/env (server.Config) and the
// per-org ManagedWarehouse contract. See migration 000004.

// ControlPlaneInstanceState describes the liveness state of a control-plane instance.
type ControlPlaneInstanceState string

const (
	ControlPlaneInstanceStateActive   ControlPlaneInstanceState = "active"
	ControlPlaneInstanceStateDraining ControlPlaneInstanceState = "draining"
	ControlPlaneInstanceStateExpired  ControlPlaneInstanceState = "expired"
)

// ControlPlaneInstance is a runtime coordination record for one control-plane process.
// These rows live in the runtime schema, not the snapshot-backed config tables.
type ControlPlaneInstance struct {
	ID      string `gorm:"primaryKey;size:255" json:"id"`
	PodName string `gorm:"size:255;not null" json:"pod_name"`
	// pod_uid + boot_id were dropped: both are already encoded into the
	// primary-key ID (<pod_uid>-<bootIDHex>) and were never read back.
	State           ControlPlaneInstanceState `gorm:"size:32;not null" json:"state"`
	StartedAt       time.Time                 `json:"started_at"`
	LastHeartbeatAt time.Time                 `gorm:"index" json:"last_heartbeat_at"`
	DrainingAt      *time.Time                `json:"draining_at,omitempty"`
	ExpiredAt       *time.Time                `json:"expired_at,omitempty"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

func (ControlPlaneInstance) TableName() string { return "cp_instances" }

// WorkerState is the durable lifecycle state for a worker pod.
type WorkerState string

const (
	WorkerStateSpawning   WorkerState = "spawning"
	WorkerStateIdle       WorkerState = "idle"
	WorkerStateReserved   WorkerState = "reserved"
	WorkerStateActivating WorkerState = "activating"
	WorkerStateHot        WorkerState = "hot"
	WorkerStateHotIdle    WorkerState = "hot_idle"
	WorkerStateDraining   WorkerState = "draining"
	WorkerStateRetired    WorkerState = "retired"
	WorkerStateLost       WorkerState = "lost"
)

// WorkerClaimMissReason classifies why a runtime-store worker claim did not
// return a worker. The empty reason is reserved for successful claims.
type WorkerClaimMissReason string

const (
	WorkerClaimMissReasonNone         WorkerClaimMissReason = ""
	WorkerClaimMissReasonNoIdle       WorkerClaimMissReason = "no_idle"
	WorkerClaimMissReasonOrgCap       WorkerClaimMissReason = "org_cap"
	WorkerClaimMissReasonShuttingDown WorkerClaimMissReason = "shutting_down"
)

// WorkerLifecycleStats is the grouped worker lifecycle state used for
// cluster-wide worker observability.
type WorkerLifecycleStats struct {
	Image       string      `json:"image"`
	State       WorkerState `json:"state"`
	Binding     string      `json:"binding"`
	Count       int64       `json:"count"`
	CPUCores    float64     `json:"cpu_cores"`
	MemoryBytes int64       `json:"memory_bytes"`
}

// WorkerRecord is the durable runtime coordination record for one worker pod.
type WorkerRecord struct {
	WorkerID int    `gorm:"primaryKey" json:"worker_id"`
	PodName  string `gorm:"size:255;not null;uniqueIndex" json:"pod_name"`
	Image    string `gorm:"size:512;index" json:"image"`
	// Worker pod-shape profile (connection-string-selected sizing). Empty
	// CPU/Memory is the default profile, so legacy rows read back as the default
	// and stay claimable by default requests. Matched alongside Image when a
	// session reserves a worker. AutoMigrate adds these columns; no migration file.
	ProfileCPU    string `gorm:"size:32;index:idx_worker_profile" json:"profile_cpu"`
	ProfileMemory string `gorm:"size:32;index:idx_worker_profile" json:"profile_memory"`
	// TTLMinutes is how long this worker stays hot-idle after its last query
	// before the janitor retires it (client-selected duckgres.worker_ttl, rounded
	// down to whole minutes). 0 = use the deployment's global hot-idle TTL
	// (default/legacy workers and legacy rows). AutoMigrate adds this
	// column; no migration file.
	TTLMinutes          int         `gorm:"default:0" json:"ttl_minutes"`
	State               WorkerState `gorm:"size:32;not null;index" json:"state"`
	OrgID               string      `gorm:"size:255;index" json:"org_id"`
	OwnerCPInstanceID   string      `gorm:"size:255;index" json:"owner_cp_instance_id"`
	OwnerEpoch          int64       `gorm:"not null" json:"owner_epoch"`
	ActivationStartedAt *time.Time  `json:"activation_started_at,omitempty"`
	LastHeartbeatAt     time.Time   `json:"last_heartbeat_at"`
	RetireReason        string      `gorm:"size:64" json:"retire_reason"`
	// S3CredentialsExpiresAt is when the most recent STS-brokered S3
	// credentials currently active in the worker's DuckDB ducklake_s3 secret
	// will expire. Stamped when the control plane mints creds (initial
	// activation, takeover, scheduled refresh) and consulted by the
	// credential refresh scheduler to pick workers nearing expiry. NULL on
	// workers that haven't had creds issued yet and on legacy
	// rows from before this column existed — both are treated as "due now"
	// by the scheduler so they get refreshed eagerly.
	S3CredentialsExpiresAt *time.Time `gorm:"index" json:"s3_credentials_expires_at,omitempty"`
	// HotIdleSince is when the worker most recently entered the hot_idle state.
	// The hot-idle TTL reaper measures idle age from this column instead of
	// updated_at, because updated_at is legitimately bumped by lease and
	// credential-refresh writes (BumpWorkerEpoch, MarkCredentialsRefreshed) that
	// do not change a worker's idleness — keying the reap clock off it let a
	// periodically-refreshed hot-idle worker reset its own TTL forever and never
	// get retired. Stamped only on the transition into hot_idle and preserved
	// across same-state upserts/refreshes. NULL on non-hot-idle rows and on
	// legacy rows predating this column; the reaper falls back to updated_at when
	// NULL so legacy hot-idle rows still expire. AutoMigrate adds this column; no
	// migration file.
	HotIdleSince *time.Time `gorm:"index" json:"hot_idle_since,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

func (WorkerRecord) TableName() string { return "worker_records" }

// FlightSessionState is the durable reconnect state for Flight-only sessions.
type FlightSessionState string

const (
	FlightSessionStateActive       FlightSessionState = "active"
	FlightSessionStateReconnecting FlightSessionState = "reconnecting"
	FlightSessionStateExpired      FlightSessionState = "expired"
	FlightSessionStateClosed       FlightSessionState = "closed"
)

// FlightSessionRecord is the durable reconnect record for Flight sessions.
type FlightSessionRecord struct {
	SessionToken string             `gorm:"primaryKey;size:255" json:"session_token"`
	Username     string             `gorm:"size:255;not null" json:"username"`
	OrgID        string             `gorm:"size:255;not null" json:"org_id"`
	WorkerID     int                `gorm:"not null;index" json:"worker_id"`
	PID          int32              `gorm:"column:p_id;not null;default:0" json:"pid"`
	OwnerEpoch   int64              `gorm:"not null" json:"owner_epoch"`
	CPInstanceID string             `gorm:"size:255" json:"cp_instance_id"`
	State        FlightSessionState `gorm:"size:32;not null" json:"state"`
	ExpiresAt    time.Time          `gorm:"index" json:"expires_at"`
	LastSeenAt   time.Time          `json:"last_seen_at"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

func (FlightSessionRecord) TableName() string { return "flight_session_records" }

// OrgResourceLimits is the current resource-admission ceiling for an org and
// the connecting user. 0 means unlimited for either dimension.
type OrgResourceLimits struct {
	OrgMaxVCPUs  int
	UserMaxVCPUs int
}

// OrgConnectionQueueEntry is a cluster-wide FIFO admission request for one org
// connection. Rows expire quickly; they coordinate fairness across CP replicas.
type OrgConnectionQueueEntry struct {
	RequestID      string     `gorm:"primaryKey;size:64" json:"request_id"`
	OrgID          string     `gorm:"size:255;not null;index:idx_org_connection_queue_pending,priority:1" json:"org_id"`
	Username       string     `gorm:"size:255;index" json:"username"`
	CPInstanceID   string     `gorm:"size:255;not null;index" json:"cp_instance_id"`
	PID            int32      `gorm:"not null" json:"pid"`
	Protocol       string     `gorm:"size:32;not null" json:"protocol"`
	RequestedVCPUs int        `gorm:"column:requested_vcpus;not null;default:1" json:"requested_vcpus"`
	EnqueuedAt     time.Time  `gorm:"not null;index:idx_org_connection_queue_pending,priority:2" json:"enqueued_at"`
	ExpiresAt      time.Time  `gorm:"not null;index" json:"expires_at"`
	GrantedAt      *time.Time `gorm:"index" json:"granted_at,omitempty"`
	// canceled_at was dropped: cancellation is a hard DELETE of the row, so the
	// column was never set to a non-NULL value.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (OrgConnectionQueueEntry) TableName() string { return "org_connection_queue" }

// OrgConnectionLease is the durable cluster-wide admission lease for a live
// session. Capacity checks count active leases, ignoring owners whose CP row
// has expired.
type OrgConnectionLease struct {
	LeaseID        string    `gorm:"primaryKey;size:64" json:"lease_id"`
	RequestID      string    `gorm:"size:64;not null;uniqueIndex" json:"request_id"`
	OrgID          string    `gorm:"size:255;not null;index" json:"org_id"`
	Username       string    `gorm:"size:255;index" json:"username"`
	CPInstanceID   string    `gorm:"size:255;not null;index" json:"cp_instance_id"`
	PID            int32     `gorm:"not null" json:"pid"`
	Protocol       string    `gorm:"size:32;not null" json:"protocol"`
	RequestedVCPUs int       `gorm:"column:requested_vcpus;not null;default:1" json:"requested_vcpus"`
	AcquiredAt     time.Time `gorm:"not null" json:"acquired_at"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (OrgConnectionLease) TableName() string { return "org_connection_leases" }

// OrgConfig is a convenience view combining org metadata with resource limits.
//
// HostnameAlias is a plain string here (empty == "no alias") because snapshot
// consumers uniformly expect non-pointer types. The Org model keeps it as
// *string to drive sparse-unique semantics in the underlying table; that
// pointer-ness is irrelevant once the data is loaded into the snapshot.
type OrgConfig struct {
	Name                    string
	DatabaseName            string
	HostnameAlias           string // empty when no alias is configured
	MaxWorkers              int
	MaxVCPUs                int
	DefaultWorkerCPU        string            // org default worker profile: pod cpu quantity ("" = unset)
	DefaultWorkerMemory     string            // org default worker profile: pod memory quantity ("" = unset)
	DefaultWorkerTTL        string            // org default worker profile: hot-idle TTL, Go duration string ("" = unset)
	DefaultWorkerMinHotIdle int               // minimum default-profile hot-idle workers to retain for this org
	DefaultTeamID           string            // org's default PostHog team id ("" = unset); prereq for pull-based compute billing
	Users                   map[string]string // username -> password
	Warehouse               *ManagedWarehouseConfig
}

// ManagedWarehouseConfig is the in-memory snapshot view of an org's warehouse metadata.
type ManagedWarehouseConfig struct {
	OrgID string

	Image           string
	DuckLakeVersion string

	WarehouseDatabase ManagedWarehouseDatabase
	MetadataStore     ManagedWarehouseMetadataStore
	PgBouncer         ManagedWarehousePgBouncer
	S3                ManagedWarehouseS3
	WorkerIdentity    ManagedWarehouseWorkerIdentity

	WarehouseDatabaseCredentials SecretRef
	MetadataStoreCredentials     SecretRef
	S3Credentials                SecretRef
	RuntimeConfig                SecretRef

	State              ManagedWarehouseProvisioningState
	StatusMessage      string
	MetadataStoreState ManagedWarehouseProvisioningState
	S3State            ManagedWarehouseProvisioningState
	IdentityState      ManagedWarehouseProvisioningState
	SecretsState       ManagedWarehouseProvisioningState
	ReadyAt            *time.Time
	FailedAt           *time.Time
}

func copyManagedWarehouseConfig(warehouse *ManagedWarehouse) *ManagedWarehouseConfig {
	if warehouse == nil {
		return nil
	}

	cfg := &ManagedWarehouseConfig{
		OrgID:                        warehouse.OrgID,
		Image:                        warehouse.Image,
		DuckLakeVersion:              warehouse.DuckLakeVersion,
		WarehouseDatabase:            warehouse.WarehouseDatabase,
		MetadataStore:                warehouse.MetadataStore,
		PgBouncer:                    warehouse.PgBouncer,
		S3:                           warehouse.S3,
		WorkerIdentity:               warehouse.WorkerIdentity,
		WarehouseDatabaseCredentials: warehouse.WarehouseDatabaseCredentials,
		MetadataStoreCredentials:     warehouse.MetadataStoreCredentials,
		S3Credentials:                warehouse.S3Credentials,
		RuntimeConfig:                warehouse.RuntimeConfig,
		State:                        warehouse.State,
		StatusMessage:                warehouse.StatusMessage,
		MetadataStoreState:           warehouse.MetadataStoreState,
		S3State:                      warehouse.S3State,
		IdentityState:                warehouse.IdentityState,
		SecretsState:                 warehouse.SecretsState,
	}
	if warehouse.ReadyAt != nil {
		readyAt := *warehouse.ReadyAt
		cfg.ReadyAt = &readyAt
	}
	if warehouse.FailedAt != nil {
		failedAt := *warehouse.FailedAt
		cfg.FailedAt = &failedAt
	}
	return cfg
}
