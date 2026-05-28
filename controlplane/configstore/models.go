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
	Name                string                 `gorm:"primaryKey;size:255" json:"name"`
	DatabaseName        string                 `gorm:"size:255;uniqueIndex" json:"database_name"`
	HostnameAlias       *string                `gorm:"size:255;uniqueIndex" json:"hostname_alias"`
	MaxWorkers          int                    `gorm:"default:0" json:"max_workers"`
	MaxConnections      int                    `gorm:"default:0" json:"max_connections"`
	MemoryBudget        string                 `gorm:"size:32" json:"memory_budget"`
	IdleTimeoutS        int                    `gorm:"default:0" json:"idle_timeout_s"`
	WorkerCPURequest    string                 `gorm:"size:32" json:"worker_cpu_request"`
	WorkerMemoryRequest string                 `gorm:"size:32" json:"worker_memory_request"`
	Users               []OrgUser              `gorm:"foreignKey:OrgID;references:Name" json:"users,omitempty"`
	Warehouse           *ManagedWarehouse      `gorm:"foreignKey:OrgID;references:Name;constraint:OnDelete:CASCADE" json:"warehouse,omitempty"`
	Trino               *ManagedWarehouseTrino `gorm:"foreignKey:OrgID;references:Name;constraint:OnDelete:CASCADE" json:"trino,omitempty"`
	CreatedAt           time.Time              `json:"created_at"`
	UpdatedAt           time.Time              `json:"updated_at"`
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
	OrgID       string    `gorm:"primaryKey;size:255" json:"org_id"`
	Username    string    `gorm:"primaryKey;size:255" json:"username"`
	Password    string    `gorm:"size:255;not null" json:"-"`
	Passthrough bool      `gorm:"not null;default:false" json:"passthrough"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (OrgUser) TableName() string { return "duckgres_org_users" }

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
type ManagedWarehouseDatabase struct {
	Region       string `gorm:"size:64" json:"region"`
	Endpoint     string `gorm:"size:512" json:"endpoint"`
	Port         int    `json:"port"`
	DatabaseName string `gorm:"size:255" json:"database_name"`
	Username     string `gorm:"size:255" json:"username"`
}

// Metadata-store kinds, stored verbatim in ManagedWarehouseMetadataStore.Kind
// and mirrored onto the Duckling CR's spec.metadataStore.type. The control
// plane provisions two of these:
//
//   - "cnpg-shard": the per-tenant Lakekeeper Iceberg catalog Postgres backend
//     on the shared CloudNativePG shard (always paired with iceberg.enabled).
//   - "external": a pre-existing Postgres (e.g. RDS), referenced by endpoint +
//     an AWS Secrets Manager secret for the password. Backs either a DuckLake
//     catalog (iceberg disabled) or the Lakekeeper catalog (iceberg enabled).
//
// "aurora" is no longer provisionable (the control plane never stands up a new
// Aurora cluster); the constant is retained so DucklingClient.Create can still
// reconcile pre-existing aurora ducklings.
const (
	MetadataStoreKindAurora    = "aurora"
	MetadataStoreKindCnpgShard = "cnpg-shard"
	MetadataStoreKindExternal  = "external"
)

// ManagedWarehouseMetadataStore stores org-scoped DuckLake metadata DB info.
type ManagedWarehouseMetadataStore struct {
	Kind         string `gorm:"size:64" json:"kind"`
	Engine       string `gorm:"size:64" json:"engine"`
	Region       string `gorm:"size:64" json:"region"`
	Endpoint     string `gorm:"size:512" json:"endpoint"`
	Port         int    `json:"port"`
	DatabaseName string `gorm:"size:255" json:"database_name"`
	Username     string `gorm:"size:255" json:"username"`

	// PasswordAWSSecret is the AWS Secrets Manager secret NAME that holds the
	// metadata DB password. Only meaningful when Kind == "external": it's
	// passed through to the Duckling CR's spec.metadataStore.external.
	// passwordAwsSecret, where the composition resolves it (via ESO) into the
	// status password the worker activator reads. Empty for aurora/cnpg-shard
	// (those mint their own credentials).
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
	Enabled bool `json:"enabled"`
}

// ManagedWarehouseS3 stores object-store metadata for an org's warehouse.
type ManagedWarehouseS3 struct {
	Provider            string `gorm:"size:64" json:"provider"`
	Region              string `gorm:"size:64" json:"region"`
	Bucket              string `gorm:"size:255" json:"bucket"`
	PathPrefix          string `gorm:"size:1024" json:"path_prefix"`
	Endpoint            string `gorm:"size:512" json:"endpoint"`
	UseSSL              bool   `json:"use_ssl"`
	URLStyle            string `gorm:"size:16" json:"url_style"`
	DeltaCatalogEnabled bool   `gorm:"default:true" json:"delta_catalog_enabled"`
	DeltaCatalogPath    string `gorm:"size:1024" json:"delta_catalog_path"`
}

// ManagedWarehouseWorkerIdentity stores org-scoped worker identity metadata.
type ManagedWarehouseWorkerIdentity struct {
	Namespace          string `gorm:"size:255" json:"namespace"`
	ServiceAccountName string `gorm:"size:255" json:"service_account_name"`
	IAMRoleARN         string `gorm:"size:512" json:"iam_role_arn"`
}

// ManagedWarehouseDuckLake captures whether the org's DuckLake catalog is
// enabled. Decoupled from the metadata-store type and from Iceberg: a duckling
// may run DuckLake, Iceberg, or both, on any metadata backend (cnpg / external
// / aurora). The DuckLake catalog lives in the metadata Postgres — the
// per-tenant database for cnpg-shard, or the metadata database for
// external/aurora.
//
// For ducklings created before this field existed the column is absent/false;
// the worker activator does NOT key off it directly — it reads the Duckling
// CR's spec.ducklake.enabled (present/absent) so legacy ducklings keep their
// implied behavior (external/aurora ⇒ DuckLake, cnpg ⇒ none).
type ManagedWarehouseDuckLake struct {
	Enabled bool `json:"enabled"`
}

// ManagedWarehouseIceberg captures per-org Iceberg catalog config. Two
// backends are supported, selected by Backend:
//
//   - "lakekeeper" (default): a per-org Lakekeeper instance vends the
//     Iceberg REST catalog. The provisioner creates the Lakekeeper CR + a
//     warehouse pointing at the org's existing S3 bucket (path
//     <s3.path-prefix>/lakekeeper/<orgid>/) and persists the endpoint +
//     OAuth2 client credentials back here. The worker activator reads
//     these and emits a (TYPE ICEBERG, CLIENT_ID/CLIENT_SECRET/
//     OAUTH2_SERVER_URI) DuckDB SECRET + ATTACH at session init.
//
//   - "s3_tables" (legacy): provisioner controller sets spec.iceberg.enabled
//     on the Duckling CR; the composition provisions a fresh S3 Tables
//     bucket and writes TableBucketArn back here. Kept as a safety net /
//     escape hatch — new orgs default to Lakekeeper.
type ManagedWarehouseIceberg struct {
	Enabled bool `json:"enabled"`

	// Backend selects the catalog backend. Empty/unset is treated as
	// "lakekeeper" by callers.
	Backend string `gorm:"size:32;default:'lakekeeper'" json:"backend"`

	// Namespace is the default Iceberg namespace inside the catalog. Used
	// for both backends.
	Namespace string `gorm:"size:255" json:"namespace"`

	// Region applies to both backends (S3 region for S3 Tables; AWS region
	// for the Lakekeeper warehouse storage profile).
	Region string `gorm:"size:64" json:"region"`

	// S3 Tables fields (Backend == "s3_tables"). Empty otherwise.
	TableBucketArn string `gorm:"size:512" json:"table_bucket_arn,omitempty"`

	// Lakekeeper fields (Backend == "lakekeeper"). Populated by the
	// provisioner after the per-org Lakekeeper is ready.
	LakekeeperEndpoint string `gorm:"size:512" json:"lakekeeper_endpoint,omitempty"`

	// LakekeeperWarehouse is the warehouse NAME (e.g. "org-acme"), not the
	// UUID. Iceberg REST clients pass this as the `warehouse` parameter to
	// /v1/config and the server returns the UUID as a prefix for subsequent
	// calls. PR2's worker-side ATTACH SQL uses this value directly.
	LakekeeperWarehouse string `gorm:"size:128" json:"lakekeeper_warehouse,omitempty"`
	LakekeeperClientID  string `gorm:"size:128" json:"lakekeeper_client_id,omitempty"`

	// LakekeeperOAuth2ServerURI is the OAuth2 token endpoint URI for the
	// duckling-side CREATE SECRET. Empty during PR1 (allowall mode);
	// populated by PR3 once OIDC SA-token auth is wired. PR2 worker code
	// must guard against empty and either skip the OAuth2 fields on the
	// CREATE SECRET statement or emit a different secret shape.
	LakekeeperOAuth2ServerURI string `gorm:"size:512" json:"lakekeeper_oauth2_server_uri,omitempty"`

	// LakekeeperClientCredentials holds the OAuth2 client_secret used by
	// the duckling to authenticate to Lakekeeper. The control plane
	// resolves this just before sending the activation payload.
	LakekeeperClientCredentials SecretRef `gorm:"embedded;embeddedPrefix:lakekeeper_client_credentials_" json:"lakekeeper_client_credentials"`
}

// IcebergBackend constants — string-typed to keep the GORM tag happy.
const (
	IcebergBackendLakekeeper = "lakekeeper"
	IcebergBackendS3Tables   = "s3_tables"
)

// ManagedWarehouseTrino captures per-org opt-in for the customer-facing Trino
// cluster. Trino access is granted at the org level: when Enabled is true, the
// provisioner extension (controlplane/provisioner/trino_provisioner.go)
// projects the org's `root` OrgUser bcrypt hash into the Trino file
// password authenticator, creates a per-org Iceberg catalog via the Trino
// REST API, and rebuilds the OPA bundle + resource-groups config.
//
// v1 is intentionally minimal — Enabled gates the projection, Tier picks
// resource-group limits. Per-user identity within an org is post-v1.
//
// FK'd by OrgID like the other sibling rows; consumed by the Trino
// provisioner sub-component in controlplane/provisioner/trino_provisioner.go.
type ManagedWarehouseTrino struct {
	OrgID string `gorm:"primaryKey;size:255" json:"org_id"`

	// Enabled gates inclusion in every projection the Trino provisioner owns
	// (password file, group file, OPA bundle catalog map, resource-groups
	// config, REST CREATE CATALOG). Defaults to false so existing
	// configstore rows never start projecting after a schema migration.
	Enabled bool `gorm:"not null;default:false" json:"enabled"`

	// Tier picks the resource-group limits applied to the org. Empty string
	// is treated as the default tier by the resource-groups generator.
	// Kept as a free-form string for now; refining into an enum is
	// post-v1 work once tier shape stabilizes.
	Tier string `gorm:"size:64" json:"tier"`

	// State / StatusMessage / ReadyAt / FailedAt mirror the
	// ManagedWarehouse lifecycle fields so operators can see what the
	// Trino reconcile loop is doing. The Trino reconcile is a single
	// batched output per tick (catalog + auth + resource-groups +
	// bundle); these fields summarize the most recent tick's outcome.
	//
	// State transitions:
	//   - pending (default after EnableTrino, before first reconcile)
	//   - provisioning (a reconcile tick is mid-flight or a previous
	//     tick partially failed and is retrying)
	//   - ready (the most recent tick succeeded across all four steps)
	//   - failed (the most recent tick errored — StatusMessage carries
	//     the per-step detail; ready_at is preserved, failed_at is set)
	//
	// On the next successful reconcile after a failed state, the row
	// flips back to ready and failed_at is cleared. We don't model the
	// plan's per-step sub-states (CatalogCreating, ProjectionReady,
	// etc.) — for v1 the four-state summary plus StatusMessage detail
	// is sufficient observability without growing the model.
	State         ManagedWarehouseProvisioningState `gorm:"size:32" json:"state"`
	StatusMessage string                            `gorm:"size:1024" json:"status_message"`
	ReadyAt       *time.Time                        `json:"ready_at,omitempty"`
	FailedAt      *time.Time                        `json:"failed_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (ManagedWarehouseTrino) TableName() string { return "duckgres_managed_warehouse_trino" }

// TrinoEnabledOrg is the join shape returned by ListTrinoEnabledOrgs: org +
// root-user bcrypt hash. The provisioner needs both at once — the password
// file projection keys org_<team_id> by the password hash — so a single
// query avoids an N+1 read pattern as the Trino-enabled org count grows.
//
// State is the row's CURRENT operational state at the time of the read,
// so the reconcile loop can decide whether each per-tick write is a
// transition (set ReadyAt / FailedAt) or a no-op preservation
// (matches the surrounding ManagedWarehouse pattern in controller.go,
// which only stamps ready_at on the first transition into ready).
type TrinoEnabledOrg struct {
	OrgID            string
	DatabaseName     string
	Tier             string
	RootPasswordHash string                            // bcrypt hash from OrgUser row where Username = "root"
	State            ManagedWarehouseProvisioningState // current state at read time
}

// ResolvedBackend returns Backend with the empty-string default applied.
// Callers should prefer this over reading Backend directly so that rows
// migrated from earlier schemas (no Backend column) behave correctly.
func (i ManagedWarehouseIceberg) ResolvedBackend() string {
	if i.Backend == "" {
		return IcebergBackendLakekeeper
	}
	return i.Backend
}

// ManagedWarehouse is the config-store source of truth for an org's managed warehouse metadata.
type ManagedWarehouse struct {
	OrgID string `gorm:"primaryKey;size:255" json:"org_id"`

	Image           string  `gorm:"size:512" json:"image"`
	DuckLakeVersion string  `gorm:"size:32" json:"ducklake_version"`
	AuroraMinACU    float64 `json:"aurora_min_acu"`
	AuroraMaxACU    float64 `json:"aurora_max_acu"`

	WarehouseDatabase ManagedWarehouseDatabase       `gorm:"embedded;embeddedPrefix:warehouse_database_" json:"warehouse_database"`
	MetadataStore     ManagedWarehouseMetadataStore  `gorm:"embedded;embeddedPrefix:metadata_store_" json:"metadata_store"`
	DataStore         ManagedWarehouseDataStore      `gorm:"embedded;embeddedPrefix:data_store_" json:"data_store"`
	PgBouncer         ManagedWarehousePgBouncer      `gorm:"embedded;embeddedPrefix:pgbouncer_" json:"pgbouncer"`
	S3                ManagedWarehouseS3             `gorm:"embedded;embeddedPrefix:s3_" json:"s3"`
	DuckLake          ManagedWarehouseDuckLake       `gorm:"embedded;embeddedPrefix:ducklake_" json:"ducklake"`
	Iceberg           ManagedWarehouseIceberg        `gorm:"embedded;embeddedPrefix:iceberg_" json:"iceberg"`
	WorkerIdentity    ManagedWarehouseWorkerIdentity `gorm:"embedded;embeddedPrefix:worker_identity_" json:"worker_identity"`

	WarehouseDatabaseCredentials SecretRef `gorm:"embedded;embeddedPrefix:warehouse_database_credentials_" json:"warehouse_database_credentials"`
	MetadataStoreCredentials     SecretRef `gorm:"embedded;embeddedPrefix:metadata_store_credentials_" json:"metadata_store_credentials"`
	S3Credentials                SecretRef `gorm:"embedded;embeddedPrefix:s3_credentials_" json:"s3_credentials"`
	RuntimeConfig                SecretRef `gorm:"embedded;embeddedPrefix:runtime_config_" json:"runtime_config"`

	State                          ManagedWarehouseProvisioningState `gorm:"size:32" json:"state"`
	StatusMessage                  string                            `gorm:"size:1024" json:"status_message"`
	WarehouseDatabaseState         ManagedWarehouseProvisioningState `gorm:"size:32" json:"warehouse_database_state"`
	WarehouseDatabaseStatusMessage string                            `gorm:"size:1024" json:"warehouse_database_status_message"`
	MetadataStoreState             ManagedWarehouseProvisioningState `gorm:"size:32" json:"metadata_store_state"`
	MetadataStoreStatusMessage     string                            `gorm:"size:1024" json:"metadata_store_status_message"`
	S3State                        ManagedWarehouseProvisioningState `gorm:"size:32" json:"s3_state"`
	S3StatusMessage                string                            `gorm:"size:1024" json:"s3_status_message"`
	IcebergState                   ManagedWarehouseProvisioningState `gorm:"size:32" json:"iceberg_state"`
	IcebergStatusMessage           string                            `gorm:"size:1024" json:"iceberg_status_message"`
	IdentityState                  ManagedWarehouseProvisioningState `gorm:"size:32" json:"identity_state"`
	IdentityStatusMessage          string                            `gorm:"size:1024" json:"identity_status_message"`
	SecretsState                   ManagedWarehouseProvisioningState `gorm:"size:32" json:"secrets_state"`
	SecretsStatusMessage           string                            `gorm:"size:1024" json:"secrets_status_message"`
	ProvisioningStartedAt          *time.Time                        `json:"provisioning_started_at"`
	ReadyAt                        *time.Time                        `json:"ready_at"`
	FailedAt                       *time.Time                        `json:"failed_at"`
	CreatedAt                      time.Time                         `json:"created_at"`
	UpdatedAt                      time.Time                         `json:"updated_at"`
}

func (ManagedWarehouse) TableName() string { return "duckgres_managed_warehouses" }

// GlobalConfig is a singleton row (ID=1) for cluster-wide settings.
type GlobalConfig struct {
	ID                  uint      `gorm:"primaryKey" json:"-"`
	MemoryBudget        string    `gorm:"size:32" json:"memory_budget"`
	MemoryRebalance     bool      `json:"memory_rebalance"`
	MaxConnections      int       `json:"max_connections"`
	IdleTimeoutS        int       `json:"idle_timeout_s"`
	WorkerQueueTimeoutS int       `json:"worker_queue_timeout_s"`
	WorkerIdleTimeoutS  int       `json:"worker_idle_timeout_s"`
	Extensions          string    `gorm:"size:1024" json:"extensions"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func (GlobalConfig) TableName() string { return "duckgres_global_config" }

// DuckLakeConfig is a singleton row (ID=1) for legacy cluster-wide DuckLake settings.
// In multi-tenant mode, the managed-warehouse contract is the intended per-org source of truth.
type DuckLakeConfig struct {
	ID                  uint      `gorm:"primaryKey" json:"-"`
	MetadataStore       string    `gorm:"size:1024" json:"metadata_store"`
	ObjectStore         string    `gorm:"size:1024" json:"object_store"`
	DataPath            string    `gorm:"size:1024" json:"data_path"`
	S3Provider          string    `gorm:"size:64" json:"s3_provider"`
	S3Endpoint          string    `gorm:"size:512" json:"s3_endpoint"`
	S3AccessKey         string    `gorm:"size:255" json:"s3_access_key"`
	S3SecretKey         string    `gorm:"size:255" json:"-"`
	S3Region            string    `gorm:"size:64" json:"s3_region"`
	S3UseSSL            bool      `json:"s3_use_ssl"`
	S3URLStyle          string    `gorm:"size:16" json:"s3_url_style"`
	S3Chain             string    `gorm:"size:255" json:"s3_chain"`
	S3Profile           string    `gorm:"size:255" json:"s3_profile"`
	DeltaCatalogEnabled bool      `gorm:"default:true" json:"delta_catalog_enabled"`
	DeltaCatalogPath    string    `gorm:"size:1024" json:"delta_catalog_path"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func (DuckLakeConfig) TableName() string { return "duckgres_ducklake_config" }

// RateLimitConfig is a singleton row (ID=1) for rate limiting.
type RateLimitConfig struct {
	ID                   uint      `gorm:"primaryKey" json:"-"`
	MaxFailedAttempts    int       `json:"max_failed_attempts"`
	FailedAttemptWindowS int       `json:"failed_attempt_window_s"`
	BanDurationS         int       `json:"ban_duration_s"`
	MaxConnectionsPerIP  int       `json:"max_connections_per_ip"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func (RateLimitConfig) TableName() string { return "duckgres_rate_limit_config" }

// QueryLogConfig is a singleton row (ID=1) for query logging.
type QueryLogConfig struct {
	ID                   uint      `gorm:"primaryKey" json:"-"`
	Enabled              bool      `json:"enabled"`
	FlushIntervalS       int       `json:"flush_interval_s"`
	BatchSize            int       `json:"batch_size"`
	CompactIntervalS     int       `json:"compact_interval_s"`
	DataInliningRowLimit int       `json:"data_inlining_row_limit"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func (QueryLogConfig) TableName() string { return "duckgres_query_log_config" }

// SchemaMigration tracks one-shot data migrations that aren't expressible
// through GORM's AutoMigrate (e.g., backfills of new column defaults onto
// existing rows). One row per migration name, inserted exactly once.
type SchemaMigration struct {
	Name      string    `gorm:"primaryKey;size:128" json:"name"`
	AppliedAt time.Time `json:"applied_at"`
}

func (SchemaMigration) TableName() string { return "duckgres_schema_migrations" }

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
	ID              string                    `gorm:"primaryKey;size:255" json:"id"`
	PodName         string                    `gorm:"size:255;not null" json:"pod_name"`
	PodUID          string                    `gorm:"size:255;not null" json:"pod_uid"`
	BootID          string                    `gorm:"size:255;not null" json:"boot_id"`
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
	WorkerClaimMissReasonGlobalCap    WorkerClaimMissReason = "global_cap"
	WorkerClaimMissReasonShuttingDown WorkerClaimMissReason = "shutting_down"
)

const WarmCapacityMissBucketSize = 10 * time.Second

// WarmCapacityMissBucket stores foreground warm-capacity misses in coarse time
// buckets so every control-plane pod contributes to one shared demand signal.
type WarmCapacityMissBucket struct {
	Scope       string                `gorm:"primaryKey;type:text" json:"scope"`
	Reason      WorkerClaimMissReason `gorm:"primaryKey;size:64" json:"reason"`
	BucketStart time.Time             `gorm:"primaryKey;index" json:"bucket_start"`
	Count       int64                 `gorm:"not null" json:"count"`
	UpdatedAt   time.Time             `gorm:"not null;index" json:"updated_at"`
}

func (WarmCapacityMissBucket) TableName() string { return "warm_capacity_miss_buckets" }

// WarmCapacityMissAggregate is the grouped demand signal read by warm-capacity
// target computation.
type WarmCapacityMissAggregate struct {
	Scope  string                `json:"scope"`
	Reason WorkerClaimMissReason `json:"reason"`
	Count  int64                 `json:"count"`
}

// WorkerLifecycleStats is the grouped worker lifecycle state used for
// cluster-wide worker observability.
type WorkerLifecycleStats struct {
	Image   string      `json:"image"`
	State   WorkerState `json:"state"`
	Binding string      `json:"binding"`
	Count   int64       `json:"count"`
}

// WorkerRecord is the durable runtime coordination record for one worker pod.
type WorkerRecord struct {
	WorkerID            int         `gorm:"primaryKey" json:"worker_id"`
	PodName             string      `gorm:"size:255;not null;uniqueIndex" json:"pod_name"`
	PodUID              string      `gorm:"size:255" json:"pod_uid"`
	Image               string      `gorm:"size:512;index" json:"image"`
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
	// workers that haven't had creds issued yet (warm pool) and on legacy
	// rows from before this column existed — both are treated as "due now"
	// by the scheduler so they get refreshed eagerly.
	S3CredentialsExpiresAt *time.Time `gorm:"index" json:"s3_credentials_expires_at,omitempty"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
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
	OwnerEpoch   int64              `gorm:"not null" json:"owner_epoch"`
	CPInstanceID string             `gorm:"size:255" json:"cp_instance_id"`
	State        FlightSessionState `gorm:"size:32;not null" json:"state"`
	ExpiresAt    time.Time          `gorm:"index" json:"expires_at"`
	LastSeenAt   time.Time          `json:"last_seen_at"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

func (FlightSessionRecord) TableName() string { return "flight_session_records" }

// OrgConnectionQueueEntry is a cluster-wide FIFO admission request for one org
// connection. Rows expire quickly; they coordinate fairness across CP replicas.
type OrgConnectionQueueEntry struct {
	RequestID    string     `gorm:"primaryKey;size:64" json:"request_id"`
	OrgID        string     `gorm:"size:255;not null;index:idx_org_connection_queue_pending,priority:1" json:"org_id"`
	CPInstanceID string     `gorm:"size:255;not null;index" json:"cp_instance_id"`
	PID          int32      `gorm:"not null" json:"pid"`
	Protocol     string     `gorm:"size:32;not null" json:"protocol"`
	EnqueuedAt   time.Time  `gorm:"not null;index:idx_org_connection_queue_pending,priority:2" json:"enqueued_at"`
	ExpiresAt    time.Time  `gorm:"not null;index" json:"expires_at"`
	GrantedAt    *time.Time `gorm:"index" json:"granted_at,omitempty"`
	CanceledAt   *time.Time `gorm:"index" json:"canceled_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

func (OrgConnectionQueueEntry) TableName() string { return "org_connection_queue" }

// OrgConnectionLease is the durable cluster-wide admission lease for a live
// session. Capacity checks count active leases, ignoring owners whose CP row
// has expired.
type OrgConnectionLease struct {
	LeaseID      string    `gorm:"primaryKey;size:64" json:"lease_id"`
	RequestID    string    `gorm:"size:64;not null;uniqueIndex" json:"request_id"`
	OrgID        string    `gorm:"size:255;not null;index" json:"org_id"`
	CPInstanceID string    `gorm:"size:255;not null;index" json:"cp_instance_id"`
	PID          int32     `gorm:"not null" json:"pid"`
	Protocol     string    `gorm:"size:32;not null" json:"protocol"`
	AcquiredAt   time.Time `gorm:"not null" json:"acquired_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (OrgConnectionLease) TableName() string { return "org_connection_leases" }

// OrgConfig is a convenience view combining org metadata with resource limits.
//
// HostnameAlias is a plain string here (empty == "no alias") because snapshot
// consumers uniformly expect non-pointer types. The Org model keeps it as
// *string to drive sparse-unique semantics in the underlying table; that
// pointer-ness is irrelevant once the data is loaded into the snapshot.
type OrgConfig struct {
	Name                string
	DatabaseName        string
	HostnameAlias       string // empty when no alias is configured
	MaxWorkers          int
	MaxConnections      int
	MemoryBudget        string
	IdleTimeoutS        int
	WorkerCPURequest    string
	WorkerMemoryRequest string
	Users               map[string]string // username -> password
	Warehouse           *ManagedWarehouseConfig
}

// ManagedWarehouseConfig is the in-memory snapshot view of an org's warehouse metadata.
type ManagedWarehouseConfig struct {
	OrgID string

	Image           string
	DuckLakeVersion string
	AuroraMinACU    float64
	AuroraMaxACU    float64

	WarehouseDatabase ManagedWarehouseDatabase
	MetadataStore     ManagedWarehouseMetadataStore
	PgBouncer         ManagedWarehousePgBouncer
	S3                ManagedWarehouseS3
	Iceberg           ManagedWarehouseIceberg
	WorkerIdentity    ManagedWarehouseWorkerIdentity

	WarehouseDatabaseCredentials SecretRef
	MetadataStoreCredentials     SecretRef
	S3Credentials                SecretRef
	RuntimeConfig                SecretRef

	State                          ManagedWarehouseProvisioningState
	StatusMessage                  string
	WarehouseDatabaseState         ManagedWarehouseProvisioningState
	WarehouseDatabaseStatusMessage string
	MetadataStoreState             ManagedWarehouseProvisioningState
	MetadataStoreStatusMessage     string
	S3State                        ManagedWarehouseProvisioningState
	S3StatusMessage                string
	IcebergState                   ManagedWarehouseProvisioningState
	IcebergStatusMessage           string
	IdentityState                  ManagedWarehouseProvisioningState
	IdentityStatusMessage          string
	SecretsState                   ManagedWarehouseProvisioningState
	SecretsStatusMessage           string
	ReadyAt                        *time.Time
	FailedAt                       *time.Time
}

func copyManagedWarehouseConfig(warehouse *ManagedWarehouse) *ManagedWarehouseConfig {
	if warehouse == nil {
		return nil
	}

	cfg := &ManagedWarehouseConfig{
		OrgID:                          warehouse.OrgID,
		Image:                          warehouse.Image,
		DuckLakeVersion:                warehouse.DuckLakeVersion,
		AuroraMinACU:                   warehouse.AuroraMinACU,
		AuroraMaxACU:                   warehouse.AuroraMaxACU,
		WarehouseDatabase:              warehouse.WarehouseDatabase,
		MetadataStore:                  warehouse.MetadataStore,
		PgBouncer:                      warehouse.PgBouncer,
		S3:                             warehouse.S3,
		Iceberg:                        warehouse.Iceberg,
		WorkerIdentity:                 warehouse.WorkerIdentity,
		WarehouseDatabaseCredentials:   warehouse.WarehouseDatabaseCredentials,
		MetadataStoreCredentials:       warehouse.MetadataStoreCredentials,
		S3Credentials:                  warehouse.S3Credentials,
		RuntimeConfig:                  warehouse.RuntimeConfig,
		State:                          warehouse.State,
		StatusMessage:                  warehouse.StatusMessage,
		WarehouseDatabaseState:         warehouse.WarehouseDatabaseState,
		WarehouseDatabaseStatusMessage: warehouse.WarehouseDatabaseStatusMessage,
		MetadataStoreState:             warehouse.MetadataStoreState,
		MetadataStoreStatusMessage:     warehouse.MetadataStoreStatusMessage,
		S3State:                        warehouse.S3State,
		S3StatusMessage:                warehouse.S3StatusMessage,
		IcebergState:                   warehouse.IcebergState,
		IcebergStatusMessage:           warehouse.IcebergStatusMessage,
		IdentityState:                  warehouse.IdentityState,
		IdentityStatusMessage:          warehouse.IdentityStatusMessage,
		SecretsState:                   warehouse.SecretsState,
		SecretsStatusMessage:           warehouse.SecretsStatusMessage,
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
