package configstore

import "time"

// Org represents a tenant with per-org resource limits.
type Org struct {
	Name                string            `gorm:"primaryKey;size:255" json:"name"`
	DatabaseName        string            `gorm:"size:255;uniqueIndex" json:"database_name"`
	MaxWorkers          int               `gorm:"default:0" json:"max_workers"`
	MemoryBudget        string            `gorm:"size:32" json:"memory_budget"`
	IdleTimeoutS        int               `gorm:"default:0" json:"idle_timeout_s"`
	WorkerCPURequest    string            `gorm:"size:32" json:"worker_cpu_request"`
	WorkerMemoryRequest string            `gorm:"size:32" json:"worker_memory_request"`
	Users               []OrgUser         `gorm:"foreignKey:OrgID;references:Name" json:"users,omitempty"`
	Warehouse           *ManagedWarehouse `gorm:"foreignKey:OrgID;references:Name;constraint:OnDelete:CASCADE" json:"warehouse,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
}

func (Org) TableName() string { return "duckgres_orgs" }

// OrgUser maps a username to an org with credentials.
type OrgUser struct {
	OrgID     string    `gorm:"primaryKey;size:255" json:"org_id"`
	Username  string    `gorm:"primaryKey;size:255" json:"username"`
	Password  string    `gorm:"size:255;not null" json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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

// ManagedWarehouseMetadataStore stores org-scoped DuckLake metadata DB info.
type ManagedWarehouseMetadataStore struct {
	Kind         string `gorm:"size:64" json:"kind"`
	Engine       string `gorm:"size:64" json:"engine"`
	Region       string `gorm:"size:64" json:"region"`
	Endpoint     string `gorm:"size:512" json:"endpoint"`
	Port         int    `json:"port"`
	DatabaseName string `gorm:"size:255" json:"database_name"`
	Username     string `gorm:"size:255" json:"username"`
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
	DeltaCatalogEnabled bool   `json:"delta_catalog_enabled"`
	DeltaCatalogPath    string `gorm:"size:1024" json:"delta_catalog_path"`
}

// ManagedWarehouseWorkerIdentity stores org-scoped worker identity metadata.
type ManagedWarehouseWorkerIdentity struct {
	Namespace          string `gorm:"size:255" json:"namespace"`
	ServiceAccountName string `gorm:"size:255" json:"service_account_name"`
	IAMRoleARN         string `gorm:"size:512" json:"iam_role_arn"`
}

// ManagedWarehouse is the config-store source of truth for an org's managed warehouse metadata.
type ManagedWarehouse struct {
	OrgID string `gorm:"primaryKey;size:255" json:"org_id"`

	Image        string  `gorm:"size:512" json:"image"`
	AuroraMinACU float64 `json:"aurora_min_acu"`
	AuroraMaxACU float64 `json:"aurora_max_acu"`

	WarehouseDatabase ManagedWarehouseDatabase       `gorm:"embedded;embeddedPrefix:warehouse_database_" json:"warehouse_database"`
	MetadataStore     ManagedWarehouseMetadataStore  `gorm:"embedded;embeddedPrefix:metadata_store_" json:"metadata_store"`
	PgBouncer         ManagedWarehousePgBouncer      `gorm:"embedded;embeddedPrefix:pgbouncer_" json:"pgbouncer"`
	S3                ManagedWarehouseS3             `gorm:"embedded;embeddedPrefix:s3_" json:"s3"`
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
	ID            uint      `gorm:"primaryKey" json:"-"`
	MetadataStore string    `gorm:"size:1024" json:"metadata_store"`
	ObjectStore   string    `gorm:"size:1024" json:"object_store"`
	DataPath      string    `gorm:"size:1024" json:"data_path"`
	S3Provider    string    `gorm:"size:64" json:"s3_provider"`
	S3Endpoint    string    `gorm:"size:512" json:"s3_endpoint"`
	S3AccessKey   string    `gorm:"size:255" json:"s3_access_key"`
	S3SecretKey   string    `gorm:"size:255" json:"-"`
	S3Region      string    `gorm:"size:64" json:"s3_region"`
	S3UseSSL      bool      `json:"s3_use_ssl"`
	S3URLStyle          string    `gorm:"size:16" json:"s3_url_style"`
	S3Chain             string    `gorm:"size:255" json:"s3_chain"`
	S3Profile           string    `gorm:"size:255" json:"s3_profile"`
	DeltaCatalogEnabled bool      `json:"delta_catalog_enabled"`
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

// WorkerRecord is the durable runtime coordination record for one worker pod.
type WorkerRecord struct {
	WorkerID            int         `gorm:"primaryKey" json:"worker_id"`
	PodName             string      `gorm:"size:255;not null;uniqueIndex" json:"pod_name"`
	PodUID              string      `gorm:"size:255" json:"pod_uid"`
	Image               string      `gorm:"size:512" json:"image"`
	State               WorkerState `gorm:"size:32;not null;index" json:"state"`
	OrgID               string      `gorm:"size:255;index" json:"org_id"`
	OwnerCPInstanceID   string      `gorm:"size:255;index" json:"owner_cp_instance_id"`
	OwnerEpoch          int64       `gorm:"not null" json:"owner_epoch"`
	ActivationStartedAt *time.Time  `json:"activation_started_at,omitempty"`
	LastHeartbeatAt     time.Time   `json:"last_heartbeat_at"`
	RetireReason        string      `gorm:"size:64" json:"retire_reason"`
	CreatedAt           time.Time   `json:"created_at"`
	UpdatedAt           time.Time   `json:"updated_at"`
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

// OrgConfig is a convenience view combining org metadata with resource limits.
type OrgConfig struct {
	Name                string
	DatabaseName        string
	MaxWorkers          int
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

	Image        string
	AuroraMinACU float64
	AuroraMaxACU float64

	WarehouseDatabase ManagedWarehouseDatabase
	MetadataStore     ManagedWarehouseMetadataStore
	PgBouncer         ManagedWarehousePgBouncer
	S3                ManagedWarehouseS3
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
		AuroraMinACU:                   warehouse.AuroraMinACU,
		AuroraMaxACU:                   warehouse.AuroraMaxACU,
		WarehouseDatabase:              warehouse.WarehouseDatabase,
		MetadataStore:                  warehouse.MetadataStore,
		PgBouncer:                      warehouse.PgBouncer,
		S3:                             warehouse.S3,
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
