package configstore

import "time"

// Team represents a tenant with per-team resource limits.
type Team struct {
	Name         string            `gorm:"primaryKey;size:255" json:"name"`
	MaxWorkers   int               `gorm:"default:0" json:"max_workers"`
	MinWorkers   int               `gorm:"default:0" json:"min_workers"`
	MemoryBudget string            `gorm:"size:32" json:"memory_budget"`
	IdleTimeoutS int               `gorm:"default:0" json:"idle_timeout_s"`
	Users        []TeamUser        `gorm:"foreignKey:TeamName;references:Name" json:"users,omitempty"`
	Warehouse    *ManagedWarehouse `gorm:"foreignKey:TeamName;references:Name;constraint:OnDelete:CASCADE" json:"warehouse,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

func (Team) TableName() string { return "duckgres_teams" }

// TeamUser maps a username to a team with credentials.
type TeamUser struct {
	Username  string    `gorm:"primaryKey;size:255" json:"username"`
	Password  string    `gorm:"size:255;not null" json:"-"`
	TeamName  string    `gorm:"size:255;not null;index" json:"team_name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (TeamUser) TableName() string { return "duckgres_team_users" }

// ManagedWarehouseProvisioningState is the lifecycle state for a managed warehouse or sub-resource.
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

// ManagedWarehouseAurora stores Aurora runtime metadata for a team's warehouse.
type ManagedWarehouseAurora struct {
	Region       string `gorm:"size:64" json:"region"`
	Endpoint     string `gorm:"size:512" json:"endpoint"`
	Port         int    `json:"port"`
	DatabaseName string `gorm:"size:255" json:"database_name"`
	Username     string `gorm:"size:255" json:"username"`
}

// ManagedWarehouseMetadataStore stores team-scoped DuckLake metadata DB info.
type ManagedWarehouseMetadataStore struct {
	Kind         string `gorm:"size:64" json:"kind"`
	Engine       string `gorm:"size:64" json:"engine"`
	Region       string `gorm:"size:64" json:"region"`
	Endpoint     string `gorm:"size:512" json:"endpoint"`
	Port         int    `json:"port"`
	DatabaseName string `gorm:"size:255" json:"database_name"`
	Username     string `gorm:"size:255" json:"username"`
}

// ManagedWarehouseS3 stores object-store metadata for a team's warehouse.
type ManagedWarehouseS3 struct {
	Provider   string `gorm:"size:64" json:"provider"`
	Region     string `gorm:"size:64" json:"region"`
	Bucket     string `gorm:"size:255" json:"bucket"`
	PathPrefix string `gorm:"size:1024" json:"path_prefix"`
	Endpoint   string `gorm:"size:512" json:"endpoint"`
	UseSSL     bool   `json:"use_ssl"`
	URLStyle   string `gorm:"size:16" json:"url_style"`
}

// ManagedWarehouseWorkerIdentity stores team-scoped worker identity metadata.
type ManagedWarehouseWorkerIdentity struct {
	Namespace          string `gorm:"size:255" json:"namespace"`
	ServiceAccountName string `gorm:"size:255" json:"service_account_name"`
	IAMRoleARN         string `gorm:"size:512" json:"iam_role_arn"`
}

// ManagedWarehouse is the config-store source of truth for a team's managed warehouse metadata.
type ManagedWarehouse struct {
	TeamName string `gorm:"primaryKey;size:255" json:"team_name"`

	Aurora         ManagedWarehouseAurora         `gorm:"embedded;embeddedPrefix:aurora_" json:"aurora"`
	MetadataStore  ManagedWarehouseMetadataStore  `gorm:"embedded;embeddedPrefix:metadata_store_" json:"metadata_store"`
	S3             ManagedWarehouseS3             `gorm:"embedded;embeddedPrefix:s3_" json:"s3"`
	WorkerIdentity ManagedWarehouseWorkerIdentity `gorm:"embedded;embeddedPrefix:worker_identity_" json:"worker_identity"`

	AuroraCredentials        SecretRef `gorm:"embedded;embeddedPrefix:aurora_credentials_" json:"aurora_credentials"`
	MetadataStoreCredentials SecretRef `gorm:"embedded;embeddedPrefix:metadata_store_credentials_" json:"metadata_store_credentials"`
	S3Credentials            SecretRef `gorm:"embedded;embeddedPrefix:s3_credentials_" json:"s3_credentials"`
	RuntimeConfig            SecretRef `gorm:"embedded;embeddedPrefix:runtime_config_" json:"runtime_config"`

	State                      ManagedWarehouseProvisioningState `gorm:"size:32" json:"state"`
	StatusMessage              string                            `gorm:"size:1024" json:"status_message"`
	AuroraState                ManagedWarehouseProvisioningState `gorm:"size:32" json:"aurora_state"`
	AuroraStatusMessage        string                            `gorm:"size:1024" json:"aurora_status_message"`
	MetadataStoreState         ManagedWarehouseProvisioningState `gorm:"size:32" json:"metadata_store_state"`
	MetadataStoreStatusMessage string                            `gorm:"size:1024" json:"metadata_store_status_message"`
	S3State                    ManagedWarehouseProvisioningState `gorm:"size:32" json:"s3_state"`
	S3StatusMessage            string                            `gorm:"size:1024" json:"s3_status_message"`
	IdentityState              ManagedWarehouseProvisioningState `gorm:"size:32" json:"identity_state"`
	IdentityStatusMessage      string                            `gorm:"size:1024" json:"identity_status_message"`
	SecretsState               ManagedWarehouseProvisioningState `gorm:"size:32" json:"secrets_state"`
	SecretsStatusMessage       string                            `gorm:"size:1024" json:"secrets_status_message"`
	ReadyAt                    *time.Time                        `json:"ready_at"`
	FailedAt                   *time.Time                        `json:"failed_at"`
	CreatedAt                  time.Time                         `json:"created_at"`
	UpdatedAt                  time.Time                         `json:"updated_at"`
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
// In multi-tenant mode, the managed-warehouse contract is the intended per-team source of truth.
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
	S3URLStyle    string    `gorm:"size:16" json:"s3_url_style"`
	S3Chain       string    `gorm:"size:255" json:"s3_chain"`
	S3Profile     string    `gorm:"size:255" json:"s3_profile"`
	UpdatedAt     time.Time `json:"updated_at"`
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

// TeamConfig is a convenience view combining team metadata with resource limits.
type TeamConfig struct {
	Name         string
	MaxWorkers   int
	MinWorkers   int
	MemoryBudget string
	IdleTimeoutS int
	Users        map[string]string // username -> password
	Warehouse    *ManagedWarehouseConfig
}

// ManagedWarehouseConfig is the in-memory snapshot view of a team's warehouse metadata.
type ManagedWarehouseConfig struct {
	TeamName string

	Aurora         ManagedWarehouseAurora
	MetadataStore  ManagedWarehouseMetadataStore
	S3             ManagedWarehouseS3
	WorkerIdentity ManagedWarehouseWorkerIdentity

	AuroraCredentials        SecretRef
	MetadataStoreCredentials SecretRef
	S3Credentials            SecretRef
	RuntimeConfig            SecretRef

	State                      ManagedWarehouseProvisioningState
	StatusMessage              string
	AuroraState                ManagedWarehouseProvisioningState
	AuroraStatusMessage        string
	MetadataStoreState         ManagedWarehouseProvisioningState
	MetadataStoreStatusMessage string
	S3State                    ManagedWarehouseProvisioningState
	S3StatusMessage            string
	IdentityState              ManagedWarehouseProvisioningState
	IdentityStatusMessage      string
	SecretsState               ManagedWarehouseProvisioningState
	SecretsStatusMessage       string
	ReadyAt                    *time.Time
	FailedAt                   *time.Time
}

func copyManagedWarehouseConfig(warehouse *ManagedWarehouse) *ManagedWarehouseConfig {
	if warehouse == nil {
		return nil
	}

	cfg := &ManagedWarehouseConfig{
		TeamName:                   warehouse.TeamName,
		Aurora:                     warehouse.Aurora,
		MetadataStore:              warehouse.MetadataStore,
		S3:                         warehouse.S3,
		WorkerIdentity:             warehouse.WorkerIdentity,
		AuroraCredentials:          warehouse.AuroraCredentials,
		MetadataStoreCredentials:   warehouse.MetadataStoreCredentials,
		S3Credentials:              warehouse.S3Credentials,
		RuntimeConfig:              warehouse.RuntimeConfig,
		State:                      warehouse.State,
		StatusMessage:              warehouse.StatusMessage,
		AuroraState:                warehouse.AuroraState,
		AuroraStatusMessage:        warehouse.AuroraStatusMessage,
		MetadataStoreState:         warehouse.MetadataStoreState,
		MetadataStoreStatusMessage: warehouse.MetadataStoreStatusMessage,
		S3State:                    warehouse.S3State,
		S3StatusMessage:            warehouse.S3StatusMessage,
		IdentityState:              warehouse.IdentityState,
		IdentityStatusMessage:      warehouse.IdentityStatusMessage,
		SecretsState:               warehouse.SecretsState,
		SecretsStatusMessage:       warehouse.SecretsStatusMessage,
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
