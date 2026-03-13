package configstore

import "time"

// Team represents a tenant with per-team resource limits.
type Team struct {
	Name         string     `gorm:"primaryKey;size:255" json:"name"`
	MaxWorkers   int        `gorm:"default:0" json:"max_workers"`
	MinWorkers   int        `gorm:"default:0" json:"min_workers"`
	MemoryBudget string     `gorm:"size:32" json:"memory_budget"`
	IdleTimeoutS int        `gorm:"default:0" json:"idle_timeout_s"`
	Users        []TeamUser `gorm:"foreignKey:TeamName;references:Name" json:"users,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
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

// DuckLakeConfig is a singleton row (ID=1) for DuckLake settings.
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
}
