package configstore

import (
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func mustHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword(%q) failed: %v", password, err)
	}
	return hash
}

func TestSnapshotBuild(t *testing.T) {
	// Verify TeamConfig construction from models
	hash1 := mustHash(t, "secret1")
	hash2 := mustHash(t, "secret2")
	hash3 := mustHash(t, "secret3")
	readyAt := time.Date(2026, time.March, 17, 12, 0, 0, 0, time.UTC)

	teams := []Team{
		{
			Name:         "analytics",
			MaxWorkers:   4,
			MemoryBudget: "8GB",
			Warehouse: &ManagedWarehouse{
				TeamName: "analytics",
				WarehouseDatabase: ManagedWarehouseDatabase{
					Region:       "us-east-1",
					Endpoint:     "analytics.cluster-xyz.us-east-1.rds.amazonaws.com",
					Port:         5432,
					DatabaseName: "analytics_wh",
					Username:     "warehouse_user",
				},
				MetadataStore: ManagedWarehouseMetadataStore{
					Kind:         "dedicated_rds",
					Engine:       "postgres",
					Region:       "us-east-1",
					Endpoint:     "analytics-meta.cluster-xyz.us-east-1.rds.amazonaws.com",
					Port:         5432,
					DatabaseName: "ducklake_metadata",
					Username:     "metadata_user",
				},
				S3: ManagedWarehouseS3{
					Provider:   "aws",
					Region:     "us-east-1",
					Bucket:     "analytics-bucket",
					PathPrefix: "ducklake/team-analytics/",
					Endpoint:   "s3.us-east-1.amazonaws.com",
					UseSSL:     true,
					URLStyle:   "vhost",
				},
				WorkerIdentity: ManagedWarehouseWorkerIdentity{
					Namespace:          "duckgres",
					ServiceAccountName: "team-analytics-worker",
					IAMRoleARN:         "arn:aws:iam::123456789012:role/team-analytics-worker",
				},
				WarehouseDatabaseCredentials: SecretRef{
					Namespace: "duckgres",
					Name:      "analytics-warehouse-db",
					Key:       "dsn",
				},
				MetadataStoreCredentials: SecretRef{
					Namespace: "duckgres",
					Name:      "analytics-metadata",
					Key:       "dsn",
				},
				S3Credentials: SecretRef{
					Namespace: "duckgres",
					Name:      "analytics-s3",
					Key:       "credentials",
				},
				RuntimeConfig: SecretRef{
					Namespace: "duckgres",
					Name:      "analytics-runtime",
					Key:       "duckgres.yaml",
				},
				State:                  ManagedWarehouseStateReady,
				StatusMessage:          "warehouse ready",
				WarehouseDatabaseState: ManagedWarehouseStateReady,
				MetadataStoreState:     ManagedWarehouseStateReady,
				S3State:                ManagedWarehouseStateReady,
				IdentityState:          ManagedWarehouseStateReady,
				SecretsState:           ManagedWarehouseStateReady,
				ReadyAt:                &readyAt,
			},
			Users: []TeamUser{
				{Username: "alice", Password: hash1, TeamName: "analytics"},
				{Username: "bob", Password: hash2, TeamName: "analytics"},
			},
		},
		{
			Name:       "ingestion",
			MaxWorkers: 2,
			Users: []TeamUser{
				{Username: "charlie", Password: hash3, TeamName: "ingestion"},
			},
		},
	}

	snap := &Snapshot{
		Teams:        make(map[string]*TeamConfig),
		UserTeam:     make(map[string]string),
		UserPassword: make(map[string]string),
	}

	for _, t2 := range teams {
		tc := &TeamConfig{
			Name:         t2.Name,
			MaxWorkers:   t2.MaxWorkers,
			MemoryBudget: t2.MemoryBudget,
			IdleTimeoutS: t2.IdleTimeoutS,
			Users:        make(map[string]string),
		}
		if t2.Warehouse != nil {
			tc.Warehouse = copyManagedWarehouseConfig(t2.Warehouse)
		}
		for _, u := range t2.Users {
			tc.Users[u.Username] = u.Password
			snap.UserTeam[u.Username] = t2.Name
			snap.UserPassword[u.Username] = u.Password
		}
		snap.Teams[t2.Name] = tc
	}

	// Verify team config
	if len(snap.Teams) != 2 {
		t.Fatalf("expected 2 teams, got %d", len(snap.Teams))
	}
	if snap.Teams["analytics"].MaxWorkers != 4 {
		t.Errorf("expected analytics max_workers=4, got %d", snap.Teams["analytics"].MaxWorkers)
	}
	if snap.Teams["analytics"].MemoryBudget != "8GB" {
		t.Errorf("expected analytics memory_budget=8GB, got %s", snap.Teams["analytics"].MemoryBudget)
	}
	if len(snap.Teams["analytics"].Users) != 2 {
		t.Errorf("expected 2 analytics users, got %d", len(snap.Teams["analytics"].Users))
	}
	if snap.Teams["analytics"].Warehouse == nil {
		t.Fatal("expected analytics warehouse to be present")
	}
	if snap.Teams["analytics"].Warehouse.WarehouseDatabase.DatabaseName != "analytics_wh" {
		t.Fatalf("expected analytics warehouse db name analytics_wh, got %q", snap.Teams["analytics"].Warehouse.WarehouseDatabase.DatabaseName)
	}
	if snap.Teams["analytics"].Warehouse.MetadataStore.Kind != "dedicated_rds" {
		t.Fatalf("expected metadata store kind dedicated_rds, got %q", snap.Teams["analytics"].Warehouse.MetadataStore.Kind)
	}
	if snap.Teams["analytics"].Warehouse.MetadataStoreCredentials.Name != "analytics-metadata" {
		t.Fatalf("expected metadata secret analytics-metadata, got %q", snap.Teams["analytics"].Warehouse.MetadataStoreCredentials.Name)
	}
	if snap.Teams["analytics"].Warehouse.RuntimeConfig.Name != "analytics-runtime" {
		t.Fatalf("expected runtime config secret analytics-runtime, got %q", snap.Teams["analytics"].Warehouse.RuntimeConfig.Name)
	}
	if snap.Teams["analytics"].Warehouse.ReadyAt == nil || !snap.Teams["analytics"].Warehouse.ReadyAt.Equal(readyAt) {
		t.Fatalf("expected ready_at %v, got %v", readyAt, snap.Teams["analytics"].Warehouse.ReadyAt)
	}
	if snap.Teams["ingestion"].Warehouse != nil {
		t.Fatal("expected ingestion warehouse to be nil")
	}

	// Verify user → team mapping
	if snap.UserTeam["alice"] != "analytics" {
		t.Errorf("expected alice in analytics, got %s", snap.UserTeam["alice"])
	}
	if snap.UserTeam["charlie"] != "ingestion" {
		t.Errorf("expected charlie in ingestion, got %s", snap.UserTeam["charlie"])
	}

	// Verify bcrypt password hashes are stored (not plaintext)
	if err := bcrypt.CompareHashAndPassword([]byte(snap.UserPassword["alice"]), []byte("secret1")); err != nil {
		t.Errorf("expected alice password hash to match 'secret1': %v", err)
	}
}

func TestHashPassword(t *testing.T) {
	hash, err := HashPassword("testpass")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("testpass")); err != nil {
		t.Errorf("bcrypt.CompareHashAndPassword failed for correct password: %v", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("wrongpass")); err == nil {
		t.Error("bcrypt.CompareHashAndPassword should have failed for wrong password")
	}
}

func TestTableNames(t *testing.T) {
	// Verify all models use the correct table names
	tests := []struct {
		model interface{ TableName() string }
		want  string
	}{
		{Team{}, "duckgres_teams"},
		{TeamUser{}, "duckgres_team_users"},
		{ManagedWarehouse{}, "duckgres_managed_warehouses"},
		{GlobalConfig{}, "duckgres_global_config"},
		{DuckLakeConfig{}, "duckgres_ducklake_config"},
		{RateLimitConfig{}, "duckgres_rate_limit_config"},
		{QueryLogConfig{}, "duckgres_query_log_config"},
	}

	for _, tt := range tests {
		if got := tt.model.TableName(); got != tt.want {
			t.Errorf("%T.TableName() = %q, want %q", tt.model, got, tt.want)
		}
	}
}
