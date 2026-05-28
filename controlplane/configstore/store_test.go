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
	// Verify OrgConfig construction from models
	hash1 := mustHash(t, "secret1")
	hash2 := mustHash(t, "secret2")
	hash3 := mustHash(t, "secret3")
	readyAt := time.Date(2026, time.March, 17, 12, 0, 0, 0, time.UTC)

	orgs := []Org{
		{
			Name:         "analytics",
			MaxWorkers:   4,
			MemoryBudget: "8GB",
			Warehouse: &ManagedWarehouse{
				OrgID: "analytics",
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
					PathPrefix: "ducklake/org-analytics/",
					Endpoint:   "s3.us-east-1.amazonaws.com",
					UseSSL:     true,
					URLStyle:   "vhost",
				},
				WorkerIdentity: ManagedWarehouseWorkerIdentity{
					Namespace:          "duckgres",
					ServiceAccountName: "org-analytics-worker",
					IAMRoleARN:         "arn:aws:iam::123456789012:role/org-analytics-worker",
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
			Users: []OrgUser{
				{Username: "alice", Password: hash1, OrgID: "analytics"},
				{Username: "bob", Password: hash2, OrgID: "analytics"},
			},
		},
		{
			Name:       "ingestion",
			MaxWorkers: 2,
			Users: []OrgUser{
				{Username: "charlie", Password: hash3, OrgID: "ingestion"},
			},
		},
	}

	snap := &Snapshot{
		Orgs:            make(map[string]*OrgConfig),
		OrgUserPassword: make(map[OrgUserKey]string),
	}

	for _, o := range orgs {
		oc := &OrgConfig{
			Name:         o.Name,
			MaxWorkers:   o.MaxWorkers,
			MemoryBudget: o.MemoryBudget,
			IdleTimeoutS: o.IdleTimeoutS,
			Users:        make(map[string]string),
		}
		if o.Warehouse != nil {
			oc.Warehouse = copyManagedWarehouseConfig(o.Warehouse)
		}
		for _, u := range o.Users {
			oc.Users[u.Username] = u.Password
			snap.OrgUserPassword[OrgUserKey{OrgID: o.Name, Username: u.Username}] = u.Password
		}
		snap.Orgs[o.Name] = oc
	}

	// Verify org config
	if len(snap.Orgs) != 2 {
		t.Fatalf("expected 2 orgs, got %d", len(snap.Orgs))
	}
	if snap.Orgs["analytics"].MaxWorkers != 4 {
		t.Errorf("expected analytics max_workers=4, got %d", snap.Orgs["analytics"].MaxWorkers)
	}
	if snap.Orgs["analytics"].MemoryBudget != "8GB" {
		t.Errorf("expected analytics memory_budget=8GB, got %s", snap.Orgs["analytics"].MemoryBudget)
	}
	if len(snap.Orgs["analytics"].Users) != 2 {
		t.Errorf("expected 2 analytics users, got %d", len(snap.Orgs["analytics"].Users))
	}
	if snap.Orgs["analytics"].Warehouse == nil {
		t.Fatal("expected analytics warehouse to be present")
	}
	if snap.Orgs["analytics"].Warehouse.WarehouseDatabase.DatabaseName != "analytics_wh" {
		t.Fatalf("expected analytics warehouse db name analytics_wh, got %q", snap.Orgs["analytics"].Warehouse.WarehouseDatabase.DatabaseName)
	}
	if snap.Orgs["analytics"].Warehouse.MetadataStore.Kind != "dedicated_rds" {
		t.Fatalf("expected metadata store kind dedicated_rds, got %q", snap.Orgs["analytics"].Warehouse.MetadataStore.Kind)
	}
	if snap.Orgs["analytics"].Warehouse.MetadataStoreCredentials.Name != "analytics-metadata" {
		t.Fatalf("expected metadata secret analytics-metadata, got %q", snap.Orgs["analytics"].Warehouse.MetadataStoreCredentials.Name)
	}
	if snap.Orgs["analytics"].Warehouse.RuntimeConfig.Name != "analytics-runtime" {
		t.Fatalf("expected runtime config secret analytics-runtime, got %q", snap.Orgs["analytics"].Warehouse.RuntimeConfig.Name)
	}
	if snap.Orgs["analytics"].Warehouse.ReadyAt == nil || !snap.Orgs["analytics"].Warehouse.ReadyAt.Equal(readyAt) {
		t.Fatalf("expected ready_at %v, got %v", readyAt, snap.Orgs["analytics"].Warehouse.ReadyAt)
	}
	if snap.Orgs["ingestion"].Warehouse != nil {
		t.Fatal("expected ingestion warehouse to be nil")
	}

	// Verify org-scoped user -> password mapping
	aliceKey := OrgUserKey{OrgID: "analytics", Username: "alice"}
	charlieKey := OrgUserKey{OrgID: "ingestion", Username: "charlie"}
	if _, ok := snap.OrgUserPassword[aliceKey]; !ok {
		t.Error("expected alice in analytics OrgUserPassword")
	}
	if _, ok := snap.OrgUserPassword[charlieKey]; !ok {
		t.Error("expected charlie in ingestion OrgUserPassword")
	}

	// Verify bcrypt password hashes are stored (not plaintext)
	if err := bcrypt.CompareHashAndPassword([]byte(snap.OrgUserPassword[aliceKey]), []byte("secret1")); err != nil {
		t.Errorf("expected alice password hash to match 'secret1': %v", err)
	}
}

func TestDatabaseNameForSNIPrefix(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"portola-uuid": {Name: "portola-uuid", DatabaseName: "portola"},
				"acme":         {Name: "acme", DatabaseName: "acme"},
			},
			DatabaseOrg: map[string]string{
				"portola": "portola-uuid",
				"acme":    "acme",
			},
			HostnameAliasOrg: map[string]string{
				"entirely-chief-wildcat": "portola-uuid",
			},
		},
	}

	// Alias resolves to the org's database_name (the security-through-obscurity case).
	if got := cs.DatabaseNameForSNIPrefix("entirely-chief-wildcat"); got != "portola" {
		t.Errorf("DatabaseNameForSNIPrefix(alias) = %q, want %q", got, "portola")
	}
	// Plain prefix passes through unchanged so legacy tenants (no alias) keep working.
	if got := cs.DatabaseNameForSNIPrefix("acme"); got != "acme" {
		t.Errorf("DatabaseNameForSNIPrefix(dbname) = %q, want %q", got, "acme")
	}
	// Unknown prefix passes through too — caller (ResolveDatabase) is the gate.
	if got := cs.DatabaseNameForSNIPrefix("nobody"); got != "nobody" {
		t.Errorf("DatabaseNameForSNIPrefix(unknown) = %q, want %q", got, "nobody")
	}
	// Empty input returns empty (don't masquerade as an unknown prefix).
	if got := cs.DatabaseNameForSNIPrefix(""); got != "" {
		t.Errorf("DatabaseNameForSNIPrefix(\"\") = %q, want \"\"", got)
	}
	// nil snapshot -> returns prefix (no panic).
	empty := &ConfigStore{}
	if got := empty.DatabaseNameForSNIPrefix("x"); got != "x" {
		t.Errorf("DatabaseNameForSNIPrefix on empty store = %q, want %q", got, "x")
	}
}

func TestResolveSNIPrefix(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"portola-uuid":    {Name: "portola-uuid", DatabaseName: "portola"},
				"team-hyphen":     {Name: "team-hyphen", DatabaseName: "team_hyphen"},
				"team_underscore": {Name: "team_underscore", DatabaseName: "teamdb"},
				"acme":            {Name: "acme", DatabaseName: "acme"},
			},
			DatabaseOrg: map[string]string{
				"portola":     "portola-uuid",
				"team_hyphen": "team-hyphen",
				"teamdb":      "team_underscore",
				"acme":        "acme",
			},
			HostnameAliasOrg: map[string]string{
				"entirely-chief-wildcat": "portola-uuid",
			},
		},
	}

	cases := []struct {
		name    string
		prefix  string
		wantOrg string
		wantDB  string
	}{
		{"hostname alias", "entirely-chief-wildcat", "portola-uuid", "portola"},
		{"database name", "acme", "acme", "acme"},
		{"hyphenated org name", "team-hyphen", "team-hyphen", "team_hyphen"},
		{"non DNS-safe org name", "team_underscore", "", ""},
		{"unknown", "nobody", "", ""},
		{"empty", "", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotOrg, gotDB := cs.ResolveSNIPrefix(tc.prefix)
			if gotOrg != tc.wantOrg || gotDB != tc.wantDB {
				t.Fatalf("ResolveSNIPrefix(%q) = (%q, %q), want (%q, %q)",
					tc.prefix, gotOrg, gotDB, tc.wantOrg, tc.wantDB)
			}
		})
	}

	empty := &ConfigStore{}
	if gotOrg, gotDB := empty.ResolveSNIPrefix("x"); gotOrg != "" || gotDB != "" {
		t.Fatalf("ResolveSNIPrefix on empty store = (%q, %q), want empty", gotOrg, gotDB)
	}
}

func TestResolvePostgresConnection(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"test-org-smoke-1778167994": {
					Name:         "test-org-smoke-1778167994",
					DatabaseName: "test_org_smoke_1778167994",
				},
				"billing": {
					Name:         "billing",
					DatabaseName: "billing_db",
				},
			},
			DatabaseOrg: map[string]string{
				"test_org_smoke_1778167994": "test-org-smoke-1778167994",
				"billing_db":                "billing",
			},
			HostnameAliasOrg: map[string]string{
				"billing-alias": "billing",
			},
			OrgUserPassword: map[OrgUserKey]string{
				{OrgID: "test-org-smoke-1778167994", Username: "root"}: mustHash(t, "secret"),
				{OrgID: "billing", Username: "root"}:                   mustHash(t, "secret"),
			},
			OrgUserPassthrough: map[OrgUserKey]bool{
				{OrgID: "test-org-smoke-1778167994", Username: "root"}: true,
			},
			OrgUserDefaultSearchPath: map[OrgUserKey]string{
				{OrgID: "billing", Username: "root"}: "iceberg.main,memory.main",
			},
		},
	}

	t.Run("explicit database must match managed SNI org", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"test_org_smoke_1778167994",
			"test-org-smoke-1778167994",
			true,
			"root",
			"secret",
		)
		if got.EffectiveDatabase != "test_org_smoke_1778167994" || got.OrgID != "test-org-smoke-1778167994" {
			t.Fatalf("effective route = (%q, %q), want explicit db/org", got.EffectiveDatabase, got.OrgID)
		}
		if got.UsedSNIDatabase {
			t.Fatalf("explicit database should take priority over SNI fallback")
		}
		if !got.DatabaseExists || !got.HostnameMatches || !got.Valid || !got.Passthrough {
			t.Fatalf("unexpected result: %+v", got)
		}
	})

	t.Run("empty database falls back to managed SNI", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"",
			"test-org-smoke-1778167994",
			true,
			"root",
			"secret",
		)
		if got.EffectiveDatabase != "test_org_smoke_1778167994" || !got.UsedSNIDatabase {
			t.Fatalf("SNI fallback result = (%q, used=%v), want test db/used", got.EffectiveDatabase, got.UsedSNIDatabase)
		}
		if !got.DatabaseExists || !got.HostnameMatches || !got.Valid {
			t.Fatalf("unexpected result: %+v", got)
		}
	})

	t.Run("two existing orgs mismatch is rejected before auth", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"test_org_smoke_1778167994",
			"billing",
			true,
			"root",
			"secret",
		)
		if !got.DatabaseExists {
			t.Fatalf("expected requested database to exist: %+v", got)
		}
		if got.HostnameMatches {
			t.Fatalf("expected SNI org billing to mismatch requested database org: %+v", got)
		}
		if got.Valid {
			t.Fatalf("mismatched managed hostname must not authenticate: %+v", got)
		}
	})

	t.Run("unknown managed SNI with explicit database is rejected before auth", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"test_org_smoke_1778167994",
			"ghostorg",
			true,
			"root",
			"secret",
		)
		if !got.DatabaseExists {
			t.Fatalf("expected requested database to exist: %+v", got)
		}
		if got.HostnameMatches {
			t.Fatalf("expected unknown managed SNI to mismatch requested database org: %+v", got)
		}
		if got.Valid {
			t.Fatalf("unknown managed SNI must not authenticate: %+v", got)
		}
	})

	t.Run("unknown SNI fallback keeps database-style error target", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"",
			"ghostorg",
			true,
			"root",
			"secret",
		)
		if got.EffectiveDatabase != "ghostorg" || got.DatabaseExists {
			t.Fatalf("unknown SNI fallback = (%q, exists=%v), want ghostorg missing", got.EffectiveDatabase, got.DatabaseExists)
		}
	})

	t.Run("valid user includes configured default search path", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"billing_db",
			"billing-alias",
			true,
			"root",
			"secret",
		)
		if !got.Valid {
			t.Fatalf("expected valid auth: %+v", got)
		}
		if got.DefaultSearchPath != "iceberg.main,memory.main" {
			t.Fatalf("DefaultSearchPath = %q, want iceberg.main,memory.main", got.DefaultSearchPath)
		}
	})
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

func TestOrgWarehouseStatus(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"no-warehouse": {Name: "no-warehouse", DatabaseName: "no_warehouse"},
				"with-ready": {
					Name: "with-ready", DatabaseName: "with_ready",
					Warehouse: &ManagedWarehouseConfig{OrgID: "with-ready", State: ManagedWarehouseStateReady},
				},
				"with-provisioning": {
					Name: "with-provisioning", DatabaseName: "with_provisioning",
					Warehouse: &ManagedWarehouseConfig{OrgID: "with-provisioning", State: ManagedWarehouseStateProvisioning},
				},
			},
		},
	}

	tests := []struct {
		orgID      string
		wantState  string
		wantExists bool
	}{
		{"unknown", "", false},
		{"no-warehouse", "", true},
		{"with-ready", "ready", true},
		{"with-provisioning", "provisioning", true},
	}
	for _, tt := range tests {
		gotState, gotExists := cs.OrgWarehouseStatus(tt.orgID)
		if gotState != tt.wantState || gotExists != tt.wantExists {
			t.Errorf("OrgWarehouseStatus(%q) = (%q, %v); want (%q, %v)",
				tt.orgID, gotState, gotExists, tt.wantState, tt.wantExists)
		}
	}
}

func TestTableNames(t *testing.T) {
	// Verify all models use the correct table names
	tests := []struct {
		model interface{ TableName() string }
		want  string
	}{
		{Org{}, "duckgres_orgs"},
		{OrgUser{}, "duckgres_org_users"},
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
