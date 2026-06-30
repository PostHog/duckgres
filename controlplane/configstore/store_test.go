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
			Name:       "analytics",
			MaxWorkers: 4,
			Warehouse: &ManagedWarehouse{
				OrgID: "analytics",
				WarehouseDatabase: ManagedWarehouseDatabase{
					Endpoint: "analytics.cluster-xyz.us-east-1.rds.amazonaws.com",
					Port:     5432,
				},
				MetadataStore: ManagedWarehouseMetadataStore{
					Kind:         "dedicated_rds",
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
					Namespace:  "duckgres",
					IAMRoleARN: "arn:aws:iam::123456789012:role/org-analytics-worker",
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
				State:              ManagedWarehouseStateReady,
				StatusMessage:      "warehouse ready",
				MetadataStoreState: ManagedWarehouseStateReady,
				S3State:            ManagedWarehouseStateReady,
				IdentityState:      ManagedWarehouseStateReady,
				SecretsState:       ManagedWarehouseStateReady,
				ReadyAt:            &readyAt,
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
			Name:       o.Name,
			MaxWorkers: o.MaxWorkers,
			Users:      make(map[string]string),
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
	if len(snap.Orgs["analytics"].Users) != 2 {
		t.Errorf("expected 2 analytics users, got %d", len(snap.Orgs["analytics"].Users))
	}
	if snap.Orgs["analytics"].Warehouse == nil {
		t.Fatal("expected analytics warehouse to be present")
	}
	if snap.Orgs["analytics"].Warehouse.MetadataStore.DatabaseName != "ducklake_metadata" {
		t.Fatalf("expected analytics metadata db name ducklake_metadata, got %q", snap.Orgs["analytics"].Warehouse.MetadataStore.DatabaseName)
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
				"tenant-alpha-uuid": {Name: "tenant-alpha-uuid", DatabaseName: "tenant_alpha"},
				"acme":              {Name: "acme", DatabaseName: "acme"},
			},
			DatabaseOrg: map[string]string{
				"tenant_alpha": "tenant-alpha-uuid",
				"acme":         "acme",
			},
			HostnameAliasOrg: map[string]string{
				"entirely-chief-wildcat": "tenant-alpha-uuid",
			},
		},
	}

	// Alias resolves to the org's database_name (the security-through-obscurity case).
	if got := cs.DatabaseNameForSNIPrefix("entirely-chief-wildcat"); got != "tenant_alpha" {
		t.Errorf("DatabaseNameForSNIPrefix(alias) = %q, want %q", got, "tenant_alpha")
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
				"tenant-alpha-id": {Name: "tenant-alpha-id", DatabaseName: "tenant_alpha"},
				"team-hyphen":     {Name: "team-hyphen", DatabaseName: "team_hyphen"},
				"team_underscore": {Name: "team_underscore", DatabaseName: "teamdb"},
				"acme":            {Name: "acme", DatabaseName: "acme"},
			},
			DatabaseOrg: map[string]string{
				"tenant_alpha": "tenant-alpha-id",
				"team_hyphen":  "team-hyphen",
				"teamdb":       "team_underscore",
				"acme":         "acme",
			},
			HostnameAliasOrg: map[string]string{
				"entirely-chief-wildcat": "tenant-alpha-id",
			},
		},
	}

	cases := []struct {
		name    string
		prefix  string
		wantOrg string
		wantDB  string
	}{
		{"hostname alias", "entirely-chief-wildcat", "tenant-alpha-id", "tenant_alpha"},
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
			OrgUserDefaultCatalog: map[OrgUserKey]string{
				{OrgID: "billing", Username: "root"}: "iceberg",
			},
		},
	}

	t.Run("org resolved from SNI; ducklake catalog selected", func(t *testing.T) {
		got := cs.ResolvePostgresConnection(
			"ducklake",
			"test-org-smoke-1778167994",
			true,
			"root",
			"secret",
		)
		if !got.SNIResolved || got.OrgID != "test-org-smoke-1778167994" {
			t.Fatalf("org = (resolved=%v, %q), want test org from SNI: %+v", got.SNIResolved, got.OrgID, got)
		}
		if !got.CatalogValid || got.EffectiveCatalog != "ducklake" {
			t.Fatalf("catalog = (valid=%v, %q), want ducklake: %+v", got.CatalogValid, got.EffectiveCatalog, got)
		}
		if !got.Valid || !got.Passthrough {
			t.Fatalf("unexpected auth result: %+v", got)
		}
	})

	t.Run("iceberg catalog selected", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("iceberg", "test-org-smoke-1778167994", true, "root", "secret")
		if !got.CatalogValid || got.EffectiveCatalog != "iceberg" {
			t.Fatalf("catalog = (valid=%v, %q), want iceberg: %+v", got.CatalogValid, got.EffectiveCatalog, got)
		}
		if !got.Valid {
			t.Fatalf("expected valid auth: %+v", got)
		}
	})

	t.Run("empty database means use the default catalog", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("", "test-org-smoke-1778167994", true, "root", "secret")
		if !got.CatalogValid || got.EffectiveCatalog != "" {
			t.Fatalf("catalog = (valid=%v, %q), want empty/use-default: %+v", got.CatalogValid, got.EffectiveCatalog, got)
		}
		if !got.Valid {
			t.Fatalf("expected valid auth: %+v", got)
		}
	})

	t.Run("legacy database name is no longer a valid catalog", func(t *testing.T) {
		// The org's old database_name is not "ducklake"/"iceberg", so it fails the
		// catalog check even though SNI+auth would otherwise succeed.
		got := cs.ResolvePostgresConnection("test_org_smoke_1778167994", "test-org-smoke-1778167994", true, "root", "secret")
		if got.CatalogValid {
			t.Fatalf("legacy database name must not be a selectable catalog: %+v", got)
		}
	})

	t.Run("unknown managed SNI does not resolve an org", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("ducklake", "ghostorg", true, "root", "secret")
		if got.SNIResolved || got.OrgID != "" {
			t.Fatalf("unknown SNI must not resolve an org: %+v", got)
		}
		if got.Valid {
			t.Fatalf("unknown SNI must not authenticate: %+v", got)
		}
	})

	t.Run("identity requires managed SNI", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("ducklake", "test-org-smoke-1778167994", false, "root", "secret")
		if got.SNIResolved || got.Valid {
			t.Fatalf("without managed SNI there is no identity: %+v", got)
		}
		// Catalog validation is independent of identity.
		if !got.CatalogValid || got.EffectiveCatalog != "ducklake" {
			t.Fatalf("catalog should still validate: %+v", got)
		}
	})

	t.Run("wrong password fails auth but resolves org", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("ducklake", "test-org-smoke-1778167994", true, "root", "wrong")
		if !got.SNIResolved || got.Valid {
			t.Fatalf("expected resolved org but failed auth: %+v", got)
		}
	})

	t.Run("valid user includes configured default catalog", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("", "billing-alias", true, "root", "secret")
		if !got.Valid {
			t.Fatalf("expected valid auth: %+v", got)
		}
		if got.OrgID != "billing" {
			t.Fatalf("OrgID = %q, want billing (via hostname alias)", got.OrgID)
		}
		if got.DefaultCatalog != "iceberg" {
			t.Fatalf("DefaultCatalog = %q, want iceberg", got.DefaultCatalog)
		}
	})
}

// TestDisabledUserEnforcement covers the per-user kill switch at the snapshot
// level: a disabled user with CORRECT credentials is refused on every auth path
// (PG resolution reports Valid+Disabled so the CP can emit a distinct error;
// Flight's ValidateOrgUser* return false), while enabled users are unaffected.
func TestDisabledUserEnforcement(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"acme": {Name: "acme", DatabaseName: "acme_db"},
			},
			DatabaseOrg: map[string]string{"acme_db": "acme"},
			OrgUserPassword: map[OrgUserKey]string{
				{OrgID: "acme", Username: "bob"}:   mustHash(t, "secret"),
				{OrgID: "acme", Username: "alice"}: mustHash(t, "secret"),
			},
			OrgUserPassthrough: map[OrgUserKey]bool{
				{OrgID: "acme", Username: "bob"}: true,
			},
			OrgUserDisabled: map[OrgUserKey]bool{
				{OrgID: "acme", Username: "bob"}: true,
			},
		},
	}

	t.Run("PG resolution: disabled user authenticates but is flagged Disabled", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("ducklake", "acme", true, "bob", "secret")
		if !got.Valid {
			t.Fatalf("disabled user with correct creds should still authenticate (Valid=true) so the CP can distinguish from a bad password: %+v", got)
		}
		if !got.Disabled {
			t.Fatalf("expected Disabled=true: %+v", got)
		}
	})

	t.Run("PG resolution: wrong password never reveals Disabled", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("ducklake", "acme", true, "bob", "wrong")
		if got.Valid || got.Disabled {
			t.Fatalf("wrong password must not authenticate or leak Disabled: %+v", got)
		}
	})

	t.Run("PG resolution: enabled user unaffected", func(t *testing.T) {
		got := cs.ResolvePostgresConnection("ducklake", "acme", true, "alice", "secret")
		if !got.Valid || got.Disabled {
			t.Fatalf("enabled user should authenticate and not be Disabled: %+v", got)
		}
	})

	t.Run("Flight ValidateOrgUser refuses disabled user", func(t *testing.T) {
		if cs.ValidateOrgUser("acme", "bob", "secret") {
			t.Fatal("ValidateOrgUser must return false for a disabled user")
		}
		if !cs.ValidateOrgUser("acme", "alice", "secret") {
			t.Fatal("ValidateOrgUser must return true for an enabled user")
		}
	})

	t.Run("ValidateOrgUserAndGetPassthrough refuses disabled user without leaking passthrough", func(t *testing.T) {
		valid, passthrough := cs.ValidateOrgUserAndGetPassthrough("acme", "bob", "secret")
		if valid || passthrough {
			t.Fatalf("disabled user: want (false,false), got (%v,%v)", valid, passthrough)
		}
		valid, _ = cs.ValidateOrgUserAndGetPassthrough("acme", "alice", "secret")
		if !valid {
			t.Fatal("enabled user should validate")
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
		{OrgUserSecret{}, "duckgres_org_user_secrets"},
		{ManagedWarehouse{}, "duckgres_managed_warehouses"},
	}

	for _, tt := range tests {
		if got := tt.model.TableName(); got != tt.want {
			t.Errorf("%T.TableName() = %q, want %q", tt.model, got, tt.want)
		}
	}
}

func TestOrgDefaultWorkerProfile(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"unset": {Name: "unset"},
				"full": {
					Name:                "full",
					DefaultWorkerCPU:    "2",
					DefaultWorkerMemory: "8Gi",
					DefaultWorkerTTL:    "75m",
				},
				"partial": {Name: "partial", DefaultWorkerTTL: "10m"},
			},
		},
	}

	tests := []struct {
		orgID                     string
		wantCPU, wantMem, wantTTL string
	}{
		{"unknown", "", "", ""},
		{"unset", "", "", ""},
		{"full", "2", "8Gi", "75m"},
		{"partial", "", "", "10m"},
	}
	for _, tt := range tests {
		cpu, mem, ttl := cs.OrgDefaultWorkerProfile(tt.orgID)
		if cpu != tt.wantCPU || mem != tt.wantMem || ttl != tt.wantTTL {
			t.Errorf("OrgDefaultWorkerProfile(%q) = (%q,%q,%q); want (%q,%q,%q)",
				tt.orgID, cpu, mem, ttl, tt.wantCPU, tt.wantMem, tt.wantTTL)
		}
	}

	// Nil snapshot (store not yet loaded) must read as "not set", not panic.
	empty := &ConfigStore{}
	if cpu, mem, ttl := empty.OrgDefaultWorkerProfile("full"); cpu != "" || mem != "" || ttl != "" {
		t.Errorf("nil-snapshot OrgDefaultWorkerProfile = (%q,%q,%q); want all empty", cpu, mem, ttl)
	}
}

func TestOrgDefaultWorkerMinHotIdle(t *testing.T) {
	cs := &ConfigStore{
		snapshot: &Snapshot{
			Orgs: map[string]*OrgConfig{
				"unset": {Name: "unset"},
				"full":  {Name: "full", DefaultWorkerMinHotIdle: 2},
			},
		},
	}

	tests := []struct {
		orgID string
		want  int
	}{
		{"unknown", 0},
		{"unset", 0},
		{"full", 2},
	}
	for _, tt := range tests {
		if got := cs.OrgDefaultWorkerMinHotIdle(tt.orgID); got != tt.want {
			t.Errorf("OrgDefaultWorkerMinHotIdle(%q) = %d; want %d", tt.orgID, got, tt.want)
		}
	}

	empty := &ConfigStore{}
	if got := empty.OrgDefaultWorkerMinHotIdle("full"); got != 0 {
		t.Errorf("nil-snapshot OrgDefaultWorkerMinHotIdle = %d; want 0", got)
	}
}
