package configstore

import (
	"testing"

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

	teams := []Team{
		{
			Name:         "analytics",
			MaxWorkers:   4,
			MinWorkers:   1,
			MemoryBudget: "8GB",
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
			MinWorkers:   t2.MinWorkers,
			MemoryBudget: t2.MemoryBudget,
			Users:        make(map[string]string),
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
