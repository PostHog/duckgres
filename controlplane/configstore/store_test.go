package configstore

import (
	"testing"
)

func TestSnapshotBuild(t *testing.T) {
	// Verify TeamConfig construction from models
	teams := []Team{
		{
			Name:         "analytics",
			MaxWorkers:   4,
			MinWorkers:   1,
			MemoryBudget: "8GB",
			Users: []TeamUser{
				{Username: "alice", Password: "secret1", TeamName: "analytics"},
				{Username: "bob", Password: "secret2", TeamName: "analytics"},
			},
		},
		{
			Name:       "ingestion",
			MaxWorkers: 2,
			Users: []TeamUser{
				{Username: "charlie", Password: "secret3", TeamName: "ingestion"},
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

	// Verify password mapping
	if snap.UserPassword["alice"] != "secret1" {
		t.Errorf("expected alice password=secret1, got %s", snap.UserPassword["alice"])
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
