//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

func TestSetOrgUserDisabledSerializesWithAdmissionPostgres(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	orgID := "config-mutation-disabled"
	username := "alice"

	if err := storeA.DB().Create(&cpconfigstore.Org{Name: orgID, DatabaseName: orgID}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := storeA.DB().Create(&cpconfigstore.OrgUser{
		OrgID:    orgID,
		Username: username,
		Password: "hash",
	}).Error; err != nil {
		t.Fatalf("create org user: %v", err)
	}

	assertConfigMutationWaitsForAdmissionLock(t, storeA, orgID, func() error {
		return storeB.SetOrgUserDisabled(orgID, username, true)
	})

	var user cpconfigstore.OrgUser
	if err := storeA.DB().First(&user, "org_id = ? AND username = ?", orgID, username).Error; err != nil {
		t.Fatalf("read org user: %v", err)
	}
	if !user.Disabled {
		t.Fatal("expected user to be disabled after the admission lock was released")
	}
}

func TestDeleteOrgTeamSerializesWithAdmissionPostgres(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	const orgID = "config-mutation-delete-team"
	targetTeamID := int64(2)

	if err := storeA.DB().Create(&cpconfigstore.Org{Name: orgID, DatabaseName: orgID}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := storeA.DB().Create([]cpconfigstore.OrgTeam{
		{OrgID: orgID, TeamID: 1, SchemaName: "team_1", Enabled: true},
		{OrgID: orgID, TeamID: targetTeamID, SchemaName: "team_2", Enabled: true},
	}).Error; err != nil {
		t.Fatalf("create org teams: %v", err)
	}
	if err := storeA.DB().Create(&cpconfigstore.OrgUser{
		OrgID:      orgID,
		Username:   "project-reader",
		Password:   "hash",
		AccessMode: cpconfigstore.OrgUserAccessModeProjectReader,
		TeamID:     &targetTeamID,
	}).Error; err != nil {
		t.Fatalf("create project reader: %v", err)
	}

	pstore := provisioning.NewGormStore(storeB)
	assertConfigMutationWaitsForAdmissionLock(t, storeA, orgID, func() error {
		_, err := pstore.DeleteOrgTeam(orgID, targetTeamID)
		return err
	})

	var readerCount int64
	if err := storeA.DB().Model(&cpconfigstore.OrgUser{}).
		Where("org_id = ? AND username = ?", orgID, "project-reader").
		Count(&readerCount).Error; err != nil {
		t.Fatalf("count project reader: %v", err)
	}
	if readerCount != 0 {
		t.Fatalf("project reader count = %d, want 0 after team deletion", readerCount)
	}
}

func assertConfigMutationWaitsForAdmissionLock(
	t *testing.T,
	store *cpconfigstore.ConfigStore,
	orgID string,
	mutation func() error,
) {
	t.Helper()

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin admission lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()

	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("read admission lock holder pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", orgConnectionAdvisoryLockKey(orgID)); err != nil {
		t.Fatalf("take admission lock: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- mutation() }()

	waitForBlockedConfigMutation(t, sqlDB, holderPID, result)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release admission lock: %v", err)
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("config mutation after admission lock release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for config mutation after admission lock release")
	}
}

func waitForBlockedConfigMutation(t *testing.T, db *sql.DB, holderPID int, result <-chan error) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-result:
			t.Fatalf("config mutation completed before admission lock release: %v", err)
		default:
		}

		var waiters int
		if err := db.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND datname = current_database()
			  AND $1 = ANY(pg_blocking_pids(pid))
		`, holderPID).Scan(&waiters); err != nil {
			t.Fatalf("poll config mutation admission-lock waiter: %v", err)
		}
		if waiters > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timed out waiting for config mutation to block on the admission lock")
}
