//go:build kubernetes

package configstore

import (
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Postgres-backed tests for IssueProjectUserServiceCredential. These run
// against the shared integration Postgres (tests/integration) and lock the
// three invariants a job depends on:
//
//  1. First call CREATES the team's project_user login with the minted hash.
//  2. Back-to-back calls REUSE the live grant — the hash and updated_at
//     do NOT move (a concurrent long-lived run's steps must not have their
//     working credential rotated out from under them mid-flight, and the
//     discovery change-marker must not wake on every job fetch).
//  3. force_rotate ALWAYS rotates, even on a fresh grant — this is how a
//     job that has nothing cached gets a plaintext without waiting out the
//     safety window.
func TestIssueProjectUserServiceCredentialLifecyclePostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-cred-org", DatabaseName: "svc_cred_org"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	team := OrgTeam{OrgID: org.Name, TeamID: 42, SchemaName: "team_42", Enabled: true}
	if err := db.Create(&team).Error; err != nil {
		t.Fatalf("create org team: %v", err)
	}

	// 1. First issue CREATES the row and returns a plaintext that auth-checks.
	issue1, err := cs.IssueProjectUserServiceCredential(org.Name, 42, "dagster:events-backfill", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}
	if !issue1.Rotated {
		t.Fatal("first issue must rotate (create)")
	}
	if issue1.Username != "posthog_team_42_rw" {
		t.Fatalf("username = %q, want posthog_team_42_rw (must match admin console derivation)", issue1.Username)
	}
	if issue1.Plaintext == "" {
		t.Fatal("first issue must return a plaintext")
	}
	var stored OrgUser
	if err := db.First(&stored, "org_id = ? AND username = ?", org.Name, issue1.Username).Error; err != nil {
		t.Fatalf("project user row not created: %v", err)
	}
	if bcrypt.CompareHashAndPassword([]byte(stored.Password), []byte(issue1.Plaintext)) != nil {
		t.Fatal("stored hash does not match the plaintext the CP handed out")
	}
	if stored.AccessMode != OrgUserAccessModeProjectUser {
		t.Fatalf("access_mode = %q, want %q (the login must bind the team's project_user namespaces)",
			stored.AccessMode, OrgUserAccessModeProjectUser)
	}
	createdHash := stored.Password
	createdAt := stored.UpdatedAt
	if got := issue1.ExpiresAt.Sub(createdAt); got < 14*time.Minute || got > 16*time.Minute {
		t.Fatalf("expires_at - updated_at = %v, want ~15m (the requested TTL)", got)
	}

	// 2. A back-to-back call reuses: same hash, same updated_at, NO plaintext.
	issue2, err := cs.IssueProjectUserServiceCredential(org.Name, 42, "dagster:events-backfill", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("second issue: %v", err)
	}
	if issue2.Rotated {
		t.Fatal("immediate re-issue must reuse the live grant, not rotate")
	}
	if issue2.Plaintext != "" {
		t.Fatal("reuse must NOT echo a plaintext (caller already holds it, or must force_rotate)")
	}
	var reread OrgUser
	if err := db.First(&reread, "org_id = ? AND username = ?", org.Name, issue1.Username).Error; err != nil {
		t.Fatalf("re-read project user: %v", err)
	}
	if reread.Password != createdHash {
		t.Fatal("reuse must not change the stored hash")
	}
	if !reread.UpdatedAt.Equal(createdAt) {
		t.Fatalf("reuse must not bump updated_at: was %v, now %v", createdAt, reread.UpdatedAt)
	}
	// The reuse path reports the ORIGINAL mint's expiry, not now+TTL.
	if !issue2.ExpiresAt.Equal(createdAt.Add(15 * time.Minute)) {
		t.Fatalf("reuse expiry %v, want %v (the live grant's real expiry)", issue2.ExpiresAt, createdAt.Add(15*time.Minute))
	}

	// 3. force_rotate replaces the credential even though the live grant is
	// nowhere near expiry.
	issue3, err := cs.IssueProjectUserServiceCredential(org.Name, 42, "dagster:events-backfill", 15*time.Minute, true)
	if err != nil {
		t.Fatalf("force rotate: %v", err)
	}
	if !issue3.Rotated {
		t.Fatal("force_rotate must rotate")
	}
	if issue3.Plaintext == "" {
		t.Fatal("force_rotate must return a plaintext")
	}
	var rotated OrgUser
	if err := db.First(&rotated, "org_id = ? AND username = ?", org.Name, issue1.Username).Error; err != nil {
		t.Fatalf("re-read after rotate: %v", err)
	}
	if rotated.Password == createdHash {
		t.Fatal("force_rotate must replace the stored hash")
	}
	if bcrypt.CompareHashAndPassword([]byte(rotated.Password), []byte(issue3.Plaintext)) != nil {
		t.Fatal("rotated hash does not match the new plaintext")
	}
	// The OLD credential no longer matches the row — proof the rotation
	// actually invalidated it. (Existing sessions keep working: expiry is
	// handshake-only, and nothing here touches live sessions.)
	if bcrypt.CompareHashAndPassword([]byte(rotated.Password), []byte(issue1.Plaintext)) == nil {
		t.Fatal("old plaintext must no longer match after rotation")
	}
}

func TestIssueProjectUserServiceCredentialValidatesScopePostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	if _, err := cs.IssueProjectUserServiceCredential("ghost", 42, "d", time.Minute, false); err == nil {
		t.Fatal("unknown org must fail")
	}

	org := Org{Name: "svc-cred-org-2", DatabaseName: "svc_cred_org_2"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if _, err := cs.IssueProjectUserServiceCredential(org.Name, 42, "d", time.Minute, false); err == nil {
		t.Fatal("unknown team must fail")
	}
	// Use raw SQL rather than db.Create(OrgTeam{Enabled: false}): gorm treats a
	// zero-value bool paired with `default:true` as "omitted" and rewrites the
	// INSERT to use the default — exactly why the store must check Enabled
	// explicitly, since the write side of this system lies by omission.
	if err := db.Exec(
		"INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at) VALUES (?, ?, ?, FALSE, now(), now())",
		org.Name, 42, "team_42",
	).Error; err != nil {
		t.Fatalf("create disabled team: %v", err)
	}
	var got OrgTeam
	if err := db.First(&got, "org_id = ? AND team_id = ?", org.Name, 42).Error; err != nil {
		t.Fatalf("read back team: %v", err)
	}
	if got.Enabled {
		t.Fatal("team read back enabled; the insert did not land the disabled state the test intends")
	}
	if _, err := cs.IssueProjectUserServiceCredential(org.Name, 42, "d", time.Minute, false); err == nil {
		t.Fatal("disabled team must fail")
	}
}

func TestIssueProjectUserServiceCredentialPrincipalRequired(t *testing.T) {
	cs := newPostgresConfigStore(t)
	if _, err := cs.IssueProjectUserServiceCredential("any", 42, "", time.Minute, false); err == nil {
		t.Fatal("empty principal must fail (audit attribution depends on it)")
	}
}

// The TTL clock is service_grant_expires_at, never updated_at: an ADMIN
// project-login rotation (UpsertProjectLogin) overwrites the same row's
// password and bumps updated_at. If the mint keyed on updated_at, the next
// job fetch would (a) misdate the grant's real expiry and (b) take the reuse
// branch and hand the job NO plaintext for a credential it never saw. This
// test pins the adversarial-review invariant: after a third-party rotation
// (simulated here by clearing the grant column exactly as the admin
// OnConflict update does), the service mint must ROTATE and return a fresh
// plaintext, not reuse.
func TestIssueProjectUserServiceCredentialAdminRotationCollisionPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	db.Create(&Org{Name: "svc-cred-org-3", DatabaseName: "svc_cred_org_3"})
	db.Create(&OrgTeam{OrgID: "svc-cred-org-3", TeamID: 42, SchemaName: "team_42", Enabled: true})

	first, err := cs.IssueProjectUserServiceCredential("svc-cred-org-3", 42, "d", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}

	// Simulate the admin rotation: admin UpsertProjectLogin's OnConflict
	// updates (among others) password + updated_at and clears
	// service_grant_expires_at. The hash it installs is NOT one the mint path
	// issued.
	adminHash, err := HashPassword("admin-issued-credential-32-chars!!")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&OrgUser{}).
		Where("org_id = ? AND username = ?", "svc-cred-org-3", first.Username).
		Updates(map[string]interface{}{
			"password":                 adminHash,
			"updated_at":               time.Now().UTC(),
			"service_grant_expires_at": nil,
		}).Error; err != nil {
		t.Fatalf("simulate admin rotation: %v", err)
	}

	second, err := cs.IssueProjectUserServiceCredential("svc-cred-org-3", 42, "d", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("second issue after admin rotation: %v", err)
	}
	if !second.Rotated {
		t.Fatal("after admin rotation the mint MUST rotate, not reuse — reusing would return no plaintext for a credential the job never saw")
	}
	if second.Plaintext == "" {
		t.Fatal("after admin rotation the mint MUST return a fresh plaintext")
	}
	var reread OrgUser
	if err := db.First(&reread, "org_id = ? AND username = ?", "svc-cred-org-3", first.Username).Error; err != nil {
		t.Fatal(err)
	}
	if reread.Password == adminHash {
		t.Fatal("rotation must replace the admin-installed hash")
	}
	if reread.ServiceGrantExpiresAt == nil {
		t.Fatal("rotation must set service_grant_expires_at")
	}
}

// An operator-disabled login must never be silently re-enabled by a service
// mint: the kill switch means something, and the mint path (driven by the
// internal secret, a machine credential) must not undo it.
func TestIssueProjectUserServiceCredentialRefusesDisabledLoginPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	db.Create(&Org{Name: "svc-cred-org-4", DatabaseName: "svc_cred_org_4"})
	db.Create(&OrgTeam{OrgID: "svc-cred-org-4", TeamID: 42, SchemaName: "team_42", Enabled: true})

	if _, err := cs.IssueProjectUserServiceCredential("svc-cred-org-4", 42, "d", 15*time.Minute, false); err != nil {
		t.Fatalf("first issue: %v", err)
	}
	if err := db.Model(&OrgUser{}).
		Where("org_id = ? AND username = ?", "svc-cred-org-4", "posthog_team_42_rw").
		Update("disabled", true).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := cs.IssueProjectUserServiceCredential("svc-cred-org-4", 42, "d", 15*time.Minute, true); err == nil {
		t.Fatal("mint against a disabled login must fail, even with force_rotate")
	}
	// Both branches refused: the row stays disabled and untouched.
	var user OrgUser
	if err := db.First(&user, "org_id = ? AND username = ?", "svc-cred-org-4", "posthog_team_42_rw").Error; err != nil {
		t.Fatal(err)
	}
	if !user.Disabled {
		t.Fatal("the login must remain disabled after a refused mint")
	}

	// A reuse-path call (no force) against the disabled login must fail too —
	// the handshake would refuse it the same as auth would.
	if _, err := cs.IssueProjectUserServiceCredential("svc-cred-org-4", 42, "d", 15*time.Minute, false); err == nil {
		t.Fatal("reuse against a disabled login must fail")
	}
}

// A row with the same username but a twisted access_mode/team (an operator
// mistake, or a hand-hacked row) must be RE-ESTABLISHED to the project_user
// invariants by the mint, not left granting the wrong scope and certainly
// not colliding a plain CREATE into the primary key.
func TestIssueProjectUserServiceCredentialReestablishesInvariantOnTwistedRowPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	db.Create(&Org{Name: "svc-cred-org-5", DatabaseName: "svc_cred_org_5"})
	db.Create(&OrgTeam{OrgID: "svc-cred-org-5", TeamID: 42, SchemaName: "team_42", Enabled: true})

	// Hand-plant a row with the service username but access_mode=unrestricted
	// and no team binding — the state a bad admin flip would leave.
	if err := db.Exec(
		"INSERT INTO duckgres_org_users (org_id, username, password, passthrough, access_mode, team_id, disabled, created_at, updated_at) VALUES (?, ?, ?, FALSE, 'unrestricted', NULL, FALSE, now(), now())",
		"svc-cred-org-5", "posthog_team_42_rw", "$2a$10$Z2IMbWec4kIV53lYNMj4Ke1sA2FxSqavOSQXiOoEAosHLzpqdzpbe",
	).Error; err != nil {
		t.Fatalf("plant twisted row: %v", err)
	}

	issue, err := cs.IssueProjectUserServiceCredential("svc-cred-org-5", 42, "d", 15*time.Minute, true)
	if err != nil {
		t.Fatalf("mint against twisted row: %v", err)
	}
	if !issue.Rotated || issue.Plaintext == "" {
		t.Fatal("a twisted row is an untrusted credential state: must rotate and return plaintext")
	}
	var user OrgUser
	if err := db.First(&user, "org_id = ? AND username = ?", "svc-cred-org-5", "posthog_team_42_rw").Error; err != nil {
		t.Fatal(err)
	}
	if user.AccessMode != OrgUserAccessModeProjectUser {
		t.Fatalf("access_mode must be re-established to project_user, got %q", user.AccessMode)
	}
	if user.TeamID == nil || *user.TeamID != 42 {
		t.Fatalf("team_id must be re-established to 42, got %+v", user.TeamID)
	}
	if user.Passthrough {
		t.Fatal("passthrough must be cleared (project-scoped logins forbid it)")
	}
}
