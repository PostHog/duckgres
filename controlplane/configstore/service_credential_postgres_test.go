//go:build kubernetes

package configstore

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// Postgres-backed tests for MintServiceCredential / RefreshServiceCredential.
// These run against the shared integration Postgres (tests/integration) and
// lock the invariants a job depends on:
//
//  1. First mint CREATES a duckgres_service_grants row (NEVER an
//     duckgres_org_users row) with a server-generated svc_ credential_id and
//     returns a plaintext that auth-checks.
//  2. Back-to-back mints for the same principal REUSE the live grant — same
//     credential_id, salt-and-hash untouched, NO plaintext (a concurrent
//     long-lived run's steps must not have their working credential rotated
//     out from under them mid-flight).
//  3. force_rotate ALWAYS rotates the live grant's secret, even on a fresh
//     one — this is how a job that has nothing cached gets a plaintext.
//  4. Refresh ALWAYS rotates the named grant (unknown → 404-mappable,
//     revoked → never resurrected).
var credentialIDShape = regexp.MustCompile(`^svc_[0-9a-f]{24}$`)

func TestMintServiceCredentialLifecyclePostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org", DatabaseName: "svc_grant_org"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	// 1. First mint creates the grant row and returns a plaintext that
	// auth-checks against the STORED hash.
	mint1, err := cs.MintServiceCredential(org.Name, "dagster:lifecycle", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("first mint: %v", err)
	}
	if !mint1.Rotated {
		t.Fatal("first mint must rotate (create)")
	}
	if !credentialIDShape.MatchString(mint1.CredentialID) {
		t.Fatalf("credential_id = %q, want svc_<24 hex>", mint1.CredentialID)
	}
	if mint1.Plaintext == "" {
		t.Fatal("first mint must return a plaintext secret")
	}
	if mint1.Principal != "dagster:lifecycle" {
		t.Fatalf("principal = %q, want the mint's audit attribution", mint1.Principal)
	}
	var stored ServiceGrant
	if err := db.First(&stored, "org_id = ? AND credential_id = ?", org.Name, mint1.CredentialID).Error; err != nil {
		t.Fatalf("service grant row not created: %v", err)
	}
	if stored.Principal != "dagster:lifecycle" {
		t.Fatalf("stored principal = %q — audit attribution must be recorded on the row", stored.Principal)
	}
	if bcrypt.CompareHashAndPassword([]byte(stored.PasswordHash), []byte(mint1.Plaintext)) != nil {
		t.Fatal("stored hash does not match the plaintext the CP handed out")
	}
	if got := stored.ExpiresAt.Sub(stored.LastRotatedAt); got < 14*time.Minute || got > 16*time.Minute {
		t.Fatalf("expires_at - last_rotated_at = %v, want ~15m (the requested TTL)", got)
	}

	// The mint must NEVER touch duckgres_org_users.
	var userCount int64
	if err := db.Model(&OrgUser{}).Where("org_id = ?", org.Name).Count(&userCount).Error; err != nil {
		t.Fatal(err)
	}
	if userCount != 0 {
		t.Fatalf("duckgres_org_users rows = %d, want 0 — service credentials are grant rows, not org users", userCount)
	}

	// 2. A back-to-back mint reuses: same credential_id, same hash, NO
	// plaintext.
	mint2, err := cs.MintServiceCredential(org.Name, "dagster:lifecycle", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("second mint: %v", err)
	}
	if mint2.Rotated {
		t.Fatal("immediate re-mint must reuse the live grant, not rotate")
	}
	if mint2.CredentialID != mint1.CredentialID {
		t.Fatalf("reuse credential_id = %q, want %q (the live grant's identity)", mint2.CredentialID, mint1.CredentialID)
	}
	if mint2.Plaintext != "" {
		t.Fatal("reuse must NOT echo a plaintext (caller already holds it, or must force_rotate)")
	}
	var reread ServiceGrant
	if err := db.First(&reread, "org_id = ? AND credential_id = ?", org.Name, mint1.CredentialID).Error; err != nil {
		t.Fatalf("re-read service grant: %v", err)
	}
	if reread.PasswordHash != stored.PasswordHash {
		t.Fatal("reuse must not change the stored hash")
	}
	// The reuse path reports the ORIGINAL mint's expiry, not now+TTL.
	if !mint2.ExpiresAt.Equal(stored.ExpiresAt.UTC()) {
		t.Fatalf("reuse expiry %v, want %v (the live grant's real expiry)", mint2.ExpiresAt, stored.ExpiresAt)
	}
	var grantCount int64
	if err := db.Model(&ServiceGrant{}).Where("org_id = ?", org.Name).Count(&grantCount).Error; err != nil {
		t.Fatal(err)
	}
	if grantCount != 1 {
		t.Fatalf("service grant rows = %d, want exactly 1 (reuse is not a second credential)", grantCount)
	}

	// 3. force_rotate replaces the secret on the SAME grant row even though
	// the live grant is nowhere near expiry.
	mint3, err := cs.MintServiceCredential(org.Name, "dagster:lifecycle", 15*time.Minute, true)
	if err != nil {
		t.Fatalf("force rotate: %v", err)
	}
	if !mint3.Rotated || mint3.Plaintext == "" {
		t.Fatal("force_rotate must rotate and return a plaintext")
	}
	if mint3.CredentialID != mint1.CredentialID {
		t.Fatalf("force rotate keeps the grant's identity: got %q, want %q", mint3.CredentialID, mint1.CredentialID)
	}
	var rotated ServiceGrant
	if err := db.First(&rotated, "org_id = ? AND credential_id = ?", org.Name, mint1.CredentialID).Error; err != nil {
		t.Fatalf("re-read after rotate: %v", err)
	}
	if rotated.PasswordHash == stored.PasswordHash {
		t.Fatal("force_rotate must replace the stored hash")
	}
	if bcrypt.CompareHashAndPassword([]byte(rotated.PasswordHash), []byte(mint3.Plaintext)) != nil {
		t.Fatal("rotated hash does not match the new plaintext")
	}
	// The OLD secret no longer matches — proof the rotation actually
	// invalidated it. (Existing sessions keep working: expiry is
	// handshake-only, and nothing here touches live sessions.)
	if bcrypt.CompareHashAndPassword([]byte(rotated.PasswordHash), []byte(mint1.Plaintext)) == nil {
		t.Fatal("old plaintext must no longer match after rotation")
	}
}

// A different principal gets its OWN credential — reuse is keyed
// (org_id, principal), never org-wide.
func TestMintServiceCredentialDistinctPrincipalsPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	db.Create(&Org{Name: "svc-grant-org-multi", DatabaseName: "svc_grant_org_multi"})

	a, err := cs.MintServiceCredential("svc-grant-org-multi", "dagster:a", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint a: %v", err)
	}
	b, err := cs.MintServiceCredential("svc-grant-org-multi", "dagster:b", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint b: %v", err)
	}
	if a.CredentialID == b.CredentialID {
		t.Fatal("distinct principals must get distinct credentials")
	}
	var count int64
	db.Model(&ServiceGrant{}).Where("org_id = ?", "svc-grant-org-multi").Count(&count)
	if count != 2 {
		t.Fatalf("grant rows = %d, want 2", count)
	}
}

func TestMintServiceCredentialValidatesInputPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)

	if _, err := cs.MintServiceCredential("ghost", "d", time.Minute, false); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("unknown org must fail with gorm.ErrRecordNotFound, got %v", err)
	}
	if _, err := cs.MintServiceCredential("any", "", time.Minute, false); err == nil {
		t.Fatal("empty principal must fail (audit attribution depends on it)")
	}
	if _, err := cs.MintServiceCredential("any", "d", 0, false); err == nil {
		t.Fatal("non-positive ttl must fail")
	}
}

func TestRefreshServiceCredentialPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org-refresh", DatabaseName: "svc_grant_org_refresh"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	mint, err := cs.MintServiceCredential(org.Name, "dagster:refresh", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	// Refresh ALWAYS rotates: new plaintext, same credential_id, same row.
	refresh, err := cs.RefreshServiceCredential(org.Name, mint.CredentialID, 30*time.Minute)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if !refresh.Rotated || refresh.Plaintext == "" {
		t.Fatal("refresh must always rotate and return the new plaintext")
	}
	if refresh.CredentialID != mint.CredentialID {
		t.Fatalf("refresh credential_id = %q, want %q", refresh.CredentialID, mint.CredentialID)
	}
	var rotated ServiceGrant
	if err := db.First(&rotated, "org_id = ? AND credential_id = ?", org.Name, mint.CredentialID).Error; err != nil {
		t.Fatal(err)
	}
	if bcrypt.CompareHashAndPassword([]byte(rotated.PasswordHash), []byte(refresh.Plaintext)) != nil {
		t.Fatal("refreshed hash does not match the new plaintext")
	}
	if bcrypt.CompareHashAndPassword([]byte(rotated.PasswordHash), []byte(mint.Plaintext)) == nil {
		t.Fatal("the minted secret must no longer match after refresh")
	}
	if got := time.Until(refresh.ExpiresAt); got < 29*time.Minute || got > 31*time.Minute {
		t.Fatalf("refresh expires_in ≈ %v, want ~30m (the refresh TTL re-arms the clock)", got)
	}

	// Unknown credential_id → not found (handler maps to 404).
	if _, err := cs.RefreshServiceCredential(org.Name, "svc_eeeeeeeeeeeeeeeeeeeeeeee", time.Minute); !errors.Is(err, ErrServiceCredentialNotFound) {
		t.Fatalf("refresh of unknown credential must fail with ErrServiceCredentialNotFound, got %v", err)
	}
	// Unknown org → the credential simply isn't there (also 404-shaped).
	if _, err := cs.RefreshServiceCredential("ghost", mint.CredentialID, time.Minute); !errors.Is(err, ErrServiceCredentialNotFound) {
		t.Fatalf("refresh against a ghost org must fail with ErrServiceCredentialNotFound, got %v", err)
	}
}

// A revoked grant is terminal: refresh refuses (never resurrects), and the
// NEXT mint for the same principal creates a NEW credential rather than
// reusing the dead row.
func TestServiceGrantRevokeIsTerminalPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org-revoke", DatabaseName: "svc_grant_org_revoke"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	mint, err := cs.MintServiceCredential(org.Name, "dagster:revoke", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if err := cs.RevokeServiceGrant(org.Name, mint.CredentialID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// Revoke clears the hash server-side and stamps revoked_at; the row stays
	// for provenance.
	var revoked ServiceGrant
	if err := db.First(&revoked, "org_id = ? AND credential_id = ?", org.Name, mint.CredentialID).Error; err != nil {
		t.Fatal(err)
	}
	if revoked.RevokedAt == nil {
		t.Fatal("revoked_at must be set")
	}
	if revoked.PasswordHash != "" {
		t.Fatal("revoke must blank the password hash so a leaked grant can never come back online")
	}

	// Refresh of a revoked grant → typed error (handler maps to 410).
	if _, err := cs.RefreshServiceCredential(org.Name, mint.CredentialID, time.Minute); !errors.Is(err, ErrServiceCredentialRevoked) {
		t.Fatalf("refresh of a revoked grant must fail with ErrServiceCredentialRevoked, got %v", err)
	}

	// A new mint for the same principal ignores the revoked row and creates a
	// NEW credential.
	mint2, err := cs.MintServiceCredential(org.Name, "dagster:revoke", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint after revoke: %v", err)
	}
	if !mint2.Rotated || mint2.Plaintext == "" {
		t.Fatal("mint after revoke must create a fresh credential with a plaintext")
	}
	if mint2.CredentialID == mint.CredentialID {
		t.Fatal("mint after revoke must not resurrect the revoked credential_id")
	}

	// Unknown credential / ghost org on the revoke path.
	if err := cs.RevokeServiceGrant(org.Name, "svc_eeeeeeeeeeeeeeeeeeeeeeee"); !errors.Is(err, ErrServiceCredentialNotFound) {
		t.Fatalf("revoke of unknown credential must fail with ErrServiceCredentialNotFound, got %v", err)
	}
	if err := cs.RevokeServiceGrant("ghost", mint.CredentialID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("revoke against a ghost org must fail with gorm.ErrRecordNotFound, got %v", err)
	}
}

// An EXPIRED (but not revoked) grant is not reusable by mint (a fresh
// credential is created), while refresh can still rotate it back to life —
// expiry only refuses NEW handshakes, so both paths are free of the
// wall-clock-kill footgun.
func TestServiceGrantExpirySemanticsPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org-expiry", DatabaseName: "svc_grant_org_expiry"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	mint, err := cs.MintServiceCredential(org.Name, "dagster:expiry", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// Age the grant past expiry.
	if err := db.Model(&ServiceGrant{}).
		Where("org_id = ? AND credential_id = ?", org.Name, mint.CredentialID).
		Update("expires_at", time.Now().UTC().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}

	// Mint creates a NEW credential — the expired one is not "live".
	mint2, err := cs.MintServiceCredential(org.Name, "dagster:expiry", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint after expiry: %v", err)
	}
	if !mint2.Rotated || mint2.CredentialID == mint.CredentialID {
		t.Fatalf("mint after expiry must create a fresh credential, got %+v", mint2)
	}

	// Refresh of the EXPIRED original still rotates it (missed-window
	// recovery without a second identity).
	refresh, err := cs.RefreshServiceCredential(org.Name, mint.CredentialID, 15*time.Minute)
	if err != nil {
		t.Fatalf("refresh of expired grant: %v", err)
	}
	if !refresh.Rotated || refresh.Plaintext == "" || !refresh.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("refresh of expired grant must rotate and re-arm expiry, got %+v", refresh)
	}
}

// ListServiceGrants is flat, all-statuses, and carries NO plaintext (the hash
// is json:"-" — asserted at the handler layer in admin tests).
func TestListServiceGrantsPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org-list", DatabaseName: "svc_grant_org_list"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	live, err := cs.MintServiceCredential(org.Name, "dagster:live", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint live: %v", err)
	}
	dead, err := cs.MintServiceCredential(org.Name, "dagster:dead", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint to-be-revoked: %v", err)
	}
	if err := cs.RevokeServiceGrant(org.Name, dead.CredentialID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	grants, err := cs.ListServiceGrants(org.Name)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(grants) != 2 {
		t.Fatalf("grants = %d, want 2 (all statuses)", len(grants))
	}
	byID := map[string]ServiceGrant{}
	for _, g := range grants {
		byID[g.CredentialID] = g
	}
	if byID[live.CredentialID].RevokedAt != nil {
		t.Fatal("the live grant must not be marked revoked")
	}
	if byID[dead.CredentialID].RevokedAt == nil {
		t.Fatal("the revoked grant must appear with revoked_at set")
	}

	// Ghost org → not found (handler maps to 404).
	if _, err := cs.ListServiceGrants("ghost"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("list against a ghost org must fail with gorm.ErrRecordNotFound, got %v", err)
	}
}

// The auth half of the contract: a minted credential resolves through the
// SNAPSHOT (bcrypt hash on the grant row), expiry/revocation refuse only NEW
// handshakes, and nothing ever resolves against duckgres_org_users.
func TestServiceGrantSnapshotAuthPostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org-auth", DatabaseName: "svc_grant_org_auth"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	mint, err := cs.MintServiceCredential(org.Name, "dagster:auth", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if err := cs.ReloadSnapshot(); err != nil {
		t.Fatalf("reload snapshot: %v", err)
	}

	// Correct secret resolves — root-shaped: valid, not passthrough, no
	// project scope.
	res := cs.ResolvePostgresConnection("ducklake", org.Name, true, mint.CredentialID, mint.Plaintext)
	if !res.Valid || res.OrgID != org.Name {
		t.Fatalf("minted credential resolution = %#v, want valid", res)
	}
	if res.QueryAccess != nil {
		t.Fatalf("service credentials are root-shaped: QueryAccess = %#v, want nil", res.QueryAccess)
	}

	// Wrong secret refuses.
	if res := cs.ResolvePostgresConnection("ducklake", org.Name, true, mint.CredentialID, "wrong-secret"); res.Valid {
		t.Fatal("wrong secret must not authenticate")
	}
	// Unknown credential_id refuses.
	if res := cs.ResolvePostgresConnection("ducklake", org.Name, true, "svc_eeeeeeeeeeeeeeeeeeeeeeee", mint.Plaintext); res.Valid {
		t.Fatal("unknown credential_id must not authenticate")
	}

	// Rotation invalidates the old secret on the next snapshot.
	if _, err := cs.RefreshServiceCredential(org.Name, mint.CredentialID, 15*time.Minute); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := cs.ReloadSnapshot(); err != nil {
		t.Fatal(err)
	}
	if res := cs.ResolvePostgresConnection("ducklake", org.Name, true, mint.CredentialID, mint.Plaintext); res.Valid {
		t.Fatal("the pre-rotation secret must fail after the snapshot sees the new hash")
	}

	// Expiry refuses NEW handshakes only: rotate to a KNOWN secret, then age
	// the grant past expiry — the (known secret, expired row) pair must fail.
	refresh, err := cs.RefreshServiceCredential(org.Name, mint.CredentialID, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ServiceGrant{}).
		Where("org_id = ? AND credential_id = ?", org.Name, mint.CredentialID).
		Update("expires_at", time.Now().UTC().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if err := cs.ReloadSnapshot(); err != nil {
		t.Fatal(err)
	}
	if res := cs.ResolvePostgresConnection("ducklake", org.Name, true, mint.CredentialID, refresh.Plaintext); res.Valid {
		t.Fatal("an expired grant must refuse new connections")
	}

	// Revocation refuses all handshakes: rotate back to life (an
	// expired-but-unrevoked grant MAY be refreshed), reload so the snapshot
	// holds the live hash, then revoke.
	final, err := cs.RefreshServiceCredential(org.Name, mint.CredentialID, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.ReloadSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := cs.RevokeServiceGrant(org.Name, mint.CredentialID); err != nil {
		t.Fatal(err)
	}
	if err := cs.ReloadSnapshot(); err != nil {
		t.Fatal(err)
	}
	if res := cs.ResolvePostgresConnection("ducklake", org.Name, true, mint.CredentialID, final.Plaintext); res.Valid {
		t.Fatal("a revoked grant must refuse connections even with the last-known secret")
	}
}

// Regression pin for the Postgres µs-timestamptz rounding shape the reuse
// probe must survive: the mint writes expires_at = captured-now + ttl with Go
// wall-clock precision, but timestamptz rounds to the microsecond on
// storage — so a live-grant probe comparing expires_at > captured-now can
// misclassify a same-instant row. MintServiceCredential's probe therefore
// compares against captured-now + one µs of slack. This test fails without
// that slack whenever the rounding lands the stored expiry inside the probe
// window boundary.
func TestMintServiceCredentialMicrosecondRoundingRacePostgres(t *testing.T) {
	cs := newPostgresConfigStore(t)
	db := cs.DB()

	org := Org{Name: "svc-grant-org-us-race", DatabaseName: "svc_grant_org_us_race"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	// Two mints for the SAME principal back-to-back: the second MUST reuse the
	// first's row even when the stored µs rounding makes expires_at look a
	// hair NEWER than the reuse probe's wall clock.
	first, err := cs.MintServiceCredential(org.Name, "dagster:us-race", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("first mint: %v", err)
	}
	second, err := cs.MintServiceCredential(org.Name, "dagster:us-race", 15*time.Minute, false)
	if err != nil {
		t.Fatalf("second mint: %v", err)
	}
	if second.Rotated {
		t.Fatal("back-to-back mint must reuse even under timestamptz rounding")
	}
	if second.CredentialID != first.CredentialID {
		t.Fatalf("reuse returned %q, want the first mint's %q", second.CredentialID, first.CredentialID)
	}
	var count int64
	db.Model(&ServiceGrant{}).Where("org_id = ?", org.Name).Count(&count)
	if count != 1 {
		t.Fatalf("grant rows = %d, want exactly 1", count)
	}
}
