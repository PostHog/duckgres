//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// killSwitchRouter mounts the live API with the given fakes (no RoleGate — that
// gating is exercised separately in TestKillSwitchRequiresAdmin).
func killSwitchRouter(live LiveInfo, fetcher PeerFetcher, users UserAdmin) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	registerLiveAPI(e.Group("/api/v1"), live, fetcher, users)
	return e
}

func doPost(t *testing.T, r *gin.Engine, path string) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
	var body map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &body)
	}
	return w.Code, body
}

// TestKillUserSessionsFansOut: the kill endpoint kills locally AND sums peer
// counts, reporting cluster-wide totals.
func TestKillUserSessionsFansOut(t *testing.T) {
	local := &fakeLiveInfo{killedPerUser: 2}
	peerBody, _ := json.Marshal(map[string]any{"killed": 3})
	fetcher := &fakePeerFetcher{postByPath: map[string][][]byte{
		"/api/v1/orgs/acme/users/bob/kill": {peerBody},
	}}
	r := killSwitchRouter(local, fetcher, nil)

	code, body := doPost(t, r, "/api/v1/orgs/acme/users/bob/kill")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%v)", code, body)
	}
	if got := body["killed"]; got != float64(5) {
		t.Fatalf("killed = %v, want 5 (2 local + 3 peer)", got)
	}
	if got := body["cp_total"]; got != float64(2) {
		t.Fatalf("cp_total = %v, want 2", got)
	}
	if len(local.killUserCalls) != 1 || local.killUserCalls[0] != (killUserCall{"acme", "bob"}) {
		t.Fatalf("local KillUserSessions calls = %+v, want one (acme,bob)", local.killUserCalls)
	}
	if fetcher.postCallCount() != 1 {
		t.Fatalf("expected one POST fan-out, got %d", fetcher.postCallCount())
	}
}

// TestKillUserSessionsScopeLocal: a peer (scope=local) call kills only locally
// and must NOT fan out again (recursion guard).
func TestKillUserSessionsScopeLocal(t *testing.T) {
	local := &fakeLiveInfo{killedPerUser: 4}
	fetcher := &fakePeerFetcher{}
	r := killSwitchRouter(local, fetcher, nil)

	code, body := doPost(t, r, "/api/v1/orgs/acme/users/bob/kill?scope=local")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if got := body["killed"]; got != float64(4) {
		t.Fatalf("killed = %v, want 4 (local only)", got)
	}
	if fetcher.postCallCount() != 0 {
		t.Fatalf("scope=local must not fan out, but PostPeers ran %d times", fetcher.postCallCount())
	}
}

// TestDisableUserPersistsReloadsKillsAndFansOut: the disable endpoint persists
// the flag, reloads the snapshot, kills locally, and fans out to peers.
func TestDisableUserPersistsReloadsKillsAndFansOut(t *testing.T) {
	local := &fakeLiveInfo{killedPerUser: 1}
	peerBody, _ := json.Marshal(map[string]any{"killed": 2})
	fetcher := &fakePeerFetcher{postByPath: map[string][][]byte{
		"/api/v1/orgs/acme/users/bob/disable": {peerBody},
	}}
	users := &fakeUserAdmin{}
	r := killSwitchRouter(local, fetcher, users)

	code, body := doPost(t, r, "/api/v1/orgs/acme/users/bob/disable")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%v)", code, body)
	}
	if body["disabled"] != true {
		t.Fatalf("disabled = %v, want true", body["disabled"])
	}
	if got := body["killed"]; got != float64(3) {
		t.Fatalf("killed = %v, want 3 (1 local + 2 peer)", got)
	}
	if !users.isDisabled("acme", "bob") {
		t.Fatalf("user was not persisted disabled")
	}
	if users.reloads < 1 {
		t.Fatalf("expected a snapshot reload, got %d", users.reloads)
	}
	if fetcher.postCallCount() != 1 {
		t.Fatalf("expected one disable fan-out, got %d", fetcher.postCallCount())
	}
}

// TestDisableUserScopeLocalReloadsAndKillsOnly: a peer disable call reloads +
// kills locally without re-persisting or re-fanning-out (the primary already
// wrote the DB row).
func TestDisableUserScopeLocalReloadsAndKillsOnly(t *testing.T) {
	local := &fakeLiveInfo{killedPerUser: 5}
	fetcher := &fakePeerFetcher{}
	users := &fakeUserAdmin{}
	r := killSwitchRouter(local, fetcher, users)

	code, body := doPost(t, r, "/api/v1/orgs/acme/users/bob/disable?scope=local")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if got := body["killed"]; got != float64(5) {
		t.Fatalf("killed = %v, want 5", got)
	}
	if users.isDisabled("acme", "bob") {
		t.Fatalf("scope=local disable must NOT persist the flag again")
	}
	if users.reloads != 1 {
		t.Fatalf("expected exactly one reload, got %d", users.reloads)
	}
	if fetcher.postCallCount() != 0 {
		t.Fatalf("scope=local disable must not fan out, ran %d", fetcher.postCallCount())
	}
}

// TestEnableUserClearsFlag: enable persists disabled=false and reloads.
func TestEnableUserClearsFlag(t *testing.T) {
	users := &fakeUserAdmin{disabled: map[string]bool{"acme/bob": true}}
	r := killSwitchRouter(&fakeLiveInfo{}, &fakePeerFetcher{}, users)

	code, body := doPost(t, r, "/api/v1/orgs/acme/users/bob/enable")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%v)", code, body)
	}
	if body["disabled"] != false {
		t.Fatalf("disabled = %v, want false", body["disabled"])
	}
	if users.isDisabled("acme", "bob") {
		t.Fatalf("user should be enabled (disabled=false)")
	}
}

// TestDisableUserNotFound: an unknown user yields 404 and does not fan out.
func TestDisableUserNotFound(t *testing.T) {
	fetcher := &fakePeerFetcher{}
	users := &fakeUserAdmin{notFound: true}
	r := killSwitchRouter(&fakeLiveInfo{}, fetcher, users)

	code, _ := doPost(t, r, "/api/v1/orgs/acme/users/ghost/disable")
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown user, got %d", code)
	}
	if fetcher.postCallCount() != 0 {
		t.Fatalf("a 404 disable must not fan out")
	}
}

// TestDisableEnableUnavailableWithoutUserAdmin: without a UserAdmin wired (nil),
// disable/enable degrade to 503 rather than panicking.
func TestDisableEnableUnavailableWithoutUserAdmin(t *testing.T) {
	r := killSwitchRouter(&fakeLiveInfo{}, &fakePeerFetcher{}, nil)
	for _, p := range []string{
		"/api/v1/orgs/acme/users/bob/disable",
		"/api/v1/orgs/acme/users/bob/enable",
	} {
		code, _ := doPost(t, r, p)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("%s without UserAdmin: expected 503, got %d", p, code)
		}
	}
}

// TestKillSwitchRequiresAdmin: all three actions are POSTs, so RoleGate (applied
// at the group level in production) rejects a viewer with 403.
func TestKillSwitchRequiresAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	grp := r.Group("/api/v1")
	grp.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Email: "viewer@posthog.com", Role: RoleViewer})
	})
	grp.Use(RoleGate())
	registerLiveAPI(grp, &fakeLiveInfo{}, &fakePeerFetcher{}, &fakeUserAdmin{})

	for _, p := range []string{
		"/api/v1/orgs/acme/users/bob/kill",
		"/api/v1/orgs/acme/users/bob/disable",
		"/api/v1/orgs/acme/users/bob/enable",
	} {
		code, _ := doPost(t, r, p)
		if code != http.StatusForbidden {
			t.Fatalf("%s as viewer: expected 403, got %d", p, code)
		}
	}
}
