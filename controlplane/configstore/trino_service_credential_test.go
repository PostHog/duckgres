package configstore

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestTrinoServiceCredentialLiveState(t *testing.T) {
	const username = "acme.svc_0123456789abcdef01234567"
	const password = "synthetic-secret"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	row := trinoServiceCredentialRecord{Enabled: true, Tier: "scale", PasswordHash: string(hash), ExpiresAt: time.Now().Add(time.Hour)}
	identity, err := trinoServiceIdentity(username, row, password)
	if err != nil || identity.User != username || identity.Groups[0] != "org_acme" || identity.Groups[1] != "tier_scale" {
		t.Fatalf("identity=%+v error=%v", identity, err)
	}
	for _, tc := range []struct {
		name     string
		change   func(*trinoServiceCredentialRecord)
		password string
	}{
		{"wrong secret after cached success", func(_ *trinoServiceCredentialRecord) {}, "wrong"},
		{"revoked after cached success", func(r *trinoServiceCredentialRecord) { now := time.Now(); r.RevokedAt = &now }, password},
		{"expired after cached success", func(r *trinoServiceCredentialRecord) { r.ExpiresAt = time.Now().Add(-time.Second) }, password},
		{"disabled after cached success", func(r *trinoServiceCredentialRecord) { r.Enabled = false }, password},
		{"rotated after cached success", func(r *trinoServiceCredentialRecord) {
			h, _ := bcrypt.GenerateFromPassword([]byte("replacement"), bcrypt.MinCost)
			r.PasswordHash = string(h)
		}, password},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := row
			tc.change(&changed)
			if _, err := trinoServiceIdentity(username, changed, tc.password); !errors.Is(err, ErrTrinoServiceCredentialDenied) {
				t.Fatalf("error=%v, want denied", err)
			}
		})
	}
	row.Tier = "growth"
	identity, err = trinoServiceIdentity(username, row, password)
	if err != nil || identity.Groups[1] != "tier_growth" {
		t.Fatalf("tier change not observed: %+v, %v", identity, err)
	}
}

func TestTrinoServiceCredentialUsername(t *testing.T) {
	for _, username := range []string{"svc_0123456789abcdef01234567", "acme.root", "acme.other.svc_0123456789abcdef01234567", "acme.svc_0123", "acme.svc_0123456789ABCDEF01234567"} {
		if trinoServiceCredentialUsername.MatchString(username) {
			t.Fatalf("accepted %q", username)
		}
	}
}

func TestTrinoServiceCredentialOwnership(t *testing.T) {
	owners := NewTrinoPrincipalOwners([]TrinoEnabledOrg{{OrgID: "org-a", DatabaseName: "acme"}})
	for _, tc := range []struct{ user, org string }{
		{"acme.svc_0123456789abcdef01234567", "org-a"},
		{"other.svc_0123456789abcdef01234567", ""},
		{"acme.other.svc_0123456789abcdef01234567", ""},
		{"acme.svc_short", ""},
	} {
		if got := owners.OrgID(tc.user); got != tc.org {
			t.Fatalf("%s: org=%q, want %q", tc.user, got, tc.org)
		}
	}
}
