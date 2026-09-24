package configstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoServiceCredentialLifecyclePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	seedTrinoOrg(t, store, "other")
	for _, org := range []string{"acme", "other"} {
		if err := store.EnableTrino(org, configstore.TrinoSettings{Tier: "scale", DefaultCellID: "registered:cell-a"}); err != nil {
			t.Fatal(err)
		}
	}
	grant, err := store.MintServiceCredential("acme", "worker:test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	username := "acmedb." + grant.CredentialID
	ctx := context.Background()
	identity, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, grant.Plaintext)
	if err != nil || identity.User != username || identity.Groups[0] != "org_acmedb" || identity.Groups[1] != "tier_scale" {
		t.Fatalf("identity=%+v, err=%v", identity, err)
	}
	for _, user := range []string{"otherdb." + grant.CredentialID, "acmedb.svc_000000000000000000000000", "acmedb.root", grant.CredentialID} {
		if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", user, grant.Plaintext); !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
			t.Fatalf("%s: err=%v", user, err)
		}
	}
	for _, cellID := range []string{"registered:cell-b", "", "unknown"} {
		if _, err := store.ValidateTrinoServiceCredential(ctx, cellID, username, grant.Plaintext); !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
			t.Fatalf("wrong cell %q authenticates: %v", cellID, err)
		}
	}
	for _, cellID := range []string{"registered:cell-b", "", "registered:cell-a"} {
		if err := store.DB().Model(&configstore.ManagedWarehouseTrino{}).Where("org_id = ?", "acme").Update("trino_cell_id", cellID).Error; err != nil {
			t.Fatal(err)
		}
		_, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, grant.Plaintext)
		if cellID == "registered:cell-a" {
			if err != nil {
				t.Fatalf("restored cell denied: %v", err)
			}
		} else if !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
			t.Fatalf("cached password bypassed reassignment to %q: %v", cellID, err)
		}
		if cellID != "" {
			if _, err := store.ValidateTrinoServiceCredential(ctx, cellID, username, grant.Plaintext); err != nil {
				t.Fatalf("current assigned cell denied: %v", err)
			}
		}
	}
	renewed, err := store.RenewServiceCredential("acme", grant.CredentialID, 2*time.Minute)
	if err != nil || renewed.Plaintext != "" || renewed.CredentialID != grant.CredentialID || !renewed.ExpiresAt.After(grant.ExpiresAt) {
		t.Fatalf("renewal failed: error=%v", err)
	}
	if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, grant.Plaintext); err != nil {
		t.Fatalf("renewal rotated secret: %v", err)
	}
	rotated, err := store.RefreshServiceCredential("acme", grant.CredentialID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, grant.Plaintext); !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
		t.Fatalf("old secret still authenticates: %v", err)
	}
	if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, rotated.Plaintext); err != nil {
		t.Fatal(err)
	}
	if err := store.DisableTrino("acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, rotated.Plaintext); !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
		t.Fatalf("disabled org authenticates: %v", err)
	}
	if err := store.EnableTrino("acme", configstore.TrinoSettings{Tier: "growth"}); err != nil {
		t.Fatal(err)
	}
	identity, err = store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, rotated.Plaintext)
	if err != nil || identity.Groups[1] != "tier_growth" {
		t.Fatalf("live tier not used: %+v, %v", identity, err)
	}
	if err := store.RevokeServiceGrant("acme", grant.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenewServiceCredential("acme", grant.CredentialID, time.Minute); !errors.Is(err, configstore.ErrServiceCredentialRevoked) {
		t.Fatalf("revoked renewal: %v", err)
	}
	if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", username, rotated.Plaintext); !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
		t.Fatalf("revoked grant authenticates: %v", err)
	}
	grant, err = store.MintServiceCredential("acme", "worker:expiry", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Model(&configstore.ServiceGrant{}).Where("org_id = ? AND credential_id = ?", "acme", grant.CredentialID).Update("expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateTrinoServiceCredential(ctx, "registered:cell-a", "acmedb."+grant.CredentialID, grant.Plaintext); !errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
		t.Fatalf("expired grant authenticates: %v", err)
	}
	if _, err := store.RenewServiceCredential("acme", grant.CredentialID, time.Minute); !errors.Is(err, configstore.ErrServiceCredentialExpired) {
		t.Fatalf("expired renewal: %v", err)
	}
}
