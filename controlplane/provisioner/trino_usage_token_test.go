//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReconcile_UsagePrincipalFailureStopsTenantProjection(t *testing.T) {
	h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{{
		OrgID: "usage-test", DatabaseName: "usage-test", Tier: "free", RootPasswordHash: "hash",
	}}, nil)
	h.store.rememberErr = errors.New("historical principal belongs to another org")
	if err := h.provisioner.Reconcile(context.Background()); err == nil {
		t.Fatal("principal mapping failure must stop tenant projection")
	}
	secret := getSecret(t, h.kube, TrinoAuthSecretName)
	if len(secret.Data[TrinoAuthSecretKeyPasswordDB]) != 0 {
		t.Fatal("tenant authentication must wait for durable usage attribution")
	}
}

func TestBootstrapUsageToken_ExistingCellCreatesAndAdoptsScopedCredential(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)
	ctx := context.Background()
	bundleToken, err := p.Bootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token, err := p.BootstrapUsageToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || token == bundleToken {
		t.Fatal("usage credential must be nonempty and distinct from bundle credential")
	}
	secret := getSecret(t, kc, TrinoUsageTokenSecretName)
	if string(secret.Data[TrinoUsageTokenSecretKey]) != token || secret.Immutable == nil || !*secret.Immutable {
		t.Fatal("usage token must match its immutable Secret")
	}
	second, err := p.BootstrapUsageToken(ctx)
	if err != nil || second != token {
		t.Fatalf("subsequent bootstrap must adopt credential: %v", err)
	}
}

func TestBootstrapUsageToken_DeletedCredentialFailsClosed(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)
	ctx := context.Background()
	if _, err := p.BootstrapUsageToken(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Delete(ctx, TrinoUsageTokenSecretName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.BootstrapUsageToken(ctx); err == nil {
		t.Fatal("must not regenerate a credential captured by running receivers and coordinators")
	}
}
