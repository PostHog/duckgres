//go:build kubernetes

package provisioner

import (
	"context"
	"sync"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"golang.org/x/crypto/bcrypt"
)

// newClusterSecretsTestProvisioner builds a provisioner wired with a
// fresh kubefake clientset + in-memory sentinel, returning all three so
// tests can drive Bootstrap and inspect the resulting K8s Secrets.
func newClusterSecretsTestProvisioner(t *testing.T) (*TrinoProvisioner, *kubefake.Clientset, *fakeSentinel) {
	t.Helper()
	kc := kubefake.NewClientset()
	sentinel := newFakeSentinel()
	p, err := NewTrinoProvisioner(TrinoProvisionerOpts{
		Store:             &fakeTrinoStore{},
		BootstrapSentinel: sentinel,
		Warehouses:        &fakeWarehouseStore{},
		Ducklings:         func(context.Context, string) (*DucklingStatus, error) { return nil, nil },
		Kubernetes:        kc,
		Namespace:         TrinoCustomerNamespace,
		Catalog:           &fakeCatalogClient{},
		BundleStore:       &opa.BundleStore{},
		BundleBuilder:     opa.NewBuilder(),
	})
	if err != nil {
		t.Fatalf("NewTrinoProvisioner: %v", err)
	}
	return p, kc, sentinel
}

func getSecret(t *testing.T, kc *kubefake.Clientset, name string) *corev1.Secret {
	t.Helper()
	s, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret %s: %v", name, err)
	}
	return s
}

func TestBootstrap_FirstBootGeneratesAllThree(t *testing.T) {
	p, kc, sentinel := newClusterSecretsTestProvisioner(t)

	token, err := p.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if token == "" {
		t.Fatal("Bootstrap returned an empty bundle token")
	}

	// All three Secrets exist with non-empty values.
	ic := getSecret(t, kc, TrinoInternalCommunicationSecretName)
	if len(ic.Data[TrinoInternalCommunicationSecretKey]) == 0 {
		t.Error("internal-communication shared-secret is empty")
	}
	bundle := getSecret(t, kc, TrinoOPABundleTokenSecretName)
	if string(bundle.Data[TrinoOPABundleTokenSecretKey]) != token {
		t.Error("returned bundle token doesn't match the K8s Secret value")
	}
	auth := getSecret(t, kc, TrinoAuthSecretName)
	plain := auth.Data[TrinoAuthSecretKeyAdminPassword]
	hash := auth.Data[TrinoAuthSecretKeyAdminPasswordHash]
	if len(plain) == 0 || len(hash) == 0 {
		t.Fatal("admin password/hash keys missing on trino-auth")
	}
	if err := bcrypt.CompareHashAndPassword(hash, plain); err != nil {
		t.Errorf("admin password/hash pair does not validate: %v", err)
	}

	// The write-once Secrets are immutable; trino-auth is not (it's
	// reprojected every tick with password.db/group.db).
	if ic.Immutable == nil || !*ic.Immutable {
		t.Error("internal-communication Secret should be immutable")
	}
	if bundle.Immutable == nil || !*bundle.Immutable {
		t.Error("bundle-token Secret should be immutable")
	}
	if auth.Immutable != nil && *auth.Immutable {
		t.Error("trino-auth Secret must NOT be immutable (reprojected each tick)")
	}

	// Sentinel got set.
	got, _ := sentinel.IsTrinoClusterBootstrapped(context.Background(), TrinoCustomerNamespace)
	if !got {
		t.Error("sentinel not marked bootstrapped after first boot")
	}
}

func TestBootstrap_SecondCallAdoptsWithoutOverwriting(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)

	tok1, err := p.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("Bootstrap (1): %v", err)
	}
	icVal1 := string(getSecret(t, kc, TrinoInternalCommunicationSecretName).Data[TrinoInternalCommunicationSecretKey])

	// Re-run: must return the SAME values and not rotate anything.
	tok2, err := p.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("Bootstrap (2): %v", err)
	}
	if tok1 != tok2 {
		t.Error("bundle token changed across Bootstrap calls — adoption should never rotate")
	}
	icVal2 := string(getSecret(t, kc, TrinoInternalCommunicationSecretName).Data[TrinoInternalCommunicationSecretKey])
	if icVal1 != icVal2 {
		t.Error("internal-communication secret value changed across Bootstrap calls — must never self-rotate")
	}
}

func TestBootstrap_DeletedSecretAfterBootstrapFailsLoud(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (1): %v", err)
	}

	// Simulate out-of-band deletion of the internal-communication Secret
	// AFTER the cluster is bootstrapped.
	if err := kc.CoreV1().Secrets(TrinoCustomerNamespace).
		Delete(context.Background(), TrinoInternalCommunicationSecretName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete internal-communication secret: %v", err)
	}

	// Next ensure must FAIL LOUD — regenerating would split-brain the
	// running cluster — not silently recreate with a new value.
	_, err := p.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected fail-loud after a managed Secret was deleted post-bootstrap; got nil")
	}
	// And it must NOT have been recreated.
	if _, gerr := kc.CoreV1().Secrets(TrinoCustomerNamespace).
		Get(context.Background(), TrinoInternalCommunicationSecretName, metav1.GetOptions{}); !apierrors.IsNotFound(gerr) {
		t.Error("internal-communication Secret was recreated after deletion — must fail loud, not regenerate")
	}
}

func TestBootstrap_PartialFirstBootConverges(t *testing.T) {
	// Secrets created but sentinel never marked (crash between K8s
	// writes and the Mark) — the next ensure must adopt the existing
	// Secrets, mark the sentinel, and succeed (not fail loud, not
	// rotate).
	p, kc, sentinel := newClusterSecretsTestProvisioner(t)

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (1): %v", err)
	}
	icVal1 := string(getSecret(t, kc, TrinoInternalCommunicationSecretName).Data[TrinoInternalCommunicationSecretKey])

	// Roll the sentinel back to simulate "Secrets exist, Mark didn't land".
	sentinel.mu.Lock()
	sentinel.bootstrapped = map[string]bool{}
	sentinel.mu.Unlock()

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (converge): %v", err)
	}
	icVal2 := string(getSecret(t, kc, TrinoInternalCommunicationSecretName).Data[TrinoInternalCommunicationSecretKey])
	if icVal1 != icVal2 {
		t.Error("partial-bootstrap convergence rotated the internal-communication secret; must adopt existing")
	}
	got, _ := sentinel.IsTrinoClusterBootstrapped(context.Background(), TrinoCustomerNamespace)
	if !got {
		t.Error("sentinel not re-marked after convergence")
	}
}

func TestBootstrap_AdminPairMismatchFailsLoud(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (1): %v", err)
	}

	// Corrupt the admin pair out-of-band: replace the hash with a bcrypt
	// of a DIFFERENT password (still a valid bcrypt, but not of the
	// stored plaintext).
	wrong, _ := bcrypt.GenerateFromPassword([]byte("a-different-password"), bcrypt.DefaultCost)
	auth := getSecret(t, kc, TrinoAuthSecretName)
	auth.Data[TrinoAuthSecretKeyAdminPasswordHash] = wrong
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Update(context.Background(), auth, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update auth secret: %v", err)
	}

	_, err := p.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected fail-loud on an inconsistent admin password/hash pair; got nil")
	}
}

func TestBootstrap_AdminKeyLossRegeneratesNotFailLoud(t *testing.T) {
	// Unlike the internal-communication secret, the admin pair has no
	// external consumer (the provisioner owns both sides), so losing it
	// post-bootstrap must SELF-HEAL by regenerating — not wedge into
	// fail-loud. Models a stray wholesale write of trino-auth during a
	// rolling upgrade that dropped the admin keys.
	p, kc, _ := newClusterSecretsTestProvisioner(t)
	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap (1): %v", err)
	}

	// Remove just the admin keys from trino-auth (Secret still exists).
	auth := getSecret(t, kc, TrinoAuthSecretName)
	delete(auth.Data, TrinoAuthSecretKeyAdminPassword)
	delete(auth.Data, TrinoAuthSecretKeyAdminPasswordHash)
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Update(context.Background(), auth, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("strip admin keys: %v", err)
	}

	// Next ensure must regenerate the pair and succeed.
	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("expected admin-key loss to self-heal, got: %v", err)
	}
	healed := getSecret(t, kc, TrinoAuthSecretName)
	if err := bcrypt.CompareHashAndPassword(
		healed.Data[TrinoAuthSecretKeyAdminPasswordHash],
		healed.Data[TrinoAuthSecretKeyAdminPassword]); err != nil {
		t.Errorf("regenerated admin pair does not validate: %v", err)
	}
}

func TestBootstrap_PromotesPreexistingMutableWriteOnceSecret(t *testing.T) {
	// If a write-once Secret was pre-created mutably (operator, or older
	// code), adoption must promote it to immutable so the write-once
	// guarantee actually holds.
	p, kc, _ := newClusterSecretsTestProvisioner(t)
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: TrinoInternalCommunicationSecretName, Namespace: TrinoCustomerNamespace},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{TrinoInternalCommunicationSecretKey: []byte("pre-existing-mutable-value")},
		// Immutable deliberately unset.
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("pre-create mutable secret: %v", err)
	}

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	ic := getSecret(t, kc, TrinoInternalCommunicationSecretName)
	if ic.Immutable == nil || !*ic.Immutable {
		t.Error("adopted pre-existing write-once Secret was not promoted to immutable")
	}
	// And the pre-existing value was adopted, not overwritten.
	if string(ic.Data[TrinoInternalCommunicationSecretKey]) != "pre-existing-mutable-value" {
		t.Error("adoption overwrote the pre-existing value")
	}
}

func TestBootstrap_ConcurrentReplicasConverge(t *testing.T) {
	// N "replicas" sharing one kubefake clientset + one sentinel race to
	// first-boot. With no DB lock, convergence rests on K8s
	// Create/AlreadyExists + adopt-the-winner. Assert every replica ends
	// on the SAME admin pair, bundle token, and internal-comm value, and
	// that the durable Secrets match what every replica returned (no
	// loser-overwrites-winner divergence).
	const replicas = 16
	kc := kubefake.NewClientset()
	sentinel := newFakeSentinel()
	newReplica := func() *TrinoProvisioner {
		p, err := NewTrinoProvisioner(TrinoProvisionerOpts{
			Store:             &fakeTrinoStore{},
			BootstrapSentinel: sentinel,
			Warehouses:        &fakeWarehouseStore{},
			Ducklings:         func(context.Context, string) (*DucklingStatus, error) { return nil, nil },
			Kubernetes:        kc, // shared
			Namespace:         TrinoCustomerNamespace,
			Catalog:           &fakeCatalogClient{},
			BundleStore:       &opa.BundleStore{},
			BundleBuilder:     opa.NewBuilder(),
		})
		if err != nil {
			t.Fatalf("NewTrinoProvisioner: %v", err)
		}
		return p
	}

	tokens := make([]string, replicas)
	errs := make([]error, replicas)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := newReplica()
			<-start
			tokens[idx], errs[idx] = p.Bootstrap(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d Bootstrap failed: %v", i, err)
		}
	}
	// All bundle tokens identical = everyone adopted the same winner.
	for i := 1; i < replicas; i++ {
		if tokens[i] != tokens[0] {
			t.Fatalf("bundle token diverged: replica %d=%q vs replica 0=%q", i, tokens[i], tokens[0])
		}
	}
	// Durable Secrets are internally consistent and match the returned token.
	if got := string(getSecret(t, kc, TrinoOPABundleTokenSecretName).Data[TrinoOPABundleTokenSecretKey]); got != tokens[0] {
		t.Errorf("durable bundle token %q != returned %q", got, tokens[0])
	}
	auth := getSecret(t, kc, TrinoAuthSecretName)
	if err := bcrypt.CompareHashAndPassword(
		auth.Data[TrinoAuthSecretKeyAdminPasswordHash],
		auth.Data[TrinoAuthSecretKeyAdminPassword]); err != nil {
		t.Errorf("durable admin pair does not validate after concurrent boot: %v", err)
	}
	// Exactly one internal-comm secret value exists (no torn set).
	if len(getSecret(t, kc, TrinoInternalCommunicationSecretName).Data[TrinoInternalCommunicationSecretKey]) == 0 {
		t.Error("internal-communication secret empty after concurrent boot")
	}
}

func TestBootstrap_TransientSentinelErrorRetries(t *testing.T) {
	p, _, sentinel := newClusterSecretsTestProvisioner(t)
	sentinel.failRead = context.DeadlineExceeded

	_, err := p.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error when the sentinel read fails transiently")
	}
	// The provisioner must NOT have assumed not-bootstrapped and written
	// Secrets — failing closed on an indeterminate sentinel is the point.
}

// --------------------------------------------------------------------------
// Observer credential: the admin console's read-only Trino identity.
//
// It shares trino-auth and the regenerate-if-missing semantics with the
// admin pair (both are provisioner-owned with no external consumer, so
// losing one self-heals within a password-file refresh rather than
// wedging), but it must be a genuinely DISTINCT credential: the whole
// point of the split is that leaking one does not yield the other's
// authority. See opa.ObserverPrincipal.
// --------------------------------------------------------------------------

func TestBootstrap_MintsObserverCredential(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	auth := getSecret(t, kc, TrinoAuthSecretName)
	plain := auth.Data[TrinoAuthSecretKeyObserverPassword]
	hash := auth.Data[TrinoAuthSecretKeyObserverPasswordHash]
	if len(plain) == 0 || len(hash) == 0 {
		t.Fatal("observer password/hash keys missing on trino-auth -- the admin console cannot authenticate to the coordinator without them")
	}
	if err := bcrypt.CompareHashAndPassword(hash, plain); err != nil {
		t.Errorf("observer password/hash pair does not validate: %v", err)
	}

	// The accessor the control plane's admin wiring reads.
	user, pass := p.ObserverCredential()
	if user != opa.ObserverPrincipal {
		t.Errorf("ObserverCredential user = %q, want %q", user, opa.ObserverPrincipal)
	}
	if pass != string(plain) {
		t.Error("ObserverCredential password does not match the projected Secret value")
	}
}

// TestBootstrap_ObserverCredentialIsDistinctFromAdmin is the split itself.
// If these ever coincide, the console's read-only identity silently carries
// CREATE/DROP CATALOG authority and the security bargain in
// opa.ObserverPrincipal is void.
func TestBootstrap_ObserverCredentialIsDistinctFromAdmin(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	auth := getSecret(t, kc, TrinoAuthSecretName)
	adminPlain := string(auth.Data[TrinoAuthSecretKeyAdminPassword])
	observerPlain := string(auth.Data[TrinoAuthSecretKeyObserverPassword])
	if adminPlain == "" || observerPlain == "" {
		t.Fatal("expected both credentials to be minted")
	}
	if adminPlain == observerPlain {
		t.Error("observer and admin passwords are identical -- they must be independent credentials")
	}
	if opa.ObserverPrincipal == opa.AdminPrincipal {
		t.Error("observer and admin principals must be distinct usernames")
	}
}

// TestBootstrap_AdoptsExistingObserverCredential: a second Bootstrap (a
// restart, or another replica) must converge on the durable pair rather
// than rotating it out from under a running console.
func TestBootstrap_AdoptsExistingObserverCredential(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("first Bootstrap: %v", err)
	}
	first := string(getSecret(t, kc, TrinoAuthSecretName).Data[TrinoAuthSecretKeyObserverPassword])

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("second Bootstrap: %v", err)
	}
	second := string(getSecret(t, kc, TrinoAuthSecretName).Data[TrinoAuthSecretKeyObserverPassword])

	if first != second {
		t.Error("second Bootstrap rotated the observer password; it must adopt the durable pair")
	}
	if _, pass := p.ObserverCredential(); pass != second {
		t.Error("ObserverCredential drifted from the Secret after re-Bootstrap")
	}
}

// TestBootstrap_ObserverPairMismatchFailsLoud mirrors the admin behaviour:
// a hand-edited half-pair is a configuration error, not something to paper
// over by regenerating (which would rotate a credential someone is using).
func TestBootstrap_ObserverPairMismatchFailsLoud(t *testing.T) {
	p, kc, sentinel := newClusterSecretsTestProvisioner(t)
	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	auth := getSecret(t, kc, TrinoAuthSecretName)
	auth.Data[TrinoAuthSecretKeyObserverPassword] = []byte("not-the-password-that-hash-is-for")
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).
		Update(context.Background(), auth, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	if ok, _ := sentinel.IsTrinoClusterBootstrapped(context.Background(), TrinoCustomerNamespace); !ok {
		t.Fatal("expected the sentinel to be set after the first boot")
	}

	if _, err := p.Bootstrap(context.Background()); err == nil {
		t.Error("Bootstrap accepted an inconsistent observer password/hash pair; it must fail loud")
	}
}

// TestBootstrap_ObserverKeyLossRegenerates: unlike the internal-communication
// secret (env-projected into long-lived pods, so regeneration would
// split-brain the cluster), the observer pair has no external consumer.
// Losing it must self-heal on the next tick.
func TestBootstrap_ObserverKeyLossRegenerates(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)
	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	auth := getSecret(t, kc, TrinoAuthSecretName)
	delete(auth.Data, TrinoAuthSecretKeyObserverPassword)
	delete(auth.Data, TrinoAuthSecretKeyObserverPasswordHash)
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).
		Update(context.Background(), auth, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update secret: %v", err)
	}

	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap after observer key loss: %v", err)
	}
	regenerated := getSecret(t, kc, TrinoAuthSecretName)
	if len(regenerated.Data[TrinoAuthSecretKeyObserverPassword]) == 0 {
		t.Error("observer credential was not regenerated after key loss")
	}
	if err := bcrypt.CompareHashAndPassword(
		regenerated.Data[TrinoAuthSecretKeyObserverPasswordHash],
		regenerated.Data[TrinoAuthSecretKeyObserverPassword],
	); err != nil {
		t.Errorf("regenerated observer pair does not validate: %v", err)
	}
}
