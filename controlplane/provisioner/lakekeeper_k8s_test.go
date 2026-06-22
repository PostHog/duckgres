//go:build kubernetes

package provisioner

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func newFakeLakekeeperClient() (*LakekeeperK8sClient, *dynamicfake.FakeDynamicClient, *kubefake.Clientset) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "lakekeeper.k8s.lakekeeper.io", Version: "v1alpha1", Kind: "Lakekeeper",
	}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "lakekeeper.k8s.lakekeeper.io", Version: "v1alpha1", Kind: "LakekeeperList",
	}, &unstructured.UnstructuredList{})
	dc := dynamicfake.NewSimpleDynamicClient(scheme)
	kc := kubefake.NewClientset()
	c := NewLakekeeperK8sClientWithClients(dc, kc, "lakekeeper")
	return c, dc, kc
}

func TestLakekeeperResourceName(t *testing.T) {
	cases := map[string]string{
		"acme":                                 "lakekeeper-acme",
		"019e417b-18c4-7a41":                   "lakekeeper-019e417b-18c4-7a41",
		"00000000-0000-0000-0000-000000000000": "lakekeeper-00000000-0000-0000-0000-000000000000",
	}
	for in, want := range cases {
		if got := LakekeeperResourceName(in); got != want {
			t.Errorf("LakekeeperResourceName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEnsureSecret_CreateThenUpdate(t *testing.T) {
	c, _, kc := newFakeLakekeeperClient()
	ctx := context.Background()
	orgID := "acme"

	data := LakekeeperSecretData{
		DBUser:             "lakekeeper_acme",
		DBPassword:         "pw-1",
		EncryptionKey:      "key-1",
		OAuth2ClientSecret: "oauth-1",
	}
	if err := c.EnsureSecret(ctx, orgID, data); err != nil {
		t.Fatalf("EnsureSecret create: %v", err)
	}
	got, err := kc.CoreV1().Secrets("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	assertSecretData(t, got, data)
	if lbl := got.Labels["duckgres/active-org"]; lbl != "acme" {
		t.Errorf("active-org label = %q, want acme", lbl)
	}

	// Idempotent update: change one field, verify it lands.
	data.DBPassword = "pw-2"
	if err := c.EnsureSecret(ctx, orgID, data); err != nil {
		t.Fatalf("EnsureSecret update: %v", err)
	}
	got, err = kc.CoreV1().Secrets("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret after update: %v", err)
	}
	assertSecretData(t, got, data)
}

func assertSecretData(t *testing.T, s *corev1.Secret, want LakekeeperSecretData) {
	t.Helper()
	get := func(k string) string { return string(s.Data[k]) }
	if get(SecretKeyDBUser) != want.DBUser {
		t.Errorf("db-user = %q, want %q", get(SecretKeyDBUser), want.DBUser)
	}
	if get(SecretKeyDBPassword) != want.DBPassword {
		t.Errorf("db-password = %q, want %q", get(SecretKeyDBPassword), want.DBPassword)
	}
	if get(SecretKeyEncryptionKey) != want.EncryptionKey {
		t.Errorf("encryption-key = %q, want %q", get(SecretKeyEncryptionKey), want.EncryptionKey)
	}
	if get(SecretKeyOAuth2ClientSecret) != want.OAuth2ClientSecret {
		t.Errorf("oauth2-client-secret = %q, want %q", get(SecretKeyOAuth2ClientSecret), want.OAuth2ClientSecret)
	}
}

func TestEnsureServiceAccount_CreateAndIdempotent(t *testing.T) {
	c, _, kc := newFakeLakekeeperClient()
	ctx := context.Background()

	if err := c.EnsureServiceAccount(ctx, "acme"); err != nil {
		t.Fatalf("EnsureServiceAccount: %v", err)
	}
	sa, err := kc.CoreV1().ServiceAccounts("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get SA: %v", err)
	}
	if sa.Labels["duckgres/active-org"] != "acme" {
		t.Errorf("active-org label = %q, want acme", sa.Labels["duckgres/active-org"])
	}
	// Re-run must not error (AlreadyExists is swallowed).
	if err := c.EnsureServiceAccount(ctx, "acme"); err != nil {
		t.Fatalf("EnsureServiceAccount re-run: %v", err)
	}
}

func TestEnsureServiceAccount_RejectsBadOrgID(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	if err := c.EnsureServiceAccount(context.Background(), "bad/org id"); err == nil {
		t.Fatal("expected error for invalid org ID")
	}
}

func TestEnsureCR_SetsServiceAccountNameWhenProvided(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	base := LakekeeperCRSpec{
		OrgID:      "acme",
		Image:      "quay.io/lakekeeper/catalog:v0.12.2",
		PGHost:     "acme-pg.local",
		PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme",
		BaseURI:    "http://lakekeeper-acme.lakekeeper.svc:8181",
	}

	// With SA set → rendered into spec.
	withSA := base
	withSA.ServiceAccountName = "lakekeeper-acme"
	if err := c.EnsureCR(ctx, withSA); err != nil {
		t.Fatalf("EnsureCR: %v", err)
	}
	got, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR: %v", err)
	}
	specMap := got.Object["spec"].(map[string]interface{})
	if specMap["serviceAccountName"] != "lakekeeper-acme" {
		t.Errorf("spec.serviceAccountName = %v, want lakekeeper-acme", specMap["serviceAccountName"])
	}
}

func TestEnsureCR_OmitsServiceAccountNameWhenEmpty(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	spec := LakekeeperCRSpec{
		OrgID:      "acme",
		Image:      "quay.io/lakekeeper/catalog:v0.12.2",
		PGHost:     "acme-pg.local",
		PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme",
		BaseURI:    "http://lakekeeper-acme.lakekeeper.svc:8181",
		// ServiceAccountName intentionally empty.
	}
	if err := c.EnsureCR(ctx, spec); err != nil {
		t.Fatalf("EnsureCR: %v", err)
	}
	got, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR: %v", err)
	}
	specMap := got.Object["spec"].(map[string]interface{})
	if _, present := specMap["serviceAccountName"]; present {
		t.Errorf("spec.serviceAccountName should be omitted when empty, got %v", specMap["serviceAccountName"])
	}
}

func TestEnsureCR_ValidatesRequiredFields(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	err := c.EnsureCR(context.Background(), LakekeeperCRSpec{OrgID: "acme"})
	if err == nil {
		t.Fatal("expected error for missing required fields")
	}
}

func TestEnsureCR_CreateAndShape(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	spec := LakekeeperCRSpec{
		OrgID:      "acme",
		Image:      "quay.io/lakekeeper/catalog:v0.12.2",
		PGHost:     "acme-pg.local",
		PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme",
		BaseURI:    "http://lakekeeper-acme.lakekeeper.svc:8181",
	}
	if err := c.EnsureCR(ctx, spec); err != nil {
		t.Fatalf("EnsureCR: %v", err)
	}

	got, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR: %v", err)
	}
	// Drill into the structure
	specMap := got.Object["spec"].(map[string]interface{})
	if specMap["image"] != spec.Image {
		t.Errorf("image = %v, want %s", specMap["image"], spec.Image)
	}
	authz := specMap["authorization"].(map[string]interface{})
	if authz["backend"] != "allowall" {
		t.Errorf("authz.backend = %v, want allowall", authz["backend"])
	}
	boot := specMap["bootstrap"].(map[string]interface{})
	if boot["enabled"] != true {
		t.Errorf("bootstrap.enabled = %v, want true", boot["enabled"])
	}
	server := specMap["server"].(map[string]interface{})
	if server["baseURI"] != spec.BaseURI {
		t.Errorf("server.baseURI = %v, want %s", server["baseURI"], spec.BaseURI)
	}
	pg := specMap["database"].(map[string]interface{})["postgres"].(map[string]interface{})
	if pg["host"] != spec.PGHost || pg["database"] != spec.PGDatabase {
		t.Errorf("pg host/db = %v/%v, want %s/%s", pg["host"], pg["database"], spec.PGHost, spec.PGDatabase)
	}
	// Resources are requests-only (no limits → Burstable).
	res := specMap["resources"].(map[string]interface{})
	reqs := res["requests"].(map[string]interface{})
	if reqs["cpu"] != lakekeeperPodCPU || reqs["memory"] != lakekeeperPodMemory {
		t.Errorf("requests cpu/mem = %v/%v, want %s/%s", reqs["cpu"], reqs["memory"], lakekeeperPodCPU, lakekeeperPodMemory)
	}
	if _, hasLimits := res["limits"]; hasLimits {
		t.Errorf("resources.limits set, want none (requests-only): %v", res["limits"])
	}
	// Two replicas.
	if specMap["replicas"] != int64(lakekeeperPodReplicas) {
		t.Errorf("replicas = %v, want %d", specMap["replicas"], lakekeeperPodReplicas)
	}
	// Prometheus scrape annotations are stamped onto the pod via podMetadata.
	ann := specMap["podMetadata"].(map[string]interface{})["annotations"].(map[string]interface{})
	if ann["prometheus.io/scrape"] != "true" {
		t.Errorf("prometheus.io/scrape = %v, want true", ann["prometheus.io/scrape"])
	}
	if ann["prometheus.io/port"] != lakekeeperMetricsPort {
		t.Errorf("prometheus.io/port = %v, want %s", ann["prometheus.io/port"], lakekeeperMetricsPort)
	}
	if ann["prometheus.io/path"] != "/metrics" {
		t.Errorf("prometheus.io/path = %v, want /metrics", ann["prometheus.io/path"])
	}
}

func TestPatchPodShape_LabelMatchedStripsLimits(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	// Two CRs for one org under different historical names, both label-tagged;
	// the first carries a stale limits block.
	names := []string{"lakekeeper-acme", "lakekeeper-a-c-m-e"}
	for i, name := range names {
		spec := map[string]interface{}{"replicas": int64(1)}
		if i == 0 {
			spec["resources"] = map[string]interface{}{
				"limits":   map[string]interface{}{"cpu": "250m", "memory": "256Mi"},
				"requests": map[string]interface{}{"cpu": "250m", "memory": "256Mi"},
			}
		}
		cr := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "lakekeeper.k8s.lakekeeper.io/v1alpha1",
			"kind":       "Lakekeeper",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": "lakekeeper",
				"labels":    map[string]interface{}{"duckgres/active-org": "acme"},
			},
			"spec": spec,
		}}
		if _, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Create(ctx, cr, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	if err := c.PatchPodShape(ctx, "acme"); err != nil {
		t.Fatalf("PatchPodShape: %v", err)
	}

	for _, name := range names {
		got, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		spec := got.Object["spec"].(map[string]interface{})
		if rv := spec["replicas"]; rv != int64(lakekeeperPodReplicas) && rv != float64(lakekeeperPodReplicas) {
			t.Errorf("%s replicas = %v, want %d", name, rv, lakekeeperPodReplicas)
		}
		res := spec["resources"].(map[string]interface{})
		if _, ok := res["limits"]; ok {
			t.Errorf("%s resources.limits still present after patch: %v", name, res["limits"])
		}
		if res["requests"].(map[string]interface{})["cpu"] != lakekeeperPodCPU {
			t.Errorf("%s requests.cpu = %v, want %s", name, res["requests"], lakekeeperPodCPU)
		}
		ann := spec["podMetadata"].(map[string]interface{})["annotations"].(map[string]interface{})
		if ann["prometheus.io/scrape"] != "true" {
			t.Errorf("%s scrape annotation not applied: %v", name, ann)
		}
	}
}

func TestEnsureCR_KubernetesAuthOff_OmitsAuthenticationBlock(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	spec := LakekeeperCRSpec{
		OrgID: "acme", Image: "img:v1", PGHost: "h", PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme", BaseURI: "http://x",
		// KubernetesAuthAudiences left empty
	}
	if err := c.EnsureCR(ctx, spec); err != nil {
		t.Fatalf("EnsureCR: %v", err)
	}
	got, _ := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	specMap := got.Object["spec"].(map[string]interface{})
	if _, present := specMap["authentication"]; present {
		t.Errorf("authentication block should be absent when KubernetesAuthAudiences is empty")
	}
}

func TestEnsureCR_KubernetesAuthOn_EmitsAuthenticationBlock(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	spec := LakekeeperCRSpec{
		OrgID: "acme", Image: "img:v1", PGHost: "h", PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme", BaseURI: "http://x",
		KubernetesAuthAudiences: []string{"lakekeeper", "lakekeeper-prod"},
	}
	if err := c.EnsureCR(ctx, spec); err != nil {
		t.Fatalf("EnsureCR: %v", err)
	}
	got, _ := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	specMap := got.Object["spec"].(map[string]interface{})
	auth, ok := specMap["authentication"].(map[string]interface{})
	if !ok {
		t.Fatalf("authentication block missing or wrong type: %T", specMap["authentication"])
	}
	k8sAuth, ok := auth["kubernetes"].(map[string]interface{})
	if !ok {
		t.Fatalf("authentication.kubernetes missing")
	}
	if k8sAuth["enabled"] != true {
		t.Errorf("authentication.kubernetes.enabled = %v, want true", k8sAuth["enabled"])
	}
	auds, ok := k8sAuth["audiences"].([]interface{})
	if !ok || len(auds) != 2 {
		t.Fatalf("audiences = %v, want [lakekeeper lakekeeper-prod]", k8sAuth["audiences"])
	}
	if auds[0] != "lakekeeper" || auds[1] != "lakekeeper-prod" {
		t.Errorf("audiences contents = %v", auds)
	}
}

func TestEnsureCR_IdempotentUpdate(t *testing.T) {
	c, dc, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	spec := LakekeeperCRSpec{
		OrgID: "acme", Image: "img:v1", PGHost: "h", PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme", BaseURI: "http://x",
	}
	if err := c.EnsureCR(ctx, spec); err != nil {
		t.Fatalf("first EnsureCR: %v", err)
	}
	spec.Image = "img:v2"
	if err := c.EnsureCR(ctx, spec); err != nil {
		t.Fatalf("second EnsureCR (update): %v", err)
	}
	// Read back the CR and confirm the update actually landed.
	got, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR after update: %v", err)
	}
	if image := got.Object["spec"].(map[string]interface{})["image"]; image != "img:v2" {
		t.Errorf("image after update = %v, want img:v2", image)
	}
}

func TestEnsureCR_RejectsEmptyOrgID(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		Image: "img:v1", PGHost: "h", PGDatabase: "lakekeeper_x",
		SecretName: "lakekeeper-x", BaseURI: "http://x",
	})
	if err == nil || !strings.Contains(err.Error(), "OrgID is required") {
		t.Fatalf("expected OrgID-required error, got: %v", err)
	}
}

func TestEnsureCR_RejectsUnsafeOrgID(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	for _, bad := range []string{"With Space", "trailing-", "-leading", "UPPER ok but space bad"} {
		err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
			OrgID: bad, Image: "img:v1", PGHost: "h", PGDatabase: "x",
			SecretName: "lakekeeper-x", BaseURI: "http://x",
		})
		if err == nil {
			t.Errorf("expected error for orgID %q, got nil", bad)
		}
	}
}

func TestEnsureSecret_RejectsUnsafeOrgID(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	err := c.EnsureSecret(context.Background(), "bad value", LakekeeperSecretData{
		DBUser: "u", DBPassword: "p", EncryptionKey: "k", OAuth2ClientSecret: "s",
	})
	if err == nil || !strings.Contains(err.Error(), "not a valid K8s label value") {
		t.Fatalf("expected label-value error, got: %v", err)
	}
}

func TestIsValidOrgIDLabel(t *testing.T) {
	cases := map[string]bool{
		"acme":                                 true,
		"019e417b-18c4-7a41-bfec-e9ae3a02deb8": true, // UUID
		"a":                                    true,
		"a.b_c-d":                              true,
		"":                                     false,
		"-leading":                             false,
		"trailing-":                            false,
		".":                                    false,
		"has space":                            false,
		// 64 chars (over the 63 limit)
		"a234567890123456789012345678901234567890123456789012345678901234": false,
	}
	for in, want := range cases {
		if got := isValidOrgIDLabel(in); got != want {
			t.Errorf("isValidOrgIDLabel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestGetCR_NotFoundReturnsNilNil(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	st, err := c.GetCR(context.Background(), "ghost-org")
	if err != nil || st != nil {
		t.Fatalf("expected (nil, nil) for absent CR, got (%v, %v)", st, err)
	}
}

func TestGetCR_ParsesStatus(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	if err := c.EnsureCR(ctx, LakekeeperCRSpec{
		OrgID: "acme", Image: "img:v1", PGHost: "h", PGDatabase: "lakekeeper_acme",
		SecretName: "lakekeeper-acme", BaseURI: "http://x",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	// Patch status on the fake. The fake dynamic client lets us mutate.
	cr, _ := c.dynamic.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{})
	cr.Object["status"] = map[string]interface{}{
		"bootstrappedAt": "2026-05-19T12:00:00Z",
		"serverID":       "abc-123",
		"readyReplicas":  int64(2),
	}
	if _, err := c.dynamic.Resource(lakekeeperGVR).Namespace("lakekeeper").Update(ctx, cr, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("inject status: %v", err)
	}

	st, err := c.GetCR(ctx, "acme")
	if err != nil {
		t.Fatalf("GetCR: %v", err)
	}
	if !st.Bootstrapped || st.ServerID != "abc-123" || st.ReadyReplicas != 2 {
		t.Errorf("status parse mismatch: %+v", st)
	}
}

func TestDeleteResources_RemovesProvisionedResources(t *testing.T) {
	c, dc, kc := newFakeLakekeeperClient()
	ctx := context.Background()
	orgID := "acme"

	// Provision the three resources DeleteForOrg is responsible for.
	if err := c.EnsureSecret(ctx, orgID, LakekeeperSecretData{
		DBUser: "lakekeeper_acme", DBPassword: "pw", EncryptionKey: "key", OAuth2ClientSecret: "oauth",
	}); err != nil {
		t.Fatalf("EnsureSecret: %v", err)
	}
	if err := c.EnsureServiceAccount(ctx, orgID); err != nil {
		t.Fatalf("EnsureServiceAccount: %v", err)
	}
	if err := c.EnsureCR(ctx, LakekeeperCRSpec{
		OrgID: orgID, Image: "quay.io/lakekeeper/catalog:v0.12.2",
		PGHost: "acme-pg.local", PGDatabase: "lakekeeper_acme", SecretName: "lakekeeper-acme",
		BaseURI: "http://lakekeeper-acme.lakekeeper.svc:8181",
	}); err != nil {
		t.Fatalf("EnsureCR: %v", err)
	}

	if err := c.DeleteCR(ctx, orgID); err != nil {
		t.Fatalf("DeleteCR: %v", err)
	}
	if err := c.DeleteSecret(ctx, orgID); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if err := c.DeleteServiceAccount(ctx, orgID); err != nil {
		t.Fatalf("DeleteServiceAccount: %v", err)
	}

	if _, err := dc.Resource(lakekeeperGVR).Namespace("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("CR still present after delete: err=%v", err)
	}
	if _, err := kc.CoreV1().Secrets("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("Secret still present after delete: err=%v", err)
	}
	if _, err := kc.CoreV1().ServiceAccounts("lakekeeper").Get(ctx, "lakekeeper-acme", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("ServiceAccount still present after delete: err=%v", err)
	}
}

func TestDeleteResources_NotFoundIsNoOp(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	ctx := context.Background()
	// Deleting resources that were never created must be a clean no-op so the
	// teardown path is safe for ducklings that never enabled Iceberg.
	if err := c.DeleteCR(ctx, "never-existed"); err != nil {
		t.Errorf("DeleteCR on missing CR: %v", err)
	}
	if err := c.DeleteSecret(ctx, "never-existed"); err != nil {
		t.Errorf("DeleteSecret on missing secret: %v", err)
	}
	if err := c.DeleteServiceAccount(ctx, "never-existed"); err != nil {
		t.Errorf("DeleteServiceAccount on missing SA: %v", err)
	}
}
