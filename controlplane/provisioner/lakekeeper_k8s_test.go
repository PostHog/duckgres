//go:build kubernetes

package provisioner

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
		"acme":                 "lakekeeper-acme",
		"019e417b-18c4-7a41":   "lakekeeper-019e417b18c47a41",
		"00000000-0000-0000-0000-000000000000": "lakekeeper-00000000000000000000000000000000",
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
	// kubefake honors StringData by populating Data with the same bytes.
	get := func(k string) string {
		if v, ok := s.StringData[k]; ok {
			return v
		}
		return string(s.Data[k])
	}
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
		Image:      "quay.io/lakekeeper/catalog:0.11.6",
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
		"acme":                                    true,
		"019e417b-18c4-7a41-bfec-e9ae3a02deb8":    true, // UUID
		"a":                                       true,
		"a.b_c-d":                                 true,
		"":                                        false,
		"-leading":                                false,
		"trailing-":                               false,
		".":                                       false,
		"has space":                               false,
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
