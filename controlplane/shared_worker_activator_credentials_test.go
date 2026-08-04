//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// testWarehouseConfig returns a minimal config-store warehouse whose metadata
// credentials resolve against the fake clientset's "metadata-creds" secret.
func testWarehouseConfig() *configstore.ManagedWarehouseConfig {
	return &configstore.ManagedWarehouseConfig{
		OrgID: "org-test",
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Endpoint:     "pg.test.local",
			Port:         5432,
			DatabaseName: "ducklake",
			Username:     "duck",
		},
		MetadataStoreCredentials: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      "metadata-creds",
			Key:       "password",
		},
		S3: configstore.ManagedWarehouseS3{
			Region: "us-east-1",
			Bucket: "test-bucket",
		},
	}
}

func testMetadataSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "metadata-creds", Namespace: "duckgres"},
		Data:       map[string][]byte{"password": []byte("hunter2")},
	}
}

// The "aws" provider path brokers STS credentials, and those expire: the
// returned expiration is what the credential-refresh scheduler keys on to
// keep the worker's DuckDB secret fresh. A nil expiry here means the
// scheduler never refreshes the worker and any session outliving the STS
// token dies with ExpiredToken mid-query.
func TestBuildDuckLakeConfigFromConfigStoreAWSProviderReturnsExpiry(t *testing.T) {
	expiration := time.Date(2026, 6, 11, 13, 0, 0, 0, time.UTC)
	broker := newSTSBroker(func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		return &AssumedCredentials{
			AccessKeyID:     "ASIATEST",
			SecretAccessKey: "secret",
			SessionToken:    "token",
			Expiration:      expiration,
		}, nil
	})
	a := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(testMetadataSecret()),
		defaultNamespace: "duckgres",
		stsBroker:        broker,
	}
	warehouse := testWarehouseConfig()
	warehouse.S3.Provider = "aws"
	warehouse.WorkerIdentity = configstore.ManagedWarehouseWorkerIdentity{
		IAMRoleARN: "arn:aws:iam::123:role/duckling-test",
	}

	dl, expiresAt, err := a.buildDuckLakeConfigFromConfigStore(context.Background(), warehouse)
	if err != nil {
		t.Fatalf("buildDuckLakeConfigFromConfigStore: %v", err)
	}
	if dl.S3AccessKey != "ASIATEST" || dl.S3SessionToken != "token" {
		t.Errorf("expected STS-brokered creds on config, got access_key=%q session_token=%q", dl.S3AccessKey, dl.S3SessionToken)
	}
	if expiresAt == nil {
		t.Fatal("expected non-nil credential expiry for STS-brokered aws provider; nil disables the credential-refresh scheduler for this worker")
	}
	if !expiresAt.Equal(expiration) {
		t.Errorf("expiresAt = %v, want %v", *expiresAt, expiration)
	}
}

// Static secret-ref credentials have no known expiration — the scheduler
// must skip them, so the path must return a nil expiry.
func TestBuildDuckLakeConfigFromConfigStoreStaticCredsNilExpiry(t *testing.T) {
	s3Secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "duckgres"},
		Data: map[string][]byte{
			"credentials": []byte(`{"access_key_id":"AKIATEST","secret_access_key":"secret"}`),
		},
	}
	a := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(testMetadataSecret(), s3Secret),
		defaultNamespace: "duckgres",
	}
	warehouse := testWarehouseConfig()
	warehouse.S3Credentials = configstore.SecretRef{
		Namespace: "duckgres",
		Name:      "s3-creds",
		Key:       "credentials",
	}

	dl, expiresAt, err := a.buildDuckLakeConfigFromConfigStore(context.Background(), warehouse)
	if err != nil {
		t.Fatalf("buildDuckLakeConfigFromConfigStore: %v", err)
	}
	if dl.S3AccessKey != "AKIATEST" {
		t.Errorf("expected static creds, got access_key=%q", dl.S3AccessKey)
	}
	if expiresAt != nil {
		t.Errorf("static creds must return nil expiry (scheduler skip), got %v", *expiresAt)
	}
}

// Pin the capture-point freshness floor: any credentials the broker hands
// out (cached or fresh) must retain more validity than the refresh
// scheduler's lookahead. If served creds could be closer to expiry than the
// lookahead, a refresh push would stamp an expiry already inside the "due"
// window and the scheduler would re-push identical creds every tick; worse,
// a statement starting on those creds would have less runway than the
// scheduler is designed to guarantee.
func TestSTSBrokerServedCredsAlwaysOutliveSchedulerLookahead(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)}
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		return &AssumedCredentials{
			AccessKeyID: "AKID",
			Expiration:  clock.Now().Add(stsSessionDuration),
		}, nil
	})

	// Sweep a full session duration in 1-minute steps; at every point the
	// served creds must outlive the scheduler lookahead.
	for i := 0; i <= int(stsSessionDuration/time.Minute); i++ {
		creds, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
		if err != nil {
			t.Fatalf("AssumeRole at +%dm: %v", i, err)
		}
		if remaining := creds.Expiration.Sub(clock.Now()); remaining <= credentialRefreshLookahead {
			t.Fatalf("at +%dm served creds have %v remaining, must exceed scheduler lookahead %v",
				i, remaining, credentialRefreshLookahead)
		}
		clock.Advance(time.Minute)
	}
}
