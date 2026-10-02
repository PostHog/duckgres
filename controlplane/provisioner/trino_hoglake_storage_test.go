//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/posthog/duckgres/controlplane/configstore"
	"io"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestTrinoHoglakeRequiresStorageReadiness(t *testing.T) {
	h, client, _, _ := newHoglakeStateHarness(t)
	h.provisioner.hoglakeStorageCheck = nil
	err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false, nil)
	if err == nil || len(h.catalog.created) != 0 {
		t.Fatalf("catalog published without a tenant storage readiness check: err=%v created=%v", err, h.catalog.created)
	}
}

type fakeHoglakeStorage struct {
	t       *testing.T
	fail    string
	calls   []string
	keys    []string
	corrupt bool
	cancel  context.CancelFunc
}

func (f *fakeHoglakeStorage) PutObject(ctx context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.calls = append(f.calls, "put")
	f.keys = append(f.keys, aws.ToString(input.Key))
	if aws.ToString(input.Bucket) != "example-bucket" || !strings.HasPrefix(aws.ToString(input.Key), "trino/warehouse-a/.duckgres-readiness/") || input.Tagging != nil {
		f.t.Fatalf("unsafe readiness upload: %v", input)
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second {
		f.t.Fatal("unbounded storage attempt")
	}
	if f.cancel != nil {
		f.cancel()
	}
	if f.fail == "put" {
		return nil, errors.New("sensitive storage detail")
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeHoglakeStorage) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.calls = append(f.calls, "get")
	if aws.ToString(input.Key) != f.keys[len(f.keys)-1] {
		f.t.Fatal("read used different key")
	}
	if f.fail == "get" {
		return nil, errors.New("sensitive storage detail")
	}
	body := hoglakeStorageProbeBody
	if f.corrupt {
		body += strings.Repeat("x", 1024)
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(body))}, nil
}

func (f *fakeHoglakeStorage) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.calls = append(f.calls, "delete")
	if aws.ToString(input.Key) != f.keys[len(f.keys)-1] {
		f.t.Fatal("cleanup used different key")
	}
	deadline, ok := ctx.Deadline()
	if ctx.Err() != nil || !ok || time.Until(deadline) > 3*time.Second {
		f.t.Fatal("cleanup must be independently bounded")
	}
	if f.fail == "delete" {
		return nil, errors.New("sensitive storage detail")
	}
	return &s3.DeleteObjectOutput{}, nil
}

func testHoglakeStorageProbe(t *testing.T, client *fakeHoglakeStorage) *HoglakeStorageProbe {
	t.Helper()
	probe := NewHoglakeStorageProbe(func(_ context.Context, role string) (string, string, string, error) {
		if role != "tenant-role" {
			t.Fatalf("wrong role: %s", role)
		}
		return "fixture-access", "fixture-secret", "fixture-session", nil
	})
	probe.newClient = func(_ context.Context, region, access, secret, token string) (hoglakeStorageClient, error) {
		if region != "us-east-1" || access != "fixture-access" || secret != "fixture-secret" || token != "fixture-session" {
			t.Fatal("tenant credentials were not passed to client")
		}
		return client, nil
	}
	return probe
}

func TestTrinoHoglakeStorageProbePermissions(t *testing.T) {
	for _, failure := range []string{"", "put", "get", "delete", "content"} {
		t.Run(failure, func(t *testing.T) {
			client := &fakeHoglakeStorage{t: t, fail: failure, corrupt: failure == "content"}
			probe := testHoglakeStorageProbe(t, client)
			err := probe.Check(context.Background(), "tenant-role", "us-east-1", "s3://example-bucket/trino/warehouse-a/")
			if (err != nil) != (failure != "") {
				t.Fatalf("failure=%s err=%v", failure, err)
			}
			if err != nil && strings.Contains(err.Error(), "sensitive") {
				t.Fatal("AWS details escaped readiness check")
			}
			want := []string{"put", "get", "delete"}
			if failure == "put" {
				want = []string{"put", "delete"}
			}
			if !slices.Equal(client.calls, want) {
				t.Fatalf("calls=%v want=%v", client.calls, want)
			}
			client.fail, client.corrupt = "", false
			if err := probe.Check(context.Background(), "tenant-role", "us-east-1", "s3://example-bucket/trino/warehouse-a/"); err != nil {
				t.Fatal(err)
			}
			if client.keys[0] != client.keys[1] {
				t.Fatal("retry created another readiness object")
			}
		})
	}
}

func TestTrinoHoglakeStorageProbeCleanupAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeHoglakeStorage{t: t, fail: "put", cancel: cancel}
	probe := testHoglakeStorageProbe(t, client)
	if err := probe.Check(ctx, "tenant-role", "us-east-1", "s3://example-bucket/trino/warehouse-a/"); err == nil {
		t.Fatal("failed upload accepted")
	}
	if !slices.Equal(client.calls, []string{"put", "delete"}) {
		t.Fatalf("cleanup missing: %v", client.calls)
	}
}

func TestTrinoHoglakeStorageProbeFailsClosed(t *testing.T) {
	for _, assume := range []AssumeRoleFunc{nil, func(context.Context, string) (string, string, string, error) { return "", "", "", nil }} {
		probe := NewHoglakeStorageProbe(assume)
		probe.newClient = func(context.Context, string, string, string, string) (hoglakeStorageClient, error) {
			t.Fatal("constructed client without tenant credentials")
			return nil, nil
		}
		if err := probe.Check(context.Background(), "tenant-role", "us-east-1", "s3://example-bucket/trino/warehouse-a/"); err == nil {
			t.Fatal("absent credentials accepted")
		}
	}
}

func TestTrinoHoglakeStorageReadinessGatesPublication(t *testing.T) {
	h, client, _, _ := newHoglakeStateHarness(t)
	attempts := 0
	denied := true
	h.provisioner.hoglakeStorageCheck = func(_ context.Context, role, region, path string) error {
		attempts++
		if role != h.ducklings["tenant-a"].IAMRoleARN || region != "us-east-1" || path != "s3://example-bucket/trino/warehouse-a/" {
			t.Fatalf("incorrect storage scope: %s %s %s", role, region, path)
		}
		if denied {
			return errors.New("waiting for storage")
		}
		return nil
	}
	reconcile := func(exists bool) error {
		return h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], exists, map[string]string{"org_tenant_a": "hoglake"})
	}
	if err := reconcile(false); err == nil || len(h.catalog.created) != 0 {
		t.Fatal("denied storage published catalog")
	}
	denied = false
	if err := reconcile(false); err != nil {
		t.Fatal(err)
	}
	if len(h.catalog.created) != 1 || attempts != 2 {
		t.Fatalf("publication after recovery: attempts=%d created=%v", attempts, h.catalog.created)
	}
	denied = true
	if err := reconcile(true); err != nil || attempts != 2 {
		t.Fatalf("existing catalog re-probed: attempts=%d err=%v", attempts, err)
	}
}

func TestTrinoHoglakeStorageDenialRemainsProvisioning(t *testing.T) {
	h, client, _, _ := newHoglakeStateHarness(t)
	h.store.orgs[0].CellID = testCellID
	h.store.orgs[0].Tier = "free"
	h.store.orgs[0].RootPasswordHash = "$2a$10$hash"
	h.provisioner.catalog = client
	h.provisioner.hoglakeDucklings = h.provisioner.ducklings
	h.ducklings["tenant-a"].ReadyCondition = true
	denied := true
	h.provisioner.hoglakeStorageCheck = func(context.Context, string, string, string) error {
		if denied {
			return errors.New("waiting for storage access")
		}
		return nil
	}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("storage propagation failed global reconcile: %v", err)
	}
	state, ok := h.store.lastState("tenant-a")
	if !ok || state.State != configstore.ManagedWarehouseStateProvisioning || len(h.catalog.created) != 0 {
		t.Fatalf("storage-denied tenant state: %+v", state)
	}
	denied = false
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, ok = h.store.lastState("tenant-a")
	if !ok || state.State != configstore.ManagedWarehouseStateReady || len(h.catalog.created) != 1 {
		t.Fatalf("storage-recovered tenant state: %+v", state)
	}
}
