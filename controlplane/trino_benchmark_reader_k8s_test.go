//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

type fakeTrinoWarehouseStore struct {
	warehouse *configstore.ManagedWarehouse
	err       error
}

func (f fakeTrinoWarehouseStore) GetManagedWarehouse(string) (*configstore.ManagedWarehouse, error) {
	return f.warehouse, f.err
}

type fakeTrinoDucklingSource struct {
	status *provisioner.DucklingStatus
	err    error
	name   string
}

func (f *fakeTrinoDucklingSource) GetStatusWithoutCredentials(_ context.Context, name string) (*provisioner.DucklingStatus, error) {
	f.name = name
	return f.status, f.err
}

func readyDucklingStatusWithReader() *provisioner.DucklingStatus {
	status := &provisioner.DucklingStatus{}
	status.MetadataStore.Type = configstore.MetadataStoreKindCnpgShard
	status.MetadataStore.PgBouncerEndpoint = "duckling-bench-org-pgbouncer.ducklings.svc.cluster.local:6432"
	status.MetadataStore.Database = "ducklake_bench_org"
	status.MetadataStore.User = "ducklake_bench_org"
	status.DataStore.BucketName = "posthog-duckling-benchorg-dev"
	status.DataStore.S3Region = "us-east-1"
	status.IAMRoleARN = trinoTestWriterRoleARN
	status.BenchmarkReader = provisioner.DucklingBenchmarkReader{
		MetadataUser: "trino_reader_bench_org",
		CredentialSecretRef: provisioner.SecretReference{
			Name: trinoTestReaderSecret, Namespace: "ducklings", Key: "password",
		},
		S3ReadOnlyRoleARN: "arn:aws:iam::123456789012:role/duckling-bench-org-trino-reader",
	}
	return status
}

func TestDucklingTrinoReaderResolverBuildsIdentityFromChartsState(t *testing.T) {
	ducklings := &fakeTrinoDucklingSource{status: readyDucklingStatusWithReader()}
	resolver, err := newDucklingTrinoReaderResolver(fakeTrinoWarehouseStore{
		warehouse: &configstore.ManagedWarehouse{OrgID: "bench-org", DucklingName: "duckling-bench-org"},
	}, ducklings)
	if err != nil {
		t.Fatalf("newDucklingTrinoReaderResolver: %v", err)
	}

	identity, err := resolver.ResolveTrinoReader(context.Background(), "bench-org")
	if err != nil {
		t.Fatalf("ResolveTrinoReader: %v", err)
	}
	if ducklings.name != "duckling-bench-org" {
		t.Fatalf("resolved duckling %q, want the warehouse row's authoritative name", ducklings.name)
	}
	if identity.MetadataUser != "trino_reader_bench_org" {
		t.Fatalf("metadata user = %q", identity.MetadataUser)
	}
	if identity.MetadataPasswordSecret.Name != trinoTestReaderSecret || identity.MetadataPasswordSecret.Namespace != "ducklings" {
		t.Fatalf("password Secret reference = %s", identity.MetadataPasswordSecret)
	}
	if identity.ReadOnlyRoleARN != "arn:aws:iam::123456789012:role/duckling-bench-org-trino-reader" {
		t.Fatalf("read-only role = %q", identity.ReadOnlyRoleARN)
	}
	// PgBouncer hop ⇒ plaintext to the pooler, exactly like the other internal
	// metadata callers.
	if identity.SSLMode != "disable" {
		t.Fatalf("sslmode = %q, want disable through the duckling pooler", identity.SSLMode)
	}
	if identity.DataPath != "s3://posthog-duckling-benchorg-dev/" {
		t.Fatalf("data path = %q", identity.DataPath)
	}
}

func TestDucklingTrinoReaderResolverFailsClosedWithoutChartsReaderBlock(t *testing.T) {
	status := readyDucklingStatusWithReader()
	status.BenchmarkReader = provisioner.DucklingBenchmarkReader{}

	resolver, err := newDucklingTrinoReaderResolver(fakeTrinoWarehouseStore{
		warehouse: &configstore.ManagedWarehouse{OrgID: "bench-org", DucklingName: "duckling-bench-org"},
	}, &fakeTrinoDucklingSource{status: status})
	if err != nil {
		t.Fatalf("newDucklingTrinoReaderResolver: %v", err)
	}

	_, err = resolver.ResolveTrinoReader(context.Background(), "bench-org")
	if !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig until the charts reader resources are deployed", err)
	}
}

func TestDucklingTrinoReaderResolverRefusesWriterRoleInTheReaderField(t *testing.T) {
	status := readyDucklingStatusWithReader()
	status.BenchmarkReader.S3ReadOnlyRoleARN = trinoTestWriterRoleARN

	resolver, err := newDucklingTrinoReaderResolver(fakeTrinoWarehouseStore{
		warehouse: &configstore.ManagedWarehouse{OrgID: "bench-org", DucklingName: "duckling-bench-org"},
	}, &fakeTrinoDucklingSource{status: status})
	if err != nil {
		t.Fatalf("newDucklingTrinoReaderResolver: %v", err)
	}

	if _, err := resolver.ResolveTrinoReader(context.Background(), "bench-org"); !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
	}
}

func TestDucklingTrinoReaderResolverFallsBackToWarehouseBucketAndRegion(t *testing.T) {
	status := readyDucklingStatusWithReader()
	status.DataStore.BucketName = ""
	status.DataStore.S3Region = ""

	warehouse := &configstore.ManagedWarehouse{OrgID: "bench-org", DucklingName: "duckling-bench-org"}
	warehouse.DataStore.BucketName = "posthog-duckling-benchorg-dev"
	warehouse.DataStore.Region = "us-east-1"

	resolver, err := newDucklingTrinoReaderResolver(fakeTrinoWarehouseStore{warehouse: warehouse}, &fakeTrinoDucklingSource{status: status})
	if err != nil {
		t.Fatalf("newDucklingTrinoReaderResolver: %v", err)
	}

	identity, err := resolver.ResolveTrinoReader(context.Background(), "bench-org")
	if err != nil {
		t.Fatalf("ResolveTrinoReader: %v", err)
	}
	if identity.Bucket != "posthog-duckling-benchorg-dev" || identity.Region != "us-east-1" {
		t.Fatalf("bucket/region = %s/%s", identity.Bucket, identity.Region)
	}
}

func TestDucklingTrinoReaderResolverFailsClosedWithoutWarehouse(t *testing.T) {
	resolver, err := newDucklingTrinoReaderResolver(fakeTrinoWarehouseStore{}, &fakeTrinoDucklingSource{status: readyDucklingStatusWithReader()})
	if err != nil {
		t.Fatalf("newDucklingTrinoReaderResolver: %v", err)
	}

	if _, err := resolver.ResolveTrinoReader(context.Background(), "bench-org"); !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
	}
}

func TestDucklingTrinoReaderResolverRequiresBothSources(t *testing.T) {
	if _, err := newDucklingTrinoReaderResolver(nil, &fakeTrinoDucklingSource{}); !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
	}
	if _, err := newDucklingTrinoReaderResolver(fakeTrinoWarehouseStore{}, nil); !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
	}
}
