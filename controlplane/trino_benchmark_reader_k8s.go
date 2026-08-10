//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

// ducklingTrinoReaderResolver is the production TrinoReaderResolver. It composes
// two existing sources of truth and adds nothing of its own:
//
//   - the config-store warehouse row: the authoritative Duckling CR name plus
//     the fallback bucket/region and the tenant's WRITER role (compared
//     against, never used); and
//   - the Duckling CR status: the metadata endpoint/database, the data bucket,
//     and — published only by the companion charts release —
//     status.benchmarkReader, holding the reader's database role, the exact
//     Secret reference for its password, and the read-only S3 role ARN.
//
// It reads the CR status through GetStatusWithoutCredentials, so resolving a
// reader identity never pulls the tenant's writer password into memory. If the
// charts release is not deployed, status.benchmarkReader is absent and
// buildTrinoReaderIdentity fails closed.
type ducklingTrinoReaderResolver struct {
	warehouses trinoReaderWarehouseStore
	ducklings  trinoReaderDucklingSource
}

// trinoReaderWarehouseStore is the config-store surface the resolver needs.
type trinoReaderWarehouseStore interface {
	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
}

// trinoReaderDucklingSource is the Duckling surface the resolver needs. It is
// deliberately the credential-free read.
type trinoReaderDucklingSource interface {
	GetStatusWithoutCredentials(ctx context.Context, name string) (*provisioner.DucklingStatus, error)
}

func newDucklingTrinoReaderResolver(warehouses trinoReaderWarehouseStore, ducklings trinoReaderDucklingSource) (*ducklingTrinoReaderResolver, error) {
	if warehouses == nil || ducklings == nil {
		return nil, fmt.Errorf("%w: Trino reader resolution needs both the config store and the Duckling client", ErrTrinoBenchmarkConfig)
	}
	return &ducklingTrinoReaderResolver{warehouses: warehouses, ducklings: ducklings}, nil
}

func (r *ducklingTrinoReaderResolver) ResolveTrinoReader(ctx context.Context, orgID string) (TrinoReaderIdentity, error) {
	warehouse, err := r.warehouses.GetManagedWarehouse(orgID)
	if err != nil {
		return TrinoReaderIdentity{}, fmt.Errorf("read managed warehouse for org %s: %w", orgID, err)
	}
	if warehouse == nil {
		return TrinoReaderIdentity{}, fmt.Errorf("%w: org %s has no managed warehouse", ErrTrinoBenchmarkConfig, orgID)
	}
	ducklingName := warehouse.DucklingName
	if ducklingName == "" {
		ducklingName = orgID
	}

	status, err := r.ducklings.GetStatusWithoutCredentials(ctx, ducklingName)
	if err != nil {
		return TrinoReaderIdentity{}, fmt.Errorf("read duckling %s status: %w", ducklingName, err)
	}
	if status == nil {
		return TrinoReaderIdentity{}, fmt.Errorf("%w: duckling %s has no status", ErrTrinoBenchmarkConfig, ducklingName)
	}

	host, port, viaPgBouncer, err := ducklingMetadataStoreAddress(status, orgID)
	if err != nil {
		return TrinoReaderIdentity{}, fmt.Errorf("%w: %v", ErrTrinoBenchmarkConfig, err)
	}
	// Same rule the internal metadata callers use: plaintext to the in-cluster
	// pooler (which carries TLS onward), TLS straight to a direct endpoint.
	sslMode := "require"
	if viaPgBouncer {
		sslMode = "disable"
	}

	bucket := status.DataStore.BucketName
	if bucket == "" {
		bucket = warehouse.DataStore.BucketName
	}
	region := status.DataStore.S3Region
	if region == "" {
		region = warehouse.DataStore.Region
	}

	reader := status.BenchmarkReader
	return buildTrinoReaderIdentity(TrinoReaderSource{
		MetadataEndpoint: net.JoinHostPort(host, strconv.Itoa(port)),
		MetadataDatabase: status.MetadataStore.Database,
		MetadataUser:     reader.MetadataUser,
		MetadataPasswordSecret: TrinoReaderSecretRef{
			Name:      reader.CredentialSecretRef.Name,
			Namespace: reader.CredentialSecretRef.Namespace,
			Key:       reader.CredentialSecretRef.Key,
		},
		Bucket:          bucket,
		Region:          region,
		ReadOnlyRoleARN: reader.S3ReadOnlyRoleARN,
		SSLMode:         sslMode,
		// Compared against, never used: a charts release that publishes the
		// tenant's own write identity here is refused outright.
		WriterRoleARN: firstNonEmptyTrinoValue(status.IAMRoleARN, warehouse.WorkerIdentity.IAMRoleARN),
		WriterUser:    status.MetadataStore.User,
	})
}

func firstNonEmptyTrinoValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
