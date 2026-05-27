package server

import (
	"testing"

	"github.com/posthog/duckgres/server/iceberg"
)

func TestShouldLoadIcebergColumnMetadataOnlyForLakekeeper(t *testing.T) {
	if !shouldLoadIcebergColumnMetadata(IcebergConfig{
		Enabled:             true,
		Backend:             iceberg.BackendLakekeeper,
		LakekeeperEndpoint:  "http://lakekeeper.invalid/catalog",
		LakekeeperWarehouse: "org-acme",
	}, false) {
		t.Fatal("expected Lakekeeper catalog to load Iceberg column metadata")
	}
	if shouldLoadIcebergColumnMetadata(IcebergConfig{
		Enabled: true,
		Backend: iceberg.BackendS3Tables,
	}, false) {
		t.Fatal("S3 Tables catalog should not use Lakekeeper REST metadata loading")
	}
	if shouldLoadIcebergColumnMetadata(IcebergConfig{
		Enabled: true,
		Backend: iceberg.BackendLakekeeper,
	}, true) {
		t.Fatal("passthrough connections should not load Iceberg column metadata")
	}
}

func TestLoadIcebergColumnMetadataUsesConnectionIcebergConfig(t *testing.T) {
	cc := &clientConn{
		server: &Server{cfg: Config{}},
	}
	SetConnectionIcebergConfig(cc, IcebergConfig{
		Enabled:             true,
		Backend:             iceberg.BackendLakekeeper,
		LakekeeperEndpoint:  "http://lakekeeper.example/catalog",
		LakekeeperWarehouse: "org-acme",
	})

	cfg := cc.effectiveIcebergConfig()
	if !shouldLoadIcebergColumnMetadata(cfg, false) {
		t.Fatalf("expected tenant Iceberg config to enable metadata loading")
	}
	if cfg.LakekeeperEndpoint != "http://lakekeeper.example/catalog" {
		t.Fatalf("tenant endpoint not preserved: %q", cfg.LakekeeperEndpoint)
	}
	if cfg.LakekeeperWarehouse != "org-acme" {
		t.Fatalf("tenant warehouse not preserved: %q", cfg.LakekeeperWarehouse)
	}
}
