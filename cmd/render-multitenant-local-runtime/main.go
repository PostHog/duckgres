package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane"
	"github.com/posthog/duckgres/controlplane/configstore"
)

func main() {
	var (
		configStoreConn      = flag.String("config-store", "postgres://duckgres:duckgres@127.0.0.1:5434/duckgres_config?sslmode=disable", "PostgreSQL config-store connection string")
		teamName             = flag.String("team", "local", "Team name to render runtime artifacts for")
		namespace            = flag.String("namespace", "duckgres", "Default Kubernetes namespace for generated artifacts")
		outputPath           = flag.String("output", "", "Write the rendered manifest to this path instead of stdout")
		dataDir              = flag.String("data-dir", "/data", "Worker data_dir value for the rendered duckgres.yaml")
		extensionsCSV        = flag.String("extensions", "ducklake", "Comma-separated worker extensions to include in duckgres.yaml")
		warehouseDBPassword  = flag.String("warehouse-db-password", "duckgres", "Warehouse database password used for local artifact rendering")
		metadataStorePass    = flag.String("metadata-store-password", "ducklake", "Metadata-store password used for local artifact rendering")
		s3AccessKey          = flag.String("s3-access-key", "minioadmin", "S3 access key used for local artifact rendering")
		s3SecretKey          = flag.String("s3-secret-key", "minioadmin", "S3 secret key used for local artifact rendering")
	)
	flag.Parse()

	store, err := configstore.NewConfigStore(*configStoreConn, time.Hour)
	if err != nil {
		exitf("connect config store: %v", err)
	}

	team := store.Snapshot().Teams[*teamName]
	if team == nil {
		exitf("team %q not found in config store", *teamName)
	}

	manifest, err := controlplane.BuildManagedWarehouseRuntimeArtifacts(team, controlplane.ManagedWarehouseSecretMaterial{
		WarehouseDatabasePassword: *warehouseDBPassword,
		MetadataStorePassword:     *metadataStorePass,
		S3AccessKey:               *s3AccessKey,
		S3SecretKey:               *s3SecretKey,
	}, controlplane.ManagedWarehouseRuntimeArtifactOptions{
		DefaultNamespace: *namespace,
		DataDir:          *dataDir,
		Extensions:       splitCSV(*extensionsCSV),
	})
	if err != nil {
		exitf("render managed warehouse runtime artifacts: %v", err)
	}

	if *outputPath == "" {
		if _, err := os.Stdout.Write(manifest); err != nil {
			exitf("write manifest to stdout: %v", err)
		}
		return
	}

	if err := os.WriteFile(*outputPath, manifest, 0o644); err != nil {
		exitf("write manifest: %v", err)
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}

	var parts []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func exitf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
