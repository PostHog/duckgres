// perf-properties-prepare validates a published fixture and emits private run inputs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/posthog/duckgres/tests/perf/properties"
	"gopkg.in/yaml.v3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	manifest := flag.String("manifest", "", "Required local path or s3:// completion manifest")
	output := flag.String("output-dir", "", "Required private output directory")
	athenaDB := flag.String("athena-database", "", "Optional existing Athena database; enables read-only table verification")
	athenaRegion := flag.String("athena-region", "", "AWS region for Athena Glue metadata; defaults to SDK region")
	athenaTable := flag.String("athena-table", "properties_events_supported", "Existing Athena table (catalog currently requires properties_events_supported)")
	flag.Parse()
	if *manifest == "" || *output == "" {
		return fmt.Errorf("-manifest and -output-dir are required")
	}
	if *athenaTable != "properties_events_supported" {
		return fmt.Errorf("-athena-table must be properties_events_supported to match the catalog")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(cfg)
	var m *properties.Manifest
	if strings.HasPrefix(*manifest, "s3://") {
		m, err = properties.LoadS3(ctx, client, *manifest)
	} else {
		m, err = properties.Load(*manifest)
	}
	if err != nil {
		return err
	}
	if err = m.VerifyInventory(ctx, client); err != nil {
		return err
	}
	if *athenaDB != "" {
		glueCfg := cfg
		if *athenaRegion != "" {
			glueCfg.Region = *athenaRegion
		}
		if err = m.VerifyAthenaTable(ctx, glue.NewFromConfig(glueCfg), *athenaDB, *athenaTable); err != nil {
			return err
		}
	}
	athenaSQL, err := m.AthenaSQL(*athenaTable)
	if err != nil {
		return err
	}
	catalog, err := yaml.Marshal(properties.Catalog(m))
	if err != nil {
		return err
	}
	if err = os.MkdirAll(*output, 0700); err != nil {
		return err
	}
	for name, data := range map[string][]byte{"setup.sql": []byte(m.SetupSQL()), "athena.sql": []byte(athenaSQL), "catalog.yaml": catalog, "dataset-version.txt": []byte("properties-v3-sha256-" + m.SHA256 + "\n")} {
		if err = os.WriteFile(filepath.Join(*output, name), data, 0600); err != nil {
			return err
		}
	}
	fmt.Printf("Validated manifest and %d objects; value validation remains sampled until paired query checks pass.\n", len(m.Files))
	return nil
}
