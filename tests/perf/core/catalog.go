package core

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadCatalog(path string) (Catalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Catalog{}, fmt.Errorf("read catalog %s: %w", path, err)
	}
	return ParseCatalog(b)
}

func ParseCatalog(raw []byte) (Catalog, error) {
	var c Catalog
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return Catalog{}, fmt.Errorf("parse catalog: %w", err)
	}
	if err := validateCatalog(c); err != nil {
		return Catalog{}, err
	}
	return c, nil
}

func validateCatalog(c Catalog) error {
	if c.Name == "" {
		return fmt.Errorf("catalog name is required")
	}
	if c.MeasureIterations <= 0 {
		return fmt.Errorf("measure_iterations must be > 0")
	}
	if c.DatasetScale <= 0 {
		return fmt.Errorf("dataset_scale must be > 0")
	}
	if len(c.Targets) == 0 {
		return fmt.Errorf("targets must include at least one protocol")
	}
	seenTargets := map[Protocol]struct{}{}
	for _, target := range c.Targets {
		if target != ProtocolPGWire && target != ProtocolFlight {
			return fmt.Errorf("unsupported target protocol %q", target)
		}
		if _, ok := seenTargets[target]; ok {
			return fmt.Errorf("duplicate target protocol %q", target)
		}
		seenTargets[target] = struct{}{}
	}
	if len(c.Queries) == 0 {
		return fmt.Errorf("queries must include at least one entry")
	}
	seenQueryIDs := map[string]struct{}{}
	for _, q := range c.Queries {
		if q.QueryID == "" {
			return fmt.Errorf("query_id is required")
		}
		if _, ok := seenQueryIDs[q.QueryID]; ok {
			return fmt.Errorf("duplicate query_id %q", q.QueryID)
		}
		seenQueryIDs[q.QueryID] = struct{}{}
		if q.IntentID == "" {
			return fmt.Errorf("query %s missing intent_id", q.QueryID)
		}
		if q.PGWireSQL == "" {
			return fmt.Errorf("query %s missing pgwire_sql", q.QueryID)
		}
		if q.DuckhogSQL == "" {
			return fmt.Errorf("query %s missing duckhog_sql", q.QueryID)
		}
	}
	return nil
}
