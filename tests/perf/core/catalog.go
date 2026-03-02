package core

import (
	"fmt"
	"os"
	"strings"

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

func ValidateReadOnlyCatalog(c Catalog) error {
	for _, q := range c.Queries {
		if err := validateSelectOnlySQL("pgwire_sql", q.QueryID, q.PGWireSQL); err != nil {
			return err
		}
		if err := validateSelectOnlySQL("duckhog_sql", q.QueryID, q.DuckhogSQL); err != nil {
			return err
		}
	}
	return nil
}

func validateSelectOnlySQL(field, queryID, sql string) error {
	trimmed := trimLeadingSQLComments(sql)
	trimmed = strings.TrimSpace(trimmed)
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return fmt.Errorf("query %s missing %s", queryID, field)
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") {
		return fmt.Errorf("query %s %s must be SELECT-only in frozen mode", queryID, field)
	}
	if len(upper) > len("SELECT") {
		next := upper[len("SELECT")]
		if (next >= 'A' && next <= 'Z') || (next >= '0' && next <= '9') || next == '_' {
			return fmt.Errorf("query %s %s must be SELECT-only in frozen mode", queryID, field)
		}
	}
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("query %s %s must contain a single SELECT statement in frozen mode", queryID, field)
	}
	return nil
}

func trimLeadingSQLComments(sql string) string {
	remaining := strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(remaining, "--"):
			idx := strings.IndexByte(remaining, '\n')
			if idx < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[idx+1:])
		case strings.HasPrefix(remaining, "/*"):
			idx := strings.Index(remaining, "*/")
			if idx < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[idx+2:])
		default:
			return remaining
		}
	}
}
