package perf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"time"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

const funnelIntent = "intent_coverage_ordered_funnel_v1"

// Profiling is deliberately restricted to this bounded aggregate workload.
// No raw plans, query info or result values enter public logs or artifacts.
func profileCatalog(catalog perfcore.Catalog) (perfcore.Catalog, string, error) {
	if os.Getenv("DUCKGRES_SCENARIO_PROFILE_ORDERED_FUNNEL") != "true" {
		return catalog, "", nil
	}
	recipient := os.Getenv("DUCKGRES_SCENARIO_PROFILE_RECIPIENT")
	if !regexp.MustCompile(`^age1[a-z0-9]+$`).MatchString(recipient) {
		return catalog, "", fmt.Errorf("profiling requires an age public recipient")
	}
	if _, err := exec.LookPath("age"); err != nil {
		return catalog, "", fmt.Errorf("profiling requires age on PATH")
	}
	for _, target := range catalog.Targets {
		if target != perfcore.ProtocolPGWireCached && target != perfcore.ProtocolPGWireUncached && target != perfcore.ProtocolTrino && target != perfcore.ProtocolTrinoCached {
			return catalog, "", fmt.Errorf("unsupported profiling target")
		}
	}
	var queries []perfcore.Query
	for _, query := range catalog.Queries {
		if query.IntentID == funnelIntent {
			if len(query.Params) != 0 {
				return catalog, "", fmt.Errorf("profiling requires a parameter-free query")
			}
			queries = append(queries, query)
		}
	}
	if len(queries) == 0 {
		return catalog, "", fmt.Errorf("ordered funnel query missing from catalog")
	}
	catalog.Queries = queries
	return catalog, recipient, nil
}

func captureProfiles(ctx context.Context, catalog perfcore.Catalog, drivers map[perfcore.Protocol]perfcore.ProtocolDriver, outputDir, recipient string) error {
	for _, target := range catalog.Targets {
		for _, query := range catalog.Queries {
			// Reuse the runner's protocol and storage-variant routing. Paired
			// catalog queries carry StorageTarget even when Targets is empty.
			singleQuery := perfcore.Catalog{Queries: []perfcore.Query{query}}
			if !singleQuery.NeedsDriver(target) {
				continue
			}
			if err := captureProfile(ctx, drivers[target], query, outputDir, recipient); err != nil {
				return err
			}
		}
	}
	return nil
}

func captureProfile(ctx context.Context, driver perfcore.ProtocolDriver, query perfcore.Query, outputDir, recipient string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	reader, ok := driver.(perfcore.ResultReader)
	if !ok {
		return fmt.Errorf("profiling driver cannot read result values")
	}
	// Save all sensitive details together, encrypted before touching disk.
	document := map[string]any{
		"protocol": driver.Protocol(), "query_id": query.QueryID, "sql": query.CanonicalSQL(),
		"frozen_source": os.Getenv("DUCKGRES_SCENARIO_FROZEN_S3_URI"),
	}
	versionQuery := perfcore.Query{PGWireSQL: "SELECT version()"}
	if driver.Protocol() == perfcore.ProtocolPGWireCached || driver.Protocol() == perfcore.ProtocolPGWireUncached {
		// version() is a PostgreSQL compatibility string on the pgwire endpoint.
		versionQuery.PGWireSQL = "SELECT library_version, source_id FROM pragma_version()"
	}
	version, profileErr := reader.ReadResults(ctx, versionQuery, nil)
	document["engine_version"] = version
	if profileErr == nil {
		var results [][]*string
		results, profileErr = reader.ReadResults(ctx, query, nil)
		document["results"] = results
	}
	var plan [][]*string
	if profileErr == nil {
		if profiler, ok := driver.(interface {
			Profile(context.Context, perfcore.Query) ([][]*string, []byte, error)
		}); ok {
			var raw []byte
			plan, raw, profileErr = profiler.Profile(ctx, query)
			if json.Valid(raw) {
				document["query_info"] = json.RawMessage(raw)
			}
		} else {
			query.PGWireSQL = "EXPLAIN ANALYZE " + query.CanonicalSQL()
			plan, profileErr = reader.ReadResults(ctx, query, nil)
		}
	}
	document["explain_analyze"] = plan
	if profileErr != nil {
		document["error"] = profileErr.Error()
	}
	raw, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode profile document")
	}
	// Use a fresh bounded context to retain partial diagnostics on query timeout.
	writeCtx, writeCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer writeCancel()
	command := exec.CommandContext(writeCtx, "age", "-r", recipient)
	command.Stdin = bytes.NewReader(raw)
	ciphertext, err := command.Output()
	if err != nil {
		return fmt.Errorf("encrypt profile failed (%T)", err)
	}
	path := filepath.Join(outputDir, "profile-"+string(driver.Protocol())+".json.age")
	if err := os.WriteFile(path, ciphertext, 0600); err != nil {
		return fmt.Errorf("write encrypted profile failed")
	}
	if profileErr != nil {
		return fmt.Errorf("instrumented execution failed; see encrypted profile")
	}
	return nil
}
