package opa

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/open-policy-agent/opa/bundle"
)

// policyRego is the policy file shipped inside every bundle this package
// builds. It is embedded at compile time so a malformed Rego file fails
// the Go build, not silently at runtime when the first bundle is built.
//
//go:embed policy.rego
var policyRego []byte

// bundleRevision is the manifest.revision stamped on bundles. OPA logs
// the revision on activation; bumping it on policy edits makes bundle
// pushes visible in OPA logs. The build encoding (data hash) further
// disambiguates bundles with the same revision but different data.
const bundleRevision = "v1"

// dataJSONPath is where user_catalogs lives inside the bundle. OPA wires
// /data.json under the package root, so the policy reads it via
// data.user_catalogs[...].
const dataJSONPath = "/data.json"

// policyPath is the in-bundle path of policy.rego. The bundle library
// requires .rego files to live under a path; the choice of name is
// cosmetic but keeps OPA's logs readable.
const policyPath = "/policy.rego"

// defaultBuilder is the production implementation of BundleBuilder. It
// has no state; the embedded policy and the input UserCatalogs are the
// only things that vary between builds.
type defaultBuilder struct{}

// NewBuilder returns a BundleBuilder that produces gzip-tarball OPA
// bundles containing this package's policy.rego and a data document
// populated from UserCatalogs.
func NewBuilder() BundleBuilder {
	return defaultBuilder{}
}

// BuildBundle assembles a bundle (manifest + policy.rego + data.json) and
// returns the compressed bytes. The output is suitable for POSTing through
// OPA's bundle service API or serving from a static bundle endpoint.
//
// uc may be nil or empty -- the resulting bundle is still well-formed and
// activates a deny-everything policy (since no user owns any catalog).
// That is the correct bootstrap behaviour: until the provisioner pushes
// a populated UserCatalogs, all customer queries are denied.
func (defaultBuilder) BuildBundle(uc UserCatalogs) ([]byte, error) {
	data, err := buildDataDocument(uc)
	if err != nil {
		return nil, fmt.Errorf("build data document: %w", err)
	}

	b := bundle.Bundle{
		Manifest: bundle.Manifest{
			Revision: bundleRevision,
			Roots:    &[]string{"trino", "user_catalogs"},
		},
		Modules: []bundle.ModuleFile{
			{
				URL:    policyPath,
				Path:   policyPath,
				Raw:    policyRego,
				Parsed: nil, // parsed lazily by the loader if needed
			},
		},
		Data: data,
	}

	// Init ensures Roots is non-nil; safe to call even though we set Roots
	// explicitly above (it's idempotent in the library).
	b.Manifest.Init()

	var buf bytes.Buffer
	w := bundle.NewWriter(&buf).UseModulePath(true)
	if err := w.Write(b); err != nil {
		return nil, fmt.Errorf("write bundle: %w", err)
	}
	return buf.Bytes(), nil
}

// buildDataDocument builds the JSON-decoded map[string]interface{} that OPA
// stores under data.<root>. We always emit `user_catalogs` even when uc is
// nil so the policy's `data.user_catalogs[user][catalog]` lookup is well-
// formed (undefined-on-missing-key, not error-on-missing-document).
func buildDataDocument(uc UserCatalogs) (map[string]interface{}, error) {
	// JSON round-trip ensures we emit canonical JSON-decoded types
	// (map[string]interface{} and bool) regardless of what the caller
	// passes in. OPA's bundle loader expects these types and treats
	// concrete map[string]map[string]bool as opaque if it ever leaks
	// through. Round-tripping is also a stable serialization for tests.
	raw, err := json.Marshal(struct {
		UserCatalogs UserCatalogs `json:"user_catalogs"`
	}{UserCatalogs: uc})
	if err != nil {
		return nil, fmt.Errorf("marshal user_catalogs: %w", err)
	}
	if uc == nil {
		// Marshalling a nil map emits "null"; substitute an empty object
		// so the policy sees `data.user_catalogs == {}` not `null`.
		raw = []byte(`{"user_catalogs":{}}`)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal user_catalogs: %w", err)
	}
	return data, nil
}

// BundleTimestamp returns the timestamp stamped into newly built bundles.
// Exposed for tests that need to assert on the bundle metadata; production
// callers should not depend on this.
//
// Today the bundle library writes the current time into the manifest's
// build metadata; this helper is a placeholder for future use if we move
// to deterministic timestamps for content addressing.
func BundleTimestamp() time.Time {
	return time.Now().UTC()
}
