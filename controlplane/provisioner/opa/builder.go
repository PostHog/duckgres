package opa

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"

	"github.com/open-policy-agent/opa/v1/bundle"
)

// policyRego is the policy file shipped inside every bundle this package
// builds. It is embedded at compile time so a malformed Rego file fails
// the Go build, not silently at runtime when the first bundle is built.
//
//go:embed policy.rego
var policyRego []byte

// bundleRevision is the schema prefix of every revision this package stamps.
// The revision itself is the prefix plus a digest of the projected data (see
// PolicyRevision): a constant could not tell a coordinator serving today's
// tenant set from one that predates it.
const bundleRevision = "v2"

// policyPath is the in-bundle path of policy.rego. The bundle library
// requires .rego files to live under a path; the choice of name is
// cosmetic but keeps OPA's logs readable.
const policyPath = "/policy.rego"

// defaultBuilder is the production implementation of BundleBuilder. It
// has no state; the embedded policy and the input GroupCatalogs are the
// only things that vary between builds.
type defaultBuilder struct{}

// NewBuilder returns a BundleBuilder that produces gzip-tarball OPA
// bundles containing this package's policy.rego and a data document
// populated from GroupCatalogs.
func NewBuilder() BundleBuilder {
	return defaultBuilder{}
}

// BuildBundle assembles a bundle (manifest + policy.rego + data.json) and
// returns the compressed bytes. The output is suitable for POSTing through
// OPA's bundle service API or serving from a static bundle endpoint.
//
// gc may be nil or empty -- the resulting bundle is still well-formed and
// activates a deny-everything policy (since no group owns any catalog).
// That is the correct bootstrap behaviour: until the provisioner pushes
// a populated GroupCatalogs, all customer queries are denied.
//
// gs carries the project scopes that narrow individual groups. A nil or empty
// GroupScopes means no group is scoped, which is the pre-scopes behaviour: a
// group reads the whole catalog it owns. Both documents are always emitted so
// the policy's `data.group_scopes[g]` lookup is undefined-on-missing-key
// rather than an error on a missing document.
func (defaultBuilder) BuildBundle(gc GroupCatalogs, gs GroupScopes) ([]byte, error) {
	data, err := buildDataDocument(gc, gs)
	if err != nil {
		return nil, fmt.Errorf("build data document: %w", err)
	}
	revision, err := PolicyRevision(gc, gs)
	if err != nil {
		return nil, err
	}
	// data.trino.revision is what a coordinator's OPA answers when it is asked
	// which authorization data it decides with (`opa.policy.revision-uri`).
	// Without this document OPA answers "undefined", the access control reports
	// no loaded revision, and a controller has no way to tell a coordinator
	// deciding with the current policy from one still serving a bundle from
	// before a tenant existed. The manifest revision alone cannot do it: it is
	// not queryable as a document.
	data["trino"] = map[string]interface{}{"revision": revision}

	b := bundle.Bundle{
		Manifest: bundle.Manifest{
			// The manifest revision carries the same value, so OPA's activation
			// log names the exact projection it loaded rather than a constant
			// that never changes.
			Revision: revision,
			Roots:    &[]string{"trino", "group_catalogs", "group_scopes"},
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

// PolicyRevision is the revision stamped on the bundle built from exactly this
// projection.
//
// It is a digest of the policy and its data, not a counter, for two reasons: the
// producer is whichever control-plane replica serves the bundle, so no replica
// owns a counter, and the value has to be comparable in both directions - a
// controller asks "is the coordinator deciding with the data I currently
// serve?", which is an equality question, not an ordering one.
//
// The POLICY BYTES are part of it, not just the data. policy.rego is embedded
// in the CONTROL PLANE binary and served to OPA as a remote bundle, so it is
// not covered by the candidate's image check at all: two duckgres versions can
// serve different rules to the same Trino and OPA images with an identical
// group map. A revision over the data alone would call a coordinator deciding
// with the previous RULES current.
func PolicyRevision(gc GroupCatalogs, gs GroupScopes) (string, error) {
	data, err := buildDataDocument(gc, gs)
	if err != nil {
		return "", fmt.Errorf("build data document: %w", err)
	}
	// Marshalling a map[string]interface{} sorts object keys, so the digest is
	// stable across builds of the same projection.
	canonical, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("canonicalize bundle data: %w", err)
	}
	digest := sha256.New()
	// Length-prefixed, so no rearrangement of policy and data bytes can produce
	// the same digest as a different pair.
	writeDigestField(digest, policyRego)
	writeDigestField(digest, canonical)
	return bundleRevision + "." + hex.EncodeToString(digest.Sum(nil)), nil
}

func writeDigestField(digest hash.Hash, field []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(field)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(field)
}

// buildDataDocument builds the JSON-decoded map[string]interface{} that OPA
// stores under data.<root>. We always emit `group_catalogs` even when gc is
// nil so the policy's `data.group_catalogs[group][catalog]` lookup is
// well-formed (undefined-on-missing-key, not error-on-missing-document).
func buildDataDocument(gc GroupCatalogs, gs GroupScopes) (map[string]interface{}, error) {
	// JSON round-trip ensures we emit canonical JSON-decoded types
	// (map[string]interface{} and bool) regardless of what the caller
	// passes in. OPA's bundle loader expects these types and treats
	// concrete map[string]map[string]bool as opaque if it ever leaks
	// through. Round-tripping is also a stable serialization for tests.
	if gc == nil {
		// Marshalling a nil map emits "null"; substitute an empty object so
		// the policy sees `data.group_catalogs == {}` not `null`.
		gc = GroupCatalogs{}
	}
	if gs == nil {
		gs = GroupScopes{}
	}
	raw, err := json.Marshal(struct {
		GroupCatalogs GroupCatalogs `json:"group_catalogs"`
		GroupScopes   GroupScopes   `json:"group_scopes"`
	}{GroupCatalogs: gc, GroupScopes: gs})
	if err != nil {
		return nil, fmt.Errorf("marshal bundle data: %w", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal bundle data: %w", err)
	}
	return data, nil
}
