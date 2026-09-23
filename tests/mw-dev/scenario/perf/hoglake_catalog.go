package perf

import (
	"errors"
	"maps"
)

func fixtureHoglakeProperties(original map[string]string, catalog string) (map[string]string, error) {
	if original["connector.name"] != "hoglake" || original["hoglake.catalog"] == "" || original["hoglake.uri"] == "" || original["fs.cache.enabled"] != "false" || original["s3.auth-type"] != "IAM_ROLE" {
		return nil, errors.New("dataset selection requires an existing uncached Hoglake benchmark catalog using IAM_ROLE authentication")
	}
	if original["s3.iam-role"] != "" && original["hoglake.catalog"] == catalog {
		return nil, errors.New("read-only benchmark fixtures require a separate Hoglake catalog from the managed tenant")
	}
	desired := maps.Clone(original)
	desired["hoglake.catalog"] = catalog
	// Managed onboarding assumes a tenant's writable storage role. Immutable
	// benchmark fixtures are read through the isolated Trino Pod Identity instead,
	// which is the default credential chain. Trino requires s3.iam-role to be set
	// exactly when s3.auth-type=IAM_ROLE, so the auth type goes with the role.
	delete(desired, "s3.iam-role")
	delete(desired, "s3.auth-type")
	return desired, nil
}
