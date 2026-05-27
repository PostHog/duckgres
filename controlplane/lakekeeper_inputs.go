//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

// Environment variables that drive the Lakekeeper provisioner. The toggle is
// off by default so existing deployments are unaffected until an operator
// explicitly opts in.
const (
	envLakekeeperEnabled = "DUCKGRES_LAKEKEEPER_PROVISIONER_ENABLED"

	// Dev/orbstack fallback inputs, used only when an org has no
	// Crossplane-provisioned Duckling CR to read infrastructure from
	// (local MinIO + a plain Postgres). In prod these come from the CR.
	envLakekeeperAdminDSN   = "DUCKGRES_LAKEKEEPER_ADMIN_DSN"
	envLakekeeperPGHost     = "DUCKGRES_LAKEKEEPER_PG_HOST"
	envLakekeeperPGPort     = "DUCKGRES_LAKEKEEPER_PG_PORT"
	envLakekeeperPGSSLMode  = "DUCKGRES_LAKEKEEPER_PG_SSLMODE"
	envLakekeeperS3Bucket   = "DUCKGRES_LAKEKEEPER_S3_BUCKET"
	envLakekeeperS3Region   = "DUCKGRES_LAKEKEEPER_S3_REGION"
	envLakekeeperS3Endpoint = "DUCKGRES_LAKEKEEPER_S3_ENDPOINT"
	envLakekeeperS3Flavor   = "DUCKGRES_LAKEKEEPER_S3_FLAVOR"
	envLakekeeperS3KeyID    = "DUCKGRES_LAKEKEEPER_S3_ACCESS_KEY_ID"
	envLakekeeperS3Secret   = "DUCKGRES_LAKEKEEPER_S3_SECRET_ACCESS_KEY"
)

// lakekeeperProvisionerEnabled reports whether the control plane should wire
// the Lakekeeper provisioner branch into the provisioning controller.
func lakekeeperProvisionerEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(envLakekeeperEnabled)))
	return v == "1" || v == "true" || v == "yes"
}

// newLakekeeperInputsResolver builds the per-org ProvisioningInputs resolver
// the provisioning controller calls before EnsureForOrg.
//
// Two sources, in priority order:
//
//  1. The Crossplane-provisioned Duckling CR (prod). The CR's metadata-store
//     master credentials double as the admin connection that can CREATE the
//     lakekeeper_<orgid> database/role, and its data-store bucket is the S3
//     warehouse Lakekeeper hands out. The Lakekeeper pod uses its own IRSA
//     identity for S3 (no static creds), so we leave them empty.
//
//  2. Environment-configured fallback (dev/orbstack with MinIO). Used when no
//     Duckling CR resolver is available, or the CR has no usable
//     metadata-store/data-store yet.
//
// KubernetesAuthAudiences is always empty here: this wires the allowall +
// NetworkPolicy deployment shape. Enabling OIDC SA-token auth is a separate,
// flag-day change (see ProvisioningInputs.KubernetesAuthAudiences).
func newLakekeeperInputsResolver(
	resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error),
) provisioner.LakekeeperInputsResolver {
	return func(ctx context.Context, w *configstore.ManagedWarehouse) (provisioner.ProvisioningInputs, error) {
		if resolveDucklingStatus != nil {
			in, ok, err := lakekeeperInputsFromDuckling(ctx, resolveDucklingStatus, w.OrgID)
			if err != nil {
				// CR exists but is malformed/incomplete — fall through to the
				// env fallback rather than hard-failing, mirroring how
				// BuildActivationRequest degrades to the config-store path.
				if env, ok := lakekeeperInputsFromEnv(); ok {
					return env, nil
				}
				return provisioner.ProvisioningInputs{}, fmt.Errorf("resolve lakekeeper inputs for org %q from duckling CR: %w", w.OrgID, err)
			}
			if ok {
				return in, nil
			}
		}
		if env, ok := lakekeeperInputsFromEnv(); ok {
			return env, nil
		}
		return provisioner.ProvisioningInputs{}, fmt.Errorf("no lakekeeper provisioning inputs for org %q: no usable Duckling CR and %s/%s/%s not set",
			w.OrgID, envLakekeeperAdminDSN, envLakekeeperS3Bucket, envLakekeeperS3Region)
	}
}

// lakekeeperInputsFromDuckling derives inputs from the Duckling CR status.
// Returns ok=false (no error) when the resolver simply has no CR for the org,
// so the caller can fall back to env config without logging it as a failure.
func lakekeeperInputsFromDuckling(
	ctx context.Context,
	resolve func(context.Context, string) (*provisioner.DucklingStatus, error),
	orgID string,
) (provisioner.ProvisioningInputs, bool, error) {
	status, err := resolve(ctx, orgID)
	if err != nil {
		return provisioner.ProvisioningInputs{}, false, nil
	}
	if status == nil || status.MetadataStore.Endpoint == "" {
		return provisioner.ProvisioningInputs{}, false, nil
	}
	if status.MetadataStore.Password == "" || status.MetadataStore.User == "" || status.MetadataStore.Database == "" {
		return provisioner.ProvisioningInputs{}, false, fmt.Errorf("duckling CR for org %q has incomplete metadata-store credentials", orgID)
	}
	if status.DataStore.BucketName == "" || status.DataStore.S3Region == "" {
		return provisioner.ProvisioningInputs{}, false, fmt.Errorf("duckling CR for org %q has no data-store bucket/region", orgID)
	}

	s3 := provisioner.S3StorageConfig{
		Bucket: status.DataStore.BucketName,
		// Keep the catalog's data under a stable prefix so it never
		// collides with DuckLake's own layout in the shared bucket.
		KeyPrefix: "lakekeeper",
		Region:    status.DataStore.S3Region,
		Flavor:    "aws",
		// Prod: Lakekeeper uses its pod IRSA identity (no static creds).
		// RoleARN is the per-org duckling role Lakekeeper assumes to vend
		// scoped S3 creds (sts-role-arn). It's the same role the Lakekeeper
		// pod runs as via Pod Identity, self-assumed (the role trusts its
		// own ARN). Sourced from the Duckling CR status.
		RoleARN: status.IAMRoleARN,
	}

	// cnpg-shard: the Lakekeeper database + role are pre-provisioned on the
	// CNPG shard by provider-sql, and status carries the per-tenant role
	// credentials (not a privileged admin). We have no superuser DSN here, so
	// the provisioner must NOT attempt CREATE DATABASE/ROLE — it takes these
	// creds verbatim. The shard's Pooler is session-mode, so the Lakekeeper
	// pod's migrations + pooled prepared statements work through it.
	if status.MetadataStore.Type == "cnpg-shard" {
		return provisioner.ProvisioningInputs{
			PGPreProvisioned: true,
			PGUser:           status.MetadataStore.User,
			PGPassword:       status.MetadataStore.Password,
			PGDatabase:       status.MetadataStore.Database,
			PGHost:           status.MetadataStore.Endpoint,
			PGPort:           5432,
			PGSSLMode:        "require",
			S3:               s3,
			// Allowall + NetworkPolicy deployment shape — no OIDC audiences yet.
			KubernetesAuthAudiences: nil,
		}, true, nil
	}

	// aurora/external: status carries the metadata-store master credentials,
	// which double as the admin connection that can CREATE the
	// lakekeeper_<orgid> database/role. Admin DDL (CREATE DATABASE/ROLE) goes
	// to the direct Aurora endpoint, not the PgBouncer pooler — transaction
	// pooling breaks CREATE DATABASE and the session-level statements
	// EnsureRole runs. We connect to the existing metadata-store database;
	// CREATE DATABASE can be issued from any database.
	adminDSN := buildAdminURLDSN(
		status.MetadataStore.Endpoint, 5432,
		status.MetadataStore.User, status.MetadataStore.Password,
		status.MetadataStore.Database, "require",
	)

	return provisioner.ProvisioningInputs{
		AdminDSN: adminDSN,
		// The Lakekeeper pod also connects to the direct endpoint: it runs its
		// own migrations and connection pool, which a transaction pooler would
		// break.
		PGHost:                  status.MetadataStore.Endpoint,
		PGPort:                  5432,
		PGSSLMode:               "require",
		S3:                      s3,
		KubernetesAuthAudiences: nil,
	}, true, nil
}

// lakekeeperInputsFromEnv builds inputs from environment variables for
// dev/orbstack (MinIO). Returns ok=false when the minimum set isn't present.
func lakekeeperInputsFromEnv() (provisioner.ProvisioningInputs, bool) {
	adminDSN := strings.TrimSpace(os.Getenv(envLakekeeperAdminDSN))
	bucket := strings.TrimSpace(os.Getenv(envLakekeeperS3Bucket))
	region := strings.TrimSpace(os.Getenv(envLakekeeperS3Region))
	if adminDSN == "" || bucket == "" || region == "" {
		return provisioner.ProvisioningInputs{}, false
	}

	pgHost := strings.TrimSpace(os.Getenv(envLakekeeperPGHost))
	pgPort := int32(0)
	if v := strings.TrimSpace(os.Getenv(envLakekeeperPGPort)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			pgPort = int32(n)
		}
	}
	sslMode := strings.TrimSpace(os.Getenv(envLakekeeperPGSSLMode))
	if sslMode == "" {
		sslMode = "require"
	}
	flavor := strings.TrimSpace(os.Getenv(envLakekeeperS3Flavor))
	if flavor == "" {
		flavor = "aws"
	}

	return provisioner.ProvisioningInputs{
		AdminDSN:  adminDSN,
		PGHost:    pgHost,
		PGPort:    pgPort,
		PGSSLMode: sslMode,
		S3: provisioner.S3StorageConfig{
			Bucket:                bucket,
			Region:                region,
			Endpoint:              strings.TrimSpace(os.Getenv(envLakekeeperS3Endpoint)),
			Flavor:                flavor,
			StaticAccessKeyID:     strings.TrimSpace(os.Getenv(envLakekeeperS3KeyID)),
			StaticAccessKeySecret: strings.TrimSpace(os.Getenv(envLakekeeperS3Secret)),
		},
		KubernetesAuthAudiences: nil,
	}, true
}

// buildAdminURLDSN renders a pgx-compatible URL-style DSN. URL form is what
// the provisioner's reDSN rewrites when it scopes a connection to a specific
// database, so we emit that form here.
func buildAdminURLDSN(host string, port int, user, password, dbName, sslMode string) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   "/" + dbName,
	}
	q := u.Query()
	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()
	return u.String()
}
