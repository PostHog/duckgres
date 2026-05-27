//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

func ducklingStatusWithWarehouse() *provisioner.DucklingStatus {
	s := &provisioner.DucklingStatus{}
	s.MetadataStore.Endpoint = "duckling-acme.cluster-xyz.us-east-1.rds.amazonaws.com"
	s.MetadataStore.PgBouncerEndpoint = "pgb-acme.svc:6432"
	s.MetadataStore.User = "ducklingacme"
	s.MetadataStore.Password = "s3cr3t"
	s.MetadataStore.Database = "ducklingacme"
	s.DataStore.BucketName = "posthog-duckling-acme"
	s.DataStore.S3Region = "us-east-1"
	s.IAMRoleARN = "arn:aws:iam::123:role/duckling-acme"
	return s
}

func TestResolverFromDucklingCR(t *testing.T) {
	// No env fallback configured, so this must come purely from the CR.
	resolve := func(context.Context, string) (*provisioner.DucklingStatus, error) {
		return ducklingStatusWithWarehouse(), nil
	}
	r := newLakekeeperInputsResolver(resolve)

	in, err := r(context.Background(), &configstore.ManagedWarehouse{OrgID: "acme"})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}

	// Admin DSN must target the DIRECT endpoint (not the PgBouncer pooler),
	// be URL-form, carry the master creds, and request sslmode=require.
	u, perr := url.Parse(in.AdminDSN)
	if perr != nil {
		t.Fatalf("admin DSN not a URL: %q (%v)", in.AdminDSN, perr)
	}
	if u.Hostname() != "duckling-acme.cluster-xyz.us-east-1.rds.amazonaws.com" {
		t.Errorf("admin DSN host = %q, want direct RDS endpoint (not pgbouncer)", u.Hostname())
	}
	if strings.Contains(in.AdminDSN, "pgb-acme") || strings.Contains(in.AdminDSN, "6432") {
		t.Errorf("admin DSN must not use the PgBouncer pooler: %q", in.AdminDSN)
	}
	if pw, _ := u.User.Password(); pw != "s3cr3t" || u.User.Username() != "ducklingacme" {
		t.Errorf("admin DSN creds wrong: %q", u.Redacted())
	}
	if got := u.Query().Get("sslmode"); got != "require" {
		t.Errorf("sslmode = %q, want require", got)
	}

	if in.PGHost != "duckling-acme.cluster-xyz.us-east-1.rds.amazonaws.com" || in.PGPort != 5432 {
		t.Errorf("PGHost/PGPort = %q/%d, want direct endpoint:5432", in.PGHost, in.PGPort)
	}
	if in.PGSSLMode != "require" {
		t.Errorf("PGSSLMode = %q, want require", in.PGSSLMode)
	}
	if in.S3.Bucket != "posthog-duckling-acme" || in.S3.Region != "us-east-1" || in.S3.Flavor != "aws" {
		t.Errorf("S3 = %+v, want bucket/region/aws from CR", in.S3)
	}
	if in.S3.StaticAccessKeyID != "" || in.S3.StaticAccessKeySecret != "" {
		t.Errorf("prod S3 must use pod IRSA, not static creds: %+v", in.S3)
	}
	if in.S3.RoleARN != "arn:aws:iam::123:role/duckling-acme" {
		t.Errorf("S3.RoleARN = %q, want the duckling role from CR status (for STS vending)", in.S3.RoleARN)
	}
	if len(in.KubernetesAuthAudiences) != 0 {
		t.Errorf("expected allowall mode (no audiences), got %v", in.KubernetesAuthAudiences)
	}
}

func TestResolverFromCnpgShardCR(t *testing.T) {
	// cnpg-shard: the Lakekeeper DB + role are pre-provisioned by provider-sql
	// on the shard, and status carries the per-tenant role creds + the
	// session-mode Pooler endpoint. The resolver must produce pre-provisioned
	// inputs with NO AdminDSN (the provisioner must not attempt CREATE
	// DATABASE/ROLE), taking the role creds verbatim.
	resolve := func(context.Context, string) (*provisioner.DucklingStatus, error) {
		s := &provisioner.DucklingStatus{}
		s.MetadataStore.Type = "cnpg-shard"
		s.MetadataStore.Endpoint = "shard-001-pooler.cnpg-shards.svc.cluster.local"
		s.MetadataStore.User = "lakekeeper_acme"
		s.MetadataStore.Password = "from-provider-sql"
		s.MetadataStore.Database = "lakekeeper_acme"
		s.DataStore.BucketName = "posthog-duckling-acme"
		s.DataStore.S3Region = "us-east-1"
		s.IAMRoleARN = "arn:aws:iam::123:role/duckling-acme"
		return s, nil
	}
	r := newLakekeeperInputsResolver(resolve)

	in, err := r(context.Background(), &configstore.ManagedWarehouse{OrgID: "acme"})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}

	if !in.PGPreProvisioned {
		t.Fatalf("PGPreProvisioned = false, want true for cnpg-shard")
	}
	if in.AdminDSN != "" {
		t.Errorf("AdminDSN = %q, want empty (no privileged DDL in cnpg-shard mode)", in.AdminDSN)
	}
	if in.PGUser != "lakekeeper_acme" || in.PGPassword != "from-provider-sql" || in.PGDatabase != "lakekeeper_acme" {
		t.Errorf("PG creds = %q/%q/%q, want the provider-sql role verbatim", in.PGUser, in.PGPassword, in.PGDatabase)
	}
	if in.PGHost != "shard-001-pooler.cnpg-shards.svc.cluster.local" || in.PGPort != 5432 {
		t.Errorf("PGHost/PGPort = %q/%d, want the session Pooler:5432", in.PGHost, in.PGPort)
	}
	if in.S3.Bucket != "posthog-duckling-acme" || in.S3.RoleARN != "arn:aws:iam::123:role/duckling-acme" {
		t.Errorf("S3 = %+v, want bucket/roleARN from CR status", in.S3)
	}
}

func TestResolverFromEnvFallback(t *testing.T) {
	t.Setenv(envLakekeeperAdminDSN, "postgres://admin:pw@127.0.0.1:5432/postgres?sslmode=disable")
	t.Setenv(envLakekeeperPGHost, "127.0.0.1")
	t.Setenv(envLakekeeperPGPort, "5432")
	t.Setenv(envLakekeeperPGSSLMode, "disable")
	t.Setenv(envLakekeeperS3Bucket, "warehouse")
	t.Setenv(envLakekeeperS3Region, "us-east-1")
	t.Setenv(envLakekeeperS3Endpoint, "http://minio.minio.svc:9000")
	t.Setenv(envLakekeeperS3Flavor, "s3-compat")
	t.Setenv(envLakekeeperS3KeyID, "minioadmin")
	t.Setenv(envLakekeeperS3Secret, "minioadmin")

	// nil resolver → straight to env fallback.
	r := newLakekeeperInputsResolver(nil)
	in, err := r(context.Background(), &configstore.ManagedWarehouse{OrgID: "dev"})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	if in.AdminDSN == "" || in.PGHost != "127.0.0.1" || in.PGPort != 5432 {
		t.Errorf("env inputs wrong: %+v", in)
	}
	if in.PGSSLMode != "disable" {
		t.Errorf("PGSSLMode = %q, want disable", in.PGSSLMode)
	}
	if in.S3.Endpoint != "http://minio.minio.svc:9000" || in.S3.Flavor != "s3-compat" {
		t.Errorf("S3 = %+v, want MinIO endpoint + s3-compat", in.S3)
	}
	if in.S3.StaticAccessKeyID != "minioadmin" || in.S3.StaticAccessKeySecret != "minioadmin" {
		t.Errorf("S3 static creds not propagated: %+v", in.S3)
	}
}

func TestResolverDucklingErrorFallsBackToEnv(t *testing.T) {
	t.Setenv(envLakekeeperAdminDSN, "postgres://admin:pw@127.0.0.1:5432/postgres?sslmode=disable")
	t.Setenv(envLakekeeperS3Bucket, "warehouse")
	t.Setenv(envLakekeeperS3Region, "us-east-1")

	// CR resolver errors (e.g. no Duckling for this org) → env fallback used.
	resolve := func(context.Context, string) (*provisioner.DucklingStatus, error) {
		return nil, errors.New("duckling not found")
	}
	r := newLakekeeperInputsResolver(resolve)
	in, err := r(context.Background(), &configstore.ManagedWarehouse{OrgID: "dev"})
	if err != nil {
		t.Fatalf("expected env fallback, got error: %v", err)
	}
	if in.S3.Bucket != "warehouse" {
		t.Errorf("expected env inputs, got %+v", in)
	}
}

func TestResolverIncompleteCRWithoutEnvErrors(t *testing.T) {
	// CR present but missing bucket, and no env configured → hard error.
	resolve := func(context.Context, string) (*provisioner.DucklingStatus, error) {
		s := &provisioner.DucklingStatus{}
		s.MetadataStore.Endpoint = "host"
		s.MetadataStore.User = "u"
		s.MetadataStore.Password = "p"
		s.MetadataStore.Database = "d"
		// DataStore left empty.
		return s, nil
	}
	r := newLakekeeperInputsResolver(resolve)
	if _, err := r(context.Background(), &configstore.ManagedWarehouse{OrgID: "acme"}); err == nil {
		t.Fatal("expected error for incomplete CR with no env fallback")
	}
}

func TestResolverNoSourcesErrors(t *testing.T) {
	r := newLakekeeperInputsResolver(nil)
	if _, err := r(context.Background(), &configstore.ManagedWarehouse{OrgID: "x"}); err == nil {
		t.Fatal("expected error when neither CR nor env provide inputs")
	}
}

func TestLakekeeperProvisionerEnabledToggle(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"", false}, {"true", true}, {"1", true}, {"yes", true}, {"TRUE", true}, {"false", false}, {"0", false},
	} {
		t.Setenv(envLakekeeperEnabled, tc.val)
		if got := lakekeeperProvisionerEnabled(); got != tc.want {
			t.Errorf("enabled(%q) = %v, want %v", tc.val, got, tc.want)
		}
	}
}
