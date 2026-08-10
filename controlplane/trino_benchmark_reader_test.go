package controlplane

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func completeTrinoReaderSource() TrinoReaderSource {
	return TrinoReaderSource{
		MetadataEndpoint: "duckling-bench-org-pgbouncer.ducklings.svc.cluster.local:6432",
		MetadataDatabase: "ducklake_bench_org",
		MetadataUser:     "trino_reader_bench_org",
		MetadataPasswordSecret: TrinoReaderSecretRef{
			Name:      "duckling-bench-org-trino-reader",
			Namespace: "ducklings",
			Key:       "password",
		},
		Bucket:          "posthog-duckling-benchorg-dev",
		Region:          "us-east-1",
		ReadOnlyRoleARN: "arn:aws:iam::123456789012:role/duckling-bench-org-trino-reader",
		SSLMode:         "disable",
		WriterRoleARN:   "arn:aws:iam::123456789012:role/duckling-bench-org",
		WriterUser:      "ducklake_bench_org",
	}
}

func TestBuildTrinoReaderIdentityFromCompleteSource(t *testing.T) {
	identity, err := buildTrinoReaderIdentity(completeTrinoReaderSource())
	if err != nil {
		t.Fatalf("buildTrinoReaderIdentity returned error: %v", err)
	}
	if identity.MetadataHost != "duckling-bench-org-pgbouncer.ducklings.svc.cluster.local" || identity.MetadataPort != 6432 {
		t.Fatalf("metadata endpoint = %s:%d", identity.MetadataHost, identity.MetadataPort)
	}
	if got, want := identity.JDBCURL(), "jdbc:postgresql://duckling-bench-org-pgbouncer.ducklings.svc.cluster.local:6432/ducklake_bench_org?sslmode=disable"; got != want {
		t.Fatalf("JDBCURL = %q, want %q", got, want)
	}
	if identity.MetadataUser != "trino_reader_bench_org" {
		t.Fatalf("metadata user = %q", identity.MetadataUser)
	}
	if identity.DataPath != "s3://posthog-duckling-benchorg-dev/" {
		t.Fatalf("data path = %q", identity.DataPath)
	}
	if identity.ReadOnlyRoleARN != "arn:aws:iam::123456789012:role/duckling-bench-org-trino-reader" {
		t.Fatalf("read-only role = %q", identity.ReadOnlyRoleARN)
	}
}

func TestBuildTrinoReaderIdentityDefaultsMetadataPort(t *testing.T) {
	source := completeTrinoReaderSource()
	source.MetadataEndpoint = "bench-org.rds.example.com"

	identity, err := buildTrinoReaderIdentity(source)
	if err != nil {
		t.Fatalf("buildTrinoReaderIdentity returned error: %v", err)
	}
	if identity.MetadataPort != 5432 {
		t.Fatalf("metadata port = %d, want the Postgres default 5432", identity.MetadataPort)
	}
}

func TestBuildTrinoReaderIdentityHonoursExplicitDataPath(t *testing.T) {
	source := completeTrinoReaderSource()
	source.DataPath = "s3://posthog-duckling-benchorg-dev/ducklake/"

	identity, err := buildTrinoReaderIdentity(source)
	if err != nil {
		t.Fatalf("buildTrinoReaderIdentity returned error: %v", err)
	}
	if identity.DataPath != "s3://posthog-duckling-benchorg-dev/ducklake/" {
		t.Fatalf("data path = %q", identity.DataPath)
	}
}

// Fail-closed: every reader field the charts publish is mandatory. A partially
// deployed charts release must never produce a half-configured Trino cluster.
func TestBuildTrinoReaderIdentityFailsClosedOnMissingFields(t *testing.T) {
	for name, mutate := range map[string]func(*TrinoReaderSource){
		"metadata endpoint":  func(s *TrinoReaderSource) { s.MetadataEndpoint = "" },
		"metadata database":  func(s *TrinoReaderSource) { s.MetadataDatabase = "" },
		"metadata user":      func(s *TrinoReaderSource) { s.MetadataUser = "" },
		"secret name":        func(s *TrinoReaderSource) { s.MetadataPasswordSecret.Name = "" },
		"secret namespace":   func(s *TrinoReaderSource) { s.MetadataPasswordSecret.Namespace = "" },
		"secret key":         func(s *TrinoReaderSource) { s.MetadataPasswordSecret.Key = "" },
		"bucket":             func(s *TrinoReaderSource) { s.Bucket = "" },
		"region":             func(s *TrinoReaderSource) { s.Region = "" },
		"read-only role arn": func(s *TrinoReaderSource) { s.ReadOnlyRoleARN = "" },
	} {
		t.Run(name, func(t *testing.T) {
			source := completeTrinoReaderSource()
			mutate(&source)
			_, err := buildTrinoReaderIdentity(source)
			if err == nil {
				t.Fatalf("missing %s must fail closed", name)
			}
			if !errors.Is(err, ErrTrinoBenchmarkConfig) {
				t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
			}
		})
	}
}

// The whole point of the feature gate: Trino gets the charts-created read-only
// identity or nothing. It must never silently reuse the tenant writer role or
// the DuckLake writer login.
func TestBuildTrinoReaderIdentityRefusesWriterCredentials(t *testing.T) {
	t.Run("writer role arn", func(t *testing.T) {
		source := completeTrinoReaderSource()
		source.ReadOnlyRoleARN = source.WriterRoleARN
		_, err := buildTrinoReaderIdentity(source)
		if !errors.Is(err, ErrTrinoBenchmarkConfig) {
			t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
		}
		if !strings.Contains(err.Error(), "writer") {
			t.Fatalf("error %q should name the writer-role collision", err)
		}
	})

	t.Run("writer database user", func(t *testing.T) {
		source := completeTrinoReaderSource()
		source.MetadataUser = source.WriterUser
		_, err := buildTrinoReaderIdentity(source)
		if !errors.Is(err, ErrTrinoBenchmarkConfig) {
			t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
		}
		if !strings.Contains(err.Error(), "writer") {
			t.Fatalf("error %q should name the writer-user collision", err)
		}
	})
}

func TestBuildTrinoReaderIdentityRejectsMalformedRoleARN(t *testing.T) {
	for _, arn := range []string{
		"duckling-bench-org-trino-reader",
		"arn:aws:s3:::posthog-duckling-benchorg-dev",
		"arn:aws:iam::123456789012:user/duckling-bench-org-trino-reader",
	} {
		source := completeTrinoReaderSource()
		source.ReadOnlyRoleARN = arn
		if _, err := buildTrinoReaderIdentity(source); !errors.Is(err, ErrTrinoBenchmarkConfig) {
			t.Fatalf("role ARN %q: error = %v, want ErrTrinoBenchmarkConfig", arn, err)
		}
	}
}

// Structural tripwire: the resolved identity must stay a pure REFERENCE. If
// someone adds a credential VALUE field here it lands in every struct that
// embeds it, and from there in logs and errors.
func TestTrinoReaderIdentityCarriesNoCredentialValues(t *testing.T) {
	typ := reflect.TypeOf(TrinoReaderIdentity{})
	for i := 0; i < typ.NumField(); i++ {
		name := strings.ToLower(typ.Field(i).Name)
		for _, banned := range []string{"password", "secretkey", "accesskey", "token", "credential"} {
			// The Secret REFERENCE is fine; a value is not.
			if strings.Contains(name, banned) && typ.Field(i).Type != reflect.TypeOf(TrinoReaderSecretRef{}) {
				t.Fatalf("TrinoReaderIdentity.%s looks like a credential value; keep only Secret references here", typ.Field(i).Name)
			}
		}
	}
}

func TestTrinoReaderIdentityStringOmitsNothingSecretAndNamesTheSecretRef(t *testing.T) {
	identity, err := buildTrinoReaderIdentity(completeTrinoReaderSource())
	if err != nil {
		t.Fatalf("buildTrinoReaderIdentity returned error: %v", err)
	}
	got := identity.String()
	if !strings.Contains(got, "ducklings/duckling-bench-org-trino-reader#password") {
		t.Fatalf("String() = %q, want the Secret reference (name only)", got)
	}
	for _, banned := range []string{"hunter2", "aws-access-key", "aws-secret-key"} {
		if strings.Contains(got, banned) {
			t.Fatalf("String() = %q leaked %q", got, banned)
		}
	}
}
