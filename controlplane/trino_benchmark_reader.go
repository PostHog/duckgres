package controlplane

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// Reader identity resolution.
//
// A benchmark Trino cluster reads the SAME DuckLake snapshot the Duckgres
// worker is being compared against, but it must do so with a strictly separate,
// read-only identity created by the companion charts release:
//
//   - a metadata-Postgres role with only the SELECT privileges the DuckLake
//     catalog needs, whose password lives in a Kubernetes Secret the control
//     plane may read by exact name; and
//   - an IAM role with only s3:ListBucket + s3:GetObject on the warehouse's own
//     data bucket, assumed by the Trino pods for renewable credentials.
//
// The tenant's DuckLake WRITER role and login are never an acceptable
// substitute. If any reader field is absent, resolution fails with
// ErrTrinoBenchmarkConfig and the whole benchmark is refused — there is no
// fallback path by construction.

// TrinoReaderSecretRef identifies one key in a namespaced Kubernetes Secret.
// This is a reference, never a value: the control plane reads the value only at
// the moment it materializes the short-lived benchmark Secret.
type TrinoReaderSecretRef struct {
	Name      string
	Namespace string
	Key       string
}

func (r TrinoReaderSecretRef) String() string {
	return r.Namespace + "/" + r.Name + "#" + r.Key
}

func (r TrinoReaderSecretRef) complete() bool {
	return r.Name != "" && r.Namespace != "" && r.Key != ""
}

// TrinoReaderSource is the raw state a resolver collects before validation. It
// includes the WRITER identity purely so buildTrinoReaderIdentity can REFUSE a
// configuration that would hand Trino writer credentials.
type TrinoReaderSource struct {
	MetadataEndpoint       string // host or host:port
	MetadataDatabase       string
	MetadataUser           string
	MetadataPasswordSecret TrinoReaderSecretRef
	Bucket                 string
	Region                 string
	DataPath               string // optional; derived from Bucket when empty
	ReadOnlyRoleARN        string
	// SSLMode is the JDBC sslmode for the metadata connection. Empty defaults
	// to "require"; an in-cluster PgBouncer hop is "disable" (the pooler
	// carries TLS onward), mirroring MetadataPostgresURL.
	SSLMode string

	// WriterRoleARN / WriterUser are the tenant's own write identities. They
	// are never used to configure Trino — only compared against, so a
	// misconfigured charts release cannot quietly grant write access.
	WriterRoleARN string
	WriterUser    string
}

// TrinoReaderIdentity is the validated, credential-free reader identity. Every
// field is safe to log; the password is represented only by its Secret
// reference.
type TrinoReaderIdentity struct {
	MetadataHost           string
	MetadataPort           int
	MetadataDatabase       string
	MetadataUser           string
	MetadataPasswordSecret TrinoReaderSecretRef
	Bucket                 string
	Region                 string
	DataPath               string
	ReadOnlyRoleARN        string
	SSLMode                string
}

// TrinoReaderResolver produces the reader identity for one org. The production
// implementation reads the Duckling CR status and the config store; unit tests
// use a fake.
type TrinoReaderResolver interface {
	ResolveTrinoReader(ctx context.Context, orgID string) (TrinoReaderIdentity, error)
}

// JDBCURL renders the connection URL the Brikk DuckLake connector's
// ducklake.catalog.database-url property takes. It never contains credentials —
// user and password are separate properties.
func (i TrinoReaderIdentity) JDBCURL() string {
	return "jdbc:postgresql://" + net.JoinHostPort(i.MetadataHost, strconv.Itoa(i.MetadataPort)) +
		"/" + i.MetadataDatabase + "?sslmode=" + i.SSLMode
}

// String renders the identity for logs. Safe by construction: the only
// credential is a Secret reference.
func (i TrinoReaderIdentity) String() string {
	return fmt.Sprintf("metadata=%s user=%s password_secret=%s data_path=%s region=%s role=%s",
		net.JoinHostPort(i.MetadataHost, strconv.Itoa(i.MetadataPort)),
		i.MetadataUser, i.MetadataPasswordSecret, i.DataPath, i.Region, i.ReadOnlyRoleARN)
}

// iamRoleARNRe matches an IAM ROLE ARN specifically — a user ARN or a bucket
// ARN in this position means the charts release published the wrong thing.
var iamRoleARNRe = regexp.MustCompile(`^arn:aws[a-z-]*:iam::\d{12}:role/.+$`)

// buildTrinoReaderIdentity validates a source and fails closed. The error text
// names the missing or colliding field so an operator can fix the charts
// release; it never contains a credential value (the source has none).
func buildTrinoReaderIdentity(source TrinoReaderSource) (TrinoReaderIdentity, error) {
	var missing []string
	for _, field := range []struct {
		name  string
		value string
	}{
		{"metadata endpoint", source.MetadataEndpoint},
		{"metadata database", source.MetadataDatabase},
		{"metadata reader user", source.MetadataUser},
		{"data bucket", source.Bucket},
		{"data bucket region", source.Region},
		{"read-only S3 role ARN", source.ReadOnlyRoleARN},
	} {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
	}
	if !source.MetadataPasswordSecret.complete() {
		missing = append(missing, "metadata reader password Secret reference (name, namespace, key)")
	}
	if len(missing) > 0 {
		return TrinoReaderIdentity{}, fmt.Errorf(
			"%w: the charts-created Trino reader identity is missing %s",
			ErrTrinoBenchmarkConfig, strings.Join(missing, ", "))
	}

	if !iamRoleARNRe.MatchString(source.ReadOnlyRoleARN) {
		return TrinoReaderIdentity{}, fmt.Errorf(
			"%w: read-only S3 role %q is not an IAM role ARN",
			ErrTrinoBenchmarkConfig, source.ReadOnlyRoleARN)
	}
	// Fail closed rather than hand Trino the tenant's write identity.
	if source.WriterRoleARN != "" && source.ReadOnlyRoleARN == source.WriterRoleARN {
		return TrinoReaderIdentity{}, fmt.Errorf(
			"%w: the Trino reader S3 role equals the warehouse writer role %q",
			ErrTrinoBenchmarkConfig, source.WriterRoleARN)
	}
	if source.WriterUser != "" && source.MetadataUser == source.WriterUser {
		return TrinoReaderIdentity{}, fmt.Errorf(
			"%w: the Trino metadata reader user equals the warehouse writer user %q",
			ErrTrinoBenchmarkConfig, source.WriterUser)
	}

	host, port, err := splitMetadataEndpoint(source.MetadataEndpoint)
	if err != nil {
		return TrinoReaderIdentity{}, fmt.Errorf("%w: %v", ErrTrinoBenchmarkConfig, err)
	}

	sslMode := strings.TrimSpace(source.SSLMode)
	if sslMode == "" {
		sslMode = "require"
	}
	switch sslMode {
	case "disable", "prefer", "require", "verify-ca", "verify-full":
	default:
		return TrinoReaderIdentity{}, fmt.Errorf(
			"%w: unsupported metadata sslmode %q", ErrTrinoBenchmarkConfig, sslMode)
	}

	dataPath := strings.TrimSpace(source.DataPath)
	if dataPath == "" {
		dataPath = "s3://" + source.Bucket + "/"
	}
	if !strings.HasPrefix(dataPath, "s3://") {
		return TrinoReaderIdentity{}, fmt.Errorf(
			"%w: data path %q is not an s3:// URI", ErrTrinoBenchmarkConfig, dataPath)
	}

	return TrinoReaderIdentity{
		MetadataHost:           host,
		MetadataPort:           port,
		MetadataDatabase:       source.MetadataDatabase,
		MetadataUser:           source.MetadataUser,
		MetadataPasswordSecret: source.MetadataPasswordSecret,
		Bucket:                 source.Bucket,
		Region:                 source.Region,
		DataPath:               dataPath,
		ReadOnlyRoleARN:        source.ReadOnlyRoleARN,
		SSLMode:                sslMode,
	}, nil
}

// splitMetadataEndpoint accepts "host" or "host:port"; a bare host defaults to
// the Postgres port.
func splitMetadataEndpoint(endpoint string) (string, int, error) {
	endpoint = strings.TrimSpace(endpoint)
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint, 5432, nil
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("metadata endpoint %q has an invalid port", endpoint)
	}
	return host, port, nil
}
