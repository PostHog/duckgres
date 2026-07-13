//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Pre-flip catalog backup for reshard operations ("safety net" layer C): before
// a reshard mutates a tenant's DuckLake metadata catalog, dump the SOURCE
// catalog and upload it to the org's OWN S3 data bucket under a reserved prefix
// so recovery is a one-command `pg_restore`. Only the Postgres catalog metadata
// (`ducklake_*` tables) is ever at risk in a reshard — the S3 parquet DATA is
// never touched — so backing up exactly that metadata is the whole belt.
//
// The artifact is a standard `pg_dump --format=custom` archive (restorable with
// stock `pg_restore`), keyed s3://<org-data-bucket>/_reshard_catalog_backups/
// op-<opID>-<UTC-timestamp>.dump. The upload uses short-lived credentials from
// the org's own IAM role (the same STS AssumeRole the worker activator uses for
// DuckLake S3 access), so the CP writes to the org bucket without a worker.

// backupPrefix is the reserved S3 key prefix under the org data bucket that
// reshard catalog backups land under — kept distinct from DuckLake's parquet so
// a lifecycle rule can expire it and it never collides with real data.
const backupPrefix = "_reshard_catalog_backups/"

// backupObjectTag is applied to every uploaded artifact so an S3 lifecycle rule
// can target reshard catalog backups by tag (see docs/design/resharding.md).
const backupObjectTag = "duckgres-reshard-catalog-backup=1"

// BackupDestination describes where a catalog backup artifact is written and
// which org IAM role to assume for write credentials.
type BackupDestination struct {
	Bucket  string // org data bucket (status.dataStore.bucketName)
	Key     string // object key under backupPrefix
	Region  string // status.dataStore.s3Region ("" → SDK default)
	RoleARN string // org IAM role (status.iamRoleARN) assumed for S3 write creds
}

// CatalogBackuper dumps a source catalog and uploads it; the runner drives it.
// The real impl (PGCatalogBackuper) shells pg_dump and PutObjects to S3; tests
// fake it.
type CatalogBackuper interface {
	// Backup dumps source's public-schema ducklake_* tables and uploads the
	// archive to dest. Returns the s3:// URI and the artifact byte size.
	Backup(ctx context.Context, source CatalogEndpoint, dest BackupDestination) (uri string, size int64, err error)
}

// AssumeRoleFunc mints short-lived AWS credentials for an IAM role ARN. The
// control plane injects an adapter over its STS broker (which lives in the
// controlplane package and cannot be imported here without a cycle).
type AssumeRoleFunc func(ctx context.Context, roleARN string) (accessKeyID, secretAccessKey, sessionToken string, err error)

// PGCatalogBackuper is the real CatalogBackuper: it runs `pg_dump` (which must
// be on PATH in the CP image — postgresql-client, pinned to PG 18 to match the
// cnpg shard major) against the source and uploads the archive to S3 with
// credentials brokered by assume.
type PGCatalogBackuper struct {
	assume AssumeRoleFunc
}

// NewPGCatalogBackuper builds the real backuper over an AssumeRole adapter.
func NewPGCatalogBackuper(assume AssumeRoleFunc) *PGCatalogBackuper {
	return &PGCatalogBackuper{assume: assume}
}

func (b *PGCatalogBackuper) Backup(ctx context.Context, source CatalogEndpoint, dest BackupDestination) (string, int64, error) {
	if b.assume == nil {
		return "", 0, fmt.Errorf("catalog backuper has no credential broker (STS unavailable)")
	}
	if dest.Bucket == "" || dest.RoleARN == "" {
		return "", 0, fmt.Errorf("backup destination missing bucket (%q) or role (%q)", dest.Bucket, dest.RoleARN)
	}

	// 1. pg_dump the source catalog to a temp file (custom format → pg_restore).
	tmp, err := os.CreateTemp("", "reshard-catalog-*.dump")
	if err != nil {
		return "", 0, fmt.Errorf("create temp dump file: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer func() { _ = os.Remove(tmpPath) }()

	port := source.Port
	if port == 0 {
		port = 5432
	}
	sslMode := source.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	// Password goes via PGPASSWORD env, NEVER argv (argv is world-visible in
	// /proc). pg_dump does not echo the password on any output path.
	cmd := exec.CommandContext(ctx, "pg_dump",
		"--format=custom",
		"--no-owner",
		"--schema=public",
		"--file="+tmpPath,
		"--host="+source.Host,
		"--port="+strconv.Itoa(port),
		"--username="+source.User,
		"--dbname="+source.Database,
	)
	cmd.Env = append(os.Environ(),
		"PGPASSWORD="+source.Password,
		"PGSSLMODE="+sslMode,
		"PGCONNECT_TIMEOUT=10",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		// pg_dump's diagnostics reference --host/--username/--dbname only (no
		// password), so echoing them is safe and operator-useful.
		return "", 0, fmt.Errorf("pg_dump %s: %w: %s", source.Redacted(), err, string(out))
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("open dump file: %w", err)
	}
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	if err != nil {
		return "", 0, fmt.Errorf("stat dump file: %w", err)
	}
	if stat.Size() == 0 {
		return "", 0, fmt.Errorf("pg_dump produced an empty archive — refusing to record a useless backup")
	}

	// 2. Upload with the org's own short-lived S3 credentials.
	accessKeyID, secretAccessKey, sessionToken, err := b.assume(ctx, dest.RoleARN)
	if err != nil {
		return "", 0, fmt.Errorf("assume org role %s for backup upload: %w", dest.RoleARN, err)
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken),
		),
	}
	if dest.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(dest.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return "", 0, fmt.Errorf("load AWS config for backup upload: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	size := stat.Size()
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(dest.Bucket),
		Key:           aws.String(dest.Key),
		Body:          f,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
		Tagging:       aws.String(backupObjectTag),
	}); err != nil {
		return "", 0, fmt.Errorf("upload backup to s3://%s/%s: %w", dest.Bucket, dest.Key, err)
	}

	return "s3://" + dest.Bucket + "/" + dest.Key, size, nil
}
