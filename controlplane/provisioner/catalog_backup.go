//go:build kubernetes

package provisioner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
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
// DuckLake S3 access).
//
// The dump is STREAMED: pg_dump writes the archive to stdout and the bytes flow
// straight into an S3 multipart upload (feature/s3/manager). There is no temp
// file and nothing ever holds the whole dump in memory — peak memory is bounded
// by partSize × concurrency regardless of catalog size. (The previous
// buffer-to-temp-file + single PutObject implementation OOM-killed a 512Mi
// control-plane pod on a ~20k-table catalog; the reshard runner also no longer
// runs inside the CP — see docs/design/resharding.md.)

// backupPrefix is the reserved S3 key prefix under the org data bucket that
// reshard catalog backups land under — kept distinct from DuckLake's parquet so
// a prefix-scoped lifecycle rule can expire it and it never collides with real
// data.
const backupPrefix = "_reshard_catalog_backups/"

// NOTE: the artifacts are deliberately NOT object-tagged. PutObject with an
// x-amz-tagging header requires s3:PutObjectTagging, which the per-org
// duckling IAM roles do not grant — on the real cluster the tagged upload
// 403s (AccessDenied on s3:PutObjectTagging; observed in the mw-dev e2e).
// Lifecycle retention targets the reserved backupPrefix instead (see
// docs/design/resharding.md § Retention), which needs no extra permission.

// backupUploadPartSize / backupUploadConcurrency bound the streaming upload's
// memory: at most partSize × concurrency (+1 part being filled) is buffered,
// independent of the dump size.
const (
	backupUploadPartSize    = 16 * 1024 * 1024 // 16 MiB
	backupUploadConcurrency = 2
)

// BackupDestination describes where a catalog backup artifact is written and
// which org IAM role to assume for write credentials.
type BackupDestination struct {
	Bucket  string // org data bucket (status.dataStore.bucketName)
	Key     string // object key under backupPrefix
	Region  string // status.dataStore.s3Region ("" → SDK default)
	RoleARN string // org IAM role (status.iamRoleARN) assumed for S3 write creds
}

// CatalogBackuper dumps a source catalog and uploads it; the runner drives it.
// The real impl (PGCatalogBackuper) streams pg_dump stdout into an S3 multipart
// upload; tests fake it.
type CatalogBackuper interface {
	// Backup dumps source's public-schema ducklake_* tables and uploads the
	// archive to dest. Returns the s3:// URI and the artifact byte size.
	Backup(ctx context.Context, source CatalogEndpoint, dest BackupDestination) (uri string, size int64, err error)
}

// AssumeRoleFunc mints short-lived AWS credentials for an IAM role ARN. The
// reshard-runner process injects an adapter over an STS broker (which lives in
// the controlplane package and cannot be imported here without a cycle).
type AssumeRoleFunc func(ctx context.Context, roleARN string) (accessKeyID, secretAccessKey, sessionToken string, err error)

// backupUploader is the slice of manager.Uploader the backuper needs; the
// seam lets tests fake S3 without network.
type backupUploader interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

// backupObjectDeleter deletes a (partial/empty) uploaded object after a failed
// dump so a broken artifact is never left looking like a valid backup.
type backupObjectDeleter interface {
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// PGCatalogBackuper is the real CatalogBackuper: it runs `pg_dump` (which must
// be on PATH in the image — postgresql-client, pinned to PG 18 to match the
// cnpg shard major) against the source and streams the archive to S3 with
// credentials brokered by assume.
type PGCatalogBackuper struct {
	assume AssumeRoleFunc

	// pgDumpCommand overrides the pg_dump binary (tests point it at a shim).
	// Empty = "pg_dump".
	pgDumpCommand string
	// newClients overrides S3 client construction (tests inject fakes).
	newClients func(ctx context.Context, dest BackupDestination, accessKeyID, secretAccessKey, sessionToken string) (backupUploader, backupObjectDeleter, error)
}

// NewPGCatalogBackuper builds the real backuper over an AssumeRole adapter.
func NewPGCatalogBackuper(assume AssumeRoleFunc) *PGCatalogBackuper {
	return &PGCatalogBackuper{assume: assume}
}

// countingReader counts the bytes the uploader consumed from the pipe.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func newBackupS3Clients(ctx context.Context, dest BackupDestination, accessKeyID, secretAccessKey, sessionToken string) (backupUploader, backupObjectDeleter, error) {
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
		return nil, nil, fmt.Errorf("load AWS config for backup upload: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = backupUploadPartSize
		u.Concurrency = backupUploadConcurrency
	})
	return uploader, client, nil
}

func (b *PGCatalogBackuper) Backup(ctx context.Context, source CatalogEndpoint, dest BackupDestination) (string, int64, error) {
	if b.assume == nil {
		return "", 0, fmt.Errorf("catalog backuper has no credential broker (STS unavailable)")
	}
	if dest.Bucket == "" || dest.RoleARN == "" {
		return "", 0, fmt.Errorf("backup destination missing bucket (%q) or role (%q)", dest.Bucket, dest.RoleARN)
	}

	// Broker the org's short-lived S3 credentials FIRST so a credential problem
	// fails fast, before pg_dump ever runs.
	accessKeyID, secretAccessKey, sessionToken, err := b.assume(ctx, dest.RoleARN)
	if err != nil {
		return "", 0, fmt.Errorf("assume org role %s for backup upload: %w", dest.RoleARN, err)
	}
	newClients := b.newClients
	if newClients == nil {
		newClients = newBackupS3Clients
	}
	uploader, deleter, err := newClients(ctx, dest, accessKeyID, secretAccessKey, sessionToken)
	if err != nil {
		return "", 0, err
	}

	port := source.Port
	if port == 0 {
		port = 5432
	}
	sslMode := source.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	pgDump := b.pgDumpCommand
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	// pg_dump writes the custom-format archive to STDOUT (no --file, no temp
	// file); stderr is captured separately for diagnostics. The password goes
	// via PGPASSWORD env, NEVER argv (argv is world-visible in /proc). pg_dump
	// does not echo the password on any output path.
	cmd := exec.CommandContext(ctx, pgDump,
		"--format=custom",
		"--no-owner",
		"--schema=public",
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
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", 0, fmt.Errorf("pg_dump stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return "", 0, fmt.Errorf("start pg_dump %s: %w", source.Redacted(), err)
	}

	// Stream stdout → multipart upload. Size is unknown up front, so no
	// ContentLength is set; the manager aborts the multipart upload itself on
	// error, so a failed stream leaves no half-assembled object behind.
	counter := &countingReader{r: stdout}
	_, uploadErr := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(dest.Bucket),
		Key:         aws.String(dest.Key),
		Body:        counter,
		ContentType: aws.String("application/octet-stream"),
		// No Tagging: it would require s3:PutObjectTagging, which the org
		// duckling roles do not hold (see the package note above).
	})
	if uploadErr != nil {
		// The uploader stopped consuming the pipe; kill pg_dump so Wait can't
		// block on a full pipe, then reap it.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return "", 0, fmt.Errorf("upload backup to s3://%s/%s: %w", dest.Bucket, dest.Key, uploadErr)
	}

	// The upload saw EOF — that happens on pg_dump success AND failure, so the
	// exit status decides whether the completed object is a real backup. On a
	// nonzero exit the (truncated) object must not be left looking like one.
	deleteUploaded := func() {
		_, _ = deleter.DeleteObject(context.WithoutCancel(ctx), &s3.DeleteObjectInput{
			Bucket: aws.String(dest.Bucket),
			Key:    aws.String(dest.Key),
		})
	}
	if waitErr := cmd.Wait(); waitErr != nil {
		deleteUploaded()
		// pg_dump's diagnostics reference --host/--username/--dbname only (no
		// password), so echoing stderr is safe and operator-useful.
		return "", 0, fmt.Errorf("pg_dump %s: %w: %s", source.Redacted(), waitErr, stderr.String())
	}
	size := counter.n.Load()
	if size == 0 {
		deleteUploaded()
		return "", 0, fmt.Errorf("pg_dump produced an empty archive — refusing to record a useless backup")
	}

	return "s3://" + dest.Bucket + "/" + dest.Key, size, nil
}
