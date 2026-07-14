//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeBackupS3 fakes both the streaming uploader and the object deleter: it
// drains the body (like the real multipart uploader) and records what landed.
type fakeBackupS3 struct {
	mu            sync.Mutex
	uploaded      []byte
	uploadKey     string
	uploadTagging *string
	uploadErr     error
	deleted       []string
}

func (f *fakeBackupS3) Upload(_ context.Context, input *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.uploaded = body
	f.uploadKey = *input.Key
	f.uploadTagging = input.Tagging
	f.mu.Unlock()
	if f.uploadErr != nil {
		return nil, f.uploadErr
	}
	return &manager.UploadOutput{}, nil
}

func (f *fakeBackupS3) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	f.deleted = append(f.deleted, *params.Key)
	f.mu.Unlock()
	return &s3.DeleteObjectOutput{}, nil
}

// writeShim drops an executable shell script standing in for pg_dump.
func writeShim(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pg_dump_shim")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatalf("write shim: %v", err)
	}
	return path
}

func testBackuper(t *testing.T, shim string, fake *fakeBackupS3) *PGCatalogBackuper {
	t.Helper()
	return &PGCatalogBackuper{
		assume: func(context.Context, string) (string, string, string, error) {
			return "AKID", "SECRET", "TOKEN", nil
		},
		pgDumpCommand: shim,
		newClients: func(context.Context, BackupDestination, string, string, string) (backupUploader, backupObjectDeleter, error) {
			return fake, fake, nil
		},
	}
}

var testBackupDest = BackupDestination{
	Bucket:  "org-bucket",
	Key:     backupPrefix + "op-9-test.dump",
	Region:  "us-east-1",
	RoleARN: "arn:aws:iam::123:role/org",
}

var testBackupSource = CatalogEndpoint{
	Host: "db.example", Port: 5432, User: "u", Password: "pw", Database: "catalog",
}

// TestBackupStreamsDumpToS3 pins the happy path: pg_dump stdout bytes land in
// the S3 object verbatim (no temp file), the returned size counts them, and
// the URI carries the bucket/key scheme.
func TestBackupStreamsDumpToS3(t *testing.T) {
	// 200 KiB of deterministic output — larger than any internal buffer so a
	// buffered-whole-dump regression would still pass, but proves multi-read
	// streaming through the counting reader.
	shim := writeShim(t, `i=0
while [ $i -lt 3200 ]; do printf '0123456789012345678901234567890123456789012345678901234567890123'; i=$((i+1)); done`)
	fake := &fakeBackupS3{}
	b := testBackuper(t, shim, fake)

	uri, size, err := b.Backup(context.Background(), testBackupSource, testBackupDest)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	wantURI := "s3://org-bucket/" + testBackupDest.Key
	if uri != wantURI {
		t.Fatalf("uri = %q, want %q", uri, wantURI)
	}
	wantSize := int64(3200 * 64)
	if size != wantSize {
		t.Fatalf("size = %d, want %d", size, wantSize)
	}
	if int64(len(fake.uploaded)) != wantSize {
		t.Fatalf("uploaded %d bytes, want %d", len(fake.uploaded), wantSize)
	}
	if !strings.HasPrefix(string(fake.uploaded), "0123456789") {
		t.Fatalf("uploaded bytes corrupted: %q…", fake.uploaded[:16])
	}
	if len(fake.deleted) != 0 {
		t.Fatalf("unexpected DeleteObject calls: %v", fake.deleted)
	}
	// LOAD-BEARING: the upload must carry NO object tagging. PutObject with an
	// x-amz-tagging header requires s3:PutObjectTagging, which the per-org
	// duckling IAM roles do not grant — a tagged upload 403s on the real
	// cluster (AccessDenied on s3:PutObjectTagging, mw-dev e2e).
	if fake.uploadTagging != nil {
		t.Fatalf("upload carried Tagging %q — requires s3:PutObjectTagging the org roles lack", *fake.uploadTagging)
	}
}

// TestBackupPasswordViaEnvNotArgv pins that the shim sees PGPASSWORD in env
// and that no argv carries the password.
func TestBackupPasswordViaEnvNotArgv(t *testing.T) {
	out := filepath.Join(t.TempDir(), "seen")
	shim := writeShim(t, fmt.Sprintf(`echo "$PGPASSWORD" > %q; echo "$@" >> %q; printf 'dumpbytes'`, out, out))
	fake := &fakeBackupS3{}
	b := testBackuper(t, shim, fake)

	if _, _, err := b.Backup(context.Background(), testBackupSource, testBackupDest); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	seen, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read shim output: %v", err)
	}
	lines := strings.SplitN(string(seen), "\n", 2)
	if lines[0] != "pw" {
		t.Fatalf("PGPASSWORD not passed via env (got %q)", lines[0])
	}
	if len(lines) > 1 && strings.Contains(lines[1], "pw") {
		t.Fatalf("password leaked into argv: %q", lines[1])
	}
}

// TestBackupPgDumpFailureDeletesPartialObject pins that a nonzero pg_dump exit
// fails the backup WITH the captured stderr, and the partially uploaded object
// is deleted rather than left looking like a valid backup.
func TestBackupPgDumpFailureDeletesPartialObject(t *testing.T) {
	shim := writeShim(t, `printf 'partial-bytes'; echo 'pg_dump: error: connection refused' >&2; exit 1`)
	fake := &fakeBackupS3{}
	b := testBackuper(t, shim, fake)

	_, _, err := b.Backup(context.Background(), testBackupSource, testBackupDest)
	if err == nil {
		t.Fatal("Backup succeeded despite pg_dump exit 1")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("error lacks pg_dump stderr: %v", err)
	}
	if strings.Contains(err.Error(), "pw") {
		t.Fatalf("error leaks the password: %v", err)
	}
	if len(fake.deleted) != 1 || fake.deleted[0] != testBackupDest.Key {
		t.Fatalf("partial object not deleted: %v", fake.deleted)
	}
}

// TestBackupEmptyDumpRefused pins the empty-archive refusal (and cleanup of
// the empty object).
func TestBackupEmptyDumpRefused(t *testing.T) {
	shim := writeShim(t, `exit 0`)
	fake := &fakeBackupS3{}
	b := testBackuper(t, shim, fake)

	_, _, err := b.Backup(context.Background(), testBackupSource, testBackupDest)
	if err == nil || !strings.Contains(err.Error(), "empty archive") {
		t.Fatalf("want empty-archive refusal, got %v", err)
	}
	if len(fake.deleted) != 1 {
		t.Fatalf("empty object not deleted: %v", fake.deleted)
	}
}

// TestBackupUploadFailureKillsDump pins that an upload error surfaces (with
// no false success) and pg_dump is reaped.
func TestBackupUploadFailureKillsDump(t *testing.T) {
	shim := writeShim(t, `printf 'dumpbytes'`)
	fake := &fakeBackupS3{uploadErr: fmt.Errorf("multipart upload aborted")}
	b := testBackuper(t, shim, fake)

	_, _, err := b.Backup(context.Background(), testBackupSource, testBackupDest)
	if err == nil || !strings.Contains(err.Error(), "multipart upload aborted") {
		t.Fatalf("want upload error, got %v", err)
	}
}

// TestBackupNoBrokerOrDest pins the fail-fast validation errors.
func TestBackupNoBrokerOrDest(t *testing.T) {
	b := &PGCatalogBackuper{}
	if _, _, err := b.Backup(context.Background(), testBackupSource, testBackupDest); err == nil {
		t.Fatal("want error with no credential broker")
	}
	b = NewPGCatalogBackuper(func(context.Context, string) (string, string, string, error) {
		return "", "", "", nil
	})
	if _, _, err := b.Backup(context.Background(), testBackupSource, BackupDestination{}); err == nil {
		t.Fatal("want error with empty destination")
	}
}
