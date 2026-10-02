//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const hoglakeStorageProbeBody = "duckgres storage readiness\n"

// TrinoHoglakeStorageCheck verifies a tenant role can use its exact catalog prefix.
type TrinoHoglakeStorageCheck func(ctx context.Context, roleARN, region, dataPath string) error

type hoglakeStorageClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// HoglakeStorageProbe gates initial catalog publication on usable tenant storage.
// Failed attempts reuse their object key; concurrent control planes have separate keys.
type HoglakeStorageProbe struct {
	assume     AssumeRoleFunc
	instanceID string
	newClient  func(context.Context, string, string, string, string) (hoglakeStorageClient, error)
}

func NewHoglakeStorageProbe(assume AssumeRoleFunc) *HoglakeStorageProbe {
	return &HoglakeStorageProbe{assume: assume, instanceID: uuid.NewString(), newClient: newHoglakeStorageClient}
}

func newHoglakeStorageClient(ctx context.Context, region, accessKey, secretKey, token string) (hoglakeStorageClient, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)),
		awsconfig.WithRetryMaxAttempts(1))
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func (p *HoglakeStorageProbe) Check(ctx context.Context, roleARN, region, dataPath string) error {
	if p.assume == nil || roleARN == "" || region == "" {
		return errors.New("hoglake storage readiness requires tenant credentials and region")
	}
	location, err := parseHoglakeDataPath(dataPath)
	if err != nil {
		return errors.New("invalid Hoglake storage readiness path")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	accessKey, secretKey, token, err := p.assume(ctx, roleARN)
	if err != nil || accessKey == "" || secretKey == "" || token == "" {
		return errors.New("waiting for Hoglake tenant storage credentials")
	}
	client, err := p.newClient(ctx, region, accessKey, secretKey, token)
	if err != nil {
		return errors.New("cannot initialize Hoglake storage readiness client")
	}
	digest := sha256.Sum256([]byte(roleARN + "\n" + region + "\n" + dataPath))
	key := strings.TrimPrefix(location.Path, "/") + ".duckgres-readiness/" + p.instanceID + "-" + fmt.Sprintf("%x", digest)
	bucket := location.Host
	_, writeErr := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(hoglakeStorageProbeBody)})
	var readErr error
	if writeErr == nil {
		readErr = readHoglakeStorageProbe(ctx, client, bucket, key)
	}
	// A timed-out PUT may have succeeded; cleanup must outlive the attempt's context.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cleanupCancel()
	_, deleteErr := client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	switch {
	case writeErr != nil:
		return errors.New("waiting for Hoglake tenant storage write access")
	case readErr != nil:
		return errors.New("waiting for Hoglake tenant storage read access")
	case deleteErr != nil:
		return errors.New("waiting for Hoglake tenant storage delete access")
	default:
		return nil
	}
}

func readHoglakeStorageProbe(ctx context.Context, client hoglakeStorageClient, bucket, key string) error {
	result, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	if result == nil || result.Body == nil {
		return errors.New("missing readiness object")
	}
	defer func() { _ = result.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(result.Body, int64(len(hoglakeStorageProbeBody)+1)))
	if err != nil {
		return err
	}
	if string(body) != hoglakeStorageProbeBody {
		return errors.New("readiness object content mismatch")
	}
	return nil
}
