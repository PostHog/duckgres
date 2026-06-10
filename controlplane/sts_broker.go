//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"golang.org/x/sync/singleflight"
)

const (
	stsSessionDuration = 1 * time.Hour
	stsSessionName     = "duckgres-cp"

	// stsCacheSafetyMargin is how long before the credentials' true expiration
	// we stop serving them from cache and mint a fresh session instead. It must
	// leave enough room for a worker activation that receives cached creds to
	// complete its DuckLake/Iceberg attach before the creds lapse.
	stsCacheSafetyMargin = 10 * time.Minute

	// stsAssumeRoleTimeout bounds the underlying AWS AssumeRole call. The call
	// is detached from the triggering caller's context (other callers may be
	// waiting on its result via singleflight), so it needs its own deadline.
	stsAssumeRoleTimeout = 1 * time.Minute
)

// assumeRoleFunc mints fresh credentials for a role ARN. It is a field on
// STSBroker so tests can substitute a fake without an AWS client.
type assumeRoleFunc func(ctx context.Context, roleARN string) (*AssumedCredentials, error)

// STSBroker brokers short-lived AWS credentials by assuming per-org IAM roles.
//
// Credentials are cached per role ARN until their true Expiration minus
// stsCacheSafetyMargin, and concurrent AssumeRole calls for the same ARN are
// collapsed into a single in-flight AWS call. This keeps a burst of N worker
// activations for one org from issuing N identical STS calls (STS throttling
// or latency blips would otherwise stall worker activation directly).
type STSBroker struct {
	assumeRole assumeRoleFunc
	now        func() time.Time

	group singleflight.Group

	mu    sync.Mutex
	cache map[string]AssumedCredentials
}

// AssumedCredentials holds the temporary credentials from STS AssumeRole.
type AssumedCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// NewSTSBroker creates an STS broker using the control plane's own credentials.
func NewSTSBroker(ctx context.Context, region string) (*STSBroker, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := sts.NewFromConfig(cfg)
	return newSTSBroker(func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		return awsAssumeRole(ctx, client, roleARN)
	}), nil
}

// newSTSBroker wires the cache/single-flight machinery around the given
// credential-minting func. Split from NewSTSBroker so tests can inject a fake.
func newSTSBroker(assumeRole assumeRoleFunc) *STSBroker {
	return &STSBroker{
		assumeRole: assumeRole,
		now:        time.Now,
		cache:      make(map[string]AssumedCredentials),
	}
}

// AssumeRole returns short-lived credentials for the given IAM role ARN,
// serving from cache when valid (Expiration is always the true STS expiration;
// the credential-refresh scheduler relies on it being accurate). Concurrent
// calls for the same ARN share one underlying AWS call.
func (b *STSBroker) AssumeRole(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
	if creds, ok := b.cachedCreds(roleARN); ok {
		return creds, nil
	}
	ch := b.group.DoChan(roleARN, func() (any, error) {
		// Re-check the cache: a refresh may have landed while this call queued
		// behind a previous single-flight round.
		if creds, ok := b.cachedCreds(roleARN); ok {
			return creds, nil
		}
		// Detach from the triggering caller's context: other callers may be
		// waiting on this result, and one caller's cancellation must not fail
		// them all. Bounded by its own timeout instead.
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stsAssumeRoleTimeout)
		defer cancel()
		creds, err := b.assumeRole(callCtx, roleARN)
		if err != nil {
			// Not cached: singleflight forgets the key once the call
			// completes, so the next AssumeRole retries against AWS.
			return nil, err
		}
		b.storeCreds(roleARN, creds)
		return creds, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*AssumedCredentials), nil
	}
}

// cachedCreds returns a copy of the cached credentials for roleARN if they are
// still valid past the safety margin.
func (b *STSBroker) cachedCreds(roleARN string) (*AssumedCredentials, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	creds, ok := b.cache[roleARN]
	if !ok || !b.now().Before(creds.Expiration.Add(-stsCacheSafetyMargin)) {
		return nil, false
	}
	out := creds
	return &out, true
}

func (b *STSBroker) storeCreds(roleARN string, creds *AssumedCredentials) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cache[roleARN] = *creds
}

// awsAssumeRole mints fresh credentials for the given IAM role ARN via STS.
func awsAssumeRole(ctx context.Context, client *sts.Client, roleARN string) (*AssumedCredentials, error) {
	durationSeconds := int32(stsSessionDuration.Seconds())
	sessionName := stsSessionName
	out, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
		DurationSeconds: &durationSeconds,
	})
	if err != nil {
		return nil, fmt.Errorf("STS AssumeRole %s: %w", roleARN, err)
	}
	if out.Credentials == nil {
		return nil, fmt.Errorf("STS AssumeRole returned nil credentials for %s", roleARN)
	}
	return &AssumedCredentials{
		AccessKeyID:     aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(out.Credentials.SessionToken),
		Expiration:      aws.ToTime(out.Credentials.Expiration),
	}, nil
}
