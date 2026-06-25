//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"golang.org/x/sync/singleflight"
)

const (
	stsSessionName = "duckgres-cp"

	// defaultSTSSessionDuration is the AssumeRole DurationSeconds used unless
	// overridden by the env-only DUCKGRES_STS_SESSION_DURATION knob.
	defaultSTSSessionDuration = 1 * time.Hour

	// minSTSSessionDuration is AWS's hard AssumeRole minimum (900s). The env
	// knob is clamped up to this so a typo can't make AssumeRole reject every
	// activation.
	minSTSSessionDuration = 15 * time.Minute

	// maxSTSSessionDuration is the effective AssumeRole ceiling in our
	// deployment: the CP's own credentials come from EKS Pod Identity (a role
	// session), so the per-org AssumeRole is role chaining, which STS
	// hard-caps at 1h — and every duckling-* role's MaxSessionDuration is
	// 3600s anyway. A DurationSeconds above that is not silently truncated:
	// STS REJECTS the AssumeRole call, which would break activation for every
	// org. The env knob is clamped down to this for the same reason it is
	// clamped up to the minimum. Raising it for real requires de-chaining the
	// assume AND raising MaxSessionDuration on every duckling role (12h
	// absolute AWS max).
	maxSTSSessionDuration = 1 * time.Hour

	// stsAssumeRoleTimeout bounds the underlying AWS AssumeRole call. The call
	// is detached from the triggering caller's context (other callers may be
	// waiting on its result via singleflight), so it needs its own deadline.
	stsAssumeRoleTimeout = 1 * time.Minute
)

// stsSessionDuration is the STS AssumeRole session duration. Env-overridable
// (DUCKGRES_STS_SESSION_DURATION, e.g. "900s" / "15m" / "1h", clamped to
// [minSTSSessionDuration, maxSTSSessionDuration]) so a soak test can shorten
// tokens enough to exercise real in-statement expiry — with the production 1h
// tokens, proving "a statement outlives its STS token" needs a >25min query
// even with the refresh scheduler running. Only shortening is supported:
// values above 1h would make AssumeRole itself fail (see
// maxSTSSessionDuration).
var stsSessionDuration = resolveSTSSessionDuration(os.Getenv("DUCKGRES_STS_SESSION_DURATION"))

// credentialRefreshLookahead is how far ahead of a worker's recorded
// credential expiry the scheduler refreshes it. Half the STS session
// duration: a worker due for refresh gets picked up well before its current
// session token actually goes stale, with a full half-life of slack to retry
// transient STS / RPC failures on subsequent ticks.
var credentialRefreshLookahead = stsSessionDuration / 2

// stsCacheSafetyMargin is how long before the credentials' true expiration
// we stop serving them from cache and mint a fresh session instead.
//
// This is the freshness floor for every credential capture point: all
// AssumeRole callers bake the result into a worker's DuckDB secret, and a
// DuckDB statement captures those credentials when it starts executing — a
// later CREATE OR REPLACE SECRET (the credential-refresh scheduler's push)
// does not re-credential an already-running statement (DuckDB resolves
// secrets through the statement's MVCC snapshot). Stock httpfs has NO
// mid-statement recovery path for scan workloads: its refresh-on-403 hook
// only runs at file open AND only when the open performs a network request,
// but DuckLake and S3-glob scans all pre-populate file
// size/etag/last-modified so opens skip the HEAD entirely and the first auth
// failure surfaces on a range GET, which is not retried (verified against
// httpfs v1.5.3 / ducklake sources; pinned by
// TestInFlightScanDiesOnCredentialRotation in tests/integration). A
// statement on stock httpfs therefore lives or dies on its starting runway:
// this margin guarantees every statement at least lookahead+5m of token
// validity at start; statements longer than that can still die with
// ExpiredToken.
//
// The PostHog httpfs fork (PostHog/duckdb-httpfs, branch
// cred-refresh-read-path) FIXES this: on an auth failure in any S3 request
// path it re-resolves the latest committed secret — picking up this
// scheduler's rotation pushes — and retries (proven by
// TestInFlightScanSurvivesRotationWithPatchedHTTPFS in tests/integration).
// Once a fork release with that patch is pinned via HTTPFS_EXTENSION_TAG in
// Dockerfile.worker, this margin becomes defense-in-depth (a statement's
// first credentials still want a healthy runway so recovery stays rare)
// rather than the only thing standing between a long statement and
// ExpiredToken.
//
// Defined as lookahead + 5m so it stays strictly greater than
// credentialRefreshLookahead by construction (at the default 1h session:
// 30m + 5m = 35m, the historical floor). If the margin were <= the
// lookahead, a refresh push could stamp an expiry already inside the
// scheduler's lookahead window, leaving the worker perpetually "due" and
// re-pushing identical cached creds every tick until the margin finally
// forces a fresh mint.
var stsCacheSafetyMargin = credentialRefreshLookahead + 5*time.Minute

// resolveSTSSessionDuration parses the env override, falling back to the
// default on empty/garbage and clamping to AWS's AssumeRole minimum.
func resolveSTSSessionDuration(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultSTSSessionDuration
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		slog.Warn("Invalid DUCKGRES_STS_SESSION_DURATION; using default.",
			"value", raw, "default", defaultSTSSessionDuration, "error", err)
		return defaultSTSSessionDuration
	}
	if d < minSTSSessionDuration {
		slog.Warn("DUCKGRES_STS_SESSION_DURATION below AWS AssumeRole minimum; clamping.",
			"value", d, "minimum", minSTSSessionDuration)
		return minSTSSessionDuration
	}
	if d > maxSTSSessionDuration {
		slog.Warn("DUCKGRES_STS_SESSION_DURATION above the role-chaining AssumeRole ceiling; clamping. "+
			"A larger DurationSeconds would make STS reject every per-org AssumeRole and break activation.",
			"value", d, "maximum", maxSTSSessionDuration)
		return maxSTSSessionDuration
	}
	return d
}

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
