//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	stsSessionDuration = 1 * time.Hour
	stsSessionName     = "duckgres-cp"
)

// STSBroker brokers short-lived AWS credentials by assuming per-org IAM roles.
type STSBroker struct {
	client    *sts.Client
	accountID string
}

// AssumedCredentials holds the temporary credentials from STS AssumeRole.
type AssumedCredentials struct {
	AccessKeyID    string
	SecretAccessKey string
	SessionToken   string
	Expiration     time.Time
}

// NewSTSBroker creates an STS broker using the control plane's own credentials.
// accountID is the AWS account ID used to construct deterministic role ARNs.
func NewSTSBroker(ctx context.Context, region, accountID string) (*STSBroker, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &STSBroker{
		client:    sts.NewFromConfig(cfg),
		accountID: accountID,
	}, nil
}

// RoleARNForOrg returns the deterministic IAM role ARN for an org.
func (b *STSBroker) RoleARNForOrg(orgID string) string {
	return fmt.Sprintf("arn:aws:iam::%s:role/duckling-%s", b.accountID, orgID)
}

// AssumeRole mints short-lived credentials for the given IAM role ARN.
func (b *STSBroker) AssumeRole(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
	durationSeconds := int32(stsSessionDuration.Seconds())
	sessionName := stsSessionName
	out, err := b.client.AssumeRole(ctx, &sts.AssumeRoleInput{
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
		AccessKeyID:    aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:   aws.ToString(out.Credentials.SessionToken),
		Expiration:     aws.ToTime(out.Credentials.Expiration),
	}, nil
}
