//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type SharedWorkerActivator struct {
	clientset        kubernetes.Interface
	defaultNamespace string
}

type TenantActivationPayload struct {
	OrgID          string                `json:"org_id"`
	Usernames      []string              `json:"usernames,omitempty"`
	LeaseExpiresAt time.Time             `json:"lease_expires_at,omitempty"`
	DuckLake       server.DuckLakeConfig `json:"ducklake"`
}

func NewSharedWorkerActivator(shared *K8sWorkerPool) *SharedWorkerActivator {
	if shared == nil {
		return nil
	}
	return &SharedWorkerActivator{
		clientset:        shared.clientset,
		defaultNamespace: shared.namespace,
	}
}

func (a *SharedWorkerActivator) ActivateReservedWorker(ctx context.Context, worker *ManagedWorker, org *configstore.OrgConfig) error {
	if org == nil {
		return fmt.Errorf("org config is required for activation")
	}
	state := worker.SharedState()
	if state.Assignment == nil {
		return fmt.Errorf("worker %d has no org assignment", worker.ID)
	}

	payload, err := a.BuildActivationRequest(ctx, org, state.Assignment)
	if err != nil {
		return err
	}
	return worker.ActivateTenant(ctx, server.WorkerActivationPayload{
		OrgID:          payload.OrgID,
		LeaseExpiresAt: payload.LeaseExpiresAt,
		DuckLake:       payload.DuckLake,
	})
}

func (a *SharedWorkerActivator) BuildActivationRequest(ctx context.Context, org *configstore.OrgConfig, assignment *WorkerAssignment) (TenantActivationPayload, error) {
	if org == nil || org.Warehouse == nil {
		return TenantActivationPayload{}, fmt.Errorf("org %q has no managed warehouse runtime", orgName(org))
	}
	if assignment == nil {
		return TenantActivationPayload{}, fmt.Errorf("worker assignment is required")
	}

	warehouse := org.Warehouse
	metadataPassword, err := a.readSecretValue(ctx, warehouse.MetadataStoreCredentials)
	if err != nil {
		return TenantActivationPayload{}, fmt.Errorf("metadata store credentials: %w", err)
	}

	dl := server.DuckLakeConfig{
		MetadataStore: buildDuckLakeMetadataStoreDSN(
			warehouse.MetadataStore.Endpoint,
			warehouse.MetadataStore.Port,
			warehouse.MetadataStore.Username,
			metadataPassword,
			warehouse.MetadataStore.DatabaseName,
		),
		ObjectStore: buildManagedWarehouseObjectStore(warehouse.S3),
		S3Endpoint:  warehouse.S3.Endpoint,
		S3Region:    warehouse.S3.Region,
		S3UseSSL:    warehouse.S3.UseSSL,
		S3URLStyle:  warehouse.S3.URLStyle,
	}

	switch {
	case warehouse.S3Credentials.Name != "":
		accessKey, secretKey, err := a.readS3Credentials(ctx, warehouse.S3Credentials)
		if err != nil {
			return TenantActivationPayload{}, fmt.Errorf("s3 credentials: %w", err)
		}
		dl.S3Provider = "config"
		dl.S3AccessKey = accessKey
		dl.S3SecretKey = secretKey
	case strings.EqualFold(warehouse.S3.Provider, "aws"):
		dl.S3Provider = "aws_sdk"
	}

	usernames := make([]string, 0, len(org.Users))
	for username := range org.Users {
		usernames = append(usernames, username)
	}
	slices.Sort(usernames)

	return TenantActivationPayload{
		OrgID:          assignment.OrgID,
		Usernames:      usernames,
		LeaseExpiresAt: assignment.LeaseExpiresAt,
		DuckLake:       dl,
	}, nil
}

func BuildTenantActivationPayload(ctx context.Context, clientset kubernetes.Interface, defaultNamespace string, org *configstore.OrgConfig) (TenantActivationPayload, error) {
	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: defaultNamespace,
	}
	assignment := &WorkerAssignment{
		OrgID: orgName(org),
	}
	return activator.BuildActivationRequest(ctx, org, assignment)
}

func (a *SharedWorkerActivator) readSecretValue(ctx context.Context, ref configstore.SecretRef) (string, error) {
	if ref.Name == "" || ref.Key == "" {
		return "", fmt.Errorf("secret ref requires name and key")
	}
	secret, err := a.clientset.CoreV1().Secrets(secretNamespace(ref, a.defaultNamespace)).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if value, ok := secret.StringData[ref.Key]; ok && value != "" {
		return value, nil
	}
	raw, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s missing key %q", secret.Namespace, secret.Name, ref.Key)
	}
	return string(raw), nil
}

func (a *SharedWorkerActivator) readS3Credentials(ctx context.Context, ref configstore.SecretRef) (string, string, error) {
	value, err := a.readSecretValue(ctx, ref)
	if err != nil {
		return "", "", err
	}

	var payload struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
	}
	if err := json.Unmarshal([]byte(value), &payload); err != nil {
		return "", "", fmt.Errorf("parse s3 credential payload: %w", err)
	}
	if payload.AccessKeyID == "" || payload.SecretAccessKey == "" {
		return "", "", fmt.Errorf("s3 credential payload requires access_key_id and secret_access_key")
	}
	return payload.AccessKeyID, payload.SecretAccessKey, nil
}

func buildDuckLakeMetadataStoreDSN(host string, port int, username, password, database string) string {
	return fmt.Sprintf(
		"postgres:host=%s port=%d user=%s password=%s dbname=%s",
		escapeDuckLakeConnStringValue(host),
		port,
		escapeDuckLakeConnStringValue(username),
		escapeDuckLakeConnStringValue(password),
		escapeDuckLakeConnStringValue(database),
	)
}

func buildManagedWarehouseObjectStore(s3 configstore.ManagedWarehouseS3) string {
	if s3.Bucket == "" {
		return ""
	}
	prefix := strings.Trim(s3.PathPrefix, "/")
	if prefix == "" {
		return fmt.Sprintf("s3://%s/", s3.Bucket)
	}
	return fmt.Sprintf("s3://%s/%s/", s3.Bucket, prefix)
}

func escapeDuckLakeConnStringValue(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		` `, `\ `,
		`'`, `\'`,
		`"`, `\"`,
		"\t", `\t`,
		"\n", `\n`,
		"\r", `\r`,
	)
	return replacer.Replace(value)
}

func secretNamespace(ref configstore.SecretRef, fallback string) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return fallback
}

func orgName(org *configstore.OrgConfig) string {
	if org == nil {
		return ""
	}
	return org.Name
}
