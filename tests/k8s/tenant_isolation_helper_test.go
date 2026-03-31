//go:build k8s_integration

package k8s_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	configStoreContainer      = "duckgres-config-store"
	duckLakeMetadataContainer = "duckgres-local-ducklake-metadata"
)

func seedTenantIsolationFixtures() error {
	for _, dbName := range []string{
		"ducklake_metadata_analytics",
		"ducklake_metadata_billing",
	} {
		if err := ensurePostgresDatabase(duckLakeMetadataContainer, "ducklake", dbName); err != nil {
			return err
		}
	}

	for _, secret := range []struct {
		name string
		data map[string]string
	}{
		{name: "analytics-warehouse-db", data: map[string]string{"dsn": "duckgres"}},
		{name: "analytics-metadata", data: map[string]string{"dsn": "ducklake"}},
		{name: "analytics-s3", data: map[string]string{"credentials": `{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`}},
		{name: "analytics-runtime", data: map[string]string{"duckgres.yaml": baseTenantRuntimeConfig()}},
		{name: "billing-warehouse-db", data: map[string]string{"dsn": "duckgres"}},
		{name: "billing-metadata", data: map[string]string{"dsn": "ducklake"}},
		{name: "billing-s3", data: map[string]string{"credentials": `{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`}},
		{name: "billing-runtime", data: map[string]string{"duckgres.yaml": baseTenantRuntimeConfig()}},
	} {
		if err := upsertTenantIsolationSecret(secret.name, secret.data); err != nil {
			return err
		}
	}

	fixturePath := filepath.Join(findProjectRoot(), "tests", "k8s", "testdata", "tenant-isolation.seed.sql")
	if err := applyConfigStoreSeedFixture(fixturePath); err != nil {
		return err
	}
	return nil
}

func baseTenantRuntimeConfig() string {
	return strings.TrimSpace(`
host: "0.0.0.0"
port: 5432
data_dir: "/data"
extensions:
  - ducklake
`) + "\n"
}

func upsertTenantIsolationSecret(name string, stringData map[string]string) error {
	secrets := clientset.CoreV1().Secrets(namespace)
	existing, err := secrets.Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get secret %s: %w", name, err)
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Type: corev1.SecretTypeOpaque,
			Data: encodeSecretData(stringData),
		}
		if _, err := secrets.Create(context.Background(), secret, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create secret %s: %w", name, err)
		}
		return nil
	}

	existing.Type = corev1.SecretTypeOpaque
	existing.Data = encodeSecretData(stringData)
	if _, err := secrets.Update(context.Background(), existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update secret %s: %w", name, err)
	}
	return nil
}

func encodeSecretData(values map[string]string) map[string][]byte {
	data := make(map[string][]byte, len(values))
	for key, value := range values {
		data[key] = []byte(value)
	}
	return data
}

func ensurePostgresDatabase(container, username, databaseName string) error {
	check := exec.Command(
		"docker", "exec", container,
		"psql", "-U", username, "-d", "postgres", "-tAc",
		fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s'", databaseName),
	)
	out, err := check.Output()
	if err != nil {
		return fmt.Errorf("check database %s in %s: %w", databaseName, container, err)
	}
	if strings.TrimSpace(string(out)) == "1" {
		return nil
	}

	create := exec.Command(
		"docker", "exec", container,
		"psql", "-v", "ON_ERROR_STOP=1", "-U", username, "-d", "postgres", "-c",
		fmt.Sprintf("CREATE DATABASE %s", databaseName),
	)
	if out, err := create.CombinedOutput(); err != nil {
		return fmt.Errorf("create database %s in %s: %w: %s", databaseName, container, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func applyConfigStoreSeedFixture(path string) error {
	fixture, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read seed fixture %s: %w", path, err)
	}

	cmd := exec.Command(
		"docker", "exec", "-i", configStoreContainer,
		"psql", "-v", "ON_ERROR_STOP=1", "-U", "duckgres", "-d", "duckgres_config",
	)
	cmd.Stdin = bytes.NewReader(fixture)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("apply seed fixture %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func waitForWorkerReplacement(oldPodName string, timeout time.Duration) (corev1.Pod, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: "app=duckgres-worker",
		})
		if err != nil {
			return corev1.Pod{}, fmt.Errorf("list worker pods: %w", err)
		}

		oldStillPresent := false
		var replacement *corev1.Pod
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.Name == oldPodName {
				oldStillPresent = true
				continue
			}
			if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
				continue
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					replacement = pod
					break
				}
			}
			if replacement != nil {
				break
			}
		}

		if !oldStillPresent && replacement != nil {
			return *replacement, nil
		}

		time.Sleep(2 * time.Second)
	}

	return corev1.Pod{}, fmt.Errorf("worker pod %s was not replaced within %s", oldPodName, timeout)
}

func findActiveOrgWorkerPodSince(orgID string, since time.Time, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		row, err := queryRuntimeStoreRow(fmt.Sprintf(
			"SELECT pod_name, state FROM cp_runtime.worker_records WHERE org_id = '%s' AND updated_at >= TIMESTAMPTZ '%s' AND state IN ('reserved', 'activating', 'hot', 'draining') ORDER BY updated_at DESC LIMIT 1",
			psqlLiteral(orgID),
			since.UTC().Format(time.RFC3339Nano),
		))
		if err != nil {
			return "", err
		}
		if len(row) == 2 && row[0] != "" {
			return row[0], nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("no active worker record for org %q appeared within %s", orgID, timeout)
}

func waitForWorkerRetirement(podName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		row, err := queryRuntimeStoreRow(fmt.Sprintf(
			"SELECT state FROM cp_runtime.worker_records WHERE pod_name = '%s' ORDER BY updated_at DESC LIMIT 1",
			psqlLiteral(podName),
		))
		if err != nil {
			return err
		}
		if len(row) == 1 && row[0] == "retired" {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("worker pod %s did not reach retired state within %s", podName, timeout)
}

func queryRuntimeStoreRow(query string) ([]string, error) {
	cmd := exec.Command(
		"docker", "exec", configStoreContainer,
		"psql", "-v", "ON_ERROR_STOP=1", "-U", "duckgres", "-d", "duckgres_config",
		"-tA", "-F", "|", "-c", query,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("query runtime store: %w: %s", err, strings.TrimSpace(string(out)))
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		return nil, nil
	}
	return strings.Split(line, "|"), nil
}

func psqlLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func minioPrefixFileCount(prefix string) (int, error) {
	cmd := exec.Command(
		"docker", "exec", "duckgres-local-minio",
		"sh", "-lc",
		fmt.Sprintf("find /data/duckgres-local/%s -type f 2>/dev/null | wc -l", prefix),
	)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("count MinIO files under %s: %w", prefix, err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, fmt.Errorf("parse MinIO file count for %s: %w", prefix, err)
	}
	return count, nil
}

func waitForMinioPrefixFileCountAtLeast(prefix string, minimum int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count, err := minioPrefixFileCount(prefix)
		if err == nil && count >= minimum {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("prefix %s did not reach %d files within %s", prefix, minimum, timeout)
}
