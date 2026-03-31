//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	workerRPCCertKey  = "tls.crt"
	workerRPCKeyKey   = "tls.key"
	workerRPCMountDir = "/etc/duckgres/worker-rpc"
	workerRPCDNSName  = "duckgres-worker"
)

func ensureWorkerRPCSecretData(data map[string][]byte) (map[string][]byte, bool, error) {
	if data == nil {
		data = make(map[string][]byte)
	}

	changed := false
	if len(data["bearer-token"]) == 0 {
		token, err := generateBearerToken()
		if err != nil {
			return nil, false, err
		}
		data["bearer-token"] = []byte(token)
		changed = true
	}
	if len(data[workerRPCCertKey]) == 0 || len(data[workerRPCKeyKey]) == 0 {
		certPEM, keyPEM, err := generateWorkerRPCServerCertificate()
		if err != nil {
			return nil, false, err
		}
		data[workerRPCCertKey] = certPEM
		data[workerRPCKeyKey] = keyPEM
		changed = true
	}
	return data, changed, nil
}

func generateBearerToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate bearer token: %w", err)
	}
	return fmt.Sprintf("%x", b), nil
}

func generateWorkerRPCServerCertificate() ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("generate worker RPC private key: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("generate worker RPC serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: workerRPCDNSName,
		},
		DNSNames:              []string{workerRPCDNSName},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create worker RPC certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return certPEM, keyPEM, nil
}

func (p *K8sWorkerPool) workerRPCSecretPrefix() string {
	if strings.TrimSpace(p.secretName) == "" {
		return "duckgres-worker-token"
	}
	return p.secretName
}

func (p *K8sWorkerPool) workerRPCSecretName(podName string) string {
	return p.workerRPCSecretPrefix() + "-" + podName
}

func (p *K8sWorkerPool) ensureWorkerRPCSecret(ctx context.Context, podName string) (string, error) {
	secretName := p.workerRPCSecretName(podName)
	existing, err := p.clientset.CoreV1().Secrets(p.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err == nil {
		var changed bool
		existing.Data, changed, err = ensureWorkerRPCSecretData(existing.Data)
		if err != nil {
			return "", err
		}
		if !changed {
			return secretName, nil
		}
		if _, err := p.clientset.CoreV1().Secrets(p.namespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return "", fmt.Errorf("update secret %s with worker RPC data: %w", secretName, err)
		}
		slog.Info("Populated missing worker RPC secret data.", "name", secretName, "pod", podName)
		return secretName, nil
	}
	if !k8serrors.IsNotFound(err) {
		return "", fmt.Errorf("get secret %s: %w", secretName, err)
	}

	data, _, err := ensureWorkerRPCSecretData(nil)
	if err != nil {
		return "", err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: p.namespace,
			Labels: map[string]string{
				"app":                    "duckgres",
				"duckgres/control-plane": p.cpID,
				"duckgres/worker-pod":    podName,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}

	if _, err := p.clientset.CoreV1().Secrets(p.namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return "", fmt.Errorf("create secret %s: %w", secretName, err)
	}
	slog.Info("Created worker RPC secret.", "name", secretName, "pod", podName)
	return secretName, nil
}

func (p *K8sWorkerPool) readWorkerRPCSecurity(ctx context.Context, podName string) (string, []byte, error) {
	secretName := p.workerRPCSecretName(podName)
	secret, err := p.clientset.CoreV1().Secrets(p.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("get secret %s: %w", secretName, err)
	}
	token, ok := secret.Data["bearer-token"]
	if !ok || len(token) == 0 {
		return "", nil, fmt.Errorf("secret %s missing %q", secretName, "bearer-token")
	}
	certPEM, ok := secret.Data[workerRPCCertKey]
	if !ok || len(certPEM) == 0 {
		return "", nil, fmt.Errorf("secret %s missing %q", secretName, workerRPCCertKey)
	}
	return string(token), certPEM, nil
}

func (p *K8sWorkerPool) deleteWorkerRPCSecret(ctx context.Context, podName string) error {
	secretName := p.workerRPCSecretName(podName)
	err := p.clientset.CoreV1().Secrets(p.namespace).Delete(ctx, secretName, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete secret %s: %w", secretName, err)
	}
	return nil
}

type workerTLSBearerCreds struct {
	token string
}

func (c *workerTLSBearerCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.token,
	}, nil
}

func (c *workerTLSBearerCreds) RequireTransportSecurity() bool {
	return true
}
