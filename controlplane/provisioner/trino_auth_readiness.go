//go:build kubernetes

package provisioner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TrinoAuthenticationReadiness waits for projected login files and the file
// providers' configured cache refresh periods before publishing readiness.
// Check takes a stable backend name so paired coordinators settle independently.
type TrinoAuthenticationReadiness interface {
	SetExpected(map[string][]byte)
	Check(context.Context, string, string, []TrinoNode) (bool, error)
}

type trinoAuthenticationObservation struct {
	fingerprint string
	since       time.Time
}

type kubernetesTrinoAuthenticationReadiness struct {
	observer     *kubernetesTrinoSecretReadiness
	mu           sync.Mutex
	expected     map[string][]byte
	observations map[string]trinoAuthenticationObservation
	now          func() time.Time
}

// NewKubernetesTrinoAuthenticationReadiness observes the chart-managed file providers.
func NewKubernetesTrinoAuthenticationReadiness(kube kubernetes.Interface, executor TrinoPodExecutor) TrinoAuthenticationReadiness {
	if executor == nil {
		executor = NewTrinoPodExecutor(kube, nil)
	}
	return &kubernetesTrinoAuthenticationReadiness{observer: &kubernetesTrinoSecretReadiness{kube: kube, executor: executor, secretName: TrinoAuthSecretName}, now: time.Now}
}

// Call after every successful projection, including when all tenants are disabled.
// A disable/re-enable cycle must invalidate the old observation even if no Check
// occurred while disabled and re-enabling restores the same file bytes.
func (c *kubernetesTrinoAuthenticationReadiness) SetExpected(data map[string][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := c.expected == nil
	for _, key := range []string{"password.db", "group.db"} {
		if !bytes.Equal(c.expected[key], data[key]) {
			changed = true
		}
	}
	if !changed {
		return
	}
	c.expected = map[string][]byte{"password.db": bytes.Clone(data["password.db"]), "group.db": bytes.Clone(data["group.db"])}
	c.observations = nil
}

func (c *kubernetesTrinoAuthenticationReadiness) Check(ctx context.Context, namespace, backend string, nodes []TrinoNode) (ready bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Any failed or incomplete observation invalidates the previous refresh window.
	key := namespace + "/" + backend
	complete := false
	defer func() {
		if !complete {
			delete(c.observations, key)
		}
	}()
	var coordinators []TrinoNode
	for _, node := range nodes {
		if node.Coordinator && strings.EqualFold(node.State, "active") {
			coordinators = append(coordinators, node)
		}
	}
	if len(coordinators) != 1 || c.expected == nil {
		return false, nil
	}
	endpoint, err := url.Parse(coordinators[0].URI)
	if err != nil {
		return false, errors.New("invalid Trino coordinator URI")
	}
	pods, err := c.observer.kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, errors.New("could not list Trino authentication pods")
	}
	var matches []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Status.PodIP == endpoint.Hostname() {
			matches = append(matches, pod)
		}
	}
	if len(matches) != 1 {
		return false, nil
	}
	pod := matches[0]
	mountPath, err := trinoAuthenticationMount(pod)
	if err != nil {
		return false, err
	}
	pending, err := c.observer.Check(ctx, namespace, mountPath, c.expected, coordinators)
	if err != nil || len(pending) > 0 {
		return false, err
	}
	container, err := trinoCredentialContainerForSecret(pod, mountPath, endpoint.Port(), []string{"password.db", "group.db"}, TrinoAuthSecretName)
	if err != nil {
		return false, err
	}
	member := trinoObservedPod{pod: pod, container: container, port: endpoint.Port()}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container.Name {
			member.status = status
		}
	}
	var fingerprint strings.Builder
	fmt.Fprintf(&fingerprint, "%s/%s/%s/%d", namespace, pod.UID, member.status.ContainerID, member.status.RestartCount)
	var refresh time.Duration
	for _, provider := range []struct{ config, name, file, key string }{
		{"password-authenticator.properties", "password-authenticator.name", "file.password-file", "password.db"},
		{"group-provider.properties", "group-provider.name", "file.group-file", "group.db"},
	} {
		execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		content, execErr := c.observer.executor.Exec(execCtx, namespace, pod.Name, container.Name, []string{"cat", path.Join("/etc/trino", provider.config)})
		cancel()
		if execErr != nil {
			return false, errors.New("could not observe Trino authentication configuration")
		}
		properties, parseErr := trinoAuthenticationProperties(content)
		if parseErr != nil {
			return false, parseErr
		}
		if properties[provider.name] != "file" || properties[provider.file] != path.Join(mountPath, provider.key) {
			return false, errors.New("unsupported Trino authentication file configuration")
		}
		period := 5 * time.Second
		if value, ok := properties["file.refresh-period"]; ok {
			period, err = time.ParseDuration(strings.ReplaceAll(value, " ", ""))
			if err != nil || period <= 0 {
				return false, errors.New("invalid Trino authentication refresh period")
			}
		}
		refresh = max(refresh, period)
		fmt.Fprintf(&fingerprint, "/%x", sha256.Sum256(content))
	}
	after, err := c.observer.kube.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || !trinoSameObservedMemberForSecret(member, *after, mountPath, []string{"password.db", "group.db"}, TrinoAuthSecretName) {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	complete = true
	observation, ok := c.observations[key]
	if !ok || observation.fingerprint != fingerprint.String() {
		if c.observations == nil {
			c.observations = make(map[string]trinoAuthenticationObservation)
		}
		c.observations[key] = trinoAuthenticationObservation{fingerprint: fingerprint.String(), since: c.now()}
		return false, nil
	}
	return c.now().Sub(observation.since) >= refresh, nil
}

func trinoAuthenticationMount(pod corev1.Pod) (string, error) {
	mounts := map[string]bool{}
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret == nil || volume.Secret.SecretName != TrinoAuthSecretName {
			continue
		}
		for _, container := range pod.Spec.Containers {
			for _, mount := range container.VolumeMounts {
				if mount.Name == volume.Name {
					mounts[mount.MountPath] = true
				}
			}
		}
	}
	if len(mounts) != 1 {
		return "", errors.New("Trino coordinator must project one authentication Secret directory")
	}
	for mount := range mounts {
		return mount, nil
	}
	return "", errors.New("missing Trino authentication mount")
}

// Accept the simple explicit properties emitted by our charts. Fail closed for
// duplicate or escaped entries rather than guessing Java properties semantics.
func trinoAuthenticationProperties(content []byte) (map[string]string, error) {
	if len(content) > 32768 {
		return nil, errors.New("invalid Trino authentication configuration")
	}
	result := map[string]string{}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if _, duplicate := result[key]; !ok || duplicate || strings.ContainsAny(line, "\\\x00") {
			return nil, errors.New("unsupported Trino authentication properties syntax")
		}
		result[key] = value
	}
	return result, nil
}
