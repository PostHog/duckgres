//go:build kubernetes

package provisioner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// TrinoSecretReadiness observes the desired credential files on the actual
// members of a backend. Missing map entries have been observed matching.
type TrinoSecretReadiness interface {
	Check(context.Context, string, string, map[string][]byte, []TrinoNode) (map[string]string, error)
}

// TrinoPodExecutor executes a read-only observation in one container. Its output
// and errors are private: the readiness checker never returns them to callers.
type TrinoPodExecutor interface {
	Exec(context.Context, string, string, string, []string) ([]byte, error)
}

type kubernetesTrinoSecretReadiness struct {
	kube     kubernetes.Interface
	executor TrinoPodExecutor
}

// NewKubernetesTrinoSecretReadiness checks mounted files without caching their
// contents or relying on deployment labels. A nil executor uses in-cluster exec.
func NewKubernetesTrinoSecretReadiness(kube kubernetes.Interface, executor TrinoPodExecutor) TrinoSecretReadiness {
	if executor == nil {
		executor = NewTrinoPodExecutor(kube, nil)
	}
	return &kubernetesTrinoSecretReadiness{kube: kube, executor: executor}
}

type trinoSPDYExecutor struct {
	kube   kubernetes.Interface
	config *rest.Config
}

// NewTrinoPodExecutor uses the supplied Kubernetes REST configuration, or lazily
// resolves in-cluster configuration when the first observation is requested.
func NewTrinoPodExecutor(kube kubernetes.Interface, config *rest.Config) TrinoPodExecutor {
	return &trinoSPDYExecutor{kube: kube, config: config}
}

func (e *trinoSPDYExecutor) Exec(ctx context.Context, namespace, pod, container string, command []string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config := e.config
	if config == nil {
		var err error
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, errors.New("trino observation client configuration unavailable")
		}
	}
	client := e.kube.CoreV1().RESTClient()
	if client == nil {
		return nil, errors.New("trino observation REST client unavailable")
	}
	endpoint := client.Post().Namespace(namespace).Resource("pods").Name(pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec).URL()
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", endpoint)
	if err != nil {
		return nil, errors.New("trino observation transport initialization failed")
	}
	var out trinoObservationBuffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: io.Discard}); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("trino mounted credential observation failed")
	}
	return out.Bytes(), nil
}

// Cap output even if the remote process behaves unexpectedly. No stderr is kept.
type trinoObservationBuffer struct{ buffer bytes.Buffer }

func (b *trinoObservationBuffer) Bytes() []byte { return b.buffer.Bytes() }

func (b *trinoObservationBuffer) Write(p []byte) (int, error) {
	if b.buffer.Len()+len(p) > 32768 {
		return 0, errors.New("trino observation output limit exceeded")
	}
	return b.buffer.Write(p)
}

const trinoSecretBatchSize = 128

// Paths travel as positional arguments, never shell source. Only a synthetic ID
// and digest leave the container; sha256sum never receives a tenant filename to
// print, and file contents are neither arguments nor output.
const trinoSecretObservationScript = `set -f
while [ "$#" -ge 2 ]; do
 id=$1
 file=$2
 shift 2
 if [ ! -f "$file" ] || [ ! -r "$file" ]; then
  printf '%s MISSING\n' "$id"
  continue
 fi
 digest=$(sha256sum < "$file" 2>/dev/null) || exit 1
 digest=${digest%% *}
 printf '%s %s\n' "$id" "$digest"
done
`

type trinoObservedPod struct {
	pod       corev1.Pod
	container corev1.Container
	status    corev1.ContainerStatus
	port      string
}

func trinoAllPending(expected map[string][]byte, reason string) map[string]string {
	pending := make(map[string]string, len(expected))
	for org := range expected {
		pending[org] = reason
	}
	return pending
}

func (c *kubernetesTrinoSecretReadiness) Check(ctx context.Context, namespace, mountPath string, expected map[string][]byte, nodes []TrinoNode) (map[string]string, error) {
	if len(expected) == 0 {
		return map[string]string{}, nil
	}
	if namespace == "" || !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath {
		return nil, errors.New("invalid Trino credential observation location")
	}
	keys := make([]string, 0, len(expected))
	for org := range expected {
		if !secretDataKeyPattern.MatchString(org) || org == "." || org == ".." {
			return nil, errors.New("invalid Trino tenant credential key")
		}
		keys = append(keys, org)
	}
	sort.Strings(keys)
	pods, err := c.kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("could not list Trino member pods for credential observation")
	}
	observed := make([]trinoObservedPod, 0, len(nodes))
	seen := make(map[string]bool)
	for _, node := range nodes {
		if !strings.EqualFold(node.State, "active") {
			continue
		}
		endpoint, err := url.Parse(node.URI)
		if err != nil || net.ParseIP(endpoint.Hostname()) == nil {
			return nil, errors.New("trino member URI must identify a pod IP")
		}
		ip := net.ParseIP(endpoint.Hostname()).String()
		if seen[ip] {
			return nil, errors.New("ambiguous Trino member pod mapping")
		}
		seen[ip] = true
		var matching []corev1.Pod
		for _, pod := range pods.Items {
			if parsed := net.ParseIP(pod.Status.PodIP); parsed != nil && parsed.String() == ip {
				matching = append(matching, pod)
			}
		}
		if len(matching) > 1 {
			return nil, errors.New("ambiguous Trino member pod mapping")
		}
		if len(matching) == 0 {
			return trinoAllPending(expected, "waiting for a Trino member pod"), nil
		}
		pod := matching[0]
		if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
			return trinoAllPending(expected, "waiting for a running Trino member pod"), nil
		}
		container, err := trinoCredentialContainer(pod, mountPath, endpoint.Port(), keys)
		if err != nil {
			return nil, err
		}
		var status corev1.ContainerStatus
		for _, candidate := range pod.Status.ContainerStatuses {
			if candidate.Name == container.Name {
				status = candidate
			}
		}
		if !status.Ready || status.State.Running == nil || status.ContainerID == "" {
			return trinoAllPending(expected, "waiting for a ready Trino member container"), nil
		}
		observed = append(observed, trinoObservedPod{pod: pod, container: container, status: status, port: endpoint.Port()})
	}
	if len(observed) == 0 {
		return trinoAllPending(expected, "waiting for active Trino members"), nil
	}
	pending := make(map[string]string)
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, 4)
	for _, member := range observed {
		wg.Add(1)
		go func(member trinoObservedPod) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				mu.Lock()
				firstErr = ctx.Err()
				mu.Unlock()
				return
			}
			result, err := c.observeMember(ctx, namespace, mountPath, keys, expected, member)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			for org, reason := range result {
				pending[org] = reason
			}
		}(member)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	after, err := c.kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("could not verify Trino member pods after credential observation")
	}
	for _, member := range observed {
		count := 0
		for _, pod := range after.Items {
			if pod.Status.PodIP != member.pod.Status.PodIP {
				continue
			}
			count++
			if !trinoSameObservedMember(member, pod, mountPath, keys) {
				return trinoAllPending(expected, "Trino membership changed during credential observation"), nil
			}
		}
		if count != 1 {
			return trinoAllPending(expected, "Trino membership changed during credential observation"), nil
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return pending, nil
}

func trinoCredentialContainer(pod corev1.Pod, mountPath, port string, keys []string) (corev1.Container, error) {
	var candidates []corev1.Container
	for _, container := range pod.Spec.Containers {
		for _, mount := range container.VolumeMounts {
			if mount.MountPath != mountPath {
				continue
			}
			if !mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
				return corev1.Container{}, errors.New("trino credential mount must be a read-only whole Secret volume")
			}
			found := false
			for _, volume := range pod.Spec.Volumes {
				if volume.Name != mount.Name || volume.Secret == nil || volume.Secret.SecretName != TrinoTenantSecretName {
					continue
				}
				found = true
				if len(volume.Secret.Items) > 0 {
					for _, key := range keys {
						mapped := false
						for _, item := range volume.Secret.Items {
							if item.Key == key && item.Path == key {
								mapped = true
							}
						}
						if !mapped {
							return corev1.Container{}, errors.New("trino credential volume does not project tenant keys at the catalog paths")
						}
					}
				}
			}
			if !found {
				return corev1.Container{}, errors.New("trino credential mount must reference the tenant Secret")
			}
			candidates = append(candidates, container)
		}
	}
	if len(candidates) > 1 && port != "" {
		number, err := strconv.Atoi(port)
		if err == nil {
			var matching []corev1.Container
			for _, container := range candidates {
				for _, p := range container.Ports {
					if int(p.ContainerPort) == number {
						matching = append(matching, container)
						break
					}
				}
			}
			candidates = matching
		}
	}
	if len(candidates) != 1 {
		return corev1.Container{}, errors.New("trino member must have one identifiable tenant credential container")
	}
	return candidates[0], nil
}

func trinoSameObservedMember(before trinoObservedPod, after corev1.Pod, mountPath string, keys []string) bool {
	if after.Name != before.pod.Name || after.UID != before.pod.UID || after.Status.Phase != corev1.PodRunning || after.DeletionTimestamp != nil {
		return false
	}
	container, err := trinoCredentialContainer(after, mountPath, before.port, keys)
	if err != nil || container.Name != before.container.Name {
		return false
	}
	for _, status := range after.Status.ContainerStatuses {
		if status.Name == before.status.Name {
			return status.Ready && status.State.Running != nil && status.ContainerID == before.status.ContainerID && status.RestartCount == before.status.RestartCount
		}
	}
	return false
}

func (c *kubernetesTrinoSecretReadiness) observeMember(ctx context.Context, namespace, mountPath string, keys []string, expected map[string][]byte, member trinoObservedPod) (map[string]string, error) {
	pending := make(map[string]string)
	for start := 0; start < len(keys); start += trinoSecretBatchSize {
		end := min(start+trinoSecretBatchSize, len(keys))
		batch := keys[start:end]
		command := []string{"/bin/sh", "-c", trinoSecretObservationScript, "trino-secret-observation"}
		for i, key := range batch {
			command = append(command, strconv.Itoa(i), path.Join(mountPath, key))
		}
		execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		output, err := c.executor.Exec(execCtx, namespace, member.pod.Name, member.container.Name, command)
		expired := execCtx.Err()
		cancel()
		if expired != nil {
			return nil, expired
		}
		if err != nil {
			return nil, errors.New("trino mounted credential observation failed")
		}
		if len(output) > 32768 {
			return nil, errors.New("invalid Trino mounted credential observation response")
		}
		fields := strings.Fields(string(output))
		if len(fields) != 2*len(batch) {
			return nil, errors.New("invalid Trino mounted credential observation response")
		}
		for i, key := range batch {
			if fields[2*i] != strconv.Itoa(i) {
				return nil, errors.New("invalid Trino mounted credential observation response")
			}
			digest := fields[2*i+1]
			if digest == "MISSING" {
				pending[key] = "waiting for tenant credential files on Trino members"
				continue
			}
			decoded, err := hex.DecodeString(digest)
			if err != nil || len(decoded) != sha256.Size {
				return nil, errors.New("invalid Trino mounted credential observation response")
			}
			wanted := sha256.Sum256(expected[key])
			if !bytes.Equal(decoded, wanted[:]) {
				pending[key] = "waiting for current tenant credentials on Trino members"
			}
		}
	}
	return pending, nil
}
