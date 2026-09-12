//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type trinoReadinessExecFunc func(context.Context, string, string, string, []string) ([]byte, error)

func (f trinoReadinessExecFunc) Exec(c context.Context, n, p, k string, a []string) ([]byte, error) {
	return f(c, n, p, k, a)
}

func trinoReadinessPod(name, ip string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "cell", UID: types.UID(name)}, Spec: corev1.PodSpec{
		Volumes:    []corev1.Volume{{Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: TrinoTenantSecretName}}}},
		Containers: []corev1.Container{{Name: "engine", VolumeMounts: []corev1.VolumeMount{{Name: "credentials", MountPath: "/secrets", ReadOnly: true}}}},
	}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, ContainerStatuses: []corev1.ContainerStatus{{Name: "engine", Ready: true, ContainerID: "container://" + name, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
}

func trinoReadinessOutput(command []string, values map[string]string) []byte {
	var b strings.Builder
	for i := 4; i < len(command); i += 2 {
		value, ok := values[filepath.Base(command[i+1])]
		if !ok {
			fmt.Fprintf(&b, "%s MISSING\n", command[i])
			continue
		}
		fmt.Fprintf(&b, "%s %x\n", command[i], sha256.Sum256([]byte(value)))
	}
	return []byte(b.String())
}

func TestTrinoSecretReadinessWorkerLag(t *testing.T) {
	kc := fake.NewClientset(trinoReadinessPod("coordinator", "10.0.0.1"), trinoReadinessPod("worker", "10.0.0.2"))
	caughtUp := false
	var calls atomic.Int32
	executor := trinoReadinessExecFunc(func(_ context.Context, ns, pod, container string, cmd []string) ([]byte, error) {
		calls.Add(1)
		if ns != "cell" || container != "engine" {
			t.Errorf("wrong exec target")
		}
		values := map[string]string{"tenant-a": "secret-a", "tenant-b": "secret-b"}
		if pod == "worker" && !caughtUp {
			values["tenant-a"] = "old"
			delete(values, "tenant-b")
		}
		for _, a := range cmd {
			if strings.Contains(a, "secret-a") || strings.Contains(a, "secret-b") {
				t.Error("secret leaked in command")
			}
		}
		return trinoReadinessOutput(cmd, values), nil
	})
	checker := NewKubernetesTrinoSecretReadiness(kc, executor)
	nodes := []TrinoNode{{ID: "coordinator", URI: "http://10.0.0.1:8080", Coordinator: true, State: "active"}, {ID: "worker", URI: "http://10.0.0.2:8080", State: "active"}}
	expected := map[string][]byte{"tenant-a": []byte("secret-a"), "tenant-b": []byte("secret-b")}
	pending, err := checker.Check(context.Background(), "cell", "/secrets", expected, nodes)
	if err != nil || len(pending) != 2 {
		t.Fatalf("worker lag: pending=%v err=%v", pending, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("want one exec per node, got %d", calls.Load())
	}
	caughtUp = true
	pending, err = checker.Check(context.Background(), "cell", "/secrets", expected, nodes)
	if err != nil || len(pending) != 0 {
		t.Fatalf("caught up: pending=%v err=%v", pending, err)
	}
}

func TestTrinoSecretReadinessMembershipAndMounts(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		mutate                      func(*corev1.Pod)
		missing, duplicate, wantErr bool
	}{
		{name: "missing", missing: true}, {name: "unready", mutate: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].Ready = false }},
		{name: "terminating", mutate: func(p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }},
		{name: "duplicate IP", duplicate: true, wantErr: true},
		{name: "wrong secret", mutate: func(p *corev1.Pod) { p.Spec.Volumes[0].Secret.SecretName = "other" }, wantErr: true},
		{name: "writable", mutate: func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].ReadOnly = false }, wantErr: true},
		{name: "subpath", mutate: func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].SubPath = "tenant" }, wantErr: true},
		{name: "remapped key", mutate: func(p *corev1.Pod) {
			p.Spec.Volumes[0].Secret.Items = []corev1.KeyToPath{{Key: "tenant", Path: "elsewhere"}}
		}, wantErr: true},
		{name: "ambiguous containers", mutate: func(p *corev1.Pod) {
			second := p.Spec.Containers[0].DeepCopy()
			second.Name = "sidecar"
			p.Spec.Containers = append(p.Spec.Containers, *second)
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewClientset()
			p := trinoReadinessPod("worker", "10.0.0.2")
			if tc.mutate != nil {
				tc.mutate(p)
			}
			if !tc.missing {
				_, _ = kc.CoreV1().Pods("cell").Create(context.Background(), p, metav1.CreateOptions{})
			}
			if tc.duplicate {
				_, _ = kc.CoreV1().Pods("cell").Create(context.Background(), trinoReadinessPod("duplicate", "10.0.0.2"), metav1.CreateOptions{})
			}
			checker := NewKubernetesTrinoSecretReadiness(kc, trinoReadinessExecFunc(func(context.Context, string, string, string, []string) ([]byte, error) {
				t.Error("unexpected exec")
				return nil, nil
			}))
			pending, err := checker.Check(context.Background(), "cell", "/secrets", map[string][]byte{"tenant": []byte("pw")}, []TrinoNode{{URI: "http://10.0.0.2:8080", State: "ACTIVE"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
			if !tc.wantErr && len(pending) != 1 {
				t.Fatalf("pending=%v", pending)
			}
		})
	}
}

func TestTrinoSecretReadinessReplacementAndErrors(t *testing.T) {
	for _, mode := range []string{"replacement", "restart", "malformed", "exec error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			kc := fake.NewClientset(trinoReadinessPod("worker", "10.0.0.2"))
			executor := trinoReadinessExecFunc(func(ctx context.Context, ns, pod, _ string, cmd []string) ([]byte, error) {
				if mode == "exec error" {
					return []byte("private stdout"), fmt.Errorf("private stderr")
				}
				if mode == "malformed" {
					return []byte("private stdout"), nil
				}
				if mode == "cancel" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				p, _ := kc.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
				if mode == "replacement" {
					p.UID = "new"
				} else {
					p.Status.ContainerStatuses[0].RestartCount++
				}
				_, _ = kc.CoreV1().Pods(ns).Update(ctx, p, metav1.UpdateOptions{})
				return trinoReadinessOutput(cmd, map[string]string{"tenant": "pw"}), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			pending, err := NewKubernetesTrinoSecretReadiness(kc, executor).Check(ctx, "cell", "/secrets", map[string][]byte{"tenant": []byte("pw")}, []TrinoNode{{URI: "http://10.0.0.2:8080", State: "active"}})
			if mode == "replacement" || mode == "restart" {
				if err != nil || len(pending) != 1 {
					t.Fatalf("pending=%v err=%v", pending, err)
				}
			} else if err == nil {
				t.Fatal("expected observation error")
			}
			if strings.Contains(fmt.Sprint(err, pending), "private") {
				t.Fatal("exec data leaked")
			}
			if mode == "cancel" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline cause lost: %v", err)
			}
		})
	}
}

func TestTrinoSecretReadinessActualShell(t *testing.T) {
	if _, err := exec.LookPath("sha256sum"); err != nil {
		t.Skip("sha256sum is required for production script test")
	}
	dir := filepath.Join(t.TempDir(), "spaces ' and ; $ literals")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	expected := make(map[string][]byte)
	for i := 0; i < 260; i++ {
		key := fmt.Sprintf("tenant-%03d", i)
		value := []byte(fmt.Sprintf("password-%d\n", i))
		expected[key] = value
		if i != 17 {
			if err := os.WriteFile(filepath.Join(dir, key), value, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	pod := trinoReadinessPod("worker", "10.0.0.2")
	pod.Spec.Containers[0].VolumeMounts[0].MountPath = dir
	var calls int
	executor := trinoReadinessExecFunc(func(ctx context.Context, _, _, _ string, cmd []string) ([]byte, error) {
		calls++
		return exec.CommandContext(ctx, cmd[0], cmd[1:]...).Output()
	})
	pending, err := NewKubernetesTrinoSecretReadiness(fake.NewClientset(pod), executor).Check(context.Background(), "cell", dir, expected, []TrinoNode{{URI: "http://10.0.0.2:8080", State: "active"}})
	if err != nil || len(pending) != 1 || pending["tenant-017"] == "" {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if calls < 2 {
		t.Fatal("expected bounded batches")
	}
}

func TestTrinoSecretReadinessConcurrencyAndDeadline(t *testing.T) {
	var pods []runtime.Object
	var nodes []TrinoNode
	for i := 1; i <= 9; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		pods = append(pods, trinoReadinessPod(fmt.Sprintf("worker-%d", i), ip))
		nodes = append(nodes, TrinoNode{URI: "http://" + ip + ":8080", State: "active"})
	}
	started := make(chan struct{}, 9)
	release := make(chan struct{})
	var running, maximum atomic.Int32
	executor := trinoReadinessExecFunc(func(ctx context.Context, _, _, _ string, cmd []string) ([]byte, error) {
		current := running.Add(1)
		defer running.Add(-1)
		for old := maximum.Load(); current > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, current) {
				break
			}
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Second {
			t.Error("exec lacks bounded deadline")
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return trinoReadinessOutput(cmd, map[string]string{"tenant": "pw"}), nil
	})
	done := make(chan error, 1)
	go func() {
		pending, err := NewKubernetesTrinoSecretReadiness(fake.NewClientset(pods...), executor).Check(context.Background(), "cell", "/secrets", map[string][]byte{"tenant": []byte("pw")}, nodes)
		if len(pending) != 0 && err == nil {
			err = fmt.Errorf("unexpected pending result")
		}
		done <- err
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("four observations did not start")
		}
	}
	select {
	case <-started:
		close(release)
		t.Fatal("more than four observations started")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 4 {
		t.Fatalf("maximum concurrency=%d", maximum.Load())
	}
}

func TestTrinoSecretReadinessOutputBound(t *testing.T) {
	var buffer trinoObservationBuffer
	if _, err := buffer.Write(make([]byte, 32768)); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Write([]byte("x")); err == nil {
		t.Fatal("unbounded output accepted")
	}
}

func TestTrinoSecretReadinessSelectsContainerByMemberPort(t *testing.T) {
	pod := trinoReadinessPod("worker", "10.0.0.2")
	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080}}
	sidecar := pod.Spec.Containers[0].DeepCopy()
	sidecar.Name = "other"
	sidecar.Ports = []corev1.ContainerPort{{ContainerPort: 9999}}
	pod.Spec.Containers = append(pod.Spec.Containers, *sidecar)
	executor := trinoReadinessExecFunc(func(_ context.Context, _, _, container string, command []string) ([]byte, error) {
		if container != "engine" {
			t.Errorf("wrong container %q", container)
		}
		return trinoReadinessOutput(command, map[string]string{"tenant": "pw"}), nil
	})
	pending, err := NewKubernetesTrinoSecretReadiness(fake.NewClientset(pod), executor).Check(context.Background(), "cell", "/secrets", map[string][]byte{"tenant": []byte("pw")}, []TrinoNode{{URI: "http://10.0.0.2:8080", State: "active"}})
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
}
