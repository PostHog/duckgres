//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"strings"
	"testing"
	"time"
)

func TestTrinoAuthenticationReadinessRefreshAndInvalidation(t *testing.T) {
	for _, delay := range []time.Duration{5 * time.Second, 60 * time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			pod := trinoReadinessPod("coordinator", "10.0.0.1")
			pod.Spec.Volumes[0].Secret.SecretName = TrinoAuthSecretName
			kube := fake.NewClientset(pod)
			values := map[string]string{"password.db": "password-v1", "group.db": "groups-v1"}
			config := fmt.Sprintf("password-authenticator.name=file\nfile.password-file=/secrets/password.db\nfile.refresh-period=%s\n", delay)
			executor := trinoReadinessExecFunc(func(_ context.Context, _, _, _ string, cmd []string) ([]byte, error) {
				if cmd[0] == "cat" {
					if cmd[1] == "/etc/trino/password-authenticator.properties" {
						return []byte(config), nil
					}
					return []byte("group-provider.name=file\nfile.group-file=/secrets/group.db\nfile.refresh-period=5s\n"), nil
				}
				return trinoReadinessOutput(cmd, values), nil
			})
			checker := NewKubernetesTrinoAuthenticationReadiness(kube, executor).(*kubernetesTrinoAuthenticationReadiness)
			now := time.Unix(100, 0)
			checker.now = func() time.Time { return now }
			desired := map[string][]byte{"password.db": []byte("password-v1"), "group.db": []byte("groups-v1")}
			checker.SetExpected(desired)
			nodes := []TrinoNode{{URI: "http://10.0.0.1:8080", Coordinator: true, State: "active"}}
			check := func(want bool) {
				t.Helper()
				got, err := checker.Check(context.Background(), "cell", "blue", nodes)
				if err != nil || got != want {
					t.Fatalf("ready=%v want=%v err=%v", got, want, err)
				}
			}
			check(false)
			now = now.Add(delay - time.Nanosecond)
			check(false)
			now = now.Add(time.Nanosecond)
			check(true)
			// Disable then re-enable before another observation must invalidate even identical content.
			checker.SetExpected(map[string][]byte{"password.db": []byte("disabled"), "group.db": []byte("disabled")})
			checker.SetExpected(desired)
			check(false)
			now = now.Add(delay)
			check(true)
			values["group.db"] = "stale"
			check(false)
			values["group.db"] = "groups-v1"
			check(false)
			now = now.Add(delay)
			check(true)
			pod.Status.ContainerStatuses[0].ContainerID = "container://replacement"
			if _, err := kube.CoreV1().Pods("cell").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			check(false)
			now = now.Add(delay)
			check(true)
			config = "password-authenticator.name=unsupported\n"
			if ready, err := checker.Check(context.Background(), "cell", "blue", nodes); ready || err == nil {
				t.Fatalf("unsupported configuration ready=%v err=%v", ready, err)
			}
		})
	}
}

func TestTrinoAuthenticationReadinessRejectsUnobservedCredentials(t *testing.T) {
	for _, scenario := range []string{"projection lag", "no coordinator", "worker only", "replacement during config read", "exec failure", "wrong file", "wrong provider", "invalid interval", "duplicate property", "unsafe mount"} {
		t.Run(scenario, func(t *testing.T) {
			pod := trinoReadinessPod("coordinator", "10.0.0.1")
			pod.Spec.Volumes[0].Secret.SecretName = TrinoAuthSecretName
			if scenario == "unsafe mount" {
				pod.Spec.Containers[0].VolumeMounts[0].SubPath = "password.db"
			}
			kube := fake.NewClientset(pod)
			values := map[string]string{"password.db": "password", "group.db": "groups"}
			if scenario == "projection lag" {
				values["password.db"] = "old"
			}
			config := "password-authenticator.name=file\nfile.password-file=/secrets/password.db\n"
			switch scenario {
			case "wrong file":
				config = "password-authenticator.name=file\nfile.password-file=/other/password.db\n"
			case "wrong provider":
				config = "password-authenticator.name=other\nfile.password-file=/secrets/password.db\n"
			case "invalid interval":
				config += "file.refresh-period=never\n"
			case "duplicate property":
				config += "file.password-file=/other/password.db\n"
			}
			executor := trinoReadinessExecFunc(func(_ context.Context, _, _, _ string, cmd []string) ([]byte, error) {
				if scenario == "exec failure" {
					return nil, fmt.Errorf("private diagnostic must not escape")
				}
				if cmd[0] == "cat" {
					if scenario == "replacement during config read" {
						pod.Status.ContainerStatuses[0].ContainerID += "-new"
						if _, err := kube.CoreV1().Pods("cell").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
							t.Fatal(err)
						}
					}
					if cmd[1] == "/etc/trino/password-authenticator.properties" {
						return []byte(config), nil
					}
					return []byte("group-provider.name=file\nfile.group-file=/secrets/group.db\n"), nil
				}
				return trinoReadinessOutput(cmd, values), nil
			})
			checker := NewKubernetesTrinoAuthenticationReadiness(kube, executor).(*kubernetesTrinoAuthenticationReadiness)
			now := time.Unix(100, 0)
			checker.now = func() time.Time { return now }
			checker.SetExpected(map[string][]byte{"password.db": []byte("password"), "group.db": []byte("groups")})
			nodes := []TrinoNode{{URI: "http://10.0.0.1:8080", Coordinator: true, State: "active"}}
			if scenario == "no coordinator" {
				nodes = nil
			}
			if scenario == "worker only" {
				nodes[0].Coordinator = false
			}
			for i := 0; i < 2; i++ {
				ready, err := checker.Check(context.Background(), "cell", "blue", nodes)
				if ready {
					t.Fatal("unverified credentials became ready")
				}
				if err != nil && strings.Contains(err.Error(), "private diagnostic") {
					t.Fatal("raw exec output leaked")
				}
				now = now.Add(time.Minute)
			}
		})
	}
}

func TestTrinoAuthenticationReadinessInterleavedBackends(t *testing.T) {
	blue := trinoReadinessPod("blue", "10.0.0.1")
	green := trinoReadinessPod("green", "10.0.0.2")
	blue.Spec.Volumes[0].Secret.SecretName = TrinoAuthSecretName
	green.Spec.Volumes[0].Secret.SecretName = TrinoAuthSecretName
	values := map[string]string{"password.db": "password", "group.db": "groups"}
	executor := trinoReadinessExecFunc(func(_ context.Context, _, _, _ string, cmd []string) ([]byte, error) {
		if cmd[0] == "cat" {
			if cmd[1] == "/etc/trino/password-authenticator.properties" {
				return []byte("password-authenticator.name=file\nfile.password-file=/secrets/password.db\n"), nil
			}
			return []byte("group-provider.name=file\nfile.group-file=/secrets/group.db\n"), nil
		}
		return trinoReadinessOutput(cmd, values), nil
	})
	checker := NewKubernetesTrinoAuthenticationReadiness(fake.NewClientset(blue, green), executor).(*kubernetesTrinoAuthenticationReadiness)
	now := time.Unix(100, 0)
	checker.now = func() time.Time { return now }
	checker.SetExpected(map[string][]byte{"password.db": []byte("password"), "group.db": []byte("groups")})
	for pass := 0; pass < 2; pass++ {
		for _, backend := range []struct{ name, uri string }{{"blue", "http://10.0.0.1:8080"}, {"green", "http://10.0.0.2:8080"}} {
			ready, err := checker.Check(context.Background(), "cell", backend.name, []TrinoNode{{URI: backend.uri, Coordinator: true, State: "active"}})
			if err != nil || ready != (pass == 1) {
				t.Fatalf("%s pass %d ready=%v err=%v", backend.name, pass, ready, err)
			}
		}
		now = now.Add(5 * time.Second)
	}
}
