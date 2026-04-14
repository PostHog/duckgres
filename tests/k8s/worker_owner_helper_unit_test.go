package k8s_test

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerPodsByControlPlaneLabel(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "worker-a",
				Labels: map[string]string{"duckgres/control-plane": "cp-a"},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "worker-b",
				Labels: map[string]string{"duckgres/control-plane": "cp-b"},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "worker-c",
				Labels: map[string]string{"duckgres/control-plane": "cp-a"},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "worker-unowned",
				Labels: map[string]string{},
			},
		},
	}

	got := workerPodsByControlPlaneLabel(pods)
	want := map[string][]string{
		"cp-a": {"worker-a", "worker-c"},
		"cp-b": {"worker-b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("workerPodsByControlPlaneLabel() = %#v, want %#v", got, want)
	}
}
