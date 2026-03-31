package k8s_test

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIsTransientDBError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "EOF", err: errors.New("EOF"), want: true},
		{name: "connection reset", err: errors.New("read: connection reset by peer"), want: true},
		{name: "broken pipe", err: errors.New("write: broken pipe"), want: true},
		{name: "bad connection", err: errors.New("driver: bad connection"), want: true},
		{name: "connection refused", err: errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"), want: true},
		{name: "not transient", err: errors.New("authentication failed"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isTransientDBError(tt.err); got != tt.want {
				t.Fatalf("isTransientDBError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestFindReadyPodName(t *testing.T) {
	t.Parallel()

	deleting := metav1.Now()
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "terminating", DeletionTimestamp: &deleting},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pending"},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "ready"},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
	}

	name, ok := findReadyPodName(pods)
	if !ok {
		t.Fatal("expected to find a ready pod")
	}
	if name != "ready" {
		t.Fatalf("expected ready pod name, got %q", name)
	}
}

func TestIsPodGoneError(t *testing.T) {
	t.Parallel()

	notFound := apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "pods"}, "duckgres-worker-1")
	if !isPodGoneError(notFound) {
		t.Fatal("expected NotFound to count as pod gone")
	}
	if isPodGoneError(errors.New("dial tcp timeout")) {
		t.Fatal("unexpectedly treated generic error as pod gone")
	}
}
