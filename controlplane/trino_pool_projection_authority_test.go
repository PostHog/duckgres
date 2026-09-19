//go:build kubernetes

package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func controlPlanePod(image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "duckgres-cp-0", Namespace: "trino-pool-example"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			// A sidecar first, deliberately: selecting containers[0] would read
			// somebody else's image.
			{Name: "linkerd-proxy", Image: "registry.example.invalid/proxy@sha256:aaaa"},
			{Name: "duckgres", Image: image},
		}},
	}
}

const (
	currentImage = "registry.example.invalid/duckgres@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	olderImage   = "registry.example.invalid/duckgres@sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

func producerFor(image string) *trinoPoolProducerIdentity {
	return &trinoPoolProducerIdentity{
		client:    fake.NewClientset(controlPlanePod(image)),
		namespace: "trino-pool-example",
		podName:   "duckgres-cp-0",
	}
}

// The counterexample the ordering alone does not answer: an OLD control plane
// wins the lease. It would publish its own older policy rules - which live in
// its binary, not in any config - and stamp them with a HIGHER revision, so no
// counter can tell that the authorization data went backwards. Only the process
// the deployment currently wants may advance the projection.
func TestAnOlderBinaryMayNotAdvanceTheProjection(t *testing.T) {
	older := producerFor(olderImage)
	eligible, own, err := older.eligible(context.Background(), currentImage)
	if err != nil {
		t.Fatalf("eligibility: %v", err)
	}
	if eligible {
		t.Fatal("a control plane running an older image was allowed to publish authorization data")
	}
	if own != olderImage {
		t.Fatalf("own image = %q, want the image this pod is actually running", own)
	}

	current := producerFor(currentImage)
	eligible, _, err = current.eligible(context.Background(), currentImage)
	if err != nil || !eligible {
		t.Fatalf("the desired publisher was refused: eligible=%v err=%v", eligible, err)
	}
}

// An intentional rollback moves the DESIRED image. The older pods become
// eligible again and the newer ones stop - which is the wanted semantics, not a
// regression: it is equality against what the deployment wants, never an
// ordering of image identities.
func TestARollbackMovesEligibilityWithTheDesiredImage(t *testing.T) {
	older, newer := producerFor(olderImage), producerFor(currentImage)

	eligible, _, err := older.eligible(context.Background(), olderImage)
	if err != nil || !eligible {
		t.Fatalf("after a rollback the desired publisher was refused: eligible=%v err=%v", eligible, err)
	}
	eligible, _, err = newer.eligible(context.Background(), olderImage)
	if err != nil {
		t.Fatalf("eligibility: %v", err)
	}
	if eligible {
		t.Fatal("a control plane the deployment no longer wants was allowed to publish")
	}
}

// Every uncertainty fails CLOSED. The failure this exists to prevent is an old
// binary publishing old rules, and "I could not check" is indistinguishable
// from it.
func TestProjectionEligibilityFailsClosed(t *testing.T) {
	// A partial Argo sync: the pods are new, the ConfigMap key is not there yet.
	if eligible, _, err := producerFor(currentImage).eligible(context.Background(), ""); err == nil || eligible {
		t.Fatal("publication was permitted with no desired publisher declared")
	}
	// No pod identity.
	anonymous := &trinoPoolProducerIdentity{client: fake.NewClientset(), namespace: "trino-pool-example"}
	if eligible, _, err := anonymous.eligible(context.Background(), currentImage); err == nil || eligible {
		t.Fatal("publication was permitted by a process that cannot identify itself")
	}
	// The pod exists but carries no container of this name.
	wrongPod := &trinoPoolProducerIdentity{
		client: fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "duckgres-cp-0", Namespace: "trino-pool-example"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: currentImage}}},
		}),
		namespace: "trino-pool-example",
		podName:   "duckgres-cp-0",
	}
	if eligible, _, err := wrongPod.eligible(context.Background(), currentImage); err == nil || eligible {
		t.Fatal("publication was permitted from a pod with no duckgres container")
	}
}

// The serving gate applies to the bundle the handler CAPTURED, and to a 304 as
// much as to a 200: a 304 tells OPA to keep what it has, which preserves
// exactly the stale authorization data the fence exists to retire.
func TestBundleHandlerRefusesAProjectionThatIsNoLongerAccepted(t *testing.T) {
	built, err := opa.NewBuilder().BuildBundle(opa.GroupCatalogs{"org_42": {"org_42": true}}, nil)
	if err != nil {
		t.Fatalf("build bundle: %v", err)
	}
	bundle := opa.NewBundle(built).WithRevision("projection-1")
	store := &opa.BundleStore{}
	store.Set(bundle)

	accepted := "projection-1"
	handler := opa.NewHandler(store, func(*http.Request) bool { return true })
	handler.AcceptedRevision = func() (string, bool) { return accepted, true }

	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want the accepted projection to be served", response.StatusCode)
	}
	etag := response.Header.Get("ETag")

	// The projection moves on. This replica still holds the previous one.
	accepted = "projection-2"

	response, err = server.Client().Get(server.URL)
	if err != nil {
		t.Fatalf("get after the projection moved: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 so OPA keeps its last-good bundle", response.StatusCode)
	}

	// And a conditional request is refused too: answering 304 would tell OPA to
	// keep the stale bundle it already activated.
	request, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	request.Header.Set("If-None-Match", etag)
	response, err = server.Client().Do(request)
	if err != nil {
		t.Fatalf("conditional get: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("conditional status = %d, want 503 rather than a 304 that preserves the stale bundle", response.StatusCode)
	}

	// An unreadable record is also a refusal: serving authorization data nobody
	// can confirm is current is the thing being prevented.
	handler.AcceptedRevision = func() (string, bool) { return "", false }
	response, err = server.Client().Get(server.URL)
	if err != nil {
		t.Fatalf("get with an unreadable record: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when the accepted projection cannot be read", response.StatusCode)
	}
}

// A legacy cell installs no gate, and its bundle serving is unchanged.
func TestBundleHandlerIsUnchangedWithoutAFence(t *testing.T) {
	built, _ := opa.NewBuilder().BuildBundle(opa.GroupCatalogs{"org_42": {"org_42": true}}, nil)
	store := &opa.BundleStore{}
	store.Set(opa.NewBundle(built))

	server := httptest.NewServer(opa.NewHandler(store, func(*http.Request) bool { return true }))
	defer server.Close()

	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want an unfenced cell to serve exactly as before", response.StatusCode)
	}
}
