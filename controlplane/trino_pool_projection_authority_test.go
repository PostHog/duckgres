//go:build kubernetes

package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
)

// recordedProjection is the durable record, with the accepted projection under
// the test's control and every read counted.
type recordedProjection struct {
	digest atomic.Pointer[string]
	reads  atomic.Int64
}

func (r *recordedProjection) GetTrinoPoolProjection(context.Context, string) (configstore.TrinoPoolProjection, error) {
	r.reads.Add(1)
	accepted := ""
	if held := r.digest.Load(); held != nil {
		accepted = *held
	}
	return configstore.TrinoPoolProjection{AcceptedDigest: accepted, AcceptedRevision: 1}, nil
}

func (r *recordedProjection) accept(digest string) { r.digest.Store(&digest) }

// The serving gate reads the durable record on EVERY request.
//
// It used to cache the answer for two seconds, on the argument that the window
// was no worse than propagation delay. It is worse: OPA's periodic downloader
// activates each bundle inline as it is fetched, so a poll that lands inside
// that window does not merely see a stale answer, it INSTALLS the superseded
// authorization data - after a newer projection has already been accepted and a
// warehouse admitted against it. Priming the reader must not make the next
// request answer from what the previous one saw.
func TestTheServingGateDoesNotServeAProjectionItHasAlreadySeenReplaced(t *testing.T) {
	record := &recordedProjection{}
	record.accept("projection-1")
	gate := &trinoPoolAcceptedProjection{store: record, poolID: "pool-1"}

	// Prime the reader: a coordinator polls this replica while it is current.
	if digest, known := gate.digest(); !known || digest != "projection-1" {
		t.Fatalf("digest = %q known=%v, want the accepted projection", digest, known)
	}
	primed := record.reads.Load()

	// The pool accepts a new projection elsewhere, and a tenant is admitted
	// against it.
	record.accept("projection-2")

	if digest, known := gate.digest(); !known || digest != "projection-2" {
		t.Fatalf("digest = %q known=%v, want the replaced projection to be refused immediately", digest, known)
	}
	if record.reads.Load() <= primed {
		t.Fatal("the gate answered from a cached read instead of the durable record")
	}
}

const (
	currentImage = "registry.example.invalid/duckgres@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	olderImage   = "registry.example.invalid/duckgres@sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

func producerFor(image string) *trinoPoolProducerIdentity {
	return &trinoPoolProducerIdentity{ownImage: image}
}

// The counterexample the ordering alone does not answer: an OLD control plane
// wins the lease. It would publish its own older policy rules - which live in
// its binary, not in any config - and stamp them with a HIGHER revision, so no
// counter can tell that the authorization data went backwards. Only the process
// the deployment currently wants may advance the projection.
func TestAnOlderBinaryMayNotAdvanceTheProjection(t *testing.T) {
	older := producerFor(olderImage)
	eligible, own, err := older.eligible(currentImage)
	if err != nil {
		t.Fatalf("eligibility: %v", err)
	}
	if eligible {
		t.Fatal("a control plane running an older image was allowed to publish authorization data")
	}
	if own != olderImage {
		t.Fatalf("own image = %q, want the image this process was started with", own)
	}

	current := producerFor(currentImage)
	eligible, _, err = current.eligible(currentImage)
	if err != nil || !eligible {
		t.Fatalf("the desired publisher was refused: eligible=%v err=%v", eligible, err)
	}
}

// The identity this process publishes under is captured from the environment at
// STARTUP and never re-derived. A pod specification can be edited under a
// running process, so what the API says the pod should run is not evidence
// about the code that is actually executing this check.
func TestProducerIdentityComesFromTheStartupEnvironment(t *testing.T) {
	t.Setenv(envTrinoPoolPublisherImage, currentImage)
	producer := newTrinoPoolProducerIdentity()
	if producer.ownImage != currentImage {
		t.Fatalf("own image = %q, want the startup value", producer.ownImage)
	}
	eligible, _, err := producer.eligible(currentImage)
	if err != nil || !eligible {
		t.Fatalf("the desired publisher was refused: eligible=%v err=%v", eligible, err)
	}

	// The desired image moves. The captured identity does not follow it.
	t.Setenv(envTrinoPoolPublisherImage, olderImage)
	if eligible, _, err := producer.eligible(olderImage); err != nil || eligible {
		t.Fatalf("a running process changed its identity mid-flight: eligible=%v err=%v", eligible, err)
	}
}

// An intentional rollback moves the DESIRED image. The older pods become
// eligible again and the newer ones stop - which is the wanted semantics, not a
// regression: it is equality against what the deployment wants, never an
// ordering of image identities.
func TestARollbackMovesEligibilityWithTheDesiredImage(t *testing.T) {
	older, newer := producerFor(olderImage), producerFor(currentImage)

	eligible, _, err := older.eligible(olderImage)
	if err != nil || !eligible {
		t.Fatalf("after a rollback the desired publisher was refused: eligible=%v err=%v", eligible, err)
	}
	eligible, _, err = newer.eligible(olderImage)
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
	if eligible, _, err := producerFor(currentImage).eligible(""); err == nil || eligible {
		t.Fatal("publication was permitted with no desired publisher declared")
	}
	// The process was started without its own image.
	anonymous := &trinoPoolProducerIdentity{}
	if eligible, _, err := anonymous.eligible(currentImage); err == nil || eligible {
		t.Fatal("publication was permitted by a process that cannot identify itself")
	}
	// A floating tag on either side. Two equal tags are not evidence that two
	// processes run the same bytes: a tag can be repointed at any time, which is
	// precisely the skew the fence exists to catch.
	floating := "registry.example.invalid/duckgres:latest"
	if eligible, _, err := producerFor(currentImage).eligible(floating); err == nil || eligible {
		t.Fatal("publication was permitted against a floating desired image")
	}
	if eligible, _, err := producerFor(floating).eligible(floating); err == nil || eligible {
		t.Fatal("publication was permitted by a process whose own image is a floating tag")
	}
	// A digest-shaped value that is not a full sha256 digest is not pinned
	// either.
	truncated := "registry.example.invalid/duckgres@sha256:1111"
	if eligible, _, err := producerFor(truncated).eligible(truncated); err == nil || eligible {
		t.Fatal("publication was permitted against a truncated digest")
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
