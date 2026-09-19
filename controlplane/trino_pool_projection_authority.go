//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Who may advance a pooled cell's authorization projection.
//
// The projection is produced by EVERY control plane from its own view, and the
// bundle is served by every replica, so the fence needs two different things:
//
//   - an ORDER, so a replica can tell that what it holds has been replaced -
//     that is the accepted revision in the config store; and
//   - an eligible PRODUCER, because an order alone does not help against binary
//     skew. An old control plane that wins the lease publishes its own older
//     `policy.rego` and would stamp it with a HIGHER revision: the counter
//     orders acceptances, not rule sets.
//
// The producer check is equality against the currently DESIRED image, never an
// ordering of image identities: this process may advance the projection only
// while the image it is running is the one the deployment currently wants. Both
// values already exist - the running image comes from this pod (the control
// plane already reads its own pod for exactly this kind of thing), and the
// desired image is one key on the SAME ConfigMap the pool re-reads immediately
// before every publication, rendered from the same helper that renders the
// Deployment. During a rollout the two disagree in one direction or the other
// and nobody publishes: the pooled bundle pauses, OPA keeps its last-good
// bundle, and no older projection is ever accepted. An intentional rollback
// moves the desired value, which makes the older pods eligible again - the
// wanted semantics, not a regression.
//
// Everything here is pooled-only. A legacy cell installs no fence and its
// projection behaves exactly as before.
const (
	// trinoPoolPublisherImageKey is the ConfigMap key charts render from the
	// same image helper the Deployment uses.
	trinoPoolPublisherImageKey = "publisher-image"

	// trinoPoolOwnImageContainer is the container whose image identifies this
	// process. Selected BY NAME: a pod with an injected sidecar has no
	// meaningful "first" container.
	trinoPoolOwnImageContainer = "duckgres"

	trinoPoolImageReadBudget = 10 * time.Second
)

// trinoPoolAcceptedProjection reads the accepted projection for the serving
// gate, with a short cache.
//
// OPA polls the bundle endpoint continuously, so an uncached read would put a
// database round trip on every poll of every replica. The cache is deliberately
// tiny: it bounds how long a replaced projection can still be served to roughly
// one poll interval, which is the same order as the propagation delay the fence
// already accepts.
type trinoPoolAcceptedProjection struct {
	store  *configstore.ConfigStore
	poolID string

	mu       sync.Mutex
	accepted string
	known    bool
	readAt   time.Time
	cacheFor time.Duration
}

func (a *trinoPoolAcceptedProjection) digestTTL() time.Duration {
	if a.cacheFor > 0 {
		return a.cacheFor
	}
	return 2 * time.Second
}

// digest reports the accepted projection, and whether it could be determined
// at all. A read failure reports NOT known, so the gate refuses to serve: the
// alternative is serving authorization data nobody can confirm is current.
func (a *trinoPoolAcceptedProjection) digest() (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.known && time.Since(a.readAt) < a.digestTTL() {
		return a.accepted, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), trinoPoolImageReadBudget)
	defer cancel()
	projection, err := a.store.GetTrinoPoolProjection(ctx, a.poolID)
	if err != nil {
		a.known = false
		return "", false
	}
	a.accepted, a.known, a.readAt = projection.AcceptedDigest, true, time.Now()
	return a.accepted, true
}

// trinoPoolProducerIdentity answers whether THIS process is the eligible
// publisher for a pooled cell.
type trinoPoolProducerIdentity struct {
	client    kubernetes.Interface
	namespace string
	podName   string
	// ownImage is cached after the first successful read: a pod's image is
	// immutable for its lifetime, so re-reading it would add an API call per
	// tick and could only ever return the same answer.
	ownImage string
}

// eligible reports whether this process may advance the projection, given the
// desired image from the configuration snapshot it is publishing from.
//
// It fails CLOSED on every uncertainty - no desired value, no pod name, a pod
// that cannot be read, a container that is not there - because the failure mode
// it exists to prevent is an old binary publishing old rules, and "I could not
// check" is indistinguishable from that.
func (p *trinoPoolProducerIdentity) eligible(ctx context.Context, desiredImage string) (bool, string, error) {
	desired := strings.TrimSpace(desiredImage)
	if desired == "" {
		return false, "", fmt.Errorf("the pool configuration carries no %q, so the eligible publisher is unknown", trinoPoolPublisherImageKey)
	}
	own, err := p.image(ctx)
	if err != nil {
		return false, "", err
	}
	if own != desired {
		return false, own, nil
	}
	return true, own, nil
}

func (p *trinoPoolProducerIdentity) image(ctx context.Context) (string, error) {
	if p.ownImage != "" {
		return p.ownImage, nil
	}
	if p.client == nil || p.namespace == "" || p.podName == "" {
		return "", fmt.Errorf("this process cannot identify its own image: POD_NAME or the namespace is unset")
	}
	ctx, cancel := context.WithTimeout(ctx, trinoPoolImageReadBudget)
	defer cancel()

	pod, err := p.client.CoreV1().Pods(p.namespace).Get(ctx, p.podName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read own pod %s/%s: %w", p.namespace, p.podName, err)
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != trinoPoolOwnImageContainer {
			continue
		}
		image := strings.TrimSpace(container.Image)
		if image == "" {
			return "", fmt.Errorf("container %q of pod %s/%s has no image", trinoPoolOwnImageContainer, p.namespace, p.podName)
		}
		p.ownImage = image
		return image, nil
	}
	return "", fmt.Errorf("pod %s/%s has no container named %q", p.namespace, p.podName, trinoPoolOwnImageContainer)
}
