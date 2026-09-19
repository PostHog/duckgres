//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
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
// values already exist and come from the same chart helper: the running image
// is handed to the process at STARTUP in DUCKGRES_TRINO_POOL_PUBLISHER_IMAGE,
// and the desired image is one key on the SAME ConfigMap the pool re-reads
// immediately before every publication. The startup value is used rather than
// the pod's own spec because a pod specification can be edited under a running
// process, while the value the process started with cannot: what this binary
// IS does not change after it starts. During a rollout the two disagree in one
// direction or the other
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

	// envTrinoPoolPublisherImage carries THIS process's own image, rendered by
	// the chart from the same helper as the ConfigMap key above.
	envTrinoPoolPublisherImage = "DUCKGRES_TRINO_POOL_PUBLISHER_IMAGE"

	trinoPoolProjectionReadBudget = 10 * time.Second
)

// trinoPoolPinnedImage matches an image pinned by content digest. A floating
// tag names different bytes at different times, so it cannot establish that
// this binary is the desired publisher; only a digest can.
var trinoPoolPinnedImage = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)

// trinoPoolAcceptedProjection reads the accepted projection for the serving
// gate.
//
// The read is NOT cached. A cache - even a two-second one - is a window in
// which a replica keeps handing out a projection the pool has already replaced,
// and the downstream consumer does not repair it: OPA's periodic downloader
// activates each bundle inline as it fetches it, so a poll that receives the
// superseded bundle installs the superseded bundle. That is exactly the
// regression this fence exists to prevent, so the freshness of the answer
// cannot be traded for the round trip that produces it.
type trinoPoolAcceptedProjection struct {
	store  trinoPoolProjectionReader
	poolID string
}

// trinoPoolProjectionReader is the durable record's read side.
type trinoPoolProjectionReader interface {
	GetTrinoPoolProjection(ctx context.Context, poolID string) (configstore.TrinoPoolProjection, error)
}

// digest reports the accepted projection, and whether it could be determined
// at all. A read failure reports NOT known, so the gate refuses to serve: the
// alternative is serving authorization data nobody can confirm is current.
func (a *trinoPoolAcceptedProjection) digest() (string, bool) {
	digest, _, known := a.record(context.Background())
	return digest, known
}

// record reports the accepted projection and the revision it was accepted at.
// A replica that holds exactly the accepted bytes stamps its Secret with that
// revision, so the revision has to come from the same read as the digest.
func (a *trinoPoolAcceptedProjection) record(ctx context.Context) (string, int64, bool) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolProjectionReadBudget)
	defer cancel()
	projection, err := a.store.GetTrinoPoolProjection(ctx, a.poolID)
	if err != nil {
		return "", 0, false
	}
	return projection.AcceptedDigest, projection.AcceptedRevision, true
}

// trinoPoolProjectionFence is the provisioner's view of the durable record.
//
// Accept refuses - as ErrTrinoProjectionNotAdvanceable - when this process is
// not the one that may advance the projection: it does not hold the pool's
// authority, or it is not running the desired publisher image. That is the
// state every replica but one is in at any moment, so it is not a reconcile
// failure; the replica keeps building and serving, and publishes nothing the
// record has not accepted.
//
// A read that FAILS is a different thing and is returned as an error: on the
// replica holding the authority, being unable to read the desired publisher or
// this pod is a real fault, not a routine refusal.
type trinoPoolProjectionFence struct {
	store        *configstore.ConfigStore
	publicID     string
	authority    *atomic.Pointer[configstore.TrinoPoolLease]
	configSource trinoPoolConfigReader
	producer     *trinoPoolProducerIdentity
	accepted     *trinoPoolAcceptedProjection
}

func (f *trinoPoolProjectionFence) Accept(
	ctx context.Context,
	build func(orgs []configstore.TrinoEnabledOrg) (string, error),
) (int64, error) {
	lease := f.authority.Load()
	if lease == nil {
		return 0, fmt.Errorf("%w: this control plane does not hold the authority for pool %s",
			provisioner.ErrTrinoProjectionNotAdvanceable, f.publicID)
	}
	// Read the desired publisher AFTER authority is held, from the live object:
	// a delayed term that still holds a lease is refused by the database's own
	// epoch check below, and a stale desired value cannot be carried in from
	// boot.
	snapshot, err := f.configSource.Snapshot(ctx)
	if err != nil {
		return 0, fmt.Errorf("read the desired publisher for pool %s: %w", f.publicID, err)
	}
	eligible, own, err := f.producer.eligible(snapshot.PublisherImage())
	if err != nil {
		return 0, err
	}
	if !eligible {
		return 0, fmt.Errorf("%w: this control plane runs %q, which is not the desired publisher %q for pool %s",
			provisioner.ErrTrinoProjectionNotAdvanceable, own, snapshot.PublisherImage(), f.publicID)
	}
	revision, _, err := f.store.AcceptTrinoPoolProjectionWith(ctx, *lease, build)
	if err != nil {
		// A refused fenced write means the authority moved while this call was
		// in flight. Another process is publishing; this one is simply no
		// longer the advancer.
		if errors.Is(err, configstore.ErrTrinoPoolConflict) {
			return 0, fmt.Errorf("%w: the pool authority moved: %w", provisioner.ErrTrinoProjectionNotAdvanceable, err)
		}
		return 0, err
	}
	return revision, nil
}

func (f *trinoPoolProjectionFence) Accepted(ctx context.Context) (string, int64, bool) {
	return f.accepted.record(ctx)
}

// trinoPoolProducerIdentity answers whether THIS process is the eligible
// publisher for a pooled cell.
//
// ownImage is read ONCE, at startup, from the environment. It is what this
// process IS, and nothing observed later can change that: a pod's spec can be
// edited, and a runtime image ID reports the platform-specific manifest the
// node resolved rather than the multi-platform digest the chart names, so
// neither answers the question the fence asks.
type trinoPoolProducerIdentity struct {
	ownImage string
}

// newTrinoPoolProducerIdentity captures this process's own image at startup.
func newTrinoPoolProducerIdentity() *trinoPoolProducerIdentity {
	return &trinoPoolProducerIdentity{ownImage: strings.TrimSpace(os.Getenv(envTrinoPoolPublisherImage))}
}

// eligible reports whether this process may advance the projection, given the
// desired image from the configuration snapshot it is publishing from.
//
// It fails CLOSED on every uncertainty - no desired value, no startup value, or
// either side naming an image by a floating tag instead of a content digest -
// because the failure mode it exists to prevent is an old binary publishing old
// rules, and "I could not check" is indistinguishable from that. A tag can name
// different bytes at different times, so two equal tags are not evidence that
// two processes run the same code.
func (p *trinoPoolProducerIdentity) eligible(desiredImage string) (bool, string, error) {
	desired := strings.TrimSpace(desiredImage)
	if desired == "" {
		return false, "", fmt.Errorf("the pool configuration carries no %q, so the eligible publisher is unknown", trinoPoolPublisherImageKey)
	}
	if !trinoPoolPinnedImage.MatchString(desired) {
		return false, "", fmt.Errorf("the pool configuration's %q is not pinned to a content digest, so it cannot identify the eligible publisher", trinoPoolPublisherImageKey)
	}
	own := p.ownImage
	if own == "" {
		return false, "", fmt.Errorf("this process was started without %s, so it cannot identify its own image", envTrinoPoolPublisherImage)
	}
	if !trinoPoolPinnedImage.MatchString(own) {
		return false, own, fmt.Errorf("%s is not pinned to a content digest, so this process cannot prove which code it runs", envTrinoPoolPublisherImage)
	}
	if own != desired {
		return false, own, nil
	}
	return true, own, nil
}
