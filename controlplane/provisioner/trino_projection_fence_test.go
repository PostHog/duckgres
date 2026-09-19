//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// recordedFence is the durable record, with the accepted projection and this
// process's right to advance it both under the test's control.
type recordedFence struct {
	advanceable      bool
	accepted         string
	acceptedRevision int64
	acceptedCalls    int
}

func (f *recordedFence) Accept(_ context.Context, build func([]configstore.TrinoEnabledOrg) (string, error)) (int64, error) {
	if !f.advanceable {
		return 0, ErrTrinoProjectionNotAdvanceable
	}
	digest, err := build(nil)
	if err != nil {
		return 0, err
	}
	f.accepted, f.acceptedRevision = digest, f.acceptedRevision+1
	return f.acceptedRevision, nil
}

func (f *recordedFence) Accepted(context.Context) (string, int64, bool) {
	f.acceptedCalls++
	return f.accepted, f.acceptedRevision, f.accepted != ""
}

func fencedProvisioner(t *testing.T) *TrinoProvisioner {
	t.Helper()
	return &TrinoProvisioner{
		kubernetes:    kubefake.NewClientset(),
		namespace:     "fence-test",
		bundleStore:   &opa.BundleStore{},
		bundleBuilder: opa.NewBuilder(),
	}
}

func (p *TrinoProvisioner) authSecretRevision(t *testing.T) (string, bool) {
	t.Helper()
	secret, err := p.kubernetes.CoreV1().Secrets(p.namespace).Get(
		context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		return "", false
	}
	return secret.Annotations[TrinoProjectionRevisionAnnotation], true
}

// A replica that may not ADVANCE the projection must still build and SERVE it.
//
// Every replica but one is in that state at any moment: they all answer the
// bundle endpoint, so a replica that refused to build would strand every
// coordinator polling it on its last-good bundle, and reporting "not the
// advancer" as a reconcile failure would mark every pooled warehouse Failed
// everywhere except on the leader. What such a replica must NOT do is publish:
// the auth Secret is written only when what it built is what the record has
// accepted.
func TestANonAdvancingReplicaServesButPublishesNothingUnaccepted(t *testing.T) {
	t.Run("behind the accepted projection", func(t *testing.T) {
		provisioner := fencedProvisioner(t)
		fence := &recordedFence{accepted: "a projection this replica did not build", acceptedRevision: 7}
		provisioner.SetProjectionFence(fence)

		if err := provisioner.reconcileFencedProjection(context.Background(), nil); err != nil {
			t.Fatalf("a replica that may not advance reported a reconcile failure: %v", err)
		}
		if _, serving := provisioner.bundleStore.Current(); !serving {
			t.Fatal("the replica served no bundle, stranding the coordinators that poll it")
		}
		if _, written := provisioner.authSecretRevision(t); written {
			t.Fatal("a projection the record has not accepted was published to the auth Secret")
		}
	})

	t.Run("holding exactly the accepted projection", func(t *testing.T) {
		advancing := fencedProvisioner(t)
		accepting := &recordedFence{advanceable: true}
		advancing.SetProjectionFence(accepting)
		if err := advancing.reconcileFencedProjection(context.Background(), nil); err != nil {
			t.Fatalf("the advancing replica failed: %v", err)
		}
		if revision, written := advancing.authSecretRevision(t); !written || revision != "1" {
			t.Fatalf("auth secret revision = %q written=%v, want the revision the fence allocated", revision, written)
		}

		// A second replica builds the SAME bytes from the same sources. It may
		// not advance anything, but what it holds is what the pool accepted, so
		// it projects it under that accepted revision.
		follower := fencedProvisioner(t)
		follower.SetProjectionFence(&recordedFence{accepted: accepting.accepted, acceptedRevision: accepting.acceptedRevision})
		if err := follower.reconcileFencedProjection(context.Background(), nil); err != nil {
			t.Fatalf("the follower failed: %v", err)
		}
		if revision, written := follower.authSecretRevision(t); !written || revision != "1" {
			t.Fatalf("follower auth secret revision = %q written=%v, want the accepted revision", revision, written)
		}
	})

	t.Run("an unreadable record", func(t *testing.T) {
		provisioner := fencedProvisioner(t)
		provisioner.SetProjectionFence(&recordedFence{})

		if err := provisioner.reconcileFencedProjection(context.Background(), nil); err != nil {
			t.Fatalf("an unreadable record was reported as a reconcile failure: %v", err)
		}
		if _, serving := provisioner.bundleStore.Current(); !serving {
			t.Fatal("the replica stopped serving because the record could not be read")
		}
		if _, written := provisioner.authSecretRevision(t); written {
			t.Fatal("the auth Secret was written without knowing what the pool accepts")
		}
	})

	t.Run("a genuine failure is still a failure", func(t *testing.T) {
		provisioner := fencedProvisioner(t)
		provisioner.SetProjectionFence(&failingFence{})
		if err := provisioner.reconcileFencedProjection(context.Background(), nil); err == nil {
			t.Fatal("a fence that could not be consulted was treated as a routine refusal")
		}
	})
}

type failingFence struct{}

func (failingFence) Accept(context.Context, func([]configstore.TrinoEnabledOrg) (string, error)) (int64, error) {
	return 0, errors.New("the desired publisher could not be read")
}

func (failingFence) Accepted(context.Context) (string, int64, bool) { return "", 0, false }
