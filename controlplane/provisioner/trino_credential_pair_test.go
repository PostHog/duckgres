//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// credentialSecretAPI models snapshot reads and resource-version conditional writes.
// Hooks interleave another writer deterministically, without fake-client scheduling.
type credentialSecretAPI struct {
	secret                *corev1.Secret
	version, gets, writes int
	afterGet              func(*credentialSecretAPI)
	beforeWrite           func(*credentialSecretAPI)
	getErr, writeErr      error
}

func (a *credentialSecretAPI) put(secret *corev1.Secret) {
	a.version++
	a.secret = secret.DeepCopy()
	a.secret.ResourceVersion = strconv.Itoa(a.version)
}

func (a *credentialSecretAPI) reactor(action ktesting.Action) (bool, runtime.Object, error) {
	var name string
	switch action := action.(type) {
	case ktesting.GetAction:
		name = action.GetName()
	case ktesting.CreateAction:
		name = action.GetObject().(*corev1.Secret).Name
	case ktesting.UpdateAction:
		name = action.GetObject().(*corev1.Secret).Name
	}
	if name != TrinoAuthSecretName {
		return false, nil, nil
	}
	resource := schema.GroupResource{Resource: "secrets"}
	switch action.GetVerb() {
	case "get":
		a.gets++
		if a.getErr != nil {
			return true, nil, a.getErr
		}
		var snapshot *corev1.Secret
		if a.secret != nil {
			snapshot = a.secret.DeepCopy()
		}
		if a.afterGet != nil {
			a.afterGet(a)
		}
		if snapshot == nil {
			return true, nil, apierrors.NewNotFound(resource, TrinoAuthSecretName)
		}
		return true, snapshot, nil
	case "create", "update":
		a.writes++
		if a.beforeWrite != nil {
			a.beforeWrite(a)
		}
		if a.writeErr != nil {
			return true, nil, a.writeErr
		}
		var desired *corev1.Secret
		if action.GetVerb() == "create" {
			desired = action.(ktesting.CreateAction).GetObject().(*corev1.Secret)
			if a.secret != nil {
				return true, nil, apierrors.NewAlreadyExists(resource, TrinoAuthSecretName)
			}
		} else {
			desired = action.(ktesting.UpdateAction).GetObject().(*corev1.Secret)
			if a.secret == nil {
				return true, nil, apierrors.NewNotFound(resource, TrinoAuthSecretName)
			}
			if desired.ResourceVersion == "" || desired.ResourceVersion != a.secret.ResourceVersion {
				return true, nil, apierrors.NewConflict(resource, TrinoAuthSecretName, errors.New("stale resource version"))
			}
		}
		a.put(desired)
		return true, a.secret.DeepCopy(), nil
	default:
		return true, nil, errors.New("unexpected secret action")
	}
}

func credentialFixture(t *testing.T, data map[string][]byte) (*TrinoProvisioner, *credentialSecretAPI) {
	t.Helper()
	a := &credentialSecretAPI{}
	if data != nil {
		a.put(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: TrinoAuthSecretName, Namespace: "credential-test", Labels: map[string]string{"retained": "label"}, Annotations: map[string]string{"retained": "annotation"}}, Data: data})
	}
	kc := kubefake.NewClientset()
	kc.PrependReactor("*", "secrets", a.reactor)
	return &TrinoProvisioner{kubernetes: kc, namespace: "credential-test"}, a
}

func TestTrinoCredentialPairSnapshotCAS(t *testing.T) {
	for _, keys := range []struct{ name, plain, hash string }{
		{"admin", TrinoAuthSecretKeyAdminPassword, TrinoAuthSecretKeyAdminPasswordHash},
		{"observer", TrinoAuthSecretKeyObserverPassword, TrinoAuthSecretKeyObserverPasswordHash},
	} {
		t.Run(keys.name, func(t *testing.T) {
			pair := func(plain string) map[string][]byte {
				hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				return map[string][]byte{keys.plain: []byte(plain), keys.hash: hash}
			}
			ensure := func(p *TrinoProvisioner) (string, string, error) {
				return p.ensureCredentialPair(context.Background(), keys.name, keys.plain, keys.hash)
			}
			assertPair := func(t *testing.T, a *credentialSecretAPI, plain, hash string) {
				t.Helper()
				if plain == "" || bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) != nil {
					t.Fatal("returned pair is invalid")
				}
				if string(a.secret.Data[keys.plain]) != plain || string(a.secret.Data[keys.hash]) != hash {
					t.Fatal("returned pair differs from durable pair")
				}
			}
			for _, initial := range []string{"absent", "nil-data", "missing-both", "missing-plain", "missing-hash", "empty-both", "empty-plain", "empty-hash"} {
				t.Run(initial, func(t *testing.T) {
					data := pair("synthetic-incomplete-value")
					switch initial {
					case "absent":
						data = nil
					case "nil-data", "missing-both":
						data = map[string][]byte{}
					case "missing-plain":
						delete(data, keys.plain)
					case "missing-hash":
						delete(data, keys.hash)
					case "empty-both":
						data[keys.plain], data[keys.hash] = nil, nil
					case "empty-plain":
						data[keys.plain] = nil
					case "empty-hash":
						data[keys.hash] = nil
					}
					if data != nil {
						data["other-key"] = []byte("retained")
					}
					p, a := credentialFixture(t, data)
					if initial == "nil-data" {
						a.secret.Data = nil
					}
					plain, hash, err := ensure(p)
					if err != nil {
						t.Fatal(err)
					}
					assertPair(t, a, plain, hash)
					if a.secret.Labels["app"] != "trino" || a.secret.Labels["duckgres/managed"] != "true" {
						t.Fatal("managed labels missing")
					}
					if data != nil {
						if initial != "nil-data" && string(a.secret.Data["other-key"]) != "retained" {
							t.Fatal("unrelated key lost")
						}
						if a.secret.Labels["retained"] != "label" || a.secret.Annotations["retained"] != "annotation" {
							t.Fatal("metadata lost")
						}
					}
					if a.writes != 1 {
						t.Fatalf("expected one establishment write, got %d", a.writes)
					}
					before := a.secret.DeepCopy()
					plain, hash, err = ensure(p)
					if err != nil {
						t.Fatal(err)
					}
					assertPair(t, a, plain, hash)
					if a.writes != 1 || !reflect.DeepEqual(before, a.secret) {
						t.Fatal("adoption changed secret")
					}
				})
			}
			t.Run("single-snapshot-validation", func(t *testing.T) {
				initial := pair("synthetic-first-value")
				p, a := credentialFixture(t, initial)
				a.afterGet = func(a *credentialSecretAPI) {
					if a.gets == 1 {
						replacement := a.secret.DeepCopy()
						replacement.Data = pair("synthetic-next-value")
						a.put(replacement)
					}
				}
				plain, hash, err := ensure(p)
				if err != nil {
					t.Fatal(err)
				}
				if plain != string(initial[keys.plain]) || hash != string(initial[keys.hash]) || a.gets != 1 || a.writes != 0 {
					t.Fatal("pair was not validated from one snapshot")
				}
			})
			for _, absent := range []bool{true, false} {
				t.Run("adopt-concurrent-winner-absent-"+strconv.FormatBool(absent), func(t *testing.T) {
					data := map[string][]byte{"other-key": []byte("retained")}
					if absent {
						data = nil
					}
					p, a := credentialFixture(t, data)
					winner := pair("synthetic-winner-value")
					a.afterGet = func(a *credentialSecretAPI) {
						if a.gets != 1 {
							return
						}
						s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: TrinoAuthSecretName}, Data: winner}
						if a.secret != nil {
							s = a.secret.DeepCopy()
							for k, v := range winner {
								s.Data[k] = v
							}
						}
						s.Data["concurrent-key"] = []byte("retained")
						a.put(s)
					}
					plain, hash, err := ensure(p)
					if err != nil {
						t.Fatal(err)
					}
					assertPair(t, a, plain, hash)
					if plain != string(winner[keys.plain]) || a.version != 2 && !absent || absent && a.version != 1 {
						t.Fatal("concurrent winner was overwritten")
					}
				})
			}
			t.Run("unrelated-writer-conflict", func(t *testing.T) {
				p, a := credentialFixture(t, map[string][]byte{})
				a.beforeWrite = func(a *credentialSecretAPI) {
					if a.writes == 1 {
						s := a.secret.DeepCopy()
						s.Data["concurrent-key"] = []byte("retained")
						a.put(s)
					}
				}
				plain, hash, err := ensure(p)
				if err != nil {
					t.Fatal(err)
				}
				assertPair(t, a, plain, hash)
				if a.writes != 2 || string(a.secret.Data["concurrent-key"]) != "retained" {
					t.Fatal("conflict did not reload and preserve other writer")
				}
			})
			t.Run("no-unconditional-merge-after-absence-check", func(t *testing.T) {
				p, a := credentialFixture(t, map[string][]byte{})
				winner := pair("synthetic-merge-winner")
				// The old third GET checked absence before a fresh, unconditional merge.
				a.afterGet = func(a *credentialSecretAPI) {
					if a.gets == 3 {
						s := a.secret.DeepCopy()
						s.Data = winner
						a.put(s)
					}
				}
				plain, hash, err := ensure(p)
				if err != nil {
					t.Fatal(err)
				}
				assertPair(t, a, plain, hash)
				if a.gets >= 3 && plain != string(winner[keys.plain]) {
					t.Fatal("absence check overwrote a concurrent winner")
				}
				if a.gets != 1 || a.writes != 1 {
					t.Fatal("establishment did not use one snapshot and one conditional write")
				}
			})
			for _, corrupt := range []string{"mismatch", "malformed"} {
				t.Run(corrupt, func(t *testing.T) {
					data := pair("synthetic-original-value")
					if corrupt == "mismatch" {
						data[keys.plain] = []byte("synthetic-different-value")
					} else {
						data[keys.hash] = []byte("invalid-bcrypt")
					}
					p, a := credentialFixture(t, data)
					before := a.secret.DeepCopy()
					plain, hash, err := ensure(p)
					if err == nil || plain != "" || hash != "" || a.writes != 0 || !reflect.DeepEqual(before, a.secret) {
						t.Fatal("corrupt complete pair was not rejected without writes")
					}
				})
			}
			for _, phase := range []string{"get", "create", "update", "conflict-exhaustion", "already-exists-exhaustion", "canceled"} {
				t.Run(phase, func(t *testing.T) {
					data := map[string][]byte{}
					if phase == "create" || phase == "already-exists-exhaustion" {
						data = nil
					}
					p, a := credentialFixture(t, data)
					failure := errors.New("synthetic API failure")
					switch phase {
					case "get":
						a.getErr = failure
					case "create", "update":
						a.writeErr = failure
					case "conflict-exhaustion":
						a.writeErr = apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, TrinoAuthSecretName, failure)
					case "already-exists-exhaustion":
						a.writeErr = apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, TrinoAuthSecretName)
					}
					before := a.secret.DeepCopy()
					ctx := context.Background()
					if phase == "canceled" {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					plain, hash, err := p.ensureCredentialPair(ctx, keys.name, keys.plain, keys.hash)
					if err == nil || plain != "" || hash != "" || !reflect.DeepEqual(before, a.secret) {
						t.Fatal("failed operation changed or returned credentials")
					}
					if strings.HasSuffix(phase, "exhaustion") {
						if a.writes != 5 {
							t.Fatalf("expected five attempts, got %d", a.writes)
						}
					} else if phase == "canceled" || phase == "get" {
						if a.writes != 0 {
							t.Fatal("read failure performed a write")
						}
					} else if a.writes != 1 {
						t.Fatalf("ambiguous write retried %d times", a.writes)
					}
				})
			}
		})
	}
}
