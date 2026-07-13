//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeReshardStore struct {
	warehouse *configstore.ManagedWarehouse
	ops       map[int64]*configstore.ReshardOperation
	nextID    int64
	createErr error

	logs      []configstore.ReshardLogEntry
	cancelled []int64
	// adopted records AdoptClaimedOperation calls (opID -> password); the
	// claimed map records CreateReshardOperationClaimed calls (opID -> runnerCP).
	adopted map[int64]string
	claimed map[int64]string
}

func newFakeReshardStore() *fakeReshardStore {
	wh := &configstore.ManagedWarehouse{
		OrgID:        "acme",
		DucklingName: "acme",
		State:        configstore.ManagedWarehouseStateReady,
	}
	wh.MetadataStore.Kind = configstore.MetadataStoreKindCnpgShard
	return &fakeReshardStore{warehouse: wh, ops: map[int64]*configstore.ReshardOperation{}, nextID: 1}
}

func (f *fakeReshardStore) CreateReshardOperation(op *configstore.ReshardOperation) error {
	if f.createErr != nil {
		return f.createErr
	}
	op.ID = f.nextID
	f.nextID++
	op.State = configstore.ReshardStatePending
	f.ops[op.ID] = op
	return nil
}

func (f *fakeReshardStore) CreateReshardOperationClaimed(op *configstore.ReshardOperation, runnerCP string) error {
	if f.createErr != nil {
		return f.createErr
	}
	op.ID = f.nextID
	f.nextID++
	op.State = configstore.ReshardStateRunning
	op.RunnerCP = runnerCP
	op.RunnerEpoch = 1
	now := time.Now().UTC()
	op.HeartbeatAt = &now
	op.StartedAt = &now
	f.ops[op.ID] = op
	if f.claimed == nil {
		f.claimed = map[int64]string{}
	}
	f.claimed[op.ID] = runnerCP
	return nil
}

func (f *fakeReshardStore) GetReshardOperation(id int64) (*configstore.ReshardOperation, error) {
	op, ok := f.ops[id]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	cp := *op
	return &cp, nil
}

func (f *fakeReshardStore) ListReshardOperationsForOrg(orgID string, _ int) ([]configstore.ReshardOperation, error) {
	var out []configstore.ReshardOperation
	for _, op := range f.ops {
		if op.OrgID == orgID {
			out = append(out, *op)
		}
	}
	return out, nil
}

func (f *fakeReshardStore) ListReshardOperations(_ int) ([]configstore.ReshardOperation, error) {
	var out []configstore.ReshardOperation
	for _, op := range f.ops {
		out = append(out, *op)
	}
	return out, nil
}

func (f *fakeReshardStore) ListReshardLog(opID, afterID int64, _ int) ([]configstore.ReshardLogEntry, error) {
	var out []configstore.ReshardLogEntry
	for _, e := range f.logs {
		if e.OperationID == opID && e.ID > afterID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeReshardStore) RequestReshardCancel(id int64) (bool, error) {
	op, ok := f.ops[id]
	if !ok || op.State.Terminal() {
		return false, nil
	}
	op.CancelRequested = true
	f.cancelled = append(f.cancelled, id)
	return true, nil
}

func (f *fakeReshardStore) FinishPendingReshardOperation(id int64, state configstore.ReshardState, errMsg string) (bool, error) {
	op, ok := f.ops[id]
	if !ok || op.State != configstore.ReshardStatePending {
		return false, nil
	}
	op.State = state
	op.Error = errMsg
	return true, nil
}

func (f *fakeReshardStore) AppendReshardLog(opID int64, level, message string) error {
	f.logs = append(f.logs, configstore.ReshardLogEntry{
		ID: int64(len(f.logs) + 1), OperationID: opID, Level: level, Message: message,
	})
	return nil
}

func (f *fakeReshardStore) GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error) {
	if f.warehouse == nil || f.warehouse.OrgID != orgID {
		return nil, gorm.ErrRecordNotFound
	}
	cp := *f.warehouse
	return &cp, nil
}

func (f *fakeReshardStore) ListExternalMetadataStores() ([]configstore.ExternalMetadataStoreInfo, error) {
	return []configstore.ExternalMetadataStoreInfo{
		{Endpoint: "known.rds.example.com", PasswordAWSSecret: "known-secret", User: "postgres", Database: "postgres"},
	}, nil
}

// The fake doubles as the local runner handle (passed as both store and runner
// to RegisterReshardAPI).
func (f *fakeReshardStore) CPID() string { return "cp-test" }

func (f *fakeReshardStore) AdoptClaimedOperation(op *configstore.ReshardOperation, password string) {
	if f.adopted == nil {
		f.adopted = map[int64]string{}
	}
	f.adopted[op.ID] = password
}

type fakeShardLister struct {
	stores map[string]DucklingMetadataStore
}

func (f fakeShardLister) CRMetadataStores(context.Context) (map[string]DucklingMetadataStore, error) {
	return f.stores, nil
}

func reshardRouter(store *fakeReshardStore) *gin.Engine {
	return reshardRouterWithCluster(store, nil)
}

func reshardRouterWithCluster(store *fakeReshardStore, cluster kubernetes.Interface) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	lister := fakeShardLister{stores: map[string]DucklingMetadataStore{
		"acme": {Kind: "cnpg-shard", Endpoint: "shard-001-pooler.cnpg-shards.svc.cluster.local"},
	}}
	RegisterReshardAPI(r.Group("/api/v1"), store, lister, store, cluster)
	return r
}

func doJSON(r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

// TestReshardStartCnpg pins the happy start path and the recorded source/
// target on the op row.
func TestReshardStartCnpg(t *testing.T) {
	store := newFakeReshardStore()
	w := doJSON(reshardRouter(store), http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"},"drain_timeout_seconds":120,"cutover_timeout_seconds":45}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d body %s, want 202", w.Code, w.Body.String())
	}
	var op configstore.ReshardOperation
	if err := json.Unmarshal(w.Body.Bytes(), &op); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if op.SourceKind != "cnpg-shard" || op.FromShard != "shard-001" || op.ToShard != "shard-002" || op.DrainTimeoutSeconds != 120 || op.CutoverTimeoutSeconds != 45 {
		t.Fatalf("op = %+v", op)
	}
	// Claim-on-create: the op is created ALREADY claimed by the local CP
	// (state running, runner_cp set) and handed to the runner to execute — no
	// pending window a sibling replica could claim.
	if op.State != configstore.ReshardStateRunning || op.RunnerCP != "cp-test" {
		t.Fatalf("op not create-claimed: state=%s runner_cp=%q", op.State, op.RunnerCP)
	}
	if store.claimed[op.ID] != "cp-test" {
		t.Fatalf("create-claimed not recorded: %v", store.claimed)
	}
	if _, ok := store.adopted[op.ID]; !ok {
		t.Fatalf("op was not adopted by the runner: %v", store.adopted)
	}
}

// TestReshardStartValidation pins every rejection: same shard, bad shard
// name, wrong warehouse state, unknown org, ext→ext, missing ext fields,
// active-op conflict.
func TestReshardStartValidation(t *testing.T) {
	cases := []struct {
		name string
		prep func(*fakeReshardStore)
		body string
		want int
	}{
		{"same shard", nil, `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-001"}}`, 400},
		{"bad shard name", nil, `{"target":{"type":"cnpg-shard","cnpg_shard":"Shard_002"}}`, 400},
		{"bad target type", nil, `{"target":{"type":"nonsense"}}`, 400},
		{"missing ext fields", nil, `{"target":{"type":"external","endpoint":"x"}}`, 400},
		{"missing ext password", nil, `{"target":{"type":"external","endpoint":"x","password_aws_secret":"s"}}`, 400},
		// RDS-managed master secrets are outside every env's ESO IAM policy
		// (posthog-*/duckling-* prefixes only) — the cutover would hang on an
		// ESO AccessDenied, so the start handler rejects them up front.
		{"rds slash master secret", nil, `{"target":{"type":"external","endpoint":"x","password_aws_secret":"rds/duckling-example/master","password":"p"}}`, 400},
		{"rds bang managed secret", nil, `{"target":{"type":"external","endpoint":"x","password_aws_secret":"rds!db-1234-abcd","password":"p"}}`, 400},
		{"not ready", func(f *fakeReshardStore) {
			f.warehouse.State = configstore.ManagedWarehouseStateProvisioning
		}, `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`, 409},
		{"active op conflict", func(f *fakeReshardStore) {
			f.createErr = configstore.ErrReshardConflict
		}, `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`, 409},
		{"ext to ext", func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = configstore.MetadataStoreKindExternal
		}, `{"target":{"type":"external","endpoint":"x","password_aws_secret":"s","password":"p"}}`, 400},
	}
	for _, tc := range cases {
		store := newFakeReshardStore()
		if tc.prep != nil {
			tc.prep(store)
		}
		w := doJSON(reshardRouter(store), http.MethodPost, "/api/v1/orgs/acme/reshard", tc.body)
		if w.Code != tc.want {
			t.Errorf("%s: status = %d body %s, want %d", tc.name, w.Code, w.Body.String(), tc.want)
		}
	}

	// Unknown org → 404.
	store := newFakeReshardStore()
	w := doJSON(reshardRouter(store), http.MethodPost, "/api/v1/orgs/ghost/reshard",
		`{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown org status = %d, want 404", w.Code)
	}
}

// TestReshardExtPasswordStashedNotPersisted pins the ephemeral-password
// contract: the password reaches the runner via the claim-on-create adopt
// handoff, the op row carries only the secret NAME, and neither the response
// nor the log contains the password. The op is also create-claimed by the
// local CP so no other replica can win it and fail the copy for want of the
// password (the production multi-replica bug).
func TestReshardExtPasswordStashedNotPersisted(t *testing.T) {
	store := newFakeReshardStore()
	w := doJSON(reshardRouter(store), http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"external","endpoint":"rds.example.com","password_aws_secret":"my-secret","user":"postgres","database":"postgres","password":"hunter2"}}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d body %s, want 202", w.Code, w.Body.String())
	}
	if store.adopted[1] != "hunter2" {
		t.Fatalf("adopted = %v, want the password handed to the runner", store.adopted)
	}
	if store.claimed[1] != "cp-test" {
		t.Fatalf("external op not create-claimed by local CP: %v", store.claimed)
	}
	op := store.ops[1]
	if op.State != configstore.ReshardStateRunning || op.RunnerCP != "cp-test" {
		t.Fatalf("external op not create-claimed: state=%s runner_cp=%q", op.State, op.RunnerCP)
	}
	if op.TargetPasswordSecret != "my-secret" || op.TargetEndpoint != "rds.example.com" {
		t.Fatalf("op = %+v", op)
	}
	if strings.Contains(w.Body.String(), "hunter2") {
		t.Fatal("response leaked the ephemeral password")
	}
	for _, e := range store.logs {
		if strings.Contains(e.Message, "hunter2") {
			t.Fatal("log leaked the ephemeral password")
		}
	}
}

// TestReshardGetListLogCancel pins the read + cancel surfaces.
func TestReshardGetListLogCancel(t *testing.T) {
	store := newFakeReshardStore()
	r := reshardRouter(store)
	if w := doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`); w.Code != 202 {
		t.Fatalf("start: %d", w.Code)
	}

	w := doJSON(r, http.MethodGet, "/api/v1/reshards/1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d", w.Code)
	}
	w = doJSON(r, http.MethodGet, "/api/v1/reshards/999", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("get missing status = %d, want 404", w.Code)
	}
	w = doJSON(r, http.MethodGet, "/api/v1/orgs/acme/reshards", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"operations"`) {
		t.Fatalf("list status = %d body %s", w.Code, w.Body.String())
	}
	// Global list (nav page): same envelope, all orgs.
	w = doJSON(r, http.MethodGet, "/api/v1/reshards", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"operations"`) {
		t.Fatalf("global list status = %d body %s", w.Code, w.Body.String())
	}

	// Incremental log poll.
	_ = store.AppendReshardLog(1, "info", "alpha")
	_ = store.AppendReshardLog(1, "info", "beta")
	w = doJSON(r, http.MethodGet, "/api/v1/reshards/1/log?after_id=0", "")
	if w.Code != http.StatusOK {
		t.Fatalf("log status = %d", w.Code)
	}
	var resp struct {
		Entries []configstore.ReshardLogEntry `json:"entries"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	afterAll := resp.Entries[len(resp.Entries)-1].ID
	w = doJSON(r, http.MethodGet, "/api/v1/reshards/1/log?after_id="+itoa(afterAll), "")
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Entries) != 0 {
		t.Fatalf("after_id poll returned %d entries, want 0", len(resp.Entries))
	}

	// Op 1 is create-claimed (running), so cancel sets the flag → 202 (the
	// runner rolls back from wherever it is).
	w = doJSON(r, http.MethodPost, "/api/v1/reshards/1/cancel", "")
	if w.Code != http.StatusAccepted || !store.ops[1].CancelRequested {
		t.Fatalf("cancel running op 1: status %d, flag %t", w.Code, store.ops[1].CancelRequested)
	}

	// A PENDING op (the no-local-runner path) finishes immediately as
	// cancelled; a re-cancel is 409 (terminal).
	store.ops[3] = &configstore.ReshardOperation{ID: 3, OrgID: "acme", State: configstore.ReshardStatePending}
	w = doJSON(r, http.MethodPost, "/api/v1/reshards/3/cancel", "")
	if w.Code != http.StatusOK {
		t.Fatalf("cancel pending status = %d body %s", w.Code, w.Body.String())
	}
	if store.ops[3].State != configstore.ReshardStateCancelled {
		t.Fatalf("pending op state = %s, want cancelled", store.ops[3].State)
	}
	w = doJSON(r, http.MethodPost, "/api/v1/reshards/3/cancel", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("re-cancel status = %d, want 409", w.Code)
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

func cnpgPod(name, shard string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: cnpgShardsNamespace,
		Labels:    map[string]string{"cnpg.io/cluster": shard},
	}}
}

type reshardTargets struct {
	Shards           []string                                `json:"shards"`
	ClusterDiscovery bool                                    `json:"cluster_discovery"`
	ExternalStores   []configstore.ExternalMetadataStoreInfo `json:"external_stores"`
}

// TestReshardTargetsClusterDiscovery pins the load-bearing property of the
// endpoint: an EMPTY shard (no tenant on it yet) shows up, discovered from the
// CNPG instance pods, unioned with the occupied shards from duckling statuses.
func TestReshardTargetsClusterDiscovery(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(
		cnpgPod("shard-001-1", "shard-001"),
		cnpgPod("shard-002-1", "shard-002"), // empty shard: no duckling on it
	)
	w := doJSON(reshardRouterWithCluster(newFakeReshardStore(), cs), http.MethodGet, "/api/v1/reshards/targets", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body %s", w.Code, w.Body.String())
	}
	var resp reshardTargets
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.ClusterDiscovery {
		t.Fatal("cluster_discovery = false, want true")
	}
	if strings.Join(resp.Shards, ",") != "shard-001,shard-002" {
		t.Fatalf("shards = %v, want [shard-001 shard-002]", resp.Shards)
	}
	if len(resp.ExternalStores) != 1 || resp.ExternalStores[0].Endpoint != "known.rds.example.com" {
		t.Fatalf("external stores = %+v", resp.ExternalStores)
	}
}

// TestReshardTargetsDegrades pins the fallbacks: an RBAC Forbidden (the e2e
// CP) degrades to the occupied shards with cluster_discovery=false, and a nil
// cluster client behaves the same.
func TestReshardTargetsDegrades(t *testing.T) {
	cs := k8sfake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", nil)
	})
	for _, cluster := range []kubernetes.Interface{cs, nil} {
		w := doJSON(reshardRouterWithCluster(newFakeReshardStore(), cluster), http.MethodGet, "/api/v1/reshards/targets", "")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d body %s", w.Code, w.Body.String())
		}
		var resp reshardTargets
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp.ClusterDiscovery {
			t.Fatal("cluster_discovery = true, want false on degrade")
		}
		if strings.Join(resp.Shards, ",") != "shard-001" {
			t.Fatalf("shards = %v, want the occupied [shard-001] fallback", resp.Shards)
		}
	}
}
