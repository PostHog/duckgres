//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

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

func (f *fakeReshardStore) SetReshardOperationPasswordURL(id int64, url string) error {
	op, ok := f.ops[id]
	if !ok || op.State != configstore.ReshardStatePending {
		return errors.New("operation is not pending")
	}
	op.PasswordURL = url
	return nil
}

func (f *fakeReshardStore) SetReshardOperationRunnerImage(id int64, image string) error {
	f.ops[id].RunnerImage = image
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

// fakeSpawner implements ReshardPodSpawner: records spawned ops (a snapshot of
// the op as handed over, incl. PasswordURL), configurable failure, and a
// deterministic password URL.
type fakeSpawner struct {
	spawned  []configstore.ReshardOperation
	spawnErr error
	noURL    bool // PasswordURLForOp returns "" (pod IP undeterminable)
}

func (f *fakeSpawner) RunnerImage(context.Context) (string, error) {
	return "example/duckgres:pinned", nil
}

func (f *fakeSpawner) SpawnReshardPod(_ context.Context, op *configstore.ReshardOperation) error {
	if f.spawnErr != nil {
		return f.spawnErr
	}
	f.spawned = append(f.spawned, *op)
	return nil
}

func (f *fakeSpawner) PasswordURLForOp(opID int64) string {
	if f.noURL {
		return ""
	}
	return "http://192.0.2.1:8080/api/v1/reshards/" + strconv.FormatInt(opID, 10) + "/password"
}

type fakeShardLister struct {
	stores map[string]DucklingMetadataStore
}

func (f fakeShardLister) CRMetadataStores(context.Context) (map[string]DucklingMetadataStore, error) {
	return f.stores, nil
}

// fakeProber records the last probe and returns a canned error (nil = success).
type fakeProber struct {
	err    error
	called bool
	// last* capture the resolved args so a test can assert defaulting.
	lastEndpoint, lastUser, lastDatabase, lastPassword, lastSSLMode string
}

func (p *fakeProber) Probe(_ context.Context, endpoint, user, database, password, sslMode string) error {
	p.called = true
	p.lastEndpoint, p.lastUser, p.lastDatabase, p.lastPassword, p.lastSSLMode = endpoint, user, database, password, sslMode
	return p.err
}

// defaultShardLister matches the default fake warehouse: a cnpg-shard org
// living on shard-001 per its duckling STATUS.
func defaultShardLister() DucklingMetadataLister {
	return fakeShardLister{stores: map[string]DucklingMetadataStore{
		"acme": {Kind: "cnpg-shard", Endpoint: "shard-001-pooler.cnpg-shards.svc.cluster.local"},
	}}
}

// externalShardLister models an org whose duckling STATUS says the catalog
// lives on an external RDS.
func externalShardLister(endpoint string) DucklingMetadataLister {
	return fakeShardLister{stores: map[string]DucklingMetadataStore{
		"acme": {Kind: "external", Endpoint: endpoint},
	}}
}

func reshardRouter(store *fakeReshardStore) *gin.Engine {
	r, _ := reshardRouterFull(store, nil, nil)
	return r
}

func reshardRouterWithCluster(store *fakeReshardStore, cluster kubernetes.Interface) *gin.Engine {
	r, _ := reshardRouterFull(store, cluster, nil)
	return r
}

// reshardRouterLister registers the API with a specific duckling lister (nil
// allowed — the degraded no-duckling-client path).
func reshardRouterLister(store *fakeReshardStore, lister DucklingMetadataLister) *gin.Engine {
	r, _ := reshardRouterSpawner(store, nil, nil, &fakeSpawner{}, "internal-secret", lister)
	return r
}

// reshardRouterFull registers the API with a fakeSpawner and an
// internal-secret identity on every request (the password endpoint gates on
// it; production wiring runs AuthMiddleware in front).
func reshardRouterFull(store *fakeReshardStore, cluster kubernetes.Interface, prober ExternalTargetProber) (*gin.Engine, *fakeSpawner) {
	return reshardRouterSpawner(store, cluster, prober, &fakeSpawner{}, "internal-secret", defaultShardLister())
}

func reshardRouterSpawner(store *fakeReshardStore, cluster kubernetes.Interface, prober ExternalTargetProber, spawner ReshardPodSpawner, identitySource string, lister DucklingMetadataLister) (*gin.Engine, *fakeSpawner) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		if identitySource != "" {
			role := RoleAdmin
			if identitySource == "sso" {
				role = RoleAdmin // an SSO ADMIN must still be refused the password
			}
			c.Set(ctxIdentityKey, &Identity{Email: identitySource, Role: role, Source: identitySource})
		}
		c.Next()
	})
	RegisterReshardAPI(r.Group("/api/v1"), store, lister, spawner, cluster, prober)
	fs, _ := spawner.(*fakeSpawner)
	return r, fs
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
	r, spawner := reshardRouterFull(store, nil, nil)
	w := doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard",
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
	// Pod model: the op is created PENDING (the dedicated runner pod claims it
	// itself) and the runner pod was spawned for it. cnpg targets carry no
	// password URL.
	if op.State != configstore.ReshardStatePending {
		t.Fatalf("op state = %s, want pending (the runner pod claims it)", op.State)
	}
	if len(spawner.spawned) != 1 || spawner.spawned[0].ID != op.ID {
		t.Fatalf("runner pod not spawned for the op: %+v", spawner.spawned)
	}
	if spawner.spawned[0].PasswordURL != "" || op.PasswordURL != "" {
		t.Fatalf("cnpg target must carry no password URL (got %q)", spawner.spawned[0].PasswordURL)
	}
}

// TestReshardStartValidation pins every rejection: same shard, bad shard
// name, wrong warehouse state, unknown org, ext→ext, missing ext fields,
// active-op conflict.
func TestReshardStartValidation(t *testing.T) {
	cases := []struct {
		name   string
		prep   func(*fakeReshardStore)
		lister DucklingMetadataLister // nil = defaultShardLister()
		body   string
		want   int
	}{
		{name: "same shard", body: `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-001"}}`, want: 400},
		{name: "bad shard name", body: `{"target":{"type":"cnpg-shard","cnpg_shard":"Shard_002"}}`, want: 400},
		{name: "bad target type", body: `{"target":{"type":"nonsense"}}`, want: 400},
		{name: "missing ext fields", body: `{"target":{"type":"external","endpoint":"x"}}`, want: 400},
		{name: "missing ext password", body: `{"target":{"type":"external","endpoint":"x","password_aws_secret":"s"}}`, want: 400},
		// RDS-managed master secrets are outside every env's ESO IAM policy
		// (posthog-*/duckling-* prefixes only) — the cutover would hang on an
		// ESO AccessDenied, so the start handler rejects them up front.
		{name: "rds slash master secret", body: `{"target":{"type":"external","endpoint":"x","password_aws_secret":"rds/duckling-example/master","password":"p"}}`, want: 400},
		{name: "rds bang managed secret", body: `{"target":{"type":"external","endpoint":"x","password_aws_secret":"rds!db-1234-abcd","password":"p"}}`, want: 400},
		{name: "not ready", prep: func(f *fakeReshardStore) {
			f.warehouse.State = configstore.ManagedWarehouseStateProvisioning
		}, body: `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`, want: 409},
		{name: "active op conflict", prep: func(f *fakeReshardStore) {
			f.createErr = configstore.ErrReshardConflict
		}, body: `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`, want: 409},
		// A genuinely-external org (row + duckling status agree) may not
		// reshard to another external store.
		{name: "ext to ext", prep: func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = configstore.MetadataStoreKindExternal
			f.warehouse.MetadataStore.Endpoint = "src.rds.example.com"
			f.warehouse.MetadataStore.PasswordAWSSecret = "duckling-src-secret"
		}, lister: externalShardLister("src.rds.example.com"),
			body: `{"target":{"type":"external","endpoint":"x","password_aws_secret":"s","password":"p"}}`, want: 400},
	}
	for _, tc := range cases {
		store := newFakeReshardStore()
		if tc.prep != nil {
			tc.prep(store)
		}
		lister := tc.lister
		if lister == nil {
			lister = defaultShardLister()
		}
		w := doJSON(reshardRouterLister(store, lister), http.MethodPost, "/api/v1/orgs/acme/reshard", tc.body)
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

// TestReshardStartSourceIdentityValidation pins the submit-time source-identity
// guard: the duckling STATUS is authoritative for where the catalog actually
// lives, and the start handler refuses (400, no op created) when that identity
// is incomplete or contradicted by the config-store row. Regression for a prod
// incident: an org whose config-store metadata block was EMPTY (defaulted to
// cnpg-shard) while its duckling status pointed at an external RDS got a
// cnpg→cnpg op with an empty from_shard — the flip-before-copy re-pointed the
// org at a fresh empty catalog and the rollback patch (empty shard) was
// rejected by the XRD.
func TestReshardStartSourceIdentityValidation(t *testing.T) {
	cnpgTarget := `{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`
	emptyLister := fakeShardLister{stores: map[string]DucklingMetadataStore{}}

	cases := []struct {
		name       string
		prep       func(*fakeReshardStore)
		lister     DucklingMetadataLister
		body       string
		wantCode   int
		wantSubstr string
	}{
		// cnpg source whose current shard cannot be resolved → the op would
		// record an empty from_shard, which cannot roll back. Refused.
		{name: "cnpg source, shard unresolvable (no CR status)", lister: emptyLister,
			body: cnpgTarget, wantCode: 400, wantSubstr: "identity is incomplete"},
		{name: "cnpg source, no duckling lister", lister: nil,
			body: cnpgTarget, wantCode: 400, wantSubstr: "identity is incomplete"},
		// Row has no kind at all AND no status to fall back on.
		{name: "empty row kind and no status", prep: func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = ""
		}, lister: emptyLister, body: cnpgTarget, wantCode: 400, wantSubstr: "identity is incomplete"},
		// Type drift between row and status — both directions.
		{name: "row cnpg, status external (drift)", lister: externalShardLister("real.rds.example.com"),
			body: cnpgTarget, wantCode: 400, wantSubstr: "identity drift"},
		{name: "row external, status cnpg (drift)", prep: func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = configstore.MetadataStoreKindExternal
			f.warehouse.MetadataStore.Endpoint = "real.rds.example.com"
			f.warehouse.MetadataStore.PasswordAWSSecret = "duckling-src-secret"
		}, lister: defaultShardLister(), body: cnpgTarget, wantCode: 400, wantSubstr: "identity drift"},
		// The incident shape: EMPTY row metadata block, status says external.
		// The source is external per the status, but the row block a rollback
		// would need (endpoint + password secret) is missing. Refused.
		{name: "incident: empty row block, external status", prep: func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = ""
		}, lister: externalShardLister("real.rds.example.com"),
			body: cnpgTarget, wantCode: 400, wantSubstr: "external block is missing"},
		// Happy: legacy empty row kind on a cnpg org — status resolves it.
		{name: "empty row kind, cnpg status resolves", prep: func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = ""
		}, lister: defaultShardLister(), body: cnpgTarget, wantCode: 202},
		// Happy: external source with row + status in agreement.
		{name: "external source consistent", prep: func(f *fakeReshardStore) {
			f.warehouse.MetadataStore.Kind = configstore.MetadataStoreKindExternal
			f.warehouse.MetadataStore.Endpoint = "real.rds.example.com"
			f.warehouse.MetadataStore.PasswordAWSSecret = "duckling-src-secret"
		}, lister: externalShardLister("real.rds.example.com"), body: cnpgTarget, wantCode: 202},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeReshardStore()
			if tc.prep != nil {
				tc.prep(store)
			}
			w := doJSON(reshardRouterLister(store, tc.lister), http.MethodPost, "/api/v1/orgs/acme/reshard", tc.body)
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d body %s, want %d", w.Code, w.Body.String(), tc.wantCode)
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %s, want substring %q", w.Body.String(), tc.wantSubstr)
			}
			if tc.wantCode != http.StatusAccepted && len(store.ops) != 0 {
				t.Fatalf("op created despite rejection (%d ops)", len(store.ops))
			}
		})
	}

	// The source side of a created op is recorded from the duckling STATUS:
	// an external source keeps from_shard empty and carries the row's external
	// block; a cnpg source records the status-derived shard (pinned by
	// TestReshardStartCnpg).
	store := newFakeReshardStore()
	store.warehouse.MetadataStore.Kind = configstore.MetadataStoreKindExternal
	store.warehouse.MetadataStore.Endpoint = "real.rds.example.com"
	store.warehouse.MetadataStore.Username = "postgres"
	store.warehouse.MetadataStore.DatabaseName = "postgres"
	store.warehouse.MetadataStore.PasswordAWSSecret = "duckling-src-secret"
	w := doJSON(reshardRouterLister(store, externalShardLister("real.rds.example.com")),
		http.MethodPost, "/api/v1/orgs/acme/reshard", cnpgTarget)
	if w.Code != http.StatusAccepted {
		t.Fatalf("ext→cnpg start: status = %d body %s, want 202", w.Code, w.Body.String())
	}
	op := store.ops[1]
	if op.SourceKind != configstore.MetadataStoreKindExternal || op.FromShard != "" ||
		op.SourceEndpoint != "real.rds.example.com" || op.SourcePasswordSecret != "duckling-src-secret" {
		t.Fatalf("op source identity = %+v", op)
	}
}

// TestReshardExtSecretNameValidation pins the positive ESO-readable allowlist
// (Fix 1): posthog-/duckling- names reach op creation; an RDS-managed master
// name gets the specific rds message; any other prefix gets the general
// allowlist message. No prober here (nil) so the connection check is skipped.
func TestReshardExtSecretNameValidation(t *testing.T) {
	cases := []struct {
		name       string
		secret     string
		wantCode   int
		wantSubstr string
	}{
		{"posthog prefix accepted", "posthog-x", http.StatusAccepted, ""},
		{"duckling prefix accepted", "duckling-x", http.StatusAccepted, ""},
		{"rds slash master rejected", "rds/some-db/master", http.StatusBadRequest, "RDS-managed master secret"},
		{"rds bang managed rejected", "rds!db-0000-1111", http.StatusBadRequest, "RDS-managed master secret"},
		{"other prefix rejected", "whatever-else", http.StatusBadRequest, "is not readable by the external-secrets role"},
	}
	for _, tc := range cases {
		store := newFakeReshardStore()
		body := `{"target":{"type":"external","endpoint":"rds.example.com","password_aws_secret":"` + tc.secret + `","password":"p"}}`
		w := doJSON(reshardRouter(store), http.MethodPost, "/api/v1/orgs/acme/reshard", body)
		if w.Code != tc.wantCode {
			t.Errorf("%s: status = %d body %s, want %d", tc.name, w.Code, w.Body.String(), tc.wantCode)
			continue
		}
		if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
			t.Errorf("%s: body %s, want substring %q", tc.name, w.Body.String(), tc.wantSubstr)
		}
		if tc.wantCode == http.StatusAccepted {
			if len(store.ops) != 1 {
				t.Errorf("%s: op not created (%d ops)", tc.name, len(store.ops))
			}
		} else if len(store.ops) != 0 {
			t.Errorf("%s: op created despite rejection (%d ops)", tc.name, len(store.ops))
		}
	}
}

// TestReshardExtPreflightProbe pins Fix 2: a passing prober lets the op be
// created (202); a failing prober rejects with a redacted 400 and creates NO
// op; a nil prober skips the check (op created). It also pins that the probe
// received the defaulted user/database + sslmode=require and that neither the
// error body nor any log leaks the password.
func TestReshardExtPreflightProbe(t *testing.T) {
	body := `{"target":{"type":"external","endpoint":"rds.example.com:6543","password_aws_secret":"duckling-x","password":"hunter2"}}`

	// Passing prober → 202, op created, probe saw the resolved args.
	store := newFakeReshardStore()
	pb := &fakeProber{}
	rp, _ := reshardRouterFull(store, nil, pb)
	w := doJSON(rp, http.MethodPost, "/api/v1/orgs/acme/reshard", body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("passing prober: status = %d body %s, want 202", w.Code, w.Body.String())
	}
	if !pb.called {
		t.Fatal("passing prober: probe was not called")
	}
	if pb.lastEndpoint != "rds.example.com:6543" || pb.lastUser != "postgres" || pb.lastDatabase != "postgres" || pb.lastSSLMode != "require" {
		t.Fatalf("passing prober: probe args = %q/%q/%q sslmode=%q, want defaulted postgres + require", pb.lastEndpoint, pb.lastUser, pb.lastDatabase, pb.lastSSLMode)
	}
	if len(store.ops) != 1 {
		t.Fatalf("passing prober: op count = %d, want 1", len(store.ops))
	}

	// Failing prober → 400, redacted message, NO op created.
	store = newFakeReshardStore()
	pb = &fakeProber{err: errors.New("connection refused")}
	rp, _ = reshardRouterFull(store, nil, pb)
	w = doJSON(rp, http.MethodPost, "/api/v1/orgs/acme/reshard", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("failing prober: status = %d body %s, want 400", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "cannot connect to the external target") || !strings.Contains(w.Body.String(), "connection refused") {
		t.Fatalf("failing prober: body %s, want the redacted connect-failure message", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "hunter2") {
		t.Fatal("failing prober: error body leaked the password")
	}
	if len(store.ops) != 0 {
		t.Fatalf("failing prober: op created despite connect failure (%d ops)", len(store.ops))
	}

	// Nil prober → check skipped, op created.
	store = newFakeReshardStore()
	rp, _ = reshardRouterFull(store, nil, nil)
	w = doJSON(rp, http.MethodPost, "/api/v1/orgs/acme/reshard", body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("nil prober: status = %d body %s, want 202", w.Code, w.Body.String())
	}
	if len(store.ops) != 1 {
		t.Fatalf("nil prober: op count = %d, want 1", len(store.ops))
	}
}

// TestReshardExtPasswordStashedNotPersisted pins the ephemeral-password
// contract in the pod model: the op row carries only the secret NAME plus the
// password PULL URL (never the password); the runner pod spawned with that URL
// wired; and neither the response nor the log contains the password. The
// password itself is served exactly once-per-request by the internal-secret-
// only endpoint (covered by TestReshardPasswordEndpoint).
func TestReshardExtPasswordStashedNotPersisted(t *testing.T) {
	store := newFakeReshardStore()
	r, spawner := reshardRouterFull(store, nil, nil)
	w := doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"external","endpoint":"rds.example.com","password_aws_secret":"duckling-my-secret","user":"postgres","database":"postgres","password":"hunter2"}}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d body %s, want 202", w.Code, w.Body.String())
	}
	op := store.ops[1]
	if op.State != configstore.ReshardStatePending {
		t.Fatalf("op state = %s, want pending", op.State)
	}
	if op.TargetPasswordSecret != "duckling-my-secret" || op.TargetEndpoint != "rds.example.com" {
		t.Fatalf("op = %+v", op)
	}
	// The pull URL is recorded on the row and wired into the spawned pod.
	wantURL := "http://192.0.2.1:8080/api/v1/reshards/1/password"
	if op.PasswordURL != wantURL {
		t.Fatalf("op.PasswordURL = %q, want %q", op.PasswordURL, wantURL)
	}
	if len(spawner.spawned) != 1 || spawner.spawned[0].PasswordURL != wantURL {
		t.Fatalf("spawned op missing the password URL: %+v", spawner.spawned)
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

// TestReshardPasswordEndpoint pins the runner pod's one-shot password pull:
// internal-secret identity gets the stashed password; an SSO ADMIN is refused
// (403 — an operator must never read a tenant credential); a terminal op or a
// replica without the stash 404s; the access is logged WITHOUT the value.
func TestReshardPasswordEndpoint(t *testing.T) {
	start := `{"target":{"type":"external","endpoint":"rds.example.com","password_aws_secret":"duckling-my-secret","password":"hunter2"}}`

	// Same handler instance must serve start + pull (the stash is in-memory).
	store := newFakeReshardStore()
	r, _ := reshardRouterFull(store, nil, nil)
	if w := doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard", start); w.Code != http.StatusAccepted {
		t.Fatalf("start: %d %s", w.Code, w.Body.String())
	}
	w := doJSON(r, http.MethodGet, "/api/v1/reshards/1/password", "")
	if w.Code != http.StatusOK {
		t.Fatalf("password pull status = %d body %s, want 200", w.Code, w.Body.String())
	}
	var resp struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Password != "hunter2" {
		t.Fatalf("password pull body = %s (err %v)", w.Body.String(), err)
	}
	for _, e := range store.logs {
		if strings.Contains(e.Message, "hunter2") {
			t.Fatal("op log leaked the password on the pull")
		}
	}

	// Terminal op → 404 and the stash entry is dropped.
	store.ops[1].State = configstore.ReshardStateFailed
	if w := doJSON(r, http.MethodGet, "/api/v1/reshards/1/password", ""); w.Code != http.StatusNotFound {
		t.Fatalf("terminal op password pull = %d, want 404", w.Code)
	}
	store.ops[1].State = configstore.ReshardStateRunning
	if w := doJSON(r, http.MethodGet, "/api/v1/reshards/1/password", ""); w.Code != http.StatusNotFound {
		t.Fatalf("post-terminal (pruned stash) pull = %d, want 404", w.Code)
	}

	// An SSO admin identity is refused — internal secret ONLY.
	ssoStore := newFakeReshardStore()
	ssoR, _ := reshardRouterSpawner(ssoStore, nil, nil, &fakeSpawner{}, "sso", defaultShardLister())
	if w := doJSON(ssoR, http.MethodPost, "/api/v1/orgs/acme/reshard", start); w.Code != http.StatusAccepted {
		t.Fatalf("sso start: %d", w.Code)
	}
	w = doJSON(ssoR, http.MethodGet, "/api/v1/reshards/1/password", "")
	if w.Code != http.StatusForbidden {
		t.Fatalf("SSO password pull = %d body %s, want 403", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "hunter2") {
		t.Fatal("SSO refusal leaked the password")
	}

	// A replica that never saw the start request has no stash → 404.
	otherStore := newFakeReshardStore()
	otherStore.ops[1] = &configstore.ReshardOperation{ID: 1, OrgID: "acme", State: configstore.ReshardStateRunning}
	otherR, _ := reshardRouterFull(otherStore, nil, nil)
	if w := doJSON(otherR, http.MethodGet, "/api/v1/reshards/1/password", ""); w.Code != http.StatusNotFound {
		t.Fatalf("stashless replica pull = %d, want 404", w.Code)
	}
}

// TestReshardStartSpawnFailure pins the pod model's start-failure contract: a
// spawner error finishes the just-created pending op as FAILED (nothing would
// ever execute it) and surfaces a 502; a spawner that cannot determine its own
// pod IP fails an ext-target start the same way; a nil spawner refuses every
// start with 503 and creates nothing.
func TestReshardStartSpawnFailure(t *testing.T) {
	// Spawn error → 502, op failed.
	store := newFakeReshardStore()
	r, _ := reshardRouterSpawner(store, nil, nil, &fakeSpawner{spawnErr: errors.New("pods is forbidden")}, "internal-secret", defaultShardLister())
	w := doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("spawn failure status = %d body %s, want 502", w.Code, w.Body.String())
	}
	if store.ops[1].State != configstore.ReshardStateFailed {
		t.Fatalf("op state = %s, want failed after spawn failure", store.ops[1].State)
	}

	// Ext target + no self pod IP → 500, op failed, no spawn attempted.
	store = newFakeReshardStore()
	sp := &fakeSpawner{noURL: true}
	r, _ = reshardRouterSpawner(store, nil, nil, sp, "internal-secret", defaultShardLister())
	w = doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"external","endpoint":"rds.example.com","password_aws_secret":"duckling-x","password":"p"}}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("no-pod-ip status = %d body %s, want 500", w.Code, w.Body.String())
	}
	if store.ops[1].State != configstore.ReshardStateFailed || len(sp.spawned) != 0 {
		t.Fatalf("no-pod-ip: op state %s, spawned %d", store.ops[1].State, len(sp.spawned))
	}

	// Nil spawner → 503, nothing created.
	store = newFakeReshardStore()
	r, _ = reshardRouterSpawner(store, nil, nil, nil, "internal-secret", defaultShardLister())
	w = doJSON(r, http.MethodPost, "/api/v1/orgs/acme/reshard",
		`{"target":{"type":"cnpg-shard","cnpg_shard":"shard-002"}}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil spawner status = %d, want 503", w.Code)
	}
	if len(store.ops) != 0 {
		t.Fatalf("nil spawner created %d ops, want 0", len(store.ops))
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

	// A RUNNING op (the pod claimed it) gets the cancel flag → 202 (the
	// runner rolls back from wherever it is).
	store.ops[2] = &configstore.ReshardOperation{ID: 2, OrgID: "acme", State: configstore.ReshardStateRunning}
	w = doJSON(r, http.MethodPost, "/api/v1/reshards/2/cancel", "")
	if w.Code != http.StatusAccepted || !store.ops[2].CancelRequested {
		t.Fatalf("cancel running op 2: status %d, flag %t", w.Code, store.ops[2].CancelRequested)
	}

	// A PENDING op (pod not started / never claimed) finishes immediately as
	// cancelled; a re-cancel is 409 (terminal). Op 1 is pending (pod model).
	w = doJSON(r, http.MethodPost, "/api/v1/reshards/1/cancel", "")
	if w.Code != http.StatusOK {
		t.Fatalf("cancel pending status = %d body %s", w.Code, w.Body.String())
	}
	if store.ops[1].State != configstore.ReshardStateCancelled {
		t.Fatalf("pending op state = %s, want cancelled", store.ops[1].State)
	}
	w = doJSON(r, http.MethodPost, "/api/v1/reshards/1/cancel", "")
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
