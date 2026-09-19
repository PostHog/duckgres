//go:build kubernetes

package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/trinocatalog"
)

// The catalog writer bridge.
//
// Today the provisioner creates a catalog by issuing CREATE CATALOG against a
// coordinator, which then writes the shared catalog table itself. That routes
// provisioning through a replaceable compute instance and inherits the
// asynchronous SQL-DDL cancellation ambiguity: a timed-out statement leaves an
// outcome nobody can resolve.
//
// This adapter presents the fenced direct publisher through the interface the
// provisioner already uses, so the reconcile loop is unchanged and the writes
// become a single fenced transaction with a journal. It is selected per cell
// and only when explicitly enabled; every other cell keeps the coordinator path
// byte for byte.
//
// Node inventory is NOT something the catalog store can answer. It is delegated
// to the existing coordinator client, so readiness still comes from the live
// cluster rather than from a table.
const (
	envTrinoPoolCatalogWriter    = "DUCKGRES_TRINO_POOL_CATALOG_WRITER_ENABLED"
	envTrinoPoolCatalogBootstrap = "DUCKGRES_TRINO_POOL_CATALOG_BOOTSTRAP"
	envTrinoPoolCatalogDSNFile   = "DUCKGRES_TRINO_POOL_CATALOG_DSN_FILE"
	// envTrinoPoolCatalogSchema names the schema the catalog tables live in.
	// The publisher credential carries a database and no schema, and the role's
	// privileges are on the cell's schema only.
	envTrinoPoolCatalogSchema = "DUCKGRES_TRINO_POOL_CATALOG_SCHEMA"

	// trinoPoolCatalogBootstrapBudget bounds the additive DDL this runs during
	// startup wiring. An unreachable database must fail readably rather than
	// hang the control plane's boot.
	trinoPoolCatalogBootstrapBudget = 30 * time.Second
)

// trinoPoolSchemaPattern is the unquoted-identifier shape. The schema is
// interpolated into a connection parameter, so anything needing quotes is
// refused rather than escaped.
var trinoPoolSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// trinoPoolRevisionStore records the published catalog revision on the pool
// row. It is an interface so the bridge can be tested without a config store.
type trinoPoolRevisionStore interface {
	RecordTrinoPoolPublicationRevision(ctx context.Context, lease configstore.TrinoPoolLease, poolID string, revision int64) error
}

// trinoPoolCatalogWriter adapts the fenced publisher to the provisioner's
// catalog client.
type trinoPoolCatalogWriter struct {
	db     *sql.DB
	cellID string
	// store records the published revision on the pool row, under the same
	// authority the publication itself was fenced by.
	store trinoPoolRevisionStore
	// authority returns the pool lease this control plane currently holds. The
	// catalog writer fence is the SAME authority as the operator's: the writer
	// epoch is the pool's authority epoch and the writer identity is the
	// per-process owner. Building the publisher per call, rather than once at
	// startup, is what makes that true - a process that has not won the pool
	// cannot write catalogs, and a superseded one stops being able to.
	authority func() (configstore.TrinoPoolLease, bool)
	// nodes is the live coordinator client. The catalog store knows nothing
	// about cluster membership, and inventing an answer here would make the
	// provisioner's readiness check meaningless.
	nodes provisioner.TrinoCatalogClient
}

// publisher builds a fenced publisher for the CURRENT authority. It refuses
// when this process holds no lease, so an unelected or superseded replica
// cannot publish at all.
func (w *trinoPoolCatalogWriter) publisher() (*trinocatalog.Publisher, error) {
	lease, ok := w.authority()
	if !ok || lease.Epoch < 1 {
		// Both sentinels matter. ErrNotWriter keeps the publish path treating
		// this as a DECISION rather than a lost outcome to resolve, and
		// ErrTrinoCatalogNotThisReplica tells the provisioner's reconcile that
		// this is not its work - so it leaves the tenants' state rows alone
		// instead of marking every pooled org Failed on every non-leader.
		return nil, fmt.Errorf("%w: this control plane does not hold the pool authority (%w)",
			trinocatalog.ErrNotWriter, provisioner.ErrTrinoCatalogNotThisReplica)
	}
	return trinocatalog.NewPublisher(w.db, w.cellID, lease.Owner, lease.Epoch)
}

// ClaimWriter takes the catalog store's writer fence for the lease the operator
// just acquired. It is called once per leadership term, not at startup: the
// claim has to follow the pool authority, or every replica would claim the cell
// on boot and the fence would distinguish nothing.
func (w *trinoPoolCatalogWriter) ClaimWriter(ctx context.Context) error {
	publisher, err := w.publisher()
	if err != nil {
		return err
	}
	state, err := publisher.Takeover(ctx)
	if err != nil {
		return fmt.Errorf("claim catalog writer: %w", err)
	}
	// Checkpoint the watermark from the state the takeover just read.
	//
	// The catalog store is authoritative for which revision is published; the
	// pool row only CACHES it for the admission gate. A previous term can have
	// committed a catalog and then failed to record the revision, and nothing
	// else republishes it - later catalogs already exist, so no later mutation
	// arrives to carry the number forward. Reading it here is the bounded
	// recovery: a leadership change is exactly when somebody can fix it.
	//
	// A failed checkpoint fails the claim. Installing the writer anyway would
	// leave the gate believing an older revision, which is how a tenant is
	// admitted and reported ready without its catalog.
	if err := w.checkpoint(ctx, state.Revision); err != nil {
		return err
	}
	return nil
}

// PublishedRevision reports the revision the catalog store itself is at.
//
// This is the authority for the admission gate: the pool row's
// publication_revision is a cache of it, and a cache that failed to update is
// indistinguishable from "nothing new was published" unless somebody asks the
// store.
func (w *trinoPoolCatalogWriter) PublishedRevision(ctx context.Context) (int64, error) {
	publisher, err := w.publisher()
	if err != nil {
		return 0, err
	}
	state, err := publisher.State(ctx)
	if err != nil {
		return 0, fmt.Errorf("read catalog writer state: %w", err)
	}
	return state.Revision, nil
}

// checkpoint records a revision on the pool row under the CURRENT authority.
func (w *trinoPoolCatalogWriter) checkpoint(ctx context.Context, revision int64) error {
	lease, held := w.authority()
	if !held || w.store == nil || revision <= 0 {
		return nil
	}
	if err := w.store.RecordTrinoPoolPublicationRevision(ctx, lease, w.cellID, revision); err != nil {
		return fmt.Errorf("checkpoint published catalog revision %d: %w", revision, err)
	}
	return nil
}

func (w *trinoPoolCatalogWriter) ListNodes(ctx context.Context) ([]provisioner.TrinoNode, error) {
	if w.nodes == nil {
		// A pooled cell has no fixed coordinator to take an inventory from. The
		// sentinel is what tells the provisioner's readiness step that this
		// probe does not APPLY here, as opposed to failing - the pool proves the
		// same thing per member, at admission, against the instance that will
		// actually serve the tenant.
		return nil, provisioner.ErrTrinoNodeInventoryUnavailable
	}
	return w.nodes.ListNodes(ctx)
}

// ListCatalogs reads the published set straight from the store, which is the
// desired state rather than one coordinator's applied view.
func (w *trinoPoolCatalogWriter) ListCatalogs(ctx context.Context) ([]string, error) {
	rows, err := w.db.QueryContext(ctx,
		`SELECT catalog_name FROM trino_catalogs WHERE cell_id = $1 ORDER BY catalog_name`, w.cellID)
	if err != nil {
		return nil, fmt.Errorf("list published catalogs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var catalogs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read published catalog: %w", err)
		}
		catalogs = append(catalogs, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read published catalogs: %w", err)
	}
	return catalogs, nil
}

// CatalogConnectors reports which connector each published catalog declares.
//
// The managed-Hoglake path refuses to adopt an existing catalog it cannot
// inspect: an org whose catalog is still DuckLake must be migrated explicitly,
// never silently re-pointed at Hoglake metadata. On a coordinator-mediated cell
// that inspection is a `system.metadata.catalogs` query; a pooled cell has no
// fixed coordinator to ask, so the same question is answered from the store the
// coordinators reconcile FROM.
//
// What this reports is the PUBLISHED definition, not a running coordinator's
// applied state, and the two differ while a member is still catching up. That
// is sound for this check and only this check - it decides whether duckgres may
// replace its own published definition. Whether any member has actually applied
// it stays with pool admission, which proves it per member against the
// published revision; nothing here may be read as evidence that a catalog is
// operational.
func (w *trinoPoolCatalogWriter) CatalogConnectors(ctx context.Context) (map[string]string, error) {
	rows, err := w.db.QueryContext(ctx,
		`SELECT catalog_name, connector_name FROM trino_catalogs WHERE cell_id = $1`, w.cellID)
	if err != nil {
		return nil, fmt.Errorf("read published catalog connectors: %w", err)
	}
	defer func() { _ = rows.Close() }()

	connectors := map[string]string{}
	for rows.Next() {
		var name, connector string
		if err := rows.Scan(&name, &connector); err != nil {
			return nil, fmt.Errorf("read published catalog connector: %w", err)
		}
		if name == "" || connector == "" {
			return nil, fmt.Errorf("published catalog inventory is incomplete for cell %s", w.cellID)
		}
		connectors[name] = connector
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read published catalog connectors: %w", err)
	}
	return connectors, nil
}

func (w *trinoPoolCatalogWriter) CreateCatalog(ctx context.Context, name string, properties map[string]string) error {
	return w.publish(ctx, trinocatalog.Mutation{
		Operation:     trinocatalog.OperationAddOrReplace,
		CatalogName:   name,
		ConnectorName: properties["connector.name"],
		Properties:    connectorProperties(properties),
	})
}

// AlterCatalog is the same publication as a create: the store holds one row per
// catalog, and the coordinators reconcile to whatever it says.
func (w *trinoPoolCatalogWriter) AlterCatalog(ctx context.Context, name string, properties map[string]string) error {
	return w.CreateCatalog(ctx, name, properties)
}

func (w *trinoPoolCatalogWriter) DropCatalog(ctx context.Context, name string) error {
	return w.publish(ctx, trinocatalog.Mutation{
		Operation:   trinocatalog.OperationRemove,
		CatalogName: name,
	})
}

// publish derives the operation id from the intent itself, so a retry of the
// same intent is recognized as a replay and returns the recorded revision
// instead of publishing twice. A CHANGED intent is a different operation, which
// is what advances the revision.
func (w *trinoPoolCatalogWriter) publish(ctx context.Context, mutation trinocatalog.Mutation) error {
	// The operation id includes the revision the store is at RIGHT NOW, not
	// only the intent's content.
	//
	// Content alone was wrong in a way that silently lost a catalog: create X,
	// drop X, then create X again with identical properties produced the same
	// operation id as the first create, hit the journal, and returned the old
	// revision without writing anything - so the recreated catalog was never
	// republished and no coordinator ever saw it again. Including the current
	// revision makes each of those three intents its own operation, while an
	// immediate retry of the SAME intent (nothing else committed in between)
	// still resolves as a replay.
	lease, held := w.authority()
	publisher, err := w.publisher()
	if err != nil {
		return err
	}
	state, err := publisher.State(ctx)
	if err != nil {
		return fmt.Errorf("read catalog writer state: %w", err)
	}
	mutation.OperationID = fmt.Sprintf("catalog.%s.r%d.%s", mutation.CatalogName, state.Revision, mutation.PayloadHash()[:16])

	result, err := publisher.Apply(ctx, mutation)
	switch {
	case err == nil:
	case isTerminalPublishError(err):
		// A fence refusal or a changed intent is a decision, not an unknown
		// outcome. Resolving it against the journal would report somebody
		// else's row as this call's success.
		return err
	default:
		// Anything else may be a lost COMMIT, whose outcome is UNKNOWN.
		// Resolving by operation id alone cannot answer it: the id carries the
		// revision this attempt read, and a committed mutation has already
		// moved it, so the retry computes a different id and misses. The INTENT
		// plus "later than the revision I read" identifies the same commit.
		resolved, resolveErr := publisher.ResolveIntentSince(ctx, mutation.CatalogName, mutation.PayloadHash(), state.Revision)
		if resolveErr != nil || resolved == nil {
			return err
		}
		result = *resolved
	}

	// The published revision is the gate a candidate must have applied before it
	// can be admitted. Recording it is what arms that gate; without it every
	// coordinator is certified at revision zero and a member missing the newest
	// tenant looks current.
	//
	// A failed checkpoint is RETURNED, not logged and dropped. The catalog is
	// committed either way, but nothing republishes it: later catalogs already
	// exist, so no future mutation carries the number forward, and the gate
	// would keep certifying members against a revision that predates this
	// tenant - admitting it, and reporting the warehouse ready, without its
	// catalog. Surfacing it holds that org not-ready until a later tick or the
	// next leadership claim checkpoints the watermark, and the publication
	// itself resolves as a replay.
	if held && w.store != nil && result.Revision > 0 {
		if err := w.store.RecordTrinoPoolPublicationRevision(ctx, lease, w.cellID, result.Revision); err != nil {
			slog.Error("Trino pool publication revision could not be recorded; tenant admission stays closed until it is.",
				"cell", w.cellID, "revision", result.Revision, "error", err)
			return fmt.Errorf("checkpoint published catalog revision %d: %w", result.Revision, err)
		}
	}
	return nil
}

// isTerminalPublishError reports an outcome the publisher DECIDED, as opposed
// to one it never got to observe.
func isTerminalPublishError(err error) bool {
	return errors.Is(err, trinocatalog.ErrFenced) ||
		errors.Is(err, trinocatalog.ErrNotWriter) ||
		errors.Is(err, trinocatalog.ErrIntentChanged)
}

// connectorProperties strips the connector name, which the store keeps in its
// own column and which is not part of the property map Trino hashes.
func connectorProperties(properties map[string]string) map[string]string {
	filtered := make(map[string]string, len(properties))
	for key, value := range properties {
		if key == "connector.name" {
			continue
		}
		filtered[key] = value
	}
	return filtered
}

// buildTrinoPoolCatalogWriter constructs the bridge for one cell. It returns
// (nil, nil) when the writer is not enabled, which leaves the existing
// coordinator-mediated path in place.
//
// The DSN is read from a file rather than an environment variable because it
// carries the publisher credential, which infra provisions separately from the
// coordinators' read-only reader role.
func buildTrinoPoolCatalogWriter(cellID string, store trinoPoolRevisionStore, authority func() (configstore.TrinoPoolLease, bool), nodes provisioner.TrinoCatalogClient) (*trinoPoolCatalogWriter, error) {
	enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(envTrinoPoolCatalogWriter)))
	if err != nil || !enabled {
		return nil, nil
	}
	path := strings.TrimSpace(os.Getenv(envTrinoPoolCatalogDSNFile))
	if path == "" {
		return nil, fmt.Errorf("%s is enabled but %s is unset", envTrinoPoolCatalogWriter, envTrinoPoolCatalogDSNFile)
	}
	raw, err := readRolloutSecretFile(path, 8192)
	if err != nil {
		return nil, fmt.Errorf("read catalog writer credential: %w", err)
	}
	// The schema the catalog tables live in.
	//
	// The credential names a DATABASE and nothing else, and the publisher role
	// holds privileges on the cell's schema alone - so unqualified SQL resolves
	// against `public`, where that role can neither create nor read anything.
	// It is required rather than defaulted: guessing a schema would produce
	// exactly that failure at the first publication, on a path an operator has
	// no reason to suspect.
	schema, err := trinoPoolCatalogSchema()
	if err != nil {
		return nil, err
	}
	dsn, err := withSearchPath(strings.TrimSpace(string(raw)), schema)
	if err != nil {
		return nil, err
	}
	// pgx, not "postgres": lib/pq is not linked into the control-plane binary,
	// so sql.Open("postgres", ...) fails at startup with `unknown driver`. The
	// rest of the control plane registers pgx (see storage_meter.go).
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open catalog store: %w", err)
	}
	// One publisher, a handful of connections: every mutation serializes on the
	// cell's writer row anyway.
	db.SetMaxOpenConns(4)

	writer := &trinoPoolCatalogWriter{db: db, cellID: cellID, store: store, authority: authority, nodes: nodes}

	if bootstrap, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv(envTrinoPoolCatalogBootstrap))); bootstrap {
		// Somebody has to create the additive tables, because a managed-reader
		// coordinator runs no DDL at all. It is explicit so a deployment that
		// has not split its database grants yet cannot do it by accident. This
		// is additive DDL only and takes no fence, because it publishes nothing.
		//
		// It is BOUNDED: this runs during startup wiring, so an unreachable
		// database must fail with a readable error rather than hang the control
		// plane's boot indefinitely. The error keeps its cause so the next
		// attempt is diagnosable.
		ctx, cancel := context.WithTimeout(context.Background(), trinoPoolCatalogBootstrapBudget)
		defer cancel()
		bootstrapper, err := trinocatalog.NewPublisher(db, cellID, "duckgres-bootstrap", 1)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure catalog bootstrap: %w", err)
		}
		if err := bootstrapper.EnsureSchema(ctx); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("bootstrap catalog store in schema %q: %w", schema, err)
		}
	}
	return writer, nil
}

// trinoPoolCatalogSchema resolves and validates the schema the catalog tables
// live in.
//
// The value is interpolated into a connection parameter, so it is checked
// against the unquoted-identifier shape rather than escaped: a schema name that
// needs quoting is a deployment mistake worth refusing, and accepting one here
// would put caller-shaped text into a connection string.
func trinoPoolCatalogSchema() (string, error) {
	schema := strings.TrimSpace(os.Getenv(envTrinoPoolCatalogSchema))
	if schema == "" {
		return "", fmt.Errorf("%s is enabled but %s is unset: the publisher credential names a database only, and the role's privileges are on the cell's schema",
			envTrinoPoolCatalogWriter, envTrinoPoolCatalogSchema)
	}
	if !trinoPoolSchemaPattern.MatchString(schema) {
		return "", fmt.Errorf("%s=%q is not a plain lower-case identifier", envTrinoPoolCatalogSchema, schema)
	}
	return schema, nil
}

// withSearchPath pins the connection's search_path to that one schema.
//
// Every statement the publisher issues is unqualified, and the reader side of
// the same tables resolves them the same way, so the schema belongs on the
// connection rather than being threaded through each statement. A DSN that
// already sets a search_path is refused instead of silently overridden: two
// sources for the same setting is how a publisher ends up writing where nobody
// is looking.
func withSearchPath(dsn, schema string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		// A keyword/value DSN ("host=... dbname=...") is not a URL. Rather than
		// re-implement that grammar, refuse it: infra provisions a URL.
		return "", fmt.Errorf("catalog writer credential is not a postgres:// URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("catalog writer credential is not a postgres:// URL")
	}
	query := parsed.Query()
	if existing := strings.TrimSpace(query.Get("search_path")); existing != "" && existing != schema {
		return "", fmt.Errorf("catalog writer credential already pins search_path=%q, which disagrees with %s=%q",
			existing, envTrinoPoolCatalogSchema, schema)
	}
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
