//go:build kubernetes

package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

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
)

// trinoPoolCatalogWriter adapts the fenced publisher to the provisioner's
// catalog client.
type trinoPoolCatalogWriter struct {
	db     *sql.DB
	cellID string
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
		return nil, fmt.Errorf("%w: this control plane does not hold the pool authority", trinocatalog.ErrNotWriter)
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
	if _, err := publisher.Takeover(ctx); err != nil {
		return fmt.Errorf("claim catalog writer: %w", err)
	}
	return nil
}

func (w *trinoPoolCatalogWriter) ListNodes(ctx context.Context) ([]provisioner.TrinoNode, error) {
	if w.nodes == nil {
		return nil, errors.New("catalog writer has no coordinator client for node inventory")
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
	publisher, err := w.publisher()
	if err != nil {
		return err
	}
	state, err := publisher.State(ctx)
	if err != nil {
		return fmt.Errorf("read catalog writer state: %w", err)
	}
	mutation.OperationID = fmt.Sprintf("catalog.%s.r%d.%s", mutation.CatalogName, state.Revision, mutation.PayloadHash()[:16])

	if _, err := publisher.Apply(ctx, mutation); err == nil {
		return nil
	} else if isTerminalPublishError(err) {
		// A fence refusal or a changed intent is a decision, not an unknown
		// outcome. Resolving it against the journal would report somebody
		// else's row as this call's success.
		return err
	}

	// Anything else may be a lost COMMIT, whose outcome is UNKNOWN. Resolve it
	// from the journal under the same operation id rather than retrying blind,
	// which could publish twice, or compensating with a drop, which could
	// delete a live catalog.
	resolved, resolveErr := publisher.ResolveOperation(ctx, mutation.OperationID)
	if resolveErr == nil && resolved != nil {
		return nil
	}
	return err
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
func buildTrinoPoolCatalogWriter(cellID string, authority func() (configstore.TrinoPoolLease, bool), nodes provisioner.TrinoCatalogClient) (*trinoPoolCatalogWriter, error) {
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
	// pgx, not "postgres": lib/pq is not linked into the control-plane binary,
	// so sql.Open("postgres", ...) fails at startup with `unknown driver`. The
	// rest of the control plane registers pgx (see storage_meter.go).
	db, err := sql.Open("pgx", strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("open catalog store: %w", err)
	}
	// One publisher, a handful of connections: every mutation serializes on the
	// cell's writer row anyway.
	db.SetMaxOpenConns(4)

	writer := &trinoPoolCatalogWriter{db: db, cellID: cellID, authority: authority, nodes: nodes}

	if bootstrap, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv(envTrinoPoolCatalogBootstrap))); bootstrap {
		// Somebody has to create the additive tables, because a managed-reader
		// coordinator runs no DDL at all. It is explicit so a deployment that
		// has not split its database grants yet cannot do it by accident. This
		// is additive DDL only and takes no fence, because it publishes nothing.
		schema, err := trinocatalog.NewPublisher(db, cellID, "duckgres-bootstrap", 1)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure catalog bootstrap: %w", err)
		}
		if err := schema.EnsureSchema(context.Background()); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("bootstrap catalog store: %w", err)
		}
	}
	return writer, nil
}
