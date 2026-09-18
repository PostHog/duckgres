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
	envTrinoPoolCatalogIdentity  = "DUCKGRES_TRINO_POOL_CATALOG_WRITER_IDENTITY"
)

// trinoPoolCatalogWriter adapts the fenced publisher to the provisioner's
// catalog client.
type trinoPoolCatalogWriter struct {
	publisher *trinocatalog.Publisher
	db        *sql.DB
	cellID    string
	// nodes is the live coordinator client. The catalog store knows nothing
	// about cluster membership, and inventing an answer here would make the
	// provisioner's readiness check meaningless.
	nodes provisioner.TrinoCatalogClient
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
	mutation.OperationID = "catalog." + mutation.CatalogName + "." + mutation.PayloadHash()[:32]
	result, err := w.publisher.Apply(ctx, mutation)
	if err == nil {
		_ = result
		return nil
	}
	// A lost COMMIT leaves an UNKNOWN outcome. Resolve it from the journal
	// under the same operation id rather than retrying blind, which could
	// publish twice, or compensating with a drop, which could delete a live
	// catalog.
	resolved, resolveErr := w.publisher.ResolveOperation(ctx, mutation.OperationID)
	if resolveErr == nil && resolved != nil {
		return nil
	}
	return err
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
func buildTrinoPoolCatalogWriter(cellID string, epoch int64, nodes provisioner.TrinoCatalogClient) (*trinoPoolCatalogWriter, error) {
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
	db, err := sql.Open("postgres", strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("open catalog store: %w", err)
	}
	// One publisher, a handful of connections: every mutation serializes on the
	// cell's writer row anyway.
	db.SetMaxOpenConns(4)

	identity := strings.TrimSpace(os.Getenv(envTrinoPoolCatalogIdentity))
	if identity == "" {
		identity = "duckgres-control-plane"
	}
	publisher, err := trinocatalog.NewPublisher(db, cellID, identity, epoch)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure catalog publisher: %w", err)
	}
	if bootstrap, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv(envTrinoPoolCatalogBootstrap))); bootstrap {
		// Somebody has to create the additive tables, because a managed-reader
		// coordinator runs no DDL at all. It is explicit so a deployment that
		// has not split its database grants yet cannot do it by accident.
		if err := publisher.EnsureSchema(context.Background()); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("bootstrap catalog store: %w", err)
		}
	}
	// Claiming the cell is explicit and serialized: it locks the same row a
	// mutation locks, so an in-flight write by a previous publisher either
	// commits first or fails its fence check.
	if _, err := publisher.Takeover(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("claim catalog writer: %w", err)
	}
	return &trinoPoolCatalogWriter{publisher: publisher, db: db, cellID: cellID, nodes: nodes}, nil
}
