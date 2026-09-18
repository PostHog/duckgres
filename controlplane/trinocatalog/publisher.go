// Package trinocatalog is the fenced direct publisher for the shared Trino
// catalog store.
//
// Today a catalog is created by issuing CREATE CATALOG against a coordinator,
// which then writes the shared `trino_catalogs` table itself. That routes
// provisioning through a replaceable compute instance and inherits the
// asynchronous SQL-DDL cancellation ambiguity: a timed-out statement has an
// unknown outcome that cannot be resolved. Here duckgres writes the store
// directly, under a row-lock fence, with a mutation journal that turns a lost
// COMMIT response into a lookup instead of a guess. Serving coordinators run as
// managed readers with read-only credentials.
//
// The physical schema is owned by CONTRACT-trino.md; the Trino reader and this
// writer are tested against the same table definitions.
package trinocatalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/trinopool"
)

// Operations recorded in the journal. The spelling is part of the cross-repo
// contract.
const (
	OperationAddOrReplace = "ADD_OR_REPLACE"
	OperationRemove       = "REMOVE"
)

// Budgets. The overall budget bounds one mutation; the lock and statement
// timeouts keep a stuck publisher from holding the cell's writer row.
const (
	DefaultTimeout   = 5 * time.Second
	lockTimeout      = "1s"
	statementTimeout = "3s"

	maxCatalogNameLength   = 255
	maxConnectorLength     = 255
	maxOperationIDLength   = 256
	maxProperties          = 256
	maxPropertyValueLength = 8192
)

var (
	// ErrFenced means a newer writer owns the cell. The mutation was not
	// applied and must never be retried under the old epoch.
	ErrFenced = errors.New("trino catalog writer is fenced by a newer epoch")
	// ErrNotWriter means this process is not the recorded writer. It is
	// deliberately distinct from ErrFenced: it also covers the "I believe I am
	// newer" case, which must NOT silently seize the cell.
	ErrNotWriter = errors.New("trino catalog writer identity or epoch does not match the recorded writer")
	// ErrIntentChanged means the same operation id was reused with different
	// content. That is a caller bug, never a replay.
	ErrIntentChanged = errors.New("trino catalog operation was replayed with different content")

	catalogNamePattern  = regexp.MustCompile(`^[a-z0-9_][a-z0-9_-]*$`)
	operationIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,256}$`)
	writerIdentityMatch = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,255}$`)
)

// Publisher writes one cell's catalogs. Its epoch and identity are the fence:
// both must match the recorded writer state for a mutation to apply.
type Publisher struct {
	db       *sql.DB
	cellID   string
	identity string
	epoch    int64
	timeout  time.Duration
}

// State is the writer-state row as the readers see it.
type State struct {
	Revision     int64
	WriterEpoch  int64
	Identity     string
	CatalogCount int
}

// Result is the outcome of one mutation. Replayed marks a journal hit, which is
// how a lost COMMIT response is resolved.
type Result struct {
	Revision     int64
	CatalogCount int
	Replayed     bool
}

// Mutation is one catalog definition change.
type Mutation struct {
	OperationID   string
	Operation     string
	CatalogName   string
	ConnectorName string
	Properties    map[string]string
}

// NewPublisher validates the writer identity. epoch must be positive: 0 is the
// seeded "nobody has ever written" value and must not be claimable.
func NewPublisher(db *sql.DB, cellID, identity string, epoch int64) (*Publisher, error) {
	if db == nil {
		return nil, errors.New("catalog publisher requires a database handle")
	}
	if cellID == "" || len(cellID) > 255 || !writerIdentityMatch.MatchString(cellID) {
		return nil, errors.New("catalog publisher requires a valid cell id")
	}
	if !writerIdentityMatch.MatchString(identity) {
		return nil, errors.New("catalog publisher requires a valid writer identity")
	}
	if epoch < 1 {
		return nil, errors.New("catalog publisher requires a positive writer epoch")
	}
	return &Publisher{db: db, cellID: cellID, identity: identity, epoch: epoch, timeout: DefaultTimeout}, nil
}

// CellID reports the cell this publisher owns.
func (p *Publisher) CellID() string { return p.cellID }

// Epoch reports the fence this publisher writes under.
func (p *Publisher) Epoch() int64 { return p.epoch }

// EnsureSchema creates the additive tables. A managed-reader coordinator runs no
// DDL at all, so somebody has to; this is that owner. It is only called when
// bootstrap is explicitly enabled.
func (p *Publisher) EnsureSchema(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS trino_catalogs (
			cell_id         varchar     NOT NULL,
			catalog_name    varchar     NOT NULL,
			connector_name  varchar     NOT NULL,
			catalog_version varchar     NOT NULL,
			properties      text        NOT NULL,
			updated_at      timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (cell_id, catalog_name)
		)`,
		`CREATE TABLE IF NOT EXISTS trino_catalog_writer_state (
			cell_id         varchar     NOT NULL,
			revision        bigint      NOT NULL,
			writer_epoch    bigint      NOT NULL,
			writer_identity varchar     NOT NULL,
			catalog_count   integer     NOT NULL,
			updated_at      timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (cell_id)
		)`,
		`CREATE TABLE IF NOT EXISTS trino_catalog_journal (
			cell_id         varchar     NOT NULL,
			revision        bigint      NOT NULL,
			operation_id    varchar     NOT NULL,
			operation       varchar     NOT NULL,
			catalog_name    varchar     NOT NULL,
			catalog_version varchar,
			payload_hash    varchar     NOT NULL,
			writer_epoch    bigint      NOT NULL,
			committed_at    timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (cell_id, revision)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS trino_catalog_journal_operation
			ON trino_catalog_journal (cell_id, operation_id)`,
	}
	for _, statement := range statements {
		if _, err := p.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("catalog store bootstrap: %w", err)
		}
	}
	return nil
}

// State seeds the writer-state row if it is missing and returns it. The seed is
// the one place where catalog_count is derived from the existing rows: an
// initialized store that already holds catalogs must never be described as
// holding zero, or every reader's completeness check fails and the fleet
// freezes on last-good state.
func (p *Publisher) State(ctx context.Context) (State, error) {
	var state State
	err := p.inTransaction(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = p.lockWriterState(ctx, tx)
		return err
	})
	return state, err
}

// Takeover is the explicit, serialized way to become the writer. It is separate
// from Apply on purpose: a mutation must never claim a higher epoch as a side
// effect, or a process that merely believes it is newer could seize the cell in
// the middle of somebody else's write. Taking over locks the same row a
// mutation locks, so an in-flight write either commits before the takeover or
// fails its fence check afterwards. A lease expiry alone decides nothing here.
func (p *Publisher) Takeover(ctx context.Context) (State, error) {
	var state State
	err := p.inTransaction(ctx, func(tx *sql.Tx) error {
		current, err := p.lockWriterState(ctx, tx)
		if err != nil {
			return err
		}
		if current.WriterEpoch > p.epoch {
			return fmt.Errorf("%w: recorded epoch %d is newer than %d", ErrFenced, current.WriterEpoch, p.epoch)
		}
		// An EQUAL epoch held by a different identity is not a takeover, it is a
		// collision: two writers believe they are the same authority. Letting the
		// second one overwrite the identity silently would leave both passing the
		// mutation fence, which is exactly the ambiguity the identity check
		// exists to remove. Only a strictly higher epoch may claim the cell.
		if current.WriterEpoch == p.epoch && current.Identity != "" && current.Identity != p.identity {
			return fmt.Errorf("%w: epoch %d is already held by %q", ErrFenced, current.WriterEpoch, current.Identity)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE trino_catalog_writer_state SET writer_epoch = $2, writer_identity = $3, updated_at = now() WHERE cell_id = $1`,
			p.cellID, p.epoch, p.identity); err != nil {
			return fmt.Errorf("claim catalog writer: %w", err)
		}
		state = State{Revision: current.Revision, WriterEpoch: p.epoch, Identity: p.identity, CatalogCount: current.CatalogCount}
		return nil
	})
	return state, err
}

// Apply publishes one catalog mutation. Everything happens in a single
// transaction: fence check, replay resolution, the definition change, the
// revision bump, the recomputed count and the journal row. Either all of it is
// visible to a reader or none of it is.
func (p *Publisher) Apply(ctx context.Context, mutation Mutation) (Result, error) {
	if err := mutation.validate(); err != nil {
		return Result{}, err
	}
	var result Result
	err := p.inTransaction(ctx, func(tx *sql.Tx) error {
		current, err := p.lockWriterState(ctx, tx)
		if err != nil {
			return err
		}
		if err := p.checkFence(current); err != nil {
			return err
		}

		// Replay resolution comes before any effect, so a retried intent can
		// never publish twice.
		if recorded, found, err := p.journalEntry(ctx, tx, mutation.OperationID); err != nil {
			return err
		} else if found {
			if recorded.payloadHash != mutation.PayloadHash() {
				return fmt.Errorf("%w: operation %q", ErrIntentChanged, mutation.OperationID)
			}
			result = Result{Revision: recorded.revision, CatalogCount: current.CatalogCount, Replayed: true}
			return nil
		}

		version, err := p.applyDefinition(ctx, tx, mutation)
		if err != nil {
			return err
		}

		revision := current.Revision + 1
		count, err := p.countCatalogs(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE trino_catalog_writer_state
			 SET revision = $2, catalog_count = $3, updated_at = now()
			 WHERE cell_id = $1`,
			p.cellID, revision, count); err != nil {
			return fmt.Errorf("advance catalog revision: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO trino_catalog_journal
			 (cell_id, revision, operation_id, operation, catalog_name, catalog_version, payload_hash, writer_epoch)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			p.cellID, revision, mutation.OperationID, mutation.Operation, mutation.CatalogName,
			version, mutation.PayloadHash(), p.epoch); err != nil {
			return fmt.Errorf("record catalog journal: %w", err)
		}
		result = Result{Revision: revision, CatalogCount: count}
		return nil
	})
	return result, err
}

// ResolveOperation reports what happened to an operation whose response was
// lost. A nil result means the mutation never committed, so it is safe to apply
// under the same operation id. This is the only correct answer to a timed-out
// COMMIT: retrying blindly could double-publish, and compensating with a DROP
// could delete a live catalog.
func (p *Publisher) ResolveOperation(ctx context.Context, operationID string) (*Result, error) {
	if !operationIDPattern.MatchString(operationID) {
		return nil, errors.New("catalog operation id is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var revision int64
	err := p.db.QueryRowContext(ctx,
		`SELECT revision FROM trino_catalog_journal WHERE cell_id = $1 AND operation_id = $2`,
		p.cellID, operationID).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve catalog operation: %w", err)
	}
	return &Result{Revision: revision, Replayed: true}, nil
}

// ResolveIntentSince reports whether a mutation with this exact intent
// committed AFTER the given revision.
//
// It exists because an operation id that carries the store's revision cannot be
// recomputed once the revision has moved: a caller that loses the COMMIT
// response and retries derives a DIFFERENT id, so resolving by id alone always
// misses the very case the journal is for. The intent - this catalog, this
// payload - plus "later than the revision I read before I tried" identifies the
// same commit without matching an older, identical publication of the same
// catalog.
func (p *Publisher) ResolveIntentSince(ctx context.Context, catalogName, payloadHash string, afterRevision int64) (*Result, error) {
	if catalogName == "" || payloadHash == "" {
		return nil, errors.New("resolving a catalog intent requires a catalog name and payload hash")
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var revision int64
	err := p.db.QueryRowContext(ctx,
		`SELECT revision FROM trino_catalog_journal
		 WHERE cell_id = $1 AND catalog_name = $2 AND payload_hash = $3 AND revision > $4
		 ORDER BY revision DESC LIMIT 1`,
		p.cellID, catalogName, payloadHash, afterRevision).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve catalog intent: %w", err)
	}
	return &Result{Revision: revision, Replayed: true}, nil
}

// checkFence implements root integration decision 2: the exact epoch AND the
// exact identity must match. A higher local epoch is not authority; it is a
// reason to call Takeover explicitly.
func (p *Publisher) checkFence(current State) error {
	if current.WriterEpoch > p.epoch {
		return fmt.Errorf("%w: recorded epoch %d is newer than %d", ErrFenced, current.WriterEpoch, p.epoch)
	}
	if current.WriterEpoch != p.epoch || current.Identity != p.identity {
		return fmt.Errorf("%w: recorded writer is %q at epoch %d, this writer is %q at epoch %d",
			ErrNotWriter, current.Identity, current.WriterEpoch, p.identity, p.epoch)
	}
	return nil
}

// lockWriterState seeds and then locks the cell's writer row. The lock is what
// serializes every publisher of the cell, so it is taken before anything is
// read or decided.
func (p *Publisher) lockWriterState(ctx context.Context, tx *sql.Tx) (State, error) {
	// Seed with the REAL row count, never a literal zero.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO trino_catalog_writer_state (cell_id, revision, writer_epoch, writer_identity, catalog_count)
		 SELECT $1::varchar, 0, 0, '', count(*) FROM trino_catalogs WHERE cell_id = $1
		 ON CONFLICT (cell_id) DO NOTHING`, p.cellID); err != nil {
		return State{}, fmt.Errorf("seed catalog writer state: %w", err)
	}
	var state State
	if err := tx.QueryRowContext(ctx,
		`SELECT revision, writer_epoch, writer_identity, catalog_count
		 FROM trino_catalog_writer_state WHERE cell_id = $1 FOR UPDATE`, p.cellID).
		Scan(&state.Revision, &state.WriterEpoch, &state.Identity, &state.CatalogCount); err != nil {
		return State{}, fmt.Errorf("lock catalog writer state: %w", err)
	}
	return state, nil
}

type journalRecord struct {
	revision    int64
	payloadHash string
}

func (p *Publisher) journalEntry(ctx context.Context, tx *sql.Tx, operationID string) (journalRecord, bool, error) {
	var record journalRecord
	err := tx.QueryRowContext(ctx,
		`SELECT revision, payload_hash FROM trino_catalog_journal WHERE cell_id = $1 AND operation_id = $2`,
		p.cellID, operationID).Scan(&record.revision, &record.payloadHash)
	if errors.Is(err, sql.ErrNoRows) {
		return journalRecord{}, false, nil
	}
	if err != nil {
		return journalRecord{}, false, fmt.Errorf("read catalog journal: %w", err)
	}
	return record, true, nil
}

// applyDefinition writes the definition change and returns the catalog version
// recorded in the journal (null for a removal).
func (p *Publisher) applyDefinition(ctx context.Context, tx *sql.Tx, mutation Mutation) (any, error) {
	if mutation.Operation == OperationRemove {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM trino_catalogs WHERE cell_id = $1 AND catalog_name = $2`,
			p.cellID, mutation.CatalogName); err != nil {
			return nil, fmt.Errorf("remove catalog: %w", err)
		}
		return nil, nil
	}
	// Properties are stored verbatim, secret references included: Trino resolves
	// ${ENV:...} per node, and resolving here would put a credential in the row.
	properties, err := json.Marshal(mutation.Properties)
	if err != nil {
		return nil, fmt.Errorf("encode catalog properties: %w", err)
	}
	version := mutation.CatalogVersion()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties, updated_at)
		 VALUES ($1,$2,$3,$4,$5, now())
		 ON CONFLICT (cell_id, catalog_name) DO UPDATE SET
			connector_name = excluded.connector_name,
			catalog_version = excluded.catalog_version,
			properties = excluded.properties,
			updated_at = now()`,
		p.cellID, mutation.CatalogName, mutation.ConnectorName, version, string(properties)); err != nil {
		return nil, fmt.Errorf("publish catalog: %w", err)
	}
	return version, nil
}

// countCatalogs recomputes the row count inside the mutation transaction. An
// optimistic increment would drift from reality the moment anything else
// touched the table — during the migration bridge, exactly what happens.
func (p *Publisher) countCatalogs(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM trino_catalogs WHERE cell_id = $1`, p.cellID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count catalogs: %w", err)
	}
	return count, nil
}

func (p *Publisher) inTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin catalog transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	// Bound the lock wait and each statement separately from the overall
	// budget, so a wedged publisher cannot hold the cell's writer row.
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '`+lockTimeout+`'`); err != nil {
		return fmt.Errorf("set lock timeout: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL statement_timeout = '`+statementTimeout+`'`); err != nil {
		return fmt.Errorf("set statement timeout: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		// The outcome is now UNKNOWN, not failed. The caller resolves it with
		// ResolveOperation against the same operation id.
		return fmt.Errorf("commit catalog transaction: %w", err)
	}
	committed = true
	return nil
}

func (m Mutation) validate() error {
	if !operationIDPattern.MatchString(m.OperationID) || len(m.OperationID) > maxOperationIDLength {
		return errors.New("catalog mutation requires a valid operation id")
	}
	if m.Operation != OperationAddOrReplace && m.Operation != OperationRemove {
		return fmt.Errorf("unsupported catalog operation %q", m.Operation)
	}
	if !catalogNamePattern.MatchString(m.CatalogName) || len(m.CatalogName) > maxCatalogNameLength {
		return errors.New("catalog mutation requires a valid catalog name")
	}
	if m.Operation == OperationRemove {
		return nil
	}
	// Only validated connector names and properties are published; the store is
	// read by every coordinator of the cell.
	if m.ConnectorName == "" || len(m.ConnectorName) > maxConnectorLength || strings.ContainsAny(m.ConnectorName, " \t\n\r'\"") {
		return errors.New("catalog mutation requires a valid connector name")
	}
	if len(m.Properties) > maxProperties {
		return errors.New("catalog mutation declares too many properties")
	}
	for key, value := range m.Properties {
		if key == "" || len(key) > 255 || strings.ContainsAny(key, " \t\n\r") {
			return fmt.Errorf("catalog property %q has an invalid name", key)
		}
		if len(value) > maxPropertyValueLength || strings.ContainsAny(value, "\n\r") {
			return fmt.Errorf("catalog property %q has an invalid value", key)
		}
	}
	return nil
}

// CatalogVersion is the content hash the coordinators compute for themselves.
// It has to match byte for byte or every unchanged catalog looks new.
func (m Mutation) CatalogVersion() string {
	if m.Operation == OperationRemove {
		return ""
	}
	return trinopool.CatalogVersion(m.CatalogName, m.ConnectorName, m.Properties)
}

// PayloadHash identifies the INTENT of a mutation, so a retry of the same
// intent is recognizable as a replay and a different intent under the same
// operation id is recognizable as a conflict. The encoding is length-prefixed
// so no concatenation of fields can collide with another.
func (m Mutation) PayloadHash() string {
	digest := sha256.New()
	write := func(value string) {
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(value)))
		_, _ = digest.Write(length)
		_, _ = digest.Write([]byte(value))
	}
	write(m.Operation)
	write(m.CatalogName)
	write(m.ConnectorName)
	keys := make([]string, 0, len(m.Properties))
	for key := range m.Properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(keys)))
	_, _ = digest.Write(length)
	for _, key := range keys {
		write(key)
		write(m.Properties[key])
	}
	return hex.EncodeToString(digest.Sum(nil))
}
