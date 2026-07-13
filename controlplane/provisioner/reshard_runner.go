//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// ReshardRunner drives reshard operations (see docs/design/resharding.md and
// configstore/reshard.go). Every CP replica runs one; a single replica wins
// each operation via the claim CAS, heartbeats it, and is fenced by the
// runner epoch on every write, so a zombie ex-runner can never corrupt an op
// another replica took over.
//
// Step order for →cnpg targets: block → drain → pause compaction → backup
// catalog (pre-flip pg_dump of the source to the org's S3 bucket — the safety
// net) → flip → wait target → copy (source survives the flip: a shard change
// re-points the provider-sql MRs in place, orphaning the old role/DB) → verify
// stability → cleanup source (cnpg sources only) → finalize.
//
// cnpg→external inverts copy and flip: copy → verify source → ORPHAN the cnpg
// source (retainCnpgOnFlip=true, so the type flip un-renders the cnpg MRs
// WITHOUT deleting the role/DB) → flip type to external → verify the external
// catalog is complete → only THEN drop the orphaned cnpg source. Any failure
// before that drop flips back to cnpg-shard and re-adopts the still-present
// role/DB by external-name — instant recovery, no empty-recreate, no copy-back.
// The pre-flip backup still runs first, and for THIS destructive direction it
// is a HARD prerequisite (a backup failure fails the op before the flip).
type ReshardRunner struct {
	store    reshardStore
	duckling reshardDucklingClient
	copier   CatalogCopier
	backuper CatalogBackuper
	cpID     string

	// pollInterval is the claim-scan cadence; configPollInterval is the CP
	// config-snapshot poll (the propagation wait after the block CAS).
	pollInterval       time.Duration
	configPollInterval time.Duration

	heartbeatInterval time.Duration
	staleAfter        time.Duration
	flipTimeout       time.Duration
	hotIdleGrace      time.Duration
	// loopPoll is the inner-loop poll cadence (drain checks, flip waits,
	// cancel checks). 5s in production; tests shrink it.
	loopPoll time.Duration

	// baseCtx is the runner's LIFECYCLE context (the one handed to Run). Ops
	// adopted straight from the admin HTTP handler (claim-on-create) execute
	// under this context, NOT the request context — a reshard runs for minutes
	// and the request ctx is cancelled the moment the 202 response returns.
	// Defaults to context.Background(); Run overwrites it with its own ctx.
	baseCtx context.Context

	// extPasswords holds the EPHEMERAL external passwords by op id: the
	// cnpg→ext target password and the ext→cnpg source password (both come
	// from the create request and are never persisted). A takeover runner
	// does not have them and fails the affected step with a clear re-run
	// message.
	extPasswords sync.Map // opID int64 -> string

	running sync.Map // opID int64 -> struct{}
}

// reshardStore is the config-store surface the runner needs (fakeable).
type reshardStore interface {
	ListClaimableReshardOperations(staleAfter time.Duration) ([]int64, error)
	ClaimReshardOperation(id int64, runnerCP string, staleAfter time.Duration) (*configstore.ReshardOperation, error)
	GetReshardOperation(id int64) (*configstore.ReshardOperation, error)
	UpdateReshardStep(id int64, runnerCP string, epoch int64, step string) error
	UpdateReshardFields(id int64, runnerCP string, epoch int64, updates map[string]interface{}) error
	TouchReshardHeartbeat(id int64, runnerCP string, epoch int64) error
	FinishReshardOperation(id int64, runnerCP string, epoch int64, state configstore.ReshardState, errMsg string) error
	AppendReshardLog(opID int64, level, message string) error

	SetWarehouseResharding(orgID string) error
	UpdateWarehouseState(orgID string, expectedState configstore.ManagedWarehouseProvisioningState, updates map[string]interface{}) error
	OrgConnectionDrainState(orgID string) (configstore.OrgConnectionDrainStatus, error)
	ListWorkerRecordsForOrg(orgID string) ([]configstore.WorkerRecord, error)
	RetireHotIdleWorker(record *configstore.WorkerRecord) (bool, error)
}

// reshardDucklingClient is the Duckling CR surface the runner needs.
type reshardDucklingClient interface {
	Get(ctx context.Context, name string) (*DucklingStatus, error)
	GetCompactionSetting(ctx context.Context, name string) (enabled, present bool, err error)
	SetCompactionEnabled(ctx context.Context, name string, enabled *bool) error
	SetMetadataStoreCnpg(ctx context.Context, name, shard string) error
	SetMetadataStoreExternal(ctx context.Context, name string, ext ExternalMetadataStoreSpec) error
	// cnpg→external orphan-adopt escape hatch:
	SetMetadataStoreRetainCnpgOnFlip(ctx context.Context, name string, retain bool) error
	GetMetadataStoreRetainCnpgOnFlip(ctx context.Context, name string) (retain, present bool, err error)
	CnpgSourceMRsOrphaned(ctx context.Context, name string) (orphaned, present bool, err error)
	SetMetadataStoreCnpgAdopt(ctx context.Context, name, shard string) error
}

// NewReshardRunner builds a runner over the real config store + duckling
// client. configPollInterval must match the CP fleet's snapshot poll so the
// post-block propagation wait is honest.
//
// Env-only knob DUCKGRES_RESHARD_FLIP_TIMEOUT (Go duration) overrides the
// DEFAULT cutover wait (15m) — how long a flip waits for the composition to
// converge before rolling back. Individual operations override it per-op via
// cutover_timeout_seconds (what the e2e's bogus-shard rollback uses to stay
// fast without shortening real cutovers).
// backuper is the pre-flip catalog backuper (nil-safe: a nil backuper skips the
// best-effort backup on non-destructive directions and HARD-fails the
// destructive cnpg→external direction, which must never flip without a backup).
func NewReshardRunner(store *configstore.ConfigStore, duckling *DucklingClient, cpID string, configPollInterval time.Duration, backuper CatalogBackuper) *ReshardRunner {
	flipTimeout := 15 * time.Minute
	if v := os.Getenv("DUCKGRES_RESHARD_FLIP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			flipTimeout = d
		} else {
			slog.Warn("Ignoring invalid DUCKGRES_RESHARD_FLIP_TIMEOUT.", "value", v, "error", err)
		}
	}
	return &ReshardRunner{
		store:              store,
		duckling:           duckling,
		copier:             PGCatalogCopier{},
		backuper:           backuper,
		cpID:               cpID,
		baseCtx:            context.Background(),
		pollInterval:       10 * time.Second,
		configPollInterval: configPollInterval,
		heartbeatInterval:  30 * time.Second,
		staleAfter:         5 * time.Minute,
		flipTimeout:        flipTimeout,
		hotIdleGrace:       30 * time.Second,
		loopPoll:           5 * time.Second,
	}
}

// CPID returns this runner's control-plane id — the value the admin handler
// passes to CreateReshardOperationClaimed so the op is created already owned by
// THIS replica (the one holding the ephemeral password).
func (r *ReshardRunner) CPID() string { return r.cpID }

// StashExternalPassword hands the runner an ephemeral password for an op it is
// expected to run. Used by AdoptClaimedOperation and by tests.
func (r *ReshardRunner) StashExternalPassword(opID int64, password string) {
	if password != "" {
		r.extPasswords.Store(opID, password)
	}
}

// AdoptClaimedOperation takes an op that was just created ALREADY claimed by
// this CP (CreateReshardOperationClaimed) and executes it directly, without
// waiting for the scanOnce poll tick. This is the second half of
// claim-on-create: the admin handler creates the op owned by this replica and
// hands it straight here so the replica that holds the ephemeral external
// password is the one that runs the copy. For external targets password is the
// ephemeral credential (stashed for copyCatalog); for cnpg targets it is "".
//
// Execution runs under the runner's LIFECYCLE context (r.baseCtx), never the
// caller's (HTTP request) context — a reshard runs for minutes. The r.running
// map dedups against a racing scanOnce pickup (which cannot happen anyway: a
// fresh-heartbeat running op owned by this CP is neither listed nor claimable
// elsewhere).
func (r *ReshardRunner) AdoptClaimedOperation(op *configstore.ReshardOperation, password string) {
	r.StashExternalPassword(op.ID, password)
	if _, alreadyRunning := r.running.LoadOrStore(op.ID, struct{}{}); alreadyRunning {
		return
	}
	ctx := r.baseCtx
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		defer r.running.Delete(op.ID)
		defer r.extPasswords.Delete(op.ID)
		r.execute(ctx, op)
	}()
}

// Run is the claim loop. Blocks until ctx is done.
func (r *ReshardRunner) Run(ctx context.Context) {
	r.baseCtx = ctx
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.scanOnce(ctx)
		}
	}
}

func (r *ReshardRunner) scanOnce(ctx context.Context) {
	ids, err := r.store.ListClaimableReshardOperations(r.staleAfter)
	if err != nil {
		slog.Warn("Reshard runner: listing claimable operations failed.", "error", err)
		return
	}
	for _, id := range ids {
		if _, alreadyRunning := r.running.Load(id); alreadyRunning {
			continue
		}
		op, err := r.store.ClaimReshardOperation(id, r.cpID, r.staleAfter)
		if err != nil {
			slog.Warn("Reshard runner: claim failed.", "op", id, "error", err)
			continue
		}
		if op == nil {
			continue // someone else won
		}
		r.running.Store(id, struct{}{})
		go func(op *configstore.ReshardOperation) {
			defer r.running.Delete(op.ID)
			defer r.extPasswords.Delete(op.ID)
			r.execute(ctx, op)
		}(op)
	}
}

// reshardAborted marks step errors that already carry operator context.
var errReshardCancelled = errors.New("cancel requested")

// opRun carries the per-operation execution state.
type opRun struct {
	r  *ReshardRunner
	op *configstore.ReshardOperation

	// progress flags for the rollback decision
	blocked          bool
	compactionPaused bool
	flipped          bool
	// retainRequested: we set retainCnpgOnFlip=true on the source (cnpg→ext
	// orphan step). If we roll back BEFORE the flip, reset it to false so the
	// still-cnpg source keeps full-lifecycle (Delete) MRs and deprovision stays
	// clean. (After the flip, recoverFromExternal's adopt patch clears it.)
	retainRequested bool

	// resolved source/target connection info (source recorded pre-flip)
	source CatalogEndpoint
	target CatalogEndpoint

	// snapshot row counts from the copy (source-stability recheck reference)
	copied CatalogCopyResult
}

func (o *opRun) logf(level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Info("Reshard: "+msg, "op", o.op.ID, "org", o.op.OrgID, "level", level)
	if err := o.r.store.AppendReshardLog(o.op.ID, level, msg); err != nil {
		slog.Warn("Reshard: appending log entry failed.", "op", o.op.ID, "error", err)
	}
}

// reshardStepOrderCnpg / reshardStepOrderExternal are the canonical execution
// orders of the op's step column, one per target direction. They exist so a
// runner that CLAIMS an op a prior epoch already advanced (takeover after a
// crash, or a switch to another replica) can reconstruct how far the work got —
// the in-process opRun progress flags all start false, but the persisted step
// records the prior runner's position.
var (
	reshardStepOrderCnpg     = []string{"blocking", "draining", "pausing_compaction", "backup_catalog", "cutover", "copying", "verifying", "cleaning_up", "finalizing"}
	reshardStepOrderExternal = []string{"blocking", "draining", "pausing_compaction", "backup_catalog", "copying", "verifying", "orphaning_source", "cutover", "verifying_external", "cleaning_up", "finalizing"}
)

// reshardStepReached reports whether the op's persisted step is at or past
// target in the op's direction order. An unknown/empty step (fresh op) → false.
func reshardStepReached(op *configstore.ReshardOperation, target string) bool {
	order := reshardStepOrderCnpg
	if op.TargetKind == configstore.MetadataStoreKindExternal {
		order = reshardStepOrderExternal
	}
	cur, tgt := -1, -1
	for i, s := range order {
		if s == op.Step {
			cur = i
		}
		if s == target {
			tgt = i
		}
	}
	return cur >= 0 && tgt >= 0 && cur >= tgt
}

// reconstructProgress re-derives the rollback progress flags from the persisted
// op row. On a fresh op it is a no-op (BlockedAt nil, step ""); on a TAKEOVER
// (crash of the owning runner, or a replica switch) it makes rollback behave as
// if this runner had performed the prior steps — otherwise the flags stay false
// and rollback silently skips unblocking the warehouse (leaving the org stuck in
// resharding) and restoring compaction. This is what let a cancel of an op whose
// owning replica had OOM-crashed mark the op cancelled while the warehouse
// stayed blocked.
//
// Steps are stamped at the START of a step; a progress flag is set at its
// SUCCESS. Reconstruction is deliberately conservative so a "started but not
// completed" step can never trigger a damaging rollback:
//   - blocked: keyed off the persisted blocked_at timestamp (written only after
//     the ready→resharding CAS succeeds) — exact, no step guessing.
//   - compactionPaused: requires the step to be STRICTLY PAST pausing_compaction
//     (i.e. backup_catalog reached), which proves pauseCompaction recorded the
//     prior setting; restoring to an unrecorded (zero) prior could wrongly
//     re-enable compaction (key-absent enables it), so we would rather leave it
//     paused than guess.
//   - flipped / retainRequested: keyed off the step reaching cutover /
//     orphaning_source. Over-marking these is safe: every post-flip rollback
//     patch (re-point shard, flip back, adopt) is idempotent and no-ops when the
//     store never actually moved.
func (o *opRun) reconstructProgress() {
	// blocked_at is written just after the ready→resharding CAS; reaching a step
	// past "blocking" also proves the CAS succeeded (run() aborts on a block
	// error), covering the sliver between the CAS and the blocked_at write.
	if o.op.BlockedAt != nil || reshardStepReached(o.op, "draining") {
		o.blocked = true
	}
	if reshardStepReached(o.op, "backup_catalog") {
		o.compactionPaused = true
	}
	if o.op.TargetKind == configstore.MetadataStoreKindExternal && reshardStepReached(o.op, "orphaning_source") {
		o.retainRequested = true
	}
	if reshardStepReached(o.op, "cutover") {
		o.flipped = true
	}
}

// flipTimeout is the per-op cutover bound, falling back to the runner
// default (15m / DUCKGRES_RESHARD_FLIP_TIMEOUT).
func (o *opRun) flipTimeout() time.Duration {
	if o.op.CutoverTimeoutSeconds > 0 {
		return time.Duration(o.op.CutoverTimeoutSeconds) * time.Second
	}
	return o.r.flipTimeout
}

func (o *opRun) step(step string) error {
	if err := o.r.store.UpdateReshardStep(o.op.ID, o.r.cpID, o.op.RunnerEpoch, step); err != nil {
		return err
	}
	o.op.Step = step
	return nil
}

func (o *opRun) fields(updates map[string]interface{}) error {
	return o.r.store.UpdateReshardFields(o.op.ID, o.r.cpID, o.op.RunnerEpoch, updates)
}

// cancelRequested refetches the op row and reports the cancel flag; a fencing
// loss surfaces as an error from the next fenced write, not here.
func (o *opRun) cancelRequested() bool {
	fresh, err := o.r.store.GetReshardOperation(o.op.ID)
	if err != nil {
		return false
	}
	return fresh.CancelRequested
}

// wait sleeps in short slices until d elapsed, ctx is done, or cancel was
// requested (returns errReshardCancelled).
func (o *opRun) wait(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.cancelRequested() {
			return errReshardCancelled
		}
		slice := time.Until(deadline)
		if slice > o.r.loopPoll {
			slice = o.r.loopPoll
		}
		time.Sleep(slice)
	}
	return nil
}

func (r *ReshardRunner) execute(ctx context.Context, op *configstore.ReshardOperation) {
	o := &opRun{r: r, op: op}
	// Recover progress flags from the persisted row so a takeover/replica-switch
	// rolls back (unblocks the warehouse, restores compaction) as if this runner
	// had done the prior steps. No-op for a fresh op.
	o.reconstructProgress()
	o.logf("info", "operation claimed by %s (epoch %d): %s → %s", r.cpID, op.RunnerEpoch, describeSource(op), describeTarget(op))

	// Heartbeat until the op function returns. A fencing loss stops the op.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	fenced := make(chan struct{})
	go func() {
		ticker := time.NewTicker(r.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := r.store.TouchReshardHeartbeat(op.ID, r.cpID, op.RunnerEpoch); err != nil {
					slog.Error("Reshard: heartbeat fenced — another runner took over; abandoning.", "op", op.ID, "error", err)
					close(fenced)
					return
				}
			}
		}
	}()

	runErr := o.run(ctx, fenced)
	if runErr == nil {
		return
	}
	if errors.Is(runErr, configstore.ErrReshardFenced) {
		o.logf("error", "operation fenced: another runner took over; this runner abandons it")
		return
	}

	cancelled := errors.Is(runErr, errReshardCancelled)
	if cancelled {
		o.logf("warn", "cancel requested — rolling back from step %q", o.op.Step)
	} else {
		o.logf("error", "step %q failed: %v — rolling back", o.op.Step, runErr)
	}
	o.rollback(ctx)

	state := configstore.ReshardStateFailed
	msg := runErr.Error()
	if cancelled {
		state = configstore.ReshardStateCancelled
		msg = "cancelled by operator"
	}
	o.report(state)
	if err := r.store.FinishReshardOperation(op.ID, r.cpID, op.RunnerEpoch, state, msg); err != nil {
		slog.Error("Reshard: finishing operation failed.", "op", op.ID, "error", err)
	}
}

func describeSource(op *configstore.ReshardOperation) string {
	if op.SourceKind == configstore.MetadataStoreKindCnpgShard {
		return "cnpg " + op.FromShard
	}
	return "external " + op.SourceEndpoint
}

func describeTarget(op *configstore.ReshardOperation) string {
	if op.TargetKind == configstore.MetadataStoreKindCnpgShard {
		return "cnpg " + op.ToShard
	}
	return "external " + op.TargetEndpoint
}

// run drives the step sequence; any returned error triggers rollback.
func (o *opRun) run(ctx context.Context, fenced <-chan struct{}) error {
	select {
	case <-fenced:
		return configstore.ErrReshardFenced
	default:
	}
	if o.cancelRequested() {
		return errReshardCancelled
	}

	if err := o.block(ctx); err != nil {
		return err
	}
	if err := o.drain(ctx); err != nil {
		return err
	}
	if err := o.pauseCompaction(ctx); err != nil {
		return err
	}
	if err := o.recordSource(ctx); err != nil {
		return err
	}
	if err := o.backupCatalog(ctx); err != nil {
		return err
	}

	toExternal := o.op.TargetKind == configstore.MetadataStoreKindExternal
	cnpgSource := o.op.SourceKind == configstore.MetadataStoreKindCnpgShard
	if toExternal {
		// Escape hatch (orphan-adopt then verified-delete): copy BEFORE flip,
		// orphan the cnpg source so the flip does NOT delete it, flip, verify
		// the external target caught the full catalog, and only THEN drop the
		// orphaned cnpg source. Any failure before the drop flips back and
		// re-adopts the still-present role/DB.
		if err := o.copyCatalog(ctx); err != nil {
			return err
		}
		if err := o.verifySourceStable(ctx); err != nil {
			return err
		}
		if cnpgSource {
			if err := o.orphanCnpgSource(ctx); err != nil {
				return err
			}
		}
		if err := o.flipToExternal(ctx); err != nil {
			return err
		}
		if err := o.verifyExternalCatalog(ctx); err != nil {
			return err
		}
		if cnpgSource {
			if err := o.dropOrphanedCnpgSource(ctx); err != nil {
				return err
			}
		}
	} else {
		if err := o.flipToCnpg(ctx); err != nil {
			return err
		}
		if err := o.copyCatalog(ctx); err != nil {
			return err
		}
		if err := o.verifySourceStable(ctx); err != nil {
			return err
		}
		if err := o.cleanupSource(ctx); err != nil {
			return err
		}
	}

	return o.finalize(ctx)
}

// ---- steps ----------------------------------------------------------------

func (o *opRun) block(ctx context.Context) error {
	if err := o.step("blocking"); err != nil {
		return err
	}
	o.logf("info", "blocking new connections: warehouse ready → resharding (advisory-locked CAS; the lease-grant path refuses resharding orgs)")
	if err := o.r.store.SetWarehouseResharding(o.op.OrgID); err != nil {
		if errors.Is(err, configstore.ErrWarehouseStateMismatch) {
			// Takeover/restart: already resharding is fine.
			if o.warehouseResharding() {
				o.logf("info", "warehouse already in resharding state (takeover or restart) — continuing")
				o.blocked = true
			} else {
				return fmt.Errorf("warehouse is not ready (another lifecycle operation in flight?): %w", err)
			}
		} else {
			return err
		}
	} else {
		o.blocked = true
	}
	now := time.Now().UTC()
	if o.op.BlockedAt == nil {
		if err := o.fields(map[string]interface{}{"blocked_at": now}); err != nil {
			return err
		}
		o.op.BlockedAt = &now
	}

	wait := 2 * o.r.configPollInterval
	if wait <= 0 {
		wait = 10 * time.Second
	}
	o.logf("info", "waiting %s for every control-plane replica to observe the resharding state (config snapshot poll)", wait)
	return o.wait(ctx, wait)
}

func (o *opRun) warehouseResharding() bool {
	// The runner has no snapshot; infer from the CAS result by attempting a
	// no-op fenced-free check: UpdateWarehouseState with expected resharding
	// and an empty-but-valid update would be abusive, so read via drain state
	// instead: a mismatch error above plus a subsequent successful resharding
	// CAS from the same state is equivalent. Keep it simple: try the CAS from
	// resharding to resharding.
	err := o.r.store.UpdateWarehouseState(o.op.OrgID, configstore.ManagedWarehouseStateResharding, map[string]interface{}{
		"state": configstore.ManagedWarehouseStateResharding,
	})
	return err == nil
}

func (o *opRun) drain(ctx context.Context) error {
	if err := o.step("draining"); err != nil {
		return err
	}
	o.logf("info", "waiting for all connections to drain…")

	timeout := time.Duration(o.op.DrainTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	var hotIdleRetireAfter time.Time
	lastLog := time.Time{}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.cancelRequested() {
			return errReshardCancelled
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("drain timed out after %s (connections or workers still active)", timeout)
		}

		conns, err := o.r.store.OrgConnectionDrainState(o.op.OrgID)
		if err != nil {
			return err
		}
		workers, err := o.r.store.ListWorkerRecordsForOrg(o.op.OrgID)
		if err != nil {
			return err
		}

		if conns.Drained() && len(workers) == 0 {
			o.logf("info", "drained: no active sessions, no queued connections, no live workers")
			return nil
		}

		if time.Since(lastLog) >= 15*time.Second {
			o.logf("info", "waiting for connections to drain: %d active sessions, %d queued connections, %d live workers",
				conns.ActiveLeases, conns.QueuedConns, len(workers))
			lastLog = time.Now()
		}

		// Sessions gone but workers linger: give hot-idle workers a grace to
		// expire naturally, then retire them through the standard CAS path
		// (the same transition the janitor's TTL reap uses — never raw pod
		// deletes; the janitor reconciler removes the pods of retired rows).
		// A live worker matters because its DuckLakeCheckpointer writes the
		// catalog independent of sessions.
		if conns.Drained() {
			if hotIdleRetireAfter.IsZero() {
				hotIdleRetireAfter = time.Now().Add(o.r.hotIdleGrace)
			}
			if time.Now().After(hotIdleRetireAfter) {
				for i := range workers {
					w := &workers[i]
					if w.State != configstore.WorkerStateHotIdle {
						continue
					}
					retired, err := o.r.store.RetireHotIdleWorker(w)
					if err != nil {
						o.logf("warn", "retiring hot-idle worker %d failed: %v", w.WorkerID, err)
						continue
					}
					if retired {
						o.logf("info", "retired lingering hot-idle worker %d (pod %s)", w.WorkerID, w.PodName)
					}
				}
			}
		} else {
			hotIdleRetireAfter = time.Time{}
		}

		time.Sleep(o.r.loopPoll)
	}
}

func (o *opRun) pauseCompaction(ctx context.Context) error {
	if err := o.step("pausing_compaction"); err != nil {
		return err
	}
	enabled, present, err := o.r.duckling.GetCompactionSetting(ctx, o.op.DucklingName)
	if err != nil {
		return fmt.Errorf("read compaction setting: %w", err)
	}
	if err := o.fields(map[string]interface{}{
		"compaction_was_present": present,
		"compaction_was_enabled": enabled,
	}); err != nil {
		return err
	}
	off := false
	if err := o.r.duckling.SetCompactionEnabled(ctx, o.op.DucklingName, &off); err != nil {
		return fmt.Errorf("pause compaction: %w", err)
	}
	o.compactionPaused = true
	o.op.CompactionWasPresent = present
	o.op.CompactionWasEnabled = enabled
	o.logf("info", "compaction paused on the duckling spec (was: present=%t enabled=%t). Note: an already-running compaction job (≤30m) can outlive this; the post-copy stability check catches its writes", present, enabled)
	return nil
}

// recordSource captures the FULL source connection info from the live CR
// status BEFORE any flip (the flip changes the status; for external sources
// it also deletes the ESO password sync and per-duckling pgbouncer).
func (o *opRun) recordSource(ctx context.Context) error {
	st, err := o.r.duckling.Get(ctx, o.op.DucklingName)
	if err != nil {
		return fmt.Errorf("read duckling status: %w", err)
	}
	ms := st.MetadataStore
	if ms.Endpoint == "" || ms.User == "" || ms.Database == "" {
		return fmt.Errorf("duckling status has incomplete metadata-store info (endpoint %q user %q database %q)", ms.Endpoint, ms.User, ms.Database)
	}

	switch o.op.SourceKind {
	case configstore.MetadataStoreKindCnpgShard:
		// Post-flip access goes DIRECT to the source pooler endpoint (the
		// orphaned role/DB and its pinned password survive a shard change).
		o.source = CatalogEndpoint{
			Host: ms.Endpoint, Port: 5432,
			User: ms.User, Password: ms.Password, Database: ms.Database,
			SSLMode: "disable",
		}
	case configstore.MetadataStoreKindExternal:
		// Post-flip the composition deletes the ESO sync + pgbouncer, so the
		// copy connects DIRECT to the RDS endpoint with TLS. The password
		// from the CR status stays valid (RDS itself is never modified) but
		// exists only in this runner's memory once the flip lands.
		o.source = CatalogEndpoint{
			Host: ms.Endpoint, Port: 5432,
			User: ms.User, Password: ms.Password, Database: ms.Database,
			SSLMode: "require",
		}
		o.r.extPasswords.Store(o.op.ID, ms.Password)
	default:
		return fmt.Errorf("unsupported source kind %q", o.op.SourceKind)
	}

	if err := o.fields(map[string]interface{}{
		"source_endpoint": ms.Endpoint,
		"source_user":     ms.User,
		"source_database": ms.Database,
	}); err != nil {
		return err
	}
	// Mirror the persisted fields onto the in-memory op so every later reader
	// (cleanup logging, the report, the ext→cnpg rollback) sees them too. For
	// cnpg sources the admin handler leaves these empty at create time.
	o.op.SourceEndpoint = ms.Endpoint
	o.op.SourceUser = ms.User
	o.op.SourceDatabase = ms.Database
	o.logf("info", "recorded source metadata store: %s", o.source.Redacted())
	return nil
}

// isDestructiveDirection reports whether this op's flip is destructive at the
// source — the cnpg→external escape hatch, where the type flip makes Crossplane
// DELETE the cnpg source role/DB. For that direction the pre-flip backup is a
// HARD prerequisite; every other direction leaves the source intact/orphaned,
// so a failed backup is only belt-and-suspenders.
func (o *opRun) isDestructiveDirection() bool {
	return o.op.SourceKind == configstore.MetadataStoreKindCnpgShard &&
		o.op.TargetKind == configstore.MetadataStoreKindExternal
}

// backupCatalog dumps the SOURCE catalog to the org's own S3 data bucket under
// _reshard_catalog_backups/ BEFORE any flip, so a catalog lost at the cutover
// is a one-command pg_restore away. Gate strength by direction: for the
// destructive cnpg→external escape hatch a backup failure FAILS the op before
// the flip (never destroy the source without a durable snapshot); for
// non-destructive directions (the source survives) a failure is logged and the
// op continues. Runs after drain + pause-compaction (source is quiescent) and
// after recordSource (o.source is resolved, including an external source's
// password).
func (o *opRun) backupCatalog(ctx context.Context) error {
	if err := o.step("backup_catalog"); err != nil {
		return err
	}
	destructive := o.isDestructiveDirection()

	if o.r.backuper == nil {
		return o.backupFailed(destructive, fmt.Errorf("no catalog backuper configured (STS/S3 unavailable)"))
	}

	// The org's data bucket + region + IAM role come from the live CR status.
	st, err := o.r.duckling.Get(ctx, o.op.DucklingName)
	if err != nil {
		return o.backupFailed(destructive, fmt.Errorf("read duckling status for backup: %w", err))
	}
	bucket := st.DataStore.BucketName
	if bucket == "" || st.IAMRoleARN == "" {
		return o.backupFailed(destructive, fmt.Errorf("duckling status missing data bucket (%q) or IAM role (%q) — cannot back up catalog", bucket, st.IAMRoleARN))
	}

	ts := time.Now().UTC().Format("20060102T150405Z")
	key := fmt.Sprintf("%sop-%d-%s.dump", backupPrefix, o.op.ID, ts)
	dest := BackupDestination{Bucket: bucket, Key: key, Region: st.DataStore.S3Region, RoleARN: st.IAMRoleARN}

	o.logf("info", "backing up source catalog %s → s3://%s/%s before the flip%s",
		o.source.Redacted(), bucket, key,
		map[bool]string{true: " (HARD prerequisite: cnpg→external destroys the source at the flip)", false: ""}[destructive])
	uri, size, err := o.r.backuper.Backup(ctx, o.source, dest)
	if err != nil {
		return o.backupFailed(destructive, fmt.Errorf("catalog backup: %w", err))
	}
	if err := o.fields(map[string]interface{}{"backup_s3_uri": uri}); err != nil {
		return err
	}
	o.op.BackupS3URI = uri
	o.logf("info", "catalog backup complete: %s (%d bytes)", uri, size)
	o.logf("info", "recover this catalog with: %s", restoreCommand(uri, o.source))
	return nil
}

// backupFailed applies the direction gate to a backup failure: a hard error for
// the destructive cnpg→external direction (fails the op before the flip), a
// warning-and-continue for every other direction.
func (o *opRun) backupFailed(destructive bool, err error) error {
	if destructive {
		return fmt.Errorf("pre-flip catalog backup is mandatory for the destructive cnpg→external direction: %w", err)
	}
	o.logf("warn", "pre-flip catalog backup failed: %v — continuing (this direction leaves the source intact, so the backup is belt-and-suspenders, not a prerequisite)", err)
	return nil
}

// restoreCommand renders the exact operator-runnable recovery command: download
// the artifact from S3 and pg_restore it into a target catalog DB. The target
// host/user/db name the recorded source (not secrets); the password is a
// <PASSWORD> placeholder so this is safe to log. Point --dbname at whichever
// catalog DB needs the data (usually the org's CURRENT catalog after a bad
// flip).
func restoreCommand(uri string, target CatalogEndpoint) string {
	port := target.Port
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf(
		"aws s3 cp %s ./catalog.dump && PGPASSWORD=<PASSWORD> pg_restore --no-owner --clean --if-exists --format=custom "+
			"--host=%s --port=%d --username=%s --dbname=%s ./catalog.dump",
		uri, target.Host, port, target.User, target.Database)
}

// flipToCnpg patches the duckling to the target cnpg shard and waits for the
// composition to converge + the target DB to answer with the tenant creds.
func (o *opRun) flipToCnpg(ctx context.Context) error {
	if err := o.step("cutover"); err != nil {
		return err
	}
	o.logf("info", "flipping duckling metadata store to cnpg shard %s (in-place re-point; source role/DB stays until cleanup)", o.op.ToShard)
	if err := o.r.duckling.SetMetadataStoreCnpg(ctx, o.op.DucklingName, o.op.ToShard); err != nil {
		return err
	}
	o.flipped = true

	targetPrefix := o.op.ToShard + "-pooler."
	deadline := time.Now().Add(o.flipTimeout())
	lastLog := time.Time{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.cancelRequested() {
			return errReshardCancelled
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("target shard %s did not become ready within %s", o.op.ToShard, o.flipTimeout())
		}

		st, err := o.r.duckling.Get(ctx, o.op.DucklingName)
		if err == nil && strings.HasPrefix(st.MetadataStore.Endpoint, targetPrefix) && st.ReadyCondition {
			o.target = CatalogEndpoint{
				Host: st.MetadataStore.Endpoint, Port: 5432,
				User: st.MetadataStore.User, Password: st.MetadataStore.Password, Database: st.MetadataStore.Database,
				SSLMode: "disable",
			}
			if probeErr := o.r.copier.Probe(ctx, o.target); probeErr == nil {
				o.logf("info", "target ready: duckling converged on %s and the tenant role answers", st.MetadataStore.Endpoint)
				return nil
			} else if time.Since(lastLog) >= 15*time.Second {
				o.logf("info", "waiting for target: composition converged, probe still failing: %v", probeErr)
				lastLog = time.Now()
			}
		} else if time.Since(lastLog) >= 15*time.Second {
			switch {
			case err != nil:
				o.logf("info", "waiting for target: reading duckling failed: %v", err)
			case st.SyncedFalseMessage != "":
				o.logf("info", "waiting for target: duckling Synced=False: %s", st.SyncedFalseMessage)
			default:
				o.logf("info", "waiting for target: duckling endpoint %q, ready=%t", st.MetadataStore.Endpoint, st.ReadyCondition)
			}
			lastLog = time.Now()
		}
		time.Sleep(o.r.loopPoll)
	}
}

// orphanCnpgSource makes the cnpg source role/DB survive the upcoming type
// flip: it sets spec.metadataStore.retainCnpgOnFlip=true (composition renders
// the cnpg Role/Database MRs WITHOUT Delete) and waits until BOTH MRs reflect
// that no-Delete policy before returning — so the subsequent flip orphans them
// instead of deleting them. Two guards make this safe:
//   - XRD-compat: after the patch we read the flag back; if it did not stick
//     (present=false / retain=false) the cluster's Duckling XRD predates the
//     field, so we REFUSE the flip — the destructive delete-on-flip with no
//     orphan-adopt safety net is exactly what this whole change avoids.
//   - Race close: we poll the actual Role/Database MRs' managementPolicies and
//     only proceed once both are no-Delete, so the flip can never un-render the
//     MRs before the retain policy propagated.
func (o *opRun) orphanCnpgSource(ctx context.Context) error {
	if err := o.step("orphaning_source"); err != nil {
		return err
	}
	o.logf("info", "retaining the cnpg source role/DB across the flip (retainCnpgOnFlip=true) so the type flip ORPHANS them instead of deleting them")
	if err := o.r.duckling.SetMetadataStoreRetainCnpgOnFlip(ctx, o.op.DucklingName, true); err != nil {
		return fmt.Errorf("set retainCnpgOnFlip: %w", err)
	}
	o.retainRequested = true

	// XRD-compat read-back: a cluster whose Duckling XRD predates the field
	// silently prunes the patch. Refuse rather than risk delete-on-flip.
	retain, present, err := o.r.duckling.GetMetadataStoreRetainCnpgOnFlip(ctx, o.op.DucklingName)
	if err != nil {
		return fmt.Errorf("read back retainCnpgOnFlip: %w", err)
	}
	if !present || !retain {
		return fmt.Errorf("the cluster's Duckling XRD does not support spec.metadataStore.retainCnpgOnFlip (patch pruned) — deploy the crossplane-config charts carrying that field BEFORE running a cnpg→external reshard; refusing to flip because the type change would DELETE the cnpg source catalog with no orphan-adopt safety net")
	}

	// Wait until the cnpg Role+Database MRs reflect the no-Delete policy, so the
	// type flip orphans (not deletes) them. Closes the race where the flip
	// un-renders the MRs before the retain policy propagated.
	deadline := time.Now().Add(o.flipTimeout())
	lastLog := time.Time{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.cancelRequested() {
			return errReshardCancelled
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cnpg source role/DB did not reflect the retain (no-Delete) policy within %s — refusing to flip (the composition may not honor retainCnpgOnFlip; charts/composition skew?)", o.flipTimeout())
		}
		orphaned, mrsPresent, err := o.r.duckling.CnpgSourceMRsOrphaned(ctx, o.op.DucklingName)
		if err == nil && mrsPresent && orphaned {
			o.logf("info", "cnpg source role/DB now carry the retain (no-Delete) policy — safe to flip")
			return nil
		}
		if time.Since(lastLog) >= 15*time.Second {
			switch {
			case err != nil:
				o.logf("info", "waiting for the cnpg source MRs to reflect the retain policy: read failed: %v", err)
			case !mrsPresent:
				o.logf("info", "waiting for the cnpg source MRs to reflect the retain policy: Role/Database MRs not both observed yet")
			default:
				o.logf("info", "waiting for the cnpg source MRs to reflect the retain (no-Delete) policy")
			}
			lastLog = time.Now()
		}
		time.Sleep(o.r.loopPoll)
	}
}

// flipToExternal is the cnpg→ext cutover: runs only AFTER copy+verify and the
// orphan step (retainCnpgOnFlip=true), so the type flip ORPHANS the cnpg source
// role/DB rather than deleting them. The explicit drop happens later, only once
// the external catalog is verified complete.
func (o *opRun) flipToExternal(ctx context.Context) error {
	if err := o.step("cutover"); err != nil {
		return err
	}
	o.logf("info", "flipping duckling metadata store to external %s — the cnpg source role/DB (%s) are RETAINED (orphaned, not deleted) so this flip is recoverable", o.op.TargetEndpoint, o.op.SourceDatabase)
	ext := ExternalMetadataStoreSpec{
		Endpoint:          o.op.TargetEndpoint,
		PasswordAWSSecret: o.op.TargetPasswordSecret,
		User:              o.op.TargetUser,
		Database:          o.op.TargetDatabase,
	}
	if err := o.r.duckling.SetMetadataStoreExternal(ctx, o.op.DucklingName, ext); err != nil {
		return err
	}
	o.flipped = true

	providedPassword := o.target.Password
	deadline := time.Now().Add(o.flipTimeout())
	lastLog := time.Time{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// No cancel check: past this flip the only ways out are forward or
		// the orphan-adopt recovery in rollback().
		if time.Now().After(deadline) {
			return fmt.Errorf("external target did not become ready within %s (ESO secret %q missing, unreadable, or wrong? the ESO role can only read allowed secret NAME patterns — e.g. duckling-*/posthog-* — and the value must be the raw password string)", o.flipTimeout(), o.op.TargetPasswordSecret)
		}

		st, err := o.r.duckling.Get(ctx, o.op.DucklingName)
		switch {
		case err != nil:
			// transient — keep polling
		case st.MetadataStore.Type == configstore.MetadataStoreKindExternal && st.MetadataStore.Password != "":
			if st.MetadataStore.Password != providedPassword {
				return fmt.Errorf("ESO-synced password from secret %q does not match the password the catalog was copied with — fix the secret and re-run", o.op.TargetPasswordSecret)
			}
			if st.ReadyCondition {
				o.logf("info", "external target ready: ESO password synced and matches, duckling Ready")
				return nil
			}
		}
		if time.Since(lastLog) >= 15*time.Second {
			msg := "waiting for external target (ESO password sync + Ready condition)"
			if err == nil && st.SyncedFalseMessage != "" {
				msg += ": " + st.SyncedFalseMessage
			}
			o.logf("info", "%s", msg)
			lastLog = time.Now()
		}
		time.Sleep(o.r.loopPoll)
	}
}

// verifyExternalCatalog is the cnpg→ext gate BEFORE dropping the orphaned cnpg
// source: it re-reads the live external target's per-table ducklake_* row
// counts and requires an EXACT match against the copy snapshot
// (o.copied.PerTableRows). An incomplete/diverged target fails here — the cnpg
// source is still present (orphaned), so rollback flips back and re-adopts it
// with no data loss. Only a clean match lets dropOrphanedCnpgSource run.
func (o *opRun) verifyExternalCatalog(ctx context.Context) error {
	if err := o.step("verifying_external"); err != nil {
		return err
	}
	current, err := o.r.copier.SnapshotCounts(ctx, o.target)
	if err != nil {
		return fmt.Errorf("re-reading external target counts: %w", err)
	}
	if len(current) != len(o.copied.PerTableRows) {
		return fmt.Errorf("external target table set differs from the copy (%d tables now, %d copied) — catalog incomplete; NOT dropping the retained cnpg source", len(current), len(o.copied.PerTableRows))
	}
	for table, want := range o.copied.PerTableRows {
		got, ok := current[table]
		if !ok {
			return fmt.Errorf("external target is missing table %s — catalog incomplete; NOT dropping the retained cnpg source", table)
		}
		if got != want {
			return fmt.Errorf("external target table %s row count %d != copied %d — catalog incomplete; NOT dropping the retained cnpg source", table, got, want)
		}
	}
	o.logf("info", "verified external target catalog: %d tables match the copied row counts exactly — safe to drop the retained cnpg source", len(current))
	return nil
}

// dropOrphanedCnpgSource drops the retained (orphaned) cnpg source database —
// cnpg→ext only, and only after verifyExternalCatalog passed. It mirrors
// cleanupSource: DROP DATABASE … WITH (FORCE) via the source pooler, best-effort
// (a leftover DB is cruft, not data loss, since the external target is verified
// live); the source role is left (orphaned, harmless — roles cannot drop
// themselves). It then clears retainCnpgOnFlip: harmless on the now-external
// tenant, but it keeps a later ext→cnpg reshard from inheriting a no-Delete cnpg
// MR that would leak on deprovision.
func (o *opRun) dropOrphanedCnpgSource(ctx context.Context) error {
	if err := o.step("cleaning_up"); err != nil {
		return err
	}
	if o.source.Database == "" {
		o.logf("error", "source database name is empty (recordSource did not run?) — skipping DROP DATABASE; the orphaned cnpg database on %s must be dropped manually", o.op.FromShard)
	} else {
		o.logf("info", "dropping the orphaned cnpg source database %s on %s (WITH FORCE) — external catalog verified complete", o.source.Database, o.op.FromShard)
		if err := o.r.copier.DropDatabase(ctx, o.source, o.source.Database); err != nil {
			o.logf("error", "DROP DATABASE on the orphaned cnpg source failed — it still exists on %s and must be dropped manually: %v", o.op.FromShard, err)
		} else {
			o.logf("info", "orphaned cnpg source database dropped (the source role remains — orphaned, harmless; roles cannot drop themselves)")
		}
	}
	if err := o.r.duckling.SetMetadataStoreRetainCnpgOnFlip(ctx, o.op.DucklingName, false); err != nil {
		o.logf("warn", "clearing retainCnpgOnFlip after cnpg→ext completion failed: %v — harmless while on external; a later ext→cnpg reshard should ensure it is false", err)
	}
	return nil
}

// copyCatalog resolves the target endpoint (for →cnpg it was resolved by the
// flip wait; for →ext it is built from the request + ephemeral password) and
// streams the catalog.
func (o *opRun) copyCatalog(ctx context.Context) error {
	if err := o.step("copying"); err != nil {
		return err
	}

	if o.op.TargetKind == configstore.MetadataStoreKindExternal {
		pw, ok := o.r.extPasswords.Load(o.op.ID)
		if !ok {
			return fmt.Errorf("external target password is not available to this runner (takeover after a crash?) — cancel and re-run the operation")
		}
		o.target = CatalogEndpoint{
			Host: o.op.TargetEndpoint, Port: 5432,
			User:     orDefault(o.op.TargetUser, "postgres"),
			Password: pw.(string),
			Database: orDefault(o.op.TargetDatabase, "postgres"),
			SSLMode:  "require",
		}
	}
	if o.op.SourceKind == configstore.MetadataStoreKindExternal && o.source.Password == "" {
		pw, ok := o.r.extPasswords.Load(o.op.ID)
		if !ok {
			return fmt.Errorf("external source password is not available to this runner (takeover after a crash?) — cancel and re-run the operation")
		}
		o.source.Password = pw.(string)
	}

	o.logf("info", "pre-flight probing source %s and target %s", o.source.Redacted(), o.target.Redacted())
	if err := o.r.copier.Probe(ctx, o.source); err != nil {
		return fmt.Errorf("source probe: %w", err)
	}
	if err := o.r.copier.Probe(ctx, o.target); err != nil {
		return fmt.Errorf("target probe: %w", err)
	}

	o.logf("info", "copying catalog tables…")
	result, err := o.r.copier.Copy(ctx, o.source, o.target, func(level, msg string) { o.logf(level, "%s", msg) })
	if err != nil {
		return fmt.Errorf("catalog copy: %w", err)
	}
	o.copied = result
	if err := o.fields(map[string]interface{}{
		"tables_copied": result.Tables,
		"rows_copied":   result.Rows,
		"bytes_copied":  result.Bytes,
	}); err != nil {
		return err
	}
	o.logf("info", "copy complete: %d tables, %d rows, %d bytes", result.Tables, result.Rows, result.Bytes)
	return nil
}

// verifySourceStable re-reads the source row counts OUTSIDE the snapshot
// transaction: any difference means something wrote the source after the
// snapshot (a straggler compaction job, a stray client) and the copy cannot
// be trusted.
func (o *opRun) verifySourceStable(ctx context.Context) error {
	if err := o.step("verifying"); err != nil {
		return err
	}
	current, err := o.r.copier.SnapshotCounts(ctx, o.source)
	if err != nil {
		return fmt.Errorf("re-reading source counts: %w", err)
	}
	if len(current) != len(o.copied.PerTableRows) {
		return fmt.Errorf("source table set changed during the copy (%d tables now, %d in snapshot) — a concurrent writer is active", len(current), len(o.copied.PerTableRows))
	}
	for table, snapCount := range o.copied.PerTableRows {
		nowCount, ok := current[table]
		if !ok {
			return fmt.Errorf("source table %s disappeared during the copy — a concurrent writer is active", table)
		}
		if nowCount != snapCount {
			return fmt.Errorf("source table %s changed during the copy (%d rows now, %d in snapshot) — a concurrent writer is active", table, nowCount, snapCount)
		}
	}
	o.logf("info", "verified: source is unchanged since the snapshot (%d tables) — no concurrent writer", len(current))
	return nil
}

// cleanupSource drops the source database — cnpg sources only, and only after
// verify. This is also the fence: any straggler writer against the old
// catalog now fails loudly instead of silently diverging.
func (o *opRun) cleanupSource(ctx context.Context) error {
	if err := o.step("cleaning_up"); err != nil {
		return err
	}
	if o.op.SourceKind != configstore.MetadataStoreKindCnpgShard {
		o.logf("info", "external source left untouched (never deleted)")
		return nil
	}
	// o.source is the copy-path source of truth (recorded from the duckling
	// status pre-flip); o.op.SourceDatabase mirrors it since recordSource.
	// Never issue a zero-length identifier: skip loudly instead.
	if o.source.Database == "" {
		o.logf("error", "source database name is empty (recordSource did not run?) — skipping DROP DATABASE; the old catalog database on %s must be dropped manually", o.op.FromShard)
		return nil
	}
	o.logf("info", "dropping source database %s on %s (WITH FORCE)", o.source.Database, o.op.FromShard)
	if err := o.r.copier.DropDatabase(ctx, o.source, o.source.Database); err != nil {
		// Loud but non-fatal: the copy is verified and live; a leftover
		// source DB is cruft plus a weaker fence, not data loss.
		o.logf("error", "DROP DATABASE on the source failed — old catalog database still exists on %s and must be dropped manually: %v", o.op.FromShard, err)
		return nil
	}
	o.logf("info", "source database dropped (the source role remains; roles cannot drop themselves)")
	return nil
}

func (o *opRun) finalize(ctx context.Context) error {
	if err := o.step("finalizing"); err != nil {
		return err
	}
	o.restoreCompaction(ctx)

	// Reconcile the warehouse config-store row with the new reality so
	// provisioning/status surfaces match the CR.
	updates := map[string]interface{}{
		"state":          configstore.ManagedWarehouseStateReady,
		"status_message": "metadata-store reshard complete",
	}
	if o.op.TargetKind == configstore.MetadataStoreKindCnpgShard && o.op.SourceKind == configstore.MetadataStoreKindExternal {
		updates["metadata_store_kind"] = configstore.MetadataStoreKindCnpgShard
		updates["metadata_store_endpoint"] = ""
		updates["metadata_store_port"] = 0
		updates["metadata_store_database_name"] = ""
		updates["metadata_store_username"] = ""
		updates["metadata_store_password_aws_secret"] = ""
		updates["pgbouncer_enabled"] = false
	}
	if o.op.TargetKind == configstore.MetadataStoreKindExternal {
		updates["metadata_store_kind"] = configstore.MetadataStoreKindExternal
		updates["metadata_store_endpoint"] = o.op.TargetEndpoint
		updates["metadata_store_port"] = 5432
		updates["metadata_store_database_name"] = o.op.TargetDatabase
		updates["metadata_store_username"] = o.op.TargetUser
		updates["metadata_store_password_aws_secret"] = o.op.TargetPasswordSecret
		// The XRD defaults stand up a per-duckling pgbouncer for external.
		updates["pgbouncer_enabled"] = true
	}
	if err := o.r.store.UpdateWarehouseState(o.op.OrgID, configstore.ManagedWarehouseStateResharding, updates); err != nil {
		return fmt.Errorf("unblock warehouse: %w", err)
	}
	now := time.Now().UTC()
	if err := o.fields(map[string]interface{}{"unblocked_at": now}); err != nil {
		return err
	}
	o.op.UnblockedAt = &now
	o.logf("info", "warehouse unblocked: resharding → ready; new sessions cold-spawn against the new metadata store")

	o.report(configstore.ReshardStateSucceeded)
	if err := o.r.store.FinishReshardOperation(o.op.ID, o.r.cpID, o.op.RunnerEpoch, configstore.ReshardStateSucceeded, ""); err != nil {
		return err
	}
	return nil
}

// restoreCompaction puts spec.ducklake.maintenance.compaction.enabled back to
// exactly its pre-reshard state (explicit value, or key-absent).
func (o *opRun) restoreCompaction(ctx context.Context) {
	if !o.compactionPaused {
		return
	}
	var restore *bool
	if o.op.CompactionWasPresent {
		v := o.op.CompactionWasEnabled
		restore = &v
	}
	if err := o.r.duckling.SetCompactionEnabled(ctx, o.op.DucklingName, restore); err != nil {
		o.logf("error", "restoring compaction setting failed (was present=%t enabled=%t): %v — fix the duckling spec manually", o.op.CompactionWasPresent, o.op.CompactionWasEnabled, err)
		return
	}
	o.compactionPaused = false
	o.logf("info", "compaction setting restored (present=%t enabled=%t)", o.op.CompactionWasPresent, o.op.CompactionWasEnabled)
}

// rollback returns the org to its pre-reshard state as far as the direction
// allows. Errors are logged, never propagated — rollback is best-effort and
// the op is marked failed/cancelled regardless.
func (o *opRun) rollback(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)

	if o.flipped {
		switch {
		case o.op.TargetKind == configstore.MetadataStoreKindCnpgShard && o.op.SourceKind == configstore.MetadataStoreKindCnpgShard:
			// Patch the source shard VALUE back — never remove the key: the
			// precedence chain would fall through to the freshly-stamped
			// (bogus) status pin.
			o.logf("warn", "rolling back: re-pointing duckling at source shard %s", o.op.FromShard)
			if err := o.r.duckling.SetMetadataStoreCnpg(ctx, o.op.DucklingName, o.op.FromShard); err != nil {
				o.logf("error", "rollback flip failed: %v — duckling still points at %s; fix manually", err, o.op.ToShard)
			}
			o.dropPartialTarget(ctx)
		case o.op.TargetKind == configstore.MetadataStoreKindCnpgShard && o.op.SourceKind == configstore.MetadataStoreKindExternal:
			// ext→cnpg rollback: back to external; cnpgShard must be removed
			// (CEL forbids it on the external type).
			o.logf("warn", "rolling back: re-pointing duckling at external source %s", o.op.SourceEndpoint)
			if err := o.r.duckling.SetMetadataStoreExternal(ctx, o.op.DucklingName, ExternalMetadataStoreSpec{
				Endpoint:          o.op.SourceEndpoint,
				PasswordAWSSecret: o.op.SourcePasswordSecret,
				User:              o.op.SourceUser,
				Database:          o.op.SourceDatabase,
			}); err != nil {
				o.logf("error", "rollback flip failed: %v — duckling still points at cnpg %s; fix manually", err, o.op.ToShard)
			}
			o.dropPartialTarget(ctx)
		case o.op.TargetKind == configstore.MetadataStoreKindExternal:
			// cnpg→ext flipped: the cnpg source was ORPHANED (retained), not
			// deleted, so recovery flips back and re-ADOPTS it — no copy-back.
			o.recoverFromExternal(ctx)
		}
	} else if o.op.TargetKind == configstore.MetadataStoreKindExternal {
		// cnpg→ext failed BEFORE the flip: source untouched.
		if o.retainRequested {
			// We set retainCnpgOnFlip=true but never flipped — reset it so the
			// still-cnpg source keeps full-lifecycle (Delete) MRs and a later
			// deprovision stays clean.
			o.logf("warn", "rolling back: clearing retainCnpgOnFlip on the still-cnpg source (restores full-lifecycle Delete so deprovision stays clean)")
			if err := o.r.duckling.SetMetadataStoreRetainCnpgOnFlip(ctx, o.op.DucklingName, false); err != nil {
				o.logf("error", "clearing retainCnpgOnFlip failed: %v — the cnpg role/DB MRs may lack Delete and a deprovision could ORPHAN (leak) them; fix by setting spec.metadataStore.retainCnpgOnFlip=false", err)
			}
		}
		if o.copied.Tables > 0 {
			// Drop what we copied onto the external target (best-effort).
			o.logf("warn", "rolling back: dropping partially copied tables from the external target")
			if err := o.r.copier.DropCatalogTables(ctx, o.target, func(level, msg string) { o.logf(level, "%s", msg) }); err != nil {
				o.logf("warn", "dropping copied tables from the external target failed: %v (harmless leftover)", err)
			}
		}
	}

	o.restoreCompaction(ctx)

	if o.blocked {
		if err := o.r.store.UpdateWarehouseState(o.op.OrgID, configstore.ManagedWarehouseStateResharding, map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateReady,
			"status_message": "metadata-store reshard rolled back",
		}); err != nil {
			o.logf("error", "unblocking warehouse failed: %v — org is still blocked; investigate immediately", err)
		} else {
			now := time.Now().UTC()
			_ = o.fields(map[string]interface{}{"unblocked_at": now})
			o.op.UnblockedAt = &now
			o.logf("info", "warehouse unblocked: resharding → ready (rolled back)")
		}
	}
}

// dropPartialTarget best-effort drops the half-copied catalog tables from a
// cnpg target after a rollback (the empty role/DB itself stays orphaned on
// the target shard — benign, adopted on a retry).
func (o *opRun) dropPartialTarget(ctx context.Context) {
	if o.target.Host == "" || o.copied.Tables == 0 {
		return
	}
	if err := o.r.copier.DropCatalogTables(ctx, o.target, func(level, msg string) { o.logf(level, "%s", msg) }); err != nil {
		o.logf("warn", "dropping partially copied tables from the target failed: %v (harmless leftover)", err)
	}
}

// recoverFromExternal handles the cnpg→ext post-flip rollback: the external
// target never became Ready, or the external-catalog verify failed. Because the
// orphan step retained the cnpg source, the role/DB are STILL PRESENT on the
// shard — so recovery flips the type back to cnpg-shard AND clears
// retainCnpgOnFlip in one patch (SetMetadataStoreCnpgAdopt), and provider-sql
// re-ADOPTS the role/DB by external-name (same pgIdent, same pinned password).
// No empty-recreate, no copy-back — the catalog never left the source. This is
// the whole point of the orphan-adopt escape hatch.
func (o *opRun) recoverFromExternal(ctx context.Context) {
	o.logf("warn", "cnpg→external cutover failed AFTER the flip — the cnpg source role/DB were RETAINED (orphaned), not deleted; recovering by flipping back to shard %s and re-adopting them (no data copy)", o.op.FromShard)

	if err := o.r.duckling.SetMetadataStoreCnpgAdopt(ctx, o.op.DucklingName, o.op.FromShard); err != nil {
		o.logf("error", "recovery flip-back failed: %v — duckling still points at the external target; the orphaned cnpg role/DB survive on %s and can be re-adopted manually (patch spec.metadataStore to {type: cnpg-shard, cnpgShard: %s, retainCnpgOnFlip: false}); fix manually", err, o.op.FromShard, o.op.FromShard)
		return
	}

	// Wait for the composition to re-adopt the role/DB on the source shard and
	// the tenant role to answer again.
	sourcePrefix := o.op.FromShard + "-pooler."
	deadline := time.Now().Add(o.flipTimeout())
	for {
		if time.Now().After(deadline) {
			o.logf("error", "recovery: source shard did not become ready within %s — org stays blocked; investigate (the orphaned role/DB should still exist on %s)", o.flipTimeout(), o.op.FromShard)
			return
		}
		st, err := o.r.duckling.Get(ctx, o.op.DucklingName)
		if err == nil && strings.HasPrefix(st.MetadataStore.Endpoint, sourcePrefix) && st.ReadyCondition {
			restored := CatalogEndpoint{
				Host: st.MetadataStore.Endpoint, Port: 5432,
				User: st.MetadataStore.User, Password: st.MetadataStore.Password, Database: st.MetadataStore.Database,
				SSLMode: "disable",
			}
			if o.r.copier.Probe(ctx, restored) == nil {
				o.logf("info", "recovery complete: duckling re-adopted the orphaned cnpg role/DB on %s and the tenant role answers — no data was copied back (the catalog never left the source)", o.op.FromShard)
				return
			}
		}
		time.Sleep(o.r.loopPoll)
	}
}

// report writes the end-of-op report to the log — also on failure/cancel.
func (o *opRun) report(state configstore.ReshardState) {
	fresh, err := o.r.store.GetReshardOperation(o.op.ID)
	if err == nil {
		o.op = fresh
	}
	var lines []string
	lines = append(lines, fmt.Sprintf("==== reshard report (%s) ====", state))
	lines = append(lines, fmt.Sprintf("org %s: %s → %s", o.op.OrgID, describeSource(o.op), describeTarget(o.op)))
	if o.op.StartedAt != nil {
		lines = append(lines, fmt.Sprintf("started %s", o.op.StartedAt.UTC().Format(time.RFC3339)))
	}
	if o.op.BlockedAt != nil {
		end := time.Now().UTC()
		if o.op.UnblockedAt != nil {
			end = o.op.UnblockedAt.UTC()
		}
		lines = append(lines, fmt.Sprintf("maintenance mode (connections blocked): %s (from %s to %s)",
			end.Sub(o.op.BlockedAt.UTC()).Round(time.Second),
			o.op.BlockedAt.UTC().Format(time.RFC3339), end.Format(time.RFC3339)))
	} else {
		lines = append(lines, "maintenance mode: never entered")
	}
	if o.op.TablesCopied > 0 || state == configstore.ReshardStateSucceeded {
		lines = append(lines, fmt.Sprintf("copied: %d tables, %d rows, %d bytes", o.op.TablesCopied, o.op.RowsCopied, o.op.BytesCopied))
	}
	if o.op.BackupS3URI != "" {
		lines = append(lines, "pre-flip catalog backup: "+o.op.BackupS3URI)
	}
	if o.op.SourceKind == configstore.MetadataStoreKindExternal {
		lines = append(lines, "external source: left untouched")
	}
	lines = append(lines, fmt.Sprintf("total runtime: %s", time.Since(o.op.CreatedAt).Round(time.Second)))
	for _, l := range lines {
		o.logf("info", "%s", l)
	}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
