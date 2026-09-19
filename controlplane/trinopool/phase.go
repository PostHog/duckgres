package trinopool

import "fmt"

// Phase is the durable lifecycle position of one pool instance. It is stored on
// the instance row and is the only thing that authorizes an external effect:
// admission, drain, and above all deletion.
type Phase string

// The planned lifecycle, then the failure branch. The two are deliberately
// separate: a coordinator that died with in-flight work has NOT drained, and
// reporting it as SEALED/RETIRED would erase that distinction from the record.
const (
	PhasePending    Phase = "PENDING"    // intent persisted, nothing created yet
	PhaseCreating   Phase = "CREATING"   // Kubernetes objects being created
	PhasePreparing  Phase = "PREPARING"  // pods exist, member registered but unroutable
	PhaseValidating Phase = "VALIDATING" // candidate validation in progress
	PhaseAdmitted   Phase = "ADMITTED"   // Gateway CAS accepted; receipt persisted
	PhaseServing    Phase = "SERVING"    // counts toward the minimum serving floor
	PhaseDraining   Phase = "DRAINING"   // no new independent work; obligations remain
	PhaseSealed     Phase = "SEALED"     // obligations finished, retirement not yet claimed
	PhaseRetiring   Phase = "RETIRING"   // irreversible retirement claimed; deletion permitted
	PhaseRetired    Phase = "RETIRED"    // resources verified absent

	PhaseSuspect        Phase = "SUSPECT"         // probe failing; excluded from new admissions
	PhaseLost           Phase = "LOST"            // authoritative evidence the process terminated
	PhaseFailureRetired Phase = "FAILURE_RETIRED" // failure receipt recorded, resources removed

	// PhaseFailedPreparing is a candidate that can never be admitted. It is NOT
	// terminal: its Kubernetes objects still exist and its Gateway member is
	// still PREPARING, which counts against the pool's live budget. A terminal
	// FAILED_PREPARING leaked a whole Trino cluster and, after a single failed
	// candidate at desired+surge, no further member could register at all - no
	// repair and no rollout. The instance is cleaned up from here and only then
	// becomes FAILURE_RETIRED.
	PhaseFailedPreparing Phase = "FAILED_PREPARING"
)

// phaseTransitions is the whole state machine. Absence is denial.
var phaseTransitions = map[Phase][]Phase{
	PhasePending:    {PhaseCreating, PhaseFailedPreparing},
	PhaseCreating:   {PhasePreparing, PhaseFailedPreparing},
	PhasePreparing:  {PhaseValidating, PhaseFailedPreparing},
	PhaseValidating: {PhaseAdmitted, PhasePreparing, PhaseFailedPreparing},
	PhaseAdmitted:   {PhaseServing, PhaseDraining, PhaseSuspect},
	PhaseServing:    {PhaseDraining, PhaseSuspect},
	PhaseDraining:   {PhaseSealed, PhaseSuspect},
	PhaseSealed:     {PhaseRetiring, PhaseSuspect},
	PhaseRetiring:   {PhaseRetired},
	PhaseRetired:    nil,

	// A suspected member always leaves; only the route depends on the
	// evidence. There is no path back to SERVING: suspicion is the Gateway's
	// state too, and it excludes the member until a fresh certified admission
	// - which a recovered-but-uncertain incarnation does not get, because the
	// pool can replace it with a certain one instead. Recording a local
	// recovery would leave this row claiming a member serves while the Gateway
	// refuses to route to it.
	PhaseSuspect:        {PhaseDraining, PhaseLost},
	PhaseLost:           {PhaseFailureRetired},
	PhaseFailureRetired: nil,
	// A failed candidate is cleaned up and then recorded as failure-retired.
	// The Gateway member it registered is walked PREPARING -> SUSPECT -> LOST
	// first, because that is what releases the pool's live slot; deletion is
	// permitted before that only because the candidate provably never admitted
	// work.
	PhaseFailedPreparing: {PhaseFailureRetired},
}

// Valid reports whether the phase is one this build knows. A row written by a
// newer binary must not be silently treated as some default.
func (p Phase) Valid() bool {
	_, known := phaseTransitions[p]
	return known
}

// Terminal reports whether the phase can never change again.
func (p Phase) Terminal() bool {
	return p.Valid() && len(phaseTransitions[p]) == 0
}

// Serving reports whether the instance counts toward the minimum serving floor.
// Only SERVING does: an ADMITTED instance has not been observed serving yet and
// a DRAINING one is by definition leaving.
func (p Phase) Serving() bool { return p == PhaseServing }

// OccupiesCapacity reports whether the instance still holds a live compute slot
// and therefore counts against desired + surge. A terminal tombstone does not:
// historical obligations on a proven-dead process must not block a replacement
// forever. Everything else does, including SUSPECT and LOST, whose pods are
// still running until deletion is verified.
func (p Phase) OccupiesCapacity() bool { return p.Valid() && !p.Terminal() }

// PermitsResourceDeletion reports whether Kubernetes objects of this instance
// may be deleted. Deletion requires either an irreversible Gateway retirement
// claim (RETIRING and later, FAILURE_RETIRED) or proof the candidate never
// admitted work (FAILED_PREPARING). A transient zero obligation count, a
// failing probe, or a SEALED member without a claim is never sufficient.
func (p Phase) PermitsResourceDeletion() bool {
	switch p {
	case PhaseRetiring, PhaseRetired, PhaseFailureRetired, PhaseFailedPreparing:
		return true
	default:
		return false
	}
}

// ValidateTransition reports whether the instance may move from one phase to
// another. Callers still have to apply it as a CAS against the stored phase;
// this only decides whether the move is legal at all.
func ValidateTransition(from, to Phase) error {
	if !from.Valid() {
		return fmt.Errorf("unknown source phase %q", from)
	}
	if !to.Valid() {
		return fmt.Errorf("unknown target phase %q", to)
	}
	for _, candidate := range phaseTransitions[from] {
		if candidate == to {
			return nil
		}
	}
	return fmt.Errorf("instance phase %s cannot move to %s", from, to)
}
