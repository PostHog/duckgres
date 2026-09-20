package trinogateway

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// These fixtures are NOT hand-written. They are produced by serializing the
// Gateway's real PoolStore records with Jackson (see the fixture generator
// recorded in STATUS-duckgres.md) and copied here verbatim. Decoding them is
// what establishes that this client and the Java producer agree; a test that
// invents both sides of the wire establishes nothing.
func readFixture(t *testing.T, name string, target any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	// Unknown fields are a contract drift signal: the Java side grew a field
	// this client does not know about, and silently ignoring it is how a
	// consumer ends up making decisions on a stale view.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode %s against the Go type: %v", name, err)
	}
}

func TestPoolStateFixtureDecodes(t *testing.T) {
	var pool PoolState
	readFixture(t, "pool_state.json", &pool)

	if pool.PoolID != "pool-001" || pool.APIMode != "POOLED" {
		t.Fatalf("pool = %+v", pool)
	}
	// The Java record calls these desiredMembers and maxRepair. An earlier
	// draft of this client used desiredCount/repairBudget, which decoded to
	// zero and would have reported a pool with no desired members.
	if pool.DesiredMembers != 3 {
		t.Fatalf("desiredMembers = %d, want 3", pool.DesiredMembers)
	}
	if pool.MaxRepair != 1 {
		t.Fatalf("maxRepair = %d, want 1", pool.MaxRepair)
	}
	if pool.ControllerEpoch != 7 || pool.MembershipGeneration != 19 {
		t.Fatalf("epoch/generation = %d/%d", pool.ControllerEpoch, pool.MembershipGeneration)
	}
	if pool.Counts["ACTIVE"] != 3 || pool.Counts["LOST"] != 1 {
		t.Fatalf("counts = %v", pool.Counts)
	}
}

func TestMemberFixtureDecodes(t *testing.T) {
	var member Member
	readFixture(t, "member.json", &member)

	if member.InstanceID != "i-0007" || member.Phase != "ACTIVE" || member.Generation != 4 {
		t.Fatalf("member = %+v", member)
	}
	if member.URL == "" || member.ExternalURL == "" {
		t.Fatal("the endpoint fields did not decode")
	}
	// The Gateway records an auth revision on the member; an earlier draft of
	// this client asserted no such thing could exist.
	if member.AuthRevision != "auth-9" {
		t.Fatalf("authRevision = %q", member.AuthRevision)
	}
	if member.Incarnation != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("incarnation = %q", member.Incarnation)
	}
}

// GET members returns a bare JSON array. An envelope-shaped client decodes it
// as an empty list, which reads as "this pool has no members" - the most
// dangerous possible misreading for a controller that creates capacity.
func TestMembersFixtureIsABareArray(t *testing.T) {
	var members []Member
	readFixture(t, "members.json", &members)
	if len(members) != 1 || members[0].InstanceID != "i-0007" {
		t.Fatalf("members = %+v", members)
	}
}

func TestObligationsFixtureDecodes(t *testing.T) {
	var obligations Obligations
	readFixture(t, "obligations.json", &obligations)

	if obligations.PendingRequests != 2 || obligations.OpenTransactions != 1 || obligations.ActiveQueries != 3 {
		t.Fatalf("obligations = %+v", obligations)
	}
	if obligations.Outstanding() != 6 {
		t.Fatalf("outstanding = %d, want 6", obligations.Outstanding())
	}
	if obligations.Drained || obligations.ReadyToSeal {
		t.Fatal("a member with outstanding work reported itself drained")
	}
}

func TestPublicationFixtureDecodes(t *testing.T) {
	var publication Publication
	readFixture(t, "publication.json", &publication)

	if publication.PublicationID != "pub-1" || publication.Phase != "OPEN" {
		t.Fatalf("publication = %+v", publication)
	}
	if len(publication.Receipts) != 1 || publication.Receipts[0].AppliedRevision != "r-42" {
		t.Fatalf("receipts = %+v", publication.Receipts)
	}
	if publication.TenantState != "PENDING" {
		t.Fatalf("tenantState = %q", publication.TenantState)
	}
}

func TestTenantAdmissionFixtureDecodes(t *testing.T) {
	var admission TenantAdmission
	readFixture(t, "tenant_admission.json", &admission)
	if admission.State != "ADMITTED" || admission.AdmittedRevision != "r-42" {
		t.Fatalf("admission = %+v", admission)
	}
}

func TestFailureReceiptFixtureDecodes(t *testing.T) {
	var receipt FailureReceipt
	readFixture(t, "failure_receipt.json", &receipt)

	if receipt.Evidence != "PROCESS_TERMINATED" {
		t.Fatalf("evidence = %q", receipt.Evidence)
	}
	// A failure receipt PRESERVES the work that was lost. Reporting it as zero
	// would turn a crash into a clean drain in the operator's record.
	if receipt.OutstandingTransactions != 1 || receipt.OutstandingQueries != 2 {
		t.Fatalf("outstanding work was not preserved: %+v", receipt)
	}
}

func TestOperationHistoryFixtureDecodes(t *testing.T) {
	var history OperationHistory
	readFixture(t, "operation_history.json", &history)

	step, found := history.Step("admit")
	if !found || step.Outcome != "OK" {
		t.Fatalf("history = %+v", history)
	}
	if step.Result["phase"] != "ACTIVE" {
		t.Fatalf("recorded result = %v", step.Result)
	}
}
