package configstore

import (
	"strconv"
	"strings"
	"testing"
)

// Every migration owns its own version number.
//
// goose keys applied migrations by that number, so two files sharing one is not
// a cosmetic clash: whichever ran first records the version, and the second is
// then considered already applied and NEVER runs. The schema it was supposed to
// create is simply absent, on every environment, with no error anywhere.
//
// This is exactly what a long-lived branch produces when it is merged: two
// sides each add "the next" migration. The tripwire is cheap and the failure it
// prevents is silent.
func TestMigrationVersionsAreUnique(t *testing.T) {
	entries, err := configStoreMigrationFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	owner := map[int64]string{}
	var highest int64
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		digits, _, found := strings.Cut(name, "_")
		if !found {
			t.Fatalf("migration %q is not <version>_<name>.sql", name)
		}
		version, err := strconv.ParseInt(digits, 10, 64)
		if err != nil || version < 1 {
			t.Fatalf("migration %q has no usable version prefix", name)
		}
		if previous, taken := owner[version]; taken {
			t.Fatalf("migrations %q and %q share version %d; one of them would never run", previous, name, version)
		}
		owner[version] = name
		if version > highest {
			highest = version
		}
	}
	// No gaps either: a hole is how two branches end up "renumbered" onto the
	// same free slot later.
	for version := int64(1); version <= highest; version++ {
		if _, present := owner[version]; !present {
			t.Fatalf("migration version %d is missing; the sequence has a hole up to %d", version, highest)
		}
	}
}
