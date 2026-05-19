//go:build kubernetes

package provisioner

import (
	"database/sql"
	"testing"
)

// cleanupDB attempts to drop a database. Best-effort: logs but does not fail.
func cleanupDB(t *testing.T, dsn, name string) {
	t.Helper()
	if dsn == "" {
		return
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Logf("cleanup open %s: %v", name, err)
		return
	}
	defer db.Close()
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + quoteIdent(name)); err != nil {
		t.Logf("cleanup drop %s: %v", name, err)
	}
}
