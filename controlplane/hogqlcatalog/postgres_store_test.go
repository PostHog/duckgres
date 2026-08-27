//go:build kubernetes

package hogqlcatalog

import (
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestPostgresStorePersistsImmutableGenerationsAcrossInstances(t *testing.T) {
	connection, err := net.DialTimeout("tcp", "127.0.0.1:35432", time.Second)
	if err != nil {
		t.Skipf("config-store Postgres is not running: %v", err)
	}
	_ = connection.Close()

	config, err := configstore.NewConfigStore(
		"host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable",
		time.Hour,
	)
	if err != nil {
		t.Fatalf("new config store: %v", err)
	}
	sqlDB, err := config.DB().DB()
	if err != nil {
		t.Fatalf("get SQL database: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := config.DB().Exec(
		"DELETE FROM duckgres_hogql_semantic_catalog_snapshots WHERE catalog_value = ?",
		testCatalog().Value,
	).Error; err != nil {
		t.Fatalf("reset catalog snapshots: %v", err)
	}

	firstProcess := NewPostgresStore(config.DB())
	expected := completeSemanticSnapshot(1)
	if err := firstProcess.Publish(t.Context(), expected); err != nil {
		t.Fatalf("publish generation 1: %v", err)
	}

	restartedProcess := NewPostgresStore(config.DB())
	pinned, err := restartedProcess.Generation(t.Context(), testCatalog(), 1)
	if err != nil {
		t.Fatalf("read generation 1 after restart: %v", err)
	}
	if pinned.Generation != 1 {
		t.Fatalf("pinned generation = %d, want 1", pinned.Generation)
	}
	if !reflect.DeepEqual(pinned, expected) {
		t.Fatalf("persisted snapshot changed after restart\n got: %#v\nwant: %#v", pinned, expected)
	}
	if err := restartedProcess.Publish(t.Context(), testSnapshot(2)); err != nil {
		t.Fatalf("publish generation 2: %v", err)
	}
	if err := firstProcess.Publish(t.Context(), testSnapshot(1)); !errors.Is(err, ErrGenerationRegression) {
		t.Fatalf("publish older generation error = %v, want ErrGenerationRegression", err)
	}
}
