//go:build kubernetes

package controlplane

import (
	"fmt"
	"os"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestProvisionerControllerEnabled(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", true},
		{"true", true},
		{"1", true},
		{"false", false},
		{"0", false},
		{"not-a-bool", true},
	} {
		t.Setenv(envProvisionerEnabled, tc.value)
		if got := provisionerControllerEnabled(); got != tc.want {
			t.Errorf("%s=%q: got %v, want %v", envProvisionerEnabled, tc.value, got, tc.want)
		}
	}
}

func TestControlHandoverForcesTheControllerOff(t *testing.T) {
	t.Setenv(envProvisionerEnabled, "true")
	controlHandedOver = true
	t.Cleanup(func() { controlHandedOver = false })
	if provisionerControllerEnabled() {
		t.Fatal("a recorded hand-over must keep the provisioning controller off")
	}
}

func TestApplyControlHandoverReadsTheStore(t *testing.T) {
	dsn := os.Getenv("DUCKGRES_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("DUCKGRES_TEST_PG_DSN not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("handover_test_%d", time.Now().UnixNano())
	if err := db.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP SCHEMA " + schema + " CASCADE") })
	scoped := db.Session(&gorm.Session{})
	if err := scoped.Exec("SET search_path TO " + schema).Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := scoped.DB()
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { controlHandedOver = false })

	t.Setenv(envTrinoPoolOperatorEnabled, "true")
	t.Setenv(envTrinoDefaultCell, "cell-001")
	if _, err := applyControlHandover(scoped); err != nil {
		t.Fatalf("a missing table is no hand-over: %v", err)
	}
	if controlHandedOver || os.Getenv(envTrinoPoolOperatorEnabled) != "true" {
		t.Fatal("no hand-over must change nothing")
	}

	if err := scoped.Exec("CREATE TABLE " + controlHandoverTable + " (component text PRIMARY KEY, owner text NOT NULL)").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := applyControlHandover(scoped); err != nil || controlHandedOver {
		t.Fatalf("an empty table is no hand-over: %v %v", err, controlHandedOver)
	}
	if err := scoped.Exec("INSERT INTO " + controlHandoverTable + " VALUES ('provisioning', 'hogtower')").Error; err != nil {
		t.Fatal(err)
	}
	owner, err := applyControlHandover(scoped)
	if err != nil {
		t.Fatal(err)
	}
	if owner != "hogtower" {
		t.Fatalf("owner = %q, want hogtower", owner)
	}
	if !controlHandedOver || provisionerControllerEnabled() || trinoPoolOperatorEnabled() {
		t.Fatal("a recorded hand-over must switch the controller and the pool operator off")
	}
	if os.Getenv(envTrinoPoolCatalogWriter) != "false" || os.Getenv(envTrinoPoolNodeDisruptionEnabled) != "false" {
		t.Fatal("a hand-over must switch the catalog writer and node disruption off")
	}
	if _, set := os.LookupEnv(envTrinoDefaultCell); set {
		t.Fatal("a hand-over must drop the default placement, which requires the operator")
	}
}
