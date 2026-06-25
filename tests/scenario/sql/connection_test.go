package sql

import (
	"strings"
	"testing"
)

func TestConnectionConfigDSNUsesSNIHostAndTCPHostAddr(t *testing.T) {
	dsn, err := ConnectionConfig{
		OrgID:           "scenario-org",
		SNISuffix:       ".dev.example",
		HostAddr:        "10.0.0.10",
		Port:            5432,
		Database:        "ducklake",
		Username:        "root",
		Password:        "root password",
		SSLMode:         "require",
		ConnectTimeout:  10,
		ApplicationName: "duckgres-scenario",
	}.DSN()
	if err != nil {
		t.Fatalf("DSN returned error: %v", err)
	}

	for _, want := range []string{
		"host=scenario-org.dev.example",
		"hostaddr=10.0.0.10",
		"port=5432",
		"user=root",
		"password='root password'",
		"dbname=ducklake",
		"sslmode=require",
		"connect_timeout=10",
		"application_name=duckgres-scenario",
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("DSN %q missing %q", dsn, want)
		}
	}
}

func TestConnectionConfigDSNRequiresSNIFields(t *testing.T) {
	_, err := ConnectionConfig{
		OrgID:    "scenario-org",
		HostAddr: "10.0.0.10",
		Port:     5432,
		Database: "ducklake",
		Username: "root",
		Password: "root-password",
	}.DSN()
	if err == nil {
		t.Fatal("expected missing SNI suffix to fail")
	}
}

func TestConnectionConfigDSNDefaultsDatabaseAndEscapesPassword(t *testing.T) {
	dsn, err := ConnectionConfig{
		OrgID:     "scenario-org",
		SNISuffix: ".dev.example",
		HostAddr:  "10.0.0.10",
		Username:  "root",
		Password:  `pa'ss\word`,
	}.DSN()
	if err != nil {
		t.Fatalf("DSN returned error: %v", err)
	}
	if !strings.Contains(dsn, "dbname=ducklake") {
		t.Fatalf("DSN %q missing default ducklake catalog", dsn)
	}
	if !strings.Contains(dsn, `password='pa\'ss\\word'`) {
		t.Fatalf("DSN %q did not escape password", dsn)
	}
}

func TestConnectionConfigDSNRejectsSuffixWithoutLeadingDot(t *testing.T) {
	_, err := ConnectionConfig{
		OrgID:     "scenario-org",
		SNISuffix: "dev.example",
		HostAddr:  "10.0.0.10",
		Username:  "root",
		Password:  "root-password",
	}.DSN()
	if err == nil {
		t.Fatal("expected SNI suffix without leading dot to fail")
	}
}
