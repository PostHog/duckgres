package sql

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestConnectionConfigPGWireUsesSNIHostWithoutHostAddr(t *testing.T) {
	connection, err := (ConnectionConfig{
		OrgID:           "scenario-org",
		SNISuffix:       ".dev.example",
		DialHost:        "10.0.0.10",
		Port:            5432,
		Database:        "ducklake",
		Username:        "root",
		Password:        "root password",
		SSLMode:         "require",
		ConnectTimeout:  10,
		ApplicationName: "duckgres-scenario",
	}).PGWire()
	if err != nil {
		t.Fatalf("PGWire returned error: %v", err)
	}
	dsn := connection.DSN

	for _, want := range []string{
		"host=scenario-org.dev.example",
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
	if strings.Contains(dsn, "hostaddr=") {
		t.Fatalf("DSN %q should not include unsupported lib/pq hostaddr", dsn)
	}
	if connection.DialAddress != "10.0.0.10:5432" {
		t.Fatalf("direct dial address = %q, want 10.0.0.10:5432", connection.DialAddress)
	}
}

func TestConnectionConfigOpenDBDialsDirectAddressAndPreservesSNI(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	sniNames := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = listener.Close() }()
		defer func() { _ = conn.Close() }()

		request := make([]byte, 8)
		if _, err := io.ReadFull(conn, request); err != nil {
			return
		}
		if _, err := conn.Write([]byte{'S'}); err != nil {
			return
		}
		_ = tls.Server(conn, &tls.Config{
			GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				sniNames <- hello.ServerName
				return nil, nil
			},
		}).Handshake()
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	db, err := (ConnectionConfig{
		OrgID:     "scenario-org",
		SNISuffix: ".ci.invalid",
		DialHost:  "127.0.0.1",
		Port:      port,
		Database:  "ducklake",
		Username:  "root",
		Password:  "root-password",
		SSLMode:   "require",
	}).OpenDB()
	if err != nil {
		t.Fatalf("OpenDB returned error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err == nil {
		t.Fatal("PingContext unexpectedly succeeded against incomplete test server")
	}

	select {
	case got := <-sniNames:
		if got != "scenario-org.ci.invalid" {
			t.Fatalf("TLS SNI = %q, want scenario-org.ci.invalid", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive a TLS ClientHello")
	}
}

func TestConnectionConfigDSNRequiresSNIFields(t *testing.T) {
	_, err := ConnectionConfig{
		OrgID:    "scenario-org",
		DialHost: "10.0.0.10",
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
		DialHost:  "10.0.0.10",
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
		DialHost:  "10.0.0.10",
		Username:  "root",
		Password:  "root-password",
	}.DSN()
	if err == nil {
		t.Fatal("expected SNI suffix without leading dot to fail")
	}
}
