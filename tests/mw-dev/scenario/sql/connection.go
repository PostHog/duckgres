package sql

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

type ConnectionConfig struct {
	OrgID           string
	SNISuffix       string
	DialHost        string
	Port            int
	Database        string
	Username        string
	Password        string
	SSLMode         string
	ConnectTimeout  int
	ApplicationName string
	Options         string
}

// PGWireConnection separates the hostname used for managed-warehouse TLS
// routing from the address used for the underlying TCP connection.
//
// lib/pq does not support libpq's hostaddr parameter. Keeping this split
// outside the DSN prevents it from resolving the managed SNI hostname.
type PGWireConnection struct {
	DSN         string
	DialAddress string
}

func (c ConnectionConfig) PGWire() (PGWireConnection, error) {
	if c.OrgID == "" {
		return PGWireConnection{}, classified(ErrorClassConfig, fmt.Errorf("org id is required for SQL connection"))
	}
	if c.SNISuffix == "" {
		return PGWireConnection{}, classified(ErrorClassConfig, fmt.Errorf("SNI suffix is required for SQL connection"))
	}
	if !strings.HasPrefix(c.SNISuffix, ".") {
		return PGWireConnection{}, classified(ErrorClassConfig, fmt.Errorf("SNI suffix must start with a dot"))
	}
	if c.DialHost == "" {
		return PGWireConnection{}, classified(ErrorClassConfig, fmt.Errorf("PG direct dial host is required for SQL connection"))
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.Database == "" {
		c.Database = "ducklake"
	}
	if c.Username == "" {
		c.Username = "root"
	}
	if c.SSLMode == "" {
		c.SSLMode = "require"
	}

	values := [][2]string{
		{"host", c.OrgID + c.SNISuffix},
		{"port", strconv.Itoa(c.Port)},
		{"user", c.Username},
		{"password", c.Password},
		{"dbname", c.Database},
		{"sslmode", c.SSLMode},
	}
	if c.ConnectTimeout > 0 {
		values = append(values, [2]string{"connect_timeout", strconv.Itoa(c.ConnectTimeout)})
	}
	if c.ApplicationName != "" {
		values = append(values, [2]string{"application_name", c.ApplicationName})
	}
	if c.Options != "" {
		values = append(values, [2]string{"options", c.Options})
	}

	parts := make([]string, 0, len(values))
	for _, kv := range values {
		parts = append(parts, kv[0]+"="+quoteConninfoValue(kv[1]))
	}
	return PGWireConnection{
		DSN:         strings.Join(parts, " "),
		DialAddress: net.JoinHostPort(c.DialHost, strconv.Itoa(c.Port)),
	}, nil
}

// WorkerProfileStartupOptions returns libpq startup options for requesting a
// sized Duckgres worker. Empty values are omitted so callers can set either
// dimension independently, matching the control-plane startup contract.
func WorkerProfileStartupOptions(cpu, memory string) string {
	parts := make([]string, 0, 4)
	if cpu != "" {
		parts = append(parts, "-c", "duckgres.worker_cpu="+cpu)
	}
	if memory != "" {
		parts = append(parts, "-c", "duckgres.worker_memory="+memory)
	}
	return strings.Join(parts, " ")
}

// AppendStartupOptions combines existing connection options with options
// supplied by a scenario step.
func AppendStartupOptions(existing, extra string) string {
	return strings.TrimSpace(strings.Join([]string{existing, extra}, " "))
}

func (c ConnectionConfig) DSN() (string, error) {
	connection, err := c.PGWire()
	if err != nil {
		return "", err
	}
	return connection.DSN, nil
}

func (c ConnectionConfig) OpenDB() (*stdsql.DB, error) {
	connection, err := c.PGWire()
	if err != nil {
		return nil, err
	}
	return connection.OpenDB()
}

func (c PGWireConnection) OpenDB() (*stdsql.DB, error) {
	if c.DSN == "" {
		return nil, classified(ErrorClassConfig, fmt.Errorf("PGWire DSN is required"))
	}
	if c.DialAddress == "" {
		return nil, classified(ErrorClassConfig, fmt.Errorf("PGWire direct dial address is required"))
	}
	connector, err := pq.NewConnector(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("create PGWire connector: %w", err)
	}
	connector.Dialer(directDialer{address: c.DialAddress})
	return stdsql.OpenDB(connector), nil
}

type directDialer struct {
	address string
	dialer  net.Dialer
}

func (d directDialer) Dial(network, _ string) (net.Conn, error) {
	return d.dialer.Dial(network, d.address)
}

func (d directDialer) DialTimeout(network, _ string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.dialer.DialContext(ctx, network, d.address)
}

func (d directDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, d.address)
}

func quoteConninfoValue(value string) string {
	if value == "" {
		return "''"
	}
	if !strings.ContainsAny(value, " \t\n\r'\\") {
		return value
	}
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}
