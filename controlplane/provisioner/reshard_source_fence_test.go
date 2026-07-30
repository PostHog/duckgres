//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recordingReshardSession struct {
	events []string
}

func (s *recordingReshardSession) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.TrimSpace(query) == "BEGIN":
		s.events = append(s.events, "begin")
	case strings.TrimSpace(query) == "ROLLBACK":
		s.events = append(s.events, "rollback")
	case strings.Contains(query, "pg_terminate_backend"):
		s.events = append(s.events, "terminate")
	default:
		return pgconn.CommandTag{}, fmt.Errorf("unexpected query: %s", query)
	}
	return pgconn.CommandTag{}, nil
}

func (s *recordingReshardSession) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if !strings.Contains(query, "pg_stat_activity") {
		return reshardCountRow{err: fmt.Errorf("unexpected query: %s", query)}
	}
	s.events = append(s.events, "count")
	return reshardCountRow{}
}

type reshardCountRow struct {
	err error
}

func (r reshardCountRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("scan destinations = %d, want 1", len(dest))
	}
	remaining, ok := dest[0].(*int64)
	if !ok {
		return fmt.Errorf("scan destination type = %T, want *int64", dest[0])
	}
	*remaining = 0
	return nil
}

func TestDisableMaintenancePinsBackendBeforeNoLogin(t *testing.T) {
	session := &recordingReshardSession{}
	err := disableMaintenanceAndTerminateOnSession(context.Background(), session, "reshard_op", func() error {
		session.events = append(session.events, "disable")
		return nil
	})
	if err != nil {
		t.Fatalf("disable maintenance: %v", err)
	}
	want := []string{"begin", "disable", "terminate", "count", "rollback"}
	if !reflect.DeepEqual(session.events, want) {
		t.Fatalf("events = %v, want %v", session.events, want)
	}
}
