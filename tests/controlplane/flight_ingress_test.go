//go:build linux || darwin

package controlplane_test

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func flightAuthContext(username, password string) context.Context {
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Basic "+token))
}

func newFlightClient(t *testing.T, port int) *flightsql.Client {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	tlsCfg := &tls.Config{InsecureSkipVerify: true} // test-only self-signed cert
	client, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(server.MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(server.MaxGRPCMessageSize),
		),
	)
	if err != nil {
		t.Fatalf("failed to create Flight SQL client: %v", err)
	}
	return client
}

func requireGetTablesIncludeSchema(t *testing.T, client *flightsql.Client, ctx context.Context) error {
	info, err := client.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
	if err != nil {
		return fmt.Errorf("GetTables(include_schema=true) failed: %w", err)
	}
	if len(info.GetEndpoint()) == 0 {
		return fmt.Errorf("GetTables returned zero endpoints")
	}

	gotSchema, err := flight.DeserializeSchema(info.GetSchema(), memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("failed to deserialize FlightInfo schema: %w", err)
	}
	if !gotSchema.Equal(schema_ref.TablesWithIncludedSchema) {
		return fmt.Errorf("unexpected FlightInfo schema for include_schema")
	}

	foundAnySchema := false
	for _, ep := range info.GetEndpoint() {
		reader, err := client.DoGet(ctx, ep.GetTicket())
		if err != nil {
			return fmt.Errorf("DoGet failed: %w", err)
		}

		for reader.Next() {
			record := reader.RecordBatch()
			names, ok := record.Column(2).(*array.String)
			if !ok {
				reader.Release()
				return fmt.Errorf("unexpected table_name column type: %T", record.Column(2))
			}
			tableSchemas, ok := record.Column(4).(*array.Binary)
			if !ok {
				reader.Release()
				return fmt.Errorf("unexpected table_schema column type: %T", record.Column(4))
			}

			for i := 0; i < int(record.NumRows()); i++ {
				if names.IsNull(i) || tableSchemas.IsNull(i) {
					continue
				}
				b := tableSchemas.Value(i)
				if len(b) == 0 {
					continue
				}
				tableSchema, err := flight.DeserializeSchema(b, memory.DefaultAllocator)
				if err != nil {
					reader.Release()
					return fmt.Errorf("failed to deserialize table schema: %w", err)
				}
				if tableSchema.NumFields() == 0 {
					continue
				}
				foundAnySchema = true
			}
		}
		if err := reader.Err(); err != nil {
			reader.Release()
			return fmt.Errorf("DoGet stream error: %w", err)
		}
		reader.Release()
	}

	if !foundAnySchema {
		return fmt.Errorf("did not find any non-empty include_schema payload")
	}
	return nil
}

func TestFlightIngressIncludeSchemaLowWorkerRegression(t *testing.T) {
	h := startControlPlane(t, cpOpts{
		flightPort: freePort(t),
		maxWorkers: 3,
	})

	const goroutines = 3
	const iterationsPerGoroutine = 8
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterationsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := newFlightClient(t, h.flightPort)
			defer func() { _ = client.Close() }()

			bootstrapCtx, bootstrapCancel := context.WithTimeout(flightAuthContext("testuser", "testpass"), 20*time.Second)
			var respHeader metadata.MD
			_, bootstrapErr := client.GetTables(bootstrapCtx, &flightsql.GetTablesOpts{}, grpc.Header(&respHeader))
			bootstrapCancel()
			if bootstrapErr != nil {
				errCh <- fmt.Errorf("worker %d bootstrap failed: %w", workerID, bootstrapErr)
				return
			}
			sessionTokens := respHeader.Get("x-duckgres-session")
			if len(sessionTokens) == 0 || strings.TrimSpace(sessionTokens[0]) == "" {
				errCh <- fmt.Errorf("worker %d bootstrap missing x-duckgres-session header", workerID)
				return
			}
			token := strings.TrimSpace(sessionTokens[0])

			for i := 0; i < iterationsPerGoroutine; i++ {
				iterBaseCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
					"x-duckgres-session", token,
				))
				iterCtx, iterCancel := context.WithTimeout(iterBaseCtx, 20*time.Second)
				err := requireGetTablesIncludeSchema(t, client, iterCtx)
				iterCancel()
				if err != nil {
					errCh <- fmt.Errorf("worker %d iteration %d failed: %w", workerID, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("flight include_schema regression: %v\nLogs:\n%s", err, h.logBuf.String())
	}
}

func TestFlightIngressServerIssuedSessionTokenAllowsTokenOnlyAuth(t *testing.T) {
	h := startControlPlane(t, cpOpts{
		flightPort: freePort(t),
		maxWorkers: 1,
	})

	client1 := newFlightClient(t, h.flightPort)
	defer func() { _ = client1.Close() }()

	var respHeader metadata.MD
	ctx1, cancel1 := context.WithTimeout(flightAuthContext("testuser", "testpass"), 20*time.Second)
	defer cancel1()

	if _, err := client1.GetTables(ctx1, &flightsql.GetTablesOpts{}, grpc.Header(&respHeader)); err != nil {
		t.Fatalf("bootstrap GetTables with basic auth failed: %v", err)
	}

	sessionTokens := respHeader.Get("x-duckgres-session")
	if len(sessionTokens) == 0 || strings.TrimSpace(sessionTokens[0]) == "" {
		t.Fatalf("expected server-issued x-duckgres-session header, got %v", respHeader)
	}

	client2 := newFlightClient(t, h.flightPort)
	defer func() { _ = client2.Close() }()

	tokenCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-duckgres-session", sessionTokens[0]))
	ctx2, cancel2 := context.WithTimeout(tokenCtx, 20*time.Second)
	defer cancel2()

	if _, err := client2.GetTables(ctx2, &flightsql.GetTablesOpts{}); err != nil {
		t.Fatalf("expected token-only GetTables to succeed, got %v", err)
	}

	tokenAndAuthCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"x-duckgres-session", sessionTokens[0],
		"authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("testuser:testpass")),
	))
	ctx3, cancel3 := context.WithTimeout(tokenAndAuthCtx, 20*time.Second)
	defer cancel3()

	if _, err := client2.GetTables(ctx3, &flightsql.GetTablesOpts{}); err != nil {
		t.Fatalf("token+basic GetTables failed: %v", err)
	}
}
