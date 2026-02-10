package controlplane

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type FlightSessionServer struct {
	flightsql.BaseServer

	worker     *Worker
	pid        int32
	remoteAddr string

	alloc memory.Allocator

	mu       sync.RWMutex
	db       *sql.DB
	username string
	queries  map[string]*flightQueryHandle
	txns     map[string]*sql.Tx
	txnOwner map[string]string
	handleID atomic.Uint64

	flightSrv flight.Server
}

type flightQueryHandle struct {
	query  string
	schema *arrow.Schema
	txnID  string
}

func NewFlightSessionServer(worker *Worker, pid int32, remoteAddr string) *FlightSessionServer {
	s := &FlightSessionServer{
		worker:     worker,
		pid:        pid,
		remoteAddr: remoteAddr,
		alloc:      memory.DefaultAllocator,
		queries:    make(map[string]*flightQueryHandle),
		txns:       make(map[string]*sql.Tx),
		txnOwner:   make(map[string]string),
	}

	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "duckgres"); err != nil {
		panic(fmt.Sprintf("register sql info server name: %v", err))
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "1.0.0"); err != nil {
		panic(fmt.Sprintf("register sql info server version: %v", err))
	}
	if err := s.RegisterSqlInfo(flightsql.SqlInfoTransactionsSupported, true); err != nil {
		panic(fmt.Sprintf("register sql info transactions supported: %v", err))
	}

	return s
}

func (s *FlightSessionServer) Serve(listener net.Listener, tlsConfig *tls.Config) error {
	s.flightSrv = flight.NewServerWithMiddleware(nil, grpc.Creds(credentials.NewTLS(tlsConfig)))
	s.flightSrv.RegisterFlightService(flightsql.NewFlightServer(s))
	s.flightSrv.InitListener(listener)
	return s.flightSrv.Serve()
}

func (s *FlightSessionServer) Close() {
	if s.flightSrv != nil {
		s.flightSrv.Shutdown()
	}

	s.mu.Lock()
	for id, tx := range s.txns {
		_ = tx.Rollback()
		delete(s.txns, id)
		delete(s.txnOwner, id)
	}
	s.db = nil
	s.mu.Unlock()

	s.worker.dbPool.CloseSession(s.pid)
}

func (s *FlightSessionServer) authAndDB(ctx context.Context) (string, *sql.DB, error) {
	username, password, err := extractBasicCredentials(ctx)
	if err != nil {
		return "", nil, err
	}

	s.worker.mu.RLock()
	expected, ok := s.worker.cfg.Users[username]
	s.worker.mu.RUnlock()
	if !ok || expected != password {
		return "", nil, status.Error(codes.Unauthenticated, "invalid username or password")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.username != "" && s.username != username {
		return "", nil, status.Error(codes.PermissionDenied, "single Flight connection cannot switch users")
	}

	if s.db == nil {
		db, err := s.worker.dbPool.CreateSession(s.pid, username)
		if err != nil {
			return "", nil, status.Errorf(codes.Internal, "open database: %v", err)
		}
		s.db = db
		s.username = username
	}

	return username, s.db, nil
}

func extractBasicCredentials(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	auth := authHeaders[0]
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
		return "", "", status.Error(codes.Unauthenticated, "expected Basic authorization")
	}

	raw, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", status.Error(codes.Unauthenticated, "invalid basic auth encoding")
	}

	creds := string(raw)
	sep := strings.IndexByte(creds, ':')
	if sep <= 0 {
		return "", "", status.Error(codes.Unauthenticated, "invalid basic auth payload")
	}

	username := creds[:sep]
	password := creds[sep+1:]
	if username == "" || password == "" {
		return "", "", status.Error(codes.Unauthenticated, "invalid basic auth credentials")
	}
	return username, password, nil
}

func (s *FlightSessionServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, db, err := s.authAndDB(ctx)
	if err != nil {
		return nil, err
	}

	query := cmd.GetQuery()
	tx, txnKey, err := s.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return nil, err
	}

	schema, err := s.getQuerySchema(ctx, db, query, tx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	handleID := fmt.Sprintf("query-%d", s.handleID.Add(1))
	s.mu.Lock()
	s.queries[handleID] = &flightQueryHandle{query: query, schema: schema, txnID: txnKey}
	s.mu.Unlock()

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handleID))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", err)
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (s *FlightSessionServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {
	_, db, err := s.authAndDB(ctx)
	if err != nil {
		return nil, nil, err
	}

	handleID := string(ticket.GetStatementHandle())

	s.mu.RLock()
	handle, ok := s.queries[handleID]
	s.mu.RUnlock()
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "query handle not found")
	}

	var tx *sql.Tx
	if handle.txnID != "" {
		s.mu.RLock()
		tx = s.txns[handle.txnID]
		s.mu.RUnlock()
		if tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
	}

	ch := make(chan flight.StreamChunk, 10)
	go func() {
		defer close(ch)
		defer func() {
			s.mu.Lock()
			delete(s.queries, handleID)
			s.mu.Unlock()
		}()

		var rows *sql.Rows
		if tx != nil {
			rows, err = tx.QueryContext(ctx, handle.query)
		} else {
			rows, err = db.QueryContext(ctx, handle.query)
		}
		if err != nil {
			ch <- flight.StreamChunk{Err: err}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		s.worker.totalQueries.Add(1)
		for {
			record, recErr := s.rowsToRecord(rows, handle.schema, 1024)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				break
			}
			ch <- flight.StreamChunk{Data: record}
		}
	}()

	return handle.schema, ch, nil
}

func (s *FlightSessionServer) DoPutCommandStatementUpdate(ctx context.Context,
	cmd flightsql.StatementUpdate) (int64, error) {
	_, db, err := s.authAndDB(ctx)
	if err != nil {
		return 0, err
	}

	tx, _, err := s.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return 0, err
	}

	query := cmd.GetQuery()
	var result sql.Result
	if tx != nil {
		result, err = tx.ExecContext(ctx, query)
	} else {
		result, err = db.ExecContext(ctx, query)
	}
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "failed to execute update: %v", err)
	}
	s.worker.totalQueries.Add(1)

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return affected, nil
}

func (s *FlightSessionServer) BeginTransaction(ctx context.Context,
	req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	username, db, err := s.authAndDB(ctx)
	if err != nil {
		return nil, err
	}
	_ = req

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	txnKey := fmt.Sprintf("txn-%d", s.handleID.Add(1))
	s.mu.Lock()
	s.txns[txnKey] = tx
	s.txnOwner[txnKey] = username
	s.mu.Unlock()

	return []byte(txnKey), nil
}

func (s *FlightSessionServer) EndTransaction(ctx context.Context,
	req flightsql.ActionEndTransactionRequest) error {
	username, _, err := s.authAndDB(ctx)
	if err != nil {
		return err
	}

	txnKey := string(req.GetTransactionId())
	if txnKey == "" {
		return status.Error(codes.InvalidArgument, "missing transaction id")
	}

	s.mu.Lock()
	tx, ok := s.txns[txnKey]
	owner := s.txnOwner[txnKey]
	if ok {
		delete(s.txns, txnKey)
		delete(s.txnOwner, txnKey)
	}
	s.mu.Unlock()

	if !ok || tx == nil {
		return status.Error(codes.NotFound, "transaction not found")
	}
	if owner != username {
		_ = tx.Rollback()
		return status.Error(codes.PermissionDenied, "transaction does not belong to authenticated user")
	}

	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		if err := tx.Commit(); err != nil {
			return status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
		}
		return nil
	case flightsql.EndTransactionRollback:
		if err := tx.Rollback(); err != nil {
			return status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
		}
		return nil
	default:
		_ = tx.Rollback()
		return status.Error(codes.InvalidArgument, "unsupported end transaction action")
	}
}

func (s *FlightSessionServer) CreatePreparedStatement(ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	_, db, err := s.authAndDB(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	tx, txnKey, err := s.getOpenTxn(req.GetTransactionId())
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	schema, err := s.getQuerySchema(ctx, db, req.GetQuery(), tx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.InvalidArgument, "failed to prepare: %v", err)
	}

	handleID := fmt.Sprintf("prep-%d", s.handleID.Add(1))
	s.mu.Lock()
	s.queries[handleID] = &flightQueryHandle{query: req.GetQuery(), schema: schema, txnID: txnKey}
	s.mu.Unlock()

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(handleID),
		DatasetSchema: schema,
	}, nil
}

func (s *FlightSessionServer) ClosePreparedStatement(ctx context.Context,
	req flightsql.ActionClosePreparedStatementRequest) error {
	_, _, err := s.authAndDB(ctx)
	if err != nil {
		return err
	}

	handleID := string(req.GetPreparedStatementHandle())
	s.mu.Lock()
	delete(s.queries, handleID)
	s.mu.Unlock()
	return nil
}

func (s *FlightSessionServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, _, err := s.authAndDB(ctx)
	if err != nil {
		return nil, err
	}

	handleID := string(cmd.GetPreparedStatementHandle())
	s.mu.RLock()
	handle, ok := s.queries[handleID]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(handle.schema, s.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(handleID)},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (s *FlightSessionServer) DoGetPreparedStatement(ctx context.Context,
	cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	_, db, err := s.authAndDB(ctx)
	if err != nil {
		return nil, nil, err
	}

	s.mu.RLock()
	handle, ok := s.queries[string(cmd.GetPreparedStatementHandle())]
	s.mu.RUnlock()
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "prepared statement not found")
	}

	var tx *sql.Tx
	if handle.txnID != "" {
		s.mu.RLock()
		tx = s.txns[handle.txnID]
		s.mu.RUnlock()
		if tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
	}

	ch := make(chan flight.StreamChunk, 10)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		if tx != nil {
			rows, err = tx.QueryContext(ctx, handle.query)
		} else {
			rows, err = db.QueryContext(ctx, handle.query)
		}
		if err != nil {
			ch <- flight.StreamChunk{Err: err}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		s.worker.totalQueries.Add(1)
		for {
			record, recErr := s.rowsToRecord(rows, handle.schema, 1024)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				break
			}
			ch <- flight.StreamChunk{Data: record}
		}
	}()

	return handle.schema, ch, nil
}

func (s *FlightSessionServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, _, err := s.authAndDB(ctx)
	if err != nil {
		return nil, err
	}
	_ = cmd

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema_ref.DBSchemas, s.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (s *FlightSessionServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {
	username, db, err := s.authAndDB(ctx)
	if err != nil {
		return nil, nil, err
	}
	activeTx := s.getActiveTxnForUser(username)

	schema := schema_ref.DBSchemas
	query := "SELECT catalog_name, schema_name AS db_schema_name FROM information_schema.schemata WHERE 1=1"
	args := make([]any, 0, 2)

	if catalog := cmd.GetCatalog(); catalog != nil && *catalog != "" {
		query += " AND catalog_name = ?"
		args = append(args, *catalog)
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		query += " AND schema_name LIKE ?"
		args = append(args, *pattern)
	}
	query += " ORDER BY catalog_name, db_schema_name"

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if activeTx != nil {
			rows, qerr = activeTx.QueryContext(ctx, query, args...)
		} else {
			rows, qerr = db.QueryContext(ctx, query, args...)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		builder := array.NewRecordBuilder(s.alloc, schema)
		defer builder.Release()
		catalogBuilder := builder.Field(0).(*array.StringBuilder)
		schemaBuilder := builder.Field(1).(*array.StringBuilder)

		for rows.Next() {
			var catalog sql.NullString
			var schemaName sql.NullString
			if scanErr := rows.Scan(&catalog, &schemaName); scanErr != nil {
				ch <- flight.StreamChunk{Err: scanErr}
				return
			}
			if catalog.Valid {
				catalogBuilder.Append(catalog.String)
			} else {
				catalogBuilder.AppendNull()
			}
			if schemaName.Valid {
				schemaBuilder.Append(schemaName.String)
			} else {
				schemaBuilder.AppendNull()
			}
		}
		if rowErr := rows.Err(); rowErr != nil {
			ch <- flight.StreamChunk{Err: rowErr}
			return
		}
		ch <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	}()

	return schema, ch, nil
}

func (s *FlightSessionServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, _, err := s.authAndDB(ctx)
	if err != nil {
		return nil, err
	}

	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (s *FlightSessionServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {
	username, db, err := s.authAndDB(ctx)
	if err != nil {
		return nil, nil, err
	}
	activeTx := s.getActiveTxnForUser(username)

	schema := schema_ref.Tables
	includeSchema := cmd.GetIncludeSchema()
	if includeSchema {
		schema = schema_ref.TablesWithIncludedSchema
	}

	query := "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE 1=1"
	args := make([]any, 0, 6)
	if catalog := cmd.GetCatalog(); catalog != nil && *catalog != "" {
		query += " AND table_catalog = ?"
		args = append(args, *catalog)
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		query += " AND table_schema LIKE ?"
		args = append(args, *pattern)
	}
	if pattern := cmd.GetTableNameFilterPattern(); pattern != nil && *pattern != "" {
		query += " AND table_name LIKE ?"
		args = append(args, *pattern)
	}
	if tableTypes := cmd.GetTableTypes(); len(tableTypes) > 0 {
		placeholders := make([]string, 0, len(tableTypes))
		for range tableTypes {
			placeholders = append(placeholders, "?")
		}
		query += " AND table_type IN (" + strings.Join(placeholders, ", ") + ")"
		for _, t := range tableTypes {
			args = append(args, t)
		}
	}
	query += " ORDER BY table_catalog, table_schema, table_name"

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if activeTx != nil {
			rows, qerr = activeTx.QueryContext(ctx, query, args...)
		} else {
			rows, qerr = db.QueryContext(ctx, query, args...)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		type tableInfo struct {
			catalog   sql.NullString
			schema    sql.NullString
			name      string
			tableType string
		}
		tables := make([]tableInfo, 0)
		for rows.Next() {
			var t tableInfo
			if scanErr := rows.Scan(&t.catalog, &t.schema, &t.name, &t.tableType); scanErr != nil {
				ch <- flight.StreamChunk{Err: scanErr}
				return
			}
			tables = append(tables, t)
		}
		if rowErr := rows.Err(); rowErr != nil {
			ch <- flight.StreamChunk{Err: rowErr}
			return
		}

		builder := array.NewRecordBuilder(s.alloc, schema)
		defer builder.Release()

		var schemaBuilder *array.BinaryBuilder
		if includeSchema {
			schemaBuilder = builder.Field(4).(*array.BinaryBuilder)
		}

		for _, t := range tables {
			if t.catalog.Valid {
				builder.Field(0).(*array.StringBuilder).Append(t.catalog.String)
			} else {
				builder.Field(0).(*array.StringBuilder).AppendNull()
			}
			if t.schema.Valid {
				builder.Field(1).(*array.StringBuilder).Append(t.schema.String)
			} else {
				builder.Field(1).(*array.StringBuilder).AppendNull()
			}
			builder.Field(2).(*array.StringBuilder).Append(t.name)
			builder.Field(3).(*array.StringBuilder).Append(t.tableType)

			if includeSchema {
				qualified := qualifyTableName(t.catalog, t.schema, t.name)
				tableSchema, schemaErr := s.getQuerySchema(ctx, db, "SELECT * FROM "+qualified, activeTx)
				if schemaErr != nil {
					ch <- flight.StreamChunk{Err: schemaErr}
					return
				}
				schemaBuilder.Append(flight.SerializeSchema(tableSchema, s.alloc))
			}
		}

		ch <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	}()

	return schema, ch, nil
}

func (s *FlightSessionServer) getOpenTxn(transactionID []byte) (*sql.Tx, string, error) {
	if len(transactionID) == 0 {
		return nil, "", nil
	}
	txnKey := string(transactionID)
	s.mu.RLock()
	tx, ok := s.txns[txnKey]
	s.mu.RUnlock()
	if !ok || tx == nil {
		return nil, "", status.Error(codes.NotFound, "transaction not found")
	}
	return tx, txnKey, nil
}

func (s *FlightSessionServer) getActiveTxnForUser(username string) *sql.Tx {
	if username == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for txnKey, owner := range s.txnOwner {
		if owner != username {
			continue
		}
		if tx, ok := s.txns[txnKey]; ok && tx != nil {
			return tx
		}
	}
	return nil
}

func (s *FlightSessionServer) getQuerySchema(ctx context.Context, db *sql.DB, query string, tx *sql.Tx) (*arrow.Schema, error) {
	queryWithLimit := query + " LIMIT 0"
	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, queryWithLimit)
	} else {
		rows, err = db.QueryContext(ctx, queryWithLimit)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		fields[i] = arrow.Field{Name: ct.Name(), Type: duckDBTypeToArrow(ct.DatabaseTypeName()), Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

func (s *FlightSessionServer) rowsToRecord(rows *sql.Rows, schema *arrow.Schema, batchSize int) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(s.alloc, schema)
	defer builder.Release()

	numFields := schema.NumFields()
	count := 0
	for rows.Next() && count < batchSize {
		values := make([]interface{}, numFields)
		valuePtrs := make([]interface{}, numFields)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, val := range values {
			appendValue(builder.Field(i), val)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return builder.NewRecordBatch(), nil
}

func duckDBTypeToArrow(dbType string) arrow.DataType {
	dbType = strings.ToUpper(dbType)
	switch {
	case strings.Contains(dbType, "INT64"), strings.Contains(dbType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(dbType, "INT32"), strings.Contains(dbType, "INTEGER"):
		return arrow.PrimitiveTypes.Int32
	case strings.Contains(dbType, "INT16"), strings.Contains(dbType, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16
	case strings.Contains(dbType, "INT8"), strings.Contains(dbType, "TINYINT"):
		return arrow.PrimitiveTypes.Int8
	case strings.Contains(dbType, "DOUBLE"), strings.Contains(dbType, "FLOAT8"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "REAL"):
		return arrow.PrimitiveTypes.Float32
	case strings.Contains(dbType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(dbType, "VARCHAR"), strings.Contains(dbType, "TEXT"), strings.Contains(dbType, "STRING"):
		return arrow.BinaryTypes.String
	case strings.Contains(dbType, "BLOB"):
		return arrow.BinaryTypes.Binary
	case strings.Contains(dbType, "DATE"):
		return arrow.FixedWidthTypes.Date32
	case strings.Contains(dbType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.BinaryTypes.String
	}
}

func appendValue(builder array.Builder, val interface{}) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch v := val.(type) {
		case int32:
			b.Append(v)
		case int64:
			b.Append(int32(v))
		case int:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			b.Append(v)
		case int32:
			b.Append(int16(v))
		default:
			b.AppendNull()
		}
	case *array.Int8Builder:
		switch v := val.(type) {
		case int8:
			b.Append(v)
		case int32:
			b.Append(int8(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := val.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

func qualifyTableName(catalog, schema sql.NullString, table string) string {
	parts := make([]string, 0, 3)
	if catalog.Valid {
		parts = append(parts, quoteIdent(catalog.String))
	}
	if schema.Valid {
		parts = append(parts, quoteIdent(schema.String))
	}
	parts = append(parts, quoteIdent(table))
	return strings.Join(parts, ".")
}

func quoteIdent(ident string) string {
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	return `"` + escaped + `"`
}
