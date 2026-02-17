package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// FlightSQLHandler implements Arrow Flight SQL for multiple sessions.
// Sessions are identified by the "x-duckgres-session" gRPC metadata header.
type FlightSQLHandler struct {
	flightsql.BaseServer
	pool  *SessionPool
	alloc memory.Allocator
}

// sessionFromContext extracts the session from gRPC metadata.
// The session token is expected in the "x-duckgres-session" header.
func (h *FlightSQLHandler) sessionFromContext(ctx context.Context) (*Session, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	tokens := md.Get("x-duckgres-session")
	if len(tokens) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing x-duckgres-session header")
	}

	session, ok := h.pool.GetSession(tokens[0])
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "session not found")
	}
	session.lastUsed.Store(time.Now().UnixNano())

	return session, nil
}

// Custom action handlers (called via customActionServer.DoAction)

func (h *FlightSQLHandler) doCreateSession(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Username string `json:"username"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid CreateSession request: %v", err)
	}
	if req.Username == "" {
		return status.Error(codes.InvalidArgument, "username is required")
	}

	// Validate username against configured users
	if _, ok := h.pool.cfg.Users[req.Username]; !ok {
		return status.Error(codes.PermissionDenied, "unknown username")
	}

	session, err := h.pool.CreateSession(req.Username)
	if err != nil {
		return status.Errorf(codes.ResourceExhausted, "create session: %v", err)
	}

	resp, _ := json.Marshal(map[string]string{
		"session_token": session.ID,
	})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doDestroySession(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		SessionToken string `json:"session_token"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid DestroySession request: %v", err)
	}
	if req.SessionToken == "" {
		return status.Error(codes.InvalidArgument, "session_token is required")
	}

	if err := h.pool.DestroySession(req.SessionToken); err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	resp, _ := json.Marshal(map[string]bool{"ok": true})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doHealthCheck(stream flight.FlightService_DoActionServer) error {
	resp, _ := json.Marshal(map[string]interface{}{
		"healthy":   true,
		"sessions":  h.pool.ActiveSessions(),
		"uptime_ns": time.Since(h.pool.startTime).Nanoseconds(),
	})
	return stream.Send(&flight.Result{Body: resp})
}

// Flight SQL method implementations

func (h *FlightSQLHandler) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	query := cmd.GetQuery()
	tx, txnKey, err := session.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return nil, err
	}

	schema, err := GetQuerySchema(ctx, session.DB, query, tx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	handleID := fmt.Sprintf("query-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.queries[handleID] = &QueryHandle{Query: query, Schema: schema, TxnID: txnKey}
	session.mu.Unlock()

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handleID))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", err)
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *FlightSQLHandler) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	handleID := string(ticket.GetStatementHandle())

	session.mu.RLock()
	handle, ok := session.queries[handleID]
	session.mu.RUnlock()
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "query handle not found")
	}

	var tx *sql.Tx
	if handle.TxnID != "" {
		session.mu.RLock()
		ttx := session.txns[handle.TxnID]
		session.mu.RUnlock()
		if ttx == nil || ttx.tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
		ttx.lastUsed.Store(time.Now().UnixNano())
		tx = ttx.tx
	}

	schema := handle.Schema

	ch := make(chan flight.StreamChunk, 10)
	go func() {
		defer close(ch)
		defer func() {
			session.mu.Lock()
			delete(session.queries, handleID)
			session.mu.Unlock()
		}()

		var rows *sql.Rows
		var qerr error
		if tx != nil {
			rows, qerr = tx.QueryContext(ctx, handle.Query)
		} else {
			rows, qerr = session.DB.QueryContext(ctx, handle.Query)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		for {
			record, recErr := RowsToRecord(h.alloc, rows, schema, 1024)
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

	return schema, ch, nil
}

func (h *FlightSQLHandler) DoPutCommandStatementUpdate(ctx context.Context,
	cmd flightsql.StatementUpdate) (int64, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return 0, err
	}

	tx, _, err := session.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return 0, err
	}

	query := cmd.GetQuery()
	var result sql.Result
	if tx != nil {
		result, err = tx.ExecContext(ctx, query)
	} else {
		result, err = session.DB.ExecContext(ctx, query)
	}
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "failed to execute update: %v", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return affected, nil
}

func (h *FlightSQLHandler) BeginTransaction(ctx context.Context,
	req flightsql.ActionBeginTransactionRequest) ([]byte, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	_ = req

	tx, err := session.DB.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	txnKey := fmt.Sprintf("txn-%d", session.handleCounter.Add(1))
	ttx := &trackedTx{tx: tx}
	ttx.lastUsed.Store(time.Now().UnixNano())

	session.mu.Lock()
	session.txns[txnKey] = ttx
	session.txnOwner[txnKey] = session.Username
	session.mu.Unlock()

	return []byte(txnKey), nil
}

func (h *FlightSQLHandler) EndTransaction(ctx context.Context,
	req flightsql.ActionEndTransactionRequest) error {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}

	txnKey := string(req.GetTransactionId())
	if txnKey == "" {
		return status.Error(codes.InvalidArgument, "missing transaction id")
	}

	session.mu.Lock()
	ttx, ok := session.txns[txnKey]
	if ok {
		delete(session.txns, txnKey)
		delete(session.txnOwner, txnKey)
	}
	session.mu.Unlock()

	if !ok || ttx == nil || ttx.tx == nil {
		return status.Error(codes.NotFound, "transaction not found")
	}

	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		if err := ttx.tx.Commit(); err != nil {
			return status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
		}
		return nil
	case flightsql.EndTransactionRollback:
		if err := ttx.tx.Rollback(); err != nil {
			return status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
		}
		return nil
	default:
		_ = ttx.tx.Rollback()
		return status.Error(codes.InvalidArgument, "unsupported end transaction action")
	}
}

func (h *FlightSQLHandler) CreatePreparedStatement(ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	tx, txnKey, err := session.getOpenTxn(req.GetTransactionId())
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	schema, err := GetQuerySchema(ctx, session.DB, req.GetQuery(), tx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.InvalidArgument, "failed to prepare: %v", err)
	}

	handleID := fmt.Sprintf("prep-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.queries[handleID] = &QueryHandle{Query: req.GetQuery(), Schema: schema, TxnID: txnKey}
	session.mu.Unlock()

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(handleID),
		DatasetSchema: schema,
	}, nil
}

func (h *FlightSQLHandler) ClosePreparedStatement(ctx context.Context,
	req flightsql.ActionClosePreparedStatementRequest) error {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}

	handleID := string(req.GetPreparedStatementHandle())
	session.mu.Lock()
	delete(session.queries, handleID)
	session.mu.Unlock()
	return nil
}

func (h *FlightSQLHandler) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	handleID := string(cmd.GetPreparedStatementHandle())
	session.mu.RLock()
	handle, ok := session.queries[handleID]
	session.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(handle.Schema, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(handleID)},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *FlightSQLHandler) DoGetPreparedStatement(ctx context.Context,
	cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	session.mu.RLock()
	handle, ok := session.queries[string(cmd.GetPreparedStatementHandle())]
	session.mu.RUnlock()
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "prepared statement not found")
	}

	var tx *sql.Tx
	if handle.TxnID != "" {
		session.mu.RLock()
		ttx := session.txns[handle.TxnID]
		session.mu.RUnlock()
		if ttx == nil || ttx.tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
		ttx.lastUsed.Store(time.Now().UnixNano())
		tx = ttx.tx
	}

	schema := handle.Schema

	ch := make(chan flight.StreamChunk, 10)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if tx != nil {
			rows, qerr = tx.QueryContext(ctx, handle.Query)
		} else {
			rows, qerr = session.DB.QueryContext(ctx, handle.Query)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		for {
			record, recErr := RowsToRecord(h.alloc, rows, schema, 1024)
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

	return schema, ch, nil
}

func (h *FlightSQLHandler) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	_, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema_ref.DBSchemas, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *FlightSQLHandler) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

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

	activeTx := session.getActiveTxn()

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if activeTx != nil {
			rows, qerr = activeTx.QueryContext(ctx, query, args...)
		} else {
			rows, qerr = session.DB.QueryContext(ctx, query, args...)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		builder := array.NewRecordBuilder(h.alloc, schema)
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

func (h *FlightSQLHandler) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	_, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *FlightSQLHandler) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

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

	activeTx := session.getActiveTxn()

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if activeTx != nil {
			rows, qerr = activeTx.QueryContext(ctx, query, args...)
		} else {
			rows, qerr = session.DB.QueryContext(ctx, query, args...)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		rowsOpen := true
		closeRows := func() error {
			if !rowsOpen {
				return nil
			}
			rowsOpen = false
			return rows.Close()
		}
		defer func() {
			_ = closeRows()
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
		// Close the metadata cursor before issuing schema probe queries on the
		// same DB/transaction.
		if closeErr := closeRows(); closeErr != nil {
			ch <- flight.StreamChunk{Err: closeErr}
			return
		}

		builder := array.NewRecordBuilder(h.alloc, schema)
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
				qualified := QualifyTableName(t.catalog, t.schema, t.name)
				tableSchema, schemaErr := GetQuerySchema(ctx, session.DB, "SELECT * FROM "+qualified, activeTx)
				if schemaErr != nil {
					ch <- flight.StreamChunk{Err: schemaErr}
					return
				}
				schemaBuilder.Append(flight.SerializeSchema(tableSchema, h.alloc))
			}
		}

		ch <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	}()

	return schema, ch, nil
}

// Session helpers

func (s *Session) getOpenTxn(transactionID []byte) (*sql.Tx, string, error) {
	if len(transactionID) == 0 {
		return nil, "", nil
	}
	txnKey := string(transactionID)
	s.mu.RLock()
	ttx, ok := s.txns[txnKey]
	s.mu.RUnlock()
	if !ok || ttx == nil || ttx.tx == nil {
		return nil, "", status.Error(codes.NotFound, "transaction not found")
	}
	ttx.lastUsed.Store(time.Now().UnixNano())
	return ttx.tx, txnKey, nil
}

func (s *Session) getActiveTxn() *sql.Tx {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ttx := range s.txns {
		if ttx != nil && ttx.tx != nil {
			ttx.lastUsed.Store(time.Now().UnixNano())
			return ttx.tx
		}
	}
	return nil
}
