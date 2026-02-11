package duckdbservice

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FlightSQLHandler implements Arrow Flight SQL for multiple sessions.
// Sessions are identified by the "x-duckgres-session" gRPC metadata header.
type FlightSQLHandler struct {
	flightsql.BaseServer
	pool  *SessionPool
	alloc memory.Allocator
}

func (h *FlightSQLHandler) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "%v", err)
	}

	query := cmd.GetQuery()
	tx, txnKey, err := session.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return nil, err
	}

	schema, err := getQuerySchema(ctx, session.DB, query, tx, h.alloc)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	handleID := fmt.Sprintf("query-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.queries[handleID] = &QueryHandle{Query: query, TxnID: txnKey}
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
		return nil, nil, status.Errorf(codes.Unauthenticated, "%v", err)
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
		tx = session.txns[handle.TxnID]
		session.mu.RUnlock()
		if tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
	}

	schema, err := getQuerySchema(ctx, session.DB, handle.Query, tx, h.alloc)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "failed to get query schema: %v", err)
	}

	ch := make(chan flight.StreamChunk, 10)
	go func() {
		defer close(ch)
		defer func() {
			session.mu.Lock()
			delete(session.queries, handleID)
			session.mu.Unlock()
		}()

		var rows *sql.Rows
		if tx != nil {
			rows, err = tx.QueryContext(ctx, handle.Query)
		} else {
			rows, err = session.DB.QueryContext(ctx, handle.Query)
		}
		if err != nil {
			ch <- flight.StreamChunk{Err: err}
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
		return 0, status.Errorf(codes.Unauthenticated, "%v", err)
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
		return nil, status.Errorf(codes.Unauthenticated, "%v", err)
	}
	_ = req

	tx, err := session.DB.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	txnKey := fmt.Sprintf("txn-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.txns[txnKey] = tx
	session.txnOwner[txnKey] = session.Username
	session.mu.Unlock()

	return []byte(txnKey), nil
}

func (h *FlightSQLHandler) EndTransaction(ctx context.Context,
	req flightsql.ActionEndTransactionRequest) error {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "%v", err)
	}

	txnKey := string(req.GetTransactionId())
	if txnKey == "" {
		return status.Error(codes.InvalidArgument, "missing transaction id")
	}

	session.mu.Lock()
	tx, ok := session.txns[txnKey]
	if ok {
		delete(session.txns, txnKey)
		delete(session.txnOwner, txnKey)
	}
	session.mu.Unlock()

	if !ok || tx == nil {
		return status.Error(codes.NotFound, "transaction not found")
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

func (h *FlightSQLHandler) CreatePreparedStatement(ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.Unauthenticated, "%v", err)
	}

	tx, txnKey, err := session.getOpenTxn(req.GetTransactionId())
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	schema, err := getQuerySchema(ctx, session.DB, req.GetQuery(), tx, h.alloc)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.InvalidArgument, "failed to prepare: %v", err)
	}

	handleID := fmt.Sprintf("prep-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.queries[handleID] = &QueryHandle{Query: req.GetQuery(), TxnID: txnKey}
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
		return status.Errorf(codes.Unauthenticated, "%v", err)
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
		return nil, status.Errorf(codes.Unauthenticated, "%v", err)
	}

	handleID := string(cmd.GetPreparedStatementHandle())
	session.mu.RLock()
	handle, ok := session.queries[handleID]
	session.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "prepared statement not found")
	}

	tx, _, err := session.getOpenTxn([]byte(handle.TxnID))
	if err != nil {
		return nil, err
	}

	schema, err := getQuerySchema(ctx, session.DB, handle.Query, tx, h.alloc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, h.alloc),
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
		return nil, nil, status.Errorf(codes.Unauthenticated, "%v", err)
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
		tx = session.txns[handle.TxnID]
		session.mu.RUnlock()
		if tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
	}

	schema, err := getQuerySchema(ctx, session.DB, handle.Query, tx, h.alloc)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "failed to get query schema: %v", err)
	}

	ch := make(chan flight.StreamChunk, 10)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		if tx != nil {
			rows, err = tx.QueryContext(ctx, handle.Query)
		} else {
			rows, err = session.DB.QueryContext(ctx, handle.Query)
		}
		if err != nil {
			ch <- flight.StreamChunk{Err: err}
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
		return nil, status.Errorf(codes.Unauthenticated, "%v", err)
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
		return nil, nil, status.Errorf(codes.Unauthenticated, "%v", err)
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
		return nil, status.Errorf(codes.Unauthenticated, "%v", err)
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
		return nil, nil, status.Errorf(codes.Unauthenticated, "%v", err)
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
				tableSchema, schemaErr := getQuerySchema(ctx, session.DB, "SELECT * FROM "+qualified, activeTx, h.alloc)
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
	tx, ok := s.txns[txnKey]
	s.mu.RUnlock()
	if !ok || tx == nil {
		return nil, "", status.Error(codes.NotFound, "transaction not found")
	}
	return tx, txnKey, nil
}

func (s *Session) getActiveTxn() *sql.Tx {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, tx := range s.txns {
		if tx != nil {
			return tx
		}
	}
	return nil
}

// getQuerySchema executes a query with LIMIT 0 to discover the result schema.
func getQuerySchema(ctx context.Context, db *sql.DB, query string, tx *sql.Tx, alloc memory.Allocator) (*arrow.Schema, error) {
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
		fields[i] = arrow.Field{Name: ct.Name(), Type: DuckDBTypeToArrow(ct.DatabaseTypeName()), Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}
