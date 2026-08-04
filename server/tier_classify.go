package server

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// statementTier says whether a statement may run on the exploratory small
// worker or must pin the connection to a normal-size worker.
//
// Classification is deliberately conservative: only statements provably free
// of session-state mutation and writes are tierSmallOK. A false pin merely
// costs a bigger pod; a false smallOK would let state accumulate on a worker
// the connection can silently migrate away from — a correctness bug. Anything
// pg_query cannot parse (DuckDB-only spellings like USE / CREATE SECRET,
// garbage) pins.
type statementTier int

const (
	tierSmallOK statementTier = iota
	tierPinning
)

func classifyStatementTier(sql string) statementTier {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return tierPinning
	}
	for _, raw := range tree.Stmts {
		if stmtTier(raw.Stmt) == tierPinning {
			return tierPinning
		}
	}
	return tierSmallOK
}

func stmtTier(node *pg_query.Node) statementTier {
	if node == nil {
		return tierPinning
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		if n.SelectStmt.GetIntoClause() != nil {
			return tierPinning // SELECT INTO creates a table
		}
		if containsMutatingNode(node.ProtoReflect()) {
			return tierPinning // writable CTE (WITH x AS (INSERT ...))
		}
		return tierSmallOK
	case *pg_query.Node_ExplainStmt:
		// EXPLAIN ANALYZE executes the inner statement, so the inner
		// statement's tier governs regardless of the ANALYZE flag.
		return stmtTier(n.ExplainStmt.GetQuery())
	case *pg_query.Node_VariableShowStmt:
		return tierSmallOK
	default:
		// Everything else — DML, DDL, COPY, SET, BEGIN, DECLARE, PREPARE,
		// VACUUM, ... — either writes or creates session state.
		return tierPinning
	}
}

// containsMutatingNode walks a statement's proto tree looking for embedded
// DML (the writable-CTE case). Same protoreflect walk idiom as
// query_access.go's walkMessage.
func containsMutatingNode(msg protoreflect.Message) bool {
	switch msg.Interface().(type) {
	case *pg_query.InsertStmt, *pg_query.UpdateStmt, *pg_query.DeleteStmt, *pg_query.MergeStmt:
		return true
	}
	found := false
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				if containsMutatingNode(l.Get(i).Message()) {
					found = true
					return false
				}
			}
		case fd.Kind() == protoreflect.MessageKind && !fd.IsMap():
			if containsMutatingNode(v.Message()) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
