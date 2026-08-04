// Package querymeta extracts what an inbound statement touches — catalogs,
// schemas, relations, columns, functions — and what class of access it is.
//
// It exists for two consumers. Today the query log records the extraction, so
// operators can answer "who is scanning this table" and so a candidate
// authorization policy can be evaluated offline against real traffic before it
// denies anything. Tomorrow the same Metadata is the input to that policy.
//
// That second consumer sets the bar. An authorization decision derived from
// this package must never be able to read "no relations referenced" from a
// statement we merely failed to parse, so every result carries Complete and
// IncompleteReason, and callers that gate on it must treat incomplete as deny.
// Empty-because-nothing-was-touched and empty-because-we-failed are different
// answers and are never conflated.
package querymeta

import (
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// AccessKind classifies a statement by the privilege it would require, rather
// than by its syntax. A statement can be more than one: a writable CTE both
// reads and writes.
type AccessKind string

const (
	// AccessRead reads tenant data.
	AccessRead AccessKind = "read"
	// AccessWrite mutates tenant data.
	AccessWrite AccessKind = "write"
	// AccessDDL changes schema.
	AccessDDL AccessKind = "ddl"
	// AccessConfig changes session or global settings.
	AccessConfig AccessKind = "config"
	// AccessAdmin changes the security or storage envelope: secrets, ATTACH,
	// extension installation, or moving tenant data OUT to an external URI.
	// Reading an external location is NOT admin — pointing read_parquet at your
	// own bucket is supported usage, and the cross-tenant question there is
	// whether the path resolves into managed DuckLake storage, which a policy
	// answers from TableFunction.Args.
	AccessAdmin AccessKind = "admin"
	// AccessTransaction is transaction control.
	AccessTransaction AccessKind = "transaction"
	// AccessMetadata is catalog introspection only (pg_catalog,
	// information_schema). Policies almost always treat this differently from
	// reading tenant data.
	AccessMetadata AccessKind = "metadata"
	// AccessUnknown means the statement's class could not be determined. It is
	// what a future gate denies on; it must never be silently dropped.
	AccessUnknown AccessKind = "unknown"
)

// Query kinds, matching ClickHouse's query_kind vocabulary where one exists.
const (
	KindSelect      = "Select"
	KindInsert      = "Insert"
	KindUpdate      = "Update"
	KindDelete      = "Delete"
	KindCreate      = "Create"
	KindDrop        = "Drop"
	KindAlter       = "Alter"
	KindCopy        = "Copy"
	KindExplain     = "Explain"
	KindSet         = "Set"
	KindShow        = "Show"
	KindTransaction = "Transaction"
	KindOther       = "Other"
)

// Relation is a referenced table, view, or catalog object.
//
// Raw preserves what the user actually wrote. Catalog and Schema are filled
// only when the statement qualified them: this package does not resolve a
// search_path it cannot see, and a guessed schema in an audit trail is worse
// than an honest blank.
type Relation struct {
	Catalog string `json:"catalog,omitempty"`
	Schema  string `json:"schema,omitempty"`
	Name    string `json:"name"`
	Raw     string `json:"raw"`
}

// Column is a referenced column. Relation is the qualifier as written, empty
// when the reference was unqualified.
type Column struct {
	Relation string `json:"relation,omitempty"`
	Name     string `json:"name"`
}

// TableFunction is a function used as a data source — read_parquet, read_csv,
// postgres_scan, glob. These reach data without naming a relation, so a policy
// that only looks at relation names cannot see them at all.
//
// Reading an external location is ordinary, permitted work: a tenant pointing
// read_parquet at their own bucket is a feature. What a policy needs to decide
// is whether the TARGET resolves inside the warehouse's managed DuckLake
// storage, which is how one tenant would reach another's data. That decision is
// a path check, so Args preserves enough of the target to make it — scheme,
// host, and path — while dropping the parts that carry credentials.
type TableFunction struct {
	Name string `json:"name"`
	// Args holds sanitized string arguments: a presigned URL's query string
	// carries credentials, so only scheme, host, and path survive.
	Args []string `json:"args,omitempty"`
	// External marks a function that reads from a location rather than a
	// relation, i.e. one whose Args are a path a policy should resolve.
	External bool `json:"external,omitempty"`
}

// Metadata is the extraction result for one inbound statement.
type Metadata struct {
	QueryKind      string          `json:"query_kind"`
	AccessKinds    []AccessKind    `json:"access_kinds"`
	ReadRelations  []Relation      `json:"read_relations,omitempty"`
	WriteRelations []Relation      `json:"write_relations,omitempty"`
	Columns        []Column        `json:"columns,omitempty"`
	Functions      []string        `json:"functions,omitempty"`
	TableFunctions []TableFunction `json:"table_functions,omitempty"`

	// ColumnsResolved reports whether every referenced column could be
	// attributed to a relation. Unqualified columns with more than one relation
	// in scope cannot be, without a catalog. A column-level policy must treat
	// false as "requires catalog resolution", which means deny.
	ColumnsResolved bool `json:"columns_resolved"`
	// SelectStar reports a `*` or `t.*` projection: every column of the
	// relations in scope.
	SelectStar bool `json:"select_star,omitempty"`
	// StatementCount is the number of top-level statements extracted.
	StatementCount int `json:"statement_count,omitempty"`

	// Complete reports whether extraction saw the whole statement. False means
	// the lists below are a floor, not the truth.
	Complete bool `json:"complete"`
	// IncompleteReason explains a false Complete.
	IncompleteReason string `json:"incomplete_reason,omitempty"`
}

// maxCollected bounds each list so one pathological statement cannot write an
// unbounded row into the query log. Hitting it marks the result incomplete —
// a truncated list must not read as an exhaustive one.
const maxCollected = 64

type extractor struct {
	meta        Metadata
	kinds       map[AccessKind]struct{}
	readSeen    map[string]struct{}
	writeSeen   map[string]struct{}
	columnSeen  map[string]struct{}
	funcSeen    map[string]struct{}
	tableFnSeen map[string]struct{}
	truncated   bool
	// relationsInScope counts relations visible to the current statement, used
	// to decide whether an unqualified column can be attributed.
	relationsInScope int
	unqualifiedCols  bool
}

// Extract parses a statement and reports what it touches.
//
// A parse failure is not an error: DuckDB-native syntax (ATTACH, CREATE SECRET,
// PIVOT, DESCRIBE) is valid here and unparseable as PostgreSQL. Those fall back
// to a lexical classification that is deliberately coarse, and the result is
// marked incomplete so no caller mistakes it for a full picture.
func Extract(sql string) Metadata {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return lexicalFallback(sql, "parse_failed")
	}
	return ExtractTree(sql, tree)
}

// ExtractTree reports what an already-parsed statement touches. The transpiler
// parses every Tier-1 statement, so callers holding a tree can extract without
// paying for a second parse.
func ExtractTree(sql string, tree *pg_query.ParseResult) Metadata {
	if tree == nil || len(tree.Stmts) == 0 {
		return lexicalFallback(sql, "empty_parse")
	}

	e := &extractor{
		kinds:       map[AccessKind]struct{}{},
		readSeen:    map[string]struct{}{},
		writeSeen:   map[string]struct{}{},
		columnSeen:  map[string]struct{}{},
		funcSeen:    map[string]struct{}{},
		tableFnSeen: map[string]struct{}{},
	}
	e.meta.Complete = true
	e.meta.StatementCount = len(tree.Stmts)

	for _, raw := range tree.Stmts {
		if raw == nil || raw.Stmt == nil {
			continue
		}
		e.statement(raw.Stmt, nil)
	}

	e.finish()
	return e.meta
}

// lexicalFallback classifies a statement we could not parse. It covers the
// DuckDB-only statements that matter most for authorization — secret DDL,
// ATTACH, extension loading — because those are exactly the ones a PostgreSQL
// parser rejects, and treating them as "unknown, nothing touched" would be the
// worst possible answer for an audit trail.
func lexicalFallback(sql, reason string) Metadata {
	meta := Metadata{
		QueryKind:        KindOther,
		Complete:         false,
		IncompleteReason: reason,
		StatementCount:   1,
	}
	keyword := leadingKeyword(sql)
	upper := strings.ToUpper(sql)

	switch {
	case containsSecretDDL(upper), strings.HasPrefix(keyword, "ATTACH"), strings.HasPrefix(keyword, "DETACH"),
		keyword == "INSTALL", keyword == "LOAD":
		meta.AccessKinds = []AccessKind{AccessAdmin}
		meta.QueryKind = KindOther
	case keyword == "SELECT", keyword == "WITH", keyword == "TABLE", keyword == "VALUES", keyword == "FROM":
		meta.AccessKinds = []AccessKind{AccessRead}
		meta.QueryKind = KindSelect
	case keyword == "DESCRIBE", keyword == "SUMMARIZE", keyword == "SHOW":
		meta.AccessKinds = []AccessKind{AccessMetadata}
		meta.QueryKind = KindShow
	case keyword == "SET" || keyword == "RESET":
		meta.AccessKinds = []AccessKind{AccessConfig}
		meta.QueryKind = KindSet
	case keyword == "INSERT", keyword == "UPDATE", keyword == "DELETE", keyword == "MERGE":
		meta.AccessKinds = []AccessKind{AccessWrite}
		meta.QueryKind = KindOther
	case keyword == "CREATE", keyword == "DROP", keyword == "ALTER", keyword == "TRUNCATE":
		meta.AccessKinds = []AccessKind{AccessDDL}
		meta.QueryKind = KindOther
	case keyword == "COPY":
		// COPY can read or write, and COPY TO an external URI is an admin-class
		// egress. Without a parse we cannot tell, so claim the union rather
		// than guess low.
		meta.AccessKinds = []AccessKind{AccessRead, AccessWrite, AccessAdmin}
		meta.QueryKind = KindCopy
	default:
		meta.AccessKinds = []AccessKind{AccessUnknown}
	}
	return meta
}

func containsSecretDDL(upper string) bool {
	return strings.Contains(upper, " SECRET ") || strings.HasSuffix(upper, " SECRET") ||
		strings.Contains(upper, " SECRET(")
}

func (e *extractor) addKind(kinds ...AccessKind) {
	for _, kind := range kinds {
		e.kinds[kind] = struct{}{}
	}
}

func (e *extractor) setKind(kind string) {
	if e.meta.QueryKind == "" {
		e.meta.QueryKind = kind
	} else if e.meta.QueryKind != kind {
		// A multi-statement string of mixed kinds has no single kind.
		e.meta.QueryKind = KindOther
	}
}

// statement dispatches on the statement node. Relations reached here are
// classified by role (read target vs write target), which a blind walk cannot
// do — grants are directional, so the split has to happen where the context is
// still known.
func (e *extractor) statement(node *pg_query.Node, cte map[string]struct{}) {
	if node == nil {
		return
	}
	switch stmt := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		e.setKind(KindSelect)
		e.addKind(AccessRead)
		e.selectStmt(stmt.SelectStmt, cte)

	case *pg_query.Node_InsertStmt:
		e.setKind(KindInsert)
		e.addKind(AccessWrite)
		scope := e.withCTEs(stmt.InsertStmt.WithClause, cte)
		e.writeRelation(stmt.InsertStmt.Relation, scope)
		for _, col := range stmt.InsertStmt.Cols {
			if target := col.GetResTarget(); target != nil && target.Name != "" {
				e.addColumn(relationName(stmt.InsertStmt.Relation), target.Name)
			}
		}
		if stmt.InsertStmt.SelectStmt != nil {
			e.addKind(AccessRead)
			e.read(stmt.InsertStmt.SelectStmt, scope)
		}
		e.readNodes(scope, stmt.InsertStmt.ReturningList...)
		if clause := stmt.InsertStmt.OnConflictClause; clause != nil {
			e.readNodes(scope, clause.TargetList...)
			e.read(clause.WhereClause, scope)
		}

	case *pg_query.Node_UpdateStmt:
		e.setKind(KindUpdate)
		e.addKind(AccessWrite, AccessRead)
		scope := e.withCTEs(stmt.UpdateStmt.WithClause, cte)
		e.writeRelation(stmt.UpdateStmt.Relation, scope)
		for _, target := range stmt.UpdateStmt.TargetList {
			if res := target.GetResTarget(); res != nil && res.Name != "" {
				e.addColumn(relationName(stmt.UpdateStmt.Relation), res.Name)
			}
			e.read(target, scope)
		}
		e.readNodes(scope, stmt.UpdateStmt.FromClause...)
		e.read(stmt.UpdateStmt.WhereClause, scope)
		e.readNodes(scope, stmt.UpdateStmt.ReturningList...)

	case *pg_query.Node_DeleteStmt:
		e.setKind(KindDelete)
		e.addKind(AccessWrite, AccessRead)
		scope := e.withCTEs(stmt.DeleteStmt.WithClause, cte)
		e.writeRelation(stmt.DeleteStmt.Relation, scope)
		e.readNodes(scope, stmt.DeleteStmt.UsingClause...)
		e.read(stmt.DeleteStmt.WhereClause, scope)
		e.readNodes(scope, stmt.DeleteStmt.ReturningList...)

	case *pg_query.Node_MergeStmt:
		e.setKind(KindOther)
		e.addKind(AccessWrite, AccessRead)
		scope := e.withCTEs(stmt.MergeStmt.WithClause, cte)
		e.writeRelation(stmt.MergeStmt.Relation, scope)
		e.read(stmt.MergeStmt.SourceRelation, scope)
		e.read(stmt.MergeStmt.JoinCondition, scope)

	case *pg_query.Node_CopyStmt:
		e.setKind(KindCopy)
		copyStmt := stmt.CopyStmt
		if copyStmt.IsFrom {
			e.addKind(AccessWrite)
			e.writeRelation(copyStmt.Relation, cte)
		} else {
			e.addKind(AccessRead)
			e.readRelation(copyStmt.Relation, cte)
		}
		if copyStmt.Query != nil {
			e.addKind(AccessRead)
			e.read(copyStmt.Query, cte)
		}
		// COPY ... TO 'file/uri' moves tenant data outside the warehouse.
		// COPY ... TO a location moves tenant data OUT of the warehouse, which
		// is a different risk from reading a location in, so it keeps the
		// admin class.
		if copyStmt.Filename != "" && !copyStmt.IsFrom {
			e.addKind(AccessAdmin)
			e.addTableFunction("copy_to", []string{sanitizeArg(copyStmt.Filename)}, true)
		}

	case *pg_query.Node_ExplainStmt:
		e.setKind(KindExplain)
		// EXPLAIN reads the plan, not the data; classify by the inner
		// statement so an EXPLAIN of a write is not mistaken for a read.
		e.statement(stmt.ExplainStmt.Query, cte)
		e.meta.QueryKind = KindExplain

	case *pg_query.Node_VariableSetStmt:
		e.setKind(KindSet)
		e.addKind(AccessConfig)
	case *pg_query.Node_VariableShowStmt:
		e.setKind(KindShow)
		e.addKind(AccessMetadata)
	case *pg_query.Node_TransactionStmt:
		e.setKind(KindTransaction)
		e.addKind(AccessTransaction)

	case *pg_query.Node_CreateStmt:
		e.ddl(KindCreate, stmt.CreateStmt.Relation, cte)
	case *pg_query.Node_CreateTableAsStmt:
		e.setKind(KindCreate)
		e.addKind(AccessDDL, AccessRead)
		if stmt.CreateTableAsStmt.Into != nil {
			e.writeRelation(stmt.CreateTableAsStmt.Into.Rel, cte)
		}
		e.read(stmt.CreateTableAsStmt.Query, cte)
	case *pg_query.Node_ViewStmt:
		e.setKind(KindCreate)
		e.addKind(AccessDDL, AccessRead)
		e.writeRelation(stmt.ViewStmt.View, cte)
		e.read(stmt.ViewStmt.Query, cte)
	case *pg_query.Node_IndexStmt:
		e.ddl(KindCreate, stmt.IndexStmt.Relation, cte)
	case *pg_query.Node_CreateSchemaStmt:
		e.setKind(KindCreate)
		e.addKind(AccessDDL)
	case *pg_query.Node_AlterTableStmt:
		e.ddl(KindAlter, stmt.AlterTableStmt.Relation, cte)
	case *pg_query.Node_RenameStmt:
		e.ddl(KindAlter, stmt.RenameStmt.Relation, cte)
	case *pg_query.Node_TruncateStmt:
		e.setKind(KindOther)
		e.addKind(AccessDDL, AccessWrite)
		for _, rel := range stmt.TruncateStmt.Relations {
			e.writeRelation(rel.GetRangeVar(), cte)
		}
	case *pg_query.Node_DropStmt:
		e.setKind(KindDrop)
		e.addKind(AccessDDL)
		for _, obj := range stmt.DropStmt.Objects {
			if list := obj.GetList(); list != nil {
				e.addWrite(relationFromNames(list.Items))
			}
		}

	case *pg_query.Node_GrantStmt, *pg_query.Node_CreateRoleStmt,
		*pg_query.Node_AlterRoleStmt, *pg_query.Node_DropRoleStmt:
		e.setKind(KindOther)
		e.addKind(AccessAdmin)

	case *pg_query.Node_CallStmt:
		// CALL invokes a procedure whose body extraction cannot see, so the
		// access it performs is genuinely undeterminable — AccessUnknown, which
		// is what a gate denies on.
		//
		// It is NOT marked incomplete: the statement parsed fine and we saw all
		// of it. "Parsed, but its access is opaque" and "we could not parse it"
		// are different facts and must stay distinguishable — a consumer that
		// treats incomplete as "retry with a better parser" would be wrong here,
		// because no parser can see inside the procedure. Both still deny.
		//
		// The procedure name is recorded so a policy can allowlist specific
		// procedures rather than refusing CALL wholesale.
		e.setKind(KindOther)
		e.addKind(AccessUnknown)
		if call := stmt.CallStmt.Funccall; call != nil {
			e.addFunction(functionName(call))
			for _, arg := range call.Args {
				e.read(arg, cte)
			}
		}

	default:
		// An unrecognized statement type is recorded as unknown rather than
		// ignored: a future gate must see that we could not classify it.
		e.setKind(KindOther)
		e.addKind(AccessUnknown)
		e.markIncomplete("unhandled_statement")
		e.read(node, cte)
	}
}

func (e *extractor) ddl(kind string, rel *pg_query.RangeVar, cte map[string]struct{}) {
	e.setKind(kind)
	e.addKind(AccessDDL)
	e.writeRelation(rel, cte)
}

// selectStmt walks a SELECT, honouring CTE scoping: a name defined by WITH is
// not a relation, and reporting it as one would put a phantom table in an audit
// trail (and, later, in a policy decision).
func (e *extractor) selectStmt(sel *pg_query.SelectStmt, cte map[string]struct{}) {
	if sel == nil {
		return
	}
	scope := e.withCTEs(sel.WithClause, cte)
	e.readNodes(scope, sel.FromClause...)
	e.readNodes(scope, sel.TargetList...)
	e.read(sel.WhereClause, scope)
	e.readNodes(scope, sel.GroupClause...)
	e.read(sel.HavingClause, scope)
	e.readNodes(scope, sel.SortClause...)
	e.readNodes(scope, sel.ValuesLists...)
	e.selectStmt(sel.Larg, scope)
	e.selectStmt(sel.Rarg, scope)
}

// withCTEs registers a WITH clause's names and walks its bodies. A writable CTE
// contributes its own write access, which is the case a command-tag-based
// classifier gets wrong: "WITH x AS (INSERT ...) SELECT" is not a read.
func (e *extractor) withCTEs(with *pg_query.WithClause, outer map[string]struct{}) map[string]struct{} {
	if with == nil {
		return outer
	}
	scope := make(map[string]struct{}, len(outer)+len(with.Ctes))
	for name := range outer {
		scope[name] = struct{}{}
	}
	if with.Recursive {
		for _, node := range with.Ctes {
			if c := node.GetCommonTableExpr(); c != nil {
				scope[strings.ToLower(c.Ctename)] = struct{}{}
			}
		}
	}
	for _, node := range with.Ctes {
		c := node.GetCommonTableExpr()
		if c == nil {
			continue
		}
		e.statement(c.Ctequery, scope)
		if !with.Recursive {
			scope[strings.ToLower(c.Ctename)] = struct{}{}
		}
	}
	return scope
}

func (e *extractor) readNodes(cte map[string]struct{}, nodes ...*pg_query.Node) {
	for _, node := range nodes {
		e.read(node, cte)
	}
}

// read walks an expression subtree, collecting read relations, columns, and
// functions.
func (e *extractor) read(node *pg_query.Node, cte map[string]struct{}) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		e.readRelation(n.RangeVar, cte)
		return
	case *pg_query.Node_SelectStmt:
		e.selectStmt(n.SelectStmt, cte)
		return
	case *pg_query.Node_CommonTableExpr:
		e.statement(n.CommonTableExpr.Ctequery, cte)
		return
	case *pg_query.Node_ColumnRef:
		e.columnRef(n.ColumnRef)
		return
	case *pg_query.Node_FuncCall:
		e.addFunction(functionName(n.FuncCall))
	case *pg_query.Node_RangeFunction:
		// A function in FROM position is a data source, not a scalar.
		for _, fn := range n.RangeFunction.Functions {
			e.rangeFunction(fn)
		}
	case *pg_query.Node_InsertStmt, *pg_query.Node_UpdateStmt,
		*pg_query.Node_DeleteStmt, *pg_query.Node_MergeStmt:
		// A writable CTE or sub-statement: re-enter statement dispatch so its
		// write target is classified as a write, not a read.
		e.statement(node, cte)
		return
	}
	e.walkChildren(node.ProtoReflect(), cte)
}

func (e *extractor) rangeFunction(node *pg_query.Node) {
	if node == nil {
		return
	}
	if list := node.GetList(); list != nil {
		for _, item := range list.Items {
			e.rangeFunction(item)
		}
		return
	}
	call := node.GetFuncCall()
	if call == nil {
		e.read(node, nil)
		return
	}
	name := functionName(call)
	e.addFunction(name)
	args := make([]string, 0, len(call.Args))
	for _, arg := range call.Args {
		if c := arg.GetAConst(); c != nil && c.GetSval() != nil {
			args = append(args, sanitizeArg(c.GetSval().Sval))
		}
	}
	external := isExternalDataFunction(name)
	e.addTableFunction(name, args, external)
	if external {
		// Reading a location is a read, not an escalation — pointing
		// read_parquet at your own bucket is supported usage. The access class
		// stays `read`; whether the target resolves into managed DuckLake
		// storage (one tenant reaching another's data) is a path question the
		// policy answers from Args, not something the function name decides.
		e.addKind(AccessRead)
	}
	for _, arg := range call.Args {
		e.read(arg, nil)
	}
}

func (e *extractor) walkChildren(message protoreflect.Message, cte map[string]struct{}) {
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind {
			return true
		}
		if field.IsList() {
			list := value.List()
			for i := range list.Len() {
				e.walkMessage(list.Get(i).Message(), cte)
			}
			return true
		}
		e.walkMessage(value.Message(), cte)
		return true
	})
}

func (e *extractor) walkMessage(message protoreflect.Message, cte map[string]struct{}) {
	if node, ok := message.Interface().(*pg_query.Node); ok {
		e.read(node, cte)
		return
	}
	e.walkChildren(message, cte)
}

func (e *extractor) columnRef(ref *pg_query.ColumnRef) {
	if ref == nil {
		return
	}
	parts := make([]string, 0, len(ref.Fields))
	star := false
	for _, field := range ref.Fields {
		if field.GetAStar() != nil {
			star = true
			continue
		}
		if s := field.GetString_(); s != nil {
			parts = append(parts, s.Sval)
		}
	}
	if star {
		e.meta.SelectStar = true
		if len(parts) > 0 {
			// t.* — every column of a named relation.
			e.addColumn(parts[len(parts)-1], "*")
		}
		return
	}
	switch len(parts) {
	case 0:
		return
	case 1:
		// Unqualified: attributable only when exactly one relation is in scope.
		e.addColumn("", parts[0])
		e.unqualifiedCols = true
	default:
		e.addColumn(parts[len(parts)-2], parts[len(parts)-1])
	}
}

func (e *extractor) readRelation(rel *pg_query.RangeVar, cte map[string]struct{}) {
	if rel == nil {
		return
	}
	if _, isCTE := cte[strings.ToLower(rel.Relname)]; isCTE && rel.Schemaname == "" {
		return
	}
	relation := relationFromRangeVar(rel)
	if isCatalogSchema(relation.Schema) {
		e.addKind(AccessMetadata)
	} else {
		e.relationsInScope++
	}
	e.addRead(relation)
}

func (e *extractor) writeRelation(rel *pg_query.RangeVar, cte map[string]struct{}) {
	if rel == nil {
		return
	}
	if _, isCTE := cte[strings.ToLower(rel.Relname)]; isCTE && rel.Schemaname == "" {
		return
	}
	e.relationsInScope++
	e.addWrite(relationFromRangeVar(rel))
}

func (e *extractor) addRead(rel Relation) {
	if rel.Name == "" {
		return
	}
	key := rel.Raw
	if _, ok := e.readSeen[key]; ok {
		return
	}
	if len(e.meta.ReadRelations) >= maxCollected {
		e.truncated = true
		return
	}
	e.readSeen[key] = struct{}{}
	e.meta.ReadRelations = append(e.meta.ReadRelations, rel)
}

func (e *extractor) addWrite(rel Relation) {
	if rel.Name == "" {
		return
	}
	key := rel.Raw
	if _, ok := e.writeSeen[key]; ok {
		return
	}
	if len(e.meta.WriteRelations) >= maxCollected {
		e.truncated = true
		return
	}
	e.writeSeen[key] = struct{}{}
	e.meta.WriteRelations = append(e.meta.WriteRelations, rel)
}

func (e *extractor) addColumn(relation, name string) {
	if name == "" {
		return
	}
	key := strings.ToLower(relation + "." + name)
	if _, ok := e.columnSeen[key]; ok {
		return
	}
	if len(e.meta.Columns) >= maxCollected {
		e.truncated = true
		return
	}
	e.columnSeen[key] = struct{}{}
	e.meta.Columns = append(e.meta.Columns, Column{Relation: relation, Name: name})
}

func (e *extractor) addFunction(name string) {
	if name == "" {
		return
	}
	if _, ok := e.funcSeen[name]; ok {
		return
	}
	if len(e.meta.Functions) >= maxCollected {
		e.truncated = true
		return
	}
	e.funcSeen[name] = struct{}{}
	e.meta.Functions = append(e.meta.Functions, name)
}

func (e *extractor) addTableFunction(name string, args []string, external bool) {
	if name == "" {
		return
	}
	if _, ok := e.tableFnSeen[name]; ok {
		return
	}
	if len(e.meta.TableFunctions) >= maxCollected {
		e.truncated = true
		return
	}
	e.tableFnSeen[name] = struct{}{}
	e.meta.TableFunctions = append(e.meta.TableFunctions, TableFunction{Name: name, Args: args, External: external})
}

func (e *extractor) markIncomplete(reason string) {
	e.meta.Complete = false
	if e.meta.IncompleteReason == "" {
		e.meta.IncompleteReason = reason
	}
}

func (e *extractor) finish() {
	if e.truncated {
		e.markIncomplete("truncated")
	}
	// A column reference we could not attribute to a relation leaves the column
	// list unresolved. One relation in scope is the only case where an
	// unqualified name is unambiguous without a catalog.
	e.meta.ColumnsResolved = !e.unqualifiedCols || e.relationsInScope <= 1
	if e.meta.QueryKind == "" {
		e.meta.QueryKind = KindOther
	}
	if len(e.kinds) == 0 {
		e.kinds[AccessUnknown] = struct{}{}
	}
	e.meta.AccessKinds = make([]AccessKind, 0, len(e.kinds))
	for kind := range e.kinds {
		e.meta.AccessKinds = append(e.meta.AccessKinds, kind)
	}
	sort.Slice(e.meta.AccessKinds, func(i, j int) bool {
		return e.meta.AccessKinds[i] < e.meta.AccessKinds[j]
	})
}

// HasKind reports whether the statement carries an access kind.
func (m Metadata) HasKind(kind AccessKind) bool {
	for _, k := range m.AccessKinds {
		if k == kind {
			return true
		}
	}
	return false
}

func relationFromRangeVar(rel *pg_query.RangeVar) Relation {
	parts := make([]string, 0, 3)
	if rel.Catalogname != "" {
		parts = append(parts, rel.Catalogname)
	}
	if rel.Schemaname != "" {
		parts = append(parts, rel.Schemaname)
	}
	parts = append(parts, rel.Relname)
	return Relation{
		Catalog: rel.Catalogname,
		Schema:  rel.Schemaname,
		Name:    rel.Relname,
		Raw:     strings.Join(parts, "."),
	}
}

func relationFromNames(items []*pg_query.Node) Relation {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if s := item.GetString_(); s != nil {
			parts = append(parts, s.Sval)
		}
	}
	if len(parts) == 0 {
		return Relation{}
	}
	rel := Relation{Name: parts[len(parts)-1], Raw: strings.Join(parts, ".")}
	if len(parts) >= 2 {
		rel.Schema = parts[len(parts)-2]
	}
	if len(parts) >= 3 {
		rel.Catalog = parts[len(parts)-3]
	}
	return rel
}

func relationName(rel *pg_query.RangeVar) string {
	if rel == nil {
		return ""
	}
	return rel.Relname
}

func functionName(call *pg_query.FuncCall) string {
	if call == nil || len(call.Funcname) == 0 {
		return ""
	}
	last := call.Funcname[len(call.Funcname)-1].GetString_()
	if last == nil {
		return ""
	}
	return strings.ToLower(last.Sval)
}

func isCatalogSchema(schema string) bool {
	switch strings.ToLower(schema) {
	case "pg_catalog", "information_schema":
		return true
	default:
		return false
	}
}

// isExternalDataFunction reports whether a table function reads from a location
// rather than a relation — i.e. whether its arguments are a path a policy needs
// to resolve. Mirrors the shape of the function list in
// server/query_access.go, but note the different purpose: this one marks
// arguments as targets, it does not mark the statement as dangerous.
func isExternalDataFunction(name string) bool {
	switch name {
	case "glob", "query", "query_table", "sniff_csv", "copy_to":
		return true
	}
	return strings.HasPrefix(name, "read_") ||
		strings.HasPrefix(name, "http_") ||
		strings.HasPrefix(name, "postgres_") ||
		strings.HasPrefix(name, "mysql_") ||
		strings.HasPrefix(name, "sqlite_") ||
		strings.HasSuffix(name, "_scan")
}

// sanitizeArg strips credential material from a table-function argument. A
// presigned URL carries its authorization in the query string, and userinfo
// carries it in the authority, so only scheme, host, and path survive.
func sanitizeArg(arg string) string {
	if i := strings.IndexAny(arg, "?#"); i >= 0 {
		arg = arg[:i] + "?…"
	}
	if scheme := strings.Index(arg, "://"); scheme >= 0 {
		rest := arg[scheme+3:]
		if at := strings.Index(rest, "@"); at >= 0 {
			arg = arg[:scheme+3] + "…@" + rest[at+1:]
		}
	}
	const maxArg = 256
	if len(arg) > maxArg {
		arg = arg[:maxArg] + "…"
	}
	return arg
}

// leadingKeyword returns the first keyword of a statement, uppercased, skipping
// comments and whitespace.
func leadingKeyword(sql string) string {
	rest := strings.TrimLeft(stripComments(sql), " \t\r\n(")
	end := strings.IndexFunc(rest, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == ';' || r == '('
	})
	if end < 0 {
		end = len(rest)
	}
	return strings.ToUpper(rest[:end])
}

func stripComments(sql string) string {
	for {
		trimmed := strings.TrimLeft(sql, " \t\r\n")
		switch {
		case strings.HasPrefix(trimmed, "--"):
			if i := strings.IndexByte(trimmed, '\n'); i >= 0 {
				sql = trimmed[i+1:]
				continue
			}
			return ""
		case strings.HasPrefix(trimmed, "/*"):
			if i := strings.Index(trimmed, "*/"); i >= 0 {
				sql = trimmed[i+2:]
				continue
			}
			return ""
		default:
			return trimmed
		}
	}
}
