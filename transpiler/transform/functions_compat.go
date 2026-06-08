package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// This file implements the PostgreSQL builtin-compatibility transforms that
// cannot be expressed as catalog macros (self-recursion, variadic templates, or
// arg-count-dependent rewrites). See docs/pg-builtin-compat-status.md.

// replaceCompatFuncNode handles compat functions that must REPLACE the whole
// FuncCall node with a different expression (vs an in-place arg edit). It is
// called from FunctionTransform.Transform's walk, before transformFuncCall.
// Returns true if it replaced node.Node.
func (t *FunctionTransform) replaceCompatFuncNode(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	name := lastFuncName(fc)
	switch name {
	case "format":
		return t.rewriteFormat(node, fc)
	case "date_trunc":
		return t.rewriteDateTrunc3(node, fc)
	case "overlay":
		return t.rewriteOverlay(node, fc)
	case "isfinite":
		return t.rewriteIsfiniteInterval(node, fc)
	}
	return false
}

// rewriteFormat rewrites format('...literal template...', args...) with PG
// %-specifiers into a || concatenation. Only fires for a literal first arg
// containing at least one %-spec; %I->quote_ident, %L->quote_literal,
// %s->CAST(.. AS VARCHAR), %%->'%'. Unsupported specs (positional/width) bail.
func (t *FunctionTransform) rewriteFormat(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	if len(fc.Args) == 0 {
		return false
	}
	tmpl := extractStringConstant(fc.Args[0])
	if tmpl == "" || !strings.Contains(tmpl, "%") {
		return false // non-literal or no specifier: leave for native format
	}
	var segments []*pg_query.Node
	var lit strings.Builder
	flushLit := func() {
		if lit.Len() > 0 {
			segments = append(segments, strConstNode(lit.String()))
			lit.Reset()
		}
	}
	argIdx := 1 // Args[0] is the template
	for i := 0; i < len(tmpl); i++ {
		if tmpl[i] != '%' {
			lit.WriteByte(tmpl[i])
			continue
		}
		if i+1 >= len(tmpl) {
			return false // dangling '%'
		}
		spec := tmpl[i+1]
		i++
		switch spec {
		case '%':
			lit.WriteByte('%')
		case 's', 'I', 'L':
			if argIdx >= len(fc.Args) {
				return false // not enough args
			}
			arg := fc.Args[argIdx]
			argIdx++
			flushLit()
			switch spec {
			case 's':
				segments = append(segments, castToVarchar(arg))
			case 'I':
				segments = append(segments, funcCallNode("quote_ident", arg))
			case 'L':
				segments = append(segments, funcCallNode("quote_literal", arg))
			}
		default:
			return false // positional/width/precision specs: out of scope, bail
		}
	}
	flushLit()
	if len(segments) == 0 {
		segments = append(segments, strConstNode(""))
	}
	node.Node = concatNodes(segments).Node
	return true
}

// rewriteDateTrunc3 rewrites the 3-arg PG14 date_trunc(field, ts, zone) into
// timezone(zone, date_trunc(field, timezone(zone, ts))). The 2-arg form is left
// to the native builtin.
func (t *FunctionTransform) rewriteDateTrunc3(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	if len(fc.Args) != 3 {
		return false
	}
	field, source, zone := fc.Args[0], fc.Args[1], fc.Args[2]
	inner := funcCallNode("timezone", zone, source)
	trunc := funcCallNode("date_trunc", field, inner)
	node.Node = funcCallNode("timezone", zone, trunc).Node
	return true
}

// rewriteOverlay rewrites overlay(s placing repl from start [for len]) into
// substr(s,1,start-1) || repl || substr(s, start+len). Default len = length(repl).
func (t *FunctionTransform) rewriteOverlay(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	if len(fc.Args) < 3 {
		return false
	}
	s, repl, start := fc.Args[0], fc.Args[1], fc.Args[2]
	var length *pg_query.Node
	if len(fc.Args) >= 4 {
		length = fc.Args[3]
	} else {
		length = funcCallNode("length", repl)
	}
	left := funcCallNode("substr", s, intConstNode(1), binExprNode("-", start, intConstNode(1)))
	right := funcCallNode("substr", s, binExprNode("+", start, length))
	node.Node = concatNodes([]*pg_query.Node{left, repl, right}).Node
	return true
}

// rewriteIsfiniteInterval replaces isfinite(<interval>) with TRUE, supplying the
// INTERVAL overload DuckDB lacks. Only fires when the argument is provably an
// interval; all other arg types stay on the native builtin.
func (t *FunctionTransform) rewriteIsfiniteInterval(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	if len(fc.Args) != 1 || !isProvablyInterval(fc.Args[0]) {
		return false
	}
	node.Node = boolConstNode(true).Node
	return true
}

// handleSubstrClamp rewrites substr/substring positional args in place to match
// PG's window semantics for non-positive start: start -> greatest(start,1) and
// (for the 3-arg form) count -> greatest(0, count + start - greatest(start,1)).
func (t *FunctionTransform) handleSubstrClamp(fc *pg_query.FuncCall) bool {
	if len(fc.Args) == 2 {
		start := fc.Args[1]
		fc.Args[1] = funcCallNode("greatest", start, intConstNode(1))
		return true
	}
	if len(fc.Args) == 3 {
		start, cnt := fc.Args[1], fc.Args[2]
		newCnt := funcCallNode("greatest", intConstNode(0),
			binExprNode("-", binExprNode("+", cnt, start), funcCallNode("greatest", start, intConstNode(1))))
		fc.Args[1] = funcCallNode("greatest", start, intConstNode(1))
		fc.Args[2] = newCnt
		return true
	}
	return false
}

// handleSubstringRegex rewrites the SQL regex form substring(text FROM pattern)
// — a 2-arg call whose second arg is a string literal — into
// regexp_extract(text, pattern, group), where group is 1 if the pattern has a
// capture group, else 0 (PG returns the first group if present, else the whole
// match). Other arg shapes (positional / FROM..FOR) are left to native substring.
func (t *FunctionTransform) handleSubstringRegex(fc *pg_query.FuncCall, funcNameIdx int) bool {
	if len(fc.Args) != 2 {
		return false
	}
	pattern := extractStringConstant(fc.Args[1])
	if pattern == "" {
		return false // non-literal or non-string second arg: native substring
	}
	group := 0
	if patternHasCaptureGroup(pattern) {
		group = 1
	}
	renameFuncAndStripPrefix(fc, funcNameIdx, "regexp_extract")
	fc.Funcformat = pg_query.CoercionForm_COERCE_EXPLICIT_CALL
	fc.Args = append(fc.Args, intConstNode(group))
	return true
}

// ---- small AST constructors ----

func lastFuncName(fc *pg_query.FuncCall) string {
	if fc == nil || len(fc.Funcname) == 0 {
		return ""
	}
	if str := fc.Funcname[len(fc.Funcname)-1].GetString_(); str != nil {
		return strings.ToLower(str.Sval)
	}
	return ""
}

func strConstNode(s string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
		Val: &pg_query.A_Const_Sval{Sval: &pg_query.String{Sval: s}},
	}}}
}

func intConstNode(n int) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
		Val: &pg_query.A_Const_Ival{Ival: &pg_query.Integer{Ival: int32(n)}},
	}}}
}

func boolConstNode(b bool) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
		Val: &pg_query.A_Const_Boolval{Boolval: &pg_query.Boolean{Boolval: b}},
	}}}
}

func funcCallNode(name string, args ...*pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_FuncCall{FuncCall: &pg_query.FuncCall{
		Funcname: []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: name}}}},
		Args:     args,
	}}}
}

func binExprNode(op string, l, r *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: &pg_query.A_Expr{
		Kind:  pg_query.A_Expr_Kind_AEXPR_OP,
		Name:  []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: op}}}},
		Lexpr: l,
		Rexpr: r,
	}}}
}

func castToVarchar(arg *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
		Arg: arg,
		TypeName: &pg_query.TypeName{
			Names: []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "varchar"}}}},
		},
	}}}
}

// concatNodes left-folds the segments with the || operator.
func concatNodes(segments []*pg_query.Node) *pg_query.Node {
	acc := segments[0]
	for _, seg := range segments[1:] {
		acc = binExprNode("||", acc, seg)
	}
	return acc
}

// patternHasCaptureGroup reports whether a regex literal contains an unescaped
// capturing '(' (i.e. '(' not preceded by '\' and not the start of '(?').
func patternHasCaptureGroup(p string) bool {
	for i := 0; i < len(p); i++ {
		if p[i] == '\\' {
			i++
			continue
		}
		if p[i] == '(' && (i+1 >= len(p) || p[i+1] != '?') {
			return true
		}
	}
	return false
}

// intervalReturningFuncs are compat/native functions whose result is an INTERVAL,
// used to recognise a provably-interval argument to isfinite().
var intervalReturningFuncs = map[string]bool{
	"make_interval": true, "justify_hours": true, "justify_days": true,
	"justify_interval": true, "age": true, "to_years": true, "to_months": true,
	"to_days": true, "to_hours": true, "to_minutes": true, "to_seconds": true,
	"to_milliseconds": true, "to_microseconds": true,
}

func isProvablyInterval(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if tc := node.GetTypeCast(); tc != nil && tc.TypeName != nil && len(tc.TypeName.Names) > 0 {
		if last := tc.TypeName.Names[len(tc.TypeName.Names)-1].GetString_(); last != nil {
			if strings.EqualFold(last.Sval, "interval") {
				return true
			}
		}
	}
	if fc := node.GetFuncCall(); fc != nil {
		return intervalReturningFuncs[lastFuncName(fc)]
	}
	return false
}
