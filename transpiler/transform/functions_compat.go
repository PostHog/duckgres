package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// This file implements the PostgreSQL builtin-compatibility transforms that
// cannot be expressed as catalog macros (self-recursion, variadic templates, or
// arg-count-dependent rewrites). Regression coverage lives in
// server/pg_compat_transforms_test.go (transpile-and-execute against DuckDB).

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
	case "array_position":
		return t.rewriteArrayPosition3(node, fc)
	}
	return false
}

// rewriteArrayPosition3 rewrites the 3-arg PG array_position(arr, elem, start)
// into list_position(list_slice(arr, greatest(start,1), len(arr)), elem) +
// greatest(start,1) - 1. DuckDB's list_position has no start overload. The
// greatest(start,1) clamp matters: a naive arr[start:] slice mishandles start=0
// (off-by-one) and negative starts (DuckDB slices from the end). list_position
// returns NULL on miss and NULL propagates through the arithmetic, matching PG.
// arr and start are each referenced twice — volatile expressions would be
// evaluated repeatedly; fine for the common literal/column case. The 2-arg form
// stays on the plain list_position rename in functionNameMapping.
func (t *FunctionTransform) rewriteArrayPosition3(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	if len(fc.Args) != 3 {
		return false
	}
	arr, elem, start := fc.Args[0], fc.Args[1], fc.Args[2]
	clamped := funcCallNode("greatest", start, intConstNode(1))
	sliced := funcCallNode("list_slice", arr, clamped, funcCallNode("len", arr))
	pos := funcCallNode("list_position", sliced, elem)
	node.Node = binExprNode("-", binExprNode("+", pos, clamped), intConstNode(1)).Node
	return true
}

// rewriteFormat rewrites format('...literal template...', args...) with PG
// %-specifiers into a || concatenation. Only fires for a literal first arg
// containing at least one %-spec. NULL handling matches PG: %s renders NULL as
// empty string (coalesce), %L as the unquoted keyword NULL (quote_nullable);
// %I with a NULL arg yields a NULL result where PG raises — acceptable, since a
// NULL identifier is a bug either way. Malformed templates (dangling %, too few
// args) and unsupported specs (%1$s positional, width/precision) are rewritten
// to an error() call: DuckDB's native format would silently return the template
// unsubstituted, which is the exact corruption mode this transform exists to fix.
func (t *FunctionTransform) rewriteFormat(node *pg_query.Node, fc *pg_query.FuncCall) bool {
	if len(fc.Args) == 0 {
		return false
	}
	tmpl := extractStringConstant(fc.Args[0])
	if tmpl == "" || !strings.Contains(tmpl, "%") {
		return false // non-literal or no specifier: leave for native format
	}
	rewriteToError := func(msg string) bool {
		node.Node = funcCallNode("error", strConstNode(msg)).Node
		return true
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
			return rewriteToError("format(): unterminated format specifier")
		}
		spec := tmpl[i+1]
		i++
		switch spec {
		case '%':
			lit.WriteByte('%')
		case 's', 'I', 'L':
			if argIdx >= len(fc.Args) {
				return rewriteToError("format(): too few arguments for format string")
			}
			arg := fc.Args[argIdx]
			argIdx++
			flushLit()
			switch spec {
			case 's':
				// PG renders a NULL %s argument as the empty string.
				segments = append(segments, coalesceNode(castToVarchar(arg), strConstNode("")))
			case 'I':
				segments = append(segments, funcCallNode("quote_ident", arg))
			case 'L':
				// quote_nullable matches PG %L exactly: NULL -> unquoted keyword NULL.
				segments = append(segments, funcCallNode("quote_nullable", arg))
			}
		default:
			return rewriteToError("format(): unsupported format specifier %" + string(spec) +
				" (only %s, %I, %L and %% are supported)")
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
// s, start and (in the FROM-only form) repl are each referenced more than once —
// volatile expressions would be evaluated repeatedly; fine for the common
// literal/column case.
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
// Note the start expression is referenced up to three times in the rewrite — a
// volatile start (e.g. a subquery or random()) would be evaluated repeatedly.
// Acceptable for the overwhelmingly common literal/column case.
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

// handleSubstring dispatches PostgreSQL substring() by arg shape:
//   - 2-arg with a string-literal second arg — the SQL regex FROM-pattern form —
//     becomes regexp_extract(text, pattern, group), group 1 if the pattern has a
//     capture group else 0 (PG returns the first group if present, else the
//     whole match).
//   - 3-arg (FROM..FOR) and 2-arg with an integer second arg are positional and
//     share substr's PG window semantics for non-positive start, so they get the
//     same clamp rewrite (deparsed as a plain call so the clamped expressions
//     don't ride on the SUBSTRING(.. FROM .. FOR ..) keyword syntax).
//   - 2-arg with a non-literal second arg is ambiguous at transpile time (text
//     column = regex, int column = positional) and is left untouched.
func (t *FunctionTransform) handleSubstring(fc *pg_query.FuncCall, funcNameIdx int) bool {
	if len(fc.Args) == 2 {
		if pattern := extractStringConstant(fc.Args[1]); pattern != "" {
			group := 0
			if patternHasCaptureGroup(pattern) {
				group = 1
			}
			renameFuncAndStripPrefix(fc, funcNameIdx, "regexp_extract")
			fc.Funcformat = pg_query.CoercionForm_COERCE_EXPLICIT_CALL
			fc.Args = append(fc.Args, intConstNode(group))
			return true
		}
		if !isIntegerConst(fc.Args[1]) {
			return false // ambiguous non-literal second arg: leave native
		}
	}
	if len(fc.Args) == 2 || len(fc.Args) == 3 {
		if t.handleSubstrClamp(fc) {
			renameFuncAndStripPrefix(fc, funcNameIdx, "substring")
			fc.Funcformat = pg_query.CoercionForm_COERCE_EXPLICIT_CALL
			return true
		}
	}
	return false
}

// isIntegerConst reports whether a node is an integer A_Const (possibly inside
// a unary minus, which pg_query folds into the constant for literals like -2).
func isIntegerConst(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if ac := node.GetAConst(); ac != nil {
		return ac.GetIval() != nil || (ac.Val == nil && !ac.Isnull)
	}
	return false
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

// coalesceNode builds a COALESCE(...) expression. COALESCE is grammar syntax,
// not a callable function — a plain FuncCall would deparse quoted and fail.
func coalesceNode(args ...*pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_CoalesceExpr{CoalesceExpr: &pg_query.CoalesceExpr{
		Args: args,
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
