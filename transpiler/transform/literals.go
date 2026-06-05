package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// LiteralTransform rewrites PostgreSQL literal syntaxes that DuckDB misparses:
//
//   - bytea hex literals: '\xDEADBEEF'::bytea — DuckDB does not understand the
//     Postgres \x hex-prefix in a string literal and drops the first byte. We
//     rewrite to unhex('DEADBEEF'), which yields the correct BLOB bytes.
//   - bit-string literals: B'101' — pg_query carries these as an A_Const bsval
//     ("b101"); DuckDB turns the deparsed form into the string 'b101'. We rewrite
//     to '101'::BIT.
//
// It walks every node and mutates matching literal nodes in place, so it must run
// BEFORE TypeMappingTransform (which would rewrite the `bytea` type name to
// `blob`, losing the signal this transform keys on).
type LiteralTransform struct{}

func NewLiteralTransform() *LiteralTransform { return &LiteralTransform{} }

func (t *LiteralTransform) Name() string { return "literals" }

func (t *LiteralTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false
	WalkFunc(tree, func(node *pg_query.Node) bool {
		// bytea hex literal: TypeCast(arg=A_Const sval "\x..", type bytea) -> unhex('..')
		if tc := node.GetTypeCast(); tc != nil && isByteaTypeName(tc.TypeName) {
			if s, ok := stringConstValue(tc.Arg); ok && hasHexPrefix(s) {
				node.Node = &pg_query.Node_FuncCall{FuncCall: unhexFuncCall(s[2:])}
				changed = true
				return true
			}
		}
		// bit-string literal: A_Const{bsval:"b101"} -> '101'::BIT
		if ac := node.GetAConst(); ac != nil {
			if bs := ac.GetBsval(); bs != nil {
				if cast := bitStringToCast(bs.Bsval); cast != nil {
					node.Node = cast.Node
					changed = true
					return true
				}
			}
		}
		return true
	})
	return changed, nil
}

// isByteaTypeName reports whether a TypeName's final element is `bytea`.
func isByteaTypeName(tn *pg_query.TypeName) bool {
	if tn == nil || len(tn.Names) == 0 {
		return false
	}
	last := tn.Names[len(tn.Names)-1].GetString_()
	return last != nil && strings.EqualFold(last.Sval, "bytea")
}

// stringConstValue returns the string value of an A_Const string literal.
func stringConstValue(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	ac := node.GetAConst()
	if ac == nil {
		return "", false
	}
	if sv := ac.GetSval(); sv != nil {
		return sv.Sval, true
	}
	return "", false
}

// hasHexPrefix reports whether a string uses Postgres bytea hex format (\x...).
func hasHexPrefix(s string) bool {
	return len(s) >= 2 && s[0] == '\\' && (s[1] == 'x' || s[1] == 'X')
}

// unhexFuncCall builds unhex('<hex>'), which DuckDB evaluates to a BLOB.
func unhexFuncCall(hex string) *pg_query.FuncCall {
	return &pg_query.FuncCall{
		Funcname: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "unhex"}}},
		},
		Args: []*pg_query.Node{strConst(hex)},
	}
}

// bitStringToCast converts a pg_query bit-string value ("b101" / "B101") to a
// '101'::BIT TypeCast node. Returns nil for hex bit-strings (X'..') or anything
// it does not recognize, leaving the original node untouched.
func bitStringToCast(bsval string) *pg_query.Node {
	if len(bsval) < 1 {
		return nil
	}
	switch bsval[0] {
	case 'b', 'B':
		bits := bsval[1:]
		if bits == "" {
			return nil
		}
		return &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
			Arg: strConst(bits),
			TypeName: &pg_query.TypeName{
				Names:   []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "BIT"}}}},
				Typemod: -1,
			},
		}}}
	}
	// Hex bit-strings (X'..') are left as-is (out of scope).
	return nil
}

// strConst builds an A_Const string-literal node.
func strConst(s string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
		Val: &pg_query.A_Const_Sval{Sval: &pg_query.String{Sval: s}},
	}}}
}
