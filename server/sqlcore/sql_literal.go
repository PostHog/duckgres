package sqlcore

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// AppendTextLiteralBytes appends data as a DuckDB standard-conforming SQL
// string literal. It writes bytes directly so callers that retain a PostgreSQL
// Bind frame do not need to allocate an intermediate string for text values.
func AppendTextLiteralBytes(dst *strings.Builder, data []byte) {
	dst.WriteByte('\'')
	for _, ch := range data {
		if ch == '\'' {
			dst.WriteByte('\'')
		}
		dst.WriteByte(ch)
	}
	dst.WriteByte('\'')
}

// AppendTextLiteralString is the string counterpart to
// AppendTextLiteralBytes. Keeping this separate avoids a string-to-byte
// conversion when a caller has already formatted a binary protocol value.
func AppendTextLiteralString(dst *strings.Builder, data string) {
	dst.WriteByte('\'')
	for i := 0; i < len(data); i++ {
		if data[i] == '\'' {
			dst.WriteByte('\'')
		}
		dst.WriteByte(data[i])
	}
	dst.WriteByte('\'')
}

// AppendSQLLiteral appends the same literal representation historically used
// by Flight interpolation. It is shared with compact Bind interpolation so
// local and remote paths retain matching NULL, binary, numeric, and time
// semantics.
func AppendSQLLiteral(dst *strings.Builder, value any) {
	if value == nil {
		dst.WriteString("NULL")
		return
	}
	switch v := value.(type) {
	case string:
		AppendTextLiteralBytes(dst, []byte(v))
	case []byte:
		dst.WriteString(`'\x`)
		dst.WriteString(hex.EncodeToString(v))
		dst.WriteString(`'::BLOB`)
	case bool:
		if v {
			dst.WriteString("TRUE")
		} else {
			dst.WriteString("FALSE")
		}
	case time.Time:
		AppendTextLiteralBytes(dst, []byte(v.Format("2006-01-02 15:04:05.999999")))
	case *big.Int:
		dst.WriteString(v.String())
	case int:
		dst.WriteString(strconv.Itoa(v))
	case int8:
		dst.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		dst.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		dst.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		dst.WriteString(strconv.FormatInt(v, 10))
	case float32:
		dst.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
	case float64:
		dst.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	default:
		AppendTextLiteralBytes(dst, []byte(fmt.Sprintf("%v", v)))
	}
}
