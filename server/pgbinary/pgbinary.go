// Package pgbinary validates and normalizes PostgreSQL binary COPY streams
// before they are handed to DuckDB's postgres_scanner extension.
package pgbinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

var copySignature = []byte("PGCOPY\n\xff\r\n\x00")

var numericTypePattern = regexp.MustCompile(`(?i)^\s*(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$`)

const (
	numericPositive = 0x0000
	numericNegative = 0x4000
	numericNaN      = 0xC000
	numericPosInf   = 0xD000
	numericNegInf   = 0xF000
)

// Numeric describes the destination DECIMAL typmod for a PostgreSQL NUMERIC
// field. PostgreSQL applies this typmod (including rounding) while loading a
// binary COPY stream; DuckDB's read_postgres_binary currently assumes the
// field's wire dscale already matches it, so we apply that coercion explicitly.
type Numeric struct {
	Precision int
	Scale     uint16
}

// Column describes scanner-sensitive properties of one binary COPY column.
// A nil Numeric means the field can be validated and copied byte-for-byte.
type Column struct {
	Numeric          *Numeric
	databaseTypeName string
	fixedWidth       int
}

// Schema is the expected tuple layout for a binary COPY stream.
type Schema struct {
	Columns []Column
}

// SchemaFromDatabaseTypes builds the validation schema carried from the
// control plane's lossless column metadata. Non-numeric types need no payload
// transformation, but still contribute to the exact tuple width.
func SchemaFromDatabaseTypes(databaseTypeNames []string) (Schema, error) {
	if len(databaseTypeNames) == 0 {
		return Schema{}, fmt.Errorf("PostgreSQL binary COPY schema has no database types")
	}
	schema := Schema{Columns: make([]Column, len(databaseTypeNames))}
	for i, databaseTypeName := range databaseTypeNames {
		upper := strings.ToUpper(strings.TrimSpace(databaseTypeName))
		schema.Columns[i].databaseTypeName = upper
		switch {
		case upper == "BOOLEAN" || upper == "BOOL":
			schema.Columns[i].fixedWidth = 1
			continue
		case upper == "BIGINT" || upper == "INT8":
			schema.Columns[i].fixedWidth = 8
			continue
		case upper == "DOUBLE" || upper == "FLOAT8":
			schema.Columns[i].fixedWidth = 8
			continue
		case upper == "DATE":
			schema.Columns[i].fixedWidth = 4
			continue
		case upper == "TIMESTAMP" || upper == "TIMESTAMPTZ" || upper == "TIMESTAMP WITH TIME ZONE":
			schema.Columns[i].fixedWidth = 8
			continue
		case upper == "VARCHAR" || strings.HasPrefix(upper, "VARCHAR(") || upper == "TEXT" || upper == "STRING" ||
			upper == "BLOB" || upper == "BYTEA":
			continue
		case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
			// Parsed below.
		default:
			return Schema{}, fmt.Errorf("column %d has unsupported PostgreSQL binary COPY type %q", i+1, databaseTypeName)
		}
		matches := numericTypePattern.FindStringSubmatch(databaseTypeName)
		if matches == nil {
			return Schema{}, fmt.Errorf("column %d NUMERIC type %q must include precision and scale", i+1, databaseTypeName)
		}
		precision, precisionErr := strconv.Atoi(matches[1])
		scale, scaleErr := strconv.ParseUint(matches[2], 10, 16)
		if precisionErr != nil || scaleErr != nil || scale > uint64(^uint16(0)) {
			return Schema{}, fmt.Errorf("column %d has invalid NUMERIC type %q", i+1, databaseTypeName)
		}
		schema.Columns[i].Numeric = &Numeric{Precision: precision, Scale: uint16(scale)}
	}
	if err := validateSchema(schema); err != nil {
		return Schema{}, err
	}
	return schema, nil
}

// Inspection reports whether a scanner-compatible rewrite is needed.
type Inspection struct {
	Rows         int64
	NeedsRewrite bool
}

type completionMode uint8

const (
	requireFileTrailer completionMode = iota
	allowProtocolCompletion
)

// Inspect validates a complete PostgreSQL binary COPY stream without retaining
// it in memory. It reports whether NUMERIC scales or a header extension require
// a scanner-compatible rewrite.
func Inspect(src io.Reader, schema Schema) (Inspection, error) {
	return process(nil, src, schema, requireFileTrailer)
}

// InspectProtocolCompleted validates a PostgreSQL binary COPY stream after its
// surrounding protocol has explicitly confirmed successful completion. In
// that mode, EOF between complete tuples is equivalent to the canonical file
// trailer and requests a rewrite that adds the canonical trailer.
func InspectProtocolCompleted(src io.Reader, schema Schema) (Inspection, error) {
	return process(nil, src, schema, allowProtocolCompletion)
}

// Rewrite validates src and writes a scanner-compatible PostgreSQL binary COPY
// stream to dst. NUMERIC values are coerced to their destination scale using
// round-half-away-from-zero, matching PostgreSQL's numeric typmod behavior.
func Rewrite(dst io.Writer, src io.Reader, schema Schema) (int64, error) {
	inspection, err := process(dst, src, schema, requireFileTrailer)
	return inspection.Rows, err
}

// RewriteProtocolCompleted validates a protocol-completed binary COPY stream
// and writes scanner-compatible file framing, including one canonical trailer
// when the protocol ended cleanly between tuples without one.
func RewriteProtocolCompleted(dst io.Writer, src io.Reader, schema Schema) (int64, error) {
	inspection, err := process(dst, src, schema, allowProtocolCompletion)
	return inspection.Rows, err
}

func process(dst io.Writer, src io.Reader, schema Schema, completion completionMode) (Inspection, error) {
	if err := validateSchema(schema); err != nil {
		return Inspection{}, err
	}

	var inspection Inspection
	signature := make([]byte, len(copySignature))
	if _, err := io.ReadFull(src, signature); err != nil {
		return inspection, fmt.Errorf("invalid PostgreSQL binary COPY header: %w", err)
	}
	if string(signature) != string(copySignature) {
		return inspection, fmt.Errorf("invalid PostgreSQL binary COPY signature")
	}

	flags, err := readUint32(src)
	if err != nil {
		return inspection, fmt.Errorf("invalid PostgreSQL binary COPY flags: %w", err)
	}
	if flags&0xffff0000 != 0 {
		return inspection, fmt.Errorf("unsupported critical PostgreSQL binary COPY flags 0x%08x", flags)
	}
	if flags != 0 {
		inspection.NeedsRewrite = true
	}
	extensionLength, err := readInt32(src)
	if err != nil {
		return inspection, fmt.Errorf("invalid PostgreSQL binary COPY header extension length: %w", err)
	}
	if extensionLength < 0 {
		return inspection, fmt.Errorf("invalid negative PostgreSQL binary COPY header extension length %d", extensionLength)
	}
	if extensionLength > 0 {
		inspection.NeedsRewrite = true
		if err := discardExact(src, int64(extensionLength)); err != nil {
			return inspection, fmt.Errorf("truncated PostgreSQL binary COPY header extension: %w", err)
		}
	}

	if dst != nil {
		if err := writeAll(dst, copySignature); err != nil {
			return inspection, err
		}
		if err := writeUint32(dst, 0); err != nil {
			return inspection, err
		}
		// postgres_scanner 1.5.3 does not skip header extensions, so emit a
		// zero-length extension after consuming any valid input extension.
		if err := writeUint32(dst, 0); err != nil {
			return inspection, err
		}
	}

	for {
		fieldCount, err := readInt16(src)
		if err != nil {
			if err == io.EOF && completion == allowProtocolCompletion {
				inspection.NeedsRewrite = true
				if dst != nil {
					if err := writeInt16(dst, -1); err != nil {
						return inspection, err
					}
				}
				return inspection, nil
			}
			return inspection, fmt.Errorf("missing or truncated PostgreSQL binary COPY trailer: %w", err)
		}
		if fieldCount == -1 {
			if dst != nil {
				if err := writeInt16(dst, -1); err != nil {
					return inspection, err
				}
			}
			var extra [1]byte
			if _, err := io.ReadFull(src, extra[:]); err == nil {
				return inspection, fmt.Errorf("trailing data after PostgreSQL binary COPY trailer")
			} else if err != io.EOF {
				return inspection, fmt.Errorf("read after PostgreSQL binary COPY trailer: %w", err)
			}
			return inspection, nil
		}
		if fieldCount < 0 {
			return inspection, fmt.Errorf("row %d has invalid field count %d", inspection.Rows+1, fieldCount)
		}
		if int(fieldCount) != len(schema.Columns) {
			return inspection, fmt.Errorf("row %d has %d fields, expected %d", inspection.Rows+1, fieldCount, len(schema.Columns))
		}
		if dst != nil {
			if err := writeInt16(dst, fieldCount); err != nil {
				return inspection, err
			}
		}

		for columnIndex, column := range schema.Columns {
			fieldLength, err := readInt32(src)
			if err != nil {
				return inspection, fmt.Errorf("row %d column %d has truncated field length: %w", inspection.Rows+1, columnIndex+1, err)
			}
			if fieldLength == -1 {
				if dst != nil {
					if err := writeInt32(dst, -1); err != nil {
						return inspection, err
					}
				}
				continue
			}
			if fieldLength < -1 {
				return inspection, fmt.Errorf("row %d column %d has invalid field length %d", inspection.Rows+1, columnIndex+1, fieldLength)
			}
			if column.fixedWidth > 0 && int(fieldLength) != column.fixedWidth {
				return inspection, fmt.Errorf(
					"row %d column %d %s field length %d, expected %d",
					inspection.Rows+1,
					columnIndex+1,
					column.databaseTypeName,
					fieldLength,
					column.fixedWidth,
				)
			}

			if column.Numeric == nil {
				if dst != nil {
					if err := writeInt32(dst, fieldLength); err != nil {
						return inspection, err
					}
					if err := copyExact(dst, src, int64(fieldLength)); err != nil {
						return inspection, fmt.Errorf("row %d column %d has truncated field data: %w", inspection.Rows+1, columnIndex+1, err)
					}
				} else if err := discardExact(src, int64(fieldLength)); err != nil {
					return inspection, fmt.Errorf("row %d column %d has truncated field data: %w", inspection.Rows+1, columnIndex+1, err)
				}
				continue
			}

			payload, err := readNumericPayload(src, fieldLength)
			if err != nil {
				return inspection, fmt.Errorf("row %d column %d has invalid NUMERIC: %w", inspection.Rows+1, columnIndex+1, err)
			}
			normalized, err := normalizeNumeric(payload, *column.Numeric)
			if err != nil {
				return inspection, fmt.Errorf("row %d column %d: %w", inspection.Rows+1, columnIndex+1, err)
			}
			if !bytes.Equal(payload, normalized) {
				inspection.NeedsRewrite = true
			}
			payload = normalized
			if dst != nil {
				if err := writeInt32(dst, int32(len(payload))); err != nil {
					return inspection, err
				}
				if err := writeAll(dst, payload); err != nil {
					return inspection, err
				}
			}
		}
		inspection.Rows++
	}
}

func validateSchema(schema Schema) error {
	if len(schema.Columns) == 0 {
		return fmt.Errorf("PostgreSQL binary COPY schema has no columns")
	}
	for i, column := range schema.Columns {
		if column.Numeric == nil {
			continue
		}
		if column.Numeric.Precision < 1 || column.Numeric.Precision > 38 ||
			int(column.Numeric.Scale) > column.Numeric.Precision {
			return fmt.Errorf("column %d has invalid DECIMAL(%d,%d)", i+1, column.Numeric.Precision, column.Numeric.Scale)
		}
	}
	return nil
}

func readNumericPayload(src io.Reader, fieldLength int32) ([]byte, error) {
	if fieldLength < 8 {
		return nil, fmt.Errorf("field length %d is smaller than the 8-byte header", fieldLength)
	}
	header := make([]byte, 8)
	if _, err := io.ReadFull(src, header); err != nil {
		return nil, err
	}
	ndigits := int(binary.BigEndian.Uint16(header[0:2]))
	expectedLength := 8 + ndigits*2
	if int(fieldLength) != expectedLength {
		return nil, fmt.Errorf("field length %d does not match ndigits %d (expected %d)", fieldLength, ndigits, expectedLength)
	}
	payload := make([]byte, expectedLength)
	copy(payload, header)
	if _, err := io.ReadFull(src, payload[8:]); err != nil {
		return nil, err
	}
	return payload, nil
}

func validateNumeric(payload []byte) error {
	if len(payload) < 8 {
		return fmt.Errorf("payload is shorter than the 8-byte header")
	}
	ndigits := int(binary.BigEndian.Uint16(payload[0:2]))
	if len(payload) != 8+ndigits*2 {
		return fmt.Errorf("payload length %d does not match ndigits %d", len(payload), ndigits)
	}
	sign := binary.BigEndian.Uint16(payload[4:6])
	dscale := binary.BigEndian.Uint16(payload[6:8])
	if dscale > 0x3fff {
		return fmt.Errorf("NUMERIC dscale %d exceeds PostgreSQL's 0x3fff mask", dscale)
	}
	switch sign {
	case numericPositive, numericNegative:
	case numericNaN, numericPosInf, numericNegInf:
		return fmt.Errorf("special NUMERIC values are not supported by a DECIMAL destination")
	default:
		return fmt.Errorf("invalid NUMERIC sign 0x%04x", sign)
	}
	for i := 0; i < ndigits; i++ {
		digit := binary.BigEndian.Uint16(payload[8+i*2 : 10+i*2])
		if digit >= 10000 {
			return fmt.Errorf("base-10000 digit %d is out of range", digit)
		}
	}
	return nil
}

func normalizeNumeric(payload []byte, target Numeric) ([]byte, error) {
	if err := validateNumeric(payload); err != nil {
		return nil, fmt.Errorf("invalid NUMERIC: %w", err)
	}
	ndigits := int(binary.BigEndian.Uint16(payload[0:2]))
	weight := int(int16(binary.BigEndian.Uint16(payload[2:4])))
	sign := binary.BigEndian.Uint16(payload[4:6])
	sourceScale := int(binary.BigEndian.Uint16(payload[6:8]))

	coefficient := new(big.Int)
	base := big.NewInt(10000)
	for i := 0; i < ndigits; i++ {
		coefficient.Mul(coefficient, base)
		coefficient.Add(coefficient, new(big.Int).SetUint64(uint64(binary.BigEndian.Uint16(payload[8+i*2:10+i*2]))))
	}

	// numeric_recv first truncates the base-10000 value to the wire dscale.
	// Only then does PostgreSQL apply the destination typmod, which may round.
	sourceShift := sourceScale + 4*(weight-ndigits+1)
	sourceUnscaled := new(big.Int).Set(coefficient)
	if sourceShift >= 0 {
		sourceUnscaled.Mul(sourceUnscaled, powerOfTen(sourceShift))
	} else {
		sourceUnscaled.Quo(sourceUnscaled, powerOfTen(-sourceShift))
	}

	unscaled := new(big.Int).Set(sourceUnscaled)
	targetShift := int(target.Scale) - sourceScale
	if targetShift >= 0 {
		unscaled.Mul(unscaled, powerOfTen(targetShift))
	} else {
		divisor := powerOfTen(-targetShift)
		remainder := new(big.Int)
		unscaled.QuoRem(unscaled, divisor, remainder)
		remainder.Lsh(remainder, 1)
		if remainder.Cmp(divisor) >= 0 {
			unscaled.Add(unscaled, big.NewInt(1))
		}
	}

	if len(unscaled.String()) > target.Precision {
		return nil, fmt.Errorf("NUMERIC value exceeds DECIMAL(%d,%d)", target.Precision, target.Scale)
	}
	return encodeNumeric(unscaled, sign == numericNegative, target.Scale), nil
}

func encodeNumeric(unscaled *big.Int, negative bool, scale uint16) []byte {
	value := new(big.Int).Abs(new(big.Int).Set(unscaled))
	if value.Sign() == 0 {
		result := make([]byte, 8)
		binary.BigEndian.PutUint16(result[6:8], scale)
		return result
	}

	scaleInt := int(scale)
	fractionalGroups := (scaleInt + 3) / 4
	padding := fractionalGroups*4 - scaleInt
	if padding > 0 {
		value.Mul(value, powerOfTen(padding))
	}

	base := big.NewInt(10000)
	var reversed []uint16
	for value.Sign() > 0 {
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(value, base, remainder)
		reversed = append(reversed, uint16(remainder.Uint64()))
		value = quotient
	}
	digits := make([]uint16, len(reversed))
	for i := range reversed {
		digits[len(reversed)-1-i] = reversed[i]
	}

	integerGroups := len(digits) - fractionalGroups
	if integerGroups < 0 {
		digits = append(make([]uint16, -integerGroups), digits...)
		integerGroups = 0
	}
	weight := integerGroups - 1

	for len(digits) > 0 && digits[len(digits)-1] == 0 {
		digits = digits[:len(digits)-1]
	}
	for len(digits) > 0 && digits[0] == 0 {
		digits = digits[1:]
		weight--
	}

	result := make([]byte, 8+len(digits)*2)
	binary.BigEndian.PutUint16(result[0:2], uint16(len(digits)))
	binary.BigEndian.PutUint16(result[2:4], uint16(int16(weight)))
	if negative {
		binary.BigEndian.PutUint16(result[4:6], numericNegative)
	}
	binary.BigEndian.PutUint16(result[6:8], scale)
	for i, digit := range digits {
		binary.BigEndian.PutUint16(result[8+i*2:10+i*2], digit)
	}
	return result
}

func powerOfTen(exponent int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil)
}

func copyExact(dst io.Writer, src io.Reader, count int64) error {
	_, err := io.CopyN(dst, src, count)
	return err
}

func discardExact(src io.Reader, count int64) error {
	_, err := io.CopyN(io.Discard, src, count)
	return err
}

func writeAll(dst io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := dst.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func readUint32(src io.Reader) (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(src, buf[:])
	return binary.BigEndian.Uint32(buf[:]), err
}

func readInt32(src io.Reader) (int32, error) {
	value, err := readUint32(src)
	return int32(value), err
}

func readInt16(src io.Reader) (int16, error) {
	var buf [2]byte
	_, err := io.ReadFull(src, buf[:])
	return int16(binary.BigEndian.Uint16(buf[:])), err
}

func writeUint32(dst io.Writer, value uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	return writeAll(dst, buf[:])
}

func writeInt32(dst io.Writer, value int32) error {
	return writeUint32(dst, uint32(value))
}

func writeInt16(dst io.Writer, value int16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(value))
	return writeAll(dst, buf[:])
}
