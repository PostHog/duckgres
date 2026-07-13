package server

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/binary"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server/wire"
)

func TestSendDataRowWithFormatsUsesTypeOIDForTextDates(t *testing.T) {
	date := time.Date(2022, 4, 1, 0, 0, 0, 0, time.UTC)
	farFutureDate := time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)
	positiveInfinity := time.Date(5881580, time.July, 11, 0, 0, 0, 0, time.UTC)
	negativeInfinity := time.Date(-5877641, time.June, 24, 0, 0, 0, 0, time.UTC)
	oneBC := time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)
	twoBC := time.Date(-1, time.January, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		value       any
		formatCodes []int16
		typeOID     int32
		want        string
	}{
		{
			name:    "implicit text format date",
			value:   farFutureDate,
			typeOID: OidDate,
			want:    "9999-12-31",
		},
		{
			name:        "explicit text format date",
			value:       farFutureDate,
			formatCodes: []int16{0},
			typeOID:     OidDate,
			want:        "9999-12-31",
		},
		{
			name:    "timestamp remains a timestamp",
			value:   date,
			typeOID: OidTimestamp,
			want:    "2022-04-01 00:00:00",
		},
		{
			name:    "date array",
			value:   []any{date, nil, date.AddDate(0, 0, 1)},
			typeOID: OidDateArray,
			want:    "{2022-04-01,NULL,2022-04-02}",
		},
		{
			name:    "positive infinity date",
			value:   positiveInfinity,
			typeOID: OidDate,
			want:    "infinity",
		},
		{
			name:    "negative infinity date",
			value:   negativeInfinity,
			typeOID: OidDate,
			want:    "-infinity",
		},
		{
			name:    "one BC date",
			value:   oneBC,
			typeOID: OidDate,
			want:    "0001-01-01 BC",
		},
		{
			name:    "two BC date",
			value:   twoBC,
			typeOID: OidDate,
			want:    "0002-01-01 BC",
		},
		{
			name:    "date array with infinities and BC dates",
			value:   []any{positiveInfinity, negativeInfinity, oneBC, twoBC},
			typeOID: OidDateArray,
			want:    `{infinity,-infinity,"0001-01-01 BC","0002-01-01 BC"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			conn := &clientConn{writer: bufio.NewWriter(&out)}

			if err := conn.sendDataRowWithFormats(
				[]any{tt.value},
				tt.formatCodes,
				[]int32{tt.typeOID},
			); err != nil {
				t.Fatalf("sendDataRowWithFormats: %v", err)
			}
			if err := conn.writer.Flush(); err != nil {
				t.Fatalf("flush DataRow: %v", err)
			}

			if got := oneColumnTextDataRow(t, out.Bytes()); got != tt.want {
				t.Fatalf("text DataRow = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatDateUsesDuckDBScannedInfinityValues(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	var positiveInfinity, negativeInfinity, oneBC, twoBC any
	err = db.QueryRow(`
		SELECT
			DATE 'infinity',
			DATE '-infinity',
			DATE '0001-01-01 (BC)',
			DATE '-0001-01-01'
	`).Scan(&positiveInfinity, &negativeInfinity, &oneBC, &twoBC)
	if err != nil {
		t.Fatalf("scan DuckDB DATE values: %v", err)
	}

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "positive infinity", value: positiveInfinity, want: "infinity"},
		{name: "negative infinity", value: negativeInfinity, want: "-infinity"},
		{name: "one BC", value: oneBC, want: "0001-01-01 BC"},
		{name: "two BC", value: twoBC, want: "0002-01-01 BC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, ok := tt.value.(time.Time)
			if !ok {
				t.Fatalf("DuckDB DATE type = %T, want time.Time", tt.value)
			}
			if got := formatDate(date); got != tt.want {
				t.Fatalf("formatDate(%v) = %q, want %q", date, got, tt.want)
			}
		})
	}
}

func oneColumnTextDataRow(t *testing.T, message []byte) string {
	t.Helper()

	if len(message) < 11 {
		t.Fatalf("DataRow message too short: %d bytes", len(message))
	}
	if message[0] != wire.MsgDataRow {
		t.Fatalf("message type = %q, want DataRow", message[0])
	}
	messageLength := int(binary.BigEndian.Uint32(message[1:5]))
	if messageLength != len(message)-1 {
		t.Fatalf("DataRow length = %d, want %d", messageLength, len(message)-1)
	}
	body := message[5:]
	if columnCount := binary.BigEndian.Uint16(body[:2]); columnCount != 1 {
		t.Fatalf("DataRow column count = %d, want 1", columnCount)
	}
	valueLength := int(int32(binary.BigEndian.Uint32(body[2:6])))
	if valueLength < 0 || valueLength != len(body)-6 {
		t.Fatalf("DataRow value length = %d, payload bytes = %d", valueLength, len(body)-6)
	}
	return string(body[6:])
}
