package wire

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// buildStartupMessage encodes a v3 startup packet: int32 length (includes
// itself), int32 protocol version, then null-terminated key/value pairs.
func buildStartupMessage(protocolVersion uint32, params map[string]string) []byte {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.BigEndian, protocolVersion)
	for k, v := range params {
		body.WriteString(k)
		body.WriteByte(0)
		body.WriteString(v)
		body.WriteByte(0)
	}
	body.WriteByte(0) // terminator

	var msg bytes.Buffer
	_ = binary.Write(&msg, binary.BigEndian, int32(body.Len()+4))
	msg.Write(body.Bytes())
	return msg.Bytes()
}

// buildRawStartup writes an arbitrary (possibly invalid) length header
// followed by payload bytes.
func buildRawStartup(length int32, payload []byte) []byte {
	var msg bytes.Buffer
	_ = binary.Write(&msg, binary.BigEndian, length)
	msg.Write(payload)
	return msg.Bytes()
}

func TestReadStartupMessageValid(t *testing.T) {
	msg := buildStartupMessage(196608, map[string]string{ // protocol 3.0
		"user":     "alice",
		"database": "db1",
	})
	params, err := ReadStartupMessage(bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("ReadStartupMessage: %v", err)
	}
	if params["user"] != "alice" || params["database"] != "db1" {
		t.Fatalf("unexpected params: %v", params)
	}
}

func TestReadStartupMessageSSLRequest(t *testing.T) {
	msg := buildStartupMessage(80877103, nil)
	// SSLRequest is exactly 8 bytes: length + request code.
	msg = msg[:8]
	binary.BigEndian.PutUint32(msg[:4], 8)
	params, err := ReadStartupMessage(bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("ReadStartupMessage: %v", err)
	}
	if params["__ssl_request"] != "true" {
		t.Fatalf("expected SSL request, got: %v", params)
	}
}

// Regression test for #715: malformed startup lengths must return an error,
// not panic with a negative/huge makeslice or an out-of-range slice. The
// startup message is read pre-auth, so a panic here is a remote DoS.
func TestReadStartupMessageInvalidLength(t *testing.T) {
	cases := []struct {
		name   string
		length int32
	}{
		{"negative", -1},
		{"most_negative", -2147483648},
		{"zero", 0},
		{"below_length_field", 3},
		{"length_only_no_version", 4}, // would slice remaining[:4] out of range
		{"partial_version", 7},
		{"just_over_cap", maxStartupMessageLength + 1},
		{"absurdly_large", 2147483647}, // would allocate ~2GiB
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := buildRawStartup(tc.length, nil)
			_, err := ReadStartupMessage(bytes.NewReader(msg))
			if err == nil {
				t.Fatalf("expected error for length %d, got nil", tc.length)
			}
			if !strings.Contains(err.Error(), "invalid startup message length") {
				t.Fatalf("unexpected error for length %d: %v", tc.length, err)
			}
		})
	}
}

func TestReadStartupMessageMaxLengthAccepted(t *testing.T) {
	// A startup message exactly at the cap must still be readable.
	body := make([]byte, maxStartupMessageLength-4)
	binary.BigEndian.PutUint32(body[:4], 196608) // protocol 3.0, rest zeros
	msg := buildRawStartup(maxStartupMessageLength, body)
	if _, err := ReadStartupMessage(bytes.NewReader(msg)); err != nil {
		t.Fatalf("ReadStartupMessage at max length: %v", err)
	}
}

// Regression test: a startup parameter whose final value lacks a trailing NUL
// must NOT panic. The value-scan guard used `valEnd > len(data)`, so an
// unterminated final value left valEnd == len(data), fell through to
// data[valEnd+1:] == data[len(data)+1:], and panicked with "slice bounds out
// of range". This is parsed PRE-AUTH on attacker-controlled bytes, so a panic
// crashes the whole shared control-plane process. The fix (`valEnd >=`) breaks
// out, returning the parameters parsed so far without error.
func TestReadStartupMessageUnterminatedValue(t *testing.T) {
	// v3.0 protocol version (196608 = 0x00030000) + "user\0X" with no trailing NUL.
	body := append([]byte{0x00, 0x03, 0x00, 0x00}, []byte("user\x00X")...)
	msg := buildRawStartup(int32(len(body)+4), body)

	// Must not panic.
	params, err := ReadStartupMessage(bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("ReadStartupMessage on unterminated value: %v", err)
	}
	// The truncated final pair is dropped (break before assignment).
	if _, ok := params["user"]; ok {
		t.Fatalf("expected truncated final pair to be dropped, got: %v", params)
	}
}

func TestReadMessageValid(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, MsgQuery, []byte("SELECT 1\x00")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	msgType, body, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msgType != MsgQuery || string(body) != "SELECT 1\x00" {
		t.Fatalf("unexpected message: type=%c body=%q", msgType, body)
	}
}

func TestReadMessageEmptyBody(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, MsgSync, nil); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	msgType, body, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msgType != MsgSync || len(body) != 0 {
		t.Fatalf("unexpected message: type=%c body=%q", msgType, body)
	}
}

// Regression test for #715 (hardening half): malformed regular-message
// lengths must return an error, not panic in make().
func TestReadMessageInvalidLength(t *testing.T) {
	cases := []struct {
		name   string
		length int32
	}{
		{"negative", -1},
		{"most_negative", -2147483648},
		{"zero", 0},
		{"below_length_field", 3},
		{"just_over_cap", maxMessageLength + 1},
		{"absurdly_large", 2147483647},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteByte(MsgQuery)
			_ = binary.Write(&buf, binary.BigEndian, tc.length)
			_, _, err := ReadMessage(&buf)
			if err == nil {
				t.Fatalf("expected error for length %d, got nil", tc.length)
			}
			if !strings.Contains(err.Error(), "invalid message length") {
				t.Fatalf("unexpected error for length %d: %v", tc.length, err)
			}
		})
	}
}
