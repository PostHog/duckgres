package server

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadStartupMessage(t *testing.T) {
	t.Run("valid startup message", func(t *testing.T) {
		// Build a startup message:
		// - 4 bytes length (including itself)
		// - 4 bytes protocol version (196608 = 3.0)
		// - key-value pairs null-terminated
		// - final null byte
		var buf bytes.Buffer

		params := map[string]string{
			"user":     "testuser",
			"database": "testdb",
		}

		// Calculate length
		bodyLen := 4 // protocol version
		for k, v := range params {
			bodyLen += len(k) + 1 + len(v) + 1
		}
		bodyLen++ // final null

		// Write length (includes itself)
		_ = binary.Write(&buf, binary.BigEndian, int32(bodyLen+4))

		// Write protocol version (3.0 = 196608)
		_ = binary.Write(&buf, binary.BigEndian, uint32(196608))

		// Write params
		for k, v := range params {
			buf.WriteString(k)
			buf.WriteByte(0)
			buf.WriteString(v)
			buf.WriteByte(0)
		}
		buf.WriteByte(0) // final null

		result, err := readStartupMessage(&buf)
		if err != nil {
			t.Fatalf("readStartupMessage() error = %v", err)
		}

		if result["user"] != "testuser" {
			t.Errorf("user = %q, want %q", result["user"], "testuser")
		}
		if result["database"] != "testdb" {
			t.Errorf("database = %q, want %q", result["database"], "testdb")
		}
	})

	t.Run("SSL request", func(t *testing.T) {
		var buf bytes.Buffer

		// SSL request: length=8, version=80877103
		_ = binary.Write(&buf, binary.BigEndian, int32(8))
		_ = binary.Write(&buf, binary.BigEndian, uint32(80877103))

		result, err := readStartupMessage(&buf)
		if err != nil {
			t.Fatalf("readStartupMessage() error = %v", err)
		}

		if result["__ssl_request"] != "true" {
			t.Error("should detect SSL request")
		}
	})

	t.Run("cancel request", func(t *testing.T) {
		var buf bytes.Buffer

		// Cancel request: length=16, version=80877102, pid, key
		_ = binary.Write(&buf, binary.BigEndian, int32(16))
		_ = binary.Write(&buf, binary.BigEndian, uint32(80877102))
		_ = binary.Write(&buf, binary.BigEndian, uint32(12345)) // pid
		_ = binary.Write(&buf, binary.BigEndian, uint32(67890)) // key

		result, err := readStartupMessage(&buf)
		if err != nil {
			t.Fatalf("readStartupMessage() error = %v", err)
		}

		if result["__cancel_request"] != "true" {
			t.Error("should detect cancel request")
		}
	})
}

func TestReadMessage(t *testing.T) {
	t.Run("simple query message", func(t *testing.T) {
		var buf bytes.Buffer

		query := "SELECT 1"
		// Query message: 'Q' + length + query + null
		buf.WriteByte('Q')
		_ = binary.Write(&buf, binary.BigEndian, int32(len(query)+5)) // length includes itself and null
		buf.WriteString(query)
		buf.WriteByte(0)

		msgType, body, err := readMessage(&buf)
		if err != nil {
			t.Fatalf("readMessage() error = %v", err)
		}

		if msgType != 'Q' {
			t.Errorf("msgType = %c, want Q", msgType)
		}

		// Body includes the null terminator
		expectedBody := query + "\x00"
		if string(body) != expectedBody {
			t.Errorf("body = %q, want %q", string(body), expectedBody)
		}
	})

	t.Run("terminate message", func(t *testing.T) {
		var buf bytes.Buffer

		// Terminate message: 'X' + length (4, just the length itself)
		buf.WriteByte('X')
		_ = binary.Write(&buf, binary.BigEndian, int32(4))

		msgType, body, err := readMessage(&buf)
		if err != nil {
			t.Fatalf("readMessage() error = %v", err)
		}

		if msgType != 'X' {
			t.Errorf("msgType = %c, want X", msgType)
		}

		if len(body) != 0 {
			t.Errorf("body length = %d, want 0", len(body))
		}
	})

	t.Run("message with binary data", func(t *testing.T) {
		var buf bytes.Buffer

		data := []byte{0x01, 0x02, 0x03, 0x00, 0xFF}
		buf.WriteByte('d') // CopyData
		_ = binary.Write(&buf, binary.BigEndian, int32(len(data)+4))
		buf.Write(data)

		msgType, body, err := readMessage(&buf)
		if err != nil {
			t.Fatalf("readMessage() error = %v", err)
		}

		if msgType != 'd' {
			t.Errorf("msgType = %c, want d", msgType)
		}

		if !bytes.Equal(body, data) {
			t.Errorf("body = %v, want %v", body, data)
		}
	})
}

func TestWriteMessage(t *testing.T) {
	t.Run("write simple message", func(t *testing.T) {
		var buf bytes.Buffer

		data := []byte("test data")
		err := writeMessage(&buf, 'T', data)
		if err != nil {
			t.Fatalf("writeMessage() error = %v", err)
		}

		// Check message type
		if buf.Bytes()[0] != 'T' {
			t.Errorf("message type = %c, want T", buf.Bytes()[0])
		}

		// Check length (includes itself = 4)
		length := binary.BigEndian.Uint32(buf.Bytes()[1:5])
		if length != uint32(len(data)+4) {
			t.Errorf("length = %d, want %d", length, len(data)+4)
		}

		// Check data
		if !bytes.Equal(buf.Bytes()[5:], data) {
			t.Errorf("data = %v, want %v", buf.Bytes()[5:], data)
		}
	})

	t.Run("write empty message", func(t *testing.T) {
		var buf bytes.Buffer

		err := writeMessage(&buf, 'Z', []byte{})
		if err != nil {
			t.Fatalf("writeMessage() error = %v", err)
		}

		// Total should be 5 bytes: type (1) + length (4)
		if buf.Len() != 5 {
			t.Errorf("buffer length = %d, want 5", buf.Len())
		}

		// Length should be 4 (just the length field itself)
		length := binary.BigEndian.Uint32(buf.Bytes()[1:5])
		if length != 4 {
			t.Errorf("length = %d, want 4", length)
		}
	})
}

func TestWriteAuthOK(t *testing.T) {
	var buf bytes.Buffer

	err := writeAuthOK(&buf)
	if err != nil {
		t.Fatalf("writeAuthOK() error = %v", err)
	}

	// Message type should be 'R'
	if buf.Bytes()[0] != 'R' {
		t.Errorf("message type = %c, want R", buf.Bytes()[0])
	}

	// Length should be 8 (4 for length + 4 for auth type)
	length := binary.BigEndian.Uint32(buf.Bytes()[1:5])
	if length != 8 {
		t.Errorf("length = %d, want 8", length)
	}

	// Auth type should be 0 (OK)
	authType := binary.BigEndian.Uint32(buf.Bytes()[5:9])
	if authType != 0 {
		t.Errorf("auth type = %d, want 0", authType)
	}
}

func TestWriteAuthCleartextPassword(t *testing.T) {
	var buf bytes.Buffer

	err := writeAuthCleartextPassword(&buf)
	if err != nil {
		t.Fatalf("writeAuthCleartextPassword() error = %v", err)
	}

	// Auth type should be 3 (cleartext password)
	authType := binary.BigEndian.Uint32(buf.Bytes()[5:9])
	if authType != 3 {
		t.Errorf("auth type = %d, want 3", authType)
	}
}

func TestWriteParameterStatus(t *testing.T) {
	var buf bytes.Buffer

	err := writeParameterStatus(&buf, "server_version", "15.0")
	if err != nil {
		t.Fatalf("writeParameterStatus() error = %v", err)
	}

	// Message type should be 'S'
	if buf.Bytes()[0] != 'S' {
		t.Errorf("message type = %c, want S", buf.Bytes()[0])
	}

	// Check data contains null-terminated name and value
	data := buf.Bytes()[5:]
	parts := bytes.Split(data, []byte{0})

	if string(parts[0]) != "server_version" {
		t.Errorf("name = %q, want %q", string(parts[0]), "server_version")
	}
	if string(parts[1]) != "15.0" {
		t.Errorf("value = %q, want %q", string(parts[1]), "15.0")
	}
}

func TestWriteBackendKeyData(t *testing.T) {
	var buf bytes.Buffer

	err := writeBackendKeyData(&buf, 12345, 67890)
	if err != nil {
		t.Fatalf("writeBackendKeyData() error = %v", err)
	}

	// Message type should be 'K'
	if buf.Bytes()[0] != 'K' {
		t.Errorf("message type = %c, want K", buf.Bytes()[0])
	}

	// Length should be 12 (4 for length + 4 for pid + 4 for key)
	length := binary.BigEndian.Uint32(buf.Bytes()[1:5])
	if length != 12 {
		t.Errorf("length = %d, want 12", length)
	}

	// Check pid
	pid := binary.BigEndian.Uint32(buf.Bytes()[5:9])
	if pid != 12345 {
		t.Errorf("pid = %d, want 12345", pid)
	}

	// Check key
	key := binary.BigEndian.Uint32(buf.Bytes()[9:13])
	if key != 67890 {
		t.Errorf("key = %d, want 67890", key)
	}
}

func TestWriteReadyForQuery(t *testing.T) {
	tests := []struct {
		name     string
		txStatus byte
	}{
		{"idle", 'I'},
		{"in transaction", 'T'},
		{"failed transaction", 'E'},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			err := writeReadyForQuery(&buf, tt.txStatus)
			if err != nil {
				t.Fatalf("writeReadyForQuery() error = %v", err)
			}

			// Message type should be 'Z'
			if buf.Bytes()[0] != 'Z' {
				t.Errorf("message type = %c, want Z", buf.Bytes()[0])
			}

			// Length should be 5 (4 for length + 1 for status)
			length := binary.BigEndian.Uint32(buf.Bytes()[1:5])
			if length != 5 {
				t.Errorf("length = %d, want 5", length)
			}

			// Check status
			if buf.Bytes()[5] != tt.txStatus {
				t.Errorf("txStatus = %c, want %c", buf.Bytes()[5], tt.txStatus)
			}
		})
	}
}

func TestWriteErrorResponse(t *testing.T) {
	var buf bytes.Buffer

	err := writeErrorResponse(&buf, "ERROR", "42601", "syntax error")
	if err != nil {
		t.Fatalf("writeErrorResponse() error = %v", err)
	}

	// Message type should be 'E'
	if buf.Bytes()[0] != 'E' {
		t.Errorf("message type = %c, want E", buf.Bytes()[0])
	}

	// Parse the error fields
	data := buf.Bytes()[5:]

	// Should contain 'S' (severity), 'C' (code), 'M' (message)
	if !bytes.Contains(data, []byte("SERROR")) {
		t.Error("should contain severity ERROR")
	}
	if !bytes.Contains(data, []byte("C42601")) {
		t.Error("should contain code 42601")
	}
	if !bytes.Contains(data, []byte("Msyntax error")) {
		t.Error("should contain message 'syntax error'")
	}
}

func TestWriteNoticeResponse(t *testing.T) {
	var buf bytes.Buffer

	err := writeNoticeResponse(&buf, "WARNING", "01000", "some warning")
	if err != nil {
		t.Fatalf("writeNoticeResponse() error = %v", err)
	}

	// Message type should be 'N' (notice)
	if buf.Bytes()[0] != 'N' {
		t.Errorf("message type = %c, want N", buf.Bytes()[0])
	}

	data := buf.Bytes()[5:]
	if !bytes.Contains(data, []byte("SWARNING")) {
		t.Error("should contain severity WARNING")
	}
}

func TestMessageRoundTrip(t *testing.T) {
	// Test that we can write a message and read it back
	testCases := []struct {
		name    string
		msgType byte
		data    []byte
	}{
		{"empty", 'Z', []byte{}},
		{"single byte", 'T', []byte{42}},
		{"text", 'Q', []byte("SELECT 1\x00")},
		{"binary", 'd', []byte{0x00, 0xFF, 0x01, 0xFE}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Write
			err := writeMessage(&buf, tc.msgType, tc.data)
			if err != nil {
				t.Fatalf("writeMessage() error = %v", err)
			}

			// Read
			msgType, body, err := readMessage(&buf)
			if err != nil {
				t.Fatalf("readMessage() error = %v", err)
			}

			if msgType != tc.msgType {
				t.Errorf("msgType = %c, want %c", msgType, tc.msgType)
			}

			if !bytes.Equal(body, tc.data) {
				t.Errorf("body = %v, want %v", body, tc.data)
			}
		})
	}
}
