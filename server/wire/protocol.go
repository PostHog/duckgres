package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PostgreSQL message types
const (
	// Frontend (client) messages
	MsgQuery       = 'Q'
	MsgTerminate   = 'X'
	MsgPassword    = 'p'
	MsgParse       = 'P'
	MsgBind        = 'B'
	MsgDescribe    = 'D'
	MsgExecute     = 'E'
	MsgSync        = 'S'
	MsgClose       = 'C'
	MsgFlush       = 'H'

	// Backend (server) messages
	MsgAuth            = 'R'
	MsgParamStatus     = 'S'
	MsgBackendKeyData  = 'K'
	MsgReadyForQuery   = 'Z'
	MsgRowDescription  = 'T'
	MsgDataRow         = 'D'
	MsgCommandComplete = 'C'
	MsgErrorResponse   = 'E'
	MsgNoticeResponse  = 'N'
	MsgEmptyQuery      = 'I'
	MsgParseComplete   = '1'
	MsgBindComplete    = '2'
	MsgCloseComplete   = '3'
	MsgNoData          = 'n'

	// COPY messages (both directions)
	MsgCopyData        = 'd' // Contains COPY data
	MsgCopyDone        = 'c' // COPY completed
	MsgCopyFail        = 'f' // COPY failed (frontend only)
	MsgCopyInResponse  = 'G' // Server ready to receive COPY data
	MsgCopyOutResponse = 'H' // Server about to send COPY data
)

// Authentication types
const (
	authOK              = 0
	authCleartextPwd    = 3
	authMD5Pwd          = 5
)

// readStartupMessage reads the initial startup message from the client
func ReadStartupMessage(r io.Reader) (map[string]string, error) {
	// Read message length (4 bytes)
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to read startup message length: %w", err)
	}

	// Read remaining bytes
	remaining := make([]byte, length-4)
	if _, err := io.ReadFull(r, remaining); err != nil {
		return nil, fmt.Errorf("failed to read startup message body: %w", err)
	}

	// Read protocol version (4 bytes)
	protocolVersion := binary.BigEndian.Uint32(remaining[:4])

	// Check for SSL request (80877103)
	if protocolVersion == 80877103 {
		return map[string]string{"__ssl_request": "true"}, nil
	}

	// Check for GSSENCRequest (80877104, PostgreSQL 12+)
	// JDBC drivers with gssEncMode=prefer send this before SSLRequest.
	if protocolVersion == 80877104 {
		return map[string]string{"__gssenc_request": "true"}, nil
	}

	// Check for cancel request (80877102)
	// Format: 4 bytes length, 4 bytes request code, 4 bytes pid, 4 bytes secret key
	if protocolVersion == 80877102 {
		if len(remaining) >= 12 {
			cancelPid := binary.BigEndian.Uint32(remaining[4:8])
			cancelSecretKey := binary.BigEndian.Uint32(remaining[8:12])
			return map[string]string{
				"__cancel_request":    "true",
				"__cancel_pid":        fmt.Sprintf("%d", cancelPid),
				"__cancel_secret_key": fmt.Sprintf("%d", cancelSecretKey),
			}, nil
		}
		return map[string]string{"__cancel_request": "true"}, nil
	}

	// Parse parameters (null-terminated key-value pairs)
	params := make(map[string]string)
	data := remaining[4:] // Skip protocol version

	for len(data) > 1 {
		// Find key
		keyEnd := 0
		for keyEnd < len(data) && data[keyEnd] != 0 {
			keyEnd++
		}
		if keyEnd >= len(data) {
			break
		}
		key := string(data[:keyEnd])
		data = data[keyEnd+1:]

		// Find value
		valEnd := 0
		for valEnd < len(data) && data[valEnd] != 0 {
			valEnd++
		}
		if valEnd > len(data) {
			break
		}
		value := string(data[:valEnd])
		data = data[valEnd+1:]

		if key != "" {
			params[key] = value
		}
	}

	return params, nil
}

// readMessage reads a single message from the client
func ReadMessage(r io.Reader) (byte, []byte, error) {
	// Read message type (1 byte)
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return 0, nil, err
	}
	msgType := typeBuf[0]

	// Read message length (4 bytes, includes itself)
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return 0, nil, fmt.Errorf("failed to read message length: %w", err)
	}

	// Read message body
	body := make([]byte, length-4)
	if length > 4 {
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, nil, fmt.Errorf("failed to read message body: %w", err)
		}
	}

	return msgType, body, nil
}

// writeMessage writes a message to the client
func WriteMessage(w io.Writer, msgType byte, data []byte) error {
	// Write message type
	if _, err := w.Write([]byte{msgType}); err != nil {
		return err
	}

	// Write length (includes itself, 4 bytes)
	length := int32(len(data) + 4)
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	// Write data
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	return nil
}

// writeAuthOK sends authentication success
func WriteAuthOK(w io.Writer) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, authOK)
	return WriteMessage(w, MsgAuth, data)
}

// writeAuthCleartextPassword requests cleartext password
func WriteAuthCleartextPassword(w io.Writer) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, authCleartextPwd)
	return WriteMessage(w, MsgAuth, data)
}

// writeParameterStatus sends a parameter status message
func WriteParameterStatus(w io.Writer, name, value string) error {
	data := []byte(name)
	data = append(data, 0)
	data = append(data, []byte(value)...)
	data = append(data, 0)
	return WriteMessage(w, MsgParamStatus, data)
}

// writeBackendKeyData sends the backend key data (for cancel requests)
func WriteBackendKeyData(w io.Writer, pid, secretKey int32) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[:4], uint32(pid))
	binary.BigEndian.PutUint32(data[4:], uint32(secretKey))
	return WriteMessage(w, MsgBackendKeyData, data)
}

// writeReadyForQuery indicates the server is ready for a new query
func WriteReadyForQuery(w io.Writer, txStatus byte) error {
	return WriteMessage(w, MsgReadyForQuery, []byte{txStatus})
}

// writeErrorResponse sends an error to the client
func WriteErrorResponse(w io.Writer, severity, code, message string) error {
	message = RedactSecrets(message)

	var data []byte

	// Severity
	data = append(data, 'S')
	data = append(data, []byte(severity)...)
	data = append(data, 0)

	// SQLSTATE code
	data = append(data, 'C')
	data = append(data, []byte(code)...)
	data = append(data, 0)

	// Message
	data = append(data, 'M')
	data = append(data, []byte(message)...)
	data = append(data, 0)

	// Terminator
	data = append(data, 0)

	return WriteMessage(w, MsgErrorResponse, data)
}

// writeNoticeResponse sends a notice/warning to the client
// Unlike errors, notices are informational and don't terminate the command
func WriteNoticeResponse(w io.Writer, severity, code, message string) error {
	var data []byte

	// Severity (WARNING, NOTICE, INFO, DEBUG, LOG)
	data = append(data, 'S')
	data = append(data, []byte(severity)...)
	data = append(data, 0)

	// SQLSTATE code
	data = append(data, 'C')
	data = append(data, []byte(code)...)
	data = append(data, 0)

	// Message
	data = append(data, 'M')
	data = append(data, []byte(message)...)
	data = append(data, 0)

	// Terminator
	data = append(data, 0)

	return WriteMessage(w, MsgNoticeResponse, data)
}

// writeCommandComplete sends a command completion message
func WriteCommandComplete(w io.Writer, tag string) error {
	data := []byte(tag)
	data = append(data, 0)
	return WriteMessage(w, MsgCommandComplete, data)
}

// writeEmptyQueryResponse sends an empty query response
func WriteEmptyQueryResponse(w io.Writer) error {
	return WriteMessage(w, MsgEmptyQuery, nil)
}

// writeParseComplete sends parse complete
func WriteParseComplete(w io.Writer) error {
	return WriteMessage(w, MsgParseComplete, nil)
}

// writeBindComplete sends bind complete
func WriteBindComplete(w io.Writer) error {
	return WriteMessage(w, MsgBindComplete, nil)
}

// writeCloseComplete sends close complete
func WriteCloseComplete(w io.Writer) error {
	return WriteMessage(w, MsgCloseComplete, nil)
}

// writeNoData sends no data response
func WriteNoData(w io.Writer) error {
	return WriteMessage(w, MsgNoData, nil)
}

// writeCopyOutResponse tells client we're about to send COPY data
// Format: overall format (0=text, 1=binary), num columns, format per column
func WriteCopyOutResponse(w io.Writer, numColumns int16, textFormat bool) error {
	var data []byte

	// Overall format (0=text)
	if textFormat {
		data = append(data, 0)
	} else {
		data = append(data, 1)
	}

	// Number of columns
	colBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(colBytes, uint16(numColumns))
	data = append(data, colBytes...)

	// Format for each column (0=text)
	for i := int16(0); i < numColumns; i++ {
		formatBytes := make([]byte, 2)
		if textFormat {
			binary.BigEndian.PutUint16(formatBytes, 0)
		} else {
			binary.BigEndian.PutUint16(formatBytes, 1)
		}
		data = append(data, formatBytes...)
	}

	return WriteMessage(w, MsgCopyOutResponse, data)
}

// writeCopyInResponse tells client to send COPY data
func WriteCopyInResponse(w io.Writer, numColumns int16, textFormat bool) error {
	var data []byte

	// Overall format (0=text)
	if textFormat {
		data = append(data, 0)
	} else {
		data = append(data, 1)
	}

	// Number of columns
	colBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(colBytes, uint16(numColumns))
	data = append(data, colBytes...)

	// Format for each column (0=text)
	for i := int16(0); i < numColumns; i++ {
		formatBytes := make([]byte, 2)
		if textFormat {
			binary.BigEndian.PutUint16(formatBytes, 0)
		} else {
			binary.BigEndian.PutUint16(formatBytes, 1)
		}
		data = append(data, formatBytes...)
	}

	return WriteMessage(w, MsgCopyInResponse, data)
}

// writeCopyData sends a row of COPY data
func WriteCopyData(w io.Writer, data []byte) error {
	return WriteMessage(w, MsgCopyData, data)
}

// writeCopyDone signals the end of COPY data
func WriteCopyDone(w io.Writer) error {
	return WriteMessage(w, MsgCopyDone, nil)
}
