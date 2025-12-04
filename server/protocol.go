package server

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PostgreSQL message types
const (
	// Frontend (client) messages
	msgQuery       = 'Q'
	msgTerminate   = 'X'
	msgPassword    = 'p'
	msgParse       = 'P'
	msgBind        = 'B'
	msgDescribe    = 'D'
	msgExecute     = 'E'
	msgSync        = 'S'
	msgClose       = 'C'
	msgFlush       = 'H'

	// Backend (server) messages
	msgAuth            = 'R'
	msgParamStatus     = 'S'
	msgBackendKeyData  = 'K'
	msgReadyForQuery   = 'Z'
	msgRowDescription  = 'T'
	msgDataRow         = 'D'
	msgCommandComplete = 'C'
	msgErrorResponse   = 'E'
	msgNoticeResponse  = 'N'
	msgEmptyQuery      = 'I'
	msgParseComplete   = '1'
	msgBindComplete    = '2'
	msgCloseComplete   = '3'
	msgNoData          = 'n'

	// COPY messages (both directions)
	msgCopyData        = 'd' // Contains COPY data
	msgCopyDone        = 'c' // COPY completed
	msgCopyFail        = 'f' // COPY failed (frontend only)
	msgCopyInResponse  = 'G' // Server ready to receive COPY data
	msgCopyOutResponse = 'H' // Server about to send COPY data
)

// Authentication types
const (
	authOK              = 0
	authCleartextPwd    = 3
	authMD5Pwd          = 5
)

// readStartupMessage reads the initial startup message from the client
func readStartupMessage(r io.Reader) (map[string]string, error) {
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

	// Check for cancel request (80877102)
	if protocolVersion == 80877102 {
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
func readMessage(r io.Reader) (byte, []byte, error) {
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
func writeMessage(w io.Writer, msgType byte, data []byte) error {
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
func writeAuthOK(w io.Writer) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, authOK)
	return writeMessage(w, msgAuth, data)
}

// writeAuthCleartextPassword requests cleartext password
func writeAuthCleartextPassword(w io.Writer) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, authCleartextPwd)
	return writeMessage(w, msgAuth, data)
}

// writeParameterStatus sends a parameter status message
func writeParameterStatus(w io.Writer, name, value string) error {
	data := []byte(name)
	data = append(data, 0)
	data = append(data, []byte(value)...)
	data = append(data, 0)
	return writeMessage(w, msgParamStatus, data)
}

// writeBackendKeyData sends the backend key data (for cancel requests)
func writeBackendKeyData(w io.Writer, pid, secretKey int32) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[:4], uint32(pid))
	binary.BigEndian.PutUint32(data[4:], uint32(secretKey))
	return writeMessage(w, msgBackendKeyData, data)
}

// writeReadyForQuery indicates the server is ready for a new query
func writeReadyForQuery(w io.Writer, txStatus byte) error {
	return writeMessage(w, msgReadyForQuery, []byte{txStatus})
}

// writeErrorResponse sends an error to the client
func writeErrorResponse(w io.Writer, severity, code, message string) error {
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

	return writeMessage(w, msgErrorResponse, data)
}

// writeCommandComplete sends a command completion message
func writeCommandComplete(w io.Writer, tag string) error {
	data := []byte(tag)
	data = append(data, 0)
	return writeMessage(w, msgCommandComplete, data)
}

// writeEmptyQueryResponse sends an empty query response
func writeEmptyQueryResponse(w io.Writer) error {
	return writeMessage(w, msgEmptyQuery, nil)
}

// writeParseComplete sends parse complete
func writeParseComplete(w io.Writer) error {
	return writeMessage(w, msgParseComplete, nil)
}

// writeBindComplete sends bind complete
func writeBindComplete(w io.Writer) error {
	return writeMessage(w, msgBindComplete, nil)
}

// writeCloseComplete sends close complete
func writeCloseComplete(w io.Writer) error {
	return writeMessage(w, msgCloseComplete, nil)
}

// writeNoData sends no data response
func writeNoData(w io.Writer) error {
	return writeMessage(w, msgNoData, nil)
}

// writeCopyOutResponse tells client we're about to send COPY data
// Format: overall format (0=text, 1=binary), num columns, format per column
func writeCopyOutResponse(w io.Writer, numColumns int16, textFormat bool) error {
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

	return writeMessage(w, msgCopyOutResponse, data)
}

// writeCopyInResponse tells client to send COPY data
func writeCopyInResponse(w io.Writer, numColumns int16, textFormat bool) error {
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

	return writeMessage(w, msgCopyInResponse, data)
}

// writeCopyData sends a row of COPY data
func writeCopyData(w io.Writer, data []byte) error {
	return writeMessage(w, msgCopyData, data)
}

// writeCopyDone signals the end of COPY data
func writeCopyDone(w io.Writer) error {
	return writeMessage(w, msgCopyDone, nil)
}
