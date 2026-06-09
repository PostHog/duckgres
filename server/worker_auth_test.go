package server

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/posthog/duckgres/server/wire"
)

// passwordMessage builds a client PasswordMessage ('p') for the given password.
func passwordMessage(t *testing.T, password string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := wire.WriteMessage(&buf, wire.MsgPassword, append([]byte(password), 0)); err != nil {
		t.Fatalf("failed to build password message: %v", err)
	}
	return buf.Bytes()
}

// runChildAuth feeds clientBytes to authenticateChildClient and returns the
// exit code plus everything the server wrote to the client.
func runChildAuth(t *testing.T, users map[string]string, username string, clientBytes []byte) (int, []byte) {
	t.Helper()
	var out bytes.Buffer
	reader := bufio.NewReader(bytes.NewReader(clientBytes))
	writer := bufio.NewWriter(&out)
	exitCode := authenticateChildClient(reader, writer, users, username, "test:5432")
	if err := writer.Flush(); err != nil {
		t.Fatalf("failed to flush writer: %v", err)
	}
	return exitCode, out.Bytes()
}

func TestAuthenticateChildClientSuccess(t *testing.T) {
	users := map[string]string{"alice": "secret"}

	exitCode, out := runChildAuth(t, users, "alice", passwordMessage(t, "secret"))
	if exitCode != ExitSuccess {
		t.Fatalf("expected ExitSuccess, got %d", exitCode)
	}

	reader := bytes.NewReader(out)
	msgType, _, err := wire.ReadMessage(reader)
	if err != nil || msgType != wire.MsgAuth {
		t.Fatalf("expected cleartext password request, got %c (err %v)", msgType, err)
	}
	msgType, body, err := wire.ReadMessage(reader)
	if err != nil || msgType != wire.MsgAuth {
		t.Fatalf("expected AuthOK, got %c (err %v)", msgType, err)
	}
	if !bytes.Equal(body, []byte{0, 0, 0, 0}) {
		t.Fatalf("expected AuthOK body, got %v", body)
	}
}

// TestAuthenticateChildClientUnknownUserIndistinguishable is a regression test
// for the user-existence oracle: the worker used to return 28P01 for unknown
// users before requesting a password, while known users got a password request
// first. Both failure paths must now be byte-for-byte identical to the client.
func TestAuthenticateChildClientUnknownUserIndistinguishable(t *testing.T) {
	users := map[string]string{"alice": "secret"}

	unknownCode, unknownOut := runChildAuth(t, users, "mallory", passwordMessage(t, "secret"))
	wrongPwCode, wrongPwOut := runChildAuth(t, users, "alice", passwordMessage(t, "wrong"))

	if unknownCode != ExitAuthFailure {
		t.Fatalf("unknown user: expected ExitAuthFailure, got %d", unknownCode)
	}
	if wrongPwCode != ExitAuthFailure {
		t.Fatalf("wrong password: expected ExitAuthFailure, got %d", wrongPwCode)
	}

	// Same message flow, same error code/message shape — no existence oracle.
	if !bytes.Equal(unknownOut, wrongPwOut) {
		t.Fatalf("unknown-user and wrong-password byte streams differ:\nunknown:  %q\nwrong-pw: %q", unknownOut, wrongPwOut)
	}

	// Both flows: password request first, then a generic 28P01 error.
	reader := bytes.NewReader(unknownOut)
	msgType, _, err := wire.ReadMessage(reader)
	if err != nil || msgType != wire.MsgAuth {
		t.Fatalf("expected cleartext password request before failure, got %c (err %v)", msgType, err)
	}
	msgType, body, err := wire.ReadMessage(reader)
	if err != nil || msgType != wire.MsgErrorResponse {
		t.Fatalf("expected ErrorResponse, got %c (err %v)", msgType, err)
	}
	if !bytes.Contains(body, []byte("28P01")) {
		t.Fatalf("expected SQLSTATE 28P01 in error, got %q", body)
	}
	if !bytes.Contains(body, []byte("password authentication failed")) {
		t.Fatalf("expected generic auth failure message, got %q", body)
	}
	if bytes.Contains(body, []byte("mallory")) || bytes.Contains(body, []byte("alice")) {
		t.Fatalf("error message must not echo the username, got %q", body)
	}
}
