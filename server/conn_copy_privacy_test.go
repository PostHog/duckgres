package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestGeneratedCopyLoadErrorResponseDoesNotExposeExecutionError(t *testing.T) {
	const leakMarker = "COPY generated_table FROM '/tmp/private-copy' VALUES ('private-value')"
	c := &clientConn{ctx: context.Background()}

	code, clientMessage, telemetryMessage := c.generatedCopyLoadErrorResponse(errors.New(leakMarker))

	if code != "22P02" {
		t.Fatalf("code = %q, want 22P02", code)
	}
	if clientMessage != "COPY failed: worker load failed" {
		t.Fatalf("client message = %q, want stable generated COPY failure", clientMessage)
	}
	for name, message := range map[string]string{
		"client":    clientMessage,
		"telemetry": telemetryMessage,
	} {
		if strings.Contains(message, leakMarker) {
			t.Fatalf("%s message leaked generated execution error: %q", name, message)
		}
	}
}

func TestGeneratedCopyLoadErrorResponseSanitizesNonCallerCancellation(t *testing.T) {
	const leakMarker = "generated COPY private-value /tmp/private-copy"
	for _, tt := range []struct {
		name string
		err  error
	}{
		{
			name: "legacy cancellation text",
			err:  errors.New("context canceled while executing " + leakMarker),
		},
		{
			name: "wrapped deadline",
			err:  fmt.Errorf("%s: %w", leakMarker, context.DeadlineExceeded),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &clientConn{ctx: context.Background()}

			code, clientMessage, telemetryMessage := c.generatedCopyLoadErrorResponse(tt.err)

			if code != "57014" {
				t.Fatalf("code = %q, want 57014", code)
			}
			if clientMessage != "COPY load canceled" {
				t.Fatalf("client message = %q, want fixed non-caller cancellation", clientMessage)
			}
			for name, message := range map[string]string{"client": clientMessage, "telemetry": telemetryMessage} {
				if strings.Contains(message, leakMarker) {
					t.Fatalf("%s message leaked generated cancellation: %q", name, message)
				}
			}
		})
	}
}

func TestCopyLoadErrorResponseRetainsLocalParserDiagnostic(t *testing.T) {
	c := &clientConn{ctx: context.Background()}
	const diagnostic = "invalid binary COPY signature"

	code, clientMessage, _ := c.copyLoadErrorResponse(errors.New(diagnostic))

	if code != "22P02" || !strings.Contains(clientMessage, diagnostic) {
		t.Fatalf("copyLoadErrorResponse() = (%q, %q), want reviewed local parser diagnostic", code, clientMessage)
	}
}

func TestCopyAppenderFallbackLogAttributesExcludeRawError(t *testing.T) {
	const leakMarker = "private copied value /tmp/private-copy"

	attrs := copyAppenderFallbackLogAttributes("public_target", errors.New(leakMarker))
	serialized := fmt.Sprint(attrs...)

	if strings.Contains(serialized, leakMarker) {
		t.Fatalf("fallback log attributes leaked Appender error: %q", serialized)
	}
	for _, required := range []string{"target_table", "public_target", "fallback_reason", "appender_failed", "error_code"} {
		if !strings.Contains(serialized, required) {
			t.Fatalf("fallback log attributes %q missing %q", serialized, required)
		}
	}
}
