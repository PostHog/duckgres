package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"testing"
)

func TestRedactingHandler(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		attrs   []slog.Attr
		notWant string // substring that must NOT appear in output
		want    string // substring that MUST appear in output
	}{
		{
			name:    "password in message",
			msg:     `connection to "host=foo password=secret123 dbname=bar" failed`,
			notWant: "secret123",
			want:    "[REDACTED]",
		},
		{
			name:    "password in attr value",
			msg:     "connection failed",
			attrs:   []slog.Attr{slog.String("dsn", "host=foo password=hunter2 dbname=bar")},
			notWant: "hunter2",
			want:    "[REDACTED]",
		},
		{
			name:    "password with colon separator",
			msg:     "err: password: s3cret in log",
			notWant: "s3cret",
			want:    "[REDACTED]",
		},
		{
			name:    "no password present",
			msg:     "normal log message",
			attrs:   []slog.Attr{slog.String("key", "value")},
			notWant: "[REDACTED]",
			want:    "normal log message",
		},
		{
			name:    "password in error attr",
			msg:     "query failed",
			attrs:   []slog.Attr{slog.Any("error", fmt.Errorf("password=oops"))},
			notWant: "oops",
			want:    "[REDACTED]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
			handler := &redactingHandler{inner: inner}
			logger := slog.New(handler)

			logger.LogAttrs(context.Background(), slog.LevelInfo, tt.msg, tt.attrs...)

			output := buf.String()
			if tt.notWant != "" {
				if bytes.Contains([]byte(output), []byte(tt.notWant)) {
					t.Errorf("output contains %q which should have been redacted:\n%s", tt.notWant, output)
				}
			}
			if tt.want != "" {
				if !bytes.Contains([]byte(output), []byte(tt.want)) {
					t.Errorf("output missing expected %q:\n%s", tt.want, output)
				}
			}
		})
	}
}
