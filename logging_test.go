package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestStampedHandlerQuotesValuesWithSpaces guards against regressions where
// the text encoder emits unquoted multi-word values (e.g. error strings),
// which break key=value parsers downstream.
func TestStampedHandlerQuotesValuesWithSpaces(t *testing.T) {
	tests := []struct {
		name      string
		attrs     []slog.Attr
		wantSub   string
		notWant   string
	}{
		{
			name:    "error with spaces is quoted",
			attrs:   []slog.Attr{slog.Any("error", errors.New("flight worker is dead")), slog.Int("worker", 41757)},
			wantSub: `error="flight worker is dead" worker=41757`,
			notWant: "error=flight worker is dead",
		},
		{
			name:    "value with embedded equals is quoted",
			attrs:   []slog.Attr{slog.String("dsn", "host=foo dbname=bar")},
			wantSub: `dsn="host=foo dbname=bar"`,
		},
		{
			name:    "simple alphanumeric stays unquoted",
			attrs:   []slog.Attr{slog.String("worker_pod", "duckgres-5fdb-worker-40772"), slog.Int("count", 3)},
			wantSub: `worker_pod=duckgres-5fdb-worker-40772 count=3`,
			notWant: `"duckgres-5fdb-worker-40772"`,
		},
		{
			name:    "empty string is quoted to stay unambiguous",
			attrs:   []slog.Attr{slog.String("reason", "")},
			wantSub: `reason=""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := &stampedHandler{out: &buf, level: slog.LevelDebug}
			logger := slog.New(h)
			logger.LogAttrs(context.Background(), slog.LevelInfo, "msg", tt.attrs...)
			out := buf.String()
			if !strings.Contains(out, tt.wantSub) {
				t.Errorf("missing %q in:\n%s", tt.wantSub, out)
			}
			if tt.notWant != "" && strings.Contains(out, tt.notWant) {
				t.Errorf("unexpected %q in:\n%s", tt.notWant, out)
			}
		})
	}
}

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
		{
			name:    "quoted password value",
			msg:     `Unable to connect at "host=db password="topSecret" dbname=prod"`,
			notWant: "topSecret",
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
