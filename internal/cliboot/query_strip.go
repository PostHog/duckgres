package cliboot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/usersecrets"
)

const (
	queryTextOff      = "off"
	queryTextRedacted = "redacted"
	queryTextOn       = "on"

	fallbackSecretErrorPlaceholder = "(error redacted: possible statement echo)"
)

var queryAttrKeys = map[string]bool{
	"query":            true,
	"sql":              true,
	"transpiled":       true,
	"transpiled_query": true,
	"statement":        true,
}

var errorAttrKeys = map[string]bool{
	"error":     true,
	"err":       true,
	"exception": true,
}

// QueryStripHandler rewrites query-shaped attrs and secret-echoing errors
// on the PostHog OTLP branch only. It never wraps stderr.
type QueryStripHandler struct {
	Inner     slog.Handler
	QueryText string // off | redacted | on
	origQuery string // unredacted snapshot from WithAttrs
}

func (h *QueryStripHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.Inner.Enabled(ctx, l)
}

func (h *QueryStripHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.Inner.Handle(ctx, stripRecord(r, h.QueryText, h.origQuery))
}

func (h *QueryStripHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	snapped := firstQueryAttr(attrs)
	if snapped == "" {
		snapped = h.origQuery
	}
	return &QueryStripHandler{
		Inner:     h.Inner.WithAttrs(rewriteAttrs(attrs, h.QueryText, snapped)),
		QueryText: h.QueryText,
		origQuery: snapped,
	}
}

func (h *QueryStripHandler) WithGroup(name string) slog.Handler {
	return &QueryStripHandler{Inner: h.Inner.WithGroup(name), QueryText: h.QueryText, origQuery: h.origQuery}
}

func stripRecord(r slog.Record, queryText, stashedQuery string) slog.Record {
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	origQuery := firstQueryAttr(attrs)
	if origQuery == "" {
		origQuery = stashedQuery
	}
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	nr.AddAttrs(rewriteAttrs(attrs, queryText, origQuery)...)
	return nr
}

func firstQueryAttr(attrs []slog.Attr) string {
	for _, a := range attrs {
		if a.Value.Kind() == slog.KindGroup {
			if q := firstQueryAttr(a.Value.Group()); q != "" {
				return q
			}
			continue
		}
		if queryAttrKeys[a.Key] {
			if s := attrString(a); s != "" {
				return s
			}
		}
	}
	return ""
}

func rewriteAttrs(attrs []slog.Attr, queryText, origQuery string) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if strings.EqualFold(a.Key, "secret_statements") {
			continue
		}
		if a.Value.Kind() == slog.KindGroup {
			a.Value = slog.GroupValue(rewriteAttrs(a.Value.Group(), queryText, origQuery)...)
			out = append(out, a)
			continue
		}
		if queryAttrKeys[a.Key] {
			switch queryText {
			case queryTextOff:
				continue
			case queryTextOn:
				out = append(out, a)
			default:
				q := attrString(a)
				if q == "" {
					q = origQuery
				}
				out = append(out, slog.String(a.Key, server.BoundQueryLogText(usersecrets.RedactForLog(q))))
			}
			continue
		}
		if errorAttrKeys[a.Key] {
			out = append(out, rewriteErrorAttr(a, origQuery))
			continue
		}
		out = append(out, a)
	}
	return out
}

func rewriteErrorAttr(a slog.Attr, origQuery string) slog.Attr {
	errText := attrString(a)
	if origQuery != "" {
		return replaceErrorText(a, usersecrets.RedactErrorForLog(origQuery, errText))
	}
	if looksLikeSecretEcho(errText) {
		return replaceErrorText(a, fallbackSecretErrorPlaceholder)
	}
	redacted := server.RedactSecrets(errText)
	if redacted != errText {
		return replaceErrorText(a, redacted)
	}
	return a
}

func replaceErrorText(a slog.Attr, text string) slog.Attr {
	if a.Value.Kind() == slog.KindAny {
		if _, ok := a.Value.Any().(error); ok {
			return slog.Any(a.Key, errors.New(text))
		}
	}
	return slog.String(a.Key, text)
}

func attrString(a slog.Attr) string {
	switch a.Value.Kind() {
	case slog.KindString:
		return a.Value.String()
	case slog.KindAny:
		if err, ok := a.Value.Any().(error); ok {
			return err.Error()
		}
		return fmt.Sprintf("%v", a.Value.Any())
	default:
		return a.Value.String()
	}
}

// looksLikeSecretEcho is the no-query fallback. Never match LINE 1: —
// DuckDB puts that on essentially every engine error.
func looksLikeSecretEcho(s string) bool {
	lower := strings.ToLower(s)
	if strings.Contains(lower, "password=") || strings.Contains(lower, "password:") {
		return true
	}
	return containsSecretToken(s)
}

func containsSecretToken(s string) bool {
	upper := strings.ToUpper(s)
	for {
		i := strings.Index(upper, "SECRET")
		if i < 0 {
			return false
		}
		beforeOK := i == 0 || !isIdentByte(upper[i-1])
		after := i + len("SECRET")
		afterOK := after == len(upper) || !isIdentByte(upper[after])
		if beforeOK && afterOK {
			return true
		}
		upper = upper[i+1:]
	}
}

func isIdentByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}
