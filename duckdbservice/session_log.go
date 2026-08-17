package duckdbservice

import "log/slog"

// attachSessionLog stamps user and pid on the session logger. Org/worker
// stay on slog.Default() via stampWorkerLogIdentity. Never SetDefault(user).
func attachSessionLog(s *Session, username string, pid int32) {
	if s == nil {
		return
	}
	attrs := []any{"user", username}
	if pid > 0 {
		attrs = append(attrs, "pid", pid)
	}
	s.logger = slog.Default().With(attrs...)
}

func clearSessionLog(s *Session) {
	if s == nil {
		return
	}
	s.logger = nil
}

// Logger returns the session-scoped logger (user+pid), or the default
// org+worker logger when the session is gone or not yet attached.
func (s *Session) Logger() *slog.Logger {
	if s == nil || s.logger == nil {
		return slog.Default()
	}
	return s.logger
}

func logStuckQuery(s *Session, attrs ...any) {
	s.Logger().Warn("Query appears stuck — no progress detected.", attrs...)
}
