package server

import "strings"

// QueryStartEvents selects which statements get a QueryStart event.
//
// QueryStart roughly doubles query-log row count, and the statements that
// benefit least from it are the ones clients emit most: BEGIN/COMMIT, SET, and
// driver catalog introspection never hang and never need in-flight visibility.
// Terminal events stay universal regardless of this setting, so nothing
// disappears from the log — cheap statements simply have no paired start row.
type QueryStartEvents string

const (
	// QueryStartEventsData logs a start event for statements that touch data or
	// change schema, and skips transaction control, session settings, and
	// catalog introspection. The default.
	QueryStartEventsData QueryStartEvents = "data"
	// QueryStartEventsAll logs a start event for every statement.
	QueryStartEventsAll QueryStartEvents = "all"
	// QueryStartEventsOff disables start events.
	QueryStartEventsOff QueryStartEvents = "off"
)

// NormalizeQueryStartEvents maps a configured value onto the closed set,
// falling back to the default for anything unrecognized. Query-log volume
// policy must never fail a boot over a typo.
func NormalizeQueryStartEvents(value string) QueryStartEvents {
	switch QueryStartEvents(strings.ToLower(strings.TrimSpace(value))) {
	case QueryStartEventsAll:
		return QueryStartEventsAll
	case QueryStartEventsOff:
		return QueryStartEventsOff
	default:
		return QueryStartEventsData
	}
}

// enabled reports whether the given statement should emit a QueryStart.
func (e QueryStartEvents) enabled(query string) bool {
	switch e {
	case QueryStartEventsOff:
		return false
	case QueryStartEventsAll:
		return query != ""
	default:
		return query != "" && startEventWorthyKeyword(leadingSQLKeyword(query))
	}
}

// startEventWorthyKeyword reports whether a statement of this kind can be
// in-flight long enough for a start event to be worth its row.
func startEventWorthyKeyword(keyword string) bool {
	switch keyword {
	case "BEGIN", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "ABORT", "END",
		"SET", "RESET", "DISCARD", "SHOW", "DEALLOCATE", "PREPARE",
		"LISTEN", "NOTIFY", "UNLISTEN", "CLOSE", "":
		return false
	default:
		return true
	}
}

// leadingSQLKeyword returns the statement's first keyword, uppercased, skipping
// leading comments and whitespace. It is a lexical peek, not a parse: the
// callers here choose logging volume and a coarse query kind, and a wrong guess
// costs a row, not correctness.
func leadingSQLKeyword(query string) string {
	rest := strings.TrimLeft(stripLeadingComments(query), " \t\r\n(")
	end := strings.IndexFunc(rest, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == ';' || r == '(' || r == '*'
	})
	if end < 0 {
		end = len(rest)
	}
	return strings.ToUpper(rest[:end])
}
