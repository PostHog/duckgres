package server

import "github.com/google/uuid"

// newQueryID mints the per-statement identifier carried by every query-log
// event, span, and error log for one inbound statement.
//
// UUIDv7 is deliberate: it is time-ordered, so IDs minted in sequence sort in
// arrival order, cluster within the query log's monthly partitions, and index
// without the write amplification a random UUID causes.
//
// This runs on the query path, so it never blocks and never fails a statement:
// an entropy failure degrades to a v4, and then to an empty ID, rather than
// panicking. An empty ID costs correlation, not correctness.
func newQueryID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	if id, err := uuid.NewRandom(); err == nil {
		return id.String()
	}
	return ""
}
