//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"

	"github.com/gin-gonic/gin"
)

// PeerFetcher fans a live-state read out to every OTHER control-plane replica.
// Live session/query state is in-memory per CP (each CP only knows the sessions
// it owns), so a single replica's view is a slice of the cluster — behind the
// load-balancer that makes the dashboard's numbers flicker as polls land on
// different pods. The aggregating handlers call FetchPeers to merge every CP's
// local view into one cluster-wide answer.
//
// FetchPeers GETs path+"?scope=local" from each peer (the scope param makes the
// peer return ONLY its own in-memory view, never re-fanning out — that's the
// recursion guard). It returns each peer's raw response body plus the total
// number of peers attempted, so a handler can report "N of M CPs responded"
// and degrade gracefully when a peer is slow/down.
type PeerFetcher interface {
	FetchPeers(ctx context.Context, path string) (bodies [][]byte, peers int)
}

// aggMeta is attached to aggregated responses so the UI can show coverage
// ("5/6 CPs") and distinguish a true empty cluster from a partial read.
type aggMeta struct {
	Responders int `json:"cp_responders"`
	Total      int `json:"cp_total"`
}

// localScope reports whether this request is a peer-to-peer fan-out call that
// must return only the local CP's view (no further fan-out).
func localScope(c *gin.Context) bool { return c.Query("scope") == "local" }

// dedupeBy keeps the first item per key, preserving order. The live lists are
// disjoint by ownership (a session/worker lives on exactly one CP), so this is
// normally a no-op — but it makes the cross-CP merge idempotent even if a
// worker briefly appears on two CPs during a takeover/handover window, or if
// self-exclusion ever fails. Keyed on worker id (unique cluster-wide).
func dedupeBy[T any, K comparable](items []T, key func(T) K) []T {
	if len(items) < 2 {
		return items
	}
	seen := make(map[K]struct{}, len(items))
	out := items[:0:0]
	for _, it := range items {
		k := key(it)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, it)
	}
	return out
}

// mergeOrgStats folds each peer's ClusterStatus into the local per-org stats:
// active_sessions and workers are both summed across CPs (each reports only the
// sessions / workers it owns — a worker is owned by exactly one CP, so the sum
// is the cluster-wide per-org count). max_workers is config-store-backed
// (identical on every CP) so the max is taken. Org identity is the name; order
// follows first-seen.
func mergeOrgStats(local []OrgStatus, bodies [][]byte) []OrgStatus {
	byName := map[string]*OrgStatus{}
	var order []string
	add := func(s OrgStatus) {
		if cur, ok := byName[s.Name]; ok {
			cur.ActiveSessions += s.ActiveSessions
			cur.Workers += s.Workers
			if s.MaxWorkers > cur.MaxWorkers {
				cur.MaxWorkers = s.MaxWorkers
			}
			return
		}
		cp := s
		byName[s.Name] = &cp
		order = append(order, s.Name)
	}
	for _, s := range local {
		add(s)
	}
	for _, b := range bodies {
		var cs ClusterStatus
		if json.Unmarshal(b, &cs) == nil {
			for _, s := range cs.Orgs {
				add(s)
			}
		}
	}
	out := make([]OrgStatus, 0, len(order))
	for _, n := range order {
		out = append(out, *byName[n])
	}
	return out
}

// mergePeer is a small generic helper: it unmarshals each peer body into E and
// appends extract(E) to acc. Bodies that fail to parse are skipped (the peer is
// simply counted as a non-responder by the caller). Returns the number of
// bodies that parsed successfully.
func mergePeer[E any, T any](acc *[]T, bodies [][]byte, extract func(E) []T) int {
	ok := 0
	for _, b := range bodies {
		var env E
		if err := json.Unmarshal(b, &env); err != nil {
			continue
		}
		*acc = append(*acc, extract(env)...)
		ok++
	}
	return ok
}
