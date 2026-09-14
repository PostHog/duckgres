//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

const maxTrinoRoutingSnapshotBytes = 8 << 20

type trinoRoutingStore interface {
	ListTrinoRoutingPrincipals(context.Context) ([]configstore.TrinoRoutingPrincipal, error)
}

type trinoRoutingSnapshot struct {
	store  trinoRoutingStore
	groups map[string]string
}

func newTrinoRoutingSnapshot(store trinoRoutingStore, cells trinoFleet) *trinoRoutingSnapshot {
	groups := make(map[string]string, len(cells))
	for _, cell := range cells {
		groups[cell.Cell.ID] = cell.Cell.RoutingGroup
	}
	return &trinoRoutingSnapshot{store: store, groups: groups}
}

type trinoRoutingEntry struct {
	Principal    string `json:"principal"`
	RoutingGroup string `json:"routingGroup"`
}

func (s *trinoRoutingSnapshot) encode(ctx context.Context) ([]byte, error) {
	rows, err := s.store.ListTrinoRoutingPrincipals(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) > configstore.MaxTrinoRoutingPrincipals {
		return nil, errors.New("too many routing principals")
	}
	routes := make([]trinoRoutingEntry, 0, len(rows))
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		if !validTrinoRoutingPrincipal(row.Principal) || seen[row.Principal] {
			return nil, errors.New("invalid routing snapshot")
		}
		seen[row.Principal] = true
		group, configured := s.groups[row.CellID]
		if !configured || row.CellID == "" {
			continue
		}
		if group == "" {
			return nil, errors.New("invalid routing snapshot")
		}
		routes = append(routes, trinoRoutingEntry{Principal: row.Principal, RoutingGroup: group})
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].Principal < routes[j].Principal })
	data, err := json.Marshal(struct {
		Routes []trinoRoutingEntry `json:"routes"`
	}{Routes: routes})
	if err != nil {
		return nil, err
	}
	if len(data) > maxTrinoRoutingSnapshotBytes {
		return nil, errors.New("routing snapshot exceeds response limit")
	}
	return data, nil
}

func validTrinoRoutingPrincipal(principal string) bool {
	return utf8.ValidString(principal) && len(principal) <= 1024 && strings.TrimSpace(principal) != "" &&
		!strings.Contains(principal, ":") && strings.IndexFunc(principal, func(r rune) bool { return r < 32 || r == 127 }) == -1
}

func (s *trinoRoutingSnapshot) handle(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	data, err := s.encode(ctx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "routing snapshot unavailable"})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}
