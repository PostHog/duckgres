//go:build kubernetes

package admin

import (
	"errors"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// NewTrinoFleetAPI builds isolated coordinator views and preserves the legacy default.
func NewTrinoFleetAPI(cells []TrinoCell, clients []TrinoCoordinatorClient, orgs TrinoOrgStore, audit *AuditStore) *TrinoAPI {
	if len(cells) == 0 || len(cells) != len(clients) || orgs == nil {
		return nil
	}
	a := &TrinoAPI{fleet: make(map[string]*TrinoAPI), orgs: orgs, audit: audit}
	stored := make(map[string]bool)
	for i, cell := range cells {
		if cell.ID == "" || a.fleet[cell.ID] != nil || stored[cell.storedID()] {
			return nil
		}
		child := NewTrinoAPI(cell, clients[i], orgs, audit)
		if child == nil {
			return nil
		}
		child.filterCell = true
		a.fleet[cell.ID] = child
		stored[cell.storedID()] = true
	}
	return a
}

func registerTrinoFleetAPI(r *gin.RouterGroup, api *TrinoAPI) {
	r.GET("/trino/cells", api.handleCells)
	r.GET("/trino/status", api.forCell((*TrinoAPI).handleStatus))
	r.GET("/trino/queries", api.forCell((*TrinoAPI).handleQueries))
	r.GET("/trino/queries/:id", api.forCell((*TrinoAPI).handleQueryDetail))
	r.POST("/trino/queries/:id/kill", api.forCell((*TrinoAPI).handleKillQuery))
	r.GET("/trino/nodes", api.forCell((*TrinoAPI).handleNodes))
	r.GET("/trino/orgs", api.forCell((*TrinoAPI).handleOrgs))
	r.GET("/orgs/:id/trino", api.handleFleetOrg)
	r.PUT("/orgs/:id/trino/cell", api.handleSelectCell)
}

func (a *TrinoAPI) forCell(handle func(*TrinoAPI, *gin.Context)) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.DefaultQuery("cell", "legacy")
		selected := a.fleet[id]
		if selected == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "unknown Trino cell"})
			return
		}
		handle(selected, c)
	}
}

func (a *TrinoAPI) handleCells(c *gin.Context) {
	cells := make([]TrinoCell, 0, len(a.fleet))
	for _, cell := range a.fleet {
		cells = append(cells, cell.cell)
	}
	sort.Slice(cells, func(i, j int) bool { return cells[i].ID < cells[j].ID })
	c.JSON(http.StatusOK, gin.H{"cells": cells})
}

func (a *TrinoAPI) handleFleetOrg(c *gin.Context) {
	row, err := a.orgs.GetManagedWarehouseTrino(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino assignment"})
		return
	}
	selected := a.fleet["legacy"]
	if row != nil && row.TrinoCellID != "" {
		selected = nil
		for _, candidate := range a.fleet {
			if candidate.cell.storedID() == row.TrinoCellID {
				selected = candidate
				break
			}
		}
	}
	if selected == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "the assigned Trino cell is not configured", "assigned": row != nil && row.TrinoCellID != ""})
		return
	}
	selected.writeOrgDetail(c, c.Param("id"), row)
}

type trinoCellSelector interface {
	SelectTrinoCell(orgID, cellID string) error
}

func (a *TrinoAPI) handleSelectCell(c *gin.Context) {
	identity := IdentityFromContext(c)
	if identity == nil || identity.Role != RoleAdmin {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
		return
	}
	var req struct {
		Cell string `json:"cell" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || a.fleet[req.Cell] == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a configured Trino cell is required"})
		return
	}
	selector, ok := a.orgs.(trinoCellSelector)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Trino cell selection is unavailable"})
		return
	}
	if err := selector.SelectTrinoCell(c.Param("id"), a.fleet[req.Cell].cell.storedID()); err != nil {
		switch {
		case errors.Is(err, configstore.ErrTrinoCellSelectionConflict):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		case errors.Is(err, configstore.ErrTrinoWarehouseNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot select Trino cell"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"cell": a.fleet[req.Cell].cell, "assigned": true})
}
