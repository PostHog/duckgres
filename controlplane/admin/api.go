//go:build kubernetes

package admin

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// WorkerStatus represents a worker's current status for the API.
type WorkerStatus struct {
	ID             int    `json:"id"`
	Team           string `json:"team"`
	ActiveSessions int    `json:"active_sessions"`
	Status         string `json:"status"`
}

// SessionStatus represents an active session for the API.
type SessionStatus struct {
	PID      int32  `json:"pid"`
	WorkerID int    `json:"worker_id"`
	Team     string `json:"team"`
}

// ClusterStatus aggregates cluster state for the dashboard.
type ClusterStatus struct {
	TotalTeams    int          `json:"total_teams"`
	TotalWorkers  int          `json:"total_workers"`
	TotalSessions int          `json:"total_sessions"`
	Teams         []TeamStatus `json:"teams"`
}

// TeamStatus is a per-team summary.
type TeamStatus struct {
	Name           string `json:"name"`
	Workers        int    `json:"workers"`
	ActiveSessions int    `json:"active_sessions"`
	MaxWorkers     int    `json:"max_workers"`
	MinWorkers     int    `json:"min_workers"`
	MemoryBudget   string `json:"memory_budget"`
}

// TeamStackInfo provides info about a team's live state.
// Implemented by the controlplane.TeamRouter via adapter.
type TeamStackInfo interface {
	// AllTeamStats returns per-team worker and session counts.
	AllTeamStats() []TeamStatus
	// AllWorkerStatuses returns all workers across teams.
	AllWorkerStatuses() []WorkerStatus
	// AllSessionStatuses returns all active sessions across teams.
	AllSessionStatuses() []SessionStatus
}

// RegisterAPI registers all admin REST endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store *configstore.ConfigStore, info TeamStackInfo) {
	h := &apiHandler{store: store, info: info}

	// Teams CRUD
	r.GET("/teams", h.listTeams)
	r.POST("/teams", h.createTeam)
	r.GET("/teams/:name", h.getTeam)
	r.PUT("/teams/:name", h.updateTeam)
	r.DELETE("/teams/:name", h.deleteTeam)

	// Users CRUD
	r.GET("/users", h.listUsers)
	r.POST("/users", h.createUser)
	r.GET("/users/:username", h.getUser)
	r.PUT("/users/:username", h.updateUser)
	r.DELETE("/users/:username", h.deleteUser)

	// Workers (read-only)
	r.GET("/workers", h.listWorkers)

	// Sessions (read-only)
	r.GET("/sessions", h.listSessions)

	// Config singletons
	r.GET("/config/global", h.getGlobalConfig)
	r.PUT("/config/global", h.updateGlobalConfig)
	r.GET("/config/ducklake", h.getDuckLakeConfig)
	r.PUT("/config/ducklake", h.updateDuckLakeConfig)
	r.GET("/config/ratelimit", h.getRateLimitConfig)
	r.PUT("/config/ratelimit", h.updateRateLimitConfig)
	r.GET("/config/querylog", h.getQueryLogConfig)
	r.PUT("/config/querylog", h.updateQueryLogConfig)

	// Overview
	r.GET("/status", h.getClusterStatus)
}

type apiHandler struct {
	store *configstore.ConfigStore
	info  TeamStackInfo
}

// --- Teams ---

func (h *apiHandler) listTeams(c *gin.Context) {
	var teams []configstore.Team
	if err := h.store.DB().Preload("Users").Find(&teams).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, teams)
}

func (h *apiHandler) createTeam(c *gin.Context) {
	var team configstore.Team
	if err := c.ShouldBindJSON(&team); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if team.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if err := h.store.DB().Create(&team).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, team)
}

func (h *apiHandler) getTeam(c *gin.Context) {
	name := c.Param("name")
	var team configstore.Team
	if err := h.store.DB().Preload("Users").First(&team, "name = ?", name).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	c.JSON(http.StatusOK, team)
}

func (h *apiHandler) updateTeam(c *gin.Context) {
	name := c.Param("name")
	var updates configstore.Team
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result := h.store.DB().Model(&configstore.Team{}).Where("name = ?", name).Updates(map[string]interface{}{
		"max_workers":    updates.MaxWorkers,
		"min_workers":    updates.MinWorkers,
		"memory_budget":  updates.MemoryBudget,
		"idle_timeout_s": updates.IdleTimeoutS,
	})
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	var team configstore.Team
	h.store.DB().Preload("Users").First(&team, "name = ?", name)
	c.JSON(http.StatusOK, team)
}

func (h *apiHandler) deleteTeam(c *gin.Context) {
	name := c.Param("name")
	// Delete users first (foreign key)
	h.store.DB().Where("team_name = ?", name).Delete(&configstore.TeamUser{})
	result := h.store.DB().Where("name = ?", name).Delete(&configstore.Team{})
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": name})
}

// --- Users ---

func (h *apiHandler) listUsers(c *gin.Context) {
	var users []configstore.TeamUser
	if err := h.store.DB().Find(&users).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, users)
}

func (h *apiHandler) createUser(c *gin.Context) {
	// Use a raw struct because TeamUser.Password has json:"-"
	var raw struct {
		Username string `json:"username"`
		Password string `json:"password"`
		TeamName string `json:"team_name"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if raw.Username == "" || raw.TeamName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username and team_name are required"})
		return
	}
	if raw.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password is required"})
		return
	}
	hash, err := configstore.HashPassword(raw.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}
	user := configstore.TeamUser{
		Username: raw.Username,
		Password: hash,
		TeamName: raw.TeamName,
	}
	if err := h.store.DB().Create(&user).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, user)
}

func (h *apiHandler) getUser(c *gin.Context) {
	username := c.Param("username")
	var user configstore.TeamUser
	if err := h.store.DB().First(&user, "username = ?", username).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) updateUser(c *gin.Context) {
	username := c.Param("username")
	var raw struct {
		Password string `json:"password"`
		TeamName string `json:"team_name"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	updates := map[string]interface{}{}
	if raw.Password != "" {
		hash, err := configstore.HashPassword(raw.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
			return
		}
		updates["password"] = hash
	}
	if raw.TeamName != "" {
		updates["team_name"] = raw.TeamName
	}
	result := h.store.DB().Model(&configstore.TeamUser{}).Where("username = ?", username).Updates(updates)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	var user configstore.TeamUser
	h.store.DB().First(&user, "username = ?", username)
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) deleteUser(c *gin.Context) {
	username := c.Param("username")
	result := h.store.DB().Where("username = ?", username).Delete(&configstore.TeamUser{})
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": username})
}

// --- Workers ---

func (h *apiHandler) listWorkers(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, []WorkerStatus{})
		return
	}
	c.JSON(http.StatusOK, h.info.AllWorkerStatuses())
}

// --- Sessions ---

func (h *apiHandler) listSessions(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, []SessionStatus{})
		return
	}
	c.JSON(http.StatusOK, h.info.AllSessionStatuses())
}

// --- Config Singletons ---

func (h *apiHandler) getGlobalConfig(c *gin.Context) {
	var cfg configstore.GlobalConfig
	h.store.DB().First(&cfg, 1)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateGlobalConfig(c *gin.Context) {
	var cfg configstore.GlobalConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	h.store.DB().Save(&cfg)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getDuckLakeConfig(c *gin.Context) {
	var cfg configstore.DuckLakeConfig
	h.store.DB().First(&cfg, 1)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateDuckLakeConfig(c *gin.Context) {
	var cfg configstore.DuckLakeConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	h.store.DB().Save(&cfg)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getRateLimitConfig(c *gin.Context) {
	var cfg configstore.RateLimitConfig
	h.store.DB().First(&cfg, 1)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateRateLimitConfig(c *gin.Context) {
	var cfg configstore.RateLimitConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	h.store.DB().Save(&cfg)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getQueryLogConfig(c *gin.Context) {
	var cfg configstore.QueryLogConfig
	h.store.DB().First(&cfg, 1)
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateQueryLogConfig(c *gin.Context) {
	var cfg configstore.QueryLogConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	h.store.DB().Save(&cfg)
	c.JSON(http.StatusOK, cfg)
}

// --- Status ---

func (h *apiHandler) getClusterStatus(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, ClusterStatus{})
		return
	}

	teamStats := h.info.AllTeamStats()
	totalWorkers := 0
	totalSessions := 0
	for _, ts := range teamStats {
		totalWorkers += ts.Workers
		totalSessions += ts.ActiveSessions
	}

	c.JSON(http.StatusOK, ClusterStatus{
		TotalTeams:    len(teamStats),
		TotalWorkers:  totalWorkers,
		TotalSessions: totalSessions,
		Teams:         teamStats,
	})
}
